"""Concurrency and thread-safety tests for PeerRegistry."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from meshweaver.network.peer_registry import PeerRegistry, PeerStatus


@pytest.fixture
def registry() -> PeerRegistry:
    """Provide a fresh registry for each test."""
    return PeerRegistry()


def register_peer(
    registry: PeerRegistry,
    node_id: str,
    host: str = "127.0.0.1",
    port: int = 8000,
    status: PeerStatus = PeerStatus.ACTIVE,
    cpu_percent: float = 10.0,
    last_seen: float | None = None,
):
    """Register a peer using the project's established API."""
    return registry.register_peer(
        node_id=node_id,
        host=host,
        port=port,
        status=status,
        cpu_percent=cpu_percent,
        last_seen=last_seen,
    )


def test_concurrent_registrations(registry: PeerRegistry) -> None:
    """Concurrent registrations should not lose peer entries."""
    num_threads = 20
    peers_per_thread = 25
    total_expected = num_threads * peers_per_thread
    barrier = threading.Barrier(num_threads)

    def register_batch(worker_id: int) -> None:
        barrier.wait()
        for idx in range(peers_per_thread):
            register_peer(
                registry,
                node_id=f"node-{worker_id}-{idx}",
                port=9000 + idx,
            )

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(register_batch, worker_id)
            for worker_id in range(num_threads)
        ]
        for future in as_completed(futures):
            future.result()

    all_peers = registry.get_all_peers()
    assert len(all_peers) == total_expected

    registered_ids = {peer.node_id for peer in all_peers}
    expected_ids = {
        f"node-{worker_id}-{idx}"
        for worker_id in range(num_threads)
        for idx in range(peers_per_thread)
    }
    assert registered_ids == expected_ids


def test_concurrent_retrievals(registry: PeerRegistry) -> None:
    """Concurrent reads should return consistent peer data."""
    num_peers = 50

    for idx in range(num_peers):
        register_peer(
            registry,
            node_id=f"peer-{idx}",
            port=8000 + idx,
            cpu_percent=float(idx),
        )

    num_threads = 16
    reads_per_thread = 100
    barrier = threading.Barrier(num_threads)

    def read_worker() -> None:
        barrier.wait()
        for step in range(reads_per_thread):
            target_idx = step % num_peers
            node_id = f"peer-{target_idx}"
            record = registry.get_peer(node_id)

            assert record is not None
            assert record.node_id == node_id
            assert record.port == 8000 + target_idx
            assert record.cpu_percent == float(target_idx)

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(read_worker)
            for _ in range(num_threads)
        ]
        for future in as_completed(futures):
            future.result()


def test_concurrent_updates_same_peer(registry: PeerRegistry) -> None:
    """Concurrent updates should not corrupt a shared peer record."""
    target_id = "shared-node"

    register_peer(
        registry,
        node_id=target_id,
        cpu_percent=0.0,
    )

    num_threads = 20
    updates_per_thread = 50
    barrier = threading.Barrier(num_threads)

    def update_worker(worker_id: int) -> None:
        barrier.wait()

        for step in range(updates_per_thread):
            cpu_value = float((worker_id * 100 + step) % 100)
            status = (
                PeerStatus.ACTIVE
                if step % 2 == 0
                else PeerStatus.INACTIVE
            )

            updated = registry.update_peer(
                target_id,
                cpu_percent=cpu_value,
                status=status,
                last_seen=time.time(),
            )
            assert updated is not None

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(update_worker, worker_id)
            for worker_id in range(num_threads)
        ]
        for future in as_completed(futures):
            future.result()

    final_record = registry.get_peer(target_id)

    assert final_record is not None
    assert final_record.node_id == target_id
    assert 0.0 <= final_record.cpu_percent < 100.0
    assert final_record.status in {
        PeerStatus.ACTIVE,
        PeerStatus.INACTIVE,
    }
    assert len(registry.get_all_peers()) == 1


def test_concurrent_reads_during_updates(registry: PeerRegistry) -> None:
    """Readers should see valid snapshots during concurrent updates."""
    node_ids = [f"worker-node-{idx}" for idx in range(10)]

    for node_id in node_ids:
        register_peer(registry, node_id=node_id, cpu_percent=5.0)

    stop_event = threading.Event()
    errors: list[Exception] = []

    def writer() -> None:
        counter = 0
        try:
            while not stop_event.is_set():
                node_id = node_ids[counter % len(node_ids)]
                updated = registry.update_peer(
                    node_id,
                    cpu_percent=float(counter % 100),
                    last_seen=time.time(),
                )
                assert updated is not None
                counter += 1
        except Exception as exc:
            errors.append(exc)

    def reader() -> None:
        try:
            while not stop_event.is_set():
                snapshot = registry.get_all_peers()

                assert len(snapshot) == len(node_ids)

                for peer in snapshot:
                    assert peer.node_id in node_ids
                    assert peer.cpu_percent is not None
                    assert 0.0 <= peer.cpu_percent < 100.0
        except Exception as exc:
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = [
            executor.submit(writer) for _ in range(4)
        ] + [
            executor.submit(reader) for _ in range(8)
        ]

        time.sleep(0.35)
        stop_event.set()

        for future in as_completed(futures):
            future.result()

    assert not errors


def test_returned_peer_objects_are_isolated(
    registry: PeerRegistry,
) -> None:
    """Changes to returned peer copies must not affect registry state."""
    register_peer(
        registry,
        node_id="isolated-node",
        host="10.0.0.5",
        port=7000,
        cpu_percent=22.5,
    )

    # A retrieved record must be detached from internal registry state.
    fetched = registry.get_peer("isolated-node")
    assert fetched is not None

    fetched.cpu_percent = 88.8
    fetched.host = "192.168.1.99"

    fetched_again = registry.get_peer("isolated-node")
    assert fetched_again is not None
    assert fetched_again.cpu_percent == 22.5
    assert fetched_again.host == "10.0.0.5"

    # Records returned by get_all_peers must also be detached.
    listed = registry.get_all_peers()
    assert len(listed) == 1

    listed[0].port = 1234

    verified = registry.get_peer("isolated-node")
    assert verified is not None
    assert verified.port == 7000


def test_registry_consistency_under_mixed_workload(
    registry: PeerRegistry,
) -> None:
    """Mixed concurrent operations should preserve registry consistency."""
    num_workers = 12
    iterations = 40
    barrier = threading.Barrier(num_workers)

    # Seed stale peers for the cleanup operation.
    for idx in range(10):
        register_peer(
            registry,
            node_id=f"stale-{idx}",
            port=6000 + idx,
            cpu_percent=1.0,
            last_seen=time.time() - 3600,
        )

    def mixed_worker(worker_id: int) -> None:
        barrier.wait()

        ephemeral_id = f"ephemeral-{worker_id}"
        persistent_id = f"persistent-{worker_id}"

        register_peer(
            registry,
            node_id=persistent_id,
            cpu_percent=10.0,
        )

        for step in range(iterations):
            register_peer(
                registry,
                node_id=ephemeral_id,
                cpu_percent=float(step),
            )

            assert registry.get_peer(ephemeral_id) is not None

            updated = registry.update_peer(
                persistent_id,
                cpu_percent=float(step),
                last_seen=time.time(),
            )
            assert updated is not None

            registry.get_all_peers()

            assert registry.remove_peer(ephemeral_id) is True

            if step % 10 == 0:
                registry.purge_inactive_peers(timeout_seconds=60.0)

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            executor.submit(mixed_worker, worker_id)
            for worker_id in range(num_workers)
        ]
        for future in as_completed(futures):
            future.result()

    # First sweep marks stale peers inactive.
    registry.purge_inactive_peers(timeout_seconds=60.0)

    # Second sweep removes peers already marked inactive.
    registry.purge_inactive_peers()

    remaining_peers = registry.get_all_peers()
    remaining_ids = {peer.node_id for peer in remaining_peers}

    expected_persistent = {
        f"persistent-{worker_id}"
        for worker_id in range(num_workers)
    }

    assert remaining_ids == expected_persistent
    assert len(remaining_peers) == num_workers
