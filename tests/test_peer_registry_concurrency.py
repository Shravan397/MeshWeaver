"""Concurrency and thread-safety tests for PeerRegistry."""

from __future__ import annotations

import copy
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from meshweaver.network.peer_registry import PeerRecord, PeerRegistry, PeerStatus


@pytest.fixture
def registry() -> PeerRegistry:
    """Provide a fresh PeerRegistry instance for each test."""
    return PeerRegistry()


def _make_peer(
    node_id: str,
    host: str = "127.0.0.1",
    port: int = 8000,
    status: PeerStatus = PeerStatus.ACTIVE,
    cpu_usage: float = 10.0,
) -> PeerRecord:
    """Helper to construct a PeerRecord with sensible defaults."""
    return PeerRecord(
        node_id=node_id,
        host=host,
        port=port,
        status=status,
        cpu_usage=cpu_usage,
        last_seen=time.time(),
    )


def test_concurrent_registrations(registry: PeerRegistry) -> None:
    """Multiple threads registering distinct peers simultaneously should not lose any entries."""
    num_threads = 20
    peers_per_thread = 25
    total_expected = num_threads * peers_per_thread
    barrier = threading.Barrier(num_threads)

    def register_batch(worker_id: int) -> None:
        barrier.wait()
        for idx in range(peers_per_thread):
            node_id = f"node-{worker_id}-{idx}"
            peer = _make_peer(node_id=node_id, port=9000 + idx)
            registry.register(peer)

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(register_batch, w) for w in range(num_threads)]
        for future in as_completed(futures):
            future.result()

    all_peers = registry.list_peers()
    assert len(all_peers) == total_expected

    registered_ids = {p.node_id for p in all_peers}
    expected_ids = {
        f"node-{w}-{idx}"
        for w in range(num_threads)
        for idx in range(peers_per_thread)
    }
    assert registered_ids == expected_ids


def test_concurrent_retrievals(registry: PeerRegistry) -> None:
    """Multiple threads reading peer records simultaneously should return consistent data."""
    num_peers = 50
    for idx in range(num_peers):
        registry.register(_make_peer(node_id=f"peer-{idx}", port=8000 + idx, cpu_usage=float(idx)))

    num_threads = 16
    reads_per_thread = 100
    barrier = threading.Barrier(num_threads)

    def read_worker() -> None:
        barrier.wait()
        for step in range(reads_per_thread):
            target_idx = step % num_peers
            node_id = f"peer-{target_idx}"
            record = registry.get(node_id)
            assert record is not None
            assert record.node_id == node_id
            assert record.port == 8000 + target_idx
            assert record.cpu_usage == float(target_idx)

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(read_worker) for _ in range(num_threads)]
        for future in as_completed(futures):
            future.result()


def test_concurrent_updates_same_peer(registry: PeerRegistry) -> None:
    """Concurrent updates to the same peer must serialize cleanly without corrupting state."""
    target_id = "shared-node"
    registry.register(_make_peer(node_id=target_id, cpu_usage=0.0, status=PeerStatus.ACTIVE))

    num_threads = 20
    updates_per_thread = 50
    barrier = threading.Barrier(num_threads)

    def update_worker(worker_id: int) -> None:
        barrier.wait()
        for step in range(updates_per_thread):
            cpu_val = float((worker_id * 100 + step) % 100)
            status = PeerStatus.ACTIVE if step % 2 == 0 else PeerStatus.BUSY
            registry.update(
                target_id,
                cpu_usage=cpu_val,
                status=status,
                last_seen=time.time(),
            )

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(update_worker, w) for w in range(num_threads)]
        for future in as_completed(futures):
            future.result()

    final_record = registry.get(target_id)
    assert final_record is not None
    assert final_record.node_id == target_id
    assert 0.0 <= final_record.cpu_usage < 100.0
    assert final_record.status in {PeerStatus.ACTIVE, PeerStatus.BUSY}
    assert len(registry.list_peers()) == 1


def test_concurrent_reads_during_updates(registry: PeerRegistry) -> None:
    """Readers should never see torn or invalid state while writers continuously update peers."""
    node_ids = [f"worker-node-{i}" for i in range(10)]
    for node_id in node_ids:
        registry.register(_make_peer(node_id=node_id, cpu_usage=5.0))

    stop_event = threading.Event()
    errors: list[Exception] = []

    def writer() -> None:
        counter = 0
        try:
            while not stop_event.is_set():
                node_id = node_ids[counter % len(node_ids)]
                registry.update(
                    node_id,
                    cpu_usage=float(counter % 100),
                    last_seen=time.time(),
                )
                counter += 1
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def reader() -> None:
        try:
            while not stop_event.is_set():
                snapshot = registry.list_peers()
                assert len(snapshot) == len(node_ids)
                for peer in snapshot:
                    assert peer.node_id in node_ids
                    assert 0.0 <= peer.cpu_usage < 100.0
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = [executor.submit(writer) for _ in range(4)] + [
            executor.submit(reader) for _ in range(8)
        ]
        time.sleep(0.35)
        stop_event.set()
        for future in as_completed(futures):
            future.result()

    assert not errors


def test_returned_peer_objects_are_isolated(registry: PeerRegistry) -> None:
    """Mutating a retrieved PeerRecord externally must not mutate internal registry state."""
    original_peer = _make_peer(
        node_id="isolated-node",
        host="10.0.0.5",
        port=7000,
        status=PeerStatus.ACTIVE,
        cpu_usage=22.5,
    )
    registry.register(original_peer)

    # Mutating the input object after registration should not affect stored state
    original_peer.cpu_usage = 99.9
    original_peer.host = "192.168.1.99"

    fetched = registry.get("isolated-node")
    assert fetched is not None
    assert fetched.cpu_usage == 22.5
    assert fetched.host == "10.0.0.5"

    # Mutating the object returned by get() should not affect stored state
    fetched.cpu_usage = 88.8
    fetched.status = PeerStatus.OFFLINE

    fetched_again = registry.get("isolated-node")
    assert fetched_again is not None
    assert fetched_again.cpu_usage == 22.5
    assert fetched_again.status == PeerStatus.ACTIVE

    # Mutating items returned by list_peers() should not affect stored state
    listed = registry.list_peers()
    assert len(listed) == 1
    listed[0].port = 1234

    verified = registry.get("isolated-node")
    assert verified is not None
    assert verified.port == 7000


def test_registry_consistency_under_mixed_workload(registry: PeerRegistry) -> None:
    """Combined register, update, read, cleanup, and remove operations maintain invariants."""
    num_workers = 12
    iterations = 40
    barrier = threading.Barrier(num_workers)

    # Seed some stale peers that cleanup should remove
    for i in range(10):
        stale_peer = PeerRecord(
            node_id=f"stale-{i}",
            host="127.0.0.1",
            port=6000 + i,
            status=PeerStatus.ACTIVE,
            cpu_usage=1.0,
            last_seen=time.time() - 3600,
        )
        registry.register(stale_peer)

    def mixed_worker(worker_id: int) -> None:
        barrier.wait()
        ephemeral_id = f"ephemeral-{worker_id}"
        persistent_id = f"persistent-{worker_id}"

        registry.register(_make_peer(node_id=persistent_id, cpu_usage=10.0))

        for step in range(iterations):
            # Register and immediately remove an ephemeral peer
            registry.register(_make_peer(node_id=ephemeral_id, cpu_usage=float(step)))
            _ = registry.get(ephemeral_id)
            registry.update(persistent_id, cpu_usage=float(step), last_seen=time.time())
            _ = registry.list_peers()
            registry.remove(ephemeral_id)

            if step % 10 == 0:
                registry.cleanup_stale(max_age=60.0)

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(mixed_worker, w) for w in range(num_workers)]
        for future in as_completed(futures):
            future.result()

    # Run a final stale cleanup to ensure all seeded stale entries are purged
    registry.cleanup_stale(max_age=60.0)

    remaining_peers = registry.list_peers()
    remaining_ids = {p.node_id for p in remaining_peers}

    # Only the persistent nodes should remain
    expected_persistent = {f"persistent-{w}" for w in range(num_workers)}
    assert remaining_ids == expected_persistent
    assert len(remaining_peers) == num_workers

    # Verify deep-copy isolation of final snapshot
    snapshot_copy = copy.deepcopy(remaining_peers)
    assert len(snapshot_copy) == num_workers
