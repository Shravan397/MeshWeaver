"""Tests for peer recovery, endpoint deduplication, heartbeat, and UDP errors."""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import Mock

import pytest

from meshweaver.__main__ import handle_message, probe_bootstrap_peers_loop
from meshweaver.network.discovery import PeerDiscovery
from meshweaver.network.heartbeat import HeartbeatManager
from meshweaver.network.peer_registry import PeerRegistry, PeerStatus
from meshweaver.network.protocol import Message, MessageType, decode_message
from meshweaver.network.udp import MeshWeaverUDPProtocol, start_udp_listener
from meshweaver.node import Node


class FakeTransport:
    """In-memory UDP transport stand-in."""

    def __init__(self, fail_with: OSError | None = None) -> None:
        self.sent_packets: list[tuple[bytes, tuple[str, int]]] = []
        self.fail_with = fail_with

    def send(self, data: bytes, address: tuple[str, int]) -> int:
        if self.fail_with is not None:
            raise self.fail_with
        self.sent_packets.append((data, address))
        return len(data)


class FakeClock:
    """Deterministic clock for testing heartbeat timing."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_heartbeat_send_failure_cleans_up_pending_requests() -> None:
    """Verify failed heartbeat sends do not leave dangling pending requests."""
    win_err = OSError("An existing connection was forcibly closed by the remote host")
    win_err.winerror = 10054  # type: ignore[attr-defined]
    transport = FakeTransport(fail_with=win_err)
    registry = PeerRegistry()
    clock = FakeClock(1500.0)

    registry.register_peer("node-worker", "127.0.0.1", 9000, last_seen=1500.0)
    hb = HeartbeatManager(
        node_id="node-submitter",
        transport=transport,
        peer_registry=registry,
        clock=clock,
    )

    msg_id = hb.send_heartbeat("node-worker")
    assert msg_id not in hb.pending_requests
    assert len(hb.pending_requests) == 0


def test_ping_pong_round_trip_communication() -> None:
    """Verify PING generation, PONG reply construction, and RTT matching."""
    transport_a = FakeTransport()
    transport_b = FakeTransport()
    registry_a = PeerRegistry()
    registry_b = PeerRegistry()
    clock = FakeClock(2000.0)

    registry_a.register_peer("node-b", "127.0.0.1", 9001, last_seen=2000.0)

    hb_a = HeartbeatManager(
        node_id="node-a",
        transport=transport_a,
        peer_registry=registry_a,
        clock=clock,
    )
    hb_b = HeartbeatManager(
        node_id="node-b",
        transport=transport_b,
        peer_registry=registry_b,
        clock=clock,
        cpu_provider=lambda: 12.5,
    )

    ping_id = hb_a.send_heartbeat("node-b")
    raw_ping, addr = transport_a.sent_packets[0]
    assert addr == ("127.0.0.1", 9001)

    ping_msg = decode_message(raw_ping)
    assert ping_msg.type == MessageType.PING
    assert ping_msg.message_id == ping_id

    clock.advance(0.05)
    pong_msg = hb_b.build_pong(ping_msg)
    assert pong_msg.type == MessageType.PONG
    assert pong_msg.payload["ref_id"] == ping_id
    assert pong_msg.payload["cpu_percent"] == 12.5

    rtt = hb_a.handle_message(pong_msg)
    assert rtt == pytest.approx(0.05)
    updated_b = registry_a.get_peer("node-b")
    assert updated_b is not None
    assert updated_b.status == PeerStatus.ACTIVE
    assert updated_b.cpu_percent == 12.5


def test_peer_recovery_with_same_and_different_node_id_after_restart() -> None:
    """Verify recovery works when a restarted node keeps or changes its node_id."""
    transport = FakeTransport()
    registry = PeerRegistry()
    discovery = PeerDiscovery(registry)
    clock = FakeClock(3000.0)

    # Case A: Peer stops, times out, and restarts with the SAME node_id.
    registry.register_peer("node-worker-1", "127.0.0.1", 9000, last_seen=3000.0)
    hb = HeartbeatManager(
        node_id="node-submitter",
        transport=transport,
        peer_registry=registry,
        interval=2.0,
        timeout=5.0,
        clock=clock,
    )

    hb.run_cycle()
    clock.advance(6.0)
    assert hb.check_timeouts() == ["node-worker-1"]
    assert registry.get_peer("node-worker-1").status == PeerStatus.INACTIVE  # type: ignore[union-attr]

    hb.run_cycle()
    probe_same = decode_message(transport.sent_packets[-1][0])
    clock.advance(0.05)
    pong_same = Message(
        type=MessageType.PONG,
        sender_id="node-worker-1",
        message_id=probe_same.message_id,
        timestamp=clock(),
        payload={"ref_id": probe_same.message_id},
    )
    discovery.handle_discovery_message(pong_same, ("127.0.0.1", 9000))
    assert hb.handle_message(pong_same) == pytest.approx(0.05)
    assert registry.get_peer("node-worker-1").status == PeerStatus.ACTIVE  # type: ignore[union-attr]

    # Case B: Peer stops again, times out, and restarts with a NEW node_id.
    hb.run_cycle()
    clock.advance(6.0)
    assert hb.check_timeouts() == ["node-worker-1"]
    assert registry.get_peer("node-worker-1").status == PeerStatus.INACTIVE  # type: ignore[union-attr]

    hb.run_cycle()
    probe_diff = decode_message(transport.sent_packets[-1][0])
    clock.advance(0.03)
    pong_diff = Message(
        type=MessageType.PONG,
        sender_id="node-worker-2",
        message_id=probe_diff.message_id,
        timestamp=clock(),
        payload={"ref_id": probe_diff.message_id},
    )
    discovery.handle_discovery_message(pong_diff, ("127.0.0.1", 9000))
    assert hb.handle_message(pong_diff) == pytest.approx(0.03)
    assert registry.get_peer("node-worker-1") is None
    recovered_new = registry.get_peer("node-worker-2")
    assert recovered_new is not None
    assert recovered_new.status == PeerStatus.ACTIVE
    assert len(registry) == 1


def test_endpoint_deduplication_preserves_unrelated_peers() -> None:
    """Verify deduplicating one endpoint does not remove peers at other endpoints."""
    registry = PeerRegistry()
    discovery = PeerDiscovery(registry)

    registry.register_peer(
        "peer-target-old", "127.0.0.1", 9000, status=PeerStatus.INACTIVE
    )
    registry.register_peer(
        "peer-other-port", "127.0.0.1", 9001, status=PeerStatus.ACTIVE
    )
    registry.register_peer(
        "peer-other-host", "10.0.0.5", 9000, status=PeerStatus.ACTIVE
    )

    msg = Message(
        type=MessageType.PONG,
        sender_id="peer-target-new",
        timestamp=4000.0,
    )
    discovery.handle_discovery_message(msg, ("127.0.0.1", 9000))

    assert registry.get_peer("peer-target-old") is None
    assert registry.get_peer("peer-target-new") is not None
    assert registry.get_peer("peer-other-port") is not None
    assert registry.get_peer("peer-other-host") is not None
    assert len(registry) == 3


def test_bootstrap_peer_recovery_when_unavailable_at_startup() -> None:
    """Verify bootstrap probe discovers a peer that starts after the local node."""

    async def run_test() -> None:
        sub_node = Node(node_id="node-submitter")
        sub_reg = PeerRegistry()
        sub_disc = PeerDiscovery(sub_reg)
        sub_hb: HeartbeatManager | None = None

        worker_node = Node(node_id="node-worker-late")
        worker_reg = PeerRegistry()
        worker_disc = PeerDiscovery(worker_reg)
        worker_hb: HeartbeatManager | None = None

        sub_transport, _ = await start_udp_listener(
            "127.0.0.1",
            0,
            decoder=decode_message,
            message_handler=lambda msg, addr, tr: (
                handle_message(msg, addr, tr, sub_node, sub_disc, sub_hb, sub_reg)
                if sub_hb is not None
                else None
            ),
        )
        # Reserve an ephemeral port, then close it before the worker starts.
        temp_transport, _ = await start_udp_listener("127.0.0.1", 0)
        worker_port = temp_transport.get_extra_info("sockname")[1]
        temp_transport.close()
        await asyncio.sleep(0)

        worker_transport = None
        probe_task = None
        try:
            sub_hb = HeartbeatManager(
                node_id=sub_node.node_id,
                transport=sub_transport,
                peer_registry=sub_reg,
            )
            probe_task = asyncio.create_task(
                probe_bootstrap_peers_loop(
                    sub_node,
                    sub_transport,
                    sub_reg,
                    [("127.0.0.1", worker_port)],
                    interval=0.05,
                )
            )

            # Let at least one bootstrap probe fire while worker is still offline.
            await asyncio.sleep(0.12)
            assert sub_reg.get_peer("node-worker-late") is None

            # Now start the worker node on that bootstrap port.
            worker_transport, _ = await start_udp_listener(
                "127.0.0.1",
                worker_port,
                decoder=decode_message,
                message_handler=lambda msg, addr, tr: (
                    handle_message(
                        msg, addr, tr, worker_node, worker_disc, worker_hb, worker_reg
                    )
                    if worker_hb is not None
                    else None
                ),
            )
            worker_hb = HeartbeatManager(
                node_id=worker_node.node_id,
                transport=worker_transport,
                peer_registry=worker_reg,
            )

            deadline = asyncio.get_running_loop().time() + 2.0
            while asyncio.get_running_loop().time() < deadline:
                discovered = sub_reg.get_peer("node-worker-late")
                if discovered is not None and discovered.status == PeerStatus.ACTIVE:
                    break
                await asyncio.sleep(0.03)

            recovered = sub_reg.get_peer("node-worker-late")
            assert recovered is not None
            assert recovered.status == PeerStatus.ACTIVE
            assert recovered.port == worker_port
        finally:
            if probe_task is not None:
                probe_task.cancel()
                try:
                    await probe_task
                except asyncio.CancelledError:
                    pass
            if worker_transport is not None:
                worker_transport.close()
            sub_transport.close()
            await asyncio.sleep(0)

    asyncio.run(run_test())


def test_heartbeat_matching_and_timeout_isolation() -> None:
    """Verify PONGs match strictly by request ID and endpoint/peer."""
    transport = FakeTransport()
    registry = PeerRegistry()
    clock = FakeClock(5000.0)

    registry.register_peer("node-1", "127.0.0.1", 9001, last_seen=5000.0)
    registry.register_peer("node-2", "127.0.0.1", 9002, last_seen=5000.0)

    hb = HeartbeatManager(
        node_id="node-main",
        transport=transport,
        peer_registry=registry,
        timeout=5.0,
        clock=clock,
    )

    ping_1 = hb.send_heartbeat("node-1")
    ping_2 = hb.send_heartbeat("node-2")

    wrong_sender_pong = Message(
        type=MessageType.PONG,
        sender_id="node-2",
        message_id=ping_1,
        timestamp=clock(),
        payload={"ref_id": ping_1},
    )
    assert hb.handle_message(wrong_sender_pong) is None
    assert ping_1 in hb.pending_requests

    clock.advance(6.0)
    valid_pong_2 = Message(
        type=MessageType.PONG,
        sender_id="node-2",
        message_id=ping_2,
        timestamp=clock(),
        payload={"ref_id": ping_2},
    )
    assert hb.handle_message(valid_pong_2) is not None
    expired = hb.check_timeouts()

    assert expired == ["node-1"]
    assert registry.get_peer("node-1").status == PeerStatus.INACTIVE  # type: ignore[union-attr]
    assert registry.get_peer("node-2").status == PeerStatus.ACTIVE  # type: ignore[union-attr]


def test_udp_error_received_ignores_winerror_10054(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify WinError 10054 does not drop the UDP listener."""
    protocol = MeshWeaverUDPProtocol()
    transport = Mock()
    transport.get_extra_info.return_value = ("127.0.0.1", 9001)
    protocol.connection_made(transport)

    win_err = OSError("An existing connection was forcibly closed by the remote host")
    win_err.winerror = 10054  # type: ignore[attr-defined]

    with caplog.at_level(logging.DEBUG):
        protocol.error_received(win_err)
        protocol.error_received(ConnectionResetError(10054, "Connection reset"))

    assert protocol.transport is transport
    assert protocol.recoverable_errors == 2
    assert "Ignored recoverable UDP socket error" in caplog.text
