"""Tests for peer recovery, endpoint deduplication, heartbeat, and UDP errors."""

from __future__ import annotations

import logging
from unittest.mock import Mock

import pytest

from meshweaver.network.discovery import PeerDiscovery
from meshweaver.network.heartbeat import HeartbeatManager
from meshweaver.network.peer_registry import PeerRegistry, PeerStatus
from meshweaver.network.protocol import Message, MessageType, decode_message
from meshweaver.network.udp import MeshWeaverUDPProtocol


class FakeTransport:
    """In-memory UDP transport stand-in."""

    def __init__(self) -> None:
        self.sent_packets: list[tuple[bytes, tuple[str, int]]] = []

    def send(self, data: bytes, address: tuple[str, int]) -> int:
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


def test_peer_failure_detection_and_rediscovery_after_restart() -> None:
    """Verify a timed-out peer is marked INACTIVE, probed again, and recovers."""
    transport = FakeTransport()
    registry = PeerRegistry()
    discovery = PeerDiscovery(registry)
    clock = FakeClock(3000.0)

    registry.register_peer("node-worker", "127.0.0.1", 9000, last_seen=3000.0)
    hb = HeartbeatManager(
        node_id="node-submitter",
        transport=transport,
        peer_registry=registry,
        interval=2.0,
        timeout=5.0,
        clock=clock,
    )

    hb.run_cycle()
    assert len(transport.sent_packets) == 1

    clock.advance(6.0)
    timed_out = hb.check_timeouts()
    assert timed_out == ["node-worker"]

    peer = registry.get_peer("node-worker")
    assert peer is not None
    assert peer.status == PeerStatus.INACTIVE

    hb.run_cycle()
    assert len(transport.sent_packets) == 2
    probe_ping = decode_message(transport.sent_packets[-1][0])

    clock.advance(0.1)
    pong = Message(
        type=MessageType.PONG,
        sender_id="node-worker",
        message_id=probe_ping.message_id,
        timestamp=clock(),
        payload={"ref_id": probe_ping.message_id},
    )
    discovery.handle_discovery_message(pong, ("127.0.0.1", 9000))
    rtt = hb.handle_message(pong)

    assert rtt == pytest.approx(0.1)
    recovered = registry.get_peer("node-worker")
    assert recovered is not None
    assert recovered.status == PeerStatus.ACTIVE


def test_new_node_id_at_existing_endpoint_replaces_stale_peer() -> None:
    """Verify restarting with a new node_id on the same port deduplicates."""
    transport = FakeTransport()
    registry = PeerRegistry()
    discovery = PeerDiscovery(registry)
    clock = FakeClock(4000.0)

    registry.register_peer(
        "node-old",
        "127.0.0.1",
        9000,
        status=PeerStatus.INACTIVE,
        last_seen=3990.0,
    )
    hb = HeartbeatManager(
        node_id="node-submitter",
        transport=transport,
        peer_registry=registry,
        clock=clock,
    )

    ping_id = hb.send_heartbeat("node-old")
    clock.advance(0.02)

    pong = Message(
        type=MessageType.PONG,
        sender_id="node-new",
        message_id=ping_id,
        timestamp=clock(),
        payload={"ref_id": ping_id},
    )
    discovery.handle_discovery_message(pong, ("127.0.0.1", 9000))
    rtt = hb.handle_message(pong)

    assert rtt == pytest.approx(0.02)
    assert registry.get_peer("node-old") is None
    assert len(registry) == 1

    new_peer = registry.get_peer("node-new")
    assert new_peer is not None
    assert new_peer.host == "127.0.0.1"
    assert new_peer.port == 9000
    assert new_peer.status == PeerStatus.ACTIVE


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
