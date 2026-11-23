"""Unit tests for HeartbeatManager using PeerRegistry and protocol messages."""

from __future__ import annotations

import time

import pytest

from meshweaver.network.heartbeat import HeartbeatManager
from meshweaver.network.peer_registry import PeerRegistry, PeerStatus
from meshweaver.network.protocol import Message, MessageType, decode_message


class FakeTransport:
    """In-memory UDP transport stand-in that records sent packets."""

    def __init__(self) -> None:
        self.sent_packets: list[tuple[bytes, tuple[str, int]]] = []

    def send(self, message: bytes, address: tuple[str, int]) -> int:
        self.sent_packets.append((message, address))
        return len(message)


class FakeClock:
    """Deterministic clock for testing timestamps and timeouts."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_send_heartbeat_records_pending_and_encodes_protocol_ping() -> None:
    transport = FakeTransport()
    registry = PeerRegistry()
    clock = FakeClock(1000.0)
    registry.register_peer("node-b", "127.0.0.1", 5001, last_seen=1000.0)

    manager = HeartbeatManager(
        node_id="node-a",
        transport=transport,  # type: ignore[arg-type]
        peer_registry=registry,
        timeout=5.0,
        clock=clock,
    )

    msg_id = manager.send_heartbeat("node-b")

    assert msg_id in manager.pending_requests
    assert manager.pending_requests[msg_id].peer_id == "node-b"
    assert manager.pending_requests[msg_id].sent_at == 1000.0
    assert len(transport.sent_packets) == 1

    raw_bytes, target_addr = transport.sent_packets[0]
    decoded = decode_message(raw_bytes)
    assert decoded.type == MessageType.PING
    assert decoded.sender_id == "node-a"
    assert decoded.message_id == msg_id
    assert target_addr == ("127.0.0.1", 5001)


def test_receive_pong_updates_last_seen_and_clears_pending() -> None:
    transport = FakeTransport()
    registry = PeerRegistry()
    clock = FakeClock(1000.0)
    registry.register_peer(
        "node-b",
        "127.0.0.1",
        5001,
        status=PeerStatus.INACTIVE,
        last_seen=990.0,
    )

    manager = HeartbeatManager(
        node_id="node-a",
        transport=transport,  # type: ignore[arg-type]
        peer_registry=registry,
        timeout=5.0,
        clock=clock,
    )

    msg_id = manager.send_heartbeat("node-b")
    clock.advance(0.4)

    pong = Message(
        type=MessageType.PONG,
        sender_id="node-b",
        message_id=msg_id,
        timestamp=clock(),
    )
    rtt = manager.handle_message(pong)

    assert rtt == pytest.approx(0.4)
    assert msg_id not in manager.pending_requests
    assert manager.response_times["node-b"] == pytest.approx(0.4)

    peer = registry.get_peer("node-b")
    assert peer is not None
    assert peer.status == PeerStatus.ACTIVE
    assert peer.last_seen == 1000.4


def test_unmatched_and_duplicate_pong_ignored() -> None:
    transport = FakeTransport()
    registry = PeerRegistry()
    clock = FakeClock(1000.0)
    registry.register_peer("node-b", "127.0.0.1", 5001, last_seen=1000.0)
    registry.register_peer("node-c", "127.0.0.1", 5002, last_seen=1000.0)

    manager = HeartbeatManager(
        node_id="node-a",
        transport=transport,  # type: ignore[arg-type]
        peer_registry=registry,
        timeout=5.0,
        clock=clock,
    )

    msg_id = manager.send_heartbeat("node-b")

    assert manager.receive_pong("node-b", "unknown-message-id") is None
    assert manager.receive_pong("node-c", msg_id) is None
    assert msg_id in manager.pending_requests

    assert manager.receive_pong("node-b", msg_id) is not None
    assert manager.receive_pong("node-b", msg_id) is None


def test_check_timeouts_marks_peer_inactive_and_cleans_up() -> None:
    transport = FakeTransport()
    registry = PeerRegistry()
    clock = FakeClock(1000.0)
    registry.register_peer("node-b", "127.0.0.1", 5001, last_seen=1000.0)

    manager = HeartbeatManager(
        node_id="node-a",
        transport=transport,  # type: ignore[arg-type]
        peer_registry=registry,
        timeout=5.0,
        clock=clock,
    )

    msg_id = manager.send_heartbeat("node-b")

    clock.advance(4.0)
    assert manager.check_timeouts() == []
    assert msg_id in manager.pending_requests

    clock.advance(2.0)
    expired_peers = manager.check_timeouts()

    assert expired_peers == ["node-b"]
    assert msg_id not in manager.pending_requests

    peer = registry.get_peer("node-b")
    assert peer is not None
    assert peer.status == PeerStatus.INACTIVE


def test_start_and_stop_background_loop() -> None:
    transport = FakeTransport()
    registry = PeerRegistry()
    registry.register_peer("node-b", "127.0.0.1", 5001)

    manager = HeartbeatManager(
        node_id="node-a",
        transport=transport,  # type: ignore[arg-type]
        peer_registry=registry,
        interval=0.05,
        timeout=1.0,
    )

    manager.start()
    assert manager.is_running is True

    deadline = time.time() + 1.0
    while len(transport.sent_packets) == 0 and time.time() < deadline:
        time.sleep(0.01)

    manager.stop()
    assert manager.is_running is False
    assert len(transport.sent_packets) >= 1
