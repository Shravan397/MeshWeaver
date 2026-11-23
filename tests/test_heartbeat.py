import json

from meshweaver.network.heartbeat import HeartbeatMonitor


class DummyTransport:
    """Captures outgoing UDP packets without opening a network socket."""

    def __init__(self) -> None:
        self.sent_packets: list[tuple[bytes, tuple[str, int]]] = []

    def send(self, message: bytes, address: tuple[str, int]) -> int:
        self.sent_packets.append((message, address))
        return len(message)


class DummyPeerRegistry:
    """Minimal peer registry for unit testing heartbeat state transitions."""

    def __init__(self) -> None:
        self.peers: dict[str, dict] = {
            "node-b": {
                "address": ("127.0.0.1", 5001),
                "status": "unknown",
                "alive": None,
            },
            "node-c": {
                "address": ("127.0.0.1", 5002),
                "status": "unknown",
                "alive": None,
            },
        }

    def get_peer(self, peer_id: str) -> dict:
        return self.peers[peer_id]

    def mark_alive(self, peer_id: str) -> None:
        self.peers[peer_id]["status"] = "alive"
        self.peers[peer_id]["alive"] = True

    def mark_unreachable(self, peer_id: str) -> None:
        self.peers[peer_id]["status"] = "unreachable"
        self.peers[peer_id]["alive"] = False


class FakeClock:
    """Deterministic clock for testing timeout behavior without sleep()."""

    def __init__(self, start: float = 100.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_ping_request_is_recorded() -> None:
    transport = DummyTransport()
    registry = DummyPeerRegistry()
    clock = FakeClock(10.0)
    monitor = HeartbeatMonitor(transport, registry, timeout=5.0, clock=clock)

    msg_id = monitor.send_heartbeat("node-b")

    assert msg_id in monitor.pending_requests
    assert monitor.pending_requests[msg_id].peer_id == "node-b"
    assert monitor.pending_requests[msg_id].sent_at == 10.0
    assert len(transport.sent_packets) == 1

    raw_data, target_addr = transport.sent_packets[0]
    decoded = json.loads(raw_data.decode("utf-8"))
    assert decoded["type"] == "PING"
    assert decoded["message_id"] == msg_id
    assert target_addr == ("127.0.0.1", 5001)


def test_matching_pong_is_accepted_and_clears_pending_request() -> None:
    transport = DummyTransport()
    registry = DummyPeerRegistry()
    clock = FakeClock(10.0)
    monitor = HeartbeatMonitor(transport, registry, timeout=5.0, clock=clock)

    msg_id = monitor.send_heartbeat("node-b")
    clock.advance(0.25)

    rtt = monitor.receive_pong("node-b", msg_id)

    assert rtt == 0.25
    assert monitor.response_times["node-b"] == 0.25
    assert registry.peers["node-b"]["status"] == "alive"
    assert msg_id not in monitor.pending_requests


def test_unknown_and_duplicate_pong_ignored() -> None:
    transport = DummyTransport()
    registry = DummyPeerRegistry()
    clock = FakeClock(10.0)
    monitor = HeartbeatMonitor(transport, registry, timeout=5.0, clock=clock)

    msg_id = monitor.send_heartbeat("node-b")

    assert monitor.receive_pong("node-b", "unknown-id-999") is None
    assert msg_id in monitor.pending_requests

    assert monitor.receive_pong("node-c", msg_id) is None
    assert msg_id in monitor.pending_requests

    assert monitor.receive_pong("node-b", msg_id) is not None
    assert monitor.receive_pong("node-b", msg_id) is None


def test_peer_marked_unreachable_after_timeout() -> None:
    transport = DummyTransport()
    registry = DummyPeerRegistry()
    clock = FakeClock(10.0)
    monitor = HeartbeatMonitor(transport, registry, timeout=5.0, clock=clock)

    msg_id = monitor.send_heartbeat("node-b")

    clock.advance(4.0)
    assert monitor.check_timeout() == []
    assert msg_id in monitor.pending_requests

    clock.advance(2.0)
    unreachable = monitor.check_timeout()

    assert unreachable == ["node-b"]
    assert registry.peers["node-b"]["status"] == "unreachable"
    assert msg_id not in monitor.pending_requests
