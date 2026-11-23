import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple


@dataclass
class PendingHeartbeat:
    """Tracks an in-flight heartbeat PING awaiting a PONG reply."""

    peer_id: strpytest tests/test_heartbeat.py -v
    message_id: str
    sent_at: float


class HeartbeatMonitor:
    """Monitors peer liveness using PING/PONG messages over existing UDP transport."""

    def __init__(
        self,
        transport: Any,
        peer_registry: Any,
        timeout: float = 5.0,
        node_id: str = "local-node",
        ping_encoder: Optional[Callable[[str, str], bytes]] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.transport = transport
        self.peer_registry = peer_registry
        self.timeout = timeout
        self.node_id = node_id
        self.ping_encoder = ping_encoder or self._default_encode_ping
        self.clock = clock

        # Maps message_id -> PendingHeartbeat
        self.pending_requests: Dict[str, PendingHeartbeat] = {}
        # Maps peer_id -> latest round-trip latency in seconds
        self.response_times: Dict[str, float] = {}

    @staticmethod
    def _default_encode_ping(node_id: str, message_id: str) -> bytes:
        payload = {
            "type": "PING",
            "sender_id": node_id,
            "message_id": message_id,
        }
        return json.dumps(payload).encode("utf-8")

    def _get_peer_address(self, peer_id: str) -> Tuple[str, int]:
        """Resolve (host, port) for peer_id from PeerRegistry."""
        if hasattr(self.peer_registry, "get_peer"):
            peer = self.peer_registry.get_peer(peer_id)
        elif hasattr(self.peer_registry, "peers"):
            peer = self.peer_registry.peers.get(peer_id)
        else:
            peer = None

        if peer is None:
            raise KeyError(f"Unknown peer_id: {peer_id}")

        if isinstance(peer, tuple) and len(peer) == 2:
            return peer
        if isinstance(peer, dict):
            if "address" in peer:
                return tuple(peer["address"])
            if "host" in peer and "port" in peer:
                return (peer["host"], int(peer["port"]))
        if hasattr(peer, "address"):
            return tuple(peer.address)
        if hasattr(peer, "host") and hasattr(peer, "port"):
            return (peer.host, int(peer.port))

        raise ValueError(f"Cannot resolve network address for peer: {peer_id}")

    def _set_peer_status(self, peer_id: str, alive: bool) -> None:
        """Update peer liveness state in PeerRegistry."""
        if alive:
            if hasattr(self.peer_registry, "mark_alive"):
                self.peer_registry.mark_alive(peer_id)
                return
            if hasattr(self.peer_registry, "update_status"):
                self.peer_registry.update_status(peer_id, "alive")
                return
        else:
            if hasattr(self.peer_registry, "mark_unreachable"):
                self.peer_registry.mark_unreachable(peer_id)
                return
            if hasattr(self.peer_registry, "update_status"):
                self.peer_registry.update_status(peer_id, "unreachable")
                return

        # Fallback if PeerRegistry stores dict/object entries directly
        peers = getattr(self.peer_registry, "peers", None)
        if isinstance(peers, dict) and peer_id in peers:
            entry = peers[peer_id]
            status = "alive" if alive else "unreachable"
            if isinstance(entry, dict):
                entry["alive"] = alive
                entry["status"] = status
            else:
                if hasattr(entry, "alive"):
                    entry.alive = alive
                if hasattr(entry, "status"):
                    entry.status = status

    def send_heartbeat(self, peer_id: str) -> str:
        """Generate a unique message ID, record send time, and dispatch a PING."""
        address = self._get_peer_address(peer_id)
        message_id = uuid.uuid4().hex
        sent_at = self.clock()

        packet = self.ping_encoder(self.node_id, message_id)
        self.pending_requests[message_id] = PendingHeartbeat(
            peer_id=peer_id,
            message_id=message_id,
            sent_at=sent_at,
        )
        self.transport.send(packet, address)
        return message_id

    def receive_pong(self, peer_id: str, message_id: str) -> Optional[float]:
        """Validate a PONG reply, record latency, mark peer alive, and clear request."""
        pending = self.pending_requests.get(message_id)
        if pending is None or pending.peer_id != peer_id:
            return None

        # Remove completed request immediately so it does not stay in memory
        del self.pending_requests[message_id]

        rtt = max(0.0, self.clock() - pending.sent_at)
        self.response_times[peer_id] = rtt
        self._set_peer_status(peer_id, alive=True)
        return rtt

    def check_timeout(self) -> List[str]:
        """Mark peers with expired heartbeat requests as unreachable and clean up."""
        now = self.clock()
        expired_ids = [
            msg_id
            for msg_id, pending in self.pending_requests.items()
            if (now - pending.sent_at) > self.timeout
        ]

        unreachable_peers: List[str] = []
        for msg_id in expired_ids:
            pending = self.pending_requests.pop(msg_id)
            self._set_peer_status(pending.peer_id, alive=False)
            if pending.peer_id not in unreachable_peers:
                unreachable_peers.append(pending.peer_id)

        return unreachable_peersgit