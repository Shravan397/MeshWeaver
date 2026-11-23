"""Heartbeat management for MeshWeaver P2P nodes."""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable
import asyncio
from dataclasses import dataclass

from meshweaver.network.peer_registry import PeerRegistry, PeerStatus
from meshweaver.network.protocol import Message, MessageType, encode_message


@dataclass
class PendingHeartbeat:
    """Tracks an in-flight heartbeat PING awaiting a PONG reply."""

    peer_id: str
    message_id: str
    sent_at: float


class HeartbeatManager:
    """Continuously monitors registered peers via PING/PONG heartbeats."""

    def __init__(
        self,
        node_id: str,
        transport: asyncio.DatagramTransport,
        peer_registry: PeerRegistry,
        interval: float = 2.0,
        timeout: float = 5.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string")
        if interval <= 0:
            raise ValueError("interval must be greater than 0")
        if timeout <= 0:
            raise ValueError("timeout must be greater than 0")

        self.node_id = node_id
        self.transport = transport
        self.peer_registry = peer_registry
        self.interval = interval
        self.timeout = timeout
        self.clock = clock

        self._pending: dict[str, PendingHeartbeat] = {}
        self._response_times: dict[str, float] = {}
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def is_running(self) -> bool:
        """Return True while the background heartbeat loop is active."""
        return self._thread is not None and self._thread.is_alive()

    @property
    def pending_requests(self) -> dict[str, PendingHeartbeat]:
        """Return a snapshot of currently pending heartbeat requests."""
        with self._lock:
            return dict(self._pending)

    @property
    def response_times(self) -> dict[str, float]:
        """Return a snapshot of recorded peer round-trip times in seconds."""
        with self._lock:
            return dict(self._response_times)

    def send_heartbeat(self, peer_id: str) -> str:
        """Send a PING message to a registered peer and track the pending request."""
        peer = self.peer_registry.get_peer(peer_id)
        if peer is None:
            raise KeyError(f"Unknown peer_id: {peer_id}")

        message_id = str(uuid.uuid4())
        sent_at = self.clock()
        ping_msg = Message(
            type=MessageType.PING,
            sender_id=self.node_id,
            message_id=message_id,
            timestamp=sent_at,
            payload={"target_id": peer_id},
        )
        encoded = encode_message(ping_msg)

        with self._lock:
            self._pending[message_id] = PendingHeartbeat(
                peer_id=peer_id,
                message_id=message_id,
                sent_at=sent_at,
            )

        if hasattr(self.transport, "sendto"): self.transport.sendto(encoded, (peer.host, peer.port))`n        else: self.transport.send(encoded, (peer.host, peer.port))
        return message_id

    def build_pong(self, ping_message: Message) -> Message:
        """Create a PONG reply matching an incoming PING message."""
        return Message(
            type=MessageType.PONG,
            sender_id=self.node_id,
            message_id=ping_message.message_id,
            timestamp=self.clock(),
            payload={"ref_id": ping_message.message_id},
        )

    def receive_pong(self, peer_id: str, message_id: str) -> float | None:
        """Process a PONG reply, update peer last_seen, and clear pending state."""
        with self._lock:
            pending = self._pending.get(message_id)
            if pending is None or pending.peer_id != peer_id:
                return None
            del self._pending[message_id]

            now = self.clock()
            rtt = max(0.0, now - pending.sent_at)
            self._response_times[peer_id] = rtt

        self.peer_registry.update_peer(
            peer_id,
            status=PeerStatus.ACTIVE,
            last_seen=now,
            refresh_last_seen=False,
        )
        return rtt

    def handle_message(self, message: Message) -> float | None:
        """Handle an incoming protocol Message if it is a PONG reply."""
        if message.type != MessageType.PONG:
            return None
        ref_id = message.payload.get("ref_id") or message.payload.get("message_id")
        target_msg_id = ref_id if isinstance(ref_id, str) else message.message_id
        return self.receive_pong(message.sender_id, target_msg_id)

    def check_timeouts(self) -> list[str]:
        """Mark peers with expired heartbeats INACTIVE and purge stale requests."""
        now = self.clock()
        expired: list[PendingHeartbeat] = []

        with self._lock:
            expired_ids = [
                msg_id
                for msg_id, pending in self._pending.items()
                if (now - pending.sent_at) > self.timeout
            ]
            for msg_id in expired_ids:
                expired.append(self._pending.pop(msg_id))

        timed_out_peers: list[str] = []
        for pending in expired:
            self.peer_registry.update_peer(
                pending.peer_id,
                status=PeerStatus.INACTIVE,
                refresh_last_seen=False,
            )
            if pending.peer_id not in timed_out_peers:
                timed_out_peers.append(pending.peer_id)

        return timed_out_peers

    def run_cycle(self) -> None:
        """Execute one timeout check and send heartbeats to active peers."""
        self.check_timeouts()
        for peer in self.peer_registry.get_all_peers(status=PeerStatus.ACTIVE):
            if peer.node_id == self.node_id:
                continue
            self.send_heartbeat(peer.node_id)

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            self.run_cycle()
            self._stop_event.wait(self.interval)

    def start(self) -> None:
        """Start the background heartbeat monitoring thread."""
        with self._lock:
            if self.is_running:
                return
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._loop,
                name=f"heartbeat-{self.node_id}",
                daemon=True,
            )
            self._thread.start()

    def stop(self, join_timeout: float = 2.0) -> None:
        """Stop the background heartbeat monitoring thread cleanly."""
        with self._lock:
            thread = self._thread
            if thread is None:
                return
            self._stop_event.set()

        thread.join(timeout=join_timeout)
        with self._lock:
            if not thread.is_alive():
                self._thread = None
