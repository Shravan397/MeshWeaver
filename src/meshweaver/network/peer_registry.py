"""Thread-safe registry for MeshWeaver network peers."""

from __future__ import annotations

import math
import threading
import time
from dataclasses import asdict, dataclass, replace
from enum import Enum
from typing import Any


class PeerStatus(str, Enum):
    """Connection state of a known peer."""

    ACTIVE = "active"
    INACTIVE = "inactive"


@dataclass
class PeerRecord:
    """Metadata associated with one peer node."""

    node_id: str
    host: str
    port: int
    status: PeerStatus = PeerStatus.ACTIVE
    cpu_percent: float | None = None
    last_seen: float = 0.0

    def __post_init__(self) -> None:
        if isinstance(self.status, str) and not isinstance(self.status, PeerStatus):
            self.status = PeerStatus(self.status.lower())
        if self.last_seen == 0.0:
            self.last_seen = time.time()

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-friendly representation."""
        data = asdict(self)
        data["status"] = self.status.value
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PeerRecord:
        """Build a peer record from a mapping."""
        return cls(
            node_id=data["node_id"],
            host=data["host"],
            port=data["port"],
            status=data.get("status", PeerStatus.ACTIVE),
            cpu_percent=data.get("cpu_percent"),
            last_seen=data.get("last_seen", time.time()),
        )


class PeerRegistry:
    """Thread-safe in-memory registry of known network peers."""

    def __init__(self) -> None:
        self._peers: dict[str, PeerRecord] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _validate_peer(
        node_id: str,
        host: str,
        port: int,
        cpu_percent: float | None,
        last_seen: float | None,
    ) -> None:
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string")
        if not isinstance(host, str) or not host.strip():
            raise ValueError("host must be a non-empty string")
        if (
            not isinstance(port, int)
            or isinstance(port, bool)
            or not 1 <= port <= 65535
        ):
            raise ValueError("port must be an integer between 1 and 65535")
        if cpu_percent is not None and (
            not isinstance(cpu_percent, (int, float))
            or isinstance(cpu_percent, bool)
            or not math.isfinite(cpu_percent)
            or not 0 <= cpu_percent <= 100
        ):
            raise ValueError("cpu_percent must be between 0 and 100")
        if last_seen is not None and (
            not isinstance(last_seen, (int, float))
            or isinstance(last_seen, bool)
            or not math.isfinite(last_seen)
        ):
            raise ValueError("last_seen must be a finite timestamp")

    @staticmethod
    def _copy(peer: PeerRecord) -> PeerRecord:
        return replace(peer)

    def register_peer(
        self,
        node_id: str,
        host: str,
        port: int,
        status: PeerStatus | str = PeerStatus.ACTIVE,
        cpu_percent: float | None = None,
        last_seen: float | None = None,
    ) -> PeerRecord:
        """Register a peer, replacing any existing record with the same ID."""
        self._validate_peer(node_id, host, port, cpu_percent, last_seen)
        try:
            peer_status = PeerStatus(status)
        except ValueError as exc:
            raise ValueError(f"Unsupported peer status: {status!r}") from exc
        record = PeerRecord(
            node_id=node_id,
            host=host,
            port=port,
            status=peer_status,
            cpu_percent=cpu_percent,
            last_seen=time.time() if last_seen is None else float(last_seen),
        )
        with self._lock:
            self._peers[node_id] = record
            return self._copy(record)

    def get_peer(self, node_id: str) -> PeerRecord | None:
        """Return a copy of a peer record, or None when unknown."""
        with self._lock:
            peer = self._peers.get(node_id)
            return self._copy(peer) if peer is not None else None

    def get_all_peers(
        self,
        status: PeerStatus | str | None = None,
        as_dicts: bool = False,
    ) -> list[PeerRecord] | list[dict[str, Any]]:
        """Return copies of all peers, optionally filtered by status."""
        target = PeerStatus(status) if status is not None else None
        with self._lock:
            peers = [
                self._copy(peer)
                for peer in self._peers.values()
                if target is None or peer.status == target
            ]
        return [peer.to_dict() for peer in peers] if as_dicts else peers

    def update_peer(
        self,
        node_id: str,
        host: str | None = None,
        port: int | None = None,
        status: PeerStatus | str | None = None,
        cpu_percent: float | None = None,
        last_seen: float | None = None,
        refresh_last_seen: bool = True,
    ) -> PeerRecord | None:
        """Update an existing peer and return a detached copy."""
        with self._lock:
            current = self._peers.get(node_id)
            if current is None:
                return None
            new_host = current.host if host is None else host
            new_port = current.port if port is None else port
            new_cpu = current.cpu_percent if cpu_percent is None else cpu_percent
            new_last_seen = (
                float(last_seen)
                if last_seen is not None
                else (time.time() if refresh_last_seen else current.last_seen)
            )
            self._validate_peer(node_id, new_host, new_port, new_cpu, new_last_seen)
            try:
                new_status = current.status if status is None else PeerStatus(status)
            except ValueError as exc:
                raise ValueError(f"Unsupported peer status: {status!r}") from exc
            updated = replace(
                current,
                host=new_host,
                port=new_port,
                status=new_status,
                cpu_percent=new_cpu,
                last_seen=new_last_seen,
            )
            self._peers[node_id] = updated
            return self._copy(updated)

    def remove_peer(self, node_id: str) -> bool:
        """Remove a peer; return True only if it existed."""
        with self._lock:
            return self._peers.pop(node_id, None) is not None

    def purge_inactive_peers(self, timeout_seconds: float | None = None) -> list[str]:
        """Remove peers already inactive and mark newly stale peers inactive.

        A peer that first becomes stale is retained as INACTIVE for this sweep.
        It is removed by a subsequent sweep, allowing callers to observe the
        state transition before deletion.
        """
        if timeout_seconds is not None and (
            not isinstance(timeout_seconds, (int, float))
            or isinstance(timeout_seconds, bool)
            or not math.isfinite(timeout_seconds)
            or timeout_seconds < 0
        ):
            raise ValueError("timeout_seconds must be a non-negative finite number")

        now = time.time()
        removed: list[str] = []
        with self._lock:
            for node_id, peer in list(self._peers.items()):
                if peer.status == PeerStatus.INACTIVE:
                    del self._peers[node_id]
                    removed.append(node_id)
                elif (
                    timeout_seconds is not None
                    and now - peer.last_seen > timeout_seconds
                ):
                    peer.status = PeerStatus.INACTIVE
        return removed

    def __len__(self) -> int:
        with self._lock:
            return len(self._peers)
