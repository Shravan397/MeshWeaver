### Peer Registry implementation for tracking MeshWeaver network nodes.###

from dataclasses import asdict, dataclass
from enum import Enum
import threading
import time
from typing import Any, Dict, List, Optional


class PeerStatus(str, Enum):
    """Enumeration of valid peer connection states."""

    ACTIVE = "active"
    INACTIVE = "inactive"


@dataclass
class PeerRecord:
    """Data contract representing a single peer node in the MeshWeaver network."""

    node_id: str
    host: str
    port: int
    status: PeerStatus = PeerStatus.ACTIVE
    cpu_percent: Optional[float] = None
    last_seen: float = 0.0

    def __post_init__(self) -> None:
        if isinstance(self.status, str):
            self.status = PeerStatus(self.status.lower())
        if self.last_seen == 0.0:
            self.last_seen = time.time()

    def to_dict(self) -> Dict[str, Any]:
        """Convert the peer record into a serializable dictionary."""
        data = asdict(self)
        data["status"] = self.status.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PeerRecord":
        """Reconstruct a PeerRecord instance from a dictionary."""
        return cls(
            node_id=data["node_id"],
            host=data["host"],
            port=int(data["port"]),
            status=PeerStatus(data.get("status", PeerStatus.ACTIVE)),
            cpu_percent=data.get("cpu_percent"),
            last_seen=float(data.get("last_seen", time.time())),
        )


class PeerRegistry:
    """Thread-safe registry maintaining state and metadata for known network peers."""

    def __init__(self) -> None:
        self._peers: Dict[str, PeerRecord] = {}
        self._lock = threading.RLock()

    def register_peer(
        self,
        node_id: str,
        host: str,
        port: int,
        status: PeerStatus | str = PeerStatus.ACTIVE,
        cpu_percent: Optional[float] = None,
        last_seen: Optional[float] = None,
    ) -> PeerRecord:
        """Register a new peer or overwrite an existing peer entry.

        Args:
            node_id: Unique node identifier.
            host: Peer IP address or hostname.
            port: Peer UDP port.
            status: Initial peer status ('active' or 'inactive').
            cpu_percent: Latest known CPU usage percentage, or None.
            last_seen: Unix timestamp of last contact; defaults to current time.

        Returns:
            The newly registered PeerRecord.

        Raises:
            ValueError: If node_id/host is empty or port is out of valid range.
        """
        if not node_id or not node_id.strip():
            raise ValueError("node_id must be a non-empty string.")
        if not host or not host.strip():
            raise ValueError("host must be a non-empty string.")
        if not (1 <= port <= 65535):
            raise ValueError(f"port must be between 1 and 65535, got {port}.")

        timestamp = last_seen if last_seen is not None else time.time()
        record = PeerRecord(
            node_id=node_id,
            host=host,
            port=port,
            status=PeerStatus(status),
            cpu_percent=cpu_percent,
            last_seen=timestamp,
        )

        with self._lock:
            self._peers[node_id] = record
            return record

    def get_peer(self, node_id: str) -> Optional[PeerRecord]:
        """Retrieve a peer record by its unique node ID.

        Args:
            node_id: Unique identifier of the peer.

        Returns:
            The matching PeerRecord, or None if not found.
        """
        with self._lock:
            return self._peers.get(node_id)

    def get_all_peers(
        self,
        status: Optional[PeerStatus | str] = None,
        as_dicts: bool = False,
    ) -> List[PeerRecord | Dict[str, Any]]:
        """Retrieve all known peers, optionally filtered by status.

        Args:
            status: Optional status filter ('active' or 'inactive').
            as_dicts: If True, returns dictionary representations instead of PeerRecords.

        Returns:
            A list of PeerRecord objects or dictionaries.
        """
        target_status = PeerStatus(status) if status is not None else None

        with self._lock:
            peers = list(self._peers.values())
            if target_status is not None:
                peers = [p for p in peers if p.status == target_status]
            return [p.to_dict() if as_dicts else p for p in peers]

    def update_peer(
        self,
        node_id: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        status: Optional[PeerStatus | str] = None,
        cpu_percent: Optional[float] = None,
        last_seen: Optional[float] = None,
        refresh_last_seen: bool = True,
    ) -> Optional[PeerRecord]:
        """Update mutable fields on an existing peer record.

        Args:
            node_id: Unique identifier of the peer to update.
            host: Updated IP address or hostname.
            port: Updated UDP port.
            status: Updated status ('active' or 'inactive').
            cpu_percent: Updated CPU utilization percentage.
            last_seen: Explicit Unix timestamp to set.
            refresh_last_seen: If True and last_seen is None, updates last_seen to now.

        Returns:
            The updated PeerRecord, or None if the node_id does not exist.
        """
        with self._lock:
            peer = self._peers.get(node_id)
            if peer is None:
                return None

            if host is not None:
                if not host.strip():
                    raise ValueError("host must be a non-empty string.")
                peer.host = host

            if port is not None:
                if not (1 <= port <= 65535):
                    raise ValueError(f"port must be between 1 and 65535, got {port}.")
                peer.port = port

            if status is not None:
                peer.status = PeerStatus(status)

            if cpu_percent is not None:
                peer.cpu_percent = cpu_percent

            if last_seen is not None:
                peer.last_seen = last_seen
            elif refresh_last_seen:
                peer.last_seen = time.time()

            return peer

    def remove_peer(self, node_id: str) -> bool:
        """Remove a peer from the registry.

        Args:
            node_id: Unique identifier of the peer to remove.

        Returns:
            True if the peer was removed, False if the node_id was not found.
        """
        with self._lock:
            if node_id in self._peers:
                del self._peers[node_id]
                return True
            return False

    def purge_inactive_peers(self, timeout_seconds: Optional[float] = None) -> List[str]:
        """Mark stale peers inactive or remove peers already marked inactive.

        Args:
            timeout_seconds: If provided, marks any active peer whose last_seen
                exceeds this age as INACTIVE before removing INACTIVE peers.

        Returns:
            List of removed node_ids.
        """
        now = time.time()
        removed_ids: List[str] = []

        with self._lock:
            for node_id, peer in list(self._peers.items()):
                if (
                    timeout_seconds is not None
                    and peer.status == PeerStatus.ACTIVE
                    and (now - peer.last_seen) > timeout_seconds
                ):
                    peer.status = PeerStatus.INACTIVE

                if peer.status == PeerStatus.INACTIVE:
                    del self._peers[node_id]
                    removed_ids.append(node_id)

        return removed_ids

    def __len__(self) -> int:
        with self._lock:
            return len(self._peers)


# Default module-level registry instance for functional imports
_default_registry = PeerRegistry()


def register_peer(
    node_id: str,
    host: str,
    port: int,
    status: PeerStatus | str = PeerStatus.ACTIVE,
    cpu_percent: Optional[float] = None,
    last_seen: Optional[float] = None,
) -> PeerRecord:
    """Register a peer in the default registry."""
    return _default_registry.register_peer(
        node_id=node_id,
        host=host,
        port=port,
        status=status,
        cpu_percent=cpu_percent,
        last_seen=last_seen,
    )


def get_peer(node_id: str) -> Optional[PeerRecord]:
    """Retrieve a peer by node_id from the default registry."""
    return _default_registry.get_peer(node_id)


def get_all_peers(
    status: Optional[PeerStatus | str] = None,
    as_dicts: bool = False,
) -> List[PeerRecord | Dict[str, Any]]:
    """Retrieve all peers from the default registry."""
    return _default_registry.get_all_peers(status=status, as_dicts=as_dicts)


def update_peer(
    node_id: str,
    host: Optional[str] = None,
    port: Optional[int] = None,
    status: Optional[PeerStatus | str] = None,
    cpu_percent: Optional[float] = None,
    last_seen: Optional[float] = None,
    refresh_last_seen: bool = True,
) -> Optional[PeerRecord]:
    """Update an existing peer in the default registry."""
    return _default_registry.update_peer(
        node_id=node_id,
        host=host,
        port=port,
        status=status,
        cpu_percent=cpu_percent,
        last_seen=last_seen,
        refresh_last_seen=refresh_last_seen,
    )


def remove_peer(node_id: str) -> bool:
    """Remove a peer by node_id from the default registry."""
    return _default_registry.remove_peer(node_id)
