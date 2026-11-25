"""Peer discovery using the existing protocol and peer registry."""

from __future__ import annotations

from .peer_registry import PeerRecord, PeerRegistry, PeerStatus
from .protocol import Message, validate_message


class PeerDiscovery:
    """Discover peers and maintain their latest network addresses."""

    def __init__(self, registry: PeerRegistry) -> None:
        if not isinstance(registry, PeerRegistry):
            raise TypeError("registry must be a PeerRegistry instance")

        self.registry = registry

    @staticmethod
    def _validate_address(address: tuple[str, int]) -> tuple[str, int]:
        """Validate and return a UDP source address."""
        if not isinstance(address, tuple) or len(address) != 2:
            raise ValueError("address must be a (host, port) tuple")

        host, port = address

        if not isinstance(host, str) or not host.strip():
            raise ValueError("address host must be a non-empty string")

        if isinstance(port, bool) or not isinstance(port, int):
            raise ValueError("address port must be an integer")

        if not 1 <= port <= 65535:
            raise ValueError("address port must be between 1 and 65535")

        return host, port

    def handle_discovery_message(
        self,
        message: Message,
        address: tuple[str, int],
    ) -> PeerRecord:
        """Validate a message and register or update its sender."""
        if not isinstance(message, Message):
            raise TypeError("message must be a protocol Message instance")

        validate_message(message)
        host, port = self._validate_address(address)

        self.registry.evict_endpoint_duplicates(message.sender_id, host, port)

        existing_peer = self.registry.get_peer(message.sender_id)
        if existing_peer is not None:
            return self.handle_duplicate_peer(message, (host, port))

        return self.registry.register_peer(
            node_id=message.sender_id,
            host=host,
            port=port,
            status=PeerStatus.ACTIVE,
        )

    def handle_duplicate_peer(
        self,
        message: Message,
        address: tuple[str, int],
    ) -> PeerRecord:
        """Update an existing peer instead of creating a duplicate."""
        if not isinstance(message, Message):
            raise TypeError("message must be a protocol Message instance")

        validate_message(message)
        host, port = self._validate_address(address)

        self.registry.evict_endpoint_duplicates(message.sender_id, host, port)

        updated_peer = self.registry.update_peer(
            node_id=message.sender_id,
            host=host,
            port=port,
            status=PeerStatus.ACTIVE,
            refresh_last_seen=True,
        )

        if updated_peer is None:
            return self.registry.register_peer(
                node_id=message.sender_id,
                host=host,
                port=port,
                status=PeerStatus.ACTIVE,
            )

        return updated_peer

    def get_discovered_peers(self) -> list[PeerRecord]:
        """Return all currently registered peers."""
        return self.registry.get_all_peers()
