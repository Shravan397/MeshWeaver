"""Tests for peer discovery."""

import pytest

from meshweaver.network.discovery import PeerDiscovery
from meshweaver.network.peer_registry import PeerRegistry
from meshweaver.network.protocol import (
    Message,
    MessageType,
    ProtocolValidationError,
)


@pytest.fixture
def discovery():
    """Create a fresh peer discovery instance for each test."""
    registry = PeerRegistry()
    return PeerDiscovery(registry)


def create_message(sender_id: str) -> Message:
    """Create a valid protocol message from a peer."""
    return Message(
        type=MessageType.PING,
        sender_id=sender_id,
    )


def test_registers_new_peer(discovery):
    """A new peer should be added to the registry."""
    message = create_message("peer-1")

    peer = discovery.handle_discovery_message(
        message,
        ("192.168.1.10", 8000),
    )

    assert peer.node_id == "peer-1"
    assert peer.host == "192.168.1.10"
    assert peer.port == 8000
    assert len(discovery.get_discovered_peers()) == 1


def test_updates_existing_peer_last_seen(discovery):
    """An existing peer should have its last_seen refreshed."""
    discovery.registry.register_peer(
        node_id="peer-1",
        host="192.168.1.10",
        port=8000,
        last_seen=1.0,
        cpu_percent=25.0,
    )

    message = create_message("peer-1")

    updated_peer = discovery.handle_discovery_message(
        message,
        ("192.168.1.20", 9000),
    )

    assert updated_peer.host == "192.168.1.20"
    assert updated_peer.port == 9000
    assert updated_peer.last_seen > 1.0
    assert updated_peer.cpu_percent == 25.0
    assert len(discovery.get_discovered_peers()) == 1


def test_duplicate_peer_does_not_create_another_record(discovery):
    """Repeated discovery messages must not create duplicate peers."""
    message = create_message("peer-1")
    address = ("192.168.1.10", 8000)

    discovery.handle_discovery_message(message, address)
    discovery.handle_discovery_message(message, address)
    discovery.handle_discovery_message(message, address)

    peers = discovery.get_discovered_peers()

    assert len(peers) == 1
    assert peers[0].node_id == "peer-1"


def test_handle_duplicate_peer_updates_existing_record(discovery):
    """The duplicate handler should update the existing peer."""
    message = create_message("peer-1")

    discovery.registry.register_peer(
        node_id="peer-1",
        host="192.168.1.10",
        port=8000,
        last_seen=1.0,
    )

    updated_peer = discovery.handle_duplicate_peer(
        message,
        ("192.168.1.30", 7000),
    )

    assert updated_peer.host == "192.168.1.30"
    assert updated_peer.port == 7000
    assert updated_peer.last_seen > 1.0
    assert len(discovery.get_discovered_peers()) == 1


def test_rejects_invalid_protocol_message(discovery):
    """Invalid protocol messages should be rejected."""
    message = create_message("peer-1")

    # Simulate a message that was modified after construction.
    message.sender_id = ""

    with pytest.raises(ProtocolValidationError):
        discovery.handle_discovery_message(
            message,
            ("192.168.1.10", 8000),
        )

    assert len(discovery.get_discovered_peers()) == 0


@pytest.mark.parametrize(
    "address",
    [
        ("", 8000),
        ("192.168.1.10", 0),
        ("192.168.1.10", 65536),
        ("192.168.1.10", "8000"),
        ("192.168.1.10", True),
        ("192.168.1.10", 8000, "extra"),
    ],
)
def test_rejects_invalid_address(discovery, address):
    """Invalid UDP source addresses should be rejected."""
    message = create_message("peer-1")

    with pytest.raises(ValueError):
        discovery.handle_discovery_message(message, address)

    assert len(discovery.get_discovered_peers()) == 0


def test_discovers_multiple_peers(discovery):
    """Multiple different peers should be registered independently."""
    peers_to_discover = [
        ("peer-1", ("192.168.1.10", 8000)),
        ("peer-2", ("192.168.1.20", 8001)),
        ("peer-3", ("192.168.1.30", 8002)),
    ]

    for sender_id, address in peers_to_discover:
        discovery.handle_discovery_message(
            create_message(sender_id),
            address,
        )

    peers = discovery.get_discovered_peers()

    assert len(peers) == 3
    assert {peer.node_id for peer in peers} == {
        "peer-1",
        "peer-2",
        "peer-3",
    }
