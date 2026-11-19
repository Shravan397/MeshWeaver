import time

import pytest

from meshweaver.network.peer_registry import PeerRegistry, PeerStatus


def test_register_and_get_peer():
    registry = PeerRegistry()
    peer = registry.register_peer("node-a", "127.0.0.1", 9000)

    assert peer.node_id == "node-a"
    assert peer.status is PeerStatus.ACTIVE
    assert registry.get_peer("node-a").port == 9000


def test_register_rejects_invalid_values():
    registry = PeerRegistry()
    with pytest.raises(ValueError):
        registry.register_peer("", "127.0.0.1", 9000)
    with pytest.raises(ValueError):
        registry.register_peer("node-a", "127.0.0.1", 70000)
    with pytest.raises(ValueError):
        registry.register_peer("node-a", "127.0.0.1", 9000, cpu_percent=101)


def test_update_and_remove_peer():
    registry = PeerRegistry()
    registry.register_peer("node-a", "127.0.0.1", 9000)
    updated = registry.update_peer("node-a", port=9001, cpu_percent=25.0)

    assert updated.port == 9001
    assert updated.cpu_percent == 25.0
    assert registry.remove_peer("node-a") is True
    assert registry.remove_peer("node-a") is False


def test_get_peer_returns_detached_copy():
    registry = PeerRegistry()
    registry.register_peer("node-a", "127.0.0.1", 9000)
    peer = registry.get_peer("node-a")
    peer.host = "malicious-change"

    assert registry.get_peer("node-a").host == "127.0.0.1"


def test_filter_and_dictionary_serialization():
    registry = PeerRegistry()
    registry.register_peer("node-a", "127.0.0.1", 9000)
    registry.register_peer("node-b", "127.0.0.1", 9001, status="inactive")

    active = registry.get_all_peers(status="active")
    dictionaries = registry.get_all_peers(as_dicts=True)

    assert [peer.node_id for peer in active] == ["node-a"]
    assert dictionaries[1]["status"] == "inactive"


def test_stale_peer_is_marked_then_removed_on_next_sweep():
    registry = PeerRegistry()
    registry.register_peer(
        "node-a", "127.0.0.1", 9000, last_seen=time.time() - 100
    )

    assert registry.purge_inactive_peers(timeout_seconds=10) == []
    assert registry.get_peer("node-a").status is PeerStatus.INACTIVE
    assert registry.purge_inactive_peers() == ["node-a"]
    assert registry.get_peer("node-a") is None
