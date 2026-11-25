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
    registry.register_peer("node-a", "127.0.0.1", 9000, last_seen=time.time() - 100)

    assert registry.purge_inactive_peers(timeout_seconds=10) == []
    assert registry.get_peer("node-a").status is PeerStatus.INACTIVE
    assert registry.purge_inactive_peers() == ["node-a"]
    assert registry.get_peer("node-a") is None


def test_register_stores_resource_metrics():
    registry = PeerRegistry()

    peer = registry.register_peer(
        "node-a",
        "127.0.0.1",
        9000,
        cpu_percent=30.0,
        ram_percent=60.0,
        ram_total_mb=8192.0,
        ram_available_mb=3276.8,
        resource_timestamp="2026-11-25T00:00:00+00:00",
    )

    assert peer.cpu_percent == 30.0
    assert peer.ram_percent == 60.0
    assert peer.ram_total_mb == 8192.0
    assert peer.ram_available_mb == 3276.8
    assert peer.resource_timestamp == "2026-11-25T00:00:00+00:00"


@pytest.mark.parametrize(
    "resource_fields",
    [
        {"ram_percent": 101},
        {"ram_percent": -1},
        {"ram_total_mb": 0},
        {"ram_available_mb": -1},
        {"ram_total_mb": 8192, "ram_available_mb": 9000},
        {"resource_timestamp": ""},
    ],
)
def test_register_rejects_invalid_resource_metrics(resource_fields):
    registry = PeerRegistry()

    with pytest.raises(ValueError):
        registry.register_peer(
            "node-a",
            "127.0.0.1",
            9000,
            **resource_fields,
        )


def test_update_preserves_existing_resource_metrics():
    registry = PeerRegistry()

    registry.register_peer(
        "node-a",
        "127.0.0.1",
        9000,
        cpu_percent=30.0,
        ram_percent=60.0,
        ram_total_mb=8192.0,
        ram_available_mb=3276.8,
        resource_timestamp="2026-11-25T00:00:00+00:00",
    )

    updated = registry.update_peer(
        "node-a",
        ram_percent=65.0,
        resource_timestamp="2026-11-25T00:00:05+00:00",
    )

    assert updated.cpu_percent == 30.0
    assert updated.ram_percent == 65.0
    assert updated.ram_total_mb == 8192.0
    assert updated.ram_available_mb == 3276.8
    assert updated.resource_timestamp == "2026-11-25T00:00:05+00:00"


def test_resource_update_does_not_refresh_last_seen():
    registry = PeerRegistry()
    original_last_seen = 1234.5

    registry.register_peer(
        "node-a",
        "127.0.0.1",
        9000,
        last_seen=original_last_seen,
    )

    updated = registry.update_peer(
        "node-a",
        cpu_percent=40.0,
        ram_percent=55.0,
        ram_total_mb=8192.0,
        ram_available_mb=3686.4,
        resource_timestamp="2026-11-25T00:00:00+00:00",
        refresh_last_seen=False,
    )

    assert updated.last_seen == original_last_seen


def test_resource_update_preserves_peer_status():
    registry = PeerRegistry()

    registry.register_peer(
        "node-a",
        "127.0.0.1",
        9000,
        status=PeerStatus.INACTIVE,
    )

    updated = registry.update_peer(
        "node-a",
        cpu_percent=40.0,
        ram_percent=55.0,
        ram_total_mb=8192.0,
        ram_available_mb=3686.4,
        resource_timestamp="2026-11-25T00:00:00+00:00",
        refresh_last_seen=False,
    )

    assert updated.status is PeerStatus.INACTIVE
