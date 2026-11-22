import math

from meshweaver.tasks.scheduler import select_best_peer


def test_selects_peer_with_lowest_cpu():
    peers = [
        {"node_id": "node-1", "status": "active", "cpu_percent": 70},
        {"node_id": "node-2", "status": "active", "cpu_percent": 20},
        {"node_id": "node-3", "status": "active", "cpu_percent": 45},
    ]

    result = select_best_peer(peers)

    assert result["node_id"] == "node-2"


def test_ignores_inactive_peers():
    peers = [
        {"node_id": "node-1", "status": "inactive", "cpu_percent": 5},
        {"node_id": "node-2", "status": "active", "cpu_percent": 30},
    ]

    result = select_best_peer(peers)

    assert result["node_id"] == "node-2"


def test_excludes_local_node():
    peers = [
        {"node_id": "local", "status": "active", "cpu_percent": 5},
        {"node_id": "remote", "status": "active", "cpu_percent": 30},
    ]

    result = select_best_peer(peers, local_node_id="local")

    assert result["node_id"] == "remote"


def test_returns_none_when_no_valid_peer_exists():
    peers = [
        {"node_id": "node-1", "status": "inactive", "cpu_percent": 10},
        {"node_id": "node-2", "status": "active", "cpu_percent": None},
    ]

    assert select_best_peer(peers) is None


def test_rejects_invalid_cpu_values():
    peers = [
        {"node_id": "node-1", "status": "active", "cpu_percent": -5},
        {"node_id": "node-2", "status": "active", "cpu_percent": 101},
        {"node_id": "node-3", "status": "active", "cpu_percent": math.nan},
        {"node_id": "node-4", "status": "active", "cpu_percent": True},
        {"node_id": "node-5", "status": "active", "cpu_percent": 25},
    ]

    result = select_best_peer(peers)

    assert result["node_id"] == "node-5"


def test_returns_none_for_empty_peer_list():
    assert select_best_peer([]) is None
