
"""Select the best peer for executing a task."""

from collections.abc import Iterable, Mapping
from math import isfinite
from typing import Any


def select_best_peer(
    peers: Iterable[Mapping[str, Any]],
    local_node_id: str | None = None,
) -> Mapping[str, Any] | None:
    """Return the active peer with the lowest valid CPU usage.

    Each peer must provide:
        node_id: Unique peer identifier
        status: Peer status (must be "active")
        cpu_percent: CPU usage between 0 and 100

    The local node is excluded when local_node_id is provided.
    Returns None when no suitable peer is available.
    """
    best_peer = None
    lowest_cpu = float("inf")

    for peer in peers:
        node_id = peer.get("node_id")
        status = peer.get("status")
        cpu = peer.get("cpu_percent")

        if not isinstance(node_id, str) or not node_id:
            continue

        if node_id == local_node_id:
            continue

        if status != "active":
            continue

        if isinstance(cpu, bool) or not isinstance(cpu, (int, float)):
            continue

        if not isfinite(cpu) or not 0 <= cpu <= 100:
            continue

        if cpu < lowest_cpu:
            lowest_cpu = cpu
            best_peer = peer

    return best_peer
