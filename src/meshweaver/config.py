
"""Configuration and validation for MeshWeaver nodes."""

from __future__ import annotations

import ipaddress
import math
import re
from dataclasses import dataclass, field


def _validate_host(host: str, field_name: str) -> None:
    """Validate an IP address or hostname without performing DNS lookup."""
    if not isinstance(host, str) or not host or host != host.strip():
        raise ValueError(f"{field_name} must be a non-empty valid host")

    try:
        ipaddress.ip_address(host)
        return
    except ValueError:
        pass

    if len(host) > 253:
        raise ValueError(f"{field_name} must be a valid IP address or hostname")

    hostname = host[:-1] if host.endswith(".") else host
    labels = hostname.split(".")

    valid_label = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$")

    if not hostname or any(not valid_label.fullmatch(label) for label in labels):
        raise ValueError(f"{field_name} must be a valid IP address or hostname")


def _validate_port(port: int, field_name: str) -> None:
    """Validate a TCP/UDP port number."""
    if (
        not isinstance(port, int)
        or isinstance(port, bool)
        or not 1 <= port <= 65535
    ):
        raise ValueError(f"{field_name} must be an integer between 1 and 65535")


def _validate_positive_number(value: float, field_name: str) -> None:
    """Validate a positive, finite numeric setting."""
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{field_name} must be a positive finite number")


@dataclass
class NodeConfig:
    """Runtime configuration for a MeshWeaver node.

    Bootstrap peers are represented as (host, port) pairs.
    """

    listen_host: str = "127.0.0.1"
    listen_port: int = 9000
    heartbeat_interval_seconds: float = 5.0
    heartbeat_timeout_seconds: float = 15.0
    bootstrap_peers: list[tuple[str, int]] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Validate all configuration values after initialization."""
        _validate_host(self.listen_host, "listen_host")
        _validate_port(self.listen_port, "listen_port")

        _validate_positive_number(
            self.heartbeat_interval_seconds,
            "heartbeat_interval_seconds",
        )
        _validate_positive_number(
            self.heartbeat_timeout_seconds,
            "heartbeat_timeout_seconds",
        )

        if not isinstance(self.bootstrap_peers, (list, tuple)):
            raise ValueError("bootstrap_peers must be a list of (host, port) pairs")

        for index, peer in enumerate(self.bootstrap_peers):
            if not isinstance(peer, (list, tuple)) or len(peer) != 2:
                raise ValueError(
                    f"bootstrap_peers[{index}] must be a (host, port) pair"
                )

            host, port = peer
            _validate_host(host, f"bootstrap_peers[{index}].host")
            _validate_port(port, f"bootstrap_peers[{index}].port")
