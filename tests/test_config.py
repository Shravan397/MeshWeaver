

import pytest

from meshweaver.config import NodeConfig


def test_valid_config_is_accepted() -> None:
    config = NodeConfig(
        listen_host="127.0.0.1",
        listen_port=9000,
        heartbeat_interval_seconds=5.0,
        heartbeat_timeout_seconds=15.0,
        bootstrap_peers=[("127.0.0.1", 9001)],
    )

    assert config.listen_host == "127.0.0.1"
    assert config.listen_port == 9000
    assert config.heartbeat_interval_seconds == 5.0
    assert config.heartbeat_timeout_seconds == 15.0
    assert config.bootstrap_peers == [("127.0.0.1", 9001)]


def test_default_config_is_accepted() -> None:
    config = NodeConfig()

    assert config.listen_host == "127.0.0.1"
    assert config.listen_port == 9000
    assert config.bootstrap_peers == []


@pytest.mark.parametrize("port", [0, -1, 65536, True, 9000.5])
def test_invalid_listen_port_is_rejected(port: int) -> None:
    with pytest.raises(ValueError, match="listen_port"):
        NodeConfig(listen_port=port)


@pytest.mark.parametrize("interval", [0, -1, float("nan"), float("inf"), True])
def test_invalid_heartbeat_interval_is_rejected(interval: float) -> None:
    with pytest.raises(ValueError, match="heartbeat_interval_seconds"):
        NodeConfig(heartbeat_interval_seconds=interval)


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("-inf"), True])
def test_invalid_heartbeat_timeout_is_rejected(timeout: float) -> None:
    with pytest.raises(ValueError, match="heartbeat_timeout_seconds"):
        NodeConfig(heartbeat_timeout_seconds=timeout)


def test_empty_bootstrap_peer_list_is_supported() -> None:
    config = NodeConfig(bootstrap_peers=[])

    assert config.bootstrap_peers == []


@pytest.mark.parametrize(
    "peer",
    [
        ("", 9001),
        ("not a valid host", 9001),
        ("127.0.0.1", 0),
        ("127.0.0.1", 65536),
        ("127.0.0.1", True),
        ("127.0.0.1", 9001.5),
        ("127.0.0.1",),
    ],
)
def test_invalid_bootstrap_peer_is_rejected(peer: tuple) -> None:
    with pytest.raises(ValueError, match="bootstrap_peers"):
        NodeConfig(bootstrap_peers=[peer])


def test_invalid_listen_host_is_rejected() -> None:
    with pytest.raises(ValueError, match="listen_host"):
        NodeConfig(listen_host="invalid host name")


def test_ipv6_address_is_accepted() -> None:
    config = NodeConfig(
        listen_host="::1",
        bootstrap_peers=[("2001:db8::1", 9001)],
    )

    assert config.listen_host == "::1"
    assert config.bootstrap_peers == [("2001:db8::1", 9001)]
