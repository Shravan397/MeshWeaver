
"""Tests for the MeshWeaver asynchronous UDP transport."""

import asyncio
import logging
from unittest.mock import Mock

from meshweaver.network.udp import (
    MeshWeaverUDPProtocol,
    start_udp_listener,
)


def test_connection_made_stores_transport() -> None:
    protocol = MeshWeaverUDPProtocol()
    transport = Mock()
    transport.get_extra_info.return_value = ("127.0.0.1", 5000)

    protocol.connection_made(transport)

    assert protocol.transport is transport
    transport.get_extra_info.assert_called_once_with("sockname")


def test_datagram_received_decodes_utf8_and_calls_handler() -> None:
    handler = Mock()
    protocol = MeshWeaverUDPProtocol(message_handler=handler)

    transport = Mock()
    protocol.connection_made(transport)

    address = ("127.0.0.1", 5001)
    protocol.datagram_received(b"Hello MeshWeaver", address)

    handler.assert_called_once_with(
        "Hello MeshWeaver",
        address,
        transport,
    )


def test_datagram_received_uses_custom_decoder() -> None:
    decoder = Mock(return_value={"message": "hello"})
    handler = Mock()
    protocol = MeshWeaverUDPProtocol(
        decoder=decoder,
        message_handler=handler,
    )

    transport = Mock()
    protocol.connection_made(transport)

    address = ("127.0.0.1", 5002)
    protocol.datagram_received(b'{"message": "hello"}', address)

    decoder.assert_called_once_with(b'{"message": "hello"}')
    handler.assert_called_once_with(
        {"message": "hello"},
        address,
        transport,
    )


def test_datagram_received_without_handler(caplog) -> None:
    protocol = MeshWeaverUDPProtocol()

    with caplog.at_level(logging.DEBUG):
        protocol.datagram_received(
            b"Hello",
            ("127.0.0.1", 5003),
        )

    assert "UDP datagram received" in caplog.text


def test_invalid_utf8_is_logged(caplog) -> None:
    protocol = MeshWeaverUDPProtocol()

    with caplog.at_level(logging.ERROR):
        protocol.datagram_received(
            b"\xff\xfe",
            ("127.0.0.1", 5004),
        )

    assert "Could not process message" in caplog.text


def test_handler_exception_is_logged(caplog) -> None:
    def failing_handler(message, address, transport):
        raise RuntimeError("Handler failed")

    protocol = MeshWeaverUDPProtocol(message_handler=failing_handler)

    transport = Mock()
    protocol.connection_made(transport)

    with caplog.at_level(logging.ERROR):
        protocol.datagram_received(
            b"Hello",
            ("127.0.0.1", 5005),
        )

    assert "Could not process message" in caplog.text
    assert "Handler failed" in caplog.text


def test_connection_lost_clears_transport() -> None:
    protocol = MeshWeaverUDPProtocol()

    transport = Mock()
    protocol.connection_made(transport)

    protocol.connection_lost(None)

    assert protocol.transport is None


def test_error_received_logs_error(caplog) -> None:
    protocol = MeshWeaverUDPProtocol()

    with caplog.at_level(logging.ERROR):
        protocol.error_received(RuntimeError("UDP test error"))

    assert "UDP error" in caplog.text


def test_start_udp_listener() -> None:
    async def run_test() -> None:
        transport, protocol = await start_udp_listener(
            host="127.0.0.1",
            port=0,
        )

        try:
            assert isinstance(protocol, MeshWeaverUDPProtocol)
            assert protocol.transport is transport
            assert transport.get_extra_info("sockname") is not None
        finally:
            transport.close()
            await asyncio.sleep(0)

        assert protocol.transport is None

    asyncio.run(run_test())


def test_udp_listener_receives_real_datagram() -> None:
    async def run_test() -> None:
        received_messages = []
        received = asyncio.Event()

        def handler(message, address, transport):
            received_messages.append((message, address))
            received.set()

        listener_transport, protocol = await start_udp_listener(
            host="127.0.0.1",
            port=0,
            message_handler=handler,
        )

        loop = asyncio.get_running_loop()
        sender_transport = None

        try:
            sender_transport, _ = await loop.create_datagram_endpoint(
                asyncio.DatagramProtocol,
                local_addr=("127.0.0.1", 0),
            )

            listener_address = listener_transport.get_extra_info("sockname")

            sender_transport.sendto(
                b"Real UDP integration test",
                listener_address,
            )

            await asyncio.wait_for(received.wait(), timeout=2)

            assert len(received_messages) == 1
            assert received_messages[0][0] == "Real UDP integration test"
            assert received_messages[0][1][0] == "127.0.0.1"

        finally:
            if sender_transport is not None:
                sender_transport.close()

            listener_transport.close()
            await asyncio.sleep(0)

        assert protocol.transport is None

    asyncio.run(run_test())
