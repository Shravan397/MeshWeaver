"""Async UDP transport for MeshWeaver."""

import asyncio
import logging
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

Decoder = Callable[[bytes], Any]
MessageHandler = Callable[
    [Any, tuple[str, int], asyncio.DatagramTransport],
    None,
]


class MeshWeaverUDPProtocol(asyncio.DatagramProtocol):
    """Handles incoming and outgoing UDP datagrams."""

    def __init__(
        self,
        decoder: Decoder | None = None,
        message_handler: MessageHandler | None = None,
    ) -> None:
        self.transport: asyncio.DatagramTransport | None = None
        self.decoder = decoder
        self.message_handler = message_handler

    def connection_made(
        self,
        transport: asyncio.BaseTransport,
    ) -> None:
        """Called when the UDP connection is established."""
        self.transport = transport  # type: ignore[assignment]

        socket_info = transport.get_extra_info("sockname")
        logger.info("UDP listener started on %s", socket_info)

    def datagram_received(
        self,
        data: bytes,
        addr: tuple[str, int],
    ) -> None:
        """Called whenever a UDP message arrives."""
        logger.debug("UDP datagram received from %s", addr)

        try:
            if self.decoder is not None:
                message = self.decoder(data)
            else:
                message = data.decode("utf-8")

            if self.message_handler is not None and self.transport is not None:
                self.message_handler(message, addr, self.transport)

        except Exception:
            logger.exception("Could not process message received from %s", addr)

    def error_received(self, exc: Exception) -> None:
        """Called when a UDP error occurs."""
        logger.error("UDP error: %s", exc)

    def connection_lost(self, exc: Exception | None) -> None:
        """Called when the UDP connection is closed."""
        self.transport = None

        if exc is not None:
            logger.error("UDP connection lost: %s", exc)
        else:
            logger.info("UDP connection closed")


async def start_udp_listener(
    host: str,
    port: int,
    decoder: Decoder | None = None,
    message_handler: MessageHandler | None = None,
) -> tuple[asyncio.DatagramTransport, MeshWeaverUDPProtocol]:
    """Start an asynchronous UDP listener."""
    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: MeshWeaverUDPProtocol(
            decoder=decoder,
            message_handler=message_handler,
        ),
        local_addr=(host, port),
    )

    return transport, protocol  # type: ignore[return-value]
