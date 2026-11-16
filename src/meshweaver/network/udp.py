
import asyncio
import logging
from typing import Any, Callable


logger = logging.getLogger(__name__)

Decoder = Callable[[bytes], Any]


class MeshWeaverUDPProtocol(asyncio.DatagramProtocol):
    """Receives and processes incoming MeshWeaver UDP messages."""

    def __init__(self, decoder: Decoder | None = None) -> None:
        self.transport: asyncio.DatagramTransport | None = None
        self.decoder = decoder

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """Called when the UDP endpoint is ready."""
        self.transport = transport  # type: ignore[assignment]
        address = transport.get_extra_info("sockname")
        logger.info("UDP listener ready at %s", address)

    def datagram_received(
        self, data: bytes, addr: tuple[str, int]
    ) -> None:
        """Called whenever a UDP message arrives."""
        print(f"\nMessage received from {addr[0]}:{addr[1]}")

        try:
            if self.decoder is not None:
                message = self.decoder(data)
            else:
                message = data.decode("utf-8")

            print(f"Message: {message}")

        except UnicodeDecodeError:
            logger.warning(
                "Received a message that is not valid UTF-8 from %s",
                addr,
            )

        except Exception:
            logger.exception(
                "Could not process message received from %s", addr
            )

    def error_received(self, exc: Exception) -> None:
        """Called when a UDP network error occurs."""
        logger.error("UDP network error: %s", exc)

    def connection_lost(self, exc: Exception | None) -> None:
        """Called when the UDP endpoint closes."""
        self.transport = None

        if exc:
            logger.error("UDP connection lost: %s", exc)
        else:
            logger.info("UDP listener closed.")


async def start_udp_listener(
    host: str,
    port: int,
    decoder: Decoder | None = None,
) -> tuple[asyncio.DatagramTransport, MeshWeaverUDPProtocol]:
    """Create and start the UDP listening endpoint."""
    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: MeshWeaverUDPProtocol(decoder),
        local_addr=(host, port),
    )

    return transport, protocol
