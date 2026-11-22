import asyncio
import socket
from typing import Optional, Tuple


class UDPClient:
    """Synchronous UDP client for CLI tools and basic testing."""

    def __init__(
        self,
        server_host: str = "127.0.0.1",
        server_port: int = 5000,
        timeout: Optional[float] = 5.0,
        broadcast: bool = False,
    ):
        self.server_address = (server_host, server_port)
        self.timeout = timeout
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if broadcast:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.settimeout(timeout)

    def send_message(
        self, message: str, target: Optional[Tuple[str, int]] = None
    ) -> None:
        """Send a UTF-8 message to the default server or a specific peer."""
        destination = target or self.server_address
        self.sock.sendto(message.encode("utf-8"), destination)

    def receive_message(self, buffer_size: int = 4096) -> Tuple[str, Tuple[str, int]]:
        """Receive a message and return (decoded_text, sender_address)."""
        data, address = self.sock.recvfrom(buffer_size)
        return data.decode("utf-8"), address

    def close(self) -> None:
        self.sock.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class _AsyncUDPProtocol(asyncio.DatagramProtocol):
    """Internal asyncio protocol to buffer incoming datagrams."""

    def __init__(self) -> None:
        self.transport: Optional[asyncio.DatagramTransport] = None
        self.inbox: asyncio.Queue[Tuple[str, Tuple[str, int]]] = asyncio.Queue()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        self.inbox.put_nowait((data.decode("utf-8"), addr))


class AsyncUDPClient:
    """Non-blocking UDP client for MeshWeaver P2P async operations."""

    def __init__(self, bind_host: str = "0.0.0.0", bind_port: int = 0) -> None:
        self.bind_address = (bind_host, bind_port)
        self._protocol: Optional[_AsyncUDPProtocol] = None
        self._transport: Optional[asyncio.DatagramTransport] = None

    async def connect(self, broadcast: bool = False) -> None:
        loop = asyncio.get_running_loop()
        transport, protocol = await loop.create_datagram_endpoint(
            _AsyncUDPProtocol,
            local_addr=self.bind_address,
            allow_broadcast=broadcast,
        )
        self._transport = transport
        self._protocol = protocol

    async def send_message(self, message: str, target: Tuple[str, int]) -> None:
        if not self._transport:
            raise RuntimeError("Client not connected. Call 'await connect()' first.")
        self._transport.sendto(message.encode("utf-8"), target)

    async def receive_message(
        self, timeout: Optional[float] = 5.0
    ) -> Tuple[str, Tuple[str, int]]:
        if not self._protocol:
            raise RuntimeError("Client not connected. Call 'await connect()' first.")
        return await asyncio.wait_for(self._protocol.inbox.get(), timeout=timeout)

    def close(self) -> None:
        if self._transport:
            self._transport.close()

    async def __aenter__(self) -> "AsyncUDPClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        self.close()