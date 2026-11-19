import socket
from typing import Optional, Tuple


class UDPTransport:
    """Simple UDP transport for sending and receiving network messages."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 5000,
        timeout: Optional[float] = 5.0,
    ):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(timeout)

    def bind(self) -> None:
        """Bind the UDP socket to the local address."""
        self.sock.bind((self.host, self.port))

    def send(self, message: bytes, address: Tuple[str, int]) -> int:
        """Send a UDP message to the specified address."""
        return self.sock.sendto(message, address)

    def receive(self, buffer_size: int = 4096) -> Tuple[bytes, Tuple[str, int]]:
        """Receive a UDP message and return its data and sender address."""
        return self.sock.recvfrom(buffer_size)

    def close(self) -> None:
        """Close the UDP socket."""
        self.sock.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()