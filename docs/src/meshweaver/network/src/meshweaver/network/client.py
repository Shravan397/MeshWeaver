import socket
from typing import Optional


class UDPClient:
    """UDP client for sending and receiving messages."""

    def __init__(
        self,
        server_host: str = "127.0.0.1",
        server_port: int = 5000,
        timeout: Optional[float] = 5.0,
    ):
        self.server_address = (server_host, server_port)
        self.timeout = timeout

        self.sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM
        )

        self.sock.settimeout(timeout)

    def send_message(self, message: str) -> None:
        """Send a message to the UDP server."""
        data = message.encode("utf-8")
        self.sock.sendto(data, self.server_address)

    def receive_message(self, buffer_size: int = 4096) -> str:
        """Receive a message from the server."""
        data, address = self.sock.recvfrom(buffer_size)
        return data.decode("utf-8")

    def close(self) -> None:
        """Close the UDP client socket."""
        self.sock.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


if __name__ == "__main__":
    client = UDPClient("127.0.0.1", 5000)

    try:
        client.send_message("Hello from MeshWeaver!")
        print("Message sent successfully")

    except socket.timeout:
        print("Connection timed out")

    finally:
        client.close()