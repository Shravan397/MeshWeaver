import argparse
import asyncio
import json
import logging

from .network.protocol import Message, decode_message, encode_message
from .network.udp import start_udp_listener
from .node import Node

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)


def handle_message(
    message: Message,
    addr: tuple[str, int],
    transport: asyncio.DatagramTransport,
    node: Node,
) -> None:
    """Handle incoming MeshWeaver messages."""

    if message.type == "PING":
        print(f"\nPING received from node: {message.sender_id}")

        response = Message(
            type="PONG",
            sender_id=node.node_id,
            payload={"reply_to": message.message_id},
        )

        transport.sendto(encode_message(response), addr)

        print(f"PONG sent to {addr[0]}:{addr[1]}")

    elif message.type == "PONG":
        print(f"\nPONG received from node: {message.sender_id}")
        print("The other node is reachable!")

    elif message.type == "ERROR":
        print(f"\nERROR received from node: {message.sender_id}")
        print(f"Details: {message.payload}")


async def run_node(
    host: str,
    port: int,
    peer_host: str | None,
    peer_port: int | None,
    send_ping: bool,
) -> None:
    """Start a MeshWeaver node and keep it running."""

    node = Node()
    transport = None

    def message_handler(
        message: Message,
        addr: tuple[str, int],
        udp_transport: asyncio.DatagramTransport,
    ) -> None:
        handle_message(message, addr, udp_transport, node)

    try:
        transport, protocol = await start_udp_listener(
            host,
            port,
            decoder=decode_message,
            message_handler=message_handler,
        )

        address = transport.get_extra_info("sockname")

        print("\n" + "=" * 40)
        print("       MESHWEAVER NODE")
        print("=" * 40)

        print("\nNode Information:")
        print(json.dumps(node.get_info(), indent=4))

        print("\nUDP Listener Started!")
        print(f"Listening on: {address[0]}:{address[1]}")

        if send_ping:
            if peer_host is None or peer_port is None:
                raise ValueError("Peer host and port are required to send PING")

            ping = Message(
                type="PING",
                sender_id=node.node_id,
                payload={"target": f"{peer_host}:{peer_port}"},
            )

            transport.sendto(
                encode_message(ping),
                (peer_host, peer_port),
            )

            print(f"\nPING sent to {peer_host}:{peer_port}")

        print("\nWaiting for incoming messages...")
        print("Press CTRL+C to stop.\n")

        await asyncio.Event().wait()

    finally:
        if transport is not None:
            transport.close()
            print("\nMeshWeaver node stopped.")


def main() -> None:
    """Start the application."""

    parser = argparse.ArgumentParser(description="Start a MeshWeaver network node.")

    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="IP address to listen on.",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=9000,
        help="UDP listening port.",
    )

    parser.add_argument(
        "--peer-host",
        help="IP address of the peer node.",
    )

    parser.add_argument(
        "--peer-port",
        type=int,
        help="Port of the peer node.",
    )

    parser.add_argument(
        "--ping",
        action="store_true",
        help="Send a PING message to the peer after starting.",
    )

    args = parser.parse_args()

    if not 0 <= args.port <= 65535:
        parser.error("Port must be between 0 and 65535.")

    if args.peer_port is not None and not 1 <= args.peer_port <= 65535:
        parser.error("Peer port must be between 1 and 65535.")

    if args.ping and (args.peer_host is None or args.peer_port is None):
        parser.error("--ping requires --peer-host and --peer-port.")

    try:
        asyncio.run(
            run_node(
                args.host,
                args.port,
                args.peer_host,
                args.peer_port,
                args.ping,
            )
        )
    except KeyboardInterrupt:
        print("\nShutdown requested by user.")


if __name__ == "__main__":
    main()
