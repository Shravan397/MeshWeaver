
import argparse
import asyncio
import json
import logging

from .node import Node
from .network.udp import start_udp_listener


logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)


async def run_node(host: str, port: int) -> None:
    """Start a MeshWeaver node and keep it running."""

    node = Node()
    transport = None

    try:
        transport, protocol = await start_udp_listener(host, port)

        address = transport.get_extra_info("sockname")

        print("\n" + "=" * 40)
        print("       MESHWEAVER NODE")
        print("=" * 40)

        print("\nNode Information:")
        print(json.dumps(node.get_info(), indent=4))

        print("\nUDP Listener Started!")
        print(f"Listening on: {address[0]}:{address[1]}")
        print("\nWaiting for incoming messages...")
        print("Press CTRL+C to stop.\n")

        await asyncio.Event().wait()

    finally:
        if transport is not None:
            transport.close()
            print("\nMeshWeaver node stopped.")


def main() -> None:
    """Start the application."""

    parser = argparse.ArgumentParser(
        description="Start a MeshWeaver network node."
    )

    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="IP address to listen on.",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=9000,
        help="UDP listening port.",
    )

    args = parser.parse_args()

    if not 0 <= args.port <= 65535:
        parser.error("Port must be between 0 and 65535.")

    try:
        asyncio.run(run_node(args.host, args.port))
    except KeyboardInterrupt:
        print("\nShutdown requested by user.")


if __name__ == "__main__":
    main()