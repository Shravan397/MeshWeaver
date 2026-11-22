import argparse
import sys
from meshweaver.network.udp import UDPTransport


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="meshweaver",
        description="MeshWeaver: Zero-Dependency P2P Async Task Broker",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host address to bind or connect to (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5000,
        help="UDP port number (default: 5000)",
    )
    args = parser.parse_args()

    print(f"Starting MeshWeaver node on {args.host}:{args.port}...")
    with UDPTransport(host=args.host, port=args.port) as transport:
        transport.bind()
        print("UDP transport bound and ready.")

    return 0


if __name__ == "__main__":
    sys.exit(main())