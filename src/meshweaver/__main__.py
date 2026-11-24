
import argparse
import asyncio
import json
import logging

from .config import NodeConfig
from .network.discovery import PeerDiscovery
from .network.heartbeat import HeartbeatManager
from .network.peer_registry import PeerRegistry
from .network.protocol import Message, decode_message, encode_message
from .network.udp import start_udp_listener
from .node import Node

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)


class ThreadSafeUDPTransport:
    """Allow background threads to send through an asyncio UDP transport."""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        transport: asyncio.DatagramTransport,
    ) -> None:
        self.loop = loop
        self.transport = transport

    def send(self, data: bytes, address: tuple[str, int]) -> None:
        """Schedule UDP sending safely on the asyncio event loop."""
        self.loop.call_soon_threadsafe(
            self.transport.sendto,
            data,
            address,
        )


async def monitor_peer_status(peer_registry: PeerRegistry) -> None:
    """Log peer status changes detected in the registry."""

    previous_statuses: dict[str, str] = {}

    while True:
        peers = peer_registry.get_all_peers()

        for peer in peers:
            current_status = getattr(
                peer.status,
                "value",
                str(peer.status),
            )

            if peer.node_id not in previous_statuses:
                previous_statuses[peer.node_id] = current_status
                continue

            previous_status = previous_statuses[peer.node_id]

            if previous_status != current_status:
                logging.warning(
                    "Peer %s status changed: %s -> %s",
                    peer.node_id,
                    previous_status,
                    current_status,
                )

                previous_statuses[peer.node_id] = current_status

        await asyncio.sleep(1)


def handle_message(
    message: Message,
    addr: tuple[str, int],
    transport: asyncio.DatagramTransport,
    node: Node,
    discovery: PeerDiscovery,
    heartbeat_manager: HeartbeatManager,
) -> None:
    """Handle incoming MeshWeaver messages."""

    # Ignore messages originating from this node.
    if message.sender_id == node.node_id:
        logging.debug("Ignoring message from this node.")
        return

    # Discover or update peers using PING/PONG messages.
    if message.type in ("PING", "PONG"):
        peer = discovery.handle_discovery_message(message, addr)

        logging.info(
            "Peer discovered/updated: %s at %s:%s",
            peer.node_id,
            peer.host,
            peer.port,
        )

    if message.type == "PING":
        logging.info(
            "PING received from node: %s",
            message.sender_id,
        )

        response = heartbeat_manager.build_pong(message)

        transport.sendto(
            encode_message(response),
            addr,
        )

        logging.info(
            "PONG sent to %s:%s",
            addr[0],
            addr[1],
        )

    elif message.type == "PONG":
        logging.info(
            "PONG received from node: %s",
            message.sender_id,
        )

        rtt = heartbeat_manager.handle_message(message)

        if rtt is not None:
            logging.info(
                "Heartbeat successful with %s (RTT: %.4f seconds)",
                message.sender_id,
                rtt,
            )
        else:
            logging.info(
                "PONG received; no matching heartbeat request was pending."
            )

    elif message.type == "ERROR":
        logging.error(
            "ERROR received from node %s: %s",
            message.sender_id,
            message.payload,
        )


async def run_node(
    config: NodeConfig,
    send_ping: bool,
) -> None:
    """Start a MeshWeaver node with discovery and heartbeat monitoring."""

    node = Node()

    # Create shared peer registry and discovery manager.
    peer_registry = PeerRegistry()
    discovery = PeerDiscovery(peer_registry)

    transport = None
    heartbeat_manager = None
    status_monitor_task: asyncio.Task[None] | None = None

    try:
        # Start the UDP listener.
        transport, _protocol = await start_udp_listener(
            config.listen_host,
            config.listen_port,
            decoder=decode_message,
            message_handler=lambda message, addr, udp_transport: (
                handle_message(
                    message,
                    addr,
                    udp_transport,
                    node,
                    discovery,
                    heartbeat_manager,
                )
                if heartbeat_manager is not None
                else None
            ),
        )

        address = transport.get_extra_info("sockname")

        # Create a thread-safe adapter for the heartbeat background thread.
        loop = asyncio.get_running_loop()

        heartbeat_transport = ThreadSafeUDPTransport(
            loop,
            transport,
        )

        # Initialize heartbeat monitoring using validated configuration.
        heartbeat_manager = HeartbeatManager(
            node_id=node.node_id,
            transport=heartbeat_transport,
            peer_registry=peer_registry,
            interval=config.heartbeat_interval_seconds,
            timeout=config.heartbeat_timeout_seconds,
        )

        print("\n" + "=" * 40)
        print("       MESHWEAVER NODE")
        print("=" * 40)

        print("\nNode Information:")
        print(json.dumps(node.get_info(), indent=4))

        print("\nUDP Listener Started!")
        print(f"Listening on: {address[0]}:{address[1]}")

        print("\nHeartbeat Configuration:")
        print(
            f"Interval: {config.heartbeat_interval_seconds} seconds"
        )
        print(
            f"Timeout: {config.heartbeat_timeout_seconds} seconds"
        )

        # Send an optional initial PING to the configured bootstrap peer.
        if send_ping:
            if not config.bootstrap_peers:
                raise ValueError(
                    "A bootstrap peer is required when --ping is enabled."
                )

            bootstrap_host, bootstrap_port = config.bootstrap_peers[0]

            ping = Message(
                type="PING",
                sender_id=node.node_id,
                payload={
                    "target": f"{bootstrap_host}:{bootstrap_port}"
                },
            )

            transport.sendto(
                encode_message(ping),
                (bootstrap_host, bootstrap_port),
            )

            logging.info(
                "Initial PING sent to %s:%s",
                bootstrap_host,
                bootstrap_port,
            )

        # Start periodic heartbeat monitoring.
        heartbeat_manager.start()

        logging.info("Heartbeat manager started.")

        # Start monitoring peer ACTIVE/INACTIVE status changes.
        status_monitor_task = asyncio.create_task(
            monitor_peer_status(peer_registry)
        )

        logging.info("Peer status monitor started.")

        print("\nWaiting for incoming messages...")
        print("Press CTRL+C to stop.\n")

        await asyncio.Event().wait()

    finally:
        # Stop the background heartbeat thread before closing UDP.
        if heartbeat_manager is not None:
            heartbeat_manager.stop()
            logging.info("Heartbeat manager stopped.")

        # Cancel and cleanly stop the peer status monitor.
        if status_monitor_task is not None:
            status_monitor_task.cancel()

            try:
                await status_monitor_task
            except asyncio.CancelledError:
                logging.info("Peer status monitor stopped.")

        # Close the UDP listener after background tasks stop.
        if transport is not None:
            transport.close()
            logging.info("UDP listener closed.")

        print("\nMeshWeaver node stopped.")


def main() -> None:
    """Start the application."""

    parser = argparse.ArgumentParser(
        description="Start a MeshWeaver network node."
    )

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
        help="IP address of the bootstrap peer.",
    )

    parser.add_argument(
        "--peer-port",
        type=int,
        help="Port of the bootstrap peer.",
    )

    parser.add_argument(
        "--ping",
        action="store_true",
        help="Send an initial PING to the bootstrap peer.",
    )

    args = parser.parse_args()

    # Both bootstrap peer arguments must be provided together.
    if (args.peer_host is None) != (args.peer_port is None):
        parser.error(
            "--peer-host and --peer-port must be provided together."
        )

    # PING requires a configured bootstrap peer.
    if args.ping and args.peer_host is None:
        parser.error(
            "--ping requires --peer-host and --peer-port."
        )

    # Validate all startup configuration before opening the UDP listener.
    try:
        config = NodeConfig(
            listen_host=args.host,
            listen_port=args.port,
            bootstrap_peers=(
                [(args.peer_host, args.peer_port)]
                if args.peer_host is not None
                and args.peer_port is not None
                else []
            ),
        )
    except ValueError as exc:
        parser.error(str(exc))

    try:
        asyncio.run(
            run_node(
                config,
                args.ping,
            )
        )
    except KeyboardInterrupt:
        print("\nShutdown requested by user.")


if __name__ == "__main__":
    main()
