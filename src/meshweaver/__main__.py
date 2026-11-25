import argparse
import asyncio
import json
import logging

from .config import NodeConfig
from .monitoring.resource_monitor import (
    ResourceMonitor,
    ResourceMonitorError,
    ResourceSnapshot,
)
from .network.discovery import PeerDiscovery
from .network.heartbeat import HeartbeatManager
from .network.peer_registry import PeerRegistry, PeerStatus
from .network.protocol import Message, decode_message, encode_message
from .network.udp import start_udp_listener
from .node import Node
from .tasks.executor import TaskExecutor

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


async def sample_cpu_loop(
    monitor: ResourceMonitor,
    cpu_state: dict[str, float | None],
    resource_state: dict[str, ResourceSnapshot | None],
    interval: float = 2.0,
) -> None:
    """Refresh CPU and RAM snapshots without blocking the event loop."""
    while True:
        try:
            snapshot = await monitor.collect()
            cpu_state["cpu_percent"] = snapshot.cpu_percent
            resource_state["snapshot"] = snapshot
        except ResourceMonitorError as exc:
            logging.debug("Resource sample skipped: %s", exc)

        await asyncio.sleep(interval)


async def monitor_resource_usage(
    node_id: str,
    peer_registry: PeerRegistry,
    transport: asyncio.DatagramTransport,
    resource_state: dict[str, ResourceSnapshot | None],
    interval: float = 5.0,
) -> None:
    """Send the latest CPU and RAM snapshot to active peers."""
    while True:
        try:
            snapshot = resource_state["snapshot"]

            if snapshot is not None:
                payload = {
                    "cpu_percent": snapshot.cpu_percent,
                    "ram_percent": snapshot.ram_percent,
                    "ram_total_mb": snapshot.ram_total_mb,
                    "ram_available_mb": snapshot.ram_available_mb,
                    "timestamp": snapshot.timestamp,
                }

                message = Message(
                    type="RESOURCE_UPDATE",
                    sender_id=node_id,
                    payload=payload,
                )
                encoded_message = encode_message(message)

                active_peers = peer_registry.get_all_peers(status=PeerStatus.ACTIVE)

                for peer in active_peers:
                    if peer.node_id == node_id:
                        continue

                    transport.sendto(
                        encoded_message,
                        (peer.host, peer.port),
                    )

        except asyncio.CancelledError:
            raise
        except Exception:
            logging.exception("Resource monitoring cycle failed.")

        await asyncio.sleep(interval)


async def dispatch_tasks_loop(
    task_executor: TaskExecutor,
    interval: float = 0.5,
) -> None:
    """Continuously requeue orphaned tasks and dispatch pending tasks."""
    while True:
        requeued = await task_executor.requeue_orphaned_tasks()

        for task_id in requeued:
            logging.warning("Requeued orphaned task as %s", task_id)

        dispatched = await task_executor.dispatch_next()

        if dispatched is not None:
            task, target_peer_id = dispatched
            logging.info(
                "Dispatched task %s (%s) to peer %s",
                task.task_id,
                task.task_type,
                target_peer_id,
            )

        await asyncio.sleep(interval)


async def probe_bootstrap_peers_loop(
    node: Node,
    transport: asyncio.DatagramTransport,
    peer_registry: PeerRegistry,
    bootstrap_peers: list[tuple[str, int]],
    interval: float,
) -> None:
    """Ensure bootstrap peers without an ACTIVE registry record are probed."""
    while True:
        await asyncio.sleep(interval)
        for host, port in bootstrap_peers:
            existing = peer_registry.get_peer_by_address(host, port)
            if existing is not None and existing.status == PeerStatus.ACTIVE:
                continue
            ping = Message(
                type="PING",
                sender_id=node.node_id,
                payload={"target": f"{host}:{port}"},
            )
            transport.sendto(encode_message(ping), (host, port))
            logging.debug("Bootstrap probe PING sent to %s:%s", host, port)


async def monitor_peer_status(peer_registry: PeerRegistry) -> None:
    """Log peer status changes detected in the registry."""
    previous_statuses: dict[str, str] = {}

    while True:
        peers = peer_registry.get_all_peers()
        active_ids = {peer.node_id for peer in peers}
        for stale_id in list(previous_statuses):
            if stale_id not in active_ids:
                del previous_statuses[stale_id]

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


async def _run_and_reply_task(
    message: Message,
    addr: tuple[str, int],
    transport: asyncio.DatagramTransport,
    task_executor: TaskExecutor,
) -> None:
    """Execute an incoming TASK_ASSIGN and send back TASK_RESULT."""
    result_msg = await task_executor.execute_assigned_task(message)
    transport.sendto(encode_message(result_msg), addr)

    logging.info(
        "TASK_RESULT sent for task %s (%s) to %s:%s",
        result_msg.payload.get("task_id"),
        result_msg.payload.get("status"),
        addr[0],
        addr[1],
    )


def handle_message(
    message: Message,
    addr: tuple[str, int],
    transport: asyncio.DatagramTransport,
    node: Node,
    discovery: PeerDiscovery,
    heartbeat_manager: HeartbeatManager,
    peer_registry: PeerRegistry,
    task_executor: TaskExecutor | None = None,
) -> None:
    """Handle incoming MeshWeaver messages."""
    if message.sender_id == node.node_id:
        logging.debug("Ignoring message from this node.")
        return

    if message.type in ("PING", "PONG"):
        peer = discovery.handle_discovery_message(message, addr)
        logging.info(
            "Peer discovered/updated: %s at %s:%s",
            peer.node_id,
            peer.host,
            peer.port,
        )

    if message.type == "PING":
        logging.info("PING received from node: %s", message.sender_id)
        response = heartbeat_manager.build_pong(message)
        transport.sendto(encode_message(response), addr)
        logging.info("PONG sent to %s:%s", addr[0], addr[1])

    elif message.type == "PONG":
        logging.info("PONG received from node: %s", message.sender_id)
        rtt = heartbeat_manager.handle_message(message)

        if rtt is not None:
            logging.info(
                "Heartbeat successful with %s (RTT: %.4f seconds)",
                message.sender_id,
                rtt,
            )
        else:
            logging.info("PONG received; no matching heartbeat request was pending.")

    elif message.type == "RESOURCE_UPDATE":
        peer = peer_registry.get_peer(message.sender_id)

        if peer is None:
            logging.warning(
                "Ignoring RESOURCE_UPDATE from unknown peer %s",
                message.sender_id,
            )
            return

        if (peer.host, peer.port) != addr:
            logging.warning(
                "Ignoring RESOURCE_UPDATE from unexpected address for %s",
                message.sender_id,
            )
            return

        peer_registry.update_peer(
            message.sender_id,
            cpu_percent=message.payload["cpu_percent"],
            ram_percent=message.payload["ram_percent"],
            ram_total_mb=message.payload["ram_total_mb"],
            ram_available_mb=message.payload["ram_available_mb"],
            resource_timestamp=message.payload["timestamp"],
            refresh_last_seen=False,
        )

        logging.info(
            "Resource update from %s: CPU %.2f%%, RAM %.2f%%",
            message.sender_id,
            message.payload["cpu_percent"],
            message.payload["ram_percent"],
        )

    elif message.type == "TASK_ASSIGN" and task_executor is not None:
        logging.info(
            "TASK_ASSIGN received from %s for task %s",
            message.sender_id,
            message.payload.get("task_id"),
        )
        asyncio.create_task(
            _run_and_reply_task(message, addr, transport, task_executor)
        )

    elif message.type == "TASK_RESULT" and task_executor is not None:
        accepted = task_executor.handle_result_message(message)

        if accepted:
            logging.info(
                "TASK_RESULT received for task %s: %s",
                message.payload.get("task_id"),
                message.payload,
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
    initial_task: tuple[str, dict] | None = None,
) -> None:
    """Start a MeshWeaver node with discovery, monitoring, and task execution."""
    node = Node()
    peer_registry = PeerRegistry()
    discovery = PeerDiscovery(peer_registry)

    resource_monitor = ResourceMonitor()
    cpu_state: dict[str, float | None] = {"cpu_percent": 0.0}
    resource_state: dict[str, ResourceSnapshot | None] = {"snapshot": None}

    transport = None
    heartbeat_manager = None
    task_executor = None
    background_tasks: list[asyncio.Task[None]] = []

    try:
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
                    peer_registry,
                    task_executor,
                )
                if heartbeat_manager is not None
                else None
            ),
        )

        address = transport.get_extra_info("sockname")
        loop = asyncio.get_running_loop()
        heartbeat_transport = ThreadSafeUDPTransport(loop, transport)

        heartbeat_manager = HeartbeatManager(
            node_id=node.node_id,
            transport=heartbeat_transport,
            peer_registry=peer_registry,
            interval=config.heartbeat_interval_seconds,
            timeout=config.heartbeat_timeout_seconds,
            cpu_provider=lambda: cpu_state["cpu_percent"],
        )

        task_executor = TaskExecutor(
            node_id=node.node_id,
            transport=transport,
            peer_registry=peer_registry,
        )

        print("\n" + "=" * 40)
        print("       MESHWEAVER NODE")
        print("=" * 40)
        print("\nNode Information:")
        print(json.dumps(node.get_info(), indent=4))
        print("\nUDP Listener Started!")
        print(f"Listening on: {address[0]}:{address[1]}")
        print("\nHeartbeat Configuration:")
        print(f"Interval: {config.heartbeat_interval_seconds} seconds")
        print(f"Timeout: {config.heartbeat_timeout_seconds} seconds")

        if send_ping:
            if not config.bootstrap_peers:
                raise ValueError("A bootstrap peer is required when --ping is enabled.")

            bootstrap_host, bootstrap_port = config.bootstrap_peers[0]
            ping = Message(
                type="PING",
                sender_id=node.node_id,
                payload={"target": f"{bootstrap_host}:{bootstrap_port}"},
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

        if initial_task is not None:
            task_type, task_payload = initial_task
            queued = await task_executor.submit_task(
                task_type,
                task_payload,
            )

            logging.info(
                "Enqueued initial task %s (%s)",
                queued.task_id,
                queued.task_type,
            )

        heartbeat_manager.start()
        logging.info("Heartbeat manager started.")

        background_tasks.extend(
            [
                asyncio.create_task(monitor_peer_status(peer_registry)),
                asyncio.create_task(
                    sample_cpu_loop(
                        resource_monitor,
                        cpu_state,
                        resource_state,
                    )
                ),
                asyncio.create_task(
                    monitor_resource_usage(
                        node.node_id,
                        peer_registry,
                        transport,
                        resource_state,
                    )
                ),
                asyncio.create_task(dispatch_tasks_loop(task_executor)),
            ]
        )
        if config.bootstrap_peers:
            background_tasks.append(
                asyncio.create_task(
                    probe_bootstrap_peers_loop(
                        node,
                        transport,
                        peer_registry,
                        config.bootstrap_peers,
                        config.heartbeat_interval_seconds,
                    )
                )
            )

        logging.info("Peer status, CPU/RAM monitoring, and task dispatcher started.")

        print("\nWaiting for incoming messages...")
        print("Press CTRL+C to stop.\n")

        await asyncio.Event().wait()

    finally:
        if heartbeat_manager is not None:
            heartbeat_manager.stop()
            logging.info("Heartbeat manager stopped.")

        for task in background_tasks:
            task.cancel()

        for task in background_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass

        if transport is not None:
            transport.close()
            logging.info("UDP listener closed.")

        print("\nMeshWeaver node stopped.")


def main() -> None:
    """Start the application."""
    parser = argparse.ArgumentParser(description="Start a MeshWeaver network node.")
    parser.add_argument("--host", default="127.0.0.1", help="IP address to listen on.")
    parser.add_argument("--port", type=int, default=9000, help="UDP listening port.")
    parser.add_argument("--peer-host", help="IP address of the bootstrap peer.")
    parser.add_argument("--peer-port", type=int, help="Port of the bootstrap peer.")
    parser.add_argument(
        "--ping",
        action="store_true",
        help="Send an initial PING to the bootstrap peer.",
    )
    parser.add_argument(
        "--submit-task",
        help=(
            "Submit a task on startup as '<type>:<json_payload>' "
            "(e.g. 'sum:{\"numbers\":[1,2,3]}')."
        ),
    )
    args = parser.parse_args()

    if (args.peer_host is None) != (args.peer_port is None):
        parser.error("--peer-host and --peer-port must be provided together.")

    if args.ping and args.peer_host is None:
        parser.error("--ping requires --peer-host and --peer-port.")

    initial_task: tuple[str, dict] | None = None

    if args.submit_task is not None:
        if ":" not in args.submit_task:
            parser.error("--submit-task must follow '<task_type>:<json_payload>'.")

        task_type, raw_json = args.submit_task.split(":", maxsplit=1)

        try:
            parsed_payload = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            parser.error(f"Invalid JSON payload in --submit-task: {exc}")

        if not isinstance(parsed_payload, dict):
            parser.error("--submit-task JSON payload must be a JSON object.")

        initial_task = (task_type.strip(), parsed_payload)

    try:
        config = NodeConfig(
            listen_host=args.host,
            listen_port=args.port,
            bootstrap_peers=(
                [(args.peer_host, args.peer_port)]
                if args.peer_host is not None and args.peer_port is not None
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
                initial_task=initial_task,
            )
        )
    except KeyboardInterrupt:
        print("\nShutdown requested by user.")


if __name__ == "__main__":
    main()
