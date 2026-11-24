"""Integration tests for live MeshWeaver peer discovery and heartbeats."""

from __future__ import annotations

import asyncio

from meshweaver.network.discovery import PeerDiscovery
from meshweaver.network.heartbeat import HeartbeatManager
from meshweaver.network.peer_registry import PeerRegistry, PeerStatus
from meshweaver.network.protocol import (
    Message,
    decode_message,
    encode_message,
)
from meshweaver.network.udp import start_udp_listener


class ThreadSafeUDPTransport:
    """Send UDP packets safely from a background heartbeat thread."""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        transport: asyncio.DatagramTransport,
    ) -> None:
        self.loop = loop
        self.transport = transport

    def send(self, data: bytes, address: tuple[str, int]) -> None:
        """Schedule the UDP send on the asyncio event loop."""
        self.loop.call_soon_threadsafe(
            self.transport.sendto,
            data,
            address,
        )


def test_two_nodes_discover_exchange_heartbeats_and_detect_failure() -> None:
    """Verify discovery, heartbeat RTT, failure detection, and cleanup."""

    async def run_test() -> None:
        loop = asyncio.get_running_loop()

        node_a_id = "integration-node-a"
        node_b_id = "integration-node-b"

        registry_a = PeerRegistry()
        registry_b = PeerRegistry()

        discovery_a = PeerDiscovery(registry_a)
        discovery_b = PeerDiscovery(registry_b)

        heartbeat_a = None
        heartbeat_b = None

        transport_a = None
        transport_b = None

        protocol_a = None
        protocol_b = None

        heartbeat_success_a = asyncio.Event()
        heartbeat_success_b = asyncio.Event()

        def handle_a(
            message: Message,
            address: tuple[str, int],
            udp_transport: asyncio.DatagramTransport,
        ) -> None:
            if message.sender_id == node_a_id:
                return

            if message.type in ("PING", "PONG"):
                discovery_a.handle_discovery_message(message, address)

            if message.type == "PING":
                assert heartbeat_a is not None
                response = heartbeat_a.build_pong(message)
                udp_transport.sendto(
                    encode_message(response),
                    address,
                )

            elif message.type == "PONG":
                assert heartbeat_a is not None
                rtt = heartbeat_a.handle_message(message)

                if rtt is not None:
                    heartbeat_success_a.set()

        def handle_b(
            message: Message,
            address: tuple[str, int],
            udp_transport: asyncio.DatagramTransport,
        ) -> None:
            if message.sender_id == node_b_id:
                return

            if message.type in ("PING", "PONG"):
                discovery_b.handle_discovery_message(message, address)

            if message.type == "PING":
                assert heartbeat_b is not None
                response = heartbeat_b.build_pong(message)
                udp_transport.sendto(
                    encode_message(response),
                    address,
                )

            elif message.type == "PONG":
                assert heartbeat_b is not None
                rtt = heartbeat_b.handle_message(message)

                if rtt is not None:
                    heartbeat_success_b.set()

        try:
            # Start two real UDP listeners on operating-system-assigned ports.
            transport_a, protocol_a = await start_udp_listener(
                host="127.0.0.1",
                port=0,
                decoder=decode_message,
                message_handler=handle_a,
            )

            transport_b, protocol_b = await start_udp_listener(
                host="127.0.0.1",
                port=0,
                decoder=decode_message,
                message_handler=handle_b,
            )

            address_a = transport_a.get_extra_info("sockname")
            address_b = transport_b.get_extra_info("sockname")

            # Register bootstrap peers so heartbeat managers know where
            # to send their first heartbeat.

            registry_b.register_peer(
                node_a_id,
                address_a[0],
                address_a[1],
            )

            # Create thread-safe adapters for background heartbeat threads.
            heartbeat_transport_a = ThreadSafeUDPTransport(
                loop,
                transport_a,
            )

            heartbeat_transport_b = ThreadSafeUDPTransport(
                loop,
                transport_b,
            )

            # Use short timings to keep the automated test fast.
            heartbeat_a = HeartbeatManager(
                node_id=node_a_id,
                transport=heartbeat_transport_a,
                peer_registry=registry_a,
                interval=0.05,
                timeout=0.25,
            )

            heartbeat_b = HeartbeatManager(
                node_id=node_b_id,
                transport=heartbeat_transport_b,
                peer_registry=registry_b,
                interval=0.05,
                timeout=0.25,
            )

            heartbeat_a.start()
            heartbeat_b.start()

            # Both nodes must receive at least one matching heartbeat reply.
            await asyncio.wait_for(
                asyncio.gather(
                    heartbeat_success_a.wait(),
                    heartbeat_success_b.wait(),
                ),
                timeout=3.0,
            )

            # Verify that discovery and heartbeat RTT tracking worked.
            peer_a = registry_a.get_peer(node_b_id)
            peer_b = registry_b.get_peer(node_a_id)

            assert peer_a is not None
            assert peer_b is not None

            assert peer_a.status == PeerStatus.ACTIVE
            assert peer_b.status == PeerStatus.ACTIVE

            assert node_b_id in heartbeat_a.response_times
            assert node_a_id in heartbeat_b.response_times

            assert heartbeat_a.response_times[node_b_id] >= 0
            assert heartbeat_b.response_times[node_a_id] >= 0

            # Stop Node B's heartbeat manager and UDP listener.
            # Node A should detect that B no longer responds.
            heartbeat_b.stop()
            transport_b.close()
            transport_b = None

            async def wait_for_inactive_peer() -> None:
                deadline = loop.time() + 3.0

                while loop.time() < deadline:
                    peer = registry_a.get_peer(node_b_id)

                    if peer is not None and peer.status == PeerStatus.INACTIVE:
                        return

                    await asyncio.sleep(0.02)

                raise AssertionError(
                    "Node A did not mark Node B as INACTIVE after timeout."
                )

            await wait_for_inactive_peer()

            inactive_peer = registry_a.get_peer(node_b_id)

            assert inactive_peer is not None
            assert inactive_peer.status == PeerStatus.INACTIVE

        finally:
            # Stop heartbeat threads before closing their transports.
            if heartbeat_a is not None:
                heartbeat_a.stop()

            if heartbeat_b is not None:
                heartbeat_b.stop()

            if transport_a is not None:
                transport_a.close()

            if transport_b is not None:
                transport_b.close()

            # Allow asyncio to process transport closure callbacks.
            await asyncio.sleep(0)

        assert protocol_a is not None
        assert protocol_b is not None
        assert protocol_a.transport is None
        assert protocol_b.transport is None

    asyncio.run(run_test())
