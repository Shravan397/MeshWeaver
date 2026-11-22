
import asyncio

from meshweaver.network.protocol import (
    Message,
    decode_message,
    encode_message,
)
from meshweaver.network.udp import start_udp_listener


def test_two_nodes_complete_ping_pong_communication():
    async def run_test():
        node_a_id = "node-a"
        node_b_id = "node-b"

        pong_received = asyncio.Event()
        received_pong = []

        node_a_transport = None
        node_b_transport = None
        node_a_protocol = None
        node_b_protocol = None

        def node_a_handler(message, address, transport):
            if message.type == "PONG":
                received_pong.append(message)
                pong_received.set()

        def node_b_handler(message, address, transport):
            if message.type == "PING":
                response = Message(
                    type="PONG",
                    sender_id=node_b_id,
                    payload={"reply_to": message.message_id},
                )
                transport.sendto(encode_message(response), address)

        try:
            node_a_transport, node_a_protocol = await start_udp_listener(
                host="127.0.0.1",
                port=0,
                decoder=decode_message,
                message_handler=node_a_handler,
            )

            node_b_transport, node_b_protocol = await start_udp_listener(
                host="127.0.0.1",
                port=0,
                decoder=decode_message,
                message_handler=node_b_handler,
            )

            node_b_address = node_b_transport.get_extra_info("sockname")

            ping = Message(
                type="PING",
                sender_id=node_a_id,
                payload={"target": node_b_id},
            )

            node_a_transport.sendto(
                encode_message(ping),
                node_b_address,
            )

            await asyncio.wait_for(pong_received.wait(), timeout=2)

            assert len(received_pong) == 1

            pong = received_pong[0]

            assert pong.type == "PONG"
            assert pong.sender_id == node_b_id
            assert pong.payload["reply_to"] == ping.message_id

            assert node_a_protocol.transport is node_a_transport
            assert node_b_protocol.transport is node_b_transport

        finally:
            if node_a_transport is not None:
                node_a_transport.close()

            if node_b_transport is not None:
                node_b_transport.close()

            await asyncio.sleep(0)

        assert node_a_protocol.transport is None
        assert node_b_protocol.transport is None

    asyncio.run(run_test())
