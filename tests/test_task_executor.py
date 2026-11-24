"""Unit and two-node UDP integration tests for TaskExecutor."""

from __future__ import annotations

import asyncio

from meshweaver.network.discovery import PeerDiscovery
from meshweaver.network.heartbeat import HeartbeatManager
from meshweaver.network.peer_registry import PeerRegistry, PeerStatus
from meshweaver.network.protocol import Message, MessageType, decode_message, encode_message
from meshweaver.network.udp import start_udp_listener
from meshweaver.tasks.executor import TaskExecutor
from meshweaver.tasks.task import TaskStatus


class DummyTransport:
    """In-memory transport capturing sent UDP packets."""

    def __init__(self) -> None:
        self.sent: list[tuple[bytes, tuple[str, int]]] = []

    def sendto(self, data: bytes, addr: tuple[str, int]) -> None:
        self.sent.append((data, addr))


def test_dispatch_selects_lowest_cpu_peer_and_completes_on_result() -> None:
    async def run_test() -> None:
        registry = PeerRegistry()
        registry.register_peer("node-busy", "127.0.0.1", 9101, status=PeerStatus.ACTIVE, cpu_percent=78.5)
        registry.register_peer("node-idle", "127.0.0.1", 9102, status=PeerStatus.ACTIVE, cpu_percent=12.0)

        transport = DummyTransport()
        executor = TaskExecutor("node-coord", transport, registry)

        task = await executor.submit_task("sum", {"numbers": [10, 20, 30]})
        dispatched = await executor.dispatch_next()

        assert dispatched is not None
        dispatched_task, target_id = dispatched
        assert dispatched_task.task_id == task.task_id
        assert target_id == "node-idle"
        assert task.status == TaskStatus.RUNNING
        assert len(transport.sent) == 1

        raw_packet, dest_addr = transport.sent[0]
        assert dest_addr == ("127.0.0.1", 9102)
        assign_msg = decode_message(raw_packet)
        assert assign_msg.type == MessageType.TASK_ASSIGN
        assert assign_msg.payload["task_id"] == task.task_id

        result_msg = Message(
            type=MessageType.TASK_RESULT,
            sender_id="node-idle",
            payload={
                "task_id": task.task_id,
                "status": TaskStatus.COMPLETED.value,
                "output": {"sum": 60},
            },
        )
        assert executor.handle_result_message(result_msg) is True
        assert task.status == TaskStatus.COMPLETED
        assert executor.get_result(task.task_id)["output"] == {"sum": 60}

    asyncio.run(run_test())


def test_requeue_orphaned_tasks_when_worker_becomes_inactive() -> None:
    async def run_test() -> None:
        registry = PeerRegistry()
        registry.register_peer("node-worker", "127.0.0.1", 9103, status=PeerStatus.ACTIVE, cpu_percent=15.0)
        transport = DummyTransport()
        executor = TaskExecutor("node-coord", transport, registry)

        original = await executor.submit_task("echo", {"msg": "hello"})
        await executor.dispatch_next()
        assert original.status == TaskStatus.RUNNING

        registry.update_peer("node-worker", status=PeerStatus.INACTIVE)
        requeued_ids = await executor.requeue_orphaned_tasks()

        assert len(requeued_ids) == 1
        assert original.status == TaskStatus.FAILED
        assert executor.task_queue.size() == 1
        retry_task = executor.task_queue.get_task(requeued_ids[0])
        assert retry_task is not None
        assert retry_task.status == TaskStatus.PENDING
        assert retry_task.payload == {"msg": "hello"}

    asyncio.run(run_test())


def test_execute_assigned_task_handles_errors_and_async_handlers() -> None:
    async def run_test() -> None:
        registry = PeerRegistry()
        executor = TaskExecutor("node-worker", DummyTransport(), registry)

        async def async_double(payload: dict) -> dict:
            await asyncio.sleep(0)
            return {"doubled": payload["value"] * 2}

        executor.register_handler("double", async_double)

        ok_msg = Message(
            type=MessageType.TASK_ASSIGN,
            sender_id="node-coord",
            payload={"task_id": "t-1", "task_type": "double", "payload": {"value": 21}},
        )
        ok_reply = await executor.execute_assigned_task(ok_msg)
        assert ok_reply.type == MessageType.TASK_RESULT
        assert ok_reply.payload["status"] == TaskStatus.COMPLETED.value
        assert ok_reply.payload["output"] == {"doubled": 42}

        bad_msg = Message(
            type=MessageType.TASK_ASSIGN,
            sender_id="node-coord",
            payload={"task_id": "t-2", "task_type": "unknown_op", "payload": {}},
        )
        bad_reply = await executor.execute_assigned_task(bad_msg)
        assert bad_reply.payload["status"] == TaskStatus.FAILED.value
        assert "Unsupported task_type" in bad_reply.payload["error"]

    asyncio.run(run_test())


def test_two_nodes_exchange_heartbeat_cpu_and_execute_task_over_udp() -> None:
    async def run_test() -> None:
        reg_a = PeerRegistry()
        reg_b = PeerRegistry()
        disc_a = PeerDiscovery(reg_a)
        disc_b = PeerDiscovery(reg_b)

        exec_a: TaskExecutor | None = None
        exec_b: TaskExecutor | None = None
        hb_a: HeartbeatManager | None = None
        hb_b: HeartbeatManager | None = None
        completed_event = asyncio.Event()

        def handler_a(msg: Message, addr: tuple[str, int], transport: asyncio.DatagramTransport) -> None:
            if msg.type in (MessageType.PING, MessageType.PONG):
                disc_a.handle_discovery_message(msg, addr)
            if msg.type == MessageType.PONG and hb_a is not None:
                hb_a.handle_message(msg)
            elif msg.type == MessageType.TASK_RESULT and exec_a is not None:
                if exec_a.handle_result_message(msg):
                    completed_event.set()

        def handler_b(msg: Message, addr: tuple[str, int], transport: asyncio.DatagramTransport) -> None:
            if msg.type in (MessageType.PING, MessageType.PONG):
                disc_b.handle_discovery_message(msg, addr)
            if msg.type == MessageType.PING and hb_b is not None:
                pong = hb_b.build_pong(msg)
                transport.sendto(encode_message(pong), addr)
            elif msg.type == MessageType.TASK_ASSIGN and exec_b is not None:
                async def _work() -> None:
                    reply = await exec_b.execute_assigned_task(msg)
                    transport.sendto(encode_message(reply), addr)

                asyncio.create_task(_work())

        trans_a, _ = await start_udp_listener("127.0.0.1", 0, decoder=decode_message, message_handler=handler_a)
        trans_b, _ = await start_udp_listener("127.0.0.1", 0, decoder=decode_message, message_handler=handler_b)

        try:
            addr_b = trans_b.get_extra_info("sockname")
            hb_a = HeartbeatManager("node-a", trans_a, reg_a, interval=1.0, timeout=5.0)
            hb_b = HeartbeatManager("node-b", trans_b, reg_b, interval=1.0, timeout=5.0, cpu_provider=lambda: 18.5)
            exec_a = TaskExecutor("node-a", trans_a, reg_a)
            exec_b = TaskExecutor("node-b", trans_b, reg_b)

            reg_a.register_peer("node-b", addr_b[0], addr_b[1], status=PeerStatus.ACTIVE)
            hb_a.send_heartbeat("node-b")
            await asyncio.sleep(0.05)

            peer_b = reg_a.get_peer("node-b")
            assert peer_b is not None
            assert peer_b.cpu_percent == 18.5

            task = await exec_a.submit_task("sha256", {"text": "meshweaver"})
            dispatched = await exec_a.dispatch_next()
            assert dispatched is not None

            await asyncio.wait_for(completed_event.wait(), timeout=2.0)
            assert task.status == TaskStatus.COMPLETED
            result = exec_a.get_result(task.task_id)
            assert result is not None
            assert len(result["output"]["sha256"]) == 64
        finally:
            trans_a.close()
            trans_b.close()
            await asyncio.sleep(0)

    asyncio.run(run_test())
