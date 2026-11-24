"""Distributed task dispatcher and worker execution for MeshWeaver."""

from __future__ import annotations

import hashlib
import inspect
from collections.abc import Awaitable, Callable
from typing import Any

from meshweaver.network.peer_registry import PeerRegistry, PeerStatus
from meshweaver.network.protocol import Message, MessageType, encode_message
from meshweaver.tasks.queue import TaskQueue
from meshweaver.tasks.scheduler import select_best_peer
from meshweaver.tasks.task import Task, TaskStatus

TaskHandler = Callable[[dict[str, Any]], Any | Awaitable[Any]]


def default_task_handlers() -> dict[str, TaskHandler]:
    """Return built-in task handlers supported by MeshWeaver workers."""

    def handle_echo(payload: dict[str, Any]) -> dict[str, Any]:
        return {"echo": payload}

    def handle_sum(payload: dict[str, Any]) -> dict[str, Any]:
        numbers = payload.get("numbers", [])
        if not isinstance(numbers, list) or not all(
            isinstance(n, (int, float)) and not isinstance(n, bool) for n in numbers
        ):
            raise ValueError("'numbers' must be a list of numbers")
        return {"sum": sum(numbers)}

    def handle_sha256(payload: dict[str, Any]) -> dict[str, Any]:
        text = payload.get("text")
        if not isinstance(text, str):
            raise ValueError("'text' must be a string")
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        return {"sha256": digest}

    return {
        "echo": handle_echo,
        "sum": handle_sum,
        "sha256": handle_sha256,
    }


class TaskExecutor:
    """Coordinate task dispatch to peers and execute incoming assigned tasks."""

    def __init__(
        self,
        node_id: str,
        transport: Any,
        peer_registry: PeerRegistry,
        task_queue: TaskQueue | None = None,
        handlers: dict[str, TaskHandler] | None = None,
    ) -> None:
        self.node_id = node_id
        self.transport = transport
        self.peer_registry = peer_registry
        self.task_queue = task_queue if task_queue is not None else TaskQueue()
        self.handlers: dict[str, TaskHandler] = (
            dict(handlers) if handlers is not None else default_task_handlers()
        )
        self._assigned_peers: dict[str, str] = {}
        self._results: dict[str, dict[str, Any]] = {}

    def register_handler(self, task_type: str, handler: TaskHandler) -> None:
        """Register a handler function for a task_type."""
        if not isinstance(task_type, str) or not task_type.strip():
            raise ValueError("task_type must be a non-empty string")
        self.handlers[task_type] = handler

    async def submit_task(self, task_type: str, payload: dict[str, Any]) -> Task:
        """Create a Task and enqueue it for peer dispatch."""
        task = Task(task_type=task_type, payload=payload)
        await self.task_queue.enqueue(task)
        return task

    def get_result(self, task_id: str) -> dict[str, Any] | None:
        """Return stored result metadata for a finished task, if available."""
        return self._results.get(task_id)

    async def dispatch_next(self) -> tuple[Task, str] | None:
        """Dequeue the next task and send TASK_ASSIGN to the best active peer."""
        peers = self.peer_registry.get_all_peers(as_dicts=True)
        best_peer = select_best_peer(peers, local_node_id=self.node_id)
        if best_peer is None:
            return None

        task = await self.task_queue.dequeue()
        if task is None:
            return None

        target_id = str(best_peer["node_id"])
        target_addr = (str(best_peer["host"]), int(best_peer["port"]))

        task.update_status(TaskStatus.RUNNING)
        self._assigned_peers[task.task_id] = target_id

        assign_msg = Message(
            type=MessageType.TASK_ASSIGN,
            sender_id=self.node_id,
            payload={
                "task_id": task.task_id,
                "task_type": task.task_type,
                "payload": task.payload,
            },
        )
        send = getattr(self.transport, "sendto", None) or self.transport.send
        send(encode_message(assign_msg), target_addr)
        return task, target_id

    async def execute_assigned_task(self, message: Message) -> Message:
        """Run an incoming TASK_ASSIGN locally and build a TASK_RESULT message."""
        task_id = message.payload.get("task_id")
        task_type = message.payload.get("task_type")
        task_payload = message.payload.get("payload", {})

        if not isinstance(task_id, str) or not isinstance(task_type, str):
            return Message(
                type=MessageType.TASK_RESULT,
                sender_id=self.node_id,
                payload={
                    "task_id": task_id or "",
                    "status": TaskStatus.FAILED.value,
                    "error": "Invalid TASK_ASSIGN payload fields",
                },
            )

        handler = self.handlers.get(task_type)
        if handler is None:
            return Message(
                type=MessageType.TASK_RESULT,
                sender_id=self.node_id,
                payload={
                    "task_id": task_id,
                    "status": TaskStatus.FAILED.value,
                    "error": f"Unsupported task_type: {task_type}",
                },
            )

        try:
            outcome = handler(task_payload if isinstance(task_payload, dict) else {})
            if inspect.isawaitable(outcome):
                outcome = await outcome
            return Message(
                type=MessageType.TASK_RESULT,
                sender_id=self.node_id,
                payload={
                    "task_id": task_id,
                    "status": TaskStatus.COMPLETED.value,
                    "output": outcome,
                },
            )
        except Exception as exc:
            return Message(
                type=MessageType.TASK_RESULT,
                sender_id=self.node_id,
                payload={
                    "task_id": task_id,
                    "status": TaskStatus.FAILED.value,
                    "error": str(exc),
                },
            )

    def handle_result_message(self, message: Message) -> bool:
        """Apply an incoming TASK_RESULT to the tracked Task lifecycle."""
        if message.type != MessageType.TASK_RESULT:
            return False

        task_id = message.payload.get("task_id")
        raw_status = message.payload.get("status")
        if not isinstance(task_id, str) or not isinstance(raw_status, str):
            return False

        task = self.task_queue.get_task(task_id)
        if task is None or task.status != TaskStatus.RUNNING:
            return False

        if raw_status == TaskStatus.COMPLETED.value:
            task.update_status(TaskStatus.COMPLETED)
        else:
            task.update_status(TaskStatus.FAILED)

        self._assigned_peers.pop(task_id, None)
        self._results[task_id] = dict(message.payload)
        return True

    async def requeue_orphaned_tasks(self) -> list[str]:
        """Re-enqueue RUNNING tasks whose assigned worker is no longer ACTIVE."""
        requeued: list[str] = []
        for task_id, peer_id in list(self._assigned_peers.items()):
            peer = self.peer_registry.get_peer(peer_id)
            if peer is None or peer.status != PeerStatus.ACTIVE:
                old_task = self.task_queue.get_task(task_id)
                self._assigned_peers.pop(task_id, None)
                if old_task is not None and old_task.status == TaskStatus.RUNNING:
                    old_task.update_status(TaskStatus.FAILED)
                    retry_task = Task(
                        task_type=old_task.task_type,
                        payload=dict(old_task.payload),
                    )
                    await self.task_queue.enqueue(retry_task)
                    requeued.append(retry_task.task_id)
        return requeued
