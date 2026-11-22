
"""Asynchronous task queue and task lookup."""

import asyncio

from meshweaver.tasks.task import Task


class TaskQueue:
    """Store tasks and provide asynchronous queue operations."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[Task] = asyncio.Queue()
        self._tasks: dict[str, Task] = {}

    async def enqueue(self, task: Task) -> None:
        """Add a task to the pending queue."""
        if not isinstance(task, Task):
            raise TypeError("Only Task instances can be enqueued")

        if task.task_id in self._tasks:
            raise ValueError(f"Task already exists: {task.task_id}")

        self._tasks[task.task_id] = task
        await self._queue.put(task)

    async def dequeue(self) -> Task | None:
        """Return the next task, or None if the queue is empty."""
        try:
            return self._queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    def get_task(self, task_id: str) -> Task | None:
        """Retrieve a task by its unique ID."""
        return self._tasks.get(task_id)

    def size(self) -> int:
        """Return the number of tasks currently pending in the queue."""
        return self._queue.qsize()
