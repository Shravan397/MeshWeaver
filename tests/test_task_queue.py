
"""Tests for the asynchronous task queue."""

import asyncio

from meshweaver.tasks.queue import TaskQueue
from meshweaver.tasks.task import Task


def test_enqueue_and_retrieve_task() -> None:
    async def run_test() -> None:
        queue = TaskQueue()
        task = Task("compute", {"value": 10})

        await queue.enqueue(task)

        assert queue.get_task(task.task_id) is task
        assert queue.size() == 1

    asyncio.run(run_test())


def test_dequeue_returns_correct_task() -> None:
    async def run_test() -> None:
        queue = TaskQueue()
        task = Task("compute", {"value": 10})

        await queue.enqueue(task)
        dequeued_task = await queue.dequeue()

        assert dequeued_task is task
        assert queue.size() == 0

    asyncio.run(run_test())


def test_empty_queue_returns_none() -> None:
    async def run_test() -> None:
        queue = TaskQueue()

        result = await queue.dequeue()

        assert result is None
        assert queue.size() == 0

    asyncio.run(run_test())


def test_task_can_be_retrieved_after_dequeue() -> None:
    async def run_test() -> None:
        queue = TaskQueue()
        task = Task("compute", {"value": 10})

        await queue.enqueue(task)
        await queue.dequeue()

        assert queue.get_task(task.task_id) is task

    asyncio.run(run_test())


def test_queue_preserves_task_order() -> None:
    async def run_test() -> None:
        queue = TaskQueue()
        task_one = Task("first", {})
        task_two = Task("second", {})

        await queue.enqueue(task_one)
        await queue.enqueue(task_two)

        assert await queue.dequeue() is task_one
        assert await queue.dequeue() is task_two

    asyncio.run(run_test())
