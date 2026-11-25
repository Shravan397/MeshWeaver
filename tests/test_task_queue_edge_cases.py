"""Edge-case tests for the asynchronous task queue."""

import asyncio

import pytest

from meshweaver.tasks.queue import TaskQueue
from meshweaver.tasks.task import Task, TaskStatus


def test_enqueue_rejects_invalid_input() -> None:
    async def run_test() -> None:
        queue = TaskQueue()

        with pytest.raises(TypeError):
            await queue.enqueue("invalid task")  # type: ignore[arg-type]

        assert queue.size() == 0

    asyncio.run(run_test())


def test_enqueue_rejects_none() -> None:
    async def run_test() -> None:
        queue = TaskQueue()

        with pytest.raises(TypeError):
            await queue.enqueue(None)  # type: ignore[arg-type]

        assert queue.size() == 0

    asyncio.run(run_test())


def test_duplicate_task_is_rejected() -> None:
    async def run_test() -> None:
        queue = TaskQueue()
        task = Task("compute", {"value": 10})

        await queue.enqueue(task)

        with pytest.raises(ValueError):
            await queue.enqueue(task)

        assert queue.size() == 1

    asyncio.run(run_test())


def test_duplicate_task_after_dequeue_is_rejected() -> None:
    async def run_test() -> None:
        queue = TaskQueue()
        task = Task("compute", {"value": 10})

        await queue.enqueue(task)
        await queue.dequeue()

        with pytest.raises(ValueError):
            await queue.enqueue(task)

        assert queue.size() == 0

    asyncio.run(run_test())


def test_queue_size_after_multiple_operations() -> None:
    async def run_test() -> None:
        queue = TaskQueue()
        task_one = Task("first", {})
        task_two = Task("second", {})
        task_three = Task("third", {})

        await queue.enqueue(task_one)
        await queue.enqueue(task_two)
        assert queue.size() == 2

        await queue.enqueue(task_three)
        assert queue.size() == 3

        await queue.dequeue()
        assert queue.size() == 2

        await queue.dequeue()
        assert queue.size() == 1

    asyncio.run(run_test())


def test_invalid_input_does_not_corrupt_existing_tasks() -> None:
    async def run_test() -> None:
        queue = TaskQueue()
        valid_task = Task("compute", {"value": 10})

        await queue.enqueue(valid_task)

        with pytest.raises(TypeError):
            await queue.enqueue("invalid")  # type: ignore[arg-type]

        assert queue.size() == 1
        assert queue.get_task(valid_task.task_id) is valid_task

    asyncio.run(run_test())


def test_completed_task_remains_retrievable() -> None:
    async def run_test() -> None:
        queue = TaskQueue()
        task = Task("compute", {"value": 10})

        await queue.enqueue(task)
        task.update_status(TaskStatus.RUNNING)
        task.update_status(TaskStatus.COMPLETED)

        await queue.dequeue()

        assert task.status == TaskStatus.COMPLETED
        assert queue.get_task(task.task_id) is task

    asyncio.run(run_test())
