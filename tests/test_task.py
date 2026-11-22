
"""Tests for the task model and status lifecycle."""

import pytest

from meshweaver.tasks.task import Task, TaskStatus


def test_task_has_unique_ids() -> None:
    task_one = Task("compute", {"value": 10})
    task_two = Task("compute", {"value": 20})

    assert task_one.task_id != task_two.task_id


def test_task_initial_status_is_pending() -> None:
    task = Task("compute", {"value": 10})

    assert task.status == TaskStatus.PENDING


def test_task_has_creation_timestamp() -> None:
    task = Task("compute", {"value": 10})

    assert task.created_at is not None


def test_valid_status_transitions() -> None:
    task = Task("compute", {"value": 10})

    task.update_status(TaskStatus.RUNNING)
    assert task.status == TaskStatus.RUNNING

    task.update_status(TaskStatus.COMPLETED)
    assert task.status == TaskStatus.COMPLETED


def test_task_can_transition_to_failed() -> None:
    task = Task("compute", {"value": 10})

    task.update_status(TaskStatus.RUNNING)
    task.update_status(TaskStatus.FAILED)

    assert task.status == TaskStatus.FAILED


def test_invalid_status_transition_is_rejected() -> None:
    task = Task("compute", {"value": 10})

    with pytest.raises(ValueError):
        task.update_status(TaskStatus.COMPLETED)


def test_completed_task_cannot_change_status() -> None:
    task = Task("compute", {"value": 10})
    task.update_status(TaskStatus.RUNNING)
    task.update_status(TaskStatus.COMPLETED)

    with pytest.raises(ValueError):
        task.update_status(TaskStatus.RUNNING)
