
"""Task model and execution status lifecycle."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4


class TaskStatus(Enum):
    """Possible execution states for a task."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass
class Task:
    """Represent a task submitted for execution."""

    task_type: str
    payload: dict[str, Any]
    task_id: str = field(
        default_factory=lambda: str(uuid4()),
        init=False,
    )
    status: TaskStatus = field(default=TaskStatus.PENDING, init=False)
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
        init=False,
    )

    def update_status(self, new_status: TaskStatus) -> None:
        """Update task status when the transition is valid."""
        allowed_transitions = {
            TaskStatus.PENDING: {TaskStatus.RUNNING},
            TaskStatus.RUNNING: {
                TaskStatus.COMPLETED,
                TaskStatus.FAILED,
            },
            TaskStatus.COMPLETED: set(),
            TaskStatus.FAILED: set(),
        }

        if not isinstance(new_status, TaskStatus):
            raise ValueError("new_status must be a TaskStatus value")

        if new_status not in allowed_transitions[self.status]:
            raise ValueError(
                f"Invalid task status transition: "
                f"{self.status.value} -> {new_status.value}"
            )

        self.status = new_status
