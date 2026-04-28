# core/job.py
# Defines the Job dataclass that groups a set of tasks under one unit of work.

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, List, Optional
import uuid
import time

from core.task import Task, TaskStatus


class JobStatus(str, Enum):
    SUBMITTED  = "SUBMITTED"
    RUNNING    = "RUNNING"
    COMPLETED  = "COMPLETED"
    FAILED     = "FAILED"


@dataclass
class Job:
    name      : str
    input_data: Any                          # raw input (path, list, …)
    job_id    : str        = field(default_factory=lambda: str(uuid.uuid4()))
    status    : JobStatus  = JobStatus.SUBMITTED
    tasks     : List[Task] = field(default_factory=list)
    results   : List[Any]  = field(default_factory=list)
    created_at: float      = field(default_factory=time.time)
    updated_at: float      = field(default_factory=time.time)

    # ── helpers ───────────────────────────────────────────────────────────────
    @property
    def total_tasks(self) -> int:
        return len(self.tasks)

    @property
    def completed_tasks(self) -> int:
        return sum(1 for t in self.tasks if t.status == TaskStatus.COMPLETED)

    @property
    def failed_tasks(self) -> int:
        return sum(1 for t in self.tasks if t.status == TaskStatus.FAILED)

    @property
    def progress(self) -> float:
        if not self.tasks:
            return 0.0
        return self.completed_tasks / self.total_tasks * 100

    def is_done(self) -> bool:
        if not self.tasks:
            return False
        return all(t.status in (TaskStatus.COMPLETED, TaskStatus.FAILED)
                   for t in self.tasks)

    # ── serialisation ─────────────────────────────────────────────────────────
    def to_dict(self) -> dict:
        return {
            "job_id"    : self.job_id,
            "name"      : self.name,
            "status"    : self.status.value,
            "total"     : self.total_tasks,
            "completed" : self.completed_tasks,
            "failed"    : self.failed_tasks,
            "progress"  : round(self.progress, 1),
            "tasks"     : [t.to_dict() for t in self.tasks],
            "results"   : self.results,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    def __repr__(self):
        return (f"Job(id={self.job_id[:8]}… name={self.name!r} "
                f"status={self.status.value} "
                f"{self.completed_tasks}/{self.total_tasks} done)")
