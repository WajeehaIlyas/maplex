# core/task.py
# Defines the Task dataclass and TaskStatus enum used throughout the system.

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional
import uuid
import time


class TaskStatus(str, Enum):
    PENDING   = "PENDING"    # waiting to be assigned
    ASSIGNED  = "ASSIGNED"   # sent to a worker, awaiting result
    COMPLETED = "COMPLETED"  # result received
    FAILED    = "FAILED"     # worker reported an error


class TaskType(str, Enum):
    MAP    = "MAP"
    REDUCE = "REDUCE"
    DUMMY  = "DUMMY"   # used in Phase 1 testing


@dataclass
class Task:
    task_type : TaskType
    payload   : Any                        # data the worker must process
    task_id   : str          = field(default_factory=lambda: str(uuid.uuid4()))
    status    : TaskStatus   = TaskStatus.PENDING
    worker_id : Optional[str] = None       # which worker was assigned
    result    : Any          = None        # filled in when completed
    error     : Optional[str] = None       # filled in on failure
    created_at: float        = field(default_factory=time.time)
    updated_at: float        = field(default_factory=time.time)

    # ── serialisation ─────────────────────────────────────────────────────────
    def to_dict(self) -> dict:
        return {
            "task_id"   : self.task_id,
            "task_type" : self.task_type.value,
            "payload"   : self.payload,
            "status"    : self.status.value,
            "worker_id" : self.worker_id,
            "result"    : self.result,
            "error"     : self.error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Task":
        t = cls(
            task_type = TaskType(d["task_type"]),
            payload   = d["payload"],
            task_id   = d["task_id"],
        )
        t.status     = TaskStatus(d["status"])
        t.worker_id  = d.get("worker_id")
        t.result     = d.get("result")
        t.error      = d.get("error")
        t.created_at = d.get("created_at", t.created_at)
        t.updated_at = d.get("updated_at", t.updated_at)
        return t

    def __repr__(self):
        return (f"Task(id={self.task_id[:8]}… type={self.task_type.value} "
                f"status={self.status.value})")
