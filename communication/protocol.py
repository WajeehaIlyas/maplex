# communication/protocol.py
# Defines standard message shapes used between master and workers.
# Acts as a single source of truth for the JSON schema — useful for
# validation and documentation.

from typing import Any, Optional


def make_register_msg(worker_id: str, host: str, port: int) -> dict:
    return {"worker_id": worker_id, "host": host, "port": port}


def make_result_msg(task_id: str, result: Any,
                    success: bool, error: Optional[str] = None) -> dict:
    return {
        "task_id": task_id,
        "result" : result,
        "success": success,
        "error"  : error,
    }


def make_task_submission(task_type: str, payload: Any) -> dict:
    """Single task entry used inside a job submission payload."""
    return {"task_type": task_type, "payload": payload}


def make_job_submission(name: str, tasks: list) -> dict:
    return {"name": name, "tasks": tasks}


# ── Response validators ───────────────────────────────────────────────────────

def is_ok(response: dict) -> bool:
    return isinstance(response, dict) and response.get("status") == "ok"


def has_error(response: dict) -> bool:
    return isinstance(response, dict) and "error" in response
