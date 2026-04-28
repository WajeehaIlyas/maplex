# core/master.py
# The Master node: registers workers, queues tasks, assigns work, collects results.

import threading
import time
import logging
from typing import Dict, List, Optional
from collections import deque

from core.task import Task, TaskStatus, TaskType
from core.job  import Job, JobStatus
import config

logging.basicConfig(
    level  = logging.INFO,
    format = "[MASTER %(asctime)s] %(levelname)s %(message)s",
    datefmt= "%H:%M:%S",
)
log = logging.getLogger("master")


class Master:
    def __init__(self):
        self._lock         = threading.Lock()

        # worker registry  {worker_id: {"host":…, "port":…, "last_seen":…}}
        self.workers       : Dict[str, dict] = {}

        # task queue (pending tasks waiting for a free worker)
        self.task_queue    : deque           = deque()

        # all known tasks  {task_id: Task}
        self.tasks         : Dict[str, Task] = {}

        # all known jobs   {job_id: Job}
        self.jobs          : Dict[str, Job]  = {}

        # background thread that watches for timed-out tasks
        self._timeout_thread = threading.Thread(
            target=self._timeout_watcher, daemon=True)
        self._timeout_thread.start()

        log.info("Master initialised — timeout watcher running.")

    # ── Worker registry ───────────────────────────────────────────────────────

    def register_worker(self, worker_id: str, host: str, port: int) -> dict:
        with self._lock:
            self.workers[worker_id] = {
                "worker_id" : worker_id,
                "host"      : host,
                "port"      : port,
                "status"    : "idle",
                "last_seen" : time.time(),
                "tasks_done": 0,
            }
        log.info(f"Worker registered: {worker_id} @ {host}:{port}")
        return {"status": "ok", "worker_id": worker_id}

    def heartbeat(self, worker_id: str) -> dict:
        with self._lock:
            if worker_id in self.workers:
                self.workers[worker_id]["last_seen"] = time.time()
                return {"status": "ok"}
        return {"status": "unknown_worker"}

    def list_workers(self) -> List[dict]:
        with self._lock:
            return list(self.workers.values())

    # ── Job / Task submission ─────────────────────────────────────────────────

    def submit_job(self, job: Job) -> Job:
        with self._lock:
            self.jobs[job.job_id] = job
            job.status = JobStatus.RUNNING
            for task in job.tasks:
                self.tasks[task.task_id] = task
                self.task_queue.append(task.task_id)
        log.info(f"Job submitted: {job.job_id[:8]}… — "
                 f"{len(job.tasks)} tasks queued.")
        return job

    def submit_tasks(self, tasks: List[Task]) -> List[str]:
        """Add standalone tasks (not attached to a Job) to the queue."""
        ids = []
        with self._lock:
            for task in tasks:
                self.tasks[task.task_id] = task
                self.task_queue.append(task.task_id)
                ids.append(task.task_id)
        log.info(f"Queued {len(tasks)} standalone task(s).")
        return ids

    # ── Task assignment (called by workers polling for work) ──────────────────

    def get_next_task(self, worker_id: str) -> Optional[dict]:
        with self._lock:
            # update last_seen
            if worker_id in self.workers:
                self.workers[worker_id]["last_seen"] = time.time()
                self.workers[worker_id]["status"]    = "idle"

            # find a pending task
            while self.task_queue:
                task_id = self.task_queue.popleft()
                task = self.tasks.get(task_id)
                if task and task.status == TaskStatus.PENDING:
                    task.status    = TaskStatus.ASSIGNED
                    task.worker_id = worker_id
                    task.updated_at= time.time()
                    if worker_id in self.workers:
                        self.workers[worker_id]["status"] = "busy"
                    log.info(f"Task {task_id[:8]}… assigned to {worker_id}")
                    return task.to_dict()
        return None   # no work available right now

    # ── Result collection ─────────────────────────────────────────────────────

    def submit_result(self, task_id: str, result: any,
                      success: bool, error: str = None) -> dict:
        with self._lock:
            task = self.tasks.get(task_id)
            if not task:
                return {"status": "error", "message": "task not found"}

            task.result     = result
            task.updated_at = time.time()
            if success:
                task.status = TaskStatus.COMPLETED
                log.info(f"Task {task_id[:8]}… completed by {task.worker_id}")
            else:
                task.status = TaskStatus.FAILED
                task.error  = error
                log.warning(f"Task {task_id[:8]}… FAILED: {error}")

            # mark worker idle again
            if task.worker_id and task.worker_id in self.workers:
                self.workers[task.worker_id]["status"]     = "idle"
                self.workers[task.worker_id]["tasks_done"] += 1

            # update parent job if any
            self._update_job_status(task)

        return {"status": "ok"}

    def _update_job_status(self, task: Task):
        """Called inside _lock — checks if parent job is now complete."""
        for job in self.jobs.values():
            if any(t.task_id == task.task_id for t in job.tasks):
                if job.is_done():
                    all_ok = all(t.status == TaskStatus.COMPLETED
                                 for t in job.tasks)
                    job.status     = JobStatus.COMPLETED if all_ok else JobStatus.FAILED
                    job.results    = [t.result for t in job.tasks
                                      if t.status == TaskStatus.COMPLETED]
                    job.updated_at = time.time()
                    log.info(f"Job {job.job_id[:8]}… → {job.status.value}")
                break

    # ── Status queries ────────────────────────────────────────────────────────

    def get_task_status(self, task_id: str) -> Optional[dict]:
        with self._lock:
            task = self.tasks.get(task_id)
            return task.to_dict() if task else None

    def get_job_status(self, job_id: str) -> Optional[dict]:
        with self._lock:
            job = self.jobs.get(job_id)
            return job.to_dict() if job else None

    def get_all_results(self) -> List[dict]:
        with self._lock:
            return [t.to_dict() for t in self.tasks.values()
                    if t.status == TaskStatus.COMPLETED]

    def get_stats(self) -> dict:
        with self._lock:
            statuses = {}
            for t in self.tasks.values():
                statuses[t.status.value] = statuses.get(t.status.value, 0) + 1
            return {
                "workers"     : len(self.workers),
                "active_workers": sum(
                    1 for w in self.workers.values() if w["status"] == "busy"),
                "queue_depth" : len(self.task_queue),
                "tasks"       : statuses,
                "jobs"        : len(self.jobs),
            }

    # ── Background: timeout watcher ───────────────────────────────────────────

    def _timeout_watcher(self):
        while True:
            time.sleep(5)
            now = time.time()
            with self._lock:
                for task in self.tasks.values():
                    if (task.status == TaskStatus.ASSIGNED and
                            now - task.updated_at > config.TASK_TIMEOUT):
                        log.warning(
                            f"Task {task.task_id[:8]}… timed out — re-queuing.")
                        task.status    = TaskStatus.PENDING
                        task.worker_id = None
                        task.updated_at= now
                        self.task_queue.append(task.task_id)
