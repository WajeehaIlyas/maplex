# communication/client.py
# HTTP client used by Worker nodes, the Pipeline, and test scripts.

import requests
import logging
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config

log     = logging.getLogger("client")
TIMEOUT = 10


class MasterClient:
    def __init__(self, base_url: str = None):
        self.base_url = base_url or config.MASTER_URL

    def _get(self, path: str, params: dict = None) -> dict:
        resp = requests.get(
            f"{self.base_url}{path}", params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, data: dict) -> dict:
        resp = requests.post(
            f"{self.base_url}{path}", json=data, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    # ── Worker API ────────────────────────────────────────────────────────────

    def register_worker(self, worker_id, host, port):
        return self._post("/worker/register",
                          {"worker_id": worker_id, "host": host, "port": port})

    def heartbeat(self, worker_id):
        return self._post("/worker/heartbeat", {"worker_id": worker_id})

    def get_task(self, worker_id):
        return self._get("/worker/task", params={"worker_id": worker_id})

    def submit_result(self, task_id, result, success=True, error=None):
        return self._post("/worker/result", {
            "task_id": task_id, "result": result,
            "success": success, "error": error,
        })

    # ── Job / task submission ─────────────────────────────────────────────────

    def submit_job(self, name: str, tasks: list) -> dict:
        return self._post("/job/submit", {"name": name, "tasks": tasks})

    def submit_standalone_task(self, task_type: str, payload) -> dict:
        """Submit a single task (used by the Pipeline for MAP/REDUCE tasks)."""
        return self._post("/task/submit",
                          {"task_type": task_type, "payload": payload})

    # ── Status queries ────────────────────────────────────────────────────────

    def get_job_status(self, job_id):
        return self._get(f"/job/{job_id}")

    def get_task_status(self, task_id):
        return self._get(f"/task/{task_id}")

    def system_status(self):
        return self._get("/status")

    def list_workers(self):
        return self._get("/workers")

    def all_results(self):
        return self._get("/results")

    def health(self):
        return self._get("/health")
