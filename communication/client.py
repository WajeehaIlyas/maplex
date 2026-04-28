# communication/client.py
# HTTP client used by Worker nodes (and test scripts) to communicate
# with the Master server.

import requests
import logging
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config

log = logging.getLogger("client")

TIMEOUT = 10   # request timeout in seconds


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

    def register_worker(self, worker_id: str, host: str, port: int) -> dict:
        return self._post("/worker/register",
                          {"worker_id": worker_id, "host": host, "port": port})

    def heartbeat(self, worker_id: str) -> dict:
        return self._post("/worker/heartbeat", {"worker_id": worker_id})

    def get_task(self, worker_id: str):
        """Returns task dict or None if no work is available."""
        result = self._get("/worker/task", params={"worker_id": worker_id})
        return result   # may be JSON null → None

    def submit_result(self, task_id: str, result,
                      success: bool = True, error: str = None) -> dict:
        return self._post("/worker/result", {
            "task_id": task_id,
            "result" : result,
            "success": success,
            "error"  : error,
        })

    # ── Client / monitoring API ───────────────────────────────────────────────

    def submit_job(self, name: str, tasks: list) -> dict:
        """
        tasks: list of {"task_type": "DUMMY", "payload": …}
        Returns {"job_id": …, "status": …, "total_tasks": …}
        """
        return self._post("/job/submit", {"name": name, "tasks": tasks})

    def get_job_status(self, job_id: str) -> dict:
        return self._get(f"/job/{job_id}")

    def get_task_status(self, task_id: str) -> dict:
        return self._get(f"/task/{task_id}")

    def system_status(self) -> dict:
        return self._get("/status")

    def list_workers(self) -> list:
        return self._get("/workers")

    def all_results(self) -> list:
        return self._get("/results")

    def health(self) -> dict:
        return self._get("/health")
