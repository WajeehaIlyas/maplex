# core/worker.py
# Worker node: registers with master, polls for tasks, executes them,
# reports results back.

import time
import logging
import threading

from communication.client import MasterClient
from core.task import TaskType
import config

logging.basicConfig(
    level  = logging.INFO,
    format = "[WORKER %(asctime)s] %(levelname)s %(message)s",
    datefmt= "%H:%M:%S",
)
log = logging.getLogger("worker")


class Worker:
    def __init__(self, worker_id: str, host: str = "127.0.0.1", port: int = 0):
        self.worker_id = worker_id
        self.host      = host
        self.port      = port
        self.client    = MasterClient()
        self.running   = False
        self._retries  = 0

        # heartbeat thread
        self._hb_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self):
        """Register with master and begin polling loop."""
        resp = self.client.register_worker(self.worker_id, self.host, self.port)
        if resp.get("status") != "ok":
            raise RuntimeError(
                f"Worker {self.worker_id} could not register: {resp}")
        log.info(f"Worker {self.worker_id} registered. Starting poll loop…")
        self.running = True
        self._hb_thread.start()
        self._poll_loop()

    def stop(self):
        self.running = False
        log.info(f"Worker {self.worker_id} stopped.")

    # ── Polling ───────────────────────────────────────────────────────────────

    def _poll_loop(self):
        while self.running:
            try:
                task_dict = self.client.get_task(self.worker_id)
                if task_dict:
                    self._handle_task(task_dict)
                    self._retries = 0
                else:
                    time.sleep(config.POLL_INTERVAL)
            except Exception as exc:
                self._retries += 1
                log.error(
                    f"Worker {self.worker_id} poll error (attempt "
                    f"{self._retries}/{config.MAX_RETRIES}): {exc}")
                if self._retries >= config.MAX_RETRIES:
                    log.critical(
                        f"Worker {self.worker_id}: max retries reached. Stopping.")
                    self.running = False
                time.sleep(config.POLL_INTERVAL * 2)

    # ── Task execution ────────────────────────────────────────────────────────

    def _handle_task(self, task_dict: dict):
        task_id   = task_dict["task_id"]
        task_type = task_dict["task_type"]
        payload   = task_dict["payload"]

        log.info(f"Worker {self.worker_id} executing task {task_id[:8]}… "
                 f"[{task_type}]")
        try:
            result = self._execute(task_type, payload)
            self.client.submit_result(task_id, result, success=True)
            log.info(f"Worker {self.worker_id} → task {task_id[:8]}… done.")
        except Exception as exc:
            log.error(
                f"Worker {self.worker_id} task {task_id[:8]}… failed: {exc}")
            self.client.submit_result(
                task_id, result=None, success=False, error=str(exc))

    def _execute(self, task_type: str, payload):
        """Dispatch to the correct handler based on task type."""
        if task_type == TaskType.DUMMY.value:
            return self._execute_dummy(payload)
        # Phase 2: MAP / REDUCE handlers will be added here
        raise ValueError(f"Unknown task type: {task_type}")

    # ── Task handlers (Phase 1: dummy only) ───────────────────────────────────

    def _execute_dummy(self, payload):
        """
        Dummy task: simulate work by squaring every number in the payload list.
        Sleep briefly to mimic real processing time.
        """
        time.sleep(0.2)   # simulate work
        if isinstance(payload, list):
            return [x ** 2 for x in payload]
        if isinstance(payload, (int, float)):
            return payload ** 2
        return f"processed: {payload}"

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    def _heartbeat_loop(self):
        while self.running:
            try:
                self.client.heartbeat(self.worker_id)
            except Exception:
                pass   # heartbeat failure is non-fatal
            time.sleep(5)
