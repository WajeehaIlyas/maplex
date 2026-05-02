# core/worker.py
# Worker node: registers with master, polls for tasks, executes them,
# reports results back.
#
# Phase 2 adds MAP and REDUCE task handlers.
# The worker resolves mapper/reducer class names from the payload and
# delegates to the appropriate mapreduce class.

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

# ── Registry: maps class-name strings → actual classes ───────────────────────
# Add new mapper/reducer classes here as Phase 3 jobs are created.

def _build_registry():
    reg = {}
    try:
        from jobs.word_count.mapper  import WordCountMapper
        from jobs.word_count.reducer import WordCountReducer
        reg["WordCountMapper"]  = WordCountMapper
        reg["WordCountReducer"] = WordCountReducer
    except ImportError:
        pass
    try:
        from jobs.log_analysis.mapper  import LogMapper
        from jobs.log_analysis.reducer import LogReducer
        reg["LogMapper"]  = LogMapper
        reg["LogReducer"] = LogReducer
    except ImportError:
        pass
    return reg

CLASS_REGISTRY = _build_registry()


class Worker:
    def __init__(self, worker_id: str, host: str = "127.0.0.1", port: int = 0):
        self.worker_id = worker_id
        self.host      = host
        self.port      = port
        self.client    = MasterClient()
        self.running   = False
        self._retries  = 0

        self._hb_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self):
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
                    f"Worker {self.worker_id} poll error "
                    f"(attempt {self._retries}/{config.MAX_RETRIES}): {exc}")
                if self._retries >= config.MAX_RETRIES:
                    log.critical(
                        f"Worker {self.worker_id}: max retries reached. Stopping.")
                    self.running = False
                time.sleep(config.POLL_INTERVAL * 2)

    # ── Task dispatch ─────────────────────────────────────────────────────────

    def _handle_task(self, task_dict: dict):
        task_id   = task_dict["task_id"]
        task_type = task_dict["task_type"]
        payload   = task_dict["payload"]

        log.info(f"Worker {self.worker_id} executing "
                 f"task {task_id[:8]}… [{task_type}]")
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
        if task_type == TaskType.DUMMY.value:
            return self._execute_dummy(payload)
        if task_type == TaskType.MAP.value:
            return self._execute_map(payload)
        if task_type == TaskType.REDUCE.value:
            return self._execute_reduce(payload)
        raise ValueError(f"Unknown task type: {task_type}")

    # ── Phase 1: dummy ────────────────────────────────────────────────────────

    def _execute_dummy(self, payload):
        time.sleep(0.2)
        if isinstance(payload, list):
            return [x ** 2 for x in payload]
        if isinstance(payload, (int, float)):
            return payload ** 2
        return f"processed: {payload}"

    # ── Phase 2: MAP ──────────────────────────────────────────────────────────

    def _execute_map(self, payload: dict):
        """
        Payload schema:
            {
              "chunk_index": int,
              "lines":       List[str],
              "mapper_cls":  str   e.g. "WordCountMapper"
            }
        Returns a list of (key, value) pairs.
        """
        mapper_cls_name = payload.get("mapper_cls", "")
        mapper_cls      = CLASS_REGISTRY.get(mapper_cls_name)
        if mapper_cls is None:
            raise ValueError(f"Unknown mapper class: {mapper_cls_name!r}. "
                             f"Available: {list(CLASS_REGISTRY.keys())}")

        mapper = mapper_cls()
        pairs  = mapper.apply(
            chunk_index = payload["chunk_index"],
            lines       = payload["lines"],
        )
        # Return as list-of-lists (JSON-serialisable)
        return [[k, v] for k, v in pairs]

    # ── Phase 2: REDUCE ───────────────────────────────────────────────────────

    def _execute_reduce(self, payload: dict):
        """
        Payload schema:
            {
              "key":          Any,
              "values":       List[Any],
              "reducer_cls":  str   e.g. "WordCountReducer"
            }
        Returns a list of (output_key, output_value) pairs.
        """
        reducer_cls_name = payload.get("reducer_cls", "")
        reducer_cls      = CLASS_REGISTRY.get(reducer_cls_name)
        if reducer_cls is None:
            raise ValueError(f"Unknown reducer class: {reducer_cls_name!r}. "
                             f"Available: {list(CLASS_REGISTRY.keys())}")

        reducer = reducer_cls()
        pairs   = reducer.apply(payload)
        return [[k, v] for k, v in pairs]

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    def _heartbeat_loop(self):
        while self.running:
            try:
                self.client.heartbeat(self.worker_id)
            except Exception:
                pass
            time.sleep(5)
