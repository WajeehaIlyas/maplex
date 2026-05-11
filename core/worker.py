# core/worker.py  —  Phase 4: adds ColourMapper and ColourReducer

import time, logging, threading
from communication.client import MasterClient
from core.task import TaskType
import config

logging.basicConfig(level=logging.INFO,
    format="[WORKER %(asctime)s] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("worker")


def _build_registry():
    reg = {}
    try:
        from jobs.word_count.mapper  import WordCountMapper
        from jobs.word_count.reducer import WordCountReducer
        reg["WordCountMapper"]  = WordCountMapper
        reg["WordCountReducer"] = WordCountReducer
    except ImportError: pass
    try:
        from jobs.log_analysis.mapper  import LogMapper
        from jobs.log_analysis.reducer import LogReducer
        reg["LogMapper"]  = LogMapper
        reg["LogReducer"] = LogReducer
    except ImportError: pass
    try:
        from jobs.image_processing.mapper  import ImageMapper
        from jobs.image_processing.reducer import ImageReducer
        reg["ImageMapper"]  = ImageMapper
        reg["ImageReducer"] = ImageReducer
    except ImportError: pass
    try:
        from jobs.image_analysis.mapper  import ColourMapper
        from jobs.image_analysis.reducer import ColourReducer
        reg["ColourMapper"]  = ColourMapper
        reg["ColourReducer"] = ColourReducer
    except ImportError: pass
    return reg

CLASS_REGISTRY = _build_registry()


class Worker:
    def __init__(self, worker_id, host="127.0.0.1", port=0):
        self.worker_id = worker_id
        self.host      = host
        self.port      = port
        self.client    = MasterClient()
        self.running   = False
        self._retries  = 0
        self._hb_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)

    def start(self):
        resp = self.client.register_worker(self.worker_id, self.host, self.port)
        if resp.get("status") != "ok":
            raise RuntimeError(f"Worker {self.worker_id} could not register: {resp}")
        log.info(f"Worker {self.worker_id} registered. Starting poll loop…")
        self.running = True
        self._hb_thread.start()
        self._poll_loop()

    def stop(self):
        self.running = False

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
                log.error(f"Worker {self.worker_id} poll error "
                          f"(attempt {self._retries}/{config.MAX_RETRIES}): {exc}")
                if self._retries >= config.MAX_RETRIES:
                    log.critical(f"Worker {self.worker_id}: max retries reached. Stopping.")
                    self.running = False
                time.sleep(config.POLL_INTERVAL * 2)

    def _handle_task(self, task_dict):
        task_id   = task_dict["task_id"]
        task_type = task_dict["task_type"]
        payload   = task_dict["payload"]
        log.info(f"Worker {self.worker_id} executing task {task_id[:8]}… [{task_type}]")
        try:
            result = self._execute(task_type, payload)
            self.client.submit_result(task_id, result, success=True)
            log.info(f"Worker {self.worker_id} → task {task_id[:8]}… done.")
        except Exception as exc:
            log.error(f"Worker {self.worker_id} task {task_id[:8]}… failed: {exc}")
            self.client.submit_result(task_id, result=None, success=False, error=str(exc))

    def _execute(self, task_type, payload):
        if task_type == TaskType.DUMMY.value:  return self._execute_dummy(payload)
        if task_type == TaskType.MAP.value:    return self._execute_map(payload)
        if task_type == TaskType.REDUCE.value: return self._execute_reduce(payload)
        raise ValueError(f"Unknown task type: {task_type}")

    def _execute_dummy(self, payload):
        time.sleep(0.2)
        if isinstance(payload, list):          return [x**2 for x in payload]
        if isinstance(payload, (int, float)):  return payload**2
        return f"processed: {payload}"

    def _execute_map(self, payload: dict):
        mapper_cls_name = payload.get("mapper_cls", "")
        mapper_cls      = CLASS_REGISTRY.get(mapper_cls_name)
        if mapper_cls is None:
            raise ValueError(f"Unknown mapper: {mapper_cls_name!r}. "
                             f"Available: {list(CLASS_REGISTRY.keys())}")
        mapper = mapper_cls()

        # ColourMapper and ImageMapper use apply_chunk with image_paths
        if hasattr(mapper, "apply_chunk") and "image_paths" in payload:
            # Image analysis job (colour analysis)
            if mapper_cls_name == "ColourMapper":
                pairs = mapper.apply_chunk(
                    chunk_index  = payload["chunk_index"],
                    image_paths  = payload["image_paths"],
                )
            else:
                # ImageMapper (transforms)
                pairs = mapper.apply_chunk(
                    chunk_index      = payload["chunk_index"],
                    image_paths      = payload["image_paths"],
                    transform        = payload["transform"],
                    transform_params = payload.get("transform_params", {}),
                    output_dir       = payload["output_dir"],
                )
        else:
            # Text mappers (word count, log analysis)
            pairs = mapper.apply(
                chunk_index = payload["chunk_index"],
                lines       = payload["lines"],
            )
        return [[k, v] for k, v in pairs]

    def _execute_reduce(self, payload: dict):
        reducer_cls_name = payload.get("reducer_cls", "")
        reducer_cls      = CLASS_REGISTRY.get(reducer_cls_name)
        if reducer_cls is None:
            raise ValueError(f"Unknown reducer: {reducer_cls_name!r}. "
                             f"Available: {list(CLASS_REGISTRY.keys())}")
        reducer = reducer_cls()
        pairs   = reducer.apply(payload)
        return [[k, v] for k, v in pairs]

    def _heartbeat_loop(self):
        while self.running:
            try: self.client.heartbeat(self.worker_id)
            except Exception: pass
            time.sleep(5)
