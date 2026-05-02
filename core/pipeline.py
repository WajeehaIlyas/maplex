# mapreduce/pipeline.py
# Pipeline orchestrator.
#
# The Pipeline class drives the full MapReduce lifecycle:
#   1. Split input into chunks  →  create MAP tasks
#   2. Submit MAP tasks to master, wait for all to complete
#   3. Shuffle: flatten + group intermediate pairs by key
#   4. Create REDUCE tasks (one per unique key group)
#   5. Submit REDUCE tasks to master, wait for all to complete
#   6. Collect and return the final output dict
#
# It communicates with the master exclusively through MasterClient so it
# works whether the master is in-process or remote.

import time
import logging
from typing import Any, Dict, List, Type

from communication.client import MasterClient
from mapreduce.mapper   import Mapper
from mapreduce.shuffler import Shuffler
from mapreduce.reducer  import Reducer
from core.task import TaskType
import config

log = logging.getLogger("pipeline")


class Pipeline:
    """
    Coordinates a full MapReduce job.

    Usage:
        pipeline = Pipeline(
            mapper_cls  = LogMapper,
            reducer_cls = LogReducer,
            client      = MasterClient(),
        )
        results = pipeline.run(lines)
    """

    def __init__(self,
                 mapper_cls  : Type[Mapper],
                 reducer_cls : Type[Reducer],
                 client      : MasterClient = None,
                 poll_interval: float = None,
                 chunk_size   : int   = None):
        self.mapper       = mapper_cls()
        self.reducer      = reducer_cls()
        self.client       = client or MasterClient()
        self.poll_interval= poll_interval or config.POLL_INTERVAL
        self.chunk_size   = chunk_size    or config.CHUNK_SIZE

    # ── Public entry point ────────────────────────────────────────────────────

    def run(self, lines: List[str]) -> Dict[Any, Any]:
        """
        Execute the full Map → Shuffle → Reduce pipeline.

        Args:
            lines: Raw input as a list of text lines.

        Returns:
            Final output dict {key: aggregated_value}.
        """
        log.info(f"Pipeline starting — {len(lines)} input lines, "
                 f"chunk_size={self.chunk_size}")

        # ── Phase A: MAP ──────────────────────────────────────────────────────
        chunks   = Mapper.split_into_chunks(lines, self.chunk_size)
        log.info(f"Split into {len(chunks)} chunks → submitting MAP tasks…")

        map_task_ids = self._submit_map_tasks(chunks)
        map_results  = self._wait_for_tasks(map_task_ids, stage="MAP")

        # ── Phase B: SHUFFLE ──────────────────────────────────────────────────
        log.info("MAP complete — shuffling intermediate pairs…")
        flat    = Shuffler.flatten_map_results(map_results)
        grouped = Shuffler.shuffle(flat)
        log.info(f"Shuffle produced {len(grouped)} unique keys")

        # ── Phase C: REDUCE ───────────────────────────────────────────────────
        payloads         = Shuffler.prepare_reduce_payloads(grouped)
        log.info(f"Submitting {len(payloads)} REDUCE tasks…")
        reduce_task_ids  = self._submit_reduce_tasks(payloads)
        reduce_results   = self._wait_for_tasks(reduce_task_ids, stage="REDUCE")

        # ── Phase D: Collect ──────────────────────────────────────────────────
        final = Reducer.collect(reduce_results)
        log.info(f"Pipeline complete — {len(final)} output keys")
        return final

    # ── MAP task submission ───────────────────────────────────────────────────

    def _submit_map_tasks(self, chunks: List[List[str]]) -> List[str]:
        """Submit one MAP task per chunk and return their task IDs."""
        task_ids = []
        for idx, chunk in enumerate(chunks):
            payload = {
                "chunk_index": idx,
                "lines"      : chunk,
                "mapper_cls" : self.mapper.__class__.__name__,
            }
            resp = self.client.submit_standalone_task(
                task_type=TaskType.MAP.value, payload=payload)
            task_ids.append(resp["task_id"])
        return task_ids

    # ── REDUCE task submission ────────────────────────────────────────────────

    def _submit_reduce_tasks(self, payloads: List[dict]) -> List[str]:
        """Submit one REDUCE task per key group and return task IDs."""
        task_ids = []
        for payload in payloads:
            # Attach reducer class name so workers know which reducer to use
            payload["reducer_cls"] = self.reducer.__class__.__name__
            resp = self.client.submit_standalone_task(
                task_type=TaskType.REDUCE.value, payload=payload)
            task_ids.append(resp["task_id"])
        return task_ids

    # ── Polling helper ────────────────────────────────────────────────────────

    def _wait_for_tasks(self, task_ids: List[str],
                        stage: str = "") -> List[Any]:
        """
        Poll the master until every task in task_ids is terminal
        (COMPLETED or FAILED).  Returns a list of results in the same
        order as task_ids.  Raises RuntimeError if any task failed.
        """
        results   = [None] * len(task_ids)
        pending   = set(task_ids)
        id_to_idx = {tid: i for i, tid in enumerate(task_ids)}
        failed    = []

        while pending:
            still_pending = set()
            for tid in pending:
                status = self.client.get_task_status(tid)
                if status["status"] == "COMPLETED":
                    results[id_to_idx[tid]] = status["result"]
                elif status["status"] == "FAILED":
                    failed.append((tid, status.get("error", "unknown")))
                else:
                    still_pending.add(tid)
            pending = still_pending
            if pending:
                time.sleep(self.poll_interval)

        if failed:
            msgs = "; ".join(f"{tid[:8]}…: {err}" for tid, err in failed)
            raise RuntimeError(f"{stage} tasks failed — {msgs}")

        done   = sum(1 for r in results if r is not None)
        log.info(f"{stage} stage complete — {done}/{len(task_ids)} tasks OK")
        return results
