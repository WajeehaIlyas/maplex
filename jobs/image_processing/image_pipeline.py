# jobs/image_processing/image_pipeline.py
# Specialised pipeline for distributed image processing.
#
# Unlike the text Pipeline, image tasks carry file paths not text lines.
# The pipeline:
#   1. Discovers all images in an input directory (or accepts a list)
#   2. Splits them into chunks → one MAP task per chunk
#   3. Each MAP task worker opens images, applies the transform, saves output
#   4. Shuffle groups results by transform name
#   5. REDUCE tasks collect output paths into a summary report
#   6. Writes a human-readable summary report to outputs/

import os
import time
import logging
import glob
from typing import List, Dict, Any

from communication.client import MasterClient
from mapreduce.shuffler   import Shuffler
from mapreduce.reducer    import Reducer
from core.task import TaskType
import config

log = logging.getLogger("image_pipeline")

# Supported image extensions
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".tiff", ".webp"}


class ImagePipeline:
    """
    Runs a single image transform as a MapReduce job.

    Usage:
        pipeline = ImagePipeline(client=MasterClient())
        report   = pipeline.run(
            image_paths    = ["img1.jpg", "img2.jpg", ...],
            transform      = "grayscale",
            transform_params = {},
            output_dir     = "data/outputs/images",
        )
    """

    def __init__(self, client: MasterClient = None,
                 chunk_size: int = None,
                 poll_interval: float = None):
        self.client        = client or MasterClient()
        self.chunk_size    = chunk_size    or config.CHUNK_SIZE
        self.poll_interval = poll_interval or config.POLL_INTERVAL

    # ── Public entry point ────────────────────────────────────────────────────

    def run(self, image_paths: List[str], transform: str,
            transform_params: dict = None,
            output_dir: str = None) -> Dict[str, Any]:
        """
        Run a distributed image processing job.

        Args:
            image_paths:       List of absolute/relative image file paths.
            transform:         Name of the transform to apply.
            transform_params:  Optional dict of extra params for the transform.
            output_dir:        Where processed images are saved.

        Returns:
            Report dict: {
              transform:        {"count": N, "paths": [...]},
              transform_error:  {"count": M, "errors": [...]}  (if any failed)
            }
        """
        output_dir = output_dir or os.path.join(config.OUTPUT_DIR, "images")
        os.makedirs(output_dir, exist_ok=True)

        log.info(f"ImagePipeline: {len(image_paths)} images, "
                 f"transform={transform!r}, output={output_dir}")

        # ── MAP ───────────────────────────────────────────────────────────────
        chunks       = self._split_images(image_paths)
        log.info(f"Split into {len(chunks)} chunks")
        map_task_ids = self._submit_map_tasks(
            chunks, transform, transform_params or {}, output_dir)
        map_results  = self._wait_for_tasks(map_task_ids, "MAP")

        # ── SHUFFLE ───────────────────────────────────────────────────────────
        flat    = Shuffler.flatten_map_results(map_results)
        grouped = Shuffler.shuffle(flat)
        log.info(f"Shuffle: {len(grouped)} key(s) — {list(grouped.keys())}")

        # ── REDUCE ────────────────────────────────────────────────────────────
        payloads         = Shuffler.prepare_reduce_payloads(grouped)
        reduce_task_ids  = self._submit_reduce_tasks(payloads)
        reduce_results   = self._wait_for_tasks(reduce_task_ids, "REDUCE")

        # ── Collect & report ──────────────────────────────────────────────────
        from jobs.image_processing.reducer import ImageReducer
        report = ImageReducer.collect_image_results(reduce_results)
        self._print_report(report, transform)
        self._save_report(report, transform, output_dir)
        return report

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _split_images(self, paths: List[str]) -> List[List[str]]:
        chunks = []
        for i in range(0, len(paths), self.chunk_size):
            chunk = paths[i: i + self.chunk_size]
            if chunk:
                chunks.append(chunk)
        return chunks

    def _submit_map_tasks(self, chunks, transform,
                          transform_params, output_dir) -> List[str]:
        ids = []
        for idx, chunk in enumerate(chunks):
            payload = {
                "chunk_index"     : idx,
                "image_paths"     : chunk,
                "transform"       : transform,
                "transform_params": transform_params,
                "output_dir"      : output_dir,
                "mapper_cls"      : "ImageMapper",
            }
            resp = self.client.submit_standalone_task(
                task_type=TaskType.MAP.value, payload=payload)
            ids.append(resp["task_id"])
        return ids

    def _submit_reduce_tasks(self, payloads) -> List[str]:
        ids = []
        for p in payloads:
            p["reducer_cls"] = "ImageReducer"
            resp = self.client.submit_standalone_task(
                task_type=TaskType.REDUCE.value, payload=p)
            ids.append(resp["task_id"])
        return ids

    def _wait_for_tasks(self, task_ids: List[str],
                        stage: str = "") -> List[Any]:
        results   = [None] * len(task_ids)
        pending   = set(task_ids)
        id_to_idx = {tid: i for i, tid in enumerate(task_ids)}

        while pending:
            still_pending = set()
            for tid in list(pending):
                s = self.client.get_task_status(tid)
                if s["status"] == "COMPLETED":
                    results[id_to_idx[tid]] = s["result"]
                elif s["status"] == "FAILED":
                    log.error(f"{stage} task {tid[:8]}… failed: {s.get('error')}")
                    results[id_to_idx[tid]] = []
                else:
                    still_pending.add(tid)
            pending = still_pending
            if pending:
                time.sleep(self.poll_interval)

        log.info(f"{stage} complete — {len(task_ids)} tasks")
        return results

    # ── Reporting ─────────────────────────────────────────────────────────────

    def _print_report(self, report: dict, transform: str):
        print(f"\n[IMAGE] Processing report — transform: {transform!r}")
        print(f"  {'KEY':<25} {'COUNT':>6}  DETAIL")
        print(f"  {'-'*25} {'-'*6}  {'-'*40}")
        for key, val in sorted(report.items()):
            count = val.get("count", 0)
            if "paths" in val:
                detail = val["paths"][0] if val["paths"] else ""
                if len(val["paths"]) > 1:
                    detail += f"  (+{len(val['paths'])-1} more)"
            else:
                detail = val.get("errors", [""])[0]
            print(f"  {key:<25} {count:>6}  {detail}")

    def _save_report(self, report: dict, transform: str, output_dir: str):
        report_path = os.path.join(output_dir, f"report_{transform}.txt")
        with open(report_path, "w") as f:
            f.write(f"Image Processing Report\n")
            f.write(f"Transform: {transform}\n")
            f.write(f"{'='*50}\n\n")
            for key, val in sorted(report.items()):
                f.write(f"{key}: {val['count']} image(s)\n")
                if "paths" in val:
                    for p in val["paths"]:
                        f.write(f"  -> {p}\n")
                if "errors" in val:
                    for e in val["errors"]:
                        f.write(f"  !! {e}\n")
                f.write("\n")
        log.info(f"Report saved: {report_path}")
        print(f"[IMAGE] Report saved to {report_path}")


# ── Utility: discover images in a directory ───────────────────────────────────

def discover_images(directory: str) -> List[str]:
    """
    Walk a directory and return all image file paths found.
    """
    paths = []
    for root, _, files in os.walk(directory):
        for fname in sorted(files):
            ext = os.path.splitext(fname)[1].lower()
            if ext in IMAGE_EXTENSIONS:
                paths.append(os.path.join(root, fname))
    return paths
