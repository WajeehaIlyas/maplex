# jobs/image_analysis/analysis_pipeline.py
# Orchestrates the Colour Palette Frequency Analysis MapReduce job.
#
# Unlike the image_processing pipeline (which applies transforms and saves files),
# this pipeline ANALYSES image content and produces a statistical report:
#
#   Input : a directory of images (or a list of paths)
#   Output: {
#     "RED":     {"total_pixels": 87420,  "percentage": 18.2, "rank": 2},
#     "BLUE":    {"total_pixels": 143200, "percentage": 29.8, "rank": 1},
#     "GREEN":   {"total_pixels": 62800,  "percentage": 13.1, "rank": 3},
#     ...
#   }
#
# Each stage runs as distributed tasks on the master/worker cluster:
#   MAP    — workers classify pixels in parallel (one task per image chunk)
#   SHUFFLE— master groups all "RED" counts from all images together
#   REDUCE — workers sum per-colour totals (one task per colour bucket)

import os
import time
import logging
from typing import List, Dict, Any

from communication.client import MasterClient
from mapreduce.shuffler   import Shuffler
from mapreduce.reducer    import Reducer
from core.task import TaskType
import config

log = logging.getLogger("colour_pipeline")

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".tiff", ".webp"}

# Display order and emoji for the report
COLOUR_DISPLAY = {
    "RED"    : ("🔴", "Reds / Crimsons"),
    "GREEN"  : ("🟢", "Greens / Foliage"),
    "BLUE"   : ("🔵", "Blues / Sky"),
    "YELLOW" : ("🟡", "Yellows / Oranges"),
    "WARM"   : ("🟠", "Warm tones (skin, amber)"),
    "COOL"   : ("🟣", "Cool tones (cyan, violet)"),
    "NEUTRAL": ("⚪", "Neutrals (gray, white, black)"),
    "__ERROR__": ("❌", "Processing errors"),
}


class ImageAnalysisPipeline:
    """
    Runs the colour frequency analysis as a MapReduce job.

    Usage:
        pipeline = ImageAnalysisPipeline(client=MasterClient())
        report   = pipeline.run(image_paths)
        pipeline.print_report(report)
    """

    def __init__(self, client: MasterClient = None,
                 chunk_size: int = None,
                 poll_interval: float = None):
        self.client        = client or MasterClient()
        self.poll_interval = poll_interval or config.POLL_INTERVAL

        # Adaptive chunk size: query master for worker count,
        # then size chunks so each worker gets ~2 tasks.
        # Falls back to config.CHUNK_SIZE if master unreachable.
        self.chunk_size = chunk_size or self._adaptive_chunk_size()

    def _adaptive_chunk_size(self) -> int:
        """
        Compute chunk size based on number of registered workers.
        More workers → larger chunks (fewer round trips, better throughput).
        Fewer workers → smaller chunks (better load balancing).
        """
        try:
            stats = self.client.system_status()
            n_workers = max(1, stats.get("workers", 1))
            # Target: each worker gets ~2 map tasks
            # chunk_size = total images / (workers * 2)
            # We don't know total yet so we use worker count as a proxy
            adaptive = max(2, n_workers * 2)
            log.info(f"Adaptive chunk size: {adaptive} "
                     f"(based on {n_workers} registered workers)")
            return adaptive
        except Exception:
            return config.CHUNK_SIZE

    # ── Public entry point ────────────────────────────────────────────────────

    def run(self, image_paths: List[str]) -> Dict[str, Any]:
        """
        Execute the full colour frequency analysis pipeline.

        Args:
            image_paths: List of image file paths to analyse.

        Returns:
            Report dict mapping colour names to stats.
        """
        n = len(image_paths)
        log.info(f"ColourAnalysis: {n} images, chunk_size={self.chunk_size}")
        print(f"\n[COLOUR] Analysing {n} images across distributed workers…")
        print(f"[COLOUR] Chunk size: {self.chunk_size} images/task  "
              f"(adaptive based on worker count)")

        t_start = time.time()

        # ── MAP ───────────────────────────────────────────────────────────────
        chunks = self._split(image_paths)
        n_map  = len(chunks)
        print(f"[COLOUR] MAP phase: {n_map} tasks submitted to master queue…")
        map_ids     = self._submit_map_tasks(chunks)
        map_results = self._wait(map_ids, "MAP")
        t_map = time.time() - t_start
        print(f"[COLOUR] MAP complete in {t_map:.2f}s  "
              f"({sum(len(r) for r in map_results if r)} intermediate pairs)")

        # ── SHUFFLE ───────────────────────────────────────────────────────────
        flat    = Shuffler.flatten_map_results(map_results)
        grouped = Shuffler.shuffle(flat)
        print(f"[COLOUR] SHUFFLE: {len(flat)} pairs → "
              f"{len(grouped)} unique colour buckets")

        # ── REDUCE ────────────────────────────────────────────────────────────
        payloads    = Shuffler.prepare_reduce_payloads(grouped)
        print(f"[COLOUR] REDUCE phase: {len(payloads)} tasks "
              f"(one per colour bucket)…")
        reduce_ids     = self._submit_reduce_tasks(payloads)
        reduce_results = self._wait(reduce_ids, "REDUCE")
        t_total = time.time() - t_start

        # ── Collect ───────────────────────────────────────────────────────────
        raw = Reducer.collect(reduce_results)
        report = self._build_report(raw, n, t_total)

        print(f"[COLOUR] Pipeline complete in {t_total:.2f}s")
        return report

    # ── Task submission ───────────────────────────────────────────────────────

    def _split(self, paths: List[str]) -> List[List[str]]:
        return [paths[i:i+self.chunk_size]
                for i in range(0, len(paths), self.chunk_size)
                if paths[i:i+self.chunk_size]]

    def _submit_map_tasks(self, chunks: List[List[str]]) -> List[str]:
        ids = []
        for idx, chunk in enumerate(chunks):
            payload = {
                "chunk_index": idx,
                "image_paths": chunk,
                "mapper_cls" : "ColourMapper",
            }
            r = self.client.submit_standalone_task(
                task_type=TaskType.MAP.value, payload=payload)
            ids.append(r["task_id"])
        return ids

    def _submit_reduce_tasks(self, payloads: List[dict]) -> List[str]:
        ids = []
        for p in payloads:
            p["reducer_cls"] = "ColourReducer"
            r = self.client.submit_standalone_task(
                task_type=TaskType.REDUCE.value, payload=p)
            ids.append(r["task_id"])
        return ids

    def _wait(self, task_ids: List[str], stage: str) -> List[Any]:
        results   = [None] * len(task_ids)
        pending   = set(task_ids)
        id_to_idx = {tid: i for i, tid in enumerate(task_ids)}
        while pending:
            still = set()
            for tid in list(pending):
                s = self.client.get_task_status(tid)
                st = s["status"]
                if st == "COMPLETED":
                    results[id_to_idx[tid]] = s["result"]
                elif st == "FAILED":
                    log.error(f"{stage} task {tid[:8]}… failed: {s.get('error')}")
                    results[id_to_idx[tid]] = []
                else:
                    still.add(tid)
            pending = still
            if pending:
                time.sleep(self.poll_interval)
        return results

    # ── Report builder ────────────────────────────────────────────────────────

    def _build_report(self, raw: dict, n_images: int,
                      elapsed: float) -> Dict[str, Any]:
        # Remove error key from totals
        errors = raw.pop("__ERROR__", 0)
        total_pixels = sum(raw.values())

        report = {
            "_meta": {
                "images_analysed" : n_images,
                "total_pixels"    : total_pixels,
                "elapsed_seconds" : round(elapsed, 3),
                "processing_errors": errors,
            }
        }
        # Sort by pixel count descending, assign rank
        ranked = sorted(raw.items(), key=lambda x: -x[1])
        for rank, (colour, count) in enumerate(ranked, 1):
            pct = count / total_pixels * 100 if total_pixels else 0
            report[colour] = {
                "total_pixels": count,
                "percentage"  : round(pct, 2),
                "rank"        : rank,
            }
        if errors:
            report["__ERROR__"] = {"total_pixels": errors, "percentage": 0, "rank": 99}
        return report

    # ── Console output ────────────────────────────────────────────────────────

    @staticmethod
    def print_report(report: dict):
        meta = report.get("_meta", {})
        print(f"\n{'═'*58}")
        print(f"  COLOUR PALETTE FREQUENCY ANALYSIS — MAPREDUCE RESULT")
        print(f"{'═'*58}")
        print(f"  Images analysed : {meta.get('images_analysed', '?')}")
        print(f"  Total pixels    : {meta.get('total_pixels', 0):,}")
        print(f"  Pipeline time   : {meta.get('elapsed_seconds', '?')}s")
        if meta.get('processing_errors'):
            print(f"  Errors          : {meta['processing_errors']}")
        print(f"{'─'*58}")
        print(f"  {'RANK':<6}{'COLOUR':<12}{'PIXELS':>12}{'PCT':>8}  BAR")
        print(f"{'─'*58}")

        colours = [(k, v) for k, v in report.items()
                   if k != "_meta" and not k.startswith("__")]
        colours.sort(key=lambda x: x[1].get("rank", 99))

        for colour, stats in colours:
            emoji, label = COLOUR_DISPLAY.get(colour, ("●", colour))
            pct   = stats["percentage"]
            rank  = stats["rank"]
            total = stats["total_pixels"]
            bar   = "█" * int(pct / 2) + "░" * (50 - int(pct / 2))
            bar   = bar[:30]
            print(f"  #{rank:<5}{colour:<12}{total:>12,}{pct:>7.1f}%  {bar}")

        print(f"{'═'*58}")

    @staticmethod
    def save_report(report: dict, output_path: str):
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w") as f:
            meta = report.get("_meta", {})
            f.write("COLOUR PALETTE FREQUENCY ANALYSIS\n")
            f.write(f"MapReduce Job Report\n{'='*50}\n\n")
            f.write(f"Images analysed : {meta.get('images_analysed')}\n")
            f.write(f"Total pixels    : {meta.get('total_pixels', 0):,}\n")
            f.write(f"Pipeline time   : {meta.get('elapsed_seconds')}s\n\n")
            f.write(f"{'RANK':<6}{'COLOUR':<12}{'PIXELS':>12}{'PCT':>8}\n")
            f.write(f"{'-'*40}\n")
            colours = [(k,v) for k,v in report.items()
                       if k != "_meta" and not k.startswith("__")]
            colours.sort(key=lambda x: x[1].get("rank", 99))
            for colour, stats in colours:
                f.write(f"{stats['rank']:<6}{colour:<12}"
                        f"{stats['total_pixels']:>12,}"
                        f"{stats['percentage']:>7.1f}%\n")
        print(f"[COLOUR] Report saved → {output_path}")


def discover_images(directory: str) -> List[str]:
    paths = []
    for root, _, files in os.walk(directory):
        for fname in sorted(files):
            if os.path.splitext(fname)[1].lower() in IMAGE_EXTENSIONS:
                paths.append(os.path.join(root, fname))
    return paths
