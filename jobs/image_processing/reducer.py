# jobs/image_processing/reducer.py
# Image-processing Reduce function.
#
# Each reduce call receives:
#   key    = transform name e.g. "grayscale"  OR  "grayscale_error"
#   values = list of output file paths (success) OR error messages (failure)
#
# It emits:
#   (transform_name, {"count": N, "paths": [...]})        — for successes
#   (transform_name + "_error", {"count": N, "errors": [...]}) — for failures

from typing import Any, Generator, List, Tuple
from mapreduce.reducer import Reducer


class ImageReducer(Reducer):
    """
    Aggregates image processing results per transform.

    Final output structure:
        {
          "grayscale":       {"count": 5, "paths": ["out1.jpg", ...]},
          "grayscale_error": {"count": 1, "errors": ["file not found"]},
          ...
        }
    """

    def reduce(self, key: str,
               values: List[Any]) -> Generator[Tuple[str, Any], None, None]:
        """
        Args:
            key:    Transform name or "transform_error"
            values: List of output paths (success) or error strings (failure)

        Yields:
            (key, summary_dict)
        """
        if key.endswith("_error"):
            yield key, {
                "count" : len(values),
                "errors": values,
            }
        else:
            yield key, {
                "count": len(values),
                "paths": sorted(values),   # sorted for deterministic output
            }

    # ── Custom collect for image results ──────────────────────────────────────

    @staticmethod
    def collect_image_results(reduce_results: list) -> dict:
        """
        Flatten reduce task results into a single summary dict.
        Each reduce result is a list of [[key, summary_dict], ...] pairs.
        """
        final = {}
        for task_result in reduce_results:
            if not task_result:
                continue
            for k, v in task_result:
                if k in final and isinstance(final[k], dict) \
                        and isinstance(v, dict):
                    # merge counts and lists
                    final[k]["count"] = final[k].get("count", 0) + v.get("count", 0)
                    if "paths" in v:
                        final[k].setdefault("paths", []).extend(v["paths"])
                    if "errors" in v:
                        final[k].setdefault("errors", []).extend(v["errors"])
                else:
                    final[k] = v
        return final
