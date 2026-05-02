# mapreduce/reducer.py
# Base Reducer class.  Subclasses override reduce() to implement the
# aggregation logic for a specific job.

from typing import Any, Generator, List, Tuple


class Reducer:
    """
    Base class for all Reduce functions.

    Subclasses must implement:
        reduce(key, values) -> Generator of (output_key, output_value)

    The Reduce phase receives every unique intermediate key together with
    ALL values that the Map phase emitted for that key (after shuffling).
    """

    def reduce(self, key: Any,
               values: List[Any]) -> Generator[Tuple[Any, Any], None, None]:
        """
        Aggregate a list of values for one key into zero or more
        (output_key, output_value) pairs.

        Must be a generator — use `yield` to emit output pairs.
        Example:
            def reduce(self, key, values):
                yield key, sum(values)
        """
        raise NotImplementedError("Subclasses must implement reduce()")

    # ── Apply reduce to a grouped payload (called inside worker) ─────────────

    def apply(self, payload: dict) -> List[Tuple[Any, Any]]:
        """
        Unwrap the payload dict produced by Shuffler.prepare_reduce_payloads()
        and run reduce().

        Args:
            payload: {"key": k, "values": [v1, v2, …]}

        Returns:
            List of (output_key, output_value) tuples.
        """
        key    = payload["key"]
        values = payload["values"]
        return list(self.reduce(key, values))

    # ── Collect final output ──────────────────────────────────────────────────

    @staticmethod
    def collect(reduce_results: List[List[Tuple]]) -> dict:
        """
        Merge multiple Reduce task results into one final output dict.

        Args:
            reduce_results: Each element is the result of one REDUCE task —
                            a list of (output_key, output_value) tuples.

        Returns:
            Plain dict {output_key: output_value} ready to display or write.
        """
        final = {}
        for task_result in reduce_results:
            if task_result:
                for k, v in task_result:
                    # If the same key appears across reduce tasks, sum it.
                    if k in final and isinstance(final[k], (int, float)) \
                            and isinstance(v, (int, float)):
                        final[k] += v
                    else:
                        final[k] = v
        return dict(sorted(final.items(), key=lambda x: str(x[0])))
