# mapreduce/shuffler.py
# The Shuffle phase sits between Map and Reduce.
# It collects all (key, value) pairs emitted by every Map task and
# groups them by key so the Reducer sees all values for the same key
# in one call.
#
# In a real distributed system this involves network transfer (shuffle
# over the wire).  Here it runs in-process on the master after all Map
# tasks complete.

from collections import defaultdict
from typing import Any, Dict, List, Tuple


class Shuffler:
    """
    Groups a flat list of (key, value) pairs into a dict of
        { key: [value1, value2, …] }
    ready to be fed into Reduce tasks.
    """

    @staticmethod
    def shuffle(pairs: List[Tuple[Any, Any]]) -> Dict[Any, List[Any]]:
        """
        Group intermediate pairs by key.

        Args:
            pairs: Flat list of (key, value) tuples from all Map tasks.

        Returns:
            Dict mapping each unique key to a sorted list of its values.
            Sorting is best-effort (skipped gracefully for non-comparable types).
        """
        grouped: Dict[Any, List[Any]] = defaultdict(list)
        for key, value in pairs:
            grouped[key].append(value)

        # Convert to plain dict and sort values where possible
        result = {}
        for key in sorted(grouped.keys(), key=str):   # sort keys lexically
            values = grouped[key]
            try:
                result[key] = sorted(values)
            except TypeError:
                result[key] = values    # values not comparable — leave as-is
        return result

    @staticmethod
    def flatten_map_results(map_results: List[List[Tuple]]) -> List[Tuple]:
        """
        Flatten the list-of-lists returned by multiple Map tasks into a
        single flat list of (key, value) pairs.

        Args:
            map_results: Each element is the result of one MAP task —
                         a list of (key, value) tuples.

        Returns:
            Single flat list of (key, value) tuples.
        """
        flat = []
        for task_result in map_results:
            if task_result:
                flat.extend(task_result)
        return flat

    @staticmethod
    def prepare_reduce_payloads(
            grouped: Dict[Any, List[Any]]
    ) -> List[Dict[str, Any]]:
        """
        Convert the grouped dict into a list of reduce-task payloads.
        Each payload is one dict: {"key": k, "values": [v1, v2, …]}.

        The pipeline creates one REDUCE task per payload entry.
        Payloads are JSON-serialisable so they travel over HTTP safely.
        """
        return [{"key": k, "values": v} for k, v in grouped.items()]
