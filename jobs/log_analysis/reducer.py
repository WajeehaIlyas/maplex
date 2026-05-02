# jobs/log_analysis/reducer.py
# Log-analysis Reduce function.
# Input : ("ERROR", [1, 1, 1, …])
# Output: ("ERROR", total_count)

from typing import Any, Generator, List, Tuple
from mapreduce.reducer import Reducer


class LogReducer(Reducer):
    """
    Sums all counts for a log level and emits (level, total).

    Final output looks like:
        ERROR    → 42
        INFO     → 130
        WARNING  → 17
        DEBUG    → 5
    """

    def reduce(self, key: str,
               values: List[int]) -> Generator[Tuple[str, int], None, None]:
        """
        Args:
            key:    Normalised log level string e.g. "ERROR"
            values: List of 1s, one per matching log line

        Yields:
            (level, total_count)
        """
        yield key, sum(values)
