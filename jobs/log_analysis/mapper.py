# jobs/log_analysis/mapper.py
# Log-analysis Map function.
# Input : one log line
# Output: (log_level, 1)  e.g. ("ERROR", 1), ("INFO", 1)

from typing import Any, Generator, Tuple
from mapreduce.mapper import Mapper
from jobs.log_analysis.log_parser import parse_line


class LogMapper(Mapper):
    """
    Parses each log line and emits (level, 1).

    Unrecognised lines (comments, blank lines, separators) are silently
    skipped — the generator simply yields nothing for them.
    """

    def map(self, key: Any, value: str) -> Generator[Tuple[str, int], None, None]:
        """
        Args:
            key:   (chunk_index, line_offset) — not used here
            value: A single raw log line

        Yields:
            ("ERROR", 1) / ("INFO", 1) / ("WARNING", 1) etc.
        """
        parsed = parse_line(value)
        if parsed and parsed.get("level"):
            yield parsed["level"], 1
