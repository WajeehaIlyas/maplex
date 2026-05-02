# mapreduce/mapper.py
# Base Mapper class.  Every concrete job mapper subclasses this and
# overrides map().  The split_into_chunks() helper is used by the
# pipeline to break raw input into task-sized pieces before
# distributing them to workers.

from typing import Any, Generator, List, Tuple
import config


class Mapper:
    """
    Base class for all Map functions.

    Subclasses must implement:
        map(key, value) -> Generator of (intermediate_key, intermediate_value)

    The key passed into map() is typically a line number or chunk index.
    The value is the actual data (a string line, a list of lines, etc.).
    """

    def map(self, key: Any, value: Any) -> Generator[Tuple[Any, Any], None, None]:
        """
        Transform one unit of input into zero or more (key, value) pairs.

        Must be a generator — use `yield` to emit pairs.
        Example:
            def map(self, key, value):
                for word in value.split():
                    yield word.lower(), 1
        """
        raise NotImplementedError("Subclasses must implement map()")

    # ── Input splitting ───────────────────────────────────────────────────────

    @staticmethod
    def split_into_chunks(lines: List[str],
                          chunk_size: int = None) -> List[List[str]]:
        """
        Divide a flat list of lines into chunks of chunk_size lines each.
        Each chunk becomes the payload of one MAP task.

        Args:
            lines:      List of text lines (strings).
            chunk_size: Lines per chunk. Defaults to config.CHUNK_SIZE.

        Returns:
            List of chunks, where each chunk is a List[str].
        """
        chunk_size = chunk_size or config.CHUNK_SIZE
        chunks = []
        for i in range(0, len(lines), chunk_size):
            chunk = lines[i: i + chunk_size]
            if chunk:
                chunks.append(chunk)
        return chunks

    @staticmethod
    def read_file_lines(filepath: str) -> List[str]:
        """
        Read a text file and return a list of non-empty stripped lines.
        """
        with open(filepath, "r", encoding="utf-8") as f:
            return [line.rstrip("\n") for line in f if line.strip()]

    # ── Apply map to a chunk (called inside worker) ───────────────────────────

    def apply(self, chunk_index: int,
              lines: List[str]) -> List[Tuple[Any, Any]]:
        """
        Run map() over every line in a chunk and collect all emitted pairs.

        Args:
            chunk_index: Identifies this chunk (used as the key base).
            lines:       The lines that make up this chunk.

        Returns:
            List of (intermediate_key, intermediate_value) tuples.
        """
        pairs = []
        for line_offset, line in enumerate(lines):
            key = (chunk_index, line_offset)
            for k, v in self.map(key, line):
                pairs.append((k, v))
        return pairs
