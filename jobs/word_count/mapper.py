# jobs/word_count/mapper.py
# Word-count Map function.
# Input : one line of text
# Output: (word, 1) for every token in the line

import re
from typing import Any, Generator, Tuple
from mapreduce.mapper import Mapper


class WordCountMapper(Mapper):
    """
    Tokenises each line into lowercase words and emits (word, 1) per token.

    Non-alphanumeric characters are stripped so "Hello," and "hello" both
    map to the same key "hello".
    """

    # Only keep sequences of letters/digits; discard punctuation and numbers
    _WORD_RE = re.compile(r"[a-zA-Z]+")

    def map(self, key: Any, value: str) -> Generator[Tuple[str, int], None, None]:
        """
        Args:
            key:   (chunk_index, line_offset) — not used for word count
            value: A single text line

        Yields:
            (word, 1) for each lowercase word found in the line
        """
        for match in self._WORD_RE.finditer(value):
            word = match.group().lower()
            if len(word) > 1:          # skip single-letter noise
                yield word, 1
