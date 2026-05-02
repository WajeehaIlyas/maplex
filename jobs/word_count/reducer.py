# jobs/word_count/reducer.py
# Word-count Reduce function.
# Input : (word, [1, 1, 1, …])
# Output: (word, total_count)

from typing import Any, Generator, List, Tuple
from mapreduce.reducer import Reducer


class WordCountReducer(Reducer):
    """
    Sums all counts for a word and emits (word, total).
    """

    def reduce(self, key: str,
               values: List[int]) -> Generator[Tuple[str, int], None, None]:
        """
        Args:
            key:    A lowercase word
            values: List of 1s emitted by the mapper for this word

        Yields:
            (word, total_count)
        """
        yield key, sum(values)
