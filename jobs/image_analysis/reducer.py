# jobs/image_analysis/reducer.py
# Colour Palette Frequency Analysis — Reduce phase.
#
# Receives: (colour_name, [count_from_img1, count_from_img2, ...])
# Emits:    (colour_name, total_pixel_count)
#
# This is where the genuine aggregation happens.
# Each reduce task receives ALL pixel counts for ONE colour bucket,
# collected from EVERY image processed by EVERY worker in the MAP phase.
# Summing them gives the dataset-wide colour distribution.

from typing import Any, Generator, List, Tuple
from mapreduce.reducer import Reducer


class ColourReducer(Reducer):
    """
    Sums pixel counts per colour bucket across the entire image dataset.

    Input:  ("RED",  [12400, 3400, 8900, 2100, ...])   ← one count per image
    Output: ("RED",  27800)                              ← total across dataset
    """

    def reduce(self, key: str,
               values: List[int]) -> Generator[Tuple[str, int], None, None]:
        yield key, sum(values)
