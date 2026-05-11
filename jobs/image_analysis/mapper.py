# jobs/image_analysis/mapper.py
# Colour Palette Frequency Analysis — Map phase.
#
# For each image the mapper:
#   1. Opens the image and reads every pixel
#   2. Classifies each pixel into one of 6 named colour buckets
#   3. Emits (colour_name, pixel_count) for each bucket that has > 0 pixels
#
# This is genuine MapReduce because:
#   - The Map phase is embarrassingly parallel (each image independent)
#   - The Shuffle groups ALL pixel counts for "RED" across ALL images together
#   - The Reduce sums them into a dataset-wide colour distribution
#   - You cannot produce the final answer without the Shuffle + Reduce stages

from typing import Any, Generator, Tuple
from PIL import Image

from mapreduce.mapper import Mapper


# ── Colour classification ─────────────────────────────────────────────────────
#
# Each pixel (R, G, B) is classified by comparing channel dominance.
# Thresholds are chosen to produce meaningful, separable buckets on natural images.

COLOUR_BUCKETS = ["RED", "GREEN", "BLUE", "YELLOW", "WARM", "COOL", "NEUTRAL"]


def classify_pixel(r: int, g: int, b: int) -> str:
    """
    Map one RGB pixel to a named colour bucket.

    Decision logic (order matters — first match wins):
      NEUTRAL : all channels within 30 of each other AND all < 200
      RED     : R strongly dominant (R > G+60 and R > B+60)
      GREEN   : G strongly dominant (G > R+40 and G > B+40)
      BLUE    : B strongly dominant (B > R+40 and B > G+30)
      YELLOW  : R and G both high, B low  (warm yellow/orange)
      WARM    : R > B by 40+ (reds, oranges, skin tones)
      COOL    : B > R by 30+ (blues, purples, cyans)
      NEUTRAL : fallback (grays, whites, mixed)
    """
    mx = max(r, g, b)
    mn = min(r, g, b)
    spread = mx - mn

    # True neutral / gray / white
    if spread < 35:
        return "NEUTRAL"

    if r > g + 55 and r > b + 55:
        return "RED"
    if g > r + 35 and g > b + 35:
        return "GREEN"
    if b > r + 35 and b > g + 25:
        return "BLUE"
    if r > 160 and g > 140 and b < 100:
        return "YELLOW"
    if r > b + 40:
        return "WARM"
    if b > r + 30:
        return "COOL"
    return "NEUTRAL"


class ColourMapper(Mapper):
    """
    Processes a chunk of image file paths.
    For each image: counts pixels per colour bucket.
    Emits: (colour_name, pixel_count)

    Payload schema (from ImageAnalysisPipeline):
        {
          "chunk_index"  : int,
          "image_paths"  : List[str],
          "mapper_cls"   : "ColourMapper"
        }
    """

    def map(self, key: Any, value: str) -> Generator[Tuple[str, int], None, None]:
        """
        value = one image file path.
        Opens the image, classifies every pixel, emits (colour, count).
        """
        image_path = value
        try:
            img = Image.open(image_path).convert("RGB")

            # Process at full resolution for benchmark accuracy.
            # Full resolution = more CPU work per task = visible speedup.
            # For very large images (>1200px) downsample to 800px wide only.
            if img.width > 1200:
                ratio = 800 / img.width
                img = img.resize(
                    (800, int(img.height * ratio)), Image.LANCZOS)

            pixels = list(img.getdata())
            counts = {c: 0 for c in COLOUR_BUCKETS}

            for r, g, b in pixels:
                bucket = classify_pixel(r, g, b)
                counts[bucket] += 1

            for colour, count in counts.items():
                if count > 0:
                    yield colour, count

        except Exception as exc:
            # Emit a special error key so the reducer can report failures
            yield "__ERROR__", 1

    def apply_chunk(self, chunk_index: int, image_paths: list) -> list:
        """Custom apply for image paths (not text lines)."""
        self._chunk_index = chunk_index
        return self.apply(chunk_index, image_paths)
