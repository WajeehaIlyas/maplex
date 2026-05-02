# jobs/image_processing/mapper.py
# Image-processing Map function.
#
# Unlike text jobs, the "input" here is a list of image file paths.
# The mapper receives a chunk of paths + the transform to apply, opens
# each image, applies the transform, saves the result, and emits:
#
#   (transform_name, output_path)   — on success
#   (transform_name + "_error", 1)  — on failure (so errors are counted too)
#
# This means the Reducer can build a summary report:
#   grayscale  → ["/path/out1.jpg", "/path/out2.jpg", …]
#   grayscale_error → 2   (if 2 images failed)

import os
from typing import Any, Generator, Tuple
from PIL import Image

from mapreduce.mapper import Mapper
from jobs.image_processing.transforms import apply_transform
import config


class ImageMapper(Mapper):
    """
    Processes a chunk of image file paths with a given transform.

    Payload schema (set by the pipeline):
        {
          "chunk_index"    : int,
          "image_paths"    : List[str],
          "transform"      : str,        e.g. "grayscale"
          "transform_params": dict,      optional extra params
          "output_dir"     : str         where to save processed images
        }

    The map() method is called once per image path in the chunk.
    key   = (chunk_index, image_index)
    value = image file path
    """

    def map(self, key: Any,
            value: str) -> Generator[Tuple[str, Any], None, None]:
        """
        value is a single image file path.
        Reads the image, applies the transform, saves it, emits result.
        """
        # These are set by apply_chunk() before map() is called
        transform      = getattr(self, "_transform",       "grayscale")
        transform_params = getattr(self, "_transform_params", {})
        output_dir     = getattr(self, "_output_dir",
                                 config.OUTPUT_DIR + "/images")

        image_path = value
        try:
            img = Image.open(image_path).convert("RGB")
            out = apply_transform(img, transform, transform_params)

            # Build output filename: original_name__transform.ext
            basename = os.path.basename(image_path)
            name, ext = os.path.splitext(basename)
            out_filename = f"{name}__{transform}{ext}"
            out_path     = os.path.join(output_dir, out_filename)

            os.makedirs(output_dir, exist_ok=True)
            out.save(out_path)

            yield transform, out_path

        except Exception as exc:
            # Emit an error count so the reducer can report failures
            yield f"{transform}_error", str(exc)

    def apply_chunk(self, chunk_index: int, image_paths: list,
                    transform: str, transform_params: dict,
                    output_dir: str) -> list:
        """
        Custom apply method for image processing.
        Sets transform context then calls the parent apply() logic.
        """
        self._transform       = transform
        self._transform_params = transform_params or {}
        self._output_dir      = output_dir
        return self.apply(chunk_index, image_paths)
