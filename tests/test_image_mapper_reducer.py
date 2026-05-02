# tests/test_image_mapper_reducer.py
# Unit tests for ImageMapper and ImageReducer.
# Creates real temporary images on disk, runs the mapper, checks outputs.

import sys, os, unittest, tempfile, shutil
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from PIL import Image
from jobs.image_processing.mapper  import ImageMapper
from jobs.image_processing.reducer import ImageReducer


def _make_test_image(path, w=60, h=40, color=(100, 150, 200)):
    img = Image.new("RGB", (w, h), color)
    img.save(path)


class TestImageMapper(unittest.TestCase):

    def setUp(self):
        # Temp dirs for input and output images
        self.input_dir  = tempfile.mkdtemp(prefix="imgtest_in_")
        self.output_dir = tempfile.mkdtemp(prefix="imgtest_out_")
        self.mapper     = ImageMapper()

        # Create 3 test images
        self.paths = []
        for i, color in enumerate([(200,50,50),(50,200,50),(50,50,200)]):
            p = os.path.join(self.input_dir, f"test_{i}.jpg")
            _make_test_image(p, color=color)
            self.paths.append(p)

    def tearDown(self):
        shutil.rmtree(self.input_dir,  ignore_errors=True)
        shutil.rmtree(self.output_dir, ignore_errors=True)

    # ── apply_chunk tests ─────────────────────────────────────────────────────

    def test_grayscale_emits_output_paths(self):
        pairs = self.mapper.apply_chunk(
            chunk_index=0, image_paths=self.paths,
            transform="grayscale", transform_params={},
            output_dir=self.output_dir)
        keys = [k for k,v in pairs]
        self.assertTrue(all(k == "grayscale" for k in keys))

    def test_output_files_created(self):
        self.mapper.apply_chunk(
            chunk_index=0, image_paths=self.paths,
            transform="grayscale", transform_params={},
            output_dir=self.output_dir)
        files = os.listdir(self.output_dir)
        self.assertEqual(len(files), 3)

    def test_output_filename_contains_transform(self):
        pairs = self.mapper.apply_chunk(
            chunk_index=0, image_paths=[self.paths[0]],
            transform="blur", transform_params={"radius": 1.0},
            output_dir=self.output_dir)
        _, out_path = pairs[0]
        self.assertIn("blur", os.path.basename(out_path))

    def test_output_is_valid_image(self):
        pairs = self.mapper.apply_chunk(
            chunk_index=0, image_paths=[self.paths[0]],
            transform="grayscale", transform_params={},
            output_dir=self.output_dir)
        _, out_path = pairs[0]
        img = Image.open(out_path)
        self.assertEqual(img.mode, "RGB")

    def test_grayscale_output_is_desaturated(self):
        pairs = self.mapper.apply_chunk(
            chunk_index=0, image_paths=[self.paths[0]],
            transform="grayscale", transform_params={},
            output_dir=self.output_dir)
        _, out_path = pairs[0]
        img = Image.open(out_path)
        px  = img.getpixel((0, 0))
        self.assertEqual(px[0], px[1])
        self.assertEqual(px[1], px[2])

    def test_invalid_path_emits_error_key(self):
        pairs = self.mapper.apply_chunk(
            chunk_index=0, image_paths=["/nonexistent/fake.jpg"],
            transform="grayscale", transform_params={},
            output_dir=self.output_dir)
        self.assertEqual(len(pairs), 1)
        key, _ = pairs[0]
        self.assertIn("error", key)

    def test_multiple_transforms(self):
        for transform in ("brightness","edge_detect","sepia","thumbnail"):
            out = tempfile.mkdtemp()
            try:
                pairs = self.mapper.apply_chunk(
                    chunk_index=0, image_paths=[self.paths[0]],
                    transform=transform, transform_params={},
                    output_dir=out)
                self.assertTrue(len(pairs) > 0, f"{transform} produced no pairs")
                key, _ = pairs[0]
                self.assertEqual(key, transform)
            finally:
                shutil.rmtree(out, ignore_errors=True)

    def test_all_images_in_chunk_processed(self):
        pairs = self.mapper.apply_chunk(
            chunk_index=0, image_paths=self.paths,
            transform="invert", transform_params={},
            output_dir=self.output_dir)
        success = [p for k,p in pairs if "error" not in k]
        self.assertEqual(len(success), 3)


class TestImageReducer(unittest.TestCase):

    def setUp(self):
        self.r = ImageReducer()

    def test_success_key_returns_count_and_paths(self):
        result = self.r.apply({
            "key"        : "grayscale",
            "values"     : ["/out/a.jpg", "/out/b.jpg"],
            "reducer_cls": "ImageReducer",
        })
        self.assertEqual(len(result), 1)
        key, val = result[0]
        self.assertEqual(key, "grayscale")
        self.assertEqual(val["count"], 2)
        self.assertIn("/out/a.jpg", val["paths"])

    def test_error_key_returns_count_and_errors(self):
        result = self.r.apply({
            "key"        : "grayscale_error",
            "values"     : ["file not found", "permission denied"],
            "reducer_cls": "ImageReducer",
        })
        key, val = result[0]
        self.assertIn("error", key)
        self.assertEqual(val["count"], 2)
        self.assertIn("file not found", val["errors"])

    def test_paths_sorted(self):
        result = self.r.apply({
            "key":    "blur",
            "values": ["/z.jpg", "/a.jpg", "/m.jpg"],
            "reducer_cls": "ImageReducer",
        })
        _, val = result[0]
        self.assertEqual(val["paths"], sorted(["/z.jpg","/a.jpg","/m.jpg"]))

    def test_collect_image_results_merges(self):
        results = [
            [["grayscale", {"count":2, "paths":["/a.jpg","/b.jpg"]}]],
            [["grayscale", {"count":1, "paths":["/c.jpg"]}]],
        ]
        final = ImageReducer.collect_image_results(results)
        self.assertEqual(final["grayscale"]["count"], 3)
        self.assertIn("/c.jpg", final["grayscale"]["paths"])

    def test_collect_handles_none(self):
        results = [ [["grayscale", {"count":1,"paths":["/a.jpg"]}]], None ]
        final = ImageReducer.collect_image_results(results)
        self.assertIn("grayscale", final)

    def test_collect_empty(self):
        self.assertEqual(ImageReducer.collect_image_results([]), {})


if __name__ == "__main__": unittest.main()
