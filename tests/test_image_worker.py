# tests/test_image_worker.py
# Unit tests for Phase 3 worker MAP dispatch with ImageMapper payloads.
# MasterClient is mocked — no HTTP calls.

import sys, os, unittest, tempfile, shutil
from unittest.mock import MagicMock, patch
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from PIL import Image
from core.worker import Worker


def make_worker():
    with patch("core.worker.MasterClient") as MC:
        mc = MagicMock()
        MC.return_value = mc
        w = Worker("test-worker", "127.0.0.1", 5100)
        w.client = mc
        return w


def _make_image(path, color=(100,150,200)):
    Image.new("RGB", (60,40), color).save(path)


class TestWorkerImageMap(unittest.TestCase):

    def setUp(self):
        self.w          = make_worker()
        self.input_dir  = tempfile.mkdtemp(prefix="wtest_in_")
        self.output_dir = tempfile.mkdtemp(prefix="wtest_out_")
        self.img_path   = os.path.join(self.input_dir, "test.jpg")
        _make_image(self.img_path)

    def tearDown(self):
        shutil.rmtree(self.input_dir,  ignore_errors=True)
        shutil.rmtree(self.output_dir, ignore_errors=True)

    def _map_payload(self, transform, params=None):
        return {
            "chunk_index"     : 0,
            "image_paths"     : [self.img_path],
            "transform"       : transform,
            "transform_params": params or {},
            "output_dir"      : self.output_dir,
            "mapper_cls"      : "ImageMapper",
        }

    def test_grayscale_map_task(self):
        result = self.w._execute("MAP", self._map_payload("grayscale"))
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        key, val = result[0]
        self.assertEqual(key, "grayscale")
        self.assertTrue(os.path.exists(val))

    def test_edge_detect_map_task(self):
        result = self.w._execute("MAP", self._map_payload("edge_detect"))
        key, val = result[0]
        self.assertEqual(key, "edge_detect")

    def test_thumbnail_map_task(self):
        result = self.w._execute("MAP", self._map_payload("thumbnail", {"size":32}))
        key, val = result[0]
        self.assertEqual(key, "thumbnail")
        img = Image.open(val)
        self.assertLessEqual(img.size[0], 32)
        self.assertLessEqual(img.size[1], 32)

    def test_brightness_with_param(self):
        result = self.w._execute("MAP", self._map_payload("brightness", {"factor":2.0}))
        key, _ = result[0]
        self.assertEqual(key, "brightness")

    def test_all_transforms_via_worker(self):
        from jobs.image_processing.transforms import list_transforms
        for t in list_transforms():
            out = tempfile.mkdtemp()
            try:
                payload = {
                    "chunk_index":0, "image_paths":[self.img_path],
                    "transform":t, "transform_params":{},
                    "output_dir":out, "mapper_cls":"ImageMapper",
                }
                result = self.w._execute("MAP", payload)
                self.assertTrue(len(result) >= 1,
                                f"Transform {t!r} produced no output")
            finally:
                shutil.rmtree(out, ignore_errors=True)

    def test_invalid_image_emits_error(self):
        payload = {
            "chunk_index":0, "image_paths":["/bad/path.jpg"],
            "transform":"grayscale", "transform_params":{},
            "output_dir":self.output_dir, "mapper_cls":"ImageMapper",
        }
        result = self.w._execute("MAP", payload)
        key, _ = result[0]
        self.assertIn("error", key)

    def test_handle_task_calls_submit_result(self):
        task = {
            "task_id"  : "tid-img-1",
            "task_type": "MAP",
            "payload"  : self._map_payload("grayscale"),
        }
        self.w._handle_task(task)
        self.w.client.submit_result.assert_called_once()

    def test_image_reduce_via_worker(self):
        result = self.w._execute("REDUCE", {
            "key"        : "grayscale",
            "values"     : ["/a.jpg","/b.jpg"],
            "reducer_cls": "ImageReducer",
        })
        self.assertEqual(len(result), 1)
        key, val = result[0]
        self.assertEqual(key, "grayscale")
        self.assertEqual(val["count"], 2)


if __name__ == "__main__": unittest.main()
