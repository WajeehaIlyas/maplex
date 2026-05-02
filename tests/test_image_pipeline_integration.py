# tests/test_image_pipeline_integration.py
# Integration tests for the full image processing pipeline.
# Uses Flask test client + in-process worker execution.

import sys, os, unittest, tempfile, shutil, time
from unittest.mock import MagicMock, patch
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from PIL import Image
from communication.server import app, master as _master
from core.worker import Worker


def _reset_master():
    _master.workers.clear(); _master.tasks.clear()
    _master.jobs.clear();    _master.task_queue.clear()


def _make_image(path, w=60, h=40, color=(100,150,200)):
    Image.new("RGB", (w,h), color).save(path)


def _drain_tasks(flask_client, worker_id="w0", max_rounds=300):
    with patch("core.worker.MasterClient") as MC:
        mc = MagicMock(); MC.return_value = mc
        w = Worker(worker_id, "127.0.0.1", 5100)
        w.client = mc
    idle = 0
    for _ in range(max_rounds):
        resp = flask_client.get(f"/worker/task?worker_id={worker_id}")
        task = resp.get_json()
        if task is None:
            idle += 1
            if idle >= 2: break
            time.sleep(0.02); continue
        idle = 0
        try:
            result = w._execute(task["task_type"], task["payload"])
            flask_client.post("/worker/result", json={
                "task_id":task["task_id"],"result":result,"success":True})
        except Exception as exc:
            flask_client.post("/worker/result", json={
                "task_id":task["task_id"],"result":None,
                "success":False,"error":str(exc)})


class TestImagePipelineIntegration(unittest.TestCase):

    def setUp(self):
        _reset_master()
        app.config["TESTING"] = True
        self.c = app.test_client()
        self.c.post("/worker/register",
                    json={"worker_id":"w0","host":"127.0.0.1","port":5100})

        # Create temp dirs and images
        self.input_dir  = tempfile.mkdtemp(prefix="ipi_in_")
        self.output_dir = tempfile.mkdtemp(prefix="ipi_out_")
        self.img_paths  = []
        colors = [(200,50,50),(50,200,50),(50,50,200),
                  (200,200,50),(50,200,200),(200,50,200)]
        for i, c in enumerate(colors):
            p = os.path.join(self.input_dir, f"img_{i}.jpg")
            _make_image(p, color=c)
            self.img_paths.append(p)

    def tearDown(self):
        shutil.rmtree(self.input_dir,  ignore_errors=True)
        shutil.rmtree(self.output_dir, ignore_errors=True)

    def _run_pipeline(self, transform, params=None, chunk_size=3):
        from mapreduce.mapper   import Mapper
        from mapreduce.shuffler import Shuffler
        from jobs.image_processing.reducer import ImageReducer

        # Split into chunks
        chunks = [self.img_paths[i:i+chunk_size]
                  for i in range(0, len(self.img_paths), chunk_size)]

        # Submit MAP tasks
        map_ids = []
        for idx, chunk in enumerate(chunks):
            r = self.c.post("/task/submit", json={
                "task_type": "MAP",
                "payload"  : {
                    "chunk_index":idx, "image_paths":chunk,
                    "transform":transform,
                    "transform_params": params or {},
                    "output_dir":self.output_dir,
                    "mapper_cls":"ImageMapper",
                },
            })
            map_ids.append(r.get_json()["task_id"])

        _drain_tasks(self.c)

        # Collect MAP results
        map_results = []
        for tid in map_ids:
            s = self.c.get(f"/task/{tid}").get_json()
            map_results.append(s.get("result", []))

        # Shuffle
        from mapreduce.shuffler import Shuffler
        flat    = Shuffler.flatten_map_results(map_results)
        grouped = Shuffler.shuffle(flat)
        payloads= Shuffler.prepare_reduce_payloads(grouped)

        # Submit REDUCE tasks
        reduce_ids = []
        for p in payloads:
            p["reducer_cls"] = "ImageReducer"
            r = self.c.post("/task/submit", json={"task_type":"REDUCE","payload":p})
            reduce_ids.append(r.get_json()["task_id"])

        _drain_tasks(self.c)

        # Collect REDUCE results
        reduce_results = []
        for tid in reduce_ids:
            s = self.c.get(f"/task/{tid}").get_json()
            reduce_results.append(s.get("result",[]))

        return ImageReducer.collect_image_results(reduce_results)

    # ── Per-transform integration tests ──────────────────────────────────────

    def test_grayscale_pipeline(self):
        report = self._run_pipeline("grayscale")
        self.assertIn("grayscale", report)
        self.assertEqual(report["grayscale"]["count"], 6)

    def test_output_files_are_real_images(self):
        report = self._run_pipeline("grayscale")
        for p in report["grayscale"]["paths"]:
            self.assertTrue(os.path.exists(p))
            img = Image.open(p)
            self.assertEqual(img.mode, "RGB")

    def test_grayscale_desaturates(self):
        report = self._run_pipeline("grayscale")
        for p in report["grayscale"]["paths"]:
            img = Image.open(p)
            px  = img.getpixel((0, 0))
            self.assertEqual(px[0], px[1])

    def test_edge_detect_pipeline(self):
        report = self._run_pipeline("edge_detect")
        self.assertIn("edge_detect", report)
        self.assertEqual(report["edge_detect"]["count"], 6)

    def test_thumbnail_pipeline_with_param(self):
        report = self._run_pipeline("thumbnail", {"size": 32})
        self.assertIn("thumbnail", report)
        for p in report["thumbnail"]["paths"]:
            img = Image.open(p)
            self.assertLessEqual(img.size[0], 32)
            self.assertLessEqual(img.size[1], 32)

    def test_brightness_pipeline(self):
        report = self._run_pipeline("brightness", {"factor": 1.8})
        self.assertIn("brightness", report)
        self.assertEqual(report["brightness"]["count"], 6)

    def test_sepia_pipeline(self):
        report = self._run_pipeline("sepia")
        self.assertIn("sepia", report)

    def test_invert_pipeline(self):
        report = self._run_pipeline("invert")
        self.assertIn("invert", report)
        self.assertEqual(report["invert"]["count"], 6)

    def test_multiple_chunks_combined(self):
        # chunk_size=2 → 3 chunks of 2 images each → should still total 6
        report = self._run_pipeline("grayscale", chunk_size=2)
        self.assertEqual(report["grayscale"]["count"], 6)

    def test_single_image_pipeline(self):
        _reset_master()
        app.config["TESTING"] = True
        c = app.test_client()
        c.post("/worker/register",
               json={"worker_id":"w0","host":"127.0.0.1","port":5100})

        r = c.post("/task/submit", json={
            "task_type":"MAP",
            "payload":{
                "chunk_index":0, "image_paths":[self.img_paths[0]],
                "transform":"blur", "transform_params":{"radius":1.0},
                "output_dir":self.output_dir, "mapper_cls":"ImageMapper",
            }
        })
        tid = r.get_json()["task_id"]
        _drain_tasks(c)
        s = c.get(f"/task/{tid}").get_json()
        self.assertEqual(s["status"], "COMPLETED")
        self.assertEqual(len(s["result"]), 1)
        key, val = s["result"][0]
        self.assertEqual(key, "blur")
        self.assertTrue(os.path.exists(val))


class TestDiscoverImages(unittest.TestCase):

    def setUp(self):
        self.d = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.d, ignore_errors=True)

    def test_finds_jpg_images(self):
        from jobs.image_processing.image_pipeline import discover_images
        for i in range(3):
            _make_image(os.path.join(self.d, f"img{i}.jpg"))
        paths = discover_images(self.d)
        self.assertEqual(len(paths), 3)

    def test_ignores_non_image_files(self):
        from jobs.image_processing.image_pipeline import discover_images
        _make_image(os.path.join(self.d, "img.jpg"))
        open(os.path.join(self.d, "notes.txt"), "w").close()
        open(os.path.join(self.d, "data.csv"), "w").close()
        paths = discover_images(self.d)
        self.assertEqual(len(paths), 1)

    def test_empty_dir(self):
        from jobs.image_processing.image_pipeline import discover_images
        self.assertEqual(discover_images(self.d), [])


if __name__ == "__main__": unittest.main()
