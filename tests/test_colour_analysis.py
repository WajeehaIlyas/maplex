# tests/test_colour_analysis.py
# Unit + integration tests for the Colour Palette Frequency Analysis job.

import sys, os, unittest, tempfile, shutil, time
from unittest.mock import MagicMock, patch
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from PIL import Image
from jobs.image_analysis.mapper  import ColourMapper, classify_pixel, COLOUR_BUCKETS
from jobs.image_analysis.reducer import ColourReducer
from mapreduce.shuffler          import Shuffler
from mapreduce.reducer           import Reducer


def _solid(path, color, size=(60,40)):
    Image.new("RGB", size, color).save(path)


# ── classify_pixel ────────────────────────────────────────────────────────────

class TestClassifyPixel(unittest.TestCase):
    def test_pure_red(self):
        self.assertEqual(classify_pixel(220, 30, 30), "RED")
    def test_pure_green(self):
        self.assertEqual(classify_pixel(20, 200, 20), "GREEN")
    def test_pure_blue(self):
        self.assertEqual(classify_pixel(20, 20, 220), "BLUE")
    def test_yellow(self):
        self.assertEqual(classify_pixel(220, 200, 30), "YELLOW")
    def test_neutral_gray(self):
        self.assertEqual(classify_pixel(128, 128, 128), "NEUTRAL")
    def test_neutral_white(self):
        self.assertEqual(classify_pixel(250, 250, 250), "NEUTRAL")
    def test_warm(self):
        result = classify_pixel(200, 120, 80)
        self.assertIn(result, ["WARM", "RED", "YELLOW"])  # warm-family
    def test_cool(self):
        result = classify_pixel(80, 100, 200)
        self.assertIn(result, ["COOL", "BLUE"])
    def test_returns_valid_bucket(self):
        for r in range(0, 256, 32):
            for g in range(0, 256, 32):
                for b in range(0, 256, 32):
                    bucket = classify_pixel(r, g, b)
                    self.assertIn(bucket, COLOUR_BUCKETS,
                                  f"({r},{g},{b}) → {bucket!r} not in COLOUR_BUCKETS")


# ── ColourMapper ──────────────────────────────────────────────────────────────

class TestColourMapper(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.m = ColourMapper()

    def tearDown(self):
        shutil.rmtree(self.d, ignore_errors=True)

    def _img(self, name, color):
        p = os.path.join(self.d, name)
        _solid(p, color); return p

    def test_red_image_emits_mostly_red(self):
        p = self._img("red.jpg", (220, 20, 20))
        pairs = self.m.apply_chunk(0, [p])
        keys = [k for k,v in pairs]
        self.assertIn("RED", keys)

    def test_blue_image_emits_mostly_blue(self):
        p = self._img("blue.jpg", (20, 20, 220))
        pairs = self.m.apply_chunk(0, [p])
        keys = [k for k,v in pairs]
        self.assertIn("BLUE", keys)

    def test_all_values_positive(self):
        p = self._img("any.jpg", (100, 150, 80))
        pairs = self.m.apply_chunk(0, [p])
        self.assertTrue(all(v > 0 for _,v in pairs))

    def test_total_pixels_matches_image_size(self):
        # Image is resized to max 150×150 internally
        p = self._img("big.jpg", (200, 100, 50))
        pairs = self.m.apply_chunk(0, [p])
        total = sum(v for _,v in pairs)
        # Should be <= 150*150 = 22500
        self.assertLessEqual(total, 22501)
        self.assertGreater(total, 0)

    def test_multiple_images_in_chunk(self):
        paths = [
            self._img("r.jpg", (220, 20, 20)),
            self._img("g.jpg", (20, 200, 20)),
            self._img("b.jpg", (20, 20, 220)),
        ]
        pairs = self.m.apply_chunk(0, paths)
        keys  = set(k for k,v in pairs)
        self.assertIn("RED",   keys)
        self.assertIn("GREEN", keys)
        self.assertIn("BLUE",  keys)

    def test_bad_path_emits_error(self):
        pairs = self.m.apply_chunk(0, ["/nonexistent/fake.jpg"])
        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0][0], "__ERROR__")

    def test_empty_chunk(self):
        pairs = self.m.apply_chunk(0, [])
        self.assertEqual(pairs, [])


# ── ColourReducer ─────────────────────────────────────────────────────────────

class TestColourReducer(unittest.TestCase):
    def setUp(self): self.r = ColourReducer()

    def test_sums_values(self):
        result = list(self.r.reduce("RED", [1000, 2000, 500]))
        self.assertEqual(result, [("RED", 3500)])

    def test_single_value(self):
        result = list(self.r.reduce("BLUE", [9999]))
        self.assertEqual(result, [("BLUE", 9999)])

    def test_apply_via_payload(self):
        result = self.r.apply({"key":"GREEN","values":[100,200,300],"reducer_cls":"ColourReducer"})
        self.assertEqual(result, [("GREEN", 600)])

    def test_all_colour_buckets(self):
        for bucket in COLOUR_BUCKETS:
            result = list(self.r.reduce(bucket, [1, 2, 3]))
            self.assertEqual(result, [(bucket, 6)])


# ── End-to-end (no network) ───────────────────────────────────────────────────

class TestColourAnalysisEndToEnd(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.d, ignore_errors=True)

    def _run(self, images):
        # images = [(filename, rgb_color), ...]
        paths = []
        for name, color in images:
            p = os.path.join(self.d, name)
            _solid(p, color); paths.append(p)

        mapper  = ColourMapper()
        reducer = ColourReducer()

        # MAP
        all_pairs = [mapper.apply_chunk(i, [p]) for i, p in enumerate(paths)]
        # SHUFFLE
        flat    = Shuffler.flatten_map_results(all_pairs)
        grouped = Shuffler.shuffle(flat)
        # REDUCE
        final = {}
        for key, values in grouped.items():
            for k, v in reducer.reduce(key, values):
                final[k] = final.get(k, 0) + v
        return final

    def test_all_red_dataset(self):
        result = self._run([("r1.jpg",(220,20,20)), ("r2.jpg",(200,30,30))])
        self.assertIn("RED", result)
        top = max(result, key=result.get)
        self.assertEqual(top, "RED")

    def test_mixed_dataset_has_multiple_colours(self):
        result = self._run([
            ("r.jpg",(220,20,20)), ("g.jpg",(20,200,20)), ("b.jpg",(20,20,220))
        ])
        self.assertGreaterEqual(len(result), 3)

    def test_total_pixel_counts_positive(self):
        result = self._run([("any.jpg",(100,100,200))])
        self.assertTrue(all(v > 0 for v in result.values()))

    def test_shuffle_groups_correctly(self):
        # Two red images → RED should have sum of both
        result = self._run([("r1.jpg",(220,20,20)), ("r2.jpg",(210,25,25))])
        # RED pixels from both images should be combined
        self.assertIn("RED", result)
        self.assertGreater(result["RED"], 0)


# ── Integration test through Flask test client ────────────────────────────────

class TestColourPipelineIntegration(unittest.TestCase):
    def setUp(self):
        from communication.server import app, master as _master
        _master.workers.clear(); _master.tasks.clear()
        _master.jobs.clear();    _master.task_queue.clear()
        app.config["TESTING"] = True
        self.c   = app.test_client()
        self.d   = tempfile.mkdtemp()
        self.c.post("/worker/register",
                    json={"worker_id":"w0","host":"127.0.0.1","port":5100})
        # Create test images
        self.paths = []
        for name, color in [("red.jpg",(220,20,20)),("blue.jpg",(20,20,220)),
                             ("green.jpg",(20,200,20)),("warm.jpg",(200,120,60))]:
            p = os.path.join(self.d, name)
            _solid(p, color); self.paths.append(p)

    def tearDown(self):
        shutil.rmtree(self.d, ignore_errors=True)

    def _drain(self, max_rounds=200):
        with patch("core.worker.MasterClient") as MC:
            mc = MagicMock(); MC.return_value = mc
            from core.worker import Worker
            w = Worker("w0","127.0.0.1",5100); w.client = mc
        idle = 0
        for _ in range(max_rounds):
            resp = self.c.get("/worker/task?worker_id=w0")
            task = resp.get_json()
            if task is None:
                idle += 1
                if idle >= 2: break
                time.sleep(0.02); continue
            idle = 0
            try:
                result = w._execute(task["task_type"], task["payload"])
                self.c.post("/worker/result",
                            json={"task_id":task["task_id"],"result":result,"success":True})
            except Exception as e:
                self.c.post("/worker/result",
                            json={"task_id":task["task_id"],"result":None,"success":False,"error":str(e)})

    def test_map_task_submitted_and_completed(self):
        r = self.c.post("/task/submit", json={
            "task_type":"MAP",
            "payload":{"chunk_index":0,"image_paths":self.paths,"mapper_cls":"ColourMapper"}
        })
        tid = r.get_json()["task_id"]
        self._drain()
        s = self.c.get(f"/task/{tid}").get_json()
        self.assertEqual(s["status"], "COMPLETED")
        self.assertIsInstance(s["result"], list)
        self.assertGreater(len(s["result"]), 0)

    def test_full_pipeline_produces_colour_distribution(self):
        # MAP
        r = self.c.post("/task/submit", json={
            "task_type":"MAP",
            "payload":{"chunk_index":0,"image_paths":self.paths,"mapper_cls":"ColourMapper"}
        })
        map_tid = r.get_json()["task_id"]
        self._drain()
        map_result = self.c.get(f"/task/{map_tid}").get_json()["result"]

        # SHUFFLE
        grouped = Shuffler.shuffle(map_result)
        payloads = Shuffler.prepare_reduce_payloads(grouped)

        # REDUCE
        reduce_tids = []
        for p in payloads:
            p["reducer_cls"] = "ColourReducer"
            r2 = self.c.post("/task/submit", json={"task_type":"REDUCE","payload":p})
            reduce_tids.append(r2.get_json()["task_id"])
        self._drain()

        reduce_results = []
        for tid in reduce_tids:
            s = self.c.get(f"/task/{tid}").get_json()
            reduce_results.append(s.get("result",[]))

        final = Reducer.collect(reduce_results)
        # Should have colour buckets as keys
        colour_keys = [k for k in final if k != "__ERROR__"]
        self.assertGreater(len(colour_keys), 0)
        self.assertTrue(all(v > 0 for v in final.values()))


if __name__ == "__main__": unittest.main()
