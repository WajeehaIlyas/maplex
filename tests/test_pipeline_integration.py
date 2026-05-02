# tests/test_pipeline_integration.py
# Integration tests for the full MapReduce pipeline using the real Flask
# server + in-process worker execution (no separate processes).
#
# Strategy: start Flask test client, manually drain task queue in-process
# using the same worker execution logic, then verify final results.

import sys, os, unittest, time, threading
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from communication.server import app, master as _master
from core.worker  import Worker
from core.task    import TaskType, TaskStatus
from unittest.mock import patch, MagicMock


def _reset_master():
    _master.workers.clear()
    _master.tasks.clear()
    _master.jobs.clear()
    _master.task_queue.clear()


def _drain_tasks(flask_client, worker_id="w0", max_rounds=200):
    """
    Simulate a worker: repeatedly GET /worker/task, execute it in-process,
    POST the result — until the queue is empty for two consecutive polls.
    """
    with patch("core.worker.MasterClient") as MC:
        mc = MagicMock()
        MC.return_value = mc
        w = Worker(worker_id=worker_id, host="127.0.0.1", port=5100)
        w.client = mc

    idle = 0
    for _ in range(max_rounds):
        resp = flask_client.get(f"/worker/task?worker_id={worker_id}")
        task = resp.get_json()
        if task is None:
            idle += 1
            if idle >= 2:
                break
            time.sleep(0.05)
            continue
        idle = 0
        try:
            result  = w._execute(task["task_type"], task["payload"])
            flask_client.post("/worker/result", json={
                "task_id": task["task_id"], "result": result, "success": True})
        except Exception as exc:
            flask_client.post("/worker/result", json={
                "task_id": task["task_id"], "result": None,
                "success": False, "error": str(exc)})


class TestWordCountPipelineIntegration(unittest.TestCase):

    def setUp(self):
        _reset_master()
        app.config["TESTING"] = True
        self.c = app.test_client()
        self.c.post("/worker/register",
                    json={"worker_id":"w0","host":"127.0.0.1","port":5100})

    def _submit_and_drain(self, lines, chunk_size=5):
        from mapreduce.mapper   import Mapper
        from mapreduce.shuffler import Shuffler
        from jobs.word_count.mapper  import WordCountMapper
        from jobs.word_count.reducer import WordCountReducer

        mapper  = WordCountMapper()
        reducer = WordCountReducer()

        # Submit MAP tasks
        chunks      = Mapper.split_into_chunks(lines, chunk_size)
        map_task_ids = []
        for idx, chunk in enumerate(chunks):
            r = self.c.post("/task/submit", json={
                "task_type": "MAP",
                "payload"  : {"chunk_index":idx,"lines":chunk,
                              "mapper_cls":"WordCountMapper"},
            })
            map_task_ids.append(r.get_json()["task_id"])

        # Drain MAP tasks
        _drain_tasks(self.c)

        # Collect MAP results & shuffle
        map_results = []
        for tid in map_task_ids:
            s = self.c.get(f"/task/{tid}").get_json()
            map_results.append(s["result"])

        flat    = Shuffler.flatten_map_results(map_results)
        grouped = Shuffler.shuffle(flat)
        payloads= Shuffler.prepare_reduce_payloads(grouped)

        # Submit REDUCE tasks
        reduce_task_ids = []
        for p in payloads:
            p["reducer_cls"] = "WordCountReducer"
            r = self.c.post("/task/submit", json={
                "task_type":"REDUCE", "payload": p})
            reduce_task_ids.append(r.get_json()["task_id"])

        # Drain REDUCE tasks
        _drain_tasks(self.c)

        # Collect results
        from mapreduce.reducer import Reducer
        reduce_results = []
        for tid in reduce_task_ids:
            s = self.c.get(f"/task/{tid}").get_json()
            reduce_results.append(s["result"])
        return Reducer.collect(reduce_results)

    def test_simple_word_count(self):
        lines  = ["hello world hello", "world foo hello"]
        result = self._submit_and_drain(lines)
        self.assertEqual(result.get("hello"), 3)
        self.assertEqual(result.get("world"), 2)
        self.assertEqual(result.get("foo"),   1)

    def test_all_words_counted(self):
        lines  = ["the cat sat on the mat", "the mat is flat"]
        result = self._submit_and_drain(lines)
        self.assertIn("cat", result)
        self.assertIn("mat", result)
        self.assertGreater(result.get("the", 0), 1)

    def test_multiple_chunks(self):
        lines = [f"word{i % 5} extra" for i in range(20)]
        result = self._submit_and_drain(lines, chunk_size=4)
        self.assertIn("extra", result)
        self.assertEqual(result.get("extra"), 20)


class TestLogAnalysisPipelineIntegration(unittest.TestCase):

    def setUp(self):
        _reset_master()
        app.config["TESTING"] = True
        self.c = app.test_client()
        self.c.post("/worker/register",
                    json={"worker_id":"w0","host":"127.0.0.1","port":5100})

    def _submit_and_drain(self, lines, chunk_size=5):
        from mapreduce.mapper   import Mapper
        from mapreduce.shuffler import Shuffler
        from mapreduce.reducer  import Reducer

        chunks       = Mapper.split_into_chunks(lines, chunk_size)
        map_task_ids = []
        for idx, chunk in enumerate(chunks):
            r = self.c.post("/task/submit", json={
                "task_type": "MAP",
                "payload"  : {"chunk_index":idx,"lines":chunk,
                              "mapper_cls":"LogMapper"},
            })
            map_task_ids.append(r.get_json()["task_id"])

        _drain_tasks(self.c)

        map_results = []
        for tid in map_task_ids:
            s = self.c.get(f"/task/{tid}").get_json()
            map_results.append(s["result"])

        flat    = Shuffler.flatten_map_results(map_results)
        grouped = Shuffler.shuffle(flat)
        payloads= Shuffler.prepare_reduce_payloads(grouped)

        reduce_task_ids = []
        for p in payloads:
            p["reducer_cls"] = "LogReducer"
            r = self.c.post("/task/submit", json={
                "task_type":"REDUCE","payload":p})
            reduce_task_ids.append(r.get_json()["task_id"])

        _drain_tasks(self.c)

        reduce_results = []
        for tid in reduce_task_ids:
            s = self.c.get(f"/task/{tid}").get_json()
            reduce_results.append(s["result"])
        return Reducer.collect(reduce_results)

    def test_basic_log_count(self):
        lines  = [
            "ERROR disk failure","ERROR timeout","ERROR db down",
            "INFO  user login","INFO  server started",
            "WARNING high mem",
        ]
        result = self._submit_and_drain(lines)
        self.assertEqual(result.get("ERROR"),   3)
        self.assertEqual(result.get("INFO"),    2)
        self.assertEqual(result.get("WARNING"), 1)

    def test_sum_equals_parseable_lines(self):
        lines  = [
            "ERROR a","INFO b","WARNING c","DEBUG d","CRITICAL e"
        ]
        result = self._submit_and_drain(lines)
        self.assertEqual(sum(result.values()), 5)

    def test_sample_log_file(self):
        logfile = os.path.join(
            os.path.dirname(__file__),"..","data","sample_logs","app.log")
        if not os.path.exists(logfile):
            self.skipTest("sample file missing")
        from mapreduce.mapper import Mapper
        lines  = Mapper.read_file_lines(logfile)
        result = self._submit_and_drain(lines)
        self.assertIn("ERROR", result)
        self.assertIn("INFO",  result)

    def test_timestamped_log_file(self):
        logfile = os.path.join(
            os.path.dirname(__file__),"..","data","sample_logs","server.log")
        if not os.path.exists(logfile):
            self.skipTest("sample file missing")
        from mapreduce.mapper import Mapper
        lines  = Mapper.read_file_lines(logfile)
        result = self._submit_and_drain(lines)
        self.assertIn("ERROR",    result)
        self.assertIn("INFO",     result)
        self.assertIn("CRITICAL", result)


if __name__ == "__main__": unittest.main()
