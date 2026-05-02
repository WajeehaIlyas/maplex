# tests/test_worker_phase2.py
# Unit tests for the Phase 2 worker MAP and REDUCE handlers.
# MasterClient is mocked so no HTTP calls happen.

import sys, os, unittest
from unittest.mock import MagicMock, patch
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.worker import Worker


def make_worker():
    with patch("core.worker.MasterClient") as MC:
        mc = MagicMock()
        MC.return_value = mc
        w = Worker(worker_id="test-worker", host="127.0.0.1", port=5100)
        w.client = mc
        return w


class TestMapExecution(unittest.TestCase):

    def setUp(self):
        self.w = make_worker()

    def test_word_count_map(self):
        payload = {
            "chunk_index": 0,
            "lines"      : ["hello world", "hello python"],
            "mapper_cls" : "WordCountMapper",
        }
        result = self.w._execute("MAP", payload)
        # result is list of [key, value] pairs
        keys = [r[0] for r in result]
        self.assertIn("hello",  keys)
        self.assertIn("world",  keys)
        self.assertIn("python", keys)

    def test_word_count_map_values_are_ones(self):
        payload = {
            "chunk_index": 0,
            "lines"      : ["the cat sat on the mat"],
            "mapper_cls" : "WordCountMapper",
        }
        result = self.w._execute("MAP", payload)
        self.assertTrue(all(v == 1 for _, v in result))

    def test_log_mapper(self):
        payload = {
            "chunk_index": 0,
            "lines"      : ["ERROR disk failure", "INFO user login", "ERROR timeout"],
            "mapper_cls" : "LogMapper",
        }
        result = self.w._execute("MAP", payload)
        keys = [r[0] for r in result]
        self.assertEqual(keys.count("ERROR"), 2)
        self.assertEqual(keys.count("INFO"),  1)

    def test_unknown_mapper_raises(self):
        payload = {
            "chunk_index": 0,
            "lines"      : ["hello"],
            "mapper_cls" : "NonExistentMapper",
        }
        with self.assertRaises(ValueError):
            self.w._execute("MAP", payload)

    def test_empty_lines_returns_empty(self):
        payload = {
            "chunk_index": 0,
            "lines"      : [],
            "mapper_cls" : "WordCountMapper",
        }
        result = self.w._execute("MAP", payload)
        self.assertEqual(result, [])


class TestReduceExecution(unittest.TestCase):

    def setUp(self):
        self.w = make_worker()

    def test_word_count_reduce(self):
        payload = {
            "key"        : "hello",
            "values"     : [1, 1, 1],
            "reducer_cls": "WordCountReducer",
        }
        result = self.w._execute("REDUCE", payload)
        self.assertEqual(result, [["hello", 3]])

    def test_log_reduce(self):
        payload = {
            "key"        : "ERROR",
            "values"     : [1, 1, 1, 1, 1],
            "reducer_cls": "LogReducer",
        }
        result = self.w._execute("REDUCE", payload)
        self.assertEqual(result, [["ERROR", 5]])

    def test_unknown_reducer_raises(self):
        payload = {
            "key"        : "x",
            "values"     : [1],
            "reducer_cls": "GhostReducer",
        }
        with self.assertRaises(ValueError):
            self.w._execute("REDUCE", payload)

    def test_single_value_reduce(self):
        payload = {
            "key"        : "world",
            "values"     : [1],
            "reducer_cls": "WordCountReducer",
        }
        result = self.w._execute("REDUCE", payload)
        self.assertEqual(result, [["world", 1]])


class TestHandleTaskPhase2(unittest.TestCase):
    """Test that _handle_task correctly calls submit_result for MAP/REDUCE."""

    def setUp(self):
        self.w = make_worker()

    def test_handle_map_task_calls_submit_result(self):
        self.w._handle_task({
            "task_id"  : "t1",
            "task_type": "MAP",
            "payload"  : {
                "chunk_index": 0,
                "lines"      : ["hello world"],
                "mapper_cls" : "WordCountMapper",
            },
        })
        self.w.client.submit_result.assert_called_once()
        args = self.w.client.submit_result.call_args
        # success kwarg should be True
        self.assertTrue(args[1].get("success", True))

    def test_handle_reduce_task_calls_submit_result(self):
        self.w._handle_task({
            "task_id"  : "t2",
            "task_type": "REDUCE",
            "payload"  : {
                "key"        : "hello",
                "values"     : [1, 1],
                "reducer_cls": "WordCountReducer",
            },
        })
        self.w.client.submit_result.assert_called_once()


if __name__ == "__main__": unittest.main()
