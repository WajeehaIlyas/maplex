# tests/test_log_analysis.py
# Unit tests for the Log Analysis job — parser, mapper, reducer, end-to-end.

import sys, os, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jobs.log_analysis.log_parser import parse_line
from jobs.log_analysis.mapper     import LogMapper
from jobs.log_analysis.reducer    import LogReducer
from mapreduce.shuffler           import Shuffler


# ── Log parser tests ──────────────────────────────────────────────────────────

class TestLogParser(unittest.TestCase):

    def test_standard_error(self):
        r = parse_line("ERROR disk failure")
        self.assertIsNotNone(r)
        self.assertEqual(r["level"],   "ERROR")
        self.assertEqual(r["message"], "disk failure")

    def test_standard_info(self):
        r = parse_line("INFO user login")
        self.assertEqual(r["level"], "INFO")

    def test_standard_warning(self):
        r = parse_line("WARNING high memory")
        self.assertEqual(r["level"], "WARNING")

    def test_warn_normalised_to_warning(self):
        r = parse_line("WARN something happened")
        self.assertEqual(r["level"], "WARNING")

    def test_fatal_normalised_to_critical(self):
        r = parse_line("FATAL system crash")
        self.assertEqual(r["level"], "CRITICAL")

    def test_timestamped_line(self):
        r = parse_line("2024-01-15 12:34:56 ERROR database failed")
        self.assertIsNotNone(r)
        self.assertEqual(r["level"], "ERROR")
        self.assertEqual(r["ts"],    "2024-01-15 12:34:56")

    def test_bracketed_level(self):
        r = parse_line("[ERROR] something broke")
        self.assertIsNotNone(r)
        self.assertEqual(r["level"], "ERROR")

    def test_apache_200_is_info(self):
        line = '192.168.1.1 - - [15/Jan/2024:00:01:00 +0000] "GET /index HTTP/1.1" 200 1234'
        r = parse_line(line)
        self.assertIsNotNone(r)
        self.assertEqual(r["level"], "INFO")

    def test_apache_500_is_error(self):
        line = '192.168.1.1 - - [15/Jan/2024:00:01:00 +0000] "GET /api HTTP/1.1" 500 0'
        r = parse_line(line)
        self.assertEqual(r["level"], "ERROR")

    def test_apache_404_is_warning(self):
        line = '10.0.0.1 - - [15/Jan/2024:00:01:00 +0000] "GET /missing HTTP/1.1" 404 100'
        r = parse_line(line)
        self.assertEqual(r["level"], "WARNING")

    def test_blank_line_returns_none(self):
        self.assertIsNone(parse_line(""))

    def test_unrecognised_line_returns_none(self):
        self.assertIsNone(parse_line("--- separator ---"))

    def test_case_insensitive(self):
        r = parse_line("error something bad")
        self.assertIsNotNone(r)
        self.assertEqual(r["level"], "ERROR")


# ── Log mapper tests ──────────────────────────────────────────────────────────

class TestLogMapper(unittest.TestCase):

    def setUp(self):
        self.m = LogMapper()

    def test_emits_error(self):
        pairs = list(self.m.map(0, "ERROR disk failure"))
        self.assertEqual(pairs, [("ERROR", 1)])

    def test_emits_info(self):
        pairs = list(self.m.map(0, "INFO user login"))
        self.assertEqual(pairs, [("INFO", 1)])

    def test_blank_line_emits_nothing(self):
        pairs = list(self.m.map(0, ""))
        self.assertEqual(pairs, [])

    def test_unrecognised_line_emits_nothing(self):
        pairs = list(self.m.map(0, "--- this is not a log ---"))
        self.assertEqual(pairs, [])

    def test_timestamped_error(self):
        pairs = list(self.m.map(0, "2024-01-15 00:00:01 ERROR db failed"))
        self.assertEqual(pairs, [("ERROR", 1)])

    def test_apply_multiple_lines(self):
        lines = [
            "ERROR disk failure",
            "INFO user login",
            "ERROR timeout",
            "WARNING high memory",
        ]
        pairs = self.m.apply(0, lines)
        from collections import Counter
        counts = Counter(k for k, _ in pairs)
        self.assertEqual(counts["ERROR"],   2)
        self.assertEqual(counts["INFO"],    1)
        self.assertEqual(counts["WARNING"], 1)


# ── Log reducer tests ─────────────────────────────────────────────────────────

class TestLogReducer(unittest.TestCase):

    def setUp(self):
        self.r = LogReducer()

    def test_sums_errors(self):
        result = list(self.r.reduce("ERROR", [1,1,1,1,1]))
        self.assertEqual(result, [("ERROR", 5)])

    def test_sums_info(self):
        result = list(self.r.reduce("INFO", [1,1]))
        self.assertEqual(result, [("INFO", 2)])

    def test_apply_via_payload(self):
        result = self.r.apply({"key":"WARNING","values":[1,1,1]})
        self.assertEqual(result, [("WARNING", 3)])


# ── End-to-end log analysis pipeline (no network) ────────────────────────────

class TestLogAnalysisEndToEnd(unittest.TestCase):

    LINES = [
        "ERROR disk failure",
        "INFO user login",
        "ERROR timeout",
        "ERROR connection refused",
        "INFO server started",
        "WARNING high memory",
        "DEBUG cache miss",
        "INFO request completed",
    ]

    def _run(self, lines):
        mapper  = LogMapper()
        reducer = LogReducer()

        # MAP
        all_pairs = [mapper.apply(0, lines)]

        # SHUFFLE
        flat    = Shuffler.flatten_map_results(all_pairs)
        grouped = Shuffler.shuffle(flat)

        # REDUCE
        final = {}
        for key, values in grouped.items():
            for k, v in reducer.reduce(key, values):
                final[k] = v
        return final

    def test_error_count(self):
        result = self._run(self.LINES)
        self.assertEqual(result.get("ERROR"), 3)

    def test_info_count(self):
        result = self._run(self.LINES)
        self.assertEqual(result.get("INFO"), 3)

    def test_warning_count(self):
        result = self._run(self.LINES)
        self.assertEqual(result.get("WARNING"), 1)

    def test_debug_count(self):
        result = self._run(self.LINES)
        self.assertEqual(result.get("DEBUG"), 1)

    def test_all_levels_present(self):
        result = self._run(self.LINES)
        for level in ("ERROR","INFO","WARNING","DEBUG"):
            self.assertIn(level, result)

    def test_counts_sum_to_line_count(self):
        result = self._run(self.LINES)
        self.assertEqual(sum(result.values()), len(self.LINES))

    def test_sample_log_file(self):
        """Runs against the real sample file in data/sample_logs/app.log."""
        logfile = os.path.join(
            os.path.dirname(__file__), "..", "data","sample_logs","app.log")
        if not os.path.exists(logfile):
            self.skipTest("sample log file not found")
        from mapreduce.mapper import Mapper
        lines  = Mapper.read_file_lines(logfile)
        result = self._run(lines)
        self.assertIn("ERROR",   result)
        self.assertIn("INFO",    result)
        self.assertGreater(result["ERROR"], 0)


if __name__ == "__main__": unittest.main()
