# tests/test_benchmark.py
# Unit tests for the speedup benchmark module.
# Does NOT actually spawn processes — tests the reporting logic only.

import sys, os, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from benchmark.speedup import SpeedupBenchmark


class TestSpeedupReport(unittest.TestCase):
    """Tests for benchmark reporting logic — no process spawning."""

    def _make_results(self):
        # Simulate results: 1 worker=8s, 2=4.3s, 3=2.9s, 4=2.2s
        raw = [
            {"workers":1, "elapsed":8.0},
            {"workers":2, "elapsed":4.3},
            {"workers":3, "elapsed":2.9},
            {"workers":4, "elapsed":2.2},
        ]
        baseline = raw[0]["elapsed"]
        for r in raw:
            r["speedup"]    = round(baseline / r["elapsed"], 3)
            r["efficiency"] = round(r["speedup"] / r["workers"], 3)
        return raw

    def test_speedup_increases_with_workers(self):
        results = self._make_results()
        speedups = [r["speedup"] for r in results]
        self.assertEqual(speedups, sorted(speedups))

    def test_baseline_speedup_is_one(self):
        results = self._make_results()
        self.assertEqual(results[0]["speedup"], 1.0)

    def test_efficiency_is_between_zero_and_one(self):
        results = self._make_results()
        for r in results:
            self.assertGreater(r["efficiency"], 0)
            self.assertLessEqual(r["efficiency"], 1.0)

    def test_max_speedup_less_than_n_workers(self):
        # Amdahl's law: speedup < N workers always
        results = self._make_results()
        for r in results[1:]:
            self.assertLess(r["speedup"], r["workers"] + 0.01)

    def test_print_report_does_not_crash(self):
        results = self._make_results()
        # Should not raise
        try:
            SpeedupBenchmark.print_report(results)
        except Exception as e:
            self.fail(f"print_report raised: {e}")

    def test_print_report_empty(self):
        try:
            SpeedupBenchmark.print_report([])
        except Exception as e:
            self.fail(f"print_report([]) raised: {e}")

    def test_save_report(self):
        import tempfile
        results = self._make_results()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt",
                                        delete=False) as f:
            path = f.name
        try:
            SpeedupBenchmark.save_report(results, path)
            self.assertTrue(os.path.exists(path))
            content = open(path).read()
            self.assertIn("1", content)
            self.assertIn("2", content)
        finally:
            os.unlink(path)

    def test_benchmark_init(self):
        b = SpeedupBenchmark(job="wordcount",
                             input_path="data/sample_logs/text.txt",
                             worker_counts=[1, 2])
        self.assertEqual(b.job, "wordcount")
        self.assertEqual(b.worker_counts, [1, 2])


if __name__ == "__main__": unittest.main()
