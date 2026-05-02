# tests/test_mapreduce_units.py
# Unit tests for Mapper, Shuffler, Reducer base classes.
# No server, no workers, no HTTP — pure logic.

import sys, os, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from mapreduce.mapper   import Mapper
from mapreduce.shuffler import Shuffler
from mapreduce.reducer  import Reducer


# ── Concrete stubs for testing base classes ───────────────────────────────────

class UpperMapper(Mapper):
    """Emits (WORD, 1) for each whitespace token."""
    def map(self, key, value):
        for tok in value.split():
            yield tok.upper(), 1

class SumReducer(Reducer):
    """Sums all values for a key."""
    def reduce(self, key, values):
        yield key, sum(values)


# ── Mapper tests ──────────────────────────────────────────────────────────────

class TestMapper(unittest.TestCase):

    def test_split_into_chunks_equal(self):
        lines  = [str(i) for i in range(10)]
        chunks = Mapper.split_into_chunks(lines, chunk_size=5)
        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0], ["0","1","2","3","4"])
        self.assertEqual(chunks[1], ["5","6","7","8","9"])

    def test_split_into_chunks_remainder(self):
        lines  = [str(i) for i in range(7)]
        chunks = Mapper.split_into_chunks(lines, chunk_size=3)
        self.assertEqual(len(chunks), 3)
        self.assertEqual(chunks[-1], ["6"])

    def test_split_empty_input(self):
        self.assertEqual(Mapper.split_into_chunks([], chunk_size=5), [])

    def test_split_chunk_larger_than_input(self):
        lines  = ["a", "b"]
        chunks = Mapper.split_into_chunks(lines, chunk_size=10)
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0], ["a", "b"])

    def test_apply_returns_pairs(self):
        m      = UpperMapper()
        pairs  = m.apply(chunk_index=0, lines=["hello world", "foo bar"])
        keys   = [k for k, v in pairs]
        self.assertIn("HELLO", keys)
        self.assertIn("WORLD", keys)
        self.assertIn("FOO",   keys)
        self.assertIn("BAR",   keys)

    def test_apply_values_are_ones(self):
        m     = UpperMapper()
        pairs = m.apply(chunk_index=0, lines=["hello"])
        self.assertTrue(all(v == 1 for _, v in pairs))

    def test_apply_empty_lines(self):
        m     = UpperMapper()
        pairs = m.apply(chunk_index=0, lines=[])
        self.assertEqual(pairs, [])

    def test_apply_blank_line(self):
        m     = UpperMapper()
        pairs = m.apply(chunk_index=0, lines=["   "])
        self.assertEqual(pairs, [])

    def test_map_not_implemented(self):
        class BadMapper(Mapper): pass
        with self.assertRaises(NotImplementedError):
            list(BadMapper().map(0, "hello"))


# ── Shuffler tests ────────────────────────────────────────────────────────────

class TestShuffler(unittest.TestCase):

    def test_shuffle_groups_by_key(self):
        pairs   = [("a",1),("b",1),("a",1),("a",1),("b",1)]
        grouped = Shuffler.shuffle(pairs)
        self.assertEqual(grouped["a"], [1,1,1])
        self.assertEqual(grouped["b"], [1,1])

    def test_shuffle_single_key(self):
        pairs   = [("x",2),("x",3)]
        grouped = Shuffler.shuffle(pairs)
        self.assertIn("x", grouped)
        self.assertEqual(sorted(grouped["x"]), [2,3])

    def test_shuffle_empty(self):
        self.assertEqual(Shuffler.shuffle([]), {})

    def test_shuffle_preserves_all_values(self):
        pairs   = [(i%3, i) for i in range(9)]
        grouped = Shuffler.shuffle(pairs)
        total   = sum(len(v) for v in grouped.values())
        self.assertEqual(total, 9)

    def test_flatten_map_results(self):
        results = [ [("a",1),("b",1)], [("a",1),("c",1)] ]
        flat    = Shuffler.flatten_map_results(results)
        self.assertEqual(len(flat), 4)
        self.assertIn(("a",1), flat)
        self.assertIn(("c",1), flat)

    def test_flatten_handles_none_entries(self):
        results = [ [("a",1)], None, [("b",1)] ]
        flat    = Shuffler.flatten_map_results(results)
        self.assertEqual(len(flat), 2)

    def test_prepare_reduce_payloads_structure(self):
        grouped  = {"ERROR":[1,1,1], "INFO":[1]}
        payloads = Shuffler.prepare_reduce_payloads(grouped)
        self.assertEqual(len(payloads), 2)
        keys = [p["key"] for p in payloads]
        self.assertIn("ERROR", keys)
        vals = next(p["values"] for p in payloads if p["key"]=="ERROR")
        self.assertEqual(vals, [1,1,1])


# ── Reducer tests ─────────────────────────────────────────────────────────────

class TestReducer(unittest.TestCase):

    def test_apply_uses_payload(self):
        r      = SumReducer()
        result = r.apply({"key":"x","values":[1,2,3]})
        self.assertEqual(result, [("x", 6)])

    def test_apply_single_value(self):
        r      = SumReducer()
        result = r.apply({"key":"y","values":[42]})
        self.assertEqual(result, [("y", 42)])

    def test_collect_merges_results(self):
        results = [ [("a",3)], [("b",5)], [("a",2)] ]
        final   = Reducer.collect(results)
        self.assertEqual(final["a"], 5)   # 3+2
        self.assertEqual(final["b"], 5)

    def test_collect_empty(self):
        self.assertEqual(Reducer.collect([]), {})

    def test_collect_handles_none_entries(self):
        results = [ [("a",1)], None, [("b",2)] ]
        final   = Reducer.collect(results)
        self.assertIn("a", final)
        self.assertIn("b", final)

    def test_reduce_not_implemented(self):
        class BadReducer(Reducer): pass
        with self.assertRaises(NotImplementedError):
            list(BadReducer().reduce("x",[1,2]))


if __name__ == "__main__": unittest.main()
