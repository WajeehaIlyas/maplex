# tests/test_word_count.py
# Unit tests for the Word Count job — mapper, reducer, and end-to-end.

import sys, os, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jobs.word_count.mapper  import WordCountMapper
from jobs.word_count.reducer import WordCountReducer
from mapreduce.shuffler      import Shuffler


class TestWordCountMapper(unittest.TestCase):

    def setUp(self):
        self.m = WordCountMapper()

    def test_basic_line(self):
        pairs = list(self.m.map(0, "hello world hello"))
        words = [k for k, v in pairs]
        self.assertEqual(words.count("hello"), 2)
        self.assertEqual(words.count("world"),  1)

    def test_values_are_ones(self):
        pairs = list(self.m.map(0, "the quick brown fox"))
        self.assertTrue(all(v == 1 for _, v in pairs))

    def test_lowercased(self):
        pairs = list(self.m.map(0, "Hello HELLO hElLo"))
        words = [k for k, v in pairs]
        self.assertTrue(all(w == "hello" for w in words))

    def test_punctuation_stripped(self):
        pairs = list(self.m.map(0, "hello, world! hello."))
        words = [k for k, v in pairs]
        self.assertIn("hello", words)
        self.assertIn("world", words)
        # No punctuation in keys
        self.assertTrue(all(w.isalpha() for w in words))

    def test_empty_line(self):
        self.assertEqual(list(self.m.map(0, "")), [])

    def test_numbers_ignored(self):
        pairs = list(self.m.map(0, "123 456 hello"))
        words = [k for k, v in pairs]
        self.assertNotIn("123", words)
        self.assertIn("hello", words)

    def test_single_letter_words_skipped(self):
        pairs = list(self.m.map(0, "a is the cat"))
        words = [k for k, v in pairs]
        self.assertNotIn("a", words)
        self.assertIn("is",  words)
        self.assertIn("the", words)
        self.assertIn("cat", words)

    def test_apply_multiple_lines(self):
        pairs = self.m.apply(0, ["the cat sat", "the cat"])
        words = [k for k, v in pairs]
        self.assertEqual(words.count("the"), 2)
        self.assertEqual(words.count("cat"), 2)
        self.assertEqual(words.count("sat"), 1)


class TestWordCountReducer(unittest.TestCase):

    def setUp(self):
        self.r = WordCountReducer()

    def test_sums_counts(self):
        result = list(self.r.reduce("hello", [1,1,1,1]))
        self.assertEqual(result, [("hello", 4)])

    def test_single_count(self):
        result = list(self.r.reduce("world", [1]))
        self.assertEqual(result, [("world", 1)])

    def test_apply_via_payload(self):
        result = self.r.apply({"key":"the","values":[1,1,1]})
        self.assertEqual(result, [("the", 3)])


class TestWordCountEndToEnd(unittest.TestCase):
    """Full pipeline logic without any network — runs map+shuffle+reduce in-process."""

    LINES = [
        "the quick brown fox jumps over the lazy dog",
        "the dog barked at the fox",
        "a quick fox is a clever fox",
    ]

    def _run_pipeline(self, lines):
        mapper  = WordCountMapper()
        reducer = WordCountReducer()

        # MAP
        all_pairs = []
        for idx, chunk in enumerate([lines]):
            pairs = mapper.apply(idx, chunk)
            all_pairs.append(pairs)

        # SHUFFLE
        flat    = Shuffler.flatten_map_results(all_pairs)
        grouped = Shuffler.shuffle(flat)

        # REDUCE
        final = {}
        for key, values in grouped.items():
            for k, v in reducer.reduce(key, values):
                final[k] = v
        return final

    def test_the_appears_most(self):
        result = self._run_pipeline(self.LINES)
        self.assertGreater(result.get("the", 0), result.get("brown", 0))

    def test_fox_appears_three_times(self):
        result = self._run_pipeline(self.LINES)
        self.assertEqual(result.get("fox"), 4)

    def test_all_words_present(self):
        result = self._run_pipeline(self.LINES)
        for word in ("quick","brown","fox","dog","lazy"):
            self.assertIn(word, result)

    def test_counts_are_positive(self):
        result = self._run_pipeline(self.LINES)
        self.assertTrue(all(v > 0 for v in result.values()))


if __name__ == "__main__": unittest.main()
