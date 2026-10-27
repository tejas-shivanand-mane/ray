"""Optional owner-run correctness checks; no downloads or benchmark execution."""

import gzip
import json
from pathlib import Path
import tempfile
import unittest

import sharded_dedup


class ShardedDedupChecks(unittest.TestCase):
    def test_duplicate_across_sources_survives_partition_and_combine(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "news.json.gz"
            records = [{"id": i, "category": source + ".example",
                        "text": "alpha beta gamma delta" if i < 2 else " ".join([source] * 4)}
                       for i, source in enumerate(sharded_dedup.SOURCES)]
            with gzip.open(path, "wt", encoding="utf-8") as stream:
                json.dump(records, stream)
            inputs = sharded_dedup.prepare(root, {"news": {"path": str(path)}})
            self.assertTrue(all(inputs["dedup_" + s]["rows"] == 1 for s in sharded_dedup.SOURCES))
            settings = {"news_tokens": 512}
            outputs = [sharded_dedup.compute(i, [], inputs, settings) for i in range(7)]
            report = sharded_dedup.compute(7, outputs, inputs, settings)
            self.assertEqual(report["documents_with_shingles"], 7)
            self.assertEqual(report["candidate_pairs"], 1)
            self.assertEqual(report["verified_pairs"], [[0, 1, 1.0]])


if __name__ == "__main__":
    unittest.main()
