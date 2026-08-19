#!/usr/bin/env python3
"""Spark 公共 fvecs producer 的数据集参数和 v3 metadata 门禁。"""

import importlib.util
import json
import struct
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parent.parent / "src" / "seed_sift1m.py"
SPEC = importlib.util.spec_from_file_location("spark_fvecs_provider", MODULE_PATH)
assert SPEC and SPEC.loader
PROVIDER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROVIDER)


class SparkProviderTest(unittest.TestCase):
    def test_parse_record_accepts_sift_and_gist_dimensions(self):
        for dimension in (128, 960):
            values = [float(index) for index in range(dimension)]
            raw = struct.pack(f"<i{dimension}f", dimension, *values)
            row_id, vector = PROVIDER.parse_record(
                (raw, 4), 1, dimension, len(raw), f"<{dimension}f"
            )
            self.assertEqual(row_id, 5)
            self.assertEqual(len(vector), dimension)
            self.assertEqual(vector[-1], float(dimension - 1))

    def test_metadata_requires_v3_and_matching_vector_dimension(self):
        with tempfile.TemporaryDirectory() as directory:
            metadata = Path(directory) / "v1.metadata.json"
            metadata.write_text(
                json.dumps(
                    {
                        "format-version": 3,
                        "properties": {"vector_dim.embedding": "960"},
                    }
                ),
                encoding="utf-8",
            )
            PROVIDER.validate_metadata(metadata, 960)

            metadata.write_text(
                json.dumps(
                    {
                        "format-version": 2,
                        "properties": {"vector_dim.embedding": "960"},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "format-version=2"):
                PROVIDER.validate_metadata(metadata, 960)


if __name__ == "__main__":
    unittest.main()
