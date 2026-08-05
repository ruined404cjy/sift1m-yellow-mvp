#!/usr/bin/env python3
"""PyIceberg 供数器中不依赖可选 wheel 的输入与 metadata 门禁测试。"""

import importlib.util
import json
import struct
import tempfile
import unittest
from io import BytesIO
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parent.parent / "src" / "seed_sift1m_pyiceberg.py"
)
SPEC = importlib.util.spec_from_file_location("sift1m_pyiceberg_provider", MODULE_PATH)
assert SPEC and SPEC.loader
PROVIDER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROVIDER)


class PyIcebergProviderTest(unittest.TestCase):
    def test_reads_little_endian_fvecs_batch(self):
        first = struct.pack("<i128f", 128, *range(128))
        second = struct.pack("<i128f", 128, *range(128, 256))
        ids, values = PROVIDER.read_fvecs_batch(BytesIO(first + second), 1, 10)
        self.assertEqual(ids, [1, 2])
        self.assertEqual(len(values), 2 * 128 * 4)
        self.assertEqual(struct.unpack_from("<f", values, 127 * 4)[0], 127.0)
        self.assertEqual(struct.unpack_from("<f", values, 128 * 4)[0], 128.0)

    def test_rejects_truncated_record(self):
        with self.assertRaises(EOFError):
            PROVIDER.read_fvecs_batch(BytesIO(b"\x80\x00\x00\x00"), 1, 1)

    def test_validates_vector_property_and_partition(self):
        metadata = {
            "format-version": 2,
            "current-snapshot-id": 10,
            "current-schema-id": 0,
            "schemas": [{
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "type": "long", "required": True},
                    {
                        "id": 2,
                        "name": "embedding",
                        "required": True,
                        "type": {
                            "type": "list",
                            "element-id": 3,
                            "element": "float",
                            "element-required": True,
                        },
                    },
                ],
            }],
            "properties": {"vector_dim.embedding": "128"},
            "default-spec-id": 0,
            "partition-specs": [{
                "spec-id": 0,
                "fields": [{
                    "source-id": 1,
                    "field-id": 1000,
                    "name": "id_bucket",
                    "transform": "bucket[32]",
                }],
            }],
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metadata.json"
            path.write_text(json.dumps(metadata), encoding="utf-8")
            validated = PROVIDER.validate_metadata(path, 32)
            self.assertEqual(validated["current-snapshot-id"], 10)

            metadata["properties"]["vector_dim.embedding"] = 128
            path.write_text(json.dumps(metadata), encoding="utf-8")
            with self.assertRaises(RuntimeError):
                PROVIDER.validate_metadata(path, 32)


if __name__ == "__main__":
    unittest.main()
