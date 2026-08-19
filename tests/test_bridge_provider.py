#!/usr/bin/env python3
"""Bridge v3 provider 的参数、内存边界和输出契约。"""

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = (ROOT / "bin/seed-bridge.sh").read_text(encoding="utf-8")
SOURCE = (ROOT / "src/seed_fvecs_bridge.rs").read_text(encoding="utf-8")


class BridgeProviderTest(unittest.TestCase):
    def test_entrypoint_uses_dataset_configuration(self):
        for name in (
            "MVP_BASE_FILE",
            "MVP_VECTOR_DIM",
            "MVP_ROW_COUNT",
            "MVP_ID_BASE",
            "MVP_BRIDGE_BATCH_ROWS",
            "MVP_COMPRESSION",
            "MVP_PARTITION_BUCKETS",
            "MVP_TARGET_FILE_SIZE_BYTES",
        ):
            self.assertIn(name, SCRIPT)
        self.assertIn("sift1m)", SCRIPT)
        self.assertIn("gist1m)", SCRIPT)

    def test_provider_calls_bridge_for_create_write_and_commit(self):
        for symbol in (
            "iceberg_bridge_table_create",
            "iceberg_bridge_table_write_partitioned_data_files",
            "iceberg_bridge_table_fast_append_commit",
            "iceberg_bridge_table_load",
        ):
            self.assertIn(symbol, SOURCE)
        self.assertIn('"format_version": "V3"', SOURCE)
        self.assertIn("VECTOR_DIM_PROPERTY", SOURCE)

    def test_staging_bounds_memory_and_enforces_file_count(self):
        self.assertIn("StreamWriter<File>", SOURCE)
        self.assertIn("stage_partition_streams", SOURCE)
        self.assertIn("writers.len() != args.partition_buckets", SOURCE)
        self.assertIn("file_count != 1", SOURCE)
        self.assertIn("total_rows != expected_rows as u64", SOURCE)
        self.assertIn("parquet_files != args.partition_buckets", SOURCE)

    def test_runner_isolates_opengauss_compiler_environment(self):
        self.assertIn("env -u CC -u CXX -u LD_LIBRARY_PATH", SCRIPT)


if __name__ == "__main__":
    unittest.main()
