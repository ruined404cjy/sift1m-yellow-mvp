#!/usr/bin/env python3
"""三条供数路径共享 32 bucket 默认值及 Rust 分区写入门禁。"""

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


class PartitionWorkflowTest(unittest.TestCase):
    def test_all_provider_entrypoints_default_to_32_buckets(self):
        for filename in (
            "seed-sift1m.sh",
            "seed-sift1m-pyiceberg.sh",
            "seed-sift1m-rust.sh",
        ):
            script = (ROOT / "bin" / filename).read_text(encoding="utf-8")
            self.assertIn("MVP_PARTITION_BUCKETS:-32", script)

        for filename in ("mvp.env.example", "perf.env.example"):
            config = (ROOT / "config" / filename).read_text(encoding="utf-8")
            self.assertIn("MVP_PARTITION_BUCKETS=32", config)

    def test_rust_fixture_writes_and_validates_partition_keys(self):
        source = (ROOT / "src/seed_sift1m_rust.rs").read_text(encoding="utf-8")
        self.assertIn("Transform::Bucket(args.partition_buckets)", source)
        self.assertIn(".partition_spec(partition_spec)", source)
        self.assertIn("RecordBatchPartitionSplitter::try_new_with_computed_values", source)
        self.assertIn("Some(partition_key)", source)
        self.assertIn("file.partition() != &expected_partition", source)
        self.assertIn("FileScanTask 缺少 partition 值", source)

    def test_catalog_attach_validates_configured_partition_spec(self):
        register = (ROOT / "bin/register-table.sh").read_text(encoding="utf-8")
        verify = (ROOT / "bin/verify-table.sh").read_text(encoding="utf-8")
        self.assertIn('partition_buckets="${MVP_PARTITION_BUCKETS:-32}"', register)
        self.assertIn('expected_transform = f"bucket[{partition_buckets}]"', register)
        self.assertIn('spec_fields[0].get("source-id") == 1', register)
        self.assertIn('spec_fields[0].get("name") == "id_bucket"', register)
        self.assertIn('partition_buckets="${MVP_PARTITION_BUCKETS:-32}"', verify)
        self.assertIn('expected = f"bucket[{partition_buckets}]"', verify)

    def test_zero_buckets_remains_supported(self):
        source = (ROOT / "src/seed_sift1m_rust.rs").read_text(encoding="utf-8")
        self.assertIn("if args.partition_buckets > 0", source)
        self.assertIn("None,\n                        batch,", source)


if __name__ == "__main__":
    unittest.main()
