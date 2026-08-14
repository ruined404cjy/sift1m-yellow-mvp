#!/usr/bin/env python3
"""索引残留清理必须受空 Registry 与完整性校验保护。"""

import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parent.parent / "bin/clean-index-artifacts.py"
SPEC = importlib.util.spec_from_file_location("clean_index_artifacts", MODULE_PATH)
assert SPEC and SPEC.loader
CLEANER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CLEANER)


class CleanIndexArtifactsTest(unittest.TestCase):
    def make_fixture(self, root: Path, indexes: list[dict]) -> tuple[Path, Path, Path]:
        table = root / "warehouse/ns/table"
        metadata_dir = table / "metadata"
        index_dir = table / "indices"
        metadata_dir.mkdir(parents=True)
        index_dir.mkdir(exist_ok=True)
        table_uuid = "11111111-1111-1111-1111-111111111111"
        registry = index_dir / "index-registry-v2-gen3.puffin"
        registry_content = b"PFA1" + json.dumps(
            {"format_version": 2, "table_uuid": table_uuid, "indexes": indexes},
            separators=(",", ":"),
        ).encode() + b"\x91\x00PFA1"
        registry.write_bytes(registry_content)
        head = {
            "registry_path": registry.as_uri(),
            "file_size_bytes": len(registry_content),
            "content_sha256": hashlib.sha256(registry_content).hexdigest(),
        }
        metadata = metadata_dir / "00003.metadata.json"
        metadata.write_text(
            json.dumps(
                {
                    "table-uuid": table_uuid,
                    "location": table.as_uri(),
                    "properties": {
                        CLEANER.REGISTRY_HEAD_PROPERTY: json.dumps(head)
                    },
                }
            ),
            encoding="utf-8",
        )
        residual = index_dir / "default/builtin.ivf_flat_v2_segment.puffin"
        residual.parent.mkdir(exist_ok=True)
        residual.write_bytes(b"segment")
        return metadata, registry, residual

    def test_empty_registry_allows_residual_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            metadata, registry, residual = self.make_fixture(Path(directory), [])
            current, removed = CLEANER.clean_residual_artifacts(metadata.as_uri())
            self.assertEqual(current, registry.resolve())
            self.assertEqual(removed, [residual])
            self.assertTrue(registry.is_file())
            self.assertFalse(residual.exists())

    def test_active_registry_rejects_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            metadata, _, residual = self.make_fixture(
                Path(directory), [{"name": "idx"}]
            )
            with self.assertRaisesRegex(ValueError, "仍含活动索引"):
                CLEANER.clean_residual_artifacts(metadata.as_uri())
            self.assertTrue(residual.is_file())

    def test_table_without_registry_or_index_directory_is_clean(self):
        with tempfile.TemporaryDirectory() as directory:
            table = Path(directory) / "warehouse/ns/table"
            metadata_dir = table / "metadata"
            metadata_dir.mkdir(parents=True)
            metadata = metadata_dir / "00001.metadata.json"
            metadata.write_text(
                json.dumps(
                    {"location": table.as_uri(), "properties": {}}
                ),
                encoding="utf-8",
            )
            self.assertEqual(
                CLEANER.clean_residual_artifacts(metadata.as_uri()),
                (None, []),
            )

    def test_absolute_table_location_from_rust_fixture_is_supported(self):
        with tempfile.TemporaryDirectory() as directory:
            table = Path(directory) / "warehouse/ns/table"
            metadata_dir = table / "metadata"
            metadata_dir.mkdir(parents=True)
            metadata = metadata_dir / "00001.metadata.json"
            metadata.write_text(
                json.dumps({"location": str(table), "properties": {}}),
                encoding="utf-8",
            )
            self.assertEqual(
                CLEANER.clean_residual_artifacts(metadata.as_uri()),
                (None, []),
            )

    def test_active_pq_artifact_matches_canonical_implementation(self):
        with tempfile.TemporaryDirectory() as directory:
            table = Path(directory) / "warehouse/ns/table"
            artifact = table / "indices/default/builtin.ivf_pq_v1_segment.puffin"
            artifact.parent.mkdir(parents=True)
            artifact.write_bytes(b"pq")
            indexes = [{
                "definition": {
                    "name": "idx_pq",
                    "implementation": "builtin.ivf_pq@1",
                },
                "state": "active",
                "partitions": [{"segments": [{"artifact_files": [{
                    "uri": artifact.as_uri(),
                    "size_bytes": artifact.stat().st_size,
                }], "algorithm_details": {
                    "dimension": 960,
                    "num_clusters": 1024,
                    "num_sub_quantizers": 60,
                    "nbits": 8,
                }}]}],
            }]
            metadata, _, _ = self.make_fixture(Path(directory), indexes)
            implementation, artifacts, details = CLEANER.verify_active_artifact(
                metadata.as_uri(), "idx_pq", "ivf_pq", 960, 60, 8
            )
            self.assertEqual(implementation, "builtin.ivf_pq@1")
            self.assertEqual(artifacts, [artifact.resolve()])
            self.assertEqual(details[0]["num_sub_quantizers"], 60)


if __name__ == "__main__":
    unittest.main()
