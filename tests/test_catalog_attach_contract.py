#!/usr/bin/env python3
"""Catalog fixture 接入流程的静态回归门禁。"""

import unittest
from pathlib import Path


SCRIPT = (
    Path(__file__).resolve().parent.parent / "bin" / "register-table.sh"
).read_text(encoding="utf-8")
DEPLOY = (
    Path(__file__).resolve().parent.parent / "bin" / "deploy.sh"
).read_text(encoding="utf-8")


class CatalogAttachContractTest(unittest.TestCase):
    def test_uses_native_register_table_with_producer_metadata(self):
        self.assertIn("iceberg_catalog.register_table(", SCRIPT)
        self.assertIn('dimension="${MVP_VECTOR_DIM:-128}"', SCRIPT)
        self.assertIn('properties.get("vector_dim.embedding")', SCRIPT)
        self.assertIn("table_uuid = metadata.get(\"table-uuid\")", SCRIPT)
        self.assertIn("field_vector_dim", SCRIPT)

    def test_does_not_mutate_catalog_internal_head(self):
        self.assertNotIn("iceberg_catalog.create_table(", SCRIPT)
        self.assertNotIn("UPDATE iceberg_catalog.tables_internal", SCRIPT)
        self.assertNotIn("SET metadata_location=", SCRIPT)
        self.assertNotIn("DROP FOREIGN TABLE", SCRIPT)
        self.assertNotIn("CREATE FOREIGN TABLE", SCRIPT)
        self.assertNotIn("SET relid=", SCRIPT)

    def test_deploy_requires_catalog_register_table(self):
        self.assertIn("'register_table'", DEPLOY)
        self.assertIn('function_count" != "6"', DEPLOY)

    def test_does_not_create_or_load_delta(self):
        self.assertNotIn("iceberg_delta", SCRIPT)
        self.assertNotIn("_delta", SCRIPT)
        self.assertNotIn("to_regclass(", SCRIPT)


if __name__ == "__main__":
    unittest.main()
