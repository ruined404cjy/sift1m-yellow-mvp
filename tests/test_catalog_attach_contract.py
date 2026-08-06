#!/usr/bin/env python3
"""Catalog fixture 接入流程的静态回归门禁。"""

import unittest
from pathlib import Path


SCRIPT = (
    Path(__file__).resolve().parent.parent / "bin" / "register-table.sh"
).read_text(encoding="utf-8")


class CatalogAttachContractTest(unittest.TestCase):
    def test_uses_catalog_create_then_metadata_switch(self):
        self.assertIn("iceberg_catalog.create_table(", SCRIPT)
        self.assertIn('"vector_dim":128', SCRIPT)
        self.assertIn("SET metadata_location=", SCRIPT)
        self.assertIn("current_snapshot_id=", SCRIPT)

    def test_forbids_register_drop_recreate_flow(self):
        self.assertNotIn("iceberg_catalog.register_table(", SCRIPT)
        self.assertNotIn("DROP FOREIGN TABLE", SCRIPT)
        self.assertNotIn("CREATE FOREIGN TABLE", SCRIPT)
        self.assertNotIn("SET relid=", SCRIPT)

    def test_does_not_create_or_load_delta(self):
        self.assertNotIn("iceberg_delta", SCRIPT)
        self.assertNotIn("_delta", SCRIPT)
        self.assertNotIn("to_regclass(", SCRIPT)


if __name__ == "__main__":
    unittest.main()
