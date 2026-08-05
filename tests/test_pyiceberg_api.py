#!/usr/bin/env python3
"""锁定 PyIceberg/PyArrow API 的小表集成测试；依赖未安装时跳过。"""

import importlib.util
import json
import struct
import tempfile
import unittest
from io import BytesIO
from pathlib import Path


HAS_PYICEBERG = all(
    importlib.util.find_spec(name) is not None
    for name in ("pyarrow", "pyiceberg", "sqlalchemy")
)
MODULE_PATH = (
    Path(__file__).resolve().parent.parent / "src" / "seed_sift1m_pyiceberg.py"
)
SPEC = importlib.util.spec_from_file_location("sift1m_pyiceberg_api_provider", MODULE_PATH)
assert SPEC and SPEC.loader
PROVIDER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROVIDER)


@unittest.skipUnless(HAS_PYICEBERG, "需要锁定 PyIceberg wheel 环境")
class PyIcebergApiTest(unittest.TestCase):
    def test_nested_field_does_not_persist_vector_dim_extension(self):
        from pyiceberg.types import FloatType, ListType, NestedField

        field = NestedField(
            2,
            "embedding",
            ListType(element_id=3, element_type=FloatType(), element_required=True),
            required=True,
            vector_dim=128,
        )
        self.assertNotIn("vector_dim", field.model_dump())
        self.assertNotIn("vector_dim", json.loads(field.model_dump_json()))

    def test_create_append_and_validate_partitioned_table(self):
        from pyiceberg.catalog import load_catalog
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.schema import Schema
        from pyiceberg.transforms import BucketTransform
        from pyiceberg.types import FloatType, ListType, LongType, NestedField

        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(
                2,
                "embedding",
                ListType(element_id=3, element_type=FloatType(), element_required=True),
                required=True,
            ),
        )
        spec = PartitionSpec(
            PartitionField(1, 1000, BucketTransform(2), "id_bucket")
        )
        raw = b"".join(
            struct.pack("<i128f", 128, *([float(row)] * 128))
            for row in (1, 2)
        )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            catalog = load_catalog(
                "api_test",
                type="sql",
                uri=f"sqlite:///{root}/catalog.db",
                warehouse=(root / "warehouse").as_uri(),
                **{"py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"},
            )
            catalog.create_namespace("ns")
            table = catalog.create_table(
                "ns.t",
                schema,
                location=(root / "warehouse/ns/t").as_uri(),
                partition_spec=spec,
                properties={
                    "format-version": "2",
                    "write.parquet.compression-codec": "uncompressed",
                    "vector_dim.embedding": "128",
                },
            )
            ids, values = PROVIDER.read_fvecs_batch(BytesIO(raw), 1, 10)
            table.append(
                PROVIDER.build_arrow_table(ids, values, schema_to_pyarrow(table.schema()))
            )
            table = catalog.load_table("ns.t")
            metadata_path = PROVIDER.local_path(table.metadata_location)
            metadata = PROVIDER.validate_metadata(metadata_path, 2)
            self.assertEqual(metadata["properties"]["vector_dim.embedding"], "128")
            self.assertTrue(list((root / "warehouse/ns/t").rglob("*.parquet")))


if __name__ == "__main__":
    unittest.main()
