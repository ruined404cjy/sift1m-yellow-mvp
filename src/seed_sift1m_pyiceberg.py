#!/usr/bin/env python3
"""使用隔离 PyIceberg 环境流式写入定长 fvecs，并输出 metadata location。"""

from __future__ import annotations

import argparse
import json
import re
import struct
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse


DIMENSION = 128
EXPECTED_ROWS = 1_000_000
IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
VECTOR_DIM_PROPERTY = "vector_dim.embedding"


def local_path(value: str) -> Path:
    """将裸路径或 file URI 转换为本地绝对路径。"""
    parsed = urlparse(value)
    if parsed.scheme not in ("", "file") or parsed.netloc not in ("", "localhost"):
        raise ValueError(f"仅支持本地 file 路径: {value}")
    path = Path(parsed.path if parsed.scheme else value).expanduser().resolve()
    if not path.is_absolute():
        raise ValueError(f"必须使用绝对路径: {value}")
    return path


def validate_identifier(value: str, label: str) -> None:
    """限制 namespace 和 table 格式，保持跨 Catalog 行为一致。"""
    if not IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"{label} 仅支持小写字母、数字和下划线: {value}")


def read_fvecs_batch(
    handle, first_id: int, max_rows: int, dimension: int = DIMENSION
) -> tuple[list[int], bytearray]:
    """读取一批 fvecs，返回连续 ID 和 little-endian float32 数据。"""
    ids: list[int] = []
    values = bytearray()
    record_bytes = 4 + dimension * 4
    for offset in range(max_rows):
        raw = handle.read(record_bytes)
        if not raw:
            break
        if len(raw) != record_bytes:
            raise EOFError(f"base 最后一条记录被截断: {len(raw)}/{record_bytes}")
        actual_dimension = struct.unpack_from("<i", raw, 0)[0]
        if actual_dimension != dimension:
            raise ValueError(
                f"第 {first_id + offset} 条向量维度为 {actual_dimension}，期望 {dimension}"
            )
        ids.append(first_id + offset)
        values.extend(raw[4:])
    return ids, values


def build_arrow_table(
    ids: list[int], values: bytearray, arrow_schema, dimension: int = DIMENSION
):
    """将连续 Float32 buffer 构造成与 Iceberg schema 完全一致的 PyArrow 表。"""
    import pyarrow as pa

    value_count = len(ids) * dimension
    value_array = pa.Array.from_buffers(
        pa.float32(), value_count, [None, pa.py_buffer(values)]
    )
    embedding_type = arrow_schema.field("embedding").type
    if pa.types.is_large_list(embedding_type):
        offsets = pa.array(
            range(0, value_count + 1, dimension),
            type=pa.int64(),
        )
        embeddings = pa.LargeListArray.from_arrays(
            offsets,
            value_array,
            type=embedding_type,
        )
    else:
        offsets = pa.array(
            range(0, value_count + 1, dimension),
            type=pa.int32(),
        )
        embeddings = pa.ListArray.from_arrays(
            offsets,
            value_array,
            type=embedding_type,
        )
    return pa.Table.from_arrays(
        [pa.array(ids, type=pa.int64()), embeddings],
        schema=arrow_schema,
    )


def validate_metadata(
    metadata_path: Path, partition_buckets: int, dimension: int = DIMENSION
) -> dict:
    """验证最终 metadata 的 snapshot、schema、分区和向量维度属性。"""
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if metadata.get("format-version") != 2:
        raise RuntimeError(f"Iceberg format-version={metadata.get('format-version')}，期望 2")
    properties = metadata.get("properties")
    actual_dim = properties.get(VECTOR_DIM_PROPERTY) if isinstance(properties, dict) else None
    if actual_dim != str(dimension):
        raise RuntimeError(
            f"metadata 属性 {VECTOR_DIM_PROPERTY}={actual_dim!r}，期望 {str(dimension)!r}"
        )
    if metadata.get("current-snapshot-id") is None:
        raise RuntimeError("最终 metadata 没有 current-snapshot-id")

    schemas = metadata.get("schemas") or [metadata.get("schema")]
    current_id = metadata.get("current-schema-id", metadata.get("schema", {}).get("schema-id"))
    current = next(
        (schema for schema in schemas if schema and schema.get("schema-id") == current_id),
        None,
    )
    fields = current.get("fields", []) if current else []
    by_name = {field.get("name"): field for field in fields}
    if by_name.get("id", {}).get("type") != "long":
        raise RuntimeError("Iceberg id 字段不是 long")
    vector_type = by_name.get("embedding", {}).get("type")
    if not isinstance(vector_type, dict) or vector_type.get("type") != "list":
        raise RuntimeError("Iceberg embedding 字段不是 list")
    if vector_type.get("element") != "float" or not vector_type.get("element-required"):
        raise RuntimeError("Iceberg embedding 元素必须是 required float")

    default_spec_id = metadata.get("default-spec-id", 0)
    specs = metadata.get("partition-specs")
    if isinstance(specs, list):
        current_spec = next(
            (spec for spec in specs if spec.get("spec-id") == default_spec_id),
            None,
        )
        spec_fields = current_spec.get("fields", []) if current_spec else []
    else:
        spec_fields = metadata.get("partition-spec", [])
    if partition_buckets == 0 and spec_fields:
        raise RuntimeError("串行表出现了非空 partition spec")
    expected_transform = f"bucket[{partition_buckets}]"
    if partition_buckets > 0 and not any(
        field.get("source-id") == 1 and field.get("transform") == expected_transform
        for field in spec_fields
    ):
        raise RuntimeError(f"缺少 bucket(id, {partition_buckets}) 分区字段")
    return metadata


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--dataset", default="sift1m")
    parser.add_argument("--dimension", type=int, default=DIMENSION)
    parser.add_argument("--rows", type=int, default=EXPECTED_ROWS)
    parser.add_argument("--id-base", type=int, choices=(0, 1), default=1)
    parser.add_argument("--batch-rows", type=int, default=131_072)
    parser.add_argument("--compression", default="uncompressed",
                        choices=("uncompressed", "zstd", "snappy", "gzip"))
    parser.add_argument("--partition-buckets", type=int, default=32)
    args = parser.parse_args()

    validate_identifier(args.namespace, "namespace")
    validate_identifier(args.table, "table")
    if args.batch_rows < 1:
        raise ValueError("batch-rows 必须大于 0")
    if args.dimension < 1 or args.rows < 1:
        raise ValueError("dimension 和 rows 必须大于 0")
    if args.partition_buckets < 0:
        raise ValueError("partition-buckets 不能小于 0")

    input_path = local_path(args.input)
    warehouse_path = local_path(args.warehouse)
    expected_bytes = args.rows * (4 + args.dimension * 4)
    if not input_path.is_file() or input_path.stat().st_size != expected_bytes:
        actual = input_path.stat().st_size if input_path.exists() else "missing"
        raise ValueError(f"{args.dataset} base 大小为 {actual}，期望 {expected_bytes}")
    table_path = warehouse_path / args.namespace / args.table
    if table_path.exists():
        raise FileExistsError(f"表目录已存在，拒绝复用旧快照: {table_path}")
    warehouse_path.mkdir(parents=True, exist_ok=True)

    import pyarrow as pa
    import pyiceberg
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
    partition_spec = PartitionSpec()
    if args.partition_buckets > 0:
        partition_spec = PartitionSpec(
            PartitionField(
                source_id=1,
                field_id=1000,
                transform=BucketTransform(args.partition_buckets),
                name="id_bucket",
            )
        )
    properties = {
        "format-version": "2",
        "write.parquet.compression-codec": args.compression,
        VECTOR_DIM_PROPERTY: str(args.dimension),
    }

    with tempfile.TemporaryDirectory(prefix=f"{args.dataset}-pyiceberg-") as catalog_dir:
        catalog = load_catalog(
            f"{args.dataset}_offline",
            type="sql",
            uri=f"sqlite:///{catalog_dir}/catalog.db",
            warehouse=warehouse_path.as_uri(),
            **{"py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"},
        )
        catalog.create_namespace(args.namespace)
        table = catalog.create_table(
            f"{args.namespace}.{args.table}",
            schema,
            location=table_path.as_uri(),
            partition_spec=partition_spec,
            properties=properties,
        )
        arrow_schema = schema_to_pyarrow(table.schema())

        written = 0
        with input_path.open("rb") as handle:
            while written < args.rows:
                ids, values = read_fvecs_batch(
                    handle,
                    written + args.id_base,
                    min(args.batch_rows, args.rows - written),
                    args.dimension,
                )
                if not ids:
                    break
                table.append(
                    build_arrow_table(ids, values, arrow_schema, args.dimension)
                )
                written += len(ids)
                print(f"PyIceberg 已写入 {written:,}/{args.rows:,}", file=sys.stderr)
            if handle.read(1):
                raise RuntimeError(f"{args.dataset} base 在预期行数后仍有多余数据")
        if written != args.rows:
            raise RuntimeError(f"写入行数为 {written}，期望 {args.rows}")

        table = catalog.load_table(f"{args.namespace}.{args.table}")
        metadata_uri = table.metadata_location

    metadata_path = local_path(metadata_uri)
    validate_metadata(metadata_path, args.partition_buckets, args.dimension)
    parquet_files = list(table_path.rglob("*.parquet"))
    if not parquet_files:
        raise RuntimeError("供数完成后未找到 Parquet 数据文件")
    parquet_bytes = sum(path.stat().st_size for path in parquet_files)

    print(f"MVP_PROVIDER=pyiceberg-{pyiceberg.__version__}-pyarrow-{pa.__version__}")
    print(f"MVP_DATASET={args.dataset}")
    print(f"MVP_ROW_COUNT={args.rows}")
    print("MVP_FIELD_VECTOR_DIM=unsupported")
    print(f"MVP_VECTOR_DIM_PROPERTY={VECTOR_DIM_PROPERTY}={args.dimension}")
    print(f"MVP_PARTITION_BUCKETS={args.partition_buckets}")
    print(f"MVP_PARQUET_FILES={len(parquet_files)}")
    print(f"MVP_PARQUET_BYTES={parquet_bytes}")
    print(f"MVP_METADATA_LOCATION={metadata_uri}")


if __name__ == "__main__":
    main()
