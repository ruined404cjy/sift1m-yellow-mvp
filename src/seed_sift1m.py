#!/usr/bin/env python3
"""使用 Spark 将定长 fvecs 数据写入本地 Iceberg v3 HadoopCatalog。"""

import argparse
import json
import re
import struct
from pathlib import Path
from urllib.parse import urlparse

IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
VECTOR_DIM_PROPERTY = "vector_dim.embedding"
FORMAT_VERSION = 3


def local_path(value: str) -> Path:
    """将裸路径或 file URI 转换为已解析的本地绝对路径。"""
    parsed = urlparse(value)
    if parsed.scheme not in ("", "file"):
        raise ValueError(f"MVP 只支持本地 file 路径: {value}")
    path = Path(parsed.path if parsed.scheme else value).expanduser().resolve()
    if not path.is_absolute():
        raise ValueError(f"必须使用绝对路径: {value}")
    return path


def validate_identifier(value: str, label: str) -> None:
    """限制标识符格式，保证 Spark SQL 插值可预测。"""
    if not IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"{label} 仅支持小写字母、数字和下划线: {value}")


def parse_record(
    item, id_base: int, dimension: int, record_bytes: int, vector_format: str
):
    """解析一条定长 fvecs 记录并附加稳定 ID。"""
    raw, ordinal = item
    raw = bytes(raw)
    if len(raw) != record_bytes:
        raise ValueError(f"记录长度为 {len(raw)}，期望 {record_bytes}")
    actual_dimension = struct.unpack_from("<i", raw, 0)[0]
    if actual_dimension != dimension:
        raise ValueError(f"记录维度为 {actual_dimension}，期望 {dimension}")
    vector = list(struct.unpack_from(vector_format, raw, 4))
    return int(ordinal) + id_base, vector


def metadata_location(table_dir: Path) -> Path:
    """根据 version-hint.text 返回当前未压缩 metadata JSON。"""
    metadata_dir = table_dir / "metadata"
    hint = metadata_dir / "version-hint.text"
    if hint.is_file():
        version = int(hint.read_text(encoding="utf-8").strip())
        candidate = metadata_dir / f"v{version}.metadata.json"
        if candidate.is_file():
            return candidate.resolve()

    candidates = []
    for path in metadata_dir.glob("v*.metadata.json"):
        match = re.fullmatch(r"v(\d+)\.metadata\.json", path.name)
        if match:
            candidates.append((int(match.group(1)), path))
    if not candidates:
        raise FileNotFoundError(f"未找到当前 metadata JSON: {metadata_dir}")
    return max(candidates, key=lambda item: item[0])[1].resolve()


def validate_metadata(metadata_path: Path, dimension: int) -> None:
    """确认 metadata 格式版本和表级向量维度审计属性。"""
    with metadata_path.open("r", encoding="utf-8") as handle:
        metadata = json.load(handle)
    actual_version = metadata.get("format-version")
    if actual_version != FORMAT_VERSION:
        raise RuntimeError(
            f"metadata format-version={actual_version!r}，期望 {FORMAT_VERSION}"
        )
    properties = metadata.get("properties")
    actual = properties.get(VECTOR_DIM_PROPERTY) if isinstance(properties, dict) else None
    if actual != str(dimension):
        raise RuntimeError(
            f"metadata 属性 {VECTOR_DIM_PROPERTY}={actual!r}，"
            f"期望 {str(dimension)!r}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--dimension", required=True, type=int)
    parser.add_argument("--rows", required=True, type=int)
    parser.add_argument("--id-base", type=int, choices=(0, 1), default=1)
    parser.add_argument("--data-files", type=int, default=8)
    parser.add_argument("--compression", default="uncompressed",
                        choices=("uncompressed", "zstd", "snappy", "lz4"))
    parser.add_argument("--partition-buckets", type=int, default=32)
    parser.add_argument("--target-file-size-bytes", type=int, default=1_073_741_824)
    args = parser.parse_args()

    validate_identifier(args.dataset, "dataset")
    validate_identifier(args.namespace, "namespace")
    validate_identifier(args.table, "table")
    if args.dimension < 1 or args.rows < 1:
        raise ValueError("dimension 和 rows 必须大于 0")
    if args.data_files < 1:
        raise ValueError("data-files 必须大于 0")
    if args.partition_buckets < 0:
        raise ValueError("partition-buckets 不能小于 0")
    if args.target_file_size_bytes < 1:
        raise ValueError("target-file-size-bytes 必须大于 0")

    input_path = local_path(args.input)
    warehouse_path = local_path(args.warehouse)
    record_bytes = 4 + args.dimension * 4
    vector_format = f"<{args.dimension}f"
    if not input_path.is_file():
        raise FileNotFoundError(input_path)
    if input_path.stat().st_size != args.rows * record_bytes:
        raise ValueError(
            f"base 文件大小为 {input_path.stat().st_size}，"
            f"期望 {args.rows * record_bytes}"
        )

    table_dir = warehouse_path / args.namespace / args.table
    if table_dir.exists():
        raise FileExistsError(
            f"表目录已存在，MVP 拒绝复用旧快照: {table_dir}"
        )
    warehouse_path.mkdir(parents=True, exist_ok=True)

    from pyspark.sql import SparkSession

    catalog = "mvp"
    identifier = f"{catalog}.{args.namespace}.{args.table}"
    spark = (
        SparkSession.builder
        .appName(f"{args.dataset}-yellow-spark-seed")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse_path.as_uri())
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(max(args.data_files, args.partition_buckets)))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark_version = spark.version

    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{args.namespace}")
        partition_clause = ""
        distribution_property = "none"
        if args.partition_buckets > 0:
            partition_clause = f" PARTITIONED BY (bucket({args.partition_buckets}, id))"
            distribution_property = "hash"

        spark.sql(
            f"""
            CREATE TABLE {identifier} (
              id BIGINT NOT NULL,
              embedding ARRAY<FLOAT> NOT NULL
            ) USING iceberg
            {partition_clause}
            TBLPROPERTIES (
              'format-version'='{FORMAT_VERSION}',
              'write.parquet.compression-codec'='{args.compression}',
              'write.metadata.compression-codec'='none',
              'write.distribution-mode'='{distribution_property}',
              'write.target-file-size-bytes'='{args.target_file_size_bytes}',
              '{VECTOR_DIM_PROPERTY}'='{args.dimension}'
            )
            """
        )

        records = spark.sparkContext.binaryRecords(input_path.as_uri(), record_bytes)
        rows = records.zipWithIndex().map(
            lambda item: parse_record(
                item,
                args.id_base,
                args.dimension,
                record_bytes,
                vector_format,
            )
        )
        from pyspark.sql.types import (
            ArrayType,
            FloatType,
            LongType,
            StructField,
            StructType,
        )

        schema = StructType([
            StructField("id", LongType(), nullable=False),
            StructField(
                "embedding",
                ArrayType(FloatType(), containsNull=False),
                nullable=False,
            ),
        ])
        frame = spark.createDataFrame(rows, schema)
        if args.partition_buckets == 0:
            frame = frame.repartition(args.data_files, "id").sortWithinPartitions("id")
        frame.writeTo(identifier).append()

        actual_rows = spark.sql(f"SELECT count(*) AS n FROM {identifier}").first()["n"]
        if actual_rows != args.rows:
            raise RuntimeError(f"写入行数为 {actual_rows}，期望 {args.rows}")
    finally:
        spark.stop()

    current_metadata = metadata_location(table_dir)
    validate_metadata(current_metadata, args.dimension)
    parquet_files = list(table_dir.rglob("*.parquet"))
    if not parquet_files:
        raise RuntimeError("供数完成后未找到 Parquet 数据文件")
    expected_files = args.partition_buckets or args.data_files
    if len(parquet_files) != expected_files:
        raise RuntimeError(
            f"Parquet 文件数为 {len(parquet_files)}，当前布局要求 {expected_files}"
        )
    parquet_bytes = sum(path.stat().st_size for path in parquet_files)
    print(f"MVP_PROVIDER=spark-{spark_version}")
    print(f"MVP_DATASET={args.dataset}")
    print(f"MVP_ROW_COUNT={args.rows}")
    print(f"MVP_ICEBERG_FORMAT_VERSION={FORMAT_VERSION}")
    print("MVP_FIELD_VECTOR_DIM=unsupported")
    print(f"MVP_VECTOR_DIM_PROPERTY={VECTOR_DIM_PROPERTY}={args.dimension}")
    print(f"MVP_PARTITION_BUCKETS={args.partition_buckets}")
    print(f"MVP_PARQUET_FILES={len(parquet_files)}")
    print(f"MVP_PARQUET_BYTES={parquet_bytes}")
    print(f"MVP_METADATA_LOCATION={current_metadata.as_uri()}")


if __name__ == "__main__":
    main()
