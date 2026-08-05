#!/usr/bin/env python3
"""使用 Spark 将 SIFT1M base fvecs 写入本地 Iceberg HadoopCatalog。"""

import argparse
import json
import re
import struct
from pathlib import Path
from urllib.parse import urlparse

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, FloatType, LongType, StructField, StructType


RECORD_BYTES = 4 + 128 * 4
EXPECTED_ROWS = 1_000_000
IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
VECTOR_DIM_PROPERTY = "vector_dim.embedding"
VECTOR_DIM_VALUE = "128"


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


def parse_record(item, id_base: int):
    """解析一条 516 字节 SIFT fvecs 记录并附加稳定 ID。"""
    raw, ordinal = item
    raw = bytes(raw)
    if len(raw) != RECORD_BYTES:
        raise ValueError(f"记录长度为 {len(raw)}，期望 {RECORD_BYTES}")
    dimension = struct.unpack_from("<i", raw, 0)[0]
    if dimension != 128:
        raise ValueError(f"记录维度为 {dimension}，期望 128")
    vector = list(struct.unpack_from("<128f", raw, 4))
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


def validate_vector_dim_property(metadata_path: Path) -> None:
    """确认当前 Iceberg metadata 保存了表级向量维度审计属性。"""
    with metadata_path.open("r", encoding="utf-8") as handle:
        metadata = json.load(handle)
    properties = metadata.get("properties")
    actual = properties.get(VECTOR_DIM_PROPERTY) if isinstance(properties, dict) else None
    if actual != VECTOR_DIM_VALUE:
        raise RuntimeError(
            f"metadata 属性 {VECTOR_DIM_PROPERTY}={actual!r}，"
            f"期望 {VECTOR_DIM_VALUE!r}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--master", default="local[*]")
    parser.add_argument("--id-base", type=int, choices=(0, 1), default=1)
    parser.add_argument("--data-files", type=int, default=8)
    parser.add_argument("--compression", default="uncompressed",
                        choices=("uncompressed", "zstd", "snappy", "lz4"))
    parser.add_argument("--partition-buckets", type=int, default=0)
    args = parser.parse_args()

    validate_identifier(args.namespace, "namespace")
    validate_identifier(args.table, "table")
    if args.data_files < 1:
        raise ValueError("data-files 必须大于 0")
    if args.partition_buckets < 0:
        raise ValueError("partition-buckets 不能小于 0")

    input_path = local_path(args.input)
    warehouse_path = local_path(args.warehouse)
    if not input_path.is_file():
        raise FileNotFoundError(input_path)
    if input_path.stat().st_size != EXPECTED_ROWS * RECORD_BYTES:
        raise ValueError(
            f"base 文件大小为 {input_path.stat().st_size}，"
            f"期望 {EXPECTED_ROWS * RECORD_BYTES}"
        )

    table_dir = warehouse_path / args.namespace / args.table
    if table_dir.exists():
        raise FileExistsError(
            f"表目录已存在，MVP 拒绝复用旧快照: {table_dir}"
        )
    warehouse_path.mkdir(parents=True, exist_ok=True)

    catalog = "mvp"
    identifier = f"{catalog}.{args.namespace}.{args.table}"
    spark = (
        SparkSession.builder
        .appName("sift1m-yellow-mvp-seed")
        .master(args.master)
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
              'format-version'='2',
              'write.parquet.compression-codec'='{args.compression}',
              'write.metadata.compression-codec'='none',
              'write.distribution-mode'='{distribution_property}',
              'write.target-file-size-bytes'='1073741824',
              '{VECTOR_DIM_PROPERTY}'='{VECTOR_DIM_VALUE}'
            )
            """
        )

        records = spark.sparkContext.binaryRecords(input_path.as_uri(), RECORD_BYTES)
        rows = records.zipWithIndex().map(lambda item: parse_record(item, args.id_base))
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
        if actual_rows != EXPECTED_ROWS:
            raise RuntimeError(f"写入行数为 {actual_rows}，期望 {EXPECTED_ROWS}")
    finally:
        spark.stop()

    current_metadata = metadata_location(table_dir)
    validate_vector_dim_property(current_metadata)
    parquet_files = list(table_dir.rglob("*.parquet"))
    if not parquet_files:
        raise RuntimeError("供数完成后未找到 Parquet 数据文件")
    parquet_bytes = sum(path.stat().st_size for path in parquet_files)
    print(f"MVP_PROVIDER=spark-{spark_version}")
    print(f"MVP_ROW_COUNT={EXPECTED_ROWS}")
    print("MVP_FIELD_VECTOR_DIM=unsupported")
    print(f"MVP_VECTOR_DIM_PROPERTY={VECTOR_DIM_PROPERTY}={VECTOR_DIM_VALUE}")
    print(f"MVP_PARTITION_BUCKETS={args.partition_buckets}")
    print(f"MVP_PARQUET_FILES={len(parquet_files)}")
    print(f"MVP_PARQUET_BYTES={parquet_bytes}")
    print(f"MVP_METADATA_LOCATION={current_metadata.as_uri()}")


if __name__ == "__main__":
    main()
