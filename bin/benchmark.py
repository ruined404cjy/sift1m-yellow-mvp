#!/usr/bin/env python3
"""SIFT1M SQL Recall/延迟 MVP；所有测量查询复用一个 gsql 会话。"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import statistics
import struct
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Iterable, Sequence


IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
TIME_RE = re.compile(r"^Time:\s+([0-9]+(?:\.[0-9]+)?)\s+ms\s*$")
QUERY_RECORD_BYTES = 4 + 128 * 4
GT_RECORD_BYTES = 4 + 100 * 4


def read_env_file(path: Path) -> dict[str, str]:
    """读取本包简单的 export KEY=value 配置，不执行 shell。"""
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("'\"")
    return values


def select_query_indices(total: int, count: int, sampling: str) -> list[int]:
    """按顺序或含首尾等距方式选择 query 序号。"""
    if not 1 <= count <= total:
        raise ValueError(f"query 数必须在 1..{total} 范围内")
    if sampling == "first":
        return list(range(count))
    if sampling == "equidistant":
        if count == 1:
            return [0]
        return [index * (total - 1) // (count - 1) for index in range(count)]
    raise ValueError(f"不支持的 query sampling: {sampling}")


def record_count(path: Path, record_bytes: int, label: str) -> int:
    """按定长记录校验文件并返回记录数。"""
    size = path.stat().st_size
    if size % record_bytes != 0:
        raise ValueError(f"{label} 文件大小 {size} 不是记录长度 {record_bytes} 的整数倍")
    return size // record_bytes


def read_fvecs(path: Path, indices: Sequence[int]) -> list[list[float]]:
    """按指定序号读取 128 维 little-endian fvecs。"""
    result: list[list[float]] = []
    with path.open("rb") as handle:
        for index in indices:
            handle.seek(index * QUERY_RECORD_BYTES)
            raw = handle.read(QUERY_RECORD_BYTES)
            if len(raw) != QUERY_RECORD_BYTES:
                raise EOFError(f"{path} 不含 query {index}")
            dimension = struct.unpack_from("<i", raw, 0)[0]
            if dimension != 128:
                raise ValueError(f"query {index} 维度为 {dimension}，期望 128")
            result.append(list(struct.unpack_from("<128f", raw, 4)))
    return result


def read_ivecs(
    path: Path, indices: Sequence[int], k: int, id_base: int
) -> list[list[int]]:
    """按指定 query 序号读取 top-100 ivecs，并按表 ID 基数修正。"""
    result: list[list[int]] = []
    with path.open("rb") as handle:
        for index in indices:
            handle.seek(index * GT_RECORD_BYTES)
            raw = handle.read(GT_RECORD_BYTES)
            if len(raw) != GT_RECORD_BYTES:
                raise EOFError(f"{path} 不含 ground truth {index}")
            dimension = struct.unpack_from("<i", raw, 0)[0]
            if dimension != 100:
                raise ValueError(
                    f"ground truth {index} 宽度为 {dimension}，期望 100"
                )
            values = struct.unpack_from("<100i", raw, 4)
            result.append([value + id_base for value in values[:k]])
    return result


def base_vector(
    handle,
    row_id: int,
    id_base: int,
    cache: dict[int, tuple[float, ...]],
) -> tuple[float, ...]:
    """按稳定 ID 随机读取一条 base vector，并缓存 Recall 复算所需行。"""
    if row_id in cache:
        return cache[row_id]
    ordinal = row_id - id_base
    if not 0 <= ordinal < 1_000_000:
        raise ValueError(f"查询返回了范围外 ID: {row_id}")
    handle.seek(ordinal * QUERY_RECORD_BYTES)
    raw = handle.read(QUERY_RECORD_BYTES)
    if len(raw) != QUERY_RECORD_BYTES:
        raise EOFError(f"base 文件中找不到 ID {row_id}")
    dimension = struct.unpack_from("<i", raw, 0)[0]
    if dimension != 128:
        raise ValueError(f"base ID {row_id} 的维度为 {dimension}")
    value = struct.unpack_from("<128f", raw, 4)
    cache[row_id] = value
    return value


def euclidean_distance(left: Sequence[float], right: Sequence[float]) -> float:
    """计算标准 L2 距离，用于兼容 ann-benchmarks 的 ties 容忍口径。"""
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(left, right)))


def vector_literal(vector: Sequence[float]) -> str:
    """生成稳定、足以还原 Float32 的 SQL 向量字面量。"""
    return "[" + ",".join(format(value, ".9g") for value in vector) + "]"


def percentile(values: Sequence[float], percent: float) -> float:
    """按线性插值计算百分位。"""
    if not values:
        raise ValueError("空样本不能计算百分位")
    ordered = sorted(values)
    position = (len(ordered) - 1) * percent / 100.0
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def validate_identifier(value: str, label: str) -> None:
    """限制 SQL 标识符和类型名，避免不确定的 quoting 行为。"""
    if not IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"{label} 仅支持小写字母、数字和下划线: {value}")


def run_gsql(
    gsql: str,
    database: str,
    port: int,
    arguments: Iterable[str],
    timeout: int,
) -> subprocess.CompletedProcess[str]:
    """执行 gsql 并返回完整文本结果。"""
    return subprocess.run(
        [gsql, "-X", "-d", database, "-p", str(port), *arguments],
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def configure_nprobe(args: argparse.Namespace, table_name: str) -> None:
    """以显式 DROP/ADD 设置外表 nprobe；DROP 不存在选项时允许失败。"""
    drop = f"ALTER FOREIGN TABLE {table_name} OPTIONS (DROP nprobe);"
    run_gsql(args.gsql, args.database, args.port, ["-c", drop], 60)
    add = f"ALTER FOREIGN TABLE {table_name} OPTIONS (ADD nprobe '{args.nprobe}');"
    result = run_gsql(args.gsql, args.database, args.port, ["-c", add], 60)
    if result.returncode != 0:
        raise RuntimeError(f"设置 nprobe 失败:\n{result.stdout}\n{result.stderr}")


def mode_settings(mode: str, query_dop: int = 1) -> str:
    """返回当前测试模式和 DOP 的会话级 GUC。"""
    dop = f"SET query_dop={query_dop};\n"
    if mode == "index":
        return (
            dop
            + "SET enable_vectorsearch=on;\n"
            "SET try_vector_engine_strategy=force;\n"
        )
    return (
        dop
        + "SET enable_vectorsearch=off;\n"
        "SET enable_indexscan=off;\n"
        "SET enable_bitmapscan=off;\n"
    )


def explain_plan(args: argparse.Namespace, query: str) -> str:
    """获取计划并执行索引/全扫门禁。"""
    sql = mode_settings(args.mode, args.query_dop) + "EXPLAIN (VERBOSE, COSTS OFF) " + query
    result = run_gsql(args.gsql, args.database, args.port, ["-c", sql], 120)
    plan = result.stdout + result.stderr
    if result.returncode != 0:
        raise RuntimeError(f"EXPLAIN 失败:\n{plan}")

    lowered = plan.lower()
    if args.mode == "index":
        markers = args.plan_marker or ["Vector Search", "bridge vector index scan"]
        missing = [marker for marker in markers if marker.lower() not in lowered]
        if missing and not args.skip_plan_gate:
            raise RuntimeError(
                "索引执行计划门禁失败，缺少: " + ", ".join(missing) + "\n" + plan
            )
    elif "bridge vector index scan" in lowered and not args.skip_plan_gate:
        raise RuntimeError("全扫计划仍出现 bridge vector index scan:\n" + plan)
    if args.require_parallel_plan and args.query_dop > 1 and not args.skip_plan_gate:
        expected_dop = f"dop: 1/{args.query_dop}"
        if "local gather" not in lowered or expected_dop not in lowered:
            raise RuntimeError(
                f"并行计划门禁失败，期望 LOCAL GATHER {expected_dop}:\n{plan}"
            )
    return plan


def generate_sql(
    args: argparse.Namespace,
    queries: Sequence[Sequence[float]],
    table_name: str,
) -> str:
    """生成一个 gsql 会话内的预热与逐查询计时 SQL。"""
    lines = [
        "\\set ON_ERROR_STOP on",
        "\\pset pager off",
        "\\pset footer off",
        "\\pset format unaligned",
        "\\pset tuples_only on",
        mode_settings(args.mode, args.query_dop),
        "\\timing off",
    ]
    for index in range(min(args.warmup, len(queries))):
        literal = vector_literal(queries[index])
        lines.append(
            f"SELECT {args.id_column} FROM {table_name} "
            f"ORDER BY {args.vector_column} <-> '{literal}'::{args.vector_cast} "
            f"LIMIT {args.k};"
        )
    lines.append("\\timing on")
    for index, vector in enumerate(queries):
        literal = vector_literal(vector)
        lines.append(f"\\echo __MVP_QUERY_BEGIN_{index}__")
        lines.append(
            f"SELECT {args.id_column} FROM {table_name} "
            f"ORDER BY {args.vector_column} <-> '{literal}'::{args.vector_cast} "
            f"LIMIT {args.k};"
        )
        lines.append(f"\\echo __MVP_QUERY_END_{index}__")
    return "\n".join(lines) + "\n"


def parse_gsql_output(output: str, query_count: int) -> tuple[list[list[int]], list[float]]:
    """解析 begin/end 哨兵之间的 ID 与 gsql Time 行。"""
    results: list[list[int]] = [[] for _ in range(query_count)]
    times: list[float | None] = [None for _ in range(query_count)]
    current: int | None = None
    begin_re = re.compile(r"^__MVP_QUERY_BEGIN_(\d+)__$")
    end_re = re.compile(r"^__MVP_QUERY_END_(\d+)__$")

    for raw_line in output.splitlines():
        line = raw_line.strip()
        begin = begin_re.fullmatch(line)
        if begin:
            current = int(begin.group(1))
            continue
        end = end_re.fullmatch(line)
        if end:
            current = None
            continue
        if current is None:
            continue
        timing = TIME_RE.fullmatch(line)
        if timing:
            times[current] = float(timing.group(1))
        elif re.fullmatch(r"-?\d+", line):
            results[current].append(int(line))

    missing = [index for index, value in enumerate(times) if value is None]
    if missing:
        raise RuntimeError(f"未解析到查询耗时，query={missing[:10]}")
    return results, [float(value) for value in times]


def main() -> None:
    root_dir = Path(__file__).resolve().parent.parent
    config = read_env_file(root_dir / "mvp.env")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=("index", "fullscan"))
    parser.add_argument(
        "--nq", type=int, default=int(config.get("MVP_TEST_NQ", "100"))
    )
    parser.add_argument("--k", type=int, default=int(config.get("MVP_TEST_K", "10")))
    parser.add_argument(
        "--warmup", type=int, default=int(config.get("MVP_TEST_WARMUP", "5"))
    )
    parser.add_argument(
        "--query-sampling",
        choices=("first", "equidistant"),
        default=config.get("MVP_QUERY_SAMPLING", "first"),
    )
    parser.add_argument("--nprobe", type=int, default=int(config.get("MVP_NPROBE", "10")))
    parser.add_argument("--query-dop", type=int, default=1)
    parser.add_argument("--require-parallel-plan", action="store_true")
    parser.add_argument("--skip-recall", action="store_true")
    parser.add_argument("--namespace", default=config.get("MVP_NAMESPACE", "sift_bench"))
    parser.add_argument("--table", default=config.get("MVP_TABLE", "sift1m_serial"))
    parser.add_argument("--id-column", default="id")
    parser.add_argument("--vector-column", default="embedding")
    parser.add_argument("--vector-cast", default=config.get("MVP_VECTOR_TYPE", "floatvector"))
    parser.add_argument("--id-base", type=int, choices=(0, 1),
                        default=int(config.get("MVP_ID_BASE", "1")))
    parser.add_argument("--query-file", type=Path,
                        default=root_dir / "downloads/sift_query.fvecs")
    parser.add_argument("--groundtruth-file", type=Path,
                        default=root_dir / "downloads/sift_groundtruth.ivecs")
    parser.add_argument("--base-file", type=Path,
                        default=root_dir / "downloads/sift_base.fvecs")
    parser.add_argument("--gsql", default=config.get("MVP_GSQL_BIN", "gsql"))
    parser.add_argument("--database", default=config.get("MVP_DB", "postgres"))
    parser.add_argument("--port", type=int, default=int(config.get("MVP_PORT", "37000")))
    parser.add_argument("--plan-marker", action="append")
    parser.add_argument("--skip-plan-gate", action="store_true")
    parser.add_argument("--timeout", type=int, default=3600)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    if not 1 <= args.nq <= 10_000:
        raise ValueError("nq 必须在 1..10000 范围内")
    if not 1 <= args.k <= 10_000:
        raise ValueError("k 必须在 1..10000 范围内")
    if args.k > 100 and not args.skip_recall:
        raise ValueError("SIFT 官方 GT 仅含 top-100；k>100 必须使用 --skip-recall")
    if args.warmup < 0 or args.nprobe < 1 or args.query_dop < 1:
        raise ValueError("warmup 不能小于 0，nprobe 和 query-dop 必须大于 0")
    for value, label in (
        (args.namespace, "namespace"),
        (args.table, "table"),
        (args.id_column, "id-column"),
        (args.vector_column, "vector-column"),
        (args.vector_cast, "vector-cast"),
    ):
        validate_identifier(value, label)

    table_name = f"{args.namespace}.{args.table}"
    total_queries = record_count(args.query_file, QUERY_RECORD_BYTES, "query")
    query_indices = select_query_indices(
        total_queries, args.nq, args.query_sampling
    )
    queries = read_fvecs(args.query_file, query_indices)
    groundtruth = None
    if not args.skip_recall:
        groundtruth_count = record_count(
            args.groundtruth_file, GT_RECORD_BYTES, "ground truth"
        )
        if groundtruth_count != total_queries:
            raise ValueError(
                f"query/ground truth 记录数不一致: {total_queries}/{groundtruth_count}"
            )
        groundtruth = read_ivecs(
            args.groundtruth_file, query_indices, args.k, args.id_base
        )
    if args.mode == "index":
        configure_nprobe(args, table_name)

    first_literal = vector_literal(queries[0])
    first_query = (
        f"SELECT {args.id_column} FROM {table_name} "
        f"ORDER BY {args.vector_column} <-> '{first_literal}'::{args.vector_cast} "
        f"LIMIT {args.k};"
    )
    plan = explain_plan(args, first_query)
    print("执行计划门禁通过。")
    print(plan)

    sql = generate_sql(args, queries, table_name)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", prefix="sift1m-bench-", suffix=".sql", delete=False
    ) as handle:
        handle.write(sql)
        sql_path = Path(handle.name)

    try:
        started = time.monotonic()
        run = run_gsql(
            args.gsql,
            args.database,
            args.port,
            ["-f", str(sql_path)],
            args.timeout,
        )
        wall_ms = (time.monotonic() - started) * 1000.0
    finally:
        sql_path.unlink(missing_ok=True)

    combined_output = run.stdout + "\n" + run.stderr
    if run.returncode != 0:
        raise RuntimeError(f"benchmark SQL 失败:\n{combined_output}")
    result_ids, query_times_ms = parse_gsql_output(combined_output, args.nq)

    per_query = []
    total_id_hits = 0
    total_distance_hits = 0
    if groundtruth is None:
        for query_index, actual, elapsed_ms in zip(
            query_indices, result_ids, query_times_ms
        ):
            per_query.append({
                "query_index": query_index,
                "elapsed_ms": elapsed_ms,
                "id_hits": None,
                "distance_threshold_hits": None,
                "returned_ids": actual[: args.k],
            })
    else:
        vector_cache: dict[int, tuple[float, ...]] = {}
        with args.base_file.open("rb") as base_handle:
            for query_index, query, actual, expected, elapsed_ms in zip(
                query_indices, queries, result_ids, groundtruth, query_times_ms
            ):
                returned = actual[: args.k]
                id_hits = len(set(returned) & set(expected))
                total_id_hits += id_hits

                kth_vector = base_vector(
                    base_handle, expected[-1], args.id_base, vector_cache
                )
                distance_threshold = euclidean_distance(query, kth_vector) + 1e-3
                distance_hits = sum(
                    euclidean_distance(
                        query,
                        base_vector(base_handle, row_id, args.id_base, vector_cache),
                    ) <= distance_threshold
                    for row_id in returned
                )
                total_distance_hits += distance_hits
                per_query.append({
                    "query_index": query_index,
                    "elapsed_ms": elapsed_ms,
                    "id_hits": id_hits,
                    "distance_threshold_hits": distance_hits,
                    "returned_ids": returned,
                })

    total_query_ms = sum(query_times_ms)
    id_recall = None if groundtruth is None else total_id_hits / float(args.nq * args.k)
    distance_recall = (
        None if groundtruth is None else total_distance_hits / float(args.nq * args.k)
    )
    summary = {
        "format_version": 1,
        "mode": args.mode,
        "table": table_name,
        "vector_cast": args.vector_cast,
        "query_count": args.nq,
        "query_sampling": args.query_sampling,
        "query_indices": query_indices,
        "top_k": args.k,
        "id_base": args.id_base,
        "nprobe": args.nprobe if args.mode == "index" else None,
        "query_dop": args.query_dop,
        "recall_source": None if groundtruth is None else "official_sift_groundtruth",
        "warmup_queries": min(args.warmup, args.nq),
        "recall_at_k_id": id_recall,
        "recall_at_k_distance_threshold": distance_recall,
        "id_hits": None if groundtruth is None else total_id_hits,
        "distance_threshold_hits": None if groundtruth is None else total_distance_hits,
        "possible_hits": None if groundtruth is None else args.nq * args.k,
        "qps_from_gsql_statement_times": args.nq / (total_query_ms / 1000.0),
        "query_time_ms": {
            "sum": total_query_ms,
            "mean": statistics.fmean(query_times_ms),
            "p50": percentile(query_times_ms, 50),
            "p95": percentile(query_times_ms, 95),
            "p99": percentile(query_times_ms, 99),
            "min": min(query_times_ms),
            "max": max(query_times_ms),
        },
        "gsql_process_wall_ms_including_startup_and_warmup": wall_ms,
        "plan": plan,
        "per_query": per_query,
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    recall_text = "Recall=skipped"
    if id_recall is not None and distance_recall is not None:
        recall_text = (
            f"ID-Recall@{args.k}={id_recall:.6f}, "
            f"Distance-Recall@{args.k}={distance_recall:.6f}"
        )
    print(
        f"{recall_text}, QPS={summary['qps_from_gsql_statement_times']:.3f}, "
        f"p50={summary['query_time_ms']['p50']:.3f}ms, "
        f"p95={summary['query_time_ms']['p95']:.3f}ms, "
        f"p99={summary['query_time_ms']['p99']:.3f}ms"
    )
    print(f"结果已保存: {args.output}")


if __name__ == "__main__":
    main()
