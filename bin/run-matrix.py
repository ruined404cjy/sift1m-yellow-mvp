#!/usr/bin/env python3
"""执行当前数据集的 K×DOP×扫描模式矩阵并汇总 JSON。"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


def csv_ints(value: str, label: str) -> list[int]:
    """解析正整数逗号列表并保持输入顺序。"""
    try:
        values = [int(item) for item in value.split(",")]
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{label} 必须是整数逗号列表") from exc
    if not values or any(item < 1 for item in values):
        raise argparse.ArgumentTypeError(f"{label} 必须全部大于 0")
    return values


def read_env_file(path: Path) -> dict[str, str]:
    """读取简单的 export KEY=value 配置，不执行 shell。"""
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip("'\"")
    return values


def main() -> None:
    root_dir = Path(__file__).resolve().parent.parent
    env_file = Path(os.environ.get("MVP_ENV_FILE", root_dir / "mvp.env"))
    config = read_env_file(env_file)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--k", default=config.get("MVP_MATRIX_K", "10,100,1000,10000")
    )
    parser.add_argument("--dop", default=config.get("MVP_MATRIX_DOP", "1,2,4,8"))
    parser.add_argument(
        "--modes", default=config.get("MVP_MATRIX_MODES", "index,fullscan")
    )
    parser.add_argument(
        "--rounds", type=int, default=int(config.get("MVP_MATRIX_ROUNDS", "1"))
    )
    parser.add_argument(
        "--nq", type=int, default=int(config.get("MVP_MATRIX_NQ", "100"))
    )
    parser.add_argument(
        "--warmup", type=int, default=int(config.get("MVP_MATRIX_WARMUP", "5"))
    )
    parser.add_argument(
        "--query-sampling",
        choices=("first", "equidistant"),
        default=config.get("MVP_QUERY_SAMPLING", "first"),
    )
    parser.add_argument("--nprobe", type=int)
    parser.add_argument("--allow-serial-fallback", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=root_dir / "state/matrix")
    args = parser.parse_args()
    gt_width = int(config.get("MVP_GT_K", "100"))

    ks = csv_ints(args.k, "k")
    dops = csv_ints(args.dop, "dop")
    modes = args.modes.split(",")
    if any(mode not in ("index", "fullscan") for mode in modes):
        raise ValueError("modes 仅支持 index,fullscan")
    if args.rounds < 1 or args.nq < 1 or args.warmup < 0:
        raise ValueError("rounds/nq 必须大于 0，warmup 不能小于 0")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for mode in modes:
        for k in ks:
            for dop in dops:
                for round_no in range(1, args.rounds + 1):
                    label = f"{mode}-k{k}-dop{dop}-r{round_no}"
                    output = args.output_dir / f"{label}.json"
                    command = [
                        sys.executable,
                        str(root_dir / "bin/benchmark.py"),
                        "--mode", mode,
                        "--nq", str(args.nq),
                        "--k", str(k),
                        "--warmup", str(args.warmup),
                        "--query-sampling", args.query_sampling,
                        "--query-dop", str(dop),
                        "--output", str(output),
                    ]
                    if k > gt_width:
                        command.append("--skip-recall")
                    if dop > 1 and not args.allow_serial_fallback:
                        command.append("--require-parallel-plan")
                    if args.nprobe is not None:
                        command.extend(("--nprobe", str(args.nprobe)))
                    print(f"运行 {label}", flush=True)
                    run = subprocess.run(command, check=False)
                    if run.returncode != 0:
                        raise RuntimeError(f"矩阵场景失败: {label}")
                    item = json.loads(output.read_text(encoding="utf-8"))
                    item["round"] = round_no
                    item["result_file"] = str(output)
                    results.append(item)

    summary = {
        "format_version": 1,
        "dataset": config.get("MVP_DATASET", "sift1m"),
        "k_values": ks,
        "dop_values": dops,
        "modes": modes,
        "rounds": args.rounds,
        "query_count_per_round": args.nq,
        "query_sampling": args.query_sampling,
        "results": results,
    }
    summary_path = args.output_dir / "summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"矩阵结果已保存: {summary_path}")


if __name__ == "__main__":
    main()
