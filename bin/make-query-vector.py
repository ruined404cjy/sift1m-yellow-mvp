#!/usr/bin/env python3
"""读取指定序号的 SIFT query，输出 SQL 向量字面量。"""

import argparse
import struct


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("query_file")
    parser.add_argument("--index", type=int, default=0)
    args = parser.parse_args()
    if args.index < 0:
        raise ValueError("index 不能小于 0")

    record_size = 4 + 128 * 4
    with open(args.query_file, "rb") as handle:
        handle.seek(args.index * record_size)
        raw = handle.read(record_size)
    if len(raw) != record_size:
        raise EOFError("查询序号超出文件范围")
    dimension = struct.unpack_from("<i", raw, 0)[0]
    if dimension != 128:
        raise ValueError(f"维度为 {dimension}，期望 128")
    values = struct.unpack_from("<128f", raw, 4)
    print("[" + ",".join(format(value, ".9g") for value in values) + "]")


if __name__ == "__main__":
    main()
