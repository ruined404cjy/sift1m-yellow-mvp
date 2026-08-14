#!/usr/bin/env bash
# 校验三个官方 GIST1M 文件的大小、SHA-256 和定长记录头。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
download_dir="$root_dir/downloads"
checksum_file="$root_dir/checksums/GIST1M_SHA256SUMS"

declare -A expected_sizes=(
  [gist_base.fvecs]=3844000000
  [gist_query.fvecs]=3844000
  [gist_groundtruth.ivecs]=404000
)

for filename in "${!expected_sizes[@]}"; do
  path="$download_dir/$filename"
  if [[ ! -f "$path" ]]; then
    echo "ERROR: 缺少 $path" >&2
    exit 1
  fi
  actual_size="$(stat -c '%s' "$path")"
  if [[ "$actual_size" != "${expected_sizes[$filename]}" ]]; then
    echo "ERROR: $filename 大小为 $actual_size，期望 ${expected_sizes[$filename]}" >&2
    if head -c 80 "$path" | grep -q 'git-lfs'; then
      echo "ERROR: 当前文件是 Git LFS 指针，不是数据文件" >&2
    fi
    exit 1
  fi
done

if [[ ! -f "$checksum_file" ]]; then
  echo "ERROR: 缺少 $checksum_file" >&2
  echo "ERROR: 联网区执行 bash bin/download-gist1m.sh 生成并随数据一同传入黄区" >&2
  exit 1
fi
(
  cd "$download_dir"
  sha256sum -c "$checksum_file"
)

python3 - "$download_dir" <<'PY'
import mmap
import struct
import sys
from pathlib import Path

download_dir = Path(sys.argv[1])
contracts = (
    ("gist_base.fvecs", 1_000_000, 960),
    ("gist_query.fvecs", 1_000, 960),
    ("gist_groundtruth.ivecs", 1_000, 100),
)
for filename, rows, width in contracts:
    path = download_dir / filename
    record_bytes = 4 + width * 4
    with path.open("rb") as handle, mmap.mmap(
        handle.fileno(), 0, access=mmap.ACCESS_READ
    ) as content:
        for row in range(rows):
            actual = struct.unpack_from("<i", content, row * record_bytes)[0]
            if actual != width:
                raise SystemExit(
                    f"ERROR: {filename} 第 {row} 条记录宽度为 {actual}，期望 {width}"
                )
PY

echo "GIST1M 校验通过。"
