#!/usr/bin/env bash
# 校验四个 SIFT1M 原始文件，拒绝 LFS 指针、截断文件和错误内容。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
download_dir="$root_dir/downloads"
checksum_file="$root_dir/checksums/SHA256SUMS"

declare -A expected_sizes=(
  [sift_base.fvecs]=516000000
  [sift_query.fvecs]=5160000
  [sift_groundtruth.ivecs]=4040000
  [sift_learn.fvecs]=51600000
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

(
  cd "$download_dir"
  sha256sum -c "$checksum_file"
)

echo "SIFT1M 校验通过。"
