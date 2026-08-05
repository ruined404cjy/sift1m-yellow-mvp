#!/usr/bin/env bash
# 在可访问外网的服务器上下载 SIFT1M，并复用统一校验流程。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
download_dir="$root_dir/downloads"
mkdir -p "$download_dir"

base_url="https://huggingface.co/datasets/qbo-odp/sift1m/resolve/main"
files=(
  sift_base.fvecs
  sift_query.fvecs
  sift_groundtruth.ivecs
  sift_learn.fvecs
)

download_one() {
  local filename="$1"
  local url="$base_url/$filename?download=true"
  local output="$download_dir/$filename"
  local partial="$output.part"

  if [[ -f "$output" ]]; then
    echo "已存在，交由校验脚本判断：$filename"
    return
  fi

  echo "下载 $filename"
  if command -v curl >/dev/null 2>&1; then
    curl -fL --retry 4 --retry-delay 3 --continue-at - \
      --output "$partial" "$url"
  elif command -v wget >/dev/null 2>&1; then
    wget --continue --output-document="$partial" "$url"
  else
    echo "ERROR: 需要 curl 或 wget" >&2
    exit 1
  fi
  mv "$partial" "$output"
}

for filename in "${files[@]}"; do
  download_one "$filename"
done

bash "$root_dir/bin/verify-sift1m.sh"
