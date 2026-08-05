#!/usr/bin/env bash
# 在与黄区 Python ABI/架构一致的联网机器下载锁定依赖。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
python_bin="${MVP_WHEELHOUSE_PYTHON:-python3}"
wheelhouse="$root_dir/wheelhouse"
requirements="$root_dir/requirements/pyiceberg-lock.txt"

mkdir -p "$wheelhouse"
env -u LD_LIBRARY_PATH "$python_bin" -m pip download \
  --disable-pip-version-check \
  --only-binary=:all: \
  --requirement "$requirements" \
  --dest "$wheelhouse"

(
  cd "$wheelhouse"
  find . -maxdepth 1 -type f -name '*.whl' -printf '%f\n' \
    | sort | while IFS= read -r filename; do
        sha256sum "$filename"
      done
) > "$wheelhouse/SHA256SUMS"
echo "Wheelhouse 已生成: $wheelhouse"
