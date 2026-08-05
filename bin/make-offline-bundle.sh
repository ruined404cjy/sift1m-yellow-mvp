#!/usr/bin/env bash
# 将已校验数据和本文件包一起封装，供完全离线的黄区服务器使用。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
parent_dir="$(dirname "$root_dir")"
base_name="$(basename "$root_dir")"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
output="${1:-$parent_dir/${base_name}-offline-${timestamp}.tar.gz}"
provider="${MVP_OFFLINE_PROVIDER:-all}"

if [[ "$provider" != "all" && "$provider" != "spark" && "$provider" != "pyiceberg" && "$provider" != "rust" ]]; then
  echo "ERROR: MVP_OFFLINE_PROVIDER 仅支持 all|spark|pyiceberg|rust" >&2
  exit 2
fi

bash "$root_dir/bin/verify-sift1m.sh"
if [[ "$provider" == "all" || "$provider" == "pyiceberg" ]]; then
  if ! compgen -G "$root_dir/wheelhouse/*.whl" >/dev/null; then
    echo "ERROR: PyIceberg 离线包缺少 wheelhouse/*.whl" >&2
    exit 1
  fi
  (cd "$root_dir/wheelhouse" && sha256sum -c SHA256SUMS)
fi

tar -C "$parent_dir" -czf "$output" \
  --exclude="$base_name/state/*" \
  --exclude='*/__pycache__' \
  --exclude='*/__pycache__/*' \
  --exclude='*.pyc' \
  --exclude="$base_name/.venv" \
  "$base_name"

sha256sum "$output"
echo "离线包已生成: $output"
