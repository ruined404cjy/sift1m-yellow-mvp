#!/usr/bin/env bash
# 将 mvp.env 切换到一组完整的 IVF-PQ 或 IVF-Flat 配置。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="${MVP_ENV_FILE:-$root_dir/mvp.env}"
profile="${1:-}"

if [[ ! -f "$env_file" ]]; then
  echo "ERROR: 缺少 $env_file" >&2
  exit 1
fi
case "$profile" in
  pq)
    index_name=idx_sift_ivfpq
    index_type=ivf_pq
    implementation=ivf_pq
    ;;
  flat)
    index_name=idx_sift_ivfflat
    index_type=ivf_flat
    implementation=ivf
    ;;
  *)
    echo "Usage: bash bin/configure-index.sh <pq|flat>" >&2
    exit 2
    ;;
esac

tmp_file="$(mktemp "${env_file}.XXXXXX")"
trap 'rm -f "$tmp_file"' EXIT
awk -v name="$index_name" -v type="$index_type" -v impl="$implementation" '
BEGIN { seen_name=0; seen_type=0; seen_impl=0 }
/^[[:space:]]*(export[[:space:]]+)?MVP_INDEX_NAME=/ {
  print "export MVP_INDEX_NAME=" name; seen_name=1; next
}
/^[[:space:]]*(export[[:space:]]+)?MVP_INDEX_TYPE=/ {
  print "export MVP_INDEX_TYPE=" type; seen_type=1; next
}
/^[[:space:]]*(export[[:space:]]+)?MVP_INDEX_IMPLEMENTATION=/ {
  print "export MVP_INDEX_IMPLEMENTATION=" impl; seen_impl=1; next
}
{ print }
END {
  if (!seen_name) print "export MVP_INDEX_NAME=" name
  if (!seen_type) print "export MVP_INDEX_TYPE=" type
  if (!seen_impl) print "export MVP_INDEX_IMPLEMENTATION=" impl
}
' "$env_file" > "$tmp_file"
chmod --reference="$env_file" "$tmp_file"
mv "$tmp_file" "$env_file"
trap - EXIT

echo "索引配置已切换: profile=$profile, index_type=$index_type, implementation=$implementation, index_name=$index_name"
