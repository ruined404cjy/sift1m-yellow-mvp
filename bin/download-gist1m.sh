#!/usr/bin/env bash
# 在可访问外网的服务器上下载官方 GIST1M 压缩包并生成传输校验清单。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
download_dir="$root_dir/downloads"
checksum_dir="$root_dir/checksums"
archive="$download_dir/gist.tar.gz"
partial="$archive.part"
source_url="${MVP_GIST_URL:-ftp://ftp.irisa.fr/local/texmex/corpus/gist.tar.gz}"
mirror_repo=fzliu/gist1m
mirror_revision=a98d7415dba638216300552059013cc627293409
expected_archive_size=2740172684
expected_archive_sha256=01469a7f1c3768853525e543d537e2dfa1adece927616405e360952e3f67df73
files=(gist_base.fvecs gist_query.fvecs gist_groundtruth.ivecs)

mkdir -p "$download_dir" "$checksum_dir"
if [[ -f "$download_dir/gist_query.fvecs" && \
      -f "$download_dir/gist_groundtruth.ivecs" && \
      -f "$download_dir/gist_base.fvecs" ]]; then
  bash "$root_dir/bin/verify-gist1m.sh"
  echo "GIST1M 数据和校验清单已就绪。"
  exit 0
fi

if [[ ! -f "$archive" ]]; then
  if [[ -z "${MVP_GIST_URL:-}" ]] && command -v hf >/dev/null 2>&1; then
    echo "通过 Hugging Face 固定 revision 下载 gist.tar.gz"
    hf download "$mirror_repo" gist.tar.gz \
      --type dataset \
      --revision "$mirror_revision" \
      --local-dir "$download_dir" \
      --max-workers 1
  elif command -v curl >/dev/null 2>&1; then
    echo "下载 $source_url"
    if ! curl -fL --retry 4 --retry-delay 3 --continue-at - \
      --output "$partial" "$source_url"; then
      echo "ERROR: 下载失败；可安装 hf 后重试，或通过 MVP_GIST_URL 指定镜像" >&2
      exit 1
    fi
    mv "$partial" "$archive"
  elif command -v wget >/dev/null 2>&1; then
    echo "下载 $source_url"
    wget --continue --output-document="$partial" "$source_url"
    mv "$partial" "$archive"
  else
    echo "ERROR: 需要 hf、curl 或 wget" >&2
    exit 1
  fi
fi

actual_archive_size="$(stat -c '%s' "$archive")"
if [[ "$actual_archive_size" != "$expected_archive_size" ]]; then
  echo "ERROR: gist.tar.gz 大小为 $actual_archive_size，期望 $expected_archive_size" >&2
  exit 1
fi
actual_archive_sha256="$(sha256sum "$archive" | awk '{print $1}')"
if [[ "$actual_archive_sha256" != "$expected_archive_sha256" ]]; then
  echo "ERROR: gist.tar.gz SHA-256 不匹配" >&2
  echo "ERROR: 实际 $actual_archive_sha256" >&2
  echo "ERROR: 期望 $expected_archive_sha256" >&2
  exit 1
fi

extract_dir="$(mktemp -d /tmp/gist1m-extract.XXXXXX)"
trap 'rm -rf -- "$extract_dir"' EXIT
tar --no-same-owner -xzf "$archive" -C "$extract_dir"
for filename in "${files[@]}"; do
  if [[ -f "$download_dir/$filename" ]]; then
    echo "已存在，保留现有文件: $filename"
    continue
  fi
  mapfile -t matches < <(find "$extract_dir" -type f -name "$filename" -print)
  if [[ "${#matches[@]}" -ne 1 ]]; then
    echo "ERROR: 压缩包中 $filename 匹配数为 ${#matches[@]}，期望 1" >&2
    exit 1
  fi
  mv "${matches[0]}" "$download_dir/$filename"
done

(
  cd "$download_dir"
  sha256sum "${files[@]}" > "$checksum_dir/GIST1M_SHA256SUMS"
)
bash "$root_dir/bin/verify-gist1m.sh"
echo "GIST1M 数据和校验清单已就绪。"
