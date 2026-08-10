#!/usr/bin/env python3
"""校验活动索引 artifact，或验证空 Registry 并删除残留文件。"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlparse


REGISTRY_HEAD_PROPERTY = "huawei.gauss-infra.index-registry-head"
INDEX_CONTRACTS = {
    "ivf_flat": ("builtin.ivf_flat@2", "builtin.ivf_flat_v2_"),
    "ivf_pq": ("builtin.ivf_pq@1", "builtin.ivf_pq_v1_"),
}


def local_path(location: str) -> Path:
    """将绝对本地路径或 file URI 转为 Path。"""
    parsed = urlparse(location)
    if parsed.scheme == "":
        path = Path(location)
    elif parsed.scheme == "file" and parsed.netloc in ("", "localhost"):
        path = Path(unquote(parsed.path))
    else:
        raise ValueError(f"仅支持绝对本地路径或 file URI: {location}")
    if not path.is_absolute():
        raise ValueError(f"location 必须指向绝对路径: {location}")
    return path.resolve()


def read_registry(path: Path) -> tuple[bytes, dict]:
    """读取 Registry Puffin 开头的 JSON payload。"""
    content = path.read_bytes()
    if not content.startswith(b"PFA1{"):
        raise ValueError(f"Registry Puffin magic 无效: {path}")
    depth = 0
    in_string = False
    escaped = False
    payload_end = None
    for offset, byte in enumerate(content[4:], start=4):
        if in_string:
            if escaped:
                escaped = False
            elif byte == ord("\\"):
                escaped = True
            elif byte == ord('"'):
                in_string = False
            continue
        if byte == ord('"'):
            in_string = True
        elif byte == ord("{"):
            depth += 1
        elif byte == ord("}"):
            depth -= 1
            if depth == 0:
                payload_end = offset + 1
                break
    if payload_end is None:
        raise ValueError(f"Registry payload JSON 不完整: {path}")
    payload = json.loads(content[4:payload_end].decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Registry payload 必须是 JSON object")
    return content, payload


def clean_residual_artifacts(metadata_uri: str) -> tuple[Optional[Path], list[Path]]:
    """仅在当前 Registry 为空时删除其余索引文件。"""
    metadata_path = local_path(metadata_uri)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    table_uri = metadata.get("location")
    properties = metadata.get("properties")
    if not isinstance(table_uri, str) or not isinstance(properties, dict):
        raise ValueError("当前 metadata 缺少 location 或 properties")
    table_root = local_path(table_uri)
    metadata_path.relative_to(table_root)
    if table_root == Path("/"):
        raise ValueError("拒绝以根目录作为 Iceberg 表目录")

    index_root = table_root / "indices"
    raw_head = properties.get(REGISTRY_HEAD_PROPERTY)
    if raw_head is None and not index_root.exists():
        return None, []
    if not isinstance(raw_head, str):
        raise ValueError("当前 metadata 缺少 RegistryHeadV2")
    head = json.loads(raw_head)
    registry_uri = head.get("registry_path")
    if not isinstance(registry_uri, str):
        raise ValueError("RegistryHeadV2 缺少 registry_path")
    registry_path = local_path(registry_uri)
    registry_path.relative_to(index_root)

    content, registry = read_registry(registry_path)
    if len(content) != head.get("file_size_bytes"):
        raise ValueError("当前 Registry 文件大小与 head 不一致")
    if hashlib.sha256(content).hexdigest() != head.get("content_sha256"):
        raise ValueError("当前 Registry SHA-256 与 head 不一致")
    if registry.get("table_uuid") != metadata.get("table-uuid"):
        raise ValueError("当前 Registry table_uuid 与 metadata 不一致")
    if registry.get("indexes") != []:
        raise ValueError("当前 Registry 仍含活动索引，拒绝删除 artifact")

    residual: list[Path] = []
    if index_root.is_dir():
        for candidate in index_root.rglob("*"):
            if candidate.is_symlink():
                raise ValueError(f"索引目录含符号链接，拒绝清理: {candidate}")
            if candidate.is_file() and candidate.resolve() != registry_path:
                residual.append(candidate)
        for candidate in residual:
            candidate.unlink()
        for directory in sorted(
            (path for path in index_root.rglob("*") if path.is_dir()),
            key=lambda path: len(path.parts),
            reverse=True,
        ):
            if not any(directory.iterdir()):
                directory.rmdir()

    remaining = [
        path for path in index_root.rglob("*")
        if path.is_file() and path.resolve() != registry_path
    ]
    if remaining:
        raise RuntimeError(f"索引目录仍有 {len(remaining)} 个残留文件")
    return registry_path, residual


def verify_active_artifact(
    metadata_uri: str, index_name: str, index_type: str
) -> tuple[str, list[Path]]:
    """校验活动向量索引的 canonical implementation 和落盘 artifact。"""
    try:
        expected_implementation, expected_prefix = INDEX_CONTRACTS[index_type]
    except KeyError as exc:
        raise ValueError(f"artifact 门禁不支持 index_type={index_type}") from exc

    metadata_path = local_path(metadata_uri)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    table_root = local_path(metadata["location"])
    properties = metadata.get("properties")
    if not isinstance(properties, dict):
        raise ValueError("当前 metadata 缺少 properties")
    raw_head = properties.get(REGISTRY_HEAD_PROPERTY)
    if not isinstance(raw_head, str):
        raise ValueError("当前 metadata 缺少 RegistryHeadV2")
    head = json.loads(raw_head)
    registry_path = local_path(head["registry_path"])
    registry_path.relative_to(table_root / "indices")
    content, registry = read_registry(registry_path)
    if len(content) != head.get("file_size_bytes"):
        raise ValueError("当前 Registry 文件大小与 head 不一致")
    if hashlib.sha256(content).hexdigest() != head.get("content_sha256"):
        raise ValueError("当前 Registry SHA-256 与 head 不一致")
    if registry.get("table_uuid") != metadata.get("table-uuid"):
        raise ValueError("当前 Registry table_uuid 与 metadata 不一致")

    matches = [
        entry for entry in registry.get("indexes", [])
        if entry.get("definition", {}).get("name") == index_name
    ]
    if len(matches) != 1:
        raise ValueError(f"Registry 中索引 {index_name} 的条目数为 {len(matches)}")
    entry = matches[0]
    implementation = entry.get("definition", {}).get("implementation")
    if implementation != expected_implementation:
        raise ValueError(
            f"Registry implementation={implementation!r}，"
            f"期望 {expected_implementation!r}"
        )
    if entry.get("state") != "active":
        raise ValueError(f"Registry 索引状态为 {entry.get('state')!r}，期望 active")

    artifacts: list[Path] = []
    for partition in entry.get("partitions", []):
        for segment in partition.get("segments", []):
            for artifact in segment.get("artifact_files", []):
                path = local_path(artifact["uri"])
                path.relative_to(table_root / "indices")
                if not path.name.startswith(expected_prefix):
                    raise ValueError(
                        f"artifact 文件名 {path.name!r} 不符合前缀 {expected_prefix!r}"
                    )
                if not path.is_file():
                    raise FileNotFoundError(f"artifact 不存在: {path}")
                if path.stat().st_size != artifact.get("size_bytes"):
                    raise ValueError(f"artifact 文件大小与 Registry 不一致: {path}")
                artifacts.append(path)
    if not artifacts:
        raise ValueError(f"Registry 索引 {index_name} 没有 artifact_files")
    return implementation, artifacts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata-location", required=True)
    parser.add_argument("--expect-index-name")
    parser.add_argument("--expect-index-type", choices=sorted(INDEX_CONTRACTS))
    args = parser.parse_args()
    if args.expect_index_name or args.expect_index_type:
        if not args.expect_index_name or not args.expect_index_type:
            parser.error("--expect-index-name 和 --expect-index-type 必须同时提供")
        implementation, artifacts = verify_active_artifact(
            args.metadata_location, args.expect_index_name, args.expect_index_type
        )
        print(f"Registry implementation: {implementation}")
        print(f"索引 artifact 文件数: {len(artifacts)}")
    else:
        registry_path, removed = clean_residual_artifacts(args.metadata_location)
        if registry_path is None:
            print("当前表没有 Registry 和索引目录。")
        else:
            print(f"当前空 Registry: {registry_path}")
        print(f"残留索引文件删除数: {len(removed)}")


if __name__ == "__main__":
    main()
