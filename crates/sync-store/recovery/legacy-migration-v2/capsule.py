#!/usr/bin/env python3
"""Verify or safely materialize an opaque pondcapsule.legacy.2."""

from __future__ import annotations

import argparse
import ctypes
import errno
import hashlib
import json
import os
import shutil
import struct
import sys
import uuid
from pathlib import Path
from typing import Any

ROOT_DOMAIN = b"pondcapsule.legacy.root.2\n"
ENTRY_TYPES = {
    "dir:physical",
    "dir:dynamic",
    "symlink",
    "file:physical:version",
    "file:dynamic",
    "table:physical:version",
    "file:physical:series",
    "table:physical:series",
    "table:dynamic",
}


class CapsuleError(ValueError):
    """The capsule violates pondcapsule.legacy.2."""


def _keys(value: Any, expected: set[str], where: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise CapsuleError(f"{where} must be an object")
    actual = set(value)
    if actual != expected:
        raise CapsuleError(
            f"{where} fields differ: missing {sorted(expected - actual)}, "
            f"extra {sorted(actual - expected)}"
        )
    return value


def _text(value: Any, where: str, *, empty: bool = False) -> str:
    if not isinstance(value, str) or (not empty and not value):
        raise CapsuleError(f"{where} must be a{' possibly empty' if empty else ' nonempty'} string")
    return value


def _integer(value: Any, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise CapsuleError(f"{where} must be an integer")
    return value


def _optional_integer(value: Any, where: str) -> int | None:
    return None if value is None else _integer(value, where)


def _hash(value: Any, where: str) -> str:
    text = _text(value, where)
    if (
        len(text) != 64
        or text.lower() != text
        or any(character not in "0123456789abcdef" for character in text)
    ):
        raise CapsuleError(f"{where} must be 64 lowercase hexadecimal characters")
    return text


def _object(value: Any, where: str) -> dict[str, Any]:
    value = _keys(value, {"hash", "size"}, where)
    digest = _hash(value["hash"], f"{where}.hash")
    size = _integer(value["size"], f"{where}.size")
    if size < 0:
        raise CapsuleError(f"{where}.size must not be negative")
    return {"hash": digest, "size": size}


def _path(value: Any, where: str) -> str:
    path = _text(value, where)
    if path == "/":
        return path
    if not path.startswith("/") or path.endswith("/") or "\0" in path:
        raise CapsuleError(f"unsafe capsule path {path!r}")
    if any(part in ("", ".", "..") for part in path[1:].split("/")):
        raise CapsuleError(f"unsafe capsule path {path!r}")
    return path


def _decode_series(data: bytes) -> list[str]:
    magic = b"dp.series.1\n"
    if not data.startswith(magic) or len(data) < len(magic) + 4:
        raise CapsuleError("source series object is not dp.series.1")
    count = struct.unpack("<I", data[len(magic) : len(magic) + 4])[0]
    body = data[len(magic) + 4 :]
    if len(body) != count * 32:
        raise CapsuleError("source dp.series.1 object is truncated or has trailing bytes")
    return [body[index : index + 32].hex() for index in range(0, len(body), 32)]


def _decode_recipe(data: bytes) -> tuple[str, bytes]:
    magic = b"dp.recipe.1\n"
    if not data.startswith(magic) or len(data) < len(magic) + 4:
        raise CapsuleError("dynamic recipe is not dp.recipe.1")
    length = struct.unpack("<I", data[len(magic) : len(magic) + 4])[0]
    end = len(magic) + 4 + length
    if end > len(data):
        raise CapsuleError("dynamic recipe factory is truncated")
    try:
        factory = data[len(magic) + 4 : end].decode("utf-8")
    except UnicodeDecodeError as error:
        raise CapsuleError(f"dynamic recipe factory is not UTF-8: {error}") from error
    if not factory:
        raise CapsuleError("dynamic recipe factory is empty")
    return factory, data[end:]


def _validate_manifest(value: Any) -> tuple[dict[str, Any], dict[str, int]]:
    manifest = _keys(value, {"format", "source", "entries"}, "manifest")
    if manifest["format"] != "pondcapsule.legacy.2":
        raise CapsuleError("manifest format is not pondcapsule.legacy.2")
    source = _keys(
        manifest["source"],
        {
            "pond_id",
            "birthplace",
            "source_tip",
            "exported_at_micros",
            "tool_version",
            "native_format",
        },
        "source",
    )
    _text(source["pond_id"], "source.pond_id")
    _text(source["birthplace"], "source.birthplace")
    _hash(source["source_tip"], "source.source_tip")
    if _integer(source["exported_at_micros"], "source.exported_at_micros") <= 0:
        raise CapsuleError("source.exported_at_micros must be positive")
    _text(source["tool_version"], "source.tool_version")
    if source["native_format"] != "dp.commit.3":
        raise CapsuleError("source.native_format is not dp.commit.3")
    if not isinstance(manifest["entries"], list):
        raise CapsuleError("entries must be an array")

    prior: bytes | None = None
    paths: dict[str, dict[str, Any]] = {}
    node_ids: set[str] = set()
    objects: dict[str, int] = {}

    def declare(obj: dict[str, Any]) -> None:
        prior_size = objects.setdefault(obj["hash"], obj["size"])
        if prior_size != obj["size"]:
            raise CapsuleError(f"object {obj['hash']} has conflicting sizes")

    for index, raw_entry in enumerate(manifest["entries"]):
        entry = _keys(
            raw_entry,
            {"path", "entry_type", "source_node_id", "node"},
            f"entries[{index}]",
        )
        path = _path(entry["path"], f"entries[{index}].path")
        encoded = path.encode()
        if prior is not None and prior >= encoded:
            raise CapsuleError("entries are not uniquely sorted by UTF-8 path bytes")
        prior = encoded
        entry_type = _text(entry["entry_type"], f"entries[{index}].entry_type")
        if entry_type not in ENTRY_TYPES:
            raise CapsuleError(f"unknown entry type {entry_type!r}")
        node_id = _text(entry["source_node_id"], f"entries[{index}].source_node_id")
        if node_id in node_ids:
            raise CapsuleError(f"duplicate source node ID {node_id!r}")
        node_ids.add(node_id)
        node = entry["node"]
        if not isinstance(node, dict) or "kind" not in node:
            raise CapsuleError(f"entries[{index}].node must have a kind")
        kind = node["kind"]
        if kind == "directory":
            _keys(node, {"kind"}, f"entries[{index}].node")
            if entry_type != "dir:physical":
                raise CapsuleError(f"directory node/type mismatch at {path!r}")
        elif kind == "symlink":
            _keys(node, {"kind", "target"}, f"entries[{index}].node")
            if entry_type != "symlink":
                raise CapsuleError(f"symlink node/type mismatch at {path!r}")
            declare(_object(node["target"], f"entries[{index}].node.target"))
        elif kind == "dynamic":
            _keys(node, {"kind", "recipe", "metadata"}, f"entries[{index}].node")
            if entry_type not in {"dir:dynamic", "file:dynamic", "table:dynamic"}:
                raise CapsuleError(f"dynamic node/type mismatch at {path!r}")
            declare(_object(node["recipe"], f"entries[{index}].node.recipe"))
            metadata = _keys(
                node["metadata"],
                {"timestamp"},
                f"entries[{index}].node.metadata",
            )
            _integer(metadata["timestamp"], f"entries[{index}].node.metadata.timestamp")
        elif kind == "physical":
            _keys(
                node,
                {
                    "kind",
                    "payload_kind",
                    "source_child_hash",
                    "series_object",
                    "versions",
                },
                f"entries[{index}].node",
            )
            expected_kind = "file" if entry_type.startswith("file:") else "table"
            if (
                entry_type
                not in {
                    "file:physical:version",
                    "file:physical:series",
                    "table:physical:version",
                    "table:physical:series",
                }
                or node["payload_kind"] != expected_kind
            ):
                raise CapsuleError(f"physical node/type mismatch at {path!r}")
            child_hash = _hash(
                node["source_child_hash"],
                f"entries[{index}].node.source_child_hash",
            )
            is_series = entry_type.endswith(":series")
            if is_series:
                series_object = _object(
                    node["series_object"],
                    f"entries[{index}].node.series_object",
                )
                if series_object["hash"] != child_hash:
                    raise CapsuleError(f"source series object/hash mismatch at {path!r}")
                declare(series_object)
            elif node["series_object"] is not None:
                raise CapsuleError(f"singleton {path!r} carries a series object")
            if not isinstance(node["versions"], list) or not node["versions"]:
                raise CapsuleError(f"physical path {path!r} has no versions")
            if not is_series and len(node["versions"]) != 1:
                raise CapsuleError(f"singleton {path!r} must have one version")
            for version_index, raw_version in enumerate(node["versions"]):
                version = _keys(
                    raw_version,
                    {
                        "source_version",
                        "objects",
                        "source_timestamp",
                        "min_event_time",
                        "max_event_time",
                        "extended_attributes",
                    },
                    f"entries[{index}].node.versions[{version_index}]",
                )
                if (
                    _integer(
                        version["source_version"],
                        f"entries[{index}].node.versions[{version_index}].source_version",
                    )
                    != version_index
                ):
                    raise CapsuleError(f"noncanonical source version at {path!r}")
                if not isinstance(version["objects"], list) or len(version["objects"]) != 1:
                    raise CapsuleError(
                        f"{path!r} version {version_index} must map to one object"
                    )
                obj = _object(
                    version["objects"][0],
                    f"entries[{index}].node.versions[{version_index}].objects[0]",
                )
                declare(obj)
                _optional_integer(
                    version["source_timestamp"],
                    f"entries[{index}].node.versions[{version_index}].source_timestamp",
                )
                minimum = _optional_integer(
                    version["min_event_time"],
                    f"entries[{index}].node.versions[{version_index}].min_event_time",
                )
                maximum = _optional_integer(
                    version["max_event_time"],
                    f"entries[{index}].node.versions[{version_index}].max_event_time",
                )
                if minimum is not None and maximum is not None and minimum > maximum:
                    raise CapsuleError(f"event bounds are reversed at {path!r}")
                attributes = version["extended_attributes"]
                if attributes is not None:
                    attributes = _text(
                        attributes,
                        f"entries[{index}].node.versions[{version_index}].extended_attributes",
                        empty=True,
                    )
                    try:
                        decoded = json.loads(attributes)
                    except json.JSONDecodeError as error:
                        raise CapsuleError(
                            f"invalid extended attributes at {path!r}: {error}"
                        ) from error
                    if not isinstance(decoded, dict):
                        raise CapsuleError(
                            f"extended attributes at {path!r} are not an object"
                        )
            if not is_series and node["versions"][0]["objects"][0]["hash"] != child_hash:
                raise CapsuleError(f"singleton source object/hash mismatch at {path!r}")
        else:
            raise CapsuleError(f"unknown node kind {kind!r}")
        paths[path] = entry

    root = paths.get("/")
    if root is None or root["entry_type"] != "dir:physical":
        raise CapsuleError("manifest root is not a physical directory")
    for path, entry in paths.items():
        if path == "/":
            continue
        parent = path.rsplit("/", 1)[0] or "/"
        parent_entry = paths.get(parent)
        if parent_entry is None or parent_entry["node"]["kind"] != "directory":
            raise CapsuleError(f"path {path!r} has missing/non-directory parent")
    return manifest, objects


def _read_object(path: Path, expected_hash: str, expected_size: int, blake3: Any) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise CapsuleError(f"object {path} is not a regular file")
    data = path.read_bytes()
    actual = blake3.blake3(data).hexdigest()
    if actual != expected_hash or len(data) != expected_size:
        raise CapsuleError(
            f"object {expected_hash} has hash {actual} and size {len(data)}, "
            f"expected size {expected_size}"
        )
    return data


def _verify_object(
    path: Path, expected_hash: str, expected_size: int, blake3: Any
) -> None:
    if path.is_symlink() or not path.is_file():
        raise CapsuleError(f"object {path} is not a regular file")
    digest = blake3.blake3()
    size = 0
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    actual = digest.hexdigest()
    if actual != expected_hash or size != expected_size:
        raise CapsuleError(
            f"object {expected_hash} has hash {actual} and size {size}, "
            f"expected size {expected_size}"
        )


def load_and_verify(root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        import blake3
    except ImportError as error:
        raise CapsuleError(f"install capsule-requirements.lock: {error}") from error
    latest = root / "recovery/refs/latest"
    if latest.is_symlink() or not latest.is_file():
        raise CapsuleError("recovery/refs/latest is not a regular file")
    reference = latest.read_bytes()
    if len(reference) != 65 or reference[64:] != b"\n":
        raise CapsuleError("latest ref must be exactly 64 lowercase hex bytes plus newline")
    try:
        root_text = reference[:64].decode("ascii")
    except UnicodeDecodeError as error:
        raise CapsuleError(f"latest ref is not ASCII: {error}") from error
    capsule_root = _hash(root_text, "latest ref")
    manifest_path = root / f"recovery/manifests/{capsule_root}.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise CapsuleError("named manifest is not a regular file")
    encoded = manifest_path.read_bytes()
    try:
        value = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CapsuleError(f"manifest is not valid canonical UTF-8 JSON: {error}") from error
    manifest, objects = _validate_manifest(value)
    canonical = json.dumps(
        manifest, ensure_ascii=False, separators=(",", ":")
    ).encode()
    if canonical != encoded:
        raise CapsuleError("manifest is not canonically encoded")
    computed_root = blake3.blake3(ROOT_DOMAIN + encoded).hexdigest()
    if computed_root != capsule_root:
        raise CapsuleError(
            f"manifest hashes to {computed_root}, latest ref names {capsule_root}"
        )
    objects_dir = root / "recovery/objects"
    if objects_dir.is_symlink() or not objects_dir.is_dir():
        raise CapsuleError("recovery/objects is not a directory")
    expected_names = {f"blake3={digest}" for digest in objects}
    actual_names = {entry.name for entry in objects_dir.iterdir()}
    if actual_names != expected_names:
        raise CapsuleError(
            f"object closure mismatch: missing {sorted(expected_names - actual_names)}, "
            f"extra {sorted(actual_names - expected_names)}"
        )
    for digest, size in objects.items():
        _verify_object(
            objects_dir / f"blake3={digest}", digest, size, blake3
        )

    def payload(digest: str) -> bytes:
        return _read_object(
            objects_dir / f"blake3={digest}",
            digest,
            objects[digest],
            blake3,
        )

    physical_versions = 0
    for entry in manifest["entries"]:
        node = entry["node"]
        if node["kind"] == "symlink":
            try:
                payload(node["target"]["hash"]).decode("utf-8")
            except UnicodeDecodeError as error:
                raise CapsuleError(
                    f"symlink target for {entry['path']!r} is not UTF-8: {error}"
                ) from error
        elif node["kind"] == "dynamic":
            _decode_recipe(payload(node["recipe"]["hash"]))
        elif node["kind"] == "physical":
            physical_versions += len(node["versions"])
            if node["series_object"] is not None:
                mapped = _decode_series(payload(node["series_object"]["hash"]))
                declared = [
                    version["objects"][0]["hash"] for version in node["versions"]
                ]
                if mapped != declared:
                    raise CapsuleError(
                        f"source leaf mapping mismatch for {entry['path']!r}: "
                        f"dp.series.1 names {mapped}, manifest names {declared}"
                    )
    return manifest, {
        "root": capsule_root,
        "entries": len(manifest["entries"]),
        "objects": len(objects),
        "physical_bytes": sum(objects.values()),
        "physical_versions": physical_versions,
    }


def _encoded_path(path: str) -> str:
    basename = "root" if path == "/" else path.rsplit("/", 1)[-1]
    hint = "".join(
        character if character.isascii() and character.isalnum() else "_"
        for character in basename
    )[:32]
    return f"{hint}-{hashlib.sha256(path.encode()).hexdigest()}"


def _rename_no_replace(source: Path, destination: Path) -> None:
    source_bytes = os.fsencode(source)
    destination_bytes = os.fsencode(destination)
    library = ctypes.CDLL(None, use_errno=True)
    if sys.platform == "darwin":
        rename = library.renamex_np
        rename.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        result = rename(source_bytes, destination_bytes, 0x00000004)
    elif sys.platform.startswith("linux"):
        rename = library.renameat2
        rename.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        result = rename(-100, source_bytes, -100, destination_bytes, 0x00000001)
    else:
        raise CapsuleError(f"atomic no-replace rename unsupported on {sys.platform}")
    if result == 0:
        return
    number = ctypes.get_errno()
    if number in (errno.EEXIST, errno.ENOTEMPTY):
        raise CapsuleError(f"destination already exists: {destination}")
    raise OSError(number, os.strerror(number), destination)


def materialize(root: Path, destination: Path) -> None:
    if destination.exists() or destination.is_symlink():
        raise CapsuleError(f"destination already exists: {destination}")
    if not destination.parent.is_dir():
        raise CapsuleError(f"destination parent does not exist: {destination.parent}")
    manifest, report = load_and_verify(root)
    staging = destination.parent / f".{destination.name}.partial-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    inventory = []
    objects_dir = root / "recovery/objects"
    for entry in manifest["entries"]:
        node = entry["node"]
        encoded = _encoded_path(entry["path"])
        if node["kind"] == "directory":
            category = "directories"
            (staging / category / encoded).mkdir(parents=True)
        elif node["kind"] == "symlink":
            category = "symlinks"
            output = staging / category / encoded
            output.mkdir(parents=True)
            shutil.copyfile(
                objects_dir / f"blake3={node['target']['hash']}",
                output / "target.bin",
            )
        elif node["kind"] == "dynamic":
            category = "dynamic-recipes"
            output = staging / category / encoded
            output.mkdir(parents=True)
            recipe = (
                objects_dir / f"blake3={node['recipe']['hash']}"
            ).read_bytes()
            factory, config = _decode_recipe(recipe)
            (output / "recipe.bin").write_bytes(recipe)
            (output / "factory.json").write_text(
                json.dumps(
                    {"factory": factory},
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                + "\n"
            )
            (output / "config.bin").write_bytes(config)
            (output / "metadata.json").write_text(
                json.dumps(node["metadata"], separators=(",", ":")) + "\n"
            )
        else:
            category = "tables" if node["payload_kind"] == "table" else "files"
            output = staging / category / encoded
            output.mkdir(parents=True)
            suffix = "parquet" if node["payload_kind"] == "table" else "bin"
            for version in node["versions"]:
                for object_index, obj in enumerate(version["objects"]):
                    shutil.copyfile(
                        objects_dir / f"blake3={obj['hash']}",
                        output
                        / (
                            f"version-{version['source_version'] + 1:06d}"
                            f"-object-{object_index + 1:06d}.{suffix}"
                        ),
                    )
            (output / "versions.json").write_text(
                json.dumps(node["versions"], ensure_ascii=False, separators=(",", ":"))
                + "\n"
            )
        inventory_entry = {
            "path": entry["path"],
            "entry_type": entry["entry_type"],
            "category": category,
            "encoded_path": encoded,
        }
        if node["kind"] == "dynamic":
            inventory_entry["metadata"] = node["metadata"]
        inventory.append(inventory_entry)
    (staging / "inventory.json").write_text(
        json.dumps(
            {"capsule": report, "entries": inventory},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    )
    _rename_no_replace(staging, destination)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    verify = subcommands.add_parser("verify")
    verify.add_argument("capsule", type=Path)
    materialize_command = subcommands.add_parser("materialize")
    materialize_command.add_argument("capsule", type=Path)
    materialize_command.add_argument("destination", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "verify":
            _, report = load_and_verify(args.capsule)
            print(json.dumps(report, sort_keys=True))
        else:
            materialize(args.capsule, args.destination)
            print(f"materialized opaque legacy capsule to {args.destination}")
        return 0
    except (CapsuleError, OSError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
