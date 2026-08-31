#!/usr/bin/env python3
"""Verify or safely materialize a pondcapsule.2 or .3 without Pond."""

from __future__ import annotations

import argparse
import ctypes
import errno
import hashlib
import json
import math
import os
import re
import shutil
import struct
import sys
import tempfile
import unicodedata
from pathlib import Path
from typing import Any, Iterator

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
HASH_RE = re.compile(r"[0-9a-f]{64}")
ROOT_DOMAINS = {
    "pondcapsule.2": b"pondcapsule.root.2\n",
    "pondcapsule.3": b"pondcapsule.root.3\n",
}
SERIES_DOMAINS = {
    "pondcapsule.2": b"pondcapsule.series.1\n",
    "pondcapsule.3": b"pondcapsule.series.3\n",
}
LEAF_DOMAIN = b"watertown.series-leaf.v1\n"
ROWS_DOMAIN = b"watertown.series-rows.v1\n"


class CapsuleError(ValueError):
    """The capsule does not satisfy its documented pondcapsule format."""


def _fail_json_number(value: str) -> None:
    raise CapsuleError(f"unsupported JSON number {value!r}")


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise CapsuleError(f"duplicate JSON field {key!r}")
        result[key] = value
    return result


def _json(data: bytes | str) -> Any:
    try:
        return json.loads(
            data,
            object_pairs_hook=_object,
            parse_float=_fail_json_number,
            parse_constant=_fail_json_number,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CapsuleError(f"invalid JSON: {error}") from error


def _keys(value: Any, expected: set[str], where: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise CapsuleError(f"{where} must be an object")
    actual = set(value)
    if actual != expected:
        raise CapsuleError(
            f"{where} fields are {sorted(actual)!r}, expected {sorted(expected)!r}"
        )
    return value


def _text(value: Any, where: str, *, nonempty: bool = False) -> str:
    if not isinstance(value, str) or (nonempty and not value):
        suffix = " nonempty" if nonempty else ""
        raise CapsuleError(f"{where} must be a{suffix} string")
    return value


def _integer(value: Any, where: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise CapsuleError(f"{where} must be an integer")
    if value < minimum or value > maximum:
        raise CapsuleError(f"{where} is outside [{minimum}, {maximum}]")
    return value


def _i64(value: Any, where: str) -> int:
    return _integer(value, where, -(1 << 63), (1 << 63) - 1)


def _u64(value: Any, where: str) -> int:
    return _integer(value, where, 0, (1 << 64) - 1)


def _hash(value: Any, where: str) -> str:
    if not isinstance(value, str) or HASH_RE.fullmatch(value) is None:
        raise CapsuleError(f"{where} must be 64 lowercase hexadecimal characters")
    return value


def _optional_i64(value: Any, where: str) -> int | None:
    return None if value is None else _i64(value, where)


def _optional_hash(value: Any, where: str) -> str | None:
    return None if value is None else _hash(value, where)


def _ordered_manifest(value: dict[str, Any]) -> dict[str, Any]:
    def ordered_object(descriptor: dict[str, Any]) -> dict[str, Any]:
        return {"hash": descriptor["hash"], "size": descriptor["size"]}

    def ordered_leaf(leaf: dict[str, Any]) -> dict[str, Any]:
        result = {
            "logical_hash": leaf["logical_hash"],
            "logical_count": leaf["logical_count"],
            "source_timestamp": leaf["source_timestamp"],
            "min_event_time": leaf["min_event_time"],
            "max_event_time": leaf["max_event_time"],
            "logical_attributes": leaf["logical_attributes"],
        }
        if "schema_fingerprint" in leaf:
            result["schema_fingerprint"] = leaf["schema_fingerprint"]
        return result

    def ordered_node(node: dict[str, Any]) -> dict[str, Any]:
        if node["kind"] == "directory":
            return {"kind": "directory"}
        if node["kind"] == "symlink":
            return {"kind": "symlink", "target": ordered_object(node["target"])}
        if node["kind"] == "dynamic":
            return {"kind": "dynamic", "recipe": ordered_object(node["recipe"])}
        result = {
            "kind": "physical",
            "payload_kind": node["payload_kind"],
            "logical_root": node["logical_root"],
            "objects": [ordered_object(item) for item in node["objects"]],
            "leaves": [ordered_leaf(item) for item in node["leaves"]],
        }
        if value["format"] == "pondcapsule.2" or "schema_fingerprint" in node:
            result["schema_fingerprint"] = node.get("schema_fingerprint")
            # Rust's serde field order puts the optional schema before root.
            result = {
                "kind": result["kind"],
                "payload_kind": result["payload_kind"],
                "schema_fingerprint": result["schema_fingerprint"],
                "logical_root": result["logical_root"],
                "objects": result["objects"],
                "leaves": result["leaves"],
            }
        return result

    source = value["source"]
    return {
        "format": value["format"],
        "source": {
            "pond_id": source["pond_id"],
            "birthplace": source["birthplace"],
            "source_tip": source["source_tip"],
            "exported_at_micros": source["exported_at_micros"],
            "tool_version": source["tool_version"],
        },
        "entries": [
            {
                "path": entry["path"],
                "entry_type": entry["entry_type"],
                "source_node_id": entry["source_node_id"],
                "node": ordered_node(entry["node"]),
            }
            for entry in value["entries"]
        ],
    }


def _canonical_json(value: dict[str, Any]) -> bytes:
    return json.dumps(
        _ordered_manifest(value),
        ensure_ascii=False,
        separators=(",", ":"),
        # V3 passes through serde_json::Value, whose map serialization is
        # lexical; V2 retains its original struct-field ordering.
        sort_keys=value["format"] == "pondcapsule.3",
    ).encode()


def _canonical_attributes(value: Any, where: str) -> str | None:
    if value is None:
        return None
    text = _text(value, where)
    parsed = _json(text)
    if not isinstance(parsed, dict):
        raise CapsuleError(f"{where} must encode a JSON object")

    def validate(item: Any) -> None:
        if isinstance(item, dict):
            for key, child in item.items():
                _text(key, where)
                validate(child)
        elif isinstance(item, list):
            for child in item:
                validate(child)
        elif isinstance(item, bool) or item is None or isinstance(item, str):
            return
        elif isinstance(item, int):
            _integer(item, where, -(1 << 63), (1 << 64) - 1)
        else:
            raise CapsuleError(f"{where} contains an unsupported value")

    validate(parsed)
    canonical = json.dumps(
        parsed, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    )
    if canonical != text:
        raise CapsuleError(f"{where} is not canonical JSON")
    return text


def _object_descriptor(value: Any, where: str) -> dict[str, Any]:
    value = _keys(value, {"hash", "size"}, where)
    return {
        "hash": _hash(value["hash"], f"{where}.hash"),
        "size": _u64(value["size"], f"{where}.size"),
    }


def _path(value: Any, where: str) -> str:
    value = _text(value, where)
    if value == "/":
        return value
    if not value.startswith("/") or value.endswith("/"):
        raise CapsuleError(f"{where} is not a canonical absolute path")
    if any(unicodedata.category(character) == "Cc" for character in value):
        raise CapsuleError(f"{where} contains a control character")
    if any(component in ("", ".", "..") for component in value[1:].split("/")):
        raise CapsuleError(f"{where} contains a noncanonical component")
    return value


def _series_root(
    payload_kind: str, fingerprint: str | None, leaves: list[dict[str, Any]], blake3: Any,
    format: str = "pondcapsule.3",
) -> str:
    digest = blake3.blake3(SERIES_DOMAINS[format])
    digest.update(b"\0" if payload_kind == "file" else b"\1")
    digest.update(b"\0" if fingerprint is None else b"\1" + bytes.fromhex(fingerprint))
    digest.update(struct.pack("<Q", len(leaves)))
    for leaf in leaves:
        digest.update(bytes.fromhex(leaf["logical_hash"]))
        digest.update(struct.pack("<Q", leaf["logical_count"]))
        flags = int(leaf["min_event_time"] is not None)
        flags |= int(leaf["max_event_time"] is not None) << 1
        flags |= int(leaf["logical_attributes"] is not None) << 2
        leaf_schema = leaf.get("schema_fingerprint")
        if format == "pondcapsule.3" and leaf_schema is not None:
            flags |= 0x08
        digest.update(bytes([flags]))
        if leaf["min_event_time"] is not None:
            digest.update(struct.pack("<q", leaf["min_event_time"]))
        if leaf["max_event_time"] is not None:
            digest.update(struct.pack("<q", leaf["max_event_time"]))
        if leaf["logical_attributes"] is not None:
            encoded = leaf["logical_attributes"].encode()
            digest.update(struct.pack("<Q", len(encoded)) + encoded)
        if format == "pondcapsule.3" and leaf_schema is not None:
            digest.update(bytes.fromhex(leaf_schema))
    return digest.hexdigest()


def _validate_manifest(value: Any, blake3: Any) -> dict[str, Any]:
    manifest = _keys(value, {"format", "source", "entries"}, "manifest")
    format = manifest["format"]
    if format not in ROOT_DOMAINS:
        raise CapsuleError("unsupported capsule format")
    source = _keys(
        manifest["source"],
        {"pond_id", "birthplace", "source_tip", "exported_at_micros", "tool_version"},
        "manifest.source",
    )
    _text(source["pond_id"], "source.pond_id", nonempty=True)
    _text(source["birthplace"], "source.birthplace", nonempty=True)
    _hash(source["source_tip"], "source.source_tip")
    if _i64(source["exported_at_micros"], "source.exported_at_micros") <= 0:
        raise CapsuleError("source.exported_at_micros must be positive")
    _text(source["tool_version"], "source.tool_version", nonempty=True)
    if not isinstance(manifest["entries"], list):
        raise CapsuleError("manifest.entries must be an array")

    prior: bytes | None = None
    by_path: dict[str, dict[str, Any]] = {}
    object_sizes: dict[str, int] = {}
    for index, entry_value in enumerate(manifest["entries"]):
        where = f"entries[{index}]"
        entry = _keys(
            entry_value, {"path", "entry_type", "source_node_id", "node"}, where
        )
        path = _path(entry["path"], f"{where}.path")
        encoded_path = path.encode()
        if prior is not None and prior >= encoded_path:
            raise CapsuleError("capsule entries are duplicated or not in canonical path order")
        prior = encoded_path
        entry_type = _text(entry["entry_type"], f"{where}.entry_type")
        if entry_type not in ENTRY_TYPES:
            raise CapsuleError(f"{where}.entry_type is unsupported")
        _text(entry["source_node_id"], f"{where}.source_node_id", nonempty=True)
        if not isinstance(entry["node"], dict):
            raise CapsuleError(f"{where}.node must be an object")
        kind = entry["node"].get("kind")
        if kind == "directory":
            _keys(entry["node"], {"kind"}, f"{where}.node")
            if entry_type != "dir:physical":
                raise CapsuleError(f"{where} directory content/type mismatch")
        elif kind == "symlink":
            node = _keys(entry["node"], {"kind", "target"}, f"{where}.node")
            if entry_type != "symlink":
                raise CapsuleError(f"{where} symlink content/type mismatch")
            descriptor = _object_descriptor(node["target"], f"{where}.node.target")
            prior_size = object_sizes.setdefault(descriptor["hash"], descriptor["size"])
            if prior_size != descriptor["size"]:
                raise CapsuleError(f"object {descriptor['hash']} has conflicting sizes")
        elif kind == "dynamic":
            node = _keys(entry["node"], {"kind", "recipe"}, f"{where}.node")
            if entry_type not in {"dir:dynamic", "file:dynamic", "table:dynamic"}:
                raise CapsuleError(f"{where} dynamic content/type mismatch")
            descriptor = _object_descriptor(node["recipe"], f"{where}.node.recipe")
            prior_size = object_sizes.setdefault(descriptor["hash"], descriptor["size"])
            if prior_size != descriptor["size"]:
                raise CapsuleError(f"object {descriptor['hash']} has conflicting sizes")
        elif kind == "physical":
            expected_node_keys = {
                "kind", "payload_kind", "logical_root", "objects", "leaves"
            }
            if format == "pondcapsule.2" or "schema_fingerprint" in entry["node"]:
                expected_node_keys.add("schema_fingerprint")
            node = _keys(entry["node"], expected_node_keys, f"{where}.node")
            payload_kind = node["payload_kind"]
            expected = (
                {"file:physical:version", "file:physical:series"}
                if payload_kind == "file"
                else {"table:physical:version", "table:physical:series"}
                if payload_kind == "table"
                else set()
            )
            if entry_type not in expected:
                raise CapsuleError(f"{where} physical payload/type mismatch")
            fingerprint = node.get("schema_fingerprint")
            if payload_kind == "file":
                if fingerprint is not None or (
                    format == "pondcapsule.3" and "schema_fingerprint" in node
                ):
                    raise CapsuleError(f"{where} file must not declare a schema")
            elif fingerprint is not None:
                fingerprint = _hash(fingerprint, f"{where}.node.schema_fingerprint")
            elif format == "pondcapsule.2":
                raise CapsuleError(f"{where} table must declare a schema")
            elif "schema_fingerprint" in node:
                raise CapsuleError(
                    f"{where} v3 schema-evolved table must omit its node schema"
                )
            _hash(node["logical_root"], f"{where}.node.logical_root")
            if not isinstance(node["objects"], list) or not isinstance(node["leaves"], list):
                raise CapsuleError(f"{where} objects and leaves must be arrays")
            objects = []
            for object_index, descriptor_value in enumerate(node["objects"]):
                descriptor = _object_descriptor(
                    descriptor_value, f"{where}.node.objects[{object_index}]"
                )
                prior_size = object_sizes.setdefault(
                    descriptor["hash"], descriptor["size"]
                )
                if prior_size != descriptor["size"]:
                    raise CapsuleError(
                        f"object {descriptor['hash']} has conflicting sizes"
                    )
                objects.append(descriptor)
            leaves = []
            for leaf_index, leaf_value in enumerate(node["leaves"]):
                leaf_where = f"{where}.node.leaves[{leaf_index}]"
                leaf = _keys(
                    leaf_value,
                    {
                        "logical_hash",
                        "logical_count",
                        "source_timestamp",
                        "min_event_time",
                        "max_event_time",
                        "logical_attributes",
                    }
                    | (
                        {"schema_fingerprint"}
                        if isinstance(leaf_value, dict)
                        and "schema_fingerprint" in leaf_value
                        else set()
                    ),
                    leaf_where,
                )
                validated = {
                    "logical_hash": _hash(
                        leaf["logical_hash"], f"{leaf_where}.logical_hash"
                    ),
                    "logical_count": _u64(
                        leaf["logical_count"], f"{leaf_where}.logical_count"
                    ),
                    "source_timestamp": _i64(
                        leaf["source_timestamp"], f"{leaf_where}.source_timestamp"
                    ),
                    "min_event_time": _optional_i64(
                        leaf["min_event_time"], f"{leaf_where}.min_event_time"
                    ),
                    "max_event_time": _optional_i64(
                        leaf["max_event_time"], f"{leaf_where}.max_event_time"
                    ),
                    "logical_attributes": _canonical_attributes(
                        leaf["logical_attributes"],
                        f"{leaf_where}.logical_attributes",
                    ),
                }
                if "schema_fingerprint" in leaf:
                    validated["schema_fingerprint"] = _optional_hash(
                        leaf["schema_fingerprint"], f"{leaf_where}.schema_fingerprint"
                    )
                if validated["logical_count"] == 0:
                    raise CapsuleError(f"{leaf_where} has zero logical count")
                if (
                    validated["min_event_time"] is not None
                    and validated["max_event_time"] is not None
                    and validated["min_event_time"] > validated["max_event_time"]
                ):
                    raise CapsuleError(f"{leaf_where} has inverted event bounds")
                leaves.append(validated)
            if payload_kind == "file" and bool(objects) != bool(leaves):
                raise CapsuleError(f"{where} file must have both objects and leaves, or neither")
            if payload_kind == "file" and any("schema_fingerprint" in leaf for leaf in leaves):
                raise CapsuleError(f"{where} file leaf must not declare a schema")
            if payload_kind == "table" and not objects:
                raise CapsuleError(f"{where} table has no Parquet schema carrier")
            if format == "pondcapsule.3" and payload_kind == "table" and not leaves and fingerprint is None:
                raise CapsuleError(f"{where} empty table must declare a node schema")
            if format == "pondcapsule.3" and payload_kind == "table" and any(
                leaf.get("schema_fingerprint") is None for leaf in leaves
            ):
                raise CapsuleError(f"{where} v3 table leaves must declare a schema")
            if format == "pondcapsule.2" and payload_kind == "table" and any(
                "schema_fingerprint" in leaf for leaf in leaves
            ):
                raise CapsuleError(f"{where} v2 table leaves must not declare a schema")
            if payload_kind == "table" and fingerprint is not None and any(
                "schema_fingerprint" in leaf and leaf["schema_fingerprint"] != fingerprint
                for leaf in leaves
            ):
                raise CapsuleError(
                    f"{where} table leaf schema differs from its homogeneous schema"
                )
            computed = _series_root(payload_kind, fingerprint, leaves, blake3, format)
            if computed != node["logical_root"]:
                raise CapsuleError(f"{where} logical series root mismatch")
        else:
            raise CapsuleError(f"{where}.node.kind is unsupported")
        by_path[path] = entry

    root = by_path.get("/")
    if root is None or root["entry_type"] != "dir:physical" or root["node"] != {
        "kind": "directory"
    }:
        raise CapsuleError("capsule root must be a physical directory")
    for path, entry in by_path.items():
        if path == "/":
            continue
        parent = path.rsplit("/", 1)[0] or "/"
        parent_entry = by_path.get(parent)
        if parent_entry is None:
            raise CapsuleError(f"path {path!r} has missing parent {parent!r}")
        if parent_entry["node"].get("kind") != "directory":
            raise CapsuleError(f"path {path!r} has a non-directory parent")
    return manifest


def _metadata_bytes(metadata: dict[bytes, bytes] | None) -> bytes:
    pairs = sorted((metadata or {}).items())
    result = struct.pack("<I", len(pairs))
    for key, value in pairs:
        result += struct.pack("<I", len(key)) + key
        result += struct.pack("<I", len(value)) + value
    return result


def _canonical_schema(schema: Any, pa: Any) -> bytes:
    result = b"watertown.series-schema.v1\n" + struct.pack("<I", len(schema))
    for field in schema:
        name = field.name.encode()
        result += struct.pack("<I", len(name)) + name + bytes([field.nullable])
        data_type = field.type.value_type if pa.types.is_dictionary(field.type) else field.type
        fixed = [
            (pa.types.is_boolean, 0),
            (pa.types.is_int8, 1),
            (pa.types.is_int16, 2),
            (pa.types.is_int32, 3),
            (pa.types.is_int64, 4),
            (pa.types.is_uint8, 5),
            (pa.types.is_uint16, 6),
            (pa.types.is_uint32, 7),
            (pa.types.is_uint64, 8),
            (pa.types.is_float32, 9),
            (pa.types.is_float64, 10),
            (pa.types.is_string, 11),
            (pa.types.is_large_string, 12),
            (pa.types.is_binary, 13),
            (pa.types.is_large_binary, 14),
            (pa.types.is_date32, 15),
        ]
        tag = next((tag for predicate, tag in fixed if predicate(data_type)), None)
        if tag is not None:
            result += bytes([tag])
        elif pa.types.is_timestamp(data_type):
            units = {"s": 0, "ms": 1, "us": 2, "ns": 3}
            if data_type.unit not in units:
                raise CapsuleError(f"unsupported timestamp unit {data_type.unit}")
            result += bytes([16, units[data_type.unit]])
            if data_type.tz is None:
                result += b"\0"
            else:
                zone = data_type.tz.encode()
                result += b"\1" + struct.pack("<I", len(zone)) + zone
        elif pa.types.is_decimal128(data_type):
            result += bytes([17, data_type.precision, data_type.scale & 0xFF])
        else:
            raise CapsuleError(f"unsupported canonical series type {data_type}")
        result += _metadata_bytes(field.metadata)
    return result + _metadata_bytes(schema.metadata)


def _canonical_batch_rows(schema: Any, batch: Any, pa: Any) -> bytes:
    result = bytearray()
    columns = []
    for index, field in enumerate(schema):
        column = batch.column(index)
        if pa.types.is_dictionary(column.type):
            column = column.dictionary_decode()
        columns.append(
            (
                field.type.value_type
                if pa.types.is_dictionary(field.type)
                else field.type,
                column,
            )
        )
    for row in range(batch.num_rows):
        for data_type, column in columns:
            scalar = column[row]
            if not scalar.is_valid:
                result.append(0)
                continue
            result.append(1)
            value = scalar.as_py()
            if pa.types.is_boolean(data_type):
                result.append(int(value))
            elif pa.types.is_int8(data_type):
                result.extend(struct.pack("<b", value))
            elif pa.types.is_int16(data_type):
                result.extend(struct.pack("<h", value))
            elif pa.types.is_int32(data_type) or pa.types.is_date32(data_type):
                raw = (
                    column.cast(pa.int32())[row].as_py()
                    if pa.types.is_date32(data_type)
                    else value
                )
                result.extend(struct.pack("<i", raw))
            elif pa.types.is_int64(data_type) or pa.types.is_timestamp(data_type):
                raw = (
                    column.cast(pa.int64())[row].as_py()
                    if pa.types.is_timestamp(data_type)
                    else value
                )
                result.extend(struct.pack("<q", raw))
            elif pa.types.is_uint8(data_type):
                result.extend(struct.pack("<B", value))
            elif pa.types.is_uint16(data_type):
                result.extend(struct.pack("<H", value))
            elif pa.types.is_uint32(data_type):
                result.extend(struct.pack("<I", value))
            elif pa.types.is_uint64(data_type):
                result.extend(struct.pack("<Q", value))
            elif pa.types.is_float32(data_type):
                result.extend(
                    struct.pack("<I", 0x7FC00000)
                    if math.isnan(value)
                    else struct.pack("<f", value)
                )
            elif pa.types.is_float64(data_type):
                result.extend(
                    struct.pack("<Q", 0x7FF8000000000000)
                    if math.isnan(value)
                    else struct.pack("<d", value)
                )
            elif pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
                encoded = value.encode()
                result.extend(struct.pack("<Q", len(encoded)) + encoded)
            elif pa.types.is_binary(data_type) or pa.types.is_large_binary(data_type):
                result.extend(struct.pack("<Q", len(value)) + value)
            elif pa.types.is_decimal128(data_type):
                decimal = value.as_tuple()
                coefficient = 0
                for digit in decimal.digits:
                    coefficient = coefficient * 10 + digit
                if decimal.sign:
                    coefficient = -coefficient
                adjustment = decimal.exponent + data_type.scale
                if adjustment >= 0:
                    unscaled = coefficient * (10**adjustment)
                else:
                    divisor = 10**-adjustment
                    unscaled, remainder = divmod(coefficient, divisor)
                    if remainder:
                        raise CapsuleError(
                            f"decimal {value} is not exactly representable as {data_type}"
                        )
                result.extend(unscaled.to_bytes(16, "little", signed=True))
            else:
                raise CapsuleError(f"unsupported canonical series value {data_type}")
    return bytes(result)


def _leaf_hasher(
    kind: int,
    fingerprint: bytes,
    count: int,
    payload_size: int,
    leaf: dict[str, Any],
    blake3: Any,
) -> Any:
    digest = blake3.blake3()
    digest.update(LEAF_DOMAIN + bytes([kind]))
    digest.update(struct.pack("<I", len(fingerprint)) + fingerprint)
    digest.update(struct.pack("<Q", count))
    digest.update(struct.pack("<Q", payload_size))
    return digest


def _finish_leaf(digest: Any, leaf: dict[str, Any]) -> str:
    flags = int(leaf["min_event_time"] is not None)
    flags |= int(leaf["max_event_time"] is not None) << 1
    digest.update(bytes([flags]))
    if leaf["min_event_time"] is not None:
        digest.update(struct.pack("<q", leaf["min_event_time"]))
    if leaf["max_event_time"] is not None:
        digest.update(struct.pack("<q", leaf["max_event_time"]))
    attributes = (
        b""
        if leaf["logical_attributes"] is None
        else leaf["logical_attributes"].encode()
    )
    digest.update(struct.pack("<I", len(attributes)) + attributes)
    return digest.hexdigest()


def _object_path(capsule: Path, descriptor: dict[str, Any]) -> Path:
    return capsule / "recovery" / "objects" / f"blake3={descriptor['hash']}"


def _require_regular_file(path: Path, where: str) -> None:
    if path.is_symlink() or not path.is_file():
        raise CapsuleError(f"{where} must be a regular file")


def _verify_objects(
    capsule: Path, manifest: dict[str, Any], blake3: Any
) -> tuple[dict[str, dict[str, Any]], int]:
    declared: dict[str, dict[str, Any]] = {}
    for entry in manifest["entries"]:
        node = entry["node"]
        descriptors = (
            [node["target"]]
            if node["kind"] == "symlink"
            else [node["recipe"]]
            if node["kind"] == "dynamic"
            else node["objects"]
            if node["kind"] == "physical"
            else []
        )
        for descriptor in descriptors:
            prior = declared.setdefault(descriptor["hash"], descriptor)
            if prior["size"] != descriptor["size"]:
                raise CapsuleError(f"object {descriptor['hash']} has conflicting sizes")
    objects_dir = capsule / "recovery" / "objects"
    if not objects_dir.is_dir():
        raise CapsuleError("recovery/objects is missing")
    entries = list(objects_dir.iterdir())
    if any(path.is_symlink() or not path.is_file() for path in entries):
        raise CapsuleError("recovery/objects contains a non-regular entry")
    actual = {path.name for path in entries}
    expected = {f"blake3={value}" for value in declared}
    if actual != expected:
        raise CapsuleError(
            f"payload closure mismatch; missing={sorted(expected - actual)!r}, "
            f"unexpected={sorted(actual - expected)!r}"
        )
    total = 0
    for descriptor in declared.values():
        digest = blake3.blake3()
        size = 0
        with _object_path(capsule, descriptor).open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
                size += len(chunk)
        if digest.hexdigest() != descriptor["hash"] or size != descriptor["size"]:
            raise CapsuleError(
                f"payload {descriptor['hash']} has hash {digest.hexdigest()} and "
                f"size {size}, expected size {descriptor['size']}"
            )
        total += size
    return declared, total


def _object_chunks(capsule: Path, objects: list[dict[str, Any]]) -> Iterator[bytes]:
    for descriptor in objects:
        with _object_path(capsule, descriptor).open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                yield chunk


def _verify_file(capsule: Path, entry: dict[str, Any], blake3: Any) -> None:
    node = entry["node"]
    chunks = _object_chunks(capsule, node["objects"])
    current = b""
    offset = 0
    for index, leaf in enumerate(node["leaves"]):
        remaining = leaf["logical_count"]
        digest = _leaf_hasher(1, b"", remaining, remaining, leaf, blake3)
        while remaining:
            if offset == len(current):
                try:
                    current = next(chunks)
                except StopIteration as error:
                    raise CapsuleError(
                        f"file {entry['path']!r} ended during leaf {index}"
                    ) from error
                offset = 0
            take = min(remaining, len(current) - offset)
            digest.update(current[offset : offset + take])
            offset += take
            remaining -= take
        computed = _finish_leaf(digest, leaf)
        if computed != leaf["logical_hash"]:
            raise CapsuleError(f"file {entry['path']!r} leaf {index} hash mismatch")
    if offset != len(current) or next(chunks, None) is not None:
        raise CapsuleError(f"file {entry['path']!r} has bytes after its final leaf")


def _table_batches(
    capsule: Path, entry: dict[str, Any], pa: Any, pq: Any
) -> Iterator[tuple[Any, Any, str]]:
    expected = entry["node"].get("schema_fingerprint")
    canonical_schema: bytes | None = None
    for descriptor in entry["node"]["objects"]:
        parquet = pq.ParquetFile(_object_path(capsule, descriptor))
        schema = parquet.schema_arrow
        encoded = _canonical_schema(schema, pa)
        fingerprint = __import__("blake3").blake3(encoded).hexdigest()
        if expected is not None and fingerprint != expected:
            raise CapsuleError(
                f"table {entry['path']!r} object {descriptor['hash']} schema mismatch"
            )
        if expected is not None and canonical_schema is not None and canonical_schema != encoded:
            raise CapsuleError(f"table {entry['path']!r} schemas are inconsistent")
        canonical_schema = encoded
        for batch in parquet.iter_batches():
            if batch.num_rows:
                fields = []
                columns = []
                for index, field in enumerate(schema):
                    column = batch.column(index)
                    data_type = field.type
                    if pa.types.is_dictionary(data_type):
                        data_type = data_type.value_type
                        column = column.dictionary_decode()
                    fields.append(
                        pa.field(
                            field.name,
                            data_type,
                            nullable=field.nullable,
                            metadata=field.metadata,
                        )
                    )
                    columns.append(column)
                logical_schema = pa.schema(fields, metadata=schema.metadata)
                yield logical_schema, pa.RecordBatch.from_arrays(
                    columns, schema=logical_schema
                ), fingerprint
    if canonical_schema is None:
        raise CapsuleError(f"table {entry['path']!r} has no schema carrier")


def _partition_batches(
    batches: Iterator[tuple[Any, Any, str]], counts: list[int]
) -> Iterator[tuple[int, Any, Any, str]]:
    leaf_index = 0
    leaf_rows = 0
    for schema, batch, fingerprint in batches:
        offset = 0
        while offset < batch.num_rows:
            if leaf_index >= len(counts):
                raise CapsuleError("table has rows after its final logical leaf")
            take = min(counts[leaf_index] - leaf_rows, batch.num_rows - offset)
            yield leaf_index, schema, batch.slice(offset, take), fingerprint
            offset += take
            leaf_rows += take
            if leaf_rows == counts[leaf_index]:
                leaf_index += 1
                leaf_rows = 0
    if leaf_index != len(counts) or leaf_rows:
        raise CapsuleError(f"table ended after {leaf_index} of {len(counts)} leaves")


def _verify_table(
    capsule: Path, entry: dict[str, Any], blake3: Any, pa: Any, pq: Any
) -> None:
    leaves = entry["node"]["leaves"]
    counts = [leaf["logical_count"] for leaf in leaves]
    canonical_lengths = [0] * len(leaves)
    for index, schema, batch, fingerprint in _partition_batches(
        _table_batches(capsule, entry, pa, pq), counts
    ):
        expected = leaves[index].get(
            "schema_fingerprint", entry["node"].get("schema_fingerprint")
        )
        if fingerprint != expected:
            raise CapsuleError(
                f"table {entry['path']!r} physical object crosses a leaf schema transition"
            )
        canonical_lengths[index] += len(_canonical_batch_rows(schema, batch, pa))
    hashers = []
    for index, leaf in enumerate(leaves):
        fingerprint = bytes.fromhex(
            leaf.get("schema_fingerprint", entry["node"].get("schema_fingerprint"))
        )
        payload_size = len(ROWS_DOMAIN) + 8 + canonical_lengths[index]
        digest = _leaf_hasher(
            0, fingerprint, leaf["logical_count"], payload_size, leaf, blake3
        )
        digest.update(ROWS_DOMAIN + struct.pack("<Q", leaf["logical_count"]))
        hashers.append(digest)
    for index, schema, batch, fingerprint in _partition_batches(
        _table_batches(capsule, entry, pa, pq), counts
    ):
        hashers[index].update(_canonical_batch_rows(schema, batch, pa))
    for index, (leaf, digest) in enumerate(zip(leaves, hashers)):
        if _finish_leaf(digest, leaf) != leaf["logical_hash"]:
            raise CapsuleError(f"table {entry['path']!r} leaf {index} hash mismatch")


def load_and_verify(capsule: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        import blake3
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as error:
        raise CapsuleError(
            f"install capsule-requirements.lock in a reviewed environment: {error}"
        ) from error
    capsule = capsule.resolve()
    latest_path = capsule / "recovery" / "refs" / "latest"
    try:
        _require_regular_file(latest_path, "recovery/refs/latest")
        latest = latest_path.read_bytes()
    except OSError as error:
        raise CapsuleError(f"read recovery/refs/latest: {error}") from error
    if len(latest) != 65 or latest[-1:] != b"\n":
        raise CapsuleError("recovery/refs/latest must be one lowercase hash plus newline")
    try:
        root = _hash(latest[:-1].decode("ascii"), "recovery/refs/latest")
    except UnicodeDecodeError as error:
        raise CapsuleError("recovery/refs/latest is not ASCII") from error
    manifest_path = capsule / "recovery" / "manifests" / f"{root}.json"
    try:
        _require_regular_file(manifest_path, f"manifest {root}")
        manifest_bytes = manifest_path.read_bytes()
    except OSError as error:
        raise CapsuleError(f"read manifest {root}: {error}") from error
    manifest = _validate_manifest(_json(manifest_bytes), blake3)
    if _canonical_json(manifest) != manifest_bytes:
        raise CapsuleError("capsule manifest is not canonically encoded")
    computed_root = blake3.blake3(ROOT_DOMAINS[manifest["format"]] + manifest_bytes).hexdigest()
    if computed_root != root:
        raise CapsuleError(f"manifest hashes to {computed_root}, latest names {root}")
    declared, physical_bytes = _verify_objects(capsule, manifest, blake3)
    logical_count = 0
    for entry in manifest["entries"]:
        node = entry["node"]
        if node["kind"] == "dynamic":
            descriptor = node["recipe"]
            _decode_dynamic_recipe(_object_path(capsule, descriptor).read_bytes())
            continue
        if node["kind"] != "physical":
            continue
        if node["payload_kind"] == "file":
            _verify_file(capsule, entry, blake3)
        else:
            _verify_table(capsule, entry, blake3, pa, pq)
        logical_count += sum(leaf["logical_count"] for leaf in node["leaves"])
    report = {
        "root": root,
        "entries": len(manifest["entries"]),
        "payload_objects": len(declared),
        "physical_bytes": physical_bytes,
        "logical_count": logical_count,
    }
    return manifest, report


def _encoded_logical_path(path: str) -> Path:
    if path == "/":
        return Path("_root")
    component = path.rsplit("/", 1)[-1]
    hint = "".join(
        character.lower()
        if character.isascii() and character.isalnum()
        else "-"
        for character in component
    ).strip("-")
    hint = re.sub(r"-+", "-", hint)[:40] or "item"
    return Path(f"p-{hint}--{hashlib.sha256(path.encode()).hexdigest()}")


def _decode_dynamic_recipe(data: bytes) -> tuple[str, bytes]:
    if data.startswith(b"watertown.recipe.v1\n"):
        magic = b"watertown.recipe.v1\n"
    elif data.startswith(b"dp.recipe.1\n"):
        magic = b"dp.recipe.1\n"
    else:
        raise CapsuleError(
            "dynamic recipe does not use watertown.recipe.v1 or dp.recipe.1 framing"
        )
    if len(data) < len(magic) + 4:
        raise CapsuleError("dynamic recipe framing is truncated")
    name_length = struct.unpack_from("<I", data, len(magic))[0]
    name_start = len(magic) + 4
    name_end = name_start + name_length
    if name_end > len(data):
        raise CapsuleError("dynamic recipe factory name is truncated")
    try:
        factory = data[name_start:name_end].decode()
    except UnicodeDecodeError as error:
        raise CapsuleError("dynamic recipe factory name is not UTF-8") from error
    return factory, data[name_end:]


def _write_file_versions(
    capsule: Path, destination: Path, entry: dict[str, Any]
) -> list[str]:
    node = entry["node"]
    if not node["leaves"]:
        target = destination / "version-000001.bin"
        target.touch(exist_ok=False)
        return [str(target)]
    chunks = _object_chunks(capsule, node["objects"])
    current = b""
    offset = 0
    outputs = []
    for index, leaf in enumerate(node["leaves"], 1):
        target = destination / f"version-{index:06d}.bin"
        with target.open("xb") as output:
            remaining = leaf["logical_count"]
            while remaining:
                if offset == len(current):
                    current = next(chunks)
                    offset = 0
                take = min(remaining, len(current) - offset)
                output.write(current[offset : offset + take])
                offset += take
                remaining -= take
        outputs.append(str(target))
    return outputs


def _write_table_versions(
    capsule: Path, destination: Path, entry: dict[str, Any], pa: Any, pq: Any
) -> list[str]:
    leaves = entry["node"]["leaves"]
    outputs = [destination / f"version-{index:06d}.parquet" for index in range(1, len(leaves) + 1)]
    writers: dict[int, Any] = {}
    try:
        for index, schema, batch, _ in _partition_batches(
            _table_batches(
                capsule, entry, pa, pq
            ),
            [leaf["logical_count"] for leaf in leaves],
        ):
            writer = writers.get(index)
            if writer is None:
                writer = pq.ParquetWriter(outputs[index], schema)
                writers[index] = writer
            writer.write_batch(batch)
    finally:
        for writer in writers.values():
            writer.close()
    return [str(path) for path in outputs]


def _rename_no_replace(source: Path, destination: Path) -> None:
    source_bytes = os.fsencode(source)
    destination_bytes = os.fsencode(destination)
    library = ctypes.CDLL(None, use_errno=True)
    if sys.platform == "darwin":
        rename = library.renamex_np
        rename.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        result = rename(source_bytes, destination_bytes, 0x00000004)
    elif sys.platform.startswith("linux"):
        try:
            rename = library.renameat2
        except AttributeError as error:
            raise CapsuleError(
                "this Linux runtime lacks atomic no-replace rename support"
            ) from error
        rename.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        result = rename(-100, source_bytes, -100, destination_bytes, 0x00000001)
    elif os.name == "nt":
        try:
            source.rename(destination)
            return
        except FileExistsError as error:
            raise CapsuleError(f"destination already exists: {destination}") from error
    else:
        raise CapsuleError(
            f"atomic no-replace materialization is unsupported on {sys.platform}"
        )
    if result == 0:
        return
    error_number = ctypes.get_errno()
    if error_number in (errno.EEXIST, errno.ENOTEMPTY):
        raise CapsuleError(f"destination already exists: {destination}")
    raise OSError(error_number, os.strerror(error_number), destination)


def materialize(capsule: Path, destination: Path) -> dict[str, Any]:
    if destination.exists():
        raise CapsuleError(f"destination already exists: {destination}")
    if not destination.parent.is_dir():
        raise CapsuleError(f"destination parent does not exist: {destination.parent}")
    manifest, report = load_and_verify(capsule)
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as error:
        raise CapsuleError(str(error)) from error
    with tempfile.TemporaryDirectory(
        prefix=f".{destination.name}.partial-", dir=destination.parent
    ) as temporary:
        staging = Path(temporary) / "materialized"
        staging.mkdir(mode=0o700)
        inventory = {
            "capsule": str(capsule.resolve()),
            "capsule_root": report["root"],
            "source": manifest["source"],
            "entries": [],
        }
        for entry in manifest["entries"]:
            node = entry["node"]
            kind = node["kind"]
            root_name = (
                "directories"
                if kind == "directory"
                else "symlinks"
                if kind == "symlink"
                else "dynamic-recipes"
                if kind == "dynamic"
                else "files"
                if node["payload_kind"] == "file"
                else "tables"
            )
            target_dir = staging / root_name / _encoded_logical_path(entry["path"])
            target_dir.mkdir(parents=True)
            outputs: list[str] = []
            if kind == "symlink":
                target = target_dir / "target.bin"
                shutil.copyfile(_object_path(capsule, node["target"]), target)
                outputs.append(str(target.relative_to(staging)))
            elif kind == "dynamic":
                target = target_dir / "recipe.bin"
                shutil.copyfile(_object_path(capsule, node["recipe"]), target)
                outputs.append(str(target.relative_to(staging)))
                factory, configuration = _decode_dynamic_recipe(target.read_bytes())
                factory_target = target_dir / "factory.json"
                factory_target.write_text(
                    json.dumps({"factory": factory}, ensure_ascii=False, indent=2) + "\n",
                    encoding="utf-8",
                )
                config_target = target_dir / "config.bin"
                config_target.write_bytes(configuration)
                outputs.extend(
                    [
                        str(factory_target.relative_to(staging)),
                        str(config_target.relative_to(staging)),
                    ]
                )
            elif kind == "physical" and node["payload_kind"] == "file":
                outputs = [
                    str(Path(value).relative_to(staging))
                    for value in _write_file_versions(capsule, target_dir, entry)
                ]
            elif kind == "physical":
                outputs = [
                    str(Path(value).relative_to(staging))
                    for value in _write_table_versions(
                        capsule, target_dir, entry, pa, pq
                    )
                ]
                if not node["leaves"]:
                    for index, descriptor in enumerate(node["objects"], 1):
                        target = target_dir / f"schema-carrier-{index:06d}.parquet"
                        shutil.copyfile(_object_path(capsule, descriptor), target)
                        outputs.append(str(target.relative_to(staging)))
            inventory["entries"].append(
                {
                    "logical_path": entry["path"],
                    "entry_type": entry["entry_type"],
                    "source_node_id": entry["source_node_id"],
                    "materialized_type": root_name,
                    "outputs": outputs,
                    "metadata": node.get("leaves", []),
                    "schema_fingerprint": node.get("schema_fingerprint"),
                    "logical_root": node.get("logical_root"),
                }
            )
        (staging / "inventory.json").write_text(
            json.dumps(inventory, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        lines = [
            "Pond-free capsule materialization",
            f"Capsule root: {report['root']}",
            f"Source pond: {manifest['source']['pond_id']}",
            "",
            "No symlink was activated and no dynamic recipe was executed.",
            "Logical path components use readable hints plus SHA-256 digests of exact UTF-8.",
            "See inventory.json for exact mappings, hashes, and leaf metadata.",
            "",
        ]
        for item in inventory["entries"]:
            outputs = ", ".join(item["outputs"]) or "(metadata only)"
            lines.append(
                f"{item['logical_path']} [{item['entry_type']}] -> {outputs}"
            )
        (staging / "README.txt").write_text(
            "\n".join(lines) + "\n", encoding="utf-8"
        )
        _rename_no_replace(staging, destination)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    verify_parser = commands.add_parser("verify", help="deeply verify a capsule")
    verify_parser.add_argument("capsule", type=Path)
    materialize_parser = commands.add_parser(
        "materialize", help="verify and export inert recovery data"
    )
    materialize_parser.add_argument("capsule", type=Path)
    materialize_parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "verify":
            _, report = load_and_verify(args.capsule)
            print(
                "verified capsule {root}: {entries} entries, {payload_objects} objects, "
                "{physical_bytes} physical bytes, {logical_count} logical bytes/rows".format(
                    **report
                )
            )
        else:
            report = materialize(args.capsule, args.destination)
            print(
                f"materialized verified capsule {report['root']} to {args.destination}"
            )
        return 0
    except (CapsuleError, OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
