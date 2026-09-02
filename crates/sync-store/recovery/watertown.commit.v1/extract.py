#!/usr/bin/env python3
"""Extract a pondcapsule.4 from a local watertown.commit.v1 Delta backup."""

from __future__ import annotations

import argparse
import ctypes
import errno
import json
import math
import os
import shutil
import struct
import sys
import tempfile
from pathlib import Path
from typing import Any, Iterator

from parquet_schema import read_parquet_schema

ENTRY_TYPES = {
    1: "dir:physical",
    2: "dir:dynamic",
    3: "symlink",
    4: "file:physical:version",
    5: "file:dynamic",
    6: "table:physical:version",
    7: "file:physical:series",
    8: "table:physical:series",
    9: "table:dynamic",
}
ENTRY_CODES = {name: code for code, name in ENTRY_TYPES.items()}
NIL_UUID = "00000000-0000-0000-0000-000000000000"


class FormatError(ValueError):
    """The source does not satisfy the documented native format."""


class Cursor:
    def __init__(self, data: bytes):
        self.data = data
        self.offset = 0

    def take(self, count: int) -> bytes:
        end = self.offset + count
        if count < 0 or end > len(self.data):
            raise FormatError("truncated native object")
        value = self.data[self.offset:end]
        self.offset = end
        return value

    def tag(self, expected: bytes) -> None:
        if self.take(len(expected)) != expected:
            raise FormatError(f"expected native tag {expected!r}")

    def u8(self) -> int:
        return self.take(1)[0]

    def u32(self) -> int:
        return struct.unpack("<I", self.take(4))[0]

    def u64(self) -> int:
        return struct.unpack("<Q", self.take(8))[0]

    def i64(self) -> int:
        return struct.unpack("<q", self.take(8))[0]

    def text(self) -> str:
        try:
            return self.take(self.u32()).decode("utf-8")
        except UnicodeDecodeError as error:
            raise FormatError(f"native string is not UTF-8: {error}") from error

    def hash(self) -> bytes:
        return self.take(32)

    def finish(self) -> None:
        if self.offset != len(self.data):
            raise FormatError(f"{len(self.data) - self.offset} trailing native bytes")


def decode_metadata(cur: Cursor) -> dict[str, Any]:
    flags = cur.u8()
    if flags & ~0x0F:
        raise FormatError(f"unknown version metadata flags {flags:#04x}")
    return {
        "min_event_time": cur.i64() if flags & 1 else None,
        "max_event_time": cur.i64() if flags & 2 else None,
        "extended_attributes": cur.text() if flags & 4 else None,
        "timestamp": cur.i64() if flags & 8 else None,
    }

def dynamic_metadata(versions: list[dict[str, Any]], path: str) -> dict[str, int] | None:
    if not versions:
        return None
    if len(versions) != 1:
        raise FormatError(
            f"dynamic node {path} has {len(versions)} metadata records; expected at most one"
        )
    metadata = versions[0]
    if (
        metadata["timestamp"] is None
        or metadata["min_event_time"] is not None
        or metadata["max_event_time"] is not None
        or metadata["extended_attributes"] is not None
    ):
        raise FormatError(f"dynamic node {path} has unsupported metadata")
    return {"timestamp": metadata["timestamp"]}


def decode_commit(data: bytes) -> dict[str, Any]:
    cur = Cursor(data)
    cur.tag(b"watertown.commit.v1\n")
    model = cur.u8()
    if model != 1:
        raise FormatError(f"unsupported content model version {model}")
    root = cur.hash()
    parent_flag = cur.u8()
    if parent_flag not in (0, 1):
        raise FormatError(f"invalid parent flag {parent_flag}")
    parent = cur.hash() if parent_flag else None
    manifest = cur.hash()
    manifest_root = cur.hash()
    result = {
        "content_model_version": model,
        "root_tree_hash": root,
        "parent_commit_hash": parent,
        "node_manifest_hash": manifest,
        "node_manifest_root": manifest_root,
        "pond_id": cur.text(),
        "seq": cur.i64(),
        "time_micros": cur.i64(),
        "author": cur.text(),
        "request": cur.text(),
    }
    cur.finish()
    return result


def decode_manifest(data: bytes) -> list[dict[str, Any]]:
    cur = Cursor(data)
    if data.startswith(b"watertown.manifest.v1\n"):
        cur.tag(b"watertown.manifest.v1\n")
    elif data.startswith(b"dp.manifest.2\n"):
        cur.tag(b"dp.manifest.2\n")
    else:
        raise FormatError("expected native manifest magic watertown.manifest.v1 or dp.manifest.2")
    entries = []
    for _ in range(cur.u32()):
        node_id, parent, name = cur.text(), cur.text(), cur.text()
        kind = cur.u8()
        if kind not in ENTRY_TYPES:
            raise FormatError(f"unknown entry type {kind}")
        child_hash = cur.hash()
        versions = [decode_metadata(cur) for _ in range(cur.u32())]
        entries.append(
            {
                "node_id": node_id,
                "parent_node_id": parent,
                "name": name,
                "entry_type": ENTRY_TYPES[kind],
                "child_hash": child_hash,
                "versions": versions,
            }
        )
    cur.finish()
    node_ids = [entry["node_id"].encode() for entry in entries]
    if node_ids != sorted(node_ids) or len(node_ids) != len(set(node_ids)):
        raise FormatError("native manifest entries are not uniquely sorted by node ID")
    return entries


def decode_tree(data: bytes) -> list[dict[str, Any]]:
    cur = Cursor(data)
    if data.startswith(b"watertown.tree.v1\n"):
        cur.tag(b"watertown.tree.v1\n")
    elif data.startswith(b"dp.tree.2\n"):
        cur.tag(b"dp.tree.2\n")
    else:
        raise FormatError("expected native tree magic watertown.tree.v1 or dp.tree.2")
    entries = []
    for _ in range(cur.u32()):
        name = cur.text()
        kind = cur.u8()
        if kind not in ENTRY_TYPES:
            raise FormatError(f"unknown entry type {kind}")
        child_hash = cur.hash()
        versions = [decode_metadata(cur) for _ in range(cur.u32())]
        entries.append({
            "name": name,
            "entry_type": ENTRY_TYPES[kind],
            "child_hash": child_hash,
            "versions": versions,
        })
    cur.finish()
    names = [entry["name"].encode() for entry in entries]
    if names != sorted(names) or len(names) != len(set(names)):
        raise FormatError("native tree entries are not uniquely sorted by name")
    return entries


def decode_series(data: bytes) -> dict[str, Any]:
    cur = Cursor(data)
    if data.startswith(b"dp.series.1\n"):
        cur.tag(b"dp.series.1\n")
        result = {
            "format": "dp.series.1",
            "hashes": [cur.hash() for _ in range(cur.u32())],
        }
        cur.finish()
        return result
    if data.startswith(b"watertown.series.v1\n"):
        revision, magic = 1, b"watertown.series.v1\n"
    elif data.startswith(b"watertown.series.v2\n"):
        revision, magic = 2, b"watertown.series.v2\n"
    else:
        raise FormatError(
            "expected series magic dp.series.1, watertown.series.v1, or "
            "watertown.series.v2"
        )
    cur.tag(magic)
    kind = cur.u8()
    if kind not in (0, 1):
        raise FormatError(f"unsupported series payload kind {kind}")
    schema = cur.take(cur.u32()) if revision == 1 else b""
    if revision == 1 and len(schema) not in (0, 32):
        raise FormatError("series schema fingerprint must be 0 or 32 bytes")
    logical_count = struct.unpack("<Q", cur.take(8))[0]
    leaf_count = struct.unpack("<Q", cur.take(8))[0]
    flags = cur.u8()
    if flags & ~0x03:
        raise FormatError(f"unknown series bounds flags {flags:#04x}")
    minimum = cur.i64() if flags & 1 else None
    maximum = cur.i64() if flags & 2 else None
    attributes = _decode_canonical_attributes(
        cur.take(cur.u32()), "series logical attributes"
    )
    root = cur.hash()
    if (logical_count == 0) != (leaf_count == 0):
        raise FormatError(
            "series logical_count and leaf_count must both be zero or both be nonzero"
        )
    if revision == 1 and ((kind == 0 and not schema) or (kind == 1 and schema)):
        raise FormatError(
            "a v1 table series requires a schema fingerprint and a file series forbids one"
        )
    empty_root = _merkle_root([], __import__("blake3"))
    if (leaf_count == 0 and root != empty_root) or (
        leaf_count > 0 and root == empty_root
    ):
        raise FormatError("series leaf count and Merkle root disagree")
    result: dict[str, Any] = {
        "format": f"watertown.series.v{revision}",
        "revision": revision,
        "kind": "table" if kind == 0 else "file",
        "schema_fingerprint": schema or None,
        "logical_count": logical_count,
        "leaf_count": leaf_count,
        "min_event_time": minimum,
        "max_event_time": maximum,
        "logical_attributes": attributes,
        "leaf_merkle_root": root,
    }
    cur.finish()
    return result


def _decode_canonical_attributes(value: bytes, where: str) -> str | None:
    if not value:
        return None
    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError as error:
        raise FormatError(f"{where} are not UTF-8") from error
    if canonical_attributes(text).encode() != value:
        raise FormatError(f"{where} are not canonical JSON")
    return text


def _expected_proof_positions(
    total: int, start: int, end: int
) -> list[tuple[int, int]]:
    positions: list[tuple[int, int]] = []

    def collect(node_start: int, count: int) -> None:
        node_end = node_start + count
        if node_end <= start or node_start >= end:
            positions.append((node_start, count))
        elif node_start < start or node_end > end:
            split = 1 << ((count - 1).bit_length() - 1)
            collect(node_start, split)
            collect(node_start + split, count - split)

    collect(0, total)
    return positions


def _decode_range_proof(data: bytes, total: int, start: int, end: int) -> list[tuple[int, int, bytes]]:
    if start >= end or end > total:
        raise FormatError(f"invalid pack leaf range [{start}, {end}) for {total} leaves")
    cur = Cursor(data)
    cur.tag(b"watertown.series-range-proof.v1\n")
    nodes = [(cur.u64(), cur.u64(), cur.hash()) for _ in range(cur.u32())]
    cur.finish()
    expected = _expected_proof_positions(total, start, end)
    if [(node[0], node[1]) for node in nodes] != expected:
        raise FormatError("pack range proof has a noncanonical shape")
    return nodes


def decode_pack(data: bytes) -> dict[str, Any]:
    if data.startswith(b"watertown.series-pack.v1\n"):
        revision, magic = 1, b"watertown.series-pack.v1\n"
    elif data.startswith(b"watertown.series-pack.v2\n"):
        revision, magic = 2, b"watertown.series-pack.v2\n"
    else:
        raise FormatError(
            "expected pack magic watertown.series-pack.v1 or watertown.series-pack.v2"
        )
    cur = Cursor(data)
    cur.tag(magic)
    series_hash = cur.hash()
    leaf_start, leaf_end, total_leaf_count = cur.u64(), cur.u64(), cur.u64()
    range_root = cur.hash()
    proof = _decode_range_proof(
        cur.take(cur.u32()), total_leaf_count, leaf_start, leaf_end
    )
    physical_hashes = [cur.hash() for _ in range(cur.u32())]
    logical_count, physical_byte_count = cur.u64(), cur.u64()
    descriptors = []
    for _ in range(cur.u32()):
        count = cur.u64()
        schema = cur.take(cur.u32()) if revision == 2 else b""
        if revision == 2 and len(schema) not in (0, 32):
            raise FormatError("pack leaf schema fingerprint must be 0 or 32 bytes")
        flags = cur.u8()
        if flags & ~0x03:
            raise FormatError(f"unknown pack leaf descriptor bounds flags {flags:#04x}")
        descriptor = {
            "logical_count": count,
            "schema_fingerprint": schema or None,
            "min_event_time": cur.i64() if flags & 1 else None,
            "max_event_time": cur.i64() if flags & 2 else None,
            "logical_attributes": _decode_canonical_attributes(
                cur.take(cur.u32()), "pack leaf descriptor logical attributes"
            ),
        }
        if count == 0:
            raise FormatError("pack leaf descriptor logical_count must be positive")
        descriptors.append(descriptor)
    cur.finish()
    if leaf_start >= leaf_end or leaf_end > total_leaf_count:
        raise FormatError("pack leaf range is empty or outside its series")
    if not physical_hashes:
        raise FormatError("pack must name at least one physical object")
    if len(descriptors) != leaf_end - leaf_start:
        raise FormatError("pack descriptor count does not match its leaf range")
    descriptor_total = sum(descriptor["logical_count"] for descriptor in descriptors)
    if descriptor_total > (1 << 64) - 1 or descriptor_total != logical_count:
        raise FormatError("pack descriptor counts do not equal pack logical_count")
    return {
        "revision": revision,
        "series_hash": series_hash,
        "leaf_start": leaf_start,
        "leaf_end": leaf_end,
        "total_leaf_count": total_leaf_count,
        "range_root": range_root,
        "range_proof": proof,
        "physical_hashes": physical_hashes,
        "logical_count": logical_count,
        "physical_byte_count": physical_byte_count,
        "descriptors": descriptors,
    }


def decode_recipe(data: bytes) -> tuple[str, bytes]:
    cur = Cursor(data)
    if data.startswith(b"watertown.recipe.v1\n"):
        cur.tag(b"watertown.recipe.v1\n")
    elif data.startswith(b"dp.recipe.1\n"):
        cur.tag(b"dp.recipe.1\n")
    else:
        raise FormatError("expected native recipe magic watertown.recipe.v1 or dp.recipe.1")
    factory = cur.text()
    config = cur.take(len(data) - cur.offset)
    return factory, config


def _encoded_metadata(metadata: dict[str, Any]) -> bytes:
    flags = int(metadata["min_event_time"] is not None)
    flags |= int(metadata["max_event_time"] is not None) << 1
    flags |= int(metadata["extended_attributes"] is not None) << 2
    flags |= int(metadata["timestamp"] is not None) << 3
    result = bytes([flags])
    if metadata["min_event_time"] is not None:
        result += struct.pack("<q", metadata["min_event_time"])
    if metadata["max_event_time"] is not None:
        result += struct.pack("<q", metadata["max_event_time"])
    if metadata["extended_attributes"] is not None:
        value = metadata["extended_attributes"].encode()
        result += struct.pack("<I", len(value)) + value
    if metadata["timestamp"] is not None:
        result += struct.pack("<q", metadata["timestamp"])
    return result


def node_manifest_root(entries: list[dict[str, Any]], blake3: Any) -> bytes:
    empty = [b""] * 257
    empty[256] = blake3.blake3(b"\x02").digest()
    for depth in range(255, -1, -1):
        empty[depth] = blake3.blake3(
            b"\x01" + empty[depth + 1] + empty[depth + 1]).digest()

    pairs = []
    for entry in entries:
        name = entry["name"].encode()
        parent = entry["parent_node_id"].encode()
        value = (
            bytes([ENTRY_CODES[entry["entry_type"]]])
            + struct.pack("<I", len(name))
            + name
            + struct.pack("<I", len(parent))
            + parent
            + entry["child_hash"]
            + struct.pack("<I", len(entry["versions"]))
            + b"".join(_encoded_metadata(version) for version in entry["versions"])
        )
        key = blake3.blake3(entry["node_id"].encode()).digest()
        pairs.append((key, blake3.blake3(value).digest()))
    pairs.sort()
    if any(left[0] == right[0] for left, right in zip(pairs, pairs[1:])):
        raise FormatError("duplicate node ID key in node manifest")

    def build(depth: int, subset: list[tuple[bytes, bytes]]) -> bytes:
        if not subset:
            return empty[depth]
        if depth == 256:
            if len(subset) != 1:
                raise FormatError("node manifest contains a BLAKE3 key collision")
            key, value = subset[0]
            return blake3.blake3(b"\x00" + key + value).digest()
        byte, shift = depth // 8, 7 - depth % 8
        split = 0
        while split < len(subset) and ((subset[split][0][byte] >> shift) & 1) == 0:
            split += 1
        left = build(depth + 1, subset[:split])
        right = build(depth + 1, subset[split:])
        return blake3.blake3(b"\x01" + left + right).digest()

    return build(0, pairs)


def canonical_attributes(text: str) -> str:
    value = json.loads(text)
    if not isinstance(value, dict):
        raise FormatError("logical attributes must be a JSON object")

    def validate(item: Any) -> None:
        if isinstance(item, dict):
            for key, child in item.items():
                if not isinstance(key, str):
                    raise FormatError("logical attribute key is not a string")
                validate(child)
        elif isinstance(item, list):
            for child in item:
                validate(child)
        elif isinstance(item, bool) or item is None or isinstance(item, str):
            pass
        elif isinstance(item, int):
            if item < -(1 << 63) or item > (1 << 64) - 1:
                raise FormatError("logical attribute integer is outside the 64-bit model")
        else:
            raise FormatError("logical attributes permit integers, not floating-point numbers")

    validate(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _metadata_bytes(metadata: dict[bytes, bytes] | None) -> bytes:
    pairs = sorted((metadata or {}).items())
    result = struct.pack("<I", len(pairs))
    for key, value in pairs:
        result += struct.pack("<I", len(key)) + key
        result += struct.pack("<I", len(value)) + value
    return result


def canonical_schema(schema: Any, pa: Any) -> bytes:
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
                raise FormatError(f"unsupported timestamp unit {data_type.unit}")
            result += bytes([16, units[data_type.unit]])
            if data_type.tz is None:
                result += b"\0"
            else:
                zone = data_type.tz.encode()
                result += b"\1" + struct.pack("<I", len(zone)) + zone
        elif pa.types.is_decimal128(data_type):
            result += bytes([17, data_type.precision, data_type.scale & 0xFF])
        else:
            raise FormatError(f"unsupported canonical series type {data_type}")
        result += _metadata_bytes(field.metadata)
    return result + _metadata_bytes(schema.metadata)


def canonical_batch_rows(schema: Any, batch: Any, pa: Any) -> bytes:
    result = bytearray()
    columns = []
    for index, field in enumerate(schema):
        column = batch.column(index)
        if pa.types.is_dictionary(column.type):
            column = column.dictionary_decode()
        data_type = (
            field.type.value_type if pa.types.is_dictionary(field.type) else field.type
        )
        if column.type != data_type:
            column = column.cast(data_type)
        columns.append((field.type.value_type if pa.types.is_dictionary(field.type) else field.type, column))
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
                raw = column.cast(pa.int32())[row].as_py() if pa.types.is_date32(data_type) else value
                result.extend(struct.pack("<i", raw))
            elif pa.types.is_int64(data_type) or pa.types.is_timestamp(data_type):
                raw = column.cast(pa.int64())[row].as_py() if pa.types.is_timestamp(data_type) else value
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
                result.extend(struct.pack("<I", 0x7FC00000) if math.isnan(value) else struct.pack("<f", value))
            elif pa.types.is_float64(data_type):
                result.extend(struct.pack("<Q", 0x7FF8000000000000) if math.isnan(value) else struct.pack("<d", value))
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
                    unscaled = coefficient * (10 ** adjustment)
                else:
                    divisor = 10 ** -adjustment
                    unscaled, remainder = divmod(coefficient, divisor)
                    if remainder:
                        raise FormatError(
                            f"decimal value {value} is not exactly representable as {data_type}"
                        )
                result.extend(unscaled.to_bytes(16, "little", signed=True))
            else:
                raise FormatError(f"unsupported canonical series value {data_type}")
    return bytes(result)


def _hash_file(path: Path, blake3: Any) -> tuple[str, int]:
    digest, size = blake3.blake3(), 0
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _copy_payload(source: Path, objects: Path, expected: bytes, blake3: Any) -> dict[str, Any]:
    expected_hex = expected.hex()
    actual, size = _hash_file(source, blake3)
    if actual != expected_hex:
        raise FormatError(f"payload {source} hashes to {actual}, expected {expected_hex}")
    target = objects / f"blake3={expected_hex}"
    if not target.exists():
        shutil.copyfile(source, target)
    return {"hash": expected_hex, "size": size}


def _leaf_hash(kind: int, fingerprint: bytes, count: int, payload_parts: Iterator[bytes],
               payload_size: int, metadata: dict[str, Any], attributes: str | None,
               blake3: Any) -> str:
    digest = blake3.blake3()
    digest.update(b"watertown.series-leaf.v1\n" + bytes([kind]))
    digest.update(struct.pack("<I", len(fingerprint)) + fingerprint)
    digest.update(struct.pack("<Q", count))
    digest.update(struct.pack("<Q", payload_size))
    for part in payload_parts:
        digest.update(part)
    flags = int(metadata["min_event_time"] is not None) | (int(metadata["max_event_time"] is not None) << 1)
    digest.update(bytes([flags]))
    if metadata["min_event_time"] is not None:
        digest.update(struct.pack("<q", metadata["min_event_time"]))
    if metadata["max_event_time"] is not None:
        digest.update(struct.pack("<q", metadata["max_event_time"]))
    encoded = b"" if attributes is None else attributes.encode()
    digest.update(struct.pack("<I", len(encoded)) + encoded)
    return digest.hexdigest()


def _series_root(kind: str, fingerprint: bytes | None, leaves: list[dict[str, Any]], blake3: Any) -> str:
    digest = blake3.blake3(b"pondcapsule.series.3\n")
    digest.update(b"\0" if kind == "file" else b"\1")
    digest.update(b"\0" if fingerprint is None else b"\1" + fingerprint)
    digest.update(struct.pack("<Q", len(leaves)))
    for leaf in leaves:
        digest.update(bytes.fromhex(leaf["logical_hash"]))
        digest.update(struct.pack("<Q", leaf["logical_count"]))
        flags = int(leaf["min_event_time"] is not None)
        flags |= int(leaf["max_event_time"] is not None) << 1
        flags |= int(leaf["logical_attributes"] is not None) << 2
        leaf_schema = leaf.get("schema_fingerprint")
        if leaf_schema is not None:
            flags |= 0x08
        digest.update(bytes([flags]))
        if leaf["min_event_time"] is not None:
            digest.update(struct.pack("<q", leaf["min_event_time"]))
        if leaf["max_event_time"] is not None:
            digest.update(struct.pack("<q", leaf["max_event_time"]))
        if leaf["logical_attributes"] is not None:
            encoded = leaf["logical_attributes"].encode()
            digest.update(struct.pack("<Q", len(encoded)) + encoded)
        if leaf_schema is not None:
            digest.update(bytes.fromhex(leaf_schema))
    return digest.hexdigest()


def _merkle_root(leaves: list[str], blake3: Any) -> bytes:
    domain = b"watertown.series-merkle.v1\n"

    def subtree(values: list[bytes]) -> bytes:
        if not values:
            return blake3.blake3(domain + b"\0").digest()
        if len(values) == 1:
            return blake3.blake3(domain + b"\1" + values[0]).digest()
        split = 1 << ((len(values) - 1).bit_length() - 1)
        return blake3.blake3(domain + b"\2" + subtree(values[:split]) + subtree(values[split:])).digest()

    return subtree([bytes.fromhex(value) for value in leaves])


def _verify_pack_root(
    pack: dict[str, Any], manifest: dict[str, Any], leaf_hashes: list[str], blake3: Any
) -> None:
    if pack["series_hash"] != manifest["hash"]:
        raise FormatError("pack series hash does not match its fetched series manifest")
    if pack["total_leaf_count"] != manifest["leaf_count"]:
        raise FormatError("pack total leaf count does not match its series manifest")
    if len(leaf_hashes) != pack["leaf_end"] - pack["leaf_start"]:
        raise FormatError("reconstructed pack leaf count does not match its range")

    proof = iter(pack["range_proof"])

    def fold(node_start: int, count: int) -> bytes:
        node_end = node_start + count
        if node_end <= pack["leaf_start"] or node_start >= pack["leaf_end"]:
            try:
                proof_start, proof_count, value = next(proof)
            except StopIteration as error:
                raise FormatError("pack range proof is truncated") from error
            if (proof_start, proof_count) != (node_start, count):
                raise FormatError("pack range proof node position mismatch")
            return value
        if node_start >= pack["leaf_start"] and node_end <= pack["leaf_end"]:
            offset = node_start - pack["leaf_start"]
            return _merkle_root(leaf_hashes[offset : offset + count], blake3)
        split = 1 << ((count - 1).bit_length() - 1)
        return blake3.blake3(
            b"watertown.series-merkle.v1\n\x02"
            + fold(node_start, split)
            + fold(node_start + split, count - split)
        ).digest()

    root = fold(0, pack["total_leaf_count"])
    if next(proof, None) is not None:
        raise FormatError("pack range proof has trailing nodes")
    if root != pack["range_root"]:
        raise FormatError("pack range proof does not reduce to its declared root")
    if root != manifest["leaf_merkle_root"]:
        raise FormatError("pack range proof root does not match its series manifest")


def _select_exact_cover(
    series_hash: bytes, leaf_count: int, candidates: list[tuple[bytes, dict[str, Any]]]
) -> list[tuple[bytes, dict[str, Any]]]:
    if leaf_count == 0:
        if candidates:
            raise FormatError("zero-leaf series has pack advertisements")
        return []
    by_start: dict[int, list[tuple[bytes, dict[str, Any]]]] = {}
    for pack_hash, pack in candidates:
        if pack["series_hash"] != series_hash:
            raise FormatError("pack is advertised under a different series hash")
        if pack["total_leaf_count"] != leaf_count:
            raise FormatError("pack total leaf count does not match its series")
        by_start.setdefault(pack["leaf_start"], []).append((pack_hash, pack))

    best: dict[int, tuple[int, tuple[bytes, ...], list[tuple[bytes, dict[str, Any]]]]] = {
        leaf_count: (0, (), [])
    }
    endpoints = sorted({0, leaf_count, *(pack["leaf_start"] for _, pack in candidates)})
    for start in reversed(endpoints):
        options = []
        for pack_hash, pack in by_start.get(start, []):
            suffix = best.get(pack["leaf_end"])
            if suffix is None:
                continue
            options.append(
                (
                    suffix[0] + 1,
                    (pack_hash,) + suffix[1],
                    [(pack_hash, pack)] + suffix[2],
                )
            )
        if options:
            best[start] = min(options, key=lambda value: (value[0], value[1]))
    try:
        return best[0][2]
    except KeyError as error:
        raise FormatError(
            f"no exact pack cover exists for series {series_hash.hex()}"
        ) from error


def _pack_schema_fingerprint(
    series: dict[str, Any], pack: dict[str, Any], descriptor: dict[str, Any]
) -> bytes | None:
    if series["kind"] == "file":
        if descriptor["schema_fingerprint"] is not None:
            raise FormatError("file pack descriptor carries a schema fingerprint")
        return None
    descriptor_schema = descriptor["schema_fingerprint"]
    if descriptor_schema is not None:
        if series["schema_fingerprint"] is not None and descriptor_schema != series["schema_fingerprint"]:
            raise FormatError("pack descriptor schema differs from its v1 series schema")
        return descriptor_schema
    if series["revision"] == 1 and pack["revision"] == 1:
        return series["schema_fingerprint"]
    raise FormatError("table pack descriptor is missing its required schema fingerprint")


class NativeBackup:
    def __init__(self, root: Path, work: Path, DeltaTable: Any, ds: Any, blake3: Any):
        self.root, self.work, self.blake3 = root, work, blake3
        self.dataset = DeltaTable(str(root)).to_pyarrow_dataset()
        self.ds = ds
        self.pond_id = self._source_pond_id()
        self.live = self._live_index()
        self.refs: dict[str, bytes] = {}
        self.objects = work / "native-objects"
        self.objects.mkdir()
        self._materialize_live()

    def _batches(self, columns: list[str], pond: str | None = None,
                 partition: str | None = None) -> Iterator[Any]:
        predicate = None
        for field, value in (("pond_id", pond), ("partition_key", partition)):
            if value is not None:
                term = self.ds.field(field) == value
                predicate = term if predicate is None else predicate & term
        return self.dataset.scanner(columns=columns, filter=predicate).to_batches()

    def _source_pond_id(self) -> str:
        latest: tuple[int, bool, bytes] | None = None
        columns = ["txn_seq", "deleted", "item_key", "value", "value_blake3"]
        for batch in self._batches(columns, NIL_UUID, "meta"):
            for row in batch.to_pylist():
                if row["item_key"] != "pond_id":
                    continue
                candidate = (row["txn_seq"], row["deleted"], row["value"])
                if latest is not None and candidate[0] == latest[0]:
                    raise FormatError("ambiguous meta/pond_id rows at the same txn_seq")
                if latest is None or candidate[0] > latest[0]:
                    if self.blake3.blake3(row["value"]).digest() != row["value_blake3"]:
                        raise FormatError("meta/pond_id value_blake3 mismatch")
                    latest = candidate
        if latest is None or latest[1]:
            raise FormatError("native backup has no live meta/pond_id")
        try:
            return latest[2].decode()
        except UnicodeDecodeError as error:
            raise FormatError("meta/pond_id is not UTF-8") from error

    def _live_index(self) -> dict[tuple[str, str], tuple[int, bool]]:
        live: dict[tuple[str, str], tuple[int, bool]] = {}
        for batch in self._batches(["partition_key", "item_key", "txn_seq", "deleted"], self.pond_id):
            for row in batch.to_pylist():
                key = (row["partition_key"], row["item_key"])
                prior = live.get(key)
                if prior is not None and row["txn_seq"] == prior[0]:
                    raise FormatError(f"ambiguous live rows for {key!r} at txn_seq {prior[0]}")
                if prior is None or row["txn_seq"] > prior[0]:
                    live[key] = (row["txn_seq"], row["deleted"])
        return live

    def _materialize_live(self) -> None:
        columns = ["partition_key", "item_key", "txn_seq", "deleted", "value", "value_blake3"]
        found: set[tuple[str, str]] = set()
        for batch in self._batches(columns, self.pond_id):
            for row in batch.to_pylist():
                key = (row["partition_key"], row["item_key"])
                selected = self.live.get(key)
                if selected is None or selected[1] or row["txn_seq"] != selected[0]:
                    continue
                if key in found:
                    raise FormatError(f"duplicate selected native row for {key!r}")
                found.add(key)
                value = row["value"]
                if self.blake3.blake3(value).digest() != row["value_blake3"]:
                    raise FormatError(f"value_blake3 mismatch for {key!r}")
                if key[0] == "refs":
                    if len(value) != 32:
                        raise FormatError(f"native ref {key[1]!r} is not 32 bytes")
                    self.refs[key[1]] = value
                elif key[0] == "objects":
                    if len(key[1]) != 64 or self.blake3.blake3(value).hexdigest() != key[1]:
                        raise FormatError(f"native object key/hash mismatch for {key[1]!r}")
                    (self.objects / key[1]).write_bytes(value)

    def object_path(self, digest: bytes) -> Path:
        inline = self.objects / digest.hex()
        if inline.is_file():
            return inline
        external = self.root / "_blobs" / f"blob={digest.hex()}"
        if external.is_file():
            return external
        raise FormatError(f"native object {digest.hex()} is missing")

    def read_object(self, digest: bytes) -> bytes:
        data = self.object_path(digest).read_bytes()
        if self.blake3.blake3(data).digest() != digest:
            raise FormatError(f"native object {digest.hex()} fails BLAKE3 verification")
        return data

    def pack_indexes(
        self, series_hash: bytes, *, required: bool = True
    ) -> list[tuple[bytes, bytes]]:
        directory = self.root / "_packs" / f"series={series_hash.hex()}"
        if not directory.exists() and not required:
            return []
        if directory.is_symlink() or not directory.is_dir():
            raise FormatError(f"series {series_hash.hex()} has no pack advertisement directory")
        indexes = []
        for path in directory.iterdir():
            name = path.name
            if (
                not name.startswith("pack=")
                or len(name) != len("pack=") + 64
                or any(byte not in "0123456789abcdef" for byte in name[5:])
                or path.is_symlink()
                or not path.is_file()
            ):
                raise FormatError(f"invalid pack advertisement entry {path}")
            advertised = bytes.fromhex(name[5:])
            data = path.read_bytes()
            if self.blake3.blake3(data).digest() != advertised:
                raise FormatError(f"pack advertisement {name} fails BLAKE3 verification")
            indexes.append((advertised, data))
        if not indexes and required:
            raise FormatError(f"series {series_hash.hex()} has no pack advertisements")
        return indexes


def verify_tree_manifest(backup: NativeBackup, commit: dict[str, Any],
                         entries: list[dict[str, Any]]) -> None:
    roots = [entry for entry in entries
             if not entry["parent_node_id"] and not entry["name"]]
    if len(roots) != 1:
        raise FormatError("native manifest must contain exactly one root")
    root = roots[0]
    if root["entry_type"] != "dir:physical":
        raise FormatError("native manifest root is not a physical directory")
    if root["child_hash"] != commit["root_tree_hash"]:
        raise FormatError("native manifest root does not match the commit root tree")

    children: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        if entry is not root:
            children.setdefault(entry["parent_node_id"], []).append(entry)
    for directory in entries:
        if directory["entry_type"] != "dir:physical":
            if directory["node_id"] in children:
                raise FormatError(
                    f"non-physical directory node {directory['node_id']!r} has children")
            continue
        tree = decode_tree(backup.read_object(directory["child_hash"]))
        expected = sorted(children.get(directory["node_id"], []),
                          key=lambda entry: entry["name"].encode())
        actual = [{
            "name": entry["name"],
            "entry_type": entry["entry_type"],
            "child_hash": entry["child_hash"],
            "versions": entry["versions"],
        } for entry in tree]
        projected = [{
            "name": entry["name"],
            "entry_type": entry["entry_type"],
            "child_hash": entry["child_hash"],
            "versions": entry["versions"],
        } for entry in expected]
        if actual != projected:
            raise FormatError(
                f"tree and manifest disagree at native node {directory['node_id']!r}")


def _reconstruct_pack_leaves(
    payload_paths: list[Path],
    descriptors: list[dict[str, Any]],
    series: dict[str, Any],
    pack: dict[str, Any],
    path: str,
    source_timestamp: int,
    pa: Any,
    pq: Any,
    blake3: Any,
) -> list[dict[str, Any]]:
    """Reconstruct one pack only; no physical bytes may spill into another pack."""
    leaves: list[dict[str, Any]] = []
    if series["kind"] == "file":
        chunks = (chunk for payload in payload_paths for chunk in _file_chunks(payload))
        current, offset = b"", 0
        for index, descriptor in enumerate(descriptors):
            count = descriptor["logical_count"]
            digest = blake3.blake3(
                b"watertown.series-leaf.v1\n\x01"
                + struct.pack("<IQQ", 0, count, count)
            )
            remaining = count
            while remaining:
                if offset == len(current):
                    try:
                        current = next(chunks)
                    except StopIteration as error:
                        raise FormatError(
                            f"file pack for {path} ends during leaf {index}"
                        ) from error
                    offset = 0
                take = min(remaining, len(current) - offset)
                digest.update(current[offset : offset + take])
                offset += take
                remaining -= take
            attributes = descriptor["logical_attributes"]
            flags = int(descriptor["min_event_time"] is not None)
            flags |= int(descriptor["max_event_time"] is not None) << 1
            digest.update(bytes([flags]))
            if descriptor["min_event_time"] is not None:
                digest.update(struct.pack("<q", descriptor["min_event_time"]))
            if descriptor["max_event_time"] is not None:
                digest.update(struct.pack("<q", descriptor["max_event_time"]))
            encoded = b"" if attributes is None else attributes.encode()
            digest.update(struct.pack("<I", len(encoded)) + encoded)
            leaves.append({
                "logical_hash": digest.hexdigest(),
                "logical_count": count,
                "source_timestamp": source_timestamp,
                "min_event_time": descriptor["min_event_time"],
                "max_event_time": descriptor["max_event_time"],
                "logical_attributes": attributes,
            })
        if offset != len(current) or next(chunks, None) is not None:
            raise FormatError(f"file pack for {path} has bytes after its final leaf")
        return leaves

    lengths = [0] * len(descriptors)

    def visit(hashers: list[Any] | None) -> None:
        index, rows = 0, 0
        for payload in payload_paths:
            parquet = pq.ParquetFile(payload)
            schema = read_parquet_schema(parquet, pa, FormatError)
            fingerprint = blake3.blake3(canonical_schema(schema, pa)).digest()
            for batch in parquet.iter_batches():
                offset = 0
                while offset < batch.num_rows:
                    if index >= len(descriptors):
                        raise FormatError(f"table pack for {path} has rows after its final leaf")
                    descriptor = descriptors[index]
                    if fingerprint != _pack_schema_fingerprint(series, pack, descriptor):
                        raise FormatError(
                            f"table pack for {path} crosses a leaf schema transition"
                        )
                    take = min(descriptor["logical_count"] - rows, batch.num_rows - offset)
                    encoded = canonical_batch_rows(schema, batch.slice(offset, take), pa)
                    if hashers is None:
                        lengths[index] += len(encoded)
                    else:
                        hashers[index].update(encoded)
                    rows += take
                    offset += take
                    if rows == descriptor["logical_count"]:
                        index, rows = index + 1, 0
        if index != len(descriptors) or rows:
            raise FormatError(f"table pack for {path} ends during a logical leaf")

    visit(None)
    hashers = []
    for descriptor, length in zip(descriptors, lengths):
        fingerprint = _pack_schema_fingerprint(series, pack, descriptor)
        count = descriptor["logical_count"]
        digest = blake3.blake3(
            b"watertown.series-leaf.v1\n\0"
            + struct.pack("<I", len(fingerprint))
            + fingerprint
            + struct.pack("<QQ", count, len(b"watertown.series-rows.v1\n") + 8 + length)
            + b"watertown.series-rows.v1\n"
            + struct.pack("<Q", count)
        )
        hashers.append(digest)
    visit(hashers)
    for descriptor, digest in zip(descriptors, hashers):
        attributes = descriptor["logical_attributes"]
        flags = int(descriptor["min_event_time"] is not None)
        flags |= int(descriptor["max_event_time"] is not None) << 1
        digest.update(bytes([flags]))
        if descriptor["min_event_time"] is not None:
            digest.update(struct.pack("<q", descriptor["min_event_time"]))
        if descriptor["max_event_time"] is not None:
            digest.update(struct.pack("<q", descriptor["max_event_time"]))
        encoded = b"" if attributes is None else attributes.encode()
        digest.update(struct.pack("<I", len(encoded)) + encoded)
        leaf = {
            "logical_hash": digest.hexdigest(),
            "logical_count": descriptor["logical_count"],
            "source_timestamp": source_timestamp,
            "min_event_time": descriptor["min_event_time"],
            "max_event_time": descriptor["max_event_time"],
            "logical_attributes": attributes,
        }
        if series["revision"] == 2:
            leaf["schema_fingerprint"] = _pack_schema_fingerprint(
                series, pack, descriptor
            ).hex()
        leaves.append(leaf)
    return leaves


def _pack_stream_node(
    backup: NativeBackup,
    objects_dir: Path,
    native: dict[str, Any],
    path: str,
    series_hash: bytes,
    series: dict[str, Any],
    pa: Any,
    pq: Any,
    blake3: Any,
) -> dict[str, Any]:
    if len(native["versions"]) != 1:
        raise FormatError(
            f"pack-aware series {path} must carry one aggregate tree metadata record"
        )
    if series["kind"] != (
        "file" if native["entry_type"].startswith("file:") else "table"
    ):
        raise FormatError(f"series {path} payload kind disagrees with its tree entry")
    if series["leaf_count"] == 0:
        if series["kind"] == "table":
            raise FormatError(f"zero-leaf table series {path} has no materializable schema")
        if series["logical_count"] != 0:
            raise FormatError(f"zero-leaf series {path} has a nonzero logical count")
        if backup.pack_indexes(series_hash, required=False):
            raise FormatError(f"zero-leaf series {path} has pack advertisements")
        return {
            "kind": "physical",
            "payload_kind": "file",
            "logical_root": _series_root("file", None, [], blake3),
            "objects": [],
            "leaves": [],
        }

    candidates: list[tuple[bytes, dict[str, Any]]] = []
    for pack_hash, bytes_ in backup.pack_indexes(series_hash):
        pack = decode_pack(bytes_)
        for descriptor in pack["descriptors"]:
            _pack_schema_fingerprint(series, pack, descriptor)
        candidates.append((pack_hash, pack))
    selected = _select_exact_cover(series_hash, series["leaf_count"], candidates)

    payload_paths: list[Path] = []
    objects: list[dict[str, Any]] = []
    descriptors: list[dict[str, Any]] = []
    selected_streams: list[tuple[dict[str, Any], list[Path]]] = []
    physical_byte_count = 0
    for pack_hash, pack in selected:
        pack_paths = []
        pack_bytes = 0
        for digest in pack["physical_hashes"]:
            source = backup.object_path(digest)
            descriptor = _copy_payload(source, objects_dir, digest, blake3)
            pack_paths.append(source)
            objects.append(descriptor)
            pack_bytes += descriptor["size"]
        if pack_bytes != pack["physical_byte_count"]:
            raise FormatError(
                f"pack {pack_hash.hex()} physical_byte_count does not match its objects"
            )
        physical_byte_count += pack_bytes
        payload_paths.extend(pack_paths)
        selected_streams.append((pack, pack_paths))
        descriptors.extend(
            [{**descriptor, "_pack_revision": pack["revision"]}
             for descriptor in pack["descriptors"]]
        )
    if sum(descriptor["logical_count"] for descriptor in descriptors) != series["logical_count"]:
        raise FormatError(f"selected packs for {path} do not match the series logical count")
    if physical_byte_count < 1:
        raise FormatError(f"selected packs for {path} contain no physical bytes")

    source_timestamp = native["versions"][0]["timestamp"] or 0
    # A pack is an independently decodable range.  In particular, another
    # selected pack must never supply an object fragment that completes it.
    verified_leaves: list[dict[str, Any]] = []
    for pack, pack_paths in selected_streams:
        pack_leaves = _reconstruct_pack_leaves(
            pack_paths,
            pack["descriptors"],
            series,
            pack,
            path,
            source_timestamp,
            pa,
            pq,
            blake3,
        )
        _verify_pack_root(
            pack,
            {**series, "hash": series_hash},
            [leaf["logical_hash"] for leaf in pack_leaves],
            blake3,
        )
        verified_leaves.extend(pack_leaves)

    leaves: list[dict[str, Any]] = []
    if series["kind"] == "file":
        chunks = (
            chunk
            for payload_path in payload_paths
            for chunk in _file_chunks(payload_path)
        )
        current = b""
        offset = 0
        for index, descriptor in enumerate(descriptors):
            remaining = descriptor["logical_count"]
            digest = blake3.blake3()
            digest.update(b"watertown.series-leaf.v1\n\x01")
            digest.update(struct.pack("<I", 0))
            digest.update(struct.pack("<Q", remaining))
            digest.update(struct.pack("<Q", remaining))
            while remaining:
                if offset == len(current):
                    try:
                        current = next(chunks)
                    except StopIteration as error:
                        raise FormatError(f"file pack for {path} ends during leaf {index}") from error
                    offset = 0
                take = min(remaining, len(current) - offset)
                digest.update(current[offset : offset + take])
                offset += take
                remaining -= take
            attributes = descriptor["logical_attributes"]
            digest.update(
                bytes([
                    int(descriptor["min_event_time"] is not None)
                    | (int(descriptor["max_event_time"] is not None) << 1)
                ])
            )
            if descriptor["min_event_time"] is not None:
                digest.update(struct.pack("<q", descriptor["min_event_time"]))
            if descriptor["max_event_time"] is not None:
                digest.update(struct.pack("<q", descriptor["max_event_time"]))
            encoded = b"" if attributes is None else attributes.encode()
            digest.update(struct.pack("<I", len(encoded)) + encoded)
            leaves.append({
                "logical_hash": digest.hexdigest(),
                "logical_count": descriptor["logical_count"],
                "source_timestamp": source_timestamp,
                "min_event_time": descriptor["min_event_time"],
                "max_event_time": descriptor["max_event_time"],
                "logical_attributes": attributes,
            })
        if offset != len(current) or next(chunks, None) is not None:
            raise FormatError(f"file pack for {path} has bytes after its final leaf")
    else:
        canonical_lengths = [0] * len(descriptors)

        def walk_rows(write: list[Any] | None) -> None:
            leaf_index, leaf_rows = 0, 0
            for payload_path in payload_paths:
                parquet = pq.ParquetFile(payload_path)
                schema = read_parquet_schema(parquet, pa, FormatError)
                fingerprint = blake3.blake3(canonical_schema(schema, pa)).digest()
                if leaf_index >= len(descriptors):
                    raise FormatError(f"table pack for {path} has rows after its final leaf")
                expected = _pack_schema_fingerprint(
                    series, {"revision": descriptors[leaf_index]["_pack_revision"]},
                    descriptors[leaf_index],
                )
                if fingerprint != expected:
                    raise FormatError(f"table pack for {path} physical object schema mismatch")
                for batch in parquet.iter_batches():
                    offset = 0
                    while offset < batch.num_rows:
                        if leaf_index >= len(descriptors):
                            raise FormatError(f"table pack for {path} has rows after its final leaf")
                        expected = _pack_schema_fingerprint(
                            series,
                            {"revision": descriptors[leaf_index]["_pack_revision"]},
                            descriptors[leaf_index],
                        )
                        if fingerprint != expected:
                            raise FormatError(
                                f"table pack for {path} crosses a leaf schema transition"
                            )
                        take = min(
                            descriptors[leaf_index]["logical_count"] - leaf_rows,
                            batch.num_rows - offset,
                        )
                        part = batch.slice(offset, take)
                        encoded = canonical_batch_rows(schema, part, pa)
                        if write is None:
                            canonical_lengths[leaf_index] += len(encoded)
                        else:
                            write[leaf_index].update(encoded)
                        leaf_rows += take
                        offset += take
                        if leaf_rows == descriptors[leaf_index]["logical_count"]:
                            leaf_index += 1
                            leaf_rows = 0
            if leaf_index != len(descriptors) or leaf_rows:
                raise FormatError(f"table pack for {path} ends during a logical leaf")

        walk_rows(None)
        hashers = []
        for descriptor, length in zip(descriptors, canonical_lengths):
            fingerprint = _pack_schema_fingerprint(
                series, {"revision": descriptor["_pack_revision"]}, descriptor
            )
            payload_size = len(b"watertown.series-rows.v1\n") + 8 + length
            digest = blake3.blake3()
            digest.update(b"watertown.series-leaf.v1\n\0")
            digest.update(struct.pack("<I", len(fingerprint)) + fingerprint)
            digest.update(struct.pack("<Q", descriptor["logical_count"]))
            digest.update(struct.pack("<Q", payload_size))
            digest.update(
                b"watertown.series-rows.v1\n"
                + struct.pack("<Q", descriptor["logical_count"])
            )
            hashers.append(digest)
        walk_rows(hashers)
        for descriptor, digest in zip(descriptors, hashers):
            attributes = descriptor["logical_attributes"]
            digest.update(
                bytes([
                    int(descriptor["min_event_time"] is not None)
                    | (int(descriptor["max_event_time"] is not None) << 1)
                ])
            )
            if descriptor["min_event_time"] is not None:
                digest.update(struct.pack("<q", descriptor["min_event_time"]))
            if descriptor["max_event_time"] is not None:
                digest.update(struct.pack("<q", descriptor["max_event_time"]))
            encoded = b"" if attributes is None else attributes.encode()
            digest.update(struct.pack("<I", len(encoded)) + encoded)
            leaf = {
                "logical_hash": digest.hexdigest(),
                "logical_count": descriptor["logical_count"],
                "source_timestamp": source_timestamp,
                "min_event_time": descriptor["min_event_time"],
                "max_event_time": descriptor["max_event_time"],
                "logical_attributes": attributes,
            }
            if series["revision"] == 2:
                leaf["schema_fingerprint"] = _pack_schema_fingerprint(
                    series, {"revision": descriptor["_pack_revision"]}, descriptor
                ).hex()
            leaves.append(leaf)

    offset = 0
    for _, pack in selected:
        count = pack["leaf_end"] - pack["leaf_start"]
        _verify_pack_root(pack, {**series, "hash": series_hash}, [
            leaf["logical_hash"] for leaf in leaves[offset : offset + count]
        ], blake3)
        offset += count
    if offset != len(leaves):
        raise FormatError(f"selected pack descriptors do not cover all leaves for {path}")
    if leaves != verified_leaves:
        raise FormatError(f"pack-local reconstruction disagrees with selected stream for {path}")
    leaves = verified_leaves
    if series["kind"] == "table":
        for leaf, descriptor in zip(leaves, descriptors):
            leaf["schema_fingerprint"] = _pack_schema_fingerprint(
                series, {"revision": descriptor["_pack_revision"]}, descriptor
            ).hex()
    min_event_time = min(
        (leaf["min_event_time"] for leaf in leaves if leaf["min_event_time"] is not None),
        default=None,
    )
    max_event_time = max(
        (leaf["max_event_time"] for leaf in leaves if leaf["max_event_time"] is not None),
        default=None,
    )
    if (min_event_time, max_event_time) != (
        series["min_event_time"], series["max_event_time"]
    ):
        raise FormatError(f"selected packs for {path} have aggregate bounds unlike the series")
    leaf_schemas = {leaf.get("schema_fingerprint") for leaf in leaves}
    node_schema = (
        next(iter(leaf_schemas))
        if series["kind"] == "table"
        and len(leaf_schemas) == 1
        and None not in leaf_schemas
        else series["schema_fingerprint"].hex()
        if series["schema_fingerprint"] is not None
        else None
    )
    result = {
        "kind": "physical",
        "payload_kind": series["kind"],
        # Per-leaf fingerprints retain schema evolution. When every leaf
        # shares one fingerprint, normalize it into the capsule's
        # homogeneous node-level schema too.
        "logical_root": _series_root(
            series["kind"],
            bytes.fromhex(node_schema) if node_schema is not None else None,
            leaves,
            blake3,
        ),
        "objects": objects,
        "leaves": leaves,
    }
    if node_schema is not None:
        result = {
            "kind": result["kind"],
            "payload_kind": result["payload_kind"],
            "schema_fingerprint": node_schema,
            "logical_root": result["logical_root"],
            "objects": result["objects"],
            "leaves": result["leaves"],
        }
    return result


def _file_chunks(path: Path) -> Iterator[bytes]:
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            yield chunk


def _extract_into(source: Path, destination: Path, ref_name: str | None,
                  commit_hex: str | None, birthplace: str) -> str:
    destination.mkdir(mode=0o700)
    try:
        return _extract_graph(source, destination, ref_name, commit_hex, birthplace)
    except Exception:
        shutil.rmtree(destination)
        raise


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
            raise FormatError(
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
            raise FormatError(f"destination already exists: {destination}") from error
    else:
        raise FormatError(
            f"atomic no-replace extraction is unsupported on {sys.platform}"
        )
    if result == 0:
        return
    error_number = ctypes.get_errno()
    if error_number in (errno.EEXIST, errno.ENOTEMPTY):
        raise FormatError(f"destination already exists: {destination}")
    raise OSError(error_number, os.strerror(error_number), destination)


def extract(source: Path, destination: Path, ref_name: str | None, commit_hex: str | None,
            birthplace: str) -> str:
    try:
        destination.lstat()
    except FileNotFoundError:
        pass
    else:
        raise FormatError(f"destination already exists: {destination}")
    if not birthplace:
        raise FormatError("birthplace must not be empty")
    source = source.resolve()
    destination = destination.parent.resolve() / destination.name
    if destination.is_relative_to(source):
        raise FormatError("destination must not be inside the source backup")
    if not destination.parent.is_dir():
        raise FormatError(f"destination parent does not exist: {destination.parent}")
    staging = Path(tempfile.mkdtemp(
        prefix=f".{destination.name}.partial-", dir=destination.parent))
    staging.rmdir()
    root = _extract_into(source, staging, ref_name, commit_hex, birthplace)
    _rename_no_replace(staging, destination)
    return root


def _extract_graph(source: Path, destination: Path, ref_name: str | None,
                   commit_hex: str | None, birthplace: str) -> str:
    try:
        import blake3
        import pyarrow as pa
        import pyarrow.dataset as ds
        import pyarrow.parquet as pq
        from deltalake import DeltaTable
    except ImportError as error:
        raise FormatError(f"install requirements.lock in a reviewed virtual environment: {error}") from error

    with tempfile.TemporaryDirectory(prefix="watertown-recovery-") as temporary:
        backup = NativeBackup(source, Path(temporary), DeltaTable, ds, blake3)
        if commit_hex is not None:
            try:
                tip = bytes.fromhex(commit_hex)
            except ValueError as error:
                raise FormatError(f"invalid commit hash: {error}") from error
            if len(tip) != 32:
                raise FormatError("commit hash must contain 32 bytes")
        else:
            if ref_name not in backup.refs:
                raise FormatError(f"native ref {ref_name!r} does not exist")
            tip = backup.refs[ref_name]
        commit_bytes = backup.read_object(tip)
        commit = decode_commit(commit_bytes)
        if commit["pond_id"] != backup.pond_id:
            raise FormatError("selected commit belongs to a different pond")
        manifest_hash = commit["node_manifest_hash"]
        manifest_bytes = backup.read_object(manifest_hash)
        native_entries = decode_manifest(manifest_bytes)
        computed_manifest_root = node_manifest_root(native_entries, blake3)
        if computed_manifest_root != commit["node_manifest_root"]:
            raise FormatError("native node-manifest Merkle root does not match the commit")
        verify_tree_manifest(backup, commit, native_entries)

        by_id = {entry["node_id"]: entry for entry in native_entries}
        if len(by_id) != len(native_entries):
            raise FormatError("native manifest contains duplicate node IDs")
        roots = [entry for entry in native_entries if not entry["parent_node_id"] and not entry["name"]]
        if len(roots) != 1:
            raise FormatError("native manifest must contain exactly one root")
        paths = {roots[0]["node_id"]: "/"}

        def resolve(node_id: str, visiting: set[str]) -> str:
            if node_id in paths:
                return paths[node_id]
            if node_id in visiting:
                raise FormatError("native manifest contains a parent cycle")
            entry = by_id.get(node_id)
            if entry is None or not entry["parent_node_id"] or not entry["name"]:
                raise FormatError(f"native node {node_id!r} is disconnected")
            if "/" in entry["name"] or entry["name"] in (".", ".."):
                raise FormatError(f"unsafe native node name {entry['name']!r}")
            visiting.add(node_id)
            parent = resolve(entry["parent_node_id"], visiting)
            visiting.remove(node_id)
            path = f"/{entry['name']}" if parent == "/" else f"{parent}/{entry['name']}"
            paths[node_id] = path
            return path

        for node_id in by_id:
            resolve(node_id, set())
        if len(set(paths.values())) != len(paths):
            raise FormatError("native manifest resolves multiple nodes to one path")

        recovery = destination / "recovery"
        objects_dir = recovery / "objects"
        manifests_dir = recovery / "manifests"
        refs_dir = recovery / "refs"
        objects_dir.mkdir(parents=True)
        manifests_dir.mkdir()
        refs_dir.mkdir()
        entries = []
        for native in native_entries:
            entry_type = native["entry_type"]
            child_hash = native["child_hash"]
            if entry_type == "dir:physical":
                node = {"kind": "directory"}
            elif entry_type == "symlink":
                node = {"kind": "symlink", "target": _copy_payload(
                    backup.object_path(child_hash), objects_dir, child_hash, blake3)}
            elif entry_type in ("dir:dynamic", "file:dynamic", "table:dynamic"):
                recipe_path = backup.object_path(child_hash)
                decode_recipe(backup.read_object(child_hash))
                node = {"kind": "dynamic", "recipe": _copy_payload(
                    recipe_path, objects_dir, child_hash, blake3)}
                metadata = dynamic_metadata(native["versions"], paths[native["node_id"]])
                if metadata is not None:
                    node["metadata"] = metadata
            else:
                payload_kind = "file" if entry_type.startswith("file:") else "table"
                series = decode_series(backup.read_object(child_hash)) \
                    if entry_type.endswith(":series") else None
                if series is not None and series["format"].startswith("watertown.series."):
                    node = _pack_stream_node(
                        backup,
                        objects_dir,
                        native,
                        paths[native["node_id"]],
                        child_hash,
                        series,
                        pa,
                        pq,
                        blake3,
                    )
                    entries.append({
                        "path": paths[native["node_id"]],
                        "entry_type": entry_type,
                        "source_node_id": native["node_id"],
                        "node": node,
                    })
                    continue
                hashes = (
                    [child_hash]
                    if series is None
                    else series["hashes"]
                )
                is_series = entry_type.endswith(":series")
                if len(hashes) != len(native["versions"]):
                    raise FormatError(f"{paths[native['node_id']]} has mismatched versions and metadata")
                objects, leaves, fingerprint = [], [], None
                for digest, metadata in zip(hashes, native["versions"]):
                    payload_path = backup.object_path(digest)
                    descriptor = _copy_payload(payload_path, objects_dir, digest, blake3)
                    attributes = None
                    if metadata["extended_attributes"] is not None:
                        attributes = canonical_attributes(metadata["extended_attributes"])
                    if payload_kind == "file":
                        if descriptor["size"] == 0:
                            if is_series:
                                raise FormatError(
                                    f"series {paths[native['node_id']]} contains an empty file "
                                    "version that pondcapsule.4 cannot represent"
                                )
                            if any(value is not None for value in metadata.values()):
                                raise FormatError(
                                    f"empty file version {paths[native['node_id']]} carries "
                                    "metadata that pondcapsule.4 cannot represent"
                                )
                            (objects_dir / f"blake3={digest.hex()}").unlink()
                            continue
                        def file_parts(path: Path = payload_path) -> Iterator[bytes]:
                            with path.open("rb") as stream:
                                while chunk := stream.read(1024 * 1024):
                                    yield chunk
                        logical_count = descriptor["size"]
                        logical_hash = _leaf_hash(
                            1, b"", logical_count, file_parts(), logical_count,
                            metadata, attributes, blake3)
                    else:
                        parquet = pq.ParquetFile(payload_path)
                        schema = read_parquet_schema(parquet, pa, FormatError)
                        current_fingerprint = blake3.blake3(canonical_schema(schema, pa)).digest()
                        if fingerprint is not None and fingerprint != current_fingerprint:
                            raise FormatError(f"table schema changes within {paths[native['node_id']]}")
                        fingerprint = current_fingerprint
                        logical_count, rows_size = 0, 0
                        for batch in parquet.iter_batches():
                            logical_count += batch.num_rows
                            rows_size += len(canonical_batch_rows(schema, batch, pa))
                        objects.append(descriptor)
                        if logical_count == 0:
                            if is_series:
                                raise FormatError(
                                    f"series {paths[native['node_id']]} contains an empty table "
                                    "version that pondcapsule.4 cannot represent"
                                )
                            if any(value is not None for value in metadata.values()):
                                raise FormatError(
                                    f"empty table version {paths[native['node_id']]} carries "
                                    "metadata that pondcapsule.4 cannot represent"
                                )
                            continue
                        def table_parts(parquet: Any = parquet, schema: Any = schema) -> Iterator[bytes]:
                            yield b"watertown.series-rows.v1\n" + struct.pack("<Q", logical_count)
                            for batch in parquet.iter_batches():
                                yield canonical_batch_rows(schema, batch, pa)
                        payload_size = len(b"watertown.series-rows.v1\n") + 8 + rows_size
                        logical_hash = _leaf_hash(
                            0, fingerprint, logical_count, table_parts(), payload_size,
                            metadata, attributes, blake3)
                    if payload_kind == "file":
                        objects.append(descriptor)
                    leaf = {
                        "logical_hash": logical_hash,
                        "logical_count": logical_count,
                        "source_timestamp": metadata["timestamp"] or 0,
                        "min_event_time": metadata["min_event_time"],
                        "max_event_time": metadata["max_event_time"],
                        "logical_attributes": attributes,
                    }
                    if payload_kind == "table":
                        leaf["schema_fingerprint"] = current_fingerprint.hex()
                    leaves.append(leaf)
                if payload_kind == "table" and fingerprint is None:
                    raise FormatError(f"table {paths[native['node_id']]} has no readable schema")
                node = {
                    "kind": "physical",
                    "payload_kind": payload_kind,
                    "logical_root": _series_root(payload_kind, fingerprint, leaves, blake3),
                    "objects": objects,
                    "leaves": leaves,
                }
                if fingerprint is not None:
                    node = {
                        "kind": node["kind"],
                        "payload_kind": node["payload_kind"],
                        "schema_fingerprint": fingerprint.hex(),
                        "logical_root": node["logical_root"],
                        "objects": node["objects"],
                        "leaves": node["leaves"],
                    }
            entries.append({
                "path": paths[native["node_id"]],
                "entry_type": entry_type,
                "source_node_id": native["node_id"],
                "node": node,
            })
        entries.sort(key=lambda entry: entry["path"].encode())
        manifest = {
            "format": "pondcapsule.4",
            "source": {
                "pond_id": backup.pond_id,
                "birthplace": birthplace,
                "source_tip": tip.hex(),
                "exported_at_micros": commit["time_micros"],
                "tool_version": "recovery-recipe-watertown.commit.v1",
            },
            "entries": entries,
        }
        manifest_bytes = json.dumps(
            manifest, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
        capsule_root = blake3.blake3(b"pondcapsule.root.4\n" + manifest_bytes).hexdigest()
        (manifests_dir / f"{capsule_root}.json").write_bytes(manifest_bytes)
        (refs_dir / "latest").write_text(capsule_root + "\n", encoding="ascii")
        kit = Path(__file__).resolve().parent
        for source_name, destination_name in (
            ("CAPSULE-README.md", "CAPSULE-README.md"),
            ("CAPSULE-FORMAT.md", "CAPSULE-FORMAT.md"),
            ("capsule.py", "capsule.py"),
            ("parquet_schema.py", "parquet_schema.py"),
            ("capsule-requirements.lock", "capsule-requirements.lock"),
            ("recover.sh", "recover.sh"),
        ):
            shutil.copyfile(kit / source_name, destination / destination_name)
        return capsule_root


def verify_fixtures(path: Path) -> None:
    fixture = json.loads(path.read_text())
    legacy_commit_bytes = bytes.fromhex(fixture["commit_hex"])
    commit_bytes = b"watertown.commit.v1\n\x01" + legacy_commit_bytes[len(b"dp.commit.3\n"):]
    manifest_bytes = bytes.fromhex(fixture["manifest_hex"])
    tree_bytes = bytes.fromhex(fixture["tree_hex"])
    series_bytes = bytes.fromhex(fixture["series_hex"])
    recipe_bytes = bytes.fromhex(fixture["recipe_hex"])
    commit = decode_commit(commit_bytes)
    assert commit["pond_id"] == "pond-x" and commit["seq"] == -7
    assert commit["parent_commit_hash"] == bytes(range(32, 64))
    manifest = decode_manifest(manifest_bytes)
    assert [entry["node_id"] for entry in manifest] == ["node-file", "root"]
    assert manifest[0]["versions"][0]["extended_attributes"] == '{"a":1}'
    assert manifest[0]["versions"][1]["timestamp"] == 124
    tree = decode_tree(tree_bytes)
    assert len(tree) == 1 and tree[0]["name"] == "data.bin"
    assert tree[0]["versions"] == manifest[0]["versions"]
    assert decode_series(series_bytes) == {
        "format": "dp.series.1",
        "hashes": [bytes(range(64, 96)), bytes(range(96, 128))],
    }
    assert decode_recipe(recipe_bytes) == ("factory-x", b"\0\xffconfig")

    malformed = [
        (decode_commit, commit_bytes + b"\0"),
        (decode_manifest, manifest_bytes[:-1]),
        (decode_tree, tree_bytes + b"\0"),
        (decode_series, series_bytes[:-1]),
        (decode_recipe, b"not-a-recipe"),
    ]
    for decoder, value in malformed:
        try:
            decoder(value)
        except FormatError:
            pass
        else:
            raise AssertionError(f"{decoder.__name__} accepted malformed input")
    for attributes in ('{"fraction":1.5}', '{"too_large":18446744073709551616}'):
        try:
            canonical_attributes(attributes)
        except FormatError:
            pass
        else:
            raise AssertionError("canonical attributes accepted unsupported numbers")

    with tempfile.TemporaryDirectory(prefix="watertown-extract-test-") as temporary:
        root = Path(temporary)
        source = root / "source"
        destination = root / "destination"
        source.mkdir()
        destination.mkdir()
        (source / "source").write_text("source")
        (destination / "destination").write_text("destination")
        try:
            _rename_no_replace(source, destination)
        except FormatError:
            pass
        else:
            raise AssertionError("no-replace promotion accepted an existing destination")
        assert (source / "source").read_text() == "source"
        assert (destination / "destination").read_text() == "destination"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", nargs="?", type=Path)
    parser.add_argument("destination", nargs="?", type=Path)
    selector = parser.add_mutually_exclusive_group()
    selector.add_argument("--ref")
    selector.add_argument("--commit")
    parser.add_argument("--birthplace")
    parser.add_argument("--verify-fixtures", type=Path)
    args = parser.parse_args()
    try:
        if args.verify_fixtures is not None:
            verify_fixtures(args.verify_fixtures)
            print("native fixtures verified")
            return 0
        if args.source is None or args.destination is None or args.birthplace is None:
            parser.error("source, destination, and --birthplace are required for extraction")
        if args.ref is None and args.commit is None:
            parser.error("one of --ref or --commit is required for extraction")
        root = extract(args.source, args.destination, args.ref, args.commit, args.birthplace)
        print(f"capsule {root} written to {args.destination}")
        return 0
    except (FormatError, OSError, AssertionError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
