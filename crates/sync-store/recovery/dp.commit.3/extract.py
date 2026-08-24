#!/usr/bin/env python3
"""Extract a pondcapsule.1 from a local dp.commit.3 Delta backup."""

from __future__ import annotations

import argparse
import json
import math
import shutil
import struct
import sys
import tempfile
from pathlib import Path
from typing import Any, Iterator

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


def decode_commit(data: bytes) -> dict[str, Any]:
    cur = Cursor(data)
    cur.tag(b"dp.commit.3\n")
    root = cur.hash()
    parent_flag = cur.u8()
    if parent_flag not in (0, 1):
        raise FormatError(f"invalid parent flag {parent_flag}")
    parent = cur.hash() if parent_flag else None
    manifest = cur.hash()
    manifest_root = cur.hash()
    result = {
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
    cur.tag(b"dp.manifest.2\n")
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
    cur.tag(b"dp.tree.2\n")
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


def decode_series(data: bytes) -> list[bytes]:
    cur = Cursor(data)
    cur.tag(b"dp.series.1\n")
    result = [cur.hash() for _ in range(cur.u32())]
    cur.finish()
    return result


def decode_recipe(data: bytes) -> tuple[str, bytes]:
    cur = Cursor(data)
    cur.tag(b"dp.recipe.1\n")
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
    result = b"dp.series-schema.1\n" + struct.pack("<I", len(schema))
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
    digest.update(b"dp.series-leaf.1\n" + bytes([kind]))
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
    digest = blake3.blake3(b"pondcapsule.series.1\n")
    digest.update(b"\0" if kind == "file" else b"\1")
    digest.update(b"\0" if fingerprint is None else b"\1" + fingerprint)
    digest.update(struct.pack("<Q", len(leaves)))
    for leaf in leaves:
        digest.update(bytes.fromhex(leaf["logical_hash"]))
        digest.update(struct.pack("<Q", leaf["logical_count"]))
        flags = int(leaf["min_event_time"] is not None)
        flags |= int(leaf["max_event_time"] is not None) << 1
        flags |= int(leaf["logical_attributes"] is not None) << 2
        digest.update(bytes([flags]))
        if leaf["min_event_time"] is not None:
            digest.update(struct.pack("<q", leaf["min_event_time"]))
        if leaf["max_event_time"] is not None:
            digest.update(struct.pack("<q", leaf["max_event_time"]))
        if leaf["logical_attributes"] is not None:
            encoded = leaf["logical_attributes"].encode()
            digest.update(struct.pack("<Q", len(encoded)) + encoded)
    return digest.hexdigest()


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


def _extract_into(source: Path, destination: Path, ref_name: str | None,
                  commit_hex: str | None, birthplace: str) -> str:
    destination.mkdir(mode=0o700)
    try:
        return _extract_graph(source, destination, ref_name, commit_hex, birthplace)
    except Exception:
        shutil.rmtree(destination)
        raise


def extract(source: Path, destination: Path, ref_name: str | None, commit_hex: str | None,
            birthplace: str) -> str:
    if destination.exists():
        raise FormatError(f"destination already exists: {destination}")
    if not birthplace:
        raise FormatError("birthplace must not be empty")
    source = source.resolve()
    destination = destination.resolve()
    if destination.is_relative_to(source):
        raise FormatError("destination must not be inside the source backup")
    if not destination.parent.is_dir():
        raise FormatError(f"destination parent does not exist: {destination.parent}")
    staging = Path(tempfile.mkdtemp(
        prefix=f".{destination.name}.partial-", dir=destination.parent))
    staging.rmdir()
    root = _extract_into(source, staging, ref_name, commit_hex, birthplace)
    staging.rename(destination)
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
            else:
                payload_kind = "file" if entry_type.startswith("file:") else "table"
                hashes = decode_series(backup.read_object(child_hash)) \
                    if entry_type.endswith(":series") else [child_hash]
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
                                    "version that pondcapsule.1 cannot represent"
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
                        schema = parquet.schema_arrow
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
                                    "version that pondcapsule.1 cannot represent"
                                )
                            continue
                        def table_parts(parquet: Any = parquet, schema: Any = schema) -> Iterator[bytes]:
                            yield b"dp.series-rows.1\n" + struct.pack("<Q", logical_count)
                            for batch in parquet.iter_batches():
                                yield canonical_batch_rows(schema, batch, pa)
                        payload_size = len(b"dp.series-rows.1\n") + 8 + rows_size
                        logical_hash = _leaf_hash(
                            0, fingerprint, logical_count, table_parts(), payload_size,
                            metadata, attributes, blake3)
                    if payload_kind == "file":
                        objects.append(descriptor)
                    leaves.append({
                        "logical_hash": logical_hash,
                        "logical_count": logical_count,
                        "source_timestamp": metadata["timestamp"] or 0,
                        "min_event_time": metadata["min_event_time"],
                        "max_event_time": metadata["max_event_time"],
                        "logical_attributes": attributes,
                    })
                if payload_kind == "table" and fingerprint is None:
                    raise FormatError(f"table {paths[native['node_id']]} has no readable schema")
                node = {
                    "kind": "physical",
                    "payload_kind": payload_kind,
                    "schema_fingerprint": None if fingerprint is None else fingerprint.hex(),
                    "logical_root": _series_root(payload_kind, fingerprint, leaves, blake3),
                    "objects": objects,
                    "leaves": leaves,
                }
            entries.append({
                "path": paths[native["node_id"]],
                "entry_type": entry_type,
                "source_node_id": native["node_id"],
                "node": node,
            })
        entries.sort(key=lambda entry: entry["path"].encode())
        manifest = {
            "format": "pondcapsule.1",
            "source": {
                "pond_id": backup.pond_id,
                "birthplace": birthplace,
                "source_tip": tip.hex(),
                "exported_at_micros": commit["time_micros"],
                "tool_version": "recovery-recipe-dp.commit.3",
            },
            "entries": entries,
        }
        manifest_bytes = json.dumps(
            manifest, ensure_ascii=False, separators=(",", ":")).encode()
        capsule_root = blake3.blake3(b"pondcapsule.root.1\n" + manifest_bytes).hexdigest()
        (manifests_dir / f"{capsule_root}.json").write_bytes(manifest_bytes)
        (refs_dir / "latest").write_text(capsule_root + "\n", encoding="ascii")
        kit = Path(__file__).resolve().parent
        for source_name, destination_name in (
            ("CAPSULE-README.md", "CAPSULE-README.md"),
            ("CAPSULE-FORMAT.md", "CAPSULE-FORMAT.md"),
            ("capsule.py", "capsule.py"),
            ("capsule-requirements.lock", "capsule-requirements.lock"),
            ("recover.sh", "recover.sh"),
        ):
            shutil.copyfile(kit / source_name, destination / destination_name)
        return capsule_root


def verify_fixtures(path: Path) -> None:
    fixture = json.loads(path.read_text())
    commit_bytes = bytes.fromhex(fixture["commit_hex"])
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
    assert decode_series(series_bytes) == [
        bytes(range(64, 96)), bytes(range(96, 128))]
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
