#!/usr/bin/env python3
"""Extract an opaque pondcapsule.legacy.1 from a local dp.commit.3 backup."""

from __future__ import annotations

import argparse
import ctypes
import errno
import json
import os
import shutil
import struct
import sys
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

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
ROOT_DOMAIN = b"pondcapsule.legacy.root.1\n"


class FormatError(ValueError):
    """The source does not satisfy the documented native or capsule format."""


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
        entries.append(
            {
                "name": name,
                "entry_type": ENTRY_TYPES[kind],
                "child_hash": child_hash,
                "versions": versions,
            }
        )
    cur.finish()
    names = [entry["name"].encode() for entry in entries]
    if names != sorted(names) or len(names) != len(set(names)):
        raise FormatError("native tree entries are not uniquely sorted by name")
    return entries


def decode_series(data: bytes) -> list[bytes]:
    cur = Cursor(data)
    cur.tag(b"dp.series.1\n")
    hashes = [cur.hash() for _ in range(cur.u32())]
    cur.finish()
    return hashes


def decode_recipe(data: bytes) -> tuple[str, bytes]:
    cur = Cursor(data)
    cur.tag(b"dp.recipe.1\n")
    factory = cur.text()
    config = cur.take(len(data) - cur.offset)
    if not factory:
        raise FormatError("legacy dynamic recipe has an empty factory")
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
            b"\x01" + empty[depth + 1] + empty[depth + 1]
        ).digest()
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
        pairs.append(
            (
                blake3.blake3(entry["node_id"].encode()).digest(),
                blake3.blake3(value).digest(),
            )
        )
    pairs.sort()
    if any(left[0] == right[0] for left, right in zip(pairs, pairs[1:])):
        raise FormatError("duplicate node ID key in native manifest")

    def build(depth: int, subset: list[tuple[bytes, bytes]]) -> bytes:
        if not subset:
            return empty[depth]
        if depth == 256:
            if len(subset) != 1:
                raise FormatError("native manifest contains a BLAKE3 key collision")
            key, value = subset[0]
            return blake3.blake3(b"\x00" + key + value).digest()
        byte, shift = depth // 8, 7 - depth % 8
        split = 0
        while split < len(subset) and ((subset[split][0][byte] >> shift) & 1) == 0:
            split += 1
        return blake3.blake3(
            b"\x01"
            + build(depth + 1, subset[:split])
            + build(depth + 1, subset[split:])
        ).digest()

    return build(0, pairs)


def _bytes(value: Any, description: str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, memoryview):
        return value.tobytes()
    raise FormatError(f"{description} is not binary")


def _local_path(uri: str) -> str:
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        if parsed.netloc not in ("", "localhost"):
            raise FormatError(f"non-local Delta data URI {uri!r}")
        return unquote(parsed.path)
    if parsed.scheme:
        raise FormatError(f"downloaded backup contains non-local Delta data URI {uri!r}")
    return uri


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class NativeBackup:
    def __init__(
        self,
        root: Path,
        work: Path,
        DeltaTable: Any,
        duckdb: Any,
        blake3: Any,
    ):
        self.root = root
        self.work = work
        self.blake3 = blake3
        table = DeltaTable(str(root))
        files = [_local_path(uri) for uri in table.file_uris()]
        if not files:
            raise FormatError("native Delta backup has no active data files")
        file_list = "[" + ",".join(_sql_string(path) for path in files) + "]"
        self.connection = duckdb.connect()
        self.connection.execute(
            "CREATE TEMP VIEW native_rows AS SELECT * FROM "
            f"read_parquet({file_list}, hive_partitioning=true, union_by_name=true)"
        )
        self.pond_id = self._source_pond_id()
        self.refs: dict[str, bytes] = {}
        self.objects = work / "native-objects"
        self.objects.mkdir()
        self._materialize_live()

    def _source_pond_id(self) -> str:
        rows = self.connection.execute(
            """
            SELECT txn_seq, deleted, value, value_blake3
            FROM native_rows
            WHERE CAST(pond_id AS VARCHAR) = ?
              AND CAST(partition_key AS VARCHAR) = 'meta'
              AND CAST(item_key AS VARCHAR) = 'pond_id'
            ORDER BY txn_seq DESC
            """,
            [NIL_UUID],
        ).fetchall()
        if not rows:
            raise FormatError("native backup has no meta/pond_id row")
        maximum = rows[0][0]
        selected = [row for row in rows if row[0] == maximum]
        if len(selected) != 1:
            raise FormatError("ambiguous meta/pond_id rows at the maximum txn_seq")
        _, deleted, value, checksum = selected[0]
        value = _bytes(value, "meta/pond_id value")
        checksum = _bytes(checksum, "meta/pond_id value_blake3")
        if deleted:
            raise FormatError("native backup's latest meta/pond_id row is deleted")
        if self.blake3.blake3(value).digest() != checksum:
            raise FormatError("meta/pond_id value_blake3 mismatch")
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError as error:
            raise FormatError("meta/pond_id is not UTF-8") from error

    def _materialize_live(self) -> None:
        rows = self.connection.execute(
            """
            WITH candidates AS (
              SELECT CAST(partition_key AS VARCHAR) AS partition_key,
                     CAST(item_key AS VARCHAR) AS item_key,
                     txn_seq, deleted, value, value_blake3,
                     MAX(txn_seq) OVER (
                       PARTITION BY CAST(partition_key AS VARCHAR),
                                    CAST(item_key AS VARCHAR)
                     ) AS maximum
              FROM native_rows
              WHERE CAST(pond_id AS VARCHAR) = ?
                AND CAST(partition_key AS VARCHAR) IN ('refs', 'objects')
            )
            SELECT partition_key, item_key, txn_seq, deleted, value, value_blake3
            FROM candidates
            WHERE txn_seq = maximum
            ORDER BY partition_key, item_key
            """,
            [self.pond_id],
        ).fetchall()
        prior_key: tuple[str, str] | None = None
        for partition, item, _, deleted, value, checksum in rows:
            key = (partition, item)
            if key == prior_key:
                raise FormatError(f"ambiguous live rows for {key!r} at maximum txn_seq")
            prior_key = key
            if deleted:
                continue
            value = _bytes(value, f"native row {key!r} value")
            checksum = _bytes(checksum, f"native row {key!r} value_blake3")
            if self.blake3.blake3(value).digest() != checksum:
                raise FormatError(f"value_blake3 mismatch for {key!r}")
            if partition == "refs":
                if len(value) != 32:
                    raise FormatError(f"native ref {item!r} is not 32 bytes")
                self.refs[item] = value
            else:
                if (
                    len(item) != 64
                    or item.lower() != item
                    or self.blake3.blake3(value).hexdigest() != item
                ):
                    raise FormatError(f"native object key/hash mismatch for {item!r}")
                (self.objects / item).write_bytes(value)

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


def verify_tree_manifest(
    backup: NativeBackup, commit: dict[str, Any], entries: list[dict[str, Any]]
) -> None:
    roots = [
        entry
        for entry in entries
        if not entry["parent_node_id"] and not entry["name"]
    ]
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
                    f"non-directory native node {directory['node_id']!r} has children"
                )
            continue
        tree = decode_tree(backup.read_object(directory["child_hash"]))
        expected = sorted(
            children.get(directory["node_id"], []),
            key=lambda entry: entry["name"].encode(),
        )
        projection = [
            {
                "name": entry["name"],
                "entry_type": entry["entry_type"],
                "child_hash": entry["child_hash"],
                "versions": entry["versions"],
            }
            for entry in expected
        ]
        if tree != projection:
            raise FormatError(
                f"tree and manifest disagree at native node {directory['node_id']!r}"
            )


def _hash_file(path: Path, blake3: Any) -> tuple[str, int]:
    digest, size = blake3.blake3(), 0
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _copy_payload(
    source: Path, objects: Path, expected: bytes, blake3: Any
) -> dict[str, Any]:
    expected_hex = expected.hex()
    actual, size = _hash_file(source, blake3)
    if actual != expected_hex:
        raise FormatError(
            f"payload {source} hashes to {actual}, expected {expected_hex}"
        )
    target = objects / f"blake3={expected_hex}"
    if target.exists():
        existing, existing_size = _hash_file(target, blake3)
        if existing != expected_hex or existing_size != size:
            raise FormatError(f"conflicting copied payload {expected_hex}")
    else:
        shutil.copyfile(source, target)
    return {"hash": expected_hex, "size": size}


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


def extract(
    source: Path,
    destination: Path,
    ref_name: str | None,
    commit_hex: str | None,
    birthplace: str,
) -> str:
    if destination.exists() or destination.is_symlink():
        raise FormatError(f"destination already exists: {destination}")
    if not birthplace:
        raise FormatError("birthplace must not be empty")
    source = source.resolve()
    destination = destination.parent.resolve() / destination.name
    if destination.is_relative_to(source):
        raise FormatError("destination must not be inside the source backup")
    if not destination.parent.is_dir():
        raise FormatError(f"destination parent does not exist: {destination.parent}")
    staging = destination.parent / f".{destination.name}.partial-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    root = _extract_graph(source, staging, ref_name, commit_hex, birthplace)
    _rename_no_replace(staging, destination)
    return root


def _extract_graph(
    source: Path,
    destination: Path,
    ref_name: str | None,
    commit_hex: str | None,
    birthplace: str,
) -> str:
    try:
        import blake3
        import duckdb
        from deltalake import DeltaTable
    except ImportError as error:
        raise FormatError(
            f"install requirements.lock in a reviewed virtual environment: {error}"
        ) from error

    work = destination / ".native-work"
    work.mkdir()
    backup = NativeBackup(source, work, DeltaTable, duckdb, blake3)
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
    commit = decode_commit(backup.read_object(tip))
    if commit["pond_id"] != backup.pond_id:
        raise FormatError("selected commit belongs to a different pond")
    manifest_bytes = backup.read_object(commit["node_manifest_hash"])
    native_entries = decode_manifest(manifest_bytes)
    if (
        node_manifest_root(native_entries, blake3)
        != commit["node_manifest_root"]
    ):
        raise FormatError("native node-manifest Merkle root does not match the commit")
    verify_tree_manifest(backup, commit, native_entries)

    by_id = {entry["node_id"]: entry for entry in native_entries}
    if len(by_id) != len(native_entries):
        raise FormatError("native manifest contains duplicate node IDs")
    roots = [
        entry
        for entry in native_entries
        if not entry["parent_node_id"] and not entry["name"]
    ]
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
        if (
            "/" in entry["name"]
            or "\0" in entry["name"]
            or entry["name"] in (".", "..")
        ):
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
        path = paths[native["node_id"]]
        if entry_type == "dir:physical":
            if native["versions"]:
                raise FormatError(f"directory {path} unexpectedly carries version metadata")
            node = {"kind": "directory"}
        elif entry_type == "symlink":
            if native["versions"]:
                raise FormatError(f"symlink {path} unexpectedly carries version metadata")
            node = {
                "kind": "symlink",
                "target": _copy_payload(
                    backup.object_path(child_hash), objects_dir, child_hash, blake3
                ),
            }
        elif entry_type in ("dir:dynamic", "file:dynamic", "table:dynamic"):
            if native["versions"]:
                raise FormatError(
                    f"dynamic entry {path} unexpectedly carries version metadata"
                )
            decode_recipe(backup.read_object(child_hash))
            node = {
                "kind": "dynamic",
                "recipe": _copy_payload(
                    backup.object_path(child_hash), objects_dir, child_hash, blake3
                ),
            }
        else:
            payload_kind = "file" if entry_type.startswith("file:") else "table"
            is_series = entry_type.endswith(":series")
            if is_series:
                series_bytes = backup.read_object(child_hash)
                hashes = decode_series(series_bytes)
                series_object = _copy_payload(
                    backup.object_path(child_hash), objects_dir, child_hash, blake3
                )
            else:
                hashes = [child_hash]
                series_object = None
            if not hashes:
                raise FormatError(f"physical path {path} has no source versions")
            if len(hashes) != len(native["versions"]):
                raise FormatError(f"{path} has mismatched versions and metadata")
            versions = []
            for index, (digest, metadata) in enumerate(
                zip(hashes, native["versions"])
            ):
                descriptor = _copy_payload(
                    backup.object_path(digest), objects_dir, digest, blake3
                )
                attributes = metadata["extended_attributes"]
                if attributes is not None:
                    try:
                        value = json.loads(attributes)
                    except json.JSONDecodeError as error:
                        raise FormatError(
                            f"{path} version {index} attributes are invalid JSON: {error}"
                        ) from error
                    if not isinstance(value, dict):
                        raise FormatError(
                            f"{path} version {index} attributes are not a JSON object"
                        )
                versions.append(
                    {
                        "source_version": index,
                        "objects": [descriptor],
                        "source_timestamp": metadata["timestamp"],
                        "min_event_time": metadata["min_event_time"],
                        "max_event_time": metadata["max_event_time"],
                        "extended_attributes": attributes,
                    }
                )
            node = {
                "kind": "physical",
                "payload_kind": payload_kind,
                "source_child_hash": child_hash.hex(),
                "series_object": series_object,
                "versions": versions,
            }
        entries.append(
            {
                "path": path,
                "entry_type": entry_type,
                "source_node_id": native["node_id"],
                "node": node,
            }
        )
    entries.sort(key=lambda entry: entry["path"].encode())
    manifest = {
        "format": "pondcapsule.legacy.1",
        "source": {
            "pond_id": backup.pond_id,
            "birthplace": birthplace,
            "source_tip": tip.hex(),
            "exported_at_micros": commit["time_micros"],
            "tool_version": "recovery-recipe-legacy-migration.1",
            "native_format": "dp.commit.3",
        },
        "entries": entries,
    }
    encoded = json.dumps(
        manifest, ensure_ascii=False, separators=(",", ":")
    ).encode()
    capsule_root = blake3.blake3(ROOT_DOMAIN + encoded).hexdigest()
    (manifests_dir / f"{capsule_root}.json").write_bytes(encoded)
    (refs_dir / "latest").write_text(capsule_root + "\n", encoding="ascii")
    shutil.rmtree(work)
    kit = Path(__file__).resolve().parent
    for name in (
        "CAPSULE-README.md",
        "CAPSULE-FORMAT.md",
        "capsule.py",
        "capsule-requirements.lock",
        "recover.sh",
    ):
        shutil.copyfile(kit / name, destination / name)
    return capsule_root


def verify_fixtures(path: Path) -> None:
    fixture = json.loads(path.read_text())
    commit = decode_commit(bytes.fromhex(fixture["commit_hex"]))
    manifest = decode_manifest(bytes.fromhex(fixture["manifest_hex"]))
    tree = decode_tree(bytes.fromhex(fixture["tree_hex"]))
    series = decode_series(bytes.fromhex(fixture["series_hex"]))
    recipe = decode_recipe(bytes.fromhex(fixture["recipe_hex"]))
    if commit["pond_id"] != "pond-x" or commit["seq"] != -7:
        raise AssertionError("commit fixture mismatch")
    if [entry["node_id"] for entry in manifest] != ["node-file", "root"]:
        raise AssertionError("manifest fixture mismatch")
    if len(tree) != 1 or tree[0]["name"] != "data.bin":
        raise AssertionError("tree fixture mismatch")
    if series != [bytes(range(64, 96)), bytes(range(96, 128))]:
        raise AssertionError("series fixture mismatch")
    if recipe != ("factory-x", b"\0\xffconfig"):
        raise AssertionError("recipe fixture mismatch")


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
            parser.error("source, destination, and --birthplace are required")
        if args.ref is None and args.commit is None:
            parser.error("one of --ref or --commit is required")
        root = extract(
            args.source, args.destination, args.ref, args.commit, args.birthplace
        )
        print(f"legacy capsule {root} written to {args.destination}")
        return 0
    except (FormatError, OSError, AssertionError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
