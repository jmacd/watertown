#!/usr/bin/env python3
"""Generate a native backup, extract it independently, and verify the capsule."""

from __future__ import annotations

import argparse
import json
import struct
import subprocess
import tempfile
from pathlib import Path

import blake3
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import write_deltalake

from extract import FormatError, decode_manifest, extract, node_manifest_root

NIL_UUID = "00000000-0000-0000-0000-000000000000"
POND_ID = "11111111-1111-1111-1111-111111111111"
NOW = 1_700_000_000_000_000


def digest(value: bytes) -> bytes:
    return blake3.blake3(value).digest()


def length_prefixed(value: bytes) -> bytes:
    return struct.pack("<I", len(value)) + value


def metadata(timestamp: int) -> bytes:
    return b"\x08" + struct.pack("<q", timestamp)


def tree_entry(name: str, kind: int, child: bytes, versions: list[bytes]) -> bytes:
    return (
        length_prefixed(name.encode())
        + bytes([kind])
        + child
        + struct.pack("<I", len(versions))
        + b"".join(versions)
    )


def manifest_entry(
    node_id: str,
    parent_id: str,
    name: str,
    kind: int,
    child: bytes,
    versions: list[bytes],
) -> bytes:
    return (
        length_prefixed(node_id.encode())
        + length_prefixed(parent_id.encode())
        + length_prefixed(name.encode())
        + bytes([kind])
        + child
        + struct.pack("<I", len(versions))
        + b"".join(versions)
    )


def build_native_backup(root: Path) -> None:
    external_file = (b"external recovery payload\n" * 4096) + b"end\n"
    table_path = root.parent / "fixture.parquet"
    schema = pa.schema(
        [
            pa.field("reading", pa.int64(), True),
            pa.field("label", pa.string(), False),
        ],
        metadata={b"origin": b"fixture"},
    )
    pq.write_table(
        pa.Table.from_arrays(
            [
                pa.array([1, None, 3], type=pa.int64()),
                pa.array(["a", "b", "c"]),
            ],
            schema=schema,
        ),
        table_path,
    )
    table = table_path.read_bytes()
    table_path.unlink()
    symlink = b"/data/file"
    recipe = (
        b"dp.recipe.1\n"
        + length_prefixed(b"fixture-factory")
        + b'{"source":"integration"}'
    )
    series = b"dp.series.1\n" + struct.pack("<I", 1) + digest(table)

    children = [
        ("dynamic", 5, digest(recipe), []),
        ("file", 4, digest(external_file), [metadata(101)]),
        ("link", 3, digest(symlink), []),
        ("table", 8, digest(series), [metadata(102)]),
    ]
    tree = (
        b"dp.tree.2\n"
        + struct.pack("<I", len(children))
        + b"".join(tree_entry(*entry) for entry in children)
    )
    tree_hash = digest(tree)
    entries = [
        ("file", "root", "file", 4, digest(external_file), [metadata(101)]),
        ("link", "root", "link", 3, digest(symlink), []),
        ("recipe", "root", "dynamic", 5, digest(recipe), []),
        ("root", "", "", 1, tree_hash, []),
        ("table", "root", "table", 8, digest(series), [metadata(102)]),
    ]
    manifest = (
        b"dp.manifest.2\n"
        + struct.pack("<I", len(entries))
        + b"".join(manifest_entry(*entry) for entry in entries)
    )
    manifest_hash = digest(manifest)
    manifest_root = node_manifest_root(decode_manifest(manifest), blake3)
    commit = (
        b"dp.commit.3\n"
        + tree_hash
        + b"\0"
        + manifest_hash
        + manifest_root
        + length_prefixed(POND_ID.encode())
        + struct.pack("<qq", 2, NOW)
        + length_prefixed(b"integration-test")
        + length_prefixed(b"recover fixture")
    )
    commit_hash = digest(commit)
    inline = {
        digest(table): table,
        digest(symlink): symlink,
        digest(recipe): recipe,
        digest(series): series,
        tree_hash: tree,
        manifest_hash: manifest,
        commit_hash: commit,
    }

    rows: list[tuple[str, str, str, int, bool, bytes, bytes, int]] = []

    def add(
        pond_id: str,
        partition: str,
        key: str,
        sequence: int,
        deleted: bool,
        value: bytes,
    ) -> None:
        rows.append(
            (
                pond_id,
                partition,
                key,
                sequence,
                deleted,
                value,
                digest(value),
                NOW,
            )
        )

    add(NIL_UUID, "meta", "pond_id", 1, False, POND_ID.encode())
    for object_hash, value in inline.items():
        add(POND_ID, "objects", object_hash.hex(), 1, False, value)
    add(POND_ID, "refs", "main", 1, False, b"\xff" * 32)
    add(POND_ID, "refs", "main", 2, False, commit_hash)
    dead = b"unreachable historical object"
    add(POND_ID, "objects", digest(dead).hex(), 1, False, dead)
    add(POND_ID, "objects", digest(dead).hex(), 2, True, b"")

    arrow = pa.table(
        {
            "pond_id": pa.array([row[0] for row in rows], type=pa.string()),
            "partition_key": pa.array([row[1] for row in rows], type=pa.string()),
            "item_key": pa.array([row[2] for row in rows], type=pa.string()),
            "txn_seq": pa.array([row[3] for row in rows], type=pa.int64()),
            "deleted": pa.array([row[4] for row in rows], type=pa.bool_()),
            "value": pa.array([row[5] for row in rows], type=pa.binary()),
            "value_blake3": pa.array([row[6] for row in rows], type=pa.binary()),
            "ts_micros": pa.array([row[7] for row in rows], type=pa.int64()),
        }
    )
    write_deltalake(root, arrow, partition_by=["pond_id", "partition_key"])
    blobs = root / "_blobs"
    blobs.mkdir()
    (blobs / f"blob={digest(external_file).hex()}").write_bytes(external_file)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pond",
        type=Path,
        required=True,
        help="current pond binary used only for independent capsule verification",
    )
    args = parser.parse_args()
    pond = args.pond.resolve()
    if not pond.is_file():
        parser.error(f"pond binary does not exist: {pond}")

    fixtures = json.loads(
        Path(__file__).with_name("native-fixtures.json").read_text())
    fixture_manifest = decode_manifest(bytes.fromhex(fixtures["manifest_hex"]))
    assert (
        node_manifest_root(fixture_manifest, blake3).hex()
        == fixtures["manifest_root_hex"]
    )

    with tempfile.TemporaryDirectory(prefix="watertown-recipe-test-") as temporary:
        workspace = Path(temporary)
        source = workspace / "native"
        destination = workspace / "capsule"
        build_native_backup(source)
        try:
            extract(source, source / "unsafe-output", "main", None, "fixture")
        except FormatError:
            pass
        else:
            raise AssertionError("extractor accepted a destination inside the backup")
        root = extract(source, destination, "main", None, "fixture")
        subprocess.run(
            [str(pond), "capsule", "verify", str(destination)],
            check=True,
        )
        print(f"integration capsule verified: {root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
