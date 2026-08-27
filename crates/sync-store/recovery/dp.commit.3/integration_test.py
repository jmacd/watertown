#!/usr/bin/env python3
"""Generate a native backup, extract it independently, and verify the capsule."""

from __future__ import annotations

import argparse
import json
import os
import struct
import subprocess
import sys
import tempfile
from decimal import Decimal
from pathlib import Path

import blake3
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import write_deltalake

from capsule import CapsuleError, _encoded_logical_path, load_and_verify, materialize
from extract import FormatError, decode_manifest, extract, node_manifest_root

NIL_UUID = "00000000-0000-0000-0000-000000000000"
POND_ID = "11111111-1111-1111-1111-111111111111"
NOW = 1_700_000_000_000_000
EXTERNAL_FILE = (b"external recovery payload\n" * 4096) + b"end\n"
SYMLINK_TARGET = b"/data/file"
RECIPE_BYTES = (
    b"dp.recipe.1\n"
    + struct.pack("<I", len(b"fixture-factory"))
    + b"fixture-factory"
    + b'{"source":"integration"}'
)


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


def build_native_backup(
    root: Path,
    *,
    include_empty_singleton: bool = False,
    include_empty_series_version: bool = False,
) -> None:
    table_path = root.parent / "fixture.parquet"
    schema = pa.schema(
        [
            pa.field("reading", pa.int64(), True),
            pa.field("label", pa.string(), False),
            pa.field("precise", pa.decimal128(38, 6), True),
        ],
        metadata={b"origin": b"fixture"},
    )
    pq.write_table(
        pa.Table.from_arrays(
            [
                pa.array([1, None, 3], type=pa.int64()),
                pa.array(["a", "b", "c"]),
                pa.array(
                    [
                        Decimal("12345678901234567890123456789012.345678"),
                        None,
                        Decimal("-99999999999999999999999999999999.999999"),
                    ],
                    type=pa.decimal128(38, 6),
                ),
            ],
            schema=schema,
        ),
        table_path,
    )
    table = table_path.read_bytes()
    table_path.unlink()
    empty_table = None
    if include_empty_series_version:
        empty_path = root.parent / "empty-fixture.parquet"
        pq.write_table(
            pa.Table.from_batches(
                [
                    pa.RecordBatch.from_arrays(
                        [
                            pa.array([], type=pa.int64()),
                            pa.array([], type=pa.string()),
                            pa.array([], type=pa.decimal128(38, 6)),
                        ],
                        schema=schema,
                    )
                ],
                schema=schema,
            ),
            empty_path,
        )
        empty_table = empty_path.read_bytes()
        empty_path.unlink()
    table_hashes = [digest(table)]
    table_versions = [metadata(102)]
    if empty_table is not None:
        table_hashes.append(digest(empty_table))
        table_versions.append(metadata(103))
    series = (
        b"dp.series.1\n"
        + struct.pack("<I", len(table_hashes))
        + b"".join(table_hashes)
    )

    children = [
        ("dynamic", 5, digest(RECIPE_BYTES), []),
        ("file", 4, digest(EXTERNAL_FILE), [metadata(101)]),
        ("link", 3, digest(SYMLINK_TARGET), []),
        ("table", 8, digest(series), table_versions),
    ]
    if include_empty_singleton:
        children.insert(1, ("empty", 4, digest(b""), [metadata(100)]))
    tree = (
        b"dp.tree.2\n"
        + struct.pack("<I", len(children))
        + b"".join(tree_entry(*entry) for entry in children)
    )
    tree_hash = digest(tree)
    entries = [
        ("file", "root", "file", 4, digest(EXTERNAL_FILE), [metadata(101)]),
        ("link", "root", "link", 3, digest(SYMLINK_TARGET), []),
        ("recipe", "root", "dynamic", 5, digest(RECIPE_BYTES), []),
        ("root", "", "", 1, tree_hash, []),
        ("table", "root", "table", 8, digest(series), table_versions),
    ]
    if include_empty_singleton:
        entries.insert(0, ("empty", "root", "empty", 4, digest(b""), [metadata(100)]))
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
        digest(SYMLINK_TARGET): SYMLINK_TARGET,
        digest(RECIPE_BYTES): RECIPE_BYTES,
        digest(series): series,
        tree_hash: tree,
        manifest_hash: manifest,
        commit_hash: commit,
    }
    if include_empty_singleton:
        inline[digest(b"")] = b""
    if empty_table is not None:
        inline[digest(empty_table)] = empty_table

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
    (blobs / f"blob={digest(EXTERNAL_FILE).hex()}").write_bytes(EXTERNAL_FILE)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pond",
        type=Path,
        help="optional current pond binary for additional compatibility verification",
    )
    args = parser.parse_args()
    pond = args.pond.resolve() if args.pond is not None else None
    if pond is not None and not pond.is_file():
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
        for name in (
            "CAPSULE-README.md",
            "CAPSULE-FORMAT.md",
            "capsule.py",
            "capsule-requirements.lock",
            "recover.sh",
        ):
            assert (destination / name).is_file(), name
        subprocess.run(
            [sys.executable, str(destination / "capsule.py"), "verify", str(destination)],
            check=True,
        )
        _, report = load_and_verify(destination)
        assert report["root"] == root

        recovered = workspace / "materialized"
        materialize(destination, recovered)
        file_path = _encoded_logical_path("/file")
        table_path = _encoded_logical_path("/table")
        link_path = _encoded_logical_path("/link")
        dynamic_path = _encoded_logical_path("/dynamic")
        assert (
            recovered / "files" / file_path / "version-000001.bin"
        ).read_bytes() == EXTERNAL_FILE
        recovered_table = pq.read_table(
            recovered / "tables" / table_path / "version-000001.parquet"
        )
        assert recovered_table.column("reading").to_pylist() == [1, None, 3]
        assert recovered_table.column("label").to_pylist() == ["a", "b", "c"]
        assert recovered_table.column("precise").to_pylist() == [
            Decimal("12345678901234567890123456789012.345678"),
            None,
            Decimal("-99999999999999999999999999999999.999999"),
        ]
        assert (
            recovered / "symlinks" / link_path / "target.bin"
        ).read_bytes() == SYMLINK_TARGET
        assert not (
            recovered / "symlinks" / link_path / "target.bin"
        ).is_symlink()
        assert (
            recovered / "dynamic-recipes" / dynamic_path / "recipe.bin"
        ).read_bytes() == RECIPE_BYTES
        assert json.loads(
            (
                recovered / "dynamic-recipes" / dynamic_path / "factory.json"
            ).read_text()
        ) == {"factory": "fixture-factory"}
        assert (
            recovered / "dynamic-recipes" / dynamic_path / "config.bin"
        ).read_bytes() == b'{"source":"integration"}'
        wrapper_recovered = workspace / "wrapper-materialized"
        wrapper_environment = workspace / "wrapper-environment"
        subprocess.run(
            [
                sys.executable,
                "-m",
                "venv",
                "--system-site-packages",
                str(wrapper_environment),
            ],
            check=True,
        )
        environment = os.environ.copy()
        environment["PYTHON"] = sys.executable
        environment["RECOVERY_VENV"] = str(wrapper_environment)
        subprocess.run(
            [
                "sh",
                str(destination / "recover.sh"),
                str(destination),
                str(wrapper_recovered),
            ],
            check=True,
            env=environment,
        )
        assert (
            wrapper_recovered / "files" / file_path / "version-000001.bin"
        ).read_bytes() == EXTERNAL_FILE
        try:
            materialize(destination, recovered)
        except CapsuleError as error:
            assert "already exists" in str(error), error
        else:
            raise AssertionError("materializer accepted an existing destination")

        if pond is not None:
            subprocess.run(
                [str(pond), "capsule", "verify", str(destination)],
                check=True,
            )
        print(f"integration capsule verified and materialized: {root}")

        empty_singleton_source = workspace / "native-empty-singleton"
        build_native_backup(empty_singleton_source, include_empty_singleton=True)
        try:
            extract(
                empty_singleton_source,
                workspace / "empty-singleton-capsule",
                "main",
                None,
                "fixture",
            )
        except FormatError as error:
            assert "empty file version" in str(error) and "metadata" in str(error), error
        else:
            raise AssertionError("extractor silently dropped empty singleton metadata")

        empty_source = workspace / "native-empty-series"
        build_native_backup(empty_source, include_empty_series_version=True)
        try:
            extract(
                empty_source,
                workspace / "empty-series-capsule",
                "main",
                None,
                "fixture",
            )
        except FormatError as error:
            assert "empty table version" in str(error), error
        else:
            raise AssertionError("extractor silently dropped an empty series version")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
