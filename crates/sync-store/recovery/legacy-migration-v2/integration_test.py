#!/usr/bin/env python3
"""Build evolving legacy tables and extract them without importing PyArrow."""

from __future__ import annotations

import ast
import json
import os
import shutil
import struct
import subprocess
import sys
import tempfile
from pathlib import Path

import blake3
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import write_deltalake

from capsule import CapsuleError, ROOT_DOMAIN, load_and_verify, materialize
from extract import decode_manifest, node_manifest_root

NIL_UUID = "00000000-0000-0000-0000-000000000000"
POND_ID = "11111111-1111-1111-1111-111111111111"
NOW = 1_700_000_000_000_000
ATTRS = '{"watertown.timestamp_column":"timestamp"}'
FILE_BYTES = b"opaque legacy file\n"
SYMLINK_TARGET = b"/data/file"
RECIPE = (
    b"dp.recipe.1\n"
    + struct.pack("<I", len(b"legacy-fixture"))
    + b"legacy-fixture"
    + b"enabled: false\n"
)


def digest(data: bytes) -> bytes:
    return blake3.blake3(data).digest()


def lp(data: bytes) -> bytes:
    return struct.pack("<I", len(data)) + data


def metadata(timestamp: int) -> bytes:
    encoded = ATTRS.encode()
    return (
        b"\x0f"
        + struct.pack("<q", timestamp)
        + struct.pack("<q", timestamp)
        + lp(encoded)
        + struct.pack("<q", timestamp)
    )


def timestamp_metadata(timestamp: int) -> bytes:
    return b"\x08" + struct.pack("<q", timestamp)


def tree_entry(name: str, kind: int, child: bytes, versions: list[bytes]) -> bytes:
    return (
        lp(name.encode())
        + bytes([kind])
        + child
        + struct.pack("<I", len(versions))
        + b"".join(versions)
    )


def manifest_entry(
    node_id: str,
    parent: str,
    name: str,
    kind: int,
    child: bytes,
    versions: list[bytes],
) -> bytes:
    return (
        lp(node_id.encode())
        + lp(parent.encode())
        + lp(name.encode())
        + bytes([kind])
        + child
        + struct.pack("<I", len(versions))
        + b"".join(versions)
    )


def parquet_bytes(schema: pa.Schema, arrays: list[pa.Array], path: Path) -> bytes:
    pq.write_table(pa.Table.from_arrays(arrays, schema=schema), path)
    data = path.read_bytes()
    path.unlink()
    return data


def build_native_backup(root: Path) -> tuple[bytes, bytes]:
    first_schema = pa.schema(
        [
            pa.field("timestamp", pa.timestamp("us"), False),
            pa.field("value", pa.string(), False),
        ]
    )
    second_schema = pa.schema(
        [
            pa.field("timestamp", pa.timestamp("us"), False),
            pa.field("value", pa.string(), False),
            pa.field("note", pa.string(), True),
        ]
    )
    first = parquet_bytes(
        first_schema,
        [
            pa.array([100], type=pa.timestamp("us")),
            pa.array(["first"], type=pa.string()),
        ],
        root.parent / "first.parquet",
    )
    second = parquet_bytes(
        second_schema,
        [
            pa.array([200], type=pa.timestamp("us")),
            pa.array(["second"], type=pa.string()),
            pa.array(["added"], type=pa.string()),
        ],
        root.parent / "second.parquet",
    )
    table_series = (
        b"dp.series.1\n"
        + struct.pack("<I", 2)
        + digest(first)
        + digest(second)
    )
    children = [
        ("dynamic", 5, digest(RECIPE), [timestamp_metadata(25)]),
        ("file", 4, digest(FILE_BYTES), [timestamp_metadata(50)]),
        ("link", 3, digest(SYMLINK_TARGET), []),
        (
            "table",
            8,
            digest(table_series),
            [metadata(100), metadata(200)],
        ),
    ]
    tree = (
        b"dp.tree.2\n"
        + struct.pack("<I", len(children))
        + b"".join(tree_entry(*entry) for entry in children)
    )
    entries = [
        ("dynamic", "root", "dynamic", 5, digest(RECIPE), [timestamp_metadata(25)]),
        (
            "file",
            "root",
            "file",
            4,
            digest(FILE_BYTES),
            [timestamp_metadata(50)],
        ),
        ("link", "root", "link", 3, digest(SYMLINK_TARGET), []),
        ("root", "", "", 1, digest(tree), []),
        (
            "table",
            "root",
            "table",
            8,
            digest(table_series),
            [metadata(100), metadata(200)],
        ),
    ]
    manifest = (
        b"dp.manifest.2\n"
        + struct.pack("<I", len(entries))
        + b"".join(manifest_entry(*entry) for entry in entries)
    )
    manifest_root = node_manifest_root(decode_manifest(manifest), blake3)
    commit = (
        b"dp.commit.3\n"
        + digest(tree)
        + b"\0"
        + digest(manifest)
        + manifest_root
        + lp(POND_ID.encode())
        + struct.pack("<qq", 2, NOW)
        + lp(b"legacy-integration")
        + lp(b"opaque migration fixture")
    )
    inline = {
        digest(first): first,
        digest(second): second,
        digest(table_series): table_series,
        digest(SYMLINK_TARGET): SYMLINK_TARGET,
        digest(RECIPE): RECIPE,
        digest(tree): tree,
        digest(manifest): manifest,
        digest(commit): commit,
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
    add(POND_ID, "refs", "main", 1, False, digest(commit))
    arrow = pa.table(
        {
            "pond_id": pa.array([row[0] for row in rows], type=pa.string()),
            "partition_key": pa.array(
                [row[1] for row in rows], type=pa.string()
            ),
            "item_key": pa.array([row[2] for row in rows], type=pa.string()),
            "txn_seq": pa.array([row[3] for row in rows], type=pa.int64()),
            "deleted": pa.array([row[4] for row in rows], type=pa.bool_()),
            "value": pa.array([row[5] for row in rows], type=pa.binary()),
            "value_blake3": pa.array(
                [row[6] for row in rows], type=pa.binary()
            ),
            "ts_micros": pa.array([row[7] for row in rows], type=pa.int64()),
        }
    )
    write_deltalake(root, arrow, partition_by=["pond_id", "partition_key"])
    blobs = root / "_blobs"
    blobs.mkdir()
    (blobs / f"blob={digest(FILE_BYTES).hex()}").write_bytes(FILE_BYTES)
    return first, second


def assert_extractor_has_no_pyarrow_imports(extractor: Path) -> None:
    tree = ast.parse(extractor.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""]
        else:
            continue
        if any(name == "pyarrow" or name.startswith("pyarrow.") for name in names):
            raise AssertionError(f"extractor imports PyArrow at line {node.lineno}")


def rewrite_mapping(capsule: Path) -> None:
    latest = (capsule / "recovery/refs/latest").read_text().strip()
    manifest_path = capsule / f"recovery/manifests/{latest}.json"
    manifest = json.loads(manifest_path.read_bytes())
    table = next(entry for entry in manifest["entries"] if entry["path"] == "/table")
    versions = table["node"]["versions"]
    versions.reverse()
    for index, version in enumerate(versions):
        version["source_version"] = index
    encoded = json.dumps(
        manifest, ensure_ascii=False, separators=(",", ":")
    ).encode()
    root = blake3.blake3(ROOT_DOMAIN + encoded).hexdigest()
    (capsule / f"recovery/manifests/{root}.json").write_bytes(encoded)
    (capsule / "recovery/refs/latest").write_text(root + "\n")


def main() -> int:
    kit = Path(__file__).resolve().parent
    extractor = kit / "extract.py"
    assert_extractor_has_no_pyarrow_imports(extractor)
    with tempfile.TemporaryDirectory(prefix="legacy-migration-test-") as temporary:
        workspace = Path(temporary)
        source = workspace / "native"
        first, second = build_native_backup(source)

        blocker = workspace / "block-pyarrow"
        blocker.mkdir()
        marker = workspace / "pyarrow-import-attempted"
        (blocker / "sitecustomize.py").write_text(
            "import builtins, os\n"
            "_original = builtins.__import__\n"
            "def _guard(name, *args, **kwargs):\n"
            "    if name == 'pyarrow' or name.startswith('pyarrow.'):\n"
            "        open(os.environ['PYARROW_IMPORT_MARKER'], 'a').close()\n"
            "        raise ImportError('PyArrow forbidden in source extractor')\n"
            "    return _original(name, *args, **kwargs)\n"
            "builtins.__import__ = _guard\n"
        )
        capsule = workspace / "capsule"
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(blocker)
        environment["PYARROW_IMPORT_MARKER"] = str(marker)
        subprocess.run(
            [
                sys.executable,
                str(extractor),
                str(source),
                str(capsule),
                "--ref",
                "main",
                "--birthplace",
                "legacy-integration",
            ],
            check=True,
            env=environment,
        )
        assert not marker.exists(), "source extractor attempted to import PyArrow"

        manifest, report = load_and_verify(capsule)
        assert report["physical_versions"] == 3
        table = next(entry for entry in manifest["entries"] if entry["path"] == "/table")
        hashes = [
            version["objects"][0]["hash"] for version in table["node"]["versions"]
        ]
        assert hashes == [digest(first).hex(), digest(second).hex()]
        assert (
            capsule / f"recovery/objects/blake3={hashes[0]}"
        ).read_bytes() == first
        assert (
            capsule / f"recovery/objects/blake3={hashes[1]}"
        ).read_bytes() == second
        recovered = workspace / "materialized"
        materialize(capsule, recovered)
        inventory = json.loads((recovered / "inventory.json").read_text())
        dynamic = next(
            entry for entry in inventory["entries"] if entry["path"] == "/dynamic"
        )
        assert dynamic["metadata"] == {"timestamp": 25}
        dynamic_root = (
            recovered / "dynamic-recipes" / dynamic["encoded_path"]
        )
        assert json.loads((dynamic_root / "factory.json").read_text()) == {
            "factory": "legacy-fixture"
        }
        assert (dynamic_root / "config.bin").read_bytes() == b"enabled: false\n"
        assert json.loads((dynamic_root / "metadata.json").read_text()) == {
            "timestamp": 25
        }
        symlink = next(
            entry for entry in inventory["entries"] if entry["path"] == "/link"
        )
        symlink_target = (
            recovered / "symlinks" / symlink["encoded_path"] / "target.bin"
        )
        assert symlink_target.read_bytes() == SYMLINK_TARGET
        assert not symlink_target.is_symlink()

        tampered = workspace / "tampered"
        shutil.copytree(capsule, tampered)
        (tampered / f"recovery/objects/blake3={hashes[0]}").write_bytes(
            b"tampered"
        )
        try:
            load_and_verify(tampered)
        except CapsuleError as error:
            assert "hash" in str(error) or "size" in str(error), error
        else:
            raise AssertionError("verifier accepted a tampered raw payload")

        mapped = workspace / "mapped"
        shutil.copytree(capsule, mapped)
        rewrite_mapping(mapped)
        try:
            load_and_verify(mapped)
        except CapsuleError as error:
            assert "mapping mismatch" in str(error), error
        else:
            raise AssertionError("verifier accepted a tampered leaf mapping")
    print("opaque legacy extraction preserved both evolving Parquet versions")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
