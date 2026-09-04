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
from extract import (
    FormatError,
    _leaf_hash,
    _merkle_root,
    _reconstruct_pack_leaves,
    _series_root,
    _pack_stream_node,
    canonical_batch_rows,
    canonical_schema,
    decode_commit,
    decode_manifest,
    decode_pack,
    decode_recipe,
    decode_series,
    decode_tree,
    extract,
    node_manifest_root,
)
from parquet_schema import read_parquet_schema

NIL_UUID = "00000000-0000-0000-0000-000000000000"
POND_ID = "11111111-1111-1111-1111-111111111111"
NOW = 1_700_000_000_000_000
EXTERNAL_FILE = (b"external recovery payload\n" * 4096) + b"end\n"
SYMLINK_TARGET = b"/data/file"
RECIPE_BYTES = (
    b"watertown.recipe.v1\n"
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


def verify_embedded_arrow_schema() -> None:
    logical = pa.schema(
        [pa.field("timestamp", pa.timestamp("s", tz="+00:00"), nullable=False)]
    )
    physical = pa.schema([pa.field("timestamp", pa.int64(), nullable=False)])
    embedded = __import__("base64").b64encode(logical.serialize().to_pybytes())

    class Metadata:
        metadata = {b"ARROW:schema": embedded}

    class Parquet:
        schema_arrow = physical
        metadata = Metadata()

    resolved = read_parquet_schema(Parquet(), pa, FormatError)
    assert resolved == logical
    assert canonical_schema(resolved, pa) == canonical_schema(logical, pa)
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2], type=pa.int64())], schema=physical
    )
    assert canonical_batch_rows(resolved, batch, pa) == canonical_batch_rows(
        logical,
        pa.RecordBatch.from_arrays(
            [pa.array([1, 2], type=pa.timestamp("s", tz="+00:00"))],
            schema=logical,
        ),
        pa,
    )


def verify_uniform_node_schema() -> None:
    with tempfile.TemporaryDirectory(prefix="watertown-uniform-pack-") as temporary:
        root = Path(temporary)
        schema = pa.schema([pa.field("reading", pa.int64(), nullable=False)])
        table = pa.Table.from_arrays([pa.array([1, 2], type=pa.int64())], schema=schema)
        physical_path = root / "uniform.parquet"
        pq.write_table(table, physical_path)
        physical = physical_path.read_bytes()
        schema_hash = digest(canonical_schema(schema, pa))
        rows = b"".join(
            canonical_batch_rows(schema, batch, pa) for batch in table.to_batches()
        )
        payload = b"watertown.series-rows.v1\n" + struct.pack("<Q", table.num_rows) + rows
        leaf = _leaf_hash(
            0,
            schema_hash,
            table.num_rows,
            iter([payload]),
            len(payload),
            {
                "min_event_time": 1,
                "max_event_time": 2,
                "extended_attributes": None,
                "timestamp": None,
            },
            '{"uniform":true}',
            blake3,
        )
        series_hash = digest(b"uniform-v2-series")
        series = {
            "kind": "table",
            "logical_count": table.num_rows,
            "leaf_count": 1,
            "min_event_time": 1,
            "max_event_time": 2,
            "logical_attributes": '{"series":"uniform"}',
            "leaf_merkle_root": _merkle_root([leaf], blake3),
        }
        descriptor = _descriptor(table.num_rows, schema_hash, 1, 2, '{"uniform":true}')
        pack = _pack(
            series_hash,
            [leaf],
            0,
            1,
            [physical],
            [descriptor],
        )

        class Backup:
            def pack_indexes(self, requested_hash: bytes) -> list[tuple[bytes, bytes]]:
                assert requested_hash == series_hash
                return [(digest(pack), pack)]

            def object_path(self, requested_hash: bytes) -> Path:
                assert requested_hash == digest(physical)
                return physical_path

        objects = root / "objects"
        objects.mkdir()
        node = _pack_stream_node(
            Backup(),
            objects,
            {
                "entry_type": "table:physical:series",
                "versions": [{"timestamp": 101}],
            },
            "/uniform.series",
            series_hash,
            series,
            pa,
            pq,
            blake3,
        )
        assert node["schema_fingerprint"] == schema_hash.hex()
        assert node["logical_root"] == _series_root(
            "table",
            schema_hash,
            node["leaves"],
            blake3,
        )


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


def _proof_bytes(leaves: list[str], start: int, end: int) -> bytes:
    nodes: list[tuple[int, int, bytes]] = []

    def visit(offset: int, values: list[str]) -> None:
        node_end = offset + len(values)
        if node_end <= start or offset >= end:
            nodes.append((offset, len(values), _merkle_root(values, blake3)))
            return
        if offset >= start and node_end <= end:
            return
        split = 1 << ((len(values) - 1).bit_length() - 1)
        visit(offset, values[:split])
        visit(offset + split, values[split:])

    visit(0, leaves)
    return (
        b"watertown.series-range-proof.v1\n"
        + struct.pack("<I", len(nodes))
        + b"".join(
            struct.pack("<QQ", offset, count) + value
            for offset, count, value in nodes
        )
    )


def _descriptor(
    count: int, schema: bytes, minimum: int, maximum: int, attributes: str
) -> bytes:
    encoded = attributes.encode()
    return (
        struct.pack("<Q", count)
        + length_prefixed(schema)
        + b"\x03"
        + struct.pack("<qq", minimum, maximum)
        + length_prefixed(encoded)
    )


def _pack(
    series_hash: bytes,
    leaves: list[str],
    start: int,
    end: int,
    physical: list[bytes],
    descriptors: list[bytes],
) -> bytes:
    proof = _proof_bytes(leaves, start, end)
    return (
        b"watertown.series-pack.v2\n"
        + series_hash
        + struct.pack("<QQQ", start, end, len(leaves))
        + _merkle_root(leaves, blake3)
        + length_prefixed(proof)
        + struct.pack("<I", len(physical))
        + b"".join(digest(value) for value in physical)
        + struct.pack("<Q", sum(struct.unpack_from("<Q", value)[0] for value in descriptors))
        + struct.pack("<Q", sum(len(value) for value in physical))
        + struct.pack("<I", len(descriptors))
        + b"".join(descriptors)
    )


def build_current_pack_backup(root: Path) -> None:
    verify_embedded_arrow_schema()
    verify_uniform_node_schema()
    schema_one = pa.schema(
        [
            pa.field("observed_at", pa.timestamp("s", tz="+00:00"), False),
            pa.field("reading", pa.int64(), True),
            pa.field("label", pa.string(), False),
        ],
        metadata={b"origin": b"current-pack-fixture"},
    )
    schema_two = pa.schema(
        [
            pa.field("observed_at", pa.timestamp("s", tz="+00:00"), False),
            pa.field("reading", pa.int64(), True),
            pa.field("label", pa.string(), False),
            pa.field("quality", pa.string(), True),
        ],
        metadata={b"origin": b"current-pack-fixture"},
    )
    logical_tables = [
        pa.Table.from_arrays(
            [
                pa.array([1, 2], type=pa.timestamp("s", tz="+00:00")),
                pa.array([1, None], type=pa.int64()),
                pa.array(["a", "b"]),
            ],
            schema=schema_one,
        ),
        pa.Table.from_arrays(
            [
                pa.array([3, 4], type=pa.timestamp("s", tz="+00:00")),
                pa.array([3, 4], type=pa.int64()),
                pa.array(["c", "d"]),
                pa.array(["good", None]),
            ],
            schema=schema_two,
        ),
        pa.Table.from_arrays(
            [
                pa.array([5], type=pa.timestamp("s", tz="+00:00")),
                pa.array([5], type=pa.int64()),
                pa.array(["e"]),
                pa.array(["suspect"]),
            ],
            schema=schema_two,
        ),
    ]

    def parquet_bytes(name: str, table: pa.Table) -> bytes:
        path = root.parent / name
        pq.write_table(table, path)
        data = path.read_bytes()
        path.unlink()
        return data

    physical_one = parquet_bytes("pack-one.parquet", logical_tables[0])
    physical_two = parquet_bytes(
        "pack-two.parquet", pa.concat_tables(logical_tables[1:])
    )
    schema_hashes = [
        digest(canonical_schema(table.schema, pa)) for table in logical_tables
    ]
    metadata_values = [
        (10, 20, '{"leaf":0}'),
        (21, 30, '{"leaf":1}'),
        (31, 40, '{"leaf":2}'),
    ]
    leaves = []
    descriptors = []
    for table, schema_hash, (minimum, maximum, attributes) in zip(
        logical_tables, schema_hashes, metadata_values
    ):
        rows = b"".join(
            canonical_batch_rows(table.schema, batch, pa)
            for batch in table.to_batches()
        )
        count = table.num_rows
        payload = b"watertown.series-rows.v1\n" + struct.pack("<Q", count) + rows
        leaves.append(
            _leaf_hash(
                0,
                schema_hash,
                count,
                iter([payload]),
                len(payload),
                {
                    "min_event_time": minimum,
                    "max_event_time": maximum,
                    "extended_attributes": None,
                    "timestamp": None,
                },
                attributes,
                blake3,
            )
        )
        descriptors.append(_descriptor(count, schema_hash, minimum, maximum, attributes))

    series = (
        b"watertown.series.v2\n"
        + b"\0"
        + struct.pack("<QQ", sum(table.num_rows for table in logical_tables), len(leaves))
        + b"\x03"
        + struct.pack("<qq", 10, 40)
        + length_prefixed(b'{"series":"current"}')
        + _merkle_root(leaves, blake3)
    )
    series_hash = digest(series)
    packs = [
        _pack(series_hash, leaves, 0, 1, [physical_one], descriptors[:1]),
        _pack(series_hash, leaves, 1, 3, [physical_two], descriptors[1:]),
    ]
    for pack in packs:
        assert decode_pack(pack)["series_hash"] == series_hash
        try:
            decode_pack(pack + b"\0")
        except FormatError:
            pass
        else:
            raise AssertionError("pack decoder accepted trailing bytes")
    tree = (
        b"watertown.tree.v1\n"
        + struct.pack("<I", 2)
        + tree_entry("dynamic", 5, digest(RECIPE_BYTES), [metadata(104)])
        + tree_entry("table", 8, series_hash, [metadata(103)])
    )
    tree_hash = digest(tree)
    entries = [
        ("dynamic", "root", "dynamic", 5, digest(RECIPE_BYTES), [metadata(104)]),
        ("root", "", "", 1, tree_hash, []),
        ("table", "root", "table", 8, series_hash, [metadata(103)]),
    ]
    manifest = (
        b"watertown.manifest.v1\n"
        + struct.pack("<I", len(entries))
        + b"".join(manifest_entry(*entry) for entry in entries)
    )
    manifest_hash = digest(manifest)
    manifest_root = node_manifest_root(decode_manifest(manifest), blake3)
    commit = (
        b"watertown.commit.v1\n"
        + b"\x01"
        + tree_hash
        + b"\0"
        + manifest_hash
        + manifest_root
        + length_prefixed(POND_ID.encode())
        + struct.pack("<qq", 2, NOW)
        + length_prefixed(b"integration-test")
        + length_prefixed(b"current pack recovery fixture")
    )
    commit_hash = digest(commit)
    rows: list[tuple[str, str, str, int, bool, bytes, bytes, int]] = []

    def add(partition: str, key: str, value: bytes) -> None:
        rows.append((POND_ID, partition, key, 1, False, value, digest(value), NOW))

    rows.append(
        (NIL_UUID, "meta", "pond_id", 1, False, POND_ID.encode(), digest(POND_ID.encode()), NOW)
    )
    for value in (
        physical_one,
        physical_two,
        RECIPE_BYTES,
        series,
        tree,
        manifest,
        commit,
    ):
        add("objects", digest(value).hex(), value)
    add("refs", "main", commit_hash)
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
    directory = root / "_packs" / f"series={series_hash.hex()}"
    directory.mkdir(parents=True)
    for pack in packs:
        (directory / f"pack={digest(pack).hex()}").write_bytes(pack)


def verify_obsolete_formats_rejected() -> None:
    obsolete = [
        (decode_commit, b"dp.commit.3\n"),
        (decode_manifest, b"dp.manifest.2\n"),
        (decode_tree, b"dp.tree.2\n"),
        (decode_series, b"dp.series.1\n"),
        (decode_series, b"watertown.series.v1\n"),
        (decode_pack, b"watertown.series-pack.v1\n"),
        (decode_recipe, b"dp.recipe.1\n"),
    ]
    for decoder, value in obsolete:
        try:
            decoder(value)
        except FormatError:
            pass
        else:
            raise AssertionError(f"{decoder.__name__} accepted obsolete format {value!r}")


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
    verify_obsolete_formats_rejected()

    with tempfile.TemporaryDirectory(prefix="watertown-recipe-test-") as temporary:
        workspace = Path(temporary)
        source = workspace / "native"
        destination = workspace / "capsule"
        build_current_pack_backup(source)
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
        verified_manifest, report = load_and_verify(destination)
        assert report["root"] == root
        assert verified_manifest["format"] == "pondcapsule.4"
        dynamic_node = next(
            entry["node"]
            for entry in verified_manifest["entries"]
            if entry["path"] == "/dynamic"
        )
        assert dynamic_node["metadata"] == {"timestamp": 104}
        table_node = next(
            entry["node"]
            for entry in verified_manifest["entries"]
            if entry["path"] == "/table"
        )
        assert "schema_fingerprint" not in table_node
        assert len({leaf["schema_fingerprint"] for leaf in table_node["leaves"]}) == 2

        recovered = workspace / "materialized"
        materialize(destination, recovered)
        table_path = _encoded_logical_path("/table")
        recovered_table = pq.read_table(
            recovered / "tables" / table_path / "version-000001.parquet"
        )
        assert recovered_table.column("reading").to_pylist() == [1, None]
        assert recovered_table.column("label").to_pylist() == ["a", "b"]
        second_table = pq.read_table(
            recovered / "tables" / table_path / "version-000002.parquet"
        )
        assert second_table.column("quality").to_pylist() == ["good", None]
        third_table = pq.read_table(
            recovered / "tables" / table_path / "version-000003.parquet"
        )
        assert third_table.column("reading").to_pylist() == [5]
        assert third_table.column("quality").to_pylist() == ["suspect"]
        wrapper_recovered = workspace / "wrapper-materialized"
        # pyarrow 21 publishes wheels through Python 3.13; avoid attempting a
        # source build on newer interpreters in this wrapper smoke test.
        if (3, 13) <= sys.version_info < (3, 14):
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
                wrapper_recovered / "tables" / table_path / "version-000003.parquet"
            ).is_file()
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

        corrupt_source = workspace / "native-corrupt-proof"
        build_current_pack_backup(corrupt_source)
        pack_directory = next((corrupt_source / "_packs").iterdir())
        for pack_path in pack_directory.iterdir():
            pack = decode_pack(pack_path.read_bytes())
            if pack["leaf_start"] != 0:
                continue
            corrupted = bytearray(pack_path.read_bytes())
            root_offset = len(b"watertown.series-pack.v2\n") + 32 + 8 * 3
            corrupted[root_offset] ^= 1
            pack_path.unlink()
            (pack_directory / f"pack={digest(bytes(corrupted)).hex()}").write_bytes(corrupted)
            break
        else:
            raise AssertionError("fixture has no first pack to corrupt")
        try:
            extract(
                corrupt_source,
                workspace / "corrupt-proof-capsule",
                "main",
                None,
                "fixture",
            )
        except FormatError as error:
            assert "range proof" in str(error), error
        else:
            raise AssertionError("extractor accepted a corrupt pack range proof")

        # A native reader validates a selected pack's descriptor range before
        # moving to its successor.  The joined byte stream below is valid, but
        # pack zero is a tampered fragment and must not borrow ``cd`` from
        # pack one to finish its advertised leaf.
        fragment_dir = workspace / "cross-pack-fragment"
        fragment_dir.mkdir()
        first_fragment = fragment_dir / "pack-zero"
        successor = fragment_dir / "pack-one"
        first_fragment.write_bytes(b"ab")
        successor.write_bytes(b"cdefgh")
        fragment_descriptor = {
            "logical_count": 4,
            "min_event_time": None,
            "max_event_time": None,
            "logical_attributes": None,
        }
        assert len(
            _reconstruct_pack_leaves(
                [first_fragment, successor],
                [fragment_descriptor, fragment_descriptor],
                {"kind": "file"},
                {},
                "/fragment",
                0,
                None,
                None,
                blake3,
            )
        ) == 2
        try:
            _reconstruct_pack_leaves(
                [first_fragment],
                [fragment_descriptor],
                {"kind": "file"},
                {},
                "/fragment",
                0,
                None,
                None,
                blake3,
            )
        except FormatError as error:
            assert "ends during leaf" in str(error), error
        else:
            raise AssertionError("pack-local validation accepted a successor fragment")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
