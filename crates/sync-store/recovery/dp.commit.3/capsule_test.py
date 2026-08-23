#!/usr/bin/env python3
"""Direct malformed-capsule tests for the standalone verifier."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import blake3

from capsule import (
    CapsuleError,
    ROOT_DOMAIN,
    _canonical_schema,
    _rename_no_replace,
    _series_root,
    _table_batches,
    load_and_verify,
    materialize,
)


def write_capsule(root: Path, manifest: dict) -> None:
    encoded = json.dumps(manifest, separators=(",", ":")).encode()
    digest = blake3.blake3(ROOT_DOMAIN + encoded).hexdigest()
    (root / "recovery" / "refs").mkdir(parents=True)
    (root / "recovery" / "manifests").mkdir()
    (root / "recovery" / "objects").mkdir()
    (root / "recovery" / "refs" / "latest").write_text(digest + "\n")
    (root / "recovery" / "manifests" / f"{digest}.json").write_bytes(encoded)


def minimal_manifest() -> dict:
    return {
        "format": "pondcapsule.1",
        "source": {
            "pond_id": "pond-test",
            "birthplace": "fixture",
            "source_tip": "00" * 32,
            "exported_at_micros": 1,
            "tool_version": "fixture",
        },
        "entries": [
            {
                "path": "/",
                "entry_type": "dir:physical",
                "source_node_id": "root",
                "node": {"kind": "directory"},
            }
        ],
    }


class CapsuleTest(unittest.TestCase):
    def test_minimal_capsule_and_destination_refusal(self) -> None:
        with tempfile.TemporaryDirectory(prefix="capsule-unit-") as temporary:
            root = Path(temporary) / "capsule"
            write_capsule(root, minimal_manifest())
            load_and_verify(root)
            destination = Path(temporary) / "output"
            destination.mkdir()
            with self.assertRaises(CapsuleError):
                materialize(root, destination)

    def test_unknown_field_fails(self) -> None:
        with tempfile.TemporaryDirectory(prefix="capsule-unit-") as temporary:
            root = Path(temporary) / "capsule"
            manifest = minimal_manifest()
            manifest["unknown"] = True
            write_capsule(root, manifest)
            with self.assertRaises(CapsuleError):
                load_and_verify(root)

    def test_empty_file_materializes_as_an_empty_version(self) -> None:
        with tempfile.TemporaryDirectory(prefix="capsule-unit-") as temporary:
            root = Path(temporary) / "capsule"
            manifest = minimal_manifest()
            manifest["entries"].append(
                {
                    "path": "/empty",
                    "entry_type": "file:physical:version",
                    "source_node_id": "empty",
                    "node": {
                        "kind": "physical",
                        "payload_kind": "file",
                        "schema_fingerprint": None,
                        "logical_root": _series_root("file", None, [], blake3),
                        "objects": [],
                        "leaves": [],
                    },
                }
            )
            write_capsule(root, manifest)
            destination = Path(temporary) / "output"
            materialize(root, destination)
            recovered = destination / "files" / "empty" / "version-000001.bin"
            self.assertTrue(recovered.is_file())
            self.assertEqual(recovered.read_bytes(), b"")

    def test_noncanonical_path_fails(self) -> None:
        with tempfile.TemporaryDirectory(prefix="capsule-unit-") as temporary:
            root = Path(temporary) / "capsule"
            manifest = minimal_manifest()
            manifest["entries"].append(
                {
                    "path": "/../escape",
                    "entry_type": "dir:physical",
                    "source_node_id": "escape",
                    "node": {"kind": "directory"},
                }
            )
            write_capsule(root, manifest)
            with self.assertRaises(CapsuleError):
                load_and_verify(root)

    def test_unicode_control_path_fails(self) -> None:
        with tempfile.TemporaryDirectory(prefix="capsule-unit-") as temporary:
            root = Path(temporary) / "capsule"
            manifest = minimal_manifest()
            manifest["entries"].append(
                {
                    "path": "/\u0085",
                    "entry_type": "dir:physical",
                    "source_node_id": "control",
                    "node": {"kind": "directory"},
                }
            )
            write_capsule(root, manifest)
            with self.assertRaises(CapsuleError):
                load_and_verify(root)

    def test_noncanonical_latest_fails(self) -> None:
        with tempfile.TemporaryDirectory(prefix="capsule-unit-") as temporary:
            root = Path(temporary) / "capsule"
            write_capsule(root, minimal_manifest())
            latest = root / "recovery" / "refs" / "latest"
            latest.write_text(latest.read_text().strip())
            with self.assertRaises(CapsuleError):
                load_and_verify(root)

    def test_reordered_manifest_fields_fail(self) -> None:
        with tempfile.TemporaryDirectory(prefix="capsule-unit-") as temporary:
            root = Path(temporary) / "capsule"
            canonical = minimal_manifest()
            reordered = {
                "entries": canonical["entries"],
                "format": canonical["format"],
                "source": canonical["source"],
            }
            write_capsule(root, reordered)
            with self.assertRaises(CapsuleError):
                load_and_verify(root)

    def test_table_batches_normalize_dictionary_encoding(self) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory(prefix="capsule-unit-") as temporary:
            root = Path(temporary) / "capsule"
            objects = root / "recovery" / "objects"
            objects.mkdir(parents=True)
            plain_hash = "11" * 32
            dictionary_hash = "22" * 32
            plain = pa.table({"value": pa.array(["a", "b"])})
            dictionary = pa.table(
                {"value": pa.array(["c", "d"]).dictionary_encode()}
            )
            pq.write_table(plain, objects / f"blake3={plain_hash}")
            pq.write_table(dictionary, objects / f"blake3={dictionary_hash}")
            fingerprint = blake3.blake3(
                _canonical_schema(plain.schema, pa)
            ).hexdigest()
            entry = {
                "path": "/table",
                "node": {
                    "schema_fingerprint": fingerprint,
                    "objects": [
                        {"hash": plain_hash, "size": 0},
                        {"hash": dictionary_hash, "size": 0},
                    ],
                },
            }
            batches = list(_table_batches(root, entry, pa, pq))
            self.assertEqual(
                [batch.column(0).to_pylist() for _, batch in batches],
                [["a", "b"], ["c", "d"]],
            )
            self.assertTrue(
                all(pa.types.is_string(schema.field("value").type) for schema, _ in batches)
            )

    def test_no_replace_promotion_refuses_a_racing_destination(self) -> None:
        with tempfile.TemporaryDirectory(prefix="capsule-unit-") as temporary:
            root = Path(temporary)
            source = root / "source"
            destination = root / "destination"
            source.mkdir()
            destination.mkdir()
            (source / "source").write_text("source")
            (destination / "destination").write_text("destination")
            with self.assertRaises(CapsuleError):
                _rename_no_replace(source, destination)
            self.assertEqual((source / "source").read_text(), "source")
            self.assertEqual(
                (destination / "destination").read_text(), "destination"
            )

    def test_unexpected_object_fails_closure(self) -> None:
        with tempfile.TemporaryDirectory(prefix="capsule-unit-") as temporary:
            root = Path(temporary) / "capsule"
            write_capsule(root, minimal_manifest())
            (root / "recovery" / "objects" / f"blake3={'00' * 32}").write_bytes(b"")
            with self.assertRaises(CapsuleError):
                load_and_verify(root)


if __name__ == "__main__":
    unittest.main()
