#!/usr/bin/env python3
"""Strict pondcapsule.legacy.2 verifier tests without Pond or PyArrow."""

from __future__ import annotations

import json
import shutil
import struct
import tempfile
from pathlib import Path

import blake3

from capsule import CapsuleError, ROOT_DOMAIN, load_and_verify


def obj(data: bytes) -> dict[str, object]:
    return {"hash": blake3.blake3(data).hexdigest(), "size": len(data)}


def write_capsule(root: Path, manifest: dict, payloads: dict[str, bytes]) -> str:
    encoded = json.dumps(
        manifest, ensure_ascii=False, separators=(",", ":")
    ).encode()
    capsule_root = blake3.blake3(ROOT_DOMAIN + encoded).hexdigest()
    (root / "recovery/refs").mkdir(parents=True)
    (root / "recovery/manifests").mkdir()
    (root / "recovery/objects").mkdir()
    (root / "recovery/refs/latest").write_text(capsule_root + "\n")
    (root / f"recovery/manifests/{capsule_root}.json").write_bytes(encoded)
    for digest, data in payloads.items():
        (root / f"recovery/objects/blake3={digest}").write_bytes(data)
    return capsule_root


def fixture() -> tuple[dict, dict[str, bytes]]:
    first = b"first"
    second = b"second"
    series = (
        b"dp.series.1\n"
        + struct.pack("<I", 2)
        + blake3.blake3(first).digest()
        + blake3.blake3(second).digest()
    )
    payloads = {
        blake3.blake3(first).hexdigest(): first,
        blake3.blake3(second).hexdigest(): second,
        blake3.blake3(series).hexdigest(): series,
    }
    manifest = {
        "format": "pondcapsule.legacy.2",
        "source": {
            "pond_id": "pond",
            "birthplace": "fixture",
            "source_tip": blake3.blake3(b"tip").hexdigest(),
            "exported_at_micros": 1,
            "tool_version": "test",
            "native_format": "dp.commit.3",
        },
        "entries": [
            {
                "path": "/",
                "entry_type": "dir:physical",
                "source_node_id": "root",
                "node": {"kind": "directory"},
            },
            {
                "path": "/series",
                "entry_type": "file:physical:series",
                "source_node_id": "series",
                "node": {
                    "kind": "physical",
                    "payload_kind": "file",
                    "source_child_hash": obj(series)["hash"],
                    "series_object": obj(series),
                    "versions": [
                        {
                            "source_version": 0,
                            "objects": [obj(first)],
                            "source_timestamp": 10,
                            "min_event_time": None,
                            "max_event_time": None,
                            "extended_attributes": None,
                        },
                        {
                            "source_version": 1,
                            "objects": [obj(second)],
                            "source_timestamp": 20,
                            "min_event_time": None,
                            "max_event_time": None,
                            "extended_attributes": None,
                        },
                    ],
                },
            },
        ],
    }
    return manifest, payloads


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="legacy-capsule-test-") as temporary:
        workspace = Path(temporary)
        manifest, payloads = fixture()
        good = workspace / "good"
        root = write_capsule(good, manifest, payloads)
        _, report = load_and_verify(good)
        assert report["root"] == root
        assert report["physical_versions"] == 2

        extra = workspace / "extra"
        shutil.copytree(good, extra)
        (extra / "recovery/objects/unexpected").write_bytes(b"extra")
        try:
            load_and_verify(extra)
        except CapsuleError as error:
            assert "closure" in str(error), error
        else:
            raise AssertionError("verifier accepted an extra raw object")

        mapped = workspace / "mapped"
        mapped_manifest, mapped_payloads = fixture()
        versions = mapped_manifest["entries"][1]["node"]["versions"]
        versions.reverse()
        for index, version in enumerate(versions):
            version["source_version"] = index
        write_capsule(mapped, mapped_manifest, mapped_payloads)
        try:
            load_and_verify(mapped)
        except CapsuleError as error:
            assert "mapping mismatch" in str(error), error
        else:
            raise AssertionError("verifier accepted a rewritten leaf mapping")
    print("opaque legacy capsule tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
