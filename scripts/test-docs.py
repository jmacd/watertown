#!/usr/bin/env python3
"""Focused regression checks for the documentation drift gates."""

import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / filename)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


checker = module("check_docs", "check-docs.py")
generator = module("generate_docs", "generate-docs.py")


class DriftTests(unittest.TestCase):
    def setUp(self):
        self.catalog = json.loads((ROOT / "docs/documentation-inventory.json").read_text())
        self.metadata = json.loads((ROOT / "docs/manual/_generated/inventory.json").read_text())

    def test_new_compiled_interface_is_not_silently_baselined(self):
        self.metadata["factories"]["factory:new-public-component"] = {}
        self.metadata["schemas"]["factory:new-public-component"] = {}
        with self.assertRaisesRegex(ValueError, "New compiled interfaces"):
            checker.validate(self.catalog, self.metadata)

    def test_completed_mapping_cannot_regress(self):
        page = next(value for value in self.catalog["pages"].values() if value["components"])
        page["components"] = []
        with self.assertRaisesRegex(ValueError, "regressed"):
            checker.validate(self.catalog, self.metadata)

    def test_removed_interface_fails(self):
        del self.metadata["factories"]["factory:synthetic-timeseries"]
        with self.assertRaisesRegex(ValueError, "removed compiled"):
            checker.validate(self.catalog, self.metadata)

    def test_mechanical_reference_tracks_flag_defaults(self):
        changed = copy.deepcopy(self.metadata)
        argument = next(arg for arg in changed["cli"]["cli:maintain"]["arguments"] if arg["long"] == "collapse-versions")
        argument["defaults"] = ["123"]
        self.assertNotEqual(generator.reference("cli:maintain", self.metadata),
                            generator.reference("cli:maintain", changed))

    def test_baseline_cannot_absorb_new_public_interfaces(self):
        previous = copy.deepcopy(self.catalog)
        self.metadata["factories"]["factory:new-public-component"] = {}
        self.catalog["public"]["factory"]["ids"].append("factory:new-public-component")
        self.catalog["pending"].append("factory:new-public-component")
        with self.assertRaisesRegex(ValueError, "cannot be grandfathered"):
            checker.validate(self.catalog, self.metadata, previous=previous)

    def test_exporter_is_deterministic_without_a_pond_or_service(self):
        with tempfile.TemporaryDirectory() as directory:
            env = dict(os.environ, POND=str(Path(directory) / "must-not-exist"),
                       HTTP_PROXY="http://127.0.0.1:1", HTTPS_PROXY="http://127.0.0.1:1")
            executable = os.environ.get("DOCS_EXPORT", str(ROOT / "target/debug/docs-export"))
            first = subprocess.check_output([executable], cwd=directory, env=env)
            second = subprocess.check_output([executable], cwd=directory, env=env)
            self.assertEqual(first, second)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_field_reference_preserves_conditional_and_open_shapes(self):
        rendered = generator.fields({
            "properties": {"config": True, "count": {"type": "integer", "minimum": 1}},
            "allOf": [{"properties": {"type": {"const": "example"}}, "required": ["type"]}],
        }, "Nested configuration")
        self.assertIn("any JSON value", rendered)
        self.assertIn("minimum=1", rendered)
        self.assertIn("variant 1", rendered)
        self.assertIn("example", rendered)


if __name__ == "__main__":
    unittest.main()
