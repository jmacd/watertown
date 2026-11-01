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

    def test_formats_cannot_regress_to_a_grouped_authority_page(self):
        self.catalog["pages"]["format:csv"]["components"].append("format:hledger")
        self.catalog["pages"]["format:hledger"]["components"] = []
        with self.assertRaisesRegex(ValueError, "individual authoritative pages"):
            checker.validate(self.catalog, self.metadata)

    def test_factory_requires_its_yaml_file_and_source_include(self):
        component = "factory:git-ingest"
        page = self.catalog["pages"][component]
        self.catalog["pages"] = {component: page}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / page["path"]
            target.parent.mkdir(parents=True)
            text = (ROOT / page["path"]).read_text()
            target.write_text(text)
            target.with_name("mod.rs").write_text("")
            with self.assertRaisesRegex(ValueError, "missing .*default.yaml"):
                checker.validate(self.catalog, self.metadata, root=root)
            example = target.with_name("default.yaml")
            example.write_text((ROOT / example.relative_to(root)).read_text())
            target.write_text(text.replace(
                generator.EXAMPLE_INCLUDE,
                "```yaml\nurl: duplicated-example\n```"))
            with self.assertRaisesRegex(ValueError, "must include its authoritative default.yaml"):
                checker.validate(self.catalog, self.metadata, root=root)
            target.write_text(text)
            duplicate = root / page["output"]
            duplicate.parent.mkdir(parents=True)
            duplicate.write_text(text)
            with self.assertRaisesRegex(ValueError, "generated page must not also exist"):
                checker.validate(self.catalog, self.metadata, root=root)

    def test_component_pages_are_assembled_without_changing_authored_sources(self):
        page = self.catalog["pages"]["factory:git-ingest"]
        source = ROOT / page["path"]
        original = source.read_bytes()
        example = source.with_name("default.yaml").read_bytes()
        with tempfile.TemporaryDirectory() as directory:
            staged = Path(directory) / "input"
            generator.stage_manual(ROOT, staged)
            rendered = (staged / page["output"]).read_text()
            self.assertIn("## Overview", rendered)
            self.assertIn("## Full configuration", rendered)
            self.assertIn("| `prefix` |", rendered)
            self.assertIn('/docs/examples/git-ingest/default.yaml', rendered)
            self.assertEqual((staged / "docs/examples/git-ingest/default.yaml").read_bytes(), example)
            self.assertFalse((staged / "crates").exists())
            for component, entry in self.catalog["pages"].items():
                if component.startswith(("factory:", "format:")):
                    self.assertTrue((staged / entry["output"]).is_file(), component)
                    self.assertFalse((ROOT / entry["output"]).exists(), component)
            self.assertEqual(source.read_bytes(), original)
            self.assertEqual(source.with_name("default.yaml").read_bytes(), example)

    def test_component_generation_rejects_unsafe_paths(self):
        page = self.catalog["pages"]["factory:git-ingest"]
        page["output"] = "../escaped.md"
        with self.assertRaisesRegex(ValueError, "expected published path"):
            generator.component_outputs(self.catalog, self.metadata)
        page["output"] = "docs/manual/reference/factories/git-ingest.md"
        page["path"] = "../documentation.md"
        with self.assertRaisesRegex(ValueError, "input-tree relative path"):
            generator.component_outputs(self.catalog, self.metadata)

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
        self.assertIn('#### type: "example"', rendered)
        self.assertIn("requirements also apply", rendered)
        self.assertIn("example", rendered)

    def test_configuration_reference_uses_reader_facing_names(self):
        rendered = generator.reference("factory:column-rename", self.metadata)
        self.assertIn("#### Direct column name mapping", rendered)
        self.assertIn("#### Regex pattern-based renaming", rendered)
        self.assertIn("array of Rename Rule", rendered)
        self.assertIn('| `type` | "pattern"', rendered)
        self.assertIn("| `replacement` |", rendered)
        for internal in ("variant 1", "variant 2", "ColumnRenameConfig",
                         "Unknown fields:", "Capability", "post-commit",
                         "deserialization", "Example: yaml"):
            self.assertNotIn(internal, rendered)

    def test_reference_alternatives_do_not_duplicate_their_definitions(self):
        rendered = generator.reference("factory:monitor-report", self.metadata)
        self.assertIn("- Rate While Check", rendered)
        self.assertEqual(rendered.count("### Rate While Check\n"), 1)
        self.assertNotIn("variant ", rendered)


if __name__ == "__main__":
    unittest.main()
