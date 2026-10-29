#!/usr/bin/env python3
"""Check the D01 pilot mappings, not the D06 compiled inventory/freshness gate."""

import json
from pathlib import Path

root = Path(__file__).resolve().parent.parent


def require(condition, message):
    if not condition:
        raise SystemExit(message)


catalog = json.loads((root / "docs/documentation-inventory.json").read_text())
public = [item for group in catalog["public"].values() for item in group["ids"]]
require(len(public) == len(set(public)), "duplicate public component ID")
excluded = {item["id"] for item in catalog["exclusions"]}
require(not excluded.intersection(public), "public component also excluded")
completed = set()
for page_id, page in catalog["pages"].items():
    text = (root / page["path"]).read_text()
    frontmatter = text.split("---", 2)[1]
    require(f"\ndoc_id: {page_id}\n" in frontmatter, f"{page['path']}: missing doc_id {page_id}")
    require(page["coverage"] in catalog["metadata_contract"]["coverage_levels"], page_id)
    for component in page["components"]:
        require(component in public, f"{page_id}: unknown component {component}")
        require(component not in completed, f"{component}: duplicate authoritative mapping")
        completed.add(component)
    for example_id in page["examples"]:
        require(example_id in catalog["examples"], f"{page_id}: unknown example {example_id}")
for example_id, example in catalog["examples"].items():
    require((root / example["directory"] / "run.sh").is_file(), f"{example_id}: missing run.sh")
    require((root / example["test"]).is_file(), f"{example_id}: missing numbered test")
    test = (root / example["test"]).read_text()
    require("check_finish" in test, f"{example_id}: missing assertions")
    require(example["coverage"] in catalog["metadata_contract"]["coverage_levels"], example_id)
for exclusion in catalog["exclusions"]:
    require(exclusion["reason"] and (root / exclusion["source"]).exists(), exclusion["id"])
print(f"Pilot mappings valid: {len(catalog['pages'])} pages, {len(completed)} completed components.")
print(f"{len(public) - len(completed)} public entries remain pending; compiled discovery/freshness starts at D06.")
