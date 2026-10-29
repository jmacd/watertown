#!/usr/bin/env python3
"""Check compiled discovery, migration baselines, and authoritative page/example mappings."""

import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent.parent


def require(condition, message):
    if not condition:
        raise ValueError(message)


def validate(catalog, metadata, root=ROOT, previous=None):
    public = [item for group in catalog["public"].values() for item in group["ids"]]
    require(len(public) == len(set(public)), "duplicate public component ID")
    excluded = {item["id"] for item in catalog["exclusions"]}
    require(not excluded.intersection(public), "public component also excluded")
    compiled = (set(metadata["cli"]) | set(metadata["factories"]) | set(metadata["formats"])
                | set(metadata["schemes"]) | set(metadata["shortcodes"]))
    require(not compiled - set(public) - excluded,
            f"New compiled interfaces need authoritative documentation or an explicit internal exclusion: {sorted(compiled - set(public) - excluded)}")
    for namespace in ("cli:", "factory:", "format:", "shortcode:", "compression:", "scheme:"):
        missing = {item for item in public if item.startswith(namespace)} - compiled - set(catalog["explicit_interfaces"])
        require(not missing, f"Catalog names removed compiled interfaces: {sorted(missing)}")
    completed = set()
    for page_id, page in catalog["pages"].items():
        path = root / page["path"]
        require(path.is_file(), f"{page_id}: missing page {page['path']}")
        text = path.read_text()
        require(text.startswith("---\n") and len(text.split("---", 2)) == 3,
                f"{page['path']}: missing frontmatter")
        frontmatter = text.split("---", 2)[1]
        require(f"\ndoc_id: {page_id}\n" in frontmatter, f"{page['path']}: missing doc_id {page_id}")
        require(page["coverage"] in catalog["metadata_contract"]["coverage_levels"], page_id)
        for component in page["components"]:
            require(component in public, f"{page_id}: unknown component {component}")
            require(component not in completed, f"{component}: duplicate authoritative mapping")
            completed.add(component)
            if component in metadata.get("schemas", {}):
                require(f"<!-- docs:reference {component} -->" in text,
                        f"{page_id}: missing generated field reference for {component}")
            if component.startswith("cli:"):
                require(f"<!-- docs:reference {component} -->" in text
                        or "<!-- docs:reference cli:commands -->" in text,
                        f"{page_id}: missing compiled command reference for {component}")
        for example_id in page["examples"]:
            require(example_id in catalog["examples"], f"{page_id}: unknown example {example_id}")
    pending = set(catalog["pending"])
    require(not completed.intersection(pending), f"Completed components must leave the baseline: {sorted(completed.intersection(pending))}")
    require(pending <= set(public), "Baseline contains an unknown public component")
    require(set(public) - completed == pending,
            f"Completed documentation regressed or baseline is invalid: {sorted((set(public) - completed) ^ pending)}")
    if previous is not None and "pending" in previous:
        require(pending <= set(previous["pending"]),
                f"New interfaces cannot be grandfathered into the migration baseline: {sorted(pending - set(previous['pending']))}")
        old_completed = {component for page in previous["pages"].values() for component in page["components"]}
        require((old_completed & set(public)) <= completed, "Previously completed mappings regressed")
    for component in public:
        if component.startswith("factory:"):
            require(component in metadata.get("schemas", {}),
                    f"{component}: public factory needs a typed deserialization schema")
    for example_id, example in catalog["examples"].items():
        require((root / example["directory"] / "run.sh").is_file(), f"{example_id}: missing run.sh")
        require((root / example["test"]).is_file(), f"{example_id}: missing numbered test")
        test = (root / example["test"]).read_text()
        require("check_finish" in test, f"{example_id}: missing assertions")
        require(example["coverage"] in catalog["metadata_contract"]["coverage_levels"], example_id)
    for exclusion in catalog["exclusions"]:
        require(exclusion["reason"] and (root / exclusion["source"]).exists(), exclusion["id"])
        require(exclusion["id"] in compiled or exclusion["id"] == "cli:control",
                f"Stale exclusion: {exclusion['id']}")
    return len(catalog["pages"]), len(completed), len(pending)


def main():
    catalog = json.loads((ROOT / "docs/documentation-inventory.json").read_text())
    metadata = json.loads((ROOT / "docs/manual/_generated/inventory.json").read_text())
    previous = None
    ref = os.environ.get("DOCS_BASE_REF", "")
    if ref and ref.strip("0"):
        subprocess.run(["git", "rev-parse", "--verify", f"{ref}^{{commit}}"], cwd=ROOT, check=True, stdout=subprocess.DEVNULL)
        path = "docs/documentation-inventory.json"
        tracked = subprocess.check_output(["git", "ls-tree", "--name-only", ref, "--", path], cwd=ROOT, text=True).strip()
        if tracked:
            previous = json.loads(subprocess.check_output(["git", "show", f"{ref}:{path}"], cwd=ROOT, text=True))
    try:
        pages, completed, pending = validate(catalog, metadata, previous=previous)
    except ValueError as error:
        raise SystemExit(str(error)) from error
    print(f"Compiled mappings valid: {pages} pages, {completed} completed components, {pending} pending.")


if __name__ == "__main__":
    main()
