# Manual authoring

`docs/manual/` owns guides, concepts, CLI documentation, and category indexes.
Factory and format documentation lives beside the corresponding implementation
in `crates/`. The build assembles both into an isolated input tree selected by
`docs/site.yaml`; component pages are not checked-in copies under `docs/manual/`.
Design notes, this workflow, `docs/archive/`, and Rust source are not pages.
`_generated` directories never become standalone pages. The manual is current
development documentation, not a multi-version stable-release site.

## Obtaining a compatible binary

Use a native `pond` with **documentation renderer contract 1** (D04 or later).
Build once with the repository's Rust toolchain using `make build`, or select
an explicit compatible prebuilt/cached executable. Production images and
Debian packages include matching sitegen vendor assets; source-built chart
examples additionally need `make vendor` once. The content-only manual does
not need vendor downloads.

## Fast prose workflow

Each component owns an editable `documentation.md` beside its implementation:

```text
crates/provider/src/factory/column_rename/
  mod.rs
  documentation.md
  default.yaml
crates/provider/src/format/csv/
  mod.rs
  documentation.md
```

Factories in separate crates keep these files there, such as
`crates/gitpond/src/factory/`. Shared implementations retain separate component
inputs without duplicating Rust: SQL-derived table/series use `table/` and
`series/` under `sql_derived/`; direct temporal reduction uses
`temporal_reduce/series/`.

Edit the component's `documentation.md` for its overview, usage, and limits.
Factory pages include the adjacent `default.yaml` using
`{{ source path="default.yaml" language="yaml" /}}`. The build resolves this to
an exact staged copy and displays it as an always-visible code block. Do not
add download links, collapse the example, or duplicate its YAML in Markdown.
Format pages keep URL examples in Markdown, not invented YAML interfaces.
Each file contains a complete configuration for the illustrated variant:
all top-level fields are present, optional defaults are explicit, and example
inputs or overrides are labeled in comments. For optional features, `null`,
`[]`, or `{}` can be the actual default, not an unfinished placeholder.
Explain runtime-derived defaults and mutually exclusive alternatives in the
YAML comments. Do not replace credential references with literal secrets.

Prose and annotated YAML files are hand-authored. Component reference markers
are empty in source; the build fills them from the cached compiled schema in
the staging tree only. It never writes into component source files.
Runnable fixtures remain separately included from `testsuite/examples/`;
an illustrative configuration is not a claim of live-service execution.

```bash
make docs-build
make docs-serve POND_BIN=/absolute/path/to/pond
```

`docs-build` invokes no Cargo, examples, services, or network downloads. Bash,
Make, tar, and Python 3.9+ (stdlib only) are required. It assembles component
pages and YAML from the checkout with cached metadata, packages the exact
shared fixtures, and replaces only marked documentation output after
success. Failure preserves the previous preview. Default output is
`.docs-preview/watertown/`; `DOCS_OUT` can select an absent directory or
existing marked output, never selected source directories.

`docs-serve` additionally uses Python 3's existing HTTP server, binds to
localhost, and **never opens a browser**. Visit
`http://127.0.0.1:8000/watertown/`; change `DOCS_PORT` if needed. Stop with
Ctrl-C. No watcher/live reload is installed.

The wrapper runs the renderer against the assembled input tree, not directly
against the checkout. Stable component URLs remain
`reference/factories/NAME.html` and `reference/formats/NAME.html`.

An incompatible binary fails explicitly; no fallback renderer is selected.
Local footer provenance says `working-tree`; CI supplies the exact source SHA.

## Compiled reference and drift checks

For interface/config changes, use the developer-only exporter:

```bash
make docs-generate
make docs-check
```

These require Rust/Cargo and Python 3 (stdlib only). `docs-export` is gated by
the `documentation` Cargo feature; ordinary production builds do not ship it
or schema dependencies. It introspects shared Clap definitions and fully linked
registries, without creating a pond or executing factories/services.

`scripts/generate-docs.py --write` updates deterministic compiled metadata under
`docs/manual/_generated/` and managed references in non-component manual pages.
Component Markdown contains only a placement marker:

```text
<!-- docs:reference factory:synthetic-timeseries -->
<!-- docs:end-reference -->
```

The staging mode fills that marker with the full reference. There are no
committed per-component generated fragments or generated pages to edit.
Edit field descriptions beside their Rust fields, then refresh the compiled
metadata with `make docs-generate`. Schema is the **deserialization** contract;
semantic constraints, side effects, prerequisites, and effective runtime
defaults still need authored explanation. The renderer never generates
reference or executes code fences.

The public reference omits registry bookkeeping and repeated schema warnings.
Section names use readable words rather than Rust type identifiers; alternative
configurations use their schema title or the opening sentence of their Rust
documentation. Only the opening paragraph is used as a section summary; keep
complete examples in `default.yaml` and longer explanations in the component
Markdown. The exported inventory retains the complete developer metadata.

`docs-check` rejects stale output, missing/removed compiled interfaces,
invalid page/example mappings, and regression of completed reference coverage.
`DOCS_BASE_REF` optionally checks against a Git base revision. Migration pending
entries are explicit; ordinary generation never expands that baseline. Current
reviewed public reference has no pending entries, but that does not imply
every service/platform/browser scenario has end-to-end coverage.

Typed-contract tests:

```bash
cargo test --locked -p cmd --features documentation --lib documentation::tests
```

These also parse every factory's `default.yaml`, check it against the compiled
schema, and ensure it includes every top-level field. Changing a factory's
fields therefore requires updating its authored YAML file, not just
regenerating the reference. `docs-check` requires colocated component inputs,
an include of the adjacent YAML, empty reference markers, and no duplicate
component pages in `docs/manual/`.

## Executable examples

`testsuite/examples/` owns runnable YAML, scripts, and data. Include those exact
files with `source`; do not maintain copied quickstarts. Follow
[fixture conventions](../testsuite/examples/README.md).

```bash
# Requires an image built from this revision; initial build needs network.
bash testsuite/build-image.sh
make docs-examples
```

Tests `057`, `100`, `213`, and `214` assert CSV/synthetic values, the strict
renderer contract, timeseries site exports, blog/RSS rules, and file-backed
replica recovery/updates. The existing harness mounts repository/fixtures
read-only in ordinary and Compose modes. No live credentials are required.

Native execution uses the same scripts/helpers:

```bash
export PATH="$PWD/target/debug:$PWD/testsuite/helpers:$PATH"
export EXAMPLES_ROOT="$PWD/testsuite/examples" REPO_ROOT="$PWD"
bash testsuite/tests/057-documentation-csv-example.sh
bash testsuite/tests/100-synthetic-timeseries-basic.sh
bash testsuite/tests/213-sitegen-documentation-pilot.sh
bash testsuite/tests/214-documentation-workflows.sh
```

Each script creates an isolated writable workspace and honors `TMPDIR`.
Coverage levels are `offline-e2e`, `local-service-e2e`, `live-service-e2e`,
`validation-only`, and `unverified`; a missing prerequisite or skipped scenario
is never a passed test. Cloud/API examples explicitly remain unverified where
no execution evidence exists. Browser interaction tests remain a separate
headless suite.

## Authoring and navigation

Frontmatter `doc_id` is a plain scalar matching the page key in
`documentation-inventory.json`. For components, `path` names the authoritative
colocated Markdown and `output` names its generated location in the assembled
manual. Non-component pages use `path` only. The catalog also records examples,
evidence levels, exclusions, and known correctness gaps.

Use manual-relative `.md` links and actual heading fragments. In component
Markdown, relative links use the published `output` location, not the crate
directory. Strict builds
validate output paths, navigation, includes/regions, links, and anchors under
`/watertown/`. Source inclusion reads only approved `include_roots`; fences,
inline code, and included source remain literal. Documentation uses system
fonts/local CSS; data layouts opt in to visualization assets.

## GitHub Pages

Repository Settings -> Pages -> Source must be **GitHub Actions** (already
configured). Protect the `github-pages` environment for trusted main.
The existing Rust CI workflow builds/validates PR previews with read-only
permissions and uploads a preview artifact; PRs do not deploy.

Trusted main waits for build/test checks, version computation, and both
production image builds. Dependency advisories remain visible in a separate
audit check, but do not gate static documentation publication or suppress
functional failures. The AMD64 matrix job uploads its own image
digest/version/revision record, avoiding
matrix-output architecture ambiguity. Pages pulls that exact digest, verifies
the OCI source revision and binary version, and runs all documentation
examples again through `scripts/docs-publish.sh`. Sources mount read-only;
scratch/output mount writable; containers run without network.

The runner supplies Bash/Make/tar and test helpers; the production image only
needs pond/runtime/vendor assets, not Make. The final strict artifact contains
the **contents** of the site root, not an extra `watertown` parent. Only the
deployment job has Pages/OIDC write permissions and uses `github-pages`.
No generated HTML is committed.

Local image-backed validation can use an immutable registry digest or local
`sha256:` image content ID with `DOCS_CONTAINER_IMAGE`, an absent `DOCS_OUT`,
`DOCS_EXPECTED_VERSION`, and `WATERTOWN_DOCS_REVISION`. Local compatible-image
checks do not substitute for the first same-revision production CI/deployment.
