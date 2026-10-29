# Manual authoring

`docs/manual/` is the public content boundary selected by `docs/site.yaml`.
Design notes, this workflow, `docs/archive/`, and the reviewed migration
inventory are not pages. `_generated` directories never become standalone
pages. The manual is current development documentation, not a multi-version
stable-release site. D11 retains final historical/duplicate cleanup.

## Obtaining a compatible binary

Use a native `pond` with **documentation renderer contract 1** (D04 or later).
Build once with the repository's Rust toolchain using `make build`, or select
an explicit compatible prebuilt/cached executable. Production images and
Debian packages include matching sitegen vendor assets; source-built chart
examples additionally need `make vendor` once. The content-only manual does
not need vendor downloads.

## Fast prose workflow

```bash
make docs-build
make docs-serve POND_BIN=/absolute/path/to/pond
```

`docs-build` invokes no Cargo, examples, services, or network downloads. Bash,
Make, and tar are required. It packages the exact shared fixtures, stages a
strict sitegen build, and replaces only marked documentation output after
success. Failure preserves the previous preview. Default output is
`.docs-preview/watertown/`; `DOCS_OUT` can select an absent directory or
existing marked output, never selected source directories.

`docs-serve` additionally uses Python 3's existing HTTP server, binds to
localhost, and **never opens a browser**. Visit
`http://127.0.0.1:8000/watertown/`; change `DOCS_PORT` if needed. Stop with
Ctrl-C. No watcher/live reload is installed.

The renderer command after preparing downloads is:

```bash
pond run -d "$PWD" host+sitegen:///docs/site.yaml -- build OUTPUT --strict
```

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

`scripts/generate-docs.py` writes deterministic metadata/fragments under
`docs/manual/_generated/` and managed regions in ordinary authored Markdown:

```text
<!-- docs:reference factory:synthetic-timeseries -->
... generated fields ...
<!-- docs:end-reference -->
```

Edit field descriptions beside their Rust fields, then regenerate. Do not edit
generated region bodies. Schema is explicitly the **deserialization** contract;
semantic constraints, side effects, prerequisites, and effective runtime
defaults still need authored explanation. The renderer never generates
reference or executes code fences.

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
`documentation-inventory.json`. That catalog maps stable public IDs to one
authoritative page and records examples, evidence levels, exclusions, and
known correctness gaps.

Use source-relative `.md` links and actual heading fragments. Strict builds
validate output paths, navigation, includes/regions, links, and anchors under
`/watertown/`. Source inclusion reads only approved `include_roots`; fences,
inline code, and included source remain literal. Documentation uses system
fonts/local CSS; data layouts opt in to visualization assets.

## GitHub Pages

Repository Settings -> Pages -> Source must be **GitHub Actions** (already
configured). Protect the `github-pages` environment for trusted main.
The existing Rust CI workflow builds/validates PR previews with read-only
permissions and uploads a preview artifact; PRs do not deploy.

Trusted main waits for audit/check and both production image builds. The AMD64
matrix job uploads its own image digest/version/revision record, avoiding
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
