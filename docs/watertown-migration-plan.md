# Migration Plan: rename `duckpond` -> `watertown`

Status: DRAFT (this-repo scope). To be merged with the separate caspar.water
deployment migration plan (see Part 3).

Related: `docs/selfmon-deb-pipeline-design.md` (the selfmon `.deb` auto-update
pipeline) is sequenced to be built **after** this rename lands. It reuses the
renamed image package path, the renamed Debian package name, and the renamed
`/usr/share/watertown/` install paths, so its work items must not begin until
the Part 4 cutover below has soaked. See Part 4, item 5.

## 1. Background & rationale

The product is currently branded **duckpond** (CLI binary `pond`). The name is
too easily confused with **DuckDB**, which is an actual runtime dependency
(the `duckdb-wasm` engine used by the site generator). To eliminate the
confusion we are removing the "duck" from *our* brand and renaming the product
to **watertown**.

Two things are explicitly NOT changing:
- The `pond` command, the `POND` environment variable, and the "pond" concept
  (a pond is the data store). This vocabulary stays.
- Any genuine **DuckDB** reference (`duckdb-*.wasm`, `duckdb` JS/API symbols,
  file names). These belong to the third-party engine and must be preserved.

Compatibility note: existing ponds do **not** need to be preserved. Ponds will
be reinitialized, so on-disk and wire-format identifiers may change freely.

## 2. Scope & rules

### 2.1 In scope
Active source tree only:
`crates/`, `docs/` (excluding `docs/archive/`), `testsuite/`, `scripts/`,
`.github/`, and top-level files (`Cargo.toml`, `README.md`, `Makefile`,
`Dockerfile`, `RELEASING.md`, `REUSE.toml`).

### 2.2 Out of scope (leave as historical record)
`docs/archive/`, `history/`, `inbox/`, `original/`, and any vendored
`node_modules/` or `vendor/dist/` third-party assets.

### 2.3 Token replacement table (the ONLY tokens to change)
| From              | To               |
|-------------------|------------------|
| `duckpond`        | `watertown`      |
| `DuckPond`        | `Watertown`      |
| `Duckpond`        | `Watertown`      |
| `DUCKPOND`        | `WATERTOWN`      |
| `DuckPondTaxonomy`| `WatertownTaxonomy` |

### 2.4 MUST NOT touch
- `duckdb*` (585 refs) -- the real DuckDB engine.
- `rubber-duck` / "rubber duck" -- code-review terminology.
- Any bare `duck` substring. NEVER run a blanket `duck` -> `water` substitution;
  it would corrupt `duckdb`.

### 2.5 Baseline counts (for verification)
Captured from the active tree at plan time:
- `duckpond` (brand, all cases): 303 occurrences -> target 0.
- `duckdb` (keep): 585 occurrences -> must remain unchanged.

## 3. Work breakdown (this repo)

### A. Workspace + packaging metadata
- `Cargo.toml`: `authors = ["DuckPond Contributors"]`, `homepage`/`repository`
  = `github.com/duckpond`, workspace `description`.
- `crates/cmd/Cargo.toml`: Debian package `name = "duckpond"`, vendor install
  paths `usr/share/duckpond/vendor`, doc path `usr/share/doc/duckpond`, long
  description. (Install-path change is deployment-coupled -- see Part 4.)

### B. On-disk / wire-format strings (safe: reinit)
- `crates/tlogfs/src/schema.rs`: `pub mod duckpond` and keys
  `duckpond.timestamp_column`, `duckpond.min_temporal_override`,
  `duckpond.max_temporal_override`.
- `crates/steward/src/content_tree.rs`: tlog origin
  `format!("duckpond/{pond_id}")`.
- Matching test origin literals in `crates/sync-store/src/tlog/checkpoint.rs`
  and `.../tiles.rs`.

### C. Data taxonomy
- `crates/tlogfs/src/data_taxonomy.rs`: `#[taxonomy(duckpond)]`, enum
  `DuckPondTaxonomy`, Debug string `<duckpond/api_key:REDACTED>`, module docs.
- All referencers of `DuckPondTaxonomy` across crates.

### D. Environment variables + FHS vendor paths
- `crates/sitegen/build.rs`: `DUCKPOND_GIT_SHA`.
- `crates/sitegen/src/factory.rs`: `DUCKPOND_VENDOR`,
  `/usr/local/share/duckpond/vendor`, `/usr/share/duckpond/vendor`,
  `~/.cache/duckpond/vendor`.
- `crates/sitegen/src/layouts.rs`: `DUCKPOND_GIT_SHA` usage + footer branding.

### E. Rust user-facing strings, logs, comments
- `crates/cmd/src/main.rs`, `common.rs`, `commands/{copy,list,temporal}.rs`.
- `crates/hydrovu/src/lib.rs`, `crates/provider/src/factory/temporal_reduce.rs`,
  `crates/provider/src/table_creation.rs`,
  `crates/steward/src/{content_tree,control_table,ship}.rs`,
  `crates/tinyfs/src/transaction_guard.rs`,
  `crates/tlogfs/src/{persistence,test_utils,tests/mod}.rs`,
  `crates/utilities/src/{lib,test_helpers,chunked_files}.rs`,
  `crates/sync-store/src/content/mod.rs`.
- Test literals in `steward/tests/{fsck_test,tlog_materialize_test}.rs`,
  `provider/tests/excel_html_integration_tests.rs`.

### F. Sitegen generated-site branding (JS/CSS assets)
- `crates/sitegen/assets/{chart.js,duckdb-shared.js,explore.js,log-viewer.js,
  overlay.js,vega-shared.js,style.css}` -- change "DuckPond" branding text only;
  keep every `duckdb` filename and API symbol.
- `crates/sitegen/src/{config.rs,factory.rs,shortcodes.rs,lib.rs}`.

### G. Active docs (content edits)
- `README.md`, `docs/cli-reference.md`, `docs/operator-guide.md`,
  `docs/configurable-settings.md`, `docs/sitegen-design.md`,
  `docs/fallback-antipattern-philosophy.md`, `docs/anti-duplication.md`.

### H. Doc file renames + reference fixups
- `docs/duckpond-overview.md` -> `docs/watertown-overview.md`.
- `docs/duckpond-system-patterns.md` -> `docs/watertown-system-patterns.md`.
- Update references in: `docs/cli-reference.md`,
  `.github/copilot-instructions.md`, `crates/tinyfs/src/transaction_guard.rs`,
  `crates/utilities/src/chunked_files.rs`.

### I. CI / repo automation
- `.github/workflows/promote.yml`, `.github/workflows/rust-ci.yml`.
  - Both set `IMAGE_NAME: ${{ github.repository }}/duckpond`. The GitHub repo
    rename changes `${{ github.repository }}` to `jmacd/watertown`, but the
    trailing `/duckpond` literal must ALSO change to `/watertown` to land on
    `ghcr.io/jmacd/watertown/watertown` (not `.../watertown/duckpond`). This is
    the hard cutover point -- see Part 4, items 1-2.
- `.github/copilot-instructions.md` (brand text + renamed doc paths).
- `Makefile`, `Dockerfile`, `RELEASING.md`, `REUSE.toml` (branded lines).

### J. Testsuite
- `testsuite/{build-image.sh,docker-compose.test.yaml,Dockerfile,run-test.sh,
  BACKLOG.md,AGENT_INSTRUCTIONS.md,WORKFLOW.md,README.md,.gitignore}`.
- `testsuite/helpers/{check.sh,generate-logs}`,
  `testsuite/browser/test.sh`, `testsuite/browser/tests/210-*.mjs`.
- Test scripts that assert brand strings: `033,041,047,400,500,502,510,523,718,
  724` (edit brand references only; never the `pond` command).

### K. Brand assets
- `caspar_duckpond.jpg` -> `caspar_watertown.jpg` + update README/doc references.
- Review `logo/` and `logo.txt~`.

## 4. External / deployment coordination (MERGE with caspar.water plan)

These items span repos and MUST be sequenced with the caspar.water deployment
migration. They are called out here so the two plans can be merged.

1. **GitHub repository rename** `jmacd/duckpond` -> `jmacd/watertown`.
   - Updates clone/remote URLs; add `Cargo.toml` homepage/repository update
     (item A) in the same window.
2. **Debian package name** `duckpond` -> `watertown` and **install paths**
   `/usr/share/duckpond/` -> `/usr/share/watertown/` (item A + D).
   - caspar.water terraform (`terraform/station/watershop/`) and
     `config/scripts/pond.sh` reference these; must change in lockstep.
3. **Staging env / native selfmon**: `/usr/bin/pond` command name is UNCHANGED.
   Only vendor-path and package-name references need updating in
   `env/watershop-selfmon.env` and related terraform.
4. **Local working copy** `.../src/duckpond` -> optional local dir rename after
   the GitHub rename.
5. **Selfmon `.deb` pipeline (build AFTER this rename).** The pipeline in
   `docs/selfmon-deb-pipeline-design.md` introduces a new deb artifact package
   `ghcr.io/jmacd/watertown/pond-deb`, a `build-deb` job + promote loop in
   `.github/workflows/{rust-ci,promote}.yml`, and box-side auto-update
   (`config/scripts/update-selfmon.sh`, a `pond-selfmon-update@` systemd
   timer, terraform `oras` install + `DEB_CHANNEL`). None of it is authored
   until the item-1/2 cutover has soaked, so it is written once under
   `watertown` names. The CLI binary stays `pond`; only the package name,
   registry path, and vendor install paths carry the rename.

Sequencing decision (RESOLVED): flip the Debian package name (`duckpond` ->
`watertown`) and the `/usr/share/duckpond/` -> `/usr/share/watertown/` install
paths **in the same window** as the repo + image-package cutover (items 1-2), in
lockstep with the caspar.water terraform. Do not keep `duckpond` packaging
temporarily: the selfmon deb pipeline (item 5) is deferred until after this
cutover precisely so it can be authored once, directly under `watertown` names.

## 5. Verification

Run after each major phase and once at the end:
```
cargo fmt --all
cargo clippy --workspace --all-features -- -D warnings
cargo test --workspace
```
Grep guards over the active tree (exclude archive/history/inbox/original):
- `grep -ri duckpond <active tree>` -> 0 matches.
- `grep -rc duckdb <active tree>` -> unchanged from baseline (585).
- `grep -rin -w duck ... | grep -v 'duckdb\|rubber-duck\|node_modules'` -> empty.

Functional smoke:
- Rebuild the image and run a sitegen test (e.g. `testsuite` 201/205/306).
- Run a replication test (e.g. `500` or `510`) against MinIO.

## 6. Suggested execution order
B/C -> E -> D/F -> A/I/J -> G/H -> K -> full presubmit -> Part 4 (external,
merged with caspar.water plan).
