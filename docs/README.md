# Manual authoring (D01-D05 pilot)

The public source boundary is `docs/manual/`, selected by `docs/site.yaml`.
Design notes, this workflow, the migration inventory, and `docs/archive/` are
not page inputs. `_generated` directories are excluded from standalone page
discovery. The four pilot pages are authoritative only for their migrated
topics; the existing CLI/operator documents remain entry points elsewhere.

## Cached-binary workflow

Obtain a native `pond` with **documentation renderer contract 1** (D04 or
later). During development, run `make build` once with Rust installed, then:

```bash
make docs-check
make docs-build
make docs-serve
```

Or select an explicit compatible prebuilt/cached binary:

```bash
make docs-build POND_BIN=/absolute/path/to/pond
make docs-serve POND_BIN=/absolute/path/to/pond
```

`docs-build` invokes no Cargo, examples, services, or network downloads. Bash,
Make, and `tar` are required. It generates fixture downloads, builds into an
isolated staging directory, and replaces only marked documentation output
after a successful strict build. Failures retain the previous preview. By
default output is `.docs-preview/watertown/`; `DOCS_OUT` can select an absent
directory or existing marked manual output. Do not select source directories.
An incompatible binary fails explicitly; there is no fallback renderer.

`docs-serve` uses Python 3's existing static HTTP server, binds to localhost,
and does **not** open a browser. Visit
`http://127.0.0.1:8000/watertown/`. Set `DOCS_PORT` to change the port. The
preview target uses the default output path so project-prefix URLs work.
Stop it with Ctrl-C; no watcher or live reload is installed.

The underlying renderer command, after preparing downloadable packages, is:

```bash
pond run -d "$PWD" host+sitegen:///docs/site.yaml -- build ./site --strict
```

The repository is host input, not a pond. Choose an output outside selected
inputs. The `--` passes factory-specific flags through the outer CLI.
`_generated` reference generation will remain a separate step, never a
render side effect. D06 introduces compiled discovery and generated-reference
freshness; today's `docs-check` checks the source-reviewed pilot mappings
only, and prints the pending count. It does not claim complete public coverage.

## Executable examples

`testsuite/examples/` owns runnable YAML, scripts, and data. Include those exact
files with `source`; do not copy snippets into an independently maintained
quickstart. Follow [fixture conventions](../testsuite/examples/README.md).

```bash
# Requires a test image built from this revision; builds may require network.
bash testsuite/build-image.sh
make docs-examples
```

The existing harness runs tests `057`, `100`, and `213`; it mounts examples and
repository sources read-only in both ordinary and Compose modes. No services
or credentials are required by the pilot. Native execution of this selection
uses the same numbered scripts and helpers:

```bash
export PATH="$PWD/target/debug:$PWD/testsuite/helpers:$PATH"
export EXAMPLES_ROOT="$PWD/testsuite/examples" REPO_ROOT="$PWD"
bash testsuite/tests/057-documentation-csv-example.sh
bash testsuite/tests/100-synthetic-timeseries-basic.sh
bash testsuite/tests/213-sitegen-documentation-pilot.sh
```

Each script creates an isolated writable workspace. Host-required tests
receive absolute source roots from the harness; never assume container paths
when writing native tests. Service/credential-dependent work must declare its
coverage level and cannot count a skip as a pass.

## Metadata and authoring contract

`documentation-inventory.json` records stable IDs, pending work, explicit
internal exclusions, test evidence, correctness follow-ups, and page/example
mappings. Frontmatter `doc_id` is a plain scalar matching its catalog page
key. Coverage is one of `offline-e2e`, `local-service-e2e`, `live-service-e2e`,
`validation-only`, or `unverified`; it describes evidence, not prose quantity.
Only complete entries leave the migration baseline. New compiled-interface
enforcement and typed reference generation begin at D06/D07, not in this gate.

Use source-relative `.md` links, with fragments when useful. Heading IDs are
stable unique slugs (`inspect`, `inspect-1`, etc.). Strict builds validate
actual output, navigation, fragments, and includes under `/watertown/`.
Source inclusion only reads configured `include_roots`; code blocks and inline
code remain literal, including shortcode examples. The documentation layout
uses system fonts and local CSS; data layouts opt in to visualization assets.

Unit checks: `cargo test --locked -p sitegen --lib`, `cargo fmt --all -- --check`,
and `cargo clippy --locked -p sitegen --all-targets -- -D warnings`.
Integration test `213` proves the readiness gate's success and failure cases
and cached-binary prose rebuild. Broad migration (D06 onward), Pages deployment,
and README redesign are deliberately not part of this pilot.
