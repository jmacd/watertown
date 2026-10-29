# Documentation system design and work sequence

Status: proposed; implementation has not started.

The manual will be built with Watertown sitegen and published through GitHub
Pages. A bounded four-page pilot establishes implementation readiness before
broad migration; it does not select the publishing tool. There is no fallback
to MkDocs or another generator. Search is deferred. This document defines the
design, migration strategy, work dependencies, and acceptance criteria. Paths
below are relative to the repository root.

## 1. Goals

Redesign the README as an approachable entrance to Watertown and build a
comprehensive online manual maintained as small, focused documents. The manual
must explain both individual interfaces and complete workflows. Hierarchical
navigation and per-page contents provide discoverability without search in
this iteration.

The central invariant is:

> The YAML and commands shown as tested examples are the same files executed
> by the testsuite, and their documented results are asserted.

The system must:

- Document every public CLI command, factory, and format provider.
- Describe configuration fields, defaults, nested variants, constraints,
  environment references, and observable behavior.
- Include guides for creating timeseries, exporting data, generating sites,
  keeping a blog, monitoring, and operating replicated ponds.
- Keep examples reproducible without production data or credentials.
- Detect missing documentation, stale generated reference, broken includes,
  and failing examples in CI.
- Preserve a fast documentation-only editing and preview workflow.
- Use sitegen's existing host-mode build path without requiring a pond.
- Limit renderer work to small, reusable sitegen capabilities rather than a
  separate documentation product.

### Non-goals

- Building a general-purpose documentation framework or a new test runner.
- Search UI, search indexes, ranking, or an external search service/indexer.
- Using MkDocs or another publishing generator, directly or through a factory
  wrapper.
- Syntax highlighting, code-copy controls, and live reload in the initial
  pilot; readable code blocks and ordinary rebuild/preview are sufficient.
- Automatically treating historical design documents as current product
  documentation.
- Generating all explanatory prose from code.
- Replacing integration tests with YAML syntax or JSON Schema validation.
- Requiring live production services to build or preview the manual.
- Redesigning runtime interfaces merely to make them easier to document.

## 2. Current state and implications

The repository already has most of the ingredients:

| Existing material | Implication |
|---|---|
| `README.md` | Retain project identity and real-world context; improve reader entry points. |
| `docs/cli-reference.md` | Migrate section by section, verifying against implementation and tests. |
| `docs/operator-guide.md` and other operational documents | Review and migrate useful operator guidance without publishing internal history wholesale. |
| `docs/archive/` and design notes | Keep outside the public manual by default. |
| `testsuite/tests/` | Reuse the numbered integration-test harness and assertion helpers. |
| `crates/sitegen/` | Reuse host-mode builds, Markdown/frontmatter, content routes, nested navigation, layouts, assets, and URL prefixes. |
| `crates/provider/src/registry.rs` | Discover factories and their capabilities from the compiled registry. |
| `crates/provider/src/format_registry.rs` | Discover registered format schemes from the compiled registry. |
| Rust configuration types and Clap definitions | Source mechanical reference details from the implementation. |

Important limitations to address:

- Factory registration provides validation hooks, not complete configuration
  schemas or documentation metadata.
- Not every existing example script asserts its results. Printing a table or
  exiting successfully is not enough to claim a documented result is tested.
- `testsuite/run-test.sh` mounts an individual script; reusable example files
  need an additional mount in both ordinary and Compose execution.
- `.gitignore` ignores names and extensions useful for fixtures, including
  `run.sh`, CSV, SQL, and Parquet files. Introduce narrow exceptions for reviewed
  example fixtures rather than weakening repository-wide exclusions.
- Test workflow documents currently name `docs/cli-reference.md` as the source
  of truth. Update those instructions as sections migrate.
- Shortcode preprocessing currently runs before Markdown parsing and does not
  protect code blocks or inline code. A manual must be able to display sitegen
  syntax without executing it.
- Content slugs default to filenames. Nested manuals need a source-to-output
  path mapping, collision detection, and Markdown-link resolution.
- Sitegen validates missing local images, but does not yet provide general
  page/fragment checking or a documentation-oriented strict mode.
- Content-only builds still write visualization scripts and attempt to copy
  vendor assets. The common page head also requests external fonts. A lean
  manual should not need these assets or network requests.
- A clean Rust build is not intrinsically lighter than installing a Markdown
  publisher. The benefit is reusing an existing binary and publishing system;
  prose preview must work with a compatible prebuilt or cached binary.

## 3. Information architecture

Use `docs/manual/` as the manual's Markdown source directory, selected by
content stages and routes in `docs/site.yaml`. Existing design notes remain
outside that selection. Exclude generated fragments from standalone page
discovery.

| Section | Contents |
|---|---|
| Getting started | Installation, querying host files, creating a pond, first timeseries |
| Concepts | Files/tables/series, versions, transactions, host and pond paths, URL schemes, patterns, factories |
| Guides | Ingestion, SQL derivation, joins, pivots, downsampling, materialization, exports, sites, blogs |
| CLI reference | One page per command, with its subcommands, arguments, flags, environment, and examples |
| Factory reference | One page per public factory, including configuration and execution semantics |
| Format reference | One page per format provider, plus native Parquet/table/series handling |
| Export reference | Selection, time boundaries, partitioning, output layout, incremental behavior, sitegen integration |
| Sitegen reference | Configuration, routes, content, frontmatter, layouts, shortcodes, navigation, charts, feeds, reports, themes |
| Operations | Automatic execution, monitoring, storage profiles, replication/imports, recovery, maintenance, limits |
| Contributing | Builds, tests, documentation authoring, adding a component, adding executable examples |

Guides answer "How do I accomplish this?" Reference pages answer "What exactly
does this interface accept and do?" Link between the two instead of copying
the same explanations.

### Initial catalog to verify

This is a planning inventory, not a replacement for compiled discovery:

| Family | Components |
|---|---|
| Data collection | `hydrovu`, `git-ingest`, `logfile-ingest`, `journal-ingest` |
| Derived data | `sql-derived-table`, `sql-derived-series`, `synthetic-timeseries`, `timeseries-join`, `timeseries-pivot`, `temporal-reduce`, `temporal-reduce-series`, `materialize-series`, `pump-state-series` |
| Composition and execution | `dynamic-dir`, `column-rename`, `exec` |
| Publishing and monitoring | `sitegen`, `monitor-report`, `email-report` |
| Operational configuration | `storage-minio`, `storage-azure`, `rate-limit` |
| Registered formats | `csv`, `oteljson`, `jsonlogs`, `weblog`, `xls`, `excelhtml`, `hledger` |

Native entry-type schemes and compression layers need their own explanation;
they are not all format-provider registrations. Internal test factories need
explicitly recorded exclusions, not accidental omissions.

## 4. Source layout and ownership

Proposed layout:

```text
docs/
  site.yaml
  templates/
    sidebar.md
  manual/
    index.md
    getting-started/
    concepts/
    guides/
    reference/
      cli/
      factories/
      formats/
      export/
      sitegen/
    operations/
    contributing/
    _generated/
testsuite/
  examples/
    csv-query/
      readings.csv
      run.sh
    synthetic-timeseries/
      config.yaml
      run.sh
    timeseries-site/
      ...
    blog/
      ...
  tests/
    100-synthetic-timeseries-basic.sh
    ...
```

Build from the repository root using the existing command mechanism:

```bash
pond run -d "$PWD" host+sitegen:///docs/site.yaml build ./site
```

The configuration and source layout are proposed, not implemented. Strict mode
will be added before this command is used for publication. The repository is a
host filesystem input, not a pond to initialize or ingest.

| Content | Authoritative source |
|---|---|
| Explanation, workflows, caveats | Focused Markdown pages |
| CLI names, arguments, flags, defaults | Clap definitions |
| Factory/provider identity and capabilities | Compiled registries |
| Configuration shape and field descriptions | Rust configuration types |
| Executable YAML, commands, and input data | `testsuite/examples/` |
| Assertions about example results | Numbered tests using existing helpers |

Generated files must be labeled as generated and not edited by hand. Avoid a
parallel handwritten schema language that duplicates the Rust types.

### Bounded sitegen extension scope

Implement these as reusable sitegen capabilities rather than a new `mkdocs`
command or an independent renderer:

| Capability | Required behavior |
|---|---|
| Literal source inclusion | Include a complete file or named region as escaped code, with source-aware errors and no execution or recursive template expansion. |
| Markdown-aware preprocessing | Preserve fenced/indented code and inline code literally, including shortcode examples. |
| Canonical page/heading index | Map source documents to output URLs, resolve relative `.md` links and fragments, assign stable unique heading IDs, and reject output collisions. |
| Documentation layout | Reuse nested navigation; add per-page contents and readable reference tables/code blocks with keyboard-accessible navigation. |
| Strict builds | Fail on broken internal links/fragments, missing includes, invalid navigation, and applicable warnings instead of publishing incomplete output. |
| Capability-based assets | Emit only assets required by selected layouts/shortcodes; content-only pages need no visualization vendor bundle or external fonts. |

The page/heading index exists for routing, navigation, and validation, not for
search. Search is not an acceptance criterion and must not expand the pilot.

Use the existing page pipeline and shortcode conventions. Keep inclusion and
reference generation separate: the renderer reads declared inputs but does not
run the tests or generate the Rust catalog as a side effect of rendering.

Introduce behavior that could affect existing sites through explicit opt-in
configuration or flags, such as a proposed documentation layout and `--strict`
build flag. Check existing sitegen behavior before changing global shortcode,
slug, or asset rules.

## 5. Executable example contract

### Example packages

An example directory contains its complete inputs, configuration, and runnable
commands. It must state its prerequisites and work from a fresh, isolated
scratch directory. It must not depend on a developer's existing pond.

The same `run.sh` is runnable by a reader and by its numbered integration test.
The test prepares an isolated workspace, runs the example, and uses `check`,
`check_contains`, `check_not_contains`, and `check_finish` to assert outcomes.
Only the test-specific assertions remain outside the reader-facing script.

Use small deterministic datasets and fixed timestamps. Avoid wall-clock
assumptions, production paths, real secrets, and unexplained dependencies.
Never delete or overwrite a reader's existing pond as part of setup.

Mount the example source directory read-only in both container execution
paths. Copy inputs to an isolated writable workspace when the example requires
modification. Provide explicit source/output directory conventions for native
or host-required tests as well.

### Inclusion in documentation

Add a literal-source inclusion facility to sitegen. For example, the following
is proposed syntax, not an existing shortcode:

```text
{{ source path="/testsuite/examples/synthetic-timeseries/config.yaml" language="yaml" /}}
{{ source path="/testsuite/examples/synthetic-timeseries/run.sh" region="create-and-query" language="bash" /}}
```

An inclusion renders an escaped code block, not executable Markdown or
template text. Included `{{ ... }}`, HTML, shell expressions, and code-fence
characters must remain literal. Documentation code blocks containing the
inclusion syntax itself must also remain literal.

Named sections use comment markers in the source script, not line numbers.
Keep setup visible or clearly linked so copied instructions remain usable.
Provide links to complete source files and downloadable example packages.
Generate downloads from the same fixtures; do not maintain a second copy.

Resolve includes through the sitegen input root and restrict them to approved
paths; do not fetch arbitrary URLs or read outside the configured source tree.
Fail on missing files, missing or ambiguous regions, and malformed markers.
Diagnostics must identify the referring page and requested source/region.
Verify that failure behavior explicitly.

### What tests must establish

Examples must check meaningful outcomes, not only exit status:

| Example | Minimum meaningful assertions |
|---|---|
| CSV query | Parsed columns and selected/aggregated values |
| Synthetic timeseries | Row count, time boundaries, representative values |
| Join/pivot/reduction | Expected output schema and calculated values |
| Materialization | Initial results and behavior on a subsequent update |
| Export | Expected files, temporal boundaries, and readable output data |
| Sitegen | Routes, page content, and referenced data/assets |
| Blog/RSS | Posts, ordering, absolute feed links, exclusion rules |
| Replication | Destination content and expected update behavior |

When documenting output, assert the relevant values or include deterministic
output captured by the test. Avoid brittle snapshots of timestamps, transaction
IDs, or unrelated formatting.

Distinguish runnable complete examples from fragments, invalid examples, and
configuration-only samples. Invalid examples should assert the intended error.

## 6. Reference-page contracts

### Factories

Every factory page must explain:

- Purpose, prerequisites, supported execution modes, and resulting node type.
- Minimal working configuration and an advanced example where useful.
- Every field's type, requiredness, default, units, valid values, constraints,
  nested structure, and conditional requirements.
- Installation, execution, inspection, and configuration-update procedures.
- Input and output schemas where applicable.
- Side effects, incremental state, ordering, time boundaries, and failure
  behavior.
- Links to executable examples and related workflows.

Do not imply all factories support host mode, automatic execution, or the same
update semantics.

### Format providers

Describe URL syntax and query parameters rather than inventing a factory YAML
interface. Cover schema inference, output types, timestamp handling, supported
compression, malformed inputs, and streaming versus buffering behavior.

### CLI

Generate mechanical command details, then add task-oriented explanation,
tested examples, mode restrictions, side effects, and recovery guidance where
appropriate. Cover subcommands and global options as well as top-level names.

### Shared and specialized reference

Keep shared rules such as environment substitution, credential references,
path resolution, patterns, and duration syntax in dedicated pages.

Sitegen requires separate reference for its YAML, Markdown frontmatter,
shortcodes, and browser behavior. A factory configuration schema alone cannot
describe these interfaces. Export also needs a shared conceptual reference
without conflating CLI-specific options with sitegen export-stage options.

## 7. Generated metadata and drift prevention

### Developer-only exporter

Add a small developer-only Rust export target that produces deterministic
machine-readable metadata from:

- The Clap command tree.
- The fully linked factory registry, including external factory crates.
- The format registry and relevant scheme metadata.
- Configuration schemas as they become available.

Do not scrape Rust with regular expressions or parse human-formatted
`--help`/`list-factories` output. The exporter must not initialize a pond,
execute a factory, contact a service, or require real credentials.

### Configuration schemas

Pilot derived JSON Schema on representative Serde configuration types, using
a derive library such as Schemars if it preserves the accepted configuration
representation. Descriptions should live beside the Rust fields.

Verify handling of renamed fields, defaults, tagged enums, optional/null
values, flattened structures, and unknown-field rules. Generate field tables
from the deserialization schema, not assumptions about serialized output.
Document semantic constraints separately when schemas cannot express them.

Exercise existing validators only where they are confirmed side-effect-free,
with explicit test environment values. Raw credential-reference validation and
expanded configuration validation are separate concerns.

Do not alter runtime acceptance rules as an incidental effect of adding
documentation metadata. Record any discovered mismatch as a correctness issue.

### Inventory and coverage

Reference-page metadata should associate pages with stable component IDs and
example/test IDs. A small coverage check compares that metadata with the
compiled inventory.

During migration, maintain an explicit baseline of existing undocumented
interfaces. New public interfaces without documentation fail immediately.
Completed entries cannot regress. Temporary baseline entries are migration
work, not permanent exclusions.

Final completeness requires no pending public entries. Internal/test-only
exclusions must have reasons. Features that are not registry entries, including
sitegen shortcodes and frontmatter, need explicit inventory coverage too.

### Fast authoring

Check deterministic generated fragments into `docs/manual/_generated/`.
CI regenerates them using the existing Rust build and rejects differences.
Reference generation produces ordinary Markdown or structured reference data
independently of rendering. Ordinary prose editing and sitegen preview use
those files with a compatible prebuilt or cached `pond` binary, without
rebuilding the Rust dependency graph.

CI uses a binary built from the same revision as the generated reference and
examples. Record the required renderer capabilities/version so a prose
contributor gets a clear error for an incompatible binary. Do not silently
substitute an older renderer or make every prose rebuild invoke Cargo.

The exporter and checks must produce stable ordering and actionable errors
identifying the component, page, or example requiring attention.

## 8. Build, validation, and publishing

### Local workflow

Document how to obtain or select a compatible `pond` binary, then add Make
targets for build, preview, reference freshness checks, and example execution.
Target names are proposed:

| Target | Responsibility |
|---|---|
| `make docs-build` | Build the manual with sitegen in strict mode, without executing examples or unconditionally rebuilding Rust |
| `make docs-serve` | Serve the generated site with an existing static HTTP server, without opening a browser automatically |
| `make docs-check` | Check reference freshness and documentation coverage |
| `make docs-examples` | Run the documented-example selection through the existing harness |

Document which targets require Rust, containers, or extra services. Keep
rendering and test execution separate; do not execute arbitrary Markdown
fences during a documentation build. Document the chosen preview-server
prerequisite rather than adding a native server/watcher to the pilot.

Rendering an ordinary content-only manual must work without a pond, live
services, visualization vendor downloads, or network access. Pages that
explicitly demonstrate charts can request their additional assets separately.

### Validation layers

| Layer | Failure conditions |
|---|---|
| Documentation build | Broken internal links/anchors, duplicate output paths, invalid navigation, missing includes/regions, rendering errors |
| Literal rendering | Shortcode, HTML, shell, or fence syntax in code examples is expanded, executed, or corrupted |
| Content-only output | Unused visualization bundles or required remote fonts are emitted for ordinary documentation pages |
| Reference freshness | Generated metadata differs from the checked-in reference |
| Inventory coverage | Missing public interfaces, unexplained exclusions, invalid example mappings |
| Configuration checks | Examples fail applicable parsing, schema, or semantic validation |
| Behavioral examples | Commands produce incorrect results or expected artifacts are missing |

Implement explicit diagnostics for source-to-output links, fragments,
navigation, and includes. Do not infer correctness just from a successful HTML
render or a log with no warnings. Exercise strict failure cases as part of the
pilot, including under the GitHub Pages URL prefix.

Use existing test requirements for local services, privileged execution, and
host-only tools. Run deterministic offline examples routinely. Run MinIO and
Linux-specific examples on suitable CI runners. Browser checks should be
headless.

Live cloud/API examples must declare their prerequisites and coverage level.
Use local fixtures or mocks where possible, and distinguish validation-only
coverage from real service execution. Missing credentials must never turn a
skipped scenario into a reported pass.

### GitHub Pages

Use the standard Pages artifact build/deploy workflow, not generated HTML
commits to a publishing branch.

- Pull requests build and validate with read-only permissions and no deployment
  secrets.
- Trusted `main` builds publish only after required checks pass for the same
  revision.
- The deployment job receives only the necessary Pages/OIDC permissions and
  uses the `github-pages` environment.
- Pin Actions to reviewed commit SHAs and use reproducible Rust dependencies.
- Set the project base URL correctly for the proposed initial address,
  `https://jmacd.github.io/watertown/`.
- Show the source revision and documentation version clearly.
- Keep generated site output and local preview artifacts out of Git.

Enable the repository's Pages publishing source as an explicit setup step.
Initially publish the current development manual with clear labeling; do not
introduce a multi-version site before a release-documentation policy exists.

## 9. README and migration strategy

The README should remain useful without becoming another manual:

1. What Watertown is and who it serves.
2. One small, reproducible example.
3. Installation and getting-started links.
4. Direct links to timeseries, export, sitegen, blogging, and operations guides.
5. Project context, contribution entry point, and license.

The README cannot rely on sitegen source inclusion when rendered on GitHub.
Use a small checked/synchronized excerpt or link to a complete executable
example; do not introduce an independently maintained quickstart.

Migrate `docs/cli-reference.md` by verified topic. For each migrated topic,
make the new page authoritative and replace the old section with a link while
retaining useful headings/anchors. Avoid maintaining two complete copies.

Review current operator guidance against the implementation. Historical and
removed interfaces belong in clearly labeled historical material, not the
current reference.

Update `testsuite/README.md`, `testsuite/AGENT_INSTRUCTIONS.md`, and
`testsuite/WORKFLOW.md` where they instruct contributors to edit the monolith.
Retain useful existing entry points as compatibility indexes.

## 10. Dependency-ordered work sequence

Each item is intended as a reviewable change or small series of changes.
Dependencies below are actual prerequisites, not merely presentation order.

| ID | Work item | Depends on | Acceptance criteria |
|---|---|---|---|
| D01 | Inventory public interfaces and existing evidence | None | Catalog commands, factories, formats, schemes, sitegen interfaces, relevant tests, gaps, and explicit internal exclusions. Agree page/example metadata and coverage levels. |
| D02 | Scaffold a host-mode sitegen manual | D01 | Add `docs/site.yaml`, a small content tree, nested navigation, local build/preview instructions, and an explicit source/output boundary. Use an existing compatible binary; no pond or Python publishing toolchain. Archives remain unpublished. |
| D03 | Support reusable example fixtures | D01 | Add narrowly scoped fixture ignore exceptions and read-only mounts for ordinary/Compose tests; document host-required handling. Existing test behavior is preserved. |
| D04 | Add bounded documentation capabilities to sitegen | D02, D03 | Implement literal code/includes, canonical source links and heading IDs, documentation layout/page contents, strict diagnostics, and lean assets. Existing sites retain intended behavior. No search, highlighter, or live-reload subsystem. |
| D05 | Validate the four-page sitegen pilot | D04 | Build a factory page, a format page, a timeseries guide, and a shortcode reference from tested fixtures. Verify nested links, literal shortcode examples, strict failures, lean output, and cached-binary authoring. Resolve readiness gaps within sitegen before broad migration. |
| D06 | Export compiled inventory and CLI metadata | D05 | Developer-only exporter discovers the fully linked interfaces, emits deterministic output without side effects, and supports renderer-independent generated-reference freshness/coverage checks. |
| D07 | Pilot typed configuration reference | D06 | Generate and validate field reference for representative factory, provider, and nested sitegen types. Demonstrate defaults, enums, optional fields, and semantic limits without changing runtime behavior. |
| D08 | Complete factory, provider, export, and sitegen reference | D07 | Every public component and configuration field is documented; examples and coverage levels are mapped; explicit non-registry interfaces are covered. Extend guides to timeseries sites and blog/RSS. |
| D09 | Complete CLI, concepts, and operational guidance | D06 | Cover all commands/subcommands and shared concepts; verify backup/import/recovery/maintenance guidance and executable examples against current behavior. |
| D10 | Publish the initial manual and redesign README | D06 | PR validation and trusted-main Pages deployment work with same-revision gates; base-path links/assets work; README points to useful tested entry points. Initial coverage limitations are visible. |
| D11 | Finish migration and enforce completeness | D08, D09, D10 | No pending public inventory entries; old links remain useful; contributor instructions point to the new system; duplicate authoritative sections are removed. Search remains deferred. |

D02 and D03 can proceed independently after D01. D05 is an implementation
readiness gate: do not expand the renderer indefinitely or begin broad migration
before the pilot meets its acceptance criteria. D08 and D09 can be split into
component-sized changes. D10 can publish an explicitly incomplete manual before
catalog completion, but must not advertise it as comprehensive until D11.

### Four-page pilot and readiness gate

The pilot contains:

1. A `synthetic-timeseries` factory reference with included YAML and commands.
2. A CSV format-provider reference with a runnable query and parameter example.
3. A timeseries guide connecting those concepts to asserted output.
4. A sitegen shortcode reference that displays shortcode and inclusion syntax
   literally while demonstrating navigation and page contents.

Include nested source directories, repeated heading names, and cross-page
fragment links. Deliberately break an include, a link, a navigation target, and
an output-path assignment to prove strict builds reject each case.

Complete the pilot through the bounded extensions in section 4, with useful
navigation, lean output, and no rebuild of Rust for ordinary prose edits.
Search, highlighting, code-copy controls, and live reload are not part of this
acceptance gate.

Unmet criteria remain sitegen implementation work, not grounds for switching
publishers. If a requirement cannot be met within the bounded extension scope,
record the blocker and obtain explicit agreement on a scope adjustment before
proceeding. Do not introduce a separate renderer or broad plugin framework.
Preserve the boundary between Markdown, example fixtures, reference generation,
and sitegen rendering so each remains independently maintainable.

### Existing test seeds and subsequent guides

- CSV query: start from `testsuite/tests/024-csv-url-scheme.sh` and
  `025-csv-workflow.sh`.
- Synthetic timeseries: adapt
  `testsuite/tests/100-synthetic-timeseries-basic.sh`, adding assertions rather
  than preserving its print-only verification.
- Timeseries site: select the smallest representative pipeline from existing
  reduction/sitegen tests, keeping data deterministic and dependencies local.
- Blog and RSS: extract fixtures from
  `testsuite/tests/207-sitegen-content-pages.sh` and
  `208-sitegen-rss-feed.sh`.

Timeseries-to-site and blog/RSS guides follow the four-page pilot rather than
expanding its acceptance gate. Do not rewrite the entire testsuite up front.
Extract only the fixtures needed for a documented workflow, then repeat the
established pattern.

## 11. Completion criteria

The documentation system is complete when:

- The README provides a clear introduction and reliable manual entry points.
- Every public interface has an identifiable authoritative reference page.
- Configuration fields and defaults agree with the accepted implementation.
- Runnable examples use the exact published fixtures and assert their outcomes.
- Code and source inclusions preserve literal syntax, including sitegen's own
  shortcodes.
- External-service coverage is explicit and never misrepresented as offline
  end-to-end coverage.
- CI rejects stale generated reference, newly undocumented interfaces, broken
  includes/links, and failing required examples.
- A contributor can add or update one component without editing a monolithic
  reference document.
- Hierarchical navigation and per-page contents make the manual usable without
  search; search remains explicitly deferred.
- Documentation-only preview uses a compatible cached/prebuilt binary and
  requires neither a pond nor a Rust rebuild.
- Content-only output has no unused visualization bundle or required remote
  font dependency.
- Pages deployment publishes the validated revision without PR deployment
  privileges or committed generated HTML.
- Existing documentation entry points remain useful and archives are clearly
  separated from current behavior.

## References

- [Sitegen entry point](../crates/sitegen/src/lib.rs)
- [Markdown and shortcode preprocessing](../crates/sitegen/src/markdown.rs)
- [Sitegen build pipeline](../crates/sitegen/src/factory.rs)
- [Nested navigation example](../testsuite/tests/210-sitegen-nested-sidebar.sh)
- [GitHub Pages custom workflows](https://docs.github.com/en/pages/getting-started-with-github-pages/using-custom-workflows-with-github-pages)
