<img src="logo/watertown-banner.svg" alt="Watertown" width="640">

[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/jmacd/watertown/badge)](https://scorecard.dev/viewer/?uri=github.com/jmacd/watertown)
[![SLSA 3](https://slsa.dev/images/gh-badge-level3.svg)](https://slsa.dev)

Watertown is a file machine.<br>
Watertown is a timeseries database.<br>
Watertown is a site generator.<br>
Watertown is a telemetry system.<br>

Watertown is local-first.<br>
Watertown is transactional.<br>
Watertown is replicated.<br>
Watertown is small.<br>

## What is Watertown?

Watertown is a file system tool to help organize process files and
file-based processes. Watertown lets you place your CSV, Parquet, and
JSON files into application-specific paths, then pattern-match and
query them using pre-built file factories or ad-hoc SQL statements.

Watertown is built in Rust, using Apache DataFusion embedded query
engine and DeltaLake transaction system. Watertown reads and writes
local storage, cloud storage, git repositories, REST APIs, and more.

Watertown's abstract file system has first-class support for tabular,
time-series, and multi-version file data. File and directory factory
instances can be registered to create dynamic, derivative file
content. Watertown includes built-in factories for combining, joining,
and reducing timeseries.

Watertown is useful for data collection, analysis, and monitoring in
small industrial settings.

## Start with a reproducible example

The [development manual](https://jmacd.github.io/watertown/) documents the
current interfaces and their verification limits. It is not a stable-release
compatibility promise; original data sources should remain independently
recoverable.

Build from a checkout with the Rust toolchain, then run the complete
[CSV example](testsuite/examples/csv-query/run.sh):

```bash
make build
export PATH="$PWD/target/debug:$PATH"
bash testsuite/examples/csv-query/run.sh
```

No pond, service, or credential is needed. The shared input has three rows,
temperature total `75`, and mean humidity `45`; numbered test `057` asserts
these results and the delimiter example. The script creates a fresh workspace
and prints its output path; the README does not maintain a second quickstart.
See [build/package instructions](docs/README.md#obtaining-a-compatible-binary)
and [releases](https://github.com/jmacd/watertown/releases).

## Guides and reference

| Task | Tested entry point / reference |
|---|---|
| Create and query timeseries | [Guide](docs/manual/guides/timeseries.md) |
| Export typed data | [Export semantics](docs/manual/reference/export.md) |
| Build a timeseries site | [Complete package and guide](docs/manual/guides/timeseries-site.md) |
| Publish a blog/RSS feed | [Host-mode guide](docs/manual/guides/blog.md) |
| Back up, restore, and maintain ponds | [Operations guide](docs/manual/guides/operations.md) |
| Find flags and configuration fields | [CLI](docs/manual/reference/cli/commands.md), [sitegen](docs/manual/reference/sitegen/configuration.md), [shared concepts](docs/manual/reference/concepts.md) |

The manual is built by `pond` itself. [Authoring instructions](docs/README.md)
explain cached-binary previews, schema-derived reference, executable fixtures,
and production-image GitHub Pages publishing.

## Who is building Watertown?

Watertown is built by the [Caspar Water System](https://casparwater.us)
for small water systems everywhere.

The author is [Joshua MacDonald](https://github.com/jmacd), an
open-source lead and software engineer at Microsoft, working in
telemetry systems. Joshua is a member of the OpenTelemetry technical
committee and co-founder of the OpenTelemetry-Arrow project, which is
bringing a high-performance telemetry pipeline in Rust to
OpenTelemetry.

## Who is using Watertown?

Caspar Water uses Watertown for telemetry, monitoring, its public
portal, and more.

The [Noyo Center for Marine Sciences](https://www.noyocenter.org/) in
Fort Bragg, CA was Watertown's first user, where Watertown gathers water
quality data for its public portal.

![Caspar Duck Pond](./caspar_watertown.jpg)

## Integration Tests

Tests live in `testsuite/tests/` as numbered shell scripts.  Each test
runs in a fresh Docker container with the `pond` binary:

```bash
make test-image                     # Build the test Docker image
make integration                    # Run all tests (skips browser tests)
make integration-all                # Run all tests including browser

# Run a single test
cd testsuite && ./run-test.sh 201

# Run interactively (explore in container)
cd testsuite && ./run-test.sh --interactive
```

## License

Apache-2.0 — see [LICENSES/](LICENSES/) for details.
