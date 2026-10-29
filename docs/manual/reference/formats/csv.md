---
title: CSV
doc_id: format:csv
---

# CSV format provider

The `csv` format interprets raw CSV bytes through Arrow and exposes a table to
SQL. It is a URL format provider, not a factory with a YAML configuration.
For pond files use `csv:///path.csv`; for host files use
`host+csv:///path.csv`, optionally rooted with `-d DIRECTORY`.

## Example

Prerequisites: Bash and `pond` on `PATH`. No pond, credentials, or service is
required. `SOURCE_DIR` and `WORK` are initialized by the complete script below.

{{ source path="/testsuite/examples/csv-query/readings.csv" language="csv" /}}

{{ source path="/testsuite/examples/csv-query/run.sh" language="bash" /}}

Test `057` asserts the parsed timestamp/temperature/humidity columns, selected
rows `(25,44)` and `(30,46)`, three input rows, temperature total `75`, mean
humidity `45`, and semicolon-file total `75`.

[Input](/testsuite/examples/csv-query/readings.csv),
[parameter input](/testsuite/examples/csv-query/semicolon.csv),
[complete script](/testsuite/examples/csv-query/run.sh),
[download package](/downloads/csv-query.tar).

## URL parameters

Quote URLs containing query parameters so the shell does not interpret `&`.
For example, the tested `?delimiter=;` selects a semicolon-separated input.

| Parameter | Type | Default |
|---|---|---|
| `delimiter` | character; use ASCII | `,` |
| `has_header` | boolean | `true` |
| `batch_size` | unsigned integer | `8192` |
| `quote` | character; use ASCII | `"` |
| `escape` | optional character | absent |
| `schema_infer_max_records` | unsigned integer | `100` |

Use positive batch/inference sizes. Unknown options are ignored. **Current
limitation:** an option-deserialization error falls back to the default options
as a set, rather than reporting a configuration error. This pilot does not
change runtime acceptance rules.

## Types and errors

Arrow infers column types from sampled records; timestamp recognition depends
on their contents. The fixture uses RFC 3339 UTC timestamps. A later value
incompatible with the inferred schema can fail decoding; CSV does not impose
a universal timestamp column or timezone.

Streaming decode uses a buffered initial chunk (up to 1 MiB) for inference,
then feeds batches to Arrow. The separate schema-inference API reads the
whole file into memory. Neither path should be described as unconditionally
constant-memory inference.

Provider URL compression layers are `+gzip`, `+zstd`, and `+bzip2`, e.g.
`host+csv+gzip:///readings.csv.gz`. Compression and malformed-file cases are
not claimed as exercised by this pilot.

## Related workflow

Continue with [creating a timeseries](../../guides/timeseries.md#creating-a-timeseries)
and compare [synthetic output](../factories/synthetic-timeseries.md#inspect).
