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

The generated shape below describes typed options before URL query decoding;
URL values are strings parsed by the provider. Use ASCII delimiter/quote/escape
characters.

<!-- docs:reference format:csv -->
## Configuration reference

Generated from the Rust **deserialization** contract. This describes shape, not all semantic validation, service prerequisites, or successful execution.

### CsvOptions

CSV format options parsed from query parameters  Based on arrow_csv::reader::Format and ReaderBuilder options. All fields have sensible defaults matching arrow_csv defaults.  # URL Examples  - csv:///data/file.csv - Default settings (comma delimiter, has header) - csv:///data/file.csv?delimiter=; - Semicolon delimiter - csv:///data/file.csv?delimiter=;&amp;has_header=false - No header row - csv:///data/file.csv?batch_size=16384 - Larger batch size

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `batch_size` | integer | no | 8192 | Number of records per batch (default: 8192) Constraints: format="uint"; minimum=0. |
| `delimiter` | string | no | "," | Field delimiter (default: ',') Constraints: minLength=1; maxLength=1. |
| `escape` | string &#124; null | no | not specified | Escape character (default: None) Constraints: minLength=1; maxLength=1. |
| `has_header` | boolean | no | true | Whether the file has a header row (default: true) |
| `quote` | string | no | "\"" | Quote character (default: '"') Constraints: minLength=1; maxLength=1. |
| `schema_infer_max_records` | integer | no | 100 | Number of rows to sample for schema inference (default: 100) Constraints: format="uint"; minimum=0. |

Unknown fields: not rejected by this shape; see runtime notes.
<!-- docs:end-reference -->

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
