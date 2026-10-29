---
title: Other format providers
doc_id: reference:formats
---

# Other format providers

Formats interpret raw bytes; they are not YAML factories. For example,
`pond cat host+jsonlogs:///tmp/events.jsonl --format=table` queries host input
without a pond; omit `host+` for pond paths. Quote URLs with query strings.
All support the common `+gzip`, `+zstd`, and `+bzip2` reader layers, not
automatic compression inferred from filename extensions. Layering a decoder
does not make a buffering parser constant-memory.

See [CSV](csv.md) for the asserted CSV package and
[shared URL rules](../concepts.md) for entry-type casts and roots.
Only XLS and hledger below have typed query options. The other four ignore
provider query options; no invented YAML/query schema is implied.
The source providers' unit tests cover parser cases; this page does not claim
every format/compression combination has an executable documentation package.

## OpenTelemetry JSON

Use `oteljson:///metrics.jsonl`. Each line is an OTLP
`ExportMetricsServiceRequest`, not arbitrary JSON or OTLP logs/traces.
Only gauge points are extracted; attributes and other metric kinds are not
retained by this provider. Output is `timestamp` (nanoseconds, no Arrow timezone)
plus nullable Float64 metric-name columns. Points sharing a timestamp form a
wide row; repeated metric/timestamp values overwrite in encounter order.
The implementation buffers observations/schema before emitting a batch.
Corrupt lines are skipped with a warning; a completed query does not certify
that every source record was accepted.

## JSON logs

Use `jsonlogs:///events.jsonl` for newline-delimited objects. Unioned keys
become nullable Utf8 columns, including numeric/timestamp fields. Strings are
unquoted, objects/arrays become compact JSON text, and missing/null values
are Arrow nulls. Cast values explicitly in SQL before numeric/time analysis.
The whole parsed input is buffered; this is not bounded-memory streaming.
Blank/NUL handling and malformed/non-object lines are parser-specific;
unparseable/non-object lines are skipped with a counted warning.

## Web logs

Use `weblog:///access.log` for Combined or Common Log Format. Output has UTC
microsecond `timestamp`, `remote_addr`, nullable `remote_user`, `method`, `path`,
`protocol`, UInt16 `status`, UInt64 `body_bytes_sent`, and nullable
`http_referer` / `http_user_agent`. CLF timestamps include an offset;
`-` in nullable text becomes null. Input is buffered into parsed entries.
Malformed lines warn and are skipped, not returned as placeholder rows.

## Excel HTML

Use `excelhtml:///hydrovu-export.html` for the HydroVu-specific HTML structure:
table `id="isi-report"`, recognized data header/row markers. This is not a
general HTML-table or binary Excel parser. Output has nullable Utf8
`Date Time` and nullable Float64 parameter columns; convert the string time
explicitly for downstream temporal operations. It buffers the HTML DOM and
rows, then emits batches of 1000. Missing tables/headers and invalid input fail.

## Excel

Use `xls:///readings.xls?sheet=Readings&header=true`. The Calamine-backed
provider reads `.xls` and `.xlsx` workbooks, but not Excel-compatible HTML (use
`excelhtml` for HydroVu HTML exports). `sheet` is required when a workbook has
zero or multiple worksheets; a missing named sheet reports available names.

For `.xlsx`, a declared Excel Table supplies its exact range and column names.
Without a declared table, header detection normally selects the first populated
row. It also recognizes report exports consisting of one-cell title rows, a
blank separator row, and a multi-column text header, excluding the title
material from output and inference. Use `header_row` only to override an
ambiguous or unusual layout. `header: false` retains every row as data and
generates column names.

Blank headers receive generated names when their columns contain data; unnamed
columns empty in every data row are omitted as worksheet layout. Named empty
columns remain in the schema. Duplicate names are rejected. Inference examines
data rows after the selected header: numeric columns become `Int64` or
`Float64`, booleans become `Boolean`, and Excel datetimes become timezone-free
microsecond timestamps. Other mixed-type columns become text without dropping
values. Empty cells are nulls and internal blank rows are retained.

Spreadsheet error cells, invalid options, unreadable workbooks, and empty
worksheets fail explicitly. Header-only worksheets expose their schema with
zero data rows. Formulas use cached values; the provider does not recalculate
them. Calamine requires seekable input, so the provider buffers the workbook
and selected worksheet in memory. Parsing and inference run on a blocking
worker; batching does not make workbook reads streaming, and SQL filters do not
avoid parsing the worksheet.

<!-- docs:reference format:xls -->
## Configuration reference

Generated from the Rust **deserialization** contract. This describes shape, not all semantic validation, service prerequisites, or successful execution.

### XlsOptions

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `batch_size` | integer | no | 8192 | Constraints: format="uint"; minimum=0. |
| `header` | boolean | no | true |  |
| `header_row` | integer &#124; null | no | null | Constraints: format="uint"; minimum=0. |
| `sheet` | string &#124; null | no | null |  |
| `table` | string &#124; null | no | null |  |

Unknown fields: **rejected**.
<!-- docs:end-reference -->

## Hledger journal

Use `hledger:///accounts.journal?batch_size=8192`. This is a single-file posting
reader, not the hledger executable or a running-balance engine. Text/parsing
are buffered; parsed posting rows are converted in batches. Amounts use exact
Decimal128(38,18); values requiring excess precision fail rather than round.
Dates use Date32. Output includes source, transaction sequence, date, status,
posting status, code, payee, description, account, commodity, amount,
`amount_inferred` (Boolean), nullable transaction/posting comments, and
discovered tag columns (nullable Utf8); inspect with `pond describe` for the
complete current schema.
Unsupported journal features are errors, not an instruction to execute includes
or external commands. Unknown options and zero batch size fail.

<!-- docs:reference format:hledger -->
## Configuration reference

Generated from the Rust **deserialization** contract. This describes shape, not all semantic validation, service prerequisites, or successful execution.

### Options

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `batch_size` | integer | no | 8192 | Constraints: format="uint"; minimum=0. |

Unknown fields: **rejected**.
<!-- docs:end-reference -->
