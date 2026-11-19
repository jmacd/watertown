---
title: Rotating log-file ingestion
doc_id: factory:logfile-ingest
---

# Rotating log-file ingestion

## Overview

`logfile-ingest` reads rotating host log files and stores raw series in a writable pond. Archived files are immutable inputs; the active file is append-only.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

For OTLP JSON, set `timestamp_field: timeUnixNano` and
`timestamp_unit: nanoseconds` together. Supported units are listed below.

## Use and behavior

Install with `pond mknod logfile-ingest /etc/logs --config-path logs.yaml`, then run `pond run /etc/logs -- push` (the default operation). `b3sum` reports source checksums; `pull` does nothing.

Timestamp extraction searches recursively for the configured JSON key and records outward-rounded microsecond bounds. Without bounds, downstream temporal reduction may recompute full history. Raw log bytes do not imply one Arrow schema; query them through an appropriate format URL.

Numeric timestamps may be integers or fractional values, encoded as JSON numbers
or strings (including exponent notation). Integer conversion remains exact.
Fractional values are conservatively bounded in whole input units, allowing for
floating-point rounding, before conversion to microseconds. These bounds do not
alter the source records. Non-finite and out-of-range values are rejected.

For default Caddy JSON logs, use `timestamp_field: ts` and
`timestamp_unit: seconds`; no Caddy timestamp-format override is needed.

For RFC3339 strings such as `"2026-11-13T03:00:00Z"` or
`"2026-11-13T03:00:00.123456789+00:00"`, explicitly select:

```yaml
timestamp_field: ts
timestamp_unit: rfc3339
```

The strings must include a complete date and time with `Z` or a numeric UTC
offset. Bounds are normalized to UTC microseconds and rounded outward, including
for sub-microsecond and pre-epoch values. Numeric epochs are not accepted in
this mode, and numeric modes do not automatically interpret date strings.

Active input is ingested only through its last complete newline-delimited
record; an incomplete tail waits for a later append. Existing corrupt-line
warnings still apply, and a temporal append with no usable timestamp fails
rather than publishing an unbounded version.

Syntactically broken JSON lines are skipped with a warning, because readers
skip them too. A well-formed JSON record whose configured timestamp is missing
or unusable is different: readers return it, so the bounds would not cover it.
The whole append is rejected, nothing is written, and the ingest offset does
not advance. The error names the host file, the record's absolute byte offset,
and its line within the rejected bytes. Every later run fails the same way, and
later records in that file are not ingested until the record is repaired. Fix
the producer, then correct that record in the source log; the next run ingests
from the same offset.

Deploy a binary supporting this mode before enabling it in configuration.
Enabling timestamp extraction affects new versions only: existing unbounded
versions are not rewritten and must still be retained conservatively by bounded
readers. Retrofitting their metadata is a separate operation.

New temporal versions record both `watertown.timestamp_column` and
`watertown.timestamp_unit` in their logical attributes. The `jsonlogs` reader
uses explicit RFC3339 or microsecond metadata to prove safe event-time mapping
through timestamp-local SQL. Other encodings and indexed legacy versions
without unit proof remain conservative; matching a column name alone does not
authorize pruning. Committed historical metadata is not modified.

<!-- docs:reference factory:logfile-ingest -->
<!-- docs:end-reference -->
