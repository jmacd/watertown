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

<!-- docs:reference factory:logfile-ingest -->
<!-- docs:end-reference -->
