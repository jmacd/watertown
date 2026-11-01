---
title: Rotating log-file ingestion
doc_id: factory:logfile-ingest
---

# Rotating log-file ingestion

## Overview

`logfile-ingest` reads rotating host log files and stores raw series in a writable pond. Archived files are immutable inputs; the active file is append-only.

## Example configuration

{{ source path="/docs/examples/logfile-ingest/default.yaml" language="yaml" /}}

For OTLP JSON, set `timestamp_field: timeUnixNano` and
`timestamp_unit: nanoseconds` together. Supported units are listed below.

## Use and behavior

Install with `pond mknod logfile-ingest /etc/logs --config-path logs.yaml`, then run `pond run /etc/logs -- push` (the default operation). `b3sum` reports source checksums; `pull` does nothing.

Timestamp extraction searches recursively for the configured JSON key and records outward-rounded microsecond bounds. Without bounds, downstream temporal reduction may recompute full history. Raw log bytes do not imply one Arrow schema; query them through an appropriate format URL.

<!-- docs:reference factory:logfile-ingest -->
## Full configuration

### Settings

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `active_pattern` | string | yes | not specified | Glob pattern for the active (append-only) log file Example: "/var/log/casparwater.json" |
| `archived_pattern` | string | yes | not specified | Glob pattern for archived (immutable) log files Example: "/var/log/casparwater-*.json" |
| `pond_path` | string | yes | not specified | Destination path within the pond (relative to pond root) Example: "logs/casparwater" |
| `timestamp_field` | string &#124; null | no | null | JSON key holding the event time of a record, looked up at any depth in each line. Example: "timeUnixNano" for OTLP metrics JSON.  Absent by default, which leaves versions without event-time bounds. That is not free: temporal-reduce reads these bounds to decide which cached segments a new source version can have touched, and a version without them is taken to span all of time (SourceRange::UNKNOWN), so every build unseals every segment and recomputes the whole history from source. Setting this is what keeps an incremental build incremental. |
| `timestamp_unit` | Timestamp Unit | no | "microseconds" | Unit of the timestamp_field values. OTLP's timeUnixNano is nanoseconds; journald's __REALTIME_TIMESTAMP is microseconds. |

### Timestamp Unit

Unit of the values found under timestamp_field.

Accepted value: "seconds" &#124; "milliseconds" &#124; "microseconds" &#124; "nanoseconds".
<!-- docs:end-reference -->
