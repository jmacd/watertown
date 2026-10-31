---
title: Rotating log-file ingestion
doc_id: factory:logfile-ingest
---

# Rotating log-file ingestion

## Overview

`logfile-ingest` reads rotating host log files and stores raw series in a writable pond. Archived files are immutable inputs; the active file is append-only.

## Example configuration

```yaml
archived_pattern: /var/log/example-*.jsonl
active_pattern: /var/log/example.jsonl
pond_path: logs/example
timestamp_field: timeUnixNano
timestamp_unit: nanoseconds
```

## Use and behavior

Install with `pond mknod logfile-ingest /etc/logs --config-path logs.yaml`, then run `pond run /etc/logs -- push` (the default operation). `b3sum` reports source checksums; `pull` does nothing.

Timestamp extraction searches recursively for the configured JSON key and records outward-rounded microsecond bounds. Without bounds, downstream temporal reduction may recompute full history. Raw log bytes do not imply one Arrow schema; query them through an appropriate format URL.

<!-- docs:reference factory:logfile-ingest -->
## Full configuration

Generated from the Rust **deserialization** contract. This describes shape, not all semantic validation, service prerequisites, or successful execution.

| Capability | Registered |
|---|---|
| directory | no |
| file | no |
| initialize | yes |
| execute | yes |
| table transform | no |
| raw validation | no |

Automatic post-commit access: `ReadWrite`.

### LogfileIngestConfig

Configuration for the logfile ingestion factory

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `active_pattern` | string | yes | not specified | Glob pattern for the active (append-only) log file Example: "/var/log/casparwater.json" |
| `archived_pattern` | string | yes | not specified | Glob pattern for archived (immutable) log files Example: "/var/log/casparwater-*.json" |
| `pond_path` | string | yes | not specified | Destination path within the pond (relative to pond root) Example: "logs/casparwater" |
| `timestamp_field` | string &#124; null | no | null | JSON key holding the event time of a record, looked up at any depth in each line. Example: "timeUnixNano" for OTLP metrics JSON.  Absent by default, which leaves versions without event-time bounds. That is not free: temporal-reduce reads these bounds to decide which cached segments a new source version can have touched, and a version without them is taken to span all of time (SourceRange::UNKNOWN), so every build unseals every segment and recomputes the whole history from source. Setting this is what keeps an incremental build incremental. |
| `timestamp_unit` | TimestampUnit | no | "microseconds" | Unit of the timestamp_field values. OTLP's timeUnixNano is nanoseconds; journald's __REALTIME_TIMESTAMP is microseconds. |

Unknown fields: **rejected**.

### TimestampUnit

Unit of the values found under timestamp_field.  tlogfs records event times in microseconds, so everything is converted to that. Microseconds is the default because it is what tlogfs itself uses.

Accepted value: "seconds" &#124; "milliseconds" &#124; "microseconds" &#124; "nanoseconds".
<!-- docs:end-reference -->

## Evidence and limitations

The sample is configuration guidance, not a claimed runnable documentation fixture. The generated schema is the deserialization contract; semantic validation, source data, platform support, credentials, and external side effects still apply. Keep original source data independently recoverable.
