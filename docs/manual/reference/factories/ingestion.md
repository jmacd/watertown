---
title: Ingestion factories
doc_id: reference:ingestion
---

# Ingestion factories

These require a real writable pond. Install using `pond mknod FACTORY
/etc/NAME --config-path FILE`, then use the factory-specific `pond run`
operation below. `pond list` / `describe` / `cat` inspect the produced data.
Shape validation does not prove external access, credentials, or data quality.

## Git

`git-ingest` exposes a `DirectoryDynamic` from a tracked Git tree:

```yaml
url: https://github.com/jmacd/watertown.git
ref: main
prefix: docs
```

`pond run /etc/git -- pull` fetches/resolves the ref; `status` reports sync
state. `push` is a no-op, not an upstream Git push. Reads can lazily initialize
the bare repository cache under the pond's `git` directory. Network/SSH access
and credentials are operator prerequisites, not embedded in this example.
`prefix` is optional, nonempty when present, with no leading/trailing slash
or `..`. The external key is `ref`, never `git_ref`. Files remain backed by
the cache; this is not physical copying of every tree entry into pond storage.

<!-- docs:reference factory:git-ingest -->
## Configuration reference

Generated from the Rust **deserialization** contract. This describes shape, not all semantic validation, service prerequisites, or successful execution.

| Capability | Registered |
|---|---|
| directory | yes |
| file | no |
| initialize | yes |
| execute | yes |
| table transform | no |
| raw validation | no |

Automatic post-commit access: `ReadWrite`.

### GitIngestConfig

Configuration for the git-ingest factory

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `prefix` | string &#124; null | no | null | Optional path prefix filter.  When set, only files under this directory in the git tree are synced, with the prefix stripped from their paths. |
| `ref` | string | yes | not specified | Git ref to track (branch name, tag, or SHA) |
| `url` | string | yes | not specified | Git remote URL (HTTPS or SSH) |

Unknown fields: **rejected**.
<!-- docs:end-reference -->

## Rotating log files

`logfile-ingest` reads host files and stores raw series in the pond:

```yaml
archived_pattern: /var/log/example-*.jsonl
active_pattern: /var/log/example.jsonl
pond_path: logs/example
timestamp_field: timeUnixNano
timestamp_unit: nanoseconds
```

Run `pond run /etc/logs -- push` (also the default). Archived files are treated
as immutable; active files as append-only. Bao/BLAKE3 state tracks changes.
Do not edit or truncate an input expecting arbitrary replacement semantics.
`b3sum` prints source checksums; `pull` does nothing. Read permissions and
stable rotation behavior are prerequisites.

Timestamp extraction finds the configured key recursively in JSON lines and
records outward-rounded microsecond bounds. Omitting it leaves unknown bounds,
which can force downstream full-history reduction. Raw log bytes do not imply
a universal Arrow schema: query through the appropriate format URL.

<!-- docs:reference factory:logfile-ingest -->
## Configuration reference

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

## Systemd journal

`journal-ingest` needs Linux, `journalctl`, and journal read permissions:

```yaml
pond_path: logs/journal
collect_kernel: true
extra_args: [--merge]
```

Run `pond run /etc/journal -- push`; `status` reports the persisted cursor and
per-unit counts. `pull` is a no-op. Collection groups JSON Lines by systemd
unit, with kernel and unclassified streams. The cursor lives under the output
path as `.journal-cursor`; removing it changes incremental collection behavior.
Default timestamps are journald microseconds. Query raw files using `jsonlogs`,
then explicitly cast timestamps/numeric fields for SQL.

<!-- docs:reference factory:journal-ingest -->
## Configuration reference

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

### JournalIngestConfig

Configuration for the journal ingestion factory

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `collect_kernel` | boolean | no | true | Whether to collect kernel ring buffer as a separate stream (default: true) |
| `extra_args` | array of string | no | [] | Extra command-line arguments appended to every journalctl invocation.  Useful for e.g. ["--merge"] to pull both the system journal and the per-user journal of the invoking user (which is what surfaces user-scope units like pond@water-prod.service to the ingester).  Default: empty. |
| `journalctl_command` | string | no | "journalctl" | Command to invoke journalctl (default: "journalctl") Can be customized for non-standard installs |
| `pond_path` | string | yes | not specified | Destination path within the pond (relative to pond root) Example: "logs/watershop" |
| `timestamp_field` | string | no | "__REALTIME_TIMESTAMP" | JSON field name containing the timestamp (default: "__REALTIME_TIMESTAMP") The value is interpreted as microseconds since Unix epoch. |

Unknown fields: **rejected**.
<!-- docs:end-reference -->

## HydroVu

`hydrovu` requires HydroVu OAuth credentials, API access, and real device IDs:

```yaml
client_id: "${env:HYDROVU_CLIENT_ID}"
client_secret: "${env:HYDROVU_CLIENT_SECRET}"
max_points_per_run: 1000
hydrovu_path: /hydrovu
devices:
  - name: example-device
    id: 123
```

Run `pond run /etc/hydrovu -- collect`. Collection stores per-device physical
series with timestamps and parameter/unit-named value columns; source API
metadata controls those names. Incremental collection resumes from existing
temporal data. Optional `archive_path` supplies read-only seed snapshots only
when the writable device has no temporal data; it is not an output target.
Inactive devices are skipped intentionally. API failures are not an offline
success. Use factory `--help` for archive operations before touching real data.

<!-- docs:reference factory:hydrovu -->
## Configuration reference

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

### HydroVuConfig

Configuration for HydroVu OAuth credentials and device list

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `archive_path` | string &#124; null | no | null | Optional path within pond holding read-only seed archives (e.g. "/hydrovu-archive"), typically git-ingested FileDynamic Parquet .series snapshots laid out as {archive_path}/devices/{id}/*.series. Consulted by find_youngest_timestamp ONLY when the writeable device directory has no temporal data (a freshly reset pond), so live collection resumes from the archive's youngest timestamp instead of crawling from epoch.  These archives are NOT written to. |
| `client_id` | string | yes | not specified | OAuth client ID (sensitive - redacted from debug output) |
| `client_secret` | string | yes | not specified | OAuth client secret (sensitive - redacted from debug output) |
| `devices` | array of HydroVuDevice | yes | not specified | Device list |
| `hydrovu_path` | string | yes | not specified | Path within pond for hydrovu data (e.g., "/hydrovu") |
| `max_points_per_run` | integer | yes | not specified | Constraints: format="uint"; minimum=0. |

Unknown fields: **rejected**.

### HydroVuDevice

Device configuration specifying which HydroVu location to collect

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `active` | boolean | no | true | When false, device is skipped during collect. Default: true. |
| `comment` | string &#124; null | no | not specified |  |
| `id` | integer | yes | not specified | Constraints: format="int64". |
| `name` | string | yes | not specified |  |

Unknown fields: **rejected**.
<!-- docs:end-reference -->

## Updating and coverage

Repeat `mknod ... --overwrite` to update a config. Existing output, cursors,
caches, and remote side effects are not reset by that command. Plan changed
paths/recipes explicitly. This page's examples are prerequisite-dependent
configuration samples, **not offline end-to-end coverage**. Original Git,
log, and API sources remain authoritative; keep them independently recoverable.
See [shared rules](../concepts.md) for substitution and [operations](../../guides/operations.md).
