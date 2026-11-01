---
title: Systemd journal ingestion
doc_id: factory:journal-ingest
---

# Systemd journal ingestion

## Overview

`journal-ingest` collects JSON Lines from the systemd journal into per-unit streams. It requires Linux, `journalctl`, and permission to read the selected journals.

## Example configuration

{{ source path="/docs/examples/journal-ingest/default.yaml" language="yaml" /}}

For example, `extra_args: [--merge]` includes journals from other namespaces
available to the host command.

## Use and behavior

Install with `pond mknod journal-ingest /etc/journal --config-path journal.yaml`, then run `pond run /etc/journal -- push`. `status` reports the persisted cursor and per-unit counts; `pull` is a no-op.

The cursor is stored beneath the output path as `.journal-cursor`. Removing it changes incremental behavior. Default timestamps are journald microseconds. Query collected files using `jsonlogs`, explicitly casting timestamp and numeric fields as needed.

<!-- docs:reference factory:journal-ingest -->
## Full configuration

### Settings

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `collect_kernel` | boolean | no | true | Whether to collect kernel ring buffer as a separate stream (default: true) |
| `extra_args` | array of string | no | [] | Extra command-line arguments appended to every journalctl invocation.  Useful for e.g. ["--merge"] to pull both the system journal and the per-user journal of the invoking user (which is what surfaces user-scope units like pond@water-prod.service to the ingester).  Default: empty. |
| `journalctl_command` | string | no | "journalctl" | Command to invoke journalctl (default: "journalctl") Can be customized for non-standard installs |
| `pond_path` | string | yes | not specified | Destination path within the pond (relative to pond root) Example: "logs/watershop" |
| `timestamp_field` | string | no | "__REALTIME_TIMESTAMP" | JSON field name containing the timestamp (default: "__REALTIME_TIMESTAMP") The value is interpreted as microseconds since Unix epoch. |
<!-- docs:end-reference -->
