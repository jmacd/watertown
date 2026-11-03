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

## Strict collector ingestion

Opt in per site/environment; existing log users are unchanged:

```yaml
archived_pattern: /var/lib/watertown-analytics/noyo-staging/views-*.jsonl
active_pattern: /var/lib/watertown-analytics/noyo-staging/views.jsonl
pond_path: /analytics/raw
timestamp_field: received_at_us
timestamp_unit: microseconds
analytics_site_id: noyo
```

This mode requires the exact collector filenames above, a matching version-one
`inventory.json`, and read access to `.rotation.lock`. It takes the collector's
shared rotation lock (a busy lock fails visibly for a later retry), validates
the predecessor chain, every record sequence, byte length, checksum, and site,
then releases the lock **before** pond writes. The active input is limited to
the inventory's published durable prefix; in-flight tails wait for the next run.

A private anonymous scratch file holds the validated snapshot. Memory and open
file counts are bounded; temporary disk usage and host scanning are proportional
to retained source bytes. Provision scratch space for a full source snapshot.
Rotation can wait during the host scan, so measure this interval on staging.

Pond files use `views-<segment_id>.jsonl` from their first append, including the
currently active segment. Rename, equal/backward timestamps and multiple missed
rotations cannot duplicate a prefix. A new append receives exact microsecond
event bounds for the typed `timestamp` column. The factory stores `_inventory.json`
in the same pond transaction as its raw appends. No-change runs write nothing.
Do not commit a transaction after the factory reports an error. `b3sum` is
unavailable in this mode because its host-filename mapping no longer applies;
the strict ingestion pass verifies the inventory instead.

Missing archives, changed closed history, conflicting prefixes, or unknown pond
membership fail visibly. Extra/duplicate `.jsonl` files in the host directory
are explicitly rejected: reconcile them outside the collector directory before
retrying. Restoring the original missing archive permits a safe retry. There is
no automatic source retention or deletion; all listed history must be available.

Query the resulting raw series through `site-analytics://`, **not** `jsonlogs://`.
An empty validated inventory is recorded even before the first raw series exists.
Inventory checkpoints prove source membership, not collection uptime: outage
coverage intervals, backups, ingestion scheduling, and rollup publication remain
separate operational work. Keep each site/environment's destination isolated.

<!-- docs:reference factory:logfile-ingest -->
<!-- docs:end-reference -->
