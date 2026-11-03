---
title: Transactional site analytics rollups
doc_id: factory:analytics-rollup
---

# Transactional site analytics rollups

`analytics-rollup` replaces complete UTC-day partitions of hourly **reported
page views**. It consumes one site's inventory-aware `logfile-ingest` destination.
It never uses a timestamp watermark to decide which records are new.

The implementation is available but **not deployed**. Select and approve the
repair horizon from source retention and ingestion lag before installing it.
This configuration is illustrative, not an approved production policy:

{{ source path="default.yaml" language="yaml" /}}

`repair_days` is required and counts UTC calendar days including today. A dirty
day before that horizon fails without publishing progress. Each site/environment
needs separate source and target directories. Source and target must not overlap.

```sh
pond mknod analytics-rollup /etc/analytics --config-path analytics.yaml
pond run /etc/analytics -- push
```

## Membership, replacement, and progress

The source's committed `_inventory.json` defines the exact accepted segment
prefixes. Normal runs compare these with the last consumed inventory, seek to
new byte ranges, validate their record identities, and find dirty UTC days.
The complete affected days are grouped by `(timestamp, page_uri)`, including
equal timestamps, backward clock movement, and events ingested late.

Every nonempty replacement is a new immutable Parquet object. One versioned
`manifest.json` selects the day objects and records the source inventory,
source-state identity, semantic recipe identity and publication time.
The caller's pond transaction commits objects, membership and progress together.
An error must abort that transaction; do not commit staged writes after failure.
Empty replacements remove old day membership. Old objects remain available to
prior snapshots; this factory does not delete them.

The recipe includes the versioned grouping semantics, source/site identity and
exact URI exclusions. Changing exclusions requires an approved full rebuild;
unrecomputed history is never relabeled with a new recipe.

No-change runs write nothing, preserving original freshness. A source-inventory
change with no new records can advance membership without creating count objects.
`collection_coverage` remains **unknown**: source completeness is not proof of
collector uptime or successful browser delivery.

## Historical repair

First display the complete rebuild plan, including UTC dates and its token:

```sh
pond run /etc/analytics -- plan
pond run /etc/analytics -- rebuild --plan TOKEN
```

Planning writes nothing. Approval applies to the exact source inventory, prior
publication, target, recipe and day set. A changed source/publication invalidates
the token; plan again. Rebuild requires all listed raw segments, including the
history needed to remove excluded counts. Missing/conflicting source membership
blocks repair instead of publishing an incomplete replacement.

## One logical table

Install [analytics-table](analytics-table.md) to expose the selected partitions.

Use `series:///analytics/views` in queries and Sitegen exports. Its schema is
`timestamp` (UTC microseconds), `page_uri` (UTF-8), and `page_views` (UInt64).
It selects only current day objects, not a glob of retained historical objects.
An empty published manifest is a typed empty table; an absent manifest is an
error. The node is read-only and supports typed queries/exports, not raw byte
reads. Providers obey the pond's stale/closed transaction checks.

## Work bounds and remaining gates

Output records are streamed in timestamp/URI order, one partition at a time.
Discovery reads only new raw suffixes, but parses the current and previous
membership manifests and inspects raw-file metadata on changed runs. Reports
include inventory bytes, prior partition count, source segments inspected,
records examined for changes, and replaced dates. These are discovery metrics,
not total aggregate-query I/O metrics.

Day queries use the existing typed-format cache and lower-bound source pruning,
plus exact half-open UTC predicates. A cold cache can decode later versions too;
the current provider does not have an upper-bound cache-warming filter.
Measure retained-history metadata and cache costs on staging before claiming
constant-time tail discovery or a strict day-only I/O bound.

The existing append-only materialization sink is unchanged. This publication
path does not yet satisfy the separate dictionary-encoding lifecycle requirement:
maintenance/import encoding, backup/rebuild operations, outage intervals,
presentation and production enablement remain subsequent work.

<!-- docs:reference factory:analytics-rollup -->
<!-- docs:end-reference -->
