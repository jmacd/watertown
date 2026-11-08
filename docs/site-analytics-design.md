# First-party site analytics

**Status:** opt-in browser instrumentation and Caddy-to-pond collection/query
configuration exist. Collection is not deployed or enabled. Public reporting
and ownership/deployment decisions remain pending.

Read [Architecture for developers](manual/guides/architecture.md) before
extending this path. Analytics should be configuration over existing services,
not another server, storage protocol, or application-owned cache.

## Current path

```text
first-visible browser beacon
  -> dedicated per-site/environment Caddy log
  -> periodic ordinary logfile-ingest into the owning pond
  -> jsonlogs and SQL-derived nodes
  -> existing Sitegen/report/export facilities (integration pending)
```

The outer repository contains the concrete configuration:

- `config/caddy/beacons.caddy`: named, privacy-filtered rotating beacon logs.
- `config/analytics.yaml`: ordinary ingestion, typed observations, daily counts.
- `README.operations.md`: opt-in setup and operational boundaries.
- `config/scripts/test-beacons.py`: Caddy routing and pond ingestion coverage.

The old standalone collector, inventory protocol, collector-specific format,
custom rollup/table factories, and host report-state command have been removed.
There is no compatibility adapter or second ingestion path. This replaces the
earlier unshipped design rather than migrating deployed analytics data.

## Requirements retained

The metric is **reported page views**, not visitors, unique readers, or verified
human visits. One top-level document makes one best-effort attempt when first
visible and no longer prerendering. Tab returns and back/forward-cache restores
do not repeat that attempt; reloads create a new document.

No cookies, visitor identifiers, client timestamps, referrers, retries, or
persistent browser queues are introduced. Failed requests must not interfere
with page use. Requests remain forgeable and coverage is not complete.

Sitegen supplies the canonical page path, not `location.href`. Query strings
and fragments from the visitor's current URL are not copied into a beacon.
Instrumentation is off by default; each subsite opts in independently.
Frontmatter and configuration exclusions remain supported.

Noyo remains independent of Caspar Water, including while mounted under
`/noyo-harbor/`. Every site/environment needs a distinct beacon endpoint and
source log. Site identity comes from trusted Caddy configuration, not a
client-supplied routing field. Separate endpoints are required because there
is no collector catalog registry resolving a shared endpoint.

## Collection and ingestion contracts

The browser sends a bodyless POST with URL-encoded `page_uri` and `catalog_id`.
Caddy checks basic shape, selects the site's named logger, and responds with
HTTP 204 and `Cache-Control: no-store`. This does not authenticate a page or
catalog and does not promise fsync-before-response durability.

Beacon requests do not also enter the ordinary access log. The dedicated log
removes the request object, response headers, and authenticated-user field.
Only selected fields are retained; unknown request parameters are not logged.
Caddy's default fractional Unix-second `ts` is accepted by ordinary ingestion,
which records conservative event-time bounds. No custom timestamp format,
event parser, or collector journal is needed.

Original Caddy logs remain recoverable source material. Ordinary
`logfile-ingest` handles appends and rotation and records event-time bounds.
Preserve source files until an explicit backup/retention policy permits
removal. The supplied Caddy snippet does not delete archives; disk monitoring
and retention approval are deployment prerequisites.

## Pond queries, not a parallel store

The example imports source versions under `/analytics/raw`, exposes typed
accepted observations at `/analytics/views`, and groups daily counts at
`/analytics/daily`. These are ordinary pond paths and query factories.
The typed projection floors timestamps to whole seconds, preserving UTC day
membership without requiring subsecond precision for page-view counts.
Parsed source versions use the shared format cache.

Daily counts use `temporal-reduce-series` with `group_by: [site_id, page_uri]`.
Grouping is a general temporal-reduction capability, including shared partial
aggregates, cache reuse, and late-data recomputation. There is no analytics-owned
materialization, cache, invalidation, or compaction machinery.

The publishing site pond can import this pond as a data source, as it does water
and septic. Sitegen should export only the public-window derived results to
Parquet and render them through normal templates. That publication wiring is
still pending; the analytics pond does not require its own publishing service.

Choose the owning pond explicitly and run ingestion there, never through an
imported read-only mount. A site-building pond may only be a consumer.
Durable shared publication facts belong in the pond; exports and local query
caches do not become independent authoritative report state.

## Public reporting boundary

The approved disclosure is daily per-page counts and daily site totals over
30 UTC calendar days, including today. Hourly detail and raw events must not
be exported through the report or an unrestricted public query interface.
Public report data must not include source paths, internal errors, or catalog
identifiers. The public publisher catalog is separate from report output.

### At-a-glance overview

The primary presentation is a stacked area chart of **daily reported page
views**, not a running cumulative total. The horizontal axis is the UTC day;
the total height is that day's count, and colored bands show the contributions
from sections such as blog, static content, monitoring, and Noyo. Individual
page detail is secondary.

The overview derives `(UTC day, section, reported views)` from daily per-page
counts and publisher-controlled section assignments. Each included view belongs
to exactly one section, so the stack adds up without double-counting. Noyo may
appear as a section in the enclosing site's overview while retaining its
independent source identity and dataset.

The agreed sections are:

- Noyo: all recognized Noyo pages, including its blog and monitoring pages.
- Blog: recognized main-site `/blog` pages.
- Monitoring: recognized main-site `/data`, `/analysis`, `/septic`, and `/explore` pages.
- Static content: the remaining recognized main-site pages.
- Other / unclassified: unrecognized paths, counted without publishing their names.

Sitegen emits publisher-controlled sections in its page catalog and a flat
`_analytics/pages.jsonl` inventory. Copy the inventory into the analytics pond
using ordinary `pond copy`; queries read the replicated pond copy, not a host
registry. The optional `config/analytics-public.yaml` joins current inventory
to daily counts by site identity and canonical path and exposes only
`timestamp`, `section`, and `views`. It deduplicates inventory keys so duplicate
rows cannot inflate counts; conflicting section labels fall into Other.

Classification uses the current inventory: changed assignments reclassify the
displayed history, and retired or otherwise unrecognized paths move to Other.
It does not use a client-supplied section or assume that a claimed catalog ID
proves membership. A missing or unreadable inventory is an unavailable report,
not evidence that every page is unknown.

Show the last 30 UTC calendar days and mark today as partial. Do not silently
fill collection gaps with zero. The public projection limits dates but does not
invent missing daily rows or establish collection coverage. Chart rendering,
coverage presentation, and automated inventory transfer remain pending.

Missing observations are not zero, and maximum observed event time is not
proof of collection uptime. Coverage remains unknown unless independently
established. Failed refreshes must not fabricate freshness or prevent core
content publication. If last-good data is published, retain its original
freshness and apply the current disclosure window.

The next integration must establish:

- The owning pond and import/publication flow for each site.
- How existing Sitegen report/export facilities consume the query results.
- Which publication/freshness facts need durable pond ownership.
- Whether historical catalog snapshots are needed for labels, and who owns
  those facts; no host-only catalog registry is assumed.

The shared Parquet dictionary policy remains independent work, with lifecycle
coverage over ordinary pond series, maintenance, restore, and export. Keeping
that policy does not require the removed analytics storage design.
