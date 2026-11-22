# First-party site analytics

**Status:** opt-in browser instrumentation and Caddy-to-pond collection/query
configuration exist. A two-site `analytics-staging` pond runs on Watershop
(independent `/analytics/caspar` and `/analytics/noyo` trees, beacon routes,
ingestion, inventory copy from site-staging, two bounded Sitegen reports),
gated by `staging_analytics_enabled` in the enclosing repository's
`analytics` branch. Production collection is not enabled. Public reporting
and production ownership/deployment decisions remain pending. Platform defects
found while staging this work are tracked in
[Platform defects found while staging](#platform-defects-found-while-staging).

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
`_analytics/pages.parquet` inventory with non-null UTF-8 `site_id`, `page_uri`,
and `section` columns, including when there are zero rows. Copy the inventory
into the analytics pond using ordinary `pond copy host:///...`, not a table or
series copy; `file+table:///analytics/pages.parquet` reads the current version
of the replicated file, not a host registry or historical replacements.
The optional `config/analytics-public.yaml` joins current inventory
to daily counts by site identity and canonical path and exposes only
`timestamp`, `section`, and `views`. It deduplicates inventory keys so duplicate
rows cannot inflate counts; conflicting section labels fall into Other.

Classification uses the current inventory: changed assignments reclassify the
displayed history, and retired or otherwise unrecognized paths move to Other.
It does not use a client-supplied section or assume that a claimed catalog ID
proves membership. A valid zero-row inventory explicitly removes all recognized
pages, putting retained views into Other. A missing or unreadable inventory is
an unavailable report, not evidence that every page is unknown.

Show the last 30 UTC calendar days and mark today as partial. Do not silently
fill collection gaps with zero. The public projection limits dates but does not
invent missing daily rows or establish collection coverage. The Sitegen
`viz renderer="stacked-area"` shortcode renders this shape using the existing
Parquet/DuckDB/Vega-Lite path. The opt-in template at
`config/analytics/site/overview.md` in the enclosing repository supplies the
section order and reporting explanation. Publication/import wiring, collection
coverage facts, and automated inventory transfer remain pending.

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

## Platform defects found while staging

Staging analytics on Watershop meant wiring `analytics-staging` into selfmon,
which exposed selfmon and query-engine defects. Track them here so analytics
work can resume without losing them. Status as of 2026-11-21.

| Item | Status |
|---|---|
| [jmacd/watertown#224](https://github.com/jmacd/watertown/pull/224): selfmon incremental reads and rollup backlog | Merged, deployed (`0.305.224`). Selfmon limiters dropped from about 530 s and 6.6 GB per tick to about 30 s and 0.5 GB after one catch-up tick. |
| [jmacd/watertown#225](https://github.com/jmacd/watertown/pull/225): `verify_append_prefix` failed on `Utf8View` vs `Utf8` | Merged, deployed (`0.306.225`). `materialize-azure-access` succeeds. |
| [jmacd/watertown#226](https://github.com/jmacd/watertown/pull/226): `temporal-reduce` did not declare its event-time axis | **Merged; deployed as `pond 0.307.226`.** Without it, `pump-state` over `/reduced/well-depth` warned "unproven event-time axis" and rebuilt from full history on every run (water-staging, site-staging). Confirmed gone on both after deploy. Sitegen time did not change (site-staging 730–900 s before and after); `pump-state` was not the bottleneck. Production runs an older image and would have regressed on promotion without this fix. |
| Selfmon `materialize-perf` rejected as a retroactive change | The new `input_event_time` contract changed the perf source rows, which the append-only target rejects. Fix: retarget to `/metrics/perf.r2.series` (outer `config/watershop-selfmon.yaml`, committed; deployed 2026-11-20 18:07Z, first `perf.r2` success **not yet confirmed**). Pond cannot delete a series. |
| Selfmon `materialize-azure-access` verifies full history every tick (about 6 min, 6 GB) | Journal versions written before #224 record event-time bounds but not `watertown.timestamp_column`, so `event_time_axis_for_url` cannot prove the axis and lineage is never timestamp-local. It never self-heals. Options: reset the throwaway selfmon pond, or treat unlabeled bounded versions as the ingest factory's default column. **Undecided.** |
| Water monitor (`/system/run/00-monitor`) crashed with "No field named system_pressure_value" during the Nov 18 pumphouse outage | Each check reads its source bounded to its window. Before [jmacd/watertown#224](https://github.com/jmacd/watertown/pull/224) a bounded cached read took its schema only from the versions inside the window. Other metrics kept arriving during the outage, so the 5m window held versions with no pressure column, and the SQL error aborted the whole report. #224 made schema membership independent of pruning, which fixes that case; production still runs an older image. A column absent from every version (a new or reset pond, or a sensor not yet reporting) still aborted the report. Fix on `jmacd/analytics_5` (**PR pending**): a missing column counts as no samples (the check reports `unknown`, with a warning) and the other checks publish. `testsuite/tests/215-monitor-report-data-gap.sh` covers both cases end to end; it fails without the fix with the production error. |
| Site sitegen reuse cost: `/sources/water/reduced/*/res=1m.series` 60–117 s each | Diagnosed on watershop: export reuse itself takes under 0.2 s. The cost is temporal-reduce source discovery at the finest level. Globbing the 121 `casparwater*.json` files took 64 s through the site import (two record queries per directory child). Each file was then re-resolved from the root and its versions listed one query at a time. All five water reduces (six septic) repeated this. Fix on `jmacd/analytics_5` (**PR pending**): directory children of physical types load without queries, version listing is batched per partition, and glob resolution and discovery are memoized per transaction, so reduces that share a source pattern discover it once. Measure site-staging sitegen time after deploy. |
| `cargo audit`: `quick-xml` 0.39.4 (RUSTSEC-2026-0194, -0195) | Pulled in only by `object_store` 0.13; the fix needs `object_store` 0.14, which DataFusion 55.3 and deltalake 1.1.4 do not yet accept. DoS only, from trusted S3/Azure responses. Option: ignore both in `.cargo/audit.toml` until upstream moves. **Undecided.** |

Operational findings, no action needed:

- The 2026-11-18 well-depth gap (10:44Z to 19:03Z) was a pumphouse power loss.
  The well pump is not controlled by the pumphouse host.
- The gateway's NFS server (UniFi appliance) stalls for 2 to 4 minutes at about
  02:03 and 03:03 Pacific each night, causing export retries and duplicate points
  but no gaps.
- Site sitegen is dominated by reuse checks, not by writing. In one site-staging run,
  `/sources/water/reduced/*/res=1m.series` took 60–117 s each while reusing 1,522 of
  1,523 partitions. The analysis stage (`drawdown-by-month`, `horner-by-month`) and the
  noyo `res=1h` params take 35–100 s each. site-prod (about 1,400 s, sometimes about
  470 s) is slower than site-staging (730–900 s). Their timers overlap at :41–:42, so
  they compete for the host.
