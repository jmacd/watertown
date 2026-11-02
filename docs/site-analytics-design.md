# First-party site analytics: design and implementation

> **Status: proposal, not implemented.** This document specifies a small,
> first-party analytics pipeline owned end-to-end by Sitegen and Watertown.
> Proposed configuration keys, factory names, endpoint paths, and schemas below
> are design interfaces, not currently available commands. Deployment and
> retention choices require approval before implementation in production.

## 1. Purpose and recommended scope

Measure interest in a small collection of generated pages without introducing
visitor tracking or a third-party analytics dependency. Sitegen knows which
pages exist and their canonical URLs; use that knowledge to identify pages
consistently from collection through storage and presentation.

The initial product is:

- One browser-reported view when an instrumented document first becomes visible.
- An hourly, per-page materialized table retaining the canonical page URI as a
  string, dictionary-encoded in Parquet.
- Daily totals and a bounded top-pages report derived from those counts.
- Explicit collection health, coverage gaps, and publication freshness.
- A static analytics page generated with the rest of the site.

Use the label **reported page views**, not visitors, unique readers, or verified
human views. Keep server **page requests** as a separate, optional diagnostic
metric; never add the two metrics together.

Do not initially implement sessions, persistent browser identifiers, cookies,
fingerprinting, geographic attribution, referrer reports, scroll tracking,
engagement duration, cross-device identity, or advertising integrations.

The important separation is:

```text
append-only observations != replaceable time-bucket counts
```

Accepted event records are immutable. An hourly count can change as ingestion
catches up, an archive is restored, or a classification rule is corrected.

## 2. Existing capabilities and actual gaps

The following is based on the checked-out source, not verification of the live
deployment. Paths in this section are relative to the Watertown root unless
prefixed with `../`.

| Component | Existing behavior | Work required |
|---|---|---|
| Sitegen routes | `crates/sitegen/src/routes.rs` expands static, content, and template routes into `PageJob`s and indexes canonical content URLs. | A complete analytics catalog across all emitted page types and recursive subsites; optional instrumentation. |
| Browser output | Sitegen controls layouts, assets, and generated HTML. | A tiny first-party script and escaped page metadata; explicit treatment of HTML passthrough and documentation layouts. |
| Collection endpoint | Static hosting is configured through Caddy in the outer repository. | A bounded, independently supervised HTTP collector writing local logs. No event-by-event pond writes. |
| Rotating ingestion | `crates/provider/src/factory/logfile_ingest/` ingests active files and immutable rotations. Configured integer JSON timestamps provide event-time bounds and active-record alignment. | A tested collector rotation contract, permissions, retention, and recovery procedure. |
| JSON parsing | `crates/provider/src/format/jsonlogs/` exposes top-level JSON fields as nullable strings. Nested values remain JSON text. It buffers parsed input and warns when skipping malformed records. | Explicit typing and strict accounting for collector records; measure memory and avoid accepting undetected event loss. |
| Existing web experiment | `../config/watershop-selfmon.yaml` ingests Caddy JSON and defines `watershop-pageviews`, counting requests by hour and status. | This is not a page-view classifier and is not exported by the current selfmon Sitegen configuration. Do not copy it as the finished design. |
| Temporal reduction | `crates/provider/src/factory/temporal_reduce/` has time-bucket aggregation, source-version caches, and late-data invalidation. | Its public configuration does not supply arbitrary grouping dimensions such as `page_uri`. A long-form page table is not a YAML-only application of that factory. |
| Append materialization | `crates/provider/src/factory/materialize_series/` selects timestamps strictly greater than the target maximum. | Unsafe for changing aggregates and for late/equal-timestamp events. Do not use this as the analytics checkpoint. |
| Replacement foundation | `crates/query-foundation/src/materialize.rs` models `Replace` publication and atomic output/progress. `frontier.rs` classifies late changes and repair policies. | `TinyFsMaterializationSink` in `crates/provider/src/query_foundation_adapter.rs` explicitly rejects replacement ranges. A production replacement sink still needs implementation. |
| Physical encoding | The deterministic table pack writer in `crates/sync-store/src/content/series_pack_builder.rs` explicitly disables dictionary encoding and compression. | An explicit, compatible dictionary policy and proof that maintenance/import preserve the requirement. |
| Publication | `../config/scripts/run.sh` builds and publishes the site using versioned build directories and a `current` symlink. | Feed it a successful aggregate snapshot without making analytics failure block core site content. |

The cloud Caddy configuration currently has no access-log directive. The
Watershop Caddy configuration writes rotating JSON logs. Production configuration
also provisions a native `site-prod` runtime on the serving host, so collection
and ingestion can be local; confirm the effective runtime before deployment.

The older [web-log design](archive/weblog-ingestion-design.md) covers access-log
parsing. This proposal extends the goal to publisher-defined page identities,
browser events, and durable aggregate publication.

## 3. Alternatives and why use a beacon

| Source | Advantages | Limitations |
|---|---|---|
| Server access logs | No browser code or new collection endpoint; excellent operational evidence. | Includes crawlers and non-page requests; cached navigation may not request HTML; URL aliases require classification. |
| Browser beacon | Carries Sitegen's canonical URI directly; separates a page view from chart/data requests; reports ordinary cached document loads. | JavaScript and network dependent; blocked by some clients; forgeable; not proof of a human reader. |
| Third-party analytics | Collection and dashboards are already operated. | External dependency, less control over data handling, and little reuse of Watertown. |
| Tracking image | Can work without JavaScript. | Visibility semantics are weak; caching and speculative loads complicate counts; puts event fields in a URL. |

Recommend a minimal first-party beacon for audience-interest counts, with
access-log analysis optional and separately named.

Here, **beacon** means a small browser-to-server analytics event. It does not
require `navigator.sendBeacon()` specifically. Prefer `fetch()` with
`keepalive: true`, `credentials: "omit"`, and `referrerPolicy: "no-referrer"`:
these provide explicit control over credentials and referrer transmission and
allow asynchronous checking of the response.

`navigator.sendBeacon()` is convenient and designed for analytics, but its
boolean return means queued, not durably recorded; it does not provide the same
request controls or an application response. Do not silently switch transports
if that weakens the privacy contract.

All browser delivery remains best-effort. Neither transport guarantees that a
view reaches the server after browser termination or network failure.

## 4. Metric definition and browser lifecycle

### 4.1 Version-one view semantics

A reported view is one accepted event generated when:

1. The document was generated with analytics enabled.
2. It has a known canonical page URI and catalog identifier.
3. It becomes visible and is not still prerendering.
4. The document has not already attempted to report its first visible view.

Use an in-memory, per-document guard. Set the guard before starting the request,
so multiple lifecycle notifications cannot issue concurrent events. Register
the necessary listeners and also check the current state, since the script can
load after the document becomes visible.

| Browser action | Version-one result |
|---|---|
| Normal navigation or reload, then visible | One attempt for the new document. |
| Open in a background tab | Wait until first visible. No event if never visible. |
| Switch away and return to the same tab | No additional attempt. |
| Restore an already-counted document from back/forward cache | No additional attempt. Its in-memory guard survives. |
| Navigate back when the browser creates a new document | One attempt when visible. |
| Prerender without activation | No attempt. |
| Prerender activation | One attempt after activation and visibility. |
| Change fragment or chart query state without a new document | No additional attempt. |
| JavaScript disabled or blocked | No beacon. Access-log requests remain a different metric. |
| Embedded frame | Do not report by default; count top-level documents only. |

This definition deliberately measures first-visible documents, not every history
traversal. Different browser cache behavior can therefore affect counts. A future
change to count back/forward-cache returns is a metric-definition change, not a
bug fix to apply invisibly.

Do not use unload handlers, synchronous requests, or navigation delays. Do not
wait for the page to close to record its view.

### 4.2 Failure and retry policy

Initially make one request attempt and no persistent retry queue. A failed
attempt must not affect navigation, charts, or rendering. Emit a concise browser
warning for a failed request or non-success response, without dumping the event
or surrounding page state. Normal visibility suppression is not a failure.

There is no event ID or visitor ID in the browser payload. Without retries,
client-side deduplication machinery is unnecessary. The in-memory guard prevents
our own normal duplicate dispatch; it cannot prevent forged or externally
replayed requests.

If retries are added later, introduce a random per-event identifier and a bounded
server deduplication policy first. Never repurpose it into a persistent browser
identifier.

## 5. Page identity: publisher-owned canonical URIs

### 5.1 Meaning of base URI

In this design, `page_uri` is the canonical absolute **path component** of the
public page URL. It includes the deployed mount prefix but excludes scheme,
host, query, and fragment:

```text
/                         home
/water                    an extensionless content page
/blog/example             a blog page
/noyo-harbor/              a subsite root
/noyo-harbor/sites/BDock    a generated subsite page
```

For a document opened at `/water.html?range=7d#t=...`, the embedded identity can
be `/water`. This is supplied at build time, not computed from `location.href`.

The canonical URI is the durable page dimension. It is not the dictionary's
integer index and not an auto-incremented page number. A genuine URL rename
creates a new identity unless an explicit reporting alias later joins them.
Titles can change without changing identity.

Keep environment separation in the collector and pond configuration. A server
configured for staging must never accept a browser field that routes events into
production. If datasets are combined later, use a server-assigned `site_id` and
environment dimension.

### 5.2 Catalog generation

Extend route expansion to produce a deterministic catalog for all instrumented
pages, not just the current content-stage URL index. Each entry includes:

| Field | Purpose |
|---|---|
| `page_uri` | Canonical, origin-relative identity. |
| `title` | Human-readable display label. |
| `aliases` | Explicit request-path aliases for optional access-log classification. |

The catalog has a schema version and a content-derived identifier. Sort entries
by canonical URI before hashing; do not include a build timestamp. Unchanged
catalog content should yield the same identifier across data-only site rebuilds.

Construct aliases from actual emitted paths and the configured serving rules.
Do not universally strip `.html`, lowercase paths, decode reserved separators,
or remove trailing slashes. These operations can merge distinct URLs.

Combine recursive subsite catalogs at the outer publishing root after applying
each mount prefix exactly once. Fail the build on conflicting canonical URIs or
aliases. The configured origin-wide collector endpoint must not accidentally
become `/noyo-harbor/_analytics/...` when rendering a subsite.

Explicit opt-out controls are needed for private/internal pages, diagnostic
pages, and previews. Do not equate "hidden from navigation" with "do not count."
Keep analytics disabled for documentation builds and preview environments unless
explicitly enabled.

HTML passthrough must have a documented contract: inject through a structured
HTML path with tests, or require an explicit opt-in marker and reject enabled
pages that cannot be instrumented. Do not insert scripts using fragile string
replacement or silently claim coverage of uninstrumented pages.

### 5.3 Markup and catalog lifecycle

Each enabled page carries escaped canonical URI and catalog metadata plus one
shared, deferred, content-addressed script. The exact HTML representation is an
implementation detail; data attributes are sufficient. Never interpolate
unescaped titles or URLs into executable JavaScript.

The deployment sequence is:

```text
build complete site and catalog
  -> validate and install catalog in collector's private registry
  -> confirm collector accepts that catalog
  -> publish the site build
```

Catalog installation is a host-side operation, not a public HTTP registration
API. Keep previous catalogs so open tabs and cached pages can still report after
a deployment. For this small site, retaining the catalog history is inexpensive.
Titles and aliases remain reconstructible for historical reports.

Reject unknown catalog IDs or unknown catalog/URI pairs and count those rejects.
Do not discard prior catalogs simply because an old build directory is removed.
An explicit emergency revocation may reject an old catalog, but it must be
observable. Allowlisting bounds cardinality; it is not authentication of views.

## 6. Collection service

### 6.1 Request contract

Proposed endpoint: `POST /_analytics/view`, same origin as the generated page.

Proposed request:

```json
{
  "schema_version": 1,
  "event": "pageview",
  "catalog_id": "<content-derived catalog identifier>",
  "page_uri": "/water"
}
```

Use a fixed schema with unknown fields rejected. No browser timestamp is needed:
the metric is grouped by collector receipt time, not a client-controlled clock.
No full URL, referrer, user agent, IP address, screen dimensions, language list,
or browser identifier belongs in the payload.

The collector validates method, media type, body size, schema, catalog membership,
and URI limits before accepting a record. Proposed starting limits are a 2 KiB
body and a 1 KiB page URI, subject to catalog validation before deployment.
Do not permit arbitrary strings to create pages or output paths.

Successful response: `204 No Content` with `Cache-Control: no-store`, after the
record reaches the collector's defined durable-log boundary.

Malformed requests receive an appropriate `4xx`; temporary capacity/storage
failures receive `429` or `503`. Never return success after a failed append.
Unknown and rejected requests contribute to health counters, not raw-body logs.

### 6.2 Hosting and isolation

Run a small dedicated process under its own service account, listening on a
loopback address or Unix socket behind Caddy. Keep it independent of Sitegen and
pond transactions. A site build, maintenance operation, or pond write lock must
not stop the collector.

The collector needs no DataFusion session, cloud credentials, or browser-visible
secret. Do not make an HTTP request launch `pond`. Reuse workspace infrastructure
where appropriate, but keep the HTTP service's runtime and dependency footprint
small.

Caddy must route the endpoint before static `try_files` rewriting and file
serving. Only the configured production host reaches the production collector;
staging has a separate service and log directory.

Prefer a narrowly scoped reader group for ingestion, a writable private log
directory for the collector, and read-only catalog files. Neither raw logs nor
the catalog registry lives under the site's public root.

### 6.3 Durability, ordering, and rotation

Write flat NDJSON records containing:

| Field | Source and meaning |
|---|---|
| `schema_version` | Collector record schema version. |
| `received_at_us` | Server UTC receipt timestamp in integer microseconds. |
| `site_id` | Server configuration, never trusted from the payload. |
| `catalog_id` | Validated catalog identity. |
| `page_uri` | Validated canonical path. |
| `segment_id` | Server-generated identity for this log segment. |
| `record_seq` | Monotonic record number within the segment. |

`(segment_id, record_seq)` names one accepted record. It is not a visitor ID.
Several records can share a timestamp; clock adjustments can make timestamps
go backward. Ordering and ingestion progress must not depend on timestamps being
unique or strictly increasing.

Use a bounded single-writer queue and durable group commit. A response becomes
successful only after the containing batch has been flushed and synchronized
according to the documented local-disk durability policy. A full queue applies
backpressure or returns failure instead of consuming unbounded memory. Local
durability is not protection from disk or host loss.

At the site's expected scale, a per-batch synchronization interval can be small;
measure latency and I/O before choosing it. Test crash recovery at every boundary.
A crash after a durable write but before its response can leave a recorded event
whose client saw failure; the no-retry policy avoids deliberately duplicating it.

Rotate by close/synchronize and rename to a unique immutable segment name, then
open a new active file with a new `segment_id`. Do not use `copytruncate`.
Synchronize directory changes where required for crash durability. Use the same
documented rotation protocol in the ingestion integration. Verify file identity
and prefix across a read, or coordinate rotation with a short host-side lock;
do not assume the existing ingester participates in the collector's writer lock.
An ingest snapshot must never mistake a replacement active file for a continuation.

Maintain a private segment inventory with predecessor identity, final record
count, and checksum for each closed segment. Reconcile it with durable active
progress during crash recovery. Per-record sequence numbers alone cannot detect
an entirely missing segment or a missing final tail. The inventory and raw-log
backup together supply that completeness evidence.

Recover an incomplete final record on startup explicitly: retain evidence,
report the truncation, and start a new segment. An acknowledged record must not
be removed by recovery. Immutable segments are never edited.

Start with uncompressed archives to simplify prefix matching and source identity.
Compression can be added only after rotation, matching, and reader-layer behavior
are covered together; file extensions alone do not select decompression in
Watertown.

### 6.4 Privacy and abuse boundaries

Use same-origin requests, omit credentials, send no referrer, set no cookies,
and expose no permissive cross-origin response policy. Validate browser Origin
and Fetch Metadata where available. Define and test handling of missing metadata;
do not claim these headers authenticate a caller.

The reverse proxy and collector inevitably see transport addresses. Suppress
ordinary request access logging for the beacon endpoint or replace it with
sanitized counters. Exclude bodies, IPs, cookies, and request headers from
application errors and traces. A proxy error log can also leak request details;
include it in deployment verification.

Apply bounded concurrency and rate limits. If transient per-address limits are
necessary, do not persist the addresses or export them into analytics. A global
bound must exist even without trustworthy client identity. Large numbers of
forged events for known pages remain possible: this is trend measurement, not
fraud-resistant accounting.

Do not bypass browser blocking or send a tracking-image fallback when the beacon
fails. Publish a brief disclosure describing fields, purpose, and retention.
Cookieless first-party collection is data-minimizing, not a blanket legal consent
exemption. Resolve deployment-specific disclosure/consent requirements before
enabling collection.

## 7. Ingestion and source authority

The collector's private source logs, plus an independently recoverable backup,
are the event authority. Pond storage must not be the sole recoverable copy
during the supported rebuild horizon.

Use `logfile-ingest` with explicit integer timestamps:

```yaml
# Illustrative existing factory configuration; host paths are proposed.
archived_pattern: /var/log/watertown-analytics/site-prod/views-*.jsonl
active_pattern: /var/log/watertown-analytics/site-prod/views.jsonl
pond_path: /analytics/raw
timestamp_field: received_at_us
timestamp_unit: microseconds
```

This uses the current integer timestamp extractor rather than Caddy's usual
fractional-seconds `ts`. Configuring a timestamp field also holds an unfinished
active record until its newline arrives. Verify rotations with the actual
collector, including rotation while ingestion is reading and multiple rotations
between runs.

Parsed canonical events have typed timestamp, URI, site, catalog, and record
identity fields. A strict validation stage must reconcile records against segment
sequence information and reject invalid accepted-event data before advancing
progress. `jsonlogs://` alone is not that contract: its warning-and-skip parser
can produce plausible but incomplete counts.

Reuse the JSON provider if strict validation can be integrated without changing
other log users' behavior. Otherwise add a narrow strict reader for collector
records. Do not change all permissive log parsing as an incidental analytics fix.

Ingestion idempotence is based on logical source membership and accepted record
identity, not filenames or maximum timestamps. Renaming an active log into its
archive name must not count its prefix again. An explicitly restored duplicate
segment must be recognized before aggregation; conflicting content for a known
segment/sequence is an error.

Keep collection, ingestion, aggregation, and Sitegen on independent cadences.
Collection is continuous. Ingestion can run more frequently than the site's
current three-hour timer. Select the pilot cadence from retention and freshness
needs; a site build must not be the only opportunity to save rotating logs.

## 8. Materialized analytics table

### 8.1 Logical schema

The primary hourly table is long-form:

| Column | Logical type | Meaning |
|---|---|---|
| `timestamp` | UTC timestamp, microseconds | Start of the half-open hourly bucket. |
| `page_uri` | UTF-8 string | Canonical page identity, including mount prefix. |
| `page_views` | UInt64 | Accepted reported views assigned to this bucket. |

The logical key is `(timestamp, page_uri)` within one site/environment and metric
definition. Exactly one current row may exist for each key. Keep source snapshot,
recipe version, catalog registry identity, and coverage in publication metadata
rather than repeating them in every count row.

Example:

```text
timestamp             page_uri          page_views
2026-11-01T10:00:00Z   /                 12
2026-11-01T10:00:00Z   /water             7
2026-11-01T10:00:00Z   /noyo-harbor/      4
2026-11-01T11:00:00Z   /water             9
```

Do not pivot every URL into its own permanent metric column. A URI dimension
supports new pages without schema changes and allows normal SQL filtering,
grouping, and joins to titles. Page count is small, so begin without top-N
cardinality truncation.

### 8.2 Dictionary encoding requirement

Keep the logical Arrow column a string. Require Parquet dictionary encoding for
`page_uri` in the ordinary bounded catalog workload, including materialized
partitions and their repacked physical representation. Dictionaries are local to
column chunks/row groups, not a global persistent ID registry.

Do not introduce a manually maintained `page_number` lookup table just for
compression. Do not require dictionary-typed Arrow arrays to propagate through
every DataFusion operator. Those are different concerns from storage encoding.

Implementation must audit all relevant writers:

- Initial materialized output.
- Physical pack maintenance.
- Native import/reconstruction.
- Public aggregate export, if it rewrites rather than copies the data.

The current deterministic pack policy explicitly disables dictionaries. Merely
relying on a Parquet default in the first writer does not satisfy the requirement.
Choose an explicit bounded dictionary policy shared by these paths, preserving
determinism for identical ordered input within a build. Prefer enabling supported
string dictionaries through an existing physical-policy mechanism if available;
otherwise introduce the smallest versioned policy needed. Do not silently alter
unrelated table encoding or wire formats.

Respect [logical series identity](logical-series-identity-design.md): physical
encoding and pack boundaries must not change logical row or series identity.
Do not bypass pack proof validation because physical bytes changed. Assess any
reader/writer policy compatibility before rollout.

Measure actual Parquet metadata and sizes before and after maintenance and
restore. Dictionary encoding can fall back when limits are exceeded; make the
catalog limits and writer thresholds sufficient for the supported workload, and
surface an unmet encoding requirement rather than assuming it was preserved.
Empty partitions do not contain dictionary pages.

### 8.3 Time and aggregation rules

Use UTC hourly buckets `[hour_start, next_hour_start)`. Count by server receipt
time. A delayed browser delivery belongs to the hour when received, not the hour
in which someone originally opened the page; no client clock is retained.
Ingestion lag or restored archives can still modify old receipt-time buckets.

Conceptually:

```sql
SELECT
  date_trunc('hour', timestamp) AS timestamp,
  page_uri,
  COUNT(*) AS page_views
FROM validated_events
GROUP BY date_trunc('hour', timestamp), page_uri
ORDER BY timestamp, page_uri;
```

Daily and coarser counts are `SUM(page_views)`, never averages of counts or
counts of hourly rows. UTC days are the initial reporting convention; local
calendar days require an explicit timezone and daylight-saving policy.

## 9. Incremental publication and late data

### 9.1 Why the existing materializer is insufficient

At 10:15 a page has 12 views in the 10:00 bucket. At 10:45 the same bucket has 29.
Appending only rows with `timestamp > previous_max` leaves 12 permanently stored.
Appending the replacement as another ordinary row makes later sums count both.

Even storing individual events with the existing timestamp-only materializer
is unsafe: an event arriving later can share the last stored timestamp or have
an earlier receipt timestamp because ingestion lagged.

Do not work around these cases by inventing slightly increasing timestamps,
discarding late records, or waiting a fixed hour and declaring it complete.

### 9.2 Recommended first durable implementation

Use **replaceable daily partitions of hourly counts**. The logical table remains
one table; partitioning is an implementation detail.

A narrowly scoped, proposed `analytics-rollup` executable factory owns:

1. Validation/typing of new logical event chunks.
2. Identification of affected UTC days from conservative source bounds.
3. Grouped SQL aggregation of the complete affected days.
4. Transactional publication of replacement partitions and exact progress.

Reuse query-foundation's snapshot/change and publication types where applicable.
Implement a separate partition-snapshot sink rather than weakening the
append-only `TinyFsMaterializationSink` contract. Do not add arbitrary analytics
logic to `temporal-reduce`, or assume it already groups by URI.

Each replacement consists of immutable Parquet content plus a manifest selecting
the current partition objects. Store both in the pond under normal transactional
authority, not in an untracked host cache. Publish the new manifest and source
progress in the same pond commit. Expose selected partitions through a provider
as one typed table for queries and Sitegen.

The sink is new work. Before coding the factory, demonstrate the smallest real
TLogFS transaction that makes partition membership and progress visible together,
including restart. Foundation test doubles alone are insufficient.

Daily partitions bound repair without requiring native row-level updates. At
larger volume, cached per-logical-chunk partial counts can reduce reparsing.
That optimization must merge source partials exactly once and must not alter
the logical table contract.

### 9.3 Publication algorithm

For each run:

1. Open one coherent source snapshot and read the last committed recipe and
   exact logical-source membership.
2. Identify new/changed logical chunks. Repacking is not a logical source change.
   Validate segment identities and detect missing or conflicting records.
3. Determine dirty days from changed event-time ranges. A recipe or historical
   classification change can deliberately dirty older days.
4. Query all authoritative events for each dirty day, not just the new tail.
   Preserve every page dimension when grouping.
5. Write complete replacement day objects. If a dirty day becomes empty, remove
   its old membership rather than retaining stale rows.
6. Atomically publish the selected partition set, coverage metadata, recipe ID,
   and consumed source identities. On failure, publish none of them.
7. Let readers open the new committed snapshot. Existing readers either retain
   their coherent snapshot or obey existing closed-context behavior.

No-change runs write no new count objects. They may publish separately justified
health/progress metadata without manufacturing count rows. Old immutable objects
become eligible for normal maintenance only when no required snapshot or backup
references them.

Do not scan every historical manifest entry or log body merely to discover an
unchanged tail without measuring that cost. Reuse logical chunk identity,
persisted bounds, and incremental source membership mechanisms.

### 9.4 Repair policy and retention

Maintain a bounded ordinary repair horizon chosen from actual ingestion lag and
source retention. Within it, repair affected days automatically. Beyond it, fail
visibly and require an explicit historical rebuild; do not advance progress past
unapplied data.

If a raw source interval has expired everywhere, an exact rebuild of that interval
is unavailable. Retained old counts must carry their original recipe identity.
Do not relabel unrecomputed history as if a new classification rule had applied.

A repair can remove counts as well as add them. Changes to exclusions, duplicate
handling, and erroneous catalog mappings therefore require replacement semantics.
Plan and display the rebuild range before an operator authorizes it.

## 10. Coverage, zeros, and operational health

Absence of events is not evidence of collection failure, and a fresh maximum
event timestamp is not evidence of complete collection.

Track collection health separately from view records:

- Collector process availability and readiness.
- Accepted, rejected, rate-limited, and storage-failed requests.
- Queue occupancy and durable-log progress.
- Segment continuity, rotation discovery, and ingestion lag.
- Aggregate source snapshot and last successful publication.
- Catalog acceptance failures and schema/sequence validation failures.

Use private health/status records or monitoring data; never generate fake views
to keep the analytics series fresh. A health request verifies readiness, while
a controlled end-to-end probe verifies append/ingest/publication using a separate
test stream excluded from audience counts.

Maintain coverage intervals through which collection and ingestion are known
operational, including successful empty scans. Do not advance them using
`MAX(event.timestamp)`. A collector outage, expired un-ingested archive, or missing
segment produces a gap.

Even operationally complete coverage means "the collector pipeline was operating,"
not "every browser successfully delivered." Blocking and client network loss are
unobservable.

Keep sparse positive counts in the core table initially. The presentation may
fill a missing page/hour with zero only when coverage is complete and the page
was eligible for collection. Otherwise show unknown/gap. Current partial buckets
must be identified as partial, not compared unqualified with full days.

## 11. Sitegen presentation and failure isolation

Sitegen consumes a committed aggregate snapshot, never opens the raw collector
directory, and never performs collection as part of rendering.

Initial presentation:

- Site-wide hourly/daily reported-view chart.
- Top pages over a fixed trailing window, with canonical links and titles.
- Optional per-page history selected from the same long-form table.
- Metric definition, UTC convention, source coverage, and "updated through."

The existing timeseries renderer often consumes wide numeric series. Do not
assume it will interpret multiple URI-valued rows at one timestamp as separate
lines automatically. Add an explicit page filter/grouping adapter or derive a
small selected-page presentation view. Preserve the long-form canonical table
regardless of chart shape.

Resolve titles through retained catalogs. If a page is retired, keep its history
and mark it retired rather than linking blindly to a now-missing URL.

Export only approved aggregate columns and public catalog labels. Raw logs,
per-event rows, collector record identities, health internals, and source
credentials must never enter public Parquet manifests or the SQL explorer.
Publishing aggregate data means visitors can download it, not just see the chart.
Decide whether hourly per-page counts are suitable for public release on this
small site; daily-only public output is an alternative without changing internal
storage.

If analytics fails, keep the last successful analytics bundle and report its
staleness while allowing core site content to publish. This must be an explicit
orchestration branch with logged failure, not a catch-all that swallows query
errors. If no successful bundle exists, render "analytics unavailable."

Preserve the bundle's data and its original freshness metadata together. A new
site build must not make old analytics look newly collected. The analytics job
still reports failure to monitoring even when the content build succeeds.

Exclude the analytics dashboard and synthetic validation routes by default to
avoid counting operational inspection as audience activity. This is a catalog
setting, not a runtime special case hidden in the SQL.

## 12. Deployment, retention, and rollback

### 12.1 Initial topology

Recommend one local collector per serving environment. Initially use the site's
existing pond for raw ingestion and private aggregate materialization, but run
collector and ingestion jobs independently of site rendering.

This avoids a new pond replication lifecycle. A separate analytics pond can
follow if privacy permissions, workload, or retention require it. Ensure existing
site-pond remote backup permissions are appropriate before storing even minimized
event data there.

Deploy staging first. Production traffic must not flow into staging; production
collection is explicit. Do not interrupt existing production data collection or
silently move the outer repository's Watertown submodule pointer.

### 12.2 Retention decisions

Agree on these separately:

| Artifact | Retention requirement |
|---|---|
| Active/rotated collector logs | Longer than the maximum supported ingestion outage, with a hard disk bound and loud capacity alerts. |
| Independently recoverable raw backup | At least the advertised exact-rebuild horizon; restore must be demonstrated. |
| Raw events in the pond | Explicit policy consistent with backup permissions and rebuild guarantees. |
| Aggregate partitions | Can outlive raw data, but old recipes may then be irreproducible. |
| Catalog history | Retain while old pages can report or retained counts need interpretation. |
| Health/gap metadata | Long enough to explain the corresponding retained counts. |

A possible pilot is a 30-day raw rebuild horizon with longer aggregate retention,
but this is not an approved default. Choose numbers from measured traffic, disk
budget, expected outages, and disclosure requirements. Do not make silent deletion
of un-ingested logs the disk-pressure policy.

### 12.3 Rollback

Instrumentation is opt-in and can be disabled in the next site build without
removing existing data. Cached pages can continue sending after that build; either
keep the collector available or explicitly disable it and accept best-effort loss.

A collector rollback must keep reading its current log/schema and retained
catalogs, or stop visibly rather than write incompatible records into an existing
segment. A materializer rollback leaves the last successful manifest visible.
Physical encoding changes need compatible readers before promotion.

Production enablement order is: storage/backup and collector, catalog installation,
instrumented pages, ingestion, verified aggregates, then public presentation.
Public presentation can be disabled independently of collection.

## 13. Proposed configuration surface

These examples illustrate interfaces to implement; they are not valid current
Sitegen or rollup configuration.

```yaml
# Proposed Sitegen opt-in.
analytics:
  enabled: true
  endpoint: /_analytics/view
  exclude:
    - /analytics
```

Only expose settings with a clear operational need. Keep the initial metric
definition fixed and versioned rather than offering many lifecycle switches.
The build generates catalog metadata and the script asset automatically.

```yaml
# Proposed analytics-rollup factory configuration.
source: /analytics/raw
target: /analytics/pageviews
bucket: 1h
partition: 1d
```

Source typing, repair horizon, retention, and dictionary policy need explicit
validated contracts before this interface is finalized. In particular, do not
make a `dictionary: true` YAML switch claim success when the physical pack writer
would undo it. Runtime identity and collector limits belong in host provisioning,
not public page configuration.

## 14. Implementation phases and exit criteria

Each phase should be independently reviewable. Documentation and tests must
distinguish implemented behavior from future configuration. Work stays in the
checked-out repository pair; branch creation and PR merging remain human-owned.

### Phase 1: page identity and disabled-by-default instrumentation

Primary code: `crates/sitegen/src/routes.rs`, `config.rs`, `layouts.rs`,
`factory/mod.rs`, and the existing asset pipeline.

- Define a complete route catalog and deterministic identity.
- Cover static, template, content, recursive subsite, and passthrough pages.
- Add the opt-in browser script and fixed first-visible semantics.
- Keep endpoints origin-relative and safely escape embedded metadata.
- Document the metric and generate configuration reference through the existing
  documentation system when new typed fields are added.

**Exit:** an enabled fixture build emits exactly the expected catalog and one
script per eligible document; disabled builds preserve existing output behavior.
Headless lifecycle tests prove no event for hidden/prerendered pages, one for
first visibility, and no second event for tab returns or counted bfcache restores.
No browser GUI is opened by tooling.

### Phase 2: collector and source-log contract

Add the smallest dedicated collector binary/service justified by existing
workspace dependencies. Avoid embedding DataFusion or pond mutation in it.
Outer-repository integration belongs in `terraform/station/` and `config/systemd/`.

- Implement bounded HTTP validation, same-origin deployment, and catalog loading.
- Implement flat integer-timestamp records, bounded writer queue, durable
  acknowledgement, rotation, and startup recovery.
- Provision least-privilege filesystem access and sanitized proxy behavior.
- Add health counters and readiness without writing synthetic audience events.

**Exit:** accepted requests survive restart at the declared durability boundary;
rejected requests never become views; storage failure never returns success;
memory and queue size remain bounded under overload. Proxy and process logs are
checked for unintended request-data retention.

### Phase 3: ingestion and a reference query

Primary code: existing `logfile-ingest`, strict collector-record validation,
and SQL/provider integration under `crates/provider/`.

- Install staging ingestion with actual rotation names.
- Establish segment/record identity and exact source snapshot membership.
- Add typed event-time bounds and gap reporting.
- Compute a full-history reference result on a small controlled fixture.

**Exit:** append, active-file rename, multiple rotations, partial writes, equal
timestamps, backward clock movement, restart, and archive restoration all produce
exactly the expected accepted-event set. Duplicate/conflicting segments are
handled explicitly. A missing archive is a coverage error, not zero views.

### Phase 4: transactional grouped materialization

Primary code: `crates/query-foundation/`, a new provider-side partition sink and
analytics factory, and actual TLogFS integration tests.

- Implement atomic partition selection and source progress.
- Group by bucket and canonical URI; publish replacement daily snapshots.
- Preserve no-change behavior and support empty replacements.
- Implement bounded repair and an explicit historical rebuild path.
- Expose a single logical long-form table.

**Exit:** incremental results equal the reference after every mutation. Injected
failures before/after publication never expose mixed data/progress. Retry does
not double count. Repacking source files does not dirty unchanged logical data.

### Phase 5: dictionary encoding and physical lifecycle

Primary code: materialization writers, `crates/sync-store/src/content/`,
`crates/tinyfs/src/arrow/parquet.rs`, and native import/export paths as required.

- Audit the complete physical write/rewrite chain.
- Select the smallest explicit deterministic string-dictionary policy.
- Verify current logical identity and pack proof invariants.
- Measure URL-column and total-file savings at representative page counts.

**Exit:** physical Parquet inspection confirms dictionary encoding for the
supported nonempty URI column before and after maintenance and restore; logical
rows/hashes are unchanged by equivalent repacking. The requirement is not met by
an initial-file-only check.

### Phase 6: presentation and staged operations

Primary code: Sitegen grouping/presentation and outer-repository site templates,
`config/site.yaml`, scripts, and deployment configuration.

- Render totals, top pages, per-page history, coverage, and original freshness.
- Export only approved aggregates.
- Add explicit last-good analytics fallback without suppressing job failure.
- Run staging through rotations, collector outages, site builds, and maintenance.
- Verify a source-log restore and complete rebuild, then seek production approval.

**Exit:** the site remains usable and deployable when analytics is unavailable;
public artifacts contain no raw event data; staging/production remain isolated;
operator-visible health explains failed or stale analytics.

## 15. Validation and performance evidence

### 15.1 Correctness matrix

| Scenario | Required result |
|---|---|
| Two pages in one hour | Two URI-keyed rows; site total equals their sum. |
| More views in an already published hour | Replace its count, neither freeze nor append a duplicate. |
| Same timestamp on different ingestion runs | Every accepted record counted once. |
| Late archive for an earlier day | Repair only the affected supported partitions. |
| Change removes all views from a partition | Old partition membership disappears atomically. |
| No new source records | No new count objects and no invented coverage from event maxima. |
| Crash between staging and manifest commit | Readers see the prior complete snapshot; retry is safe. |
| Rotation beyond the previously ingested size | Match the old prefix and ingest the new active file separately. |
| Catalog changes while an old tab remains open | Old valid catalog/URI pairs are still accepted. |
| Query/fragment/`.html` alias | Beacon uses the publisher's canonical URI. |
| Recursive subsite mount | Prefix appears exactly once; endpoint remains the configured origin path. |
| Source pack maintenance | Logical event membership and counts remain unchanged. |
| Raw source unavailable for requested repair | Visible blocked rebuild; do not publish approximate replacement. |
| Collector outage followed by recovery | Gap remains distinguishable from a quiet interval. |
| Analytics query or export failure | Core site can publish last-good analytics with stale metadata. |

Use a synthetic fixture with known expected counts, including both same-time
records and late records. Check totals and individual URI/bucket keys, not just
averages or row counts that can hide duplicates.

### 15.2 Performance matrix

Measure rather than infer incrementality from the presence of a watermark.

Record source bytes opened/parsed, logical chunks examined, input/output rows,
peak RSS, wall time, physical bytes written, objects replaced, and export bytes.
Separate metadata-discovery cost from historical payload reads.

Run these cases on increasing history sizes with the same page catalog:

- Initial bootstrap.
- No-change tick.
- A fixed small append in the current day.
- A fixed late batch in one older day.
- Multiple rotations between runs.
- Maintenance followed by an unchanged analytics tick.
- Full explicit rebuild.

With daily replacement, a current-day update may re-read that day's input and
rewrite that day's counts. It must not reparse all prior days or rewrite all
historical count objects. A repair should scale with affected days, not total
retained history. State any metadata cost that still grows with history.

Compare runs with, for example, 1, 10, and 100 days of older unchanged data while
holding the dirty day constant. The expected payload work is constant across
those runs. Establish numerical time/memory budgets from the first real workload;
do not claim success solely from correct counts.

Measure dictionary encoding on realistic low-cardinality URIs, including long
paths and catalog growth, before and after packing. Do not rely only on tiny
fixtures where Parquet metadata dominates the result.

Use existing Rust tests, shell integration tests, and headless browser tooling.
Select targeted tests during implementation and run the repository's required
formatting, documentation, clippy, and CI checks before completing code phases.
Do not open GUI browsers for automation.

## 16. Decisions to confirm before production implementation

The recommended design is concrete enough to prototype, but these product and
operational choices remain explicit approval points:

1. Confirm first-visible-document semantics, including not counting an already
   reported back/forward-cache restoration.
2. Confirm that canonical mounted path is the requested base-URI dimension;
   add a site dimension only if combining origins/environments.
3. Choose public granularity: hourly per-page counts or daily-only publication.
4. Approve raw-event/backup retention, repair horizon, and disclosure/consent
   treatment.
5. Approve collector packaging and the initial same-site-pond topology.
6. Review dictionary-policy compatibility and the new partition sink before
   treating durable analytics as production-ready.

None of these requires visitor tracking. The minimum useful system remains a
small publisher-owned page catalog, a best-effort browser event, a durable
append-only source log, and a repairable dictionary-encoded count table.

## References

- [Sitegen design](sitegen-design.md)
- [Earlier web-log ingestion design](archive/weblog-ingestion-design.md)
- [Logical series identity and physical packs](logical-series-identity-design.md)
- [Timeseries correctness and performance plan](timeseries-datafusion-correctness-plan.md)
- [Rotating log ingestion](../crates/provider/src/factory/logfile_ingest/documentation.md)
- [JSON log provider](../crates/provider/src/format/jsonlogs/documentation.md)
- [Temporal reduction](../crates/provider/src/factory/temporal_reduce/documentation.md)
- [MDN: `navigator.sendBeacon()`](https://developer.mozilla.org/en-US/docs/Web/API/Navigator/sendBeacon)
- [MDN: `pageshow` and back/forward-cache behavior](https://developer.mozilla.org/en-US/docs/Web/API/Window/pageshow_event)

Browser references were consulted while preparing this proposal. The browser
script and real serving configuration still require the lifecycle and privacy
validation described above.
