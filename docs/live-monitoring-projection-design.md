# Independent Live Monitoring

> **Status:** Design proposal (unimplemented).
>
> This document proposes a bounded in-memory TinyFS image for live monitoring.
> The image contains selected source data and TinyFS factory definitions.
> Existing Watertown resolution and DataFusion synthesize query providers from
> that image without access to the source pond.

## 1. Purpose

Monitoring may eventually evaluate water observations every 15 minutes while
the website continues rebuilding every three hours. The production ponds
currently run hourly. That cadence should not change until several months of
storage-access measurements establish the Azure transaction cost of doing so.

The monitor itself belongs on Watershop, next to the authoritative local pond.
It should evaluate a committed update without waiting for that commit's Azure
backup and should continue using the last complete data image if the pond or
refresh path becomes unavailable.

The design makes one architectural move:

> Build a bounded `MemoryPersistence` image containing the TinyFS data and
> factory definitions required by monitors, validate it, and atomically publish
> it to the monitor runtime.

After publication, existing Watertown providers and DataFusion resolve factory
nodes and perform all derivation in memory. Unlike site generation, monitoring
does not materialize factory-generated output into the image.

## 2. Primary invariant

A published monitoring image is:

- a complete view of one committed pond snapshot;
- limited to declared monitor tables, their factory definitions, and the
  bounded source data needed to resolve them;
- self-contained, with no live pond, Delta Lake, cache, or site-generation
  dependency;
- immutable after publication; and
- replaced only by another complete, validated image.

If refresh fails, the current image remains queryable and its increasing age is
visible to monitor policy.

The first implementation isolates monitoring from pond and refresh failures
while the process remains alive. It does not claim process-failure isolation.

## 3. Architecture

```text
Watershop
  committed local pond snapshot
    |\
    | \-> independent pond backup to existing Azure Blob storage
    |
    \-> copy bounded data and factory definitions
        -> candidate MemoryPersistence image
        -> normal TinyFS factory resolution and DataFusion planning
        -> atomic image publication
        -> scheduled SQL monitors
        -> durable condition state
        -> small status publication to Azure

Azure
  Static Web Apps shell -> status API or status object
  Flex Function timer   -> stale-Watershop detection and ACS SMS
```

Site generation reads the pond on its own schedule and is not part of this
path. It can remain on Watershop and publish its completed static output to
Azure every three hours.

The full Watertown/DataFusion monitor does not run in an Azure Function.
Azure receives compact results, not the recent source window. This removes the
remote push from alert latency and avoids rebuilding a memory image in a
scale-to-zero runtime for every observation.

## 4. Why `MemoryPersistence`

`MemoryPersistence` already implements the TinyFS persistence abstraction used
by Watertown providers. It preserves the model that existing code understands:

- TinyFS paths and node identities;
- physical series and their versions;
- dynamic nodes and their stored factory configuration;
- ordinary file and table formats;
- `QueryableFile` and `TableProvider` construction;
- a `ProviderContext` backed by in-memory persistence; and
- DataFusion query execution.

The monitoring image should be another use of these abstractions, not a second
namespace, table, chunk, or provider system.

The image owns a `ProviderContext` created with its `MemoryPersistence`.
`cache_dir` and `pond_path` remain unset. Once the candidate is published, no
writer or transaction interface is exposed to monitor execution.

`MemoryPersistence` already stores dynamic-node factory type and configuration,
so copied factory nodes can be resolved normally against the in-memory
filesystem. The implementation may need a focused extension so copied
physical-series versions preserve the source metadata needed for bounded
reads, especially event-time bounds and stable source-version identity. That
metadata belongs in the TinyFS abstraction because it is useful to every
in-memory series reader, not only monitoring.

## 5. Monitor definition

A monitor declares the query tables visible to its SQL and the source data that
must be copied into the image:

```yaml
id: high-water
interval: 15m

tables:
  water_levels:
    path: /monitoring/water/res=1h.series

image:
  sources:
    - path: /observations/water-levels
      retain: 54h
  definitions:
    - path: /monitoring/water

query: |
  SELECT
    station_id AS condition_key,
    MAX(level) AS observed_value
  FROM water_levels
  WHERE timestamp >= $window_start
  GROUP BY station_id
  HAVING MAX(level) > 8.0

window: 48h
for: 30m
resolve_after: 30m

freshness:
  inputs:
    - /observations/water-levels
  warn_after: 30m
  fail_after: 2h
  on_stale: retain
```

In this example `/monitoring/water` is a copied `temporal-reduce` factory node
whose configuration reads `/observations/water-levels`. Resolving
`res=1h.series` constructs the existing factory-backed table provider inside
the image. Its aggregated rows are not copied from the pond and are not
written back into `MemoryPersistence`.

`tables` maps monitor-local DataFusion names to TinyFS paths. A path may name a
physical queryable file or a queryable node synthesized by a factory.

`image.sources` selects stored data to copy, with bounds for physical series.
`image.definitions` selects the factory nodes, directories, symlinks, and small
ordinary files needed to resolve the tables. The initial implementation makes
this closure explicit rather than adding factory-specific dependency analysis.
Candidate validation fails if a copied factory refers to something outside the
image.

For a monitor over a raw series, the table may refer to the source path
directly:

```yaml
tables:
  water_levels:
    path: /observations/water-levels

image:
  sources:
    - path: /observations/water-levels
    retain: 54h
```

The initial implementation requires explicit table and image declarations
rather than inferring them from SQL or factory configurations. This keeps
retention reviewable and avoids making dependency analysis part of the storage
design. Validation rejects SQL that refers to an undeclared table.

## 6. Selection and retention

The image contains the union of source data and definitions declared by enabled
monitors. Factory definitions are small; retention limits apply primarily to
physical series data.

If several monitors select the same source series, the image retains enough
data for the largest requirement:

```text
physical retention = maximum monitor window + lateness allowance
```

For example, a 48-hour query window with six hours of allowed lateness retains
54 hours of source data.

The builder uses `SeriesReadBounds::from_event_time_lo` when reading physical
series from the committed pond snapshot. These bounds conservatively prune
whole source versions. Versions without temporal bounds are retained rather
than silently discarded.

Physical retention is not the query correctness predicate. Monitor SQL applies
the exact row-level event-time predicate using its evaluation time. Therefore:

- rows age out logically even if no new pond commit occurs;
- late rows can affect a recent window while they remain physically retained;
  and
- source-version boundaries may retain some older rows without changing query
  results.

Changing an enabled monitor, its tables, source selection, definitions, or
retention requirement causes the next refresh to build an image for the new
complete input set.

## 7. Factory resolution and query-time derivation

The monitoring image copies physical data and dynamic factory definitions. It
does not copy or persist factory-generated output.

When a monitor resolves a factory-backed table, Watertown invokes the same
factory/provider path it normally uses, but the `ProviderContext` points to the
published `MemoryPersistence`. A factory such as `temporal-reduce` therefore
reads its bounded in-memory inputs and constructs its normal DataFusion
provider. Recomputing temporal aggregation over a small in-memory window is
expected to be inexpensive.

This distinction has three useful consequences:

1. Site generation and monitoring share factory and query semantics.
2. The published image has no hidden dependency on pond paths, caches, or
   external I/O.
3. Derived data is synthesized from bounded source data instead of becoming
   another retained copy.

Only queryable factories whose dependencies resolve entirely inside the image
belong on this path. Monitoring does not run factory initialization, executable
commands, ingestion, storage, site export, or providers that require external
I/O. Candidate validation resolves every declared table after source pond
access is removed, so an accidental external dependency prevents publication.

## 8. Image model

The host publishes one object:

```rust
struct MonitoringImage {
    generation: u64,
    source: SourceFrontier,
    built_at_micros: i64,
    inputs: HashMap<String, InputMetadata>,
    persistence: Arc<MemoryPersistence>,
    query: Arc<ProviderContext>,
}

struct SourceFrontier {
    pond_id: PondId,
    txn_seq: i64,
    commit_hash: ObjectHash,
    committed_at_micros: i64,
}

struct InputMetadata {
    path: String,
    retained_after_micros: i64,
    latest_event_time_micros: Option<i64>,
}
```

The concrete fields may follow existing commit and metadata types. The
important property is ownership: everything required to resolve and query an
input is held by the image.

`MemoryPersistence` is mutable while a candidate is being built. Publication
transfers it into an immutable role. Monitor code receives a pinned
`Arc<MonitoringImage>` and cannot mutate its filesystem.

## 9. Refresh protocol

Refresh always operates on one committed pond snapshot:

1. Read enabled monitor definitions.
2. Compute the union of table paths, source selections, factory definitions,
   and physical retention cutoffs.
3. Open one committed pond snapshot.
4. Create a fresh `MemoryPersistence`.
5. Recreate the selected TinyFS namespace and copy bounded physical series,
   factory definitions, symlinks, and required ordinary data, preserving
   relevant source metadata.
6. Construct the ordinary in-memory `ProviderContext`.
7. Remove source-pond access and resolve every declared table through existing
   Watertown factory and provider code.
8. Parse and plan every enabled monitor query against its declared tables.
9. Record the source frontier, image time, and input event-time metadata.
10. Atomically replace the current `Arc<MonitoringImage>`.
11. Evaluate monitors against the newly published image.
12. Commit resulting condition state and notification intent locally.
13. Publish a compact status artifact independently of the full pond backup.

Any error before publication discards the candidate. There is no partial
update and no mutation of the current image.

The first implementation performs a complete bounded rebuild after each
relevant local pond commit. The Steward-to-monitor notification is an
in-process signal, Unix-domain socket message, or localhost request carrying a
pond identity and committed frontier. It is only a wake-up hint: the monitor
opens and verifies the committed snapshot itself.

The notification is not a public webhook and does not wait for Azure. Pond
backup, status publication, and monitor evaluation are independent post-commit
effects with durable retry state. A two-day in-memory window rebuilt at the
eventual monitoring cadence is the baseline to measure before introducing
incremental complexity.

## 10. Query execution

Each evaluation pins the current image and resolves that monitor's declared
TinyFS table paths under their configured DataFusion names. Physical and
factory-backed tables use the same interface. It then executes the monitor SQL
with values such as `$window_start` derived from a controlled evaluation clock.

The query receives no source-pond context. Its `ProviderContext` refers only to
the image's `MemoryPersistence`, with no pond path or format-cache directory.

The initial SQL contract is:

- read-only `SELECT` queries only;
- only declared monitor tables are visible;
- zero result rows mean no active conditions;
- each result row has a non-null `condition_key`;
- execution has time, memory, and result-row limits; and
- query failure is reported as monitor failure, never as an empty result.

An evaluation retains its pinned image even if refresh publishes a newer one.
It therefore cannot mix source snapshots.

## 11. Freshness and condition state

Every evaluation records:

- image generation;
- source commit and transaction sequence;
- evaluation time;
- image publication time;
- time since the source frontier was committed; and
- latest observed event time for each declared input, when available.

This separates two questions:

- **pipeline freshness:** how far the published image is behind expected pond
  commits;
- **input freshness:** how recent the observations in a selected physical
  source are.

The initial stale-data policy for safety monitors is:

1. retain an active condition rather than resolve it from stale or failed data;
2. emit a separate freshness or evaluation incident; and
3. resume ordinary transitions after a successful fresh evaluation.

Condition state, evaluation health, and freshness are separate values. A
condition can remain firing while its latest evaluation is stale or failed.

Monitor state starts in memory if necessary to prove the query path. SQLite is
the preferred first durable extension. Condition updates and notification
outbox insertion should then be one transaction so delivery failure cannot
interrupt evaluation.

## 12. Failure behavior

| Failure | Required behavior |
|---|---|
| Pond unavailable | Continue scheduled queries against the current image and report increasing staleness |
| Refresh fails | Discard the candidate and retain the current image |
| Refresh task stops | Continue scheduled queries; supervision reports and restarts refresh |
| Selected input cannot be read | Reject the complete candidate |
| Monitor input is missing | Mark that monitor invalid; never substitute an empty table |
| Monitor SQL is invalid | Keep its prior condition state, report configuration failure, and continue other monitors |
| Monitor execution fails | Keep its prior condition state, report evaluation failure, and continue other monitors |
| Notification delivery fails | Retain an outbox entry for retry |
| Azure pond backup fails | Retain local monitoring results and retry backup independently |
| Azure status publication fails | Retain the local result, retry publication, and let the cloud stale timer expose the outage |
| Watershop becomes unavailable | Continue serving the last status; cloud timer changes it to stale and may send an SMS |
| Site generation fails | No effect on monitoring |
| Query overlaps publication | Complete against the image pinned when the query began |
| Monitor process crashes | Monitoring stops until restart in the initial deployment |

## 13. Resource policy

The configured input windows bound the retained source data. Candidate
construction can temporarily overlap the current image, and active queries can
pin older images, so retained-data size is not a hard process RSS limit.

The initial deployment therefore also needs:

- a maximum candidate size;
- a DataFusion execution memory pool;
- a query concurrency limit;
- a query deadline; and
- cancellation of queries that exceed that deadline.

If a candidate exceeds its limit, refresh fails and the current complete image
remains published. The system never shortens a requested retention window or
evicts arbitrary in-window data to make a candidate fit.

## 14. Deployment and cost decision

### 14.1 Current repository topology

The production data path already has the pieces needed for a low-cost
deployment:

- water, septic, and noyo run on Watershop and publish to the existing
  `casparwaterprod` West US 2 Hot/LRS storage account;
- steady source volume is approximately 15-17 MB/day, or 0.45-0.51 GB/month;
- `site-prod` already has a three-hour schedule and can remain on Watershop;
- Azure Communication Services already exists for email, although an
  SMS-capable number is not yet provisioned; and
- the Linode host serves the generated site and proxies
  `influx.casparwater.us`.

Replacing the website host does not by itself replace InfluxDB. That endpoint
must be retired or relocated before the Linode can be deleted.

### 14.2 Preferred deployment

The current preferred deployment is:

1. Keep the producer ponds, monitor engine, and `site-prod` on Watershop.
2. Evaluate monitors from the local committed pond through the bounded
   `MemoryPersistence` image described here.
3. Push pond backups to the existing private Azure containers independently.
4. Publish the completed static site to Azure Static Web Apps Free every three
   hours.
5. Publish only a compact current-status artifact on monitor transitions and
   successful evaluations.
6. Use a small scale-to-zero Azure Function to serve private status, detect a
   stale Watershop publisher on a timer, deduplicate transitions, and send ACS
   SMS.

The browser loads a stable static HTML/JavaScript shell and fetches current
status. A 15-minute monitoring interval does not imply 2,880 complete site
deployments per month.

Running Watertown in Functions is not planned. If all computation must later
leave Watershop, a scheduled Container Apps Job is the compatible alternative:
it can run the existing Rust/DataFusion process to completion and scale to
zero. That migration should be justified by operational requirements, not
assumed to be cheaper.

### 14.3 Cost snapshot and uncertainty

The following planning estimates were collected on 2026-09-17 for West US 2
pay-as-you-go pricing:

| Replacement | Approximate incremental monthly cost |
|---|---:|
| Static Web Apps Free and Flex Functions | $0 at the expected workload, before storage and SMS |
| ACS toll-free number and 20 one-segment US messages | $2.20 |
| Static Web Apps Standard | $9 |
| Linux App Service Basic B1 | $12.41 |
| B1s VM, 32-GiB Standard SSD, and Standard IPv4 | $13.64 |
| Blob static website behind Front Door Standard | more than $35 |

Static Web Apps Free is the least expensive custom-domain HTTPS frontend.
Direct Blob static hosting does not provide HTTPS for a custom hostname
without another edge service. The Free plan has no SLA; Standard should be
chosen only if that changes from a personal operational site to an
availability commitment.

Pricing references:

- [Azure Static Web Apps plans](https://learn.microsoft.com/azure/static-web-apps/plans)
- [Azure Functions pricing](https://azure.microsoft.com/pricing/details/functions/)
- [Azure Blob Storage pricing](https://azure.microsoft.com/pricing/details/storage/blobs/)
- [Azure Communication Services SMS pricing](https://learn.microsoft.com/azure/communication-services/concepts/sms-pricing)
- [Azure Front Door pricing](https://azure.microsoft.com/pricing/details/frontdoor/)

Compute and storage capacity are not the important unknowns. The existing
11.4-GB seed costs about $0.21/month in Hot LRS, and each new 0.5-GB month adds
less than one cent to subsequent monthly capacity cost. Functions and Event
Grid remain inside their free grants at this scale.

Azure storage transactions are the uncertainty. The repository records about
1,100-1,600 physical provider requests for an ordinary mature push and a
worst-case rate near 38,000 requests/day at the current hourly cadence. If the
same fixed work occurs four times as often:

| Producer cadence | Approximate requests/month | Cost bound from current Azure operation prices |
|---|---:|---:|
| Hourly worst case | 1.14 million | $0.46-$5.70 |
| Every 15 minutes | 4.56 million | $1.82-$22.80 |

The range is wide because Azure prices read/other operations at approximately
$0.004 per 10,000 and write/list operations at approximately $0.05 per 10,000.
One aggregate `ops` counter cannot select the correct price.

### 14.4 Observation period

Deployment selection and any move from hourly to 15-minute full pond pushes
are paused for at least 90 days of representative measurements. Selfmon
materializes one event per completed Azure storage-meter scope at:

```text
/metrics/azure-access.series
```

Each row retains:

- timestamp, pond, and non-sensitive remote label;
- inherited ungoverned operation and byte arrears;
- total operations and bytes;
- GET, HEAD, LIST, PUT, multipart, delete, and copy operations and bytes; and
- operation counts by storage path class, including Delta metadata, content
  objects, receipts, publications, packs, blobs, recovery, and fallback.

This complements the metrics already retained by selfmon:

- limiter charged and independently observed operations and bytes;
- pond timer health, last-run age, and actual run duration;
- transaction rate, local pond size, Parquet count, and Delta-log count;
- producer and sitegen peak RSS; and
- selfmon step failures.

The access events come from Watertown's existing
`storage_access_summary` journal record at the physical `ObjectStore`
boundary. Selfmon ingests that journal once and incrementally materializes a
typed physical series. It does not poll Azure or introduce a competing
counter.

A first monthly query can preserve the categories needed to apply Azure's
prices at decision time:

```sql
SELECT
  date_trunc('month', timestamp) AS month,
  pond,
  COUNT(*) AS remote_scopes,
  SUM(get_ops) AS get_ops,
  SUM(head_ops) AS head_ops,
  SUM(list_ops) AS list_ops,
  SUM(put_ops) AS put_ops,
  SUM(multipart_ops) AS multipart_ops,
  SUM(delete_ops) AS delete_ops,
  SUM(copy_ops) AS copy_ops,
  SUM(get_bytes) AS downloaded_bytes,
  SUM(put_bytes + multipart_bytes) AS uploaded_bytes
FROM source
GROUP BY date_trunc('month', timestamp), pond
ORDER BY month, pond
```

Run it against `series:///metrics/azure-access.series`. Do not collapse these
columns into read/write billing classes in storage: Azure can change meter
definitions and prices, while the physical operation facts remain valid.

At the end of the observation period, group the series by calendar month,
pond, and operation category. Reconcile those totals with Azure Cost
Management meters for read, write, list/create, other operations, capacity,
and egress. The decision requires:

1. p50, p95, and maximum operations per successful producer push;
2. operation mix and bytes by pond and storage path class;
3. actual pushes, failures, retries, and no-op pushes per month;
4. retained Azure capacity growth;
5. measured producer, monitor-image, query, and sitegen duration and peak RSS;
6. status API traffic and Application Insights ingestion; and
7. actual recurring ACS number, carrier, and message charges.

Do not extrapolate a 15-minute Azure bill solely by multiplying the current
aggregate limiter value by four. After the hourly baseline is stable, trial
one producer at 15 minutes and compare its measured category mix before
changing the other ponds.

## 15. Implementation phases

### Phase 0: Measure the existing system

- Deploy the selfmon Azure-access materialization.
- Keep production producer timers hourly.
- Retain at least 90 days of access, limiter, runtime, memory, and size data.
- Reconcile application counts with Azure billing meters monthly.
- Run a bounded 15-minute trial on one producer only after the hourly baseline
  is trustworthy.

**Acceptance test:** every production Azure push or pull produces one typed
access row per remote scope, category totals sum to `total_ops`, and monthly
aggregates can be reconciled to Azure's billed operation categories.

### Phase 1: Static image

- Define `MonitoringImage` and its source metadata.
- Copy one bounded physical series and a `temporal-reduce` factory definition
  into `MemoryPersistence`.
- Resolve the factory-generated table through its existing Watertown provider.
- Remove access to the source pond.
- Run a representative DataFusion query over the in-memory aggregation.

**Acceptance test:** after publication, deny all source-pond access and obtain
the same bounded query result as a direct pond query.

### Phase 2: Monitor input planning

- Define explicit table, source, definition, and retention configuration.
- Compute the union required by enabled monitors.
- Build a complete image from one committed snapshot.
- Validate that SQL uses only declared tables.

**Acceptance test:** two monitors sharing an input with different windows
produce one image retaining the larger requirement and correct results for
both windows.

### Phase 3: Atomic refresh

- Add a host containing the current `Arc<MonitoringImage>`.
- Build candidates independently.
- Publish with one atomic pointer replacement.
- Preserve the current image on every candidate failure.

**Acceptance test:** a query pinned to generation `N` finishes on `N` while new
queries begin on `N+1`.

### Phase 4: Scheduled monitoring

- Evaluate monitors after successful refresh and on their configured timers.
- Add condition, pending, resolution, stale, and evaluation-error behavior.
- Expose image and input freshness with every result.

**Acceptance test:** monitoring continues against the last image during a pond
outage and never resolves an active condition solely because data became
stale.

### Phase 5: Durable operational state

- Store condition state and notification outbox entries in SQLite.
- Deliver notifications asynchronously and idempotently.

### Phase 6: Measure before optimizing

Measure complete rebuild time, peak memory, retained image size, and query
latency under the expected two-day water workload.

Only measured problems justify optimizations such as:

- reusing unchanged in-memory versions;
- incrementally copying committed source versions;
- caching query plans;
- deriving input dependencies from SQL; or
- checkpointing an image for process-restart independence.

These optimizations must preserve the same published-image and query
interfaces.

## 16. Non-goals

The initial implementation does not:

- create a projection-specific namespace or table-provider hierarchy;
- materialize derived factory output;
- run initializing, executable, ingest, storage, export, or external-I/O
  factories in the monitor runtime;
- consume uncommitted MQTT or OTAP batches;
- wait for an Azure pond backup before evaluating a local commit;
- run the Watertown/DataFusion monitor in Azure Functions;
- rebuild the complete static site at the monitoring cadence;
- incrementally replay the complete pond commit chain;
- provide exactly-once external notification delivery;
- persist images across process restart; or
- survive failure of the monitor process.

## 17. Open implementation questions

The initial implementation needs answers to a small set of concrete questions:

1. Which existing TinyFS operation should copy a physical-series version while
   preserving its event-time and identity metadata?
2. What read-only wrapper should prevent mutation after a
   `MemoryPersistence` candidate is published?
3. Should image source/definition selection remain explicit, or should
   queryable factories eventually expose their TinyFS dependencies?
4. How should committed pond metadata map onto `SourceFrontier` using existing
   types?
5. What initial candidate-size, query-memory, concurrency, and deadline limits
   fit the deployed water workload?

These questions refine existing abstractions. They do not change the core
architecture: bounded TinyFS data and factory definitions are copied into
memory, existing Watertown tools synthesize and query providers there, and
complete images are replaced atomically.
