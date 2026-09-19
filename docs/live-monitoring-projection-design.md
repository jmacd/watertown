# Transactional Live Monitoring

> **Status:** Revised design with an implemented and tested transactional
> read-after-write path. Physical-series providers are coherent snapshots:
> mutations invalidate older providers, and closed transaction state cannot be
> reused.

## 1. Purpose

Monitoring should be a small extension of the existing pond write path, not a
second data system. Watertown already has the necessary storage and query
abstractions:

- TinyFS physical series for durable Parquet data;
- one transaction `State` shared by filesystem and provider operations;
- DataFusion table providers over physical-series versions; and
- normal pond commits for atomic publication.

The monitor should use those abstractions directly. It does not need a copied
`MemoryPersistence` filesystem, a projection-specific namespace, SQLite, or a
second commit for monitor results.

The central workflow is:

```text
begin pond write transaction
    -> write observations
    -> build bounded providers over the staged physical series
    -> evaluate monitor SQL with DataFusion
    -> append evaluation, transition, and notification-intent rows
    -> commit observations and monitor state once
    -> publish the committed status to the in-process service cache
    -> deliver notifications asynchronously
```

The website remains independent and may continue rebuilding every three
hours. Azure pond backup is also independent. Neither is on the alert-latency
path.

## 2. Transaction coherence contract

TLogFS reads combine committed Delta records with the transaction's pending
records. The same transaction `State` backs TinyFS reads and its
`ProviderContext`.

The transaction exposes one shared coherence state:

- every completed visibility-changing mutation advances a generation;
- a physical-series provider is an immutable snapshot of one generation;
- requesting the same provider from the same `ProviderContext` after a
  mutation builds a new snapshot instead of returning the cached old one;
- scanning or executing a provider after its generation becomes stale returns
  an explicit error;
- physical providers enumerate their exact live version URLs, so DataFusion's
  list-files cache cannot preserve an earlier wildcard listing;
- each executing provider partition holds a query guard while it can still
  read transaction state;
- each open file writer holds a transaction-global writer guard keyed by
  `FileID`, including writers opened through distinct TinyFS handles;
- commit is rejected while a writer or transaction read is active; and
- commit, abort, and guard drop close the shared state, so later TinyFS and
  provider operations fail rather than return a stale subset.

This is snapshot invalidation, not mutable providers. A completed query result
remains usable, but a provider or plan from generation N cannot be used after
generation N+1. Callers request a provider again after any intervening write.

The Steward integration tests in
`crates/steward/tests/read_after_write_test.rs` verify:

1. a physical series written in a transaction is immediately readable through
   raw TinyFS;
2. a provider created after that write exposes the pending Parquet version to
   DataFusion;
3. monitor state can be written after the query and read through both TinyFS
   and DataFusion before commit;
4. observations and monitor state survive one combined commit; and
5. committed series history and a newly staged version are both visible after
   an earlier provider primed the same context;
6. the earlier registered provider fails as stale rather than returning its
   old subset;
7. multiple staged versions are queried together;
8. schema evolution across staged versions produces a merged query schema;
9. `LatestVersion` selects only the highest live version;
10. a provider context retained across commit or abort fails as closed; and
11. commit rejects unfinished writers and active transaction reads.

The implementation also scopes version lookup by `pond_id`, `part_id`,
`node_id`, and version, and uses version number rather than wall-clock time for
latest-version ordering.

The contract currently applies to local physical TinyFS series, which are the
only initial monitor inputs. Dynamic factories and external sources remain
outside the monitor transaction contract.

## 3. Architecture

```text
Watershop producer process
  Steward write transaction
    -> physical observation series
    -> bounded TinyFS providers
    -> DataFusion monitor SQL
    -> physical monitoring event series
    -> one atomic pond commit
          |\
          | \-> independent Azure pond backup
          |
          \-> committed-status notification
                -> local monitoring service
                -> in-memory current-status cache
                -> HTTPS status endpoint
                -> asynchronous SMS delivery
```

The producer computes authoritative monitor state because only it owns the
open write transaction. A long-running local monitoring service receives a
small notification after commit. A Unix-domain socket or authenticated
localhost endpoint is sufficient on Watershop; a public write webhook is not
required.

The notification is a wake-up hint carrying the pond identity, committed
frontier, and optionally the already computed compact status. The service
accepts only committed state. If it misses a hint or restarts, it reconstructs
current status from the durable monitoring series.

The HTTPS service serves the latest committed status from memory. Memory is a
serving cache, not a database and not part of commit correctness.

## 4. Initial monitor input restriction

Initial monitors query local physical pond series only.

They do not resolve dynamic factory nodes, run ingestion or storage factories,
materialize factory output, access external sources, read cross-pond imports
written in the same transaction, or query the internal committed-only
`delta_table`. Temporal reduction and similar operations belong in monitor SQL
over the bounded raw inputs:

```sql
SELECT
  station_id AS condition_key,
  MAX(level) AS observed_value
FROM water_levels
WHERE timestamp >= $window_start
GROUP BY station_id
HAVING MAX(level) > 8.0
```

This deliberately removes factory lifecycle and external-I/O failure domains
from the transaction that persists observations. DataFusion can perform the
small aggregation over the recent physical data directly.

Factory-backed monitor inputs can be reconsidered only after the raw-series
path is operational. A future factory must be read-only, side-effect-free, and
resolve entirely through the current transaction context.

## 5. Monitor definition

A monitor explicitly declares its physical input tables and query policy:

```yaml
id: high-water

tables:
  water_levels:
    path: /observations/water-levels.series

query: |
  SELECT
    station_id AS condition_key,
    MAX(level) AS observed_value
  FROM water_levels
  WHERE timestamp >= $window_start
  GROUP BY station_id
  HAVING MAX(level) > 8.0

window: 48h
lateness: 6h
for: 30m
resolve_after: 30m

freshness:
  warn_after: 30m
  fail_after: 2h
  on_stale: retain
```

The initial implementation keeps table declarations explicit rather than
inferring paths from SQL. Validation rejects:

- undeclared tables;
- paths that are not physical queryable series;
- non-`SELECT` SQL;
- missing or null condition keys; and
- unsupported external or dynamic inputs.

Definitions are loaded and validated before producer work begins where
possible. Runtime planning and execution errors still become explicit
evaluation-error records rather than empty successful results.

## 6. Bounded query execution

There is no separate in-memory retention policy because no source data is
copied into another filesystem. Pond retention remains the producer's durable
data policy.

For each evaluation:

1. finish all source writers;
2. use the transaction's `ProviderContext`;
3. derive `window_start` from a controlled evaluation timestamp;
4. request each physical series after the writes through a bounded provider
   with an event-time lower bound of `window_start - lateness`;
5. let `SeriesReadBounds` prune whole Parquet versions that cannot contribute;
6. apply the exact row-level timestamp predicate in SQL; and
7. execute and fully collect the result with a deadline, memory-pool limit,
   and result-row limit.

Versions without temporal metadata are retained conservatively. Version
pruning is an optimization; the SQL predicate is the correctness boundary.
All physical provider modes, including unbounded and latest-version reads,
enumerate explicit current version URLs. Bounds reduce that exact set further.

The configured window bounds the scanned input and query memory, not the
durable pond history. Rows age out logically even when no new commit occurs,
and late observations can still affect the window while within the declared
lateness allowance.

`WD::read_table_as_batch` is not the logical-series read path. For a
`TablePhysicalSeries` it reads the latest single Parquet version, whereas the
DataFusion provider unions the live versions. Monitoring must therefore use
the provider path for historical windows; raw TinyFS reads are useful only for
version-specific checks.

## 7. Durable monitoring data

Monitoring state is append-only Parquet in TinyFS:

```text
/monitoring/evaluations.series
/monitoring/conditions.series
/monitoring/notifications.series
```

An implementation may partition these paths by producer or monitor if
measurements justify it. It should batch all rows of one kind for one source
update into one record batch rather than produce a file per condition.

### Evaluations

Each evaluation row records at least:

- pond and source transaction identity;
- monitor ID and definition version;
- evaluation timestamp;
- input frontier and latest input event time;
- outcome: success, stale, planning error, execution error, or limit error;
- duration and rows examined/returned where available; and
- error category and bounded diagnostic text.

### Conditions

Condition rows are transition events, not mutable records:

- pending;
- firing;
- resolved;
- retained because input is stale or evaluation failed; and
- optionally acknowledged or silenced.

Each transition has a stable monitor/condition key, sequence, source
transaction, observed value, and transition timestamp. Current state is
reconstructed with a DataFusion window query such as:

```sql
SELECT *
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY monitor_id, condition_key
      ORDER BY transition_seq DESC
    ) AS rank
  FROM conditions
)
WHERE rank = 1
```

### Notification intents

The observation transaction appends notification intent for newly firing or
resolved conditions. Delivery workers append attempted, delivered, or failed
events in later independent transactions. This provides at-least-once
delivery without making network I/O part of the observation commit.

Those later delivery acknowledgements are not a second monitor-evaluation
commit. The authoritative observation, evaluation, transition, and initial
notification intent are already atomic.

## 8. Transaction protocol

For one producer update:

1. Begin one Steward write transaction.
2. Write all observation batches and finish their writers.
3. Obtain or reuse the transaction's `ProviderContext`.
4. Capture one evaluation timestamp.
5. Load enabled monitor definitions applicable to the written sources.
6. Request bounded explicit-version providers for those physical
   source series.
7. Query prior committed condition transitions needed for `for`,
   `resolve_after`, deduplication, and stale-data behavior.
8. Run and fully collect each monitor query.
9. Deregister its temporary DataFusion tables.
10. Convert results into evaluation, condition-transition, and notification
   rows.
11. Append those rows to the monitoring physical series and shut down every
    output writer.
12. Commit once. The transaction rejects the commit if a writer or underlying
    transaction read is still active.
13. Discard the transaction's filesystem, contexts, providers, and plans.
14. Only after commit succeeds, replace the service's in-memory status or send
    the local committed-status notification.
15. Schedule Azure backup, status publication, and notification delivery
    independently.

No network operation occurs while the pond transaction is open.

## 9. Error and commit policy

Monitor configuration or query failure should not discard valid source
observations. The evaluator records an evaluation error, retains prior active
conditions, and commits the observation update.

Failures that prevent the combined durable state from being written are
different:

| Failure | Behavior |
|---|---|
| Monitor SQL planning/execution fails | Append evaluation error, retain prior conditions, commit observations |
| Monitor exceeds time, memory, or result limit | Append limit error, retain prior conditions, commit observations |
| Input is stale | Record stale evaluation and retain active conditions |
| Required physical input is missing | Record invalid-input error; never substitute an empty table |
| Monitoring event batch cannot be encoded | Fail the combined transaction |
| A source or monitor-output writer has not shut down | Reject commit before planning or persistence |
| A query stream is still executing at commit | Reject commit; never drain pending records under an active read |
| Pond commit fails | Publish no new authoritative status; retain the previously committed service state |
| A transaction context is used after commit or abort | Reject it as closed rather than returning cached rows |
| Local service notification fails | Commit remains authoritative; service catches up from the pond |
| SMS delivery fails | Retain notification intent and retry asynchronously |
| Azure backup fails | Retain local commit and retry backup independently |
| Site generation fails | No effect on monitoring |

A value observed before a failed pond commit is not authoritative. A future
safety-critical ingest path may emit a separate provisional incident, but it
must be labeled provisional and must not resolve or replace committed monitor
state.

## 10. Freshness

Every evaluation distinguishes:

- **commit freshness:** age of the latest successful producer transaction;
- **input freshness:** age of the latest event in each selected source; and
- **evaluation freshness:** age and outcome of the latest monitor run.

The initial stale-data policy is:

1. never resolve an active condition solely because input is stale or a query
   failed;
2. append a separate freshness/evaluation incident; and
3. resume ordinary transitions after a successful fresh evaluation.

The HTTPS response exposes all three freshness values so a green condition
state cannot hide a stopped producer or evaluator.

## 11. Live service and process failure

The service keeps a compact immutable current-status object in memory and
atomically replaces it only for a newer committed frontier. Readers never
observe a partial update.

On startup it queries the monitoring Parquet series to reconstruct:

- latest condition per key;
- latest evaluation per monitor;
- pending notification intents; and
- source/evaluation freshness.

If the producer, service, or Watershop host is unavailable, the last locally
committed state remains correct but may not be externally reachable. A compact
Azure status publication and cloud stale timer can expose that host-level
failure without moving the full monitor engine into Azure.

The public HTTPS endpoint is read-only and must not expose pond mutation APIs,
filesystem paths, raw query execution, or producer webhooks.

## 12. Deployment and cost decision

### 12.1 Current repository topology

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

### 12.2 Preferred deployment

The current preferred deployment is:

1. Keep the producer ponds, transaction evaluator, monitoring service, and
   `site-prod` on Watershop.
2. Evaluate monitors directly inside each local observation transaction.
3. Push pond backups to the existing private Azure containers independently.
4. Publish the completed static site to Azure Static Web Apps Free every three
   hours.
5. Publish only a compact current-status artifact on monitor transitions and
   successful evaluations.
6. Use a small scale-to-zero Azure Function to detect a stale Watershop
   publisher on a timer, deduplicate transitions, and send ACS SMS if this is
   simpler than direct delivery from Watershop.

The browser loads a stable static HTML/JavaScript shell and fetches current
status. A 15-minute monitoring interval does not imply 2,880 complete site
deployments per month.

Running Watertown in Functions is not planned. If all computation must later
leave Watershop, a scheduled Container Apps Job is the compatible alternative:
it can run the existing Rust/DataFusion process to completion and scale to
zero. That migration should be justified by operational requirements, not
assumed to be cheaper.

### 12.3 Cost snapshot and uncertainty

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

### 12.4 Observation period

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
5. measured producer, monitor-query, service, and sitegen duration and peak
   RSS;
6. status API traffic and Application Insights ingestion; and
7. actual recurring ACS number, carrier, and message charges.

Do not extrapolate a 15-minute Azure bill solely by multiplying the current
aggregate limiter value by four. After the hourly baseline is stable, trial
one producer at 15 minutes and compare its measured category mix before
changing the other ponds.

## 13. Implementation plan

### Parallel track: Measure Azure traffic

- Keep production producer timers hourly.
- Retain at least 90 days of access, limiter, runtime, memory, and size data.
- Reconcile application counts with Azure billing meters monthly.
- Run a bounded 15-minute trial on one producer only after the hourly baseline
  is trustworthy.

**Acceptance:** every production Azure push or pull produces a typed access
row per remote scope, category totals sum to `total_ops`, and monthly
aggregates reconcile with Azure billing categories.

### Phase 0: Transaction/query coherence — implemented

- Transaction-global writer guards reject duplicate writers across distinct
  handles and reject commit while any writer remains unfinished.
- A shared mutation generation versions `ProviderContext` cache entries and
  invalidates physical providers and plans built before a later mutation.
- Physical providers enumerate exact live version URLs for all, bounded,
  latest, and specific-version reads, bypassing mutable wildcard listings.
- Provider execution holds transaction-read guards; commit cannot drain
  pending state while an underlying provider stream is reading it.
- Commit, abort, and guard drop close the shared state. Old physical providers
  and TinyFS persistence operations return a closed-state error.
- Version loads include `pond_id`, and latest-version selection uses the
  highest live version number.

**Acceptance:** the regression matrix covers provider-before-write,
provider-after-write in the same context, multiple pending appends, bounded
and unbounded reads, unfinished writers, active transaction reads,
latest-version selection, commit, and old-context use. Each case sees the
complete selected snapshot or returns a specific stale/closed error; none
silently returns the previously observed stale subset.

### Phase 1: Define physical monitor records

- Define schemas for evaluations, condition transitions, and notification
  events.
- Include source transaction identity and monitor definition version in every
  row.
- Add helpers that batch rows and append each series at most once per producer
  update.
- Add DataFusion queries that reconstruct current condition and notification
  state.

**Acceptance:** state reconstructed from append-only Parquet is identical
before and after process restart.

### Phase 2: Define and validate monitors

- Add explicit raw physical-series table declarations.
- Parse and validate read-only SQL.
- Reject dynamic, external, and missing inputs.
- Derive version bounds from window and lateness.
- Enforce query deadlines, memory limits, and result limits.

**Acceptance:** a representative temporal aggregation scans only eligible
versions, applies the exact row predicate, and produces deterministic
condition keys.

### Phase 3: Integrate the transaction evaluator

- Add an evaluator that receives the existing Steward transaction after all
  source writers finish.
- Enter the enforced query phase and construct bounded explicit-version
  providers from its fresh context.
- Query prior condition state and current staged observations.
- Fully collect and close all query streams.
- Append evaluation, transition, and notification batches.
- Finish all output writers.
- Commit once through the existing producer transaction.
- Keep monitor query failures distinct from failures to encode or persist
  monitoring state.

**Acceptance:** one transaction writes observations, queries them, writes
monitor state, and commits both; injected query failure commits observations
with an error evaluation, while injected storage failure commits neither.

### Phase 4: Serve committed state

- Add a local service that reconstructs status from the monitoring series on
  startup.
- Accept only committed-frontier notifications from local producers.
- Atomically replace the in-memory status object.
- Serve a read-only authenticated HTTPS status endpoint.
- Expose commit, input, and evaluation freshness.

**Acceptance:** a service restart reconstructs the same status; a notification
sent for a failed commit is rejected; concurrent readers see either the old or
new complete status.

### Phase 5: Deliver alerts

- Read durable notification intents after commit.
- Send SMS through the selected ACS path.
- Append attempted, delivered, and failed events.
- Use stable idempotency keys and retry with bounded backoff.
- Recover pending intents after restart.

**Acceptance:** crash and retry tests demonstrate at-least-once delivery
without losing an intent; repeated delivery attempts retain one stable event
identity.

### Phase 6: Publish compact cloud status

- Publish only the compact committed status to Azure.
- Add a scale-to-zero stale-publisher check if host-level outage alerts are
  required.
- Keep full pond backup and static-site publication independent.

**Acceptance:** Watershop loss leaves the last status available and causes the
cloud view to become explicitly stale without running Watertown in Azure.

## 14. Non-goals

The initial implementation does not:

- build or publish a copied `MemoryPersistence` image;
- introduce SQLite or another mutable state database;
- require a second commit for monitor evaluation;
- query dynamic factories or external sources;
- materialize temporal aggregation before monitor SQL;
- run network notification or Azure publication inside a pond transaction;
- wait for Azure backup before evaluating a local update;
- rebuild the static site at monitor cadence;
- provide exactly-once SMS delivery; or
- treat uncommitted observations as authoritative incidents.

## 15. Open decisions

The one-commit architecture has the required physical-series transaction
coherence. Remaining product decisions are:

1. Should a monitor definition error append one evaluation-error row on every
   producer run, or be rate-limited after the first unchanged error?
2. Which query time, memory, and result-row limits fit the measured water
   workload?
3. Should SMS delivery run directly on Watershop or through a small Azure
   Function?
4. What authentication and exposure model should the HTTPS status endpoint
   use?
5. Which producer should receive the first transactional monitor integration?

These decisions do not change the core model: staged physical observations are
queried through the current transaction, monitoring events are appended to
TinyFS Parquet, and all authoritative state commits once.
