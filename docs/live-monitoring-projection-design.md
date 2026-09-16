# Independent Live Monitoring Projection

> **Status:** Design proposal (unimplemented).
>
> This document proposes a bounded, in-memory projection of a committed
> tlogfs pond for reliable DataFusion monitoring. The projection is primarily
> a failure-domain boundary, not a query cache: after a generation is
> published, monitoring continues without access to the source pond, Delta
> Lake, TinyFS persistence, format caches, site generation, or a functioning
> projection refresh task.

---

## 0. Context

Ponds currently combine several activities that need not run at the same
cadence. A water pond may ingest and commit observations every 15 minutes,
while regenerating the complete website is appropriate only every three hours.
Monitoring should be able to evaluate every committed water update without
making site generation more frequent or coupling its reliability to the
website build.

The intended near-term cycle is:

```text
water ingestion
      |
      v
water pond commit -- every 15 minutes
      |
      +--> refresh recent projection
      |       |
      |       +--> evaluate water monitors
      |
      +----------------------------+
                                   |
site-generation scheduler -- every 3 hours
                                   |
                                   v
                         read latest pond state
```

The longer-term system may receive Arrow-native telemetry through a Rust MQTT
surface and the OpenTelemetry OTAP dataflow engine. Those paths could admit
data to monitoring before a tlogfs commit. They are deliberately outside the
initial scope. The projection model should accept a source-neutral batch later
without requiring its monitor runtime to acquire pond dependencies.

## 1. Primary requirement

The design is governed by one guarantee:

> After projection generation `N` is published, scheduled monitoring remains
> functional against `N` if the source pond becomes unavailable or projection
> refresh stops.

This implies:

- A published generation is self-contained.
- It contains no live tlogfs readers or pond-backed `TableProvider`s.
- Refresh is atomic; partial candidates are never visible.
- Refresh failure preserves the previous valid generation.
- Monitor evaluations expose the age and source frontier of their input.
- Source failure never silently looks like healthy, empty data.
- Site generation failure has no effect on projection or monitor execution.

Memory materialization may also improve repeated query cost, but that is not
its reason for existence.

## 2. Goals

1. Present a bounded recent-time view of selected pond series to DataFusion.
2. Preserve a coherent source frontier across all tables in one generation.
3. Consume and publish each authoritative pond content commit in strict order.
4. Continue monitoring the last valid generation during pond outages.
5. Make stale data explicit to monitor policy and alert output.
6. Reuse unchanged Arrow data between generations.
7. Bound retained memory and reject incomplete candidates under pressure.
8. Leave a clean input seam for future MQTT and OTAP Arrow batches.
9. Eventually support restart without immediate pond access through a local
   projection checkpoint.

## 3. Non-goals

The initial project does not:

- replace tlogfs as the authoritative telemetry store;
- introduce a second writable TinyFS filesystem;
- alter current ingestion or site-generation scheduling;
- execute arbitrary effectful factories in the monitor process;
- provide exactly-once external notifications;
- handle multiple ponds in one consistency domain;
- consume pre-commit MQTT or OTAP data;
- persist the first implementation's projection across process restart;
- survive failure of the monitor process itself in the initial single-process
  deployment.

## 4. Why this is not `MemoryPersistence`

`MemoryPersistence` models TinyFS nodes, byte streams, and file versions. The
monitoring projection needs resolved Arrow schemas, `RecordBatch` chunks,
event-time bounds, cheap eviction, immutable publication, and DataFusion table
providers.

Routing a projected table through `MemoryPersistence` would:

- serialize Arrow back into a byte-oriented filesystem representation;
- duplicate persistence and version semantics that tlogfs already owns;
- carry TinyFS transaction and handle dependencies into monitoring;
- make atomic multi-table publication difficult;
- weaken the desired failure-domain boundary.

TinyFS remains important at the projection input boundary. It supplies the
namespace, typed nodes, version metadata, bounded reads, and dynamic recipes.
The published output is a smaller query-oriented model.

## 5. Consistency unit

A projection generation is an immutable view of one committed pond frontier:

```rust
struct ProjectionGeneration {
    projection_id: ProjectionId,
    generation: u64,
    source: SourceFrontier,
    window: WindowDefinition,
    namespace: ProjectedNamespace,
    tables: HashMap<TableId, ProjectedTable>,
    monitors: Vec<MonitorDefinition>,
    query: Arc<GenerationQueryContext>,
    built_at_micros: i64,
}

struct SourceFrontier {
    pond_id: PondId,
    txn_seq: i64,
    commit_hash: ObjectHash,
    parent_commit_hash: Option<ObjectHash>,
    root_tree_hash: ObjectHash,
    manifest_root: ObjectHash,
    committed_at_micros: i64,
}
```

Bootstrap resolves all required inputs from one committed snapshot. Incremental
refresh then consumes the pond's immutable `watertown.commit.v2` objects in
strict parent-hash and transaction-sequence order. Each commit carries the
resulting manifest root and its canonical bounded manifest changes. A
generation is the result of applying exactly one complete commit to the prior
generation; it must not combine table A at transaction 418 with table B at
transaction 417 unless the projection explicitly declares separate consistency
domains.

Each monitor evaluation pins an `Arc<ProjectionGeneration>` and uses only that
generation's DataFusion catalog. A concurrent refresh may publish the next
generation, but the running evaluation completes against the generation it
started with.

## 6. Component boundaries

```text
                  pond-dependent process or component
       +------------------------------------------------+
       |                                                |
tlogfs pond --> ProjectionSource --> ProjectionBuilder  |
       |                                  |             |
       +----------------------------------|-------------+
                                          |
                               validated immutable image
                                          |
       +----------------------------------|-------------+
       |                                  v             |
       |                          ProjectionHost         |
       |                                  |             |
       |                         DataFusion catalog      |
       |                                  |             |
       |                          MonitorRuntime         |
       |                                                |
       +------------------------------------------------+
                   pond-independent monitoring domain
```

The initial components run in one process. Their crate and API boundaries make
source and refresh-task failure isolation real and testable, but they do not
claim process-crash isolation. A later deployment may move projection refresh
to a separate process without changing the generation or monitor APIs.

### 6.1 `ProjectionSource`

This is the only projection component that understands tlogfs:

```rust
#[async_trait]
trait ProjectionSource {
    async fn bootstrap(
        &self,
        spec: &ProjectionSpec,
    ) -> Result<SourceSnapshot>;

    async fn commits_after(
        &self,
        frontier: &SourceFrontier,
        limit: usize,
    ) -> Result<Vec<sync_store::content::Commit>>;

    async fn materialize(
        &self,
        spec: &ProjectionSpec,
        commit: &sync_store::content::Commit,
    ) -> Result<CommitMaterialization>;
}
```

Its responsibilities are:

- open one consistent committed snapshot for bootstrap;
- resolve configured TinyFS paths and globs;
- list selected physical-series versions;
- use `SeriesReadBounds` for conservative version pruning;
- decode selected versions into Arrow batches;
- collect timestamp-column and schema metadata;
- collect monitor definitions and pure dynamic recipes;
- expose the existing shared content `Commit` as the authoritative incremental
  change contract;
- read the permanent commit chain in strict parent/sequence order;
- materialize selected objects and changed-series packs named by each commit;
- authenticate the exact source frontier and carry its target manifest root;
- return owned data with no live pond-backed handles.

The first implementation reads commits from the pond-local authoritative log
and content store. A later source adapter may read the same commit and content
model from remote storage. Bootstrap and recovery may materialize a complete
manifest, but normal refresh does not invent a second projection-specific
change feed or rescan the complete namespace.

### 6.2 `ProjectionBuilder`

The builder converts a bootstrap snapshot or next content commit into a
candidate generation:

```rust
struct ProjectionBuilder {
    current: Arc<ProjectionGeneration>,
    candidate: MutableGeneration,
}
```

It:

- reuses unchanged chunks from the current generation;
- applies canonical manifest additions, replacements, moves, and removals;
- adds newly committed selected series material;
- merges compatible schemas;
- applies exact event-time filtering;
- removes physically expired chunks;
- resolves monitor dependencies;
- measures retained Arrow buffer memory;
- validates the complete candidate;
- builds a generation-scoped DataFusion catalog;
- freezes the candidate into an immutable generation.

The builder cannot publish. A failed build is discarded as a unit.

### 6.3 `ProjectionHost`

The host owns the current generation and has no tlogfs dependency:

```rust
struct ProjectionHost {
    current: ArcSwap<ProjectionGeneration>,
    status: watch::Sender<ProjectionStatus>,
}

impl ProjectionHost {
    fn snapshot(&self) -> Arc<ProjectionGeneration>;
    fn status(&self) -> ProjectionStatus;
    fn publish(&self, candidate: ProjectionGeneration);
}
```

Publication is a single atomic pointer replacement. Existing readers retain
their old `Arc` and its catalog; new readers receive the new generation.
`ProjectionHost` does not expose independent table lookup, because resolving a
table through the mutable host after pinning a generation could mix providers
from different frontiers.

### 6.4 `MonitorRuntime`

The monitor runtime consumes only a projection host, a clock, monitor state,
and alert sinks:

```rust
struct MonitorRuntime {
    projection: Arc<ProjectionHost>,
    clock: Arc<dyn Clock>,
    state: Arc<dyn MonitorStateStore>,
    sinks: Vec<Arc<dyn AlertSink>>,
}
```

It does not know how to:

- open or transact with a pond;
- read Delta Lake;
- resolve a TinyFS path;
- locate a Parquet file;
- use a pond format cache;
- regenerate a website.

This negative interface is essential to the reliability goal.

## 7. Projection configuration

An initial configuration could be:

```yaml
id: water-monitoring

source:
  pond: water

selection:
  include:
    - /observations/**
    - /telemetry/water/**
  exclude:
    - /derived/site/**

window:
  event_time: 2d
  lateness_allowance: 6h

refresh:
  interval: 15m
  timeout: 5m

resources:
  memory_limit: 2GiB

freshness:
  warn_after: 30m
  fail_after: 2h
```

The physical retention window is:

```text
event-time query window + lateness allowance
```

For a two-day query window and a six-hour lateness allowance, the projection
retains 54 hours. Exact monitor queries still apply a two-day predicate. The
allowance permits late records to alter recent monitor results without
retaining unbounded history.

## 8. In-memory table representation

Tables retain chunks corresponding to source versions:

```rust
struct ProjectedTable {
    id: TableId,
    pond_path: String,
    file_id: FileID,
    timestamp_column: String,
    schema: SchemaRef,
    chunks: Arc<[ProjectedChunk]>,
    loaded_through_version: u64,
    last_changed_txn_seq: i64,
    last_changed_at_micros: i64,
}

struct ProjectedChunk {
    source_version: u64,
    content_hash: Option<String>,
    min_event_time: i64,
    max_event_time: i64,
    batches: Arc<[RecordBatch]>,
    memory_bytes: usize,
}
```

This shape allows:

- sharing unchanged chunks between generations;
- allocating only new or boundary data during refresh;
- deduplication by source file and version;
- dropping fully expired chunks without scanning rows;
- filtering only boundary chunks;
- exposing chunks as DataFusion partitions without copying all rows into a
  replacement `MemTable`.

If source versions lack temporal bounds, they must be retained conservatively
until decoded and bounded. The builder measures the resulting Arrow buffers and
rejects an over-budget candidate. Missing bounds must never silently drop data.

## 9. Time semantics

The system distinguishes:

- **event time:** timestamp carried by an observation;
- **commit time:** time at which tlogfs committed its version;
- **projection time:** time at which a generation was built;
- **evaluation time:** time at which a monitor query runs.

A typical monitor window is:

```text
event_time >= evaluation_time - configured_window
```

Per-version minimum and maximum event times are pruning metadata, not a
correctness predicate. The projector or query must apply an exact row-level
predicate.

Evaluation correctness must not depend on physical eviction running at an
exact instant. If the clock advances while no pond commit occurs, queries use a
new cutoff against the same immutable generation. A maintenance refresh may
later remove chunks that cannot match any valid query.

## 10. Refresh protocol

The refresh state machine is:

```text
Idle
  |
  v
Discovering source frontier
  |
  v
Building candidate
  |
  v
Validating candidate
  |
  +---- failure ----> retain current generation; report degraded
  |
  v
Publishing atomically
  |
  v
Idle
```

Attempted and published frontiers are distinct:

```rust
struct ProjectionStatus {
    state: RefreshState,
    current_generation: u64,
    published_frontier: SourceFrontier,
    attempted_frontier: Option<SourceFrontier>,
    last_success_micros: i64,
    last_error: Option<ProjectionError>,
    memory_bytes: usize,
}
```

If transaction 418 is corrupt or incompatible, generation 417 remains current.
The next refresh retries 418 and later commits remain queued. Strict ordering
does not permit a skip, including an acknowledged skip. Recovery requires
repairing the referenced source content, deliberately changing the projection
specification, disabling an invalid monitor, or restoring a verified snapshot
at exactly frontier 418.

### 10.1 Initial snapshot

For each selected series:

1. Compute the physical retention cutoff.
2. Use `SeriesReadBounds::from_event_time_lo(cutoff)`.
3. List and load retained source versions.
4. Decode them into Arrow.
5. Apply exact event-time filtering where required.
6. Record source version, hash, schema, and temporal bounds.
7. Build monitor definitions and dependencies.
8. Validate and publish one complete generation.

### 10.2 Incremental refresh

For each next commit in the chain:

1. Verify pond identity, transaction sequence, parent commit hash, and commit
   hash.
2. Apply its canonical manifest changes to the projected namespace, including
   additions, removals, renames, type changes, and symlink changes. Resolve
   affected paths using the before/after records and bounded parent-chain
   lookups against the persistent manifest map.
3. Determine which changed nodes enter, leave, or remain in the configured
   selection.
4. Materialize selected introduced objects and changed-series packs.
5. Deduplicate series material by `(FileID, version)` and verify repeated
   content hashes.
6. Combine unseen versions with the physical event-time cutoff.
7. Reuse unchanged chunks and evict expired chunks.
8. Verify materialized object and pack hashes against the authenticated commit,
   and record its manifest root as the candidate frontier.
9. Build and validate a complete candidate.
10. Atomically publish one generation for that commit.
11. Evaluate all enabled monitors.

A commit that changes no selected Arrow data still publishes a metadata-only
generation, reusing every table chunk. This records that the complete pond
chain has been verified without pretending that any selected series received
new observations. In the initial implementation every published generation
triggers all monitors; a separate timer evaluates monitors when commits stop so
pending durations, logical windows, and freshness thresholds still advance.

At approximately 15-minute water commits, a two-day window spans about 192
commit intervals per continuously updated series. This is a reasonable initial
scale while still exercising long-running refresh behavior.

## 11. Candidate validation

A candidate is publishable only when:

- its frontier is not older than the current frontier;
- every node selected from the resulting namespace resolves;
- every required table has a known timestamp column;
- every Arrow batch matches the published schema;
- schema evolution is explicitly compatible;
- exact time filtering succeeds;
- no duplicate source versions have conflicting hashes;
- retained Arrow buffer memory is within the configured projection limit;
- no table is incomplete because an input read failed.

Monitor inputs may be declared optional. A missing required monitor dependency
invalidates that monitor but does not invalidate the data generation. It must
never be replaced by an empty table: zero matching rows could otherwise be
mistaken for a healthy condition.

Monitor validation is isolated from data-generation validation. Each enabled
monitor's SQL is parsed and planned against the candidate catalog. An invalid
monitor enters a configuration-error state, retains any prior firing condition
instead of resolving it, and emits a separate configuration incident. It does
not prevent publication of the data generation or execution of other monitors.
A deleted or renamed required series therefore requires a deliberate monitor
configuration update; the host neither preserves a ghost table nor substitutes
an empty one.

## 12. Memory-pressure policy

The projector should not evict arbitrary in-window data to admit a fresh
generation.

It should:

1. Drop chunks outside physical retention.
2. Reuse existing immutable chunks.
3. Measure candidate Arrow buffer memory before publication.
4. Reject the candidate if it still exceeds the retained projection limit.
5. Continue monitoring the previous complete generation.
6. Report memory pressure and increasing pipeline staleness.

Old but known-complete data is safer than a new incomplete projection.

The configured window is fixed and tuned by the operator for the deployment's
available memory. The projector never shortens it automatically under pressure.
The projection `memory_limit` bounds retained Arrow buffers, not total process
RSS: candidate construction, pinned prior generations, and DataFusion
execution can overlap. DataFusion therefore uses its own execution memory pool
and query-admission limit. The configuration and status API should name these
budgets separately rather than claim one hard process-memory bound.

## 13. Projected namespace

The monitor runtime does not need a complete operational TinyFS. It needs
stable names for projected inputs and enough metadata to explain dependencies:

```rust
struct ProjectedNamespace {
    by_path: HashMap<String, ProjectedNode>,
    by_id: HashMap<FileID, String>,
}

enum ProjectedNode {
    Directory,
    Table(TableId),
    OrdinaryFile(SourceDescriptor),
    DynamicRecipe {
        factory: String,
        config: Bytes,
    },
    Symlink {
        target: String,
    },
}
```

Projected tables are self-contained. Other nodes may be visible for discovery
without being readable in the isolated runtime unless explicitly
materialized. The namespace does not pretend to support TinyFS writes,
transactions, arbitrary historical versions, or mutation.

## 14. DataFusion catalog

Each generation owns a catalog and query context containing only self-contained
providers. The provider for a projected table scans its immutable Arrow chunks
as partitions. It must not call back into `ProviderContext.persistence`.

Monitor SQL is planned against the candidate catalog during validation and
again executed against a pinned published generation. A provider cache, if
used, is generation-scoped. Query code receives the pinned generation, never a
mutable-host table resolver, so providers from different frontiers cannot be
mixed.

## 15. Factory reuse

The current `ProviderContext` combines a DataFusion session with
`PersistenceLayer`, TinyFS transaction construction, pond/cache paths, provider
caches, and site-export hints. Importing it unchanged would violate the
monitor-runtime boundary.

A smaller query-side interface is needed:

```rust
struct QueryContext {
    session: Arc<SessionContext>,
    tables: Arc<dyn TableResolver>,
}

#[async_trait]
trait TableResolver {
    async fn resolve(
        &self,
        reference: &str,
    ) -> Result<Arc<dyn TableProvider>>;
}
```

Factories should eventually declare capabilities:

```rust
enum FactoryCapability {
    PureTableTransform,
    PondRead,
    PondWrite,
    ExternalIo,
    Executable,
}
```

Only pure table transforms belong inside the isolated monitor runtime.
Candidate examples include SQL-derived tables, joins, pivots, renames, and
temporal reductions when every input is already projected. Ingest, storage,
site generation, initialization, and executable factories remain outside.

The first implementation should use plain SQL over projected physical tables.
Factory refactoring should follow only after the reliability boundary works.

## 16. Monitor model

A monitor definition identifies:

- a stable monitor ID;
- SQL producing zero or more active conditions;
- a condition-key column;
- evaluation interval;
- required projection freshness;
- pending and resolution durations;
- no-data and stale-data policies;
- labels, annotations, and notification routes.

Example:

```yaml
id: high-water
interval: 15m
query: |
  SELECT
    station_id AS condition_key,
    MAX(level) AS observed_value
  FROM water_levels
  WHERE timestamp >= $window_start
  GROUP BY station_id
  HAVING MAX(level) > 8.0

for: 30m
resolve_after: 30m

freshness:
  live_inputs:
    - water_levels
  warn_after: 30m
  fail_after: 2h
  on_stale: retain
```

State is keyed by `(monitor_id, condition_key)`:

```rust
enum ConditionState {
    Inactive,
    Pending { since_micros: i64 },
    Firing { since_micros: i64 },
    Resolved { at_micros: i64 },
    Unknown { since_micros: i64 },
}
```

Every evaluation records:

```rust
struct EvaluationContext {
    generation: u64,
    source_frontier: SourceFrontier,
    evaluation_time_micros: i64,
    pipeline_age_micros: i64,
    live_input_ages: HashMap<TableId, i64>,
    window_start_micros: i64,
}
```

Every state transition can therefore explain which pond transaction and
projection generation produced it.

Pipeline freshness and input freshness are distinct. Pipeline age measures the
time since the latest commit successfully consumed by the projector. A monitor
may additionally declare selected series as live inputs and set
monitor-specific thresholds for the time since each input last changed. Static
lookup tables need not be declared live. A pond that continues committing does
not by itself make a stalled live series fresh.

## 17. Freshness behavior

Source unavailability must be visible but must not stop the runtime.

Useful stale-data policies are:

- `retain`: preserve the prior condition state without resolving it;
- `evaluate-stale`: run SQL and label results stale;
- `unknown`: transition conditions to an explicit unknown state;
- `alert`: create a separate projection-freshness incident.

For safety monitoring, the initial default should be:

1. retain active condition state;
2. emit a separate freshness condition;
3. never resolve an alert solely because source data stopped arriving.

## 18. Monitor and notification state

If independence is the objective, monitor state cannot exist only in the
source pond. A small local state store, likely SQLite, should eventually hold:

- current condition states;
- last evaluated generation;
- notification idempotency keys;
- pending notification outbox entries;
- projection checkpoint metadata.

This is operational state, not authoritative telemetry.

Condition updates and outbox insertion should be atomic:

```text
monitor transition
    |
    +--> update condition state
    +--> insert notification outbox row
              |
              v
        asynchronous delivery
```

Email, MQTT, or webhook failure then cannot break query evaluation. External
delivery is idempotent by monitor, condition key, transition, and generation.

## 19. Restart independence

The first implementation may rebuild from tlogfs after restart. Runtime
independence during an outage is the first milestone; restart independence is
the next.

A later local checkpoint can use Arrow IPC or Parquet chunks:

```text
projection/
  current.manifest
  generations/
    000042/
      manifest.json
      table-<id>-chunk-<version>.arrow
```

Checkpoint publication is:

1. Write a candidate under a temporary generation name.
2. Flush and validate every file.
3. Write and flush the generation manifest.
4. Atomically rename the generation into place.
5. Atomically replace `current.manifest`.
6. Publish the equivalent in-memory generation.

On restart:

1. Load the newest valid local checkpoint.
2. Resume monitoring immediately with recorded freshness.
3. Attempt pond refresh in the background.
4. Atomically replace the generation when refresh succeeds.

The checkpoint is reconstructible and does not become a second authoritative
pond.

## 20. Future live input seam

Future MQTT and OTAP paths should produce a source-neutral envelope:

```rust
struct ProjectionBatch {
    batch_id: BatchId,
    destination: PondPath,
    source_file: Option<FileID>,
    source_version: Option<u64>,
    schema_fingerprint: SchemaFingerprint,
    min_event_time: Option<i64>,
    max_event_time: Option<i64>,
    batches: Arc<[RecordBatch]>,
}
```

The initial tlogfs source adapter produces this from committed versions.
Future Arrow-native ingestion can produce it before or alongside persistence.
Deduplication requires a stable batch identity persisted with the corresponding
tlogfs version.

That future path must preserve:

- tlogfs as recovery truth;
- idempotent reconciliation between live and committed data;
- explicit provisional versus committed evaluation frontiers;
- monitor independence from the transport implementation.

No MQTT or OTAP dependency is required to implement the current design.

## 21. Failure behavior

| Failure | Required behavior |
|---|---|
| Pond unavailable | Continue monitoring the current generation; freshness degrades |
| Projection refresh task stops or panics | The monitor runtime continues serving its current generation; supervision reports and restarts refresh |
| Monitor process crashes, phase 1 | Monitoring is unavailable until the process restarts and rebuilds from the pond |
| Refresh fails | Retain current generation and retry the same source frontier |
| One commit or source version is corrupt | Reject the whole candidate; block later commits until operator recovery |
| Schema becomes incompatible | Reject candidate and report the affected table |
| Memory limit is exceeded | Reject candidate; never install partial data |
| One monitor definition is invalid | Retain its prior firing state, mark it configuration-error unknown, emit a configuration incident, and continue publication |
| One monitor execution fails | Retain its prior firing state, record an evaluation error, and continue other monitors and refresh |
| Notification endpoint fails | Outbox retries; monitor execution continues |
| Site generation fails | No effect on projection or monitors |
| Query overlaps publication | Query completes against its pinned generation |
| Clock advances without commits | Exact query cutoff advances; freshness is accurate |
| Monitor process restarts, phase 1 | Rebuild from pond before evaluating |
| Monitor process restarts, checkpoint phase | Load local generation and evaluate while refreshing |

## 22. Testing strategy

### 22.1 Projection correctness

- A static projection returns the same bounded rows as direct tlogfs queries.
- Exact row filtering removes old rows from boundary versions.
- Versions wholly outside the window are not materialized.
- Unknown temporal bounds are retained conservatively.
- Duplicate source versions do not duplicate rows.
- Conflicting content for the same source version fails validation.
- Compatible schema evolution produces the expected merged schema.
- Incompatible schema evolution rejects the candidate.
- Commit parent/hash/sequence discontinuity blocks publication.
- Manifest additions, removals, renames, and type changes update the projected
  namespace exactly.
- A metadata-only commit publishes a new frontier while reusing all Arrow
  chunks.

### 22.2 Atomicity and concurrency

- A query begun on generation `N` finishes on `N` while `N+1` publishes.
- New queries use `N+1` immediately after publication.
- No query observes a mixture of generations.
- A failed candidate leaves the current pointer unchanged.
- Shared old chunks remain alive until the last pinned generation releases
  them.

### 22.3 Time behavior

- Rows age out logically without a new pond commit.
- Physical maintenance later releases expired chunks.
- Late rows inside the allowance enter the correct query window.
- Rows outside the allowance follow the configured policy.
- A controlled test clock makes every cutoff deterministic.

### 22.4 Failure isolation

- Build a generation, remove all pond access, and continue querying it.
- Continue scheduled monitor transitions while pond refresh fails.
- Surface increasing pipeline age.
- Preserve a firing condition during pipeline or live-input staleness.
- Recover by publishing a newer complete generation when pond access returns.
- Monitor SQL failure does not affect other monitors.
- Invalid monitor configuration does not block data-generation publication.
- Deleting a required series disables the dependent monitor without installing
  an empty replacement table.
- Notification failure does not affect monitor state.

### 22.5 Memory

- Candidate memory estimation includes Arrow buffers without double-counting
  shared buffers.
- Incremental refresh shares unchanged chunks.
- Expired chunks are released after pinned generations finish.
- Over-budget candidates are rejected before publication.
- Repeated refreshes reach a memory plateau for a fixed physical window.
- Unknown temporal bounds are decoded conservatively and charged to the same
  retained-buffer limit.

### 22.6 Restart checkpoint

- A complete checkpoint loads without pond access.
- A partially written generation is ignored.
- A corrupt current manifest falls back to the newest valid generation.
- Condition state and notification outbox recover independently.

## 23. Implementation phases

### Phase 1: Static independent projection

- Define immutable generation, table, chunk, frontier, and status types.
- Build one two-day projection from a committed water pond snapshot.
- Register self-contained DataFusion providers.
- Close or deny access to the source pond.
- Prove queries continue to run.

**Acceptance test:** after generation 1 is installed, remove source-pond access,
advance a controlled clock through several query intervals, and obtain correct
bounded results with accurate stale metadata.

### Phase 2: Atomic refresh

- Add `ProjectionHost`.
- Build candidates without changing the published generation.
- Validate and atomically replace generations.
- Test concurrent queries during replacement.

### Phase 3: Incremental committed updates

- Consume the shared content commit chain in strict parent/sequence order.
- Apply canonical manifest changes, including removals and renames.
- Load only selected introduced objects and changed-series packs.
- Reuse unchanged chunks and evict expired chunks.
- Publish metadata-only generations for commits with no selected Arrow changes.
- Reject and retry blocked commits without advancing to later frontiers.

### Phase 4: SQL monitor runtime

- Define monitor SQL and condition-key contracts.
- Add freshness, no-data, pending, firing, and resolution behavior.
- Isolate invalid monitor definitions from projection publication.
- Evaluate all monitors after every published commit and on a separate timer.
- Store initial condition state in memory.

### Phase 5: Local operational state

- Persist condition state and notification outbox in SQLite.
- Add idempotent alert delivery.
- Preserve monitor progress across runtime restart.

### Phase 6: Projection checkpoint

- Persist immutable Arrow generations locally.
- Load the last valid generation before contacting the pond.
- Refresh in the background.

### Phase 7: Pure factory reuse

- Introduce `QueryContext` and `TableResolver`.
- Classify factory capabilities.
- Migrate selected pure transforms into the isolated runtime.

### Phase 8: Future live admission

- Add MQTT and OTAP `ProjectionBatch` producers.
- Persist stable batch identities in tlogfs.
- Reconcile provisional live data with committed versions.
- Preserve the same projection and monitor interfaces.

## 24. Relationship to TinyFS redesign

This project should precede broad `PersistenceLayer` decomposition because it
will reveal the practical boundaries more clearly:

- namespace and node persistence;
- versioned-series access;
- committed snapshot and change feed;
- projection/read policy;
- query-provider construction;
- writable transactions.

The projection should not force hostmount, overlay, or memory persistence to
pretend they support versioned live views. Future capability traits can express
which backends provide committed frontiers, version metadata, and incremental
change discovery.

The independent hostmount rename defect remains valid but is orthogonal to this
design.

## 25. Open decisions

1. When the refresh component should move out of the initial monitor process
   and which explicit image-transfer protocol it should use.
2. How selected paths map to stable SQL table names.
3. Which schema changes are automatically compatible.
4. Whether monitor definitions live in the source pond, local configuration,
   or both with explicit precedence.
5. Which stale-data policy is the default for safety monitors.
6. Whether phase-one monitor state is in memory or starts directly with SQLite.
7. Whether Arrow IPC or Parquet is preferable for local generation
   checkpoints.
8. Which existing factories qualify as pure transforms after dependency
   review.

These decisions can be made incrementally. None changes the core invariant:
only a complete, validated, self-contained generation is published to the
monitor runtime.
