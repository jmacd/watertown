# DataFusion Query Foundation Correctness and Performance Plan

> **Status:** accepted design and implementation plan as of 2026-09-28.
>
> **Implementation:** Phase 0 passed in `477292a8`; the Phase 1 through Phase 8
> gates are implemented on the current development branch.
>
> **Historical baseline:** `b6913e2f9ca68eb0a1cad27970fca2f6e8487833`.
>
> **Research commits:** `ccba84e0`, `4f28e44b`, `a0b117b3`, and `cbf62ddb`.
> These commits contain useful regressions and experiments, but they are not the
> architectural starting point for this plan.
>
> **First delivery gate:** prove every Watertown query shape in a fresh
> workspace crate before integrating production TinyFS, TLogFS, provider
> factories, or backup.

## 1. Decision

Watertown will build a fresh executable foundation for its DataFusion
integration instead of continuing to repair join, pivot, combine, reduce, and
materialize behavior independently.

The foundation will:

1. live in a new Watertown workspace crate;
2. initially depend on Arrow, DataFusion, Parquet, and `object_store`, not
   TinyFS, TLogFS, `provider`, `steward`, or `sync-store`;
3. model immutable query snapshots and chunks explicitly;
4. construct built-in operations as typed DataFusion logical plans and
   expressions;
5. reserve SQL text for user-authored SQL;
6. prove all current timeseries and non-timeseries query shapes before
   production storage integration;
7. treat measured work, I/O, and memory as correctness requirements;
8. tolerate bounded out-of-order arrival through a settled frontier and an
   efficiently queryable unsealed region;
9. declare overlap and row-identity semantics per dataset or composition; and
10. integrate upward through TinyFS, TLogFS/Delta, provider factories, site
    consumers, and native-v2 incremental backup only after the isolated model
    passes its gates.

The working crate name in this document is `query-foundation`. The name may
change before scaffolding, but the dependency boundary and gates may not be
weakened by a rename.

## 2. Why the previous approach is insufficient

The immediate production symptom was a Noyo pond run taking far longer than
the amount of source data could justify. The graph repeatedly evaluated
dynamic sources across parameters and resolutions, and some paths
re-materialized equivalent work many times in one run.

That incident exposed a broader pattern:

- time bounds sometimes selected versions without filtering rows;
- projections or predicates stopped at custom provider and execution wrappers;
- generated SQL obscured the intended operator and locality contracts;
- materialization could evaluate complete history to produce a small suffix;
- materialization collected and copied the complete result before writing;
- late data was conflated with append progress and could be omitted silently;
- same-scope archive/live composition had no explicit overlap semantics;
- cache identity, query identity, source freshness, and wildcard membership
  were mixed together;
- per-execution optimization and cross-execution reuse were treated as one
  problem; and
- fixes were made inside production factory implementations before a small
  executable model proved that the underlying DataFusion abstractions compose
  correctly and efficiently.

The result was a game of whack-a-mole: fixing one plan shape could expose or
reintroduce a defect in another because the system lacked shared contracts for
snapshot visibility, bounds, locality, row identity, change impact, and
physical work.

The earlier plan correctly identified many concrete defects. Its mistake was
to continue sequencing production factory changes before establishing this
shared foundation.

## 3. Architectural principles

### 3.1 Separate five independent concepts

The foundation must not conflate:

| Concept | Meaning |
|---|---|
| Snapshot identity | Exact immutable chunk membership visible to one query |
| Event time | Domain time used for bounds, joins, windows, and reduction |
| Chunk sequence | Deterministic arrival/publication order |
| Row identity | Dataset-specific duplicate or revision semantics |
| Physical packing | Parquet objects, row groups, compression, and backup packs |

Consequences:

- event time need not be ordered by chunk arrival;
- chunk sequence does not imply last-write-wins unless a dataset declares that
  policy;
- a timestamp is not automatically a unique row key;
- replacing a physical Parquet layout without changing logical rows must not
  change logical content identity; and
- a query snapshot names exact logical input membership without relying on
  object-store directory listing.

### 3.2 Standard DataFusion operators first

The foundation should use ordinary DataFusion logical and physical operators
wherever their semantics suffice:

- projection;
- filter;
- union;
- join;
- aggregate;
- sort only when semantically required; and
- standard Parquet scans.

Aliases, casts, scope prefixes, and typed null padding are projection
expressions. Built-in join, pivot, combine, and reduce operations are typed
logical plans, not generated SQL strings.

A custom optimizer rule or execution node is permitted only after a regression
demonstrates that standard DataFusion cannot satisfy a measured requirement.
The custom component must then preserve or conservatively discard projection,
predicate, ordering, equivalence, partitioning, memory, and metric properties.

### 3.3 Per-execution efficiency and cross-execution reuse are different

Within one execution:

- requirements must reach physical leaves;
- a source must not be scanned repeatedly unless the operation requires it;
- streaming operators must not collect unbounded output;
- unnecessary global sorts and distinct operations are defects; and
- peak memory must be bounded by active execution state.

Across executions:

- immutable decoding may be cached by content identity;
- unchanged aggregate buckets may be reused;
- coarser reductions may fold finer partials;
- a no-change run must scan zero source rows; and
- an append or bounded late repair must cost work independent of retained
  history.

A persistent cache cannot compensate for an inefficient plan within one
execution.

### 3.4 No silent fallback

Watertown must not silently:

- drop late rows;
- change overlap semantics;
- treat an unsupported local query as incremental;
- fall back from an incremental path to a full-history path;
- use incomplete cache directory contents as authoritative;
- weaken transaction-generation checks;
- collect a stream because a streaming sink failed; or
- perform a full backup inventory because an incremental baseline is unknown.

Unsupported or invalid states return an actionable error. An intentionally
global plan must be visible in plans and metrics.

## 4. Foundation data model

The isolated crate will model exact query snapshots over immutable chunks.
Names below are illustrative; behavior is normative.

```rust
struct DatasetSnapshot {
    snapshot_id: SnapshotId,
    schema: SchemaRef,
    chunks: Arc<[ChunkDescriptor]>,
    event_time: Option<EventTimeContract>,
    overlap: OverlapPolicy,
}

struct ChunkDescriptor {
    chunk_id: ContentId,
    sequence: u64,
    object: ObjectDescriptor,
    schema: SchemaRef,
    logical_count: u64,
    event_time_bounds: Option<TimeInterval>,
    column_statistics: ChunkStatistics,
    ordering: Vec<SortExpr>,
    uniqueness: UniquenessContract,
}
```

Required properties:

1. Chunks are immutable.
2. A snapshot names the exact ordered chunk set.
3. A query captures one snapshot before planning.
4. Publishing a later snapshot cannot change an existing query.
5. Missing statistics reduce pruning but never remove data.
6. Known statistics may prune only when non-overlap is proven.
7. Logical content identity is independent of physical Parquet packing.
8. Timeseries metadata is optional; ordinary tables use the same provider
   boundary.
9. Schema evolution is explicit and tested.
10. Object access is range-readable and instrumented.

The initial model uses opaque test identities. It must not import current
`sync-store` wire types merely to share hashes. Production identity adapters
will be added after the query contracts are stable, avoiding a dependency
cycle between query, filesystem, and replication crates.

## 5. Overlap and row identity

There is no universal duplicate policy. Every dataset or same-scope
composition declares one of the following classes:

```rust
enum OverlapPolicy {
    PreserveAll,
    RequireDisjoint,
    RejectDuplicateKey { key: Vec<ColumnId> },
    PreferBySequence { key: Vec<ColumnId> },
}
```

The exact API may evolve, but these semantic choices must remain distinct.

### 5.1 Preserve all

All rows are events. Equal timestamps or equal values do not imply
duplication. Joins and pivots must account explicitly for multiplicity and
tests must cover many-to-many results.

### 5.2 Require disjoint

Chunk event-time ranges or declared logical keys may not overlap. The
foundation validates the contract and returns an error on violation. This is
appropriate when archive and live sources have an operational handoff that
must be clean.

### 5.3 Reject duplicate key

Overlapping ranges are allowed, but duplicate logical keys are invalid. The
error must identify the conflicting sources and key.

### 5.4 Prefer by sequence

A declared key identifies revisions, and a deterministic chunk sequence
selects the winner. Predicates that could change winner selection must remain
above reconciliation. This is an optional dataset policy, not Watertown's
universal timeseries model.

### 5.5 Same-scope combine

The existing use of a full-row distinct union does not establish any of these
semantics and blocks normal projection. The foundation will represent
same-scope composition explicitly:

- disjoint or preserve-all composition uses `UNION ALL BY NAME`;
- duplicate rejection performs only the key validation required by the
  contract;
- precedence performs key-based reconciliation with explicit ordering; and
- no implementation may use global full-row distinct as an undocumented
  approximation.

## 6. Event time, lateness, and the settled frontier

Slightly out-of-order data is expected. The foundation therefore models:

```text
settled history | efficiently queryable unsealed region | future
```

The settled frontier is a progress and cost promise, not a claim that
retroactive data is physically impossible.

Required behavior:

1. Immutable chunks may arrive out of event-time order.
2. Disorder inside the unsealed region is ordinary.
3. A source supplies or explicitly derives `settled_through`.
4. The engine does not invent a frontier from wall-clock time without a
   declared source policy.
5. Unsealed queries prune chunks and retain only bounded active state.
6. Ordinary appends do not rescan settled history.
7. A new chunk at or before the settled frontier is a retroactive change.
8. A retroactive change either repairs the affected output interval or returns
   an explicit policy error.
9. No watermark comparison may silently omit a retroactive row.
10. Frontier advancement and output publication are atomic where progress is
    persisted.

Tests must distinguish:

- normal append;
- normal disorder inside the unsealed region;
- a row exactly at the frontier;
- a row behind the frontier but within supported repair;
- a row beyond the supported repair policy; and
- a no-data frontier advance.

## 7. Transform locality and change impact

The term "incremental lineage" is too broad. Each derived operation must
separately describe:

1. the input ranges required for a requested output range;
2. the output ranges affected by an input change; and
3. any persistent state needed across executions.

An illustrative contract is:

```rust
trait IncrementalRecipe {
    fn required_input_ranges(
        &self,
        requested_output: TimeInterval,
    ) -> InputRanges;

    fn affected_output_ranges(
        &self,
        changes: &ChangeSet,
    ) -> OutputRanges;

    fn state_contract(&self) -> StateContract;
}
```

### 7.1 Locality classes

| Class | Examples | Range behavior |
|---|---|---|
| Row-local | projection, rename, cast, null padding | same input/output interval |
| Timestamp-local | timestamp join, pivot, declared combine | same interval on each input |
| Window-local | bucketed reduce, finite rolling window | expand to bucket/window boundaries |
| Stateful append | cumulative operation with checkpointable state | prior state plus suffix |
| Global | rank over all history, unrestricted SQL | complete input unless separately proven |

Join, pivot, and combine are timestamp-local only after their overlap policy
and row-identity semantics are known.

### 7.2 Change sets

A `ChangeSet` must identify at least:

- added immutable chunks;
- removed membership, where the source permits it;
- changed event-time intervals;
- wildcard member additions or removals;
- source recipe changes;
- schema changes; and
- settled-frontier movement.

Semantic recipe identity and physical freshness are separate:

- changing a transform changes recipe identity;
- appending an ordinary source chunk changes freshness and affected ranges;
- adding a wildcard member changes membership and affected ranges;
- repacking the same logical content changes neither recipe nor logical
  freshness; and
- caches use explicit manifests, never directory listing as authority.

### 7.3 Arbitrary SQL

User SQL remains conservative by default. It may contain global windows,
cumulative calculations, non-local joins, or ordering requirements.

An explicitly declared local SQL mode may be added only after the typed
built-in workloads pass. It must:

- declare output and input event-time columns;
- declare locality or required context;
- validate supported plan shapes after SQL planning;
- include the declaration in recipe identity;
- reject unsupported constructs; and
- preserve an exact output predicate.

The implementation must not infer locality from SQL text.

## 8. DataFusion provider boundary

The intended plan shape is:

```text
DatasetSnapshot
    -> ChunkTableProvider
        -> typed DataFusion logical plan
            -> standard DataFusion physical operators
                -> Parquet DataSourceExec
```

`ChunkTableProvider` is the principal custom provider boundary. It must:

1. capture an immutable `DatasetSnapshot`;
2. expose the snapshot schema;
3. receive projection, filters, and limit through `TableProvider::scan`;
4. use filters to prune candidate chunks conservatively;
5. retain exact residual predicates in the logical plan;
6. build ordinary Parquet sources with per-object statistics;
7. expose valid ordering only when every selected chunk supports it;
8. report `Exact` only when the complete expression is enforced;
9. remain correct when statistics are absent; and
10. publish detailed physical-work metrics.

The provider may report filter support as `Inexact` while using an expression
for chunk pruning. Correctness is established by the retained filter, not by
the pruning decision.

### 8.1 Typed plans

Built-in operations use DataFusion expressions and logical plan builders:

- column transforms are projections;
- same-scope combination is a policy-aware union/reconciliation plan;
- timeseries join is a typed full outer join over declared keys;
- pivot is a typed composition of projections and joins;
- reduction is a typed aggregate plan;
- materialization consumes an execution stream; and
- user SQL is planned through DataFusion's SQL frontend and then classified
  conservatively.

The foundation must not carry both a generated-SQL and typed implementation of
the same built-in operation. One implementation and one set of semantics are
easier to reason about and test.

### 8.2 Projection, filters, and bounds

Every bounded timeseries path must have both:

1. conservative chunk or file selection using metadata; and
2. an exact row predicate in the logical plan.

A version or chunk whose metadata overlaps a requested interval is retained,
but rows outside the interval are still filtered. A narrow projection reaches
the Parquet scan unless an intervening operation semantically requires another
column.

### 8.3 Ordering

Ordering is evidence, not an assumption:

- chunk ordering must be declared and validated;
- union ordering is preserved only when compatible;
- reconciliation may require key ordering;
- cached reduced runs may use sort-preserving merge when their manifests prove
  compatible ranges;
- a global sort is added only when required by the output or downstream
  operator; and
- plan tests reject false ordering claims.

## 9. Fresh-crate module plan

The exact module names may evolve, but responsibilities should begin as:

```text
query-foundation/
  src/
    snapshot.rs       immutable datasets, chunks, membership
    statistics.rs     event-time and column pruning metadata
    overlap.rs        dataset row-identity and overlap contracts
    locality.rs       required-input and affected-output intervals
    provider.rs       ChunkTableProvider
    plans/
      transform.rs
      combine.rs
      join.rs
      pivot.rs
      reduce.rs
    materialize.rs    abstract atomic streaming sink and progress
    metrics.rs        physical work and memory observations
    testkit.rs        instrumented object store and fixture builders
```

Initial dependencies:

- Arrow;
- DataFusion;
- Parquet;
- `object_store`;
- Tokio/futures as required by those APIs; and
- test-only utilities.

The crate must not initially depend on:

- `tinyfs`;
- `tlogfs`;
- `provider`;
- `steward`;
- `sync-store`;
- site generation;
- Noyo configuration; or
- production Delta tables.

Tests will use real Parquet and an instrumented in-memory object store.
`MemTable` is acceptable for small semantic fixtures but cannot satisfy an
I/O, projection, pruning, or memory gate.

## 10. Query-shape inventory

The first delivery gate is complete only when every shape below has result,
plan, I/O/work, memory, and failure evidence where applicable.

### 10.1 Physical tables and series

- one immutable Parquet object;
- multiple immutable chunks as one dataset;
- explicit snapshot membership;
- time-bounded and unbounded scans;
- ordinary non-timeseries filters;
- narrow projection;
- limit;
- absent statistics;
- versions spanning both sides of a bound;
- differing schemas;
- snapshot isolation while a later snapshot is published; and
- invalid or missing physical objects.

### 10.2 Logical transforms

- alias and rename;
- type-preserving cast;
- type-changing cast;
- cast failure;
- scope prefix;
- typed null padding;
- mixed predicates over inner and padded columns;
- timestamp-unit changes;
- projection pruning through every transform; and
- correct residual filters after every transform.

### 10.3 Same-scope combine

- disjoint archive/live ranges;
- overlapping ranges under every policy;
- duplicate timestamps with distinct rows;
- duplicate logical keys;
- deterministic precedence;
- schema-aligned union;
- missing columns;
- wildcard member addition/removal; and
- narrow downstream projection.

### 10.4 Timeseries join

- two-way full outer timestamp join;
- three-way accumulated timestamp join;
- timestamps absent from the first input;
- sparse inputs;
- empty input;
- equal timestamps with multiple rows;
- different scopes;
- same-scope composition before join;
- independently bounded inputs;
- projection through join;
- join followed by filter;
- join followed by reduction; and
- one physical scan per source occurrence.

### 10.5 Timeseries pivot

- one and multiple selected measurements;
- missing measurements;
- missing sites;
- sparse timestamps;
- padded columns;
- multiple rows at one timestamp under declared policy;
- independently bounded source joins;
- adding one parameter reads only that parameter's columns;
- pivot followed by reduction; and
- no separate timestamp-spine rescan.

### 10.6 Reduce and downsample

- fixed-width buckets;
- calendar buckets where supported;
- multiple group keys;
- null values;
- multiple resolutions;
- coarser levels folded from finer partials;
- open buckets in the unsealed region;
- sealed buckets;
- ordinary append;
- out-of-order input within the unsealed region;
- retroactive repair;
- repair rejection when unsupported;
- no-change reuse;
- source membership changes; and
- exact dirty-bucket accounting.

### 10.7 Derived composition

- transform over physical source;
- derived source over derived source;
- combine followed by join;
- join followed by pivot;
- pivot followed by reduce;
- timeseries joined with a dimension table;
- declared timestamp-local SQL;
- unrestricted global SQL;
- window functions;
- finite-lookback calculations;
- cumulative calculations; and
- empty and fully pruned inputs.

### 10.8 Materialization model

Before TinyFS integration, the crate will provide an abstract transactional
stream sink proving:

- bounded streaming write;
- exactly one visible output on success;
- no visible output after a stream failure;
- no visible output after writer close failure;
- no `collect` or complete-result concatenation;
- exact temporal metadata and row count computed incrementally;
- no-change writes no output;
- progress and output publish atomically;
- strict boundary predicates avoid duplicate watermark rows;
- unsealed changes are included;
- retroactive changes repair or fail explicitly; and
- memory is bounded by DataFusion batches and writer buffers.

This sink is a test contract, not a substitute for the later TinyFS adapter.

## 11. Validation framework

Correct rows alone do not satisfy any delivery gate.

### 11.1 Result correctness

Each workload must assert complete rows, schemas, ordering where promised, and
errors where required. Property tests should vary:

- chunk boundaries;
- row-group boundaries;
- projection sets;
- predicate forms;
- input ordering;
- empty chunks;
- missing statistics;
- overlap patterns;
- event-time disorder; and
- batch sizes.

Equivalent logical data with different physical packing must produce identical
query results and logical identities.

### 11.2 Plan shape

Normalize plans and assert structural properties:

- expected `DataSourceExec` count;
- expected leaf projection;
- expected Parquet predicate;
- retained exact row predicate;
- no `MemoryExec` beneath a Parquet query path;
- no full-row distinct for a policy that does not require it;
- no duplicate timestamp-spine scan;
- no unnecessary global sort;
- sort-preserving merge only with proven ordering;
- no scan for pruned chunks; and
- no filter removed because of a false `Exact` declaration.

Assertions must avoid unstable generated identifiers and cosmetic formatting.

### 11.3 I/O and work counters

The test object store and provider must record:

- candidate chunks;
- chunks retained and pruned;
- object metadata calls;
- objects opened;
- range requests;
- bytes requested and returned;
- Parquet row groups considered and decoded;
- available and projected columns;
- input batches and rows;
- rows entering reconciliation, join, and aggregate operators;
- duplicate or conflict counts;
- scans per source;
- partial states reused and rebuilt;
- output batches and bytes; and
- reasons for every cache miss or rebuild.

Tests assert upper bounds. Logging a number without an assertion is not
evidence.

### 11.4 Memory

Use DataFusion's memory pool plus test-specific writer observations to assert:

- peak reserved execution memory;
- peak buffered writer memory;
- no result-sized concatenation;
- join state bounded by the active requested range;
- reduce state bounded by open buckets and configured grouping;
- no full decoded Parquet table retained; and
- no retained-history growth in one-append materialization.

### 11.5 Failure behavior

Inject failures at:

- object metadata read;
- object range read;
- Parquet decode;
- transform evaluation;
- reconciliation;
- DataFusion stream after several batches;
- state or cache write;
- stream writer close;
- progress publication; and
- snapshot publication.

Every failure must be returned. No partial result, state manifest, output
version, or progress record may become authoritative.

### 11.6 Scale invariance

Run representative no-change, append, and late-change cases after 1, 100, and
1,000 prior chunks or logical leaves.

Required asymptotic behavior:

- warm no-change scans zero source rows;
- one append costs the new chunks plus the bounded unsealed region;
- late repair costs intersecting chunks and dirty windows;
- adding a reduction resolution does not rescan raw history;
- adding a pivot parameter reads only required columns;
- peak memory is independent of retained history; and
- source scan count is independent of unrelated consumers.

Absolute wall-clock targets come after structural and physical-work tests are
stable.

## 12. Implementation phases and gates

Each phase should be a separately reviewable sequence of commits. A phase does
not pass until its narrowest measurable efficiency regressions pass.

### Phase 0: preserve evidence and establish the crate

1. Scaffold the fresh workspace crate.
2. Copy no production factory implementation.
3. Reproduce prior bugs as black-box result, plan, or work regressions.
4. Add the instrumented object store and metrics vocabulary.
5. Record DataFusion and Parquet versions used by every baseline.

The initial Phase 0 baseline uses the versions locked on 2026-09-28:

| Component | Version |
|---|---:|
| DataFusion | 51.0.0 |
| Arrow | 57.3.0 |
| Parquet | 57.3.0 |
| `object_store` | 0.12.4 |

**Gate:** the crate builds independently and can prove projection, predicate,
row-group pruning, byte-range reads, and snapshot isolation over real Parquet.

### Phase 1: physical snapshot provider

1. Implement snapshot and chunk descriptors.
2. Implement conservative statistics pruning.
3. Implement `ChunkTableProvider`.
4. Retain exact residual predicates.
5. Preserve valid ordering and discard unsupported claims.
6. Complete the physical table/series matrix.

**Gate:** bounded and projected scans have correct results and hard I/O bounds;
missing statistics cannot cause data loss.

### Phase 2: transforms and overlap semantics

1. Implement logical projection transforms.
2. Implement each overlap policy.
3. Implement typed same-scope combine.
4. Prove safe predicate and projection movement.
5. Prove conflict and duplicate errors.

**Gate:** no transform manually synthesizes physical optimizer metadata, and
same-scope composition performs only work required by its declared policy.

### Phase 3: typed join and pivot

1. Implement accumulated full outer timestamp join.
2. Implement typed pivot.
3. Propagate bounds independently to every input.
4. Prove leaf projection through each plan.
5. Cover sparse, duplicate, empty, and wildcard cases.

**Gate:** every source appears once per required plan occurrence, and no
timestamp spine or full-row distinct creates repeated or global work.

### Phase 4: reduce, downsample, and change impact

1. Implement locality and `ChangeSet` contracts.
2. Implement window-local dirty-range calculation.
3. Implement fine-to-coarse reusable partial state.
4. Implement settled and unsealed behavior.
5. Implement late repair and explicit rejection.
6. Cover scale invariance after 1, 100, and 1,000 chunks.

**Gate:** no-change, append, and repair costs are independent of settled
history, and every rebuild has an asserted reason and range.

### Phase 5: derived composition and SQL boundary

1. Compose every built-in query shape.
2. Add user SQL planning.
3. Keep arbitrary SQL global.
4. Add an explicit local SQL declaration only if needed by a demonstrated
   Watertown workload.
5. Validate declared locality against planned operators.

**Gate:** all current query shapes pass result, plan, work, memory, and failure
tests without production storage dependencies.

### Phase 6: abstract materialization

1. Implement the transactional streaming sink contract.
2. Couple progress and output atomically.
3. Exercise ordinary disorder and retroactive changes.
4. Prove bounded memory and failure atomicity.

**Gate:** every materialization scenario passes without `collect`, silent row
loss, or partial publication.

Phase 6 completes the first delivery gate. Production integration must not
begin earlier merely because one factory-shaped example works.

### Phase 7: TinyFS integration

1. Adapt exact TinyFS version membership to foundation snapshots.
2. Adapt TinyFS/object-store range reads to foundation chunks.
3. Preserve transaction-generation coherence.
4. Implement TinyFS atomic streaming series writing.
5. Map logical leaf metadata without making Parquet layout part of identity.
6. Run the full foundation contract against memory persistence.

**Gate:** memory TinyFS matches the isolated model for results and measured
work, and failed writes expose no version.

The Phase 7 contract runs typed transform, combine, join, pivot, reduce, SQL,
and materialization paths over exact MemoryPersistence versions. It measures
range and metadata operations, proves zero reads for fully pruned input,
proves one-append work remains bounded at 1, 100, and 1,000 retained versions,
and verifies that failed or aborted staged writes expose no version. Logical
chunk identity comes from validated native-v2 leaf metadata and is unchanged
when identical rows are repacked into different Parquet row-group layouts.

### Phase 8: TLogFS and Delta integration

1. Capture one coherent TLogFS/Delta snapshot for planning.
2. Map committed immutable versions to chunk descriptors.
3. Preserve same-transaction visibility and stale-provider rejection.
4. Avoid eager decoding or `MemTable` conversion.
5. Run the complete cross-persistence matrix.
6. Prove Delta age does not increase small-query planning or scan work.

**Gate:** memory and TLogFS produce identical semantics, while TLogFS physical
work remains bounded by selected chunks and row groups.

The Phase 8 cross-persistence contract runs exact snapshot, projected scan,
append, no-output progress, repair rejection, stream failure, commit, abort,
and stale-context cases on both memory and TLogFS. TLogFS committed lookups
are scoped by pond, partition, and node rather than loading a whole directory.
Delta commits write node-isolated Parquet objects with node and version
statistics; an exact-version physical plan is unchanged after 16 additional
commits containing both selected-node history and unrelated nodes.
Existing Delta objects without these statistics remain readable, but require
a maintenance rewrite before they provide the same pruning bound.

### Phase 9: provider and factory integration

1. Make provider construction adapt TinyFS/TLogFS snapshots to the foundation.
2. Convert built-in factory configurations to typed recipes.
3. Integrate combine before join and pivot.
4. Integrate join and pivot before temporal reduction.
5. Integrate reduction and reusable partial manifests.
6. Integrate materialization last.
7. Remove superseded generated SQL and custom execution paths.
8. Make global and non-incremental paths visible in logs and metrics.

**Gate:** the production factories satisfy the same tests and counters as the
foundation; there is no behavior-only adapter that loses the plan contract.

Phase 9 integration now constructs wildcard combines, timeseries joins, and
timeseries pivots as typed logical plans. Temporal reduction retains its
incremental segment/hot cache and reconstructs user-visible aggregates from
stored mergeable partials with typed expressions over the ordered file scan;
its bounded source scan also builds multi-column mergeable partials through a
typed timestamp-window recipe. Segment sealing, hot-window recomputation,
coarser folding, and segment compaction reuse that recipe with typed
half-open event-time bounds and associative partial merges. The production
plan has neither `MemoryExec` nor a global `SortExec`. The watermark frontier
is a typed one-row maximum over aligned buckets and does not allocate
per-bucket state. The non-cache single-pass path uses the same typed source
registration, partial recipe, and reconstruction; generated reduction SQL
remains only as a test oracle. Production `materialize-series` streams the
ordered suffix into the transactional TinyFS sink, atomically publishing exact
output metadata and recipe/source/frontier progress; it does not collect or
concatenate the result, and a no-row run publishes progress without an empty
series version. Visibility of intentionally global/non-incremental paths
is explicit: arbitrary SQL is conservatively declared global, cacheless
temporal reduction and first-run materialization are declared
non-incremental, and each decision emits an info log plus shared provider
counters. Phase 9 is complete.

### Phase 10: consumers and Noyo qualification

1. Integrate reports and monitoring bounded reads.
2. Integrate deterministic site export.
3. Remove avoidable partition-enumeration scans.
4. Run the Noyo benchmark protocol in Section 14.
5. Qualify water and septic graphs for the same invariants.

**Gate:** production graphs satisfy the asymptotic requirements without
materialized intermediate series added to conceal plan defects.

Reports and monitoring now construct event-time-bounded providers and retain
exact row predicates. Status-grid journal reads use event-time bounds for
initial seeding and version watermarks thereafter. Deterministic site export
verifies and reuses an unchanged manifest prefix, performs zero source scans
for a no-change build, and streams a dirty tail or full rewrite from one ordered
source execution with one Parquet writer open at a time. It no longer executes
the source solely to enumerate partitions.

The production Noyo join/pivot graph now declares recursive immutable lineage
and propagates event-time bounds through every derived layer. Transform factory
configuration participates in recipe identity, so changing a transform cannot
reuse stale aggregate state. Query lineage is shared transaction-coherently
across provider contexts, while discovered dynamic schemas are persisted only
for an exact physical-lineage match. A TLogFS-backed join → pivot → reduce
regression proves cold state creation, byte-identical no-change reuse in a fresh
transaction, and manifest advancement after a physical-leaf append without a
global or non-incremental planning decision. Shared planning counters also prove
the warm run executes no dynamic source and the append executes it exactly once
with an event-time lower bound that excludes the oldest retained history.
Ordinary disorder inside the hot window and retroactive data in a later sealed
segment each execute one bounded repair, advance only affected aggregate state,
and avoid fallback. Adding a join output under the pivot wildcard expands the
output schema and builds a distinct aggregate namespace without reusing stale
state or taking a non-incremental path.

A retained warm no-change run over the 20 parameter/resolution outputs completed
site generation in 2.813 seconds (3.47 seconds total wall time, 130.6 MB process
max RSS, 12.01 MB CLI-reported peak). It recorded 20 schema-lineage cache hits,
20 deterministic-export reuses, zero schema reconstructions, zero full-rewrite
export queries, and zero `temporal-reduce-cache-unavailable` decisions. This
qualifies warm dynamic-graph cache selection and improves substantially on the
12.025-second pre-lineage site-generation baseline. A subsequent instrumented
run reported 2.930 seconds for site generation, zero global/non-incremental
plans, zero dynamic/export source executions, 128 reused export partitions, and
zero written partitions in one summary record. Append, disorder,
retroactive, and wildcard-membership replays against the full production Noyo
dataset; history-independent export verification; remaining detailed counters;
and data-bearing water/septic qualification remain before the Phase 10 gate is
complete.

The complete water and septic production configurations also apply successfully
to isolated fresh ponds. Empty temporal inputs with explicit aggregation columns
now expose their configured output schema, allowing downstream water SQL and
monitoring to report unknown rather than fail planning on a timestamp-only
placeholder. Septic required no compatibility fix. These empty-input runs
validate every configured factory and the fresh-pond behavior, but they are not
substitutes for data-bearing performance qualification.

Data-bearing isolated runs now cover every configured water and septic output
without deployment state or credentials. The repository's sanitized septic
fixture produced all 24 direct temporal-reduce outputs in 3.43 seconds cold and
2.71 seconds warm (109.6 MiB and 86.2 MiB maximum RSS); neither pass recorded a
global, non-incremental, or cache-unavailable decision. A synthetic four-hour
water fixture produced all 25 direct reduction outputs in 3.10 seconds cold and
2.89 seconds warm (90.9 MiB and 87.3 MiB maximum RSS), and the complete monitor
run succeeded. This run exposed and fixed a planning-boundary defect: DataFusion
expands a registered typed `ViewTable` to anonymous internal scans, so user-SQL
source validation now checks the parsed SQL relations before provider expansion
while continuing to reject undeclared sources.

Water's eight downstream pump-state, drawdown, Horner, calibration, usage, leak,
and annotation outputs also execute successfully. They remain explicitly global
and non-incremental: both cold and warm traversals recorded eight
`arbitrary-user-sql` decisions and took 1.77 and 1.74 seconds (99.6 MiB and
99.3 MiB maximum RSS). This is a measured Phase 10 gate blocker, not a silent
fallback. Those windowed and aggregate SQL graphs require an explicit
incremental design or an accepted global-work policy before water satisfies the
same asymptotic invariant as the direct reductions.

The first typed migration removes `/usage/well-usage-rate` from that global
set. `sql-derived-series` now accepts an explicit single-source
`timestamp-local` contract for immutable projection/filter SQL, validates the
user AST before DataFusion expands nested typed views, propagates read bounds,
and exposes recursive physical lineage. Unannotated SQL remains global by
default, and aggregate, join, window, sort, limit, subquery, and non-immutable
function plans are rejected under the local contract. A complete eight-output water
traversal now reports seven global/non-incremental plans and one
timestamp-local incremental plan. The remaining seven stateful analytics still
require typed window, fixed-window reduction, or bounded-repair recipes.

The reusable fixed-window piece is now available as
`temporal-reduce-series`, a directly addressable single-resolution file factory
that uses the same lineage-keyed partial manifests, bounded repair, and
associative folding as `temporal-reduce`. It requires one exact logical source
URL and can map default aggregate names such as `usage_gpm.sum` to stable public
columns such as `gallons`. A persistent TLogFS regression over a
timestamp-local SQL source proves the direct file's public schema, cold state
creation, byte-identical no-change reuse in a fresh transaction, and one
bounded source execution after append.

An attempted `/usage/well-usage-daily` migration exposed the next dependency
rather than satisfying the gate. Although `/usage/well-usage-rate` is itself a
timestamp-local projection, its source is the still-global
`/pump-state/well-pump-state` window query. The complete recursive source
therefore cannot provide bounded lineage, and the direct reducer correctly
emitted `temporal-reduce-cache-unavailable` and used its visible single-pass
path. The isolated four-hour fixture still produced the expected public daily
schema (`timestamp`, `gallons`, `pump_minutes`) and values, but the production
configuration was not migrated because that would only relabel a retained
history scan. A typed pump-state recipe with persisted open-episode and repair
boundary state is now a prerequisite for incrementally reducing daily usage.

That prerequisite now has an executable foundation contract. The typed
pump-state recipe computes the 60-minute trailing ceiling with a bounded
monotonic deque, preserves minute-gap island semantics, splits each disturbed
island at its earliest minimum, and marks the final island provisional.
Persisted disturbed-island spans anchor both append replacement of an open
episode and retroactive suffix repair; source reads include only the required
ceiling context before that anchor. Tests prove that a later trough revises the
whole provisional episode, a changed static boundary can rejoin the preceding
episode, malformed ordering fails, and closed-episode append work is independent
of retained history. Production provider/cache integration and comparison
against the existing SQL output remain before `/pump-state/well-pump-state` can
adopt the recipe.

### Phase 11: native-v2 backup and restore verification

1. Map query-visible immutable logical leaves to current
   `watertown.series.v3` identities.
2. Preserve per-leaf count, event-time bounds, schema fingerprint, and logical
   attributes in `watertown.series-pack.v4` descriptors.
3. Prove an append supplies a suffix pack without rereading retained history.
4. Prove repacking changes physical layout but not logical identity.
5. Prove query compaction cannot alter backup semantics.
6. Re-run publication no-op, append, retry, pull, and capsule verification.

**Gate:** query and replication planes share immutable identities and metadata
without backup executing a query or query planning from backup advertisements.

## 13. Production integration boundaries

### 13.1 TinyFS

TinyFS will supply:

- immutable version membership;
- transaction-coherent visibility;
- object length and range reads;
- logical leaf metadata;
- atomic streamed append; and
- failure atomicity.

It should not implement factory-specific query planning.

### 13.2 TLogFS and Delta

Delta remains a transactional visibility and concurrency mechanism. It should
not require eager full-table decoding to expose a queryable Parquet snapshot.

TLogFS must prove:

- exact snapshot capture;
- same-transaction visibility;
- stale-provider failure;
- bounded latest-state lookup;
- selected-version and row-group pruning; and
- query cost independent of unrelated Delta history.

### 13.3 Provider

The provider package will translate Watertown URLs and factory configuration
into:

- exact source snapshots;
- typed logical recipes;
- locality declarations;
- overlap policies;
- bounds;
- cache/state manifests; and
- instrumented execution.

It should not create hidden materialization boundaries or carry alternative
implementations of foundation operators.

### 13.4 Factories

Factories become declarative recipes and persistent-state coordinators:

- combine declares overlap behavior;
- join and pivot declare keys and output shape;
- reduce declares windows and reusable state;
- materialize declares sink progress and repair policy;
- SQL-derived factories declare no locality unless validated; and
- synthetic sources implement the same projection, filter, and metric
  contracts as physical sources.

### 13.5 Replication and backup

Backup remains a separate execution plane:

```text
shared immutable snapshot and logical-series metadata
        /                                      \
DataFusion query plane                  replication plane
chunk pruning and typed plans           Merkle diff and suffix packs
```

Backup must not discover changes by:

- executing a `TableProvider`;
- scanning logical series rows;
- listing all remote objects or packs;
- folding all retained leaves; or
- materializing a factory output.

Query planning must not:

- depend on remote publication advertisements;
- treat a backup pack as semantic query identity;
- change results when a physically equivalent pack is selected; or
- weaken snapshot coherence to reuse remote objects.

The current native-v2 invariants remain requirements:

- BLAKE3 logical series identity over canonical logical content;
- packing-independent ordered leaves;
- bounded Merkle append frontier;
- suffix packs for ordinary append;
- immutable objects before packs before visible publication state;
- fixed-size active publication state;
- acknowledged no-change return before remote open; and
- `pondcapsule.4` as the explicit verified reset boundary.

## 14. Noyo benchmark protocol

The Noyo graph remains the principal production performance qualification:

```text
git HydroVu archives ----\
live HydroVu series ------- typed same-scope combine and join
legacy Excel/HTML --------/
                                   |
                                   +--> temporal reduce by site
                                   |
                                   +--> typed pivot by parameter
                                              |
                                              +--> temporal reduce by parameter
```

Run at least:

1. **Cold cache**
   - no format or aggregate state;
   - establishes unavoidable historical work.
2. **Warm no-change**
   - identical snapshot and recipes;
   - expected source row scans: zero.
3. **One normal append**
   - one new live HydroVu chunk;
   - work proportional to that chunk and the unsealed region.
4. **One ordinary out-of-order append**
   - data inside the unsealed region;
   - only open windows and affected outputs change.
5. **One retroactive append**
   - data behind the settled frontier;
   - only declared repair intervals rebuild, or the run fails explicitly.
6. **Wildcard membership change**
   - one source added or removed;
   - only affected recipes and intervals change.

Record:

- wall-clock time by stage;
- peak RSS and DataFusion memory reservation;
- chunks and objects considered/opened;
- range requests and bytes;
- row groups and columns decoded;
- rows entering combine, join, pivot, and aggregate;
- reconciliation conflicts or duplicates;
- partial-state hit/miss/rebuild reason;
- output partitions reused and rewritten;
- factory source execution counts; and
- backup operations and bytes caused by the resulting commit.

Required asymptotic outcomes:

- warm no-change cost is independent of history;
- one-append cost is independent of history;
- ordinary disorder costs no more than the bounded unsealed region;
- retroactive repair is bounded by declared affected intervals;
- each source is scanned once per mathematically required occurrence;
- another resolution does not rescan raw history;
- another pivot parameter reads only required columns;
- site export does not reevaluate an expensive source merely to enumerate
  partitions; and
- backup publishes only objects and suffix packs introduced by the commit.

## 15. InfluxDB/IOx lessons

InfluxDB/IOx is useful prior art, not an implementation template.

Relevant lessons:

- DataFusion can remain the ordinary relational engine while a storage-facing
  chunk abstraction exposes statistics, ordering, and duplicate guarantees.
- Mutable and persisted membership must be captured consistently so rows are
  not omitted or exposed twice during a tier transition.
- Pruning occurs at catalog/chunk, file, row-group, and row levels; each level
  requires separate metrics.
- Domain-specific deduplication belongs in an explicit semantic operator, and
  predicates that could change winner selection must remain above it.
- Late data can be represented by immutable overlapping chunks, with
  compaction as a later physical operation.
- Custom optimizer rules are justified for demonstrated domain requirements,
  not merely because the workload is timeseries.
- Query logs should expose planned files, rows, bytes, duplicate counts, and
  peak memory.

Watertown differs in important ways:

- not every dataset has InfluxDB last-write-wins semantics;
- logical series leaves and native-v2 backup identities already have explicit
  packing-independent meaning;
- Watertown needs bounded settled/unsealed behavior for small industrial
  deployments; and
- backup efficiency and query efficiency share immutable identities but remain
  separate execution planes.

The foundation should therefore adopt the explicit chunk/provider and metrics
lessons without imposing IOx's universal primary-key reconciliation model.

## 16. Disposition of previous findings

The earlier audit findings remain mandatory regressions:

| Previous finding | Foundation disposition |
|---|---|
| Null-padding filter support order | mixed-filter transform regression |
| Type-changing rename pushdown | cast predicate and residual-filter regression |
| Custom transform execution nodes | logical projection requirement |
| Git Parquet eager load | real range-readable Parquet source gate |
| Same-scope full-row distinct | explicit overlap-policy combine |
| Version pruning without row bounds | metadata pruning plus exact row predicate |
| Unbounded materialize | abstract streaming sink and later TinyFS adapter |
| SQL-derived locality ambiguity | global default and validated explicit locality |
| Wildcard membership identity | separate recipe identity and change set |
| Synthetic ignores filters/projection | common provider contract |
| Export repeated scans | consumer execution-count gate |
| Obsolete join SQL | typed plan only; remove generated built-in SQL |

The fixes in `a0b117b3` and `cbf62ddb` demonstrate useful directions:
logical projections and exact event-time filters. They should be compared with
the foundation results during integration, not copied automatically.

The dynamic rollup work in `ccba84e0` also provides valuable regressions for
recursive bounds, accumulated timestamp joins, fine-to-coarse partials, late
repair, and manifest-authoritative caches. Those behaviors must be reproduced
through the new contracts before production adoption.

## 17. Definition of done

The program is complete only when:

1. the fresh crate proves every query shape in Section 10;
2. every gate includes result, plan, physical-work, memory, and failure
   evidence where applicable;
3. typed built-in plans replace generated SQL;
4. overlap behavior is declared per dataset or composition;
5. ordinary out-of-order input is efficient inside an explicit unsealed
   region;
6. retroactive input repairs or fails visibly;
7. bounded paths perform metadata pruning and retain exact row predicates;
8. projections reach physical Parquet leaves;
9. materialization is streaming and failure-atomic;
10. no-change and one-append work are independent of retained history;
11. memory and TLogFS satisfy the same contracts;
12. production factories are adapters to the proven foundation;
13. Noyo satisfies cold, warm, append, disorder, repair, and membership-change
    asymptotic gates;
14. query and native-v2 backup share immutable identities without coupling
    their execution;
15. backup no-op, suffix-only append, pull, retry, and capsule verification
    remain bounded and correct;
16. `cargo clippy --workspace --all-features -- -D warnings` passes; and
17. operator and design documentation describe implemented behavior rather
    than aspirational behavior.

## 18. Non-goals

- Do not add materialized intermediate Noyo series to hide inefficient plans.
- Do not reproduce TinyFS, TLogFS, Delta, or native-v2 backup inside the fresh
  crate.
- Do not copy current factory implementations before their abstractions are
  proven.
- Do not infer locality from arbitrary SQL text.
- Do not impose universal last-write-wins or universal deduplication.
- Do not assume timestamp uniqueness.
- Do not make physical Parquet packing part of logical identity.
- Do not use cache or object-store directory listing as authoritative
  membership.
- Do not optimize by dropping late, conflicting, or inconvenient data.
- Do not treat wall-clock benchmarks as substitutes for physical-work
  assertions.
- Do not add custom DataFusion operators without a demonstrated requirement.
- Do not weaken transaction coherence, publication atomicity, or visible error
  handling for performance.

The intended end state is one coherent system:

- immutable storage snapshots expose range-readable chunks;
- DataFusion performs ordinary optimization over typed logical plans;
- explicit locality and overlap contracts govern incremental behavior;
- persistent state reuses work across executions without hiding inefficient
  plans;
- TinyFS and TLogFS adapt their transactional storage to the same query
  contracts; and
- native-v2 backup transfers only new immutable content without executing the
  query plane.
