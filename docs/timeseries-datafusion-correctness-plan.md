# Time-Series DataFusion Correctness and Performance Plan

> **Status:** implementation plan following the September 2026 audit; Steps 1
> through 3 are complete.
>
> **Baseline commit:** `ccba84e0` (`provider: optimize dynamic timeseries rollups`).
>
> **Scope:** TinyFS/TLogFS table providers, SQL-derived series, time-series
> join and pivot, temporal reduction, table transforms, materialization,
> synthetic sources, and site/export consumers.

## 1. Purpose

Watertown's time-series layer should behave like a composable DataFusion system,
not like a sequence of hidden materialization boundaries. A consumer should be
able to request a time range and a subset of columns, and those requirements
should reach the physical Parquet leaves whenever the intervening operations
make that safe.

The target architecture has five rules:

1. Physical and externally backed data are exposed as lazy, range-readable
   `TableProvider`s.
2. Schema transforms are represented as DataFusion expressions whenever
   possible, so DataFusion owns column mapping, type coercion, ordering, and
   predicate safety.
3. Timestamp-local derived factories propagate conservative read bounds to
   every input and retain an exact row predicate in the logical plan.
4. Persistent caches avoid repeated work across executions, but never
   compensate for an inefficient plan within one execution.
5. Every optimization preserves transactional visibility, fails loudly on
   stale or incomplete data, and behaves identically across memory and TLogFS
   persistence.

This document records what is already correct, the remaining defects, and an
ordered implementation and validation plan.

## 2. Scope and inventory

### 2.1 Queryable files

| Component | Role | Current execution model |
|---|---|---|
| Physical table/series | Durable Parquet leaves | Explicit-version `ListingTable` over the TinyFS object store |
| `sql-derived-table` | Arbitrary SQL over table inputs | Cached `ViewTable` logical plan |
| `sql-derived-series` | Arbitrary SQL over series inputs | Cached `ViewTable` logical plan |
| `timeseries-join` | Timestamp-aligned, scoped union/join | Generated SQL over lazy input providers |
| `timeseries-pivot` | Select the same measurements across sites | Generated full-outer-join SQL over lazy inputs |
| `temporal-reduce` | Multi-resolution aggregation | Incremental sealed-run/hot-window cache when eligible; single-pass SQL fallback |
| `synthetic-timeseries` | Generated test/source data | `StreamingTable` over a custom `PartitionStream` |

### 2.2 Transforms

| Component | Role | Current implementation |
|---|---|---|
| `column-rename` | Rename, normalize, and optionally cast columns | Logical `ViewTable` projection |
| Scope prefix | Prefix non-time columns per source | Logical alias projection |
| Null padding | Add missing nullable columns before union/pivot | Logical typed-null projection |
| `CoherentTableProvider` | Enforce transaction generation validity | Transparent provider and execution wrapper |

### 2.3 Consumers

| Component | Read behavior |
|---|---|
| `materialize-series` | Reads a derived source after a target watermark and appends one physical version |
| Site export | Writes deterministic, time-partitioned Parquet |
| Site reports | Read bounded windows and project timestamp plus one value column |
| Monitoring/status | Reads bounded recent windows, with durable summaries where configured |

### 2.4 Noyo production graph

The Noyo graph is:

```text
git HydroVu archives ----\
live HydroVu series ------- timeseries-join (/combined/<site>)
legacy Excel/HTML --------/
                                   |
                                   +--> temporal-reduce (/reduced/single_site)
                                   |
                                   +--> timeseries-pivot (/singled/<parameter>)
                                              |
                                              +--> temporal-reduce
                                                   (/reduced/single_param)
```

As observed in the paired checkout on 2026-09-26, the source repository
contains seven Git-backed archive Parquets totaling about 7.46 MiB compressed
and eight legacy HydroVu HTML files totaling about 11.94 MiB. Those sizes are
not intrinsically large enough to justify a multi-hour build. The historical
cost came from repeatedly evaluating the dynamic graph across parameters and
resolutions, combined with source and adapter boundaries that prevented normal
pushdown.

## 3. What commit `ccba84e0` corrected

The baseline commit made the following structural improvements:

- `timeseries-join` and `timeseries-pivot` expose recursive incremental lineage.
- Semantic graph identity is separated from physical leaf versions for ordinary
  append-only series updates.
- `SeriesReadBounds` reaches dynamic join/pivot inputs.
- Every bounded dynamic input also receives a real event-time predicate before
  union and join construction.
- Source providers are combined with lazy `ViewTable` unions rather than
  collected into intermediate `MemTable`s.
- `CoherentExec` participates in DataFusion's physical filter-pushdown traversal.
- Pivot uses one coalesced full-outer-join chain instead of a distinct timestamp
  spine followed by a second scan of every source.
- Multiway joins compare each later source with the accumulated coalesced
  timestamp, preserving timestamps absent from the first source.
- Temporal-reduce can cache eligible dynamic graphs, repair late data, and fold
  coarser resolutions from finer cached partials.
- Reduced cache reads use explicit manifest members and declared timestamp
  ordering, allowing `SortPreservingMergeExec` instead of a full-history sort.

The corresponding plan tests establish the important local invariant:

```text
consumer time predicate
  -> dynamic input predicate
    -> DataSourceExec predicate / Parquet pruning predicate

consumer projection
  -> join or pivot projection
    -> narrow DataSourceExec projection
```

These changes are the correct foundation. The remaining work should preserve
this structure rather than adding a second materialized Noyo pipeline.

## 4. Required invariants

The implementation work below should be judged against these invariants.

### 4.1 Correctness

1. A pushed predicate has exactly the same meaning before and after a transform.
2. `supports_filters_pushdown` returns one result per input expression in the
   same order.
3. A provider reports `Exact` only when its scan enforces the complete
   predicate.
4. Late or backfilled source rows are either incorporated or rejected by an
   explicit monotonicity contract; they are never silently ignored.
5. A cache manifest is authoritative. Missing named members are errors and
   unreferenced files are ignored.
6. A provider created for one transaction generation cannot read after a
   mutation or transaction close.
7. Adding or replacing physical data invalidates data freshness without
   unnecessarily changing the semantic identity of an otherwise unchanged
   query.

### 4.2 Per-execution efficiency

1. Time bounds appear as row predicates in the logical plan, not only as
   version-selection hints.
2. Projections reach leaf scans unless an operator semantically requires the
   omitted columns.
3. A source appears once in the physical plan unless repeated evaluation is
   mathematically necessary.
4. Streaming operators do not collect or concatenate an unbounded result.
5. Sorts exist only where output ordering is part of the contract or required
   by a downstream operator.
6. Custom execution wrappers preserve valid ordering, equivalence, and
   partitioning metadata, or conservatively discard it without advertising
   incorrect properties.

### 4.3 Cross-execution efficiency

1. Immutable source decoding is cached by content identity.
2. Unchanged aggregate buckets are not recomputed.
3. Coarser reductions fold finer partials instead of rescanning raw history.
4. A no-change run performs metadata validation but no source-row scan.
5. One append costs work proportional to the new version plus the bounded hot
   window, independent of total retained history.

## 5. Findings

### 5.1 Critical: null-padding returns filter support in the wrong order

**Status:** corrected by Steps 1 and 2.

Location:

- `crates/provider/src/transform/null_padding.rs:107-154`

The provider separates filters into two collections:

- predicates referencing padded columns immediately append `Unsupported` to
  `results`;
- predicates referencing only inner columns are sent to the inner provider;
- the returned inner statuses are appended afterward.

That does not preserve the positions of the input filters. For input filters
`[inner_column_filter, padded_column_filter]`, a provider returning `Exact` for
the first filter produces:

```text
actual:   [Unsupported, Exact]
required: [Exact, Unsupported]
```

This was reproduced with a temporary audit test. It is a correctness defect,
not merely a missed optimization. DataFusion can believe the padded-column
predicate was enforced exactly, remove the outer filter, and then receive
unfiltered rows because `NullPaddingTableProvider::scan` deliberately excludes
padded-column predicates from the inner scan.

#### Implementation

Immediate surgical correction:

1. Allocate a result vector with one slot per input filter.
2. Record `(original_index, filter)` for each inner predicate.
3. Fill padded-column positions with `Unsupported`.
4. Ask the inner provider about only the inner predicates.
5. Copy each returned status back to its recorded original index.
6. Validate that the inner provider returned exactly one status per delegated
   expression; otherwise return an internal error.

Preferred architectural correction:

1. Replace `NullPaddingTableProvider` with a logical projection:

   ```sql
   SELECT
     existing_columns,
     CAST(NULL AS expected_type) AS missing_column
   FROM source
   ```

2. Store that plan in a `ViewTable`.
3. Let DataFusion perform projection pruning and predicate simplification.
4. Remove `NullPaddingExec` once all callers use the logical projection.

#### Required tests

- Mixed filter order: inner then padded.
- Mixed filter order: padded then inner.
- Several interleaved filters with distinct `Exact`, `Inexact`, and
  `Unsupported` inner responses.
- Query result where a padded-column predicate would remove all rows.
- Projection containing only padded columns.
- Projection that interleaves padded and inner columns.
- Physical plan proving an inner-column predicate reaches the leaf scan.

### 5.2 Critical: type-changing column rename has an unsound pushdown contract

**Status:** corrected by Steps 1 and 2.

Locations:

- `crates/provider/src/transform/column_rename.rs:112-160`
- `crates/provider/src/transform/column_rename.rs:181-229`
- `crates/provider/src/transform/column_rename.rs:233-370`
- `caspar.water/config/noyo.yaml:124-146`

`ColumnRenameTableProvider` rewrites a predicate by changing column names only.
The actual type cast occurs later in `ColumnRenameExec`.

For Noyo, the transform declares:

```yaml
from: "Date Time"
to: "timestamp"
cast: timestamp
```

A predicate over the output timestamp therefore has this semantic form:

```text
CAST("Date Time" AS TIMESTAMP) >= timestamp_literal
```

The delegated expression currently has this different form:

```text
"Date Time" >= timestamp_literal
```

An audit probe established both consequences:

- With a normal Parquet `ListingTable`, DataFusion retains an outer
  `FilterExec`, so the answer is correct, but the physical plan contains no
  Parquet predicate. Every selected legacy row is decoded and cast before the
  time filter is evaluated.
- With an inner provider claiming `Exact`, DataFusion removes the outer filter.
  The probe returned two rows instead of one, demonstrating that the generic
  provider contract can produce wrong results.

#### Implementation

The preferred correction is to stop implementing rename/cast as an opaque
physical wrapper.

1. Build a logical projection over the inner provider.
2. For unchanged columns, emit the source column.
3. For renamed columns, emit `source_column.alias(new_name)`.
4. For cast columns, emit
   `cast(source_column, target_type).alias(new_name)`.
5. Wrap the resulting logical plan in a `ViewTable`.
6. Let DataFusion decide which predicates can cross the projection and cast.
7. Remove `ColumnRenameExec` and its manually synthesized plan properties.

If this migration must be staged, the safe intermediate behavior is:

- type-preserving renames may delegate rewritten filters;
- any filter referencing a type-changing column returns `Unsupported`;
- `scan` must not send such a predicate to the inner provider;
- the outer filter remains above the cast.

Do not mark a casted predicate `Exact` merely because the inner provider accepts
the renamed expression.

#### Required tests

- Exact inner provider plus a casted filter must retain correct filtering.
- Inexact Parquet provider must keep the outer post-cast filter.
- Type-preserving timestamp rename should still reach the Parquet scan.
- Mixed predicates over casted and untouched columns.
- Invalid source values must retain the current visible cast error.
- Projection must decode only requested columns plus columns needed by filters.
- Plan properties must preserve timestamp ordering only when the projection or
  cast makes that statement valid.

### 5.3 High: custom transform execution nodes obscure optimizer properties

**Status:** corrected by Step 2.

Locations:

- `crates/provider/src/transform/column_rename.rs:248-268`
- `crates/provider/src/transform/null_padding.rs:218-337`
- `crates/tinyfs/src/coherence.rs:314-348`

`CoherentExec` is correctly transparent: it returns its child's properties and
explicitly forwards physical filters.

The transform nodes do not:

- `ColumnRenameExec` replaces all equivalence properties with an empty set and
  changes partitioning to `UnknownPartitioning`. A type-preserving scope prefix
  therefore loses valid timestamp ordering and partitioning information.
- `NullPaddingExec` creates empty equivalence properties but copies the inner
  partitioning expression unchanged. If a projection inserts a padded column
  before an inner column, the copied physical column index can describe the
  wrong output column.
- Neither node implements physical filter-pushdown traversal, so filters
  introduced or refined after physical planning cannot cross the wrapper.

The projection-based replacement in sections 5.1 and 5.2 solves these problems
using DataFusion's native expression and property machinery. Hand-maintaining
equivalence and partitioning rewrites should be the fallback, not the target.

### 5.4 High: Git-backed Parquet casts eagerly load and decode complete files

Locations:

- `crates/gitpond/src/tree.rs:312-358`
- `crates/provider/src/provider_api.rs:865-910`
- `crates/provider/src/factory/sql_derived.rs:963-979`
- `crates/provider/src/factory/sql_derived.rs:1202-1308`
- `crates/hydrovu/src/lib.rs:558-564`

The current path is:

```text
GitBlobFile::metadata
  -> read complete Git blob to determine size

GitBlobFile::async_reader
  -> read complete Git blob into Vec

read_pond_node_as_parquet
  -> read_to_end into another Vec
  -> ParquetRecordBatchReaderBuilder
  -> decode every batch
  -> MemTable

DataFusion query
  -> apply projection and predicate to already-decoded arrays
```

Consequences:

- no Parquet footer-only schema read;
- no row-group pruning;
- no page/index pruning;
- no late materialization;
- no source projection;
- no bounded I/O;
- multiple complete compressed and decoded representations can coexist;
- the resulting `MemTable` is retained by cached derived plans for the
  transaction lifetime.

#### Target design

Expose a data-archetype Parquet cast as a normal Parquet scan.

1. Give the Git blob a stable content identity without reading it. The Git blob
   OID is already a content identity and contributes to the dynamic node ID.
2. Expose length and range reads for the blob.
3. Register a read-only object-store URL whose object maps to that immutable
   blob, or copy the unchanged compressed Parquet bytes once into a
   content-addressed local file cache.
4. Construct a `ListingTable` over the resulting object URL.
5. Keep the cast provider cache separate from consumer transforms, so multiple
   joins and pivots share one raw Parquet provider.
6. Include blob identity in the cache key.
7. Fail if the Git reference changes while a provider is being built; never
   combine metadata from one blob with bytes from another.

The direct object-store implementation is preferable because it avoids a copy.
A compressed-byte cache is still acceptable because it preserves Parquet as
Parquet; it is not a materialized derived series.

#### Required tests

- Schema inference reads only footer ranges.
- A timestamp filter reads only matching row groups.
- A narrow projection does not decode unrelated value columns.
- Two consumers of the same blob reuse one provider/source identity.
- A changed Git blob invalidates the provider.
- A stale provider fails rather than reading a mixture of Git revisions.
- Invalid Parquet fails with the source path and blob identity.

### 5.5 High: same-scope join uses full-row distinct union

Location:

- `crates/provider/src/factory/timeseries_join.rs:398-468`

Inputs with the same scope are combined using:

```sql
SELECT * FROM filtered0
UNION BY NAME
SELECT * FROM filtered1
```

`UNION` is distinct. DataFusion must compare complete rows before it can discard
duplicates. That has two consequences:

1. a global distinct/hash operation is added before the timestamp join;
2. an outer projection cannot safely remove unused columns below the distinct,
   because every column participates in row identity.

This is directly relevant to Noyo, where archive and live sources for one
instrument share a scope. A parameter pivot that needs one measurement may
still require every source column to establish full-row distinctness.

Full-row distinct also does not establish the semantic property the join
actually needs: at most one row per timestamp per scope. Two rows with the same
timestamp and different values survive and can multiply rows in a later
full-outer join.

#### Required design decision

Choose and document one same-scope contract:

1. **Non-overlapping segments:** source ranges must not overlap. Validate this
   from metadata and use `UNION ALL BY NAME`.
2. **Identical overlap allowed:** use `UNION ALL BY NAME`, then deduplicate by a
   declared key with an explicit equality/conflict check.
3. **Source precedence:** define archive/live priority and choose one row per
   timestamp deterministically.

For the current HydroVu layout, the preferred contract is non-overlapping
append/archive segments with a hard validation error on conflicting overlap.
This permits `UNION ALL BY NAME`, full projection pushdown, and no global
full-row distinct.

#### Required tests

- Non-overlapping archive and live inputs use `UnionExec`, not a distinct
  aggregate.
- Only columns required by the downstream pivot reach each Parquet scan.
- Identical boundary rows follow the documented policy.
- Conflicting same-timestamp rows fail or resolve by explicit precedence.
- Duplicate timestamps cannot create a many-to-many join explosion.

### 5.6 High: bounded physical/cache providers prune versions, not rows

**Status:** corrected by Step 3.

Locations:

- `crates/provider/src/table_provider_options.rs:13-27`
- `crates/provider/src/table_creation.rs:176-295`
- `crates/provider/src/factory/temporal_reduce.rs:1320-1368`
- `crates/provider/src/factory/temporal_reduce.rs:3047-3173`

`SeriesReadBounds` deliberately defines a conservative version-selection
optimization. A version is excluded only when metadata proves it cannot
overlap the requested interval. A retained version can still contain rows
before the bound.

The SQL-derived dynamic path adds the required exact row predicate in
`SqlDerivedFile::apply_event_time_bound`. The direct physical and format-cache
paths used by temporal-reduce do not add an equally explicit source predicate;
they rely on version granularity and any optimizer movement of later bucket
filters.

This remains correct because downstream bucket-range predicates reject
irrelevant output. It is not the ideal scan plan: one large version overlapping
the hot window can be decoded and aggregated from its beginning on every
incremental rebuild.

#### Implementation

1. Add a shared helper that wraps any source provider with an event-time
   predicate represented as a `ViewTable`.
2. Use it after version pruning in:
   - physical-series providers used by temporal-reduce;
   - format-cache `CachedSet` providers;
   - dynamic source unions;
   - materialize-series.
3. Keep the exact predicate even when version metadata prunes every earlier
   version. The two layers have different purposes.
4. Ensure timestamp unit conversion uses the existing ceil semantics from
   `SqlDerivedFile::event_time_lower_bound`.
5. Avoid duplicating this conversion logic across factories.

#### Required tests

- One retained Parquet version spans both sides of the bound.
- The physical plan contains a leaf `DataSourceExec` predicate.
- The physical plan retains any residual `FilterExec` required by the
  provider's exactness classification; it removes one only when the scan or
  physical pushdown enforces an equivalent predicate.
- Rows before the bound never enter the aggregate.
- Unknown version bounds retain the file but still apply the row predicate.

### 5.7 High: materialize-series is unbounded, non-streaming, and late-data blind

Locations:

- `crates/provider/src/factory/materialize_series.rs:122-175`
- `crates/provider/src/factory/materialize_series.rs:197-270`
- `crates/tinyfs/src/arrow/parquet.rs:469-504`

The current algorithm:

1. obtains the target watermark;
2. creates an unbounded source provider;
3. applies `time > watermark` above the entire derived source;
4. globally sorts the delta;
5. collects every output batch;
6. concatenates the batches into one `RecordBatch`;
7. serializes that complete batch into another in-memory Parquet buffer;
8. appends the buffer as one series version.

For a full-outer-join source, DataFusion is not required to infer that a
predicate on the final coalesced timestamp can be distributed to every input.
The first four steps can therefore evaluate all source history to produce a
small delta. The last four use memory proportional to the complete delta, with
multiple simultaneous copies.

The watermark algorithm also silently excludes newly arrived rows whose event
time is less than or equal to the existing maximum. That is acceptable only
under an explicit, enforced monotonic-source contract.

#### Implementation

1. Convert the watermark to a conservative inclusive `SeriesReadBounds`
   event-time lower bound.
2. Call `create_table_provider_bounded`.
3. Retain the exact outer `time > watermark` filter to exclude the already
   stored boundary row.
4. Add `write_series_from_stream` to TinyFS:
   - accept a `SendableRecordBatchStream`;
   - write through `AsyncArrowWriter`;
   - compute min/max event time and row count incrementally;
   - publish exactly one series version after successful close;
   - leave no visible version after a failed stream or writer.
5. Feed the sorted DataFusion execution stream directly into that writer.
6. Avoid `collect`, `concat_batches`, and a complete serialized buffer.
7. Add a source-progress record containing lineage/version identity.
8. Either:
   - reject a source version whose minimum time is at or below the committed
     watermark, under a declared monotonic contract; or
   - configure allowed lateness and rebuild/replace the affected target range
     using an explicit deduplication key.

Do not continue silently dropping late data.

#### Required tests

- Initial materialization uses bounded memory across many batches.
- A no-change run writes no target version.
- One append scans only the bounded source tail.
- The watermark row is not duplicated.
- A late row is handled by the configured policy.
- Failure after several streamed batches publishes no partial version.
- Memory and TLogFS produce identical target rows and temporal metadata.

### 5.8 Medium: arbitrary SQL-derived series have no timestamp-local contract

Locations:

- `crates/provider/src/factory/sql_derived.rs:1654-1796`
- `crates/provider/src/factory/lazy_sql_file.rs:117-218`

`sql-derived-series` correctly remains conservative. Arbitrary SQL can contain
global windows, cumulative calculations, non-local joins, and final ordering;
blindly pushing an output time bound into its inputs would change results.

The downside is that simple timestamp-local SQL has no way to advertise that
property. It cannot expose incremental lineage and its default
`QueryableFile::as_table_provider_bounded` behavior is effectively unbounded.

#### Implementation

Add an explicit, declarative mode rather than SQL-text heuristics:

```yaml
incremental:
  mode: timestamp-local
  output_time_column: timestamp
  input_time_columns:
    source: timestamp
```

For this mode:

1. include the declaration in semantic identity;
2. apply exact row bounds to declared inputs;
3. expose recursive lineage;
4. validate that every named input and output time column exists;
5. reject unsupported constructs unless correctness can be proven;
6. keep ordinary `sql-derived-series` unchanged and conservative.

Queries containing cumulative windows, rank over all history, or cross-time
joins must remain non-local unless they define the preceding context needed to
compute a bounded output.

### 5.9 Medium: wildcard membership is mixed into semantic identity

Locations:

- `crates/provider/src/factory/lineage.rs:121-205`
- `crates/provider/src/factory/timeseries_join.rs:285-349`
- `crates/provider/src/factory/timeseries_pivot.rs:273-316`

The lineage builder correctly keeps changing versions out of the recipe hash
for a stable physical node. However, it includes every resolved source path and
`FileID` in the canonical semantic bytes.

Adding a new file under an unchanged wildcard therefore changes semantic
identity and invalidates all aggregate levels. That is conservative and
correct, but it treats a data-set membership change as a query-definition
change.

#### Implementation

1. Hash configured edges: factory kind, pattern URL, ranges, scopes, requested
   columns, and transforms.
2. For nested dynamic sources, hash their semantic recipe identities.
3. Keep resolved durable leaf IDs and versions only in the freshness leaf set.
4. Adding a physical leaf under an unchanged wildcard should mark the
   appropriate event-time range dirty, not invalidate the cache namespace.
5. Adding a dynamic child with a genuinely different nested recipe must still
   invalidate semantic identity.
6. Preserve deterministic ordering and cycle detection.

#### Required tests

- Appending a version changes freshness but not semantic identity.
- Adding a physical file under a wildcard changes leaves but not semantics.
- Removing a physical file changes leaves and triggers the required repair.
- Changing a transform changes semantics.
- Changing a nested dynamic recipe changes semantics.
- Reordering directory enumeration changes neither semantics nor freshness.

### 5.10 Medium: synthetic-timeseries ignores filters and projected generation

Locations:

- `crates/provider/src/factory/synthetic_timeseries.rs:229-320`
- `crates/provider/src/factory/synthetic_timeseries.rs:460-500`

The source streams batches and therefore has bounded memory, which is good.
However, `StreamingTable` does not translate timestamp filters into generator
bounds. `SyntheticBatchStream` also computes every configured point column for
every generated row, even when the scan projects only one point.

#### Implementation

Replace `StreamingTable` with a small custom `TableProvider` that:

1. recognizes supported lower and upper predicates on the configured time
   column;
2. intersects them with the configured generation interval;
3. maps projection indices to the requested waveform definitions;
4. generates only required timestamps and values;
5. reports exact support for predicates fully enforced by generation;
6. retains unsupported predicates above the scan.

This is lower production priority but should embody the same provider contract
used everywhere else.

### 5.11 Medium: export performs avoidable repeated scans

Locations:

- `crates/provider/src/export.rs:420-538`
- `crates/provider/src/export.rs:611-714`
- `crates/provider/src/export.rs:723-930`

A full rewrite first runs `SELECT DISTINCT` to enumerate partitions and then
runs `COPY`. If the number of partitions exceeds the writer budget, it runs one
additional source query per partition chunk. Incremental reconcile scans the
source to enumerate every partition and then issues one filtered query per
changed partition.

For temporal-reduce output this is usually acceptable because the provider is
the compact cached rollup, not the raw dynamic graph. For arbitrary derived
series it can repeat expensive computation.

#### Implementation

1. For the common case below the writer limit, run one partitioned `COPY`
   without a preliminary distinct-partition scan.
2. Discover and normalize produced files afterward.
3. For incremental reconcile, derive reusable historical partitions from the
   seed manifest and enumerate only timestamps at or after `changed_since`.
4. Preserve deletion handling by comparing the changed-tail result with seeded
   partitions at or after that boundary.
5. For large partition counts, retain chunking but ensure every chunk predicate
   reaches physical leaves.
6. Verify whether global `ORDER BY timestamp` is required by the output
   contract. If ordering is per partition, use partition-aware ordering rather
   than a global pre-partition sort.

### 5.12 Low: obsolete join SQL remains in validation

Locations:

- `crates/provider/src/factory/timeseries_join.rs:138-230`
- `crates/provider/src/factory/timeseries_join.rs:590-603`

`generate_timeseries_join_sql` is no longer the execution path, but validation
still calls it. It contains the previous `USING` join and terminal `ORDER BY`
shape, while execution uses `generate_union_join_sql`.

This is not currently an execution cost, but duplicate SQL generation can let
validation and execution semantics drift. Validation should inspect the
configuration directly and, where planning validation is desired, plan the
same SQL generator used at execution.

## 6. Factory-by-factory disposition

### Physical table and series providers

**Assessment:** sound foundation.

- Explicit live-version URLs prevent stale or superseded files from reappearing
  through directory listing.
- Cache keys include version selection and bounds.
- `CoherentTableProvider` rejects stale providers after mutation.
- The TinyFS object store supports ranged reads and Parquet can perform
  projection and row-group pruning.
- Memory and TLogFS implement the same bounded queryable-file interface.

**Remaining work:** add the exact row predicate described in section 5.6 and
retain plan tests across both persistence backends.

### Format providers and format cache

**Assessment:** good once cached.

- Source parsing streams into immutable Parquet sidecars.
- Sidecars are content/version keyed.
- Reads use an explicit live file set.
- Missing live sidecars fail instead of silently omitting rows.

**Remaining work:** exact row bounds after conservative sidecar selection, and
plan/I/O tests proving row-group and projection pruning.

### SQL-derived table and series

**Assessment:** correct conservative abstraction, incomplete optimization
contract.

- `ViewTable` preserves composability.
- Source unions are lazy.
- Arbitrary SQL must not claim timestamp locality.

**Remaining work:** add the explicit timestamp-local mode, remove unnecessary
terminal ordering from configurations where ordering is only presentational,
and ensure transient table registration does not retain redundant raw
providers for the lifetime of a large build.

### Time-series join

**Assessment:** the full-outer-join correction is sound.

- Inputs are scanned once per execution path.
- Later joins use accumulated coalesced time.
- Bounds are injected into each input before the join.
- No terminal sort exists in the active generator.

**Remaining work:** define same-scope overlap semantics and remove full-row
distinct when possible; retire the obsolete validation generator; migrate scope
prefixing to a logical projection.

### Time-series pivot

**Assessment:** the new single-scan algorithm is sound.

- It no longer builds a separate distinct timestamp spine.
- It projects only configured measurements.
- Bounds recurse through the source joins.

**Remaining work:** replace null-padding and scope-prefix wrappers with logical
projections. The null-padding filter-order bug must be fixed before treating the
provider as generally safe.

### Temporal reduce

**Assessment:** strongest part of the system.

- Immutable leaves and semantic recipe identity are separated for stable
  source nodes.
- The finest level scans source data; coarser levels fold finer partials.
- Sealed segments and a bounded hot window prevent history-sized aggregation.
- Late data unseals and repairs affected ranges.
- Cache corruption is surfaced.
- Reads use explicit manifest members and declared ordering.
- Export hints avoid rewriting unchanged site partitions.

**Remaining work:** exact source row predicates, wildcard-membership identity,
and an explicit timestamp-local contract for eligible SQL-derived sources.

The single-pass fallback is correct but intentionally O(history). It should
remain visible in logs/metrics so a configuration change cannot silently move a
production graph off the incremental path.

### Materialize series

**Assessment:** useful boundary with an incomplete incremental contract.

It should be retained for cases where a durable typed series is a genuine
product, such as self-monitoring. It should not be used as the remedy for an
inefficient Noyo dynamic graph.

**Remaining work:** bounded source construction, streaming writes, and explicit
late-data behavior.

### Synthetic timeseries

**Assessment:** bounded-memory but optimizer-opaque.

**Remaining work:** filter-aware and projection-aware generation.

### Site reports and monitoring

**Assessment:** generally correct consumer behavior.

`sitegen::report::collect_samples` constructs a bounded provider and queries
only the timestamp and requested value columns. Monitoring uses bounded windows
for recent status. These consumers demonstrate the desired API shape.

### Site export

**Assessment:** correct and deterministic, but can repeat scans.

Temporal-reduce's export hints substantially reduce steady-state cost. Generic
derived exports still need the improvements in section 5.11.

## 7. Step-by-step implementation sequence

Each step should be a separately reviewable commit. Do not combine behavior
changes with unrelated cleanup.

Every step must also add or run the narrowest relevant efficiency regression.
Depending on the path, that means plan-shape assertions, hard upper bounds on
I/O or work counters, bounded-memory checks, or an instrumented Noyo benchmark.
Correct results alone do not satisfy a step's exit condition. Record a
before/after baseline when the step changes execution work; do not defer
performance validation until the end of the sequence.

### Step 1 (complete): lock down transform correctness

1. Add permanent regressions reproducing:
   - null-padding mixed filter ordering;
   - exact pushdown through a type-changing rename returning wrong rows;
   - current inexact Parquet behavior retaining a post-cast filter.
2. Apply the positional null-padding fix.
3. Make casted-column predicates unsupported as the immediate safety fix.
4. Assert that unaffected conjuncts are still delegated and unused Parquet
   columns remain projected out.
5. Run provider tests and strict workspace clippy.

**Exit condition:** no custom provider can report `Exact` for a predicate it
does not enforce, and the safety fix does not disable independent predicate or
projection pushdown.

### Step 2 (complete): replace custom transforms with logical projections

1. Introduce a helper that builds a projection `ViewTable` over a provider.
2. Migrate scope prefixing.
3. Migrate type-preserving column rename.
4. Migrate type-changing casts.
5. Migrate null padding.
6. Remove custom execution nodes after plan/result parity is established.

**Exit condition:** aliases, casts, and typed nulls appear as ordinary DataFusion
projection expressions; no transform manually synthesizes optimizer metadata.

### Step 3 (complete): centralize exact event-time bounds

1. Extract timestamp-unit conversion from `SqlDerivedFile`.
2. Add a shared `bounded_table_provider(provider, column, bounds)` helper.
3. Apply it after version pruning for physical and cached-format sources.
4. Use it in dynamic SQL-derived construction.
5. Use it in temporal-reduce source construction.
6. Add explain-plan assertions for each source kind.

**Exit condition:** every bounded source plan has both conservative file
selection and an exact row predicate.

### Step 4: make materialization genuinely incremental

1. Pass a conservative source bound derived from the target watermark.
2. Keep the strict outer predicate.
3. Add streaming series-version writing.
4. Remove result collection and concatenation.
5. Record source lineage/progress.
6. Implement and document the late-data policy.
7. Enroll memory and TLogFS in the same materialization contract.

**Exit condition:** one append has memory bounded by execution batch and Parquet
writer buffers, and a late row cannot disappear silently.

### Step 5: expose Git Parquet lazily

1. Expose Git blob identity and length without loading its contents.
2. Implement immutable range reads.
3. Build a Parquet `ListingTable` over the blob object.
4. Cache the raw provider by blob identity.
5. Remove `read_pond_node_as_parquet` from query paths.
6. Keep a focused helper only if a non-query use truly needs full decoding.

**Exit condition:** querying one projected column and a narrow time range does
not read or decode the complete Git archive.

### Step 6: establish same-scope union semantics

1. Document the required archive/live overlap rule.
2. Add overlap and duplicate-timestamp fixtures.
3. Implement validation or deterministic precedence.
4. Change the common non-overlap path to `UNION ALL BY NAME`.
5. Assert narrow leaf projections beneath the union.

**Exit condition:** same-scope composition is both semantically explicit and
free of unnecessary full-row distinct work.

### Step 7: stabilize wildcard lineage

1. Separate configured edge identity from resolved leaf membership.
2. Keep nested semantic identities in the recipe.
3. Move physical node IDs and versions into the freshness set.
4. Verify append, add, remove, transform-change, and cycle cases.

**Exit condition:** adding ordinary data under an unchanged wildcard repairs
only the affected time range.

### Step 8: add timestamp-local SQL-derived mode

1. Define the configuration schema.
2. Validate declared input/output time columns.
3. Propagate bounds.
4. expose recursive lineage.
5. Reject unsupported SQL shapes rather than guessing.
6. Convert only demonstrably local existing configurations.

**Exit condition:** simple projections and row-local calculations can
participate in incremental reduction without weakening arbitrary SQL
correctness.

### Step 9: improve synthetic and export providers

1. Make synthetic generation honor time filters and projections.
2. Remove unnecessary export partition-enumeration scans.
3. Bound incremental partition discovery to `changed_since`.
4. Verify partition predicates reach source scans.
5. Revisit global output sorting based on the actual exported-file ordering
   contract.

**Exit condition:** test sources follow the production provider contract and
exports do not reevaluate an expensive source merely to discover output paths.

### Step 10: remove obsolete paths and document the contract

1. Delete `generate_timeseries_join_sql` after validation uses the active
   generator or direct config checks.
2. Remove obsolete custom execution wrappers.
3. Update `cli-reference.md` for timestamp-local SQL and materializer late-data
   behavior.
4. Update `temporal-reduce-bounded-memory-design.md` with the shared exact-bound
   helper and wildcard-lineage rules.

## 8. Validation framework

Result tests alone are insufficient. Every important path needs four forms of
evidence.

### 8.1 Result correctness

Run the same fixtures against memory and TLogFS:

| Scenario | Memory | TLogFS |
|---|---:|---:|
| Unbounded physical series | required | required |
| Bounded physical series | required | required |
| Same-transaction append visibility | required | required |
| Stale provider after append | required | required |
| Join with missing timestamps | required | required |
| Three-way accumulated timestamp join | required | required |
| Pivot with missing columns | required | required |
| Mixed inner/padded predicates | required | required |
| Casted timestamp predicate | required | required |
| Late materializer input | required | required |
| Late temporal-reduce input | required | required |
| Wildcard member add/remove | required | required |

### 8.2 Plan shape

Normalize physical-plan text and assert structural properties:

- one `DataSourceExec` per physical source occurrence;
- expected leaf `projection=[...]`;
- expected Parquet `predicate=...`;
- no residual global `SortExec` when existing ordering suffices;
- `SortPreservingMergeExec` for cached reduced runs;
- no `DataSourceExec` for pruned versions;
- no `MemoryExec` beneath Git-backed Parquet scans;
- no distinct aggregate for validated non-overlapping same-scope unions;
- no `FilterExec` removed on the basis of a false `Exact` declaration.

Avoid assertions tied to unstable generated IDs or cosmetic explain formatting.

### 8.3 I/O and work counters

Instrument the TinyFS object store and format cache in tests:

- object metadata calls;
- byte ranges requested;
- total bytes returned;
- source versions opened;
- Parquet row groups decoded;
- format files parsed;
- synthetic rows and point values generated;
- aggregate cache levels rebuilt;
- export source executions.

The tests should assert upper bounds, not only log these values.

### 8.4 Memory behavior

Use bounded fixtures large enough to produce many record batches:

- materialization peak memory must not scale with delta row count;
- temporal-reduce peak memory must scale with hot-window bucket count, not
  retained history;
- Git Parquet scans must not retain a full decoded table;
- export memory must remain bounded by writer and execution batches.

### 8.5 Failure behavior

Inject failures at:

- source range read;
- format-cache write;
- transform evaluation;
- DataFusion stream after several batches;
- series writer close;
- manifest write and rename;
- transaction commit.

Every failure must be returned. No partial version, cache manifest, or export
may become authoritative.

## 9. Noyo benchmark protocol

Functional tests prove correctness; they do not prove that the production graph
is fast. Measure the deployed Noyo graph in four modes:

1. **Cold cache**
   - empty format and aggregate caches;
   - complete site generation;
   - establishes unavoidable historical work.
2. **Warm, no changes**
   - identical source identities and versions;
   - should perform metadata validation and export reuse only;
   - expected source row scans: zero.
3. **One normal append**
   - one new live HydroVu version;
   - work should be proportional to that version plus allowed-lateness windows;
   - unchanged sites/parameters/resolutions should reuse caches.
4. **One late append**
   - one row behind the sealed frontier but within the supported repair model;
   - only affected segments and export partitions should be rebuilt.

Record for each run:

- wall-clock time by factory and export stage;
- peak RSS;
- source files and versions opened;
- bytes read;
- Parquet row groups decoded;
- rows entering each join, pivot, and aggregate;
- cache hit/miss/rebuild reason;
- output partitions reused and rewritten.

The important acceptance criteria are asymptotic:

- warm no-change cost is independent of history;
- one-append cost is independent of history;
- peak memory is bounded by batches, join state for the active range, and the
  configured hot window;
- adding another output resolution does not rescan raw history;
- adding another pivot parameter reads only that parameter's columns.

Absolute timing targets should be set after the first instrumented run on
Watershop hardware.

## 10. Definition of done

The review is complete only when:

1. the two transform correctness defects have permanent regressions and fixes;
2. all bounded paths show both file pruning and row predicates;
3. materialization is streaming and has explicit late-data semantics;
4. Git-backed Parquet remains lazy through DataFusion;
5. same-scope union semantics are explicit and plan-tested;
6. wildcard data changes do not unnecessarily invalidate query semantics;
7. the cross-persistence matrix passes for memory and TLogFS;
8. `cargo clippy --workspace --all-features -- -D warnings` passes;
9. cold, warm, append, and late Noyo benchmarks satisfy the asymptotic
   invariants above; and
10. documentation describes the implemented behavior rather than a proposed
    behavior.

## 11. Non-goals

- Do not add materialized intermediate Noyo series merely to hide inefficient
  dynamic plans.
- Do not infer timestamp locality from arbitrary SQL text.
- Do not weaken stale-provider or transaction-generation checks for speed.
- Do not silently fall back from a failed incremental path to a full-history
  path.
- Do not use cache directory listing as authority for readable members.
- Do not optimize by dropping late or conflicting data.

The intended end state is simple: DataFusion performs ordinary local
optimization through transparent logical plans, TinyFS provides coherent and
range-readable leaves, and temporal-reduce supplies the separate persistent
reuse needed across builds.
