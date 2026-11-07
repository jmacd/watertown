---
title: Architecture for developers
doc_id: guide:architecture
---

# Architecture for developers

Start here before adding a factory, derived dataset, cache, or publication
workflow. Watertown is a transactional, query-native filesystem: it stores
source material, names computations as filesystem objects, and constructs
reports through shared query services. It is not just an ingestion archive
behind an application's separate storage system.

This guide describes current architectural responsibilities and boundaries.
Use [shared concepts](../reference/concepts.md) for URL and path semantics
and individual component references for configuration and behavior.
Design proposals and historical implementation notes outside the manual
provide background, not evidence that a capability has shipped.

## Authority and ownership

The pond's `data/` directory contains one Delta Lake table and its associated
content. Durable shared state belongs in the pond. Local operational state,
query caches, and published exports have different responsibilities:

| Kind of state | Responsibility |
|---|---|
| Ingested source material | Pond content with provenance; retain independently recoverable original sources. |
| Factory configurations | Pond nodes that name computations and their inputs; environment references resolve in the executing replica. |
| Durable derived results and publication metadata | Pond content when these facts must survive, replicate, or be consumed by another pond. |
| Parsed formats and partial aggregates | Reconstructable caches owned by the shared query services. |
| Transaction diagnostics and remote acknowledgements | Local control/operator state, not automatically replicated application data. |
| Static sites and exported Parquet | Published artifacts derived from pond content, not a backup or an independent source of truth. |

Do not introduce a host-side JSON file or database as the sole record of a
shared application fact merely because it is convenient for a renderer.
Conversely, an external collector's original journal is source material:
keeping original observations recoverable is not the same as maintaining a
second authoritative derived-report store.

Choose the owning pond explicitly. An importing pond consumes another pond's
content; it does not thereby own that content or acquire permission to mutate
it. A site-building pond may be only a consumer. Use the factory's effective
root when resolving imported or nested content, not an assumed global root.
See [patterns and roots](../reference/concepts.md#patterns-and-effective-roots)
and [operations](operations.md).

## Directories are a storage-locality decision

The Delta table is partitioned by `pond_id` and `part_id`. Each physical
directory is self-partitioned: its node identity determines its partition.
Ordinary files in that directory share the directory's partition; a physical
child directory owns another partition. Pond identity namespaces imported
content.

For example, files directly under `/observations/station-a/` share a
partition, while `/observations/station-b/` has a different one. Adding a
hundred filenames under one directory does not create a hundred Delta
partitions. A dynamic directory is a computed namespace and does not
automatically create physical storage partitions.

Node reads constrain pond, partition, and node identity, allowing Delta
partition pruning. Directory layout therefore controls locality of filesystem
records. It is not a promise that every query reads only one partition:
wildcards may select many directories, and a selected series may span many
versions.

Keep three layers distinct:

- **Namespace locality:** which directory partition holds a node's records.
- **Logical query membership:** which nodes and immutable versions contribute.
- **Physical packing:** how content is encoded in Parquet, external blobs,
  or maintenance packs.

Changing physical packing must not change logical series identity or row
semantics. Not every payload is inline in its directory's Delta Parquet:
large content can live in external objects referenced by the records.

## Choose a node before inventing a store

| Need | Existing mechanism | Boundary |
|---|---|---|
| A replaceable file or table | Physical versioned node | Read the selected version, not a concatenation of historical replacements. |
| An immutable sequence of observations | Physical file/table series | Appends accumulate; timestamps alone do not define row identity or deduplication. |
| A query over existing inputs | SQL-derived table or series | A dynamic node computes a view; it does not imply persisted result rows. |
| Time-bucket aggregation | Temporal reduction | Shared partial state and late-data invalidation; not a general arbitrary-dimension SQL cache. |
| Persisting a strictly newer suffix | Series materialization | Append-only, not replacement of corrected historical results. |
| A static site or external dataset | Sitegen and export | Publication is separate from durable pond state and replication. |

The references for [SQL-derived series](../reference/factories/sql-derived-series.md),
[temporal reduction](../reference/factories/temporal-reduce.md),
[direct temporal reduction](../reference/factories/temporal-reduce-series.md),
and [materialization](../reference/factories/materialize-series.md) describe
their public interfaces.

In particular, `materialize-series` reads beyond the target event-time
watermark. It does not repair late rows behind that watermark and must not
be used as a general checkpoint for changing aggregates. The query foundation
models replacement publication, but an abstract contract is not evidence that
every concrete sink implements it.

## Reuse the query services

### Parsing and exact membership

Format providers decode source files into Arrow batches. The shared format
cache stores parsed versions as Parquet so repeated reads need not parse
unchanged bytes. Pond-native Parquet already supplies columnar inputs.

Version-cache readers use explicitly selected live versions, not arbitrary
cache-directory listings. Reconciliation and query-bound pruning have
different jobs: reconciliation considers the complete live set; pruning
selects the subset needed for this query. A query's narrow window must not
be mistaken for deletion of the rest of the source.

The query foundation models snapshots with exact immutable chunk membership.
Logical series leaf identity is independent of Parquet compression and packing.
Arrival sequence, event time, row identity, and physical object identity are
not interchangeable.

### Incremental reduction

Temporal reduction owns the existing partial-aggregate cache. It tracks
source identity, recipe/configuration identity, and event-time ranges;
stores sealed segments plus an active window; and can derive coarser
resolutions from finer partials. Compatible unchanged inputs reuse existing
aggregates without executing the raw source again.

Changes invalidate affected output according to the available bounds.
Late arrivals can require reopening previously sealed ranges. Missing bounds
must be treated conservatively, not interpreted as absence of data. Recipe
changes or unsupported locality can require broader work; caching is not
proof that every execution is bounded independently of retained history.

These caches are reconstructable platform state under the pond's local cache
directory, not application-owned report stores. A missing required cache
member must not silently turn into an incomplete count. Do not copy the cache
manifest, invalidation, or compaction machinery into a feature-specific layer.

### Locality is an explicit contract

Arbitrary user SQL is conservatively global. The `timestamp-local`
SQL-derived declaration is a validated, restricted projection/filter contract,
not a way to label an arbitrary join or aggregate incremental.

Eligible derived nodes expose recursive source lineage so downstream
reductions can identify unchanged physical inputs without executing the entire
derived query. When a node cannot supply that contract, do not manufacture a
freshness key from a filename, wall clock, or maximum observed timestamp.

Read bounds are also specific contracts. `SeriesReadBounds` currently carries
an event-time lower bound and an exclusive version watermark; it is not an
arbitrary two-sided interval. Version pruning retains inputs with unknown
time bounds. Exact row filtering remains the query's responsibility.

## Transactions and execution context

`tinyfs` supplies filesystem and persistence interfaces; `tlogfs` implements
them over Delta; `provider` supplies formats, factories, and query services;
`query-foundation` defines storage-independent query contracts and typed plans;
`steward` coordinates transactions, content identity, and publication.

Pond-backed factories operate within the caller's transaction and supplied
`ProviderContext`. Reuse its persistence, DataFusion session, and effective
filesystem root. Opening another persistence instance or unrelated session
inside a factory can lose pending writes, coherence, and shared services.

Completed mutations advance the transaction generation. Provider-cache entries
are generation-aware, stale providers cannot simply be reused, and closed
transaction contexts are not reusable. Writers and query streams must finish
before commit. In-memory provider reuse is distinct from cross-process,
content-keyed cache reuse.

Related durable outputs and progress must become visible in the same pond
transaction. Streaming materialization should use the existing sink contracts,
not collect an unbounded result just to write it. Failed writes must fail the
transaction rather than publish progress describing absent output.

For content-changing commits, Steward records content identity and the
pond-resident commit log together with the data. Local diagnostics, exports,
and remote publication follow the authoritative commit. A failed remote push
does not undo a successful local commit; it must remain visible and retryable.
Likewise, a local failed-attempt record is not automatically report metadata
that another pond will receive.

## A source-to-publication example

Consider timestamped station measurements, not a new storage subsystem:

1. **Ingest observations.** An existing ingestion factory writes source
   versions and its ingestion progress transactionally. Choose directories
   for the station locality you need and record event-time bounds.
2. **Name the computation.** A SQL-derived node can project or filter the
   source. Declare timestamp locality only when the query satisfies it.
   The computation's configuration is durable; evaluating it need not create
   another physical series.
3. **Reduce through shared services.** Temporal reduction supplies hourly and
   daily views and manages reusable partials. An unchanged eligible source
   reuses the aggregates; changed input drives invalidation.
4. **Publish through the existing exporter.** Reduction providers expose output
   change hints. Export can verify and reuse unchanged partitions rather than
   rewrite them, and Sitegen can publish the resulting site.
5. **Distinguish publication from persistence.** If the application requires a
   durable approved snapshot or publication fact, give that fact pond ownership
   and transactional semantics. Neither a query cache nor an exported website
   automatically supplies that contract.

This is a responsibility map, not an executable fixture or a claim that every
combination has end-to-end coverage. The runnable
[timeseries](timeseries.md) and [timeseries-site](timeseries-site.md) guides
provide concrete examples without duplicating their configurations here.

## Before extending the platform

For each new responsibility, identify the existing service and its limits
first. State which facts are authoritative, their owning pond, the transaction
boundary, the exact source identity, and the behavior under late data or
failed publication. Explain locality and no-change work separately from
retention and physical compaction.

If an existing interface lacks a required semantic capability, document the
gap explicitly. Prefer a focused shared extension over another feature-owned
cache or persistence protocol. Tests should establish observable correctness
and work bounds, not merely preserve an incidental storage layout.

## Implementation map

These links point to the default branch; use the corresponding paths in your
checkout when working on another revision.

| Responsibility | Implementation entry points |
|---|---|
| Directory partitions and exact node reads | [tlogfs directory](https://github.com/jmacd/watertown/blob/main/crates/tlogfs/src/directory.rs), [persistence](https://github.com/jmacd/watertown/blob/main/crates/tlogfs/src/persistence.rs) |
| Transaction context and coherence | [ProviderContext](https://github.com/jmacd/watertown/blob/main/crates/tinyfs/src/context.rs), [coherence](https://github.com/jmacd/watertown/blob/main/crates/tinyfs/src/coherence.rs) |
| Parsed-version caching and membership | [format cache](https://github.com/jmacd/watertown/blob/main/crates/provider/src/format_cache.rs), [version cache](https://github.com/jmacd/watertown/blob/main/crates/provider/src/version_cache.rs) |
| Partial aggregates and incremental reduction | [partial cache](https://github.com/jmacd/watertown/blob/main/crates/provider/src/partial_aggregate_cache.rs), [temporal reduction](https://github.com/jmacd/watertown/blob/main/crates/provider/src/factory/temporal_reduce/mod.rs) |
| Query and materialization contracts | [query foundation](https://github.com/jmacd/watertown/tree/main/crates/query-foundation/src), [pond adapters](https://github.com/jmacd/watertown/blob/main/crates/provider/src/query_foundation_adapter.rs) |
| Export reuse and post-commit publication | [export](https://github.com/jmacd/watertown/blob/main/crates/provider/src/export.rs), [Steward guard](https://github.com/jmacd/watertown/blob/main/crates/steward/src/guard.rs) |
