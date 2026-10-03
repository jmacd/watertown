# Bounded-Memory Temporal Reduce: Sealed Runs + Hot Window

> **Status:** Phases 1, 2 & 3 **implemented** and integrated with the typed
> query foundation. Motivated by the watershop **selfmon** deployment, which
> was run deliberately as a long-lived stressor and exposed unbounded memory
> growth in the `temporal-reduce` read path. This note records the reorganization
> of the reduced-series materialization so that **no operator ever holds state
> proportional to total history**, while **all history is retained and remains
> fully queryable** (this bounds *memory*, not *how far back you can query*). The
> raw object model, the series delta layout, and the factory configuration
> surface are carried forward unchanged; only *how the reduced output is merged,
> cached, and stored* changes.

---

## Current integration

This design supplied the sealed-run, hot-window, and hierarchical-partial
storage machinery used by the completed DataFusion query-foundation work.
Production behavior now has these additional guarantees:

- partial creation, merge, reconstruction, and fixed-window reduction are
  typed DataFusion plans; generated reduction SQL remains only as a test oracle;
- recipe identity, exact source membership, settled frontier, and dirty ranges
  are explicit contracts rather than cache-directory inference;
- recursive physical lineage lets a cached reduction become a bounded source
  for another reduction;
- `temporal-reduce-series` exposes one exact source and resolution directly,
  avoiding a materialized intermediate series;
- a no-change read in a fresh process executes no dynamic source;
- append, ordinary disorder, and supported retroactive repair each have
  separate bounded-work tests; unsupported repair fails visibly; and
- transactional streaming publication exposes neither partial output nor
  progress after failure.

The SQL names and line references below document the implementation that led
to the sealed/hot layout. Later sections already record where production SQL
was replaced by typed partial plans. The storage invariants remain current
even where historical function names have moved.

## 0. Problem statement

Selfmon renders a dashboard every ~1 min by reducing an append-only perf/journal
series into `res=60/600/3600/…` rollups (`temporal-reduce`) and building a site
from them. Over a single uninterrupted run ("epoch") its peak memory grows
**without bound** and is reset only by wiping the instance.

Observed on watershop (`pond-selfmon@watershop-selfmon`, daily-max of pond's own
"Peak memory usage", one epoch):

| Date | Daily max (tracked) |
|---|---|
| 2026-07-09 | 2220 MB |
| 2026-07-10 | 2287 MB |
| 2026-07-11 | 2329 MB |
| 2026-07-12 | 2344 MB |
| 2026-07-13 | 2362 MB |
| 2026-07-14 | 2372 MB |
| 2026-07-15 | 2383 MB |

Monotonic, ~+25 MB/day, no plateau; live process **RSS ≈ 3.9 GB** (RSS includes
mmap'd parquet and hash-table state the internal counter does not track).
Previous epochs show the same sawtooth: a steady climb (May 7 ~120 MB → Jun 10
~1.1 GB) reset only by reinit/`reset_instances`.

Storage is *not* the problem — `maintain --prune/--keep-txns` already bounds the
control oplog (control dir = 25 MB, healthy). The problem is **memory**, and the
sharper framing that drives this design:

> Processing more data should cost more **time**, not more **memory**. A
> streaming scan is O(1) memory regardless of length. If memory scales with
> total history, some operator is holding O(N) state.

## 1. Root cause

Two blocking (pipeline-breaking) operators run over **all of history on every
build**, in `TemporalReduce::…` merged-cache construction:

### 1a. `GROUP BY time_bucket` over the entire partials set — the primary O(N)

`partials_table` is a listing table over the **whole** partials directory (all
cached versions = full history):

```
crates/provider/src/factory/temporal_reduce.rs:531-543   // register listing_table_from_dir(glob_dir) as __rollup_partials_*
crates/provider/src/factory/temporal_reduce.rs:1499-1517  // merge_sql: SELECT date_bin(...) AS time_bucket, <merge_exprs> FROM partials GROUP BY time_bucket ORDER BY time_bucket
```

Hash aggregation is *stateful and blocking*: it builds an in-memory hash table
with **one accumulator per distinct group** and emits nothing until all input is
consumed. The group key is `time_bucket`, so:

```
groups = number of time buckets over all history = O(N)
```

That group table is resident memory proportional to total data. This is the
operation that fundamentally needs more RAM for more data — not the scan, the
**group state**. Unlike a sort it does not spill gracefully, and its size is
dictated by group cardinality, which grows linearly with elapsed wall-clock.

### 1b. Redundant global `ORDER BY ts` over the whole reconstructed series

The incremental splice re-sorts the entire series (old cache ⧺ freshly merged
tail) even though both inputs are already sorted and disjoint at `lo`:

```
crates/provider/src/factory/temporal_reduce.rs:628-634
    SELECT * FROM (
        SELECT * FROM {old}  WHERE ts <  lo    -- entire history, already sorted
        UNION ALL
        SELECT * FROM ({merge}) WHERE ts >= lo -- new tail, merge_sql already ORDER BY time_bucket
    ) ORDER BY ts                              -- <-- forces a full Sort over all N rows
```

`{old}` is a parquet cache written sorted; `{merge}` ends in `ORDER BY
time_bucket` (§1a); and everything in branch 1 is `< lo ≤` everything in branch
2. The outer `ORDER BY` is therefore **redundant** but forces DataFusion to plan
a `Sort` buffering all N rows (spills, but still large + O(N) bookkeeping).

### 1c. Why "increments" don't help today

The code *is* reaching for incrementality — per-version partials, a persisted
`frontier` (`write_frontier`, `temporal_reduce.rs:515-518`),
`find_uncached_members` (`:470`), and a `dirty_lo` splice point (`:547,582`) —
but the **read-side reconstruction still re-groups and re-sorts all history**
every build and treats the merged cache as mutable. Nothing is ever *final*.

## 2. Key insight: time buckets seal

Time-bucketing has a property the current design discards:

> A reduced bucket is a pure function of a bounded, contiguous slice of input.
> Once its window is far enough in the past that **no future ingest can land in
> it**, its aggregate is final forever.

So `GROUP BY time_bucket` is not the flaw — downsampling *is* time-bucketing. The
flaw is grouping over **all** time, **every** build, and treating the result as
**mutable**. A sealed bucket should be computed **once** and never re-scanned,
re-grouped, or re-sorted.

## 3. Proposed architecture: sealed append-only runs + bounded hot window

Reorganize each reduced series into two zones separated by a **watermark**
(derived from the existing `frontier` minus an allowed-lateness bound):

- **Sealed history** — buckets below the watermark. Immutable. Written **once**
  as sorted, append-only run files (LSM-style segments). Never re-scanned,
  re-grouped, or re-sorted. Reading them is concatenation.
- **Hot window** — the bounded set of open buckets above the watermark that can
  still receive late/straddling data. The **only** place `GROUP BY` runs; holds
  O(hot-window) state — constant, independent of history.

Every stage becomes bounded **in memory** — while retaining *all* history and
keeping it fully queryable:

| Stage | Today | Proposed |
|---|---|---|
| Aggregate memory | O(all buckets) hash table | O(hot window) |
| Rebuild compute / tick | O(all partials) re-group + O(N) re-sort | O(new data) |
| Read memory | full re-merge + global sort, O(N) | k-way **SortPreservingMerge** of sorted runs + hot set, **O(1)** |
| Read time (full history) | O(N) | O(N) — but streamed, constant memory |
| Disk | grows (append-only) | grows (append-only) — **unchanged; all history kept** |

Read path = `SortPreservingMerge(sealed_run_0 … sealed_run_k, hot_buckets)` over
already-sorted, disjoint inputs → streaming, O(1) memory, no global sort, no
global GROUP BY.

**Memory and retention are independent.** This design bounds *memory*, not *how
far back you can query*. Every bucket ever computed stays on disk and stays
readable forever; a full-history scan simply takes longer (O(N) time) while using
constant memory (O(1)). Nothing here drops, expires, or windows the data.

This is the standard streaming-aggregation model (watermarks + allowed lateness
+ immutable output runs). Watertown already has the substrate: series types
store **append-only deltas concatenated on read**
(`temporal_reduce.rs:452-456`), and the raw layer is log-structured.

## 4. Concrete change proposal (increments toward the target architecture)

The end state (§3) is the goal from the first commit. The phases below are
**increments that each land real structural progress toward it** — not throwaway
stopgaps. Every phase moves the system closer to sealed-runs + hot-window and is
independently reviewable and shippable.

### Phase 1 — Bound the aggregate and the read to the hot window

The two O(N) operators (§1a, §1b) are eliminated by only ever
grouping/sorting the buckets that can still change:

1. **Scope the aggregate to the dirty window.** Push `time_bucket >= lo` *into*
   the partials scan feeding `merge_sql` so the hash table only holds recent
   (open) buckets, never all history. `merge_sql` (`:1499`) gains a lower-bound
   predicate on the `date_bin` expression; the splice (around `:608-644`) passes
   `dirty_lo` down instead of filtering the merged output afterward.
2. **Replace the global `ORDER BY ts` with an order-preserving concat** (`:631`).
   Branch 1 (`ts < lo`, sealed prefix) and branch 2 (`ts >= lo`, freshly merged
   tail) are each already sorted and disjoint at `lo`, so emit branch 1 then
   branch 2 under a `SortPreservingMerge` (declare both inputs sorted on `ts`) —
   no buffering Sort.

This already makes per-tick **aggregate + sort memory** O(hot window) instead of
O(N), using the *same* watermark concept (`lo` = the dirty boundary) that Phase 2
generalizes. It is the first real slice of the target model, not a detour.

> **Remaining after Phase 1:** branch 1 is still copied *through* the merged
> cache each rebuild — O(N) *I/O and time*, but O(1) *memory* (streamed). Phase 2
> removes that copy by making the sealed prefix immutable so it is never
> rewritten.

### Phase 2 — Sealed immutable runs + explicit watermark

Turn the "sealed prefix" from Phase 1 into first-class immutable storage,
replacing the single mutable merged-output cache
(`rollup_cache::merged_cache_path` / `write_merged_cache` /
`verify_merged_cache` / `*_coverage`):

1. **Watermark.** Define `watermark = frontier − allowed_lateness` (§5). A bucket
   whose window end `≤ watermark` is *sealed* and can never change again.
2. **Sealed runs.** When buckets cross the watermark, append them **once** to an
   immutable sorted run file (content-addressed; never rewritten). Maintain a
   small manifest of run files + their `[lo,hi]` bucket ranges. Optionally
   compact adjacent small runs in the background (LSM leveling) — a bounded,
   O(run) operation, never O(N).
3. **Hot state only.** `GROUP BY` runs exclusively over partials whose bucket
   `> watermark`. Bounded group table, permanently.
4. **Read = SortPreservingMerge** over (sealed runs ⧺ hot buckets). No global
   sort, no global GROUP BY, no rewrite of history. The `ProviderContext` export
   hint (`digest` / `changed_since`) is derived from the newest sealed run + hot
   set.

This removes the O(N) per-tick I/O left after Phase 1 and makes memory O(hot
window) end-to-end. The existing `frontier`, per-version partials, and `dirty_lo`
machinery are **repurposed** (frontier → watermark source; partials → hot-window
inputs), not thrown away.

### Phase 3 — Hierarchical rollup (cheaper coarse history; still keeps everything)

Build `res=600` by folding sealed `res=60` runs, `res=3600` from `res=600`, etc.
The resolutions already nest (`parse_nesting_resolutions`, `:1076`), so each
level is a bounded fold over the level below and is exponentially smaller.

This is a **compute/disk efficiency** improvement, **not** a retention policy:
all resolutions, including fine `res=60`, are kept for all history and stay
queryable. Coarse levels just make long-range queries and rebuilds cheaper. No
data is ever dropped, expired, or windowed.

## 5. The one semantic decision this forces

**Maximum allowed lateness — when is a bucket final?** Ingests can deliver
out-of-order/straddling data across ticks, so a bucket is only sealable once no
future version can contribute to it. Options:

- **A. Fixed lateness bound** (e.g. seal buckets whose end `< frontier − 5 min`).
  Simple; late data beyond the bound is dropped or lands in a correction run.
- **B. Source-driven** (seal at `frontier` when the source is strictly
  append-only in time — true for the perf/journal series here). Zero lateness;
  maximal sealing.
- **C. Per-factory config** (`allowed_lateness: <duration>`), defaulting to B.

**Decided: C, defaulting to a 1-day window** (`allowed_lateness`, humantime;
default `DEFAULT_ALLOWED_LATENESS = 1d`). Data arriving older than the sealed
watermark (beyond `allowed_lateness`) is a **hard error** suggesting `--rebuild`,
never a silent drop or unbounded backfill — consistent with the project's
prefer-hard-failures stance. Changing `allowed_lateness` (recorded in the
manifest, not `cfg_hash`) resets the resolution's cache and rebuilds from the
retained partials. Note this is purely about *when a bucket stops changing* (so
it can be sealed) — it does **not** bound retention; sealed buckets are kept
forever.

## 6. Compatibility & migration

- Config surface (`resolutions`, `aggregations`, `in_pattern`, …) unchanged.
- **All history is retained and queryable throughout** — no phase drops, expires,
  or windows data. Only the *memory* used to compute and serve it changes.
- Phase 1 is cache-compatible (no on-disk format change).
- Phase 2 changes the on-disk reduced-cache layout; migrate by discarding the
  old merged cache and rebuilding once from partials (a single cold O(N) build,
  then bounded memory forever). Corrupt/absent caches already trigger a full
  rebuild today, so the fallback path exists.
- Raw series, object model, transparency log, and `steward`/`sync-store` remote
  formats are untouched.
- **Applies to every `temporal-reduce` consumer** (site-prod/-staging dashboards,
  the noyo subsite, selfmon, and any third-party pond) with no config change.
  Selfmon is merely the highest-cadence, longest-epoch stressor that surfaced it
  first.

## 7. Risks & validation

- **Output equivalence.** Golden test: Phase 1/2 output byte-identical (or
  aggregate-identical modulo float assoc) to the current full rebuild on a fixed
  fixture. Reuse existing `temporal_reduce` tests (`test_factory.rs`,
  `test_support.rs`).
- **Full-history queryability.** Assert a query spanning genesis→now returns the
  same rows before and after each phase (retention is never reduced), and that it
  runs in constant memory (O(1)) regardless of series length.
- **Late data correctness.** Test straddling/out-of-order versions against the
  chosen lateness bound; assert sealed buckets are never mutated and late-beyond-
  bound data follows the documented policy (drop vs correction run) — hard
  failure, no silent fallback.
- **Memory regression guard.** Extend the selfmon stressor: assert daily-max peak
  memory plateaus across an epoch (flat, not monotonic) on a synthetic
  ever-growing series, while disk keeps growing (proving history is retained).
- **Interaction with `POND_MEMORY_LIMIT_MB`** (FairSpillPool,
  `tlogfs/src/persistence.rs`): Phase 2 should make the reduced path fit well
  under the pool regardless of history; verify the pool is no longer the binding
  constraint for selfmon sitegen builds.

## 8. Implementation progress

| Phase | Scope | Status |
|---|---|---|
| 1 | Hot-window GROUP BY + order-preserving concat read (cache-compatible) | **Implemented** |
| 2 | Watermark + sealed immutable runs + bounded hot window; ListingTable read | **Implemented** |
| 3 | Hierarchical rollup (cheaper coarse history; retains all data) | **Implemented** (step 1: partials-in-runs + read-time reconstruction; step 2: coarser-from-finer cascade fold) |

### Phase 1 implementation notes

- `AggSqlPieces::merge_sql` gained an optional `lower_bound` (epoch seconds).
  When set, `WHERE CAST(EXTRACT(EPOCH FROM date_bin(...)) AS BIGINT) >= lo` is
  pushed *into* the partials scan, so the `GROUP BY` hash table only ever holds
  the hot window (§1a). The full-rebuild arm (cache miss) still passes `None`.
- The splice's cached branch is registered with a declared `file_sort_order` on
  the timestamp column and `optimizer.prefer_existing_sort = true`, so the outer
  `ORDER BY` is satisfied by a streaming `SortPreservingMergeExec` over the
  already-sorted sealed prefix instead of a buffering `Sort` over all N rows
  (§1b). Verified via `EXPLAIN`: the sealed-prefix `DataSourceExec` carries
  `output_ordering=[timestamp ASC]` with no `SortExec`; only the bounded hot
  tail is sorted. `ORDER BY` is retained for correctness (a bare `UNION ALL`
  would let `CoalescePartitions` interleave the branches).
- **Latent-bug fix.** All ts boundary comparisons now use
  `CAST(EXTRACT(EPOCH FROM ts) AS BIGINT)`. Previously the splice compared the
  merged cache's nanosecond `timestamp` against the epoch-seconds `dirty_lo`
  with `CAST(ts AS BIGINT)`, so the `ts < lo` prefix branch was *always empty*;
  the incremental splice silently full-remerged every build via the tail branch
  (still correct output, but no prefix reuse). Phase 1 both scopes the tail and
  makes prefix reuse actually happen.

### Phase 2 implementation notes

- **Config.** `TemporalReduceConfig` gained `allowed_lateness: Option<String>`
  (humantime, e.g. `"1d"`, `"36h"`), defaulting to **1 day**
  (`DEFAULT_ALLOWED_LATENESS`). It is **not** part of `cfg_hash`; instead the
  value is recorded in the per-resolution manifest. Changing it is detected as a
  manifest mismatch, which wipes the resolution's cache dir and rebuilds from the
  retained partials (partials are unaffected, so no re-ingest).
- **On-disk layout.** Each resolution lives in
  `{cache}/merged_{cfg}_{node}/res{secs}/`:
  - `run-{seq:08}.parquet` — immutable sealed runs, written once, never rewritten.
  - `hot.parquet` — the open window, recomputed every build.
  - `manifest.json` — `{ allowed_lateness_secs, sealed_hi_secs, next_seq,
    runs[{name,lo_secs,hi_secs,digest}], hot_digest, covered[] }`. Its serialized
    bytes' blake3 is the export-hint digest (deterministic: fixed serde field
    order + `BTreeSet` for `covered`).
- **Watermark.** `watermark = align_down(max_output_bucket_secs − allowed_lateness,
  interval)`, using `div_euclid` so negative watermarks (short history) align
  correctly. Buckets that start `< watermark` are sealed; buckets `>= watermark`
  are hot. `max_output_bucket_secs` is a single bounded `MAX(EXTRACT(EPOCH …))`
  scan over the partials — no per-bucket state.
- **Build classification** (against `manifest.covered` vs the current partial set):
  *Reuse* (equal → serve as-is), *Incremental* (superset → advance watermark,
  seal `[old_sealed_hi, watermark)` into a new run if non-empty, recompute
  `hot.parquet` from `[sealed_hi, ∞)`), or *Rebuild* (any other shape, or a
  missing/mismatched manifest → wipe + full rebuild). Sealing and the hot
  recompute both read the **current** partials, so within-window late data lands
  in `hot.parquet`.
- **Bounded per-build cost.** Sealed runs are never re-read or rewritten; each
  build only re-merges the hot window (`~allowed_lateness` of buckets) plus at
  most one newly-sealed span. This removes the O(N) per-tick I/O that Phase 1
  still paid to copy the sealed prefix through the merged cache.
- **Hard-fail on beyond-window backfill (§5, decision C).** If an incremental
  build's earliest new output bucket precedes `sealed_hi_secs`, it would reopen
  an immutable run; this is a hard error suggesting `--rebuild`, never a silent
  unbounded backfill. Within-window backfill (older than nothing sealed) merges
  normally.
- **Read path (bounded, streaming).** `listing_table_for_res_dir` serves a
  `ListingTable` over all runs + `hot.parquet`; consumers apply their own
  `ORDER BY timestamp`. Missing manifest runs or a missing hot file are hard
  `CacheCorrupt` errors. To keep a whole-series `ORDER BY timestamp` at **O(1)**
  memory (design §3) rather than an O(N) buffering `SortExec`, three things line
  up: (1) each run/hot file is written internally sorted on the bucket timestamp
  (`write_merge_to` ends `ORDER BY time_bucket`) and the files are disjoint /
  totally ordered (runs cover `[lo,hi)` spans, hot covers `[sealed_hi, ∞)`);
  (2) `listing_table_for_res_dir` declares this by setting
  `with_collect_stat(true)` + `with_file_sort_order([ts ASC])` on the
  `ListingOptions`; and (3) `try_rollup_table_provider` enables
  `execution.split_file_groups_by_statistics` on the shared provider session
  (off by default). With statistics collected and that option on, DataFusion
  emits the ordered scan as either a single ordered file group (scan
  `output_ordering`, sequential streaming read) or a k-way
  `SortPreservingMergeExec` across per-file partitions — both stream without
  buffering the series. Because the reduced provider is built and later queried
  on that same session (export, sitegen), the option reaches all production read
  paths. Verified by `test_sealed_read_uses_sort_preserving_merge`, which EXPLAINs
  a full-history `ORDER BY` over a multi-run resolution and asserts no `SortExec`.
- **Export hint.** digest = manifest digest (stable when unchanged);
  `changed_since = Some(dirty_lo_secs)` on incremental builds, `None` on
  rebuild/reuse.
- **Removed Phase-1 code.** The single-file merged-output cache helpers
  (`merged_cache_path`, `write_merged_cache`, `verify_merged_cache`,
  `read_merged_digest`, `{read,write}_merged_coverage`, `listing_table_for_file`
  and their `.blake3`/`.coverage` sidecars) are fully superseded by the
  sealed-runs manifest and were deleted.

### Phase 3 implementation notes (step 1: partials-in-runs + read-time reconstruction)

Phase 3 lets a coarser resolution be built by folding a finer resolution's
sealed runs (exponentially cheaper coarse history) instead of re-scanning the
finest partials. That fold is only correct if the runs store **associatively
mergeable** quantities: `Sum`, `Count`, `Min`, `Max` fold, but a reconstructed
`Avg` does **not** (`avg(avg) ≠ avg` without count weighting). Step 1 makes the
on-disk runs foldable without changing any observable output; the cross-level
fold itself (step 2) is a follow-up.

- **Runs store partials, not output.** `SealedManifest` gained a `format` field
  (`SEALED_FORMAT = "partials-v1"`). Sealed runs and `hot.parquet` are now
  written by `AggSqlPieces::merge_partials_sql`, whose final projection emits the
  **merged partial columns** (`__p_sum_*`, `__p_count_*`, `__p_min_*`,
  `__p_max_*`, `__p_cstar_*`) plus the timestamp — the same `merged` CTE as
  `merge_sql`, just without the reconstruction step. This is verified by
  `test_sealed_runs_store_partials_not_output`, which asserts a run's parquet
  schema carries `__p_sum_*`/`__p_count_*` and **not** `temperature.avg`.
- **Reconstruction at read time.** `try_rollup_table_provider` wraps the
  partials `ListingTable` in a `ViewTable` whose plan is
  `AggSqlPieces::reconstruct_sql` = `SELECT ts, <reconstruct_exprs> FROM
  <partials>`. This reproduces the exact output columns (names, order, values,
  including `Avg = Sum / Count`) that `merge_sql` produced directly, so every
  consumer sees identical results. The listing provider is embedded in the
  view's logical plan (it is registered, planned, then deregistered), so the view
  resolves in any consumer session.
- **Streaming read preserved.** The reconstruction projection passes the
  timestamp column through unchanged, so the scan's declared `output_ordering`
  (the Phase-2 hardening) still satisfies a consumer's `ORDER BY timestamp`
  without a `SortExec`. `test_sealed_read_uses_sort_preserving_merge` now
  exercises the full path through the view (in a separate consumer session) and
  still asserts no `SortExec`.
- **Migration.** The `format` mismatch is handled like `allowed_lateness`:
  a manifest whose `format` differs from `SEALED_FORMAT` (including legacy
  manifests, which deserialize to `""`) wipes and rebuilds the resolution's res
  dir from the retained finest partials — a single cold rebuild, no re-ingest.
- **`merge_sql` retained (test-only).** It stays as the reference definition of
  the reconstructed output and as the equivalence oracle in tests; production no
  longer calls it. `merge_partials_sql` + `reconstruct_sql` compose to exactly
  `merge_sql`.

**Remaining (step 2): implemented.** See the notes below.

### Phase 3 implementation notes (step 2: coarser-from-finer cascade fold)

Step 2 turns each coarser resolution into a **bounded fold over the level below**
instead of re-scanning the shared finest partials every build. Because the stored
partials are associative (step 1) and the configured resolutions nest (each
coarser interval is an integer multiple of the next-finer one, enforced by
`parse_nesting_resolutions`), folding a coarse bucket from the next-finer runs +
hot yields exactly the same result as folding it from the finest partials — the
output is unchanged; only the I/O per level changes.

- **Cascade build.** `try_rollup_table_provider` builds the chain
  finest → this file's resolution. The finest level folds the shared partials
  (`build_level_from_partials`, input bucket column `time_bucket`); each coarser
  level folds the next-finer level's `ListingTable` (runs + hot) via
  `build_level_from_finer` (input bucket column = the timestamp column). The
  finer provider is registered as a temporary `__rollup_finer_*` table, folded,
  then deregistered. A consumer of any resolution transparently materializes the
  finer levels it depends on, and each level reuses its cache when unchanged, so
  the cascade is cheap after the first cold build.
- **Shared seal + hot.** Both level builders call `seal_and_recompute`, which
  computes the watermark from a bounded `MAX` over the input, seals
  `[sealed_hi, watermark)` into a new immutable run (skipping empty spans), and
  recomputes the open hot window `[sealed_hi, ∞)`. All fold SQL is
  `merge_partials_sql`, generalized in step 2 to take the input bucket column so
  it can re-bin either `time_bucket` (finest) or the finer level's timestamp.
- **Bounded advance.** A coarse level's watermark aligns down at least as far as
  the finer level's (coarser interval, same `allowed_lateness`), so
  `coarse_sealed_hi ≤ finer_sealed_hi`: the finer history below a coarse sealed
  bucket is already immutable, so the coarse sealed runs stay valid and the level
  can **advance** in place (seal newly-frozen buckets, recompute hot) rather than
  rebuild. The hot recompute reads only `< coarse_interval + allowed_lateness` of
  finer input, so per-build cost stays bounded.
- **Freshness keys per level.** The finest level keys freshness on its partial
  member set (`covered`) and hard-fails on a backfill older than its watermark
  (Phase 2). A coarse level instead records the finer manifest digest it folded
  (`source_digest`) and whether the finer level was rebuilt (`finer_rebuilt`,
  threaded down the chain): it **reuses** when the finer level was reused and its
  digest is unchanged, **advances** when the finer level only appended, and
  **rebuilds** (wipe + fold from empty) when the finer level was rebuilt (e.g. a
  member removal) or no compatible manifest exists. No beyond-window hard-fail is
  needed at coarse levels — the finest level already enforces the append-only
  contract for the whole chain.
- **Manifest format.** `SEALED_FORMAT` was bumped to `"partials-v2"` and
  `SealedManifest` gained `source_digest: Option<String>`. A format mismatch
  wipes and rebuilds the level (single cold rebuild, no re-ingest), as with
  `allowed_lateness` and the step-1 migration.
- **Verification.** `test_rollup_cascading_resolutions_share_finest_partials`
  remains the correctness oracle (cascade output == single-pass for Avg/Min/Max
  across `1h`/`6h`/`1d`, with the raw source scanned only once).
  `test_rollup_cascade_materializes_finer_levels_on_disk` asserts that requesting
  only the coarsest resolution creates every intermediate `res{secs}` dir with
  its own manifest + hot file and records `source_digest` on the coarse levels
  (and `None` on the finest).
