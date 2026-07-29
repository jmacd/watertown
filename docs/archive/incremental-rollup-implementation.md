> **SUPERSEDED (see `docs/rollup-storage-redesign.md`).** Kept for history.
>
> Two of this document's load-bearing claims are no longer true of the code:
>
> - *"No watermark, no mutable state file."* There is a watermark and a
>   `manifest.json` per resolution. Content addressing alone bounded neither the
>   file count nor the per-build merge cost.
> - *"Enabling invariant: sequential input."* The rollup no longer requires it.
>   The cache is keyed by the time range each file covers, so late data unseals
>   the segments covering it and recomputes them. The sequentiality frontier and
>   the `--rebuild` recovery it needed are both gone; `--rebuild` survives only
>   as a maintenance escape hatch.
>
> Its instruction to "mirror `format_cache.rs` **exactly**, in a new sibling
> module" is also worth not repeating: the copies drifted, and the resulting
> mechanisms were individually incomplete rather than redundant, so correctness
> depended on all of them agreeing with nothing forcing it.

# Incremental temporal-reduce rollup: repair design and plan

Status: design + implementation plan. Phase 1 (decomposable-aggregate
lowering), phase 2 (per-version partial-aggregate cache, wired through the
temporal-reduce format-provider path), phase 3 (cascading rollups: finest
partials shared and re-binned across nesting resolutions), and phase 4
(sequentiality-violation detection plus the `--rebuild` recovery flag on
`export`) have all landed.

This document is the concrete, code-grounded repair plan for the dominant
remaining build-time inefficiency in duckpond: `temporal-reduce` exports cost
`O(history)` per resolution per build instead of `O(new data)`. It operationalizes
the architecture sketched in `incremental-timeseries-rollup.md` into landable
phases with exact code anchors.

Code citations use `crate/file.rs:line` and were verified against the working
tree (branch `jmacd/57`) when written.

## 1. The defect, precisely

A `temporal-reduce` node downsamples one source series into several
per-resolution aggregations. For Caspar Water that is 5 metrics x 5 resolutions
= 25 materialized exports per site build.

- `generate_temporal_sql` emits a single
  `... GROUP BY date_bin({interval}, {ts}, epoch)` with **no time predicate**
  over the whole source table
  (`crates/provider/src/factory/temporal_reduce.rs:556-577`).
- The node is `table:dynamic`, recomputed on every read; sitegen materializes
  each resolution to a static parquet at build time via
  `provider::export::export_series_to_parquet`
  (`crates/sitegen/src/factory.rs:1088`).
- Net cost per build = `O(history) x #resolutions x #metrics`, and it grows
  without bound as the pond ages.

The format cache already makes the **leaf parse** incremental: a single
`ListingTable` is built from per-version cached parquet, and only newly ingested
versions are parsed (`crates/provider/src/factory/sql_derived.rs:968-995`,
`crates/provider/src/format_cache.rs`). It does **not** make the **aggregation**
incremental: the `GROUP BY date_bin` still scans every cached row on every build.
This document closes that second gap.

## 2. Enabling invariant: sequential input

HydroVu and oteljson ingest are structurally sequential: event time is
non-decreasing and append-only. Under this invariant a time bucket is **sealed**
once the input frontier passes its upper boundary and can never change again.
Only the single open frontier bucket is mutable between builds.

Late-arriving data (event time older than already-ingested data) is an invariant
violation, not a routine case. Per the project no-fallback philosophy it is
detected and surfaced as a hard error, with `--rebuild` as the recovery tool,
never silently merged.

## 3. Prerequisite: decomposable aggregates

Supported aggregations are `Avg, Min, Max, Count, Sum`
(`crates/provider/src/factory/temporal_reduce.rs:80-86`). All are decomposable
over a partition union except `Avg`, and `Avg = Sum / Count`.

> Invariant for all phases below: store `Sum` and `Count` partials internally;
> reconstruct `Avg = Sum / Count` at read time. Never persist `Avg` as a
> non-mergeable value.

With that, every stored rollup value is associative:

| Aggregate | Partial stored | Merge across partitions |
|-----------|----------------|-------------------------|
| Sum   | sum         | `SUM(sum)`              |
| Count | count       | `SUM(count)`            |
| Min   | min         | `MIN(min)`              |
| Max   | max         | `MAX(max)`              |
| Avg   | sum, count  | `SUM(sum)/SUM(count)`   |

## 4. Architecture

Two cache tiers, both reusing the existing content-addressed, throwaway,
per-version-immutable cache discipline under `{POND}/cache/`.

### 4.1 Tier: per-version partial-aggregate cache (the core win)

Ingest already lands one immutable parquet per input version, each identified by
its blake3 (the same key the format cache uses,
`crates/provider/src/format_cache.rs:55-68`). For a sequential source those
versions are time-disjoint except at adjacent boundaries.

```
for each input version V (blake3 already known from the format cache):
    if rollup-cache has partials(V, finest_resolution): reuse
    else: compute partials over V only, keyed by bucket:
          (time_bucket, sum_c, count_c, min_c, max_c) per aggregated column c
          write to {POND}/cache/rollup_{cfg_hash}_{node}/v{ver}_{blake3}.parquet
read(resolution) = merge_partials(all cached partials for that resolution)
```

- **Cache key = input version blake3.** Immutable versions are never
  invalidated; one new ingest version produces exactly one new partial.
- **No watermark, no mutable state file.** Immutability plus content addressing
  do the bookkeeping; `rm -rf {POND}/cache/` stays safe.
- **Per-build cost = O(new versions)**, typically one.

Boundary buckets: a wall-clock bucket may straddle two adjacent versions. Because
partials are decomposable, the read-time merge groups partials by `time_bucket`
and combines them, reconstructing the straddling bucket exactly. No special case
beyond `GROUP BY time_bucket` over the unioned partials.

### 4.2 Tier: cascading rollups (coarse views nearly free)

Coarse resolutions must never touch raw history. Build a chain where each level
re-buckets the next-finer level's partials:

```
raw --(4.1)--> finest partials --> coarse_1 partials --> coarse_2 partials ...
```

- A coarser level applies `date_bin(coarse_interval, time_bucket, epoch)` over
  the finer partials and merges the decomposable values.
- Each coarser level scans a tiny finer level, not raw. Total work falls from
  `O(history x #resolutions)` to roughly `O(history)` once at the finest level
  (via 4.1) plus `O(small)` per coarser level.
- Requires nesting resolutions: each coarser interval must be an integer
  multiple of the next finer one, so finer buckets never split across a coarser
  boundary. Reject non-nesting resolution sets at validation
  (`crates/provider/src/factory/temporal_reduce.rs:1029-1033`).

### 4.3 Read-time merge SQL (sketch)

```sql
SELECT time_bucket AS timestamp,
       SUM(sum_x)               AS "x.sum",
       SUM(count_x)             AS "x.count",
       SUM(sum_x)/SUM(count_x)  AS "x.avg",   -- Avg reconstructed
       MIN(min_x)               AS "x.min",
       MAX(max_x)               AS "x.max"
FROM   <union of cached per-version partials for this resolution>
GROUP BY time_bucket
ORDER BY time_bucket
```

The union of partials is one row per (bucket, version), so the merge is cheap and
independent of raw history length.

## 5. Cache layout and keys

Mirror `crates/provider/src/format_cache.rs` exactly, in a new sibling module
`crates/provider/src/rollup_cache.rs`.

```
{POND}/cache/rollup_{cfg_hash}_{node_id}/
    v{version}_{blake3}.parquet      # finest-resolution per-version partials
    res={interval}/v{version}_{blake3}.parquet   # optional, cascaded partials
```

- `cfg_hash` = stable hash over the aggregation set, time column, and resolution
  list. A config change yields a fresh cache namespace rather than mixing
  semantics into one directory. Changing config invalidates semantics, not
  content, so a new namespace is the correct response, and the old one is reaped
  by normal cache pruning (`efficiency-priorities.md` section 7).
- `blake3` = the input version blake3 already surfaced by the format-cache path
  (`format_cache::find_uncached_versions` / `FileVersionInfo.blake3`).
- New API surface parallels the format cache:
  `find_uncached_versions`, `cache_write_version`,
  `listing_table_from_cache`. Reuse `FileVersionInfo`.

## 6. Code change map

| Area | File / anchor | Change |
|------|---------------|--------|
| Avg lowering | `temporal_reduce.rs:80-98`, `498-577` | Internally emit `Sum`+`Count` for `Avg`; reconstruct `Avg=Sum/Count` in the final SELECT. Output schema and column aliases unchanged. |
| SQL split | `generate_temporal_sql` (`temporal_reduce.rs:498-578`) | Split into (a) per-version partial SQL keyed by `time_bucket`, (b) cross-version merge SQL. |
| Rollup cache | new `crates/provider/src/rollup_cache.rs` | Key by input version blake3; `find_uncached_versions` / `cache_write_version` / `listing_table_from_cache` analogues of `format_cache.rs`. |
| Wiring | `sql_derived.rs:960-995` glob path; `TemporalReduceSqlFile` (`temporal_reduce.rs:147-184`) | After the format cache yields per-version parquet, compute/reuse partials, then serve the merge SQL over partials instead of the raw ListingTable. |
| Resolution nesting | validation near `temporal_reduce.rs:1029-1033` | Parse resolutions into a nesting chain; reject non-nesting sets. |
| Sequentiality guard | partial-build path | Compare new input min-ts against the prior frontier; regression is a hard error that gates `--rebuild`. |
| `--rebuild` | export / maintain surface | Drop the `rollup_{cfg_hash}_{node}` namespace and recompute all partials. |

## 7. Correctness notes

- **Open bucket**: the frontier bucket is simply the partial of the newest, still
  growing input version; it is recomputed whenever that version's blake3 changes.
  No special handling.
- **Sequentiality**: the design assumes non-decreasing event time. If violated,
  an old sealed bucket would need a partial that is no longer recomputed, leaving
  stale output. Detection compares new input min-ts against the prior frontier;
  a regression is a hard failure, not a silent merge, consistent with the
  no-fallback philosophy.
- **Cache safety**: identical to the format cache. Per-version-immutable, content
  addressed, throwaway. `rm -rf {POND}/cache/` recomputes on next read.
- **Config change**: folded into `cfg_hash`, yielding a fresh namespace.
- **Byte-for-byte**: partial-merged output must equal a single full
  `GROUP BY date_bin` recompute, including reconstructed `Avg`.

## 8. Tests (mirror the 534/535 cache-incrementality style)

Testsuite scripts under `duckpond/testsuite/tests/`:

- **Incrementality**: N input versions; assert build K computes exactly one new
  partial and reuses K-1 cached ones; per-build work independent of history
  length. Use a `[SAVE]`/`[REUSE]`-style log line as 534/535 do.
- **Correctness**: partial-merged output == single full `GROUP BY date_bin`
  recompute, byte-for-byte, including `Avg`.
- **Boundary bucket**: a bucket split across two versions reconstructs exactly.
- **Cascade**: `1d` built from cascaded partials == `1d` direct from raw.
- **Invariant violation**: out-of-order input is detected and surfaced as a hard
  error, not silently miscomputed; `--rebuild` restores correctness.
- **Config change**: changing the aggregation set or resolution list switches to
  a fresh cache namespace and recomputes.

Rust unit tests in `temporal_reduce.rs` cover the `Avg`->`Sum`/`Count` lowering
and the per-version partial SQL generation in isolation.

## 9. Phasing

Each phase is independently landable and presubmit-clean
(`cargo fmt --all`, `cargo clippy --workspace --all-features -- -D warnings`,
`cargo test --workspace`).

1. **Decomposable-aggregate lowering** (`Avg`->`Sum`/`Count`). Pure refactor, no
   behavior change, no cache. Safe to land alone; unblocks everything.
   **DONE** -- `generate_temporal_sql` now computes `Sum`/`Count`/`Min`/`Max`/
   `COUNT(*)` partials (deduplicated) in the inner CTE and reconstructs every
   output column, including `Avg = Sum / Count`, in the final SELECT. Output
   schema, column order, and values are unchanged. Unit tests cover the lowering
   and partial deduplication.
2. **Per-version partial cache** (section 4.1). Delivers `O(delta)` builds at the
   finest resolution. The biggest win.
3. **Cascading rollups** (section 4.2). Makes coarse and all-time views nearly
   free.
4. **Sequentiality-violation detection + `--rebuild` recovery path**.
   **DONE** -- a per-source-node frontier sidecar (`{source}.frontier` in the
   rollup glob dir) records the maximum sealed `time_bucket`. Uncached versions
   are processed oldest-first; a version whose earliest bucket precedes the
   persisted frontier is a hard error, never a silent merge. `export --rebuild`
   drops every `rollup_*` namespace under `{POND}/cache/` via
   `rollup_cache::drop_all` so the next read recomputes from scratch.

## 10. Rollout and gating

- The partial cache lives under `{POND}/cache/` and is throwaway, so the feature
  is safe to enable by default: a cold cache simply recomputes once, matching
  today's cost, then every subsequent build is incremental.
- No new persistent pond state and no schema migration. The only on-disk artifact
  is cache content, already covered by cache pruning
  (`efficiency-priorities.md` section 7).
- Verifiable via selfmon build-time and peak-memory series: per-build cost should
  flatten from growing-with-history to roughly constant.

## 11. Alternatives considered (not chosen)

- **Windowed push-down** (`WHERE ts >= anchor - window` per resolution): bounds
  per-build scan but drops history outside each window and needs a rebuild for
  any backfill. Simpler but lossy; superseded by section 4.1, which keeps full
  history at `O(delta)`.
- **Incremental materialized view + watermark**: textbook continuous aggregate;
  correct but introduces mutable watermark/append state that section 4.1 avoids
  by leaning on immutability.
- **LSM-tiered series compaction + hot tail**: the eventual home that also
  addresses oplog/version growth (`efficiency-priorities.md` section 6), but a
  much larger change than sections 4.1/4.2.

## 12. Related documents

- `docs/incremental-timeseries-rollup.md` -- the originating architecture sketch
  this plan operationalizes.
- `docs/format-cache-design.md` / `docs/format-cache-implementation.md` -- the
  content-addressed cache discipline this design reuses.
- `docs/efficiency-priorities.md` -- the broader maintenance/I/O design; section
  7 (format cache pruning) and section 10 (I/O hotspots) are adjacent.
- `docs/memory-analysis-2026-02-23.md` -- earlier sitegen-export memory study;
  its Problems 1 and 2 are since mitigated (glob format cache, export-table
  deregister), leaving the full-history aggregation addressed here.
