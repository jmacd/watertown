# Sitegen full-site build: memory profile and incremental-export plan

Status: partly implemented; see the 2026-08-03 update below for what has landed
and what these measurements no longer describe. Code-grounded as of 2026-07-01.
Measurements taken on watershop against a copy of the prod site pond
(pond 0.52.0). Citations use `crate/file.rs:line`.

Update 2026-08-03: the plan in §5 has been worked through -- two items landed,
one was measured and rejected, and the remaining one is characterized in §6.
The §1 measurements no longer describe current behaviour.

- Incremental export landed: deterministic per-partition `data.parquet` names
  plus an `.export-manifest.json` of digests let a build reuse unchanged
  partitions. Warm site builds now peak at ~405 MB rather than ~2.1 GB.
- The full-rewrite fan-out is now bounded. `full_rewrite_partitioned` splits the
  rewrite into chunks of at most `MAX_OPEN_PARTITIONS` partitions, so a cold
  build no longer holds ~1400 partition writers open at once. Re-measured on
  prod data, peak scales as ~50 MB + ~1.4 MB per open writer, which matches the
  2148 MB / 1381 writers recorded in §1.
- P0 (allocator tuning, `MALLOC_ARENA_MAX`) was implemented and measured, and
  **did not reproduce the predicted win, so it was not adopted.** Running the
  same 1412-partition single-series export with and without
  `MALLOC_ARENA_MAX=2` / `MALLOC_TRIM_THRESHOLD_=131072` /
  `MALLOC_MMAP_THRESHOLD_=131072` gave peak RSS of 431 MB both times. The
  variables were confirmed present in the container's environment, and the
  runtime is glibc 2.36 with `PanicOnLargeAlloc` wrapping the system allocator,
  so the tunables were genuinely active and genuinely inert.
- That experiment also showed **tracked heap badly overstates RSS on the export
  path**: the same run reported 2016 MB of PEAK_ALLOC heap but only 431 MB of
  peak RSS (cgroup `memory.peak`). The per-partition parquet writer buffers are
  largely allocated-but-untouched pages. So on this workload the ~2 GB figure
  was never resident memory, and the effective ceiling was the
  `PanicOnLargeAlloc::new(3000)` accounting guard rather than RAM.
- Caveat: the §1 "~2629 MB RSS" was a *full* site build across all series with
  ~60 threads, not the single-series export measured above. Full-build RSS has
  not been re-measured, so §1's RSS-above-heap relationship may still hold there.
- P1 (the `analysis` stage) is now measured, and it is the remaining peak once
  exports are bounded -- see §6.

Goal: run the whole Caspar Water site build (sitegen exporting from the site
pond, which imports the water/septic/noyo subponds) on a low-memory machine.
This document captures what was measured, how to reproduce the sampling, the
root cause, and a prioritized plan to cut the peak.

Related: `efficiency-dataflow.md` (§5.2, §6 candidates 6.2/6.3),
`incremental-rollup-implementation.md`, `sitegen-design.md`.

---

## 1. Key measurements

- Peak: **~2148 MB tracked heap (PEAK_ALLOC) / ~2629 MB RSS**. Prod journal
  `Run summary`: `peak_mem_mb=2148, elapsed_s=667`.
- RSS runs ~0.5 GB above tracked heap due to mmap'd parquet, ~60 thread stacks,
  and glibc arena fragmentation. RSS is the number that OOMs a small board.
- The peak is set entirely in the **first ~50 s**, after which RSS is flat
  (glibc allocator retention; freed pages are not returned):
  - t=0->14s -> 2159 MB: the 5 water `metrics` `1m` exports, each fanning out
    to **1381 day-partitions**.
  - t=14->50s -> 2626 MB: the `analysis` stage (pump-cycles / cycle-summary),
    a full-history recompute.
  - The noyo subsite does NOT raise the peak (it recomputes cold but plateaus).
- Cheap allocator lever, independent of this plan: `MALLOC_ARENA_MAX=2` plus
  128K trim/mmap thresholds cut peak RSS 2629 -> 1463 MB (44%), measured.

---

## 2. How to reproduce the sampling

The live ponds run as **podman containers** (image
`ghcr.io/jmacd/duckpond/duckpond:{prod,latest}-arm64`), with the pond in podman
volumes `pond-site-{prod,staging}` mounted at `/pond`, driven by systemd user
timers `pond@<instance>.timer` -> `config/scripts/run.sh <instance>`.

Host: `watershop.casparwater.us`, 12 cores, 64 GB, aarch64 Linux, passwordless
sudo.

Reproduce against a COPY so the live pond is never touched:

1. Copy the pond volume data:
   `sudo cp -a <podman volume>/pond-site-prod/_data /tmp/prof-pond`.
2. Extract the matching binary from the image. The host `/usr/bin/pond` is a
   DIFFERENT 0.52.0 build and rejects the newer node config via
   `#[serde(deny_unknown_fields)]` ("No sitegen factory config found in YAML"):
   `podman cp $(podman create <image>):/usr/local/bin/pond /tmp/pond-image`.
3. Optional (only needed to finish the build; the peak is reached before this):
   extract the image `/usr/local/share/duckpond/vendor` to the host same path,
   or the build errors at the final asset-copy step.
4. samply CPU profile: `sudo sysctl kernel.perf_event_paranoid=1`, then run under
   `samply record --unstable-presymbolicate --save-only -o out.json -- <build cmd>`.
   samply 0.13.1 is CPU-only (no memory flag). The binary has `.symtab` but no
   DWARF, so `--unstable-presymbolicate` (which writes an `out.json.syms.json`
   sidecar) is REQUIRED for offline symbolication.
5. RSS timeline: run the build and sample `/proc/<pid>/VmRSS` every ~0.3 s.

Build command (whole site, three subponds):

```
POND=/tmp/prof-pond SITE_BASE_URL=/ RUST_LOG=info POND_MAX_ALLOC_MB=3000 \
  /tmp/pond-image run /system/etc/90-sitegen build <outdir>
```

`run.sh` also does content/template/img pulls and `pull water/noyo/septic`
first; those do not affect the export peak.

Cleanup after sampling: `sudo sysctl kernel.perf_event_paranoid=2`; remove
`/tmp/prof-pond`, `/tmp/pond-image`, and the host `/usr/local/share/duckpond`
if it was added.

Gotchas:
- `/usr/bin/time` is not installed on watershop.
- The host-binary vs image-binary schema mismatch (step 2) is the first trap.
- `RUST_LOG=warn` suppresses the `Peak memory usage: NN MB` line; use `info`.

Instrumentation already in the tree: `PanicOnLargeAlloc` wrapping
`peak_alloc::PeakAlloc` (`crates/cmd/src/panic_alloc.rs`, default cap
`POND_MAX_ALLOC_MB=3000`); the `Peak memory usage: NN MB` line at exit
(`crates/cmd/src/main.rs`); selfmon scrapes it into `sitegen_peak_rss.bytes`.

---

## 3. Root cause

- `crates/provider/src/export.rs:319-362` -- `export_series_to_parquet` /
  `export_table_provider_to_parquet` do `remove_dir_all(export_dir)` then
  `COPY (SELECT *) TO ... PARTITIONED BY (...)`: a full O(history) rewrite of
  ALL partitions every build. The `remove_dir_all` exists to avoid DataFusion's
  UUID-named COPY output accumulating duplicate files, so incrementalizing it
  requires deterministic per-partition file names plus reconciliation, not just
  dropping the wipe. Peak memory here is the demux holding many concurrent
  partition writers (1381 for a 1m series).
- `crates/sitegen/src/factory.rs:1031-1190` -- `run_queryable_file_export`: the
  per-series export loop. `node_path` is in scope, so the source
  `NodeMetadata.version` is cheaply available at the call site as a change key.
- `crates/provider/src/factory/temporal_reduce.rs:1127-1148` -- `merge_sql` is a
  `GROUP BY date_bin` over ALL cached finest partials, once per resolution
  (O(history/interval) x N resolutions), materializing all buckets to find the
  frontier.

### Change-key primitives (the "pond-sha")

The keys needed to skip unchanged work already exist:

- Per-series: `OplogEntry.version` (`crates/tlogfs/src/schema.rs:221`) exposed as
  `NodeMetadata.version` (`crates/tinyfs/src/metadata.rs`); bumps on every append.
- Content hash: `FileVersionInfo.blake3` (cumulative bao). The existing rollup
  and format caches already key on `(version, blake3)`
  (`crates/provider/src/rollup_cache.rs`, `crates/provider/src/format_cache.rs`)
  under `{POND}/cache/`, which persists across builds.
- Affected time range per version: `OplogEntry.min_event_time` /
  `max_event_time` (`crates/tlogfs/src/schema.rs:244`) -- only frontier
  partitions can change; historical day/month/quarter partitions are immutable.
- Whole-pond gate: `PondTxnMetadata.txn_seq` + `DeltaTable::version()`
  (`crates/tlogfs/src/persistence.rs`).

### Blocker

`config/scripts/run.sh` (site case) builds into a FRESH `build-<timestamp>`
directory each run, symlinks `current`, and keeps the last 3. Exports are never
reused across builds. Incremental export requires seeding the new build from
`current` plus a manifest of which source version each partition was built from.

---

## 4. Potential (daily incremental tick)

| | Full rebuild (today) | Daily incremental |
|---|---|---|
| Partition files written | ~7,470 (water 7,280, analysis 10, septic 36, noyo ~144) | ~30-50 |
| 1m export fan-out | 1381 writers/series | 1-2 writers/series |
| Peak live heap | ~2.1 GB | MB-scale (one day of aggregated rows) |

Per-series changed partitions on a daily tick: 1m -> current day; 10m -> current
month; 1h -> current quarter; 6h/1d -> current year (~0.5% of partitions).

Bounding the MEMORY peak needs BOTH changes, which compose: the merged-output
cache bounds the aggregation working set, and the incremental export bounds the
write fan-out. The `analysis` stage is a SEPARATE O(history) computation and may
still hold the peak after export and merge are fixed -- confirm its share first.

---

## 5. Prioritized plan

- **P0 -- Allocator tuning (cheap, independent).** `MALLOC_ARENA_MAX=2` (+128K
  trim/mmap thresholds) or jemalloc/mimalloc in the container/systemd env.
  Measured 44% RSS cut. Does not reduce true heap demand; buys headroom while
  the incremental work is built.
- **P1 -- Measure the `analysis` stage's exact peak contribution.** Re-run the
  sampling (§2) and read RSS at the metrics/analysis boundary. Decides whether
  P3+P4 alone reach the target (e.g. 1 GB) or P5 is also required.
- **P2 -- Manifest + seed-from-current (enabler, no algorithm change).** Write a
  per-partition manifest recording `(node_id, version, blake3, time-range)`; seed
  the new build directory from `current` (copy/hardlink `data/`). Touches
  `run.sh` and a small manifest reader/writer.
- **P3 -- Incremental export write** (`efficiency-dataflow.md` 6.3). Deterministic
  per-partition file names (replacing DataFusion's UUID output); skip partitions
  whose source `(version, blake3)` is unchanged; only rewrite frontier
  partitions. Biggest single win for both memory and time.
- **P4 -- Merged-output cache** (`efficiency-dataflow.md` 6.2). Cache
  per-resolution merged buckets keyed like the partial cache; only the frontier
  bucket and newly-added buckets recompute. Bounds the aggregation working set so
  the export's frontier-only path is truly O(delta).
- **P5 -- Incrementalize the `analysis` stage** if P1 shows it still holds the
  peak (pump-cycles / cycle-summary full-history recompute).

Note: the noyo subsite recomputes cold every build (~15 s/series, ~340 s of wall
time) -- a TIME cost, not a memory-peak cost. Separate optimization (a subsite
export cache).

---

## 6. The analysis stage (measured 2026-08-03)

With exports bounded, the `analysis` stage sets the peak for a warm build.
Measured on prod/staging data via `pond cat` on each series (each `pond run`
step is its own process, so `Peak memory usage` attributes per step):

| Query | Tracked heap peak |
| --- | --- |
| `COUNT(*)` over the same source | 9.16 MB |
| one `SUM(...) OVER (ORDER BY timestamp ROWS UNBOUNDED PRECEDING)` | 83.57 MB |
| `horner-by-month` | 402.05 MB |
| `drawdown-by-month` | 442.55 MB (staging) / 482.41 MB (prod) |

Reading: the source scan streams (9 MB), and the sort plus a single unbounded
window costs ~75 MB more. Neither explains 442 MB. The cost is the *shape* of
`drawdown-by-month`: a `with_meta` CTE (itself three unbounded windows) is
referenced five times -- by four filter CTEs that are then joined back
pairwise. DataFusion does not materialize CTEs, so that window pipeline is
re-evaluated per reference and each join builds a hash table over the full
event history.

### Measurement noise: read this before trusting any number here

Peak heap on the analysis stage varies **±30% run to run for a byte-identical
query**. Six interleaved repetitions of `drawdown-by-month` on unchanged
staging data gave 390, 398, 400, 401, 474 and 515 MB (median 400.6).

Consequently **single-shot A/B comparisons of this stage are meaningless**, and
several earlier entries in this document that were taken as single samples
should be read as "no measured difference" rather than as the small deltas they
report. Always interleave at least six repetitions and compare medians.

### What was tried and rejected

**Fusing the four qualifier CTEs (rejected).** `long_enough`, `clean_start`,
`real_drawdown` and `not_stale` are all
`SELECT pump_event_id FROM with_meta GROUP BY pump_event_id HAVING <pred>`,
each joined back to `with_meta` on that same key. They collapse into a single
grouped aggregate with the four predicates ANDed (using
`MIN/MAX/SUM(CASE WHEN ... END)` so no `FILTER` support is required), taking
`with_meta` from five scans to two and from four hash joins to one.

Output equivalence was verified exactly: wrapping both variants in an
order-independent integer digest (`COUNT(*)`, summed `elapsed_s`, summed `n`,
and the three percentile columns summed as `CAST(ROUND(x*1e6) AS BIGINT)`)
produced identical values on all eight columns over 5924 output rows.

Despite doing strictly less relational work, its peak heap measured **higher**:
median 467.5 MB against a 400.6 MB baseline over six interleaved repetitions.
Peak heap on this stage is evidently not proportional to the amount of
relational work, so the rewrite was not adopted.

**Bounding the windows with `PARTITION BY month` (rejected).** Measured 475 MB
against a 442 MB baseline -- but as a single sample, i.e. inside the noise band,
so the honest reading is "no improvement demonstrated". DataFusion does not
infer that `date_trunc('month', timestamp)` is monotone in `timestamp`, so it
sorts on `[month_start, timestamp]` and still materializes, now with a wider
sort key.

That rewrite is also non-trivial to make *correct*: `pump_event_id` is a running
`SUM` over all history, so partitioning it by month restarts the counter and
makes January's event 1 collide with February's event 1 in every downstream
`PARTITION BY pump_event_id`. A correct version needs a composite key such as
`epoch(month_start) * 1000000 + pump_seq`, and `WHERE pump_event_id > 0` must
become `WHERE pump_seq > 0` once the key is unconditionally positive.

### Declaring a source ordering: ruled out

It looked promising to propagate output ordering through the row-preserving
`NullPaddingExec` and `ColumnRenameExec` (both build an empty
`EquivalenceProperties::new(schema)`, discarding any child ordering) so the
windows' `ORDER BY timestamp` could stream instead of sorting.

**This is unsound for these sources.** The concatenated multi-version ingest
series are only *mostly* timestamp-sorted; there are genuinely out-of-order
sections, which is why the rollups carry a max-late-arrival setting.
`allowed_lateness` cannot be borrowed as a disorder bound either: it is a
cache-freezing policy that explicitly tolerates being violated -- "Data
arriving older than the sealed watermark is not an error: the segments covering
it are unsealed and recomputed from source" (`factory/temporal_reduce.rs`).
Declaring an ordering that does not actually hold would silently corrupt window
results.

It would also not be worth much: the sort is not the expensive part. The source
scan streams at 9 MB and adding one unbounded window costs only 84 MB, against
a ~400 MB total.

### Where this leaves the analysis stage

No query-shape change tried so far moves the peak outside the noise band, and
peak RSS for pond runs is well under the tracked-heap figure anyway (§ the
2026-08-03 update). The analysis stage sits at roughly 400-500 MB tracked heap
and is not currently the thing preventing a 1-2 GB machine from working.
Further effort here should start by establishing a *repeatable* measurement
(medians over interleaved repetitions, ideally with `target_partitions` pinned
so scheduling variance is removed) before any more rewrites are attempted.
