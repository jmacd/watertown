# Bounded-Memory Journal Status: Event-Time-Pruned Series Reads

> **Status:** Design proposal (unimplemented). Sibling to
> `temporal-reduce-bounded-memory-design.md`, which bounded the
> `temporal-reduce` read path. That fix (PR #114, released in `0.114.178`)
> resolved the memory growth for the **`temporal-reduce` consumers**
> (`site-staging`/`site-prod` dashboards, the noyo subsite). It did **not**
> touch the **selfmon dashboard itself**, whose memory growth comes from a
> *different* operator: the `status_grid` shortcode reading an append-only
> journal `FilePhysicalSeries` through the `jsonlogs://` format path, which
> **concatenates every version of all history into one in-memory batch** with
> no event-time pruning. This note proposes threading the per-version
> event-time bounds that ingest already records into the series read, so a
> status render only ever scans a **bounded hot window**, while all journal
> history is retained and remains fully queryable.

---

## 0. Problem statement

The selfmon dashboard is rebuilt every ~1 min by `run-selfmon.sh`, ending in a
single `pond run /system/etc/sitegen build`. Its `status_grid` renders one card
per systemd unit from the ingested journal; its charts come from
`sql-derived-series` + `timeseries-join`. **None of these use
`temporal-reduce`.** Yet selfmon's own "Peak memory usage" climbs across an
epoch and its live sitegen build RSS sits at **≈ 3.9–4.1 GB** (peaks of
4072–4086 MB observed under `0.114.178`, i.e. *after* the temporal-reduce fix),
reset only by wiping/reinit the instance.

The framing from the sibling design applies verbatim:

> Processing more data should cost more **time**, not more **memory**. A
> streaming scan is O(1) memory regardless of length. If memory scales with
> total history, some operator is holding O(N) state.

Here the O(N) operator is the **per-render read of the journal
`FilePhysicalSeries`**, whose version count grows once per ingest tick forever,
and which is read in full — all versions concatenated into memory — on every
render.

`docs/selfmon-design.md §1` already measured this cost (cold `COUNT(*)` over the
kernel journal: 34 s, opening all versioned Parquets; adding
`WHERE __REALTIME_TIMESTAMP >= now()-30min` did **not** help) and noted: "every
status_grid render re-pays the full cost. As the journal grows, per-render cost
grows unboundedly." This document turns that observation into a concrete fix.

---

## 1. Root cause

### 1a. `status_grid` reads through the MemTable path — whole series in memory

`run_status_grid_queries` (`crates/sitegen/src/factory.rs:592`) builds its
journal provider **without** a `ProviderContext`:

```rust
let journal_provider = provider::Provider::new(Arc::new(fs.clone())).with_root(root.clone());
```

(`crates/sitegen/src/factory.rs:649-652`). With no `ProviderContext` there is no
`cache_dir`, so `create_table_provider` skips the cached-Parquet path and falls
back to `create_memtable_from_url` (`crates/provider/src/provider_api.rs:165-181,
414-434`). That function does:

```rust
let reader = self.fs.open_url(url).await?;             // one reader over the whole logical file
let (schema, mut stream) = format_provider.open_stream(reader, url).await?;
// collect ALL batches into a MemTable
```

For a `FilePhysicalSeries`, `fs.open_url` resolves to `async_file_reader`, which
detects the series type and calls **`async_file_reader_series(&records)` —
"concatenate all versions oldest-to-newest"**
(`crates/tlogfs/src/persistence.rs:2578-2585`). So the reader streams the
concatenation of *every live version* of the unit's journal, and
`JsonLogsProvider::open_stream` materializes the whole thing into a **single
`Utf8` `RecordBatch`** (`crates/provider/src/format/jsonlogs.rs:132-145`,
`single_batch_stream`). Memory is O(total retained journal length), per unit,
per render.

> Note: the `cache/jsonlogs_*` directories present on watershop come from the
> *other* jsonlogs consumer — the `sql-derived-series` factory that feeds the
> perf charts (it has a `ProviderContext` + cache). That path incrementally
> converts each version to Parquet once, but still builds a `ListingTable` over
> **all** cached versions and scans them per query (see §3, Piece A.2). The
> `status_grid` path does not use the cache at all.

### 1b. The status/tail queries are unfiltered full-history aggregations

For each unit, `query_pond_status` (`crates/sitegen/src/factory.rs:874`) runs:

```sql
SELECT
  MAX(ts)                                                   AS last_seen,
  MAX(CASE WHEN MESSAGE LIKE '%Run summary %outcome=ok%'  THEN ts      END) AS last_ok_us,
  MAX(CASE WHEN MESSAGE LIKE '%Run summary %outcome=ok%'  THEN MESSAGE END) AS last_ok_msg,
  MAX(CASE WHEN MESSAGE LIKE '%Run summary %outcome=err%' THEN ts      END) AS last_err_us,
  MAX(CASE WHEN MESSAGE LIKE '%Run summary %outcome=err%' THEN MESSAGE END) AS last_err_msg,
  MAX(CASE WHEN MESSAGE LIKE '%Peak memory usage:%'       THEN MESSAGE END) AS last_peak_msg
FROM (SELECT CAST("__REALTIME_TIMESTAMP" AS BIGINT) AS ts, "MESSAGE" AS MESSAGE FROM <unit>) t
```

(`crates/sitegen/src/factory.rs:899-909`) plus a "tail N" query
(`:958-972`). Neither has a time bound: every render scans **every row of every
version**. Cost is O(total retained journal length), not O(hot window).

> **Latent correctness bug (independent of memory).** `MAX(ts)` and
> `MAX(MESSAGE)` are aggregated **independently**, so `last_ok_msg` /
> `last_err_msg` / `last_peak_msg` are the *lexicographically greatest* message,
> not the message belonging to the latest matching timestamp. Any fix that
> merges a windowed result with a carried-forward summary (§3 Piece B) must pair
> `(ts, message)` atomically — e.g. `arg_max(MESSAGE, ts)` — and we should fix
> the existing query the same way while here.

### 1c. Per-version event-time bounds are recorded on write but never read

`journal-ingest` writes each tick as a new `FilePhysicalSeries` version and, when
it has bounds, stamps `set_temporal_metadata(min, max, "__REALTIME_TIMESTAMP")`
(`crates/provider/src/factory/journal_ingest.rs:400-414`). These land in the
tlogfs oplog / Delta stats columns `min_event_time,max_event_time`, one row per
version (`crates/tlogfs/src/persistence.rs:500,614`), and are **already exposed
per version** on `FileVersionInfo.extended_metadata` for series files
(`crates/tlogfs/src/persistence.rs:3966-3986`). Nothing on the read side
consults them: neither `async_file_reader_series` (which concatenates
unconditionally) nor the status SQL (which has no predicate). The distinct
`get_temporal_bounds` accessor (`crates/tlogfs/src/persistence.rs:1650`) returns
the file-wide `min_override/max_override` used for data-quality filtering, **not**
per-version data-derived event bounds.

**Net:** the status render holds O(total history) in memory because the series
read concatenates all versions and the query scans them all, even though the
per-version bounds needed to skip old versions are already on disk.

---

## 2. What the status card actually needs

Per unit, a render needs (see `crates/sitegen/src/shortcodes.rs:881-923`):

- **`last_seen`** — `MAX(ts)`.
- **`last_peak_msg`** / peak RSS — the most recent `Peak memory usage:` line.
- **tail N** — the newest `tail_lines` messages.
- **`last_ok`, `last_err`** — the last ok/err `Run summary` `(ts, message)`.

Any of these can be arbitrarily old for a unit that has gone silent (ingest does
**not** enforce event-time monotonicity across versions, so we cannot assume "the
newest version holds the max"). Therefore a purely time-windowed scan would blank
out cards for quiet units. The correctness-preserving design keeps a **bounded,
durable, per-unit summary** of exactly these fields, updated incrementally as new
versions are ingested, and renders from `merge(window, summary)`. The *scan* is
bounded to a hot window; the *summary* is O(#units), never O(history).

---

## 3. Proposed architecture

Two composable pieces. Piece A bounds the series read generically (benefits every
`jsonlogs`/series consumer). Piece B is the status-grid summary that preserves
correctness for fields that predate the window.

### A. Event-time lower bound in the series read

Give the series read an optional lower bound `lo` (epoch µs) and skip versions
provably below it, using the per-version bounds already on `FileVersionInfo`.

1. **Prune the concatenation (the status_grid path).** Add an optional
   `event_time_lower_bound` to the series reader so `async_file_reader_series`
   (`crates/tlogfs/src/persistence.rs:2578`) concatenates only versions with
   `max_event_time >= lo`. **Versions whose `max_event_time` is absent are always
   retained** (bounds are written conditionally, `journal_ingest.rs:407`; a
   missing bound must never drop data). This alone bounds the MemTable read that
   dominates selfmon.

2. **Prune the cached `ListingTable` (the perf-chart path).** For the
   `ProviderContext` + cache consumer, include only cached version-Parquets whose
   version `max_event_time >= lo` when constructing the `ListingTable`
   (`crates/provider/src/table_creation.rs` / `ensure_url_cached`,
   `crates/provider/src/provider_api.rs:459`). Note the existing
   `table_creation.rs:134-160` "multi-file temporal bounds" TODO refers to the
   builtin-Parquet `FileID` path, not this jsonlogs path; both want the same
   per-version predicate.

3. **Intra-version pushdown (optional).** Because `__REALTIME_TIMESTAMP` is
   `Utf8`, DataFusion cannot row-group-prune the straddling boundary version.
   Materialize a `BIGINT` event-time column when writing cache Parquet (the cache
   schema is ours to shape) **and** have the reader apply a predicate on that
   column, so the one boundary version is row-group pruned rather than fully
   scanned. Without an actual predicate the column does nothing, so this step is
   reader-predicate + column together or not at all.

4. **Plumb the bound from the caller.** Thread `lo` from `status_grid`
   (`now - hot_window`, §4) down to the series read. For the no-context MemTable
   path this needs either a new `Provider` read method that carries the bound or a
   URL convention (e.g. a query parameter) understood by `open_url`. Callers that
   pass no bound keep today's full-history behavior (back-compatible).

### B. Status-grid durable per-unit summary (version-watermark maintained)

`status_grid` maintains a small sidecar — one row per unit — holding
`last_seen`, `last_peak (ts,msg)`, `last_ok (ts,msg)`, `last_err (ts,msg)`, and
the bounded `tail_lines`. It is maintained by a **version watermark**, not by
repeated wall-clock rescans:

- Persist, per unit, the highest journal **version** already folded into the
  summary (the "processed watermark").
- **First run / backfill:** with no watermark, do a single cold full read to seed
  the summary (a one-time O(N) build, exactly like the temporal-reduce migration).
- **Each tick:** read only versions **newer than the watermark** (bounded — one or
  a few per tick), fold their ok/err/peak/last_seen/tail via `arg_max(msg, ts)`
  pairing, and advance the watermark. Every version is processed **exactly once**,
  so no event is missed even across prolonged sitegen downtime or late-arriving
  data (which appears as a new version above the watermark).
- **Render** from the summary directly (it already reflects all history);
  optionally union the newest hot-window versions for immediacy. Because the
  summary carries `last_seen`/peak/tail too, cards for units silent longer than
  any window still render correctly.

The summary is bounded (O(#units × tail_lines)) and lives in-pond (e.g.
`/derived/status-summary`) so it is covered by the same maintain/prune lifecycle
and survives across builds.

### Result

Per-render scan is bounded to versions above the watermark (Piece B) and/or above
`now - hot_window` (Piece A); the summary is O(#units); **all journal history is
retained on disk and fully queryable** by explicit time-range queries — only the
default status render is bounded. Memory becomes flat across an epoch.

---

## 4. Configuration

- **`status_grid.hot_window`** (humantime; default
  `DEFAULT_STATUS_HOT_WINDOW = 1d`). Lookback for any immediacy union in Piece B
  and the lower bound handed to Piece A. With a durable version watermark the
  summary is authoritative regardless of this value, so shrinking it never blanks
  a card; it only trades scan width for freshness of the union.
- Piece A's pruning is transparent to existing configs: with no lower bound,
  behavior is unchanged (full history), so other `series://` / `jsonlogs://`
  consumers are unaffected until they opt in.

This mirrors the temporal-reduce decision to make the window a per-factory config
defaulting to 1 day, consistent with the project's prefer-hard-failures,
no-silent-drop stance: history is never dropped, only the default *scan* is
bounded; a missing per-version bound retains (never drops) the version.

---

## 5. Compatibility & migration

- **All journal history is retained and queryable throughout** — this bounds
  *memory*, not *retention*. An explicit `WHERE ts BETWEEN …` query still reads
  arbitrarily far back (paying O(range), never O(total)).
- **Series read change is additive.** The new lower bound defaults to "none"
  (full read); only opt-in callers prune.
- **Cache column (A.3) is additive.** New cache Parquets gain a `BIGINT`
  event-time column; existing `cache/jsonlogs_*` dirs are a derived, rebuildable
  artifact (safe to wipe; they re-materialize incrementally from the source
  series).
- **Summary bootstrap.** First run after deploy does one cold full read to seed
  the per-unit summary, then bounded forever (the temporal-reduce migration
  shape).
- **No on-disk change** to the raw object model, the journal series delta layout,
  ingest, or `set_temporal_metadata`. We only *read* bounds ingest already writes.

---

## 6. Risks & validation

- **No missed events.** Golden test: with the version-watermark maintainer, a
  unit whose last `outcome=ok` (or `err`, or `Peak memory`) is older than any
  window still renders the correct `(ts, message)` from the summary; a unit that
  errored once long ago and went quiet keeps its red card. Include a late-arriving
  version (older event time, newer version number) and assert it is folded exactly
  once.
- **Pair integrity.** Assert `last_ok_msg`/`last_err_msg`/`last_peak_msg` belong
  to the row with the matching max timestamp (`arg_max`), fixing the current
  independent-`MAX` bug (§1b).
- **Windowed == full for recent fields.** On a fixture where all activity is
  within the window, assert `last_seen`, peak, and tail-N are byte-identical
  between windowed render and full-history render.
- **Pruning correctness.** A version with `max_event_time < lo` is never opened
  (assert via a read counter); a straddling version is opened and row-group-pruned
  (A.3); a version with **NULL** bounds is always retained.
- **Memory regression guard.** Extend the selfmon stressor: assert daily-max peak
  memory **plateaus** across an epoch (flat, not monotonic) on an ever-growing
  synthetic journal, while disk keeps growing (history retained). This is the
  acceptance criterion — the same guard proposed for temporal-reduce.
- **`POND_MEMORY_LIMIT_MB` interaction** (FairSpillPool): after the fix the status
  render should fit far under the 2048 MB selfmon pool regardless of journal
  length; verify the pool is no longer the binding constraint.

---

## 7. Implementation phases

| Phase | Scope | Status |
|---|---|---|
| 1 | Optional `event_time_lower_bound` on the series reader; prune `async_file_reader_series` version concatenation by per-version `max_event_time` (retain NULL); plumb the bound from `status_grid` | **Done** (commit `9ce877ff`) |
| 2 | Durable per-unit version watermark + summary (Piece B): backfill once, fold each unseen version exactly once via `arg_max(msg, ts)`; render from the summary; fix the independent-`MAX` pairing bug | **Done** (commits `cbe21da1`, `ac0b3d4d`) |
| 3 | Cached-`ListingTable` pruning (A.2): the `SeriesReadBounds` now also prune the cached path — `create_cached_table_from_url` lists only the version Parquets whose per-version `max_event_time`/version pass the bounds (empty → 0-row table, never a full scan). The status-grid perf enrichment passes its existing `hot_window` through this path and combines all latest liveness fields into one provider scan. A.3 (materialized `BIGINT` event-time column + intra-version row-group predicate) remains optional. | **Done (A.2 + status-grid perf caller)** — A.3 deferred (optional) |
| 4 | Selfmon memory-plateau stressor test + docs; retire the `docs/selfmon-design.md §1` open item. Added `series_bounded_read_memory_plateaus_as_history_grows` (tlogfs): as an append-only `FilePhysicalSeries` grows 4→8→16 versions, a fixed-width event-time-bounded read stays byte-for-byte flat while the unbounded read grows monotonically. Also exposed per-version `max_event_time` on `FileVersionInfo` for `FilePhysicalSeries` (not just Table series), so the A.2 cache prune is effective for real jsonlogs journals, not just the read-path filter. | **Done** |
| 5 | Pre-filter status discovery by `unit_globs` before loading TinyFS children. The live broad `*.jsonl` traversal loaded 1,461 nodes in 359.851s/9.283 GB; two narrowed pond-unit globs loaded ten nodes in 8.429s/285.65 MB. Noncanonical layouts retain generic traversal. | **Done** |

### Notes / open questions

- **Carrying the bound to the no-context path.** The status_grid provider has no
  `ProviderContext`; decide between (a) a new `Provider` read method that takes an
  event-time lower bound, or (b) a URL query-parameter convention consumed by
  `open_url`/`async_file_reader_series`. (a) is more explicit and type-safe.
- **Where the summary lives.** Prefer in-pond (`/derived/status-summary`) so the
  maintain/prune lifecycle covers it and it survives across builds; the sitegen
  state dir is the fallback.
- **Straddle predicate.** Version pruning is `max_event_time >= lo` (inclusive) so
  the boundary version is retained; A.3's row-group predicate then trims within it.
- **Monotonicity.** Do **not** assume event-time monotonicity across versions;
  the watermark is on *version number*, and folds use `arg_max` on event time.
- **Generalization.** Piece A is deliberately generic; once landed it also bounds
  the `sql-derived-series` jsonlogs scans feeding the perf charts.
