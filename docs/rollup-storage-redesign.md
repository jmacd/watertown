# Rollup cache storage redesign: fewer files, correctly

Status: proposed. Supersedes the storage decisions in
`docs/archive/incremental-rollup-implementation.md` (PR #87), whose stated
justification no longer holds.

## 1. Summary

The temporal-reduce rollup cache stores its state as a large number of very
small Parquet files. Three separate populations grow without bound, on three
different axes, and every one of the silent-double-counting bugs fixed in #122
was a consequence of how those files are *keyed* rather than of the aggregation
logic itself.

This document proposes replacing the two-layer (per-version partials → sealed
runs) design with a single layer: **immutable segments keyed by the output time
range they cover, plus one open hot segment, compacted by the same size-tiered
policy already used for physical-series collapse in tlogfs.**

The per-version partials layer is deleted outright. Partial *aggregate state*
(`__p_sum`, `__p_count`, …) is retained unchanged — that part is correct and
conventional — but it becomes a column layout inside segments rather than a file
naming scheme.

Net effect: file count per resolution goes from *one per build, forever* to a
number bounded by `O(FANOUT · log_FANOUT(total_bytes / target_bytes))` — a few
dozen — and the cache stops being able to observe version collapse at all.

## 2. What exists today

### 2.1 Layers

```
{cache_dir}/
  {scheme}_{node_id}/                       (A) format cache
      v{version}-{blake3}.parquet                one file per source version
  rollup_{cfg_hash}_{site_node}/
      partials/                             (B) rollup partials
          {node_id}_{version}_{blake3}.parquet   one file per source version
      merged/res{secs}/                     (C) sealed runs
          run-00000000.parquet                   one file per *build*
          run-00000001.parquet
          hot.parquet
          manifest.json
```

- **(A) format cache** — `crates/provider/src/format_cache.rs`. The decoded
  Parquet of each source version.
- **(B) partials** — `crates/provider/src/factory/temporal_reduce.rs`,
  `write_version_partial` (line 1301). Each source version pre-aggregated to the
  *finest* interval, in mergeable partial columns.
- **(C) sealed runs** — `crates/provider/src/rollup_cache.rs`, `SealedRun` /
  `SealedManifest` (lines 287, 307). Output buckets below a watermark frozen
  into immutable files; buckets above it recomputed into `hot.parquet` each
  build.

### 2.2 How a build proceeds

`try_rollup_table_provider` (temporal_reduce.rs:431):

1. For each source node, reconcile (B) against live versions, write a partial
   for each live version missing one (line 530–605).
2. Register all live partials as one table (line 611-620).
3. Finest resolution: `build_level_from_partials` (line 1026) compares
   `manifest.covered` — *a set of partial filenames* — against the current
   directory listing to choose `Reuse` / `Incremental` / `Rebuild`.
4. `seal_and_recompute` (line 951) seals `[sealed_hi, watermark)` into a new run
   and rewrites `hot.parquet`.
5. Coarser resolutions: `build_level_from_finer` (line 1172) folds the
   next-finer level's *segments*, keyed on `source_digest`.

Note step 5. The coarse levels already do the right thing.

## 3. What is wrong

### 3.1 Three unbounded populations, three axes

| | grows per | bounded today? | garbage collected? |
|---|---|---|---|
| (A) format cache | source version ever written | **no** | **no** |
| (B) partials | live source version | yes, since #122 | yes (`reconcile`) |
| (C) sealed runs | **build** (wall-clock) | **no** | **no** |

Specifics:

**(A) is never collected.** `SidecarDir::reconcile` exists and works, but
`grep -rn '\.reconcile('` shows exactly one non-test caller:
temporal_reduce.rs:530, for partials. `format_cache` only ever calls
`cached_set` (format_cache.rs:134). That call made reads *safe* — the scan names
live files explicitly instead of listing the directory — but nothing deletes the
superseded ones. A previously measured deployment held 21,659 files / 96 MB at
~4.4 KB average. Every version collapse adds a merged file and orphans ten.

**(C) grows with wall-clock time, not data.** `m.runs.push(...)`
(temporal_reduce.rs:988) is the only writer to `SealedManifest::runs`;
`listing_table_for_res_dir` (rollup_cache.rs:455) is the only reader. There is
no compaction anywhere in the crate. `seal_and_recompute` appends a run whenever
the watermark advances, which is every build that sees new data. Under hourly
builds that is ~8,760 files per resolution per year, each holding one hour of
buckets, and with a nesting chain of resolutions it is that figure times the
number of levels.

**(B) is now bounded but still mis-keyed** — see below.

### 3.2 The two mistakes are independent

It is worth separating them, because they have different fixes and different
blast radii.

**M1 — cache entries are keyed by the storage artifact that produced them, not
by the content they summarize.**

A partial is named `{node_id}_{version}_{blake3}`. But what it *contains* is "the
aggregate of finest-interval buckets in some time range." Version identity is an
implementation detail of how the source happens to be stored, and it is not
stable: tlogfs collapse rewrites ten versions into one merged version with
identical content and a *new, higher* version number.

Every consequence of M1 shows up as a guard bolted on afterwards:

- the sequentiality frontier (`read_frontier` / `write_frontier`,
  rollup_cache.rs:170–232) and the disjointness argument at
  temporal_reduce.rs:504–520;
- the `disjoint_versions` split between series and non-series sources;
- the `--rebuild` hard error when a version backfills a sealed bucket
  (temporal_reduce.rs:1100);
- `reconcile` itself, added in #122 because collapse leftovers were being summed
  twice (testsuite 733);
- the requirement that reads name files explicitly rather than list a directory.

None of those guards would have anything to guard if the key were the covered
time range. Two builds that summarize the same time range from the same content
would produce the same key whether or not a collapse happened in between.

**M2 — sealing is triggered by the watermark advancing, not by having enough
data to justify a file.**

`seal_and_recompute` seals on `wm > m.sealed_hi_secs`. That is a *time* trigger.
The correct trigger for "should this become a file" is *size*. This is entirely
independent of M1: even with a perfect key, sealing once per build produces one
file per build.

M2 is currently doing more damage than M1, and is much cheaper to fix.

## 4. What conventional systems do

The concept of partial aggregate state is universal and must be kept — you
cannot merge `avg`, so you store `sum` and `count` and divide at read
(`reconstruct_sql`, temporal_reduce.rs:2117). ClickHouse `AggregateFunction`
state columns, TimescaleDB continuous aggregates, Druid metric rollup and Flink
accumulators all do exactly this.

What none of them do is key that state by *which input artifact produced it*.
They key by the time bucket the state summarizes, and they solve the two
resulting problems as follows:

| problem | conventional answer | here today |
|---|---|---|
| small files accumulate | background compaction merges adjacent segments | *nothing* |
| which range changed? | invalidation log / dirty ranges | diff the *filename set* (`manifest.covered`) |
| late data | recompute the affected range | hard error, `--rebuild` |

The important observation is that the conventional shape is **already in this
codebase**: `build_level_from_finer` (temporal_reduce.rs:1172) folds the finer
level's immutable range-keyed segments into the coarser level, keyed on
`source_digest`. It is the finest level, and only the finest level, that reaches
sideways into per-version files.

So this is not a proposal to invent something. It is a proposal to make the
finest level use the pattern the other levels already use, and to add the one
piece nobody wrote: compaction.

## 5. Proposed design

### 5.1 Principle

> One structure, at every level: a sequence of immutable segments keyed by the
> half-open output-bucket range `[lo, hi)` they cover, plus one open hot
> segment. Segments are merged by a size-tiered policy. Nothing anywhere is
> keyed by a source version.

### 5.2 On-disk layout

```
{cache_dir}/rollup_{cfg_hash}_{site_node}/merged/res{secs}/
    seg-00000000.parquet     immutable, covers [lo, hi)
    seg-00000007.parquet     immutable, covers [hi, hi')
    hot.parquet              open buckets >= sealed_hi, recomputed each build
    manifest.json
```

The `partials/` directory and the per-node `frontier` files are deleted. The
column layout of a segment is exactly today's `partials-v2`: `time_bucket` plus
the mergeable partial columns. `reconstruct_sql` at read time is unchanged.

### 5.3 Manifest

```rust
pub struct SegmentManifest {
    pub format: String,                 // bump to "segments-v3"
    pub allowed_lateness_secs: i64,
    pub sealed_hi_secs: Option<i64>,
    pub next_seq: u64,
    pub segments: Vec<Segment>,         // ascending, disjoint, contiguous
    pub hot_digest: Option<String>,

    /// Finest level only: blake3 of each LIVE source version this cache
    /// reflects, with its event-time range. Replaces `covered`.
    pub sources: BTreeMap<String, SourceVersion>,   // blake3 -> range

    /// Coarser levels only: unchanged.
    pub source_digest: Option<String>,
}

pub struct Segment {
    pub name: String,
    pub lo_secs: Option<i64>,   // None = genesis, unbounded below
    pub hi_secs: i64,           // exclusive
    pub rows: u64,
    pub bytes: u64,             // drives the size-tiered policy
    pub digest: String,
}

pub struct SourceVersion {
    pub min_event_time: i64,
    pub max_event_time: i64,
}
```

`covered: BTreeSet<String>` (a set of *filenames*) is replaced by
`sources: BTreeMap<blake3, range>` (a set of *content identities* with the time
range each covers). This is the single most important change in the document.
Note the size argument: the number of live versions is bounded by tlogfs
collapse (`max_live`), so this map is bounded, whereas a directory of one file
per version ever written was not.

### 5.4 Build algorithm (finest level)

Replaces `build_level_from_partials`.

```
1. Read manifest. Wipe if format or allowed_lateness mismatch.

2. live := live versions of every source node (already computed at
   temporal_reduce.rs:525 via LiveVersions::from_persistence).
   now := { v.blake3 -> (v.min_event_time, v.max_event_time) }

   For series sources these come free: min_event_time / max_event_time are
   already surfaced on FileVersionInfo.extended_metadata
   (tlogfs/src/persistence.rs:4601-4615). No scan.

   For non-series sources, fall back to the existing version_bucket_span scan
   (temporal_reduce.rs:776) for versions not already in `sources`.

3. Plan:
     now == m.sources                     -> Reuse
     otherwise dirty := union of the event-time ranges of
                        (now \ m.sources)  keyed by blake3
              plus, for any digest in (m.sources \ now) that is NOT explained
              by a collapse-equivalent replacement, its range too.
              -> Incremental { dirty }
     no compatible manifest               -> Rebuild

4. Incremental:
     - dirty_lo := floor(min(dirty) to output interval)
     - if dirty_lo < sealed_hi: the dirty range reopens sealed segments.
       Recompute those segments in place (§5.6) rather than erroring.
     - advance watermark, seal, recompute hot  (as today)

5. Compact (§5.5).

6. Write manifest atomically, then delete files no longer referenced.
```

Step 2 is where partials die. The finest level aggregates **directly from the
source**, not from a partial cache:

```rust
// today
let sql = pieces.merge_partials_sql(interval, ts, partials_table, "time_bucket", lo, hi);

// proposed
let src = format_cache::listing_table_from_cache_bounded(
    cache_dir, scheme, node_id, &live_versions,
    &SeriesReadBounds::from_event_time_lo(dirty_lo_micros),   // prunes files
    ctx,
)?;
let sql = pieces.partial_sql_ranged(interval, ts, src_table, &available, dirty_lo, dirty_hi);
```

This is the crux of why partials are unnecessary. `listing_table_from_cache_bounded`
(format_cache.rs:124) *already* prunes to the version files whose recorded
`max_event_time` reaches the requested lower bound. The per-version partial cache
was buying "don't rescan old versions"; the bounded listing table buys the same
thing, from data already on `OplogEntry`, without a second file population and
without a version-shaped cache key.

Two details to get right, both of which are latent hazards:

- **`SeriesReadBounds` today has only a lower bound** (`event_time_lo`,
  tinyfs/src/file.rs:25-34); there is no upper bound. That is *safe* — the
  aggregation's `WHERE` clause bounds the buckets, so an over-broad file set
  costs I/O rather than correctness — but it means an incremental build reads
  every version at or after `dirty_lo`. For append-only ingest that is the tail
  and is fine. If a use case makes it expensive, add `event_time_hi` rather than
  working around it.
- **Units differ.** `event_time_lo` is epoch *microseconds*
  (tinyfs/src/file.rs:26); segment ranges and `sealed_hi_secs` are epoch
  *seconds*. Conversion must be explicit and rounded *outward* (floor the lower
  bound) so pruning can never drop a contributing version. Note that
  `retains` already treats a missing bound as "retain", for the same reason.

The extra cost is re-aggregating the raw rows of versions that overlap the dirty
range, rather than reading their pre-aggregated partials. Under append-only
ingest the dirty range is the tail, so that is one or two versions — precisely
the versions the old design would have had to write partials for anyway.

### 5.5 Compaction

New. Deliberately reuses the policy already proven in
`tlogfs::persistence::choose_collapse_window` (persistence.rs:262), including
`size_class` (line 203) and `COLLAPSE_FANOUT = 10` (line 197).

```
loop {
    let window = choose_merge_window(&m.segments, MAX_SEGMENTS)?;   // same rule
    merge segments[window] into one new seg-{next_seq}.parquet
        via merge_partials_sql over just those files,
        covering [segments[window.start].lo, segments[window.end-1].hi)
    replace them in m.segments
}
```

Two properties carry over from the tlogfs policy and both matter:

- **Merge only same-size-class neighbours.** This is what stops a large
  accumulated segment from being rewritten to absorb a few kilobytes of new
  buckets. Without it compaction is `O(N²)` in bytes written.
- **The ragged-input backstop picks the *cheapest* window, not the oldest.**
  Anchoring at index 0 defeats tiering, because after any previous merge the
  oldest segment *is* the large one. This is documented at persistence.rs:249-254
  and was itself a bug fixed on the #122 branch; it should not be rediscovered
  here.

Using literally the same function is preferable to writing a second one. If it
cannot be shared directly (it takes `&[&OplogEntry]`), it should be refactored to
take `&[u64]` of sizes and be called from both places. **Two tiering policies
that are meant to agree but are written twice is exactly the duplication that
produced this branch's bugs** — `docs/archive/incremental-rollup-implementation.md`
§5 instructed "mirror `format_cache.rs` exactly, in a new sibling module", and the
two copies then diverged.

`MAX_SEGMENTS` bounds read fan-out; a target segment size (say 8 MB) bounds the
small end. Both are settings, not constants, and belong with the other rollup
settings.

### 5.6 Late data stops being an error

Today, data older than `sealed_hi` is a hard error recovered only by
`--rebuild` (temporal_reduce.rs:1100, and again at line 559 for the frontier).
That error exists because a sealed run is keyed by *nothing that would let you
find it again* — you know the range, but merging into it would have had to
reconcile against per-version partials that may have already been folded.

With range-keyed segments, a late arrival is ordinary work: find the segments
overlapping `[dirty_lo, dirty_hi)` — a range lookup in `m.segments` — recompute
exactly those from source, and swap them in. It is the same operation as
compaction, with a different window selection rule.

This should be capped (e.g. refuse to recompute more than N segments in one
build, then fall back to a full rebuild with a clear message) so a pathological
backfill does not silently rewrite all history.

### 5.7 Read path

`listing_table_for_res_dir` (rollup_cache.rs:455) currently verifies that every
manifest run exists, and then builds a `ListingTable` over the **whole
directory**. Those two are not the same set. Any file in the directory that the
manifest does not reference — an orphan from a crash between writing a segment
and updating the manifest, or from a compaction that was interrupted before its
inputs were deleted — is silently included in the scan and double-counted.

This is the identical failure mode as the format-cache directory listing that
#122 fixed, and compaction makes it far more reachable, because compaction's
normal operation *creates* a window in which both the inputs and the output
exist on disk.

**The reader must name files explicitly from the manifest**, exactly as
`CachedSet` does. `ListingTableConfig` accepts a list of file URLs; this is a
small change and it should land whether or not the rest of this design does.

Ordering guarantees are unaffected: segments remain individually sorted and
mutually disjoint, so `split_file_groups_by_statistics` and the streaming
`SortPreservingMergeExec` read path (temporal_reduce.rs:625-640) work as before.

## 6. Correctness argument

**Coverage.** `m.segments` is contiguous and disjoint from genesis to
`sealed_hi`, and `hot` covers `[sealed_hi, ∞)`. Compaction replaces a contiguous
sub-sequence with one segment covering the union of its ranges, which preserves
both properties. Recompute-in-place preserves them likewise.

**No double counting, structurally.** A bucket appears in exactly one segment
or in hot, because the ranges partition the axis. There is no path by which a
stale file participates, because the reader enumerates the manifest and the
manifest holds one entry per range. Compare with today, where disjointness is a
*precondition* on how the input versions happen to be laid out, asserted by the
`disjoint_versions` flag and defended by the frontier.

**Collapse invisibility.** A tlogfs collapse replaces versions `v1..v10` with a
merged version whose content is their concatenation. The set of live blake3s
changes, so `now != m.sources` and the plan is `Incremental` with the dirty
range = the collapsed versions' event-time span. Recomputing that span from the
merged version yields identical partial sums, because the row multiset is
identical. The result is byte-identical segments. This is the property the
current design cannot have, since its cache key changed.

An optimization worth having: if the merged version's event-time range is
exactly the union of the ranges of the versions it replaced, and the union of
row counts matches, the rebuild is provably a no-op and can be skipped by
rewriting `m.sources` in place. That turns "collapse forces a rebuild" into
"collapse forces a manifest edit."

**Idempotence.** Building twice with no source change is `Reuse` (`now ==
m.sources`), which touches nothing.

## 7. Resulting file counts

Per resolution, with `FANOUT = 10` and an 8 MB target:

| total rollup output | segments | today (hourly builds) |
|---|---|---|
| 80 MB | ~10 | 8,760 / year |
| 800 MB | ~20 | 8,760 / year |
| 8 GB | ~30 | 8,760 / year |

Plus `hot.parquet` and `manifest.json`. The count is logarithmic in data volume
and **independent of build frequency**, which is the actual defect being fixed.

The format cache (population A) is a separate problem with the same shape; §10
sequences it separately.

## 8. What this deletes

- `write_version_partial` (temporal_reduce.rs:1301)
- `partials_dir`, `partials_dir_at`, `list_glob_members` (rollup_cache.rs:100-122,
  248)
- `read_frontier` / `write_frontier` / `frontier_path` (rollup_cache.rs:170-232)
  and the whole sequentiality-frontier concept
- `version_bucket_span` for series sources (retained for non-series)
- the `disjoint_versions` branch (temporal_reduce.rs:519)
- `SealedManifest::covered` and the filename-set diff (temporal_reduce.rs:1051-1080)
- both `--rebuild` hard errors for backfill (temporal_reduce.rs:551-566, 1100-1114)
- `reconcile`'s use for partials — the `SidecarDir` abstraction stays, for the
  format cache

That is roughly 400 lines of logic and, more to the point, every guard whose
existence was owed to M1.

## 9. Migration

Free. These caches are derived data and disposable. Bumping `SEALED_FORMAT`
(rollup_cache.rs:301) from `"partials-v2"` to `"segments-v3"` already causes
`build_level_from_partials` to wipe and rebuild the res dir (line 1044). The old
`partials/` directory should be removed once on first sight of a v3 manifest.

No user-visible behaviour changes: `reconstruct_sql` output is column-for-column
identical, and the export hint / digest contract is unchanged.

## 10. Sequencing

Ordered so that each step is independently valuable and independently
revertible. All landed; P4 was done before P3, being independent of it.

| | | |
|---|---|---|
| P0 | read by manifest | `61c08f53` |
| P1 | seal on size | `bf9b0b1d` |
| P2 | compaction | `d97ee7c4` |
| P3 | re-key on content, delete partials | `03b633df`, renamed in `2cfd8662` |
| P4 | reconcile the format cache | `64880b47` |
| P5 | retire the stale design doc | `51540fa4` |
| P6 | review fixes (§13) | this commit |

**P0 — read by manifest, not by directory listing.** (§5.7) Small, strictly a
bug fix, no format change. Should land regardless.

**P1 — seal on size, not per build.** (M2) Accumulate frozen buckets in `hot`
until they exceed the target size before writing a segment. Confined to
`seal_and_recompute`; no key change, no format change beyond adding `bytes`.
This alone removes the one-file-per-build growth, which is the worst of the
three populations.

**P2 — compaction.** (§5.5) Needs P1's `bytes`. Bounds the count permanently and
lets P1's target be small.

**P3 — re-key on content, delete partials.** (M1, §5.3–5.4) The large change:
`sources` replaces `covered`, the finest level folds from the bounded source
listing table, and the guards in §8 are deleted. Everything in §6 depends on
this step.

**P4 — reconcile the format cache.** Population A. Independent of P0–P3; a
single `reconcile` call on the path that already computes live versions. Worth
confirming first that nothing depends on a superseded version file remaining
readable.

**P5 — retire the stale design doc.** `docs/archive/incremental-rollup-implementation.md`
still asserts "no watermark, no mutable state file" (void since Phase 2 added
both) and the sequential-input invariant (void since P3). It is archived
history, so it now carries a header pointing here rather than being edited to
say something it never said.

## 11. Testing

The recurring failure on the #122 branch was tests that ran the buggy scenario
but asserted something *invariant under the bug* — testsuite 050 asserted `avg`,
which is `SUM(sum)/SUM(count)` and therefore unchanged when both are doubled.
Tests here must assert quantities that move:

- **Assert `count` and `sum`, never only `avg`.**
- **Collapse-invariance:** build a rollup; collapse the source series; rebuild;
  assert the segment digests are byte-identical. This test is the entire point
  of P3 and fails loudly today.
- **Compaction shape:** after N builds, assert `segments.len()` is bounded and
  that size classes are actually tiered — mirroring
  `assert_tiered_beyond_a_watermark` in `steward/src/reclaim.rs`, which exists
  because a test silently became toothless when everything collapsed into one
  tier.
- **Orphan rejection:** drop an unreferenced Parquet into a res dir, assert the
  read result is unchanged. Fails today.
- **Late data:** append data older than `sealed_hi`, assert the correct totals
  rather than an error.
- **Crash windows:** kill between segment write and manifest write; between
  manifest write and input deletion. Assert reads are correct in both.
- **Equivalence:** full single-pass `GROUP BY` over all source rows equals the
  segmented result, as `test_partial_then_merge_equals_full_single_pass`
  (temporal_reduce.rs:2718) already does for partials.

## 12. Open questions

1. **Target segment size and `MAX_SEGMENTS`.** 8 MB and ~50 are guesses. They
   should be measured against the real water/septic series, not chosen here.
2. **Should compaction be synchronous with the build?** Doing it inline is
   simplest and the amortized cost is bounded, but it makes a single export
   occasionally slow. The alternative is a `pond maintenance` step, which is
   where `reclaim` already lives.
3. **Non-series sources.** `FilePhysicalVersion` re-snapshots overlap, so its
   versions are not disjoint and it has no `min/max_event_time`. The proposal
   keeps `version_bucket_span` for it. It may be cleaner to declare rollup
   unsupported for overlapping sources rather than carry the branch.
4. **Cost of the collapse-triggered recompute.** §6 sketches a no-op
   optimization; without it, a collapse recomputes the collapsed span from
   source. That is bounded and correct, but its real cost on water/septic is
   unmeasured — note that selfmon uses `series:///` and bypasses the rollup
   cache entirely, so this has never been observed in production.
5. **Whether `hot.parquet` should itself be several files** if
   `allowed_lateness` is large. Probably not; if it is large enough to matter,
   P1's size trigger already splits it.

## 13. What the post-landing review found

A read of the whole branch after P5 turned up four defects, all of them
consequences of the same thing: P3 moved decisions from "which files exist" to
"what the manifest records", and three places kept reasoning about the old
question.

**A watermark must be bucket-aligned.** `dirty_lo` was floored to the second,
not to the output interval, and then used as the new watermark. Everything else
treats `sealed_hi_secs` as a bucket boundary -- the read filters on a bucket
START -- so an unaligned watermark leaves the bucket containing the dirty point
in neither a segment (they stop at the aligned edge below it) nor the hot window
(it begins at or above the watermark). That bucket, including the backfilled
rows that made it dirty, is dropped, and the next build reuses. Fixed by
`floor_secs_to_bucket`, applied at both levels.

**Unsealing must propagate up the cascade.** A coarser level keyed its freshness
on the finer level's digest plus whether it was *rebuilt*. Late data is not a
rebuild -- it is the ordinary incremental path -- so the coarse level kept its
own sealed segments and reported pre-backfill values forever. The finest level's
dirty point is now passed up the chain, and each level unseals from it, aligned
to its own interval.

This surfaced a latent ambiguity: `changed_since: Option<i64>` carried three
meanings on the same `None` -- "nothing changed", "rebuilt", and "the dirty
range is unbounded". That is harmless for the export hint, which treats every
`None` as "rewrite all", but the coarse level must unseal *everything* in the
last two cases and *nothing* in the first. It is now the explicit
`LevelChange::{Nothing, Since(secs), Everything}`.

**Content-keying removed a self-correction.** `cached_set` silently skipped a
live version whose sidecar was absent. Under the old design the key was the set
of partial *filenames*, so a version with no file simply looked uncovered and
was rebuilt; keying on content instead means the build records that version as
covered while having read none of its rows -- an undercount that agrees with
itself forever. `CachedSet` now reports those versions and the rollup fails the
build rather than publishing coverage it does not have. The window is real
because P4's `reconcile` deletes sidecars on the read path, which takes no lock.

**`hot.parquet` is the one file mutated in place.** It is published before the
manifest that records the watermark it was built for, so a crash between them
leaves buckets in no member of the cache -- served indefinitely, because an
unchanged source makes the next build reuse. The recorded `hot_digest` was never
checked despite §5.7 claiming it was; it is now verified per build, and a
disagreement rebuilds the resolution.

The test lesson from §11 recurred a third time: `lateness_config` declared only
`Max`, and `max(x, x) == x` is invariant under double-counting exactly as `avg`
is. The seal and compaction tests were asserting a statistic that cannot move.
They now carry `Sum`.
