# Selfmon findings — storage amplification and coverage gaps

**Status: working notes, committed for traceability rather than as
documentation.** Investigated 2026-07-25/26 against the live
`watershop-selfmon` instance. Every number below is measured on the host, not
estimated.

Updated 2026-07-26: items 1-3 implemented (§8, §9, item 3); two findings not in
the original audit added (§10, §11).

Updated 2026-07-27 after a review of the whole branch, which found that §10 had
been fixed in only one of two identical caches and that four further defects
had no test. See §12. **Nothing here is deployed yet** — the watertown work is
unpushed and the submodule pointer has not moved.

Selfmon exists to exercise watertown and surface its inefficiencies. It did.
This is what it found.

---

## TL;DR

| Finding | Severity | Status |
|---|---|---|
| **Collapse output is an O(N²) write amplifier** — superseded blobs never GC'd | **high** | **fixed** §8, item 1 |
| No GC for superseded `_large_files` blobs — 8.2 GB unreclaimable | **high** | **fixed** §9, item 2 |
| **Collapse blinds both read-pruning predicates** — bounded reads silently become full-history reads (§7) | **high** | **partly fixed** by tiering: event-time pruning survives, `version_gt` does not (§7 "Consequence") |
| **Reading a collapsed series double-counts every row** (§10) | **high** | **fixed** in the format cache; the same bug in the rollup cache was live until §12 |
| **Rollup partials double-counted a collapsed window** (§12) | **high** | **fixed** §12, testsuite 733 |
| reclaim's delete commits replayed as duplicate transactions by `pond rebuild` (§12) | medium | **fixed** §12 |
| reclaim deleted rows and swept blobs with no content-root check (§12) | medium | **fixed** §12 |
| A corrupt bao outboard silently produced a wrong collapse baseline (§12) | medium | **fixed** §12 |
| The collapse backstop rewrote the largest run, defeating tiering (§12) | medium | **fixed** §12 |
| Selfmon has no `TablePhysicalSeries`, so that collapse path is never exercised | **high** (coverage) | **fixed** in config; NOT YET DEPLOYED |
| **Tick steps failed silently, and a failed read published as a fast read** (§11) | **high** | **fixed** in caspar.water |
| `logfile-ingest` silently truncates a series when its active file is rotated (§11) | medium | open, latent |
| Stale format-cache parquets are never pruned (§10) | low | **fixed** §12 (rollup); format cache still leaks disk only |
| `libpod-conmon-*` journal files unbounded in count; one is 165.8 MB | medium | open (item 4) |
| `pond copy` versions identical content — 9,769 versions of a 19-byte file | medium | open (item 5) |
| `_minio-admin.env` measured as if it were a pond (duplicate + dead data) | low | open (item 7) |
| `interval = "1min"` is unachievable; ticks take ~3 min | low | open (item 9) |

Compaction itself is **working correctly** — see below. The problem is that
compaction reaches only 0.5% of the pond.

---

## 1. Compaction is working; it just can't reach most of the pond

Per-tick `maintain --compact --prune --collapse-versions 100` holds the
compaction-reachable surface at:

| area | size | files |
|---|---|---|
| live data parquet | **50.4 MB** | **18** |
| control | 37 MB | — |
| tlog | 6.2 MB | — |

18 files / 50 MB for a pond committing ~2.2×/min for a month is a good steady
state. The recurring `Compaction committed (+4/-14 files)` and
`Compacted control (+1/-37)` log lines are it doing its job. **No action needed
on compaction.**

But the pond on disk is 11 GB:

| area | size | files | reachable by maintenance? |
|---|---|---|---|
| `data/_large_files` | **8.2 GB** | 34,076 | **no — no GC exists** |
| `data/_delta_log` | 2.7 GB | 461 checkpoints | yes (retention works) |
| `cache/jsonlogs_*` | 96 MB | 21,659 | n/a (derived cache) |
| compaction-reachable | 50 MB | 18 | yes |

### `_delta_log` is fine — do not "fix" it

Checkpoint size across the retained 24 h window is flat, slightly declining:

```
5.87 → 5.86 → 5.84 → 5.82 → 5.80 → 5.78 MB   (oldest → newest)
```

`maintenance.data_log_retention_minutes = 1440` is enforced to the minute
(oldest entry is exactly 24 h old). A plausible-sounding theory — "checkpoints
inflate as `_large_files` grows" — is **wrong**; checkpoints do not enumerate the
blob store.

Still worth questioning separately: a **5.8 MB checkpoint for 50 MB of live
data**, written once per tick, is ~2.6 GB/day of write amplification. The cause
is the version count, see §3.

---

## 2. `_large_files`: 8.2 GB for 386.6 MB of live content (~21× amplification)

**These are not series.** Every one is a plain `[FILE]` entry.

Mechanism: in tlogfs each write creates a new version of the **whole file**, and
any version whose content exceeds the large-file threshold is externalized as
its own immutable content-addressed parquet blob under `data/_large_files`.
Superseded versions' blobs are never reclaimed. `pond maintain --help` confirms
there is no GC option; `--compact` / `--prune` / `--collapse-versions` all
operate on delta-log-tracked data only.

Measured totals:

```
live file content     386.6 MB   (2,220 files, 94,419 versions)
_large_files on disk    8.2 GB   (34,076 blobs)
```

### Source 1 — **collapse output is the O(N²) engine** (the main finding)

The `/logs/journal/<unit>.jsonl` and `/measure/*.jsonl` entries are
`FilePhysicalSeries` (confirmed via `pond describe`; `pond list` renders them as
`[FILE]`, which is misleading). Each ingest appends a **version** holding only
the new bytes — that is why `journal_ingest` logs
`Writing 175 entries (219525 bytes)` while the entry's `Size:` stays at 113 KB.

The loop:

1. Every tick appends ~1 version to each active series.
2. When a series passes **100 live versions** (`--collapse-versions 100`),
   `collapse_file_series` merges **all** live versions into one new version
   containing the **entire series history**.
3. That merged object is the whole accumulated file — for the busiest series
   it is now **96.3 MB**, and it grows ~1.0 MB per collapse cycle.
4. It is written to `_large_files` as a new content-addressed blob, and the
   **previous collapse's blob is never reclaimed**.
5. Cadence: 100 versions ÷ ~1 per tick ÷ ~3 min per tick ≈ **5.3 h**.

The measured blob timestamps match that cadence exactly, and the sizes form a
clean arithmetic progression — successive collapse outputs of the same series:

```
Jul 21 14:58   74.74 MB      Jul 24 06:20   86.82 MB
Jul 21 20:14   75.73 MB      Jul 24 12:05   87.87 MB
Jul 22 01:33   76.73 MB      Jul 24 17:30   88.93 MB
Jul 22 06:57   77.75 MB      Jul 24 22:39   89.95 MB
Jul 22 12:21   78.76 MB      Jul 25 04:07   91.05 MB
Jul 22 17:45   79.82 MB      Jul 25 09:09   92.12 MB
Jul 22 23:07   80.86 MB      Jul 25 14:16   93.19 MB
Jul 23 04:14   81.84 MB      Jul 25 19:32   94.23 MB
Jul 23 09:21   82.84 MB      Jul 26 00:52   95.24 MB
Jul 23 14:31   83.82 MB      Jul 26 06:15   96.30 MB
Jul 23 19:44   84.83 MB
Jul 24 01:01   85.83 MB
```

**The math.** If a series grows by δ per collapse cycle, then after N collapses
the store holds δ(1 + 2 + … + N) = **δN²/2** bytes, while live content is only
δN. Amplification is therefore **N/2** and grows without bound.

Measured: those 22 retained outputs alone sum to **~1.9 GB** for a series whose
live size is 96.3 MB. Extrapolating over the pond's life (~122 collapse cycles
since Jun 29) gives ~5.9 GB — i.e. the bulk of the 8.2 GB.

Corroborating aggregate — blob *count* per day is flat while average blob *size*
(and thus bytes/day) doubled in 11 days:

| day | files/day | MB/day | avg blob |
|---|---|---|---|
| Jul 15 | 1,004 | 298 | 304 KB |
| Jul 20 | 1,238 | 555 | 459 KB |
| Jul 25 | 1,182 | 612 | 531 KB |
| Jul 26 | — | — | 626 KB |

Growth rate is itself growing ~30 MB/day/day → ~1.5 GB/day within a month.

> **The counter-intuitive part:** *lowering* `--collapse-versions` makes this
> **worse**, because it triggers more frequent full rewrites. Collapse is a
> read-side optimisation that costs O(N²) on the write side whenever its
> superseded outputs are not GC'd. This compounds the already-known coupling
> that caused the Jul 25 sealed-bucket outage — the threshold is dangerous in
> *both* directions, and neither bound is enforced in code.

Note the feedback loop: `user-pond-selfmon@watershop-selfmon.service.jsonl` is
selfmon ingesting **its own** log output.

### Source 2 — one-shot giant blobs, one per *container run*

podman gives every container run its own transient systemd scope
`user-libpod-conmon-<sha>.scope`, so journal ingest creates a **new file per
container run** and writes it once:

```
conmon: 617 files, 645 versions, 373.1 MB  (97% of live bytes)

165.8 MB  user-libpod-conmon-830c5b87....scope.jsonl   v1   ← one site-prod
 63.8 MB  user-libpod-conmon-c5553a3f....scope.jsonl   v1     sitegen container
 49.6 MB  user-libpod-conmon-a0c0b377....scope.jsonl   v1
 24.1 MB / 15.5 MB / 7.5 MB / 7.4 MB / 5.6 MB ...
```

This set is **unbounded in file count** — one more file forever, per container
run, never rotated, never expired. And the more verbose watertown's container
logging becomes, the more selfmon permanently stores.

### Source 3 — `pond copy` versions identical content

`config/scripts/run-selfmon.sh` (caspar.water) unconditionally re-copies the
site templates into the pond every tick, with no change detection:

```
/system/site/sidebar.md    19 BYTES   v9769    content: "{{ content_nav /}}"
/system/site/metrics.md      539 B    v9769
/system/site/status.md       738 B    v9768
```

~29,000 versions carrying zero information. Too small to be externalized, so
this does not feed `_large_files` — but it is a third of all version rows, and
therefore a third of the metadata written into every 5.8 MB checkpoint.

---

## 3. Why a tick costs ~3 minutes

`interval = "1min"` is configured, but measured wall time over 7 consecutive
runs was `3m15s, 3m26s, 3m00s, 2m49s, 2m57s, 2m55s, 3m06s`, at 3m23s–4m03s CPU
each. The service therefore **never idles** — it finishes and systemd
immediately restarts it, ~100% duty cycle, ~1.2 of the host's 12 cores.
(Host load 2.16, 62 GB RAM — tolerable, but it does compete with the site
builds, and is a plausible contributor to the long-unexplained "site-prod is
~2.3× slower than noyo-prod on byte-identical inputs".)

Phase budget of a 175 s tick (self-recorded in
`/var/log/watertown-selfmon/watershop-selfmon/{.sitegen-last.json,_self.jsonl}`):

| phase | time | share |
|---|---|---|
| sitegen build | ~62 s | 35 % |
| logfile-ingest ×11 (one `pond run` process each) | ~53 s | 30 % |
| measure fan-out ×10 (2× `find` + `du -sb` + `pond list` + `pond log`) | ~35 s | 20 % |
| maintain (pre-tick) | ~14 s | 8 % |
| journal + caddy ingest | ~10 s | 6 % |

sitegen is stable at 60–66 s with 1.37 GB peak RSS.

The scaling factor is that **selfmon's own pond dwarfs everything it monitors**:

| pond | list.s | parquet files | size |
|---|---|---|---|
| **watershop-selfmon** | **2.57** | **56,245** | **11.7 GB** |
| site-staging | 0.71 | 3,420 | 1.0 GB |
| site-prod | 0.38 | 1,758 | 1.0 GB |
| water-staging | 0.54 | 1,754 | 0.6 GB |
| water-prod | 0.56 | 854 | 0.4 GB |
| septic/noyo ×4 | 0.22–0.46 | 251–525 | ~0.1 GB |

---

## 4. Coverage: `FilePhysicalSeries` collapse **is** exercised; `TablePhysicalSeries` is not

Selfmon runs `FilePhysicalSeries` collapse constantly — measured over 24 h:

```
433 ticks   collapse: 0 file(s) collapsed  (0 candidates)
 16 ticks   collapse: 1 file(s) collapsed, 101 version(s) superseded
  1 tick    collapse: 2 file(s) collapsed, 202 version(s) superseded
```

i.e. **18 file-collapses/day**. So the "0 candidate(s)" seen on most ticks is
just the threshold self-gating between cycles, not an idle code path.

What selfmon has **none** of is `TablePhysicalSeries`. Its 12 `[SER]` entries are
all `v1` dynamic/derived (`sql-derived-series`, `timeseries-join`), computed at
read time; every *physical* series it owns is a `FilePhysicalSeries`.

**That asymmetry is why the `TablePhysicalSeries` version-collapse bug (#121)
survived undetected until noyo hit it in production.** The file-series path was
continuously exercised here; the table-series path never was.

Fix (**done in config, not yet deployed** — see item 3): a `materialize-series`
factory pins the `/derived/perf` join into a real `TablePhysicalSeries` at
`/metrics/perf.series`, fed by the per-tick metrics, so both collapse paths are
exercised continuously. Until the submodule pointer is bumped, **this asymmetry
still exists in production**.

---

## 5. Minor: `_minio-admin` is measured as if it were a pond

`run-selfmon.sh` loops over `env/*.env`, which includes `_minio-admin.env` —
a file holding only S3 credentials, **no `POND`**. It is not skipped, because
`run-selfmon.sh` exports `POND` (its own) earlier, so `measure-pond.sh`'s
`: "${POND:?}"` guard passes on the *inherited* value.

Proof from the emitted data: `_minio-admin.jsonl` reports
`size.bytes: 11673469731` (11.7 GB) and `committed.txn_ids: 70871` — i.e. it is a
mislabeled **duplicate measurement of the selfmon pond itself**, which is the
single most expensive probe (2.33 s `pond list` + 3 tree walks over 56 k files),
run every tick.

It is also entirely dead: there is no `/system/etc/measure/_minio-admin` mknod,
so the resulting 2.0 MB jsonl is never ingested.

Fix (caspar.water): skip `_*.env` in both loops in `run-selfmon.sh`, or require
`POND` to come from the env file itself rather than being inherited.

---

## 6. Unrelated housekeeping found along the way

`/home/jmacd/pond-watershop-selfmon-staging` is **32 GB**, last written
2026-04-27. Fully orphaned: no systemd unit, no env file, not in
`local.instances` (terraform calls it a retired name).
`cleanup-stale-pond-units.sh` reaps retired *units*, but nothing reclaims native
selfmon *data dirs* — they live at `/home/jmacd/pond-<name>`, outside podman
volume management. Disk is not tight (894 GB, 30 % used) but retiring a selfmon
instance currently strands its data forever.

---

## 7. Collapse destroys the metadata that read-pruning depends on

Distinct from the O(N²) write problem in §2, and undocumented. Found by asking
what happens to per-version byte ranges after a merge.

### What is lost

The merged row records a single `size`/`blake3`, a single `version` (`V`, the
highest), and temporal bounds taken as `min()`/`max()` **across all absorbed
versions** (`crates/tlogfs/src/persistence.rs:1201-1203`). Per-version
boundaries within the byte stream are not retained anywhere.

### Why it matters

`SeriesReadBounds::retains` (`crates/tinyfs/src/file.rs:70`) prunes using
exactly the two things collapse erases:

```rust
let event_time_ok = max_event_time.is_none_or(|max_ts| max_ts >= lo);
let version_ok = version > watermark;
```

After a collapse both degrade to "retain everything":

- **`version_gt = K`** for any `K < V`: the merged row's version is `V`, so
  `V > K` holds and the row is returned **whole**.
- **`event_time_lo = lo`**: the merged row's `max_event_time` is the global
  maximum, so it is retained for essentially any bound.

So a bounded, incremental read silently becomes a full-history read.
`crates/sitegen/src/factory.rs:840` (the status_grid fold) is exactly that
consumer: once per collapse, per series — every ~5.3 h — it re-reads the entire
series through DataFusion instead of the ~1 MB of new data it asked for.

### It is a cost bug, not a correctness bug

The re-read does **not** double-count. `status_summary::merge` is an idempotent
semilattice: every scalar field is latest-by-timestamp via `keep_later`, and
tails dedupe on `(ts_us, msg)`. Re-folding already-folded data is a no-op.

Note however that the doc comment on `processed_version`
(`crates/sitegen/src/status_summary.rs:62-64`) claims *"every version is
processed exactly once"* — that is **false** after a collapse. It is harmless
only because the fold happens to be idempotent, i.e. an undocumented invariant
is load-bearing for a property it was never designed to provide.

### Consequence for the fix

This is a second, independent argument for range-based (`[lo, hi]`) collapse
runs beyond write amplification: each run carries its own version range *and*
its own `min`/`max_event_time`, so pruning survives at run granularity instead
of being all-or-nothing.

**Only half of this was actually delivered.** Tiering restored *event-time*
pruning at run granularity, as described below. It did **not** restore
`version_gt` pruning: a merged run still takes a fresh highest version number
while standing in for mid-stream content, so version order no longer matches
byte order and an incremental reader keyed on `version_gt` still cannot prune —
it degrades from `O(N)` to `O(fanout)` rather than to `O(1)`. The rule that
fixes this exists (`CollapseRange` / `is_superseded` in `tlogfs::schema`) but is
confined to tlogfs; `tinyfs::FileVersionInfo` never gained the collapse range,
so nothing above tinyfs can apply it. See §12 "Still open".

The shape is also right. Under tiered collapse, recent versions stay loose and
unmerged, so full per-version resolution is preserved precisely where
incremental readers operate — the tail — and only cold history is coarsened,
which nobody prunes into. Today's scheme does the opposite: it coarsens
everything, including the hot tail that `version_gt` depends on.

---

## Recommended order

**watertown**

1. ~~**Range-based (tiered) collapse.**~~ **DONE** (branch `jmacd/analysis8`,
   uncommitted). `collapsed_through` is now paired with `collapsed_from` to form
   the range `[lo, hi]` a merged row supersedes, and `collapse_file_series`
   merges a bounded window of same-size-class versions
   (`COLLAPSE_FANOUT = 10`) instead of the whole history. Write volume goes from
   `O(N^2)` to `O(N log N)`, and per-run `min`/`max_event_time` keeps the read
   pruning of §7 alive. Implementation notes:
   - Version numbers are never reassigned, so **version order is no longer byte
     order**. Three readers were ordering by version and are fixed:
     `async_file_reader_series`, `metadata()` (was returning the highest-version
     row, which after tiering is a *mid-stream* run, so appends resumed from the
     wrong bao prefix), and `read_pending_bytes` (walked version numbers
     backward, interleaving runs with loose versions).
   - A merged run inherits the cumulative bao outboard of the newest version it
     absorbs, verbatim. Collapse only regroups an unchanged byte stream, so the
     prefix through `hi` is byte-identical; recomputing it as a fresh first
     version is correct only for a run starting at v1.
   - ~~`TablePhysicalSeries` still merges the full live set.~~ Superseded by
     §8: it was tiered too, via a version-bounded provider.
2. ~~**GC for superseded `_large_files` blobs.**~~ **DONE** (see §9). Collapse
   now deletes the rows it superseded and sweeps the blobs they were the last
   referrers of. This is what reclaims Source 1's ~5.9 GB.
3. ~~Give selfmon a `TablePhysicalSeries` so that collapse path is exercised
   too.~~ **DONE in config, NOT YET DEPLOYED.** A new `materialize-series`
   factory pins the `/derived/perf` join into a real `TablePhysicalSeries` at
   `/metrics/perf.series` (caspar.water `1f587f3`). **The coverage gap that let
   #121 ship stays open in production until watertown merges and the submodule
   pointer is bumped** — the deployed `pond` has no such factory.
4. Filter or aggregate `libpod-conmon-*` scope logs at ingest — storing a
   166 MB container log verbatim is not what selfmon is for, and the file set
   is unbounded in count.
5. Make `pond copy` a no-op when the content hash is unchanged (fixes §2
   Source 3 generically, not just here).
6. Document/enforce the `--collapse-versions` bounds: it is bounded **below** by
   `allowed_lateness` (rewrite depth → sealed-bucket outages) and **above** by
   read cost (~58 ms/version), and lowering it *increases* O(N²) write
   amplification. Neither bound is enforced in code today. Note that tiering
   (1) largely retires this hazard: merges touch only old, sealed windows and
   never reach back toward the `allowed_lateness` horizon.

**caspar.water**

7. Skip `_*.env` in `run-selfmon.sh`.
8. Reclaim the 32 GB orphan; teach cleanup to reap native pond dirs.
9. Revisit `interval = "1min"` once the above land.

## 8. Tiering extended to `TablePhysicalSeries` (done)

`collapse_table_series` had the same whole-history defect as the file path, for
a different reason: `reencode_table_series` scanned the series through
`create_table_provider(id, .., TableProviderOptions::default())`, whose default
`VersionSelection::AllVersions` means *the entire series*. Every trigger
re-encoded every row ever written.

The missing primitive already existed: `TableProviderOptions::additional_urls`
takes explicit per-version URLs and still runs `infer_schema`, so schema merging
across versions is preserved. Windowing therefore needed no new
`VersionSelection` variant and no object-store change --
`reencode_table_versions(id, &[versions], ts_col)` builds one
`TinyFsPathBuilder::url_specific_version` per window row.

Three changes:
- window selection via the same `choose_collapse_window(&live, max_live)` as the
  file path (parquet byte sizes feed `size_class` naturally);
- `lo`/`hi` and the event-time bounds now come from the *window*, not all live
  rows, so `SeriesReadBounds::retains` keeps its pruning resolution;
- verification is window-local: it re-scans only the merged version and compares
  its row count to the window's, instead of re-scanning the whole series (that
  check was itself an O(N) read on every collapse).

The read path needed nothing: `tinyfs_object_store` enumerates versions via
`list_file_versions`, which already honors collapse ranges.

Note the file path's ordering hazard does *not* apply here. Table versions are
unioned rather than byte-concatenated, so a merged run's position in version
order is irrelevant to content; only its range matters, for supersession and
pruning.

## 9. Reclamation: the second half of collapse (done)

Tiering (§7, item 1) fixed the *rate* at which blobs are stranded; it could not
return a byte. Collapse appended a merged run but left the rows it superseded in
the table, and **a superseded row still references its blob**, so the blob stayed
pinned. A blob sweep alone would therefore have freed nothing — the rows have to
go first.

Reclamation runs inside `Ship::collapse_versions`, with no new flag or command,
because it is not a separate feature: without it collapse is only half an
operation.

1. Delete superseded series rows from the data Delta table.
2. Mark-sweep `_large_files`, deleting every blob whose blake3 no longer appears
   in any remaining row.

**The order cannot be inverted.** `pond fsck`'s content pass reads *every* row —
including superseded ones — and requires each large-file blob to exist, so
sweeping while the rows survive turns a healthy pond into a failing one.
Reclaiming *after* the collapse transaction commits is what makes it crash-safe:
an interruption leaves the superseded rows in place, which is exactly the
pre-reclaim state, whereas deleting first could lose content if the merged run
never landed.

**Supersession is decided by `live_series_versions`** — the same definition the
read path uses — and never by a watermark or a range test. A merged run carries a
fresh highest version while standing for content in the middle of the stream, so
a run's own version can fall *inside a later run's range*. Both "everything below
K" and "anything inside a range" would delete live runs. This is the same hazard
that produced six separate silent corruptions during the tiering work.

**The sweep is a mark-sweep by hash, global over all `pond_id`s**, because blobs
are content-addressed: one file backs many rows across many nodes and ponds
(cross-pond imports mirror rows verbatim), so a per-row delete would free a blob
another row still needs. It runs under the pond write lock, which is the only
thing that distinguishes garbage from a blob a concurrent writer has staged but
not yet committed.

Deleting these rows is **Merkle-neutral**: steward's content fold already prunes
them, so `root_tree_hash` is unchanged and mirrors — which never received them —
stay converged. Testsuite 717/718/726/727 (push/pull, fsck replica equality,
incremental mirror pull) pass unchanged, which is the real proof.

`pond maintain` now collapses **before** `ship.maintain()`, mirroring the
existing `--prune` placement, so the delete's tombstones are reclaimed by the
same checkpoint + vacuum pass.

### What this does and does not reclaim

- **Source 1 (~5.9 GB, the bulk):** reclaimed. Despite `pond list` rendering them
  `[FILE]`, these are `FilePhysicalSeries` versions, and the stranded collapse
  outputs are precisely the superseded rows reclamation deletes.
- **Source 2 (~373 MB of conmon logs):** *not* reclaimed, and correctly so —
  those are live `v1` content. Bounding them is an ingest-filtering / retention
  decision (item 4), not garbage collection.
- **Source 3 (~29,000 no-op `pond copy` versions):** unaffected; they are too
  small to be externalized and never touch `_large_files`. They remain a third of
  all version rows and thus of every checkpoint (item 5).

Still unreclaimed by anything: `FilePhysicalVersion` / `TablePhysicalVersion`
files (the 21,659 `cache/jsonlogs_*` population) are never collapse candidates at
all, so they have no supersession relation and accumulate without bound.

---

## 10. Reading a collapsed series double-counted every row

Found while extending testsuite coverage, not from the disk audit. It was
**not** on the original list and is the most serious correctness defect here.

`listing_table_from_cache` built its ListingTable by **globbing** the node's
cache directory:

```
{cache}/{scheme}_{node_id}/*.parquet
```

That is correct only while "every parquet ever written" equals "every version
that is live". Collapse breaks exactly that equality: it replaces a run of
versions with one merged version holding the same bytes, and the superseded
versions' parquets stay on disk. The glob returned both, so every row appeared
twice — then four times after the next collapse.

Reproduced in isolation and pinned as testsuite `731`: a derived read returns
**9 rows before `pond maintain --collapse-versions`, 18 after**.

### Why it went unnoticed

`pond cat` reads the pond directly and stayed correct throughout, so the raw
file looked fine while every *derived* read doubled. The corruption scales with
collapse activity, so it was worst on exactly the ponds that ingest the most.

It affects **every external-format read** — `jsonlogs`, `oteljson`, `csv`,
`weblog`, `excelhtml` — i.e. every ingest pipeline in selfmon and noyo.

### The fix

The bounded read path was already correct: it names each live version's file
explicitly. It simply delegated to the globbing variant whenever bounds were
`NONE`, which is the common case. Both paths now name live version files
explicitly (`SeriesReadBounds::NONE.retains()` is true for everything, so the
merge is safe), and schema merging was scoped to the retained files via a new
`merge_parquet_schemas()`.

### Still open: nothing prunes the stale format-cache parquets

Reclamation (§9) does not help. It deletes superseded rows and blobs *inside*
the pond; the format cache is a separate host-side artifact keyed by version +
content hash, and nothing sweeps it. A disk-waste issue rather than a
correctness one — the 21,659 `cache/jsonlogs_*` files in §1's table are this
population. Natural home is `pond maintain`.

### Wrong scope: this section claimed more coverage than it had

The fix above covered `format_cache`. It did **not** cover `rollup_cache`,
which is a near-identical copy of the same code and had the same defect —
with the worse consequence that it silently *double-counted* rather than
merely re-read. That is §12.

---

## 11. Silent failures in the tick itself

Selfmon exists to surface watertown's problems. Its own driver was hiding its.

### Three classes, in `run-selfmon.sh`

1. **Truly silent.** The per-pond metric ingests ran under `2>/dev/null || true`,
   discarding the exit code *and* the error text. This is the pipeline feeding
   every chart: had it failed, the dataset would simply stop advancing while the
   page kept rendering the last good data as if nothing were wrong — with
   nothing, anywhere, to say otherwise.
2. **Worse than silent — actively misleading.** The read benchmark timed
   `pond cat` under `|| true` and published the elapsed time *regardless*. A read
   that errored out in 5 ms was recorded as a 200x speed-up: a failure that
   looked like the best tick we ever had.
3. **Semi-silent.** Steps that echoed `WARNING:` to a journal nobody reads.

Non-fatal remains the right policy — aborting mid-tick is what historically
wedged the pond — but non-fatal must not mean invisible.

### Fixed (caspar.water `4692a56`)

Every non-fatal step now runs through a `step` wrapper that lets the tick
continue while recording the failure in three channels:

| channel | catches |
|---|---|
| stderr | lands in the journal |
| `.tick-status.json`, written from an **EXIT trap** | clean step failures *and* aborts / OOM kills |
| a **`tick.failures` metric** in `_self.jsonl` | reaches the dashboard |

The third is the point: failures belong on the chart the operator already
watches. `tick.failures` and `read.ok` flow through `/derived/p-_self` ->
`/metrics/perf.series` -> `/reduced`, verified end to end. `tick.failures` lags
one tick for the same reason `sitegen.seconds` does — the record is written
before those steps have run.

The tick also now exits non-zero when any step failed, so systemd marks the run
failed. Reporting success after five failed steps is itself a silent failure.
Safe because it happens in the EXIT trap, after all work is done, on a
timer-driven one-shot unit.

Schema compatibility was verified rather than assumed: the live `_self.jsonl`
predates both new fields, and `logfile-ingest` widens the schema on append, so
old rows read back as `NULL` instead of failing the `CAST` and taking down the
whole join.

### Open, latent: `logfile-ingest` truncates a rotated series

Found while testing the above. If the active log is **rotated** rather than
appended to — the old file renamed to match `archived_pattern`, a fresh active
file created — ingest reports:

```
Logfile ingestion complete: 1 new (260 bytes), 0 appended, 1 unchanged
outcome=ok
```

...and **silently discards the previously ingested content**. Three pre-rotation
rows vanished; `pond describe` showed `Version: 1`, `Size: 260 bytes`. Exit
status is 0 and nothing is logged above INFO.

Not reachable today — nothing rotates these files, so every mknod's
`archived_pattern` currently matches zero files. But the patterns exist in the
config precisely because rotation is anticipated, so this is armed for whenever
someone adds logrotate. Worth a testsuite case regardless of whether the
behaviour is judged wrong: silently returning `ok` while dropping data is not an
acceptable outcome either way.

---

## 12. Branch review: one live corruption and four untested defects

Added 2026-07-27 after reviewing the whole `jmacd/analysis8` branch rather than
each change in isolation. The review was prompted by the sense that details did
not add up; they did not.

### 12.1 The same bug, fixed in one of two copies (the live corruption)

§10 fixed version-collapse double-counting in `format_cache`. `rollup_cache` is
a near-identical copy of that module — one of its functions was even documented
as *"Mirrors `format_cache::cache_version_path` keying exactly"* — and it still
had the defect.

The mechanism: `find_uncached_members` only ever **added** partials named
`{node}_v{version}_{blake3}.parquet`, and the read side **globbed the
directory**. After `maintain --collapse-versions`, the merged version got a new
partial while the superseded versions' partials stayed. The merge is
`SUM(__p_sum_i), SUM(__p_count_i) GROUP BY bucket`, so every row in the
collapsed window was summed twice — and again on each later collapse.

Scope: the rollup cache engages only when the input scheme has a
`FormatProvider` (`oteljson`, `jsonlogs`, `csv`). Builtin `series:///`,
`file:///`, `table:///` bypass it, so selfmon's own
`materialize-series → temporal-reduce` pipeline was **not** affected.
`water.yaml` / `septic.yaml` reading `oteljson:///ingest/*.json` **were**, with
`config/scripts/run.sh` running `maintain --compact --collapse-versions 100`
hourly.

**Why it hid for a year.** Testsuite `050` already ran the exact sequence —
collapse, then an oteljson temporal-reduce — but asserted only on `avg` and
bucket count. `avg` is `SUM(sum)/SUM(count)`, unchanged when both double; the
`GROUP BY` re-collapses duplicate buckets so the count is unchanged too. Both
assertions are invariant under precisely this corruption. `temporal_reduce.rs`
also explicitly skips its sequentiality guard for series inputs, justified on
the grounds that *"distinct versions never share a row"* — the one invariant
collapse breaks.

Pinned red by testsuite `733` (96 → 192), now green.

### 12.2 The fix: name the abstraction instead of patching the copy

Patching `rollup_cache` would have left two copies of a shape that had already
proven it silently diverges. Both modules are the same thing: a directory of
per-version Parquet sidecars that must be a projection of a node's **live**
versions. `crates/provider/src/version_cache.rs` now names it once, with three
barriers chosen so the bug class is unwritable rather than merely fixed:

1. **`LiveVersions`** — a version list is a node-scoped type, not a bare `Vec`,
   so "did the caller pass the live set?" is a type question, not a review one.
2. **`SidecarDir::reconcile`** — both **adds** missing sidecars and **removes**
   stale ones. A cache that only adds is how a superseded sidecar survives to be
   counted twice.
3. **`CachedSet`** — the only route to a `TableProvider`, carrying explicit file
   paths. There is deliberately **no** `fn(&Path) -> TableProvider` in the
   module, because every instance of this bug has been exactly that function.

`SidecarNaming::NodeScoped` guards the rollup directory, which is shared by
every node one `*` pattern matched: reconciliation there must never delete a
sibling node's partials.

**Behaviour change.** With superseded partials now deleted, a sealed run built
on them is no longer a valid incremental base, so the level is **rebuilt** from
the live partials instead of hard-failing with *"backfills an already-sealed
bucket"*. That error was the site-staging outage. Raising `allowed_lateness` to
14d only suppressed it — and left the silent doubling in its place.

**Migration.** Reconciliation fixes future reads only. Water/septic sealed runs
already froze doubled values and need a one-time `--rebuild` or a `cfg_hash`
bump.

### 12.3 Four defects with no test

- **reclaim's delete commits were replayed as transactions.** They stamped the
  collapse's `txn_seq` under `pond_txn`, and `reconstruct_txn_history` yields one
  transaction per such commit, so `pond rebuild` replayed N+1 duplicate lifecycle
  records at one sequence. Now stamped `pond_maintenance`.
- **reclaim asserted nothing about what it deleted.** The strictly safer
  `compact` verifies its `root_tree_hash`; reclaim, which deletes rows and sweeps
  blobs permanently, did not. It now checks the content root either side of the
  deletes **before** the sweep — deleted rows are recoverable from Delta history
  or a mirror, a swept blob is not.
- **A corrupt bao outboard silently produced a wrong baseline.** `.ok()`
  conflated "no outboard" with "unparseable outboard", falling back to the
  first-version baseline that the adjacent comment says is wrong for any later
  run. Now a hard error.
- **The collapse backstop rewrote the largest run.** It merged `0..FANOUT`, and
  after any previous collapse index 0 *is* the accumulated run — the `O(N²)`
  behaviour tiering exists to prevent. It fires far more often than it looks,
  because callers pass the same number as both the candidacy threshold and
  `max_live`, making `live.len() > max_live` true by construction. It now merges
  the cheapest window of the same width.

### Still open

- **`version_gt` pruning is still blind after collapse** (§7). The fix is to
  plumb `collapsed_from` / `collapsed_through` onto `tinyfs::FileVersionInfo`
  and make raw `version` unusable for ordering, which would kill this class at
  compile time. Two known sites order on raw `version`:
  `format_cache` pruning and `tinyfs_object_store`'s `max_by_key(|v| v.version)`.
- **Water/septic need a one-time `--rebuild`** before their reduced series can
  be trusted (§12.2).
- **The format cache still leaks stale parquets to disk** (§10). Correctness is
  fine — reads name live files explicitly — but nothing sweeps them.
