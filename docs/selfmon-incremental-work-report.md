# Watershop selfmon: incremental-work investigation

Date: 2026-11-13 UTC.

Status: investigation complete; the first F1 slice is committed as `7fbf1c29`,
its NodeMetadata continuation as `4f107fba`, and request-scoped range-read reuse
as `bbfcb661`. F2 is committed as `6b632c3d`. F4's raw timestamp support is
committed as `9cd956fa` in Watertown and its configuration as `88b8e20` in the
outer repository. Azure-access locality is committed as `962fee77` in Watertown
and `f4d8c43` in the outer repository. Exact-timestamp limiter grouping is
committed as `3e36a1d1` and its configuration as `f6e4c22`. F3's one-day
sealable-backlog policy is committed as `8f235c8a`, with explanatory outer
comments in `42e5d2a`. F5's analytics selfmon wiring is committed as `0418c1c`.
Historical metadata still needs follow-up, and no deployment was performed as
part of these fixes.
This report records an observed baseline, not a claim about the host's current
state after subsequent runs or upgrades.

## Purpose and conclusion

Watershop self-monitoring is an intentional stress test of Watertown's
incremental-update efficiency. The objective is to identify systematic work
amplification, not make the dashboard appear healthy by reducing its workload,
discarding history, resetting caches, or slowing its timer.

**The workload is producing small incremental outputs while repeatedly doing
large amounts of upstream work.** Output partition reuse is functioning, but
does not establish that source discovery, metadata access, provider construction,
or hot-window aggregation are work-bounded.

There are four performance findings and one separate configuration failure:

| ID | Finding | Evidence strength |
|---|---|---|
| F1 | Latest-record and version-metadata access load unnecessary historical rows or inline payloads; range requests repeat record lookup. | Direct code evidence; contribution to total time not individually profiled. |
| F2 | Materialization constructs its source without the target watermark's read bound. | Direct code evidence, consistent with large reads for very small appends. |
| F3 | Sparse aggregate output leaves a roughly 26-day hot window containing 943 eligible source versions. | Active manifests plus the implemented sealing policy. |
| F4 | Raw perf ingestion lacks event-time bounds; some derived inputs also lack a validated local execution contract. | Configuration and implementation evidence. |
| F5 | Dynamic pond discovery finds analytics-staging, but selfmon has no corresponding ingestion node. | Repeated runtime failure plus configuration mismatch. |

These are not five independent explanations for the same bytes. In particular,
F1 can amplify the cost of the source fan-out retained by F2 and F3; F4 limits
the pruning that fixing F2 can achieve.

## Scope and provenance

The host inspection was performed around 2026-11-13 02:51-03:01 UTC and included
recent completed ticks. The detailed I/O example below is the tick ending at
02:36:04 UTC.

| Item | Observed baseline |
|---|---|
| Instance | `watershop-selfmon` on Watershop |
| Service | `pond-selfmon@watershop-selfmon.service` |
| Native executable | `/usr/bin/pond`, version `0.301.222` |
| Runtime revision | `281187ae228aa082c3bda22ceb9798cd8e97e2cd` |
| Watertown source used for the trace | `f27b8acd0d198a4f73b792d8b47399517421536a` |
| Runtime-to-source difference | Only `Cargo.toml` and `Cargo.lock`; the traced engine source is unchanged. |
| Nominal schedule | One-minute timer; observed work lasts roughly an hour. |
| Selfmon data footprint | About 16.5 GiB |
| Selfmon control footprint | About 12 MiB |
| Selfmon cache footprint | About 834 MiB |

The deployed script and YAML matched the local outer-repository files by SHA-256:

```text
config/scripts/run-selfmon.sh
99cde4eafd54847e1fd81ec4e115b4a74f4ae048d182864f65b669d8a74438cd

config/watershop-selfmon.yaml
e20f61024c94934fd4d0437cd541e93127680650994ab848d6bdefdcd9c5019a
```

Paths beginning with `crates/` below are relative to Watertown. Paths beginning
with `caspar.water/config/` identify files in the enclosing repository of the
pair, not files in this submodule. Line references describe the inspected
baseline and may move as the findings are addressed.

The investigation was read-only. No services were restarted, no cadence or
configuration was changed, and no pond data or cache was cleared. No test suite
was run as part of this investigation.

## Workload and measurement interpretation

The relevant tick sequence is:

```text
maintain
  -> full-history journal COUNT benchmark and per-pond probes
  -> journal, Caddy, and perf-log ingestion
  -> template copies
  -> materialize perf, limiters, and Azure-access series
  -> Sitegen rollup exports, status grid, and HTML
```

The script deliberately exercises maintenance with:

```text
--compact --collapse-versions 100 --prune --allow-no-remote --keep-txns 1000
```

It also deliberately benchmarks:

```sql
SELECT COUNT(*) FROM source
```

over `jsonlogs:///logs/journal/*.jsonl`. That is a full-history query by design,
not an incremental append whose entire input should disappear from the work.
Recent recorded benchmark durations were approximately 441, 443, 529, and
460 seconds. They must be kept separate from incremental-work regressions.

### Logical reads are not physical disk reads

The script snapshots `/proc/$$/io`. Linux accumulates waited-for child I/O into
the parent, allowing the script to report per-step and whole-tick deltas.
`rchar` measures logical bytes read; it is not the number of bytes fetched from
disk and is not an exclusive measure of Parquet source bytes. Reads served from
cache and other read operations contribute.

Six observed completed ticks reported these `rchar` totals, in bytes:

```text
92,854,165,124
92,637,259,025
93,170,002,920
93,390,225,727
93,511,566,880
93,836,015,490
```

This is repeated behavior, not a single cold-cache event. Physical reads were
only roughly 1-12 MB per tick, with reported writes around 0.56-0.60 GB. The
evidence points to repeatedly processing cached data, not 93 GB of physical disk
traffic per tick. It does not by itself identify the CPU cost of each operation.

### Per-step logical-read example

For the tick ending at 02:36:04 UTC:

| Step | Logical reads, GiB |
|---|---:|
| Maintenance | 7.595 |
| Journal ingestion | 2.965 |
| Caddy ingestion | 0.040 |
| Per-pond perf ingests combined | 7.808 |
| Pond probes | 0.523 |
| Materialize perf | 1.054 |
| Materialize limiters | 0.682 |
| Materialize Azure access | 2.648 |
| Sitegen | 18.679 |
| Work outside the individually accounted steps | 45.396 |
| **Whole tick** | **87.392** |

The last category is a residual, not an attributed subsystem. It includes the
intentional journal benchmark, cursor/list checks, template copies, and other
unwrapped work. It would be incorrect to call all 45.396 GiB an incremental
cache defect. Values are rounded.

In the inspected runs, individual perf ingests appended only 145-273 bytes but
performed approximately 0.73-0.81 GB of logical reads each. The materializers
appended 10 perf rows, 7 limiter rows, and approximately 4-6 Azure-access rows.
Small output size alone is therefore a misleading success criterion.

### Sitegen localization

The retained Sitegen summary showed:

| Export or phase | Seconds | Reuse detail |
|---|---:|---|
| Perf, finest resolution (`1m`) | 801.423 | 54 partitions reused, 1 written |
| Limiters, finest resolution (`1m`) | 656.306 | 54 partitions reused, 1 written |
| Each coarser resolution | Approximately 0.8-1.03 | Much cheaper than the finest level |
| All exports | 1465.129 | 114 partitions reused, 10 written |
| Status-grid discovery | 6.427 | 11 files discovered |
| Complete status-grid phase | 49.398 | Includes discovery |
| HTML rendering | 0.546 | Not the main cost |

These are phase measurements, not all independent additive totals. Recent
overall Sitegen durations were approximately 1462-1759 seconds.

The rollup export summaries reported `global_plans=0`,
`non_incremental_plans=0`, and `dynamic_source_executions=0`. That is consistent
with reducing materialized native series, rather than accidentally executing
the raw dynamic join beneath each rollup. Those counters describe plan
classification; they do not prove cheap metadata access or a small hot window.

## F1. Metadata access loads unnecessary history and payloads

### Evidence

In [persistence.rs](../crates/tlogfs/src/persistence.rs):

- `query_latest_record` at line 4149 checks pending records first, then calls
  `query_committed_node_records` and takes the first returned record.
- `query_latest_directory_record` at line 4189 calls that same helper and finds
  the first directory record. Its comment describes a `LIMIT 1` optimization
  that the helper does not implement.
- `query_committed_node_records` at line 4222 executes the following shape and
  collects/deserializes every returned `OplogEntry`:

```sql
SELECT * FROM delta_table
WHERE pond_id = ... AND part_id = ... AND node_id = ...
ORDER BY version DESC, timestamp DESC
```

There is no `LIMIT`. These are exact-node queries, not unfiltered whole-pond
queries, but their returned work still grows with that node's retained history.

`list_file_versions` at line 4577 also obtains full records through
`query_records`, computes live membership, and only then converts the records
to `FileVersionInfo`. For inline files, that conversion currently derives the
logical size from `content.len()` at line 168. A metadata-only replacement must
preserve the correct size; simply omitting the content column is not sufficient.

`file_version_info` at line 4618 calls `load_file_version_record`, which does
have an exact-version predicate and `LIMIT 1`, but still selects the complete
record. Thus this second path is unnecessary payload access, not an unlimited
history query.

In [tinyfs_object_store.rs](../crates/provider/src/tinyfs_object_store.rs),
`get_opts` first resolves version metadata around line 206. For a range request
it then calls `read_file_version_range` around line 282; that operation loads
the full version record again. A small inline-file range therefore does not
mean only that range's bytes were retrieved from the oplog. Large-file payloads
use a separate reader, so this is not a claim that every external large-file
payload is always read in full.

The persistence wrappers at lines 2132-2166 hold the shared inner mutex across
these asynchronous operations. Extra per-version queries can therefore also
serialize work that looks concurrent at the object-store layer.

### Consequence and uncertainty

Metadata discovery can read payload history before any data-level time
predicate has a chance to help. Repeating metadata and range lookups across
hundreds of source versions is a credible multiplier.

The call paths are confirmed. Their exact fraction of the 801-second and
656-second finest-resolution exports is not measured. Nor does adding SQL
`LIMIT 1` alone prove constant physical work: sorting, projection, file layout,
and predicate pruning still determine what the engine reads.

### Corrective direction

Separate latest-record selection, version-metadata enumeration, and content
access. Bound the returned latest-record query, filter directory kinds before
limiting where required, and avoid retrieving inline payloads for metadata-only
operations. Resolve a version once where a request needs both metadata and a
range, without introducing stale or unscoped metadata caches.

Preserve pending-write visibility, pond/partition/node identity, logical version
ordering, live-series collapse semantics, exact object sizes, and transaction
coherence. Existing metadata fields or a deliberately designed fallback may be
needed for older inline records whose size cannot be obtained independently.

### Acceptance criteria

- Latest-file and latest-directory results remain correct with pending writes,
  multiple historical versions, and imported identities from different ponds.
- Version enumeration retains the same live membership after collapse without
  transferring all inline content into application memory just to list it.
- Metadata-only, range, and full reads retain correct sizes and bytes for inline
  and large files, including not-found and conditional-request behavior.
- A real-TLogFS regression with growing retained payload history measures
  selected columns, rows, or bytes, not just output correctness or wall time.
  Any remaining history-sized metadata enumeration is identified explicitly.

### Local implementation update, 2026-11-13

The first F1 slice is committed as `7fbf1c29` in
`crates/tlogfs/src/persistence.rs`:

- Latest committed record lookup selects only `(version, timestamp)` while
  finding the newest revision, then fetches the exact matching record. Directory
  kinds are filtered before selecting the revision. The full-history helper
  remains available to callers that actually require history.
- `list_file_versions` and `file_version_info` use a narrow `VersionMetadata`
  projection that excludes both `content` and `bao_outboard`. Pending rows are
  projected in memory without cloning their payloads. An exact pending-version
  lookup no longer queries committed storage first.
- Live membership uses the existing shared collapse-range rules. Ordering,
  timestamp bounds, hashes, logical identity, and schema metadata are preserved.
- Normal file sizes come from the already-persisted `size` field. A committed
  record missing its size falls back to reading that exact record's inline
  content, with a warning for files. Negative sizes, or missing size without
  recoverable inline content, return errors instead of zero or a wrapped value.

Three real-Delta regressions failed on the original implementation and pass
after the change. They inspect projections and predicates forwarded to the real
Delta provider, alongside result correctness, pending/foreign identity
isolation, timestamp tie-breaking, collapse membership, and size compatibility.
All 122 TLogFS library tests passed for that slice.

The next slice, covering general `NodeMetadata` lookup, is committed as
`4f107fba`:

- Candidate selection reads only version, timestamp, size, and collapse-range
  fields. Ordinary nodes select the latest committed candidate; byte series
  retain the metadata needed to determine the live end of the stream.
- Pending candidates retain their in-memory origin without cloning content or
  outboards. Version/timestamp ordering and committed precedence on an exact
  tie are preserved.
- Byte-series selection uses the same range-containment and content-order
  rules as other readers. It selects the last live nonempty row, falling back
  to the latest row when necessary. A high-numbered mid-stream merge must not
  replace the stream-tail metadata used to resume appends.
- Only the selected committed revision's metadata and Bao outboard are then
  requested, never its file contents. A selected pending row needs no committed
  outboard lookup. Cumulative hash conversion is shared with the existing
  `OplogEntry::metadata` path.
- Missing-size metadata, empty-series fallback, unwritten-node defaults,
  directory/symlink metadata, and not-found behavior are preserved.

Three additional real-Delta regressions failed before this continuation and
passed afterward. All 125 TLogFS library tests, workspace and test-target Clippy,
formatting, and documentation checks passed for that slice.

The third slice, request-scoped version resolution, is committed as `bbfcb661`:

- TinyFS exposes a `ResolvedFileVersion` handle with version metadata and
  consuming operations for opening a reader or reading a range. Its default
  implementation defers to existing backend methods. The caching decorator
  forwards a backend's specialized resolution rather than hiding it.
- TLogFS retains one exact version record for the request. Metadata and body
  operations reuse it; pending versions are found before querying committed
  storage. Existing mutex use and transaction-closure checks are retained.
- Ordinary exact-version object-store GETs use this handle for both ranged and
  full responses. Real-Delta regressions demonstrate one record query rather
  than two for inline and large-file ranges; pending reads need no committed
  query. This measures record-query count, not a promised halving of total I/O.
- HEAD and conditional requests retain their metadata-first path, so rejected
  conditions do not force a content read. Unversioned/latest requests retain
  their existing selection path. These cases intentionally do not use the
  single-resolution fast path.
- Range validation still precedes opening external content. Resolution can
  already load inline bytes before validating a range; there is no cross-request
  cache. A resolved handle rejects new open/range operations after transaction
  closure, including through the default backend implementation.

Validation passed for 377 provider, 231 TinyFS, and 129 TLogFS library tests,
required workspace Clippy, TLogFS test-target Clippy, formatting, and
documentation checks. The extra TinyFS test-target Clippy invocation failed on
the pre-existing `clippy::let_unit_value` warning at
`crates/tinyfs/src/tests/memory.rs:24`; that unrelated file is unchanged, and no
lint allowance was added.

The original F1 eager-payload and ordinary exact-version duplicate-lookup paths
are now addressed in code. Remaining cost qualifications are important:
byte-series candidate enumeration still grows with record history, and
conditional/latest object requests retain separate metadata and content phases.
F1's runtime benefit remains to be measured on the retained Watershop workload.

The regressions establish query projection and exact-revision selection, not
constant-time Delta scans or measured physical bytes. No Watershop before/after
performance comparison has been made, and F2-F5 are unchanged.

## F2. Materialization applies its watermark too late for source construction

### Evidence

In [materialize_series/mod.rs](../crates/provider/src/factory/materialize_series/mod.rs):

- `table_for`, lines 113-138, delegates to `table_for_bounded` with
  `SeriesReadBounds::NONE`.
- `execute`, line 420, constructs the source through that unbounded helper.
- It obtains the source timestamp type, then reads the target watermark at
  line 435.
- At line 448 it applies the row predicate `timestamp > watermark`.

The row filter is already present and is necessary. The missing piece is a
source-construction bound, not another equivalent filter.

The target-watermark implementation is more careful: it uses live-version
metadata for exact bounds where possible and a bounded tail scan for
nanosecond precision. Its bounded calls are for the target, not the source.
Legacy targets with missing metadata have a visible full-scan fallback.

### Consequence and constraints

The materializer can append only the correct new rows while still paying for
unbounded version discovery, schema discovery, or format-cache preparation.
Subsequent DataFusion optimization cannot undo work already performed during
provider construction. This does not imply every source row must always be
decoded: later pushdown may still eliminate some execution work.

There is a real sequencing dependency: watermark interpretation currently uses
the source timestamp type. A fix needs a cheap schema/type path or another
correct way to determine the target bound. Constructing the full source once
for schema and then rebuilding it with bounds would retain much of the defect.

### Corrective direction

Propagate a conservative target-derived lower bound into source provider
construction while retaining the exact strict row filter. Preserve timestamp
units and sub-microsecond precision; a coarse metadata bound must never exclude
eligible rows. Bootstrap and missing-metadata fallbacks must remain explicit.

Do not silently expand this change into new late-arrival or upsert semantics.
The current materializer is append-only and uses a strict watermark. Correcting
older source rows is a separate semantic question from avoiding unnecessary
work for the existing behavior.

### Acceptance criteria

- A timestamp-local source receives a bounded construction request on a warm
  append and idle run, without an initial unbounded provider build.
- Bootstrap, empty targets, legacy bounds, and nanosecond timestamps retain
  correct behavior; a boundary row is neither duplicated nor lost.
- Growing history outside the retained input window does not cause payload or
  footer work proportional to all old versions when metadata is sufficient.
- Global queries are not incorrectly made timestamp-local just to pass the
  performance case.

F4 is a dependency for realizing the full benefit on raw selfmon perf logs.

### Local implementation update, 2026-11-13

F2 is committed as `6b632c3d`, with the shared time-column capability approved
during review.

The materializer now reads target version metadata first and derives a
conservative microsecond lower bound before constructing the source provider.
It does not build an unbounded source merely to discover its timestamp type.
After bounded construction supplies that type, the existing target-watermark
logic resolves the exact scalar and the strict `timestamp > watermark` filter
remains in place. The already-read target metadata is reused.

Bounds are only applied when the requested column matches an established source
axis:

- `QueryableFile::event_time_column` defaults to unknown. Timestamp-local SQL
  and timeseries joins expose their declared output axes; global SQL does not.
- The shared Provider resolver reads the declared timestamp column from native
  table-series metadata, without constructing its data provider. Nonempty
  versions and glob members must agree.
- Missing metadata, a different column, an unknown query contract, host files,
  or external-format transformations retain conservative unbounded execution.
  The materializer logs why a warm source is unbounded rather than guessing
  from the name or shape of its schema.
- Bootstrap still reads the source in full. A legacy target without usable
  bounds retains the visible full-watermark-scan fallback.

The precision regressions exposed related boundary problems that are fixed in
this slice:

- A nanosecond row can be newer than the exact watermark while sharing its
  microsecond bin. The publication layer previously rejected such a row using
  its second, microsecond-only boundary. For nanosecond sources, the already
  exact frame predicate now enforces the append boundary without that lossy
  second check; progress metadata remains in microseconds.
- Source hints and exact target tail scans conservatively retain the preceding
  bin where legacy negative-nanosecond metadata may have truncated toward zero.
  This supports mixed old and new bounds without rewriting stored history.
- Shared batch and Parquet-footer timestamp extraction now floor negative
  nanoseconds to microseconds, agreeing with query execution. This also fixes
  the staged-output/commit-metadata mismatch for pre-epoch materialization.

Real-TLogFS regressions use separate transactions like the production CLI runs.
They demonstrate that warm native, timestamp-local SQL, and joined sources no
longer load payloads from before the target watermark during construction or
execution. They also check append/idle behavior, global aggregation correctness,
and mixed legacy/current negative-nanosecond bounds without duplicates.
Additional cases cover a mismatched time column, exact nanosecond boundaries,
and consistent batch/footer rounding.

Validation passed for 378 provider, 231 TinyFS, and 131 TLogFS library tests,
required workspace Clippy, provider/TLogFS test-target Clippy, formatting, and
documentation checks. The previously noted optional TinyFS test-target lint
warning is unrelated and unchanged.

This fixes source-bound propagation, not every source's ability to prune.
Raw perf versions without event-time metadata still require F4, and global
limiter/Azure projections remain global. No Watershop performance improvement
has yet been measured.

## F3. Sealing bounds compressed output better than upstream work

### Observed active state

The two active `segments-v4` namespaces were:

```text
perf:
merged_68325e8255de787f_00000000-0001-7209-b828-f340394edfda

limiters:
merged_57b2c1d94a43e21d_00000000-0001-7291-bf28-2e2afdf96561
```

| Property | Perf | Limiters |
|---|---:|---:|
| Sources recorded in manifest | 1988 | 1988 |
| Sources with unknown event-time bounds | 0 | 0 |
| Allowed lateness | 86400 seconds | 86400 seconds |
| Sealed segments per resolution | 1 | 1 |
| Finest `sealed_hi_secs` | 1792304520 | 1792304520 |
| Finest sealed frontier, UTC | 2026-10-18 06:22 | 2026-10-18 06:22 |
| Versions retained by the observed lower bound | 943 | 943 |
| Finest hot-file bytes | 274778 | 110406 |

Newest source events were around November 13, 02:00 UTC. The hot window
therefore covered about 26 days. The retained-version count is a source-selection
observation, not a separately measured count of physical reads by every query.
An older namespace last modified on October 8 was not the active cache and
must not be used to diagnose current reuse.

### Mechanism

[partial_aggregate_cache.rs:173-180](../crates/provider/src/partial_aggregate_cache.rs#L173-L180)
sets a 1 MiB sealing target and a 30-day active-span backstop.
[temporal_reduce/mod.rs:1537-1645](../crates/provider/src/factory/temporal_reduce/mod.rs#L1537-L1645)
seals when either trigger is reached and a newer sealable watermark exists.
The span test is measured to the lateness-adjusted watermark, not simply the
latest event.

Until sealing advances, the hot window starts at the old sealed frontier.
The source read bound around lines 1898-1947 must cover both this window and
any earlier dirty range. The implementation then recomputes hot output from
the retained input.

This behavior follows the current policy; it is not evidence that the manifest
is corrupt or that one-day allowed lateness is ignored. Allowed lateness sets
eligibility for sealing, not a promise that only one day remains active.

The outer YAML at lines 910-923 already recognizes the size gate, but estimates
a roughly 1-2.8-day window. The actual sparse workload does not satisfy that
estimate. Both hot files are far below 1 MiB despite the upstream fan-out.

### Corrective options and tradeoffs

A tighter event-time-span trigger is the simplest existing mechanism to bound
this case. A source-version or estimated-input-work trigger would address
fan-out more directly, but needs a well-defined cost measure and must not make
the decision itself expensive. Merely lowering the byte target still makes
compressed output a proxy for input cost.

Any choice must balance bounded active work against excessive tiny segments.
Keep size-tiered compaction and source-set invalidation. Do not remove the
handling of deleted, replaced, collapsed, or backfilled sources: an earlier
dirty range legitimately requires unsealing and recomputation. No particular
new threshold was selected by the initial diagnosis; the approved policy is
recorded below.

### Acceptance criteria

- With thousands of sparse source chunks and small aggregate output, the active
  input window advances under an explicit work/span budget before the byte
  threshold alone would force it.
- Ordinary appends avoid repeatedly reopening unchanged sealed history;
  unchanged source snapshots reuse the existing result.
- Late additions, replacements, deletions, and collapse still recompute every
  affected bucket, including across a sealed boundary.
- Correctness matches a full recomputation, and segment count/compaction cost
  do not grow unchecked with build frequency.

### Local implementation update, 2026-11-15

The approved first step reduces the existing event-time backlog trigger from
30 days to one day. It is committed as `8f235c8a` in Watertown; outer policy
comments are committed as `42e5d2a`.

Here, backlog means older results that are already eligible for reuse but are
still inside the file recomputed on updates. It runs from the current sealed
frontier (or the oldest bucket before the first seal) to the
lateness-adjusted watermark. It does not include the recent allowed-lateness
tail.

The 1 MiB byte trigger, configured lateness, segment compaction, cache format,
and source-change invalidation rules are unchanged. No new configuration
setting was added. With selfmon's one-day lateness, regular sparse builds
generally keep roughly one to two days hot, subject to bucket granularity and
build cadence. This bounds extra duration, not absolute bytes or source count.

Existing caches are not invalidated by the threshold change. The next changed
build may pay for an older oversized hot window once before advancing the
sealed frontier; subsequent appends use that newer input bound. Unchanged
source snapshots remain reusable without a timer-driven seal.

The updated sparse regression checks the threshold below 1 MiB, verifies both
maximum and sum results across sealed/hot storage, and confirms a late addition
repairs an older sealed bucket without losing or double-counting other data.
The real-TLogFS dynamic-source case now seals a three-day sparse history, keeps
sealed files byte-for-byte unchanged during sub-day appends, advances again
after a day of eligible backlog, and uses the new frontier on the next append.
The cache namespace remains unchanged.

All 380 provider and 133 TLogFS library tests, required workspace and affected
test-target Clippy, formatting, generated-reference freshness, and documentation
checks passed. The outer selfmon file changes only explanatory comments; its
YAML data is unchanged.

No running cache was reset, no source history was deleted, and no service cadence
or deployment was changed. Runtime improvement still needs measurement on the
retained Watershop workload.

## F4. Raw perf metadata and derived locality limit pruning

The `logfile-ingest` definitions in
`caspar.water/config/watershop-selfmon.yaml:90-217` specify input paths but no
`timestamp_field`. In
[logfile_ingest/mod.rs:375-405](../crates/provider/src/factory/logfile_ingest/mod.rs#L375-L405),
`attach_temporal_bounds` consequently leaves version event-time bounds unset.

Unknown bounds must be retained conservatively. A provider cannot safely
exclude an old version merely because its version number or modification time
looks old.

Selfmon's raw `ts` values are ISO timestamps. The current generic extractor,
`timestamp_bounds` around line 295, accepts numeric values and numeric strings,
not ISO timestamps. **Adding only `timestamp_field: ts` is not a valid fix.**
When timestamp extraction is declared but cannot produce bounds, the writer
requires temporal metadata and should reject the write rather than silently
store an unbounded version.

The per-pond typed perf projections already declare timestamp locality around
YAML lines 675-800. The limiter aggregation/pivot and Azure-access projections
are currently global. Their actual dependency contracts must be reviewed before
introducing bounds; a complex aggregation is not safely local just because its
output has a timestamp column.

The materialized perf and limiter series already have known event bounds, as
the F3 manifests show. This gap affects raw-source/materialization work; it does
not explain the 943-version rollup selection through unknown bounds.

### Corrective direction and acceptance

Choose either a supported numeric timestamp emitted by the measurement producer
or shared ISO timestamp extraction, then configure the matching field/unit.
Keep malformed input visible. Validate locality for eligible derived
transformations rather than bypassing the global-plan safeguards.

Existing unbounded chunks require an explicit compatibility plan. Newly bounded
appends will not automatically repair old metadata, and old unknown ranges
must not be pruned optimistically. Any backfill or migration should be a separate,
approved operation, not a hidden reset.

Acceptance should cover actual selfmon records, timestamp conversion and
rounding, partial-line ingestion, malformed timestamps, and retained legacy
chunks. A warm materialization with bounded raw versions must demonstrate
pruning without changing the result relative to a full source query.

### Local implementation update, 2026-11-14

The raw timestamp portion of F4 is committed as `9cd956fa` in Watertown, with
the selfmon configuration committed as `88b8e20` in the outer repository.
The approved approach preserves the existing producers and adds an explicit
`timestamp_unit: rfc3339` interpretation to `logfile-ingest`.

- RFC3339 mode accepts complete date-time strings with `Z` or a numeric UTC
  offset. It normalizes them to epoch microseconds, rounding the lower bound
  down and the upper bound up for sub-microsecond values, including before 1970.
- Numeric timestamp modes retain their existing parsing and conversion
  behavior. There is no automatic switch between numeric epochs and date
  strings; the operator selects the representation.
- Bounds are computed from newly ingested slices. Active incomplete lines wait
  for completion, and an append with no usable timestamp fails its declared
  temporal-write requirement. Existing corrupt-line warning behavior is
  unchanged.
- `caspar.water/config/watershop-selfmon.yaml` opts all 11 existing measure
  ingestion nodes into the mode: `timestamp_field: ts` for perf logs and
  `timestamp_field: timestamp` for the limiter feed. Producer output, ingestion
  paths, and unrelated YAML documents are unchanged.
- The factory documentation and generated schema reference describe the mode
  and its compatibility requirements. A supporting binary must be deployed
  before applying the new selfmon configuration.

Unit cases cover offsets, pre-epoch and fractional precision, nested timestamps,
invalid dates, missing zones, and explicit numeric/string mode separation. A
real-TLogFS append regression starts with an unbounded legacy version, enables
timestamp extraction, and verifies that only new versions receive bounds while
the old metadata/hash remain unchanged. It also checks partial-line deferral,
idle passes, and a rejected invalid append leaving committed content intact.

Validation passed for 380 provider and 132 TLogFS library tests, required
workspace and provider/TLogFS test-target Clippy, formatting, generated-reference
freshness, and documentation checks. The outer configuration check verified all
11 field/mode pairs and that other YAML data is unchanged.

F4 is **not fully closed**. Previously unbounded pond versions are still unknown
and must remain conservatively readable. Their metadata has not been backfilled
or rewritten. The separate Azure-access and limiter locality continuations are
described below. Locality changes are not an automatic effect
of selecting an RFC3339 parser. No configuration was applied to the running
host, and no runtime speedup has been measured.

### Azure-access locality continuation, 2026-11-15

`azure-access` names a selfmon query executed locally over journal messages
about storage activity. It is not an Azure SQL backend. The other selfmon
datasets include ordinary pond performance measurements (`perf`) and budget
utilization (`limiters`, which also includes Azure-related budgets). The
optimization depends on a query's operations, not which service its data names.

The Azure-access step is committed as `962fee77` in Watertown and `f4d8c43`
in the outer repository:

- Timestamp-local SQL now accepts a `Sort` only when it has no `fetch` limit.
  Existing immutable-expression checks apply to sort expressions as well.
- `LIMIT`, `OFFSET`, top-k sorts, aggregates, window operations, subquery
  expressions, and non-immutable functions remain rejected. Global SQL is not
  automatically reclassified.
- The outer configuration declares only `/derived/azure-access` timestamp-local.
  Its complete SQL text, including its existing `ORDER BY timestamp`, is
  unchanged. The limiter query is untouched.
- A binary supporting non-limiting local sorts must be deployed before enabling
  this locality declaration. No running configuration was changed here.

The shared query-foundation regressions compare a full ordered result restricted
by time with the corresponding locally planned time-restricted result. They
also retain explicit rejection cases for limits, offsets, volatile sort
expressions, and a logical sort carrying a fetch bound. The real-TLogFS
materialization regression now exercises a sorted timestamp-local source.

The exact Azure SQL was extracted from the outer YAML and executed through
`plan_user_sql` against synthetic journal-shaped records. The full global
result, restricted afterward, matched the timestamp-local result over bounded
input in every column and in ordering. Non-Azure destinations and non-production
units were included to exercise the existing filters. The temporary validation
program was removed, and the YAML comparison confirmed that only locality
metadata was added.

The query-foundation tests and all 380 provider / 132 TLogFS library tests
passed. One materialization test executable was initially terminated by
SIGKILL before reporting results; its isolated rerun passed all ten tests.
Required workspace and affected test-target Clippy, formatting, generated
reference freshness, and documentation checks also passed.

Limiter SQL needs the separate treatment described below for its exact-timestamp
grouping. The Azure change does not allow that aggregation under the existing
projection/filter contract, rewrite historical metadata, or establish a measured
Watershop speedup.

### Limiter locality continuation, 2026-11-15

The approved `timestamp-grouped` mode is committed as `3e36a1d1` in Watertown,
with configuration committed as `f6e4c22` in the outer repository. It is a
separate opt-in contract; ordinary `timestamp-local` SQL still rejects aggregation.

The new mode requires at least one aggregation and exactly one grouping key:
the declared event-time column. That key must be typed before aggregation, and
its name and value must survive every projection. The existing limiter
conversion from a timestamp string to a nanosecond timestamp is supported,
along with unchanged-column/no-op-cast projections. Bucketing, reduced time
precision, parsing strings only after grouping, time arithmetic, or relabeling
an aggregate result as event time are rejected.

Aggregate functions and their expressions must be immutable. Limits, offsets,
top-k sorts, windows, joins, grouping sets, and subquery expressions remain
unsupported. Guard cases also cover volatile aggregate arguments, filters,
ordering expressions, and scalar subqueries inside aggregate arguments.

The outer configuration opts only `/derived/limiters` into the new mode.
Its complete SQL, including every percentage calculation, filter, `MAX(CASE...)`
expression, and `ORDER BY timestamp`, is unchanged. A supporting binary is
required before applying this declaration.

The query-foundation regressions compare complete full-input timestamp groups
with bounded-input results, including duplicates and null measurements. They
also verify that equivalent zoned timestamp strings are parsed before grouping
and that distinct nanosecond instants remain distinct groups. A real-TLogFS
materialization test splits one timestamp's input across multiple versions,
checks that both contribute, verifies that older source payloads are pruned,
and confirms that an idle pass does not append again.

The exact deployment SQL was also executed against synthetic limiter records
covering duplicate samples, zero denominators, incomplete observation windows,
Azure-mirror scope naming, excluded ponds/limiters, and error records. Every
output column and ordering matched the full global result restricted to the
same time range. The temporary checker was removed, and the YAML comparison
confirmed that only locality metadata was added.

Query-foundation tests, all 380 provider and 133 TLogFS library tests, required
workspace and affected test-target Clippy, formatting, generated-reference
freshness, and documentation checks passed.

This establishes exact-time query dependence, not a promise that a timestamp
group can never receive late input. The materializer's existing append-only
semantics are unchanged. No historical metadata was rewritten, and no runtime
configuration or workload was changed.

## F5. Analytics pond discovery and declarations disagree

`caspar.water/config/scripts/run-selfmon.sh:371-407` discovers non-underscore
`env/*.env` files and invokes the corresponding
`/system/etc/measure/<pond>` ingestion node.

The new `analytics-staging.env` is discovered, but
`caspar.water/config/watershop-selfmon.yaml` has neither its ingestion node nor
its typed perf projection/join mapping. Every inspected tick logs
`Configuration file not found` for `/system/etc/measure/analytics-staging`.
The recorded tick status identifies:

```text
failures: 1
steps: ingest:measure:analytics-staging
```

This explains the service's failure status, not the hour of work. The script
continues after recording the failed step, and its EXIT trap ultimately returns
nonzero so systemd marks the run failed.

The status JSON and I/O log can show `exit_rc: 0` alongside `failures: 1` because
the trap captures and writes the incoming exit status before converting it to
the final nonzero status. That field should not be read as proof of overall
success or as a swallowed failure.

The minimal correction is to add the analytics ingestion definition and the
matching typed perf/join wiring, preserving schema compatibility. A broader
generated inventory is an option, not required scope. At minimum, configuration
validation should catch a discovered pond without corresponding declarations.

Acceptance requires ingestion and dashboard visibility for analytics-staging,
no missing-node error, and continuing visible failure reporting if a node is
actually absent. Do not make the tick green by skipping or suppressing the error.

### Local implementation update, 2026-11-15

The missing wiring is committed to the outer repository as `0418c1c`:

- `/system/etc/measure/analytics-staging` ingests the existing per-pond metrics
  log using `timestamp_field: ts` and the explicit RFC3339 mode.
- `/derived/p-analytics-staging` uses the same typed `pondseries` SQL as other
  regular ponds and declares its timestamp-local contract.
- `/derived/perf` includes that projection under the `analytics-staging` scope.
  The new input is appended without reordering existing inputs.

The renderer needs no analytics-specific change: charts discover numeric
columns across the exported rows, and the status grid already resolves
`series:///derived/p-{pond}` for the ordinary `pond@` service names. This wires
analytics into the existing pipeline rather than creating another dashboard.

`config/scripts/test-selfmon.py` checks the ingest/projection/join/status paths
against Terraform's configured instance inventory. It detects missing,
miswired, duplicate, or orphaned perf stages and verifies the shared projection
and timestamp settings. Every configured instance is required, but additional
fully wired monitoring entries are permitted: deployment inventory and retained
monitoring history need not have identical lifecycles. The check was verified
against both the committed Terraform inventory and the pending deployment
inventory. It failed on the original missing analytics definition and passes
after the wiring change. It runs with Python and PyYAML; the command
is documented beside the selfmon inventory comments.

The runtime probe script and its failure reporting are unchanged. Limiter
columns are not expanded in this change, and historical materialized analytics
perf values are not backfilled. These are configuration checks, not a claim
that the running service is already healthy: the updated binary/configuration
still need an approved deployment and runtime verification.

## What this investigation does not establish

- It does not attribute a precise percentage of CPU or logical reads to F1,
  F2, or F3. No CPU profile or syscall trace was captured.
- It does not show that physical storage bandwidth is exhausted. Most measured
  reads were logical, with very little physical read I/O.
- It does not show that exported output partition reuse is broken. The retained
  summaries show extensive reuse.
- It does not classify the full-journal benchmark or all maintenance work as
  an incremental-update defect.
- It does not establish that native commit processing always performs a full
  tree fold. `guard::write_reserved_nodes` uses a manifest-root cursor and
  `incremental_spine_inputs_v2`, not the older similarly named implementation.
  A full-fold oracle is explicitly gated by `POND_VERIFY_FOLD`. That flag was
  absent from the checked selfmon env and user service-manager environment;
  the recent journal contained no enabling warning. This is not a complete
  profile of the running commit path.
- It does not establish backtrace capture as a dominant cost. `PerfTrace`
  uses `Backtrace::capture`, not forced capture; backtrace-enabling settings were
  not found in the checked diagnostic environment.
- Schema fingerprint deduplication exists in
  [table_creation.rs:146](../crates/provider/src/table_creation.rs#L146).
  Whether older retained versions lack fingerprints and incur additional
  footer reads was not measured.

Attempts to sample direct child processes through `/proc` were inconclusive;
child PIDs changed as the tick progressed. Those attempts supply no quantitative
attribution. No debugger was attached and no profiling software was installed.

## Historical metadata and rollout follow-up

The F1-F5 code/configuration fixes are committed. With explicit approval, the
eight Watertown commits were pushed on `jmacd/analytics_repair` and opened as
[jmacd/watertown#224](https://github.com/jmacd/watertown/pull/224), with the
`build-container` label requesting PR container/package artifacts. No merge or
deployment was performed. The outer fixes remain local, and the report remains
unstaged. The outer repository also still contains earlier, separate analytics
deployment/content work that must not be silently folded into this repair series.

A read-only assessment of legacy timestamp metadata found:

- `maintain_command` in `crates/cmd/src/commands/maintain.rs` offers checkpoint,
  compaction, control pruning, and pack-only maintenance, not a historical
  timestamp-bounds repair operation.
- The canonical series leaf encoder in
  `crates/sync-store/src/content/series_leaf.rs:694-724` hashes the presence and
  values of event-time bounds along with logical attributes. The
  `file_leaf_hash` path at lines 842-874 uses that encoding.
- TLogFS's `set_extended_attributes` updates only a pending version and
  re-stamps its logical identity before commit. It is not an API for changing
  older committed rows.
- Shared format-cache pruning currently obtains bounds from `FileVersionInfo`
  (`crates/provider/src/format_cache.rs:137-178`); it does not itself provide a
  historical bound-enrichment command.

Consequently, adding bounds to a committed series row is not merely filling in
an innocuous cache field. An in-place update could invalidate its committed
logical identity. No such update, re-ingestion, deletion, or cache reset was
attempted.

The recommended next step is to complete review of the published Watertown
changes and build the corresponding image, then perform an explicitly approved staging
rollout and compare ordinary warmed ticks against the recorded baseline.
Only after measuring the residual legacy cost should a separate enrichment
design be selected. Possibilities include verified, non-authoritative hints in
the shared cache or an explicit logical replacement/migration; neither is
implemented or approved here.

## Suggested one-at-a-time work plan

Keep the finding IDs above stable when discussing or implementing individual
fixes. This sequence is a recommendation, not authorization to deploy changes.

| Order | Work item | Reason |
|---|---|---|
| 1 | F1: bounded latest-record access and metadata/content separation | Shared lower-layer amplification affects many otherwise reasonable callers. Split latest-record and version/range work if needed for reviewability. |
| 2 | F2: bounded source construction in materialize-series | Establishes the missing API-level work contract; exercise it with sources that already have reliable bounds. |
| 3 | F4: raw perf bounds and validated derived locality | Makes that contract useful through the complete selfmon materialization path. |
| 4 | F3: a defensible active-input work/span limit | Stops sparse rollups from repeatedly rebuilding weeks of eligible input. |
| Independent | F5: analytics selfmon declarations | Restores health reporting without pretending to solve the performance issues. |

For each performance fix, retain the workload and compare a warm no-change pass
and a small-append pass against increasing retained history. Measure work at the
boundary being repaired: queried rows/columns, content bytes, metadata requests,
opened source versions, or source-construction bounds. Wall time alone is noisy;
output correctness alone misses this entire class of regression.

Use focused real-TLogFS coverage where persistence behavior matters. A
MemoryPersistence-only test cannot expose Delta `SELECT *`, inline-payload
materialization, or the real object-store request sequence. Keep full-result
equivalence checks for bounds, late data, and invalidation cases.

After an approved deployment, compare several ordinary warmed ticks rather than
one cold or freshly reset run. Record the exact native executable revision and
configuration hashes, distinguish the intentional benchmark from incremental
steps, and retain errors as failures rather than treating fast failure as a
performance improvement. No before/after improvement has yet been measured.

## Evidence locations for follow-up

The source references above are the durable code trail. Runtime evidence came
from these existing host artifacts; they rotate or are overwritten, so this
report preserves selected values rather than a complete forensic log archive:

```text
User journal:
  pond-selfmon@watershop-selfmon.service

/var/log/watertown-selfmon/watershop-selfmon/
  .tick-status.json
  .sitegen-last.json
  .sitegen-last.log
  _self.jsonl

Selfmon pond:
  cache/merged_*/res*/manifest.json
```

Before any new measurement, re-identify the running process and active cache
namespaces rather than relying on old PIDs or directory names. Preserve original
inputs and existing history so each fix is evaluated against the stress case
that exposed it.
