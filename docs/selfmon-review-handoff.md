# Watershop selfmon review: handoff

Implementation/publication snapshot: 2026-11-16 UTC (2026-11-15 evening Pacific).
Architectural review addendum: 2026-11-16 morning Pacific.
Local commit/publication check: 2026-11-16 evening Pacific.

## Start here

The selfmon investigation produced four performance findings and one missing
monitoring definition. Their original engine fixes are published in
[jmacd/watertown#224](https://github.com/jmacd/watertown/pull/224), **Incremental
source reads and sparse rollup backlog**. Companion configuration
fixes are committed locally in the outer `caspar.water` repository, not in that
PR.

The subsequent code-quality and test-coverage review found four correctness
issues. All four are now addressed in local Watertown commits through
`945d938d`, **not yet pushed to the PR**. The associated configuration migration
is outer commit `cadead6`. Those four findings are closed, but the subsequent
architectural review identified three additional follow-ups, A1-A3 below.
Their implementation and both supporting reports are recorded in the same-branch
follow-up commit containing this handoff. They have not yet been pushed to the PR.

The user wants these issues pursued on the existing Watertown branch
`jmacd/analytics_repair` before merging `jmacd/watertown#224`. Do not create a
new branch or treat the remaining work as deployment-only validation. The
implementation follows the user's explicit **reject-and-verify** choice:
unsupported retroactive changes fail, and existing targets need verified
historical consistency before checkpoint adoption.

The user previously reported that Watershop was down. No live inspection was
attempted during this review; current health and the outage cause are unverified.
These fixes have not been deployed. Do not claim that they restored health or
improved runtime performance.

Read [the detailed investigation report](selfmon-incremental-work-report.md)
for historical baseline evidence, original implementation details, caveats, and
acceptance criteria. Its status predates the review follow-ups recorded below.
Both that report and this handoff are included in the local follow-up commit.

## Repository and publication state

Work stays inside the existing pair:
`/Volumes/sourcecode/src/caspar.water2` and its `watertown` submodule.

The following local snapshot predates the A1-A3 follow-up commit:

| Repository | Branch | HEAD | Ahead / behind recorded `origin/main` |
|---|---|---|---|
| `caspar.water` | `analytics` | `cadead6` | 12 / 1 |
| `watertown` | `jmacd/analytics_repair` | `945d938d` | 12 / 0 |

These are snapshot comparisons with local remote-tracking refs, not a promise
that GitHub has not advanced. GitHub was checked separately: the Watertown PR
targets `main`, is open, and still has head
`8f235c8a48c4e18fac3a08634ef39d1f6fe374d5`. The four local review-fix commits
are not part of that published head. The 2026-11-16 evening Pacific publication
check confirmed that the PR remains open at this same head; neither the R1-R4
commits nor A1-A3 are published yet.

The PR carries `build-container`, requesting container and Debian-package
artifacts. The latest published-head runs have completed:

- [Rust CI, run 43683324930](https://github.com/jmacd/watertown/actions/runs/43683324930):
  Check, Build & Test passed; container and Debian-package publication jobs
  passed for amd64 and arm64. Dependency audit failed on `quick-xml` advisories
  [RUSTSEC-2026-0194](https://rustsec.org/advisories/RUSTSEC-2026-0194) and
  [RUSTSEC-2026-0195](https://rustsec.org/advisories/RUSTSEC-2026-0195).
- [Integration Tests, run 43683324907](https://github.com/jmacd/watertown/actions/runs/43683324907):
  container, S3/MinIO compose, and browser tests passed.

The audit failure remains unresolved by this review. Published artifacts belong
to the old PR head, not the local fixes; artifact hashes and an installed native
executable were not inspected.

The PR was closed and reopened once after attaching the label because the build
gate reads labels from the triggering event. Initial runs `43683274076` and
`43683274131` were deliberately cancelled; do not mistake them for current
failures.

No review-fix push, PR merge, deployment, outer-repository push, or
submodule-pointer commit was performed during this review.

## Coverage of the selfmon findings

| ID | Finding | Implemented response | What is outside the PR |
|---|---|---|---|
| F1 | Metadata/latest-version lookups loaded unnecessary historical payloads; range requests repeated record lookup. | Lightweight metadata projections, selected-record loading, and request-scoped exact-version reuse. | Live measurement of the improvement. |
| F2 | Materialization built its source without using the saved target watermark to bound reads. | Shared event-time-column capability and conservative bounds before source construction, followed by exact row filtering. | Unknown or mismatched axes deliberately remain unbounded; A2/A3 below address raw-format construction costs locally. |
| F3 | Sparse output kept roughly 26 days of rollup input in the repeatedly recomputed hot window. | Reduce sealable backlog from 30 days to one day; preserve lateness, byte trigger, compaction, and late-correction behavior. | First catch-up build and subsequent warm behavior need live measurement. |
| F4 | Raw measurement logs lacked timestamp bounds; some derived queries lacked a safe bounded-execution declaration. | Explicit RFC3339 ingestion, non-limiting timestamp-local sorts, and restricted exact-timestamp grouping. | Outer configuration enables these features; local A2 adds raw-format bound propagation for proven encodings. Old versions are not backfilled. |
| F5 | Discovery included `analytics-staging`, but its selfmon ingestion/projection/join wiring was absent. | Add the complete monitoring path and inventory consistency regression. | Entire fix is in outer commit `0418c1c`, not this PR; live health remains unverified. |

These findings interact: F1 makes each lookup expensive, F2/F4 determine how
much history gets considered, and F3 determines how much recent aggregation is
repeated. Their costs should not be added as independent estimates.

### Published Watertown commits

| Commit | Change |
|---|---|
| `7fbf1c29` | Bound latest-record and version-metadata reads. |
| `4f107fba` | Avoid historical payload reads in node metadata lookup. |
| `bbfcb661` | Reuse resolved file versions within object-store reads. |
| `6b632c3d` | Bound materialization sources using declared event-time axes. |
| `9cd956fa` | Support explicit RFC3339 timestamps in logfile ingestion. |
| `962fee77` | Allow non-limiting sorts in timestamp-local SQL. |
| `3e36a1d1` | Add restricted exact-timestamp grouping for local SQL. |
| `8f235c8a` | Limit sparse rollup sealable backlog to one day. |

### Local review follow-ups, not yet published

These review IDs are separate from the original performance IDs F1-F5 above.

| Review finding | Commit | Implemented response |
|---|---|---|
| R1 (P1): wrappers forwarded bounds onto unverified input time axes. | `cc5f18e9` | Check each input's declared/recorded axis before forwarding event-time bounds. Unknown/mismatched axes and unproven transforms use a warned, unbounded event-time read; independent version bounds remain unchanged. |
| R2 (P1): ordinary timestamp-local SQL could replace or transform event time. | `da507afe` | Trace timestamp preservation through projections and CTEs. Add explicit `input_event_time` for safe parsing/renaming; reject arithmetic, replacement, and lossy casts. Also repair the R1 pump-state performance regression by declaring its output axis and guarding its source axis. |
| R3 (P1): mixed-validity JSON could publish bounds excluding retained records. | `ff669d61` | Reject temporal writes if any parsed JSON record has missing or unusable timestamps, including nested-key collisions. Continue skipping syntactically corrupt JSON discarded by readers. Preserve committed content and retry offsets after rejection. |
| R4 (P2): pruning old versions could remove columns needed for planning. | `945d938d` | Discover the complete query-selected live schema independently of row pruning, including native multi-source reductions. Retain older-only columns with NULLs in newer chunks; exclude orphan/superseded sidecars even for empty scans. Prefer retained native representatives of each schema fingerprint. |

### Original R1-R4 validation completed

Targeted suites passed during the fixes; this was not a full workspace test run
or a live performance measurement. Counts below overlap and must not be summed.

| Area | Completed coverage |
|---|---|
| SQL preservation | 9 query-foundation library tests and 9 `phase5_sql` integration tests; parsing, renaming, hidden CTE changes, precision loss, grouped SQL, and declared view boundaries. |
| Source axes and materialization | Latest materialization selection: 5 tests, including mismatched/unknown axes, separate appended chunks, historical-only columns, append/idle passes, and homogeneous-schema bounded-read checks. |
| Derived reductions | 4 dynamic-lineage tests; 7 pump-state tests, including safe source-axis fallback; 8 join/pivot tests during R1. |
| Ingestion | 31 provider logfile-ingest tests and 16 TLogFS logfile-ingest tests; reader-backed mixed-record regression, committed-state preservation after failed appends, corrected retry, and idle behavior. |
| Schema/cache/reduction | 69 provider tests covering format cache, version cache, format-provider selection, and temporal reductions. After the final native-schema deduplication refinement, the 5 materialization tests and native-glob regression passed again. |

Validation ran from `watertown` with
`CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0`.
`cargo fmt --all -- --check`, `cargo clippy --workspace --all-features -- -D warnings`,
and `git diff --check` passed. Companion YAML parsing and locality-mapping checks
also passed. This historical record predates the A1-A3 implementation below.

### Companion outer commits, not published in this batch

| Commit | Change |
|---|---|
| `88b8e20` | Enable RFC3339 bounds for measurement ingestion. |
| `f4d8c43` | Declare Azure-access projection timestamp-local. |
| `f6e4c22` | Declare limiter aggregation timestamp-grouped. |
| `42e5d2a` | Document the one-day rollup backlog limit. |
| `0418c1c` | Wire analytics-staging metrics into monitoring. |
| `cadead6` | Declare selfmon input timestamp mappings and mark analytics' rounding query global without changing its results. |

The main configuration is `config/watershop-selfmon.yaml`. The inventory
regression is `config/scripts/test-selfmon.py` and uses Python plus PyYAML.
It requires every Terraform-configured instance to be monitored and requires
ingest/projection/join inventories to agree. Complete extra monitoring entries
are allowed so deployment inventory and retained monitoring history can evolve
separately. The pending Terraform analytics definition is not a prerequisite
for the standalone F5 commit.

## Architectural follow-ups: same-branch implementation, not yet published

The review found the layering, shared-cache reuse, conservative pruning, and
immutable-identity rules consistent with the project. The original findings and
acceptance criteria are retained below, followed by the implemented responses.
A1 exposed a correctness problem in the pipeline's retry behavior; A2 and A3
identified incomplete incremental-efficiency contracts.

Use the shared contracts in
[the query-foundation correctness record](timeseries-datafusion-correctness-plan.md),
especially sections 3.3, 6, 7, and 13.4. Do not solve these by discarding history,
adding selfmon-specific caches, weakening axis checks, or hiding failures.
Source references in the original findings describe the reviewed implementation
and may move.
Paths beginning with `config/` are in the outer repository.

### A1 (P1): materialization can silently omit recovered input

`materialize_series/mod.rs:491-553` filters source rows with a strict
`timestamp > target_watermark` predicate and records a rejecting repair policy,
but does not classify source changes before filtering them. Recording
`RepairPolicy::reject()` does not reject late rows that have already disappeared
from the input stream.

Selfmon deliberately continues after individual ingest failures
(`config/scripts/run-selfmon.sh:154-159,385-436`). A successful input can advance
the joined perf target beyond another input's failed batch. When that batch is
retried, its older measurements are permanently excluded from the materialized
target. Exact-timestamp grouping also does not prove that a group is complete
across future source appends.

The foundation requires retroactive changes to repair or fail explicitly;
maximum observed event time is not, by itself, a source-settled frontier.
Track source changes and classify them before applying append bounds, reusing
the foundation's frontier/materialization contracts. New upsert behavior is not
implicitly approved: unsupported late changes must produce an actionable error
without advancing progress, rather than disappear.

Acceptance: cover one failed input while another advances the target, followed
by successful ingestion retry; changes exactly at the watermark; and a later
contribution to an existing timestamp group. Verify repair or explicit rejection,
preserved committed state on failure, and ordinary append/idle behavior.

### A2 (P2): raw-format inputs still lose materialization bounds

All three selfmon materializers ultimately consume `jsonlogs://`: perf through
its typed projections and join, limiters through exact-time grouping, and Azure
access through journal projection. `Provider::event_time_column_for_url`
(`crates/provider/src/provider_api.rs:910-913`) returns unknown for external
formats. SQL-derived source registration consequently clears the event-time
bound (`crates/provider/src/factory/sql_derived/mod.rs:1341-1368`), even for newly
ingested versions with valid bounds.

This fallback preserves results, but leaves F2/F4 incomplete for the actual
selfmon source-construction path. A SQL input-column declaration proves neither
the physical index nor the format transformation. Add a shared format-provider
capability establishing the relationship between recorded bounds and exposed
event time, while preserving conservative handling of genuinely unknown axes.
Later DataFusion row filtering does not undo unbounded provider preparation.

Acceptance: exercise actual perf, limiter, and journal timestamp representations
through their configured derived pipelines. Warm append and idle cases must
demonstrate source-version pruning and equality with the full-source result.
Unknown/mismatched mappings and legacy unknown-bound versions must remain safe
and visible; do not infer proof from a matching column name.

### A3 (P2): cached-format schema discovery still scales with history

R4 correctly separates logical schema membership from row pruning. However,
`CachedSet` includes every live sidecar in `schema_files`, and fresh provider
construction opens every member's Parquet footer to reconstruct the schema
(`crates/provider/src/version_cache.rs:280-297,394-396,489-496`). This happens
even for unchanged schemas and empty bounded scans. The native schema-fingerprint
deduplication does not cover this cached-format path.

Preserve the complete query-selected live schema, but maintain reusable schema
metadata in the shared cache rather than reconstructing it from every historical
sidecar on each warm construction. Exact live membership remains authoritative;
orphan/superseded files must not contribute, and missing required members must
not silently narrow the schema.

Acceptance: measure footer/schema-discovery work across growing retained
raw-format histories with a fixed schema and bounded append/idle queries.
Include fresh provider contexts, older-only columns, schema changes, empty row
selections, cache reconstruction, and missing/superseded sidecars. Warm footer
work must not grow with every historical version; output equality alone does
not satisfy the performance contract.

### Implemented responses

| ID | Local implementation |
|---|---|
| A1 | Track recursive source lineage and exact target identity in a non-authoritative shared-cache checkpoint. Use the foundation's change classifier for proven timestamp-local appends. Verify the affected historical prefix before accepting potential late changes; compare multisets so duplicates and changed exact-time groups cannot disappear. Reject differences before opening the publication sink. |
| A2 | Add a shared format event-time capability. Newly ingested logfile and journal versions record timestamp column and unit. `jsonlogs` proves RFC3339 and integer-microsecond mappings, and SQL validation checks the declared parsing conversion against that encoding. Unknown mappings and unproven transforms remain conservative. |
| A3 | Persist a deduplicated Arrow schema index beside immutable shared sidecars. Fresh provider contexts reuse it without reopening historical schema footers. The exact live file list still determines membership, including empty bounded scans and older-only columns; missing required files or corrupt metadata fail explicitly. |

A1 deliberately does **not** implement replacement/upsert. Missing checkpoints,
changed recipes, missing target bounds, source removals/replacements, and
unproven change locality require full-prefix verification. This work is warned
and counted as non-incremental. A consistent old target can establish a
checkpoint; an inconsistent target fails and requires explicit reconciliation
or rebuild. Checkpoint loss does not bypass this check. Cached checkpoints
name the exact target snapshot, so rollback or target changes invalidate reuse.
Unchanged, tracked snapshots skip source execution entirely.

A3's first construction or index reconstruction reads the required live schema
footers. Warm construction still enumerates live metadata and checks file
existence; it is not claimed to have constant total CPU or filesystem work.
It avoids repeated historical footer discovery, without narrowing the schema
to the retained row files or including orphan sidecars.

### A1-A3 validation completed

These are local results, not a full workspace test run or live measurement.
Counts overlap; targeted reruns are not additional distinct tests.

| Area | Completed coverage |
|---|---|
| Shared foundation | 9 library tests, 10 `phase5_sql` tests, and 11 `phase6_materialize` tests; encoding-aware timestamp preservation, duplicate multiplicity, and schema evolution during prefix verification. |
| Provider and TinyFS | 386 provider library tests and 231 TinyFS library tests passed. The legacy-target regression was rerun after adding explicit coverage for targets without temporal metadata. |
| Real storage | 7 materialization regressions and 16 logfile-ingestion regressions passed. Covers committed join-input recovery, unchanged target state after rejection, same-watermark/group changes, global-window prefix changes, and ordinary bounded appends. |
| Selfmon format paths | RFC3339 perf projections/join, exact-time limiter grouping, and microsecond journal projection match full-source results. Physical-plan assertions cover append and empty idle bounds: old indexed chunks are excluded while unknown-bound legacy chunks remain. The final expanded pipeline regression passed again. |
| Cached schemas | Histories of 1, 16, and 128 sidecars reuse metadata across fresh contexts with zero warm schema-discovery footer reads. Cache reconstruction, missing files, corrupt metadata, schema evolution, and empty scans are covered. The schema regression passed again after the lint refinement. |

`cargo fmt --all -- --check`,
`cargo clippy --workspace --all-features -- -D warnings`, `make docs-check`,
and `git diff --check` passed. Documentation generation refreshed the stale
inventory for the earlier local ingestion/input-axis changes; documentation
checks include 13 passing regression tests. Validation used
`CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0`.
The macOS linker emitted its large `__eh_frame` compact-unwind warning; it did
not fail the builds or tests.

The A1-A3 changes and both supporting reports are recorded together in the local
follow-up commit, still unpublished. The subsequent publication check confirmed
that the PR head is unchanged. No merge, deployment, historical metadata rewrite,
or submodule pointer update was performed during this implementation.

These are pre-merge implementation changes, not claims about live Watershop
health or measured runtime improvement. The original review was read-only;
the R1-R4 validation record above is separate from validation of these changes.
The human retains responsibility for merging the PR.

## Important implementation boundaries

- F1 preserves physical-series stream-tail and collapse semantics. Selecting
  the highest version alone is not equivalent to selecting the live stream tail.
- F2 retains strict row filtering after the conservative source hint. Unknown
  axes are a deliberate fallback; nanosecond and negative-timestamp handling
  must not be simplified into lossy microsecond-only filtering.
- External-format axes require explicit format and recorded metadata proof.
  `jsonlogs` supports the RFC3339 and microsecond encodings used by selfmon;
  other encodings remain unproven. An SQL input mapping alone does not prove a
  physical index. Indexed legacy versions without timestamp-unit proof still
  force conservative full event-time reads.
- RFC3339 parsing is explicit, not automatic reinterpretation of numeric data.
  Perf logs use field `ts`; limiter logs use `timestamp`.
- `locality.input_event_time` defaults to output `event_time`. Selfmon perf
  projections now map `ts`, and Azure access maps `__REALTIME_TIMESTAMP`.
  Supported conversions include RFC3339-to-nanosecond casts and integer
  epoch-microsecond parsing. Analytics' `floor(...)` conversion remains
  unchanged but is declared `global`, not timestamp-preserving.
- `timestamp-grouped` allows grouping on one exact typed event-time key. It is
  not permission for arbitrary aggregates, time bucketing, windows, or limits,
  and does not add late-data repair semantics to append-only materialization.
  This restriction does not excuse silent omission of late input; see A1.
- Configured temporal ingestion now fails the whole write on a parsed record
  with missing/malformed timestamps; it does not silently drop that record or
  store an unbounded version. Fix the source/configuration before retrying.
- Bounds select data rows, not the logical schema. An older-only native schema
  may require an older Parquet footer read; fingerprints avoid redundant reads
  where retained chunks already describe it. Cached formats merge schemas from
  explicit live sidecars, and missing schema members fail rather than narrow
  the schema silently. A3 reuses persisted schema metadata for those sidecars.
- The Azure-access query runs locally over journal-derived storage summaries.
  "Global" versus "timestamp-local" describes dependency behavior, not whether
  the query runs in Azure.
- One day means **sealable backlog**, excluding the allowed-lateness tail.
  With the default one-day lateness, sparse hot data is roughly one to two days,
  subject to cadence/granularity. The first build of an oversized old hot window
  can still be expensive.

## Legacy timestamp metadata remains a separate decision

Newly ingested versions can carry bounds; historical unknown-bound versions
remain conservatively included. No backfill or migration was implemented.
Unknown-bound versions do not prevent pruning proven modern versions because
the old versions themselves are always retained. Indexed legacy raw versions
without unit proof instead prevent format-axis proof; deployment alone does
not make those histories fully incremental.
The review fixes also do not repair previously ingested versions whose bounds
may already be too narrow; any such historical data needs separate assessment.

Do not patch committed bounds in place. Event-time bounds and logical attributes
participate in series leaf hashes:
`crates/sync-store/src/content/series_leaf.rs:694-724,842-874`.
TLogFS's `set_extended_attributes` changes pending rows and re-stamps identity
before commit; it is not a historical-edit API. `pond maintain` does not provide
this bounds-repair operation.

After A1-A3 are published and an approved rollout occurs, measure residual work
from legacy timestamp metadata, then decide whether migration is justified.
This separate migration decision does not defer the pre-merge follow-ups. Verified
non-authoritative hints in the shared cache or an explicit logical migration
are possible future designs, not approved or implemented solutions.

## Baseline and eventual runtime comparison

The recorded inspection was around 2026-11-13 02:51-03:01 UTC, with native
`pond 0.301.222`, revision `281187ae228aa082c3bda22ceb9798cd8e97e2cd`.
It is historical evidence, not the current host state.

- A nominal one-minute tick took roughly an hour.
- Six completed ticks each reported about 93 GB of logical reads (`rchar`),
  but only roughly 1-12 MB of physical reads.
- Tiny perf appends of 145-273 bytes incurred roughly 0.73-0.81 GB of logical reads.
- Finest perf and limiter rollups took about 801 and 656 seconds respectively.
- Output reuse already worked: 114 export partitions reused, 10 written.
- The hot window was about 26 days with 943 eligible source versions.

The script intentionally runs a full-history journal `COUNT(*)` benchmark.
That workload remains unchanged and must be accounted for separately. Neither
its cost nor all unwrapped work is an incremental-update defect.

After an approved rollout, compare several ordinary warmed ticks with history
and workload intact. Record actual executable revision and configuration hashes,
per-step logical reads and timing, rollup source counts/frontiers, export reuse,
failures, and analytics visibility. Distinguish a one-time catch-up build from
steady-state work; fast failure is not an improvement.

Original evidence locations:

```text
User journal: pond-selfmon@watershop-selfmon.service

/var/log/watertown-selfmon/watershop-selfmon/
  .tick-status.json
  .sitegen-last.json
  .sitegen-last.log
  _self.jsonl

Selfmon pond:
  cache/merged_*/res*/manifest.json
```

Re-identify active processes and cache namespaces rather than reusing old PIDs
or assuming old namespace names are still active.

## Safe continuation

1. When publication is authorized, push the four local Watertown review-fix
   commits to the existing PR branch. Recheck CI and artifact identity for
   `945d938d` or its successor; the published head's successful jobs do not
   validate the unpushed fixes. Resolve the dependency-audit blocker from its
   evidence rather than treating the PR as fully green.
2. Leave PR merging to the human. Publishing the PR did not authorize deployment.
3. Review publication of the outer configuration separately. Its branch contains
   earlier analytics commits and is behind recorded `origin/main`; it is not
   an isolated selfmon branch.
4. When Watershop is available and deployment is explicitly approved, install a
   supporting binary **before** applying RFC3339/grouped-locality/input-mapping
   configuration.
   Confirm the actual native `/usr/bin/pond`, not only a container tag.
5. Measure warmed behavior and monitoring health. Only then decide whether
   historical-bound enrichment is necessary.

Follow the pair's `AGENTS.md`: report both branch states before changes; never
create/switch branches, amend, rebase, or merge PRs. Stay inside this pair. A
submodule-pointer bump requires explicit approval and a dedicated commit.

Do not reset caches, discard/re-ingest authoritative logs, rewrite old metadata,
slow the timer, or suppress missing-node errors to make the stress test appear
healthy. Outage recovery and deployment require their own authorization.

## Preserve pending local work

Neither index had staged paths at this refresh. Watertown has no tracked
uncommitted source changes; the investigation report and this handoff remain
untracked. Updating this handoff does not add a PR commit.

The outer worktree still contains earlier analytics deployment/content changes,
separate from the committed selfmon fixes:

```text
README.operations.md
config/analytics/site/overview.md
config/noyo.yaml
config/site.yaml
config/scripts/pond.sh
config/scripts/run.sh
config/scripts/test-beacons.py
site/content/monitoring.md
terraform/station/watershop/.gitignore
terraform/station/watershop/Caddyfile
terraform/station/watershop/terraform.tfvars.example
terraform/station/watershop/variables.tf
terraform/station/watershop/watershop.tf
watertown                         (unstaged gitlink)
IDEA                              (untracked)
config/analytics/site-pond.yaml.tftpl  (untracked)
config/scripts/install-caddy.sh       (untracked)
config/scripts/run-analytics.sh       (untracked)
```

Preserve these files and keep both handoff/report documents unstaged unless
explicitly asked otherwise. Do not use broad staging to publish the selfmon
configuration.
