# Reusing pond sessions across multi-step workloads

Status: proposed.

## 1. Summary

Watertown's command model is intentionally simple: files name data, executable
factory nodes name computations, and `pond run PATH [ARGS...]` executes one
factory. That model is a good user interface, but the current CLI gives every
`pond run` invocation a completely separate process, pond open, transaction,
provider state, commit, and post-commit sequence.

That cost is acceptable for the ordinary water, septic, and noyo collectors,
where one `run` executes the whole collection factory. It is a poor fit for
`watershop-selfmon`, whose shell tick composes many small factories. A steady
selfmon tick currently launches about 51 `pond` processes. Twenty-seven of them
open the selfmon pond; eleven separate invocations each open the same pond and
commit one small metric feed.

Measurements from the native `0.160.223` selfmon deployment make the
amplification visible:

| Per completed tick | Observed |
|---|---:|
| userspace bytes read (`rchar`) | 3.52 GB average |
| read syscalls (`syscr`) | 1.27 million average |
| block-device bytes read | 0 |
| block-device bytes written | 65.8 MB average |
| wall-clock duration | approximately 9-10 minutes |
| selfmon data footprint | approximately 125 MB |

The zero block reads mean the Linux page cache is effective. It does not make
the work free: cached reads still consume CPU, perform syscalls, parse Delta
metadata, rebuild query state, and repeatedly reconstruct the same logical
view.

This document proposes a reusable **pond session** that can execute an explicit
multi-step plan while keeping correctly versioned in-memory state. Transaction
boundaries remain explicit. A first implementation can preserve one transaction
per factory while removing process and pond-open repetition; a later
transaction group can execute several compatible factories atomically.

The desired invariant is:

```text
cost(tick) <= fixed session overhead
              + O(changed input)
              + O(explicitly selected maintenance inputs)
              + O(changed export)
```

Cost must not scale with the number of commands in a shell script multiplied by
the full retained pond state.

## 2. The workload that exposed the problem

### 2.1 One selfmon tick today

`caspar.water/config/scripts/run-selfmon.sh` performs the following steady-state
sequence:

| Phase | `pond` processes | Selfmon pond opens |
|---|---:|---:|
| maintain/checkpoint/compact/prune | 1 | 1 |
| cursor check and journal read benchmark | 3 | 3 |
| probe nine ponds (`list`, `log`, `limits`) | 27 | 3 |
| ingest journal and Caddy sources | 2 | 2 |
| ingest `_self`, nine pond feeds, and limiter feed | 11 | 11 |
| copy four site templates | 4 | 4 |
| materialize perf and limiter series | 2 | 2 |
| generate the site | 1 | 1 |
| **Total** | **51** | **27** |

The other 24 pond opens inspect the eight non-selfmon ponds. They are a
secondary opportunity: a single inspection command per pond could replace
three opens. The larger immediate problem is repeatedly opening and writing the
same selfmon pond.

The eleven metric ingests illustrate the amplification. Each invocation reads
approximately 45-51 MB through userspace in order to append what is commonly
one small JSON row. Together they account for roughly 530 MB of reads per tick.

Maintenance is currently the largest single step:

| Maintenance metric | Recent steady range |
|---|---:|
| userspace reads | approximately 1.7-1.8 GB |
| read syscalls | approximately 600,000 |
| block writes | approximately 9-12 MB |
| wall-clock duration | approximately 5 minutes |

The maintained work remains physically regular (about 24 data files and 43
control files compacted per tick), so repeated metadata and state
reconstruction, rather than increasing selected input count, is a leading
suspect.

### 2.2 Current pond composition

At the time of measurement, the selfmon data table contained approximately:

| Population | Bytes | Files |
|---|---:|---:|
| data payload outside `_delta_log` | 56.0 MB | 303 |
| data `_delta_log` | 69.0 MB | 2,933 |
| checkpoint Parquet within `_delta_log` | 62.6 MB | 131 |
| JSON commits within `_delta_log` | 6.4 MB | 2,802 |

The log had not yet crossed its configured one-day retention boundary. That
matters for the eventual steady-state size, but it does not justify rereading
gigabytes during every tick.

### 2.3 What the counters mean

The selfmon wrapper snapshots `/proc/$$/io`. Linux rolls a waited-for child's
I/O counters into the parent shell, so the final line covers all completed
commands in the tick.

- `rchar` and `wchar` count bytes passed through read/write syscalls. They
  include page-cache hits and pipes and are not unique storage bytes.
- `syscr` and `syscw` count read/write syscalls.
- `read_bytes` and `write_bytes` count block-device traffic attributed by the
  kernel.

These measurements diagnose CPU and software I/O amplification even when a warm
page cache hides the cost from the physical disk.

## 3. Why separate `run` commands repeat work

`crates/cmd/src/commands/run.rs::run_pond_command` currently:

1. calls `ShipContext::open_pond`;
2. loads factory modes and pond metadata;
3. begins a write transaction;
4. resolves and reads one factory configuration;
5. constructs one `FactoryContext`;
6. executes one factory;
7. commits or aborts the transaction; and
8. returns, allowing the process and all remaining state to disappear.

Every shell invocation repeats that lifecycle.

There are two different kinds of reusable state, and the design must not
conflate them:

1. **Pond-session state.** `Ship` owns the open data persistence, control table,
   pond identity, and transaction-sequence state. Keeping one `Ship` alive
   avoids repeatedly opening the pond and reconstructing pond-wide metadata.
2. **Transaction state.** `tlogfs::State`, its TinyFS view, provider context,
   object store, and DataFusion `SessionContext` belong to a transaction.
   `TransactionGuard::commit` deliberately clears `persistence.state` and
   `persistence.fs`. Keeping the CLI process alive does not by itself make a
   transaction-local provider safe to reuse after a commit.

The steward commit path adds another reason to group intentionally. Every
content-changing write:

- writes the reserved node-manifest and commit-log nodes;
- commits the data Delta transaction;
- records the control-table lifecycle;
- reconciles transparency-log output; and
- dispatches post-commit factories and push remotes using new transactions.

Eleven tiny commits therefore cost more than one commit containing the same
eleven logical appends. They also produce eleven commit-log leaves, index
versions, and post-commit dispatch opportunities.

## 4. Terminology

### 4.1 Pond session

A **pond session** is one open `Steward::Pond`/`Ship` used for a bounded plan.
It may execute several sequential transactions. It owns only state that can be
made correct across transaction boundaries.

A session is not a transaction and does not imply atomicity.

### 4.2 Transaction group

A **transaction group** is an ordered list of factory executions inside one
write transaction. All steps commit together or abort together.

This is the direct equivalent of scripting several `run` commands in one
transaction, but it is only valid when later factories can correctly observe
earlier pending writes.

### 4.3 Plan

A **plan** is the declarative sequence executed by a session. It names:

- transaction groups;
- read-only or export steps between commits;
- required ordering;
- failure policy; and
- maintenance policy.

The plan is orchestration over existing executable factory nodes, not a
replacement for them.

## 5. Required invariants

### S1. Explicit transaction boundaries

No implicit rule may guess which factories belong in one transaction. A plan
must state its boundaries. This keeps transaction size, rollback scope, audit
history, and post-commit behavior reviewable.

### S2. Read-your-writes is proven, not assumed

If factory B consumes output written by factory A in the same transaction, B
must see the pending TinyFS state and pending file versions exactly once.
Provider table creation, object-store reads, and format caches must all honor
that view.

Until integration tests prove this for a factory pair, place the factories in
separate transactions within the same session.

### S3. Committed caches are version-keyed

Reusable caches must be keyed by the committed data/control Delta versions (and
other relevant identity such as pond ID), not merely by path. After commit:

- immutable content-addressed entries may remain;
- entries derived from the old committed snapshot must be invalidated or
  advanced;
- the next transaction must see the new tip; and
- an intervening external writer must be detected.

### S4. Post-commit dispatch occurs once per committed transaction

A transaction group gets one normal steward commit and therefore one
post-commit factory/remote sequence. The workflow executor must not invoke
child-command post-commit behavior after each nested factory.

Post-commit dispatch must not be broadly suppressed to make batching easier.
The existing suppression mechanism is a narrow recovery-import safety feature,
not a general workflow option.

### S5. No silent partial success

An atomic transaction group is fail-fast: one failed child aborts the group.
Plans may contain multiple transaction groups for deliberate failure isolation,
but a `continue_on_error` flag must not quietly commit a partially successful
atomic group.

If a post-commit export or maintenance step fails after data is durable, the
result must say both facts explicitly:

```text
data commit durable; export failed
data commit durable; maintenance failed
```

The process exits nonzero even though retry must not duplicate the committed
input.

### S6. The filesystem/factory model remains primary

Plans name the same executable paths users already run:

```text
/system/etc/journal
/system/etc/measure/water-prod
/system/etc/materialize-perf
/system/etc/sitegen
```

Factories do not gain private cross-factory APIs merely to participate in a
plan.

### S7. Session lifetime is bounded

This is not a proposal for a permanent daemon. A session lasts for one explicit
plan, then closes. Resource lifetime and failure recovery remain easy to
reason about.

## 6. Proposed execution model

### 6.1 First implementation: reuse the session, preserve commits

The lowest-risk first step is:

```text
open pond once
for each plan step:
    begin transaction
    execute one factory using existing semantics
    commit or abort
    refresh the session's committed view
close pond
```

This preserves the current transaction and failure behavior while removing
process startup, repeated pond opens, repeated control-table initialization,
and any snapshot/provider state that can safely move to session scope.

This phase is also a measurement tool. It tells us how much amplification comes
from opening the pond versus committing each small write.

### 6.2 Second implementation: explicit atomic factory groups

Once read-your-writes is covered, a plan may group compatible factories:

```yaml
version: watertown.session.v1
steps:
  - transaction:
      - run: /system/etc/journal
        args: [push]
      - run: /system/etc/caddy-access
        args: [push]
      - run_glob: /system/etc/measure/*
        args: [push]

  - transaction:
      - run: /system/etc/materialize-perf
      - run: /system/etc/materialize-limiters

  - export:
      run: /system/etc/sitegen
      args: [build, "${env:SITE_OUT}"]
```

The exact syntax is an open decision. The semantic requirements are more
important:

- glob expansion is deterministic and logged;
- every expanded factory path and argument appears in transaction metadata;
- groups are ordered;
- a group is all-or-nothing;
- each commit refreshes the session view; and
- export reads a named committed version.

### 6.3 Why a multi-transaction plan belongs above an ordinary factory

An ordinary factory receives a `FactoryContext` for an already-active
transaction. It should not commit that transaction and begin another one.
Therefore:

- a factory can implement an atomic nested factory group inside its existing
  transaction; but
- a plan containing several commits, maintenance, and committed-snapshot export
  must be executed by the steward/command layer that owns transaction
  boundaries.

The plan itself may still live as a normal file in the pond. The executor can be
a CLI command that reads the plan and invokes existing factory paths:

```text
pond session /system/etc/selfmon-session
```

This preserves the filesystem interface without giving provider factories
control over steward lifecycle.

## 7. Recommended selfmon lifecycle

A safe initial selfmon plan is:

```text
open one selfmon pond session

1. preflight maintenance if due

2. collect external measurements
   - other ponds may initially remain separate sessions

3. begin ingest transaction
   - journal push
   - Caddy push
   - _self metric push
   - all per-pond metric pushes
   - limiter metric push
   commit once

4. begin materialization transaction
   - materialize perf
   - materialize limiters
   commit once

5. begin read transaction pinned to the resulting committed version
   - generate site into a staging directory
   - close read transaction
   - atomically publish staged site

6. run threshold-driven postflight maintenance if due

close session
```

Keeping ingestion and materialization separate initially avoids assuming
read-your-writes. If tests prove that materializers see pending ingest versions
correctly, both groups can become one transaction.

Static site templates should not be copied every tick. Deployment should update
them when their content digest changes. If runtime synchronization remains
necessary, copy all changed templates in one transaction.

### 7.1 Export failure semantics

Site generation should read committed data. If it fails:

- the ingest/materialization commits remain durable;
- the previously published site remains in place;
- the staged output is retained or removed according to an explicit policy;
- the plan exits nonzero; and
- the next plan can retry export without re-ingesting source data.

This is preferable to rendering from uncommitted writes directly into the live
site directory, which can publish a view of data that later fails to commit.

### 7.2 Other-pond probes

`measure-pond.sh` currently executes `list`, `log --limit 1`, and `limits` as
three processes for each pond. A separate read-only inspection API can return
the required bounded metrics in one open:

```text
pond inspect --format json \
    --txn-tip --file-counts --size --list-timing --limits
```

This optimization is independent of selfmon session reuse and can follow it.

## 8. Session cache architecture

### 8.1 What may be reused

The session may retain:

- open `Ship`/`OpLogPersistence` ownership;
- immutable configuration bytes keyed by node content identity;
- factory registry state;
- content-addressed decoded objects;
- Delta snapshots keyed by table version;
- DataFusion planning/runtime structures whose inputs are immutable and
  version-keyed; and
- provider format-cache entries already keyed by physical content.

### 8.2 What must remain transaction-local

The following state must not leak blindly across commits:

- TinyFS pending operations;
- working-directory views containing uncommitted nodes;
- transaction metadata and sequence;
- mutable object-store overlays;
- table providers built over a particular pending version set; and
- any cache whose key omits committed table version or content identity.

### 8.3 Advancing after commit

Today `TransactionGuard::commit` clears transaction `State` and `FS`. A reusable
session therefore needs an explicit advance operation:

```text
CommittedView {
    pond_id,
    data_delta_version,
    control_delta_version,
    content_tip,
}

session.advance(commit_outcome)
```

The advance operation installs the new committed table state, invalidates stale
derived providers, and retains only immutable/version-keyed cache entries.

### 8.4 Concurrent writers

Reusing a `Ship` after releasing a write transaction is unsafe if another
process can commit before the next step and the session assumes its cached tip
is still current.

The executor must choose one of two explicit policies:

1. hold a session-level writer lease for the coherent write portion of the
   plan; or
2. reacquire the normal transaction lock and revalidate/refresh both table
   tips before every transaction.

Holding a writer lock across a long site export is undesirable. The recommended
shape is to hold it only through the write groups, commit, release it, and have
export read an immutable committed version.

## 9. Maintenance lifecycle

### 9.1 Do not rely only on graceful close

"Maintain on close" is useful but insufficient. Destructors and shell traps do
not run after SIGKILL, host loss, or some OOM failures. Selfmon previously
demonstrated exactly this failure mode: maintenance at the end of a tick could
be skipped, allowing uncheckpointed versions to accumulate until the next read
failed.

The session should combine:

1. a cheap preflight check before expensive reads;
2. threshold-driven maintenance after commits or on graceful close; and
3. retry on the next open when maintenance remains due.

### 9.2 Thresholds, not unconditional full work

Automatic maintenance should select work from bounded indicators:

- uncheckpointed Delta tail length;
- removable log age/retention boundary;
- count and size of small live Parquet files;
- control-row retention horizon;
- physical-series pack layout threshold; and
- explicit elapsed-time backstop.

The check itself must not scan all retained history. Its cost must be bounded by
the latest checkpoint/tail and compact metadata indexes.

### 9.3 Post-commit failure reporting

Maintenance may fail after the user data transaction is durable. It cannot
rollback that transaction. The plan result and control records must distinguish:

- transaction aborted before commit;
- transaction committed, maintenance completed;
- transaction committed, maintenance deferred because thresholds were not met;
- transaction committed, maintenance failed and must retry.

No path reports success when required maintenance failed.

## 10. Failure model

| Failure point | Durable data | Export | Required result |
|---|---|---|---|
| child factory inside atomic group | none from group | unchanged | abort group; nonzero |
| group commit | none from group | unchanged | abort/recover; nonzero |
| later transaction group | earlier groups remain | unchanged | report exact durable frontier; nonzero |
| site build | all prior commits remain | old site remains | retain/discard staging explicitly; nonzero |
| site publish rename | all prior commits remain | old or new atomically | nonzero unless new site is confirmed |
| postflight maintenance | all prior commits remain | new site may exist | mark maintenance retry due; nonzero |
| process killed mid-transaction | existing recovery rules | old site remains | recover on next open |
| process killed after commit | commit remains | possibly old site | resume from persisted plan frontier |

For resumability, a plan must derive progress from durable outputs and
watermarks, not from a success-shaped local marker written before commit.

## 11. Observability

Session execution should emit one structured summary per step, transaction, and
plan:

```text
pond_session_step plan=selfmon step=ingest:journal transaction=1 \
    outcome=ok elapsed_s=... rchar=... write_bytes=...

pond_session_transaction plan=selfmon transaction=1 factories=13 \
    outcome=committed delta_version=... txn_seq=... elapsed_s=...

pond_session_summary plan=selfmon transactions=2 factories=15 \
    outcome=ok elapsed_s=... rchar=... write_bytes=...
```

Required counters include:

- pond opens;
- transaction begins, commits, aborts, and no-ops;
- factory count;
- committed Delta versions and transaction sequences;
- provider/snapshot cache hits and misses;
- userspace and block I/O;
- syscall counts when available;
- elapsed time and peak memory;
- maintenance candidates selected and files actually read/written; and
- export files reused, written, and removed.

The production acceptance criterion is a plateau: at a stable update rate and
after retention fills, these values must stop increasing with total transaction
history.

## 12. Performance targets

For the measured approximately 125 MB selfmon pond:

| Metric | Current | Initial target | Strong target |
|---|---:|---:|---:|
| userspace reads per tick | 3.5 GB | 150-300 MB | 20-100 MB |
| read syscalls per tick | 1.27 million | less than 100,000 | 10,000-50,000 |
| block writes per tick | 66 MB | 50-70 MB with full site rewrite | 2-15 MB incremental |
| wall-clock duration | 9-10 minutes | less than 1 minute | 10-30 seconds |
| maintenance reads | 1.7-1.8 GB | less than 100 MB | selected inputs plus bounded metadata |

These are engineering targets, not correctness thresholds. The non-negotiable
property is that cost remains bounded by current changes, retained windows, and
explicitly selected maintenance work.

## 13. Implementation plan

### Phase 0: retain the diagnostics

Keep the current process-level I/O summaries while implementation proceeds.
They provide the baseline and catch regressions that wall-clock tests miss.

### Phase 1: extract factory-path execution

Move the reusable body of
`cmd::commands::run::run_pond_command_impl` behind an internal API that:

- accepts `&mut steward::Transaction`;
- resolves one factory path;
- expands environment references;
- resolves mode/arguments;
- constructs `FactoryContext`; and
- executes through `FactoryRegistry`.

It must not open, commit, abort, or dispatch post-commit work. Those remain with
the caller that owns the transaction.

### Phase 2: add a session executor with existing commit boundaries

Add a plan executor that opens one `Steward`, executes one factory per
transaction, refreshes committed session state after every commit, and records
step/transaction summaries.

Run the selfmon-shaped integration workload and quantify the improvement before
adding atomic grouping.

### Phase 3: add explicit transaction groups

Allow several factory-path executions against one transaction. Add
read-your-writes tests for:

- logfile ingest followed by a read of the appended suffix;
- several logfile ingests writing distinct nodes;
- journal/Caddy/metric ingest combinations;
- one child failure after earlier pending writes;
- exactly one commit-log leaf and post-commit dispatch; and
- deterministic command metadata containing every expanded child.

### Phase 4: make session caches version-aware

Promote only safe immutable/version-keyed state out of transaction scope. Add
tip revalidation and stale-cache tests, including an intervening external
writer between session transactions.

### Phase 5: committed-snapshot export

Run sitegen against an explicit committed snapshot, write to a staging
directory, and atomically publish. Record reuse/write counts so a full 51 MB
rewrite cannot hide.

### Phase 6: threshold-driven maintenance

Replace unconditional full maintenance with a cheap due check and bounded work
selection. Preserve preflight retry so failed graceful-close maintenance never
wedges the next session.

### Phase 7: consolidate cross-pond probes

Add one bounded inspection command per pond to replace the current
`list`/`log`/`limits` triple.

## 14. Required tests

### Correctness

- A one-factory plan matches `pond run` output and transaction history.
- A grouped transaction commits all child writes atomically.
- A failed child leaves no group writes visible.
- Later factories see earlier pending writes only when the API promises
  read-your-writes.
- Post-commit factories and remotes run exactly once per group commit.
- A no-op group does not create a content commit.
- Export sees the intended committed version.
- Export failure does not rollback committed ingestion.
- Maintenance failure is loud and retries on the next session.
- Crash recovery preserves the exact durable transaction frontier.

### Cache safety

- A committed snapshot is never reused under a different Delta version.
- An external writer between transactions forces refresh or fails the session
  with a clear stale-tip error.
- Aborted transaction state never enters session caches.
- Content-addressed immutable cache entries remain reusable across commits.

### Efficiency

Use a production-shaped integration test with at least:

- ten small logfile-ingest nodes;
- two materializations;
- one export;
- enough Delta history to require a checkpoint; and
- unchanged and changed-input cycles.

Assert:

- one pond open per plan;
- the declared number of transactions;
- one post-commit dispatch per content-changing transaction;
- no per-factory full pond reopen;
- unchanged cycles avoid unnecessary writes; and
- operation counts plateau after retention and maintenance thresholds settle.

Absolute byte thresholds should be generous enough for implementation changes,
but the test must reject cost proportional to retained transaction count.

## 15. Decisions and open questions

### Recommended decisions

1. Implement session reuse before transaction grouping.
2. Keep transaction boundaries explicit in the plan.
3. Begin with separate ingest and materialization transactions.
4. Export only committed snapshots and publish atomically.
5. Run post-commit dispatch once per actual commit.
6. Use threshold-driven maintenance with preflight retry.
7. Preserve executable factory paths as the plan's unit of composition.

### Open questions

1. Is the user-facing command `pond session`, `pond batch`, or another name?
2. Is the plan a new schema stored in an ordinary pond file, or an attachment
   with a dedicated entry type?
3. Which provider paths already support read-your-writes, and which require
   an explicit pending-state overlay?
4. Should a session hold a writer lease across all write groups or revalidate
   before each group?
5. Which DataFusion/provider caches are safe to promote to session scope?
6. How is a committed snapshot pinned for export while later writers proceed?
7. What persisted state records an interrupted plan's resumable frontier?
8. Which maintenance indicators can be read without scanning retained
   history?

The first implementation should answer these questions with instrumentation and
tests rather than optimistic cache reuse.
