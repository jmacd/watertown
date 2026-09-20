# Post-commit live monitoring

> **Status:** The first static-monitoring slice is implemented. It evaluates
> committed pond data after a write, publishes a local HTML and JSON status,
> then runs remote backup. Alert delivery and durable monitor-event series are
> deferred.

## 1. Decisions

The first implementation uses these explicit choices:

- A committed read-only snapshot is opened after the primary write commits.
  The original write transaction, filesystem, providers, and plans remain
  closed.
- The snapshot is opened from the `OpLogPersistence` returned by commit. Its
  Delta table already contains the new snapshot, so post-commit discovery does
  not reopen or rescan the pond.
- Read-only and read-write post-commit factories are distinct capabilities.
  Read-only factories share the committed snapshot; existing mutating
  factories continue to receive separate write transactions.
- `/system/run/*` remains lexically ordered. The water monitor is
  `/system/run/00-monitor`, so it runs before other factories. Remote backup
  runs after the complete factory queue.
- Generated monitoring artifacts belong to the host publication directory,
  not the pond namespace. They do not require a second pond commit and are not
  included in pond backup.
- A monitoring failure does not prevent backup. Factory execution continues,
  all configured remotes are attempted, and the command then returns a visible
  error containing every post-commit failure.
- The initial low-well rule uses the raw `well_depth_value` field. It alarms
  when at least one value exists in the trailing three hours and every
  observed value is strictly below 40 m. Gaps and sample age do not suppress
  this first rule. No samples produce `unknown`, not an alarm.
- The chlorine-feed rule uses raw `chlorine_level_value` observations and the
  canonical one-minute pump-state series. It accumulates only positive gauge
  deltas while the preceding aligned pump state is `pumping`; refill drops are
  ignored.

These choices implement the requested small local status page without
weakening transaction coherence or prematurely introducing a long-running
service, notification transport, or second monitoring database.

## 2. Transaction coherence boundary

TLogFS write commit closes the transaction's shared `CoherenceState` and
clears the transaction `State` and filesystem. Existing TinyFS handles,
cached providers, registered DataFusion tables, and plans from that state must
continue to fail as closed. Post-commit monitoring does not add an exception.

`tlogfs::TransactionGuard::commit` returns the underlying
`&mut OpLogPersistence` after installing the finalized Delta snapshot in its
table handle. Steward immediately calls `begin_read` on that persistence with
the committed sequence. This creates a new:

- TLogFS `State`;
- TinyFS filesystem;
- `ProviderContext`;
- DataFusion session and object-store registration; and
- open coherence generation.

The read snapshot is therefore fresh and usable while the original write
snapshot remains permanently closed. Closing post-commit execution commits
the read guard as a read no-op, which closes this second context too.

The cross-persistence contracts remain the underlying correctness baseline:

- `tinyfs::testing::persistence_contract` verifies independent handles,
  transaction-global writer exclusion, version and range reads, cache
  invalidation, and stale-provider rejection for memory and TLogFS.
- `provider::testing::assert_series_read_after_write` verifies real DataFusion
  read-after-write and provider rebuilding for both backends.
- `crates/tlogfs/src/tests/persistence_contract.rs` verifies commit, abort,
  fresh-snapshot durability, and closure of old providers.
- `crates/steward/tests/test_post_commit_factory.rs` verifies that a read-only
  post-commit factory sees a file from the just-completed commit and does not
  allocate another write sequence.
- `crates/cmd/tests/monitor_post_commit.rs` commits measurement, condition, and
  monitor-factory nodes through a real TLogFS `Ship`, then verifies post-commit
  `rate-while` JSON/HTML output and confirms that monitoring allocates no
  second write sequence.

## 3. Post-commit dispatch

The successful write path is:

```text
primary write transaction
  -> finish pending writers and queries
  -> commit Delta data
  -> close original coherence state
  -> record DataCommitted
  -> open fresh read snapshot from returned OpLogPersistence
  -> discover and sort /system/run/*
  -> execute factories in lexical order
       read-only factory  -> shared committed read snapshot + host side effects
       read-write factory -> independent post-commit pond write transaction
  -> close shared read snapshot
  -> attempt every push-mode remote backup
  -> return success, or one combined visible post-commit error
```

The shared read snapshot represents the primary parent commit. A read-only
factory does not observe pond writes made by an earlier read-write
post-commit factory in the same queue. This is deliberate: observers report
the committed source transaction, not a moving sequence of secondary commits.
The initial monitor sorts first, so this distinction is unambiguous for water.

Factory registration declares automatic access:

```rust
register_executable_factory!(
    name: "monitor-report",
    description: "...",
    post_commit: read_only,
    validate: validate_config,
    initialize: initialize,
    execute: execute
);
```

Existing executable factories default to `ReadWrite`. Steward invokes
read-only factories with `ExecutionMode::ControlReader`; manual `pond run`
continues to use `PondReadWriter`.

## 4. Monitor configuration

Each pond carries its own `/system/run` configuration. The first water
configuration is:

```yaml
version: v1
kind: mknod
metadata:
  path: /system/run/00-monitor
spec:
  factory: monitor-report
  config:
    pond: "${env:POND_INSTANCE}"
    title: "Caspar Water status"
    output_dir: "${env:MONITOR_OUTPUT_DIR}"
    checks:
      - id: well-depth-low
        label: "Well depth below 40"
        source: "oteljson:///ingest/casparwater*.json"
        timestamp_column: timestamp
        value_column: well_depth_value
        unit: m
        threshold: 40
        window: 3h
      - id: chlorine-feed-response
        label: "Chlorine feed responds while well pump runs"
        type: rate-while
        measurement:
          source: "oteljson:///ingest/casparwater*.json"
          timestamp: timestamp
          column: chlorine_level_value
          accumulation: positive-deltas
        condition:
          source: "series:///pump-state/well-pump-state"
          timestamp: timestamp
          predicate:
            column: phase
            operator: eq
            value: pumping
        alignment:
          method: previous
          tolerance: 2m
        window: 24h
        minimum_active_time: 30m
        alarm:
          operator: lt
          value: 2.5
          unit: sensor-units-per-pump-hour
```

The raw field is `well_depth_value`, not `well_depth_level`. It is measured in
meters. The source is the read-only OTel JSON projection over physical files
already ingested under `/ingest`; monitor execution performs no ingestion or
other external I/O.

Configuration validation rejects:

- missing pond, title, output directory, or checks;
- duplicate or path-unsafe check IDs;
- empty source or column names;
- non-finite thresholds; and
- invalid or zero windows, evidence durations, or alignment tolerances.

Environment references are stored verbatim in pond history and expanded at
execution. Expansion errors are returned; post-commit execution no longer
falls back to unexpanded configuration bytes.

## 5. Initial water rule

For each check, the factory:

1. fixes `period_end` to the monitor execution time;
2. calculates `period_start = period_end - window`;
3. asks the provider for an event-time-bounded source beginning at
   `period_start`;
4. applies exact row predicates for
   `[period_start, period_end)` and non-null values in DataFusion;
5. converts timestamps to UTC microseconds and values to `Float64`;
6. rejects non-finite values explicitly; and
7. classifies the collected observations.

Classification is:

```text
no observations                         -> unknown
all observations have value < threshold -> alarm
one or more values >= threshold          -> healthy
```

Equality at 40 m is healthy because the configured condition is strictly
"below 40". Null values are not observations. Observation gaps do not affect
the first rule, as explicitly selected; the page still displays sample count,
first and last observation times, latest value, minimum, and maximum so an
operator can see sparse coverage.

## 6. Rate-while rules

`rate-while` is a typed monitor, not user-supplied SQL. Its current vocabulary
is deliberately bounded:

- `accumulation: positive-deltas`;
- condition predicate `operator: eq`;
- alignment `method: previous`; and
- alarm `operator: lt`.

The measurement query uses the reusable DataFusion window function:

```sql
counter_delta(CAST(chlorine_level_value AS DOUBLE))
  OVER (ORDER BY timestamp)
```

`counter_delta` returns null for the first row or a null predecessor, the
increase for a positive delta, and zero for an unchanged or decreasing value.
It rejects non-finite inputs. Watertown registers the function in every
DataFusion session that receives the TinyFS object store, so monitor SQL and
other pond queries share the same semantics.

For every consecutive measurement interval, the monitor finds the latest
condition sample at or before the interval start. The interval is usable only
when both the condition age and measurement interval length are within the
configured tolerance. This attributes the delta ending at `t` to the state
during the preceding interval rather than the state observed at `t`.
Qualifying elapsed seconds and positive deltas are summed separately:

```text
rate = 3600 * accumulated_positive_change / active_seconds

active_seconds < minimum_active_time -> unknown
rate < alarm.value                   -> alarm
otherwise                            -> healthy
```

Actual elapsed time is used; row counts are never treated as minutes. The
status evidence includes active time, required active time, accumulated
change, rate, and aligned/unaligned interval counts.

The water threshold was calibrated from 90 days of raw half-minute chlorine
observations and canonical one-minute pump state. Across 2,140 rolling
24-hour windows having at least 30 pump-active minutes, the median positive
rate was 3.82 sensor units per pump-hour. A 2.5 threshold isolated one
continuous 42-hour low-response period on July 15-16, 2026 and no other
period in that history. This is intentionally conservative relative to the
normal distribution rather than a guessed constant.

## 7. Publication protocol

Each run produces:

```text
/var/www/monitor/<pond>/
  index.html
  status.json
```

`status.json` is versioned with `schema_version: 2` and contains:

- pond and report title;
- generation time and committed transaction sequence;
- overall `healthy`, `alarm`, or `unknown` state; and
- per-check rule, source, unit, threshold, window, sample count, time bounds,
  latest value, minimum, and maximum. Rate checks additionally include their
  condition source, active and required evidence time, accumulated change,
  calculated rate, and alignment counts.

The HTML is self-contained and refreshes once per minute. It does not fetch
the JSON file, so a reader cannot observe a partially updated asset graph.
Each artifact is written to a unique temporary sibling, flushed with
`sync_all`, and renamed over its destination. JSON is published first and
HTML last; the HTML rename is the page-publication boundary. The output
directory is synced after both renames.

## 8. Watershop deployment

Terraform creates `/var/www/monitor/<instance>` owned by the Watertown user
and writes these per-instance environment values:

```text
POND_INSTANCE=<instance>
MONITOR_OUTPUT_DIR=/var/www/monitor/<instance>
```

For containerized ponds, `pond.sh` bind-mounts the host output directory at
`/monitor` and overrides `MONITOR_OUTPUT_DIR=/monitor` inside the container.
The monitor therefore has write access only to its own publication directory,
while pond input data remains under the normal pond mount.

Caddy serves the common root with path stripping:

```caddyfile
handle_path /monitor/* {
    root * /var/www/monitor
    header Cache-Control "no-cache"
    file_server
}
```

The initial pages are consequently available on Watershop's local HTTP
listener at:

```text
/monitor/water-staging/
/monitor/water-prod/
```

Resetting an instance removes and recreates its monitor directory so an old
status page cannot survive a pond reset.

## 9. Failure and audit policy

The primary data commit is already durable before monitoring starts and cannot
be rolled back by a post-commit error.

For every discovered factory, Steward records a pending post-commit record.
A read-only factory records only a parent `PostPushCompleted` or
`PostPushFailed` terminal record because it has no child data transaction.
Read-write factories retain their existing child transaction lifecycle.

Failures are accumulated rather than short-circuiting:

- one factory failure does not prevent later factories;
- factory discovery, environment expansion, execution, publication, and audit
  failures are all surfaced;
- remote backup is attempted after the factory queue even when monitoring
  failed; and
- the command returns an error after backup when either phase failed, joining
  both errors when necessary.

This preserves backup availability without allowing an unattended command to
report success after a failed status publication.

## 10. Deferred work

The first slice intentionally does not provide:

- missing-data, freshness, or maximum-gap alarms;
- a requirement for observations to cover the full three-hour window;
- hysteresis or a sustained healthy period before recovery;
- durable monitor evaluation and transition series in the pond;
- push notifications;
- a long-running status service;
- authentication beyond Watershop's existing local-network Caddy exposure;
- per-check deadlines, result-row limits, or memory budgets beyond existing
  DataFusion and process limits; or
- periodic reevaluation when a producer run makes no data commit.

The last point matters for dead-man monitoring: a stopped source can also stop
new commits, leaving the last static page in place. The generated timestamp
makes that visible to a human, but automatic stale-data alerting requires a
separate periodic evaluator or a freshness-aware self-monitor. Alert push
should be added only after freshness and recovery semantics are chosen; it
should consume the already-published status rather than evaluate a different
rule.
