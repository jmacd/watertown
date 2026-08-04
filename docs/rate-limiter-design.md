# Rate Limiters: Governing Remote Resource Consumption

> **Status:** Design proposal (unimplemented). Motivated by a real incident:
> under-development code produced a runaway process that consumed remote
> resources and incurred unwelcome charges. Watertown's stated goal is a
> **low-cost, resilient** service, so the ability to bound what a pond may
> spend against a remote is a first-class safety property, not a tuning knob.
>
> This document proposes a general **rate-limiter factory** plus a small
> **limiter helper**, and applies the first instance of it to the remote
> backup push path (`push_content_to_remote`). A second application --
> bounding alert message volume -- is sketched to confirm the abstraction
> generalizes.

---

## 0. Problem statement

A pond that is attached to a remote (`/sys/remotes/<name>`) pushes its content
closure after every commit. The dispatch is automatic and unattended:

```rust
// crates/steward/src/guard.rs:410
self.run_post_commit_remotes().await;
```

`run_post_commit_remotes` (`crates/steward/src/guard.rs:1188-1312`) discovers
every attachment in push/both mode, opens a `ContentRemote`, and calls
`push_content_to_remote`. That function streams every not-yet-present external
blob to the remote:

```rust
// crates/steward/src/content_push.rs:118-136
for hash in &materialized.external_blobs {
    if remote.has_blob(*hash).await? { continue; }
    let reader = ship.data_persistence()
        .open_large_file_reader_by_hash(&hash.to_hex()).await?;
    remote.put_blob(*hash, reader).await?;
}
```

Every iteration is at minimum one `HEAD` against the object store
(`has_blob`, `crates/sync-store/src/content_remote.rs:280-286`) and, on a
miss, a multipart upload of the whole blob
(`put_blob`, `crates/sync-store/src/content_remote.rs:299-330`). The batched
`push_commit` (`content_remote.rs:169-190`) then writes one Delta commit
carrying every inline object.

**There is no bound on any of this.** Nothing in the repository implements
rate limiting, throttling, quotas, or byte accounting -- a survey for
`TokenBucket` / `governor` / `quota` / `budget` / `bytes_written` turns up
nothing but unrelated uses of the word "limit". A misbehaving ingest loop that
commits in a tight cycle will push on every commit, and on S3 each `HEAD`,
each `PUT`, and each transferred byte is billable. The failure is silent,
unattended, and compounds until someone reads an invoice.

`docs/remote-pond-preview-design.md §2.1` already establishes that transfer
volume against staging is substantial (a full clone of `s3://water-staging` is
~11.4 GB) and that the MinIO endpoint on `watershop:9000` is deliberately
throttled. We have a bandwidth *ceiling* by accident of network shaping; we do
not have a *budget*.

### 0.1 What we want

A declarative, per-pond object that says "this action may consume at most
**10 MiB/day**" (or 5 iops/second, or 1 GiB/day), which:

1. is **configured in the pond** like any other factory node, so it is
   versioned, replicated, and visible in the tree;
2. keeps its **running state outside the pond**, in the disposable control
   table, so limiter bookkeeping never becomes pond history and never
   participates in the content-addressed fold;
3. is **referenced by name (path)** from whatever it governs, so one limiter
   can be shared and new consumers can be added without new machinery;
4. **governs actions in the pond but never writes the pond.**

---

## 1. Why the control table is the right home for limiter state

The operator guide draws the line explicitly:

| Kind | Examples | Where it lives | Replicated? |
|------|----------|----------------|-------------|
| **Universal** | User data, factory configs, `/sys/remotes/<name>` attachment YAML | Data Delta table | Yes |
| **Per-replica** | Pond identity cache, remote modes, push/pull watermarks | Control table | No |

> "The **data table** is canonical and replicable. The **control table** is a
> per-instance cache and audit log; it is disposable and can be rebuilt from
> the data table."
> -- `docs/operator-guide.md:48-55`

Limiter state is per-replica by nature. Two replicas of the same pond spend
against *different* remotes from *different* network positions; a shared,
replicated consumption counter would be actively wrong. And a limiter window
is worthless after a few hours, so nothing is lost by making it disposable.

There is direct precedent: the push watermark is already a raw control-table
key written right after a successful push.

```rust
// crates/steward/src/guard.rs:1296-1302
ship.control_table_mut()
    .raw_config_set(&format!("last_pushed_tip:{}", attachment.url), &tip_hex)
    .await
```

The API is small and already public:

```rust
// crates/steward/src/control_table.rs:793-807
pub async fn raw_config_get(&self, key: &str) -> Result<Option<String>, StewardError>;
pub async fn raw_config_set(&mut self, key: &str, value: &str) -> Result<(), StewardError>;
```

**Decision L1.** Limiter windows live in the control table under raw config
keys of the form `limiter:<pond-path>`, e.g.
`limiter:/sys/limits/backup-bytes`. They use `raw_config_*` rather than
`get_setting`/`set_setting` (`control_table.rs:265-288`) because the latter
adds a `setting:` prefix and is the operator-facing knob namespace; limiter
windows are machine state, not operator settings.

### 1.1 Consequence: rebuild-control resets the window

`pond rebuild-control` reconstructs lifecycle rows from the data table's commit
history and warns:

> "settings were NOT recovered: re-attach remotes with `pond remote add` /
> `pond backup add`, then re-baseline `pond verify`"
> -- `crates/cmd/src/commands/rebuild_control.rs:36-40`

So a rebuild (or a wiped control dir) **fails the limiter open**: the window
starts empty and a full budget becomes immediately available. This is the one
place where the disposability we want for hygiene works against the safety
property we want for cost. See Open Question O1 (§8).

### 1.2 Consequence: control writes are Delta commits

`raw_config_set` writes a row to the control Delta table
(`crates/steward/src/inner_control/table.rs:266-294`). That is *not* cheap and
it takes the control write lock -- `attach-remotes.sh` in the caspar.water
config already documents an incident where "the concurrent push holds the
control `write.lock` so the next maintain cannot compact", wedging the pond.

**Decision L2.** A limiter performs **exactly one control read at open and at
most one control write at close**, per governed operation batch. It never
writes per-blob. Individual measurements accumulate in memory inside the
`Limiter` value and are folded into the persisted window once, at commit.
This is what "record update in control FS on commit" must mean in practice.

---

## 2. The `rate-limit` factory

### 2.1 Registration

Factories are compile-time entries in a `linkme` distributed slice, declared
through `register_dynamic_factory!` (`crates/provider/src/registry.rs:456-510`)
and described by `DynamicFactory` (`crates/provider/src/registry.rs:139-203`).
Only `validate_config: fn(config: &[u8]) -> TinyFSResult<Value>` is mandatory;
`create_directory`, `create_file`, `initialize`, `execute`, and
`apply_table_transform` are all optional.

A rate limiter is a **leaf config node**: it has no content to serve and
nothing to execute. It exists so that (a) the rate is declared in the pond and
replicated with it, and (b) other config can reference it by path.

**Decision L3.** `rate-limit` registers with the `file:` form, creating a small
read-only file node whose bytes are the canonical rendering of the parsed rate
(so `pond cat /sys/limits/backup-bytes` shows the effective policy). It
implements `validate_config` strictly and supplies no `execute`.

Placement: `crates/provider/src/factory/rate_limit.rs`, registered as
`rate-limit`. The name must not collide with a format provider or a builtin
scheme; `SchemeRegistry::find_conflicts` (`registry.rs:255-275`) already
asserts this in tests.

### 2.2 Configuration schema

Applied through the ordinary `pond apply` path. `ResourceDoc` / `MknodSpec`
(`crates/cmd/src/commands/apply.rs:58-89`) are `deny_unknown_fields`, and
`apply` calls `FactoryRegistry::validate_config` before instantiation
(`apply.rs:278-291`), so a malformed rate is rejected at apply time rather
than at push time.

**Decision L4a.** The **unit is one string; the magnitudes are numbers.**
`limit` and `burst` are YAML scalars, not embedded in prose. A single `unit`
field carries the dimension, the scale, and the period, and both magnitudes are
read in terms of it.

```yaml
version: v1
kind: mknod
metadata:
  path: /sys/limits/backup-bytes
spec:
  factory: rate-limit
  config:
    unit: MiB/day
    limit: 10
    burst: 1
```

```yaml
version: v1
kind: mknod
metadata:
  path: /sys/limits/backup-ops
spec:
  factory: rate-limit
  config:
    unit: iops/second
    limit: 5
    burst: 20
```

The Rust shape:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimitConfig {
    /// "<scale>/<period>", e.g. "MiB/day", "iops/second", "B/hour".
    /// Carries dimension (bytes vs. operations), scale, and period.
    pub unit: String,
    /// How many `unit`s per period are permitted.  `unit: MiB/day` with
    /// `limit: 10` means 10 MiB per day.
    pub limit: f64,
    /// Optional instantaneous allowance, in the **scale** component of
    /// `unit` only -- the period does not apply to a burst.  `unit:
    /// MiB/day` with `burst: 1` means 1 MiB may be spent faster than the
    /// smoothed rate.  Defaults to one period's worth of `limit`, i.e. a
    /// pure sliding window with no extra allowance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub burst: Option<f64>,
}
```

Keeping the magnitudes as YAML numbers means they are typed, comparable, and
templatable by ordinary YAML tooling; only the dimensional vocabulary needs a
parser. It also makes `burst`'s relationship to `unit` unambiguous, which the
`"20 iops"` string form obscured: `burst` shares the scale, never the period.

Parsing follows the `config_util.rs` house pattern
(`crates/provider/src/factory/config_util.rs:1-41`: `parse_yaml_config<T>()`
for bytes, `config_from_value<T>()` for an already-parsed `Value`). Only the
`unit` string needs a parser; it resolves, together with the two numbers, into:

```rust
/// The dimension a limiter governs.  This is the unit contract between a
/// limiter and its callers (§3.1); scale and period are not part of it.
pub enum LimitUnit { Bytes, Ops }

pub struct RateSpec {
    pub unit:   LimitUnit,
    pub amount: u64,      // limit x scale, in base units: bytes, or operations
    pub window: Duration, // the period `amount` applies over
    pub burst:  u64,      // burst x scale, in base units
}
```

**Decision L4b.** The `unit` grammar is `<scale>/<period>`, deliberately small
and explicit:

- Byte scales are **binary only**: `B`, `KiB`, `MiB`, `GiB`, `TiB`. Decimal SI
  byte units are rejected rather than silently reinterpreted -- an operator
  who writes `MB` gets an error naming `MiB`, which is cheaper than a
  1000-vs-1024 surprise in a cost control.
- Operation scales: `iops`, `ops`, `op` -- all synonyms for a dimensionless
  count of 1. (`iops/second` is therefore mildly redundant, which is fine; it
  reads naturally and the period is what actually carries the meaning.)
- Periods: `second`, `minute`, `hour`, `day` (and the abbreviations `s`, `m`,
  `h`, `d`). No `week`/`month` -- calendar arithmetic has no place here.
- The separator is `/` with optional surrounding whitespace.
- `limit` and `burst` may be fractional; they resolve to whole base units by
  rounding **down**, and a `limit` that rounds to 0 is rejected (a limiter
  that permits nothing is a configuration mistake, not a policy).

Rejecting rather than guessing is the house position; see
`docs/fallback-antipattern-philosophy.md`.

---

## 3. The limiter helper

The helper is the only thing consumers touch. It belongs in
`crates/steward/src/limiter.rs`, because it needs both a `Ship` (to resolve
the factory config node in the pond) and a `ControlTable` (to load and store
the window), and steward is where those meet.

```rust
pub struct Limiter {
    path:   String,       // pond path of the rate-limit node; the limiter's name
    spec:   RateSpec,     // parsed from the factory config in the pond
    window: Window,       // loaded from control; mutated in memory
    pending: u64,         // charged this session, not yet persisted
}

impl Limiter {
    /// Bind to the limiter at `path`, declaring the unit this caller
    /// charges in.  Resolves `path` in the pond, requires it to be a
    /// `rate-limit` factory node, parses its config, **checks the declared
    /// unit against the configured one**, and loads `limiter:<path>` from
    /// control.
    ///
    /// Errors if the path does not exist, is not a rate-limit node, or its
    /// unit disagrees with `expect` (§3.1).
    pub async fn open(
        ship: &Ship,
        path: &str,
        expect: LimitUnit,
    ) -> Result<Self, LimiterError>;

    /// Would charging `amount` exceed the policy as of `now`?  Pure: no I/O,
    /// no mutation.  Expired window entries are pruned first.
    pub fn check(&mut self, amount: u64) -> Result<(), LimiterError>;

    /// Charge `amount` against the in-memory window.  Call only after a
    /// successful `check` and a successful action.
    pub fn record(&mut self, amount: u64);

    /// Atomically fold `pending` into the persisted window.  Exactly one
    /// control-table write (Decision L2).  Call once, on commit.
    pub async fn commit(&mut self, ct: &mut ControlTable) -> Result<(), LimiterError>;

    /// Human summary for status output: used / limit / window / reset-in.
    pub fn state(&self) -> LimiterState;
}
```

Usage is `open` → (`check`, act, `record`)* → `commit`.

`amount` is always in **base units** — bytes, or operations. A caller never
scales, and never needs to know whether the policy is written as `MiB/day` or
`GiB/day`.

### 3.1 The unit contract, checked at bind time

A limiter and its caller must agree about what is being counted. Pointing a
byte limiter at an operation counter is not a runtime anomaly to be discovered
from a wrong-looking number — it is a configuration error, and it should fail
loudly the moment the two are bound together.

**Decision L10.** `Limiter::open` takes the unit the caller expects and
verifies it against the configured one. Agreement is required; disagreement is
`LimiterError::UnitMismatch { path, configured, expected }`.

```rust
// The push path counts bytes and operations, so it binds two limiters
// and says so:
let bytes = Limiter::open(ship, &a.limiter_bytes, LimitUnit::Bytes).await?;
let ops   = Limiter::open(ship, &a.limiter_ops,   LimitUnit::Ops).await?;
```

If an operator writes `unit: iops/second` into a node that the push path binds
as `LimitUnit::Bytes`, the bind fails with a message naming the path, what the
node says, and what the caller wanted. It never silently governs the wrong
quantity.

**The contract is the dimension only — not the scale, not the period.** This
is the important part of the decision:

- The **caller** owns the dimension. Code that transfers blobs knows,
  statically and permanently, that it is spending bytes. That is a property of
  the code, so the code declares it.
- The **operator** owns the scale and the period. Whether the budget is
  `10 MiB/day` or `2 GiB/hour` is a policy question that should be
  re-answerable by editing YAML.

Requiring the caller to match the full unit string would invert this: every
change of budget granularity would become a code change, and callers would be
coupled to policy they have no business knowing. Requiring nothing would let a
byte budget silently govern an operation count. Checking the dimension is
exactly the line between the two.

Because `LimitUnit` is a two-variant enum in the caller's source, the check is
also a compile-time-visible statement of intent at every bind site — you can
grep for who spends bytes.

### 3.2 Window representation

The literal reading of "load from control, append new measurement, check
before the action" is an append-only log of `(timestamp, amount)` pairs. That
is exact, but unbounded: a limiter in `iops` terms with a 1-day window and
thousands of blobs per push accumulates an entry per operation, and the whole
log is serialized into a control-table string on every commit.

**Decision L5.** The window is a **bucketed sliding window**: the period is
divided into a fixed number of fixed-width buckets, and a measurement is added
to the bucket containing its timestamp. Expired buckets are dropped on load.

```json
{
  "v": 1,
  "unit": "bytes",
  "window_us": 86400000000,
  "bucket_us": 900000000,
  "buckets": [[1767225600000000, 4194304], [1767226500000000, 1048576]]
}
```

- 96 buckets for a `day` window (15-minute granularity), 60 for `hour`
  (1-minute), 60 for `minute` (1-second), 20 for `second` (50 ms).
- Serialized size is bounded by bucket count, independent of operation count.
- Worst-case accounting error is one bucket's width of leading edge, i.e.
  ≤ ~1% over-permissiveness at the window boundary. For a cost guard that is
  well inside the noise; for the alternative (exactness) we would pay
  unbounded state.

`check(amount)` is then: prune buckets older than `now - window`, sum the
survivors, and require `sum + pending + amount <= amount_limit`, with `burst`
governing the additional allowance available within a single bucket.

**Decision L6.** The limiter is **advisory-fail-closed within a run and
fail-open across a control loss**. Concretely: if the window loads, the limit
is enforced strictly; if the control key is absent (fresh pond, or post
`rebuild-control`), the window is empty and the full budget is available. It
does *not* refuse to run because it has no history. See Open Question O1.

---

## 4. Applying it: the remote attachment

### 4.1 Schema change

`RemoteAttachment` (`crates/steward/src/remote_config.rs:38-65`) gains one
optional field, a **map keyed by dimension**:

```rust
    /// Limiters governing transfers to this remote, keyed by the unit the
    /// push/pull path spends: `bytes` and/or `ops`.  Each value is the pond
    /// path of a `rate-limit` factory node, whose configured `unit` must
    /// agree with the key (§3.1).  An absent key means that dimension is
    /// ungoverned -- an empty map is today's unlimited behavior.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub limits: BTreeMap<String, String>,
```

```yaml
limits:
  bytes: /sys/limits/backup-bytes
  ops:   /sys/limits/backup-ops
```

This follows the existing convention exactly: every optional field on that
struct is `#[serde(default, skip_serializing_if = ...)]`, so an attachment
without limits serializes byte-identically to today's YAML and existing
`/sys/remotes/<name>` documents keep parsing.

**Decision L7.** A map keyed by dimension, not a single path and not
a bare list. The units contract of §3.1 makes this the only shape that stays
honest:

- With a single `limiter: String`, the push path spends in **two** dimensions
  (bytes transferred, requests issued) but could only declare one. The other
  would go ungoverned or, worse, get charged into a limiter of the wrong
  dimension — precisely what L10 exists to prevent.
- With a bare `Vec<String>`, the caller could not declare anything at bind
  time; it would have to open each node, read its unit, and *infer* the
  binding. That inverts the contract — the configuration would be telling the
  code what it meant, rather than the code stating its expectation and having
  it checked.
- With a map, **the key is the caller's declaration, written by the
  operator.** `limits.bytes` is bound as `LimitUnit::Bytes` and the node's
  `unit:` is verified against it. The operator states the intent, the code
  states the same intent, and the two are checked against each other. An
  unrecognized key is rejected at parse time, so `limits: {byte: ...}` is an
  error rather than a silently ignored policy.

This also closes Open Question O2 (bytes *and* ops simultaneously) rather than
deferring it, at no cost in the common case: a remote with no limits carries no
`limits` key at all.

> **Compatibility caveat.** `RemoteAttachment` is `#[serde(deny_unknown_fields)]`
> (`remote_config.rs:39`) and the attachment YAML **is replicated** (it is
> universal state per `operator-guide.md:48-52`). A *new* binary reads *old*
> YAML fine, but an *old* binary reading YAML written by a new binary with
> `limits` set will fail to parse. Rollout must therefore be: deploy the binary
> everywhere first, set limits second. This is the same ordering constraint any
> `deny_unknown_fields` schema addition carries here, and is worth stating in
> the release notes.

### 4.2 Authoring: `pond apply`, not CLI flags

The limiter binding is **policy**, and policy belongs in the versioned YAML
that describes the deployment — not in an argv array assembled by a shell
script. Today, though, `/sys/remotes/<name>` has exactly one writer:
`add_remote_attachment_internal` (`crates/cmd/src/commands/remote.rs:145-196`),
fed entirely by clap flags from `RemoteAddOptions` (`crates/cmd/src/main.rs:186-206`).

This was a deliberate D6 choice, and it is worth restating why, because the
proposal here partially reverses it. Remotes used to be an ordinary mknod
factory node — `factory: remote` with `mode:` and a `config:` block
(`docs/archive/remote-backup-design.md:930-945`) — and D6 replaced that with
two imperative verbs:

> "The pre-D6 factory-based remote model (`pond sync`, `pond remote add <yaml>`,
> `/sys/run/<N>-backup`, `import:` config sections, `pond context`) is gone."
> -- `docs/operator-guide.md:13-17`

Two things a passive config node could not do drove that:

1. **Secret discipline.** The attachment YAML replicates, so a literal
   `secret_access_key` would leak to every replica. `add_remote_attachment_internal`
   enforces an `${env:VAR}` reference imperatively, with a long explanatory
   error (`remote.rs:190-196`).
2. **Live validation at attach time.** Attaching creates or verifies the remote
   Delta table and checks the remote's `store_id` against the local `pond_id`
   *before* any pond-side state changes, closing a gap that otherwise surfaced
   as `delta error: Not a Delta table` on the first push (`remote.rs:214-240`).

**Decision L11.** Keep both properties; move the *authoring surface* to YAML.
`pond apply` gains `kind: backup` and `kind: remote`, whose `spec` is the
attachment document itself, and which route through the **same**
`add_remote_attachment_internal` — so the secret check and the store_id/Delta
validation still run, unchanged, on exactly the same code path.

```yaml
version: v1
kind: backup
metadata:
  path: /sys/remotes/origin
spec:
  url: s3://water-staging
  region: ${env:S3_REGION}
  endpoint: ${env:S3_ENDPOINT}
  access_key_id: ${env:S3_ACCESS_KEY}
  secret_access_key: ${env:S3_SECRET_KEY}
  allow_http: true
  limits:
    bytes: /sys/limits/backup-bytes
    ops:   /sys/limits/backup-ops
```

```yaml
version: v1
kind: remote
metadata:
  path: /sys/remotes/water
spec:
  url: ${env:WATER_S3_URL}
  mount: /sources/water
  limits:
    bytes: /sys/limits/import-bytes
```

Notes on the shape:

- `metadata.path` is the attachment's location, matching how every other
  `pond apply` resource identifies itself (`crates/cmd/src/commands/apply.rs:73-79`);
  the remote's logical name is its basename.
- `kind` carries the direction, which is the one thing the two verbs really
  distinguished (`RemoteMode::Push`/`Both` vs. `Pull`). `bidirectional: true`
  on a `kind: backup` selects `Both`; `mount` is required for `kind: remote`
  and rejected on `kind: backup`, mirroring the existing verb signatures
  (`remote.rs:64-145`).
- `apply` already expands `${env:...}` and deliberately stores the *raw*
  reference text in the pond (`apply.rs:293-297`, and the same discipline in
  `mknod.rs:28-58`), which is exactly the behavior the secret rule needs.
- `apply` is already idempotent-by-overwrite and multi-document, so the whole
  attach set for an instance is one file.
- Each `limits` entry is validated at apply time: the path must exist, must be
  a `rate-limit` node, and its `unit` must match the key. Failing here is much
  better than failing on the first unattended post-commit push.

Deployment wiring lives in the outer repo. `config/scripts/attach-remotes.sh`
currently reconstructs each attachment as a bash array of flags and re-runs it
with `--overwrite` on every deploy; under this design it collapses to a
`pond apply` of a per-instance YAML file, with the `case` over pond type
becoming a choice of which file to apply. Note that `watershop-selfmon`
deliberately has **no** backup remote (documented at length in that script) and
so needs no limits.

**The CLI verbs stay.** `pond backup add` / `pond remote add` remain the
interactive path and keep working; they simply gain no `--limiter` flag. An
operator attaching a remote by hand gets an ungoverned remote, which is the
current behavior and is fine for the interactive case. Limits are a deployment
property, and deployments go through `pond apply`.

### 4.3 Enforcement point

In `push_content_to_remote` (`crates/steward/src/content_push.rs:62-163`),
binding both dimensions up front so a unit mismatch fails before any remote
I/O:

```rust
let mut bytes = attachment.limits.get("bytes")
    .map(|p| Limiter::open(ship, p, LimitUnit::Bytes)).transpose().await?;
let mut ops = attachment.limits.get("ops")
    .map(|p| Limiter::open(ship, p, LimitUnit::Ops)).transpose().await?;

for hash in &materialized.external_blobs {
    if let Some(l) = &mut ops { l.check(1)?; }              // the HEAD costs an op
    if remote.has_blob(*hash).await? {
        if let Some(l) = &mut ops { l.record(1); }
        continue;
    }
    if let Some(l) = &mut ops { l.record(1); }
    let size = blob_size_on_disk(ship, hash).await?;
    if let Some(l) = &mut bytes { l.check(size)?; }
    let reader = ship.data_persistence()
        .open_large_file_reader_by_hash(&hash.to_hex()).await?;
    remote.put_blob(*hash, reader).await?;
    if let Some(l) = &mut bytes { l.record(size); }
}
```

`check` before the action and `record` after it, exactly as specified: a
transfer that fails is not charged, and a transfer is never started that the
budget cannot cover.

**Blob sizes are available locally and cheaply.** `MaterializedObjects`
carries only hashes (`crates/steward/src/content_tree.rs:140-149`), but each
external blob has a local file, resolvable via
`tlogfs::large_files::find_large_file_path` (`crates/tlogfs/src/large_files.rs:172`)
-- the same lookup `open_large_file_reader_by_hash`
(`crates/tlogfs/src/persistence.rs:1005-1012`) and `fsck`
(`crates/steward/src/fsck.rs:299`) already perform. A local `metadata()` gives
the byte count with no remote round-trip. This matters: the reverted
remote-read spike had to add `ContentRemote::object_size()` precisely because
it lacked local knowledge; the push side does not have that problem.

The batched `push_commit` is charged as one op plus the summed length of the
inline object bytes, checked before the call and recorded after. Finally:

```rust
for l in [bytes.as_mut(), ops.as_mut()].into_iter().flatten() {
    l.commit(ship.control_table_mut()).await?;
}
```

one control write per bound limiter, at the end, alongside the existing
`last_pushed_tip:` write at `guard.rs:1296-1302`.

### 4.4 Behavior when the budget is exhausted

**Decision L8. Hard-fail the push.** The pond's data commit has already
happened and is durable; a push is a mirror operation that is safe to retry.
`push_content_to_remote` returns
`StewardError::RateLimited { limiter, used, limit, window, retry_after }`.

The two callers then differ appropriately, and this falls out of existing
structure with no new plumbing:

- **`pond push` (interactive):** the error propagates and the command exits
  non-zero with a message naming the limiter, the budget, and how long until
  enough window frees up. The operator sees it immediately.
- **Post-commit auto-push (unattended):** `run_post_commit_remotes` already
  logs and continues rather than failing the commit
  (`guard.rs:1310-1312`: "Continue with the next remote; one bad target
  shouldn't poison the others"). A rate-limited push therefore logs an error
  and is skipped -- the pond keeps accepting writes, and the *next* commit's
  push retries. Since the content closure is recomputed from scratch each
  push and `has_blob` skips what the remote already holds
  (`content_push.rs:118-124`), a retry naturally resumes rather than restarts.

That is the desired shape: **the limiter throttles spending, it does not
threaten availability.** A pond under a saturated limiter is a pond whose
backup is stale, which is a condition we can alert on -- not a pond that has
stopped working.

**Decision L9.** Rate-limited pushes must be *visible*. `pond status` gains a
line per limiter (used / limit / window / next reset), sourced from
`Limiter::state()`. A silent throttle would replicate the original failure
mode with the sign flipped.

### 4.5 Measuring what we govern (Decision L12)

A limit is only as good as the number it is set from, and we do not yet know
what a normal day of pond→remote traffic costs (§6, Phase 1). The plan is to
set limits generously, measure against them, and tighten later -- which
requires the *usage* to be visible, not just the *refusals*.

The window in the control table cannot serve that purpose. It is per-replica
and disposable by design (§1), so it is exactly the wrong place to keep a
number you intend to chart: a `rebuild-control` erases it, and nothing about it
survives to be compared week over week. Enforcement wants ephemeral state;
monitoring wants durable state. They are different requirements and they get
different homes.

So spending is also recorded into the pond, at a single shared series:

```
/sys/limits/usage
```

| column | meaning |
|--------|---------|
| `timestamp` | when the spending happened (event-time column, µs UTC) |
| `limiter` | pond path of the governing node, e.g. `/sys/limits/backup-bytes` |
| `unit` | `bytes` or `ops` |
| `amount` | units spent by that activity -- **already a delta** |
| `used` | sliding-window total afterwards |
| `limit` | the budget in force at the time |
| `window_secs` | the window length |

One series rather than one per limiter: monitoring wants a single table to
group by `limiter`, and a per-limiter series would multiply pond nodes with
every policy added. `amount` is a delta rather than a running total, so it
charts as a rate directly and is immune to the window being reset underneath
it; `used`/`limit` give the headroom figure to alert on. Carrying `limit` in
every row keeps a chart honest across a retune, rather than plotting last
week's usage against this week's budget.

**Why the emission is deferred.** Spending happens during the post-commit
push, and a push cannot write the pond: writing would commit, which would
push, which would spend, which would write. The accumulated samples are
therefore parked in the control table under `limiter-usage-pending` and
flushed into the pond at the **start of the next write transaction** --
piggybacking on a write the caller was going to do anyway.

That breaks the recursion and is self-limiting: the write that emits sample
*N* triggers a push that queues sample *N+1*, emitted by the following write.
A pond that stops being written stops emitting, which is correct, because an
idle pond is also not spending. (The pending key is capped so a long-idle pond
cannot grow it without bound; the newest samples win.)

**Why the queue drains at commit, not at read.** The rows are written through
the transaction, so they are durable only if that transaction commits -- and
that is exactly when the queue is drained. An aborted write re-emits its
samples next time rather than dropping them. The drain removes the emitted
prefix rather than clearing the key, so samples queued in between (notably by
the post-commit push that runs moments later) survive.

**A metric must never cause an outage.** Every failure on this path -- an
unreadable queue, a failed series append -- is logged and dropped. Refusing a
user's write because a counter could not be recorded would trade a monitoring
gap for exactly the kind of unavailability §4.4 is at pains to avoid.

Note that this makes the limiter's *first* pond write, which §3 said would
never happen. The rule it does not break is the one that matters: **the
limiter still never writes the pond as part of governing an action.** It
governs, records to control, and a later unrelated transaction carries the
observation out. Enforcement remains a pure function of pond-resident policy
and control-resident state.

---

## 5. Generalizing: the second limiter

The abstraction is worth building only if the second use costs almost nothing.
Bounding alert message volume:

```yaml
version: v1
kind: mknod
metadata:
  path: /sys/limits/alerts
spec:
  factory: rate-limit
  config:
    unit: ops/hour
    limit: 20
    burst: 5
```

The alert sender binds it, declaring its dimension:

```rust
let mut alerts = Limiter::open(ship, "/sys/limits/alerts", LimitUnit::Ops).await?;
```

then calls `check(1)` before sending and `record(1)` after, and `commit`s once
per batch. Nothing about the factory, the helper, the storage key convention,
or the control-table interaction changes. The only per-consumer work is (a)
deciding what a unit means for that action and (b) choosing where the config
field that names the limiter lives.

Note the unit check earns its keep immediately here: if someone pastes
`unit: MiB/day` into the alerts node, the alert sender refuses to bind rather
than counting messages against a byte budget.

This is the test the design has to pass, and it passes: **the limiter knows
nothing about remotes.**

---

## 6. Phasing

The ordering below deliberately puts measurement before enforcement. We do not
currently know what a normal day of pond→remote traffic costs, and a limit set
without that number is either useless or an outage.

**Phase 0 -- restore local backup coverage on staging.**
Not all watershop staging instances currently exercise MinIO local backup.
Re-run `attach-remotes.sh` for the producer instances so `water`, `septic`, and
`noyo` staging each push to their MinIO bucket again, and confirm pushes
succeed. No watertown code change. (`watershop-selfmon` stays remote-less by
design.)

**Phase 1 -- measure, do not limit.**
Add byte and operation accounting to `push_content_to_remote` and record the
per-push totals. This is the `Limiter` machinery running with no policy
attached -- or, more cheaply, a `PushMeter` that the limiter later consumes.
Run it for long enough to characterize each instance: bytes/day, objects/day,
blobs/day, and the shape of the distribution (steady trickle vs. spiky
re-push). `docs/remote-pond-preview-design.md §2.1` gives the pull-side
picture; this gives us the push-side one, which we have never measured.

**Phase 2 -- the factory and the helper.**
`crates/provider/src/factory/rate_limit.rs` and
`crates/steward/src/limiter.rs`, with unit tests, and no consumer. Testable in
isolation: rate parsing (including the rejections), window pruning across
bucket boundaries, burst behavior, control round-trip, and the
absent-key-means-empty-window path.

**Phase 3 -- wire the remote.**
`RemoteAttachment.limits`, the `kind: backup` / `kind: remote` resources in
`pond apply` (§4.2), enforcement in `push_content_to_remote`, `pond status`
reporting, and converting `attach-remotes.sh` to apply per-instance YAML. Roll
out with limits set generously (say 10× the Phase 1 measured p99) so the first
production exposure proves the plumbing without risking a stale backup. Tighten
afterwards. Note the rollout ordering forced by `deny_unknown_fields` (§4.1):
binaries everywhere first, `limits` in the YAML second.

**Phase 4 -- the second consumer.**
Alert volume, per §5, confirming the abstraction generalizes.

---

## 7. Testing

- **Unit parsing:** `MiB/day`, `iops/second`, `B/hour`, `GiB/d` resolve to the
  expected `(LimitUnit, scale, window)`; `MB/day`, `MiB/week`, `MiB`, `/day`,
  and `furlongs/day` are rejected with messages naming the accepted forms.
  `limit`/`burst` are read as numbers, so `limit: "10 MiB"` is a type error
  from serde rather than a parser special case.
- **Magnitudes:** `unit: MiB/day, limit: 10` yields `amount == 10 * 1024 *
  1024`; a fractional `limit: 0.5` with `unit: MiB/day` yields 524288; a
  `limit` that rounds to 0 is rejected; an omitted `burst` defaults to one
  period's worth. `pond apply` rejects all of these at apply time
  (`apply.rs:278-291`), not at push time.
- **Unit contract (§3.1):** binding a `unit: iops/second` node with
  `LimitUnit::Bytes` returns `UnitMismatch` naming the path, configured unit,
  and expected unit; binding a `MiB/day` node with `LimitUnit::Bytes` succeeds;
  changing a node from `MiB/day` to `GiB/hour` does **not** break any caller
  (scale and period are outside the contract).
- **Window:** charging exactly to the limit succeeds; one more base unit
  fails; advancing a mock clock past the window frees the full budget;
  advancing by half the window frees approximately half; bucket rollover does
  not lose or double-count.
- **Persistence:** `open` → `record` → `commit` → re-`open` observes the
  charge; `commit` performs exactly one `raw_config_set`; a missing control
  key yields an empty window rather than an error.
- **Push integration:** extend the existing `crates/steward/tests/content_push_test.rs`
  harness (which already builds a `ContentRemote::create_at(...)` against a
  temp dir) with an attachment carrying a tiny `limits.bytes`; assert that the
  first push succeeds, that a second push exceeding the budget returns
  `StewardError::RateLimited`, and that the remote holds exactly the objects
  from the first push -- i.e. the limiter stopped the transfer rather than
  corrupting it. Separately assert that `limits.bytes` pointing at an
  `iops/second` node fails the push *before* any remote I/O occurs.
- **Compatibility:** an existing `/sys/remotes/<name>` YAML with no `limits`
  key round-trips unchanged, and an attachment with no limits serializes
  without the field (`skip_serializing_if`); `limits: {byte: ...}` (misspelled
  dimension) is rejected rather than ignored.
- **Apply path:** `kind: backup` and `kind: remote` produce byte-identical
  `/sys/remotes/<name>` documents to the equivalent `pond backup add` /
  `pond remote add` invocations, and a literal (non-`${env:}`) secret is
  rejected through the apply path exactly as through the verb
  (`remote.rs:190-196`).

---

## 8. Open questions

**O1 -- fail-open after control loss.** Per §1.1, `pond rebuild-control` or a
wiped control directory resets every window to empty, handing back a full
budget. For a 10 MiB/day limiter that is a bounded, acceptable loss. For a
limiter that is the only thing standing between a runaway loop and a bill, a
reset that a runaway loop could *cause* (by wedging the pond into a recovery
path) is more troubling. Options: (a) accept it and rely on Decision L9's
visibility; (b) persist windows to a small sidecar file under the control
directory that survives `rebuild-control`; (c) treat an absent window as
"assume the window is full" for the first period after a rebuild, which is
maximally safe and maximally annoying. Leaning (a) for Phase 3, revisit with
real data.

**O2 -- ~~one limiter or several per remote~~. Closed** by Decision L7:
`limits` is a map keyed by dimension, so bytes and ops are governed
independently and simultaneously. Phase 1 measurement still decides whether the
`ops` key is worth *setting* on our workload, but the schema no longer needs to
change either way.

**O3 -- where the clock comes from.** Bucketing needs a monotonic-ish wall
clock, and the window is stored in absolute microseconds. A backwards clock
step would mis-bucket. The control table already stamps `ts_micros`
(`crates/steward/src/inner_control/table.rs:166-260`), so the pond has a
convention to follow; the limiter should use the same source and clamp
negative deltas to zero.

**O4 -- multi-process access.** Two pond processes against the same control
table could interleave load/modify/store and lose a charge. The control write
lock referenced in `attach-remotes.sh` mostly serializes this in practice, and
losing a charge is a bounded under-count rather than a correctness violation.
Worth confirming rather than assuming, once Phase 1 tells us how often
concurrent pushes actually happen.

**O5 -- what governs pulls.** This design covers the push path because that is
where the incident occurred. `pond pull` and the cross-pond import remotes
configured for the `site` instance also spend remote resources, and
`docs/remote-pond-preview-design.md §2.1` shows the pull side moving far more
bytes than the push side. The same `attachment.limits` map should govern
`content_pull.rs` -- the schema and the `kind: remote` apply resource already
carry it (§4.1, §4.2); only the enforcement calls are missing. Out of scope for
sequencing reasons, not design ones.

---

## 9. Summary of decisions

| ID | Decision |
|----|----------|
| L1 | Limiter state lives in the control table under `limiter:<pond-path>` via `raw_config_*`. |
| L2 | One control read at open, at most one control write at commit. Never per-operation. |
| L3 | `rate-limit` is a leaf file factory: `validate_config` + a read-only rendering; no `execute`. |
| L4a | Config separates the `unit` string from numeric `limit` and `burst`; `burst` shares the unit's scale, never its period. |
| L4b | `unit` grammar is `<scale>/<period>`; binary byte scales only; decimal SI and calendar periods rejected, not guessed. |
| L5 | Bucketed sliding window -- bounded state, ≤ ~1% boundary error, instead of an unbounded measurement log. |
| L6 | Strict enforcement when the window loads; empty window (full budget) when the control key is absent. |
| L7 | `RemoteAttachment.limits: BTreeMap<String, String>` -- limiter paths keyed by dimension (`bytes`, `ops`), optional. |
| L8 | Exhausted budget hard-fails the push; the data commit is already durable and the next push resumes. |
| L9 | Limiter state is reported by `pond status`; a silent throttle is not acceptable. |
| L10 | Callers declare the unit they spend at `Limiter::open`; a mismatch with the node's configured unit is a bind-time error. |
| L11 | Limiter bindings are authored in YAML via new `pond apply` `kind: backup` / `kind: remote` resources routed through the existing attach validation -- not via CLI flags. |
| L12 | Spending is *also* emitted into the pond as a durable metric series at `/sys/limits/usage`, flushed at the start of the next write transaction. |
