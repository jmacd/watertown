# Watertown Operator Guide

This guide is for the person who keeps a Watertown running: creating
ponds, attaching remotes, replicating, running maintenance, monitoring
health, and recovering from failures.

It assumes familiarity with the data commands (`pond copy`, `pond cat`,
`pond list`, `pond mknod`, `pond run`).  For the full command/flag
reference see [docs/cli-reference.md](cli-reference.md); for the design
rationale behind the remote model see
[docs/remote-redesign.md](archive/remote-redesign.md).

> This guide reflects the post-D6 CLI.  The pre-D6 factory-based remote
> model (`pond sync`, `pond remote add <yaml>`, `/sys/run/<N>-backup`,
> `import:` config sections, `pond context`) is gone; the historical
> version is archived at
> [docs/archive/operator-guide-pre-d6.md](archive/operator-guide-pre-d6.md).

---

## 1. Concepts

A **pond** is a transactional, query-native filesystem backed by Delta
Lake.  Every write is one atomic transaction recorded in the Delta
commit log.

A **remote** is an S3-compatible bucket or a local directory that holds
replicated copies of a pond's transactions as **bundles**.  Two kinds of
attachment exist, distinguished only by direction:

| Attachment | Command | Direction | Use |
|-----------|---------|-----------|-----|
| **Backup** | `pond backup add` | push (or push+pull) | Send this pond's writes to an offsite copy |
| **Remote** | `pond remote add` | pull | Receive another pond's data (mirror or import) |

A pull remote attaches at a **mount path**:

- **Mirror** (`PATH = /`): the remote's `store_id` equals this pond's
  `pond_id`.  This pond is a replica of the producer; pulled data lands
  as this pond's own data.
- **Cross-pond import** (`PATH = /imports/<name>`): the remote's
  `store_id` differs from this pond's `pond_id`.  The foreign pond's
  data is mounted read-through at the configured path, alongside this
  pond's own data.

### Universal vs. per-replica state

| Kind | Examples | Where it lives | Replicated? |
|------|----------|----------------|-------------|
| **Universal** | User data, factory configs, `/sys/remotes/<name>` attachment YAML | Data Delta table | Yes (push/pull) |
| **Per-replica** | Pond identity cache, remote modes, push/pull watermarks | Control table | No |

The **data table** is canonical and replicable.  The **control table**
is a per-instance cache and audit log; it is disposable and can be
rebuilt from the data table (see [§6 Recovery](#6-recovery)).

### Transaction lifecycle states

Each transaction is recorded in the control table as a sequence of
lifecycle records.  `pond log` and `pond log --txn-seq N` surface them:

| State | Meaning |
|-------|---------|
| `Begin` | Transaction started. |
| `DataCommitted` | The data Delta commit landed; this is the terminal record for a successful **write**. Carries the Delta version + partition checksums. |
| `Completed` | A read (or no-op write) finished cleanly. |
| `Failed` | The transaction aborted; the reason is in the record metadata. |
| `PostPushPending` / `PostPushStarted` / `PostPushCompleted` / `PostPushFailed` | Lifecycle of the post-commit auto-push of one write to one push/both-mode remote. |

A transaction with a `Begin` but no terminal record is **incomplete**
(the process crashed mid-write); `pond recover` resolves it.

---

## 2. On-disk and filesystem layout

```
{POND}/
  data/                     Data Delta table (the filesystem itself; replicable)
    _delta_log/             Transaction log + pond_txn commit metadata
    _large_files/           Content-addressed store for files >64KB
    pond_id=<uuid>/part_id=<uuid>/   Partitions (one per directory)
  control/                  Control table (per-replica; NOT replicated)
    _delta_log/
```

In-pond system paths:

```
/sys/                       Steward-owned (replicated)
  remotes/<name>            Remote/backup attachment YAML (one per attachment)
/system/                    cmd-owned factory nodes (replicated)
  run/                      Auto-executing factories (run after each commit)
  etc/                      Manually triggered factories (pond run)
  site/                     Static content (templates)
/...                        User data
```

> Note the two separate trees: **`/sys/remotes/`** holds remote
> attachments (managed by `pond remote`/`pond backup`); **`/system/run`**
> and **`/system/etc`** hold factory nodes (managed by `pond mknod`/`pond
> run`).  Do not confuse them.

---

## 3. Setup

### Create a pond

```
$ export POND=/data/mysite
$ pond init
```

`$POND` (or `--pond <path>`) selects the pond for every command.

### Attach a backup (push side)

A backup mirrors the whole pond to an offsite copy.  Push-only by
default; `--bidirectional` makes it push+pull.

```
# Local directory backup
$ pond backup add origin file:///backups/mysite

# S3 backup.  Credentials must be `${env:VAR}` references (single-quoted so
# the shell does NOT expand them): the attachment config is replicated to
# every backup, so a literal secret would be exposed on all replicas.  Each
# replica resolves the value from its own environment at use time.  A literal
# `secret_access_key` is rejected at attach time.
$ pond backup add origin s3://my-bucket/mysite \
    --region us-west-2 \
    --access-key-id '${env:AWS_ACCESS_KEY_ID}' \
    --secret-access-key '${env:AWS_SECRET_ACCESS_KEY}'

# MinIO / R2 / other S3-compatible endpoint
$ pond backup add origin s3://my-bucket/mysite \
    --endpoint http://minio.local:9000 --allow-http \
    --access-key-id '${env:AWS_ACCESS_KEY_ID}' \
    --secret-access-key '${env:AWS_SECRET_ACCESS_KEY}'
```

Attaching auto-initializes the remote (creates the Delta table stamped
with this pond's `pond_id`).  From then on, **every write transaction is
automatically pushed** to push/both-mode backups after it commits (the
steward scans `/sys/remotes/*` post-commit).  You only run `pond push`
manually to retry after a failure.

### Attach a pull remote (replica or import)

```
# Mirror restart: this pond is a replica of the producer (same pond_id)
$ pond remote add upstream s3://my-bucket/mysite /

# Cross-pond import: mount a foreign pond's data read-through
$ pond remote add septic s3://other-bucket/septic /imports/septic
```

The first `pond pull` from a cross-pond remote materializes the mount
entry at the configured path automatically.

### Bootstrap a full clone (`pond restore`)

To spin up a **fresh local replica** of a pond that is already published
to a remote -- the quickest, most reliable way to get real
production/staging data onto a workstation for development or preview --
use `pond restore`.  In one command it discovers the source pond's id
from the remote, stamps a local pond with that id, attaches the remote as
a mirror at `/`, and pulls the full content graph.  It **refuses to run
over an existing pond** (choose an empty `$POND` target), so it is safe
to script.

```
# Local directory backup
$ POND=~/preview-mysite pond restore origin file:///backups/mysite

# S3 / MinIO.  Same credential rules as `pond backup add`:
# `--secret-access-key` must be a single-quoted `${env:VAR}` reference, and the
# variable must be exported so the pond process can resolve it at use time.
$ export S3_SECRET_KEY=...
$ POND=~/preview-mysite pond restore origin s3://my-bucket/mysite \
    --region us-west-2 --endpoint http://minio.local:9000 --allow-http \
    --access-key-id caspar \
    --secret-access-key '${env:S3_SECRET_KEY}'
```

`restore` fetches the **entire object graph** reachable from the remote
tip (every reachable blob, tree, and commit) and verifies it folds to the
remote's root hash, so the first run transfers the pond's full live
content.  The graph fetch loads the remote's object index once rather than
per object, so a clone runs at roughly stream-once speed over the link
(e.g. a multi-GB staging pond over LAN MinIO clones in a few minutes, not
hours).

A restored clone is a normal, fully queryable pond -- `pond list`,
`pond cat <path> --sql "..."`, and factory runs all work against it with
no network access.  Keep it current with an incremental `pond pull origin`,
which transfers only the objects the clone is missing.

### List and detach

```
$ pond remote list                 # all attachments (push + pull)
$ pond backup list                 # push-side attachments only

$ pond remote remove upstream      # detach: drop YAML + watermarks
$ pond remote remove --purge septic  # also drop the cross-pond mount entry
```

### Preview an in-development site locally

To iterate on site content -- the blog or new analysis pages -- against
real data before deploying, build a **local preview pond**, generate the
static site from it, and serve it.  There are two shapes, depending on how
many data sources the site needs:

1. **Single-pond site (`pond restore`).**  If the site's content and data
   live in one pond, clone it with `pond restore` (above), then generate
   and serve:

   ```
   $ POND=~/preview-mysite pond restore origin s3://my-bucket/mysite ...
   $ POND=~/preview-mysite pond run /system/etc/90-sitegen build ./build
   $ (cd ./build && python3 -m http.server 8080)   # http://localhost:8080/
   ```

2. **Composite site (init + cross-pond imports).**  If the site stitches
   together several data ponds plus git-managed content (the usual shape
   for a public site), bootstrap a fresh site pond and pull each source
   in:

   ```
   $ POND=~/preview-site pond init
   $ POND=~/preview-site pond apply -f config/site.yaml   # git-ingest + import remotes + sitegen node
   $ POND=~/preview-site pond run /content pull            # site content from git
   $ POND=~/preview-site pond run /system/etc/10-water pull  # cross-pond data import (repeat per source)
   $ POND=~/preview-site pond run /system/etc/90-sitegen build ./build
   $ (cd ./build && python3 -m http.server 8080)
   ```

   The cross-pond `pull` steps fetch only the objects the local pond is
   missing, using the same one-shot object-index load as `restore`, so a
   first full import of a large staging pond completes in minutes.

The sitegen node path (`/system/etc/90-sitegen`) and the set of import
remotes are defined by the deployment's site config, not by the platform.
A deployment typically wraps these steps in scripts; the caspar.water
site, for example, keeps a `local/` harness (`setup.sh` -> `sync.sh` ->
`generate.sh` -> `serve.sh`, plus `refresh.sh` for fast content-only
re-iteration) -- prefer such scripts over running the commands by hand.

> Rebuild the site after each content change with the sitegen `build`
> run; use an incremental `pull` (git content and/or data) only when the
> upstream source has changed.  Only committed git content is visible to
> git-ingest, so commit local edits before regenerating.

#### Previewing sitegen asset (JS/CSS) changes

The chart engine assets (`overlay.js`, `vega-shared.js`, `chart.js`,
`style.css`, ...) are compiled *into* the `pond` binary with `include_str!`,
so editing one has no effect until the binary is rebuilt.  The site config's
SQL (e.g. a `sql-derived-series`) runs on whatever binary renders it, but a
chart's JavaScript ships only in a new image -- a local preview with a
freshly built binary is the only way to validate a chart change before merge.

1. Rebuild a **debug** binary (fast, incremental; do not build `--release`):

   ```
   $ cargo build -p cmd --bin pond
   ```

2. Render and serve with that binary; re-run `build` after every asset edit
   plus rebuild, and hard-refresh the browser:

   ```
   $ POND=./preview SITE_BASE_URL=/ ./target/debug/pond \
       run /system/etc/90-sitegen build ./out
   $ (cd ./out && python3 -m http.server 8080)
   ```

When the page under test is driven by a **derived series computed in a
separate data pond** (e.g. the caspar.water `analysis` exports are computed
in the *water* pond), you need not import that whole source.  Export each
series to Parquet once and inject it straight into a throwaway preview pond:

```
# On the host holding the data, dump each series the page reads
# (--format raw = Parquet bytes):
$ POND=/path/to/water/_data pond cat 'oteljson:///ingest/*.json' \
    --sql "$(cat series-query.sql)" --format raw > series.parquet

# Locally: minimal preview pond, then copy the series into its export tree:
$ POND=./preview pond init --birthplace preview
$ POND=./preview pond apply -f preview.yaml   # git-ingest content/templates + sitegen node
$ POND=./preview pond mkdir /sources/water /sources/water/analysis
$ POND=./preview pond copy host+series:///abs/path/series.parquet \
    /sources/water/analysis/<name>
$ POND=./preview SITE_BASE_URL=/ ./target/debug/pond \
    run /system/etc/90-sitegen build ./out
```

`preview.yaml` is a trimmed site config: git-ingest the content, templates and
images; an `exports:` stage matching the injected path (`/sources/water/
analysis/*`); and just the route(s) and sidebar for the page(s) under test.
Add the `explore:` block if the page's "Explore this data" cross-link needs
exercising.  Commit local template/content edits before regenerating (only
committed git content is visible to git-ingest); the injected Parquet is
local and needs no commit.

---

## 4. Routine operation

### Push and pull

```
# Manually push pending writes to push/both remotes (retry after failure)
$ pond push                # all push/both remotes
$ pond push origin         # one named remote

# Pull new bundles from pull/both remotes
$ pond pull                # all pull/both remotes
$ pond pull upstream       # one named remote
```

Push/both backups are pushed automatically after each commit, so
`pond push` is normally only needed to recover from a transient failure.
Pulls are always manual (or scripted via cron -- see [§7](#7-cron--systemd)).

### Maintenance

`pond maintain` runs Delta Lake housekeeping (checkpoint + vacuum) on the
**local** data and control tables.  `--compact` additionally merges
small parquet files into fewer large ones.

```
$ pond maintain
$ pond maintain --compact
```

`--compact` records the data-table merge as a **Compact transaction**, so
the next `pond push` emits a Compact bundle that backups can use as a
restart baseline (see [§6](#6-recovery)).  Push before (or right after)
compacting so the baseline reaches your backup.

> `pond maintain` operates on the local pond only; it does not push to or
> prune remotes.  Remote-side bundle retention is not currently exposed as
> a `pond` CLI command.

---

## 5. Monitoring

### Status

`pond status` is a fast, offline health aggregate (it reads only the
local control table and `/sys/remotes/*` -- no network).

```
$ pond status
Pond Status
===========

Identity
  Pond ID:   019e9b27-d392-7506-b875-b096dc5a41af
  Created:   2026-06-06 04:18:59 UTC by jmacd@host
  Location:  /data/mysite

Local state
  Last write seq:  4
  Recovery:        OK (no incomplete transactions)

Remotes (1)
  origin  [push]
    url:          s3://my-bucket/mysite
    mount:        / (mirror)
    last pushed:  4 (up to date)
```

Push "lag" (`up to date` / `behind local by N txn` / `never pushed`) is
computed from local watermarks.  To cross-check against what the remote
actually recorded, use `pond verify`.

### Verify

`pond verify` compares this pond's current live data against a remote's
recorded partition checksums.

```
$ pond verify origin
[OK] verify origin: live data matches remote at seq=4
```

A mismatch prints per-partition detail and the divergence boundary.

> Verify is symmetric: a replica bootstrapped from a remote verifies
> cleanly against it (the producer's `pond_init` transaction is replicated
> as a normal bundle, so the replica is byte-identical).

### Transaction log

```
$ pond log                 # recent transactions (default 10)
$ pond log --limit 50
$ pond log --txn-seq 42    # full lifecycle detail for one transaction
$ pond log --incomplete    # transactions that need `pond recover`
```

`pond log` reconstructs, for each write transaction, the originating CLI
command (from the `pond_txn` metadata in the data Delta commit), the
timing and status (from the control table), and a one-line change summary
(files/directories/partitions touched, from the data table).  Pure reads
are omitted; crashed writes appear under `pond log --incomplete`.

### Configuration

```
$ pond config              # pond ID, creation, factory modes, settings
$ pond config set <key> <value>
```

---

## 6. Recovery

### Crash recovery (incomplete transaction)

If a write crashes mid-flight, the next command reports an incomplete
transaction.  `pond recover` marks it resolved and restores consistency.

```
$ pond recover
```

### Rebuild a lost control table

If the control table is lost or corrupt (but the data table survives),
the pond can no longer be opened.  `pond rebuild-control` reconstructs
the control table from the data Delta table's commit history.

```
$ pond rebuild-control          # control table is missing
$ pond rebuild-control --force  # move an existing (corrupt) one aside first
```

It recovers pond identity and the transaction-log skeleton.  It does
**not** recover operator settings (remote modes, watermarks) or per-txn
checksums, so afterwards you must **re-attach remotes** and treat
`pond verify` as re-baselined.  See
[cli-reference.md](cli-reference.md#pond-rebuild-control-d63) for the
full recovery semantics.

### Consumer fell below the retention horizon

Under content-addressed sync there is no per-transaction bundle to prune,
so a consumer can always recover by re-fetching the objects reachable
from the remote's current tip:

```
$ pond pull upstream
```

A mirror re-fetch rebuilds the local content closure from the remote's
published tip; a cross-pond pull refreshes only the foreign pond's
footprint.

### Capsule recovery and storage-format migration

Use a portable capsule when a native backup format must be recovered by a
future Watertown version or migrated into a fresh pond. The current
`watertown.commit.v1` recovery recipe produces `pondcapsule.4`. V4 preserves
the schema identity of every table leaf and persisted dynamic-node timestamps.
`pondcapsule.1` through `.3` remain readable frozen compatibility formats, but
V3 and earlier cannot recover dynamic metadata they did not encode.

This is not the routine `pond pull` path. A capsule is a deliberate,
authenticated recovery boundary:

1. Quiesce every source writer and restart mechanism. For a format migration,
   use the current binary to freeze the source and record its exact tip:

   ```bash
   POND="$SOURCE_POND" pond freeze enable --reason "approved storage-format migration"
   POND="$SOURCE_POND" pond freeze status
   ```

   Old source binaries may not honor the freeze marker, so stopping their
   service, timer, supervisor, and operator sessions is still required.

2. Publish and inspect the current target-format recipe, then push and verify
   the selected exact native snapshot:

   ```bash
   POND="$SOURCE_POND" pond capsule recipe publish backup
   POND="$SOURCE_POND" pond capsule recipe inspect backup
   POND="$SOURCE_POND" pond push backup
   POND="$SOURCE_POND" pond verify --exact backup
   ```

   Record the exact source tip and the recipe hash printed by `inspect`.
   Retrieve the hash-addressed bootstrap at
   `recovery/recipes/watertown.commit.v1/<recipe-hash>/README.sh`; do not
   assume the older discoverable `recovery/README.sh` is the current kit.
   Authenticate the bootstrap with the documented `pondcapsule.recipe.1`
   domain hash before executing it.

3. Using the authenticated kit, download a complete read-only copy of the
   native backup, extract by the recorded commit hash, and verify the capsule:

   ```bash
   python extract.py "$BACKUP" "$CAPSULE" \
     --commit "$SOURCE_TIP" --birthplace "$SOURCE_BIRTHPLACE"
   python "$CAPSULE/capsule.py" verify "$CAPSULE"
   python "$CAPSULE/capsule.py" materialize "$CAPSULE" "$MATERIALIZED"
   ```

   `CAPSULE` and `MATERIALIZED` must not exist before creation. Keep the
   backup, capsule, and materialized output read-only after verification.

4. Import only into a new target. Import keeps post-commit dispatch and
   automatic pushes suppressed until the operator deliberately promotes it:

   ```bash
   POND="$TARGET_POND" pond capsule import "$CAPSULE" \
     --birthplace "$TARGET_BIRTHPLACE" --experimental
   POND="$TARGET_POND" pond fsck --quick
   POND="$TARGET_POND" pond remote list
   ```

   Review `CAPSULE_IMPORT_PROVENANCE.json`, validate critical data, schemas,
   timestamps, and remotes, and ensure the target does not resolve to source
   storage. Attach or retain only an isolated target backup. Enable
   `post_commit_dispatch` and start exactly one writer only after those
   checks, including a separate target-format backup/recovery drill, succeed.

Follow [capsule-recovery-runbook.md](capsule-recovery-runbook.md) for the
complete watershop rehearsal and Azure production procedure. It specifies the
immutable-recipe authentication, source shutdown, remote review, recovery
drill, cutover, and rollback gates in detail. Use `pond freeze disable` only
for an explicit pre-cutover rollback after the target remains inert.

### Destructive recovery

`pond emergency` provides destructive recovery operations (e.g. erasing
a remote bucket).  Use with care; see `pond emergency --help`.

---

## 7. Cron / systemd

### Data collection (factory) every minute

```bash
# /etc/cron.d/watertown-collect
* * * * *  watertown  POND=/data/mysite pond run 20-hydrovu collect 2>&1 | logger -t watertown
```

### Replica pull every 5 minutes

```bash
# /etc/cron.d/watertown-pull
*/5 * * * *  watertown  POND=/data/replica pond pull 2>&1 | logger -t watertown-pull
```

### Weekly compaction

```bash
# /etc/cron.d/watertown-maintain
0 3 * * 0  watertown  POND=/data/mysite pond maintain --compact 2>&1 | logger -t watertown-maintain
```

### systemd oneshot + timer

```ini
# /etc/systemd/system/watertown-collect.service
[Service]
Type=oneshot
User=watertown
Environment=POND=/data/mysite
ExecStart=/usr/local/bin/pond run 20-hydrovu collect
```

```ini
# /etc/systemd/system/watertown-collect.timer
[Timer]
OnCalendar=minutely
Persistent=true
[Install]
WantedBy=timers.target
```

> Scripts and cron jobs should set `$POND` explicitly rather than relying
> on any interactive default.

---

## 8. Runbook

### "A backup push failed"

```
$ pond status          # shows the backup `behind local by N txn`
$ pond push origin     # retry
$ pond status          # back to `up to date`
```

### "Replica is behind"

```
$ pond pull upstream
$ pond status
```

### "Replica pull says 'below retention horizon'"

```
$ pond pull upstream
```

### "Control table is corrupt / pond won't open"

```
$ pond rebuild-control --force
# then re-attach remotes:
$ pond backup add origin s3://my-bucket/mysite ...
```

### "Disk is filling up"

```
$ pond maintain --compact
```

### "Transaction failed, data looks inconsistent"

```
$ pond log --incomplete
$ pond recover
```

### "Is my backup actually consistent?"

```
$ pond verify origin     # run on the producer side
```

---

## 9. Quick reference

```
SETUP
  pond init                                   Create a pond
  pond restore <name> <url> [s3 opts]         Bootstrap a full local clone (dev/preview)
  pond backup add <name> <url> [s3 opts]      Attach a push backup
  pond remote add <name> <url> <path>         Attach a pull remote (/ = mirror)
  pond remote list / pond backup list         List attachments
  pond remote remove [--purge] <name>         Detach

ROUTINE
  pond push [name]                            Push pending writes (retry)
  pond pull [name]                            Pull new bundles
  pond maintain [--compact]                   Local checkpoint/vacuum/compact

MONITORING
  pond status                                 Identity + watermarks + recovery
  pond verify [name]                          Compare local data vs remote
  pond log [--limit N|--txn-seq N|--incomplete]
  pond config [set <key> <value>]

RECOVERY
  pond recover                                Resolve incomplete transactions
  pond rebuild-control [--force]              Rebuild control table from data
  pond capsule recipe publish <backup>        Publish current V4 recovery kit
  pond capsule recipe inspect <backup>        Inspect current V4 recovery kit
  pond capsule verify <capsule-dir>           Verify a downloaded V1-V4 capsule
  pond capsule import <capsule-dir> --birthplace <label> --experimental
                                              Import only into a new inert pond
  pond freeze enable|status|disable            Guard a migration source
  pond emergency ...                          Destructive recovery
```

S3 options for `pond restore` / `pond backup add` / `pond remote add`:
`--region`, `--access-key-id`, `--secret-access-key`, `--endpoint`,
`--allow-http`.
`--secret-access-key` must be a `${env:VAR}` reference (single-quoted),
not a literal secret.  See [cli-reference.md](cli-reference.md) for the
complete list.

---

## 10. Future direction: content-addressed sync

> This section is **forward-looking**.  It describes the operator-visible
> end-state of the content-addressed (CA) migration, not current behavior.
> Nothing here ships until the parity gate in the migration plan passes.  The
> design is in [content-addressed-pond-design.md](archive/content-addressed-pond-design.md);
> the phased plan and its acceptance gates are in
> [remote-redesign.md](archive/remote-redesign.md) (the CA1-CA4 arc).  Until then, the
> sections above remain authoritative.

Today a pond replicates as a stream of per-transaction **bundles**
(manifest + checksums + data), and each remote tracks a `(pond_id, seq)`
**frontier**.  Under content-addressed sync the pond becomes a store of
immutable, hash-named objects -- blobs (file/series bytes), trees
(directories), and commits (one per transaction, carrying provenance) -- and
replication transfers only the objects a peer is missing.  The shipped command
surface (`pond push` / `pull` / `verify` / `status` / `maintain`) is preserved;
what changes is the meaning behind it.

### What changes for you

- **Frontier becomes a commit hash.**  "last pushed seq" in `pond status` and
  the per-remote watermark become a single **tip commit hash** per ref.  Two
  ponds are identical exactly when their tip commit hashes (equivalently, their
  root tree hashes) match -- a single comparison instead of a per-partition
  checksum list.

- **Sync transfers objects, not bundles.**  Push/pull compare tip commits and
  exchange only the blobs and trees the other side lacks, addressed by hash.
  Identical content that already exists on the remote (even from a different
  pond or transaction) is not re-sent.  There is no per-transaction bundle to
  retain or prune; retention becomes "objects reachable from the commits you
  keep."

- **Verify localizes divergence.**  `pond verify` compares root tree hashes and,
  on mismatch, descends the tree to report the **specific divergent subtree**
  rather than a flat per-partition checksum diff.  A clean match is one hash
  comparison.

- **`fsck` / integrity becomes a CAS scrub.**  Integrity checking re-hashes each
  reachable object and confirms `name == hash` (plus byte-range chains for
  large blobs and reachability from a ref), replacing the two-level partition
  checksum Merkle.  Corruption is reported as the offending object hash.

- **Compaction is an ordinary rewrite commit.**  `pond maintain --compact` no
  longer emits a special "Compact bundle restart baseline"; it records a
  rewrite commit like any other write.  Recovering a replica that fell behind
  becomes "fetch from any commit the producer still retains," so
  `restart-from-compact` is subsumed by normal commit-DAG fetch.

- **Cross-pond import is a graft.**  Mounting a foreign pond attaches its commit
  chain by reference and pulls the reachable objects, instead of importing
  per-partition bundles.  Provenance (which pond/seq/author produced a commit)
  lives in the commit object and is still surfaced by `pond log`.

### What stays the same

- Single-writer-per-pond, so each pond is still a **linear commit chain** and
  push/pull are still fast-forward (no merge).
- The **data is canonical, the control table is disposable** -- you can still
  rebuild local bookkeeping from the data history.
- Credentials handling, attachment model (backup vs. remote), and the cron /
  systemd patterns in [§7](#7-cron--systemd) are unaffected.
