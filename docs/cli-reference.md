# Watertown CLI Reference

> **Living documentation** - Built from experimentation, updated when confusion is discovered.

## Quick Reference

| Command | Purpose | Example |
|---------|---------|---------|
| `pond init` | Initialize a new pond | `pond init --birthplace water-prod` |
| `pond mkdir` | Create directories | `pond mkdir /data` |
| `pond copy` | Copy files into pond | `pond copy host:///tmp/data.csv /data/` |
| `pond list` | List files (glob patterns) | `pond list '**/*'` |
| `pond cat` | Read file contents | `pond cat host+csv:///tmp/data.csv --format=table` |
| `pond describe` | Show file schema | `pond describe /data/*.csv` |
| `pond mknod` | Create factory nodes | `pond mknod --config f.yaml /path` |
| `pond run` | Execute factory nodes | `pond run 20-foo collect` |
| `pond run` | Execute from host config | `pond run host+sitegen:///config.yaml build .` |
| `pond log` | View transaction history | `pond log --limit 20` |
| `pond remote add` | Attach a pull-mode remote (mirror or import) | `pond remote add upstream s3://bucket /imports/upstream` |
| `pond backup add` | Attach a backup (push or push+pull) | `pond backup add origin s3://bucket` |
| `pond remote list` | List attached remotes (all) | `pond remote list` |
| `pond backup list` | List push-side attachments | `pond backup list` |
| `pond remote remove` | Detach a remote (also works for backups) | `pond remote remove origin` |
| `pond remote remove --purge` | Detach AND drop the materialized mount entry | `pond remote remove --purge upstream` |
| `pond push` | Push to push/both-mode remotes | `pond push` |
| `pond capsule` | Publish or verify a portable recovery snapshot | `pond capsule publish azure` |
| `pond pull` | Pull from pull/both-mode remotes | `pond pull` |
| `pond restore` | Bootstrap a whole-pond replica from a backup (disaster recovery) | `pond restore origin file:///mnt/backups/origin` |
| `pond maintain` | Delta maintenance; `--compact` records a pushable Compact bundle | `pond maintain --compact` |
| `pond verify` | Compare local data against remote checksums (D6.1) | `pond verify origin` |
| `pond fsck` | Local integrity check: content checksums + content-tree root fingerprint | `pond fsck --verbose` |
| `pond status` | Operator status aggregate: identity, watermarks, recovery (D6.2) | `pond status` |
| `pond tlog verify` | Verify the transparency log (inclusion, append-only, faithfulness) (D5) | `pond tlog verify` |
| `pond rebuild-control` | Reconstruct a lost control table from data (D6.3) | `pond rebuild-control` |
| `pond config` | Show/set pond configuration | `pond config` |

### Two Operating Modes

Watertown commands work in two modes:

- **Pond mode**: Operations on a transactional filesystem at `$POND`.
  Files are persistent, versioned, and replicable.
- **Host mode** (`host+` prefix): Read-only operations on host
  filesystem files.  No `$POND` required.  The same query engine and
  factory system work directly on local files.

| Mode | `pond cat` | `pond run` | `pond copy` |
|------|-----------|-----------|-------------|
| Pond | `pond cat /data/file` | `pond run 20-hydrovu collect` | `pond copy host:///f /data/f` |
| Host | `pond cat host+csv:///f` | `pond run host+sitegen:///c.yaml build .` | N/A (read-only) |

## Environment

| Variable | Purpose | Default |
|----------|---------|---------|
| `POND` | Path to pond storage | Required for pond mode; not needed for `host+` operations |
| `RUST_LOG` | Logging level | `info` |

## Commands in Detail

### pond init

Initialize a new pond at the path specified by `$POND`.

```bash
export POND=/data/mypond
pond init --birthplace water-prod
```

Creates the pond directory structure with Delta Lake metadata.

The required `--birthplace` is an immutable label for where the pond is
created (for example a hostname, site, or deployment name).  It is
recorded permanently in the pond's identity metadata and shown by
`pond status`, `pond log`, and `pond config`.

---

### pond list

List files and directories matching a glob pattern.

> **Unlike `ls`**, `pond list` is **recursive by default**.  Running `pond list`
> with no arguments lists every file in the pond.  Use `pond list /` for just
> the top-level entries.

```bash
# List everything (default behavior)
pond list              # defaults to '**/*' - all files recursively

# List root directory entries
pond list /            # top-level files and directories only

# List specific directory contents  
pond list /data/       # trailing slash lists contents of /data
pond list '/data/*'    # equivalent to above
pond list '/data/**/*' # recursive under /data

# Match specific entry (file or directory)
pond list /data        # shows the /data entry itself (not contents)

# Pattern matching
pond list '**/*.csv'   # all CSV files
pond list '/sensors/*/readings.series'
```

**Options**:

| Flag | Purpose |
|------|---------|
| `-l`, `--long` | Show full entry type names (e.g., `table:series`, `table:dynamic`) |
| `-a`, `--all` | Show hidden files |

**Pattern Behavior Summary**:

| Pattern | Meaning |
|---------|---------|
| (none) | All files recursively (`**/*`) |
| `/` | Root directory entries only |
| `/dir/` | Contents of /dir (trailing slash = `/dir/*`) |
| `/dir` | Entry named 'dir' exactly |
| `**/*.ext` | All files with extension recursively |

> **Tip**: Use trailing slash to list directory contents: `pond list /data/`

---

### pond mkdir

Create directories in the pond.

```bash
pond mkdir /data
pond mkdir /sensors/temperature
```

Parent directories are created automatically (like `mkdir -p`).

---

### pond copy

Copy files between host filesystem and the pond.

#### Copy IN (host → pond)

Source URLs must have the `host://` or `host+` prefix. The entry type is encoded in the URL:

```bash
# Copy raw data (default): store as opaque bytes
pond copy host:///tmp/data.csv /data/readings.csv

# Copy as queryable Parquet table (validates PAR1 magic bytes)
pond copy host+table:///tmp/data.parquet /data/readings.parquet

# Copy as time-series Parquet (validates PAR1 + extracts temporal bounds)
pond copy host+series:///tmp/readings.parquet /data/readings.parquet

# Copy to directory (keeps filename)
pond copy host:///tmp/data.csv /data/
```

| URL Prefix | Entry Type | Validates | Use Case |
|------------|-----------|-----------|----------|
| `host:///path` | data (raw bytes) | None | CSV, JSON, any file |
| `host+file:///path` | data (raw bytes) | None | Same as `host:///` |
| `host+table:///path` | queryable Parquet | PAR1 magic | Parquet tables |
| `host+series:///path` | time-series Parquet | PAR1 magic + temporal | Parquet with timestamp |

> **Note**: `host+table:///` validates that the input is valid Parquet (PAR1 magic bytes).
> To copy a CSV file, use `host:///` (raw data), then query with `csv://` URL scheme.

```bash
# Correct: Copy CSV as raw data, then query with csv:// prefix
pond copy host:///tmp/data.csv /data/readings.csv
pond cat csv:///data/readings.csv --sql "SELECT * FROM source WHERE temp > 20"

# Wrong: host+table:// will error on CSV (not valid Parquet)
pond copy host+table:///tmp/data.csv /data/readings.csv  # ERROR: missing PAR1 magic
```

#### Copy OUT (pond → host)

Export files from the pond to the host filesystem. The destination must have the `host://` prefix.
Glob patterns are supported for matching multiple files.

```bash
# Export all series files preserving directory structure
pond copy '/hydrovu/devices/**/*.series' host:///tmp/export

# Export a single file
pond copy /data/readings.parquet host:///tmp/output
```

The export format is determined by the source entry type:
- table/series entries → exported as Parquet (via DataFusion)
- data entries → exported as raw bytes (bit-for-bit copy)

#### --strip-prefix

When copying out, pond paths are preserved relative to the destination. This can produce
unwanted nesting (e.g. exporting `/hydrovu/...` into a directory called `hydrovu/` creates
`hydrovu/hydrovu/...`). Use `--strip-prefix` to remove a leading path prefix:

```bash
# Without --strip-prefix: creates output/hydrovu/devices/123/foo.series
pond copy '/hydrovu/**/*.series' host:///tmp/output

# With --strip-prefix: creates output/devices/123/foo.series
pond copy '/hydrovu/**/*.series' host:///tmp/output --strip-prefix=/hydrovu
```

---

### pond cat

Read and optionally transform file contents. Works on both **pond files** and
**host filesystem files** (via `host+` URL prefix). No `$POND` is required when
reading host files.

```bash
# Read raw file contents from pond
pond cat /data/readings.csv

# Read a host file directly (no pond needed)
pond cat host+file:///tmp/readme.txt

# Query a host CSV with SQL
pond cat host+csv:///tmp/data.csv --format=table --sql "SELECT * FROM source WHERE temp > 20"

# Query a host zstd-compressed CSV
pond cat host+csv+zstd:///tmp/data.csv.zst --format=table

# Query a host Parquet file directly as a table (no pond needed)
pond cat host+table:///tmp/snapshot.parquet --format=table
pond cat host+table:///tmp/snapshot.parquet --sql "SELECT count(*) FROM source"

# Query pond CSV files with SQL (use csv:// prefix)
pond cat csv:///data/readings.csv --sql "SELECT * FROM source WHERE temp > 20"

# Query OtelJSON Lines files (use oteljson:// prefix)
pond cat oteljson:///logs/metrics.json --sql "SELECT * FROM source ORDER BY timestamp"

# Query Parquet files (stored with host+table:// via pond copy)
pond cat /data/readings.parquet --sql "SELECT AVG(temperature) as avg_temp FROM source"
```

The table is always named `source` in SQL queries.

#### Output Format (`--format`)

| Flag | Output | When to use |
|------|--------|-------------|
| `--format=raw` (default) | Parquet bytes (binary) | Piping to files, downstream tools |
| `--format=table` | Human-readable text table | Terminal display, debugging |

> **Note**: `--format` on `pond cat` controls **output display** (raw bytes vs ASCII table).
> `pond copy` does not have a `--format` flag — entry type is encoded in the source URL
> (e.g., `host+table:///path` for Parquet tables).

`--sql` controls **what** data is queried; `--format` controls **how** it's displayed.

```bash
# Parquet bytes to file (default)
pond cat oteljson:///ingest/data.json --sql "SELECT * FROM source" > output.parquet

# Human-readable table to terminal
pond cat oteljson:///ingest/data.json --format=table --sql "SELECT * FROM source LIMIT 5"

# Human-readable table, no SQL (full SELECT * ORDER BY timestamp)
pond cat oteljson:///ingest/data.json --format=table
```

#### URL Schemes

URL schemes tell `pond cat` how to parse the file. Pond files and host files
use the same format/compression/entry-type syntax — host files just add the
`host+` prefix.

**General syntax**: `[host+]format[+compression][+entrytype]:///path`

| Scheme | Purpose | Example |
|--------|---------|--------|
| `csv://` | Parse as CSV (pond) | `pond cat csv:///data/file.csv --sql "..."` |
| `host+csv://` | Parse as CSV (host) | `pond cat host+csv:///tmp/file.csv --format=table` |
| `host+csv+zstd://` | Zstd-compressed CSV (host) | `pond cat host+csv+zstd:///tmp/file.csv.zst --format=table` |
| `host+csv+series://` | CSV as time-series (host) | `pond cat host+csv+series:///tmp/ts.csv --format=table` |
| `oteljson://` | Parse as OtelJSON Lines | `pond cat oteljson:///logs/metrics.json --sql "..."` |
| `excelhtml://` | Parse as Excel HTML | `pond cat excelhtml:///data/export.html --sql "..."` |
| `host+file://` | Raw bytes (host) | `pond cat host+file:///tmp/readme.txt` |
| `file://` | Raw bytes or Parquet (pond) | `pond cat file:///data/file.parquet` |
| (none) | Auto-detect (pond) | `pond cat /data/file.csv` |

**Supported compressions**: `zstd`, `gzip`, `bzip2`

**Supported entry types**: `table`, `series`

⚠️ **Important**: The `--sql` flag (or `--query` alias) only works when the file can be parsed as a table:
- Parquet files (stored as table or series entry types)
- CSV files when using `csv://` or `host+csv://` prefix
- OtelJSON Lines files when using `oteljson://` prefix (two-pass: discovers all metric names as columns)
- Raw data files without a scheme will output raw bytes, ignoring `--sql`

#### Schema Discovery with `information_schema`

DataFusion's `information_schema` is enabled, so you can discover column names dynamically:

```bash
# List all columns in the source table
pond cat oteljson:///ingest/data.json --format=table --sql "
  SELECT column_name, data_type
  FROM information_schema.columns
  WHERE table_name = 'source'
  ORDER BY column_name
"
```

This is useful when exploring unfamiliar data — you don't need to know the column
names in advance. `pond describe` also shows the schema, but `information_schema`
lets you query metadata with SQL alongside your data queries.

---

### pond describe

Show schema information for files.

```bash
pond describe /data/readings.csv
pond describe '/sensors/**/*.parquet'
```

---

### pond mknod

Create a factory node from a YAML configuration.

```bash
# From config file
pond mknod --config /path/to/config.yaml /destination/path

# Specific factory types
pond mknod sql-derived-table /derived/view --config-path filter.yaml
pond mknod hydrovu /system/etc/20-hydrovu --config-path hydrovu.yaml
pond mknod sitegen /system/etc/90-sitegen --config-path site.yaml
```

See [Factory Types](#factory-types) for configuration options.

---

### pond run

Execute a factory node's commands.  Works on both **pond nodes** and
**host filesystem config files** (via `host+` URL prefix).

```bash
# Run data collection (full path)
pond run /system/etc/20-hydrovu collect

# Short name resolution (tries /system/run/ then /system/etc/)
pond run 20-hydrovu collect

# Build site
pond run /system/etc/90-sitegen build ./dist
```

Short names (without a leading `/`) are resolved by checking `/system/run/{name}`
then `/system/etc/{name}`. The first path that exists wins. If neither exists,
falls back to `/system/run/{name}` (which will produce a clear error downstream).

> **Note (D4+)**: remote-backup `push`/`pull`/`verify`/`show` are no longer
> invoked through `pond run` on a factory node.  Use the top-level
> `pond push`, `pond pull`, and `pond remote ...` verbs instead (see below).
> Configurations live under `/sys/remotes/<name>`, not under
> `/system/run/<N>-remote`.

#### Host Mode (`host+factory://`)

When the path uses a `host+` URL, `pond run` reads the config from the
host filesystem and executes the factory named by the URL scheme.  **No
`$POND` is required.**  This is useful for running any factory against
a local config file without first setting up a pond.

```bash
# Build a site from a local config
pond run host+sitegen:///path/to/site.yaml build ./dist
```

The URL scheme (`sitegen`, etc.) must be a registered factory name.
Format providers like `csv` are not valid here -- use `pond cat` for those.

**Config file format**: the YAML file at the host path is passed directly
to the factory, exactly as if it were stored inside a pond node.

---

### pond log

View transaction history and audit trail.

```bash
# Recent transactions (default: last 10)
pond log
pond log --limit 20

# Transaction details
pond log --txn-seq 42

# Show incomplete operations (for recovery)
pond log --incomplete
```

---

### pond remote / pond backup

Watertown splits remote attachment into two verbs that match operator
intent (D5.7b):

- **`pond remote add NAME URL PATH`** -- attach a **pull-mode** remote
  and mount it at PATH.  `PATH = /` is a mirror restart of this pond's
  own backup (foreign store_id must match this pond's pond_id).
  Non-root PATH is a cross-pond import (foreign store_id must differ;
  imported data appears under PATH).
- **`pond backup add NAME URL [--bidirectional]`** -- attach a backup
  remote.  Push-only by default; `--bidirectional` enables both push
  and pull (the rare bidirectional case).  Backups always mirror the
  entire pond -- there is no PATH because the local pond IS the source.

Each attachment is a small YAML file under `/sys/remotes/<name>`
(portable; no per-pond watermarks).  The local-only mode and mount
path live in the control table's raw_config map.

> **Credentials must be `${env:VAR}` references, not literal secrets.**
> The attachment YAML is replicated to every backup, so a literal
> `--secret-access-key` would expose the secret on all replicas.
> `pond remote add` / `pond backup add` therefore rejects a literal
> `secret_access_key`: pass an env reference (single-quoted so your
> shell does not expand it) and set the value in the environment.  The
> reference text replicates; each replica resolves it locally at use
> time.

```bash
# Attach an S3 backup with credentials (push-only).
# Export the secret in the environment; pass it as an env reference.
export AWS_SECRET_ACCESS_KEY=...   # the actual secret, never persisted
pond backup add origin s3://my-bucket \
    --region us-east-1 \
    --endpoint http://localhost:9000 \
    --access-key-id minioadmin \
    --secret-access-key '${env:AWS_SECRET_ACCESS_KEY}' \
    --allow-http

# Attach a bidirectional remote (push + pull, e.g. a federated hub)
pond backup add hub s3://hub-bucket --bidirectional \
    --region us-east-1 \
    --access-key-id ... --secret-access-key '${env:AWS_SECRET_ACCESS_KEY}'

# Attach a pull-mode remote as a cross-pond import
pond remote add upstream s3://prod-bucket /imports/upstream \
    --region us-east-1 \
    --access-key-id ... --secret-access-key '${env:AWS_SECRET_ACCESS_KEY}'

# Attach a pull-mode remote as a mirror restart of an EXISTING replica
# (a pond that already carries the source pond_id).  To bootstrap a
# replica from a blank machine, use `pond restore` instead (it stamps the
# source pond_id first); a bare `pond remote add ... /` on a freshly
# `pond init`-ed pond is refused on a pond_id mismatch.
pond remote add origin file:///mnt/backups/origin /

# List all remotes (both pull and backup)
pond remote list

# List backups only
pond backup list

# Detach a remote (clears config + watermarks; preserves any materialized
# mount entry so previously-imported data is still readable by path).
pond remote remove origin

# Detach AND remove the mount entry created by a cross-pond import
# (only meaningful for pull-mode remotes with a non-root mount_path).
pond remote remove --purge upstream
```

`pond remote add` writes `/sys/remotes/<name>` as YAML and records
`remote_mode:<name>` and `remote_mount_path:<name>` in the control
table.  Re-adding the same name errors unless `--overwrite` is given.

**Attach-time conflict checks** (pull-mode only):

- The same `mount_path` cannot be used by two different pull-mode
  remotes.  Trailing slashes are normalized, so `/imports/x` and
  `/imports/x/` are treated as identical.  `--overwrite` is only
  honored when the conflicting attachment has the *same* name as the
  one being attached.
- The same foreign `store_id` cannot be mounted under two different
  pull-mode names.  Mirror-restart attachments (`PATH = /`, foreign
  `store_id` == local `pond_id`) are exempt because they do not
  materialize a mount entry.

**Cross-pond mounts are read-only.**  Any write inside an
`/imports/<name>/...` subtree -- whether by `pond copy`, by an
explicit `pond run`, or by an auto-executing factory configured in
the foreign pond -- is refused with `ReadOnlyImport`.  The steward
also filters its `/system/run/*` scan by local `pond_id`, so a
factory living in the foreign pond's filesystem is never
auto-executed by the local steward.

**`--purge` semantics.**  By default, `pond remote remove` is a
**detach**: it clears `/sys/remotes/<name>`, the
`remote_mode:<name>` key, the `remote_mount_path:<name>` key, and the
per-URL watermarks, but leaves any materialized mount entry in place
(so imported data stays readable through the original path).  With
`--purge`, the mount entry is also unlinked from its parent
directory.  `--purge` is a no-op on backup-mode attachments (they
have no `mount_path`).  Note: physically vacuuming the foreign
`pond_id`'s rows from the underlying Delta log is deferred -- the
rows become unreachable by path after purge and are eligible for a
future compaction.

---

### pond push

Push pending local transactions to one or more remotes.

```bash
# Push every remote with mode=push or mode=both
pond push

# Push only the named remote
pond push origin
```

After every write transaction, the steward also auto-pushes all
`push`/`both`-mode remotes -- so this command is mostly used when an
earlier auto-push failed (transient network), or right after attaching
a brand-new remote. A successful native push is followed by recovery-capsule
publication. If capsule publication fails, the native backup remains durable
and the previous verified capsule remains current, but the push reports failure
so retrying completes the portable backup.

---

### pond capsule

Publish and verify portable logical recovery snapshots. Recovery capsules are
stored as a canonical manifest plus plain BLAKE3-addressed file and Parquet
objects, so they can be downloaded with Azure CLI or MinIO Client without a
source-format `pond` binary.

```bash
# Explicitly publish or repair the capsule on a named backup.
# Normal successful pushes also do this automatically.
pond capsule publish azure

# List the current and retained generations at a named remote.
pond capsule list azure

# Verify and summarize a capsule downloaded into ./recovered/recovery/.
pond capsule inspect ./recovered

# Verification-only spelling for automation.
pond capsule verify ./recovered
```

Explicit publication reuploads and verifies the full payload closure, repairing
any stray or corrupt same-name object. Automatic changed-tip publication reuses
only payloads declared by the prior verified generation.

Each published generation contains `RUNBOOK.txt`, `download-az.sh`, and
`download-mc.sh`. The scripts embed the immutable capsule root and exact object
list, but no credentials. Download and review a script before running it; use
managed identity or an already-authenticated client.

Inspection does not open or modify a pond. It validates the latest reference,
canonical manifest, every physical object hash and size, every Parquet schema,
every ordered logical file/table leaf hash, and each logical series root.
See `recovery-capsule-design.md` for the format-upgrade and cleanup design.

---

### pond pull

Pull new bundles from one or more remotes.

```bash
# Pull every remote with mode=pull or mode=both
pond pull

# Pull only the named remote
pond pull upstream

# Discard and atomically rebuild only this named cross-pond graft
pond pull upstream --rebuild-graft
```

> **Cross-pond pull bootstrap (D5.7b.2):** when a pull-mode remote
> is attached with a non-root `mount_path` (e.g. `/imports/upstream`)
> and its `store_id` differs from the local `pond_id`, the first
> `pond pull` automatically materializes the mount entry under the
> configured path.  No manual seeding is required.  Mirror restarts
> (`PATH = /`, same `store_id`) require a pond that already carries the
> source `pond_id`; use **`pond restore`** (below) to bootstrap one from
> a blank machine.

`--rebuild-graft` is an explicit recovery operation for a non-root graft. It
replaces only that foreign pond partition, validates the replacement before
commit, and commits the mount and graft pin in the same transaction. It refuses
to unlink local content or a different pond's graft at the configured path.

Ordinary pulls are fast-forward only: a differing remote tip must descend from
the recorded `last_pulled_tip`. A stale or out-of-order remote ref is rejected
rather than rolling a mirror or graft backward. Use `--rebuild-graft` for an
intentional cross-pond replacement.

---

### pond restore

Bootstrap a whole-pond replica from a backup published to a remote --
the operator entry point for disaster recovery.

`pond init` always mints a FRESH `pond_id`, so a freshly-inited pond can
never attach its own backup as a mirror (`pond remote add ... /` refuses
on a `pond_id` mismatch).  `pond restore` closes that gap: it discovers
the SOURCE pond's id by opening the remote read-only, stamps a replica
shell carrying that id, attaches the remote as a pull-mode mirror at `/`,
and pulls the full content graph to rebuild the pond by node_id.

```bash
# Restore a whole pond from a file:// backup into the current pond dir
pond restore origin file:///mnt/backups/origin

# Restore from S3 (same credential flags as `pond backup add`)
pond restore origin s3://my-bucket \
    --region us-east-1 \
    --access-key-id AKIA... \
    --secret-access-key '${env:AWS_SECRET_ACCESS_KEY}'
```

- **Refuses to run over an existing pond** -- restore bootstraps a fresh
  replica; remove the target directory (or choose an empty one) first.
  On any failure it removes the partially-created shell so a retry starts
  clean.
- After a successful restore the local pond IS the source: same
  `pond_id` and the same content **tip commit hash** (the canonical
  cross-replica fingerprint).  Byte-for-byte content -- including
  external `>64KB` blobs -- is reproduced.
- The `NAME` attachment is left configured as a mirror, so a later
  `pond pull NAME` tracks the upstream incrementally.

> The `pond fsck` content root matches across identity-preserving mirrors
> (a byte copy, or a `pond pull` mirror that keeps the producer's `pond_id`),
> because it hashes content, not local transaction history.  A cross-pond
> consumer keeps its own `pond_id`, so its top-level root differs by design;
> the tip commit hash is the canonical cross-pond fingerprint.  See
> `pond verify`.

---

### pond verify (D6.1)

Compare this pond's CURRENT content tip against one or more remotes'
published tip.  The content-addressed remote holds a single object
closure and one tip ref per pond -- there is no `(pond_id, seq)`
frontier -- so verify reports the commit-graph relationship between the
local tip and the remote's published tip.

```bash
# Verify against every attached remote
pond verify

# Verify against only the named remote
pond verify origin
```

Output (per remote):
- `[OK] verify <name>: live data matches remote tip at seq=<N>` -- the
  remote's published tip equals the local tip (up to date).
- `[OK] verify <name>: remote tip at seq=<N> is behind local by <K> commit(s); push to catch up`
  -- the remote's tip is an ancestor of the local tip; the producer has
  unpushed local commits (lag, not drift).
- `[OK] verify <name>: remote has no published tip yet (nothing pushed)`
  -- the remote holds no ref under `main`.
- `[OK] verify <name>: no local commits and remote is empty` -- both
  sides are empty.
- `[MISMATCH] verify <name>: remote tip at seq=<N> is not in this pond's history (diverged)`
  -- the remote published a commit this pond does not have.

Each line is followed by the local and remote tip commit hashes.  Exit
code is non-zero if any remote diverges or fails to load.

> Verify is symmetric: a replica bootstrapped from a remote (via
> `pond pull`) verifies cleanly against that remote, because it rebuilds
> the same content closure and tip the producer published.

---

### pond fsck

Local, **offline** filesystem-check.  Unlike `pond verify` (which compares
against a *remote*), `fsck` validates the pond against itself and prints a
single deterministic **content root** that fingerprints the live content of
every `pond_id` in the data table -- including cross-pond imports.

```bash
# Print the content root (one line, 64 hex chars)
pond fsck

# Skip the content-rehash pass; compute only the structural root (fast)
pond fsck --quick

# Per-partition breakdown + content statistics
pond fsck --verbose
```

The root is a content tree of content trees.  A directory *is* a partition,
and its recursive `tree_hash` (fold-excluding the reserved INDEX/LOG nodes)
is its checksum; the top-level root is a `tree_hash` over the per-pond
`root_tree_hash`es, keyed by `pond_id`:

```text
root                                  <- tree_hash over per-pond roots
|- pond_id -> root_tree_hash          (that pond's content tree)
|  |- directory (part_id) -> tree_hash
|  |  |- child_hash of each entry (recursive)
|  |  +- ...
|  +- ...
+- ...
```

It is **layout-, compaction-, and lineage-independent** (it hashes content,
not parquet file layout or local transaction history), so two replicas of
the same pond -- a byte copy, or a *mirror* rebuilt independently by
`pond pull` -- produce the **same root hex**.  Comparing two replicas is
therefore a string compare:

```bash
pond fsck            # on replica A  ->  a1b2c3...
pond fsck            # on replica B  ->  a1b2c3...   (identical iff in sync)
```

> **What "replica" means here.**  The top-level root folds the per-pond
> content trees keyed by `pond_id`, so equal-root comparison applies to
> **identity-preserving** replicas: a byte copy of the pond directory, or a
> *mirror* that keeps the producer's `pond_id`.  It is **not** a content-only
> fingerprint: two ponds created independently with the same files get
> different random `pond_id`s and therefore different roots.  A **cross-pond
> consumer** (`pond remote add ... /imports/<name>`) keeps its own `pond_id`
> and mounts the producer's partitions, so its root differs by design --
> compare such a consumer at the **partition** level instead: every producer
> partition line in `pond fsck --verbose`
> (`<pond_id>/<part_id>  rows=N  <tree_hash>`) reappears verbatim in the
> consumer's `--verbose` output once it has pulled.

**Content-checksum pass** (default; skipped with `--quick`): the structural
root commits to each file's *recorded* `blake3`, but not to the bytes it
points at.  By default `fsck` also re-hashes inline file content and re-reads
every external `_large_files/blake3=<hash>.parquet` blob, confirming the
bytes match their recorded `blake3`.  This catches bit-rot / blob corruption
that the structural root alone cannot see.

Output:
- Default: the content root on one line.
- `--verbose`: `Root checksum`, a per-partition digest list, and counts of
  live entries / inline content / blobs verified, ending in `Result: OK` or
  `Result: FAILED`.

Exit code is non-zero if any content check fails; each failure is logged
with the offending `pond_id/part_id`, node, version, and the mismatching
`blake3`.  Because content errors are collected (not fatal on first hit),
one corrupt blob does not hide the rest of the report.

---

### pond status (D6.2)

Show an operator-facing status aggregate for the current pond.  This is
a fast, **offline** command -- it reads only the local control table and
`/sys/remotes/*` attachments and never opens a remote or touches the
network, so it is safe on a pond whose remotes are unreachable.

```bash
pond status
```

Output sections:
- **Identity** -- pond ID, creation time / creator, on-disk location.
- **Local state** -- last committed write sequence and recovery health
  (lists incomplete transactions with a `pond recover` hint if any are
  found).
- **Remotes** -- one block per attached remote/backup: url, mount path
  (`/ (mirror)` for mirror attachments), and sync watermarks.  For
  push/both remotes the `last pushed` line shows lag relative to the
  local `last write seq` (`up to date`, `behind local by N txn`, or
  `never pushed`).  For pull/both remotes the `last pulled` watermark is
  shown.

> **Note:** push "lag" is computed purely from local watermarks
> (`last_write_seq` vs `last_pushed_seq:<url>`).  To cross-check the
> consumer's data against what a remote actually recorded, use
> `pond verify`.

---

### pond tlog (D5)

Inspect and verify the pond's **transparency log**: an append-only RFC 6962
SHA-256 Merkle tree over the linear commit spine, published as C2SP
`tlog-tiles` under `{POND}/tlog`.  Every write transaction appends one leaf
(its commit object).  These are read-only, **offline** commands.

Signing (the log's trust root) is deferred, but the log's *key-free*
properties are fully verifiable today: `pond tlog verify` proves
tamper-evidence and append-only growth without any key.

```bash
pond tlog show      # checkpoint, checkpoint history, tree size
pond tlog verify    # verify the log; exit non-zero if any check fails
```

**`pond tlog show`** prints the current checkpoint (origin, tree size, root),
the log directory, and the append-only checkpoint history (`{POND}/tlog/checkpoints`),
one line per checkpoint ever published.

**`pond tlog verify`** runs four checks and prints a `[PASS]`/`[FAIL]` line for
each, exiting non-zero if any fails:
- **Checkpoint reproduced** -- the level-0 tiles re-fold to the checkpoint's
  size and root.
- **Inclusion** -- every published leaf proves inclusion against the checkpoint
  root (RFC 6962 inclusion proof).
- **Append-only consistency** -- every checkpoint in the history is an
  append-only prefix of the current tree (RFC 6962 consistency proof).
- **Faithfulness** -- the published leaves equal `hash_leaf(commit_object)` for
  every spine-bearing `DataCommitted` record in the control table, leaf for
  leaf, tying the published log back to the authoritative source of truth.

The published tiles are a **derived, re-materializable export** of the
control-table commit spine.  If the export is ever dropped or lags (a crash, an
I/O error, an unwritable `{POND}/tlog`), it self-heals on the next commit: the
writer replays every missing leaf from the control table, so verification
returns clean.

---

### pond rebuild-control (D6.3)

Reconstruct the control table from the data Delta table.  This is a
**disaster-recovery** command for the case where the data Delta table
survives but the control table is lost or corrupt (so `pond` can no
longer open the pond).

```bash
# Rebuild when the control table is missing
pond rebuild-control

# Move an existing (corrupt) control table aside and rebuild
pond rebuild-control --force
```

**Recovers:**
- **Pond identity** -- the canonical `pond_id` from the data table's
  bootstrap row, plus a birth timestamp from the bootstrap commit.  The
  birth hostname/username are not stored in the data table and are
  recorded as `unknown`.
- **Transaction-log skeleton** -- one `Begin` + `DataCommitted` (plus a
  trailing `Completed` for the bootstrap) per write transaction found in
  the data Delta commit history, so `pond log` and `pond status` work
  again.  The original `txn_seq`, `txn_id`, CLI args, Delta version, and
  commit timestamp are all recovered from the `pond_txn` commit
  metadata.
- **Commit spine** -- the `commit_hash` / `root_tree_hash` /
  `commit_object` of every content-changing commit are replayed from the
  authoritative, pond-resident LOG node, so tip lookups and
  content-addressed `pond push` work against a rebuilt control table.

**Does NOT recover (operator follow-up required):**
- **Remote attachments' settings** -- remote modes and
  `last_pushed_seq` / `last_pulled_seq` watermarks live only in the
  control table.  Re-attach remotes with `pond remote add` /
  `pond backup add` after a rebuild.

Per-transaction partition checksums are no longer recorded (retired in
favour of the pond-resident content tree, Decision D9 step 5b), so a
rebuilt control table is byte-equivalent to a normally committed one for
integrity purposes -- `pond fsck` and `pond verify` both work against a
rebuilt control table without re-baselining.

> **Safety:** if a real control table already exists, the rebuild is
> refused unless `--force` is given.  With `--force`, the existing
> control directory is moved aside to a `control.bak.<unix_ts>` sibling
> before the new one is written.  A stale empty `control/` directory
> left behind by a failed open is removed automatically (no `--force`
> needed).

> **Note:** `pond rebuild-control` is designed for primary ponds.  A
> freshly restored replica that has not yet done local writes should be
> re-bootstrapped via `pond remote add` + `pond pull`, not rebuilt.

---

### pond maintain

Run Delta Lake maintenance on the pond's data and control tables:
checkpoint creation, commit-log cleanup, and vacuum of stale parquet
files.  Safe to run routinely (e.g. from cron) to keep table-open cost
bounded.

```bash
# Checkpoint + vacuum (no file merging)
pond maintain

# Also compact: merge many small parquet files into fewer large ones
pond maintain --compact
```

`--compact` compacts the pond's own-`pond_id` partitions as a **recorded,
pushable transaction**: the merge is written to the control table as a
`Compact` commit, so a subsequent `pond push` publishes the compacted
content closure.

Compaction never changes logical content -- watertown snapshots each
partition's checksum before and after the merge and aborts if they
differ.  A run with nothing to merge is a clean no-op.

---

### pond config

Show or set pond configuration (ID, factory modes, metadata, settings).

```bash
# Show configuration
pond config

# Set a configuration value
pond config set <key> <value>
```

> **Note:** `pond control` is still available as a hidden alias for backward compatibility.

---

## Filesystem Conventions

Several well-known paths under the pond have special meaning to the
steward and CLI.

### `/sys/remotes/<name>` -- remote attachments (D4)

Each file under `/sys/remotes/<name>` is a small YAML document recording
one remote's URL + mode + (S3) credentials.  Managed via `pond remote
add`, `pond remote remove`, `pond remote list`.  After every write
transaction the steward iterates these and auto-pushes any whose mode is
`push` or `both`.

### `/system/{run,etc,site}/` -- factory nodes

Factory nodes live under `/system/` in directories that determine their
auto-execution behavior:

| Directory | Purpose | Post-commit | Examples |
|-----------|---------|------------|----------|
| `/system/run/` | Auto-executing factories | Yes (default: `push`) | (currently rare; see note) |
| `/system/etc/` | Manually triggered or passive | No | `hydrovu`, `sitegen`, `column-rename`, `logfile-ingest`, `journal-ingest` |
| `/system/site/` | Static content (templates) | No | Markdown page templates |

> **Note (D4+)**: the legacy `remote` factory used to live in
> `/system/run/<N>-remote`; that pathway is gone.  Remote sync is now
> handled by `/sys/remotes/<name>` + `pond push|pull` (see above).
> `/system/run/` is still scanned post-commit for any future factories
> that opt into auto-execution, but no in-tree factory currently uses it.

**Post-commit auto-execution:** After every write transaction, the
steward scans `/system/run/*` and executes each factory with its
configured mode (default: `push`).  Only factories that support the
mode will succeed -- this is why `hydrovu` and `sitegen` must NOT be
placed in `/system/run/`.  Separately, the steward iterates
`/sys/remotes/*` and auto-pushes any `push`/`both`-mode remotes.

**Short name resolution:** `pond run` resolves bare names (no leading
`/`) by checking `/system/run/{name}` then `/system/etc/{name}`.

**Naming convention:** Use numeric prefixes for ordering:
`10-hrename`, `20-hydrovu`, `90-sitegen`.

**Setup pattern:**
```bash
pond mkdir -p /system/etc
pond mkdir -p /system/site

# Manually triggered factories
pond mknod column-rename /system/etc/10-hrename --config-path hrename.yaml
pond mknod hydrovu /system/etc/20-hydrovu --config-path hydrovu.yaml
pond mknod sitegen /system/etc/90-sitegen --config-path site.yaml

# Static content
pond copy host:///path/to/templates /system/site --overwrite

# Backup attachment (managed by `pond backup add`, not `pond mknod`)
pond backup add origin s3://my-bucket \
    --region us-east-1 --access-key-id ... --secret-access-key '${env:AWS_SECRET_ACCESS_KEY}'
```

---

## Factory Types

### sql-derived-table

Apply SQL transformation to a single file.

```yaml
factory: "sql-derived-table"
config:
  patterns:
    source: "table:///raw/data.csv"
  query: "SELECT timestamp, value * 2 as doubled FROM source"
```

### sql-derived-series

Apply SQL to multiple files/versions (time series).

```yaml
factory: "sql-derived-series"
config:
  patterns:
    source: "series:///sensors/*"
  query: "SELECT * FROM source WHERE temperature > 20"
```

### synthetic-timeseries

Generate synthetic timeseries with configurable waveforms. Produces Arrow
RecordBatches in memory (no files on disk) — useful for testing, demos, and
development with deterministic, visually distinct data.

Each named **point** becomes a Float64 column. Its value at every timestamp is the
**sum** of one or more waveform **components**, so you can layer signals to create
complex but predictable shapes.

```yaml
start: "2024-01-01T00:00:00Z"
end: "2024-01-02T00:00:00Z"
interval: "15m"
time_column: "timestamp"          # optional, default: "timestamp"
points:
  - name: "temperature"
    components:
      - type: sine
        amplitude: 10.0
        period: "24h"
        offset: 20.0              # baseline value
      - type: line
        slope: 0.0002             # slow upward drift

  - name: "pressure"
    components:
      - type: sine
        amplitude: 5.0
        period: "12h"
        offset: 1013.0
      - type: square
        amplitude: 2.0
        period: "6h"

  - name: "humidity"
    components:
      - type: triangle
        amplitude: 15.0
        period: "8h"
        offset: 60.0
      - type: sine
        amplitude: 3.0
        period: "3h"
        phase: 1.57               # phase offset in radians
```

**Component types:**

| Type | Formula | Parameters |
|------|---------|------------|
| `sine` | `offset + amplitude × sin(2π·t/period + phase)` | `amplitude`, `period`, `offset`, `phase` |
| `triangle` | `offset + amplitude × tri(t/period + phase)` | `amplitude`, `period`, `offset`, `phase` |
| `square` | `offset + amplitude × sign(sin(2π·t/period + phase))` | `amplitude`, `period`, `offset`, `phase` |
| `line` | `offset + slope × t` | `slope`, `offset` |

- `t` is seconds elapsed since `start`.
- `period` and `interval` accept human-readable durations: `30s`, `15m`, `1h`, `2d`, `1h30m`, etc.
- All parameters default to `0.0` if omitted.

**Usage:**

```bash
# Create the factory node
pond mkdir /sensors
pond mknod synthetic-timeseries /sensors/synth --config-path synth.yaml

# Query with SQL (table is always named "source")
pond cat /sensors/synth --sql "SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM source"
pond cat /sensors/synth --sql "SELECT * FROM source ORDER BY timestamp LIMIT 10"

# Check value ranges
pond cat /sensors/synth --sql "
  SELECT
    MIN(temperature) AS temp_min, MAX(temperature) AS temp_max,
    MIN(pressure)    AS pres_min, MAX(pressure)    AS pres_max
  FROM source
"
```

**Notes:**
- Data is generated on every query from the config — there is no stored state.
- The node appears as `TableDynamic` in `pond list` output.
- The timestamp column is `Timestamp(Millisecond, UTC)`.
- Point names must be unique; at least one point with at least one component is required.
- Works with `pond describe`, `pond cat --sql`, and any downstream factory
  that reads `series:///` or `file:///` patterns.

---

### dynamic-dir

A virtual directory whose child entries are produced by other factories.
Each entry specifies a `name`, a `factory` type, and a `config` block.
`dynamic-dir` is the glue that lets you compose multiple factory outputs
under a single path — for example, several `timeseries-join` or
`synthetic-timeseries` nodes side-by-side.

```yaml
factory: "dynamic-dir"
config:
  entries:
    - name: "station_a"
      factory: "synthetic-timeseries"
      config:
        start: "2024-01-01T00:00:00Z"
        end:   "2024-01-15T00:00:00Z"
        interval: "1h"
        points:
          - name: "temperature"
            components:
              - type: sine
                amplitude: 5.0
                period: "24h"
                offset: 20.0

    - name: "combined"
      factory: "timeseries-join"
      config:
        inputs:
          - pattern: "series:///sensors/station_a"
            scope: "A"
          - pattern: "series:///sensors/station_b"
            scope: "B"
```

- Nested factory configs are validated recursively at `mknod` time.
- The directory is **read-only** — no `pond copy` into it.
- Child entries appear with the `EntryType` reported by each factory's
  metadata (e.g. `TableDynamic` for timeseries-join).
- Each child gets a deterministic `FileID` derived from the parent's
  `NodeID`, the entry name, factory, and config.

**Usage:**

```bash
pond mkdir /sensors
pond mknod dynamic-dir /sensors/all --config-path all.yaml

# Browse the virtual directory
pond list /sensors/all

# Query a child entry directly
pond cat /sensors/all/station_a --sql "SELECT COUNT(*) FROM source"
```

---

### timeseries-join

Combines two or more time-series inputs into a single wide table by
FULL OUTER JOIN on the time column. Each input can have an optional
**scope** prefix (column names become `Scope.OriginalColumn`), an
optional **time range** filter, and optional **transforms**.

Inputs that share the same `scope` are merged with `UNION BY NAME`
first, then the distinct scope groups are joined. This lets you stitch
together device replacements (same scope, non-overlapping ranges) while
also combining data from different sensor types (different scopes).

```yaml
factory: "timeseries-join"
config:
  time_column: "timestamp"          # optional, default: "timestamp"
  inputs:
    - pattern: "series:///data/station_a"
      scope: "A"
    - pattern: "series:///data/station_b"
      scope: "B"
```

**Input fields:**

| Field | Required | Description |
|-------|----------|-------------|
| `pattern` | yes | URL pattern to match input files. Supported schemes: `series`, `csv`, `excelhtml`, `file`. Glob wildcards (`*`, `**`) are supported. |
| `scope` | no | Prefix added to every non-timestamp column: `Scope.Column`. If omitted, columns keep their original names. |
| `range.begin` | no | ISO 8601 start time — rows before this are excluded. |
| `range.end` | no | ISO 8601 end time — rows after this are excluded. |
| `transforms` | no | List of paths to table-transform factories applied to this input before joining (e.g. `["/system/etc/10-hrename"]`). |

**Behaviour:**

- At least **two inputs** are required (use `sql-derived-series` for one).
- The result is ordered by the time column.
- Where one input has data and another does not, the missing columns are
  `NULL` (FULL OUTER JOIN semantics).
- The output time column is `COALESCE`-d across all scope groups so there
  are no NULLs in the time column itself.
- The node reports `EntryType::TableDynamic`.

**Production example** (combine.yaml inside a `dynamic-dir`):

```yaml
entries:
  - name: "Silver"
    factory: "timeseries-join"
    config:
      inputs:
        - pattern: "/hydrovu/devices/**/SilverVulink1.series"
          scope: Vulink
          range:
            end: 2024-05-30T00:00:00Z
        - pattern: "/hydrovu/devices/**/SilverVulink2.series"
          scope: Vulink
          range:
            begin: 2024-05-30T00:00:00Z
        - pattern: "/hydrovu/devices/**/SilverAT500.series"
          scope: AT500_Surface
```

**Usage:**

```bash
pond mkdir /combined
pond mknod dynamic-dir /combined --config-path combine.yaml

# See all joined series
pond list /combined

# Query the combined data
pond cat /combined/Silver --sql "SELECT * FROM source LIMIT 10"

# Check the time span
pond cat /combined/Silver --sql "
  SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM source
"
```

---

### timeseries-pivot

Selects specific columns from multiple inputs matched by a glob pattern,
producing a wide table with one row per unique timestamp. Use it to pull
a single measurement (e.g. dissolved oxygen) across all sites into one
queryable view.

Each matched input's columns are **scoped** with the captured wildcard
segment as prefix (e.g. `Silver.DO.mg/L`, `BDock.DO.mg/L`). Missing
columns are `NULL`-padded automatically.

```yaml
factory: "timeseries-pivot"
config:
  pattern: "series:///combined/*"     # wildcard captures the site name
  columns:
    - "AT500_Surface.DO.mg/L"
    - "AT500_Bottom.DO.mg/L"
  time_column: "timestamp"            # optional, default: "timestamp"
  transforms:                         # optional
    - "/system/etc/10-hrename"
```

**Config fields:**

| Field | Required | Description |
|-------|----------|-------------|
| `pattern` | yes | URL pattern with a `*` wildcard. The captured segment becomes the scope prefix for that input's columns. Supported schemes: `series`, `csv`, `excelhtml`, `file`, `data`, `table`, `oteljson`. |
| `columns` | yes | List of column names to select from each matched input. At least one column required. |
| `time_column` | no | Name of the time column. Default: `"timestamp"`. |
| `transforms` | no | List of paths to table-transform factories applied to each input. |

**Behaviour:**

- The pattern `series:///combined/*` matching `/combined/Silver` and
  `/combined/BDock` produces columns like:
  `timestamp`, `Silver.AT500_Surface.DO.mg/L`, `BDock.AT500_Surface.DO.mg/L`, …
- Uses LEFT JOIN on the time column (not FULL OUTER JOIN), from a CTE
  of all unique timestamps across all inputs.
- Columns that don't exist in a particular input are `NULL`-padded
  (Float64) via the `null_padding` transform.
- The node reports `EntryType::TableDynamic`.

**Production example** (single.yaml inside a `dynamic-dir`):

```yaml
entries:
  - name: "DO"
    factory: "timeseries-pivot"
    config:
      pattern: "/combined/*"
      columns:
        - "AT500_Surface.DO.mg/L"
        - "AT500_Bottom.DO.mg/L"

  - name: "Temperature"
    factory: "timeseries-pivot"
    config:
      pattern: "/combined/*"
      columns:
        - "AT500_Surface.Temperature.C"
        - "AT500_Bottom.Temperature.C"
```

**Usage:**

```bash
pond mknod dynamic-dir /pivot --config-path single.yaml

# See the pivoted views
pond list /pivot

# Query dissolved oxygen across all sites
pond cat /pivot/DO --sql "SELECT * FROM source ORDER BY timestamp LIMIT 20"
```

---

### temporal-reduce

Creates time-bucketed aggregations of source time-series at one or more
resolutions. The factory produces a **directory** (not a file) with this
structure:

```
/reduce/<site>/res=1h.series
/reduce/<site>/res=6h.series
/reduce/<site>/res=1d.series
```

The source pattern can match multiple files; each match becomes a
separate site subdirectory named by `out_pattern` substitution.

```yaml
factory: "temporal-reduce"
config:
  in_pattern: "series:///sources/*"   # glob — captured group is $0
  out_pattern: "$0"                   # output site name from captured group
  time_column: "timestamp"
  resolutions: ["1h", "6h", "1d"]
  aggregations:
    - type: "avg"
      columns: ["temperature", "pressure"]
    - type: "min"
      columns: ["temperature"]
    - type: "max"
      columns: ["temperature"]
    - type: "count"
      columns: ["*"]                  # count of rows per bucket
```

**Config fields:**

| Field | Required | Description |
|-------|----------|-------------|
| `in_pattern` | yes | URL pattern to match source series. Glob wildcards (`*`, `**`) supported. Captured groups become `$0`, `$1`, … for `out_pattern`. |
| `out_pattern` | yes | Output site name using captured groups (e.g. `"$0"`). |
| `time_column` | yes | Name of the timestamp column in the source. |
| `resolutions` | yes | List of time bucket sizes. Parsed with humantime: `"1h"`, `"6h"`, `"1d"`, `"30m"`, etc. |
| `aggregations` | yes | List of aggregation operations (see below). |
| `transforms` | no | List of paths to table-transform factories applied to each input before aggregation. |

**Aggregation operations:**

| Field | Required | Description |
|-------|----------|-------------|
| `type` | yes | One of `avg`, `min`, `max`, `sum`, `count`. |
| `columns` | no | List of column names to aggregate. Supports single-`*` glob patterns (e.g. `"Vulink*.Temperature.C"`). If omitted, applies to all numeric columns discovered from the source schema. Use `["*"]` with `count` for row count. |

Output column names are `original_column.agg_type` — e.g.
`temperature.avg`, `temperature.min`, `pressure.avg`.

**Behaviour:**

- Uses `DATE_TRUNC` for time bucketing, so buckets align to calendar
  boundaries (hour 0, midnight, etc.).
- Source schema is discovered dynamically on first query — column names
  in `columns` are matched against actual schema at runtime.
- Each resolution file (`res=1h.series`, etc.) is an independent
  `TableDynamic` node backed by `SqlDerivedFile`.
- The directory structure is **read-only**.
- When `in_pattern` matches multiple files mapping to the same
  `out_pattern`, all files are joined via UNION ALL inside
  `SqlDerivedFile`.  This supports rotating-log-file ingestion where
  many data fragments share the same schema.

**Caveat — schema inference:** Schema discovery reads the lexicographically
last matching file (the newest for timestamped filenames like rotating logs).
This works correctly when columns are only added over time — the newest file
has the most complete schema.  If files have **incompatible schemas** (columns
renamed or removed), the inferred schema may be incorrect.  Use
`timeseries-join` for heterogeneous-schema sources that require per-file
schema discovery and column alignment.

**Usage:**

```bash
# Create the source data
pond mkdir /sources
pond mknod synthetic-timeseries /sources/weather --config-path weather.yaml

# Create the temporal-reduce factory
pond mknod temporal-reduce /reduce --config-path reduce.yaml

# Browse the directory structure
pond list /reduce                        # → /reduce/weather
pond list /reduce/weather                # → res=1h.series, res=6h.series, ...

# Query the hourly aggregation
pond cat /reduce/weather/res=1h.series --sql "
  SELECT timestamp,
         ROUND(\"temperature.avg\", 2) AS temp_avg,
         ROUND(\"temperature.min\", 2) AS temp_min,
         ROUND(\"temperature.max\", 2) AS temp_max
  FROM source
  ORDER BY timestamp
"

# Check schema
pond describe /reduce/weather/res=1h.series
```

### sitegen

Static site generator using Maudit + Maud. Reads data from the pond,
applies Markdown templates with shortcodes, and produces a complete
static HTML site with interactive charts (DuckDB-WASM + Vega-Lite).

```yaml
factory: sitegen

site:
  title: "My Dashboard"
  base_url: "/"                       # Use "/subdir/" for non-root deploy

content:
  - name: "pages"
    pattern: "/content/*.md"          # Glob for content pages

exports:
  - name: "params"
    pattern: "/reduced/single_param/*/*.series"
    target_points: 1500               # Points per plot (auto-partitions)
  - name: "sites"
    pattern: "/reduced/single_site/*/*.series"
    target_points: 1500

routes:
  - name: "home"
    type: static
    slug: ""                          # Root: /index.html
    page: "/site/index.md"
    routes:
      - name: "pages"
        type: content
        slug: ""                      # One page per content file
        content: "pages"
      - name: "params"
        type: static
        slug: "params"
        routes:
          - name: "param-detail"
            type: template
            slug: "$0"                # /params/Temperature.html, etc.
            page: "/site/data.md"
            export: "params"

partials:
  sidebar: "/site/sidebar.md"

static:                               # Extra files to copy (text only)
  - pattern: "/img/logo.svg"

sidebar:                              # Ordered sidebar sections (flat pills)
  - "Main"                            # Only pages with section: Main appear
```

**Config fields:**

| Field | Required | Description |
|-------|----------|-------------|
| `site.title` | yes | Site title, used in HTML `<title>` and layout |
| `site.base_url` | yes | Base URL path: `"/"` for root, `"/myapp/"` for subdirectory |
| `content` | no | List of content stages -- glob markdown files for metadata-driven pages |
| `exports` | no | List of data export stages (see below) |
| `reports` | no | Named offline reports built from export stages (see below) |
| `routes` | yes | Hierarchical route tree |
| `partials` | no | Named Markdown partials (e.g., sidebar) |
| `static` | no | List of patterns for static files to copy (text files only) |
| `sidebar` | no | Ordered list of section names for flat sidebar rendering |

**Content fields:**

| Field | Required | Description |
|-------|----------|-------------|
| `name` | yes | Name referenced by `content:` in routes and `content_nav` shortcode |
| `pattern` | yes | Glob pattern matching markdown files (e.g., `"/content/*.md"`) |

**Export fields:**

| Field | Required | Description |
|-------|----------|-------------|
| `name` | yes | Name referenced by `export:` in routes |
| `pattern` | yes | Glob pattern matching pond files. `*` captures become `$0`, `$1`, ... |
| `target_points` | no | Target data points per plot (default: 1500). Used to auto-compute temporal partitions per resolution. |

**Named reports:**

```yaml
reports:
  weekly:
    title: "Weekly pond summary"
    message: "Please check the instruments and glance at the latest data."
    window: 7d
    include_pond_size: true
    sections:
      - export: params
        captures: [well-depth, data, res=1h]
        value: well_depth_value.avg
        unit: m
        summary: range
        chart: true
        href: data/well-depth.html
```

Each section selects exactly one file by matching the wildcard captures from
its named export. A missing `title` reuses the first capture's entry in
`labels`, falling back to a title-cased capture. `range` renders latest,
minimum, and maximum values; `sum` renders a total. Generate a local preview
without credentials or network delivery:

```bash
pond run /system/etc/90-sitegen report weekly ./report-preview
```

The output directory contains `report.html`, `report.txt`, and one PNG per
charted section.

**Route types:**

| Type | Produces | Has `export`? | `slug` semantics |
|------|----------|---------------|------------------|
| `static` | One page at that path | No | Literal slug (empty = root) |
| `template` | One page per unique `$0` value | Yes | `$0` expands from matched captures |
| `content` | One page per content file | No | Uses slug from frontmatter |

**Behaviour:**

- Routes are hierarchical — nested routes inherit the parent slug as prefix.
- Template routes generate one HTML page per unique `$0` value from the linked export.
- Export stages run first: match data files, extract temporal bounds, export Parquet with partitioning.
- Each template page receives its matched `ExportedFile` structs as context for shortcodes.

**Layouts** (set in Markdown frontmatter):

| Layout | Purpose |
|--------|---------|
| `default` | Full-width content (home pages, hero sections) |
| `page` | Sidebar + article wrapper for content pages |
| `data` | Sidebar + chart area with JS chart infrastructure |

**Shortcodes** (used in Markdown templates):

| Shortcode | Purpose | Example |
|-----------|---------|---------|
| `{{ $0 }}` | First capture group value | Page title from pattern match |
| `{{ $1 }}` | Second capture group | Resolution name, etc. |
| `{{ chart /}}` | Chart container + DuckDB-WASM chart renderer | Data pages |
| `{{ breadcrumb /}}` | Breadcrumb navigation from route hierarchy | Data pages |
| `{{ nav_list collection="name" base="/path" /}}` | Navigation list from export collection | Sidebar |
| `{{ content_nav content="pages" /}}` | Navigation from content pages (see below) | Sidebar |
| `{{ base_url /}}` | Site base URL from config | Links in sidebar |
| `{{ site_title /}}` | Site title from config | Sidebar header |
| `{{ figure src="..." caption="..." float="right" /}}` | Figure with optional float | Content pages |

⚠️ **Shortcode syntax**: Use self-closing `{{ name /}}` form. Attributes use
`key="value"` syntax: `{{ nav_list collection="params" base="/params" /}}`.
In the title frontmatter, `{{ $0 }}` does NOT need the closing `/}}`.

**Markdown templates — example data page (`data.md`):**

```markdown
---
title: "{{ $0 }}"
layout: data
---

# {{ $0 }}

{{ breadcrumb /}}

{{ chart /}}
```

**Content page frontmatter:**

Content pages use YAML frontmatter to control navigation and rendering:

```yaml
---
title: Water                          # Page title (shown in nav + <title>)
weight: 10                            # Sort order (lower = higher in nav)
layout: page                          # Layout: default, page, or data
section: Main                         # Sidebar section (must match site.yaml sidebar list)
hidden: true                          # Optional: render page but hide from nav
slug: custom-url                      # Optional: override URL slug (default: filename)
---
```

| Field | Default | Description |
|-------|---------|-------------|
| `title` | filename | Page title |
| `weight` | 100 | Sort priority within section (lower = first) |
| `layout` | `default` | HTML layout template |
| `section` | none | Sidebar section name -- must match a `sidebar:` entry to appear |
| `hidden` | `false` | If true, page is generated but excluded from navigation |
| `slug` | filename | URL slug override |

**Sidebar rendering with `content_nav`:**

The `{{ content_nav content="pages" /}}` shortcode renders sidebar navigation
from content page frontmatter. It has two modes:

**Flat mode** (when `sidebar:` is defined in site.yaml):

Renders a flat `<ul>` of pill-style links. Only pages whose `section:` matches
one of the listed sidebar sections appear. Order follows the `sidebar:` list
(section order), then `weight:` within each section.

```yaml
# site.yaml
sidebar:
  - "Main"          # 9 core pages
```

```yaml
# content/water.md frontmatter
section: Main       # Appears in sidebar
weight: 10          # First in the list
```

```yaml
# content/envdraw.md frontmatter
section: Blog       # "Blog" not in sidebar list, so NOT shown in sidebar
weight: 60          # Reachable via Blog page content instead
```

**Grouped mode** (when `sidebar:` is absent):

Falls back to collapsible section groups. Pages are grouped by `section:`,
with expand/collapse behaviour. The section containing the current page
is auto-expanded.

**Markdown templates — example sidebar (`sidebar.md`):**

```markdown
{{ content_nav content="pages" /}}
```

That single shortcode generates the entire sidebar from frontmatter metadata.
Pages without a matching `section:` or with `hidden: true` are excluded.

**Build command:**

```bash
pond run /system/etc/90-sitegen build ./dist
# or using short name:
pond run 90-sitegen build ./dist
```

This single command: reads the site config → runs export stages (auto-partitions
exported Parquet per resolution based on `target_points`) → renders all routes
(Markdown → HTML via Maud layouts) → writes everything to `./dist`.

**Serving locally:**

```bash
# Vite (live reload on re-build)
npx vite ./dist --port 4174 --open

# or plain HTTP
python3 -m http.server 8000 -d ./dist
```

**Usage:**

```bash
# Store templates in the pond
pond copy host:///path/to/templates /system/site --overwrite

# Create the sitegen factory node
pond mknod sitegen /system/etc/90-sitegen --config-path /path/to/site.yaml

# Build the site
pond run 90-sitegen build ./dist

# Update templates without recreating the factory
pond copy host:///path/to/templates /system/site --overwrite

# Update factory config
pond mknod sitegen /system/etc/90-sitegen --overwrite --config-path /path/to/site.yaml
```

**Notes:**
- The generated site uses DuckDB-WASM to load Parquet files client-side -- no server needed.
- Charts use Vega-Lite for rendering.
- Vendor JS is bundled into the binary and written to the output `vendor/` directory -- no CDN and no Node.js build toolchain required at page load.
- Built-in CSS (`style.css`) and JS (`chart.js`) are compiled into the binary via `include_str!()` and written to the output directory automatically.
- Google Fonts (Inter) are loaded via preconnect links in all layouts.
- The `static:` config copies text files from the pond to the output root (binary files like images must use filename-only paths; SVG works since it is text).
- The site is fully self-contained after build -- deploy to any static host.

**RSS Feed:**

Sitegen can generate an RSS 2.0 feed (`feed.xml`) from blog content pages.
To enable, add `site_url` to the site config:

```yaml
site:
  title: "My Site"
  base_url: "/"
  site_url: "https://example.com"   # Required for RSS (absolute URLs)
```

When `site_url` is set, sitegen automatically:
- Generates `feed.xml` in the output directory
- Adds an RSS icon to the top bar (next to the GitHub icon)
- Adds `<link rel="alternate" type="application/rss+xml">` autodiscovery to all pages

By default, the feed includes all non-hidden content pages with `section: "Blog"`
and a `date:` field, sorted newest-first.

Optional `feed:` section for customization:

```yaml
feed:
  section: "Blog"                # Section to include (default: "Blog")
  content: "pages"               # Content stage name (default: first stage)
  description: "Latest updates"  # Channel description (default: site title)
```

If `site_url` is not set, RSS generation is silently skipped.

---

### column-rename

Transform factory for renaming, casting, or dropping columns. Applied via
the `transforms` field of other factories (timeseries-join, timeseries-pivot,
temporal-reduce).

```yaml
rules:
  - type: direct
    from: "Date Time"
    to: "timestamp"
    cast: timestamp              # Optional: cast to type
  - type: pattern
    pattern: "^(.+) \\((.+)\\)$"
    replacement: "$1.$2"         # Regex capture groups
```

**Rule types:**

| Type | Fields | Description |
|------|--------|-------------|
| `direct` | `from`, `to`, optional `cast` | Rename one column exactly; optionally cast its type |
| `pattern` | `pattern`, `replacement` | Regex match on column names; `$1`, `$2` for capture groups |

**Usage:**

```bash
# Create the transform node
pond mknod column-rename /system/etc/10-hrename --config-path hrename.yaml

# Reference it from other factories via transforms field
```

```yaml
# In a timeseries-join config:
inputs:
  - pattern: "series:///data/readings"
    scope: "Station"
    transforms: ["/system/etc/10-hrename"]
```

**Notes:**
- The transform is applied at query time, not stored — it wraps the source TableProvider.
- Multiple transforms are applied in order.
- The `cast` field supports: `timestamp`, `float64`, `int64`, `utf8`.

---

### remote (REMOVED in D4)

The legacy `remote` factory (`pond mknod remote /system/run/<N>-backup
--config-path ...`) was removed in D4.  Its capabilities are now
delivered by the top-level CLI:

- Configure -> `pond remote add <name> <url> <path>` (pull) or
  `pond backup add <name> <url> [--bidirectional]` (push); see
  [pond remote / pond backup](#pond-remote--pond-backup) above.
- Push     -> `pond push [<name>]` (also auto-runs post-commit).
- Pull     -> `pond pull [<name>]` (after first-pull bootstrap via Rust API).
- Inspect  -> not yet exposed at the CLI (was `pond run .../show`); track
  via `pond log` plus `mc ls`/`aws s3 ls` against the bucket directly.
- Verify   -> not yet exposed at the CLI (was `pond run .../verify`);
  bundle integrity is checked end-to-end inside `pond pull` today.

Cross-pond import is available via `pond remote add NAME URL PATH`
where `PATH != /` (D5.7b).  The legacy `import:` config block remains
unused; cross-pond data is reached through the mount path rather than
through factory-level imports.

### hydrovu

HydroVu API data collection.

```yaml
client_id: "xxx"
client_secret: "yyy"
devices:
  - name: "Station A"
    id: 12345
    scope: "StationA"
```

### logfile-ingest

Mirror rotating log files from the host filesystem into the pond. Tracks files with bao-tree blake3 digests for efficient append detection.

```yaml
# Pattern for archived (immutable) log files
archived_pattern: /var/log/app/app.log.*
# Pattern for the active (append-only) log file
active_pattern: /var/log/app/app.log
# Destination path within the pond
pond_path: /logs/app
# Optional: JSON field holding each record's event time. Setting it declares
# that this node carries logs or metrics, which makes ingest record per-version
# event-time bounds. Omit it for opaque byte streams.
timestamp_field: timeUnixNano
# Unit of that field: seconds | milliseconds | microseconds | nanoseconds
# (default: microseconds)
timestamp_unit: nanoseconds
```

**Usage:**
```bash
# Create the factory node
pond mknod logfile-ingest /system/etc/10-ingest --config-path ingest.yaml

# Run ingestion
pond run /system/etc/10-ingest
pond run 10-ingest              # short name works too

# Verify checksums (b3sum format)
pond run 10-ingest b3sum
```

**Behavior:**
- **Archived files** (matching `archived_pattern`): Immutable - ingested once, verified unchanged
- **Active file** (matching `active_pattern`): Append-only - detects new bytes via cumulative bao-tree hash
- **Rotation detection**: When active file shrinks or content prefix changes, searches for matching archived file

**Event-time bounds (`timestamp_field`):**

`temporal-reduce` keys its partial-aggregate cache on each source version's event-time range. A version stored without one is taken to span *all* of time, so every sealed segment is dropped and the rollup recomputes its whole history on every build — visible only as a slow, memory-hungry build.

Setting `timestamp_field` declares the node temporal and changes three things:

- Each written version records the min/max of that field across its records. Corrupt lines are skipped and counted, matching what the reader does with them (NUL padding is stripped, unparseable lines are dropped), so one torn record in a large rotation does not discard the event times of every other record in it.
- Slices taken from the **active** file are cut at the last newline, so a version never contains a half-written record. The withheld bytes are stored on the next run, once the writer has finished the line; appends are detected by size, so nothing is lost. Archived and rotated files are final and are always stored whole.
- A version in which *no* record yields a usable timestamp **fails the write** rather than being stored unbounded. Reaching that point means the configured field or unit does not match the records.

Leaving `timestamp_field` unset keeps the file an opaque byte stream: no bounds, no alignment, stored byte-for-byte. Use that for anything without one record per line.

**Important:** Files >64KB are stored externally in parquet (not inline in oplog). See `docs/large-file-storage-implementation.md`.

**For Linux system logs (/var/log/syslog):**

Configure logrotate with `nocompress` to keep archived logs readable:

```
# /etc/logrotate.d/syslog-nocompress
/var/log/syslog
/var/log/messages
{
    rotate 4
    weekly
    missingok
    notifempty
    nocompress          # Required for logfile-ingest
    create 644 root adm
    postrotate
        /usr/lib/rsyslog/rsyslog-rotate 2>/dev/null || true
    endscript
}
```

Example configuration for syslog:
```yaml
archived_pattern: /var/log/syslog.*
active_pattern: /var/log/syslog
pond_path: /logs/system
```

**Container testing notes:**

When testing in Docker containers, rsyslog needs kernel logging disabled:
```bash
# Disable imklog module (not available in containers)
sed -i 's/module(load="imklog")/#module(load="imklog")/' /etc/rsyslog.conf
rsyslogd

# Generate log entries with logger command
logger -t myapp "Test message"
```

---

### journal-ingest

Ingest the systemd journal (and optionally the kernel ring buffer) into the
pond as per-unit JSON Lines series. Runs `journalctl -o json` incrementally,
tracking progress with a stored cursor so each run only collects new entries.

```yaml
# Destination path within the pond (required)
pond_path: logs/watershop
# journalctl binary (default: "journalctl")
journalctl_command: journalctl
# Collect the kernel ring buffer as a separate stream (default: true)
collect_kernel: true
# JSON field holding the timestamp, in microseconds since epoch
# (default: "__REALTIME_TIMESTAMP")
timestamp_field: __REALTIME_TIMESTAMP
# Extra args appended to every journalctl invocation (default: []).
# Use ["--merge"] to also pull the invoking user's journal so user-scope
# units (e.g. pond@water-prod.service) are captured.
extra_args: ["--merge"]
```

**Usage:**
```bash
# Create the factory node (manually triggered -> /system/etc/)
pond mknod journal-ingest /system/etc/15-journal --config-path journal.yaml

# Collect new journal entries (default subcommand)
pond run /system/etc/15-journal
pond run 15-journal             # short name works too

# Show the cursor position and per-unit entry counts
pond run 15-journal status
```

**Behavior:**
- Entries are grouped into one `.jsonl` series file per systemd unit under
  `pond_path`: `<unit>.jsonl`, `kernel.jsonl` (kernel transport),
  `user-<unit>.jsonl` (user-scope units), and `other.jsonl` (no unit).
- Output files are `data:series` (FilePhysicalSeries) -- versioned raw JSON
  Lines; each run appends a new version with the collected entries.
- A `.journal-cursor` file in `pond_path` records the last journal cursor, so
  collection is incremental and idempotent across runs (ideal for cron).

---

## Glob Patterns

Watertown uses glob patterns for file matching:

| Pattern | Matches |
|---------|---------|
| `*` | Any single path component |
| `**` | Any number of path components |
| `?` | Any single character |
| `[abc]` | Character class |

Examples:
- `**/*` - All files everywhere
- `/*` - Files in root only
- `/data/**/*.csv` - All CSVs under /data
- `/sensors/*/latest.series` - latest.series in any sensor subdirectory

---

## End-to-End Pipeline Examples

Watertown factories compose into data pipelines. Here are two real-world patterns.

### Multi-Site / Multi-Parameter (Noyo)

The Noyo Harbor example monitors water quality at multiple sites, each with multiple
sensor parameters. The pipeline fans out then fans in:

```
hydrovu (collector)
    → /hydrovu/devices/**/*.series       (raw per-device series)

timeseries-join (/combined)
    → /combined/NorthDock                (all params at one site, wide table)
    → /combined/SouthDock

timeseries-pivot (/singled)
    → /singled/Temperature               (one param across all sites, wide table)
    → /singled/DO

temporal-reduce (/reduced)
    → /reduced/single_site/NorthDock/res={1h,6h,1d}.series
    → /reduced/single_param/Temperature/res={1h,6h,1d}.series

sitegen (/system/etc/90-sitegen)
    → dist/ (HTML + Parquet, served as static site)
```

**Key configs:** `noyo/combine.yaml` (join), `noyo/single.yaml` (pivot),
`noyo/reduce.yaml` (reduce), `noyo/site.yaml` (sitegen).

**Setup pattern:**
```bash
pond init
pond mkdir -p /system/etc
pond mkdir -p /system/site
pond copy host:///path/to/templates /system/site    # Markdown templates
pond mknod column-rename /system/etc/10-hrename --config-path hrename.yaml
pond mknod dynamic-dir /combined --config-path combine.yaml
pond mknod dynamic-dir /singled  --config-path single.yaml
pond mknod dynamic-dir /reduced  --config-path reduce.yaml
pond mknod hydrovu /system/etc/20-hydrovu --config-path hydrovu.yaml
pond mknod sitegen /system/etc/90-sitegen --config-path site.yaml
pond backup add origin s3://my-bucket \
    --region us-east-1 --access-key-id ... --secret-access-key '${env:AWS_SECRET_ACCESS_KEY}'

# Operational cycle:
pond run 20-hydrovu collect              # Collect data
pond run 90-sitegen build ./dist         # Build site
# `pond push origin` runs automatically post-commit (mode=push)
```

### Single-Source (Septic)

The septic station example has a single OTelJSON data source with ~40 metrics from two
scopes (BME280 environment sensor + Orenco Modbus registers) at different sample rates.
There's no need for join/pivot — the `oteljson://` URL scheme provides a single wide table.

```
logfile-ingest (/system/etc/10-ingest)
    → /ingest/septicstation.json         (raw OTelJSON Lines file)

temporal-reduce (/reduced)                using oteljson:// URL scheme
    → /reduced/septic/res={1h,6h,1d}.series

sitegen (/system/etc/90-sitegen)
    → dist/ (HTML + Parquet)
```

**Key difference from Noyo:** The `in_pattern` in `temporal-reduce` uses the
`oteljson://` URL scheme directly, skipping the join/pivot stages:

```yaml
factory: "temporal-reduce"
config:
  in_pattern: "oteljson:///ingest/septicstation.json"
  out_pattern: "septic"
  time_column: "timestamp"
  resolutions: [1h, 6h, 1d]
  aggregations:
    - type: "avg"
      columns:
        - "septicstation_temperature"
        - "orenco_RT_Pump1_Amps"
        # ... all gauge metrics
```

**Setup pattern:**
```bash
pond init
pond mkdir -p /system/etc
pond mkdir -p /system/site
pond mkdir -p /ingest
pond copy host:///path/to/templates /system/site
pond mknod logfile-ingest /system/etc/10-ingest --config-path ingest.yaml
pond backup add origin s3://my-bucket \
    --region us-east-1 --access-key-id ... --secret-access-key '${env:AWS_SECRET_ACCESS_KEY}'
pond mknod temporal-reduce /reduced --config-path reduce.yaml
pond mknod sitegen /system/etc/90-sitegen --config-path site.yaml

# Operational cycle:
pond run 10-ingest                          # ingest new log data
pond run 90-sitegen build ./dist            # build site (reduce is dynamic)
# `pond push origin` runs automatically post-commit (mode=push)
```

**Notes on single-source pipelines:**
- `oteljson://` discovers all metric names as columns automatically (two-pass parser).
- Different sample rates (e.g., 5min Modbus, 10min BME280) produce NULL-padded rows
  in the merged table. `temporal-reduce` aggregations handle NULLs correctly — `avg`,
  `min`, `max` skip NULLs per SQL semantics.
- No glob in `in_pattern` means `out_pattern` is a literal name (not `$0`).
- `temporal-reduce` is a dynamic factory — its output is computed on read. No
  explicit `pond run /reduced update` step is needed; sitegen triggers computation
  when it reads the export data during `build`.

### Choosing a Pipeline Shape

| Scenario | Pipeline | Why |
|----------|----------|-----|
| Multiple sites, multiple params | join → pivot → reduce → sitegen | Need cross-site and cross-param views |
| Single source, many metrics | reduce → sitegen | oteljson:// or csv:// provides the wide table |
| Single source, few metrics | reduce → sitegen | Same, but simpler reduce config |
| Raw file archive only | logfile-ingest + remote | No analysis, just backup |

---

## Factory Type Quick Reference

| Factory | Kind | Created By | Output Type |
|---------|------|------------|-------------|
| `synthetic-timeseries` | dynamic | `pond mknod` | `TableDynamic` |
| `sql-derived-table` | dynamic | `pond mknod` | `TableDynamic` |
| `sql-derived-series` | dynamic | `pond mknod` | `TableDynamic` |
| `dynamic-dir` | dynamic | `pond mknod` | `DynamicDirectory` |
| `timeseries-join` | dynamic | `pond mknod` | `TableDynamic` |
| `timeseries-pivot` | dynamic | `pond mknod` | `TableDynamic` |
| `temporal-reduce` | dynamic | `pond mknod` | `DynamicDirectory` of `TableDynamic` |
| `column-rename` | transform | `pond mknod` | Wraps `TableProvider` |
| `sitegen` | executable | `pond mknod` + `pond run ... build` | Static files on host |
| `logfile-ingest` | executable | `pond mknod` + `pond run` | `data` entries in pond |
| `journal-ingest` | executable | `pond mknod` + `pond run [status]` | `data:series` JSON Lines in pond |
| `hydrovu` | executable | `pond mknod` + `pond run ... collect` | `table:series` in pond |
| `remote` | executable | `pond mknod` + `pond run ... push/pull` | Backup bundles on S3 |

**Dynamic** factories compute on every read — no stored state.
**Executable** factories have side effects — run explicitly with `pond run`.
**Transform** factories are referenced via the `transforms` field of other factories.

---

## Troubleshooting

### "No matching files" when files exist

**Cause**: Pattern doesn't match actual paths.
**Fix**: Use `pond list '**/*'` first to see what exists, then refine pattern.

### `pond cat` outputs binary garbage

**Cause**: You're catting a table/series entry without `--format=table` or `--sql`.
The default output format is Parquet bytes (binary).
**Fix**: Add `--format=table` for human-readable output, or `--sql "SELECT * FROM source LIMIT 10"`.

### `--format` does the wrong thing

**Cause**: `--format` on `pond cat` controls output display (raw vs table).
`pond copy` does not use `--format` — entry type is in the source URL.
**Fix**: For `pond cat`, use `--format=raw` (default, binary) or `--format=table` (ASCII).
For `pond copy`, use `host:///` (data), `host+table:///` (Parquet), or `host+series:///` (time-series).

### `host+table:///` errors on CSV

**Cause**: `host+table:///` validates PAR1 magic bytes — only Parquet input accepted.
**Fix**: Use `host:///` for CSV (stores as raw bytes). Query CSV with `pond cat csv:///path --sql "..."`.

### Export pattern matches nothing in sitegen

**Cause**: The export `pattern` in site.yaml doesn't match what temporal-reduce creates.
**Fix**: Run `pond list '/reduced/**'` to see the actual directory structure. Common
mismatch: the reduce output might be `/reduced/site/res=1h.series` but the export pattern
expects `/reduced/site/*.series` or `/reduced/site/*/*.series`.

### oteljson:// queries are slow

**Cause**: The two-pass parser reads the entire file twice — once to discover columns,
once to parse data. Large files (>100MB) can be slow.
**Fix**: For repeated queries, consider ingesting into a table:series via SQL pipeline.
For one-off exploration, use `--sql` with `LIMIT` to reduce output.

### Dynamic factory shows stale data

**Cause**: Dynamic factories are re-computed on every read, but they read from their
source which may itself be cached or stale.
**Fix**: For logfile-ingest sources, run `pond run 10-ingest` first to ingest new data.
The dynamic factory chain (temporal-reduce → sitegen) will then see the updated source.

### temporal-reduce columns not found

**Cause**: Column names in `aggregations.columns` don't match the source schema.
Column names are matched against the actual schema at runtime.
**Fix**: Discover column names first:
```bash
pond cat oteljson:///ingest/data.json --format=table --sql "
  SELECT column_name FROM information_schema.columns
  WHERE table_name = 'source' ORDER BY column_name
"
```

### sitegen build fails with "unknown shortcode"

**Cause**: Typo in Markdown template or using wrong shortcode syntax.
**Fix**: Check shortcode names: `{{ $0 }}`, `{{ chart /}}`, `{{ breadcrumb /}}`,
`{{ nav_list collection="..." base="..." /}}`. Note the self-closing `/}}` syntax.

---

## See Also

- [testsuite/](../testsuite/) - Runnable test scripts
- [watertown-overview.md](watertown-overview.md) - Architecture overview
