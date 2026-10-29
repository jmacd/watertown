---
title: CLI command reference
doc_id: reference:cli
---

# CLI command reference

Mechanical interfaces below come from the same Clap definitions as `pond`.
Defaults shown are parser defaults, not every operational fallback. Parent
commands group their documented subcommands. Global options are listed
[separately](global-options.md).

## Choosing an operation

| Commands | Mode, side effects, and verified entry points |
|---|---|
| `init` | Creates a new pond identity at `--pond` / `POND`; required immutable birthplace. [Synthetic example](../factories/synthetic-timeseries.md). |
| `list`, `describe`, `cat` | Pond or explicit host input. Listing enumerates; describe inspects schema; cat reads bytes or SQL results. A dynamic read can initialize/query caches or contact its declared source. [CSV](../formats/csv.md). |
| `mkdir`, `copy`, `mknod`, `apply` | Pond mutations. Copy can import host input; mknod installs a factory/config, not collected measurements. Apply is declarative create/update. Existing destinations/configs need the command's explicit semantics, not an assumed overwrite. |
| `list-factories` | Lists linked capabilities without a pond. Registration is not proof of host support or service readiness. |
| `run` | Executes a registered factory against the selected context. Host sitegen is supported; writable collectors/exec require a pond. Arguments after `--` belong to the factory, whose own help/contract defines them. |
| `export` | Writes host Parquet from queryable input; not a whole-pond backup. [Export semantics](../export.md). |
| `show`, `log`, `status`, `limits` | Pond/operator inspection. Log lifecycle data can differ from replicated logical data. Limits reports bounded limiter state, not a rescan of usage history. |
| `backup`, `push` | Configure/advance whole-pond push destinations; may contact services and upload immutable data/publication records. [Recovery package](../../guides/operations.md). |
| `remote`, `pull` | Configure/refresh pull mirrors or foreign imports. Removing an attachment is not deleting remote storage. `--purge` additionally removes a local import mount. Rebuild-graft requires a named remote. |
| `restore` | Requires an absent local target and adopts the backup's identity; then tracks it by pull. Not cross-pond import. |
| `verify`, `fsck`, `tlog` | Remote checksum/tip verification, local integrity, and transparency-log inspection/verification are distinct checks. `--exact` requires tip equality. |
| `recover`, `rebuild-control` | Repair interrupted transaction metadata or reconstruct control state. Preserve evidence and assess status before mutation; not substitutes for independent backups. |
| `maintain` | Local checkpoint/vacuum and explicitly selected compaction/pruning/pack operations. Start with applicable dry-run inspection. Retention-only pruning can destroy recoverable history. |
| `capsule` | Publish recipes/snapshots or inspect/verify/import downloaded recovery data. Import uses a fresh identity and resumable staging; activation is a separate safety preflight. |
| `freeze` | Persistently freeze data writes, inspect the protected tip, or explicitly re-enable writes. Does not stop external source changes. |
| `config` | Show/set pond configuration, not arbitrary factory YAML fields. Changing operational settings can affect future transactions. |
| `emergency erase-bucket` | Destructive external-storage deletion requiring explicit dangerous confirmation. Never part of quickstart, recovery, or routine validation. |

Use the [operations guide](../../guides/operations.md) for ordering, recovery
boundaries, capsule activation, and maintenance risks. Service-dependent
commands are not reported as offline-tested just because their parser is
compiled. Test `214` covers file-backed backup/restore/pull and subsequent
updates; tests `057`/`100` cover the small query/install workflows.

<!-- docs:reference cli:commands -->
## apply


```text
pond apply
```

Apply configuration files (idempotent create-or-update)  Each file contains a prelude (kind, path, version) separated from the config body by '---'. Creates new nodes or updates changed ones.

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -f | 1.. value(s) | yes | not specified | Configuration files to apply |
| -h, --help | flag (no value) | no | not specified | Print help (see a summary with '-h') |

Global options are documented in [Global options](global-options.md).


## backup


```text
pond backup
```

Manage backup attachments (push-side of remote split, D5.7b)

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |

Global options are documented in [Global options](global-options.md).


## backup add


```text
pond backup add
```

Attach a backup remote (push-only by default; --bidirectional makes it push+pull).  Backups always mirror the entire pond

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| --access-key-id, --access-key | 1 value(s) | no | not specified | S3 access key id |
| --allow-http | flag (no value) | no | false | Allow plain HTTP (required for local MinIO) |
| --bidirectional | flag (no value) | no | false | Also pull from this remote (push + pull, i.e. mode=both) |
| --endpoint | 1 value(s) | no | not specified | Custom S3 endpoint (e.g., for MinIO or R2) |
| -h, --help | flag (no value) | no | not specified | Print help |
| NAME | 1 value(s) | yes | not specified | Logical name for the backup (e.g., "origin", "backup-s3") |
| --overwrite | flag (no value) | no | false | Replace an existing attachment of the same name |
| --region | 1 value(s) | no | not specified | AWS region (S3 only) |
| --secret-access-key, --secret-key | 1 value(s) | no | not specified | S3 secret access key |
| URL | 1 value(s) | yes | not specified | Remote URL (file:///path or s3://bucket/prefix) |

Global options are documented in [Global options](global-options.md).


## backup cleanup-uploads


```text
pond backup cleanup-uploads
```

Delete abandoned immutable-payload upload staging objects

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |
| NAME | 1 value(s) | yes | not specified | Logical name of the backup |
| --older-than-seconds | 1 value(s) | no | 86400 | Delete staging objects at least this old |

Global options are documented in [Global options](global-options.md).


## backup list


```text
pond backup list
```

List backup-mode attachments only

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |

Global options are documented in [Global options](global-options.md).


## backup publish-consolidated


```text
pond backup publish-consolidated
```

Publish verified local whole-range series packs to this backup.  This is the only operation that installs remote consolidated-pack locators. It validates the backup's pond identity and exact current main publication, uploads immutable physical objects, then each pack, then its locator. It never runs as part of collection or ordinary push.

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help (see a summary with '-h') |
| NAME | 1 value(s) | yes | not specified | Logical name of a push/both backup |

Global options are documented in [Global options](global-options.md).


## backup remove


```text
pond backup remove
```

Remove a backup attachment (alias of pond remote remove for symmetry; same semantics: deletes config and clears watermarks). Backups have no mount_path, so --purge is a no-op for them but accepted for symmetry

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |
| NAME | 1 value(s) | yes | not specified | Logical name of the backup to remove |
| --purge | flag (no value) | no | false | Accepted for symmetry with pond remote remove --purge; has no effect on backups (no mount entry to purge) |

Global options are documented in [Global options](global-options.md).


## capsule


```text
pond capsule
```

Install recovery recipes or verify portable recovery capsules

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |

Global options are documented in [Global options](global-options.md).


## capsule activate


```text
pond capsule activate
```

Safely enable post-commit dispatch on a completed capsule import.  Reads and validates every restored /sys/remotes/* attachment and /system/run/* dynamic config without executing factories. Dispatch remains suppressed if any entry is invalid or unsafe.

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help (see a summary with '-h') |

Global options are documented in [Global options](global-options.md).


## capsule import


```text
pond capsule import
```

Import a pondcapsule.4 into a brand-new pond.  The target must not already exist. A private sibling staging directory is created next to it, given a fresh pond identity, and populated in bounded, journaled transactions with post-commit factory execution and remote auto-push suppressed. Retrying the same command resumes the matching staging directory at an exact entry/leaf cursor. Only after the staged result is re-verified against the capsule's logical contract is it renamed atomically onto the target. On any failure the staging directory is left in place for inspection rather than silently removed. The promoted pond remains inert until pond capsule activate passes its safety preflight.

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| --birthplace | 1 value(s) | yes | not specified | Birthplace of the freshly minted target pond identity (see pond init --birthplace) |
| -h, --help | flag (no value) | no | not specified | Print help (see a summary with '-h') |
| PATH | 1 value(s) | yes | not specified | Directory containing the downloaded recovery/ tree |

Global options are documented in [Global options](global-options.md).


## capsule inspect


```text
pond capsule inspect
```

Verify a downloaded pondcapsule.4 without opening a pond

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |
| PATH | 1 value(s) | yes | not specified | Directory containing the downloaded recovery/ tree |

Global options are documented in [Global options](global-options.md).


## capsule publish


```text
pond capsule publish
```

Build and publish the current portable pondcapsule.4 snapshot

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |
| NAME | 1 value(s) | yes | not specified | Logical backup name |

Global options are documented in [Global options](global-options.md).


## capsule recipe


```text
pond capsule recipe
```

Publish or inspect a static native-format recovery recipe

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |

Global options are documented in [Global options](global-options.md).


## capsule recipe inspect


```text
pond capsule recipe inspect
```

Verify the watertown.commit.v1 -&gt; pondcapsule.4 recipe

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |
| NAME | 1 value(s) | yes | not specified | Remote attachment name |

Global options are documented in [Global options](global-options.md).


## capsule recipe publish


```text
pond capsule recipe publish
```

Install the watertown.commit.v1 -&gt; pondcapsule.4 recipe

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |
| NAME | 1 value(s) | yes | not specified | Backup attachment name |

Global options are documented in [Global options](global-options.md).


## capsule verify


```text
pond capsule verify
```

Verify a pondcapsule.4 and all leaf identities

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |
| PATH | 1 value(s) | yes | not specified | Directory containing the downloaded recovery/ tree |

Global options are documented in [Global options](global-options.md).


## cat


```text
pond cat
```

Read a file from the pond

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| --explain | flag (no value) | no | false | Show pattern resolution and schema without executing the query |
| --format | 1 value(s) | no | auto | Output format [default: auto] [possible values: auto, raw, table] auto: table for queryable types when stdout is a TTY, raw otherwise |
| -h, --help | flag (no value) | no | not specified | Print help |
| PATH | 1 value(s) | yes | not specified | File path to read |
| --sql, --query | 1 value(s) | no | not specified | SQL query to execute on the file:series data |
| --time-end | 1 value(s) | no | not specified | Time range end (Unix timestamp in milliseconds, optional) |
| --time-start | 1 value(s) | no | not specified | Time range start (Unix timestamp in milliseconds, optional) |

Global options are documented in [Global options](global-options.md).


## config


```text
pond config
```

Show or set pond configuration

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |

Global options are documented in [Global options](global-options.md).


## config set


```text
pond config set
```

Set a configuration value

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |
| KEY | 1 value(s) | yes | not specified | Configuration key |
| VALUE | 1 value(s) | yes | not specified | Configuration value |

Global options are documented in [Global options](global-options.md).


## copy


```text
pond copy
```

Copy files into or out of the pond  Copy IN (host -&gt; pond): pond copy host:///file.csv /dest/path Entry type is encoded in the source URL: host:///file.csv              -&gt; raw data (default) host+table:///file.parquet    -&gt; queryable parquet table host+series:///file.parquet   -&gt; time-series parquet Copy OUT (pond -&gt; host): pond copy '/pattern/**/*.series' host:///output/dir

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| DEST | 1 value(s) | yes | not specified | Destination: pond path (copy IN) or host:///path (copy OUT) |
| -h, --help | flag (no value) | no | not specified | Print help (see a summary with '-h') |
| SOURCES | 1.. value(s) | yes | not specified | Source paths: host URLs (copy IN) or pond paths/patterns (copy OUT) |
| --strip-prefix | 1 value(s) | no | not specified | Strip a leading path prefix from pond paths when copying OUT. e.g. --strip-prefix=/hydrovu copies /hydrovu/devices/123/foo.series to &lt;dest&gt;/devices/123/foo.series instead of &lt;dest&gt;/hydrovu/devices/123/foo.series |

Global options are documented in [Global options](global-options.md).


## describe


```text
pond describe
```

Describe file schemas and types

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |
| PATTERN | 1 value(s) | no | **/* | Pattern to match (supports wildcards, defaults to "**/*") |

Global options are documented in [Global options](global-options.md).


## emergency


```text
pond emergency
```

Emergency operations (destructive, use with care)

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |

Global options are documented in [Global options](global-options.md).


## emergency erase-bucket


```text
pond emergency erase-bucket
```

Erase all objects in an S3 bucket. Requires --dangerous flag

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| --access-key | 1 value(s) | yes | not specified | S3 access key |
| --allow-http | flag (no value) | no | false | Allow HTTP (non-TLS) connections |
| --dangerous | flag (no value) | no | false | Required safety flag to confirm destructive operation |
| --endpoint | 1 value(s) | yes | not specified | S3 endpoint (e.g., "http://localhost:9000" for MinIO) |
| -h, --help | flag (no value) | no | not specified | Print help |
| --region | 1 value(s) | no | us-east-1 | S3 region |
| --secret-key | 1 value(s) | yes | not specified | S3 secret key |
| URL | 1 value(s) | yes | not specified | S3 bucket URL (e.g., "s3://water-staging") |

Global options are documented in [Global options](global-options.md).


## export


```text
pond export
```

Export pond data to external Parquet files with time partitioning

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| --dir | 1 value(s) | yes | not specified | Output directory for exported files |
| --end-time | 1 value(s) | no | not specified | Time range end (human-readable, e.g., "2024-12-31 23:59:59", "2024-12-31T23:59:59Z") |
| -h, --help | flag (no value) | no | not specified | Print help |
| -p, --pattern | 1 value(s) | no | not specified | File patterns to export (e.g., "/sensors/*.series") |
| --rebuild | flag (no value) | no | false | Drop the temporal-reduce partial-aggregate cache before exporting, forcing a full recompute. A maintenance escape hatch: the cache is derived state, so discarding it can only cost time |
| --start-time | 1 value(s) | no | not specified | Time range start (human-readable, e.g., "2024-01-01 00:00:00", "2024-01-01T00:00:00Z") |
| --temporal | 1 value(s) | no |  | Temporal partitioning levels (comma-separated: year,month,day,hour,minute) |

Global options are documented in [Global options](global-options.md).


## freeze


```text
pond freeze
```

Manage the persistent pond-wide data-write freeze

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |

Global options are documented in [Global options](global-options.md).


## freeze disable


```text
pond freeze disable
```

Re-enable data writes

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |

Global options are documented in [Global options](global-options.md).


## freeze enable


```text
pond freeze enable
```

Reject future data writes; fails while another writer holds the pond

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |
| --reason | 1 value(s) | yes | not specified | Auditable operator reason for freezing this pond |

Global options are documented in [Global options](global-options.md).


## freeze status


```text
pond freeze status
```

Report whether writes are frozen and show the protected content tip

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |

Global options are documented in [Global options](global-options.md).


## fsck


```text
pond fsck
```

Filesystem-check: verify content checksums and Merkle trees, and print a single root checksum that exhaustively fingerprints every row in the pond (across all pond_ids).  Two replicas are identical iff their root checksums match

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |
| --quick | flag (no value) | no | false | Skip the content-rehash pass; compute only the structural root checksum (fast) |
| --verbose | flag (no value) | no | false | Print a per-partition breakdown and content statistics in addition to the root checksum |

Global options are documented in [Global options](global-options.md).


## init


```text
pond init
```

Initialize a new pond

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| --birthplace | 1 value(s) | yes | not specified | Birthplace of the pond: an immutable label for where this pond is created (for example a hostname, site, or deployment name). Recorded permanently in the pond's identity metadata |
| -h, --help | flag (no value) | no | not specified | Print help |

Global options are documented in [Global options](global-options.md).


## limits


```text
pond limits
```

Show bounded per-limiter state without scanning usage history

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| --format | table, json, jsonl | no | table | Output format for operators or collectors |
| -h, --help | flag (no value) | no | not specified | Print help |

Global options are documented in [Global options](global-options.md).


## list


```text
pond list
```

List files and directories (ls -l style)

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -a, --all | flag (no value) | no | false | Show all files including hidden ones |
| -h, --help | flag (no value) | no | not specified | Print help |
| -l, --long | flag (no value) | no | false | Show full entry type names (e.g., table:series, table:dynamic) |
| PATTERN | 1 value(s) | no | **/* | Pattern to match (supports wildcards, defaults to "**/*") |

Global options are documented in [Global options](global-options.md).


## list-factories


```text
pond list-factories
```

List available dynamic node factories

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |

Global options are documented in [Global options](global-options.md).


## log


```text
pond log
```

View transaction history and audit trail

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |
| --incomplete | flag (no value) | no | false | Show incomplete operations (for recovery) Conflicts with: txn_seq. |
| --limit | 1 value(s) | no | 10 | Number of recent transactions to show |
| --txn-seq | 1 value(s) | no | not specified | Show detailed lifecycle for a specific transaction sequence number Conflicts with: incomplete. |

Global options are documented in [Global options](global-options.md).


## maintain


```text
pond maintain
```

Run Delta Lake maintenance (checkpoint, vacuum, optional compaction)

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| --allow-no-remote | flag (no value) | no | false | With --prune: permit pruning when no push-mode remote is attached (retention-only; pruned history is then unrecoverable) |
| --collapse-versions | 1 value(s) | no | 0 | Repack native-v2 series whose live physical fanout exceeds this threshold into verified whole-range packs under local data/_packs. Pass 0 to disable (the default). This never rewrites or reclaims user logical-series rows; publish selected packs explicitly with pond backup publish-consolidated NAME |
| --compact | flag (no value) | no | false | Also compact small parquet files into larger ones |
| --dry-run | flag (no value) | no | false | Report which local series packs would be rebuilt, without changing anything |
| -h, --help | flag (no value) | no | not specified | Print help |
| --keep-txns | 1 value(s) | no | 1000 | With --prune: number of most-recent transactions to always retain |
| --prune | flag (no value) | no | false | Also prune replicated control-table lifecycle history at or below a safe horizon, reclaimed by this same checkpoint + vacuum pass |

Global options are documented in [Global options](global-options.md).


## mkdir


```text
pond mkdir
```

Create a directory in the pond

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |
| -p, --parents | flag (no value) | no | false | Create parent directories as needed (like mkdir -p) |
| PATH | 1 value(s) | yes | not specified | Directory path to create |

Global options are documented in [Global options](global-options.md).


## mknod


```text
pond mknod
```

Create node (factory objects like CSV, SQL views, etc.)

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| --config-path | 1 value(s) | yes | not specified | Path to configuration file for the factory |
| FACTORY_TYPE | 1 value(s) | yes | not specified | Factory type to create (use 'list-factories' to see all available types) |
| -h, --help | flag (no value) | no | not specified | Print help |
| --overwrite | flag (no value) | no | false | Overwrite existing dynamic node with new configuration |
| PATH | 1 value(s) | yes | not specified | Path where the node will be created |

Global options are documented in [Global options](global-options.md).


## pull


```text
pond pull
```

Pull new bundles from one or more remotes (D4)

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |
| NAME | 1 value(s) | no | not specified | Remote name.  Omit to pull every remote in pull or both mode |
| --rebuild-graft | flag (no value) | no | false | Atomically replace this remote's graft partition, mount, and pin |

Global options are documented in [Global options](global-options.md).


## push


```text
pond push
```

Push pending local transactions to one or more remotes (D4)

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |
| NAME | 1 value(s) | no | not specified | Remote name (from pond remote add).  Omit to push every remote in push or both mode |

Global options are documented in [Global options](global-options.md).


## rebuild-control


```text
pond rebuild-control
```

Rebuild the control table from the data Delta table when the control table is lost or corrupt (D6).  Recovers pond identity and the transaction-log skeleton; settings/watermarks must be re-established by re-attaching remotes

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| --force | flag (no value) | no | false | Move an existing control table aside (to control.bak.&lt;ts&gt;) and rebuild.  Required when a control table already exists |
| -h, --help | flag (no value) | no | not specified | Print help |

Global options are documented in [Global options](global-options.md).


## recover


```text
pond recover
```

Recover from crash by checking and restoring transaction metadata

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |

Global options are documented in [Global options](global-options.md).


## remote


```text
pond remote
```

Manage remote attachments under /sys/remotes/ (D4)

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |

Global options are documented in [Global options](global-options.md).


## remote add


```text
pond remote add
```

Attach a pull-mode remote and mount it at PATH.  PATH = / is a mirror restart (foreign store_id must match this pond's pond_id).  Non-root PATH is a cross-pond import (foreign store_id must differ).

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| --access-key-id, --access-key | 1 value(s) | no | not specified | S3 access key id |
| --allow-http | flag (no value) | no | false | Allow plain HTTP (required for local MinIO) |
| --endpoint | 1 value(s) | no | not specified | Custom S3 endpoint (e.g., for MinIO or R2) |
| -h, --help | flag (no value) | no | not specified | Print help (see a summary with '-h') |
| NAME | 1 value(s) | yes | not specified | Logical name for the remote (e.g., "upstream") |
| --overwrite | flag (no value) | no | false | Replace an existing attachment of the same name |
| PATH | 1 value(s) | yes | not specified | In-pond mount path.  Use / for a mirror restart of this pond's own backup, or /imports/&lt;name&gt; for cross-pond import |
| --region | 1 value(s) | no | not specified | AWS region (S3 only) |
| --secret-access-key, --secret-key | 1 value(s) | no | not specified | S3 secret access key |
| URL | 1 value(s) | yes | not specified | Remote URL (file:///path or s3://bucket/prefix) |

Global options are documented in [Global options](global-options.md).


## remote list


```text
pond remote list
```

List all attached remotes (both pull and backup)

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |

Global options are documented in [Global options](global-options.md).


## remote remove


```text
pond remote remove
```

Remove a remote attachment and clear its watermarks.  By default this is a **detach**: the YAML config and watermark keys are removed but any cross-pond mount entry materialized at mount_path is kept (the imported data snapshot stays readable).  Pass --purge to also remove the mount entry itself (the foreign data becomes inaccessible by path even if it remains in the Delta log).

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help (see a summary with '-h') |
| NAME | 1 value(s) | yes | not specified | Logical name of the remote to remove |
| --purge | flag (no value) | no | false | Also remove the mount entry at the remote's mount_path (cross-pond imports only).  Without this flag the imported data remains visible via the existing mount path |

Global options are documented in [Global options](global-options.md).


## restore


```text
pond restore
```

Restore a whole pond from a backup published to a remote.  Bootstraps a FRESH replica: discovers the source pond's id from the remote, stamps a local pond carrying that id, attaches the remote as a mirror at /, and pulls the full content graph.  Refuses to run over an existing pond.  Use pond pull &lt;name&gt; afterward to track the upstream.

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| --access-key-id, --access-key | 1 value(s) | no | not specified | S3 access key id |
| --allow-http | flag (no value) | no | false | Allow plain HTTP (required for local MinIO) |
| --endpoint | 1 value(s) | no | not specified | Custom S3 endpoint (e.g., for MinIO or R2) |
| -h, --help | flag (no value) | no | not specified | Print help (see a summary with '-h') |
| NAME | 1 value(s) | yes | not specified | Logical name to attach the restored mirror under (e.g., "origin") |
| --overwrite | flag (no value) | no | false | Replace an existing attachment of the same name |
| --region | 1 value(s) | no | not specified | AWS region (S3 only) |
| --secret-access-key, --secret-key | 1 value(s) | no | not specified | S3 secret access key |
| URL | 1 value(s) | yes | not specified | Remote URL (file:///path or s3://bucket/prefix) of the backup |

Global options are documented in [Global options](global-options.md).


## run


```text
pond run
```

Execute a run configuration (e.g., hydrovu collector)

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| ARGS | 0.. value(s) | no | not specified | Additional arguments to pass to the factory (e.g., subcommands like 'replicate', 'list-bundles') |
| -h, --help | flag (no value) | no | not specified | Print help |
| PATH | 1 value(s) | yes | not specified | Path to the configuration file to execute |

Global options are documented in [Global options](global-options.md).


## show


```text
pond show
```

Show pond contents

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |
| -m, --mode | 1 value(s) | no | brief | Display mode: brief (summary stats), concise (1 line per tx), or detailed (full dump) |

Global options are documented in [Global options](global-options.md).


## status


```text
pond status
```

Show an operator-facing status aggregate: identity, local commit state, recovery health, and per-remote sync watermarks (D6)

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |

Global options are documented in [Global options](global-options.md).


## tlog


```text
pond tlog
```

Inspect and verify the pond's transparency log (D5)

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |

Global options are documented in [Global options](global-options.md).


## tlog show


```text
pond tlog show
```

Show the current checkpoint, checkpoint history, and tree size

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |

Global options are documented in [Global options](global-options.md).


## tlog verify


```text
pond tlog verify
```

Verify inclusion of every leaf, append-only consistency across every published checkpoint, and faithfulness to the control-table commit spine

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -h, --help | flag (no value) | no | not specified | Print help |

Global options are documented in [Global options](global-options.md).


## verify


```text
pond verify
```

Verify local data matches one or more remotes' recorded checksums (D6)

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| --exact | flag (no value) | no | false | Exit nonzero unless every remote tip exactly equals the local tip |
| -h, --help | flag (no value) | no | not specified | Print help |
| NAME | 1 value(s) | no | not specified | Remote or backup name.  Omit to verify against every attachment |

Global options are documented in [Global options](global-options.md).
<!-- docs:end-reference -->
