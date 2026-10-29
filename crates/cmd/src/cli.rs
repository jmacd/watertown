// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Shared Clap definitions for the runtime CLI and developer documentation exporter.

use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

/// Control table subcommands (hidden -- use `pond log`, `pond sync`, `pond config` instead)
#[derive(Debug, Subcommand)]
pub enum ControlCommand {
    /// Show recent transactions with summary status
    Recent {
        /// Number of recent transactions to show
        #[arg(long, default_value = "10")]
        limit: usize,
    },
    /// Show detailed lifecycle for a specific transaction
    Detail {
        /// Transaction sequence number
        #[arg(long)]
        txn_seq: i64,
    },
    /// Show incomplete operations (for recovery)
    Incomplete,
    /// Show pond configuration (ID, factory modes, metadata, settings)
    ShowConfig,
    /// Set a configuration value
    SetConfig {
        /// Configuration key
        key: String,
        /// Configuration value
        value: String,
    },
    /// Shrink the control table by deleting replicated lifecycle history.
    ///
    /// Setting rows (watermarks, store_id, mount, mode) are always kept.
    /// The prune horizon is the lesser of the minimum push-remote
    /// `last_pushed_seq` and `last_committed - keep-txns`, so only
    /// fully-replicated, beyond-retention transactions are removed.  A
    /// checkpoint + vacuum runs afterwards to reclaim disk space.
    Prune {
        /// Number of most-recent transactions to always retain for
        /// `pond log` / recovery, even if already replicated.
        #[arg(long, default_value = "1000")]
        keep_txns: i64,
        /// Compute and report the horizon without deleting anything.
        #[arg(long)]
        dry_run: bool,
        /// Permit pruning when no push-mode remote is attached.  Pruned
        /// history is then unrecoverable and a future remote must
        /// bootstrap via a fresh `pond pull`.
        #[arg(long)]
        allow_no_remote: bool,
    },
}

/// Config subcommands
#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    /// Set a configuration value
    Set {
        /// Configuration key
        key: String,
        /// Configuration value
        value: String,
    },
}

/// Persistent pond-wide data-write freeze operations.
#[derive(Debug, Subcommand)]
pub enum FreezeCommand {
    /// Reject future data writes; fails while another writer holds the pond.
    Enable {
        /// Auditable operator reason for freezing this pond.
        #[arg(long)]
        reason: String,
    },
    /// Report whether writes are frozen and show the protected content tip.
    Status,
    /// Re-enable data writes.
    Disable,
}

#[derive(Parser)]
#[command(author, version = env!("WATERTOWN_VERSION"), about = "Watertown - A very small data lake")]
#[command(name = "pond")]
pub struct Cli {
    /// Pond path override (defaults to POND env var)
    #[arg(long, global = true)]
    pub pond: Option<PathBuf>,

    /// Host directory root for host+ URL operations (defaults to none;
    /// when set, host+ paths are resolved relative to this directory)
    #[arg(short = 'd', long = "directory", global = true)]
    pub directory: Option<PathBuf>,

    /// Mount factory definitions onto host filesystem paths.
    /// Format: <mount_path>=host+<factory>:///<config_path>
    /// Example: --hostmount /reduced=host+dyndir:///reduce.yaml
    #[arg(long = "hostmount", global = true)]
    pub hostmount: Vec<String>,

    #[command(subcommand)]
    pub command: Commands,
}

/// Remote management subcommands (D4: replaces `/system/run/<N>-remote`
/// factory configs with `/sys/remotes/<name>` YAML attachments).
///
/// As of D5.7b the verb is split: `pond remote add` always attaches a
/// **pull-only** remote (mirror restart or cross-pond import).  Use
/// `pond backup add` to attach a push-mode (or bidirectional) remote.
#[derive(Debug, Subcommand)]
pub enum RemoteCommand {
    /// Attach a pull-mode remote and mount it at PATH.
    ///
    /// `PATH = /` is a mirror restart (foreign store_id must match this
    /// pond's pond_id).  Non-root PATH is a cross-pond import (foreign
    /// store_id must differ).
    Add {
        /// Logical name for the remote (e.g., "upstream").
        name: String,
        /// Remote URL (`file:///path` or `s3://bucket/prefix`).
        url: String,
        /// In-pond mount path.  Use `/` for a mirror restart of this
        /// pond's own backup, or `/imports/<name>` for cross-pond import.
        path: String,
        #[command(flatten)]
        options: RemoteAddOptions,
    },
    /// Remove a remote attachment and clear its watermarks.
    ///
    /// By default this is a **detach**: the YAML config and watermark
    /// keys are removed but any cross-pond mount entry materialized
    /// at `mount_path` is kept (the imported data snapshot stays
    /// readable).  Pass `--purge` to also remove the mount entry
    /// itself (the foreign data becomes inaccessible by path even if
    /// it remains in the Delta log).
    Remove {
        /// Logical name of the remote to remove.
        name: String,
        /// Also remove the mount entry at the remote's `mount_path`
        /// (cross-pond imports only).  Without this flag the imported
        /// data remains visible via the existing mount path.
        #[arg(long)]
        purge: bool,
    },
    /// List all attached remotes (both pull and backup).
    List,
}

/// Backup-side subcommands (D5.7b): the push half of the remote split.
#[derive(Debug, Subcommand)]
pub enum BackupCommand {
    /// Attach a backup remote (push-only by default; `--bidirectional`
    /// makes it push+pull).  Backups always mirror the entire pond.
    Add {
        /// Logical name for the backup (e.g., "origin", "backup-s3").
        name: String,
        /// Remote URL (`file:///path` or `s3://bucket/prefix`).
        url: String,
        /// Also pull from this remote (push + pull, i.e. mode=both).
        #[arg(long)]
        bidirectional: bool,
        #[command(flatten)]
        options: RemoteAddOptions,
    },
    /// Remove a backup attachment (alias of `pond remote remove` for
    /// symmetry; same semantics: deletes config and clears watermarks).
    /// Backups have no `mount_path`, so `--purge` is a no-op for them
    /// but accepted for symmetry.
    Remove {
        /// Logical name of the backup to remove.
        name: String,
        /// Accepted for symmetry with `pond remote remove --purge`;
        /// has no effect on backups (no mount entry to purge).
        #[arg(long)]
        purge: bool,
    },
    /// Delete abandoned immutable-payload upload staging objects.
    CleanupUploads {
        /// Logical name of the backup.
        name: String,
        /// Delete staging objects at least this old.
        #[arg(long, default_value_t = 86_400)]
        older_than_seconds: i64,
    },
    /// Publish verified local whole-range series packs to this backup.
    ///
    /// This is the only operation that installs remote consolidated-pack
    /// locators. It validates the backup's pond identity and exact current
    /// `main` publication, uploads immutable physical objects, then each pack,
    /// then its locator. It never runs as part of collection or ordinary push.
    PublishConsolidated {
        /// Logical name of a push/both backup.
        name: String,
    },
    /// List backup-mode attachments only.
    List,
}

/// Portable recovery-capsule operations.
#[derive(Debug, Subcommand)]
pub enum CapsuleCommand {
    /// Build and publish the current portable `pondcapsule.4` snapshot.
    Publish {
        /// Logical backup name.
        name: String,
    },
    /// Publish or inspect a static native-format recovery recipe.
    Recipe {
        #[command(subcommand)]
        command: CapsuleRecipeCommand,
    },
    /// Verify a downloaded pondcapsule.4 without opening a pond.
    Inspect {
        /// Directory containing the downloaded `recovery/` tree.
        path: PathBuf,
    },
    /// Verify a pondcapsule.4 and all leaf identities.
    Verify {
        /// Directory containing the downloaded `recovery/` tree.
        path: PathBuf,
    },
    /// Import a pondcapsule.4 into a brand-new pond.
    ///
    /// The target must not already exist. A private sibling staging
    /// directory is created next to it, given a fresh pond identity, and
    /// populated in bounded, journaled transactions with post-commit factory
    /// execution and remote auto-push suppressed. Retrying the same command
    /// resumes the matching staging directory at an exact entry/leaf cursor.
    /// Only after the staged result is re-verified against the capsule's
    /// logical contract is it renamed atomically onto the target. On any
    /// failure the staging directory is left in place for inspection rather
    /// than silently removed. The promoted pond remains inert until
    /// `pond capsule activate` passes its safety preflight.
    Import {
        /// Directory containing the downloaded `recovery/` tree.
        path: PathBuf,
        /// Birthplace of the freshly minted target pond identity (see
        /// `pond init --birthplace`).
        #[arg(long)]
        birthplace: String,
    },
    /// Safely enable post-commit dispatch on a completed capsule import.
    ///
    /// Reads and validates every restored `/sys/remotes/*` attachment and
    /// `/system/run/*` dynamic config without executing factories. Dispatch
    /// remains suppressed if any entry is invalid or unsafe.
    Activate,
}

#[derive(Debug, Subcommand)]
pub enum CapsuleRecipeCommand {
    /// Install the watertown.commit.v1 -> pondcapsule.4 recipe.
    Publish {
        /// Backup attachment name.
        name: String,
    },
    /// Verify the watertown.commit.v1 -> pondcapsule.4 recipe.
    Inspect {
        /// Remote attachment name.
        name: String,
    },
}

/// Shared options for `pond remote add` and `pond backup add`.
#[derive(Debug, Args)]
pub struct RemoteAddOptions {
    /// AWS region (S3 only).
    #[arg(long)]
    pub region: Option<String>,
    /// S3 access key id.
    #[arg(long = "access-key-id", alias = "access-key")]
    pub access_key_id: Option<String>,
    /// S3 secret access key.
    #[arg(long = "secret-access-key", alias = "secret-key")]
    pub secret_access_key: Option<String>,
    /// Custom S3 endpoint (e.g., for MinIO or R2).
    #[arg(long)]
    pub endpoint: Option<String>,
    /// Allow plain HTTP (required for local MinIO).
    #[arg(long)]
    pub allow_http: bool,
    /// Replace an existing attachment of the same name.
    #[arg(long)]
    pub overwrite: bool,
}

/// Pond user commands.
#[derive(Subcommand)]
pub enum Commands {
    /// Initialize a new pond
    Init {
        /// Birthplace of the pond: an immutable label for where this pond
        /// is created (for example a hostname, site, or deployment name).
        /// Recorded permanently in the pond's identity metadata.
        #[arg(long)]
        birthplace: String,
    },
    /// Recover from crash by checking and restoring transaction metadata
    Recover,
    /// Run Delta Lake maintenance (checkpoint, vacuum, optional compaction)
    Maintain {
        /// Also compact small parquet files into larger ones
        #[arg(long)]
        compact: bool,
        /// Repack native-v2 series whose live physical fanout exceeds this
        /// threshold into verified whole-range packs under local `data/_packs`.
        /// Pass 0 to disable (the default). This never rewrites or reclaims user
        /// logical-series rows; publish selected packs explicitly with
        /// `pond backup publish-consolidated NAME`.
        #[arg(long, default_value = "0")]
        collapse_versions: usize,
        /// Also prune replicated control-table lifecycle history at or below a
        /// safe horizon, reclaimed by this same checkpoint + vacuum pass.
        #[arg(long)]
        prune: bool,
        /// With --prune: number of most-recent transactions to always retain.
        #[arg(long, default_value = "1000")]
        keep_txns: i64,
        /// With --prune: permit pruning when no push-mode remote is attached
        /// (retention-only; pruned history is then unrecoverable).
        #[arg(long)]
        allow_no_remote: bool,
        /// Report which local series packs would be rebuilt, without changing
        /// anything.
        #[arg(long)]
        dry_run: bool,
    },
    /// Show pond contents
    Show {
        /// Display mode: brief (summary stats), concise (1 line per tx), or detailed (full dump)
        #[arg(long, short = 'm', default_value = "brief")]
        mode: String,
    },
    /// View transaction history and audit trail
    Log {
        /// Number of recent transactions to show
        #[arg(long, default_value = "10")]
        limit: usize,
        /// Show detailed lifecycle for a specific transaction sequence number
        #[arg(long, conflicts_with = "incomplete")]
        txn_seq: Option<i64>,
        /// Show incomplete operations (for recovery)
        #[arg(long, conflicts_with = "txn_seq")]
        incomplete: bool,
    },
    /// Push pending local transactions to one or more remotes (D4).
    Push {
        /// Remote name (from `pond remote add`).  Omit to push every remote
        /// in `push` or `both` mode.
        name: Option<String>,
    },
    /// Pull new bundles from one or more remotes (D4).
    Pull {
        /// Remote name.  Omit to pull every remote in `pull` or `both` mode.
        name: Option<String>,
        /// Atomically replace this remote's graft partition, mount, and pin.
        #[arg(long, requires = "name")]
        rebuild_graft: bool,
    },
    /// Restore a whole pond from a backup published to a remote.
    ///
    /// Bootstraps a FRESH replica: discovers the source pond's id from the
    /// remote, stamps a local pond carrying that id, attaches the remote as a
    /// mirror at `/`, and pulls the full content graph.  Refuses to run over an
    /// existing pond.  Use `pond pull <name>` afterward to track the upstream.
    Restore {
        /// Logical name to attach the restored mirror under (e.g., "origin").
        name: String,
        /// Remote URL (`file:///path` or `s3://bucket/prefix`) of the backup.
        url: String,
        #[command(flatten)]
        options: RemoteAddOptions,
    },
    /// Verify local data matches one or more remotes' recorded checksums (D6).
    Verify {
        /// Remote or backup name.  Omit to verify against every attachment.
        name: Option<String>,
        /// Exit nonzero unless every remote tip exactly equals the local tip.
        #[arg(long)]
        exact: bool,
    },
    /// Show an operator-facing status aggregate: identity, local commit
    /// state, recovery health, and per-remote sync watermarks (D6).
    Status,
    /// Show bounded per-limiter state without scanning usage history.
    Limits {
        /// Output format for operators or collectors.
        #[arg(long, default_value = "table", value_parser = ["table", "json", "jsonl"])]
        format: String,
    },
    /// Inspect and verify the pond's transparency log (D5).
    Tlog {
        #[command(subcommand)]
        command: TlogCommand,
    },
    /// Filesystem-check: verify content checksums and Merkle trees, and
    /// print a single root checksum that exhaustively fingerprints every
    /// row in the pond (across all pond_ids).  Two replicas are identical
    /// iff their root checksums match.
    Fsck {
        /// Skip the content-rehash pass; compute only the structural
        /// root checksum (fast).
        #[arg(long)]
        quick: bool,
        /// Print a per-partition breakdown and content statistics in
        /// addition to the root checksum.
        #[arg(long)]
        verbose: bool,
    },
    /// Rebuild the control table from the data Delta table when the
    /// control table is lost or corrupt (D6).  Recovers pond identity
    /// and the transaction-log skeleton; settings/watermarks must be
    /// re-established by re-attaching remotes.
    RebuildControl {
        /// Move an existing control table aside (to control.bak.<ts>)
        /// and rebuild.  Required when a control table already exists.
        #[arg(long)]
        force: bool,
    },
    /// Manage remote attachments under `/sys/remotes/` (D4).
    Remote {
        #[command(subcommand)]
        command: RemoteCommand,
    },
    /// Manage backup attachments (push-side of remote split, D5.7b).
    Backup {
        #[command(subcommand)]
        command: BackupCommand,
    },
    /// Install recovery recipes or verify portable recovery capsules.
    Capsule {
        #[command(subcommand)]
        command: CapsuleCommand,
    },
    /// Manage the persistent pond-wide data-write freeze.
    Freeze {
        #[command(subcommand)]
        command: FreezeCommand,
    },
    /// Show or set pond configuration
    Config {
        #[command(subcommand)]
        command: Option<ConfigCommand>,
    },
    /// (Hidden) Legacy control table interface -- use `pond log`, `pond sync`, `pond config`
    #[command(hide = true)]
    Control {
        #[command(subcommand)]
        command: ControlCommand,
    },
    /// List files and directories (ls -l style)
    List {
        /// Pattern to match (supports wildcards, defaults to "**/*")
        #[arg(default_value = "**/*")]
        pattern: String,
        /// Show all files including hidden ones
        #[arg(short, long)]
        all: bool,
        /// Show full entry type names (e.g., table:series, table:dynamic)
        #[arg(short, long)]
        long: bool,
    },
    /// Describe file schemas and types
    Describe {
        /// Pattern to match (supports wildcards, defaults to "**/*")
        #[arg(default_value = "**/*")]
        pattern: String,
    },
    /// Read a file from the pond
    Cat {
        /// File path to read
        path: String,
        /// Output format [default: auto] [possible values: auto, raw, table, csv]
        /// auto: table for queryable types, raw for non-queryable files
        #[arg(long, default_value = "auto")]
        format: String,
        /// Time range start (Unix timestamp in milliseconds, optional)
        #[arg(long)]
        time_start: Option<i64>,
        /// Time range end (Unix timestamp in milliseconds, optional)
        #[arg(long)]
        time_end: Option<i64>,
        /// SQL query to execute on the file:series data
        #[arg(long = "sql", visible_alias = "query")]
        query: Option<String>,
        /// Show pattern resolution and schema without executing the query
        #[arg(long)]
        explain: bool,
    },
    /// Copy files into or out of the pond
    ///
    /// Copy IN (host -> pond): pond copy host:///file.csv /dest/path
    ///   Entry type is encoded in the source URL:
    ///     host:///file.csv              -> raw data (default)
    ///     host+table:///file.parquet    -> queryable parquet table
    ///     host+series:///file.parquet   -> time-series parquet
    /// Copy OUT (pond -> host): pond copy '/pattern/**/*.series' host:///output/dir
    Copy {
        /// Source paths: host URLs (copy IN) or pond paths/patterns (copy OUT)
        #[arg(required = true)]
        sources: Vec<String>,
        /// Destination: pond path (copy IN) or host:///path (copy OUT)
        dest: String,
        /// Strip a leading path prefix from pond paths when copying OUT.
        /// e.g. --strip-prefix=/hydrovu copies /hydrovu/devices/123/foo.series
        /// to <dest>/devices/123/foo.series instead of <dest>/hydrovu/devices/123/foo.series
        #[arg(long)]
        strip_prefix: Option<String>,
    },
    /// Create a directory in the pond
    Mkdir {
        /// Directory path to create
        path: String,
        /// Create parent directories as needed (like mkdir -p)
        #[arg(short = 'p', long = "parents")]
        parents: bool,
    },
    /// Create node (factory objects like CSV, SQL views, etc.)
    Mknod {
        /// Factory type to create (use 'list-factories' to see all available types)
        factory_type: String,
        /// Path where the node will be created
        path: String,
        /// Path to configuration file for the factory
        #[arg(long)]
        config_path: String,
        /// Overwrite existing dynamic node with new configuration
        #[arg(long)]
        overwrite: bool,
    },
    /// Apply configuration files (idempotent create-or-update)
    ///
    /// Each file contains a prelude (kind, path, version) separated from
    /// the config body by '---'. Creates new nodes or updates changed ones.
    Apply {
        /// Configuration files to apply
        #[arg(short = 'f', required = true, num_args = 1..)]
        files: Vec<String>,
    },
    /// List available dynamic node factories
    ListFactories,
    /// Execute a run configuration (e.g., hydrovu collector)
    Run {
        /// Path to the configuration file to execute
        path: String,
        /// Additional arguments to pass to the factory (e.g., subcommands like 'replicate', 'list-bundles')
        #[arg(num_args = 0..)]
        args: Vec<String>,
    },
    /// Export pond data to external Parquet files with time partitioning
    Export {
        /// File patterns to export (e.g., "/sensors/*.series")
        #[arg(short, long)]
        pattern: Vec<String>,
        /// Output directory for exported files
        #[arg(long)]
        dir: PathBuf,
        /// Temporal partitioning levels (comma-separated: year,month,day,hour,minute)
        #[arg(long, default_value = "")]
        temporal: String,
        /// Time range start (human-readable, e.g., "2024-01-01 00:00:00", "2024-01-01T00:00:00Z")
        #[arg(long)]
        start_time: Option<String>,
        /// Time range end (human-readable, e.g., "2024-12-31 23:59:59", "2024-12-31T23:59:59Z")
        #[arg(long)]
        end_time: Option<String>,
        /// Drop the temporal-reduce partial-aggregate cache before exporting, forcing a
        /// full recompute. A maintenance escape hatch: the cache is derived
        /// state, so discarding it can only cost time.
        #[arg(long)]
        rebuild: bool,
    },
    /// Emergency operations (destructive, use with care)
    #[command(subcommand)]
    Emergency(EmergencyCommand),
}

/// Transparency-log subcommands (D5).
#[derive(Debug, Subcommand)]
pub enum TlogCommand {
    /// Show the current checkpoint, checkpoint history, and tree size.
    Show,
    /// Verify inclusion of every leaf, append-only consistency across every
    /// published checkpoint, and faithfulness to the control-table commit spine.
    Verify,
}

/// Emergency subcommands for destructive operations.
#[derive(Debug, Subcommand)]
pub enum EmergencyCommand {
    /// Erase all objects in an S3 bucket. Requires --dangerous flag.
    EraseBucket {
        /// S3 bucket URL (e.g., "s3://water-staging")
        url: String,
        /// S3 endpoint (e.g., "http://localhost:9000" for MinIO)
        #[arg(long)]
        endpoint: String,
        /// S3 region
        #[arg(long, default_value = "us-east-1")]
        region: String,
        /// S3 access key
        #[arg(long)]
        access_key: String,
        /// S3 secret key
        #[arg(long)]
        secret_key: String,
        /// Allow HTTP (non-TLS) connections
        #[arg(long)]
        allow_http: bool,
        /// Required safety flag to confirm destructive operation
        #[arg(long)]
        dangerous: bool,
    },
}
