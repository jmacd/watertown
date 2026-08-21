// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use common::ShipContext;
use panic_alloc::PanicOnLargeAlloc;
use std::path::PathBuf;
use std::time::Instant;

mod commands;
mod common;
mod panic_alloc;

// External modules
use billing as _;
use gitpond as _;
use hydrovu as _;
use sitegen as _;

#[global_allocator]
static PEAK_ALLOC: PanicOnLargeAlloc = PanicOnLargeAlloc::new(3000);

/// Control table subcommands (hidden -- use `pond log`, `pond sync`, `pond config` instead)
#[derive(Debug, Subcommand)]
enum ControlCommand {
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
enum ConfigCommand {
    /// Set a configuration value
    Set {
        /// Configuration key
        key: String,
        /// Configuration value
        value: String,
    },
}

#[derive(Parser)]
#[command(author, version = env!("WATERTOWN_VERSION"), about = "Watertown - A very small data lake")]
#[command(name = "pond")]
struct Cli {
    /// Pond path override (defaults to POND env var)
    #[arg(long, global = true)]
    pond: Option<PathBuf>,

    /// Host directory root for host+ URL operations (defaults to none;
    /// when set, host+ paths are resolved relative to this directory)
    #[arg(short = 'd', long = "directory", global = true)]
    directory: Option<PathBuf>,

    /// Mount factory definitions onto host filesystem paths.
    /// Format: <mount_path>=host+<factory>:///<config_path>
    /// Example: --hostmount /reduced=host+dyndir:///reduce.yaml
    #[arg(long = "hostmount", global = true)]
    hostmount: Vec<String>,

    #[command(subcommand)]
    command: Commands,
}

/// Remote management subcommands (D4: replaces `/system/run/<N>-remote`
/// factory configs with `/sys/remotes/<name>` YAML attachments).
///
/// As of D5.7b the verb is split: `pond remote add` always attaches a
/// **pull-only** remote (mirror restart or cross-pond import).  Use
/// `pond backup add` to attach a push-mode (or bidirectional) remote.
#[derive(Debug, Subcommand)]
enum RemoteCommand {
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
enum BackupCommand {
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
    /// List backup-mode attachments only.
    List,
}

/// Portable recovery-capsule operations.
#[derive(Debug, Subcommand)]
enum CapsuleCommand {
    /// Publish or inspect the static native-format recovery recipe.
    Recipe {
        #[command(subcommand)]
        command: CapsuleRecipeCommand,
    },
    /// Verify and summarize a downloaded capsule without opening a pond.
    Inspect {
        /// Directory containing the downloaded `recovery/` tree.
        path: PathBuf,
    },
    /// Verify a downloaded capsule and all logical leaf identities.
    Verify {
        /// Directory containing the downloaded `recovery/` tree.
        path: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum CapsuleRecipeCommand {
    /// Install the reviewed dp.commit.3 recipe without overwriting differences.
    Publish {
        /// Backup attachment name.
        name: String,
    },
    /// Verify both immutable and discoverable recipe objects.
    Inspect {
        /// Remote attachment name.
        name: String,
    },
}

/// Shared options for `pond remote add` and `pond backup add`.
#[derive(Debug, Args)]
struct RemoteAddOptions {
    /// AWS region (S3 only).
    #[arg(long)]
    region: Option<String>,
    /// S3 access key id.
    #[arg(long = "access-key-id", alias = "access-key")]
    access_key_id: Option<String>,
    /// S3 secret access key.
    #[arg(long = "secret-access-key", alias = "secret-key")]
    secret_access_key: Option<String>,
    /// Custom S3 endpoint (e.g., for MinIO or R2).
    #[arg(long)]
    endpoint: Option<String>,
    /// Allow plain HTTP (required for local MinIO).
    #[arg(long)]
    allow_http: bool,
    /// Replace an existing attachment of the same name.
    #[arg(long)]
    overwrite: bool,
}

/// Pond user commands.
#[derive(Subcommand)]
enum Commands {
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
        /// Also collapse multi-version `data:series` files whose live version
        /// count exceeds this threshold into a single merged version. Pass 0 to
        /// disable (the default).
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
        /// Report what maintenance would do -- which series collapse would
        /// merge and how many bytes that would push -- without changing
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
        /// Output format [default: auto] [possible values: auto, raw, table]
        /// auto: table for queryable types when stdout is a TTY, raw otherwise
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
enum TlogCommand {
    /// Show the current checkpoint, checkpoint history, and tree size.
    Show,
    /// Verify inclusion of every leaf, append-only consistency across every
    /// published checkpoint, and faithfulness to the control-table commit spine.
    Verify,
}

/// Emergency subcommands for destructive operations.
#[derive(Debug, Subcommand)]
enum EmergencyCommand {
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

#[allow(clippy::print_stdout)]
fn print_handler(output: &str) {
    print!("{}", output);
}

#[tokio::main]
async fn main() -> Result<()> {
    // Capture original command line arguments before clap parsing for transaction metadata
    let original_args: Vec<String> = std::env::args().collect();

    // Initialize env_logger from RUST_LOG; default to `info` when RUST_LOG is not set
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::debug!("Main function started");
    let cli = Cli::parse();
    log::debug!("CLI parsed successfully");

    // Parse hostmount specs
    let mount_specs: Vec<tinyfs::hostmount::MountSpec> = cli
        .hostmount
        .iter()
        .map(|s| tinyfs::hostmount::MountSpec::parse(s))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("Invalid --hostmount: {}", e))?;

    // Create the ship context
    let ship_context = ShipContext::new(
        cli.pond.as_ref(),
        cli.directory.as_ref(),
        mount_specs,
        original_args.clone(),
    );

    // Capture the path/args of the `Run` subcommand (if any) so we can
    // emit a structured "Run summary" log line at process exit naming
    // the factory and elapsed time.  See commands::run_summary.
    let run_meta: Option<(String, Vec<String>)> = if let Commands::Run { path, args } = &cli.command
    {
        Some((path.clone(), args.clone()))
    } else {
        None
    };
    let started = Instant::now();

    let result = match cli.command {
        Commands::Init { birthplace } => {
            // Init command creates a new empty pond.
            commands::init_command(&ship_context, &birthplace).await
        }
        Commands::Recover => {
            // Recover command works with potentially damaged pond, handle specially
            commands::recover_command(&ship_context).await
        }
        Commands::Maintain {
            compact,
            collapse_versions,
            prune,
            keep_txns,
            allow_no_remote,
            dry_run,
        } => {
            commands::maintain_command(
                &ship_context,
                compact,
                collapse_versions,
                prune,
                keep_txns,
                allow_no_remote,
                dry_run,
            )
            .await
        }

        // Read-only commands that use ShipContext for consistency
        Commands::Show { mode } => {
            commands::show_command(&ship_context, &mode, print_handler).await
        }
        Commands::Log {
            limit,
            txn_seq,
            incomplete,
        } => {
            let control_mode = if let Some(seq) = txn_seq {
                commands::control::ControlMode::Detail { txn_seq: seq }
            } else if incomplete {
                commands::control::ControlMode::Incomplete
            } else {
                commands::control::ControlMode::Recent { limit }
            };
            commands::control_command(&ship_context, control_mode).await
        }
        Commands::Push { name } => commands::push_command(&ship_context, name).await,
        Commands::Pull {
            name,
            rebuild_graft,
        } => {
            if rebuild_graft {
                commands::pull_command_with_rebuild(&ship_context, name, true).await
            } else {
                commands::pull_command(&ship_context, name).await
            }
        }
        Commands::Restore { name, url, options } => {
            commands::restore_command(
                &ship_context,
                &name,
                &url,
                options.region,
                options.access_key_id,
                options.secret_access_key,
                options.endpoint,
                options.allow_http,
            )
            .await
        }
        Commands::Verify { name } => commands::verify_command(&ship_context, name).await,
        Commands::Status => commands::status_command(&ship_context).await,
        Commands::Limits { format } => commands::limits_command(&ship_context, &format).await,
        Commands::Tlog { command } => match command {
            TlogCommand::Show => commands::tlog_show_command(&ship_context).await,
            TlogCommand::Verify => commands::tlog_verify_command(&ship_context).await,
        },
        Commands::Fsck { quick, verbose } => {
            commands::fsck_command(&ship_context, quick, verbose).await
        }
        Commands::RebuildControl { force } => {
            commands::rebuild_control_command(&ship_context, force).await
        }
        Commands::Remote { command } => match command {
            RemoteCommand::Add {
                name,
                url,
                path,
                options,
            } => {
                commands::add_remote_command(
                    &ship_context,
                    &name,
                    &url,
                    &path,
                    options.region,
                    options.access_key_id,
                    options.secret_access_key,
                    options.endpoint,
                    options.allow_http,
                    options.overwrite,
                )
                .await
            }
            RemoteCommand::Remove { name, purge } => {
                commands::remove_remote_command(&ship_context, &name, purge).await
            }
            RemoteCommand::List => commands::list_remotes_command(&ship_context, None).await,
        },
        Commands::Backup { command } => match command {
            BackupCommand::Add {
                name,
                url,
                bidirectional,
                options,
            } => {
                commands::add_backup_command(
                    &ship_context,
                    &name,
                    &url,
                    bidirectional,
                    options.region,
                    options.access_key_id,
                    options.secret_access_key,
                    options.endpoint,
                    options.allow_http,
                    options.overwrite,
                )
                .await
            }
            BackupCommand::Remove { name, purge } => {
                commands::remove_remote_command(&ship_context, &name, purge).await
            }
            BackupCommand::List => {
                commands::list_remotes_command(
                    &ship_context,
                    Some(commands::RemoteListFilter::BackupsOnly),
                )
                .await
            }
        },
        Commands::Capsule { command } => match command {
            CapsuleCommand::Recipe { command } => {
                let (name, action) = match command {
                    CapsuleRecipeCommand::Publish { name } => {
                        (name, commands::RecoveryRecipeAction::Publish)
                    }
                    CapsuleRecipeCommand::Inspect { name } => {
                        (name, commands::RecoveryRecipeAction::Inspect)
                    }
                };
                commands::capsule_recipe_command(&ship_context, &name, action).await
            }
            CapsuleCommand::Inspect { path } | CapsuleCommand::Verify { path } => {
                commands::capsule_inspect_command(&path)
            }
        },
        Commands::Config { command } => {
            let control_mode = match command {
                Some(ConfigCommand::Set { key, value }) => {
                    commands::control::ControlMode::SetConfig { key, value }
                }
                None => commands::control::ControlMode::ShowConfig,
            };
            commands::control_command(&ship_context, control_mode).await
        }
        Commands::Control { command } => {
            let control_mode = match command {
                ControlCommand::Recent { limit } => {
                    commands::control::ControlMode::Recent { limit }
                }
                ControlCommand::Detail { txn_seq } => {
                    commands::control::ControlMode::Detail { txn_seq }
                }
                ControlCommand::Incomplete => commands::control::ControlMode::Incomplete,
                ControlCommand::ShowConfig => commands::control::ControlMode::ShowConfig,
                ControlCommand::SetConfig { key, value } => {
                    commands::control::ControlMode::SetConfig { key, value }
                }
                ControlCommand::Prune {
                    keep_txns,
                    dry_run,
                    allow_no_remote,
                } => commands::control::ControlMode::Prune {
                    keep_txns,
                    dry_run,
                    allow_no_remote,
                },
            };
            commands::control_command(&ship_context, control_mode).await
        }
        Commands::List { pattern, all, long } => {
            commands::list_command(&ship_context, &pattern, all, long, print_handler).await
        }
        Commands::Describe { pattern } => {
            commands::describe_command(&ship_context, &pattern, print_handler).await
        }
        Commands::Cat {
            path,
            format,
            time_start,
            time_end,
            query,
            explain,
        } => {
            commands::cat_command(
                &ship_context,
                &path,
                &format,
                None,
                time_start,
                time_end,
                query.as_deref(),
                explain,
            )
            .await
        }

        // Write commands that use scoped transactions
        Commands::Copy {
            sources,
            dest,
            strip_prefix,
        } => {
            let options = commands::CopyOptions { strip_prefix };
            commands::copy_command(&ship_context, &sources, &dest, &options).await
        }
        Commands::Mkdir { path, parents } => {
            commands::mkdir_command(&ship_context, &path, parents).await
        }
        Commands::Mknod {
            factory_type,
            path,
            config_path,
            overwrite,
        } => {
            commands::mknod_command(&ship_context, &factory_type, &path, &config_path, overwrite)
                .await
        }
        Commands::ListFactories => commands::list_factories_command().await,
        Commands::Apply { files } => commands::apply_command(&ship_context, &files).await,
        Commands::Run { path, args } => commands::run_command(&ship_context, &path, args).await,
        Commands::Export {
            pattern,
            dir,
            temporal,
            start_time,
            end_time,
            rebuild,
        } => {
            commands::export_command(
                &ship_context,
                &pattern,
                &dir.to_string_lossy(),
                &temporal,
                start_time,
                end_time,
                rebuild,
            )
            .await
        }
        Commands::Emergency(cmd) => match cmd {
            EmergencyCommand::EraseBucket {
                url,
                endpoint,
                region,
                access_key,
                secret_key,
                allow_http,
                dangerous,
            } => {
                commands::emergency::erase_bucket(
                    &url,
                    &endpoint,
                    &region,
                    &access_key,
                    &secret_key,
                    allow_http,
                    dangerous,
                )
                .await
            }
        },
    };

    // Log peak memory usage
    let peak_mem = PEAK_ALLOC.peak_usage_as_mb();
    log::info!("Peak memory usage: {:.2} MB", peak_mem);

    // For `pond run`, also emit a structured one-line summary that
    // names the resolved factory.  This is what selfmon/observability
    // grep for when correlating ticks with their factories: the bare
    // CLI args ("/system/etc/measure/septic-prod push") don't tell
    // you which factory ran.
    if let Some((path, args)) = run_meta {
        let factory = commands::run_summary::take_factory().unwrap_or_else(|| "-".to_string());
        let elapsed = started.elapsed();
        let outcome = if result.is_ok() { "ok" } else { "err" };
        log::info!(
            "Run summary  path={}  factory={}  args={:?}  elapsed_s={:.3}  peak_mem_mb={:.2}  outcome={}",
            path,
            factory,
            args,
            elapsed.as_secs_f64(),
            peak_mem,
            outcome,
        );
    }

    // Print large allocations report
    PEAK_ALLOC.print_large_allocs();

    result
}
