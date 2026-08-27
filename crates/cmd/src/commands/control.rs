// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Control table query command - Show transaction status and post-commit execution
//!
//! Displays information from the control table including:
//! - Transaction lifecycle (begin, data_committed, completed, failed)
//! - Post-commit factory execution (pending, started, completed, failed)
//! - Error messages and duration metrics
#![allow(clippy::print_stdout)]

use crate::common::ShipContext;
use anyhow::{Result, anyhow};
use serde::Deserialize;

/// Recent transaction record from control table query.  Post-D2 the
/// control table no longer carries `cli_args` / `error_message` /
/// `transaction_type` columns; tx kind is inferred from the lifecycle
/// records present and error text lives in `metadata_json`.
#[derive(Debug, Deserialize)]
struct RecentTransaction {
    txn_seq: i64,
    txn_id: String,
    tx_kind: String,
    started_at: Option<i64>,
    final_state: Option<String>,
    error_metadata: Option<String>,
    duration_ms: Option<i64>,
}

/// Transaction detail record from control table query.  Post-D2 schema:
/// post-commit record attributes (`parent_seq`, `execution_seq`,
/// `factory_name`, `config_path`, `error_message`) live in
/// `metadata_json`.
#[derive(Debug, Deserialize)]
struct TransactionDetail {
    txn_seq: i64,
    txn_id: String,
    record_kind: String,
    ts_micros: i64,
    has_parent_seq: bool,
    #[allow(dead_code)]
    parent_seq: i64,
    has_commit_kind: bool,
    commit_kind: String,
    has_duration_ms: bool,
    duration_ms: i64,
    metadata_json: String,
}

/// Decoded post-commit attributes packed by `record_post_commit_*` into
/// `metadata_json` (see `steward::control_table::PostCommitMetadata`).
#[derive(Debug, Default, Deserialize)]
struct PostCommitMetadata {
    #[serde(default)]
    execution_seq: Option<i64>,
    #[serde(default)]
    factory_name: Option<String>,
    #[serde(default)]
    config_path: Option<String>,
    #[serde(default)]
    error_message: Option<String>,
}

/// Control command modes
#[derive(Debug, Clone)]
pub enum ControlMode {
    /// Show recent transactions with summary status
    Recent { limit: usize },
    /// Show detailed lifecycle for specific transaction
    Detail { txn_seq: i64 },
    /// Show incomplete operations (for recovery)
    Incomplete,
    /// Show pond configuration (ID, factory modes, metadata, settings)
    ShowConfig,
    /// Set a configuration value (key=value)
    SetConfig { key: String, value: String },
    /// Shrink the control table by deleting replicated lifecycle history
    /// at or below a safe horizon, then checkpoint + vacuum to reclaim
    /// space.
    Prune {
        keep_txns: i64,
        dry_run: bool,
        allow_no_remote: bool,
    },
}

/// Show control table information
pub async fn control_command(ship_context: &ShipContext, mode: ControlMode) -> Result<()> {
    // Use the Ship's control table (already open and updated with all commits)
    let mut ship = ship_context.open_pond().await?;

    match mode {
        ControlMode::Recent { limit } => {
            crate::common::with_read_transaction(&mut ship, vec!["log".to_string()], async |tx| {
                show_recent_transactions(tx, limit).await
            })
            .await?;
        }
        ControlMode::Detail { txn_seq } => {
            crate::common::with_read_transaction(&mut ship, vec!["log".to_string()], async |tx| {
                show_transaction_detail(tx, txn_seq).await
            })
            .await?;
        }
        ControlMode::Incomplete => {
            show_incomplete_operations(ship.control_table_mut()).await?;
        }
        ControlMode::ShowConfig => {
            show_pond_config(ship.control_table_mut()).await?;
        }
        ControlMode::SetConfig { key, value } => {
            set_pond_config(ship.control_table_mut(), &key, &value).await?;
        }
        ControlMode::Prune {
            keep_txns,
            dry_run,
            allow_no_remote,
        } => {
            prune_control_table(&mut ship, keep_txns, dry_run, allow_no_remote).await?;
        }
    }

    Ok(())
}

/// Per-transaction operation summary, aggregated from the data Delta
/// table.  Reads (which never write a data commit) have no entry.
#[derive(Debug, Deserialize)]
struct OpSummary {
    txn_seq: i64,
    ops: i64,
    files: i64,
    dirs: i64,
    partitions: i64,
}

/// Render the enriched transaction log (the `pond log` default view).
///
/// Combines three sources so the operator can see, at a glance, *what*
/// each transaction did and whether it succeeded:
///   * control table -- lifecycle (status, timing, duration, errors);
///   * data Delta commit metadata (`pond_txn`) -- the original CLI
///     command that produced the transaction;
///   * data Delta table -- a count of the objects the transaction wrote.
///
/// Output is newest-first, matching `pond show` and `git log`.
/// Build a map from `txn_seq` to the original CLI command (argv) by
/// reading the `pond_txn` metadata embedded in each data Delta commit.
/// Compaction/optimize commits carry no `pond_txn` and are skipped.
async fn build_command_map(
    pond: &steward::StewardTransactionGuard<'_>,
) -> Result<std::collections::HashMap<i64, Vec<String>>> {
    let mut command_map: std::collections::HashMap<i64, Vec<String>> =
        std::collections::HashMap::new();
    let commits = pond
        .get_commit_history(None)
        .await
        .map_err(|e| anyhow!("Failed to read commit history: {}", e))?;
    for commit in &commits {
        if let Some(meta) = tlogfs::PondTxnMetadata::from_delta_metadata(&commit.info) {
            _ = command_map.insert(meta.txn_seq, meta.user.args);
        }
    }
    Ok(command_map)
}

/// Render a stored command argv as a single canonical `pond ...` line.
fn render_command(args: &[String]) -> String {
    format!("pond {}", normalize_command_args(args))
}

async fn show_recent_transactions(tx: &mut steward::Transaction<'_>, limit: usize) -> Result<()> {
    use std::collections::HashMap;

    // --- Phase 1: control-table lifecycle + commit-history commands ---
    // These only need shared access to the pond guard; collect everything
    // into owned values so the borrow ends before we reach for the data
    // session context (which needs a mutable borrow of `tx`).
    let (transactions, command_map) = {
        let pond = tx
            .as_pond()
            .ok_or_else(|| anyhow!("`pond log` requires a pond transaction"))?;

        pond.control_table().print_banner();

        let ctx = pond.control_table().session_context();

        // Post-D2 lean schema lives at table "control".  A transaction's
        // kind is inferred from its lifecycle records: a tx with a
        // `data_committed` record is a "write"; anything else is a "read".
        // `recent_seqs` keeps only transactions that actually attempted a
        // write (they have a `data_committed` or `failed` record), so pure
        // reads -- including this `pond log` invocation's own in-flight read
        // -- never appear.  Crashed/incomplete writes are surfaced
        // separately by `pond log --incomplete`.
        let sql = format!(
            r#"
            WITH user_txns AS (
                SELECT *
                FROM control
                WHERE record_kind <> 'setting'
                  AND NOT (has_parent_seq = true AND parent_seq <> txn_seq)
            ),
            recent_seqs AS (
                SELECT txn_seq
                FROM user_txns
                WHERE txn_seq > 0
                GROUP BY txn_seq
                HAVING MAX(CASE WHEN record_kind IN ('data_committed', 'failed') THEN 1 ELSE 0 END) = 1
                ORDER BY txn_seq DESC
                LIMIT {limit}
            )
            SELECT
                t.txn_seq,
                t.txn_id,
                CASE
                    WHEN MAX(CASE WHEN t.record_kind = 'data_committed' THEN 1 ELSE 0 END) = 1
                        THEN 'write'
                    ELSE 'read'
                END AS tx_kind,
                MAX(CASE WHEN t.record_kind = 'begin' THEN t.ts_micros END) AS started_at,
                MAX(CASE
                    WHEN t.record_kind IN ('data_committed', 'completed', 'failed')
                        THEN t.record_kind
                END) AS final_state,
                MAX(CASE WHEN t.record_kind = 'failed' THEN t.metadata_json END) AS error_metadata,
                MAX(CASE WHEN t.has_duration_ms = true THEN t.duration_ms END) AS duration_ms
            FROM user_txns t
            WHERE t.txn_seq IN (SELECT txn_seq FROM recent_seqs)
            GROUP BY t.txn_seq, t.txn_id
            ORDER BY t.txn_seq DESC, started_at DESC
            "#,
            limit = limit
        );

        let batches = ctx
            .sql(&sql)
            .await
            .map_err(|e| anyhow!("Failed to query recent transactions: {}", e))?
            .collect()
            .await
            .map_err(|e| anyhow!("Failed to collect query results: {}", e))?;

        let mut transactions = Vec::new();
        for batch in &batches {
            let batch_txns: Vec<RecentTransaction> = serde_arrow::from_record_batch(batch)
                .map_err(|e| anyhow!("Failed to deserialize transaction records: {}", e))?;
            transactions.extend(batch_txns);
        }

        // Map txn_seq -> original CLI command from data Delta commit
        // metadata (`pond_txn`).
        let command_map = build_command_map(pond).await?;

        (transactions, command_map)
    };

    // --- Phase 2: per-transaction operation summary from the data table ---
    let op_map = if transactions.is_empty() {
        HashMap::new()
    } else {
        let seq_list = transactions
            .iter()
            .map(|t| t.txn_seq.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let session = tx
            .session_context()
            .await
            .map_err(|e| anyhow!("Failed to get session context: {}", e))?;
        let sql = format!(
            r#"
            SELECT
                txn_seq,
                CAST(COUNT(*) AS BIGINT) AS ops,
                CAST(COUNT(DISTINCT CASE WHEN file_type <> 'directory' THEN node_id END) AS BIGINT) AS files,
                CAST(COUNT(DISTINCT CASE WHEN file_type = 'directory' THEN node_id END) AS BIGINT) AS dirs,
                CAST(COUNT(DISTINCT part_id) AS BIGINT) AS partitions
            FROM delta_table
            WHERE txn_seq IN ({seq_list})
            GROUP BY txn_seq
            "#
        );
        let batches = session
            .sql(&sql)
            .await
            .map_err(|e| anyhow!("Failed to query operation summary: {}", e))?
            .collect()
            .await
            .map_err(|e| anyhow!("Failed to collect operation summary: {}", e))?;
        let mut map: HashMap<i64, OpSummary> = HashMap::new();
        for batch in &batches {
            let rows: Vec<OpSummary> = serde_arrow::from_record_batch(batch)
                .map_err(|e| anyhow!("Failed to deserialize operation summary: {}", e))?;
            for row in rows {
                _ = map.insert(row.txn_seq, row);
            }
        }
        map
    };

    // --- Phase 3: render ---
    println!("\n+===========================================================================+");
    println!("|                          TRANSACTION LOG                                   |");
    println!("+===========================================================================+\n");

    if transactions.is_empty() {
        println!("No transactions found.\n");
        return Ok(());
    }

    for txn in &transactions {
        let status = match txn.final_state.as_deref() {
            Some("data_committed") => "COMMITTED",
            Some("completed") => "COMPLETED",
            Some("failed") => "FAILED",
            Some(_) => "UNKNOWN",
            None => "INCOMPLETE",
        };

        let when = txn
            .started_at
            .map(format_timestamp)
            .unwrap_or_else(|| "<unknown time>".to_string());
        let duration = txn
            .duration_ms
            .map(|d| format!("{}ms", d))
            .unwrap_or_else(|| "n/a".to_string());

        let command = match command_map.get(&txn.txn_seq) {
            Some(args) if !args.is_empty() => render_command(args),
            _ if txn.tx_kind == "read" => "(read-only, no changes)".to_string(),
            _ => "(no command recorded)".to_string(),
        };

        println!(
            "+- Transaction {}  ({})  {} {}",
            txn.txn_seq,
            txn.tx_kind,
            status,
            "-".repeat(40usize.saturating_sub(status.len() + txn.tx_kind.len()))
        );
        println!("|  Command  : {}", command);
        println!("|  When     : {}  ({})", when, duration);
        println!("|  Changes  : {}", format_changes(op_map.get(&txn.txn_seq)));
        println!("|  TxnID    : {}", txn.txn_id);

        if let Some(reason) = txn
            .error_metadata
            .as_deref()
            .and_then(extract_failure_reason)
        {
            println!("|  Error    : {}", truncate_error(&reason));
        }

        println!("+----------------------------------------------------------------------------");
    }
    println!();

    Ok(())
}

/// Render a human-readable change summary from an [`OpSummary`].
fn format_changes(summary: Option<&OpSummary>) -> String {
    let Some(s) = summary else {
        return "(no data changes)".to_string();
    };
    let mut parts = Vec::new();
    if s.files > 0 {
        parts.push(format!("{} file{}", s.files, plural(s.files)));
    }
    if s.dirs > 0 {
        parts.push(format!(
            "{} director{}",
            s.dirs,
            if s.dirs == 1 { "y" } else { "ies" }
        ));
    }
    let what = if parts.is_empty() {
        "no objects".to_string()
    } else {
        parts.join(", ")
    };
    format!(
        "{} operation{} -> {} across {} partition{}",
        s.ops,
        plural(s.ops),
        what,
        s.partitions,
        plural(s.partitions),
    )
}

/// Return "s" for plural counts, "" for singular.
fn plural(n: i64) -> &'static str {
    if n == 1 { "" } else { "s" }
}

/// Normalize stored command args for display.  Args may or may not carry a
/// leading binary token (legacy hand-built metadata used "pond"; the init
/// transaction stores ["pond", "init"]; argv-based metadata strips argv[0]).
/// Drop any leading "pond" token and re-join so the caller can prefix a
/// single canonical "pond ".
fn normalize_command_args(args: &[String]) -> String {
    let rest = match args.first() {
        Some(first) if first == "pond" => &args[1..],
        _ => args,
    };
    rest.join(" ")
}

/// Show detailed lifecycle for a specific transaction
async fn show_transaction_detail(tx: &mut steward::Transaction<'_>, txn_seq: i64) -> Result<()> {
    let pond = tx
        .as_pond()
        .ok_or_else(|| anyhow!("`pond log` requires a pond transaction"))?;

    // Control table automatically sees latest Delta commits via DataFusion
    pond.control_table().print_banner();

    // The original CLI command for this transaction lives in the data
    // Delta commit metadata (`pond_txn`), not the control table.
    let command = build_command_map(pond)
        .await?
        .get(&txn_seq)
        .filter(|args| !args.is_empty())
        .map(|args| render_command(args));

    // Use control table's SessionContext (following tlogfs pattern)
    let ctx = pond.control_table().session_context();

    // Post-D2 lean schema: post-commit records carry the parent
    // transaction's seq in `parent_seq` (with `has_parent_seq=true`).
    // Main-transaction records have `has_parent_seq=false`.  Order the
    // result so main records appear first, then post-commit records
    // grouped by their original time ordering.
    let sql = format!(
        r#"
        SELECT
            txn_seq,
            txn_id,
            record_kind,
            ts_micros,
            has_parent_seq,
            parent_seq,
            has_commit_kind,
            commit_kind,
            has_duration_ms,
            duration_ms,
            metadata_json
        FROM control
        WHERE record_kind <> 'setting'
          AND (
              (txn_seq = {seq} AND has_parent_seq = false)
              OR (has_parent_seq = true AND parent_seq = {seq})
          )
        ORDER BY
            CASE WHEN has_parent_seq = false THEN 0 ELSE 1 END,
            ts_micros
        "#,
        seq = txn_seq
    );

    let df = ctx
        .sql(&sql)
        .await
        .map_err(|e| anyhow!("Failed to query transaction detail: {}", e))?;

    let batches = df
        .collect()
        .await
        .map_err(|e| anyhow!("Failed to collect query results: {}", e))?;

    println!("\n+===========================================================================+");
    println!(
        "|                     TRANSACTION DETAIL: {}                                   |",
        txn_seq
    );
    println!("+===========================================================================+\n");

    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        println!("Transaction {} not found.\n", txn_seq);
        return Ok(());
    }

    // Deserialize batches into structs using serde_arrow
    let mut details = Vec::new();
    for batch in &batches {
        let batch_details: Vec<TransactionDetail> = serde_arrow::from_record_batch(batch)
            .map_err(|e| anyhow!("Failed to deserialize transaction detail records: {}", e))?;
        details.extend(batch_details);
    }

    // Track whether we're showing main transaction or post-commit tasks
    let mut in_post_commit = false;

    for detail in details {
        let current_txn_seq = detail.txn_seq;
        let txn_id = &detail.txn_id;
        let record_kind = detail.record_kind.as_str();
        let timestamp = format_timestamp(detail.ts_micros);
        let is_post_commit = detail.has_parent_seq;

        // Section header for post-commit tasks
        if is_post_commit && !in_post_commit {
            in_post_commit = true;
            println!("\n=== POST-COMMIT TASKS ===============================================\n");
        }

        let duration_str = || -> String {
            if detail.has_duration_ms {
                format!("{}ms", detail.duration_ms)
            } else {
                "N/A".to_string()
            }
        };

        match record_kind {
            "begin" => {
                println!("+- BEGIN Transaction --------------------------------------");
                println!("|  Sequence     : {}", current_txn_seq);
                println!("|  UUID         : {}", txn_id);
                println!("|  Timestamp    : {}", timestamp);
                match &command {
                    Some(cmd) => println!("|  Command      : {}", cmd),
                    None => println!("|  Command      : (no command recorded)"),
                }
                println!("+----------------------------------------------------------------");
            }
            "data_committed" => {
                let version = parse_data_delta_version(&detail.metadata_json);
                let commit_kind = if detail.has_commit_kind {
                    detail.commit_kind.as_str()
                } else {
                    "write"
                };
                println!(
                    "|  [OK] DATA COMMITTED at {} ({}, version {}, duration: {})",
                    timestamp,
                    commit_kind,
                    version,
                    duration_str()
                );
            }
            "completed" => {
                println!(
                    "|  [OK] COMPLETED at {} (duration: {})",
                    timestamp,
                    duration_str()
                );
            }
            "failed" => {
                let error = extract_failure_reason(&detail.metadata_json)
                    .unwrap_or_else(|| "<no error message>".to_string());
                println!(
                    "|  [FAIL] FAILED at {} (duration: {})",
                    timestamp,
                    duration_str()
                );
                println!("|  Error: {}", error);
            }
            "post_push_pending" => {
                let pc = parse_post_commit_metadata(&detail.metadata_json);
                let exec_seq = pc.execution_seq.unwrap_or(0);
                let factory = pc.factory_name.as_deref().unwrap_or("<unknown>");
                let config = pc.config_path.as_deref().unwrap_or("<unknown>");
                println!(
                    "+- POST-COMMIT TASK #{} PENDING ------------------------------",
                    exec_seq
                );
                println!("|  Factory      : {}", factory);
                println!("|  Config       : {}", config);
                println!("|  Timestamp    : {}", timestamp);
            }
            "post_push_started" => {
                println!("|  [RUN] STARTED at {}", timestamp);
            }
            "post_push_completed" => {
                println!(
                    "|  [OK] COMPLETED at {} (duration: {})",
                    timestamp,
                    duration_str()
                );
                println!("+----------------------------------------------------------------");
            }
            "post_push_failed" => {
                let pc = parse_post_commit_metadata(&detail.metadata_json);
                let error = pc.error_message.as_deref().unwrap_or("<no error message>");
                println!(
                    "|  [FAIL] FAILED at {} (duration: {})",
                    timestamp,
                    duration_str()
                );
                println!("|  Error: {}", error);
                println!("+----------------------------------------------------------------");
            }
            _ => {
                println!("|  {} at {}", record_kind, timestamp);
            }
        }
    }

    println!();
    Ok(())
}

/// Extract `reason` field from a `Failed` record's `metadata_json` payload.
fn extract_failure_reason(json: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(json)
        .ok()
        .and_then(|v| v.get("reason").and_then(|r| r.as_str().map(String::from)))
}

/// Extract `data_delta_version` from a `DataCommitted` record's
/// `metadata_json`.  Returns 0 if the field is missing (older records).
fn parse_data_delta_version(json: &str) -> i64 {
    serde_json::from_str::<serde_json::Value>(json)
        .ok()
        .and_then(|v| v.get("data_delta_version").and_then(|n| n.as_i64()))
        .unwrap_or(0)
}

/// Decode the post-commit attribute payload from `metadata_json`.
fn parse_post_commit_metadata(json: &str) -> PostCommitMetadata {
    serde_json::from_str(json).unwrap_or_default()
}

/// System directories where factory nodes live.
///
/// - `/system/run/`  -- auto-executing on post-commit (remote push/pull)
/// - `/system/etc/`  -- manually triggered or passive (hydrovu, sitegen, column-rename)
pub const SYSTEM_RUN_DIR: &str = "/system/run";
pub const SYSTEM_ETC_DIR: &str = "/system/etc";

/// Resolve a bare factory name against `/system/run/` then `/system/etc/`.
/// Returns the first path that exists, or falls back to `/system/run/{name}`.
pub async fn resolve_short_factory_name(ship_context: &ShipContext, name: &str) -> Result<String> {
    if name.starts_with('/') {
        return Ok(name.to_string());
    }

    let mut ship = ship_context.open_pond().await?;
    let tx = ship
        .begin_read(&steward::PondUserMetadata::new(vec![
            "resolve-factory".to_string(),
        ]))
        .await?;

    let root = tx.root().await?;

    // Try /system/run/ first (auto-executing), then /system/etc/ (manual)
    for dir in &[SYSTEM_RUN_DIR, SYSTEM_ETC_DIR] {
        let candidate = format!("{}/{}", dir, name);
        if root.resolve_path(&candidate).await.is_ok() {
            _ = tx.commit().await?;
            log::info!("Resolved short name '{}' -> '{}'", name, candidate);
            return Ok(candidate);
        }
    }

    _ = tx.commit().await?;
    // Fall back to /system/run/ (will produce a clear error downstream)
    let fallback = format!("{}/{}", SYSTEM_RUN_DIR, name);
    log::info!(
        "Short name '{}' not found, defaulting to '{}'",
        name,
        fallback
    );
    Ok(fallback)
}

/// Show incomplete operations for recovery
async fn show_incomplete_operations(control_table: &mut steward::ControlTable) -> Result<()> {
    // Control table automatically sees latest Delta commits via DataFusion
    control_table.print_banner();

    println!("\n+===========================================================================+");
    println!("|                      INCOMPLETE OPERATIONS                                 |");
    println!("+===========================================================================+\n");

    // Use existing method from control table
    let incomplete = control_table
        .find_incomplete_transactions()
        .await
        .map_err(|e| anyhow!("Failed to find incomplete transactions: {}", e))?;

    if incomplete.is_empty() {
        println!("No incomplete transactions found. All operations completed successfully.\n");
        return Ok(());
    }

    println!("Found {} incomplete transaction(s):\n", incomplete.len());

    for (txn_meta, data_fs_version) in incomplete {
        println!(
            "+- Transaction {} --------------------------------------------",
            txn_meta.txn_seq
        );
        println!("|  UUID         : {}", txn_meta.user.txn_id);
        println!("|  Status       : [WARN]  Incomplete (crashed during execution)");

        if data_fs_version > 0 {
            println!(
                "|  Data Version : {} (data was committed before crash)",
                data_fs_version
            );
        } else {
            println!("|  Data Version : N/A (crashed before data commit)");
        }

        // CLI args are no longer captured by the post-D2 control table;
        // see the corresponding `pond_txn` Delta commit metadata if the
        // original command is needed for diagnostics.

        println!("+----------------------------------------------------------------");
        println!();
    }

    println!("To recover, you may need to manually inspect or retry these operations.\n");

    Ok(())
}

/// Format microsecond timestamp as human-readable string
fn format_timestamp(micros: i64) -> String {
    use chrono::{TimeZone, Utc};
    let dt = Utc.timestamp_opt(micros / 1_000_000, ((micros % 1_000_000) * 1000) as u32);
    match dt.single() {
        Some(datetime) => datetime.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        None => format!("<invalid timestamp: {}>", micros),
    }
}

/// Truncate error message for display
fn truncate_error(error: &str) -> String {
    const MAX_LEN: usize = 100;
    if error.len() <= MAX_LEN {
        error.to_string()
    } else {
        format!("{}...", &error[..MAX_LEN])
    }
}

/// Show pond configuration (ID, factory modes, metadata, settings)
async fn show_pond_config(control_table: &steward::ControlTable) -> Result<()> {
    let metadata = control_table.get_pond_metadata();

    println!("Pond Configuration");
    println!("==================");
    println!();
    println!("Pond ID:        {}", metadata.pond_id);
    println!(
        "Created:        {}",
        chrono::DateTime::from_timestamp_micros(metadata.birth_timestamp)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| "unknown".to_string())
    );
    println!("Created by:     {}", metadata.birth_username);
    println!(
        "Birthplace:     {}",
        if metadata.birthplace.is_empty() {
            "(unspecified)"
        } else {
            &metadata.birthplace
        }
    );
    println!();
    println!("Factory Modes:");
    println!("--------------");

    let factory_modes = control_table.factory_modes();
    if factory_modes.is_empty() {
        println!("  (none configured)");
    } else {
        for (factory_name, mode) in factory_modes.iter() {
            println!("  {:20} {}", format!("{}:", factory_name), mode);
        }
    }

    println!();
    println!("Settings:");
    println!("---------");

    let settings = control_table.settings();
    if settings.is_empty() {
        println!("  (none configured)");
    } else {
        for (key, value) in settings.iter() {
            println!("  {:20} {}", format!("{}:", key), value);
        }
    }

    Ok(())
}

/// Set a pond configuration value
async fn set_pond_config(
    control_table: &mut steward::ControlTable,
    key: &str,
    value: &str,
) -> Result<()> {
    control_table
        .set_setting(key, value)
        .await
        .map_err(|e| anyhow!("Failed to set setting: {}", e))?;

    println!("[OK] Set '{}' = '{}'", key, value);
    Ok(())
}

/// Single-column COUNT(*) result row.
#[derive(Debug, Deserialize)]
struct CountRow {
    n: i64,
}

/// Outcome of computing the safe control-table prune horizon.
pub struct PruneHorizon {
    /// Highest sequence number whose lifecycle rows may be deleted.
    pub horizon: i64,
    /// `MAX(txn_seq)` with a `data_committed` record.
    pub last_committed: i64,
    /// `last_committed - keep_txns`.
    pub retention_horizon: i64,
    /// Minimum `last_pushed_seq` across push remotes, or `None` when no
    /// push remote is attached.
    pub replication_horizon: Option<i64>,
}

/// Compute the safe prune horizon: the lesser of the minimum push-remote
/// `last_pushed_seq` and `last_committed - keep_txns`.  Read-only.
///
/// Returns an error when no push remote is attached unless
/// `allow_no_remote` is set, or when a push remote has no watermark yet.
pub async fn compute_prune_horizon(
    ship: &mut steward::Steward,
    keep_txns: i64,
    allow_no_remote: bool,
) -> Result<PruneHorizon> {
    use crate::commands::remote::{list_remote_names, load_remote_attachment};
    use steward::{REMOTE_MODE_PREFIX, RemoteMode};

    if keep_txns < 0 {
        return Err(anyhow!("keep-txns must be >= 0"));
    }

    let last_committed = ship
        .control_table()
        .get_last_write_sequence()
        .await
        .map_err(|e| anyhow!("Failed to read last committed seq: {}", e))?;

    // Enumerate push-mode remotes and gather their replication watermarks.
    let names = list_remote_names(ship).await?;
    let mut push_remotes: Vec<(String, String)> = Vec::new();
    for name in names {
        let mode = ship
            .control_table()
            .raw_config_get(&format!("{REMOTE_MODE_PREFIX}{name}"))
            .await
            .unwrap_or_default()
            .unwrap_or_else(|| "push".to_string());
        let pushes = RemoteMode::parse(&mode).map(|m| m.pushes()).unwrap_or(true);
        if !pushes {
            continue;
        }
        let attachment = load_remote_attachment(ship, &name).await?;
        push_remotes.push((name, attachment.url));
    }

    let replication_horizon = if push_remotes.is_empty() {
        if !allow_no_remote {
            return Err(anyhow!(
                "no push-mode remote attached; refusing to prune unreplicated history. \
                 Re-run with --allow-no-remote to prune by retention only. Pruned history \
                is then unrecoverable and any future remote must bootstrap via a fresh \
                `pond pull`."
            ));
        }
        None
    } else {
        let mut min_seq: Option<i64> = None;
        for (name, url) in &push_remotes {
            let watermark = ship
                .control_table()
                .raw_config_get(&format!("last_pushed_seq:{}", url))
                .await
                .unwrap_or_default()
                .and_then(|s| s.parse::<i64>().ok());
            let seq = watermark.ok_or_else(|| {
                anyhow!(
                    "push remote '{}' ({}) has no last_pushed_seq watermark; nothing has \
                     been replicated to it, refusing to prune.",
                    name,
                    url
                )
            })?;
            min_seq = Some(min_seq.map_or(seq, |m| m.min(seq)));
        }
        min_seq
    };

    let retention_horizon = last_committed - keep_txns;
    let horizon = match replication_horizon {
        Some(r) => r.min(retention_horizon),
        None => retention_horizon,
    };

    Ok(PruneHorizon {
        horizon,
        last_committed,
        retention_horizon,
        replication_horizon,
    })
}

/// Print the computed horizon breakdown.
fn print_prune_horizon(h: &PruneHorizon, keep_txns: i64) {
    println!("Control-table prune:");
    println!("  last committed seq : {}", h.last_committed);
    println!("  keep-txns          : {}", keep_txns);
    println!("  retention horizon  : {}", h.retention_horizon);
    match h.replication_horizon {
        Some(r) => println!("  replication horizon: {} (min push last_pushed_seq)", r),
        None => println!("  replication horizon: (no push remote)"),
    }
    println!("  effective horizon  : {}", h.horizon);
}

/// Delete local-pond lifecycle history at or below `horizon` and report
/// the row count.  Does NOT checkpoint/vacuum; the caller is expected to
/// run `Ship::maintain` afterwards so the deletion is reclaimed in the
/// same maintenance pass.  Returns the number of rows deleted (0 when the
/// horizon is below 1).
pub async fn prune_history_at_horizon(ship: &mut steward::Steward, horizon: i64) -> Result<usize> {
    if horizon < 1 {
        return Ok(0);
    }
    let deleted = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("control prune requires a pond steward"))?
        .prune_control_history(
            horizon,
            &steward::PondUserMetadata::new(vec![
                "pond".to_string(),
                "control".to_string(),
                "prune".to_string(),
            ]),
        )
        .await
        .map_err(|e| anyhow!("prune failed: {}", e))?;
    Ok(deleted)
}

/// `pond control prune`: compute the horizon, then either report
/// (`dry_run`) or delete and reclaim space via a standalone checkpoint +
/// vacuum.
async fn prune_control_table(
    ship: &mut steward::Steward,
    keep_txns: i64,
    dry_run: bool,
    allow_no_remote: bool,
) -> Result<()> {
    let h = compute_prune_horizon(ship, keep_txns, allow_no_remote).await?;
    print_prune_horizon(&h, keep_txns);

    if h.horizon < 1 {
        println!("Nothing to prune (horizon < 1).");
        return Ok(());
    }

    if dry_run {
        let pond_id = ship.control_table().pond_id_uuid();
        let ctx = ship.control_table().session_context();
        let count_sql = format!(
            "SELECT COUNT(*) AS n FROM control \
             WHERE pond_id = '{pid}' AND record_kind != 'setting' AND txn_seq <= {h}",
            pid = pond_id,
            h = h.horizon,
        );
        let batches = ctx
            .sql(&count_sql)
            .await
            .map_err(|e| anyhow!("count query failed: {}", e))?
            .collect()
            .await
            .map_err(|e| anyhow!("count collect failed: {}", e))?;
        let mut candidate_rows = 0i64;
        for batch in &batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let rows: Vec<CountRow> = serde_arrow::from_record_batch(batch)
                .map_err(|e| anyhow!("count decode failed: {}", e))?;
            if let Some(r) = rows.first() {
                candidate_rows = r.n;
            }
        }
        println!(
            "  would delete       : {} rows (txn_seq <= {})  [dry-run]",
            candidate_rows, h.horizon
        );
        return Ok(());
    }

    let deleted = prune_history_at_horizon(ship, h.horizon).await?;
    println!("  deleted            : {} rows", deleted);

    println!("  reclaiming space (checkpoint + vacuum) ...");
    let _report = ship
        .maintain(true, false)
        .await
        .map_err(|error| anyhow!("Failed to reclaim pruned control history: {error}"))?;
    println!("[OK] Control table pruned at horizon {}.", h.horizon);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_error() {
        let short = "Short error";
        assert_eq!(truncate_error(short), "Short error");

        let long = "a".repeat(150);
        let truncated = truncate_error(&long);
        assert!(truncated.ends_with("..."));
        assert!(truncated.len() <= 103); // 100 + "..."
    }
}
