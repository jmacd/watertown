// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Result, anyhow};

use crate::common::{ShipContext, format_node_id};
use url::Url;
use utilities::banner;

/// Label for transaction sequence in detailed view headers
const TRANSACTION_LABEL: &str = "Transaction";

/// Show pond contents with a closure for handling output
pub async fn show_command<F>(ship_context: &ShipContext, mode: &str, mut handler: F) -> Result<()>
where
    F: FnMut(&str),
{
    let mut ship = ship_context.open_pond().await?;

    let result =
        crate::common::with_read_transaction(&mut ship, vec!["show".to_string()], async |tx| {
            show_filesystem_transactions(tx, mode)
                .await
                .map_err(|e| anyhow!("Failed to access data filesystem: {}", e))
        })
        .await?;

    handler(&result);
    Ok(())
}

async fn show_filesystem_transactions(
    tx: &mut steward::Transaction<'_>,
    mode: &str,
) -> Result<String, steward::StewardError> {
    // Get the pond-specific guard for commit history and store path
    let pond = tx
        .as_pond()
        .expect("show command requires a pond transaction");

    // Get commit history from the filesystem
    let commit_history = pond
        .get_commit_history(None)
        .await
        .map_err(steward::StewardError::DataInit)?;

    if commit_history.is_empty() {
        return Ok("No transactions found in this filesystem.".to_string());
    }

    // Get store path for display purposes
    let store_path = pond
        .store_path()
        .map_err(steward::StewardError::DataInit)?
        .to_string_lossy()
        .to_string();
    let pond_path = std::path::Path::new(&store_path)
        .parent()
        .ok_or_else(|| steward::StewardError::Dyn("Invalid store path".into()))?
        .to_path_buf();

    // Route to appropriate display mode
    match mode {
        "brief" => show_brief_mode(&commit_history, &store_path, &pond_path, tx).await,
        "concise" => show_concise_mode(tx).await,
        "detailed" => show_detailed_mode(&commit_history, &store_path, &pond_path, tx).await,
        _ => Err(steward::StewardError::Dyn(
            format!(
                "Invalid mode '{}'. Use 'brief', 'concise', or 'detailed'.",
                mode
            )
            .into(),
        )),
    }
}

/// Show brief summary statistics about the pond
async fn show_brief_mode(
    commit_history: &[deltalake::kernel::CommitInfo],
    store_path: &str,
    _pond_path: &std::path::Path,
    tx: &mut steward::Transaction<'_>,
) -> Result<String, steward::StewardError> {
    use std::collections::HashMap;

    let mut output = String::new();

    // Access control table through pond-specific guard (uses Ship's cached instance)
    let control_table = tx
        .as_pond()
        .expect("show command requires a pond transaction")
        .control_table();

    control_table.print_banner();

    // Get DataFusion session context from the existing transaction guard
    let session_ctx = tx.session_context().await.map_err(|e| {
        steward::StewardError::Dyn(format!("Failed to get session context: {}", e).into())
    })?;

    // Query partition statistics using SQL against the pre-registered delta_table
    let partition_stats_sql = "
        SELECT 
            part_id,
            COUNT(*) as total_rows,
            SUM(CASE WHEN file_type = 'directory' THEN 1 ELSE 0 END) as directory_rows,
            COUNT(DISTINCT CASE WHEN file_type != 'directory' THEN node_id ELSE NULL END) as distinct_files,
            SUM(CASE WHEN file_type != 'directory' THEN 1 ELSE 0 END) as file_version_rows
        FROM delta_table
        GROUP BY part_id
    ";

    let df = session_ctx.sql(partition_stats_sql).await.map_err(|e| {
        steward::StewardError::Dyn(format!("Failed to query partition stats: {}", e).into())
    })?;

    let batches = df.collect().await.map_err(|e| {
        steward::StewardError::Dyn(format!("Failed to collect partition stats: {}", e).into())
    })?;

    // Parse the results into our PartitionStats structure
    let mut partition_map: HashMap<String, PartitionStats> = HashMap::new();

    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::DataType;
    use arrow_cast::cast;

    for batch in batches {
        // Cast part_id from Dictionary to plain Utf8 if needed
        let part_id_col = batch
            .column_by_name("part_id")
            .ok_or_else(|| steward::StewardError::Dyn("Missing part_id column".into()))?;
        let part_id_string_col = match part_id_col.data_type() {
            DataType::Utf8 => part_id_col.clone(),
            _ => cast(part_id_col.as_ref(), &DataType::Utf8).map_err(|e| {
                steward::StewardError::Dyn(format!("Failed to cast part_id to Utf8: {}", e).into())
            })?,
        };
        let part_ids = part_id_string_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                steward::StewardError::Dyn(
                    format!(
                        "Failed to downcast part_id column. Actual type: {:?}",
                        part_id_col.data_type()
                    )
                    .into(),
                )
            })?;
        let total_rows = batch
            .column_by_name("total_rows")
            .ok_or_else(|| steward::StewardError::Dyn("Missing total_rows column".into()))?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast total_rows column".into())
            })?;
        let directory_rows = batch
            .column_by_name("directory_rows")
            .ok_or_else(|| steward::StewardError::Dyn("Missing directory_rows column".into()))?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast directory_rows column".into())
            })?;
        let distinct_files = batch
            .column_by_name("distinct_files")
            .ok_or_else(|| steward::StewardError::Dyn("Missing distinct_files column".into()))?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast distinct_files column".into())
            })?;
        let file_versions = batch
            .column_by_name("file_version_rows")
            .ok_or_else(|| steward::StewardError::Dyn("Missing file_version_rows column".into()))?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast file_versions column".into())
            })?;

        for i in 0..batch.num_rows() {
            let part_id = part_ids.value(i).to_string();
            let stats = PartitionStats {
                total_rows: total_rows.value(i) as usize,
                directory_rows: directory_rows.value(i) as usize,
                distinct_files: distinct_files.value(i) as usize,
                file_versions: file_versions.value(i) as usize,
                path_name: None,
            };
            _ = partition_map.insert(part_id, stats);
        }
    }

    // Get aggregate file statistics from Delta Lake metadata
    let url = Url::from_directory_path(store_path)
        .or_else(|_| Url::from_file_path(store_path))
        .map_err(|_| {
            steward::StewardError::Dyn(
                format!("Failed to create URL from path: {}", store_path).into(),
            )
        })?;
    let table = deltalake::open_table(url).await.map_err(|e| {
        steward::StewardError::Dyn(format!("Failed to open Delta table: {}", e).into())
    })?;

    let snapshot = table
        .snapshot()
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to get snapshot: {}", e).into()))?;

    let log_store = table.log_store();
    let mut file_stream = snapshot.snapshot().file_views(log_store.as_ref(), None);

    use futures::stream::StreamExt;

    let mut total_parquet_files = 0;
    let mut total_parquet_bytes: i64 = 0;

    while let Some(add_result) = file_stream.next().await {
        if let Ok(add_action) = add_result {
            total_parquet_files += 1;
            total_parquet_bytes += add_action.size();
        }
    }

    // Resolve partition paths by traversing TinyFS.  Partition map keys
    // are the raw OpLog `part_id` hex strings.  A partition is owned by a
    // self-partitioned physical directory, so its path is found by
    // reconstructing that directory's FileID (part_id + pond_id) and
    // looking it up in a FileID-keyed path map -- mirroring detailed mode.
    log::debug!("Resolving partition paths via TinyFS...");
    let root = tx
        .root()
        .await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to get root: {}", e).into()))?;

    let matches = root.collect_matches("**/*").await.map_err(|e| {
        steward::StewardError::Dyn(format!("Failed to collect matches: {}", e).into())
    })?;

    // Build a FileID-string -> path map from all matched entries, plus the
    // root directory (which collect_matches does not itself yield).
    let mut path_map: HashMap<String, String> = HashMap::new();
    for (node_path, _captured) in matches {
        _ = path_map.insert(
            node_path.id().to_string(),
            node_path.path().to_string_lossy().to_string(),
        );
    }
    let root_file_id = root.node_path().id();
    let pond_id = root_file_id.pond_id();
    _ = path_map.insert(root_file_id.to_string(), "/".to_string());

    // For each partition, reconstruct the owning directory's FileID and
    // resolve its path.
    for (part_id_hex, stats) in partition_map.iter_mut() {
        let Ok(part_id) = tinyfs::PartID::from_hex_string(part_id_hex) else {
            continue;
        };
        let dir_file_id = tinyfs::FileID::from_physical_dir_node_id(part_id.to_node_id(), pond_id);
        if let Some(path) = path_map.get(&dir_file_id.to_string()) {
            stats.path_name = Some(path.clone());
        }
    }

    // Format the output
    output.push('\n');
    output.push_str(
        "+============================================================================+\n",
    );
    output.push_str(
        "|                            POND SUMMARY                                    |\n",
    );
    output.push_str(
        "+============================================================================+\n",
    );
    output.push('\n');

    output.push_str(&format!(
        "  Transactions       : {}\n",
        commit_history.len()
    ));
    output.push_str(&format!(
        "  Delta Lake Version : {}\n",
        table.version().unwrap_or(0)
    ));
    output.push('\n');

    output.push_str("  Storage Statistics\n");
    output.push_str("  ------------------\n");
    output.push_str(&format!("  Parquet Files      : {}\n", total_parquet_files));
    output.push_str(&format!(
        "  Total Size         : {}\n",
        format_byte_size(total_parquet_bytes)
    ));
    output.push_str(&format!("  Partitions         : {}\n", partition_map.len()));
    output.push('\n');

    // Sort partitions by total rows
    let mut partition_vec: Vec<_> = partition_map.into_iter().collect();
    partition_vec.sort_by_key(|b| std::cmp::Reverse(b.1.total_rows));

    // Show all partitions with detailed breakdown
    output.push_str("  Partitions (by row count)\n");
    output.push_str("  -------------------------\n");
    for (part_id, stats) in partition_vec.iter() {
        let path_display = stats.path_name.as_deref().unwrap_or("<unknown>");

        output.push_str(&format!(
            "\n  {} {}\n",
            format_node_id(part_id),
            path_display
        ));
        output.push_str(&format!(
            "    {} rows ({} dir, {} files, {} versions)\n",
            stats.total_rows, stats.directory_rows, stats.distinct_files, stats.file_versions
        ));
    }
    output.push('\n');

    Ok(output)
}

#[derive(Default)]
struct PartitionStats {
    total_rows: usize,
    directory_rows: usize,
    distinct_files: usize,
    file_versions: usize,
    path_name: Option<String>,
}

/// Stub kept for source-level compatibility.  Post-D2 the control table
/// no longer persists CLI args; original commands live in data Delta
/// commit metadata (`pond_txn`).  `pond show` no longer renders a
/// `Command:` line.
async fn query_transaction_commands(
    _control_table: &steward::ControlTable,
) -> Result<std::collections::HashMap<i64, Vec<String>>, steward::StewardError> {
    Ok(std::collections::HashMap::new())
}

/// Show a one-line-per-transaction summary (newest first).  Each line
/// reports the transaction sequence, how many OpLog operations it wrote,
/// how many distinct nodes it touched, and when it committed.
async fn show_concise_mode(
    tx: &mut steward::Transaction<'_>,
) -> Result<String, steward::StewardError> {
    use arrow::array::{Array, Int64Array};

    let control_table = tx
        .as_pond()
        .expect("show command requires a pond transaction")
        .control_table();
    control_table.print_banner();

    let session_ctx = tx.session_context().await.map_err(|e| {
        steward::StewardError::Dyn(format!("Failed to get session context: {}", e).into())
    })?;

    let sql = "
        SELECT txn_seq,
            CAST(COUNT(*) AS BIGINT) AS ops,
            CAST(COUNT(DISTINCT node_id) AS BIGINT) AS nodes,
            CAST(MAX(timestamp) AS BIGINT) AS ts
        FROM delta_table
        GROUP BY txn_seq
        ORDER BY txn_seq DESC
    ";

    let batches = session_ctx
        .sql(sql)
        .await
        .map_err(|e| {
            steward::StewardError::Dyn(format!("Failed to query transactions: {}", e).into())
        })?
        .collect()
        .await
        .map_err(|e| {
            steward::StewardError::Dyn(format!("Failed to collect transactions: {}", e).into())
        })?;

    let mut output = String::new();
    output.push_str("\nTransactions (newest first)\n");
    output.push_str("---------------------------\n");

    let mut any_rows = false;
    for batch in &batches {
        let col = |name: &str| -> Result<&Int64Array, steward::StewardError> {
            batch
                .column_by_name(name)
                .ok_or_else(|| {
                    steward::StewardError::Dyn(format!("Missing {} column", name).into())
                })?
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    steward::StewardError::Dyn(format!("Failed to downcast {} column", name).into())
                })
        };
        let txn_seqs = col("txn_seq")?;
        let ops = col("ops")?;
        let nodes = col("nodes")?;
        let ts = col("ts")?;

        for i in 0..batch.num_rows() {
            any_rows = true;
            let when = if ts.is_null(i) {
                "unknown time".to_string()
            } else {
                format_micros_timestamp(ts.value(i))
            };
            output.push_str(&format!(
                "  seq {:>5}  {:>4} op(s)  {:>4} node(s)  {}\n",
                txn_seqs.value(i),
                ops.value(i),
                nodes.value(i),
                when,
            ));
        }
    }

    if !any_rows {
        output.push_str("  (no transactions)\n");
    }
    output.push('\n');

    Ok(output)
}

/// Format a microsecond Unix timestamp as a readable UTC string.
fn format_micros_timestamp(micros: i64) -> String {
    chrono::DateTime::from_timestamp_micros(micros)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| format!("<invalid timestamp: {}>", micros))
}

/// Show detailed transaction log using transaction sequences
async fn show_detailed_mode(
    _commit_history: &[deltalake::kernel::CommitInfo],
    _store_path: &str,
    _pond_path: &std::path::Path,
    tx: &mut steward::Transaction<'_>,
) -> Result<String, steward::StewardError> {
    use std::collections::HashMap;

    let mut output = String::new();

    // Access control table through pond-specific guard (uses Ship's cached instance)
    let control_table = tx
        .as_pond()
        .expect("show command requires a pond transaction")
        .control_table();

    control_table.print_banner();

    // Query control table for transaction commands
    // Build a map of txn_seq -> cli_args
    let command_map = query_transaction_commands(control_table).await?;

    // Get session context from the existing transaction guard (no need to create a new one)
    let session_ctx = tx.session_context().await.map_err(|e| {
        steward::StewardError::Dyn(format!("Failed to get session context: {}", e).into())
    })?;

    // Build path map by traversing TinyFS (same as brief mode)
    log::debug!("Resolving partition paths via TinyFS...");
    let mut path_map: HashMap<String, String> = HashMap::new();

    // Access TinyFS root through the Steward transaction guard (it derefs to FS)
    let fs: &tinyfs::FS = &*tx;
    let root = fs
        .root()
        .await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to get root: {}", e).into()))?;

    let matches = root.collect_matches("**/*").await.map_err(|e| {
        steward::StewardError::Dyn(format!("Failed to collect matches: {}", e).into())
    })?;

    // Build path map from all matched entries
    for (node_path, _captured) in matches {
        let node_id = node_path.id();
        let path = node_path.path();
        let node_id_str = node_id.to_string();
        _ = path_map.insert(node_id_str, path.to_string_lossy().to_string());
    }

    // Also add the root directory
    let root_node_id = root.node_path().id();
    let pond_id = root_node_id.pond_id();
    _ = path_map.insert(root_node_id.to_string(), "/".to_string());

    // Query ALL operations in a single query, ordered by transaction (newest first)
    // This is vastly more efficient than querying each transaction separately (was 79 queries!)
    let all_ops_sql = "
        SELECT txn_seq, part_id, node_id, file_type, version, timestamp, size, content, factory
        FROM delta_table
        ORDER BY txn_seq DESC, part_id, node_id, version
    ";

    let df = session_ctx.sql(all_ops_sql).await.map_err(|e| {
        steward::StewardError::Dyn(format!("Failed to query operations: {}", e).into())
    })?;

    let batches = df.collect().await.map_err(|e| {
        steward::StewardError::Dyn(format!("Failed to collect operations: {}", e).into())
    })?;

    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        return Ok("No transactions found.\n".to_string());
    }

    // Group batches by transaction sequence
    use arrow::array::{Array, Int64Array};
    use std::collections::BTreeMap;

    let mut txn_batches: BTreeMap<i64, Vec<arrow::record_batch::RecordBatch>> = BTreeMap::new();

    for batch in batches {
        let txn_seq_col = batch
            .column_by_name("txn_seq")
            .ok_or_else(|| steward::StewardError::Dyn("Missing txn_seq column".into()))?;
        let txn_seqs = txn_seq_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast txn_seq column".into())
            })?;

        // Group rows by txn_seq within this batch
        let mut current_txn_rows: HashMap<i64, Vec<usize>> = HashMap::new();
        for row_idx in 0..batch.num_rows() {
            let txn_seq = txn_seqs.value(row_idx);
            current_txn_rows.entry(txn_seq).or_default().push(row_idx);
        }

        // Create a separate batch for each transaction
        for (txn_seq, row_indices) in current_txn_rows {
            let txn_batch = batch.slice(row_indices[0], row_indices.len());
            txn_batches.entry(txn_seq).or_default().push(txn_batch);
        }
    }

    // Format output for each transaction (in descending order)
    for (txn_seq, batches_for_txn) in txn_batches.iter().rev() {
        // Extract unique PartIDs and NodeIDs for the header
        use arrow::array::StringArray;
        use arrow::datatypes::DataType;
        use std::collections::BTreeSet;

        let mut part_ids: BTreeSet<String> = BTreeSet::new();
        let mut node_ids: BTreeSet<String> = BTreeSet::new();

        for batch in batches_for_txn.iter() {
            // Extract part_id column
            if let Some(part_id_col) = batch.column_by_name("part_id") {
                let part_id_string_col = match part_id_col.data_type() {
                    DataType::Utf8 => part_id_col.clone(),
                    _ => arrow_cast::cast(part_id_col.as_ref(), &DataType::Utf8).map_err(|e| {
                        steward::StewardError::Dyn(format!("Failed to cast part_id: {}", e).into())
                    })?,
                };
                if let Some(part_id_array) =
                    part_id_string_col.as_any().downcast_ref::<StringArray>()
                {
                    for i in 0..part_id_array.len() {
                        if !part_id_array.is_null(i) {
                            _ = part_ids.insert(part_id_array.value(i).to_string());
                        }
                    }
                }
            }

            // Extract node_id column
            if let Some(node_id_col) = batch.column_by_name("node_id") {
                let node_id_string_col = match node_id_col.data_type() {
                    DataType::Utf8 => node_id_col.clone(),
                    _ => arrow_cast::cast(node_id_col.as_ref(), &DataType::Utf8).map_err(|e| {
                        steward::StewardError::Dyn(format!("Failed to cast node_id: {}", e).into())
                    })?,
                };
                if let Some(node_id_array) =
                    node_id_string_col.as_any().downcast_ref::<StringArray>()
                {
                    for i in 0..node_id_array.len() {
                        if !node_id_array.is_null(i) {
                            _ = node_ids.insert(node_id_array.value(i).to_string());
                        }
                    }
                }
            }
        }

        // Collect unique UUIDs and format with hyphens using uuid7's to_string()
        let mut unique_uuids: BTreeSet<String> = BTreeSet::new();
        for id_hex in part_ids.iter().chain(node_ids.iter()) {
            // Parse hex string as UUID and format with hyphens
            if let Ok(uuid) = id_hex.parse::<uuid7::Uuid>() {
                _ = unique_uuids.insert(uuid.to_string());
            } else {
                // If parsing fails, use the original hex string
                _ = unique_uuids.insert(id_hex.clone());
            }
        }
        let unique_ids: Vec<String> = unique_uuids.into_iter().collect();

        // Format transaction banner using reusable formatter
        output.push_str(&banner::format_transaction_banner(
            TRANSACTION_LABEL,
            *txn_seq,
            unique_ids,
        ));

        // CLI args are not captured by the post-D2 control table; the
        // original command lives in data Delta commit metadata
        // (`pond_txn`).  Skip the `Command:` line; `command_map` is
        // intentionally always empty.
        let _ = &command_map;
        output.push('\n');

        // Format operations for this transaction
        let formatted_ops =
            format_operations_from_batches(batches_for_txn.clone(), &path_map, pond_id)?;

        if formatted_ops.is_empty() {
            output.push_str("  (No operations found - this should not happen)\n");
        } else {
            for op in formatted_ops {
                output.push_str(&format!("  {}\n", op));
            }
        }
    }

    Ok(output)
}

/// Format byte size as human-readable string
fn format_byte_size(bytes: i64) -> String {
    if bytes < 0 {
        return "unknown size".to_string();
    }

    let bytes = bytes as f64;
    if bytes < 1024.0 {
        format!("{} bytes", bytes as i64)
    } else if bytes < 1024.0 * 1024.0 {
        format!("{:.1} KB", bytes / 1024.0)
    } else if bytes < 1024.0 * 1024.0 * 1024.0 {
        format!("{:.1} MB", bytes / (1024.0 * 1024.0))
    } else {
        format!("{:.1} GB", bytes / (1024.0 * 1024.0 * 1024.0))
    }
}

/// Format operations from Arrow batches (used by transaction sequence display)
fn format_operations_from_batches(
    batches: Vec<arrow::record_batch::RecordBatch>,
    path_map: &std::collections::HashMap<String, String>,
    pond_id: uuid7::Uuid,
) -> Result<Vec<String>, steward::StewardError> {
    use arrow::array::{Array, BinaryArray, Int64Array, StringArray};
    use arrow::datatypes::DataType;
    use arrow_cast::cast;
    use std::collections::HashMap;
    use tinyfs::tree_format::TreeNode;

    // Group operations by partition for better readability
    let mut partition_groups: HashMap<String, Vec<(String, Vec<String>)>> = HashMap::new();

    for batch in batches {
        // Extract columns
        let part_id_col = batch
            .column_by_name("part_id")
            .ok_or_else(|| steward::StewardError::Dyn("Missing part_id column".into()))?;
        let part_id_string_col = match part_id_col.data_type() {
            DataType::Utf8 => part_id_col.clone(),
            _ => cast(part_id_col.as_ref(), &DataType::Utf8).map_err(|e| {
                steward::StewardError::Dyn(format!("Failed to cast part_id: {}", e).into())
            })?,
        };
        let part_ids = part_id_string_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast part_id column".into())
            })?;

        let node_id_col = batch
            .column_by_name("node_id")
            .ok_or_else(|| steward::StewardError::Dyn("Missing node_id column".into()))?;
        let node_id_string_col = match node_id_col.data_type() {
            DataType::Utf8 => node_id_col.clone(),
            _ => cast(node_id_col.as_ref(), &DataType::Utf8).map_err(|e| {
                steward::StewardError::Dyn(format!("Failed to cast node_id: {}", e).into())
            })?,
        };
        let node_ids = node_id_string_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast node_id column".into())
            })?;

        let file_type_col = batch
            .column_by_name("file_type")
            .ok_or_else(|| steward::StewardError::Dyn("Missing file_type column".into()))?;
        let file_type_string_col = match file_type_col.data_type() {
            DataType::Utf8 => file_type_col.clone(),
            _ => cast(file_type_col.as_ref(), &DataType::Utf8).map_err(|e| {
                steward::StewardError::Dyn(format!("Failed to cast file_type: {}", e).into())
            })?,
        };
        let file_types = file_type_string_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast file_type column".into())
            })?;

        let version_col = batch
            .column_by_name("version")
            .ok_or_else(|| steward::StewardError::Dyn("Missing version column".into()))?;
        let versions = version_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast version column".into())
            })?;

        let size_col = batch
            .column_by_name("size")
            .ok_or_else(|| steward::StewardError::Dyn("Missing size column".into()))?;
        let sizes = size_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| steward::StewardError::Dyn("Failed to downcast size column".into()))?;

        let content_col = batch
            .column_by_name("content")
            .ok_or_else(|| steward::StewardError::Dyn("Missing content column".into()))?;
        let content_binary_col = match content_col.data_type() {
            DataType::Binary => content_col.clone(),
            _ => cast(content_col.as_ref(), &DataType::Binary).map_err(|e| {
                steward::StewardError::Dyn(format!("Failed to cast content: {}", e).into())
            })?,
        };
        let contents = content_binary_col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast content column".into())
            })?;

        let factory_col = batch
            .column_by_name("factory")
            .ok_or_else(|| steward::StewardError::Dyn("Missing factory column".into()))?;
        let factory_string_col = match factory_col.data_type() {
            DataType::Utf8 => factory_col.clone(),
            _ => cast(factory_col.as_ref(), &DataType::Utf8).map_err(|e| {
                steward::StewardError::Dyn(format!("Failed to cast factory: {}", e).into())
            })?,
        };
        let factories = factory_string_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast factory column".into())
            })?;

        // Format each row
        for i in 0..batch.num_rows() {
            let part_id_str = part_ids.value(i);
            let part_id = tinyfs::PartID::from_hex_string(part_id_str).map_err(|e| {
                steward::StewardError::Dyn(format!("Invalid part_id: {}", e).into())
            })?;
            let node_id_str = node_ids.value(i);
            let node_id = tinyfs::NodeID::from_hex_string(node_id_str).map_err(|e| {
                steward::StewardError::Dyn(format!("Invalid node_id: {}", e).into())
            })?;
            let file_type = file_types.value(i);
            let version = versions.value(i);
            let size = if sizes.is_null(i) { -1 } else { sizes.value(i) };
            let factory = if factories.is_null(i) {
                None
            } else {
                Some(factories.value(i))
            };

            // For directories, decode content and show entries
            let (detail_display, dir_entries) = if file_type == "directory" {
                // Check if this is a dynamic directory
                let is_dynamic = factory.is_some() && factory != Some("tlogfs");

                if is_dynamic {
                    // Dynamic directories don't have parseable content
                    let factory_name = factory.unwrap_or("unknown");
                    (format!(" (dynamic: {})", factory_name), Vec::new())
                } else if contents.is_null(i) {
                    (" (0 entries)".to_string(), Vec::new())
                } else {
                    let content_bytes = contents.value(i);
                    // Decode directory entries for static directories
                    match decode_directory_entries(content_bytes) {
                        Ok(entries) => {
                            let count = entries.len();
                            let entry_word = if count == 1 { "entry" } else { "entries" };
                            (format!(" ({} {})", count, entry_word), entries)
                        }
                        Err(e) => {
                            // If decode fails, just show that it has content
                            (format!(" (decode error: {})", e), Vec::new())
                        }
                    }
                }
            } else {
                // For files, show size
                (format!(" ({})", format_byte_size(size)), Vec::new())
            };

            let operation = format!(
                "Node {} {} v{}{}",
                format_node_id(&node_id),
                file_type,
                version,
                detail_display
            );

            // Convert directory entries to strings
            let entry_strings: Vec<String> = dir_entries
                .iter()
                .map(|entry| format!("{} -> {}", entry.name, format_node_id(&entry.child_node_id)))
                .collect();

            // Group by partition (directory) - use PartID for grouping
            let part_key = part_id.to_string();
            let part_entry = partition_groups.entry(part_key).or_default();
            part_entry.push((operation, entry_strings));
        }
    }

    // Format output grouped by partition using tree formatter
    let mut result = Vec::new();
    let mut sorted_partitions: Vec<_> = partition_groups.into_iter().collect();
    sorted_partitions.sort_by_key(|(part_id, _)| part_id.clone());

    for (part_id_str, ops) in sorted_partitions {
        // Parse part_id string back to PartID to construct directory FileID
        let part_id = tinyfs::PartID::from_hex_string(&part_id_str).map_err(|e| {
            steward::StewardError::Dyn(format!("Invalid part_id in map: {}", e).into())
        })?;

        // For directory partitions, the FileID is self-partitioned (part_id == node_id)
        let dir_file_id = tinyfs::FileID::from_physical_dir_node_id(part_id.to_node_id(), pond_id);

        let path_display = path_map
            .get(&dir_file_id.to_string())
            .map(|s| s.as_str())
            .unwrap_or("<unknown>");

        // Add partition label (not indented)
        result.push(format!(
            "  Partition {} {}:",
            format_node_id(&part_id_str),
            path_display
        ));

        // Create a tree for the operations under this partition
        let mut partition_root = TreeNode::new(""); // Empty root node

        for (operation, entries) in ops {
            // Create a node for this operation
            let mut node = TreeNode::new(operation);

            // Add directory entries as children
            for entry in entries {
                node.add_child(TreeNode::new(entry));
            }

            partition_root.add_child(node);
        }

        // Format the tree and add to result, skipping the empty root line and indenting the rest
        let tree_output = tinyfs::tree_format::format_tree(&partition_root);
        for (i, line) in tree_output.lines().enumerate() {
            if i == 0 {
                continue; // Skip the empty root label
            }
            result.push(format!("    {}", line)); // Indent tree structure relative to partition label
        }
        result.push(String::new()); // Blank line after each partition
    }

    Ok(result)
}

/// Decode directory content and return entries with their names and node IDs
fn decode_directory_entries(content_bytes: &[u8]) -> Result<Vec<tlogfs::DirectoryEntry>, String> {
    // Use the public helper from tlogfs
    let entries = tlogfs::schema::decode_directory_entries(content_bytes)
        .map_err(|e| format!("Failed to decode: {}", e))?;

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::init::init_command;
    use crate::common::ShipContext;
    use log::debug;
    use tempfile::TempDir;

    struct TestSetup {
        _temp_dir: TempDir,
        ship_context: ShipContext,
    }

    impl TestSetup {
        async fn new() -> Result<Self> {
            let temp_dir = TempDir::new().expect("Failed to create temp directory");
            let pond_path = temp_dir.path().join("test_pond");

            // Create ship context for initialization
            let init_args = vec!["pond".to_string(), "init".to_string()];
            let ship_context = ShipContext::pond_only(Some(pond_path.clone()), init_args.clone());

            // Initialize pond
            init_command(&ship_context, "test-host")
                .await
                .expect("Failed to initialize pond");

            Ok(TestSetup {
                _temp_dir: temp_dir,
                ship_context,
            })
        }
    }

    #[tokio::test]
    async fn test_show_empty_pond() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        let mut results = Vec::new();
        show_command(&setup.ship_context, "detailed", |output| {
            results.push(output.to_string());
        })
        .await
        .expect("Show command failed");

        // Should have at least the pond initialization transaction
        assert!(
            !results.is_empty(),
            "Should have at least initialization transaction"
        );

        // Check that output contains transaction information
        let output = results.join("");
        assert!(
            output.contains("Transaction"),
            "Should contain transaction information"
        );
    }

    #[tokio::test]
    async fn test_show_command_format() {
        use crate::common::ShipContext;
        use steward::Ship;
        use tempfile::tempdir;

        // Create a temporary pond using steward (like production)
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        // Initialize pond using steward - this creates the full Delta Lake setup
        let mut ship = Ship::create_pond(&pond_path, "test-host")
            .await
            .expect("Failed to initialize pond");

        // Add a transaction with some file operations
        let args = vec!["test_command".to_string(), "test_arg".to_string()];
        ship.write_transaction(&steward::PondUserMetadata::new(args), async |fs| {
            let data_root = fs.root().await?;
            _ = tinyfs::async_helpers::convenience::create_file_path(
                &data_root,
                "/example.txt",
                b"test content for show",
            )
            .await?;
            Ok(())
        })
        .await
        .expect("Failed to execute test transaction");

        // Create ship context for show command
        let ship_context =
            ShipContext::pond_only(Some(pond_path.clone()), vec!["test".to_string()]);

        // Capture show command output
        let mut captured_output = String::new();
        show_command(&ship_context, "detailed", |output: &str| {
            captured_output.push_str(output);
        })
        .await
        .expect("Show command should work");

        debug!("Show command output:\n{}", captured_output);

        // Test the new transaction sequence based format:

        // 1. Should have transaction sequence headers
        assert!(
            captured_output.contains(TRANSACTION_LABEL),
            "Should contain transaction headers"
        );

        // 2. Should show partition groups with paths
        assert!(
            captured_output.contains("Partition"),
            "Should contain partition information"
        );
        // The root partition should show "/" path
        assert!(
            captured_output.contains(" /:"),
            "Should show root directory path in partition line"
        );

        // 3. Should show the file we created
        assert!(
            captured_output.contains("file:physical"),
            "Should show the file we created"
        );

        // 4. Should not be empty
        assert!(
            !captured_output.trim().is_empty(),
            "Show output should not be empty"
        );
    }
}
