// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Simplified replication tests
//!
//! Tests verify the basic replication and sync workflow without
//! full command-line integration

use anyhow::Result;
use cmd::commands::init_command;
use cmd::common::ShipContext;
use datafusion::prelude::SessionContext;
use log::debug;
use std::path::Path;
use std::path::PathBuf;
use tempfile::TempDir;
use uuid7::Uuid;

/// Simple test setup
struct SimpleReplicationTest {
    source_pond: PathBuf,
}

impl SimpleReplicationTest {
    fn new() -> Result<Self> {
        let source_temp = TempDir::new()?;

        Ok(Self {
            source_pond: source_temp.path().to_path_buf(),
        })
    }

    async fn init_source(&self) -> Result<()> {
        let ship_context =
            ShipContext::pond_only(Some(self.source_pond.clone()), vec!["pond".to_string()]);

        init_command(&ship_context, "test-host").await?;
        Ok(())
    }

    async fn verify_source_pond_identity(&self) -> Result<Uuid> {
        let ship_context =
            ShipContext::pond_only(Some(self.source_pond.clone()), vec!["pond".to_string()]);

        let ship = ship_context.open_pond().await?;
        let control_table = ship.control_table();

        let pond_metadata = control_table.get_pond_metadata();

        Ok(pond_metadata.pond_id)
    }

    /// Compare two ponds to verify they have the same identity
    async fn compare_pond_identity(
        pond1_path: &Path,
        pond2_path: &Path,
    ) -> Result<(Uuid, Uuid, bool)> {
        let ship1 = ShipContext::pond_only(Some(&pond1_path), vec!["pond".to_string()])
            .open_pond()
            .await?;
        let ship2 = ShipContext::pond_only(Some(&pond2_path), vec!["pond".to_string()])
            .open_pond()
            .await?;

        let metadata1 = ship1.control_table().get_pond_metadata().clone();

        let metadata2 = ship2.control_table().get_pond_metadata().clone();

        // Check if identity matches
        let matches = metadata1 == metadata2;

        Ok((metadata1.pond_id, metadata2.pond_id, matches))
    }

    /// Compare transaction sequences between two ponds
    async fn compare_transaction_sequences(
        pond1_path: &Path,
        pond2_path: &Path,
    ) -> Result<(i64, i64)> {
        let ship1 = ShipContext::pond_only(Some(&pond1_path), vec!["pond".to_string()])
            .open_pond()
            .await?;
        let ship2 = ShipContext::pond_only(Some(&pond2_path), vec!["pond".to_string()])
            .open_pond()
            .await?;

        let last_seq_1 = ship1.control_table().get_last_write_sequence().await?;
        let last_seq_2 = ship2.control_table().get_last_write_sequence().await?;

        Ok((last_seq_1, last_seq_2))
    }

    /// Query transaction records from a pond.  Post-D2 schema lives at
    /// table "control" with `record_kind` (not `record_type`) and no
    /// `transaction_type` column.  We filter to "write" lifecycle events
    /// by selecting txn_seqs that have a `data_committed` record, then
    /// returning every non-setting non-post-commit lifecycle row for
    /// those seqs.
    async fn get_transaction_records(pond_path: &Path) -> Result<Vec<(i64, String)>> {
        let ship = ShipContext::pond_only(Some(&pond_path), vec!["pond".to_string()])
            .open_pond()
            .await?;

        let control_table = ship.control_table();
        let ctx = SessionContext::new();

        _ = ctx
            .register_table("control", control_table.table().table_provider().await?)
            .map_err(|e| anyhow::anyhow!("Failed to register table: {}", e))?;

        let sql = r#"
            WITH write_seqs AS (
                SELECT DISTINCT txn_seq
                FROM control
                WHERE record_kind = 'data_committed'
            )
            SELECT DISTINCT txn_seq, record_kind
            FROM control
            WHERE record_kind <> 'setting'
              AND NOT (has_parent_seq = true AND parent_seq <> txn_seq)
              AND txn_seq IN (SELECT txn_seq FROM write_seqs)
            ORDER BY txn_seq, record_kind
        "#;

        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to query: {}", e))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to collect: {}", e))?;

        let mut records = Vec::new();

        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }

            let txn_seqs = batch
                .column_by_name("txn_seq")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();

            let record_kind_col = batch.column_by_name("record_kind").unwrap();
            let record_kind_utf8 =
                arrow_cast::cast(record_kind_col.as_ref(), &arrow::datatypes::DataType::Utf8)?;
            let record_kinds = record_kind_utf8
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();

            for i in 0..batch.num_rows() {
                records.push((txn_seqs.value(i), record_kinds.value(i).to_string()));
            }
        }

        Ok(records)
    }

    /// Get comprehensive statistics about a pond's structure
    async fn get_pond_stats(pond_path: &Path) -> Result<PondStats, anyhow::Error> {
        let ship = ShipContext::pond_only(Some(&pond_path), vec!["pond".to_string()])
            .open_pond()
            .await?;

        let control_table = ship.control_table();
        let ctx = SessionContext::new();

        // Register control table under the post-D2 lean schema's
        // canonical name ("control").
        _ = ctx
            .register_table("control", control_table.table().table_provider().await?)
            .map_err(|e| anyhow::anyhow!("Failed to register control table: {}", e))?;

        // Get transaction count -- count `data_committed` records (one
        // per successful write transaction).
        let txn_count = ctx
            .sql("SELECT COUNT(*) as cnt FROM control WHERE record_kind = 'data_committed'")
            .await?
            .collect()
            .await?;
        let transaction_count = txn_count[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);

        // Get last write sequence
        let last_write_seq = control_table.get_last_write_sequence().await?;

        // Get commit history from data persistence
        let pond_id = control_table.get_pond_metadata().pond_id.to_string();
        let persistence = tlogfs::OpLogPersistence::open(
            &format!("{}/data", pond_path.to_string_lossy()),
            pond_id,
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open data persistence: {}", e))?;

        let commit_history = persistence
            .get_commit_history(None) // Get all commits
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get commit history: {}", e))?;

        let commit_count = commit_history.len() as i64;

        // Query oplog entries (nodes created/modified)
        let data_table = persistence.table();
        _ = ctx
            .register_table("oplog", data_table.table_provider().await?)
            .map_err(|e| anyhow::anyhow!("Failed to register oplog table: {}", e))?;

        // Count nodes by file_type
        let node_stats = ctx
            .sql(
                r#"
                SELECT 
                    file_type,
                    COUNT(DISTINCT node_id) as count
                FROM oplog
                GROUP BY file_type
                ORDER BY file_type
                "#,
            )
            .await?
            .collect()
            .await?;

        let mut nodes_by_type = std::collections::HashMap::new();
        for batch in node_stats {
            if batch.num_rows() == 0 {
                continue;
            }

            let file_type_col = batch.column_by_name("file_type").unwrap();
            let file_type_utf8 =
                arrow_cast::cast(file_type_col.as_ref(), &arrow::datatypes::DataType::Utf8)?;
            let file_types = file_type_utf8
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();

            let counts = batch
                .column_by_name("count")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();

            for i in 0..batch.num_rows() {
                let file_type = file_types.value(i).to_string();
                let count = counts.value(i);
                _ = nodes_by_type.insert(file_type, count);
            }
        }

        // Count total unique nodes
        let total_nodes = ctx
            .sql("SELECT COUNT(DISTINCT node_id) as cnt FROM oplog")
            .await?
            .collect()
            .await?;
        let total_node_count = total_nodes[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);

        // Count partitions (unique part_id values)
        let partition_count = ctx
            .sql("SELECT COUNT(DISTINCT part_id) as cnt FROM oplog")
            .await?
            .collect()
            .await?;
        let partition_count = partition_count[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);

        Ok(PondStats {
            transaction_count,
            last_write_seq,
            commit_count,
            total_node_count,
            partition_count,
            nodes_by_type,
        })
    }
}

/// Statistics about a pond's structure for comparison
#[derive(Debug, Clone, PartialEq)]
struct PondStats {
    /// Number of write transactions in control table
    transaction_count: i64,
    /// Last write sequence number
    last_write_seq: i64,
    /// Number of commits in data store history
    commit_count: i64,
    /// Total number of unique nodes
    total_node_count: i64,
    /// Number of unique partitions (part_id values)
    partition_count: i64,
    /// Breakdown of nodes by file_type (DirectoryPhysical, FilePhysicalVersion, etc.)
    nodes_by_type: std::collections::HashMap<String, i64>,
}

#[tokio::test]
async fn test_simple_pond_creation() -> Result<()> {
    use cmd::commands::mkdir_command;

    let test = SimpleReplicationTest::new()?;
    test.init_source().await?;

    let _pond_id = test.verify_source_pond_identity().await?;

    // Get initial stats
    let stats_initial = SimpleReplicationTest::get_pond_stats(&test.source_pond).await?;
    debug!(
        "Initial stats: txn_count={}, node_count={}, partitions={}",
        stats_initial.transaction_count,
        stats_initial.total_node_count,
        stats_initial.partition_count
    );

    // Write some data: create multiple directories
    let ship_context =
        ShipContext::pond_only(Some(test.source_pond.clone()), vec!["pond".to_string()]);

    mkdir_command(&ship_context, "/data", false).await?;
    mkdir_command(&ship_context, "/logs/app1", true).await?; // with parents
    mkdir_command(&ship_context, "/logs/app2", true).await?;
    mkdir_command(&ship_context, "/output", false).await?;

    // Get updated stats
    let stats_after = SimpleReplicationTest::get_pond_stats(&test.source_pond).await?;
    debug!(
        "After writes: txn_count={}, node_count={}, partitions={}",
        stats_after.transaction_count, stats_after.total_node_count, stats_after.partition_count
    );

    // Verify we wrote multiple transactions
    assert!(
        stats_after.transaction_count > stats_initial.transaction_count,
        "Should have more transactions after writes (initial: {}, after: {})",
        stats_initial.transaction_count,
        stats_after.transaction_count
    );

    // Verify we created multiple nodes (directories)
    assert!(
        stats_after.total_node_count > stats_initial.total_node_count,
        "Should have more nodes after creating directories (initial: {}, after: {})",
        stats_initial.total_node_count,
        stats_after.total_node_count
    );

    // We should have created at least 5 new directories: /data, /logs, /logs/app1, /logs/app2, /output
    assert!(
        stats_after.total_node_count >= stats_initial.total_node_count + 5,
        "Should have at least 5 new directories (created 5 directories explicitly)"
    );

    Ok(())
}

#[tokio::test]
async fn test_two_independent_ponds_have_different_ids() -> Result<()> {
    let test1 = SimpleReplicationTest::new()?;
    test1.init_source().await?;
    let pond_id_1 = test1.verify_source_pond_identity().await?;

    let test2 = SimpleReplicationTest::new()?;
    test2.init_source().await?;
    let pond_id_2 = test2.verify_source_pond_identity().await?;

    assert_ne!(
        pond_id_1, pond_id_2,
        "Two independent ponds should have different IDs"
    );

    Ok(())
}

#[tokio::test]
async fn test_pond_identity_comparison() -> Result<()> {
    // Create two ponds
    let test1 = SimpleReplicationTest::new()?;
    test1.init_source().await?;

    let test2 = SimpleReplicationTest::new()?;
    test2.init_source().await?;

    // Compare identities - should be different
    let (id1, id2, matches) =
        SimpleReplicationTest::compare_pond_identity(&test1.source_pond, &test2.source_pond)
            .await?;

    assert_ne!(id1, id2, "Two independent ponds should have different IDs");
    assert!(
        !matches,
        "Two independent ponds should not have matching identity"
    );

    // Compare transaction sequences - both should have same initial sequence
    let (seq1, seq2) = SimpleReplicationTest::compare_transaction_sequences(
        &test1.source_pond,
        &test2.source_pond,
    )
    .await?;

    // Both ponds just created, should have the same initial sequence (likely 0 or 1)
    assert_eq!(
        seq1, seq2,
        "Newly created ponds should have same initial sequence"
    );

    Ok(())
}

#[tokio::test]
async fn test_query_transaction_records() -> Result<()> {
    let test = SimpleReplicationTest::new()?;
    test.init_source().await?;

    // Query transaction records
    let records = SimpleReplicationTest::get_transaction_records(&test.source_pond).await?;

    // A newly initialized pond should have at least one transaction record
    assert!(
        !records.is_empty(),
        "Initialized pond should have transaction records"
    );

    // Check that we have "begin" record (init creates a transaction)
    let record_types: Vec<String> = records.iter().map(|(_, rt)| rt.clone()).collect();
    assert!(
        record_types.contains(&"begin".to_string()),
        "Init should create transaction with 'begin' record type"
    );

    // Verify transaction sequences are sequential (use DISTINCT to avoid counting multiple records per txn)
    let txn_seqs: Vec<i64> = records.iter().map(|(seq, _)| *seq).collect();
    let mut unique_seqs: Vec<i64> = txn_seqs.to_vec();
    unique_seqs.sort();
    unique_seqs.dedup();

    for window in unique_seqs.windows(2) {
        assert!(
            window[1] > window[0],
            "Transaction sequences should be monotonically increasing"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_pond_structural_statistics() -> Result<()> {
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use cmd::commands::mkdir_command;
    use cmd::commands::{CopyOptions, copy_command};
    use parquet::arrow::ArrowWriter;
    use std::fs;
    use std::fs::File;
    use std::sync::Arc;

    let test = SimpleReplicationTest::new()?;
    test.init_source().await?;

    // Create a temp file to copy into the pond as data file
    let temp_file = tempfile::NamedTempFile::new()?;
    fs::write(temp_file.path(), b"test data content")?;
    let temp_path = temp_file.path().to_str().unwrap().to_string();

    // Create a temp parquet file to copy as table
    let temp_parquet = tempfile::NamedTempFile::with_suffix(".parquet")?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )?;
    let file = File::create(temp_parquet.path())?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    let _ = writer.close()?;
    let parquet_path = temp_parquet.path().to_str().unwrap().to_string();

    let ship_context =
        ShipContext::pond_only(Some(test.source_pond.clone()), vec!["pond".to_string()]);

    // Create directories and copy files
    mkdir_command(&ship_context, "/data", false).await?;
    mkdir_command(&ship_context, "/tables", false).await?;

    // Copy files of different types (entry type encoded in URL)
    let sources1 = vec![format!("host:///{}", temp_path)];
    let sources2 = vec![format!("host+table:///{}", parquet_path)];
    copy_command(
        &ship_context,
        &sources1,
        "/data/file1.txt",
        &CopyOptions::default(),
    )
    .await?;
    copy_command(
        &ship_context,
        &sources2,
        "/tables/table1.parquet",
        &CopyOptions::default(),
    )
    .await?;

    // Get statistics about the pond structure
    let stats = SimpleReplicationTest::get_pond_stats(&test.source_pond).await?;

    // Debug: print what we got
    debug!("Pond stats: {:?}", stats);
    debug!("Nodes by type: {:?}", stats.nodes_by_type);

    // Verify we have multiple transactions (init + 2 mkdir + 2 copy = 5)
    assert!(
        stats.transaction_count >= 5,
        "Should have at least 5 transactions (was: {})",
        stats.transaction_count
    );

    assert!(stats.commit_count >= 5, "Should have at least 5 commits");

    // Should have multiple nodes: root + 2 directories + 2 files = at least 5
    assert!(
        stats.total_node_count >= 5,
        "Should have at least 5 nodes (was: {})",
        stats.total_node_count
    );

    // Verify we have directory nodes
    let has_directories = stats.nodes_by_type.contains_key("dir:physical")
        || stats.nodes_by_type.contains_key("dir:dynamic");
    assert!(
        has_directories,
        "Should have directory nodes. Got: {:?}",
        stats.nodes_by_type.keys().collect::<Vec<_>>()
    );

    // Verify we have file nodes of different types
    let has_data_files = stats.nodes_by_type.contains_key("file:physical:version");
    let has_table_files = stats.nodes_by_type.contains_key("table:physical:version");

    assert!(
        has_data_files,
        "Should have data file nodes. Got: {:?}",
        stats.nodes_by_type.keys().collect::<Vec<_>>()
    );

    assert!(
        has_table_files,
        "Should have table file nodes. Got: {:?}",
        stats.nodes_by_type.keys().collect::<Vec<_>>()
    );

    // Verify we have multiple partitions
    assert!(
        stats.partition_count >= 3,
        "Should have at least 3 partitions (was: {})",
        stats.partition_count
    );

    Ok(())
}

#[tokio::test]
async fn test_compare_pond_structures() -> Result<()> {
    // Create two independent ponds
    let test1 = SimpleReplicationTest::new()?;
    test1.init_source().await?;

    let test2 = SimpleReplicationTest::new()?;
    test2.init_source().await?;

    // Get statistics from both
    let stats1 = SimpleReplicationTest::get_pond_stats(&test1.source_pond).await?;
    let stats2 = SimpleReplicationTest::get_pond_stats(&test2.source_pond).await?;

    // Two freshly initialized ponds should have identical structure
    assert_eq!(
        stats1.transaction_count, stats2.transaction_count,
        "Transaction counts should match"
    );
    assert_eq!(
        stats1.commit_count, stats2.commit_count,
        "Commit counts should match"
    );
    assert_eq!(
        stats1.total_node_count, stats2.total_node_count,
        "Total node counts should match"
    );
    assert_eq!(
        stats1.partition_count, stats2.partition_count,
        "Partition counts should match"
    );
    assert_eq!(
        stats1.nodes_by_type, stats2.nodes_by_type,
        "Node type distributions should match"
    );

    // But they should NOT have the same identity (different pond IDs)
    let (id1, id2, matches) =
        SimpleReplicationTest::compare_pond_identity(&test1.source_pond, &test2.source_pond)
            .await?;
    assert_ne!(id1, id2, "Different ponds should have different IDs");
    assert!(!matches, "Different ponds should not match identity");

    Ok(())
}
