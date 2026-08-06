// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for pond control command and post-commit factory execution
//!
//! These tests verify:
//! 1. Post-commit factory sequencing works correctly
//! 2. Control table tracks all execution states
//! 3. Independent factory execution (failures don't block others)
//! 4. pond control command output for various scenarios
//! 5. Recovery query patterns work correctly

use anyhow::Result;
use cmd::commands::control::{ControlMode, control_command};
use cmd::commands::{init_command, mkdir_command, mknod_command};
use cmd::common::ShipContext;
use tempfile::TempDir;

/// Test setup with pond initialization
struct TestSetup {
    _temp_dir: TempDir,
    ship_context: ShipContext,
}

impl TestSetup {
    async fn new() -> Result<Self> {
        let temp_dir = TempDir::new()?;
        let pond_path = temp_dir.path().join("test_pond");

        // Create ship context for initialization
        let init_args = vec!["pond".to_string(), "init".to_string()];
        let ship_context = ShipContext::pond_only(Some(&pond_path), init_args);

        // Initialize pond
        init_command(&ship_context, "test-host").await?;

        Ok(TestSetup {
            _temp_dir: temp_dir,
            ship_context,
        })
    }

    /// Create a test factory configuration file
    async fn create_factory_config(&self, path: &str, content: &str) -> Result<()> {
        // Create file in temp directory (sibling to pond)
        let config_path = self._temp_dir.path().join(path);
        tokio::fs::write(&config_path, content).await?;
        Ok(())
    }

    /// Get absolute path to a factory config file
    fn config_path(&self, filename: &str) -> String {
        self._temp_dir
            .path()
            .join(filename)
            .to_string_lossy()
            .to_string()
    }

    /// Set factory mode for a factory
    async fn set_factory_mode(&self, factory_name: &str, mode: &str) -> Result<()> {
        let mut ship = self.ship_context.open_pond().await?;
        let control_table = ship.control_table_mut();
        control_table
            .set_factory_mode(factory_name, mode)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to set factory mode: {}", e))
    }

    /// Execute a write transaction to trigger post-commit
    async fn execute_write_transaction(&self, description: &str) -> Result<()> {
        let mut ship = self.ship_context.open_pond().await?;
        let args = vec!["test".to_string(), description.to_string()];
        let description_owned = description.to_string();

        ship.write_transaction(&steward::PondUserMetadata::new(args), async move |fs| {
            let desc = description_owned.clone();
            let root = fs.root().await?;

            // Create /test directory if it doesn't exist
            if !root.exists("/test").await {
                _ = root.create_dir_path("/test").await?;
            }

            // Create a simple file to make this a write transaction
            _ = tinyfs::async_helpers::convenience::create_file_path(
                &root,
                &format!("/test/{}.txt", desc),
                format!("Test data for {}", desc).as_bytes(),
            )
            .await?;

            Ok(())
        })
        .await?;

        Ok(())
    }

    /// Get the last transaction sequence number from control table
    async fn get_last_txn_seq(&self) -> Result<i64> {
        // Open ship to access cached control table
        let ship = self.ship_context.open_pond().await?;
        let control_table = ship.control_table();

        control_table
            .get_last_write_sequence()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get last txn seq: {}", e))
    }

    /// Query transaction records from control table.  Post-D2 the
    /// control table schema is lean: post-commit attributes live in
    /// `metadata_json` and the lifecycle kinds were renamed
    /// `post_commit_*` -> `post_push_*`.  This helper hides both
    /// changes from callers by:
    ///
    /// - Reading the lean columns + `metadata_json` from table `control`.
    /// - Decoding `factory_name`, `config_path`, `execution_seq`,
    ///   `error_message` from `metadata_json`.
    /// - Translating `post_push_*` record kinds back to the legacy
    ///   `post_commit_*` strings so existing test assertions still
    ///   read naturally.
    async fn query_transaction_records(
        &self,
        txn_seq: i64,
    ) -> Result<Vec<TransactionRecordSummary>> {
        use arrow::array::{Array, BooleanArray, Int64Array, StringArray};

        let ship = self.ship_context.open_pond().await?;
        let control_table = ship.control_table();
        let ctx = control_table.session_context();

        let sql = format!(
            r#"
            SELECT
                record_kind,
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

            let record_kinds = batch
                .column_by_name("record_kind")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let has_durations = batch
                .column_by_name("has_duration_ms")
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            let durations = batch
                .column_by_name("duration_ms")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let metadatas = batch
                .column_by_name("metadata_json")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            for i in 0..batch.num_rows() {
                // Translate post_push_* (lean schema) -> post_commit_*
                // (legacy label) so test assertions remain stable.
                let kind = match record_kinds.value(i) {
                    "post_push_pending" => "post_commit_pending".to_string(),
                    "post_push_started" => "post_commit_started".to_string(),
                    "post_push_completed" => "post_commit_completed".to_string(),
                    "post_push_failed" => "post_commit_failed".to_string(),
                    other => other.to_string(),
                };

                let meta_json = metadatas.value(i);
                let meta: serde_json::Value =
                    serde_json::from_str(meta_json).unwrap_or(serde_json::json!({}));

                let factory_name = meta
                    .get("factory_name")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                let config_path = meta
                    .get("config_path")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                let execution_seq = meta
                    .get("execution_seq")
                    .and_then(|v| v.as_i64())
                    .map(|n| n as i32);
                // The error message lives directly in `metadata_json`
                // for post_push_failed records, and under `reason` for
                // lifecycle `failed` records.
                let error_message = meta
                    .get("error_message")
                    .and_then(|v| v.as_str())
                    .map(String::from)
                    .or_else(|| {
                        meta.get("reason")
                            .and_then(|v| v.as_str())
                            .map(String::from)
                    });
                let duration_ms = if has_durations.value(i) {
                    Some(durations.value(i))
                } else {
                    None
                };

                records.push(TransactionRecordSummary {
                    record_type: kind,
                    factory_name,
                    config_path,
                    execution_seq,
                    error_message,
                    duration_ms,
                });
            }
        }

        Ok(records)
    }
}

#[derive(Debug, Clone)]
struct TransactionRecordSummary {
    record_type: String,
    factory_name: Option<String>,
    config_path: Option<String>,
    execution_seq: Option<i32>,
    error_message: Option<String>,
    duration_ms: Option<i64>,
}

// ============================================================================
// Test 1: Post-Commit Factory Sequencing
// ============================================================================

#[tokio::test]
async fn test_post_commit_single_factory_success() {
    let setup = TestSetup::new().await.expect("Failed to create test setup");

    // Create /system/run directory
    mkdir_command(&setup.ship_context, "/system/run", true)
        .await
        .expect("Failed to create /system/run");

    // Create a test factory config that will succeed
    setup
        .create_factory_config(
            "test-factory.yaml",
            r#"
message: "Test factory executed successfully"
repeat_count: 1
"#,
        )
        .await
        .expect("Failed to create config");

    // Set factory mode BEFORE creating factory node to prevent error logs
    setup
        .set_factory_mode("test-executor", "push")
        .await
        .expect("Failed to set factory mode");

    // Register the factory
    mknod_command(
        &setup.ship_context,
        "test-executor",
        "/system/run/10-test",
        &setup.config_path("test-factory.yaml"),
        false,
    )
    .await
    .expect("Failed to create factory node");

    // Execute a write transaction (triggers post-commit)
    setup
        .execute_write_transaction("trigger_post_commit")
        .await
        .expect("Failed to execute write transaction");

    // Get the transaction sequence
    let txn_seq = setup
        .get_last_txn_seq()
        .await
        .expect("Failed to get txn seq");

    // Query control table for this transaction
    let records = setup
        .query_transaction_records(txn_seq)
        .await
        .expect("Failed to query records");

    // Verify we have the expected records
    let record_types: Vec<String> = records.iter().map(|r| r.record_type.clone()).collect();

    assert!(
        record_types.contains(&"begin".to_string()),
        "Should have begin record"
    );
    assert!(
        record_types.contains(&"data_committed".to_string()),
        "Should have data_committed record"
    );
    assert!(
        record_types.contains(&"post_commit_pending".to_string()),
        "Should have post_commit_pending record"
    );
    assert!(
        record_types.contains(&"post_commit_completed".to_string()),
        "Should have post_commit_completed record"
    );

    // Verify factory details in pending record
    let pending_record = records
        .iter()
        .find(|r| r.record_type == "post_commit_pending")
        .expect("Should have pending record");

    assert_eq!(
        pending_record.factory_name.as_deref(),
        Some("test-executor")
    );
    assert_eq!(
        pending_record.config_path.as_deref(),
        Some("/system/run/10-test")
    );
    assert_eq!(pending_record.execution_seq, Some(1));

    // Verify completed record has duration
    let completed_record = records
        .iter()
        .find(|r| r.record_type == "post_commit_completed")
        .expect("Should have completed record");

    assert!(
        completed_record.duration_ms.is_some(),
        "Should have duration"
    );
    assert!(
        completed_record.duration_ms.unwrap() >= 0,
        "Duration should be non-negative"
    );
}

#[tokio::test]
async fn test_post_commit_multiple_factories_all_succeed() {
    let setup = TestSetup::new().await.expect("Failed to create test setup");

    // Create /system/run directory
    mkdir_command(&setup.ship_context, "/system/run", true)
        .await
        .expect("Failed to create /system/run");

    // Set factory mode early to prevent error logs
    setup
        .set_factory_mode("test-executor", "push")
        .await
        .expect("Failed to set factory mode");

    // Create three factory configs
    for i in 1..=3 {
        let config_filename = format!("test-factory-{}.yaml", i);
        setup
            .create_factory_config(
                &config_filename,
                &format!(
                    r#"
message: "Test factory {} executed"
repeat_count: 1
"#,
                    i
                ),
            )
            .await
            .expect("Failed to create config");

        mknod_command(
            &setup.ship_context,
            "test-executor",
            &format!("/system/run/{:02}-test-{}", i * 10, i),
            &setup.config_path(&config_filename),
            false,
        )
        .await
        .expect("Failed to create factory node");
    }

    // Execute a write transaction
    setup
        .execute_write_transaction("trigger_three_factories")
        .await
        .expect("Failed to execute write transaction");

    let txn_seq = setup
        .get_last_txn_seq()
        .await
        .expect("Failed to get txn seq");
    let records = setup
        .query_transaction_records(txn_seq)
        .await
        .expect("Failed to query records");

    // Should have 3 pending, 3 completed (started records were removed; the
    // factory's own Begin row marks execution start).
    let pending_count = records
        .iter()
        .filter(|r| r.record_type == "post_commit_pending")
        .count();
    let completed_count = records
        .iter()
        .filter(|r| r.record_type == "post_commit_completed")
        .count();

    assert_eq!(pending_count, 3, "Should have 3 pending records");
    assert_eq!(completed_count, 3, "Should have 3 completed records");

    // Verify execution order (execution_seq should be 1, 2, 3)
    let mut pending_records: Vec<_> = records
        .iter()
        .filter(|r| r.record_type == "post_commit_pending")
        .collect();
    pending_records.sort_by_key(|r| r.execution_seq);

    assert_eq!(pending_records[0].execution_seq, Some(1));
    assert_eq!(pending_records[1].execution_seq, Some(2));
    assert_eq!(pending_records[2].execution_seq, Some(3));

    // Verify factory names match the order
    assert!(
        pending_records[0]
            .config_path
            .as_deref()
            .unwrap()
            .contains("10-test-1")
    );
    assert!(
        pending_records[1]
            .config_path
            .as_deref()
            .unwrap()
            .contains("20-test-2")
    );
    assert!(
        pending_records[2]
            .config_path
            .as_deref()
            .unwrap()
            .contains("30-test-3")
    );
}

// ============================================================================
// Test 2: Independent Execution (Failure Isolation)
// ============================================================================

/// CRITICAL TEST: Verify independent factory execution with predictable failures
///
/// This test validates:
/// 1. Multiple factories (success, fail, success) execute independently
/// 2. One factory's failure doesn't block others
/// 3. Control table captures complete lifecycle for each factory
/// 4. Error messages, duration, and factory details are recorded correctly
/// 5. pond control command can display the failure history
#[tokio::test]
async fn test_post_commit_independent_execution_with_failure() {
    let setup = TestSetup::new().await.expect("Failed to create test setup");

    // Create /system/run directory
    mkdir_command(&setup.ship_context, "/system/run", true)
        .await
        .expect("Failed to create /system/run");

    // Set factory mode early to prevent error logs
    setup
        .set_factory_mode("test-executor", "push")
        .await
        .expect("Failed to set factory mode");

    // Factory 1: Success
    setup
        .create_factory_config(
            "test-success-1.yaml",
            r#"
message: "First factory succeeds"
repeat_count: 1
"#,
        )
        .await
        .expect("Failed to create config");

    mknod_command(
        &setup.ship_context,
        "test-executor",
        "/system/run/10-success",
        &setup.config_path("test-success-1.yaml"),
        false,
    )
    .await
    .expect("Failed to create factory node");

    // Factory 2: Intentional Failure (will always fail with predictable error)
    setup
        .create_factory_config(
            "test-fail.yaml",
            r#"
message: "Second factory intentionally fails"
repeat_count: 1
fail: true
"#,
        )
        .await
        .expect("Failed to create config");

    mknod_command(
        &setup.ship_context,
        "test-executor",
        "/system/run/20-fail",
        &setup.config_path("test-fail.yaml"),
        false,
    )
    .await
    .expect("Failed to create factory node");

    // Factory 3: Success (proves factory 2's failure didn't block execution)
    setup
        .create_factory_config(
            "test-success-2.yaml",
            r#"
message: "Third factory succeeds despite previous failure"
repeat_count: 1
"#,
        )
        .await
        .expect("Failed to create config");

    mknod_command(
        &setup.ship_context,
        "test-executor",
        "/system/run/30-success",
        &setup.config_path("test-success-2.yaml"),
        false,
    )
    .await
    .expect("Failed to create factory node");

    // Execute write transaction to trigger post-commit
    setup
        .execute_write_transaction("test_failure_isolation")
        .await
        .expect("Failed to execute write transaction");

    let txn_seq = setup
        .get_last_txn_seq()
        .await
        .expect("Failed to get txn seq");
    let records = setup
        .query_transaction_records(txn_seq)
        .await
        .expect("Failed to query records");

    println!(
        "\n=== Independent Execution Test: Transaction {} ===",
        txn_seq
    );
    println!("Total records: {}", records.len());

    // Print all records for debugging
    for record in &records {
        println!(
            "  {:20} exec_seq={:?} factory={:?} error={:?}",
            record.record_type,
            record.execution_seq,
            record.factory_name,
            record.error_message.as_ref().map(|s| &s[..s.len().min(50)])
        );
    }

    // ========================================================================
    // VERIFY RECORD COUNTS (Explicit assertions as requested)
    // ========================================================================

    let pending_count = records
        .iter()
        .filter(|r| r.record_type == "post_commit_pending")
        .count();
    let completed_count = records
        .iter()
        .filter(|r| r.record_type == "post_commit_completed")
        .count();
    let failed_count = records
        .iter()
        .filter(|r| r.record_type == "post_commit_failed")
        .count();

    assert_eq!(
        pending_count, 3,
        "Should have exactly 3 pending records (one per factory)"
    );
    assert_eq!(
        completed_count, 2,
        "Should have exactly 2 completed records (factories 1 and 3 succeeded)"
    );
    assert_eq!(
        failed_count, 1,
        "Should have exactly 1 failed record (factory 2 failed)"
    );

    println!(
        "[OK] Record counts: {} pending, {} completed, {} failed",
        pending_count, completed_count, failed_count
    );

    // ========================================================================
    // VERIFY PENDING RECORDS (Factory details captured)
    // ========================================================================

    let pending_records: Vec<_> = records
        .iter()
        .filter(|r| r.record_type == "post_commit_pending")
        .collect();

    // All pending records should have factory name
    for pending in &pending_records {
        assert_eq!(
            pending.factory_name.as_deref(),
            Some("test-executor"),
            "Pending record should have factory_name"
        );
        assert!(
            pending.config_path.is_some(),
            "Pending record should have config_path"
        );
        assert!(
            pending.execution_seq.is_some(),
            "Pending record should have execution_seq"
        );
    }

    // Verify execution sequence is 1, 2, 3
    let mut exec_seqs: Vec<_> = pending_records
        .iter()
        .filter_map(|r| r.execution_seq)
        .collect();
    exec_seqs.sort();
    assert_eq!(
        exec_seqs,
        vec![1, 2, 3],
        "Execution sequences should be 1, 2, 3"
    );

    println!("[OK] Pending records have correct factory details and sequencing");

    // ========================================================================
    // VERIFY FAILED RECORD (Error message and duration captured)
    // ========================================================================

    let failed_record = records
        .iter()
        .find(|r| r.record_type == "post_commit_failed")
        .expect("Should have exactly one failed record");

    // Verify error message is captured
    assert!(
        failed_record.error_message.is_some(),
        "Failed record must have error_message"
    );
    let error_msg = failed_record.error_message.as_ref().unwrap();
    assert!(
        error_msg.contains("intentionally failed"),
        "Error message should contain 'intentionally failed', got: {}",
        error_msg
    );

    // Verify duration is captured (even for failures)
    assert!(
        failed_record.duration_ms.is_some(),
        "Failed record must have duration_ms"
    );
    assert!(
        failed_record.duration_ms.unwrap() >= 0,
        "Duration should be non-negative"
    );

    // Verify execution_seq identifies which factory failed
    assert_eq!(
        failed_record.execution_seq,
        Some(2),
        "Failed record should be for execution_seq=2 (middle factory)"
    );

    println!("[OK] Failed record has error_message and duration_ms");
    println!("  Error: {}", error_msg);
    println!("  Duration: {} ms", failed_record.duration_ms.unwrap());

    // ========================================================================
    // VERIFY COMPLETED RECORDS (Success factories have duration)
    // ========================================================================

    let completed_records: Vec<_> = records
        .iter()
        .filter(|r| r.record_type == "post_commit_completed")
        .collect();

    for completed in &completed_records {
        assert!(
            completed.duration_ms.is_some(),
            "Completed record should have duration_ms"
        );
        assert!(
            completed.duration_ms.unwrap() >= 0,
            "Duration should be non-negative"
        );
        assert!(
            completed.error_message.is_none(),
            "Completed record should not have error_message"
        );
    }

    // Verify it's factories 1 and 3 that succeeded
    let mut completed_seqs: Vec<_> = completed_records
        .iter()
        .filter_map(|r| r.execution_seq)
        .collect();
    completed_seqs.sort();
    assert_eq!(
        completed_seqs,
        vec![1, 3],
        "Factories 1 and 3 should have completed successfully"
    );

    println!("[OK] Completed records (exec_seq 1, 3) have duration_ms, no errors");

    // ========================================================================
    // VERIFY POND CONTROL COMMAND WORKS
    // ========================================================================

    // Test detail mode for this transaction
    println!("\n=== Testing pond control detail mode ===");
    let detail_mode = ControlMode::Detail { txn_seq };
    control_command(&setup.ship_context, detail_mode)
        .await
        .expect("pond control detail should not fail");

    // Test incomplete mode (should show the failed operation if any exist)
    println!("\n=== Testing pond control incomplete mode ===");
    let incomplete_mode = ControlMode::Incomplete;
    control_command(&setup.ship_context, incomplete_mode)
        .await
        .expect("pond control incomplete should not fail");

    println!("\n[OK] Independent execution test PASSED");
    println!("   - 3 factories executed independently");
    println!("   - 1 failure did not block other factories");
    println!("   - Control table captured all lifecycle events");
    println!("   - Error messages and durations recorded correctly");
    println!("   - pond control command displays failure history");
}

// ============================================================================
// Test 3: Control Table Direct Queries (Not command output)
// ============================================================================

#[tokio::test]
async fn test_control_command_runs_without_panic() {
    let setup = TestSetup::new().await.expect("Failed to create test setup");

    // Execute a transaction
    setup
        .execute_write_transaction("test_control_command")
        .await
        .expect("Failed to execute transaction");

    // Verify all three modes run without panicking
    // We can't easily verify output, but we can verify no crash
    let mode = ControlMode::Recent { limit: 5 };
    control_command(&setup.ship_context, mode)
        .await
        .expect("Recent mode should not panic");

    let txn_seq = setup
        .get_last_txn_seq()
        .await
        .expect("Failed to get txn seq");
    let mode = ControlMode::Detail { txn_seq };
    control_command(&setup.ship_context, mode)
        .await
        .expect("Detail mode should not panic");

    let mode = ControlMode::Incomplete;
    control_command(&setup.ship_context, mode)
        .await
        .expect("Incomplete mode should not panic");
}

// ============================================================================
// Test 4: Recovery Query Patterns
// ============================================================================

#[tokio::test]
async fn test_query_pending_never_started() {
    let setup = TestSetup::new().await.expect("Failed to create test setup");

    // This test would require simulating a crash after pending records created
    // but before started records
    // For unit tests, we can verify the query structure works

    // Open ship to access cached control table
    let ship = setup
        .ship_context
        .open_pond()
        .await
        .expect("Failed to open pond");
    let control_table = ship.control_table();

    // Use control table's SessionContext (following tlogfs pattern)
    let ctx = control_table.session_context();

    // Query for pending tasks that were never started.  Post-D2 lean
    // schema: post-commit attributes live in `metadata_json`; the
    // parent transaction's seq is in `parent_seq` (with
    // `has_parent_seq=true`); execution_seq is JSON-extracted.
    let sql = r#"
        SELECT
            parent_seq AS parent_txn_seq,
            json_get_str(metadata_json, 'execution_seq') AS execution_seq,
            json_get_str(metadata_json, 'factory_name')  AS factory_name,
            json_get_str(metadata_json, 'config_path')   AS config_path
        FROM control p
        WHERE p.record_kind = 'post_push_pending'
          AND NOT EXISTS (
              SELECT 1 FROM control s
              WHERE s.record_kind = 'post_push_started'
                AND s.parent_seq = p.parent_seq
                AND json_get_str(s.metadata_json, 'execution_seq')
                  = json_get_str(p.metadata_json, 'execution_seq')
          )
    "#;

    let df = ctx.sql(sql).await.expect("Query should succeed");
    let batches = df.collect().await.expect("Collect should succeed");

    // In a real scenario with incomplete execution, we'd verify rows returned
    // For now, just verify the query executes
    println!("Pending never started: {} batches", batches.len());
}

#[tokio::test]
async fn test_query_started_never_completed() {
    let setup = TestSetup::new().await.expect("Failed to create test setup");

    // Open ship to access cached control table
    let ship = setup
        .ship_context
        .open_pond()
        .await
        .expect("Failed to open pond");
    let control_table = ship.control_table();

    // Use control table's SessionContext (following tlogfs pattern)
    let ctx = control_table.session_context();

    // Query for tasks that started but never completed/failed.
    // Same lean-schema considerations as above.
    let sql = r#"
        SELECT
            p.parent_seq AS parent_txn_seq,
            json_get_str(p.metadata_json, 'execution_seq') AS execution_seq,
            json_get_str(p.metadata_json, 'factory_name')  AS factory_name,
            json_get_str(p.metadata_json, 'config_path')   AS config_path
        FROM control p
        WHERE p.record_kind = 'post_push_pending'
          AND EXISTS (
              SELECT 1 FROM control s
              WHERE s.record_kind = 'post_push_started'
                AND s.parent_seq = p.parent_seq
                AND json_get_str(s.metadata_json, 'execution_seq')
                  = json_get_str(p.metadata_json, 'execution_seq')
          )
          AND NOT EXISTS (
              SELECT 1 FROM control c
              WHERE c.record_kind IN ('post_push_completed', 'post_push_failed')
                AND c.parent_seq = p.parent_seq
                AND json_get_str(c.metadata_json, 'execution_seq')
                  = json_get_str(p.metadata_json, 'execution_seq')
          )
    "#;

    let df = ctx.sql(sql).await.expect("Query should succeed");
    let batches = df.collect().await.expect("Collect should succeed");

    println!("Started never completed: {} batches", batches.len());
}

#[tokio::test]
async fn test_parent_txn_seq_execution_seq_identity() {
    let setup = TestSetup::new().await.expect("Failed to create test setup");

    // Create multiple post-commit factories
    mkdir_command(&setup.ship_context, "/system/run", true)
        .await
        .expect("Failed to create /system/run");

    for i in 1..=3 {
        let config_filename = format!("identity-test-{}.yaml", i);
        setup
            .create_factory_config(
                &config_filename,
                &format!(
                    r#"
message: "Identity test factory {}"
repeat_count: 1
"#,
                    i
                ),
            )
            .await
            .expect("Failed to create config");

        mknod_command(
            &setup.ship_context,
            "test-executor",
            &format!("/system/run/{:02}-identity-{}", i * 10, i),
            &setup.config_path(&config_filename),
            false,
        )
        .await
        .expect("Failed to create factory node");
    }

    // Set factory mode to "push"
    setup
        .set_factory_mode("test-executor", "push")
        .await
        .expect("Failed to set factory mode");

    setup
        .execute_write_transaction("identity_test")
        .await
        .expect("Failed to execute transaction");

    let txn_seq = setup
        .get_last_txn_seq()
        .await
        .expect("Failed to get txn seq");
    let records = setup
        .query_transaction_records(txn_seq)
        .await
        .expect("Failed to query records");

    // Verify (parent_txn_seq, execution_seq) uniquely identifies each task
    let post_commit_records: Vec<_> = records
        .iter()
        .filter(|r| r.record_type.starts_with("post_commit"))
        .collect();

    // Group by execution_seq
    for exec_seq in 1..=3 {
        let task_records: Vec<_> = post_commit_records
            .iter()
            .filter(|r| r.execution_seq == Some(exec_seq))
            .collect();

        // Should have pending and a completion for each execution_seq.
        // Started records were removed; the factory's own Begin row marks
        // execution start.
        let has_pending = task_records
            .iter()
            .any(|r| r.record_type == "post_commit_pending");
        let has_completion = task_records.iter().any(|r| {
            r.record_type == "post_commit_completed" || r.record_type == "post_commit_failed"
        });

        assert!(has_pending, "Task {} should have pending record", exec_seq);
        assert!(
            has_completion,
            "Task {} should have completion record",
            exec_seq
        );

        // Verify factory name is consistent across lifecycle
        let factory_names: Vec<_> = task_records
            .iter()
            .filter_map(|r| r.factory_name.as_ref())
            .collect();

        if !factory_names.is_empty() {
            let first_name = factory_names[0];
            assert!(
                factory_names.iter().all(|name| *name == first_name),
                "Factory name should be consistent across lifecycle for task {}",
                exec_seq
            );
        }
    }
}

/// CRITICAL TEST: Verify post-commit factories can see data committed at txn_seq+1
///
/// This test validates the core assumption that post-commit factories execute
/// with a read-only transaction at txn_seq+1 and can see the data that was
/// just committed in the parent transaction.
#[tokio::test]
async fn test_version_visibility_post_commit_sees_committed_data() {
    let setup = TestSetup::new().await.expect("Failed to create test setup");

    // Create /system/run and /data directories
    mkdir_command(&setup.ship_context, "/system/run", true)
        .await
        .expect("Failed to create /system/run");

    mkdir_command(&setup.ship_context, "/data", true)
        .await
        .expect("Failed to create data directory");

    // Define the test data that we'll write and verify
    let test_file_path = "/data/visibility-test.txt";
    let test_content = "VISIBILITY_TEST_DATA_v1_committed_at_txn";

    // Step 1: Write data in a transaction
    let mut ship = setup
        .ship_context
        .open_pond()
        .await
        .expect("Failed to open pond");
    let args = vec!["write".to_string(), "visibility_test".to_string()];
    let content_clone = test_content.to_string();
    let path_clone = test_file_path.to_string();

    ship.write_transaction(&steward::PondUserMetadata::new(args), async move |fs| {
        let content = content_clone.clone();
        let path = path_clone.clone();
        let root = fs.root().await?;

        // Write the test file with known content
        _ = tinyfs::async_helpers::convenience::create_file_path(&root, &path, content.as_bytes())
            .await?;

        Ok(())
    })
    .await
    .expect("Failed to write test data");

    // Step 2: Create a post-commit factory that will read the file
    // Set factory mode BEFORE mknod to prevent error logs
    setup
        .set_factory_mode("test-executor", "push")
        .await
        .expect("Failed to set factory mode");

    setup
        .create_factory_config(
            "visibility-reader.yaml",
            &format!(
                r#"
message: "Reading committed data to verify version visibility"
repeat_count: 1
file_to_read: "{}"
expected_content: "{}"
"#,
                test_file_path, test_content
            ),
        )
        .await
        .expect("Failed to create config");

    mknod_command(
        &setup.ship_context,
        "test-executor",
        "/system/run/10-visibility-reader",
        &setup.config_path("visibility-reader.yaml"),
        false,
    )
    .await
    .expect("Failed to create factory node");

    // Step 3: Execute a transaction to trigger post-commit (doesn't need to write more data)
    setup
        .execute_write_transaction("trigger_visibility_test")
        .await
        .expect("Failed to execute trigger transaction");

    // Step 4: Verify the factory executed successfully
    let txn_seq = setup
        .get_last_txn_seq()
        .await
        .expect("Failed to get txn seq");
    let records = setup
        .query_transaction_records(txn_seq)
        .await
        .expect("Failed to query records");

    // Debug: Print all records
    println!("Transaction {} has {} records:", txn_seq, records.len());
    for record in &records {
        println!(
            "  - {:?}: factory={:?}, exec_seq={:?}, error={:?}",
            record.record_type, record.factory_name, record.execution_seq, record.error_message
        );
    }

    // Find the post-commit completed record
    let completed_record = records
        .iter()
        .find(|r| r.record_type == "post_commit_completed")
        .expect("Should have post_commit_completed record");

    // Verify it succeeded (no error message)
    assert!(
        completed_record.error_message.is_none(),
        "Post-commit factory should have succeeded without errors. Error: {:?}",
        completed_record.error_message
    );

    // Verify execution_seq matches
    assert_eq!(completed_record.execution_seq, Some(1));

    // Verify the pending record has the correct factory details
    let pending_record = records
        .iter()
        .find(|r| r.record_type == "post_commit_pending")
        .expect("Should have post_commit_pending record");

    assert_eq!(
        pending_record.factory_name.as_deref(),
        Some("test-executor")
    );
    assert_eq!(
        pending_record.config_path.as_deref(),
        Some("/system/run/10-visibility-reader")
    );

    // If we got here, the factory successfully read and verified the content
    // This proves that post-commit factories see data committed at txn_seq+1
    println!(
        "[OK] Version visibility test PASSED: Post-commit factory successfully read data committed in parent transaction"
    );
}

// ============================================================================
// Test 5: Transaction Completion Records (Basic Transactions)
// ============================================================================

/// CRITICAL TEST: Verify that basic transactions (init, mkdir, write) record completion
///
/// This test addresses the bug where transactions show as INCOMPLETE in pond control.
/// Every transaction should have:
/// 1. "begin" record when transaction starts
/// 2. "data_committed" record when write transaction commits (or "completed" for read-only)
#[tokio::test]
async fn test_transaction_completion_records_written() {
    let setup = TestSetup::new().await.expect("Failed to create test setup");

    // Transaction 1: init (already completed during setup)
    let init_seq = 1;
    let init_records = setup
        .query_transaction_records(init_seq)
        .await
        .expect("Failed to query init transaction");

    println!("\n=== Transaction 1 (init) ===");
    for record in &init_records {
        println!(
            "  {:20} error={:?}",
            record.record_type, record.error_message
        );
    }

    // Verify init has both begin and data_committed
    let has_begin = init_records.iter().any(|r| r.record_type == "begin");
    let has_committed = init_records
        .iter()
        .any(|r| r.record_type == "data_committed");

    assert!(has_begin, "Init transaction should have 'begin' record");
    assert!(
        has_committed,
        "Init transaction should have 'data_committed' record"
    );

    // Transaction 2: mkdir (write transaction)
    mkdir_command(&setup.ship_context, "/test", false)
        .await
        .expect("Failed to create /test directory");

    let mkdir_seq = setup
        .get_last_txn_seq()
        .await
        .expect("Failed to get txn seq");
    let mkdir_records = setup
        .query_transaction_records(mkdir_seq)
        .await
        .expect("Failed to query mkdir transaction");

    println!("\n=== Transaction {} (mkdir) ===", mkdir_seq);
    for record in &mkdir_records {
        println!(
            "  {:20} error={:?}",
            record.record_type, record.error_message
        );
    }

    // Verify mkdir has both begin and data_committed
    let has_begin = mkdir_records.iter().any(|r| r.record_type == "begin");
    let has_committed = mkdir_records
        .iter()
        .any(|r| r.record_type == "data_committed");

    assert!(has_begin, "mkdir transaction should have 'begin' record");
    assert!(
        has_committed,
        "mkdir transaction should have 'data_committed' record"
    );

    // Transaction 3: Regular write transaction
    setup
        .execute_write_transaction("test_completion")
        .await
        .expect("Failed to execute write transaction");

    let write_seq = setup
        .get_last_txn_seq()
        .await
        .expect("Failed to get txn seq");
    let write_records = setup
        .query_transaction_records(write_seq)
        .await
        .expect("Failed to query write transaction");

    println!("\n=== Transaction {} (write) ===", write_seq);
    for record in &write_records {
        println!(
            "  {:20} error={:?}",
            record.record_type, record.error_message
        );
    }

    // Verify write has both begin and data_committed
    let has_begin = write_records.iter().any(|r| r.record_type == "begin");
    let has_committed = write_records
        .iter()
        .any(|r| r.record_type == "data_committed");

    assert!(has_begin, "write transaction should have 'begin' record");
    assert!(
        has_committed,
        "write transaction should have 'data_committed' record"
    );

    println!("\n[OK] Transaction completion records test PASSED");
    println!("   - All transactions have 'begin' records");
    println!("   - Write transactions have 'data_committed' records");

    // Verify pond control command shows these as completed, not incomplete
    println!("\n=== Verifying pond control shows transactions as completed ===");
    control_command(&setup.ship_context, ControlMode::Recent { limit: 10 })
        .await
        .expect("control command should succeed");
}

/// Test that replica pond preserves transaction sequence numbers from source
///
/// This test verifies the fix for a bug where replica ponds had an off-by-one
/// error in transaction sequences. The bug was caused by calling create_pond("test-host")
/// during restoration, which recorded an initial "pond init" transaction, then
/// bundle restoration added the source's "pond init" as a second transaction.
///
/// The fix uses create_pond_for_restoration() which creates infrastructure
/// without recording transaction #1, letting bundle restoration replay ALL
/// transactions with their original sequence numbers.
#[tokio::test]
async fn test_replica_preserves_transaction_sequences() {
    println!("\n=== Testing replica transaction sequence preservation ===");

    let temp_dir = TempDir::new().expect("Failed to create temp directory");

    // Part 1: Verify that create_pond("test-host") records transaction #1
    println!("\n--- Part 1: Normal pond creation ---");
    let normal_path = temp_dir.path().join("normal_pond");
    let normal_context = ShipContext::pond_only(
        Some(normal_path.clone()),
        vec!["pond".to_string(), "init".to_string()],
    );

    let normal_ship = normal_context
        .create_pond("test-host")
        .await
        .expect("Failed to create normal pond");

    let normal_last_seq = normal_ship
        .control_table()
        .get_last_write_sequence()
        .await
        .expect("Failed to get normal pond sequence");

    println!(
        "Normal pond after create_pond(): last_txn_seq = {}",
        normal_last_seq
    );
    assert_eq!(
        normal_last_seq, 1,
        "Normal pond should have transaction #1 from create_pond()"
    );

    // Verify the transaction is recorded in control table
    let normal_txns = get_transaction_cli_args(&normal_context, 10)
        .await
        .expect("Failed to get normal pond transactions");

    println!("Found {} transactions:", normal_txns.len());
    for (seq, args) in &normal_txns {
        println!("  txn_seq={}: {:?}", seq, args);
    }

    assert_eq!(
        normal_txns.len(),
        1,
        "Normal pond should have 1 transaction"
    );
    assert_eq!(normal_txns[0].0, 1, "First txn should be seq=1");
    // Post-D2: cli_args are no longer persisted in the control table;
    // the helper returns an empty Vec for each transaction.  The
    // original command lives in data Delta commit metadata
    // (`pond_txn`) for callers that need it.
    assert!(
        normal_txns[0].1.is_empty(),
        "Post-D2 control table does not persist cli_args; got {:?}",
        normal_txns[0].1
    );

    drop(normal_ship);

    // Part 2: Verify that create_pond_for_restoration() does NOT record transaction #1
    println!("\n--- Part 2: Restoration-ready pond creation ---");
    let replica_path = temp_dir.path().join("replica_pond");
    let replica_context = ShipContext::pond_only(
        Some(replica_path.clone()),
        vec!["pond".to_string(), "init".to_string()],
    );

    // Create replica preserving the pond identity (restoration means cloning metadata)
    // For this test, create a fresh metadata since we're just testing the mechanism
    let replica_metadata = steward::PondMetadata::default();
    let replica_ship = replica_context
        .create_pond_for_restoration(replica_metadata)
        .await
        .expect("Failed to create replica infrastructure");

    let replica_last_seq = replica_ship
        .control_table()
        .get_last_write_sequence()
        .await
        .expect("Failed to get replica pond sequence");

    println!(
        "Replica pond after create_pond_for_restoration(): last_txn_seq = {}",
        replica_last_seq
    );
    assert_eq!(
        replica_last_seq, 0,
        "Replica pond should have NO transactions (last_txn_seq=0) from create_pond_for_restoration()"
    );

    // Verify NO transactions are recorded in control table (only metadata partition exists)
    let replica_txns = get_transaction_cli_args(&replica_context, 10)
        .await
        .expect("Failed to get replica pond transactions");

    assert_eq!(
        replica_txns.len(),
        0,
        "Replica pond should have 0 transactions initially"
    );

    drop(replica_ship);

    // Part 3: Verify that restoring "pond init" bundle creates transaction #1 (not #2)
    println!("\n--- Part 3: Simulating first bundle restoration ---");

    // Reopen replica to simulate bundle restoration
    let mut replica_ship = replica_context
        .open_pond()
        .await
        .expect("Failed to reopen replica");

    // Simulate restoring the first bundle with "pond init" command
    // This should create transaction #1, not #2
    let tx = replica_ship
        .begin_write(&steward::PondUserMetadata::new(vec![
            "pond".to_string(),
            "init".to_string(),
        ]))
        .await
        .expect("Failed to begin transaction");

    tx.as_pond()
        .expect("test requires a pond transaction")
        .initialize_root_directory()
        .await
        .map_err(steward::StewardError::DataInit)
        .expect("Failed to initialize root directory");

    _ = tx.commit().await.expect("Failed to commit");

    let after_restore_seq = replica_ship
        .control_table()
        .get_last_write_sequence()
        .await
        .expect("Failed to get sequence after restoration");

    println!(
        "Replica pond after restoring first bundle: last_txn_seq = {}",
        after_restore_seq
    );
    assert_eq!(
        after_restore_seq, 1,
        "After restoring first bundle, replica should be at txn_seq=1 (not 2)"
    );

    // Verify the restored transaction has correct sequence
    let after_restore_txns = get_transaction_cli_args(&replica_context, 10)
        .await
        .expect("Failed to get transactions after restoration");

    assert_eq!(
        after_restore_txns.len(),
        1,
        "Replica should have 1 transaction after first bundle"
    );
    assert_eq!(
        after_restore_txns[0].0, 1,
        "Restored transaction should be seq=1 (THE BUG FIX: not seq=2)"
    );
    // Post-D2 cli_args are not persisted; see comment above.
    assert!(
        after_restore_txns[0].1.is_empty(),
        "Post-D2 control table does not persist cli_args; got {:?}",
        after_restore_txns[0].1
    );

    drop(replica_ship);

    println!("\n[OK] Replica transaction sequence preservation test PASSED");
    println!("   - create_pond() creates txn_seq=1 (normal initialization)");
    println!("   - create_pond_for_restoration() creates txn_seq=0 (no initial transaction)");
    println!("   - First bundle restoration creates txn_seq=1 (not txn_seq=2 - bug fixed!)");
}

/// Helper to get transaction cli_args from a pond
/// Return (txn_seq, cli_args) for each user transaction in the
/// control table.  Post-D2 the lean schema does not persist cli_args
/// at all; callers receive an empty `Vec<String>` for each tx (the
/// original command lives in data Delta commit metadata).  Kept as a
/// helper so the test reads as a single source of truth for "list
/// transactions visible in the control table".
async fn get_transaction_cli_args(
    ship_context: &ShipContext,
    limit: usize,
) -> Result<Vec<(i64, Vec<String>)>> {
    let mut ship = ship_context.open_pond().await?;
    let control_table = ship.control_table_mut();

    let ctx = control_table.session_context();
    let sql = format!(
        r#"
        SELECT DISTINCT txn_seq
        FROM control
        WHERE record_kind = 'begin'
          AND has_parent_seq = false
          AND txn_seq > 0
        ORDER BY txn_seq ASC
        LIMIT {limit}
        "#,
        limit = limit
    );

    let df = ctx.sql(&sql).await?;
    let batches = df.collect().await?;

    let mut records = Vec::new();
    for batch in batches {
        use arrow::array::{Array, Int64Array};

        let txn_seqs = batch
            .column_by_name("txn_seq")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            records.push((txn_seqs.value(i), Vec::new()));
        }
    }

    Ok(records)
}

/// Count control-table rows matching a SQL `WHERE` predicate fragment.
async fn count_control_rows(ship_context: &ShipContext, where_clause: &str) -> Result<i64> {
    use arrow::array::{Array, Int64Array};

    let ship = ship_context.open_pond().await?;
    let ctx = ship.control_table().session_context();
    let sql = format!("SELECT COUNT(*) AS n FROM control WHERE {}", where_clause);
    let batches = ctx.sql(&sql).await?.collect().await?;
    let mut n = 0i64;
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        if !arr.is_null(0) {
            n = arr.value(0);
        }
    }
    Ok(n)
}

/// `pond control prune` deletes replicated lifecycle history at or below
/// the horizon while preserving `Setting` rows and the newest `keep_txns`
/// transactions.  Uses the no-remote retention path (`allow_no_remote`).
#[tokio::test]
async fn test_control_prune_shrinks_history_preserving_settings() {
    let setup = TestSetup::new().await.expect("setup");

    // Generate several committed write transactions.
    for i in 0..6 {
        setup
            .execute_write_transaction(&format!("prune_write_{}", i))
            .await
            .expect("write txn");
    }

    // Record a Setting row that must survive the prune.
    {
        let mut ship = setup.ship_context.open_pond().await.expect("open");
        ship.control_table_mut()
            .set_setting("prune_marker", "keepme")
            .await
            .expect("set setting");
    }

    let last_committed = setup.get_last_txn_seq().await.expect("last seq");
    let keep_txns = 2i64;
    let horizon = last_committed - keep_txns;
    assert!(horizon >= 1, "need history below horizon for the test");

    let below_before = count_control_rows(
        &setup.ship_context,
        &format!("record_kind != 'setting' AND txn_seq <= {}", horizon),
    )
    .await
    .expect("count below");
    assert!(below_before > 0, "expected prunable rows below horizon");

    let settings_before = count_control_rows(&setup.ship_context, "record_kind = 'setting'")
        .await
        .expect("count settings");
    assert!(settings_before > 0, "expected at least one setting row");

    // Prune (no remote attached -> retention-only path).
    control_command(
        &setup.ship_context,
        ControlMode::Prune {
            keep_txns,
            dry_run: false,
            allow_no_remote: true,
        },
    )
    .await
    .expect("prune");

    // All non-setting rows at/below the horizon are gone.
    let below_after = count_control_rows(
        &setup.ship_context,
        &format!("record_kind != 'setting' AND txn_seq <= {}", horizon),
    )
    .await
    .expect("count below after");
    assert_eq!(below_after, 0, "history at/below horizon should be deleted");

    // Setting rows are untouched.
    let settings_after = count_control_rows(&setup.ship_context, "record_kind = 'setting'")
        .await
        .expect("count settings after");
    assert_eq!(
        settings_after, settings_before,
        "setting rows must be preserved"
    );

    // The newest transactions survive, so MAX(txn_seq) is unchanged.
    let last_after = setup.get_last_txn_seq().await.expect("last seq after");
    assert_eq!(
        last_after, last_committed,
        "newest committed seq must be preserved"
    );

    // Rows above the horizon survive.
    let above_after = count_control_rows(
        &setup.ship_context,
        &format!("record_kind != 'setting' AND txn_seq > {}", horizon),
    )
    .await
    .expect("count above after");
    assert!(above_after > 0, "rows above horizon must survive");
}

/// Without a push remote and without `--allow-no-remote`, prune refuses.
#[tokio::test]
async fn test_control_prune_refuses_without_remote() {
    let setup = TestSetup::new().await.expect("setup");
    setup
        .execute_write_transaction("prune_norem")
        .await
        .expect("write txn");

    let result = control_command(
        &setup.ship_context,
        ControlMode::Prune {
            keep_txns: 0,
            dry_run: false,
            allow_no_remote: false,
        },
    )
    .await;
    assert!(
        result.is_err(),
        "prune should refuse without a push remote unless --allow-no-remote"
    );
}

/// `pond maintain --prune` deletes replicated history in the SAME pass as
/// checkpoint/vacuum (no separate ship open), preserving Setting rows and
/// the newest `keep_txns` transactions.
#[tokio::test]
async fn test_maintain_prune_shrinks_in_one_pass() {
    use cmd::commands::maintain_command;

    let setup = TestSetup::new().await.expect("setup");
    for i in 0..6 {
        setup
            .execute_write_transaction(&format!("mprune_{}", i))
            .await
            .expect("write txn");
    }
    {
        let mut ship = setup.ship_context.open_pond().await.expect("open");
        ship.control_table_mut()
            .set_setting("mprune_marker", "keepme")
            .await
            .expect("set setting");
    }

    let last_committed = setup.get_last_txn_seq().await.expect("last seq");
    let keep_txns = 2i64;
    let horizon = last_committed - keep_txns;
    assert!(horizon >= 1);

    let below_before = count_control_rows(
        &setup.ship_context,
        &format!("record_kind != 'setting' AND txn_seq <= {}", horizon),
    )
    .await
    .expect("count below");
    assert!(below_before > 0);

    // compact=false, collapse=0, prune=true, keep_txns, allow_no_remote=true.
    maintain_command(&setup.ship_context, false, 0, true, keep_txns, true, false)
        .await
        .expect("maintain --prune");

    let below_after = count_control_rows(
        &setup.ship_context,
        &format!("record_kind != 'setting' AND txn_seq <= {}", horizon),
    )
    .await
    .expect("count below after");
    assert_eq!(below_after, 0, "history at/below horizon should be deleted");

    let settings_after = count_control_rows(&setup.ship_context, "record_kind = 'setting'")
        .await
        .expect("count settings after");
    assert!(settings_after > 0, "setting rows must be preserved");

    let last_after = setup.get_last_txn_seq().await.expect("last seq after");
    assert_eq!(last_after, last_committed, "newest committed seq preserved");
}
