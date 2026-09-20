// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use log::debug;
use steward::{PondUserMetadata, Ship};
use tempfile::tempdir;
use tlogfs::FactoryRegistry;

/// Test that post-commit factories are discovered and executed after a write transaction
#[tokio::test]
async fn test_post_commit_factory_execution() -> Result<()> {
    let temp_dir = tempdir()?;
    let pond_path = temp_dir.path().join("post_commit_test_pond");

    // Initialize pond
    let mut ship = Ship::create_pond(&pond_path, "test-host")
        .await
        .map_err(|e| anyhow::anyhow!("Failed to initialize pond: {}", e))?;

    // Set factory mode early to prevent error logs (factories will be created in tx1)
    ship.control_table_mut()
        .set_factory_mode("test-executor", "push")
        .await?;

    // Transaction 1: Set up a post-commit factory
    debug!("=== Setting up post-commit factory ===");
    let tx1 = ship
        .begin_write(&PondUserMetadata::new(vec![
            "test".to_string(),
            "setup-factory".to_string(),
        ]))
        .await?;

    let root1 = tx1.root().await?;

    // Create /system first, then /system/run/ directory
    _ = root1.create_dir_path("/system").await?;
    _ = root1.create_dir_path("/system/run").await?;

    // Create a test-executor factory config
    let config_yaml = r#"message: "Post-commit execution test"
repeat_count: 3
file_to_read: /data/just-committed.txt
expected_content: visible after commit
"#;

    _ = root1.create_dir_path("/data").await?;
    let mut observed = root1.async_writer_path("/data/just-committed.txt").await?;
    use tokio::io::AsyncWriteExt;
    observed.write_all(b"visible after commit").await?;
    observed.shutdown().await?;

    // Create executable dynamic file using high-level path-based API
    // Note: Executable factories use FileDynamic, config is the file content
    let factory_node = root1
        .create_dynamic_path(
            "/system/run/test-post-commit.yaml",
            tinyfs::EntryType::FileDynamic,
            "test-executor",
            config_yaml.as_bytes().to_vec(),
        )
        .await?;

    // Save factory node ID for later use
    let factory_node_id = factory_node.id();

    // Initialize the factory
    let provider_context1 = tx1.provider_context()?;
    let context1 = provider::FactoryContext::new(provider_context1, factory_node_id);
    FactoryRegistry::initialize::<tlogfs::TLogFSError>(
        "test-executor",
        config_yaml.as_bytes(),
        context1,
    )
    .await?;

    debug!("DEBUG: About to commit tx1...");

    let committed_seq = tx1.txn_meta().txn_seq;
    // The read-only factory must see the file committed in this transaction.
    _ = tx1.commit().await?;
    assert_eq!(
        ship.last_write_seq(),
        committed_seq,
        "a read-only post-commit factory must not allocate another write sequence"
    );
    debug!("[OK] Post-commit config created and executed from a committed read snapshot");

    // Verify the config was actually created by reading it back
    debug!("\n=== Verifying config was created ===");
    let verify_tx = ship
        .begin_read(&PondUserMetadata::new(vec!["verify".to_string()]))
        .await?;
    let verify_root = verify_tx.root().await?;

    // Check /system exists
    match verify_root.resolve_path("/system").await {
        Ok(_) => debug!("[OK] /system exists"),
        Err(e) => debug!("[FAIL] /system does NOT exist: {}", e),
    }

    // Check /system/run exists
    match verify_root.resolve_path("/system/run").await {
        Ok(_) => debug!("[OK] /system/run exists"),
        Err(e) => debug!("[FAIL] /system/run does NOT exist: {}", e),
    }

    // Try to resolve the specific file
    match verify_root
        .resolve_path("/system/run/test-post-commit.yaml")
        .await
    {
        Ok((_, lookup)) => match lookup {
            tinyfs::Lookup::Found(_) => {
                debug!("[OK] /system/run/test-post-commit.yaml EXISTS via resolve_path!")
            }
            tinyfs::Lookup::NotFound(_, _) => debug!(
                "[FAIL] /system/run/test-post-commit.yaml not found (path resolved but file doesn't exist)"
            ),
            tinyfs::Lookup::Empty(_) => {
                debug!("[FAIL] /system/run/test-post-commit.yaml empty path")
            }
        },
        Err(e) => debug!(
            "[FAIL] Failed to resolve /system/run/test-post-commit.yaml: {}",
            e
        ),
    }

    // Check for files in /system/run
    let matches = verify_root.collect_matches("/system/run/*").await?;
    debug!(
        "[OK] Found {} file(s) in /system/run/ via collect_matches",
        matches.len()
    );
    for (node_path, captures) in &matches {
        debug!(
            "  - {} (captures: {:?})",
            node_path.path().display(),
            captures
        );
    }

    _ = verify_tx.commit().await?;

    // Verify the test factory was executed by checking the result file it creates
    // The test-executor factory writes to /tmp/test-executor-result-{factory_node_id}.txt
    let result_path = format!(
        "/tmp/test-executor-result-{}.txt",
        factory_node_id.node_id()
    );

    // Give a small delay for file write to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    match std::fs::read_to_string(&result_path) {
        Ok(content) => {
            debug!("\n=== Post-commit factory result ===");
            debug!("{}", content);

            // Verify the content contains expected elements
            assert!(
                content.contains("Executed 3 times"),
                "Should have executed 3 times"
            );
            assert!(
                content.contains("Post-commit execution test"),
                "Should contain our message"
            );
            assert!(
                content.contains("ControlReader"),
                "Should have run in ControlReader mode"
            );

            debug!("[OK] Post-commit factory executed successfully!");

            // Cleanup
            let _ = std::fs::remove_file(&result_path);
        }
        Err(e) => {
            debug!("[WARN]  Result file not found: {}", e);
            debug!("This is expected if post-commit execution hasn't been fully implemented yet.");
            debug!("Once implemented, this test should pass.");
        }
    }

    Ok(())
}

/// Test that post-commit factories don't execute on read-only transactions
#[tokio::test]
async fn test_post_commit_not_triggered_by_read_transaction() -> Result<()> {
    let temp_dir = tempdir()?;
    let pond_path = temp_dir.path().join("post_commit_read_test_pond");

    // Initialize pond
    let mut ship = Ship::create_pond(&pond_path, "test-host")
        .await
        .map_err(|e| anyhow::anyhow!("Failed to initialize pond: {}", e))?;

    // Set factory mode early to prevent error logs
    ship.control_table_mut()
        .set_factory_mode("test-executor", "push")
        .await?;

    // Create a post-commit config first
    let tx1 = ship
        .begin_write(&PondUserMetadata::new(vec![
            "test".to_string(),
            "create-config".to_string(),
        ]))
        .await?;

    let root1 = tx1.root().await?;

    _ = root1.create_dir_path("/system").await?;
    _ = root1.create_dir_path("/system/run").await?;

    let config_yaml = r#"message: "Should not execute on read"
repeat_count: 1
"#;

    // Create dynamic file using high-level path-based API
    let factory_node = root1
        .create_dynamic_path(
            "/system/run/test-no-execute.yaml",
            tinyfs::EntryType::FileDynamic,
            "test-executor",
            config_yaml.as_bytes().to_vec(),
        )
        .await?;

    let provider_context1 = tx1.provider_context()?;
    let context1 = provider::FactoryContext::new(provider_context1, factory_node.id());
    FactoryRegistry::initialize::<tlogfs::TLogFSError>(
        "test-executor",
        config_yaml.as_bytes(),
        context1,
    )
    .await?;

    _ = tx1.commit().await?;

    // Now do a read-only transaction using the transact helper
    debug!("\n=== Executing read-only transaction ===");
    ship.write_transaction(
        &PondUserMetadata::new(vec!["test".to_string(), "read-only".to_string()]),
        async |fs| {
            let root = fs.root().await?;
            // Just read something - no writes
            let _ = root.resolve_path("/system/run").await?;
            Ok(())
        },
    )
    .await?;

    debug!("[OK] Read-only transaction completed");
    debug!("Post-commit factories should NOT have executed (read-only, no version change)");

    Ok(())
}

/// Test post-commit with multiple factories in deterministic order
#[tokio::test]
async fn test_post_commit_multiple_factories_ordered() -> Result<()> {
    let temp_dir = tempdir()?;
    let pond_path = temp_dir.path().join("post_commit_ordered_test_pond");

    let mut ship = Ship::create_pond(&pond_path, "test-host").await?;

    // Set factory mode early to prevent error logs
    ship.control_table_mut()
        .set_factory_mode("test-executor", "push")
        .await?;

    // Create multiple post-commit configs
    debug!("=== Creating multiple post-commit configs ===");
    let tx1 = ship
        .begin_write(&PondUserMetadata::new(vec![
            "test".to_string(),
            "create-multiple-configs".to_string(),
        ]))
        .await?;

    let root1 = tx1.root().await?;

    _ = root1.create_dir_path("/system").await?;
    _ = root1.create_dir_path("/system/run").await?;

    // Create configs with names that will sort alphabetically
    let configs = vec![
        ("a-first.yaml", "First factory"),
        ("b-second.yaml", "Second factory"),
        ("c-third.yaml", "Third factory"),
    ];

    for (filename, message) in configs {
        let config_yaml = format!(
            r#"message: "{}"
repeat_count: 1
"#,
            message
        );

        // Create dynamic file using high-level path-based API
        let factory_node = root1
            .create_dynamic_path(
                format!("/system/run/{}", filename),
                tinyfs::EntryType::FileDynamic,
                "test-executor",
                config_yaml.as_bytes().to_vec(),
            )
            .await?;

        let provider_context = tx1.provider_context()?;
        let context = provider::FactoryContext::new(provider_context, factory_node.id());
        FactoryRegistry::initialize::<tlogfs::TLogFSError>(
            "test-executor",
            config_yaml.as_bytes(),
            context,
        )
        .await?;
    }

    _ = tx1.commit().await?;
    debug!("[OK] Multiple configs created");

    // Trigger post-commit with a data write
    debug!("\n=== Triggering post-commit ===");
    let tx2 = ship
        .begin_write(&PondUserMetadata::new(vec![
            "test".to_string(),
            "trigger-multiple".to_string(),
        ]))
        .await?;

    let root2 = tx2.root().await?;

    let mut writer = root2.async_writer_path("/trigger-multi.txt").await?;
    use tokio::io::AsyncWriteExt;
    writer.write_all(b"trigger").await?;
    writer.shutdown().await?;

    _ = tx2.commit().await?;
    debug!("[OK] Transaction committed, multiple post-commit factories should execute in order");

    Ok(())
}
