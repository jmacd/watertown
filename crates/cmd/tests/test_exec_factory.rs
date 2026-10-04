// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! End-to-end smoke test for the `exec` factory (design doc §6): stage a
//! pond input into a sandbox, run a trivial program against it, and commit
//! the declared output back into the pond as one transaction.
//!
//! Requires `bwrap` and Linux namespaces (see `docs/exec-factory-design.md`);
//! ignored on other platforms so `cargo test` passes on macOS dev machines.
//! Validate on macOS via the Docker loop documented there.

use cmd::common::ShipContext;
use provider::registry::ExecutionContext;
use steward::PondUserMetadata;
use tempfile::TempDir;
use tlogfs::FactoryRegistry;

async fn setup_test_ship() -> (ShipContext, TempDir) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let pond_path = temp_dir.path().join("test_pond");

    let ship_context = ShipContext {
        pond_path: Some(pond_path.clone()),
        host_root: None,
        mount_specs: Vec::new(),
        original_args: vec!["pond".to_string(), "init".to_string()],
    };

    cmd::commands::init::init_command(&ship_context, "test-host")
        .await
        .expect("Failed to initialize pond");

    (ship_context, temp_dir)
}

#[tokio::test]
#[cfg_attr(not(target_os = "linux"), ignore)]
async fn exec_factory_runs_cat_and_commits_output() {
    let (ship_context, _temp_dir) = setup_test_ship().await;

    let config_yaml = r#"
program: /bin/sh
args: ["-c", "cat data/in.txt > data/out.txt"]
inputs: ["/data/in.txt"]
outputs: ["/data/out.txt"]
"#;

    let mut ship = ship_context.open_pond().await.expect("open pond");
    let tx = ship
        .begin_write(&PondUserMetadata::new(vec![
            "test".to_string(),
            "exec-factory-smoke".to_string(),
        ]))
        .await
        .expect("begin transaction");

    let root = tx.root().await.expect("root");
    _ = root
        .create_dir_path("/data")
        .await
        .expect("create /data dir");
    root.write_file_path_from_slice("/data/in.txt", b"hello from the pond")
        .await
        .expect("write input file");

    _ = root
        .create_dir_path("/configs")
        .await
        .expect("create /configs dir");
    let (parent_wd, _) = root.resolve_path("/configs").await.expect("resolve /configs");
    let parent_node_id = parent_wd.node_path().id();

    let _node_path = root
        .create_dynamic_path(
            "/configs/billing-cat",
            tinyfs::EntryType::FileDynamic,
            "exec",
            config_yaml.as_bytes().to_vec(),
        )
        .await
        .expect("create exec config node");

    let provider_context = tx.provider_context().expect("provider context");
    let context = provider::FactoryContext::new(provider_context, parent_node_id);
    FactoryRegistry::initialize::<tlogfs::TLogFSError>(
        "exec",
        config_yaml.as_bytes(),
        context.clone(),
    )
    .await
    .expect("initialize exec factory");

    FactoryRegistry::execute::<tlogfs::TLogFSError>(
        "exec",
        config_yaml.as_bytes(),
        context,
        ExecutionContext::pond_readwriter(vec![]),
    )
    .await
    .expect("execute exec factory (requires bwrap on Linux)");

    let output = root
        .read_file_path_to_vec("/data/out.txt")
        .await
        .expect("read committed output");
    assert_eq!(output, b"hello from the pond");

    _ = tx.commit().await.expect("commit transaction");
}

#[tokio::test]
#[cfg_attr(not(target_os = "linux"), ignore)]
async fn exec_factory_nonzero_exit_leaves_pond_unchanged() {
    let (ship_context, _temp_dir) = setup_test_ship().await;

    let config_yaml = r#"
program: /bin/sh
args: ["-c", "echo should not be committed > data/out.txt; exit 1"]
inputs: []
outputs: ["/data/out.txt"]
"#;

    let mut ship = ship_context.open_pond().await.expect("open pond");
    let tx = ship
        .begin_write(&PondUserMetadata::new(vec![
            "test".to_string(),
            "exec-factory-failure".to_string(),
        ]))
        .await
        .expect("begin transaction");

    let root = tx.root().await.expect("root");
    _ = root
        .create_dir_path("/configs")
        .await
        .expect("create /configs dir");
    let (parent_wd, _) = root.resolve_path("/configs").await.expect("resolve /configs");
    let parent_node_id = parent_wd.node_path().id();

    let _node_path = root
        .create_dynamic_path(
            "/configs/billing-fail",
            tinyfs::EntryType::FileDynamic,
            "exec",
            config_yaml.as_bytes().to_vec(),
        )
        .await
        .expect("create exec config node");

    let provider_context = tx.provider_context().expect("provider context");
    let context = provider::FactoryContext::new(provider_context, parent_node_id);
    FactoryRegistry::initialize::<tlogfs::TLogFSError>(
        "exec",
        config_yaml.as_bytes(),
        context.clone(),
    )
    .await
    .expect("initialize exec factory");

    let result = FactoryRegistry::execute::<tlogfs::TLogFSError>(
        "exec",
        config_yaml.as_bytes(),
        context,
        ExecutionContext::pond_readwriter(vec![]),
    )
    .await;

    assert!(
        result.is_err(),
        "a nonzero exit must fail execute(), not commit a partial output"
    );
    assert!(!root.exists("/data/out.txt").await);
}
