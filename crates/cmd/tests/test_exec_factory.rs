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
    let (parent_wd, _) = root
        .resolve_path("/configs")
        .await
        .expect("resolve /configs");
    let parent_node_id = parent_wd.node_path().id();

    let _node_path = root
        .create_dynamic_path(
            "/configs/exec-cat",
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
    let (parent_wd, _) = root
        .resolve_path("/configs")
        .await
        .expect("resolve /configs");
    let parent_node_id = parent_wd.node_path().id();

    let _node_path = root
        .create_dynamic_path(
            "/configs/exec-fail",
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

#[tokio::test]
#[cfg_attr(not(target_os = "linux"), ignore)]
async fn exec_factory_rejects_deletion_of_prior_output() {
    let (ship_context, _temp_dir) = setup_test_ship().await;

    // The program deletes a declared output that already existed in the
    // pond before this run. Per the design doc, deletions are never
    // committed in v1: the whole run must fail and the prior content must
    // still be there afterward.
    let config_yaml = r#"
program: /bin/sh
args: ["-c", "rm -f data/out.txt"]
inputs: []
outputs: ["/data/out.txt"]
"#;

    let mut ship = ship_context.open_pond().await.expect("open pond");
    let tx = ship
        .begin_write(&PondUserMetadata::new(vec![
            "test".to_string(),
            "exec-factory-deletion-setup".to_string(),
        ]))
        .await
        .expect("begin setup transaction");
    let root = tx.root().await.expect("root");
    _ = root
        .create_dir_path("/data")
        .await
        .expect("create /data dir");
    root.write_file_path_from_slice("/data/out.txt", b"prior output, must survive")
        .await
        .expect("seed prior output");
    _ = tx.commit().await.expect("commit setup transaction");

    let mut ship = ship_context.open_pond().await.expect("reopen pond");
    let tx = ship
        .begin_write(&PondUserMetadata::new(vec![
            "test".to_string(),
            "exec-factory-deletion-run".to_string(),
        ]))
        .await
        .expect("begin run transaction");
    let root = tx.root().await.expect("root");

    _ = root
        .create_dir_path("/configs")
        .await
        .expect("create /configs dir");
    let (parent_wd, _) = root
        .resolve_path("/configs")
        .await
        .expect("resolve /configs");
    let parent_node_id = parent_wd.node_path().id();

    let _node_path = root
        .create_dynamic_path(
            "/configs/exec-delete",
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
        "deleting a declared output that pre-existed must fail the run"
    );

    let surviving = root
        .read_file_path_to_vec("/data/out.txt")
        .await
        .expect("prior output must still be readable after the rejected run");
    assert_eq!(surviving, b"prior output, must survive");
}

#[tokio::test]
#[cfg_attr(not(target_os = "linux"), ignore)]
async fn exec_factory_rejects_deletion_of_prior_output_under_dir_prefix() {
    let (ship_context, _temp_dir) = setup_test_ship().await;

    // Same guarantee as `exec_factory_rejects_deletion_of_prior_output`, but
    // for a directory-prefix output (`outputs: ["/reports/"]`) with a
    // pre-existing file nested a level deep, and NOT also listed under
    // `inputs`. Exercises the recursive pre-staging in
    // `stage_existing_outputs`'s `OutputSpec::DirPrefix` branch.
    let config_yaml = r#"
program: /bin/sh
args: ["-c", "rm -f reports/2026/january.txt"]
inputs: []
outputs: ["/reports/"]
"#;

    let mut ship = ship_context.open_pond().await.expect("open pond");
    let tx = ship
        .begin_write(&PondUserMetadata::new(vec![
            "test".to_string(),
            "exec-factory-dirprefix-deletion-setup".to_string(),
        ]))
        .await
        .expect("begin setup transaction");
    let root = tx.root().await.expect("root");
    _ = root
        .create_dir_all("/reports/2026")
        .await
        .expect("create /reports/2026 dir");
    root.write_file_path_from_slice("/reports/2026/january.txt", b"prior report, must survive")
        .await
        .expect("seed prior output");
    _ = tx.commit().await.expect("commit setup transaction");

    let mut ship = ship_context.open_pond().await.expect("reopen pond");
    let tx = ship
        .begin_write(&PondUserMetadata::new(vec![
            "test".to_string(),
            "exec-factory-dirprefix-deletion-run".to_string(),
        ]))
        .await
        .expect("begin run transaction");
    let root = tx.root().await.expect("root");

    _ = root
        .create_dir_path("/configs")
        .await
        .expect("create /configs dir");
    let (parent_wd, _) = root
        .resolve_path("/configs")
        .await
        .expect("resolve /configs");
    let parent_node_id = parent_wd.node_path().id();

    let _node_path = root
        .create_dynamic_path(
            "/configs/exec-delete-dirprefix",
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
        "deleting a declared output nested under a directory-prefix output must fail the run"
    );

    let surviving = root
        .read_file_path_to_vec("/reports/2026/january.txt")
        .await
        .expect("prior nested output must still be readable after the rejected run");
    assert_eq!(surviving, b"prior report, must survive");
}

#[tokio::test]
#[cfg_attr(not(target_os = "linux"), ignore)]
async fn exec_factory_series_output_appends_across_multiple_runs() {
    let (ship_context, _temp_dir) = setup_test_ship().await;

    // Each run only ever appends one more line. `series_outputs` maps this
    // onto `tinyfs::EntryType::FilePhysicalSeries`: every run commits only
    // the *new* suffix as the next version, and reading the path
    // concatenates all versions -- so after two runs the pond should show
    // the full two-line journal, not a duplicated or truncated one.
    let config_step1 = r#"
program: /bin/sh
args: ["-c", "echo 'entry one' >> accounting/journal.ledger"]
inputs: []
outputs: []
series_outputs: ["/accounting/journal.ledger"]
"#;
    let config_step2 = r#"
program: /bin/sh
args: ["-c", "echo 'entry two' >> accounting/journal.ledger"]
inputs: []
outputs: []
series_outputs: ["/accounting/journal.ledger"]
"#;

    // First run: the series output doesn't exist in the pond yet.
    let mut ship = ship_context.open_pond().await.expect("open pond");
    let tx = ship
        .begin_write(&PondUserMetadata::new(vec![
            "test".to_string(),
            "exec-factory-series-step1".to_string(),
        ]))
        .await
        .expect("begin step1 transaction");
    let root = tx.root().await.expect("root");
    _ = root
        .create_dir_path("/configs")
        .await
        .expect("create /configs dir");
    let (parent_wd, _) = root
        .resolve_path("/configs")
        .await
        .expect("resolve /configs");
    let parent_node_id = parent_wd.node_path().id();
    let _node_path = root
        .create_dynamic_path(
            "/configs/journal-step1",
            tinyfs::EntryType::FileDynamic,
            "exec",
            config_step1.as_bytes().to_vec(),
        )
        .await
        .expect("create exec config node");
    let provider_context = tx.provider_context().expect("provider context");
    let context = provider::FactoryContext::new(provider_context, parent_node_id);
    FactoryRegistry::initialize::<tlogfs::TLogFSError>(
        "exec",
        config_step1.as_bytes(),
        context.clone(),
    )
    .await
    .expect("initialize exec factory");
    FactoryRegistry::execute::<tlogfs::TLogFSError>(
        "exec",
        config_step1.as_bytes(),
        context,
        ExecutionContext::pond_readwriter(vec![]),
    )
    .await
    .expect("execute step1 (requires bwrap on Linux)");

    let after_step1 = root
        .read_file_path_to_vec("/accounting/journal.ledger")
        .await
        .expect("read journal after step1");
    assert_eq!(after_step1, b"entry one\n");
    _ = tx.commit().await.expect("commit step1 transaction");

    // Second run: the series output already exists with one version;
    // only "entry two\n" should be committed as the next version.
    let mut ship = ship_context.open_pond().await.expect("reopen pond");
    let tx = ship
        .begin_write(&PondUserMetadata::new(vec![
            "test".to_string(),
            "exec-factory-series-step2".to_string(),
        ]))
        .await
        .expect("begin step2 transaction");
    let root = tx.root().await.expect("root");
    let (parent_wd, _) = root
        .resolve_path("/configs")
        .await
        .expect("resolve /configs");
    let parent_node_id = parent_wd.node_path().id();
    let _node_path = root
        .create_dynamic_path(
            "/configs/journal-step2",
            tinyfs::EntryType::FileDynamic,
            "exec",
            config_step2.as_bytes().to_vec(),
        )
        .await
        .expect("create exec config node");
    let provider_context = tx.provider_context().expect("provider context");
    let context = provider::FactoryContext::new(provider_context, parent_node_id);
    FactoryRegistry::initialize::<tlogfs::TLogFSError>(
        "exec",
        config_step2.as_bytes(),
        context.clone(),
    )
    .await
    .expect("initialize exec factory");
    FactoryRegistry::execute::<tlogfs::TLogFSError>(
        "exec",
        config_step2.as_bytes(),
        context,
        ExecutionContext::pond_readwriter(vec![]),
    )
    .await
    .expect("execute step2 (requires bwrap on Linux)");

    let after_step2 = root
        .read_file_path_to_vec("/accounting/journal.ledger")
        .await
        .expect("read journal after step2");
    assert_eq!(after_step2, b"entry one\nentry two\n");
    _ = tx.commit().await.expect("commit step2 transaction");
}

#[tokio::test]
#[cfg_attr(not(target_os = "linux"), ignore)]
async fn exec_factory_series_output_rejects_rewrite() {
    let (ship_context, _temp_dir) = setup_test_ship().await;

    let config_seed = r#"
program: /bin/sh
args: ["-c", "echo 'entry one' >> accounting/journal.ledger"]
inputs: []
outputs: []
series_outputs: ["/accounting/journal.ledger"]
"#;
    // Overwrites (`>`) instead of appending (`>>`): this must be rejected,
    // not silently committed as a new "first version" that would make the
    // concatenated read go backwards.
    let config_rewrite = r#"
program: /bin/sh
args: ["-c", "echo 'REWRITTEN' > accounting/journal.ledger"]
inputs: []
outputs: []
series_outputs: ["/accounting/journal.ledger"]
"#;

    let mut ship = ship_context.open_pond().await.expect("open pond");
    let tx = ship
        .begin_write(&PondUserMetadata::new(vec![
            "test".to_string(),
            "exec-factory-series-rewrite-seed".to_string(),
        ]))
        .await
        .expect("begin seed transaction");
    let root = tx.root().await.expect("root");
    _ = root
        .create_dir_path("/configs")
        .await
        .expect("create /configs dir");
    let (parent_wd, _) = root
        .resolve_path("/configs")
        .await
        .expect("resolve /configs");
    let parent_node_id = parent_wd.node_path().id();
    let _node_path = root
        .create_dynamic_path(
            "/configs/journal-seed",
            tinyfs::EntryType::FileDynamic,
            "exec",
            config_seed.as_bytes().to_vec(),
        )
        .await
        .expect("create exec config node");
    let provider_context = tx.provider_context().expect("provider context");
    let context = provider::FactoryContext::new(provider_context, parent_node_id);
    FactoryRegistry::initialize::<tlogfs::TLogFSError>(
        "exec",
        config_seed.as_bytes(),
        context.clone(),
    )
    .await
    .expect("initialize exec factory");
    FactoryRegistry::execute::<tlogfs::TLogFSError>(
        "exec",
        config_seed.as_bytes(),
        context,
        ExecutionContext::pond_readwriter(vec![]),
    )
    .await
    .expect("execute seed run (requires bwrap on Linux)");
    _ = tx.commit().await.expect("commit seed transaction");

    let mut ship = ship_context.open_pond().await.expect("reopen pond");
    let tx = ship
        .begin_write(&PondUserMetadata::new(vec![
            "test".to_string(),
            "exec-factory-series-rewrite-attempt".to_string(),
        ]))
        .await
        .expect("begin rewrite transaction");
    let root = tx.root().await.expect("root");
    let (parent_wd, _) = root
        .resolve_path("/configs")
        .await
        .expect("resolve /configs");
    let parent_node_id = parent_wd.node_path().id();
    let _node_path = root
        .create_dynamic_path(
            "/configs/journal-rewrite",
            tinyfs::EntryType::FileDynamic,
            "exec",
            config_rewrite.as_bytes().to_vec(),
        )
        .await
        .expect("create exec config node");
    let provider_context = tx.provider_context().expect("provider context");
    let context = provider::FactoryContext::new(provider_context, parent_node_id);
    FactoryRegistry::initialize::<tlogfs::TLogFSError>(
        "exec",
        config_rewrite.as_bytes(),
        context.clone(),
    )
    .await
    .expect("initialize exec factory");

    let result = FactoryRegistry::execute::<tlogfs::TLogFSError>(
        "exec",
        config_rewrite.as_bytes(),
        context,
        ExecutionContext::pond_readwriter(vec![]),
    )
    .await;

    assert!(
        result.is_err(),
        "overwriting instead of appending to a series output must fail the run"
    );

    let surviving = root
        .read_file_path_to_vec("/accounting/journal.ledger")
        .await
        .expect("prior series content must still be readable after the rejected run");
    assert_eq!(surviving, b"entry one\n");
}
