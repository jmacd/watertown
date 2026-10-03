// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::persistence::OpLogPersistence;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use tinyfs::arrow::ParquetExt;
use tinyfs::testing::persistence_contract::{
    BYTE_SERIES_PATH, FIRST_VERSION_CONTENT, SECOND_VERSION_CONTENT,
    assert_active_transaction_read_write,
};

#[tokio::test]
async fn active_transaction_matches_tinyfs_persistence_contract() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("pond");
    let mut persistence =
        OpLogPersistence::create_test(path.to_str().expect("temporary path must be UTF-8"))
            .await
            .expect("create TLogFS persistence");

    let tx = persistence.begin_test().await.expect("begin write");
    let root = tx.root().await.expect("root");
    let context = tx.state().expect("transaction state").as_provider_context();
    let artifact = assert_active_transaction_read_write(&root, &context)
        .await
        .expect("active transaction contract");
    provider::testing::assert_series_read_after_write(&root, &context).await;
    provider::testing::assert_foundation_snapshot(&root, &context).await;
    provider::testing::assert_materialization_append(&root, &context).await;
    provider::testing::assert_materialization_failure_contract(&root, &context).await;
    let materialized_versions = root
        .list_file_versions(provider::testing::MATERIALIZED_SERIES_PATH)
        .await
        .expect("materialized version metadata");
    let materialized_metadata = materialized_versions[0]
        .extended_metadata
        .as_ref()
        .expect("materialized extended metadata");
    assert!(materialized_metadata.contains_key("logical_leaf_hash"));
    assert_eq!(
        materialized_metadata
            .get("logical_count")
            .map(String::as_str),
        Some("3")
    );
    assert!(materialized_metadata.contains_key("series_schema_fingerprint"));
    tx.commit_test().await.expect("commit contract writes");

    provider::testing::assert_registered_provider_closed(&context).await;
    provider::testing::assert_foundation_provider_closed(&context).await;
    let closed_error = context
        .persistence
        .read_file_version(artifact.file_id, artifact.first_version)
        .await
        .expect_err("a committed transaction context must be closed");
    assert!(
        closed_error.to_string().contains("closed"),
        "unexpected post-commit error: {closed_error}"
    );

    let tx = persistence
        .begin_test()
        .await
        .expect("begin verification read");
    let root = tx.root().await.expect("verification root");
    let context = tx
        .state()
        .expect("verification state")
        .as_provider_context();
    assert_eq!(
        root.read_file_path_to_vec(BYTE_SERIES_PATH)
            .await
            .expect("read committed series"),
        [FIRST_VERSION_CONTENT, SECOND_VERSION_CONTENT].concat()
    );
    let versions = root
        .list_file_versions(BYTE_SERIES_PATH)
        .await
        .expect("list committed versions");
    assert_eq!(
        versions
            .iter()
            .map(|version| version.version)
            .collect::<Vec<_>>(),
        vec![artifact.first_version, artifact.second_version]
    );
    provider::testing::assert_series_row_count(&root, &context, 3).await;
    provider::testing::assert_foundation_snapshot(&root, &context).await;
    assert_eq!(
        root.read_table_as_batch(provider::testing::MATERIALIZED_SERIES_PATH)
            .await
            .expect("read persisted materialization")
            .num_rows(),
        3
    );
    assert_eq!(
        root.list_file_versions(format!(
            "{}.materialization-progress",
            provider::testing::EMPTY_MATERIALIZED_SERIES_PATH
        ))
        .await
        .expect("persisted no-output progress")
        .len(),
        1
    );
}

#[tokio::test]
async fn aborted_transaction_closes_context_and_discards_writes() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("pond");
    let mut persistence =
        OpLogPersistence::create_test(path.to_str().expect("temporary path must be UTF-8"))
            .await
            .expect("create TLogFS persistence");

    let context = {
        let tx = persistence.begin_test().await.expect("begin aborted write");
        let root = tx.root().await.expect("root");
        root.write_file_path_from_slice("/aborted.txt", b"must not commit")
            .await
            .expect("stage aborted file");
        let context = tx.state().expect("transaction state").as_provider_context();
        provider::testing::assert_series_read_after_write(&root, &context).await;
        drop(tx);
        context
    };

    provider::testing::assert_registered_provider_closed(&context).await;
    let closed_error = context
        .persistence
        .metadata(tinyfs::FileID::root_for(context.persistence.pond_uuid()))
        .await
        .expect_err("an aborted transaction context must be closed");
    assert!(
        closed_error.to_string().contains("closed"),
        "unexpected post-abort error: {closed_error}"
    );

    let tx = persistence
        .begin_test()
        .await
        .expect("begin verification read");
    let root = tx.root().await.expect("verification root");
    assert!(
        !root.exists(std::path::Path::new("/aborted.txt")).await,
        "an aborted write must not become visible"
    );
    assert!(
        !root
            .exists(std::path::Path::new(provider::testing::TABLE_SERIES_PATH))
            .await,
        "aborted series versions must not become visible"
    );
}

#[tokio::test]
async fn delta_history_does_not_expand_exact_version_plan() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("pond");
    let mut persistence =
        OpLogPersistence::create_test(path.to_str().expect("temporary path must be UTF-8"))
            .await
            .expect("create TLogFS persistence");

    let target_id = {
        let tx = persistence.begin_test().await.expect("begin target write");
        let root = tx.root().await.expect("target root");
        _ = root
            .create_series_from_batch(
                "/target.series",
                &arrow_array::RecordBatch::try_from_iter([(
                    "timestamp",
                    std::sync::Arc::new(arrow_array::Int64Array::from(vec![1]))
                        as arrow_array::ArrayRef,
                )])
                .expect("target batch"),
                Some("timestamp"),
            )
            .await
            .expect("target series");
        let id = root
            .get_node_path("/target.series")
            .await
            .expect("target node")
            .id();
        tx.commit_test().await.expect("commit target");
        id
    };

    async fn exact_node_plan(persistence: &mut OpLogPersistence, id: tinyfs::FileID) -> String {
        let tx = persistence.begin_test().await.expect("begin plan snapshot");
        let context = tx.state().expect("plan state").as_provider_context();
        let frame = context
            .datafusion_session
            .sql(&format!(
                "SELECT * FROM delta_table WHERE pond_id = '{}' AND part_id = '{}' AND node_id = \
                 '{}' AND version = 1 LIMIT 1",
                id.pond_id(),
                id.part_id(),
                id.node_id()
            ))
            .await
            .expect("plan exact node query");
        let physical = frame
            .create_physical_plan()
            .await
            .expect("exact node physical plan");
        DisplayableExecutionPlan::new(physical.as_ref())
            .indent(true)
            .to_string()
    }

    let initial = exact_node_plan(&mut persistence, target_id).await;
    for index in 0..16 {
        let tx = persistence
            .begin_test()
            .await
            .expect("begin unrelated write");
        let root = tx.root().await.expect("unrelated root");
        root.write_file_path_from_slice(
            format!("/unrelated-{index:04}.txt"),
            format!("unrelated-{index}").as_bytes(),
        )
        .await
        .expect("unrelated write");
        _ = root
            .write_series_from_batch(
                "/target.series",
                &arrow_array::RecordBatch::try_from_iter([(
                    "timestamp",
                    std::sync::Arc::new(arrow_array::Int64Array::from(vec![index as i64 + 2]))
                        as arrow_array::ArrayRef,
                )])
                .expect("target append batch"),
                Some("timestamp"),
            )
            .await
            .expect("target history append");
        tx.commit_test().await.expect("commit unrelated write");
    }
    let aged = exact_node_plan(&mut persistence, target_id).await;
    assert_eq!(aged, initial, "Delta age changed exact-node plan:\n{aged}");
}
