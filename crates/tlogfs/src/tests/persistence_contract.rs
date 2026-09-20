// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::persistence::OpLogPersistence;
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
    tx.commit_test().await.expect("commit contract writes");

    provider::testing::assert_registered_provider_closed(&context).await;
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
