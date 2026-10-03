// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;
use std::sync::Once;

use cmd::commands::{add_backup_command, init_command, push_command, restore_command};
use cmd::common::ShipContext;
use futures::StreamExt;
use steward::{FsckOptions, PondUserMetadata};
use tempfile::TempDir;
use tinyfs::EntryType;
use tokio::io::AsyncWriteExt;

static INIT_LOG: Once = Once::new();

fn init_log() {
    INIT_LOG.call_once(|| {
        let _ = env_logger::builder().is_test(true).try_init();
    });
}

fn context(path: &Path, args: &[&str]) -> ShipContext {
    ShipContext::pond_only(
        Some(path),
        args.iter().map(|arg| (*arg).to_owned()).collect(),
    )
}

async fn write_file(ctx: &ShipContext, path: &str, content: &[u8]) -> anyhow::Result<()> {
    let mut steward = ctx.open_pond().await?;
    let content = content.to_vec();
    steward
        .write_transaction(
            &PondUserMetadata::new(vec!["restore-cli-test".to_owned()]),
            async move |transaction| {
                let root = transaction.root().await?;
                let mut writer = root
                    .async_writer_path_with_type(path, EntryType::FilePhysicalVersion)
                    .await?;
                writer.write_all(&content).await?;
                writer.shutdown().await?;
                Ok(())
            },
        )
        .await?;
    Ok(())
}

async fn read_file(ctx: &ShipContext, path: &str) -> anyhow::Result<Vec<u8>> {
    let mut steward = ctx.open_pond().await?;
    let transaction = steward
        .begin_read(&PondUserMetadata::new(vec!["restore-cli-read".to_owned()]))
        .await?;
    let bytes = transaction
        .root()
        .await?
        .read_file_path_to_vec(path)
        .await?;
    _ = transaction.commit().await?;
    Ok(bytes)
}

async fn install_unknown_automatic_factory(ctx: &ShipContext) {
    let mut steward = ctx.open_pond().await.expect("open source");
    let error = steward
        .write_transaction(
            &PondUserMetadata::new(vec!["install-automatic-factory".to_owned()]),
            async move |transaction| {
                let root = transaction.root().await?;
                _ = root.create_dir_all("/system/run").await?;
                _ = root
                    .create_dynamic_path(
                        "/system/run/10-restored",
                        EntryType::FileDynamic,
                        "no-such-restored-factory",
                        b"key: value\n".to_vec(),
                    )
                    .await?;
                Ok(())
            },
        )
        .await
        .expect_err("source commit must expose the deliberately unknown factory");
    assert!(error.to_string().contains("no-such-restored-factory"));
}

#[tokio::test]
async fn restore_bootstrap_keeps_factories_inert_then_resumes_dispatch() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let source_path = scratch.path().join("source");
    let restored_path = scratch.path().join("restored");
    let remote_path = scratch.path().join("remote");
    let remote_url = format!("file://{}", remote_path.display());
    let source = context(&source_path, &["pond", "init"]);
    let restored = context(&restored_path, &["pond", "restore"]);

    init_command(&source, "restore-cli-source")
        .await
        .expect("initialize source");
    write_file(&source, "/payload.txt", b"published payload")
        .await
        .expect("write source payload");
    add_backup_command(
        &source,
        "origin",
        &remote_url,
        false,
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("attach source backup");
    push_command(&source, Some("origin".to_owned()))
        .await
        .expect("publish source");

    install_unknown_automatic_factory(&source).await;
    push_command(&source, Some("origin".to_owned()))
        .await
        .expect("publish automatic factory");

    restore_command(
        &restored,
        "origin",
        &remote_url,
        None,
        None,
        None,
        None,
        false,
    )
    .await
    .expect("restore must not dispatch imported automatic factories");

    assert_eq!(
        read_file(&restored, "/payload.txt")
            .await
            .expect("read restored payload"),
        b"published payload"
    );
    {
        let mut steward = restored.open_pond().await.expect("inspect restored pond");
        let transaction = steward
            .begin_read(&PondUserMetadata::new(vec![
                "inspect-restored-recipe".to_owned(),
            ]))
            .await
            .expect("begin restored read");
        let root = transaction.root().await.expect("restored root");
        let run_dir = root
            .open_dir_path("/system/run")
            .await
            .expect("open restored automatic factory directory");
        let mut entries = run_dir.entries().await.expect("list restored recipes");
        let mut restored_recipe = None;
        while let Some(entry) = entries.next().await {
            let entry = entry.expect("restored recipe metadata");
            if entry.name == "10-restored" {
                restored_recipe = Some(entry);
                break;
            }
        }
        assert_eq!(
            restored_recipe
                .expect("restore must preserve the automatic factory recipe")
                .entry_type,
            EntryType::FileDynamic
        );
        _ = transaction.commit().await.expect("commit restored read");
    }

    let source_steward = source.open_pond().await.expect("reopen source");
    let restored_steward = restored.open_pond().await.expect("reopen restored pond");
    let source_pond = source_steward.as_pond().expect("source pond");
    let restored_pond = restored_steward.as_pond().expect("restored pond");
    assert_eq!(
        source_pond.control_table().pond_id_uuid(),
        restored_pond.control_table().pond_id_uuid(),
        "restore must adopt the source pond identity"
    );
    let source_fsck = steward::fsck(source_pond, FsckOptions::default())
        .await
        .expect("fsck source");
    let restored_fsck = steward::fsck(restored_pond, FsckOptions::default())
        .await
        .expect("fsck restored pond");
    assert!(source_fsck.ok(), "source fsck: {:?}", source_fsck.errors);
    assert!(
        restored_fsck.ok(),
        "restored fsck: {:?}",
        restored_fsck.errors
    );
    assert_eq!(
        source_fsck.root, restored_fsck.root,
        "restored content must exactly match the publication"
    );
    drop(source_steward);
    drop(restored_steward);

    let error = write_file(&restored, "/after-restore.txt", b"trigger")
        .await
        .expect_err("ordinary post-restore write must resume factory dispatch");
    assert!(error.to_string().contains("no-such-restored-factory"));
    assert_eq!(
        read_file(&restored, "/after-restore.txt")
            .await
            .expect("the triggering write is durable"),
        b"trigger"
    );
}

#[tokio::test]
async fn restore_pull_failure_removes_replica_shell_paths() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let source_path = scratch.path().join("source");
    let restored_path = scratch.path().join("restored");
    let remote_path = scratch.path().join("remote");
    let remote_url = format!("file://{}", remote_path.display());
    let source = context(&source_path, &["pond", "init"]);
    let restored = context(&restored_path, &["pond", "restore"]);

    init_command(&source, "restore-cli-cleanup-source")
        .await
        .expect("initialize source");
    write_file(&source, "/payload.txt", b"published payload")
        .await
        .expect("write source payload");
    add_backup_command(
        &source,
        "origin",
        &remote_url,
        false,
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("attach source backup");
    push_command(&source, Some("origin".to_owned()))
        .await
        .expect("publish source");

    let remote = sync_store::ContentRemote::open_at_url(&remote_url, Default::default())
        .await
        .expect("open publication");
    let publication = remote
        .current_publication("main")
        .await
        .expect("read publication")
        .expect("published state");
    let missing_tip = publication.snapshot_tip.to_hex();
    std::fs::remove_file(
        remote_path
            .join("_content/v2/objects")
            .join(format!("blake3={missing_tip}")),
    )
    .expect("remove published tip object");

    let error = restore_command(
        &restored,
        "origin",
        &remote_url,
        None,
        None,
        None,
        None,
        false,
    )
    .await
    .expect_err("restore must fail when the published tip object is missing");
    let error_detail = format!("{error:#}");
    assert!(
        error_detail.contains(&missing_tip),
        "restore must report the missing published object: {error_detail}"
    );

    for path in ["data", "control", "tlog"] {
        assert!(
            !restored_path.join(path).exists(),
            "failed restore must remove {}",
            restored_path.join(path).display()
        );
    }
}
