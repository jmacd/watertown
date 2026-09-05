// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! D4 CLI integration test: `pond remote add` -> `pond push` -> `pond pull`
//! roundtrip using a `file://` remote, exercised entirely through the
//! library entry points (no spawned subprocesses).

use cmd::commands::{
    add_backup_command, add_remote_command, init_command, list_remotes_command, pull_command,
    push_command, remote::remote_config_path, status_command, verify_command,
};
use cmd::common::ShipContext;
use std::sync::Once;
use steward::{PondUserMetadata, REMOTE_MODE_PREFIX, REMOTE_MOUNT_PATH_PREFIX};
use tempfile::TempDir;
use tinyfs::EntryType;
use tokio::io::AsyncWriteExt;

static INIT_LOG: Once = Once::new();
fn init_log() {
    INIT_LOG.call_once(|| {
        let _ = env_logger::builder().is_test(true).try_init();
    });
}

/// Build a `ShipContext` for a brand-new pond at `pond_path`.
fn ctx_for(pond_path: &std::path::Path, args: Vec<&str>) -> ShipContext {
    ShipContext {
        pond_path: Some(pond_path.to_path_buf()),
        host_root: None,
        mount_specs: Vec::new(),
        original_args: args.into_iter().map(String::from).collect(),
    }
}

/// Write a small file at `path` within `ship`.  Uses a one-shot write
/// transaction, so the file lives in its own `txn_seq`.
async fn write_small_file(
    ctx: &ShipContext,
    path: &str,
    bytes: &[u8],
    args: Vec<&str>,
) -> anyhow::Result<()> {
    let mut ship = ctx.open_pond().await?;
    ship.write_transaction(
        &PondUserMetadata::new(args.into_iter().map(String::from).collect()),
        async |fs| {
            let root = fs.root().await?;
            let mut w = root
                .async_writer_path_with_type(path, EntryType::FilePhysicalVersion)
                .await?;
            w.write_all(bytes)
                .await
                .map_err(|e| steward::StewardError::Aborted(format!("write: {}", e)))?;
            w.shutdown()
                .await
                .map_err(|e| steward::StewardError::Aborted(format!("close: {}", e)))?;
            Ok(())
        },
    )
    .await?;
    Ok(())
}

/// Read a file from `ctx` as Vec<u8>.
async fn read_small_file(ctx: &ShipContext, path: &str) -> anyhow::Result<Vec<u8>> {
    let mut ship = ctx.open_pond().await?;
    let tx = ship
        .begin_read(&PondUserMetadata::new(vec![
            "verify".to_string(),
            path.to_string(),
        ]))
        .await?;
    let bytes = {
        let fs = &*tx;
        let root = fs.root().await?;
        root.read_file_path_to_vec(path).await?
    };
    let _ = tx.commit().await?;
    Ok(bytes)
}

/// End-to-end: `pond init` -> write a file -> `pond remote add` -> `pond push`
/// -> bootstrap a second pond -> `pond pull` -> read the file out.
#[tokio::test]
async fn pond_remote_push_pull_roundtrip() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let src_pond = scratch.path().join("src_pond");
    let dst_pond = scratch.path().join("dst_pond");
    let remote_path = scratch.path().join("remote_bucket");
    let remote_url = format!("file://{}", remote_path.display());

    // 1) Source pond + one user write transaction.
    let src_ctx = ctx_for(&src_pond, vec!["pond", "init"]);
    init_command(&src_ctx, "test-host").await.expect("init src");
    write_small_file(
        &src_ctx,
        "/hello.txt",
        b"hello from source pond",
        vec!["copy", "hello.txt"],
    )
    .await
    .expect("write hello.txt");

    // 2) `pond backup add origin file://...` auto-initializes a fresh
    // ContentRemote at the URL; first push sends the content closure.
    add_backup_command(
        &src_ctx,
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
    .expect("remote add");
    push_command(&src_ctx, Some("origin".to_string()))
        .await
        .expect("push");

    // 3) Empty replica (same pond_id, minimal root v1), pull -> mirror rebuild.
    let src_pond_id = {
        let ship = src_ctx.open_pond().await.expect("open src");
        ship.control_table().pond_id_uuid()
    };
    let _ = steward::Ship::create_replica(&dst_pond, src_pond_id)
        .await
        .expect("create dst replica");
    let dst_ctx = ctx_for(&dst_pond, vec!["pond", "pull"]);
    add_remote_command(
        &dst_ctx,
        "origin",
        &remote_url,
        "/",
        None,
        None,
        None,
        None,
        false,
        true,
    )
    .await
    .expect("re-attach origin on dst");
    pull_command(&dst_ctx, Some("origin".to_string()))
        .await
        .expect("pull origin on dst");

    let bytes = read_small_file(&dst_ctx, "/hello.txt")
        .await
        .expect("read /hello.txt on dst");
    assert_eq!(bytes, b"hello from source pond");
    list_remotes_command(&src_ctx, None)
        .await
        .expect("list src");
}

#[tokio::test]
async fn pond_remote_list_rejects_unreadable_attachment() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let remote_path = scratch.path().join("remote");
    let remote_url = format!("file://{}", remote_path.display());
    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init pond");
    add_backup_command(
        &ctx,
        "broken",
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
    .expect("create remote attachment");
    write_small_file(
        &ctx,
        &remote_config_path("broken"),
        b"not: [valid remote yaml",
        vec!["test", "write-broken-remote"],
    )
    .await
    .expect("write malformed remote attachment");

    let error = list_remotes_command(&ctx, None)
        .await
        .expect_err("remote list must reject an unreadable attachment");
    assert!(
        error
            .to_string()
            .contains("could not read /sys/remotes/broken"),
        "unexpected error: {error}"
    );
}

/// Regression test for P1-BUG-LF-REPLICATION (D5.9): a file larger than
/// `tlogfs::large_files::LARGE_FILE_THRESHOLD` (64 KiB) is externalized
/// into `<pond>/data/_large_files/blake3_16=…/blake3=….parquet`, and
/// the partition parquet committed to Delta carries only a `blake3`
/// reference, not the bytes.  Before D5.9, `Steward::actions_at_version`
/// listed only Delta `Add` paths, so the body blob never made it into
/// the push bundle.  This test pushes such a file and verifies the
/// replica sees the full byte content via `read_file_path_to_vec`.
#[tokio::test]
async fn pond_remote_push_pull_large_file_roundtrip() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let src_pond = scratch.path().join("src_pond");
    let dst_pond = scratch.path().join("dst_pond");
    let remote_path = scratch.path().join("remote_bucket");
    let remote_url = format!("file://{}", remote_path.display());

    // 80 KiB > LARGE_FILE_THRESHOLD (64 KiB).  Use a non-trivial
    // byte pattern so any silent truncation/replacement is detected.
    let big: Vec<u8> = (0..80 * 1024).map(|i| (i as u8).wrapping_mul(31)).collect();
    assert!(
        big.len() > tlogfs::large_files::LARGE_FILE_THRESHOLD,
        "test payload must exceed LARGE_FILE_THRESHOLD"
    );

    // 1) Source pond + one user write transaction.
    let src_ctx = ctx_for(&src_pond, vec!["pond", "init"]);
    init_command(&src_ctx, "test-host").await.expect("init src");
    write_small_file(&src_ctx, "/big.bin", &big, vec!["copy", "big.bin"])
        .await
        .expect("write big.bin");

    // Sanity: confirm the source actually wrote the bytes through the
    // large-file path (a `_large_files/` directory must now exist).
    {
        let large_dir = src_pond.join("data").join("_large_files");
        assert!(
            large_dir.exists(),
            "_large_files/ should exist after writing a >64KiB file: {:?}",
            large_dir
        );
    }

    // 2) Attach (auto-init) + push content closure.
    add_backup_command(
        &src_ctx,
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
    .expect("backup add");
    push_command(&src_ctx, Some("origin".to_string()))
        .await
        .expect("push");

    // D7 streaming: the large blob must transfer out-of-row -- present in the
    // remote's sibling `_blobs/` store, absent from the Delta row table -- so a
    // multi-gigabyte value never bloats the remote's parquet rows.
    {
        let blobs_dir = remote_path.join("_blobs");
        assert!(
            blobs_dir.exists(),
            "remote should hold the large blob under _blobs/: {:?}",
            blobs_dir
        );
        let n_blobs = std::fs::read_dir(&blobs_dir)
            .expect("read _blobs")
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("blob="))
            .count();
        assert!(
            n_blobs >= 1,
            "expected at least one external blob, got {n_blobs}"
        );
    }

    // 3) Replica + mirror pull.
    let src_pond_id = {
        let ship = src_ctx.open_pond().await.expect("open src");
        ship.control_table().pond_id_uuid()
    };
    let _ = steward::Ship::create_replica(&dst_pond, src_pond_id)
        .await
        .expect("create dst replica");
    let dst_ctx = ctx_for(&dst_pond, vec!["pond", "pull"]);
    add_remote_command(
        &dst_ctx,
        "origin",
        &remote_url,
        "/",
        None,
        None,
        None,
        None,
        false,
        true,
    )
    .await
    .expect("re-attach origin on dst");
    pull_command(&dst_ctx, Some("origin".to_string()))
        .await
        .expect("pull origin on dst");

    // 5) The replica's `_large_files/` directory must now exist with
    //    the body blob; otherwise we have a regression of the bug.
    {
        let dst_large = dst_pond.join("data").join("_large_files");
        assert!(
            dst_large.exists(),
            "replica is missing _large_files/ (P1-BUG-LF-REPLICATION regression): {:?}",
            dst_large
        );
    }

    // 6) Byte-for-byte verification through the regular read path
    //    (which goes through OplogEntry -> find_large_file_path -> blob).
    let bytes = read_small_file(&dst_ctx, "/big.bin")
        .await
        .expect("read /big.bin on dst");
    assert_eq!(
        bytes.len(),
        big.len(),
        "replica byte length differs from source"
    );
    assert_eq!(bytes, big, "replica content differs from source");
}

/// `pond push` with no remotes is a no-op success.
#[tokio::test]
async fn pond_push_no_remotes_is_noop() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");
    push_command(&ctx, None).await.expect("push noop");
}

/// `pond pull` with no remotes is a no-op success.
#[tokio::test]
async fn pond_pull_no_remotes_is_noop() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");
    pull_command(&ctx, None).await.expect("pull noop");
}

/// D6.1: `pond verify` reports the happy path after a clean push.
///
/// Push a single small file, then call `verify`.  Because the live
/// data matches the bundle we just published, the report must come
/// back `ok=true` with `remote_latest_seq=Some(_)`.
#[tokio::test]
async fn pond_verify_ok_after_push() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let remote_path = scratch.path().join("remote_bucket");
    let remote_url = format!("file://{}", remote_path.display());

    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");
    write_small_file(&ctx, "/v.txt", b"verify me", vec!["copy", "v.txt"])
        .await
        .expect("write v.txt");

    // Pre-create the remote bucket so `backup add` is a pure attach.
    {
        let store_id = {
            let ship = ctx.open_pond().await.expect("open");
            ship.control_table().pond_id_uuid()
        };
        std::fs::create_dir_all(&remote_path).expect("mkdir remote");
        let _ = sync_store::ContentRemote::create_at_url(&remote_url, store_id, Default::default())
            .await
            .expect("create remote");
    }
    add_backup_command(
        &ctx,
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
    .expect("backup add");
    push_command(&ctx, Some("origin".to_string()))
        .await
        .expect("push");

    // Named-target form: must succeed.
    verify_command(&ctx, Some("origin".to_string()), false)
        .await
        .expect("verify origin");

    // All-targets form: must succeed too (single attachment, also OK).
    verify_command(&ctx, None, false).await.expect("verify all");
    verify_command(&ctx, Some("origin".to_string()), true)
        .await
        .expect("exact verify origin");
}

/// Content-native verify reports the exact commit-graph relationship: when the
/// remote's published tip is an ancestor of the local tip (the producer has
/// local commits it has not published), verify reports `RemoteBehind`, which is
/// still `ok` -- lag, not drift.
///
/// A Push-mode backup in `/system/run/` auto-pushes after every write, so a
/// normal local write can never stay unpushed.  To construct genuine lag we
/// write two commits with no remote attached, then publish with the remote ref
/// pointed at the *earlier* commit -- exactly the state a stalled or
/// behind-by-one publisher leaves behind.
#[tokio::test]
async fn pond_verify_reports_remote_behind_after_local_write() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let remote_path = scratch.path().join("remote_bucket");
    let remote_url = format!("file://{}", remote_path.display());

    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");
    // Two local commits, no remote attached: no auto-push fires.
    write_small_file(&ctx, "/a.txt", b"first", vec!["copy", "a.txt"])
        .await
        .expect("write a.txt");
    write_small_file(&ctx, "/b.txt", b"second", vec!["copy", "b.txt"])
        .await
        .expect("write b.txt");

    let ship = ctx.open_pond().await.expect("open");
    let local_seq = ship
        .control_table()
        .get_last_write_sequence()
        .await
        .expect("seq");

    // The previous commit (one before the local tip) becomes the remote's
    // published tip: an ancestor the local pond is exactly one commit ahead of.
    let earlier_seq = local_seq - 1;
    let earlier_hash_hex = ship
        .control_table()
        .commit_hash_at(earlier_seq)
        .await
        .expect("commit hash")
        .expect("earlier commit recorded");
    let earlier_obj_hex = ship
        .control_table()
        .commit_object_at(earlier_seq)
        .await
        .expect("commit object")
        .expect("earlier commit object recorded");
    let earlier_tip =
        sync_store::content::ObjectHash::from_hex(&earlier_hash_hex).expect("tip hash");
    let earlier_obj: Vec<u8> = (0..earlier_obj_hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&earlier_obj_hex[i..i + 2], 16).expect("hex byte"))
        .collect();

    let store_id = ship.control_table().pond_id_uuid();
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");
    let mut remote =
        sync_store::ContentRemote::create_at_url(&remote_url, store_id, Default::default())
            .await
            .expect("create remote");
    // Publish the earlier commit object and point "main" at it.  verify only
    // reads the tip ref and that one commit object, so this faithfully models a
    // remote that is behind by one commit.
    let _published_seq = remote
        .push_commit(&[(earlier_tip, earlier_obj)], "main", earlier_tip)
        .await
        .expect("publish earlier tip");

    let report =
        steward::verify_content_against_remote(ship.as_pond().expect("pond"), &remote, "main")
            .await
            .expect("verify content");
    assert!(report.ok, "remote-behind is still ok (lag, not corruption)");
    assert_eq!(
        report.state,
        steward::ContentVerifyState::RemoteBehind { local_unpushed: 1 },
        "exactly one unpushed local commit"
    );
    assert_eq!(report.remote_tip, Some(earlier_tip));
    assert_eq!(report.remote_seq, Some(earlier_seq));
}

/// A remote that serves bytes NOT hashing to the tip key it published under
/// must make `pond verify` hard-error, not report the attacker-chosen seq those
/// bytes decode to.  Models an untrusted/corrupt content remote.
#[tokio::test]
async fn pond_verify_rejects_tampered_remote_tip() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let remote_path = scratch.path().join("remote_bucket");
    let remote_url = format!("file://{}", remote_path.display());

    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");
    write_small_file(&ctx, "/a.txt", b"first", vec!["copy", "a.txt"])
        .await
        .expect("write a.txt");
    write_small_file(&ctx, "/b.txt", b"second", vec!["copy", "b.txt"])
        .await
        .expect("write b.txt");

    let ship = ctx.open_pond().await.expect("open");
    let local_seq = ship
        .control_table()
        .get_last_write_sequence()
        .await
        .expect("seq");

    // Publish the tip UNDER an earlier commit's hash key, but store the LOCAL
    // TIP's commit bytes there.  Those bytes decode to a valid commit with a
    // different seq and do NOT hash to the key -- exactly a tampered remote.
    let earlier_seq = local_seq - 1;
    let earlier_hash_hex = ship
        .control_table()
        .commit_hash_at(earlier_seq)
        .await
        .expect("commit hash")
        .expect("earlier commit recorded");
    let tip_key = sync_store::content::ObjectHash::from_hex(&earlier_hash_hex).expect("tip hash");
    let tampered_obj_hex = ship
        .control_table()
        .commit_object_at(local_seq)
        .await
        .expect("commit object")
        .expect("local tip commit object recorded");
    let tampered_bytes: Vec<u8> = (0..tampered_obj_hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&tampered_obj_hex[i..i + 2], 16).expect("hex byte"))
        .collect();

    let store_id = ship.control_table().pond_id_uuid();
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");
    let mut remote =
        sync_store::ContentRemote::create_at_url(&remote_url, store_id, Default::default())
            .await
            .expect("create remote");
    let _ = remote
        .push_commit(&[(tip_key, tampered_bytes)], "main", tip_key)
        .await
        .expect("publish tampered tip");

    let err =
        steward::verify_content_against_remote(ship.as_pond().expect("pond"), &remote, "main")
            .await
            .expect_err("tampered remote tip must be rejected");
    assert!(
        format!("{err}").contains("remote tip commit hashes to"),
        "unexpected error: {err}"
    );
}

/// `pond verify` with no remotes attached is a no-op success.
#[tokio::test]
async fn pond_verify_no_remotes_is_noop() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");
    verify_command(&ctx, None, false)
        .await
        .expect("verify noop");
}

/// D6.2: `pond status` renders cleanly on a fresh pond with no remotes.
#[tokio::test]
async fn pond_status_fresh_pond() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");
    status_command(&ctx).await.expect("status fresh");
}

/// D6.2: `pond status` renders cleanly after attaching a backup and
/// pushing (exercises identity, last_write_seq, remote enumeration,
/// and the push-watermark lag formatting).
#[tokio::test]
async fn pond_status_with_backup() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let remote_path = scratch.path().join("remote_bucket");
    let remote_url = format!("file://{}", remote_path.display());

    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");
    write_small_file(&ctx, "/s.txt", b"status", vec!["copy", "s.txt"])
        .await
        .expect("write s.txt");
    {
        let store_id = {
            let ship = ctx.open_pond().await.expect("open");
            ship.control_table().pond_id_uuid()
        };
        std::fs::create_dir_all(&remote_path).expect("mkdir remote");
        let _ = sync_store::ContentRemote::create_at_url(&remote_url, store_id, Default::default())
            .await
            .expect("create remote");
    }
    add_backup_command(
        &ctx,
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
    .expect("backup add");
    push_command(&ctx, Some("origin".to_string()))
        .await
        .expect("push");

    status_command(&ctx).await.expect("status with backup");
}

/// `pond remote add` rejects duplicate names without --overwrite.
#[tokio::test]
async fn pond_remote_add_rejects_duplicate() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    // Two distinct empty directories that `add` will auto-init as
    // fresh remote Delta tables (with our pond's pond_id as store_id).
    let remote_a = scratch.path().join("remote_a");
    let remote_b = scratch.path().join("remote_b");
    let url_a = format!("file://{}", remote_a.display());
    let url_b = format!("file://{}", remote_b.display());
    std::fs::create_dir_all(&remote_a).expect("mkdir remote_a");
    std::fs::create_dir_all(&remote_b).expect("mkdir remote_b");

    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");

    add_backup_command(
        &ctx, "origin", &url_a, false, None, None, None, None, false, false,
    )
    .await
    .expect("first add");

    let err = add_backup_command(
        &ctx, "origin", &url_b, false, None, None, None, None, false, false,
    )
    .await
    .expect_err("duplicate add should fail");
    let msg = err.to_string();
    assert!(
        msg.contains("already exists") || msg.contains("--overwrite"),
        "expected duplicate-error message, got: {}",
        msg
    );

    // --overwrite succeeds.
    add_backup_command(
        &ctx, "origin", &url_b, false, None, None, None, None, false, true, // overwrite
    )
    .await
    .expect("overwrite add");
}

/// D4.4 post-commit auto-push: a normal write transaction (no explicit
/// `pond push`) should publish to every `/sys/remotes/*` entry whose
/// mode is `push` or `both`.  We verify this by pulling on a second
/// pond and reading back the file written on src AFTER `remote add`.
#[tokio::test]
async fn post_commit_auto_push_publishes_to_file_remote() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let src_pond = scratch.path().join("src_pond");
    let dst_pond = scratch.path().join("dst_pond");
    let remote_path = scratch.path().join("remote_bucket");
    let remote_url = format!("file://{}", remote_path.display());

    // 1) Source pond.
    let src_ctx = ctx_for(&src_pond, vec!["pond", "init"]);
    init_command(&src_ctx, "test-host").await.expect("init src");

    // 2) `pond backup add origin` (push-only) auto-initializes the remote.
    add_backup_command(
        &src_ctx,
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
    .expect("remote add");

    // 3) Write a file -- the only write; post-commit dispatcher content-pushes.
    write_small_file(
        &src_ctx,
        "/auto.txt",
        b"published by auto-push",
        vec!["copy", "auto.txt"],
    )
    .await
    .expect("write auto.txt");

    // 4) Replica + mirror pull sees the auto-pushed file.
    let store_id = {
        let ship = src_ctx.open_pond().await.expect("open src");
        ship.control_table().pond_id_uuid()
    };
    let _ = steward::Ship::create_replica(&dst_pond, store_id)
        .await
        .expect("create dst replica");
    let dst_ctx = ctx_for(&dst_pond, vec!["pond", "pull"]);
    add_remote_command(
        &dst_ctx,
        "origin",
        &remote_url,
        "/",
        None,
        None,
        None,
        None,
        false,
        true,
    )
    .await
    .expect("re-attach origin on dst");
    pull_command(&dst_ctx, Some("origin".to_string()))
        .await
        .expect("pull on dst");

    let bytes = read_small_file(&dst_ctx, "/auto.txt")
        .await
        .expect("read /auto.txt on dst");
    assert_eq!(bytes, b"published by auto-push");
}

/// D4.4: a remote with mode=pull must NOT be auto-pushed to.  We
/// verify the watermark `last_pushed_seq:<url>` stays unset after a
/// write transaction.
#[tokio::test]
async fn post_commit_auto_push_skips_pull_mode_remotes() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let src_pond = scratch.path().join("src_pond");
    let remote_path = scratch.path().join("remote_bucket");
    let remote_url = format!("file://{}", remote_path.display());

    let src_ctx = ctx_for(&src_pond, vec!["pond", "init"]);
    init_command(&src_ctx, "test-host").await.expect("init src");

    // A bare URL with pull mode at root; nothing has pushed yet, so no
    // content remote exists.  Pull-add must not auto-create one.  We make
    // the dir exist so add can attach the attachment YAML.
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");

    let _ = add_remote_command(
        &src_ctx,
        "origin",
        &remote_url,
        "/", // pull-mode mirror at root
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect_err("pull-mode add on empty remote must fail");

    write_small_file(
        &src_ctx,
        "/nopush.txt",
        b"should not be auto-pushed",
        vec!["copy", "nopush.txt"],
    )
    .await
    .expect("write nopush.txt");

    // No content remote was created, so a fetch finds nothing.
    let remote = sync_store::ContentRemote::open_at_url(&remote_url, Default::default()).await;
    assert!(
        remote.is_err(),
        "pull-mode add must not auto-create a content remote"
    );
}

/// `pond remote add --mode push` against an empty directory should
/// initialize a fresh Delta table at the URL (no separate
/// `Remote::create_at_url` call required).  The subsequent push then
/// works without any manual bootstrap, which is the production CLI
/// contract: `pond init` -> `pond remote add` -> `pond push` is a
/// complete sequence.
#[tokio::test]
async fn pond_remote_add_auto_initializes_fresh_remote() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("src_pond");
    let remote_path = scratch.path().join("remote_bucket");
    let remote_url = format!("file://{}", remote_path.display());
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");

    let src_ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&src_ctx, "test-host").await.expect("init src");

    // No Remote::create_at_url call here -- add_backup_command must do it.
    add_backup_command(
        &src_ctx,
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
    .expect("remote add (auto-init)");

    // The remote should now exist as a Delta table with our pond_id
    // as its store_id.
    let expected_pond_id = {
        let ship = src_ctx.open_pond().await.expect("open src");
        ship.control_table().pond_id_uuid()
    };
    let remote = sync_store::ContentRemote::open_at_url(&remote_url, Default::default())
        .await
        .expect("open auto-initialized remote");
    assert_eq!(
        remote.pond_id(),
        expected_pond_id,
        "auto-initialized remote must carry our pond_id"
    );

    // And a subsequent push (with no pre-init) must succeed.
    write_small_file(&src_ctx, "/hello.txt", b"hello", vec!["copy", "hello.txt"])
        .await
        .expect("write hello.txt");
    // The post-commit auto-push already published; an explicit push
    // should be a no-op rather than an error.
    push_command(&src_ctx, Some("origin".to_string()))
        .await
        .expect("explicit push after auto-init");
}

/// `pond remote add --mode push` against a URL that already holds a
/// FOREIGN pond's Delta table (different store_id) must refuse.
/// Otherwise we'd silently overwrite or corrupt the foreign pond's
/// remote.
#[tokio::test]
async fn pond_remote_add_push_refuses_foreign_store_id() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("src_pond");
    let remote_path = scratch.path().join("foreign_bucket");
    let remote_url = format!("file://{}", remote_path.display());
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");

    // Pre-create the remote with a DIFFERENT pond_id (simulating a
    // foreign pond's content remote).
    let foreign_store_id = uuid::Uuid::new_v4();
    let _ =
        sync_store::ContentRemote::create_at_url(&remote_url, foreign_store_id, Default::default())
            .await
            .expect("create foreign remote");

    let src_ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&src_ctx, "test-host").await.expect("init src");

    let err = add_backup_command(
        &src_ctx,
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
    .expect_err("push-mode add against foreign store_id should fail");
    let msg = err.to_string();
    assert!(
        msg.contains("does not match"),
        "expected pond_id-mismatch error, got: {}",
        msg
    );
}

/// `pond remote add --mode pull` against an EMPTY URL must refuse:
/// a consumer cannot initialize an upstream pond; the operator must
/// set up the upstream first.
#[tokio::test]
async fn pond_remote_add_pull_refuses_empty_remote() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("dst_pond");
    let remote_path = scratch.path().join("empty_bucket");
    let remote_url = format!("file://{}", remote_path.display());
    std::fs::create_dir_all(&remote_path).expect("mkdir empty remote");

    let dst_ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&dst_ctx, "test-host").await.expect("init dst");

    let err = add_remote_command(
        &dst_ctx,
        "upstream",
        &remote_url,
        "/imports/upstream",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect_err("pull-mode add against empty remote should fail");
    let msg = err.to_string();
    assert!(
        msg.contains("not a content remote") && msg.contains("pull-mode"),
        "expected empty-remote-refusal error, got: {}",
        msg
    );
}

/// D5.7b.1: `pond remote add` persists the mount path under
/// `remote_mount_path:<name>` in raw_config; `pond backup add` leaves
/// the key empty.
#[tokio::test]
async fn pond_remote_add_persists_mount_path() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");

    // Two remotes: an upstream we'll mount as a pull, and a backup we
    // attach for push.
    let upstream_path = scratch.path().join("upstream_bucket");
    let upstream_url = format!("file://{}", upstream_path.display());
    let backup_path = scratch.path().join("backup_bucket");
    let backup_url = format!("file://{}", backup_path.display());
    std::fs::create_dir_all(&upstream_path).expect("mkdir upstream");
    std::fs::create_dir_all(&backup_path).expect("mkdir backup");

    // Initialize upstream remote with a foreign store_id so the pull
    // attach is allowed (any store_id is OK for pull).
    let foreign_id = uuid::Uuid::new_v4();
    let _ = sync_store::ContentRemote::create_at_url(&upstream_url, foreign_id, Default::default())
        .await
        .expect("init upstream");

    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");

    // Pull attach with an explicit non-root mount path.
    add_remote_command(
        &ctx,
        "upstream",
        &upstream_url,
        "/imports/upstream",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("pull attach");

    // Backup attach (push-only, no mount path).
    add_backup_command(
        &ctx,
        "origin",
        &backup_url,
        false,
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("backup attach");

    // Verify control-table state.
    let ship = ctx.open_pond().await.expect("reopen");
    let upstream_mount = ship
        .control_table()
        .raw_config_get(&format!("{REMOTE_MOUNT_PATH_PREFIX}upstream"))
        .await
        .expect("get mount key");
    assert_eq!(
        upstream_mount.as_deref(),
        Some("/imports/upstream"),
        "pull attach must record mount path"
    );
    let origin_mount = ship
        .control_table()
        .raw_config_get(&format!("{REMOTE_MOUNT_PATH_PREFIX}origin"))
        .await
        .expect("get mount key for backup");
    assert!(
        origin_mount.as_deref().is_none_or(str::is_empty),
        "backup attach must leave mount path empty, got {:?}",
        origin_mount
    );

    let upstream_mode = ship
        .control_table()
        .raw_config_get(&format!("{REMOTE_MODE_PREFIX}upstream"))
        .await
        .expect("get mode")
        .expect("mode set");
    assert_eq!(upstream_mode, "pull");
    let origin_mode = ship
        .control_table()
        .raw_config_get(&format!("{REMOTE_MODE_PREFIX}origin"))
        .await
        .expect("get mode")
        .expect("mode set");
    assert_eq!(origin_mode, "push");
}

/// D5.7b.1: `pond backup add --bidirectional` (i.e. mode=both) keeps the
/// push initialization semantics AND records mode=both for the pull
/// dispatcher.
#[tokio::test]
async fn pond_backup_add_bidirectional_records_both_mode() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let remote_path = scratch.path().join("remote_bucket");
    let remote_url = format!("file://{}", remote_path.display());
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");

    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");

    add_backup_command(
        &ctx,
        "origin",
        &remote_url,
        true, // bidirectional
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("bidirectional backup attach");

    let ship = ctx.open_pond().await.expect("reopen");
    let mode = ship
        .control_table()
        .raw_config_get(&format!("{REMOTE_MODE_PREFIX}origin"))
        .await
        .expect("get mode")
        .expect("mode set");
    assert_eq!(mode, "both", "bidirectional backup should record mode=both");
}

/// D5.7b.1: `pond remote add` rejects a non-absolute mount path.
#[tokio::test]
async fn pond_remote_add_rejects_relative_mount_path() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let remote_path = scratch.path().join("remote_bucket");
    let remote_url = format!("file://{}", remote_path.display());
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");

    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");

    let err = add_remote_command(
        &ctx,
        "upstream",
        &remote_url,
        "imports/upstream", // not absolute
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect_err("relative path should be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("must be absolute") && msg.contains("imports/upstream"),
        "expected absolute-path-required error, got: {}",
        msg
    );
}

/// D5.7b.1: `pond remote remove` clears both mode AND mount-path keys
/// from raw_config (no orphaned state in the control table).
#[tokio::test]
async fn pond_remote_remove_clears_mount_path_key() {
    use cmd::commands::remove_remote_command;
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let remote_path = scratch.path().join("remote_bucket");
    let remote_url = format!("file://{}", remote_path.display());
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");

    // Pre-init the remote with a foreign store_id (pull attach).
    let foreign_id = uuid::Uuid::new_v4();
    let _ = sync_store::ContentRemote::create_at_url(&remote_url, foreign_id, Default::default())
        .await
        .expect("create foreign remote");

    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");

    add_remote_command(
        &ctx,
        "upstream",
        &remote_url,
        "/imports/upstream",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("attach");

    remove_remote_command(&ctx, "upstream", false)
        .await
        .expect("remove");

    let ship = ctx.open_pond().await.expect("reopen");
    let mount = ship
        .control_table()
        .raw_config_get(&format!("{REMOTE_MOUNT_PATH_PREFIX}upstream"))
        .await
        .expect("get mount key");
    assert!(
        mount.as_deref().is_none_or(str::is_empty),
        "mount path key must be cleared after remove, got {:?}",
        mount
    );
}

/// D5.7b.2: `pond remote add NAME URL /` against a remote whose
/// store_id does NOT match this pond's pond_id is refused (mirror
/// restart requires matching ids).
#[tokio::test]
async fn pond_remote_add_root_path_refuses_foreign_store_id() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let remote_path = scratch.path().join("foreign_bucket");
    let remote_url = format!("file://{}", remote_path.display());
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");

    let foreign_id = uuid::Uuid::new_v4();
    let _ = sync_store::ContentRemote::create_at_url(&remote_url, foreign_id, Default::default())
        .await
        .expect("create foreign remote");

    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");

    let err = add_remote_command(
        &ctx,
        "upstream",
        &remote_url,
        "/", // mirror mode -- must match local pond_id
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect_err("mirror attach against foreign store_id should fail");
    let msg = err.to_string();
    assert!(
        msg.contains("does not match"),
        "expected mirror pond_id-mismatch error, got: {}",
        msg
    );
}

/// D5.7b.2: `pond remote add NAME URL /imports/x` against a remote
/// whose store_id MATCHES this pond's pond_id is refused (cross-pond
/// import requires a foreign pond).
#[tokio::test]
async fn pond_remote_add_nonroot_path_refuses_matching_store_id() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let remote_path = scratch.path().join("self_bucket");
    let remote_url = format!("file://{}", remote_path.display());
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");

    // Init the pond first so we know its pond_id, then create a remote
    // with the same store_id (simulating a self-mirror -- which is
    // valid for `/` but invalid for a non-root mount).
    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");
    let local_id = {
        let ship = ctx.open_pond().await.expect("open");
        ship.control_table().pond_id_uuid()
    };
    let _ = sync_store::ContentRemote::create_at_url(&remote_url, local_id, Default::default())
        .await
        .expect("create remote with our pond_id");

    let err = add_remote_command(
        &ctx,
        "upstream",
        &remote_url,
        "/imports/upstream",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect_err("cross-pond import attach against matching store_id should fail");
    let msg = err.to_string();
    assert!(
        msg.contains("cross-pond import") && msg.contains("matches"),
        "expected cross-pond-mismatch error, got: {}",
        msg
    );
}

/// D5.7b.2: end-to-end cross-pond pull materializes a directory entry
/// at the mount path that yields the foreign pond's root.
///
/// Scenario:
///   1. pond A initialized + a file written.
///   2. pond A pushes to a file:// remote.
///   3. pond B initialized (its own fresh pond_id).
///   4. pond B `pond remote add upstream file://... /imports/A`
///      (foreign store_id).
///   5. pond B `pond pull upstream`.
///   6. pond B can read /imports/A/<file> as foreign data.
#[tokio::test]
async fn cross_pond_pull_materializes_mount_entry() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let a_pond = scratch.path().join("a_pond");
    let b_pond = scratch.path().join("b_pond");
    let remote_path = scratch.path().join("a_remote");
    let remote_url = format!("file://{}", remote_path.display());

    // 1) Pond A: init + write a file.
    let a_ctx = ctx_for(&a_pond, vec!["pond", "init"]);
    init_command(&a_ctx, "test-host").await.expect("init A");
    write_small_file(
        &a_ctx,
        "/sensor.txt",
        b"data from upstream pond A",
        vec!["copy", "sensor.txt"],
    )
    .await
    .expect("write sensor.txt on A");

    // 2) Pond A pushes to its remote.
    let a_pond_id = {
        let ship = a_ctx.open_pond().await.expect("open A");
        ship.control_table().pond_id_uuid()
    };
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");
    let _ = sync_store::ContentRemote::create_at_url(&remote_url, a_pond_id, Default::default())
        .await
        .expect("create A's remote");
    add_backup_command(
        &a_ctx,
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
    .expect("backup attach on A");
    push_command(&a_ctx, Some("origin".to_string()))
        .await
        .expect("push from A");

    // 3) Pond B: init (gets its own distinct pond_id).
    let b_ctx = ctx_for(&b_pond, vec!["pond", "init"]);
    init_command(&b_ctx, "test-host").await.expect("init B");
    let b_pond_id = {
        let ship = b_ctx.open_pond().await.expect("open B");
        ship.control_table().pond_id_uuid()
    };
    assert_ne!(
        a_pond_id, b_pond_id,
        "test invariant: A and B must have different pond_ids"
    );

    // 4) Pond B attaches A's remote at /imports/upstream (cross-pond
    //    import; foreign store_id required).
    add_remote_command(
        &b_ctx,
        "upstream",
        &remote_url,
        "/imports/upstream",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("cross-pond attach on B");

    // 5) Pond B pulls from upstream -- this triggers mount
    //    materialization in pull_one.
    pull_command(&b_ctx, Some("upstream".to_string()))
        .await
        .expect("pull upstream on B");

    // 6) The mount entry should be readable on B.  Read the foreign
    //    file via /imports/upstream/sensor.txt.
    let bytes = read_small_file(&b_ctx, "/imports/upstream/sensor.txt")
        .await
        .expect("read foreign file through mount");
    assert_eq!(bytes, b"data from upstream pond A");

    // 7) The mount entry itself carries A's pond_id (not B's).  This
    //    is what makes subsequent navigation through it resolve in
    //    A's namespace.
    {
        use steward::PondUserMetadata;
        let mut ship = b_ctx.open_pond().await.expect("reopen B");
        let tx = ship
            .begin_read(&PondUserMetadata::new(vec!["verify".to_string()]))
            .await
            .expect("begin read");
        {
            let fs = &*tx;
            let root = fs.root().await.expect("root B");
            let imports = root.open_dir_path("/imports").await.expect("open /imports");
            let entry = imports
                .get("upstream")
                .await
                .expect("get upstream")
                .expect("upstream entry present");
            let entry_pond = entry.node.id().pond_id();
            let a_uuid7 = uuid7::Uuid::from(*a_pond_id.as_bytes());
            assert_eq!(
                entry_pond, a_uuid7,
                "mount entry must carry foreign pond_id"
            );
        }
        let _ = tx.commit().await.expect("commit read");
    }

    // 8) A second pull is idempotent (no error, no duplicate mount).
    pull_command(&b_ctx, Some("upstream".to_string()))
        .await
        .expect("second pull is idempotent");
    let bytes2 = read_small_file(&b_ctx, "/imports/upstream/sensor.txt")
        .await
        .expect("read foreign file after second pull");
    assert_eq!(bytes2, b"data from upstream pond A");
}

/// Per-pond_id seq allocator: importing a foreign pond's high-seq
/// bundles must NOT inflate the LOCAL pond's own seq space.  Before the
/// per-pond_id allocator, `apply_pulled_bundle` bumped a single global
/// `last_txn_seq` to the foreign frontier, inflating the local pond's
/// seq numbering and leaving gaps in its history.  This pins the
/// decoupled behavior: a cross-pond import leaves the local allocator
/// untouched, and a subsequent local write takes the next CONTIGUOUS
/// local seq -- not `foreign_frontier + 1`.
#[tokio::test]
async fn cross_pond_pull_does_not_inflate_local_seq() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let a_pond = scratch.path().join("a_pond");
    let b_pond = scratch.path().join("b_pond");
    let remote_path = scratch.path().join("a_remote");
    let remote_url = format!("file://{}", remote_path.display());

    // 1) Pond A: init (seq 1) + four writes (seq 2..5) so A's frontier is high.
    let a_ctx = ctx_for(&a_pond, vec!["pond", "init"]);
    init_command(&a_ctx, "test-host").await.expect("init A");
    for i in 0..4 {
        write_small_file(
            &a_ctx,
            &format!("/f{i}.txt"),
            format!("payload {i}").as_bytes(),
            vec!["copy", "f.txt"],
        )
        .await
        .expect("write on A");
    }
    let a_pond_id = {
        let ship = a_ctx.open_pond().await.expect("open A");
        ship.control_table().pond_id_uuid()
    };
    let a_seq = {
        let mut ship = a_ctx.open_pond().await.expect("open A");
        ship.as_pond_mut().expect("pond steward").last_write_seq()
    };
    assert!(a_seq >= 5, "A's frontier should be high, got {a_seq}");

    // 2) Pond A pushes to its remote.
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");
    let _ = sync_store::ContentRemote::create_at_url(&remote_url, a_pond_id, Default::default())
        .await
        .expect("create A's remote");
    add_backup_command(
        &a_ctx,
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
    .expect("backup attach on A");
    push_command(&a_ctx, Some("origin".to_string()))
        .await
        .expect("push from A");

    // 3) Pond B: init -> local seq 1.
    let b_ctx = ctx_for(&b_pond, vec!["pond", "init"]);
    init_command(&b_ctx, "test-host").await.expect("init B");
    let b_seq_before = {
        let mut ship = b_ctx.open_pond().await.expect("open B");
        ship.as_pond_mut().expect("pond steward").last_write_seq()
    };
    assert_eq!(b_seq_before, 1, "B starts at local seq 1");

    // 4) B attaches A's remote and pulls A's high-seq bundles.
    add_remote_command(
        &b_ctx,
        "upstream",
        &remote_url,
        "/imports/upstream",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("cross-pond attach on B");
    pull_command(&b_ctx, Some("upstream".to_string()))
        .await
        .expect("pull upstream on B");

    // 5) The import must NOT inflate B's local allocator to A's frontier.
    //    B's local seq only advances for B's OWN local writes (the remote
    //    config write + the pull's mount materialization), staying well
    //    below A's foreign frontier.  Before the per-pond_id allocator,
    //    `apply_pulled_bundle` bumped the global seq to A's frontier, so
    //    this value would have been >= a_seq.
    let b_seq_after_pull = {
        let mut ship = b_ctx.open_pond().await.expect("reopen B");
        ship.as_pond_mut().expect("pond steward").last_write_seq()
    };
    assert!(
        b_seq_after_pull < a_seq,
        "cross-pond import must not inflate B's local seq to A's frontier \
         (got {b_seq_after_pull}, A frontier {a_seq})"
    );

    // Sanity: imported content is readable through the mount.
    let bytes = read_small_file(&b_ctx, "/imports/upstream/f0.txt")
        .await
        .expect("read foreign file");
    assert_eq!(bytes, b"payload 0");

    // 6) A subsequent LOCAL write on B takes the next CONTIGUOUS local seq
    //    (b_seq_after_pull + 1), not the foreign frontier + 1.
    write_small_file(
        &b_ctx,
        "/local.txt",
        b"B local data",
        vec!["copy", "local.txt"],
    )
    .await
    .expect("local write on B");
    let b_seq_after_write = {
        let mut ship = b_ctx.open_pond().await.expect("reopen B");
        ship.as_pond_mut().expect("pond steward").last_write_seq()
    };
    assert_eq!(
        b_seq_after_write,
        b_seq_after_pull + 1,
        "B's local write must take the next contiguous local seq, not foreign frontier+1"
    );
}

/// D5.7b.3: writes anywhere inside a foreign mount must be refused
/// with `ReadOnlyImport`.  This covers create_file_path,
/// async_writer_path, create_dir_path, rename, remove, and insert_node.
#[tokio::test]
async fn foreign_mount_writes_are_refused() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let a_pond = scratch.path().join("a_pond");
    let b_pond = scratch.path().join("b_pond");
    let remote_path = scratch.path().join("a_remote");
    let remote_url = format!("file://{}", remote_path.display());

    // Set up A with a small file and push to remote.
    let a_ctx = ctx_for(&a_pond, vec!["pond", "init"]);
    init_command(&a_ctx, "test-host").await.expect("init A");
    write_small_file(&a_ctx, "/data.txt", b"upstream", vec!["copy", "data.txt"])
        .await
        .expect("write data.txt on A");
    let a_pond_id = {
        let ship = a_ctx.open_pond().await.expect("open A");
        ship.control_table().pond_id_uuid()
    };
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");
    let _ = sync_store::ContentRemote::create_at_url(&remote_url, a_pond_id, Default::default())
        .await
        .expect("create A's remote");
    add_backup_command(
        &a_ctx,
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
    .expect("backup attach on A");
    push_command(&a_ctx, Some("origin".to_string()))
        .await
        .expect("push from A");

    // B pulls A as a cross-pond import.
    let b_ctx = ctx_for(&b_pond, vec!["pond", "init"]);
    init_command(&b_ctx, "test-host").await.expect("init B");
    add_remote_command(
        &b_ctx,
        "upstream",
        &remote_url,
        "/imports/upstream",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("cross-pond attach on B");
    pull_command(&b_ctx, Some("upstream".to_string()))
        .await
        .expect("pull upstream on B");

    // Attempt to write inside the foreign mount.  This must error
    // with ReadOnlyImport.  We use a fresh write transaction and
    // assert the closure returns an Err that surfaces as Aborted
    // (the write_transaction wraps the inner error).
    let write_result = {
        let mut ship = b_ctx.open_pond().await.expect("reopen B");
        ship.write_transaction(
            &PondUserMetadata::new(vec!["test".to_string(), "foreign-write".to_string()]),
            async |fs| {
                let root = fs.root().await?;
                let res = root
                    .async_writer_path_with_type(
                        "/imports/upstream/should_fail.txt",
                        EntryType::FilePhysicalVersion,
                    )
                    .await;
                match res {
                    Ok(_) => Err(steward::StewardError::Aborted(
                        "expected foreign write to be refused".to_string(),
                    )),
                    Err(tinyfs::Error::ReadOnlyImport(_)) => {
                        // Force the transaction to abort so we don't
                        // accidentally commit any unrelated state.
                        Err(steward::StewardError::Aborted(
                            "read_only_import".to_string(),
                        ))
                    }
                    Err(other) => Err(steward::StewardError::Aborted(format!(
                        "wrong error variant: {:?}",
                        other
                    ))),
                }
            },
        )
        .await
    };

    match write_result {
        Err(steward::StewardError::Aborted(msg)) if msg == "read_only_import" => {}
        Err(other) => panic!("unexpected error: {:?}", other),
        Ok(()) => panic!("expected foreign write to be refused"),
    }

    // Sanity check: the original foreign file is still intact.
    let bytes = read_small_file(&b_ctx, "/imports/upstream/data.txt")
        .await
        .expect("foreign file still readable");
    assert_eq!(bytes, b"upstream");
}

/// D5.7b.3: post-commit auto-exec scanning of /system/run/* must
/// skip entries that live under a foreign mount.  We install a real
/// factory at A's /system/run/scratch, cross-pond-import A into B,
/// then perform a local write on B and assert post-commit didn't
/// try to execute the foreign factory.  Without the filter, the
/// foreign factory's discovery code path would log warnings about
/// "No factory associated with config" or attempt execution with
/// the wrong pond_id; with the filter, the foreign entry is silently
/// skipped.
#[tokio::test]
async fn foreign_post_commit_factories_skipped_in_auto_exec() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let a_pond = scratch.path().join("a_pond");
    let b_pond = scratch.path().join("b_pond");
    let remote_path = scratch.path().join("a_remote");
    let remote_url = format!("file://{}", remote_path.display());
    let cfg_dir = scratch.path().join("cfg");
    std::fs::create_dir_all(&cfg_dir).expect("mkdir cfg");

    // Write a tiny sql-derived-table config for use with mknod.
    let factory_cfg = cfg_dir.join("scratch.yaml");
    std::fs::write(
        &factory_cfg,
        "patterns:\n  source: \"table:///data/*.table\"\nquery: \"SELECT 1\"\n",
    )
    .expect("write factory config");

    // 1) Pond A: init, attach a push backup, push, then install a
    //    factory at /system/run/scratch via mknod so the
    //    /system/run/ directory has a real dynamic-node entry that
    //    cross-pond import will replicate to B.
    let a_ctx = ctx_for(&a_pond, vec!["pond", "init"]);
    init_command(&a_ctx, "test-host").await.expect("init A");
    let a_pond_id = {
        let ship = a_ctx.open_pond().await.expect("open A");
        ship.control_table().pond_id_uuid()
    };
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");
    let _ = sync_store::ContentRemote::create_at_url(&remote_url, a_pond_id, Default::default())
        .await
        .expect("create A's remote");
    add_backup_command(
        &a_ctx,
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
    .expect("backup attach on A");
    // Install /system/run/scratch on A (the directory will be auto-created).
    cmd::commands::mkdir_command(&a_ctx, "/system/run", true)
        .await
        .expect("mkdir -p /system/run on A");
    cmd::commands::mknod_command(
        &a_ctx,
        "sql-derived-table",
        "/system/run/scratch",
        factory_cfg.to_str().expect("cfg path utf-8"),
        false,
    )
    .await
    .expect("mknod /system/run/scratch on A");
    // Trigger one more write to push the factory entry through to remote.
    write_small_file(
        &a_ctx,
        "/from_a.txt",
        b"hello from A",
        vec!["copy", "from_a.txt"],
    )
    .await
    .expect("write from_a.txt on A (also auto-pushes everything)");

    // 2) Pond B: init.
    let b_ctx = ctx_for(&b_pond, vec!["pond", "init"]);
    init_command(&b_ctx, "test-host").await.expect("init B");

    // 3) B cross-pond-imports A at /imports/upstream.
    add_remote_command(
        &b_ctx,
        "upstream",
        &remote_url,
        "/imports/upstream",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("cross-pond attach on B");
    pull_command(&b_ctx, Some("upstream".to_string()))
        .await
        .expect("pull upstream on B");

    // 4) Make a local write on B.  The post-commit /system/run/*
    //    scan WILL discover /imports/upstream/system/run/scratch
    //    (it's a real factory config).  Without the local-pond
    //    filter, the scan would attempt to instantiate it under
    //    B's transaction with A's pond_id, breaking the invariant
    //    that auto-exec runs only local-pond factories.  With the
    //    filter, the foreign entry is silently skipped and the
    //    write succeeds cleanly.
    write_small_file(
        &b_ctx,
        "/local_b.txt",
        b"hello from B",
        vec!["copy", "local_b.txt"],
    )
    .await
    .expect("local write on B must succeed (foreign factory skipped)");

    // 5) Sanity: the foreign factory config IS visible by explicit
    //    path traversal.  This proves the entry exists; the test's
    //    value is that step 4 succeeded despite it.
    use steward::PondUserMetadata;
    let mut ship = b_ctx.open_pond().await.expect("reopen B");
    let tx = ship
        .begin_read(&PondUserMetadata::new(vec![
            "verify-foreign-run".to_string(),
        ]))
        .await
        .expect("begin read");
    {
        let fs = &*tx;
        let root = fs.root().await.expect("root B");
        let dir = root
            .open_dir_path("/imports/upstream/system/run")
            .await
            .expect("open foreign /system/run");
        let entry = dir
            .get("scratch")
            .await
            .expect("get scratch")
            .expect("foreign scratch factory present");
        // The foreign entry MUST carry A's pond_id (this is exactly
        // what the auto-exec filter keys on).
        let a_uuid7 = uuid7::Uuid::from(*a_pond_id.as_bytes());
        assert_eq!(
            entry.node.id().pond_id(),
            a_uuid7,
            "foreign factory entry must carry A's pond_id"
        );
    }
    let _ = tx.commit().await.expect("commit read");
}

/// D5.7b.4: `pond remote remove NAME` defaults to **detach** for
/// cross-pond imports: the YAML config and watermark keys go away,
/// but the mount entry at `mount_path` is preserved so the imported
/// data snapshot stays readable.
#[tokio::test]
async fn pond_remote_remove_detach_preserves_mount_entry() {
    use cmd::commands::remove_remote_command;
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let a_pond = scratch.path().join("a_pond");
    let b_pond = scratch.path().join("b_pond");
    let remote_path = scratch.path().join("a_remote");
    let remote_url = format!("file://{}", remote_path.display());

    // Set up A with a file, push to remote.
    let a_ctx = ctx_for(&a_pond, vec!["pond", "init"]);
    init_command(&a_ctx, "test-host").await.expect("init A");
    write_small_file(&a_ctx, "/blob.txt", b"keep me", vec!["copy", "blob.txt"])
        .await
        .expect("write blob.txt on A");
    let a_pond_id = {
        let ship = a_ctx.open_pond().await.expect("open A");
        ship.control_table().pond_id_uuid()
    };
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");
    let _ = sync_store::ContentRemote::create_at_url(&remote_url, a_pond_id, Default::default())
        .await
        .expect("create A's remote");
    add_backup_command(
        &a_ctx,
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
    .expect("backup attach on A");
    push_command(&a_ctx, Some("origin".to_string()))
        .await
        .expect("push from A");

    // B cross-pond-imports A and pulls.
    let b_ctx = ctx_for(&b_pond, vec!["pond", "init"]);
    init_command(&b_ctx, "test-host").await.expect("init B");
    add_remote_command(
        &b_ctx,
        "upstream",
        &remote_url,
        "/imports/upstream",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("cross-pond attach on B");
    pull_command(&b_ctx, Some("upstream".to_string()))
        .await
        .expect("pull upstream on B");

    // Sanity: imported file is readable.
    let bytes = read_small_file(&b_ctx, "/imports/upstream/blob.txt")
        .await
        .expect("read imported blob.txt");
    assert_eq!(bytes, b"keep me");

    // Default remove = detach (purge=false).
    remove_remote_command(&b_ctx, "upstream", false)
        .await
        .expect("remove detach");

    // Config and watermarks are cleared.
    let cfg_path = remote_config_path("upstream");
    {
        let mut ship = b_ctx.open_pond().await.expect("reopen B");
        let tx = ship
            .begin_read(&PondUserMetadata::new(vec!["verify-detach".to_string()]))
            .await
            .expect("begin read");
        {
            let fs = &*tx;
            let root = fs.root().await.expect("root B");
            assert!(
                !root.exists(&cfg_path).await,
                "/sys/remotes/upstream must be gone"
            );
        }
        let _ = tx.commit().await.expect("commit read");
    }
    let ship = b_ctx.open_pond().await.expect("reopen B for watermark");
    let mount_key = ship
        .control_table()
        .raw_config_get(&format!("{REMOTE_MOUNT_PATH_PREFIX}upstream"))
        .await
        .expect("get mount key");
    assert!(
        mount_key.as_deref().is_none_or(str::is_empty),
        "mount path key must be cleared after detach, got {:?}",
        mount_key
    );

    // But the imported data snapshot is STILL readable through the
    // preserved mount entry.  This is the defining property of detach.
    let bytes = read_small_file(&b_ctx, "/imports/upstream/blob.txt")
        .await
        .expect("read imported blob.txt after detach");
    assert_eq!(bytes, b"keep me");
}

/// D5.7b.4: `pond remote remove NAME --purge` removes both the
/// attachment config AND the mount entry at `mount_path`.  After
/// purge, the imported data is no longer reachable by path.
#[tokio::test]
async fn pond_remote_remove_purge_drops_mount_entry() {
    use cmd::commands::remove_remote_command;
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let a_pond = scratch.path().join("a_pond");
    let b_pond = scratch.path().join("b_pond");
    let remote_path = scratch.path().join("a_remote");
    let remote_url = format!("file://{}", remote_path.display());

    let a_ctx = ctx_for(&a_pond, vec!["pond", "init"]);
    init_command(&a_ctx, "test-host").await.expect("init A");
    write_small_file(&a_ctx, "/blob.txt", b"keep me", vec!["copy", "blob.txt"])
        .await
        .expect("write blob.txt on A");
    let a_pond_id = {
        let ship = a_ctx.open_pond().await.expect("open A");
        ship.control_table().pond_id_uuid()
    };
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");
    let _ = sync_store::ContentRemote::create_at_url(&remote_url, a_pond_id, Default::default())
        .await
        .expect("create A's remote");
    add_backup_command(
        &a_ctx,
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
    .expect("backup attach on A");
    push_command(&a_ctx, Some("origin".to_string()))
        .await
        .expect("push from A");

    let b_ctx = ctx_for(&b_pond, vec!["pond", "init"]);
    init_command(&b_ctx, "test-host").await.expect("init B");
    add_remote_command(
        &b_ctx,
        "upstream",
        &remote_url,
        "/imports/upstream",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("cross-pond attach on B");
    pull_command(&b_ctx, Some("upstream".to_string()))
        .await
        .expect("pull upstream on B");

    // Sanity.
    let bytes = read_small_file(&b_ctx, "/imports/upstream/blob.txt")
        .await
        .expect("read imported blob.txt");
    assert_eq!(bytes, b"keep me");

    // Remove with --purge.
    remove_remote_command(&b_ctx, "upstream", true)
        .await
        .expect("remove purge");

    // Config gone, AND mount entry gone.
    let cfg_path = remote_config_path("upstream");
    let mut ship = b_ctx.open_pond().await.expect("reopen B");
    let tx = ship
        .begin_read(&PondUserMetadata::new(vec!["verify-purge".to_string()]))
        .await
        .expect("begin read");
    {
        let fs = &*tx;
        let root = fs.root().await.expect("root B");
        assert!(
            !root.exists(&cfg_path).await,
            "/sys/remotes/upstream must be gone after purge"
        );
        assert!(
            !root.exists("/imports/upstream").await,
            "/imports/upstream mount entry must be gone after purge"
        );
    }
    let _ = tx.commit().await.expect("commit read");

    // Reading through the purged path now errors.
    let result = read_small_file(&b_ctx, "/imports/upstream/blob.txt").await;
    assert!(
        result.is_err(),
        "reading purged mount must error; got Ok({:?})",
        result.ok()
    );
}

/// D5.7b.4: `--purge` on a backup-mode (push-only) attachment is a
/// no-op for the mount (backups have no mount_path) but the config
/// and watermarks still get cleared.  This documents the symmetry.
#[tokio::test]
async fn pond_backup_remove_purge_is_no_op_for_mount() {
    use cmd::commands::remove_remote_command;
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let remote_path = scratch.path().join("remote_bucket");
    let remote_url = format!("file://{}", remote_path.display());

    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");
    let pond_id = {
        let ship = ctx.open_pond().await.expect("open");
        ship.control_table().pond_id_uuid()
    };
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");
    let _ = sync_store::ContentRemote::create_at_url(&remote_url, pond_id, Default::default())
        .await
        .expect("create remote");
    add_backup_command(
        &ctx,
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
    .expect("backup attach");

    // Purge a backup: succeeds; no mount entry to remove.
    remove_remote_command(&ctx, "origin", true)
        .await
        .expect("remove --purge on backup must succeed");

    let cfg_path = remote_config_path("origin");
    let mut ship = ctx.open_pond().await.expect("reopen");
    let tx = ship
        .begin_read(&PondUserMetadata::new(vec![
            "verify-backup-purge".to_string(),
        ]))
        .await
        .expect("begin read");
    {
        let fs = &*tx;
        let root = fs.root().await.expect("root");
        assert!(
            !root.exists(&cfg_path).await,
            "/sys/remotes/origin must be gone after purge"
        );
    }
    let _ = tx.commit().await.expect("commit read");
}

/// D5.7b.5: two pull-mode remotes cannot share the same mount_path.
#[tokio::test]
async fn pond_remote_add_refuses_duplicate_mount_path() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let remote_a = scratch.path().join("remote_a");
    let remote_b = scratch.path().join("remote_b");
    let url_a = format!("file://{}", remote_a.display());
    let url_b = format!("file://{}", remote_b.display());
    std::fs::create_dir_all(&remote_a).expect("mkdir a");
    std::fs::create_dir_all(&remote_b).expect("mkdir b");

    // Two distinct foreign upstreams.
    let foreign_a = uuid::Uuid::new_v4();
    let foreign_b = uuid::Uuid::new_v4();
    let _ = sync_store::ContentRemote::create_at_url(&url_a, foreign_a, Default::default())
        .await
        .expect("create remote a");
    let _ = sync_store::ContentRemote::create_at_url(&url_b, foreign_b, Default::default())
        .await
        .expect("create remote b");

    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");

    add_remote_command(
        &ctx,
        "first",
        &url_a,
        "/imports/shared",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("first attach");

    let err = add_remote_command(
        &ctx,
        "second",
        &url_b,
        "/imports/shared",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect_err("second attach with same mount_path must fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("mount path") && msg.contains("already used"),
        "error must explain mount-path collision; got: {msg}"
    );
}

/// D5.7b.5: trailing slashes don't sneak past the mount-path
/// duplicate check.  Adding `/imports/shared/` after `/imports/shared`
/// must also fail.
#[tokio::test]
async fn pond_remote_add_refuses_duplicate_mount_path_trailing_slash() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let remote_a = scratch.path().join("remote_a");
    let remote_b = scratch.path().join("remote_b");
    let url_a = format!("file://{}", remote_a.display());
    let url_b = format!("file://{}", remote_b.display());
    std::fs::create_dir_all(&remote_a).expect("mkdir a");
    std::fs::create_dir_all(&remote_b).expect("mkdir b");

    let foreign_a = uuid::Uuid::new_v4();
    let foreign_b = uuid::Uuid::new_v4();
    let _ = sync_store::ContentRemote::create_at_url(&url_a, foreign_a, Default::default())
        .await
        .expect("create remote a");
    let _ = sync_store::ContentRemote::create_at_url(&url_b, foreign_b, Default::default())
        .await
        .expect("create remote b");

    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");

    add_remote_command(
        &ctx,
        "first",
        &url_a,
        "/imports/shared",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("first attach");

    let err = add_remote_command(
        &ctx,
        "second",
        &url_b,
        "/imports/shared/",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect_err("trailing-slash variant must also be detected");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("mount path"),
        "expected mount-path collision error; got: {msg}"
    );
}

/// D5.7b.5: two different pull-mode names cannot point at the same
/// foreign store_id.
#[tokio::test]
async fn pond_remote_add_refuses_duplicate_foreign_store_id() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let remote_path = scratch.path().join("remote_bucket");
    let url = format!("file://{}", remote_path.display());
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");

    let foreign = uuid::Uuid::new_v4();
    let _ = sync_store::ContentRemote::create_at_url(&url, foreign, Default::default())
        .await
        .expect("create remote");

    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");

    add_remote_command(
        &ctx,
        "first",
        &url,
        "/imports/first",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("first attach");

    // Same URL (same store_id) under a different name + mount path.
    let err = add_remote_command(
        &ctx,
        "duplicate",
        &url,
        "/imports/duplicate",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect_err("second attach with same store_id must fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("already mounted") && msg.contains("store_id"),
        "error must explain store_id collision; got: {msg}"
    );
}

/// D5.7b.5: --overwrite of the same NAME still works (no false
/// positive in the duplicate-path check).
#[tokio::test]
async fn pond_remote_add_overwrite_same_name_same_path_succeeds() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let pond_path = scratch.path().join("pond");
    let remote_path = scratch.path().join("remote_bucket");
    let url = format!("file://{}", remote_path.display());
    std::fs::create_dir_all(&remote_path).expect("mkdir remote");

    let foreign = uuid::Uuid::new_v4();
    let _ = sync_store::ContentRemote::create_at_url(&url, foreign, Default::default())
        .await
        .expect("create remote");

    let ctx = ctx_for(&pond_path, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");

    add_remote_command(
        &ctx,
        "upstream",
        &url,
        "/imports/upstream",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("first attach");

    add_remote_command(
        &ctx,
        "upstream",
        &url,
        "/imports/upstream",
        None,
        None,
        None,
        None,
        false,
        true,
    )
    .await
    .expect("--overwrite of same name + same path must succeed");
}

/// D5.8.9 / docs/d5.8-resume.md step 7: 3-deep cross-pond chain pins the
/// non-transitive replication invariant.
///
/// Setup:
///
/// ```text
///   Pond A  --push-->  file://A_remote
///                          ^
///                          | pull (mount at /imports/A)
///   Pond B  --push-->  file://B_remote
///                          ^
///                          | pull (mount at /imports/B)
///   Pond C
/// ```
///
/// `steward::remote_adapter::actions_at_version` filters outbound
/// bundles to rows where `partition_values["pond_id"]` equals the
/// pushing pond's UUID.  When B pushes to `B_remote`, the foreign
/// mount-entry row that materialized `/imports/A` (which carries A's
/// pond_id) is intentionally excluded.  Therefore:
///
///   - C MUST be able to read B's local content via `/imports/B/...`.
///   - C MUST NOT see A's content via `/imports/B/imports/A/...`.
///   - `/imports/B/imports` may still appear as a child entry in C
///     (B-owned `create_dir_all` parent), but the foreign child row
///     under it is not pushed.
///
/// If anyone removes the pond_id filter in `actions_at_version` without
/// a deliberate cross-pond-transitivity design change, this test breaks
/// loudly.  Shell-level coverage lives in
/// `testsuite/tests/531-recursive-cross-pond-import.sh`.
#[tokio::test]
async fn cross_pond_3deep_does_not_re_replicate_foreign_mount() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let a_pond = scratch.path().join("a_pond");
    let b_pond = scratch.path().join("b_pond");
    let c_pond = scratch.path().join("c_pond");
    let a_remote_path = scratch.path().join("a_remote");
    let b_remote_path = scratch.path().join("b_remote");
    let a_url = format!("file://{}", a_remote_path.display());
    let b_url = format!("file://{}", b_remote_path.display());

    // --- Pond A: init + write + push to A's bucket. -----------------
    let a_ctx = ctx_for(&a_pond, vec!["pond", "init"]);
    init_command(&a_ctx, "test-host").await.expect("init A");
    write_small_file(&a_ctx, "/a.txt", b"from pond A", vec!["copy", "a.txt"])
        .await
        .expect("write a.txt on A");
    let a_pond_id = {
        let ship = a_ctx.open_pond().await.expect("open A");
        ship.control_table().pond_id_uuid()
    };
    std::fs::create_dir_all(&a_remote_path).expect("mkdir a_remote");
    let _ = sync_store::ContentRemote::create_at_url(&a_url, a_pond_id, Default::default())
        .await
        .expect("create A's remote");
    add_backup_command(
        &a_ctx, "originA", &a_url, false, None, None, None, None, false, false,
    )
    .await
    .expect("backup attach on A");
    push_command(&a_ctx, Some("originA".to_string()))
        .await
        .expect("push from A");

    // --- Pond B: init + mount A + pull + own write + push to B. -----
    let b_ctx = ctx_for(&b_pond, vec!["pond", "init"]);
    init_command(&b_ctx, "test-host").await.expect("init B");
    add_remote_command(
        &b_ctx,
        "upstreamA",
        &a_url,
        "/imports/A",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("cross-pond attach A on B");
    pull_command(&b_ctx, Some("upstreamA".to_string()))
        .await
        .expect("pull A on B");
    let b_pond_id = {
        let ship = b_ctx.open_pond().await.expect("open B");
        ship.control_table().pond_id_uuid()
    };
    assert_ne!(a_pond_id, b_pond_id, "A and B must have distinct pond_ids");

    // Sanity: B reads A through its mount (2-deep works).
    let b_view_of_a = read_small_file(&b_ctx, "/imports/A/a.txt")
        .await
        .expect("B reads A via mount");
    assert_eq!(b_view_of_a, b"from pond A");

    write_small_file(&b_ctx, "/b.txt", b"from pond B", vec!["copy", "b.txt"])
        .await
        .expect("write b.txt on B");
    std::fs::create_dir_all(&b_remote_path).expect("mkdir b_remote");
    let _ = sync_store::ContentRemote::create_at_url(&b_url, b_pond_id, Default::default())
        .await
        .expect("create B's remote");
    add_backup_command(
        &b_ctx, "originB", &b_url, false, None, None, None, None, false, false,
    )
    .await
    .expect("backup attach on B");
    push_command(&b_ctx, Some("originB".to_string()))
        .await
        .expect("push from B");

    // --- Pond C: init + mount B + pull. ------------------------------
    let c_ctx = ctx_for(&c_pond, vec!["pond", "init"]);
    init_command(&c_ctx, "test-host").await.expect("init C");
    let c_pond_id = {
        let ship = c_ctx.open_pond().await.expect("open C");
        ship.control_table().pond_id_uuid()
    };
    assert_ne!(b_pond_id, c_pond_id, "B and C must have distinct pond_ids");
    assert_ne!(a_pond_id, c_pond_id, "A and C must have distinct pond_ids");

    add_remote_command(
        &c_ctx,
        "upstreamB",
        &b_url,
        "/imports/B",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("cross-pond attach B on C");
    pull_command(&c_ctx, Some("upstreamB".to_string()))
        .await
        .expect("pull B on C");

    // --- INVARIANT 1: C reads B's local content. --------------------
    let c_view_of_b = read_small_file(&c_ctx, "/imports/B/b.txt")
        .await
        .expect("C reads B's local /b.txt through /imports/B");
    assert_eq!(c_view_of_b, b"from pond B");

    // --- INVARIANT 2: 3-deep transitivity is BLOCKED. ---------------
    // Reading A's content through B's mount must fail on C.  We try a
    // few resolution paths (the foreign sub-mount is filtered out at
    // various levels) and assert NONE of them succeed.
    {
        let err = read_small_file(&c_ctx, "/imports/B/imports/A/a.txt")
            .await
            .expect_err("C must NOT read A through /imports/B/imports/A (3-deep blocked)");
        let msg = format!("{err:#}");
        log::info!("expected 3-deep read failure: {msg}");
    }

    // --- INVARIANT 3: A's pond_id never surfaces in C's tree. -------
    // The mount-entry row for /imports/B/imports/A carries A's pond_id;
    // it should have been filtered out of B's push and so should NOT
    // be locatable as a child entry of /imports/B/imports on C.
    {
        use steward::PondUserMetadata;
        let mut ship = c_ctx.open_pond().await.expect("reopen C");
        let tx = ship
            .begin_read(&PondUserMetadata::new(vec![
                "verify".to_string(),
                "3deep-non-transitive".to_string(),
            ]))
            .await
            .expect("begin read");
        {
            let fs = &*tx;
            let root = fs.root().await.expect("root C");
            // /imports/B is the foreign-root mount; pond_id == B's.
            let imports_b = root
                .open_dir_path("/imports/B")
                .await
                .expect("open /imports/B");
            let imports_b_entry = root
                .get_node_path("/imports/B")
                .await
                .expect("locate /imports/B");
            let a_uuid7 = uuid7::Uuid::from(*a_pond_id.as_bytes());
            let b_uuid7 = uuid7::Uuid::from(*b_pond_id.as_bytes());
            assert_eq!(
                imports_b_entry.node.id().pond_id(),
                b_uuid7,
                "/imports/B itself must carry B's pond_id"
            );

            // /imports/B/imports IS present as a B-owned dir entry
            // (B materialized it via create_dir_all when attaching A).
            let inner_imports_entry = imports_b
                .get("imports")
                .await
                .expect("get /imports/B/imports");
            assert!(
                inner_imports_entry.is_some(),
                "the B-owned /imports parent is replicated"
            );
            let inner_imports = inner_imports_entry.expect("present");
            assert_eq!(
                inner_imports.node.id().pond_id(),
                b_uuid7,
                "the inner /imports dir must carry B's pond_id, not A's"
            );

            // The /imports/B/imports/A foreign mount-entry row was
            // filtered out of B's push (its pond_id was A's, not B's).
            // Either the body of /imports/B/imports is unresolvable on
            // C (no partition data) or the 'A' child lookup returns
            // None.  Both outcomes are acceptable; both pin the
            // non-transitivity invariant.
            let inner_dir_result = imports_b.open_dir_path("imports").await;
            let a_absent = match inner_dir_result {
                Err(_) => true, // listing failed: partition not pushed
                Ok(inner_dir) => match inner_dir.get("A").await {
                    Ok(None) => true, // entry not present
                    Err(_) => true,   // lookup failed
                    Ok(Some(entry)) => {
                        // If somehow the entry IS present, its pond_id
                        // MUST NOT be A's -- that would mean A's data
                        // had been re-replicated through B (i.e. the
                        // filter was broken).
                        entry.node.id().pond_id() != a_uuid7
                    }
                },
            };
            assert!(
                a_absent,
                "/imports/B/imports/A must NOT carry A's pond_id (would mean push filter is broken)"
            );
        }
        let _ = tx.commit().await.expect("commit read");
    }

    // --- INVARIANT 4: A and B remain fully usable. ------------------
    // Reading A through B's mount still works after C pulled.
    let b_view_of_a_after = read_small_file(&b_ctx, "/imports/A/a.txt")
        .await
        .expect("B still reads A after C's pull");
    assert_eq!(b_view_of_a_after, b"from pond A");

    // --- INVARIANT 5: the graft pin is a replicated content reference.
    // B records `/sys/grafts/upstreamA` pinning A's tip; that file is
    // B-owned content, so it is covered by B's commit and replicates to
    // C.  C therefore holds a content-addressed reference to A's tip
    // WITHOUT having fetched A's closure (invariants 2/3 still hold).
    {
        let a_tip = {
            let a_remote = sync_store::ContentRemote::open_at_url(&a_url, Default::default())
                .await
                .expect("open A's remote");
            a_remote
                .get_tip("main")
                .await
                .expect("get A's tip")
                .expect("A has a tip")
        };

        // B holds the pin it wrote.
        let b_pin_bytes = read_small_file(&b_ctx, "/sys/grafts/upstreamA")
            .await
            .expect("B holds its graft pin for upstreamA");
        let b_pin = steward::GraftPin::from_yaml_bytes(&b_pin_bytes).expect("parse B's graft pin");
        assert_eq!(b_pin.foreign_pond_id, a_pond_id.to_string());
        assert_eq!(b_pin.mount_path, "/imports/A");
        assert_eq!(b_pin.pinned_tip, a_tip.to_hex());

        // C, having imported B at /imports/B, sees B's graft pin under the
        // mount -- a content reference revealing that B grafts A at tip T,
        // learned WITHOUT C fetching A's content closure.
        let c_pin_bytes = read_small_file(&c_ctx, "/imports/B/sys/grafts/upstreamA")
            .await
            .expect("C sees B's replicated graft pin under /imports/B");
        let c_pin = steward::GraftPin::from_yaml_bytes(&c_pin_bytes).expect("parse C's graft pin");
        assert_eq!(c_pin, b_pin, "C's replicated graft pin must equal B's");
    }
}

/// Variant 2b (`pond://` local content source): a consumer pond imports a
/// producer pond clone directly from local disk -- no S3/`file://` remote,
/// no `pond push`.  This is the local develop-and-preview workflow: clone a
/// producer (e.g. water), edit and iterate, and have a consumer import its
/// derived content directly from the on-disk clone.
///
/// Mirrors `cross_pond_pull_materializes_mount_entry` but replaces the
/// push-to-`file://`-remote step with a `pond://<producer-path>` URL, so it
/// exercises `open_content_source` -> `LocalPondSource` -> `fetch_object_graph`
/// -> `import_pond` end-to-end.
#[tokio::test]
async fn cross_pond_pull_via_pond_scheme_materializes_mount_entry() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let a_pond = scratch.path().join("a_pond");
    let b_pond = scratch.path().join("b_pond");

    // 1) Producer pond A: init + write a file.  No remote, no push.
    let a_ctx = ctx_for(&a_pond, vec!["pond", "init"]);
    init_command(&a_ctx, "test-host").await.expect("init A");
    write_small_file(
        &a_ctx,
        "/sensor.txt",
        b"data from local producer pond A",
        vec!["copy", "sensor.txt"],
    )
    .await
    .expect("write sensor.txt on A");
    let a_pond_id = {
        let ship = a_ctx.open_pond().await.expect("open A");
        ship.control_table().pond_id_uuid()
    };

    // A `pond://` URL points straight at A's on-disk clone.
    let pond_url = format!("pond://{}", a_pond.display());

    // 2) Consumer pond B: init (distinct pond_id).
    let b_ctx = ctx_for(&b_pond, vec!["pond", "init"]);
    init_command(&b_ctx, "test-host").await.expect("init B");
    let b_pond_id = {
        let ship = b_ctx.open_pond().await.expect("open B");
        ship.control_table().pond_id_uuid()
    };
    assert_ne!(
        a_pond_id, b_pond_id,
        "test invariant: A and B must have different pond_ids"
    );

    // 3) B attaches A as a cross-pond import via `pond://` at
    //    /imports/upstream.  `remote add` opens a LocalPondSource to learn
    //    A's (foreign) store_id and verify it differs from B's.
    add_remote_command(
        &b_ctx,
        "upstream",
        &pond_url,
        "/imports/upstream",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("cross-pond pond:// attach on B");

    // 4) B pulls -- fetch_object_graph + import_pond sourced entirely from
    //    A's local clone (no network / no S3).
    pull_command(&b_ctx, Some("upstream".to_string()))
        .await
        .expect("pull upstream (pond://) on B");

    // 5) The foreign file is readable on B through the mount.
    let bytes = read_small_file(&b_ctx, "/imports/upstream/sensor.txt")
        .await
        .expect("read foreign file through pond:// mount");
    assert_eq!(bytes, b"data from local producer pond A");

    // 6) The mount entry carries A's pond_id (foreign namespace), exactly as
    //    an S3/file import would -- so navigation through it resolves in A's
    //    namespace (the property cross-pond dynamic-series resolution needs).
    {
        use steward::PondUserMetadata;
        let mut ship = b_ctx.open_pond().await.expect("reopen B");
        let tx = ship
            .begin_read(&PondUserMetadata::new(vec!["verify".to_string()]))
            .await
            .expect("begin read");
        {
            let fs = &*tx;
            let root = fs.root().await.expect("root B");
            let imports = root.open_dir_path("/imports").await.expect("open /imports");
            let entry = imports
                .get("upstream")
                .await
                .expect("get upstream")
                .expect("upstream entry present");
            let entry_pond = entry.node.id().pond_id();
            let a_uuid7 = uuid7::Uuid::from(*a_pond_id.as_bytes());
            assert_eq!(
                entry_pond, a_uuid7,
                "mount entry must carry foreign pond_id"
            );
        }
        let _ = tx.commit().await.expect("commit read");
    }

    // 7) A second pull is idempotent.
    pull_command(&b_ctx, Some("upstream".to_string()))
        .await
        .expect("second pond:// pull is idempotent");
    let bytes2 = read_small_file(&b_ctx, "/imports/upstream/sensor.txt")
        .await
        .expect("read foreign file after second pond:// pull");
    assert_eq!(bytes2, b"data from local producer pond A");
}

/// After the producer clone is edited (a new commit), re-pulling the
/// `pond://` import brings the consumer up to the producer's new tip -- the
/// "edit and iterate" half of the develop-and-preview loop.
#[tokio::test]
async fn cross_pond_pull_via_pond_scheme_follows_producer_edits() {
    init_log();
    let scratch = TempDir::new().expect("tempdir");
    let a_pond = scratch.path().join("a_pond");
    let b_pond = scratch.path().join("b_pond");

    let a_ctx = ctx_for(&a_pond, vec!["pond", "init"]);
    init_command(&a_ctx, "test-host").await.expect("init A");
    write_small_file(&a_ctx, "/v.txt", b"v1", vec!["copy", "v.txt"])
        .await
        .expect("write v1 on A");

    let pond_url = format!("pond://{}", a_pond.display());

    let b_ctx = ctx_for(&b_pond, vec!["pond", "init"]);
    init_command(&b_ctx, "test-host").await.expect("init B");
    add_remote_command(
        &b_ctx,
        "upstream",
        &pond_url,
        "/imports/upstream",
        None,
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("attach pond:// on B");
    pull_command(&b_ctx, Some("upstream".to_string()))
        .await
        .expect("first pull on B");
    assert_eq!(
        read_small_file(&b_ctx, "/imports/upstream/v.txt")
            .await
            .expect("read v1 through mount"),
        b"v1"
    );

    // Producer edits: overwrite the file (new version -> new tip).
    write_small_file(&a_ctx, "/v.txt", b"v2-edited", vec!["copy", "v.txt"])
        .await
        .expect("write v2 on A");

    // Re-pull picks up the producer's new tip.
    pull_command(&b_ctx, Some("upstream".to_string()))
        .await
        .expect("re-pull on B after producer edit");
    assert_eq!(
        read_small_file(&b_ctx, "/imports/upstream/v.txt")
            .await
            .expect("read v2 through mount"),
        b"v2-edited",
        "re-pull must follow the producer's edit"
    );
}
