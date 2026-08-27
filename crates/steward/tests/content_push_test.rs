// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for `steward::push_content_to_remote`: the producer side
//! of the single delta-managed content-addressed remote (design Section 8,
//! Decisions D6 and D7).

use steward::{ContentVerifyState, Ship, push_content_to_remote, verify_content_against_remote};
use sync_store::ContentRemote;
use sync_store::content::{Commit, ObjectHash};
use tempfile::tempdir;
use tinyfs::async_helpers::convenience::create_file_path;
use tlogfs::PondUserMetadata;

fn meta(label: &str) -> PondUserMetadata {
    PondUserMetadata::new(vec!["test".into(), label.into()])
}

async fn write_file(ship: &mut Ship, path: &str, bytes: &[u8]) {
    let bytes = bytes.to_vec();
    ship.write_transaction(&meta("write"), async move |fs| {
        let root = fs.root().await?;
        let _ = create_file_path(&root, path, &bytes).await?;
        Ok(())
    })
    .await
    .expect("write transaction");
}

async fn new_pond(label: &str) -> (tempfile::TempDir, Ship) {
    let tmp = tempdir().expect("tempdir");
    let ship = Ship::create_pond(tmp.path().join("pond"), label)
        .await
        .expect("create pond");
    (tmp, ship)
}

/// A push lands the tip ref and the full inline object closure, and the tip
/// commit object decodes to a root tree that is itself present on the remote.
#[tokio::test]
async fn push_lands_objects_and_decodable_tip() {
    let (_t, mut ship) = new_pond("push").await;
    write_file(&mut ship, "/a.txt", b"alpha").await;
    write_file(&mut ship, "/b.txt", b"beta").await;

    let pond_id = uuid::Uuid::parse_str(ship.data_persistence().pond_id()).expect("pond id");

    let remote_dir = tempdir().expect("remote dir");
    let mut remote = ContentRemote::create_at(remote_dir.path().join("remote"), pond_id)
        .await
        .expect("create remote");

    let outcome = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("push");

    let _ = remote
        .inspect_recovery_recipe_dp_commit_3()
        .await
        .expect("push installs the Pond-free recovery recipe");
    assert!(outcome.objects_pushed >= 1);
    assert_eq!(outcome.ref_name, "main");

    let tip = remote.get_tip("main").await.expect("tip").expect("ref set");
    assert_eq!(tip, outcome.tip);

    let tip_bytes = remote
        .get_object(tip)
        .await
        .expect("get tip object")
        .expect("tip object present");
    let commit = Commit::decode(&tip_bytes).expect("decode commit");

    let root_present = remote
        .has_object(commit.root_tree_hash)
        .await
        .expect("has root tree");
    assert!(root_present, "root tree object must be on the remote");

    assert!(
        remote
            .latest_capsule()
            .await
            .expect("read capsule ref")
            .is_none(),
        "native pushes do not publish per-commit recovery capsules"
    );
}

/// Every object on the remote hashes to its key: pushing preserves the
/// content-addressing invariant a consumer relies on to verify by hash.
#[tokio::test]
async fn pushed_objects_are_content_addressed() {
    let (_t, mut ship) = new_pond("push-ca").await;
    write_file(&mut ship, "/a.txt", b"alpha").await;

    let pond_id = uuid::Uuid::parse_str(ship.data_persistence().pond_id()).expect("pond id");
    let remote_dir = tempdir().expect("remote dir");
    let mut remote = ContentRemote::create_at(remote_dir.path().join("remote"), pond_id)
        .await
        .expect("create remote");

    let outcome = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("push");

    let tip = remote.get_tip("main").await.expect("tip").expect("ref set");
    let tip_bytes = remote.get_object(tip).await.expect("get").expect("present");
    assert_eq!(
        ObjectHash::of_bytes(&tip_bytes),
        tip,
        "tip object bytes must hash to the tip ref"
    );

    let commit = Commit::decode(&tip_bytes).expect("decode commit");
    let root_bytes = remote
        .get_object(commit.root_tree_hash)
        .await
        .expect("get root")
        .expect("root present");
    assert_eq!(
        ObjectHash::of_bytes(&root_bytes),
        commit.root_tree_hash,
        "root tree object bytes must hash to its key"
    );

    let _ = outcome;
}

/// Re-pushing the same tip is idempotent: the ref stays put and the objects
/// re-put cleanly.
#[tokio::test]
async fn re_push_same_tip_is_idempotent() {
    let (_t, mut ship) = new_pond("push-idem").await;
    write_file(&mut ship, "/a.txt", b"alpha").await;

    let pond_id = uuid::Uuid::parse_str(ship.data_persistence().pond_id()).expect("pond id");
    let remote_dir = tempdir().expect("remote dir");
    let mut remote = ContentRemote::create_at(remote_dir.path().join("remote"), pond_id)
        .await
        .expect("create remote");

    let first = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("first push");
    let second = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("second push");

    assert_eq!(first.tip, second.tip, "tip is unchanged by a re-push");
    let tip = remote.get_tip("main").await.expect("tip").expect("ref set");
    assert_eq!(tip, first.tip);
    assert!(
        remote
            .capsule_roots()
            .await
            .expect("capsule roots")
            .is_empty(),
        "native pushes do not maintain capsule generations"
    );
}

#[tokio::test]
async fn verify_reads_authoritative_tip_with_stale_open_ship() {
    let (pond_dir, mut writer) = new_pond("verify-fresh-tip").await;
    write_file(&mut writer, "/a.txt", b"alpha").await;

    let pond_id = uuid::Uuid::parse_str(writer.data_persistence().pond_id()).expect("pond id");
    let remote_dir = tempdir().expect("remote dir");
    let mut remote = ContentRemote::create_at(remote_dir.path().join("remote"), pond_id)
        .await
        .expect("create remote");
    let first = push_content_to_remote(&writer, &mut remote, "main")
        .await
        .expect("push first tip");

    let stale_open_ship = Ship::open_pond(pond_dir.path().join("pond"))
        .await
        .expect("open verifier before next write");
    write_file(&mut writer, "/b.txt", b"beta").await;

    let report = verify_content_against_remote(&stale_open_ship, &remote, "main")
        .await
        .expect("verify from stale-open Ship");
    assert_ne!(report.local_tip, Some(first.tip));
    assert_eq!(
        report.state,
        ContentVerifyState::RemoteBehind { local_unpushed: 1 }
    );
}

#[tokio::test]
async fn verify_rejects_remote_ref_with_missing_tip_object() {
    let (_pond_dir, mut ship) = new_pond("verify-missing-tip").await;
    write_file(&mut ship, "/a.txt", b"alpha").await;
    let tip_seq = ship
        .control_table()
        .latest_spine_seq()
        .await
        .expect("read tip seq")
        .expect("content tip");
    let tip = ObjectHash::from_hex(
        &ship
            .control_table()
            .commit_hash_at(tip_seq)
            .await
            .expect("read tip")
            .expect("tip hash"),
    )
    .expect("decode tip");

    let pond_id = uuid::Uuid::parse_str(ship.data_persistence().pond_id()).expect("pond id");
    let remote_dir = tempdir().expect("remote dir");
    let mut remote = ContentRemote::create_at(remote_dir.path().join("remote"), pond_id)
        .await
        .expect("create remote");
    let _remote_seq = remote
        .push_commit(&[], "main", tip)
        .await
        .expect("publish dangling tip ref");

    let error = verify_content_against_remote(&ship, &remote, "main")
        .await
        .expect_err("dangling remote tip must fail verification");
    assert!(error.to_string().contains("has no commit object"));
}
