// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! D6.3 integration tests for `steward::rebuild_control_table`: recover
//! the control table from the data Delta table after the control table
//! is lost or corrupt.

use anyhow::Result;
use steward::{PondUserMetadata, Ship, get_control_path, rebuild_control_table};
use tempfile::tempdir;
use tinyfs::EntryType;
use tokio::io::AsyncWriteExt;

/// Write a small file in its own write transaction.
async fn write_file(ship: &mut Ship, path: &str, bytes: &[u8], args: Vec<&str>) -> Result<()> {
    let meta = PondUserMetadata::new(args.into_iter().map(String::from).collect());
    ship.write_transaction(&meta, async move |fs| {
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
    })
    .await?;
    Ok(())
}

/// Full disaster-recovery roundtrip: build a pond, capture its identity
/// and last write seq, delete the control table, rebuild it from data,
/// then reopen and confirm identity + seq + data survived.
#[tokio::test]
async fn rebuild_control_recovers_identity_and_history() -> Result<()> {
    let temp = tempdir()?;
    let pond_path = temp.path().join("pond");

    // 1) Build a pond with several write transactions.
    let (orig_pond_id, orig_last_seq) = {
        let mut ship = Ship::create_pond(&pond_path, "test-host").await?;
        write_file(&mut ship, "/a.txt", b"alpha", vec!["copy", "a.txt"]).await?;
        write_file(&mut ship, "/b.txt", b"beta", vec!["copy", "b.txt"]).await?;
        let pond_id = ship.control_table().pond_id_uuid();
        let last_seq = ship.last_write_seq();
        (pond_id, last_seq)
    };
    assert!(
        orig_last_seq >= 3,
        "expected init + 2 writes, got {orig_last_seq}"
    );

    // 2) Simulate control-table loss.
    let control_path = get_control_path(&pond_path);
    std::fs::remove_dir_all(&control_path)?;
    assert!(!control_path.exists());

    // Opening must now fail (no control table).
    assert!(
        Ship::open_pond(&pond_path).await.is_err(),
        "pond should not open with a missing control table"
    );

    // 3) Rebuild the control table from the data Delta table.
    let report = rebuild_control_table(&pond_path, false).await?;
    assert_eq!(report.pond_id, orig_pond_id, "recovered pond_id must match");
    assert_eq!(
        report.last_txn_seq, orig_last_seq,
        "recovered last_txn_seq must match"
    );
    assert_eq!(
        report.txns_reconstructed as i64, orig_last_seq,
        "one reconstructed txn per committed seq"
    );
    assert!(
        report.backup_path.is_none(),
        "no backup expected when control was fully removed"
    );

    // 4) Reopen and verify identity, seq, and data all survived.
    let mut ship = Ship::open_pond(&pond_path).await?;
    assert_eq!(ship.control_table().pond_id_uuid(), orig_pond_id);
    assert_eq!(ship.last_write_seq(), orig_last_seq);
    assert_eq!(
        ship.control_table().get_last_write_sequence().await?,
        orig_last_seq,
        "control table's last_committed_seq must reflect the rebuilt history"
    );

    // Data is readable through the reopened pond.
    let tx = ship
        .begin_read(&PondUserMetadata::new(vec!["read".into(), "a".into()]))
        .await?;
    let bytes = {
        let fs = &*tx;
        let root = fs.root().await?;
        root.read_file_path_to_vec("/a.txt").await?
    };
    let _ = tx.commit().await?;
    assert_eq!(bytes, b"alpha");

    Ok(())
}

/// Rebuild refuses to clobber an existing control table unless `force`
/// is set, and moves it aside (to `control.bak.*`) when it is.
#[tokio::test]
async fn rebuild_control_requires_force_when_control_exists() -> Result<()> {
    let temp = tempdir()?;
    let pond_path = temp.path().join("pond");

    {
        let mut ship = Ship::create_pond(&pond_path, "test-host").await?;
        write_file(&mut ship, "/x.txt", b"x", vec!["copy", "x.txt"]).await?;
    }

    // Without force: must refuse (a real control table is present).
    let err = rebuild_control_table(&pond_path, false)
        .await
        .expect_err("rebuild without force must refuse to clobber control");
    assert!(
        err.to_string().contains("--force"),
        "error should mention --force, got: {}",
        err
    );

    // With force: succeeds and moves the old control table aside.
    let report = rebuild_control_table(&pond_path, true).await?;
    let backup = report
        .backup_path
        .expect("force rebuild should report a backup path");
    assert!(
        backup.exists(),
        "backed-up control dir must exist: {backup:?}"
    );
    assert!(
        backup
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("control.bak.")),
        "backup should be named control.bak.<ts>, got {backup:?}"
    );

    // Pond still opens after the forced rebuild.
    let ship = Ship::open_pond(&pond_path).await?;
    assert_eq!(ship.last_write_seq(), report.last_txn_seq);

    Ok(())
}

/// Phase 6 "rebuild from pond" validation: the pond's *content* is fully
/// reconstructable from the data FS alone.  Build a pond with a mix of files,
/// nested directories, an overwrite, and a delete; capture the local `fsck`
/// content root and the `compute_content_tree` root; discard the control
/// table; rebuild it from data; then confirm both roots are byte-identical.
///
/// The commit spine (`commit_hash` / `root_tree_hash` / `commit_object`) lives
/// authoritatively in the data-FS LOG node.  This asserts both that the content
/// roots survive and that the rebuild replays the spine from the log so the
/// control-table spine cache is restored (tip commit hash + full spine order).
#[tokio::test]
async fn rebuild_control_preserves_content_roots() -> Result<()> {
    use steward::FsckOptions;

    let temp = tempdir()?;
    let pond_path = temp.path().join("pond");

    // 1) Build a pond with a variety of live content.
    let (fsck_root_before, content_root_before, tip_before, seq_before, spine_before) = {
        let mut ship = Ship::create_pond(&pond_path, "test-host").await?;
        write_file(&mut ship, "/a.txt", b"alpha", vec!["copy", "a.txt"]).await?;
        ship.write_transaction(
            &PondUserMetadata::new(vec!["mkdir".into(), "sub".into()]),
            async move |fs| {
                let root = fs.root().await?;
                let _ = root.create_dir_all("/sub").await?;
                Ok(())
            },
        )
        .await?;
        write_file(&mut ship, "/sub/b.txt", b"beta", vec!["copy", "b.txt"]).await?;
        write_file(&mut ship, "/sub/c.txt", b"gamma", vec!["copy", "c.txt"]).await?;
        // Overwrite one file (new version) and delete another.
        write_file(&mut ship, "/a.txt", b"alpha-2", vec!["copy", "a.txt"]).await?;
        ship.write_transaction(
            &PondUserMetadata::new(vec!["rm".into(), "c.txt".into()]),
            async move |fs| {
                let root = fs.root().await?;
                let sub = root.open_dir_path("/sub").await?;
                sub.remove_entry("c.txt").await?;
                Ok(())
            },
        )
        .await?;

        let fsck = steward::fsck(&ship, FsckOptions::default()).await?;
        let content = steward::compute_content_tree(&ship).await?;
        let seq = ship.control_table().get_last_write_sequence().await?;
        let tip = ship.control_table().commit_hash_at(seq).await?;
        let spine = ship.control_table().commit_objects_in_order().await?;
        (fsck.root_hex(), content.root_tree_hash, tip, seq, spine)
    };
    assert!(
        tip_before.is_some(),
        "the live pond must have a stamped tip commit hash"
    );
    assert!(
        !spine_before.is_empty(),
        "the live pond must have a non-empty commit spine"
    );

    // 2) Discard the control table entirely.
    let control_path = get_control_path(&pond_path);
    std::fs::remove_dir_all(&control_path)?;
    assert!(!control_path.exists());

    // 3) Rebuild the control table from the data table alone.
    let _ = rebuild_control_table(&pond_path, false).await?;

    // 4) Reopen and recompute both roots; they must be unchanged, proving the
    //    content is reconstructable from the data FS without the control table.
    let ship = Ship::open_pond(&pond_path).await?;
    let fsck_root_after = steward::fsck(&ship, FsckOptions::default())
        .await?
        .root_hex();
    let content_root_after = steward::compute_content_tree(&ship).await?.root_tree_hash;
    let seq_after = ship.control_table().get_last_write_sequence().await?;
    let tip_after = ship.control_table().commit_hash_at(seq_after).await?;
    let spine_after = ship.control_table().commit_objects_in_order().await?;

    assert_eq!(
        fsck_root_before, fsck_root_after,
        "fsck content root must survive a control-table discard + rebuild"
    );
    assert_eq!(
        content_root_before, content_root_after,
        "content-tree root must survive a control-table discard + rebuild"
    );
    // Spine replay: the tip commit hash and the full commit-object sequence
    // must be recovered from the log node, not left empty.
    assert_eq!(
        seq_before, seq_after,
        "last write seq must match after rebuild"
    );
    assert_eq!(
        tip_before, tip_after,
        "tip commit hash must be replayed from the log node after rebuild"
    );
    assert_eq!(
        spine_before, spine_after,
        "the full commit-object spine must be replayed from the log node"
    );

    Ok(())
}

/// Reclamation's delete commits must not be replayed as transactions.
///
/// `reclaim` runs as the tail of a collapse and deliberately consumes no
/// sequence of its own.  Its deletes are chunked, so stamping them with the
/// collapse's `txn_seq` under the `pond_txn` key would make
/// `reconstruct_txn_history` -- which yields one transaction per `pond_txn`
/// commit -- emit several entries for that single sequence, and rebuild would
/// replay a duplicate lifecycle record for each.  Maintenance commits are
/// stamped under `pond_maintenance` instead, so they stay attributable on disk
/// while being invisible to transaction reconstruction.
#[tokio::test]
async fn rebuild_after_collapse_reclaim_replays_one_txn_per_seq() -> Result<()> {
    let temp = tempdir()?;
    let pond_path = temp.path().join("pond");

    let (orig_last_seq, reclaimed_rows) = {
        let mut ship = Ship::create_pond(&pond_path, "reclaim-rebuild").await?;

        // Enough versions of a collapsible series that a collapse has real
        // work, and reclaim has real rows to delete.
        for i in 0..12u64 {
            let meta = PondUserMetadata::new(vec!["test".into(), format!("append{i}")]);
            let bytes = vec![b'a'.wrapping_add((i % 26) as u8); 4096];
            ship.write_transaction(&meta, async move |fs| {
                let root = fs.root().await?;
                let mut w = root
                    .async_writer_path_with_type("/events.series", EntryType::FilePhysicalSeries)
                    .await?;
                w.write_all(&bytes)
                    .await
                    .map_err(|e| steward::StewardError::Aborted(format!("write: {}", e)))?;
                w.shutdown()
                    .await
                    .map_err(|e| steward::StewardError::Aborted(format!("close: {}", e)))?;
                Ok(())
            })
            .await?;
        }

        let report = ship.collapse_versions(1).await?;
        assert!(report.files_collapsed > 0, "series should have collapsed");
        assert!(
            report.reclaimed.rows_deleted > 0,
            "collapse must have superseded rows to reclaim"
        );
        (ship.last_write_seq(), report.reclaimed.rows_deleted)
    };

    std::fs::remove_dir_all(get_control_path(&pond_path))?;
    let report = rebuild_control_table(&pond_path, false).await?;

    assert_eq!(
        report.last_txn_seq, orig_last_seq,
        "reclaim consumes no sequence, so the recovered frontier is the collapse's"
    );
    assert_eq!(
        report.txns_reconstructed as i64, orig_last_seq,
        "exactly one reconstructed txn per committed seq: reclaim deleted {reclaimed_rows} \
         row(s) across its own Delta commits, and none of them may appear as a transaction"
    );

    // The rebuilt control table must agree, and the pond must still open.
    let mut ship = Ship::open_pond(&pond_path).await?;
    assert_eq!(ship.last_write_seq(), orig_last_seq);
    assert_eq!(
        ship.control_table().get_last_write_sequence().await?,
        orig_last_seq
    );

    // And a further write continues from the recovered frontier.
    write_file(&mut ship, "/after.txt", b"ok", vec!["copy", "after"]).await?;
    assert_eq!(ship.last_write_seq(), orig_last_seq + 1);

    Ok(())
}
