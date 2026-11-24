// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for Decision D5: every spine-bearing write commit appends
//! its leaf to the pond's transparency-log tiles under `{POND}/tlog` and
//! re-emits the C2SP checkpoint.  The checkpoint root and inclusion proofs are
//! verified against leaves reconstructed independently from the control table.

use steward::{Ship, get_tlog_path};
use sync_store::tlog::hash_leaf;
use sync_store::{
    Checkpoint, LogHash, TileLog, TransparencyLog, verify_consistency, verify_inclusion,
};
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

/// Reconstruct the log's leaf sequence directly from the control table's
/// per-seq commit objects, so the tile checkpoint can be checked against an
/// independent source of truth.
async fn leaves_from_control_table(ship: &Ship, up_to_seq: i64) -> Vec<LogHash> {
    let ct = ship.control_table();
    let mut leaves = Vec::new();
    for seq in 1..=up_to_seq {
        if let Some(hex) = ct.commit_object_at(seq).await.expect("commit_object_at") {
            let bytes = hex::decode(&hex).expect("commit object hex");
            leaves.push(hash_leaf(&bytes));
        }
    }
    leaves
}

#[tokio::test]
async fn checkpoint_grows_and_root_matches_commit_spine() {
    let tmp = tempdir().expect("tempdir");
    let pond = tmp.path().join("pond");
    let mut ship = Ship::create_pond(&pond, "tlog-test")
        .await
        .expect("create pond");

    let tlog_dir = get_tlog_path(&pond);

    for (i, (path, body)) in [("/a.txt", "one"), ("/b.txt", "two"), ("/c.txt", "three")]
        .into_iter()
        .enumerate()
    {
        write_file(&mut ship, path, body.as_bytes()).await;

        let cp = Checkpoint::read(&tlog_dir)
            .expect("read checkpoint")
            .expect("checkpoint present after a user write");
        assert_eq!(
            cp.size,
            (i + 1) as u64,
            "one leaf appended per spine-bearing commit"
        );

        let leaves = leaves_from_control_table(&ship, ship.last_write_seq()).await;
        let mut reference = TransparencyLog::new();
        for l in &leaves {
            let _ = reference.append_leaf_hash(*l);
        }
        assert_eq!(
            cp.root,
            reference.root(),
            "checkpoint root must equal the RFC 6962 fold of the commit spine"
        );
    }

    // Origin binds the checkpoint to this pond's identity.
    let pond_id = ship.control_table().pond_id_uuid();
    let cp = Checkpoint::read(&tlog_dir).unwrap().unwrap();
    assert_eq!(cp.origin, format!("watertown/{pond_id}"));
}

#[tokio::test]
async fn tile_leaves_prove_inclusion_against_published_checkpoint() {
    let tmp = tempdir().expect("tempdir");
    let pond = tmp.path().join("pond");
    let mut ship = Ship::create_pond(&pond, "tlog-proof")
        .await
        .expect("create pond");

    for i in 0..5 {
        write_file(
            &mut ship,
            &format!("/f{i}.txt"),
            format!("body-{i}").as_bytes(),
        )
        .await;
    }

    let tlog_dir = get_tlog_path(&pond);
    let cp = Checkpoint::read(&tlog_dir).unwrap().unwrap();
    assert_eq!(cp.size, 5);

    // Read the leaves back out of the level-0 tiles and prove every one against
    // the published checkpoint root.
    let log = TileLog::new(&tlog_dir, cp.origin.clone());
    let leaves = log.load_leaves().expect("load leaves from tiles");
    assert_eq!(leaves.len(), 5);

    let mut reference = TransparencyLog::new();
    for l in &leaves {
        let _ = reference.append_leaf_hash(*l);
    }
    for (i, l) in leaves.iter().enumerate() {
        let proof = reference.inclusion_proof(i).expect("proof");
        assert!(
            verify_inclusion(l, i, leaves.len(), &proof, &cp.root),
            "leaf {i} should prove against the published checkpoint"
        );
    }
}

/// A dropped/lagging export (design Decision D5, Remaining work item 1) must
/// self-heal on the next commit: the reconciliation drives the export from the
/// committed leaf count, replaying every missing commit leaf, so even a fully
/// deleted `{POND}/tlog` is rebuilt to match the control-table leaf sequence.
#[tokio::test]
async fn dropped_export_reconciles_to_committed_leaf_count_on_next_commit() {
    let tmp = tempdir().expect("tempdir");
    let pond = tmp.path().join("pond");
    let mut ship = Ship::create_pond(&pond, "tlog-heal")
        .await
        .expect("create pond");

    let tlog_dir = get_tlog_path(&pond);

    // Three committed leaves, exported normally.
    for i in 0..3 {
        write_file(&mut ship, &format!("/a{i}.txt"), format!("v{i}").as_bytes()).await;
    }
    assert_eq!(
        Checkpoint::read(&tlog_dir).unwrap().unwrap().size,
        3,
        "three leaves exported before the drop"
    );

    // Simulate the export lagging the committed log: wipe the entire tile
    // directory (worst case -- crash/unwritable dir left nothing behind).
    std::fs::remove_dir_all(&tlog_dir).expect("drop tlog export");
    assert!(
        Checkpoint::read(&tlog_dir).unwrap().is_none(),
        "export is gone after the drop"
    );

    // The next commit must replay all four committed leaves, not just its own.
    write_file(&mut ship, "/a3.txt", b"v3").await;

    let cp = Checkpoint::read(&tlog_dir)
        .expect("read checkpoint")
        .expect("checkpoint rebuilt after the drop");
    assert_eq!(
        cp.size, 4,
        "reconciliation replays every committed leaf, not just the newest"
    );

    // Root and every tile leaf must match the control-table leaf sequence.
    let leaves = leaves_from_control_table(&ship, ship.last_write_seq()).await;
    assert_eq!(leaves.len(), 4);
    let mut reference = TransparencyLog::new();
    for l in &leaves {
        let _ = reference.append_leaf_hash(*l);
    }
    assert_eq!(
        cp.root,
        reference.root(),
        "healed checkpoint root must equal the control-table leaf fold"
    );

    let tile_leaves = TileLog::new(&tlog_dir, cp.origin.clone())
        .load_leaves()
        .expect("load leaves from rebuilt tiles");
    assert_eq!(tile_leaves, leaves, "rebuilt tile leaves match the log");
}

#[tokio::test]
async fn lagging_checkpoint_catches_up_to_the_same_tiles_as_full_replay() {
    let tmp = tempdir().unwrap();
    let pond = tmp.path().join("pond");
    let mut ship = Ship::create_pond(&pond, "tlog-lag").await.unwrap();
    let dir = get_tlog_path(&pond);
    write_file(&mut ship, "/first", b"first").await;
    let prefix = tmp.path().join("one-leaf-export");
    std::fs::rename(&dir, &prefix).unwrap();
    write_file(&mut ship, "/second", b"second").await;
    std::fs::rename(&dir, tmp.path().join("two-leaf-export")).unwrap();
    std::fs::rename(prefix, &dir).unwrap();
    write_file(&mut ship, "/third", b"third").await;

    let hashes = leaves_from_control_table(&ship, ship.last_write_seq()).await;
    let oracle = TileLog::new(
        tmp.path().join("replayed"),
        format!("watertown/{}", ship.control_table().pond_id_uuid()),
    );
    let expected = oracle.materialize(&hashes).unwrap();
    assert_eq!(Checkpoint::read(&dir).unwrap(), Some(expected));
    let tile = sync_store::tlog::data_tile_rel(0, 0, hashes.len());
    assert_eq!(
        std::fs::read(dir.join(&tile)).unwrap(),
        std::fs::read(oracle.dir().join(tile)).unwrap()
    );
}

/// Every published checkpoint is recorded in the append-only history, and each
/// one proves append-only consistency against the current tree (the key-free
/// half of the transparency log, verifiable without any signing key).
#[tokio::test]
async fn checkpoint_history_records_and_proves_consistency() {
    let tmp = tempdir().expect("tempdir");
    let pond = tmp.path().join("pond");
    let mut ship = Ship::create_pond(&pond, "tlog-history")
        .await
        .expect("create pond");
    let tlog_dir = get_tlog_path(&pond);

    for i in 0..4 {
        write_file(&mut ship, &format!("/h{i}.txt"), format!("b{i}").as_bytes()).await;
    }

    // One history record per spine-bearing commit, sizes 1..=4 in order.
    let history = Checkpoint::read_history(&tlog_dir).expect("read history");
    assert_eq!(
        history.iter().map(|c| c.size).collect::<Vec<_>>(),
        vec![1, 2, 3, 4],
        "every published checkpoint is recorded, in order"
    );

    // The newest history entry matches the live checkpoint.
    let cp = Checkpoint::read(&tlog_dir).unwrap().unwrap();
    assert_eq!(history.last().unwrap().root, cp.root);
    assert_eq!(history.last().unwrap().size, cp.size);

    // Every historical checkpoint is an append-only prefix of the current tree.
    let leaves = TileLog::new(&tlog_dir, cp.origin.clone())
        .load_leaves()
        .expect("load leaves");
    let mut tree = TransparencyLog::new();
    for l in &leaves {
        let _ = tree.append_leaf_hash(*l);
    }
    for h in &history {
        let proof = tree
            .consistency_proof(h.size as usize)
            .expect("consistency proof");
        assert!(
            verify_consistency(h.size as usize, leaves.len(), &h.root, &cp.root, &proof),
            "checkpoint at size {} must prove consistent with the current log",
            h.size
        );
    }
}
