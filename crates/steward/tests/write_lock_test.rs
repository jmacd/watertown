// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the steward write lock (D5.7a.1).
//!
//! These tests verify two end-user-visible invariants that the lower-level
//! `write_lock` unit tests cannot exercise:
//!
//! 1. **Cross-Ship process exclusion.** When one `Ship` instance holds an
//!    active write transaction on a pond, a second `Ship` opened on the
//!    same pond must fail to begin its own write with `PondLocked`.  Once
//!    the first writer drops its guard, the lock must be released so the
//!    second writer can proceed.
//!
//! 2. **No control-table records for reads.** After D5.7a.1, read
//!    transactions must not emit `begin` / `completed` records into the
//!    steward control table.  Counting the control rows before and after
//!    a read transaction proves the change.

use anyhow::Result;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use steward::{PondUserMetadata, Ship, StewardError};
use tempfile::tempdir;

/// Count rows in the control delta table (across all partitions).
async fn count_control_rows(control_path: &std::path::Path) -> Result<usize> {
    let url = url::Url::from_directory_path(control_path)
        .or_else(|_| url::Url::from_file_path(control_path))
        .map_err(|_| anyhow::anyhow!("invalid control path"))?;
    let table = deltalake::open_table(url).await?;
    let ctx = SessionContext::new();
    let _ = ctx.register_table("control", Arc::new(table))?;
    let df = ctx.sql("SELECT COUNT(*) AS c FROM control").await?;
    let batches = df.collect().await?;
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count is int64");
    Ok(arr.value(0) as usize)
}

#[tokio::test]
async fn write_lock_blocks_concurrent_writer() -> Result<()> {
    let temp = tempdir()?;
    let pond_path = temp.path().join("pond");

    // Create pond + first Ship.
    let mut ship_a = Ship::create_pond(&pond_path, "test-host")
        .await
        .map_err(|e| anyhow::anyhow!("create_pond: {e}"))?;

    let meta_a = PondUserMetadata::new(vec!["test".into(), "writer-a".into()]);
    let guard_a = ship_a
        .begin_write(&meta_a)
        .await
        .map_err(|e| anyhow::anyhow!("ship A begin_write: {e}"))?;

    // Second Ship on the same pond — should observe the lock.
    let mut ship_b = Ship::open_pond(&pond_path)
        .await
        .map_err(|e| anyhow::anyhow!("ship B open_pond: {e}"))?;
    let meta_b = PondUserMetadata::new(vec!["test".into(), "writer-b".into()]);
    match ship_b.begin_write(&meta_b).await {
        Ok(_g) => panic!("second writer should be rejected while ship A holds the lock"),
        Err(StewardError::PondLocked {
            holder_pid, path, ..
        }) => {
            assert_eq!(holder_pid, Some(std::process::id()));
            assert!(path.ends_with("write.lock"));
        }
        Err(other) => panic!("expected PondLocked, got: {other:?}"),
    }

    // Drop ship A's guard — releases the lock.
    drop(guard_a);

    // ship B can now acquire.
    let guard_b = ship_b
        .begin_write(&meta_b)
        .await
        .map_err(|e| anyhow::anyhow!("ship B begin_write after A drop: {e}"))?;
    drop(guard_b);

    Ok(())
}

#[tokio::test]
async fn read_transaction_emits_no_control_records() -> Result<()> {
    let temp = tempdir()?;
    let pond_path = temp.path().join("pond");
    let control_path = steward::get_control_path(&pond_path);

    let mut ship = Ship::create_pond(&pond_path, "test-host")
        .await
        .map_err(|e| anyhow::anyhow!("create_pond: {e}"))?;

    let before = count_control_rows(&control_path).await?;

    // Open and commit a read transaction.  After D5.7a.1, this must not
    // append any rows to the control table.
    let meta = PondUserMetadata::new(vec!["test".into(), "reader".into()]);
    let guard = ship
        .begin_read(&meta)
        .await
        .map_err(|e| anyhow::anyhow!("begin_read: {e}"))?;
    let _ = guard
        .commit()
        .await
        .map_err(|e| anyhow::anyhow!("commit read: {e}"))?;

    let after = count_control_rows(&control_path).await?;
    assert_eq!(
        before, after,
        "read transaction should not append rows to the control table \
         (before={before}, after={after})"
    );
    Ok(())
}

#[tokio::test]
async fn persistent_freeze_blocks_old_and_new_writers_but_allows_reads() -> Result<()> {
    let temp = tempdir()?;
    let pond_path = temp.path().join("pond");
    let mut freezer = Ship::create_pond(&pond_path, "test-host")
        .await
        .map_err(|e| anyhow::anyhow!("create_pond: {e}"))?;
    freezer
        .write_transaction(
            &PondUserMetadata::new(vec!["test".into(), "seed".into()]),
            async |fs| {
                let root = fs.root().await?;
                let _ = root.create_dir_all("/data").await?;
                Ok(())
            },
        )
        .await?;
    let mut already_open = Ship::open_pond(&pond_path)
        .await
        .map_err(|e| anyhow::anyhow!("open pre-freeze writer: {e}"))?;

    let tip_seq = freezer
        .control_table()
        .latest_spine_seq()
        .await?
        .expect("new pond has a content tip");
    let expected_tip = freezer
        .control_table()
        .commit_hash_at(tip_seq)
        .await?
        .expect("tip sequence has a commit hash");

    let freeze_meta = PondUserMetadata::new(vec!["freeze".into(), "enable".into()]);
    let (freeze, created) = freezer
        .freeze_writes(&freeze_meta, "storage format migration".to_string())
        .await?;
    assert!(created);
    assert_eq!(freeze.source_tip.as_deref(), Some(expected_tip.as_str()));
    assert_eq!(
        freezer.write_freeze()?.as_ref(),
        Some(&freeze),
        "freezing Ship sees persisted marker"
    );

    let read_meta = PondUserMetadata::new(vec!["test".into(), "reader".into()]);
    let reader = already_open.begin_read(&read_meta).await?;
    let _read_seq = reader.commit().await?;

    let write_meta = PondUserMetadata::new(vec!["test".into(), "writer".into()]);
    assert!(matches!(
        already_open.begin_write(&write_meta).await,
        Err(StewardError::PondWriteFrozen { .. })
    ));

    let mut newly_opened = Ship::open_pond(&pond_path).await?;
    assert_eq!(newly_opened.write_freeze()?.as_ref(), Some(&freeze));
    assert!(matches!(
        newly_opened.begin_write(&write_meta).await,
        Err(StewardError::PondWriteFrozen { .. })
    ));
    assert!(matches!(
        newly_opened.maintain(true, true).await,
        Err(StewardError::PondWriteFrozen { .. })
    ));
    assert!(matches!(
        newly_opened
            .prune_control_history(1, &PondUserMetadata::new(vec!["prune".into()]))
            .await,
        Err(StewardError::PondWriteFrozen { .. })
    ));

    let removed = freezer
        .unfreeze_writes(&PondUserMetadata::new(vec![
            "freeze".into(),
            "disable".into(),
        ]))?
        .expect("remove persisted freeze");
    assert_eq!(removed, freeze);
    assert!(freezer.write_freeze()?.is_none());

    let writer = already_open.begin_write(&write_meta).await?;
    drop(writer);
    Ok(())
}

#[tokio::test]
async fn freeze_refuses_to_race_an_active_writer() -> Result<()> {
    let temp = tempdir()?;
    let pond_path = temp.path().join("pond");
    let mut writer = Ship::create_pond(&pond_path, "test-host").await?;
    let mut freezer = Ship::open_pond(&pond_path).await?;

    let write_meta = PondUserMetadata::new(vec!["test".into(), "writer".into()]);
    let transaction = writer.begin_write(&write_meta).await?;
    let error = freezer
        .freeze_writes(
            &PondUserMetadata::new(vec!["freeze".into(), "enable".into()]),
            "storage format migration".to_string(),
        )
        .await
        .expect_err("freeze must not race an active writer");
    assert!(matches!(error, StewardError::PondLocked { .. }));
    drop(transaction);
    assert!(freezer.write_freeze()?.is_none());
    Ok(())
}

#[tokio::test]
async fn unfreeze_does_not_require_a_readable_control_table() -> Result<()> {
    let temp = tempdir()?;
    let pond_path = temp.path().join("pond");
    let mut ship = Ship::create_pond(&pond_path, "test-host").await?;
    let (_freeze, created) = ship
        .freeze_writes(
            &PondUserMetadata::new(vec!["freeze".into(), "enable".into()]),
            "control recovery test".to_string(),
        )
        .await?;
    assert!(created);

    let control_path = steward::get_control_path(&pond_path);
    std::fs::rename(
        control_path.join("_delta_log"),
        control_path.join("_delta_log.damaged"),
    )?;
    assert!(Ship::open_pond(&pond_path).await.is_err());

    let removed = steward::unfreeze_pond_writes(
        &pond_path,
        &PondUserMetadata::new(vec!["freeze".into(), "disable".into()]),
    )?
    .expect("remove freeze without opening control table");
    assert_eq!(removed.reason, "control recovery test");
    assert!(steward::read_pond_write_freeze(&pond_path)?.is_none());
    Ok(())
}
