// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the [`Store`] type.

use sync_store::{Op, Store};
use tempfile::TempDir;
use uuid::Uuid;

fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

/// Test pond_id "A".  Distinct from `pid_b()` so cross-pond mistakes
/// surface in test failures rather than passing silently.
fn pid_a() -> Uuid {
    Uuid::from_u128(0xa1_0000_0000_0000_0000_0000_0000_0000)
}

/// Test pond_id "B".
fn pid_b() -> Uuid {
    Uuid::from_u128(0xb2_0000_0000_0000_0000_0000_0000_0000)
}

#[tokio::test]
async fn create_then_open_roundtrip() {
    init_logger();
    let dir = TempDir::new().unwrap();
    {
        let _store = Store::create(dir.path()).await.expect("create");
    }
    let store = Store::open(dir.path()).await.expect("open");
    assert_eq!(store.last_txn_seq(pid_a()).await.unwrap(), 0);
    assert!(store.partitions(pid_a()).await.unwrap().is_empty());
}

#[tokio::test]
async fn create_fails_if_already_exists() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let _ = Store::create(dir.path()).await.unwrap();
    let err = Store::create(dir.path()).await;
    assert!(err.is_err(), "second create must fail");
}

#[tokio::test]
async fn put_then_get_returns_value() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let seq = store
        .put(pid_a(), "p1", "k1", b"hello".to_vec())
        .await
        .unwrap();
    assert_eq!(seq, 1);
    let v = store.get(pid_a(), "p1", "k1").await.unwrap();
    assert_eq!(v, Some(b"hello".to_vec()));
}

#[tokio::test]
async fn get_returns_none_for_missing_partition() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let store = Store::create(dir.path()).await.unwrap();
    let v = store.get(pid_a(), "nope", "nope").await.unwrap();
    assert_eq!(v, None);
}

#[tokio::test]
async fn get_returns_none_for_missing_key() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store
        .put(pid_a(), "p1", "k1", b"v1".to_vec())
        .await
        .unwrap();
    let v = store.get(pid_a(), "p1", "k2").await.unwrap();
    assert_eq!(v, None);
}

#[tokio::test]
async fn get_many_returns_only_requested_latest_live_values() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store.put(pid_a(), "p1", "a", b"A1".to_vec()).await.unwrap();
    let _ = store.put(pid_a(), "p1", "b", b"B1".to_vec()).await.unwrap();
    let _ = store.put(pid_a(), "p1", "a", b"A2".to_vec()).await.unwrap();
    let _ = store.delete(pid_a(), "p1", "b").await.unwrap();
    let _ = store
        .put(pid_a(), "p1", "other", b"X".to_vec())
        .await
        .unwrap();
    let _ = store
        .put(pid_a(), "p2", "a", b"wrong partition".to_vec())
        .await
        .unwrap();
    let _ = store
        .put(pid_b(), "p1", "a", b"wrong pond".to_vec())
        .await
        .unwrap();

    let values = store
        .get_many(
            pid_a(),
            "p1",
            &["missing".into(), "b".into(), "a".into(), "a".into()],
        )
        .await
        .unwrap();

    assert_eq!(values.len(), 1);
    assert_eq!(values.get("a"), Some(&b"A2".to_vec()));
}

#[tokio::test]
async fn get_many_empty_request_reads_nothing() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let store = Store::create(dir.path()).await.unwrap();
    assert!(store.get_many(pid_a(), "p1", &[]).await.unwrap().is_empty());
}

#[tokio::test]
async fn put_overwrites_returns_latest() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store
        .put(pid_a(), "p1", "k1", b"v1".to_vec())
        .await
        .unwrap();
    let _ = store
        .put(pid_a(), "p1", "k1", b"v2".to_vec())
        .await
        .unwrap();
    let _ = store
        .put(pid_a(), "p1", "k1", b"v3".to_vec())
        .await
        .unwrap();
    let v = store.get(pid_a(), "p1", "k1").await.unwrap();
    assert_eq!(v, Some(b"v3".to_vec()));
    assert_eq!(store.last_txn_seq(pid_a()).await.unwrap(), 3);
}

#[tokio::test]
async fn delete_makes_get_return_none() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store
        .put(pid_a(), "p1", "k1", b"v1".to_vec())
        .await
        .unwrap();
    let _ = store.delete(pid_a(), "p1", "k1").await.unwrap();
    assert_eq!(store.get(pid_a(), "p1", "k1").await.unwrap(), None);
}

#[tokio::test]
async fn put_after_delete_resurrects_item() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store
        .put(pid_a(), "p1", "k1", b"v1".to_vec())
        .await
        .unwrap();
    let _ = store.delete(pid_a(), "p1", "k1").await.unwrap();
    let _ = store
        .put(pid_a(), "p1", "k1", b"v2".to_vec())
        .await
        .unwrap();
    assert_eq!(
        store.get(pid_a(), "p1", "k1").await.unwrap(),
        Some(b"v2".to_vec())
    );
}

#[tokio::test]
async fn delete_of_missing_key_is_a_tombstone_with_no_observable_effect() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store.delete(pid_a(), "p1", "ghost").await.unwrap();
    assert_eq!(store.get(pid_a(), "p1", "ghost").await.unwrap(), None);
}

#[tokio::test]
async fn list_returns_live_items_sorted_excluding_tombstones() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store.put(pid_a(), "p1", "b", b"B".to_vec()).await.unwrap();
    let _ = store.put(pid_a(), "p1", "a", b"A".to_vec()).await.unwrap();
    let _ = store.put(pid_a(), "p1", "c", b"C".to_vec()).await.unwrap();
    let _ = store.delete(pid_a(), "p1", "b").await.unwrap();
    let listed = store.list(pid_a(), "p1").await.unwrap();
    assert_eq!(
        listed,
        vec![
            ("a".to_string(), b"A".to_vec()),
            ("c".to_string(), b"C".to_vec()),
        ]
    );
}

#[tokio::test]
async fn list_empty_partition_returns_empty_vec() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let store = Store::create(dir.path()).await.unwrap();
    assert!(store.list(pid_a(), "nope").await.unwrap().is_empty());
}

#[tokio::test]
async fn partitions_lists_all_keys_used() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store
        .put(pid_a(), "alpha", "k", b"v".to_vec())
        .await
        .unwrap();
    let _ = store
        .put(pid_a(), "beta", "k", b"v".to_vec())
        .await
        .unwrap();
    let _ = store
        .put(pid_a(), "alpha", "k2", b"v".to_vec())
        .await
        .unwrap();
    let parts = store.partitions(pid_a()).await.unwrap();
    assert_eq!(parts, vec!["alpha".to_string(), "beta".to_string()]);
}

#[tokio::test]
async fn partitions_includes_partitions_with_only_tombstones() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store
        .put(pid_a(), "ghosts", "k", b"v".to_vec())
        .await
        .unwrap();
    let _ = store.delete(pid_a(), "ghosts", "k").await.unwrap();
    assert_eq!(
        store.partitions(pid_a()).await.unwrap(),
        vec!["ghosts".to_string()]
    );
    assert!(store.list(pid_a(), "ghosts").await.unwrap().is_empty());
}

#[tokio::test]
async fn apply_batch_writes_one_commit_for_many_ops() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();

    let v0 = store.delta_version();
    store
        .apply_batch(
            pid_a(),
            42,
            1_700_000_000_000_000,
            vec![
                Op::Put {
                    partition: "p".into(),
                    key: "a".into(),
                    value: b"A".to_vec(),
                },
                Op::Put {
                    partition: "p".into(),
                    key: "b".into(),
                    value: b"B".to_vec(),
                },
                Op::Put {
                    partition: "q".into(),
                    key: "x".into(),
                    value: b"X".to_vec(),
                },
            ],
        )
        .await
        .unwrap();
    let v1 = store.delta_version();
    assert_eq!(v1, v0 + 1, "exactly one Delta commit for the batch");
    assert_eq!(store.last_txn_seq(pid_a()).await.unwrap(), 42);
    assert_eq!(
        store.get(pid_a(), "p", "a").await.unwrap(),
        Some(b"A".to_vec())
    );
    assert_eq!(
        store.get(pid_a(), "p", "b").await.unwrap(),
        Some(b"B".to_vec())
    );
    assert_eq!(
        store.get(pid_a(), "q", "x").await.unwrap(),
        Some(b"X".to_vec())
    );
}

#[tokio::test]
async fn apply_batch_coalesces_repeated_ops_on_same_key() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();

    store
        .apply_batch(
            pid_a(),
            1,
            0,
            vec![
                Op::Put {
                    partition: "p".into(),
                    key: "k".into(),
                    value: b"first".to_vec(),
                },
                Op::Put {
                    partition: "p".into(),
                    key: "k".into(),
                    value: b"middle".to_vec(),
                },
                Op::Delete {
                    partition: "p".into(),
                    key: "k".into(),
                },
                Op::Put {
                    partition: "p".into(),
                    key: "k".into(),
                    value: b"last".to_vec(),
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        store.get(pid_a(), "p", "k").await.unwrap(),
        Some(b"last".to_vec())
    );
}

#[tokio::test]
async fn apply_batch_empty_is_a_noop() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let v_before = store.delta_version();
    store.apply_batch(pid_a(), 99, 0, vec![]).await.unwrap();
    let v_after = store.delta_version();
    assert_eq!(v_before, v_after, "empty batch must NOT produce a commit");
    assert_eq!(store.last_txn_seq(pid_a()).await.unwrap(), 0);
}

#[tokio::test]
async fn cross_partition_writes_are_independent() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store
        .put(pid_a(), "p1", "k", b"v_p1".to_vec())
        .await
        .unwrap();
    let _ = store
        .put(pid_a(), "p2", "k", b"v_p2".to_vec())
        .await
        .unwrap();
    assert_eq!(
        store.get(pid_a(), "p1", "k").await.unwrap(),
        Some(b"v_p1".to_vec())
    );
    assert_eq!(
        store.get(pid_a(), "p2", "k").await.unwrap(),
        Some(b"v_p2".to_vec())
    );
    assert_eq!(store.list(pid_a(), "p1").await.unwrap().len(), 1);
    assert_eq!(store.list(pid_a(), "p2").await.unwrap().len(), 1);
}

#[tokio::test]
async fn empty_value_is_distinguishable_from_tombstone() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store.put(pid_a(), "p", "k", Vec::new()).await.unwrap();
    assert_eq!(
        store.get(pid_a(), "p", "k").await.unwrap(),
        Some(Vec::new()),
        "empty-bytes value is a real value, not absence"
    );
    let _ = store.delete(pid_a(), "p", "k").await.unwrap();
    assert_eq!(store.get(pid_a(), "p", "k").await.unwrap(), None);
}

#[tokio::test]
async fn keys_with_sql_metacharacters_roundtrip() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let weird = "weird'key;--";
    let _ = store.put(pid_a(), "p", weird, b"v".to_vec()).await.unwrap();
    assert_eq!(
        store.get(pid_a(), "p", weird).await.unwrap(),
        Some(b"v".to_vec())
    );
    let listed = store.list(pid_a(), "p").await.unwrap();
    assert_eq!(listed, vec![(weird.to_string(), b"v".to_vec())]);
}

#[tokio::test]
async fn actions_at_version_returns_adds_for_a_write() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let _ = store.put(pid_a(), "p", "k", b"v".to_vec()).await.unwrap();
    let v = store.delta_version();
    assert!(v > 0, "version advanced past initial");

    let (adds, removes) = store.actions_at_version(v).await.unwrap();
    assert!(!adds.is_empty(), "write produces at least one Add action");
    assert!(removes.is_empty(), "first write has no Remove actions");
    for add in &adds {
        assert!(add.size > 0, "Add reports a positive file size");
        assert!(!add.path.is_empty(), "Add has a path");
    }

    // Add paths must be a subset of the table's current data files
    // (they were just added; nothing's been removed).
    let current: std::collections::HashSet<_> = store
        .data_files()
        .unwrap()
        .into_iter()
        // Strip any Url scheme prefix; data_files returns absolute
        // file:// URIs while Add paths are relative.
        .map(|p| {
            p.rsplit_once('/')
                .map(|(_, name)| name.to_string())
                .unwrap_or(p)
        })
        .collect();
    for add in &adds {
        let leaf = add
            .path
            .rsplit_once('/')
            .map(|(_, name)| name.to_string())
            .unwrap_or_else(|| add.path.clone());
        assert!(
            current.contains(&leaf),
            "Add file leaf `{}` (full `{}`) appears in current data_files",
            leaf,
            add.path,
        );
    }
}

#[tokio::test]
async fn actions_at_version_returns_adds_and_removes_for_a_compact() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    // Three small writes -> three small parquets in partition `p`.
    for i in 0..3 {
        let _ = store
            .put(pid_a(), "p", &format!("k{}", i), b"v".to_vec())
            .await
            .unwrap();
    }
    let pre_version = store.delta_version();
    let metrics = store.compact(pid_a(), None).await.unwrap();

    if metrics.is_noop() {
        // delta-rs decided nothing to merge for these tiny files; can't
        // exercise the Adds-and-Removes assertion. Still verify the
        // pre-compact write's actions are reachable.
        let (adds, removes) = store.actions_at_version(pre_version).await.unwrap();
        assert!(!adds.is_empty());
        assert!(removes.is_empty());
        return;
    }

    let post_version = store.delta_version();
    assert!(post_version > pre_version, "compact advanced version");

    let (adds, removes) = store.actions_at_version(post_version).await.unwrap();
    assert!(
        !adds.is_empty(),
        "compact recorded at least one Add (merged)"
    );
    assert!(
        !removes.is_empty(),
        "compact recorded Removes for the small parquets it merged",
    );
    assert_eq!(
        adds.len() as u64,
        metrics.num_files_added,
        "Add count matches metrics.num_files_added",
    );
    assert_eq!(
        removes.len() as u64,
        metrics.num_files_removed,
        "Remove count matches metrics.num_files_removed",
    );
}

#[tokio::test]
async fn actions_at_version_errors_on_missing_version() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let store = Store::create(dir.path()).await.unwrap();
    // No writes; delta-rs only has the initial schema commit at version 0.
    let result = store.actions_at_version(99).await;
    assert!(result.is_err(), "missing version errors");
}

// ---- library-api-coverage gap-filling test ----

#[tokio::test]
async fn commit_actions_directly_writes_add_remove() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();
    let initial_version = store.delta_version();

    use std::collections::HashMap;
    let mut partition_values = HashMap::new();
    let _ = partition_values.insert("pond_id".to_string(), Some(pid_a().to_string()));
    let _ = partition_values.insert("partition_key".to_string(), Some("p".to_string()));

    let add = deltalake::kernel::Add {
        path: format!("pond_id={}/partition_key=p/test-001.parquet", pid_a()),
        partition_values,
        size: 0,
        modification_time: 0,
        data_change: true,
        ..Default::default()
    };
    let actions = vec![deltalake::kernel::Action::Add(add)];
    let new_version = store
        .commit_actions(
            actions,
            deltalake::protocol::DeltaOperation::Write {
                mode: deltalake::protocol::SaveMode::Append,
                partition_by: Some(vec!["pond_id".to_string(), "partition_key".to_string()]),
                predicate: None,
            },
        )
        .await
        .unwrap();

    assert!(
        new_version > initial_version,
        "commit_actions advanced delta version"
    );

    let (adds, removes) = store.actions_at_version(new_version).await.unwrap();
    assert_eq!(adds.len(), 1, "one add committed");
    assert!(removes.is_empty(), "no removes");
    assert_eq!(
        adds[0].path,
        format!("pond_id={}/partition_key=p/test-001.parquet", pid_a())
    );
}

#[tokio::test]
async fn rows_are_isolated_by_pond_id() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();

    // Two different ponds writing to the same (partition, key) namespace.
    let _ = store
        .put(pid_a(), "p1", "k", b"from_a".to_vec())
        .await
        .unwrap();
    let _ = store
        .put(pid_b(), "p1", "k", b"from_b".to_vec())
        .await
        .unwrap();

    // Reads scoped to each pond return the right value.
    assert_eq!(
        store.get(pid_a(), "p1", "k").await.unwrap(),
        Some(b"from_a".to_vec())
    );
    assert_eq!(
        store.get(pid_b(), "p1", "k").await.unwrap(),
        Some(b"from_b".to_vec())
    );

    // Listing is per-pond.
    assert_eq!(store.list(pid_a(), "p1").await.unwrap().len(), 1);
    assert_eq!(store.list(pid_b(), "p1").await.unwrap().len(), 1);

    // Per-pond seq spaces.
    assert_eq!(store.last_txn_seq(pid_a()).await.unwrap(), 1);
    assert_eq!(store.last_txn_seq(pid_b()).await.unwrap(), 1);

    // Both ponds appear in all_partitions.
    let all = store.all_partitions().await.unwrap();
    assert!(all.contains(&(pid_a(), "p1".to_string())));
    assert!(all.contains(&(pid_b(), "p1".to_string())));
}

#[tokio::test]
async fn compact_is_pond_scoped_only() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let mut store = Store::create(dir.path()).await.unwrap();

    // Several small writes by pond A.
    for i in 0..3 {
        let _ = store
            .put(pid_a(), "p", &format!("k{}", i), b"v".to_vec())
            .await
            .unwrap();
    }
    // One write by pond B in a same-named partition.
    let _ = store.put(pid_b(), "p", "k0", b"vb".to_vec()).await.unwrap();

    // Compact A's partition.  Pond B's data file must remain untouched.
    let pre_files: std::collections::HashSet<_> = store.data_files().unwrap().into_iter().collect();
    let _metrics = store.compact(pid_a(), Some("p")).await.unwrap();
    let post_files: std::collections::HashSet<_> =
        store.data_files().unwrap().into_iter().collect();

    // B's content is still readable.
    assert_eq!(
        store.get(pid_b(), "p", "k0").await.unwrap(),
        Some(b"vb".to_vec())
    );

    // Any file removed by compact must have been an A file (its path
    // contains pond_id=A's UUID).  This is implicit in delta partition
    // semantics but worth asserting once.
    let removed: Vec<_> = pre_files.difference(&post_files).collect();
    let pid_a_str = pid_a().to_string();
    for f in removed {
        assert!(
            f.contains(&pid_a_str),
            "compact removed a file not in pond A: {}",
            f
        );
    }
}
