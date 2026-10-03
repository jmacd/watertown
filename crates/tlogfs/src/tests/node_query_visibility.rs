// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Tests for node-scoped committed queries and pending-write visibility.
//!
//! These tests verify that reads within the same transaction always see
//! pending writes after node-scoped committed data has been queried.

use crate::persistence::OpLogPersistence;
use arrow_array::record_batch;
use log::debug;
use tinyfs::arrow::ParquetExt;

use super::test_dir;

/// After reading a committed file, a new file in the same directory remains
/// visible and readable in the same transaction.
#[tokio::test]
async fn test_node_query_then_new_file_same_partition() {
    let store_path = test_dir();

    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("create persistence");

    // Transaction 1: create a file and commit.
    {
        let tx = persistence.begin_test().await.expect("begin tx1");
        let root = tx.root().await.expect("root");

        let batch = record_batch!(
            ("timestamp", Int64, [1_000_000_i64]),
            ("value", Float64, [1.0_f64])
        )
        .expect("batch");

        _ = root
            .create_series_from_batch("first.series", &batch, Some("timestamp"))
            .await
            .expect("create first.series");

        tx.commit_test().await.expect("commit tx1");
    }

    // Transaction 2: read committed state, then create a visible second file.
    {
        let tx = persistence.begin_test().await.expect("begin tx2");
        let root = tx.root().await.expect("root");

        // Read committed node state.
        let b1 = root
            .read_table_as_batch("first.series")
            .await
            .expect("read first.series");
        assert_eq!(b1.num_rows(), 1, "first.series should have 1 row");
        debug!("[OK] read first.series: {} rows", b1.num_rows());

        // Create second file in the same partition (root directory).
        let batch2 = record_batch!(
            ("timestamp", Int64, [2_000_000_i64]),
            ("value", Float64, [2.0_f64])
        )
        .expect("batch2");

        _ = root
            .create_series_from_batch("second.series", &batch2, Some("timestamp"))
            .await
            .expect("create second.series");
        debug!("[OK] created second.series");

        // The new file must be visible.
        let exists = root.exists(std::path::Path::new("second.series")).await;
        assert!(exists, "second.series must be visible after creation");

        // Must also be readable.
        let b2 = root
            .read_table_as_batch("second.series")
            .await
            .expect("read second.series");
        assert_eq!(b2.num_rows(), 1, "second.series should have 1 row");
        debug!("[OK] second.series readable: {} rows", b2.num_rows());

        // Original file must still be readable too.
        let b1_again = root
            .read_table_as_batch("first.series")
            .await
            .expect("re-read first.series");
        assert_eq!(b1_again.num_rows(), 1);
        debug!("[OK] first.series still readable");

        tx.commit_test().await.expect("commit tx2");
    }
}

/// After reading a committed file, appending in the same transaction must
/// expose the new latest version.
#[tokio::test]
async fn test_node_query_then_append_version() {
    let store_path = test_dir();

    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("create persistence");

    // Transaction 1: create a series with one row and commit.
    {
        let tx = persistence.begin_test().await.expect("begin tx1");
        let root = tx.root().await.expect("root");

        let batch = record_batch!(
            ("timestamp", Int64, [1_000_000_i64]),
            ("value", Float64, [1.0_f64])
        )
        .expect("batch");

        _ = root
            .create_series_from_batch("sensor.series", &batch, Some("timestamp"))
            .await
            .expect("create series");

        tx.commit_test().await.expect("commit tx1");
    }

    // Transaction 2: read, append, and verify latest.
    {
        let tx = persistence.begin_test().await.expect("begin tx2");
        let root = tx.root().await.expect("root");

        // Read v1 from committed state.
        let b1 = root
            .read_table_as_batch("sensor.series")
            .await
            .expect("read v1");
        assert_eq!(b1.num_rows(), 1, "v1 should have 1 row");
        debug!("[OK] read v1: {} rows", b1.num_rows());

        // Append v2.
        let batch2 = record_batch!(
            ("timestamp", Int64, [2_000_000_i64]),
            ("value", Float64, [99.0_f64])
        )
        .expect("batch2");

        _ = root
            .write_series_from_batch("sensor.series", &batch2, Some("timestamp"))
            .await
            .expect("append v2");
        debug!("[OK] appended v2");

        // read_table_as_batch reads the LATEST single version.
        // After appending v2, it should return v2's data (1 row, value=99).
        let b2 = root
            .read_table_as_batch("sensor.series")
            .await
            .expect("read after append");
        assert_eq!(b2.num_rows(), 1, "latest version should have 1 row");

        // Verify it is the NEW version's data.
        let values = b2
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .expect("float64 column");
        assert!(
            (values.value(0) - 99.0).abs() < f64::EPSILON,
            "latest version should contain v2 data (99.0), got {}",
            values.value(0)
        );
        debug!("[OK] latest version has v2 data: {}", values.value(0));

        tx.commit_test().await.expect("commit tx2");
    }
}

/// Writing a new file after listing a directory must remain visible.
#[tokio::test]
async fn test_directory_query_then_new_file_listing() {
    use futures::stream::StreamExt;

    let store_path = test_dir();

    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("create persistence");

    // Transaction 1: create a subdirectory with one file.
    {
        let tx = persistence.begin_test().await.expect("begin tx1");
        let root = tx.root().await.expect("root");

        _ = root.create_dir_all("data").await.expect("mkdir data");

        let batch = record_batch!(
            ("timestamp", Int64, [100_i64]),
            ("value", Float64, [10.0_f64])
        )
        .expect("batch");

        _ = root
            .create_series_from_batch("data/first.series", &batch, Some("timestamp"))
            .await
            .expect("create first.series");

        tx.commit_test().await.expect("commit tx1");
    }

    // Transaction 2: list the directory, then add and observe a second file.
    {
        let tx = persistence.begin_test().await.expect("begin tx2");
        let root = tx.root().await.expect("root");

        // List committed /data contents.
        let data_dir = root.open_dir_path("data").await.expect("open data");
        let mut stream = data_dir.entries().await.expect("entries before");
        let mut entries_before = Vec::new();
        while let Some(entry_result) = stream.next().await {
            let entry = entry_result.expect("entry");
            entries_before.push(entry.name.clone());
        }
        assert_eq!(entries_before.len(), 1, "should have 1 entry before");
        assert_eq!(entries_before[0], "first.series");
        debug!("[OK] listed before: {:?}", entries_before);

        // Create a second file in the same directory.
        let batch2 = record_batch!(
            ("timestamp", Int64, [200_i64]),
            ("value", Float64, [20.0_f64])
        )
        .expect("batch2");

        _ = root
            .create_series_from_batch("data/second.series", &batch2, Some("timestamp"))
            .await
            .expect("create second.series");
        debug!("[OK] created second.series");

        // Re-list -- must see both files.
        let data_dir2 = root.open_dir_path("data").await.expect("open data again");
        let mut stream2 = data_dir2.entries().await.expect("entries after");
        let mut entries_after = Vec::new();
        while let Some(entry_result) = stream2.next().await {
            let entry = entry_result.expect("entry");
            entries_after.push(entry.name.clone());
        }
        entries_after.sort();
        assert_eq!(
            entries_after.len(),
            2,
            "should have 2 entries after adding second.series"
        );
        assert_eq!(entries_after[0], "first.series");
        assert_eq!(entries_after[1], "second.series");
        debug!("[OK] listed after: {:?}", entries_after);

        tx.commit_test().await.expect("commit tx2");
    }
}

/// A new transaction sees committed writes from the preceding transaction.
#[tokio::test]
async fn test_node_queries_are_fresh_across_transactions() {
    let store_path = test_dir();

    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("create persistence");

    // Transaction 1: create two files and commit.
    {
        let tx = persistence.begin_test().await.expect("begin tx1");
        let root = tx.root().await.expect("root");

        let batch = record_batch!(("timestamp", Int64, [1_i64]), ("value", Float64, [1.0_f64]))
            .expect("batch");

        _ = root
            .create_series_from_batch("a.series", &batch, Some("timestamp"))
            .await
            .expect("create a");

        tx.commit_test().await.expect("commit tx1");
    }

    // Transaction 2: add a second file and commit.
    {
        let tx = persistence.begin_test().await.expect("begin tx2");
        let root = tx.root().await.expect("root");

        // Read committed a.series.
        let b = root
            .read_table_as_batch("a.series")
            .await
            .expect("read a in tx2");
        assert_eq!(b.num_rows(), 1);

        // Create b.series.
        let batch2 = record_batch!(("timestamp", Int64, [2_i64]), ("value", Float64, [2.0_f64]))
            .expect("batch2");

        _ = root
            .create_series_from_batch("b.series", &batch2, Some("timestamp"))
            .await
            .expect("create b in tx2");

        tx.commit_test().await.expect("commit tx2");
    }

    // Transaction 3 must see both committed files from Delta Lake.
    {
        let tx = persistence.begin_test().await.expect("begin tx3");
        let root = tx.root().await.expect("root");

        let exists_a = root.exists(std::path::Path::new("a.series")).await;
        let exists_b = root.exists(std::path::Path::new("b.series")).await;
        assert!(exists_a, "a.series should exist in tx3");
        assert!(exists_b, "b.series should exist in tx3");

        let ba = root
            .read_table_as_batch("a.series")
            .await
            .expect("read a in tx3");
        let bb = root
            .read_table_as_batch("b.series")
            .await
            .expect("read b in tx3");
        assert_eq!(ba.num_rows(), 1, "a.series should have 1 row");
        assert_eq!(bb.num_rows(), 1, "b.series should have 1 row");
        debug!("[OK] tx3 sees both committed files");
    }
}
