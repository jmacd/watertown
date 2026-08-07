// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! A remote's cost must track the work asked of it, not its age.
//!
//! A traced push of water-prod made 1198 requests to MinIO, of which about
//! 1186 were proportional to history rather than to the push: replaying an
//! uncheckpointed commit log, and reading 197 accumulated data files to
//! allocate one `txn_seq`.  Both grow forever, which is the shape of a
//! runaway bill even when nothing is being uploaded.
//!
//! Request counts cannot be asserted in one process -- delta-rs caches table
//! state, so a second open answers from memory and reports two requests no
//! matter how old the remote is, while production pays because every pond
//! tick is a fresh process opening the remote cold.  These tests therefore
//! assert the two structural properties those requests were being spent on.

use std::collections::HashMap;
use sync_store::Store;
use sync_store::testing::in_memory_remote_url;
use uuid::Uuid;

/// Well past the compaction interval, so the test would notice compaction
/// that runs once and then never again.
const WRITES: usize = 60;

async fn write_many(store: &mut Store, pond_id: Uuid) {
    for i in 0..WRITES {
        let _ = store
            .put(pond_id, "refs", "main", format!("tip{i}").into_bytes())
            .await
            .expect("put");
    }
}

/// The number of files a point lookup must open stays bounded.
///
/// Every write lands a new small file, and a lookup opens every file in the
/// partition, so without merging them the price of reading one key would grow
/// with the number of writes that came before it.
#[tokio::test]
async fn a_remote_does_not_accumulate_a_file_per_write() {
    let pond_id = Uuid::new_v4();
    let url = in_memory_remote_url("file-accumulation");
    let mut store = Store::create_at_url(&url, HashMap::new())
        .await
        .expect("create store");
    write_many(&mut store, pond_id).await;

    let live = store.data_files().expect("data files").len();
    assert!(
        live < WRITES / 2,
        "{WRITES} writes left {live} live data files: a lookup opens every one \
         of them, so this is a cost that grows with the remote's history \
         rather than with the work being done"
    );
}

/// Allocating the next `txn_seq` reads the log, not the data.
///
/// The count must survive a reopen: if it did not, the fallback scan over
/// every data file would silently come back, which is the linear cost this
/// replaced.
#[tokio::test]
async fn the_next_txn_seq_is_read_from_the_log() {
    let pond_id = Uuid::new_v4();
    let url = in_memory_remote_url("txn-seq-from-log");
    let mut store = Store::create_at_url(&url, HashMap::new())
        .await
        .expect("create store");
    write_many(&mut store, pond_id).await;
    drop(store);

    let store = Store::open_at_url(&url, HashMap::new())
        .await
        .expect("reopen store");
    let recorded = store
        .app_transaction_version(pond_id)
        .await
        .expect("app transaction");
    assert_eq!(
        recorded,
        Some(WRITES as i64),
        "the commit must carry the pond's txn_seq as a Delta app-transaction, \
         or allocation falls back to scanning every data file"
    );
    assert_eq!(
        store.last_txn_seq(pond_id).await.expect("last txn seq"),
        WRITES as i64
    );
}
