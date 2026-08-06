// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! A remote's read cost must track the work being asked of it, not the age of
//! the table.
//!
//! Without a checkpoint, `_delta_log/_last_checkpoint` never appears and every
//! reader replays the whole commit history to learn the current state.  A
//! traced push of a production pond spent 609 of its 1198 requests doing
//! exactly that, and the count grows forever.  These tests hold the line.

use object_store::ObjectStore;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use sync_store::Store;
use sync_store::metered_store::{StorageMeter, with_meter};
use sync_store::testing::in_memory_remote_url;
use uuid::Uuid;

#[derive(Debug, Default)]
struct CountingMeter {
    ops: AtomicU64,
}

impl StorageMeter for CountingMeter {
    fn check(&self, _ops: u64, _bytes: u64) -> Result<(), String> {
        Ok(())
    }

    fn record(&self, ops: u64, _bytes: u64) {
        let _ = self.ops.fetch_add(ops, Ordering::Relaxed);
    }
}

/// Enough versions to cross the checkpoint interval twice over, so the test
/// would notice a checkpoint that is written once and then never again.
const WRITES: usize = 25;

async fn write_versions(store: &mut Store, pond_id: Uuid) {
    for i in 0..WRITES {
        let _ = store
            .put(pond_id, "data", &format!("key{i}"), vec![b'x'; 32])
            .await
            .expect("put");
    }
}

/// Committing past the interval leaves a checkpoint behind.
#[tokio::test]
async fn a_busy_remote_writes_a_checkpoint() {
    let pond_id = Uuid::new_v4();
    let url = in_memory_remote_url("checkpoint-written");
    let mut store = Store::create_at_url(&url, [].into())
        .await
        .expect("create store");
    write_versions(&mut store, pond_id).await;

    let last = sync_store::testing::in_memory_backing()
        .head(
            &object_store::path::Path::parse(
                "checkpoint-written/remote/_delta_log/_last_checkpoint",
            )
            .expect("path"),
        )
        .await;
    assert!(
        last.is_ok(),
        "a remote committed to {WRITES} times must have a checkpoint; \
         without one every reader replays the whole log: {last:?}"
    );
}

/// Opening a long-lived remote costs a small constant, not one read per
/// version.  This is the property the checkpoint exists to buy, and the one a
/// budget would otherwise be spent on.
#[tokio::test]
async fn opening_a_remote_does_not_pay_per_version() {
    let pond_id = Uuid::new_v4();
    let url = in_memory_remote_url("checkpoint-read-cost");
    let mut store = Store::create_at_url(&url, [].into())
        .await
        .expect("create store");
    write_versions(&mut store, pond_id).await;
    drop(store);

    let meter = Arc::new(CountingMeter::default());
    let counted: Arc<dyn StorageMeter> = meter.clone();
    let opened = with_meter(counted, async {
        Store::open_at_url(&url, [].into())
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    })
    .await;
    assert!(opened.is_ok(), "open: {opened:?}");

    let ops = meter.ops.load(Ordering::Relaxed);
    assert!(
        ops < WRITES as u64,
        "opening a remote with {WRITES} versions cost {ops} requests, which is \
         the per-version log replay a checkpoint is supposed to collapse"
    );
}
