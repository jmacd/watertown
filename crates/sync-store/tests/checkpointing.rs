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
use sync_store::Store;
use sync_store::testing::in_memory_remote_url;
use uuid::Uuid;

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
