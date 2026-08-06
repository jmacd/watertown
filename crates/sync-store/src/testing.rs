// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! A `gs://` backend held in memory, so tests can exercise the same metered
//! object store production uses without reaching a network.
//!
//! Metering deliberately does not cover the `file` scheme.  A local pond's own
//! Delta table speaks that scheme, and a push reads the pond while it writes
//! the remote, so a blanket wrap would charge local reads to the remote's
//! budget.  A file-backed remote is therefore unmetered and cannot be used to
//! test what a push spends.
//!
//! `gs://` is recognised by delta-rs, is never used by a local pond, and is
//! not a scheme watertown otherwise supports, so swapping in an in-memory
//! store keeps such tests end-to-end: every byte and request they charge is
//! one the Delta protocol really made.
//!
//! The scheme matters.  Registration is process-wide and last-writer-wins, so
//! an `s3://` backend would be replaced the moment any sibling test in the
//! same binary applied a storage profile and registered the real S3 handlers
//! -- the tests would then try to reach a network.  Nothing registers `gs://`.

use crate::metered_store::MeteredStore;
use deltalake::logstore::{
    LogStore, LogStoreFactory, ObjectStoreFactory, ObjectStoreRef, StorageConfig, default_logstore,
    logstore_factories, object_store_factories,
};
use deltalake::{DeltaResult, Path};
use object_store::memory::InMemory;
use std::sync::{Arc, LazyLock, Once};
use url::Url;

/// One process-wide store; callers are isolated by the path within it.
static BACKING: LazyLock<Arc<InMemory>> = LazyLock::new(|| Arc::new(InMemory::new()));
static REGISTER: Once = Once::new();

#[derive(Clone, Default, Debug)]
struct InMemoryGcsFactory {}

impl ObjectStoreFactory for InMemoryGcsFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        _config: &StorageConfig,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        // The bucket is the URL host rather than part of the key, so fold it
        // into the prefix to keep buckets distinct in the one backing store.
        let bucket = url.host_str().unwrap_or("bucket");
        let prefix = Path::parse(format!("{bucket}{}", url.path()))?;
        Ok((Arc::new(MeteredStore::new(BACKING.clone())), prefix))
    }
}

#[derive(Clone, Default, Debug)]
struct InMemoryGcsLogStoreFactory {}

impl LogStoreFactory for InMemoryGcsLogStoreFactory {
    fn with_options(
        &self,
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(
            prefixed_store,
            root_store,
            location,
            options,
        ))
    }
}

/// Register the in-memory `gs://` backend.  Idempotent.
pub fn register_in_memory_backend() {
    REGISTER.call_once(|| {
        let url = Url::parse("gs://").expect("valid scheme URL");
        let _ =
            object_store_factories().insert(url.clone(), Arc::new(InMemoryGcsFactory::default()));
        let _ = logstore_factories().insert(url, Arc::new(InMemoryGcsLogStoreFactory::default()));
    });
}

/// A metered `gs://` URL unique to `name`, with the backend registered.
///
/// Names must be distinct across tests in a process: delta-rs keys a table by
/// its path, so a shared path is a table two tests would both try to create.
pub fn in_memory_remote_url(name: &str) -> String {
    register_in_memory_backend();
    format!("gs://watertown-test/{name}/remote")
}
