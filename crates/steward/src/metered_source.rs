// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! A [`ContentSource`] whose physical storage traffic is charged to a budget.
//!
//! # What this wrapper is still for
//!
//! Not for charging: the budget is bound to the remote's URL for as long as
//! the guard lives, so every request the inner source makes is charged whether
//! it passes through this type or not.  A pull also reads the *local* pond,
//! and that traffic is structurally outside the budget because it goes to a
//! different remote -- a fact of where the bytes go rather than of when the
//! call was made.
//!
//! What is left is translation.  A budget's refusal reaches a caller flattened
//! into an `object_store` error, and [`Self::charged`] turns it back into
//! [`StewardError::RateLimited`] so an exhausted limit reads as a throttle
//! rather than as an outage.  Holding the guard here also ties the budget's
//! lifetime to the source's, which is what keeps a [`BlobReader`] metered
//! while it drains.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::StewardError;
use crate::content_source::{BlobReader, ContentSource};
use crate::limiter::LimiterSet;
use crate::storage_meter::MeterGuard;
use sync_store::PublicationState;
use sync_store::content::{ObjectHash, PackDescriptor, PublicationRecord};

/// Wraps a source so every request it makes to a remote store is charged.
pub struct MeteredSource {
    inner: Arc<dyn ContentSource>,
    guard: MeterGuard,
}

impl MeteredSource {
    /// Take `limits` for the wrapper's lifetime, leaving the caller's set
    /// empty until [`Self::finish`] returns it.
    pub fn new(inner: Arc<dyn ContentSource>, url: &str, limits: &mut LimiterSet) -> Self {
        Self {
            inner,
            guard: MeterGuard::new(url, limits),
        }
    }

    /// Adopt a guard that is already charging, so the work that produced
    /// `inner` is billed to the same budget as the work done through it.
    ///
    /// Opening a remote is not a local act -- it lists the log and reads every
    /// commit since the last checkpoint -- so the open belongs inside the
    /// budget rather than in front of it.
    pub fn with_guard(inner: Arc<dyn ContentSource>, guard: MeterGuard) -> Self {
        Self { inner, guard }
    }

    /// Return the spending to `limits`, and report the refusal that stopped
    /// the work if a budget said no.
    pub fn finish(self, limits: &mut LimiterSet) -> Option<StewardError> {
        self.guard.finish(limits)
    }

    /// Map a storage-layer failure back to the refusal that caused it.
    ///
    /// The object store can only return its own error type, so a budget's
    /// "no" arrives flattened into a string.  Preferring the recorded refusal
    /// keeps [`StewardError::RateLimited`] intact, which is what tells an
    /// operator nothing is broken.
    fn charged<T>(&self, outcome: Result<T, StewardError>) -> Result<T, StewardError> {
        match outcome {
            Err(e) => Err(self.guard.refusal().unwrap_or(e)),
            ok => ok,
        }
    }
}

#[async_trait]
impl ContentSource for MeteredSource {
    fn pond_id(&self) -> Uuid {
        self.inner.pond_id()
    }

    async fn get_tip(&self, ref_name: &str) -> Result<Option<ObjectHash>, StewardError> {
        let outcome = self.inner.get_tip(ref_name).await;
        self.charged(outcome)
    }

    async fn get_publication_state(
        &self,
        ref_name: &str,
    ) -> Result<Option<PublicationState>, StewardError> {
        let outcome = self.inner.get_publication_state(ref_name).await;
        self.charged(outcome)
    }

    async fn get_publication_record(
        &self,
        hash: ObjectHash,
    ) -> Result<Option<PublicationRecord>, StewardError> {
        let outcome = self.inner.get_publication_record(hash).await;
        self.charged(outcome)
    }

    async fn get_publication_pack(
        &self,
        descriptor: PackDescriptor,
    ) -> Result<Option<Vec<u8>>, StewardError> {
        let outcome = self.inner.get_publication_pack(descriptor).await;
        self.charged(outcome)
    }

    async fn get_object(&self, hash: ObjectHash) -> Result<Option<Vec<u8>>, StewardError> {
        let outcome = self.inner.get_object(hash).await;
        self.charged(outcome)
    }

    async fn object_size(&self, hash: ObjectHash) -> Result<Option<u64>, StewardError> {
        let outcome = self.inner.object_size(hash).await;
        self.charged(outcome)
    }

    async fn get_objects(
        &self,
        hashes: &[ObjectHash],
    ) -> Result<HashMap<ObjectHash, Vec<u8>>, StewardError> {
        let outcome = self.inner.get_objects(hashes).await;
        self.charged(outcome)
    }

    async fn has_blob(&self, hash: ObjectHash) -> Result<bool, StewardError> {
        let outcome = self.inner.has_blob(hash).await;
        self.charged(outcome)
    }

    async fn list_blobs(&self) -> Result<HashSet<ObjectHash>, StewardError> {
        let outcome = self.inner.list_blobs().await;
        self.charged(outcome)
    }

    async fn get_blob_reader(&self, hash: ObjectHash) -> Result<Option<BlobReader>, StewardError> {
        let outcome = self.inner.get_blob_reader(hash).await;
        self.charged(outcome)
    }

    async fn get_series_pack(
        &self,
        series_hash: ObjectHash,
    ) -> Result<Option<PackDescriptor>, StewardError> {
        let outcome = self.inner.get_series_pack(series_hash).await;
        self.charged(outcome)
    }

    async fn get_consolidated_series_pack(
        &self,
        series_hash: ObjectHash,
    ) -> Result<Option<PackDescriptor>, StewardError> {
        let outcome = self.inner.get_consolidated_series_pack(series_hash).await;
        self.charged(outcome)
    }

    async fn list_pack_hashes(
        &self,
        series_hash: ObjectHash,
    ) -> Result<HashSet<ObjectHash>, StewardError> {
        let outcome = self.inner.list_pack_hashes(series_hash).await;
        self.charged(outcome)
    }

    async fn get_pack_index(
        &self,
        series_hash: ObjectHash,
        pack_hash: ObjectHash,
    ) -> Result<Option<Vec<u8>>, StewardError> {
        let outcome = self.inner.get_pack_index(series_hash, pack_hash).await;
        self.charged(outcome)
    }
}
