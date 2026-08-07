// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! A [`ContentSource`] whose physical storage traffic is charged to a budget.
//!
//! # Why a wrapper rather than one scope around the pull
//!
//! The meter is ambient (a task-local), so the obvious thing is to install it
//! once around the whole pull.  Two reasons not to:
//!
//! * A pull reads the local pond as well as the remote, and only the remote's
//!   traffic is billed.  Installing the meter for the whole pull relies on the
//!   local store not being wrapped; installing it only around calls to the
//!   remote source makes the boundary structural.
//! * The pull future is already large, and wrapping it adds to a value that
//!   must live on a thread's stack.  Scoping many small calls costs nothing.
//!
//! A streaming read started inside a scope keeps charging after the scope
//! ends: [`sync_store::metered_store`] captures the meter into the stream
//! rather than looking it up per chunk.  So a [`BlobReader`] handed out here
//! is still metered while it drains.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::StewardError;
use crate::content_source::{BlobReader, ContentSource};
use crate::limiter::LimiterSet;
use crate::storage_meter::MeterGuard;
use sync_store::content::ObjectHash;

/// Wraps a source so every request it makes to a remote store is charged.
pub struct MeteredSource {
    inner: Arc<dyn ContentSource>,
    guard: MeterGuard,
}

impl MeteredSource {
    /// Take `limits` for the wrapper's lifetime, leaving the caller's set
    /// empty until [`Self::finish`] returns it.
    pub fn new(inner: Arc<dyn ContentSource>, limits: &mut LimiterSet) -> Self {
        Self {
            inner,
            guard: MeterGuard::new(limits),
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
        let outcome = self.guard.scope(self.inner.get_tip(ref_name)).await;
        self.charged(outcome)
    }

    async fn get_object(&self, hash: ObjectHash) -> Result<Option<Vec<u8>>, StewardError> {
        let outcome = self.guard.scope(self.inner.get_object(hash)).await;
        self.charged(outcome)
    }

    async fn has_blob(&self, hash: ObjectHash) -> Result<bool, StewardError> {
        let outcome = self.guard.scope(self.inner.has_blob(hash)).await;
        self.charged(outcome)
    }

    async fn list_blobs(&self) -> Result<HashSet<ObjectHash>, StewardError> {
        let outcome = self.guard.scope(self.inner.list_blobs()).await;
        self.charged(outcome)
    }

    async fn get_blob_reader(&self, hash: ObjectHash) -> Result<Option<BlobReader>, StewardError> {
        let outcome = self.guard.scope(self.inner.get_blob_reader(hash)).await;
        self.charged(outcome)
    }

    async fn preload_objects(&self) -> Result<(), StewardError> {
        let outcome = self.guard.scope(self.inner.preload_objects()).await;
        self.charged(outcome)
    }

    fn clear_object_cache(&self) {
        self.inner.clear_object_cache();
    }
}
