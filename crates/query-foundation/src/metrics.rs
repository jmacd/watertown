// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Physical object-store work observed while planning and executing queries.

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// A stable snapshot of object-store work counters.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectStoreMetricsSnapshot {
    /// Metadata reads issued through `head`.
    pub metadata_calls: u64,
    /// Successful non-metadata object reads.
    pub object_gets: u64,
    /// Reads carrying an explicit byte range.
    pub range_requests: u64,
    /// Reads requesting a complete object.
    pub full_object_reads: u64,
    /// Bytes returned by successful object reads.
    pub bytes_returned: u64,
    /// Largest successful object read.
    pub max_read_bytes: u64,
    /// Recursive object listings.
    pub list_calls: u64,
    /// Objects opened for data or Parquet metadata.
    pub opened_objects: BTreeSet<String>,
}

/// Concurrent counters shared by an instrumented object store and its tests.
#[derive(Debug, Default)]
pub struct ObjectStoreMetrics {
    metadata_calls: AtomicU64,
    object_gets: AtomicU64,
    range_requests: AtomicU64,
    full_object_reads: AtomicU64,
    bytes_returned: AtomicU64,
    max_read_bytes: AtomicU64,
    list_calls: AtomicU64,
    opened_objects: Mutex<BTreeSet<String>>,
}

impl ObjectStoreMetrics {
    pub(crate) fn record_metadata_call(&self) {
        _ = self.metadata_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_list_call(&self) {
        _ = self.list_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_read(&self, object: &str, bytes: u64, ranged: bool) {
        _ = self.object_gets.fetch_add(1, Ordering::Relaxed);
        if ranged {
            _ = self.range_requests.fetch_add(1, Ordering::Relaxed);
        } else {
            _ = self.full_object_reads.fetch_add(1, Ordering::Relaxed);
        }
        _ = self.bytes_returned.fetch_add(bytes, Ordering::Relaxed);
        _ = self.max_read_bytes.fetch_max(bytes, Ordering::Relaxed);
        _ = self
            .opened_objects
            .lock()
            .expect("object-store metrics lock poisoned")
            .insert(object.to_owned());
    }

    /// Capture all counters at one instant.
    #[must_use]
    pub fn snapshot(&self) -> ObjectStoreMetricsSnapshot {
        ObjectStoreMetricsSnapshot {
            metadata_calls: self.metadata_calls.load(Ordering::Relaxed),
            object_gets: self.object_gets.load(Ordering::Relaxed),
            range_requests: self.range_requests.load(Ordering::Relaxed),
            full_object_reads: self.full_object_reads.load(Ordering::Relaxed),
            bytes_returned: self.bytes_returned.load(Ordering::Relaxed),
            max_read_bytes: self.max_read_bytes.load(Ordering::Relaxed),
            list_calls: self.list_calls.load(Ordering::Relaxed),
            opened_objects: self
                .opened_objects
                .lock()
                .expect("object-store metrics lock poisoned")
                .clone(),
        }
    }

    /// Reset all counters before a distinct measured execution.
    pub fn reset(&self) {
        self.metadata_calls.store(0, Ordering::Relaxed);
        self.object_gets.store(0, Ordering::Relaxed);
        self.range_requests.store(0, Ordering::Relaxed);
        self.full_object_reads.store(0, Ordering::Relaxed);
        self.bytes_returned.store(0, Ordering::Relaxed);
        self.max_read_bytes.store(0, Ordering::Relaxed);
        self.list_calls.store(0, Ordering::Relaxed);
        self.opened_objects
            .lock()
            .expect("object-store metrics lock poisoned")
            .clear();
    }
}

/// A stable snapshot of conservative chunk-pruning decisions.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChunkPruningMetricsSnapshot {
    /// Provider scans planned.
    pub scans: u64,
    /// Snapshot chunks considered across all scans.
    pub candidate_chunks: u64,
    /// Chunks retained for Parquet planning.
    pub retained_chunks: u64,
    /// Chunks excluded because metadata proved non-overlap.
    pub pruned_chunks: u64,
    /// Retained chunks lacking event-time statistics.
    pub missing_statistics: u64,
}

/// Concurrent counters for provider-level pruning work.
#[derive(Debug, Default)]
pub struct ChunkPruningMetrics {
    scans: AtomicU64,
    candidate_chunks: AtomicU64,
    retained_chunks: AtomicU64,
    pruned_chunks: AtomicU64,
    missing_statistics: AtomicU64,
}

impl ChunkPruningMetrics {
    pub(crate) fn record_scan(
        &self,
        candidate_chunks: u64,
        retained_chunks: u64,
        missing_statistics: u64,
    ) {
        _ = self.scans.fetch_add(1, Ordering::Relaxed);
        _ = self
            .candidate_chunks
            .fetch_add(candidate_chunks, Ordering::Relaxed);
        _ = self
            .retained_chunks
            .fetch_add(retained_chunks, Ordering::Relaxed);
        _ = self
            .pruned_chunks
            .fetch_add(candidate_chunks - retained_chunks, Ordering::Relaxed);
        _ = self
            .missing_statistics
            .fetch_add(missing_statistics, Ordering::Relaxed);
    }

    /// Capture all counters at one instant.
    #[must_use]
    pub fn snapshot(&self) -> ChunkPruningMetricsSnapshot {
        ChunkPruningMetricsSnapshot {
            scans: self.scans.load(Ordering::Relaxed),
            candidate_chunks: self.candidate_chunks.load(Ordering::Relaxed),
            retained_chunks: self.retained_chunks.load(Ordering::Relaxed),
            pruned_chunks: self.pruned_chunks.load(Ordering::Relaxed),
            missing_statistics: self.missing_statistics.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters before a distinct measured planning operation.
    pub fn reset(&self) {
        self.scans.store(0, Ordering::Relaxed);
        self.candidate_chunks.store(0, Ordering::Relaxed);
        self.retained_chunks.store(0, Ordering::Relaxed);
        self.pruned_chunks.store(0, Ordering::Relaxed);
        self.missing_statistics.store(0, Ordering::Relaxed);
    }
}
