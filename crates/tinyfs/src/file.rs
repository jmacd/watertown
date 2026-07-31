// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use super::error;
use super::metadata::Metadata;
use crate::error::ResultExt;
use async_trait::async_trait;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};

/// Trait that combines AsyncRead and AsyncSeek for random access file operations
pub trait AsyncReadSeek: AsyncRead + AsyncSeek + Send + Unpin {}

/// Blanket implementation for types that implement both AsyncRead and AsyncSeek
impl<T: AsyncRead + AsyncSeek + Send + Unpin> AsyncReadSeek for T {}

/// Optional bounds applied when reading an append-only *series* file, used to
/// keep per-read memory bounded regardless of retained history. Both bounds are
/// combined with AND; a `None` field imposes no constraint. Non-series backends
/// (and files without per-version metadata) ignore these bounds. The all-`None`
/// value ([`SeriesReadBounds::NONE`]) reads the full series, unchanged.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct SeriesReadBounds {
    /// Retain only versions whose recorded `max_event_time` is at or above this
    /// value (epoch microseconds). Versions lacking a recorded bound are always
    /// retained (a missing bound must never drop data).
    pub event_time_lo: Option<i64>,
    /// Retain only versions whose version number is strictly greater than this
    /// watermark. Used to fold only journal versions not yet seen by a durable
    /// summary.
    pub version_gt: Option<i64>,
}

impl SeriesReadBounds {
    /// No bounds: read the full series (equivalent to the unbounded reader).
    pub const NONE: Self = Self {
        event_time_lo: None,
        version_gt: None,
    };

    /// Bound by an event-time lower bound only (epoch microseconds).
    #[must_use]
    pub fn from_event_time_lo(event_time_lo: i64) -> Self {
        Self {
            event_time_lo: Some(event_time_lo),
            version_gt: None,
        }
    }

    /// Bound by an exclusive version watermark only.
    #[must_use]
    pub fn from_version_gt(version_gt: i64) -> Self {
        Self {
            event_time_lo: None,
            version_gt: Some(version_gt),
        }
    }

    /// Decide whether a series version is retained by these bounds. Both bounds
    /// combine with AND (a `None` field imposes no constraint):
    ///
    /// - `event_time_lo`: retain iff the version's recorded `max_event_time` is
    ///   at or above the bound. A version with no recorded bound (`None`) is
    ///   **always retained** — a missing bound must never silently drop data.
    /// - `version_gt`: retain iff the version number is strictly greater than
    ///   the watermark.
    #[must_use]
    pub fn retains(&self, max_event_time: Option<i64>, version: i64) -> bool {
        let event_time_ok = match self.event_time_lo {
            Some(lo) => max_event_time.is_none_or(|max_ts| max_ts >= lo),
            None => true,
        };
        let version_ok = match self.version_gt {
            Some(watermark) => version > watermark,
            None => true,
        };
        event_time_ok && version_ok
    }
}

/// Trait for writers that can accept file metadata (e.g., temporal bounds from parquet)
/// This allows metadata extracted during serialization to be passed to the storage layer
/// without re-reading the file
#[async_trait]
pub trait FileMetadataWriter: AsyncWrite + Send + Unpin {
    /// Set temporal metadata for series files (used when metadata is known at write time)
    fn set_temporal_metadata(&mut self, min: i64, max: i64, timestamp_column: String);

    /// Infer temporal bounds from the written parquet file by reading only the footer.
    /// After calling this, further writes will fail. Calls shutdown() internally.
    /// Returns (min_timestamp, max_timestamp, timestamp_column_name)
    async fn infer_temporal_bounds(&mut self) -> error::Result<(i64, i64, String)>;
}

/// Simple handle wrapper - no external state management
#[derive(Clone)]
pub struct Handle(Arc<tokio::sync::Mutex<Box<dyn File>>>);

/// Represents a file with binary content.
/// This design uses streaming I/O as the fundamental operations.
/// Implementations handle their own state management and write protection.
#[async_trait]
pub trait File: Metadata + Send + Sync {
    /// Create a reader stream - implementation specific
    async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncReadSeek>>>;

    /// Create a reader stream that, for append-only series files, may prune
    /// versions per the supplied [`SeriesReadBounds`] (event-time lower bound
    /// and/or version watermark). Backends without per-version metadata (and
    /// non-series files) ignore the bounds. The default delegates to
    /// [`File::async_reader`], reading the full content.
    async fn async_reader_bounded(
        &self,
        _bounds: SeriesReadBounds,
    ) -> error::Result<Pin<Box<dyn AsyncReadSeek>>> {
        self.async_reader().await
    }

    /// Create a writer stream - implementation specific
    /// Returns a writer that can optionally accept metadata via FileMetadataWriter trait
    async fn async_writer(&self) -> error::Result<Pin<Box<dyn FileMetadataWriter>>>;

    /// Create a writer for a series version that *replaces* all earlier versions
    /// of this node -- replicating a source-side compaction during content pull.
    ///
    /// Unlike [`File::async_writer`], the returned writer starts a fresh
    /// content/bao baseline (it does not resume the previous version's frontier)
    /// and stamps the persisted row with a `collapsed_through` sentinel so the
    /// read path and content fold both drop the superseded predecessors. The
    /// default delegates to [`File::async_writer`] for backends that have no
    /// version sentinels.
    async fn async_writer_collapsing(&self) -> error::Result<Pin<Box<dyn FileMetadataWriter>>> {
        self.async_writer().await
    }

    /// Allow downcasting to concrete file types
    fn as_any(&self) -> &dyn std::any::Any;

    /// Try to upcast this File to QueryableFile if it supports SQL queries
    ///
    /// Default implementation returns None. Files that implement QueryableFile
    /// should override this to return Some(self).
    fn as_queryable(&self) -> Option<&dyn QueryableFile> {
        None
    }
}

/// Trait for files that can be queried via DataFusion SQL
#[async_trait]
pub trait QueryableFile: File {
    /// Convert this file into a DataFusion TableProvider for SQL queries
    ///
    /// The id parameter identifies the file, and context provides access to
    /// the persistence layer and other resources needed for query execution.
    async fn as_table_provider(
        &self,
        id: crate::FileID,
        context: &crate::ProviderContext,
    ) -> error::Result<Arc<dyn datafusion::catalog::TableProvider>>;

    /// As [`QueryableFile::as_table_provider`], but for append-only series the
    /// supplied [`SeriesReadBounds`] prune which versions are scanned, so an
    /// incremental reader does not pay for the whole retained history.
    ///
    /// The bounds are a conservative superset filter, never a correctness
    /// filter, so ignoring them is always safe. The default does exactly that,
    /// which keeps every non-series and metadata-less backend correct.
    async fn as_table_provider_bounded(
        &self,
        id: crate::FileID,
        context: &crate::ProviderContext,
        _bounds: SeriesReadBounds,
    ) -> error::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        self.as_table_provider(id, context).await
    }
}

impl Handle {
    pub fn new(file: Arc<tokio::sync::Mutex<Box<dyn File>>>) -> Self {
        Self(file)
    }

    /// Get an async reader - delegated to implementation
    pub async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncReadSeek>>> {
        let file = self.0.lock().await;
        file.async_reader().await
    }

    /// Get an async reader that may prune series versions per `bounds`
    /// - delegated to implementation (see [`File::async_reader_bounded`]).
    pub async fn async_reader_bounded(
        &self,
        bounds: SeriesReadBounds,
    ) -> error::Result<Pin<Box<dyn AsyncReadSeek>>> {
        let file = self.0.lock().await;
        file.async_reader_bounded(bounds).await
    }

    /// Get an async writer - delegated to implementation  
    pub async fn async_writer(&self) -> error::Result<Pin<Box<dyn FileMetadataWriter>>> {
        let file = self.0.lock().await;
        file.async_writer().await
    }

    /// Get an async writer that replaces all earlier versions of a series node
    /// with a fresh baseline (see [`File::async_writer_collapsing`]).
    pub async fn async_writer_collapsing(&self) -> error::Result<Pin<Box<dyn FileMetadataWriter>>> {
        let file = self.0.lock().await;
        file.async_writer_collapsing().await
    }

    /// Get metadata through the file handle
    pub async fn metadata(&self) -> error::Result<crate::NodeMetadata> {
        let file = self.0.lock().await;
        file.metadata().await
    }

    /// Internal method: Write file content directly (used by buffer helpers)
    pub async fn write_file(&self, content: &[u8]) -> error::Result<()> {
        let mut writer = self.async_writer().await?;
        use tokio::io::AsyncWriteExt;
        writer
            .write_all(content)
            .await
            .map_other_context("Failed to write file content")?;
        writer
            .shutdown()
            .await
            .map_other_context("Failed to complete file write")
    }

    /// Access the underlying file for downcasting (clones the Arc)
    pub async fn get_file(&self) -> Arc<tokio::sync::Mutex<Box<dyn File>>> {
        self.0.clone()
    }
}
