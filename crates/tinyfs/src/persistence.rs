// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::EntryType;
use crate::error::Result;
use crate::node::{FileID, Node};
use crate::transaction_guard::TransactionState;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::ops::Range;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

/// Information about a specific version of a file
#[derive(Debug, Clone)]
pub struct FileVersionInfo {
    /// Version number (monotonically increasing)
    pub version: u64,
    /// Timestamp when this version was created (Unix microseconds)
    pub timestamp: i64,
    /// Size of the file content in bytes
    pub size: u64,
    /// BLAKE3 hash of the content (for integrity checking)
    pub blake3: Option<String>,
    /// Entry type for this version
    pub entry_type: EntryType,
    /// Extended metadata for this version
    pub extended_metadata: Option<HashMap<String, String>>,
}

/// One request's resolved immutable file version.
///
/// Resolution may load inline bytes, but external content remains unopened
/// until a consuming body operation. Callers should keep the handle
/// request-scoped. Transactional backends must reject new open/range operations
/// after their transaction closes.
#[async_trait]
pub trait ResolvedFileVersion: Send + Sync {
    fn info(&self) -> &FileVersionInfo;

    async fn open(self: Box<Self>) -> Result<Pin<Box<dyn crate::AsyncReadSeek>>>;

    async fn read_range(self: Box<Self>, range: Range<u64>) -> Result<Bytes>;
}

struct DeferredFileVersion<'a, P: PersistenceLayer + ?Sized> {
    persistence: &'a P,
    id: FileID,
    info: FileVersionInfo,
}

#[async_trait]
impl<P: PersistenceLayer + ?Sized> ResolvedFileVersion for DeferredFileVersion<'_, P> {
    fn info(&self) -> &FileVersionInfo {
        &self.info
    }

    async fn open(self: Box<Self>) -> Result<Pin<Box<dyn crate::AsyncReadSeek>>> {
        if let Some(coherence) = self.persistence.coherence_state() {
            coherence.ensure_open()?;
        }
        self.persistence
            .open_file_version(self.id, self.info.version)
            .await
    }

    async fn read_range(self: Box<Self>, range: Range<u64>) -> Result<Bytes> {
        if let Some(coherence) = self.persistence.coherence_state() {
            coherence.ensure_open()?;
        }
        self.persistence
            .read_file_version_range(self.id, self.info.version, range)
            .await
    }
}

/// Validate and clamp a logical file-version range.
///
/// The end offset is clamped to the content size. An empty range is accepted
/// only at or before EOF; a start beyond EOF or a reversed range is rejected.
pub fn validate_file_version_range(range: Range<u64>, size: u64) -> Result<Range<usize>> {
    if range.start > range.end
        || range.start > size
        || (range.start == size && range.end > range.start)
    {
        return Err(crate::Error::invalid_range(range.start, range.end, size));
    }

    let end = range.end.min(size);
    let start = usize::try_from(range.start)
        .map_err(|_| crate::Error::invalid_range(range.start, range.end, size))?;
    let end = usize::try_from(end)
        .map_err(|_| crate::Error::invalid_range(range.start, range.end, size))?;
    Ok(start..end)
}

/// Pure persistence layer - no caching, no NodeRef management
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait PersistenceLayer: Send + Sync {
    /// Downcast support for accessing concrete implementation methods
    fn as_any(&self) -> &dyn std::any::Any;

    /// Get the transaction state for this persistence layer
    ///
    /// This allows transaction guards to be created from the persistence layer
    fn transaction_state(&self) -> Arc<TransactionState>;

    /// Shared lifecycle and mutation generation for transaction-scoped
    /// persistence. Backends without transactional cache coherence may return
    /// `None`.
    fn coherence_state(&self) -> Option<Arc<crate::CoherenceState>> {
        None
    }

    /// Get the pond UUID for this persistence layer.
    /// Returns the real pond UUID for OpLog-backed ponds, or a well-known
    /// placeholder for memory/hostmount persistence.
    fn pond_uuid(&self) -> uuid7::Uuid {
        crate::local_pond_uuid()
    }

    async fn load_node(&self, file_id: FileID) -> Result<Node>;

    async fn store_node(&self, node: &Node) -> Result<()>;

    async fn create_file_node(&self, file_id: FileID) -> Result<Node>;

    async fn create_directory_node(&self, id: FileID) -> Result<Node>;

    /// Initialize an empty root directory for a *foreign* pond_id, so a
    /// cross-pond import can rebuild its tree before any rows exist.  The
    /// default is a no-op for persistence layers that have no notion of
    /// foreign ponds (memory, hostmount).
    async fn initialize_foreign_root(&self, _pond_id: uuid7::Uuid) -> Result<()> {
        Ok(())
    }

    /// Create a symlink node pointing at `target`.
    ///
    /// `mtime` sets the node's timestamp explicitly, in microseconds since the
    /// Unix epoch; `None` means now.  Replication passes the source's value so
    /// a mirrored node keeps the time it was originally written rather than
    /// claiming to have been modified at pull time.
    async fn create_symlink_node(
        &self,
        id: FileID,
        target: &Path,
        mtime: Option<i64>,
    ) -> Result<Node>;

    /// Create a dynamic node from its factory type and config.
    ///
    /// `mtime` behaves as in [`create_symlink_node`].
    ///
    /// [`create_symlink_node`]: PersistenceLayer::create_symlink_node
    async fn create_dynamic_node(
        &self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
        mtime: Option<i64>,
    ) -> Result<Node>;

    async fn get_dynamic_node_config(&self, id: FileID) -> Result<Option<(String, Vec<u8>)>>; // (factory_type, config)

    async fn update_dynamic_node_config(
        &self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<()>;

    /// Get consolidated metadata for a node
    /// Requires both node_id and part_id for efficient querying
    async fn metadata(&self, id: FileID) -> Result<crate::NodeMetadata>;

    /// List all versions of a file, returning metadata for each version
    /// Returns versions in chronological order (oldest to newest)
    async fn list_file_versions(&self, id: FileID) -> Result<Vec<FileVersionInfo>>;

    /// List the versions of several files, in the order of `ids`.
    ///
    /// Equivalent to calling [`Self::list_file_versions`] for each id.
    /// Backends whose per-call cost is a query should override this to share
    /// one query across files of the same partition.
    async fn list_files_versions(&self, ids: &[FileID]) -> Result<Vec<Vec<FileVersionInfo>>> {
        let mut listed = Vec::with_capacity(ids.len());
        for id in ids {
            listed.push(self.list_file_versions(*id).await?);
        }
        Ok(listed)
    }

    /// Stable identity of the complete readable persistence snapshot.
    ///
    /// Equal identities must mean every node, version, and directory entry
    /// visible through this layer is unchanged. Backends that cannot provide
    /// that guarantee return `None`; callers must then inspect their inputs
    /// conservatively.
    async fn snapshot_identity(&self) -> Result<Option<String>> {
        Ok(None)
    }

    /// Point-read metadata for one exact immutable file version.
    ///
    /// Backends should override this when they can avoid enumerating retained
    /// history. The default preserves compatibility but is not suitable for
    /// history-independent query execution.
    async fn file_version_info(&self, id: FileID, version: u64) -> Result<Option<FileVersionInfo>> {
        Ok(self
            .list_file_versions(id)
            .await?
            .into_iter()
            .find(|info| info.version == version))
    }

    /// Resolve metadata and body access for one exact version.
    ///
    /// The default defers body access to the existing read methods. Backends
    /// may retain a resolved record for this request to avoid looking it up
    /// again. Resolution may read inline content; metadata-only callers should
    /// use [`Self::file_version_info`] instead.
    async fn resolve_file_version(
        &self,
        id: FileID,
        version: u64,
    ) -> Result<Option<Box<dyn ResolvedFileVersion + '_>>> {
        if let Some(coherence) = self.coherence_state() {
            coherence.ensure_open()?;
        }
        let Some(info) = self.file_version_info(id, version).await? else {
            return Ok(None);
        };
        Ok(Some(Box::new(DeferredFileVersion {
            persistence: self,
            id,
            info,
        })))
    }

    /// Read content of a specific version of a file
    /// If version is None, reads the latest version
    async fn read_file_version(&self, id: FileID, version: u64) -> Result<Vec<u8>>;

    /// Open a streaming, seekable reader for a specific logical file version.
    async fn open_file_version(
        &self,
        id: FileID,
        version: u64,
    ) -> Result<Pin<Box<dyn crate::AsyncReadSeek>>>;

    /// Read a byte range from a specific logical file version.
    ///
    /// Implementations must avoid materializing the complete version when the
    /// backend supports random access.
    async fn read_file_version_range(
        &self,
        id: FileID,
        version: u64,
        range: Range<u64>,
    ) -> Result<Bytes>;

    /// Set extended attributes on an existing node
    /// This should modify the pending version of the node in the current transaction
    async fn set_extended_attributes(
        &self,
        id: FileID,
        attributes: HashMap<String, String>,
    ) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::{PersistenceLayer, validate_file_version_range};
    use crate::Error;

    #[test]
    fn version_ranges_are_clamped_and_validated() {
        assert_eq!(validate_file_version_range(2..8, 10).unwrap(), 2..8);
        assert_eq!(validate_file_version_range(8..20, 10).unwrap(), 8..10);
        assert_eq!(validate_file_version_range(10..10, 10).unwrap(), 10..10);
        assert_eq!(
            validate_file_version_range(10..11, 10),
            Err(Error::invalid_range(10, 11, 10))
        );
        assert_eq!(
            validate_file_version_range(std::ops::Range { start: 8, end: 7 }, 10,),
            Err(Error::invalid_range(8, 7, 10))
        );
    }

    #[tokio::test]
    async fn resolved_versions_defer_body_and_reject_closed_context() {
        let persistence = crate::MemoryPersistence::default();
        let id = crate::FileID::new_in_partition(
            crate::FileID::root().part_id(),
            crate::EntryType::FilePhysicalVersion,
            crate::local_pond_uuid(),
        );
        persistence
            .store_file_version(id, 1, b"0123456789".to_vec())
            .await
            .expect("store");
        persistence.reset_metrics();
        let resolved = persistence
            .resolve_file_version(id, 1)
            .await
            .expect("resolve")
            .unwrap();
        assert_eq!(resolved.info().size, 10);
        assert_eq!(
            persistence.metrics().bytes_read,
            0,
            "default resolution must defer content"
        );
        assert_eq!(
            resolved.read_range(2..5).await.expect("range").as_ref(),
            b"234"
        );
        assert_eq!(persistence.metrics().range_reads, 1);
        assert_eq!(persistence.metrics().bytes_read, 3);

        let ranged = persistence
            .resolve_file_version(id, 1)
            .await
            .expect("range handle")
            .unwrap();
        let streamed = persistence
            .resolve_file_version(id, 1)
            .await
            .expect("stream handle")
            .unwrap();
        persistence.coherence_state().expect("coherence").close();
        assert!(
            ranged
                .read_range(0..1)
                .await
                .expect_err("closed range")
                .to_string()
                .contains("closed")
        );
        assert!(
            streamed
                .open()
                .await
                .err()
                .expect("closed stream")
                .to_string()
                .contains("closed")
        );
        assert!(
            persistence
                .resolve_file_version(id, 1)
                .await
                .err()
                .expect("closed resolution")
                .to_string()
                .contains("closed")
        );
        assert_eq!(
            persistence.metrics().bytes_read,
            3,
            "closed handles must not read content"
        );
    }
}
