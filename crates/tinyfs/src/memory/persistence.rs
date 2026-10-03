// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::error::{Error, Result};
use crate::memory::MemoryDirectory;
use crate::node::{FileID, Node, NodeType};
use crate::persistence::{FileVersionInfo, PersistenceLayer};
use crate::transaction_guard::TransactionState;
use crate::{EntryType, NodeMetadata};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::io::Cursor;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

/// Version information for a file in memory persistence
#[derive(Debug, Clone)]
struct MemoryFileVersion {
    version: u64,
    timestamp: i64,
    content: Vec<u8>,
    entry_type: EntryType,
    extended_metadata: Option<HashMap<String, String>>,
    /// Bao-tree outboard data for verified streaming (optional)
    bao_outboard: Option<Vec<u8>>,
    /// Blake3 hash computed at write time (for integrity verification)
    blake3: String,
}

fn memory_version_info(version: &MemoryFileVersion) -> FileVersionInfo {
    FileVersionInfo {
        version: version.version,
        timestamp: version.timestamp,
        size: version.content.len() as u64,
        blake3: Some(version.blake3.clone()),
        entry_type: version.entry_type,
        extended_metadata: version.extended_metadata.clone(),
    }
}

/// In-memory persistence layer for testing and derived file computation
/// This implements the PersistenceLayer trait using in-memory storage
#[derive(Clone)]
pub struct MemoryPersistence {
    state: Arc<Mutex<State>>,
    coherence: Arc<crate::CoherenceState>,
    /// Transaction state for enforcing single-writer pattern
    pub txn_state: Arc<TransactionState>,
    metrics: Arc<MemoryPersistenceMetricCounters>,
    #[cfg(test)]
    fail_next_store: Arc<AtomicBool>,
}

#[derive(Default)]
struct MemoryPersistenceMetricCounters {
    version_lists: AtomicU64,
    version_info_reads: AtomicU64,
    version_reads: AtomicU64,
    version_opens: AtomicU64,
    range_reads: AtomicU64,
    tail_reads: AtomicU64,
    tail_bytes: AtomicU64,
    bytes_read: AtomicU64,
}

/// Observable persistence work for MemoryPersistence contract tests.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemoryPersistenceMetrics {
    /// Version-membership list operations.
    pub version_lists: u64,
    /// Exact-version metadata point reads.
    pub version_info_reads: u64,
    /// Complete-version reads.
    pub version_reads: u64,
    /// Complete-version streaming opens.
    pub version_opens: u64,
    /// Bounded version-range reads.
    pub range_reads: u64,
    /// Bounded reads of the preceding series tail for Bao continuation.
    pub tail_reads: u64,
    /// Bytes returned by preceding-series-tail reads.
    pub tail_bytes: u64,
    /// Total bytes returned by reads and opens.
    pub bytes_read: u64,
}

pub struct State {
    // Store multiple versions of each file: (node_id, part_id) -> Vec<MemoryFileVersion>
    // Also used for dynamic nodes (FileDynamic, DirectoryDynamic, FileExecutable) - config is the content
    file_versions: HashMap<FileID, Vec<MemoryFileVersion>>,

    // Non-file nodes (directories, symlinks): (node_id, part_id) -> Node
    nodes: HashMap<FileID, Node>,
}

impl Default for State {
    fn default() -> Self {
        let root_dir = Node::new(
            FileID::root(),
            NodeType::Directory(MemoryDirectory::new_handle()),
        );
        Self {
            file_versions: HashMap::new(),
            nodes: HashMap::from([(root_dir.id, root_dir)]),
        }
    }
}

impl Default for MemoryPersistence {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            coherence: Arc::new(crate::CoherenceState::default()),
            txn_state: Arc::new(TransactionState::new()),
            metrics: Arc::new(MemoryPersistenceMetricCounters::default()),
            #[cfg(test)]
            fail_next_store: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl PersistenceLayer for MemoryPersistence {
    /// Downcast support for accessing concrete implementation methods
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// Get the transaction state for this persistence layer
    fn transaction_state(&self) -> Arc<TransactionState> {
        self.txn_state.clone()
    }

    fn coherence_state(&self) -> Option<Arc<crate::CoherenceState>> {
        Some(self.coherence.clone())
    }

    // Node operations
    async fn load_node(&self, id: FileID) -> Result<Node> {
        self.state.lock().await.load_node(id).await
    }

    async fn store_node(&self, node: &Node) -> Result<()> {
        let _mutation = self.coherence.begin_mutation()?;
        self.state.lock().await.store_node(node).await?;
        _ = self.coherence.advance()?;
        Ok(())
    }

    // Factory methods for creating nodes directly with persistence
    async fn create_file_node(&self, id: FileID) -> Result<Node> {
        // Create MemoryFile with a reference to this persistence layer
        // This enables FilePhysicalSeries version concatenation
        let file_handle = crate::memory::MemoryFile::new_handle(id, self.clone(), id.entry_type());
        Ok(Node::new(id, NodeType::File(file_handle)))
    }

    async fn create_directory_node(&self, id: FileID) -> Result<Node> {
        self.state.lock().await.create_directory_node(id).await
    }

    async fn create_symlink_node(
        &self,
        id: FileID,
        target: &std::path::Path,
        _mtime: Option<i64>,
    ) -> Result<Node> {
        let _mutation = self.coherence.begin_mutation()?;
        let node = self
            .state
            .lock()
            .await
            .create_symlink_node(id, target)
            .await?;
        _ = self.coherence.advance()?;
        Ok(node)
    }

    async fn create_dynamic_node(
        &self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
        _mtime: Option<i64>,
    ) -> Result<Node> {
        let _mutation = self.coherence.begin_mutation()?;
        let entry_type = id.entry_type();
        if !entry_type.is_dynamic() {
            return Err(Error::Other(format!(
                "create_dynamic_node called with non-dynamic entry type: {entry_type}"
            )));
        }

        self.state
            .lock()
            .await
            .store_dynamic_node_config(id, factory_type, config_content)
            .await?;
        _ = self.coherence.advance()?;

        let node_type = if entry_type.is_directory() {
            NodeType::Directory(MemoryDirectory::new_handle_with_entry_type(entry_type))
        } else {
            NodeType::File(crate::memory::MemoryFile::new_handle(
                id,
                self.clone(),
                entry_type,
            ))
        };
        Ok(Node::new(id, node_type))
    }

    async fn get_dynamic_node_config(&self, id: FileID) -> Result<Option<(String, Vec<u8>)>> {
        self.state.lock().await.get_dynamic_node_config(id).await
    }

    async fn update_dynamic_node_config(
        &self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<()> {
        let _mutation = self.coherence.begin_mutation()?;
        self.state
            .lock()
            .await
            .update_dynamic_node_config(id, factory_type, config_content)
            .await?;
        _ = self.coherence.advance()?;
        Ok(())
    }

    async fn metadata(&self, id: FileID) -> Result<NodeMetadata> {
        let node = {
            let state = self.state.lock().await;
            if let Some(latest) = state
                .file_versions
                .get(&id)
                .and_then(|versions| versions.last())
            {
                return Ok(NodeMetadata {
                    version: latest.version,
                    size: Some(latest.content.len() as u64),
                    blake3: Some(latest.blake3.clone()),
                    bao_outboard: latest.bao_outboard.clone(),
                    entry_type: latest.entry_type,
                    timestamp: latest.timestamp,
                });
            }
            state.nodes.get(&id).cloned().ok_or_else(|| {
                Error::NotFound(std::path::PathBuf::from(format!("Node {id} not found")))
            })?
        };

        match node.node_type {
            NodeType::File(_) => Ok(NodeMetadata {
                version: 0,
                size: Some(0),
                blake3: None,
                bao_outboard: None,
                entry_type: id.entry_type(),
                timestamp: 0,
            }),
            NodeType::Directory(handle) => handle.metadata().await,
            NodeType::Symlink(handle) => handle.metadata().await,
        }
    }

    async fn list_file_versions(&self, id: FileID) -> Result<Vec<FileVersionInfo>> {
        _ = self.metrics.version_lists.fetch_add(1, Ordering::Relaxed);
        self.state.lock().await.list_file_versions(id).await
    }

    async fn file_version_info(&self, id: FileID, version: u64) -> Result<Option<FileVersionInfo>> {
        _ = self
            .metrics
            .version_info_reads
            .fetch_add(1, Ordering::Relaxed);
        Ok(self.state.lock().await.file_version_info(id, version))
    }

    async fn read_file_version(&self, id: FileID, version: u64) -> Result<Vec<u8>> {
        _ = self.metrics.version_reads.fetch_add(1, Ordering::Relaxed);
        let content = self
            .state
            .lock()
            .await
            .read_file_version(id, version)
            .await?;
        _ = self
            .metrics
            .bytes_read
            .fetch_add(content.len() as u64, Ordering::Relaxed);
        Ok(content)
    }

    async fn open_file_version(
        &self,
        id: FileID,
        version: u64,
    ) -> Result<Pin<Box<dyn crate::AsyncReadSeek>>> {
        _ = self.metrics.version_opens.fetch_add(1, Ordering::Relaxed);
        let content = self
            .state
            .lock()
            .await
            .read_file_version(id, version)
            .await?;
        _ = self
            .metrics
            .bytes_read
            .fetch_add(content.len() as u64, Ordering::Relaxed);
        Ok(Box::pin(Cursor::new(content)))
    }

    async fn read_file_version_range(
        &self,
        id: FileID,
        version: u64,
        range: Range<u64>,
    ) -> Result<Bytes> {
        _ = self.metrics.range_reads.fetch_add(1, Ordering::Relaxed);
        let content = self
            .state
            .lock()
            .await
            .read_file_version_range(id, version, range)
            .await?;
        _ = self
            .metrics
            .bytes_read
            .fetch_add(content.len() as u64, Ordering::Relaxed);
        Ok(content)
    }

    async fn set_extended_attributes(
        &self,
        id: FileID,
        attributes: HashMap<String, String>,
    ) -> Result<()> {
        let _mutation = self.coherence.begin_mutation()?;
        self.state
            .lock()
            .await
            .set_extended_attributes(id, attributes)
            .await?;
        _ = self.coherence.advance()?;
        Ok(())
    }
}

impl MemoryPersistence {
    /// Reset observable read-work counters.
    pub fn reset_metrics(&self) {
        self.metrics.version_lists.store(0, Ordering::Relaxed);
        self.metrics.version_info_reads.store(0, Ordering::Relaxed);
        self.metrics.version_reads.store(0, Ordering::Relaxed);
        self.metrics.version_opens.store(0, Ordering::Relaxed);
        self.metrics.range_reads.store(0, Ordering::Relaxed);
        self.metrics.tail_reads.store(0, Ordering::Relaxed);
        self.metrics.tail_bytes.store(0, Ordering::Relaxed);
        self.metrics.bytes_read.store(0, Ordering::Relaxed);
    }

    /// Snapshot observable read work.
    #[must_use]
    pub fn metrics(&self) -> MemoryPersistenceMetrics {
        MemoryPersistenceMetrics {
            version_lists: self.metrics.version_lists.load(Ordering::Relaxed),
            version_info_reads: self.metrics.version_info_reads.load(Ordering::Relaxed),
            version_reads: self.metrics.version_reads.load(Ordering::Relaxed),
            version_opens: self.metrics.version_opens.load(Ordering::Relaxed),
            range_reads: self.metrics.range_reads.load(Ordering::Relaxed),
            tail_reads: self.metrics.tail_reads.load(Ordering::Relaxed),
            tail_bytes: self.metrics.tail_bytes.load(Ordering::Relaxed),
            bytes_read: self.metrics.bytes_read.load(Ordering::Relaxed),
        }
    }

    /// Read at most `size` trailing bytes preceding one series version.
    pub async fn read_file_tail_before(
        &self,
        id: FileID,
        version_exclusive: u64,
        size: usize,
    ) -> Result<Vec<u8>> {
        _ = self.metrics.tail_reads.fetch_add(1, Ordering::Relaxed);
        let bytes = self
            .state
            .lock()
            .await
            .read_file_tail_before(id, version_exclusive, size)?;
        _ = self
            .metrics
            .tail_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }

    #[cfg(test)]
    pub(crate) fn fail_next_store(&self) {
        self.fail_next_store.store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    fn take_store_failure(&self) -> Result<()> {
        if self.fail_next_store.swap(false, Ordering::SeqCst) {
            return Err(Error::Other(
                "injected memory persistence store failure".to_string(),
            ));
        }
        Ok(())
    }

    /// Store a file version for testing
    ///
    /// Adds a new version of a file to the in-memory storage. Versions are stored
    /// in order and can be retrieved via list_file_versions() or read_file_version().
    pub async fn store_file_version(
        &self,
        id: FileID,
        version: u64,
        content: Vec<u8>,
    ) -> Result<()> {
        #[cfg(test)]
        self.take_store_failure()?;
        let _mutation = self.coherence.begin_mutation()?;
        self.state
            .lock()
            .await
            .store_file_version(id, version, content)
            .await?;
        _ = self.coherence.advance()?;
        Ok(())
    }

    /// Store a file version with extended metadata (for testing)
    pub async fn store_file_version_with_metadata(
        &self,
        id: FileID,
        version: u64,
        content: Vec<u8>,
        entry_type: EntryType,
        extended_metadata: Option<HashMap<String, String>>,
    ) -> Result<()> {
        #[cfg(test)]
        self.take_store_failure()?;
        let _mutation = self.coherence.begin_mutation()?;
        self.state
            .lock()
            .await
            .store_file_version_with_metadata(id, version, content, entry_type, extended_metadata)
            .await?;
        _ = self.coherence.advance()?;
        Ok(())
    }

    /// Store a file version with bao_outboard data (for testing)
    pub async fn store_file_version_with_bao(
        &self,
        id: FileID,
        version: u64,
        content: Vec<u8>,
        bao_outboard: Vec<u8>,
    ) -> Result<()> {
        #[cfg(test)]
        self.take_store_failure()?;
        let _mutation = self.coherence.begin_mutation()?;
        self.state
            .lock()
            .await
            .store_file_version_with_bao(id, version, content, bao_outboard)
            .await?;
        _ = self.coherence.advance()?;
        Ok(())
    }

    /// Store a file version with both integrity data and extended metadata.
    pub async fn store_file_version_with_bao_and_metadata(
        &self,
        id: FileID,
        version: u64,
        content: Vec<u8>,
        bao_outboard: Vec<u8>,
        extended_metadata: Option<HashMap<String, String>>,
    ) -> Result<()> {
        #[cfg(test)]
        self.take_store_failure()?;
        let _mutation = self.coherence.begin_mutation()?;
        self.state
            .lock()
            .await
            .store_file_version_with_bao_and_metadata(
                id,
                version,
                content,
                bao_outboard,
                extended_metadata,
            )
            .await?;
        _ = self.coherence.advance()?;
        Ok(())
    }

    /// Allocate next version number for a file write
    /// This ensures proper version sequencing for FilePhysicalSeries
    pub async fn allocate_version_for_write(&self, id: FileID) -> Result<u64> {
        let state = self.state.lock().await;
        let next_version = if let Some(versions) = state.file_versions.get(&id) {
            versions.last().map(|v| v.version + 1).unwrap_or(1)
        } else {
            1
        };
        Ok(next_version)
    }

    /// Corrupt file content at a specific byte offset (for testing)
    ///
    /// This mutates the stored content to simulate corruption for validation testing.
    /// Returns an error if the file version doesn't exist or the offset is out of bounds.
    pub async fn corrupt_file_content(
        &self,
        id: FileID,
        version: u64,
        byte_offset: usize,
        new_value: u8,
    ) -> Result<()> {
        let mut state = self.state.lock().await;

        let versions = state
            .file_versions
            .get_mut(&id)
            .ok_or_else(|| Error::not_found(format!("File {id} not found")))?;

        let file_version = versions
            .iter_mut()
            .find(|v| v.version == version)
            .ok_or_else(|| {
                Error::not_found(format!("Version {version} not found for file {id}"))
            })?;

        if byte_offset >= file_version.content.len() {
            return Err(Error::Other(format!(
                "Byte offset {} out of bounds (content size: {})",
                byte_offset,
                file_version.content.len()
            )));
        }

        file_version.content[byte_offset] = new_value;
        Ok(())
    }
}

impl State {
    async fn load_node(&self, id: FileID) -> Result<Node> {
        match self.nodes.get(&id) {
            Some(node) => Ok(node.clone()),
            None => Err(Error::IDNotFound(id)),
        }
    }

    async fn store_node(&mut self, node: &Node) -> Result<()> {
        _ = self.nodes.insert(node.id, node.clone());
        Ok(())
    }

    async fn create_directory_node(&self, id: FileID) -> Result<Node> {
        let dir_handle = MemoryDirectory::new_handle();
        Ok(Node::new(id, NodeType::Directory(dir_handle)))
    }

    async fn create_symlink_node(&mut self, id: FileID, target: &std::path::Path) -> Result<Node> {
        let symlink_handle = crate::memory::MemorySymlink::new_handle(target.to_path_buf());
        let node = Node::new(id, NodeType::Symlink(symlink_handle.clone()));
        self.store_node(&node).await?;
        Ok(node)
    }

    async fn store_file_version(
        &mut self,
        id: FileID,
        version: u64,
        content: Vec<u8>,
    ) -> Result<()> {
        self.store_file_version_full(id, version, content, id.entry_type(), None, None)
            .await
    }

    async fn store_file_version_with_metadata(
        &mut self,
        id: FileID,
        version: u64,
        content: Vec<u8>,
        entry_type: EntryType,
        extended_metadata: Option<HashMap<String, String>>,
    ) -> Result<()> {
        self.store_file_version_full(id, version, content, entry_type, extended_metadata, None)
            .await
    }

    async fn store_file_version_with_bao(
        &mut self,
        id: FileID,
        version: u64,
        content: Vec<u8>,
        bao_outboard: Vec<u8>,
    ) -> Result<()> {
        self.store_file_version_with_bao_and_metadata(id, version, content, bao_outboard, None)
            .await
    }

    async fn store_file_version_with_bao_and_metadata(
        &mut self,
        id: FileID,
        version: u64,
        content: Vec<u8>,
        bao_outboard: Vec<u8>,
        extended_metadata: Option<HashMap<String, String>>,
    ) -> Result<()> {
        // For series types, extract cumulative_blake3 from SeriesOutboard
        // For version types, extract root hash from VersionOutboard
        // This avoids hashing the content twice
        let blake3_from_outboard =
            Self::extract_blake3_from_outboard(&bao_outboard, id.entry_type());

        self.store_file_version_full_with_blake3(
            id,
            version,
            content,
            id.entry_type(),
            extended_metadata,
            Some(bao_outboard),
            blake3_from_outboard,
        )
        .await
    }

    /// Extract blake3 hash from bao_outboard based on entry type
    fn extract_blake3_from_outboard(bao_outboard: &[u8], entry_type: EntryType) -> Option<String> {
        match entry_type {
            EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries => {
                // For series types, use cumulative_blake3 from SeriesOutboard
                utilities::bao_outboard::SeriesOutboard::from_bytes(bao_outboard)
                    .ok()
                    .map(|so| {
                        blake3::Hash::from(so.cumulative_blake3)
                            .to_hex()
                            .to_string()
                    })
            }
            EntryType::FilePhysicalVersion => {
                // For version types, compute root from outboard
                utilities::bao_outboard::VersionOutboard::from_bytes(bao_outboard)
                    .ok()
                    .and_then(|vo| {
                        // The VersionOutboard stores the outboard, we need to compute root
                        // For now, fall back to None (will compute from content)
                        // TODO: Store root hash in VersionOutboard
                        let _ = vo;
                        None
                    })
            }
            _ => None,
        }
    }

    async fn store_file_version_full(
        &mut self,
        id: FileID,
        version: u64,
        content: Vec<u8>,
        entry_type: EntryType,
        extended_metadata: Option<HashMap<String, String>>,
        bao_outboard: Option<Vec<u8>>,
    ) -> Result<()> {
        self.store_file_version_full_with_blake3(
            id,
            version,
            content,
            entry_type,
            extended_metadata,
            bao_outboard,
            None, // Compute from content
        )
        .await
    }

    async fn store_file_version_full_with_blake3(
        &mut self,
        id: FileID,
        version: u64,
        content: Vec<u8>,
        entry_type: EntryType,
        extended_metadata: Option<HashMap<String, String>>,
        bao_outboard: Option<Vec<u8>>,
        precomputed_blake3: Option<String>,
    ) -> Result<()> {
        // Use precomputed blake3 if provided (for series types from SeriesOutboard)
        // Otherwise compute fresh from content (for version types)
        let blake3 =
            precomputed_blake3.unwrap_or_else(|| blake3::hash(&content).to_hex().to_string());

        let file_version = MemoryFileVersion {
            version,
            timestamp: chrono::Utc::now().timestamp_micros(),
            content,
            entry_type,
            extended_metadata,
            bao_outboard,
            blake3,
        };

        self.file_versions.entry(id).or_default().push(file_version);

        Ok(())
    }

    async fn list_file_versions(&self, id: FileID) -> Result<Vec<FileVersionInfo>> {
        if let Some(versions) = self.file_versions.get(&id) {
            let version_infos = versions.iter().map(memory_version_info).collect();
            Ok(version_infos)
        } else {
            Ok(Vec::new())
        }
    }

    fn file_version_info(&self, id: FileID, version: u64) -> Option<FileVersionInfo> {
        let versions = self.file_versions.get(&id)?;
        let candidate = usize::try_from(version.checked_sub(1)?)
            .ok()
            .and_then(|index| versions.get(index))
            .filter(|candidate| candidate.version == version)
            .or_else(|| {
                versions
                    .binary_search_by_key(&version, |candidate| candidate.version)
                    .ok()
                    .and_then(|index| versions.get(index))
            })?;
        Some(memory_version_info(candidate))
    }

    fn read_file_tail_before(
        &self,
        id: FileID,
        version_exclusive: u64,
        size: usize,
    ) -> Result<Vec<u8>> {
        let versions = self.file_versions.get(&id).ok_or_else(|| {
            Error::NotFound(std::path::PathBuf::from(format!("File {id} not found")))
        })?;
        let mut chunks = Vec::new();
        let mut remaining = size;
        for version in versions
            .iter()
            .rev()
            .filter(|version| version.version < version_exclusive)
        {
            if remaining == 0 {
                break;
            }
            let start = version.content.len().saturating_sub(remaining);
            remaining = remaining.saturating_sub(version.content.len() - start);
            chunks.push(&version.content[start..]);
        }
        let mut result = Vec::with_capacity(size - remaining);
        for chunk in chunks.into_iter().rev() {
            result.extend_from_slice(chunk);
        }
        Ok(result)
    }

    async fn read_file_version(&self, id: FileID, version: u64) -> Result<Vec<u8>> {
        if let Some(versions) = self.file_versions.get(&id) {
            if let Some(file_version) = versions.iter().find(|fv| fv.version == version) {
                Ok(file_version.content.clone())
            } else {
                Err(Error::NotFound(std::path::PathBuf::from(format!(
                    "Version {version} of file {id} not found"
                ))))
            }
            // None => {
            //     if let Some(latest) = versions.last() {
            //         Ok(latest.content.clone())
            //     } else {
            //         Err(Error::NotFound(std::path::PathBuf::from(format!(
            //             "No versions of file {id} found",
            //         ))))
            //     }
            // }
        } else {
            Err(Error::NotFound(std::path::PathBuf::from(format!(
                "File {id} not found",
            ))))
        }
    }

    async fn read_file_version_range(
        &self,
        id: FileID,
        version: u64,
        range: Range<u64>,
    ) -> Result<Bytes> {
        let versions = self.file_versions.get(&id).ok_or_else(|| {
            Error::NotFound(std::path::PathBuf::from(format!(
                "No versions found for file {id}"
            )))
        })?;
        let file_version = versions
            .iter()
            .find(|file_version| file_version.version == version)
            .ok_or_else(|| {
                Error::NotFound(std::path::PathBuf::from(format!(
                    "Version {version} of file {id} not found"
                )))
            })?;
        let range = crate::persistence::validate_file_version_range(
            range,
            file_version.content.len() as u64,
        )?;
        Ok(Bytes::copy_from_slice(&file_version.content[range]))
    }

    async fn set_extended_attributes(
        &mut self,
        id: FileID,
        attributes: HashMap<String, String>,
    ) -> Result<()> {
        if let Some(versions) = self.file_versions.get_mut(&id) {
            if let Some(latest_version) = versions.last_mut() {
                latest_version.extended_metadata = Some(attributes);
                Ok(())
            } else {
                Err(Error::NotFound(std::path::PathBuf::from(format!(
                    "No versions of file {id} found",
                ))))
            }
        } else {
            Err(Error::NotFound(std::path::PathBuf::from(format!(
                "File {id} not found",
            ))))
        }
    }

    /// Store config for a dynamic node (called by MemoryPersistence::create_dynamic_node)
    async fn store_dynamic_node_config(
        &mut self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<()> {
        // Dynamic nodes are stored like files with config as content
        // Factory type goes in extended_metadata["factory"]
        let mut extended_metadata = HashMap::new();
        _ = extended_metadata.insert("factory".to_string(), factory_type.to_string());

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("positive")
            .as_micros() as i64;

        // Compute blake3 at write time for integrity verification
        let blake3_hash = blake3::hash(&config_content);
        let blake3 = blake3_hash.to_hex().to_string();

        let version = MemoryFileVersion {
            version: 1,
            timestamp,
            content: config_content,
            entry_type: id.entry_type(),
            extended_metadata: Some(extended_metadata),
            bao_outboard: None,
            blake3,
        };

        self.file_versions.entry(id).or_default().push(version);
        Ok(())
    }

    async fn get_dynamic_node_config(&self, id: FileID) -> Result<Option<(String, Vec<u8>)>> {
        if let Some(versions) = self.file_versions.get(&id)
            && let Some(latest) = versions.last()
            && let Some(ref metadata) = latest.extended_metadata
            && let Some(factory_type) = metadata.get("factory")
        {
            return Ok(Some((factory_type.clone(), latest.content.clone())));
        }
        Ok(None)
    }

    async fn update_dynamic_node_config(
        &mut self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<()> {
        let mut extended_metadata = HashMap::new();
        _ = extended_metadata.insert("factory".to_string(), factory_type.to_string());

        let new_version = if let Some(versions) = self.file_versions.get(&id) {
            let next_version = versions.last().map(|v| v.version + 1).unwrap_or(1);
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time is after UNIX_EPOCH")
                .as_micros() as i64;

            // Compute blake3 at write time for integrity verification
            let blake3_hash = blake3::hash(&config_content);
            let blake3 = blake3_hash.to_hex().to_string();

            MemoryFileVersion {
                version: next_version,
                timestamp,
                content: config_content,
                entry_type: id.entry_type(),
                extended_metadata: Some(extended_metadata),
                bao_outboard: None,
                blake3,
            }
        } else {
            return Err(Error::Other(format!("Dynamic node not found: {}", id)));
        };

        self.file_versions
            .get_mut(&id)
            .ok_or_else(|| Error::Other(format!("Dynamic node not found: {}", id)))?
            .push(new_version);

        Ok(())
    }
}
