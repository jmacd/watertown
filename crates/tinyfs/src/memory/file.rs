// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::EntryType;
use crate::FileID;
use crate::chained_reader::ChainedReader;
use crate::error;
use crate::error::ResultExt;
use crate::file::{AsyncReadSeek, File, Handle};
use crate::memory::MemoryPersistence;
use crate::metadata::{Metadata, NodeMetadata};
use crate::persistence::PersistenceLayer;
use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;
use tokio::sync::{Mutex, RwLock};

/// Represents a file backed by memory with integrated write protection
/// This implementation stores file content in a Vec<u8> and manages
/// its own write state to prevent concurrent writes.
///
/// MemoryFile has a reference to MemoryPersistence to support:
/// - FilePhysicalSeries: reads all versions and concatenates them
/// - Future features requiring version access
pub struct MemoryFile {
    /// File identifier
    id: FileID,
    /// Reference to persistence layer for version lookups
    persistence: MemoryPersistence,
    /// Current content (latest version written via async_writer)
    content: Arc<Mutex<Vec<u8>>>,
    /// Write state for preventing concurrent writes
    write_state: Arc<RwLock<WriteState>>,
    /// Entry type (determines read behavior)
    entry_type: EntryType,
}

#[derive(Debug, Clone, PartialEq)]
enum WriteState {
    Ready,
    Writing,
}

#[async_trait]
impl Metadata for MemoryFile {
    async fn metadata(&self) -> error::Result<NodeMetadata> {
        // Query persistence for latest metadata (includes versions and bao_outboard)
        self.persistence.metadata(self.id).await
    }
}

#[async_trait]
impl File for MemoryFile {
    async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncReadSeek>>> {
        // Check write state
        let state = self.write_state.read().await;
        if *state == WriteState::Writing {
            return Err(error::Error::Other(
                "File is currently being written".to_string(),
            ));
        }
        drop(state);

        // For FilePhysicalSeries and TablePhysicalSeries, concatenate all versions in oldest-to-newest order
        if self.entry_type == EntryType::FilePhysicalSeries
            || self.entry_type == EntryType::TablePhysicalSeries
        {
            let versions = self.persistence.list_file_versions(self.id).await?;

            if versions.is_empty() {
                // No versions yet, return empty reader
                return Ok(Box::pin(std::io::Cursor::new(Vec::new())));
            }

            // Read all versions and collect their contents
            let mut chunks = Vec::with_capacity(versions.len());
            for version_info in &versions {
                let content = self
                    .persistence
                    .read_file_version(self.id, version_info.version)
                    .await?;
                chunks.push(content);
            }

            // Concatenate all chunks for cumulative validation
            let total_content: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();

            // Get metadata - blake3 field contains cumulative bao-tree root for series types
            let metadata = self.persistence.metadata(self.id).await?;

            // Validate cumulative content using bao-tree root hash
            // The metadata.blake3 field stores the cumulative root (from SeriesOutboard.cumulative_blake3)
            if let Some(ref stored_blake3) = metadata.blake3 {
                let mut state = utilities::bao_outboard::IncrementalHashState::new();
                state.ingest(&total_content);
                let computed_root = state.root_hash();
                let computed_hex = computed_root.to_hex().to_string();

                if &computed_hex != stored_blake3 {
                    return Err(error::Error::Other(format!(
                        "Cumulative hash mismatch: expected {}, got {}",
                        stored_blake3, computed_hex
                    )));
                }
            }

            // Use ChainedReader to concatenate all versions
            Ok(Box::pin(ChainedReader::from_bytes(chunks)))
        } else {
            // Read from persistence (single version) - ensures corruption is visible
            let versions = self.persistence.list_file_versions(self.id).await?;

            if versions.is_empty() {
                // No versions yet, return empty reader
                return Ok(Box::pin(std::io::Cursor::new(Vec::new())));
            }

            // Get the latest (and only) version for FilePhysicalVersion
            let latest_version = versions
                .last()
                .ok_or_else(|| error::Error::Other("No versions found".to_string()))?;

            let content = self
                .persistence
                .read_file_version(self.id, latest_version.version)
                .await?;

            // Validate using VersionOutboard (bao-tree) and blake3 hash
            let metadata = self.persistence.metadata(self.id).await?;

            // For FilePhysicalVersion, validate content integrity
            // Small files (<= 16KB with chunk_log=4) have empty bao outboards, rely on blake3
            if let Some(bao_outboard_bytes) = &metadata.bao_outboard {
                use utilities::bao_outboard::VersionOutboard;
                let version_outboard = VersionOutboard::from_bytes(bao_outboard_bytes)
                    .map_other_context("Failed to deserialize VersionOutboard")?;

                // Only validate with bao-tree if outboard is non-empty (file > 16KB)
                if !version_outboard.outboard.is_empty() {
                    utilities::bao_outboard::verify_prefix(
                        &content,
                        &version_outboard.outboard,
                        version_outboard.size,
                    )
                    .map_other_context("Bao-tree validation failed")?;
                }
            }

            // Always verify blake3 hash (works for all file sizes)
            let computed_hash = blake3::hash(&content);
            if let Some(stored_blake3) = &metadata.blake3 {
                let computed_blake3_hex = computed_hash.to_hex().to_string();
                if &computed_blake3_hex != stored_blake3 {
                    return Err(error::Error::Other(format!(
                        "Blake3 mismatch: expected {}, got {}",
                        stored_blake3, computed_blake3_hex
                    )));
                }
            }

            Ok(Box::pin(std::io::Cursor::new(content)))
        }
    }

    async fn async_writer(&self) -> error::Result<Pin<Box<dyn crate::file::FileMetadataWriter>>> {
        // Acquire write lock
        let mut state = self.write_state.write().await;
        if *state == WriteState::Writing {
            return Err(error::Error::Other(
                "File is already being written".to_string(),
            ));
        }
        *state = WriteState::Writing;
        drop(state);

        // Allocate version number (like OpLogFile does)
        let allocated_version = self.persistence.allocate_version_for_write(self.id).await?;

        Ok(Box::pin(MemoryFileWriter::new(
            self.id,
            self.persistence.clone(),
            allocated_version,
            self.entry_type,
            self.content.clone(),
            self.write_state.clone(),
        )))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_queryable(&self) -> Option<&dyn crate::file::QueryableFile> {
        Some(self)
    }
}

#[async_trait]
impl crate::file::QueryableFile for MemoryFile {
    async fn as_table_provider(
        &self,
        id: FileID,
        context: &crate::ProviderContext,
    ) -> error::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        use datafusion::datasource::file_format::parquet::ParquetFormat;
        use datafusion::datasource::listing::ListingTable;
        use datafusion::datasource::listing::{
            ListingOptions, ListingTableConfig, ListingTableUrl,
        };

        // Use the same pattern as tlogfs: create a ListingTable with a tinyfs:// URL
        // The TinyFsObjectStore (registered in SessionContext) handles reading from MemoryPersistence

        // Build URL pattern for this file: tinyfs:///pond/{pond_id}/part/{part_id}/node/{node_id}/version/
        // This matches the TinyFsObjectStore path format expectations
        let url_pattern = format!(
            "tinyfs:///pond/{}/part/{}/node/{}/version/",
            id.pond_id(),
            id.part_id(),
            id.node_id()
        );

        let table_url =
            ListingTableUrl::parse(&url_pattern).map_other_context("Failed to parse table URL")?;

        // Create ListingTable configuration with Parquet format
        let file_format = Arc::new(ParquetFormat::default());
        let listing_options = ListingOptions::new(file_format);
        let config = ListingTableConfig::new(table_url).with_listing_options(listing_options);

        // Infer schema from the SessionContext (which will use the registered ObjectStore)
        let ctx = &context.datafusion_session;
        let config_with_schema = config
            .infer_schema(&ctx.state())
            .await
            .map_other_context("Schema inference failed")?;

        // Create ListingTable
        let listing_table = ListingTable::try_new(config_with_schema)
            .map_other_context("ListingTable creation failed")?;

        Ok(Arc::new(listing_table))
    }
}

impl MemoryFile {
    /// Create a new MemoryFile with the given id, persistence, and entry type
    #[must_use]
    pub fn new(id: FileID, persistence: MemoryPersistence, entry_type: EntryType) -> Self {
        MemoryFile {
            id,
            persistence,
            content: Arc::new(Mutex::new(Vec::new())),
            write_state: Arc::new(RwLock::new(WriteState::Ready)),
            entry_type,
        }
    }

    /// Create a new MemoryFile handle with the given id, persistence, and entry type
    #[must_use]
    pub fn new_handle(id: FileID, persistence: MemoryPersistence, entry_type: EntryType) -> Handle {
        let memory_file = Self::new(id, persistence, entry_type);
        Handle::new(Arc::new(Mutex::new(Box::new(memory_file))))
    }
}

/// Writer that resets state on drop and stores content with version in persistence
struct MemoryFileWriter {
    id: FileID,
    persistence: MemoryPersistence,
    allocated_version: u64,
    entry_type: EntryType,
    content: Arc<Mutex<Vec<u8>>>,
    write_state: Arc<RwLock<WriteState>>,
    buffer: Vec<u8>,
    completed: bool,
    completion_future: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl MemoryFileWriter {
    fn new(
        id: FileID,
        persistence: MemoryPersistence,
        allocated_version: u64,
        entry_type: EntryType,
        content: Arc<Mutex<Vec<u8>>>,
        write_state: Arc<RwLock<WriteState>>,
    ) -> Self {
        Self {
            id,
            persistence,
            allocated_version,
            entry_type,
            content,
            write_state,
            buffer: Vec::new(),
            completed: false,
            completion_future: None,
        }
    }
}

#[async_trait]
impl crate::file::FileMetadataWriter for MemoryFileWriter {
    fn set_temporal_metadata(&mut self, _min: i64, _max: i64, _timestamp_column: String) {
        // Memory files don't persist metadata - this is a no-op
        // In a real implementation, we'd store this in MemoryFile
    }

    async fn infer_temporal_bounds(&mut self) -> error::Result<(i64, i64, String)> {
        // For memory files, we don't support infer_temporal_bounds
        // This would require reading the bytes and parsing parquet
        Err(crate::Error::Other(
            "infer_temporal_bounds not supported for memory files".to_string(),
        ))
    }
}

impl Drop for MemoryFileWriter {
    fn drop(&mut self) {
        if !self.completed {
            // Reset write state on drop (panic safety)
            if let Ok(mut state) = self.write_state.try_write() {
                *state = WriteState::Ready;
            }
        }
    }
}

impl AsyncWrite for MemoryFileWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        if self.completed {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Writer already completed",
            )));
        }
        self.buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        if self.completed {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Writer already completed",
            )))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if self.completed {
            return Poll::Ready(Ok(()));
        }

        // Create completion future if not already created
        if self.completion_future.is_none() {
            let id = self.id;
            let persistence = self.persistence.clone();
            let allocated_version = self.allocated_version;
            let entry_type = self.entry_type;
            let content = self.content.clone();
            let write_state = self.write_state.clone();
            let buffer = std::mem::take(&mut self.buffer);

            let future = Box::pin(async move {
                // Compute bao_outboard if this is a series or version type
                let bao_outboard = match entry_type {
                    EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries => {
                        // Get previous version's bao_outboard (if any)
                        let prev_version = allocated_version.saturating_sub(1);

                        let prev_bao = if prev_version > 0 {
                            // Get bao_outboard from metadata
                            persistence
                                .metadata(id)
                                .await
                                .ok()
                                .and_then(|meta| meta.bao_outboard)
                        } else {
                            None
                        };

                        let series_outboard = if let Some(prev_bao_bytes) = prev_bao {
                            // Deserialize previous SeriesOutboard
                            if let Ok(prev_outboard) =
                                utilities::bao_outboard::SeriesOutboard::from_bytes(&prev_bao_bytes)
                            {
                                // Calculate pending bytes needed from previous content
                                let pending_size = (prev_outboard.cumulative_size
                                    % utilities::bao_outboard::BLOCK_SIZE as u64)
                                    as usize;

                                // Efficiently read only the pending bytes we need
                                // Read versions from newest to oldest until we have enough bytes
                                let pending_bytes = if pending_size > 0 {
                                    let versions = persistence
                                        .list_file_versions(id)
                                        .await
                                        .unwrap_or_default();

                                    // Collect bytes from tail, reading only necessary versions
                                    let mut tail_bytes = Vec::with_capacity(pending_size);

                                    // Iterate versions in reverse (newest first)
                                    for v in versions.iter().rev() {
                                        if tail_bytes.len() >= pending_size {
                                            break;
                                        }

                                        let bytes_still_needed = pending_size - tail_bytes.len();

                                        if v.size as usize >= bytes_still_needed {
                                            // This version has enough bytes - read only the tail we need
                                            let version_content = persistence
                                                .read_file_version(id, v.version)
                                                .await
                                                .unwrap_or_default();
                                            let start = version_content
                                                .len()
                                                .saturating_sub(bytes_still_needed);
                                            // Prepend to tail_bytes (since we're going backwards)
                                            let mut new_tail = version_content[start..].to_vec();
                                            new_tail.append(&mut tail_bytes);
                                            tail_bytes = new_tail;
                                        } else {
                                            // Need entire version - prepend it
                                            let version_content = persistence
                                                .read_file_version(id, v.version)
                                                .await
                                                .unwrap_or_default();
                                            let mut new_tail = version_content;
                                            new_tail.append(&mut tail_bytes);
                                            tail_bytes = new_tail;
                                        }
                                    }

                                    // Trim to exact pending size (should already be correct, but safety)
                                    if tail_bytes.len() > pending_size {
                                        tail_bytes =
                                            tail_bytes[tail_bytes.len() - pending_size..].to_vec();
                                    }

                                    tail_bytes
                                } else {
                                    Vec::new()
                                };

                                // Append new version
                                Some(
                                    utilities::bao_outboard::SeriesOutboard::append_version_inline(
                                        &prev_outboard,
                                        &pending_bytes,
                                        &buffer,
                                    ),
                                )
                            } else {
                                None
                            }
                        } else {
                            // First version
                            Some(
                                utilities::bao_outboard::SeriesOutboard::first_version_inline(
                                    &buffer,
                                ),
                            )
                        };

                        series_outboard.map(|so| so.to_bytes())
                    }
                    EntryType::FilePhysicalVersion => {
                        // Compute standalone VersionOutboard
                        let version_outboard =
                            utilities::bao_outboard::VersionOutboard::new(&buffer);
                        Some(version_outboard.to_bytes())
                    }
                    _ => None,
                };

                // Store content with version and bao_outboard in persistence
                let store_result = if let Some(bao_bytes) = bao_outboard {
                    persistence
                        .store_file_version_with_bao(
                            id,
                            allocated_version,
                            buffer.clone(),
                            bao_bytes,
                        )
                        .await
                } else {
                    persistence
                        .store_file_version(id, allocated_version, buffer.clone())
                        .await
                };

                if let Err(_e) = store_result {
                    // Failed to store version - silent failure as this is internal state
                }

                // Update content (for backward compatibility with tests that read from content directly)
                {
                    let mut content_guard = content.lock().await;
                    *content_guard = buffer;
                }

                // Reset write state
                {
                    let mut state = write_state.write().await;
                    *state = WriteState::Ready;
                }
            });
            self.completion_future = Some(future);
        }

        // Poll the completion future
        match self
            .completion_future
            .as_mut()
            .expect("infallible")
            .as_mut()
            .poll(cx)
        {
            Poll::Ready(()) => {
                self.completed = true;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
