// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::EntryType;
use crate::error::Result;
use crate::node::{FileID, Node};
use crate::transaction_guard::TransactionState;
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
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

/// Pure persistence layer - no caching, no NodeRef management
#[async_trait]
pub trait PersistenceLayer: Send + Sync {
    /// Downcast support for accessing concrete implementation methods
    fn as_any(&self) -> &dyn std::any::Any;

    /// Get the transaction state for this persistence layer
    ///
    /// This allows transaction guards to be created from the persistence layer
    fn transaction_state(&self) -> Arc<TransactionState>;

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

    /// Read content of a specific version of a file
    /// If version is None, reads the latest version
    async fn read_file_version(&self, id: FileID, version: u64) -> Result<Vec<u8>>;

    /// Set extended attributes on an existing node
    /// This should modify the pending version of the node in the current transaction
    async fn set_extended_attributes(
        &self,
        id: FileID,
        attributes: HashMap<String, String>,
    ) -> Result<()>;
}
