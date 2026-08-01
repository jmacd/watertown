// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::error::ResultExt;
use crate::error::{Error, Result};
use crate::node::{FileID, Node, NodeType};
use crate::persistence::{FileVersionInfo, PersistenceLayer};
use crate::transaction_guard::TransactionState;
use crate::{EntryType, NodeMetadata};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Host filesystem persistence layer.
///
/// Maps a host directory tree onto TinyFS's `PersistenceLayer` trait.
/// The root FileID maps to the configured host root directory.
/// Child FileIDs are deterministic (computed from path via `FileID::from_content`).
///
/// A registry maps `FileID` -> host path so that `load_node` can reconstruct
/// nodes from their IDs. The registry is populated as directories are listed
/// and nodes are created.
pub struct HostmountPersistence {
    /// The host directory that maps to TinyFS root `/`
    root_path: PathBuf,
    /// FileID -> host path mapping, populated as nodes are discovered
    path_registry: Arc<Mutex<HashMap<FileID, PathBuf>>>,
    /// Transaction state (no-op for hostmount -- no real transactions)
    txn_state: Arc<TransactionState>,
}

impl HostmountPersistence {
    /// Create a new hostmount persistence rooted at `root_path`.
    ///
    /// The directory must exist and be a directory.
    pub fn new(root_path: PathBuf) -> Result<Self> {
        let canonical = root_path.canonicalize().map_other_context(format!(
            "Hostmount root '{}' cannot be resolved",
            root_path.display()
        ))?;

        if !canonical.is_dir() {
            return Err(Error::Other(format!(
                "Hostmount root '{}' is not a directory",
                canonical.display()
            )));
        }

        let mut registry = HashMap::new();
        let _ = registry.insert(FileID::root(), canonical.clone());

        Ok(Self {
            root_path: canonical,
            path_registry: Arc::new(Mutex::new(registry)),
            txn_state: Arc::new(TransactionState::new()),
        })
    }

    /// Register a FileID -> host path mapping.
    ///
    /// Called internally when nodes are constructed (by HostDirectory::get/entries)
    /// so that future `load_node` calls can reconstruct them.
    pub async fn register_path(&self, id: FileID, path: PathBuf) {
        let mut registry = self.path_registry.lock().await;
        let _ = registry.insert(id, path);
    }

    /// Look up the host path for a FileID
    pub async fn resolve_path(&self, id: FileID) -> Option<PathBuf> {
        let registry = self.path_registry.lock().await;
        registry.get(&id).cloned()
    }

    /// Get the hostmount root path
    #[must_use]
    pub fn root_path(&self) -> &Path {
        &self.root_path
    }

    /// Build a Node from a host path with the given FileID
    fn build_node_from_path(path: &Path, file_id: FileID) -> Result<Node> {
        let metadata = std::fs::metadata(path)
            .map_other_context(format!("Failed to stat '{}'", path.display()))?;

        let node_type = if metadata.is_dir() {
            NodeType::Directory(super::HostDirectory::new_handle(
                path.to_path_buf(),
                file_id,
            ))
        } else {
            NodeType::File(super::HostFile::new_handle(path.to_path_buf(), file_id))
        };

        Ok(Node::new(file_id, node_type))
    }
}

#[async_trait]
impl PersistenceLayer for HostmountPersistence {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn transaction_state(&self) -> Arc<TransactionState> {
        self.txn_state.clone()
    }

    async fn load_node(&self, id: FileID) -> Result<Node> {
        let registry = self.path_registry.lock().await;
        let path = registry.get(&id).ok_or(Error::IDNotFound(id))?;
        let path = path.clone();
        drop(registry);

        Self::build_node_from_path(&path, id)
    }

    async fn store_node(&self, node: &Node) -> Result<()> {
        // For hostmount, store_node is used by FS::create_node to persist
        // newly created nodes. We don't need to do anything here since
        // the Directory::insert already creates the file/dir on the host.
        // But we should ensure the path is registered.
        let registry = self.path_registry.lock().await;
        if !registry.contains_key(&node.id) {
            log::debug!(
                "HostmountPersistence::store_node called for unregistered node {}",
                node.id
            );
        }
        Ok(())
    }

    async fn create_file_node(&self, id: FileID) -> Result<Node> {
        // Create a HostFile node -- the actual file on disk is created
        // by Directory::insert, not here. This just returns the Node.
        let registry = self.path_registry.lock().await;
        let path = registry.get(&id).cloned();
        drop(registry);

        match path {
            Some(p) => Ok(Node::new(
                id,
                NodeType::File(super::HostFile::new_handle(p, id)),
            )),
            None => {
                // Path unknown -- the file will be placed by Directory::insert
                // which calls HostFile::set_path through the shared Handle.
                Ok(Node::new(
                    id,
                    NodeType::File(super::HostFile::new_pending_handle(id)),
                ))
            }
        }
    }

    async fn create_directory_node(&self, id: FileID) -> Result<Node> {
        let registry = self.path_registry.lock().await;
        let path = registry.get(&id).cloned();
        drop(registry);

        match path {
            Some(p) => Ok(Node::new(
                id,
                NodeType::Directory(super::HostDirectory::new_handle(p, id)),
            )),
            None => {
                // Path unknown -- the directory will be placed by Directory::insert
                // which calls HostDirectory::set_path through the shared Handle.
                Ok(Node::new(
                    id,
                    NodeType::Directory(super::HostDirectory::new_pending_handle(id)),
                ))
            }
        }
    }

    async fn create_symlink_node(&self, _id: FileID, _target: &Path) -> Result<Node> {
        Err(Error::Other(
            "Symlinks are not supported on hostmount".to_string(),
        ))
    }

    async fn create_dynamic_node(
        &self,
        _id: FileID,
        _factory_type: &str,
        _config_content: Vec<u8>,
    ) -> Result<Node> {
        Err(Error::Other(
            "Dynamic nodes are not supported on hostmount".to_string(),
        ))
    }

    async fn get_dynamic_node_config(&self, _id: FileID) -> Result<Option<(String, Vec<u8>)>> {
        // No dynamic nodes on hostmount
        Ok(None)
    }

    async fn update_dynamic_node_config(
        &self,
        _id: FileID,
        _factory_type: &str,
        _config_content: Vec<u8>,
    ) -> Result<()> {
        Err(Error::Other(
            "Dynamic nodes are not supported on hostmount".to_string(),
        ))
    }

    async fn metadata(&self, id: FileID) -> Result<NodeMetadata> {
        let registry = self.path_registry.lock().await;
        let path = registry.get(&id).ok_or(Error::IDNotFound(id))?.clone();
        drop(registry);

        let metadata = tokio::fs::metadata(&path)
            .await
            .map_other_context(format!("Failed to stat '{}'", path.display()))?;

        let timestamp = metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);

        if metadata.is_dir() {
            Ok(NodeMetadata {
                version: 1,
                size: None,
                blake3: None,
                bao_outboard: None,
                entry_type: EntryType::DirectoryPhysical,
                timestamp,
            })
        } else {
            Ok(NodeMetadata {
                version: 1,
                size: Some(metadata.len()),
                blake3: None,
                bao_outboard: None,
                entry_type: id.entry_type(),
                timestamp,
            })
        }
    }

    async fn list_file_versions(&self, id: FileID) -> Result<Vec<FileVersionInfo>> {
        // Host files have exactly one "version" -- the current file content
        let registry = self.path_registry.lock().await;
        let path = registry.get(&id).ok_or(Error::IDNotFound(id))?.clone();
        drop(registry);

        let metadata = tokio::fs::metadata(&path)
            .await
            .map_other_context(format!("Failed to stat '{}'", path.display()))?;

        if metadata.is_dir() {
            return Ok(Vec::new());
        }

        let timestamp = metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);

        Ok(vec![FileVersionInfo {
            version: 1,
            timestamp,
            size: metadata.len(),
            blake3: None,
            entry_type: id.entry_type(),
            extended_metadata: None,
        }])
    }

    async fn read_file_version(&self, id: FileID, _version: u64) -> Result<Vec<u8>> {
        // Always reads the current file content (one "version")
        let registry = self.path_registry.lock().await;
        let path = registry.get(&id).ok_or(Error::IDNotFound(id))?.clone();
        drop(registry);

        tokio::fs::read(&path)
            .await
            .map_other_context(format!("Failed to read '{}'", path.display()))
    }

    async fn set_extended_attributes(
        &self,
        _id: FileID,
        _attributes: HashMap<String, String>,
    ) -> Result<()> {
        // No-op: host files don't support extended attributes through tinyfs
        log::debug!("set_extended_attributes is a no-op on hostmount");
        Ok(())
    }
}
