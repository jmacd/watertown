// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Overlay persistence -- wraps HostmountPersistence with factory-mounted nodes
//!
//! `OverlayPersistence` implements `PersistenceLayer` by delegating to an inner
//! `HostmountPersistence` while intercepting `load_node` at the root to return
//! an `OverlayDirectory` that merges host files with factory-mounted nodes.
//!
//! Mount specs are processed at construction time: config files are read from
//! the host, factory nodes are created via `FactoryRegistry`, and overlay
//! entries are stored for injection into the root directory.

use crate::NodeMetadata;
use crate::error::Result;
use crate::node::{FileID, Node, NodeType};
use crate::persistence::{FileVersionInfo, PersistenceLayer};
use crate::transaction_guard::TransactionState;
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use super::overlay::OverlayDirectory;
use super::{HostDirectory, HostmountPersistence};

/// Persistence layer that overlays factory-mounted nodes on a hostmount.
///
/// Wraps `HostmountPersistence` and injects overlay entries at mount points.
/// The root directory is replaced with an `OverlayDirectory` that merges
/// host entries and factory nodes.
pub struct OverlayPersistence {
    /// The underlying host filesystem persistence
    inner: HostmountPersistence,
    /// Factory-mounted nodes at root level: name -> Node
    root_overlays: HashMap<String, Node>,
}

impl OverlayPersistence {
    /// Create a new overlay persistence wrapping the given hostmount persistence.
    ///
    /// `root_overlays` contains factory-created nodes to mount at the root level.
    /// For now, only depth-1 mounts are supported (e.g., `/reduced`).
    /// Deeper mounts (e.g., `/data/live`) will require intermediate overlay
    /// directories and will be added in a future phase.
    #[must_use]
    pub fn new(inner: HostmountPersistence, root_overlays: HashMap<String, Node>) -> Self {
        Self {
            inner,
            root_overlays,
        }
    }

    /// Get the hostmount root path.
    #[must_use]
    pub fn root_path(&self) -> &Path {
        self.inner.root_path()
    }
}

#[async_trait]
impl PersistenceLayer for OverlayPersistence {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn transaction_state(&self) -> Arc<TransactionState> {
        self.inner.transaction_state()
    }

    async fn load_node(&self, id: FileID) -> Result<Node> {
        if id == FileID::root() && !self.root_overlays.is_empty() {
            // Return an OverlayDirectory for the root
            let root_path = self.inner.root_path().to_path_buf();
            let host_dir = HostDirectory::new(root_path, FileID::root());
            let overlay_dir = OverlayDirectory::new(host_dir, self.root_overlays.clone());
            Ok(Node::new(
                FileID::root(),
                NodeType::Directory(overlay_dir.create_handle()),
            ))
        } else {
            self.inner.load_node(id).await
        }
    }

    async fn store_node(&self, node: &Node) -> Result<()> {
        self.inner.store_node(node).await
    }

    async fn create_file_node(&self, id: FileID) -> Result<Node> {
        self.inner.create_file_node(id).await
    }

    async fn create_directory_node(&self, id: FileID) -> Result<Node> {
        self.inner.create_directory_node(id).await
    }

    async fn create_symlink_node(&self, id: FileID, target: &Path) -> Result<Node> {
        self.inner.create_symlink_node(id, target).await
    }

    async fn create_dynamic_node(
        &self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<Node> {
        self.inner
            .create_dynamic_node(id, factory_type, config_content)
            .await
    }

    async fn get_dynamic_node_config(&self, id: FileID) -> Result<Option<(String, Vec<u8>)>> {
        self.inner.get_dynamic_node_config(id).await
    }

    async fn update_dynamic_node_config(
        &self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<()> {
        self.inner
            .update_dynamic_node_config(id, factory_type, config_content)
            .await
    }

    async fn metadata(&self, id: FileID) -> Result<NodeMetadata> {
        self.inner.metadata(id).await
    }

    async fn list_file_versions(&self, id: FileID) -> Result<Vec<FileVersionInfo>> {
        self.inner.list_file_versions(id).await
    }

    async fn read_file_version(&self, id: FileID, version: u64) -> Result<Vec<u8>> {
        self.inner.read_file_version(id, version).await
    }

    async fn set_extended_attributes(
        &self,
        id: FileID,
        attributes: HashMap<String, String>,
    ) -> Result<()> {
        self.inner.set_extended_attributes(id, attributes).await
    }
}
