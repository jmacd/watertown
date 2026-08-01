// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::EntryType;
use crate::caching_persistence::CachingPersistence;
use crate::error::*;
use crate::node::*;
use crate::persistence::{FileVersionInfo, PersistenceLayer};
use crate::wd::WD;

/// Main filesystem structure - pure persistence layer architecture (Phase 5)
#[derive(Clone)]
pub struct FS {
    pub(crate) persistence: Arc<dyn PersistenceLayer>,

    /// Coordination state for loop detection    
    busy: Arc<Mutex<HashSet<FileID>>>,
}

impl FS {
    /// Creates a filesystem with a PersistenceLayer
    /// Automatically wraps the persistence layer with caching for improved performance
    pub async fn new<P: PersistenceLayer + 'static>(persistence: P) -> Result<Self> {
        // Wrap persistence layer with caching decorator for transparent node caching
        let cached_persistence = CachingPersistence::new(persistence);

        Ok(FS {
            persistence: Arc::new(cached_persistence),
            busy: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    /// Creates a filesystem from an Arc<dyn PersistenceLayer>
    /// Does not add caching - assumes the persistence layer is already wrapped if needed
    #[must_use]
    pub fn from_arc(persistence: Arc<dyn PersistenceLayer>) -> Self {
        FS {
            persistence,
            busy: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Returns a working directory context for the root directory
    /// The root directory must be explicitly initialized before calling this method
    pub async fn root(&self) -> Result<WD> {
        let root_id = FileID::root_for(self.persistence.pond_uuid());
        let root_node = self.persistence.load_node(root_id).await?;
        let node = NodePath {
            node: root_node,
            path: "/".into(),
        };
        WD::new(node.clone(), self.clone(), node).await
    }

    pub async fn wd(&self, np: &NodePath, effective_root: NodePath) -> Result<WD> {
        WD::new(np.clone(), self.clone(), effective_root).await
    }

    /// Construct a directory node handle for the root of a foreign pond,
    /// without writing to persistence.
    ///
    /// The foreign pond's data is expected to already exist in this
    /// filesystem's persistence layer (typically after a cross-pond
    /// `apply_pulled_bundle` deposited it). This method returns a `Node`
    /// whose `FileID` has `pond_id = foreign_pond_id` and the well-known
    /// root `node_id`/`part_id`; pass it to [`WD::insert_node`] to
    /// materialize a mount point in a local directory.
    ///
    /// Safe to call even if the foreign root has not yet been pulled --
    /// the returned handle just lazy-loads from persistence on access.
    pub async fn foreign_root_node(&self, foreign_pond_id: uuid7::Uuid) -> Result<Node> {
        let foreign_root_id = FileID::root_for(foreign_pond_id);
        self.persistence
            .create_directory_node(foreign_root_id)
            .await
    }

    /// Initialize an empty root directory for a foreign pond, so a cross-pond
    /// import can populate its tree.  Must be called within a write
    /// transaction; the empty root commits with the import's other writes.
    pub async fn initialize_foreign_root(&self, foreign_pond_id: uuid7::Uuid) -> Result<()> {
        let foreign_root_id = FileID::root_for(foreign_pond_id);
        let root_node = self
            .persistence
            .create_directory_node(foreign_root_id)
            .await?;
        self.persistence.store_node(&root_node).await?;
        self.persistence
            .initialize_foreign_root(foreign_pond_id)
            .await
    }

    /// Create a new node with persistence
    pub(crate) async fn create_node(&self, parent_id: FileID, node_type: NodeType) -> Result<Node> {
        let id = parent_id.new_child_id(node_type.entry_type().await?);
        let node = Node::new(id, node_type);
        self.persistence.store_node(&node).await?;
        Ok(node)
    }

    // Loop detection methods - these work the same regardless of persistence vs backend
    pub(crate) async fn enter_node(&self, node: &NodePath) -> Result<()> {
        let mut busy = self.busy.lock().await;
        let id = node.id();
        if busy.contains(&id) {
            return Err(Error::visit_loop(node.path()));
        }
        _ = busy.insert(id);
        Ok(())
    }

    pub(crate) async fn exit_node(&self, node: &NodePath) {
        let mut busy = self.busy.lock().await;
        _ = busy.remove(&node.id());
    }

    pub(crate) async fn create_file_node_pending_write(
        &self,
        parent_id: FileID,
        entry_type: EntryType,
    ) -> Result<Node> {
        // Generate a new node ID
        let id = parent_id.new_child_id(entry_type);

        // Create the file node in memory only - no immediate persistence
        self.persistence.create_file_node(id).await
    }

    /// Create a new symlink node and return its Node
    pub(crate) async fn create_symlink_node(
        &self,
        parent_id: FileID,
        target: &str,
    ) -> Result<Node> {
        let id = parent_id.new_child_id(EntryType::Symlink);

        let target_path = std::path::Path::new(target);
        self.persistence
            .create_symlink_node(id, target_path, None)
            .await
    }

    /// List all versions of a file
    pub(crate) async fn list_file_versions(&self, id: FileID) -> Result<Vec<FileVersionInfo>> {
        self.persistence.list_file_versions(id).await
    }

    /// Read a specific version of a file
    /// @@@ BAD
    pub(crate) async fn read_file_version(&self, id: FileID, version: u64) -> Result<Vec<u8>> {
        self.persistence.read_file_version(id, version).await
    }

    /// Create a dynamic directory node with factory type and configuration
    pub(crate) async fn create_dynamic_node(
        &self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
        mtime: Option<i64>,
    ) -> Result<Node> {
        self.persistence
            .create_dynamic_node(id, factory_type, config_content, mtime)
            .await
    }

    /// Update the configuration of an existing dynamic node
    pub(crate) async fn update_dynamic_node_config(
        &self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<()> {
        self.persistence
            .update_dynamic_node_config(id, factory_type, config_content)
            .await
    }

    /// Read the factory type and configuration of a dynamic node
    pub async fn get_dynamic_node_config(&self, id: FileID) -> Result<Option<(String, Vec<u8>)>> {
        self.persistence.get_dynamic_node_config(id).await
    }

    /// Set extended attributes on an existing node
    pub(crate) async fn set_extended_attributes(
        &self,
        id: FileID,
        attributes: HashMap<String, String>,
    ) -> Result<()> {
        self.persistence
            .set_extended_attributes(id, attributes)
            .await
    }
}

impl std::fmt::Debug for FS {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FS{{}}")
    }
}

impl PartialEq<FS> for FS {
    fn eq(&self, other: &FS) -> bool {
        std::ptr::eq(self, other)
    }
}
