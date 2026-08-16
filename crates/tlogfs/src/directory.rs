// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::persistence::State;
use async_trait::async_trait;
use futures::Stream;
use log::debug;
use std::pin::Pin;
use std::sync::Arc;
use tinyfs::ResultExt;
use tinyfs::{
    DirHandle, Directory, FileID, Metadata, Node, NodeMetadata, persistence::PersistenceLayer,
};
use tokio::sync::Mutex;

pub struct OpLogDirectory {
    /// Unique node identifier for this directory
    id: FileID,

    /// Reference to persistence layer (single source of truth)
    state: State,
}

impl OpLogDirectory {
    /// Create new directory instance with persistence layer dependency injection
    #[must_use]
    pub fn new(id: FileID, state: State) -> Self {
        debug!("directory part {id}");

        Self { id, state }
    }

    /// Create a DirHandle from this directory
    #[must_use]
    pub fn create_handle(self) -> DirHandle {
        DirHandle::new(Arc::new(Mutex::new(Box::new(self))))
    }
}

#[async_trait]
impl Metadata for OpLogDirectory {
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        self.state.metadata(self.id).await
    }
}

#[async_trait]
impl Directory for OpLogDirectory {
    async fn get(&self, name: &str) -> tinyfs::Result<Option<Node>> {
        debug!("OpLogDirectory::get('{name}')");

        // Load directory state into memory if not already present
        self.state
            .ensure_directory_loaded(self.id)
            .await
            .map_other_context("Failed to load directory")?;

        // Get entry from in-memory state
        let entry_opt = self
            .state
            .get_directory_entry(self.id, name)
            .await
            .map_other_context("Failed to get directory entry")?;

        let Some(entry) = entry_opt else {
            debug!("OpLogDirectory::get: entry '{name}' not found");
            return Ok(None);
        };

        debug!("OpLogDirectory::get: found entry '{name}'!");

        // Construct FileID based on entry type:
        // - Physical directories: part_id == node_id (self-partitioned)
        // - Everything else: part_id == parent's part_id
        // Use the entry's stored pond_id for cross-pond imports,
        // falling back to the parent's pond_id for local entries.
        let child_pond_id = entry
            .pond_id
            .as_ref()
            .and_then(|s| s.parse::<uuid7::Uuid>().ok())
            .unwrap_or_else(|| self.id.pond_id());

        let child_file_id = if entry.entry_type == tinyfs::EntryType::DirectoryPhysical {
            FileID::from_physical_dir_node_id(entry.child_node_id, child_pond_id)
        } else {
            FileID::new_from_ids(self.id.part_id(), entry.child_node_id, child_pond_id)
        };

        // Load the child node
        let child_node = self.state.load_node(child_file_id).await?;
        Ok(Some(child_node))
    }

    async fn entry(&self, name: &str) -> tinyfs::Result<Option<tinyfs::DirectoryEntry>> {
        self.state
            .ensure_directory_loaded(self.id)
            .await
            .map_other_context("Failed to load directory")?;
        self.state
            .get_directory_entry(self.id, name)
            .await
            .map_other_context("Failed to get directory entry")
    }

    async fn insert(&mut self, name: String, node: Node) -> tinyfs::Result<()> {
        debug!("OpLogDirectory::insert('{name}', {:?})", node.id());

        // Ensure directory is loaded in memory
        self.state
            .ensure_directory_loaded(self.id)
            .await
            .map_other_context("Failed to load directory")?;

        // Create the new entry
        let node_id = node.id();
        let mut new_entry = tinyfs::DirectoryEntry::new(
            name.clone(),
            node_id.node_id(),
            node_id.entry_type(),
            1, // version_last_modified - will be set properly at flush time
        );

        // For cross-pond imports: if the child's pond_id differs from
        // this directory's pond_id, record it on the entry so get()
        // can reconstruct the correct FileID later.
        if node_id.pond_id() != self.id.pond_id() {
            new_entry.pond_id = Some(node_id.pond_id().to_string());
        }

        // Insert into in-memory state (checks for duplicates and marks as modified)
        self.state
            .insert_directory_entry(self.id, new_entry)
            .await
            .map_other_context("Failed to insert directory entry")?;

        Ok(())
    }

    async fn remove(&mut self, name: &str) -> tinyfs::Result<Option<Node>> {
        debug!("OpLogDirectory::remove('{name}')");

        // Ensure directory is loaded in memory
        self.state
            .ensure_directory_loaded(self.id)
            .await
            .map_other_context("Failed to load directory")?;

        // Remove from in-memory state
        let removed_entry = self
            .state
            .remove_directory_entry(self.id, name)
            .await
            .map_other_context("Failed to remove directory entry")?;

        // If entry existed, load and return the node
        match removed_entry {
            Some(entry) => {
                let child_pond_id = entry
                    .pond_id
                    .as_ref()
                    .and_then(|s| s.parse::<uuid7::Uuid>().ok())
                    .unwrap_or_else(|| self.id.pond_id());

                let child_file_id = if entry.entry_type == tinyfs::EntryType::DirectoryPhysical {
                    FileID::from_physical_dir_node_id(entry.child_node_id, child_pond_id)
                } else {
                    FileID::new_from_ids(self.id.part_id(), entry.child_node_id, child_pond_id)
                };
                let child_node = self.state.load_node(child_file_id).await?;
                Ok(Some(child_node))
            }
            None => Ok(None),
        }
    }

    async fn unlink(&mut self, name: &str) -> tinyfs::Result<bool> {
        self.state
            .ensure_directory_loaded(self.id)
            .await
            .map_other_context("Failed to load directory")?;
        Ok(self
            .state
            .remove_directory_entry(self.id, name)
            .await
            .map_other_context("Failed to remove directory entry")?
            .is_some())
    }

    async fn entries(
        &self,
    ) -> tinyfs::Result<Pin<Box<dyn Stream<Item = tinyfs::Result<tinyfs::DirectoryEntry>> + Send>>>
    {
        debug!("OpLogDirectory::entries() called for directory {}", self.id);

        // Load directory state into memory if not already present
        self.state
            .ensure_directory_loaded(self.id)
            .await
            .map_other_context("Failed to load directory")?;

        // Get all entries from in-memory state
        let entries = self
            .state
            .get_all_directory_entries(self.id)
            .await
            .map_other_context("Failed to get directory entries")?;

        debug!(
            "OpLogDirectory::entries() - returning {} entries",
            entries.len()
        );

        // Convert Vec<DirectoryEntry> into a stream
        let stream = futures::stream::iter(entries.into_iter().map(Ok));
        Ok(Box::pin(stream))
    }
}
