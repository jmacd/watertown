// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::EntryType;
use crate::dir::{Directory, DirectoryEntry, Handle};
use crate::error::{Error, Result};
use crate::metadata::{Metadata, NodeMetadata};
use crate::node::Node;
use async_trait::async_trait;
use futures::stream::{self, Stream};
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;

/// Represents a directory backed by a BTreeMap
/// This implementation stores directory entries in memory and is suitable for
/// testing, development, and lightweight filesystem operations.
pub struct MemoryDirectory {
    entries: BTreeMap<String, Node>,
    entry_type: EntryType,
}

#[async_trait]
impl Metadata for MemoryDirectory {
    async fn metadata(&self) -> Result<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,         // Memory directories don't track versions
            size: None,         // Directories don't have sizes
            blake3: None,       // Directories don't have checksums
            bao_outboard: None, // Directories don't have bao-tree data
            entry_type: self.entry_type,
            timestamp: 0, // TODO
        })
    }
}

#[async_trait]
impl Directory for MemoryDirectory {
    async fn get(&self, name: &str) -> Result<Option<Node>> {
        Ok(self.entries.get(name).cloned())
    }

    async fn insert(&mut self, name: String, id: Node) -> Result<()> {
        if self.entries.insert(name.clone(), id).is_some() {
            // Note this is not a full path.
            return Err(Error::already_exists(&name));
        }
        Ok(())
    }

    async fn remove(&mut self, name: &str) -> Result<Option<Node>> {
        Ok(self.entries.remove(name))
    }

    async fn entries(&self) -> Result<Pin<Box<dyn Stream<Item = Result<DirectoryEntry>> + Send>>> {
        let mut items = Vec::new();
        for (name, node) in &self.entries {
            let dir_entry = DirectoryEntry::new(
                name.clone(),
                node.id().node_id(),
                node.entry_type(),
                0, // Version not tracked in memory @@@
            );
            items.push(Ok(dir_entry));
        }
        Ok(Box::pin(stream::iter(items)))
    }
}

impl MemoryDirectory {
    /// Create a new MemoryDirectory handle
    #[must_use]
    pub fn new_handle() -> Handle {
        Self::new_handle_with_entry_type(EntryType::DirectoryPhysical)
    }

    /// Create a new MemoryDirectory handle with an explicit directory type.
    #[must_use]
    pub fn new_handle_with_entry_type(entry_type: EntryType) -> Handle {
        debug_assert!(entry_type.is_directory());
        Handle::new(Arc::new(tokio::sync::Mutex::new(Box::new(
            MemoryDirectory {
                entries: BTreeMap::new(),
                entry_type,
            },
        ))))
    }

    /// Convert this MemoryDirectory into a Handle
    #[must_use]
    pub fn to_handle(self) -> Handle {
        Handle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }
}

impl Default for MemoryDirectory {
    /// Create a new empty MemoryDirectory
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            entry_type: EntryType::DirectoryPhysical,
        }
    }
}
