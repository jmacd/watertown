// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use crate::EntryType;
use crate::error::*;
use crate::metadata::Metadata;
use crate::node::*;
use async_trait::async_trait;
use futures::{StreamExt, stream::Stream};

/// Lightweight directory entry information without loading the full node.
/// Contains just enough information to filter entries and determine partition assignment.
#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct DirectoryEntry {
    /// Entry name within the directory
    pub name: String,
    /// NodeID of the child (combined with directory's part_id to form FileID)
    pub child_node_id: NodeID,
    /// Comprehensive entry type, redundant.
    pub entry_type: EntryType,
    /// Version number when this entry was last modified
    pub version_last_modified: i64,
    /// Pond identity for cross-pond imports. None means the child belongs
    /// to the same pond as its parent directory. Some(uuid) means the
    /// child is from a foreign pond (e.g., an imported mount point).
    /// Existing serialized data without this field deserializes as None.
    #[serde(default)]
    pub pond_id: Option<String>,
}

impl DirectoryEntry {
    /// Create a new directory entry (local, same pond as parent)
    #[must_use]
    pub fn new(
        name: String,
        child_node_id: NodeID,
        entry_type: EntryType,
        version_last_modified: i64,
    ) -> Self {
        Self {
            name,
            child_node_id,
            entry_type,
            version_last_modified,
            pond_id: None,
        }
    }

    /// Set the pond_id for a cross-pond import entry.
    #[must_use]
    pub fn with_pond_id(mut self, pond_id: String) -> Self {
        self.pond_id = Some(pond_id);
        self
    }
}

/// Represents a directory containing named entries.
#[async_trait]
pub trait Directory: Metadata + Send + Sync {
    async fn get(&self, name: &str) -> Result<Option<Node>>;

    /// Get lightweight entry metadata without loading the referenced node.
    async fn entry(&self, name: &str) -> Result<Option<DirectoryEntry>> {
        let mut entries = self.entries().await?;
        while let Some(entry) = entries.next().await {
            let entry = entry?;
            if entry.name == name {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    async fn insert(&mut self, name: String, id: Node) -> Result<()>;

    /// Remove a directory entry by name.
    /// Returns the removed node if it existed, None if not found.
    /// Does NOT delete the underlying node - only removes the directory link.
    async fn remove(&mut self, name: &str) -> Result<Option<Node>>;

    /// Unlink a directory entry without loading the referenced node.
    ///
    /// Implementations should override this when dangling entries must remain
    /// removable. The default preserves compatibility for read-through stores.
    async fn unlink(&mut self, name: &str) -> Result<bool> {
        Ok(self.remove(name).await?.is_some())
    }

    /// Returns a stream of directory entries (lightweight metadata) without loading full nodes.
    /// Callers should use batch loading to load multiple nodes efficiently.
    /// Returns DirectoryEntry which contains the name, so no tuple needed.
    async fn entries(&self) -> Result<Pin<Box<dyn Stream<Item = Result<DirectoryEntry>> + Send>>>;
}

/// A handle for a refcounted directory.
#[derive(Clone)]
pub struct Handle(Arc<tokio::sync::Mutex<Box<dyn Directory>>>);

/// Represents a Dir/File/Symlink handle with the active path.
#[derive(Clone)]
pub struct Pathed<T> {
    pub handle: T,
    path: PathBuf,
}

impl<T> Pathed<T> {
    /// Return a new Pathed with the given path, keeping the same handle.
    #[must_use]
    pub fn with_path(self, path: PathBuf) -> Self {
        Self {
            handle: self.handle,
            path,
        }
    }
}

impl Handle {
    pub fn new(r: Arc<tokio::sync::Mutex<Box<dyn Directory>>>) -> Self {
        Self(r)
    }

    pub async fn get(&self, name: &str) -> Result<Option<Node>> {
        let dir = self.0.lock().await;
        dir.get(name).await
    }

    pub async fn entry(&self, name: &str) -> Result<Option<DirectoryEntry>> {
        let dir = self.0.lock().await;
        dir.entry(name).await
    }

    pub async fn insert(&self, name: String, id: Node) -> Result<()> {
        log::debug!(
            "Handle::insert() - forwarding to Directory trait: {name}",
            name = name
        );
        let mut dir = self.0.lock().await;
        let type_name = std::any::type_name::<dyn Directory>();
        log::debug!(
            "Handle::insert() - calling Directory::insert() on: {type_name}",
            type_name = type_name
        );
        let result = dir.insert(name, id).await;
        let result_str = result
            .as_ref()
            .map(|_| "Ok")
            .map_err(|e| format!("Err({})", e));
        let result_display = format!("{:?}", result_str);
        log::debug!(
            "Handle::insert() - Directory::insert() completed with result: {result_display}",
            result_display = result_display
        );
        result
    }

    pub async fn remove(&self, name: &str) -> Result<Option<Node>> {
        log::debug!("Handle::remove() - forwarding to Directory trait: {name}");
        let mut dir = self.0.lock().await;
        dir.remove(name).await
    }

    pub async fn unlink(&self, name: &str) -> Result<bool> {
        let mut dir = self.0.lock().await;
        dir.unlink(name).await
    }

    pub async fn entries(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<DirectoryEntry>> + Send>>> {
        let dir = self.0.lock().await;
        dir.entries().await
    }

    /// Get metadata through the directory handle
    pub async fn metadata(&self) -> Result<crate::NodeMetadata> {
        let dir = self.0.lock().await;
        dir.metadata().await
    }

    /// Access the underlying directory for mutation (clones the Arc)
    #[must_use]
    pub fn get_inner(&self) -> Arc<tokio::sync::Mutex<Box<dyn Directory>>> {
        self.0.clone()
    }
}

impl<T> Pathed<T> {
    pub fn new<P: AsRef<Path>>(path: P, handle: T) -> Self {
        Self {
            handle,
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn path(&self) -> PathBuf {
        self.path.to_path_buf()
    }
}

impl Pathed<crate::file::Handle> {
    /// Get async reader with seek support for file content
    pub async fn async_reader(&self) -> Result<Pin<Box<dyn crate::file::AsyncReadSeek>>> {
        self.handle.async_reader().await
    }

    /// Get async reader that may prune append-only series versions per the
    /// supplied [`crate::file::SeriesReadBounds`].
    /// See [`crate::file::File::async_reader_bounded`].
    pub async fn async_reader_bounded(
        &self,
        bounds: crate::file::SeriesReadBounds,
    ) -> Result<Pin<Box<dyn crate::file::AsyncReadSeek>>> {
        self.handle.async_reader_bounded(bounds).await
    }

    /// Get async writer for streaming file content
    pub async fn async_writer(&self) -> Result<Pin<Box<dyn crate::file::FileMetadataWriter>>> {
        self.handle.async_writer().await
    }

    /// Get async writer that replaces all earlier versions of a series node with
    /// a fresh baseline (see [`crate::file::File::async_writer_collapsing`]).
    pub async fn async_writer_collapsing(
        &self,
    ) -> Result<Pin<Box<dyn crate::file::FileMetadataWriter>>> {
        self.handle.async_writer_collapsing().await
    }

    /// Get metadata through the file handle
    pub async fn metadata(&self) -> Result<crate::NodeMetadata> {
        self.handle.metadata().await
    }
}

impl Pathed<Handle> {
    pub async fn get(&self, name: &str) -> Result<Option<NodePath>> {
        if let Some(nr) = self.handle.get(name).await? {
            Ok(Some(NodePath {
                node: nr,
                path: self.path.join(name),
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn insert(&self, name: String, id: Node) -> Result<()> {
        self.handle.insert(name, id).await
    }
}

impl Pathed<crate::symlink::Handle> {
    pub async fn readlink(&self) -> Result<PathBuf> {
        self.handle.readlink().await
    }
}
