// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::EntryType;
use crate::dir::*;
use crate::error::ResultExt;
use crate::error::*;
use crate::fs::FS;
use crate::node::*;
use crate::symlink::*;
use async_trait::async_trait;
use futures::stream::Stream;
use log::debug;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use tokio::io::AsyncRead;
use utilities::glob::{WildcardComponent, parse_glob};

/// Context for operations within a specific directory
#[derive(Clone)]
pub struct WD {
    np: NodePath,
    fs: FS,
    dref: DirNode,
    effective_root: NodePath,
}

/// Result of path resolution
pub enum Lookup {
    Empty(NodePath),
    Found(NodePath),
    NotFound(PathBuf, String),
}

/// Copy destination semantics
#[derive(Debug)]
pub enum CopyDestination {
    /// Target is explicitly a directory (trailing slash)
    Directory,
    /// Target is an existing directory (no trailing slash)
    ExistingDirectory,
    /// Target is an existing file (no trailing slash)
    ExistingFile,
    /// Target is a new path (no trailing slash)
    NewPath(String),
}

impl WD {
    pub(crate) async fn new(np: NodePath, fs: FS, effective_root: NodePath) -> Result<Self> {
        let dref = np.as_dir().await?;
        Ok(Self {
            np,
            fs,
            dref,
            effective_root,
        })
    }

    /// Detects if a path has a trailing slash, which indicates directory intent
    fn has_trailing_slash<P: AsRef<Path>>(path: P) -> bool {
        let path_str = path.as_ref().to_string_lossy();
        path_str.ends_with('/') && path_str.len() > 1 // Don't count root "/" as trailing
    }

    /// Strips trailing slash from path for component parsing
    fn strip_trailing_slash<P: AsRef<Path>>(path: P) -> PathBuf {
        let path_str = path.as_ref().to_string_lossy();
        if Self::has_trailing_slash(&path) {
            PathBuf::from(path_str.trim_end_matches('/'))
        } else {
            path.as_ref().to_path_buf()
        }
    }

    /// Get directory entries without loading nodes (for filtering before batch load)
    async fn get_entries(&self) -> Result<Vec<DirectoryEntry>> {
        use futures::StreamExt;
        let mut stream = self.dref.handle.entries().await?;
        let mut entries = Vec::new();
        while let Some(result) = stream.next().await {
            let entry = result?;
            // The reserved node-manifest index node is internal infrastructure
            // and never appears in directory listings or traversal.
            if entry.child_node_id.is_index() || entry.child_node_id.is_log() {
                continue;
            }
            entries.push(entry);
        }
        Ok(entries)
    }

    /// Get a stream of lightweight directory entries (does NOT load nodes)
    /// Use this for inspecting directory contents without loading all nodes.
    /// For efficient node loading, use pattern matching or batch operations.
    ///
    /// The reserved node-manifest index node ([`crate::INDEX_NODE_UUID`]) is
    /// filtered out so it never surfaces in user-facing enumeration.
    pub async fn entries(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<DirectoryEntry>> + Send>>> {
        use futures::StreamExt;
        let stream = self.dref.handle.entries().await?;
        Ok(Box::pin(stream.filter(|result| {
            let keep = match result {
                Ok(entry) => !entry.child_node_id.is_index() && !entry.child_node_id.is_log(),
                Err(_) => true,
            };
            async move { keep }
        })))
    }

    /// Get a single node by name (loads the node from persistence)
    /// Note: For multiple nodes, prefer batch loading via pattern matching
    pub async fn get(&self, name: &str) -> Result<Option<NodePath>> {
        self.dref.get(name).await
    }

    fn id(&self) -> FileID {
        self.np.id()
    }

    /// Mark the current directory as the effective root for path resolution.
    /// All absolute paths will resolve relative to this position.
    /// Symlinks cannot escape above this boundary.
    #[must_use]
    pub fn as_root(mut self) -> Self {
        self.effective_root = self.np.clone();
        // Reset the display path to "/" so that paths built from this
        // root (e.g. by collect_matches) are relative to the new root,
        // not the global tree.
        self.np.path = "/".into();
        self.effective_root.path = "/".into();
        self.dref = self.dref.with_path("/".into());
        self
    }

    /// Returns a reference to the effective root.
    #[must_use]
    pub fn effective_root(&self) -> &NodePath {
        &self.effective_root
    }

    /// Check if this WD is at its effective root boundary.
    #[must_use]
    pub fn is_at_root_boundary(&self) -> bool {
        self.np.id() == self.effective_root.id()
    }

    /// Check that this WD is writable (not inside a read-only foreign import).
    /// A directory is "foreign" when its pond_id differs from the effective
    /// root's pond_id, meaning it was imported from another pond.
    fn check_writable(&self) -> Result<()> {
        if self.np.id().pond_id() != self.effective_root.id().pond_id() {
            return Err(Error::read_only_import(self.np.path()));
        }
        Ok(())
    }

    /// Create a child WD that inherits the effective root from self.
    async fn child_wd(&self, np: &NodePath) -> Result<WD> {
        WD::new(np.clone(), self.fs.clone(), self.effective_root.clone()).await
    }

    #[must_use]
    pub fn node_path(&self) -> NodePath {
        self.np.clone()
    }

    /// Insert a pre-created node into this directory.
    /// Used for cross-pond import where the node has a foreign FileID.
    ///
    /// The parent directory (this WD) must itself be writable -- you
    /// cannot insert into a foreign-mount subdirectory.  The inserted
    /// node, however, may carry any pond_id (this is exactly how
    /// cross-pond mount entries are materialized: local parent,
    /// foreign child).
    pub async fn insert_node(&self, name: &str, node: Node) -> Result<NodePath> {
        self.check_writable()?;
        self.dref.insert(name.to_string(), node.clone()).await?;
        Ok(NodePath {
            node,
            path: self.dref.path().join(name),
        })
    }

    // Generic node creation method for all node types
    async fn create_node<T, F>(&self, name: &str, node_creator: F) -> Result<NodePath>
    where
        F: FnOnce() -> T,
        T: Into<NodeType>,
    {
        let node_type = node_creator().into();
        let node = self.fs.create_node(self.id(), node_type).await?;
        self.dref.insert(name.to_string(), node.clone()).await?;
        Ok(NodePath {
            node,
            path: self.dref.path().join(name),
        })
    }

    // Generic path-based node creation for all node types
    pub async fn create_node_path<P, F>(&self, path: P, node_creator: F) -> Result<NodePath>
    where
        P: AsRef<Path>,
        F: FnOnce() -> Result<NodeType>,
    {
        let path_clone = path.as_ref().to_path_buf();
        self.in_path(path.as_ref(), |wd, entry| async move {
            wd.check_writable()?;
            match entry {
                Lookup::NotFound(_, name) => {
                    let node_type = node_creator()?;
                    wd.create_node(&name, || node_type).await
                }
                Lookup::Found(_) => {
                    // Need to use async block to match the other arm
                    async { Err(Error::already_exists(&path_clone)) }.await
                }
                Lookup::Empty(_) => Err(Error::empty_path()),
            }
        })
        .await
    }

    pub async fn get_node_path<P>(&self, path: P) -> Result<NodePath>
    where
        P: AsRef<Path>,
    {
        let path_ref = path.as_ref();
        self.in_path(path_ref, |_wd, entry| async move {
            match entry {
                Lookup::NotFound(_, _) => Err(Error::not_found(path_ref)),
                Lookup::Found(np) => Ok(np),
                Lookup::Empty(np) => Ok(np),
            }
        })
        .await
    }

    /// Check if a path exists in the filesystem
    pub async fn exists<P>(&self, path: P) -> bool
    where
        P: AsRef<Path>,
    {
        self.get_node_path(path).await.is_ok()
    }

    /// Creates a file at the specified path with streaming content
    pub async fn create_file_path_streaming<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<(NodePath, Pin<Box<dyn crate::file::FileMetadataWriter>>)> {
        self.create_file_path_streaming_with_type(path, EntryType::FilePhysicalVersion)
            .await
    }

    /// Creates a file at the specified path with streaming content and specified entry type
    pub async fn create_file_path_streaming_with_type<P: AsRef<Path>>(
        &self,
        path: P,
        entry_type: EntryType,
    ) -> Result<(NodePath, Pin<Box<dyn crate::file::FileMetadataWriter>>)> {
        let path_clone = path.as_ref().to_path_buf();

        let node_path = self
            .in_path(path.as_ref(), |wd, entry| async move {
                wd.check_writable()?;
                match entry {
                    Lookup::NotFound(_, name) => {
                        // Use the actual parent directory's node ID
                        let parent_id = wd.id();

                        // Create empty file node first with specified entry type
                        let node = wd
                            .fs
                            .create_file_node_pending_write(parent_id, entry_type)
                            .await?;

                        // Store the node so it can be loaded later
                        wd.fs.persistence.store_node(&node).await?;

                        // Insert into the directory and return NodePath
                        wd.dref.insert(name.clone(), node.clone()).await?;
                        Ok(NodePath {
                            node,
                            path: wd.dref.path().join(&name),
                        })
                    }
                    Lookup::Found(_) => Err(Error::already_exists(&path_clone)),
                    Lookup::Empty(_) => Err(Error::empty_path()),
                }
            })
            .await?;

        // Get streaming writer for the created file
        let file_handle = node_path.as_file().await?;
        let writer = file_handle.async_writer().await?;
        Ok((node_path, writer))
    }

    /// Creates a file writer for the specified path (convenience method)
    pub async fn create_file_writer<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<Pin<Box<dyn crate::file::FileMetadataWriter>>> {
        let (_, writer) = self.create_file_path_streaming(path).await?;
        Ok(writer)
    }

    /// Creates a file writer for the specified path with specific entry type (convenience method)
    pub async fn create_file_writer_with_type<P: AsRef<Path>>(
        &self,
        path: P,
        entry_type: EntryType,
    ) -> Result<Pin<Box<dyn crate::file::FileMetadataWriter>>> {
        let (_, writer) = self
            .create_file_path_streaming_with_type(path, entry_type)
            .await?;
        Ok(writer)
    }
    /// Creates a symlink at the specified path
    pub async fn create_symlink_path<P, T>(&self, path: P, target: T) -> Result<NodePath>
    where
        P: AsRef<Path>,
        T: AsRef<str>,
    {
        let target = target.as_ref();
        let path = path.as_ref();

        self.in_path(path, |wd, entry| async move {
            wd.check_writable()?;
            match entry {
                Lookup::NotFound(_, name) => {
                    let parent_id = wd.id();
                    let node = wd.fs.create_symlink_node(parent_id, target).await?;

                    // Store the node so it can be loaded later
                    wd.fs.persistence.store_node(&node).await?;

                    // Insert into the directory and return NodePath
                    wd.dref.insert(name.clone(), node.clone()).await?;
                    Ok(NodePath {
                        node,
                        path: wd.dref.path().join(&name), // @@@ Repetitive
                    })
                }
                Lookup::Found(_) => Err(Error::already_exists(path)),
                Lookup::Empty(_) => Err(Error::empty_path()),
            }
        })
        .await
    }

    /// Creates a directory at the specified path.
    /// The parent directory must already exist; use `create_dir_all` for mkdir -p semantics.
    pub async fn create_dir_path<P: AsRef<Path>>(&self, path: P) -> Result<WD> {
        let path_clone = path.as_ref().to_path_buf();

        let node = self
            .in_path(path.as_ref(), |wd, entry| async move {
                wd.check_writable()?;
                match entry {
                    Lookup::NotFound(_, name) => {
                        let id = FileID::new_physical_dir_id(wd.effective_root.id().pond_id());
                        let node = wd.fs.persistence.create_directory_node(id).await?;
                        wd.fs.persistence.store_node(&node).await?;

                        wd.dref.insert(name.clone(), node.clone()).await?;
                        Ok(NodePath::new(node, wd.dref.path().join(&name)))
                    }
                    Lookup::Found(_) => Err(Error::already_exists(&path_clone)),
                    Lookup::Empty(_) => Err(Error::empty_path()),
                }
            })
            .await?;

        self.child_wd(&node).await
    }

    /// Adopt a directory child under `name` with an explicit `node_id`,
    /// returning a working directory for it.
    ///
    /// Used by content-addressed pull to mirror a source pond's node
    /// identities (Decision D8): the consumer adopts the source's `node_id`
    /// so the rebuilt pond is row-identical and later pulls are a
    /// `node_id`-keyed diff.  `name` must be a direct child of this directory
    /// and must not already exist.  The child's partition is derived from the
    /// adopted `node_id` (physical directories are self-partitioned).
    pub async fn insert_directory_with_id(&self, name: &str, node_id: NodeID) -> Result<WD> {
        self.check_writable()?;
        if self.dref.get(name).await?.is_some() {
            return Err(Error::already_exists(name));
        }
        let id = self.id().child_id(node_id);
        let node = self.fs.persistence.create_directory_node(id).await?;
        self.fs.persistence.store_node(&node).await?;
        self.dref.insert(name.to_string(), node.clone()).await?;
        let np = NodePath::new(node, self.dref.path().join(name));
        self.child_wd(&np).await
    }

    /// Adopt an empty file or table child under `name` with an explicit
    /// `node_id`, returning a streaming writer for its first version.
    ///
    /// The entry type is carried by the `node_id` nibble, so a series node
    /// adopts the series type automatically.  Mirrors
    /// [`create_file_path_streaming_with_type`] but adopts a caller-supplied
    /// id; the returned writer must be used to write the first version (a
    /// pending file has no row to re-resolve by path yet).  `name` must be a
    /// direct child of this directory and must not already exist.
    ///
    /// [`create_file_path_streaming_with_type`]: WD::create_file_path_streaming_with_type
    pub async fn create_file_with_id(
        &self,
        name: &str,
        node_id: NodeID,
    ) -> Result<Pin<Box<dyn crate::file::FileMetadataWriter>>> {
        self.check_writable()?;
        if self.dref.get(name).await?.is_some() {
            return Err(Error::already_exists(name));
        }
        let id = self.id().child_id(node_id);
        let node = self.fs.persistence.create_file_node(id).await?;
        self.fs.persistence.store_node(&node).await?;
        self.dref.insert(name.to_string(), node.clone()).await?;
        let np = NodePath::new(node, self.dref.path().join(name));
        let file_handle = np.as_file().await?;
        file_handle.async_writer().await
    }

    /// Adopt a symlink child under `name` with an explicit `node_id`, pointing
    /// at `target`.  `name` must be a direct child of this directory and must
    /// not already exist.
    ///
    /// `mtime` sets the node's timestamp explicitly (microseconds since the
    /// Unix epoch); `None` means now.  Replication passes the source's mtime so
    /// the mirrored node keeps it.
    pub async fn insert_symlink_with_id(
        &self,
        name: &str,
        node_id: NodeID,
        target: &str,
        mtime: Option<i64>,
    ) -> Result<()> {
        self.check_writable()?;
        if self.dref.get(name).await?.is_some() {
            return Err(Error::already_exists(name));
        }
        let id = self.id().child_id(node_id);
        let node = self
            .fs
            .persistence
            .create_symlink_node(id, Path::new(target), mtime)
            .await?;
        self.fs.persistence.store_node(&node).await?;
        self.dref.insert(name.to_string(), node).await?;
        Ok(())
    }

    /// Adopt a dynamic child under `name` with an explicit `node_id`, from its
    /// factory type and config.  `name` must be a direct child of this
    /// directory and must not already exist.
    ///
    /// `mtime` behaves as in [`insert_symlink_with_id`].
    ///
    /// [`insert_symlink_with_id`]: WD::insert_symlink_with_id
    pub async fn insert_dynamic_with_id(
        &self,
        name: &str,
        node_id: NodeID,
        factory_type: &str,
        config_content: Vec<u8>,
        mtime: Option<i64>,
    ) -> Result<()> {
        self.check_writable()?;
        if self.dref.get(name).await?.is_some() {
            return Err(Error::already_exists(name));
        }
        let id = self.id().child_id(node_id);
        let node = self
            .fs
            .create_dynamic_node(id, factory_type, config_content, mtime)
            .await?;
        self.dref.insert(name.to_string(), node).await?;
        Ok(())
    }
    /// If the directory already exists, returns Ok with a WD to that directory.
    /// Note: Parent directory references (..) in the path are not supported and will error.
    pub async fn create_dir_all<P: AsRef<Path>>(&self, path: P) -> Result<WD> {
        let path = path.as_ref();
        let mut current_wd = self.clone();

        // Split the path into components and create each one if it doesn't exist
        for component in path.components() {
            match component {
                Component::RootDir => {
                    // Skip root - we start from self which may or may not be root
                    continue;
                }
                Component::CurDir => {
                    // Skip current dir references
                    continue;
                }
                Component::ParentDir => {
                    // Parent dir references are not supported in create_dir_all
                    return Err(Error::Other(
                        "Parent directory references (..) not supported in create_dir_all"
                            .to_string(),
                    ));
                }
                Component::Prefix(_) => {
                    return Err(Error::prefix_not_supported(path));
                }
                Component::Normal(name) => {
                    let name_str = name.to_string_lossy().to_string();

                    // Try to get the entry - if it exists, navigate into it
                    let ddir = current_wd.dref.clone();
                    match ddir.get(&name_str).await {
                        Ok(Some(child)) => {
                            // Entry exists - make sure it's a directory and navigate into it
                            match &child.node.node_type {
                                NodeType::Directory(_) => {
                                    current_wd = current_wd
                                        .child_wd(&NodePath::new(
                                            child.node.clone(),
                                            ddir.path().join(&name_str),
                                        ))
                                        .await?;
                                }
                                _ => {
                                    return Err(Error::not_a_directory(path));
                                }
                            }
                        }
                        Ok(None) => {
                            // Entry doesn't exist - create the directory
                            current_wd.check_writable()?;
                            let id = FileID::new_physical_dir_id(
                                current_wd.effective_root.id().pond_id(),
                            );
                            let node = current_wd.fs.persistence.create_directory_node(id).await?;
                            current_wd.fs.persistence.store_node(&node).await?;
                            current_wd
                                .dref
                                .insert(name_str.clone(), node.clone())
                                .await?;
                            current_wd = current_wd
                                .child_wd(&NodePath::new(node, ddir.path().join(&name_str)))
                                .await?;
                        }
                        Err(e) => {
                            return Err(Error::Other(format!(
                                "Failed to access '{}' in path {}: {}",
                                name_str,
                                path.display(),
                                e
                            )));
                        }
                    }
                }
            }
        }

        Ok(current_wd)
    }

    /// Rename an entry in this directory.
    /// This is a compound operation: remove the old name + insert with new name.
    /// The underlying node's FileID and version history are preserved.
    ///
    /// # Arguments
    /// * `old_name` - Current name of the entry
    /// * `new_name` - New name for the entry
    ///
    /// # Returns
    /// * `Ok(())` if rename succeeded
    /// * `Err(NotFound)` if old_name doesn't exist
    /// * `Err(AlreadyExists)` if new_name already exists
    pub async fn rename_entry(&self, old_name: &str, new_name: &str) -> Result<()> {
        self.check_writable()?;
        // Check if new name already exists
        if self.dref.get(new_name).await?.is_some() {
            return Err(Error::already_exists(new_name));
        }

        // Remove old entry (returns the node)
        let node = self
            .dref
            .handle
            .remove(old_name)
            .await?
            .ok_or_else(|| Error::not_found(old_name))?;

        // Insert with new name
        self.dref.insert(new_name.to_string(), node).await?;

        debug!(
            "Renamed entry '{}' -> '{}' in directory",
            old_name, new_name
        );

        Ok(())
    }

    /// Remove an entry from this directory by name.
    ///
    /// Returns `Ok(())` if the entry was found and removed, `Err(NotFound)` otherwise.
    /// The removed node is dropped -- this is a destructive unlink.
    pub async fn remove_entry(&self, name: &str) -> Result<()> {
        self.check_writable()?;
        let _node = self
            .dref
            .handle
            .remove(name)
            .await?
            .ok_or_else(|| Error::not_found(name))?;

        debug!("Removed entry '{}' from directory", name);

        Ok(())
    }
    /// Get metadata for a file at the specified path
    pub async fn metadata_for_path<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<crate::metadata::NodeMetadata> {
        let (_, lookup) = self.resolve_path(path).await?;
        match lookup {
            Lookup::Found(node) => {
                let file_node = node.as_file().await?;
                file_node.metadata().await
            }
            Lookup::NotFound(full_path, _) => Err(Error::not_found(&full_path)),
            Lookup::Empty(_) => Err(Error::empty_path()),
        }
    }

    /// Get all versions of a file:series at the specified path
    pub async fn list_file_versions<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<Vec<crate::persistence::FileVersionInfo>> {
        let (_, lookup) = self.resolve_path(path).await?;
        match lookup {
            Lookup::Found(node) => self.fs.list_file_versions(node.id()).await,
            Lookup::NotFound(full_path, _) => Err(Error::not_found(&full_path)),
            Lookup::Empty(_) => Err(Error::empty_path()),
        }
    }

    /// Read a specific version of a file:series at the specified path
    pub async fn read_file_version<P: AsRef<Path>>(
        &self,
        path: P,
        version: u64,
    ) -> Result<Vec<u8>> {
        let (_, lookup) = self.resolve_path(path).await?;
        match lookup {
            Lookup::Found(node) => self.fs.read_file_version(node.id(), version).await,
            Lookup::NotFound(full_path, _) => Err(Error::not_found(&full_path)),
            Lookup::Empty(_) => Err(Error::empty_path()),
        }
    }

    /// Get an async reader for a file at the specified path (supports both streaming and seeking)
    pub async fn async_reader_path<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<Pin<Box<dyn crate::file::AsyncReadSeek>>> {
        self.async_reader_path_bounded(path, crate::file::SeriesReadBounds::NONE)
            .await
    }

    /// As [`WD::async_reader_path`], but passes [`crate::file::SeriesReadBounds`]
    /// to the file reader so append-only series nodes can prune versions
    /// (see [`crate::file::File::async_reader_bounded`]).
    pub async fn async_reader_path_bounded<P: AsRef<Path>>(
        &self,
        path: P,
        bounds: crate::file::SeriesReadBounds,
    ) -> Result<Pin<Box<dyn crate::file::AsyncReadSeek>>> {
        let (_, lookup) = self.resolve_path(path).await?;
        match lookup {
            Lookup::Found(node) => node.as_file().await?.async_reader_bounded(bounds).await,
            Lookup::NotFound(full_path, _) => Err(Error::not_found(&full_path)),
            Lookup::Empty(_) => Err(Error::empty_path()),
        }
    }

    /// Get an async writer for a file at the specified path (streaming)
    pub async fn async_writer_path<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<Pin<Box<dyn crate::file::FileMetadataWriter>>> {
        self.async_writer_path_with_type(path, EntryType::FilePhysicalVersion)
            .await
    }

    /// Get an async writer for a file at the specified path with specified entry type (streaming)
    pub async fn async_writer_path_with_type<P: AsRef<Path>>(
        &self,
        path: P,
        entry_type: EntryType,
    ) -> Result<Pin<Box<dyn crate::file::FileMetadataWriter>>> {
        let path_ref = path.as_ref();
        let (wd, lookup) = self.resolve_path(path_ref).await?;
        match lookup {
            Lookup::Found(node) => {
                wd.check_writable()?;
                log::debug!(
                    "async_writer_path_with_type: file exists at path '{}', returning existing file writer (no directory update)",
                    path_ref.display()
                );
                node.as_file().await?.async_writer().await
            }
            Lookup::NotFound(_, _) => {
                log::debug!(
                    "async_writer_path_with_type: file NOT found at path '{}', creating new file (WILL update directory)",
                    path_ref.display()
                );
                // File doesn't exist, create it with the specified entry type
                let (_, writer) = self
                    .create_file_path_streaming_with_type(path, entry_type)
                    .await?;
                Ok(writer)
            }
            Lookup::Empty(_) => Err(Error::empty_path()),
        }
    }

    /// Like [`WD::async_writer_path_with_type`], but the returned writer replaces
    /// every earlier version of an existing series node with a fresh baseline,
    /// stamping a `collapsed_through` sentinel. Used by content-addressed pull to
    /// replicate a source-side series compaction. The node must already exist; a
    /// missing node is created normally (nothing to collapse).
    pub async fn async_writer_path_collapsing_with_type<P: AsRef<Path>>(
        &self,
        path: P,
        entry_type: EntryType,
    ) -> Result<Pin<Box<dyn crate::file::FileMetadataWriter>>> {
        let path_ref = path.as_ref();
        let (wd, lookup) = self.resolve_path(path_ref).await?;
        match lookup {
            Lookup::Found(node) => {
                wd.check_writable()?;
                node.as_file().await?.async_writer_collapsing().await
            }
            Lookup::NotFound(_, _) => {
                let (_, writer) = self
                    .create_file_path_streaming_with_type(path, entry_type)
                    .await?;
                Ok(writer)
            }
            Lookup::Empty(_) => Err(Error::empty_path()),
        }
    }

    /// Opens a directory at the specified path and returns a new working directory for it
    pub async fn open_dir_path<P: AsRef<Path>>(&self, path: P) -> Result<WD> {
        let (resolved_wd, lookup) = self.resolve_path(path).await?;
        match lookup {
            Lookup::Found(node) => resolved_wd.child_wd(&node).await,
            Lookup::NotFound(full_path, _) => Err(Error::not_found(&full_path)),
            Lookup::Empty(node) => resolved_wd.child_wd(&node).await,
        }
    }

    /// Resolves a path and returns the working directory and lookup result
    pub async fn resolve_path<P: AsRef<Path>>(&self, path: P) -> Result<(WD, Lookup)> {
        let stack = vec![self.np.clone()];
        let (node, handle, detected_root) = self.resolve(&stack, path.as_ref(), 0).await?;
        let mut wd = self.child_wd(&node).await?;
        // If we crossed a pond boundary, update the effective root
        if let Some(mount_point) = detected_root {
            wd.effective_root = mount_point;
        }
        Ok((wd, handle))
    }

    /// Performs an operation on a path
    pub async fn in_path<F, Fut, P, T>(&self, path: P, op: F) -> Result<T>
    where
        F: FnOnce(WD, Lookup) -> Fut, // Pass owned WD instead of &WD
        Fut: std::future::Future<Output = Result<T>>,
        P: AsRef<Path>,
    {
        let (wd, lookup) = self.resolve_path(path).await?;
        op(wd, lookup).await // Pass owned wd instead of &wd
    }

    fn resolve<'a, P>(
        &'a self,
        stack_in: &'a [NodePath],
        path: P,
        depth: u32,
    ) -> Pin<Box<dyn Future<Output = Result<(NodePath, Lookup, Option<NodePath>)>> + Send + 'a>>
    where
        P: AsRef<Path> + Send + 'a,
    {
        Box::pin(async move {
            let path = path.as_ref();
            let path_debug = format!("{:?}", path);
            debug!(
                "resolve: starting with path = {path}, depth = {depth}",
                path = path_debug,
                depth = depth
            );
            let mut stack = stack_in.to_vec();
            let mut components = path.components().peekable();
            // Track if we cross a pond boundary during resolution.
            // When a child has a different pond_id than its parent,
            // the parent is the mount point / effective root.
            let mut detected_root: Option<NodePath> = None;

            // Iterate through the components of the path
            for comp in &mut components {
                match comp {
                    Component::Prefix(_) => {
                        return Err(Error::prefix_not_supported(path));
                    }
                    Component::RootDir => {
                        // Reset stack to effective root (chroot semantics)
                        stack.clear();
                        stack.push(self.effective_root.clone());
                        continue;
                    }
                    Component::CurDir => {
                        continue;
                    }
                    Component::ParentDir => {
                        if stack.len() <= 1 {
                            // At root boundary: stay put (chroot semantics)
                            continue;
                        }
                        // Check if popping would go above effective root
                        if stack.last().map(|n| n.id()) == Some(self.effective_root.id()) {
                            continue;
                        }
                        _ = stack.pop();
                    }
                    Component::Normal(name) => {
                        let dnode = stack.last().expect("stack not empty").clone();
                        let ddir = dnode.as_dir().await?;
                        let name = name.to_string_lossy().to_string();

                        let name_bound = &name;
                        debug!("resolve: Looking up name '{name_bound}' in directory");
                        let get_result = ddir.get(&name).await;

                        match get_result {
                            Err(dir_error) => {
                                // Directory access failed - this is ALWAYS an error, not NotFound!
                                // Examples: dynamic dir factory not registered, permission denied, etc.
                                // DO NOT convert to NotFound - that masks the real problem
                                let name_bound2 = &name;
                                debug!(
                                    "resolve: Directory access error for '{}': {}",
                                    name_bound2, dir_error
                                );
                                // Propagate the real error with context
                                return Err(Error::Other(format!(
                                    "Failed to access '{}' in path {}: {}",
                                    name,
                                    path.display(),
                                    dir_error
                                )));
                            }
                            Ok(None) => {
                                let name_bound2 = &name;
                                debug!("resolve: Name '{}' not found", name_bound2);
                                // This is OK in the last position
                                if components.peek().is_some() {
                                    return Err(Error::not_found(path));
                                } else {
                                    return Ok((
                                        dnode,
                                        Lookup::NotFound(path.to_path_buf(), name),
                                        detected_root,
                                    ));
                                }
                            }
                            Ok(Some(child)) => {
                                match &child.node.node_type {
                                    NodeType::Symlink(link) => {
                                        let target = link.readlink().await?;
                                        let (newsz, relp) =
                                            crate::path::normalize(&target, &stack)?;
                                        if depth >= SYMLINK_LOOP_LIMIT {
                                            return Err(Error::symlink_loop(target));
                                        }
                                        // Clamp stack at effective root boundary
                                        let min_sz = stack
                                            .iter()
                                            .position(|n| n.id() == self.effective_root.id())
                                            .map(|p| p + 1)
                                            .unwrap_or(1);
                                        let clamped = newsz.max(min_sz);
                                        let (_, handle, sub_detected) = self
                                            .resolve(&stack[0..clamped], relp, depth + 1)
                                            .await?;
                                        if detected_root.is_none() {
                                            detected_root = sub_detected;
                                        }
                                        match handle {
                                            Lookup::Found(node) => {
                                                stack.push(node);
                                            }
                                            Lookup::NotFound(_, _) => {
                                                return Err(Error::not_found(
                                                    link.readlink().await?,
                                                ));
                                            }
                                            Lookup::Empty(node) => {
                                                stack.push(node);
                                            }
                                        }
                                    }
                                    _ => {
                                        // File or Directory.
                                        // Detect pond boundary: if child has different
                                        // pond_id than parent, the parent is a mount point.
                                        if detected_root.is_none() {
                                            let parent = stack.last().expect("stack not empty");
                                            if child.id().pond_id() != parent.id().pond_id() {
                                                detected_root = Some(parent.clone());
                                            }
                                        }
                                        stack.push(child.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }

            let stack_len = stack.len();
            debug!(
                "resolve: End of component loop, stack.len() = {stack_len}",
                stack_len = stack_len
            );
            if stack.is_empty() {
                Err(Error::internal("empty resolve stack"))
            } else if stack.len() == 1 {
                let dir = stack.pop().expect("stack not empty");
                let is_root_path = path
                    .components()
                    .all(|c| matches!(c, Component::RootDir | Component::CurDir));
                if is_root_path {
                    debug!("resolve: Returning Found case (root path)");
                    Ok((dir.clone(), Lookup::Found(dir), detected_root))
                } else {
                    debug!("resolve: Returning Empty case");
                    Ok((dir.clone(), Lookup::Empty(dir), detected_root))
                }
            } else {
                debug!("resolve: Returning Found case");
                let found = stack.pop().expect("stack.len > 1");
                let dir = stack.pop().expect("stack.len > 1");
                Ok((dir, Lookup::Found(found), detected_root))
            }
        }) // Close the Box::pin(async move { block
    }

    /// Visits all filesystem entries matching the given wildcard pattern using a visitor
    pub async fn visit_with_visitor<P, V, T>(&self, pattern: P, visitor: &mut V) -> Result<Vec<T>>
    where
        P: AsRef<Path>,
        V: Visitor<T>,
        T: Send,
    {
        let pattern = if self.is_at_root_boundary() {
            crate::path::strip_root(pattern)
        } else {
            pattern.as_ref().to_path_buf()
        };
        let pattern_components: Vec<_> = parse_glob(pattern)?.collect();

        if pattern_components.is_empty() {
            return Err(Error::empty_path());
        }

        let mut visited = Vec::new();
        let mut captured = Vec::new();
        let mut stack = vec![self.np.clone()];
        let mut results = Vec::new();

        self.visit_recursive_with_visitor(
            &pattern_components,
            &mut visited,
            &mut captured,
            &mut stack,
            &mut results,
            visitor,
        )
        .await?;

        Ok(results)
    }

    /// Returns all matching entries as a simple collection
    pub async fn collect_matches<P: AsRef<Path>>(
        &self,
        pattern: P,
    ) -> Result<Vec<(NodePath, Vec<String>)>> {
        let mut visitor = CollectingVisitor::new();
        _ = self.visit_with_visitor(pattern, &mut visitor).await?;
        Ok(visitor.results)
    }

    fn visit_recursive_with_visitor<'a, V, T>(
        &'a self,
        pattern: &'a [WildcardComponent],
        visited: &'a mut Vec<HashSet<FileID>>,
        captured: &'a mut Vec<String>,
        stack: &'a mut Vec<NodePath>,
        results: &'a mut Vec<T>,
        visitor: &'a mut V,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>
    where
        V: Visitor<T>,
        T: Send,
    {
        Box::pin(async move {
            // @@@ Seems redundant
            _ = self.np.as_dir().await?;

            // Handle empty pattern case
            if pattern.is_empty() {
                return Ok(());
            }

            match &pattern[0] {
                WildcardComponent::Normal(name) => {
                    // Direct match with a literal name
                    if let Some(child) = self.dref.get(name).await? {
                        self.visit_match_with_visitor(
                            child, false, pattern, visited, captured, stack, results, visitor,
                        )
                        .await?;
                    }
                }
                WildcardComponent::Wildcard { .. } => {
                    let all_entries = self.get_entries().await?;
                    debug!(
                        "[SEARCH] Wildcard pattern: got {} total entries from directory {}",
                        all_entries.len(),
                        self.id()
                    );

                    // Filter by name pattern first (no node loading!)
                    let matching_entries: Vec<_> = all_entries
                        .into_iter()
                        .filter(|entry| pattern[0].match_component(&entry.name).is_some())
                        .collect();
                    debug!(
                        "[SEARCH] Wildcard pattern: {} entries matched name pattern",
                        matching_entries.len()
                    );

                    // Load matching nodes individually (handles mixed partitions)
                    for entry in matching_entries {
                        if let Some(wildcard_captures) = pattern[0].match_component(&entry.name)
                            && let Some(child) = self.dref.get(&entry.name).await?
                        {
                            debug!("[SEARCH] Wildcard pattern: loaded child '{}'", entry.name);
                            // Add all captures from this wildcard component
                            let captures_count = wildcard_captures.len();
                            captured.extend(wildcard_captures);
                            self.visit_match_with_visitor(
                                child, false, pattern, visited, captured, stack, results, visitor,
                            )
                            .await?;
                            // Remove all captures we added
                            for _ in 0..captures_count {
                                _ = captured.pop();
                            }
                        }
                    }
                }
                WildcardComponent::DoubleWildcard { .. } => {
                    // For DoubleWildcard, we need to handle two cases:
                    // 1. Match zero directories (current directory) - continue with next pattern component
                    // 2. Match one or more directories - recurse into each child with the same pattern

                    // Case 1: Match zero directories - try the next pattern component in current directory
                    if pattern.len() > 1 {
                        self.visit_recursive_with_visitor(
                            &pattern[1..],
                            visited,
                            captured,
                            stack,
                            results,
                            visitor,
                        )
                        .await?;
                    } else {
                        // Special case: If /** is the terminal component, match all files in current directory
                        // This handles patterns like "/**" and "dir/**" which should match all files recursively
                        let all_entries = self.get_entries().await?;
                        for entry in all_entries {
                            // Match files (not directories, those are handled below in Case 2)
                            if !entry.entry_type.is_directory()
                                && let Some(child) = self.dref.get(&entry.name).await?
                            {
                                captured.push(entry.name.clone());
                                self.visit_match_with_visitor(
                                    child, true, pattern, visited, captured, stack, results,
                                    visitor,
                                )
                                .await?;
                                _ = captured.pop();
                            }
                        }
                    }

                    // Case 2: Match one or more directories - recurse into child directories only
                    // Load directories individually as needed (avoid batch load across partitions)
                    let all_entries = self.get_entries().await?;

                    for entry in all_entries {
                        // Only recurse into directories
                        if entry.entry_type.is_directory() {
                            // Load this directory node individually
                            if let Some(child) = self.dref.get(&entry.name).await? {
                                captured.push(entry.name.clone());
                                self.visit_match_with_visitor(
                                    child, true, pattern, visited, captured, stack, results,
                                    visitor,
                                )
                                .await?;
                                _ = captured.pop();
                            }
                        }
                    }
                }
            };

            Ok(())
        })
    }

    async fn visit_match_with_visitor<V, T>(
        &self,
        child: NodePath,
        is_double: bool,
        pattern: &[WildcardComponent],
        visited: &mut Vec<HashSet<FileID>>,
        captured: &mut Vec<String>,
        stack: &mut Vec<NodePath>,
        results: &mut Vec<T>,
        visitor: &mut V,
    ) -> Result<()>
    where
        V: Visitor<T>,
        T: Send,
    {
        debug!(
            "[SEARCH] visit_match_with_visitor: child={}, pattern.len()={}",
            child.id(),
            pattern.len()
        );
        // Ensure the same node is does repeat the scan at the same
        // level in the pattern.
        if visited.len() <= pattern.len() {
            visited.resize(pattern.len() + 1, HashSet::default());
        }
        let set = visited.get_mut(pattern.len()).expect("checked above");
        let id = child.id();
        if set.get(&id).is_some() {
            return Ok(());
        }
        _ = set.insert(id);

        // If we're in the last position, visit the node.
        // For DoubleWildcard, we should also continue recursing into directories.
        let is_double_wildcard = matches!(pattern[0], WildcardComponent::DoubleWildcard { .. });
        if pattern.len() == 1 {
            let result = visitor.visit(child.clone(), captured).await?;
            results.push(result);

            // For DoubleWildcard patterns, we need to continue recursing even at the terminal position
            if !is_double_wildcard {
                return Ok(());
            }
            // Continue to the recursion logic below for DoubleWildcard
        }

        // If the component is a symlink, resolve it.
        let mut current = child.clone();
        if let Some(link) = child.into_symlink().await {
            let link_target = link.readlink().await?;
            let (_, handle, _) = self.resolve(stack, link_target, 0).await?;
            match handle {
                Lookup::Found(np) => current = np,
                Lookup::NotFound(fp, _) => return Err(Error::not_found(fp)),
                Lookup::Empty(np) => current = np,
            }
        }

        // If the component is a directory, recurse.
        if current.id().entry_type().is_directory() {
            // Prevent dynamic file expansion from recursing.
            self.fs.enter_node(&current).await?;
            // Ensure correct parent directory for resolve().
            stack.push(child.clone());

            // Recursive visit.
            let cd = self.child_wd(&current).await?;
            if is_double {
                // If **, there are two recursive branches.
                cd.visit_recursive_with_visitor(
                    pattern, visited, captured, stack, results, visitor,
                )
                .await?;
            }
            cd.visit_recursive_with_visitor(
                &pattern[1..],
                visited,
                captured,
                stack,
                results,
                visitor,
            )
            .await?;

            _ = stack.pop();
            self.fs.exit_node(&current).await;
        }

        Ok(())
    }

    /// Resolves a path for copy destination semantics
    /// - path/ (with trailing slash) means "copy INTO this directory"  
    /// - path (without trailing slash) could be file or directory
    ///
    /// TODO: Does this belong in tinyfs?
    pub async fn resolve_copy_destination<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<(WD, CopyDestination)> {
        let has_trailing_slash = Self::has_trailing_slash(&path);

        if has_trailing_slash {
            // Trailing slash means "must be a directory, copy INTO it"
            let clean_path = Self::strip_trailing_slash(&path);
            let (wd, lookup) = self.resolve_path(clean_path).await?;

            match lookup {
                Lookup::Found(found_node) => {
                    // This call fails if not a directory
                    let dest_wd = self.child_wd(&found_node).await?;
                    Ok((dest_wd, CopyDestination::Directory))
                }
                Lookup::NotFound(_, _) => Err(Error::not_found(&path)),
                Lookup::Empty(_) => {
                    // This shouldn't happen with clean path, but treat as directory
                    Ok((wd, CopyDestination::Directory))
                }
            }
        } else {
            // No trailing slash - ambiguous, check what exists
            let (wd, lookup) = self.resolve_path(&path).await?;

            match lookup {
                Lookup::Found(found_node) => {
                    if found_node.id().entry_type().is_directory() {
                        let dest_wd = self.child_wd(&found_node).await?;
                        Ok((dest_wd, CopyDestination::ExistingDirectory))
                    } else {
                        Ok((wd, CopyDestination::ExistingFile))
                    }
                }
                Lookup::NotFound(_, name) => {
                    // Destination doesn't exist - could create file or directory
                    Ok((wd, CopyDestination::NewPath(name)))
                }
                Lookup::Empty(_) => {
                    // Empty component - treat as directory
                    Ok((wd, CopyDestination::Directory))
                }
            }
        }
    }

    /// Create a dynamic directory node with factory type and configuration
    /// This exposes dynamic node creation through the TinyFS API
    pub async fn create_dynamic_path<P: AsRef<Path>>(
        &self,
        path: P,
        entry_type: EntryType,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<NodePath> {
        self.create_dynamic_path_with_overwrite(
            path,
            entry_type,
            factory_type,
            config_content,
            false,
        )
        .await
    }

    /// Read entire file content via path (convenience for tests/special cases)
    /// WARNING: Loads entire file into memory. Use async_reader_path() for streaming.
    pub async fn read_file_path_to_vec<P: AsRef<Path>>(&self, path: P) -> Result<Vec<u8>> {
        let reader = self.async_reader_path(path).await?;
        // Convert AsyncReadSeek to AsyncRead + Send
        let async_read: Pin<Box<dyn AsyncRead + Send>> = Box::pin(reader);
        crate::async_helpers::buffer_helpers::read_all_to_vec(async_read)
            .await
            .map_other_context("Failed to read file content")
    }

    /// Write entire buffer to file via path (convenience for tests/special cases)
    /// WARNING: Assumes entire content fits in memory. Use async_writer_path() for streaming.
    pub async fn write_file_path_from_slice<P: AsRef<Path>>(
        &self,
        path: P,
        content: &[u8],
    ) -> Result<()> {
        let mut writer = self.async_writer_path(path).await?;
        use tokio::io::AsyncWriteExt;
        writer
            .write_all(content)
            .await
            .map_other_context("Failed to write file content")?;
        writer
            .shutdown()
            .await
            .map_other_context("Failed to shutdown writer")?;
        Ok(())
    }

    /// Set extended attributes on an existing file
    /// This modifies the pending version of the file in the current transaction
    pub async fn set_extended_attributes<P: AsRef<Path>>(
        &self,
        path: P,
        attributes: HashMap<String, String>,
    ) -> Result<()> {
        // Resolve the path to get both the file's NodeID and its parent directory
        let (wd, lookup) = self.resolve_path(path).await?;

        match lookup {
            Lookup::Found(node_path) => {
                wd.check_writable()?;
                self.fs
                    .set_extended_attributes(node_path.id(), attributes)
                    .await
            }
            Lookup::NotFound(full_path, _) => Err(Error::not_found(&full_path)),
            Lookup::Empty(_) => Err(Error::empty_path()),
        }
    }

    /// Create a dynamic node with overwrite support
    pub async fn create_dynamic_path_with_overwrite<P: AsRef<Path>>(
        &self,
        path: P,
        entry_type: EntryType,
        factory_type: &str,
        config_content: Vec<u8>,
        overwrite: bool,
    ) -> Result<NodePath> {
        let path_clone = path.as_ref().to_path_buf();
        let node = self
            .in_path(path.as_ref(), |wd, entry| async move {
                wd.check_writable()?;
                match entry {
                    Lookup::Found(existing) if overwrite => {
                        // For overwrite, we need to update the configuration of the existing dynamic node
                        let existing_id = existing.id();

                        assert_eq!(existing_id.entry_type(), entry_type);

                        // Update the dynamic node configuration via FS layer
                        wd.fs
                            .update_dynamic_node_config(existing_id, factory_type, config_content)
                            .await?;

                        // Return the existing node path
                        Ok(existing)
                    }
                    Lookup::NotFound(_, name) => {
                        // For overwrite operations, we should try to recreate/overwrite anyway
                        // since the parent directory info is available in wd
                        let id = wd.id().new_child_id(entry_type);
                        let node = wd
                            .fs
                            .create_dynamic_node(id, factory_type, config_content, None)
                            .await?;

                        // Insert into parent directory with the given name
                        wd.dref.insert(name.clone(), node.clone()).await?;

                        Ok(NodePath::new(node, wd.dref.path().join(&name)))
                    }
                    Lookup::Found(_) => Err(Error::already_exists(&path_clone)),
                    Lookup::Empty(_) => Err(Error::empty_path()),
                }
            })
            .await?;

        Ok(node)
    }
}

impl std::fmt::Debug for WD {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WD{{path:{:?}}}", self.dref.path())
    }
}

impl PartialEq<WD> for WD {
    fn eq(&self, other: &WD) -> bool {
        self.id() == other.id()
    }
}

/// Trait for visiting filesystem nodes during glob traversal
#[async_trait]
pub trait Visitor<T>: Send {
    /// Called for each matching node with the node path and captured wildcard groups
    async fn visit(&mut self, node: NodePath, captured: &[String]) -> Result<T>;
}

/// Simple visitor that collects all matching nodes
pub struct CollectingVisitor {
    pub results: Vec<(NodePath, Vec<String>)>,
}

impl CollectingVisitor {
    pub fn new() -> Self {
        Self {
            results: Vec::new(),
        }
    }
}

#[async_trait]
impl Visitor<()> for CollectingVisitor {
    async fn visit(&mut self, node: NodePath, captured: &[String]) -> Result<()> {
        self.results.push((node, captured.to_vec()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Tests for working directory path resolution
    //!
    //! These tests focus on the `resolve_path` function, particularly edge cases
    //! around root directory resolution that have caused bugs in the past.

    use super::*;
    use crate::memory;

    #[tokio::test]
    async fn test_resolve_root_path_returns_found() {
        // Regression test for bug where resolving "/" returned Lookup::Empty instead of Lookup::Found
        // This bug caused "Parent directory not found: /" errors when creating nodes at root level
        let fs = memory::new_fs().await;
        let root = fs.root().await.expect("Failed to get root");

        // Test 1: Resolve "/" explicitly
        let (wd, lookup) = root.resolve_path("/").await.expect("Failed to resolve /");
        match lookup {
            Lookup::Found(node) => {
                assert!(node.id().has_root_ids(), "Should resolve to root node");
            }
            Lookup::Empty(_) => {
                panic!("Bug: resolve_path(\"/\") returned Empty instead of Found");
            }
            Lookup::NotFound(path, name) => {
                panic!(
                    "Bug: resolve_path(\"/\") returned NotFound({:?}, {})",
                    path, name
                );
            }
        }
        assert!(
            wd.node_path().id().has_root_ids(),
            "Working directory should be root"
        );

        // Test 2: Resolve "." from root (should also be Found)
        let (wd2, lookup2) = root.resolve_path(".").await.expect("Failed to resolve .");
        match lookup2 {
            Lookup::Found(node) => {
                assert!(node.id().has_root_ids(), "Should resolve to root node");
            }
            Lookup::Empty(_) => {
                panic!("Bug: resolve_path(\".\") returned Empty instead of Found");
            }
            Lookup::NotFound(path, name) => {
                panic!(
                    "Bug: resolve_path(\".\") returned NotFound({:?}, {})",
                    path, name
                );
            }
        }
        assert!(
            wd2.node_path().id().has_root_ids(),
            "Working directory should be root"
        );
    }

    #[tokio::test]
    async fn test_resolve_root_as_parent() {
        // Test that we can get the root's node ID when it's a parent directory
        // This is the use case that failed in mknod with paths like "/test_node"
        let fs = memory::new_fs().await;
        let root = fs.root().await.expect("Failed to get root");

        // Create a test directory at root level
        _ = root
            .create_dir_path("test_dir")
            .await
            .expect("Failed to create test_dir");

        // Now resolve the parent of "/test_dir" which should be "/"
        let parent_path = Path::new("/test_dir").parent().unwrap_or(Path::new("/"));

        let (parent_wd, parent_lookup) = root
            .resolve_path(parent_path)
            .await
            .expect("Failed to resolve parent");

        // The parent should be Found (the root directory)
        match parent_lookup {
            Lookup::Found(node) => {
                let node_id = node.id();
                let root_id = FileID::root();
                assert_eq!(node_id, root_id, "Parent node ID should be root ID");
            }
            Lookup::Empty(_) => {
                panic!("Bug: Parent path \"/\" returned Empty instead of Found");
            }
            Lookup::NotFound(path, name) => {
                panic!("Bug: Parent path returned NotFound({:?}, {})", path, name);
            }
        }
        assert!(
            parent_wd.node_path().id().has_root_ids(),
            "Parent working directory should be root"
        );
    }

    #[tokio::test]
    async fn test_resolve_nested_path_parent() {
        // Test that resolving a nested path's parent works correctly
        // This should exercise the normal case (stack.len() > 1)
        let fs = memory::new_fs().await;
        let root = fs.root().await.expect("Failed to get root");

        // Create nested structure: /a/b
        _ = root
            .create_dir_path("/a")
            .await
            .expect("Failed to create a");
        _ = root
            .create_dir_path("/a/b")
            .await
            .expect("Failed to create b");

        // Resolve parent of "/a/b" which is "/a"
        let parent_path = Path::new("/a/b").parent().unwrap();

        let (parent_wd, parent_lookup) = root
            .resolve_path(parent_path)
            .await
            .expect("Failed to resolve parent /a");

        // Should find the "a" directory
        match parent_lookup {
            Lookup::Found(_) => {
                // Expected case
            }
            Lookup::Empty(_) => {
                panic!("Bug: Nested parent path returned Empty instead of Found");
            }
            Lookup::NotFound(path, name) => {
                panic!("Bug: Nested parent returned NotFound({:?}, {})", path, name);
            }
        }

        // Verify we can use this to create a child
        _ = parent_wd
            .create_dir_path("c")
            .await
            .expect("Should be able to create /a/c");
    }
}
