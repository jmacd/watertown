// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Dynamic directory factory for TinyFS

use crate::{FactoryContext, FactoryRegistry, register_dynamic_factory};
use async_trait::async_trait;
use futures::stream;
use log::{debug, error};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tinyfs::ResultExt;
use tinyfs::{
    DirHandle, Directory, EntryType, Metadata, Node, NodeMetadata, Result as TinyFSResult,
};

/// Configuration for a single directory entry
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DynamicDirEntry {
    /// Name of the entry in the directory
    pub name: String,
    /// Factory type to use for creating this entry
    pub factory: String,
    /// Configuration for the specific factory (as raw Value for flexibility)
    pub config: Value,
}

/// Configuration for the dynamic directory
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DynamicDirConfig {
    /// List of directory entries to create
    pub entries: Vec<DynamicDirEntry>,
}

/// Dynamic directory that creates entries using configured factories
pub struct DynamicDirDirectory {
    config: DynamicDirConfig,
    context: FactoryContext,
    /// Cache of created nodes to avoid recreating them on each access
    entry_cache: tokio::sync::RwLock<HashMap<String, Node>>,
}

impl DynamicDirDirectory {
    /// Create a new dynamic directory with the given configuration and context
    #[must_use]
    pub fn new(config: DynamicDirConfig, context: FactoryContext) -> Self {
        let entries_count = config.entries.len();
        debug!("DynamicDirDirectory::new - creating directory with {entries_count} entries");

        // Log each entry for debugging
        for entry in &config.entries {
            debug!(
                "DynamicDirDirectory::new - entry '{}' using factory '{}'",
                entry.name, entry.factory
            );
        }

        Self {
            config,
            context,
            entry_cache: tokio::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Create a DirHandle from this dynamic directory
    pub fn create_handle(self) -> DirHandle {
        DirHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }

    /// Create a FactoryContext for a child entry, preserving pond_metadata and txn_seq
    fn create_child_context(&self, child_file_id: tinyfs::FileID) -> FactoryContext {
        let ctx = if let Some(ref pm) = self.context.pond_metadata {
            FactoryContext::with_metadata(self.context.context.clone(), child_file_id, pm.clone())
        } else {
            FactoryContext::new(self.context.context.clone(), child_file_id)
        };
        let ctx = ctx.with_txn_seq(self.context.txn_seq);
        // Propagate effective_root so child factories resolve absolute
        // paths within the same scope (e.g., cross-pond import mount).
        if let Some(er) = self.context.effective_root() {
            ctx.with_effective_root(er.clone())
        } else {
            ctx
        }
    }

    /// Create a node for a specific entry using its configured factory
    async fn create_entry_node(&self, entry: &DynamicDirEntry) -> TinyFSResult<Node> {
        debug!(
            "DynamicDirDirectory::create_entry_node - creating entry '{}' with factory '{}'",
            entry.name, entry.factory
        );

        // Convert the configuration to JSON bytes for factory validation
        let config_bytes = serde_json::to_vec(&entry.config).map_other_context(format!(
            "Failed to serialize config for entry '{}'",
            entry.name
        ))?;

        // Deterministically generate FileID for entry node based on entry name, factory, and config.
        // This MUST be computed before creating the factory so the factory receives its own
        // identity, not the parent's. Without this, sibling entries in the same dynamic-dir
        // would share the parent's file_id, causing their children to collide.
        let mut id_bytes = Vec::new();
        id_bytes.extend_from_slice(entry.name.as_bytes());
        id_bytes.extend_from_slice(entry.factory.as_bytes());
        id_bytes.extend_from_slice(&config_bytes);
        let parent_part_id = tinyfs::PartID::from_node_id(self.context.file_id.node_id());

        // Determine whether this factory creates directories or files
        let creates_directory = FactoryRegistry::factory_creates_directory(&entry.factory)?;

        // Create the appropriate node type based on the factory's capabilities.
        // Each child gets a FactoryContext with its own file_id so that factories which
        // derive child partition IDs from context.file_id (e.g., temporal-reduce) produce
        // unique, non-colliding children.
        let (node_type, entry_type) = if creates_directory {
            let entry_type = EntryType::DirectoryDynamic;
            let child_file_id = tinyfs::FileID::from_content(
                parent_part_id,
                entry_type,
                &id_bytes,
                self.context.file_id.pond_id(),
            );
            let child_context = self.create_child_context(child_file_id);
            let dir_handle =
                FactoryRegistry::create_directory(&entry.factory, &config_bytes, child_context)?;
            debug!(
                "DynamicDirDirectory::create_entry_node - created directory for entry '{}'",
                entry.name
            );
            (tinyfs::NodeType::Directory(dir_handle), entry_type)
        } else {
            // For file entries, we need the created file's metadata to determine the exact
            // entry_type. Pre-compute a file_id with TableDynamic (the common case for file
            // factories like sql-derived-table/series), then verify after creation.
            let preliminary_type = EntryType::TableDynamic;
            let child_file_id = tinyfs::FileID::from_content(
                parent_part_id,
                preliminary_type,
                &id_bytes,
                self.context.file_id.pond_id(),
            );
            let child_context = self.create_child_context(child_file_id);
            let file_handle =
                FactoryRegistry::create_file(&entry.factory, &config_bytes, child_context).await?;
            // Query the file's metadata to get the correct EntryType
            let metadata = file_handle.metadata().await?;
            let actual_type = metadata.entry_type;
            if actual_type != preliminary_type {
                debug!(
                    "DynamicDirDirectory::create_entry_node - entry '{}' actual type {:?} differs from preliminary {:?}; file_id will use actual type",
                    entry.name, actual_type, preliminary_type
                );
            }
            debug!(
                "DynamicDirDirectory::create_entry_node - created file for entry '{}'",
                entry.name
            );
            (tinyfs::NodeType::File(file_handle), actual_type)
        };

        let file_id = tinyfs::FileID::from_content(
            parent_part_id,
            entry_type,
            &id_bytes,
            self.context.file_id.pond_id(),
        );
        let node_ref = Node::new(file_id, node_type);

        Ok(node_ref)
    }

    /// Get or create a cached entry node
    async fn get_entry_node(&self, entry_name: &str) -> TinyFSResult<Option<Node>> {
        // Check cache first
        {
            let cache = self.entry_cache.read().await;
            if let Some(node_ref) = cache.get(entry_name) {
                debug!(
                    "DynamicDirDirectory::get_entry_node - returning cached entry '{entry_name}'"
                );
                return Ok(Some(node_ref.clone()));
            }
        }

        // Find the entry configuration
        let entry = self.config.entries.iter().find(|e| e.name == entry_name);

        if let Some(entry) = entry {
            // Create the node
            let node_ref = self.create_entry_node(entry).await?;

            // Cache it
            {
                let mut cache = self.entry_cache.write().await;
                _ = cache.insert(entry_name.to_string(), node_ref.clone());
            }

            Ok(Some(node_ref))
        } else {
            debug!(
                "DynamicDirDirectory::get_entry_node - entry '{entry_name}' not found in configuration"
            );
            Ok(None)
        }
    }
}

#[async_trait]
impl Directory for DynamicDirDirectory {
    async fn get(&self, name: &str) -> tinyfs::Result<Option<Node>> {
        debug!("DynamicDirDirectory::get - looking for entry '{name}'");
        self.get_entry_node(name).await
    }

    async fn insert(&mut self, _name: String, _id: Node) -> tinyfs::Result<()> {
        debug!("DynamicDirDirectory::insert - mutation not permitted on dynamic directory");
        Err(tinyfs::Error::Other(
            "Dynamic directory is read-only".to_string(),
        ))
    }

    async fn remove(&mut self, _name: &str) -> tinyfs::Result<Option<Node>> {
        debug!("DynamicDirDirectory::remove - mutation not permitted on dynamic directory");
        Err(tinyfs::Error::Other(
            "Dynamic directory is read-only".to_string(),
        ))
    }

    async fn entries(
        &self,
    ) -> tinyfs::Result<
        std::pin::Pin<
            Box<dyn futures::Stream<Item = tinyfs::Result<tinyfs::DirectoryEntry>> + Send>,
        >,
    > {
        let entries_count = self.config.entries.len();
        debug!("DynamicDirDirectory::entries - listing {entries_count} configured entries");

        let mut results = Vec::new();

        for entry in &self.config.entries {
            // Dynamic entries need to be created/loaded to get their real node_id
            match self.get_entry_node(&entry.name).await {
                Ok(Some(node_ref)) => {
                    debug!(
                        "DynamicDirDirectory::entries - successfully created entry '{}'",
                        entry.name
                    );
                    // Extract node_id and entry_type from the created node's FileID
                    let dir_entry = tinyfs::DirectoryEntry::new(
                        entry.name.clone(),
                        node_ref.id().node_id(),
                        node_ref.id().entry_type(),
                        0, // No version for dynamic entries
                    );
                    results.push(Ok(dir_entry));
                }
                Ok(None) => {
                    let error_msg = format!("Entry '{}' not found in configuration", entry.name);
                    error!("DynamicDirDirectory::entries - {error_msg}");
                    results.push(Err(tinyfs::Error::Other(error_msg)));
                }
                Err(e) => {
                    let error_msg = format!("Failed to create entry '{}': {}", entry.name, e);
                    error!("DynamicDirDirectory::entries - {error_msg}");
                    results.push(Err(tinyfs::Error::Other(error_msg)));
                }
            }
        }

        let results_count = results.len();
        debug!("DynamicDirDirectory::entries - returning {results_count} entries");
        Ok(Box::pin(stream::iter(results)))
    }
}

#[async_trait]
impl Metadata for DynamicDirDirectory {
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: None,
            blake3: None,
            bao_outboard: None,
            entry_type: EntryType::DirectoryDynamic,
            timestamp: self.context.node_mtime.unwrap_or(0),
        })
    }
}

// Factory functions for the linkme registration system
fn create_dynamic_dir_handle(config: Value, context: FactoryContext) -> TinyFSResult<DirHandle> {
    let config: DynamicDirConfig =
        serde_json::from_value(config).map_other_context("Invalid dynamic directory config")?;

    // Note: Node caching is now handled automatically by CachingPersistence decorator
    // No need for manual caching here - the decorator will cache this node by FileID

    // Instrument: log parent_id, entry names, and config hash for debugging
    let parent_node_id = context.file_id.part_id();
    let entry_names: Vec<_> = config.entries.iter().map(|e| e.name.clone()).collect();
    let mut hasher = DefaultHasher::new();
    config.hash(&mut hasher);
    let config_hash = hasher.finish();
    debug!(
        "[INSTRUMENT] create_dynamic_dir_handle: parent_node_id={:?}, entry_names={:?}, config_hash={:x}",
        parent_node_id, entry_names, config_hash
    );

    // Create new instance
    // CachingPersistence will cache this automatically on create_dynamic_node()
    let dynamic_dir = DynamicDirDirectory::new(config, context.clone());
    let dir_handle = dynamic_dir.create_handle();

    debug!(
        "[INSTRUMENT] created dynamic directory for config_hash={:x} (will be cached by CachingPersistence)",
        config_hash
    );

    Ok(dir_handle)
}

fn validate_dynamic_dir_config(config: &[u8]) -> TinyFSResult<Value> {
    // Parse as YAML first (user format)
    let mut yaml_config: DynamicDirConfig =
        serde_yaml::from_slice(config).map_other_context("Invalid YAML config")?;

    // Validate that entries list is not empty
    if yaml_config.entries.is_empty() {
        return Err(tinyfs::Error::Other(
            "Entries list cannot be empty".to_string(),
        ));
    }

    // Validate each entry
    for (i, entry) in yaml_config.entries.iter_mut().enumerate() {
        // Check that name is not empty
        if entry.name.trim().is_empty() {
            return Err(tinyfs::Error::Other(format!("Entry {} has empty name", i)));
        }

        // Check that factory name is not empty
        if entry.factory.trim().is_empty() {
            return Err(tinyfs::Error::Other(format!(
                "Entry '{}' has empty factory name",
                entry.name
            )));
        }

        // Verify that the factory exists
        if FactoryRegistry::get_factory(&entry.factory).is_none() {
            return Err(tinyfs::Error::Other(format!(
                "Entry '{}' uses unknown factory '{}'",
                entry.name, entry.factory
            )));
        }

        // Serialize entry config as JSON bytes for nested factory validation
        let entry_config_bytes = serde_json::to_vec(&entry.config).map_other_context(format!(
            "Failed to serialize config for entry '{}'",
            entry.name
        ))?;

        // Validate the nested factory's configuration
        let validated_config =
            FactoryRegistry::validate_config(&entry.factory, &entry_config_bytes)?;

        // Update the entry's config with the validated version (normalized JSON)
        entry.config = validated_config;
    }

    // Return the full config as a JSON Value (after all nested validations)
    serde_json::to_value(&yaml_config).map_other_context("Failed to convert to JSON")
}

register_dynamic_factory!(
    name: "dynamic-dir",
    description: "Dynamic directory that creates entries from factory configurations",
    directory: create_dynamic_dir_handle,
    validate: validate_dynamic_dir_config
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dynamic_dir_config_serialization() {
        let config = DynamicDirConfig {
            entries: vec![
                DynamicDirEntry {
                    name: "file1".to_string(),
                    factory: "test-factory".to_string(),
                    config: serde_json::json!({"key": "value"}),
                },
                DynamicDirEntry {
                    name: "file2".to_string(),
                    factory: "another-factory".to_string(),
                    config: serde_json::json!({"number": 42}),
                },
            ],
        };

        // Serialize to JSON
        let json = serde_json::to_string(&config).unwrap();

        // Deserialize back
        let deserialized: DynamicDirConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_dynamic_dir_entry_hash() {
        let entry1 = DynamicDirEntry {
            name: "test".to_string(),
            factory: "factory1".to_string(),
            config: serde_json::json!({"key": "value"}),
        };

        let entry2 = DynamicDirEntry {
            name: "test".to_string(),
            factory: "factory1".to_string(),
            config: serde_json::json!({"key": "value"}),
        };

        let entry3 = DynamicDirEntry {
            name: "test".to_string(),
            factory: "factory2".to_string(), // Different factory
            config: serde_json::json!({"key": "value"}),
        };

        // Same entries should have same hash
        let mut hasher1 = DefaultHasher::new();
        entry1.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        entry2.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        assert_eq!(hash1, hash2);

        // Different factory should have different hash
        let mut hasher3 = DefaultHasher::new();
        entry3.hash(&mut hasher3);
        let hash3 = hasher3.finish();

        assert_ne!(hash1, hash3);
    }
}
