// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Provider context and factory context for dynamic node creation
//!
//! This module defines the abstraction layer between factories and persistence implementations.
//! ProviderContext holds a tinyfs Persistence layer for transaction management.

use crate::error::ResultExt;
use crate::{FileID, PersistenceLayer};
use datafusion::execution::context::SessionContext;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Result type for tinyfs context operations
pub type Result<T> = std::result::Result<T, crate::Error>;

/// Hint published by an incremental reduction provider while building a node's
/// table provider, consumed by the sitegen export layer to skip rewriting
/// unchanged output partitions.
///
/// `digest` identifies the current merged output content. When it equals the
/// digest recorded in the seed manifest, the entire series output is unchanged
/// and every partition file can be reused. `changed_since` bounds which output
/// buckets changed when the digest differs: buckets with a timestamp strictly
/// below it are unchanged, so their partitions can be reused. `None` means the
/// output was fully rebuilt and every partition must be rewritten.
#[derive(Clone, Debug)]
pub struct ExportHint {
    pub digest: String,
    pub changed_since: Option<i64>,
}

/// Provider context - holds tinyfs Persistence for transaction management
///
/// This struct provides factories with:
/// - Direct access to DataFusion SessionContext for SQL execution
/// - Table provider caching for performance optimization
/// - tinyfs Persistence layer for creating transaction guards
///
/// All fields are concrete - no trait objects to downcast.
#[derive(Clone)]
pub struct ProviderContext {
    /// DataFusion session for SQL execution (direct access, no async needed)
    pub datafusion_session: Arc<SessionContext>,

    /// Table provider cache for performance
    pub table_provider_cache: Arc<
        std::sync::Mutex<
            std::collections::HashMap<String, Arc<dyn datafusion::catalog::TableProvider>>,
        >,
    >,

    /// TinyFS persistence layer for transaction management
    pub persistence: Arc<dyn PersistenceLayer>,

    /// Format provider cache directory ({POND}/cache/), if available.
    /// None for in-memory persistence (tests).
    pub cache_dir: Option<PathBuf>,

    /// Pond root directory ({POND}/), if available.
    /// None for in-memory persistence (tests).
    pub pond_path: Option<PathBuf>,

    /// Per-node export hints published by incremental reduction providers
    /// during `as_table_provider`, keyed by the node's `FileID` string. The
    /// export layer reads these immediately after obtaining the table provider
    /// to decide which output partitions are unchanged.
    pub export_hints: Arc<std::sync::Mutex<std::collections::HashMap<String, ExportHint>>>,
}

impl ProviderContext {
    /// Create a new provider context from concrete values
    pub fn new(
        datafusion_session: Arc<SessionContext>,
        persistence: Arc<dyn PersistenceLayer>,
    ) -> Self {
        Self {
            datafusion_session,
            table_provider_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            persistence,
            cache_dir: None,
            pond_path: None,
            export_hints: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Create a provider context with a format cache directory
    #[must_use]
    pub fn with_cache_dir(mut self, cache_dir: PathBuf) -> Self {
        self.cache_dir = Some(cache_dir);
        self
    }

    /// Create a provider context with the pond root directory
    #[must_use]
    pub fn with_pond_path(mut self, pond_path: PathBuf) -> Self {
        self.pond_path = Some(pond_path);
        self
    }

    /// Get cached TableProvider by cache key
    #[must_use]
    pub fn get_table_provider_cache(
        &self,
        key: &str,
    ) -> Option<Arc<dyn datafusion::catalog::TableProvider>> {
        self.table_provider_cache.lock().ok()?.get(key).cloned()
    }

    /// Set cached TableProvider
    pub fn set_table_provider_cache(
        &self,
        key: String,
        provider: Arc<dyn datafusion::catalog::TableProvider>,
    ) -> Result<()> {
        _ = self
            .table_provider_cache
            .lock()
            .map_other_context("Mutex poisoned")?
            .insert(key, provider);
        Ok(())
    }

    /// Publish an export hint for a node, keyed by its `FileID` string.
    pub fn set_export_hint(&self, id: &FileID, hint: ExportHint) -> Result<()> {
        _ = self
            .export_hints
            .lock()
            .map_other_context("export_hints mutex poisoned")?
            .insert(id.to_string(), hint);
        Ok(())
    }

    /// Read a previously published export hint for a node.
    #[must_use]
    pub fn get_export_hint(&self, id: &FileID) -> Option<ExportHint> {
        self.export_hints.lock().ok()?.get(&id.to_string()).cloned()
    }

    /// Create a filesystem from the persistence layer
    #[must_use]
    pub fn filesystem(&self) -> crate::FS {
        crate::FS::from_arc(self.persistence.clone())
    }

    /// Begin a transaction with the transaction guard pattern
    ///
    /// Returns a TransactionGuard that enforces the single-transaction rule.
    /// The guard provides access to the filesystem and automatically cleans up on drop.
    pub fn begin_transaction(&self) -> crate::Result<crate::TransactionGuard> {
        let fs = self.filesystem();
        let txn_state = self.persistence.transaction_state();
        txn_state.begin(fs, None)
    }

    /// Create a minimal test context with sensible defaults
    ///
    /// This is a convenience constructor for testing that creates a ProviderContext with:
    /// - Fresh DataFusion SessionContext (default configuration)
    /// - Empty template variables HashMap
    /// - Provided persistence layer
    ///
    /// This enables testing provider code without requiring tlogfs State.
    ///
    /// # Example
    /// ```ignore
    /// use tinyfs::{ProviderContext, MemoryPersistence};
    /// use std::sync::Arc;
    ///
    /// let persistence = Arc::new(MemoryPersistence::default());
    /// let context = ProviderContext::new_for_testing(persistence);
    /// ```
    pub fn new_for_testing(persistence: Arc<dyn PersistenceLayer>) -> Self {
        // Create default DataFusion session
        let datafusion_session = Arc::new(SessionContext::new());

        Self::new(datafusion_session, persistence)
    }

    /// Return the format provider cache directory, if available.
    #[must_use]
    pub fn cache_dir(&self) -> Option<&Path> {
        self.cache_dir.as_deref()
    }

    /// Get the pond root directory, if available
    #[must_use]
    pub fn pond_path(&self) -> Option<&Path> {
        self.pond_path.as_deref()
    }
}

/// Factory context for creating dynamic nodes
///
/// This struct provides the complete context needed by factories:
/// - Access to persistence layer via ProviderContext
/// - FileID providing node and partition identity
/// - Optional pond metadata (pond_id, birth_timestamp, etc.)
/// - Current transaction sequence number
/// - Effective root for path resolution scoping
#[derive(Clone)]
pub struct FactoryContext {
    /// Access to persistence layer operations
    pub context: ProviderContext,
    /// FileID for context-aware factories (provides both node and partition info)
    pub file_id: FileID,
    /// Pond identity metadata (pond_id, birth_timestamp, etc.)
    /// Provided by Steward when creating factory contexts
    pub pond_metadata: Option<PondMetadata>,
    /// Current transaction sequence number from persistence layer
    /// Provided by Steward for backup/replication operations
    pub txn_seq: i64,
    /// Persisted modification time for the dynamic node being materialized.
    ///
    /// Dynamic factory implementations use this for node metadata only; it
    /// is intentionally not inherited by virtual children they create.
    pub node_mtime: Option<i64>,
    /// Effective root for path resolution. When set, factories resolve
    /// absolute paths relative to this node rather than the global root.
    /// Used for cross-pond imports where foreign factories must resolve
    /// paths within their imported mount point.
    effective_root: Option<crate::node::NodePath>,
}

impl FactoryContext {
    /// Create a new factory context with the given provider context and file_id
    #[must_use]
    pub fn new(context: ProviderContext, file_id: FileID) -> Self {
        Self {
            context,
            file_id,
            pond_metadata: None,
            txn_seq: 0,
            node_mtime: None,
            effective_root: None,
        }
    }

    /// Create a factory context with pond metadata
    #[must_use]
    pub fn with_metadata(
        context: ProviderContext,
        file_id: FileID,
        pond_metadata: PondMetadata,
    ) -> Self {
        Self {
            context,
            file_id,
            pond_metadata: Some(pond_metadata),
            txn_seq: 0,
            node_mtime: None,
            effective_root: None,
        }
    }

    /// Set the transaction sequence number
    #[must_use]
    pub fn with_txn_seq(mut self, txn_seq: i64) -> Self {
        self.txn_seq = txn_seq;
        self
    }

    /// Set the persisted modification time for the dynamic node.
    #[must_use]
    pub fn with_node_mtime(mut self, node_mtime: i64) -> Self {
        self.node_mtime = Some(node_mtime);
        self
    }

    /// Set the effective root for path resolution scoping.
    /// When set, root() returns a WD chrooted to this node.
    #[must_use]
    pub fn with_effective_root(mut self, root: crate::node::NodePath) -> Self {
        self.effective_root = Some(root);
        self
    }

    /// Get the effective root, if set.
    #[must_use]
    pub fn effective_root(&self) -> Option<&crate::node::NodePath> {
        self.effective_root.as_ref()
    }

    /// Get the filesystem root for this factory context.
    ///
    /// If an effective root is set (e.g., for cross-pond imports), returns
    /// a WD chrooted to that mount point. Otherwise returns the global root.
    /// Factories should use this instead of context.filesystem().root().
    pub async fn root(&self) -> crate::Result<crate::WD> {
        let fs = self.context.filesystem();
        match &self.effective_root {
            Some(er) => {
                let wd = fs.wd(er, er.clone()).await?;
                Ok(wd)
            }
            None => fs.root().await,
        }
    }
}

/// Pond identity metadata - immutable information about the pond's origin
///
/// This metadata is created once when the pond is initialized and preserved across replicas.
/// It provides traceability and identity for distributed pond systems.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PondMetadata {
    /// Unique identifier for this pond (UUID v7)
    pub pond_id: uuid7::Uuid,
    /// Timestamp when this pond was originally created (microseconds since epoch)
    pub birth_timestamp: i64,
    /// Birthplace of the pond: a user-asserted, immutable label for where
    /// this pond was originally created (for example a hostname, site, or
    /// deployment name).  Set once at `pond init` and preserved across replicas.
    pub birthplace: String,
    /// Username who originally created the pond
    pub birth_username: String,
}

impl Default for PondMetadata {
    /// Create new pond metadata for a freshly initialized pond
    fn default() -> Self {
        let pond_id = uuid7::uuid7();
        let birth_timestamp = chrono::Utc::now().timestamp_micros();

        // Birthplace is user-asserted at `pond init`; the default is empty
        // and is overridden by the value the operator provides.
        let birthplace = String::new();

        let birth_username = std::env::var("USER")
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or("unknown".into());

        Self {
            pond_id,
            birth_timestamp,
            birthplace,
            birth_username,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ProviderContext;
    use crate::memory::persistence::MemoryPersistence;
    use datafusion::execution::context::SessionContext;
    use std::path::PathBuf;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_provider_context_transaction_guard() {
        // Create a provider context with memory persistence
        let persistence = MemoryPersistence::default();
        let session = Arc::new(SessionContext::new());
        let context = ProviderContext::new(session, Arc::new(persistence));

        // Begin a transaction using the guard pattern
        let guard = context
            .begin_transaction()
            .expect("Should create transaction");

        // Access filesystem through the guard
        let root = guard.root().await.expect("Should get root");

        // Verify root exists
        assert_eq!(root.node_path().path, PathBuf::from("/"));

        // Guard automatically cleans up on drop
        drop(guard);

        // Can create another transaction after the first one is dropped
        let _guard2 = context
            .begin_transaction()
            .expect("Should create second transaction");
    }

    #[tokio::test]
    async fn test_provider_context_filesystem() {
        // Create a provider context
        let persistence = MemoryPersistence::default();
        let session = Arc::new(SessionContext::new());
        let context = ProviderContext::new(session, Arc::new(persistence));

        // Get filesystem directly (no guard - for cases where guard isn't needed)
        let fs = context.filesystem();
        let root = fs.root().await.expect("Should get root");

        assert_eq!(root.node_path().path, PathBuf::from("/"));
    }

    #[tokio::test]
    async fn test_factory_context_root_without_effective_root() {
        // Without effective_root, FactoryContext::root() returns the global root.
        let persistence = MemoryPersistence::default();
        let session = Arc::new(SessionContext::new());
        let context = ProviderContext::new(session, Arc::new(persistence));

        let factory_ctx = super::FactoryContext::new(context, crate::FileID::root());
        let root = factory_ctx.root().await.expect("Should get root");
        assert!(root.effective_root().id().has_root_ids());
    }

    #[tokio::test]
    async fn test_factory_context_root_with_effective_root() {
        // With effective_root set, FactoryContext::root() returns a chrooted WD.
        let persistence = MemoryPersistence::default();
        let session = Arc::new(SessionContext::new());
        let context = ProviderContext::new(session, Arc::new(persistence));
        let fs = context.filesystem();
        let root = fs.root().await.expect("Should get root");

        // Create /sub/
        let sub = root.create_dir_path("sub").await.unwrap();
        let sub_np = sub.node_path();

        // Build factory context with effective_root = /sub/
        let factory_ctx = super::FactoryContext::new(context, crate::FileID::root())
            .with_effective_root(sub_np.clone());
        let chrooted = factory_ctx.root().await.expect("Should get chrooted root");

        // The returned WD should be at /sub/ with effective_root = /sub/
        assert_eq!(chrooted.node_path().id(), sub_np.id());
        assert_eq!(chrooted.effective_root().id(), sub_np.id());
        assert!(chrooted.is_at_root_boundary());
    }
}
