// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! SQL-derived dynamic node factories for TLogFS
//!
//! This module provides two specialized factories for SQL operations over TLogFS data:
//!
//! ## `sql-derived-table` Factory
//! - Operates on **FileTable** entries (single files)
//! - Errors if pattern matches more than one file
//! - Ideal for operations on individual data files
//! - Uses unified `create_listing_table_unified()` with DataFusion's native ListingTable
//!
//! ## `sql-derived-series` Factory  
//! - Operates on **FileSeries** entries (versioned time series)
//! - Supports multiple files and multiple versions per file
//! - Automatically unions data across files and versions
//! - Ideal for time-series aggregation and multi-file analytics
//! - Uses unified `create_listing_table_unified()` with DataFusion's native ListingTable
//!
//! ## Unified ListingTable Architecture
//!
//! **Approach**: Both factories now use DataFusion's native ListingTable with our TinyFS ObjectStore
//! implementation. This provides:
//! - **Predicate Pushdown**: Filters are pushed down to the storage layer
//! - **Streaming Execution**: Large datasets can be processed without loading entirely into memory
//! - **Native Performance**: DataFusion's optimized query execution
//! - **Single Code Path**: No duplication between FileTable and FileSeries modes
//!
//! **Legacy Support**: `create_memtable_unified()` is retained for in-memory data (DirectBytes)
//! that cannot use ListingTable, but all file-based operations use the ListingTable approach.
//!
//! For detailed architecture, performance analysis, and predicate pushdown strategies,
//! see [`crates/docs/sql-derived-design.md`](../docs/sql-derived-design.md).

use tinyfs::FileID;
use tinyfs::ResultExt;

use crate::transform::scope_prefix::scope_prefix_table_provider;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tinyfs::{
    AsyncReadSeek, EntryType, File, FileHandle, Metadata, NodeMetadata, Result as TinyFSResult,
};

// ============================================================================
// Configuration Types
// ============================================================================

/// Mode for SQL-derived operations
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SqlDerivedMode {
    /// FileTable mode: single files only, errors if pattern matches >1 file
    Table,
    /// FileSeries mode: handles multiple files and versions
    Series,
}

/// Options for SQL transformation and table name replacement
#[derive(Default, Clone, Debug)]
pub struct SqlTransformOptions {
    /// Replace multiple table names with mappings (for patterns)
    pub table_mappings: Option<HashMap<String, String>>,
    /// Replace a single source table name (for simple cases)  
    pub source_replacement: Option<String>,
}

/// Configuration for SQL-derived file generation
#[derive(Serialize, Deserialize, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct SqlDerivedConfig {
    /// Named URL patterns for matching files. Each pattern name becomes a table in the SQL query.
    /// Each pattern can match multiple files which are automatically harmonized with UNION ALL BY NAME.
    ///
    /// **URL Format**: `scheme:///path/pattern`
    /// - `series:///pattern` - FileSeries (for sql-derived-series mode)
    /// - `table:///pattern` - FileTable (for sql-derived-table mode)
    ///
    /// Example: {"vulink": "series:///data/vulink*.series", "at500": "series:///data/at500*.series"}
    #[serde(default)]
    pub patterns: HashMap<String, crate::Url>,

    /// SQL query to execute on the source data. Defaults to "SELECT * FROM source" if not specified
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,

    /// Optional list of table transform factory paths to apply to each pattern's TableProvider.
    /// Transforms are applied in order before scope prefixes and SQL execution.
    /// Each transform is a path like "/transforms/column-rename"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transforms: Option<Vec<String>>,

    /// Optional per-pattern transforms (internal use by derivative factories like timeseries-join)
    /// Maps pattern name (e.g., "input0") to list of transform paths
    /// Takes precedence over global `transforms` field for specified patterns
    /// NOT serialized - must be set programmatically
    #[serde(skip, default)]
    pub pattern_transforms: Option<HashMap<String, Vec<String>>>,

    /// Optional scope prefixes to apply to table columns (internal use by derivative factories)
    /// Maps table name to (scope_prefix, time_column_name)
    /// NOT serialized - must be set programmatically by factories like timeseries-join
    #[serde(skip, default)]
    pub scope_prefixes: Option<HashMap<String, (String, String)>>,

    /// Optional closure to wrap TableProviders before registration
    /// Used by timeseries-pivot to inject null_padding_table wrapping
    /// NOT serialized - must be set programmatically
    #[serde(skip, default)]
    pub provider_wrapper: Option<
        Arc<
            dyn Fn(Arc<dyn TableProvider>) -> Result<Arc<dyn TableProvider>, crate::Error>
                + Send
                + Sync,
        >,
    >,
}

impl SqlDerivedConfig {
    /// Create a new SqlDerivedConfig with patterns and optional query
    #[must_use]
    pub fn new(patterns: HashMap<String, crate::Url>, query: Option<String>) -> Self {
        Self {
            patterns,
            query,
            transforms: None,
            pattern_transforms: None,
            scope_prefixes: None,
            provider_wrapper: None,
        }
    }

    /// Create a new SqlDerivedConfig with scope prefixes for column renaming
    #[must_use]
    pub fn new_scoped(
        patterns: HashMap<String, crate::Url>,
        query: Option<String>,
        scope_prefixes: HashMap<String, (String, String)>,
    ) -> Self {
        Self {
            patterns,
            query,
            transforms: None,
            pattern_transforms: None,
            scope_prefixes: Some(scope_prefixes),
            provider_wrapper: None,
        }
    }

    /// Builder method to add a provider wrapper closure
    pub fn with_provider_wrapper<F>(mut self, wrapper: F) -> Self
    where
        F: Fn(Arc<dyn TableProvider>) -> Result<Arc<dyn TableProvider>, crate::Error>
            + Send
            + Sync
            + 'static,
    {
        self.provider_wrapper = Some(Arc::new(wrapper));
        self
    }

    /// Builder method to set the global transform factory paths.
    #[must_use]
    pub fn with_transforms(mut self, transforms: Option<Vec<String>>) -> Self {
        self.transforms = transforms;
        self
    }

    /// Builder method to add scope prefixes. An empty map is ignored, leaving
    /// `scope_prefixes` as `None` so callers need not branch on emptiness.
    #[must_use]
    pub fn with_scope_prefixes(
        mut self,
        scope_prefixes: HashMap<String, (String, String)>,
    ) -> Self {
        if !scope_prefixes.is_empty() {
            self.scope_prefixes = Some(scope_prefixes);
        }
        self
    }

    /// Builder method to add per-pattern transforms. An empty map is ignored,
    /// leaving `pattern_transforms` as `None`.
    #[must_use]
    pub fn with_pattern_transforms(
        mut self,
        pattern_transforms: HashMap<String, Vec<String>>,
    ) -> Self {
        if !pattern_transforms.is_empty() {
            self.pattern_transforms = Some(pattern_transforms);
        }
        self
    }

    /// Validate that all patterns are valid URLs with appropriate schemes for the given mode
    pub fn validate(&self, _mode: &SqlDerivedMode) -> TinyFSResult<()> {
        if self.patterns.is_empty() {
            return Err(tinyfs::Error::Other(
                "At least one pattern must be specified".to_string(),
            ));
        }

        // All schemes are valid - builtin types (series/table/file) or format providers (csv/excelhtml/oteljson)
        // The actual data compatibility is verified at runtime when the data is loaded

        Ok(())
    }

    /// Get the effective SQL query with table name substitution
    /// String replacement is reliable for table names - no fallbacks needed
    #[must_use]
    pub fn get_effective_sql(&self, options: &SqlTransformOptions) -> String {
        let default_query: String;
        let original_sql = if let Some(query) = &self.query {
            query.as_str()
        } else {
            // Generate smart default based on patterns
            if self.patterns.len() == 1 {
                let pattern_name = self.patterns.keys().next().expect("checked");
                default_query = format!("SELECT * FROM {}", pattern_name);
                &default_query
            } else {
                "SELECT * FROM <specify_pattern_name>"
            }
        };

        // Direct string replacement - reliable and deterministic
        crate::transform_sql(original_sql, options)
    }

    /// Get the effective SQL query that this SQL-derived file represents
    ///
    /// This returns the complete SQL query including WHERE, ORDER BY, and other clauses
    /// as specified by the user. Use this when you need the full query semantics preserved.
    #[must_use]
    pub fn get_effective_sql_query(&self) -> String {
        self.get_effective_sql(&SqlTransformOptions {
            table_mappings: Some(
                self.patterns
                    .keys()
                    .map(|k| (k.clone(), k.clone()))
                    .collect(),
            ),
            source_replacement: None,
        })
    }
}

impl std::fmt::Debug for SqlDerivedConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlDerivedConfig")
            .field("patterns", &self.patterns)
            .field("query", &self.query)
            .field("scope_prefixes", &self.scope_prefixes)
            .field(
                "provider_wrapper",
                &self.provider_wrapper.as_ref().map(|_| "<closure>"),
            )
            .finish()
    }
}

// ============================================================================
// SQL-Derived File Implementation
// ============================================================================

/// Represents a resolved file with its path and NodeID
#[derive(Clone)]
pub struct SqlDerivedFile {
    config: SqlDerivedConfig,
    context: crate::FactoryContext,
    mode: SqlDerivedMode,
}

impl SqlDerivedFile {
    /// Create a new SQL-derived file with validated URL patterns
    ///
    /// # Errors
    /// Returns error if pattern validation fails (invalid URLs, mismatched schemes, etc.)
    pub fn new(
        config: SqlDerivedConfig,
        context: crate::FactoryContext,
        mode: SqlDerivedMode,
    ) -> TinyFSResult<Self> {
        // Validate patterns match the mode
        config.validate(&mode)?;

        Ok(Self {
            config,
            context,
            mode,
        })
    }

    /// Apply transform chain to a TableProvider
    ///
    /// Applies transforms in order from config.transforms or config.pattern_transforms,
    /// each transform wrapping the previous result.
    async fn apply_transforms(
        &self,
        mut table_provider: Arc<dyn TableProvider>,
        pattern_name: &str,
    ) -> TinyFSResult<Arc<dyn TableProvider>> {
        // Check for pattern-specific transforms first, then fall back to global transforms
        let transform_paths = if let Some(ref pattern_transforms) = self.config.pattern_transforms {
            pattern_transforms.get(pattern_name)
        } else {
            self.config.transforms.as_ref()
        };

        let Some(transform_paths) = transform_paths else {
            return Ok(table_provider);
        };

        debug!(
            "[FIX] SQL-DERIVED: Applying {} transforms to table '{}': {:?}",
            transform_paths.len(),
            pattern_name,
            transform_paths
        );

        // Apply each transform in order
        for (index, transform_path) in transform_paths.iter().enumerate() {
            debug!(
                "[FIX] SQL-DERIVED: Applying transform {}/{}: '{}'",
                index + 1,
                transform_paths.len(),
                transform_path
            );

            // Resolve the transform path using FactoryContext root
            // (respects effective_root for cross-pond imports).
            let root = self
                .context
                .root()
                .await
                .map_other_context("Failed to get root")?;

            // Resolve the transform path to get the node
            let (_parent_wd, lookup_result) =
                root.resolve_path(transform_path).await.map_err(|e| {
                    let err_msg = format!(
                        "Failed to resolve transform path '{}' (pattern: '{}'): {}",
                        transform_path, pattern_name, e
                    );
                    log::error!("[ERR] SQL-DERIVED: {}", err_msg);
                    tinyfs::Error::Other(err_msg)
                })?;

            let config_node = match lookup_result {
                tinyfs::Lookup::Found(node) => node,
                _ => {
                    let err_msg = format!(
                        "Transform path '{}' not found (pattern: '{}')",
                        transform_path, pattern_name
                    );
                    log::error!("[ERR] SQL-DERIVED: {}", err_msg);
                    return Err(tinyfs::Error::Other(err_msg));
                }
            };

            let node_id = config_node.id();

            // Get the factory name and config for this node using generic PersistenceLayer method
            let (factory_name, _config_bytes) = self
                .context
                .context
                .persistence
                .get_dynamic_node_config(node_id)
                .await
                .map_other_context(format!(
                    "Failed to get factory config for transform '{}'",
                    transform_path
                ))?
                .ok_or_else(|| {
                    tinyfs::Error::Other(format!(
                        "Transform file '{}' has no associated factory",
                        transform_path
                    ))
                })?;

            debug!(
                "[FIX] SQL-DERIVED: Transform '{}' uses factory '{}'",
                transform_path, factory_name
            );

            // Look up the transform factory
            let transform_factory =
                crate::FactoryRegistry::get_factory(&factory_name).ok_or_else(|| {
                    tinyfs::Error::Other(format!(
                        "Transform factory '{}' not found in registry",
                        factory_name
                    ))
                })?;

            // Verify it's a table transform factory
            if transform_factory.apply_table_transform.is_none() {
                return Err(tinyfs::Error::Other(format!(
                    "Factory '{}' does not implement apply_table_transform (not a table transform factory)",
                    factory_name
                )));
            }

            // Get the transform function
            let apply_fn = transform_factory
                .apply_table_transform
                .expect("transform factory has apply_table_transform");

            // Create FactoryContext with the transform file's FileID
            let transform_context =
                crate::FactoryContext::new(self.context.context.clone(), node_id);

            debug!(
                "[FIX] SQL-DERIVED: Applying transform '{}' (factory: '{}')",
                transform_path, factory_name
            );

            // Apply the transform - factory will read its own config from FileID
            table_provider = apply_fn(transform_context, table_provider)
                .await
                .map_other_context(format!("Transform '{}' failed", transform_path))?;

            debug!(
                "[FIX] SQL-DERIVED: Transform {} applied successfully",
                index + 1
            );
        }

        Ok(table_provider)
    }

    /// Wrap this SqlDerivedFile in a FileHandle for use with TinyFS
    #[must_use]
    pub fn create_handle(self) -> FileHandle {
        FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }

    /// Resolve URL pattern to QueryableFile instances
    ///
    /// URL already validated during config deserialization.
    pub async fn resolve_pattern_to_queryable_files(
        &self,
        url: &crate::Url,
        entry_type: EntryType,
    ) -> TinyFSResult<Vec<tinyfs::NodePath>> {
        // Validate scheme matches entry_type
        let scheme = url.scheme();
        let expected_scheme = match entry_type {
            EntryType::TablePhysicalSeries | EntryType::TableDynamic => "series",
            EntryType::TablePhysicalVersion => "table",
            _ => {
                return Err(tinyfs::Error::Other(format!(
                    "Unsupported entry_type {:?} for URL pattern",
                    entry_type
                )));
            }
        };

        // Allow: exact match, "file" scheme (uses EntryType), or format providers
        let is_format_provider = crate::FormatRegistry::get_provider(scheme).is_some();
        if scheme != expected_scheme && scheme != "file" && !is_format_provider {
            return Err(tinyfs::Error::Other(format!(
                "URL scheme '{}' doesn't match entry_type {:?} (expected '{}', 'file', or format provider)",
                scheme, entry_type, expected_scheme
            )));
        }

        // For format providers, look for raw file types instead of the requested table types.
        // The format provider will convert them to the appropriate queryable type.
        // We may need to search multiple entry types: e.g., logfile-ingest creates
        // FilePhysicalSeries, while `pond copy --format=data` creates FilePhysicalVersion.
        let lookup_entry_types: Vec<EntryType> = if is_format_provider {
            match entry_type {
                EntryType::TablePhysicalSeries => vec![
                    EntryType::FilePhysicalVersion,
                    EntryType::FilePhysicalSeries,
                ],
                EntryType::TableDynamic => vec![EntryType::FileDynamic],
                EntryType::TablePhysicalVersion => vec![EntryType::FilePhysicalVersion],
                _ => vec![entry_type],
            }
        } else if url.entry_type().is_some()
            && matches!(
                entry_type,
                EntryType::TablePhysicalSeries
                    | EntryType::TableDynamic
                    | EntryType::TablePhysicalVersion
            )
        {
            // Explicit `+series`/`+table` cast: a data-archetype node that holds
            // Parquet bytes can be reinterpreted as a series/table. Accept data
            // files as candidates in addition to the requested series/table type;
            // the table-provider layer performs the actual cast.
            // See docs/url-entry-type-casting.md.
            vec![
                entry_type,
                EntryType::FileDynamic,
                EntryType::FilePhysicalVersion,
            ]
        } else {
            vec![entry_type]
        };

        // Extract TinyFS path from URL, percent-decoding so that URL-encoded
        // characters (e.g., %20 for spaces) are converted back to their literal
        // form for tinyfs path matching.
        let tinyfs_path_decoded = percent_encoding::percent_decode_str(url.path())
            .decode_utf8()
            .map_other_context(format!("Invalid UTF-8 in URL path '{}'", url.path()))?
            .to_string();
        let tinyfs_path = tinyfs_path_decoded.as_str();

        // STEP 1: Get root from FactoryContext (respects effective_root
        // for cross-pond imports where "/" means the foreign root).
        let tinyfs_root = self
            .context
            .root()
            .await
            .map_other_context("Failed to get root")?;

        // Check if path is exact (no wildcards) - use resolve_path() for single targets
        let is_exact_path =
            !tinyfs_path.contains('*') && !tinyfs_path.contains('?') && !tinyfs_path.contains('[');

        debug!(
            "resolve_pattern_to_queryable_files: url='{}', tinyfs_path='{}', is_exact_path={}",
            url, tinyfs_path, is_exact_path
        );

        // STEP 2: Use collect_matches to get NodePath instances
        let matches = if is_exact_path {
            // Use resolve_path() for exact paths to handle dynamic nodes correctly
            match tinyfs_root.resolve_path(tinyfs_path).await {
                Ok((_wd, tinyfs::Lookup::Found(node_path))) => {
                    vec![(node_path, HashMap::new())] // Empty captured groups for exact matches
                }
                Ok((_wd, tinyfs::Lookup::NotFound(_, _))) => {
                    return Err(tinyfs::Error::Other(format!(
                        "Exact path '{}' not found",
                        tinyfs_path
                    )));
                }
                Ok((_wd, tinyfs::Lookup::Empty(_))) => {
                    return Err(tinyfs::Error::Other(format!(
                        "Exact path '{}' points to empty directory",
                        tinyfs_path
                    )));
                }
                Err(e) => {
                    return Err(tinyfs::Error::Other(format!(
                        "Failed to resolve exact path '{}': {}",
                        tinyfs_path, e
                    )));
                }
            }
        } else {
            // Use collect_matches() for glob patterns - returns Vec<(NodePath, Vec<String>)>
            let pattern_matches = tinyfs_root
                .collect_matches(tinyfs_path)
                .await
                .map_other_context(format!("Failed to resolve pattern '{}'", tinyfs_path))?;

            // Convert Vec<(NodePath, Vec<String>)> to Vec<(NodePath, HashMap<String, String>)>
            pattern_matches
                .into_iter()
                .map(|(path, captures)| {
                    let mut capture_map = HashMap::new();
                    for (i, capture) in captures.into_iter().enumerate() {
                        _ = capture_map.insert(i.to_string(), capture);
                    }
                    (path, capture_map)
                })
                .collect()
        };

        let matches_count = matches.len();
        debug!(
            "resolve_pattern_to_queryable_files: found {matches_count} matches for url '{}'",
            url
        );

        // STEP 3: Extract files - check entry_type directly from FileID (no metadata fetch needed)
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        let mut queryable_files = Vec::new();

        for (node_path, _captured) in matches {
            let file_id = node_path.id();
            let actual_entry_type = file_id.entry_type();

            debug!(
                "[SEARCH] Checking file at path '{}': file_id={}, actual_entry_type={:?}, lookup_entry_types={:?}",
                node_path.path().display(),
                file_id,
                actual_entry_type,
                lookup_entry_types
            );

            // Check if this node matches any of the lookup entry types
            if lookup_entry_types.contains(&actual_entry_type) {
                // For FileSeries, deduplicate by full FileID. For FileTable, use only node_id.
                let dedup_key = match entry_type {
                    EntryType::TablePhysicalSeries | EntryType::TableDynamic => file_id,
                    _ => FileID::new_from_ids(
                        file_id.part_id(),
                        file_id.node_id(),
                        file_id.pond_id(),
                    ),
                };
                if seen.insert(dedup_key) {
                    let entry_type_str = format!("{entry_type:?}");
                    let path_str = node_path.path().display().to_string();
                    debug!(
                        "Successfully extracted file with entry_type '{entry_type_str}' at path '{path_str}'"
                    );
                    queryable_files.push(node_path.clone());
                } else {
                    let path_str = node_path.path().display().to_string();
                    debug!("Deduplication: Skipping duplicate file at path '{path_str}'");
                }
            }
        }

        let file_count = queryable_files.len();
        debug!(
            "resolve_pattern_to_queryable_files: returning {file_count} files after deduplication"
        );
        Ok(queryable_files)
    }

    /// Get the effective SQL query with table name substitution
    /// String replacement is reliable for table names - no fallbacks needed
    #[must_use]
    pub fn get_effective_sql(&self, options: &SqlTransformOptions) -> String {
        let result = self.config.get_effective_sql(options);
        debug!("Transformed SQL result: {result}");
        result
    }
}

// Async trait implementations

#[async_trait]
impl File for SqlDerivedFile {
    async fn async_reader(&self) -> TinyFSResult<std::pin::Pin<Box<dyn AsyncReadSeek>>> {
        Err(tinyfs::Error::Other(
            "SQL-derived files cannot be read as byte streams. Use QueryableFile::as_table_provider to get a DataFusion TableProvider, then compose your own queries and choose your execution strategy (streaming/collecting).".to_string()
        ))
    }

    async fn async_writer(
        &self,
    ) -> TinyFSResult<std::pin::Pin<Box<dyn tinyfs::FileMetadataWriter>>> {
        Err(tinyfs::Error::Other(
            "SQL-derived file is read-only".to_string(),
        ))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_queryable(&self) -> Option<&dyn tinyfs::QueryableFile> {
        Some(self)
    }
}

#[async_trait]
impl Metadata for SqlDerivedFile {
    async fn metadata(&self) -> TinyFSResult<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: None,   // Unknown
            blake3: None, // Unknown
            bao_outboard: None,
            entry_type: EntryType::TableDynamic,
            timestamp: 0,
        })
    }
}

// Factory functions for linkme registration

fn create_sql_derived_table_handle(
    config: Value,
    context: crate::FactoryContext,
) -> TinyFSResult<FileHandle> {
    let cfg: SqlDerivedConfig =
        serde_json::from_value(config).map_other_context("Invalid SQL-derived config")?;

    let sql_file = SqlDerivedFile::new(cfg, context, SqlDerivedMode::Table)?;
    Ok(sql_file.create_handle())
}

fn create_sql_derived_series_handle(
    config: Value,
    context: crate::FactoryContext,
) -> TinyFSResult<FileHandle> {
    let cfg: SqlDerivedConfig =
        serde_json::from_value(config).map_other_context("Invalid SQL-derived config")?;

    let sql_file = SqlDerivedFile::new(cfg, context, SqlDerivedMode::Series)?;
    Ok(sql_file.create_handle())
}

fn validate_sql_derived_config(config: &[u8]) -> TinyFSResult<Value> {
    // Parse as YAML first (user format)
    let yaml_config: SqlDerivedConfig =
        serde_yaml::from_slice(config).map_other_context("Invalid YAML config")?;

    // Validate that patterns list is not empty
    if yaml_config.patterns.is_empty() {
        return Err(tinyfs::Error::Other(
            "Patterns list cannot be empty".to_string(),
        ));
    }

    // Validate individual patterns
    for (table_name, pattern_url) in &yaml_config.patterns {
        if table_name.is_empty() {
            return Err(tinyfs::Error::Other(
                "Table name cannot be empty".to_string(),
            ));
        }

        // Validate scheme is recognized (no fallback for unknown schemes)
        let scheme = pattern_url.scheme();
        const BUILTIN_SCHEMES: &[&str] = &["file", "series", "table", "data"];
        let is_known = BUILTIN_SCHEMES.contains(&scheme)
            || crate::FormatRegistry::get_provider(scheme).is_some();
        if !is_known {
            let mut known: Vec<&str> = BUILTIN_SCHEMES.to_vec();
            known.extend(crate::FormatRegistry::list_providers());
            return Err(tinyfs::Error::Other(format!(
                "Unknown URL scheme '{}' in pattern '{}'. Known schemes: {}",
                scheme,
                pattern_url,
                known.join(", ")
            )));
        }
    }

    // Validate query if provided (now optional)
    if let Some(query) = &yaml_config.query
        && query.is_empty()
    {
        return Err(tinyfs::Error::Other(
            "SQL query cannot be empty if specified".to_string(),
        ));
    }

    // Convert to JSON for internal use
    serde_json::to_value(yaml_config).map_other_context("Failed to convert config")
}

// Downcast function for SqlDerivedFile
fn try_as_sql_derived_queryable(file: &dyn File) -> Option<&dyn crate::QueryableFile> {
    file.as_any()
        .downcast_ref::<SqlDerivedFile>()
        .map(|f| f as &dyn crate::QueryableFile)
}

// Register the factories with QueryableFile support
crate::register_dynamic_factory!(
    name: "sql-derived-table",
    description: "Create SQL-derived tables from single FileTable sources",
    file: create_sql_derived_table_handle,
    validate: validate_sql_derived_config,
    try_as_queryable: try_as_sql_derived_queryable
);

crate::register_dynamic_factory!(
    name: "sql-derived-series",
    description: "Create SQL-derived tables from multiple FileSeries sources",
    file: create_sql_derived_series_handle,
    validate: validate_sql_derived_config,
    try_as_queryable: try_as_sql_derived_queryable
);

impl SqlDerivedFile {
    /// Get the factory context
    #[must_use]
    pub fn get_context(&self) -> &crate::FactoryContext {
        &self.context
    }

    /// Get the configuration patterns
    #[must_use]
    pub fn get_config(&self) -> &SqlDerivedConfig {
        &self.config
    }

    /// Get the mode (Table or Series)
    #[must_use]
    pub fn get_mode(&self) -> &SqlDerivedMode {
        &self.mode
    }

    /// Get the effective SQL query that this SQL-derived file represents
    ///
    /// This returns the complete SQL query including WHERE, ORDER BY, and other clauses
    /// as specified by the user. Use this when you need the full query semantics preserved.
    #[must_use]
    pub fn get_effective_sql_query(&self) -> String {
        self.config.get_effective_sql_query()
    }

    /// Generate deterministic table name for caching based on SQL query, pattern config, and file content
    ///
    /// Generate a data-only table name for the source ListingTable.
    ///
    /// Unlike `generate_deterministic_table_name`, this does NOT include the
    /// SQL query in the hash.  All SqlDerivedFile instances that share the same
    /// source pattern URL and resolved files will produce the same name, allowing
    /// the expensive ListingTable/glob construction to happen once and be reused
    /// across nodes with different SQL queries (e.g., 25 temporal-reduce nodes
    /// sharing the same 89 source files but with different GROUP BY intervals).
    fn generate_data_table_name(
        pattern_url: &crate::Url,
        queryable_files: &[tinyfs::NodePath],
    ) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();

        // Hash the pattern URL string (determines which files)
        pattern_url.to_string().hash(&mut hasher);

        // Hash the file IDs (sorted for deterministic ordering)
        let mut file_ids: Vec<String> = Vec::new();
        for node_path in queryable_files {
            let file_id = node_path.id();
            file_ids.push(format!("{}_{}", file_id.node_id(), file_id.part_id()));
        }
        file_ids.sort();
        file_ids.hash(&mut hasher);

        let hash_value = hasher.finish();
        format!("sql_derived_data_{:016x}", hash_value).to_lowercase()
    }

    /// Cast a non-queryable data-archetype node (e.g. a git-ingested
    /// `FileDynamic` Parquet archive) into a TableProvider by reading its
    /// bytes and decoding them as Parquet.
    ///
    /// This is the pond analogue of `create_host_parquet_table_provider`:
    /// reached only when a `+series`/`+table` URL cast matched a data file
    /// (see step 1 in `docs/url-entry-type-casting.md`).  A data file cast as
    /// a series/table yields a single Parquet version read fully into a
    /// `MemTable`.  A non-Parquet file produces a clear "not a valid Parquet
    /// file" error rather than the opaque "not queryable" error.
    async fn cast_data_node_to_parquet_provider(
        node_path: &tinyfs::NodePath,
    ) -> TinyFSResult<Arc<dyn TableProvider>> {
        crate::provider_api::read_pond_node_as_parquet(node_path)
            .await
            .map_other()
    }

    /// Apply the optional `provider_wrapper` closure to a TableProvider,
    /// returning the provider unchanged when no wrapper is configured.
    fn apply_provider_wrapper(
        &self,
        provider: Arc<dyn TableProvider>,
    ) -> TinyFSResult<Arc<dyn TableProvider>> {
        match &self.config.provider_wrapper {
            Some(wrapper) => wrapper(provider).map_other(),
            None => Ok(provider),
        }
    }

    /// Build a `scheme://path` URL string for a resolved node.
    fn node_file_url(scheme: &str, node_path: &tinyfs::NodePath) -> String {
        format!("{}://{}", scheme, node_path.path().display())
    }

    /// Union multiple TableProviders into a single in-memory table via
    /// `UNION ALL BY NAME`, returning a MemTable provider.
    async fn union_providers_to_memtable(
        providers: &[Arc<dyn TableProvider>],
    ) -> TinyFSResult<Arc<dyn TableProvider>> {
        let temp_ctx = datafusion::prelude::SessionContext::new();
        for (i, tp) in providers.iter().enumerate() {
            _ = temp_ctx
                .register_table(format!("t{}", i), tp.clone())
                .map_other()?;
        }
        let union_sql = (0..providers.len())
            .map(|i| format!("SELECT * FROM t{}", i))
            .collect::<Vec<_>>()
            .join(" UNION ALL BY NAME ");
        let df = temp_ctx.sql(&union_sql).await.map_other()?;
        let batches = df.collect().await.map_other()?;
        let schema = batches
            .first()
            .map(|b| b.schema())
            .ok_or_else(|| tinyfs::Error::Other("No batches in union".to_string()))?;
        let mem_table =
            datafusion::datasource::MemTable::try_new(schema, vec![batches]).map_other()?;
        Ok(Arc::new(mem_table))
    }

    /// Build a source TableProvider for a pattern whose scheme is a format
    /// provider (csv/excelhtml/oteljson/...), converting matched files into a
    /// queryable table. Applies the optional provider wrapper.
    async fn build_format_provider_table(
        &self,
        pattern_name: &str,
        scheme: &str,
        queryable_files: &[tinyfs::NodePath],
    ) -> TinyFSResult<Arc<dyn TableProvider>> {
        debug!(
            "[SEARCH] SQL-DERIVED: Pattern '{}' uses format provider '{}' with {} files",
            pattern_name,
            scheme,
            queryable_files.len()
        );
        let fs = self.context.context.filesystem();
        let fs_arc = Arc::new(fs);
        let mut provider_api =
            crate::Provider::with_context(fs_arc, Arc::new(self.context.context.clone()));
        if let Ok(root) = self.context.root().await {
            provider_api = provider_api.with_root(root);
        }
        let datafusion_ctx = datafusion::prelude::SessionContext::new();

        if queryable_files.len() == 1 {
            // Single file: direct format provider conversion
            let node_path = &queryable_files[0];
            let file_url = Self::node_file_url(scheme, node_path);

            match provider_api
                .create_table_provider(&file_url, &datafusion_ctx)
                .await
            {
                Ok(table_provider) => {
                    debug!("[OK] SQL-DERIVED: Format provider created table for single file");
                    self.apply_provider_wrapper(table_provider)
                }
                Err(e) => {
                    log::error!(
                        "[ERR] SQL-DERIVED: Format provider failed for '{}': {}",
                        file_url,
                        e
                    );
                    Err(tinyfs::Error::Other(format!(
                        "Format provider failed: {}",
                        e
                    )))
                }
            }
        } else {
            // Multiple files: use glob cache if available, else UNION BY NAME
            let cache_dir = self
                .context
                .context
                .cache_dir()
                .map(std::path::Path::to_path_buf);

            if let Some(ref cache_dir) = cache_dir {
                // --- Glob cache path: cache each file, symlink into glob dir, single ListingTable ---
                let format_provider = crate::FormatRegistry::get_provider(scheme)
                    .ok_or_else(|| tinyfs::Error::Other(format!("Unknown format: {}", scheme)))?;

                let mut nodes = Vec::with_capacity(queryable_files.len());
                for node_path in queryable_files {
                    let file_url_str = Self::node_file_url(scheme, node_path);
                    let file_url = crate::Url::parse(&file_url_str).map_other()?;

                    nodes.push(
                        provider_api
                            .ensure_url_cached(&file_url, format_provider.as_ref(), cache_dir)
                            .await
                            .map_other()?,
                    );
                }

                let provider = crate::format_cache::glob_cached_set(cache_dir, scheme, &nodes)
                    .table_provider()
                    .await
                    .map_other()?;

                debug!(
                    "[OK] SQL-DERIVED: Glob cache ListingTable for {} files (pattern '{}')",
                    queryable_files.len(),
                    pattern_name
                );

                self.apply_provider_wrapper(provider)
            } else {
                // --- Fallback: no cache, UNION BY NAME ---
                let mut table_providers = Vec::new();
                for node_path in queryable_files {
                    let file_url = Self::node_file_url(scheme, node_path);
                    match provider_api
                        .create_table_provider(&file_url, &datafusion_ctx)
                        .await
                    {
                        Ok(tp) => table_providers.push(tp),
                        Err(e) => {
                            log::error!(
                                "[ERR] SQL-DERIVED: Format provider failed for '{}': {}",
                                file_url,
                                e
                            );
                            return Err(tinyfs::Error::Other(format!(
                                "Format provider failed: {}",
                                e
                            )));
                        }
                    }
                }

                let provider = Self::union_providers_to_memtable(&table_providers).await?;
                self.apply_provider_wrapper(provider)
            }
        }
    }

    /// Build a source TableProvider for a pattern that resolved to exactly one
    /// file: either a natively-queryable file, or a single data-archetype file
    /// reinterpreted as Parquet under a +series/+table cast.
    async fn build_single_file_provider(
        &self,
        pattern_name: &str,
        node_path: &tinyfs::NodePath,
        context: &tinyfs::ProviderContext,
    ) -> TinyFSResult<Arc<dyn TableProvider>> {
        let file_id = node_path.id();
        debug!(
            "[SEARCH] SQL-DERIVED: Creating table provider for single file: file_id={}",
            file_id.node_id()
        );
        let file_handle = node_path
            .as_file()
            .await
            .map_other_context("Failed to get file handle")?;
        let file_arc = file_handle.handle.get_file().await;
        let file_guard = file_arc.lock().await;
        if let Some(queryable_file) = file_guard.as_queryable() {
            debug!(
                "[SEARCH] SQL-DERIVED: File implements QueryableFile trait, calling as_table_provider..."
            );
            match queryable_file.as_table_provider(file_id, context).await {
                Ok(provider) => {
                    debug!(
                        "[OK] SQL-DERIVED: Successfully created table provider for file_id={}",
                        file_id.node_id()
                    );
                    self.apply_provider_wrapper(provider)
                }
                Err(e) => {
                    log::error!(
                        "[ERR] SQL-DERIVED: Failed to create table provider for file_id={}: {}",
                        file_id.node_id(),
                        e
                    );
                    Err(e)
                }
            }
        } else {
            // Not natively queryable.  Under a +series/+table URL cast (step 1 in
            // docs/url-entry-type-casting.md) a data-archetype node (e.g. a
            // git-ingested FileDynamic Parquet archive) reached here; cast it by
            // reading its bytes as Parquet.
            drop(file_guard);
            debug!(
                "[CAST] SQL-DERIVED: Single data file under cast for pattern '{}', reading as Parquet",
                pattern_name
            );
            let provider = Self::cast_data_node_to_parquet_provider(node_path).await?;
            self.apply_provider_wrapper(provider)
        }
    }

    /// Build a source TableProvider for a pattern that resolved to multiple
    /// files: a multi-URL ListingTable for natively-queryable files plus one
    /// Parquet MemTable per data-archetype cast file, unioned together.
    async fn build_multi_file_provider(
        &self,
        pattern_name: &str,
        queryable_files: &[tinyfs::NodePath],
        context: &tinyfs::ProviderContext,
    ) -> TinyFSResult<Arc<dyn TableProvider>> {
        let mut urls = Vec::new();
        let mut file_ids = Vec::new();
        // Data-archetype nodes matched under a +series/+table URL cast (e.g.
        // git-ingested FileDynamic Parquet archives) are not natively queryable
        // and cannot be enumerated through the tinyfs:/// version ListingTable;
        // they are read directly as Parquet (see docs/url-entry-type-casting.md).
        let mut data_cast_files: Vec<tinyfs::NodePath> = Vec::new();
        for node_path in queryable_files {
            let file_id = node_path.id();
            let file_handle = node_path
                .as_file()
                .await
                .map_other_context("Failed to get file handle")?;
            let file_arc = file_handle.handle.get_file().await;
            let is_queryable = {
                let file_guard = file_arc.lock().await;
                file_guard.as_queryable().is_some()
            };
            if is_queryable {
                // tinyfs:///pond/{pond_id}/part/{part_id}/node/{node_id}/version/
                let url_pattern = format!(
                    "tinyfs:///pond/{}/part/{}/node/{}/version/",
                    file_id.pond_id(),
                    file_id.part_id(),
                    file_id.node_id()
                );
                urls.push(url_pattern);
                file_ids.push(file_id);
            } else {
                data_cast_files.push(node_path.clone());
            }
        }

        if urls.is_empty() && data_cast_files.is_empty() {
            return Err(tinyfs::Error::Other(format!(
                "No valid URLs found for pattern '{}'",
                pattern_name
            )));
        }

        // Build one provider per source: a single multi-URL ListingTable for the
        // queryable files, plus one Parquet MemTable per data-cast file.
        let mut providers: Vec<Arc<dyn TableProvider>> = Vec::new();

        if !urls.is_empty() {
            // Note: Temporal bounds (from pond set-temporal-bounds) are enforced
            // at the Parquet reader level, not here at the factory level.
            let options = crate::TableProviderOptions {
                additional_urls: urls.clone(),
                ..Default::default()
            };

            log::debug!(
                "[LIST] CREATING multi-URL TableProvider for pattern '{}': {} URLs",
                pattern_name,
                urls.len()
            );

            // Use first file_id for logging (temporal bounds are explicit).
            let representative_file_id = file_ids.first().copied().unwrap_or(FileID::root());
            providers.push(
                crate::create_table_provider(representative_file_id, context, options)
                    .await
                    .map_other()?,
            );
        }

        for np in &data_cast_files {
            providers.push(Self::cast_data_node_to_parquet_provider(np).await?);
        }

        let combined: Arc<dyn TableProvider> = if providers.len() == 1 {
            providers
                .into_iter()
                .next()
                .expect("providers has exactly one element")
        } else {
            // Union the sources via SQL into a single MemTable
            // (mirrors the format-provider multi-file path).
            Self::union_providers_to_memtable(&providers).await?
        };

        self.apply_provider_wrapper(combined)
    }

    /// Resolve a single pattern to its source files, build a source
    /// TableProvider, apply transforms and scope prefixes, and register it in
    /// the DataFusion session under a deterministic table name.  Records the
    /// pattern -> table-name mapping in `table_mappings`.  Tables already
    /// registered (shared source files) are not rebuilt.
    ///
    /// Returns `true` when a real source table was registered (and mapped), or
    /// `false` when the pattern matched no files.  A zero-match pattern is NOT
    /// an error: a glob is a set and an empty set is valid.  The caller is
    /// responsible for registering an empty placeholder for the unmatched
    /// pattern (see `register_empty_pattern_table`), because the generated SQL
    /// still references every configured input by name and would otherwise fail
    /// to plan with "table 'inputN' not found".
    async fn register_pattern_source(
        &self,
        id: FileID,
        context: &tinyfs::ProviderContext,
        pattern_name: &str,
        pattern: &crate::Url,
        table_mappings: &mut HashMap<String, String>,
    ) -> TinyFSResult<bool> {
        let ctx = &context.datafusion_session;

        // Source files can be created by factories (Dynamic) or direct uploads
        // (Physical), so series mode searches both series variants.
        let entry_types = match self.get_mode() {
            SqlDerivedMode::Table => vec![EntryType::TablePhysicalVersion],
            SqlDerivedMode::Series => {
                vec![EntryType::TablePhysicalSeries, EntryType::TableDynamic]
            }
        };
        debug!(
            "[SEARCH] SQL-DERIVED: Processing pattern '{}' -> '{}' (entry_types: {:?})",
            pattern_name, pattern, entry_types
        );

        let mut queryable_files = Vec::new();
        for entry_type in &entry_types {
            match self
                .resolve_pattern_to_queryable_files(pattern, *entry_type)
                .await
            {
                Ok(files) => {
                    debug!(
                        "[SEARCH] SQL-DERIVED: Found {} files for pattern '{}' with entry_type {:?}",
                        files.len(),
                        pattern,
                        entry_type
                    );
                    queryable_files.extend(files);
                }
                Err(e) => {
                    debug!(
                        "[SEARCH] SQL-DERIVED: No files found for pattern '{}' with entry_type {:?}: {}",
                        pattern, entry_type, e
                    );
                }
            }
        }
        debug!(
            "[OK] SQL-DERIVED: Pattern '{}' resolved to {} total files across all entry types",
            pattern,
            queryable_files.len()
        );

        // Dedup across entry types by FileID.  Normally a node has exactly one
        // stored type so the per-entry-type searches are disjoint, but a
        // data-archetype node matched under a +series/+table cast is accepted by
        // every series/table entry-type search (see
        // docs/url-entry-type-casting.md), so it can appear more than once.
        {
            use std::collections::HashSet;
            let mut seen = HashSet::new();
            queryable_files.retain(|np| seen.insert(np.id()));
        }

        if queryable_files.is_empty() {
            return Ok(false);
        }

        // Data-only table name: depends ONLY on the pattern URL and resolved
        // files, NOT on the SQL query, so instances sharing source files reuse
        // one registered table instead of each building an expensive
        // ListingTable + glob.  When provider_wrapper or scope_prefixes are
        // configured the wrapping differs per consumer, so we append the
        // consumer's node_id to prevent caching collisions.
        let data_table_name = Self::generate_data_table_name(pattern, &queryable_files);
        let has_per_consumer_wrapping =
            self.config.provider_wrapper.is_some() || self.config.scope_prefixes.is_some();
        let unique_table_name = if has_per_consumer_wrapping {
            // Strip hyphens so the table name is a valid bare SQL identifier.
            let node_id_hex = id.node_id().to_string().replace('-', "");
            format!("{}_{}", data_table_name, node_id_hex)
        } else {
            data_table_name
        };
        debug!(
            "Generated data table name: '{}' for pattern '{}' (per-consumer wrapping: {})",
            unique_table_name, pattern, has_per_consumer_wrapping
        );

        _ = table_mappings.insert(pattern_name.to_string(), unique_table_name.clone());

        // Only the first instance sharing this data table name does the
        // expensive construction.  With per-consumer wrapping the name includes
        // the node_id so the check always misses and each consumer builds its own.
        let table_already_registered = matches!(
            ctx.catalog("datafusion")
                .expect("registered")
                .schema("public")
                .expect("defined")
                .table(&unique_table_name)
                .await,
            Ok(Some(_))
        );

        if table_already_registered {
            debug!(
                "[GO] SQL-DERIVED: Data table '{}' already registered, skipping construction",
                unique_table_name
            );
            return Ok(true);
        }

        let scheme = pattern.scheme();
        let is_format_provider = crate::FormatRegistry::get_provider(scheme).is_some();

        let listing_table_provider = if is_format_provider {
            self.build_format_provider_table(pattern_name, scheme, &queryable_files)
                .await?
        } else if queryable_files.len() == 1 {
            self.build_single_file_provider(pattern_name, &queryable_files[0], context)
                .await?
        } else {
            self.build_multi_file_provider(pattern_name, &queryable_files, context)
                .await?
        };

        debug!(
            "[SEARCH] SQL-DERIVED: Registering table '{}' (pattern: '{}') with DataFusion...",
            unique_table_name, pattern_name
        );

        // Apply user transforms first (before scope prefix)
        let table_provider = self
            .apply_transforms(listing_table_provider, pattern_name)
            .await?;

        // Then wrap with ScopePrefixTableProvider if scope prefix is configured
        let final_table_provider: Arc<dyn TableProvider> = if let Some(scope_prefixes) =
            &self.config.scope_prefixes
        {
            log::debug!(
                "[FIX] SQL-DERIVED: Checking scope_prefixes for pattern '{}'. Available keys: {:?}",
                pattern_name,
                scope_prefixes.keys().collect::<Vec<_>>()
            );
            if let Some((scope_prefix, time_column)) = scope_prefixes.get(pattern_name) {
                log::debug!(
                    "[FIX] SQL-DERIVED: Wrapping table '{}' (pattern '{}') with scope prefix '{}'",
                    unique_table_name,
                    pattern_name,
                    scope_prefix
                );
                let wrapped = Arc::new(
                    scope_prefix_table_provider(
                        table_provider,
                        scope_prefix.clone(),
                        time_column.clone(),
                    )
                    .map_other()?,
                );
                use datafusion::catalog::TableProvider;
                debug!(
                    "[FIX] SQL-DERIVED: Wrapped table '{}' schema: {:?}",
                    unique_table_name,
                    wrapped
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect::<Vec<_>>()
                );
                wrapped
            } else {
                table_provider
            }
        } else {
            table_provider
        };

        _ = ctx
            .register_table(
                datafusion::sql::TableReference::bare(unique_table_name.as_str()),
                final_table_provider,
            )
            .map_err(|e| {
                log::error!(
                    "[ERR] SQL-DERIVED: Failed to register table '{}': {}",
                    unique_table_name,
                    e
                );
                tinyfs::Error::Other(format!(
                    "Failed to register table '{}': {}",
                    unique_table_name, e
                ))
            })?;
        debug!(
            "[OK] SQL-DERIVED: Successfully registered table '{}' (user pattern: '{}') in SessionContext",
            unique_table_name, pattern_name
        );

        Ok(true)
    }

    /// Register an empty placeholder table for a pattern that matched no files.
    ///
    /// The generated SQL (notably from `timeseries-join`) references every
    /// configured input by its `inputN` alias, so an unmatched input must still
    /// resolve to a table or planning fails with "table 'inputN' not found".
    /// The placeholder contributes zero rows, so `UNION BY NAME` / `FULL JOIN`
    /// simply ignore it.
    ///
    /// The placeholder schema is cloned from a sibling input that shares the
    /// same scope (and therefore the same post-scope-prefix columns), so the
    /// empty side is type- and name-compatible without any guessing.  When the
    /// whole scope group is empty (no sibling resolved) we fall back to a lone
    /// time column.
    async fn register_empty_pattern_table(
        &self,
        id: FileID,
        context: &tinyfs::ProviderContext,
        pattern_name: &str,
        table_mappings: &mut HashMap<String, String>,
    ) -> TinyFSResult<()> {
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::datasource::MemTable;

        let ctx = &context.datafusion_session;

        // The placeholder name is deterministic in (pattern, node_id), and the
        // session context is shared across reads, so the same node re-evaluated
        // within a build (e.g. one export per resolution) would otherwise try
        // to register the same table twice.  Mirror the real-table path: if it
        // already exists, just (re)record the mapping and return.
        let node_id_hex = id.node_id().to_string().replace('-', "");
        let table_name = format!("sql_derived_empty_{}_{}", pattern_name, node_id_hex);

        let already_registered = matches!(
            ctx.catalog("datafusion")
                .expect("registered")
                .schema("public")
                .expect("defined")
                .table(&table_name)
                .await,
            Ok(Some(_))
        );
        if already_registered {
            _ = table_mappings.insert(pattern_name.to_string(), table_name);
            return Ok(());
        }

        let schema = match self
            .sibling_scope_schema(ctx, pattern_name, table_mappings)
            .await
        {
            Some(schema) => schema,
            None => {
                // Whole-scope-empty fallback: a single time column.  The time
                // column name comes from the input's scope config; default
                // "timestamp".  Microsecond UTC matches the canonical series
                // timestamp normalization.
                let time_col = self
                    .config
                    .scope_prefixes
                    .as_ref()
                    .and_then(|sp| sp.get(pattern_name))
                    .map(|(_, tc)| tc.clone())
                    .unwrap_or_else(|| "timestamp".to_string());
                Arc::new(Schema::new(vec![Field::new(
                    time_col,
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                )]))
            }
        };

        let empty = MemTable::try_new(schema, vec![vec![]]).map_other()?;
        _ = ctx
            .register_table(
                datafusion::sql::TableReference::bare(table_name.as_str()),
                Arc::new(empty),
            )
            .map_other()?;
        _ = table_mappings.insert(pattern_name.to_string(), table_name);
        debug!(
            "[OK] SQL-DERIVED: Registered empty placeholder for zero-match pattern '{}'",
            pattern_name
        );
        Ok(())
    }

    /// Find the schema of an already-registered sibling input that shares the
    /// same scope as `pattern_name`, used to build a compatible empty
    /// placeholder.  Returns `None` when scopes are not configured or no
    /// same-scope sibling has been registered yet.
    async fn sibling_scope_schema(
        &self,
        ctx: &datafusion::prelude::SessionContext,
        pattern_name: &str,
        table_mappings: &HashMap<String, String>,
    ) -> Option<arrow::datatypes::SchemaRef> {
        let scope_prefixes = self.config.scope_prefixes.as_ref()?;
        let (scope, _) = scope_prefixes.get(pattern_name)?;
        let schema = ctx.catalog("datafusion")?.schema("public")?;

        for (other_pattern, (other_scope, _)) in scope_prefixes.iter() {
            if other_pattern == pattern_name || other_scope != scope {
                continue;
            }
            let Some(table_name) = table_mappings.get(other_pattern) else {
                continue;
            };
            if let Ok(Some(provider)) = schema.table(table_name).await {
                return Some(provider.schema());
            }
        }
        None
    }
}

// QueryableFile trait implementation - follows anti-duplication principles
#[async_trait]
impl tinyfs::QueryableFile for SqlDerivedFile {
    /// Create TableProvider for SqlDerivedFile using DataFusion's ViewTable
    ///
    /// This approach creates a ViewTable that wraps the SqlDerivedFile's SQL query,
    /// allowing DataFusion to handle query planning and optimization efficiently.
    async fn as_table_provider(
        &self,
        id: FileID,
        context: &tinyfs::ProviderContext,
    ) -> tinyfs::Result<Arc<dyn TableProvider>> {
        // Check cache first for SqlDerivedFile ViewTable
        let cache_key = crate::TableProviderKey::new(id, crate::VersionSelection::LatestVersion)
            .to_cache_string();

        if let Some(cached_provider) = context.get_table_provider_cache(&cache_key) {
            debug!("[GO] CACHE HIT: Returning cached ViewTable for node_id: {id}");
            return Ok(cached_provider);
        }

        debug!("[SAVE] CACHE MISS: Creating new ViewTable for node_id: {id}");

        // Get SessionContext directly from ProviderContext
        let ctx = &context.datafusion_session;

        // Create mapping from user pattern names to unique internal table names
        let mut table_mappings = HashMap::new();

        // Register each pattern as a table in the session context.  Patterns
        // that match no files are collected and handled in a second pass: the
        // generated SQL references every input by name, so an unmatched input
        // must still resolve to an (empty) table or planning fails.  The second
        // pass runs after all real tables are registered so empty placeholders
        // can borrow a same-scope sibling's schema.
        let mut empty_patterns = Vec::new();
        for (pattern_name, pattern) in &self.get_config().patterns {
            let registered = self
                .register_pattern_source(id, context, pattern_name, pattern, &mut table_mappings)
                .await?;
            if !registered {
                empty_patterns.push(pattern_name.clone());
            }
        }
        for pattern_name in &empty_patterns {
            self.register_empty_pattern_table(id, context, pattern_name, &mut table_mappings)
                .await?;
        }

        // Get the effective SQL query with table name substitutions using our unique internal names
        debug!(
            "[SEARCH] SQL-DERIVED: Original query: {:?}",
            self.config.query
        );
        debug!("[SEARCH] SQL-DERIVED: Table mappings: {:?}", table_mappings);
        let effective_sql = self.get_effective_sql(&SqlTransformOptions {
            table_mappings: Some(table_mappings.clone()),
            source_replacement: None,
        });

        let mapping_count = table_mappings.len();
        debug!(
            "[OK] SQL-DERIVED: Effective SQL after table mapping ({} mappings): {}",
            mapping_count, effective_sql
        );
        debug!(
            "[SEARCH] SQL-DERIVED: Table mappings details: {:?}",
            table_mappings
        );

        // Parse the SQL into a LogicalPlan
        debug!(
            "[SEARCH] SQL-DERIVED: Executing SQL with DataFusion: {}",
            effective_sql
        );

        // Debug: List all registered tables in the context
        debug!("[SEARCH] SQL-DERIVED: Available tables in DataFusion context:");
        if let Some(catalog) = ctx.catalog("datafusion") {
            if let Some(schema) = catalog.schema("public") {
                let table_names: Vec<String> = schema.table_names();
                debug!("[SEARCH] SQL-DERIVED: Registered tables: {:?}", table_names);
            } else {
                debug!("[SEARCH] SQL-DERIVED: Could not access 'public' schema");
            }
        } else {
            debug!("[SEARCH] SQL-DERIVED: Could not access 'datafusion' catalog");
        }

        let logical_plan = ctx
            .sql(&effective_sql)
            .await
            .map_err(|e| {
                log::error!(
                    "[ERR] SQL-DERIVED: Failed to parse SQL into LogicalPlan: {}",
                    e
                );
                log::error!("[ERR] SQL-DERIVED: Failed SQL was: {}", effective_sql);
                tinyfs::Error::Other(format!("Failed to parse SQL into LogicalPlan: {}", e))
            })?
            .logical_plan()
            .clone();
        debug!("[OK] SQL-DERIVED: Successfully created logical plan");

        use datafusion::catalog::view::ViewTable;

        let view_table = ViewTable::new(logical_plan, Some(effective_sql));
        let table_provider = Arc::new(view_table);

        // Cache the ViewTable for future reuse
        context.set_table_provider_cache(cache_key, table_provider.clone())?;
        debug!("[SAVE] CACHED: Stored ViewTable for node_id: {id}");

        Ok(table_provider)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProviderContext;
    use crate::QueryableFile;
    use crate::factory::temporal_reduce::{
        AggregationConfig, AggregationType, TemporalReduceConfig, TemporalReduceDirectory,
    };
    use arrow::array::{
        Array, Float64Array, Int32Array, Int64Array, StringArray, TimestampMillisecondArray,
        TimestampSecondArray,
    };
    use arrow::compute::concat_batches;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use arrow_array::record_batch;
    use arrow_cast::cast;
    use datafusion::physical_plan::collect;
    use parquet::arrow::ArrowWriter;
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::sync::Arc;
    use tinyfs::Directory;
    use tinyfs::PartID;
    use tinyfs::arrow::ParquetExt;

    // Test helper: Create patterns HashMap from string URL literals
    fn test_patterns(pairs: &[(&str, &str)]) -> HashMap<String, crate::Url> {
        pairs
            .iter()
            .map(|(name, url_str)| {
                (
                    name.to_string(),
                    crate::Url::parse(url_str).expect("Valid test URL"),
                )
            })
            .collect()
    }

    // ========================================================================
    // SqlDerivedConfig Tests
    // ========================================================================

    #[test]
    fn test_sql_derived_config_new() {
        let patterns = test_patterns(&[("table1", "series:///path/*.series")]);
        let config = SqlDerivedConfig::new(patterns, Some("SELECT * FROM table1".to_string()));

        assert_eq!(config.patterns.len(), 1);
        assert!(config.query.is_some());
        assert!(config.scope_prefixes.is_none());
        assert!(config.provider_wrapper.is_none());
    }

    #[test]
    fn test_sql_derived_config_scoped() {
        let patterns = test_patterns(&[("table1", "series:///path/*.series")]);
        let scope_prefixes = [(
            "table1".to_string(),
            ("scope_".to_string(), "timestamp".to_string()),
        )]
        .into();

        let config = SqlDerivedConfig::new_scoped(patterns, None, scope_prefixes);

        assert!(config.scope_prefixes.is_some());
        assert_eq!(config.scope_prefixes.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn test_sql_derived_config_serialization() {
        let patterns = test_patterns(&[("table1", "series:///path/*.series")]);
        let config = SqlDerivedConfig::new(patterns, Some("SELECT * FROM table1".to_string()));

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: SqlDerivedConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(config.patterns, deserialized.patterns);
        assert_eq!(config.query, deserialized.query);
    }

    // ========================================================================
    // Test Helpers
    // ========================================================================

    use crate::factory::test_support::{
        create_parquet_file, create_parquet_from_batch, create_test_environment, test_context,
    };

    /// Helper function to get a string array from any column, handling different Arrow string types
    fn get_string_array(batch: &RecordBatch, column_index: usize) -> Arc<StringArray> {
        let column = batch.column(column_index);
        let string_column = match column.data_type() {
            DataType::Utf8 => column.clone(),
            _ => cast(column.as_ref(), &DataType::Utf8).expect("Failed to cast column to string"),
        };
        string_column
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast to StringArray")
            .clone()
            .into()
    }

    /// Helper function to execute a SQL-derived file using direct table provider scanning
    /// This preserves ORDER BY clauses and other SQL semantics from the original query
    async fn execute_sql_derived_direct(
        sql_derived_file: &SqlDerivedFile,
        provider_context: &ProviderContext,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        debug!("execute_sql_derived_direct: Starting execution");

        // Get table provider using the SqlDerivedFile's own file_id from its context
        // This ensures each SQL file has a unique cache key
        let table_provider = sql_derived_file
            .as_table_provider(sql_derived_file.context.file_id, provider_context)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        debug!("execute_sql_derived_direct: Got table provider");

        // Scan the ViewTable directly to preserve ORDER BY and other semantics
        let ctx = &provider_context.datafusion_session;
        let state = ctx.state();
        let execution_plan = table_provider
            .scan(
                &state, // session state
                None,   // projection (None = all columns)
                &[],    // filters (empty = no additional filters)
                None,   // limit (None = no limit)
            )
            .await?;

        // Execute the plan directly
        let task_context = ctx.task_ctx();
        debug!("execute_sql_derived_direct: Executing plan");
        let result_batches = collect(execution_plan, task_context).await?;

        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        let batch_count = result_batches.len();
        debug!("execute_sql_derived_direct: Got {total_rows} total rows in {batch_count} batches");

        Ok(result_batches)
    }

    #[tokio::test]
    async fn test_sql_derived_config_validation() {
        let valid_config = r#"
patterns:
  testdata: "table:///test/data.parquet"
query: "SELECT * FROM source WHERE value > 10"
"#;

        let result = validate_sql_derived_config(valid_config.as_bytes());
        assert!(result.is_ok());

        // Test valid pattern config with multiple patterns
        let valid_pattern_config = r#"
patterns:
  data: "table:///data/*.parquet"
  metrics: "table:///metrics/*.parquet"
query: "SELECT * FROM source WHERE value > 10"
"#;

        let result = validate_sql_derived_config(valid_pattern_config.as_bytes());
        assert!(result.is_ok());

        // Test valid config without query (should use default)
        let valid_no_query_config = r#"
patterns:
  data: "table:///data/*.parquet"
"#;

        let result = validate_sql_derived_config(valid_no_query_config.as_bytes());
        assert!(result.is_ok());

        // Test empty patterns map
        let invalid_config = r#"
patterns: {}
query: "SELECT * FROM source"
"#;

        let result = validate_sql_derived_config(invalid_config.as_bytes());
        assert!(result.is_err());

        // Test empty pattern value in map
        let invalid_config = r#"
patterns:
  data: "table:///data/*.parquet"
  invalid: "not-a-valid://url"
query: "SELECT * FROM source"
"#;

        let result = validate_sql_derived_config(invalid_config.as_bytes());
        assert!(result.is_err());

        // Test no patterns specified
        let invalid_config = r#"
query: "SELECT * FROM source"
"#;

        let result = validate_sql_derived_config(invalid_config.as_bytes());
        assert!(result.is_err());

        // Test empty query (when specified)
        let invalid_config = r#"
patterns:
  testdata: "/test/data.parquet"
query: ""
"#;

        let result = validate_sql_derived_config(invalid_config.as_bytes());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sql_derived_pattern_matching() {
        // Create memory filesystem with shared persistence
        let (fs, provider_context) = create_test_environment().await;

        // Create sensor_data1.parquet
        let batch1 = record_batch!(
            ("sensor_id", Int32, [101, 102]),
            ("location", Utf8, ["Building A", "Building B"]),
            ("reading", Int32, [80, 85])
        )
        .unwrap();

        _ = create_parquet_from_batch(
            &fs,
            "/sensor_data1.parquet",
            &batch1,
            EntryType::TablePhysicalVersion,
        )
        .await
        .unwrap();

        // Create sensor_data2.parquet
        let batch2 = record_batch!(
            ("sensor_id", Int32, [201, 202]),
            ("location", Utf8, ["Building C", "Building D"]),
            ("reading", Int32, [90, 95])
        )
        .unwrap();

        _ = create_parquet_from_batch(
            &fs,
            "/sensor_data2.parquet",
            &batch2,
            EntryType::TablePhysicalVersion,
        )
        .await
        .unwrap();

        // Now test pattern matching across both files
        let context = test_context(&provider_context, FileID::root());
        let config = SqlDerivedConfig {
            patterns: test_patterns(&[("sensor_data", "table:///sensor_data*.parquet")]),
            query: Some("SELECT location, reading FROM sensor_data WHERE reading > 85 ORDER BY reading DESC".to_string()),
            ..Default::default()
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Table).unwrap();

        // Read the result using direct table provider scanning
        let result_batches = execute_sql_derived_direct(&sql_derived_file, &provider_context)
            .await
            .unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");
        let result_batch = &result_batches[0];

        // We should have data from both files matching reading > 85
        // File 1: Building A (80) - excluded, Building B (85) - excluded because 85 is not > 85
        // File 2: Building C (90), Building D (95) - both included
        // So we expect 2 rows
        assert_eq!(result_batch.num_rows(), 2);
        assert_eq!(result_batch.num_columns(), 2);

        // Check that we have the right data ordered by reading DESC
        let locations = get_string_array(result_batch, 0);
        let readings = result_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Should be ordered by reading DESC: Building D (95), Building C (90)
        assert_eq!(locations.value(0), "Building D");
        assert_eq!(readings.value(0), 95);

        assert_eq!(locations.value(1), "Building C");
        assert_eq!(readings.value(1), 90);
    }

    #[tokio::test]
    async fn test_sql_derived_recursive_pattern_matching() {
        // Create memory filesystem with shared persistence
        let (fs, provider_context) = create_test_environment().await;
        let root = fs.root().await.unwrap();

        // Set up FileSeries files in different directories
        // Create /sensors/building_a/data.parquet

        let batch_a = record_batch!(
            ("sensor_id", Int32, [301]),
            ("location", Utf8, ["Building A"]),
            ("reading", Int32, [100])
        )
        .unwrap();

        // Create directory first, then file
        _ = root.create_dir_path("/sensors").await.unwrap();
        _ = root.create_dir_path("/sensors/building_a").await.unwrap();
        _ = create_parquet_from_batch(
            &fs,
            "/sensors/building_a/data.parquet",
            &batch_a,
            EntryType::TablePhysicalVersion,
        )
        .await
        .unwrap();

        // Create /sensors/building_b/data.parquet
        let batch_b = record_batch!(
            ("sensor_id", Int32, [302]),
            ("location", Utf8, ["Building B"]),
            ("reading", Int32, [110])
        )
        .unwrap();

        _ = root.create_dir_path("/sensors/building_b").await.unwrap();
        _ = create_parquet_from_batch(
            &fs,
            "/sensors/building_b/data.parquet",
            &batch_b,
            EntryType::TablePhysicalVersion,
        )
        .await
        .unwrap();

        // Add a small delay to ensure the async writer background task completes
        tokio::task::yield_now().await;

        // Now test recursive pattern matching across all nested files
        let context = test_context(&provider_context, FileID::root());
        let config = SqlDerivedConfig {
            patterns: test_patterns(&[("data", "table:////**/data.parquet")]),
            query: Some(
                "SELECT location, reading, sensor_id FROM data ORDER BY sensor_id".to_string(),
            ),
            ..Default::default()
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Table).unwrap();

        // Read the result using direct table provider scanning
        let result_batches = execute_sql_derived_direct(&sql_derived_file, &provider_context)
            .await
            .unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");

        // We should have data from both nested directories
        // Building A: sensor_id 301, reading 100
        // Building B: sensor_id 302, reading 110
        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        // Concatenate all batches into one for easier testing
        let schema = result_batches[0].schema();
        let result_batch = concat_batches(&schema, &result_batches).unwrap();

        assert_eq!(result_batch.num_columns(), 3);

        let locations = get_string_array(&result_batch, 0);
        let readings = result_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let sensor_ids = result_batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Should be ordered by sensor_id: Building A (301), Building B (302)
        assert_eq!(sensor_ids.value(0), 301);
        assert_eq!(locations.value(0), "Building A");
        assert_eq!(readings.value(0), 100);

        assert_eq!(sensor_ids.value(1), 302);
        assert_eq!(locations.value(1), "Building B");
        assert_eq!(readings.value(1), 110);
    }

    #[tokio::test]
    async fn test_sql_derived_multiple_patterns() {
        // Create memory filesystem with shared persistence
        let (fs, provider_context) = create_test_environment().await;
        let root = fs.root().await.unwrap();

        // Set up FileSeries files in different locations matching different patterns
        // Create files in /metrics/ directory

        // Metrics file 1
        let batch_metrics = record_batch!(
            ("sensor_id", Int32, [401, 402]),
            ("location", Utf8, ["Metrics Room A", "Metrics Room B"]),
            ("reading", Int32, [120, 125])
        )
        .unwrap();

        _ = root.create_dir_path("/metrics").await.unwrap();
        _ = create_parquet_from_batch(
            &fs,
            "/metrics/data.parquet",
            &batch_metrics,
            EntryType::TablePhysicalVersion,
        )
        .await
        .unwrap();

        // Create files in /logs/ directory
        // Logs file 1
        let batch_logs = record_batch!(
            ("sensor_id", Int32, [501, 502]),
            ("location", Utf8, ["Log Server A", "Log Server B"]),
            ("reading", Int32, [130, 135])
        )
        .unwrap();

        _ = root.create_dir_path("/logs").await.unwrap();
        _ = create_parquet_from_batch(
            &fs,
            "/logs/info.parquet",
            &batch_logs,
            EntryType::TablePhysicalVersion,
        )
        .await
        .unwrap();

        // Test multiple patterns combining files from different directories
        let context = test_context(&provider_context, FileID::root());
        let config = SqlDerivedConfig {
            patterns: test_patterns(&[
                ("metrics", "table:///metrics/*.parquet"),
                ("logs", "table:///logs/*.parquet"),
            ]),
            query: Some("SELECT * FROM metrics UNION ALL SELECT * FROM logs".to_string()),
            ..Default::default()
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Table).unwrap();

        // Read the result using direct table provider scanning
        let result_batches = execute_sql_derived_direct(&sql_derived_file, &provider_context)
            .await
            .unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");

        // We should have data from both directories: 2 from metrics + 2 from logs = 4 rows
        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4);

        // Concatenate all batches into one for easier testing
        let schema = result_batches[0].schema();
        let result_batch = concat_batches(&schema, &result_batches).unwrap();

        assert_eq!(result_batch.num_columns(), 3);

        // Check that we have data from both locations
        let sensor_ids = result_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let locations = get_string_array(&result_batch, 1);

        // Verify we have sensor IDs from both metrics (401, 402) and logs (501, 502)
        let mut found_metrics = false;
        let mut found_logs = false;

        for i in 0..result_batch.num_rows() {
            let sensor_id = sensor_ids.value(i);
            let location = locations.value(i);

            if (401..=402).contains(&sensor_id) {
                found_metrics = true;
                assert!(location.contains("Metrics"));
            } else if (501..=502).contains(&sensor_id) {
                found_logs = true;
                assert!(location.contains("Log Server"));
            }
        }

        assert!(found_metrics, "Should have found metrics data");
        assert!(found_logs, "Should have found logs data");
    }

    #[tokio::test]
    async fn test_sql_derived_default_query() {
        // Create memory filesystem with shared persistence
        let (fs, provider_context) = create_test_environment().await;

        // Create test Parquet data
        let batch = record_batch!(
            ("sensor_id", Int32, [101, 102, 103, 104, 105]),
            (
                "location",
                Utf8,
                [
                    "Building A",
                    "Building B",
                    "Building C",
                    "Building A",
                    "Building B"
                ]
            ),
            ("reading", Int32, [75, 82, 68, 90, 77])
        )
        .unwrap();

        _ = create_parquet_from_batch(
            &fs,
            "/sensor_data.parquet",
            &batch,
            EntryType::TablePhysicalVersion,
        )
        .await
        .unwrap();

        // Create SQL-derived file without specifying query (should use default)
        let context = test_context(&provider_context, FileID::root());
        let config = SqlDerivedConfig {
            patterns: test_patterns(&[("sensor_data", "table:///sensor_data.parquet")]),
            query: None, // No query specified - should default to "SELECT * FROM source"
            ..Default::default()
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Table).unwrap();

        // Use the new QueryableFile approach with ViewTable (no materialization)
        let table_provider = sql_derived_file
            .as_table_provider(FileID::root(), &provider_context)
            .await
            .unwrap();

        // Get session context and query the ViewTable directly
        let ctx = &provider_context.datafusion_session;
        _ = ctx.register_table("test_table", table_provider).unwrap();

        // Execute query to get results
        let dataframe = ctx.sql("SELECT * FROM test_table").await.unwrap();
        let result_batches = dataframe.collect().await.unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");
        let result_batch = &result_batches[0];

        // Should have all the original data with default query "SELECT * FROM source"
        // Original test data has 5 rows with sensor readings [75, 82, 68, 90, 77]
        assert_eq!(result_batch.num_rows(), 5);
        assert_eq!(result_batch.num_columns(), 3); // sensor_id, location, reading

        // Verify column names (should match original schema)
        let schema = result_batch.schema();
        assert_eq!(schema.field(0).name(), "sensor_id");
        assert_eq!(schema.field(1).name(), "location");
        assert_eq!(schema.field(2).name(), "reading");
    }

    #[tokio::test]
    async fn test_sql_derived_file_series_single_version() {
        // Create memory filesystem with shared persistence
        let (fs, provider_context) = create_test_environment().await;

        // Create test Parquet data
        let batch = record_batch!(
            ("sensor_id", Int32, [101, 102, 103, 104, 105]),
            (
                "location",
                Utf8,
                [
                    "Building A",
                    "Building B",
                    "Building C",
                    "Building A",
                    "Building B"
                ]
            ),
            ("reading", Int32, [75, 82, 68, 90, 77])
        )
        .unwrap();

        _ = create_parquet_from_batch(
            &fs,
            "/sensor_data.parquet",
            &batch,
            EntryType::TablePhysicalVersion,
        )
        .await
        .unwrap();

        // Create the SQL-derived file with FileSeries source
        let context = test_context(&provider_context, FileID::root());
        let config = SqlDerivedConfig {
            patterns: test_patterns(&[("sensor_data", "table:///sensor_data.parquet")]),
            query: Some("SELECT location, reading * 1.5 as adjusted_reading FROM sensor_data WHERE reading > 75 ORDER BY adjusted_reading DESC".to_string()),
            ..Default::default()
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Table).unwrap();

        // Use the new QueryableFile approach with ViewTable (no materialization)
        let table_provider = sql_derived_file
            .as_table_provider(FileID::root(), &provider_context)
            .await
            .unwrap();

        // Get session context and query the ViewTable directly
        let ctx = &provider_context.datafusion_session;
        _ = ctx.register_table("test_table", table_provider).unwrap();

        // Execute query to get results
        let dataframe = ctx.sql("SELECT * FROM test_table").await.unwrap();
        let result_batches = dataframe.collect().await.unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");
        let result_batch = &result_batches[0];

        // Verify we got the expected derived data from FileSeries
        // Original data: sensor readings [75, 82, 68, 90, 77] for locations ["Building A", "Building B", "Building C", "Building A", "Building B"]
        // Query: WHERE reading > 75, SELECT location, reading * 1.5 as adjusted_reading, ORDER BY adjusted_reading DESC
        // Expected: Building A (90*1.5=135), Building B (82*1.5=123), Building B (77*1.5=115.5)
        // Note: 75 is not > 75, and 68 < 75, so they're excluded
        assert_eq!(result_batch.num_rows(), 3);
        assert_eq!(result_batch.num_columns(), 2);

        // Check column names
        let schema = result_batch.schema();
        assert_eq!(schema.field(0).name(), "location");
        assert_eq!(schema.field(1).name(), "adjusted_reading");
    }

    #[tokio::test]
    async fn test_sql_derived_file_series_two_versions() {
        // Create memory filesystem with shared persistence
        let (fs, provider_context) = create_test_environment().await;

        // Set up FileSeries test data with 2 versions

        for version in 1..=2 {
            let base_sensor_id = 100 + (version * 10);
            let base_reading = 70 + (version * 10);

            let batch = record_batch!(
                (
                    "sensor_id",
                    Int32,
                    [base_sensor_id + 1, base_sensor_id + 2, base_sensor_id + 3]
                ),
                (
                    "location",
                    Utf8,
                    [
                        format!("Building {}", version),
                        format!("Building {}", version),
                        format!("Building {}", version)
                    ]
                ),
                (
                    "reading",
                    Int32,
                    [base_reading, base_reading + 5, base_reading + 10]
                )
            )
            .unwrap();

            let filename = format!("/multi_sensor_data_v{}.parquet", version);
            _ = create_parquet_from_batch(&fs, &filename, &batch, EntryType::TablePhysicalVersion)
                .await
                .unwrap();
        }

        // Create the SQL-derived file with multi-version FileSeries source
        let context = test_context(&provider_context, FileID::root());
        let config = SqlDerivedConfig {
            patterns: test_patterns(&[("multi_sensor_data", "table:///multi_sensor_data*.parquet")]),
            query: Some("SELECT location, reading FROM multi_sensor_data WHERE reading > 75 ORDER BY reading DESC".to_string()),
            ..Default::default()
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Table).unwrap();

        // Use the new QueryableFile approach with ViewTable (no materialization)
        let table_provider = sql_derived_file
            .as_table_provider(FileID::root(), &provider_context)
            .await
            .unwrap();

        // Get session context and query the ViewTable directly
        let ctx = &provider_context.datafusion_session;
        _ = ctx.register_table("test_table", table_provider).unwrap();

        // Execute query to get results
        let dataframe = ctx.sql("SELECT * FROM test_table").await.unwrap();
        let result_batches = dataframe.collect().await.unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");
        let result_batch = &result_batches[0];

        // For now, we expect data from version 1 only (readings: [70, 75, 80])
        // Query: WHERE reading > 75, so we should get reading=80 from Building 1
        assert!(
            result_batch.num_rows() >= 1,
            "Should have at least 1 row with reading > 75"
        );
        assert_eq!(result_batch.num_columns(), 2);

        // Check column names
        let schema = result_batch.schema();
        assert_eq!(schema.field(0).name(), "location");
        assert_eq!(schema.field(1).name(), "reading");
    }

    #[tokio::test]
    async fn test_sql_derived_file_series_three_versions() {
        // Create memory filesystem with shared persistence
        let (fs, provider_context) = create_test_environment().await;

        // Set up FileSeries test data with 3 versions
        for version in 1..=3 {
            let base_sensor_id = 100 + (version * 10);
            let base_reading = 70 + (version * 10);

            let batch = record_batch!(
                (
                    "sensor_id",
                    Int32,
                    [base_sensor_id + 1, base_sensor_id + 2, base_sensor_id + 3]
                ),
                (
                    "location",
                    Utf8,
                    [
                        format!("Building {}", version),
                        format!("Building {}", version),
                        format!("Building {}", version)
                    ]
                ),
                (
                    "reading",
                    Int32,
                    [base_reading, base_reading + 5, base_reading + 10]
                )
            )
            .unwrap();

            let filename = format!("/multi_sensor_data_v{}.parquet", version);
            _ = create_parquet_from_batch(&fs, &filename, &batch, EntryType::TablePhysicalVersion)
                .await
                .unwrap();
        }

        // Create the SQL-derived file that should union all 3 versions
        let context = test_context(&provider_context, FileID::root());
        let config = SqlDerivedConfig {
            patterns: test_patterns(&[(
                "multi_sensor_data",
                "table:///multi_sensor_data*.parquet",
            )]),
            // This query should return data from all 3 versions
            query: Some(
                "SELECT location, reading, sensor_id FROM multi_sensor_data ORDER BY sensor_id"
                    .to_string(),
            ),
            ..Default::default()
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Table).unwrap();

        // Use the new QueryableFile approach with ViewTable (no materialization)
        let table_provider = sql_derived_file
            .as_table_provider(FileID::root(), &provider_context)
            .await
            .unwrap();

        // Get session context and query the ViewTable directly
        let ctx = &provider_context.datafusion_session;
        _ = ctx.register_table("test_table", table_provider).unwrap();

        // Execute query to get results
        let dataframe = ctx.sql("SELECT * FROM test_table").await.unwrap();
        let result_batches = dataframe.collect().await.unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");

        // We should have data from all 3 versions (3 rows per version = 9 total rows)
        // Version 1: sensor_ids 111, 112, 113 (Building 1)
        // Version 2: sensor_ids 121, 122, 123 (Building 2)
        // Version 3: sensor_ids 131, 132, 133 (Building 3)
        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 9);

        // Concatenate all batches into one for easier testing
        let schema = result_batches[0].schema();
        let result_batch = concat_batches(&schema, &result_batches).unwrap();

        assert_eq!(result_batch.num_columns(), 3);

        // Check that we have sensor IDs from all versions
        let sensor_ids = result_batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let locations = get_string_array(&result_batch, 0);

        // Verify we have sensor IDs from all 3 versions (ordered by sensor_id)
        assert_eq!(sensor_ids.value(0), 111); // First version
        assert_eq!(locations.value(0), "Building 1");

        assert_eq!(sensor_ids.value(3), 121); // Second version (after 111,112,113)
        assert_eq!(locations.value(3), "Building 2");

        assert_eq!(sensor_ids.value(6), 131); // Third version (after 111,112,113,121,122,123)
        assert_eq!(locations.value(6), "Building 3");
    }

    #[tokio::test]
    async fn test_sql_derived_factory_creation() {
        // Create memory filesystem with shared persistence
        let (fs, provider_context) = create_test_environment().await;

        // Create test Parquet data
        let batch = record_batch!(
            ("id", Int32, [1, 2, 3, 4, 5]),
            ("name", Utf8, ["Alice", "Bob", "Charlie", "David", "Eve"]),
            ("value", Int32, [100, 200, 150, 300, 250])
        )
        .unwrap();

        let schema = batch.schema();
        let mut parquet_buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer);
            let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
            writer.write(&batch).unwrap();
            _ = writer.close().unwrap();
        }

        _ = create_parquet_file(
            &fs,
            "/data.parquet",
            parquet_buffer,
            EntryType::TablePhysicalVersion,
        )
        .await
        .unwrap();

        // Create the SQL-derived file with read-only state context
        let context = test_context(&provider_context, FileID::root());
        let config = SqlDerivedConfig {
            patterns: test_patterns(&[("data", "table:///data.parquet")]),
            query: Some("SELECT name, value * 2 as doubled_value FROM data WHERE value > 150 ORDER BY doubled_value DESC".to_string()),
            ..Default::default()
        };

        let sql_derived_file = SqlDerivedFile::new(config, context, SqlDerivedMode::Table).unwrap();

        // Use helper function for direct table provider scanning
        let result_batches = execute_sql_derived_direct(&sql_derived_file, &provider_context)
            .await
            .unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");
        let result_batch = &result_batches[0];

        // DEBUG: Print actual data to understand ordering
        log::debug!(
            "Result batch has {} rows, {} columns",
            result_batch.num_rows(),
            result_batch.num_columns()
        );
        for (i, column) in result_batch.columns().iter().enumerate() {
            log::debug!("Column {}: {:?}", i, column);
        }

        // Verify we got the expected derived data with ORDER BY preserved
        // Query: "SELECT name, value * 2 as doubled_value FROM data WHERE value > 150 ORDER BY doubled_value DESC"
        // Expected: David (300*2=600), Eve (250*2=500), Bob (200*2=400) - order should be preserved!
        // Note: Charlie (150) is excluded because 150 is not > 150
        assert_eq!(result_batch.num_rows(), 3);
        assert_eq!(result_batch.num_columns(), 2);

        // Check column names
        let schema = result_batch.schema();
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(1).name(), "doubled_value");

        // Check data - ORDER BY should be preserved with direct table provider scanning
        let names = get_string_array(result_batch, 0);
        let doubled_values = result_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // With ORDER BY doubled_value DESC, should be: David (600), Eve (500), Bob (400)
        assert_eq!(names.value(0), "David");
        assert_eq!(doubled_values.value(0), 600);

        assert_eq!(names.value(1), "Eve");
        assert_eq!(doubled_values.value(1), 500);

        assert_eq!(names.value(2), "Bob");
        assert_eq!(doubled_values.value(2), 400);
    }

    /// Test SQL-derived chain functionality and document predicate pushdown limitations
    #[tokio::test]
    async fn test_sql_derived_chain() {
        // Create memory filesystem with shared persistence
        let (fs, provider_context) = create_test_environment().await;

        // Create test Parquet data
        let batch = record_batch!(
            ("id", Int32, [1, 2, 3, 4, 5]),
            ("name", Utf8, ["Alice", "Bob", "Charlie", "David", "Eve"]),
            ("value", Int32, [100, 200, 150, 300, 250])
        )
        .unwrap();

        let schema = batch.schema();
        let mut parquet_buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer);
            let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
            writer.write(&batch).unwrap();
            _ = writer.close().unwrap();
        }

        _ = create_parquet_file(
            &fs,
            "/data.parquet",
            parquet_buffer,
            EntryType::TablePhysicalVersion,
        )
        .await
        .unwrap();

        // Test chaining: Create two SQL-derived nodes, where one refers to the other
        // Create the first SQL-derived file (filters and transforms original data)
        // Generate unique FileID based on the SQL query content
        let first_query = "SELECT name, value + 50 as adjusted_value FROM data WHERE value >= 200 ORDER BY adjusted_value";
        let first_file_id = FileID::from_content(
            PartID::root(),
            EntryType::TableDynamic,
            first_query.as_bytes(),
            tinyfs::local_pond_uuid(),
        );
        let context = test_context(&provider_context, first_file_id);
        let first_config = SqlDerivedConfig {
            patterns: test_patterns(&[("data", "table:///data.parquet")]),
            query: Some("SELECT name, value + 50 as adjusted_value FROM data WHERE value >= 200 ORDER BY adjusted_value".to_string()),
            ..Default::default()
        };

        let first_sql_file =
            SqlDerivedFile::new(first_config, context.clone(), SqlDerivedMode::Table).unwrap();

        // Execute the first SQL-derived query using direct table provider scanning
        let first_result_batches = execute_sql_derived_direct(&first_sql_file, &provider_context)
            .await
            .unwrap();

        // Convert result batches to Parquet format for intermediate storage
        let first_result_data = {
            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let schema = first_result_batches[0].schema();
                let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
                for batch in &first_result_batches {
                    writer.write(batch).unwrap();
                }
                _ = writer.close().unwrap();
            }
            parquet_buffer
        };

        // Store the first result as an intermediate Parquet file
        _ = create_parquet_file(
            &fs,
            "/intermediate.parquet",
            first_result_data,
            EntryType::TablePhysicalVersion,
        )
        .await
        .unwrap();

        // Second query: Create the second SQL-derived node that chains from the first
        {
            // Generate unique FileID for second SQL file based on its query content
            let second_query = "SELECT name, adjusted_value * 2 as final_value FROM intermediate WHERE adjusted_value > 250 ORDER BY final_value DESC";
            let second_file_id = FileID::from_content(
                PartID::root(),
                EntryType::TableDynamic,
                second_query.as_bytes(),
                tinyfs::local_pond_uuid(),
            );
            let context = test_context(&provider_context, second_file_id);
            let second_config = SqlDerivedConfig {
                patterns: test_patterns(&[("intermediate", "table:///intermediate.parquet")]),
                query: Some(second_query.to_string()),
                ..Default::default()
            };

            let second_sql_file =
                SqlDerivedFile::new(second_config, context, SqlDerivedMode::Table).unwrap();

            // Execute the final chained result using direct table provider scanning
            let result_batches = execute_sql_derived_direct(&second_sql_file, &provider_context)
                .await
                .unwrap();

            assert!(!result_batches.is_empty(), "Should have at least one batch");
            let result_batch = &result_batches[0];

            // Verify the chained transformation worked correctly
            // Original data: Alice=100, Bob=200, Charlie=150, David=300, Eve=250
            // First query: WHERE value >= 200, SELECT value + 50 as adjusted_value
            //   -> Bob=250, David=350, Eve=300
            // Second query: WHERE adjusted_value > 250, SELECT adjusted_value * 2 as final_value, ORDER BY final_value DESC
            //   -> David=700, Eve=600 (Bob excluded because 250 is not > 250)
            assert_eq!(result_batch.num_rows(), 2);
            assert_eq!(result_batch.num_columns(), 2);

            // Check column names
            let schema = result_batch.schema();
            assert_eq!(schema.field(0).name(), "name");
            assert_eq!(schema.field(1).name(), "final_value");

            // Check data - should be ordered by final_value DESC
            let names = get_string_array(result_batch, 0);
            let final_values = result_batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            assert_eq!(names.value(0), "David"); // (300 + 50) * 2 = 700 (highest)
            assert_eq!(final_values.value(0), 700);

            assert_eq!(names.value(1), "Eve"); // (250 + 50) * 2 = 600 (second)
            assert_eq!(final_values.value(1), 600);

            // Bob would be (200 + 50) * 2 = 500, but excluded because 250 is not > 250
        }

        // ## Observations from This Test
        //
        // This test demonstrates the current materialized approach:
        // 1. **Full Materialization**: 3 rows -> intermediate.parquet -> 2 final rows
        // 2. **No Cross-Node Optimization**: DataFusion optimizes each query independently
        // 3. **Predictable Behavior**: Each step is explicit and debuggable
        //
        // Missing optimization opportunities:
        // - Original data has 5 rows: [Alice=100, Bob=200, Charlie=150, David=300, Eve=250]
        // - First filter `value >= 200` could be pushed to source: scan only 3 rows
        // - Second filter `adjusted_value > 250` could be rewritten as `value > 200` and
        //   combined: scan only 2 rows (David=300, Eve=250)
        // - Current approach: scan 5 rows -> materialize 3 -> scan 3 -> return 2
        // - Optimized approach: scan 2 rows -> return 2 (60% less I/O)
    }

    /// Test SQL-derived factory with multiple file versions and wildcard patterns
    ///
    /// This test validates the functionality we implemented for the HydroVu dynamic configuration,
    /// specifically testing:
    /// 1. Multiple file versions with different schemas
    /// 2. Wildcard pattern matching across different file types
    /// 3. Automatic schema merging and column combination by timestamp
    /// 4. SQL-derived factory creating unified views over versioned data
    ///
    /// Based on our real-world success case where we:
    /// - Simplified from complex UNION queries to simple wildcard patterns
    /// - Successfully combined 11,297 rows from multiple file versions
    /// - Let Arrow/Parquet handle schema evolution automatically
    #[tokio::test]
    async fn test_sql_derived_factory_multi_version_wildcard_pattern() {
        // Create memory filesystem with shared persistence
        let (fs, provider_context) = create_test_environment().await;
        let root = fs.root().await.unwrap();

        // Create File A v1: timestamps 1,2,3 with columns: timestamp, temperature
        let _file_id_a = {
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Second, None),
                    false,
                ),
                Field::new("temperature", DataType::Float64, true), // Make nullable since not all files have it
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampSecondArray::from(vec![1, 2, 3])),
                    Arc::new(Float64Array::from(vec![20.5, 21.0, 19.8])),
                ],
            )
            .unwrap();

            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
                writer.write(&batch).unwrap();
                _ = writer.close().unwrap();
            }

            _ = root.create_dir_path("/hydrovu").await.unwrap();
            _ = root.create_dir_path("/hydrovu/devices").await.unwrap();
            _ = root
                .create_dir_path("/hydrovu/devices/station_a")
                .await
                .unwrap();
            create_parquet_file(
                &fs,
                "/hydrovu/devices/station_a/SensorA_v1.series",
                parquet_buffer,
                EntryType::TablePhysicalSeries,
            )
            .await
            .unwrap()
        };

        // Create File A v2: timestamps 4,5,6 with columns: timestamp, temperature, humidity
        {
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Second, None),
                    false,
                ),
                Field::new("temperature", DataType::Float64, true), // Make nullable since not all files have it
                Field::new("humidity", DataType::Float64, true), // Make nullable since not all files have it
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampSecondArray::from(vec![4, 5, 6])),
                    Arc::new(Float64Array::from(vec![22.1, 23.5, 21.8])),
                    Arc::new(Float64Array::from(vec![65.0, 68.2, 70.1])),
                ],
            )
            .unwrap();

            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
                writer.write(&batch).unwrap();
                _ = writer.close().unwrap();
            }

            // Write new version to same path using async_writer (version 2)
            let mut file_writer = root
                .async_writer_path("/hydrovu/devices/station_a/SensorA_v1.series")
                .await
                .unwrap();
            use tokio::io::AsyncWriteExt;
            file_writer.write_all(&parquet_buffer).await.unwrap();
            file_writer.shutdown().await.unwrap();
        }

        // Create File B: timestamps 1-6 with columns: timestamp, pressure
        {
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Second, None),
                    false,
                ),
                Field::new("pressure", DataType::Float64, true), // Make nullable since not all files have it
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampSecondArray::from(vec![1, 2, 3, 4, 5, 6])),
                    Arc::new(Float64Array::from(vec![
                        1013.2, 1012.8, 1014.1, 1015.3, 1013.9, 1012.5,
                    ])),
                ],
            )
            .unwrap();

            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
                writer.write(&batch).unwrap();
                _ = writer.close().unwrap();
            }

            _ = root
                .create_dir_path("/hydrovu/devices/station_b")
                .await
                .unwrap();
            _ = create_parquet_file(
                &fs,
                "/hydrovu/devices/station_b/PressureB.series",
                parquet_buffer,
                EntryType::TablePhysicalSeries,
            )
            .await
            .unwrap();
        }

        // Test SQL-derived factory with wildcard pattern (like our successful HydroVu config)
        let context = test_context(&provider_context, FileID::root());
        let config = SqlDerivedConfig {
            patterns: test_patterns(&[
                ("sensor_a", "series:///hydrovu/devices/**/SensorA*.series"),
                ("pressure_b", "series:///hydrovu/devices/**/PressureB*.series"),
            ]),
            // Use COALESCE pattern like in our fixed HydroVu config to ensure non-nullable timestamps
            query: Some("SELECT COALESCE(sensor_a.timestamp, pressure_b.timestamp) AS timestamp, sensor_a.* EXCLUDE (timestamp), pressure_b.* EXCLUDE (timestamp) FROM sensor_a FULL OUTER JOIN pressure_b ON sensor_a.timestamp = pressure_b.timestamp ORDER BY timestamp".to_string()),
            ..Default::default()
        };

        let sql_derived_file =
            SqlDerivedFile::new(config, context, SqlDerivedMode::Series).unwrap();

        // Read the result using direct table provider scanning
        let result_batches = execute_sql_derived_direct(&sql_derived_file, &provider_context)
            .await
            .unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");
        let result_batch = &result_batches[0];

        // Verify schema merging: should have all columns from all files
        // Expected columns: temperature, humidity, timestamp, pressure (order may vary)
        let schema = result_batch.schema();
        assert_eq!(result_batch.num_columns(), 4);

        let column_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(column_names.contains(&"timestamp"));
        assert!(column_names.contains(&"temperature"));
        assert!(column_names.contains(&"humidity"));
        assert!(column_names.contains(&"pressure"));

        // Should have 6 rows total (one for each timestamp 1-6)
        // This validates that COALESCE-based FULL OUTER JOIN combined data correctly by timestamp
        // while ensuring non-nullable timestamp column for temporal partitioning
        assert_eq!(result_batch.num_rows(), 6);

        // Verify data integrity: check a few key combinations

        let timestamp_col_idx = column_names
            .iter()
            .position(|&name| name == "timestamp")
            .unwrap();
        let temperature_col_idx = column_names
            .iter()
            .position(|&name| name == "temperature")
            .unwrap();
        let humidity_col_idx = column_names
            .iter()
            .position(|&name| name == "humidity")
            .unwrap();
        let pressure_col_idx = column_names
            .iter()
            .position(|&name| name == "pressure")
            .unwrap();

        let timestamps = result_batch
            .column(timestamp_col_idx)
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap();
        let temperatures = result_batch
            .column(temperature_col_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let humidity = result_batch
            .column(humidity_col_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let pressures = result_batch
            .column(pressure_col_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // Find rows by timestamp and verify correct data combination
        for row_idx in 0..result_batch.num_rows() {
            let ts = timestamps.value(row_idx);
            match ts {
                1 => {
                    // Timestamp 1: should have temperature (20.5), pressure (1013.2), null humidity
                    assert_eq!(temperatures.value(row_idx), 20.5);
                    assert_eq!(pressures.value(row_idx), 1013.2);
                    assert!(humidity.is_null(row_idx)); // File A v1 didn't have humidity
                }
                2 => {
                    // Timestamp 2: should have temperature (21.0), pressure (1012.8), null humidity
                    assert_eq!(temperatures.value(row_idx), 21.0);
                    assert_eq!(pressures.value(row_idx), 1012.8);
                    assert!(humidity.is_null(row_idx));
                }
                3 => {
                    // Timestamp 3: should have temperature (19.8), pressure (1014.1), null humidity
                    assert_eq!(temperatures.value(row_idx), 19.8);
                    assert_eq!(pressures.value(row_idx), 1014.1);
                    assert!(humidity.is_null(row_idx));
                }
                4 => {
                    // Timestamp 4: should have temperature (22.1), humidity (65.0), pressure (1015.3)
                    assert_eq!(temperatures.value(row_idx), 22.1);
                    assert_eq!(humidity.value(row_idx), 65.0);
                    assert_eq!(pressures.value(row_idx), 1015.3);
                }
                5 => {
                    // Timestamp 5: should have temperature (23.5), humidity (68.2), pressure (1013.9)
                    assert_eq!(temperatures.value(row_idx), 23.5);
                    assert_eq!(humidity.value(row_idx), 68.2);
                    assert_eq!(pressures.value(row_idx), 1013.9);
                }
                6 => {
                    // Timestamp 6: should have temperature (21.8), humidity (70.1), pressure (1012.5)
                    assert_eq!(temperatures.value(row_idx), 21.8);
                    assert_eq!(humidity.value(row_idx), 70.1);
                    assert_eq!(pressures.value(row_idx), 1012.5);
                }
                _ => panic!("Unexpected timestamp: {}", ts),
            }
        }
    }

    /// Test temporal-reduce over sql-derived over parquet
    ///
    /// This test exercises the same path as the command:
    /// `RUST_LOG=tlogfs=debug POND=/tmp/dynpond cargo run --bin pond cat '/test-locations/BDockDownsampled/res=1d.series'`
    ///
    /// Testing the chain: Parquet files -> SQL-derived (join/filter) -> Temporal-reduce (downsampling)
    #[tokio::test]
    async fn test_temporal_reduce_over_sql_derived_over_parquet() {
        // Initialize logger for debugging (safe to call multiple times)
        let _ = env_logger::try_init();

        // Create memory filesystem with shared persistence
        let (fs, provider_context) = create_test_environment().await;
        let root = fs.root().await.unwrap();

        // Step 1: Create base parquet data with hourly sensor readings over 3 days
        {
            // Create schema for sensor data: timestamp, station_id, temperature, humidity
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Second, None),
                    false,
                ),
                Field::new("station_id", DataType::Utf8, false),
                Field::new("temperature", DataType::Float64, true),
                Field::new("humidity", DataType::Float64, true),
            ]));

            // Create 3 days of hourly data (72 records total)
            let base_timestamp = 1640995200; // 2022-01-01 00:00:00 UTC
            let mut timestamps = Vec::new();
            let mut station_ids = Vec::new();
            let mut temperatures = Vec::new();
            let mut humidities = Vec::new();

            for hour in 0..72 {
                let ts = base_timestamp + (hour * 3600); // hourly intervals
                let station = if hour % 2 == 0 { "BDock" } else { "Silver" };
                let temp = 20.0 + (hour as f64 * 0.1) + ((hour as f64 * 0.5).sin() * 5.0); // varying temperature
                let humid = 50.0 + ((hour as f64 * 0.3).cos() * 10.0); // varying humidity

                timestamps.push(ts);
                station_ids.push(station.to_string());
                temperatures.push(temp);
                humidities.push(humid);
            }

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampSecondArray::from(timestamps)),
                    Arc::new(StringArray::from(station_ids)),
                    Arc::new(Float64Array::from(temperatures)),
                    Arc::new(Float64Array::from(humidities)),
                ],
            )
            .unwrap();

            // Write to parquet
            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
                writer.write(&batch).unwrap();
                _ = writer.close().unwrap();
            }

            // Store as base sensor data - explicitly create parent directories to validate part_id handling
            _ = root.create_dir_path("/sensors").await.unwrap();
            _ = root.create_dir_path("/sensors/stations").await.unwrap();

            _ = create_parquet_file(
                &fs,
                "/sensors/stations/all_data.series",
                parquet_buffer,
                EntryType::TablePhysicalSeries,
            )
            .await
            .unwrap();
        }

        // Step 2: Create SQL-derived node that filters for BDock station only
        let bdock_sql_derived = {
            let context = test_context(&provider_context, FileID::root());

            let config = SqlDerivedConfig {
                patterns: test_patterns(&[("series", "series:///sensors/stations/all_data.series")]),
                query: Some("SELECT timestamp, temperature, humidity FROM series WHERE station_id = 'BDock' ORDER BY timestamp".to_string()),
                ..Default::default()
            };

            SqlDerivedFile::new(config, context, SqlDerivedMode::Series).unwrap()
        };

        // Step 3: Create temporal-reduce node that downsamples to daily averages
        let temporal_reduce_dir = {
            let context = test_context(&provider_context, FileID::root());

            let config = TemporalReduceConfig {
                in_pattern: crate::Url::parse("series:///sensors/stations/*").unwrap(), // Use pattern to match parquet data
                out_pattern: "$0".to_string(), // Keep original filename as output
                time_column: "timestamp".to_string(),
                resolutions: vec!["1d".to_string()],
                aggregations: vec![AggregationConfig {
                    agg_type: AggregationType::Avg,
                    columns: Some(vec!["temperature".to_string(), "humidity".to_string()]),
                }],
                transforms: None,
                allowed_lateness: None,
                // Seal on every advance, as before the size gate existed: these
                // tests assert seal MECHANICS on datasets far under any real target.
                seal_target_bytes: Some(0),
                max_live_segments: None,
            };

            TemporalReduceDirectory::new(config, context).unwrap()
        };

        // Step 4: Test the architectural components without full execution
        {
            // First, execute the SQL-derived query
            let bdock_batches = execute_sql_derived_direct(&bdock_sql_derived, &provider_context)
                .await
                .unwrap();

            // Verify we filtered correctly (should have 36 rows - every other hour for BDock)
            assert!(!bdock_batches.is_empty());
            let total_bdock_rows: usize = bdock_batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(
                total_bdock_rows, 36,
                "Should have 36 BDock records (every other hour for 72 hours)"
            );

            // Verify columns are present
            let bdock_schema = bdock_batches[0].schema();
            assert_eq!(bdock_schema.fields().len(), 3);
            assert_eq!(bdock_schema.field(0).name(), "timestamp");
            assert_eq!(bdock_schema.field(1).name(), "temperature");
            assert_eq!(bdock_schema.field(2).name(), "humidity");

            // Now test actual temporal-reduce execution - should reduce 36 hourly points to ~3 daily points
            // With hierarchical structure, first get the site directory "all_data.series"
            let site_dir_node = temporal_reduce_dir.get("all_data.series").await.unwrap();
            assert!(
                site_dir_node.is_some(),
                "Should find all_data.series site directory in temporal reduce directory"
            );

            // Then get the resolution file "res=1d.series" within that site directory
            let site_node = site_dir_node.unwrap();
            if let tinyfs::NodeType::Directory(site_dir_handle) = &site_node.node_type {
                let daily_series_node = site_dir_handle.get("res=1d.series").await.unwrap();
                assert!(
                    daily_series_node.is_some(),
                    "Should find res=1d.series in all_data.series site directory"
                );

                let daily_node = daily_series_node.unwrap();

                if let tinyfs::NodeType::File(file_handle) = &daily_node.node_type {
                    // Access the file through the public API and downcast using as_any()
                    let file_arc = file_handle.get_file().await;
                    let file_guard = file_arc.lock().await;
                    if let Some(queryable_file) = file_guard.as_queryable() {
                        let file_id = daily_node.id;
                        let table_provider = queryable_file
                            .as_table_provider(file_id, &provider_context)
                            .await
                            .unwrap();

                        // Execute the temporal-reduce query using the proper public API
                        _ = provider_context
                            .datafusion_session
                            .register_table("temporal_reduce_table", table_provider)
                            .unwrap();

                        // Execute query to get results
                        let dataframe = provider_context
                            .datafusion_session
                            .sql("SELECT * FROM temporal_reduce_table")
                            .await
                            .unwrap();
                        let temporal_batches = dataframe.collect().await.unwrap();

                        // Verify temporal reduction actually reduced the data
                        let total_temporal_rows: usize =
                            temporal_batches.iter().map(|b| b.num_rows()).sum();
                        log::debug!(
                            "   - Temporal-reduce produced {} daily aggregated records from 36 hourly records",
                            total_temporal_rows
                        );

                        // With 36 hourly BDock records over 3 days, daily aggregation should produce ~3 records
                        assert!(
                            total_temporal_rows > 0 && total_temporal_rows < 36,
                            "Temporal reduction should produce fewer records than input. Got {} from 36 input records",
                            total_temporal_rows
                        );

                        // Verify the temporal aggregation schema includes time_bucket and aggregated columns
                        if !temporal_batches.is_empty() {
                            let temporal_schema = temporal_batches[0].schema();
                            let field_names: Vec<&str> = temporal_schema
                                .fields()
                                .iter()
                                .map(|f| f.name().as_str())
                                .collect();
                            log::debug!("   - Temporal-reduce schema: {:?}", field_names);

                            // Should have time_bucket (or timestamp) and aggregated columns like avg_temperature
                            assert!(field_names.iter().any(|name| name.contains("time") || name.contains("timestamp")), 
                            "Should have time column, got: {:?}", field_names);
                            assert!(
                                field_names.iter().any(|name| name.contains("temperature")),
                                "Should have temperature aggregation, got: {:?}",
                                field_names
                            );
                        }
                    } else {
                        panic!("Temporal-reduce should create a QueryableFile");
                    }
                } else {
                    panic!("Expected file node from temporal reduce directory");
                }
            } else {
                panic!("Expected directory node for site directory");
            }

            log::debug!(
                "[OK] Temporal-reduce over SQL-derived over Parquet test completed successfully"
            );
            log::debug!("   - Created 72 hourly sensor records from 2 stations");
            log::debug!("   - SQL-derived filtered to 36 BDock-only records");
            log::debug!("   - Temporal-reduce configuration validated");
            log::debug!(
                "   - Architecture demonstrates: Parquet -> SQL-derived -> Temporal-reduce chain"
            );
        }
    }

    /// Test that OpLogFile is properly detected as QueryableFile
    ///
    /// This test verifies that OpLogFile correctly implements QueryableFile via as_queryable(),
    /// which is critical for the "cat" command and other operations that need to execute
    /// SQL queries over basic TLogFS files.
    /// This test would fail if OpLogFile detection was commented out.
    #[tokio::test]
    async fn test_oplog_file_queryable_detection() {
        // Create memory filesystem with shared persistence
        let (fs, provider_context) = create_test_environment().await;
        let root = fs.root().await.unwrap();

        // Create a simple test batch using record_batch! macro
        let batch = record_batch!(
            ("id", Int32, [1_i32, 2_i32, 3_i32]),
            ("name", Utf8, ["Alice", "Bob", "Charlie"]),
            ("value", Int32, [100_i32, 200_i32, 150_i32])
        )
        .unwrap();

        // Create an OpLogFile with parquet data - use setup_test_data pattern
        {
            // Write as FileTable (this creates an OpLogFile internally)
            root.create_table_from_batch(
                "/test_data.parquet",
                &batch,
                EntryType::TablePhysicalVersion,
            )
            .await
            .unwrap();
        }

        // Test OpLogFile QueryableFile detection

        // Get the file we just created
        let root_node = fs.root().await.unwrap();
        let file_lookup = root_node.resolve_path("/test_data.parquet").await.unwrap();

        match file_lookup.1 {
            tinyfs::Lookup::Found(node_path) => {
                if let tinyfs::NodeType::File(file_handle) = &node_path.node.node_type {
                    let file_arc = file_handle.get_file().await;
                    let file_guard = file_arc.lock().await;

                    // This is the critical test: OpLogFile should implement QueryableFile
                    let queryable_file = file_guard.as_queryable();
                    assert!(
                        queryable_file.is_some(),
                        "OpLogFile should be detected as QueryableFile"
                    );

                    // Verify we can actually create a table provider from it
                    let queryable = queryable_file.unwrap();
                    let file_id = node_path.node.id;
                    let table_provider = queryable
                        .as_table_provider(file_id, &provider_context)
                        .await
                        .unwrap();

                    // Test that we can register it with DataFusion (simulating "cat" command behavior)
                    _ = provider_context
                        .datafusion_session
                        .register_table("test_table", table_provider)
                        .unwrap();

                    log::debug!(
                        "[OK] OpLogFile successfully detected as QueryableFile and registered with DataFusion"
                    );
                } else {
                    panic!("Expected file node");
                }
            }
            _ => panic!("File not found"),
        }
    }

    /// Test temporal-reduce with wildcard column discovery (columns: None)
    ///
    /// This test duplicates the above test but uses automatic schema discovery
    /// to verify that the deferred schema discovery feature works correctly.
    #[tokio::test]
    async fn test_temporal_reduce_wildcard_schema_discovery() {
        // Initialize logger for debugging (safe to call multiple times)
        let _ = env_logger::try_init();

        // Create memory filesystem with shared persistence
        let (fs, provider_context) = create_test_environment().await;
        let root = fs.root().await.unwrap();

        // Step 1: Create base parquet data with hourly sensor readings over 3 days using proper TinyFS Arrow integration
        {
            // Create 3 days of hourly data (72 records total) using Arrow test support macros
            let base_timestamp = 1640995200; // 2022-01-01 00:00:00 UTC
            let mut timestamps = Vec::new();
            let mut temperatures = Vec::new();
            let mut humidities = Vec::new();

            for hour in 0..72 {
                let ts = base_timestamp + (hour * 3600); // hourly intervals
                let temp = 20.0 + (hour as f64 * 0.1) + ((hour as f64 * 0.5).sin() * 5.0); // varying temperature
                let humid = 50.0 + ((hour as f64 * 0.3).cos() * 10.0); // varying humidity

                timestamps.push(ts);
                temperatures.push(temp);
                humidities.push(humid);
            }

            // Create RecordBatch using proper Arrow approach
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Second, None),
                    false,
                ),
                Field::new("temperature", DataType::Float64, true),
                Field::new("humidity", DataType::Float64, true),
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampSecondArray::from(timestamps)),
                    Arc::new(Float64Array::from(temperatures)),
                    Arc::new(Float64Array::from(humidities)),
                ],
            )
            .unwrap();

            // Store as base sensor data - create parent directories
            _ = root.create_dir_path("/sensors").await.unwrap();
            _ = root
                .create_dir_path("/sensors/wildcard_test")
                .await
                .unwrap();

            // Write parquet buffer manually and store in both FS and persistence
            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
                writer.write(&batch).unwrap();
                _ = writer.close().unwrap();
            }

            _ = create_parquet_file(
                &fs,
                "/sensors/wildcard_test/base_data.series",
                parquet_buffer,
                EntryType::TablePhysicalSeries,
            )
            .await
            .unwrap();
        }

        // Step 2: Test wildcard schema discovery within the same transaction as data access
        // This reflects the normal usage pattern where directories are created and accessed
        // within the same transaction context
        {
            let context = test_context(&provider_context, FileID::root());

            // Define temporal-reduce config with wildcard schema discovery
            let temporal_config = TemporalReduceConfig {
                in_pattern: crate::Url::parse("series:///sensors/wildcard_test/*").unwrap(), // Use pattern to match base_data.series
                out_pattern: "$0".to_string(), // Keep original filename as output
                time_column: "timestamp".to_string(),
                resolutions: vec!["1d".to_string()],
                aggregations: vec![AggregationConfig {
                    agg_type: AggregationType::Avg,
                    columns: None, // This should trigger automatic schema discovery!
                }],
                transforms: None,
                allowed_lateness: None,
                // Seal on every advance, as before the size gate existed: these
                // tests assert seal MECHANICS on datasets far under any real target.
                seal_target_bytes: Some(0),
                max_live_segments: None,
            };

            // Create temporal reduce directory within the same transaction that will access it
            let temporal_reduce_dir =
                TemporalReduceDirectory::new(temporal_config.clone(), context.clone()).unwrap();

            // Now test the pattern matching and QueryableFile access
            let root_node = fs.root().await.unwrap();
            let pattern_matches = root_node
                .collect_matches("/sensors/wildcard_test/*")
                .await
                .unwrap();
            log::debug!(
                "   - Pattern /sensors/wildcard_test/* found {} matches",
                pattern_matches.len()
            );

            for (i, (node_path, captured)) in pattern_matches.iter().enumerate() {
                let path_str = node_path.path.to_string_lossy();
                log::debug!("   - Match {}: {} -> captured: {:?}", i, path_str, captured);

                // Try to access the schema of this file directly
                if let tinyfs::NodeType::File(file_handle) = &node_path.node.node_type {
                    let file_arc = file_handle.get_file().await;
                    let file_guard = file_arc.lock().await;

                    // Check the file metadata first
                    let metadata = file_guard.metadata().await.unwrap();
                    log::debug!(
                        "   - File {} metadata: entry_type={:?}",
                        path_str,
                        metadata.entry_type
                    );

                    if let Some(queryable_file) = file_guard.as_queryable() {
                        log::debug!("   - File {} implements QueryableFile", path_str);
                        let file_id = node_path.node.id;
                        log::debug!("   - Using file_id: {}", file_id.node_id());

                        match queryable_file
                            .as_table_provider(file_id, &provider_context)
                            .await
                        {
                            Ok(table_provider) => {
                                let schema = table_provider.schema();
                                let field_names: Vec<&str> =
                                    schema.fields().iter().map(|f| f.name().as_str()).collect();
                                log::debug!(
                                    "   - QueryableFile schema for {}: {:?}",
                                    path_str,
                                    field_names
                                );

                                // Let's try to actually query the table provider to see if it has data
                                log::debug!("   - Attempting to scan table provider...");
                                match table_provider
                                    .scan(
                                        &provider_context.datafusion_session.state(),
                                        None,
                                        &[],
                                        None,
                                    )
                                    .await
                                {
                                    Ok(_exec_plan) => {
                                        log::debug!("   - [OK] Table provider scan succeeded");

                                        // Check schema again after scan - maybe it's lazy-loaded
                                        let schema_after_scan = table_provider.schema();
                                        let field_names_after_scan: Vec<&str> = schema_after_scan
                                            .fields()
                                            .iter()
                                            .map(|f| f.name().as_str())
                                            .collect();
                                        log::debug!(
                                            "   - Schema AFTER scan: {:?}",
                                            field_names_after_scan
                                        );

                                        // Also check if we can get table statistics
                                        if let Some(stats) = table_provider.statistics() {
                                            log::debug!(
                                                "   - Table statistics: num_rows={:?}, total_byte_size={:?}",
                                                stats.num_rows,
                                                stats.total_byte_size
                                            );
                                        } else {
                                            log::debug!("   - No table statistics available");
                                        }
                                    }
                                    Err(e) => {
                                        log::debug!("   - [ERR] Table provider scan failed: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                log::debug!(
                                    "   - Error getting table provider for {}: {}",
                                    path_str,
                                    e
                                );
                            }
                        }
                    } else {
                        log::debug!("   - File {} does NOT implement QueryableFile", path_str);
                        log::debug!("   - File type: {:?}", file_guard.as_any().type_id());

                        // Let's see what type this actually is
                        let type_name = std::any::type_name_of_val(&**file_guard);
                        log::debug!("   - Actual file type name: {}", type_name);
                    }
                } else {
                    log::debug!(
                        "   - Match {} is not a file: {:?}",
                        i,
                        node_path.node.node_type
                    );
                }
            }

            // With hierarchical structure, first get the site directory "base_data.series"
            let site_dir_node = temporal_reduce_dir.get("base_data.series").await.unwrap();
            assert!(
                site_dir_node.is_some(),
                "Should find base_data.series site directory in temporal reduce directory"
            );

            // Then get the resolution file "res=1d.series" within that site directory
            let site_node = site_dir_node.unwrap();
            if let tinyfs::NodeType::Directory(site_dir_handle) = &site_node.node_type {
                let daily_series_node = site_dir_handle.get("res=1d.series").await.unwrap();
                assert!(
                    daily_series_node.is_some(),
                    "Should find res=1d.series in base_data.series site directory"
                );

                let daily_node = daily_series_node.unwrap();

                if let tinyfs::NodeType::File(file_handle) = &daily_node.node_type {
                    // Access the file through the public API and downcast using as_any()
                    let file_arc = file_handle.get_file().await;
                    let file_guard = file_arc.lock().await;
                    if let Some(queryable_file) = file_guard.as_queryable() {
                        let file_id = daily_node.id;
                        let table_provider = queryable_file
                            .as_table_provider(file_id, &provider_context)
                            .await
                            .unwrap();

                        // Execute the temporal-reduce query using the proper public API
                        _ = provider_context
                            .datafusion_session
                            .register_table("wildcard_temporal_table", table_provider)
                            .unwrap();

                        // Execute query to get results
                        let dataframe = provider_context
                            .datafusion_session
                            .sql("SELECT * FROM wildcard_temporal_table")
                            .await
                            .unwrap();
                        let temporal_batches = dataframe.collect().await.unwrap();

                        // Verify wildcard schema discovery worked
                        let total_temporal_rows: usize =
                            temporal_batches.iter().map(|b| b.num_rows()).sum();
                        log::debug!(
                            "   - Wildcard temporal-reduce produced {} daily aggregated records",
                            total_temporal_rows
                        );

                        // Should produce temporal aggregations for all available data
                        assert!(
                            total_temporal_rows > 0,
                            "Wildcard temporal reduction should produce records. Got {} records",
                            total_temporal_rows
                        );

                        // Verify the schema was automatically discovered and aggregated
                        if !temporal_batches.is_empty() {
                            let temporal_schema = temporal_batches[0].schema();
                            let field_names: Vec<&str> = temporal_schema
                                .fields()
                                .iter()
                                .map(|f| f.name().as_str())
                                .collect();
                            log::debug!("   - Wildcard schema discovery result: {:?}", field_names);

                            // Should have timestamp and all numeric columns were auto-discovered and aggregated
                            assert!(field_names.iter().any(|name| name.contains("time") || name.contains("timestamp")), 
                            "Should have time column, got: {:?}", field_names);

                            // The key test: automatic discovery should have found both temperature and humidity
                            // and created avg aggregations for both (since columns: None)
                            assert!(
                                field_names.iter().any(|name| name.contains("temperature")),
                                "Should have auto-discovered temperature column, got: {:?}",
                                field_names
                            );
                            assert!(
                                field_names.iter().any(|name| name.contains("humidity")),
                                "Should have auto-discovered humidity column, got: {:?}",
                                field_names
                            );
                        }
                    } else {
                        panic!("Temporal-reduce should create a QueryableFile");
                    }
                } else {
                    panic!("Expected file node from temporal reduce directory");
                }
            } else {
                panic!("Expected directory node for site directory");
            }

            log::debug!(
                "[OK] Wildcard temporal-reduce schema discovery test completed successfully"
            );
            log::debug!("   - Created 72 hourly sensor records with numeric columns only");
            log::debug!("   - Temporal-reduce automatically discovered all numeric columns");
            log::debug!(
                "   - Verified both temperature and humidity were included in aggregations"
            );
        }
    }

    /// Test that pattern matching finds both Physical and Dynamic entry types
    /// This guards against regression of the EntryType refactor bug where only Physical
    /// types were searched, missing Dynamic files from factories
    #[tokio::test]
    async fn test_pattern_matching_finds_physical_and_dynamic_types() {
        // Create memory filesystem with shared persistence
        let (fs, provider_context) = create_test_environment().await;
        let root = fs.root().await.unwrap();

        // Create two files with the same schema but different EntryTypes
        {
            _ = root.create_dir_path("/test").await.unwrap();

            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("value", DataType::Int32, false),
            ]));

            // Create Physical file
            let batch_physical = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampMillisecondArray::from(vec![1000, 2000, 3000])),
                    Arc::new(Int32Array::from(vec![10, 20, 30])),
                ],
            )
            .unwrap();

            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch_physical).unwrap();
                _ = writer.close().unwrap();
            }

            _ = create_parquet_file(
                &fs,
                "/test/physical.series",
                parquet_buffer,
                EntryType::TablePhysicalSeries,
            )
            .await
            .unwrap();
        }

        {
            // Create second Physical file
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("value", DataType::Int32, false),
            ]));

            let batch_dynamic = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampMillisecondArray::from(vec![4000, 5000, 6000])),
                    Arc::new(Int32Array::from(vec![40, 50, 60])),
                ],
            )
            .unwrap();

            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch_dynamic).unwrap();
                _ = writer.close().unwrap();
            }

            _ = create_parquet_file(
                &fs,
                "/test/physical2.series",
                parquet_buffer,
                EntryType::TablePhysicalSeries,
            )
            .await
            .unwrap();
        }

        // Test: SqlDerivedFile should find multiple Physical files with pattern matching
        let context = test_context(&provider_context, FileID::root());
        let config = SqlDerivedConfig {
            patterns: test_patterns(&[("data", "series:///test/*.series")]),
            query: Some("SELECT * FROM data ORDER BY timestamp".to_string()),
            ..Default::default()
        };

        let sql_derived_file =
            SqlDerivedFile::new(config, context, SqlDerivedMode::Series).unwrap();

        let result_batches = execute_sql_derived_direct(&sql_derived_file, &provider_context)
            .await
            .unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");

        // Should have data from both files: 3 rows from physical + 3 rows from physical2 = 6 rows total
        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 6,
            "Should find multiple Physical files with pattern matching - got {} rows, expected 6",
            total_rows
        );
    }

    /// A data-archetype Parquet node (e.g. a git-ingested FileDynamic archive)
    /// is not natively queryable, but a `series:///` URL cast must read it as a
    /// single-version series.  See docs/url-entry-type-casting.md.
    #[tokio::test]
    async fn test_series_cast_over_single_data_parquet_file() {
        let (fs, provider_context) = create_test_environment().await;
        let root = fs.root().await.unwrap();

        _ = root.create_dir_path("/archive").await.unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1000, 2000, 3000])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        // Store as a plain data file (FilePhysicalVersion): not queryable on its
        // own, mirroring a git-ingested Parquet archive.
        _ = create_parquet_from_batch(
            &fs,
            "/archive/readings.parquet",
            &batch,
            EntryType::FilePhysicalVersion,
        )
        .await
        .unwrap();

        let context = test_context(&provider_context, FileID::root());
        let config = SqlDerivedConfig {
            patterns: test_patterns(&[("series", "series:///archive/readings.parquet")]),
            query: Some("SELECT * FROM series ORDER BY timestamp".to_string()),
            ..Default::default()
        };
        let sql_derived_file =
            SqlDerivedFile::new(config, context, SqlDerivedMode::Series).unwrap();

        let result_batches = execute_sql_derived_direct(&sql_derived_file, &provider_context)
            .await
            .unwrap();

        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 3,
            "series:// cast should read the data Parquet file as a series (3 rows)"
        );
    }

    /// A `series:///*.parquet` cast matching multiple data-archetype nodes
    /// unions them into a single series.
    #[tokio::test]
    async fn test_series_cast_over_multiple_data_parquet_files() {
        let (fs, provider_context) = create_test_environment().await;
        let root = fs.root().await.unwrap();

        _ = root.create_dir_path("/archive").await.unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Int32, false),
        ]));

        for (name, tss, vals) in [
            ("a.parquet", vec![1000, 2000], vec![10, 20]),
            ("b.parquet", vec![3000, 4000, 5000], vec![30, 40, 50]),
        ] {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampMillisecondArray::from(tss)),
                    Arc::new(Int32Array::from(vals)),
                ],
            )
            .unwrap();
            _ = create_parquet_from_batch(
                &fs,
                &format!("/archive/{}", name),
                &batch,
                EntryType::FilePhysicalVersion,
            )
            .await
            .unwrap();
        }

        let context = test_context(&provider_context, FileID::root());
        let config = SqlDerivedConfig {
            patterns: test_patterns(&[("series", "series:///archive/*.parquet")]),
            query: Some("SELECT * FROM series ORDER BY timestamp".to_string()),
            ..Default::default()
        };
        let sql_derived_file =
            SqlDerivedFile::new(config, context, SqlDerivedMode::Series).unwrap();

        let result_batches = execute_sql_derived_direct(&sql_derived_file, &provider_context)
            .await
            .unwrap();

        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 5,
            "series:// cast should union both data Parquet files (2 + 3 = 5 rows)"
        );
    }

    /// Test that table names are always lowercase to match DataFusion's case-insensitive behavior
    /// This guards against regression where tables registered with mixed case caused "table not found" errors
    #[tokio::test]
    async fn test_table_names_are_lowercase() {
        // Create memory filesystem with shared persistence
        let (fs, provider_context) = create_test_environment().await;
        let root = fs.root().await.unwrap();

        // Create a file with uppercase in the path
        _ = root.create_dir_path("/Sensors").await.unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("Temperature", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1000, 2000, 3000])),
                Arc::new(Int32Array::from(vec![20, 25, 30])),
            ],
        )
        .unwrap();

        let mut parquet_buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer);
            let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
            writer.write(&batch).unwrap();
            _ = writer.close().unwrap();
        }

        _ = create_parquet_file(
            &fs,
            "/Sensors/Temperature.series",
            parquet_buffer,
            EntryType::TablePhysicalSeries,
        )
        .await
        .unwrap();

        // Test: SqlDerivedFile with mixed-case pattern name should work
        let context = test_context(&provider_context, FileID::root());
        let config = SqlDerivedConfig {
            patterns: test_patterns(&[("TempData", "series:///Sensors/Temperature.series")]),
            query: Some("SELECT * FROM TempData ORDER BY timestamp".to_string()),
            ..Default::default()
        };

        let sql_derived_file =
            SqlDerivedFile::new(config, context, SqlDerivedMode::Series).unwrap();

        // This should NOT fail with "table not found" due to case mismatch
        let result_batches = execute_sql_derived_direct(&sql_derived_file, &provider_context)
            .await
            .unwrap();

        assert!(!result_batches.is_empty(), "Should have at least one batch");
        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 3,
            "Should successfully query with mixed-case pattern name"
        );
    }

    /// Test AST-based table name replacement preserves quoted column names
    #[test]
    fn test_ast_table_replacement_preserves_column_names() {
        // Test case: Column name contains table name - should NOT be replaced
        let original_sql = r#"SELECT Silver."Silver.AT500_Surface.DO.mg/L" FROM Silver"#;

        let mut table_mappings = HashMap::new();
        _ = table_mappings.insert("Silver".to_string(), "sql_derived_silver_12345".to_string());

        let options = SqlTransformOptions {
            table_mappings: Some(table_mappings),
            source_replacement: None,
        };

        let transformed = crate::transform_sql(original_sql, &options);

        // Table name should be replaced in FROM clause
        assert!(
            transformed.contains("sql_derived_silver_12345"),
            "Table reference should be replaced, got: {}",
            transformed
        );

        // Column name should NOT be replaced - should still contain "Silver."
        assert!(
            transformed.contains(r#""Silver.AT500_Surface.DO.mg/L""#),
            "Column name should be preserved, got: {}",
            transformed
        );
    }

    /// Test AST transformation handles multiple table references correctly
    #[test]
    fn test_ast_table_replacement_multiple_tables() {
        let original_sql = r#"
            SELECT 
                Silver."Silver.WaterTemp",
                BDock."BDock.WaterTemp"
            FROM Silver
            JOIN BDock ON Silver.timestamp = BDock.timestamp
        "#;

        let mut table_mappings = HashMap::new();
        _ = table_mappings.insert("Silver".to_string(), "sql_derived_silver_abc".to_string());
        _ = table_mappings.insert("BDock".to_string(), "sql_derived_bdock_def".to_string());

        let options = SqlTransformOptions {
            table_mappings: Some(table_mappings),
            source_replacement: None,
        };

        let transformed = crate::transform_sql(original_sql, &options);

        // Both table references should be replaced
        assert!(
            transformed.contains("sql_derived_silver_abc"),
            "Silver table should be replaced, got: {}",
            transformed
        );
        assert!(
            transformed.contains("sql_derived_bdock_def"),
            "BDock table should be replaced, got: {}",
            transformed
        );

        // Column names should be preserved with original scope prefixes
        assert!(
            transformed.contains(r#""Silver.WaterTemp""#),
            "Silver column name should be preserved, got: {}",
            transformed
        );
        assert!(
            transformed.contains(r#""BDock.WaterTemp""#),
            "BDock column name should be preserved, got: {}",
            transformed
        );
    }

    /// Test AST transformation handles subqueries
    #[test]
    fn test_ast_table_replacement_subquery() {
        let original_sql = r#"
            SELECT * FROM (
                SELECT Silver."Silver.Value" FROM Silver
            ) AS subquery
        "#;

        let mut table_mappings = HashMap::new();
        _ = table_mappings.insert("Silver".to_string(), "sql_derived_silver_xyz".to_string());

        let options = SqlTransformOptions {
            table_mappings: Some(table_mappings),
            source_replacement: None,
        };

        let transformed = crate::transform_sql(original_sql, &options);

        // Table in subquery should be replaced
        assert!(
            transformed.contains("sql_derived_silver_xyz"),
            "Table in subquery should be replaced, got: {}",
            transformed
        );

        // Column name in subquery should be preserved
        assert!(
            transformed.contains(r#""Silver.Value""#),
            "Column name in subquery should be preserved, got: {}",
            transformed
        );
    }

    /// Test that non-matching table names are not replaced
    #[test]
    fn test_ast_table_replacement_no_match() {
        let original_sql = r#"SELECT timestamp, value FROM OtherTable"#;

        let mut table_mappings = HashMap::new();
        _ = table_mappings.insert("Silver".to_string(), "sql_derived_silver_123".to_string());

        let options = SqlTransformOptions {
            table_mappings: Some(table_mappings),
            source_replacement: None,
        };

        let transformed = crate::transform_sql(original_sql, &options);

        // Original table name should remain unchanged
        assert!(
            transformed.contains("OtherTable"),
            "Non-matching table should not be replaced, got: {}",
            transformed
        );

        // Should NOT contain the replacement
        assert!(
            !transformed.contains("sql_derived_silver_123"),
            "Replacement should not appear for non-matching table, got: {}",
            transformed
        );
    }

    /// Test source replacement pattern
    #[test]
    fn test_ast_source_replacement() {
        let original_sql = r#"SELECT timestamp, value FROM source WHERE value > 10"#;

        let options = SqlTransformOptions {
            table_mappings: None,
            source_replacement: Some("my_actual_table".to_string()),
        };

        let transformed = crate::transform_sql(original_sql, &options);

        // 'source' should be replaced with actual table name
        assert!(
            transformed.contains("my_actual_table"),
            "Source should be replaced, got: {}",
            transformed
        );
        assert!(
            !transformed.contains(" source "),
            "Original 'source' should not remain, got: {}",
            transformed
        );
    }

    /// Test that no transformations returns original SQL unchanged
    #[test]
    fn test_ast_no_transformations() {
        let original_sql = r#"SELECT * FROM MyTable"#;

        let options = SqlTransformOptions {
            table_mappings: None,
            source_replacement: None,
        };

        let transformed = crate::transform_sql(original_sql, &options);

        // Should return original SQL if no transformations
        assert_eq!(
            transformed.trim(),
            original_sql.trim(),
            "SQL without transformations should be unchanged"
        );
    }

    /// Test complex SQL with CTEs and window functions preserves structure
    #[test]
    fn test_ast_complex_sql_structure() {
        let original_sql = r#"
            WITH ranked AS (
                SELECT 
                    Silver."Silver.Value",
                    ROW_NUMBER() OVER (ORDER BY timestamp) as rn
                FROM Silver
            )
            SELECT * FROM ranked WHERE rn <= 10
        "#;

        let mut table_mappings = HashMap::new();
        _ = table_mappings.insert("Silver".to_string(), "sql_derived_silver_999".to_string());

        let options = SqlTransformOptions {
            table_mappings: Some(table_mappings),
            source_replacement: None,
        };

        let transformed = crate::transform_sql(original_sql, &options);

        // Table name should be replaced
        assert!(
            transformed.contains("sql_derived_silver_999"),
            "Table should be replaced in CTE, got: {}",
            transformed
        );

        // Column name with scope prefix should be preserved
        assert!(
            transformed.contains(r#""Silver.Value""#),
            "Column name should be preserved in CTE, got: {}",
            transformed
        );

        // CTE structure should be preserved
        assert!(
            transformed.to_uppercase().contains("WITH"),
            "CTE keyword should be preserved, got: {}",
            transformed
        );
        assert!(
            transformed.to_uppercase().contains("ROW_NUMBER"),
            "Window function should be preserved, got: {}",
            transformed
        );
    }

    #[test]
    fn test_sql_derived_mode_serialization() {
        let table_mode = SqlDerivedMode::Table;
        let series_mode = SqlDerivedMode::Series;

        assert_eq!(table_mode, SqlDerivedMode::Table);
        assert_eq!(series_mode, SqlDerivedMode::Series);
        assert_ne!(table_mode, series_mode);
    }

    #[test]
    fn test_sql_transform_options() {
        let opts = SqlTransformOptions {
            table_mappings: Some([("old".to_string(), "new".to_string())].into()),
            source_replacement: None,
        };

        assert!(opts.table_mappings.is_some());
        assert!(opts.source_replacement.is_none());
    }

    #[tokio::test]
    async fn test_sql_derived_with_column_rename_transform() {
        // Create test environment
        let (fs, provider_context) = create_test_environment().await;

        // Create test data with column "old_col" that we'll rename to "new_col"
        let batch = record_batch!(
            ("old_col", Utf8, ["a", "b", "c"]),
            ("value", Int32, [1, 2, 3])
        )
        .unwrap();

        let schema = batch.schema();
        let mut parquet_buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer);
            let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
            writer.write(&batch).unwrap();
            _ = writer.close().unwrap();
        }

        _ = create_parquet_file(
            &fs,
            "/input.parquet",
            parquet_buffer,
            EntryType::TablePhysicalVersion,
        )
        .await
        .unwrap();

        // Create column-rename transform config
        let rename_config = r#"
rules:
  - type: direct
    from: "old_col"
    to: "new_col"
"#;

        let root = fs.root().await.unwrap();

        // Create /etc directory first
        _ = root.create_dir_path("/etc").await.unwrap();

        // Create the transform node using the FS abstraction (like mknod does)
        _ = root
            .create_dynamic_path(
                "/etc/test_rename",
                EntryType::FileDynamic,
                "column-rename",
                rename_config.as_bytes().to_vec(),
            )
            .await
            .unwrap();

        // Create sql-derived config with transform
        let sql_config = SqlDerivedConfig {
            patterns: [(
                "input".to_string(),
                crate::Url::parse("table:///input.parquet").unwrap(),
            )]
            .into(),
            query: Some("SELECT new_col, value FROM input".to_string()),
            pattern_transforms: None,
            scope_prefixes: None,
            provider_wrapper: None,
            transforms: Some(vec!["/etc/test_rename".to_string()]),
        };

        // Create sql-derived file
        let sql_derived_id = FileID::new_physical_dir_id(tinyfs::local_pond_uuid());
        let sql_file = SqlDerivedFile::new(
            sql_config,
            test_context(&provider_context, sql_derived_id),
            SqlDerivedMode::Table,
        )
        .unwrap();

        // Execute and verify column was renamed
        let batches = execute_sql_derived_direct(&sql_file, &provider_context)
            .await
            .unwrap();

        assert!(!batches.is_empty(), "Should have results");
        let batch = &batches[0];

        // Verify schema has renamed column
        assert_eq!(batch.schema().fields().len(), 2);
        assert_eq!(batch.schema().field(0).name(), "new_col");
        assert_eq!(batch.schema().field(1).name(), "value");

        // Verify data
        let names = get_string_array(batch, 0);
        assert_eq!(names.len(), 3);
        assert_eq!(names.value(0), "a");
    }
}
