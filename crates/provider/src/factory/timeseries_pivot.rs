// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Timeseries Pivot Factory
//!
//! Creates a typed sparse pivot over specific columns from inputs matched by a
//! pattern. Resolved inputs are cached for the transaction-scoped file instance.
//!
//! Example config:
//! ```yaml
//! factory: "timeseries-pivot"
//! config:
//!   pattern: "series:///combined/*"  # Captures site name
//!   columns:
//!     - "AT500_Surface.DO.mg/L"
//!     - "AT500_Bottom.DO.mg/L"
//! ```

use crate::factory::sql_derived::{SqlDerivedConfig, SqlDerivedFile};
use crate::register_dynamic_factory;
use datafusion::catalog::TableProvider;
use datafusion::catalog::view::ViewTable;
use datafusion::common::Column;
use datafusion::logical_expr::col;
use query_foundation::plans::pivot::{PivotInput, pivot_measurements};
use query_foundation::plans::transform::null_pad;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use tinyfs::ResultExt;
use tinyfs::{FileHandle, FileID, Result as TinyFSResult};
use tokio::sync::Mutex;

/// Configuration for timeseries-pivot factory
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeseriesPivotConfig {
    /// Pattern to match input files (e.g., "series:///combined/*")
    pub pattern: crate::Url,

    /// List of column names to pivot across all matched inputs
    pub columns: Vec<String>,

    /// Time column name (default: "timestamp")
    #[serde(default = "default_time_column")]
    pub time_column: String,

    /// Optional list of table transform factory paths to apply to input TableProvider.
    /// Transforms are applied in order before SQL execution.
    /// Each transform is a path like "/etc/hydro_rename"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transforms: Option<Vec<String>>,
}

fn default_time_column() -> String {
    "timestamp".to_string()
}

/// File implementation that builds a typed pivot from current source schemas.
pub struct TimeseriesPivotFile {
    config: TimeseriesPivotConfig,
    context: crate::FactoryContext,
    /// Lazily-built source resolver (resolved on first read, then cached).
    inner: Arc<Mutex<Option<SqlDerivedFile>>>,
    aliases: Arc<Mutex<Option<Vec<String>>>>,
}

impl TimeseriesPivotFile {
    #[must_use]
    pub fn new(config: TimeseriesPivotConfig, context: crate::FactoryContext) -> Self {
        Self {
            config,
            context,
            inner: Arc::new(Mutex::new(None)),
            aliases: Arc::new(Mutex::new(None)),
        }
    }

    /// Resolve pattern to matched inputs, extracting captured site names
    async fn resolve_pattern(&self) -> TinyFSResult<Vec<(String, String)>> {
        // Use context.root() to respect effective_root for cross-pond imports
        let tinyfs_root = self.context.root().await?;

        // Extract filesystem path from URL for pattern matching
        let pattern_path = self.config.pattern.path();

        // Use collect_matches to find matching files with captured groups
        let pattern_matches = tinyfs_root.collect_matches(pattern_path).await?;

        if pattern_matches.is_empty() {
            return Err(tinyfs::Error::Other(format!(
                "No files matched pattern: {}",
                self.config.pattern
            )));
        }

        // Extract site names from captured groups (first wildcard capture)
        let mut matches = Vec::new();
        let mut aliases = BTreeSet::new();
        for (node_path, captured_groups) in pattern_matches {
            // Get path string
            let path_buf = node_path.path();
            let path_str = path_buf.to_string_lossy().to_string();

            let alias = if !captured_groups.is_empty() {
                // Use first captured group as site name (index 0)
                captured_groups[0].clone()
            } else {
                // If no capture, use the last path component
                path_buf
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string()
            };
            if !aliases.insert(alias.clone()) {
                return Err(tinyfs::Error::Other(format!(
                    "timeseries-pivot pattern '{}' produced duplicate alias '{alias}'",
                    self.config.pattern
                )));
            }
            matches.push((alias, path_str));
        }

        Ok(matches)
    }

    /// Ensure the source resolver is created with stable matched aliases.
    async fn ensure_inner(&self) -> TinyFSResult<()> {
        crate::factory::lazy_sql_file::ensure_inner_series(&self.inner, &self.context, || async {
            log::debug!(
                "[SEARCH] TIMESERIES-PIVOT: Resolving pattern '{}' for {} columns",
                self.config.pattern,
                self.config.columns.len()
            );

            // Resolve pattern to get current matched inputs
            let matched_inputs = self.resolve_pattern().await?;

            log::debug!(
                "[LIST] TIMESERIES-PIVOT: Pattern matched {} inputs: {:?}",
                matched_inputs.len(),
                matched_inputs.iter().map(|(a, _)| a).collect::<Vec<_>>()
            );

            if matched_inputs.is_empty() {
                return Err(tinyfs::Error::Other(
                    "Timeseries-pivot pattern matched no inputs".to_string(),
                ));
            }

            let mut patterns = HashMap::new();
            let mut scope_prefixes = HashMap::new();
            let mut aliases = Vec::with_capacity(matched_inputs.len());
            for (alias, path) in &matched_inputs {
                let source = if path.starts_with('/') {
                    format!("file+series://{path}")
                } else {
                    format!("file+series:///{path}")
                };
                let url = crate::Url::parse(&source).map_err(|error| {
                    tinyfs::Error::Other(format!(
                        "failed to build timeseries-pivot source URL for '{path}': {error}"
                    ))
                })?;
                _ = patterns.insert(alias.clone(), url);
                _ = scope_prefixes.insert(
                    alias.clone(),
                    (alias.clone(), self.config.time_column.clone()),
                );
                aliases.push(alias.clone());
            }
            *self.aliases.lock().await = Some(aliases);
            Ok(SqlDerivedConfig::new_scoped(patterns, None, scope_prefixes)
                .with_transforms(self.config.transforms.clone()))
        })
        .await
    }

    #[must_use]
    pub fn create_handle(self) -> FileHandle {
        FileHandle::new(Arc::new(Mutex::new(Box::new(self))))
    }

    async fn typed_pivot_provider(
        &self,
        id: FileID,
        context: &tinyfs::ProviderContext,
        bounds: tinyfs::SeriesReadBounds,
    ) -> TinyFSResult<Arc<dyn TableProvider>> {
        let cache_key = crate::TableProviderKey::with_bounds(
            id,
            crate::VersionSelection::LatestVersion,
            bounds,
        )
        .to_cache_string();
        if let Some(cached) = context.get_table_provider_cache(&cache_key) {
            return Ok(cached);
        }
        if self.config.columns.is_empty() {
            return Err(tinyfs::Error::Other(
                "timeseries-pivot requires at least one column".to_owned(),
            ));
        }

        self.ensure_inner().await?;
        let inner = self.inner.lock().await;
        let (source_tables, empty_sources) = inner
            .as_ref()
            .expect("inner initialized by ensure_inner")
            .register_source_tables_bounded(id, context, bounds)
            .await?;
        if !empty_sources.is_empty() {
            return Err(tinyfs::Error::Other(format!(
                "timeseries-pivot resolved sources disappeared before planning: {empty_sources:?}"
            )));
        }
        let aliases = self
            .aliases
            .lock()
            .await
            .clone()
            .expect("aliases initialized with source resolver");

        let mut inputs = Vec::with_capacity(aliases.len());
        for alias in aliases {
            let table_name = source_tables.get(&alias).ok_or_else(|| {
                tinyfs::Error::Other(format!(
                    "timeseries-pivot source table is missing for alias '{alias}'"
                ))
            })?;
            let frame = context
                .datafusion_session
                .table(table_name)
                .await
                .map_err(|error| {
                    tinyfs::Error::Other(format!(
                        "failed to open timeseries-pivot source '{alias}': {error}"
                    ))
                })?;
            let missing = self
                .config
                .columns
                .iter()
                .map(|column| format!("{alias}.{column}"))
                .filter(|column| frame.schema().field_with_unqualified_name(column).is_err())
                .map(|column| (column, arrow::datatypes::DataType::Float64))
                .collect();
            let frame = null_pad(frame, missing).map_err(|error| {
                tinyfs::Error::Other(format!(
                    "failed to pad timeseries-pivot source '{alias}': {error}"
                ))
            })?;
            let first = self.config.columns.first().expect("validated columns");
            let first_name = format!("{alias}.{first}");
            let input = self.config.columns.iter().skip(1).fold(
                PivotInput::new(
                    frame,
                    self.config.time_column.as_str(),
                    first_name.clone(),
                    first_name,
                ),
                |input, column| {
                    let name = format!("{alias}.{column}");
                    input.with_value(name.clone(), name)
                },
            );
            inputs.push(input);
        }

        let pivot = pivot_measurements(inputs, Vec::new(), &self.config.time_column)
            .and_then(|frame| {
                frame.sort(vec![
                    col(Column::from_name(&self.config.time_column)).sort(true, true),
                ])
            })
            .map_err(|error| {
                tinyfs::Error::Other(format!("failed to plan timeseries pivot: {error}"))
            })?;
        let provider: Arc<dyn TableProvider> = Arc::new(ViewTable::new(
            pivot.logical_plan().clone(),
            Some("typed timeseries-pivot".to_owned()),
        ));
        context.set_table_provider_cache(cache_key, Arc::clone(&provider))?;
        Ok(provider)
    }
}

crate::factory::lazy_sql_file::impl_lazy_sql_derived_file_metadata!(TimeseriesPivotFile);

#[async_trait::async_trait]
impl tinyfs::QueryableFile for TimeseriesPivotFile {
    async fn as_table_provider(
        &self,
        id: FileID,
        context: &tinyfs::ProviderContext,
    ) -> TinyFSResult<Arc<dyn TableProvider>> {
        self.typed_pivot_provider(id, context, tinyfs::SeriesReadBounds::NONE)
            .await
    }

    async fn as_table_provider_bounded(
        &self,
        id: FileID,
        context: &tinyfs::ProviderContext,
        bounds: tinyfs::SeriesReadBounds,
    ) -> TinyFSResult<Arc<dyn TableProvider>> {
        self.typed_pivot_provider(id, context, bounds).await
    }

    async fn query_lineage(
        &self,
        _id: FileID,
        context: &tinyfs::ProviderContext,
    ) -> TinyFSResult<Option<tinyfs::QueryLineage>> {
        let recipe = serde_json::to_vec(&self.config)
            .map_err(|error| tinyfs::Error::Other(format!("pivot lineage recipe: {error}")))?;
        let mut recipe_hasher = blake3::Hasher::new();
        _ = recipe_hasher.update(b"watertown:timeseries-pivot-lineage:v1");
        update_lineage_hash(&mut recipe_hasher, &recipe);

        let root = self.context.root().await?;
        for transform_path in self.config.transforms.iter().flatten() {
            let (_, lookup) = root.resolve_path(transform_path).await.map_err(|error| {
                tinyfs::Error::Other(format!(
                    "pivot lineage could not resolve transform '{transform_path}': {error}"
                ))
            })?;
            let transform_node = match lookup {
                tinyfs::Lookup::Found(node) => node,
                _ => {
                    return Err(tinyfs::Error::Other(format!(
                        "pivot lineage transform '{transform_path}' was not found"
                    )));
                }
            };
            let (factory_name, config_bytes) = context
                .persistence
                .get_dynamic_node_config(transform_node.id())
                .await?
                .ok_or_else(|| {
                    tinyfs::Error::Other(format!(
                        "pivot lineage transform '{transform_path}' has no factory config"
                    ))
                })?;
            update_lineage_hash(&mut recipe_hasher, transform_path.as_bytes());
            update_lineage_hash(&mut recipe_hasher, factory_name.as_bytes());
            update_lineage_hash(&mut recipe_hasher, &config_bytes);
        }

        let mut lineage = tinyfs::QueryLineage::new(recipe_hasher.finalize().to_hex().to_string());
        let fs = self.context.context.filesystem();
        let mut provider = crate::Provider::with_context(Arc::new(fs), Arc::new(context.clone()));
        provider = provider.with_root(root);
        let Some(nested) = provider
            .query_lineage_for_url(&self.config.pattern.to_string())
            .await
            .map_err(|error| tinyfs::Error::Other(format!("pivot lineage: {error}")))?
        else {
            return Ok(None);
        };
        lineage.extend(nested);
        Ok(Some(lineage))
    }
}

fn update_lineage_hash(hasher: &mut blake3::Hasher, value: &[u8]) {
    _ = hasher.update(&(value.len() as u64).to_le_bytes());
    _ = hasher.update(value);
}

impl std::fmt::Debug for TimeseriesPivotFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimeseriesPivotFile")
            .field("pattern", &self.config.pattern)
            .field("columns", &self.config.columns)
            .finish()
    }
}

// Factory registration

fn create_timeseries_pivot_handle(
    config: Value,
    context: crate::FactoryContext,
) -> TinyFSResult<FileHandle> {
    let cfg: TimeseriesPivotConfig =
        crate::factory::config_util::config_from_value(config, "Invalid timeseries-pivot config")?;

    let pivot_file = TimeseriesPivotFile::new(cfg, context);
    Ok(pivot_file.create_handle())
}

fn validate_timeseries_pivot_config(config: &[u8]) -> TinyFSResult<Value> {
    let (_config_value, cfg) = crate::factory::config_util::parse_yaml_config::<
        TimeseriesPivotConfig,
    >(config, "Invalid timeseries-pivot config")?;

    // Validate scheme is recognized (no fallback for unknown schemes)
    let scheme = cfg.pattern.scheme();
    const KNOWN_SCHEMES: &[&str] = &[
        "file",
        "series",
        "table",
        "data",
        "csv",
        "excelhtml",
        "oteljson",
    ];
    if !KNOWN_SCHEMES.contains(&scheme) {
        return Err(tinyfs::Error::Other(format!(
            "Unknown URL scheme '{}' in pattern '{}'. Known schemes: {}",
            scheme,
            cfg.pattern,
            KNOWN_SCHEMES.join(", ")
        )));
    }

    if cfg.columns.is_empty() {
        return Err(tinyfs::Error::Other(
            "At least one column must be specified".to_string(),
        ));
    }

    serde_json::to_value(&cfg).map_other_context("Failed to convert config")
}

register_dynamic_factory!(
    name: "timeseries-pivot",
    description: "Pivot a timeseries by value column",
    file: create_timeseries_pivot_handle,
    validate: validate_timeseries_pivot_config
);

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, TimestampSecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::display::DisplayableExecutionPlan;
    use tinyfs::FileID;

    use crate::QueryableFile;
    use crate::factory::test_support::{
        create_parquet_from_batch, create_test_environment, test_context,
    };

    #[tokio::test]
    async fn production_pivot_uses_one_scan_per_source() {
        let (fs, provider_context) = create_test_environment().await;
        let root = fs.root().await.unwrap();
        _ = root.create_dir_all("/combined").await.unwrap();

        let silver_schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("WaterTemp", DataType::Float64, false),
        ]));
        let silver = RecordBatch::try_new(
            silver_schema,
            vec![
                Arc::new(TimestampSecondArray::from(vec![1, 2])),
                Arc::new(Float64Array::from(vec![10.0, 20.0])),
            ],
        )
        .unwrap();
        _ = create_parquet_from_batch(
            &fs,
            "/combined/Silver",
            &silver,
            tinyfs::EntryType::TablePhysicalSeries,
        )
        .await
        .unwrap();

        let bdock_schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("DO", DataType::Float64, false),
        ]));
        let bdock = RecordBatch::try_new(
            bdock_schema,
            vec![
                Arc::new(TimestampSecondArray::from(vec![2, 3])),
                Arc::new(Float64Array::from(vec![7.0, 8.0])),
            ],
        )
        .unwrap();
        _ = create_parquet_from_batch(
            &fs,
            "/combined/BDock",
            &bdock,
            tinyfs::EntryType::TablePhysicalSeries,
        )
        .await
        .unwrap();

        let file = TimeseriesPivotFile::new(
            TimeseriesPivotConfig {
                pattern: crate::Url::parse("series:///combined/*").unwrap(),
                columns: vec!["WaterTemp".to_owned(), "DO".to_owned()],
                time_column: "timestamp".to_owned(),
                transforms: None,
            },
            test_context(&provider_context, FileID::root()),
        );
        let lineage = file
            .query_lineage(FileID::root(), &provider_context)
            .await
            .unwrap()
            .expect("typed pivot declares recursive lineage");
        assert_eq!(
            lineage
                .leaves
                .iter()
                .filter(|leaf| !leaf.identity.starts_with("recipe:"))
                .count(),
            2
        );
        let table = file
            .as_table_provider(FileID::root(), &provider_context)
            .await
            .unwrap();
        let context = &provider_context.datafusion_session;
        _ = context.register_table("pivoted", table).unwrap();
        let frame = context
            .sql("SELECT * FROM pivoted ORDER BY timestamp")
            .await
            .unwrap();
        let physical = frame.clone().create_physical_plan().await.unwrap();
        let plan = DisplayableExecutionPlan::new(physical.as_ref())
            .indent(true)
            .to_string();
        assert_eq!(plan.matches("DataSourceExec").count(), 2, "{plan}");
        assert!(!plan.contains("MemoryExec"), "{plan}");
        assert!(!plan.contains("NullPaddingExec"), "{plan}");

        let batches = frame.collect().await.unwrap();
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);
        for name in [
            "Silver.WaterTemp",
            "Silver.DO",
            "BDock.WaterTemp",
            "BDock.DO",
        ] {
            assert!(batch.column_by_name(name).is_some(), "missing {name}");
        }
        assert_eq!(batch.column_by_name("Silver.DO").unwrap().null_count(), 3);
        assert_eq!(
            batch
                .column_by_name("BDock.WaterTemp")
                .unwrap()
                .null_count(),
            3
        );
    }

    #[test]
    fn test_config_validation() {
        // Valid config - just the config section as JSON/YAML
        let valid_config = r#"
pattern: "series:///combined/*"
columns:
  - "WaterTemp"
  - "DO"
time_column: "time"
"#;
        let result = validate_timeseries_pivot_config(valid_config.as_bytes());
        assert!(result.is_ok(), "Valid config should pass: {:?}", result);

        // Invalid URL pattern should fail
        let invalid_pattern = r#"
pattern: "invalid-scheme://bad/url"
columns:
  - "WaterTemp"
time_column: "time"
"#;
        let result = validate_timeseries_pivot_config(invalid_pattern.as_bytes());
        assert!(result.is_err(), "Invalid URL pattern should fail");

        // No columns should fail
        let no_columns = r#"
pattern: "series:///combined/*"
columns: []
time_column: "time"
"#;
        let result = validate_timeseries_pivot_config(no_columns.as_bytes());
        assert!(result.is_err(), "Empty columns list should fail");
    }
}
