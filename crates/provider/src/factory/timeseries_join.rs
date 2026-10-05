// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Timeseries Join Factory for TLogFS
//!
//! This factory composes same-scope sources and joins distinct scopes by
//! timestamp using typed query-foundation plans.

use crate::factory::sql_derived::{SqlDerivedConfig, SqlDerivedFile};
use crate::register_dynamic_factory;
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{DateTime, Utc};
use datafusion::catalog::TableProvider;
use datafusion::catalog::view::ViewTable;
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::{col, lit};
use query_foundation::overlap::OverlapPolicy;
use query_foundation::plans::combine::{CombineInput, combine_same_scope};
use query_foundation::plans::join::{TimestampJoinInput, accumulated_full_outer_join};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use tinyfs::{FileHandle, FileID, Result as TinyFSResult};

/// Time range bounds for filtering
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeRange {
    /// Optional start time (ISO 8601 format)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub begin: Option<String>,

    /// Optional end time (ISO 8601 format)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
}

/// Input source with URL pattern and optional time range
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeseriesInput {
    /// URL pattern for matching time series files
    ///
    /// Supported URL schemes:
    /// - `series:///pattern` - Builtin TinyFS FileSeries (Parquet)
    /// - `series+gzip:///pattern` - Compressed FileSeries
    /// - `csv:///pattern` - CSV files (requires format conversion)
    /// - `csv+gzip:///pattern?delimiter=;` - CSV with decompression and options
    /// - `excelhtml:///pattern` - HydroVu HTML exports
    ///
    /// Example: `series:///data/sensors/*.series`
    pub pattern: crate::Url,

    /// Optional time range filter for this specific input
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<TimeRange>,

    /// Optional scope prefix to add to all column names (e.g.,
    /// "temperature" -> "BDock.temperature")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,

    /// Optional list of table transform factory paths to apply to this input's TableProvider.
    /// Transforms are applied in order before scope prefixes and SQL execution.
    /// Each transform is a path like "/etc/hydro_rename"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transforms: Option<Vec<String>>,
}

/// Configuration for the timeseries-join factory
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeseriesJoinConfig {
    /// Name of the timestamp column (defaults to "timestamp")
    #[serde(default = "default_time_column")]
    pub time_column: String,

    /// List of input sources with patterns and optional time ranges
    pub inputs: Vec<TimeseriesInput>,
}

fn default_time_column() -> String {
    "timestamp".to_string()
}

impl TimeseriesJoinConfig {
    /// Validate that all input patterns are valid URLs with appropriate schemes
    pub fn validate(&self) -> TinyFSResult<()> {
        if self.inputs.is_empty() {
            return Err(tinyfs::Error::Other(
                "At least one input must be specified".to_string(),
            ));
        }

        if self.inputs.len() == 1 {
            return Err(tinyfs::Error::Other(
                "Timeseries join requires at least 2 inputs. Use sql-derived-series for single sources.".to_string(),
            ));
        }

        // Validate each input pattern is a valid URL
        for (i, input) in self.inputs.iter().enumerate() {
            // URL already validated during deserialization
            let scheme = input.pattern.scheme();
            match scheme {
                "series" | "csv" | "excelhtml" | "file" => {
                    // Valid timeseries sources (file uses EntryType to determine type)
                }
                _ => {
                    return Err(tinyfs::Error::Other(format!(
                        "Input {} uses unsupported scheme '{}' for timeseries data. Supported: series, csv, excelhtml, file",
                        i, scheme
                    )));
                }
            }

            // Validate time range timestamps if present
            if let Some(range) = &input.range {
                if let Some(begin) = &range.begin {
                    let _ = validate_timestamp(begin)?;
                }
                if let Some(end) = &range.end {
                    let _ = validate_timestamp(end)?;
                }
            }
        }

        Ok(())
    }
}

/// Validate and parse an ISO 8601 timestamp
fn validate_timestamp(ts_str: &str) -> TinyFSResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(ts_str)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| {
            tinyfs::Error::Other(format!(
                "Invalid timestamp '{}': {}. Expected ISO 8601/RFC 3339 format (e.g., '2023-11-06T14:00:00Z')",
                ts_str, e
            ))
        })
}

/// Timeseries join file implementation
/// Uses SqlDerivedFile only for source resolution, transforms, and scoping.
pub struct TimeseriesJoinFile {
    config: TimeseriesJoinConfig,
    context: crate::FactoryContext,
    // Lazy-initialized source resolver.
    inner: Arc<tokio::sync::Mutex<Option<SqlDerivedFile>>>,
}

impl TimeseriesJoinFile {
    /// Create a new TimeseriesJoinFile with validated URL patterns
    ///
    /// # Errors
    /// Returns error if config validation fails (invalid URLs, unsupported schemes, etc.)
    pub fn new(config: TimeseriesJoinConfig, context: crate::FactoryContext) -> TinyFSResult<Self> {
        // Validate config immediately on creation
        config.validate()?;

        Ok(Self {
            config,
            context,
            inner: Arc::new(tokio::sync::Mutex::new(None)),
        })
    }

    /// Ensure the inner SqlDerivedFile is created
    async fn ensure_inner(&self) -> TinyFSResult<()> {
        crate::factory::lazy_sql_file::ensure_inner_series(&self.inner, &self.context, || async {
            let mut patterns = HashMap::new();
            let mut scope_prefixes = HashMap::new();
            let mut pattern_transforms = HashMap::new();
            for (index, input) in self.config.inputs.iter().enumerate() {
                let table_name = format!("input{index}");
                _ = patterns.insert(table_name.clone(), input.pattern.clone());
                if let Some(scope) = &input.scope {
                    _ = scope_prefixes.insert(
                        table_name.clone(),
                        (scope.clone(), self.config.time_column.clone()),
                    );
                }
                if let Some(transforms) = &input.transforms {
                    _ = pattern_transforms.insert(table_name, transforms.clone());
                }
            }
            Ok(SqlDerivedConfig::new(patterns, None)
                .with_scope_prefixes(scope_prefixes)
                .with_pattern_transforms(pattern_transforms))
        })
        .await
    }

    #[must_use]
    pub fn create_handle(self) -> FileHandle {
        FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }

    async fn typed_join_provider(
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

        self.ensure_inner().await?;
        let inner = self.inner.lock().await;
        let (source_tables, empty_sources) = inner
            .as_ref()
            .expect("inner initialized by ensure_inner")
            .register_source_tables_bounded(id, context, bounds)
            .await?;

        let mut scope_groups: BTreeMap<String, Vec<(usize, datafusion::dataframe::DataFrame)>> =
            BTreeMap::new();
        let mut empty_fallback = None;
        for (index, input) in self.config.inputs.iter().enumerate() {
            let pattern_name = format!("input{index}");
            let table_name = source_tables.get(&pattern_name).ok_or_else(|| {
                tinyfs::Error::Other(format!(
                    "timeseries-join source table is missing for pattern '{pattern_name}'"
                ))
            })?;
            let frame = context
                .datafusion_session
                .table(table_name)
                .await
                .map_err(|error| {
                    tinyfs::Error::Other(format!(
                        "failed to open timeseries-join source '{pattern_name}': {error}"
                    ))
                })?;
            let frame = apply_time_range(frame, &self.config.time_column, input.range.as_ref())?;
            if empty_sources.contains(&pattern_name) {
                if empty_fallback.is_none() {
                    empty_fallback = Some(frame);
                }
                continue;
            }
            let scope = input
                .scope
                .clone()
                .unwrap_or_else(|| format!("_none_{index}"));
            scope_groups.entry(scope).or_default().push((index, frame));
        }

        let mut scopes = Vec::with_capacity(scope_groups.len());
        for (scope, frames) in scope_groups {
            let combined = combine_same_scope(
                frames
                    .into_iter()
                    .map(|(sequence, frame)| {
                        CombineInput::new(
                            format!("{scope}:input{sequence}"),
                            sequence as u64,
                            frame,
                        )
                    })
                    .collect(),
                &OverlapPolicy::PreserveAll,
            )
            .await
            .map_err(|error| {
                tinyfs::Error::Other(format!(
                    "failed to combine timeseries-join scope '{scope}': {error}"
                ))
            })?;
            scopes.push(combined);
        }

        let joined = if scopes.is_empty() {
            empty_fallback.ok_or_else(|| {
                tinyfs::Error::Other(
                    "timeseries-join produced no source scopes or empty fallback".to_owned(),
                )
            })?
        } else if scopes.len() == 1 {
            scopes.pop().expect("one scope")
        } else {
            accumulated_full_outer_join(
                scopes
                    .into_iter()
                    .map(|frame| TimestampJoinInput::new(frame, self.config.time_column.as_str()))
                    .collect(),
                &self.config.time_column,
            )
            .map_err(|error| {
                tinyfs::Error::Other(format!("failed to plan timeseries join: {error}"))
            })?
        };
        let ordered = joined
            .sort(vec![
                col(Column::from_name(&self.config.time_column)).sort(true, true),
            ])
            .map_err(|error| {
                tinyfs::Error::Other(format!("failed to order timeseries join: {error}"))
            })?;
        let provider: Arc<dyn TableProvider> = Arc::new(ViewTable::new(
            ordered.logical_plan().clone(),
            Some("typed timeseries-join".to_owned()),
        ));
        context.set_table_provider_cache(cache_key, Arc::clone(&provider))?;
        Ok(provider)
    }
}

crate::factory::lazy_sql_file::impl_lazy_sql_derived_file_metadata!(TimeseriesJoinFile);

#[async_trait::async_trait]
impl tinyfs::QueryableFile for TimeseriesJoinFile {
    async fn as_table_provider(
        &self,
        id: FileID,
        context: &tinyfs::ProviderContext,
    ) -> TinyFSResult<Arc<dyn TableProvider>> {
        self.typed_join_provider(id, context, tinyfs::SeriesReadBounds::NONE)
            .await
    }

    async fn as_table_provider_bounded(
        &self,
        id: FileID,
        context: &tinyfs::ProviderContext,
        bounds: tinyfs::SeriesReadBounds,
    ) -> TinyFSResult<Arc<dyn TableProvider>> {
        self.typed_join_provider(id, context, bounds).await
    }

    async fn query_lineage(
        &self,
        _id: FileID,
        context: &tinyfs::ProviderContext,
    ) -> TinyFSResult<Option<tinyfs::QueryLineage>> {
        let recipe = serde_json::to_vec(&self.config)
            .map_err(|error| tinyfs::Error::Other(format!("join lineage recipe: {error}")))?;
        let mut recipe_hasher = blake3::Hasher::new();
        _ = recipe_hasher.update(b"watertown:timeseries-join-lineage:v1");
        update_lineage_hash(&mut recipe_hasher, &recipe);

        let root = self.context.root().await?;
        for input in &self.config.inputs {
            for transform_path in input.transforms.iter().flatten() {
                let (_, lookup) = root.resolve_path(transform_path).await.map_err(|error| {
                    tinyfs::Error::Other(format!(
                        "join lineage could not resolve transform '{transform_path}': {error}"
                    ))
                })?;
                let transform_node = match lookup {
                    tinyfs::Lookup::Found(node) => node,
                    _ => {
                        return Err(tinyfs::Error::Other(format!(
                            "join lineage transform '{transform_path}' was not found"
                        )));
                    }
                };
                let (factory_name, config_bytes) = context
                    .persistence
                    .get_dynamic_node_config(transform_node.id())
                    .await?
                    .ok_or_else(|| {
                        tinyfs::Error::Other(format!(
                            "join lineage transform '{transform_path}' has no factory config"
                        ))
                    })?;
                update_lineage_hash(&mut recipe_hasher, transform_path.as_bytes());
                update_lineage_hash(&mut recipe_hasher, factory_name.as_bytes());
                update_lineage_hash(&mut recipe_hasher, &config_bytes);
            }
        }

        let mut lineage = tinyfs::QueryLineage::new(recipe_hasher.finalize().to_hex().to_string());
        let fs = self.context.context.filesystem();
        let mut provider = crate::Provider::with_context(Arc::new(fs), Arc::new(context.clone()));
        provider = provider.with_root(root);
        for input in &self.config.inputs {
            let Some(nested) = provider
                .query_lineage_for_url(&input.pattern.to_string())
                .await
                .map_err(|error| tinyfs::Error::Other(format!("join lineage: {error}")))?
            else {
                log::debug!(
                    "query lineage unavailable for timeseries-join input '{}'",
                    input.pattern
                );
                return Ok(None);
            };
            lineage.extend(nested);
        }
        Ok(Some(lineage))
    }
}

fn update_lineage_hash(hasher: &mut blake3::Hasher, value: &[u8]) {
    _ = hasher.update(&(value.len() as u64).to_le_bytes());
    _ = hasher.update(value);
}

fn apply_time_range(
    frame: datafusion::dataframe::DataFrame,
    time_column: &str,
    range: Option<&TimeRange>,
) -> TinyFSResult<datafusion::dataframe::DataFrame> {
    let Some(range) = range else {
        return Ok(frame);
    };
    let data_type = frame
        .schema()
        .field_with_unqualified_name(time_column)
        .map_err(|_| {
            tinyfs::Error::Other(format!(
                "timeseries-join input is missing time column '{time_column}'"
            ))
        })?
        .data_type()
        .clone();
    let lower = range
        .begin
        .as_deref()
        .map(|value| timestamp_scalar(value, &data_type))
        .transpose()?;
    let upper = range
        .end
        .as_deref()
        .map(|value| timestamp_scalar(value, &data_type))
        .transpose()?;
    let time = col(Column::from_name(time_column));
    let predicate = match (lower, upper) {
        (Some(lower), Some(upper)) => time.clone().gt_eq(lit(lower)).and(time.lt_eq(lit(upper))),
        (Some(lower), None) => time.gt_eq(lit(lower)),
        (None, Some(upper)) => time.lt_eq(lit(upper)),
        (None, None) => return Ok(frame),
    };
    frame
        .filter(predicate)
        .map_err(|error| tinyfs::Error::Other(format!("invalid timeseries-join range: {error}")))
}

fn timestamp_scalar(value: &str, data_type: &DataType) -> TinyFSResult<ScalarValue> {
    let timestamp = validate_timestamp(value)?;
    let scalar = match data_type {
        DataType::Date64 => ScalarValue::Date64(Some(timestamp.timestamp_millis())),
        DataType::Timestamp(TimeUnit::Second, timezone) => {
            ScalarValue::TimestampSecond(Some(timestamp.timestamp()), timezone.clone())
        }
        DataType::Timestamp(TimeUnit::Millisecond, timezone) => {
            ScalarValue::TimestampMillisecond(Some(timestamp.timestamp_millis()), timezone.clone())
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            ScalarValue::TimestampMicrosecond(Some(timestamp.timestamp_micros()), timezone.clone())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, timezone) => ScalarValue::TimestampNanosecond(
            Some(timestamp.timestamp_nanos_opt().ok_or_else(|| {
                tinyfs::Error::Other(format!("timestamp '{value}' is outside nanosecond range"))
            })?),
            timezone.clone(),
        ),
        _ => {
            return Err(tinyfs::Error::Other(format!(
                "timeseries-join range requires a date or timestamp time column, found {data_type}"
            )));
        }
    };
    Ok(scalar)
}

// Factory functions

fn create_timeseries_join_handle(
    config: Value,
    context: crate::FactoryContext,
) -> TinyFSResult<FileHandle> {
    let cfg: TimeseriesJoinConfig =
        crate::factory::config_util::config_from_value(config, "Invalid timeseries-join config")?;

    let join_file = TimeseriesJoinFile::new(cfg, context)?;
    Ok(join_file.create_handle())
}

fn validate_timeseries_join_config(config: &[u8]) -> TinyFSResult<Value> {
    let (config_value, cfg) = crate::factory::config_util::parse_yaml_config::<TimeseriesJoinConfig>(
        config,
        "Invalid timeseries-join config",
    )?;

    cfg.validate()?;

    Ok(config_value)
}

// Register the factory
register_dynamic_factory!(
    name: "timeseries-join",
    description: "Create time series join files with automatic COALESCE and FULL OUTER JOIN",
    file: create_timeseries_join_handle,
    validate: validate_timeseries_join_config
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueryableFile;
    use arrow::array::{Array, Float64Array, TimestampSecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_plan::display::DisplayableExecutionPlan;
    use parquet::arrow::ArrowWriter;
    use std::io::Cursor;
    use std::sync::Arc;
    use tinyfs::{EntryType, FileID};

    use crate::factory::test_support::{
        create_parquet_file, create_test_environment, create_text_file, test_context,
    };

    #[test]
    fn test_validation_errors() {
        // Empty inputs
        let config_empty = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![],
        };
        assert!(config_empty.validate().is_err());

        // Single input
        let config_single = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![TimeseriesInput {
                pattern: crate::Url::parse("series:///solo.series").unwrap(),
                scope: None,
                range: None,
                transforms: None,
            }],
        };
        assert!(config_single.validate().is_err());

        // Invalid timestamp format
        let config_bad_time = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///a.series").unwrap(),
                    scope: None,
                    range: Some(TimeRange {
                        begin: Some("not-a-timestamp".to_string()),
                        end: None,
                    }),
                    transforms: None,
                },
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///b.series").unwrap(),
                    scope: None,
                    range: None,
                    transforms: None,
                },
            ],
        };
        assert!(config_bad_time.validate().is_err());
    }

    #[tokio::test]
    async fn test_timeseries_join_factory_integration() {
        let (fs, provider_context) = create_test_environment().await;

        // Create source1.series with timestamps [1, 2, 3] and temp_a column
        let schema1 = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("temp_a", DataType::Float64, false),
        ]));

        let timestamps1 = TimestampSecondArray::from(vec![1, 2, 3]);
        let temps1 = Float64Array::from(vec![10.0, 20.0, 30.0]);
        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![Arc::new(timestamps1), Arc::new(temps1)],
        )
        .unwrap();

        let mut parquet_buffer1 = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer1);
            let mut writer1 = ArrowWriter::try_new(cursor, schema1, None).unwrap();
            writer1.write(&batch1).unwrap();
            _ = writer1.close().unwrap();
        }

        _ = create_parquet_file(
            &fs,
            "/source1.series",
            parquet_buffer1,
            EntryType::TablePhysicalSeries,
        )
        .await
        .unwrap();

        // Create source2.series with timestamps [2, 3, 4] and temp_b column
        let schema2 = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("temp_b", DataType::Float64, false),
        ]));

        let timestamps2 = TimestampSecondArray::from(vec![2, 3, 4]);
        let temps2 = Float64Array::from(vec![15.0, 25.0, 35.0]);
        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![Arc::new(timestamps2), Arc::new(temps2)],
        )
        .unwrap();

        let mut parquet_buffer2 = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer2);
            let mut writer2 = ArrowWriter::try_new(cursor, schema2, None).unwrap();
            writer2.write(&batch2).unwrap();
            _ = writer2.close().unwrap();
        }

        _ = create_parquet_file(
            &fs,
            "/source2.series",
            parquet_buffer2,
            EntryType::TablePhysicalSeries,
        )
        .await
        .unwrap();

        // Now create the timeseries join
        let context = test_context(&provider_context, FileID::root());

        let config = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///source1.series").unwrap(),
                    scope: None,
                    range: None,
                    transforms: None,
                },
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///source2.series").unwrap(),
                    scope: None,
                    range: None,
                    transforms: None,
                },
            ],
        };

        let join_file = TimeseriesJoinFile::new(config, context).unwrap();

        let lineage = join_file
            .query_lineage(FileID::root(), &provider_context)
            .await
            .unwrap()
            .expect("typed join declares recursive lineage");
        assert_eq!(
            lineage
                .leaves
                .iter()
                .filter(|leaf| !leaf.identity.starts_with("recipe:"))
                .count(),
            2
        );

        // Test as_table_provider
        let table_provider = join_file
            .as_table_provider(FileID::root(), &provider_context)
            .await
            .unwrap();

        // Register and query
        let ctx = &provider_context.datafusion_session;
        _ = ctx.register_table("joined", table_provider).unwrap();

        let df = ctx
            .sql("SELECT * FROM joined ORDER BY timestamp")
            .await
            .unwrap();
        let physical = df.clone().create_physical_plan().await.unwrap();
        let plan = DisplayableExecutionPlan::new(physical.as_ref())
            .indent(true)
            .to_string();
        assert!(plan.contains("join_type=Full"), "{plan}");
        assert!(
            !plan.contains("MemoryExec"),
            "typed timeseries join materialized its inputs:\n{plan}"
        );
        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty());
        let batch = &batches[0];

        // Should have timestamps [1, 2, 3, 4] due to FULL OUTER JOIN
        assert_eq!(batch.num_rows(), 4);

        // Should have columns: timestamp, temp_a, temp_b
        assert_eq!(batch.num_columns(), 3);
    }

    #[tokio::test]
    async fn test_timeseries_join_with_scope_prefixes() {
        let (fs, provider_context) = create_test_environment().await;

        // Create source1.series with temp column
        let schema1 = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("temp", DataType::Float64, false),
        ]));
        let timestamps1 = TimestampSecondArray::from(vec![1, 2, 3]);
        let temps1 = Float64Array::from(vec![10.0, 20.0, 30.0]);
        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![Arc::new(timestamps1), Arc::new(temps1)],
        )
        .unwrap();
        let mut parquet_buffer1 = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer1);
            let mut writer1 = ArrowWriter::try_new(cursor, schema1, None).unwrap();
            writer1.write(&batch1).unwrap();
            _ = writer1.close().unwrap();
        }
        _ = create_parquet_file(
            &fs,
            "/source1.series",
            parquet_buffer1,
            EntryType::TablePhysicalSeries,
        )
        .await
        .unwrap();

        // Create source2.series with pressure column
        let schema2 = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("pressure", DataType::Float64, false),
        ]));
        let timestamps2 = TimestampSecondArray::from(vec![2, 3, 4]);
        let pressures = Float64Array::from(vec![100.0, 101.0, 102.0]);
        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![Arc::new(timestamps2), Arc::new(pressures)],
        )
        .unwrap();
        let mut parquet_buffer2 = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer2);
            let mut writer2 = ArrowWriter::try_new(cursor, schema2, None).unwrap();
            writer2.write(&batch2).unwrap();
            _ = writer2.close().unwrap();
        }
        _ = create_parquet_file(
            &fs,
            "/source2.series",
            parquet_buffer2,
            EntryType::TablePhysicalSeries,
        )
        .await
        .unwrap();

        // Test with scope prefixes
        let root_id = FileID::root();
        let context = test_context(&provider_context, root_id);

        let config = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///source1.series").unwrap(),
                    scope: Some("BDock".to_string()),
                    range: None,
                    transforms: None,
                },
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///source2.series").unwrap(),
                    scope: Some("ADock".to_string()),
                    range: None,
                    transforms: None,
                },
            ],
        };

        let join_file = TimeseriesJoinFile::new(config, context).unwrap();
        let table_provider = join_file
            .as_table_provider(root_id, &provider_context)
            .await
            .unwrap();

        // Query to verify scoped column names - use direct scan to avoid SQL optimizer
        let ctx = &provider_context.datafusion_session;
        let df_state = ctx.state();
        let plan = table_provider
            .scan(&df_state, None, &[], None)
            .await
            .unwrap();

        let task_ctx = ctx.task_ctx();
        let stream = plan.execute(0, task_ctx).unwrap();

        use futures::StreamExt;
        let batches: Vec<_> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        assert!(!batches.is_empty());
        let batch = &batches[0];
        let schema = batch.schema();

        // Verify column names include scope prefixes
        let column_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            column_names.contains(&"timestamp"),
            "Should have timestamp column"
        );
        assert!(
            column_names.contains(&"BDock.temp"),
            "Should have BDock.temp column"
        );
        assert!(
            column_names.contains(&"ADock.pressure"),
            "Should have ADock.pressure column"
        );
    }

    // Regression: a timeseries-join input whose glob matches zero nodes must
    // not break the whole query.  The generated SQL references every input by
    // its `inputN` alias, so before the empty-placeholder fix an unmatched
    // input produced "table 'inputN' not found" and aborted the build.  This
    // mirrors the noyo combine, where a decommissioned instrument has archive
    // data but no live `/hydrovu/...` series, leaving the live input empty.
    #[tokio::test]
    async fn test_timeseries_join_zero_match_input_is_empty_set() {
        let (fs, provider_context) = create_test_environment().await;

        // Helper to write a single-column series at `path`.
        async fn write_series(
            fs: &tinyfs::FS,
            path: &str,
            col: &str,
            ts: Vec<i64>,
            vals: Vec<f64>,
        ) {
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Second, None),
                    false,
                ),
                Field::new(col, DataType::Float64, false),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampSecondArray::from(ts)),
                    Arc::new(Float64Array::from(vals)),
                ],
            )
            .unwrap();
            let mut buf = Vec::new();
            {
                let cursor = Cursor::new(&mut buf);
                let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
                writer.write(&batch).unwrap();
                _ = writer.close().unwrap();
            }
            _ = create_parquet_file(fs, path, buf, EntryType::TablePhysicalSeries)
                .await
                .unwrap();
        }

        // "archive" (resolves) and a same-scope "live" input that matches
        // nothing, plus a second scope that resolves so we exercise the join.
        write_series(
            &fs,
            "/silver_archive.series",
            "temp",
            vec![1, 2, 3],
            vec![10.0, 20.0, 30.0],
        )
        .await;
        write_series(
            &fs,
            "/field_live.series",
            "pressure",
            vec![2, 3, 4],
            vec![100.0, 101.0, 102.0],
        )
        .await;

        let root_id = FileID::root();
        let context = test_context(&provider_context, root_id);

        let config = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///silver_archive.series").unwrap(),
                    scope: Some("Silver".to_string()),
                    range: None,
                    transforms: None,
                },
                // Decommissioned-instrument live counterpart: matches no node.
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///does_not_exist_*.series").unwrap(),
                    scope: Some("Silver".to_string()),
                    range: None,
                    transforms: None,
                },
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///field_live.series").unwrap(),
                    scope: Some("Field".to_string()),
                    range: None,
                    transforms: None,
                },
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///absent_scope_*.series").unwrap(),
                    scope: Some("Absent".to_string()),
                    range: None,
                    transforms: None,
                },
            ],
        };

        let join_file = TimeseriesJoinFile::new(config, context).unwrap();
        let table_provider = join_file
            .as_table_provider(root_id, &provider_context)
            .await
            .expect("zero-match input must not fail the join");

        // Re-evaluate the SAME node through a different ProviderContext that
        // shares the DataFusion session but has a fresh provider cache.  This
        // is what happens when a derived node is read more than once within a
        // single build (e.g. one sitegen export per resolution): the cache
        // misses, so registration runs again against the shared session.  The
        // empty placeholder must be registered idempotently and not fail with
        // "table 'sql_derived_empty_...' already exists".
        let provider_context2 = crate::ProviderContext::new(
            provider_context.datafusion_session.clone(),
            provider_context.persistence.clone(),
        );
        let _second = join_file
            .as_table_provider(root_id, &provider_context2)
            .await
            .expect("re-registering the empty placeholder must be idempotent");

        let ctx = &provider_context.datafusion_session;
        let df_state = ctx.state();
        let plan = table_provider
            .scan(&df_state, None, &[], None)
            .await
            .unwrap();
        let task_ctx = ctx.task_ctx();
        let stream = plan.execute(0, task_ctx).unwrap();

        use futures::StreamExt;
        let batches: Vec<_> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        assert!(!batches.is_empty(), "join should still produce rows");
        let column_names: Vec<String> = batches[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert!(
            column_names.iter().any(|n| n == "Silver.temp"),
            "resolved Silver input data must survive the empty same-scope sibling, got {column_names:?}"
        );
        assert!(
            column_names.iter().any(|n| n == "Field.pressure"),
            "second scope must be present, got {column_names:?}"
        );
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4, "union of timestamps 1,2,3,4 = 4 rows");
    }

    #[tokio::test]
    async fn test_timeseries_join_same_scope_non_overlapping_ranges() {
        // Test the Silver case: two Vulink devices with same scope but non-overlapping time ranges
        // This should use UNION BY NAME and produce proper column names with scope prefix
        let (fs, provider_context) = create_test_environment().await;

        // Create vulink1.series with temp and conductivity columns (timestamps 1-3)
        let schema1 = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("temp", DataType::Float64, false),
            Field::new("conductivity", DataType::Float64, false),
        ]));
        let timestamps1 = TimestampSecondArray::from(vec![1, 2, 3]);
        let temps1 = Float64Array::from(vec![10.0, 20.0, 30.0]);
        let cond1 = Float64Array::from(vec![100.0, 110.0, 120.0]);
        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![Arc::new(timestamps1), Arc::new(temps1), Arc::new(cond1)],
        )
        .unwrap();
        let mut parquet_buffer1 = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer1);
            let mut writer1 = ArrowWriter::try_new(cursor, schema1, None).unwrap();
            writer1.write(&batch1).unwrap();
            _ = writer1.close().unwrap();
        }
        _ = create_parquet_file(
            &fs,
            "/vulink1.series",
            parquet_buffer1,
            EntryType::TablePhysicalSeries,
        )
        .await
        .unwrap();

        // Create vulink2.series with same schema (timestamps 5-7, non-overlapping)
        let schema2 = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("temp", DataType::Float64, false),
            Field::new("conductivity", DataType::Float64, false),
        ]));
        let timestamps2 = TimestampSecondArray::from(vec![5, 6, 7]);
        let temps2 = Float64Array::from(vec![40.0, 50.0, 60.0]);
        let cond2 = Float64Array::from(vec![130.0, 140.0, 150.0]);
        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![Arc::new(timestamps2), Arc::new(temps2), Arc::new(cond2)],
        )
        .unwrap();
        let mut parquet_buffer2 = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer2);
            let mut writer2 = ArrowWriter::try_new(cursor, schema2, None).unwrap();
            writer2.write(&batch2).unwrap();
            _ = writer2.close().unwrap();
        }
        _ = create_parquet_file(
            &fs,
            "/vulink2.series",
            parquet_buffer2,
            EntryType::TablePhysicalSeries,
        )
        .await
        .unwrap();

        // Create at500.series with different columns (timestamps 2-6, overlapping both)
        let schema3 = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("pressure", DataType::Float64, false),
        ]));
        let timestamps3 = TimestampSecondArray::from(vec![2, 4, 6]);
        let pressures = Float64Array::from(vec![1000.0, 1010.0, 1020.0]);
        let batch3 = RecordBatch::try_new(
            schema3.clone(),
            vec![Arc::new(timestamps3), Arc::new(pressures)],
        )
        .unwrap();
        let mut parquet_buffer3 = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer3);
            let mut writer3 = ArrowWriter::try_new(cursor, schema3, None).unwrap();
            writer3.write(&batch3).unwrap();
            _ = writer3.close().unwrap();
        }
        _ = create_parquet_file(
            &fs,
            "/at500.series",
            parquet_buffer3,
            EntryType::TablePhysicalSeries,
        )
        .await
        .unwrap();

        // Test with same scope for both Vulinks
        let root_id = FileID::root();
        let context = test_context(&provider_context, root_id);

        let config = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///vulink1.series").unwrap(),
                    scope: Some("Vulink".to_string()),
                    range: Some(TimeRange {
                        begin: None,
                        end: Some("1970-01-01T00:00:03Z".to_string()),
                    }),
                    transforms: None,
                },
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///vulink2.series").unwrap(),
                    scope: Some("Vulink".to_string()),
                    range: Some(TimeRange {
                        begin: Some("1970-01-01T00:00:05Z".to_string()),
                        end: None,
                    }),
                    transforms: None,
                },
                TimeseriesInput {
                    pattern: crate::Url::parse("series:///at500.series").unwrap(),
                    scope: Some("AT500_Surface".to_string()),
                    range: None,
                    transforms: None,
                },
            ],
        };

        let join_file = TimeseriesJoinFile::new(config, context).unwrap();
        let table_provider = join_file
            .as_table_provider(root_id, &provider_context)
            .await
            .unwrap();

        // Query to verify scoped column names and UNION BY NAME behavior
        let ctx = &provider_context.datafusion_session;
        let df_state = ctx.state();
        let plan = table_provider
            .scan(&df_state, None, &[], None)
            .await
            .unwrap();

        let task_ctx = ctx.task_ctx();
        let stream = plan.execute(0, task_ctx).unwrap();

        use futures::StreamExt;
        let batches: Vec<_> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        assert!(!batches.is_empty());
        let batch = &batches[0];
        let schema = batch.schema();

        // Verify column names
        let column_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        assert!(
            column_names.contains(&"timestamp"),
            "Should have timestamp column"
        );
        assert!(
            column_names.contains(&"Vulink.temp"),
            "Should have Vulink.temp column"
        );
        assert!(
            column_names.contains(&"Vulink.conductivity"),
            "Should have Vulink.conductivity column"
        );
        assert!(
            column_names.contains(&"AT500_Surface.pressure"),
            "Should have AT500_Surface.pressure column"
        );

        // Verify we have all unique timestamps from all sources (1,2,3,4,5,6,7)
        assert_eq!(batch.num_rows(), 7, "Should have 7 unique timestamps");
    }

    #[tokio::test]
    async fn test_timeseries_join_csv_different_schemas() {
        // Test typed joins over CSV inputs with different schemas.
        let (fs, provider_context) = create_test_environment().await;

        // Create sensor1.csv with timestamp, temp, humidity
        let csv1_content = "timestamp,temp,humidity\n\
                           2024-01-01T00:00:00Z,20.5,65.0\n\
                           2024-01-01T01:00:00Z,21.0,63.0\n\
                           2024-01-01T02:00:00Z,21.5,62.0\n";

        _ = create_text_file(
            &fs,
            "/sensor1.csv",
            csv1_content.as_bytes().to_vec(),
            EntryType::FilePhysicalVersion,
        )
        .await
        .unwrap();

        // Create sensor2.csv with timestamp, temp, pressure (different schema - no humidity, has pressure)
        let csv2_content = "timestamp,temp,pressure\n\
                           2024-01-01T01:00:00Z,22.0,1013.0\n\
                           2024-01-01T02:00:00Z,22.5,1012.0\n\
                           2024-01-01T03:00:00Z,23.0,1011.0\n";

        _ = create_text_file(
            &fs,
            "/sensor2.csv",
            csv2_content.as_bytes().to_vec(),
            EntryType::FilePhysicalVersion,
        )
        .await
        .unwrap();

        // Create timeseries-join with CSV patterns
        let config = TimeseriesJoinConfig {
            time_column: "timestamp".to_string(),
            inputs: vec![
                TimeseriesInput {
                    pattern: crate::Url::parse("csv:///sensor1.csv").unwrap(),
                    range: None,
                    transforms: None,
                    scope: Some("sensor1".to_string()),
                },
                TimeseriesInput {
                    pattern: crate::Url::parse("csv:///sensor2.csv").unwrap(),
                    range: None,
                    transforms: None,
                    scope: Some("sensor2".to_string()),
                },
            ],
        };

        let factory_context = test_context(&provider_context, FileID::root());

        let join_file = TimeseriesJoinFile::new(config, factory_context).unwrap();

        // Typed join planning must handle schema differences.
        let table_provider = join_file
            .as_table_provider(FileID::root(), &provider_context)
            .await
            .expect("Should create table provider");

        // Query the joined data
        let ctx = SessionContext::new();
        _ = ctx.register_table("joined", table_provider).unwrap();
        let df = ctx
            .sql("SELECT * FROM joined ORDER BY timestamp")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty(), "Should have results");
        let batch = &batches[0];

        // Verify schema includes columns from both sensors (with NULL for missing values)
        let schema = batch.schema();
        assert!(
            schema.column_with_name("timestamp").is_some(),
            "Should have timestamp column"
        );
        assert!(
            schema.column_with_name("sensor1.temp").is_some(),
            "Should have sensor1.temp column"
        );
        assert!(
            schema.column_with_name("sensor1.humidity").is_some(),
            "Should have sensor1.humidity column (NULL for sensor2 rows)"
        );
        assert!(
            schema.column_with_name("sensor2.temp").is_some(),
            "Should have sensor2.temp column"
        );
        assert!(
            schema.column_with_name("sensor2.pressure").is_some(),
            "Should have sensor2.pressure column (NULL for sensor1 rows)"
        );

        // Verify we have all timestamps from both sensors (4 unique: 00:00, 01:00, 02:00, 03:00)
        assert_eq!(batch.num_rows(), 4, "Should have 4 unique timestamps");

        // Verify full-outer alignment filled NULLs correctly.
        // At 00:00: sensor1 has data, sensor2 should be NULL
        // At 01:00: both have data
        // At 02:00: both have data
        // At 03:00: sensor2 has data, sensor1 should be NULL
        use arrow::array::AsArray;
        let sensor1_humidity = batch
            .column_by_name("sensor1.humidity")
            .unwrap()
            .as_primitive::<arrow::datatypes::Float64Type>();
        let sensor2_pressure = batch
            .column_by_name("sensor2.pressure")
            .unwrap()
            .as_primitive::<arrow::datatypes::Float64Type>();

        // First row (00:00): sensor1.humidity=65.0, sensor2.pressure=NULL
        assert_eq!(sensor1_humidity.value(0), 65.0);
        assert!(
            sensor2_pressure.is_null(0),
            "sensor2.pressure should be NULL at 00:00"
        );

        // Last row (03:00): sensor1.humidity=NULL, sensor2.pressure=1011.0
        assert!(
            sensor1_humidity.is_null(3),
            "sensor1.humidity should be NULL at 03:00"
        );
        assert_eq!(sensor2_pressure.value(3), 1011.0);
    }
}
