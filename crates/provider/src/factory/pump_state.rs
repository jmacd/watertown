// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Streaming adaptive pump-state classification.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Column;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, cast, col, lit};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::StreamExt;
use query_foundation::plans::pump_state::{
    DepthSample, PumpPhase, PumpStateMachine, PumpStateRecipe, PumpStateRow,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinyfs::{EntryType, FileID, NodeMetadata, Result as TinyFSResult, ResultExt};

use crate::{FactoryContext, register_dynamic_factory};

const OUTPUT_BATCH_ROWS: usize = 8 * 1024;

/// Configuration for a typed adaptive pump-state series.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PumpStateConfig {
    /// Exact logical source URL.
    pub source: crate::Url,
    /// Source event-time column.
    pub time_column: String,
    /// Source well-depth column.
    pub depth_column: String,
    /// Width of the trailing ceiling.
    pub lookback: String,
    /// Minimum depth drop below the trailing ceiling that starts disturbance.
    pub disturbance_drop: f64,
    /// Inclusive valid depth floor.
    pub valid_depth_min: f64,
    /// Inclusive valid depth ceiling.
    pub valid_depth_max: f64,
}

impl PumpStateConfig {
    fn recipe(&self) -> TinyFSResult<PumpStateRecipe> {
        let lookback =
            humantime::parse_duration(&self.lookback).map_other_context("pump-state lookback")?;
        if lookback.subsec_nanos() != 0 || lookback.as_secs() % 60 != 0 {
            return Err(tinyfs::Error::Other(format!(
                "pump-state lookback '{}' must be an exact whole number of minutes",
                self.lookback
            )));
        }
        let minutes = i64::try_from(lookback.as_secs() / 60)
            .map_other_context("pump-state lookback exceeds i64 minutes")?;
        PumpStateRecipe::try_new(minutes, self.disturbance_drop)
            .map_err(|error| tinyfs::Error::Other(error.to_string()))
    }

    fn validate(&self) -> TinyFSResult<()> {
        if self.source.path().contains(['*', '?']) {
            return Err(tinyfs::Error::Other(
                "pump-state-series requires one exact logical source URL".to_owned(),
            ));
        }
        if self.time_column.is_empty() || self.depth_column.is_empty() {
            return Err(tinyfs::Error::Other(
                "pump-state-series column names must not be empty".to_owned(),
            ));
        }
        if !self.valid_depth_min.is_finite()
            || !self.valid_depth_max.is_finite()
            || self.valid_depth_min >= self.valid_depth_max
        {
            return Err(tinyfs::Error::Other(format!(
                "pump-state-series valid depth range [{}, {}] is invalid",
                self.valid_depth_min, self.valid_depth_max
            )));
        }
        _ = self.recipe()?;
        Ok(())
    }
}

struct PumpStateFile {
    config: PumpStateConfig,
    context: FactoryContext,
}

impl PumpStateFile {
    async fn source_provider(
        &self,
        context: &tinyfs::ProviderContext,
    ) -> TinyFSResult<Arc<dyn TableProvider>> {
        let root = self.context.root().await?;
        let fs = self.context.context.filesystem();
        let provider =
            crate::Provider::with_context(Arc::new(fs), Arc::new(context.clone())).with_root(root);
        provider
            .create_provider_for_url(&self.config.source.to_string(), &context.datafusion_session)
            .await
            .map_other_context("pump-state source provider")
    }
}

#[async_trait]
impl tinyfs::File for PumpStateFile {
    async fn async_reader(&self) -> TinyFSResult<Pin<Box<dyn tinyfs::AsyncReadSeek>>> {
        Err(tinyfs::Error::Other(
            "pump-state-series does not support direct byte reading".to_owned(),
        ))
    }

    async fn async_writer(&self) -> TinyFSResult<Pin<Box<dyn tinyfs::FileMetadataWriter>>> {
        Err(tinyfs::Error::Other(
            "pump-state-series is read-only".to_owned(),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_queryable(&self) -> Option<&dyn tinyfs::QueryableFile> {
        Some(self)
    }
}

#[async_trait]
impl tinyfs::Metadata for PumpStateFile {
    async fn metadata(&self) -> TinyFSResult<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: None,
            blake3: None,
            bao_outboard: None,
            entry_type: EntryType::TableDynamic,
            timestamp: 0,
        })
    }
}

#[async_trait]
impl tinyfs::QueryableFile for PumpStateFile {
    async fn as_table_provider(
        &self,
        _id: FileID,
        context: &tinyfs::ProviderContext,
    ) -> TinyFSResult<Arc<dyn TableProvider>> {
        context.record_global_plan();
        context.record_non_incremental_plan();
        log::info!(
            "query-plan visibility: node={} locality=global incremental=false reason=pump-state-boundary-cache-unavailable",
            self.context.file_id
        );

        let source = self.source_provider(context).await?;
        let source_schema = source.schema();
        _ = source_schema
            .field_with_name(&self.config.time_column)
            .map_other_context("pump-state time column")?;
        _ = source_schema
            .field_with_name(&self.config.depth_column)
            .map_other_context("pump-state depth column")?;
        let time = col(Column::from_name(&self.config.time_column));
        let depth = col(Column::from_name(&self.config.depth_column));
        let frame = context
            .datafusion_session
            .read_table(source)
            .map_other_context("read pump-state source")?
            .filter(
                time.clone()
                    .is_not_null()
                    .and(depth.clone().is_not_null())
                    .and(depth.clone().gt_eq(lit(self.config.valid_depth_min)))
                    .and(depth.clone().lt_eq(lit(self.config.valid_depth_max))),
            )
            .map_other_context("filter pump-state source")?
            .select(vec![
                cast(
                    time.clone(),
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                )
                .alias("timestamp"),
                cast(depth, DataType::Float64).alias("depth"),
            ])
            .map_other_context("project pump-state source")?
            .sort(vec![col("timestamp").sort(true, true)])
            .map_other_context("order pump-state source")?;
        let ordered: Arc<dyn TableProvider> = Arc::new(datafusion::catalog::view::ViewTable::new(
            frame.logical_plan().clone(),
            Some("ordered typed pump-state source".to_owned()),
        ));
        Ok(Arc::new(PumpStateTableProvider::new(
            ordered,
            self.config.recipe()?,
        )))
    }
}

#[derive(Debug)]
struct PumpStateTableProvider {
    source: Arc<dyn TableProvider>,
    recipe: PumpStateRecipe,
    schema: SchemaRef,
}

impl PumpStateTableProvider {
    fn new(source: Arc<dyn TableProvider>, recipe: PumpStateRecipe) -> Self {
        Self {
            source,
            recipe,
            schema: pump_state_schema(),
        }
    }
}

#[async_trait]
impl TableProvider for PumpStateTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let source = self.source.scan(state, None, &[], None).await?;
        if source.properties().output_partitioning().partition_count() != 1 {
            return Err(DataFusionError::Plan(format!(
                "pump-state ordered source must have one partition, got {}",
                source.properties().output_partitioning().partition_count()
            )));
        }
        Ok(Arc::new(PumpStateExec::new(
            source,
            self.recipe,
            projection.cloned(),
        )?))
    }
}

struct PumpStateExec {
    source: Arc<dyn ExecutionPlan>,
    recipe: PumpStateRecipe,
    projection: Option<Vec<usize>>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl PumpStateExec {
    fn new(
        source: Arc<dyn ExecutionPlan>,
        recipe: PumpStateRecipe,
        projection: Option<Vec<usize>>,
    ) -> DataFusionResult<Self> {
        let full_schema = pump_state_schema();
        let schema = match &projection {
            Some(projection) => Arc::new(full_schema.project(projection)?),
            None => full_schema,
        };
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            source.properties().emission_type,
            source.properties().boundedness,
        );
        Ok(Self {
            source,
            recipe,
            projection,
            schema,
            properties,
        })
    }
}

impl std::fmt::Debug for PumpStateExec {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("PumpStateExec").finish()
    }
}

impl DisplayAs for PumpStateExec {
    fn fmt_as(
        &self,
        _display_type: DisplayFormatType,
        formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(formatter, "PumpStateExec")
    }
}

impl ExecutionPlan for PumpStateExec {
    fn name(&self) -> &str {
        "PumpStateExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.source]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "PumpStateExec expects exactly one child".to_owned(),
            ));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.recipe,
            self.projection.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "PumpStateExec has only partition 0, got {partition}"
            )));
        }
        let mut source = self.source.execute(0, context)?;
        let recipe = self.recipe;
        let projection = self.projection.clone();
        let output_schema = Arc::clone(&self.schema);
        let stream_schema = Arc::clone(&output_schema);
        let stream = async_stream::try_stream! {
            let mut machine = PumpStateMachine::new(recipe);
            let mut pending = Vec::with_capacity(OUTPUT_BATCH_ROWS);
            while let Some(batch) = source.next().await {
                let batch = batch?;
                let timestamps = batch
                    .column_by_name("timestamp")
                    .ok_or_else(|| DataFusionError::Execution(
                        "pump-state source batch has no timestamp column".to_owned()
                    ))?
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| DataFusionError::Execution(
                        "pump-state source timestamp is not Timestamp(Microsecond)".to_owned()
                    ))?;
                let depths = batch
                    .column_by_name("depth")
                    .ok_or_else(|| DataFusionError::Execution(
                        "pump-state source batch has no depth column".to_owned()
                    ))?
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| DataFusionError::Execution(
                        "pump-state source depth is not Float64".to_owned()
                    ))?;
                for row in 0..batch.num_rows() {
                    let event_time = timestamps.value(row);
                    pending.extend(machine.push(DepthSample {
                        event_time,
                        minute: event_time.div_euclid(60_000_000),
                        depth: depths.value(row),
                    })?);
                    while pending.len() >= OUTPUT_BATCH_ROWS {
                        let remainder = pending.split_off(OUTPUT_BATCH_ROWS);
                        let emitted = std::mem::replace(&mut pending, remainder);
                        yield rows_to_batch(
                            &emitted,
                            projection.as_deref(),
                            Arc::clone(&output_schema),
                        )?;
                    }
                }
            }
            pending.extend(machine.finish().provisional_rows);
            if !pending.is_empty() {
                yield rows_to_batch(&pending, projection.as_deref(), output_schema)?;
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }
}

fn pump_state_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("depth", DataType::Float64, false),
        Field::new("phase", DataType::Utf8, false),
    ]))
}

fn rows_to_batch(
    rows: &[PumpStateRow],
    projection: Option<&[usize]>,
    output_schema: SchemaRef,
) -> DataFusionResult<RecordBatch> {
    let full_columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampMicrosecondArray::from_iter_values(
            rows.iter().map(|row| row.event_time),
        )),
        Arc::new(Float64Array::from_iter_values(
            rows.iter().map(|row| row.depth),
        )),
        Arc::new(StringArray::from_iter_values(rows.iter().map(
            |row| match row.phase {
                PumpPhase::Static => "static",
                PumpPhase::Pumping => "pumping",
                PumpPhase::Recovering => "recovering",
            },
        ))),
    ];
    let columns = match projection {
        Some(projection) => projection
            .iter()
            .map(|index| Arc::clone(&full_columns[*index]))
            .collect(),
        None => full_columns,
    };
    RecordBatch::try_new(output_schema, columns)
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

fn validate_config(config: &[u8]) -> TinyFSResult<Value> {
    let (value, config) = crate::factory::config_util::parse_yaml_config::<PumpStateConfig>(
        config,
        "Invalid pump-state-series config",
    )?;
    config.validate()?;
    Ok(value)
}

fn create_file(config: Value, context: FactoryContext) -> TinyFSResult<tinyfs::FileHandle> {
    let config: PumpStateConfig =
        crate::factory::config_util::config_from_value(config, "Invalid pump-state-series config")?;
    Ok(tinyfs::FileHandle::new(Arc::new(tokio::sync::Mutex::new(
        Box::new(PumpStateFile { config, context }),
    ))))
}

register_dynamic_factory!(
    name: "pump-state-series",
    description: "Classify adaptive pumping, recovering, and static well-depth episodes",
    file: create_file,
    validate: validate_config
);

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::record_batch::RecordBatch;
    use tinyfs::arrow::parquet::ParquetExt;

    fn source_batch() -> RecordBatch {
        let mut timestamps = Vec::new();
        let mut depths = Vec::new();
        for minute in 0..=60 {
            timestamps.push(minute * 60_000_000);
            depths.push(45.0);
        }
        for (minute, depth) in [
            (61, 44.5),
            (62, 44.0),
            (63, 43.5),
            (64, 43.8),
            (65, 44.2),
            (66, 44.8),
        ] {
            timestamps.push(minute * 60_000_000);
            depths.push(depth);
        }
        RecordBatch::try_from_iter([
            (
                "timestamp",
                Arc::new(TimestampMicrosecondArray::from(timestamps)) as ArrayRef,
            ),
            (
                "well_depth_value.avg",
                Arc::new(Float64Array::from(depths)) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn rows(batches: &[RecordBatch]) -> Vec<(i64, u64, String)> {
        let mut rows = Vec::new();
        for batch in batches {
            let timestamps = batch
                .column_by_name("timestamp")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let depths = batch
                .column_by_name("depth")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let phases = batch
                .column_by_name("phase")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            rows.extend((0..batch.num_rows()).map(|row| {
                (
                    timestamps.value(row),
                    depths.value(row).to_bits(),
                    phases.value(row).to_owned(),
                )
            }));
        }
        rows
    }

    #[tokio::test]
    async fn typed_classifier_matches_production_sql() {
        let (fs, provider_context) = crate::factory::test_support::create_test_environment().await;
        let root = fs.root().await.unwrap();
        _ = root
            .write_series_from_batch("/depth.series", &source_batch(), Some("timestamp"))
            .await
            .unwrap();

        let config = br#"
source: series:///depth.series
time_column: timestamp
depth_column: well_depth_value.avg
lookback: 60m
disturbance_drop: 0.3
valid_depth_min: 10.0
valid_depth_max: 60.0
"#;
        let handle = crate::FactoryRegistry::create_file(
            "pump-state-series",
            config,
            crate::factory::test_support::test_context(&provider_context, FileID::root()),
        )
        .await
        .unwrap();
        let file = handle.get_file().await;
        let guard = file.lock().await;
        let typed = guard
            .as_queryable()
            .unwrap()
            .as_table_provider(FileID::root(), &provider_context)
            .await
            .unwrap();
        drop(guard);
        let typed_batches = provider_context
            .datafusion_session
            .read_table(typed)
            .unwrap()
            .collect()
            .await
            .unwrap();

        let source_node = root.get_node_path("/depth.series").await.unwrap();
        let source = crate::create_table_provider(
            source_node.id(),
            &provider_context,
            crate::TableProviderOptions::default(),
        )
        .await
        .unwrap();
        _ = provider_context
            .datafusion_session
            .register_table("depth_source", source)
            .unwrap();
        let oracle = provider_context
            .datafusion_session
            .sql(
                r#"
WITH r AS (
  SELECT timestamp AS ts,
    CAST(EXTRACT(EPOCH FROM timestamp)/60 AS BIGINT) AS min_idx,
    "well_depth_value.avg" AS depth
  FROM depth_source
  WHERE "well_depth_value.avg" IS NOT NULL
    AND "well_depth_value.avg" BETWEEN 10 AND 60
),
base AS (
  SELECT ts, min_idx, depth,
    MAX(depth) OVER (ORDER BY min_idx
      RANGE BETWEEN 60 PRECEDING AND CURRENT ROW) AS ceil60
  FROM r
),
dist AS (
  SELECT ts, min_idx, depth,
    CASE WHEN depth < ceil60 - 0.3 THEN 1 ELSE 0 END AS disturbed
  FROM base
),
isl AS (
  SELECT ts, min_idx, depth, disturbed,
    min_idx - ROW_NUMBER() OVER (
      PARTITION BY disturbed ORDER BY min_idx) AS grp
  FROM dist
),
troughed AS (
  SELECT min_idx,
    first_value(min_idx) OVER (PARTITION BY grp
      ORDER BY depth ASC, min_idx ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
      AS trough_idx
  FROM isl WHERE disturbed = 1
)
SELECT d.ts AS timestamp, d.depth,
  CASE
    WHEN d.disturbed = 1 AND d.min_idx <= t.trough_idx THEN 'pumping'
    WHEN d.disturbed = 1 THEN 'recovering'
    ELSE 'static'
  END AS phase
FROM dist d
LEFT JOIN troughed t USING (min_idx)
ORDER BY d.ts
"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(rows(&typed_batches), rows(&oracle));
        assert_eq!(
            provider_context
                .plan_visibility_metrics()
                .non_incremental_plans,
            1
        );
    }

    #[test]
    fn rejects_wildcard_and_non_minute_config() {
        let wildcard = br#"
source: series:///depth/*
time_column: timestamp
depth_column: depth
lookback: 60m
disturbance_drop: 0.3
valid_depth_min: 10.0
valid_depth_max: 60.0
"#;
        assert!(
            crate::FactoryRegistry::validate_config("pump-state-series", wildcard)
                .unwrap_err()
                .to_string()
                .contains("one exact logical source URL")
        );
        let fractional = std::str::from_utf8(wildcard)
            .unwrap()
            .replace("series:///depth/*", "series:///depth")
            .replace("60m", "90s");
        assert!(
            crate::FactoryRegistry::validate_config("pump-state-series", fractional.as_bytes())
                .unwrap_err()
                .to_string()
                .contains("whole number of minutes")
        );
    }
}
