// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Storage-agnostic transactional streaming materialization.

use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;

use crate::frontier::SettledState;
use crate::statistics::TimeInterval;

/// Progress made authoritative with one materialized output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializationProgress {
    recipe_id: Arc<str>,
    source_state_id: Arc<str>,
    settled: SettledState,
}

impl MaterializationProgress {
    /// Validate exact recipe, source-state, and frontier progress.
    pub fn try_new(
        recipe_id: impl Into<Arc<str>>,
        source_state_id: impl Into<Arc<str>>,
        settled: SettledState,
    ) -> Result<Self> {
        let recipe_id = recipe_id.into();
        let source_state_id = source_state_id.into();
        if recipe_id.is_empty() {
            return Err(DataFusionError::Plan(
                "materialization recipe identity must not be empty".to_owned(),
            ));
        }
        if source_state_id.is_empty() {
            return Err(DataFusionError::Plan(
                "materialization source-state identity must not be empty".to_owned(),
            ));
        }
        Ok(Self {
            recipe_id,
            source_state_id,
            settled,
        })
    }

    /// Semantic recipe identity.
    #[must_use]
    pub fn recipe_id(&self) -> &str {
        &self.recipe_id
    }

    /// Exact logical source-state identity.
    #[must_use]
    pub fn source_state_id(&self) -> &str {
        &self.source_state_id
    }

    /// Source-supplied frontier state.
    #[must_use]
    pub fn settled(&self) -> SettledState {
        self.settled
    }
}

/// Exact metadata computed while consuming one output stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializedOutput {
    output_id: Arc<str>,
    rows: u64,
    event_time_bounds: TimeInterval,
}

impl MaterializedOutput {
    /// Staged output identity.
    #[must_use]
    pub fn output_id(&self) -> &str {
        &self.output_id
    }

    /// Exact output row count.
    #[must_use]
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Exact inclusive output event-time bounds.
    #[must_use]
    pub fn event_time_bounds(&self) -> TimeInterval {
        self.event_time_bounds
    }
}

/// One atomic output/progress publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializationCommit {
    /// Output metadata, absent for a no-row run.
    pub output: Option<MaterializedOutput>,
    /// Progress that becomes authoritative in the same transaction.
    pub progress: MaterializationProgress,
}

/// Measured active work while streaming one materialization.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MaterializationMetrics {
    /// Non-empty batches written.
    pub batches_written: u64,
    /// Rows written.
    pub rows_written: u64,
    /// Largest individual input batch.
    pub peak_batch_rows: u64,
}

/// Result of one successful atomic materialization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializationOutcome {
    /// Published output metadata, absent when the stream contained no rows.
    pub output: Option<MaterializedOutput>,
    /// Bounded streaming work.
    pub metrics: MaterializationMetrics,
}

/// One hidden staged writer.
///
/// `commit` must atomically publish its optional output and progress. If it
/// returns an error, neither may be authoritative.
#[async_trait]
pub trait TransactionalBatchWriter: Send {
    /// Write one non-empty batch to hidden staged state.
    async fn write(&mut self, batch: &RecordBatch) -> Result<()>;

    /// Finish staged output bytes without making them visible.
    async fn close(&mut self) -> Result<()>;

    /// Atomically publish output and progress, consuming the stage.
    async fn commit(self: Box<Self>, commit: MaterializationCommit) -> Result<()>;

    /// Discard hidden staged state.
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// Factory for one hidden transactional output stage.
#[async_trait]
pub trait TransactionalMaterializationSink: Sync {
    /// Begin staging one stable output identity.
    async fn begin(&self, output_id: &str) -> Result<Box<dyn TransactionalBatchWriter>>;
}

/// Consume a DataFusion stream without collecting, then atomically publish its
/// exact metadata and progress.
pub async fn materialize_stream(
    sink: &dyn TransactionalMaterializationSink,
    output_id: impl Into<Arc<str>>,
    event_time: &str,
    progress: MaterializationProgress,
    mut stream: SendableRecordBatchStream,
) -> Result<MaterializationOutcome> {
    let output_id = output_id.into();
    if output_id.is_empty() {
        return Err(DataFusionError::Plan(
            "materialization output identity must not be empty".to_owned(),
        ));
    }
    if event_time.is_empty() {
        return Err(DataFusionError::Plan(
            "materialization event-time column must not be empty".to_owned(),
        ));
    }
    let mut writer = sink.begin(&output_id).await?;
    let mut metrics = MaterializationMetrics::default();
    let mut bounds = None;

    while let Some(batch) = stream.next().await {
        let batch = match batch {
            Ok(batch) => batch,
            Err(error) => return abort_after(writer, error).await,
        };
        if batch.num_rows() == 0 {
            continue;
        }
        let batch_bounds = match event_time_bounds(&batch, event_time) {
            Ok(bounds) => bounds,
            Err(error) => return abort_after(writer, error).await,
        };
        let next_bounds = match bounds {
            Some(current) => match union_bounds(current, batch_bounds) {
                Ok(bounds) => bounds,
                Err(error) => return abort_after(writer, error).await,
            },
            None => batch_bounds,
        };
        if let Err(error) = writer.write(&batch).await {
            return abort_after(writer, error).await;
        }
        metrics.batches_written += 1;
        metrics.rows_written += batch.num_rows() as u64;
        metrics.peak_batch_rows = metrics.peak_batch_rows.max(batch.num_rows() as u64);
        bounds = Some(next_bounds);
    }

    if let Err(error) = writer.close().await {
        return abort_after(writer, error).await;
    }
    let output = bounds.map(|event_time_bounds| MaterializedOutput {
        output_id,
        rows: metrics.rows_written,
        event_time_bounds,
    });
    writer
        .commit(MaterializationCommit {
            output: output.clone(),
            progress,
        })
        .await?;
    Ok(MaterializationOutcome { output, metrics })
}

fn event_time_bounds(batch: &RecordBatch, event_time: &str) -> Result<TimeInterval> {
    let column = batch
        .column_by_name(event_time)
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "materialization batch is missing event-time column '{event_time}'"
            ))
        })?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "materialization event-time column '{event_time}' must be Int64"
            ))
        })?;
    if column.null_count() != 0 {
        return Err(DataFusionError::Execution(format!(
            "materialization event-time column '{event_time}' must not contain nulls"
        )));
    }
    let mut minimum = i64::MAX;
    let mut maximum = i64::MIN;
    for index in 0..column.len() {
        minimum = minimum.min(column.value(index));
        maximum = maximum.max(column.value(index));
    }
    TimeInterval::try_new(minimum, maximum)
}

fn union_bounds(left: TimeInterval, right: TimeInterval) -> Result<TimeInterval> {
    TimeInterval::try_new(left.min().min(right.min()), left.max().max(right.max()))
}

async fn abort_after<T>(
    writer: Box<dyn TransactionalBatchWriter>,
    error: DataFusionError,
) -> Result<T> {
    match writer.abort().await {
        Ok(()) => Err(error),
        Err(abort_error) => Err(DataFusionError::Execution(format!(
            "materialization failed: {error}; staged-output abort also failed: {abort_error}"
        ))),
    }
}
