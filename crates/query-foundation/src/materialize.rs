// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Storage-agnostic transactional streaming materialization.

use std::sync::Arc;

use arrow::array::{
    Array, Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::Column;
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{col, lit};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;

use crate::frontier::{ChangeDisposition, SettledState};
use crate::locality::ChangeExtent;
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

/// Why existing output intervals must be replaced.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterializationReplaceReason {
    /// New rows arrived inside the active unsealed region.
    UnsealedDisorder,
    /// New rows arrived at or behind the settled frontier.
    RetroactiveRepair,
}

/// Atomic visibility operation for one materialization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaterializationPublication {
    /// Publish no output. Sinks with an independent frontier may advance
    /// progress; target-derived append-only sinks may commit a no-op.
    NoOutput,
    /// Add output strictly after the prior observed event time.
    Append {
        /// Previous inclusive observed position, absent for first output.
        after: Option<i64>,
    },
    /// Replace complete affected output intervals.
    Replace {
        /// Normalized intervals replaced by this output.
        ranges: Vec<TimeInterval>,
        /// Why replacement rather than append is required.
        reason: MaterializationReplaceReason,
    },
}

/// One atomic output/progress publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializationCommit {
    /// Output metadata, absent for a no-row run.
    pub output: Option<MaterializedOutput>,
    /// Visibility operation applied atomically with progress.
    pub publication: MaterializationPublication,
    /// Progress associated with the publication. Sinks with target-derived
    /// append-only progress need not persist it for `NoOutput`.
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
    /// Visibility operation committed with progress.
    pub publication: MaterializationPublication,
    /// Bounded streaming work.
    pub metrics: MaterializationMetrics,
}

/// Exact raw-input predicate for one materialization execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaterializationInput {
    lower: i64,
    lower_inclusive: bool,
    upper_inclusive: i64,
}

impl MaterializationInput {
    /// Lower event-time boundary.
    #[must_use]
    pub fn lower(self) -> i64 {
        self.lower
    }

    /// Whether the lower event-time boundary is inclusive.
    #[must_use]
    pub fn lower_inclusive(self) -> bool {
        self.lower_inclusive
    }

    /// Inclusive upper event-time boundary.
    #[must_use]
    pub fn upper_inclusive(self) -> i64 {
        self.upper_inclusive
    }
}

/// Classified bounded work and atomic publication intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializationPlan {
    input: Option<MaterializationInput>,
    publication: MaterializationPublication,
}

impl MaterializationPlan {
    /// Exact input predicate, absent when no source execution is needed.
    #[must_use]
    pub fn input(&self) -> Option<MaterializationInput> {
        self.input
    }

    /// Atomic visibility operation.
    #[must_use]
    pub fn publication(&self) -> &MaterializationPublication {
        &self.publication
    }

    /// Apply the exact input predicate to an Int64 event-time frame.
    ///
    /// Returns `None` when progress can advance without source execution.
    pub fn filter(&self, frame: DataFrame, event_time: &str) -> Result<Option<DataFrame>> {
        let Some(input) = self.input else {
            return Ok(None);
        };
        let field = frame
            .schema()
            .field_with_unqualified_name(event_time)
            .map_err(|_| {
                DataFusionError::Plan(format!(
                    "materialization input is missing event-time column '{event_time}'"
                ))
            })?;
        if field.data_type() != &DataType::Int64 {
            return Err(DataFusionError::Plan(format!(
                "materialization event-time column '{event_time}' must be Int64, got {}",
                field.data_type()
            )));
        }
        let event_time = col(Column::from_name(event_time));
        let lower = if input.lower_inclusive {
            event_time.clone().gt_eq(lit(input.lower))
        } else {
            event_time.clone().gt(lit(input.lower))
        };
        Ok(Some(frame.filter(
            lower.and(event_time.lt_eq(lit(input.upper_inclusive))),
        )?))
    }
}

/// Classify one logical change into exact input and publication work.
pub fn plan_materialization_change(
    prior: SettledState,
    extent: ChangeExtent,
) -> Result<MaterializationPlan> {
    let disposition = prior.classify(extent)?;
    let (input, publication) = match disposition {
        ChangeDisposition::NoRows => (None, MaterializationPublication::NoOutput),
        ChangeDisposition::Append { changed } => {
            let after = prior.observed_through();
            let input = MaterializationInput {
                lower: after.unwrap_or(changed.min()),
                lower_inclusive: after.is_none(),
                upper_inclusive: changed.max(),
            };
            (Some(input), MaterializationPublication::Append { after })
        }
        ChangeDisposition::UnsealedDisorder { changed } => (
            Some(inclusive_input(changed)),
            MaterializationPublication::Replace {
                ranges: vec![changed],
                reason: MaterializationReplaceReason::UnsealedDisorder,
            },
        ),
        ChangeDisposition::Repair { changed, .. } => (
            Some(inclusive_input(changed)),
            MaterializationPublication::Replace {
                ranges: vec![changed],
                reason: MaterializationReplaceReason::RetroactiveRepair,
            },
        ),
    };
    Ok(MaterializationPlan { input, publication })
}

/// One hidden staged writer.
///
/// `commit` must atomically publish its optional output and any durable
/// progress supported by the sink. If it returns an error, neither may be
/// authoritative. A target-derived append-only sink may commit `NoOutput`
/// without writing progress.
#[allow(clippy::double_must_use)]
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
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait TransactionalMaterializationSink: Sync {
    /// Begin staging one stable output identity for a validated publication
    /// operation. Implementations must reject unsupported publication modes
    /// before opening a writer.
    async fn begin(
        &self,
        output_id: &str,
        publication: &MaterializationPublication,
    ) -> Result<Box<dyn TransactionalBatchWriter>>;
}

/// Consume a DataFusion stream without collecting, then atomically publish its
/// exact metadata and applicable progress.
pub async fn materialize_stream(
    sink: &dyn TransactionalMaterializationSink,
    output_id: impl Into<Arc<str>>,
    event_time: &str,
    progress: MaterializationProgress,
    publication: MaterializationPublication,
    stream: SendableRecordBatchStream,
) -> Result<MaterializationOutcome> {
    materialize_stream_with_progress(
        sink,
        output_id,
        event_time,
        move |_| Ok(progress),
        publication,
        stream,
    )
    .await
}

/// Consume a DataFusion stream without collecting, derive exact progress from
/// the consumed output, then atomically publish both.
///
/// The deferred callback lets an append-only adapter use the stream's measured
/// upper event-time bound as its source frontier without executing a separate
/// full-source aggregate. Callback failure aborts the hidden output stage.
pub async fn materialize_stream_with_progress<F>(
    sink: &dyn TransactionalMaterializationSink,
    output_id: impl Into<Arc<str>>,
    event_time: &str,
    progress: F,
    publication: MaterializationPublication,
    mut stream: SendableRecordBatchStream,
) -> Result<MaterializationOutcome>
where
    F: FnOnce(Option<&MaterializedOutput>) -> Result<MaterializationProgress>,
{
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
    validate_publication(&publication)?;
    let mut writer = sink.begin(&output_id, &publication).await?;
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
        let event_times = match normalized_event_times(&batch, event_time) {
            Ok(values) => values,
            Err(error) => return abort_after(writer, error).await,
        };
        let batch_bounds = match event_time_bounds(&event_times) {
            Ok(bounds) => bounds,
            Err(error) => return abort_after(writer, error).await,
        };
        if let Err(error) = validate_batch_publication(&event_times, &publication) {
            return abort_after(writer, error).await;
        }
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
    let publication = match (output.is_some(), publication) {
        (true, MaterializationPublication::NoOutput) => {
            return abort_after(
                writer,
                DataFusionError::Plan(
                    "non-empty materialization cannot use NoOutput publication".to_owned(),
                ),
            )
            .await;
        }
        (false, MaterializationPublication::Append { .. }) => MaterializationPublication::NoOutput,
        (_, publication) => publication,
    };
    let progress = match progress(output.as_ref()) {
        Ok(progress) => progress,
        Err(error) => return abort_after(writer, error).await,
    };
    writer
        .commit(MaterializationCommit {
            output: output.clone(),
            publication: publication.clone(),
            progress,
        })
        .await?;
    Ok(MaterializationOutcome {
        output,
        publication,
        metrics,
    })
}

fn inclusive_input(interval: TimeInterval) -> MaterializationInput {
    MaterializationInput {
        lower: interval.min(),
        lower_inclusive: true,
        upper_inclusive: interval.max(),
    }
}

fn validate_publication(publication: &MaterializationPublication) -> Result<()> {
    if let MaterializationPublication::Replace { ranges, .. } = publication {
        if ranges.is_empty() {
            return Err(DataFusionError::Plan(
                "replacement publication requires at least one output interval".to_owned(),
            ));
        }
        for pair in ranges.windows(2) {
            let touches_or_overlaps = pair[0]
                .max()
                .checked_add(1)
                .is_none_or(|next| pair[1].min() <= next);
            if touches_or_overlaps {
                return Err(DataFusionError::Plan(
                    "replacement publication intervals must be ordered, disjoint, and normalized"
                        .to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_batch_publication(
    event_times: &[i64],
    publication: &MaterializationPublication,
) -> Result<()> {
    for value in event_times {
        let valid = match publication {
            MaterializationPublication::NoOutput => false,
            MaterializationPublication::Append { after } => {
                after.is_none_or(|boundary| *value > boundary)
            }
            MaterializationPublication::Replace { ranges, .. } => ranges
                .iter()
                .any(|range| *value >= range.min() && *value <= range.max()),
        };
        if !valid {
            return Err(DataFusionError::Execution(format!(
                "materialization event time {value} falls outside declared publication boundaries {publication:?}"
            )));
        }
    }
    Ok(())
}

fn normalized_event_times(batch: &RecordBatch, event_time: &str) -> Result<Vec<i64>> {
    let column = batch.column_by_name(event_time).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "materialization batch is missing event-time column '{event_time}'"
        ))
    })?;
    if column.null_count() != 0 {
        return Err(DataFusionError::Execution(format!(
            "materialization event-time column '{event_time}' must not contain nulls"
        )));
    }
    macro_rules! normalized {
        ($array:ty, $convert:expr) => {
            if let Some(values) = column.as_any().downcast_ref::<$array>() {
                return values
                    .values()
                    .iter()
                    .copied()
                    .map($convert)
                    .collect::<Result<Vec<_>>>();
            }
        };
    }
    normalized!(Int64Array, |value: i64| Ok(value));
    normalized!(TimestampMicrosecondArray, |value: i64| Ok(value));
    normalized!(TimestampSecondArray, |value: i64| value
        .checked_mul(1_000_000)
        .ok_or_else(|| DataFusionError::Execution(
            "materialization second timestamp overflows microseconds".to_owned()
        )));
    normalized!(TimestampMillisecondArray, |value: i64| value
        .checked_mul(1_000)
        .ok_or_else(|| DataFusionError::Execution(
            "materialization millisecond timestamp overflows microseconds".to_owned()
        )));
    normalized!(TimestampNanosecondArray, |value: i64| Ok(
        value.div_euclid(1_000)
    ));
    Err(DataFusionError::Execution(format!(
        "materialization event-time column '{event_time}' must be Int64 or Timestamp, got {}",
        column.data_type()
    )))
}

fn event_time_bounds(event_times: &[i64]) -> Result<TimeInterval> {
    let mut minimum = i64::MAX;
    let mut maximum = i64::MIN;
    for value in event_times {
        minimum = minimum.min(*value);
        maximum = maximum.max(*value);
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
