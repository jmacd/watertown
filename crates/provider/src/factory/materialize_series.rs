// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Materialize a derived series into a physical `TablePhysicalSeries`.
//!
//! Watertown's typed signals are normally *derived*: `sql-derived-series` and
//! `timeseries-join` nodes recompute their output from the underlying ingested
//! bytes on every read.  That is the right default -- it costs no storage and
//! can never go stale -- but it means the cost of a query grows with the whole
//! history, and it means a pond built only from log ingest contains no
//! `TablePhysicalSeries` at all.
//!
//! This factory turns such a signal into a stored one.  Each run it asks the
//! target series how far it has already been materialized, selects only the
//! source rows beyond that watermark, and appends them as ONE new version.
//! The result is an append-only physical series with one version per tick --
//! the same shape `hydrovu` produces and the local pack-maintenance path can
//! repack without rewriting or reclaiming its Oplog rows.
//!
//! It is deliberately incremental rather than a snapshot-and-replace: a
//! rewrite-everything materializer would reintroduce exactly the `O(N^2)` write
//! amplification that size-tiered collapse exists to remove.

use crate::{ExecutionContext, FactoryContext, register_executable_factory};
use arrow::datatypes::{DataType, TimeUnit};
use clap::{Parser, Subcommand};
use datafusion::common::Column;
use datafusion::functions_aggregate::expr_fn::max;
use datafusion::prelude::{SessionContext, col, lit};
use datafusion::scalar::ScalarValue;
use log::{debug, info, warn};
use query_foundation::frontier::{RepairPolicy, SettledState};
use query_foundation::materialize::{
    MaterializationProgress, MaterializationPublication, materialize_stream_with_progress,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tinyfs::Result as TinyFSResult;
use tinyfs::ResultExt;

/// Subcommands, mirroring the ingest factories so `pond run <node> push`
/// works uniformly across everything a tick invokes.
#[derive(Debug, Parser)]
struct MaterializeCommand {
    #[command(subcommand)]
    command: Option<MaterializeSubcommand>,
}

#[derive(Debug, Subcommand)]
enum MaterializeSubcommand {
    /// Append any source rows beyond the target's watermark (the default).
    Push,
    /// Accepted for uniformity; materialization only ever flows one way.
    Pull,
}

fn parse_command(ctx: ExecutionContext) -> Result<MaterializeCommand, tinyfs::Error> {
    let args: Vec<String> = std::iter::once("factory".to_string())
        .chain(ctx.args().iter().cloned())
        .collect();
    MaterializeCommand::try_parse_from(args)
        .map_err(|e| tinyfs::Error::Other(format!("Command parse error: {}", e)))
}

/// Configuration for the materialize-series factory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializeSeriesConfig {
    /// URL of the signal to materialize, e.g.
    /// `series:///derived/p-water-prod`.  Anything the provider can turn into
    /// a table works, so a derived node, a physical series, or a raw log read
    /// through a format scheme are all valid sources.
    pub source: crate::Url,

    /// Pond path of the `TablePhysicalSeries` to append to.  Created on the
    /// first run.
    pub target: String,

    /// Event-time column, present in the source and used both as the
    /// watermark and as the series' temporal index.
    pub time_column: String,
}

impl MaterializeSeriesConfig {
    fn validate(&self) -> TinyFSResult<()> {
        if self.target.is_empty() {
            return Err(tinyfs::Error::Other(
                "materialize-series: `target` must not be empty".to_string(),
            ));
        }
        if self.time_column.is_empty() {
            return Err(tinyfs::Error::Other(
                "materialize-series: `time_column` must not be empty".to_string(),
            ));
        }
        Ok(())
    }
}

fn validate_config(config: &[u8]) -> TinyFSResult<Value> {
    let config: MaterializeSeriesConfig =
        serde_yaml::from_slice(config).map_other_context("Invalid config YAML")?;
    config.validate()?;
    serde_json::to_value(&config).map_other_context("Failed to serialize config")
}

async fn initialize(_config: Value, _context: FactoryContext) -> Result<(), tinyfs::Error> {
    // The target series is created lazily by the first append, so there is
    // nothing to set up.
    Ok(())
}

/// Build a DataFusion table for `url` in this pond's context.
async fn table_for(
    context: &FactoryContext,
    url: &str,
    ctx: &SessionContext,
) -> Result<Arc<dyn datafusion::catalog::TableProvider>, tinyfs::Error> {
    table_for_bounded(context, url, ctx, tinyfs::SeriesReadBounds::NONE).await
}

async fn table_for_bounded(
    context: &FactoryContext,
    url: &str,
    ctx: &SessionContext,
    bounds: tinyfs::SeriesReadBounds,
) -> Result<Arc<dyn datafusion::catalog::TableProvider>, tinyfs::Error> {
    let fs = context.context.filesystem();
    let mut provider =
        crate::Provider::with_context(Arc::new(fs), Arc::new(context.context.clone()));
    if let Ok(root) = context.root().await {
        provider = provider.with_root(root);
    }
    provider
        .create_provider_for_url_bounded(url, ctx, bounds)
        .await
        .map_err(|e| tinyfs::Error::Other(format!("materialize-series: source '{url}': {e}")))
}

/// The largest event time in one frame, or `None` when it has no rows.
async fn max_event_time(
    frame: datafusion::dataframe::DataFrame,
    time_column: &str,
    context: &str,
) -> Result<Option<ScalarValue>, tinyfs::Error> {
    let batches = frame
        .aggregate(
            vec![],
            vec![max(col(Column::from_name(time_column))).alias("watermark")],
        )
        .map_err(|error| tinyfs::Error::Other(format!("{context}: plan maximum: {error}")))?
        .collect()
        .await
        .map_err(|error| tinyfs::Error::Other(format!("{context}: scan maximum: {error}")))?;
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let scalar = ScalarValue::try_from_array(batch.column(0), 0)
            .map_err(|error| tinyfs::Error::Other(format!("{context}: decode maximum: {error}")))?;
        if !scalar.is_null() {
            return Ok(Some(scalar));
        }
    }
    Ok(None)
}

/// One metadata-derived target watermark plan.
#[derive(Debug)]
enum TargetWatermark {
    /// The target has no live versions.
    Empty,
    /// Per-version microsecond bounds exactly represent this event-time type.
    Exact(ScalarValue),
    /// Nanosecond values require an exact tail query because persisted bounds
    /// intentionally have microsecond precision.
    BoundedNanosecond { lower: ScalarValue, lower_us: i64 },
    /// A legacy version has no usable bound; retain the full-scan fallback.
    FullScan { reason: String },
}

fn target_watermark_from_metadata(
    versions: &[tinyfs::FileVersionInfo],
    time_column: &str,
    data_type: &DataType,
) -> TinyFSResult<TargetWatermark> {
    if versions.is_empty() {
        return Ok(TargetWatermark::Empty);
    }

    let mut max_us = None;
    for version in versions {
        let Some(metadata) = version.extended_metadata.as_ref() else {
            return Ok(TargetWatermark::FullScan {
                reason: format!("version {} has no extended metadata", version.version),
            });
        };
        let Some(raw_max) = metadata.get("max_event_time") else {
            return Ok(TargetWatermark::FullScan {
                reason: format!("version {} has no max_event_time", version.version),
            });
        };
        let parsed_max = raw_max.parse::<i64>().map_err(|error| {
            tinyfs::Error::Other(format!(
                "materialize-series: target version {} has invalid max_event_time {raw_max:?}: \
                 {error}",
                version.version
            ))
        })?;
        let Some(raw_attributes) = metadata.get("extended_attributes") else {
            return Ok(TargetWatermark::FullScan {
                reason: format!("version {} has no logical attributes", version.version),
            });
        };
        let attributes: Value = serde_json::from_str(raw_attributes).map_other_context(format!(
            "materialize-series: target version {} has invalid logical attributes",
            version.version
        ))?;
        let recorded_column = attributes
            .get("watertown.timestamp_column")
            .and_then(Value::as_str);
        match recorded_column {
            None => {
                return Ok(TargetWatermark::FullScan {
                    reason: format!(
                        "version {} has no recorded timestamp column",
                        version.version
                    ),
                });
            }
            Some(recorded) if recorded != time_column => {
                return Err(tinyfs::Error::Other(format!(
                    "materialize-series: target version {} records timestamp column \
                     {recorded:?}, expected {time_column:?}",
                    version.version
                )));
            }
            Some(_) => {}
        }
        max_us = Some(max_us.map_or(parsed_max, |current: i64| current.max(parsed_max)));
    }

    let max_us = max_us.expect("nonempty versions yielded at least one bound");
    let scalar = match data_type {
        DataType::Int64 => TargetWatermark::Exact(ScalarValue::Int64(Some(max_us))),
        DataType::Timestamp(TimeUnit::Second, timezone) => {
            if max_us.rem_euclid(1_000_000) != 0 {
                return Ok(TargetWatermark::FullScan {
                    reason: format!(
                        "microsecond watermark {max_us} is not aligned to whole seconds"
                    ),
                });
            }
            TargetWatermark::Exact(ScalarValue::TimestampSecond(
                Some(max_us.div_euclid(1_000_000)),
                timezone.clone(),
            ))
        }
        DataType::Timestamp(TimeUnit::Millisecond, timezone) => {
            if max_us.rem_euclid(1_000) != 0 {
                return Ok(TargetWatermark::FullScan {
                    reason: format!(
                        "microsecond watermark {max_us} is not aligned to whole milliseconds"
                    ),
                });
            }
            TargetWatermark::Exact(ScalarValue::TimestampMillisecond(
                Some(max_us.div_euclid(1_000)),
                timezone.clone(),
            ))
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => TargetWatermark::Exact(
            ScalarValue::TimestampMicrosecond(Some(max_us), timezone.clone()),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, timezone) => {
            let lower_ns = max_us.checked_mul(1_000).ok_or_else(|| {
                tinyfs::Error::Other(format!(
                    "materialize-series: target watermark {max_us}µs overflows nanoseconds"
                ))
            })?;
            // Historical nanosecond bounds used integer division toward zero.
            // For non-positive values, retain the complete microsecond bin so
            // the exact tail scan cannot prune a pre-epoch maximum.
            let lower_ns = if max_us <= 0 {
                lower_ns.checked_sub(999).ok_or_else(|| {
                    tinyfs::Error::Other(format!(
                        "materialize-series: target watermark {max_us}µs underflows nanoseconds"
                    ))
                })?
            } else {
                lower_ns
            };
            TargetWatermark::BoundedNanosecond {
                lower: ScalarValue::TimestampNanosecond(Some(lower_ns), timezone.clone()),
                lower_us: max_us,
            }
        }
        other => {
            return Err(tinyfs::Error::Other(format!(
                "materialize-series: unsupported event-time type {other}"
            )));
        }
    };
    Ok(scalar)
}

/// The largest event time already materialized into `target`, or `None` when
/// the target does not exist yet.
///
/// Every live physical-series version records its event-time maximum in epoch
/// microseconds. Taking the maximum across all live versions remains correct
/// after size-tiered collapse, unlike inspecting only the numerically newest
/// version. Microsecond and coarser target types use that value directly.
/// Nanosecond targets perform an exact `MAX` only over the metadata-selected
/// tail because sub-microsecond precision is not present in the version bound.
/// A legacy target with missing bounds falls back visibly to the full scan.
async fn read_watermark(
    context: &FactoryContext,
    config: &MaterializeSeriesConfig,
    source_time_type: &DataType,
) -> Result<Option<ScalarValue>, tinyfs::Error> {
    let root = context.root().await?;
    if !root.exists(&config.target).await {
        return Ok(None);
    }
    let metadata = root.metadata_for_path(&config.target).await?;
    if metadata.entry_type != tinyfs::EntryType::TablePhysicalSeries {
        return Err(tinyfs::Error::Other(format!(
            "materialize-series: target {} must be a physical table series, got {:?}",
            config.target, metadata.entry_type
        )));
    }

    let versions = root.list_file_versions(&config.target).await?;
    let plan = target_watermark_from_metadata(&versions, &config.time_column, source_time_type)?;
    match plan {
        TargetWatermark::Empty => return Ok(None),
        TargetWatermark::Exact(watermark) => {
            debug!(
                "materialize-series: target {} watermark {:?} from {} live version bound(s)",
                config.target,
                watermark,
                versions.len()
            );
            return Ok(Some(watermark));
        }
        TargetWatermark::BoundedNanosecond { .. } => {}
        TargetWatermark::FullScan { ref reason } => {
            warn!(
                "materialize-series: target {} requires a full watermark scan: {}",
                config.target, reason
            );
        }
    }

    let url = format!("series://{}", config.target);
    let ctx = &context.context.datafusion_session;
    let table = match &plan {
        TargetWatermark::BoundedNanosecond { lower_us, .. } => {
            table_for_bounded(
                context,
                &url,
                ctx,
                tinyfs::SeriesReadBounds::from_event_time_lo(*lower_us),
            )
            .await?
        }
        TargetWatermark::FullScan { .. } => table_for(context, &url, ctx).await?,
        TargetWatermark::Empty | TargetWatermark::Exact(_) => {
            unreachable!("empty and exact watermark plans return before provider construction")
        }
    };
    let mut frame = ctx
        .read_table(table)
        .map_err(|error| tinyfs::Error::Other(format!("materialize-series: target: {error}")))?;
    if let TargetWatermark::BoundedNanosecond { lower, .. } = plan {
        frame = frame
            .filter(col(Column::from_name(&config.time_column)).gt_eq(lit(lower)))
            .map_err(|error| {
                tinyfs::Error::Other(format!(
                    "materialize-series: bound target watermark scan: {error}"
                ))
            })?;
    }
    let watermark = max_event_time(
        frame,
        &config.time_column,
        "materialize-series: target watermark",
    )
    .await?;
    watermark.map(Some).ok_or_else(|| {
        tinyfs::Error::Other(format!(
            "materialize-series: nonempty target {} produced no rows while resolving its \
             watermark across {} live version(s)",
            config.target,
            versions.len()
        ))
    })
}

/// Append the source rows beyond the target's watermark as one new version.
pub async fn execute(
    config: Value,
    context: FactoryContext,
    ctx: ExecutionContext,
) -> Result<(), tinyfs::Error> {
    let config: MaterializeSeriesConfig =
        serde_json::from_value(config).map_other_context("Invalid config")?;
    config.validate()?;

    if let Some(MaterializeSubcommand::Pull) = parse_command(ctx)?.command {
        info!("materialize-series: 'pull' is a no-op (rows only flow source -> target)");
        return Ok(());
    }

    let session = &context.context.datafusion_session;
    let source = table_for(&context, &config.source.to_string(), session).await?;
    let mut frame = session
        .read_table(source)
        .map_err(|e| tinyfs::Error::Other(format!("materialize-series: read source: {e}")))?;
    let source_time_type = frame
        .schema()
        .field_with_unqualified_name(&config.time_column)
        .map_err(|error| {
            tinyfs::Error::Other(format!(
                "materialize-series: source event-time column {}: {error}",
                config.time_column
            ))
        })?
        .data_type()
        .clone();
    let watermark = read_watermark(&context, &config, &source_time_type).await?;
    if watermark.is_none() {
        context.context.record_non_incremental_plan();
        info!(
            "query-plan visibility: node={} locality=timestamp-local incremental=false reason=materialization-bootstrap",
            context.file_id
        );
    }

    // Strictly greater-than: the watermark row is already stored, and the
    // target is append-only, so re-emitting it would duplicate rather than
    // update.
    if let Some(ref bound) = watermark {
        frame = frame
            .filter(col(Column::from_name(&config.time_column)).gt(lit(bound.clone())))
            .map_err(|e| tinyfs::Error::Other(format!("materialize-series: filter: {e}")))?;
    }
    let frame = frame
        .sort_by(vec![col(Column::from_name(&config.time_column))])
        .map_err(|e| tinyfs::Error::Other(format!("materialize-series: sort: {e}")))?;
    let root = context.root().await?;
    let recipe_bytes = serde_json::to_vec(&config)
        .map_other_context("materialize-series: serialize recipe identity")?;
    let recipe_id = blake3::hash(&recipe_bytes).to_hex().to_string();
    let after = watermark.as_ref().map(scalar_event_time_us).transpose()?;
    let source_generation = context
        .context
        .persistence
        .coherence_state()
        .map(|state| state.generation());
    let source_url = config.source.to_string();
    let stream = frame
        .execute_stream()
        .await
        .map_err(|error| tinyfs::Error::Other(format!("materialize-series: execute: {error}")))?;
    let sink = crate::query_foundation_adapter::TinyFsMaterializationSink::new(
        root,
        &context.context,
        config.time_column.clone(),
    );
    let outcome = materialize_stream_with_progress(
        &sink,
        config.target.clone(),
        &config.time_column,
        move |output| {
            let source_max_us = output
                .map(|output| output.event_time_bounds().max())
                .or(after);
            let source_state_id = format!(
                "source={source_url};generation={source_generation:?};observed={source_max_us:?}"
            );
            MaterializationProgress::try_new(
                recipe_id,
                source_state_id,
                SettledState::try_new(source_max_us, source_max_us, RepairPolicy::reject())?,
            )
        },
        MaterializationPublication::Append { after },
        stream,
    )
    .await
    .map_err(|error| tinyfs::Error::Other(format!("materialize-series: publish: {error}")))?;

    match outcome.output {
        Some(output) => info!(
            "materialize-series: appended {} row(s) in {} batch(es) to {} covering [{}, {}], peak batch {} row(s)",
            outcome.metrics.rows_written,
            outcome.metrics.batches_written,
            config.target,
            output.event_time_bounds().min(),
            output.event_time_bounds().max(),
            outcome.metrics.peak_batch_rows,
        ),
        None => debug!(
            "materialize-series: {} is up to date (watermark {:?}); committed progress without output",
            config.target, watermark
        ),
    }
    Ok(())
}

fn scalar_event_time_us(value: &ScalarValue) -> TinyFSResult<i64> {
    let overflow = || {
        tinyfs::Error::Other(format!(
            "materialize-series: event-time value {value:?} overflows microseconds"
        ))
    };
    match value {
        ScalarValue::Int64(Some(value)) | ScalarValue::TimestampMicrosecond(Some(value), _) => {
            Ok(*value)
        }
        ScalarValue::TimestampSecond(Some(value), _) => {
            value.checked_mul(1_000_000).ok_or_else(overflow)
        }
        ScalarValue::TimestampMillisecond(Some(value), _) => {
            value.checked_mul(1_000).ok_or_else(overflow)
        }
        ScalarValue::TimestampNanosecond(Some(value), _) => Ok(value.div_euclid(1_000)),
        _ => Err(tinyfs::Error::Other(format!(
            "materialize-series: unsupported event-time value {value:?}"
        ))),
    }
}

register_executable_factory!(
    name: "materialize-series",
    description: "Incrementally materialize a derived signal into a physical table series",
    validate: validate_config,
    initialize: initialize,
    execute: execute
);

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, TimestampMicrosecondArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tinyfs::arrow::parquet::ParquetExt;
    use tinyfs::{EntryType, FileID, FileVersionInfo};

    fn target_version(version: u64, max_event_time: &str, time_column: &str) -> FileVersionInfo {
        FileVersionInfo {
            version,
            timestamp: i64::try_from(version).unwrap(),
            size: 1,
            blake3: None,
            entry_type: EntryType::TablePhysicalSeries,
            extended_metadata: Some(HashMap::from([
                ("max_event_time".to_owned(), max_event_time.to_owned()),
                (
                    "extended_attributes".to_owned(),
                    serde_json::json!({
                        "watertown.timestamp_column": time_column,
                    })
                    .to_string(),
                ),
            ])),
        }
    }

    #[test]
    fn target_watermark_uses_every_live_version_bound() {
        let versions = vec![
            target_version(8, "300", "timestamp"),
            target_version(20, "200", "timestamp"),
        ];
        let watermark = target_watermark_from_metadata(
            &versions,
            "timestamp",
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap();
        assert!(matches!(
            watermark,
            TargetWatermark::Exact(ScalarValue::TimestampMicrosecond(Some(300), None))
        ));
    }

    #[test]
    fn target_watermark_bounds_nanoseconds_without_losing_exactness() {
        let versions = vec![target_version(1, "123", "timestamp")];
        let watermark = target_watermark_from_metadata(
            &versions,
            "timestamp",
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap();
        assert!(matches!(
            watermark,
            TargetWatermark::BoundedNanosecond {
                lower: ScalarValue::TimestampNanosecond(Some(123_000), None),
                lower_us: 123,
            }
        ));
    }

    #[test]
    fn target_watermark_bounds_negative_nanoseconds_conservatively() {
        let versions = vec![target_version(1, "-1", "timestamp")];
        let watermark = target_watermark_from_metadata(
            &versions,
            "timestamp",
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap();
        assert!(matches!(
            watermark,
            TargetWatermark::BoundedNanosecond {
                lower: ScalarValue::TimestampNanosecond(Some(-1_999), None),
                lower_us: -1,
            }
        ));
    }

    #[test]
    fn target_watermark_falls_back_for_legacy_metadata() {
        let mut version = target_version(1, "123", "timestamp");
        let _previous = version
            .extended_metadata
            .as_mut()
            .unwrap()
            .insert("extended_attributes".to_owned(), "{}".to_owned());
        let watermark = target_watermark_from_metadata(
            &[version],
            "timestamp",
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap();
        assert!(matches!(
            watermark,
            TargetWatermark::FullScan { reason }
                if reason == "version 1 has no recorded timestamp column"
        ));
    }

    #[test]
    fn target_watermark_falls_back_for_unaligned_coarse_types() {
        let versions = vec![target_version(1, "123", "timestamp")];
        let watermark = target_watermark_from_metadata(
            &versions,
            "timestamp",
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )
        .unwrap();
        assert!(matches!(
            watermark,
            TargetWatermark::FullScan { reason }
                if reason == "microsecond watermark 123 is not aligned to whole milliseconds"
        ));
    }

    #[test]
    fn target_watermark_rejects_a_different_timestamp_column() {
        let versions = vec![target_version(1, "123", "recorded")];
        let error = target_watermark_from_metadata(
            &versions,
            "configured",
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("records timestamp column \"recorded\", expected \"configured\"")
        );
    }

    #[tokio::test]
    async fn materialize_series_streams_and_skips_empty_versions() {
        let (fs, provider_context) = crate::factory::test_support::create_test_environment().await;
        let root = fs.root().await.unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])),
            ],
        )
        .unwrap();
        _ = root
            .write_series_from_batch("/source.series", &batch, Some("timestamp"))
            .await
            .unwrap();

        let config = serde_json::to_value(MaterializeSeriesConfig {
            source: crate::Url::parse("series:///source.series").unwrap(),
            target: "/target.series".to_owned(),
            time_column: "timestamp".to_owned(),
        })
        .unwrap();
        let context = crate::factory::test_support::test_context(&provider_context, FileID::root());
        execute(
            config.clone(),
            context.clone(),
            ExecutionContext::pond_readwriter(vec!["push".to_owned()]),
        )
        .await
        .unwrap();
        let output = root.read_table_as_batch("/target.series").await.unwrap();
        assert_eq!(output.num_rows(), 3);
        assert_eq!(
            root.list_file_versions("/target.series")
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            provider_context
                .plan_visibility_metrics()
                .non_incremental_plans,
            1,
            "first-run full-history materialization must be visible"
        );

        execute(
            config,
            context,
            ExecutionContext::pond_readwriter(vec!["push".to_owned()]),
        )
        .await
        .unwrap();
        assert_eq!(
            root.list_file_versions("/target.series")
                .await
                .unwrap()
                .len(),
            1,
            "an empty suffix must not create an empty target version"
        );
        assert!(
            root.exists(std::path::Path::new(
                "/target.series.materialization-progress"
            ))
            .await
        );
        let progress: Value = serde_json::from_slice(
            &root
                .read_file_path_to_vec("/target.series.materialization-progress")
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            progress["watertown.materialization.observed_through"], 3,
            "an empty suffix must retain the prior observed frontier"
        );
        assert_eq!(
            progress["watertown.materialization.settled_through"], 3,
            "an empty suffix must retain the prior settled frontier"
        );
        assert!(
            progress["watertown.materialization.source_state_id"]
                .as_str()
                .unwrap()
                .contains("observed=Some(3)"),
            "an empty suffix must not publish a rewound source-state identity"
        );
        assert_eq!(
            provider_context
                .plan_visibility_metrics()
                .non_incremental_plans,
            1,
            "incremental no-op must not add another non-incremental plan"
        );
    }

    #[tokio::test]
    async fn materialize_series_preserves_an_exact_nanosecond_frontier() {
        let (fs, provider_context) = crate::factory::test_support::create_test_environment().await;
        let root = fs.root().await.unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
        ]));
        let first = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![1_001, 1_999])),
                Arc::new(Float64Array::from(vec![10.0, 20.0])),
            ],
        )
        .unwrap();
        _ = root
            .write_series_from_batch("/source-ns.series", &first, Some("timestamp"))
            .await
            .unwrap();

        let config = serde_json::to_value(MaterializeSeriesConfig {
            source: crate::Url::parse("series:///source-ns.series").unwrap(),
            target: "/target-ns.series".to_owned(),
            time_column: "timestamp".to_owned(),
        })
        .unwrap();
        execute(
            config.clone(),
            crate::factory::test_support::test_context(&provider_context, FileID::root()),
            ExecutionContext::pond_readwriter(vec!["push".to_owned()]),
        )
        .await
        .unwrap();
        let target_versions = root.list_file_versions("/target-ns.series").await.unwrap();
        assert!(matches!(
            target_watermark_from_metadata(
                &target_versions,
                "timestamp",
                &DataType::Timestamp(TimeUnit::Nanosecond, None),
            )
            .unwrap(),
            TargetWatermark::BoundedNanosecond {
                lower: ScalarValue::TimestampNanosecond(Some(1_000), None),
                lower_us: 1,
            }
        ));

        let second = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![2_001])),
                Arc::new(Float64Array::from(vec![30.0])),
            ],
        )
        .unwrap();
        _ = root
            .write_series_from_batch("/source-ns.series", &second, Some("timestamp"))
            .await
            .unwrap();
        execute(
            config,
            crate::factory::test_support::test_context(&provider_context, FileID::root()),
            ExecutionContext::pond_readwriter(vec!["push".to_owned()]),
        )
        .await
        .unwrap();

        let verification_context =
            crate::factory::test_support::test_context(&provider_context, FileID::root());
        let session = &provider_context.datafusion_session;
        let target = table_for(&verification_context, "series:///target-ns.series", session)
            .await
            .unwrap();
        let batches = session.read_table(target).unwrap().collect().await.unwrap();
        let timestamps = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("timestamp")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        assert_eq!(timestamps, vec![1_001, 1_999, 2_001]);
        assert_eq!(
            root.list_file_versions("/target-ns.series")
                .await
                .unwrap()
                .len(),
            2
        );
    }
}
