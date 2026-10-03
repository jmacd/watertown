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
use clap::{Parser, Subcommand};
use datafusion::common::Column;
use datafusion::functions_aggregate::expr_fn::max;
use datafusion::prelude::{SessionContext, col, lit};
use datafusion::scalar::ScalarValue;
use log::{debug, info};
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
    let fs = context.context.filesystem();
    let mut provider =
        crate::Provider::with_context(Arc::new(fs), Arc::new(context.context.clone()));
    if let Ok(root) = context.root().await {
        provider = provider.with_root(root);
    }
    provider
        .create_table_provider(url, ctx)
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

/// The largest event time already materialized into `target`, or `None` when
/// the target does not exist yet.
///
/// Deliberately `max()` over the WHOLE target rather than a peek at its newest
/// version: once size-tiered collapse has run, the highest version number is a
/// merged run standing for content in the MIDDLE of the stream, so "latest
/// version" is not "latest data".  Reading a stale watermark that way would
/// silently re-append rows that are already stored.  DataFusion answers this
/// from parquet statistics, so it does not decode row groups.
async fn read_watermark(
    context: &FactoryContext,
    config: &MaterializeSeriesConfig,
) -> Result<Option<ScalarValue>, tinyfs::Error> {
    let root = context.root().await?;
    if !root.exists(&config.target).await {
        return Ok(None);
    }

    let url = format!("series://{}", config.target);
    let ctx = &context.context.datafusion_session;
    let table = table_for(context, &url, ctx).await?;
    max_event_time(
        ctx.read_table(table).map_err(|error| {
            tinyfs::Error::Other(format!("materialize-series: target: {error}"))
        })?,
        &config.time_column,
        "materialize-series: target watermark",
    )
    .await
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

    let watermark = read_watermark(&context, &config).await?;
    if watermark.is_none() {
        context.context.record_non_incremental_plan();
        info!(
            "query-plan visibility: node={} locality=timestamp-local incremental=false reason=materialization-bootstrap",
            context.file_id
        );
    }

    let session = &context.context.datafusion_session;
    let source = table_for(&context, &config.source.to_string(), session).await?;
    let mut frame = session
        .read_table(source)
        .map_err(|e| tinyfs::Error::Other(format!("materialize-series: read source: {e}")))?;

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
    use arrow::array::{Float64Array, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use tinyfs::FileID;
    use tinyfs::arrow::parquet::ParquetExt;

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
}
