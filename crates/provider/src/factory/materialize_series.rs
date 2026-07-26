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
//! the same shape `hydrovu` produces, and therefore the same shape the
//! collapse/reclaim path operates on.
//!
//! It is deliberately incremental rather than a snapshot-and-replace: a
//! rewrite-everything materializer would reintroduce exactly the `O(N^2)` write
//! amplification that size-tiered collapse exists to remove.

use crate::{ExecutionContext, FactoryContext, register_executable_factory};
use arrow::compute::concat_batches;
use clap::{Parser, Subcommand};
use datafusion::prelude::{SessionConfig, SessionContext, col, lit};
use datafusion::scalar::ScalarValue;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tinyfs::Result as TinyFSResult;
use tinyfs::ResultExt;
use tinyfs::arrow::parquet::ParquetExt;

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

/// A private session that can still resolve `tinyfs:///` URLs.
///
/// Reading a *physical* series goes through a `ListingTable` over `tinyfs://`
/// paths, which only resolves if the tinyfs object store is registered -- and
/// it is registered on the persistence layer's own session, not on a fresh
/// one.  Borrowing that session's `RuntimeEnv` inherits the object-store
/// registry while keeping a private catalog, so the table names used here
/// cannot collide with anything else running in the transaction.
fn scratch_session(context: &FactoryContext) -> SessionContext {
    SessionContext::new_with_config_rt(
        SessionConfig::new(),
        context.context.datafusion_session.runtime_env(),
    )
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
    let ctx = scratch_session(context);
    let table = table_for(context, &url, &ctx).await?;
    let _previous = ctx
        .register_table("target", table)
        .map_err(|e| tinyfs::Error::Other(format!("materialize-series: register target: {e}")))?;

    let batches = ctx
        .sql(&format!(
            "SELECT max(\"{}\") AS watermark FROM target",
            config.time_column
        ))
        .await
        .map_err(|e| tinyfs::Error::Other(format!("materialize-series: watermark sql: {e}")))?
        .collect()
        .await
        .map_err(|e| tinyfs::Error::Other(format!("materialize-series: watermark scan: {e}")))?;

    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let scalar = ScalarValue::try_from_array(batch.column(0), 0).map_err(|e| {
            tinyfs::Error::Other(format!("materialize-series: watermark decode: {e}"))
        })?;
        if !scalar.is_null() {
            return Ok(Some(scalar));
        }
    }
    Ok(None)
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

    let session = scratch_session(&context);
    let source = table_for(&context, &config.source.to_string(), &session).await?;
    let _previous = session
        .register_table("source", source)
        .map_err(|e| tinyfs::Error::Other(format!("materialize-series: register source: {e}")))?;

    let mut frame = session
        .table("source")
        .await
        .map_err(|e| tinyfs::Error::Other(format!("materialize-series: read source: {e}")))?;

    // Strictly greater-than: the watermark row is already stored, and the
    // target is append-only, so re-emitting it would duplicate rather than
    // update.
    if let Some(ref bound) = watermark {
        frame = frame
            .filter(col(&config.time_column).gt(lit(bound.clone())))
            .map_err(|e| tinyfs::Error::Other(format!("materialize-series: filter: {e}")))?;
    }
    let frame = frame
        .sort_by(vec![col(&config.time_column)])
        .map_err(|e| tinyfs::Error::Other(format!("materialize-series: sort: {e}")))?;

    let schema: arrow::datatypes::SchemaRef = Arc::new(frame.schema().as_arrow().clone());
    let batches = frame
        .collect()
        .await
        .map_err(|e| tinyfs::Error::Other(format!("materialize-series: collect: {e}")))?;

    let rows: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    if rows == 0 {
        // The common case on a tick with no new source data.  Writing an empty
        // version would burn a version number and a parquet file for nothing.
        debug!(
            "materialize-series: {} is up to date (watermark {:?})",
            config.target, watermark
        );
        return Ok(());
    }

    // One version per run, not one per batch: versions are the unit collapse
    // works on, so a run that emitted several would multiply the very count
    // that triggers collapse.
    let batch = concat_batches(&schema, &batches)
        .map_err(|e| tinyfs::Error::Other(format!("materialize-series: concat: {e}")))?;

    let root = context.root().await?;
    let (min_time, max_time) = root
        .write_series_from_batch(&config.target, &batch, Some(&config.time_column))
        .await?;

    info!(
        "materialize-series: appended {} row(s) to {} covering [{}, {}]",
        rows, config.target, min_time, max_time
    );
    Ok(())
}

register_executable_factory!(
    name: "materialize-series",
    description: "Incrementally materialize a derived signal into a physical table series",
    validate: validate_config,
    initialize: initialize,
    execute: execute
);
