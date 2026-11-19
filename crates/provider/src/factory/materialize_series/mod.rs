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
use query_foundation::locality::ChangeExtent;
use query_foundation::materialize::{
    MaterializationProgress, MaterializationPublication, materialize_stream_with_progress,
    plan_materialization_change, verify_append_prefix,
};
use query_foundation::statistics::TimeInterval;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;
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
#[cfg_attr(feature = "documentation", derive(schemars::JsonSchema))]
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
                lower_us: lower_ns.div_euclid(1_000),
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

async fn target_versions(
    context: &FactoryContext,
    config: &MaterializeSeriesConfig,
) -> TinyFSResult<Vec<tinyfs::FileVersionInfo>> {
    let root = context.root().await?;
    if !root.exists(&config.target).await {
        return Ok(Vec::new());
    }
    let metadata = root.metadata_for_path(&config.target).await?;
    if metadata.entry_type != tinyfs::EntryType::TablePhysicalSeries {
        return Err(tinyfs::Error::Other(format!(
            "materialize-series: target {} must be a physical table series, got {:?}",
            config.target, metadata.entry_type
        )));
    }

    root.list_file_versions(&config.target).await
}

/// `source_axis` is the source's resolved event-time column, computed once
/// per `execute` because resolving it parses every source version's metadata.
fn source_read_bounds(
    config: &MaterializeSeriesConfig,
    versions: &[tinyfs::FileVersionInfo],
    source_axis: Option<&str>,
) -> TinyFSResult<tinyfs::SeriesReadBounds> {
    // The target's microsecond metadata is a conservative hint even before
    // the source's timestamp type is known. The exact filter is applied later.
    let hint = match target_watermark_from_metadata(
        versions,
        &config.time_column,
        &DataType::Int64,
    )? {
        TargetWatermark::Empty => return Ok(tinyfs::SeriesReadBounds::NONE),
        TargetWatermark::FullScan { reason } => {
            warn!(
                "materialize-series: source {} is unbounded because target metadata is incomplete: {reason}",
                config.source
            );
            return Ok(tinyfs::SeriesReadBounds::NONE);
        }
        TargetWatermark::Exact(ScalarValue::Int64(Some(max_us))) => max_us,
        _ => unreachable!("microsecond metadata hint is exact, empty or unavailable"),
    };
    if source_axis != Some(config.time_column.as_str()) {
        warn!(
            "materialize-series: source {} is unbounded: declared event-time column {:?} does not establish requested column {:?}",
            config.source, source_axis, config.time_column
        );
        return Ok(tinyfs::SeriesReadBounds::NONE);
    }
    // Legacy nanosecond metadata truncated toward zero. Retain the preceding
    // microsecond bin until the source type and exact target maximum are known.
    let lower = if hint <= 0 {
        hint.saturating_sub(1)
    } else {
        hint
    };
    Ok(tinyfs::SeriesReadBounds::from_event_time_lo(lower))
}

/// Resolve the exact target watermark from already-read version metadata.
/// Nanosecond targets need a bounded tail scan; legacy metadata needs a visible
/// full scan. Microsecond and coarser types normally require no target payload.
async fn read_watermark(
    context: &FactoryContext,
    config: &MaterializeSeriesConfig,
    versions: &[tinyfs::FileVersionInfo],
    source_time_type: &DataType,
) -> Result<Option<ScalarValue>, tinyfs::Error> {
    let plan = target_watermark_from_metadata(versions, &config.time_column, source_time_type)?;
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

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceCheckpoint {
    format: u32,
    recipe: String,
    target: String,
    lineage: Option<tinyfs::QueryLineage>,
}

enum SourceChange {
    Unchanged,
    Append,
    /// `removed` describes prior source leaves that are gone or changed. They
    /// are named if verification rejects, because lost source rows are a
    /// retroactive change that cannot be appended.
    Verify {
        lower_us: Option<i64>,
        removed: Vec<String>,
    },
}

impl SourceChange {
    fn full() -> Self {
        Self::Verify {
            lower_us: None,
            removed: Vec::new(),
        }
    }
}

/// Exact microsecond extent of a leaf, or `None` when it cannot bound a
/// verification (unknown bounds or a nested recipe identity).
fn leaf_range(leaf: &tinyfs::QuerySourceLeaf) -> TinyFSResult<Option<(i64, i64)>> {
    if leaf.identity.starts_with("recipe:") {
        return Ok(None);
    }
    let (Some(lo), Some(hi)) = (leaf.min_event_time, leaf.max_event_time) else {
        return Ok(None);
    };
    if lo > hi {
        return Err(tinyfs::Error::Other(format!(
            "materialize-series: invalid source range [{lo}, {hi}] for {}",
            leaf.identity
        )));
    }
    Ok(Some((lo, hi)))
}

fn widen(extent: &mut Option<(i64, i64)>, (lo, hi): (i64, i64)) {
    *extent = Some(match *extent {
        None => (lo, hi),
        Some((min, max)) => (min.min(lo), max.max(hi)),
    });
}

fn describe_leaf(leaf: &tinyfs::QuerySourceLeaf) -> String {
    match (leaf.min_event_time, leaf.max_event_time) {
        (Some(lo), Some(hi)) => format!("{} [{lo}, {hi}]", leaf.identity),
        _ => format!("{} [unbounded]", leaf.identity),
    }
}

fn source_change(
    prior: Option<&SourceCheckpoint>,
    recipe: &str,
    target: &str,
    lineage: Option<&tinyfs::QueryLineage>,
    watermark_us: Option<i64>,
    matching_axis: bool,
) -> TinyFSResult<SourceChange> {
    let Some(prior) = prior.filter(|prior| prior.recipe == recipe && prior.target == target) else {
        return Ok(SourceChange::full());
    };
    let (Some(before), Some(now)) = (prior.lineage.as_ref(), lineage) else {
        return Ok(SourceChange::full());
    };
    if before == now {
        return Ok(SourceChange::Unchanged);
    }
    if watermark_us.is_none()
        || !matching_axis
        || !now.timestamp_local
        || before.recipe_identity != now.recipe_identity
    {
        return Ok(SourceChange::full());
    }
    let previous: BTreeMap<_, _> = before
        .leaves
        .iter()
        .map(|leaf| (&leaf.identity, leaf))
        .collect();
    let current: BTreeMap<_, _> = now
        .leaves
        .iter()
        .map(|leaf| (&leaf.identity, leaf))
        .collect();
    // Removal, replacement, repacking, or a rename out of the source pattern is
    // never assumed to be an append. Timestamp locality confines its effect to
    // the leaf's own event-time range, so a bounded leaf only needs an exact
    // check from that range onward; an unbounded one needs all history.
    let mut removed = Vec::new();
    let mut removed_extent = None;
    let mut removed_unbounded = false;
    for (identity, leaf) in &previous {
        if current.get(identity) == Some(leaf) {
            continue;
        }
        removed.push(describe_leaf(leaf));
        match leaf_range(leaf)? {
            Some(range) => widen(&mut removed_extent, range),
            None => removed_unbounded = true,
        }
    }
    let mut extent = None;
    for (identity, leaf) in &current {
        if previous.get(identity) == Some(leaf) {
            continue;
        }
        match leaf_range(leaf)? {
            Some(range) => widen(&mut extent, range),
            None => {
                return Ok(SourceChange::Verify {
                    lower_us: None,
                    removed,
                });
            }
        }
    }
    if removed_unbounded {
        return Ok(SourceChange::Verify {
            lower_us: None,
            removed,
        });
    }
    if let Some((removed_lo, _)) = removed_extent {
        let lo = extent.map_or(removed_lo, |(added_lo, _)| added_lo.min(removed_lo));
        return Ok(SourceChange::Verify {
            lower_us: Some(lo.saturating_sub(1)),
            removed,
        });
    }
    let Some((lo, hi)) = extent else {
        return Ok(SourceChange::full());
    };
    let prior = SettledState::try_new(watermark_us, watermark_us, RepairPolicy::reject())
        .map_other_context("materialize-series: prior frontier")?;
    let changed = ChangeExtent::Bounded(
        TimeInterval::try_new(lo, hi).map_other_context("materialize-series: changed range")?,
    );
    match plan_materialization_change(prior, changed) {
        Ok(_) => Ok(SourceChange::Append),
        // Bounds are conservative microseconds. A same-bin nanosecond append
        // or a filtered-out source row can still leave the exact prefix intact.
        Err(_) => Ok(SourceChange::Verify {
            lower_us: Some(lo.saturating_sub(1)),
            removed: Vec::new(),
        }),
    }
}

async fn target_identity(
    context: &FactoryContext,
    config: &MaterializeSeriesConfig,
    versions: &[tinyfs::FileVersionInfo],
) -> TinyFSResult<String> {
    if versions.is_empty() {
        return Ok("empty".to_owned());
    }
    let root = context.root().await?;
    let id = root.get_node_path(&config.target).await?.id();
    let mut members = versions
        .iter()
        .map(|version| {
            (
                version.version,
                version.timestamp,
                version.size,
                version.blake3.as_deref(),
            )
        })
        .collect::<Vec<_>>();
    members.sort();
    let bytes = serde_json::to_vec(&(id.to_string(), members))
        .map_other_context("materialize-series: target identity")?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

async fn checkpoint_path(
    context: &FactoryContext,
    config: &MaterializeSeriesConfig,
) -> TinyFSResult<Option<PathBuf>> {
    let Some(cache) = context.context.cache_dir() else {
        return Ok(None);
    };
    let key = format!(
        "{}:{}",
        context.root().await?.node_path().id(),
        config.target
    );
    Ok(Some(cache.join(format!(
        "materialize_{}.json",
        blake3::hash(key.as_bytes()).to_hex()
    ))))
}

async fn read_checkpoint(path: Option<&std::path::Path>) -> TinyFSResult<Option<SourceCheckpoint>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(tinyfs::Error::Other(format!(
                "materialize-series: read checkpoint {}: {error}",
                path.display()
            )));
        }
    };
    let checkpoint: SourceCheckpoint = serde_json::from_slice(&bytes).map_other_context(
        format!("materialize-series: invalid checkpoint {}", path.display()),
    )?;
    if checkpoint.format != 1 {
        return Err(tinyfs::Error::Other(format!(
            "materialize-series: unsupported checkpoint format {} at {}",
            checkpoint.format,
            path.display()
        )));
    }
    Ok(Some(checkpoint))
}

/// Express a microsecond lower bound in the watermark's own unit and timezone.
/// Casting a timezone-less value would read it as local wall-clock time in a
/// non-UTC zone, shifting the bound or making it NULL at a DST ambiguity.
/// Coarser units round down so the verified window only widens.
fn verification_lower_bound(watermark: &ScalarValue, lower_us: i64) -> TinyFSResult<ScalarValue> {
    Ok(match watermark {
        ScalarValue::Int64(_) => ScalarValue::Int64(Some(lower_us)),
        ScalarValue::TimestampSecond(_, tz) => {
            ScalarValue::TimestampSecond(Some(lower_us.div_euclid(1_000_000)), tz.clone())
        }
        ScalarValue::TimestampMillisecond(_, tz) => {
            ScalarValue::TimestampMillisecond(Some(lower_us.div_euclid(1_000)), tz.clone())
        }
        ScalarValue::TimestampMicrosecond(_, tz) => {
            ScalarValue::TimestampMicrosecond(Some(lower_us), tz.clone())
        }
        ScalarValue::TimestampNanosecond(_, tz) => ScalarValue::TimestampNanosecond(
            Some(lower_us.checked_mul(1_000).ok_or_else(|| {
                tinyfs::Error::Other(format!(
                    "materialize-series: verification lower bound {lower_us}us overflows nanoseconds"
                ))
            })?),
            tz.clone(),
        ),
        other => {
            return Err(tinyfs::Error::Other(format!(
                "materialize-series: unsupported verification watermark type {}",
                other.data_type()
            )));
        }
    })
}

fn prefix_frame(
    frame: datafusion::dataframe::DataFrame,
    time_column: &str,
    watermark: &ScalarValue,
    lower_us: Option<i64>,
) -> TinyFSResult<datafusion::dataframe::DataFrame> {
    let time = col(Column::from_name(time_column));
    let mut predicate = time.clone().lt_eq(lit(watermark.clone()));
    if let Some(lower) = lower_us {
        predicate = predicate.and(time.gt_eq(lit(verification_lower_bound(watermark, lower)?)));
    }
    frame
        .filter(predicate)
        .map_other_context("materialize-series: verify prefix")
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
    let versions = target_versions(&context, &config).await?;
    let recipe_bytes = serde_json::to_vec(&config)
        .map_other_context("materialize-series: serialize recipe identity")?;
    let recipe_id = blake3::hash(&recipe_bytes).to_hex().to_string();
    let provider = crate::Provider::with_context(
        Arc::new(context.context.filesystem()),
        Arc::new(context.context.clone()),
    )
    .with_root(context.root().await?);
    let mut lineage = provider
        .query_lineage_for_url(&config.source.to_string())
        .await
        .map_other_context("materialize-series: source lineage")?;
    if let Some(lineage) = &mut lineage {
        lineage.leaves.sort_by(|left, right| {
            (&left.identity, left.min_event_time, left.max_event_time).cmp(&(
                &right.identity,
                right.min_event_time,
                right.max_event_time,
            ))
        });
    }
    let checkpoint_path = checkpoint_path(&context, &config).await?;
    let prior = read_checkpoint(checkpoint_path.as_deref()).await?;
    let target = target_identity(&context, &config, &versions).await?;
    let hint =
        match target_watermark_from_metadata(&versions, &config.time_column, &DataType::Int64)? {
            TargetWatermark::Exact(ScalarValue::Int64(value)) => value,
            _ => None,
        };
    let source_axis = provider
        .event_time_column_for_url(&config.source.to_string())
        .await
        .map_other_context("materialize-series: source event-time contract")?;
    let matching_axis = source_axis.as_deref() == Some(config.time_column.as_str());
    let change = source_change(
        prior.as_ref(),
        &recipe_id,
        &target,
        lineage.as_ref(),
        hint,
        matching_axis,
    )?;
    if matches!(change, SourceChange::Unchanged) {
        debug!(
            "materialize-series: {} and its exact source snapshot are unchanged",
            config.target
        );
        return Ok(());
    }
    let mut bounds = source_read_bounds(&config, &versions, source_axis.as_deref())?;
    if let SourceChange::Verify { lower_us, .. } = change {
        bounds.event_time_lo = match (bounds.event_time_lo, lower_us) {
            (Some(current), Some(lower)) => Some(current.min(lower)),
            _ => None,
        };
    }
    let source = table_for_bounded(&context, &config.source.to_string(), session, bounds).await?;
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
    let watermark = read_watermark(&context, &config, &versions, &source_time_type).await?;
    if let (Some(watermark), SourceChange::Verify { lower_us, removed }) = (&watermark, &change) {
        context.context.record_materialization_verification();
        if lower_us.is_none() {
            context.context.record_non_incremental_plan();
        }
        let removed_note = if removed.is_empty() {
            String::new()
        } else {
            const SHOWN: usize = 5;
            let more = removed.len().saturating_sub(SHOWN);
            format!(
                "; {} source version(s) were removed or replaced since the last checkpoint \
                 (for example by log rotation renaming an ingested file out of the source \
                 pattern, or repacking): {}{}",
                removed.len(),
                removed[..removed.len().min(SHOWN)].join(", "),
                if more > 0 {
                    format!(", and {more} more")
                } else {
                    String::new()
                }
            )
        };
        warn!(
            "materialize-series: verifying {} against source history before append; lower_us={lower_us:?}; \
             missing/unproven checkpoints never authorize skipping historical changes{removed_note}",
            config.target
        );
        let target = table_for_bounded(
            &context,
            &format!("series://{}", config.target),
            session,
            lower_us.map_or(
                tinyfs::SeriesReadBounds::NONE,
                tinyfs::SeriesReadBounds::from_event_time_lo,
            ),
        )
        .await?;
        let target = session
            .read_table(target)
            .map_other_context("materialize-series: verification target")?;
        verify_append_prefix(
            prefix_frame(frame.clone(), &config.time_column, watermark, *lower_us)?,
            prefix_frame(target, &config.time_column, watermark, *lower_us)?,
        )
        .await
        .map_other_context(format!(
            "materialize-series: target {target} was not advanced{removed_note}. Recover by \
             restoring the source, or by retargeting: set \
             `target` to a new path in this factory's config, re-apply it with `pond mknod \
             materialize-series <factory-node> --config-path <config> --overwrite`, and repoint \
             consumers of {target}; the next run rebuilds the new target from source (see the \
             materialize-series documentation, \"Recovering from a rejection\")",
            target = config.target
        ))?;
    }
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
    let after = watermark.as_ref().map(scalar_event_time_us).transpose()?;
    // The frame already enforces the exact predicate. A second microsecond-only
    // publication boundary would reject valid nanosecond rows in the same bin.
    let publication_after = if matches!(
        source_time_type,
        DataType::Timestamp(TimeUnit::Nanosecond, _)
    ) {
        None
    } else {
        after
    };
    let source_state_id = match &lineage {
        Some(lineage) => blake3::hash(
            &serde_json::to_vec(lineage)
                .map_other_context("materialize-series: source snapshot identity")?,
        )
        .to_hex()
        .to_string(),
        None => format!("untracked:{}", config.source),
    };
    let publication_recipe = recipe_id.clone();
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
            MaterializationProgress::try_new(
                publication_recipe,
                source_state_id,
                SettledState::try_new(source_max_us, source_max_us, RepairPolicy::reject())?,
            )
        },
        MaterializationPublication::Append {
            after: publication_after,
        },
        stream,
    )
    .await
    .map_err(|error| tinyfs::Error::Other(format!("materialize-series: publish: {error}")))?;

    // This cache is not authoritative progress. It names exact target content,
    // so a rollback, concurrent publication, or cache loss forces verification.
    if let Some(path) = checkpoint_path {
        let target_versions = target_versions(&context, &config).await?;
        let checkpoint = SourceCheckpoint {
            format: 1,
            recipe: recipe_id,
            target: target_identity(&context, &config, &target_versions).await?,
            lineage,
        };
        crate::version_cache::write_cache_json(&path, &checkpoint)
            .await
            .map_other_context("materialize-series: publish source checkpoint")?;
    }
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
                lower_us: -2,
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
    async fn materialization_verifies_legacy_targets_and_rejects_same_time_appends() {
        for target_time_column in [Some("timestamp"), None] {
            let (fs, context) = crate::factory::test_support::create_test_environment().await;
            let cache = tempfile::tempdir().unwrap();
            let context = context.with_cache_dir(cache.path().to_path_buf());
            let root = fs.root().await.unwrap();
            let batch = |values| {
                RecordBatch::try_from_iter([(
                    "timestamp",
                    Arc::new(TimestampMicrosecondArray::from(values)) as arrow::array::ArrayRef,
                )])
                .unwrap()
            };
            _ = root
                .write_series_from_batch("/source", &batch(vec![1, 2]), Some("timestamp"))
                .await
                .unwrap();
            _ = root
                .write_series_from_batch("/target", &batch(vec![1, 2]), target_time_column)
                .await
                .unwrap();
            let config = MaterializeSeriesConfig {
                source: crate::Url::parse("series:///source").unwrap(),
                target: "/target".to_owned(),
                time_column: "timestamp".to_owned(),
            };
            let factory = crate::factory::test_support::test_context(&context, FileID::root());
            let run = || {
                execute(
                    serde_json::to_value(&config).unwrap(),
                    factory.clone(),
                    ExecutionContext::pond_readwriter(Vec::new()),
                )
            };
            run().await.unwrap();
            let path = checkpoint_path(&factory, &config).await.unwrap().unwrap();
            std::fs::remove_file(&path).unwrap();
            run()
                .await
                .expect("a consistent existing target can establish its checkpoint");
            let checkpoint = std::fs::read(&path).unwrap();
            _ = root
                .write_series_from_batch("/source", &batch(vec![2]), Some("timestamp"))
                .await
                .unwrap();
            let error = run()
                .await
                .expect_err("an extra event at the watermark must not disappear")
                .to_string();
            for expected in [
                "rejects retroactive changes",
                "target /target was not advanced",
                "or by retargeting",
                "--overwrite",
            ] {
                assert!(error.contains(expected), "missing {expected:?} in: {error}");
            }
            assert_eq!(std::fs::read(&path).unwrap(), checkpoint);
            assert_eq!(root.list_file_versions("/target").await.unwrap().len(), 1);
            std::fs::remove_file(&path).unwrap();
            assert!(
                run().await.is_err(),
                "an inconsistent legacy target must not be adopted"
            );
            assert!(!path.exists());
        }
    }

    fn leaf(identity: &str, bounds: Option<(i64, i64)>) -> tinyfs::QuerySourceLeaf {
        tinyfs::QuerySourceLeaf {
            identity: identity.to_owned(),
            min_event_time: bounds.map(|(lo, _)| lo),
            max_event_time: bounds.map(|(_, hi)| hi),
        }
    }

    fn lineage(leaves: Vec<tinyfs::QuerySourceLeaf>) -> tinyfs::QueryLineage {
        let mut lineage = tinyfs::QueryLineage::new("pattern:test".to_owned());
        lineage.timestamp_local = true;
        lineage.leaves = leaves;
        lineage
    }

    #[test]
    fn verification_lower_bound_keeps_unit_and_timezone() -> TinyFSResult<()> {
        let tz: Option<Arc<str>> = Some("America/Los_Angeles".into());
        let lower_us = -1_500_001;
        for (watermark, expected) in [
            (
                ScalarValue::TimestampNanosecond(Some(0), tz.clone()),
                ScalarValue::TimestampNanosecond(Some(-1_500_001_000), tz.clone()),
            ),
            (
                ScalarValue::TimestampMicrosecond(Some(0), tz.clone()),
                ScalarValue::TimestampMicrosecond(Some(lower_us), tz.clone()),
            ),
            (
                ScalarValue::TimestampMillisecond(Some(0), tz.clone()),
                ScalarValue::TimestampMillisecond(Some(-1_501), tz.clone()),
            ),
            (
                ScalarValue::TimestampSecond(Some(0), tz.clone()),
                ScalarValue::TimestampSecond(Some(-2), tz.clone()),
            ),
            (
                ScalarValue::Int64(Some(0)),
                ScalarValue::Int64(Some(lower_us)),
            ),
        ] {
            assert_eq!(verification_lower_bound(&watermark, lower_us)?, expected);
        }
        assert!(
            verification_lower_bound(
                &ScalarValue::TimestampNanosecond(Some(0), None),
                i64::MAX / 10
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn removed_source_leaves_bound_verification_by_their_range() {
        let prior = |leaves| SourceCheckpoint {
            format: 1,
            recipe: "r".to_owned(),
            target: "t".to_owned(),
            lineage: Some(lineage(leaves)),
        };
        let lower = |before: Vec<_>, now: Vec<_>| match source_change(
            Some(&prior(before)),
            "r",
            "t",
            Some(&lineage(now)),
            Some(100),
            true,
        )
        .unwrap()
        {
            SourceChange::Verify { lower_us, removed } => (lower_us, removed.len()),
            _ => panic!("a removed leaf must be verified"),
        };
        let old = leaf("a:1", Some((0, 9)));
        let b = leaf("b:1", Some((50, 60)));
        // Removal alone re-proves only the removed range.
        assert_eq!(
            lower(vec![old.clone(), b.clone()], vec![old.clone()]),
            (Some(49), 1)
        );
        // Repacking under a new identity starts from the earliest of both.
        assert_eq!(
            lower(
                vec![old.clone(), b.clone()],
                vec![old.clone(), leaf("c:1", Some((40, 60)))]
            ),
            (Some(39), 1)
        );
        // A removed leaf without bounds can have affected any time.
        assert_eq!(
            lower(vec![old.clone(), leaf("u:1", None)], vec![old.clone()]),
            (None, 1)
        );
    }

    #[tokio::test]
    async fn materialization_bounds_repack_verification_and_names_lost_sources() {
        let (fs, context) = crate::factory::test_support::create_test_environment().await;
        let cache = tempfile::tempdir().unwrap();
        let context = context.with_cache_dir(cache.path().to_path_buf());
        let root = fs.root().await.unwrap();
        let batch = |values| {
            RecordBatch::try_from_iter([(
                "timestamp",
                Arc::new(TimestampMicrosecondArray::from(values)) as arrow::array::ArrayRef,
            )])
            .unwrap()
        };
        let src = root.create_dir_path("/src").await.unwrap();
        let write = |path: &'static str, values: Vec<i64>| {
            let root = root.clone();
            let context = context.clone();
            let (lo, hi) = (values[0], values[values.len() - 1]);
            async move {
                _ = root
                    .write_series_from_batch(path, &batch(values), Some("timestamp"))
                    .await
                    .unwrap();
                let id = root.get_node_path(path).await.unwrap().id();
                context
                    .persistence
                    .set_extended_attributes(
                        id,
                        HashMap::from([
                            ("min_event_time".to_owned(), lo.to_string()),
                            ("max_event_time".to_owned(), hi.to_string()),
                            (
                                "extended_attributes".to_owned(),
                                r#"{"watertown.timestamp_column":"timestamp"}"#.to_owned(),
                            ),
                        ]),
                    )
                    .await
                    .unwrap();
            }
        };
        write("/src/a.series", vec![1, 2]).await;
        write("/src/b.series", vec![10, 11]).await;
        let config = MaterializeSeriesConfig {
            source: crate::Url::parse("series:///src/*.series").unwrap(),
            target: "/target".to_owned(),
            time_column: "timestamp".to_owned(),
        };
        let factory = crate::factory::test_support::test_context(&context, FileID::root());
        let run = || {
            execute(
                serde_json::to_value(&config).unwrap(),
                factory.clone(),
                ExecutionContext::pond_readwriter(Vec::new()),
            )
        };
        run().await.unwrap();
        run().await.unwrap();
        let metrics = context.plan_visibility_metrics();
        assert_eq!(metrics.materialization_verifications, 0);

        // Repacking b's rows under a new identity is checked from b's range and passes.
        src.remove_entry("b.series").await.unwrap();
        write("/src/c.series", vec![10, 11]).await;
        run().await.expect("repacked rows match the target");
        let after = context.plan_visibility_metrics();
        assert_eq!(after.materialization_verifications, 1);
        assert_eq!(
            after.non_incremental_plans, metrics.non_incremental_plans,
            "a bounded removed leaf must not force a full-history check"
        );
        assert_eq!(root.list_file_versions("/target").await.unwrap().len(), 1);

        // Losing rows the target already holds is rejected and names the source.
        src.remove_entry("c.series").await.unwrap();
        let error = run()
            .await
            .expect_err("rows lost from the source must not be ignored")
            .to_string();
        for expected in [
            "rejects retroactive changes",
            "1 source version(s) were removed or replaced",
            "[10, 11]",
            "or by retargeting",
        ] {
            assert!(error.contains(expected), "missing {expected:?} in: {error}");
        }
        assert_eq!(
            context.plan_visibility_metrics().non_incremental_plans,
            metrics.non_incremental_plans
        );
    }

    #[tokio::test]
    async fn materialize_series_streams_and_skips_empty_versions() {
        let (fs, provider_context) = crate::factory::test_support::create_test_environment().await;
        let cache = tempfile::tempdir().unwrap();
        let provider_context = provider_context.with_cache_dir(cache.path().to_path_buf());
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

        for _ in 0..2 {
            execute(
                config.clone(),
                context.clone(),
                ExecutionContext::pond_readwriter(vec!["push".to_owned()]),
            )
            .await
            .unwrap();
        }
        assert_eq!(
            root.list_file_versions("/target.series")
                .await
                .unwrap()
                .len(),
            1,
            "an empty suffix must not create an empty target version"
        );
        assert!(
            !root
                .exists(std::path::Path::new(
                    "/target.series.materialization-progress"
                ))
                .await,
            "empty suffixes must not publish independent progress"
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
                Arc::new(TimestampNanosecondArray::from(vec![1_001, 1_500])),
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
                Arc::new(TimestampNanosecondArray::from(vec![1_999, 2_001])),
                Arc::new(Float64Array::from(vec![30.0, 40.0])),
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
        assert_eq!(timestamps, vec![1_001, 1_500, 1_999, 2_001]);
        assert_eq!(
            root.list_file_versions("/target-ns.series")
                .await
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn materialize_does_not_prune_using_a_different_source_time_column() {
        let (fs, provider_context) = crate::factory::test_support::create_test_environment().await;
        let root = fs.root().await.unwrap();
        let batch = |value| {
            RecordBatch::try_from_iter([
                (
                    "timestamp",
                    Arc::new(TimestampMicrosecondArray::from(vec![value]))
                        as arrow::array::ArrayRef,
                ),
                (
                    "other_time",
                    Arc::new(TimestampMicrosecondArray::from(vec![value * 100]))
                        as arrow::array::ArrayRef,
                ),
            ])
            .unwrap()
        };
        let config = serde_json::json!({
            "source": "series:///source.series",
            "target": "/target.series",
            "time_column": "other_time",
        });
        let context = crate::factory::test_support::test_context(&provider_context, FileID::root());
        for values in [1..=2, 3..=4] {
            for value in values {
                _ = root
                    .write_series_from_batch("/source.series", &batch(value), Some("timestamp"))
                    .await
                    .unwrap();
                let id = root.get_node_path("/source.series").await.unwrap().id();
                provider_context
                    .persistence
                    .set_extended_attributes(
                        id,
                        HashMap::from([
                            ("min_event_time".to_owned(), value.to_string()),
                            ("max_event_time".to_owned(), value.to_string()),
                            (
                                "extended_attributes".to_owned(),
                                r#"{"watertown.timestamp_column":"timestamp"}"#.to_owned(),
                            ),
                        ]),
                    )
                    .await
                    .unwrap();
            }
            execute(
                config.clone(),
                context.clone(),
                ExecutionContext::pond_readwriter(Vec::new()),
            )
            .await
            .unwrap();
        }
        let context = crate::factory::test_support::test_context(&provider_context, FileID::root());
        let table = table_for(
            &context,
            "series:///target.series",
            &provider_context.datafusion_session,
        )
        .await
        .unwrap();
        let output = provider_context
            .datafusion_session
            .read_table(table)
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            output.iter().map(RecordBatch::num_rows).sum::<usize>(),
            4,
            "a different temporal index must not hide valid rows"
        );
    }
}
