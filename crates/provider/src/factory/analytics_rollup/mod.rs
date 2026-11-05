// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

pub mod partitions;
mod table;
pub use table::AnalyticsTableConfig;

use crate::site_analytics::{Inventory, MAX_INVENTORY_BYTES, MAX_RECORD_BYTES, parse_record};
use crate::{ExecutionContext, FactoryContext, Provider};
use arrow::{datatypes::DataType, record_batch::RecordBatch};
use clap::{Parser, Subcommand};
use datafusion::{
    functions::datetime::expr_fn::date_trunc,
    functions_aggregate::expr_fn::count,
    logical_expr::{ExprSchemable, col, lit},
    physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
    scalar::ScalarValue,
};
use futures::StreamExt;
use partitions::{DAY_US, Manifest, PartitionSink, source_id};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::SeekFrom,
    sync::Arc,
};
use tinyfs::{Result as TinyResult, ResultExt};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "documentation", derive(schemars::JsonSchema))]
pub struct AnalyticsRollupConfig {
    /// Dedicated inventory-aware logfile-ingest destination.
    pub source: String,
    /// Dedicated directory for immutable objects and the selection manifest.
    pub target: String,
    /// Stable independent site identity.
    pub site_id: String,
    /// Explicit ordinary repair horizon in UTC days, including today.
    pub repair_days: u32,
    /// Exact canonical URIs excluded from counts; changing this requires rebuild.
    #[serde(default)]
    pub exclude: BTreeSet<String>,
}

fn fail(message: &str) -> tinyfs::Error {
    tinyfs::Error::Other(format!("analytics-rollup: {message}"))
}

pub(super) fn validate_path(path: &str) -> TinyResult<()> {
    if path == "/"
        || !path.starts_with('/')
        || path.ends_with('/')
        || path
            .split('/')
            .skip(1)
            .any(|p| p.is_empty() || matches!(p, "." | ".."))
        || !path
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/_-.".contains(&b))
    {
        return Err(fail(
            "use a canonical absolute pond path with ASCII letters, digits, '/', '_', '-' or '.'",
        ));
    }
    Ok(())
}

impl AnalyticsRollupConfig {
    pub fn validate(&self) -> TinyResult<()> {
        validate_path(&self.source)?;
        validate_path(&self.target)?;
        crate::site_analytics::validate_site(&self.site_id).map_other()?;
        if self.source == self.target
            || self.source.starts_with(&format!("{}/", self.target))
            || self.target.starts_with(&format!("{}/", self.source))
            || self.repair_days == 0
        {
            return Err(fail(
                "source/target must be separate and repair_days must be positive",
            ));
        }
        if self.exclude.iter().any(|uri| {
            !uri.starts_with('/')
                || uri.len() > 1024
                || uri.contains("//")
                || uri.contains(['\\', '%', '?', '#'])
                || uri.chars().any(|c| c.is_control() || c.is_whitespace())
                || uri.split('/').any(|p| matches!(p, "." | ".."))
        }) {
            return Err(fail(
                "exclusions must be bounded canonical absolute page URIs",
            ));
        }
        Ok(())
    }

    fn recipe(&self) -> TinyResult<String> {
        let bytes =
            serde_json::to_vec(&(2, &self.source, &self.site_id, &self.exclude)).map_other()?;
        Ok(blake3::hash(&bytes).to_hex().to_string())
    }
}

#[derive(Parser)]
struct Command {
    #[command(subcommand)]
    command: Option<Action>,
}

#[derive(Subcommand)]
enum Action {
    /// Replace dirty days within the explicit repair horizon (default).
    Push,
    /// Display a full historical rebuild plan and its approval token; no writes.
    Plan,
    /// Apply exactly the historical rebuild described by a prior plan.
    Rebuild {
        #[arg(long)]
        plan: String,
    },
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct RollupReport {
    pub source_inventory_bytes: usize,
    pub prior_partitions: usize,
    pub source_segments: usize,
    pub records_examined_for_changes: u64,
    pub days_replaced: Vec<i64>,
    pub days_utc: Vec<String>,
    pub published: bool,
    pub rebuild_token: Option<String>,
}

async fn load_source(
    config: &AnalyticsRollupConfig,
    context: &FactoryContext,
) -> TinyResult<(Inventory, usize)> {
    let root = context.context.filesystem().root().await?;
    let reader = root
        .async_reader_path(format!("{}/_inventory.json", config.source))
        .await?;
    let mut bytes = Vec::new();
    let _ = reader
        .take(MAX_INVENTORY_BYTES + 1)
        .read_to_end(&mut bytes)
        .await
        .map_other()?;
    Ok((
        Inventory::parse(&bytes, &config.site_id).map_other()?,
        bytes.len(),
    ))
}

async fn dirty_days(
    config: &AnalyticsRollupConfig,
    context: &FactoryContext,
    source: &Inventory,
    prior: Option<&Inventory>,
    report: &mut RollupReport,
) -> TinyResult<BTreeSet<i64>> {
    let root = context.context.filesystem().root().await?;
    let old: BTreeMap<_, _> = prior
        .into_iter()
        .flat_map(Inventory::segments)
        .map(|s| (s.segment_id, s))
        .collect();
    let expected: BTreeSet<_> = source
        .segments()
        .filter(|s| s.bytes > 0)
        .map(|s| format!("{}/{}", config.source, s.filename()))
        .collect();
    let actual: BTreeSet<_> = root
        .collect_matches(&format!("{}/views-*.jsonl", config.source))
        .await?
        .into_iter()
        .map(|(node, _)| node.path().to_string_lossy().to_string())
        .collect();
    if actual != expected {
        return Err(fail(
            "raw source membership differs from ingestion inventory",
        ));
    }
    let mut days = BTreeSet::new();
    for segment in source.segments().filter(|s| s.bytes > 0) {
        report.source_segments += 1;
        let path = format!("{}/{}", config.source, segment.filename());
        let node = root.get_node_path(&path).await?;
        let metadata = context.context.persistence.metadata(node.id()).await?;
        let state = crate::series_append::PondSeriesState::from_metadata(&metadata, &path)?;
        if metadata.entry_type != tinyfs::EntryType::FilePhysicalSeries
            || state.cumulative_size != segment.bytes
        {
            return Err(fail(
                "raw source length/type differs from ingestion inventory",
            ));
        }
        let start = old.get(&segment.segment_id).map_or(0, |s| s.bytes);
        if start == segment.bytes {
            continue;
        }
        let mut reader = root.async_reader_path(&path).await?;
        let _ = reader.seek(SeekFrom::Start(start)).await.map_other()?;
        let mut reader = BufReader::new(reader.take(segment.bytes - start));
        let mut seq = old.get(&segment.segment_id).map_or(0, |s| s.record_count);
        let mut bytes = 0u64;
        loop {
            let mut line = Vec::new();
            let _ = (&mut reader)
                .take(MAX_RECORD_BYTES + 1)
                .read_until(b'\n', &mut line)
                .await
                .map_other()?;
            if line.is_empty() {
                break;
            }
            let record = parse_record(&line).map_other()?;
            seq = seq
                .checked_add(1)
                .ok_or_else(|| fail("record sequence overflow"))?;
            if record.record_seq != seq
                || record.segment_id != segment.segment_id
                || record.site_id != config.site_id
            {
                return Err(fail(
                    "raw source identity or sequence conflicts with checkpoint",
                ));
            }
            let day = record
                .received_at_us
                .div_euclid(DAY_US)
                .checked_mul(DAY_US)
                .ok_or_else(|| fail("UTC day overflow"))?;
            let _ = days.insert(day);
            bytes += line.len() as u64;
            report.records_examined_for_changes += 1;
        }
        if seq != segment.record_count || bytes != segment.bytes - start {
            return Err(fail("raw source record count differs from checkpoint"));
        }
    }
    Ok(days)
}

async fn aggregate_day(
    config: &AnalyticsRollupConfig,
    context: &FactoryContext,
    day: i64,
) -> TinyResult<SendableRecordBatchStream> {
    let end = day
        .checked_add(DAY_US)
        .ok_or_else(|| fail("UTC day overflow"))?;
    let provider = Provider::with_context(
        Arc::new(context.context.filesystem()),
        Arc::new(context.context.clone()),
    );
    let table = provider
        .create_provider_for_url_bounded(
            &format!("site-analytics://{}/views-*.jsonl", config.source),
            &context.context.datafusion_session,
            tinyfs::SeriesReadBounds::from_event_time_lo(day),
        )
        .await
        .map_other()?;
    let mut frame = context
        .context
        .datafusion_session
        .read_table(table)
        .map_other()?;
    let timestamp = |value| {
        lit(ScalarValue::TimestampMicrosecond(
            Some(value),
            Some("UTC".into()),
        ))
    };
    frame = frame
        .filter(
            col("timestamp")
                .gt_eq(timestamp(day))
                .and(col("timestamp").lt(timestamp(end))),
        )
        .map_other()?;
    if !config.exclude.is_empty() {
        frame = frame
            .filter(col("page_uri").in_list(
                config.exclude.iter().map(|uri| lit(uri.clone())).collect(),
                true,
            ))
            .map_other()?;
    }
    let frame = frame
        .aggregate(
            vec![
                date_trunc(lit("hour"), col("timestamp")).alias("timestamp"),
                col("page_uri"),
            ],
            vec![count(lit(1i32)).alias("page_views")],
        )
        .map_other()?;
    let views = col("page_views")
        .cast_to(&DataType::UInt64, frame.schema())
        .map_other()?
        .alias("page_views");
    let frame = frame
        .select(vec![col("timestamp"), col("page_uri"), views])
        .map_other()?
        .sort(vec![
            col("timestamp").sort(true, false),
            col("page_uri").sort(true, false),
        ])
        .map_other()?;
    let stream = frame.execute_stream().await.map_other()?.map(|batch| {
        let batch = batch?;
        Ok(RecordBatch::try_new(
            partitions::schema(),
            batch.columns().to_vec(),
        )?)
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        partitions::schema(),
        stream,
    )))
}

/// Run inside one writable pond transaction; the caller must abort on error.
pub async fn run(
    config: &AnalyticsRollupConfig,
    context: &FactoryContext,
    rebuild: bool,
    approval: Option<&str>,
    now_us: i64,
) -> TinyResult<RollupReport> {
    config.validate()?;
    let sink = PartitionSink::new(context.context.clone(), config.target.clone());
    let prior = sink.load().await?;
    let (source, source_inventory_bytes) = load_source(config, context).await?;
    if let Some(prior) = &prior {
        if prior.source_path != config.source || prior.source.site_id != config.site_id {
            return Err(fail("target belongs to a different source/site"));
        }
        source.extends(&prior.source).map_other()?;
    }
    let recipe_id = config.recipe()?;
    let source_state_id = source_id(&source)?;
    let mut report = RollupReport {
        source_inventory_bytes,
        prior_partitions: prior.as_ref().map_or(0, |p| p.days.len()),
        ..Default::default()
    };
    if !rebuild {
        if prior.as_ref().is_some_and(|p| p.recipe_id != recipe_id) {
            return Err(fail("recipe changed; run plan then rebuild --plan TOKEN"));
        }
        if prior
            .as_ref()
            .is_some_and(|p| p.source_state_id == source_state_id)
        {
            return Ok(report);
        }
    }
    let mut days = dirty_days(
        config,
        context,
        &source,
        if rebuild {
            None
        } else {
            prior.as_ref().map(|p| &p.source)
        },
        &mut report,
    )
    .await?;
    if rebuild {
        if let Some(prior) = &prior {
            days.extend(prior.days.keys());
        }
    } else {
        let first = now_us
            .div_euclid(DAY_US)
            .checked_sub(i64::from(config.repair_days) - 1)
            .and_then(|d| d.checked_mul(DAY_US))
            .ok_or_else(|| fail("repair horizon overflow"))?;
        if days.iter().any(|day| *day < first) {
            return Err(fail(
                "dirty day exceeds repair_days; run plan then rebuild --plan TOKEN",
            ));
        }
    }
    report.days_replaced = days.iter().copied().collect();
    report.days_utc = days
        .iter()
        .map(|day| {
            chrono::DateTime::from_timestamp_micros(*day)
                .map(|date| date.format("%Y-%m-%d").to_string())
                .ok_or_else(|| fail("UTC day is outside the supported calendar"))
        })
        .collect::<TinyResult<_>>()?;
    if rebuild {
        let token = blake3::hash(
            &serde_json::to_vec(&(
                &config.target,
                &recipe_id,
                &source_state_id,
                &prior,
                &report.days_replaced,
            ))
            .map_other()?,
        )
        .to_hex()
        .to_string();
        report.rebuild_token = Some(token.clone());
        match approval {
            None => return Ok(report),
            Some(approved) if approved == token => {}
            Some(_) => return Err(fail("rebuild plan is stale or mismatched; plan again")),
        }
    }
    let mut selected = prior
        .as_ref()
        .map_or_else(BTreeMap::new, |p| p.days.clone());
    for day in days {
        let stream = aggregate_day(config, context, day).await?;
        match sink.stage_day(day, stream).await? {
            Some(partition) => {
                let _ = selected.insert(day, partition);
            }
            None => {
                let _ = selected.remove(&day);
            }
        }
    }
    sink.publish(&Manifest {
        schema_version: 1,
        source_path: config.source.clone(),
        recipe_id,
        source_state_id,
        source,
        published_at_us: now_us,
        collection_coverage: "unknown".into(),
        days: selected,
    })
    .await?;
    report.published = true;
    Ok(report)
}

fn validate_config(bytes: &[u8]) -> TinyResult<serde_json::Value> {
    let config: AnalyticsRollupConfig = serde_yaml::from_slice(bytes).map_other()?;
    config.validate()?;
    serde_json::to_value(config).map_other()
}

async fn initialize(_config: serde_json::Value, _context: FactoryContext) -> TinyResult<()> {
    Ok(())
}

#[allow(clippy::print_stdout)]
pub async fn execute(
    config: serde_json::Value,
    context: FactoryContext,
    ctx: ExecutionContext,
) -> TinyResult<()> {
    let config: AnalyticsRollupConfig = serde_json::from_value(config).map_other()?;
    let args = std::iter::once("analytics-rollup".to_owned()).chain(ctx.args().iter().cloned());
    let command = Command::try_parse_from(args).map_other()?;
    let (rebuild, approval) = match &command.command {
        Some(Action::Plan) => (true, None),
        Some(Action::Rebuild { plan }) => (true, Some(plan.as_str())),
        _ => (false, None),
    };
    let report = run(
        &config,
        &context,
        rebuild,
        approval,
        chrono::Utc::now().timestamp_micros(),
    )
    .await?;
    println!("{}", serde_json::to_string(&report).map_other()?);
    Ok(())
}

crate::register_executable_factory!(
    name: "analytics-rollup",
    description: "Transactionally replace daily snapshots of hourly reported page views",
    validate: validate_config,
    initialize: initialize,
    execute: execute
);
