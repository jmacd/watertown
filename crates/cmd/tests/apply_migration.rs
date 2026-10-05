// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;
use std::sync::{Arc, Once};

use anyhow::{Result, anyhow};
use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use cmd::commands::{apply_command, init_command};
use cmd::common::ShipContext;
use steward::PondUserMetadata;
use tempfile::TempDir;
use tinyfs::arrow::parquet::ParquetExt;

static INIT_LOG: Once = Once::new();

const SOURCE_PATH: &str = "/reduced/well-depth/data/res=1m.series";
const PUMP_PATH: &str = "/pump-state/well-pump-state";
const RATE_PATH: &str = "/usage/well-usage-rate";
const DAILY_PATH: &str = "/usage/well-usage-daily";

const LEGACY_CONFIG: &str = r#"
version: v1
kind: mknod
metadata:
  path: /pump-state
spec:
  factory: dynamic-dir
  config:
    entries:
      - name: well-pump-state
        factory: sql-derived-series
        config:
          patterns:
            depth: "series:///reduced/well-depth/data/res=1m.series"
          query: >-
            WITH r AS (
              SELECT timestamp AS ts,
                CAST(EXTRACT(EPOCH FROM timestamp)/60 AS BIGINT) AS min_idx,
                "well_depth_value.avg" AS depth
              FROM depth
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
---
version: v1
kind: mknod
metadata:
  path: /usage
spec:
  factory: dynamic-dir
  config:
    entries:
      - name: well-usage-rate
        factory: sql-derived-series
        config:
          patterns:
            state: "series:///pump-state/well-pump-state"
          query: >-
            SELECT
              timestamp,
              depth,
              CASE WHEN phase = 'pumping'
                THEN 429.5 / (160.0 - 3.28084*depth)
                ELSE 0.0 END AS usage_gpm
            FROM state
            WHERE timestamp >= TIMESTAMP '2024-02-15'
            ORDER BY timestamp
      - name: well-usage-daily
        factory: sql-derived-series
        config:
          patterns:
            state: "series:///pump-state/well-pump-state"
          query: >-
            SELECT
              date_trunc('day', timestamp) AS timestamp,
              SUM(CASE WHEN phase = 'pumping'
                THEN 429.5 / (160.0 - 3.28084*depth)
                ELSE 0.0 END) AS gallons,
              CAST(SUM(CASE WHEN phase = 'pumping' THEN 1 ELSE 0 END)
                AS BIGINT) AS pump_minutes
            FROM state
            WHERE timestamp >= TIMESTAMP '2024-02-15'
            GROUP BY 1
            ORDER BY 1
"#;

const TYPED_CONFIG: &str = r#"
version: v1
kind: mknod
metadata:
  path: /pump-state
spec:
  factory: dynamic-dir
  config:
    entries:
      - name: well-pump-state
        factory: pump-state-series
        config:
          source: "series:///reduced/well-depth/data/res=1m.series"
          time_column: timestamp
          depth_column: well_depth_value.avg
          lookback: 60m
          disturbance_drop: 0.3
          valid_depth_min: 10.0
          valid_depth_max: 60.0
---
version: v1
kind: mknod
metadata:
  path: /usage
spec:
  factory: dynamic-dir
  config:
    entries:
      - name: well-usage-rate
        factory: sql-derived-series
        config:
          locality:
            type: timestamp-local
            event_time: timestamp
          patterns:
            state: "series:///pump-state/well-pump-state"
          query: >-
            SELECT
              timestamp,
              depth,
              CASE WHEN phase = 'pumping'
                THEN 429.5 / (160.0 - 3.28084*depth)
                ELSE 0.0 END AS usage_gpm,
              CASE WHEN phase = 'pumping' THEN 1 ELSE 0 END AS pump_minutes
            FROM state
            WHERE timestamp >= TIMESTAMP '2024-02-15'
      - name: well-usage-daily
        factory: temporal-reduce-series
        config:
          in_pattern: "series:///usage/well-usage-rate"
          time_column: timestamp
          resolution: 1d
          allowed_lateness: 14d
          seal_target_bytes: 0
          aggregations:
            - type: sum
              columns:
                - usage_gpm
                - pump_minutes
          output_aliases:
            "usage_gpm.sum": gallons
            "pump_minutes.sum": pump_minutes
"#;

#[derive(Debug, PartialEq)]
struct PumpRow {
    timestamp: i64,
    depth_bits: u64,
    phase: String,
}

#[derive(Debug, PartialEq)]
struct RateRow {
    timestamp: i64,
    depth_bits: u64,
    usage_gpm_bits: u64,
}

#[derive(Debug)]
struct DailyRow {
    timestamp: i64,
    gallons: f64,
    pump_minutes: i64,
}

fn init_log() {
    INIT_LOG.call_once(|| {
        let _ = env_logger::builder().is_test(true).try_init();
    });
}

fn context(path: &Path, args: &[&str]) -> ShipContext {
    ShipContext::pond_only(
        Some(path),
        args.iter().map(|arg| (*arg).to_owned()).collect(),
    )
}

fn source_batch() -> RecordBatch {
    let start = chrono::DateTime::parse_from_rfc3339("2024-02-15T00:00:00Z")
        .expect("fixture timestamp")
        .timestamp_micros();
    let mut samples = (0..=60)
        .map(|minute| (start + minute * 60_000_000, 45.0))
        .collect::<Vec<_>>();
    samples.extend([
        (start + 61 * 60_000_000, 44.5),
        (start + 62 * 60_000_000, 44.0),
        (start + 63 * 60_000_000, 43.5),
        (start + 64 * 60_000_000, 43.8),
        (start + 65 * 60_000_000, 44.2),
        (start + 66 * 60_000_000, 44.8),
        (start + 1_440 * 60_000_000, 45.0),
        (start + 2_880 * 60_000_000, 45.0),
    ]);
    RecordBatch::try_from_iter([
        (
            "timestamp",
            Arc::new(TimestampMicrosecondArray::from(
                samples
                    .iter()
                    .map(|(timestamp, _)| *timestamp)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "well_depth_value.avg",
            Arc::new(Float64Array::from(
                samples.iter().map(|(_, depth)| *depth).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
    ])
    .expect("source batch")
}

async fn apply_yaml(ctx: &ShipContext, scratch: &Path, name: &str, yaml: &str) -> Result<()> {
    let path = scratch.join(name);
    std::fs::write(&path, yaml)?;
    apply_command(ctx, &[path.to_string_lossy().into_owned()]).await
}

async fn write_source(ctx: &ShipContext) -> Result<()> {
    let mut steward = ctx.open_pond().await?;
    let batch = source_batch();
    steward
        .write_transaction(
            &PondUserMetadata::new(vec!["apply-migration-source".to_owned()]),
            async move |transaction| {
                let root = transaction.root().await?;
                _ = root.create_dir_all("/reduced/well-depth/data").await?;
                _ = root
                    .create_series_from_batch(SOURCE_PATH, &batch, Some("timestamp"))
                    .await?;
                Ok(())
            },
        )
        .await?;
    Ok(())
}

async fn query_series(ctx: &ShipContext, path: &str) -> Result<Vec<RecordBatch>> {
    let mut steward = ctx.open_pond().await?;
    let transaction = steward
        .begin_read(&PondUserMetadata::new(vec![
            "apply-migration-query".to_owned(),
        ]))
        .await?;
    let root = transaction.root().await?;
    let node = root.get_node_path(Path::new(path)).await?;
    let file = node.as_file().await?;
    let handle = file.handle.get_file().await;
    let guard = handle.lock().await;
    let provider_context = transaction.provider_context()?;
    let table = guard
        .as_queryable()
        .ok_or_else(|| anyhow!("{path} is not queryable"))?
        .as_table_provider(node.id(), &provider_context)
        .await?;
    drop(guard);
    let batches = provider_context
        .datafusion_session
        .read_table(table)?
        .collect()
        .await?;
    _ = transaction.commit().await?;
    Ok(batches)
}

fn pump_rows(batches: &[RecordBatch]) -> Vec<PumpRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let timestamps = batch
            .column_by_name("timestamp")
            .expect("pump timestamp")
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp micros");
        let depths = batch
            .column_by_name("depth")
            .expect("pump depth")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float depth");
        let phases = batch
            .column_by_name("phase")
            .expect("pump phase")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string phase");
        rows.extend((0..batch.num_rows()).map(|row| PumpRow {
            timestamp: timestamps.value(row),
            depth_bits: depths.value(row).to_bits(),
            phase: phases.value(row).to_owned(),
        }));
    }
    rows.sort_by_key(|row| row.timestamp);
    rows
}

fn daily_rows(batches: &[RecordBatch]) -> Vec<DailyRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let timestamps = batch
            .column_by_name("timestamp")
            .expect("daily timestamp")
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp micros");
        let gallons = batch
            .column_by_name("gallons")
            .expect("daily gallons")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float gallons");
        let pump_minutes = batch
            .column_by_name("pump_minutes")
            .expect("daily pump minutes")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("integer pump minutes");
        rows.extend((0..batch.num_rows()).map(|row| DailyRow {
            timestamp: timestamps.value(row),
            gallons: gallons.value(row),
            pump_minutes: pump_minutes.value(row),
        }));
    }
    rows.sort_by_key(|row| row.timestamp);
    rows
}

fn rate_rows(batches: &[RecordBatch]) -> Vec<RateRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let timestamps = batch
            .column_by_name("timestamp")
            .expect("rate timestamp")
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp micros");
        let depths = batch
            .column_by_name("depth")
            .expect("rate depth")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float depth");
        let usage_gpm = batch
            .column_by_name("usage_gpm")
            .expect("usage rate")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float usage rate");
        rows.extend((0..batch.num_rows()).map(|row| RateRow {
            timestamp: timestamps.value(row),
            depth_bits: depths.value(row).to_bits(),
            usage_gpm_bits: usage_gpm.value(row).to_bits(),
        }));
    }
    rows.sort_by_key(|row| row.timestamp);
    rows
}

fn schema_names(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .first()
        .expect("query must return a batch")
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_owned())
        .collect()
}

async fn dynamic_config(ctx: &ShipContext, path: &str) -> Result<String> {
    let mut steward = ctx.open_pond().await?;
    let transaction = steward
        .begin_read(&PondUserMetadata::new(vec![
            "apply-migration-config".to_owned(),
        ]))
        .await?;
    let root = transaction.root().await?;
    let node = root.get_node_path(Path::new(path)).await?;
    let (_, bytes) = transaction
        .get_dynamic_node_config(node.id())
        .await?
        .ok_or_else(|| anyhow!("{path} has no dynamic config"))?;
    _ = transaction.commit().await?;
    Ok(String::from_utf8(bytes)?)
}

#[tokio::test]
async fn apply_migrates_legacy_sql_graph_to_typed_factories_in_place() -> Result<()> {
    init_log();
    let scratch = TempDir::new()?;
    let pond_path = scratch.path().join("pond");
    let ctx = context(&pond_path, &["pond", "apply"]);

    init_command(&ctx, "apply-migration-source").await?;
    write_source(&ctx).await?;
    apply_yaml(&ctx, scratch.path(), "legacy.yaml", LEGACY_CONFIG).await?;

    let legacy_pump = pump_rows(&query_series(&ctx, PUMP_PATH).await?);
    let legacy_rate_batches = query_series(&ctx, RATE_PATH).await?;
    assert_eq!(
        schema_names(&legacy_rate_batches),
        vec!["timestamp", "depth", "usage_gpm"]
    );
    let legacy_rate = rate_rows(&legacy_rate_batches);
    let legacy_daily = daily_rows(&query_series(&ctx, DAILY_PATH).await?);
    assert_eq!(legacy_pump.len(), 69);
    assert_eq!(
        legacy_pump
            .iter()
            .filter(|row| row.phase == "pumping")
            .count(),
        3
    );
    assert_eq!(legacy_daily.len(), 3);

    apply_yaml(&ctx, scratch.path(), "typed.yaml", TYPED_CONFIG).await?;

    let typed_pump = pump_rows(&query_series(&ctx, PUMP_PATH).await?);
    let typed_rate_batches = query_series(&ctx, RATE_PATH).await?;
    assert_eq!(
        schema_names(&typed_rate_batches),
        vec!["timestamp", "depth", "usage_gpm", "pump_minutes"]
    );
    let typed_rate = rate_rows(&typed_rate_batches);
    let typed_daily = daily_rows(&query_series(&ctx, DAILY_PATH).await?);
    assert_eq!(typed_pump, legacy_pump);
    assert_eq!(typed_rate, legacy_rate);
    assert_eq!(typed_daily.len(), legacy_daily.len());
    for (typed, legacy) in typed_daily.iter().zip(&legacy_daily) {
        assert_eq!(typed.timestamp, legacy.timestamp);
        assert_eq!(typed.pump_minutes, legacy.pump_minutes);
        assert!(
            (typed.gallons - legacy.gallons).abs() <= 1e-12,
            "daily gallons changed at {}: typed={} legacy={}",
            typed.timestamp,
            typed.gallons,
            legacy.gallons
        );
    }

    let pump_config = dynamic_config(&ctx, "/pump-state").await?;
    let usage_config = dynamic_config(&ctx, "/usage").await?;
    assert!(pump_config.contains("factory: pump-state-series"));
    assert!(usage_config.contains("factory: temporal-reduce-series"));
    assert!(usage_config.contains("type: timestamp-local"));

    Ok(())
}
