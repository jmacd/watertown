// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use arrow::array::{Float64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use cmd as _;
use std::sync::Arc;
use steward::{PondUserMetadata, Ship};
use tempfile::tempdir;
use tinyfs::arrow::ParquetExt;
use tlogfs::FactoryRegistry;

#[tokio::test]
async fn rate_while_monitor_reads_committed_snapshot_after_commit() -> Result<()> {
    let temporary = tempdir()?;
    let pond_path = temporary.path().join("pond");
    let output_path = temporary.path().join("monitor");
    let mut ship = Ship::create_pond(&pond_path, "test-host").await?;
    ship.control_table_mut()
        .set_factory_mode("monitor-report", "push")
        .await?;

    let transaction = ship
        .begin_write(&PondUserMetadata::new(vec![
            "test".to_string(),
            "monitor-post-commit".to_string(),
        ]))
        .await?;
    let root = transaction.root().await?;
    let now = Utc::now();
    let timestamps = (0..5)
        .map(|minutes| (now - chrono::Duration::minutes(5 - minutes)).timestamp_micros())
        .collect::<Vec<_>>();

    let measurement_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("chlorine_level", DataType::Float64, false),
        ])),
        vec![
            Arc::new(TimestampMicrosecondArray::from(timestamps.clone())),
            Arc::new(Float64Array::from(vec![10.0, 11.0, 12.0, 5.0, 6.0])),
        ],
    )?;
    _ = root
        .create_series_from_batch("/chlorine", &measurement_batch, Some("timestamp"))
        .await?;

    let condition_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("phase", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(TimestampMicrosecondArray::from(timestamps)),
            Arc::new(StringArray::from(vec![
                "pumping", "pumping", "pumping", "pumping", "pumping",
            ])),
        ],
    )?;
    _ = root
        .create_series_from_batch("/pump-state", &condition_batch, Some("timestamp"))
        .await?;

    _ = root.create_dir_path("/system").await?;
    _ = root.create_dir_path("/system/run").await?;
    let config = serde_json::json!({
        "pond": "integration-test",
        "title": "Integration monitor",
        "output_dir": output_path,
        "checks": [{
            "id": "chlorine-feed-response",
            "label": "Chlorine feed responds while well pump runs",
            "description": "Chlorine level must increase while the well pump is running.",
            "href": "/data/chlorine-level.html",
            "type": "rate-while",
            "measurement": {
                "source": "series:///chlorine",
                "timestamp": "timestamp",
                "column": "chlorine_level",
                "accumulation": "positive-deltas"
            },
            "condition": {
                "source": "series:///pump-state",
                "timestamp": "timestamp",
                "predicate": {
                    "column": "phase",
                    "operator": "eq",
                    "value": "pumping"
                }
            },
            "alignment": {
                "method": "previous",
                "tolerance": "2m"
            },
            "window": "24h",
            "minimum_active_time": "3m",
            "alarm": {
                "operator": "lt",
                "value": 40.0,
                "unit": "sensor-units-per-pump-hour"
            }
        }]
    });
    let config_yaml = serde_yaml::to_string(&config)?;
    let factory_node = root
        .create_dynamic_path(
            "/system/run/00-monitor",
            tinyfs::EntryType::FileDynamic,
            "monitor-report",
            config_yaml.as_bytes().to_vec(),
        )
        .await?;
    FactoryRegistry::initialize::<tlogfs::TLogFSError>(
        "monitor-report",
        config_yaml.as_bytes(),
        provider::FactoryContext::new(transaction.provider_context()?, factory_node.id()),
    )
    .await?;

    let committed_sequence = transaction.txn_meta().txn_seq;
    _ = transaction.commit().await?;
    assert_eq!(
        ship.last_write_seq(),
        committed_sequence,
        "read-only monitoring must not allocate a post-commit write sequence"
    );

    let status: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output_path.join("status.json"))?)?;
    assert_eq!(status["schema_version"], 3);
    assert_eq!(status["transaction_sequence"], committed_sequence);
    assert_eq!(status["state"], "healthy");
    assert_eq!(status["checks"][0]["rule"], "rate-while");
    assert_eq!(status["checks"][0]["active_seconds"], 240.0);
    assert_eq!(status["checks"][0]["accumulated_change"], 3.0);
    assert_eq!(status["checks"][0]["rate"], 45.0);
    assert_eq!(status["checks"][0]["aligned_interval_count"], 4);
    assert_eq!(status["checks"][0]["unaligned_interval_count"], 0);
    assert_eq!(status["checks"][0]["href"], "/data/chlorine-level.html");
    assert!(!output_path.join("index.html").exists());
    Ok(())
}
