// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::SessionContext;
use provider::query_foundation_adapter::capture_tinyfs_snapshot;
use query_foundation::overlap::OverlapPolicy;
use query_foundation::snapshot::EventTimeContract;
use tinyfs::arrow::ParquetExt;
use tinyfs::{FS, MemoryPersistence, ProviderContext};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

fn batch(timestamps: Vec<i64>, values: Vec<f64>) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Float64Array::from(values)),
        ],
    )?)
}

async fn row_count(context: &SessionContext, table: &str) -> Result<i64> {
    let batches = context
        .sql(&format!("SELECT COUNT(*) AS rows FROM {table}"))
        .await?
        .collect()
        .await?;
    Ok(batches[0]
        .column_by_name("rows")
        .expect("count output must exist")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("COUNT must return Int64")
        .value(0))
}

#[tokio::test]
async fn captures_exact_versions_and_rejects_stale_generation_execution() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let filesystem = FS::new(persistence.clone())
        .await
        .expect("memory filesystem");
    let session = Arc::new(SessionContext::new());
    _ = provider::register_tinyfs_object_store(&session, persistence.clone())
        .expect("register TinyFS object store");
    let context = ProviderContext::new(session.clone(), Arc::new(persistence));
    let root = filesystem.root().await.expect("memory root");
    _ = root
        .create_series_from_batch(
            "/events.series",
            &batch(vec![1, 2], vec![10.0, 20.0])?,
            Some("ts"),
        )
        .await
        .expect("first series version");
    let file_id = root
        .get_node_path("/events.series")
        .await
        .expect("series node")
        .id();

    let first = capture_tinyfs_snapshot(
        &context,
        file_id,
        "snapshot-0001",
        schema(),
        Some(EventTimeContract::new("ts")),
        OverlapPolicy::PreserveAll,
    )
    .await?;
    assert_eq!(first.snapshot().chunks().len(), 1);
    assert_eq!(first.snapshot().chunks()[0].logical_count(), 2);
    assert_eq!(
        first.snapshot().chunks()[0]
            .event_time_bounds()
            .expect("footer statistics"),
        query_foundation::statistics::TimeInterval::try_new(1, 2)?
    );
    assert!(
        first.snapshot().chunks()[0]
            .object()
            .url()
            .to_string()
            .ends_with("/version/1.parquet")
    );
    _ = session.register_table("captured_before", first.table_provider()?)?;
    assert_eq!(row_count(&session, "captured_before").await?, 2);

    _ = root
        .write_series_from_batch("/events.series", &batch(vec![3], vec![30.0])?, Some("ts"))
        .await
        .expect("second series version");
    let stale_error = row_count(&session, "captured_before")
        .await
        .expect_err("provider from earlier persistence generation must fail");
    assert!(
        stale_error.to_string().contains("provider is stale"),
        "{stale_error}"
    );

    let second = capture_tinyfs_snapshot(
        &context,
        file_id,
        "snapshot-0002",
        schema(),
        Some(EventTimeContract::new("ts")),
        OverlapPolicy::PreserveAll,
    )
    .await?;
    assert_eq!(second.snapshot().chunks().len(), 2);
    assert_eq!(
        second
            .snapshot()
            .chunks()
            .iter()
            .map(|chunk| chunk.logical_count())
            .sum::<u64>(),
        3
    );
    _ = session.register_table("captured_after", second.table_provider()?)?;
    assert_eq!(row_count(&session, "captured_after").await?, 3);

    let frame = session
        .sql("SELECT ts FROM captured_after WHERE ts >= 3")
        .await?;
    let physical = frame.create_physical_plan().await?;
    let display = DisplayableExecutionPlan::new(physical.as_ref())
        .indent(true)
        .to_string();
    assert_eq!(display.matches("DataSourceExec").count(), 1, "{display}");
    assert!(display.contains("projection=[ts]"), "{display}");
    assert!(display.contains("ts@0 >= 3"), "{display}");
    assert!(!display.contains("value"), "{display}");

    Ok(())
}
