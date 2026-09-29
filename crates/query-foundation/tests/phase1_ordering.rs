// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::logical_expr::{col, lit};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use query_foundation::snapshot::{ChunkDescriptor, DatasetSnapshot, EventTimeContract};
use query_foundation::statistics::TimeInterval;
use query_foundation::testkit::FoundationFixture;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

fn batch(first_timestamp: i64) -> Result<RecordBatch> {
    let timestamps = (first_timestamp..first_timestamp + 4).collect::<Vec<_>>();
    let values = timestamps
        .iter()
        .map(|timestamp| *timestamp as f64 / 10.0)
        .collect::<Vec<_>>();
    Ok(RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Float64Array::from(values)),
        ],
    )?)
}

async fn ordered_snapshot(
    second_chunk_ordered: bool,
) -> Result<(FoundationFixture, DatasetSnapshot)> {
    let fixture = FoundationFixture::new(schema());
    _ = fixture
        .put_parquet("chunks/chunk-0001.parquet", &[batch(0)?], 4)
        .await?;
    _ = fixture
        .put_parquet("chunks/chunk-0002.parquet", &[batch(100)?], 4)
        .await?;
    let ordering = vec![col("ts").sort(true, true)];
    let first = ChunkDescriptor::new(
        "chunk-0001",
        0,
        fixture.object_descriptor("chunks/chunk-0001.parquet")?,
        schema(),
        4,
        Some(TimeInterval::try_new(0, 3)?),
    )
    .with_ordering(ordering.clone());
    let mut second = ChunkDescriptor::new(
        "chunk-0002",
        1,
        fixture.object_descriptor("chunks/chunk-0002.parquet")?,
        schema(),
        4,
        Some(TimeInterval::try_new(100, 103)?),
    );
    if second_chunk_ordered {
        second = second.with_ordering(ordering);
    }
    let snapshot = DatasetSnapshot::try_new(
        "snapshot-0001",
        schema(),
        vec![first, second],
        Some(EventTimeContract::new("ts")),
    )?;
    Ok((fixture, snapshot))
}

#[tokio::test]
async fn preserves_ordering_when_pruning_selects_one_proven_chunk() -> Result<()> {
    let (fixture, snapshot) = ordered_snapshot(true).await?;
    let context = fixture.context()?;
    _ = context.register_table("series", snapshot.table_provider()?)?;

    let dataframe = context
        .table("series")
        .await?
        .filter(col("ts").lt(lit(4_i64)))?
        .sort(vec![col("ts").sort(true, true)])?;
    let plan = dataframe.create_physical_plan().await?;
    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        !display
            .lines()
            .any(|line| line.trim_start().starts_with("SortExec:")),
        "{display}"
    );

    let batches = datafusion::physical_plan::collect(plan, context.task_ctx()).await?;
    let timestamps = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("timestamp column")
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(timestamps, vec![0, 1, 2, 3]);

    Ok(())
}

#[tokio::test]
async fn discards_incomplete_chunk_ordering_evidence() -> Result<()> {
    let (fixture, snapshot) = ordered_snapshot(false).await?;
    let context = fixture.context()?;
    _ = context.register_table("series", snapshot.table_provider()?)?;

    let dataframe = context
        .table("series")
        .await?
        .sort(vec![col("ts").sort(true, true)])?;
    let plan = dataframe.create_physical_plan().await?;
    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        display
            .lines()
            .any(|line| line.trim_start().starts_with("SortExec:")),
        "{display}"
    );

    Ok(())
}
