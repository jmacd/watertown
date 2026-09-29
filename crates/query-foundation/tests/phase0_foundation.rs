// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::logical_expr::{col, lit};
use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::SessionContext;
use query_foundation::snapshot::DatasetSnapshot;
use query_foundation::testkit::FoundationFixture;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("unused", DataType::Utf8, false),
    ]))
}

fn batch(first_timestamp: i64) -> Result<RecordBatch> {
    let timestamps = (first_timestamp..first_timestamp + 4).collect::<Vec<_>>();
    let values = timestamps
        .iter()
        .map(|timestamp| *timestamp as f64 / 10.0)
        .collect::<Vec<_>>();
    let unused = timestamps
        .iter()
        .map(|timestamp| format!("unprojected-{timestamp:04}-{}", "x".repeat(150_000)))
        .collect::<Vec<_>>();
    Ok(RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(unused)),
        ],
    )?)
}

fn register_snapshot(
    context: &SessionContext,
    name: &str,
    snapshot: &DatasetSnapshot,
) -> Result<()> {
    _ = context.register_table(name, snapshot.table_provider()?)?;
    Ok(())
}

async fn row_count(context: &SessionContext, table: &str) -> Result<usize> {
    Ok(context
        .table(table)
        .await?
        .collect()
        .await?
        .iter()
        .map(RecordBatch::num_rows)
        .sum())
}

#[tokio::test]
async fn proves_projection_predicate_pruning_and_bounded_range_reads() -> Result<()> {
    let fixture = FoundationFixture::new(schema());
    let object_size = fixture
        .put_parquet(
            "chunks/two-row-groups.parquet",
            &[batch(0)?, batch(100)?],
            4,
        )
        .await?;
    let snapshot = fixture.snapshot("snapshot-0001", &["chunks/two-row-groups.parquet"])?;

    let context = fixture.context()?;
    register_snapshot(&context, "series", &snapshot)?;
    fixture.metrics().reset();

    let dataframe = context
        .table("series")
        .await?
        .filter(col("ts").gt_eq(lit(100_i64)))?
        .select(vec![col("value")])?;
    let plan = dataframe.create_physical_plan().await?;
    let batches = collect(Arc::clone(&plan), context.task_ctx()).await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
    let values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("value projection must be Float64")
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![10.0, 10.1, 10.2, 10.3]);

    let plan_with_metrics = DisplayableExecutionPlan::with_metrics(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        plan_with_metrics.contains("DataSourceExec"),
        "{plan_with_metrics}"
    );
    assert!(
        plan_with_metrics.contains("projection=[ts, value]"),
        "{plan_with_metrics}"
    );
    assert!(!plan_with_metrics.contains("unused"), "{plan_with_metrics}");
    assert!(
        plan_with_metrics.contains("predicate=ts@0 >= 100"),
        "{plan_with_metrics}"
    );
    let row_group_metrics = plan_with_metrics
        .split_once("row_groups_pruned_statistics=")
        .and_then(|(_, metrics)| metrics.split_once(',').map(|(value, _)| value))
        .expect("Parquet row-group metrics must be present");
    assert!(row_group_metrics.contains("2 total"), "{plan_with_metrics}");
    assert!(
        row_group_metrics.contains("1 matched"),
        "{plan_with_metrics}"
    );

    let filtered_work = fixture.metrics().snapshot();
    assert_eq!(filtered_work.full_object_reads, 0);
    assert!((1..=8).contains(&filtered_work.range_requests));
    assert!(filtered_work.max_read_bytes < object_size);
    assert_eq!(
        filtered_work.opened_objects,
        BTreeSet::from(["chunks/two-row-groups.parquet".to_owned()])
    );

    fixture.metrics().reset();
    let full_context = fixture.context()?;
    register_snapshot(&full_context, "series", &snapshot)?;
    let full_dataframe = full_context
        .table("series")
        .await?
        .select(vec![col("ts"), col("value")])?;
    let full_plan = full_dataframe.create_physical_plan().await?;
    let full_batches = collect(full_plan, full_context.task_ctx()).await?;
    assert_eq!(
        full_batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        8
    );
    let full_work = fixture.metrics().snapshot();
    assert_eq!(full_work.full_object_reads, 0);
    assert!(filtered_work.bytes_returned < full_work.bytes_returned);

    Ok(())
}

#[tokio::test]
async fn snapshot_membership_is_immutable_after_new_chunk_publication() -> Result<()> {
    let fixture = FoundationFixture::new(schema());
    _ = fixture
        .put_parquet("chunks/chunk-0001.parquet", &[batch(0)?], 4)
        .await?;
    let first_snapshot = fixture.snapshot("snapshot-0001", &["chunks/chunk-0001.parquet"])?;

    _ = fixture
        .put_parquet("chunks/chunk-0002.parquet", &[batch(100)?], 4)
        .await?;

    let first_context = fixture.context()?;
    register_snapshot(&first_context, "series", &first_snapshot)?;
    fixture.metrics().reset();
    assert_eq!(row_count(&first_context, "series").await?, 4);
    assert_eq!(
        fixture.metrics().snapshot().opened_objects,
        BTreeSet::from(["chunks/chunk-0001.parquet".to_owned()])
    );

    let second_snapshot = fixture.snapshot(
        "snapshot-0002",
        &["chunks/chunk-0001.parquet", "chunks/chunk-0002.parquet"],
    )?;
    let second_context = fixture.context()?;
    register_snapshot(&second_context, "series", &second_snapshot)?;
    assert_eq!(row_count(&second_context, "series").await?, 8);
    assert_eq!(first_snapshot.snapshot_id(), "snapshot-0001");
    assert_eq!(first_snapshot.chunks().len(), 1);
    assert_eq!(second_snapshot.chunks().len(), 2);

    Ok(())
}
