// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::logical_expr::{TableProviderFilterPushDown, col, lit};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use query_foundation::metrics::ChunkPruningMetrics;
use query_foundation::statistics::TimeInterval;
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
        .map(|timestamp| format!("unused-{timestamp}"))
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

#[tokio::test]
async fn prunes_only_proven_disjoint_chunks_and_retains_exact_filter() -> Result<()> {
    let fixture = FoundationFixture::new(schema());
    _ = fixture
        .put_parquet("chunks/chunk-0001.parquet", &[batch(0)?], 4)
        .await?;
    _ = fixture
        .put_parquet("chunks/chunk-0002.parquet", &[batch(100)?], 4)
        .await?;
    _ = fixture
        .put_parquet("chunks/chunk-0003.parquet", &[batch(200)?], 4)
        .await?;
    let snapshot = fixture.timeseries_snapshot(
        "snapshot-0001",
        "ts",
        &[
            (
                "chunks/chunk-0001.parquet",
                4,
                Some(TimeInterval::try_new(0, 3)?),
            ),
            (
                "chunks/chunk-0002.parquet",
                4,
                Some(TimeInterval::try_new(100, 103)?),
            ),
            ("chunks/chunk-0003.parquet", 4, None),
        ],
    )?;
    let pruning = Arc::new(ChunkPruningMetrics::default());
    let provider = snapshot.table_provider_with_metrics(Arc::clone(&pruning));
    let pushed = provider.supports_filters_pushdown(&[&col("ts").gt_eq(lit(100_i64))])?;
    assert_eq!(pushed, vec![TableProviderFilterPushDown::Inexact]);

    let context = fixture.context()?;
    _ = context.register_table("series", provider)?;
    fixture.metrics().reset();
    pruning.reset();
    let dataframe = context
        .table("series")
        .await?
        .filter(
            col("ts")
                .gt_eq(lit(100_i64))
                .and(col("ts").lt(lit(104_i64))),
        )?
        .select(vec![col("value")])?;
    let plan = dataframe.create_physical_plan().await?;
    let batches = datafusion::physical_plan::collect(Arc::clone(&plan), context.task_ctx()).await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);

    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(display.contains("FilterExec"), "{display}");
    assert!(display.contains("ts@0 >= 100"), "{display}");
    assert!(display.contains("ts@0 < 104"), "{display}");
    assert!(display.contains("projection=[ts, value]"), "{display}");
    assert!(!display.contains("unused"), "{display}");

    let pruning_work = pruning.snapshot();
    assert_eq!(pruning_work.scans, 1);
    assert_eq!(pruning_work.candidate_chunks, 3);
    assert_eq!(pruning_work.retained_chunks, 2);
    assert_eq!(pruning_work.pruned_chunks, 1);
    assert_eq!(pruning_work.missing_statistics, 1);
    assert_eq!(
        fixture.metrics().snapshot().opened_objects,
        BTreeSet::from([
            "chunks/chunk-0002.parquet".to_owned(),
            "chunks/chunk-0003.parquet".to_owned(),
        ])
    );

    Ok(())
}

#[tokio::test]
async fn missing_statistics_never_remove_matching_rows() -> Result<()> {
    let fixture = FoundationFixture::new(schema());
    _ = fixture
        .put_parquet("chunks/known.parquet", &[batch(0)?], 4)
        .await?;
    _ = fixture
        .put_parquet("chunks/unknown.parquet", &[batch(200)?], 4)
        .await?;
    let snapshot = fixture.timeseries_snapshot(
        "snapshot-0001",
        "ts",
        &[
            (
                "chunks/known.parquet",
                4,
                Some(TimeInterval::try_new(0, 3)?),
            ),
            ("chunks/unknown.parquet", 4, None),
        ],
    )?;
    let pruning = Arc::new(ChunkPruningMetrics::default());
    let context = fixture.context()?;
    _ = context.register_table(
        "series",
        snapshot.table_provider_with_metrics(Arc::clone(&pruning)),
    )?;
    fixture.metrics().reset();

    let batches = context
        .table("series")
        .await?
        .filter(
            col("ts")
                .gt_eq(lit(200_i64))
                .and(col("ts").lt(lit(204_i64))),
        )?
        .collect()
        .await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
    assert_eq!(
        fixture.metrics().snapshot().opened_objects,
        BTreeSet::from(["chunks/unknown.parquet".to_owned()])
    );
    assert_eq!(pruning.snapshot().missing_statistics, 1);

    Ok(())
}

#[tokio::test]
async fn fully_pruned_snapshot_reads_no_objects() -> Result<()> {
    let fixture = FoundationFixture::new(schema());
    _ = fixture
        .put_parquet("chunks/known.parquet", &[batch(0)?], 4)
        .await?;
    let snapshot = fixture.timeseries_snapshot(
        "snapshot-0001",
        "ts",
        &[(
            "chunks/known.parquet",
            4,
            Some(TimeInterval::try_new(0, 3)?),
        )],
    )?;
    let pruning = Arc::new(ChunkPruningMetrics::default());
    let context = fixture.context()?;
    _ = context.register_table(
        "series",
        snapshot.table_provider_with_metrics(Arc::clone(&pruning)),
    )?;
    fixture.metrics().reset();

    let batches = context
        .table("series")
        .await?
        .filter(col("ts").gt_eq(lit(100_i64)))?
        .collect()
        .await?;
    assert!(batches.is_empty());
    assert_eq!(fixture.metrics().snapshot().object_gets, 0);
    assert_eq!(pruning.snapshot().pruned_chunks, 1);

    Ok(())
}
