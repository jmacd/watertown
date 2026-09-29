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
use query_foundation::metrics::ChunkPruningMetrics;
use query_foundation::overlap::OverlapPolicy;
use query_foundation::snapshot::{ChunkDescriptor, DatasetSnapshot, EventTimeContract};
use query_foundation::testkit::FoundationFixture;

fn base_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

fn base_batch(first_id: i64) -> Result<RecordBatch> {
    let ids = (first_id..first_id + 4).collect::<Vec<_>>();
    let values = ids.iter().map(|id| *id as f64 / 10.0).collect::<Vec<_>>();
    Ok(RecordBatch::try_new(
        base_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(values)),
        ],
    )?)
}

#[tokio::test]
async fn ordinary_table_filters_all_chunks_and_honors_limit() -> Result<()> {
    let fixture = FoundationFixture::new(base_schema());
    _ = fixture
        .put_parquet("tables/chunk-0001.parquet", &[base_batch(0)?], 4)
        .await?;
    _ = fixture
        .put_parquet("tables/chunk-0002.parquet", &[base_batch(100)?], 4)
        .await?;
    let snapshot = fixture.snapshot(
        "snapshot-0001",
        &[
            ("tables/chunk-0001.parquet", 4),
            ("tables/chunk-0002.parquet", 4),
        ],
    )?;
    let pruning = Arc::new(ChunkPruningMetrics::default());
    let context = fixture.context()?;
    _ = context.register_table(
        "ordinary",
        snapshot.table_provider_with_metrics(Arc::clone(&pruning)),
    )?;
    fixture.metrics().reset();

    let batches = context
        .table("ordinary")
        .await?
        .filter(col("value").gt_eq(lit(10.0_f64)))?
        .select(vec![col("id")])?
        .limit(0, Some(2))?
        .collect()
        .await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert_eq!(
        fixture.metrics().snapshot().opened_objects,
        BTreeSet::from([
            "tables/chunk-0001.parquet".to_owned(),
            "tables/chunk-0002.parquet".to_owned(),
        ])
    );
    assert_eq!(pruning.snapshot().candidate_chunks, 2);
    assert_eq!(pruning.snapshot().pruned_chunks, 0);

    Ok(())
}

#[tokio::test]
async fn empty_snapshot_returns_empty_without_object_work() -> Result<()> {
    let fixture = FoundationFixture::new(base_schema());
    let snapshot = fixture.snapshot("snapshot-empty", &[])?;
    let context = fixture.context()?;
    _ = context.register_table("ordinary", snapshot.table_provider()?)?;
    fixture.metrics().reset();

    let batches = context.table("ordinary").await?.collect().await?;
    assert!(batches.is_empty());
    assert_eq!(fixture.metrics().snapshot().object_gets, 0);

    Ok(())
}

#[tokio::test]
async fn nullable_schema_evolution_reads_old_and_new_chunks() -> Result<()> {
    let old_schema = base_schema();
    let merged_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("quality", DataType::Utf8, true),
    ]));
    let fixture = FoundationFixture::new(Arc::clone(&merged_schema));
    _ = fixture
        .put_parquet("tables/old.parquet", &[base_batch(0)?], 4)
        .await?;
    let new_batch = RecordBatch::try_new(
        Arc::clone(&merged_schema),
        vec![
            Arc::new(Int64Array::from(vec![100, 101, 102, 103])),
            Arc::new(Float64Array::from(vec![10.0, 10.1, 10.2, 10.3])),
            Arc::new(StringArray::from(vec![
                Some("good"),
                Some("good"),
                None,
                Some("good"),
            ])),
        ],
    )?;
    _ = fixture
        .put_parquet("tables/new.parquet", &[new_batch], 4)
        .await?;
    let chunks = vec![
        ChunkDescriptor::new(
            "old",
            0,
            fixture.object_descriptor("tables/old.parquet")?,
            old_schema,
            4,
            None,
        ),
        ChunkDescriptor::new(
            "new",
            1,
            fixture.object_descriptor("tables/new.parquet")?,
            Arc::clone(&merged_schema),
            4,
            None,
        ),
    ];
    let snapshot = DatasetSnapshot::try_new(
        "snapshot-0001",
        Arc::clone(&merged_schema),
        chunks,
        None,
        OverlapPolicy::PreserveAll,
    )?;
    let context = fixture.context()?;
    _ = context.register_table("ordinary", snapshot.table_provider()?)?;

    let batches = context
        .table("ordinary")
        .await?
        .select(vec![col("id"), col("quality")])?
        .collect()
        .await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 8);
    let nulls = batches
        .iter()
        .map(|batch| batch.column(1).null_count())
        .sum::<usize>();
    assert_eq!(nulls, 5);

    Ok(())
}

#[test]
fn invalid_snapshot_metadata_fails_before_planning() -> Result<()> {
    let fixture = FoundationFixture::new(base_schema());
    let chunk = |id: &'static str, sequence: u64| -> Result<ChunkDescriptor> {
        Ok(ChunkDescriptor::new(
            id,
            sequence,
            fixture.object_descriptor("tables/chunk.parquet")?,
            base_schema(),
            4,
            None,
        ))
    };
    let error = DatasetSnapshot::try_new(
        "snapshot-invalid",
        base_schema(),
        vec![chunk("duplicate", 0)?, chunk("duplicate", 1)?],
        None,
        OverlapPolicy::PreserveAll,
    )
    .expect_err("duplicate chunk identity must fail");
    assert!(error.to_string().contains("duplicate chunk identity"));

    let error = DatasetSnapshot::try_new(
        "snapshot-invalid",
        base_schema(),
        vec![chunk("later", 1)?, chunk("earlier", 0)?],
        None,
        OverlapPolicy::PreserveAll,
    )
    .expect_err("unordered chunk sequence must fail");
    assert!(error.to_string().contains("not strictly greater"));

    let error = DatasetSnapshot::try_new(
        "snapshot-invalid",
        base_schema(),
        vec![chunk("chunk", 0)?],
        Some(EventTimeContract::new("missing")),
        OverlapPolicy::PreserveAll,
    )
    .expect_err("missing event-time column must fail");
    assert!(
        error
            .to_string()
            .contains("absent from the snapshot schema")
    );

    let error = DatasetSnapshot::try_new(
        "snapshot-invalid",
        base_schema(),
        vec![ChunkDescriptor::new(
            "collection",
            0,
            fixture.object_descriptor("tables/")?,
            base_schema(),
            4,
            None,
        )],
        None,
        OverlapPolicy::PreserveAll,
    )
    .expect_err("collection membership must fail");
    assert!(error.to_string().contains("exact object"));

    Ok(())
}

#[tokio::test]
async fn missing_physical_object_returns_an_error() -> Result<()> {
    let fixture = FoundationFixture::new(base_schema());
    let snapshot = fixture.snapshot("snapshot-0001", &[("tables/missing.parquet", 4)])?;
    let context = fixture.context()?;
    _ = context.register_table("ordinary", snapshot.table_provider()?)?;

    let error = context
        .table("ordinary")
        .await?
        .collect()
        .await
        .expect_err("missing snapshot object must fail");
    assert!(
        error.to_string().contains("not found") || error.to_string().contains("NotFound"),
        "{error}"
    );

    Ok(())
}
