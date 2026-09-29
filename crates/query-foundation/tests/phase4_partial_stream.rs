// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;
use query_foundation::partial::{
    PartialKey, PartialManifestBuilder, PartialStreamMetrics, manifest_from_stream,
};
use query_foundation::plans::reduce::{FixedWindowReduce, reduce_fixed_windows};
use query_foundation::statistics::TimeInterval;
use query_foundation::testkit::FoundationFixture;

fn reduced_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("bucket_start", DataType::Int64, false),
        Field::new("site", DataType::Utf8, false),
        Field::new("rows", DataType::Int64, false),
        Field::new("non_null", DataType::Int64, false),
        Field::new("sum", DataType::Float64, true),
        Field::new("min", DataType::Float64, true),
        Field::new("max", DataType::Float64, true),
    ]))
}

fn reduced_batch(
    buckets: Vec<i64>,
    sites: Vec<&str>,
    rows: Vec<i64>,
    non_null: Vec<i64>,
    sums: Vec<Option<f64>>,
    mins: Vec<Option<f64>>,
    maxes: Vec<Option<f64>>,
) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        reduced_schema(),
        vec![
            Arc::new(Int64Array::from(buckets)),
            Arc::new(StringArray::from(sites)),
            Arc::new(Int64Array::from(rows)),
            Arc::new(Int64Array::from(non_null)),
            Arc::new(Float64Array::from(sums)),
            Arc::new(Float64Array::from(mins)),
            Arc::new(Float64Array::from(maxes)),
        ],
    )?)
}

#[test]
fn builder_merges_partitioned_partials_and_tracks_peak_batch_only() -> Result<()> {
    let mut builder = PartialManifestBuilder::try_new("recipe", "source-state", 10, 0, ["site"])?;
    builder.push(&reduced_batch(
        vec![0, 10],
        vec!["a", "a"],
        vec![2, 2],
        vec![1, 0],
        vec![Some(3.0), None],
        vec![Some(3.0), None],
        vec![Some(3.0), None],
    )?)?;
    builder.push(&reduced_batch(
        vec![0],
        vec!["a"],
        vec![1],
        vec![1],
        vec![Some(5.0)],
        vec![Some(5.0)],
        vec![Some(5.0)],
    )?)?;
    let (manifest, metrics) = builder.finish()?;
    assert_eq!(
        metrics,
        PartialStreamMetrics {
            batches: 2,
            aggregate_rows: 3,
            peak_batch_rows: 2,
            retained_partials: 2,
        }
    );
    let key = PartialKey::new(0, [r#"Utf8("a")"#]);
    let merged = manifest
        .partials()
        .get(&key)
        .expect("partitioned key must be merged");
    assert_eq!(merged.rows(), 3);
    assert_eq!(merged.non_null(), 2);
    assert_eq!(merged.sum(), 8.0);
    assert_eq!(merged.min(), Some(3.0));
    assert_eq!(merged.max(), Some(5.0));
    let all_null = manifest
        .partials()
        .get(&PartialKey::new(10, [r#"Utf8("a")"#]))
        .expect("all-null key must exist");
    assert_eq!(all_null.rows(), 2);
    assert_eq!(all_null.non_null(), 0);
    assert_eq!(all_null.sum(), 0.0);
    assert_eq!(all_null.min(), None);

    Ok(())
}

#[tokio::test]
async fn real_reduction_stream_builds_manifest_without_collect() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("site", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
    ]));
    let source = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![0, 1, 10])),
            Arc::new(StringArray::from(vec!["a", "a", "a"])),
            Arc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0)])),
        ],
    )?;
    let fixture = FoundationFixture::new(schema);
    _ = fixture
        .put_parquet("reduce/stream.parquet", &[source], 4)
        .await?;
    let snapshot = fixture.timeseries_snapshot(
        "snapshot",
        "ts",
        &[(
            "reduce/stream.parquet",
            3,
            Some(TimeInterval::try_new(0, 10)?),
        )],
    )?;
    let context = fixture.context()?;
    _ = context.register_table("source", snapshot.table_provider()?)?;
    let recipe = FixedWindowReduce::try_new("ts", "value", ["site"], 10, 0)?;
    let reduced = reduce_fixed_windows(context.table("source").await?, &recipe)?;
    let stream = reduced.execute_stream().await?;
    let builder = PartialManifestBuilder::try_new("recipe", "snapshot", 10, 0, ["site"])?;
    let (manifest, metrics) = manifest_from_stream(stream, builder).await?;
    assert_eq!(manifest.partials().len(), 2);
    assert!(metrics.batches >= 1);
    assert_eq!(metrics.aggregate_rows, 2);
    assert!(metrics.peak_batch_rows <= 2);

    Ok(())
}

#[tokio::test]
async fn stream_and_aggregate_invariant_failures_surface() -> Result<()> {
    let stream = Box::pin(RecordBatchStreamAdapter::new(
        reduced_schema(),
        stream::iter(vec![Err(DataFusionError::Execution(
            "injected stream failure".to_owned(),
        ))]),
    ));
    let builder = PartialManifestBuilder::try_new("recipe", "source-state", 10, 0, ["site"])?;
    let error = manifest_from_stream(stream, builder)
        .await
        .expect_err("stream failure must surface");
    assert!(
        error.to_string().contains("injected stream failure"),
        "{error}"
    );

    let mut builder = PartialManifestBuilder::try_new("recipe", "source-state", 10, 0, ["site"])?;
    let error = builder
        .push(&reduced_batch(
            vec![0, 10],
            vec!["a", "a"],
            vec![1, 1],
            vec![1, 0],
            vec![Some(1.0), Some(1.0)],
            vec![Some(1.0), None],
            vec![Some(1.0), None],
        )?)
        .expect_err("invalid all-null aggregate must fail");
    assert!(error.to_string().contains("empty partial"), "{error}");
    let (manifest, metrics) = builder.finish()?;
    assert!(manifest.partials().is_empty());
    assert_eq!(metrics, PartialStreamMetrics::default());

    Ok(())
}
