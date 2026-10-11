// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use datafusion::logical_expr::{col, lit};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use query_foundation::overlap::{OverlapPolicy, RowIdentity};
use query_foundation::plans::combine::{CombineInput, combine_same_scope};
use query_foundation::snapshot::{ChunkDescriptor, DatasetSnapshot, EventTimeContract};
use query_foundation::statistics::TimeInterval;
use query_foundation::testkit::FoundationFixture;

fn series_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn series_batch(timestamps: Vec<i64>, values: Vec<f64>) -> Result<RecordBatch> {
    let payloads = timestamps
        .iter()
        .map(|timestamp| format!("{timestamp}-{}", "x".repeat(400_000)))
        .collect::<Vec<_>>();
    Ok(RecordBatch::try_new(
        series_schema(),
        vec![
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(payloads)),
        ],
    )?)
}

async fn series_frames(
    archive: RecordBatch,
    live: RecordBatch,
) -> Result<(FoundationFixture, DataFrame, DataFrame)> {
    let fixture = FoundationFixture::new(series_schema());
    _ = fixture
        .put_parquet("combine/archive.parquet", &[archive], 4)
        .await?;
    _ = fixture
        .put_parquet("combine/live.parquet", &[live], 4)
        .await?;
    let archive = one_chunk_snapshot(
        &fixture,
        "archive",
        "combine/archive.parquet",
        series_schema(),
        2,
        Some((1, 2)),
    )?;
    let live = one_chunk_snapshot(
        &fixture,
        "live",
        "combine/live.parquet",
        series_schema(),
        2,
        Some((2, 3)),
    )?;
    let context = fixture.context()?;
    _ = context.register_table("archive", archive.table_provider()?)?;
    _ = context.register_table("live", live.table_provider()?)?;
    let archive = context.table("archive").await?;
    let live = context.table("live").await?;
    Ok((fixture, archive, live))
}

fn one_chunk_snapshot(
    fixture: &FoundationFixture,
    id: &str,
    path: &str,
    schema: SchemaRef,
    logical_count: u64,
    bounds: Option<(i64, i64)>,
) -> Result<DatasetSnapshot> {
    let bounds = bounds
        .map(|(min, max)| TimeInterval::try_new(min, max))
        .transpose()?;
    DatasetSnapshot::try_new(
        format!("{id}-snapshot"),
        Arc::clone(&schema),
        vec![ChunkDescriptor::new(
            format!("{id}-chunk"),
            1,
            fixture.object_descriptor(path)?,
            schema,
            logical_count,
            bounds,
        )],
        Some(EventTimeContract::new("ts")),
        OverlapPolicy::PreserveAll,
    )
}

fn input(source: &str, sequence: u64, frame: DataFrame) -> CombineInput {
    CombineInput::new(source, sequence, frame)
}

fn rows(batches: &[RecordBatch]) -> Vec<(i64, f64)> {
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
            let timestamps = batch
                .column_by_name("ts")
                .expect("ts must exist")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("ts must be Int64");
            let values = batch
                .column_by_name("value")
                .expect("value must exist")
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("value must be Float64");
            (0..batch.num_rows())
                .map(|index| (timestamps.value(index), values.value(index)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.total_cmp(&right.1)));
    rows
}

#[tokio::test]
async fn preserve_all_aligns_schemas_and_prunes_leaf_columns() -> Result<()> {
    let archive_schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("archive_note", DataType::Utf8, false),
    ]));
    let live_schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("live_note", DataType::Utf8, false),
    ]));
    let fixture = FoundationFixture::new(Arc::clone(&archive_schema));
    let archive_batch = RecordBatch::try_new(
        Arc::clone(&archive_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Float64Array::from(vec![10.0, 20.0])),
            Arc::new(StringArray::from(vec!["old-1", "old-2"])),
        ],
    )?;
    let live_batch = RecordBatch::try_new(
        Arc::clone(&live_schema),
        vec![
            Arc::new(Int64Array::from(vec![2, 3])),
            Arc::new(Float64Array::from(vec![200.0, 300.0])),
            Arc::new(StringArray::from(vec!["new-2", "new-3"])),
        ],
    )?;
    _ = fixture
        .put_parquet("combine/archive.parquet", &[archive_batch], 4)
        .await?;
    _ = fixture
        .put_parquet("combine/live.parquet", &[live_batch], 4)
        .await?;
    let archive = one_chunk_snapshot(
        &fixture,
        "archive",
        "combine/archive.parquet",
        archive_schema,
        2,
        Some((1, 2)),
    )?;
    let live = one_chunk_snapshot(
        &fixture,
        "live",
        "combine/live.parquet",
        live_schema,
        2,
        Some((2, 3)),
    )?;
    let context = fixture.context()?;
    _ = context.register_table("archive", archive.table_provider()?)?;
    _ = context.register_table("live", live.table_provider()?)?;

    let combined = combine_same_scope(
        vec![
            input("archive", 1, context.table("archive").await?),
            input("live", 2, context.table("live").await?),
        ],
        &OverlapPolicy::PreserveAll,
    )
    .await?;
    let batches = combined.clone().collect().await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
    let schema = batches[0].schema();
    assert!(schema.field_with_name("archive_note").is_ok());
    assert!(schema.field_with_name("live_note").is_ok());
    assert_eq!(
        batches
            .iter()
            .map(|batch| {
                batch
                    .column_by_name("archive_note")
                    .expect("archive_note must exist")
                    .null_count()
            })
            .sum::<usize>(),
        2
    );
    assert_eq!(
        batches
            .iter()
            .map(|batch| {
                batch
                    .column_by_name("live_note")
                    .expect("live_note must exist")
                    .null_count()
            })
            .sum::<usize>(),
        2
    );

    let plan = combined
        .select(vec![col("ts")])?
        .create_physical_plan()
        .await?;
    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert_eq!(display.matches("DataSourceExec").count(), 2, "{display}");
    assert_eq!(display.matches("projection=[ts]").count(), 2, "{display}");
    assert!(!display.contains("archive_note"), "{display}");
    assert!(!display.contains("live_note"), "{display}");
    assert!(!display.contains("AggregateExec"), "{display}");
    assert!(!display.contains("WindowAggExec"), "{display}");

    Ok(())
}

#[tokio::test]
async fn combine_captures_exact_input_membership() -> Result<()> {
    let (_fixture, archive, live) = series_frames(
        series_batch(vec![1, 2], vec![10.0, 20.0])?,
        series_batch(vec![3, 4], vec![30.0, 40.0])?,
    )
    .await?;
    let captured = combine_same_scope(
        vec![
            input("archive", 1, archive.clone()),
            input("live", 2, live.clone()),
        ],
        &OverlapPolicy::PreserveAll,
    )
    .await?;
    let expanded = combine_same_scope(
        vec![
            input("archive", 1, archive),
            input("live", 2, live.clone()),
            input("later-member", 3, live.clone()),
        ],
        &OverlapPolicy::PreserveAll,
    )
    .await?;
    let reduced =
        combine_same_scope(vec![input("live", 2, live)], &OverlapPolicy::PreserveAll).await?;

    assert_eq!(
        captured
            .collect()
            .await?
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        4
    );
    assert_eq!(
        expanded
            .collect()
            .await?
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        6
    );
    assert_eq!(
        reduced
            .collect()
            .await?
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );

    Ok(())
}

#[tokio::test]
async fn require_disjoint_validates_complete_source_ranges() -> Result<()> {
    let (_fixture, archive, live) = series_frames(
        series_batch(vec![1, 2], vec![10.0, 20.0])?,
        series_batch(vec![2, 3], vec![200.0, 300.0])?,
    )
    .await?;
    let disjoint = combine_same_scope(
        vec![
            input("archive", 1, archive.clone())
                .with_event_time_ranges(vec![TimeInterval::try_new(1, 2)?]),
            input("live", 2, live.clone())
                .with_event_time_ranges(vec![TimeInterval::try_new(3, 4)?]),
        ],
        &OverlapPolicy::RequireDisjoint,
    )
    .await?;
    assert_eq!(
        disjoint
            .collect()
            .await?
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        4
    );

    let error = combine_same_scope(
        vec![
            input("archive", 1, archive.clone())
                .with_event_time_ranges(vec![TimeInterval::try_new(1, 2)?]),
            input("live", 2, live.clone())
                .with_event_time_ranges(vec![TimeInterval::try_new(2, 3)?]),
        ],
        &OverlapPolicy::RequireDisjoint,
    )
    .await
    .expect_err("overlapping source ranges must fail");
    assert!(error.to_string().contains("archive"), "{error}");
    assert!(error.to_string().contains("live"), "{error}");

    let error = combine_same_scope(
        vec![input("archive", 1, archive), input("live", 2, live)],
        &OverlapPolicy::RequireDisjoint,
    )
    .await
    .expect_err("missing source ranges must fail");
    assert!(
        error.to_string().contains("complete event-time ranges"),
        "{error}"
    );

    Ok(())
}

#[tokio::test]
async fn reject_duplicate_key_names_conflicting_sources_and_key() -> Result<()> {
    let (_fixture, archive, live) = series_frames(
        series_batch(vec![1, 2], vec![10.0, 20.0])?,
        series_batch(vec![2, 3], vec![200.0, 300.0])?,
    )
    .await?;
    let policy = OverlapPolicy::reject_duplicate_key(RowIdentity::try_new(["ts"])?);
    let error = combine_same_scope(
        vec![input("archive", 1, archive), input("live", 2, live)],
        &policy,
    )
    .await
    .expect_err("duplicate logical key must fail");
    let message = error.to_string();
    assert!(message.contains("duplicate logical key"), "{message}");
    assert!(message.contains("ts=2"), "{message}");
    assert!(message.contains("archive"), "{message}");
    assert!(message.contains("live"), "{message}");

    let (fixture, archive, live) = series_frames(
        series_batch(vec![1, 2], vec![10.0, 20.0])?,
        series_batch(vec![3, 4], vec![30.0, 40.0])?,
    )
    .await?;
    fixture.metrics().reset();
    let combined = combine_same_scope(
        vec![input("archive", 1, archive), input("live", 2, live)],
        &policy,
    )
    .await?;
    let validation_work = fixture.metrics().snapshot();
    assert_eq!(validation_work.opened_objects.len(), 2);
    fixture.metrics().reset();
    assert_eq!(
        combined
            .collect()
            .await?
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        4
    );
    let output_work = fixture.metrics().snapshot();
    assert!(
        validation_work.bytes_returned < output_work.bytes_returned,
        "validation={validation_work:?}, output={output_work:?}"
    );

    Ok(())
}

#[tokio::test]
async fn prefer_sequence_is_deterministic_and_keeps_predicates_above_reconciliation() -> Result<()>
{
    let (_fixture, archive, live) = series_frames(
        series_batch(vec![1, 2], vec![100.0, 200.0])?,
        series_batch(vec![2, 3], vec![20.0, 30.0])?,
    )
    .await?;
    let policy = OverlapPolicy::prefer_by_sequence(RowIdentity::try_new(["ts"])?);
    let combined = combine_same_scope(
        vec![
            input("archive", 1, archive.clone()),
            input("live", 2, live.clone()),
        ],
        &policy,
    )
    .await?;
    assert_eq!(
        rows(&combined.clone().collect().await?),
        vec![(1, 100.0), (2, 20.0), (3, 30.0)]
    );

    let filtered = combined.filter(col("value").gt(lit(150.0_f64)))?;
    let plan = filtered.clone().create_physical_plan().await?;
    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(display.contains("WindowAggExec"), "{display}");
    let window = display
        .find("WindowAggExec")
        .expect("window must be present");
    let filter = display.find("FilterExec").expect("filter must be present");
    assert!(filter < window, "{display}");
    assert!(filtered.collect().await?.is_empty());

    let narrow = combine_same_scope(
        vec![input("archive", 1, archive), input("live", 2, live)],
        &policy,
    )
    .await?
    .select(vec![col("ts")])?;
    let plan = narrow.create_physical_plan().await?;
    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert_eq!(display.matches("DataSourceExec").count(), 2, "{display}");
    assert!(
        display.contains("projection=[ts, 1 as __watertown_combine_sequence]"),
        "{display}"
    );
    assert!(
        display.contains("projection=[ts, 2 as __watertown_combine_sequence]"),
        "{display}"
    );
    assert!(!display.contains("projection=[ts, value]"), "{display}");

    let (_fixture, archive, live) = series_frames(
        series_batch(vec![1, 2], vec![100.0, 200.0])?,
        series_batch(vec![2, 2], vec![20.0, 21.0])?,
    )
    .await?;
    let error = combine_same_scope(
        vec![input("archive", 1, archive), input("live", 2, live)],
        &policy,
    )
    .await
    .expect_err("equal-sequence duplicate keys must fail");
    let message = error.to_string();
    assert!(
        message.contains("PreferBySequence is ambiguous"),
        "{message}"
    );
    assert!(message.contains("ts=2"), "{message}");
    assert!(message.contains("live"), "{message}");
    assert!(message.contains("sequence 2"), "{message}");

    Ok(())
}

#[tokio::test]
async fn invalid_combine_definitions_fail_before_output_planning() -> Result<()> {
    let (_fixture, archive, live) = series_frames(
        series_batch(vec![1, 2], vec![10.0, 20.0])?,
        series_batch(vec![3, 4], vec![30.0, 40.0])?,
    )
    .await?;
    let error = combine_same_scope(Vec::new(), &OverlapPolicy::PreserveAll)
        .await
        .expect_err("empty combine input must fail");
    assert!(
        error.to_string().contains("requires at least one input"),
        "{error}"
    );

    let missing_key = OverlapPolicy::reject_duplicate_key(RowIdentity::try_new(["missing"])?);
    let error = combine_same_scope(
        vec![
            input("archive", 1, archive.clone()),
            input("live", 2, live.clone()),
        ],
        &missing_key,
    )
    .await
    .expect_err("missing logical key column must fail");
    assert!(
        error
            .to_string()
            .contains("missing row identity column 'missing'"),
        "{error}"
    );

    let error = combine_same_scope(
        vec![
            input("source", 1, archive.clone()),
            input("source", 2, live.clone()),
        ],
        &OverlapPolicy::PreserveAll,
    )
    .await
    .expect_err("duplicate source identity must fail");
    assert!(error.to_string().contains("duplicate same-scope source"));

    let error = combine_same_scope(
        vec![input("archive", 1, archive), input("live", 1, live)],
        &OverlapPolicy::PreserveAll,
    )
    .await
    .expect_err("duplicate source sequence must fail");
    assert!(
        error
            .to_string()
            .contains("duplicate same-scope source sequence"),
        "{error}"
    );

    Ok(())
}
