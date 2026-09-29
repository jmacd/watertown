// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::common::{Column, ScalarValue};
use datafusion::error::Result;
use datafusion::logical_expr::{col, lit};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use query_foundation::plans::transform::{
    ProjectionColumn, cast_column, null_pad, project, rename_column, scope_prefix,
};
use query_foundation::testkit::FoundationFixture;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("raw", DataType::Float64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("unused", DataType::Utf8, false),
    ]))
}

fn batch() -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![0, 1, 2, 3])),
            Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0])),
            Arc::new(StringArray::from(vec!["bad", "20", "30", "40"])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
        ],
    )?)
}

async fn fixture_frame() -> Result<(FoundationFixture, datafusion::dataframe::DataFrame)> {
    let fixture = FoundationFixture::new(schema());
    _ = fixture
        .put_parquet("chunks/chunk.parquet", &[batch()?], 4)
        .await?;
    let snapshot = fixture.snapshot("snapshot-0001", &[("chunks/chunk.parquet", 4)])?;
    let context = fixture.context()?;
    _ = context.register_table("series", snapshot.table_provider()?)?;
    let frame = context.table("series").await?;
    Ok((fixture, frame))
}

#[tokio::test]
async fn rename_and_scope_are_logical_projections_with_leaf_pushdown() -> Result<()> {
    let (fixture, frame) = fixture_frame().await?;
    let frame = rename_column(frame, "raw", "value")?;
    let frame = scope_prefix(frame, "Station", "ts")?
        .filter(col(Column::from_name("Station.value")).gt(lit(10.0_f64)))?
        .select(vec![col("ts"), col(Column::from_name("Station.value"))])?;
    fixture.metrics().reset();
    let context = frame.task_ctx();
    let plan = frame.create_physical_plan().await?;
    let batches = datafusion::physical_plan::collect(Arc::clone(&plan), Arc::new(context)).await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);

    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(display.contains("projection=[ts, raw]"), "{display}");
    assert!(!display.contains("unused"), "{display}");
    assert!(!display.contains("label"), "{display}");
    assert!(display.contains("raw@1 > 10"), "{display}");
    assert!(!display.contains("MemoryExec"), "{display}");

    Ok(())
}

#[tokio::test]
async fn strict_cast_preserves_residual_filter_and_surfaces_failure() -> Result<()> {
    let (_fixture, frame) = fixture_frame().await?;
    let frame = cast_column(frame, "raw", DataType::Int64)?
        .filter(col("raw").gt(lit(10_i64)))?
        .select(vec![col("raw")])?;
    let task_context = Arc::new(frame.task_ctx());
    let plan = frame.create_physical_plan().await?;
    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(display.contains("FilterExec"), "{display}");
    assert!(display.contains("CAST(raw@0 AS Int64) > 10"), "{display}");
    assert!(display.contains("projection=[raw]"), "{display}");
    let batches = datafusion::physical_plan::collect(plan, task_context).await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);

    let (_fixture, frame) = fixture_frame().await?;
    let error = cast_column(frame, "label", DataType::Int64)?
        .collect()
        .await
        .expect_err("strict invalid cast must fail");
    assert!(error.to_string().contains("Cast error"), "{error}");

    Ok(())
}

#[tokio::test]
async fn null_padding_is_typed_and_mixed_predicates_stay_correct() -> Result<()> {
    let (_fixture, frame) = fixture_frame().await?;
    let frame = null_pad(frame, vec![("quality".to_owned(), DataType::Utf8)])?
        .filter(col("raw").gt(lit(10.0_f64)).and(col("quality").is_null()))?
        .select(vec![col("raw"), col("quality")])?;
    let task_context = Arc::new(frame.task_ctx());
    let plan = frame.create_physical_plan().await?;
    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(display.contains("FilterExec"), "{display}");
    assert!(display.contains("raw@0 > 10"), "{display}");
    assert!(display.contains("projection=[raw]"), "{display}");
    assert!(!display.contains("unused"), "{display}");
    assert!(!display.contains("label"), "{display}");
    let batches = datafusion::physical_plan::collect(plan, task_context).await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.column(1).null_count())
            .sum::<usize>(),
        3
    );
    assert_eq!(batches[0].schema().field(1).data_type(), &DataType::Utf8);

    Ok(())
}

#[tokio::test]
async fn timestamp_unit_cast_uses_projection_and_invalid_padding_fails() -> Result<()> {
    let (_fixture, frame) = fixture_frame().await?;
    let frame = cast_column(
        frame,
        "ts",
        DataType::Timestamp(TimeUnit::Millisecond, None),
    )?
    .filter(col("ts").gt(lit(ScalarValue::TimestampMillisecond(Some(1), None))))?
    .select(vec![col("ts")])?;
    assert_eq!(
        frame
            .schema()
            .field_with_unqualified_name("ts")?
            .data_type(),
        &DataType::Timestamp(TimeUnit::Millisecond, None)
    );
    let task_context = Arc::new(frame.task_ctx());
    let plan = frame.create_physical_plan().await?;
    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(display.contains("FilterExec"), "{display}");
    assert!(display.contains("CAST(ts@0 AS Timestamp"), "{display}");
    assert!(display.contains("projection=[ts]"), "{display}");
    assert!(!display.contains("unused"), "{display}");
    let batches = datafusion::physical_plan::collect(plan, task_context).await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);

    let (_fixture, frame) = fixture_frame().await?;
    let error = null_pad(frame, vec![("raw".to_owned(), DataType::Int64)])
        .expect_err("incompatible padding type must fail");
    assert!(
        error.to_string().contains("existing column 'raw'"),
        "{error}"
    );

    Ok(())
}

#[tokio::test]
async fn direct_alias_and_type_preserving_cast_remain_logical() -> Result<()> {
    let (_fixture, frame) = fixture_frame().await?;
    let frame = project(
        frame,
        vec![
            ProjectionColumn::identity("ts"),
            ProjectionColumn::cast("raw", "value", DataType::Float64),
        ],
    )?
    .filter(col("value").gt(lit(10.0_f64)))?
    .select(vec![col("value")])?;
    assert_eq!(
        frame
            .schema()
            .field_with_unqualified_name("value")?
            .data_type(),
        &DataType::Float64
    );
    let task_context = Arc::new(frame.task_ctx());
    let plan = frame.create_physical_plan().await?;
    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(display.contains("FilterExec"), "{display}");
    assert!(display.contains("projection=[raw]"), "{display}");
    assert!(!display.contains("MemoryExec"), "{display}");
    let batches = datafusion::physical_plan::collect(plan, task_context).await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);

    Ok(())
}

#[tokio::test]
async fn invalid_transform_definitions_fail_during_planning() -> Result<()> {
    let (_fixture, frame) = fixture_frame().await?;
    let error = project(
        frame,
        vec![
            ProjectionColumn::rename("raw", "value"),
            ProjectionColumn::rename("label", "value"),
        ],
    )
    .expect_err("duplicate aliases must fail");
    assert!(
        error.to_string().contains("duplicate projection output"),
        "{error}"
    );

    let (_fixture, frame) = fixture_frame().await?;
    let error =
        rename_column(frame, "missing", "value").expect_err("missing rename source must fail");
    assert!(error.to_string().contains("missing"), "{error}");

    let (_fixture, frame) = fixture_frame().await?;
    let error =
        cast_column(frame, "missing", DataType::Int64).expect_err("missing cast source must fail");
    assert!(error.to_string().contains("missing"), "{error}");

    let (_fixture, frame) = fixture_frame().await?;
    let error = scope_prefix(frame, "", "ts").expect_err("empty scope must fail");
    assert!(error.to_string().contains("must not be empty"), "{error}");

    Ok(())
}
