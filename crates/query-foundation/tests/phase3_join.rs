// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::Column;
use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::{col, lit};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::SessionContext;
use query_foundation::overlap::OverlapPolicy;
use query_foundation::plans::combine::{CombineInput, combine_same_scope};
use query_foundation::plans::join::{TimestampJoinInput, accumulated_full_outer_join};
use query_foundation::snapshot::{ChunkDescriptor, DatasetSnapshot, EventTimeContract};
use query_foundation::statistics::TimeInterval;
use query_foundation::testkit::FoundationFixture;

fn schema(event_time: &str, value: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(event_time, DataType::Int64, false),
        Field::new(value, DataType::Float64, false),
        Field::new(format!("{value}.unused"), DataType::Utf8, false),
    ]))
}

fn batch(schema: SchemaRef, timestamps: Vec<i64>, values: Vec<f64>) -> Result<RecordBatch> {
    let unused = timestamps
        .iter()
        .map(|timestamp| format!("unused-{timestamp}"))
        .collect::<Vec<_>>();
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(unused)),
        ],
    )?)
}

async fn register_frame(
    fixture: &FoundationFixture,
    context: &SessionContext,
    table: &str,
    event_time: &str,
    value: &str,
    timestamps: Vec<i64>,
    values: Vec<f64>,
) -> Result<DataFrame> {
    let schema = schema(event_time, value);
    let path = format!("join/{table}.parquet");
    let logical_count = timestamps.len() as u64;
    let bounds = match (timestamps.iter().min(), timestamps.iter().max()) {
        (Some(min), Some(max)) => Some(TimeInterval::try_new(*min, *max)?),
        _ => None,
    };
    _ = fixture
        .put_parquet(&path, &[batch(Arc::clone(&schema), timestamps, values)?], 4)
        .await?;
    let snapshot = DatasetSnapshot::try_new(
        format!("{table}-snapshot"),
        Arc::clone(&schema),
        vec![ChunkDescriptor::new(
            format!("{table}-chunk"),
            1,
            fixture.object_descriptor(&path)?,
            schema,
            logical_count,
            bounds,
        )],
        Some(EventTimeContract::new(event_time)),
        OverlapPolicy::PreserveAll,
    )?;
    _ = context.register_table(table, snapshot.table_provider()?)?;
    context.table(table).await
}

fn timestamp_counts(batches: &[RecordBatch], event_time: &str) -> BTreeMap<i64, usize> {
    let mut counts = BTreeMap::new();
    for batch in batches {
        let timestamps = batch
            .column_by_name(event_time)
            .expect("event-time column must exist")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("event-time column must be Int64");
        for index in 0..batch.num_rows() {
            assert!(!timestamps.is_null(index));
            *counts.entry(timestamps.value(index)).or_default() += 1;
        }
    }
    counts
}

#[tokio::test]
async fn two_way_join_preserves_sparse_many_to_many_rows_and_leaf_projection() -> Result<()> {
    let fixture = FoundationFixture::new(schema("ts", "A.value"));
    let context = fixture.context()?;
    let left = register_frame(
        &fixture,
        &context,
        "left",
        "ts",
        "A.value",
        vec![1, 2, 2],
        vec![10.0, 20.0, 21.0],
    )
    .await?;
    let right = register_frame(
        &fixture,
        &context,
        "right",
        "time",
        "B.value",
        vec![2, 2, 3],
        vec![200.0, 201.0, 300.0],
    )
    .await?;

    let joined = accumulated_full_outer_join(
        vec![
            TimestampJoinInput::new(left, "ts"),
            TimestampJoinInput::new(right, "time"),
        ],
        "ts",
    )?;
    let batches = joined.clone().collect().await?;
    assert_eq!(
        timestamp_counts(&batches, "ts"),
        BTreeMap::from([(1, 1), (2, 4), (3, 1)])
    );

    let narrow = joined
        .clone()
        .select(vec![col("ts"), col(Column::from_name("A.value"))])?;
    let plan = narrow.create_physical_plan().await?;
    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert_eq!(display.matches("DataSourceExec").count(), 2, "{display}");
    assert!(display.contains("projection=[ts, A.value]"), "{display}");
    assert!(display.contains("projection=[time]"), "{display}");
    assert!(!display.contains("B.value"), "{display}");
    assert!(!display.contains("unused"), "{display}");
    assert!(!display.contains("Distinct"), "{display}");

    let filtered = joined
        .clone()
        .filter(col(Column::from_name("A.value")).gt(lit(20.0_f64)))?;
    let plan = filtered.clone().create_physical_plan().await?;
    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(display.contains("HashJoinExec"), "{display}");
    assert!(display.contains("predicate=A.value@1 > 20"), "{display}");
    assert_eq!(
        timestamp_counts(&filtered.collect().await?, "ts"),
        BTreeMap::from([(2, 2)])
    );

    let reduced = joined.aggregate(
        Vec::<datafusion::logical_expr::Expr>::new(),
        vec![count(lit(1_i64)).alias("rows")],
    )?;
    let plan = reduced.clone().create_physical_plan().await?;
    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(display.contains("AggregateExec"), "{display}");
    assert_eq!(display.matches("DataSourceExec").count(), 2, "{display}");
    let batches = reduced.collect().await?;
    let count = batches[0]
        .column_by_name("rows")
        .expect("row count must exist")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("COUNT must return Int64")
        .value(0);
    assert_eq!(count, 6);

    Ok(())
}

#[tokio::test]
async fn accumulated_join_keeps_timestamps_absent_from_first_and_empty_inputs() -> Result<()> {
    let fixture = FoundationFixture::new(schema("ts", "A.value"));
    let context = fixture.context()?;
    let first = register_frame(
        &fixture,
        &context,
        "first",
        "ts",
        "A.value",
        vec![1],
        vec![10.0],
    )
    .await?;
    let empty =
        register_frame(&fixture, &context, "empty", "ts", "B.value", vec![], vec![]).await?;
    let third = register_frame(
        &fixture,
        &context,
        "third",
        "ts",
        "C.value",
        vec![2, 3],
        vec![20.0, 30.0],
    )
    .await?;
    let joined = accumulated_full_outer_join(
        vec![
            TimestampJoinInput::new(first, "ts"),
            TimestampJoinInput::new(empty, "ts"),
            TimestampJoinInput::new(third, "ts"),
        ],
        "ts",
    )?;
    assert_eq!(
        timestamp_counts(&joined.clone().collect().await?, "ts"),
        BTreeMap::from([(1, 1), (2, 1), (3, 1)])
    );
    let plan = joined.create_physical_plan().await?;
    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert_eq!(display.matches("DataSourceExec").count(), 3, "{display}");
    assert_eq!(display.matches("HashJoinExec").count(), 2, "{display}");

    Ok(())
}

#[tokio::test]
async fn each_join_input_applies_its_own_exact_bounds() -> Result<()> {
    let fixture = FoundationFixture::new(schema("ts", "A.value"));
    let context = fixture.context()?;
    let left = register_frame(
        &fixture,
        &context,
        "left",
        "ts",
        "A.value",
        vec![1, 2],
        vec![10.0, 20.0],
    )
    .await?;
    let right = register_frame(
        &fixture,
        &context,
        "right",
        "ts",
        "B.value",
        vec![2, 3],
        vec![200.0, 300.0],
    )
    .await?;
    let joined = accumulated_full_outer_join(
        vec![
            TimestampJoinInput::new(left, "ts").with_bounds(TimeInterval::try_new(2, 2)?),
            TimestampJoinInput::new(right, "ts").with_bounds(TimeInterval::try_new(3, 3)?),
        ],
        "ts",
    )?;
    let plan = joined.clone().create_physical_plan().await?;
    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(display.contains("predicate=ts@0 = 2"), "{display}");
    assert!(display.contains("predicate=ts@0 = 3"), "{display}");
    assert_eq!(
        timestamp_counts(&joined.collect().await?, "ts"),
        BTreeMap::from([(2, 1), (3, 1)])
    );

    Ok(())
}

#[tokio::test]
async fn same_scope_combine_precedes_cross_scope_join() -> Result<()> {
    let fixture = FoundationFixture::new(schema("ts", "Station.value"));
    let context = fixture.context()?;
    let archive = register_frame(
        &fixture,
        &context,
        "archive",
        "ts",
        "Station.value",
        vec![1],
        vec![10.0],
    )
    .await?;
    let live = register_frame(
        &fixture,
        &context,
        "live",
        "ts",
        "Station.value",
        vec![2],
        vec![20.0],
    )
    .await?;
    let weather = register_frame(
        &fixture,
        &context,
        "weather",
        "ts",
        "Weather.rain",
        vec![1, 2],
        vec![0.1, 0.2],
    )
    .await?;
    let station = combine_same_scope(
        vec![
            CombineInput::new("archive", 1, archive),
            CombineInput::new("live", 2, live),
        ],
        &OverlapPolicy::PreserveAll,
    )
    .await?;
    let joined = accumulated_full_outer_join(
        vec![
            TimestampJoinInput::new(station, "ts"),
            TimestampJoinInput::new(weather, "ts"),
        ],
        "ts",
    )?;
    assert_eq!(
        timestamp_counts(&joined.collect().await?, "ts"),
        BTreeMap::from([(1, 1), (2, 1)])
    );

    Ok(())
}

#[tokio::test]
async fn invalid_join_definitions_fail_during_planning() -> Result<()> {
    let fixture = FoundationFixture::new(schema("ts", "value"));
    let context = fixture.context()?;
    let left = register_frame(
        &fixture,
        &context,
        "left",
        "ts",
        "value",
        vec![1],
        vec![10.0],
    )
    .await?;
    let right = register_frame(
        &fixture,
        &context,
        "right",
        "ts",
        "value",
        vec![2],
        vec![20.0],
    )
    .await?;
    let error = accumulated_full_outer_join(
        vec![
            TimestampJoinInput::new(left.clone(), "ts"),
            TimestampJoinInput::new(right.clone(), "ts"),
        ],
        "ts",
    )
    .expect_err("unscoped duplicate outputs must fail");
    assert!(
        error.to_string().contains("scope columns or combine"),
        "{error}"
    );

    let error = accumulated_full_outer_join(
        vec![
            TimestampJoinInput::new(left.clone(), "missing"),
            TimestampJoinInput::new(right, "ts"),
        ],
        "ts",
    )
    .expect_err("missing event-time column must fail");
    assert!(
        error.to_string().contains("missing event-time column"),
        "{error}"
    );

    let error = accumulated_full_outer_join(vec![TimestampJoinInput::new(left, "ts")], "ts")
        .expect_err("one input must fail");
    assert!(error.to_string().contains("at least two inputs"), "{error}");

    Ok(())
}
