// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::{col, lit};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::SessionContext;
use query_foundation::overlap::OverlapPolicy;
use query_foundation::plans::pivot::{MissingPivotColumn, PivotInput, pivot_measurements};
use query_foundation::snapshot::{ChunkDescriptor, DatasetSnapshot, EventTimeContract};
use query_foundation::statistics::TimeInterval;
use query_foundation::testkit::FoundationFixture;

fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("unused", DataType::Utf8, false),
    ]))
}

async fn register_measurement(
    fixture: &FoundationFixture,
    context: &SessionContext,
    name: &str,
    timestamps: Vec<i64>,
    values: Vec<f64>,
) -> Result<DataFrame> {
    let schema = source_schema();
    let unused = timestamps
        .iter()
        .map(|timestamp| format!("unused-{timestamp}"))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(timestamps.clone())),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(unused)),
        ],
    )?;
    let path = format!("pivot/{name}.parquet");
    _ = fixture.put_parquet(&path, &[batch], 4).await?;
    let bounds = match (timestamps.iter().min(), timestamps.iter().max()) {
        (Some(min), Some(max)) => Some(TimeInterval::try_new(*min, *max)?),
        _ => None,
    };
    let snapshot = DatasetSnapshot::try_new(
        format!("{name}-snapshot"),
        Arc::clone(&schema),
        vec![ChunkDescriptor::new(
            format!("{name}-chunk"),
            1,
            fixture.object_descriptor(&path)?,
            schema,
            timestamps.len() as u64,
            bounds,
        )],
        Some(EventTimeContract::new("ts")),
        OverlapPolicy::PreserveAll,
    )?;
    _ = context.register_table(name, snapshot.table_provider()?)?;
    context.table(name).await
}

fn timestamp_counts(batches: &[RecordBatch]) -> BTreeMap<i64, usize> {
    let mut counts = BTreeMap::new();
    for batch in batches {
        let timestamps = batch
            .column_by_name("ts")
            .expect("ts must exist")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ts must be Int64");
        for index in 0..batch.num_rows() {
            assert!(!timestamps.is_null(index));
            *counts.entry(timestamps.value(index)).or_default() += 1;
        }
    }
    counts
}

#[tokio::test]
async fn one_measurement_pivot_is_only_a_projected_scan() -> Result<()> {
    let fixture = FoundationFixture::new(source_schema());
    let context = fixture.context()?;
    let temperature = register_measurement(
        &fixture,
        &context,
        "temperature",
        vec![1, 2],
        vec![10.0, 20.0],
    )
    .await?;
    let pivot = pivot_measurements(
        vec![PivotInput::new(temperature, "ts", "value", "temperature")],
        vec![],
        "ts",
    )?;
    let batches = pivot.clone().collect().await?;
    assert_eq!(timestamp_counts(&batches), BTreeMap::from([(1, 1), (2, 1)]));
    assert!(batches[0].schema().field_with_name("temperature").is_ok());

    let plan = pivot.create_physical_plan().await?;
    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert_eq!(display.matches("DataSourceExec").count(), 1, "{display}");
    assert!(
        display.contains("projection=[ts, value@1 as temperature]"),
        "{display}"
    );
    assert!(!display.contains("HashJoinExec"), "{display}");
    assert!(!display.contains("unused"), "{display}");

    Ok(())
}

#[tokio::test]
async fn sparse_pivot_aligns_measurements_and_pads_absent_columns() -> Result<()> {
    let fixture = FoundationFixture::new(source_schema());
    let context = fixture.context()?;
    let temperature = register_measurement(
        &fixture,
        &context,
        "temperature",
        vec![1, 2],
        vec![10.0, 20.0],
    )
    .await?;
    let oxygen =
        register_measurement(&fixture, &context, "oxygen", vec![2, 3], vec![8.0, 7.0]).await?;
    let pivot = pivot_measurements(
        vec![
            PivotInput::new(temperature, "ts", "value", "temperature"),
            PivotInput::new(oxygen, "ts", "value", "oxygen"),
        ],
        vec![MissingPivotColumn::new("pressure", DataType::Float64)],
        "ts",
    )?;
    let batches = pivot.clone().collect().await?;
    assert_eq!(
        timestamp_counts(&batches),
        BTreeMap::from([(1, 1), (2, 1), (3, 1)])
    );
    assert_eq!(
        batches
            .iter()
            .map(|batch| {
                batch
                    .column_by_name("pressure")
                    .expect("pressure must exist")
                    .null_count()
            })
            .sum::<usize>(),
        3
    );

    let plan = pivot.create_physical_plan().await?;
    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert_eq!(display.matches("DataSourceExec").count(), 2, "{display}");
    assert_eq!(display.matches("value@1 as").count(), 2, "{display}");
    assert!(display.contains("value@1 as temperature"), "{display}");
    assert!(display.contains("value@1 as oxygen"), "{display}");
    assert_eq!(display.matches("HashJoinExec").count(), 1, "{display}");
    assert!(!display.contains("unused"), "{display}");

    Ok(())
}

#[tokio::test]
async fn duplicate_timestamps_preserve_multiplicity_under_independent_bounds() -> Result<()> {
    let fixture = FoundationFixture::new(source_schema());
    let context = fixture.context()?;
    let temperature = register_measurement(
        &fixture,
        &context,
        "temperature",
        vec![1, 2, 2],
        vec![10.0, 20.0, 21.0],
    )
    .await?;
    let oxygen = register_measurement(
        &fixture,
        &context,
        "oxygen",
        vec![2, 2, 3],
        vec![8.0, 8.1, 7.0],
    )
    .await?;
    let pivot = pivot_measurements(
        vec![
            PivotInput::new(temperature, "ts", "value", "temperature")
                .with_bounds(TimeInterval::try_new(2, 2)?),
            PivotInput::new(oxygen, "ts", "value", "oxygen")
                .with_bounds(TimeInterval::try_new(2, 3)?),
        ],
        vec![],
        "ts",
    )?;
    let plan = pivot.clone().create_physical_plan().await?;
    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(display.contains("predicate=ts@0 = 2"), "{display}");
    assert!(display.contains("ts@0 >= 2"), "{display}");
    assert!(display.contains("ts@0 <= 3"), "{display}");
    assert_eq!(
        timestamp_counts(&pivot.collect().await?),
        BTreeMap::from([(2, 4), (3, 1)])
    );

    Ok(())
}

#[tokio::test]
async fn empty_measurement_and_downstream_projection_do_not_add_spine_scans() -> Result<()> {
    let fixture = FoundationFixture::new(source_schema());
    let context = fixture.context()?;
    let temperature = register_measurement(
        &fixture,
        &context,
        "temperature",
        vec![1, 2],
        vec![10.0, 20.0],
    )
    .await?;
    let empty = register_measurement(&fixture, &context, "empty", vec![], vec![]).await?;
    let pivot = pivot_measurements(
        vec![
            PivotInput::new(temperature, "ts", "value", "temperature"),
            PivotInput::new(empty, "ts", "value", "missing_site"),
        ],
        vec![],
        "ts",
    )?;
    assert_eq!(
        timestamp_counts(&pivot.clone().collect().await?),
        BTreeMap::from([(1, 1), (2, 1)])
    );

    let narrow = pivot.clone().select(vec![col("ts"), col("temperature")])?;
    let plan = narrow.create_physical_plan().await?;
    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert_eq!(display.matches("DataSourceExec").count(), 2, "{display}");
    assert!(
        display.contains(
            "projection=[ts@0 as __watertown_join_accumulated_time, value@1 as temperature]"
        ),
        "{display}"
    );
    assert!(
        display.contains("projection=[ts@0 as __watertown_join_input_time_1]"),
        "{display}"
    );
    assert!(!display.contains("unused"), "{display}");

    let reduced = pivot.aggregate(
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
        .expect("rows must exist")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("COUNT must return Int64")
        .value(0);
    assert_eq!(count, 2);

    Ok(())
}

#[tokio::test]
async fn invalid_pivot_definitions_fail_during_planning() -> Result<()> {
    let fixture = FoundationFixture::new(source_schema());
    let context = fixture.context()?;
    let temperature =
        register_measurement(&fixture, &context, "temperature", vec![1], vec![10.0]).await?;

    let error = pivot_measurements(vec![], vec![], "ts")
        .expect_err("pivot without present inputs must fail");
    assert!(
        error
            .to_string()
            .contains("at least one present measurement"),
        "{error}"
    );

    let error = pivot_measurements(
        vec![PivotInput::new(
            temperature.clone(),
            "ts",
            "value",
            "temperature",
        )],
        vec![MissingPivotColumn::new("temperature", DataType::Float64)],
        "ts",
    )
    .expect_err("duplicate pivot outputs must fail");
    assert!(
        error.to_string().contains("duplicate pivot output"),
        "{error}"
    );

    let error = pivot_measurements(
        vec![PivotInput::new(
            temperature.clone(),
            "ts",
            "missing",
            "temperature",
        )],
        vec![],
        "ts",
    )
    .expect_err("missing value column must fail");
    assert!(
        error.to_string().contains("missing value column"),
        "{error}"
    );

    let error = pivot_measurements(
        vec![PivotInput::new(temperature, "ts", "ts", "temperature")],
        vec![],
        "ts",
    )
    .expect_err("event-time value must fail");
    assert!(
        error.to_string().contains("cannot also be the value"),
        "{error}"
    );

    Ok(())
}
