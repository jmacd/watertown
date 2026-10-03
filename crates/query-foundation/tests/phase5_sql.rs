// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::SessionContext;
use query_foundation::locality::{LocalityClass, RangeRequirement};
use query_foundation::sql::{UserSqlDeclaration, plan_user_sql};
use query_foundation::statistics::TimeInterval;
use query_foundation::testkit::FoundationFixture;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("unused", DataType::Utf8, false),
    ]))
}

async fn fixture() -> Result<(FoundationFixture, SessionContext)> {
    let fixture = FoundationFixture::new(schema());
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![0, 10, 19])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])),
        ],
    )?;
    _ = fixture
        .put_parquet("sql/source.parquet", &[batch], 2)
        .await?;
    let snapshot = fixture.timeseries_snapshot(
        "sql-snapshot",
        "ts",
        &[("sql/source.parquet", 3, Some(TimeInterval::try_new(0, 19)?))],
    )?;
    let context = fixture.context()?;
    _ = context.register_table("source", snapshot.table_provider()?)?;
    Ok((fixture, context))
}

async fn physical_plan(frame: datafusion::dataframe::DataFrame) -> Result<String> {
    let plan = frame.create_physical_plan().await?;
    Ok(DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string())
}

#[tokio::test]
async fn unrestricted_window_and_cumulative_sql_remain_explicitly_global() -> Result<()> {
    let (_fixture, context) = fixture().await?;
    let planned = plan_user_sql(
        &context,
        "SELECT ts, value,
                LAG(value, 1) OVER (ORDER BY ts) AS prior_value,
                SUM(value) OVER (
                    ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_value
         FROM source
         ORDER BY ts",
        UserSqlDeclaration::global(["source"])?,
    )
    .await?;
    assert_eq!(planned.locality().class(), LocalityClass::Global);
    assert_eq!(
        planned
            .locality()
            .required_input_ranges(TimeInterval::try_new(10, 10)?)?
            .get("source"),
        Some(&RangeRequirement::Complete)
    );

    let frame = planned.into_frame();
    let batches = frame.clone().collect().await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    let cumulative = batches[0]
        .column_by_name("cumulative_value")
        .expect("cumulative output must exist")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("SUM must return Float64");
    assert_eq!(
        (0..cumulative.len())
            .map(|index| cumulative.value(index))
            .collect::<Vec<_>>(),
        vec![1.0, 3.0, 6.0]
    );

    let display = physical_plan(frame).await?;
    assert_eq!(display.matches("DataSourceExec").count(), 1, "{display}");
    assert!(display.contains("WindowAggExec"), "{display}");
    assert!(display.contains("SortExec"), "{display}");
    assert!(display.contains("projection=[ts, value]"), "{display}");
    assert!(!display.contains("unused"), "{display}");

    Ok(())
}

#[tokio::test]
async fn declared_timestamp_local_sql_proves_operator_and_range_contracts() -> Result<()> {
    let (_fixture, context) = fixture().await?;
    let planned = plan_user_sql(
        &context,
        "SELECT ts, value * 2.0 AS doubled
         FROM source
         WHERE ts >= 10 AND ts <= 19",
        UserSqlDeclaration::timestamp_local("source", "ts")?,
    )
    .await?;
    assert_eq!(planned.locality().class(), LocalityClass::TimestampLocal);
    assert_eq!(
        planned
            .locality()
            .required_input_ranges(TimeInterval::try_new(10, 19)?)?
            .get("source"),
        Some(&RangeRequirement::Bounded(vec![TimeInterval::try_new(
            10, 19
        )?]))
    );

    let frame = planned.into_frame();
    assert_eq!(
        frame
            .clone()
            .collect()
            .await?
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );
    let display = physical_plan(frame).await?;
    assert_eq!(display.matches("DataSourceExec").count(), 1, "{display}");
    assert!(display.contains("projection=[ts, value]"), "{display}");
    assert!(display.contains("ts@0 >= 10"), "{display}");
    assert!(display.contains("ts@0 <= 19"), "{display}");
    assert!(!display.contains("unused"), "{display}");

    Ok(())
}

#[tokio::test]
async fn global_sql_preserves_empty_and_fully_pruned_execution() -> Result<()> {
    let (fixture, context) = fixture().await?;
    fixture.metrics().reset();
    let frame = plan_user_sql(
        &context,
        "SELECT ts, value FROM source WHERE ts > 100",
        UserSqlDeclaration::global(["source"])?,
    )
    .await?
    .into_frame();
    let batches = frame.collect().await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    let work = fixture.metrics().snapshot();
    assert_eq!(work.object_gets, 0, "{work:?}");
    assert!(work.opened_objects.is_empty(), "{work:?}");

    Ok(())
}

#[tokio::test]
async fn local_declarations_reject_unproven_or_incomplete_plans() -> Result<()> {
    let (_fixture, context) = fixture().await?;
    for (sql, expected) in [
        ("SELECT ts, SUM(value) FROM source GROUP BY ts", "Aggregate"),
        (
            "SELECT ts, LAG(value) OVER (ORDER BY ts) FROM source",
            "Window",
        ),
        ("SELECT ts, value FROM source ORDER BY ts", "Sort"),
        ("SELECT ts, value FROM source LIMIT 1", "Limit"),
        (
            "SELECT left_source.ts
             FROM source left_source
             JOIN source right_source
               ON left_source.ts = right_source.ts",
            "Join",
        ),
    ] {
        let error = plan_user_sql(
            &context,
            sql,
            UserSqlDeclaration::timestamp_local("source", "ts")?,
        )
        .await
        .err()
        .expect("unsupported local SQL must fail");
        assert!(error.to_string().contains(expected), "{error}");
    }

    let error = plan_user_sql(
        &context,
        "SELECT value FROM source",
        UserSqlDeclaration::timestamp_local("source", "ts")?,
    )
    .await
    .err()
    .expect("local SQL without event time must fail");
    assert!(
        error.to_string().contains("must retain event-time"),
        "{error}"
    );

    let error = plan_user_sql(
        &context,
        "SELECT ts, random() AS sample FROM source",
        UserSqlDeclaration::timestamp_local("source", "ts")?,
    )
    .await
    .err()
    .expect("non-immutable local SQL function must fail");
    assert!(error.to_string().contains("not immutable"), "{error}");

    let error = plan_user_sql(
        &context,
        "SELECT ts FROM source",
        UserSqlDeclaration::global(["other"])?,
    )
    .await
    .err()
    .expect("undeclared SQL source must fail");
    assert!(error.to_string().contains("does not match"), "{error}");

    let error = plan_user_sql(
        &context,
        "CREATE TABLE forbidden AS SELECT * FROM source",
        UserSqlDeclaration::global(["source"])?,
    )
    .await
    .err()
    .expect("DDL must fail");
    assert!(error.to_string().contains("DDL not supported"), "{error}");

    let error = plan_user_sql(&context, " ", UserSqlDeclaration::global(["source"])?)
        .await
        .err()
        .expect("empty SQL must fail");
    assert!(error.to_string().contains("must not be empty"), "{error}");

    Ok(())
}
