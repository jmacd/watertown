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

#[tokio::test]
async fn declared_text_time_encoding_must_match_the_sql_conversion() -> Result<()> {
    use query_foundation::sql::EventTimeEncoding;
    let context = SessionContext::new();
    _ = context.register_table(
        "source",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(Arc::new(
            Schema::new(vec![Field::new("raw_time", DataType::Utf8, false)]),
        ))),
    )?;
    for (encoding, expression, valid) in [
        (
            EventTimeEncoding::Rfc3339,
            "CAST(raw_time AS TIMESTAMP)",
            true,
        ),
        (
            EventTimeEncoding::Microseconds,
            "to_timestamp_micros(CAST(raw_time AS BIGINT))",
            true,
        ),
        (
            EventTimeEncoding::Microseconds,
            "CAST(raw_time AS TIMESTAMP)",
            false,
        ),
        (
            EventTimeEncoding::Rfc3339,
            "to_timestamp_micros(CAST(raw_time AS BIGINT))",
            false,
        ),
    ] {
        let declaration = UserSqlDeclaration::timestamp_local("source", "timestamp")?
            .with_input_event_time("raw_time")?
            .with_input_encoding(encoding);
        let planned = plan_user_sql(
            &context,
            &format!("SELECT {expression} AS timestamp FROM source"),
            declaration,
        )
        .await;
        assert_eq!(planned.is_ok(), valid, "{encoding:?}: {expression}");
    }
    Ok(())
}

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
    let full = plan_user_sql(
        &context,
        "SELECT ts, value * 2.0 AS doubled FROM source ORDER BY doubled DESC, ts",
        UserSqlDeclaration::global(["source"])?,
    )
    .await?
    .into_frame()
    .collect()
    .await?;
    let expected = full
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("ts")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .filter(|time| (10..=19).contains(time))
        .collect::<Vec<_>>();
    let planned = plan_user_sql(
        &context,
        "SELECT ts, value * 2.0 AS doubled
         FROM source
         WHERE ts >= 10 AND ts <= 19
         ORDER BY doubled DESC, ts",
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
    let bounded = frame.clone().collect().await?;
    let actual = bounded
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("ts")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual, expected,
        "time selection must preserve the full query's ordering"
    );
    assert_eq!(actual, [19, 10]);
    let display = physical_plan(frame).await?;
    assert_eq!(display.matches("DataSourceExec").count(), 1, "{display}");
    assert!(display.contains("projection=[ts, value]"), "{display}");
    assert!(display.contains("ts@0 >= 10"), "{display}");
    assert!(display.contains("ts@0 <= 19"), "{display}");
    assert!(display.contains("SortExec"), "{display}");
    assert!(!display.contains("unused"), "{display}");

    Ok(())
}

#[tokio::test]
async fn timestamp_local_rejects_time_changes_including_hidden_projections() -> Result<()> {
    let (_fixture, context) = fixture().await?;
    for sql in [
        "SELECT ts + 1000 AS ts, value FROM source",
        "SELECT CAST(value AS BIGINT) AS ts FROM source",
        "SELECT 1000 AS ts FROM source",
        "WITH changed AS (SELECT ts + 1000 AS ts, value FROM source)
         SELECT ts, value FROM changed",
        "WITH swapped AS (SELECT ts AS original, CAST(value AS BIGINT) AS ts FROM source)
         SELECT ts FROM swapped",
        "SELECT CAST(ts AS TIMESTAMP) AS ts FROM source",
        "SELECT to_timestamp_seconds(ts) AS ts FROM source",
        "SELECT to_timestamp_micros(ts + 1000) AS ts FROM source",
        "SELECT to_timestamp_micros(CAST(value AS BIGINT)) AS ts FROM source",
    ] {
        let error = plan_user_sql(
            &context,
            sql,
            UserSqlDeclaration::timestamp_local("source", "ts")?,
        )
        .await
        .err()
        .expect("time-changing local SQL must fail");
        assert!(
            error.to_string().contains("must preserve"),
            "{sql}: {error}"
        );
        _ = plan_user_sql(&context, sql, UserSqlDeclaration::global(["source"])?).await?;
    }
    let sql = "WITH renamed AS (SELECT ts AS original, value AS ts FROM source)
               SELECT original AS timestamp, ts AS value FROM renamed";
    assert!(
        plan_user_sql(
            &context,
            sql,
            UserSqlDeclaration::timestamp_local("source", "timestamp")?
        )
        .await
        .is_err(),
        "a renamed input requires an explicit mapping"
    );
    let batches = plan_user_sql(
        &context,
        sql,
        UserSqlDeclaration::timestamp_local("source", "timestamp")?.with_input_event_time("ts")?,
    )
    .await?
    .into_frame()
    .collect()
    .await?;
    let times = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("timestamp")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(times, [0, 10, 19]);
    Ok(())
}

#[tokio::test]
async fn timestamp_local_parses_and_renames_without_losing_precision() -> Result<()> {
    use arrow::array::{
        ArrayRef, StringArray, TimestampMicrosecondArray, TimestampNanosecondArray,
    };
    use arrow::compute::concat_batches;
    use datafusion::datasource::MemTable;

    let batch = RecordBatch::try_from_iter([
        (
            "ts",
            Arc::new(StringArray::from(vec![
                "1969-12-31T23:59:59.999999999Z",
                "1970-01-01T01:00:00.000000100+01:00",
                "1970-01-01T00:00:00.000000200Z",
            ])) as ArrayRef,
        ),
        (
            "__REALTIME_TIMESTAMP",
            Arc::new(StringArray::from(vec!["-1", "1100", "1200"])) as ArrayRef,
        ),
    ])?;
    let context = SessionContext::new();
    _ = context.register_table(
        "source",
        Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?),
    )?;
    for sql in [
        "SELECT CAST(ts AS TIMESTAMP) AS timestamp FROM source ORDER BY timestamp",
        "WITH renamed(raw_time) AS (SELECT ts FROM source),
         typed AS (SELECT CAST(raw_time AS TIMESTAMP) AS parsed FROM renamed)
         SELECT parsed AS timestamp FROM typed ORDER BY timestamp",
    ] {
        let batches = plan_user_sql(
            &context,
            sql,
            UserSqlDeclaration::timestamp_local("source", "timestamp")?
                .with_input_event_time("ts")?,
        )
        .await?
        .into_frame()
        .collect()
        .await?;
        let batch = concat_batches(&batches[0].schema(), &batches)?;
        assert_eq!(
            batch
                .column_by_name("timestamp")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .values()
                .as_ref(),
            &[-1, 100, 200]
        );
    }
    let batches = plan_user_sql(
        &context,
        "SELECT to_timestamp_micros(CAST(\"__REALTIME_TIMESTAMP\" AS BIGINT)) AS timestamp
         FROM source ORDER BY timestamp",
        UserSqlDeclaration::timestamp_local("source", "timestamp")?
            .with_input_event_time("__REALTIME_TIMESTAMP")?,
    )
    .await?
    .into_frame()
    .collect()
    .await?;
    let batch = concat_batches(&batches[0].schema(), &batches)?;
    assert_eq!(
        batch
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .values()
            .as_ref(),
        &[-1, 1100, 1200]
    );
    for sql in [
        "SELECT CAST(ts AS TIMESTAMP(3)) AS timestamp FROM source",
        "WITH typed AS (SELECT CAST(ts AS TIMESTAMP) AS ts FROM source)
         SELECT CAST(ts AS TIMESTAMP(6)) AS timestamp FROM typed",
        "WITH typed AS (SELECT CAST(ts AS TIMESTAMP) AS ts FROM source)
         SELECT to_timestamp_micros(ts) AS timestamp FROM typed",
        "SELECT CAST(substr(ts, 1, 19) AS TIMESTAMP) AS timestamp FROM source",
        "SELECT to_timestamp_micros(CAST(CAST(ts AS DOUBLE) AS BIGINT)) AS timestamp FROM source",
    ] {
        assert!(
            plan_user_sql(
                &context,
                sql,
                UserSqlDeclaration::timestamp_local("source", "timestamp")?
                    .with_input_event_time("ts")?,
            )
            .await
            .is_err(),
            "lossy conversion accepted: {sql}"
        );
    }
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
async fn exact_timestamp_groups_match_full_results_under_time_restriction() -> Result<()> {
    use arrow::array::{BooleanArray, StringArray};
    use arrow::compute::{concat_batches, filter_record_batch};
    use datafusion::catalog::view::ViewTable;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::{col, lit};

    let grouped_schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("value", DataType::Float64, true),
        Field::new("unused", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        grouped_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![0, 10, 10, 19, 19])),
            Arc::new(Float64Array::from(vec![
                Some(1.0),
                Some(2.0),
                Some(5.0),
                None,
                Some(6.0),
            ])),
            Arc::new(StringArray::from(vec!["ops", "ops", "ops", "ops", "bytes"])),
        ],
    )?;
    let context = SessionContext::new();
    _ = context.register_table(
        "source",
        Arc::new(MemTable::try_new(grouped_schema, vec![vec![batch]])?),
    )?;
    let query = "WITH selected AS (SELECT ts, value, unused FROM source)
        SELECT ts, MAX(CASE WHEN unused = 'ops' THEN value END) AS ops,
               SUM(value) AS total, COUNT(*) AS samples
        FROM selected GROUP BY ts ORDER BY ts";
    let full = plan_user_sql(&context, query, UserSqlDeclaration::global(["source"])?)
        .await?
        .into_frame()
        .collect()
        .await?;
    let full = concat_batches(&full[0].schema(), &full)?;
    let times = full
        .column_by_name("ts")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let expected = filter_record_batch(
        &full,
        &BooleanArray::from(
            times
                .values()
                .iter()
                .map(|time| *time >= 10)
                .collect::<Vec<_>>(),
        ),
    )?;
    let bounded = context
        .table("source")
        .await?
        .filter(col("ts").gt_eq(lit(10_i64)))?;
    _ = context.register_table(
        "bounded",
        Arc::new(ViewTable::new(bounded.logical_plan().clone(), None)),
    )?;
    let planned = plan_user_sql(
        &context,
        &query.replace("FROM source", "FROM bounded"),
        UserSqlDeclaration::timestamp_grouped("bounded", "ts")?,
    )
    .await?;
    assert_eq!(planned.locality().class(), LocalityClass::TimestampLocal);
    assert_eq!(
        planned
            .locality()
            .required_input_ranges(TimeInterval::try_new(10, 19)?)?
            .get("bounded"),
        Some(&RangeRequirement::Bounded(vec![TimeInterval::try_new(
            10, 19
        )?]))
    );
    let actual = planned.into_frame().collect().await?;
    let actual = concat_batches(&actual[0].schema(), &actual)?;
    assert_eq!(
        actual, expected,
        "all members of each retained timestamp group must contribute"
    );
    let counts = actual
        .column_by_name("samples")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.values().as_ref(), &[2, 2]);
    Ok(())
}

#[tokio::test]
async fn timestamp_grouped_rejects_nonlocal_and_time_changing_plans() -> Result<()> {
    let (_fixture, context) = fixture().await?;
    for sql in [
        "SELECT MAX(ts) AS ts, SUM(value) FROM source",
        "SELECT unused, MAX(ts) AS ts, SUM(value) FROM source GROUP BY unused",
        "SELECT ts / 10 AS ts, SUM(value) FROM source GROUP BY ts / 10",
        "WITH bucketed AS (SELECT ts / 10 AS ts, value FROM source)
         SELECT ts, SUM(value) FROM bucketed GROUP BY ts",
        "SELECT SUM(value) AS ts FROM source GROUP BY ts",
        "SELECT ts + 1 AS ts, SUM(value) FROM source GROUP BY ts",
        "SELECT ts, SUM(value) FROM source GROUP BY ts, unused",
        "SELECT ts, SUM(value) FROM source GROUP BY ROLLUP(ts)",
        "SELECT ts, SUM(value) FROM source GROUP BY ts ORDER BY ts LIMIT 1",
        "SELECT ts, SUM(value) FROM source GROUP BY ts ORDER BY ts OFFSET 1",
        "SELECT ts, SUM(random()) FROM source GROUP BY ts",
        "SELECT ts, MAX(value) FILTER (WHERE random() > 0.5) FROM source GROUP BY ts",
        "SELECT ts, ARRAY_AGG(value ORDER BY random()) FROM source GROUP BY ts",
        "SELECT ts, SUM((SELECT MAX(value) FROM source)) FROM source GROUP BY ts",
        "SELECT ts, SUM(value) FROM source GROUP BY ts ORDER BY random()",
        "SELECT ts, SUM(value) OVER () FROM source",
        "SELECT ts, value FROM source",
    ] {
        assert!(
            plan_user_sql(
                &context,
                sql,
                UserSqlDeclaration::timestamp_grouped("source", "ts")?
            )
            .await
            .is_err(),
            "unsafe or ungrouped declaration was accepted: {sql}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn timestamp_grouped_requires_order_for_order_sensitive_aggregates() -> Result<()> {
    let (_fixture, context) = fixture().await?;
    let declaration = || UserSqlDeclaration::timestamp_grouped("source", "ts");
    for (sql, expected) in [
        (
            "SELECT ts, FIRST_VALUE(value) AS v FROM source GROUP BY ts",
            "'first_value' depends on input order",
        ),
        (
            "SELECT ts, LAST_VALUE(value) AS v FROM source GROUP BY ts",
            "'last_value' depends on input order",
        ),
        (
            "SELECT ts, ARRAY_AGG(value) AS v FROM source GROUP BY ts",
            "'array_agg' depends on input order",
        ),
        (
            "SELECT ts, STRING_AGG(unused, ',') AS v FROM source GROUP BY ts",
            "'string_agg' depends on input order",
        ),
        (
            "SELECT ts, ANY_VALUE(value) AS v FROM source GROUP BY ts",
            "'any_value' can differ between reads",
        ),
        (
            "SELECT ts, APPROX_MEDIAN(value) AS v FROM source GROUP BY ts",
            "'approx_median' can differ between reads",
        ),
        (
            "SELECT ts, APPROX_PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) AS v
             FROM source GROUP BY ts",
            "'approx_percentile_cont' can differ between reads",
        ),
    ] {
        let error = plan_user_sql(&context, sql, declaration()?)
            .await
            .err()
            .unwrap_or_else(|| panic!("order-sensitive aggregate was accepted: {sql}"));
        assert!(error.to_string().contains(expected), "{sql}: {error}");
    }
    for sql in [
        "SELECT ts, FIRST_VALUE(value ORDER BY unused) AS v FROM source GROUP BY ts",
        "SELECT ts, ARRAY_AGG(value ORDER BY value) AS v FROM source GROUP BY ts",
        "SELECT ts, MAX(CASE WHEN unused = 'ops' THEN value END) AS ops,
                SUM(value) AS total, COUNT(*) AS samples, MIN(value) AS low,
                AVG(value) AS mean
         FROM source GROUP BY ts",
    ] {
        _ = plan_user_sql(&context, sql, declaration()?).await?;
    }
    Ok(())
}

#[tokio::test]
async fn timestamp_grouped_parses_text_without_bucketing_instants() -> Result<()> {
    use arrow::array::{ArrayRef, StringArray, TimestampNanosecondArray};
    use arrow::compute::concat_batches;
    use datafusion::datasource::MemTable;

    let batch = RecordBatch::try_from_iter([
        (
            "ts",
            Arc::new(StringArray::from(vec![
                "1970-01-01T00:00:00.000000100Z",
                "1970-01-01T01:00:00.000000100+01:00",
                "1970-01-01T00:00:00.000000200Z",
            ])) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Float64Array::from(vec![1.0, 2.0, 4.0])) as ArrayRef,
        ),
    ])?;
    let context = SessionContext::new();
    _ = context.register_table(
        "source",
        Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?),
    )?;
    for event_time in ["ts", "timestamp"] {
        let query = format!(
            "WITH typed AS (SELECT CAST(ts AS TIMESTAMP) AS parsed, value FROM source)
             SELECT parsed AS {event_time}, SUM(value) AS total
             FROM typed GROUP BY parsed ORDER BY {event_time}"
        );
        let batches = plan_user_sql(
            &context,
            &query,
            UserSqlDeclaration::timestamp_grouped("source", event_time)?
                .with_input_event_time("ts")?,
        )
        .await?
        .into_frame()
        .collect()
        .await?;
        let batch = concat_batches(&batches[0].schema(), &batches)?;
        assert_eq!(
            batch
                .column_by_name(event_time)
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .values()
                .as_ref(),
            &[100, 200]
        );
        assert_eq!(
            batch
                .column_by_name("total")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[3.0, 4.0]
        );
    }
    for sql in [
        "SELECT CAST(ts AS TIMESTAMP) AS ts, SUM(value) FROM source GROUP BY ts",
        "WITH typed AS (SELECT CAST(ts AS TIMESTAMP(3)) AS ts, value FROM source)
         SELECT ts, SUM(value) FROM typed GROUP BY ts",
        "WITH typed AS (SELECT CAST(ts AS TIMESTAMP) AS ts, value FROM source),
         bucketed AS (SELECT CAST(ts AS TIMESTAMP(3)) AS ts, value FROM typed)
         SELECT ts, SUM(value) FROM bucketed GROUP BY ts",
    ] {
        assert!(
            plan_user_sql(
                &context,
                sql,
                UserSqlDeclaration::timestamp_grouped("source", "ts")?
            )
            .await
            .is_err(),
            "precision-reducing time conversion was accepted: {sql}"
        );
    }
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
        ("SELECT ts, value FROM source ORDER BY ts LIMIT 1", "Limit"),
        ("SELECT ts, value FROM source ORDER BY ts OFFSET 1", "Limit"),
        ("SELECT ts, value FROM source LIMIT 1", "Limit"),
        (
            "SELECT ts, value FROM source ORDER BY random()",
            "not immutable",
        ),
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
