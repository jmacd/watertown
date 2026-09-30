// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::SessionContext;
use query_foundation::plans::reduce::{
    FixedWindowReduce, TimestampPartialAggregate, TimestampWindowReduce, reduce_fixed_windows,
    reduce_timestamp_windows,
};
use query_foundation::statistics::TimeInterval;
use query_foundation::testkit::FoundationFixture;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("site", DataType::Utf8, false),
        Field::new("sensor", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
        Field::new("unused", DataType::Utf8, false),
    ]))
}

fn batch() -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![-1, 0, 1, 9, 10, 11, 19, 20])),
            Arc::new(StringArray::from(vec![
                "a", "a", "a", "a", "a", "a", "a", "b",
            ])),
            Arc::new(StringArray::from(vec![
                "x", "x", "x", "x", "x", "x", "x", "x",
            ])),
            Arc::new(Float64Array::from(vec![
                Some(1.0),
                None,
                Some(3.0),
                Some(5.0),
                Some(7.0),
                Some(9.0),
                None,
                Some(11.0),
            ])),
            Arc::new(StringArray::from(vec![
                "u-1", "u0", "u1", "u9", "u10", "u11", "u19", "u20",
            ])),
        ],
    )?)
}

async fn fixture_frame() -> Result<(FoundationFixture, datafusion::dataframe::DataFrame)> {
    let fixture = FoundationFixture::new(schema());
    _ = fixture
        .put_parquet("reduce/source.parquet", &[batch()?], 4)
        .await?;
    let snapshot = fixture.timeseries_snapshot(
        "snapshot-0001",
        "ts",
        &[(
            "reduce/source.parquet",
            8,
            Some(TimeInterval::try_new(-1, 20)?),
        )],
    )?;
    let context = fixture.context()?;
    _ = context.register_table("source", snapshot.table_provider()?)?;
    let frame = context.table("source").await?;
    Ok((fixture, frame))
}

type ReducedRow = (i64, String, String, i64, i64, f64, f64, f64);

fn reduced_rows(batches: &[RecordBatch]) -> Vec<ReducedRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let bucket = batch
            .column_by_name("bucket_start")
            .expect("bucket_start must exist")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("bucket_start must be Int64");
        let site = batch
            .column_by_name("site")
            .expect("site must exist")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("site must be Utf8");
        let sensor = batch
            .column_by_name("sensor")
            .expect("sensor must exist")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("sensor must be Utf8");
        let row_count = batch
            .column_by_name("rows")
            .expect("rows must exist")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("rows must be Int64");
        let non_null = batch
            .column_by_name("non_null")
            .expect("non_null must exist")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("non_null must be Int64");
        let sum = batch
            .column_by_name("sum")
            .expect("sum must exist")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("sum must be Float64");
        let min = batch
            .column_by_name("min")
            .expect("min must exist")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("min must be Float64");
        let max = batch
            .column_by_name("max")
            .expect("max must exist")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("max must be Float64");
        for index in 0..batch.num_rows() {
            rows.push((
                bucket.value(index),
                site.value(index).to_owned(),
                sensor.value(index).to_owned(),
                row_count.value(index),
                non_null.value(index),
                sum.value(index),
                min.value(index),
                max.value(index),
            ));
        }
    }
    rows.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
    rows
}

#[tokio::test]
async fn fixed_windows_handle_boundaries_negative_time_groups_and_nulls() -> Result<()> {
    let (_fixture, frame) = fixture_frame().await?;
    let recipe = FixedWindowReduce::try_new("ts", "value", ["site", "sensor"], 10, 0)?;
    let reduced = reduce_fixed_windows(frame, &recipe)?;
    assert_eq!(
        reduced_rows(&reduced.clone().collect().await?),
        vec![
            (-10, "a".to_owned(), "x".to_owned(), 1, 1, 1.0, 1.0, 1.0),
            (0, "a".to_owned(), "x".to_owned(), 3, 2, 8.0, 3.0, 5.0),
            (10, "a".to_owned(), "x".to_owned(), 3, 2, 16.0, 7.0, 9.0,),
            (20, "b".to_owned(), "x".to_owned(), 1, 1, 11.0, 11.0, 11.0,),
        ]
    );

    let plan = reduced.create_physical_plan().await?;
    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(display.contains("AggregateExec"), "{display}");
    assert_eq!(display.matches("DataSourceExec").count(), 1, "{display}");
    assert!(
        display.contains("projection=[ts, site, sensor, value]"),
        "{display}"
    );
    assert!(!display.contains("unused"), "{display}");
    assert!(!display.contains("MemoryExec"), "{display}");

    Ok(())
}

#[tokio::test]
async fn timestamp_windows_emit_multiple_mergeable_partials() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("temperature", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![
                0, 30_000, 60_000, 90_000,
            ])),
            Arc::new(Float64Array::from(vec![
                Some(2.0),
                None,
                Some(4.0),
                Some(8.0),
            ])),
        ],
    )?;
    let context = SessionContext::new();
    let source = Arc::new(MemTable::try_new(schema, vec![vec![batch]])?);
    let recipe = TimestampWindowReduce::try_new(
        "timestamp",
        Duration::from_secs(60),
        vec![
            TimestampPartialAggregate::CountStar {
                output: "rows".to_owned(),
            },
            TimestampPartialAggregate::Count {
                value: "temperature".to_owned(),
                output: "non_null".to_owned(),
            },
            TimestampPartialAggregate::Sum {
                value: "temperature".to_owned(),
                output: "sum".to_owned(),
            },
            TimestampPartialAggregate::Min {
                value: "temperature".to_owned(),
                output: "min".to_owned(),
            },
            TimestampPartialAggregate::Max {
                value: "temperature".to_owned(),
                output: "max".to_owned(),
            },
        ],
    )?;
    let batches = reduce_timestamp_windows(context.read_table(source)?, &recipe)?
        .sort(vec![
            datafusion::logical_expr::col("time_bucket").sort(true, true),
        ])?
        .collect()
        .await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert_eq!(
        arrow::util::pretty::pretty_format_batches(&batches)?.to_string(),
        "+---------------------+------+----------+------+-----+-----+\n\
         | time_bucket         | rows | non_null | sum  | min | max |\n\
         +---------------------+------+----------+------+-----+-----+\n\
         | 1970-01-01T00:00:00 | 2    | 1        | 2.0  | 2.0 | 2.0 |\n\
         | 1970-01-01T00:01:00 | 2    | 2        | 12.0 | 4.0 | 8.0 |\n\
         +---------------------+------+----------+------+-----+-----+"
    );
    Ok(())
}

#[tokio::test]
async fn exact_bounds_reach_the_scan_and_limit_dirty_buckets() -> Result<()> {
    let (_fixture, frame) = fixture_frame().await?;
    let recipe = FixedWindowReduce::try_new("ts", "value", ["site"], 10, 0)?
        .with_bounds(TimeInterval::try_new(0, 10)?);
    let reduced = reduce_fixed_windows(frame, &recipe)?;
    let plan = reduced.clone().create_physical_plan().await?;
    let display = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(display.contains("ts@0 >= 0"), "{display}");
    assert!(display.contains("ts@0 <= 10"), "{display}");
    assert_eq!(
        reduced_rows_without_sensor(&reduced.collect().await?),
        vec![
            (0, "a".to_owned(), 3, 2, 8.0),
            (10, "a".to_owned(), 1, 1, 7.0),
        ]
    );

    Ok(())
}

fn reduced_rows_without_sensor(batches: &[RecordBatch]) -> Vec<(i64, String, i64, i64, f64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let bucket = batch
            .column_by_name("bucket_start")
            .expect("bucket_start must exist")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("bucket_start must be Int64");
        let site = batch
            .column_by_name("site")
            .expect("site must exist")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("site must be Utf8");
        let row_count = batch
            .column_by_name("rows")
            .expect("rows must exist")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("rows must be Int64");
        let non_null = batch
            .column_by_name("non_null")
            .expect("non_null must exist")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("non_null must be Int64");
        let sum = batch
            .column_by_name("sum")
            .expect("sum must exist")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("sum must be Float64");
        for index in 0..batch.num_rows() {
            rows.push((
                bucket.value(index),
                site.value(index).to_owned(),
                row_count.value(index),
                non_null.value(index),
                sum.value(index),
            ));
        }
    }
    rows.sort_by_key(|row| row.0);
    rows
}

#[tokio::test]
async fn fully_pruned_reduction_reads_no_objects() -> Result<()> {
    let (fixture, frame) = fixture_frame().await?;
    let recipe = FixedWindowReduce::try_new("ts", "value", ["site"], 10, 0)?
        .with_bounds(TimeInterval::try_new(100, 110)?);
    fixture.metrics().reset();
    let batches = reduce_fixed_windows(frame, &recipe)?.collect().await?;
    assert!(batches.is_empty());
    assert_eq!(fixture.metrics().snapshot().object_gets, 0);

    Ok(())
}

#[tokio::test]
async fn multiple_cold_resolutions_each_use_one_source_occurrence() -> Result<()> {
    let (_fixture, frame) = fixture_frame().await?;
    for width in [10, 20] {
        let recipe = FixedWindowReduce::try_new("ts", "value", ["site"], width, 0)?;
        let plan = reduce_fixed_windows(frame.clone(), &recipe)?
            .create_physical_plan()
            .await?;
        let display = DisplayableExecutionPlan::new(plan.as_ref())
            .indent(true)
            .to_string();
        assert_eq!(display.matches("DataSourceExec").count(), 1, "{display}");
    }

    Ok(())
}

#[tokio::test]
async fn invalid_reduction_definitions_fail_during_planning() -> Result<()> {
    let (_fixture, frame) = fixture_frame().await?;
    let error = FixedWindowReduce::try_new("ts", "value", ["site"], 0, 0)
        .expect_err("zero-width reduction must fail");
    assert!(error.to_string().contains("must be positive"), "{error}");

    let error = FixedWindowReduce::try_new("ts", "value", ["sum"], 10, 0)
        .expect_err("group and output collision must fail");
    assert!(error.to_string().contains("aggregate output"), "{error}");

    let recipe = FixedWindowReduce::try_new("ts", "unused", ["site"], 10, 0)?;
    let error =
        reduce_fixed_windows(frame.clone(), &recipe).expect_err("non-numeric value must fail");
    assert!(error.to_string().contains("must be Float64"), "{error}");

    let recipe = FixedWindowReduce::try_new("ts", "value", ["missing"], 10, 0)?;
    let error = reduce_fixed_windows(frame, &recipe).expect_err("missing group column must fail");
    assert!(
        error.to_string().contains("missing group column"),
        "{error}"
    );

    Ok(())
}
