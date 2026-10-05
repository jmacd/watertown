// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::Column;
use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use datafusion::logical_expr::{JoinType, col};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::SessionContext;
use query_foundation::overlap::OverlapPolicy;
use query_foundation::plans::combine::{CombineInput, combine_same_scope};
use query_foundation::plans::join::{TimestampJoinInput, accumulated_full_outer_join};
use query_foundation::plans::pivot::{PivotInput, pivot_measurements};
use query_foundation::plans::reduce::{FixedWindowReduce, reduce_fixed_windows};
use query_foundation::plans::transform::{rename_column, scope_prefix};
use query_foundation::snapshot::{ChunkDescriptor, DatasetSnapshot, EventTimeContract};
use query_foundation::statistics::TimeInterval;
use query_foundation::testkit::FoundationFixture;

fn series_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("site", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
        Field::new("unused", DataType::Utf8, false),
    ]))
}

async fn register_series(
    fixture: &FoundationFixture,
    context: &SessionContext,
    name: &str,
    timestamps: Vec<i64>,
    sites: Vec<&str>,
    values: Vec<Option<f64>>,
) -> Result<DataFrame> {
    let schema = series_schema();
    let unused = timestamps
        .iter()
        .map(|timestamp| format!("unused-{timestamp}"))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(timestamps.clone())),
            Arc::new(StringArray::from(sites)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(unused)),
        ],
    )?;
    let path = format!("composition/{name}.parquet");
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

async fn physical_plan(frame: DataFrame) -> Result<String> {
    let plan = frame.create_physical_plan().await?;
    Ok(DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string())
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

#[tokio::test]
async fn transforms_compose_over_physical_and_derived_sources() -> Result<()> {
    let fixture = FoundationFixture::new(series_schema());
    let context = fixture.context()?;
    let source = register_series(
        &fixture,
        &context,
        "transform_source",
        vec![1, 2],
        vec!["a", "a"],
        vec![Some(10.0), Some(20.0)],
    )
    .await?;

    let renamed = rename_column(source, "value", "reading")?;
    let scoped = scope_prefix(renamed, "pond", "ts")?;
    let result = scoped.select(vec![col("ts"), col(Column::from_name("pond.reading"))])?;
    assert_eq!(row_count(&result.clone().collect().await?), 2);

    let display = physical_plan(result).await?;
    assert_eq!(display.matches("DataSourceExec").count(), 1, "{display}");
    assert!(display.contains("projection=[ts, value]"), "{display}");
    assert!(!display.contains("unused"), "{display}");
    assert!(!display.contains("site"), "{display}");

    Ok(())
}

#[tokio::test]
async fn same_scope_combine_composes_before_cross_scope_join() -> Result<()> {
    let fixture = FoundationFixture::new(series_schema());
    let context = fixture.context()?;
    let archive = register_series(
        &fixture,
        &context,
        "archive",
        vec![1],
        vec!["a"],
        vec![Some(10.0)],
    )
    .await?;
    let live = register_series(
        &fixture,
        &context,
        "live",
        vec![2],
        vec!["a"],
        vec![Some(20.0)],
    )
    .await?;
    let reference = register_series(
        &fixture,
        &context,
        "reference",
        vec![1, 2],
        vec!["a", "a"],
        vec![Some(100.0), Some(200.0)],
    )
    .await?;

    let combined = combine_same_scope(
        vec![
            CombineInput::new("archive", 1, archive),
            CombineInput::new("live", 2, live),
        ],
        &OverlapPolicy::PreserveAll,
    )
    .await?;
    let combined = scope_prefix(combined, "observed", "ts")?;
    let reference = scope_prefix(reference, "reference", "ts")?;
    let joined = accumulated_full_outer_join(
        vec![
            TimestampJoinInput::new(combined, "ts"),
            TimestampJoinInput::new(reference, "ts"),
        ],
        "ts",
    )?
    .select(vec![
        col("ts"),
        col(Column::from_name("observed.value")),
        col(Column::from_name("reference.value")),
    ])?;
    assert_eq!(row_count(&joined.clone().collect().await?), 2);

    let display = physical_plan(joined).await?;
    assert_eq!(display.matches("DataSourceExec").count(), 3, "{display}");
    assert_eq!(display.matches("projection=[ts, value]").count(), 3);
    assert_eq!(display.matches("HashJoinExec").count(), 1, "{display}");
    assert!(!display.contains("unused"), "{display}");
    assert!(!display.contains("site"), "{display}");

    Ok(())
}

#[tokio::test]
async fn join_pivot_and_reduce_remain_one_composed_plan() -> Result<()> {
    let fixture = FoundationFixture::new(series_schema());
    let context = fixture.context()?;
    let temperature = scope_prefix(
        register_series(
            &fixture,
            &context,
            "temperature",
            vec![1, 11],
            vec!["a", "a"],
            vec![Some(10.0), Some(20.0)],
        )
        .await?,
        "temperature",
        "ts",
    )?;
    let pressure = scope_prefix(
        register_series(
            &fixture,
            &context,
            "pressure",
            vec![1, 11],
            vec!["a", "a"],
            vec![Some(1.0), Some(2.0)],
        )
        .await?,
        "pressure",
        "ts",
    )?;
    let joined = accumulated_full_outer_join(
        vec![
            TimestampJoinInput::new(temperature, "ts"),
            TimestampJoinInput::new(pressure, "ts"),
        ],
        "ts",
    )?;
    let pivoted = pivot_measurements(
        vec![PivotInput::new(
            joined,
            "ts",
            "temperature.value",
            "temperature",
        )],
        vec![],
        "ts",
    )?;
    let reduced = reduce_fixed_windows(
        pivoted,
        &FixedWindowReduce::try_new("ts", "temperature", Vec::<&str>::new(), 10, 0)?,
    )?;
    assert_eq!(row_count(&reduced.clone().collect().await?), 2);

    let display = physical_plan(reduced).await?;
    assert_eq!(display.matches("DataSourceExec").count(), 2, "{display}");
    assert_eq!(display.matches("projection=[ts, value]").count(), 1);
    assert_eq!(display.matches("projection=[ts]").count(), 1);
    assert_eq!(display.matches("HashJoinExec").count(), 1, "{display}");
    assert!(display.contains("AggregateExec"), "{display}");
    assert!(!display.contains("unused"), "{display}");
    assert!(!display.contains("site"), "{display}");

    Ok(())
}

#[tokio::test]
async fn timeseries_join_with_dimension_prunes_both_leaves() -> Result<()> {
    let fixture = FoundationFixture::new(series_schema());
    let context = fixture.context()?;
    let series = register_series(
        &fixture,
        &context,
        "series",
        vec![1, 2],
        vec!["a", "b"],
        vec![Some(10.0), Some(20.0)],
    )
    .await?;

    let dimension_schema = Arc::new(Schema::new(vec![
        Field::new("site", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("dimension_unused", DataType::Utf8, false),
    ]));
    let dimension_batch = RecordBatch::try_new(
        Arc::clone(&dimension_schema),
        vec![
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(StringArray::from(vec!["north", "south"])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ],
    )?;
    let dimension_path = "composition/sites.parquet";
    _ = fixture
        .put_parquet(dimension_path, &[dimension_batch], 4)
        .await?;
    let dimension_snapshot = DatasetSnapshot::try_new(
        "sites-snapshot",
        Arc::clone(&dimension_schema),
        vec![ChunkDescriptor::new(
            "sites-chunk",
            1,
            fixture.object_descriptor(dimension_path)?,
            dimension_schema,
            2,
            None,
        )],
        None,
        OverlapPolicy::PreserveAll,
    )?;
    _ = context.register_table("sites", dimension_snapshot.table_provider()?)?;
    let dimension = context.table("sites").await?;

    let joined = series
        .join(dimension, JoinType::Inner, &["site"], &["site"], None)?
        .select(vec![col("ts"), col("region")])?;
    assert_eq!(row_count(&joined.clone().collect().await?), 2);

    let display = physical_plan(joined).await?;
    assert_eq!(display.matches("DataSourceExec").count(), 2, "{display}");
    assert!(display.contains("projection=[ts, site]"), "{display}");
    assert!(display.contains("projection=[site, region]"), "{display}");
    assert!(!display.contains("unused"), "{display}");

    Ok(())
}
