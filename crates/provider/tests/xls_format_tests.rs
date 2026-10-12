// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use provider::{FormatRegistry, SchemeKind, SchemeRegistry, Url};
use std::io::Cursor;
use std::sync::Arc;

const SINGLE: &[u8] = include_bytes!("fixtures/single.xls");
const MULTIPLE: &[u8] = include_bytes!("fixtures/multiple.xls");

async fn read(
    bytes: &[u8],
    query: &str,
) -> provider::Result<(
    arrow::datatypes::SchemaRef,
    Vec<arrow::record_batch::RecordBatch>,
)> {
    let url = Url::parse(&format!("xls:///workbook.xls{query}"))?;
    let provider = FormatRegistry::get_provider("xls").expect("XLS is registered");
    let (schema, stream) = provider
        .open_stream(Box::pin(Cursor::new(bytes.to_vec())), &url)
        .await?;
    Ok((schema, stream.try_collect().await?))
}

#[tokio::test]
async fn xls_typed_cells_nulls_mixed_columns_and_batching() -> provider::Result<()> {
    assert_eq!(SchemeRegistry::classify("xls"), Some(SchemeKind::Format));
    let (schema, batches) = read(SINGLE, "?batch_size=2").await?;
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].num_rows(), 2);
    assert_eq!(batches[1].num_rows(), 1);
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        [
            "name",
            "value",
            "active",
            "when",
            "mixed",
            "column_6",
            "late_mixed"
        ]
    );
    assert_eq!(schema.field(1).data_type(), &DataType::Float64);
    assert_eq!(schema.field(2).data_type(), &DataType::Boolean);
    assert_eq!(
        schema.field(3).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(schema.field(4).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(5).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(6).data_type(), &DataType::Utf8);
    let values = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("numeric");
    assert_eq!(values.value(0), 10.0);
    assert_eq!(values.value(1), 20.5);
    let active = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("boolean");
    assert!(active.value(0));
    assert!(!active.value(1));
    let mixed = batches[0]
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("mixed text");
    assert_eq!(mixed.value(0), "1");
    assert_eq!(mixed.value(1), "text");
    assert_eq!(batches[0].column(5).null_count(), 2);
    assert!(batches[1].column(2).is_null(0));
    assert!(batches[1].column(3).is_null(0));
    assert!(batches[1].column(4).is_null(0));
    let inferred = FormatRegistry::get_provider("xls")
        .expect("registered")
        .infer_schema(
            Box::pin(Cursor::new(SINGLE.to_vec())),
            &Url::parse("xls:///workbook.xls")?,
        )
        .await?;
    assert_eq!(schema, inferred);
    Ok(())
}

#[tokio::test]
async fn xls_worksheet_selection_and_headerless_tables() -> provider::Result<()> {
    let error = read(MULTIPLE, "")
        .await
        .expect_err("must select sheet")
        .to_string();
    assert!(error.contains("specify ?sheet=Name"));
    assert!(error.contains("Readings"));
    assert!(
        read(MULTIPLE, "?sheet=Missing")
            .await
            .expect_err("missing sheet")
            .to_string()
            .contains("not found")
    );
    let (_, batches) = read(MULTIPLE, "?sheet=Other%20Sheet").await?;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let (schema, batches) = read(MULTIPLE, "?sheet=NoHeader&header=false").await?;
    assert_eq!(schema.field(0).name(), "column_1");
    assert_eq!(schema.field(1).name(), "column_2");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(batches[0].num_rows(), 2);
    assert!(
        read(MULTIPLE, "?sheet=Duplicate")
            .await
            .expect_err("duplicate headers")
            .to_string()
            .contains("duplicate column name")
    );
    let (_, batches) = read(MULTIPLE, "?sheet=Duplicate&header=false").await?;
    assert_eq!(batches[0].num_rows(), 2);
    Ok(())
}

#[tokio::test]
async fn xls_empty_sheets_errors_and_invalid_options_are_explicit() -> provider::Result<()> {
    assert!(
        read(MULTIPLE, "?sheet=Empty")
            .await
            .expect_err("empty sheet")
            .to_string()
            .contains("is empty")
    );
    let (schema, batches) = read(MULTIPLE, "?sheet=HeaderOnly").await?;
    assert_eq!(schema.fields().len(), 2);
    assert!(batches.is_empty());
    let error = read(MULTIPLE, "?sheet=Errors&batch_size=1")
        .await
        .expect_err("spreadsheet error")
        .to_string();
    assert!(error.contains("row 5, column 2"));
    assert!(error.contains("#DIV/0!"));
    assert!(
        read(MULTIPLE, "?sheet=HeaderError")
            .await
            .expect_err("header error")
            .to_string()
            .contains("#REF!")
    );
    for query in [
        "?batch_size=0",
        "?batch_size=oops",
        "?header=invalid",
        "?unknown=true",
    ] {
        assert!(read(SINGLE, query).await.is_err(), "{query}");
    }
    assert!(
        read(b"not an Excel workbook", "")
            .await
            .expect_err("invalid workbook")
            .to_string()
            .contains("cannot open binary Excel workbook")
    );
    assert!(read(&SINGLE[..100], "").await.is_err());
    Ok(())
}

#[tokio::test]
async fn xls_host_datafusion_query_without_pond() -> provider::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("workbook.xls");
    std::fs::write(&path, SINGLE)?;
    let url = Url::parse(&format!("host+xls://{}?batch_size=2", path.display()))?;
    let table = provider::create_memtable_from_host_url(&url).await?;
    let ctx = SessionContext::new();
    _ = ctx.register_table("source", table)?;
    let batches = ctx.sql(
        "SELECT count(*) AS n, sum(value) AS total FROM source WHERE active = false OR value >= 30"
    ).await?.collect().await?;
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count")
            .value(0),
        2
    );
    assert_eq!(
        batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("sum")
            .value(0),
        50.5
    );
    let batches = ctx
        .sql("SELECT name FROM source WHERE \"when\" = TIMESTAMP '2026-01-01 12:00:00'")
        .await?
        .collect()
        .await?;
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name")
            .value(0),
        "alpha"
    );
    Ok(())
}

#[tokio::test]
async fn xls_pond_datafusion_query() -> provider::Result<()> {
    let fs = Arc::new(tinyfs::FS::new(tinyfs::MemoryPersistence::default()).await?);
    let root = fs.root().await?;
    root.write_file_path_from_slice("/workbook.xls", SINGLE)
        .await?;
    let provider = provider::Provider::new(fs);
    let ctx = SessionContext::new();
    let table = provider
        .create_table_provider("xls:///workbook.xls", &ctx)
        .await?;
    _ = ctx.register_table("source", table)?;
    let batches = ctx
        .sql("SELECT name FROM source WHERE value > 25")
        .await?
        .collect()
        .await?;
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name")
            .value(0),
        "gamma"
    );
    Ok(())
}
