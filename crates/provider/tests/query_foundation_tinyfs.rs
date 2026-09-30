// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use futures::stream;
use provider::query_foundation_adapter::{TinyFsMaterializationSink, capture_tinyfs_snapshot};
use query_foundation::frontier::{RepairPolicy, SettledState};
use query_foundation::materialize::{
    MaterializationProgress, MaterializationPublication, TransactionalMaterializationSink,
    materialize_stream,
};
use query_foundation::overlap::OverlapPolicy;
use query_foundation::snapshot::EventTimeContract;
use tinyfs::arrow::ParquetExt;
use tinyfs::arrow::parquet::StreamingSeriesWriter;
use tinyfs::{FS, MemoryPersistence, ProviderContext};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

fn batch(timestamps: Vec<i64>, values: Vec<f64>) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Float64Array::from(values)),
        ],
    )?)
}

fn record_stream(batches: Vec<Result<RecordBatch>>) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        schema(),
        stream::iter(batches),
    ))
}

fn progress(
    source_state_id: &str,
    observed_through: Option<i64>,
) -> Result<MaterializationProgress> {
    MaterializationProgress::try_new(
        "recipe-0001",
        source_state_id,
        SettledState::try_new(observed_through, observed_through, RepairPolicy::reject())?,
    )
}

async fn row_count(context: &SessionContext, table: &str) -> Result<i64> {
    let batches = context
        .sql(&format!("SELECT COUNT(*) AS rows FROM {table}"))
        .await?
        .collect()
        .await?;
    Ok(batches[0]
        .column_by_name("rows")
        .expect("count output must exist")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("COUNT must return Int64")
        .value(0))
}

#[tokio::test]
async fn captures_exact_versions_and_rejects_stale_generation_execution() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let filesystem = FS::new(persistence.clone())
        .await
        .expect("memory filesystem");
    let session = Arc::new(SessionContext::new());
    _ = provider::register_tinyfs_object_store(&session, persistence.clone())
        .expect("register TinyFS object store");
    let context = ProviderContext::new(session.clone(), Arc::new(persistence));
    let root = filesystem.root().await.expect("memory root");
    _ = root
        .create_series_from_batch(
            "/events.series",
            &batch(vec![1, 2], vec![10.0, 20.0])?,
            Some("ts"),
        )
        .await
        .expect("first series version");
    let file_id = root
        .get_node_path("/events.series")
        .await
        .expect("series node")
        .id();

    let first = capture_tinyfs_snapshot(
        &context,
        file_id,
        "snapshot-0001",
        schema(),
        Some(EventTimeContract::new("ts")),
        OverlapPolicy::PreserveAll,
    )
    .await?;
    assert_eq!(first.snapshot().chunks().len(), 1);
    assert_eq!(first.snapshot().chunks()[0].logical_count(), 2);
    assert_eq!(
        first.snapshot().chunks()[0]
            .event_time_bounds()
            .expect("footer statistics"),
        query_foundation::statistics::TimeInterval::try_new(1, 2)?
    );
    assert!(
        first.snapshot().chunks()[0]
            .object()
            .url()
            .to_string()
            .ends_with("/version/1.parquet")
    );
    _ = session.register_table("captured_before", first.table_provider()?)?;
    assert_eq!(row_count(&session, "captured_before").await?, 2);

    _ = root
        .write_series_from_batch("/events.series", &batch(vec![3], vec![30.0])?, Some("ts"))
        .await
        .expect("second series version");
    let stale_error = row_count(&session, "captured_before")
        .await
        .expect_err("provider from earlier persistence generation must fail");
    assert!(
        stale_error.to_string().contains("provider is stale"),
        "{stale_error}"
    );

    let second = capture_tinyfs_snapshot(
        &context,
        file_id,
        "snapshot-0002",
        schema(),
        Some(EventTimeContract::new("ts")),
        OverlapPolicy::PreserveAll,
    )
    .await?;
    assert_eq!(second.snapshot().chunks().len(), 2);
    assert_eq!(
        second
            .snapshot()
            .chunks()
            .iter()
            .map(|chunk| chunk.logical_count())
            .sum::<u64>(),
        3
    );
    _ = session.register_table("captured_after", second.table_provider()?)?;
    assert_eq!(row_count(&session, "captured_after").await?, 3);

    let frame = session
        .sql("SELECT ts FROM captured_after WHERE ts >= 3")
        .await?;
    let physical = frame.create_physical_plan().await?;
    let display = DisplayableExecutionPlan::new(physical.as_ref())
        .indent(true)
        .to_string();
    assert_eq!(display.matches("DataSourceExec").count(), 1, "{display}");
    assert!(display.contains("projection=[ts]"), "{display}");
    assert!(display.contains("ts@0 >= 3"), "{display}");
    assert!(!display.contains("value"), "{display}");

    Ok(())
}

#[tokio::test]
async fn appends_output_and_progress_in_one_tinyfs_version() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let filesystem = FS::new(persistence.clone())
        .await
        .expect("memory filesystem");
    let context = ProviderContext::new(Arc::new(SessionContext::new()), Arc::new(persistence));
    let root = filesystem.root().await.expect("memory root");
    let sink = TinyFsMaterializationSink::new(root.clone(), &context, "ts");

    let outcome = materialize_stream(
        &sink,
        "/materialized.series",
        "ts",
        progress("source-0001", Some(3))?,
        MaterializationPublication::Append { after: None },
        record_stream(vec![
            batch(vec![1, 2], vec![10.0, 20.0]),
            batch(vec![3], vec![30.0]),
        ]),
    )
    .await?;
    assert_eq!(outcome.metrics.rows_written, 3);
    assert!(matches!(
        outcome.publication,
        MaterializationPublication::Append { after: None }
    ));

    let versions = root
        .list_file_versions("/materialized.series")
        .await
        .expect("materialized versions");
    assert_eq!(versions.len(), 1);
    let attributes = versions[0]
        .extended_metadata
        .as_ref()
        .and_then(|metadata| metadata.get("extended_attributes"))
        .expect("materialization progress attributes");
    let attributes: serde_json::Value =
        serde_json::from_str(attributes).expect("valid progress JSON");
    assert_eq!(
        attributes["watertown.materialization.recipe_id"],
        "recipe-0001"
    );
    assert_eq!(
        attributes["watertown.materialization.source_state_id"],
        "source-0001"
    );
    assert_eq!(attributes["watertown.materialization.observed_through"], 3);
    assert_eq!(attributes["watertown.timestamp_column"], "ts");
    assert_eq!(
        root.read_table_as_batch("/materialized.series")
            .await
            .expect("published output")
            .num_rows(),
        3
    );
    Ok(())
}

#[tokio::test]
async fn no_rows_publish_one_progress_record_without_output() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let filesystem = FS::new(persistence.clone())
        .await
        .expect("memory filesystem");
    let context = ProviderContext::new(Arc::new(SessionContext::new()), Arc::new(persistence));
    let root = filesystem.root().await.expect("memory root");
    let sink = TinyFsMaterializationSink::new(root.clone(), &context, "ts");

    let outcome = materialize_stream(
        &sink,
        "/empty-materialized.series",
        "ts",
        progress("source-0002", Some(10))?,
        MaterializationPublication::Append { after: Some(3) },
        record_stream(Vec::new()),
    )
    .await?;
    assert!(outcome.output.is_none());
    assert!(matches!(
        outcome.publication,
        MaterializationPublication::NoOutput
    ));
    assert!(
        !root
            .exists(std::path::Path::new("/empty-materialized.series"))
            .await
    );
    let progress = root
        .read_file_path_to_vec("/empty-materialized.series.materialization-progress")
        .await
        .expect("progress record");
    let progress: serde_json::Value =
        serde_json::from_slice(&progress).expect("valid progress JSON");
    assert_eq!(
        progress["watertown.materialization.source_state_id"],
        "source-0002"
    );
    assert_eq!(
        root.list_file_versions("/empty-materialized.series.materialization-progress")
            .await
            .expect("progress versions")
            .len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn rejects_repair_before_opening_a_tinyfs_writer() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let filesystem = FS::new(persistence.clone())
        .await
        .expect("memory filesystem");
    let context = ProviderContext::new(Arc::new(SessionContext::new()), Arc::new(persistence));
    let root = filesystem.root().await.expect("memory root");
    let sink = TinyFsMaterializationSink::new(root.clone(), &context, "ts");
    let error = match sink
        .begin(
            "/unsupported-repair.series",
            &MaterializationPublication::Replace {
                ranges: vec![query_foundation::statistics::TimeInterval::try_new(1, 2)?],
                reason:
                    query_foundation::materialize::MaterializationReplaceReason::RetroactiveRepair,
            },
        )
        .await
    {
        Ok(_) => panic!("append-only TinyFS must reject repair"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("append-only"), "{error}");
    assert!(
        !root
            .exists(std::path::Path::new("/unsupported-repair.series"))
            .await
    );
    Ok(())
}

#[tokio::test]
async fn stream_failure_and_stale_context_publish_no_version() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let filesystem = FS::new(persistence.clone())
        .await
        .expect("memory filesystem");
    let context = ProviderContext::new(Arc::new(SessionContext::new()), Arc::new(persistence));
    let root = filesystem.root().await.expect("memory root");
    let sink = TinyFsMaterializationSink::new(root.clone(), &context, "ts");

    let stream_error = materialize_stream(
        &sink,
        "/failed-materialized.series",
        "ts",
        progress("source-failed", Some(2))?,
        MaterializationPublication::Append { after: None },
        record_stream(vec![
            batch(vec![1], vec![10.0]),
            Err(DataFusionError::Execution(
                "injected materialization stream failure".to_owned(),
            )),
        ]),
    )
    .await
    .expect_err("stream failure must surface");
    assert!(
        stream_error
            .to_string()
            .contains("injected materialization stream failure"),
        "{stream_error}"
    );
    assert!(
        root.list_file_versions("/failed-materialized.series")
            .await
            .expect("failed output versions")
            .is_empty()
    );

    let mut writer = sink
        .begin(
            "/stale-materialized.series",
            &MaterializationPublication::Append { after: None },
        )
        .await?;
    _ = root
        .create_series_from_batch("/external.series", &batch(vec![1], vec![1.0])?, Some("ts"))
        .await
        .expect("external mutation");
    let stale_error = writer
        .write(&batch(vec![2], vec![2.0])?)
        .await
        .expect_err("stale transaction context must fail");
    assert!(stale_error.to_string().contains("stale"), "{stale_error}");
    writer.abort().await?;
    assert!(
        !root
            .exists(std::path::Path::new("/stale-materialized.series"))
            .await
    );
    Ok(())
}

#[tokio::test]
async fn logical_chunk_identity_survives_physical_parquet_repacking() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let filesystem = FS::new(persistence.clone())
        .await
        .expect("memory filesystem");
    let session = Arc::new(SessionContext::new());
    let context = ProviderContext::new(session, Arc::new(persistence));
    let root = filesystem.root().await.expect("memory root");
    let logical_hash = "ab".repeat(32);
    let schema_fingerprint = "cd".repeat(32);

    for (path, row_group_rows) in [
        ("/packed-small.series", 1usize),
        ("/packed-large.series", 8_192usize),
    ] {
        let mut writer = StreamingSeriesWriter::try_new_with_max_row_group_rows(
            &root,
            path,
            schema(),
            "ts",
            row_group_rows,
        )
        .await
        .expect("open packed writer");
        writer
            .write(&batch(vec![1, 2, 3], vec![10.0, 20.0, 30.0])?)
            .await
            .expect("write packed rows");
        writer
            .set_logical_leaf_metadata(logical_hash.clone(), 3, schema_fingerprint.clone())
            .expect("logical leaf metadata");
        _ = writer.finish().await.expect("finish packed writer");
    }

    let small_versions = root
        .list_file_versions("/packed-small.series")
        .await
        .expect("small versions");
    let large_versions = root
        .list_file_versions("/packed-large.series")
        .await
        .expect("large versions");
    assert_ne!(
        small_versions[0].blake3, large_versions[0].blake3,
        "test requires physically distinct Parquet objects"
    );

    let mut chunk_ids = Vec::new();
    for (path, snapshot_id) in [
        ("/packed-small.series", "packed-small"),
        ("/packed-large.series", "packed-large"),
    ] {
        let file_id = root.get_node_path(path).await.expect("packed node").id();
        let snapshot = capture_tinyfs_snapshot(
            &context,
            file_id,
            snapshot_id,
            schema(),
            Some(EventTimeContract::new("ts")),
            OverlapPolicy::PreserveAll,
        )
        .await?;
        chunk_ids.push(snapshot.snapshot().chunks()[0].chunk_id().to_owned());
    }
    assert_eq!(chunk_ids, vec![logical_hash.clone(), logical_hash]);
    Ok(())
}
