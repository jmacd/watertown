// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use provider::TableProviderOptions;
use steward::Ship;
use tinyfs::arrow::ParquetExt;
use tlogfs::PondUserMetadata;

fn meta(label: &str) -> PondUserMetadata {
    PondUserMetadata::new(vec!["test".into(), label.into()])
}

fn observations() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1_000_i64, 2_000, 3_000])),
            Arc::new(Float64Array::from(vec![4.0_f64, 12.5, 8.0])),
        ],
    )
    .expect("observation batch")
}

fn monitor_state(max_value: f64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("max_value", DataType::Float64, false),
            Field::new("firing", DataType::Boolean, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![3_000_i64])),
            Arc::new(Float64Array::from(vec![max_value])),
            Arc::new(BooleanArray::from(vec![max_value > 10.0])),
        ],
    )
    .expect("monitor-state batch")
}

async fn register_pending_table(
    root: &tinyfs::WD,
    context: &tinyfs::ProviderContext,
    path: &str,
    table_name: &str,
) {
    register_table(
        root,
        context,
        path,
        table_name,
        TableProviderOptions::default(),
    )
    .await;
}

async fn register_table(
    root: &tinyfs::WD,
    context: &tinyfs::ProviderContext,
    path: &str,
    table_name: &str,
    options: TableProviderOptions,
) {
    let id = root.get_node_path(path).await.expect("pending node").id();
    let table = provider::create_table_provider(id, context, options)
        .await
        .expect("table provider over pending version");
    let _ = context
        .datafusion_session
        .register_table(table_name, table)
        .expect("register pending table");
}

async fn query_max(context: &tinyfs::ProviderContext, table_name: &str) -> f64 {
    let batches = context
        .datafusion_session
        .sql(&format!("SELECT MAX(value) AS max_value FROM {table_name}"))
        .await
        .expect("plan max query")
        .collect()
        .await
        .expect("execute max query");
    let values = batches[0]
        .column_by_name("max_value")
        .expect("max_value")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("max_value type");
    values.value(0)
}

async fn query_firing(context: &tinyfs::ProviderContext, table_name: &str) -> bool {
    let batches = context
        .datafusion_session
        .sql(&format!("SELECT firing FROM {table_name}"))
        .await
        .expect("plan firing query")
        .collect()
        .await
        .expect("execute firing query");
    let values = batches[0]
        .column_by_name("firing")
        .expect("firing")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("firing type");
    values.value(0)
}

async fn query_count(context: &tinyfs::ProviderContext, table_name: &str) -> i64 {
    query_count_result(context, table_name)
        .await
        .expect("execute count query")
}

async fn query_count_result(
    context: &tinyfs::ProviderContext,
    table_name: &str,
) -> datafusion::error::Result<i64> {
    let batches = context
        .datafusion_session
        .sql(&format!("SELECT COUNT(*) AS row_count FROM {table_name}"))
        .await?
        .collect()
        .await?;
    let values = batches[0]
        .column_by_name("row_count")
        .expect("row_count")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("row_count type");
    Ok(values.value(0))
}

#[tokio::test]
async fn write_query_write_query_in_one_transaction() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut ship = Ship::create_pond(temp.path().join("pond"), "read-after-write")
        .await
        .expect("create pond");

    let tx = ship
        .begin_write(&meta("mixed-transaction"))
        .await
        .expect("begin write");
    let root = tx.root().await.expect("root");
    let _ = root.create_dir_all("/observations").await.expect("mkdir");
    let _ = root.create_dir_all("/monitoring").await.expect("mkdir");

    let context = tx.provider_context().expect("provider context");
    let _ = root
        .create_series_from_batch(
            "/observations/water.series",
            &observations(),
            Some("timestamp"),
        )
        .await
        .expect("write observations");
    let pending_observations = root
        .read_table_as_batch("/observations/water.series")
        .await
        .expect("read pending observations through TinyFS");
    assert_eq!(pending_observations.num_rows(), 3);

    register_pending_table(
        &root,
        &context,
        "/observations/water.series",
        "observations",
    )
    .await;
    let max_value = query_max(&context, "observations").await;
    assert_eq!(max_value, 12.5);

    let _ = root
        .create_series_from_batch(
            "/monitoring/state.series",
            &monitor_state(max_value),
            Some("timestamp"),
        )
        .await
        .expect("write monitor state after query");
    let pending_monitor_state = root
        .read_table_as_batch("/monitoring/state.series")
        .await
        .expect("read pending monitor state through TinyFS");
    assert_eq!(pending_monitor_state.num_rows(), 1);
    register_pending_table(&root, &context, "/monitoring/state.series", "monitor_state").await;
    assert!(query_firing(&context, "monitor_state").await);

    let committed_version = tx.commit().await.expect("single commit");
    assert!(committed_version.is_some());

    let tx = ship
        .begin_read(&meta("verify-commit"))
        .await
        .expect("begin verification read");
    let root = tx.root().await.expect("verification root");
    let context = tx.provider_context().expect("verification context");
    register_pending_table(
        &root,
        &context,
        "/observations/water.series",
        "committed_observations",
    )
    .await;
    register_pending_table(
        &root,
        &context,
        "/monitoring/state.series",
        "committed_monitor_state",
    )
    .await;
    assert_eq!(query_max(&context, "committed_observations").await, 12.5);
    assert!(query_firing(&context, "committed_monitor_state").await);
    let committed_version = tx.commit().await.expect("finish verification read");
    assert!(committed_version.is_none());
}

#[tokio::test]
async fn query_combines_committed_and_pending_series_versions() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut ship = Ship::create_pond(temp.path().join("pond"), "mixed-history")
        .await
        .expect("create pond");

    let tx = ship
        .begin_write(&meta("initial-observations"))
        .await
        .expect("begin initial write");
    let root = tx.root().await.expect("root");
    let _ = root.create_dir_all("/observations").await.expect("mkdir");
    let _ = root
        .create_series_from_batch(
            "/observations/water.series",
            &observations(),
            Some("timestamp"),
        )
        .await
        .expect("write committed observations");
    let _ = tx.commit().await.expect("commit initial observations");

    let tx = ship
        .begin_write(&meta("append-observations"))
        .await
        .expect("begin append write");
    let root = tx.root().await.expect("root");
    let context_before_write = tx.provider_context().expect("provider context");
    register_pending_table(
        &root,
        &context_before_write,
        "/observations/water.series",
        "committed_observations_before_append",
    )
    .await;
    assert_eq!(
        query_count(
            &context_before_write,
            "committed_observations_before_append"
        )
        .await,
        3
    );

    let appended = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![4_000_i64])),
            Arc::new(Float64Array::from(vec![20.0_f64])),
        ],
    )
    .expect("appended observations");
    let _ = root
        .write_series_from_batch("/observations/water.series", &appended, Some("timestamp"))
        .await
        .expect("append pending observations");

    let stale_error = query_count_result(
        &context_before_write,
        "committed_observations_before_append",
    )
    .await
    .expect_err("provider created before append must be stale");
    assert!(stale_error.to_string().contains("provider is stale"));

    register_pending_table(
        &root,
        &context_before_write,
        "/observations/water.series",
        "mixed_observations_same_context",
    )
    .await;
    assert_eq!(
        query_count(&context_before_write, "mixed_observations_same_context").await,
        4
    );

    let context_after_write = tx.provider_context().expect("fresh provider context");
    register_table(
        &root,
        &context_after_write,
        "/observations/water.series",
        "mixed_observations",
        TableProviderOptions {
            bounds: tinyfs::SeriesReadBounds::from_event_time_lo(0),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        query_count(&context_after_write, "mixed_observations").await,
        4
    );
    assert_eq!(
        query_max(&context_after_write, "mixed_observations").await,
        20.0
    );

    let _ = tx.commit().await.expect("commit appended observations");
}

#[tokio::test]
async fn query_combines_multiple_pending_series_versions() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut ship = Ship::create_pond(temp.path().join("pond"), "pending-history")
        .await
        .expect("create pond");

    let tx = ship
        .begin_write(&meta("multiple-pending"))
        .await
        .expect("begin write");
    let root = tx.root().await.expect("root");
    let _ = root.create_dir_all("/observations").await.expect("mkdir");
    let _ = root
        .create_series_from_batch(
            "/observations/water.series",
            &observations(),
            Some("timestamp"),
        )
        .await
        .expect("write first pending version");
    let appended = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![4_000_i64])),
            Arc::new(Float64Array::from(vec![20.0_f64])),
        ],
    )
    .expect("appended observations");
    let _ = root
        .write_series_from_batch("/observations/water.series", &appended, Some("timestamp"))
        .await
        .expect("write second pending version");

    let context = tx.provider_context().expect("provider context");
    register_table(
        &root,
        &context,
        "/observations/water.series",
        "pending_observations",
        TableProviderOptions {
            bounds: tinyfs::SeriesReadBounds::from_event_time_lo(0),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(query_count(&context, "pending_observations").await, 4);
    assert_eq!(query_max(&context, "pending_observations").await, 20.0);

    let _ = tx.commit().await.expect("commit pending versions");
}

#[tokio::test]
async fn provider_context_is_closed_after_commit() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut ship = Ship::create_pond(temp.path().join("pond"), "closed-context")
        .await
        .expect("create pond");

    let tx = ship
        .begin_write(&meta("close-context"))
        .await
        .expect("begin write");
    let root = tx.root().await.expect("root");
    let _ = root.create_dir_all("/observations").await.expect("mkdir");
    let _ = root
        .create_series_from_batch(
            "/observations/water.series",
            &observations(),
            Some("timestamp"),
        )
        .await
        .expect("write observations");
    let context = tx.provider_context().expect("provider context");
    register_pending_table(
        &root,
        &context,
        "/observations/water.series",
        "observations",
    )
    .await;
    assert_eq!(query_count(&context, "observations").await, 3);

    let _ = tx.commit().await.expect("commit");
    let error = query_count_result(&context, "observations")
        .await
        .expect_err("old context must not remain readable");
    assert!(error.to_string().contains("closed"));
}

#[tokio::test]
async fn provider_context_is_closed_after_abort() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut ship = Ship::create_pond(temp.path().join("pond"), "aborted-context")
        .await
        .expect("create pond");

    let tx = ship
        .begin_write(&meta("abort-context"))
        .await
        .expect("begin write");
    let root = tx.root().await.expect("root");
    let _ = root.create_dir_all("/observations").await.expect("mkdir");
    let _ = root
        .create_series_from_batch(
            "/observations/water.series",
            &observations(),
            Some("timestamp"),
        )
        .await
        .expect("write observations");
    let context = tx.provider_context().expect("provider context");
    register_pending_table(
        &root,
        &context,
        "/observations/water.series",
        "observations",
    )
    .await;

    let _ = tx.abort("test abort").await;
    let error = query_count_result(&context, "observations")
        .await
        .expect_err("old context must not remain readable");
    assert!(error.to_string().contains("closed"));
}

#[tokio::test]
async fn commit_rejects_unfinished_writer() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut ship = Ship::create_pond(temp.path().join("pond"), "unfinished-writer")
        .await
        .expect("create pond");

    let tx = ship
        .begin_write(&meta("unfinished-writer"))
        .await
        .expect("begin write");
    let root = tx.root().await.expect("root");
    let _ = root.create_dir_all("/observations").await.expect("mkdir");
    let writer = root
        .create_file_writer_with_type(
            "/observations/water.series",
            tinyfs::EntryType::TablePhysicalSeries,
        )
        .await
        .expect("open writer");

    let error = tx
        .commit()
        .await
        .expect_err("commit must reject unfinished writer");
    assert!(error.to_string().contains("unfinished writer"));
    drop(writer);
}

#[tokio::test]
async fn commit_rejects_active_transaction_read() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut ship = Ship::create_pond(temp.path().join("pond"), "active-query")
        .await
        .expect("create pond");

    let tx = ship
        .begin_write(&meta("active-query"))
        .await
        .expect("begin write");
    let root = tx.root().await.expect("root");
    let _ = root.create_dir_all("/observations").await.expect("mkdir");
    let _ = root
        .create_series_from_batch(
            "/observations/water.series",
            &observations(),
            Some("timestamp"),
        )
        .await
        .expect("write observations");
    let context = tx.provider_context().expect("provider context");
    register_pending_table(
        &root,
        &context,
        "/observations/water.series",
        "observations",
    )
    .await;
    let coherence = context
        .persistence
        .coherence_state()
        .expect("transaction coherence");
    let query_guard = coherence
        .begin_query(coherence.generation())
        .expect("start transaction read");

    let error = tx
        .commit()
        .await
        .expect_err("commit must reject active query stream");
    assert!(error.to_string().contains("active query stream"));
    drop(query_guard);
}

#[tokio::test]
async fn latest_version_provider_selects_only_highest_version() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut ship = Ship::create_pond(temp.path().join("pond"), "latest-version")
        .await
        .expect("create pond");

    let tx = ship
        .begin_write(&meta("latest-version"))
        .await
        .expect("begin write");
    let root = tx.root().await.expect("root");
    let _ = root.create_dir_all("/observations").await.expect("mkdir");
    let _ = root
        .create_series_from_batch(
            "/observations/water.series",
            &observations(),
            Some("timestamp"),
        )
        .await
        .expect("write first version");
    let appended = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![4_000_i64])),
            Arc::new(Float64Array::from(vec![20.0_f64])),
        ],
    )
    .expect("appended observations");
    let _ = root
        .write_series_from_batch("/observations/water.series", &appended, Some("timestamp"))
        .await
        .expect("write second version");

    let context = tx.provider_context().expect("provider context");
    register_table(
        &root,
        &context,
        "/observations/water.series",
        "latest_observations",
        TableProviderOptions {
            version_selection: provider::VersionSelection::LatestVersion,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(query_count(&context, "latest_observations").await, 1);
    assert_eq!(query_max(&context, "latest_observations").await, 20.0);

    let _ = tx.commit().await.expect("commit");
}

#[tokio::test]
async fn provider_merges_schema_across_pending_versions() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut ship = Ship::create_pond(temp.path().join("pond"), "schema-evolution")
        .await
        .expect("create pond");

    let tx = ship
        .begin_write(&meta("schema-evolution"))
        .await
        .expect("begin write");
    let root = tx.root().await.expect("root");
    let _ = root.create_dir_all("/observations").await.expect("mkdir");
    let _ = root
        .create_series_from_batch(
            "/observations/water.series",
            &observations(),
            Some("timestamp"),
        )
        .await
        .expect("write first schema");
    let evolved = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("quality", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![4_000_i64])),
            Arc::new(Float64Array::from(vec![20.0_f64])),
            Arc::new(StringArray::from(vec![Some("good")])),
        ],
    )
    .expect("evolved observations");
    let _ = root
        .write_series_from_batch("/observations/water.series", &evolved, Some("timestamp"))
        .await
        .expect("write evolved schema");

    let context = tx.provider_context().expect("provider context");
    register_pending_table(
        &root,
        &context,
        "/observations/water.series",
        "observations",
    )
    .await;
    let batches = context
        .datafusion_session
        .sql("SELECT COUNT(*) AS rows, COUNT(quality) AS quality_rows FROM observations")
        .await
        .expect("plan schema-evolution query")
        .collect()
        .await
        .expect("execute schema-evolution query");
    let rows = batches[0]
        .column_by_name("rows")
        .expect("rows")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("rows type")
        .value(0);
    let quality_rows = batches[0]
        .column_by_name("quality_rows")
        .expect("quality_rows")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("quality_rows type")
        .value(0);
    assert_eq!(rows, 4);
    assert_eq!(quality_rows, 1);

    let _ = tx.commit().await.expect("commit");
}
