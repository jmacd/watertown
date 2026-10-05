// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Reusable provider contract assertions for persistence backends.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;
use query_foundation::frontier::{RepairPolicy, SettledState};
use query_foundation::materialize::{
    MaterializationProgress, MaterializationPublication, MaterializationReplaceReason,
    TransactionalMaterializationSink, materialize_stream,
};
use tinyfs::arrow::ParquetExt;

use crate::query_foundation_adapter::TinyFsMaterializationSink;
use crate::query_foundation_adapter::capture_tinyfs_snapshot;
use crate::{TableProviderOptions, create_table_provider};

pub const TABLE_SERIES_PATH: &str = "/provider-contract/events.series";
pub const MATERIALIZED_SERIES_PATH: &str = "/provider-contract/materialized.series";
pub const EMPTY_MATERIALIZED_SERIES_PATH: &str = "/provider-contract/empty-materialized.series";

fn timestamp_batch(timestamps: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(timestamps))],
    )
    .expect("timestamp batch")
}

async fn row_count(
    context: &tinyfs::ProviderContext,
    table_name: &str,
) -> datafusion::error::Result<i64> {
    let batches = context
        .datafusion_session
        .sql(&format!("SELECT COUNT(*) AS row_count FROM {table_name}"))
        .await?
        .collect()
        .await?;
    Ok(batches[0]
        .column_by_name("row_count")
        .expect("row_count column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("row_count type")
        .value(0))
}

/// Assert DataFusion read-after-write and provider invalidation semantics.
///
/// The supplied filesystem and provider context must represent the same
/// active persistence snapshot, with the TinyFS object store registered on the
/// DataFusion session.
pub async fn assert_series_read_after_write(root: &tinyfs::WD, context: &tinyfs::ProviderContext) {
    let _ = root
        .create_dir_all("/provider-contract")
        .await
        .expect("provider contract directory");
    _ = root
        .create_series_from_batch(
            TABLE_SERIES_PATH,
            &timestamp_batch(vec![1, 2]),
            Some("timestamp"),
        )
        .await
        .expect("first in-transaction series version");

    let id = root
        .get_node_path(TABLE_SERIES_PATH)
        .await
        .expect("provider contract series")
        .id();
    let provider_before = create_table_provider(id, context, TableProviderOptions::default())
        .await
        .expect("provider over first in-transaction version");
    _ = context
        .datafusion_session
        .register_table("provider_contract_before", provider_before)
        .expect("register provider before append");
    assert_eq!(
        row_count(context, "provider_contract_before")
            .await
            .expect("query first in-transaction version"),
        2,
        "DataFusion must see a completed series write before commit"
    );

    _ = root
        .write_series_from_batch(
            TABLE_SERIES_PATH,
            &timestamp_batch(vec![3]),
            Some("timestamp"),
        )
        .await
        .expect("second in-transaction series version");

    let stale_error = row_count(context, "provider_contract_before")
        .await
        .expect_err("the provider built before append must be stale");
    assert!(
        stale_error.to_string().contains("provider is stale"),
        "unexpected stale-provider error: {stale_error}"
    );

    let provider_after = create_table_provider(id, context, TableProviderOptions::default())
        .await
        .expect("provider over both in-transaction versions");
    _ = context
        .datafusion_session
        .register_table("provider_contract_after", provider_after)
        .expect("register provider after append");
    assert_eq!(
        row_count(context, "provider_contract_after")
            .await
            .expect("query both in-transaction versions"),
        3,
        "a rebuilt provider in the same context must see both series versions"
    );
}

/// Assert that the provider registered by [`assert_series_read_after_write`]
/// cannot execute after its transaction snapshot has closed.
pub async fn assert_registered_provider_closed(context: &tinyfs::ProviderContext) {
    let closed_error = row_count(context, "provider_contract_after")
        .await
        .expect_err("a provider from a closed transaction must not execute");
    assert!(
        closed_error.to_string().contains("closed"),
        "unexpected closed-provider error: {closed_error}"
    );
}

/// Assert that the contract series is queryable from a fresh persistence
/// snapshot with the expected total row count.
pub async fn assert_series_row_count(
    root: &tinyfs::WD,
    context: &tinyfs::ProviderContext,
    expected_rows: i64,
) {
    let id = root
        .get_node_path(TABLE_SERIES_PATH)
        .await
        .expect("persisted provider contract series")
        .id();
    let provider = create_table_provider(id, context, TableProviderOptions::default())
        .await
        .expect("provider over persisted contract series");
    _ = context
        .datafusion_session
        .register_table("provider_contract_reopened", provider)
        .expect("register reopened provider");
    assert_eq!(
        row_count(context, "provider_contract_reopened")
            .await
            .expect("query persisted contract series"),
        expected_rows
    );
}

/// Assert append publication and progress metadata on any TinyFS backend.
pub async fn assert_materialization_append(root: &tinyfs::WD, context: &tinyfs::ProviderContext) {
    let sink = TinyFsMaterializationSink::new(root.clone(), context, "timestamp");
    let progress = MaterializationProgress::try_new(
        "contract-recipe-0001",
        "contract-source-0001",
        SettledState::try_new(Some(3), Some(3), RepairPolicy::reject())
            .expect("contract settled state"),
    )
    .expect("contract progress");
    let stream = Box::pin(RecordBatchStreamAdapter::new(
        timestamp_batch(Vec::new()).schema(),
        stream::iter(vec![
            Ok(timestamp_batch(vec![1, 2])),
            Ok(timestamp_batch(vec![3])),
        ]),
    ));
    let outcome = materialize_stream(
        &sink,
        MATERIALIZED_SERIES_PATH,
        "timestamp",
        progress,
        MaterializationPublication::Append { after: None },
        stream,
    )
    .await
    .expect("materialize append");
    assert_eq!(outcome.metrics.rows_written, 3);

    let versions = root
        .list_file_versions(MATERIALIZED_SERIES_PATH)
        .await
        .expect("materialized versions");
    assert_eq!(versions.len(), 1);
    let attributes = versions[0]
        .extended_metadata
        .as_ref()
        .and_then(|metadata| metadata.get("extended_attributes"))
        .expect("materialization attributes");
    let attributes: serde_json::Value =
        serde_json::from_str(attributes).expect("materialization attributes JSON");
    assert_eq!(
        attributes["watertown.materialization.recipe_id"],
        "contract-recipe-0001"
    );
    assert_eq!(
        attributes["watertown.materialization.source_state_id"],
        "contract-source-0001"
    );
    assert_eq!(attributes["watertown.timestamp_column"], "timestamp");
    assert_eq!(
        root.read_table_as_batch(MATERIALIZED_SERIES_PATH)
            .await
            .expect("materialized batch")
            .num_rows(),
        3
    );
}

/// Assert exact foundation snapshot planning over a persistence backend.
pub async fn assert_foundation_snapshot(root: &tinyfs::WD, context: &tinyfs::ProviderContext) {
    let id = root
        .get_node_path(TABLE_SERIES_PATH)
        .await
        .expect("foundation contract series")
        .id();
    let snapshot = capture_tinyfs_snapshot(
        context,
        id,
        "persistence-contract-snapshot",
        timestamp_batch(Vec::new()).schema(),
        Some(query_foundation::snapshot::EventTimeContract::new(
            "timestamp",
        )),
        query_foundation::overlap::OverlapPolicy::PreserveAll,
    )
    .await
    .expect("capture foundation snapshot");
    assert_eq!(snapshot.snapshot().chunks().len(), 2);
    assert_eq!(
        snapshot
            .snapshot()
            .chunks()
            .iter()
            .map(query_foundation::snapshot::ChunkDescriptor::logical_count)
            .sum::<u64>(),
        3
    );
    _ = context
        .datafusion_session
        .register_table(
            "foundation_persistence_contract",
            snapshot.table_provider().expect("provider"),
        )
        .expect("register foundation contract");
    let frame = context
        .datafusion_session
        .sql("SELECT timestamp FROM foundation_persistence_contract WHERE timestamp >= 2")
        .await
        .expect("plan foundation query");
    let physical = frame
        .clone()
        .create_physical_plan()
        .await
        .expect("foundation physical plan");
    let display = DisplayableExecutionPlan::new(physical.as_ref())
        .indent(true)
        .to_string();
    assert_eq!(display.matches("DataSourceExec").count(), 1, "{display}");
    assert!(display.contains("projection=[timestamp]"), "{display}");
    assert!(display.contains("timestamp@0 >= 2"), "{display}");
    assert!(!display.contains("MemoryExec"), "{display}");
    assert_eq!(
        frame
            .collect()
            .await
            .expect("execute foundation query")
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );
}

/// Assert a registered foundation provider rejects a closed transaction.
pub async fn assert_foundation_provider_closed(context: &tinyfs::ProviderContext) {
    let error = context
        .datafusion_session
        .sql("SELECT COUNT(*) FROM foundation_persistence_contract")
        .await
        .expect("closed-provider SQL planning")
        .collect()
        .await
        .expect_err("foundation provider from a closed transaction must fail");
    assert!(
        error.to_string().contains("closed"),
        "unexpected closed foundation-provider error: {error}"
    );
}

/// Assert no-output, unsupported repair, and stream-failure publication rules.
pub async fn assert_materialization_failure_contract(
    root: &tinyfs::WD,
    context: &tinyfs::ProviderContext,
) {
    let sink = TinyFsMaterializationSink::new(root.clone(), context, "timestamp");
    let no_output_progress = MaterializationProgress::try_new(
        "contract-recipe-0001",
        "contract-source-empty",
        SettledState::try_new(Some(3), Some(3), RepairPolicy::reject())
            .expect("empty settled state"),
    )
    .expect("empty progress");
    let empty_stream = Box::pin(RecordBatchStreamAdapter::new(
        timestamp_batch(Vec::new()).schema(),
        stream::iter(Vec::<datafusion::error::Result<RecordBatch>>::new()),
    ));
    let outcome = materialize_stream(
        &sink,
        EMPTY_MATERIALIZED_SERIES_PATH,
        "timestamp",
        no_output_progress,
        MaterializationPublication::Append { after: Some(3) },
        empty_stream,
    )
    .await
    .expect("no-output materialization");
    assert!(outcome.output.is_none());
    assert!(matches!(
        outcome.publication,
        MaterializationPublication::NoOutput
    ));
    assert!(
        !root
            .exists(std::path::Path::new(EMPTY_MATERIALIZED_SERIES_PATH))
            .await
    );
    assert_eq!(
        root.list_file_versions(format!(
            "{EMPTY_MATERIALIZED_SERIES_PATH}.materialization-progress"
        ))
        .await
        .expect("no-output progress versions")
        .len(),
        1
    );

    let repair_path = "/provider-contract/unsupported-repair.series";
    let repair = sink
        .begin(
            repair_path,
            &MaterializationPublication::Replace {
                ranges: vec![
                    query_foundation::statistics::TimeInterval::try_new(1, 2)
                        .expect("repair interval"),
                ],
                reason: MaterializationReplaceReason::RetroactiveRepair,
            },
        )
        .await;
    assert!(repair.is_err(), "append-only sink must reject repair");
    assert!(!root.exists(std::path::Path::new(repair_path)).await);

    let failed_path = "/provider-contract/failed-materialized.series";
    let failed_progress = MaterializationProgress::try_new(
        "contract-recipe-0001",
        "contract-source-failed",
        SettledState::try_new(Some(4), Some(4), RepairPolicy::reject())
            .expect("failed settled state"),
    )
    .expect("failed progress");
    let failed_stream = Box::pin(RecordBatchStreamAdapter::new(
        timestamp_batch(Vec::new()).schema(),
        stream::iter(vec![
            Ok(timestamp_batch(vec![4])),
            Err(DataFusionError::Execution(
                "injected cross-persistence stream failure".to_owned(),
            )),
        ]),
    ));
    let error = materialize_stream(
        &sink,
        failed_path,
        "timestamp",
        failed_progress,
        MaterializationPublication::Append { after: Some(3) },
        failed_stream,
    )
    .await
    .expect_err("stream failure must surface");
    assert!(
        error
            .to_string()
            .contains("injected cross-persistence stream failure"),
        "{error}"
    );
    assert!(
        root.list_file_versions(failed_path)
            .await
            .expect("failed materialization versions")
            .is_empty()
    );
}
