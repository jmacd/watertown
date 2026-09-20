// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Reusable provider contract assertions for persistence backends.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use tinyfs::arrow::ParquetExt;

use crate::{TableProviderOptions, create_table_provider};

pub const TABLE_SERIES_PATH: &str = "/provider-contract/events.series";

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
