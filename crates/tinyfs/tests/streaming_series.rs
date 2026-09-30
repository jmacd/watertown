// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;
use tinyfs::arrow::ParquetExt;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "timestamp",
        DataType::Int64,
        false,
    )]))
}

fn batch(start: i64, rows: usize) -> DataFusionResult<RecordBatch> {
    Ok(RecordBatch::try_new(
        schema(),
        vec![Arc::new(Int64Array::from_iter_values(
            (0..rows).map(|offset| start + offset as i64),
        ))],
    )?)
}

fn record_stream(batches: Vec<DataFusionResult<RecordBatch>>) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        schema(),
        stream::iter(batches),
    ))
}

#[tokio::test]
async fn writes_many_batches_directly_to_one_series_version() {
    let filesystem = tinyfs::memory::new_fs().await;
    let root = filesystem.root().await.expect("memory root");
    let batches = (0..100)
        .map(|index| batch(index * 2, 2))
        .collect::<Vec<_>>();
    let (minimum, maximum) = root
        .create_series_from_stream(
            "/streamed.series",
            record_stream(batches),
            Some("timestamp"),
        )
        .await
        .expect("stream series");
    assert_eq!((minimum, maximum), (0, 199));
    let versions = root
        .list_file_versions("/streamed.series")
        .await
        .expect("streamed versions");
    assert_eq!(versions.len(), 1);
    let result = root
        .read_table_as_batch("/streamed.series")
        .await
        .expect("read streamed series");
    assert_eq!(result.num_rows(), 200);
}

#[tokio::test]
async fn stream_failure_publishes_no_series_version() {
    let filesystem = tinyfs::memory::new_fs().await;
    let root = filesystem.root().await.expect("memory root");
    let error = root
        .create_series_from_stream(
            "/failed.series",
            record_stream(vec![
                batch(0, 2),
                Err(DataFusionError::Execution(
                    "injected stream failure".to_string(),
                )),
            ]),
            Some("timestamp"),
        )
        .await
        .expect_err("stream failure must surface");
    assert!(
        error.to_string().contains("injected stream failure"),
        "{error}"
    );
    let versions = root
        .list_file_versions("/failed.series")
        .await
        .expect("failed series node remains inspectable");
    assert!(versions.is_empty());
}

#[tokio::test]
async fn empty_batches_do_not_create_empty_versions_or_break_bounds() {
    let filesystem = tinyfs::memory::new_fs().await;
    let root = filesystem.root().await.expect("memory root");
    let empty = batch(0, 0).expect("empty batch");
    let (minimum, maximum) = root
        .create_series_from_stream(
            "/empty-batches.series",
            record_stream(vec![Ok(empty.clone()), batch(10, 2), Ok(empty)]),
            Some("timestamp"),
        )
        .await
        .expect("stream with empty batches");
    assert_eq!((minimum, maximum), (10, 11));

    let error = root
        .create_series_from_stream(
            "/only-empty.series",
            record_stream(vec![batch(0, 0)]),
            Some("timestamp"),
        )
        .await
        .expect_err("all-empty stream must not create a version");
    assert!(error.to_string().contains("Empty stream"), "{error}");
    assert!(
        !root
            .exists(std::path::Path::new("/only-empty.series"))
            .await
    );
}
