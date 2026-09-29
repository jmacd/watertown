// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, Mutex};

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;
use query_foundation::frontier::{RepairPolicy, SettledState};
use query_foundation::materialize::{
    MaterializationCommit, MaterializationProgress, MaterializedOutput, TransactionalBatchWriter,
    TransactionalMaterializationSink, materialize_stream,
};

#[derive(Clone, Copy, Debug, Default)]
enum FailurePoint {
    #[default]
    None,
    Write(u64),
    Close,
    ProgressPublication,
    OutputPublication,
    Abort,
}

#[derive(Debug, Default)]
struct SinkState {
    active_stages: u64,
    writes: u64,
    closes: u64,
    outputs: Vec<MaterializedOutput>,
    progress: Option<MaterializationProgress>,
}

#[derive(Clone, Debug, Default)]
struct RecordingSink {
    state: Arc<Mutex<SinkState>>,
    failure: FailurePoint,
}

impl RecordingSink {
    fn failing(failure: FailurePoint) -> Self {
        Self {
            failure,
            ..Self::default()
        }
    }
}

struct RecordingWriter {
    state: Arc<Mutex<SinkState>>,
    failure: FailurePoint,
    write_calls: u64,
}

#[async_trait]
impl TransactionalMaterializationSink for RecordingSink {
    async fn begin(&self, _output_id: &str) -> Result<Box<dyn TransactionalBatchWriter>> {
        self.state
            .lock()
            .expect("sink state lock poisoned")
            .active_stages += 1;
        Ok(Box::new(RecordingWriter {
            state: Arc::clone(&self.state),
            failure: self.failure,
            write_calls: 0,
        }))
    }
}

#[async_trait]
impl TransactionalBatchWriter for RecordingWriter {
    async fn write(&mut self, _batch: &RecordBatch) -> Result<()> {
        self.write_calls += 1;
        if matches!(self.failure, FailurePoint::Write(call) if call == self.write_calls) {
            return Err(DataFusionError::Execution(
                "injected staged write failure".to_owned(),
            ));
        }
        self.state.lock().expect("sink state lock poisoned").writes += 1;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if matches!(self.failure, FailurePoint::Close) {
            return Err(DataFusionError::Execution(
                "injected writer close failure".to_owned(),
            ));
        }
        self.state.lock().expect("sink state lock poisoned").closes += 1;
        Ok(())
    }

    async fn commit(self: Box<Self>, commit: MaterializationCommit) -> Result<()> {
        let mut state = self.state.lock().expect("sink state lock poisoned");
        state.active_stages -= 1;
        match self.failure {
            FailurePoint::ProgressPublication => {
                return Err(DataFusionError::Execution(
                    "injected progress publication failure".to_owned(),
                ));
            }
            FailurePoint::OutputPublication => {
                return Err(DataFusionError::Execution(
                    "injected output publication failure".to_owned(),
                ));
            }
            _ => {}
        }
        if let Some(output) = commit.output {
            state.outputs.push(output);
        }
        state.progress = Some(commit.progress);
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        if matches!(self.failure, FailurePoint::Abort) {
            return Err(DataFusionError::Execution(
                "injected staged abort failure".to_owned(),
            ));
        }
        self.state
            .lock()
            .expect("sink state lock poisoned")
            .active_stages -= 1;
        Ok(())
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

fn batch(start: i64, rows: usize) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|offset| start + offset as i64),
            )),
            Arc::new(Float64Array::from_iter_values(
                (0..rows).map(|offset| (start + offset as i64) as f64),
            )),
        ],
    )?)
}

fn stream_from(batches: Vec<Result<RecordBatch>>) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        schema(),
        stream::iter(batches),
    ))
}

fn progress(source_state_id: &str) -> Result<MaterializationProgress> {
    MaterializationProgress::try_new(
        "recipe-1",
        source_state_id,
        SettledState::try_new(Some(100), Some(120), RepairPolicy::within(20)?)?,
    )
}

#[tokio::test]
async fn streams_many_batches_with_bounded_active_memory_and_exact_metadata() -> Result<()> {
    let sink = RecordingSink::default();
    let batches = (0..100)
        .map(|index| batch(index * 2, 2))
        .collect::<Vec<_>>();
    let outcome = materialize_stream(
        &sink,
        "output-0001",
        "ts",
        progress("source-state-1")?,
        stream_from(batches),
    )
    .await?;
    let output = outcome
        .output
        .expect("non-empty stream must publish output");
    assert_eq!(output.output_id(), "output-0001");
    assert_eq!(output.rows(), 200);
    assert_eq!(output.event_time_bounds().min(), 0);
    assert_eq!(output.event_time_bounds().max(), 199);
    assert_eq!(outcome.metrics.batches_written, 100);
    assert_eq!(outcome.metrics.rows_written, 200);
    assert_eq!(outcome.metrics.peak_batch_rows, 2);

    let state = sink.state.lock().expect("sink state lock poisoned");
    assert_eq!(state.active_stages, 0);
    assert_eq!(state.writes, 100);
    assert_eq!(state.closes, 1);
    assert_eq!(state.outputs, vec![output]);
    assert_eq!(
        state
            .progress
            .as_ref()
            .expect("progress must be visible")
            .source_state_id(),
        "source-state-1"
    );

    Ok(())
}

#[tokio::test]
async fn no_rows_publish_progress_without_an_output_write() -> Result<()> {
    let sink = RecordingSink::default();
    let outcome = materialize_stream(
        &sink,
        "output-0002",
        "ts",
        progress("source-state-2")?,
        stream_from(vec![]),
    )
    .await?;
    assert!(outcome.output.is_none());
    assert_eq!(outcome.metrics.rows_written, 0);

    let state = sink.state.lock().expect("sink state lock poisoned");
    assert_eq!(state.active_stages, 0);
    assert_eq!(state.writes, 0);
    assert_eq!(state.closes, 1);
    assert!(state.outputs.is_empty());
    assert_eq!(
        state
            .progress
            .as_ref()
            .expect("progress must advance")
            .source_state_id(),
        "source-state-2"
    );

    Ok(())
}

#[tokio::test]
async fn every_streaming_and_publication_failure_is_atomic() -> Result<()> {
    let scenarios = [
        (
            RecordingSink::default(),
            stream_from(vec![
                batch(0, 2),
                batch(2, 2),
                Err(DataFusionError::Execution(
                    "injected stream failure".to_owned(),
                )),
            ]),
            "stream failure",
        ),
        (
            RecordingSink::failing(FailurePoint::Write(2)),
            stream_from(vec![batch(0, 2), batch(2, 2)]),
            "write failure",
        ),
        (
            RecordingSink::failing(FailurePoint::Close),
            stream_from(vec![batch(0, 2)]),
            "close failure",
        ),
        (
            RecordingSink::failing(FailurePoint::ProgressPublication),
            stream_from(vec![batch(0, 2)]),
            "progress publication failure",
        ),
        (
            RecordingSink::failing(FailurePoint::OutputPublication),
            stream_from(vec![batch(0, 2)]),
            "output publication failure",
        ),
    ];

    for (sink, stream, expected) in scenarios {
        let error = materialize_stream(
            &sink,
            "failed-output",
            "ts",
            progress("failed-state")?,
            stream,
        )
        .await
        .expect_err("injected materialization failure must surface");
        assert!(error.to_string().contains(expected), "{error}");
        let state = sink.state.lock().expect("sink state lock poisoned");
        assert_eq!(state.active_stages, 0, "{expected}");
        assert!(state.outputs.is_empty(), "{expected}");
        assert!(state.progress.is_none(), "{expected}");
    }

    Ok(())
}

#[tokio::test]
async fn metadata_and_abort_failures_are_both_visible() -> Result<()> {
    let sink = RecordingSink::failing(FailurePoint::Abort);
    let invalid_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        false,
    )]));
    let invalid_batch = RecordBatch::try_new(
        invalid_schema.clone(),
        vec![Arc::new(Float64Array::from(vec![1.0]))],
    )?;
    let stream = Box::pin(RecordBatchStreamAdapter::new(
        invalid_schema,
        stream::iter(vec![Ok(invalid_batch)]),
    ));
    let error = materialize_stream(
        &sink,
        "failed-output",
        "ts",
        progress("failed-state")?,
        stream,
    )
    .await
    .expect_err("invalid metadata and abort must fail");
    assert!(error.to_string().contains("missing event-time"), "{error}");
    assert!(error.to_string().contains("abort also failed"), "{error}");
    let state = sink.state.lock().expect("sink state lock poisoned");
    assert!(state.outputs.is_empty());
    assert!(state.progress.is_none());

    Ok(())
}
