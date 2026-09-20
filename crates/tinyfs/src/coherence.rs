// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

use crate::{Error, FileID, Result};

const OPEN: u8 = 0;
const COMMITTING: u8 = 1;
const CLOSED: u8 = 2;

/// Shared transaction-lifecycle and cache-coherence state.
///
/// A generation identifies one readable in-transaction snapshot. Completed
/// mutations advance it, making previously constructed providers stale.
/// Writer, query, and mutation guards prevent commit from draining pending
/// state while an operation is still using it.
#[derive(Debug, Default)]
pub struct CoherenceState {
    generation: AtomicU64,
    lifecycle: AtomicU8,
    active_queries: AtomicUsize,
    active_mutations: AtomicUsize,
    active_writers: Mutex<HashSet<FileID>>,
}

impl CoherenceState {
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn ensure_open(&self) -> Result<()> {
        match self.lifecycle.load(Ordering::Acquire) {
            OPEN => Ok(()),
            COMMITTING => Err(Error::Other(
                "Transaction state is committing and cannot be read".to_string(),
            )),
            CLOSED => Err(Error::Other(
                "Transaction state is closed and cannot be reused".to_string(),
            )),
            state => Err(Error::Other(format!(
                "Transaction state has invalid lifecycle value {state}"
            ))),
        }
    }

    pub fn ensure_generation(&self, expected: u64) -> Result<()> {
        self.ensure_open()?;
        let actual = self.generation();
        if actual != expected {
            return Err(Error::Other(format!(
                "Table provider is stale: transaction generation advanced from {expected} to {actual}"
            )));
        }
        Ok(())
    }

    pub fn advance(&self) -> Result<u64> {
        self.ensure_open()?;
        Ok(self.generation.fetch_add(1, Ordering::AcqRel) + 1)
    }

    pub fn begin_writer(self: &Arc<Self>, id: FileID) -> Result<WriterGuard> {
        self.ensure_open()?;
        {
            let mut active = self
                .active_writers
                .lock()
                .map_err(|_| Error::Other("Active-writer mutex poisoned".to_string()))?;
            if !active.insert(id) {
                return Err(Error::Other(format!(
                    "File {id} is already being written in this transaction"
                )));
            }
        }
        if let Err(error) = self.ensure_open() {
            if let Ok(mut active) = self.active_writers.lock() {
                _ = active.remove(&id);
            }
            return Err(error);
        }
        Ok(WriterGuard {
            state: Arc::clone(self),
            id,
        })
    }

    pub fn begin_query(self: &Arc<Self>, expected_generation: u64) -> Result<QueryGuard> {
        self.ensure_generation(expected_generation)?;
        _ = self.active_queries.fetch_add(1, Ordering::AcqRel);
        if let Err(error) = self.ensure_generation(expected_generation) {
            _ = self.active_queries.fetch_sub(1, Ordering::AcqRel);
            return Err(error);
        }
        Ok(QueryGuard {
            state: Arc::clone(self),
        })
    }

    pub fn begin_mutation(self: &Arc<Self>) -> Result<MutationGuard> {
        self.ensure_open()?;
        _ = self.active_mutations.fetch_add(1, Ordering::AcqRel);
        if let Err(error) = self.ensure_open() {
            _ = self.active_mutations.fetch_sub(1, Ordering::AcqRel);
            return Err(error);
        }
        Ok(MutationGuard {
            state: Arc::clone(self),
        })
    }

    pub fn begin_commit(&self) -> Result<()> {
        _ = self
            .lifecycle
            .compare_exchange(OPEN, COMMITTING, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|state| {
                Error::Other(format!(
                    "Cannot begin commit from transaction lifecycle state {state}"
                ))
            })?;

        let (active_writers, active_queries, active_mutations) = match self.active_counts() {
            Ok(counts) => counts,
            Err(error) => {
                self.lifecycle.store(CLOSED, Ordering::Release);
                return Err(error);
            }
        };
        if active_writers != 0 || active_queries != 0 || active_mutations != 0 {
            self.lifecycle.store(CLOSED, Ordering::Release);
            return Err(Error::Other(format!(
                "Cannot commit transaction with {active_writers} unfinished writer(s), {active_queries} active query stream(s), and {active_mutations} active mutation(s)"
            )));
        }
        Ok(())
    }

    pub fn ensure_quiescent(&self) -> Result<()> {
        self.ensure_open()?;
        let (active_writers, active_queries, active_mutations) = self.active_counts()?;
        if active_writers != 0 || active_queries != 0 || active_mutations != 0 {
            return Err(Error::Other(format!(
                "Cannot commit transaction with {active_writers} unfinished writer(s), {active_queries} active query stream(s), and {active_mutations} active mutation(s)"
            )));
        }
        Ok(())
    }

    fn active_counts(&self) -> Result<(usize, usize, usize)> {
        let active_writers = self
            .active_writers
            .lock()
            .map_err(|_| Error::Other("Active-writer mutex poisoned".to_string()))?
            .len();
        let active_queries = self.active_queries.load(Ordering::Acquire);
        let active_mutations = self.active_mutations.load(Ordering::Acquire);
        Ok((active_writers, active_queries, active_mutations))
    }

    pub fn close(&self) {
        self.lifecycle.store(CLOSED, Ordering::Release);
    }
}

pub struct WriterGuard {
    state: Arc<CoherenceState>,
    id: FileID,
}

impl Drop for WriterGuard {
    fn drop(&mut self) {
        if let Ok(mut active) = self.state.active_writers.lock() {
            _ = active.remove(&self.id);
        }
    }
}

pub struct QueryGuard {
    state: Arc<CoherenceState>,
}

pub struct MutationGuard {
    state: Arc<CoherenceState>,
}

impl Drop for MutationGuard {
    fn drop(&mut self) {
        _ = self.state.active_mutations.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Drop for QueryGuard {
    fn drop(&mut self) {
        _ = self.state.active_queries.fetch_sub(1, Ordering::AcqRel);
    }
}

fn datafusion_error(error: Error) -> DataFusionError {
    DataFusionError::Execution(error.to_string())
}

/// Wrap a table provider with the transaction generation it represents.
pub fn coherent_table_provider(
    inner: Arc<dyn TableProvider>,
    coherence: Option<Arc<CoherenceState>>,
    generation: Option<u64>,
) -> Arc<dyn TableProvider> {
    let Some(coherence) = coherence else {
        return inner;
    };
    let generation = generation.unwrap_or_else(|| coherence.generation());
    Arc::new(CoherentTableProvider {
        inner,
        coherence,
        generation,
    })
}

#[derive(Debug)]
struct CoherentTableProvider {
    inner: Arc<dyn TableProvider>,
    coherence: Arc<CoherenceState>,
    generation: u64,
}

#[async_trait]
impl TableProvider for CoherentTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.coherence
            .ensure_generation(self.generation)
            .map_err(datafusion_error)?;
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.coherence
            .ensure_generation(self.generation)
            .map_err(datafusion_error)?;
        let inner = self.inner.scan(state, projection, filters, limit).await?;
        Ok(Arc::new(CoherentExec {
            inner,
            coherence: self.coherence.clone(),
            generation: self.generation,
        }))
    }
}

#[derive(Debug)]
struct CoherentExec {
    inner: Arc<dyn ExecutionPlan>,
    coherence: Arc<CoherenceState>,
    generation: u64,
}

impl DisplayAs for CoherentExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => write!(f, "CoherentExec"),
        }
    }
}

impl ExecutionPlan for CoherentExec {
    fn name(&self) -> &str {
        "CoherentExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "CoherentExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self {
            inner: children[0].clone(),
            coherence: self.coherence.clone(),
            generation: self.generation,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let query_guard = self
            .coherence
            .begin_query(self.generation)
            .map_err(datafusion_error)?;
        let inner_stream = self.inner.execute(partition, context)?;
        let schema = inner_stream.schema();
        let stream = futures::stream::unfold(
            (inner_stream, query_guard),
            |(mut inner_stream, query_guard)| async move {
                inner_stream
                    .next()
                    .await
                    .map(|batch| (batch, (inner_stream, query_guard)))
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionContext;

    #[test]
    fn mutations_stale_prior_generations() {
        let state = CoherenceState::default();
        assert_eq!(state.generation(), 0);
        assert!(state.ensure_generation(0).is_ok());
        assert_eq!(state.advance().unwrap(), 1);
        assert!(state.ensure_generation(0).is_err());
        assert!(state.ensure_generation(1).is_ok());
    }

    #[test]
    fn commit_rejects_active_writer_and_closes_state() {
        let state = Arc::new(CoherenceState::default());
        let _writer = state.begin_writer(FileID::root()).unwrap();
        assert!(state.begin_commit().is_err());
        assert!(state.ensure_open().is_err());
    }

    #[test]
    fn commit_rejects_active_query_and_closes_state() {
        let state = Arc::new(CoherenceState::default());
        let _query = state.begin_query(0).unwrap();
        assert!(state.begin_commit().is_err());
        assert!(state.ensure_open().is_err());
    }

    #[test]
    fn commit_rejects_active_mutation_and_closes_state() {
        let state = Arc::new(CoherenceState::default());
        let _mutation = state.begin_mutation().unwrap();
        assert!(state.begin_commit().is_err());
        assert!(state.ensure_open().is_err());
    }

    #[tokio::test]
    async fn executing_provider_holds_query_guard() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap();
        let table: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
        let state = Arc::new(CoherenceState::default());
        let provider = coherent_table_provider(table, Some(state.clone()), Some(0));
        let context = SessionContext::new();
        let plan = provider
            .scan(&context.state(), None, &[], None)
            .await
            .unwrap();
        let stream = plan.execute(0, context.task_ctx()).unwrap();

        assert!(state.begin_commit().is_err());
        drop(stream);
    }
}
