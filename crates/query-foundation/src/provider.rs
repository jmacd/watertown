// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! DataFusion provider over exact immutable chunk membership.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, SortExpr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use object_store::ObjectStoreExt;

use crate::metrics::ChunkPruningMetrics;
use crate::snapshot::{ChunkDescriptor, DatasetSnapshot};
use crate::statistics::event_time_bounds;

/// Provider that prunes exact snapshot membership using conservative metadata.
#[derive(Debug)]
pub struct ChunkTableProvider {
    snapshot: DatasetSnapshot,
    metrics: Arc<ChunkPruningMetrics>,
}

impl ChunkTableProvider {
    /// Construct a provider over one immutable snapshot.
    #[must_use]
    pub fn new(snapshot: DatasetSnapshot, metrics: Arc<ChunkPruningMetrics>) -> Self {
        Self { snapshot, metrics }
    }

    /// Provider-level pruning counters.
    #[must_use]
    pub fn metrics(&self) -> &Arc<ChunkPruningMetrics> {
        &self.metrics
    }
}

#[async_trait]
impl TableProvider for ChunkTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(self.snapshot.schema())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let query_bounds = self
            .snapshot
            .event_time()
            .and_then(|contract| event_time_bounds(filters, contract.column()));
        let mut missing_statistics = 0_u64;
        let selected = self
            .snapshot
            .chunks()
            .iter()
            .filter(|chunk| match (query_bounds, chunk.event_time_bounds()) {
                (Some(bounds), Some(interval)) => bounds.retains(interval),
                (Some(_), None) => {
                    missing_statistics += 1;
                    true
                }
                (None, _) => true,
            })
            .collect::<Vec<_>>();
        self.metrics.record_scan(
            self.snapshot.chunks().len() as u64,
            selected.len() as u64,
            missing_statistics,
        );

        if selected.is_empty() {
            return datafusion::datasource::empty::EmptyTable::new(self.schema())
                .scan(state, projection, filters, limit)
                .await;
        }

        for chunk in &selected {
            let object = chunk.object().url();
            let store = state.runtime_env().object_store(object.object_store())?;
            _ = store.head(object.prefix()).await?;
        }

        let format = ParquetFormat::default().with_enable_pruning(true);
        let mut options = ListingOptions::new(Arc::new(format));
        if let Some(ordering) = common_ordering(&self.snapshot, &selected) {
            options = options.with_file_sort_order(vec![ordering]);
        }
        let objects = selected
            .iter()
            .map(|chunk| chunk.object().url().clone())
            .collect();
        let config = ListingTableConfig::new_with_multi_paths(objects)
            .with_listing_options(options)
            .with_schema(self.schema());
        ListingTable::try_new(config)?
            .scan(state, projection, filters, limit)
            .await
    }
}

fn common_ordering(
    snapshot: &DatasetSnapshot,
    selected: &[&ChunkDescriptor],
) -> Option<Vec<SortExpr>> {
    let first = selected.first()?.ordering();
    if first.is_empty()
        || selected
            .iter()
            .skip(1)
            .any(|chunk| chunk.ordering() != first)
    {
        return None;
    }
    if selected.len() == 1 {
        return Some(first.to_vec());
    }

    let event_time = snapshot.event_time()?.column();
    let leading = first.first()?;
    if !leading
        .expr
        .try_as_col()
        .is_some_and(|column| column.name == event_time)
    {
        return None;
    }
    let intervals = selected
        .iter()
        .map(|chunk| chunk.event_time_bounds())
        .collect::<Option<Vec<_>>>()?;
    let monotonic = intervals.windows(2).all(|pair| {
        if leading.asc {
            pair[0].max() <= pair[1].min()
        } else {
            pair[0].min() >= pair[1].max()
        }
    });
    monotonic.then(|| first.to_vec())
}
