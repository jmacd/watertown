// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! DataFusion provider over exact immutable chunk membership.

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;

use crate::metrics::ChunkPruningMetrics;
use crate::snapshot::DatasetSnapshot;
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
    fn as_any(&self) -> &dyn Any {
        self
    }

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
            .map(|chunk| chunk.object().url().clone())
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

        for object in &selected {
            let store = state.runtime_env().object_store(object.object_store())?;
            _ = store.head(object.prefix()).await?;
        }

        let format = ParquetFormat::default().with_enable_pruning(true);
        let options = ListingOptions::new(Arc::new(format));
        let config = ListingTableConfig::new_with_multi_paths(selected)
            .with_listing_options(options)
            .with_schema(self.schema());
        ListingTable::try_new(config)?
            .scan(state, projection, filters, limit)
            .await
    }
}
