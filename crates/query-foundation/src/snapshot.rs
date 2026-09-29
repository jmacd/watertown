// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Immutable query snapshot membership.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::Result;

/// Exact immutable Parquet object membership captured for one query snapshot.
#[derive(Clone, Debug)]
pub struct DatasetSnapshot {
    snapshot_id: Arc<str>,
    schema: SchemaRef,
    objects: Arc<[ListingTableUrl]>,
}

impl DatasetSnapshot {
    /// Capture an exact set of Parquet objects.
    #[must_use]
    pub fn new(
        snapshot_id: impl Into<Arc<str>>,
        schema: SchemaRef,
        objects: Vec<ListingTableUrl>,
    ) -> Self {
        Self {
            snapshot_id: snapshot_id.into(),
            schema,
            objects: objects.into(),
        }
    }

    /// Stable identity supplied by the snapshot publisher.
    #[must_use]
    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    /// Exact object URLs visible to this snapshot.
    #[must_use]
    pub fn objects(&self) -> &[ListingTableUrl] {
        &self.objects
    }

    /// Build a standard DataFusion Parquet listing table over exact membership.
    pub fn table_provider(&self) -> Result<Arc<dyn TableProvider>> {
        let format = ParquetFormat::default().with_enable_pruning(true);
        let options = ListingOptions::new(Arc::new(format));
        let config = ListingTableConfig::new_with_multi_paths(self.objects.to_vec())
            .with_listing_options(options)
            .with_schema(Arc::clone(&self.schema));
        Ok(Arc::new(ListingTable::try_new(config)?))
    }
}
