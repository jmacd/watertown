// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use super::{fail, partitions::PartitionSink, validate_path};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{pin::Pin, sync::Arc};
use tinyfs::{File, FileHandle, Metadata, QueryableFile, Result, ResultExt};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "documentation", derive(schemars::JsonSchema))]
pub struct AnalyticsTableConfig {
    /// Directory containing the committed rollup selection manifest.
    pub target: String,
}

struct AnalyticsTable(AnalyticsTableConfig);

#[async_trait]
impl File for AnalyticsTable {
    async fn async_reader(&self) -> Result<Pin<Box<dyn tinyfs::AsyncReadSeek>>> {
        Err(fail(
            "use typed queries or pond export to read analytics-table",
        ))
    }
    async fn async_writer(&self) -> Result<Pin<Box<dyn tinyfs::FileMetadataWriter>>> {
        Err(fail("analytics-table is read-only"))
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_queryable(&self) -> Option<&dyn QueryableFile> {
        Some(self)
    }
}

#[async_trait]
impl Metadata for AnalyticsTable {
    async fn metadata(&self) -> Result<tinyfs::NodeMetadata> {
        Ok(tinyfs::NodeMetadata {
            version: 1,
            size: None,
            blake3: None,
            bao_outboard: None,
            entry_type: tinyfs::EntryType::TableDynamic,
            timestamp: 0,
        })
    }
}

#[async_trait]
impl QueryableFile for AnalyticsTable {
    async fn as_table_provider(
        &self,
        _id: tinyfs::FileID,
        context: &tinyfs::ProviderContext,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        let sink = PartitionSink::new(context.clone(), self.0.target.clone());
        let manifest = sink
            .load()
            .await?
            .ok_or_else(|| fail("no rollup snapshot has been published"))?;
        sink.table(&manifest).await
    }
}

fn validate_config(bytes: &[u8]) -> Result<serde_json::Value> {
    let config: AnalyticsTableConfig = serde_yaml::from_slice(bytes).map_other()?;
    validate_path(&config.target)?;
    serde_json::to_value(config).map_other()
}

fn create(config: serde_json::Value, _context: crate::FactoryContext) -> Result<FileHandle> {
    let config: AnalyticsTableConfig = serde_json::from_value(config).map_other()?;
    validate_path(&config.target)?;
    Ok(FileHandle::new(Arc::new(tokio::sync::Mutex::new(
        Box::new(AnalyticsTable(config)),
    ))))
}

crate::register_dynamic_factory!(
    name: "analytics-table",
    description: "Query the currently selected hourly analytics partitions as one table",
    file: create,
    validate: validate_config
);
