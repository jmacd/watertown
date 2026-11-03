// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Immutable daily objects selected with progress by one transactional manifest.

use arrow::{
    array::{Array, StringArray, TimestampMicrosecondArray, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
};
use datafusion::{catalog::TableProvider, physical_plan::SendableRecordBatchStream};
use futures::StreamExt;
use query_foundation::{
    overlap::OverlapPolicy,
    snapshot::{ChunkDescriptor, DatasetSnapshot, EventTimeContract},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use tinyfs::{EntryType, ProviderContext, Result, ResultExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub use crate::site_analytics::Inventory;

pub const DAY_US: i64 = 86_400_000_000;
pub const HOUR_US: i64 = 3_600_000_000;
const MAX_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;

#[must_use]
pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("page_uri", DataType::Utf8, false),
        Field::new("page_views", DataType::UInt64, false),
    ]))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Partition {
    pub object: uuid::Uuid,
    pub rows: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub schema_version: u32,
    pub source_path: String,
    pub recipe_id: String,
    pub source_state_id: String,
    pub source: Inventory,
    pub published_at_us: i64,
    // Source membership does not establish collector uptime.
    pub collection_coverage: String,
    pub days: BTreeMap<i64, Partition>,
}

pub fn source_id(source: &Inventory) -> Result<String> {
    Ok(blake3::hash(&serde_json::to_vec(source).map_other()?)
        .to_hex()
        .to_string())
}

pub struct PartitionSink {
    context: ProviderContext,
    target: String,
}

impl PartitionSink {
    #[must_use]
    pub fn new(context: ProviderContext, target: String) -> Self {
        Self { context, target }
    }

    fn object_path(&self, object: uuid::Uuid) -> String {
        format!("{}/_objects/{object}.series", self.target)
    }

    pub async fn load(&self) -> Result<Option<Manifest>> {
        let root = self.context.filesystem().root().await?;
        let reader = match root
            .async_reader_path(format!("{}/manifest.json", self.target))
            .await
        {
            Ok(reader) => reader,
            Err(tinyfs::Error::NotFound(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        let mut bytes = Vec::new();
        let _ = reader
            .take(MAX_MANIFEST_BYTES + 1)
            .read_to_end(&mut bytes)
            .await
            .map_other()?;
        if bytes.len() as u64 > MAX_MANIFEST_BYTES {
            return Err(tinyfs::Error::Other(
                "analytics manifest exceeds supported bound".into(),
            ));
        }
        let manifest: Manifest = serde_json::from_slice(&bytes).map_other()?;
        validate_manifest(&manifest)?;
        Ok(Some(manifest))
    }

    pub async fn stage_day(
        &self,
        day: i64,
        mut stream: SendableRecordBatchStream,
    ) -> Result<Option<Partition>> {
        if day.rem_euclid(DAY_US) != 0 {
            return Err(tinyfs::Error::Other(
                "partition is not a UTC day boundary".into(),
            ));
        }
        let root = self.context.filesystem().root().await?;
        let object = uuid::Uuid::new_v4();
        let mut writer = None;
        let mut previous: Option<(i64, String)> = None;
        let mut rows = 0u64;
        while let Some(batch) = stream.next().await {
            let batch = batch.map_other()?;
            if batch.schema() != schema() {
                return Err(tinyfs::Error::Other(
                    "invalid analytics output schema".into(),
                ));
            }
            let times = batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("validated schema");
            let paths = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("validated schema");
            let counts = batch
                .column(2)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("validated schema");
            for row in 0..batch.num_rows() {
                let key = (times.value(row), paths.value(row).to_owned());
                if times.is_null(row)
                    || paths.is_null(row)
                    || counts.is_null(row)
                    || counts.value(row) == 0
                    || key.0.div_euclid(DAY_US) != day.div_euclid(DAY_US)
                    || key.0.rem_euclid(HOUR_US) != 0
                    || previous.as_ref().is_some_and(|prior| prior >= &key)
                {
                    return Err(tinyfs::Error::Other(
                        "invalid or duplicate hourly partition key/count".into(),
                    ));
                }
                previous = Some(key);
                rows += 1;
            }
            if batch.num_rows() == 0 {
                continue;
            }
            if writer.is_none() {
                let _ = root
                    .create_dir_all(format!("{}/_objects", self.target))
                    .await?;
                writer = Some(
                    tinyfs::arrow::parquet::StreamingSeriesWriter::try_new(
                        &root,
                        self.object_path(object),
                        schema(),
                        "timestamp",
                    )
                    .await?,
                );
            }
            writer
                .as_mut()
                .expect("initialized writer")
                .write(&batch)
                .await?;
        }
        if let Some(writer) = writer {
            let _ = writer.finish().await?;
            Ok(Some(Partition { object, rows }))
        } else {
            Ok(None)
        }
    }

    /// Call in the same pond transaction as staging; an error requires abort.
    pub async fn publish(&self, manifest: &Manifest) -> Result<()> {
        validate_manifest(manifest)?;
        let bytes = serde_json::to_vec(manifest).map_other()?;
        if bytes.len() as u64 > MAX_MANIFEST_BYTES {
            return Err(tinyfs::Error::Other(
                "analytics manifest exceeds supported bound".into(),
            ));
        }
        let root = self.context.filesystem().root().await?;
        let _ = root.create_dir_all(&self.target).await?;
        let mut writer = root
            .async_writer_path_with_type(
                format!("{}/manifest.json", self.target),
                EntryType::FilePhysicalVersion,
            )
            .await?;
        writer.write_all(&bytes).await.map_other()?;
        writer.shutdown().await.map_other()
    }

    pub async fn table(&self, manifest: &Manifest) -> Result<Arc<dyn TableProvider>> {
        validate_manifest(manifest)?;
        let coherence = self.context.persistence.coherence_state();
        let generation = coherence.as_ref().map(|state| state.generation());
        let root = self.context.filesystem().root().await?;
        let mut chunks = Vec::new();
        for (day, partition) in &manifest.days {
            let node = root
                .get_node_path(self.object_path(partition.object))
                .await?;
            let snapshot = crate::query_foundation_adapter::capture_tinyfs_snapshot(
                &self.context,
                node.id(),
                partition.object.to_string(),
                schema(),
                Some(EventTimeContract::new("timestamp")),
                OverlapPolicy::PreserveAll,
            )
            .await
            .map_other()?;
            if snapshot
                .snapshot()
                .chunks()
                .iter()
                .map(|chunk| chunk.logical_count())
                .sum::<u64>()
                != partition.rows
            {
                return Err(tinyfs::Error::Other(
                    "selected analytics partition changed".into(),
                ));
            }
            for chunk in snapshot.snapshot().chunks() {
                if chunk.event_time_bounds().is_none_or(|bounds| {
                    bounds.min().div_euclid(DAY_US) != day.div_euclid(DAY_US)
                        || bounds.max().div_euclid(DAY_US) != day.div_euclid(DAY_US)
                }) {
                    return Err(tinyfs::Error::Other(
                        "selected partition is outside its UTC day".into(),
                    ));
                }
                chunks.push(ChunkDescriptor::new(
                    format!("{}:{}", partition.object, chunk.chunk_id()),
                    chunks.len() as u64,
                    chunk.object().clone(),
                    chunk.schema().clone(),
                    chunk.logical_count(),
                    chunk.event_time_bounds(),
                ));
            }
        }
        let snapshot = DatasetSnapshot::try_new(
            source_id(&manifest.source)?,
            schema(),
            chunks,
            Some(EventTimeContract::new("timestamp")),
            OverlapPolicy::PreserveAll,
        )
        .map_other()?;
        Ok(tinyfs::coherent_table_provider(
            snapshot.table_provider().map_other()?,
            coherence,
            generation,
        ))
    }
}

fn validate_manifest(manifest: &Manifest) -> Result<()> {
    if manifest.schema_version != 1
        || manifest.recipe_id.is_empty()
        || manifest.collection_coverage != "unknown"
        || manifest.source_state_id != source_id(&manifest.source)?
        || manifest
            .days
            .iter()
            .any(|(day, partition)| day.rem_euclid(DAY_US) != 0 || partition.rows == 0)
    {
        return Err(tinyfs::Error::Other(
            "invalid analytics selection/progress manifest".into(),
        ));
    }
    let bytes = serde_json::to_vec(&manifest.source).map_other()?;
    let _ = Inventory::parse(&bytes, &manifest.source.site_id).map_other()?;
    let objects: std::collections::BTreeSet<_> = manifest.days.values().map(|p| p.object).collect();
    if objects.len() != manifest.days.len() {
        return Err(tinyfs::Error::Other(
            "analytics manifest selects a partition twice".into(),
        ));
    }
    Ok(())
}
