// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

mod client;
mod models;

// Factory registration for TLogFS dynamic nodes
pub mod factory;

pub use crate::client::Client;
pub use crate::models::{HydroVuConfig, HydroVuDevice};

use crate::models::{Names, WideRecord};
use anyhow::{Context, Result, anyhow};
use arrow_array::builder::{Float64Builder, TimestampSecondBuilder};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, TimeUnit};
use chrono::{DateTime, SecondsFormat, Utc};
use log::debug;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tinyfs::FS;

/// Summary of collection for a single device
#[derive(Clone, Debug)]
pub struct DeviceSummary {
    pub device_id: i64,
    pub device_name: String,
    pub records_collected: usize,
    pub start_timestamp: i64,
    pub final_timestamp: i64,
}

/// Result of data collection operation
pub struct CollectionResult {
    pub records_collected: usize,
    pub final_timestamps: HashMap<i64, i64>,
    pub device_summaries: Vec<DeviceSummary>,
}

/// Result of single device collection
struct DeviceCollectionResult {
    records_collected: usize,
    start_timestamp: i64,
    final_timestamp: Option<i64>,
}

/// Convert timestamp in **microseconds** (Watertown canonical unit) to RFC3339 date string
pub fn utc2date(utc_micros: i64) -> Result<String> {
    let seconds = utc_micros / 1_000_000;
    Ok(DateTime::from_timestamp(seconds, 0)
        .ok_or_else(|| anyhow::anyhow!("cannot convert timestamp {} to date", utc_micros))?
        .to_rfc3339_opts(SecondsFormat::Secs, true))
}

/// Main HydroVu data collector
pub struct HydroVuCollector {
    config: HydroVuConfig,
    client: Client,
    names: Names,
}

fn getenv(name: &str) -> Result<String> {
    env::var(name).map_err(|e| anyhow!("missing env {}: {}", name, e))
}

pub fn get_key() -> Result<(String, String)> {
    let key_id = getenv("HYDRO_KEY_ID")?;
    let key_val = getenv("HYDRO_KEY_VALUE")?;
    Ok((key_id, key_val))
}

/// Archive active series files by compacting them to single-version archives.
///
/// For each device, finds the active multi-version series file, reads all versions
/// through a DataFusion table provider (which merges them into a single logical
/// table), streams the merged data through an ArrowWriter to produce a compact
/// single-version parquet file, writes it as a new archive entry, and removes
/// the old multi-version entry.
///
/// Returns Vec of (new_pond_path, old_name) for the export script.
pub async fn archive_devices(
    fs: &FS,
    config: &HydroVuConfig,
    device_filter: Option<&str>,
    provider_ctx: &tinyfs::ProviderContext,
) -> Result<Vec<(String, String)>> {
    let root = fs.root().await?;
    let today = Utc::now().format("%Y%m%d").to_string();
    let mut archived = Vec::new();

    for device in &config.devices {
        // Apply device name filter if provided
        if let Some(filter) = device_filter
            && device.name != filter
        {
            continue;
        }

        let device_dir_path = format!("{}/devices/{}", config.hydrovu_path, device.id);

        // Navigate to the device directory
        let device_dir = match root.open_dir_path(&device_dir_path).await {
            Ok(dir) => dir,
            Err(e) => {
                log::warn!(
                    "Skipping device {} ({}): directory not found: {}",
                    device.name,
                    device.id,
                    e
                );
                continue;
            }
        };

        // Try active-style name first, then legacy name
        let active_name = format!("{}_active.series", device.name);
        let legacy_name = format!("{}.series", device.name);

        let old_name = if device_dir.get(&active_name).await?.is_some() {
            active_name
        } else if device_dir.get(&legacy_name).await?.is_some() {
            legacy_name
        } else {
            log::info!(
                "Skipping device {} ({}): no active or legacy series file found",
                device.name,
                device.id
            );
            continue;
        };

        let new_name = format!("{}_archive_{}.series", device.name, today);

        // Check that the archive name doesn't already exist
        if device_dir.get(&new_name).await?.is_some() {
            log::warn!(
                "Skipping device {} ({}): archive file {} already exists",
                device.name,
                device.id,
                new_name
            );
            continue;
        }

        let old_path = format!("{}/{}", device_dir_path, old_name);
        let new_path = format!("{}/{}", device_dir_path, new_name);

        log::info!(
            "Compacting device {} ({}): {} -> {}",
            device.name,
            device.id,
            old_name,
            new_name,
        );

        match compact_series(&root, &old_path, &new_path, provider_ctx).await? {
            Some((min_time, max_time)) => {
                log::info!("  Compacted: temporal bounds {} .. {}", min_time, max_time);

                // Remove the old multi-version entry
                device_dir
                    .remove_entry(&old_name)
                    .await
                    .map_err(|e| anyhow!("Failed to remove old entry '{}': {}", old_name, e))?;

                archived.push((new_path, old_name));
            }
            None => {
                log::info!(
                    "Skipping device {} ({}): series '{}' is empty",
                    device.name,
                    device.id,
                    old_name
                );
            }
        }
    }

    Ok(archived)
}

/// Compact a multi-version series into a single-version parquet file.
///
/// Resolves the source file, reads all versions merged through DataFusion,
/// and writes the result as a new single-version series at `new_path`.
///
/// Returns `Ok(Some((min, max)))` on success, `Ok(None)` if the source is empty.
async fn compact_series(
    root: &tinyfs::WD,
    old_path: &str,
    new_path: &str,
    provider_ctx: &tinyfs::ProviderContext,
) -> Result<Option<(i64, i64)>> {
    use tinyfs::Lookup;

    // Resolve the source file to get a table provider
    let (_, lookup) = root
        .resolve_path(old_path)
        .await
        .map_err(|e| anyhow!("Failed to resolve '{}': {}", old_path, e))?;

    let node_path = match lookup {
        Lookup::Found(np) => np,
        _ => return Err(anyhow!("Could not resolve series file '{}'", old_path)),
    };

    let file_handle = node_path
        .as_file()
        .await
        .map_err(|e| anyhow!("Failed to open file '{}': {}", old_path, e))?;

    let file_arc = file_handle.handle.get_file().await;
    let file_guard = file_arc.lock().await;

    let queryable = file_guard
        .as_queryable()
        .ok_or_else(|| anyhow!("File '{}' is not queryable", old_path))?;

    let table_provider = queryable
        .as_table_provider(node_path.id(), provider_ctx)
        .await
        .map_err(|e| anyhow!("Failed to get table provider for '{}': {}", old_path, e))?;

    drop(file_guard);

    // Register in DataFusion, execute query, always deregister on exit
    let ctx = &provider_ctx.datafusion_session;
    let table_ref = datafusion::common::TableReference::bare("_compact");

    _ = ctx
        .register_table(table_ref.clone(), table_provider)
        .map_err(|e| anyhow!("Failed to register table: {}", e))?;

    let result = run_compaction_query(root, new_path, ctx).await;

    // Always clean up the registration, even on error
    let _ = ctx.deregister_table(table_ref);

    result
}

/// Execute the compaction SQL and stream the result into a new series file.
///
/// Returns `Ok(None)` if the source produced no data.
async fn run_compaction_query(
    root: &tinyfs::WD,
    new_path: &str,
    ctx: &datafusion::execution::context::SessionContext,
) -> Result<Option<(i64, i64)>> {
    use futures::stream::StreamExt;
    use tinyfs::arrow::ParquetExt;

    let df = ctx
        .sql("SELECT * FROM _compact ORDER BY timestamp")
        .await
        .map_err(|e| anyhow!("SQL error: {}", e))?;

    let mut stream = df
        .execute_stream()
        .await
        .map_err(|e| anyhow!("Stream error: {}", e))?;

    // Peek first batch -- return None for empty series
    let first_batch = match stream.next().await {
        Some(Ok(batch)) if batch.num_rows() > 0 => batch,
        Some(Err(e)) => return Err(anyhow!("Read error: {}", e)),
        _ => return Ok(None),
    };

    // Reconstruct a full stream with the peeked batch prepended
    let schema = first_batch.schema();
    let combined = futures::stream::once(async move {
        Ok(first_batch) as std::result::Result<RecordBatch, datafusion::common::DataFusionError>
    })
    .chain(stream);
    let combined_stream = Box::pin(
        datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, combined),
    );

    let (min_time, max_time) = root
        .create_series_from_stream(new_path, combined_stream, Some("timestamp"))
        .await
        .map_err(|e| anyhow!("Failed to write compacted series '{}': {}", new_path, e))?;

    Ok(Some((min_time, max_time)))
}

impl HydroVuCollector {
    /// Create a new HydroVu collector
    pub async fn new(config: HydroVuConfig) -> Result<Self> {
        debug!("Creating HydroVu collector");

        let (key_id, key_val) = get_key()?;

        // Create HydroVu API client
        let client = Client::new(key_id, key_val)
            .await
            .with_context(|| "Failed to create HydroVu API client")?;

        // Initialize empty names dictionary (will be updated on first collection)
        let names = Names {
            parameters: BTreeMap::new(),
            units: BTreeMap::new(),
        };

        Ok(Self {
            config,
            client,
            names,
        })
    }

    /// Core data collection function
    /// Works within a single transaction - caller manages transaction lifecycle
    pub async fn collect_data(&mut self, fs: &FS) -> Result<CollectionResult> {
        let device_count = self.config.devices.len();
        debug!("Starting HydroVu data collection for {device_count} devices");

        // Update dictionaries if needed
        debug!("Updating parameter and unit dictionaries");
        self.update_dictionaries().await?;

        let mut total_records = 0;
        let mut final_timestamps = HashMap::new();
        let mut device_summaries = Vec::new();

        for device in self.config.devices.clone() {
            if !device.active {
                debug!(
                    "Skipping inactive device '{}' (ID: {})",
                    device.name, device.id
                );
                continue;
            }
            let result = self.collect_device(&device, fs).await?;
            total_records += result.records_collected;

            if let Some(final_ts) = result.final_timestamp
                && final_ts > 0
            {
                _ = final_timestamps.insert(device.id, final_ts);
            }

            // Create summary for this device
            device_summaries.push(DeviceSummary {
                device_id: device.id,
                device_name: device.name.clone(),
                records_collected: result.records_collected,
                start_timestamp: result.start_timestamp,
                final_timestamp: result.final_timestamp.unwrap_or(result.start_timestamp),
            });
        }

        // Report final results
        debug!("HydroVu data collection completed");

        Ok(CollectionResult {
            records_collected: total_records,
            final_timestamps,
            device_summaries,
        })
    }

    async fn collect_device(
        &mut self,
        device: &HydroVuDevice,
        fs: &FS,
    ) -> Result<DeviceCollectionResult> {
        let device_id = device.id;
        let device_name = device.name.clone();
        debug!("Processing device {device_id}");

        let device = device.clone();
        let client = self.client.clone();
        let names = self.names.clone();
        let hydrovu_path = self.config.hydrovu_path.clone();
        let archive_path = self.config.archive_path.clone();
        let max_points = self.config.max_points_per_run;

        // Call internal collection function directly - no sub-transaction needed
        // We're already running within the caller's single transaction
        // Only one fetch per run (max_points_per_run enforced per run)
        let result = Self::collect_device_data_internal(
            fs,
            hydrovu_path,
            archive_path,
            client,
            names,
            device,
            max_points,
        )
        .await
        .map_err(|e| {
            anyhow!(
                "Failed to collect data for device '{}' (ID: {}): {}",
                device_name,
                device_id,
                e
            )
        })?;

        let total_records = result.0;
        let start_timestamp = result.1;
        let final_timestamp = if result.2 > 0 { Some(result.2) } else { None };

        if total_records == 0 {
            debug!("No more data available for device {device_id}");
        }

        Ok(DeviceCollectionResult {
            records_collected: total_records,
            start_timestamp,
            final_timestamp,
        })
    }

    /// Update parameter and unit dictionaries from HydroVu API
    /// This is called automatically before each collection run
    async fn update_dictionaries(&mut self) -> Result<()> {
        debug!("Updating HydroVu parameter and unit dictionaries");
        let names = self
            .client
            .fetch_names()
            .await
            .with_context(|| "Failed to update parameter and unit dictionaries")?;
        self.names = names;
        debug!("Successfully updated dictionaries");
        Ok(())
    }

    /// Find the youngest (most recent) timestamp (in microseconds, the Watertown
    /// canonical unit) for a device by scanning ALL *.series files in the
    /// writeable device directory via the oplog temporal metadata.
    ///
    /// When the writeable directory has no temporal data (a freshly reset pond)
    /// and an `archive_path` is configured, fall back to the read-only seed
    /// archives at `{archive_path}/devices/{device_id}/*.series`.  Those are
    /// git-ingested `FileDynamic` Parquet snapshots with no oplog metadata, so
    /// they are queried directly with a `series://` cast + `SELECT max(timestamp)`.
    /// This lets live collection resume from the archive's youngest point instead
    /// of crawling from epoch, without copying the archives into the oplog.
    async fn find_youngest_timestamp(
        fs: &FS,
        hydrovu_path: &str,
        archive_path: Option<&str>,
        device_id: i64,
        device_name: &str,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let device_dir_path = format!("{}/devices/{}", hydrovu_path, device_id);

        let root_wd = fs
            .root()
            .await
            .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        // Scan the writeable device directory's oplog metadata for max timestamp.
        let mut max_timestamp: Option<i64> = None;
        if let Ok(device_dir) = root_wd.open_dir_path(&device_dir_path).await {
            use futures::StreamExt;
            let mut entries_stream = device_dir
                .entries()
                .await
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

            let mut series_paths = Vec::new();
            while let Some(entry) = entries_stream.next().await {
                let entry = entry
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                if entry.name.ends_with(".series") && entry.entry_type.is_file() {
                    series_paths.push(format!("{}/{}", device_dir_path, entry.name));
                }
            }

            debug!(
                "Found {} writeable .series files for device {device_id}: {:?}",
                series_paths.len(),
                series_paths
            );

            for series_path in &series_paths {
                let version_infos = root_wd.list_file_versions(series_path).await.map_err(|e| {
                    steward::StewardError::Dyn(
                        format!("Failed to query file versions for {}: {}", series_path, e).into(),
                    )
                })?;

                for (i, version_info) in version_infos.iter().enumerate() {
                    if let Some(metadata) = &version_info.extended_metadata
                        && let (Some(min_str), Some(max_str)) = (
                            metadata.get("min_event_time"),
                            metadata.get("max_event_time"),
                        )
                        && let (Ok(min_time), Ok(max_time)) =
                            (min_str.parse::<i64>(), max_str.parse::<i64>())
                    {
                        debug!(
                            "{} version {i}: temporal range {min_time}..{max_time}",
                            series_path
                        );
                        max_timestamp =
                            Some(max_timestamp.map_or(max_time, |current| current.max(max_time)));
                    }
                }
            }
        } else {
            debug!("Writeable device directory not found for device {device_id}");
        }

        // Writeable data present: resume from its youngest timestamp.
        if let Some(timestamp) = max_timestamp {
            // Add 1 second (1M microseconds) to avoid duplicate when API uses second resolution
            let next_timestamp = timestamp + 1_000_000;
            debug!(
                "Found youngest timestamp {timestamp} across writeable series for device {device_id}, will continue from {next_timestamp}"
            );
            return Ok(next_timestamp);
        }

        // Writeable directory empty: fall back to the read-only seed archives.
        if let Some(archive_path) = archive_path {
            if let Some(archive_micros) =
                Self::find_archive_youngest_micros(fs, archive_path, device_id).await?
            {
                let next_timestamp = archive_micros + 1_000_000;
                debug!(
                    "Device {device_id}: no writeable data; resuming from archive youngest {archive_micros}, will continue from {next_timestamp}"
                );
                return Ok(next_timestamp);
            }

            // An archive_path is configured precisely so a freshly reset pond
            // resumes from the seed archives produced by the hydrovu factory.
            // A missing resume point means this device's seed archive is absent
            // or unreadable.  Crawling HydroVu from epoch issues an unbounded
            // query that times out the server, so fail hard and surface the
            // misconfiguration instead of silently degrading.
            return Err(steward::StewardError::Dyn(
                format!(
                    "Device {device_id} ({device_name}): no writeable data and no resume point in seed archive '{archive_path}/devices/{device_id}'; refusing to crawl HydroVu from epoch"
                )
                .into(),
            )
            .into());
        }

        debug!(
            "Device {device_id}: no writeable data and no archive_path configured, starting from epoch"
        );
        Ok(0)
    }

    /// Query the read-only seed archives for a device's youngest timestamp,
    /// returned in microseconds (Watertown canonical unit).  Reads every
    /// `{archive_path}/devices/{device_id}/*.series` file via a `series://`
    /// cast (the same Provider builtin path used by `timeseries-join`) and
    /// takes `max(timestamp)` across all.  No new file-access mechanism: the
    /// cast flows through the shared `read_pond_node_as_parquet` helper in the
    /// provider crate.  TODO(perf): that helper currently materializes whole
    /// files in memory; the right fix is in the provider cache, not here.
    ///
    /// Returns `Ok(None)` when no archive files exist for the device.
    async fn find_archive_youngest_micros(
        fs: &FS,
        archive_path: &str,
        device_id: i64,
    ) -> Result<Option<i64>, Box<dyn std::error::Error + Send + Sync>> {
        use datafusion::prelude::SessionContext;
        use futures::StreamExt;

        let archive_dir_path = format!("{}/devices/{}", archive_path, device_id);

        let root_wd = fs
            .root()
            .await
            .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        let archive_dir = match root_wd.open_dir_path(&archive_dir_path).await {
            Ok(dir) => dir,
            Err(_) => {
                debug!("No archive directory '{archive_dir_path}' for device {device_id}");
                return Ok(None);
            }
        };

        let mut archive_files = Vec::new();
        let mut entries_stream = archive_dir
            .entries()
            .await
            .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
        while let Some(entry) = entries_stream.next().await {
            let entry = entry
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            if entry.name.ends_with(".series") && entry.entry_type.is_file() {
                archive_files.push(format!("{}/{}", archive_dir_path, entry.name));
            }
        }

        if archive_files.is_empty() {
            debug!("No archive .series files for device {device_id} in '{archive_dir_path}'");
            return Ok(None);
        }

        let provider = provider::Provider::new(Arc::new(fs.clone()));
        let ctx = SessionContext::new();
        let mut max_micros: Option<i64> = None;

        for (i, file_path) in archive_files.iter().enumerate() {
            // Reuse the canonical builtin `+series` cast: same Provider entry
            // point used by `timeseries-join` for git-ingested archives.  No
            // separate file-access path lives in this crate.
            let url = format!("series://{}", file_path);
            let table = provider
                .create_table_provider(&url, &ctx)
                .await
                .map_err(|e| {
                    steward::StewardError::Dyn(
                        format!("Failed to open archive '{}' as series: {}", file_path, e).into(),
                    )
                })?;
            let table_name = format!("_archive_{}", i);
            let _ = ctx.register_table(&table_name, table).map_err(|e| {
                steward::StewardError::Dyn(
                    format!(
                        "Failed to register archive table for '{}': {}",
                        file_path, e
                    )
                    .into(),
                )
            })?;

            let batches = ctx
                .sql(&format!(
                    "SELECT max(timestamp) AS m FROM \"{}\"",
                    table_name
                ))
                .await
                .map_err(|e| {
                    steward::StewardError::Dyn(
                        format!("Failed to query max(timestamp) for '{}': {}", file_path, e).into(),
                    )
                })?
                .collect()
                .await
                .map_err(|e| {
                    steward::StewardError::Dyn(
                        format!(
                            "Failed to collect max(timestamp) for '{}': {}",
                            file_path, e
                        )
                        .into(),
                    )
                })?;
            let _ = ctx.deregister_table(&table_name);

            if let Some(micros) = Self::scalar_timestamp_to_micros(&batches)? {
                debug!(
                    "Archive '{}' max(timestamp) -> {} micros",
                    file_path, micros
                );
                max_micros = Some(max_micros.map_or(micros, |cur| cur.max(micros)));
            }
        }

        Ok(max_micros)
    }

    /// Extract a single `max(timestamp)` scalar from a query result and
    /// normalize it to microseconds (Watertown canonical unit).
    ///
    /// HydroVu archives are produced by the hydrovu factory itself, so the
    /// timestamp column type is known.  Unsupported Arrow types hard-fail
    /// rather than silently degrade -- a type mismatch indicates a real
    /// regression that should not be hidden.  The seed archives in production
    /// use INT64 seconds; collector-written active series use
    /// `Timestamp(Second)`.  Other logical Timestamp units are accepted and
    /// scaled per their TimeUnit.
    fn scalar_timestamp_to_micros(
        batches: &[RecordBatch],
    ) -> Result<Option<i64>, Box<dyn std::error::Error + Send + Sync>> {
        use arrow_array::{
            Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
            TimestampNanosecondArray, TimestampSecondArray,
        };

        let Some(batch) = batches.iter().find(|b| b.num_rows() > 0) else {
            return Ok(None);
        };
        let column = batch.column(0);
        if column.is_null(0) {
            return Ok(None);
        }

        let micros = match column.data_type() {
            DataType::Int64 => {
                let arr = column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        steward::StewardError::Dyn(
                            "max(timestamp) result claims Int64 but downcast failed".into(),
                        )
                    })?;
                arr.value(0) * 1_000_000
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                let arr = column
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| {
                        steward::StewardError::Dyn(
                            "max(timestamp) result claims Timestamp(Second) but downcast failed"
                                .into(),
                        )
                    })?;
                arr.value(0) * 1_000_000
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                let arr = column
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        steward::StewardError::Dyn(
                            "max(timestamp) result claims Timestamp(Millisecond) but downcast failed"
                                .into(),
                        )
                    })?;
                arr.value(0) * 1_000
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let arr = column
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        steward::StewardError::Dyn(
                            "max(timestamp) result claims Timestamp(Microsecond) but downcast failed"
                                .into(),
                        )
                    })?;
                arr.value(0)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let arr = column
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        steward::StewardError::Dyn(
                            "max(timestamp) result claims Timestamp(Nanosecond) but downcast failed"
                                .into(),
                        )
                    })?;
                arr.value(0) / 1_000
            }
            other => {
                return Err(steward::StewardError::Dyn(
                    format!(
                        "Unsupported archive max(timestamp) type {:?}; expected INT64 (seconds) or a logical Timestamp",
                        other
                    )
                    .into(),
                )
                .into());
            }
        };

        Ok(Some(micros))
    }

    /// Core device data collection function - handles both counting and timestamp tracking
    /// Works within caller's transaction - no sub-transactions
    /// Returns (records_collected, start_timestamp, final_timestamp)
    async fn collect_device_data_internal(
        fs: &FS,
        hydrovu_path: String,
        archive_path: Option<String>,
        client: Client,
        names: Names,
        device: HydroVuDevice,
        max_points_per_run: usize,
    ) -> Result<(usize, i64, i64), Box<dyn std::error::Error + Send + Sync>> {
        let device_id = device.id;
        debug!("Starting data collection for device {device_id}");

        // Step 1: Find the youngest timestamp using TinyFS metadata API
        let youngest_timestamp = Self::find_youngest_timestamp(
            fs,
            &hydrovu_path,
            archive_path.as_deref(),
            device_id,
            &device.name,
        )
        .await?;

        let start_date =
            utc2date(youngest_timestamp).unwrap_or_else(|_| "invalid date".to_string());
        debug!(
            "Device {device_id} collection starting from timestamp: {youngest_timestamp} ({start_date})"
        );

        // Get root working directory for file operations
        let root_wd = fs
            .root()
            .await
            .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        // Step 2: Fetch data from API within the transaction with row limit
        let location_readings = client
            .fetch_location_data(device_id, youngest_timestamp, max_points_per_run)
            .await
            .map_err(|e| {
                // Concise error - detailed diagnostics already printed to stderr by client
                steward::StewardError::DataInit(tlogfs::TLogFSError::Io(std::io::Error::other(
                    format!(
                        "HydroVu API failed for device '{}' (location {}): {}",
                        device.name, device_id, e
                    ),
                )))
            })?;

        // Convert to wide records
        let wide_records =
            WideRecord::from_location_readings(&location_readings, &names.units, &names.parameters)
                .map_err(|e| {
                    steward::StewardError::DataInit(tlogfs::TLogFSError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Invalid timestamp in API response: {e}"),
                    )))
                })?;

        if wide_records.is_empty() {
            debug!("No new records for device {device_id}");
            return Ok((0, youngest_timestamp, youngest_timestamp));
        }

        let count = wide_records.len();
        debug!("Fetched {count} new records from API (client handled row limiting)");

        // Track final timestamp if needed (in microseconds -- Watertown canonical unit)
        let mut final_timestamp = youngest_timestamp;
        if !wide_records.is_empty() {
            let oldest_timestamp = wide_records
                .iter()
                .map(|r| r.timestamp.timestamp() * 1_000_000)
                .min()
                .unwrap_or(0);
            let newest_timestamp = wide_records
                .iter()
                .map(|r| r.timestamp.timestamp() * 1_000_000)
                .max()
                .unwrap_or(0);
            let oldest_date =
                utc2date(oldest_timestamp).unwrap_or_else(|_| "invalid date".to_string());
            let newest_date =
                utc2date(newest_timestamp).unwrap_or_else(|_| "invalid date".to_string());
            debug!(
                "Device {device_id} collected data from {oldest_timestamp} ({oldest_date}) to {newest_timestamp} ({newest_date}) ({count} records)"
            );
            final_timestamp = newest_timestamp;
        }

        // Step 3: Store data in filesystem within same transaction
        let schema = HydroVuCollector::create_arrow_schema_from_wide_records(&wide_records)
            .map_err(|e| {
                steward::StewardError::DataInit(tlogfs::TLogFSError::Io(std::io::Error::other(
                    format!("Schema error: {e}"),
                )))
            })?;

        let record_count = wide_records.len();
        debug!("Converting {record_count} records to Arrow format");

        // Convert WideRecord batch to Arrow RecordBatch
        let record_batch =
            HydroVuCollector::convert_wide_records_to_arrow(&wide_records, &schema, device_id)
                .map_err(|e| {
                    steward::StewardError::DataInit(tlogfs::TLogFSError::Io(std::io::Error::other(
                        e.to_string(),
                    )))
                })?;

        // Use TinyFS's clean API: pass RecordBatch, let TinyFS handle parquet conversion and metadata
        // write_series_from_batch handles both create (first run) and append (subsequent runs)
        let device_path = format!(
            "{hydrovu_path}/devices/{device_id}/{}_active.series",
            device.name
        );
        use tinyfs::arrow::ParquetExt;
        let (_min_ts, _max_ts) = root_wd
            .write_series_from_batch(&device_path, &record_batch, Some("timestamp"))
            .await
            .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        debug!("Processed {count} records for device {device_id}");

        // Log completion with timestamp range for next run
        if !wide_records.is_empty() {
            let next_start_timestamp = final_timestamp + 1_000_000;
            let next_date =
                utc2date(next_start_timestamp).unwrap_or_else(|_| "invalid date".to_string());
            debug!(
                "Device {device_id} collection completed. Next run will start from timestamp: {next_start_timestamp} ({next_date})"
            );
        }

        Ok((count, youngest_timestamp, final_timestamp))
    }

    /// Create Arrow schema from WideRecord data
    fn create_arrow_schema_from_wide_records(
        records: &[WideRecord],
    ) -> Result<arrow_schema::Schema> {
        if records.is_empty() {
            // Return empty schema with just timestamp field
            let fields = vec![Arc::new(Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())),
                false,
            ))];
            return Ok(arrow_schema::Schema::new(fields));
        }

        // Collect all unique parameter names from the records
        let mut all_parameters = BTreeSet::new();
        for record in records {
            all_parameters.extend(record.parameters.keys().cloned());
        }

        // Build schema with timestamp field + parameter fields
        let mut fields = Vec::new();

        // Add timestamp field first (matching the base schema format)
        fields.push(Arc::new(Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())),
            false,
        )));

        // Add parameter fields (sorted for consistency)
        for param_name in all_parameters {
            fields.push(Arc::new(Field::new(param_name, DataType::Float64, true)));
        }

        let schema = arrow_schema::Schema::new(fields);
        Ok(schema)
    }

    /// Convert WideRecord batch to Arrow RecordBatch
    fn convert_wide_records_to_arrow(
        records: &[WideRecord],
        schema: &arrow_schema::Schema,
        device_id: i64, // Used for logging only, not stored in data
    ) -> Result<RecordBatch> {
        if records.is_empty() {
            return Err(anyhow::anyhow!("Cannot convert empty records to Arrow"));
        }

        let num_records = records.len();

        // Build timestamp column
        let mut timestamp_builder = TimestampSecondBuilder::new();

        // Build parameter columns - collect all parameter names from schema
        let mut param_builders: HashMap<String, Float64Builder> = HashMap::new();
        for field in schema.fields() {
            let field_name = field.name();
            // Skip the timestamp field
            if field_name != "timestamp" {
                _ = param_builders.insert(field_name.clone(), Float64Builder::new());
            }
        }

        // Fill builders with data
        for record in records {
            timestamp_builder.append_value(record.timestamp.timestamp());

            // Fill parameter columns (nullable - Some values may not exist for all timestamps)
            for (param_name, builder) in param_builders.iter_mut() {
                if let Some(value) = record.parameters.get(param_name) {
                    match value {
                        Some(v) => builder.append_value(*v),
                        None => builder.append_null(),
                    }
                } else {
                    builder.append_null();
                }
            }
        }

        // Finalize arrays in the exact order of schema fields
        let mut arrays: Vec<Arc<dyn Array>> = Vec::new();

        // Create arrays in schema field order
        for field in schema.fields() {
            let field_name = field.name();
            if field_name == "timestamp" {
                // Add timestamp array - match the schema's timezone format
                arrays.push(Arc::new(
                    timestamp_builder.finish().with_timezone_opt(Some("+00:00")),
                ));
            } else {
                // Add parameter array
                if let Some(mut builder) = param_builders.remove(field_name) {
                    // Parameter has data in this batch
                    arrays.push(Arc::new(builder.finish()));
                } else {
                    // Parameter exists in schema but has no data in this batch - create null array
                    let mut null_builder = Float64Builder::new();
                    for _ in 0..num_records {
                        null_builder.append_null();
                    }
                    arrays.push(Arc::new(null_builder.finish()));
                }
            }
        }

        let schema_fields = schema.fields().len();
        let array_count = arrays.len();
        let schema_field_names: Vec<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        let array_lengths: Vec<usize> = arrays.iter().map(|a| a.len()).collect();

        let record_batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays)
            .map_err(|e| {
                debug!("Arrow error details: {e}");
                e
            })
            .with_context(|| {
                format!(
                    "Failed to create Arrow RecordBatch for device {}: schema has {} fields {:?}, but {} arrays provided with lengths {:?}", 
                     device_id, schema_fields, schema_field_names, array_count, array_lengths
                )
            })?;

        let batch_rows = record_batch.num_rows();
        debug!("Created Arrow RecordBatch with {batch_rows} rows");
        Ok(record_batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::RecordBatch;
    use arrow_array::builder::{Float64Builder, TimestampSecondBuilder};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tinyfs::arrow::ParquetExt;

    /// Test that simulates HydroVu collector pattern: write twice, check for duplicate versions
    #[tokio::test]
    async fn test_no_duplicate_versions_on_multiple_writes() -> Result<()> {
        // Setup
        let temp_dir = TempDir::new()?;
        let store_path = temp_dir.path().join("test_pond");

        // Initialize pond (creates root directory)
        let mut persistence = tlogfs::OpLogPersistence::create(
            store_path.to_str().unwrap(),
            uuid7::uuid7().to_string(),
            tlogfs::PondUserMetadata::new(vec!["test_init".to_string()]),
        )
        .await?;

        let txn_meta = tlogfs::PondTxnMetadata::new(
            2, // txn_seq=2 because root init used txn_seq=1
            tlogfs::PondUserMetadata::new(vec!["test_duplicate_versions".to_string()]),
        );
        let tx = persistence.begin_write(&txn_meta).await?;

        // TransactionGuard derefs to FS, so we can use it directly
        let root = tx.root().await?;

        // Create the /hydrovu directory
        let _ = root.create_dir_path("/hydrovu").await?;

        // Create test data path
        let test_path = "/hydrovu/test_device.series";

        // Create first batch (simulating first HydroVu run)
        let schema1 = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("temperature", DataType::Float64, true),
        ]));

        let mut ts_builder1 = TimestampSecondBuilder::new();
        let mut temp_builder1 = Float64Builder::new();

        // Add 3 records
        for i in 0..3 {
            ts_builder1.append_value(1000000 + i);
            temp_builder1.append_value(20.0 + i as f64);
        }

        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![
                Arc::new(ts_builder1.finish()),
                Arc::new(temp_builder1.finish()),
            ],
        )?;

        // Write first batch (creates version 1)
        debug!("Writing first batch...");
        let _ = root
            .write_series_from_batch(test_path, &batch1, Some("timestamp"))
            .await?;

        // Create second batch (simulating second HydroVu run)
        let mut ts_builder2 = TimestampSecondBuilder::new();
        let mut temp_builder2 = Float64Builder::new();

        // Add 3 more records
        for i in 3..6 {
            ts_builder2.append_value(1000000 + i);
            temp_builder2.append_value(20.0 + i as f64);
        }

        let batch2 = RecordBatch::try_new(
            schema1.clone(),
            vec![
                Arc::new(ts_builder2.finish()),
                Arc::new(temp_builder2.finish()),
            ],
        )?;

        // Write second batch (should create version 2)
        debug!("Writing second batch...");
        let _ = root
            .write_series_from_batch(test_path, &batch2, Some("timestamp"))
            .await?;

        // Commit transaction (txn_seq=2)
        let _ = tx.commit().await?;

        // Now check versions using list_file_versions
        // Read transactions use last_write_sequence (which is 2 after the commit above)
        let pond_id = persistence.pond_id().to_string();
        let mut persistence2 =
            tlogfs::OpLogPersistence::open(store_path.to_str().unwrap(), pond_id).await?;
        let txn_meta2 = tlogfs::PondTxnMetadata::new(
            2, // Use last_write_sequence from the previous commit
            tlogfs::PondUserMetadata::new(vec!["test_check_versions".to_string()]),
        );
        let tx2 = persistence2.begin_read(&txn_meta2).await?;
        let root2 = tx2.root().await?;

        let versions = root2.list_file_versions(test_path).await?;

        // Check for duplicate versions
        let mut seen_versions = std::collections::HashSet::new();
        let mut duplicate_found = false;
        for version_info in &versions {
            if !seen_versions.insert(version_info.version) {
                log::error!(
                    "[ERR] DUPLICATE VERSION FOUND: version {} appears multiple times",
                    version_info.version
                );
                duplicate_found = true;
            }
        }

        // Log all versions for debugging
        debug!("All versions found:");
        for version_info in &versions {
            debug!(
                "  Version {}: timestamp={}",
                version_info.version, version_info.timestamp
            );
        }

        let _ = tx2.commit().await?;

        // Assert no duplicates
        assert!(
            !duplicate_found,
            "Duplicate versions found! Versions: {:?}",
            versions.iter().map(|v| v.version).collect::<Vec<_>>()
        );

        // Assert we have at least 2 versions (one per write_series_from_batch call)
        // NOTE: Implementation may create multiple versions per write (e.g., node creation + content write)
        assert!(
            versions.len() >= 2,
            "Expected at least 2 versions, got {}. This test verifies no DUPLICATE versions, not exact count.",
            versions.len()
        );

        // Verify all versions are unique (no reuse)
        let mut sorted_versions: Vec<_> = versions.iter().map(|v| v.version).collect();
        sorted_versions.sort();
        sorted_versions.dedup();
        assert_eq!(
            sorted_versions.len(),
            versions.len(),
            "Version deduplication changed count - indicates duplicate versions exist!"
        );

        Ok(())
    }

    /// A configured `archive_path` is the resume contract for a freshly reset
    /// pond: when neither writeable data nor a seed archive yields a resume
    /// point, `find_youngest_timestamp` must fail hard rather than return epoch
    /// and trigger an unbounded HydroVu crawl.  With no `archive_path`, epoch is
    /// the legitimate first-collection start.
    #[tokio::test]
    async fn test_find_youngest_timestamp_no_archive_hard_fails() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let store_path = temp_dir.path().join("test_pond");

        let mut persistence = tlogfs::OpLogPersistence::create(
            store_path.to_str().unwrap(),
            uuid7::uuid7().to_string(),
            tlogfs::PondUserMetadata::new(vec!["test_init".to_string()]),
        )
        .await?;

        let txn_meta = tlogfs::PondTxnMetadata::new(
            2,
            tlogfs::PondUserMetadata::new(vec!["test_find_youngest".to_string()]),
        );
        let tx = persistence.begin_write(&txn_meta).await?;

        let device_id = 1234567890i64;

        // archive_path configured but no writeable data and no seed archive
        // present: must hard-fail instead of crawling from epoch.
        let err = HydroVuCollector::find_youngest_timestamp(
            &tx,
            "/hydrovu",
            Some("/hydrovu-archive"),
            device_id,
            "TestDevice",
        )
        .await
        .expect_err("expected hard failure when archive resume point is missing");
        let msg = err.to_string();
        assert!(
            msg.contains("refusing to crawl HydroVu from epoch"),
            "unexpected error message: {msg}"
        );

        // No archive_path configured: epoch is the legitimate first start.
        let start = HydroVuCollector::find_youngest_timestamp(
            &tx,
            "/hydrovu",
            None,
            device_id,
            "TestDevice",
        )
        .await
        .expect("epoch start should succeed when no archive_path is set");
        assert_eq!(start, 0, "expected epoch start when no archive_path is set");

        let _ = tx.commit().await?;
        Ok(())
    }

    /// Archive timestamp normalization uses strict typing with hard failures on
    /// unsupported types.
    #[test]
    fn test_scalar_timestamp_to_micros_units_and_strictness() {
        use arrow_array::{
            Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
            TimestampNanosecondArray, TimestampSecondArray,
        };

        fn one_col(array: Arc<dyn Array>) -> Vec<RecordBatch> {
            let field = Field::new("m", array.data_type().clone(), true);
            let schema = Arc::new(Schema::new(vec![field]));
            vec![RecordBatch::try_new(schema, vec![array]).unwrap()]
        }

        // INT64 seconds (the real archive layout) -> micros.
        let b = one_col(Arc::new(Int64Array::from(vec![1_770_000_000i64])));
        assert_eq!(
            HydroVuCollector::scalar_timestamp_to_micros(&b).unwrap(),
            Some(1_770_000_000_000_000)
        );

        // Logical Timestamp types scale per their unit.
        let b = one_col(Arc::new(TimestampSecondArray::from(vec![1_700i64])));
        assert_eq!(
            HydroVuCollector::scalar_timestamp_to_micros(&b).unwrap(),
            Some(1_700_000_000)
        );
        let b = one_col(Arc::new(TimestampMillisecondArray::from(vec![1_700i64])));
        assert_eq!(
            HydroVuCollector::scalar_timestamp_to_micros(&b).unwrap(),
            Some(1_700_000)
        );
        let b = one_col(Arc::new(TimestampMicrosecondArray::from(vec![1_700i64])));
        assert_eq!(
            HydroVuCollector::scalar_timestamp_to_micros(&b).unwrap(),
            Some(1_700)
        );
        let b = one_col(Arc::new(TimestampNanosecondArray::from(vec![1_700_000i64])));
        assert_eq!(
            HydroVuCollector::scalar_timestamp_to_micros(&b).unwrap(),
            Some(1_700)
        );

        // NULL / empty -> Ok(None): the SQL aggregate had no rows / a NULL max.
        let b = one_col(Arc::new(Int64Array::from(vec![None::<i64>])));
        assert_eq!(
            HydroVuCollector::scalar_timestamp_to_micros(&b).unwrap(),
            None
        );
        assert_eq!(
            HydroVuCollector::scalar_timestamp_to_micros(&[]).unwrap(),
            None
        );

        // Unsupported type hard-fails (no silent skip).
        let b = one_col(Arc::new(arrow_array::StringArray::from(vec!["x"])));
        assert!(
            HydroVuCollector::scalar_timestamp_to_micros(&b).is_err(),
            "String timestamp column must hard-fail"
        );
    }
}
