// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! HydroVu executable factory
//!
//! This factory creates run configuration nodes for HydroVu data collection.
//! Configuration is stored as a file:dynamic node that can be:
//! - Read with `pond cat /path/to/config`
//! - Executed with `pond run /path/to/config`

use crate::HydroVuConfig;
use clap::Parser;
use provider::FactoryContext;
use provider::registry::{ExecutionContext, ExecutionMode, FactoryCommand};
use serde_json::Value;
use tinyfs::Result as TinyFSResult;
use tinyfs::ResultExt;
use tlogfs::TLogFSError;

/// HydroVu command-line interface
#[derive(Debug, Parser)]
enum HydroVuCommand {
    /// Collect data from HydroVu API
    ///
    /// Fetches sensor data from configured devices and stores it in the pond.
    /// Devices with `active: false` in the config are skipped.
    ///
    /// Example: pond run /system/etc/20-hydrovu collect
    Collect,

    /// Archive active series data
    ///
    /// Renames each device's `{Name}_active.series` (or legacy `{Name}.series`)
    /// to `{Name}_archive_YYYYMMDD.series`. Prints a `pond copy` script to
    /// export the archived files to the host filesystem.
    ///
    /// Example: pond run /system/etc/20-hydrovu archive
    /// Example: pond run /system/etc/20-hydrovu archive --device NoyoCenterVulink_1
    Archive {
        /// Only archive this device (by name). If omitted, archives all devices.
        #[arg(long)]
        device: Option<String>,
    },
}

impl FactoryCommand for HydroVuCommand {
    fn allowed(&self) -> ExecutionMode {
        match self {
            Self::Collect => ExecutionMode::PondReadWriter,
            Self::Archive { .. } => ExecutionMode::PondReadWriter,
        }
    }
}

/// Validate HydroVu configuration from YAML bytes
fn validate_hydrovu_config(config_bytes: &[u8]) -> TinyFSResult<Value> {
    let config_str = std::str::from_utf8(config_bytes).map_other_context("Invalid UTF-8")?;

    let config: HydroVuConfig =
        serde_yaml::from_str(config_str).map_other_context("Invalid YAML")?;

    // Use as_declassified() to access actual values for validation
    if config.client_id.as_declassified().is_empty() {
        return Err(tinyfs::Error::InvalidConfig(
            "client_id cannot be empty".into(),
        ));
    }

    if config.client_secret.as_declassified().is_empty() {
        return Err(tinyfs::Error::InvalidConfig(
            "client_secret cannot be empty".into(),
        ));
    }

    if config.devices.is_empty() {
        return Err(tinyfs::Error::InvalidConfig(
            "At least one device must be configured".into(),
        ));
    }

    for device in &config.devices {
        if device.name.is_empty() {
            return Err(tinyfs::Error::InvalidConfig(format!(
                "Device name cannot be empty for device ID {}",
                device.id
            )));
        }
    }

    if config.max_points_per_run == 0 {
        return Err(tinyfs::Error::InvalidConfig(
            "max_points_per_run must be greater than 0".into(),
        ));
    }

    serde_json::to_value(config).map_other_context("Serialization error")
}

/// Create a HydroVu configuration file (simple, no FS operations)
/// Initialize HydroVu factory after node creation (runs outside lock)
/// This creates the directory structure required for data collection
async fn initialize_hydrovu(config: Value, context: FactoryContext) -> Result<(), TLogFSError> {
    let parsed_config: HydroVuConfig = serde_json::from_value(config)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Invalid config: {}", e))))?;

    // Create directory structure now that lock is released
    create_directory_structure(&parsed_config, &context)
        .await
        .map_err(TLogFSError::TinyFS)?;

    Ok(())
}

/// Create HydroVu directory structure.
///
/// Uses `create_dir_all` (mkdir -p semantics) so this is safe to call on
/// `mknod --overwrite` when directories already exist.
async fn create_directory_structure(
    config: &HydroVuConfig,
    context: &FactoryContext,
) -> TinyFSResult<()> {
    log::debug!("Creating HydroVu directory structure");

    // Extract State from provider context
    let state = tlogfs::extract_state(context).map_other()?;

    // Create FS from State - this is SAFE here because create_dynamic_file_node has returned and released its lock
    let fs = tinyfs::FS::new(state.clone()).await?;

    let root = fs.root().await?;

    // Create base HydroVu directory
    let hydrovu_path = &config.hydrovu_path;
    log::debug!("Creating HydroVu base directory: {}", hydrovu_path);
    _ = root.create_dir_all(hydrovu_path).await?;

    // Create devices directory
    let devices_path = format!("{}/devices", config.hydrovu_path);
    log::debug!("Creating devices directory: {}", devices_path);
    _ = root.create_dir_all(&devices_path).await?;

    // Create directory for each configured device
    for device in &config.devices {
        let device_id = device.id;
        let device_name = &device.name;
        let device_path = format!("{}/{}", devices_path, device_id);
        log::debug!(
            "Creating device directory: {} ({})",
            device_path,
            device_name
        );
        _ = root.create_dir_all(&device_path).await?;
    }

    log::debug!("HydroVu directory structure created successfully");
    Ok(())
}

/// Execute HydroVu data collection
async fn execute_hydrovu(
    config: Value,
    context: FactoryContext,
    ctx: ExecutionContext,
) -> Result<(), TLogFSError> {
    log::info!("[HYDRO] HYDROVU FACTORY");
    log::info!("   Context: {:?}", ctx);

    let cmd: HydroVuCommand = ctx.to_command::<HydroVuCommand, TLogFSError>()?;

    log::info!("   Command: {:?}", cmd);

    // Parse the configuration
    let hydrovu_config: HydroVuConfig = serde_json::from_value(config)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Invalid config: {}", e))))?;

    log::debug!(
        "Executing HydroVu collection with config: {:?}",
        hydrovu_config
    );

    // Dispatch to command handler
    match cmd {
        HydroVuCommand::Collect => execute_collect(hydrovu_config, context).await,
        HydroVuCommand::Archive { device } => {
            execute_archive(hydrovu_config, context, device).await
        }
    }
}

/// Execute the collect command - fetch data from HydroVu API
async fn execute_collect(
    hydrovu_config: HydroVuConfig,
    context: FactoryContext,
) -> Result<(), TLogFSError> {
    // The transaction is already started by the caller (pond run command)
    // We work directly with State which provides DataFusion context and filesystem access

    // Create the collector (no Ship needed, just API client setup)
    let mut collector = crate::HydroVuCollector::new(hydrovu_config.clone())
        .await
        .map_err(|e| {
            TLogFSError::TinyFS(tinyfs::Error::Other(format!(
                "Failed to create collector: {}",
                e
            )))
        })?;

    // Extract State from provider context
    let state = tlogfs::extract_state(&context)?;

    // Create filesystem from state
    let fs = tinyfs::FS::new(state.clone())
        .await
        .map_err(TLogFSError::TinyFS)?;

    // Run collection within the existing transaction
    let result = collector.collect_data(&fs).await.map_err(|e| {
        TLogFSError::TinyFS(tinyfs::Error::Other(format!(
            "HydroVu data collection failed: {}",
            e
        )))
    })?;

    // Print summary for each device
    for summary in &result.device_summaries {
        let start_date = crate::utc2date(summary.start_timestamp)
            .unwrap_or_else(|_| format!("{}", summary.start_timestamp));
        let final_date = crate::utc2date(summary.final_timestamp)
            .unwrap_or_else(|_| format!("{}", summary.final_timestamp));

        log::info!(
            "Device {} collected {} records from {} to {}",
            summary.device_id,
            summary.records_collected,
            start_date,
            final_date,
        );
    }

    log::info!(
        "HydroVu collection complete: {} total records collected",
        result.records_collected
    );

    Ok(())
}

/// Execute the archive command - compact and archive active series files
async fn execute_archive(
    hydrovu_config: HydroVuConfig,
    context: FactoryContext,
    device_filter: Option<String>,
) -> Result<(), TLogFSError> {
    let state = tlogfs::extract_state(&context)?;
    let provider_ctx = state.as_provider_context();
    let fs = tinyfs::FS::new(state.clone())
        .await
        .map_err(TLogFSError::TinyFS)?;

    let result = crate::archive_devices(
        &fs,
        &hydrovu_config,
        device_filter.as_deref(),
        &provider_ctx,
    )
    .await
    .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;

    // Print summary and export script
    if result.is_empty() {
        log::info!("No series files found to archive.");
    } else {
        log::info!("Archived {} series files.", result.len());
        log::info!("Export archived files with these commands:");
        for (pond_path, _old_name) in &result {
            log::info!("pond copy '{}' host://<dest>{}", pond_path, pond_path);
        }
    }

    Ok(())
}

// Register the factory
// Note: No file parameter - executable factories don't need create_file
// Config bytes ARE the file content (YAML), handled by ConfigFile wrapper
provider::register_executable_factory!(
    name: "hydrovu",
    description: "HydroVu data collector configuration",
    validate: validate_hydrovu_config,
    initialize: initialize_hydrovu,
    execute: execute_hydrovu
);
