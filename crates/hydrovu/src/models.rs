// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tlogfs::data_taxonomy::{ApiKey, ApiSecret};

/// Configuration for HydroVu OAuth credentials and device list
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "documentation", derive(schemars::JsonSchema))]
pub struct HydroVuConfig {
    /// OAuth client ID (sensitive - redacted from debug output)
    #[cfg_attr(feature = "documentation", schemars(with = "String"))]
    pub client_id: ApiKey<String>,

    /// OAuth client secret (sensitive - redacted from debug output)
    #[cfg_attr(feature = "documentation", schemars(with = "String"))]
    pub client_secret: ApiSecret<String>,

    pub max_points_per_run: usize,

    /// Path within pond for hydrovu data (e.g., "/hydrovu")
    pub hydrovu_path: String,

    /// Optional path within pond holding read-only seed archives (e.g.
    /// "/hydrovu-archive"), typically git-ingested `FileDynamic` Parquet
    /// `.series` snapshots laid out as `{archive_path}/devices/{id}/*.series`.
    /// Consulted by `find_youngest_timestamp` ONLY when the writeable device
    /// directory has no temporal data (a freshly reset pond), so live
    /// collection resumes from the archive's youngest timestamp instead of
    /// crawling from epoch.  These archives are NOT written to.
    #[serde(default)]
    pub archive_path: Option<String>,

    /// Device list
    pub devices: Vec<HydroVuDevice>,
}

/// Device configuration specifying which HydroVu location to collect
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "documentation", derive(schemars::JsonSchema))]
pub struct HydroVuDevice {
    pub name: String,
    pub id: i64,
    /// When false, device is skipped during `collect`. Default: true.
    #[serde(default = "default_active")]
    pub active: bool,
    pub comment: Option<String>,
}

fn default_active() -> bool {
    true
}

/// Names mapping returned by the HydroVu API
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Names {
    pub parameters: BTreeMap<String, String>,
    pub units: BTreeMap<String, String>,
}

/// Location data structure from HydroVu API
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Location {
    pub description: String,
    pub id: i64,
    pub name: String,
    pub gps: LatLong,
}

/// GPS coordinates
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LatLong {
    pub latitude: f64,
    pub longitude: f64,
}

/// Location readings response from HydroVu API
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LocationReadings {
    pub location_id: i64,
    pub parameters: Vec<ParameterInfo>,
}

/// Parameter information with readings
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ParameterInfo {
    pub custom_parameter: bool,
    pub parameter_id: String,
    pub unit_id: String,
    pub readings: Vec<Reading>,
}

/// Individual sensor reading
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Reading {
    pub timestamp: i64, // Unix timestamp in seconds
    pub value: f64,
}

/// Wide record structure for Arrow storage (joined by timestamp)
#[derive(Debug, Clone)]
pub(crate) struct WideRecord {
    pub(crate) timestamp: DateTime<Utc>,
    pub(crate) parameters: BTreeMap<String, Option<f64>>,
}

impl WideRecord {
    /// Convert from API response to timestamp-joined wide records,
    /// joining records by timestamp
    pub(crate) fn from_location_readings(
        location_readings: &LocationReadings,
        units: &BTreeMap<String, String>,
        parameters: &BTreeMap<String, String>,
    ) -> anyhow::Result<Vec<Self>> {
        use std::collections::BTreeSet;

        // Collect all unique timestamps from all parameters
        let all_timestamps: BTreeSet<i64> = location_readings
            .parameters
            .iter()
            .flat_map(|p| p.readings.iter().map(|r| r.timestamp))
            .collect();

        // Create wide records, one per timestamp
        let mut wide_records = Vec::new();

        for timestamp_sec in all_timestamps {
            let timestamp = DateTime::from_timestamp(timestamp_sec, 0)
                .ok_or_else(|| anyhow::anyhow!("Invalid timestamp from API: {}", timestamp_sec))?;

            // Create a map for all parameter values at this timestamp
            let mut parameter_values = BTreeMap::new();

            // Fill in values from each parameter that has a reading at this timestamp
            for param_info in &location_readings.parameters {
                let value = param_info
                    .readings
                    .iter()
                    .find(|r| r.timestamp == timestamp_sec)
                    .map(|r| r.value);

                // Create column name using original HydroVu convention: {param_name}.{unit_name}
                let param_name = parameters
                    .get(&param_info.parameter_id)
                    .unwrap_or(&param_info.parameter_id); // Fall back to ID if not found in dictionary
                let unit_name = units
                    .get(&param_info.unit_id)
                    .unwrap_or(&param_info.unit_id); // Fall back to ID if not found in dictionary

                let column_name = format!("{}.{}", param_name, unit_name);

                _ = parameter_values.insert(column_name, value);
            }

            wide_records.push(WideRecord {
                timestamp,
                parameters: parameter_values,
            });
        }

        Ok(wide_records)
    }
}
