// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Bounded, machine-readable limiter state for monitoring.
//!
//! Unlike `/sys/limits/usage`, this command never scans metric history.  It
//! reads each configured limiter's constant-size bucket state from control,
//! so a one-minute selfmon probe stays constant in retained pond history.

#![allow(clippy::print_stdout)]

use crate::commands::remote::{list_remote_names, load_remote_attachment};
use crate::common::ShipContext;
use anyhow::{Result, anyhow, bail};
use serde::Serialize;
use std::collections::BTreeMap;
use steward::LimiterState;

/// One limiter at one sampling instant.
///
/// `remotes` is comma-separated for JSON-log compatibility.  Several remotes
/// may deliberately share one limiter; deduplicating by `(path, unit)` keeps a
/// shared site ingress budget from appearing three times.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LimiterStatusRow {
    pub timestamp: String,
    pub pond: String,
    pub remotes: String,
    pub limiter: String,
    pub unit: String,
    pub charged: Option<u64>,
    pub observed: Option<u64>,
    pub observed_since_us: Option<i64>,
    pub observed_window_complete: Option<bool>,
    pub limit: Option<u64>,
    pub burst_charged: Option<u64>,
    pub burst_observed: Option<u64>,
    pub burst: Option<u64>,
    pub window_secs: Option<u64>,
    pub burst_window_secs: Option<u64>,
    pub reset_secs: Option<u64>,
    pub burst_reset_secs: Option<u64>,
    pub error: Option<String>,
}

impl LimiterStatusRow {
    fn healthy(timestamp: &str, pond: &str, remotes: String, state: LimiterState) -> Self {
        Self {
            timestamp: timestamp.to_string(),
            pond: pond.to_string(),
            remotes,
            limiter: state.path,
            unit: state.unit.as_str().to_string(),
            charged: Some(state.used),
            observed: Some(state.observed),
            observed_since_us: state.observed_since_us,
            observed_window_complete: Some(state.observed_window_complete),
            limit: Some(state.limit),
            burst_charged: Some(state.burst_used),
            burst_observed: Some(state.burst_observed),
            burst: Some(state.burst),
            window_secs: Some(state.window.as_secs()),
            burst_window_secs: Some(state.burst_window.as_secs()),
            reset_secs: Some(state.reset_in.as_secs()),
            burst_reset_secs: Some(state.burst_reset_in.as_secs()),
            error: None,
        }
    }

    fn broken(
        timestamp: &str,
        pond: &str,
        remotes: String,
        limiter: String,
        unit: String,
        error: String,
    ) -> Self {
        Self {
            timestamp: timestamp.to_string(),
            pond: pond.to_string(),
            remotes,
            limiter,
            unit,
            charged: None,
            observed: None,
            observed_since_us: None,
            observed_window_complete: None,
            limit: None,
            burst_charged: None,
            burst_observed: None,
            burst: None,
            window_secs: None,
            burst_window_secs: None,
            reset_secs: None,
            burst_reset_secs: None,
            error: Some(error),
        }
    }
}

/// Read every distinct limiter configured on this pond.
pub async fn collect_limiter_status(ship_context: &ShipContext) -> Result<Vec<LimiterStatusRow>> {
    let mut ship = ship_context.open_pond().await?;
    let pond = ship.control_table().get_pond_metadata().birthplace.clone();
    let timestamp = chrono::Utc::now().to_rfc3339();

    // First collect identity only.  Opening a limiter needs mutable pond
    // access, so separating the attachment pass also avoids opening a shared
    // site limiter once per remote.
    let mut configured: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    for name in list_remote_names(&mut ship).await? {
        let attachment = load_remote_attachment(&mut ship, &name)
            .await
            .map_err(|e| anyhow!("read remote `{name}`: {e}"))?;
        for (unit, path) in attachment
            .resolved_limits()
            .map_err(|e| anyhow!("read limits for remote `{name}`: {e}"))?
        {
            configured
                .entry((path, unit.as_str().to_string()))
                .or_default()
                .push(name.clone());
        }
    }

    let pond_ship = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("limits requires a pond steward"))?;
    let mut rows = Vec::with_capacity(configured.len());
    for ((path, unit_text), mut remotes) in configured {
        remotes.sort();
        remotes.dedup();
        let remotes = remotes.join(",");
        let unit = provider::factory::rate_limit::LimitUnit::parse(&unit_text)
            .map_err(|e| anyhow!("internal limiter unit `{unit_text}`: {e}"))?;
        let row = match steward::Limiter::open(pond_ship, &path, unit).await {
            Ok(limiter) => LimiterStatusRow::healthy(&timestamp, &pond, remotes, limiter.state()),
            Err(e) => {
                LimiterStatusRow::broken(&timestamp, &pond, remotes, path, unit_text, e.to_string())
            }
        };
        rows.push(row);
    }
    Ok(rows)
}

/// Print limiter state as JSON Lines (for collectors), JSON, or a compact
/// operator table.
pub async fn limits_command(ship_context: &ShipContext, format: &str) -> Result<()> {
    let rows = collect_limiter_status(ship_context).await?;
    match format {
        "jsonl" => {
            for row in rows {
                println!("{}", serde_json::to_string(&row)?);
            }
        }
        "json" => println!("{}", serde_json::to_string_pretty(&rows)?),
        "table" => {
            println!(
                "{:<24} {:<8} {:>12} {:>12} {:>12} {:>12} {:>12}",
                "limiter", "unit", "charged", "observed", "limit", "burst used", "burst limit"
            );
            for row in rows {
                if let Some(error) = row.error {
                    println!("{:<24} {:<8} ERROR: {}", row.limiter, row.unit, error);
                    continue;
                }
                println!(
                    "{:<24} {:<8} {:>12} {:>12} {:>12} {:>12} {:>12}",
                    row.limiter,
                    row.unit,
                    row.charged.unwrap_or_default(),
                    row.observed.unwrap_or_default(),
                    row.limit.unwrap_or_default(),
                    row.burst_charged.unwrap_or_default(),
                    row.burst.unwrap_or_default(),
                );
            }
        }
        other => bail!("unknown limits format `{other}`; expected table, json, or jsonl"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broken_rows_keep_limiter_identity() {
        let row = LimiterStatusRow::broken(
            "2026-08-07T00:00:00Z",
            "water-staging",
            "origin".to_string(),
            "/sys/limits/backup-ops".to_string(),
            "ops".to_string(),
            "bad policy".to_string(),
        );
        let json = serde_json::to_value(row).unwrap();
        assert_eq!(json["limiter"], "/sys/limits/backup-ops");
        assert_eq!(json["error"], "bad policy");
        assert!(json["charged"].is_null());
    }
}
