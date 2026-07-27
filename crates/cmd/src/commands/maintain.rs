// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::common::ShipContext;
use anyhow::{Result, anyhow};
use log::info;

/// Run Delta Lake maintenance on both data and control tables.
///
/// Performs checkpoint creation, log cleanup, and vacuum.
/// When `compact` is true, also merges small parquet files.
/// When `collapse_versions` is non-zero, also collapses multi-version
/// `data:series` files whose live version count exceeds that threshold.
/// When `prune` is true, first deletes replicated control-table lifecycle
/// history at or below a safe horizon so the subsequent checkpoint +
/// vacuum reclaims it in the SAME pass (no extra ship open / read txn).
/// Collapse likewise runs before the checkpoint + vacuum, since its
/// reclamation phase deletes the rows that collapse superseded.
pub async fn maintain_command(
    ship_context: &ShipContext,
    compact: bool,
    collapse_versions: usize,
    prune: bool,
    keep_txns: i64,
    allow_no_remote: bool,
) -> Result<()> {
    let pond_path = ship_context.resolve_pond_path()?;
    info!("Running maintenance on pond: {}", pond_path.display());

    let mut ship = ship_context
        .open_pond()
        .await
        .map_err(|e| anyhow!("Failed to open pond: {}", e))?;

    // Prune BEFORE maintain so the deletion's tombstones are reclaimed by
    // the checkpoint + vacuum below, in the same maintenance pass.
    let pruned = if prune {
        let h =
            crate::commands::control::compute_prune_horizon(&mut ship, keep_txns, allow_no_remote)
                .await?;
        let deleted =
            crate::commands::control::prune_history_at_horizon(&mut ship, h.horizon).await?;
        Some((h.horizon, deleted))
    } else {
        None
    };

    // Collapse BEFORE maintain, for the same reason as prune above: collapse's
    // reclamation phase deletes superseded rows, and those tombstones are only
    // turned back into free space by the checkpoint + vacuum that follows.
    let collapse_report = if collapse_versions > 0 {
        Some(
            ship.collapse_versions(collapse_versions)
                .await
                .map_err(|e| anyhow!("Version collapse failed: {}", e))?,
        )
    } else {
        None
    };

    let report = ship.maintain(true, compact).await;

    // Print results to stdout
    #[allow(clippy::print_stdout)]
    {
        if let Some((horizon, deleted)) = pruned {
            if horizon < 1 {
                println!("  control prune: nothing to prune (horizon < 1)");
            } else {
                println!(
                    "  control prune: deleted {} rows at/below seq {}",
                    deleted, horizon
                );
            }
        }
        if let Some(ref data) = report.data {
            println!("{}", data);
        }
        if let Some(ref control) = report.control {
            println!("{}", control);
        }
        if compact && report.data.as_ref().map(|d| d.compacted).unwrap_or(false) {
            println!(
                "  data compaction reclaimed local storage; the content is \
                 unchanged, so replicas need no update"
            );
        }
        if let Some(ref collapse) = collapse_report {
            println!("{}", collapse);
        }
    }

    info!("[OK] Maintenance completed");
    Ok(())
}
