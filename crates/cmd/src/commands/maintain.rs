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
/// When `collapse_versions` is non-zero, also runs production, pack-only
/// physical maintenance (`docs/logical-series-identity-design.md`) on
/// `data:series` files whose current physical representation (live
/// version/physical-object count) exceeds that threshold:
/// `Ship::collapse_versions` repacks each over-threshold native
/// logical-series-v2 series into a smaller, bounded set of
/// content-addressed physical pack objects published under this pond's own
/// local `data/_packs` namespace. This never rewrites or deletes an Oplog
/// append row and never changes a series' `watertown.series.v3` manifest, content
/// tree/commit root, Delta version, or txn sequence -- only local disk
/// state under `_packs/` is mutated, so it always returns `Ok`. A pre-v2
/// (legacy) series carries no persisted v2 leaf identity and is reported as
/// unsupported by this operation, not as an error.
/// When `prune` is true, first deletes replicated control-table lifecycle
/// history at or below a safe horizon so the subsequent checkpoint +
/// vacuum reclaims it in the SAME pass (no extra ship open / read txn).
/// Pack maintenance likewise runs before checkpoint + vacuum because it also
/// deletes superseded rows of the reserved `.pond-node-index`. User-series
/// rows and `_large_files` are never reclaimed through this option.
///
/// When `dry_run` is true nothing is modified: the command reports what
/// collapse and prune *would* do and returns.
#[allow(clippy::fn_params_excessive_bools)]
pub async fn maintain_command(
    ship_context: &ShipContext,
    compact: bool,
    collapse_versions: usize,
    prune: bool,
    keep_txns: i64,
    allow_no_remote: bool,
    dry_run: bool,
) -> Result<()> {
    let pond_path = ship_context.resolve_pond_path()?;
    info!("Running maintenance on pond: {}", pond_path.display());

    let mut ship = ship_context
        .open_pond()
        .await
        .map_err(|e| anyhow!("Failed to open pond: {}", e))?;

    if dry_run {
        return report_dry_run(
            &mut ship,
            collapse_versions,
            prune,
            keep_txns,
            allow_no_remote,
        )
        .await;
    }

    if let Some(freeze) = ship
        .as_pond()
        .ok_or_else(|| anyhow!("maintenance requires a pond steward"))?
        .write_freeze()?
    {
        return Err(anyhow!(
            "pond writes are frozen at tip {} (frozen_at={}, reason={}); maintenance made no changes",
            freeze.source_tip.as_deref().unwrap_or("<none>"),
            freeze.frozen_at.to_rfc3339(),
            freeze.reason
        ));
    }

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

    // Pack maintenance runs before the checkpoint/vacuum so tombstones from
    // deleting obsolete reserved-index pointers can be reclaimed in this pass.
    // It never deletes user-series rows or their `_large_files` payloads.
    let collapse_report = if collapse_versions > 0 {
        Some(
            ship.collapse_versions(collapse_versions)
                .await
                .map_err(|e| anyhow!("Pack-only maintenance failed: {}", e))?,
        )
    } else {
        None
    };

    let report = ship
        .maintain(true, compact)
        .await
        .map_err(|error| anyhow!("Maintenance failed: {error}"))?;

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

/// Report what maintenance would do, changing nothing.
///
/// Pack maintenance is the operation worth previewing: it reads selected live
/// series and creates an additive whole-range physical layout under local
/// `_packs`, without changing logical content or publishing remotely.
async fn report_dry_run(
    ship: &mut steward::Steward,
    collapse_versions: usize,
    prune: bool,
    keep_txns: i64,
    allow_no_remote: bool,
) -> Result<()> {
    let horizon = if prune {
        Some(
            crate::commands::control::compute_prune_horizon(ship, keep_txns, allow_no_remote)
                .await?,
        )
    } else {
        None
    };

    // Resolve node identities to paths so the report names files rather than
    // UUIDs.  Done only when there is something to name.  Shares
    // `Ship::survey_pack_maintenance`'s discovery with `Ship::collapse_versions`
    // (through the private `pack_maintenance` module), so this preview can
    // never disagree with what a real run would do.
    let candidates = if collapse_versions > 0 {
        ship.survey_pack_maintenance(collapse_versions).await?
    } else {
        Vec::new()
    };
    let paths = if candidates.is_empty() {
        std::collections::HashMap::new()
    } else {
        ship.node_paths().await?
    };

    #[allow(clippy::print_stdout)]
    {
        println!("Dry run: nothing was modified.");

        if let Some(h) = horizon {
            if h.horizon < 1 {
                println!("  control prune: nothing to prune (horizon < 1)");
            } else {
                println!(
                    "  control prune: would delete rows at/below seq {} (last committed {})",
                    h.horizon, h.last_committed
                );
            }
        }

        if collapse_versions == 0 {
            println!("  collapse: not requested (pass --collapse-versions N)");
        } else if candidates.is_empty() {
            println!("  collapse: no series exceeds {collapse_versions} live versions");
        } else {
            let needs_repack = candidates
                .iter()
                .filter(|c| c.outcome == steward::PackCandidateOutcome::NeedsRepack)
                .count();
            let unsupported_legacy = candidates
                .iter()
                .filter(|c| c.outcome == steward::PackCandidateOutcome::UnsupportedLegacy)
                .count();
            println!(
                "  collapse: {} series exceed {} live versions ({} would be repacked, \
                 {} already at their achievable bounded layout, {} pre-v2/unsupported)",
                candidates.len(),
                collapse_versions,
                needs_repack,
                candidates.len() - needs_repack - unsupported_legacy,
                unsupported_legacy,
            );
            let mut rows: Vec<_> = candidates.iter().collect();
            rows.sort_by_key(|c| std::cmp::Reverse(c.logical_count));
            for c in rows {
                let name = paths
                    .get(&c.file_id)
                    .map_or_else(|| c.file_id.node_id().to_string(), |p| p.clone());
                println!("    {name}: {c}");
            }
            if needs_repack > 0 {
                println!(
                    "  a repack publishes new physical pack objects and a pack index to this \
                     pond's own local `data/_packs` storage (or `pond://` sidecar storage for a \
                     replica) only. `pond push` never uploads them implicitly; after the matching \
                     snapshot is published, run `pond backup publish-consolidated NAME` to upload \
                     verified objects, pack, and immutable consolidated locator under the named \
                     push/both backup's limiter. This never rewrites or reclaims a user Oplog row \
                     or changes a series' watertown.series.v3 manifest, content root, Delta \
                     version, or txn sequence."
                );
            }
        }
    }

    Ok(())
}
