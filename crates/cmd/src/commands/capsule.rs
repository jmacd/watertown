// SPDX-License-Identifier: Apache-2.0

//! Operator commands for portable recovery capsules.

use anyhow::{Result, anyhow};

use crate::commands::remote::{RemoteMode, load_remote_attachment, remote_mode_for};
use crate::common::ShipContext;

/// Verify and summarize a downloaded recovery capsule without opening a pond.
pub fn capsule_inspect_command(path: &std::path::Path) -> Result<()> {
    let report = sync_store::verify_capsule_directory(path)
        .map_err(|error| anyhow!("verify recovery capsule at {}: {error}", path.display()))?;
    log::info!(
        "[OK] capsule verified (root={}, entries={}, payload_objects={}, physical_bytes={}, logical_count={})",
        report.root,
        report.entries,
        report.payload_objects,
        report.physical_bytes,
        report.logical_count
    );
    Ok(())
}

/// Build and publish a verified recovery capsule to a named backup remote.
pub async fn capsule_publish_command(ship_context: &ShipContext, name: &str) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;
    match remote_mode_for(&ship, name).await? {
        RemoteMode::Push | RemoteMode::Both => {}
        RemoteMode::Pull => {
            return Err(anyhow!(
                "remote `{name}` is pull-only; recovery capsules publish only to backup remotes"
            ));
        }
    }
    let attachment = load_remote_attachment(&mut ship, name).await?;
    let pond = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("capsule publish requires a pond steward"))?;
    let storage_options = steward::storage_profile::prepare_storage(pond, &attachment).await?;
    let limit_spec = attachment.resolved_limits()?;
    let pond = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("capsule publish requires a pond steward"))?;
    let mut limits = steward::LimiterSet::open(pond, &limit_spec)
        .await
        .map_err(|error| anyhow!("bind limiters for remote `{name}`: {error}"))?;

    let pond = ship
        .as_pond()
        .ok_or_else(|| anyhow!("capsule publish requires a pond steward"))?;
    let published = steward::open_and_publish_capsule_limited(
        pond,
        &attachment.url,
        storage_options,
        &mut limits,
    )
    .await;

    let pond = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("capsule publish requires a pond steward"))?;
    if let Err(error) = limits.commit(pond.control_table_mut()).await {
        log::warn!("[WARN] capsule publish {name}: failed to record limiter usage: {error}");
    }

    let outcome = published
        .map_err(|error| anyhow!("capsule publish {name} ({}): {error}", attachment.url))?;
    log::info!(
        "[OK] capsule publish {name} complete (root={}, payloads_uploaded={}, payloads_total={})",
        outcome.root,
        outcome.payloads_uploaded,
        outcome.payloads_total
    );
    Ok(())
}

/// List retained recovery-capsule generations at a named remote.
pub async fn capsule_list_command(ship_context: &ShipContext, name: &str) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;
    let attachment = load_remote_attachment(&mut ship, name).await?;
    let pond = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("capsule list requires a pond steward"))?;
    let storage_options = steward::storage_profile::prepare_storage(pond, &attachment).await?;
    let limit_spec = attachment.resolved_limits()?;
    let pond = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("capsule list requires a pond steward"))?;
    let mut limits = steward::LimiterSet::open(pond, &limit_spec)
        .await
        .map_err(|error| anyhow!("bind limiters for remote `{name}`: {error}"))?;

    let generations =
        steward::open_and_list_capsules_limited(&attachment.url, storage_options, &mut limits)
            .await;

    let pond = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("capsule list requires a pond steward"))?;
    if let Err(error) = limits.commit(pond.control_table_mut()).await {
        log::warn!("[WARN] capsule list {name}: failed to record limiter usage: {error}");
    }

    let generations = generations
        .map_err(|error| anyhow!("capsule list {name} ({}): {error}", attachment.url))?;
    if generations.is_empty() {
        log::info!("No recovery capsules are retained at `{name}`.");
        return Ok(());
    }
    for (index, (root, manifest)) in generations.iter().enumerate() {
        let current = if index == 0 { " current" } else { "" };
        log::info!(
            "{}{} source_tip={} exported_at_micros={} entries={}",
            root,
            current,
            manifest.source.source_tip,
            manifest.source.exported_at_micros,
            manifest.entries.len()
        );
    }
    Ok(())
}
