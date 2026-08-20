// SPDX-License-Identifier: Apache-2.0

//! Operator commands for portable recovery capsules.

use anyhow::{Result, anyhow};

use crate::commands::remote::{RemoteMode, load_remote_attachment, remote_mode_for};
use crate::common::ShipContext;

/// Retention-GC operation selected by the CLI.
pub enum CapsuleGcAction {
    /// Create a new immutable plan file.
    Plan {
        output: std::path::PathBuf,
        grace_hours: u64,
    },
    /// Revalidate a plan without deleting.
    Verify { plan: std::path::PathBuf },
    /// Apply a plan matching the reviewed hash.
    Apply {
        plan: std::path::PathBuf,
        reviewed_hash: String,
    },
}

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

/// Plan, verify, or apply retained-generation garbage collection.
pub async fn capsule_gc_command(
    ship_context: &ShipContext,
    name: &str,
    action: CapsuleGcAction,
) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;
    let attachment = load_remote_attachment(&mut ship, name).await?;
    let pond = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("capsule gc requires a pond steward"))?;
    let storage_options = steward::storage_profile::prepare_storage(pond, &attachment).await?;
    let limit_spec = attachment.resolved_limits()?;
    let pond = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("capsule gc requires a pond steward"))?;
    let mut limits = steward::LimiterSet::open(pond, &limit_spec)
        .await
        .map_err(|error| anyhow!("bind limiters for remote `{name}`: {error}"))?;

    let operation: Result<()> = async {
        match action {
        CapsuleGcAction::Plan {
            output,
            grace_hours,
        } => {
            let grace_micros = i64::try_from(grace_hours)
                .ok()
                .and_then(|hours| hours.checked_mul(60 * 60 * 1_000_000))
                .ok_or_else(|| anyhow!("capsule GC grace period is too large"))?;
            let plan = steward::open_and_plan_capsule_gc_limited(
                &attachment.url,
                storage_options,
                grace_micros,
                &mut limits,
            )
            .await
            .map_err(|error| anyhow!("capsule gc plan {name}: {error}"))?;
            let bytes = sync_store::capsule_gc_plan_bytes(&plan)
                .map_err(|error| anyhow!("encode capsule GC plan: {error}"))?;
            let hash = sync_store::capsule_gc_plan_hash(&plan)
                .map_err(|error| anyhow!("hash capsule GC plan: {error}"))?;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&output)
                .map_err(|error| {
                    anyhow!("create capsule GC plan {}: {error}", output.display())
                })?;
            std::io::Write::write_all(&mut file, &bytes).map_err(|error| {
                anyhow!("write capsule GC plan {}: {error}", output.display())
            })?;
            let total_bytes = plan
                .deletions
                .iter()
                .try_fold(0u64, |total, object| total.checked_add(object.size))
                .ok_or_else(|| anyhow!("capsule GC deletion bytes exceed u64::MAX"))?;
            log::info!(
                "[OK] capsule GC plan written to {} (hash={}, objects={}, bytes={}, not_before_micros={})",
                output.display(),
                hash,
                plan.deletions.len(),
                total_bytes,
                plan.not_before_micros
            );
            Ok(())
        }
        CapsuleGcAction::Verify { plan } => {
            let plan_value = read_gc_plan(&plan)?;
            let hash = sync_store::capsule_gc_plan_hash(&plan_value)
                .map_err(|error| anyhow!("hash capsule GC plan: {error}"))?;
            steward::open_and_verify_capsule_gc_limited(
                &attachment.url,
                storage_options,
                plan_value,
                &mut limits,
            )
            .await
            .map_err(|error| anyhow!("capsule gc verify {name}: {error}"))?;
            log::info!("[OK] capsule GC plan verified (hash={hash})");
            Ok(())
        }
        CapsuleGcAction::Apply {
            plan,
            reviewed_hash,
        } => {
            if reviewed_hash
                .bytes()
                .any(|byte| byte.is_ascii_uppercase())
            {
                return Err(anyhow!("reviewed plan hash must use lowercase hexadecimal"));
            }
            let reviewed_hash =
                sync_store::ObjectHash::from_hex(&reviewed_hash).map_err(|error| {
                    anyhow!("invalid reviewed capsule GC plan hash: {error}")
                })?;
            let plan_value = read_gc_plan(&plan)?;
            let deleted = steward::open_and_apply_capsule_gc_limited(
                &attachment.url,
                storage_options,
                plan_value,
                reviewed_hash,
                &mut limits,
            )
            .await
            .map_err(|error| anyhow!("capsule gc apply {name}: {error}"))?;
            log::info!("[OK] capsule GC applied (objects_deleted={deleted})");
            Ok(())
        }
        }
    }
    .await;

    let pond = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("capsule gc requires a pond steward"))?;
    if let Err(error) = limits.commit(pond.control_table_mut()).await {
        log::warn!("[WARN] capsule gc {name}: failed to record limiter usage: {error}");
    }
    operation
}

fn read_gc_plan(path: &std::path::Path) -> Result<sync_store::CapsuleGcPlan> {
    let bytes = std::fs::read(path)
        .map_err(|error| anyhow!("read capsule GC plan {}: {error}", path.display()))?;
    sync_store::decode_capsule_gc_plan(&bytes)
        .map_err(|error| anyhow!("decode capsule GC plan {}: {error}", path.display()))
}
