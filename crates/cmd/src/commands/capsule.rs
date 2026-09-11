// SPDX-License-Identifier: Apache-2.0

//! Operator commands for portable recovery capsules.

use anyhow::{Result, anyhow};

use crate::commands::remote::{RemoteMode, load_remote_attachment, remote_mode_for};
use crate::common::ShipContext;

/// Static recovery-recipe operation selected by the CLI.
#[derive(Clone, Copy)]
pub enum RecoveryRecipeAction {
    /// Install immutable and discoverable bootstrap objects.
    Publish,
    /// Verify both bootstrap objects against the reviewed build.
    Inspect,
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

/// Materialize a downloaded recovery capsule into a brand-new pond at
/// `target`.
///
/// See [`steward::import_capsule`] for the full staged-import contract
/// (fresh identity, suppressed post-commit dispatch during staging,
/// atomic rename only after the staged result re-verifies against the
/// capsule's logical contract).
pub async fn capsule_import_command(
    path: &std::path::Path,
    target: &std::path::Path,
    birthplace: &str,
) -> Result<()> {
    let report = steward::import_capsule(path, target, birthplace.to_string())
        .await
        .map_err(|error| {
            anyhow!(
                "import recovery capsule from {} into {}: {error}",
                path.display(),
                target.display()
            )
        })?;
    log::info!(
        "[OK] capsule imported into {} (pond_id={}, source_pond_id={}, capsule_root={}, \
         entries={}, directories={}, physical={}, symlinks={}, dynamic={}, logical_count={}, \
         batches={})",
        report.target.display(),
        report.target_pond_id,
        report.source_pond_id,
        report.capsule_root,
        report.entries,
        report.directories,
        report.physical,
        report.symlinks,
        report.dynamic,
        report.logical_count,
        report.batches
    );
    log::warn!(
        "capsule import persistently disabled automatic post-commit factories and remote pushes \
         at {}; run `POND={} pond capsule activate` after repairing any restored remote or \
         automatic factory configuration",
        report.target.display(),
        report.target.display()
    );
    Ok(())
}

/// Preflight restored automatic configuration and enable post-commit
/// dispatch only if every remote and `/system/run/*` config is safe.
pub async fn capsule_activate_command(target: &std::path::Path) -> Result<()> {
    let report = steward::activate_capsule_import(target)
        .await
        .map_err(|error| anyhow!("activate capsule import at {}: {error}", target.display()))?;
    log::info!(
        "[OK] capsule import activated at {} (remotes={}, run_configs={})",
        report.target.display(),
        report.remotes,
        report.run_configs
    );
    Ok(())
}

/// Build the current logical snapshot and publish an immutable
/// `pondcapsule.4` generation to one backup.
pub async fn capsule_publish_command(ship_context: &ShipContext, name: &str) -> Result<()> {
    let mut steward = ship_context.open_pond().await?;
    match remote_mode_for(&steward, name).await? {
        RemoteMode::Push | RemoteMode::Both => {}
        RemoteMode::Pull => {
            return Err(anyhow!(
                "remote `{name}` is pull-only; capsules publish only to backup remotes"
            ));
        }
    }
    let attachment = load_remote_attachment(&mut steward, name).await?;
    let storage_options = {
        let pond = steward
            .as_pond_mut()
            .ok_or_else(|| anyhow!("capsule publish requires a pond steward"))?;
        steward::storage_profile::prepare_storage(pond, &attachment).await?
    };
    let limit_spec = attachment.resolved_limits()?;
    let mut limits = {
        let pond = steward
            .as_pond_mut()
            .ok_or_else(|| anyhow!("capsule publish requires a pond steward"))?;
        steward::LimiterSet::open(pond, &limit_spec)
            .await
            .map_err(|error| anyhow!("bind limiters for remote `{name}`: {error}"))?
    };
    let build = steward::build_recovery_capsule(
        steward
            .as_pond()
            .ok_or_else(|| anyhow!("capsule publish requires a pond steward"))?,
    )
    .await?;
    let manifest = build.manifest.clone();
    let objects_dir = build.payloads.objects_dir().to_path_buf();
    let url = attachment.url.clone();
    let publish_url = url.clone();
    let operation = steward::storage_meter::metered_op(
        &url,
        &mut limits,
        Box::pin(async move {
            let remote = sync_store::ContentRemote::open_at_url(&publish_url, storage_options)
                .await
                .map_err(|error| anyhow!("open backup for capsule publication: {error}"))?;
            remote
                .publish_capsule_directory(&manifest, &objects_dir)
                .await
                .map_err(|error| anyhow!("publish capsule: {error}"))
        }),
    )
    .await;
    let pond = steward
        .as_pond_mut()
        .ok_or_else(|| anyhow!("capsule publish requires a pond steward"))?;
    limits
        .commit(pond.control_table_mut())
        .await
        .map_err(|error| anyhow!("record capsule-publication limiter usage: {error}"))?;
    let outcome = operation?;
    log::info!(
        "[OK] capsule published to {} (root={}, payloads_uploaded={}, payloads_total={})",
        name,
        outcome.root,
        outcome.payloads_uploaded,
        outcome.payloads_total
    );
    Ok(())
}

/// Publish or inspect one explicit native-format recovery recipe.
pub async fn capsule_recipe_command(
    ship_context: &ShipContext,
    name: &str,
    action: RecoveryRecipeAction,
) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;
    if matches!(action, RecoveryRecipeAction::Publish) {
        match remote_mode_for(&ship, name).await? {
            RemoteMode::Push | RemoteMode::Both => {}
            RemoteMode::Pull => {
                return Err(anyhow!(
                    "remote `{name}` is pull-only; recovery recipes publish only to backup remotes"
                ));
            }
        }
    }
    let attachment = load_remote_attachment(&mut ship, name).await?;
    let pond = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("capsule recipe requires a pond steward"))?;
    let storage_options = steward::storage_profile::prepare_storage(pond, &attachment).await?;
    let limit_spec = attachment.resolved_limits()?;
    let pond = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("capsule recipe requires a pond steward"))?;
    let mut limits = steward::LimiterSet::open(pond, &limit_spec)
        .await
        .map_err(|error| anyhow!("bind limiters for remote `{name}`: {error}"))?;

    let operation: Result<()> = match action {
        RecoveryRecipeAction::Publish => {
            steward::open_and_publish_recovery_recipe_limited(
                &attachment.url,
                storage_options,
                &mut limits,
            )
            .await
            .map_err(|error| anyhow!("capsule recipe publish {name}: {error}"))
            .map(|outcome| {
                log::info!(
                    "[OK] recovery recipe installed (native_format=watertown.commit.v1, capsule_format=pondcapsule.4, hash={}, versioned_created={}, discoverable_created={})",
                    outcome.recipe_hash,
                    outcome.versioned_created,
                    outcome.discoverable_created
                );
            })
        }
        RecoveryRecipeAction::Inspect => {
            steward::open_and_inspect_recovery_recipe_limited(
                &attachment.url,
                storage_options,
                &mut limits,
            )
            .await
            .map_err(|error| anyhow!("capsule recipe inspect {name}: {error}"))
            .map(|hash| {
                log::info!(
                    "[OK] recovery recipe verified (native_format=watertown.commit.v1, capsule_format=pondcapsule.4, hash={hash})"
                );
            })
        }
    };

    let pond = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("capsule recipe requires a pond steward"))?;
    if let Err(error) = limits.commit(pond.control_table_mut()).await {
        log::warn!("[WARN] capsule recipe {name}: failed to record limiter usage: {error}");
    }
    operation
}
