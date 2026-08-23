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
         entries={}, directories={}, physical={}, symlinks={}, dynamic={}, logical_count={})",
        report.target.display(),
        report.target_pond_id,
        report.source_pond_id,
        report.capsule_root,
        report.entries,
        report.directories,
        report.physical,
        report.symlinks,
        report.dynamic,
        report.logical_count
    );
    log::warn!(
        "capsule import persistently disabled automatic post-commit factories and remote pushes \
         at {}; review and preflight the restored namespace before setting \
         `post_commit_dispatch` to `enabled` with `pond control set-config`",
        report.target.display()
    );
    Ok(())
}

/// Publish or inspect the static native-format recovery recipe.
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
                    "[OK] recovery recipe installed (hash={}, versioned_created={}, discoverable_created={})",
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
                log::info!("[OK] recovery recipe verified (hash={hash}, format=dp.commit.3)");
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
