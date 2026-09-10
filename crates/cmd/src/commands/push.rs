// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `pond push [<name>]` -- push the pond's content closure to one or more
//! remotes via the content-addressed [`sync_store::ContentRemote`] pipeline.

use crate::commands::remote::{
    RemoteMode, list_remote_names, load_remote_attachment, remote_mode_for,
};
use crate::common::ShipContext;
use anyhow::{Result, anyhow};

/// Push to `name`, or to every remote in `push`/`both` mode when `name` is
/// `None`.  Each remote is processed independently: a failure on one does
/// NOT halt the others.
pub async fn push_command(ship_context: &ShipContext, name: Option<String>) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;

    let targets: Vec<String> = if let Some(n) = name {
        vec![n]
    } else {
        let all = list_remote_names(&mut ship).await?;
        let mut filtered = Vec::new();
        for n in all {
            match remote_mode_for(&ship, &n).await? {
                RemoteMode::Push | RemoteMode::Both => filtered.push(n),
                RemoteMode::Pull => {
                    log::debug!("skip {}: mode=pull", n);
                }
            }
        }
        filtered
    };

    if targets.is_empty() {
        log::info!("no remotes to push to");
        return Ok(());
    }

    // Carry the causes, not just the count.  A push can now fail for a
    // reason the operator is expected to act on -- an exhausted budget, with
    // a retry time -- and burying that in the log while returning "one or
    // more pushes failed" makes a routine throttle look like an outage.
    let mut failures: Vec<String> = Vec::new();
    for name in targets {
        if let Err(e) = push_one(&mut ship, &name).await {
            log::error!("[ERR] push {}: {}", name, e);
            failures.push(format!("{}: {}", name, e));
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("push failed -- {}", failures.join("; ")))
    }
}

/// Push the pond's current content closure and tip commit to the named
/// remote under the `main` ref via the content-addressed pipeline.
async fn push_one(ship: &mut steward::Steward, name: &str) -> Result<()> {
    let attachment = load_remote_attachment(ship, name).await?;
    let ship_ref = ship
        .as_pond()
        .ok_or_else(|| anyhow!("push requires a pond steward (not a host steward)"))?;
    if steward::remote_ref_is_acknowledged(ship_ref, &attachment.url, "main").await? {
        log::info!(
            "[OK] push {} skipped: remote already acknowledged the current content tip",
            name
        );
        return Ok(());
    }

    // One dispatch, from the profile when there is one and from the URL only
    // when there is not (Decision A8).
    let ship_pre = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("push requires a pond steward (not a host steward)"))?;
    let storage_options = steward::storage_profile::prepare_storage(ship_pre, &attachment).await?;

    // Bind the limiters before touching the network, so a missing node or a
    // wrong unit fails for free rather than halfway through a transfer.
    let limit_spec = attachment.resolved_limits()?;
    let ship_mut = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("push requires a pond steward (not a host steward)"))?;
    let mut limits = steward::LimiterSet::open(ship_mut, &limit_spec)
        .await
        .map_err(|e| anyhow!("bind limiters for remote `{}`: {}", name, e))?;

    let ship_ref = ship
        .as_pond()
        .ok_or_else(|| anyhow!("push requires a pond steward (not a host steward)"))?;

    // Open inside the budget, not before it: opening a Delta table lists the
    // log and reads every commit since the last checkpoint, which is the
    // larger half of a push's traffic.
    let pushed = steward::open_and_push_to_remote_limited(
        ship_ref,
        &attachment.url,
        storage_options,
        "main",
        &mut limits,
    )
    .await;

    // Persist the windows whether or not the push succeeded: a push that
    // failed partway still transferred what it transferred, and a budget that
    // forgets the spending of failed attempts is a budget a retry loop can
    // spend without bound -- the exact failure this exists to prevent.
    let ship_mut = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("push requires a pond steward (not a host steward)"))?;
    if let Err(e) = limits.commit(ship_mut.control_table_mut()).await {
        log::warn!(
            "[WARN] push {}: failed to record limiter usage: {}",
            name,
            e
        );
    }

    let outcome = pushed.map_err(|e| anyhow!("push {} ({}): {}", name, attachment.url, e))?;

    let tip_hex = outcome.tip.to_hex();
    log::info!(
        "[OK] push {} complete (objects_pushed={}, tip={})",
        name,
        outcome.objects_pushed,
        tip_hex
    );

    steward::write_push_ack(ship.control_table_mut(), &attachment.url, &outcome.state)
        .await
        .map_err(|e| anyhow!("record publication acknowledgement for `{}`: {}", name, e))?;
    Ok(())
}

/// Delete abandoned immutable-payload upload staging objects older than the
/// requested grace period.
pub async fn cleanup_backup_uploads_command(
    ship_context: &ShipContext,
    name: &str,
    older_than_seconds: i64,
) -> Result<()> {
    if older_than_seconds < 0 {
        return Err(anyhow!("older-than-seconds must not be negative"));
    }
    let mut ship = ship_context.open_pond().await?;
    match remote_mode_for(&ship, name).await? {
        RemoteMode::Push | RemoteMode::Both => {}
        RemoteMode::Pull => {
            return Err(anyhow!(
                "remote `{name}` is pull-only; upload cleanup applies to backups"
            ));
        }
    }
    let attachment = load_remote_attachment(&mut ship, name).await?;
    let storage_options = {
        let pond = ship
            .as_pond_mut()
            .ok_or_else(|| anyhow!("upload cleanup requires a pond steward"))?;
        steward::storage_profile::prepare_storage(pond, &attachment).await?
    };
    let limit_spec = attachment.resolved_limits()?;
    let mut limits = {
        let pond = ship
            .as_pond_mut()
            .ok_or_else(|| anyhow!("upload cleanup requires a pond steward"))?;
        steward::LimiterSet::open(pond, &limit_spec)
            .await
            .map_err(|error| anyhow!("bind limiters for backup `{name}`: {error}"))?
    };
    let url = attachment.url.clone();
    let cleanup_url = url.clone();
    let age = chrono::Duration::try_seconds(older_than_seconds)
        .ok_or_else(|| anyhow!("older-than-seconds is out of range"))?;
    let cutoff = chrono::Utc::now()
        .checked_sub_signed(age)
        .ok_or_else(|| anyhow!("older-than-seconds is out of range"))?;
    let cleanup = steward::storage_meter::metered_op(
        &url,
        &mut limits,
        Box::pin(async move {
            let remote = sync_store::ContentRemote::open_at_url(&cleanup_url, storage_options)
                .await
                .map_err(anyhow::Error::from)?;
            remote
                .cleanup_stale_uploads(cutoff)
                .await
                .map_err(anyhow::Error::from)
        }),
    )
    .await;

    let pond = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("upload cleanup requires a pond steward"))?;
    limits
        .commit(pond.control_table_mut())
        .await
        .map_err(|error| anyhow!("record upload-cleanup limiter usage: {error}"))?;
    let outcome = cleanup.map_err(|error| anyhow!("clean backup `{name}` uploads: {error}"))?;
    log::info!(
        "[OK] backup {} upload cleanup complete (objects_deleted={}, bytes_deleted={})",
        name,
        outcome.objects_deleted,
        outcome.bytes_deleted
    );
    Ok(())
}

/// Publish locally maintained whole-range series packs to one named backup.
pub async fn publish_consolidated_packs_command(
    ship_context: &ShipContext,
    name: &str,
) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;
    match remote_mode_for(&ship, name).await? {
        RemoteMode::Push | RemoteMode::Both => {}
        RemoteMode::Pull => {
            return Err(anyhow!(
                "remote `{name}` is pull-only; consolidated packs may be published only to a \
                 push/both backup"
            ));
        }
    }
    let attachment = load_remote_attachment(&mut ship, name).await?;
    let storage_options = {
        let pond = ship
            .as_pond_mut()
            .ok_or_else(|| anyhow!("consolidated-pack publication requires a pond steward"))?;
        steward::storage_profile::prepare_storage(pond, &attachment).await?
    };
    let limit_spec = attachment.resolved_limits()?;
    let mut limits = {
        let pond = ship
            .as_pond_mut()
            .ok_or_else(|| anyhow!("consolidated-pack publication requires a pond steward"))?;
        steward::LimiterSet::open(pond, &limit_spec)
            .await
            .map_err(|error| anyhow!("bind limiters for backup `{name}`: {error}"))?
    };
    let published = {
        let pond = ship
            .as_pond()
            .ok_or_else(|| anyhow!("consolidated-pack publication requires a pond steward"))?;
        steward::open_and_publish_local_consolidated_packs_limited(
            pond,
            &attachment.url,
            storage_options,
            "main",
            &mut limits,
        )
        .await
    };

    let usage_error = {
        let pond = ship
            .as_pond_mut()
            .ok_or_else(|| anyhow!("consolidated-pack publication requires a pond steward"))?;
        limits.commit(pond.control_table_mut()).await.err()
    };
    let outcome = match (published, usage_error) {
        (Ok(outcome), None) => outcome,
        (Ok(_), Some(error)) => {
            return Err(anyhow!(
                "published consolidated packs to `{name}`, but failed to record limiter usage: \
                 {error}"
            ));
        }
        (Err(error), None) => {
            return Err(anyhow!(
                "publish consolidated packs to `{name}` ({}): {error}",
                attachment.url
            ));
        }
        (Err(error), Some(usage_error)) => {
            return Err(anyhow!(
                "publish consolidated packs to `{name}` ({}): {error}; additionally failed to \
                 record limiter usage: {usage_error}",
                attachment.url
            ));
        }
    };

    for selection in &outcome.selections {
        log::info!(
            "[PACK] series={} pack={} objects={} bytes={}",
            selection.series_hash,
            selection.pack_hash,
            selection.objects,
            selection.bytes
        );
    }
    log::info!(
        "[OK] backup {} consolidated packs published (series={}, packs={}, objects={}, bytes={}, \
         objects_created={}, bytes_created={}, packs_created={}, locators_created={})",
        name,
        outcome.selections.len(),
        outcome.selections.len(),
        outcome.objects_selected,
        outcome.bytes_selected,
        outcome.objects_created,
        outcome.bytes_created,
        outcome.packs_created,
        outcome.locators_created
    );
    Ok(())
}
