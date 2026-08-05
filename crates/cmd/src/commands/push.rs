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

    let mut remote = sync_store::ContentRemote::open_at_url(&attachment.url, storage_options)
        .await
        .map_err(|e| anyhow!("open remote `{}` ({}): {}", name, attachment.url, e))?;

    let pushed =
        steward::push_content_to_remote_limited(ship_ref, &mut remote, "main", &mut limits).await;

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

    // Record the per-ref frontier: the single commit hash we last pushed to
    // this remote (the CA3 replacement for the retired per-pond seq watermark).
    ship.control_table_mut()
        .raw_config_set(&format!("last_pushed_tip:{}", attachment.url), &tip_hex)
        .await
        .map_err(|e| anyhow!("record last_pushed_tip for `{}`: {}", name, e))?;
    Ok(())
}
