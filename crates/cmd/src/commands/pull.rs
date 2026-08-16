// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `pond pull [<name>]` -- pull the content-addressed object graph from one or
//! more remotes.  A root (or absent) mount path mirrors the source into the
//! local pond; a non-root mount path is a cross-pond import that rebuilds the
//! foreign pond's tree under its own pond_id and mounts it at the path.

use crate::commands::remote::{
    RemoteMode, list_remote_names, load_remote_attachment, remote_mode_for,
};
use crate::common::ShipContext;
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use steward::REMOTE_MOUNT_PATH_PREFIX;

/// Pull from `name`, or from every remote in `pull`/`both` mode when `name`
/// is `None`.  Each remote is processed independently.
pub async fn pull_command(ship_context: &ShipContext, name: Option<String>) -> Result<()> {
    pull_command_with_rebuild(ship_context, name, false).await
}

pub async fn pull_command_with_rebuild(
    ship_context: &ShipContext,
    name: Option<String>,
    rebuild_graft: bool,
) -> Result<()> {
    if rebuild_graft && name.is_none() {
        return Err(anyhow!(
            "`pond pull --rebuild-graft` requires one remote name"
        ));
    }
    let mut ship = ship_context.open_pond().await?;

    let targets: Vec<String> = if let Some(n) = name {
        vec![n]
    } else {
        let all = list_remote_names(&mut ship).await?;
        let mut filtered = Vec::new();
        for n in all {
            match remote_mode_for(&ship, &n).await? {
                RemoteMode::Pull | RemoteMode::Both => filtered.push(n),
                RemoteMode::Push => {
                    log::debug!("skip {}: mode=push", n);
                }
            }
        }
        filtered
    };

    if targets.is_empty() {
        log::info!("no remotes to pull from");
        return Ok(());
    }

    // Carry the causes, not just the count -- the same reason `pond push`
    // does.  Now that ingress is governed, a pull can fail for a reason the
    // operator is expected to act on (an exhausted budget, with a retry time),
    // and burying that in the log while returning "one or more pulls failed"
    // makes a routine throttle look like an outage.
    let mut failures: Vec<String> = Vec::new();
    for name in targets {
        if let Err(e) = pull_one(&mut ship, &name, rebuild_graft).await {
            log::error!("[ERR] pull {}: {}", name, e);
            failures.push(format!("{}: {}", name, e));
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("pull failed -- {}", failures.join("; ")))
    }
}

async fn pulled_frontier(
    ship: &mut steward::Ship,
    url: &str,
    name: &str,
    graft: Option<(&str, uuid::Uuid)>,
) -> Result<Option<String>> {
    let watermark = ship
        .control_table()
        .raw_config_get(&format!("last_pulled_tip:{url}"))
        .await
        .map_err(|e| anyhow!("read last_pulled_tip for `{name}`: {e}"))?
        .filter(|tip| !tip.is_empty());
    if let Some((mount_path, foreign_pond_id)) = graft {
        let pin_path = steward::GraftPin::pin_path(name);
        let tx = ship
            .begin_read(&steward::PondUserMetadata::new(vec![
                "pull".to_string(),
                "read-graft-pin".to_string(),
                name.to_string(),
            ]))
            .await?;
        let pin_bytes = {
            let root = tx.root().await?;
            if root.exists(&pin_path).await {
                Some(root.read_file_path_to_vec(&pin_path).await?)
            } else {
                None
            }
        };
        let _ = tx.commit().await?;
        let Some(bytes) = pin_bytes else {
            return Ok(watermark);
        };
        let pin = steward::GraftPin::from_yaml_bytes(&bytes)
            .map_err(|e| anyhow!("parse graft pin `{pin_path}`: {e}"))?;
        let same_mount = pin.mount_path.trim_end_matches('/') == mount_path.trim_end_matches('/');
        if pin.foreign_pond_id == foreign_pond_id.to_string() && same_mount {
            return Ok(Some(pin.pinned_tip));
        }
        return Ok(None);
    }
    Ok(watermark)
}

/// Return `true` when the remote's tip commit for ref `main` already equals the
/// durable graft pin (or mirror watermark), so the graph fetch can be skipped.
async fn already_at_tip(
    ship: &mut steward::Ship,
    remote: &dyn steward::ContentSource,
    url: &str,
    name: &str,
    graft: Option<(&str, uuid::Uuid)>,
) -> Result<bool> {
    let remote_tip = remote
        .get_tip("main")
        .await
        .map_err(|e| anyhow!("get tip from `{url}`: {e}"))?;
    let last_pulled = pulled_frontier(ship, url, name, graft).await?;
    if let (Some(tip), Some(prev)) = (remote_tip, last_pulled.as_deref())
        && tip.to_hex() == prev
    {
        if graft.is_some() {
            let key = format!("last_pulled_tip:{url}");
            let tip_hex = tip.to_hex();
            let watermark = ship
                .control_table()
                .raw_config_get(&key)
                .await
                .map_err(|e| anyhow!("read last_pulled_tip for `{name}`: {e}"))?;
            if watermark.as_deref() != Some(tip_hex.as_str()) {
                ship.control_table_mut()
                    .raw_config_set(&key, &tip_hex)
                    .await
                    .map_err(|e| anyhow!("repair last_pulled_tip for `{name}`: {e}"))?;
            }
        }
        log::info!("[OK] pull {name} already up to date (tip={prev})");
        return Ok(true);
    }
    Ok(false)
}

async fn require_fast_forward(
    ship: &mut steward::Ship,
    graph: &steward::FetchedGraph,
    url: &str,
    name: &str,
    graft: Option<(&str, uuid::Uuid)>,
) -> Result<()> {
    let Some(previous) = pulled_frontier(ship, url, name, graft).await? else {
        return Ok(());
    };
    let previous_hash = sync_store::content::ObjectHash::from_hex(&previous)
        .map_err(|e| anyhow!("invalid last_pulled_tip for `{name}`: {e}"))?;
    if graph
        .commits
        .iter()
        .any(|(commit_hash, _)| *commit_hash == previous_hash)
    {
        return Ok(());
    }
    let remote_tip = graph
        .tip
        .map(|tip| tip.to_hex())
        .unwrap_or_else(|| "<empty>".to_string());
    Err(anyhow!(
        "remote `{name}` tip {remote_tip} does not descend from last pulled tip {previous}; refusing non-fast-forward pull"
    ))
}

/// Open a [`steward::ContentSource`] for `attachment`: a `pond://<path>` URL
/// resolves to a producer pond clone on local disk
/// ([`steward::LocalPondSource`]) for the local develop-and-preview workflow;
/// any other URL (`s3://`, `file://`) opens a content-addressed remote store.
/// `storage_options` is resolved by the caller (Decision A5), since reading a
/// storage profile needs the pond and this function must not.
async fn open_content_source(
    attachment: &steward::RemoteAttachment,
    storage_options: HashMap<String, String>,
) -> Result<Box<dyn steward::ContentSource>> {
    if let Some(path) = attachment.url.strip_prefix("pond://") {
        let source = steward::LocalPondSource::open(path)
            .await
            .map_err(|e| anyhow!("open local pond source at `{}`: {}", path, e))?;
        Ok(Box::new(source))
    } else {
        let remote = sync_store::ContentRemote::open_at_url(&attachment.url, storage_options)
            .await
            .map_err(|e| anyhow!("open content remote at `{}`: {}", attachment.url, e))?;
        Ok(Box::new(remote))
    }
}

async fn pull_one(ship: &mut steward::Steward, name: &str, rebuild_graft: bool) -> Result<()> {
    let attachment = load_remote_attachment(ship, name).await?;

    // One dispatch, from the profile when there is one (Decision A8).  A
    // `pond://` source uses no storage options at all, but resolving here
    // keeps the rule in one place.
    let storage_options = {
        let pond = ship
            .as_pond_mut()
            .ok_or_else(|| anyhow!("pull requires a pond steward (not a host steward)"))?;
        steward::storage_profile::prepare_storage(pond, &attachment).await?
    };

    // Bind the limiters before touching the network, so a missing node or a
    // wrong unit fails for free rather than halfway through a transfer.
    let limit_spec = attachment.resolved_limits()?;
    let ship_mut = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("pull requires a pond steward (not a host steward)"))?;
    let mut limits = steward::LimiterSet::open(ship_mut, &limit_spec)
        .await
        .map_err(|e| anyhow!("bind limiters for remote `{}`: {}", name, e))?;

    let ship_pre = ship
        .as_pond()
        .ok_or_else(|| anyhow!("pull requires a pond steward (not a host steward)"))?;
    let mount_path: Option<String> = ship_pre
        .control_table()
        .raw_config_get(&format!("{REMOTE_MOUNT_PATH_PREFIX}{name}"))
        .await
        .map_err(|e| anyhow!("read mount_path for `{}`: {}", name, e))?
        .filter(|s| !s.is_empty() && s != "/");

    // Bind the budget to the remote's URL before opening it, so the open is
    // charged too: opening a Delta table lists the log and reads every commit
    // since the last checkpoint, which is not a local act.  Charging follows
    // the URL rather than the call, so the local pond's own traffic is
    // structurally outside this budget -- it goes somewhere else.
    let guard = steward::storage_meter::MeterGuard::new(&attachment.url, &mut limits);
    let opened = open_content_source(&attachment, storage_options).await;
    let source = match opened {
        Ok(s) => steward::metered_source::MeteredSource::with_guard(s.into(), guard),
        Err(e) => {
            // The open spent whatever it spent before failing; return it
            // before reporting, so a remote that fails to open on a timer
            // cannot be retried for free.
            let refusal = guard.finish(&mut limits);
            let ship_mut = ship
                .as_pond_mut()
                .ok_or_else(|| anyhow!("pull requires a pond steward (not a host steward)"))?;
            if let Err(e) = limits.commit(ship_mut.control_table_mut()).await {
                log::warn!(
                    "[WARN] pull {}: failed to record limiter usage: {}",
                    name,
                    e
                );
            }
            return Err(match refusal {
                Some(r) => anyhow::Error::new(r),
                None => anyhow!("open remote `{}` ({}): {}", name, attachment.url, e),
            });
        }
    };

    // Mirror restart / backup restore (root or no mount): pull the full
    // content graph and rebuild the local pond by node_id.  Cross-pond import
    // (non-root mount): fetch the foreign content graph and rebuild it under
    // the foreign pond_id, then mount it.
    let result: Result<()> = match mount_path {
        None if rebuild_graft => Err(anyhow!(
            "remote `{name}` is a mirror; --rebuild-graft only replaces a non-root graft"
        )),
        None => pull_mirror(ship, name, &attachment, &source).await,
        Some(mount_path) => {
            pull_import(ship, name, &attachment, &source, &mount_path, rebuild_graft).await
        }
    };

    // A budget's refusal outranks the storage error it surfaced as, so an
    // exhausted limit reads as a throttle rather than as an outage.
    let result = match source.finish(&mut limits) {
        Some(refusal) if result.is_err() => Err(anyhow::Error::new(refusal)),
        _ => result,
    };

    // Persist the windows whether or not the pull succeeded, for the same
    // reason a push does: a pull that failed partway still transferred what it
    // transferred, and a budget that forgets the spending of failed attempts is
    // a budget a retry loop can spend without bound.
    let ship_mut = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("pull requires a pond steward (not a host steward)"))?;
    if let Err(e) = limits.commit(ship_mut.control_table_mut()).await {
        log::warn!(
            "[WARN] pull {}: failed to record limiter usage: {}",
            name,
            e
        );
    }

    result
}

/// Cross-pond import: fetch the foreign content graph, rebuild it under the
/// foreign pond_id, and mount it at `mount_path` (guaranteed non-root).
async fn pull_import(
    ship: &mut steward::Steward,
    name: &str,
    attachment: &steward::RemoteAttachment,
    remote: &dyn steward::ContentSource,
    mount_path: &str,
    rebuild_graft: bool,
) -> Result<()> {
    let ship_ref = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("pull requires a pond steward (not a host steward)"))?;

    let local_pond_id = ship_ref.control_table().pond_id_uuid();
    let foreign_pond_id = remote.pond_id();

    if foreign_pond_id == local_pond_id {
        return Err(anyhow!(
            "remote `{}` has mount_path `{}` but its store_id matches this pond's \
             pond_id; cross-pond import requires a foreign store_id",
            name,
            mount_path
        ));
    }

    // Incremental short-circuit (CA3): if the remote tip already equals the
    // tip we last pulled, the mount is up to date -- skip the full graph fetch
    // and re-import entirely.  This is the bandwidth-bug guard: without it,
    // every pull re-walks and re-downloads the whole reachable object closure.
    let graft_identity = Some((mount_path, remote.pond_id()));
    if !rebuild_graft
        && already_at_tip(ship_ref, remote, &attachment.url, name, graft_identity).await?
    {
        return Ok(());
    }

    // Fetch the foreign object graph and rebuild it under the foreign pond_id
    // partition, then mount it.  The local allocator stays contiguous; only the
    // foreign pond's seq frontier advances inside `import_pond`.
    let graph = steward::fetch_object_graph(remote, "main")
        .await
        .map_err(|e| anyhow!("fetch from `{}`: {}", attachment.url, e))?;
    if graph.is_empty() {
        log::info!(
            "pull {}: remote ref `main` is empty; nothing to import",
            name
        );
        return Ok(());
    }
    require_fast_forward(ship_ref, &graph, &attachment.url, name, graft_identity).await?;
    let foreign_uuid7 = uuid7::Uuid::from(*foreign_pond_id.as_bytes());
    let pinned_tip = graph
        .tip
        .ok_or_else(|| anyhow!("imported graph from `{}` has no tip commit", name))?;
    let outcome = if rebuild_graft {
        steward::replace_graft(ship_ref, remote, &graph, foreign_uuid7, name, mount_path).await
    } else {
        steward::import_graft(ship_ref, remote, &graph, foreign_uuid7, name, mount_path).await
    }
    .map_err(|e| anyhow!("import from `{}`: {}", attachment.url, e))?;
    log::info!(
        "[OK] pull {} complete (cross-pond import: {:?})",
        name,
        outcome
    );

    // Record the per-ref frontier we last pulled: the foreign tip commit hash
    // now atomically imported, mounted, and pinned. If this control-table write
    // fails, a retry safely repeats the idempotent graft transaction.
    ship_ref
        .control_table_mut()
        .raw_config_set(
            &format!("last_pulled_tip:{}", attachment.url),
            &pinned_tip.to_hex(),
        )
        .await
        .map_err(|e| anyhow!("record last_pulled_tip for `{}`: {}", name, e))?;

    Ok(())
}

/// Mirror restart / backup restore: fetch the remote's full content graph
/// for ref `main` and rebuild the local pond by node_id.  Used when the
/// attachment has no mount path (or `/`).
async fn pull_mirror(
    ship: &mut steward::Steward,
    name: &str,
    attachment: &steward::RemoteAttachment,
    remote: &dyn steward::ContentSource,
) -> Result<()> {
    let ship_ref = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("pull requires a pond steward (not a host steward)"))?;

    // Incremental short-circuit (CA3): skip the full graph fetch and rebuild
    // when the mirror already reflects the remote tip.
    if already_at_tip(ship_ref, remote, &attachment.url, name, None).await? {
        return Ok(());
    }

    let graph = steward::fetch_object_graph(remote, "main")
        .await
        .map_err(|e| anyhow!("fetch from `{}`: {}", attachment.url, e))?;
    if graph.is_empty() {
        log::info!(
            "pull {}: remote ref `main` is empty; nothing to rebuild",
            name
        );
        return Ok(());
    }
    require_fast_forward(ship_ref, &graph, &attachment.url, name, None).await?;
    let outcome = steward::rebuild_pond(ship_ref, remote, &graph)
        .await
        .map_err(|e| anyhow!("rebuild from `{}`: {}", attachment.url, e))?;
    log::info!(
        "[OK] pull {} complete (mirror rebuild: {:?})",
        name,
        outcome
    );

    // Record the per-ref frontier we last pulled: the tip commit hash the
    // mirror now reflects (CA3 replacement for the retired seq watermark).
    if let Some(tip) = graph.tip {
        ship_ref
            .control_table_mut()
            .raw_config_set(
                &format!("last_pulled_tip:{}", attachment.url),
                &tip.to_hex(),
            )
            .await
            .map_err(|e| anyhow!("record last_pulled_tip for `{}`: {}", name, e))?;
    }
    Ok(())
}

/// Split an absolute mount path into (parent_dir, leaf_name).
/// Errors if the path is `/` (root mount is mirror mode, handled
/// elsewhere) or has no leaf segment.
pub(crate) fn split_mount_path(path: &str) -> Result<(&str, &str)> {
    steward::split_mount_path(path).map_err(anyhow::Error::msg)
}

#[cfg(test)]
mod tests {
    use super::split_mount_path;

    #[test]
    fn split_mount_path_top_level() {
        assert_eq!(split_mount_path("/imports").unwrap(), ("/", "imports"));
    }

    #[test]
    fn split_mount_path_nested() {
        assert_eq!(
            split_mount_path("/imports/upstream").unwrap(),
            ("/imports", "upstream")
        );
    }

    #[test]
    fn split_mount_path_trailing_slash() {
        assert_eq!(
            split_mount_path("/imports/upstream/").unwrap(),
            ("/imports", "upstream")
        );
    }

    #[test]
    fn split_mount_path_root_rejected() {
        assert!(split_mount_path("/").is_err());
    }

    #[test]
    fn split_mount_path_relative_rejected() {
        assert!(split_mount_path("imports/upstream").is_err());
    }
}
