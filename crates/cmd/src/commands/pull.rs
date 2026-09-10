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
    let _ = url;
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
            return Ok(None);
        };
        let pin = steward::GraftPin::from_yaml_bytes(&bytes)
            .map_err(|e| anyhow!("parse graft pin `{pin_path}`: {e}"))?;
        let same_mount = pin.mount_path.trim_end_matches('/') == mount_path.trim_end_matches('/');
        if pin.foreign_pond_id == foreign_pond_id.to_string() && same_mount {
            return Ok(Some(pin.pinned_tip));
        }
        return Ok(None);
    }
    Ok(None)
}

struct PullPosition {
    previous: Option<sync_store::content::ObjectHash>,
    pinned: Option<sync_store::content::ObjectHash>,
    acknowledged: Option<sync_store::PublicationState>,
    remote_state: Option<sync_store::PublicationState>,
    exact_ack: bool,
    already_at_tip: bool,
}

/// Resolve the durable prior tip once and compare it with the remote's current
/// tip, so graph fetching and fast-forward validation share the same boundary.
async fn pull_position(
    ship: &mut steward::Ship,
    remote: &dyn steward::ContentSource,
    url: &str,
    name: &str,
    graft: Option<(&str, uuid::Uuid)>,
) -> Result<PullPosition> {
    let remote_state = remote
        .get_publication_state("main")
        .await
        .map_err(|e| anyhow!("get publication state from `{url}`: {e}"))?;
    let acknowledged = steward::read_pull_ack(ship.control_table(), url, remote.pond_id(), "main")
        .await
        .map_err(|e| anyhow!("read consumer acknowledgement for `{name}`: {e}"))?
        .map(|ack| ack.state(url, remote.pond_id(), "main"))
        .transpose()
        .map_err(|e| anyhow!("validate consumer acknowledgement for `{name}`: {e}"))?;
    let pinned = pulled_frontier(ship, url, name, graft)
        .await?
        .map(|tip| {
            sync_store::content::ObjectHash::from_hex(&tip)
                .map_err(|e| anyhow!("invalid graft pinned tip for `{name}`: {e}"))
        })
        .transpose()?;
    let previous = if graft.is_some() {
        pinned
    } else {
        acknowledged.as_ref().map(|state| state.snapshot_tip)
    };
    if let (Some(state), Some(acknowledged)) = (&remote_state, &acknowledged)
        && state.snapshot_tip == acknowledged.snapshot_tip
        && !steward::same_publication_identity(state, acknowledged)
    {
        return Err(anyhow!(
            "remote `{name}` publication identity changed at acknowledged tip {}; refusing to \
             overwrite the structured acknowledgement",
            state.snapshot_tip
        ));
    }

    let exact_ack = remote_state
        .as_ref()
        .zip(acknowledged.as_ref())
        .is_some_and(|(state, acknowledged)| {
            steward::same_publication_identity(state, acknowledged)
        });
    let pinned_at_head = graft.is_some()
        && remote_state
            .as_ref()
            .is_some_and(|state| Some(state.snapshot_tip) == pinned);
    let already_at_tip = if graft.is_some() {
        pinned_at_head
    } else {
        exact_ack
    };
    Ok(PullPosition {
        previous,
        pinned,
        acknowledged,
        remote_state,
        exact_ack,
        already_at_tip,
    })
}

async fn authenticate_exact_identity_noop(
    ship: &mut steward::Ship,
    remote: &dyn steward::ContentSource,
    url: &str,
    name: &str,
    mount_path: Option<&str>,
    position: &PullPosition,
) -> Result<()> {
    let state = position
        .remote_state
        .as_ref()
        .ok_or_else(|| anyhow!("remote `{name}` lost its publication row"))?;
    let commit = if let Some(acknowledged) = position
        .acknowledged
        .as_ref()
        .filter(|_| !position.exact_ack)
    {
        let graph =
            steward::fetch_object_graph_from_acknowledgement(remote, state.clone(), acknowledged)
                .await
                .map_err(|e| anyhow!("fetch acknowledgement lineage for `{name}`: {e}"))?;
        steward::authenticate_publication_window(&graph, acknowledged)
            .map_err(|e| anyhow!("authenticate acknowledgement lineage for `{name}`: {e}"))?;
        graph
            .commits
            .first()
            .map(|(_, commit)| commit.clone())
            .ok_or_else(|| anyhow!("authenticated publication for `{name}` has no tip commit"))?
    } else {
        steward::authenticated_publication_head_commit(remote, state)
            .await
            .map_err(|e| anyhow!("authenticate immutable publication head for `{name}`: {e}"))?
    };

    if let Some(mount_path) = mount_path {
        steward::authenticate_graft_publication_head(ship, state, &commit, name, mount_path)
            .await
            .map_err(|e| anyhow!("authenticate graft destination for `{name}`: {e}"))?;
    } else {
        let pond_id = ship.data_persistence().pond_id().to_string();
        steward::authenticate_destination_publication_head(ship, state, &commit, &pond_id)
            .await
            .map_err(|e| anyhow!("authenticate mirror destination for `{name}`: {e}"))?;
    }

    if !position.exact_ack {
        steward::write_pull_ack(ship.control_table_mut(), url, state)
            .await
            .map_err(|e| anyhow!("restore consumer acknowledgement for `{name}`: {e}"))?;
    }
    log::info!(
        "[OK] pull {name} already up to date (tip={})",
        state.snapshot_tip
    );
    Ok(())
}

async fn require_fast_forward(
    graph: &steward::FetchedGraph,
    name: &str,
    previous: Option<sync_store::content::ObjectHash>,
) -> Result<()> {
    let Some(previous_hash) = previous else {
        return Ok(());
    };
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
        "remote `{name}` tip {remote_tip} does not descend from last pulled tip {previous_hash}; refusing non-fast-forward pull"
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
        Some(refusal) => Err(anyhow::Error::new(refusal)),
        None => result,
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
    let position = pull_position(ship_ref, remote, &attachment.url, name, graft_identity).await?;
    if !rebuild_graft && position.already_at_tip {
        return authenticate_exact_identity_noop(
            ship_ref,
            remote,
            &attachment.url,
            name,
            Some(mount_path),
            &position,
        )
        .await;
    }
    // Fetch the foreign object graph and rebuild it under the foreign pond_id
    // partition, then mount it.  The local allocator stays contiguous; only the
    // foreign pond's seq frontier advances inside `import_pond`.
    let mut authenticated_boundary = None;
    let graph = match position.remote_state.clone() {
        None => steward::FetchedGraph::default(),
        Some(state) if rebuild_graft => {
            steward::fetch_object_graph_at_publication(remote, state, None)
                .await
                .map_err(|e| anyhow!("fetch from `{}`: {}", attachment.url, e))?
        }
        Some(state) => match (position.acknowledged.as_ref(), position.pinned) {
            (Some(acknowledged), pinned)
                if !steward::same_publication_identity(&state, acknowledged) =>
            {
                let authenticated =
                    steward::fetch_object_graph_from_acknowledgement(remote, state, acknowledged)
                        .await
                        .map_err(|e| anyhow!("fetch from `{}`: {}", attachment.url, e))?;
                steward::authenticate_publication_window(&authenticated, acknowledged)
                    .map_err(|e| anyhow!("authenticate graft lineage for `{name}`: {e}"))?;
                if let Some(pinned) = pinned {
                    let (boundary, remaining) = steward::narrow_authenticated_publication_window(
                        &authenticated,
                        acknowledged,
                        pinned,
                    )
                    .map_err(|e| anyhow!("authenticate graft pin for `{name}`: {e}"))?;
                    authenticated_boundary = Some(boundary);
                    remaining
                } else {
                    authenticated
                }
            }
            (None, Some(pinned)) => {
                let authenticated =
                    steward::fetch_object_graph_at_publication(remote, state, Some(pinned))
                        .await
                        .map_err(|e| anyhow!("fetch from `{}`: {}", attachment.url, e))?;
                let boundary =
                    steward::authenticate_pinned_publication_boundary(&authenticated, pinned)
                        .map_err(|e| {
                            anyhow!("authenticate pinned graft lineage for `{name}`: {e}")
                        })?;
                authenticated_boundary = Some(boundary);
                authenticated
            }
            _ => steward::fetch_object_graph_at_publication(remote, state, None)
                .await
                .map_err(|e| anyhow!("fetch from `{}`: {}", attachment.url, e))?,
        },
    };
    if !rebuild_graft {
        if let Some(boundary) = authenticated_boundary.as_ref() {
            let boundary_commit = graph
                .commits
                .iter()
                .find(|(hash, commit)| {
                    *hash == boundary.snapshot_tip && commit.manifest_root == boundary.manifest_root
                })
                .map(|(_, commit)| commit)
                .ok_or_else(|| {
                    anyhow!(
                        "authenticated graft boundary {} has no matching commit",
                        boundary.snapshot_tip
                    )
                })?;
            steward::authenticate_graft_publication_head(
                ship_ref,
                boundary,
                boundary_commit,
                name,
                mount_path,
            )
            .await
            .map_err(|e| anyhow!("authenticate graft boundary for `{name}`: {e}"))?;
        }
        require_fast_forward(
            &graph,
            name,
            authenticated_boundary
                .as_ref()
                .map(|state| state.snapshot_tip),
        )
        .await?;
    }
    if graph.tip.is_none() {
        log::info!(
            "pull {}: remote ref `main` is empty; nothing to import",
            name
        );
        return Ok(());
    }
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
    let state = graph
        .publication_state
        .as_ref()
        .ok_or_else(|| anyhow!("imported graph from `{}` has no publication state", name))?;
    debug_assert_eq!(state.snapshot_tip, pinned_tip);
    steward::write_pull_ack(ship_ref.control_table_mut(), &attachment.url, state)
        .await
        .map_err(|e| anyhow!("record consumer acknowledgement for `{}`: {}", name, e))?;

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
    let position = pull_position(ship_ref, remote, &attachment.url, name, None).await?;
    if position.already_at_tip {
        return authenticate_exact_identity_noop(
            ship_ref,
            remote,
            &attachment.url,
            name,
            None,
            &position,
        )
        .await;
    }

    let mut graph = match position.remote_state.clone() {
        None => steward::FetchedGraph::default(),
        Some(state) => {
            if let Some(acknowledged) = position.acknowledged.as_ref() {
                let graph =
                    steward::fetch_object_graph_from_acknowledgement(remote, state, acknowledged)
                        .await
                        .map_err(|e| anyhow!("fetch from `{}`: {}", attachment.url, e))?;
                steward::authenticate_publication_window(&graph, acknowledged).map_err(|e| {
                    anyhow!(
                        "authenticate publication lineage from consumer acknowledgement for \
                         `{name}`: {e}"
                    )
                })?;
                graph
            } else {
                steward::fetch_object_graph_at_publication(remote, state, None)
                    .await
                    .map_err(|e| anyhow!("fetch from `{}`: {}", attachment.url, e))?
            }
        }
    };
    require_fast_forward(&graph, name, position.previous).await?;
    if graph.tip.is_none() {
        log::info!(
            "pull {}: remote ref `main` is empty; nothing to rebuild",
            name
        );
        return Ok(());
    }
    if steward::destination_matches_publication(ship_ref, &graph)
        .await
        .map_err(|e| anyhow!("authenticate existing mirror state for `{}`: {}", name, e))?
    {
        let state = graph
            .publication_state
            .as_ref()
            .ok_or_else(|| anyhow!("fetched graph from `{}` has no publication state", name))?;
        steward::write_pull_ack(ship_ref.control_table_mut(), &attachment.url, state)
            .await
            .map_err(|e| anyhow!("repair consumer acknowledgement for `{}`: {}", name, e))?;
        log::info!(
            "[OK] pull {} recovered already-applied publication (tip={})",
            name,
            state.snapshot_tip
        );
        return Ok(());
    }
    if let Some(acknowledged) = position.acknowledged.as_ref()
        && let Some((recovered, remaining)) =
            steward::recover_applied_publication(ship_ref, &graph, acknowledged)
                .await
                .map_err(|e| anyhow!("recover applied mirror frontier for `{name}`: {e}"))?
    {
        steward::write_pull_ack(ship_ref.control_table_mut(), &attachment.url, &recovered)
            .await
            .map_err(|e| {
                anyhow!("repair intermediate consumer acknowledgement for `{name}`: {e}")
            })?;
        log::info!(
            "[OK] pull {} recovered intermediate applied publication (tip={})",
            name,
            recovered.snapshot_tip
        );
        graph = remaining;
    }
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
    let state = graph
        .publication_state
        .as_ref()
        .ok_or_else(|| anyhow!("rebuilt graph from `{}` has no publication state", name))?;
    steward::write_pull_ack(ship_ref.control_table_mut(), &attachment.url, state)
        .await
        .map_err(|e| anyhow!("record consumer acknowledgement for `{}`: {}", name, e))?;
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
