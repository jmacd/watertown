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
use steward::{REMOTE_MOUNT_PATH_PREFIX, StewardError};
use uuid::Uuid;

/// Pull from `name`, or from every remote in `pull`/`both` mode when `name`
/// is `None`.  Each remote is processed independently.
pub async fn pull_command(ship_context: &ShipContext, name: Option<String>) -> Result<()> {
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
        if let Err(e) = pull_one(&mut ship, &name).await {
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

/// Return `true` when the remote's tip commit for ref `main` already equals the
/// tip we last pulled for `url`, meaning the local mirror/mount is up to date
/// and the full graph fetch can be skipped.  Records nothing; the caller
/// refreshes `last_pulled_tip` only after a real rebuild/import lands.
async fn already_at_tip(
    ship: &steward::Ship,
    remote: &dyn steward::ContentSource,
    url: &str,
    name: &str,
) -> Result<bool> {
    let remote_tip = remote
        .get_tip("main")
        .await
        .map_err(|e| anyhow!("get tip from `{}`: {}", url, e))?;
    let last_pulled = ship
        .control_table()
        .raw_config_get(&format!("last_pulled_tip:{url}"))
        .await
        .map_err(|e| anyhow!("read last_pulled_tip for `{}`: {}", name, e))?;
    if let (Some(tip), Some(prev)) = (remote_tip, last_pulled.as_deref())
        && tip.to_hex() == prev
    {
        log::info!("[OK] pull {name} already up to date (tip={prev})");
        return Ok(true);
    }
    Ok(false)
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

async fn pull_one(ship: &mut steward::Steward, name: &str) -> Result<()> {
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
    let limits = steward::LimiterSet::open(ship_mut, &limit_spec)
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

    // Open once, here, so both modes below share one source -- and one
    // governed wrapper.  Every remote read either mode performs goes through
    // it, which is what makes ingress governed by construction rather than by
    // remembering to charge at each call site.
    let source = open_content_source(&attachment, storage_options)
        .await
        .map_err(|e| anyhow!("open remote `{}` ({}): {}", name, attachment.url, e))?;
    let governed = steward::GovernedSource::new(source.as_ref(), limits);

    // Mirror restart / backup restore (root or no mount): pull the full
    // content graph and rebuild the local pond by node_id.  Cross-pond import
    // (non-root mount): fetch the foreign content graph and rebuild it under
    // the foreign pond_id, then mount it.
    let result = match mount_path {
        None => pull_mirror(ship, name, &attachment, &governed).await,
        Some(mount_path) => pull_import(ship, name, &attachment, &governed, &mount_path).await,
    };

    // Persist the windows whether or not the pull succeeded, for the same
    // reason a push does: a pull that failed partway still transferred what it
    // transferred, and a budget that forgets the spending of failed attempts is
    // a budget a retry loop can spend without bound.
    let mut limits = governed.into_limits();
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
    if already_at_tip(ship_ref, remote, &attachment.url, name).await? {
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
    let foreign_uuid7 = uuid7::Uuid::from(*foreign_pond_id.as_bytes());
    let outcome = steward::import_pond(ship_ref, remote, &graph, foreign_uuid7)
        .await
        .map_err(|e| anyhow!("import from `{}`: {}", attachment.url, e))?;
    log::info!(
        "[OK] pull {} complete (cross-pond import: {:?})",
        name,
        outcome
    );

    // Materialize the mount and record the graft pin in ONE transaction.  The
    // mount node is omitted from the content-tree fold (keeping the graft
    // non-transitive); the pin file is ordinary pond-owned content -- covered
    // by our commit hash and replicated to consumers as a content-addressed
    // reference to the foreign tip.
    let pinned_tip = graph
        .tip
        .ok_or_else(|| anyhow!("imported graph from `{}` has no tip commit", name))?;
    materialize_mount(
        ship_ref,
        name,
        mount_path,
        foreign_pond_id,
        &pinned_tip.to_hex(),
    )
    .await?;

    // Record the per-ref frontier we last pulled: the foreign tip commit hash
    // now mounted (CA3 replacement for the retired seq watermark).
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
    if already_at_tip(ship_ref, remote, &attachment.url, name).await? {
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

/// Insert (idempotently) a directory entry at `mount_path` whose `pond_id` is
/// the foreign pond's id, and record the graft pin at `/sys/grafts/<name>` --
/// both in a SINGLE transaction so a cross-pond pull adds exactly one local
/// commit.  Reading through the mount entry yields the foreign pond's tree; the
/// pin file is ordinary pond-owned content (covered by our commit hash and
/// replicated to consumers) that pins the foreign tip commit hash without
/// re-replicating the foreign closure.
async fn materialize_mount(
    ship: &mut steward::Ship,
    name: &str,
    mount_path: &str,
    foreign_pond_id: Uuid,
    pinned_tip_hex: &str,
) -> Result<()> {
    let (parent, leaf) = split_mount_path(mount_path)?;
    let name_owned = name.to_string();
    let mount_owned = mount_path.to_string();
    let parent_owned = parent.to_string();
    let leaf_owned = leaf.to_string();

    let pin = steward::GraftPin {
        foreign_pond_id: foreign_pond_id.to_string(),
        mount_path: mount_path.to_string(),
        pinned_tip: pinned_tip_hex.to_string(),
    };
    let pin_yaml = pin
        .to_yaml()
        .map_err(|e| anyhow!("serialize graft pin for `{}`: {}", name, e))?;
    let pin_path = steward::GraftPin::pin_path(name);
    let pin_name = name_owned.clone();

    ship.write_transaction(
        &steward::PondUserMetadata::new(vec![
            "pull".to_string(),
            "mount".to_string(),
            name_owned,
            mount_owned,
        ]),
        async move |fs| {
            use tinyfs::EntryType;
            use tokio::io::AsyncWriteExt;

            let root = fs.root().await?;
            let _ = root.create_dir_all(&parent_owned).await?;
            let parent_wd = root.open_dir_path(&parent_owned).await?;

            let foreign_uuid7 = uuid7::Uuid::from(*foreign_pond_id.as_bytes());

            if let Some(existing) = parent_wd.get(&leaf_owned).await? {
                let existing_pond = existing.node.id().pond_id();
                if existing_pond != foreign_uuid7 {
                    return Err(StewardError::Aborted(format!(
                        "mount path `{}/{}` already exists with pond_id {}; cannot \
                         attach foreign pond {}",
                        parent_owned, leaf_owned, existing_pond, foreign_uuid7
                    )));
                }
                // Idempotent: mount already points at the right pond.
            } else {
                let foreign_node = fs.foreign_root_node(foreign_uuid7).await?;
                let _ = parent_wd.insert_node(&leaf_owned, foreign_node).await?;
            }

            // Record / refresh the graft pin in the same transaction.
            let _ = root.create_dir_all(steward::SYS_DIR).await?;
            let _ = root.create_dir_all(steward::SYS_GRAFTS_DIR).await?;
            if root.exists(&pin_path).await {
                let grafts_dir = root.open_dir_path(steward::SYS_GRAFTS_DIR).await?;
                grafts_dir.remove_entry(&pin_name).await.map_err(|e| {
                    StewardError::Aborted(format!("remove existing graft pin {}: {}", pin_path, e))
                })?;
            }
            let mut writer = root
                .async_writer_path_with_type(&pin_path, EntryType::FilePhysicalVersion)
                .await?;
            writer
                .write_all(pin_yaml.as_bytes())
                .await
                .map_err(|e| StewardError::Aborted(format!("write graft pin: {}", e)))?;
            writer
                .shutdown()
                .await
                .map_err(|e| StewardError::Aborted(format!("close graft pin: {}", e)))?;
            Ok(())
        },
    )
    .await
    .map_err(|e| anyhow!("materialize mount `{}`: {}", mount_path, e))?;
    Ok(())
}

/// Split an absolute mount path into (parent_dir, leaf_name).
/// Errors if the path is `/` (root mount is mirror mode, handled
/// elsewhere) or has no leaf segment.
pub(crate) fn split_mount_path(path: &str) -> Result<(&str, &str)> {
    if !path.starts_with('/') {
        return Err(anyhow!("mount path `{}` must be absolute", path));
    }
    if path == "/" {
        return Err(anyhow!(
            "internal: split_mount_path called on `/` (mirror restart should be filtered earlier)"
        ));
    }
    let trimmed = path.trim_end_matches('/');
    let last_slash = trimmed
        .rfind('/')
        .ok_or_else(|| anyhow!("mount path `{}` has no leaf", path))?;
    let parent = if last_slash == 0 {
        "/"
    } else {
        &trimmed[..last_slash]
    };
    let leaf = &trimmed[last_slash + 1..];
    if leaf.is_empty() {
        return Err(anyhow!("mount path `{}` has empty leaf", path));
    }
    Ok((parent, leaf))
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
