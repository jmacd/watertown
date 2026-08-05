// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `pond restore <name> <url>` -- bootstrap a whole-pond replica from a
//! backup published to a content-addressed remote.
//!
//! This is the operator entry point for disaster recovery.  `pond init` always
//! mints a FRESH pond_id, so a freshly-inited pond can never attach its own
//! backup as a mirror (`pond remote add ... /` refuses on a pond_id mismatch),
//! and the only code that stamps a replica id ([`steward::Steward::create_replica`])
//! was previously reachable only from Rust tests (BACKLOG: CA-MIRROR-RESTORE).
//!
//! Restore closes that gap in three steps:
//!   1. discover the SOURCE pond's id by opening the remote read-only;
//!   2. stamp a replica shell carrying that id via `create_replica`;
//!   3. attach the remote as a pull-mode mirror at `/` and pull the full
//!      content graph (mirror rebuild via [`steward::rebuild_pond`]).
//!
//! After a successful restore the local pond IS the source (same pond_id, same
//! content tip commit hash -- the canonical cross-replica fingerprint), and the
//! `name` attachment is left configured as a mirror so later `pond pull <name>`
//! tracks the upstream incrementally.

use crate::commands::pull::pull_command;
use crate::commands::remote::add_remote_command;
use crate::common::ShipContext;
use anyhow::{Result, anyhow};

/// Restore a whole pond from `url` into the target pond path.
///
/// Refuses to run over an existing pond.  On success the target carries the
/// source's `pond_id` and content tip, with `name` attached as a mirror-mode
/// pull remote.
#[allow(clippy::too_many_arguments)]
pub async fn restore_command(
    ship_context: &ShipContext,
    name: &str,
    url: &str,
    region: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    endpoint: Option<String>,
    allow_http: bool,
) -> Result<()> {
    let pond_path = ship_context.resolve_pond_path()?;
    let data_path = pond_path.join("data");
    let control_path = pond_path.join("control");

    // Restore bootstraps a fresh replica; it must not clobber an existing pond.
    if data_path.exists() || control_path.exists() {
        return Err(anyhow!(
            "refusing to restore over an existing pond at {}: `pond restore` bootstraps a \
             fresh replica. Remove the directory (or choose an empty target) and retry, or \
             use `pond pull {}` to update an already-restored pond.",
            pond_path.display(),
            name
        ));
    }

    // Discover the SOURCE pond_id by opening the remote read-only.  A restore
    // target adopts this id so the mirror pull rebuilds by node_id.
    let attachment = steward::RemoteAttachment {
        url: url.to_string(),
        region: region.clone().unwrap_or_default(),
        access_key_id: access_key_id.clone().unwrap_or_default(),
        secret_access_key: secret_access_key.clone().unwrap_or_default(),
        endpoint: endpoint.clone().unwrap_or_default(),
        allow_http,
        // A restore bootstraps a fresh replica by reading; it spends no
        // governed budget and outlives this function only as storage options.
        limits: std::collections::BTreeMap::new(),
        // Restore takes its connection from CLI arguments: the pond it is
        // bootstrapping does not exist yet, so there is no profile node to
        // read one from.
        storage: None,
    };
    if url.starts_with("s3://") {
        sync_store::register_s3_handlers();
    }
    let storage_options = attachment.to_storage_options()?;
    let remote = sync_store::ContentRemote::open_at_url(url, storage_options)
        .await
        .map_err(|e| {
            anyhow!(
                "open remote `{}` at {}: {}. Restore requires an existing published pond; \
                 check the URL and credentials.",
                name,
                url,
                e
            )
        })?;
    let source_pond_id = remote.pond_id();
    log::info!("[SEARCH] restore: source pond_id={source_pond_id} at {url}");

    // Stamp a replica shell carrying the source id, then attach + pull.  On any
    // later failure remove the freshly created shell so a retry starts clean
    // (we verified above that neither dir existed before this call).
    let _replica = steward::Steward::create_replica(&pond_path, source_pond_id)
        .await
        .map_err(|e| anyhow!("create replica shell (pond_id={source_pond_id}): {e}"))?;
    log::info!("[OK] restore: created replica shell (pond_id={source_pond_id})");

    let wired = wire_and_pull(
        ship_context,
        name,
        url,
        region,
        access_key_id,
        secret_access_key,
        endpoint,
        allow_http,
    )
    .await;

    match wired {
        Ok(()) => {
            log::info!("[OK] restore complete from {url} (mirror `{name}` attached at /)");
            Ok(())
        }
        Err(e) => {
            let _ = std::fs::remove_dir_all(&data_path);
            let _ = std::fs::remove_dir_all(&control_path);
            Err(anyhow!("restore from {url} failed: {e}"))
        }
    }
}

/// Attach `name` as a mirror-mode pull remote at `/` and pull the full graph.
/// Factored out so the caller can clean up the replica shell on failure.
#[allow(clippy::too_many_arguments)]
async fn wire_and_pull(
    ship_context: &ShipContext,
    name: &str,
    url: &str,
    region: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    endpoint: Option<String>,
    allow_http: bool,
) -> Result<()> {
    // Mount at `/` is the mirror-restart form: the remote's store_id must equal
    // this (now-replica) pond's pond_id, which holds by construction.  The pull
    // then rebuilds the tree by node_id and records `last_pulled_tip`.
    add_remote_command(
        ship_context,
        name,
        url,
        "/",
        region,
        access_key_id,
        secret_access_key,
        endpoint,
        allow_http,
        false,
    )
    .await?;

    pull_command(ship_context, Some(name.to_string())).await
}
