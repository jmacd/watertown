// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::print_stdout)]

//! `pond remote` subcommands: `add`, `remove`, `list`.
//!
//! Remote attachments live as small YAML files under `/sys/remotes/<name>`.
//! Per-remote runtime state (`last_pushed_tip:<url>`, `last_pulled_tip:<url>`,
//! `remote_mode:<name>`) lives in the control table's raw_config map; the
//! YAML on disk is intentionally portable (no per-pond frontier state).  The
//! tips are the CA3 single-commit-hash frontier that replaced the retired
//! per-pond seq watermarks.
//!
//! The data types ([`RemoteAttachment`] / [`RemoteMode`]) live in the
//! [`steward`] crate so the post-commit auto-push dispatcher can use them
//! too.  See [`steward::remote_config`] for the YAML schema.

use crate::common::ShipContext;
use anyhow::{Result, anyhow};
use std::collections::BTreeMap;
use steward::{
    ContentSource, REMOTE_MODE_PREFIX, REMOTE_MOUNT_PATH_PREFIX, SYS_DIR, SYS_REMOTES_DIR,
};
use tinyfs::EntryType;
use tokio::io::AsyncWriteExt;

// Re-export the types that moved to steward so existing callers using
// `commands::remote::{RemoteAttachment, RemoteMode}` keep working.
pub use steward::{RemoteAttachment, RemoteMode};

/// Filter passed to [`list_remotes_command`] for displaying a subset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteListFilter {
    /// Show only push or both-mode attachments (the backup side).
    BackupsOnly,
}

/// Validate a remote name: single path segment, non-empty, no slashes,
/// no leading dot, ASCII printable.
fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(anyhow!("remote name must not be empty"));
    }
    if name.contains('/') {
        return Err(anyhow!("remote name `{}` must not contain `/`", name));
    }
    if name.starts_with('.') {
        return Err(anyhow!("remote name `{}` must not start with `.`", name));
    }
    if !name.chars().all(|c| c.is_ascii_graphic()) {
        return Err(anyhow!(
            "remote name `{}` must be ASCII printable (no whitespace or control chars)",
            name
        ));
    }
    Ok(())
}

/// Build the in-pond path for a remote attachment config.
#[must_use]
pub fn remote_config_path(name: &str) -> String {
    format!("{SYS_REMOTES_DIR}/{name}")
}

/// `pond remote add <name> <url> <path> [--region=...] ...`
///
/// Always attaches a **pull-mode** remote and mounts it at `path`.
/// `path = "/"` is a mirror restart (foreign store_id must equal this
/// pond's pond_id); non-root `path` is a cross-pond import (foreign
/// store_id must differ).  Both validations are checked at first pull
/// (D5.7b.2); this verb only records the configuration.
#[allow(clippy::too_many_arguments)]
pub async fn add_remote_command(
    ship_context: &ShipContext,
    name: &str,
    url: &str,
    path: &str,
    region: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    endpoint: Option<String>,
    allow_http: bool,
    overwrite: bool,
) -> Result<()> {
    if !path.starts_with('/') {
        return Err(anyhow!(
            "remote mount path `{}` must be absolute (start with `/`); use `/` for a \
             mirror restart or `/imports/<name>` for a cross-pond import",
            path
        ));
    }
    attach_remote(
        ship_context,
        name,
        RemoteAttachment {
            url: url.to_string(),
            region: region.unwrap_or_default(),
            access_key_id: access_key_id.unwrap_or_default(),
            secret_access_key: secret_access_key.unwrap_or_default(),
            endpoint: endpoint.unwrap_or_default(),
            allow_http,
            limits: BTreeMap::new(),
            // The CLI verbs author inline attachments only; storage profiles
            // are a `pond apply` surface (Decision A2/L11).
            storage: None,
        },
        RemoteMode::Pull,
        Some(path),
        overwrite,
        false,
    )
    .await
}

/// `pond backup add <name> <url> [--bidirectional] [--region=...] ...`
///
/// Attaches a backup remote.  Push-only by default; `bidirectional=true`
/// also enables pulls (mode=both).  Backups always mirror the entire
/// pond -- there is no PATH because the local pond IS the source.
#[allow(clippy::too_many_arguments)]
pub async fn add_backup_command(
    ship_context: &ShipContext,
    name: &str,
    url: &str,
    bidirectional: bool,
    region: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    endpoint: Option<String>,
    allow_http: bool,
    overwrite: bool,
) -> Result<()> {
    let mode = if bidirectional {
        RemoteMode::Both
    } else {
        RemoteMode::Push
    };
    attach_remote(
        ship_context,
        name,
        RemoteAttachment {
            url: url.to_string(),
            region: region.unwrap_or_default(),
            access_key_id: access_key_id.unwrap_or_default(),
            secret_access_key: secret_access_key.unwrap_or_default(),
            endpoint: endpoint.unwrap_or_default(),
            allow_http,
            limits: BTreeMap::new(),
            // The CLI verbs author inline attachments only; storage profiles
            // are a `pond apply` surface (Decision A2/L11).
            storage: None,
        },
        mode,
        None,
        overwrite,
        false,
    )
    .await
}

/// Attach `attachment` as the remote named `name`.
///
/// The single writer of `/sys/remotes/<name>`.  Every route in -- `pond remote
/// add` (pull), `pond backup add` (push/both), and `pond apply` with
/// `kind: remote`/`kind: backup` -- funnels through here, so all three get the
/// same secret discipline, the same mount-conflict and store_id checks, and
/// the same live remote initialization.  That was the point of D6, and it is
/// preserved by widening this function rather than adding a second writer
/// (Decision L11).
///
/// Persists the attachment YAML, records mode and -- for pull -- the mount
/// path, and auto-initializes the remote Delta table.
#[allow(clippy::too_many_arguments)]
pub async fn attach_remote(
    ship_context: &ShipContext,
    name: &str,
    attachment: RemoteAttachment,
    mode: RemoteMode,
    mount_path: Option<&str>,
    overwrite: bool,
    migrate_watermark: bool,
) -> Result<()> {
    validate_name(name)?;
    let url = attachment.url.as_str();
    let _parsed =
        url::Url::parse(url).map_err(|e| anyhow!("invalid remote URL `{}`: {}", url, e))?;

    // Reject an unknown dimension key or a relative limiter path here, at
    // attach time, rather than at the first push.  A limiter that silently
    // fails to bind is worse than no limiter at all.
    // Limits bind in both directions: a push charges its budget inline, and a
    // pull charges the same budget through the governed content source.  So a
    // pull-only remote may carry `limits` and they will be enforced -- egress
    // was the first bill to arrive, but ingress is billed too.
    let _ = attachment
        .resolved_limits()
        .map_err(|e| anyhow!("remote `{}`: {}", name, e))?;

    attachment
        .check_storage_profile()
        .map_err(|e| anyhow!("remote `{}`: {}", name, e))?;

    // The attachment YAML at /sys/remotes/<name> is an oplog row that
    // `pond push` replicates to every backup.  A literal secret would
    // therefore be exposed on all replicas, so require the secret to be a
    // `${env:VAR}` reference: the reference text replicates harmlessly and
    // each replica resolves the value from its own environment at use time.
    if !attachment.secret_access_key.is_empty()
        && !utilities::env_substitution::has_env_refs(&attachment.secret_access_key)
    {
        return Err(anyhow!(
            "secret_access_key must be an environment reference such as \
             ${{env:AWS_SECRET_ACCESS_KEY}}, not a literal secret: the remote config \
             at {}/{} is replicated to every backup, so a literal secret would be \
             exposed on all replicas. Set the secret in the environment and pass \
             `--secret-access-key '${{env:AWS_SECRET_ACCESS_KEY}}'`.",
            SYS_REMOTES_DIR,
            name
        ));
    }

    let yaml = serde_yaml::to_string(&attachment)
        .map_err(|e| anyhow!("failed to serialize remote attachment YAML: {}", e))?;

    let config_path = remote_config_path(name);
    let name_owned = name.to_string();
    let mode_str = mode.as_str();

    let mut ship = ship_context.open_pond().await?;
    let existing_attachment_yaml = match load_remote_attachment(&mut ship, name).await {
        Ok(existing) => Some(
            serde_yaml::to_string(&existing)
                .map_err(|e| anyhow!("failed to serialize existing remote `{name}`: {e}"))?,
        ),
        Err(_) => None,
    };
    let existing_mode = ship
        .control_table()
        .raw_config_get(&format!("{REMOTE_MODE_PREFIX}{name}"))
        .await
        .map_err(|e| anyhow!("failed to read mode for remote `{name}`: {e}"))?
        .unwrap_or_else(|| RemoteMode::Push.as_str().to_string());
    let existing_mount = ship
        .control_table()
        .raw_config_get(&format!("{REMOTE_MOUNT_PATH_PREFIX}{name}"))
        .await
        .map_err(|e| anyhow!("failed to read mount for remote `{name}`: {e}"))?
        .unwrap_or_default();

    let watermark_migration = if migrate_watermark {
        Some(prepare_watermark_migration(&mut ship, name, url, mode, mount_path, overwrite).await?)
    } else {
        None
    };

    // D5.7b.5: mount-conflict pre-checks (local only) for pull-mode
    // remotes.  Refuse if another pull-mode attachment already mounts
    // the same `mount_path` (would create overlapping mount entries).
    // The store_id-duplication check happens below, after we open the
    // remote URL to learn its store_id.
    if mode == RemoteMode::Pull
        && let Some(mp) = mount_path
    {
        validate_no_mount_path_conflict(&mut ship, name, mp, overwrite).await?;
    }

    // Initialize (or verify) the remote Delta table at `url` BEFORE any
    // pond-side state changes.  This closes a gap that would otherwise
    // surface as `delta error: Not a Delta table` on the first `pond
    // push`: nothing else in the CLI ever calls `Remote::create_at_url`.
    //
    // Three cases:
    //
    // - URL already holds a Delta table:
    //     - push/both: store_id MUST match our pond_id (refuse otherwise,
    //       to avoid silently writing into a foreign pond's remote).
    //     - pull with mount_path == "/": store_id MUST match our pond_id
    //       (mirror restart).
    //     - pull with mount_path != "/": store_id MUST differ from our
    //       pond_id (cross-pond import requires a foreign pond).
    //
    // - URL has no Delta table:
    //     - push/both: create one with our pond_id as the store_id.
    //     - pull:      refuse (consumer cannot bootstrap an empty
    //       upstream; operator should set up the upstream pond first).
    let local_pond_id = ship.control_table().pond_id_uuid();
    // One dispatch (Decision A8).  This also validates the profile reference
    // -- it must exist, parse, and serve this URL's scheme -- before any
    // pond-side state changes, on the same principle as the limiter check
    // above: a binding that silently fails to resolve is worse than none.
    let storage_options = {
        let pond = ship
            .as_pond_mut()
            .ok_or_else(|| anyhow!("attach requires a pond steward (not a host steward)"))?;
        steward::storage_profile::prepare_storage(pond, &attachment).await?
    };
    let limit_spec = attachment.resolved_limits()?;
    {
        let pond = ship
            .as_pond_mut()
            .ok_or_else(|| anyhow!("attach requires a pond steward (not a host steward)"))?;
        let _limits = steward::LimiterSet::open(pond, &limit_spec)
            .await
            .map_err(|e| anyhow!("bind limiters for remote `{name}`: {e}"))?;
    }
    let is_import = mode == RemoteMode::Pull && matches!(mount_path, Some(p) if p != "/");
    if is_import {
        // Cross-pond import: content-addressed pull (graph fetch + foreign-pond
        // rebuild + mount).  The source must expose a foreign pond store_id --
        // either a content remote (`s3://`, `file://`) or a producer pond clone
        // on local disk (`pond://<path>`, the develop-and-preview workflow).
        let remote_store_id = if let Some(path) = attachment.url.strip_prefix("pond://") {
            match steward::LocalPondSource::open(path).await {
                Ok(source) => {
                    verify_watermark_destination(
                        &source,
                        watermark_migration.as_ref(),
                        &attachment.url,
                    )
                    .await?;
                    source.pond_id()
                }
                Err(e) => {
                    return Err(anyhow!(
                        "remote `{}` at {} is not a readable local pond: {}; pull-mode \
                         `pond://` remotes must point at an existing pond clone on disk.",
                        name,
                        attachment.url,
                        e
                    ));
                }
            }
        } else {
            match sync_store::ContentRemote::open_at_url(&attachment.url, storage_options.clone())
                .await
            {
                Ok(remote) => {
                    verify_watermark_destination(
                        &remote,
                        watermark_migration.as_ref(),
                        &attachment.url,
                    )
                    .await?;
                    remote.pond_id()
                }
                Err(_) => {
                    return Err(anyhow!(
                        "remote `{}` at {} is not a content remote; pull-mode remotes must \
                         point at an existing pond. The consumer cannot initialize an empty \
                         upstream remote.",
                        name,
                        attachment.url
                    ));
                }
            }
        };
        if remote_store_id == local_pond_id {
            return Err(anyhow!(
                "remote `{}` at {} has store_id {} which matches this pond's pond_id; \
                 mount path is reserved for cross-pond imports (foreign store_id must \
                 differ). Use `/` to attach this remote as a mirror restart.",
                name,
                attachment.url,
                remote_store_id
            ));
        }
        validate_no_foreign_store_id_collision(&mut ship, name, remote_store_id, overwrite).await?;
        log::info!(
            "remote {} ({}) ready for import (store_id={})",
            name,
            attachment.url,
            remote_store_id
        );
    } else {
        // Mirror restart, backup, or push/both: content-addressed remote.
        match sync_store::ContentRemote::open_at_url(&attachment.url, storage_options.clone()).await
        {
            Ok(remote) => {
                if remote.pond_id() != local_pond_id {
                    return Err(anyhow!(
                        "remote `{}` at {} has pond_id {} which does not match this pond's \
                         pond_id {}; mirror/backup requires the same pond. Use a non-root mount \
                         path like `/imports/{}` for a cross-pond import.",
                        name,
                        attachment.url,
                        remote.pond_id(),
                        local_pond_id,
                        name
                    ));
                }
                log::info!(
                    "remote {} ({}) already initialized (pond_id={})",
                    name,
                    attachment.url,
                    local_pond_id
                );
            }
            Err(_) if mode == RemoteMode::Pull => {
                return Err(anyhow!(
                    "remote `{}` at {} is not a content remote; pull-mode mirrors must point at \
                     an existing pond. The consumer cannot initialize an empty upstream remote.",
                    name,
                    attachment.url
                ));
            }
            Err(_) => {
                let _ = sync_store::ContentRemote::create_at_url(
                    &attachment.url,
                    local_pond_id,
                    storage_options,
                )
                .await
                .map_err(|e| {
                    anyhow!(
                        "failed to initialize remote `{}` at {}: {}",
                        name,
                        attachment.url,
                        e
                    )
                })?;
                log::info!(
                    "[OK] initialized remote {} at {} (pond_id={})",
                    name,
                    attachment.url,
                    local_pond_id
                );
            }
        }
    }

    let mount_value = mount_path.unwrap_or("");
    if overwrite
        && !migrate_watermark
        && existing_attachment_yaml.as_deref() == Some(yaml.as_str())
        && existing_mode == mode_str
        && existing_mount == mount_value
    {
        log::info!("remote {} already has the requested configuration", name);
        return Ok(());
    }

    // Record the operating mode BEFORE the data write commits.  The
    // write transaction's post-commit auto-push dispatcher reads
    // `remote_mode:<name>` from the control table to decide whether to
    // push to a freshly-added remote.  If we wrote the mode after the
    // data commit, a `mode=pull` remote would be auto-pushed to once
    // (because the dispatcher would see the default `push`).
    let mode_key = format!("{REMOTE_MODE_PREFIX}{name}");
    ship.control_table_mut()
        .raw_config_set(&mode_key, mode_str)
        .await
        .map_err(|e| anyhow!("Failed to set remote mode `{}`: {}", mode_str, e))?;

    // Record (or clear) the mount path.  Pull-mode remotes always set
    // this; push/both leave it empty so a future `pond remote add`
    // -overwriting- a backup with a pull entry starts from a clean slate.
    let mount_key = format!("{REMOTE_MOUNT_PATH_PREFIX}{name}");
    ship.control_table_mut()
        .raw_config_set(&mount_key, mount_value)
        .await
        .map_err(|e| anyhow!("Failed to set remote mount path: {}", e))?;

    ship.write_transaction(
        &steward::PondUserMetadata::new(vec![
            "remote".to_string(),
            "add".to_string(),
            name_owned.clone(),
            url.to_string(),
        ]),
        async |fs| {
            let root = fs.root().await?;

            // Idempotent parent setup: ensure /sys and /sys/remotes exist.
            let _ = root.create_dir_all(SYS_DIR).await?;
            let _ = root.create_dir_all(SYS_REMOTES_DIR).await?;

            if root.exists(&config_path).await {
                if !overwrite {
                    return Err(steward::StewardError::Aborted(format!(
                        "remote `{}` already exists at {} (use --overwrite to replace)",
                        name_owned, config_path
                    )));
                }
                let remotes_dir = root.open_dir_path(SYS_REMOTES_DIR).await?;
                remotes_dir.remove_entry(&name_owned).await.map_err(|e| {
                    steward::StewardError::Aborted(format!(
                        "failed to remove existing remote config {}: {}",
                        config_path, e
                    ))
                })?;
            }

            let mut writer = root
                .async_writer_path_with_type(&config_path, EntryType::FilePhysicalVersion)
                .await?;
            writer
                .write_all(yaml.as_bytes())
                .await
                .map_err(|e| steward::StewardError::Aborted(format!("write yaml: {}", e)))?;
            writer
                .shutdown()
                .await
                .map_err(|e| steward::StewardError::Aborted(format!("close yaml: {}", e)))?;

            Ok(())
        },
    )
    .await
    .map_err(|e| anyhow!("Failed to add remote: {}", e))?;

    if let Some(migration) = watermark_migration {
        ship.control_table_mut()
            .raw_config_set(&format!("last_pulled_tip:{url}"), &migration.tip)
            .await
            .map_err(|e| anyhow!("record migrated pull watermark for `{name}`: {e}"))?;
        if migration.old_url == url {
            log::info!(
                "[OK] verified existing pull watermark for {} at {} ({})",
                name,
                url,
                migration.tip
            );
        } else {
            log::info!(
                "[OK] migrated pull watermark for {}: {} -> {} at {}",
                name,
                migration.old_url,
                url,
                migration.tip
            );
        }
    }

    match mount_path {
        Some(p) => log::info!(
            "[OK] added remote {} -> {} (mode={}, mount={})",
            name,
            url,
            mode.as_str(),
            p
        ),
        None => log::info!(
            "[OK] added remote {} -> {} (mode={})",
            name,
            url,
            mode.as_str()
        ),
    }
    Ok(())
}

#[derive(Debug)]
struct PullWatermarkMigration {
    old_url: String,
    tip: String,
    foreign_pond_id: String,
}

impl PullWatermarkMigration {
    fn verify(&self, remote_pond_id: uuid::Uuid, remote_tip: Option<&str>) -> Result<()> {
        if remote_pond_id.to_string() != self.foreign_pond_id {
            return Err(anyhow!(
                "cannot migrate pull watermark: replacement remote has pond_id {}, \
                 but graft pin records {}",
                remote_pond_id,
                self.foreign_pond_id
            ));
        }
        match remote_tip {
            Some(tip) if tip == self.tip => Ok(()),
            Some(tip) => Err(anyhow!(
                "cannot migrate pull watermark: replacement remote tip {} does not \
                 exactly match pinned tip {}; refresh the source pond while both remotes \
                 are synchronized before retrying",
                tip,
                self.tip
            )),
            None => Err(anyhow!(
                "cannot migrate pull watermark: replacement remote has no `main` tip"
            )),
        }
    }
}

async fn prepare_watermark_migration(
    ship: &mut steward::Steward,
    name: &str,
    new_url: &str,
    mode: RemoteMode,
    mount_path: Option<&str>,
    overwrite: bool,
) -> Result<PullWatermarkMigration> {
    if !overwrite || mode != RemoteMode::Pull {
        return Err(anyhow!(
            "pull watermark migration requires overwrite of a pull remote"
        ));
    }
    let mount_path = mount_path
        .filter(|path| *path != "/")
        .ok_or_else(|| anyhow!("pull watermark migration requires a cross-pond mount path"))?;
    let old_attachment = load_remote_attachment(ship, name).await.map_err(|e| {
        anyhow!(
            "cannot migrate pull watermark: existing remote `{}` is unavailable: {}",
            name,
            e
        )
    })?;
    if remote_mode_for(ship, name).await? != RemoteMode::Pull {
        return Err(anyhow!(
            "cannot migrate pull watermark: existing remote `{}` is not pull-mode",
            name
        ));
    }
    let old_mount = ship
        .control_table()
        .raw_config_get(&format!("{REMOTE_MOUNT_PATH_PREFIX}{name}"))
        .await
        .map_err(|e| anyhow!("read existing mount path for `{name}`: {e}"))?
        .ok_or_else(|| {
            anyhow!("cannot migrate pull watermark: existing remote has no mount path")
        })?;
    if !paths_equivalent(&old_mount, mount_path) {
        return Err(anyhow!(
            "cannot migrate pull watermark: existing mount `{}` differs from replacement mount `{}`",
            old_mount,
            mount_path
        ));
    }

    let tip = ship
        .control_table()
        .raw_config_get(&format!("last_pulled_tip:{}", old_attachment.url))
        .await
        .map_err(|e| anyhow!("read existing pull watermark for `{name}`: {e}"))?
        .filter(|tip| !tip.is_empty())
        .ok_or_else(|| {
            if old_attachment.url == new_url {
                anyhow!(
                    "cannot verify completed pull watermark migration: replacement URL `{}` \
                     is already attached but has no pull watermark; restore the original \
                     attachment and retry the migration",
                    old_attachment.url
                )
            } else {
                anyhow!(
                    "cannot migrate pull watermark: existing URL `{}` has no pull watermark",
                    old_attachment.url
                )
            }
        })?;

    let pin_path = steward::GraftPin::pin_path(name);
    let tx = ship
        .begin_read(&steward::PondUserMetadata::new(vec![
            "remote".to_string(),
            "migrate-watermark".to_string(),
            name.to_string(),
        ]))
        .await
        .map_err(|e| anyhow!("begin graft pin read for `{name}`: {e}"))?;
    let pin_bytes = {
        let root = tx.root().await?;
        if !root.exists(&pin_path).await {
            return Err(anyhow!(
                "cannot migrate pull watermark: graft pin `{}` does not exist",
                pin_path
            ));
        }
        root.read_file_path_to_vec(&pin_path)
            .await
            .map_err(|e| anyhow!("read graft pin `{pin_path}`: {e}"))?
    };
    let _ = tx
        .commit()
        .await
        .map_err(|e| anyhow!("commit graft pin read for `{name}`: {e}"))?;
    let pin = steward::GraftPin::from_yaml_bytes(&pin_bytes)
        .map_err(|e| anyhow!("parse graft pin `{pin_path}`: {e}"))?;
    if !paths_equivalent(&pin.mount_path, mount_path) {
        return Err(anyhow!(
            "cannot migrate pull watermark: graft pin mount `{}` differs from replacement mount `{}`",
            pin.mount_path,
            mount_path
        ));
    }
    if pin.pinned_tip != tip {
        return Err(anyhow!(
            "cannot migrate pull watermark: graft pin tip {} differs from existing URL watermark {}",
            pin.pinned_tip,
            tip
        ));
    }

    Ok(PullWatermarkMigration {
        old_url: old_attachment.url,
        tip,
        foreign_pond_id: pin.foreign_pond_id,
    })
}

async fn verify_watermark_destination(
    source: &impl ContentSource,
    migration: Option<&PullWatermarkMigration>,
    url: &str,
) -> Result<()> {
    let Some(migration) = migration else {
        return Ok(());
    };
    let tip = source
        .get_tip("main")
        .await
        .map_err(|e| anyhow!("read tip from `{url}`: {e}"))?;
    let tip_text = tip.as_ref().map(ToString::to_string);
    migration.verify(source.pond_id(), tip_text.as_deref())?;
    let tip = tip.expect("verified destination tip is present");
    if source
        .get_object(tip)
        .await
        .map_err(|e| anyhow!("read pinned commit {} from `{url}`: {e}", migration.tip))?
        .is_none()
    {
        return Err(anyhow!(
            "cannot migrate pull watermark: replacement remote does not contain pinned commit {}",
            migration.tip
        ));
    }
    Ok(())
}

/// D5.7b.5: refuse if another pull-mode attachment already mounts
/// the same `mount_path`.  Re-attaching the same NAME with --overwrite
/// is permitted (the existing config will be replaced).
async fn validate_no_mount_path_conflict(
    ship: &mut steward::Steward,
    new_name: &str,
    new_mount_path: &str,
    overwrite: bool,
) -> Result<()> {
    let existing_names = list_remote_names(ship).await?;
    for existing_name in existing_names {
        if existing_name == new_name {
            // Same-name re-attach is allowed only with --overwrite;
            // the existing-name check happens in the write
            // transaction below (with a clear error message).  Skip
            // here so the diagnostic for `--overwrite` collisions
            // remains the one inside the transaction.
            continue;
        }
        let mount_key = format!("{REMOTE_MOUNT_PATH_PREFIX}{existing_name}");
        let existing_mount = ship
            .control_table()
            .raw_config_get(&mount_key)
            .await
            .map_err(|e| anyhow!("read mount key for `{}`: {}", existing_name, e))?;
        match existing_mount {
            Some(p) if !p.is_empty() && paths_equivalent(&p, new_mount_path) => {
                if overwrite {
                    return Err(anyhow!(
                        "mount path `{}` is already used by remote `{}`; --overwrite \
                         applies only to the same-named attachment (`{}`), not to a \
                         different remote. Remove `{}` first or pick a different mount \
                         path.",
                        new_mount_path,
                        existing_name,
                        new_name,
                        existing_name
                    ));
                }
                return Err(anyhow!(
                    "mount path `{}` is already used by remote `{}`; pick a different \
                     mount path or remove `{}` first.",
                    new_mount_path,
                    existing_name,
                    existing_name
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

/// D5.7b.5: refuse if another pull-mode attachment is already mounting
/// the same foreign store_id at a different name.  This would create
/// two mount entries for the same foreign pond with ambiguous resolution.
async fn validate_no_foreign_store_id_collision(
    ship: &mut steward::Steward,
    new_name: &str,
    new_store_id: uuid::Uuid,
    overwrite: bool,
) -> Result<()> {
    let existing_names = list_remote_names(ship).await?;
    for existing_name in existing_names {
        if existing_name == new_name {
            continue;
        }
        let attachment = match load_remote_attachment(ship, &existing_name).await {
            Ok(a) => a,
            Err(_) => continue,
        };
        let mode_str = ship
            .control_table()
            .raw_config_get(&format!("{REMOTE_MODE_PREFIX}{existing_name}"))
            .await
            .map_err(|e| anyhow!("read mode for `{}`: {}", existing_name, e))?
            .unwrap_or_else(|| "push".to_string());
        let parsed_mode = match RemoteMode::parse(&mode_str) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if parsed_mode != RemoteMode::Pull {
            continue;
        }
        // Probe the existing remote to learn its store_id.
        let pond = match ship.as_pond_mut() {
            Some(p) => p,
            None => continue,
        };
        let storage_options =
            match steward::storage_profile::prepare_storage(pond, &attachment).await {
                Ok(o) => o,
                // A probe that cannot be configured tells us nothing about a
                // mount conflict; the attachment's own validation reports it.
                Err(_) => continue,
            };
        let existing_store_id =
            match sync_store::ContentRemote::open_at_url(&attachment.url, storage_options).await {
                Ok(r) => r.pond_id(),
                Err(_) => continue,
            };
        if existing_store_id == new_store_id {
            if overwrite {
                return Err(anyhow!(
                    "foreign store_id `{}` is already mounted by remote `{}` (mode=pull); \
                     --overwrite cannot resolve a cross-remote collision. Remove `{}` first \
                     or use a different upstream pond.",
                    new_store_id,
                    existing_name,
                    existing_name
                ));
            }
            return Err(anyhow!(
                "foreign store_id `{}` is already mounted by remote `{}` (mode=pull); \
                 attaching the same upstream twice creates ambiguous mount paths. Remove \
                 `{}` first or use a different upstream pond.",
                new_store_id,
                existing_name,
                existing_name
            ));
        }
    }
    Ok(())
}

/// True if two pond paths are equivalent after normalizing trailing
/// slashes (but treating `/foo` and `/foo/` as the same).
fn paths_equivalent(a: &str, b: &str) -> bool {
    let an = a.trim_end_matches('/');
    let bn = b.trim_end_matches('/');
    // Special-case root: "/" trims to "" -- treat both as "/".
    let an = if an.is_empty() { "/" } else { an };
    let bn = if bn.is_empty() { "/" } else { bn };
    an == bn
}

/// `pond remote remove <name>` -- delete the attachment config and clear
/// associated raw_config keys.  Does NOT yet refuse on pending PostPush*
/// records (a future enhancement once D4.4 lands).
/// `pond remote remove <name> [--purge]` -- delete the attachment
/// config and clear associated raw_config keys.  By default this is
/// a **detach**: any cross-pond mount entry materialized at the
/// remote's `mount_path` is preserved (so the imported data stays
/// visible).  With `--purge`, the mount entry is also removed.
///
/// Note: `--purge` does NOT physically delete the foreign pond_id's
/// rows from the underlying Delta log (that would be a partitioned
/// Delta `delete` predicate, deferred to a follow-up).  Once the
/// mount entry is gone, the foreign rows are unreachable by path
/// and will be eligible for vacuum at the next checkpoint cycle.
pub async fn remove_remote_command(
    ship_context: &ShipContext,
    name: &str,
    purge: bool,
) -> Result<()> {
    validate_name(name)?;
    let config_path = remote_config_path(name);
    let name_owned = name.to_string();

    let mut ship = ship_context.open_pond().await?;

    // Look up the URL before deleting so we can clear its watermark keys.
    let url_to_clear: Option<String> = {
        match load_remote_attachment(&mut ship, &name_owned).await {
            Ok(attachment) => Some(attachment.url),
            Err(_) => None,
        }
    };

    // Look up the mount path before deleting; if `purge` is set and
    // the mount path is non-root, we must remove the mount entry in
    // the same write transaction that removes the config.
    let mount_path_to_purge: Option<String> = if purge {
        let key = format!("{REMOTE_MOUNT_PATH_PREFIX}{name}");
        match ship.control_table().raw_config_get(&key).await {
            Ok(Some(v)) if !v.is_empty() && v != "/" => Some(v),
            _ => None,
        }
    } else {
        None
    };

    ship.write_transaction(
        &steward::PondUserMetadata::new(if purge {
            vec![
                "remote".to_string(),
                "remove".to_string(),
                "--purge".to_string(),
                name_owned.clone(),
            ]
        } else {
            vec![
                "remote".to_string(),
                "remove".to_string(),
                name_owned.clone(),
            ]
        }),
        async |fs| {
            let root = fs.root().await?;
            if !root.exists(&config_path).await {
                return Err(steward::StewardError::Aborted(format!(
                    "no remote named `{}` (looked at {})",
                    name_owned, config_path
                )));
            }
            let remotes_dir = root.open_dir_path(SYS_REMOTES_DIR).await?;
            remotes_dir.remove_entry(&name_owned).await.map_err(|e| {
                steward::StewardError::Aborted(format!(
                    "failed to delete remote config {}: {}",
                    config_path, e
                ))
            })?;

            // D5.7b.4: purge mount entry if requested and present.
            // The mount entry is a normal directory entry under its
            // parent (e.g., /imports/upstream under /imports), so we
            // resolve the parent and unlink the leaf name.
            if let Some(mount_path) = &mount_path_to_purge {
                let (parent, leaf) = super::pull::split_mount_path(mount_path).map_err(|e| {
                    steward::StewardError::Aborted(format!(
                        "invalid mount_path `{}`: {}",
                        mount_path, e
                    ))
                })?;
                if root.exists(mount_path).await {
                    let parent_dir = root.open_dir_path(parent).await?;
                    parent_dir.remove_entry(leaf).await.map_err(|e| {
                        steward::StewardError::Aborted(format!(
                            "failed to purge mount entry {}: {}",
                            mount_path, e
                        ))
                    })?;
                    log::info!("[OK] purged mount entry {}", mount_path);
                } else {
                    log::warn!(
                        "[WARN] mount_path {} not found in pond; nothing to purge",
                        mount_path
                    );
                }
            }
            Ok(())
        },
    )
    .await
    .map_err(|e| anyhow!("Failed to remove remote: {}", e))?;

    // Clear runtime state.  All failures here are best-effort warnings;
    // the YAML on disk is already gone.
    let mode_key = format!("{REMOTE_MODE_PREFIX}{name}");
    if let Err(e) = ship.control_table_mut().raw_config_set(&mode_key, "").await {
        log::warn!("[WARN] failed to clear {}: {}", mode_key, e);
    }
    let mount_key = format!("{REMOTE_MOUNT_PATH_PREFIX}{name}");
    if let Err(e) = ship
        .control_table_mut()
        .raw_config_set(&mount_key, "")
        .await
    {
        log::warn!("[WARN] failed to clear {}: {}", mount_key, e);
    }
    if let Some(url) = url_to_clear {
        for key in [
            format!("last_pushed_tip:{url}"),
            format!("last_pulled_tip:{url}"),
        ] {
            if let Err(e) = ship.control_table_mut().raw_config_set(&key, "").await {
                log::warn!("[WARN] failed to clear {}: {}", key, e);
            }
        }
    }

    if purge {
        log::info!("[OK] removed remote {} (with --purge)", name);
    } else {
        log::info!("[OK] removed remote {} (detached; data preserved)", name);
    }
    Ok(())
}

/// `pond remote list` / `pond backup list` -- print each remote with
/// URL, mode, mount path, and the most recent push/pull watermarks.
///
/// `filter`:
/// - `None`: show every remote.
/// - `Some(BackupsOnly)`: show only push/both-mode attachments.
/// - `Some(PullsOnly)`: show only pull-mode attachments.
pub async fn list_remotes_command(
    ship_context: &ShipContext,
    filter: Option<RemoteListFilter>,
) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;
    let entries = list_remote_names(&mut ship).await?;

    if entries.is_empty() {
        let suggestion = match filter {
            Some(RemoteListFilter::BackupsOnly) => {
                "(no remotes; use `pond backup add <name> <url>` to attach one)"
            }
            _ => "(no remotes; use `pond remote add <name> <url> <path>` to attach one)",
        };
        println!("{}", suggestion);
        return Ok(());
    }

    println!(
        "{:<20} {:<50} {:<6} {:<20} {:<18} {:<18}",
        "NAME", "URL", "MODE", "MOUNT", "PUSHED_TIP", "PULLED_TIP"
    );
    for name in entries {
        let attachment = load_remote_attachment(&mut ship, &name)
            .await
            .map_err(|error| anyhow!("could not read /sys/remotes/{name}: {error}"))?;
        let mode = ship
            .control_table()
            .raw_config_get(&format!("{REMOTE_MODE_PREFIX}{name}"))
            .await
            .map_err(|error| anyhow!("could not read mode for remote `{name}`: {error}"))?
            .unwrap_or_else(|| "push".to_string());
        let parsed_mode = RemoteMode::parse(&mode)
            .map_err(|error| anyhow!("could not read mode for remote `{name}`: {error}"))?;
        match filter {
            Some(RemoteListFilter::BackupsOnly) if !parsed_mode.pushes() => continue,
            _ => {}
        }
        let mount = ship
            .control_table()
            .raw_config_get(&format!("{REMOTE_MOUNT_PATH_PREFIX}{name}"))
            .await
            .map_err(|error| anyhow!("could not read mount for remote `{name}`: {error}"))?
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "-".to_string());
        let pushed_tip = short_tip(
            ship.control_table()
                .raw_config_get(&format!("last_pushed_tip:{}", attachment.url))
                .await
                .map_err(|error| {
                    anyhow!("could not read pushed tip for remote `{name}`: {error}")
                })?,
        );
        let pulled_tip = short_tip(
            ship.control_table()
                .raw_config_get(&format!("last_pulled_tip:{}", attachment.url))
                .await
                .map_err(|error| {
                    anyhow!("could not read pulled tip for remote `{name}`: {error}")
                })?,
        );
        println!(
            "{:<20} {:<50} {:<6} {:<20} {:<18} {:<18}",
            name, attachment.url, mode, mount, pushed_tip, pulled_tip
        );
    }
    Ok(())
}

/// Render a stored 64-hex tip commit hash as a short, stable 16-char prefix for
/// operator display, or `-` when the ref has never been pushed/pulled.
fn short_tip(stored: Option<String>) -> String {
    match stored.filter(|s| !s.is_empty()) {
        Some(hex) => hex.chars().take(16).collect(),
        None => "-".to_string(),
    }
}

/// Read and parse the YAML for `<name>` from `/sys/remotes/<name>`.
///
/// Returns an error if the file does not exist or fails to parse.
pub async fn load_remote_attachment(
    ship: &mut steward::Steward,
    name: &str,
) -> Result<RemoteAttachment> {
    validate_name(name)?;
    let config_path = remote_config_path(name);
    let name_owned = name.to_string();

    let tx = ship
        .begin_read(&steward::PondUserMetadata::new(vec![
            "remote".to_string(),
            "load".to_string(),
            name_owned.clone(),
        ]))
        .await
        .map_err(|e| anyhow!("failed to begin read transaction: {}", e))?;

    let yaml_bytes = {
        let fs = &*tx;
        let root = fs
            .root()
            .await
            .map_err(|e| anyhow!("failed to open root: {}", e))?;
        if !root.exists(&config_path).await {
            let _ = tx
                .commit()
                .await
                .map_err(|e| anyhow!("failed to commit read tx: {}", e))?;
            return Err(anyhow!("no remote named `{}` ({})", name, config_path));
        }
        root.read_file_path_to_vec(&config_path)
            .await
            .map_err(|e| anyhow!("failed to read {}: {}", config_path, e))?
    };
    let _ = tx
        .commit()
        .await
        .map_err(|e| anyhow!("failed to commit read tx: {}", e))?;

    let attachment: RemoteAttachment = RemoteAttachment::from_yaml_bytes(&yaml_bytes)
        .map_err(|e| anyhow!("failed to parse {}: {}", config_path, e))?;
    Ok(attachment)
}

/// List all remote names under `/sys/remotes/`.  Returns an empty Vec if
/// `/sys/remotes/` does not exist.
pub async fn list_remote_names(ship: &mut steward::Steward) -> Result<Vec<String>> {
    use crate::common::FileInfoVisitor;
    let tx = ship
        .begin_read(&steward::PondUserMetadata::new(vec![
            "remote".to_string(),
            "list".to_string(),
        ]))
        .await
        .map_err(|e| anyhow!("failed to begin read transaction: {}", e))?;

    let names: Vec<String> = {
        let fs = &*tx;
        let root = fs
            .root()
            .await
            .map_err(|e| anyhow!("failed to open root: {}", e))?;
        if !root.exists(SYS_REMOTES_DIR).await {
            let _ = tx
                .commit()
                .await
                .map_err(|e| anyhow!("failed to commit read tx: {}", e))?;
            return Ok(Vec::new());
        }
        let pattern = format!("{SYS_REMOTES_DIR}/*");
        let mut visitor = FileInfoVisitor::new(true);
        let infos = root
            .visit_with_visitor(&pattern, &mut visitor)
            .await
            .map_err(|e| anyhow!("failed to list {}: {}", pattern, e))?;
        infos
            .into_iter()
            .filter_map(|info| {
                std::path::Path::new(&info.path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(str::to_string)
            })
            .collect()
    };
    let _ = tx
        .commit()
        .await
        .map_err(|e| anyhow!("failed to commit read tx: {}", e))?;
    Ok(names)
}

/// Lookup the operating mode for `<name>`.  Defaults to `Push` if unset.
pub async fn remote_mode_for(ship: &steward::Steward, name: &str) -> Result<RemoteMode> {
    let raw = ship
        .control_table()
        .raw_config_get(&format!("{REMOTE_MODE_PREFIX}{name}"))
        .await
        .map_err(|e| anyhow!("failed to read remote mode for `{}`: {}", name, e))?;
    match raw {
        Some(s) if !s.is_empty() => {
            RemoteMode::parse(&s).map_err(|e| anyhow!("invalid remote mode for `{}`: {}", name, e))
        }
        _ => Ok(RemoteMode::Push),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_name_accepts_simple() {
        validate_name("origin").unwrap();
        validate_name("backup-01").unwrap();
        validate_name("s3_minio").unwrap();
    }

    #[test]
    fn test_validate_name_rejects_bad() {
        assert!(validate_name("").is_err());
        assert!(validate_name("a/b").is_err());
        assert!(validate_name(".hidden").is_err());
        assert!(validate_name("has space").is_err());
        assert!(validate_name("tab\there").is_err());
    }

    #[test]
    fn test_remote_config_path_format() {
        assert_eq!(remote_config_path("origin"), "/sys/remotes/origin");
    }
}
