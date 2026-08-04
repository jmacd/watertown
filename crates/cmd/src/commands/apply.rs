// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! CLI command for idempotent create-or-update of pond resources.
//!
//! `pond apply -f file1.yaml [file2.yaml ...]`
//!
//! Each YAML file contains one or more resource documents separated by `---`.
//! Each document follows a k8s-inspired structure:
//!
//! ```yaml
//! version: v1
//! kind: mknod
//! metadata:
//!   path: /system/etc/10-water
//! spec:
//!   factory: remote
//!   region: "${env:S3_REGION}"
//!   url: "${env:WATER_S3_URL}"
//! ```
//!
//! Supported kinds: `mkdir`, `copy`, `mknod`
//!
//! Multiple resources per file:
//! ```yaml
//! version: v1
//! kind: mkdir
//! metadata:
//!   path: /system/etc
//! ---
//! version: v1
//! kind: mkdir
//! metadata:
//!   path: /sources
//! ---
//! version: v1
//! kind: mknod
//! metadata:
//!   path: /system/etc/10-water
//! spec:
//!   factory: remote
//!   region: "${env:S3_REGION}"
//! ```

use crate::commands::remote::{RemoteMode, attach_remote};
use crate::common::ShipContext;
use anyhow::{Result, anyhow};
use provider::FactoryRegistry;
use serde::Deserialize;
use std::collections::HashSet;
use std::fs;
use utilities::env_substitution;

// ---------------------------------------------------------------------------
// Resource document structure
// ---------------------------------------------------------------------------

/// A single resource document parsed from YAML.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceDoc {
    /// Format version (must be "v1")
    version: String,
    /// Resource kind: "mkdir", "copy", or "mknod"
    kind: String,
    /// Resource metadata (path in the pond)
    metadata: ResourceMetadata,
    /// Kind-specific configuration (optional for mkdir)
    #[serde(default)]
    spec: Option<serde_yaml::Value>,
}

/// Resource metadata -- identity and location in the pond.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceMetadata {
    /// Path in the pond
    path: String,
}

/// Spec for `kind: mknod` -- factory type plus factory-specific config.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MknodSpec {
    /// Factory type (e.g., "remote", "sitegen", "dynamic-dir")
    factory: String,
    /// Factory-specific configuration (what was previously the config file body)
    config: serde_yaml::Value,
}

/// Control fields for `kind: remote` / `kind: backup`, stripped from the spec
/// before the remainder is read as a [`RemoteAttachment`].
///
/// These describe *how to attach*, not *what to attach to*, which is why they
/// are not part of the replicated attachment document.
const REMOTE_CONTROL_KEYS: [&str; 3] = ["mount", "bidirectional", "overwrite"];

/// Spec for `kind: copy`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CopySpec {
    /// Host source URL (e.g., "host:///path/to/content")
    source: String,
    /// If true, overwrite existing destination. If false (default), error if exists.
    #[serde(default)]
    overwrite: bool,
}

// ---------------------------------------------------------------------------
// Internal representation after parsing + validation
// ---------------------------------------------------------------------------

/// Parsed and validated apply kind.
#[derive(Debug)]
enum ApplyKind {
    /// Create a directory (pond mkdir -p)
    Mkdir,
    /// Copy host content into pond (pond copy)
    Copy(CopySpec),
    /// Create/update a factory node (pond mknod)
    Mknod {
        factory_type: String,
        /// Raw config YAML (with ${env:...} intact, for storage)
        raw_config: String,
        /// Expanded config YAML (for validation and initialization)
        expanded_config: String,
    },
    /// Attach a remote (`kind: remote`, pull) or a backup (`kind: backup`,
    /// push/both).  Applied outside the main transaction because attaching
    /// performs live remote validation and runs its own write.
    Remote {
        name: String,
        attachment: steward::RemoteAttachment,
        mode: RemoteMode,
        /// Mount path, pull-mode only.
        mount: Option<String>,
        overwrite: bool,
    },
}

/// A parsed apply spec ready for processing.
#[derive(Debug)]
struct ApplySpec {
    /// Source file path (for error messages)
    source_file: String,
    /// Pond path from metadata
    path: String,
    /// Parsed and validated kind
    kind: ApplyKind,
}

/// Result of applying a single spec.
#[derive(Debug, PartialEq, Eq)]
enum ApplyResult {
    Created,
    Updated,
    Unchanged,
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// Split a file into YAML documents separated by `---`.
fn split_yaml_documents(content: &str) -> Vec<String> {
    let mut documents = Vec::new();
    let mut current = String::new();

    for line in content.lines() {
        if line == "---" {
            if !current.trim().is_empty() {
                documents.push(std::mem::take(&mut current));
            } else {
                current.clear();
            }
        } else {
            if !current.is_empty() {
                current.push('\n');
            }
            current.push_str(line);
        }
    }
    if !current.trim().is_empty() {
        documents.push(current);
    }
    documents
}

/// Parse a single resource document into a validated ApplySpec.
fn parse_resource(yaml: &str, source_file: &str, index: usize) -> Result<ApplySpec> {
    let doc: ResourceDoc = serde_yaml::from_str(yaml).map_err(|e| {
        anyhow!(
            "{}[{}]: failed to parse resource: {}",
            source_file,
            index,
            e
        )
    })?;

    if doc.version != "v1" {
        return Err(anyhow!(
            "{}[{}]: unsupported version '{}', expected 'v1'",
            source_file,
            index,
            doc.version
        ));
    }

    if !doc.metadata.path.starts_with('/') {
        return Err(anyhow!(
            "{}[{}]: metadata.path must be absolute, got '{}'",
            source_file,
            index,
            doc.metadata.path
        ));
    }

    let kind = parse_kind(&doc, source_file, index)?;

    Ok(ApplySpec {
        source_file: source_file.to_string(),
        path: doc.metadata.path,
        kind,
    })
}

/// Parse the kind-specific spec into an ApplyKind.
fn parse_kind(doc: &ResourceDoc, source_file: &str, index: usize) -> Result<ApplyKind> {
    match doc.kind.as_str() {
        "mkdir" => {
            if let Some(ref spec) = doc.spec
                && !spec.is_null()
            {
                return Err(anyhow!(
                    "{}[{}]: mkdir does not accept a spec",
                    source_file,
                    index
                ));
            }
            Ok(ApplyKind::Mkdir)
        }
        "copy" => {
            let spec_value = doc.spec.as_ref().ok_or_else(|| {
                anyhow!(
                    "{}[{}]: copy requires a spec with 'source'",
                    source_file,
                    index
                )
            })?;

            let spec_yaml = serde_yaml::to_string(spec_value).map_err(|e| {
                anyhow!(
                    "{}[{}]: failed to serialize copy spec: {}",
                    source_file,
                    index,
                    e
                )
            })?;
            let expanded = env_substitution::substitute_env_vars(&spec_yaml)
                .map_err(|e| anyhow!("{}[{}]: env expansion failed: {}", source_file, index, e))?;
            let config: CopySpec = serde_yaml::from_str(&expanded)
                .map_err(|e| anyhow!("{}[{}]: invalid copy spec: {}", source_file, index, e))?;

            if !config.source.starts_with("host:///") && !config.source.starts_with("host+") {
                return Err(anyhow!(
                    "{}[{}]: copy source must be a host URL (host:///...), got '{}'",
                    source_file,
                    index,
                    config.source
                ));
            }

            Ok(ApplyKind::Copy(config))
        }
        "mknod" => {
            let spec_value = doc.spec.as_ref().ok_or_else(|| {
                anyhow!(
                    "{}[{}]: mknod requires a spec with 'factory'",
                    source_file,
                    index
                )
            })?;

            let mknod: MknodSpec = serde_yaml::from_value(spec_value.clone())
                .map_err(|e| anyhow!("{}[{}]: invalid mknod spec: {}", source_file, index, e))?;

            let raw_config = serde_yaml::to_string(&mknod.config).map_err(|e| {
                anyhow!(
                    "{}[{}]: failed to serialize factory config: {}",
                    source_file,
                    index,
                    e
                )
            })?;

            let expanded = env_substitution::substitute_env_vars(&raw_config)
                .map_err(|e| anyhow!("{}[{}]: env expansion failed: {}", source_file, index, e))?;

            _ = FactoryRegistry::validate_config(&mknod.factory, expanded.as_bytes()).map_err(
                |e| {
                    anyhow!(
                        "{}[{}]: invalid config for factory '{}': {}",
                        source_file,
                        index,
                        mknod.factory,
                        e
                    )
                },
            )?;

            Ok(ApplyKind::Mknod {
                factory_type: mknod.factory,
                raw_config,
                expanded_config: expanded,
            })
        }
        "remote" | "backup" => parse_remote_kind(doc, source_file, index),
        other => Err(anyhow!(
            "{}[{}]: unknown kind '{}', expected 'mkdir', 'copy', 'mknod', \
             'remote', or 'backup'",
            source_file,
            index,
            other
        )),
    }
}

/// Parse `kind: remote` (pull) or `kind: backup` (push/both).
///
/// The spec is the attachment document itself plus a few attach-time control
/// fields.  Those control fields are removed first and the remainder is read
/// as a [`steward::RemoteAttachment`], which is `deny_unknown_fields` -- so a
/// typo in a credential field is an error here rather than a silently dropped
/// setting, and the apply schema cannot drift from the attachment schema
/// because there is only one schema.
fn parse_remote_kind(doc: &ResourceDoc, source_file: &str, index: usize) -> Result<ApplyKind> {
    let is_backup = doc.kind == "backup";

    // The resource *is* the pond path the attachment lives at, so the name is
    // read from it rather than duplicated in the spec.  This also makes the
    // duplicate-path check cover remotes for free.
    let name = doc
        .metadata
        .path
        .strip_prefix(&format!("{}/", steward::SYS_REMOTES_DIR))
        .filter(|n| !n.is_empty() && !n.contains('/'))
        .ok_or_else(|| {
            anyhow!(
                "{}[{}]: kind '{}' requires metadata.path of the form '{}/<name>', got '{}'",
                source_file,
                index,
                doc.kind,
                steward::SYS_REMOTES_DIR,
                doc.metadata.path
            )
        })?
        .to_string();

    let spec_value = doc.spec.as_ref().ok_or_else(|| {
        anyhow!(
            "{}[{}]: kind '{}' requires a spec with 'url'",
            source_file,
            index,
            doc.kind
        )
    })?;

    let serde_yaml::Value::Mapping(mut mapping) = spec_value.clone() else {
        return Err(anyhow!(
            "{}[{}]: kind '{}' spec must be a mapping",
            source_file,
            index,
            doc.kind
        ));
    };

    let mut control: std::collections::HashMap<&str, serde_yaml::Value> =
        std::collections::HashMap::new();
    for key in REMOTE_CONTROL_KEYS {
        if let Some(v) = mapping.remove(serde_yaml::Value::String(key.to_string())) {
            let _ = control.insert(key, v);
        }
    }

    let as_bool = |key: &str| -> Result<bool> {
        match control.get(key) {
            None => Ok(false),
            Some(serde_yaml::Value::Bool(b)) => Ok(*b),
            Some(other) => Err(anyhow!(
                "{}[{}]: '{}' must be true or false, got {:?}",
                source_file,
                index,
                key,
                other
            )),
        }
    };
    let bidirectional = as_bool("bidirectional")?;
    let overwrite = as_bool("overwrite")?;

    let mount = match control.get("mount") {
        None => None,
        Some(serde_yaml::Value::String(p)) => Some(p.clone()),
        Some(other) => {
            return Err(anyhow!(
                "{}[{}]: 'mount' must be a string path, got {:?}",
                source_file,
                index,
                other
            ));
        }
    };

    let mut attachment: steward::RemoteAttachment =
        serde_yaml::from_value(serde_yaml::Value::Mapping(mapping)).map_err(|e| {
            anyhow!(
                "{}[{}]: invalid {} spec: {}",
                source_file,
                index,
                doc.kind,
                e
            )
        })?;

    // Expand `${env:...}` in the URL only.  The URL is needed *here*, at
    // attach time, to open and validate the remote.  Every other field is
    // deliberately left as written: `to_storage_options` resolves references
    // per-replica at use time, so storing the reference rather than the value
    // keeps credentials out of the replicated attachment document -- the same
    // discipline D6 enforces for `secret_access_key`, applied to all of them.
    attachment.url = env_substitution::substitute_env_vars(&attachment.url).map_err(|e| {
        anyhow!(
            "{}[{}]: env expansion failed for url: {}",
            source_file,
            index,
            e
        )
    })?;

    let _limits = attachment
        .resolved_limits()
        .map_err(|e| anyhow!("{}[{}]: {}", source_file, index, e))?;

    let mode = if is_backup {
        if mount.is_some() {
            return Err(anyhow!(
                "{}[{}]: 'mount' is not valid for kind 'backup': a backup mirrors the \
                 whole pond, so the local pond IS the source",
                source_file,
                index
            ));
        }
        if bidirectional {
            RemoteMode::Both
        } else {
            RemoteMode::Push
        }
    } else {
        if bidirectional {
            return Err(anyhow!(
                "{}[{}]: 'bidirectional' is not valid for kind 'remote': remotes are \
                 always pull-mode. Use kind 'backup' with bidirectional: true.",
                source_file,
                index
            ));
        }
        RemoteMode::Pull
    };

    if mode == RemoteMode::Pull {
        let mount_path = mount.as_deref().ok_or_else(|| {
            anyhow!(
                "{}[{}]: kind 'remote' requires 'mount' ('/' for a mirror restart, \
                 '/imports/<name>' for a cross-pond import)",
                source_file,
                index
            )
        })?;
        if !mount_path.starts_with('/') {
            return Err(anyhow!(
                "{}[{}]: 'mount' must be absolute, got '{}'",
                source_file,
                index,
                mount_path
            ));
        }
    }

    Ok(ApplyKind::Remote {
        name,
        attachment,
        mode,
        mount,
        overwrite,
    })
}

// ---------------------------------------------------------------------------
// Command entry point
// ---------------------------------------------------------------------------

/// Apply one or more configuration files to the pond.
///
/// Each file may contain multiple resource documents separated by `---`.
/// All changes are made in a single transaction.
pub async fn apply_command(ship_context: &ShipContext, files: &[String]) -> Result<()> {
    if files.is_empty() {
        return Err(anyhow!("no files specified; use -f <file.yaml>"));
    }

    // Phase 1: Parse and validate all resources before opening a transaction.
    let mut specs = Vec::new();
    let mut claimed_paths: HashSet<String> = HashSet::new();

    for file_path in files {
        let content = fs::read_to_string(file_path)
            .map_err(|e| anyhow!("failed to read '{}': {}", file_path, e))?;

        let documents = split_yaml_documents(&content);
        if documents.is_empty() {
            return Err(anyhow!("{}: no YAML documents found", file_path));
        }

        for (i, doc_yaml) in documents.iter().enumerate() {
            let spec = parse_resource(doc_yaml, file_path, i)?;

            if !claimed_paths.insert(spec.path.clone()) {
                return Err(anyhow!(
                    "{}[{}]: duplicate path '{}' (already claimed by another resource)",
                    file_path,
                    i,
                    spec.path
                ));
            }

            specs.push(spec);
        }
    }

    // Sort: mkdir first, then copy, then mknod, then remotes; within each
    // group by path depth.  Remotes come last because an attachment may name
    // limiter nodes created by a `mknod` in the same apply, and attaching
    // validates those names -- so the nodes must be committed first.
    specs.sort_by(|a, b| {
        let order = |k: &ApplyKind| match k {
            ApplyKind::Mkdir => 0,
            ApplyKind::Copy(_) => 1,
            ApplyKind::Mknod { .. } => 2,
            ApplyKind::Remote { .. } => 3,
        };
        order(&a.kind)
            .cmp(&order(&b.kind))
            .then_with(|| {
                a.path
                    .matches('/')
                    .count()
                    .cmp(&b.path.matches('/').count())
            })
            .then(a.path.cmp(&b.path))
    });

    // Remotes are applied after -- and outside -- the main transaction:
    // attaching opens the remote to validate its store_id and runs its own
    // write, so it cannot be folded into a transaction that is still open.
    let (remote_specs, specs): (Vec<ApplySpec>, Vec<ApplySpec>) = specs
        .into_iter()
        .partition(|s| matches!(s.kind, ApplyKind::Remote { .. }));

    // Phase 2: Open transaction and apply
    let mut ship = ship_context.open_pond().await?;

    // Host steward for copy operations (read-only)
    let needs_host = specs.iter().any(|s| matches!(s.kind, ApplyKind::Copy(_)));
    let mut host_ship = if needs_host {
        Some(ship_context.open_host()?)
    } else {
        None
    };
    let host_read = if let Some(ref mut hs) = host_ship {
        Some(
            hs.begin_read(&steward::PondUserMetadata::new(vec![
                "apply-copy-source".to_string(),
            ]))
            .await
            .map_err(|e| anyhow!("apply: failed to open host for copy: {}", e))?,
        )
    } else {
        None
    };

    let tx = ship
        .begin_write(&ship_context.command_metadata())
        .await
        .map_err(|e| anyhow!("apply: failed to begin transaction: {}", e))?;

    let mut created = 0usize;
    let mut updated = 0usize;
    let mut unchanged = 0usize;

    for spec in &specs {
        let result = apply_one(&tx, &tx, spec, host_read.as_ref())
            .await
            .map_err(|e| anyhow!("{}: {}", spec.source_file, e))?;

        match result {
            ApplyResult::Created => {
                log::info!("  created  {}", spec.path);
                created += 1;
            }
            ApplyResult::Updated => {
                log::info!("  updated  {}", spec.path);
                updated += 1;
            }
            ApplyResult::Unchanged => {
                log::info!("  unchanged  {}", spec.path);
                unchanged += 1;
            }
        }
    }

    // Commit host read transaction if opened
    if let Some(host_tx) = host_read {
        _ = host_tx
            .commit()
            .await
            .map_err(|e| anyhow!("apply: failed to commit host read transaction: {}", e))?;
    }

    if created > 0 || updated > 0 {
        _ = tx
            .commit()
            .await
            .map_err(|e| anyhow!("apply: failed to commit transaction: {}", e))?;
        log::info!(
            "apply: {} created, {} updated, {} unchanged",
            created,
            updated,
            unchanged
        );
    } else {
        log::info!(
            "apply: all {} resource(s) unchanged, no transaction committed",
            unchanged
        );
        drop(tx);
    }

    // Phase 3: attach remotes.  `attach_remote` is the single writer of
    // /sys/remotes/<name> (Decision L11), so `pond apply` gets exactly the
    // same secret discipline, mount-conflict checks, and live remote
    // initialization as `pond backup add`.
    //
    // `ship` is dropped first: attaching opens the pond itself, and the pond
    // lock is not reentrant.
    drop(ship);
    for spec in &remote_specs {
        let ApplyKind::Remote {
            name,
            attachment,
            mode,
            mount,
            overwrite,
        } = &spec.kind
        else {
            continue;
        };
        attach_remote(
            ship_context,
            name,
            attachment.clone(),
            *mode,
            mount.as_deref(),
            *overwrite,
        )
        .await
        .map_err(|e| {
            anyhow!(
                "{}: attach {} `{}`: {}",
                spec.source_file,
                mode.as_str(),
                name,
                e
            )
        })?;
        log::info!("  attached  {}", spec.path);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Per-kind apply logic
// ---------------------------------------------------------------------------

/// Apply a single spec within an open transaction.
async fn apply_one(
    tx: &steward::Transaction<'_>,
    fs: &tinyfs::FS,
    spec: &ApplySpec,
    host_read: Option<&steward::Transaction<'_>>,
) -> Result<ApplyResult> {
    match &spec.kind {
        ApplyKind::Mkdir => apply_mkdir(fs, &spec.path).await,
        ApplyKind::Copy(config) => {
            let host_tx = host_read.ok_or_else(|| {
                anyhow!("copy requires host access but host steward not available")
            })?;
            apply_copy(fs, &spec.path, config, host_tx).await
        }
        // Attached after the transaction closes; see `apply_command` phase 3.
        ApplyKind::Remote { .. } => Ok(ApplyResult::Unchanged),
        ApplyKind::Mknod {
            factory_type,
            raw_config,
            expanded_config,
        } => {
            apply_mknod(
                tx,
                fs,
                &spec.path,
                factory_type,
                raw_config,
                expanded_config,
            )
            .await
        }
    }
}

/// Apply a mkdir spec: create the directory with -p semantics.
async fn apply_mkdir(fs: &tinyfs::FS, path: &str) -> Result<ApplyResult> {
    let root = fs.root().await?;

    let components: Vec<&str> = path
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();

    if components.is_empty() {
        return Ok(ApplyResult::Unchanged);
    }

    let mut any_created = false;
    let mut current_path = String::new();
    for component in components {
        current_path.push('/');
        current_path.push_str(component);

        if root.exists(&current_path).await {
            match root.open_dir_path(&current_path).await {
                Ok(_) => continue,
                Err(_) => {
                    return Err(anyhow!(
                        "path '{}' exists but is not a directory",
                        current_path
                    ));
                }
            }
        } else {
            _ = root
                .create_dir_path(&current_path)
                .await
                .map_err(|e| anyhow!("failed to create directory '{}': {}", current_path, e))?;
            any_created = true;
        }
    }

    if any_created {
        Ok(ApplyResult::Created)
    } else {
        Ok(ApplyResult::Unchanged)
    }
}

/// Apply a copy spec: copy host content into the pond.
async fn apply_copy(
    fs: &tinyfs::FS,
    dest_path: &str,
    config: &CopySpec,
    host_tx: &steward::Transaction<'_>,
) -> Result<ApplyResult> {
    let pond_root = fs.root().await?;
    let host_root = host_tx.root().await?;

    let host_path = parse_host_source(&config.source)?;

    // Ensure parent dirs exist
    ensure_parent_dirs(&pond_root, dest_path).await?;

    if host_root.open_dir_path(&host_path).await.is_ok() {
        // Directory copy
        if !config.overwrite && pond_root.exists(dest_path).await {
            return Err(anyhow!(
                "destination '{}' already exists; set overwrite: true to replace",
                dest_path
            ));
        }
        copy_directory_into_pond(&host_root, &pond_root, &host_path, dest_path).await?;
        Ok(ApplyResult::Created)
    } else {
        // Single file copy
        if !config.overwrite && pond_root.exists(dest_path).await {
            return Err(anyhow!(
                "destination '{}' already exists; set overwrite: true to replace",
                dest_path
            ));
        }
        copy_single_file_into_pond(&host_root, &pond_root, &host_path, dest_path).await?;
        Ok(ApplyResult::Created)
    }
}

/// Apply a mknod spec: create or update a factory node.
async fn apply_mknod(
    tx: &steward::Transaction<'_>,
    fs: &tinyfs::FS,
    path: &str,
    factory_type: &str,
    raw_config: &str,
    expanded_config: &str,
) -> Result<ApplyResult> {
    let root = fs.root().await?;

    ensure_parent_dirs(&root, path).await?;

    let (_, lookup) = root.resolve_path(path).await?;

    match lookup {
        tinyfs::Lookup::Found(existing_node) => {
            let node_id = existing_node.id();

            // Verify factory type matches
            let existing_factory = tx
                .get_factory_for_node(node_id)
                .await
                .map_err(|e| anyhow!("failed to get factory for '{}': {}", path, e))?;

            if let Some(ref factory_name) = existing_factory
                && factory_name != factory_type
            {
                return Err(anyhow!(
                    "factory type mismatch at '{}': existing is '{}', apply specifies '{}'. \
                    Cannot change factory type in place; remove the node first.",
                    path,
                    factory_name,
                    factory_type
                ));
            }

            // Compare config
            let existing_config = fs
                .get_dynamic_node_config(node_id)
                .await
                .map_err(|e| anyhow!("failed to read existing config at '{}': {}", path, e))?;

            let new_config_bytes = raw_config.as_bytes();

            if let Some((_, ref existing_bytes)) = existing_config
                && existing_bytes == new_config_bytes
            {
                return Ok(ApplyResult::Unchanged);
            }

            // Update
            let factory = FactoryRegistry::get_factory(factory_type)
                .ok_or_else(|| anyhow!("unknown factory type: {}", factory_type))?;
            let entry_type = determine_entry_type(factory, factory_type)?;

            if node_id.entry_type() != entry_type {
                return Err(anyhow!(
                    "entry type mismatch at '{}': existing {:?} vs factory '{}' {:?}",
                    path,
                    node_id.entry_type(),
                    factory_type,
                    entry_type
                ));
            }

            _ = root
                .create_dynamic_path_with_overwrite(
                    path,
                    entry_type,
                    factory_type,
                    new_config_bytes.to_vec(),
                    true,
                )
                .await
                .map_err(|e| anyhow!("failed to update config at '{}': {}", path, e))?;

            run_factory_init(tx, &root, path, factory_type, expanded_config).await?;
            Ok(ApplyResult::Updated)
        }
        tinyfs::Lookup::NotFound(_, _) => {
            let factory = FactoryRegistry::get_factory(factory_type)
                .ok_or_else(|| anyhow!("unknown factory type: {}", factory_type))?;
            let entry_type = determine_entry_type(factory, factory_type)?;

            _ = root
                .create_dynamic_path(
                    path,
                    entry_type,
                    factory_type,
                    raw_config.as_bytes().to_vec(),
                )
                .await
                .map_err(|e| anyhow!("failed to create node at '{}': {}", path, e))?;

            run_factory_init(tx, &root, path, factory_type, expanded_config).await?;
            Ok(ApplyResult::Created)
        }
        tinyfs::Lookup::Empty(_) => Err(anyhow!("empty path")),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Determine the entry type for a factory (mirrors mknod logic).
fn determine_entry_type(
    factory: &provider::DynamicFactory,
    factory_name: &str,
) -> Result<tinyfs::EntryType> {
    if factory.create_directory.is_some() {
        Ok(tinyfs::EntryType::DirectoryDynamic)
    } else if factory.create_file.is_some()
        || factory.execute.is_some()
        || factory.apply_table_transform.is_some()
    {
        if factory.try_as_queryable.is_some() {
            Ok(tinyfs::EntryType::TableDynamic)
        } else {
            Ok(tinyfs::EntryType::FileDynamic)
        }
    } else {
        Err(anyhow!(
            "factory '{}' does not support creating directories or files",
            factory_name
        ))
    }
}

/// Ensure all parent directories exist for the given path (mkdir -p semantics).
async fn ensure_parent_dirs(root: &tinyfs::WD, path: &str) -> Result<()> {
    let parent = std::path::Path::new(path)
        .parent()
        .unwrap_or(std::path::Path::new("/"));

    let components: Vec<&str> = parent
        .to_str()
        .unwrap_or("/")
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();

    let mut current_path = String::new();
    for component in components {
        current_path.push('/');
        current_path.push_str(component);

        if root.exists(&current_path).await {
            match root.open_dir_path(&current_path).await {
                Ok(_) => continue,
                Err(_) => {
                    return Err(anyhow!(
                        "path '{}' exists but is not a directory",
                        current_path
                    ));
                }
            }
        } else {
            _ = root.create_dir_path(&current_path).await.map_err(|e| {
                anyhow!(
                    "failed to create parent directory '{}': {}",
                    current_path,
                    e
                )
            })?;
        }
    }
    Ok(())
}

/// Run factory initialization after creating or updating a node.
async fn run_factory_init(
    tx: &steward::Transaction<'_>,
    root: &tinyfs::WD,
    path: &str,
    factory_type: &str,
    expanded_config: &str,
) -> Result<()> {
    let parent_path = std::path::Path::new(path)
        .parent()
        .unwrap_or(std::path::Path::new("/"));
    let parent_node_path = root.resolve_path(parent_path).await?;
    let parent_node_id = match parent_node_path.1 {
        tinyfs::Lookup::Found(node) => node.id(),
        _ => {
            return Err(anyhow!(
                "parent directory not found: {}",
                parent_path.display()
            ));
        }
    };

    let provider_context = tx
        .provider_context()
        .map_err(|e| anyhow!("failed to get provider context: {}", e))?;
    let context = provider::FactoryContext::new(provider_context, parent_node_id);

    FactoryRegistry::initialize::<tlogfs::TLogFSError>(
        factory_type,
        expanded_config.as_bytes(),
        context,
    )
    .await
    .map_err(|e| {
        anyhow!(
            "factory initialization failed for '{}': {}",
            factory_type,
            e
        )
    })?;

    Ok(())
}

/// Parse host source URL to extract the filesystem path.
fn parse_host_source(source: &str) -> Result<String> {
    if let Some(path) = source.strip_prefix("host:///") {
        Ok(format!("/{}", path))
    } else if source.starts_with("host+") {
        if let Some(pos) = source.find(":///") {
            Ok(format!("/{}", &source[pos + 4..]))
        } else {
            Err(anyhow!("invalid host URL: '{}'", source))
        }
    } else {
        Err(anyhow!(
            "copy source must be a host URL (host:///...), got '{}'",
            source
        ))
    }
}

/// Recursively copy a directory from host into the pond.
async fn copy_directory_into_pond(
    host_root: &tinyfs::WD,
    pond_root: &tinyfs::WD,
    host_path: &str,
    pond_path: &str,
) -> Result<()> {
    use futures::StreamExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    ensure_parent_dirs(pond_root, &format!("{}/x", pond_path)).await?;
    if !pond_root.exists(pond_path).await {
        _ = pond_root
            .create_dir_path(pond_path)
            .await
            .map_err(|e| anyhow!("failed to create destination dir '{}': {}", pond_path, e))?;
    }

    let host_dir = host_root
        .open_dir_path(host_path)
        .await
        .map_err(|e| anyhow!("failed to open host dir '{}': {}", host_path, e))?;

    let mut entries = host_dir.entries().await?;

    while let Some(entry) = entries.next().await {
        let entry = entry
            .map_err(|e| anyhow!("failed to read host dir entry in '{}': {}", host_path, e))?;
        let name = &entry.name;
        let src = format!("{}/{}", host_path, name);
        let dst = format!("{}/{}", pond_path, name);

        if entry.entry_type.is_directory() {
            Box::pin(copy_directory_into_pond(host_root, pond_root, &src, &dst)).await?;
        } else {
            let mut reader = host_root
                .async_reader_path(&src)
                .await
                .map_err(|e| anyhow!("failed to read host file '{}': {}", src, e))?;

            let mut content = Vec::new();
            _ = reader
                .read_to_end(&mut content)
                .await
                .map_err(|e| anyhow!("failed to read host file '{}': {}", src, e))?;

            let mut writer = pond_root
                .async_writer_path(&dst)
                .await
                .map_err(|e| anyhow!("failed to write pond file '{}': {}", dst, e))?;

            writer
                .write_all(&content)
                .await
                .map_err(|e| anyhow!("failed to write pond file '{}': {}", dst, e))?;
            writer
                .shutdown()
                .await
                .map_err(|e| anyhow!("failed to finalize pond file '{}': {}", dst, e))?;
        }
    }

    Ok(())
}

/// Copy a single file from host into the pond.
async fn copy_single_file_into_pond(
    host_root: &tinyfs::WD,
    pond_root: &tinyfs::WD,
    host_path: &str,
    pond_path: &str,
) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut reader = host_root
        .async_reader_path(host_path)
        .await
        .map_err(|e| anyhow!("failed to read host file '{}': {}", host_path, e))?;

    let mut content = Vec::new();
    _ = reader
        .read_to_end(&mut content)
        .await
        .map_err(|e| anyhow!("failed to read host file '{}': {}", host_path, e))?;

    let mut writer = pond_root
        .async_writer_path(pond_path)
        .await
        .map_err(|e| anyhow!("failed to write pond file '{}': {}", pond_path, e))?;

    writer
        .write_all(&content)
        .await
        .map_err(|e| anyhow!("failed to write pond file '{}': {}", pond_path, e))?;
    writer
        .shutdown()
        .await
        .map_err(|e| anyhow!("failed to finalize pond file '{}': {}", pond_path, e))?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::init::init_command;
    use crate::common::ShipContext;
    use std::path::PathBuf;
    use tempfile::TempDir;

    struct TestSetup {
        temp_dir: TempDir,
        ship_context: ShipContext,
    }

    impl TestSetup {
        async fn new() -> Result<Self> {
            let temp_dir = TempDir::new().expect("Failed to create temp directory");
            let pond_path = temp_dir.path().join("test_pond");

            let init_args = vec!["pond".to_string(), "init".to_string()];
            let ship_context = ShipContext::pond_only(Some(&pond_path), init_args.clone());
            init_command(&ship_context, "test-host").await?;

            Ok(Self {
                temp_dir,
                ship_context,
            })
        }

        /// Write a resource file and return its path
        fn write_resource(&self, filename: &str, content: &str) -> PathBuf {
            let file_path = self.temp_dir.path().join(filename);
            fs::write(&file_path, content).expect("Failed to write resource file");
            file_path
        }

        /// Verify that a node exists at the given path
        async fn node_exists(&self, pond_path: &str) -> bool {
            let mut ship = self.ship_context.open_pond().await.unwrap();
            let tx = ship
                .begin_read(&steward::PondUserMetadata::new(vec![
                    "test_verify".to_string(),
                ]))
                .await
                .unwrap();

            let result = {
                let fs_ref: &tinyfs::FS = &tx;
                let root = fs_ref.root().await.unwrap();
                root.exists(pond_path).await
            };

            _ = tx.commit().await.unwrap();
            result
        }

        /// Read config bytes from a dynamic node
        async fn read_node_config(&self, pond_path: &str) -> Vec<u8> {
            let mut ship = self.ship_context.open_pond().await.unwrap();
            let tx = ship
                .begin_read(&steward::PondUserMetadata::new(vec![
                    "test_read".to_string(),
                ]))
                .await
                .unwrap();

            let result = {
                let fs_ref: &tinyfs::FS = &tx;
                let root = fs_ref.root().await.unwrap();
                let (_, lookup) = root.resolve_path(pond_path).await.unwrap();
                match lookup {
                    tinyfs::Lookup::Found(node) => {
                        let config = fs_ref.get_dynamic_node_config(node.id()).await.unwrap();
                        config.map(|(_, bytes)| bytes).unwrap_or_default()
                    }
                    _ => panic!("Node not found: {}", pond_path),
                }
            };

            _ = tx.commit().await.unwrap();
            result
        }
    }

    // -- parsing tests --

    #[test]
    fn test_split_yaml_documents_single() {
        let docs = split_yaml_documents("version: v1\nkind: mkdir\nmetadata:\n  path: /a\n");
        assert_eq!(docs.len(), 1);
    }

    #[test]
    fn test_split_yaml_documents_multi() {
        let content = "version: v1\nkind: mkdir\nmetadata:\n  path: /a\n---\nversion: v1\nkind: mkdir\nmetadata:\n  path: /b\n";
        let docs = split_yaml_documents(content);
        assert_eq!(docs.len(), 2);
    }

    #[test]
    fn test_parse_resource_mkdir() {
        let yaml = "version: v1\nkind: mkdir\nmetadata:\n  path: /system/etc\n";
        let spec = parse_resource(yaml, "test.yaml", 0).unwrap();
        assert_eq!(spec.path, "/system/etc");
        assert!(matches!(spec.kind, ApplyKind::Mkdir));
    }

    #[test]
    fn test_parse_resource_bad_version() {
        let yaml = "version: v2\nkind: mkdir\nmetadata:\n  path: /a\n";
        let result = parse_resource(yaml, "test.yaml", 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("v1"));
    }

    #[test]
    fn test_parse_resource_relative_path() {
        let yaml = "version: v1\nkind: mkdir\nmetadata:\n  path: relative\n";
        let result = parse_resource(yaml, "test.yaml", 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("absolute"));
    }

    #[test]
    fn test_parse_resource_unknown_kind() {
        let yaml = "version: v1\nkind: frobnicate\nmetadata:\n  path: /a\n";
        let result = parse_resource(yaml, "test.yaml", 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown kind"));
    }

    #[test]
    fn test_parse_resource_unknown_field() {
        let yaml = "version: v1\nkind: mkdir\nmetadata:\n  path: /a\nextra: bad\n";
        let result = parse_resource(yaml, "test.yaml", 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown field"));
    }

    #[test]
    fn test_parse_resource_copy() {
        let yaml = "version: v1\nkind: copy\nmetadata:\n  path: /content\nspec:\n  source: \"host:///tmp/content\"\n";
        let spec = parse_resource(yaml, "test.yaml", 0).unwrap();
        assert_eq!(spec.path, "/content");
        assert!(matches!(spec.kind, ApplyKind::Copy(_)));
    }

    #[test]
    fn test_parse_resource_copy_invalid_source() {
        let yaml = "version: v1\nkind: copy\nmetadata:\n  path: /content\nspec:\n  source: \"/local/path\"\n";
        let result = parse_resource(yaml, "test.yaml", 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("host URL"));
    }

    #[test]
    fn test_parse_resource_mknod() {
        let yaml = "version: v1\nkind: mknod\nmetadata:\n  path: /data/derived\nspec:\n  factory: sql-derived-table\n  config:\n    patterns:\n      source: \"table:///data/*.table\"\n    query: \"SELECT 1\"\n";
        let spec = parse_resource(yaml, "test.yaml", 0).unwrap();
        assert_eq!(spec.path, "/data/derived");
        assert!(matches!(spec.kind, ApplyKind::Mknod { .. }));
    }

    // -- integration tests --

    #[tokio::test]
    async fn test_apply_mknod_create() -> Result<()> {
        let setup = TestSetup::new().await?;

        let path = setup.write_resource("derived.yaml",
            "version: v1\nkind: mknod\nmetadata:\n  path: /data/derived\nspec:\n  factory: sql-derived-table\n  config:\n    patterns:\n      source: \"table:///data/*.table\"\n    query: \"SELECT * FROM source\"\n",
        );

        apply_command(&setup.ship_context, &[path.to_string_lossy().to_string()]).await?;
        assert!(setup.node_exists("/data/derived").await);
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_mknod_idempotent() -> Result<()> {
        let setup = TestSetup::new().await?;

        let path = setup.write_resource("derived.yaml",
            "version: v1\nkind: mknod\nmetadata:\n  path: /data/derived\nspec:\n  factory: sql-derived-table\n  config:\n    patterns:\n      source: \"table:///data/*.table\"\n    query: \"SELECT * FROM source\"\n",
        );
        let arg = path.to_string_lossy().to_string();

        apply_command(&setup.ship_context, std::slice::from_ref(&arg)).await?;
        apply_command(&setup.ship_context, &[arg]).await?;
        assert!(setup.node_exists("/data/derived").await);
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_mknod_update() -> Result<()> {
        let setup = TestSetup::new().await?;

        let p1 = setup.write_resource("d1.yaml",
            "version: v1\nkind: mknod\nmetadata:\n  path: /data/derived\nspec:\n  factory: sql-derived-table\n  config:\n    patterns:\n      source: \"table:///data/*.table\"\n    query: \"SELECT 1\"\n",
        );
        apply_command(&setup.ship_context, &[p1.to_string_lossy().to_string()]).await?;

        let p2 = setup.write_resource("d2.yaml",
            "version: v1\nkind: mknod\nmetadata:\n  path: /data/derived\nspec:\n  factory: sql-derived-table\n  config:\n    patterns:\n      source: \"table:///data/*.table\"\n    query: \"SELECT 42\"\n",
        );
        apply_command(&setup.ship_context, &[p2.to_string_lossy().to_string()]).await?;

        let config = setup.read_node_config("/data/derived").await;
        let config_str = String::from_utf8(config).unwrap();
        assert!(config_str.contains("42"));
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_mkdir() -> Result<()> {
        let setup = TestSetup::new().await?;

        let path = setup.write_resource(
            "dirs.yaml",
            "version: v1\nkind: mkdir\nmetadata:\n  path: /system/etc\n",
        );
        apply_command(&setup.ship_context, &[path.to_string_lossy().to_string()]).await?;
        assert!(setup.node_exists("/system/etc").await);
        assert!(setup.node_exists("/system").await);
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_mkdir_idempotent() -> Result<()> {
        let setup = TestSetup::new().await?;

        let path = setup.write_resource(
            "dirs.yaml",
            "version: v1\nkind: mkdir\nmetadata:\n  path: /mydir\n",
        );
        let arg = path.to_string_lossy().to_string();

        apply_command(&setup.ship_context, std::slice::from_ref(&arg)).await?;
        apply_command(&setup.ship_context, &[arg]).await?;
        assert!(setup.node_exists("/mydir").await);
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_multi_resource_file() -> Result<()> {
        let setup = TestSetup::new().await?;

        let path = setup.write_resource("multi.yaml",
            "version: v1\nkind: mkdir\nmetadata:\n  path: /system/etc\n---\nversion: v1\nkind: mkdir\nmetadata:\n  path: /sources\n---\nversion: v1\nkind: mknod\nmetadata:\n  path: /data/derived\nspec:\n  factory: sql-derived-table\n  config:\n    patterns:\n      source: \"table:///data/*.table\"\n    query: \"SELECT 1\"\n",
        );

        apply_command(&setup.ship_context, &[path.to_string_lossy().to_string()]).await?;
        assert!(setup.node_exists("/system/etc").await);
        assert!(setup.node_exists("/sources").await);
        assert!(setup.node_exists("/data/derived").await);
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_duplicate_path_error() -> Result<()> {
        let setup = TestSetup::new().await?;

        let path = setup.write_resource("dup.yaml",
            "version: v1\nkind: mkdir\nmetadata:\n  path: /mydir\n---\nversion: v1\nkind: mkdir\nmetadata:\n  path: /mydir\n",
        );

        let result =
            apply_command(&setup.ship_context, &[path.to_string_lossy().to_string()]).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("duplicate path"));
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_no_files_error() -> Result<()> {
        let setup = TestSetup::new().await?;
        let result = apply_command(&setup.ship_context, &[]).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no files"));
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_invalid_factory() -> Result<()> {
        let setup = TestSetup::new().await?;

        let path = setup.write_resource("bad.yaml",
            "version: v1\nkind: mknod\nmetadata:\n  path: /data/bad\nspec:\n  factory: nonexistent-factory\n  foo: bar\n",
        );

        let result =
            apply_command(&setup.ship_context, &[path.to_string_lossy().to_string()]).await;
        assert!(result.is_err());
        assert!(!setup.node_exists("/data/bad").await);
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_auto_creates_parent_dirs() -> Result<()> {
        let setup = TestSetup::new().await?;

        let path = setup.write_resource("deep.yaml",
            "version: v1\nkind: mknod\nmetadata:\n  path: /system/etc/deep/nested/derived\nspec:\n  factory: sql-derived-table\n  config:\n    patterns:\n      source: \"table:///data/*.table\"\n    query: \"SELECT * FROM source\"\n",
        );

        apply_command(&setup.ship_context, &[path.to_string_lossy().to_string()]).await?;
        assert!(setup.node_exists("/system/etc/deep/nested/derived").await);
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_mknod_factory_type_mismatch() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create a node with sql-derived-table
        let p1 = setup.write_resource("d1.yaml",
            "version: v1\nkind: mknod\nmetadata:\n  path: /data/node\nspec:\n  factory: sql-derived-table\n  config:\n    patterns:\n      source: \"table:///data/*.table\"\n    query: \"SELECT 1\"\n",
        );
        apply_command(&setup.ship_context, &[p1.to_string_lossy().to_string()]).await?;

        // Try to apply with dynamic-dir factory at the same path
        let p2 = setup.write_resource("d2.yaml",
            "version: v1\nkind: mknod\nmetadata:\n  path: /data/node\nspec:\n  factory: dynamic-dir\n  config:\n    entries:\n      - name: child\n        factory: sql-derived-table\n        config:\n          patterns:\n            source: \"table:///x/*.table\"\n          query: \"SELECT 1\"\n",
        );
        let result = apply_command(&setup.ship_context, &[p2.to_string_lossy().to_string()]).await;
        assert!(result.is_err(), "should fail: {:?}", result);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("mismatch") || err.contains("type"),
            "error should mention type mismatch, got: {}",
            err
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_mkdir_non_dir_conflict() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create a factory node at /data/node
        let p1 = setup.write_resource("node.yaml",
            "version: v1\nkind: mknod\nmetadata:\n  path: /data/node\nspec:\n  factory: sql-derived-table\n  config:\n    patterns:\n      source: \"table:///data/*.table\"\n    query: \"SELECT 1\"\n",
        );
        apply_command(&setup.ship_context, &[p1.to_string_lossy().to_string()]).await?;

        // Try to mkdir at that same path -- should fail (not a directory)
        let p2 = setup.write_resource(
            "dir.yaml",
            "version: v1\nkind: mkdir\nmetadata:\n  path: /data/node\n",
        );
        let result = apply_command(&setup.ship_context, &[p2.to_string_lossy().to_string()]).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not a directory"));
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_ordering_mkdir_before_mknod() -> Result<()> {
        let setup = TestSetup::new().await?;

        // File lists mknod BEFORE mkdir, but apply should sort mkdir first
        let path = setup.write_resource("reversed.yaml",
            "version: v1\nkind: mknod\nmetadata:\n  path: /custom/derived\nspec:\n  factory: sql-derived-table\n  config:\n    patterns:\n      source: \"table:///data/*.table\"\n    query: \"SELECT 1\"\n---\nversion: v1\nkind: mkdir\nmetadata:\n  path: /custom\n",
        );

        apply_command(&setup.ship_context, &[path.to_string_lossy().to_string()]).await?;
        assert!(setup.node_exists("/custom").await);
        assert!(setup.node_exists("/custom/derived").await);
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_empty_file_error() -> Result<()> {
        let setup = TestSetup::new().await?;

        let path = setup.write_resource("empty.yaml", "");
        let result =
            apply_command(&setup.ship_context, &[path.to_string_lossy().to_string()]).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("no YAML documents")
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_mkdir_spec_rejected() -> Result<()> {
        let setup = TestSetup::new().await?;

        // mkdir with a non-null spec should be rejected
        let path = setup.write_resource(
            "bad_mkdir.yaml",
            "version: v1\nkind: mkdir\nmetadata:\n  path: /mydir\nspec:\n  extra: stuff\n",
        );
        let result =
            apply_command(&setup.ship_context, &[path.to_string_lossy().to_string()]).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("mkdir does not accept a spec")
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_mknod_missing_factory() -> Result<()> {
        let setup = TestSetup::new().await?;

        let path = setup.write_resource("no_factory.yaml",
            "version: v1\nkind: mknod\nmetadata:\n  path: /data/node\nspec:\n  config:\n    foo: bar\n",
        );
        let result =
            apply_command(&setup.ship_context, &[path.to_string_lossy().to_string()]).await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_mknod_missing_spec() -> Result<()> {
        let setup = TestSetup::new().await?;

        let path = setup.write_resource(
            "no_spec.yaml",
            "version: v1\nkind: mknod\nmetadata:\n  path: /data/node\n",
        );
        let result =
            apply_command(&setup.ship_context, &[path.to_string_lossy().to_string()]).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("mknod requires a spec")
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_multiple_files() -> Result<()> {
        let setup = TestSetup::new().await?;

        let f1 = setup.write_resource(
            "dirs.yaml",
            "version: v1\nkind: mkdir\nmetadata:\n  path: /alpha\n",
        );
        let f2 = setup.write_resource("nodes.yaml",
            "version: v1\nkind: mknod\nmetadata:\n  path: /alpha/derived\nspec:\n  factory: sql-derived-table\n  config:\n    patterns:\n      source: \"table:///data/*.table\"\n    query: \"SELECT 1\"\n",
        );

        apply_command(
            &setup.ship_context,
            &[
                f1.to_string_lossy().to_string(),
                f2.to_string_lossy().to_string(),
            ],
        )
        .await?;
        assert!(setup.node_exists("/alpha").await);
        assert!(setup.node_exists("/alpha/derived").await);
        Ok(())
    }
}
