// SPDX-License-Identifier: Apache-2.0

//! Build a format-independent recovery capsule from the current live pond.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sync_store::{
    CapsuleEntry, CapsuleLeaf, CapsuleManifest, CapsuleNode, CapsuleObject, CapsulePayloadKind,
    CapsuleSource, IncrementalFileLeafHasher, IncrementalTableLeafHasher, ObjectHash,
    capsule_series_root, decode_manifest, decode_recipe, decode_series,
    encode_canonical_attributes, encode_canonical_batch_rows, schema_fingerprint,
};
use tinyfs::EntryType;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{LimiterSet, Ship, StewardError};

/// A verified capsule manifest and the plain payload objects it references.
#[derive(Debug)]
pub struct CapsuleBuild {
    /// Canonical logical snapshot.
    pub manifest: CapsuleManifest,
    /// Distinct payloads staged on disk by BLAKE3 content address.
    pub payloads: CapsulePayloads,
    reused_payloads: HashSet<ObjectHash>,
}

impl CapsuleBuild {
    /// Number of payload objects inherited from the prior capsule generation.
    #[must_use]
    pub fn reused_payload_count(&self) -> usize {
        self.reused_payloads.len()
    }
}

/// Disk-backed capsule payload closure.
#[derive(Debug)]
pub struct CapsulePayloads {
    directory: tempfile::TempDir,
    objects: BTreeMap<ObjectHash, CapsuleObject>,
}

impl CapsulePayloads {
    fn new() -> Result<Self, StewardError> {
        Ok(Self {
            directory: tempfile::tempdir().map_err(|error| {
                StewardError::Content(format!("create capsule staging directory: {error}"))
            })?,
            objects: BTreeMap::new(),
        })
    }

    /// Directory containing payload files named `blake3=<hash>`.
    #[must_use]
    pub fn objects_dir(&self) -> &Path {
        self.directory.path()
    }

    /// Number of distinct staged payload objects.
    #[must_use]
    pub fn len(&self) -> usize {
        self.objects.len()
    }

    /// Whether no payload objects are staged.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }

    /// Path of a staged payload.
    #[must_use]
    pub fn path(&self, hash: ObjectHash) -> PathBuf {
        self.directory
            .path()
            .join(format!("blake3={}", hash.to_hex()))
    }

    /// Staged payload descriptors in hash order.
    pub fn objects(&self) -> impl Iterator<Item = &CapsuleObject> {
        self.objects.values()
    }
}

/// Build a recovery capsule from the pond's current immutable content tip.
///
/// Payloads are staged on disk and released from memory one source object at a
/// time. A single native table leaf is still decoded as one unit while its
/// logical hash is computed.
///
/// # Errors
///
/// Returns an error when the source content graph is inconsistent, a payload
/// is missing or corrupt, a table uses an unsupported logical schema, or the
/// resulting capsule violates its format contract.
pub async fn build_recovery_capsule(ship: &Ship) -> Result<CapsuleBuild, StewardError> {
    build_recovery_capsule_with_prior(ship, None).await
}

/// Build a capsule while reusing unchanged logical leaves from `prior`.
///
/// Reused payloads are not restaged; publication must use the matching
/// incremental publisher so their presence in the current remote generation
/// is checked before advancing the reference.
pub async fn build_recovery_capsule_incremental(
    ship: &Ship,
    prior: &CapsuleManifest,
) -> Result<CapsuleBuild, StewardError> {
    build_recovery_capsule_with_prior(ship, Some(prior)).await
}

async fn build_recovery_capsule_with_prior(
    ship: &Ship,
    prior: Option<&CapsuleManifest>,
) -> Result<CapsuleBuild, StewardError> {
    let materialized = crate::content_tree::materialize_content_objects(ship).await?;
    build_recovery_capsule_from_materialized(ship, &materialized, prior).await
}

pub(crate) async fn build_recovery_capsule_from_materialized(
    ship: &Ship,
    materialized: &crate::content_tree::MaterializedObjects,
    prior: Option<&CapsuleManifest>,
) -> Result<CapsuleBuild, StewardError> {
    let (_, manifest_bytes) = materialized.manifest.as_ref().ok_or_else(|| {
        StewardError::Content("materialized pond has no node manifest".to_string())
    })?;
    let native_entries = decode_manifest(manifest_bytes).map_err(StewardError::Content)?;
    let paths = resolve_paths(&native_entries)?;

    let pond_id = ship.control_table().pond_id_uuid().to_string();
    let commits =
        crate::content_tree::read_log_leaves(ship.data_persistence().table().clone(), &pond_id)
            .await?;
    let tip_bytes = commits
        .last()
        .ok_or_else(|| StewardError::Content("pond has no content tip".to_string()))?;
    let source_commit = sync_store::Commit::decode(tip_bytes)
        .map_err(|error| StewardError::Content(format!("decode content tip: {error}")))?;
    let source_tip = source_commit.hash();

    let mut payloads = CapsulePayloads::new()?;
    let mut reused_payloads = HashSet::new();
    let mut entries = Vec::with_capacity(native_entries.len());
    for native in &native_entries {
        let path = paths
            .get(&native.node_id)
            .cloned()
            .ok_or_else(|| StewardError::Content(format!("no path for node {}", native.node_id)))?;
        let prior_node = prior
            .and_then(|manifest| manifest.entries.iter().find(|entry| entry.path == path))
            .map(|entry| &entry.node);
        let node = match native.entry_type {
            EntryType::DirectoryPhysical => CapsuleNode::Directory,
            EntryType::Symlink => {
                let target = match prior_node {
                    Some(CapsuleNode::Symlink { target }) if target.hash == native.child_hash => {
                        let _ = reused_payloads.insert(target.hash);
                        target.clone()
                    }
                    _ => {
                        stage_payload(ship, materialized, native.child_hash, &mut payloads).await?
                    }
                };
                CapsuleNode::Symlink { target }
            }

            EntryType::DirectoryDynamic | EntryType::FileDynamic | EntryType::TableDynamic => {
                let recipe = match prior_node {
                    Some(CapsuleNode::Dynamic { recipe }) if recipe.hash == native.child_hash => {
                        let _ = reused_payloads.insert(recipe.hash);
                        recipe.clone()
                    }
                    _ => {
                        let recipe =
                            stage_payload(ship, materialized, native.child_hash, &mut payloads)
                                .await?;
                        let bytes = std::fs::read(payloads.path(recipe.hash)).map_err(|error| {
                            StewardError::Content(format!("read staged recipe for {path}: {error}"))
                        })?;
                        let _ = decode_recipe(&bytes).map_err(|error| {
                            StewardError::Content(format!("decode recipe for {path}: {error}"))
                        })?;
                        recipe
                    }
                };
                CapsuleNode::Dynamic { recipe }
            }
            EntryType::FilePhysicalVersion
            | EntryType::FilePhysicalSeries
            | EntryType::TablePhysicalVersion
            | EntryType::TablePhysicalSeries => {
                build_physical_node(
                    ship,
                    materialized,
                    native,
                    &path,
                    prior_node,
                    &mut payloads,
                    &mut reused_payloads,
                )
                .await?
            }
        };
        entries.push(CapsuleEntry {
            path,
            entry_type: native.entry_type,
            source_node_id: native.node_id.clone(),
            node,
        });
    }

    let metadata = ship.control_table().pond_metadata();
    let source = CapsuleSource {
        pond_id,
        birthplace: metadata.birthplace.clone(),
        source_tip,
        // Derive generation time from the immutable source tip so retrying an
        // unchanged push produces the same manifest root.
        exported_at_micros: source_commit.provenance.time_micros,
        tool_version: env!("CARGO_PKG_VERSION").to_string(),
    };
    let manifest = CapsuleManifest::new(source, entries).map_err(StewardError::Content)?;
    let declared: HashSet<ObjectHash> = manifest
        .payload_objects()
        .map_err(StewardError::Content)?
        .into_iter()
        .map(|object| object.hash)
        .collect();
    let actual: HashSet<ObjectHash> = payloads
        .objects
        .keys()
        .copied()
        .chain(reused_payloads.iter().copied())
        .collect();
    if declared != actual {
        return Err(StewardError::Content(
            "capsule payload closure differs from materialized payloads".to_string(),
        ));
    }
    Ok(CapsuleBuild {
        manifest,
        payloads,
        reused_payloads,
    })
}

/// Build and publish a capsule to an attached remote under its storage budget.
///
/// Local capsule construction happens before network access. Opening the
/// remote and every publication request are metered as one governed operation.
pub async fn open_and_publish_capsule_limited(
    ship: &Ship,
    url: &str,
    storage_options: HashMap<String, String>,
    limits: &mut LimiterSet,
) -> Result<sync_store::CapsulePublishOutcome, StewardError> {
    let capsule = build_recovery_capsule(ship).await?;
    crate::storage_meter::metered_op(
        url,
        limits,
        Box::pin(async move {
            let remote = sync_store::ContentRemote::open_at_url(url, storage_options)
                .await
                .map_err(|error| StewardError::Aborted(format!("open remote {url}: {error}")))?;
            remote
                .publish_capsule_directory(&capsule.manifest, capsule.payloads.objects_dir())
                .await
                .map_err(|error| {
                    StewardError::Content(format!("publish recovery capsule: {error}"))
                })
        }),
    )
    .await
}

/// List retained capsule generations from a remote under its storage budget.
pub async fn open_and_list_capsules_limited(
    url: &str,
    storage_options: HashMap<String, String>,
    limits: &mut LimiterSet,
) -> Result<Vec<(ObjectHash, CapsuleManifest)>, StewardError> {
    crate::storage_meter::metered_op(
        url,
        limits,
        Box::pin(async move {
            let remote = sync_store::ContentRemote::open_at_url(url, storage_options)
                .await
                .map_err(|error| StewardError::Aborted(format!("open remote {url}: {error}")))?;
            let roots = remote.capsule_roots().await.map_err(|error| {
                StewardError::Content(format!("list recovery capsules: {error}"))
            })?;
            let mut generations = Vec::with_capacity(roots.len());
            for root in roots {
                let manifest = remote.capsule_manifest(root).await.map_err(|error| {
                    StewardError::Content(format!("read recovery capsule {root}: {error}"))
                })?;
                generations.push((root, manifest));
            }
            Ok(generations)
        }),
    )
    .await
}

async fn build_physical_node(
    ship: &Ship,
    materialized: &crate::content_tree::MaterializedObjects,
    native: &sync_store::ManifestEntry,
    path: &str,
    prior_node: Option<&CapsuleNode>,
    payloads: &mut CapsulePayloads,
    reused_payloads: &mut HashSet<ObjectHash>,
) -> Result<CapsuleNode, StewardError> {
    let payload_kind = match native.entry_type {
        EntryType::FilePhysicalVersion | EntryType::FilePhysicalSeries => CapsulePayloadKind::File,
        EntryType::TablePhysicalVersion | EntryType::TablePhysicalSeries => {
            CapsulePayloadKind::Table
        }
        _ => {
            return Err(StewardError::Content(format!(
                "non-physical node passed to physical capsule builder: {path}"
            )));
        }
    };
    let hashes = match native.entry_type {
        EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries => {
            let bytes = materialized.inline.get(&native.child_hash).ok_or_else(|| {
                StewardError::Content(format!("series object {} is missing", native.child_hash))
            })?;
            decode_series(bytes).map_err(|error| {
                StewardError::Content(format!("decode series object for {path}: {error}"))
            })?
        }
        EntryType::FilePhysicalVersion | EntryType::TablePhysicalVersion => {
            vec![native.child_hash]
        }
        _ => unreachable!("entry type checked above"),
    };
    if hashes.len() != native.versions.len() {
        return Err(StewardError::Content(format!(
            "{path} has {} payload versions but {} metadata versions",
            hashes.len(),
            native.versions.len()
        )));
    }

    let mut objects = Vec::new();
    let mut leaves = Vec::new();
    let mut table_schema: Option<ObjectHash> = None;
    for (hash, metadata) in hashes.into_iter().zip(&native.versions) {
        let attributes = metadata
            .extended_attributes
            .as_deref()
            .map(encode_canonical_attributes)
            .transpose()
            .map_err(|error| {
                StewardError::Content(format!("logical attributes for {path}: {error}"))
            })?
            .map(|bytes| {
                String::from_utf8(bytes)
                    .expect("canonical logical attributes are always valid UTF-8")
            });
        if let Some((object, leaf, schema)) = reusable_prior_leaf(
            prior_node,
            payload_kind,
            hash,
            metadata.timestamp.unwrap_or_default(),
            metadata.min_event_time,
            metadata.max_event_time,
            attributes.as_deref(),
        ) {
            if let Some(schema) = schema {
                if let Some(expected) = table_schema
                    && expected != schema
                {
                    return Err(StewardError::Content(format!(
                        "reused table schema changes within capsule node {path}: {expected} != {schema}"
                    )));
                }
                table_schema = Some(schema);
            }
            let _ = reused_payloads.insert(object.hash);
            objects.push(object);
            leaves.push(leaf);
            continue;
        }

        let object = stage_payload(ship, materialized, hash, payloads).await?;
        let staged_path = payloads.path(hash);

        let (logical_hash, logical_count, schema) = match payload_kind {
            CapsulePayloadKind::File => {
                if object.size == 0 {
                    let _ = payloads.objects.remove(&hash);
                    continue;
                }
                let mut leaf_hasher = IncrementalFileLeafHasher::new(
                    object.size,
                    metadata.min_event_time,
                    metadata.max_event_time,
                    attributes.as_deref().map(str::as_bytes),
                )
                .map_err(|error| {
                    StewardError::Content(format!("hash file leaf for {path}: {error}"))
                })?;
                let mut file = std::fs::File::open(&staged_path).map_err(|error| {
                    StewardError::Content(format!("open staged file leaf for {path}: {error}"))
                })?;
                let mut buffer = vec![0u8; 1024 * 1024];
                loop {
                    let count = std::io::Read::read(&mut file, &mut buffer).map_err(|error| {
                        StewardError::Content(format!("read staged file leaf for {path}: {error}"))
                    })?;
                    if count == 0 {
                        break;
                    }
                    leaf_hasher.write(&buffer[..count]).map_err(|error| {
                        StewardError::Content(format!("hash file leaf for {path}: {error}"))
                    })?;
                }
                let logical_hash = leaf_hasher.finish().map_err(|error| {
                    StewardError::Content(format!("finish file leaf for {path}: {error}"))
                })?;
                (logical_hash, object.size, None)
            }
            CapsulePayloadKind::Table => {
                let file = std::fs::File::open(&staged_path).map_err(|error| {
                    StewardError::Content(format!("open staged table leaf for {path}: {error}"))
                })?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
                    StewardError::Content(format!("open Parquet leaf for {path}: {error}"))
                })?;
                let schema = builder.schema().as_ref().clone();
                let fingerprint = schema_fingerprint(&schema).map_err(|error| {
                    StewardError::Content(format!("fingerprint table schema for {path}: {error}"))
                })?;
                if let Some(expected) = table_schema
                    && expected != fingerprint
                {
                    return Err(StewardError::Content(format!(
                        "table schema changes within capsule node {path}: {expected} != {fingerprint}"
                    )));
                }
                table_schema = Some(fingerprint);
                let mut logical_count = 0u64;
                let mut canonical_rows_len = 0u64;
                for batch in builder.build().map_err(|error| {
                    StewardError::Content(format!("build Parquet reader for {path}: {error}"))
                })? {
                    let batch = batch.map_err(|error| {
                        StewardError::Content(format!("read Parquet rows for {path}: {error}"))
                    })?;
                    logical_count = logical_count
                        .checked_add(batch.num_rows() as u64)
                        .ok_or_else(|| {
                            StewardError::Content(format!(
                                "table leaf row count for {path} exceeds u64::MAX"
                            ))
                        })?;
                    let row_bytes =
                        encode_canonical_batch_rows(&schema, &batch).map_err(|error| {
                            StewardError::Content(format!("encode table rows for {path}: {error}"))
                        })?;
                    canonical_rows_len = canonical_rows_len
                        .checked_add(u64::try_from(row_bytes.len()).map_err(|_| {
                            StewardError::Content(format!(
                                "canonical table rows for {path} exceed u64::MAX"
                            ))
                        })?)
                        .ok_or_else(|| {
                            StewardError::Content(format!(
                                "canonical table rows for {path} exceed u64::MAX"
                            ))
                        })?;
                }
                if logical_count == 0 {
                    objects.push(object);
                    continue;
                }
                let mut leaf_hasher = IncrementalTableLeafHasher::new(
                    &schema,
                    logical_count,
                    canonical_rows_len,
                    metadata.min_event_time,
                    metadata.max_event_time,
                    attributes.as_deref().map(str::as_bytes),
                )
                .map_err(|error| {
                    StewardError::Content(format!("prepare table leaf for {path}: {error}"))
                })?;
                let file = std::fs::File::open(&staged_path).map_err(|error| {
                    StewardError::Content(format!("reopen staged table leaf for {path}: {error}"))
                })?;
                let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                    .map_err(|error| {
                        StewardError::Content(format!("reopen Parquet leaf for {path}: {error}"))
                    })?
                    .build()
                    .map_err(|error| {
                        StewardError::Content(format!("rebuild Parquet reader for {path}: {error}"))
                    })?;
                for batch in reader {
                    let batch = batch.map_err(|error| {
                        StewardError::Content(format!("reread Parquet rows for {path}: {error}"))
                    })?;
                    leaf_hasher.write_batch(&batch).map_err(|error| {
                        StewardError::Content(format!("hash table rows for {path}: {error}"))
                    })?;
                }
                let logical_hash = leaf_hasher.finish().map_err(|error| {
                    StewardError::Content(format!("finish table leaf for {path}: {error}"))
                })?;
                (logical_hash, logical_count, Some(fingerprint))
            }
        };
        if let Some(schema) = schema {
            table_schema = Some(schema);
        }
        objects.push(object);
        leaves.push(CapsuleLeaf {
            logical_hash,
            logical_count,
            source_timestamp: metadata.timestamp.unwrap_or_default(),
            min_event_time: metadata.min_event_time,
            max_event_time: metadata.max_event_time,
            logical_attributes: attributes,
        });
    }
    if payload_kind == CapsulePayloadKind::Table && table_schema.is_none() {
        return Err(StewardError::Content(format!(
            "table node {path} has no readable schema"
        )));
    }
    let logical_root = capsule_series_root(payload_kind, table_schema, &leaves);
    Ok(CapsuleNode::Physical {
        payload_kind,
        schema_fingerprint: table_schema,
        logical_root,
        objects,
        leaves,
    })
}

#[allow(clippy::too_many_arguments)]
fn reusable_prior_leaf(
    prior_node: Option<&CapsuleNode>,
    payload_kind: CapsulePayloadKind,
    source_hash: ObjectHash,
    source_timestamp: i64,
    min_event_time: Option<i64>,
    max_event_time: Option<i64>,
    logical_attributes: Option<&str>,
) -> Option<(CapsuleObject, CapsuleLeaf, Option<ObjectHash>)> {
    let CapsuleNode::Physical {
        payload_kind: prior_kind,
        schema_fingerprint,
        objects,
        leaves,
        ..
    } = prior_node?
    else {
        return None;
    };
    if *prior_kind != payload_kind || objects.len() != leaves.len() {
        return None;
    }
    objects
        .iter()
        .zip(leaves)
        .find(|(object, leaf)| {
            object.hash == source_hash
                && leaf.source_timestamp == source_timestamp
                && leaf.min_event_time == min_event_time
                && leaf.max_event_time == max_event_time
                && leaf.logical_attributes.as_deref() == logical_attributes
        })
        .map(|(object, leaf)| (object.clone(), leaf.clone(), *schema_fingerprint))
}

async fn stage_payload(
    ship: &Ship,
    materialized: &crate::content_tree::MaterializedObjects,
    hash: ObjectHash,
    payloads: &mut CapsulePayloads,
) -> Result<CapsuleObject, StewardError> {
    if let Some(existing) = payloads.objects.get(&hash) {
        return Ok(existing.clone());
    }
    if let Some(bytes) = materialized.inline.get(&hash) {
        return insert_payload(payloads, bytes.clone());
    }
    if !materialized.external_blobs.contains(&hash) {
        return Err(StewardError::Content(format!(
            "content object {hash} is missing"
        )));
    }
    let mut reader = ship
        .data_persistence()
        .open_large_file_reader_by_hash(&hash.to_hex())
        .await
        .map_err(|error| StewardError::Content(format!("open large file {hash}: {error}")))?;
    let partial = payloads
        .objects_dir()
        .join(format!(".partial-{}", hash.to_hex()));
    let mut file = tokio::fs::File::create(&partial).await.map_err(|error| {
        StewardError::Content(format!("create staged capsule payload {hash}: {error}"))
    })?;
    let mut hasher = blake3::Hasher::new();
    let mut size = 0u64;
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let count = reader
            .read(&mut buffer)
            .await
            .map_err(|error| StewardError::Content(format!("read large file {hash}: {error}")))?;
        if count == 0 {
            break;
        }
        size = size.checked_add(count as u64).ok_or_else(|| {
            StewardError::Content(format!("capsule payload {hash} exceeds u64::MAX"))
        })?;
        let _ = hasher.update(&buffer[..count]);
        file.write_all(&buffer[..count])
            .await
            .map_err(|error| StewardError::Content(format!("stage large file {hash}: {error}")))?;
    }
    file.flush().await.map_err(|error| {
        StewardError::Content(format!("flush staged capsule payload {hash}: {error}"))
    })?;
    drop(file);
    let computed = ObjectHash::from_bytes(*hasher.finalize().as_bytes());
    if computed != hash {
        let _ = tokio::fs::remove_file(&partial).await;
        return Err(StewardError::Content(format!(
            "payload bytes hash to {computed}, expected {hash}"
        )));
    }
    tokio::fs::rename(&partial, payloads.path(hash))
        .await
        .map_err(|error| {
            StewardError::Content(format!("finish staged capsule payload {hash}: {error}"))
        })?;
    let object = CapsuleObject { hash, size };
    let _ = payloads.objects.insert(hash, object.clone());
    Ok(object)
}

fn insert_payload(
    payloads: &mut CapsulePayloads,
    bytes: Vec<u8>,
) -> Result<CapsuleObject, StewardError> {
    let hash = ObjectHash::of_bytes(&bytes);
    let size = u64::try_from(bytes.len())
        .map_err(|_| StewardError::Content("capsule payload exceeds u64::MAX".to_string()))?;
    let object = CapsuleObject { hash, size };
    if let Some(existing) = payloads.objects.get(&hash) {
        if existing.size != size {
            return Err(StewardError::Content(format!(
                "capsule payload hash collision at {hash}"
            )));
        }
        return Ok(existing.clone());
    }
    std::fs::write(payloads.path(hash), &bytes)
        .map_err(|error| StewardError::Content(format!("stage capsule payload {hash}: {error}")))?;
    let _ = payloads.objects.insert(hash, object.clone());
    Ok(object)
}

fn resolve_paths(
    entries: &[sync_store::ManifestEntry],
) -> Result<HashMap<String, String>, StewardError> {
    let by_id: HashMap<&str, &sync_store::ManifestEntry> = entries
        .iter()
        .map(|entry| (entry.node_id.as_str(), entry))
        .collect();
    let root = entries
        .iter()
        .find(|entry| entry.parent_node_id.is_empty() && entry.name.is_empty())
        .ok_or_else(|| StewardError::Content("native manifest has no root".to_string()))?;
    let mut paths = HashMap::new();
    let _ = paths.insert(root.node_id.clone(), "/".to_string());
    let mut visiting = HashSet::new();
    for entry in entries {
        let _ = resolve_path(&entry.node_id, &by_id, &mut paths, &mut visiting)?;
    }
    Ok(paths)
}

fn resolve_path(
    node_id: &str,
    entries: &HashMap<&str, &sync_store::ManifestEntry>,
    paths: &mut HashMap<String, String>,
    visiting: &mut HashSet<String>,
) -> Result<String, StewardError> {
    if let Some(path) = paths.get(node_id) {
        return Ok(path.clone());
    }
    if !visiting.insert(node_id.to_string()) {
        return Err(StewardError::Content(format!(
            "cycle in native manifest at node {node_id}"
        )));
    }
    let entry = entries.get(node_id).ok_or_else(|| {
        StewardError::Content(format!("native manifest references missing node {node_id}"))
    })?;
    let parent = resolve_path(&entry.parent_node_id, entries, paths, visiting)?;
    let path = if parent == "/" {
        format!("/{}", entry.name)
    } else {
        format!("{parent}/{}", entry.name)
    };
    let _ = visiting.remove(node_id);
    let _ = paths.insert(node_id.to_string(), path.clone());
    Ok(path)
}
