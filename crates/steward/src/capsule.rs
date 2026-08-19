// SPDX-License-Identifier: Apache-2.0

//! Build a format-independent recovery capsule from the current live pond.

use std::collections::{BTreeMap, HashMap, HashSet};

use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sync_store::{
    CapsuleEntry, CapsuleLeaf, CapsuleManifest, CapsuleNode, CapsuleObject, CapsulePayloadKind,
    CapsuleSource, ObjectHash, capsule_series_root, decode_manifest, decode_recipe, decode_series,
    encode_canonical_attributes, file_leaf_hash, schema_fingerprint, table_leaf_hash,
};
use tinyfs::EntryType;
use tokio::io::AsyncReadExt;

use crate::{LimiterSet, Ship, StewardError};

/// A verified capsule manifest and the plain payload objects it references.
#[derive(Debug)]
pub struct CapsuleBuild {
    /// Canonical logical snapshot.
    pub manifest: CapsuleManifest,
    /// Distinct payload bytes keyed by their BLAKE3 content address.
    pub payloads: BTreeMap<ObjectHash, Vec<u8>>,
}

/// Build a recovery capsule from the pond's current immutable content tip.
///
/// This phase-one implementation retains the complete payload closure in
/// memory. It must be replaced with bounded staging/streaming before publishing
/// a production-sized first capsule; that change does not affect the wire
/// contract.
///
/// # Errors
///
/// Returns an error when the source content graph is inconsistent, a payload
/// is missing or corrupt, a table uses an unsupported logical schema, or the
/// resulting capsule violates its format contract.
pub async fn build_recovery_capsule(ship: &Ship) -> Result<CapsuleBuild, StewardError> {
    let materialized = crate::content_tree::materialize_content_objects(ship).await?;
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

    let mut payloads = BTreeMap::new();
    let mut entries = Vec::with_capacity(native_entries.len());
    for native in &native_entries {
        let path = paths
            .get(&native.node_id)
            .cloned()
            .ok_or_else(|| StewardError::Content(format!("no path for node {}", native.node_id)))?;
        let node = match native.entry_type {
            EntryType::DirectoryPhysical => CapsuleNode::Directory,
            EntryType::Symlink => {
                let bytes = object_bytes(ship, &materialized, native.child_hash).await?;
                let target = insert_payload(&mut payloads, bytes)?;
                CapsuleNode::Symlink { target }
            }

            EntryType::DirectoryDynamic | EntryType::FileDynamic | EntryType::TableDynamic => {
                let bytes = object_bytes(ship, &materialized, native.child_hash).await?;
                let _ = decode_recipe(&bytes).map_err(|error| {
                    StewardError::Content(format!("decode recipe for {path}: {error}"))
                })?;
                let recipe = insert_payload(&mut payloads, bytes)?;
                CapsuleNode::Dynamic { recipe }
            }
            EntryType::FilePhysicalVersion
            | EntryType::FilePhysicalSeries
            | EntryType::TablePhysicalVersion
            | EntryType::TablePhysicalSeries => {
                build_physical_node(ship, &materialized, native, &path, &mut payloads).await?
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
    let actual: HashSet<ObjectHash> = payloads.keys().copied().collect();
    if declared != actual {
        return Err(StewardError::Content(
            "capsule payload closure differs from materialized payloads".to_string(),
        ));
    }
    Ok(CapsuleBuild { manifest, payloads })
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
                .publish_capsule(&capsule.manifest, &capsule.payloads)
                .await
                .map_err(|error| {
                    StewardError::Content(format!("publish recovery capsule: {error}"))
                })
        }),
    )
    .await
}

async fn build_physical_node(
    ship: &Ship,
    materialized: &crate::content_tree::MaterializedObjects,
    native: &sync_store::ManifestEntry,
    path: &str,
    payloads: &mut BTreeMap<ObjectHash, Vec<u8>>,
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
        let bytes = object_bytes(ship, materialized, hash).await?;
        if ObjectHash::of_bytes(&bytes) != hash {
            return Err(StewardError::Content(format!(
                "payload for {path} does not hash to {hash}"
            )));
        }
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

        let (logical_hash, logical_count, schema) = match payload_kind {
            CapsulePayloadKind::File => {
                if bytes.is_empty() {
                    continue;
                }
                let logical_hash = file_leaf_hash(
                    &bytes,
                    metadata.min_event_time,
                    metadata.max_event_time,
                    attributes.as_deref(),
                )
                .map_err(|error| {
                    StewardError::Content(format!("hash file leaf for {path}: {error}"))
                })?;
                (
                    logical_hash,
                    u64::try_from(bytes.len()).map_err(|_| {
                        StewardError::Content(format!("file leaf for {path} exceeds u64::MAX"))
                    })?,
                    None,
                )
            }
            CapsulePayloadKind::Table => {
                let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.clone()))
                    .map_err(|error| {
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
                let batches = builder
                    .build()
                    .map_err(|error| {
                        StewardError::Content(format!("build Parquet reader for {path}: {error}"))
                    })?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| {
                        StewardError::Content(format!("read Parquet rows for {path}: {error}"))
                    })?;
                let logical_count = batches.iter().try_fold(0u64, |count, batch| {
                    count.checked_add(batch.num_rows() as u64).ok_or_else(|| {
                        StewardError::Content(format!(
                            "table leaf row count for {path} exceeds u64::MAX"
                        ))
                    })
                })?;
                if logical_count == 0 {
                    objects.push(insert_payload(payloads, bytes)?);
                    continue;
                }
                let logical_hash = table_leaf_hash(
                    &schema,
                    &batches,
                    metadata.min_event_time,
                    metadata.max_event_time,
                    attributes.as_deref(),
                )
                .map_err(|error| {
                    StewardError::Content(format!("hash table leaf for {path}: {error}"))
                })?;
                (logical_hash, logical_count, Some(fingerprint))
            }
        };
        if let Some(schema) = schema {
            table_schema = Some(schema);
        }
        objects.push(insert_payload(payloads, bytes)?);
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

async fn object_bytes(
    ship: &Ship,
    materialized: &crate::content_tree::MaterializedObjects,
    hash: ObjectHash,
) -> Result<Vec<u8>, StewardError> {
    if let Some(bytes) = materialized.inline.get(&hash) {
        return Ok(bytes.clone());
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
    let mut bytes = Vec::new();
    let _ = reader
        .read_to_end(&mut bytes)
        .await
        .map_err(|error| StewardError::Content(format!("read large file {hash}: {error}")))?;
    Ok(bytes)
}

fn insert_payload(
    payloads: &mut BTreeMap<ObjectHash, Vec<u8>>,
    bytes: Vec<u8>,
) -> Result<CapsuleObject, StewardError> {
    let hash = ObjectHash::of_bytes(&bytes);
    let size = u64::try_from(bytes.len())
        .map_err(|_| StewardError::Content("capsule payload exceeds u64::MAX".to_string()))?;
    if let Some(existing) = payloads.insert(hash, bytes.clone())
        && existing != bytes
    {
        return Err(StewardError::Content(format!(
            "capsule payload hash collision at {hash}"
        )));
    }
    Ok(CapsuleObject { hash, size })
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
