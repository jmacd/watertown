// SPDX-License-Identifier: Apache-2.0

//! Opaque legacy-migration capsule envelope.
//!
//! `pondcapsule.legacy.1` and `pondcapsule.legacy.2` are intentionally separate
//! from the frozen logical `pondcapsule.1` and homogeneous-schema `pondcapsule.2` protocols. They
//! authenticates the exact legacy payload bytes and their native
//! `dp.series.1` version mapping without interpreting Parquet.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;

use serde::{Deserialize, Serialize};
use tinyfs::EntryType;

use super::{ObjectHash, decode_recipe, decode_series};

/// Frozen opaque capsule format that did not represent dynamic-node metadata.
pub const LEGACY_CAPSULE_FORMAT_V1: &str = "pondcapsule.legacy.1";
/// Opaque capsule format that preserves dynamic-node timestamps.
pub const LEGACY_CAPSULE_FORMAT_V2: &str = "pondcapsule.legacy.2";
/// Current opaque capsule format used for legacy migration.
pub const LEGACY_CAPSULE_FORMAT: &str = LEGACY_CAPSULE_FORMAT_V2;
/// Native source format accepted by `pondcapsule.legacy.1`.
pub const LEGACY_NATIVE_FORMAT_DP_COMMIT_3: &str = "dp.commit.3";

const LEGACY_CAPSULE_ROOT_DOMAIN_V1: &[u8] = b"pondcapsule.legacy.root.1\n";
const LEGACY_CAPSULE_ROOT_DOMAIN_V2: &[u8] = b"pondcapsule.legacy.root.2\n";
const LEGACY_RECIPE_MAGIC: &[u8] = b"dp.recipe.1\n";

/// Provenance of the verified legacy snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyCapsuleSource {
    /// Source pond identity.
    pub pond_id: String,
    /// Human-readable source birthplace label.
    pub birthplace: String,
    /// Native `dp.commit.3` source tip.
    #[serde(with = "hash_serde")]
    pub source_tip: ObjectHash,
    /// Source commit time, in microseconds since the Unix epoch.
    pub exported_at_micros: i64,
    /// Exact extractor version.
    pub tool_version: String,
    /// Native graph protocol; currently exactly `dp.commit.3`.
    pub native_format: String,
}

/// Exact raw object descriptor.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyCapsuleObject {
    /// BLAKE3 of the exact stored bytes.
    #[serde(with = "hash_serde")]
    pub hash: ObjectHash,
    /// Exact stored byte length.
    pub size: u64,
}

/// Physical payload interpretation owned by the target importer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyCapsulePayloadKind {
    /// Opaque file bytes.
    File,
    /// Opaque Parquet bytes, analyzed only by the target.
    Table,
}

/// One source physical version and its exact native metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyCapsuleVersion {
    /// Zero-based position in the source version sequence.
    pub source_version: u64,
    /// Ordered raw objects making up this source version.
    pub objects: Vec<LegacyCapsuleObject>,
    /// Source modification time, if the native metadata carried one.
    pub source_timestamp: Option<i64>,
    /// Independently optional source event-time lower bound.
    pub min_event_time: Option<i64>,
    /// Independently optional source event-time upper bound.
    pub max_event_time: Option<i64>,
    /// Exact source extended-attributes JSON text.
    pub extended_attributes: Option<String>,
}

/// Exact synthetic metadata persisted for a legacy dynamic node.
///
/// Dynamic nodes have no physical versions, but the native graph can retain
/// one metadata record for their creation timestamp.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyCapsuleDynamicMetadata {
    /// Dynamic node modification time in microseconds since the Unix epoch.
    pub timestamp: i64,
}

/// Opaque content of one legacy namespace entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LegacyCapsuleNode {
    /// Physical directory.
    Directory,
    /// Raw UTF-8 symlink target.
    Symlink {
        /// Exact native target object.
        target: LegacyCapsuleObject,
    },
    /// Raw `dp.recipe.1` dynamic recipe.
    Dynamic {
        /// Exact native recipe object.
        recipe: LegacyCapsuleObject,
        /// Synthetic dynamic-node metadata. Absent in frozen
        /// `pondcapsule.legacy.1` capsules.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<LegacyCapsuleDynamicMetadata>,
    },
    /// Physical file or table versions.
    Physical {
        /// Target interpretation of each raw version.
        payload_kind: LegacyCapsulePayloadKind,
        /// Native manifest child hash.
        #[serde(with = "hash_serde")]
        source_child_hash: ObjectHash,
        /// Exact `dp.series.1` object for a series; absent for a singleton.
        series_object: Option<LegacyCapsuleObject>,
        /// Ordered source versions.
        versions: Vec<LegacyCapsuleVersion>,
    },
}

/// One canonical live path in a legacy capsule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyCapsuleEntry {
    /// Canonical absolute path.
    pub path: String,
    /// Original filesystem entry type.
    pub entry_type: EntryType,
    /// Original native node identity, retained as provenance.
    pub source_node_id: String,
    /// Opaque entry content and physical mapping.
    pub node: LegacyCapsuleNode,
}

/// Complete opaque legacy snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyCapsuleManifest {
    /// A supported legacy capsule format.
    pub format: String,
    /// Verified source snapshot provenance.
    pub source: LegacyCapsuleSource,
    /// Canonical entries sorted by UTF-8 path bytes.
    pub entries: Vec<LegacyCapsuleEntry>,
}

/// Summary of a deeply verified legacy capsule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyCapsuleVerifyReport {
    /// Verified legacy capsule root.
    pub root: ObjectHash,
    /// Number of namespace entries.
    pub entries: usize,
    /// Number of distinct raw objects in the exact closure.
    pub payload_objects: usize,
    /// Sum of distinct raw object bytes.
    pub physical_bytes: u64,
    /// Number of mapped physical versions.
    pub physical_versions: usize,
}

impl LegacyCapsuleManifest {
    /// Construct, sort, and validate an opaque legacy manifest.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid source provenance, topology, entry
    /// semantics, version ordering, metadata, or object declarations.
    pub fn new(
        source: LegacyCapsuleSource,
        mut entries: Vec<LegacyCapsuleEntry>,
    ) -> Result<Self, String> {
        entries.sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
        let manifest = Self {
            format: LEGACY_CAPSULE_FORMAT.to_string(),
            source,
            entries,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    /// Validate the complete legacy envelope without opening raw objects.
    ///
    /// # Errors
    ///
    /// Returns a specific error for the first invariant violation.
    pub fn validate(&self) -> Result<(), String> {
        if self.format != LEGACY_CAPSULE_FORMAT_V1 && self.format != LEGACY_CAPSULE_FORMAT_V2 {
            return Err(format!(
                "unsupported legacy capsule format {:?}; expected {LEGACY_CAPSULE_FORMAT_V1:?} \
                 or {LEGACY_CAPSULE_FORMAT_V2:?}",
                self.format
            ));
        }
        if self.source.native_format != LEGACY_NATIVE_FORMAT_DP_COMMIT_3 {
            return Err(format!(
                "unsupported legacy native format {:?}; expected \
                 {LEGACY_NATIVE_FORMAT_DP_COMMIT_3:?}",
                self.source.native_format
            ));
        }
        if self.source.pond_id.is_empty() {
            return Err("legacy capsule source pond_id must not be empty".to_string());
        }
        if self.source.birthplace.is_empty() {
            return Err("legacy capsule source birthplace must not be empty".to_string());
        }
        if self.source.tool_version.is_empty() {
            return Err("legacy capsule source tool_version must not be empty".to_string());
        }
        if self.source.exported_at_micros <= 0 {
            return Err("legacy capsule source exported_at_micros must be positive".to_string());
        }

        let mut prior_path: Option<&str> = None;
        let mut entries_by_path = BTreeMap::new();
        let mut node_ids = HashSet::new();
        for entry in &self.entries {
            validate_path(&entry.path)?;
            if let Some(prior) = prior_path {
                match prior.as_bytes().cmp(entry.path.as_bytes()) {
                    std::cmp::Ordering::Less => {}
                    std::cmp::Ordering::Equal => {
                        return Err(format!("duplicate legacy capsule path {:?}", entry.path));
                    }
                    std::cmp::Ordering::Greater => {
                        return Err(
                            "legacy capsule entries are not in canonical path order".to_string()
                        );
                    }
                }
            }
            prior_path = Some(&entry.path);
            if entry.source_node_id.is_empty() {
                return Err(format!(
                    "legacy capsule path {:?} has an empty source_node_id",
                    entry.path
                ));
            }
            if !node_ids.insert(entry.source_node_id.as_str()) {
                return Err(format!(
                    "duplicate legacy capsule source_node_id {:?}",
                    entry.source_node_id
                ));
            }
            validate_entry(entry, self.format == LEGACY_CAPSULE_FORMAT_V2)?;
            let _ = entries_by_path.insert(entry.path.as_str(), entry);
        }

        let Some(root) = entries_by_path.get("/") else {
            return Err("legacy capsule manifest has no root entry".to_string());
        };
        if root.entry_type != EntryType::DirectoryPhysical
            || !matches!(root.node, LegacyCapsuleNode::Directory)
        {
            return Err("legacy capsule root must be a physical directory".to_string());
        }
        for entry in self.entries.iter().filter(|entry| entry.path != "/") {
            let parent = parent_path(&entry.path);
            let Some(parent_entry) = entries_by_path.get(parent.as_str()) else {
                return Err(format!(
                    "legacy capsule path {:?} has missing parent {:?}",
                    entry.path, parent
                ));
            };
            if !matches!(parent_entry.node, LegacyCapsuleNode::Directory) {
                return Err(format!(
                    "legacy capsule path {:?} has non-directory parent {:?}",
                    entry.path, parent
                ));
            }
        }
        let _ = self.payload_objects()?;
        Ok(())
    }

    /// Distinct raw object closure, sorted by hash.
    ///
    /// # Errors
    ///
    /// Returns an error if one hash is declared with conflicting sizes.
    pub fn payload_objects(&self) -> Result<Vec<LegacyCapsuleObject>, String> {
        let mut objects = BTreeMap::new();
        for entry in &self.entries {
            for object in entry_objects(&entry.node) {
                if let Some(prior_size) = objects.insert(object.hash, object.size)
                    && prior_size != object.size
                {
                    return Err(format!(
                        "legacy capsule object {} has conflicting sizes {prior_size} and {}",
                        object.hash, object.size
                    ));
                }
            }
        }
        Ok(objects
            .into_iter()
            .map(|(hash, size)| LegacyCapsuleObject { hash, size })
            .collect())
    }
}

/// Encode a validated legacy manifest as canonical JSON.
///
/// # Errors
///
/// Returns an error if validation or serialization fails.
pub fn legacy_capsule_manifest_bytes(manifest: &LegacyCapsuleManifest) -> Result<Vec<u8>, String> {
    manifest.validate()?;
    serde_json::to_vec(manifest).map_err(|error| format!("encode legacy capsule manifest: {error}"))
}

/// Compute the domain-separated root of a legacy manifest.
///
/// # Errors
///
/// Returns an error if the manifest is invalid.
pub fn legacy_capsule_root(manifest: &LegacyCapsuleManifest) -> Result<ObjectHash, String> {
    let bytes = legacy_capsule_manifest_bytes(manifest)?;
    let mut hasher = blake3::Hasher::new();
    let domain = match manifest.format.as_str() {
        LEGACY_CAPSULE_FORMAT_V1 => LEGACY_CAPSULE_ROOT_DOMAIN_V1,
        LEGACY_CAPSULE_FORMAT_V2 => LEGACY_CAPSULE_ROOT_DOMAIN_V2,
        _ => unreachable!("manifest validation accepts only supported formats"),
    };
    let _ = hasher.update(domain);
    let _ = hasher.update(&bytes);
    Ok(ObjectHash::from_bytes(*hasher.finalize().as_bytes()))
}

/// Decode canonical `pondcapsule.legacy.1` or `pondcapsule.legacy.2` JSON bytes.
///
/// # Errors
///
/// Rejects malformed JSON, unknown fields, invalid semantics, and any
/// noncanonical JSON encoding.
pub fn decode_legacy_capsule_manifest(bytes: &[u8]) -> Result<LegacyCapsuleManifest, String> {
    let manifest: LegacyCapsuleManifest = serde_json::from_slice(bytes)
        .map_err(|error| format!("decode legacy capsule manifest: {error}"))?;
    manifest.validate()?;
    if legacy_capsule_manifest_bytes(&manifest)? != bytes {
        return Err("legacy capsule manifest is not canonically encoded".to_string());
    }
    Ok(manifest)
}

/// Read the manifest named by `recovery/refs/latest`.
///
/// # Errors
///
/// Rejects an unsafe layout, malformed exact ref, invalid manifest, or root
/// mismatch.
pub fn read_legacy_capsule_manifest(
    path: &Path,
) -> Result<(LegacyCapsuleManifest, ObjectHash), String> {
    let recovery = path.join("recovery");
    let reference_path = recovery.join("refs/latest");
    require_regular_file(&reference_path, "legacy capsule latest ref")?;
    let reference = std::fs::read(&reference_path)
        .map_err(|error| format!("read recovery/refs/latest: {error}"))?;
    if reference.len() != 65 || reference[64] != b'\n' {
        return Err(
            "legacy capsule latest ref must be exactly 64 lowercase hex bytes plus newline"
                .to_string(),
        );
    }
    let root_text = std::str::from_utf8(&reference[..64])
        .map_err(|error| format!("legacy capsule latest ref is not ASCII: {error}"))?;
    if root_text
        .bytes()
        .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
    {
        return Err("legacy capsule latest ref must use lowercase hexadecimal".to_string());
    }
    let root = ObjectHash::from_hex(root_text)?;
    let manifest_path = recovery
        .join("manifests")
        .join(format!("{}.json", root.to_hex()));
    require_regular_file(&manifest_path, "legacy capsule manifest")?;
    let bytes = std::fs::read(&manifest_path)
        .map_err(|error| format!("read legacy capsule manifest {root}: {error}"))?;
    let manifest = decode_legacy_capsule_manifest(&bytes)?;
    let computed = legacy_capsule_root(&manifest)?;
    if computed != root {
        return Err(format!(
            "legacy capsule manifest hashes to {computed}, latest ref names {root}"
        ));
    }
    Ok((manifest, root))
}

/// Deeply verify a downloaded legacy capsule.
///
/// Parquet objects remain opaque: verification checks only exact bytes and
/// the native `dp.series.1` leaf-to-object mapping.
///
/// # Errors
///
/// Rejects missing, extra, non-regular, corrupt, or semantically mismapped
/// objects.
pub fn verify_legacy_capsule_directory(path: &Path) -> Result<LegacyCapsuleVerifyReport, String> {
    let (manifest, root) = read_legacy_capsule_manifest(path)?;
    verify_legacy_capsule_payload_directory_at_root(&manifest, &path.join("recovery/objects"), root)
}

/// Deeply verify a legacy manifest against an on-disk exact object closure.
///
/// # Errors
///
/// Rejects a corrupt, missing, extra, non-regular, or mismapped object.
pub fn verify_legacy_capsule_payload_directory(
    manifest: &LegacyCapsuleManifest,
    objects_dir: &Path,
) -> Result<LegacyCapsuleVerifyReport, String> {
    let root = legacy_capsule_root(manifest)?;
    verify_legacy_capsule_payload_directory_at_root(manifest, objects_dir, root)
}

fn verify_legacy_capsule_payload_directory_at_root(
    manifest: &LegacyCapsuleManifest,
    objects_dir: &Path,
    root: ObjectHash,
) -> Result<LegacyCapsuleVerifyReport, String> {
    manifest.validate()?;
    let objects = manifest.payload_objects()?;
    let expected_names: BTreeSet<String> = objects
        .iter()
        .map(|object| format!("blake3={}", object.hash.to_hex()))
        .collect();
    let actual_names = exact_regular_file_names(objects_dir)?;
    if actual_names != expected_names {
        let missing: Vec<_> = expected_names.difference(&actual_names).cloned().collect();
        let extra: Vec<_> = actual_names.difference(&expected_names).cloned().collect();
        return Err(format!(
            "legacy capsule object closure mismatch: missing {missing:?}, extra {extra:?}"
        ));
    }

    let mut physical_bytes = 0u64;
    for object in &objects {
        verify_object_file(objects_dir, object)?;
        physical_bytes = physical_bytes
            .checked_add(object.size)
            .ok_or_else(|| "legacy capsule physical bytes exceed u64::MAX".to_string())?;
    }

    let mut physical_versions = 0usize;
    for entry in &manifest.entries {
        match &entry.node {
            LegacyCapsuleNode::Directory => {}
            LegacyCapsuleNode::Symlink { target } => {
                let bytes = read_verified_object(objects_dir, target)?;
                std::str::from_utf8(&bytes).map_err(|error| {
                    format!(
                        "legacy capsule symlink target for {:?} is not UTF-8: {error}",
                        entry.path
                    )
                })?;
            }
            LegacyCapsuleNode::Dynamic { recipe, .. } => {
                let bytes = read_verified_object(objects_dir, recipe)?;
                if !bytes.starts_with(LEGACY_RECIPE_MAGIC) {
                    return Err(format!(
                        "legacy capsule dynamic recipe for {:?} is not dp.recipe.1",
                        entry.path
                    ));
                }
                let _ = decode_recipe(&bytes).map_err(|error| {
                    format!(
                        "decode legacy capsule dynamic recipe for {:?}: {error}",
                        entry.path
                    )
                })?;
            }
            LegacyCapsuleNode::Physical {
                series_object,
                versions,
                ..
            } => {
                physical_versions = physical_versions
                    .checked_add(versions.len())
                    .ok_or_else(|| "legacy capsule version count exceeds usize::MAX".to_string())?;
                if let Some(series_object) = series_object {
                    let bytes = read_verified_object(objects_dir, series_object)?;
                    let hashes = decode_series(&bytes).map_err(|error| {
                        format!(
                            "decode legacy capsule series mapping for {:?}: {error}",
                            entry.path
                        )
                    })?;
                    let declared: Vec<ObjectHash> = versions
                        .iter()
                        .map(|version| version.objects[0].hash)
                        .collect();
                    if hashes != declared {
                        return Err(format!(
                            "legacy capsule source leaf mapping mismatch for {:?}: \
                             dp.series.1 names {:?}, manifest versions name {:?}",
                            entry.path, hashes, declared
                        ));
                    }
                }
            }
        }
    }

    Ok(LegacyCapsuleVerifyReport {
        root,
        entries: manifest.entries.len(),
        payload_objects: objects.len(),
        physical_bytes,
        physical_versions,
    })
}

fn validate_entry(
    entry: &LegacyCapsuleEntry,
    supports_dynamic_metadata: bool,
) -> Result<(), String> {
    match (&entry.entry_type, &entry.node) {
        (EntryType::DirectoryPhysical, LegacyCapsuleNode::Directory)
        | (EntryType::Symlink, LegacyCapsuleNode::Symlink { .. }) => Ok(()),
        (
            EntryType::DirectoryDynamic | EntryType::FileDynamic | EntryType::TableDynamic,
            LegacyCapsuleNode::Dynamic { metadata, .. },
        ) => {
            match (supports_dynamic_metadata, metadata) {
                (false, Some(_)) => {
                    return Err(format!(
                        "legacy capsule path {:?} has dynamic metadata in frozen \
                         {LEGACY_CAPSULE_FORMAT_V1}",
                        entry.path
                    ));
                }
                (true, None) => {
                    return Err(format!(
                        "legacy capsule path {:?} has no dynamic metadata in \
                         {LEGACY_CAPSULE_FORMAT_V2}",
                        entry.path
                    ));
                }
                _ => {}
            }
            Ok(())
        }
        (
            EntryType::FilePhysicalVersion
            | EntryType::FilePhysicalSeries
            | EntryType::TablePhysicalVersion
            | EntryType::TablePhysicalSeries,
            LegacyCapsuleNode::Physical {
                payload_kind,
                source_child_hash,
                series_object,
                versions,
            },
        ) => validate_physical(
            &entry.path,
            entry.entry_type,
            *payload_kind,
            *source_child_hash,
            series_object.as_ref(),
            versions,
        ),
        _ => Err(format!(
            "legacy capsule path {:?} entry type {:?} does not match node {:?}",
            entry.path, entry.entry_type, entry.node
        )),
    }
}

fn validate_physical(
    path: &str,
    entry_type: EntryType,
    payload_kind: LegacyCapsulePayloadKind,
    source_child_hash: ObjectHash,
    series_object: Option<&LegacyCapsuleObject>,
    versions: &[LegacyCapsuleVersion],
) -> Result<(), String> {
    let expected_kind = match entry_type {
        EntryType::FilePhysicalVersion | EntryType::FilePhysicalSeries => {
            LegacyCapsulePayloadKind::File
        }
        EntryType::TablePhysicalVersion | EntryType::TablePhysicalSeries => {
            LegacyCapsulePayloadKind::Table
        }
        _ => unreachable!("caller restricts physical entry types"),
    };
    if payload_kind != expected_kind {
        return Err(format!(
            "legacy capsule path {path:?} payload kind {payload_kind:?} disagrees with \
             entry type {entry_type:?}"
        ));
    }
    let is_series = matches!(
        entry_type,
        EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries
    );
    if versions.is_empty() {
        return Err(format!(
            "legacy capsule physical path {path:?} must carry at least one source version"
        ));
    }
    if is_series {
        let series = series_object.ok_or_else(|| {
            format!("legacy capsule series path {path:?} has no source series object")
        })?;
        if series.hash != source_child_hash {
            return Err(format!(
                "legacy capsule series path {path:?} source child hash {source_child_hash} \
                 disagrees with series object {}",
                series.hash
            ));
        }
    } else {
        if series_object.is_some() {
            return Err(format!(
                "legacy capsule singleton path {path:?} must not carry a series object"
            ));
        }
        if versions.len() != 1 {
            return Err(format!(
                "legacy capsule singleton path {path:?} must carry exactly one version"
            ));
        }
    }

    for (index, version) in versions.iter().enumerate() {
        if version.source_version != index as u64 {
            return Err(format!(
                "legacy capsule path {path:?} version {index} declares source_version {}",
                version.source_version
            ));
        }
        if version.objects.len() != 1 {
            return Err(format!(
                "legacy capsule path {path:?} version {index} must map to exactly one raw \
                 dp.commit.3 payload object"
            ));
        }
        if let Some((minimum, maximum)) = version.min_event_time.zip(version.max_event_time)
            && minimum > maximum
        {
            return Err(format!(
                "legacy capsule path {path:?} version {index} minimum event time exceeds maximum"
            ));
        }
        if let Some(attributes) = &version.extended_attributes {
            let value: serde_json::Value = serde_json::from_str(attributes).map_err(|error| {
                format!(
                    "legacy capsule path {path:?} version {index} has invalid extended \
                     attributes JSON: {error}"
                )
            })?;
            if !value.is_object() {
                return Err(format!(
                    "legacy capsule path {path:?} version {index} extended attributes must be a \
                     JSON object"
                ));
            }
        }
    }
    if !is_series && versions[0].objects[0].hash != source_child_hash {
        return Err(format!(
            "legacy capsule singleton path {path:?} source child hash {source_child_hash} \
             disagrees with payload object {}",
            versions[0].objects[0].hash
        ));
    }
    Ok(())
}

fn entry_objects(node: &LegacyCapsuleNode) -> Vec<&LegacyCapsuleObject> {
    match node {
        LegacyCapsuleNode::Directory => Vec::new(),
        LegacyCapsuleNode::Symlink { target } => vec![target],
        LegacyCapsuleNode::Dynamic { recipe, .. } => vec![recipe],
        LegacyCapsuleNode::Physical {
            series_object,
            versions,
            ..
        } => series_object
            .iter()
            .chain(versions.iter().flat_map(|version| version.objects.iter()))
            .collect(),
    }
}

fn validate_path(path: &str) -> Result<(), String> {
    if path == "/" {
        return Ok(());
    }
    if !path.starts_with('/') {
        return Err(format!("legacy capsule path {path:?} is not absolute"));
    }
    if path.ends_with('/') {
        return Err(format!("legacy capsule path {path:?} has a trailing slash"));
    }
    if path.contains('\0') {
        return Err(format!("legacy capsule path {path:?} contains NUL"));
    }
    for component in path[1..].split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(format!(
                "legacy capsule path {path:?} has unsafe component {component:?}"
            ));
        }
    }
    Ok(())
}

fn parent_path(path: &str) -> String {
    let index = path.rfind('/').expect("validated absolute path");
    if index == 0 {
        "/".to_string()
    } else {
        path[..index].to_string()
    }
}

fn require_regular_file(path: &Path, description: &str) -> Result<(), String> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| format!("inspect {description} {}: {error}", path.display()))?;
    if !metadata.file_type().is_file() {
        return Err(format!(
            "{description} {} is not a regular file",
            path.display()
        ));
    }
    Ok(())
}

fn exact_regular_file_names(path: &Path) -> Result<BTreeSet<String>, String> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| format!("inspect legacy capsule objects directory: {error}"))?;
    if !metadata.file_type().is_dir() {
        return Err("legacy capsule recovery/objects is not a directory".to_string());
    }
    let mut names = BTreeSet::new();
    for entry in std::fs::read_dir(path)
        .map_err(|error| format!("read legacy capsule objects directory: {error}"))?
    {
        let entry = entry
            .map_err(|error| format!("read legacy capsule object directory entry: {error}"))?;
        let metadata = std::fs::symlink_metadata(entry.path()).map_err(|error| {
            format!(
                "inspect legacy capsule object {}: {error}",
                entry.path().display()
            )
        })?;
        if !metadata.file_type().is_file() {
            return Err(format!(
                "legacy capsule object {} is not a regular file",
                entry.path().display()
            ));
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| "legacy capsule object filename is not valid Unicode ASCII".to_string())?;
        let _ = names.insert(name);
    }
    Ok(names)
}

fn read_verified_object(
    objects_dir: &Path,
    object: &LegacyCapsuleObject,
) -> Result<Vec<u8>, String> {
    let path = objects_dir.join(format!("blake3={}", object.hash.to_hex()));
    require_regular_file(&path, "legacy capsule object")?;
    let bytes = std::fs::read(&path)
        .map_err(|error| format!("read legacy capsule object {}: {error}", object.hash))?;
    let computed = ObjectHash::of_bytes(&bytes);
    if computed != object.hash || bytes.len() as u64 != object.size {
        return Err(format!(
            "legacy capsule object {} has hash {computed} and size {}, expected size {}",
            object.hash,
            bytes.len(),
            object.size
        ));
    }
    Ok(bytes)
}

fn verify_object_file(objects_dir: &Path, object: &LegacyCapsuleObject) -> Result<(), String> {
    let path = objects_dir.join(format!("blake3={}", object.hash.to_hex()));
    require_regular_file(&path, "legacy capsule object")?;
    let mut file = std::fs::File::open(&path)
        .map_err(|error| format!("open legacy capsule object {}: {error}", object.hash))?;
    let mut hasher = blake3::Hasher::new();
    let mut size = 0u64;
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let count = std::io::Read::read(&mut file, &mut buffer)
            .map_err(|error| format!("read legacy capsule object {}: {error}", object.hash))?;
        if count == 0 {
            break;
        }
        let _ = hasher.update(&buffer[..count]);
        size = size
            .checked_add(count as u64)
            .ok_or_else(|| format!("legacy capsule object {} exceeds u64::MAX", object.hash))?;
    }
    let computed = ObjectHash::from_bytes(*hasher.finalize().as_bytes());
    if computed != object.hash || size != object.size {
        return Err(format!(
            "legacy capsule object {} has hash {computed} and size {size}, expected size {}",
            object.hash, object.size
        ));
    }
    Ok(())
}

mod hash_serde {
    use serde::{Deserialize, Deserializer, Serializer};

    use super::ObjectHash;

    pub fn serialize<S>(hash: &ObjectHash, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hash.to_hex())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<ObjectHash, D::Error>
    where
        D: Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        if text.bytes().any(|byte| byte.is_ascii_uppercase()) {
            return Err(serde::de::Error::custom(
                "legacy capsule hashes must use lowercase hexadecimal",
            ));
        }
        ObjectHash::from_hex(&text).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(bytes: &[u8]) -> LegacyCapsuleObject {
        LegacyCapsuleObject {
            hash: ObjectHash::of_bytes(bytes),
            size: bytes.len() as u64,
        }
    }

    fn fixture() -> (LegacyCapsuleManifest, BTreeMap<ObjectHash, Vec<u8>>) {
        let first = b"first".to_vec();
        let second = b"second".to_vec();
        let hashes = vec![ObjectHash::of_bytes(&first), ObjectHash::of_bytes(&second)];
        let series = super::super::encode_series(&hashes);
        let source = LegacyCapsuleSource {
            pond_id: "pond".to_string(),
            birthplace: "legacy".to_string(),
            source_tip: ObjectHash::of_bytes(b"tip"),
            exported_at_micros: 1,
            tool_version: "test".to_string(),
            native_format: LEGACY_NATIVE_FORMAT_DP_COMMIT_3.to_string(),
        };
        let manifest = LegacyCapsuleManifest::new(
            source,
            vec![
                LegacyCapsuleEntry {
                    path: "/".to_string(),
                    entry_type: EntryType::DirectoryPhysical,
                    source_node_id: "root".to_string(),
                    node: LegacyCapsuleNode::Directory,
                },
                LegacyCapsuleEntry {
                    path: "/series".to_string(),
                    entry_type: EntryType::FilePhysicalSeries,
                    source_node_id: "series".to_string(),
                    node: LegacyCapsuleNode::Physical {
                        payload_kind: LegacyCapsulePayloadKind::File,
                        source_child_hash: ObjectHash::of_bytes(&series),
                        series_object: Some(object(&series)),
                        versions: vec![
                            LegacyCapsuleVersion {
                                source_version: 0,
                                objects: vec![object(&first)],
                                source_timestamp: Some(10),
                                min_event_time: None,
                                max_event_time: None,
                                extended_attributes: None,
                            },
                            LegacyCapsuleVersion {
                                source_version: 1,
                                objects: vec![object(&second)],
                                source_timestamp: Some(20),
                                min_event_time: None,
                                max_event_time: None,
                                extended_attributes: None,
                            },
                        ],
                    },
                },
            ],
        )
        .unwrap();
        let payloads = BTreeMap::from([
            (ObjectHash::of_bytes(&first), first),
            (ObjectHash::of_bytes(&second), second),
            (ObjectHash::of_bytes(&series), series),
        ]);
        (manifest, payloads)
    }

    fn materialize(
        root: &Path,
        manifest: &LegacyCapsuleManifest,
        payloads: &BTreeMap<ObjectHash, Vec<u8>>,
    ) {
        let capsule_root = legacy_capsule_root(manifest).unwrap();
        std::fs::create_dir_all(root.join("recovery/refs")).unwrap();
        std::fs::create_dir_all(root.join("recovery/manifests")).unwrap();
        std::fs::create_dir_all(root.join("recovery/objects")).unwrap();
        std::fs::write(
            root.join("recovery/refs/latest"),
            format!("{}\n", capsule_root.to_hex()),
        )
        .unwrap();
        std::fs::write(
            root.join(format!("recovery/manifests/{}.json", capsule_root.to_hex())),
            legacy_capsule_manifest_bytes(manifest).unwrap(),
        )
        .unwrap();
        for (hash, bytes) in payloads {
            std::fs::write(
                root.join(format!("recovery/objects/blake3={}", hash.to_hex())),
                bytes,
            )
            .unwrap();
        }
    }

    #[test]
    fn verifies_exact_opaque_mapping_and_closure() {
        let temporary = tempfile::tempdir().unwrap();
        let (manifest, payloads) = fixture();
        materialize(temporary.path(), &manifest, &payloads);
        let report = verify_legacy_capsule_directory(temporary.path()).unwrap();
        assert_eq!(report.physical_versions, 2);

        std::fs::write(
            temporary.path().join("recovery/objects/blake3=extra"),
            b"extra",
        )
        .unwrap();
        let error = verify_legacy_capsule_directory(temporary.path()).unwrap_err();
        assert!(error.contains("extra"));
    }

    #[test]
    fn rejects_mapping_rewritten_away_from_native_series_object() {
        let temporary = tempfile::tempdir().unwrap();
        let (mut manifest, payloads) = fixture();
        let LegacyCapsuleNode::Physical { versions, .. } = &mut manifest.entries[1].node else {
            panic!("physical fixture");
        };
        versions.swap(0, 1);
        versions[0].source_version = 0;
        versions[1].source_version = 1;
        materialize(temporary.path(), &manifest, &payloads);
        let error = verify_legacy_capsule_directory(temporary.path()).unwrap_err();
        assert!(error.contains("mapping mismatch"), "{error}");
    }

    #[test]
    fn preserves_dynamic_metadata_only_in_legacy_v2() {
        let recipe = b"dp.recipe.1\n\x04\0\0\0testconfig".to_vec();
        let source = LegacyCapsuleSource {
            pond_id: "pond".to_string(),
            birthplace: "legacy".to_string(),
            source_tip: ObjectHash::of_bytes(b"tip"),
            exported_at_micros: 1,
            tool_version: "test".to_string(),
            native_format: LEGACY_NATIVE_FORMAT_DP_COMMIT_3.to_string(),
        };
        let manifest = LegacyCapsuleManifest::new(
            source,
            vec![
                LegacyCapsuleEntry {
                    path: "/".to_string(),
                    entry_type: EntryType::DirectoryPhysical,
                    source_node_id: "root".to_string(),
                    node: LegacyCapsuleNode::Directory,
                },
                LegacyCapsuleEntry {
                    path: "/dynamic".to_string(),
                    entry_type: EntryType::DirectoryDynamic,
                    source_node_id: "dynamic".to_string(),
                    node: LegacyCapsuleNode::Dynamic {
                        recipe: object(&recipe),
                        metadata: Some(LegacyCapsuleDynamicMetadata { timestamp: 42 }),
                    },
                },
            ],
        )
        .unwrap();
        assert_eq!(manifest.format, LEGACY_CAPSULE_FORMAT_V2);
        assert!(
            legacy_capsule_manifest_bytes(&manifest)
                .unwrap()
                .windows(br#""metadata":{"timestamp":42}"#.len())
                .any(|window| window == br#""metadata":{"timestamp":42}"#)
        );

        let mut frozen = manifest;
        frozen.format = LEGACY_CAPSULE_FORMAT_V1.to_string();
        assert!(frozen.validate().unwrap_err().contains("dynamic metadata"));
    }
}
