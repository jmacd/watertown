// SPDX-License-Identifier: Apache-2.0

//! Stable logical recovery-capsule manifest.
//!
//! A capsule is deliberately independent of the native commit and Delta
//! encodings. Ordinary object-store clients copy the manifest and its declared
//! payload objects; a compatible importer verifies this module's logical
//! contract before writing a fresh pond.

use std::collections::{BTreeMap, VecDeque};
use std::path::Path;

use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use tinyfs::EntryType;

use super::ObjectHash;

/// Recovery-capsule format identifier.
pub const CAPSULE_FORMAT_V1: &str = "pondcapsule.1";
/// The second portable capsule format used by newly-created Watertown ponds.
pub const CAPSULE_FORMAT_V2: &str = "pondcapsule.2";
/// The third portable capsule format, with a schema identity on every table leaf.
pub const CAPSULE_FORMAT_V3: &str = "pondcapsule.3";
/// The fourth portable capsule format, preserving dynamic-node metadata.
pub const CAPSULE_FORMAT_V4: &str = "pondcapsule.4";

const CAPSULE_ROOT_DOMAIN: &[u8] = b"pondcapsule.root.1\n";
const CAPSULE_ROOT_DOMAIN_V2: &[u8] = b"pondcapsule.root.2\n";
const CAPSULE_ROOT_DOMAIN_V3: &[u8] = b"pondcapsule.root.3\n";
const CAPSULE_ROOT_DOMAIN_V4: &[u8] = b"pondcapsule.root.4\n";
const CAPSULE_SERIES_DOMAIN: &[u8] = b"pondcapsule.series.1\n";
const CAPSULE_SERIES_DOMAIN_V3: &[u8] = b"pondcapsule.series.3\n";
const LOGICAL_LEAF_DOMAIN: &[u8] = b"watertown.series-leaf.v1\n";

/// Provenance of the live snapshot represented by a capsule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapsuleSource {
    /// Source pond identity.
    pub pond_id: String,
    /// Source pond birthplace label.
    pub birthplace: String,
    /// Native source content tip at which the snapshot was taken.
    #[serde(with = "hash_serde")]
    pub source_tip: ObjectHash,
    /// Export time in microseconds since the Unix epoch.
    pub exported_at_micros: i64,
    /// Exact producer version that created the capsule.
    pub tool_version: String,
}

/// Kind of logical stream carried by a physical node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapsulePayloadKind {
    /// Exact byte stream.
    File,
    /// Ordered Arrow rows encoded physically as standard Parquet.
    Table,
}

/// One immutable, independently downloadable payload object.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapsuleObject {
    /// BLAKE3 of the exact stored object bytes.
    #[serde(with = "hash_serde")]
    pub hash: ObjectHash,
    /// Exact object length.
    pub size: u64,
}

/// Exact synthetic metadata persisted for a dynamic node.
///
/// Dynamic nodes have no physical versions, but a native manifest can retain
/// one record containing their creation timestamp.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapsuleDynamicMetadata {
    /// Dynamic node modification time in microseconds since the Unix epoch.
    pub timestamp: i64,
}

/// One ordered logical append leaf in a physical file or table stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapsuleLeaf {
    /// BLAKE3 logical identity under the capsule v1 canonical leaf rules.
    #[serde(with = "hash_serde")]
    pub logical_hash: ObjectHash,
    /// Byte count for files or row count for tables.
    pub logical_count: u64,
    /// Source version modification time, in microseconds since the Unix epoch.
    pub source_timestamp: i64,
    /// Independently optional minimum event time, in microseconds.
    pub min_event_time: Option<i64>,
    /// Independently optional maximum event time, in microseconds.
    pub max_event_time: Option<i64>,
    /// Canonical JSON object containing the source leaf's logical attributes.
    pub logical_attributes: Option<String>,
    /// BLAKE3 of this table leaf's canonical Arrow schema.
    ///
    /// Present for every table leaf in `pondcapsule.3` and `.4`. Files never
    /// declare schemas; v1/v2 keep their frozen representation without it.
    #[serde(
        default,
        with = "optional_hash_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub schema_fingerprint: Option<ObjectHash>,
}

/// Format-independent logical content of one live namespace entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CapsuleNode {
    /// A physical or dynamic directory with no payload.
    Directory,
    /// A symlink target encoded as one exact raw object.
    Symlink {
        /// Object containing the target path bytes.
        target: CapsuleObject,
    },
    /// A dynamic-node recipe encoded as one exact object.
    Dynamic {
        /// Object containing the native recipe bytes.
        recipe: CapsuleObject,
        /// Synthetic dynamic-node metadata.
        ///
        /// Absent in frozen `pondcapsule.1` through `.3` capsules. A V4
        /// producer records it whenever the native node carries it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<CapsuleDynamicMetadata>,
    },
    /// A physical file or table stream.
    Physical {
        /// Logical payload interpretation.
        payload_kind: CapsulePayloadKind,
        /// BLAKE3 of the canonical logical Arrow schema for table payloads.
        /// File payloads must leave this absent.
        #[serde(default, with = "optional_hash_serde")]
        schema_fingerprint: Option<ObjectHash>,
        /// Domain-separated root over the ordered logical leaf descriptors.
        #[serde(with = "hash_serde")]
        logical_root: ObjectHash,
        /// Ordered physical stream objects. Their decoded content is
        /// concatenated before being partitioned by `leaves`.
        objects: Vec<CapsuleObject>,
        /// Ordered logical append leaves.
        leaves: Vec<CapsuleLeaf>,
    },
}

/// One canonical live path in the capsule inventory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapsuleEntry {
    /// Canonical absolute path. The root is `/`.
    pub path: String,
    /// Original filesystem entry type.
    pub entry_type: EntryType,
    /// Source node identity, retained as provenance rather than adopted by a
    /// fresh imported pond.
    pub source_node_id: String,
    /// Format-independent logical content.
    pub node: CapsuleNode,
}

/// Complete live logical snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapsuleManifest {
    /// Must equal one of the supported `pondcapsule.*` format identifiers.
    pub format: String,
    /// Source snapshot provenance.
    pub source: CapsuleSource,
    /// Canonical entries sorted by UTF-8 path bytes.
    pub entries: Vec<CapsuleEntry>,
}

/// Summary of a fully verified downloaded capsule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapsuleVerifyReport {
    /// Verified capsule root.
    pub root: ObjectHash,
    /// Number of live namespace entries.
    pub entries: usize,
    /// Number of distinct physical payload objects.
    pub payload_objects: usize,
    /// Sum of distinct physical payload bytes.
    pub physical_bytes: u64,
    /// Sum of logical bytes and rows across every leaf.
    pub logical_count: u64,
}

impl CapsuleManifest {
    /// Construct and validate a v1 manifest.
    ///
    /// Entries are sorted canonically before validation and encoding.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid source, path topology, entry/content
    /// mismatch, logical descriptor, or noncanonical attribute object.
    pub fn new(source: CapsuleSource, entries: Vec<CapsuleEntry>) -> Result<Self, String> {
        Self::new_with_format(CAPSULE_FORMAT_V1, source, entries)
    }

    /// Construct and validate a v2 manifest for the new Watertown native format.
    pub fn new_v2(source: CapsuleSource, entries: Vec<CapsuleEntry>) -> Result<Self, String> {
        Self::new_with_format(CAPSULE_FORMAT_V2, source, entries)
    }

    /// Construct and validate a v3 manifest.
    pub fn new_v3(source: CapsuleSource, entries: Vec<CapsuleEntry>) -> Result<Self, String> {
        Self::new_with_format(CAPSULE_FORMAT_V3, source, entries)
    }

    /// Construct and validate a v4 manifest.
    pub fn new_v4(source: CapsuleSource, entries: Vec<CapsuleEntry>) -> Result<Self, String> {
        Self::new_with_format(CAPSULE_FORMAT_V4, source, entries)
    }

    fn new_with_format(
        format: &str,
        source: CapsuleSource,
        mut entries: Vec<CapsuleEntry>,
    ) -> Result<Self, String> {
        entries.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
        let manifest = Self {
            format: format.to_string(),
            source,
            entries,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    /// Validate the complete manifest and its logical topology.
    ///
    /// # Errors
    ///
    /// Returns a specific diagnostic for the first violated capsule invariant.
    pub fn validate(&self) -> Result<(), String> {
        if !matches!(
            self.format.as_str(),
            CAPSULE_FORMAT_V1 | CAPSULE_FORMAT_V2 | CAPSULE_FORMAT_V3 | CAPSULE_FORMAT_V4
        ) {
            return Err(format!(
                "unsupported capsule format {:?}; expected {CAPSULE_FORMAT_V1:?}, \
                 {CAPSULE_FORMAT_V2:?}, {CAPSULE_FORMAT_V3:?}, or {CAPSULE_FORMAT_V4:?}",
                self.format
            ));
        }
        if self.source.pond_id.is_empty() {
            return Err("capsule source pond_id must not be empty".to_string());
        }
        if self.source.birthplace.is_empty() {
            return Err("capsule source birthplace must not be empty".to_string());
        }
        if self.source.tool_version.is_empty() {
            return Err("capsule source tool_version must not be empty".to_string());
        }
        if self.source.exported_at_micros <= 0 {
            return Err("capsule source exported_at_micros must be positive".to_string());
        }

        let mut prior_path: Option<&str> = None;
        let mut entries_by_path = BTreeMap::new();
        for entry in &self.entries {
            validate_path(&entry.path)?;
            if let Some(prior) = prior_path {
                match prior.as_bytes().cmp(entry.path.as_bytes()) {
                    std::cmp::Ordering::Less => {}
                    std::cmp::Ordering::Equal => {
                        return Err(format!("duplicate capsule path {:?}", entry.path));
                    }
                    std::cmp::Ordering::Greater => {
                        return Err("capsule entries are not in canonical path order".to_string());
                    }
                }
            }
            prior_path = Some(&entry.path);
            validate_entry(entry, &self.format)?;
            let _ = entries_by_path.insert(entry.path.as_str(), entry);
        }

        let Some(root) = entries_by_path.get("/") else {
            return Err("capsule manifest has no root entry".to_string());
        };
        if root.entry_type != EntryType::DirectoryPhysical
            || !matches!(root.node, CapsuleNode::Directory)
        {
            return Err("capsule root entry must be a physical directory".to_string());
        }

        for entry in self.entries.iter().filter(|entry| entry.path != "/") {
            let parent = parent_path(&entry.path);
            let Some(parent_entry) = entries_by_path.get(parent.as_str()) else {
                return Err(format!(
                    "capsule path {:?} has missing parent {:?}",
                    entry.path, parent
                ));
            };
            if !matches!(parent_entry.node, CapsuleNode::Directory) {
                return Err(format!(
                    "capsule path {:?} has non-directory parent {:?}",
                    entry.path, parent
                ));
            }
        }
        Ok(())
    }

    /// Distinct payload objects declared by this snapshot, sorted by hash.
    ///
    /// # Errors
    ///
    /// Returns an error if one hash is declared with inconsistent sizes.
    pub fn payload_objects(&self) -> Result<Vec<CapsuleObject>, String> {
        let mut objects = BTreeMap::new();
        for entry in &self.entries {
            for object in entry_objects(&entry.node) {
                if let Some(size) = objects.insert(object.hash, object.size)
                    && size != object.size
                {
                    return Err(format!(
                        "capsule object {} has conflicting sizes {size} and {}",
                        object.hash, object.size
                    ));
                }
            }
        }
        Ok(objects
            .into_iter()
            .map(|(hash, size)| CapsuleObject { hash, size })
            .collect())
    }
}

/// Encode a validated manifest as canonical JSON bytes.
///
/// # Errors
///
/// Returns an error if validation or serialization fails.
pub fn capsule_manifest_bytes(manifest: &CapsuleManifest) -> Result<Vec<u8>, String> {
    manifest.validate()?;
    if !is_schema_evolved_format(&manifest.format) {
        return serde_json::to_vec(manifest)
            .map_err(|error| format!("encode capsule manifest: {error}"));
    }
    let mut value = serde_json::to_value(manifest)
        .map_err(|error| format!("encode capsule manifest: {error}"))?;
    let entries = value
        .get_mut("entries")
        .and_then(serde_json::Value::as_array_mut)
        .expect("serialized capsule manifest contains entries");
    for entry in entries {
        let Some(node) = entry
            .get_mut("node")
            .and_then(serde_json::Value::as_object_mut)
        else {
            continue;
        };
        if node.get("kind").and_then(serde_json::Value::as_str) == Some("physical")
            && node.get("schema_fingerprint") == Some(&serde_json::Value::Null)
        {
            let _ = node.remove("schema_fingerprint");
        }
    }
    serde_json::to_vec(&value).map_err(|error| format!("encode v3 capsule manifest: {error}"))
}

/// Compute the domain-separated BLAKE3 root of a canonical manifest.
///
/// # Errors
///
/// Returns an error if the manifest is invalid.
pub fn capsule_root(manifest: &CapsuleManifest) -> Result<ObjectHash, String> {
    let bytes = capsule_manifest_bytes(manifest)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(match manifest.format.as_str() {
        CAPSULE_FORMAT_V1 => CAPSULE_ROOT_DOMAIN,
        CAPSULE_FORMAT_V2 => CAPSULE_ROOT_DOMAIN_V2,
        CAPSULE_FORMAT_V3 => CAPSULE_ROOT_DOMAIN_V3,
        CAPSULE_FORMAT_V4 => CAPSULE_ROOT_DOMAIN_V4,
        _ => unreachable!("manifest validation checks the capsule format"),
    });
    hasher.update(&bytes);
    Ok(ObjectHash::from_bytes(*hasher.finalize().as_bytes()))
}

/// Compute one physical node's logical series root.
///
/// The root commits to payload kind, table schema identity, and every ordered
/// logical leaf descriptor. Physical payload objects do not contribute.
#[must_use]
pub fn capsule_series_root(
    payload_kind: CapsulePayloadKind,
    schema_fingerprint: Option<ObjectHash>,
    leaves: &[CapsuleLeaf],
) -> ObjectHash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CAPSULE_SERIES_DOMAIN);
    hasher.update(&[match payload_kind {
        CapsulePayloadKind::File => 0,
        CapsulePayloadKind::Table => 1,
    }]);
    match schema_fingerprint {
        Some(hash) => {
            hasher.update(&[1]);
            hasher.update(hash.as_bytes());
        }

        None => {
            hasher.update(&[0]);
        }
    }
    hasher.update(
        &u64::try_from(leaves.len())
            .expect("capsule leaf count exceeds u64::MAX")
            .to_le_bytes(),
    );
    for leaf in leaves {
        hasher.update(leaf.logical_hash.as_bytes());
        hasher.update(&leaf.logical_count.to_le_bytes());
        let mut flags = 0u8;
        if leaf.min_event_time.is_some() {
            flags |= 0x01;
        }
        if leaf.max_event_time.is_some() {
            flags |= 0x02;
        }
        if leaf.logical_attributes.is_some() {
            flags |= 0x04;
        }
        hasher.update(&[flags]);
        if let Some(minimum) = leaf.min_event_time {
            hasher.update(&minimum.to_le_bytes());
        }
        if let Some(maximum) = leaf.max_event_time {
            hasher.update(&maximum.to_le_bytes());
        }
        if let Some(attributes) = &leaf.logical_attributes {
            hasher.update(
                &u64::try_from(attributes.len())
                    .expect("capsule logical attributes exceed u64::MAX")
                    .to_le_bytes(),
            );
            hasher.update(attributes.as_bytes());
        }
    }
    ObjectHash::from_bytes(*hasher.finalize().as_bytes())
}

/// Compute a v3 physical node's logical series root.
///
/// Unlike v1/v2, every table leaf's schema fingerprint is framed into the
/// root. The distinct domain prevents a v2 root from being reinterpreted as a
/// v3 root.
#[must_use]
pub fn capsule_series_root_v3(
    payload_kind: CapsulePayloadKind,
    schema_fingerprint: Option<ObjectHash>,
    leaves: &[CapsuleLeaf],
) -> ObjectHash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CAPSULE_SERIES_DOMAIN_V3);
    hasher.update(&[match payload_kind {
        CapsulePayloadKind::File => 0,
        CapsulePayloadKind::Table => 1,
    }]);
    match schema_fingerprint {
        Some(hash) => {
            hasher.update(&[1]);
            hasher.update(hash.as_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
    hasher.update(
        &u64::try_from(leaves.len())
            .expect("capsule leaf count exceeds u64::MAX")
            .to_le_bytes(),
    );
    for leaf in leaves {
        hasher.update(leaf.logical_hash.as_bytes());
        hasher.update(&leaf.logical_count.to_le_bytes());
        let mut flags = 0u8;
        if leaf.min_event_time.is_some() {
            flags |= 0x01;
        }
        if leaf.max_event_time.is_some() {
            flags |= 0x02;
        }
        if leaf.logical_attributes.is_some() {
            flags |= 0x04;
        }
        if leaf.schema_fingerprint.is_some() {
            flags |= 0x08;
        }
        hasher.update(&[flags]);
        if let Some(minimum) = leaf.min_event_time {
            hasher.update(&minimum.to_le_bytes());
        }
        if let Some(maximum) = leaf.max_event_time {
            hasher.update(&maximum.to_le_bytes());
        }
        if let Some(attributes) = &leaf.logical_attributes {
            hasher.update(
                &u64::try_from(attributes.len())
                    .expect("capsule logical attributes exceed u64::MAX")
                    .to_le_bytes(),
            );
            hasher.update(attributes.as_bytes());
        }
        if let Some(schema) = leaf.schema_fingerprint {
            hasher.update(schema.as_bytes());
        }
    }
    ObjectHash::from_bytes(*hasher.finalize().as_bytes())
}

/// Compute the logical-series-v2 leaf identity used by a capsule.
///
/// `canonical_payload` is the exact byte range for a file leaf or canonical
/// row bytes for a table leaf. `logical_attributes`, when present, must already
/// be encoded by [`encode_capsule_attributes`].
///
/// # Errors
///
/// Returns an error for an empty leaf, a payload/schema mismatch, invalid
/// bounds, or noncanonical logical attributes.
#[allow(clippy::too_many_arguments)]
pub fn capsule_leaf_hash(
    payload_kind: CapsulePayloadKind,
    schema_fingerprint: Option<ObjectHash>,
    logical_count: u64,
    canonical_payload: &[u8],
    min_event_time: Option<i64>,
    max_event_time: Option<i64>,
    logical_attributes: Option<&str>,
) -> Result<ObjectHash, String> {
    if logical_count == 0 || canonical_payload.is_empty() {
        return Err("a capsule logical leaf must not be empty".to_string());
    }
    if let Some((minimum, maximum)) = min_event_time.zip(max_event_time)
        && minimum > maximum
    {
        return Err("logical leaf minimum event time exceeds maximum".to_string());
    }
    let schema_bytes = schema_fingerprint.map(|hash| *hash.as_bytes());
    let (kind, schema) = match (payload_kind, schema_bytes.as_ref()) {
        (CapsulePayloadKind::File, None) => (1u8, &[][..]),
        (CapsulePayloadKind::Table, Some(schema)) => (0u8, &schema[..]),
        (CapsulePayloadKind::File, Some(_)) => {
            return Err("file logical leaf must not declare a schema".to_string());
        }
        (CapsulePayloadKind::Table, None) => {
            return Err("table logical leaf must declare a schema".to_string());
        }
    };
    let attributes = match logical_attributes {
        Some(attributes) => {
            let canonical = encode_capsule_attributes(attributes)?;
            if canonical.as_slice() != attributes.as_bytes() {
                return Err("logical attributes are not canonical JSON".to_string());
            }
            canonical
        }
        None => Vec::new(),
    };

    let mut preimage = Vec::new();
    preimage.extend_from_slice(LOGICAL_LEAF_DOMAIN);
    preimage.push(kind);
    push_len_prefixed_u32(&mut preimage, schema)?;
    preimage.extend_from_slice(&logical_count.to_le_bytes());
    push_len_prefixed_u64(&mut preimage, canonical_payload)?;
    let mut flags = 0u8;
    if min_event_time.is_some() {
        flags |= 0x01;
    }
    if max_event_time.is_some() {
        flags |= 0x02;
    }
    preimage.push(flags);
    if let Some(minimum) = min_event_time {
        preimage.extend_from_slice(&minimum.to_le_bytes());
    }
    if let Some(maximum) = max_event_time {
        preimage.extend_from_slice(&maximum.to_le_bytes());
    }
    push_len_prefixed_u32(&mut preimage, &attributes)?;
    Ok(ObjectHash::of_bytes(&preimage))
}

/// Decode and validate canonical v1 manifest bytes.
///
/// Noncanonical JSON encodings are rejected even when they deserialize to the
/// same values, so a manifest root always names exactly one byte encoding.
///
/// # Errors
///
/// Returns an error for malformed JSON, an unknown field, a violated manifest
/// invariant, or a noncanonical encoding.
pub fn decode_capsule_manifest(bytes: &[u8]) -> Result<CapsuleManifest, String> {
    let manifest: CapsuleManifest = serde_json::from_slice(bytes)
        .map_err(|error| format!("decode capsule manifest: {error}"))?;
    manifest.validate()?;
    let canonical = capsule_manifest_bytes(&manifest)?;
    if canonical != bytes {
        return Err("capsule manifest is not canonically encoded".to_string());
    }
    Ok(manifest)
}

/// Verify a capsule downloaded into its documented `recovery/` layout.
///
/// This validates the latest ref and canonical manifest, every declared
/// physical object, every decoded Parquet schema, every logical leaf hash, and
/// every series root before returning a summary.
///
/// # Errors
///
/// Returns an error for any missing, malformed, unsupported, or mismatched
/// component. No pond data is written.
pub fn verify_capsule_directory(path: &Path) -> Result<CapsuleVerifyReport, String> {
    let (manifest, root) = read_capsule_manifest(path)?;
    verify_capsule_payload_directory_at_root(
        &manifest,
        &path.join("recovery").join("objects"),
        root,
    )
}

/// Read and validate the canonical manifest named by a downloaded capsule
/// directory's `recovery/refs/latest`, without touching any payload object.
///
/// This is the layout-parsing half of [`verify_capsule_directory`], factored
/// out so a caller that needs the decoded [`CapsuleManifest`] itself -- for
/// example a staged importer that must walk every entry -- does not have to
/// re-derive the `refs/latest` -> `manifests/<hash>.json` lookup or re-check
/// the root hash.
///
/// # Errors
///
/// Returns an error if `refs/latest` or the named manifest is missing or
/// malformed, or if the manifest does not hash to the root it names.
pub fn read_capsule_manifest(path: &Path) -> Result<(CapsuleManifest, ObjectHash), String> {
    let recovery = path.join("recovery");
    let reference = std::fs::read_to_string(recovery.join("refs/latest"))
        .map_err(|error| format!("read recovery/refs/latest: {error}"))?;
    let root_text = reference.trim_end();
    if root_text.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return Err("capsule latest ref must use lowercase hexadecimal".to_string());
    }
    let root = ObjectHash::from_hex(root_text)?;
    let manifest_bytes = std::fs::read(
        recovery
            .join("manifests")
            .join(format!("{}.json", root.to_hex())),
    )
    .map_err(|error| format!("read capsule manifest {root}: {error}"))?;
    let manifest = decode_capsule_manifest(&manifest_bytes)?;
    let computed_root = capsule_root(&manifest)?;
    if computed_root != root {
        return Err(format!(
            "capsule manifest hashes to {computed_root}, latest ref names {root}"
        ));
    }
    Ok((manifest, root))
}

/// Deeply verify a candidate manifest against its in-memory payload closure.
///
/// This is the publication-side counterpart to [`verify_capsule_directory`].
/// It prevents a logically invalid generation from becoming current even when
/// every supplied object independently has the expected content hash.
pub fn verify_capsule_payloads(
    manifest: &CapsuleManifest,
    payloads: &BTreeMap<ObjectHash, Vec<u8>>,
) -> Result<CapsuleVerifyReport, String> {
    let root = capsule_root(manifest)?;
    let declared = manifest.payload_objects()?;
    if declared.len() != payloads.len() {
        return Err(format!(
            "capsule declares {} payloads but verifier received {}",
            declared.len(),
            payloads.len()
        ));
    }
    verify_capsule_with(root, manifest, |hash| {
        payloads
            .get(&hash)
            .cloned()
            .ok_or_else(|| format!("capsule payload {hash} was not supplied"))
    })
}

/// Deeply verify a candidate manifest against payload files in `objects_dir`.
///
/// Each file must be named `blake3=<hash>`. Payloads are loaded and released
/// one object at a time, so verification does not retain the complete capsule
/// closure in memory.
pub fn verify_capsule_payload_directory(
    manifest: &CapsuleManifest,
    objects_dir: &Path,
) -> Result<CapsuleVerifyReport, String> {
    let root = capsule_root(manifest)?;
    verify_capsule_payload_directory_at_root(manifest, objects_dir, root)
}

/// Verify a sparse incremental payload directory against a prior manifest.
///
/// Omitted payloads are accepted only when their object and logical leaf
/// descriptors occur unchanged at the same path in `prior`. Every staged
/// physical leaf is deeply rehashed.
pub fn verify_incremental_capsule_payload_directory(
    manifest: &CapsuleManifest,
    prior: &CapsuleManifest,
    objects_dir: &Path,
) -> Result<(), String> {
    manifest.validate()?;
    prior.validate()?;
    for entry in &manifest.entries {
        let prior_node = prior
            .entries
            .iter()
            .find(|candidate| candidate.path == entry.path)
            .map(|candidate| &candidate.node);
        if prior_node == Some(&entry.node) {
            continue;
        }
        match &entry.node {
            CapsuleNode::Directory => {}
            CapsuleNode::Symlink { target } | CapsuleNode::Dynamic { recipe: target, .. } => {
                if staged_payload_exists(objects_dir, target.hash)? {
                    verify_payload_file(objects_dir, target)?;
                } else {
                    return Err(format!(
                        "unstaged capsule payload {} changed at path {:?}",
                        target.hash, entry.path
                    ));
                }
            }
            CapsuleNode::Physical {
                payload_kind,
                schema_fingerprint,
                objects,
                leaves,
                ..
            } => {
                if objects.len() != leaves.len() {
                    let mut missing = false;
                    for object in objects {
                        missing |= !staged_payload_exists(objects_dir, object.hash)?;
                    }
                    if missing {
                        return Err(format!(
                            "sparse changed physical path {:?} has unaligned objects and leaves",
                            entry.path
                        ));
                    }
                    for object in objects {
                        verify_payload_file(objects_dir, object)?;
                    }
                    if is_schema_evolved_format(&manifest.format)
                        && *payload_kind == CapsulePayloadKind::Table
                    {
                        verify_table_stream_v3_directory(
                            objects_dir,
                            &entry.path,
                            *schema_fingerprint,
                            objects,
                            leaves,
                        )?;
                    } else {
                        verify_staged_physical(
                            objects_dir,
                            &entry.path,
                            *payload_kind,
                            *schema_fingerprint,
                            objects,
                            leaves,
                            manifest.format == CAPSULE_FORMAT_V1,
                        )?;
                    }
                    continue;
                }

                let prior_pairs = match prior_node {
                    Some(CapsuleNode::Physical {
                        payload_kind: prior_kind,
                        schema_fingerprint: prior_schema,
                        objects: prior_objects,
                        leaves: prior_leaves,
                        ..
                    }) if prior_kind == payload_kind
                        && prior_schema == schema_fingerprint
                        && prior_objects.len() == prior_leaves.len() =>
                    {
                        prior_objects.iter().zip(prior_leaves).collect::<Vec<_>>()
                    }
                    _ => Vec::new(),
                };
                let mut staged_objects = Vec::new();
                let mut staged_leaves = Vec::new();
                for (object, leaf) in objects.iter().zip(leaves) {
                    if staged_payload_exists(objects_dir, object.hash)? {
                        verify_payload_file(objects_dir, object)?;
                        staged_objects.push(object.clone());
                        staged_leaves.push(leaf.clone());
                    } else if !prior_pairs.iter().any(|(prior_object, prior_leaf)| {
                        *prior_object == object && *prior_leaf == leaf
                    }) {
                        return Err(format!(
                            "unstaged capsule leaf changed at path {:?} for payload {}",
                            entry.path, object.hash
                        ));
                    }
                }
                if !staged_objects.is_empty() {
                    if is_schema_evolved_format(&manifest.format)
                        && *payload_kind == CapsulePayloadKind::Table
                    {
                        verify_table_stream_v3_directory(
                            objects_dir,
                            &entry.path,
                            *schema_fingerprint,
                            &staged_objects,
                            &staged_leaves,
                        )?;
                    } else {
                        verify_staged_physical(
                            objects_dir,
                            &entry.path,
                            *payload_kind,
                            *schema_fingerprint,
                            &staged_objects,
                            &staged_leaves,
                            manifest.format == CAPSULE_FORMAT_V1,
                        )?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn staged_payload_exists(objects_dir: &Path, hash: ObjectHash) -> Result<bool, String> {
    objects_dir
        .join(format!("blake3={}", hash.to_hex()))
        .try_exists()
        .map_err(|error| format!("inspect staged capsule payload {hash}: {error}"))
}

fn verify_staged_physical(
    objects_dir: &Path,
    path: &str,
    payload_kind: CapsulePayloadKind,
    schema_fingerprint: Option<ObjectHash>,
    objects: &[CapsuleObject],
    leaves: &[CapsuleLeaf],
    legacy: bool,
) -> Result<(), String> {
    match payload_kind {
        CapsulePayloadKind::File => {
            verify_file_stream_directory(objects_dir, path, objects, leaves, legacy)
        }
        CapsulePayloadKind::Table => verify_table_stream_directory(
            objects_dir,
            path,
            schema_fingerprint
                .ok_or_else(|| format!("table {path:?} has no schema fingerprint"))?,
            objects,
            leaves,
            legacy,
        ),
    }
}

fn verify_capsule_payload_directory_at_root(
    manifest: &CapsuleManifest,
    objects_dir: &Path,
    root: ObjectHash,
) -> Result<CapsuleVerifyReport, String> {
    let objects = manifest.payload_objects()?;
    let mut physical_bytes = 0u64;
    for object in &objects {
        verify_payload_file(objects_dir, object)?;
        physical_bytes = physical_bytes
            .checked_add(object.size)
            .ok_or_else(|| "capsule physical byte count exceeds u64::MAX".to_string())?;
    }

    let mut logical_count = 0u64;
    for entry in &manifest.entries {
        if let CapsuleNode::Physical {
            payload_kind,
            schema_fingerprint,
            logical_root,
            objects,
            leaves,
        } = &entry.node
        {
            match payload_kind {
                CapsulePayloadKind::File => {
                    verify_file_stream_directory(
                        objects_dir,
                        &entry.path,
                        objects,
                        leaves,
                        manifest.format == CAPSULE_FORMAT_V1,
                    )?;
                }
                CapsulePayloadKind::Table => {
                    if is_schema_evolved_format(&manifest.format) {
                        verify_table_stream_v3_directory(
                            objects_dir,
                            &entry.path,
                            *schema_fingerprint,
                            objects,
                            leaves,
                        )?;
                    } else {
                        verify_table_stream_directory(
                            objects_dir,
                            &entry.path,
                            schema_fingerprint.ok_or_else(|| {
                                format!("table {} has no schema fingerprint", entry.path)
                            })?,
                            objects,
                            leaves,
                            manifest.format == CAPSULE_FORMAT_V1,
                        )?;
                    }
                }
            }
            let computed = capsule_series_root_for_format(
                &manifest.format,
                *payload_kind,
                *schema_fingerprint,
                leaves,
            );
            if computed != *logical_root {
                return Err(format!(
                    "capsule path {:?} series root mismatch",
                    entry.path
                ));
            }
            for leaf in leaves {
                logical_count = logical_count
                    .checked_add(leaf.logical_count)
                    .ok_or_else(|| "capsule logical count exceeds u64::MAX".to_string())?;
            }
        }
    }

    Ok(CapsuleVerifyReport {
        root,
        entries: manifest.entries.len(),
        payload_objects: objects.len(),
        physical_bytes,
        logical_count,
    })
}

fn verify_payload_file(objects_dir: &Path, object: &CapsuleObject) -> Result<(), String> {
    let mut file =
        std::fs::File::open(objects_dir.join(format!("blake3={}", object.hash.to_hex())))
            .map_err(|error| format!("open staged capsule payload {}: {error}", object.hash))?;
    let mut hasher = blake3::Hasher::new();
    let mut size = 0u64;
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let count = std::io::Read::read(&mut file, &mut buffer)
            .map_err(|error| format!("read staged capsule payload {}: {error}", object.hash))?;
        if count == 0 {
            break;
        }
        let _ = hasher.update(&buffer[..count]);
        size = size
            .checked_add(count as u64)
            .ok_or_else(|| format!("capsule payload {} exceeds u64::MAX", object.hash))?;
    }
    let computed = ObjectHash::from_bytes(*hasher.finalize().as_bytes());
    if computed != object.hash || size != object.size {
        return Err(format!(
            "capsule payload {} has hash {computed} and size {size}, expected size {}",
            object.hash, object.size
        ));
    }
    Ok(())
}

fn verify_file_stream_directory(
    objects_dir: &Path,
    path: &str,
    objects: &[CapsuleObject],
    leaves: &[CapsuleLeaf],
    legacy: bool,
) -> Result<(), String> {
    let mut leaf_index = 0usize;
    let mut hasher = leaves
        .first()
        .map(|leaf| new_file_leaf_hasher(leaf, legacy))
        .transpose()
        .map_err(|error| format!("prepare file {path:?} leaf 0: {error}"))?;
    let mut buffer = vec![0u8; 1024 * 1024];
    for object in objects {
        let mut file =
            std::fs::File::open(objects_dir.join(format!("blake3={}", object.hash.to_hex())))
                .map_err(|error| format!("open file {path:?} object {}: {error}", object.hash))?;
        loop {
            let count = std::io::Read::read(&mut file, &mut buffer)
                .map_err(|error| format!("read file {path:?} object {}: {error}", object.hash))?;
            if count == 0 {
                break;
            }
            let mut offset = 0usize;
            while offset < count {
                let current = hasher.as_mut().ok_or_else(|| {
                    format!("file {path:?} has bytes after its final logical leaf")
                })?;
                let take = usize::try_from(current.remaining())
                    .unwrap_or(usize::MAX)
                    .min(count - offset);
                current
                    .write(&buffer[offset..offset + take])
                    .map_err(|error| format!("hash file {path:?} leaf {leaf_index}: {error}"))?;
                offset += take;
                if current.remaining() == 0 {
                    let computed = hasher
                        .take()
                        .expect("completed file leaf hasher exists")
                        .finish()
                        .map_err(|error| {
                            format!("finish file {path:?} leaf {leaf_index}: {error}")
                        })?;
                    if computed != leaves[leaf_index].logical_hash {
                        return Err(format!(
                            "file {path:?} leaf {leaf_index} hashes to {computed}, manifest names {}",
                            leaves[leaf_index].logical_hash
                        ));
                    }
                    leaf_index += 1;
                    hasher = leaves
                        .get(leaf_index)
                        .map(|leaf| new_file_leaf_hasher(leaf, legacy))
                        .transpose()
                        .map_err(|error| {
                            format!("prepare file {path:?} leaf {leaf_index}: {error}")
                        })?;
                }
            }
        }
    }
    if leaf_index != leaves.len() {
        return Err(format!(
            "file {path:?} ended after {leaf_index} of {} logical leaves",
            leaves.len()
        ));
    }
    Ok(())
}

fn new_file_leaf_hasher(
    leaf: &CapsuleLeaf,
    legacy: bool,
) -> Result<super::series_leaf::IncrementalFileLeafHasher, String> {
    super::series_leaf::IncrementalFileLeafHasher::new_with_magic(
        if legacy {
            super::series_leaf::legacy_leaf_magic()
        } else {
            b"watertown.series-leaf.v1\n"
        },
        leaf.logical_count,
        leaf.min_event_time,
        leaf.max_event_time,
        leaf.logical_attributes.as_deref().map(str::as_bytes),
    )
}

fn verify_table_stream_v3_directory(
    objects_dir: &Path,
    path: &str,
    node_schema: Option<ObjectHash>,
    objects: &[CapsuleObject],
    leaves: &[CapsuleLeaf],
) -> Result<(), String> {
    let (schemas, canonical_lengths) =
        scan_table_stream_v3_directory(objects_dir, path, node_schema, objects, leaves)?;
    if leaves.is_empty() {
        return Ok(());
    }

    let mut leaf_index = 0usize;
    let mut leaf_rows = 0u64;
    let mut hasher: Option<super::series_leaf::IncrementalTableLeafHasher> = None;
    for object in objects {
        let file =
            std::fs::File::open(objects_dir.join(format!("blake3={}", object.hash.to_hex())))
                .map_err(|error| {
                    format!("reopen table {path:?} object {}: {error}", object.hash)
                })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|error| format!("reopen table {path:?} object {}: {error}", object.hash))?;
        let object_schema = super::series_leaf::canonicalize_schema(builder.schema().as_ref())
            .map_err(|error| format!("canonicalize table {path:?} schema: {error}"))?;
        for batch in builder
            .build()
            .map_err(|error| format!("rebuild table {path:?} reader: {error}"))?
        {
            let batch = batch.map_err(|error| format!("reread table {path:?} rows: {error}"))?;
            let mut offset = 0usize;
            while offset < batch.num_rows() {
                let leaf = leaves.get(leaf_index).ok_or_else(|| {
                    format!("table {path:?} has rows after its final logical leaf")
                })?;
                if hasher.is_none() {
                    hasher = Some(new_table_leaf_hasher(
                        schemas[leaf_index].as_ref(),
                        leaf,
                        canonical_lengths[leaf_index],
                        false,
                    )?);
                }
                let remaining = leaf
                    .logical_count
                    .checked_sub(leaf_rows)
                    .ok_or_else(|| format!("table {path:?} leaf {leaf_index} row underflow"))?;
                let take = usize::try_from(remaining)
                    .unwrap_or(usize::MAX)
                    .min(batch.num_rows() - offset);
                hasher
                    .as_mut()
                    .expect("active v3 table leaf hasher")
                    .write_batch(&batch.slice(offset, take))
                    .map_err(|error| format!("hash table {path:?} leaf {leaf_index}: {error}"))?;
                leaf_rows += take as u64;
                offset += take;
                if leaf_rows == leaf.logical_count {
                    let computed = hasher
                        .take()
                        .expect("completed v3 table leaf hasher")
                        .finish()
                        .map_err(|error| {
                            format!("finish table {path:?} leaf {leaf_index}: {error}")
                        })?;
                    if computed != leaf.logical_hash {
                        return Err(format!(
                            "table {path:?} leaf {leaf_index} hashes to {computed}, manifest names {}",
                            leaf.logical_hash
                        ));
                    }
                    leaf_index += 1;
                    leaf_rows = 0;
                }
            }
        }
        let _ = object_schema;
    }
    if leaf_index != leaves.len() || leaf_rows != 0 {
        return Err(format!(
            "table {path:?} ended after {leaf_index} of {} logical leaves",
            leaves.len()
        ));
    }
    Ok(())
}

fn scan_table_stream_v3_directory(
    objects_dir: &Path,
    path: &str,
    node_schema: Option<ObjectHash>,
    objects: &[CapsuleObject],
    leaves: &[CapsuleLeaf],
) -> Result<(Vec<std::sync::Arc<arrow_schema::Schema>>, Vec<u64>), String> {
    let mut schemas = Vec::with_capacity(leaves.len());
    let mut canonical_lengths = vec![0u64; leaves.len()];
    let mut leaf_index = 0usize;
    let mut leaf_rows = 0u64;
    for object in objects {
        let file =
            std::fs::File::open(objects_dir.join(format!("blake3={}", object.hash.to_hex())))
                .map_err(|error| format!("open table {path:?} object {}: {error}", object.hash))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|error| format!("open table {path:?} object {}: {error}", object.hash))?;
        let object_schema = super::series_leaf::canonicalize_schema(builder.schema().as_ref())
            .map_err(|error| format!("canonicalize table {path:?} schema: {error}"))?;
        let object_fingerprint = super::series_leaf::schema_fingerprint(&object_schema)
            .map_err(|error| format!("fingerprint table {path:?}: {error}"))?;
        if leaves.is_empty() {
            let expected = node_schema
                .ok_or_else(|| format!("empty v3 table {path:?} has no node schema fingerprint"))?;
            if object_fingerprint != expected {
                return Err(format!(
                    "empty v3 table {path:?} object {} schema hashes to {object_fingerprint}, \
                     expected node schema {expected}",
                    object.hash
                ));
            }
        } else {
            let leaf = leaves.get(leaf_index).ok_or_else(|| {
                format!("table {path:?} has physical objects after its final logical leaf")
            })?;
            let expected = leaf.schema_fingerprint.ok_or_else(|| {
                format!("table {path:?} leaf {leaf_index} has no v3 schema fingerprint")
            })?;
            if object_fingerprint != expected {
                return Err(format!(
                    "table {path:?} object {} schema hashes to {object_fingerprint}, \
                     but leaf {leaf_index} requires {expected}; schema transitions must \
                     occur at physical object boundaries",
                    object.hash
                ));
            }
        }
        let mut object_rows = 0u64;
        for batch in builder
            .build()
            .map_err(|error| format!("build table {path:?} reader: {error}"))?
        {
            let batch = batch.map_err(|error| format!("read table {path:?} rows: {error}"))?;
            object_rows = object_rows
                .checked_add(batch.num_rows() as u64)
                .ok_or_else(|| {
                    format!(
                        "table {path:?} object {} row count exceeds u64::MAX",
                        object.hash
                    )
                })?;
            let mut offset = 0usize;
            while offset < batch.num_rows() {
                let leaf = leaves.get(leaf_index).ok_or_else(|| {
                    format!("table {path:?} has rows after its final logical leaf")
                })?;
                let expected = leaf.schema_fingerprint.ok_or_else(|| {
                    format!("table {path:?} leaf {leaf_index} has no v3 schema fingerprint")
                })?;
                if object_fingerprint != expected {
                    return Err(format!(
                        "table {path:?} object {} schema hashes to {object_fingerprint}, \
                         but leaf {leaf_index} requires {expected}; schema transitions must \
                         occur at physical object boundaries",
                        object.hash
                    ));
                }
                if schemas.len() == leaf_index {
                    schemas.push(object_schema.clone());
                } else if schemas[leaf_index].as_ref() != object_schema.as_ref() {
                    return Err(format!(
                        "table {path:?} leaf {leaf_index} has noncanonical physical schemas"
                    ));
                }
                let remaining = leaf
                    .logical_count
                    .checked_sub(leaf_rows)
                    .ok_or_else(|| format!("table {path:?} leaf {leaf_index} row underflow"))?;
                let take = usize::try_from(remaining)
                    .unwrap_or(usize::MAX)
                    .min(batch.num_rows() - offset);
                let bytes = super::series_leaf::encode_canonical_batch_rows(
                    object_schema.as_ref(),
                    &batch.slice(offset, take),
                )
                .map_err(|error| {
                    format!("encode table {path:?} leaf {leaf_index} rows: {error}")
                })?;
                canonical_lengths[leaf_index] = canonical_lengths[leaf_index]
                    .checked_add(u64::try_from(bytes.len()).map_err(|_| {
                        format!("table {path:?} leaf {leaf_index} bytes exceed u64::MAX")
                    })?)
                    .ok_or_else(|| {
                        format!("table {path:?} leaf {leaf_index} bytes exceed u64::MAX")
                    })?;
                leaf_rows += take as u64;
                offset += take;
                if leaf_rows == leaf.logical_count {
                    leaf_index += 1;
                    leaf_rows = 0;
                }
            }
        }
        if !leaves.is_empty() && object_rows == 0 {
            return Err(format!(
                "table {path:?} object {} has no rows for a logical leaf",
                object.hash
            ));
        }
    }
    if leaf_index != leaves.len() || leaf_rows != 0 {
        return Err(format!(
            "table {path:?} ended after {leaf_index} of {} logical leaves",
            leaves.len()
        ));
    }
    if schemas.len() != leaves.len() {
        return Err(format!(
            "table {path:?} has no rows for one or more declared logical leaves"
        ));
    }
    Ok((schemas, canonical_lengths))
}

fn verify_table_stream_directory(
    objects_dir: &Path,
    path: &str,
    expected_schema: ObjectHash,
    objects: &[CapsuleObject],
    leaves: &[CapsuleLeaf],
    legacy: bool,
) -> Result<(), String> {
    let mut schema: Option<std::sync::Arc<arrow_schema::Schema>> = None;
    let mut canonical_lengths = vec![0u64; leaves.len()];
    let mut leaf_index = 0usize;
    let mut leaf_rows = 0u64;

    for object in objects {
        let file =
            std::fs::File::open(objects_dir.join(format!("blake3={}", object.hash.to_hex())))
                .map_err(|error| format!("open table {path:?} object {}: {error}", object.hash))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|error| format!("open table {path:?} object {}: {error}", object.hash))?;
        let canonical_schema =
            super::series_leaf::canonicalize_schema(builder.schema().as_ref())
                .map_err(|error| format!("canonicalize table {path:?} schema: {error}"))?;
        let fingerprint = if legacy {
            super::series_leaf::schema_fingerprint_with_magic(
                &canonical_schema,
                super::series_leaf::legacy_schema_magic(),
            )
        } else {
            super::series_leaf::schema_fingerprint(&canonical_schema)
        }
        .map_err(|error| format!("fingerprint table {path:?}: {error}"))?;
        if fingerprint != expected_schema {
            return Err(format!(
                "table {path:?} object {} schema hashes to {fingerprint}, expected {expected_schema}",
                object.hash
            ));
        }
        if let Some(prior) = &schema {
            if prior.as_ref() != canonical_schema.as_ref() {
                return Err(format!(
                    "table {path:?} has logically inconsistent Arrow schemas"
                ));
            }
        } else {
            schema = Some(canonical_schema);
        }
        for batch in builder
            .build()
            .map_err(|error| format!("build table {path:?} reader: {error}"))?
        {
            let batch = batch.map_err(|error| format!("read table {path:?} rows: {error}"))?;
            let mut offset = 0usize;
            while offset < batch.num_rows() {
                let leaf = leaves.get(leaf_index).ok_or_else(|| {
                    format!("table {path:?} has rows after its final logical leaf")
                })?;
                let remaining = leaf
                    .logical_count
                    .checked_sub(leaf_rows)
                    .ok_or_else(|| format!("table {path:?} leaf {leaf_index} row underflow"))?;
                let take = usize::try_from(remaining)
                    .unwrap_or(usize::MAX)
                    .min(batch.num_rows() - offset);
                let slice = batch.slice(offset, take);
                let bytes = super::series_leaf::encode_canonical_batch_rows(
                    schema.as_ref().expect("table schema established"),
                    &slice,
                )
                .map_err(|error| {
                    format!("encode table {path:?} leaf {leaf_index} rows: {error}")
                })?;
                canonical_lengths[leaf_index] = canonical_lengths[leaf_index]
                    .checked_add(u64::try_from(bytes.len()).map_err(|_| {
                        format!("table {path:?} leaf {leaf_index} bytes exceed u64::MAX")
                    })?)
                    .ok_or_else(|| {
                        format!("table {path:?} leaf {leaf_index} bytes exceed u64::MAX")
                    })?;
                leaf_rows += take as u64;
                offset += take;
                if leaf_rows == leaf.logical_count {
                    leaf_index += 1;
                    leaf_rows = 0;
                }
            }
        }
    }
    let schema = schema.ok_or_else(|| format!("table {path:?} has no Parquet schema carrier"))?;
    if leaf_index != leaves.len() || leaf_rows != 0 {
        return Err(format!(
            "table {path:?} ended after {leaf_index} of {} logical leaves",
            leaves.len()
        ));
    }
    if leaves.is_empty() {
        return Ok(());
    }

    let mut leaf_index = 0usize;
    let mut leaf_rows = 0u64;
    let mut hasher = Some(new_table_leaf_hasher(
        &schema,
        &leaves[0],
        canonical_lengths[0],
        legacy,
    )?);
    for object in objects {
        let file =
            std::fs::File::open(objects_dir.join(format!("blake3={}", object.hash.to_hex())))
                .map_err(|error| {
                    format!("reopen table {path:?} object {}: {error}", object.hash)
                })?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|error| format!("reopen table {path:?} object {}: {error}", object.hash))?
            .build()
            .map_err(|error| format!("rebuild table {path:?} reader: {error}"))?;
        for batch in reader {
            let batch = batch.map_err(|error| format!("reread table {path:?} rows: {error}"))?;
            let mut offset = 0usize;
            while offset < batch.num_rows() {
                let leaf = &leaves[leaf_index];
                let remaining = leaf.logical_count - leaf_rows;
                let take = usize::try_from(remaining)
                    .unwrap_or(usize::MAX)
                    .min(batch.num_rows() - offset);
                let slice = batch.slice(offset, take);
                hasher
                    .as_mut()
                    .expect("active table leaf hasher")
                    .write_batch(&slice)
                    .map_err(|error| format!("hash table {path:?} leaf {leaf_index}: {error}"))?;
                leaf_rows += take as u64;
                offset += take;
                if leaf_rows == leaf.logical_count {
                    let computed = hasher
                        .take()
                        .expect("completed table leaf hasher")
                        .finish()
                        .map_err(|error| {
                            format!("finish table {path:?} leaf {leaf_index}: {error}")
                        })?;
                    if computed != leaf.logical_hash {
                        return Err(format!(
                            "table {path:?} leaf {leaf_index} hashes to {computed}, manifest names {}",
                            leaf.logical_hash
                        ));
                    }
                    leaf_index += 1;
                    leaf_rows = 0;
                    hasher = leaves
                        .get(leaf_index)
                        .map(|leaf| {
                            new_table_leaf_hasher(
                                &schema,
                                leaf,
                                canonical_lengths[leaf_index],
                                legacy,
                            )
                        })
                        .transpose()?;
                }
            }
        }
    }
    if leaf_index != leaves.len() {
        return Err(format!(
            "table {path:?} ended after {leaf_index} of {} logical leaves",
            leaves.len()
        ));
    }
    Ok(())
}

fn new_table_leaf_hasher(
    schema: &arrow_schema::Schema,
    leaf: &CapsuleLeaf,
    canonical_rows_len: u64,
    legacy: bool,
) -> Result<super::series_leaf::IncrementalTableLeafHasher, String> {
    if legacy {
        super::series_leaf::IncrementalTableLeafHasher::new_with_magic_and_rows_domain(
            super::series_leaf::legacy_leaf_magic(),
            schema,
            leaf.logical_count,
            canonical_rows_len,
            leaf.min_event_time,
            leaf.max_event_time,
            leaf.logical_attributes.as_deref().map(str::as_bytes),
            super::series_leaf::legacy_rows_magic(),
            super::series_leaf::legacy_schema_magic(),
        )
    } else {
        super::series_leaf::IncrementalTableLeafHasher::new(
            schema,
            leaf.logical_count,
            canonical_rows_len,
            leaf.min_event_time,
            leaf.max_event_time,
            leaf.logical_attributes.as_deref().map(str::as_bytes),
        )
    }
}

fn verify_capsule_with<F>(
    root: ObjectHash,
    manifest: &CapsuleManifest,
    mut read_payload: F,
) -> Result<CapsuleVerifyReport, String>
where
    F: FnMut(ObjectHash) -> Result<Vec<u8>, String>,
{
    let objects = manifest.payload_objects()?;
    let mut physical_bytes = 0u64;
    for object in &objects {
        let bytes = read_payload(object.hash)?;
        verify_payload(object, &bytes)?;
        physical_bytes = physical_bytes
            .checked_add(object.size)
            .ok_or_else(|| "capsule physical byte count exceeds u64::MAX".to_string())?;
    }

    let mut logical_count = 0u64;
    for entry in &manifest.entries {
        if let CapsuleNode::Physical {
            payload_kind,
            schema_fingerprint,
            logical_root,
            objects,
            leaves,
        } = &entry.node
        {
            match payload_kind {
                CapsulePayloadKind::File => {
                    verify_file_stream(&mut read_payload, &entry.path, objects, leaves)?;
                }
                CapsulePayloadKind::Table => {
                    if is_schema_evolved_format(&manifest.format) {
                        verify_table_stream_v3(
                            &mut read_payload,
                            &entry.path,
                            *schema_fingerprint,
                            objects,
                            leaves,
                        )?;
                    } else {
                        verify_table_stream(
                            &mut read_payload,
                            &entry.path,
                            schema_fingerprint.ok_or_else(|| {
                                format!("table {} has no schema fingerprint", entry.path)
                            })?,
                            objects,
                            leaves,
                        )?;
                    }
                }
            }
            let computed = capsule_series_root_for_format(
                &manifest.format,
                *payload_kind,
                *schema_fingerprint,
                leaves,
            );
            if computed != *logical_root {
                return Err(format!(
                    "capsule path {:?} series root mismatch",
                    entry.path
                ));
            }
            for leaf in leaves {
                logical_count = logical_count
                    .checked_add(leaf.logical_count)
                    .ok_or_else(|| "capsule logical count exceeds u64::MAX".to_string())?;
            }
        }
    }

    Ok(CapsuleVerifyReport {
        root,
        entries: manifest.entries.len(),
        payload_objects: objects.len(),
        physical_bytes,
        logical_count,
    })
}

fn verify_file_stream(
    read_payload: &mut impl FnMut(ObjectHash) -> Result<Vec<u8>, String>,
    path: &str,
    objects: &[CapsuleObject],
    leaves: &[CapsuleLeaf],
) -> Result<(), String> {
    let mut stream = Vec::new();
    for object in objects {
        let bytes = read_payload(object.hash)?;
        verify_payload(object, &bytes)?;
        stream.extend_from_slice(&bytes);
    }
    let declared = leaves.iter().try_fold(0u64, |total, leaf| {
        total
            .checked_add(leaf.logical_count)
            .ok_or_else(|| format!("file {path:?} logical count exceeds u64::MAX"))
    })?;
    if u64::try_from(stream.len()).ok() != Some(declared) {
        return Err(format!(
            "file {path:?} stream has {} bytes, leaves declare {declared}",
            stream.len()
        ));
    }
    let mut offset = 0usize;
    for (index, leaf) in leaves.iter().enumerate() {
        let count = usize::try_from(leaf.logical_count)
            .map_err(|_| format!("file {path:?} leaf {index} is too large for this platform"))?;
        let end = offset
            .checked_add(count)
            .ok_or_else(|| format!("file {path:?} leaf {index} offset overflow"))?;
        let computed = super::series_leaf::file_leaf_hash(
            &stream[offset..end],
            leaf.min_event_time,
            leaf.max_event_time,
            leaf.logical_attributes.as_deref(),
        )
        .map_err(|error| format!("hash file {path:?} leaf {index}: {error}"))?;
        if computed != leaf.logical_hash {
            return Err(format!(
                "file {path:?} leaf {index} hashes to {computed}, manifest names {}",
                leaf.logical_hash
            ));
        }
        offset = end;
    }
    Ok(())
}

fn verify_table_stream(
    read_payload: &mut impl FnMut(ObjectHash) -> Result<Vec<u8>, String>,
    path: &str,
    expected_schema: ObjectHash,
    objects: &[CapsuleObject],
    leaves: &[CapsuleLeaf],
) -> Result<(), String> {
    let mut schema: Option<std::sync::Arc<arrow_schema::Schema>> = None;
    let mut batches = VecDeque::new();
    let mut total_rows = 0u64;
    for object in objects {
        let bytes = read_payload(object.hash)?;
        verify_payload(object, &bytes)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
            .map_err(|error| format!("open table {path:?} object {}: {error}", object.hash))?;
        let object_schema = builder.schema().as_ref().clone();
        let canonical_schema = super::series_leaf::canonicalize_schema(&object_schema)
            .map_err(|error| format!("canonicalize table {path:?} schema: {error}"))?;
        let fingerprint = super::series_leaf::schema_fingerprint(&canonical_schema)
            .map_err(|error| format!("fingerprint table {path:?}: {error}"))?;
        if fingerprint != expected_schema {
            return Err(format!(
                "table {path:?} object {} schema hashes to {fingerprint}, expected {expected_schema}",
                object.hash
            ));
        }
        if let Some(prior) = &schema {
            if prior.as_ref() != canonical_schema.as_ref() {
                return Err(format!(
                    "table {path:?} has logically inconsistent Arrow schemas"
                ));
            }
        } else {
            schema = Some(canonical_schema);
        }
        for batch in builder
            .build()
            .map_err(|error| format!("build table {path:?} reader: {error}"))?
        {
            let batch = batch.map_err(|error| format!("read table {path:?} rows: {error}"))?;
            total_rows = total_rows
                .checked_add(batch.num_rows() as u64)
                .ok_or_else(|| format!("table {path:?} row count exceeds u64::MAX"))?;
            if batch.num_rows() > 0 {
                batches.push_back(batch);
            }
        }
    }
    let schema = schema.ok_or_else(|| format!("table {path:?} has no Parquet schema carrier"))?;
    let declared = leaves.iter().try_fold(0u64, |total, leaf| {
        total
            .checked_add(leaf.logical_count)
            .ok_or_else(|| format!("table {path:?} logical count exceeds u64::MAX"))
    })?;
    if total_rows != declared {
        return Err(format!(
            "table {path:?} stream has {total_rows} rows, leaves declare {declared}"
        ));
    }

    for (index, leaf) in leaves.iter().enumerate() {
        let mut remaining = usize::try_from(leaf.logical_count)
            .map_err(|_| format!("table {path:?} leaf {index} is too large for this platform"))?;
        let mut leaf_batches = Vec::new();
        while remaining > 0 {
            let batch = batches
                .pop_front()
                .ok_or_else(|| format!("table {path:?} ended while reconstructing leaf {index}"))?;
            if batch.num_rows() <= remaining {
                remaining -= batch.num_rows();
                leaf_batches.push(batch);
            } else {
                leaf_batches.push(batch.slice(0, remaining));
                batches.push_front(batch.slice(remaining, batch.num_rows() - remaining));
                remaining = 0;
            }
        }
        let computed = super::series_leaf::table_leaf_hash(
            &schema,
            &leaf_batches,
            leaf.min_event_time,
            leaf.max_event_time,
            leaf.logical_attributes.as_deref(),
        )
        .map_err(|error| format!("hash table {path:?} leaf {index}: {error}"))?;
        if computed != leaf.logical_hash {
            return Err(format!(
                "table {path:?} leaf {index} hashes to {computed}, manifest names {}",
                leaf.logical_hash
            ));
        }
    }
    if !batches.is_empty() {
        return Err(format!(
            "table {path:?} has unrepresented rows after its last leaf"
        ));
    }
    Ok(())
}

fn verify_table_stream_v3(
    read_payload: &mut impl FnMut(ObjectHash) -> Result<Vec<u8>, String>,
    path: &str,
    node_schema: Option<ObjectHash>,
    objects: &[CapsuleObject],
    leaves: &[CapsuleLeaf],
) -> Result<(), String> {
    let mut records = Vec::new();
    let mut leaf_index = 0usize;
    let mut leaf_rows = 0u64;
    let mut schemas = Vec::with_capacity(leaves.len());
    let mut canonical_lengths = vec![0u64; leaves.len()];
    for object in objects {
        let bytes = read_payload(object.hash)?;
        verify_payload(object, &bytes)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
            .map_err(|error| format!("open table {path:?} object {}: {error}", object.hash))?;
        let schema = super::series_leaf::canonicalize_schema(builder.schema().as_ref())
            .map_err(|error| format!("canonicalize table {path:?} schema: {error}"))?;
        let fingerprint = super::series_leaf::schema_fingerprint(&schema)
            .map_err(|error| format!("fingerprint table {path:?}: {error}"))?;
        if leaves.is_empty() {
            let expected = node_schema
                .ok_or_else(|| format!("empty v3 table {path:?} has no node schema fingerprint"))?;
            if fingerprint != expected {
                return Err(format!(
                    "empty v3 table {path:?} object {} schema hashes to {fingerprint}, \
                     expected node schema {expected}",
                    object.hash
                ));
            }
        } else {
            let leaf = leaves.get(leaf_index).ok_or_else(|| {
                format!("table {path:?} has physical objects after its final logical leaf")
            })?;
            let expected = leaf.schema_fingerprint.ok_or_else(|| {
                format!("table {path:?} leaf {leaf_index} has no v3 schema fingerprint")
            })?;
            if fingerprint != expected {
                return Err(format!(
                    "table {path:?} object {} schema hashes to {fingerprint}, but leaf \
                     {leaf_index} requires {expected}; schema transitions must occur at \
                     physical object boundaries",
                    object.hash
                ));
            }
        }
        let mut object_rows = 0u64;
        for batch in builder
            .build()
            .map_err(|error| format!("build table {path:?} reader: {error}"))?
        {
            let batch = batch.map_err(|error| format!("read table {path:?} rows: {error}"))?;
            object_rows = object_rows
                .checked_add(batch.num_rows() as u64)
                .ok_or_else(|| {
                    format!(
                        "table {path:?} object {} row count exceeds u64::MAX",
                        object.hash
                    )
                })?;
            let mut offset = 0usize;
            while offset < batch.num_rows() {
                let leaf = leaves.get(leaf_index).ok_or_else(|| {
                    format!("table {path:?} has rows after its final logical leaf")
                })?;
                let expected = leaf.schema_fingerprint.ok_or_else(|| {
                    format!("table {path:?} leaf {leaf_index} has no v3 schema fingerprint")
                })?;
                if fingerprint != expected {
                    return Err(format!(
                        "table {path:?} object {} schema hashes to {fingerprint}, but leaf \
                         {leaf_index} requires {expected}; schema transitions must occur at \
                         physical object boundaries",
                        object.hash
                    ));
                }
                if schemas.len() == leaf_index {
                    schemas.push(schema.clone());
                }
                let remaining = leaf
                    .logical_count
                    .checked_sub(leaf_rows)
                    .ok_or_else(|| format!("table {path:?} leaf {leaf_index} row underflow"))?;
                let take = usize::try_from(remaining)
                    .unwrap_or(usize::MAX)
                    .min(batch.num_rows() - offset);
                let slice = batch.slice(offset, take);
                let canonical = super::series_leaf::encode_canonical_batch_rows(&schema, &slice)
                    .map_err(|error| {
                        format!("encode table {path:?} leaf {leaf_index} rows: {error}")
                    })?;
                canonical_lengths[leaf_index] = canonical_lengths[leaf_index]
                    .checked_add(u64::try_from(canonical.len()).map_err(|_| {
                        format!("table {path:?} leaf {leaf_index} bytes exceed u64::MAX")
                    })?)
                    .ok_or_else(|| {
                        format!("table {path:?} leaf {leaf_index} bytes exceed u64::MAX")
                    })?;
                records.push((leaf_index, slice));
                leaf_rows += take as u64;
                offset += take;
                if leaf_rows == leaf.logical_count {
                    leaf_index += 1;
                    leaf_rows = 0;
                }
            }
        }
        if !leaves.is_empty() && object_rows == 0 {
            return Err(format!(
                "table {path:?} object {} has no rows for a logical leaf",
                object.hash
            ));
        }
    }
    if leaf_index != leaves.len() || leaf_rows != 0 || schemas.len() != leaves.len() {
        return Err(format!(
            "table {path:?} ended after {leaf_index} of {} logical leaves",
            leaves.len()
        ));
    }
    let mut current = 0usize;
    let mut hasher = if leaves.is_empty() {
        None
    } else {
        Some(new_table_leaf_hasher(
            schemas[0].as_ref(),
            &leaves[0],
            canonical_lengths[0],
            false,
        )?)
    };
    for (index, batch) in records {
        while current < index {
            let computed = hasher
                .take()
                .expect("completed v3 table leaf hasher")
                .finish()
                .map_err(|error| format!("finish table {path:?} leaf {current}: {error}"))?;
            if computed != leaves[current].logical_hash {
                return Err(format!(
                    "table {path:?} leaf {current} hashes to {computed}, manifest names {}",
                    leaves[current].logical_hash
                ));
            }
            current += 1;
            hasher = Some(new_table_leaf_hasher(
                schemas[current].as_ref(),
                &leaves[current],
                canonical_lengths[current],
                false,
            )?);
        }
        hasher
            .as_mut()
            .expect("active v3 table leaf hasher")
            .write_batch(&batch)
            .map_err(|error| format!("hash table {path:?} leaf {current}: {error}"))?;
    }
    while current < leaves.len() {
        let computed = hasher
            .take()
            .expect("completed v3 table leaf hasher")
            .finish()
            .map_err(|error| format!("finish table {path:?} leaf {current}: {error}"))?;
        if computed != leaves[current].logical_hash {
            return Err(format!(
                "table {path:?} leaf {current} hashes to {computed}, manifest names {}",
                leaves[current].logical_hash
            ));
        }
        current += 1;
        if current < leaves.len() {
            hasher = Some(new_table_leaf_hasher(
                schemas[current].as_ref(),
                &leaves[current],
                canonical_lengths[current],
                false,
            )?);
        }
    }
    Ok(())
}

fn verify_payload(object: &CapsuleObject, bytes: &[u8]) -> Result<(), String> {
    if ObjectHash::of_bytes(bytes) != object.hash {
        return Err(format!(
            "capsule payload {} fails BLAKE3 verification",
            object.hash
        ));
    }
    if u64::try_from(bytes.len()).ok() != Some(object.size) {
        return Err(format!(
            "capsule payload {} has size {}, expected {}",
            object.hash,
            bytes.len(),
            object.size
        ));
    }
    Ok(())
}

fn capsule_series_root_for_format(
    format: &str,
    payload_kind: CapsulePayloadKind,
    schema_fingerprint: Option<ObjectHash>,
    leaves: &[CapsuleLeaf],
) -> ObjectHash {
    if is_schema_evolved_format(format) {
        capsule_series_root_v3(payload_kind, schema_fingerprint, leaves)
    } else {
        capsule_series_root(payload_kind, schema_fingerprint, leaves)
    }
}

fn is_schema_evolved_format(format: &str) -> bool {
    matches!(format, CAPSULE_FORMAT_V3 | CAPSULE_FORMAT_V4)
}

fn validate_entry(entry: &CapsuleEntry, format: &str) -> Result<(), String> {
    if entry.source_node_id.is_empty() {
        return Err(format!(
            "capsule path {:?} has an empty source_node_id",
            entry.path
        ));
    }
    match (&entry.node, entry.entry_type) {
        (CapsuleNode::Directory, EntryType::DirectoryPhysical)
        | (CapsuleNode::Symlink { .. }, EntryType::Symlink) => {}
        (
            CapsuleNode::Dynamic { metadata, .. },
            EntryType::DirectoryDynamic | EntryType::FileDynamic | EntryType::TableDynamic,
        ) => {
            if metadata.is_some() && format != CAPSULE_FORMAT_V4 {
                return Err(format!(
                    "capsule path {:?} has dynamic metadata unsupported by {format}",
                    entry.path
                ));
            }
        }
        (
            CapsuleNode::Physical {
                payload_kind,
                schema_fingerprint,
                logical_root,
                objects,
                leaves,
            },
            entry_type,
        ) => {
            let expected_kind = match entry_type {
                EntryType::FilePhysicalVersion | EntryType::FilePhysicalSeries => {
                    CapsulePayloadKind::File
                }
                EntryType::TablePhysicalVersion | EntryType::TablePhysicalSeries => {
                    CapsulePayloadKind::Table
                }
                _ => {
                    return Err(format!(
                        "capsule path {:?} has physical content incompatible with {entry_type}",
                        entry.path
                    ));
                }
            };
            if *payload_kind != expected_kind {
                return Err(format!(
                    "capsule path {:?} payload kind does not match {entry_type}",
                    entry.path
                ));
            }
            match payload_kind {
                CapsulePayloadKind::File if schema_fingerprint.is_some() => {
                    return Err(format!(
                        "capsule file path {:?} must not declare a schema fingerprint",
                        entry.path
                    ));
                }
                CapsulePayloadKind::Table
                    if !is_schema_evolved_format(format) && schema_fingerprint.is_none() =>
                {
                    return Err(format!(
                        "capsule table path {:?} must declare a schema fingerprint",
                        entry.path
                    ));
                }
                CapsulePayloadKind::File | CapsulePayloadKind::Table => {}
            }
            match payload_kind {
                CapsulePayloadKind::File if leaves.is_empty() != objects.is_empty() => {
                    return Err(format!(
                        "capsule file path {:?} must have both logical leaves and payload objects, or neither",
                        entry.path
                    ));
                }
                CapsulePayloadKind::Table if objects.is_empty() => {
                    return Err(format!(
                        "capsule table path {:?} has no Parquet schema carrier",
                        entry.path
                    ));
                }
                CapsulePayloadKind::File | CapsulePayloadKind::Table => {}
            }
            if *payload_kind == CapsulePayloadKind::Table
                && is_schema_evolved_format(format)
                && leaves.is_empty()
                && schema_fingerprint.is_none()
            {
                return Err(format!(
                    "empty v3 table path {:?} must declare a node schema fingerprint",
                    entry.path
                ));
            }
            for (index, leaf) in leaves.iter().enumerate() {
                if leaf.logical_count == 0 {
                    return Err(format!(
                        "capsule path {:?} leaf {index} has zero logical count",
                        entry.path
                    ));
                }
                if let Some((minimum, maximum)) = leaf.min_event_time.zip(leaf.max_event_time)
                    && minimum > maximum
                {
                    return Err(format!(
                        "capsule path {:?} leaf {index} has minimum event time greater than maximum",
                        entry.path
                    ));
                }
                if let Some(attributes) = &leaf.logical_attributes {
                    validate_capsule_attributes(attributes).map_err(|error| {
                        format!(
                            "capsule path {:?} leaf {index} attributes: {error}",
                            entry.path
                        )
                    })?;
                }
                if *payload_kind == CapsulePayloadKind::File && leaf.schema_fingerprint.is_some() {
                    return Err(format!(
                        "capsule file path {:?} leaf {index} must not declare a schema fingerprint",
                        entry.path
                    ));
                }
                if *payload_kind == CapsulePayloadKind::Table {
                    if is_schema_evolved_format(format) {
                        let leaf_schema = leaf.schema_fingerprint.ok_or_else(|| {
                            format!(
                                "capsule v3 table path {:?} leaf {index} must declare a schema fingerprint",
                                entry.path
                            )
                        })?;
                        if let Some(node_schema) = schema_fingerprint
                            && *node_schema != leaf_schema
                        {
                            return Err(format!(
                                "capsule v3 table path {:?} leaf {index} schema fingerprint differs from node schema",
                                entry.path
                            ));
                        }
                    } else if leaf.schema_fingerprint.is_some() {
                        return Err(format!(
                            "capsule v1/v2 table path {:?} leaf {index} must not declare a schema fingerprint",
                            entry.path
                        ));
                    }
                }
            }
            let computed_root =
                capsule_series_root_for_format(format, *payload_kind, *schema_fingerprint, leaves);
            if *logical_root != computed_root {
                return Err(format!(
                    "capsule path {:?} logical root mismatch: declared {}, computed {}",
                    entry.path, logical_root, computed_root
                ));
            }
        }
        _ => {
            return Err(format!(
                "capsule path {:?} content does not match entry type {}",
                entry.path, entry.entry_type
            ));
        }
    }
    Ok(())
}

fn validate_path(path: &str) -> Result<(), String> {
    if path == "/" {
        return Ok(());
    }
    if !path.starts_with('/') {
        return Err(format!("capsule path {path:?} is not absolute"));
    }
    if path.ends_with('/') {
        return Err(format!("capsule path {path:?} has a trailing slash"));
    }
    if path.chars().any(char::is_control) {
        return Err(format!(
            "capsule path {path:?} contains a control character"
        ));
    }
    for component in path[1..].split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(format!(
                "capsule path {path:?} contains a noncanonical component"
            ));
        }
    }
    Ok(())
}

fn parent_path(path: &str) -> String {
    let split = path
        .rfind('/')
        .expect("validated absolute path has a slash");
    if split == 0 {
        "/".to_string()
    } else {
        path[..split].to_string()
    }
}

fn entry_objects(node: &CapsuleNode) -> Box<dyn Iterator<Item = &CapsuleObject> + '_> {
    match node {
        CapsuleNode::Directory => Box::new(std::iter::empty()),
        CapsuleNode::Symlink { target } => Box::new(std::iter::once(target)),
        CapsuleNode::Dynamic { recipe, .. } => Box::new(std::iter::once(recipe)),
        CapsuleNode::Physical { objects, .. } => Box::new(objects.iter()),
    }
}

/// Encode logical attributes using the logical-series-v2 canonical JSON rules.
///
/// Object keys are recursively sorted by UTF-8 bytes, insignificant whitespace
/// is removed, and numbers are restricted to signed or unsigned 64-bit
/// integers.
///
/// # Errors
///
/// Returns an error for malformed JSON, a non-object top-level value, or a
/// number outside the supported integer model.
pub fn encode_capsule_attributes(attributes: &str) -> Result<Vec<u8>, String> {
    super::series_leaf::encode_canonical_attributes(attributes)
}

fn validate_capsule_attributes(attributes: &str) -> Result<(), String> {
    super::series_leaf::validate_canonical_attributes(attributes.as_bytes())
}

fn push_len_prefixed_u32(output: &mut Vec<u8>, value: &[u8]) -> Result<(), String> {
    let length =
        u32::try_from(value.len()).map_err(|_| "capsule field exceeds u32::MAX".to_string())?;
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(value);
    Ok(())
}

fn push_len_prefixed_u64(output: &mut Vec<u8>, value: &[u8]) -> Result<(), String> {
    let length =
        u64::try_from(value.len()).map_err(|_| "capsule field exceeds u64::MAX".to_string())?;
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(value);
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
                "capsule hashes must use lowercase hexadecimal",
            ));
        }
        ObjectHash::from_hex(&text).map_err(serde::de::Error::custom)
    }
}

mod optional_hash_serde {
    use serde::{Deserialize, Deserializer, Serializer};

    use super::{ObjectHash, hash_serde};

    pub fn serialize<S>(hash: &Option<ObjectHash>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match hash {
            Some(hash) => serializer.serialize_some(&hash.to_hex()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<ObjectHash>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let text = Option::<String>::deserialize(deserializer)?;
        text.map(|text| {
            let deserializer = serde::de::value::StringDeserializer::<D::Error>::new(text);
            hash_serde::deserialize(deserializer)
        })
        .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{DictionaryArray, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

    fn hash(text: &str) -> ObjectHash {
        ObjectHash::of_bytes(text.as_bytes())
    }

    fn object(text: &str) -> CapsuleObject {
        CapsuleObject {
            hash: hash(text),
            size: u64::try_from(text.len()).expect("test string length fits u64"),
        }
    }

    fn source() -> CapsuleSource {
        CapsuleSource {
            pond_id: "0198f00d-cafe-7000-8000-000000000001".to_string(),
            birthplace: "watershop".to_string(),
            source_tip: hash("tip"),
            exported_at_micros: 1_700_000_000_000_000,
            tool_version: "0.52.0".to_string(),
        }
    }

    fn root() -> CapsuleEntry {
        CapsuleEntry {
            path: "/".to_string(),
            entry_type: EntryType::DirectoryPhysical,
            source_node_id: "root".to_string(),
            node: CapsuleNode::Directory,
        }
    }

    fn file(path: &str, node_id: &str, content: &str) -> CapsuleEntry {
        let attributes = r#"{"a":"1","z":"2"}"#;
        let leaves = vec![CapsuleLeaf {
            logical_hash: capsule_leaf_hash(
                CapsulePayloadKind::File,
                None,
                u64::try_from(content.len()).expect("test string length fits u64"),
                content.as_bytes(),
                None,
                None,
                Some(attributes),
            )
            .unwrap(),
            logical_count: u64::try_from(content.len()).expect("test string length fits u64"),
            source_timestamp: 1_700_000_000_000_000,
            min_event_time: None,
            max_event_time: None,
            logical_attributes: Some(attributes.to_string()),
            schema_fingerprint: None,
        }];
        CapsuleEntry {
            path: path.to_string(),
            entry_type: EntryType::FilePhysicalSeries,
            source_node_id: node_id.to_string(),
            node: CapsuleNode::Physical {
                payload_kind: CapsulePayloadKind::File,
                schema_fingerprint: None,
                logical_root: capsule_series_root(CapsulePayloadKind::File, None, &leaves),
                objects: vec![object(content)],
                leaves,
            },
        }
    }

    fn parquet_bytes(batch: &RecordBatch) -> Vec<u8> {
        let mut bytes = Vec::new();
        {
            let mut writer =
                ArrowWriter::try_new(&mut bytes, batch.schema(), None).expect("Parquet writer");
            writer.write(batch).expect("write batch");
            writer.close().expect("close writer");
        }
        bytes
    }

    #[test]
    fn verifier_accepts_dictionary_and_plain_physical_table_objects() {
        let plain_schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        let plain_batch = RecordBatch::try_new(
            plain_schema.clone(),
            vec![Arc::new(StringArray::from(vec![Some("a"), Some("b")]))],
        )
        .unwrap();
        let dictionary_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
            true,
        )]));
        let dictionary: DictionaryArray<arrow_array::types::UInt16Type> =
            vec![Some("c"), Some("d")].into_iter().collect();
        let dictionary_batch =
            RecordBatch::try_new(dictionary_schema, vec![Arc::new(dictionary)]).unwrap();
        let plain_bytes = parquet_bytes(&plain_batch);
        let dictionary_bytes = parquet_bytes(&dictionary_batch);
        let objects = [plain_bytes, dictionary_bytes]
            .into_iter()
            .map(|bytes| {
                let hash = ObjectHash::of_bytes(&bytes);
                (
                    CapsuleObject {
                        hash,
                        size: bytes.len() as u64,
                    },
                    bytes,
                )
            })
            .collect::<Vec<_>>();
        let canonical_schema = super::super::series_leaf::canonicalize_schema(&plain_schema)
            .expect("canonical schema");
        let schema_fingerprint =
            super::super::series_leaf::schema_fingerprint(&canonical_schema).unwrap();
        let leaf = CapsuleLeaf {
            logical_hash: super::super::series_leaf::table_leaf_hash(
                &canonical_schema,
                &[plain_batch, dictionary_batch],
                None,
                None,
                None,
            )
            .unwrap(),
            logical_count: 4,
            source_timestamp: 1_700_000_000_000_000,
            min_event_time: None,
            max_event_time: None,
            logical_attributes: None,
            schema_fingerprint: None,
        };
        let manifest = CapsuleManifest::new_v2(
            source(),
            vec![
                root(),
                CapsuleEntry {
                    path: "/table".to_string(),
                    entry_type: EntryType::TablePhysicalSeries,
                    source_node_id: "table".to_string(),
                    node: CapsuleNode::Physical {
                        payload_kind: CapsulePayloadKind::Table,
                        schema_fingerprint: Some(schema_fingerprint),
                        logical_root: capsule_series_root(
                            CapsulePayloadKind::Table,
                            Some(schema_fingerprint),
                            std::slice::from_ref(&leaf),
                        ),
                        objects: objects.iter().map(|(object, _)| object.clone()).collect(),
                        leaves: vec![leaf],
                    },
                },
            ],
        )
        .unwrap();
        let payloads = objects
            .into_iter()
            .map(|(object, bytes)| (object.hash, bytes))
            .collect();

        verify_capsule_payloads(&manifest, &payloads).expect("mixed physical encodings verify");
        let directory = tempfile::tempdir().unwrap();
        for (hash, bytes) in &payloads {
            std::fs::write(
                directory.path().join(format!("blake3={}", hash.to_hex())),
                bytes,
            )
            .unwrap();
        }
        verify_capsule_payload_directory(&manifest, directory.path())
            .expect("streamed mixed physical encodings verify");
    }

    #[test]
    fn v3_schema_evolved_table_commits_and_verifies_each_leaf_schema() {
        let first_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));
        let first_batch = RecordBatch::try_new(
            first_schema.clone(),
            vec![Arc::new(StringArray::from(vec!["one"]))],
        )
        .unwrap();
        let second_schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Utf8, false),
            Field::new("note", DataType::Utf8, true),
        ]));
        let second_batch = RecordBatch::try_new(
            second_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["two"])),
                Arc::new(StringArray::from(vec![Some("evolved")])),
            ],
        )
        .unwrap();
        let first_schema = super::super::series_leaf::canonicalize_schema(&first_schema).unwrap();
        let second_schema = super::super::series_leaf::canonicalize_schema(&second_schema).unwrap();
        let first_fingerprint =
            super::super::series_leaf::schema_fingerprint(&first_schema).unwrap();
        let second_fingerprint =
            super::super::series_leaf::schema_fingerprint(&second_schema).unwrap();
        let leaves = vec![
            CapsuleLeaf {
                logical_hash: super::super::series_leaf::table_leaf_hash(
                    &first_schema,
                    std::slice::from_ref(&first_batch),
                    None,
                    None,
                    None,
                )
                .unwrap(),
                logical_count: 1,
                source_timestamp: 1,
                min_event_time: None,
                max_event_time: None,
                logical_attributes: None,
                schema_fingerprint: Some(first_fingerprint),
            },
            CapsuleLeaf {
                logical_hash: super::super::series_leaf::table_leaf_hash(
                    &second_schema,
                    std::slice::from_ref(&second_batch),
                    None,
                    None,
                    None,
                )
                .unwrap(),
                logical_count: 1,
                source_timestamp: 2,
                min_event_time: None,
                max_event_time: None,
                logical_attributes: None,
                schema_fingerprint: Some(second_fingerprint),
            },
        ];
        let first_bytes = parquet_bytes(&first_batch);
        let second_bytes = parquet_bytes(&second_batch);
        let objects = vec![
            CapsuleObject {
                hash: ObjectHash::of_bytes(&first_bytes),
                size: first_bytes.len() as u64,
            },
            CapsuleObject {
                hash: ObjectHash::of_bytes(&second_bytes),
                size: second_bytes.len() as u64,
            },
        ];
        let manifest = CapsuleManifest::new_v3(
            source(),
            vec![
                root(),
                CapsuleEntry {
                    path: "/table".to_string(),
                    entry_type: EntryType::TablePhysicalSeries,
                    source_node_id: "table".to_string(),
                    node: CapsuleNode::Physical {
                        payload_kind: CapsulePayloadKind::Table,
                        schema_fingerprint: None,
                        logical_root: capsule_series_root_v3(
                            CapsulePayloadKind::Table,
                            None,
                            &leaves,
                        ),
                        objects: objects.clone(),
                        leaves,
                    },
                },
            ],
        )
        .unwrap();
        assert_ne!(
            capsule_series_root(
                CapsulePayloadKind::Table,
                None,
                match &manifest.entries[1].node {
                    CapsuleNode::Physical { leaves, .. } => leaves,
                    _ => unreachable!(),
                }
            ),
            match &manifest.entries[1].node {
                CapsuleNode::Physical { logical_root, .. } => *logical_root,
                _ => unreachable!(),
            }
        );
        let payloads = BTreeMap::from([
            (objects[0].hash, first_bytes),
            (objects[1].hash, second_bytes),
        ]);
        let encoded = String::from_utf8(capsule_manifest_bytes(&manifest).unwrap()).unwrap();
        assert!(
            !encoded.contains("\"schema_fingerprint\":null"),
            "v3 omits absent node schemas rather than encoding null"
        );
        verify_capsule_payloads(&manifest, &payloads).expect("v3 evolved table verifies");
    }

    #[test]
    fn v3_rejects_a_schema_transition_inside_one_parquet_object() {
        let first_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            first_schema.clone(),
            vec![Arc::new(StringArray::from(vec!["one", "two"]))],
        )
        .unwrap();
        let second_schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Utf8, false),
            Field::new("note", DataType::Utf8, true),
        ]));
        let first_schema = super::super::series_leaf::canonicalize_schema(&first_schema).unwrap();
        let second_schema = super::super::series_leaf::canonicalize_schema(&second_schema).unwrap();
        let first_fingerprint =
            super::super::series_leaf::schema_fingerprint(&first_schema).unwrap();
        let second_fingerprint =
            super::super::series_leaf::schema_fingerprint(&second_schema).unwrap();
        let bytes = parquet_bytes(&batch);
        let object = CapsuleObject {
            hash: ObjectHash::of_bytes(&bytes),
            size: bytes.len() as u64,
        };
        let leaves = vec![
            CapsuleLeaf {
                logical_hash: super::super::series_leaf::table_leaf_hash(
                    &first_schema,
                    &[batch.slice(0, 1)],
                    None,
                    None,
                    None,
                )
                .unwrap(),
                logical_count: 1,
                source_timestamp: 1,
                min_event_time: None,
                max_event_time: None,
                logical_attributes: None,
                schema_fingerprint: Some(first_fingerprint),
            },
            CapsuleLeaf {
                logical_hash: hash("unreachable"),
                logical_count: 1,
                source_timestamp: 2,
                min_event_time: None,
                max_event_time: None,
                logical_attributes: None,
                schema_fingerprint: Some(second_fingerprint),
            },
        ];
        let manifest = CapsuleManifest::new_v3(
            source(),
            vec![
                root(),
                CapsuleEntry {
                    path: "/table".to_string(),
                    entry_type: EntryType::TablePhysicalSeries,
                    source_node_id: "table".to_string(),
                    node: CapsuleNode::Physical {
                        payload_kind: CapsulePayloadKind::Table,
                        schema_fingerprint: None,
                        logical_root: capsule_series_root_v3(
                            CapsulePayloadKind::Table,
                            None,
                            &leaves,
                        ),
                        objects: vec![object.clone()],
                        leaves,
                    },
                },
            ],
        )
        .unwrap();
        let error = verify_capsule_payloads(&manifest, &BTreeMap::from([(object.hash, bytes)]))
            .expect_err("a schema transition cannot occur inside a Parquet object");
        assert!(error.contains("schema transitions must occur at physical object boundaries"));
    }

    #[test]
    fn v3_rejects_missing_or_conflicting_leaf_schemas() {
        let schema = hash("schema");
        let leaf = CapsuleLeaf {
            logical_hash: hash("leaf"),
            logical_count: 1,
            source_timestamp: 1,
            min_event_time: None,
            max_event_time: None,
            logical_attributes: None,
            schema_fingerprint: None,
        };
        let node = CapsuleNode::Physical {
            payload_kind: CapsulePayloadKind::Table,
            schema_fingerprint: None,
            logical_root: capsule_series_root_v3(
                CapsulePayloadKind::Table,
                None,
                std::slice::from_ref(&leaf),
            ),
            objects: vec![object("parquet")],
            leaves: vec![leaf.clone()],
        };
        assert!(
            CapsuleManifest::new_v3(
                source(),
                vec![
                    root(),
                    CapsuleEntry {
                        path: "/table".to_string(),
                        entry_type: EntryType::TablePhysicalSeries,
                        source_node_id: "table".to_string(),
                        node,
                    },
                ],
            )
            .unwrap_err()
            .contains("must declare a schema")
        );
        let mut conflicting = leaf;
        conflicting.schema_fingerprint = Some(hash("other"));
        let root_hash = capsule_series_root_v3(
            CapsulePayloadKind::Table,
            Some(schema),
            std::slice::from_ref(&conflicting),
        );
        assert!(
            CapsuleManifest::new_v3(
                source(),
                vec![
                    root(),
                    CapsuleEntry {
                        path: "/table".to_string(),
                        entry_type: EntryType::TablePhysicalSeries,
                        source_node_id: "table".to_string(),
                        node: CapsuleNode::Physical {
                            payload_kind: CapsulePayloadKind::Table,
                            schema_fingerprint: Some(schema),
                            logical_root: root_hash,
                            objects: vec![object("parquet")],
                            leaves: vec![conflicting.clone()],
                        },
                    },
                ],
            )
            .unwrap_err()
            .contains("differs from node schema")
        );
        let with_leaf_schema = CapsuleNode::Physical {
            payload_kind: CapsulePayloadKind::Table,
            schema_fingerprint: Some(schema),
            logical_root: capsule_series_root(
                CapsulePayloadKind::Table,
                Some(schema),
                std::slice::from_ref(&conflicting),
            ),
            objects: vec![object("parquet")],
            leaves: vec![conflicting],
        };
        assert!(
            CapsuleManifest::new_v2(
                source(),
                vec![
                    root(),
                    CapsuleEntry {
                        path: "/table".to_string(),
                        entry_type: EntryType::TablePhysicalSeries,
                        source_node_id: "table".to_string(),
                        node: with_leaf_schema,
                    },
                ],
            )
            .unwrap_err()
            .contains("v1/v2")
        );
    }

    #[test]
    fn manifest_new_sorts_and_round_trips_canonical_json() {
        let manifest = CapsuleManifest::new(
            source(),
            vec![
                file("/data/b", "b", "beta"),
                root(),
                CapsuleEntry {
                    path: "/data".to_string(),
                    entry_type: EntryType::DirectoryPhysical,
                    source_node_id: "data".to_string(),
                    node: CapsuleNode::Directory,
                },
                file("/data/a", "a", "alpha"),
            ],
        )
        .unwrap();
        assert_eq!(
            manifest
                .entries
                .iter()
                .map(|entry| entry.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/", "/data", "/data/a", "/data/b"]
        );

        let bytes = capsule_manifest_bytes(&manifest).unwrap();
        assert_eq!(decode_capsule_manifest(&bytes).unwrap(), manifest);
    }

    #[test]
    fn manifest_root_is_order_independent_at_construction() {
        let directory = CapsuleEntry {
            path: "/data".to_string(),
            entry_type: EntryType::DirectoryPhysical,
            source_node_id: "data".to_string(),
            node: CapsuleNode::Directory,
        };
        let first = CapsuleManifest::new(
            source(),
            vec![root(), directory.clone(), file("/data/a", "a", "alpha")],
        )
        .unwrap();
        let second = CapsuleManifest::new(
            source(),
            vec![file("/data/a", "a", "alpha"), directory, root()],
        )
        .unwrap();
        assert_eq!(
            capsule_manifest_bytes(&first).unwrap(),
            capsule_manifest_bytes(&second).unwrap()
        );
        assert_eq!(
            capsule_root(&first).unwrap(),
            capsule_root(&second).unwrap()
        );
    }

    #[test]
    fn rejects_noncanonical_json_bytes() {
        let manifest = CapsuleManifest::new(source(), vec![root()]).unwrap();
        let canonical = String::from_utf8(capsule_manifest_bytes(&manifest).unwrap()).unwrap();
        let noncanonical = canonical.replacen("{", "{\n", 1);
        assert!(decode_capsule_manifest(noncanonical.as_bytes()).is_err());
    }

    #[test]
    fn rejects_bad_topology_and_paths() {
        let missing_parent =
            CapsuleManifest::new(source(), vec![root(), file("/missing/a", "a", "x")]);
        assert!(missing_parent.unwrap_err().contains("missing parent"));

        let traversal =
            CapsuleManifest::new(source(), vec![root(), file("/data/../secret", "a", "x")]);
        assert!(traversal.unwrap_err().contains("noncanonical component"));
    }

    #[test]
    fn rejects_entry_content_mismatch() {
        let bad = CapsuleEntry {
            path: "/bad".to_string(),
            entry_type: EntryType::Symlink,
            source_node_id: "bad".to_string(),
            node: CapsuleNode::Directory,
        };
        assert!(
            CapsuleManifest::new(source(), vec![root(), bad])
                .unwrap_err()
                .contains("does not match")
        );
    }

    #[test]
    fn logical_attributes_require_v2_canonical_json() {
        let mut unsorted = file("/bad", "bad", "x");
        let CapsuleNode::Physical { leaves, .. } = &mut unsorted.node else {
            unreachable!()
        };
        leaves[0].logical_attributes = Some(r#"{"z":"1","a":"2"}"#.to_string());
        assert!(
            CapsuleManifest::new(source(), vec![root(), unsorted])
                .unwrap_err()
                .contains("canonical JSON")
        );

        let mut nested = file("/bad", "bad", "x");
        let CapsuleNode::Physical {
            leaves,
            logical_root,
            ..
        } = &mut nested.node
        else {
            unreachable!()
        };
        leaves[0].logical_attributes = Some(r#"{"a":{"x":true},"z":1.5}"#.to_string());
        *logical_root = capsule_series_root(CapsulePayloadKind::File, None, leaves);
        assert!(
            CapsuleManifest::new(source(), vec![root(), nested])
                .unwrap_err()
                .contains("not an i64 or u64")
        );
    }

    #[test]
    fn payload_closure_deduplicates_and_rejects_conflicting_sizes() {
        let mut duplicate = file("/a", "a", "same");
        let mut second = file("/b", "b", "same");
        let manifest =
            CapsuleManifest::new(source(), vec![root(), duplicate.clone(), second.clone()])
                .unwrap();
        assert_eq!(manifest.payload_objects().unwrap().len(), 1);

        let CapsuleNode::Physical { objects, .. } = &mut second.node else {
            unreachable!()
        };
        objects[0].size += 1;
        let conflicting =
            CapsuleManifest::new(source(), vec![root(), duplicate.clone(), second]).unwrap();
        assert!(
            conflicting
                .payload_objects()
                .unwrap_err()
                .contains("conflicting sizes")
        );

        let CapsuleNode::Physical {
            leaves,
            logical_root,
            ..
        } = &mut duplicate.node
        else {
            unreachable!()
        };
        leaves[0].logical_count = 0;
        *logical_root = capsule_series_root(CapsulePayloadKind::File, None, leaves);
        assert!(
            CapsuleManifest::new(source(), vec![root(), duplicate])
                .unwrap_err()
                .contains("zero logical count")
        );
    }

    #[test]
    fn rejects_invalid_bounds_and_logical_root() {
        let mut invalid_bounds = file("/bad", "bad", "x");
        let CapsuleNode::Physical {
            leaves,
            logical_root,
            ..
        } = &mut invalid_bounds.node
        else {
            unreachable!()
        };
        leaves[0].min_event_time = Some(20);
        leaves[0].max_event_time = Some(10);
        *logical_root = capsule_series_root(CapsulePayloadKind::File, None, leaves);
        assert!(
            CapsuleManifest::new(source(), vec![root(), invalid_bounds])
                .unwrap_err()
                .contains("minimum event time")
        );

        let mut invalid_root = file("/bad", "bad", "x");
        let CapsuleNode::Physical { logical_root, .. } = &mut invalid_root.node else {
            unreachable!()
        };
        *logical_root = hash("wrong");
        assert!(
            CapsuleManifest::new(source(), vec![root(), invalid_root])
                .unwrap_err()
                .contains("logical root mismatch")
        );
    }

    #[test]
    fn empty_physical_node_is_explicitly_representable() {
        let empty = CapsuleEntry {
            path: "/empty".to_string(),
            entry_type: EntryType::FilePhysicalVersion,
            source_node_id: "empty".to_string(),
            node: CapsuleNode::Physical {
                payload_kind: CapsulePayloadKind::File,
                schema_fingerprint: None,
                logical_root: capsule_series_root(CapsulePayloadKind::File, None, &[]),
                objects: Vec::new(),
                leaves: Vec::new(),
            },
        };
        CapsuleManifest::new(source(), vec![root(), empty]).unwrap();
    }

    #[test]
    fn root_golden_vector() {
        let manifest = CapsuleManifest::new(source(), vec![root()]).unwrap();
        assert_eq!(
            capsule_root(&manifest).unwrap().to_hex(),
            "9b6891de5b45220d72199fe63df2e7417260ad03e1de2fb66a2ebb1ac41e375f"
        );
    }

    #[test]
    fn v2_manifest_uses_distinct_format_and_root_domain() {
        let source = source();
        let entries = vec![root()];
        let v1 = CapsuleManifest::new(source.clone(), entries.clone()).unwrap();
        let v2 = CapsuleManifest::new_v2(source, entries).unwrap();
        assert_eq!(v2.format, CAPSULE_FORMAT_V2);
        assert_ne!(capsule_root(&v1).unwrap(), capsule_root(&v2).unwrap());
        assert_eq!(
            decode_capsule_manifest(&capsule_manifest_bytes(&v2).unwrap())
                .unwrap()
                .format,
            CAPSULE_FORMAT_V2
        );
    }

    #[test]
    fn v4_authenticates_dynamic_node_metadata() {
        let dynamic = CapsuleEntry {
            path: "/dynamic".to_string(),
            entry_type: EntryType::DirectoryDynamic,
            source_node_id: "dynamic".to_string(),
            node: CapsuleNode::Dynamic {
                recipe: object("recipe"),
                metadata: Some(CapsuleDynamicMetadata { timestamp: 42 }),
            },
        };
        let v3 = CapsuleManifest::new_v3(source(), vec![root(), dynamic.clone()]);
        assert!(
            v3.unwrap_err()
                .contains("dynamic metadata unsupported by pondcapsule.3")
        );

        let v4 = CapsuleManifest::new_v4(source(), vec![root(), dynamic]).unwrap();
        let encoded = capsule_manifest_bytes(&v4).unwrap();
        assert!(
            String::from_utf8(encoded.clone())
                .unwrap()
                .contains(r#""metadata":{"timestamp":42}"#)
        );
        assert_eq!(decode_capsule_manifest(&encoded).unwrap(), v4);
    }

    #[test]
    fn logical_file_leaf_matches_v2_golden_vector() {
        let payload = b"pressure,depth\n1.0,2.0\n";
        let logical_hash = capsule_leaf_hash(
            CapsulePayloadKind::File,
            None,
            u64::try_from(payload.len()).unwrap(),
            payload,
            Some(1_700_000_000_000_000),
            Some(1_700_000_001_000_000),
            Some(r#"{"source":"csv"}"#),
        )
        .unwrap();
        assert_eq!(
            logical_hash.to_hex(),
            "316b52fa1dc942471704cb2498faf9e73903d46ec7a4d03b778ac02125648ad6"
        );
    }

    #[test]
    fn representative_manifest_golden_vector() {
        let schema = hash("schema");
        let table_leaves = vec![CapsuleLeaf {
            logical_hash: capsule_leaf_hash(
                CapsulePayloadKind::Table,
                Some(schema),
                1,
                b"row",
                Some(100),
                Some(100),
                None,
            )
            .unwrap(),
            logical_count: 1,
            source_timestamp: 1_700_000_000_000_000,
            min_event_time: Some(100),
            max_event_time: Some(100),
            logical_attributes: None,
            schema_fingerprint: None,
        }];
        let manifest = CapsuleManifest::new(
            source(),
            vec![
                root(),
                CapsuleEntry {
                    path: "/dynamic".to_string(),
                    entry_type: EntryType::TableDynamic,
                    source_node_id: "dynamic".to_string(),
                    node: CapsuleNode::Dynamic {
                        recipe: object("recipe"),
                        metadata: None,
                    },
                },
                CapsuleEntry {
                    path: "/link".to_string(),
                    entry_type: EntryType::Symlink,
                    source_node_id: "link".to_string(),
                    node: CapsuleNode::Symlink {
                        target: object("/target"),
                    },
                },
                CapsuleEntry {
                    path: "/t\u{00e9}l\u{00e9}metry".to_string(),
                    entry_type: EntryType::TablePhysicalSeries,
                    source_node_id: "table".to_string(),
                    node: CapsuleNode::Physical {
                        payload_kind: CapsulePayloadKind::Table,
                        schema_fingerprint: Some(schema),
                        logical_root: capsule_series_root(
                            CapsulePayloadKind::Table,
                            Some(schema),
                            &table_leaves,
                        ),
                        objects: vec![object("parquet")],
                        leaves: table_leaves,
                    },
                },
            ],
        )
        .unwrap();
        let bytes = capsule_manifest_bytes(&manifest).unwrap();
        assert_eq!(
            capsule_root(&manifest).unwrap().to_hex(),
            "0cebe535670d309ec8bde50d530e997a8bbb186d26ce2d94bb94764929241f62"
        );
        assert_eq!(decode_capsule_manifest(&bytes).unwrap(), manifest);
    }
}
