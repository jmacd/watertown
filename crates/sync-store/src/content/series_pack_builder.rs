// SPDX-License-Identifier: Apache-2.0

//! Pure pack builder/repacker: turns logical leaves already in hand into
//! physical objects plus a self-verified [`PackIndex`]
//! (`docs/logical-series-identity-design.md` delivery gate 5).
//!
//! This module is exactly as pure as its siblings [`super::series_leaf`],
//! [`super::series_manifest`], and [`super::series_pack`]: it knows nothing
//! about `pond maintain`, `Ship::collapse_versions`, tlogfs, remotes, or how
//! a caller obtained its input leaves. Given [`FileLeafInput`]s or
//! [`TableLeafInput`]s the caller already holds, plus the independently
//! fetched [`SeriesManifest`] they claim to belong to, it produces physical
//! objects (raw byte ranges for a file pack, self-contained Parquet files
//! for a table pack) and a [`PackIndex`] that has already checked itself
//! with [`super::series_pack::verify_pack_against_manifest`] before ever
//! being returned.
//!
//! # A pack can only be minted for the real, named manifest
//!
//! Nothing here trusts a caller-supplied hash or count. [`FileLeafInput`]
//! and [`TableLeafInput`] each recompute their own leaf hash from real
//! content at construction time ([`super::series_leaf::file_leaf_hash_canonical`]
//! / [`super::series_leaf::table_leaf_hash_canonical`]), and
//! [`build_file_pack`]/[`build_table_pack`] both require that recomputed
//! hash to equal the corresponding entry of the caller-supplied *whole
//! series* ordered leaf-hash list, which in turn must itself fold (via
//! [`super::series_merkle::merkle_root`]) to the supplied
//! [`SeriesManifest`]'s own `leaf_merkle_root`. A pack built here for the
//! wrong manifest, the wrong range, or content that does not match what the
//! manifest actually committed to is therefore not merely discouraged --
//! it is a hard construction error, not something only caught later by a
//! reader.
//!
//! # Physical layout is deterministic and leaf-independent
//!
//! [`FilePackLayout`] and [`TablePackLayout`] cap physical objects by
//! logical size (bytes, rows) rather than by leaf count or guessed
//! compressed size. Physical object boundaries are computed independently
//! of logical leaf boundaries -- a leaf may be split across two physical
//! objects, and one physical object may hold many leaves -- while row/byte
//! order is always preserved exactly and no object is ever empty. Table
//! physical objects are self-contained Parquet files written with pinned,
//! explicit [`parquet::file::properties::WriterProperties`]
//! ([`deterministic_writer_properties`]) so that repacking identical input
//! under an identical layout always reproduces bit-identical physical
//! object bytes within one build.
//!
//! # Memory use
//!
//! [`build_file_pack`] and [`build_table_pack`] both take `leaves` by
//! shared reference: the caller retains ownership of every input leaf's
//! memory for its own lifetime, and this module never copies the whole
//! series into one buffer. Beyond the caller's own `leaves` slice, these
//! functions construct at most one physical target object at a time (a
//! growing byte buffer for a file pack; a small run of zero-copy
//! [`RecordBatch`] slices for a table pack). [`BuiltSeriesPack`] retains every
//! finished object's bytes for its caller to publish, so total returned memory
//! is O(the pack's physical bytes), in addition to the caller-owned leaves.
//! Native maintenance wiring must replace this collecting return value with a
//! streaming publication sink before using it for large production series.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};

use super::ObjectHash;
use super::series_leaf::{
    file_leaf_hash_canonical, schema_fingerprint, table_leaf_hash_canonical,
    validate_canonical_attributes,
};
use super::series_manifest::{PayloadKind, SeriesManifest};
use super::series_merkle::{generate_range_proof, merkle_root};
use super::series_pack::{PackIndex, PackLeafDescriptor, verify_pack_against_manifest};

/// One validated file-payload logical leaf, ready to be packed.
///
/// Constructed only via [`FileLeafInput::new`], which recomputes this
/// leaf's real [`super::series_leaf::file_leaf_hash_canonical`] hash from
/// `bytes` (rather than trusting any caller-supplied hash or count) and
/// derives its [`PackLeafDescriptor`] from that same validated content.
#[derive(Debug, Clone)]
pub struct FileLeafInput {
    bytes: Vec<u8>,
    descriptor: PackLeafDescriptor,
    leaf_hash: ObjectHash,
}

impl FileLeafInput {
    /// Construct a validated file leaf input.
    ///
    /// `logical_attributes`, when given, must already be canonical
    /// logical-attribute bytes exactly as
    /// [`super::series_leaf::encode_canonical_attributes`] would produce
    /// them; pass `None`, not `Some(b"{}".to_vec())`, for "no logical
    /// attributes at all".
    ///
    /// # Errors
    ///
    /// Returns an error if `bytes` is empty, if `logical_attributes` is
    /// `Some` but not canonical JSON object bytes or is `Some(&[])`, or if
    /// deriving this leaf's [`PackLeafDescriptor`] fails.
    pub fn new(
        bytes: Vec<u8>,
        min_event_time: Option<i64>,
        max_event_time: Option<i64>,
        logical_attributes: Option<Vec<u8>>,
    ) -> Result<Self, String> {
        if let Some(attrs) = &logical_attributes {
            if attrs.is_empty() {
                return Err(
                    "file leaf input logical attributes must be None, not Some(&[]), to mean \
                     \"absent\""
                        .to_string(),
                );
            }
            validate_canonical_attributes(attrs)?;
        }
        let leaf_hash = file_leaf_hash_canonical(
            &bytes,
            min_event_time,
            max_event_time,
            logical_attributes.as_deref(),
        )?;
        // `logical_count` is derived from `bytes.len()` itself, never
        // accepted as a separate caller-supplied argument.
        let logical_count = bytes.len() as u64;
        let descriptor = PackLeafDescriptor::new(
            logical_count,
            min_event_time,
            max_event_time,
            logical_attributes,
        )?;
        Ok(Self {
            bytes,
            descriptor,
            leaf_hash,
        })
    }

    /// This leaf's exact byte range, in order.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// This leaf's byte count (`bytes().len()` as `u64`).
    #[must_use]
    pub fn byte_count(&self) -> u64 {
        self.descriptor.logical_count()
    }

    /// This leaf's derived [`PackLeafDescriptor`].
    #[must_use]
    pub fn descriptor(&self) -> &PackLeafDescriptor {
        &self.descriptor
    }

    /// This leaf's real, recomputed-from-content identity hash (see the
    /// module docs for why this is never caller-supplied).
    #[must_use]
    pub fn leaf_hash(&self) -> ObjectHash {
        self.leaf_hash
    }
}

/// One validated table-payload logical leaf, ready to be packed.
///
/// Constructed only via [`TableLeafInput::new`], which recomputes this
/// leaf's real [`super::series_leaf::table_leaf_hash_canonical`] hash from
/// `schema`/`batches` (rather than trusting any caller-supplied hash or
/// count) and derives its [`PackLeafDescriptor`] from that same validated
/// content.
#[derive(Debug, Clone)]
pub struct TableLeafInput {
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
    descriptor: PackLeafDescriptor,
    leaf_hash: ObjectHash,
    schema_fingerprint: ObjectHash,
}

impl TableLeafInput {
    /// Construct a validated table leaf input.
    ///
    /// `batches` is this leaf's ordered `RecordBatch` content (append
    /// order, then original row order within a batch); the total row count
    /// across every batch must be at least one. See [`FileLeafInput::new`]
    /// for the `logical_attributes` absent-vs-empty convention.
    ///
    /// # Errors
    ///
    /// Returns an error if `schema` contains an unsupported logical type
    /// (see [`super::series_leaf::schema_fingerprint`]), if `batches`'
    /// total row count is zero or a batch's columns do not match `schema`,
    /// if `logical_attributes` is `Some` but not canonical JSON object
    /// bytes or is `Some(&[])`, or if deriving this leaf's
    /// [`PackLeafDescriptor`] fails.
    pub fn new(
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
        min_event_time: Option<i64>,
        max_event_time: Option<i64>,
        logical_attributes: Option<Vec<u8>>,
    ) -> Result<Self, String> {
        if let Some(attrs) = &logical_attributes {
            if attrs.is_empty() {
                return Err(
                    "table leaf input logical attributes must be None, not Some(&[]), to mean \
                     \"absent\""
                        .to_string(),
                );
            }
            validate_canonical_attributes(attrs)?;
        }
        let fingerprint = schema_fingerprint(&schema)?;
        let leaf_hash = table_leaf_hash_canonical(
            &schema,
            &batches,
            min_event_time,
            max_event_time,
            logical_attributes.as_deref(),
        )?;
        // `logical_count` (the row count) is derived from `batches` itself,
        // never accepted as a separate caller-supplied argument.
        let logical_count = batches.iter().try_fold(0u64, |total, batch| {
            total
                .checked_add(batch.num_rows() as u64)
                .ok_or_else(|| "table leaf input row count exceeds u64::MAX".to_string())
        })?;
        let descriptor = PackLeafDescriptor::new(
            logical_count,
            min_event_time,
            max_event_time,
            logical_attributes,
        )?;
        Ok(Self {
            schema,
            batches,
            descriptor,
            leaf_hash,
            schema_fingerprint: fingerprint,
        })
    }

    /// This leaf's schema.
    #[must_use]
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// This leaf's ordered `RecordBatch` content.
    #[must_use]
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// This leaf's row count (sum of `batches()`' row counts).
    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.descriptor.logical_count()
    }

    /// This leaf's derived [`PackLeafDescriptor`].
    #[must_use]
    pub fn descriptor(&self) -> &PackLeafDescriptor {
        &self.descriptor
    }

    /// This leaf's real, recomputed-from-content identity hash (see the
    /// module docs for why this is never caller-supplied).
    #[must_use]
    pub fn leaf_hash(&self) -> ObjectHash {
        self.leaf_hash
    }

    /// This leaf's canonical schema fingerprint
    /// ([`super::series_leaf::schema_fingerprint`] of [`Self::schema`]).
    #[must_use]
    pub fn schema_fingerprint(&self) -> ObjectHash {
        self.schema_fingerprint
    }
}

/// Physical layout policy for a file pack: the maximum number of bytes any
/// one physical object may hold.
///
/// Layout is logical-size based and independent of leaf boundaries: see the
/// module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FilePackLayout {
    max_bytes_per_object: u64,
}

impl FilePackLayout {
    /// Construct a layout policy capping each physical object at
    /// `max_bytes_per_object` bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if `max_bytes_per_object` is `0`.
    pub fn new(max_bytes_per_object: u64) -> Result<Self, String> {
        if max_bytes_per_object == 0 {
            return Err("max_bytes_per_object must be positive".to_string());
        }
        Ok(Self {
            max_bytes_per_object,
        })
    }

    /// The configured maximum physical object size, in bytes.
    #[must_use]
    pub fn max_bytes_per_object(&self) -> u64 {
        self.max_bytes_per_object
    }
}

/// Physical layout policy for a table pack: the maximum number of rows any
/// one physical Parquet object may hold.
///
/// Layout is logical-size based and independent of leaf boundaries: see the
/// module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TablePackLayout {
    max_rows_per_object: u64,
}

impl TablePackLayout {
    /// Construct a layout policy capping each physical object at
    /// `max_rows_per_object` rows.
    ///
    /// # Errors
    ///
    /// Returns an error if `max_rows_per_object` is `0`.
    pub fn new(max_rows_per_object: u64) -> Result<Self, String> {
        if max_rows_per_object == 0 {
            return Err("max_rows_per_object must be positive".to_string());
        }
        Ok(Self {
            max_rows_per_object,
        })
    }

    /// The configured maximum physical object size, in rows.
    #[must_use]
    pub fn max_rows_per_object(&self) -> u64 {
        self.max_rows_per_object
    }
}

/// The result of a successful [`build_file_pack`]/[`build_table_pack`]
/// call: a self-verified [`PackIndex`] plus every physical object it names,
/// each already hashed to its own content address.
#[derive(Debug, Clone)]
pub struct BuiltSeriesPack {
    /// The built, self-verified pack index.
    pub index: PackIndex,
    /// Every physical object the index names, `(content hash, bytes)`, in
    /// the same order as [`PackIndex::physical_object_hashes`].
    pub physical_objects: Vec<(ObjectHash, Vec<u8>)>,
}

/// Check that `manifest` is really the object named by `manifest_hash`, that
/// it declares the payload kind/schema fingerprint a builder is about to
/// pack, and that `whole_series_leaf_hashes` is really its whole ordered
/// leaf-hash list (matching both its declared `leaf_count` and its
/// `leaf_merkle_root`).
///
/// This is the check that makes it impossible to mint a pack for a
/// different manifest than the one actually supplied: every later step
/// (range proof, recomputed leaf hashes, [`verify_pack_against_manifest`])
/// is only meaningful once this binding is established.
fn verify_manifest_binding(
    manifest_hash: ObjectHash,
    manifest: &SeriesManifest,
    whole_series_leaf_hashes: &[ObjectHash],
    expected_kind: PayloadKind,
    expected_schema_fingerprint: Option<ObjectHash>,
) -> Result<(), String> {
    if manifest.hash() != manifest_hash {
        return Err(format!(
            "supplied manifest_hash {manifest_hash} does not match the supplied manifest's own hash {}",
            manifest.hash()
        ));
    }
    if manifest.payload_kind() != expected_kind {
        return Err(format!(
            "manifest declares payload kind {:?} but a {expected_kind:?} pack was requested",
            manifest.payload_kind()
        ));
    }
    if manifest.schema_fingerprint() != expected_schema_fingerprint {
        return Err(format!(
            "manifest schema_fingerprint {:?} does not match the leaves' schema fingerprint {:?}",
            manifest.schema_fingerprint(),
            expected_schema_fingerprint
        ));
    }
    let whole_len = u64::try_from(whole_series_leaf_hashes.len())
        .map_err(|_| "whole-series leaf hash list length exceeds u64::MAX".to_string())?;
    if whole_len != manifest.leaf_count() {
        return Err(format!(
            "whole-series leaf hash list has {whole_len} entry(ies) but the manifest declares leaf_count {}",
            manifest.leaf_count()
        ));
    }
    let computed_root = merkle_root(whole_series_leaf_hashes);
    if computed_root != manifest.leaf_merkle_root() {
        return Err(
            "whole-series leaf hash list's Merkle root does not match the manifest's leaf_merkle_root"
                .to_string(),
        );
    }
    Ok(())
}

/// When `[leaf_start, leaf_end)` is the *entire* series (`leaf_start == 0`
/// and `leaf_end == manifest.leaf_count()`), cross-check this pack's own
/// aggregate logical count and event-time bounds -- derived purely from
/// `descriptors` -- against the manifest's own declared aggregate fields.
///
/// Mirrors the aggregate check `steward`'s dual reader performs across a
/// whole series' selected packs (delivery gate 4), specialized to the case
/// where one single pack is that whole series.
fn verify_full_range_aggregate(
    manifest: &SeriesManifest,
    descriptors: &[PackLeafDescriptor],
    logical_count: u64,
) -> Result<(), String> {
    if logical_count != manifest.logical_count() {
        return Err(format!(
            "full-range pack's logical_count {logical_count} does not match the manifest's logical_count {}",
            manifest.logical_count()
        ));
    }
    let mut min: Option<i64> = None;
    let mut max: Option<i64> = None;
    for descriptor in descriptors {
        if let Some(v) = descriptor.min_event_time() {
            min = Some(min.map_or(v, |cur| cur.min(v)));
        }
        if let Some(v) = descriptor.max_event_time() {
            max = Some(max.map_or(v, |cur| cur.max(v)));
        }
    }
    if min != manifest.min_event_time() {
        return Err(format!(
            "full-range pack's aggregate min_event_time {min:?} does not match the manifest's {:?}",
            manifest.min_event_time()
        ));
    }
    if max != manifest.max_event_time() {
        return Err(format!(
            "full-range pack's aggregate max_event_time {max:?} does not match the manifest's {:?}",
            manifest.max_event_time()
        ));
    }
    Ok(())
}

/// Recompute every leaf's real content hash and require it equals the
/// corresponding entry of `whole_series_leaf_hashes[leaf_start..leaf_end)`,
/// returning those recomputed hashes in leaf order for later use as
/// [`verify_pack_against_manifest`]'s `range_leaf_hashes`.
///
/// This is the per-leaf half of "a builder must not be able to mint a pack
/// for a different manifest": [`verify_manifest_binding`] binds the whole
/// list to the manifest; this binds each individual supplied leaf's real
/// content to that same whole-series list.
fn bind_and_recompute_range<T, H>(
    whole_series_leaf_hashes: &[ObjectHash],
    leaf_start: u64,
    leaf_len: u64,
    leaves: &[T],
    leaf_hash_of: H,
) -> Result<(u64, Vec<ObjectHash>), String>
where
    H: Fn(&T) -> ObjectHash,
{
    let leaf_end = leaf_start
        .checked_add(leaf_len)
        .ok_or_else(|| "pack leaf range overflows u64".to_string())?;
    let start_usize =
        usize::try_from(leaf_start).map_err(|_| "leaf_start does not fit in usize".to_string())?;
    let end_usize =
        usize::try_from(leaf_end).map_err(|_| "leaf_end does not fit in usize".to_string())?;
    let expected_range = whole_series_leaf_hashes
        .get(start_usize..end_usize)
        .ok_or_else(|| {
            format!(
                "pack range [{leaf_start}, {leaf_end}) is out of bounds of the whole-series leaf hash list of length {}",
                whole_series_leaf_hashes.len()
            )
        })?;
    let mut range_leaf_hashes = Vec::with_capacity(expected_range.len());
    for (i, leaf) in leaves.iter().enumerate() {
        let recomputed = leaf_hash_of(leaf);
        let expected = expected_range[i];
        if recomputed != expected {
            return Err(format!(
                "leaf at range index {i} (whole-series leaf {}) recomputed hash {recomputed} does not \
                 match the whole-series leaf hash {expected}",
                leaf_start + i as u64
            ));
        }
        range_leaf_hashes.push(recomputed);
    }
    Ok((leaf_end, range_leaf_hashes))
}

/// Build a file pack covering leaves `[leaf_start, leaf_start +
/// leaves.len())` of the whole series named by `manifest`/`manifest_hash`,
/// splitting `leaves`' bytes into physical objects per `layout`.
///
/// See the module docs for exactly what is checked before this returns:
/// every leaf's hash is recomputed from its real bytes and bound to
/// `whole_series_leaf_hashes` and, through it, to `manifest`; the produced
/// [`PackIndex`] is checked against `manifest` with
/// [`verify_pack_against_manifest`] before being handed back.
///
/// # Errors
///
/// Returns an error if `leaves` is empty; if `manifest_hash` does not equal
/// `manifest.hash()`; if `manifest` is not a
/// [`super::series_manifest::PayloadKind::File`] series; if
/// `whole_series_leaf_hashes` does not match `manifest`'s declared
/// `leaf_count`/`leaf_merkle_root`; if `[leaf_start, leaf_start +
/// leaves.len())` is out of bounds of the whole series; if any leaf's
/// recomputed hash does not equal its whole-series counterpart; if the
/// range is the entire series and its aggregate logical count or
/// event-time bounds disagree with `manifest`'s own; or if constructing the
/// resulting [`PackIndex`] or its self-verification fails.
pub fn build_file_pack(
    manifest_hash: ObjectHash,
    manifest: &SeriesManifest,
    whole_series_leaf_hashes: &[ObjectHash],
    leaf_start: u64,
    leaves: &[FileLeafInput],
    layout: &FilePackLayout,
) -> Result<BuiltSeriesPack, String> {
    if leaves.is_empty() {
        return Err("a pack must cover at least one logical leaf".to_string());
    }
    verify_manifest_binding(
        manifest_hash,
        manifest,
        whole_series_leaf_hashes,
        PayloadKind::File,
        None,
    )?;

    let leaf_len = leaves.len() as u64;
    let (leaf_end, range_leaf_hashes) = bind_and_recompute_range(
        whole_series_leaf_hashes,
        leaf_start,
        leaf_len,
        leaves,
        FileLeafInput::leaf_hash,
    )?;

    let mut logical_count: u64 = 0;
    let mut descriptors = Vec::with_capacity(leaves.len());
    for leaf in leaves {
        logical_count = logical_count
            .checked_add(leaf.byte_count())
            .ok_or_else(|| "pack logical_count overflows u64".to_string())?;
        descriptors.push(leaf.descriptor().clone());
    }

    if leaf_start == 0 && leaf_end == manifest.leaf_count() {
        verify_full_range_aggregate(manifest, &descriptors, logical_count)?;
    }

    let cap = usize::try_from(layout.max_bytes_per_object())
        .map_err(|_| "max_bytes_per_object does not fit in usize on this platform".to_string())?;
    let physical_objects = build_file_physical_objects(leaves, cap);
    let physical_byte_count = checked_physical_byte_count(&physical_objects)?;
    let physical_object_hashes: Vec<ObjectHash> =
        physical_objects.iter().map(|(hash, _)| *hash).collect();

    let start_usize =
        usize::try_from(leaf_start).map_err(|_| "leaf_start does not fit in usize".to_string())?;
    let end_usize =
        usize::try_from(leaf_end).map_err(|_| "leaf_end does not fit in usize".to_string())?;
    let range_proof = generate_range_proof(whole_series_leaf_hashes, start_usize, end_usize)?;
    let range_root = manifest.leaf_merkle_root();

    let index = PackIndex::new(
        manifest_hash,
        leaf_start,
        leaf_end,
        manifest.leaf_count(),
        range_root,
        range_proof,
        physical_object_hashes,
        logical_count,
        physical_byte_count,
        descriptors,
    )?;

    verify_pack_against_manifest(manifest_hash, manifest, &index, &range_leaf_hashes)?;

    Ok(BuiltSeriesPack {
        index,
        physical_objects,
    })
}

/// Build a table pack covering leaves `[leaf_start, leaf_start +
/// leaves.len())` of the whole series named by `manifest`/`manifest_hash`,
/// splitting `leaves`' rows into self-contained Parquet physical objects
/// per `layout`.
///
/// Every leaf in `leaves` must share the identical canonical schema
/// fingerprint (that of `leaves[0]`), and that fingerprint must equal
/// `manifest.schema_fingerprint()`. See [`build_file_pack`]'s docs for the
/// rest of what this checks before returning.
///
/// # Errors
///
/// Returns an error if `leaves` is empty; if any leaf's schema fingerprint
/// differs from `leaves[0]`'s; if `manifest_hash` does not equal
/// `manifest.hash()`; if `manifest` is not a
/// [`super::series_manifest::PayloadKind::Table`] series or its
/// `schema_fingerprint` does not match the leaves'; if
/// `whole_series_leaf_hashes` does not match `manifest`'s declared
/// `leaf_count`/`leaf_merkle_root`; if `[leaf_start, leaf_start +
/// leaves.len())` is out of bounds of the whole series; if any leaf's
/// recomputed hash does not equal its whole-series counterpart; if the
/// range is the entire series and its aggregate logical count or
/// event-time bounds disagree with `manifest`'s own; if writing a physical
/// Parquet object fails; or if constructing the resulting [`PackIndex`] or
/// its self-verification fails.
pub fn build_table_pack(
    manifest_hash: ObjectHash,
    manifest: &SeriesManifest,
    whole_series_leaf_hashes: &[ObjectHash],
    leaf_start: u64,
    leaves: &[TableLeafInput],
    layout: &TablePackLayout,
) -> Result<BuiltSeriesPack, String> {
    let Some(first) = leaves.first() else {
        return Err("a pack must cover at least one logical leaf".to_string());
    };
    let schema = Arc::clone(first.schema());
    let fingerprint = first.schema_fingerprint();
    for (i, leaf) in leaves.iter().enumerate().skip(1) {
        if leaf.schema_fingerprint() != fingerprint {
            return Err(format!(
                "leaf {i} has schema fingerprint {} but leaf 0 has {fingerprint} (every leaf in a \
                 table pack must share one canonical schema)",
                leaf.schema_fingerprint()
            ));
        }
    }
    verify_manifest_binding(
        manifest_hash,
        manifest,
        whole_series_leaf_hashes,
        PayloadKind::Table,
        Some(fingerprint),
    )?;

    let leaf_len = leaves.len() as u64;
    let (leaf_end, range_leaf_hashes) = bind_and_recompute_range(
        whole_series_leaf_hashes,
        leaf_start,
        leaf_len,
        leaves,
        TableLeafInput::leaf_hash,
    )?;

    let mut logical_count: u64 = 0;
    let mut descriptors = Vec::with_capacity(leaves.len());
    for leaf in leaves {
        logical_count = logical_count
            .checked_add(leaf.row_count())
            .ok_or_else(|| "pack logical_count overflows u64".to_string())?;
        descriptors.push(leaf.descriptor().clone());
    }

    if leaf_start == 0 && leaf_end == manifest.leaf_count() {
        verify_full_range_aggregate(manifest, &descriptors, logical_count)?;
    }

    let physical_objects = build_table_physical_objects(&schema, leaves, layout)?;
    let physical_byte_count = checked_physical_byte_count(&physical_objects)?;
    let physical_object_hashes: Vec<ObjectHash> =
        physical_objects.iter().map(|(hash, _)| *hash).collect();

    let start_usize =
        usize::try_from(leaf_start).map_err(|_| "leaf_start does not fit in usize".to_string())?;
    let end_usize =
        usize::try_from(leaf_end).map_err(|_| "leaf_end does not fit in usize".to_string())?;
    let range_proof = generate_range_proof(whole_series_leaf_hashes, start_usize, end_usize)?;
    let range_root = manifest.leaf_merkle_root();

    let index = PackIndex::new(
        manifest_hash,
        leaf_start,
        leaf_end,
        manifest.leaf_count(),
        range_root,
        range_proof,
        physical_object_hashes,
        logical_count,
        physical_byte_count,
        descriptors,
    )?;

    verify_pack_against_manifest(manifest_hash, manifest, &index, &range_leaf_hashes)?;

    Ok(BuiltSeriesPack {
        index,
        physical_objects,
    })
}

/// Sum every physical object's byte length into a checked `u64` total.
fn checked_physical_byte_count(physical_objects: &[(ObjectHash, Vec<u8>)]) -> Result<u64, String> {
    physical_objects.iter().try_fold(0u64, |total, (_, bytes)| {
        total
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| "physical_byte_count overflows u64".to_string())
    })
}

/// Split `leaves`' bytes, in order, into physical objects each capped at
/// `cap` bytes: physical boundaries are independent of leaf boundaries (a
/// leaf may be split across objects) and no object is ever empty.
///
/// Holds at most one target object's buffer (bounded by `cap`) plus the one
/// leaf currently being consumed; never buffers the whole series.
fn build_file_physical_objects(leaves: &[FileLeafInput], cap: usize) -> Vec<(ObjectHash, Vec<u8>)> {
    let mut objects = Vec::new();
    let mut current: Vec<u8> = Vec::new();
    for leaf in leaves {
        let mut remaining = leaf.bytes();
        while !remaining.is_empty() {
            let space = cap - current.len();
            let take = space.min(remaining.len());
            current.extend_from_slice(&remaining[..take]);
            remaining = &remaining[take..];
            if current.len() == cap {
                let hash = ObjectHash::of_bytes(&current);
                objects.push((hash, std::mem::take(&mut current)));
            }
        }
    }
    if !current.is_empty() {
        let hash = ObjectHash::of_bytes(&current);
        objects.push((hash, current));
    }
    objects
}

/// Split `leaves`' rows, in order, into physical Parquet objects each
/// capped at `layout.max_rows_per_object()` rows: physical boundaries are
/// independent of leaf boundaries (a leaf's rows may be split across
/// objects) and no object is ever empty.
///
/// Holds at most one target object's worth of zero-copy `RecordBatch`
/// slices (bounded by the row cap) at a time; slicing a `RecordBatch`
/// shares its underlying Arrow buffers rather than copying, so this never
/// buffers the whole series' rows.
///
/// # Errors
///
/// Propagates a Parquet encode error from [`write_table_object`].
fn build_table_physical_objects(
    schema: &Arc<Schema>,
    leaves: &[TableLeafInput],
    layout: &TablePackLayout,
) -> Result<Vec<(ObjectHash, Vec<u8>)>, String> {
    let cap = layout.max_rows_per_object();
    let mut objects = Vec::new();
    let mut current_pieces: Vec<RecordBatch> = Vec::new();
    let mut current_rows: u64 = 0;
    for leaf in leaves {
        for batch in leaf.batches() {
            let total = batch.num_rows();
            let mut offset = 0usize;
            while offset < total {
                let space = cap - current_rows;
                let space_usize = usize::try_from(space).unwrap_or(usize::MAX);
                let take = space_usize.min(total - offset);
                if take > 0 {
                    current_pieces.push(batch.slice(offset, take));
                    current_rows += take as u64;
                    offset += take;
                }
                if current_rows == cap {
                    objects.push(write_table_object(schema, &current_pieces)?);
                    current_pieces.clear();
                    current_rows = 0;
                }
            }
        }
    }
    if !current_pieces.is_empty() {
        objects.push(write_table_object(schema, &current_pieces)?);
    }
    Ok(objects)
}

/// The fixed [`WriterProperties`] every table physical object is written
/// with: pinned explicitly (rather than left at the `parquet` crate's own
/// defaults) so that repacking identical input under an identical layout
/// always reproduces bit-identical physical object bytes within one build.
/// Compression is disabled, dictionary encoding is disabled, and the writer
/// version and statistics level are fixed, removing every degree of
/// freedom that could otherwise vary output bytes for identical logical
/// content without changing the properties themselves.
fn deterministic_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build()
}

/// Encode `batches` (already the exact rows of one logical unit -- a table
/// physical object here, or a single reconstructed logical leaf for a v2
/// materializer) into a self-contained Parquet buffer, using the identical
/// pinned [`deterministic_writer_properties`] every table physical object in
/// this module is written with.
///
/// Exposed beyond this module for `steward`'s native v2 materialization
/// (`docs/logical-series-identity-design.md`), which must re-encode a
/// fetched-and-verified table leaf's decoded rows into valid Parquet bytes
/// before writing them through the destination pond's ordinary series
/// writer -- using the same deterministic encoder this module already
/// verifies pack objects with, rather than a second, possibly-divergent
/// encoder.
///
/// # Errors
///
/// Returns an error if the Parquet writer cannot be constructed, a batch
/// cannot be written, or the writer cannot be closed.
pub fn encode_table_leaf_parquet(
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
) -> Result<Vec<u8>, String> {
    let props = deterministic_writer_properties();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, Arc::clone(schema), Some(props))
        .map_err(|e| format!("create parquet writer: {e}"))?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| format!("write parquet batch: {e}"))?;
    }
    writer
        .close()
        .map_err(|e| format!("close parquet writer: {e}"))?;
    Ok(bytes)
}

/// Write one complete, independently-readable physical Parquet object from
/// `pieces` (already-sliced `RecordBatch`es, in row order), returning its
/// content hash and encoded bytes.
///
/// `pieces` is exactly one target object's worth of rows -- never a whole
/// pack's -- so this function's own memory use is bounded to one physical
/// object.
fn write_table_object(
    schema: &Arc<Schema>,
    pieces: &[RecordBatch],
) -> Result<(ObjectHash, Vec<u8>), String> {
    let bytes = encode_table_leaf_parquet(schema, pieces)?;
    let hash = ObjectHash::of_bytes(&bytes);
    Ok((hash, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content::series_merkle::merkle_root as whole_merkle_root;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn i64_string_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]))
    }

    fn batch(schema: &Arc<Schema>, ids: &[i64], labels: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(labels.to_vec())),
            ],
        )
        .expect("record batch")
    }

    /// A whole file series, its manifest, and its ordered leaf hashes,
    /// partitioned into `leaf_byte_counts` logical leaves.
    struct FileSeriesFixture {
        manifest: SeriesManifest,
        manifest_hash: ObjectHash,
        leaf_hashes: Vec<ObjectHash>,
        leaves: Vec<FileLeafInput>,
    }

    fn build_file_series(bytes: &[u8], leaf_byte_counts: &[usize]) -> FileSeriesFixture {
        assert_eq!(leaf_byte_counts.iter().sum::<usize>(), bytes.len());
        let mut leaves = Vec::with_capacity(leaf_byte_counts.len());
        let mut leaf_hashes = Vec::with_capacity(leaf_byte_counts.len());
        let mut offset = 0usize;
        for &count in leaf_byte_counts {
            let slice = bytes[offset..offset + count].to_vec();
            let leaf = FileLeafInput::new(slice, None, None, None).expect("leaf input");
            leaf_hashes.push(leaf.leaf_hash());
            leaves.push(leaf);
            offset += count;
        }
        let root = whole_merkle_root(&leaf_hashes);
        let manifest = SeriesManifest::new(
            PayloadKind::File,
            None,
            bytes.len() as u64,
            leaf_byte_counts.len() as u64,
            None,
            None,
            None,
            root,
        )
        .expect("valid manifest");
        let manifest_hash = manifest.hash();
        FileSeriesFixture {
            manifest,
            manifest_hash,
            leaf_hashes,
            leaves,
        }
    }

    /// A whole table series, its manifest, and its ordered leaf hashes,
    /// partitioned into `leaf_row_counts` logical leaves.
    struct TableSeriesFixture {
        manifest: SeriesManifest,
        manifest_hash: ObjectHash,
        leaf_hashes: Vec<ObjectHash>,
        leaves: Vec<TableLeafInput>,
    }

    fn build_table_series(
        schema: &Arc<Schema>,
        rows: &[(i64, &str)],
        leaf_row_counts: &[usize],
    ) -> TableSeriesFixture {
        assert_eq!(leaf_row_counts.iter().sum::<usize>(), rows.len());
        let fingerprint = schema_fingerprint(schema).expect("schema fingerprint");
        let mut leaves = Vec::with_capacity(leaf_row_counts.len());
        let mut leaf_hashes = Vec::with_capacity(leaf_row_counts.len());
        let mut offset = 0usize;
        for &count in leaf_row_counts {
            let slice = &rows[offset..offset + count];
            let ids: Vec<i64> = slice.iter().map(|(id, _)| *id).collect();
            let labels: Vec<&str> = slice.iter().map(|(_, l)| *l).collect();
            let b = batch(schema, &ids, &labels);
            let leaf =
                TableLeafInput::new(schema.clone(), vec![b], None, None, None).expect("leaf input");
            leaf_hashes.push(leaf.leaf_hash());
            leaves.push(leaf);
            offset += count;
        }
        let root = whole_merkle_root(&leaf_hashes);
        let manifest = SeriesManifest::new(
            PayloadKind::Table,
            Some(fingerprint),
            rows.len() as u64,
            leaf_row_counts.len() as u64,
            None,
            None,
            None,
            root,
        )
        .expect("valid manifest");
        let manifest_hash = manifest.hash();
        TableSeriesFixture {
            manifest,
            manifest_hash,
            leaf_hashes,
            leaves,
        }
    }

    fn decode_parquet(bytes: &[u8]) -> (Arc<Schema>, Vec<RecordBatch>) {
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes.to_vec()))
            .expect("open parquet");
        let schema = builder.schema().clone();
        let reader = builder.build().expect("build reader");
        let batches: Vec<RecordBatch> = reader.map(|b| b.expect("decode batch")).collect();
        (schema, batches)
    }

    // -- File pack: layouts, determinism, verification -----------------

    #[test]
    fn file_pack_one_object_layout_verifies() {
        let bytes = b"abcdefghij".to_vec();
        let fixture = build_file_series(&bytes, &[4, 6]);
        let layout = FilePackLayout::new(100).expect("layout");
        let built = build_file_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &layout,
        )
        .expect("build file pack");
        assert_eq!(built.physical_objects.len(), 1);
        assert_eq!(built.index.series_hash(), fixture.manifest_hash);
        assert_eq!(built.index.leaf_start(), 0);
        assert_eq!(built.index.leaf_end(), 2);
        assert_eq!(built.index.logical_count(), bytes.len() as u64);
        let (_, obj_bytes) = &built.physical_objects[0];
        assert_eq!(obj_bytes.as_slice(), bytes.as_slice());
    }

    #[test]
    fn file_pack_allows_repeated_content_objects() {
        let fixture = build_file_series(b"abcdabcd", &[4, 4]);
        let built = build_file_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &FilePackLayout::new(4).unwrap(),
        )
        .expect("repeated content is a valid ordered physical stream");
        assert_eq!(built.physical_objects.len(), 2);
        assert_eq!(
            built.index.physical_object_hashes()[0],
            built.index.physical_object_hashes()[1]
        );
    }

    #[test]
    fn file_pack_large_cap_does_not_reserve_the_policy_limit() {
        let fixture = build_file_series(b"small", &[5]);
        let built = build_file_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &FilePackLayout::new(u64::MAX).unwrap(),
        )
        .expect("an unlimited cap must allocate only actual content");
        assert_eq!(built.physical_objects.len(), 1);
        assert_eq!(built.physical_objects[0].1, b"small");
    }

    #[test]
    fn file_pack_one_object_per_leaf_layout_verifies() {
        let bytes = b"abcdefghij".to_vec();
        let fixture = build_file_series(&bytes, &[4, 6]);
        let layout = FilePackLayout::new(4).expect("layout");
        let built = build_file_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &layout,
        )
        .expect("build file pack");
        // 4-byte cap over a [4, 6] byte split produces objects [4, 4, 2]:
        // one object matches leaf 0 exactly, but leaf 1 (6 bytes) is split
        // across two objects.
        assert_eq!(built.physical_objects.len(), 3);
        for (_, obj) in &built.physical_objects {
            assert!(!obj.is_empty(), "no physical object may be empty");
        }
        let mut reassembled = Vec::new();
        for (_, obj) in &built.physical_objects {
            reassembled.extend_from_slice(obj);
        }
        assert_eq!(reassembled, bytes);
    }

    #[test]
    fn file_pack_uneven_layout_splits_a_leaf_and_all_layouts_share_manifest() {
        let bytes = b"abcdefghijkl".to_vec();
        let fixture = build_file_series(&bytes, &[5, 7]);

        let whole_layout = FilePackLayout::new(100).expect("layout");
        let whole = build_file_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &whole_layout,
        )
        .expect("whole build");

        // Neither leaf boundary (5) nor the series end (12) lines up with
        // an object boundary (4, 8, 12): both leaves cross at least one
        // physical-object seam.
        let uneven_layout = FilePackLayout::new(4).expect("layout");
        let uneven = build_file_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &uneven_layout,
        )
        .expect("uneven build");

        assert_eq!(whole.physical_objects.len(), 1);
        assert_eq!(uneven.physical_objects.len(), 3);
        // Different physical layouts, different pack hashes...
        assert_ne!(whole.index.hash(), uneven.index.hash());
        let whole_hashes: std::collections::HashSet<_> =
            whole.physical_objects.iter().map(|(h, _)| *h).collect();
        let uneven_hashes: std::collections::HashSet<_> =
            uneven.physical_objects.iter().map(|(h, _)| *h).collect();
        assert_ne!(whole_hashes, uneven_hashes);
        // ...but identical manifest hash, root, and metadata.
        assert_eq!(whole.index.series_hash(), uneven.index.series_hash());
        assert_eq!(whole.index.range_root(), uneven.index.range_root());
        assert_eq!(
            whole.index.total_leaf_count(),
            uneven.index.total_leaf_count()
        );
        assert_eq!(whole.index.logical_count(), uneven.index.logical_count());
        assert_eq!(fixture.manifest_hash, fixture.manifest.hash());
    }

    #[test]
    fn file_pack_repeated_repack_is_deterministic() {
        let bytes = b"abcdefghijkl".to_vec();
        let fixture = build_file_series(&bytes, &[5, 7]);
        let layout = FilePackLayout::new(4).expect("layout");
        let first = build_file_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &layout,
        )
        .expect("first build");
        let second = build_file_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &layout,
        )
        .expect("second build");
        assert_eq!(first.index, second.index);
        assert_eq!(first.physical_objects, second.physical_objects);
    }

    #[test]
    fn file_pack_rejects_zero_layout_limit() {
        let bytes = b"abcd".to_vec();
        let fixture = build_file_series(&bytes, &[4]);
        assert!(FilePackLayout::new(0).is_err());
        // Fall back to a valid layout to isolate the empty-leaves case below.
        let _ = fixture;
    }

    #[test]
    fn file_pack_rejects_empty_leaves_list() {
        let bytes = b"abcd".to_vec();
        let fixture = build_file_series(&bytes, &[4]);
        let layout = FilePackLayout::new(10).expect("layout");
        let err = build_file_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &[],
            &layout,
        )
        .expect_err("empty leaves must fail");
        assert!(err.contains("at least one logical leaf"));
    }

    #[test]
    fn file_leaf_input_rejects_empty_bytes() {
        assert!(FileLeafInput::new(Vec::new(), None, None, None).is_err());
    }

    #[test]
    fn file_pack_rejects_wrong_manifest_hash() {
        let bytes = b"abcd".to_vec();
        let fixture = build_file_series(&bytes, &[4]);
        let layout = FilePackLayout::new(10).expect("layout");
        let wrong_hash = ObjectHash::of_bytes(b"not the manifest hash");
        let err = build_file_pack(
            wrong_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &layout,
        )
        .expect_err("wrong manifest_hash must fail");
        assert!(err.contains("does not match the supplied manifest's own hash"));
    }

    #[test]
    fn file_pack_rejects_wrong_root() {
        let bytes = b"abcd".to_vec();
        let fixture = build_file_series(&bytes, &[4]);
        let layout = FilePackLayout::new(10).expect("layout");
        let mut tampered = fixture.leaf_hashes.clone();
        tampered[0] = ObjectHash::of_bytes(b"tampered");
        let err = build_file_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &tampered,
            0,
            &fixture.leaves,
            &layout,
        )
        .expect_err("wrong root must fail");
        assert!(err.contains("leaf_merkle_root"));
    }

    #[test]
    fn file_pack_rejects_wrong_count() {
        let bytes = b"abcd".to_vec();
        let fixture = build_file_series(&bytes, &[4]);
        let layout = FilePackLayout::new(10).expect("layout");
        let mut too_many = fixture.leaf_hashes.clone();
        too_many.push(ObjectHash::of_bytes(b"extra"));
        let err = build_file_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &too_many,
            0,
            &fixture.leaves,
            &layout,
        )
        .expect_err("wrong count must fail");
        assert!(err.contains("leaf_count"));
    }

    #[test]
    fn file_pack_rejects_wrong_kind() {
        let bytes = b"abcd".to_vec();
        let fixture = build_file_series(&bytes, &[4]);
        // A table-kind manifest with the same root shape.
        let root = whole_merkle_root(&fixture.leaf_hashes);
        let schema = i64_string_schema();
        let fingerprint = schema_fingerprint(&schema).expect("fingerprint");
        let table_manifest = SeriesManifest::new(
            PayloadKind::Table,
            Some(fingerprint),
            4,
            1,
            None,
            None,
            None,
            root,
        )
        .expect("table manifest");
        let table_manifest_hash = table_manifest.hash();
        let layout = FilePackLayout::new(10).expect("layout");
        let err = build_file_pack(
            table_manifest_hash,
            &table_manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &layout,
        )
        .expect_err("wrong kind must fail");
        assert!(err.contains("payload kind"));
    }

    #[test]
    fn file_pack_rejects_wrong_range() {
        let bytes = b"abcdefgh".to_vec();
        let fixture = build_file_series(&bytes, &[4, 4]);
        let layout = FilePackLayout::new(10).expect("layout");
        // leaf_start=1 with 2 leaves supplied reaches leaf_end=3, exceeding
        // the series' total leaf_count of 2.
        let err = build_file_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            1,
            &fixture.leaves,
            &layout,
        )
        .expect_err("wrong range must fail");
        assert!(err.contains("out of bounds"));
    }

    #[test]
    fn file_pack_rejects_wrong_leaf_hash() {
        let bytes = b"abcdefgh".to_vec();
        let fixture = build_file_series(&bytes, &[4, 4]);
        let layout = FilePackLayout::new(10).expect("layout");
        // A leaf whose real content hash cannot equal the whole-series
        // hash recorded for that position.
        let substituted = FileLeafInput::new(b"XXXX".to_vec(), None, None, None).expect("leaf");
        let leaves = vec![substituted, fixture.leaves[1].clone()];
        let err = build_file_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &leaves,
            &layout,
        )
        .expect_err("wrong leaf hash must fail");
        assert!(err.contains("does not match the whole-series leaf hash"));
    }

    #[test]
    fn file_pack_rejects_wrong_aggregate_bounds() {
        let bytes = b"abcd".to_vec();
        let leaf = FileLeafInput::new(bytes.clone(), Some(10), Some(20), None).expect("leaf");
        let leaf_hash = leaf.leaf_hash();
        let root = whole_merkle_root(&[leaf_hash]);
        // Manifest declares different aggregate bounds than the leaf
        // actually carries.
        let manifest = SeriesManifest::new(
            PayloadKind::File,
            None,
            4,
            1,
            Some(999),
            Some(1000),
            None,
            root,
        )
        .expect("manifest");
        let manifest_hash = manifest.hash();
        let layout = FilePackLayout::new(10).expect("layout");
        let err = build_file_pack(manifest_hash, &manifest, &[leaf_hash], 0, &[leaf], &layout)
            .expect_err("wrong aggregate bounds must fail");
        assert!(
            err.contains("aggregate min_event_time") || err.contains("aggregate max_event_time")
        );
    }

    #[test]
    fn file_pack_rejects_wrong_attrs() {
        let attrs = super::super::series_leaf::encode_canonical_attributes(r#"{"a":1}"#)
            .expect("canonical attrs");
        let bytes = b"abcd".to_vec();
        let real_leaf =
            FileLeafInput::new(bytes.clone(), None, None, Some(attrs.clone())).expect("real leaf");
        let real_hash = real_leaf.leaf_hash();
        let root = whole_merkle_root(&[real_hash]);
        let manifest = SeriesManifest::new(PayloadKind::File, None, 4, 1, None, None, None, root)
            .expect("manifest");
        let manifest_hash = manifest.hash();
        // Same bytes, different (also canonical) attributes: the recomputed
        // hash cannot match the real one.
        let other_attrs = super::super::series_leaf::encode_canonical_attributes(r#"{"a":2}"#)
            .expect("canonical attrs");
        let wrong_leaf =
            FileLeafInput::new(bytes, None, None, Some(other_attrs)).expect("wrong leaf");
        let layout = FilePackLayout::new(10).expect("layout");
        let err = build_file_pack(
            manifest_hash,
            &manifest,
            &[real_hash],
            0,
            &[wrong_leaf],
            &layout,
        )
        .expect_err("wrong attrs must fail");
        assert!(err.contains("does not match the whole-series leaf hash"));
    }

    #[test]
    fn file_pack_self_verifies_and_object_hashes_and_byte_count_match() {
        let bytes = b"abcdefghijkl".to_vec();
        let fixture = build_file_series(&bytes, &[5, 7]);
        let layout = FilePackLayout::new(4).expect("layout");
        let built = build_file_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &layout,
        )
        .expect("build");
        for (hash, obj_bytes) in &built.physical_objects {
            assert_eq!(*hash, ObjectHash::of_bytes(obj_bytes));
        }
        let sum: u64 = built
            .physical_objects
            .iter()
            .map(|(_, b)| b.len() as u64)
            .sum();
        assert_eq!(sum, built.index.physical_byte_count());
        assert_eq!(
            built.index.physical_object_hashes().len(),
            built.physical_objects.len()
        );
        // Already self-verified by `build_file_pack` itself; repeat here
        // explicitly so a future refactor cannot silently drop that call.
        let range_leaves: Vec<ObjectHash> = fixture.leaves.iter().map(|l| l.leaf_hash()).collect();
        verify_pack_against_manifest(
            fixture.manifest_hash,
            &fixture.manifest,
            &built.index,
            &range_leaves,
        )
        .expect("self-verification must still hold");
    }

    // -- Table pack: layouts, determinism, decode, verification --------

    #[test]
    fn table_pack_one_object_layout_verifies_and_decodes() {
        let schema = i64_string_schema();
        let rows: Vec<(i64, &str)> = vec![(1, "a"), (2, "b"), (3, "c"), (4, "d")];
        let fixture = build_table_series(&schema, &rows, &[2, 2]);
        let layout = TablePackLayout::new(100).expect("layout");
        let built = build_table_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &layout,
        )
        .expect("build table pack");
        assert_eq!(built.physical_objects.len(), 1);
        let (_, obj_bytes) = &built.physical_objects[0];
        let (_, decoded_batches) = decode_parquet(obj_bytes);
        let total_rows: usize = decoded_batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, rows.len());
        let ids: Vec<i64> = decoded_batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id column")
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, vec![1, 2, 3, 4]);
    }

    #[test]
    fn table_pack_allows_repeated_content_objects() {
        let schema = i64_string_schema();
        let rows = [(1, "x"), (2, "y"), (1, "x"), (2, "y")];
        let fixture = build_table_series(&schema, &rows, &[2, 2]);
        let built = build_table_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &TablePackLayout::new(2).unwrap(),
        )
        .expect("identical Parquet objects may be reused in stream order");
        assert_eq!(built.physical_objects.len(), 2);
        assert_eq!(
            built.index.physical_object_hashes()[0],
            built.index.physical_object_hashes()[1]
        );
    }

    #[test]
    fn table_pack_max_rows_layout_crosses_leaf_boundaries_and_preserves_rows() {
        let schema = i64_string_schema();
        let rows: Vec<(i64, &str)> = (1..=10).map(|i| (i, "row")).collect();
        // Leaves [3, 3, 4]; objects capped at 4 rows -> [4, 4, 2], crossing
        // leaf boundaries in both directions.
        let fixture = build_table_series(&schema, &rows, &[3, 3, 4]);
        let layout = TablePackLayout::new(4).expect("layout");
        let built = build_table_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &layout,
        )
        .expect("build table pack");
        assert_eq!(built.physical_objects.len(), 3);

        let mut all_ids: Vec<i64> = Vec::new();
        for (_, obj_bytes) in &built.physical_objects {
            let (_, decoded_batches) = decode_parquet(obj_bytes);
            for b in &decoded_batches {
                let ids = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id column")
                    .values()
                    .to_vec();
                all_ids.extend(ids);
            }
        }
        let expected_ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
        assert_eq!(all_ids, expected_ids);
    }

    #[test]
    fn table_pack_different_layouts_share_manifest_but_differ_physically() {
        let schema = i64_string_schema();
        let rows: Vec<(i64, &str)> = (1..=10).map(|i| (i, "row")).collect();
        let fixture = build_table_series(&schema, &rows, &[3, 3, 4]);

        let whole_layout = TablePackLayout::new(100).expect("layout");
        let whole = build_table_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &whole_layout,
        )
        .expect("whole build");

        let split_layout = TablePackLayout::new(4).expect("layout");
        let split = build_table_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &split_layout,
        )
        .expect("split build");

        assert_eq!(whole.physical_objects.len(), 1);
        assert_eq!(split.physical_objects.len(), 3);
        assert_ne!(whole.index.hash(), split.index.hash());
        assert_eq!(whole.index.series_hash(), split.index.series_hash());
        assert_eq!(whole.index.range_root(), split.index.range_root());
        assert_eq!(whole.index.logical_count(), split.index.logical_count());
    }

    #[test]
    fn table_pack_repeated_repack_is_deterministic() {
        let schema = i64_string_schema();
        let rows: Vec<(i64, &str)> = (1..=10).map(|i| (i, "row")).collect();
        let fixture = build_table_series(&schema, &rows, &[3, 3, 4]);
        let layout = TablePackLayout::new(4).expect("layout");
        let first = build_table_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &layout,
        )
        .expect("first build");
        let second = build_table_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &layout,
        )
        .expect("second build");
        assert_eq!(first.index, second.index);
        assert_eq!(first.physical_objects, second.physical_objects);
    }

    #[test]
    fn table_pack_rejects_zero_layout_limit() {
        assert!(TablePackLayout::new(0).is_err());
    }

    #[test]
    fn table_pack_rejects_empty_leaves_list() {
        let schema = i64_string_schema();
        let rows: Vec<(i64, &str)> = vec![(1, "a")];
        let fixture = build_table_series(&schema, &rows, &[1]);
        let layout = TablePackLayout::new(10).expect("layout");
        let err = build_table_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &[],
            &layout,
        )
        .expect_err("empty leaves must fail");
        assert!(err.contains("at least one logical leaf"));
    }

    #[test]
    fn table_leaf_input_rejects_empty_batches() {
        let schema = i64_string_schema();
        let empty_batch = batch(&schema, &[], &[]);
        let err = TableLeafInput::new(schema, vec![empty_batch], None, None, None)
            .expect_err("zero rows must fail");
        assert!(err.contains("at least one row"));
    }

    #[test]
    fn table_pack_rejects_wrong_manifest_hash() {
        let schema = i64_string_schema();
        let rows: Vec<(i64, &str)> = vec![(1, "a")];
        let fixture = build_table_series(&schema, &rows, &[1]);
        let layout = TablePackLayout::new(10).expect("layout");
        let wrong_hash = ObjectHash::of_bytes(b"not it");
        let err = build_table_pack(
            wrong_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &layout,
        )
        .expect_err("wrong manifest hash must fail");
        assert!(err.contains("does not match the supplied manifest's own hash"));
    }

    #[test]
    fn table_pack_rejects_wrong_root() {
        let schema = i64_string_schema();
        let rows: Vec<(i64, &str)> = vec![(1, "a"), (2, "b")];
        let fixture = build_table_series(&schema, &rows, &[1, 1]);
        let layout = TablePackLayout::new(10).expect("layout");
        let mut tampered = fixture.leaf_hashes.clone();
        tampered[0] = ObjectHash::of_bytes(b"tampered");
        let err = build_table_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &tampered,
            0,
            &fixture.leaves,
            &layout,
        )
        .expect_err("wrong root must fail");
        assert!(err.contains("leaf_merkle_root"));
    }

    #[test]
    fn table_pack_rejects_wrong_count() {
        let schema = i64_string_schema();
        let rows: Vec<(i64, &str)> = vec![(1, "a")];
        let fixture = build_table_series(&schema, &rows, &[1]);
        let layout = TablePackLayout::new(10).expect("layout");
        let mut too_many = fixture.leaf_hashes.clone();
        too_many.push(ObjectHash::of_bytes(b"extra"));
        let err = build_table_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &too_many,
            0,
            &fixture.leaves,
            &layout,
        )
        .expect_err("wrong count must fail");
        assert!(err.contains("leaf_count"));
    }

    #[test]
    fn table_pack_rejects_wrong_kind() {
        let schema = i64_string_schema();
        let rows: Vec<(i64, &str)> = vec![(1, "a")];
        let fixture = build_table_series(&schema, &rows, &[1]);
        let root = whole_merkle_root(&fixture.leaf_hashes);
        let file_manifest =
            SeriesManifest::new(PayloadKind::File, None, 1, 1, None, None, None, root)
                .expect("file manifest");
        let file_manifest_hash = file_manifest.hash();
        let layout = TablePackLayout::new(10).expect("layout");
        let err = build_table_pack(
            file_manifest_hash,
            &file_manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &layout,
        )
        .expect_err("wrong kind must fail");
        assert!(err.contains("payload kind"));
    }

    #[test]
    fn table_pack_rejects_wrong_schema() {
        let schema = i64_string_schema();
        let rows: Vec<(i64, &str)> = vec![(1, "a")];
        let fixture = build_table_series(&schema, &rows, &[1]);
        let other_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let other_fingerprint = schema_fingerprint(&other_schema).expect("fingerprint");
        let root = whole_merkle_root(&fixture.leaf_hashes);
        let manifest = SeriesManifest::new(
            PayloadKind::Table,
            Some(other_fingerprint),
            1,
            1,
            None,
            None,
            None,
            root,
        )
        .expect("manifest with a different schema fingerprint");
        let manifest_hash = manifest.hash();
        let layout = TablePackLayout::new(10).expect("layout");
        let err = build_table_pack(
            manifest_hash,
            &manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &layout,
        )
        .expect_err("wrong schema must fail");
        assert!(err.contains("schema_fingerprint"));
    }

    #[test]
    fn table_pack_rejects_leaf_with_different_schema_than_other_leaves() {
        let schema = i64_string_schema();
        let rows: Vec<(i64, &str)> = vec![(1, "a"), (2, "b")];
        let fixture = build_table_series(&schema, &rows, &[1, 1]);
        let other_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let other_batch = RecordBatch::try_new(
            other_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![2]))],
        )
        .expect("other batch");
        let other_leaf = TableLeafInput::new(other_schema, vec![other_batch], None, None, None)
            .expect("other leaf input");
        let leaves = vec![fixture.leaves[0].clone(), other_leaf];
        let layout = TablePackLayout::new(10).expect("layout");
        let err = build_table_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &leaves,
            &layout,
        )
        .expect_err("mismatched leaf schemas must fail");
        assert!(err.contains("must share one canonical schema"));
    }

    #[test]
    fn table_pack_rejects_wrong_range() {
        let schema = i64_string_schema();
        let rows: Vec<(i64, &str)> = vec![(1, "a"), (2, "b")];
        let fixture = build_table_series(&schema, &rows, &[1, 1]);
        let layout = TablePackLayout::new(10).expect("layout");
        let err = build_table_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            1,
            &fixture.leaves,
            &layout,
        )
        .expect_err("wrong range must fail");
        assert!(err.contains("out of bounds"));
    }

    #[test]
    fn table_pack_rejects_wrong_leaf_hash() {
        let schema = i64_string_schema();
        let rows: Vec<(i64, &str)> = vec![(1, "a"), (2, "b")];
        let fixture = build_table_series(&schema, &rows, &[1, 1]);
        let layout = TablePackLayout::new(10).expect("layout");
        let substituted_batch = batch(&schema, &[99], &["zz"]);
        let substituted =
            TableLeafInput::new(schema.clone(), vec![substituted_batch], None, None, None)
                .expect("substituted leaf");
        let leaves = vec![substituted, fixture.leaves[1].clone()];
        let err = build_table_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &leaves,
            &layout,
        )
        .expect_err("wrong leaf hash must fail");
        assert!(err.contains("does not match the whole-series leaf hash"));
    }

    #[test]
    fn table_pack_rejects_wrong_aggregate_bounds() {
        let schema = i64_string_schema();
        let b = batch(&schema, &[1], &["a"]);
        let leaf = TableLeafInput::new(schema, vec![b], Some(10), Some(20), None)
            .expect("leaf with bounds");
        let leaf_hash = leaf.leaf_hash();
        let fingerprint = leaf.schema_fingerprint();
        let root = whole_merkle_root(&[leaf_hash]);
        let manifest = SeriesManifest::new(
            PayloadKind::Table,
            Some(fingerprint),
            1,
            1,
            Some(999),
            Some(1000),
            None,
            root,
        )
        .expect("manifest with wrong aggregate bounds");
        let manifest_hash = manifest.hash();
        let layout = TablePackLayout::new(10).expect("layout");
        let err = build_table_pack(manifest_hash, &manifest, &[leaf_hash], 0, &[leaf], &layout)
            .expect_err("wrong aggregate bounds must fail");
        assert!(
            err.contains("aggregate min_event_time") || err.contains("aggregate max_event_time")
        );
    }

    #[test]
    fn table_pack_self_verifies_and_object_hashes_and_byte_count_match() {
        let schema = i64_string_schema();
        let rows: Vec<(i64, &str)> = (1..=10).map(|i| (i, "row")).collect();
        let fixture = build_table_series(&schema, &rows, &[3, 3, 4]);
        let layout = TablePackLayout::new(4).expect("layout");
        let built = build_table_pack(
            fixture.manifest_hash,
            &fixture.manifest,
            &fixture.leaf_hashes,
            0,
            &fixture.leaves,
            &layout,
        )
        .expect("build");
        for (hash, obj_bytes) in &built.physical_objects {
            assert_eq!(*hash, ObjectHash::of_bytes(obj_bytes));
        }
        let sum: u64 = built
            .physical_objects
            .iter()
            .map(|(_, b)| b.len() as u64)
            .sum();
        assert_eq!(sum, built.index.physical_byte_count());
        let unique: std::collections::HashSet<_> =
            built.physical_objects.iter().map(|(h, _)| *h).collect();
        assert_eq!(unique.len(), built.physical_objects.len());
        let range_leaves: Vec<ObjectHash> = fixture.leaves.iter().map(|l| l.leaf_hash()).collect();
        verify_pack_against_manifest(
            fixture.manifest_hash,
            &fixture.manifest,
            &built.index,
            &range_leaves,
        )
        .expect("self-verification must still hold");
    }
}
