// SPDX-License-Identifier: Apache-2.0

//! `watertown.series-pack.v4` pack index: a physical pack's proof of membership in a
//! logical series, and the verification helper that checks a pack against a
//! decoded series manifest.
//!
//! `docs/logical-series-identity-design.md` delivery gate 2. A [`PackIndex`]
//! is deliberately excluded from logical content identity -- it is derived
//! storage metadata, like Delta file layout -- but it must still prove that
//! the physical objects it names really do reconstruct a contiguous,
//! ordered range of the logical series named by [`PackIndex::series_hash`].
//! That proof is exactly a [`super::series_merkle::RangeProof`]: nothing
//! about Parquet, Bao, or physical byte layout enters this module.
//!
//! # A self-consistent range root alone is insufficient
//!
//! A pack could construct an internally self-consistent
//! `(range_root, range_proof)` pair for the *wrong* series (or for
//! fabricated leaves that never appeared in any real series) and that pair
//! would still fold correctly on its own. What actually binds a pack to one
//! real, published series is [`verify_pack_against_manifest`] checking the
//! computed root against an **independently fetched**
//! [`super::series_manifest::SeriesManifest`]'s own leaf Merkle root -- not
//! merely re-deriving the pack's own declared fields from each other. See
//! that function's doc for the exact four checks it performs.
//!
//! # Per-leaf descriptors and the physical object stream
//!
//! A [`PackIndex`] carries exactly one [`PackLeafDescriptor`] for each
//! logical leaf in `[leaf_start, leaf_end)`, in leaf order -- the accepted
//! per-leaf descriptor model for this pack codec. Each descriptor carries
//! the authenticated logical leaf hash plus independent per-leaf metadata
//! (logical row/byte count, optional
//! min/max event-time bounds, optional canonical logical attributes); it
//! carries no physical byte offsets.
//!
//! [`PackIndex::object_spans`] records the ordered physical objects together
//! with each object's pack-relative logical interval (bytes for files, rows
//! for tables) and cumulative physical-byte interval.  Logical object
//! boundaries remain independent of leaf boundaries, but the explicit spans
//! let an incremental reader omit objects wholly contained in a locally-held,
//! authenticated leaf prefix.
//!
//! # Linked series states
//!
//! A canonical append segment additionally names `parent_series_hash`, the
//! immutable manifest for the complete prefix ending at `leaf_start`. Root and
//! explicitly consolidated segments have no parent and cover the whole range.
//! The link is part of these content-addressed pack bytes, so old segment
//! metadata is never rewritten when a series grows.

use std::collections::BTreeMap;

use super::series_leaf::{LEAF_HAS_MAX, LEAF_HAS_MIN, validate_canonical_attributes};
use super::series_manifest::{PayloadKind, SeriesManifest};
use super::series_merkle::{
    MerkleFrontier, RangeProof, decode_range_proof, encode_range_proof, verify_range_proof,
};
use super::{Cursor, ObjectHash, push_len_prefixed};

/// Magic header for the current pack-index wire format.
const PACK_MAGIC: &[u8] = b"watertown.series-pack.v4\n";

/// Known `bounds_flags` bits for a [`PackLeafDescriptor`]; any other bit set
/// is a decode error, matching [`super::series_leaf`]'s and
/// [`super::series_manifest`]'s "unknown flag" convention.
const KNOWN_DESCRIPTOR_BOUNDS_FLAGS: u8 = LEAF_HAS_MIN | LEAF_HAS_MAX;

/// The smallest number of bytes one encoded [`PackLeafDescriptor`] can occupy
/// on the wire: a `u64` logical count, a `u8` bounds-flags byte, and a `u32`
/// (zero) logical-attributes length, with no bounds or attribute bytes
/// present. Used to bound a hostile descriptor count's pre-allocation.
const MIN_DESCRIPTOR_WIRE_BYTES: usize = 32 + 8 + 4 + 1 + 4;

/// One encoded [`PackObjectSpan`]: hash plus four `u64` interval endpoints.
const OBJECT_SPAN_WIRE_BYTES: usize = 32 + 8 + 8 + 8 + 8;

/// One logical leaf's per-leaf metadata within a `watertown.series-pack.v4` pack
/// index: exactly one descriptor for each logical leaf in
/// `[leaf_start, leaf_end)`, in leaf order.
///
/// A descriptor carries the leaf's own logical row/byte count, independently
/// optional min/max event-time bounds, and optional canonical logical-
/// attribute JSON bytes -- the same per-leaf metadata shape as
/// [`super::series_leaf::table_leaf_hash`]/[`super::series_leaf::file_leaf_hash`],
/// but recorded here (not hashed into the logical leaf itself) so a reader
/// can partition decoded physical content into leaves and answer metadata
/// queries (event-time bounds, attributes) without decoding every physical
/// object. See the module docs for how a reader is expected to use these
/// against the physical object stream.
///
/// Fields are private and only reachable through validated v3 construction
/// ([`PackLeafDescriptor::new_with_leaf_hash`] or
/// [`PackLeafDescriptor::new_with_leaf_hash_and_schema`]) or strict decode
/// (via [`PackIndex::decode`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackLeafDescriptor {
    logical_leaf_hash: ObjectHash,
    logical_count: u64,
    schema_fingerprint: Option<ObjectHash>,
    min_event_time: Option<i64>,
    max_event_time: Option<i64>,
    logical_attributes: Option<Vec<u8>>,
}

struct DecodedDescriptorMetadata {
    min_event_time: Option<i64>,
    max_event_time: Option<i64>,
    logical_attributes: Option<Vec<u8>>,
}

impl PackLeafDescriptor {
    /// Construct a v3 descriptor carrying its logical leaf hash.
    pub fn new_with_leaf_hash(
        logical_leaf_hash: ObjectHash,
        logical_count: u64,
        min_event_time: Option<i64>,
        max_event_time: Option<i64>,
        logical_attributes: Option<Vec<u8>>,
    ) -> Result<Self, String> {
        validate_descriptor(logical_count, &logical_attributes)?;
        Ok(Self {
            logical_leaf_hash,
            logical_count,
            schema_fingerprint: None,
            min_event_time,
            max_event_time,
            logical_attributes,
        })
    }

    /// Construct a descriptor for the v4 pack codec, carrying its logical
    /// leaf hash and optionally its schema fingerprint.
    pub fn new_with_leaf_hash_and_schema(
        logical_leaf_hash: ObjectHash,
        logical_count: u64,
        schema_fingerprint: Option<ObjectHash>,
        min_event_time: Option<i64>,
        max_event_time: Option<i64>,
        logical_attributes: Option<Vec<u8>>,
    ) -> Result<Self, String> {
        validate_descriptor(logical_count, &logical_attributes)?;
        Ok(Self {
            logical_leaf_hash,
            logical_count,
            schema_fingerprint,
            min_event_time,
            max_event_time,
            logical_attributes,
        })
    }

    /// Return a clone carrying `schema_fingerprint`.
    pub fn with_schema_fingerprint(&self, schema_fingerprint: ObjectHash) -> Self {
        let mut descriptor = self.clone();
        descriptor.schema_fingerprint = Some(schema_fingerprint);
        descriptor
    }

    /// Return a clone carrying the authenticated logical leaf hash.
    #[must_use]
    pub fn with_logical_leaf_hash(&self, logical_leaf_hash: ObjectHash) -> Self {
        let mut descriptor = self.clone();
        descriptor.logical_leaf_hash = logical_leaf_hash;
        descriptor
    }

    /// This leaf's logical identity hash.
    #[must_use]
    pub fn logical_leaf_hash(&self) -> ObjectHash {
        self.logical_leaf_hash
    }

    /// This leaf's logical row (table) or byte (file) count.
    #[must_use]
    pub fn logical_count(&self) -> u64 {
        self.logical_count
    }

    /// The schema fingerprint intrinsically carried by a v3 descriptor.
    ///
    /// Table descriptors carry `Some`; file descriptors carry `None`.
    #[must_use]
    pub fn schema_fingerprint(&self) -> Option<ObjectHash> {
        self.schema_fingerprint
    }

    /// This leaf's minimum event time, if it carried one.
    #[must_use]
    pub fn min_event_time(&self) -> Option<i64> {
        self.min_event_time
    }

    /// This leaf's maximum event time, if it carried one.
    #[must_use]
    pub fn max_event_time(&self) -> Option<i64> {
        self.max_event_time
    }

    /// This leaf's canonical logical attributes bytes, if any were set.
    #[must_use]
    pub fn logical_attributes(&self) -> Option<&[u8]> {
        self.logical_attributes.as_deref()
    }

    fn encode_into(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.logical_leaf_hash.as_bytes());
        buf.extend_from_slice(&self.logical_count.to_le_bytes());
        match self.schema_fingerprint {
            Some(fingerprint) => push_len_prefixed(buf, fingerprint.as_bytes()),
            None => push_len_prefixed(buf, &[]),
        }
        self.encode_bounds_and_attributes(buf);
    }

    fn encode_bounds_and_attributes(&self, buf: &mut Vec<u8>) {
        let mut flags = 0u8;
        if self.min_event_time.is_some() {
            flags |= LEAF_HAS_MIN;
        }
        if self.max_event_time.is_some() {
            flags |= LEAF_HAS_MAX;
        }
        buf.push(flags);
        if let Some(v) = self.min_event_time {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        if let Some(v) = self.max_event_time {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        match &self.logical_attributes {
            Some(attrs) => push_len_prefixed(buf, attrs),
            None => push_len_prefixed(buf, &[]),
        }
    }

    /// Decode one current-format descriptor from `cur`.
    fn decode_from(cur: &mut Cursor<'_>) -> Result<Self, String> {
        let logical_leaf_hash = cur.take_hash()?;
        let logical_count = cur.take_u64()?;
        let schema_bytes = cur.take_len_prefixed()?;
        let schema_fingerprint = if schema_bytes.is_empty() {
            None
        } else if schema_bytes.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(schema_bytes);
            Some(ObjectHash::from_bytes(arr))
        } else {
            return Err(format!(
                "pack leaf schema fingerprint must be 0 or 32 bytes, got {}",
                schema_bytes.len()
            ));
        };
        let metadata = Self::decode_bounds_and_attributes(cur)?;
        Self::new_with_leaf_hash_and_schema(
            logical_leaf_hash,
            logical_count,
            schema_fingerprint,
            metadata.min_event_time,
            metadata.max_event_time,
            metadata.logical_attributes,
        )
    }

    fn decode_bounds_and_attributes(
        cur: &mut Cursor<'_>,
    ) -> Result<DecodedDescriptorMetadata, String> {
        let flags = cur.take_u8()?;
        if flags & !KNOWN_DESCRIPTOR_BOUNDS_FLAGS != 0 {
            return Err(format!(
                "unknown pack leaf descriptor bounds flags: {flags:#04x}"
            ));
        }

        let min_event_time = if flags & LEAF_HAS_MIN != 0 {
            Some(cur.take_i64()?)
        } else {
            None
        };
        let max_event_time = if flags & LEAF_HAS_MAX != 0 {
            Some(cur.take_i64()?)
        } else {
            None
        };
        let attrs_bytes = cur.take_len_prefixed()?;
        let logical_attributes = if attrs_bytes.is_empty() {
            None
        } else {
            Some(attrs_bytes.to_vec())
        };
        Ok(DecodedDescriptorMetadata {
            min_event_time,
            max_event_time,
            logical_attributes,
        })
    }
}

/// One physical object's position in a pack's logical and encoded streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackObjectSpan {
    object_hash: ObjectHash,
    logical_start: u64,
    logical_end: u64,
    physical_start: u64,
    physical_end: u64,
}

impl PackObjectSpan {
    /// Construct one nonempty object span.
    pub fn new(
        object_hash: ObjectHash,
        logical_start: u64,
        logical_end: u64,
        physical_start: u64,
        physical_end: u64,
    ) -> Result<Self, String> {
        if logical_start >= logical_end {
            return Err(format!(
                "pack object logical span must be nonempty: [{logical_start}, {logical_end})"
            ));
        }
        if physical_start >= physical_end {
            return Err(format!(
                "pack object physical span must be nonempty: [{physical_start}, {physical_end})"
            ));
        }
        Ok(Self {
            object_hash,
            logical_start,
            logical_end,
            physical_start,
            physical_end,
        })
    }

    #[must_use]
    /// Return the content hash naming this physical object.
    pub fn object_hash(&self) -> ObjectHash {
        self.object_hash
    }

    #[must_use]
    /// Return the inclusive start in the pack-relative logical stream.
    pub fn logical_start(&self) -> u64 {
        self.logical_start
    }

    #[must_use]
    /// Return the exclusive end in the pack-relative logical stream.
    pub fn logical_end(&self) -> u64 {
        self.logical_end
    }

    #[must_use]
    /// Return the inclusive start in the concatenated physical byte stream.
    pub fn physical_start(&self) -> u64 {
        self.physical_start
    }

    #[must_use]
    /// Return the exclusive end in the concatenated physical byte stream.
    pub fn physical_end(&self) -> u64 {
        self.physical_end
    }

    #[must_use]
    /// Return this object's encoded byte length.
    pub fn physical_len(&self) -> u64 {
        self.physical_end - self.physical_start
    }
}

/// Shared invariant checks for descriptor construction and
/// [`PackLeafDescriptor::decode_from`].
fn validate_descriptor(
    logical_count: u64,
    logical_attributes: &Option<Vec<u8>>,
) -> Result<(), String> {
    if logical_count == 0 {
        return Err(
            "pack leaf descriptor logical_count must be positive (empty leaves are not a supported model)"
                .to_string(),
        );
    }
    if let Some(attrs) = logical_attributes {
        if attrs.is_empty() {
            return Err(
                "pack leaf descriptor logical attributes must be None, not Some(&[]), to mean \"absent\""
                    .to_string(),
            );
        }
        validate_canonical_attributes(attrs)?;
    }
    Ok(())
}

/// A `watertown.series-pack.v4` pack index: one contiguous logical-leaf range,
/// covered by one or more physical objects, together with its membership
/// proof against a named series.
///
/// `[leaf_start, leaf_end)` is a half-open range: `leaf_end` is exclusive,
/// matching Rust's own range convention and [`super::series_merkle`]'s
/// `start`/`end` parameters.
///
/// Fields are private and only reachable through validated construction
/// ([`PackIndex::new_with_spans`]) or strict decode ([`PackIndex::decode`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackIndex {
    series_hash: ObjectHash,
    parent_series_hash: Option<ObjectHash>,
    leaf_start: u64,
    leaf_end: u64,
    total_leaf_count: u64,
    range_root: ObjectHash,
    range_proof: RangeProof,
    object_spans: Vec<PackObjectSpan>,
    physical_object_hashes: Vec<ObjectHash>,
    logical_count: u64,
    physical_byte_count: u64,
    leaf_descriptors: Vec<PackLeafDescriptor>,
}

impl PackIndex {
    /// Construct a strictly validated v4 pack index.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_spans(
        series_hash: ObjectHash,
        leaf_start: u64,
        leaf_end: u64,
        total_leaf_count: u64,
        range_root: ObjectHash,
        range_proof: RangeProof,
        object_spans: Vec<PackObjectSpan>,
        logical_count: u64,
        physical_byte_count: u64,
        leaf_descriptors: Vec<PackLeafDescriptor>,
    ) -> Result<Self, String> {
        Self::new_segment_with_spans(
            series_hash,
            None,
            leaf_start,
            leaf_end,
            total_leaf_count,
            range_root,
            range_proof,
            object_spans,
            logical_count,
            physical_byte_count,
            leaf_descriptors,
        )
    }

    /// Construct a validated pack index that may link this series state to
    /// the immutable prior series state whose leaf prefix it extends.
    ///
    /// Generic derived packs may leave `parent_series_hash` absent. A pack
    /// installed as a canonical series locator must additionally pass
    /// [`Self::validate_series_segment`].
    #[allow(clippy::too_many_arguments)]
    pub fn new_segment_with_spans(
        series_hash: ObjectHash,
        parent_series_hash: Option<ObjectHash>,
        leaf_start: u64,
        leaf_end: u64,
        total_leaf_count: u64,
        range_root: ObjectHash,
        range_proof: RangeProof,
        object_spans: Vec<PackObjectSpan>,
        logical_count: u64,
        physical_byte_count: u64,
        leaf_descriptors: Vec<PackLeafDescriptor>,
    ) -> Result<Self, String> {
        if parent_series_hash == Some(series_hash) {
            return Err("pack parent_series_hash must not equal series_hash".to_string());
        }
        validate(
            leaf_start,
            leaf_end,
            total_leaf_count,
            range_root,
            &range_proof,
            &object_spans,
            &leaf_descriptors,
            logical_count,
            physical_byte_count,
        )?;
        let physical_object_hashes = object_spans.iter().map(|span| span.object_hash).collect();
        Ok(Self {
            series_hash,
            parent_series_hash,
            leaf_start,
            leaf_end,
            total_leaf_count,
            range_root,
            range_proof,
            object_spans,
            physical_object_hashes,
            logical_count,
            physical_byte_count,
            leaf_descriptors,
        })
    }

    /// The `watertown.series.v3` object hash this pack claims to belong to.
    #[must_use]
    pub fn series_hash(&self) -> ObjectHash {
        self.series_hash
    }

    /// Immutable prior series state whose complete leaf sequence precedes
    /// this pack's range.
    #[must_use]
    pub fn parent_series_hash(&self) -> Option<ObjectHash> {
        self.parent_series_hash
    }

    /// Validate that this pack can serve as one canonical linked series
    /// segment.
    ///
    /// A root segment (including an explicit consolidated pack) covers the
    /// complete series and has no parent. An append segment covers the
    /// nonempty suffix ending at the current series tip and names its prior
    /// immutable series state.
    pub fn validate_series_segment(&self) -> Result<(), String> {
        if self.leaf_end != self.total_leaf_count {
            return Err(format!(
                "canonical series segment must end at total_leaf_count {}, got leaf_end {}",
                self.total_leaf_count, self.leaf_end
            ));
        }
        match self.parent_series_hash {
            None if self.leaf_start == 0 => Ok(()),
            None => Err(format!(
                "root series segment must start at leaf 0, got {}",
                self.leaf_start
            )),
            Some(_) if self.leaf_start > 0 => Ok(()),
            Some(_) => Err("append series segment must start after leaf 0".to_string()),
        }
    }

    /// The first logical leaf index this pack covers (inclusive).
    #[must_use]
    pub fn leaf_start(&self) -> u64 {
        self.leaf_start
    }

    /// One past the last logical leaf index this pack covers (exclusive).
    #[must_use]
    pub fn leaf_end(&self) -> u64 {
        self.leaf_end
    }

    /// The total leaf count of the series this pack claims to belong to.
    #[must_use]
    pub fn total_leaf_count(&self) -> u64 {
        self.total_leaf_count
    }

    /// The whole-series root `range_proof` must reduce this range's actual
    /// leaf hashes to. See [`PackIndex::new_with_spans`]'s docs for why this is not a
    /// standalone range-only hash.
    #[must_use]
    pub fn range_root(&self) -> ObjectHash {
        self.range_root
    }

    /// The membership proof binding `[leaf_start, leaf_end)` to
    /// `range_root`.
    #[must_use]
    pub fn range_proof(&self) -> &RangeProof {
        &self.range_proof
    }

    /// The ordered physical object hashes composing this pack.
    ///
    /// Hashes may repeat when identical content occurs more than once.
    #[must_use]
    pub fn physical_object_hashes(&self) -> &[ObjectHash] {
        &self.physical_object_hashes
    }

    /// Ordered physical objects and their pack-relative logical/physical spans.
    #[must_use]
    pub fn object_spans(&self) -> &[PackObjectSpan] {
        &self.object_spans
    }

    /// The declared logical count this pack covers: total rows (table) or
    /// total bytes (file). Not cross-checked against decoded content by this
    /// module; see [`verify_pack_against_manifest`]'s docs for exactly what
    /// is and is not proven.
    #[must_use]
    pub fn logical_count(&self) -> u64 {
        self.logical_count
    }

    /// The declared total physical byte count of every object this pack
    /// names.
    #[must_use]
    pub fn physical_byte_count(&self) -> u64 {
        self.physical_byte_count
    }

    /// The ordered per-leaf descriptors: exactly one for each logical leaf
    /// in `[leaf_start, leaf_end)`, in leaf order. See the module docs for
    /// how a reader is expected to use these to partition the decoded
    /// physical object stream into leaves.
    #[must_use]
    pub fn leaf_descriptors(&self) -> &[PackLeafDescriptor] {
        &self.leaf_descriptors
    }

    /// Serialize this pack index into its `watertown.series-pack.v4` wire bytes:
    ///
    /// ```text
    /// PACK_MAGIC
    /// 32      series_hash
    /// u8      parent_series_hash presence
    /// [32]    parent_series_hash
    /// u64 LE  leaf_start
    /// u64 LE  leaf_end
    /// u64 LE  total_leaf_count
    /// 32      range_root
    /// u32 LE  range_proof length + encoded range proof bytes
    /// u64 LE  logical_count
    /// u64 LE  physical_byte_count
    /// u32 LE  leaf descriptor count, then that many descriptors:
    ///           32      logical_leaf_hash
    ///           u64 LE  logical_count
    ///           u32 LE  schema_fingerprint length (0 or 32) + bytes
    ///           u8      bounds_flags
    ///           [i64 LE min_event_time]
    ///           [i64 LE max_event_time]
    ///           u32 LE  logical_attributes length (0 = absent) + bytes
    /// u32 LE  object span count, then that many spans:
    ///           32      object_hash
    ///           u64 LE  logical_start
    ///           u64 LE  logical_end
    ///           u64 LE  physical_start
    ///           u64 LE  physical_end
    /// ```
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let proof_bytes = encode_range_proof(&self.range_proof);
        let mut buf = Vec::with_capacity(
            PACK_MAGIC.len()
                + 32
                + 1
                + usize::from(self.parent_series_hash.is_some()) * 32
                + 8
                + 8
                + 8
                + 32
                + 4
                + proof_bytes.len()
                + 8
                + 8
                + 4
                + self.leaf_descriptors.len() * MIN_DESCRIPTOR_WIRE_BYTES
                + 4
                + self.object_spans.len() * OBJECT_SPAN_WIRE_BYTES,
        );
        buf.extend_from_slice(PACK_MAGIC);
        buf.extend_from_slice(self.series_hash.as_bytes());
        match self.parent_series_hash {
            Some(parent) => {
                buf.push(1);
                buf.extend_from_slice(parent.as_bytes());
            }
            None => buf.push(0),
        }
        buf.extend_from_slice(&self.leaf_start.to_le_bytes());
        buf.extend_from_slice(&self.leaf_end.to_le_bytes());
        buf.extend_from_slice(&self.total_leaf_count.to_le_bytes());
        buf.extend_from_slice(self.range_root.as_bytes());
        push_len_prefixed(&mut buf, &proof_bytes);
        buf.extend_from_slice(&self.logical_count.to_le_bytes());
        buf.extend_from_slice(&self.physical_byte_count.to_le_bytes());
        let descriptor_count = u32::try_from(self.leaf_descriptors.len())
            .expect("pack leaf descriptor count exceeds u32::MAX");
        buf.extend_from_slice(&descriptor_count.to_le_bytes());
        for descriptor in &self.leaf_descriptors {
            descriptor.encode_into(&mut buf);
        }
        let object_count =
            u32::try_from(self.object_spans.len()).expect("physical object count exceeds u32::MAX");
        buf.extend_from_slice(&object_count.to_le_bytes());
        for span in &self.object_spans {
            buf.extend_from_slice(span.object_hash.as_bytes());
            buf.extend_from_slice(&span.logical_start.to_le_bytes());
            buf.extend_from_slice(&span.logical_end.to_le_bytes());
            buf.extend_from_slice(&span.physical_start.to_le_bytes());
            buf.extend_from_slice(&span.physical_end.to_le_bytes());
        }
        buf
    }

    /// This pack index's own content address: `blake3` of
    /// [`PackIndex::encode`].
    #[must_use]
    pub fn hash(&self) -> ObjectHash {
        ObjectHash::of_bytes(&self.encode())
    }

    /// Decode a `watertown.series-pack.v4` pack index (the inverse of
    /// [`PackIndex::encode`]), applying the same invariants as
    /// [`PackIndex::new_with_spans`].
    ///
    /// # Errors
    ///
    /// Returns an error if the magic header is wrong, the buffer is
    /// truncated or has trailing bytes, the embedded range proof fails its
    /// own strict decode (see
    /// [`super::series_merkle::decode_range_proof`]), a leaf descriptor's
    /// `bounds_flags` has an unknown bit set or its logical attributes are
    /// not canonical JSON, or any of [`PackIndex::new_with_spans`]'s invariants fail
    /// (nonempty/in-bounds range, a nonempty and duplicate-free physical
    /// object list, exactly one descriptor per leaf, descriptor counts
    /// summing to `logical_count`).
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let mut cur = Cursor::new(bytes);
        cur.expect_tag(PACK_MAGIC)?;
        let series_hash = cur.take_hash()?;
        let parent_series_hash = match cur.take_u8()? {
            0 => None,
            1 => Some(cur.take_hash()?),
            other => return Err(format!("invalid pack parent flag {other}")),
        };
        let leaf_start = cur.take_u64()?;
        let leaf_end = cur.take_u64()?;
        let total_leaf_count = cur.take_u64()?;
        let range_root = cur.take_hash()?;
        let proof_bytes = cur.take_len_prefixed()?;
        let leaf_start_usize = usize::try_from(leaf_start)
            .map_err(|_| "leaf_start does not fit in usize on this platform".to_string())?;
        let leaf_end_usize = usize::try_from(leaf_end)
            .map_err(|_| "leaf_end does not fit in usize on this platform".to_string())?;
        let total_leaf_count_usize = usize::try_from(total_leaf_count)
            .map_err(|_| "total_leaf_count does not fit in usize on this platform".to_string())?;
        let range_proof = decode_range_proof(
            proof_bytes,
            total_leaf_count_usize,
            leaf_start_usize,
            leaf_end_usize,
        )?;
        let logical_count = cur.take_u64()?;
        let physical_byte_count = cur.take_u64()?;
        let descriptor_count = cur.take_u32()? as usize;
        let mut leaf_descriptors =
            Vec::with_capacity(cur.bounded_capacity(descriptor_count, MIN_DESCRIPTOR_WIRE_BYTES));
        for _ in 0..descriptor_count {
            leaf_descriptors.push(PackLeafDescriptor::decode_from(&mut cur)?);
        }
        let object_count = cur.take_u32()? as usize;
        let mut object_spans =
            Vec::with_capacity(cur.bounded_capacity(object_count, OBJECT_SPAN_WIRE_BYTES));
        for _ in 0..object_count {
            object_spans.push(PackObjectSpan::new(
                cur.take_hash()?,
                cur.take_u64()?,
                cur.take_u64()?,
                cur.take_u64()?,
                cur.take_u64()?,
            )?);
        }
        if !cur.is_empty() {
            return Err(format!(
                "{} trailing byte(s) after pack index",
                cur.remaining()
            ));
        }
        Self::new_segment_with_spans(
            series_hash,
            parent_series_hash,
            leaf_start,
            leaf_end,
            total_leaf_count,
            range_root,
            range_proof,
            object_spans,
            logical_count,
            physical_byte_count,
            leaf_descriptors,
        )
    }
}

/// Shared invariant checks for [`PackIndex::new_with_spans`] and [`PackIndex::decode`].
#[allow(clippy::too_many_arguments)]
fn validate(
    leaf_start: u64,
    leaf_end: u64,
    total_leaf_count: u64,
    range_root: ObjectHash,
    range_proof: &RangeProof,
    object_spans: &[PackObjectSpan],
    leaf_descriptors: &[PackLeafDescriptor],
    logical_count: u64,
    physical_byte_count: u64,
) -> Result<(), String> {
    if leaf_start >= leaf_end {
        return Err(format!(
            "pack leaf range must be nonempty: leaf_start={leaf_start} leaf_end={leaf_end}"
        ));
    }
    if leaf_end > total_leaf_count {
        return Err(format!(
            "pack leaf_end {leaf_end} exceeds total_leaf_count {total_leaf_count}"
        ));
    }
    if object_spans.is_empty() {
        return Err("pack must name at least one physical object".to_string());
    }
    // Repeated hashes are meaningful: the physical stream may contain the
    // same content-addressed object more than once, and readers concatenate
    // entries in this exact order.
    // Exactly one descriptor per logical leaf in [leaf_start, leaf_end):
    // `leaf_end - leaf_start` cannot overflow because `leaf_start < leaf_end`
    // was just checked above.
    let expected_descriptor_count = leaf_end - leaf_start;
    let actual_descriptor_count = u64::try_from(leaf_descriptors.len())
        .map_err(|_| "pack leaf descriptor count exceeds u64::MAX".to_string())?;
    if actual_descriptor_count != expected_descriptor_count {
        return Err(format!(
            "pack has {actual_descriptor_count} leaf descriptor(s) but its range [{leaf_start}, {leaf_end}) needs exactly {expected_descriptor_count}"
        ));
    }
    let mut descriptor_logical_sum: u64 = 0;
    for descriptor in leaf_descriptors {
        descriptor_logical_sum = descriptor_logical_sum
            .checked_add(descriptor.logical_count())
            .ok_or_else(|| "pack leaf descriptor logical_count sum overflows u64".to_string())?;
    }
    if descriptor_logical_sum != logical_count {
        return Err(format!(
            "pack leaf descriptor logical_count sum {descriptor_logical_sum} does not equal declared logical_count {logical_count}"
        ));
    }
    let descriptor_hashes: Vec<ObjectHash> = leaf_descriptors
        .iter()
        .map(PackLeafDescriptor::logical_leaf_hash)
        .collect();
    let computed_root = verify_range_proof(
        usize::try_from(total_leaf_count)
            .map_err(|_| "total_leaf_count does not fit in usize on this platform".to_string())?,
        usize::try_from(leaf_start)
            .map_err(|_| "leaf_start does not fit in usize on this platform".to_string())?,
        usize::try_from(leaf_end)
            .map_err(|_| "leaf_end does not fit in usize on this platform".to_string())?,
        &descriptor_hashes,
        range_proof,
    )?;
    if computed_root != range_root {
        return Err(
            "pack descriptor leaf hashes and range proof do not reduce to range_root".to_string(),
        );
    }

    validate_object_spans(
        object_spans,
        leaf_descriptors,
        logical_count,
        physical_byte_count,
    )?;
    let leaf_start_u64 = leaf_start;
    let leaf_end_u64 = leaf_end;
    let total_leaf_count_u64 = total_leaf_count;
    super::series_merkle::validate_range_proof_shape(
        range_proof,
        total_leaf_count_u64,
        leaf_start_u64,
        leaf_end_u64,
    )
}

fn validate_object_spans(
    object_spans: &[PackObjectSpan],
    leaf_descriptors: &[PackLeafDescriptor],
    logical_count: u64,
    physical_byte_count: u64,
) -> Result<(), String> {
    let all_file = leaf_descriptors
        .iter()
        .all(|descriptor| descriptor.schema_fingerprint().is_none());
    let all_table = leaf_descriptors
        .iter()
        .all(|descriptor| descriptor.schema_fingerprint().is_some());
    if !all_file && !all_table {
        return Err("pack leaf descriptors mix absent and present schema fingerprints".to_string());
    }

    let mut logical_cursor = 0u64;
    let mut physical_cursor = 0u64;
    let mut lengths_by_hash: BTreeMap<ObjectHash, u64> = BTreeMap::new();
    let mut leaf_ranges = Vec::with_capacity(leaf_descriptors.len());
    let mut leaf_cursor = 0u64;
    for descriptor in leaf_descriptors {
        let end = leaf_cursor
            .checked_add(descriptor.logical_count())
            .ok_or_else(|| "pack leaf logical range overflows u64".to_string())?;
        leaf_ranges.push((leaf_cursor, end, descriptor.schema_fingerprint()));
        leaf_cursor = end;
    }

    for (index, span) in object_spans.iter().enumerate() {
        if span.logical_start != logical_cursor {
            return Err(format!(
                "pack object span {index} logical_start {} does not continue cursor {logical_cursor}",
                span.logical_start
            ));
        }
        if span.physical_start != physical_cursor {
            return Err(format!(
                "pack object span {index} physical_start {} does not continue cursor {physical_cursor}",
                span.physical_start
            ));
        }
        let logical_len = span
            .logical_end
            .checked_sub(span.logical_start)
            .ok_or_else(|| format!("pack object span {index} has an inverted logical interval"))?;
        let physical_len = span
            .physical_end
            .checked_sub(span.physical_start)
            .ok_or_else(|| format!("pack object span {index} has an inverted physical interval"))?;
        if logical_len == 0 || physical_len == 0 {
            return Err(format!("pack object span {index} must be nonempty"));
        }
        if all_file && logical_len != physical_len {
            return Err(format!(
                "file pack object span {index} covers {logical_len} logical byte(s) but {physical_len} physical byte(s)"
            ));
        }
        if all_table {
            let mut fingerprint: Option<ObjectHash> = None;
            for (leaf_start, leaf_end, schema) in &leaf_ranges {
                if *leaf_end <= span.logical_start || *leaf_start >= span.logical_end {
                    continue;
                }
                if let Some(existing) = fingerprint
                    && Some(existing) != *schema
                {
                    return Err(format!(
                        "table pack object span {index} crosses a schema-fingerprint transition"
                    ));
                }
                fingerprint = *schema;
            }
            if fingerprint.is_none() {
                return Err(format!(
                    "table pack object span {index} intersects no logical leaf"
                ));
            }
        }
        if let Some(previous_len) = lengths_by_hash.insert(span.object_hash, physical_len)
            && previous_len != physical_len
        {
            return Err(format!(
                "repeated physical object {} declares conflicting byte lengths {previous_len} and {physical_len}",
                span.object_hash
            ));
        }
        logical_cursor = span.logical_end;
        physical_cursor = span.physical_end;
    }
    if logical_cursor != logical_count {
        return Err(format!(
            "pack object logical spans cover {logical_cursor} unit(s), expected {logical_count}"
        ));
    }
    if physical_cursor != physical_byte_count {
        return Err(format!(
            "pack object physical spans cover {physical_cursor} byte(s), expected {physical_byte_count}"
        ));
    }
    Ok(())
}

/// Resolve one descriptor's schema fingerprint against its manifest.
///
/// Table descriptors carry their own fingerprint. File descriptors must
/// never carry one.
pub fn effective_leaf_schema_fingerprint(
    manifest: &SeriesManifest,
    _pack: &PackIndex,
    descriptor: &PackLeafDescriptor,
) -> Result<Option<ObjectHash>, String> {
    match manifest.payload_kind() {
        PayloadKind::File => {
            if descriptor.schema_fingerprint().is_some() {
                return Err(
                    "a file pack leaf descriptor must not carry a schema fingerprint".to_string(),
                );
            }
            Ok(None)
        }
        PayloadKind::Table => descriptor.schema_fingerprint().map(Some).ok_or_else(|| {
            "a table pack leaf descriptor requires a schema fingerprint".to_string()
        }),
    }
}

/// Verify a decoded [`PackIndex`] against an independently-fetched
/// [`SeriesManifest`] and the leaf hashes a caller has recomputed from the
/// pack's decoded physical content (for example, re-running
/// [`super::series_leaf::table_leaf_hash`] over decoded Parquet row groups).
///
/// This performs exactly four checks, matching
/// `docs/logical-series-identity-design.md`'s pack-acceptance rules:
///
/// 1. **Series hash binding**: `pack.series_hash()` must equal
///    `manifest_hash` (the hash under which `manifest` was actually fetched
///    -- this function trusts the caller to supply the real one, since a
///    pure codec cannot itself fetch anything).
/// 2. **Exact range length**: `range_leaf_hashes.len()` must equal
///    `pack.leaf_end() - pack.leaf_start()`, and `pack.total_leaf_count()`
///    must equal `manifest.leaf_count()`.
/// 3. **Range root**: folding `range_leaf_hashes` and `pack.range_proof()`
///    (via [`super::series_merkle::verify_range_proof`]) must reproduce
///    exactly `pack.range_root()`.
/// 4. **Membership proof against the manifest root**: that same folded
///    root must also equal `manifest.leaf_merkle_root()` -- the check that
///    actually rules out a pack whose proof is internally self-consistent
///    but belongs to a different (or fabricated) series.
///
/// # What this does *not* verify
///
/// This function never inspects Parquet bytes, Bao outboards, row content,
/// or schema metadata; it only reasons about already-recomputed
/// [`ObjectHash`] leaf hashes. Whether `range_leaf_hashes` were actually
/// derived correctly from the pack's physical objects -- decoding Parquet
/// correctly, hashing rows with the right schema, checking Bao/BLAKE3 over
/// the physical bytes -- is entirely the caller's responsibility.
///
/// # Errors
///
/// Returns a descriptive error the moment any of the four checks above
/// fails.
pub fn verify_pack_against_manifest(
    manifest_hash: ObjectHash,
    manifest: &SeriesManifest,
    pack: &PackIndex,
    range_leaf_hashes: &[ObjectHash],
) -> Result<(), String> {
    if pack.series_hash() != manifest_hash {
        return Err(
            "pack's series_hash does not match the fetched series manifest's hash".to_string(),
        );
    }
    if pack.total_leaf_count() != manifest.leaf_count() {
        return Err(format!(
            "pack declares total_leaf_count {} but the series manifest has leaf_count {}",
            pack.total_leaf_count(),
            manifest.leaf_count()
        ));
    }
    for (index, descriptor) in pack.leaf_descriptors().iter().enumerate() {
        effective_leaf_schema_fingerprint(manifest, pack, descriptor).map_err(|e| {
            format!("pack leaf descriptor {index} is incompatible with the manifest: {e}")
        })?;
    }
    let expected_len = usize::try_from(pack.leaf_end() - pack.leaf_start())
        .map_err(|_| "pack leaf range does not fit in usize on this platform".to_string())?;
    if range_leaf_hashes.len() != expected_len {
        return Err(format!(
            "expected {expected_len} recomputed leaf hash(es) for pack range, got {}",
            range_leaf_hashes.len()
        ));
    }
    for (index, (descriptor, recomputed)) in pack
        .leaf_descriptors()
        .iter()
        .zip(range_leaf_hashes)
        .enumerate()
    {
        if descriptor.logical_leaf_hash() != *recomputed {
            return Err(format!(
                "pack leaf descriptor {index} declares logical hash {} but payload recomputed to {recomputed}",
                descriptor.logical_leaf_hash()
            ));
        }
    }
    let leaf_start = usize::try_from(pack.leaf_start())
        .map_err(|_| "leaf_start does not fit in usize on this platform".to_string())?;
    let leaf_end = usize::try_from(pack.leaf_end())
        .map_err(|_| "leaf_end does not fit in usize on this platform".to_string())?;
    let total_leaf_count = usize::try_from(pack.total_leaf_count())
        .map_err(|_| "total_leaf_count does not fit in usize on this platform".to_string())?;
    let computed_root = verify_range_proof(
        total_leaf_count,
        leaf_start,
        leaf_end,
        range_leaf_hashes,
        pack.range_proof(),
    )?;
    if computed_root != pack.range_root() {
        return Err(
            "range proof does not reduce to the pack's own declared range root".to_string(),
        );
    }
    if computed_root != manifest.leaf_merkle_root() {
        return Err(
            "range proof root does not match the series manifest's leaf Merkle root".to_string(),
        );
    }
    Ok(())
}

/// Verify a whole-range root/consolidated pack against its current series
/// manifest.
///
/// In addition to [`verify_pack_against_manifest`]'s Merkle membership checks,
/// this requires `[0, manifest.leaf_count())`, no parent series link, and exact
/// aggregate logical count, event bounds, latest logical attributes, and
/// canonical frontier.
pub fn verify_complete_pack_against_manifest(
    manifest_hash: ObjectHash,
    manifest: &SeriesManifest,
    pack: &PackIndex,
) -> Result<(), String> {
    pack.validate_series_segment()?;
    if pack.parent_series_hash().is_some()
        || pack.leaf_start() != 0
        || pack.leaf_end() != manifest.leaf_count()
    {
        return Err(
            "consolidated pack must be a parentless whole-range series segment".to_string(),
        );
    }
    let leaf_hashes = pack
        .leaf_descriptors()
        .iter()
        .map(PackLeafDescriptor::logical_leaf_hash)
        .collect::<Vec<_>>();
    verify_pack_against_manifest(manifest_hash, manifest, pack, &leaf_hashes)?;
    if pack.logical_count() != manifest.logical_count() {
        return Err(format!(
            "complete pack covers {} logical unit(s), manifest declares {}",
            pack.logical_count(),
            manifest.logical_count()
        ));
    }
    let mut min: Option<i64> = None;
    let mut max: Option<i64> = None;
    for descriptor in pack.leaf_descriptors() {
        if let Some(value) = descriptor.min_event_time() {
            min = Some(min.map_or(value, |current| current.min(value)));
        }
        if let Some(value) = descriptor.max_event_time() {
            max = Some(max.map_or(value, |current| current.max(value)));
        }
    }
    if min != manifest.min_event_time() || max != manifest.max_event_time() {
        return Err("complete pack aggregate event bounds disagree with the manifest".to_string());
    }
    let attributes = pack
        .leaf_descriptors()
        .last()
        .and_then(PackLeafDescriptor::logical_attributes);
    if attributes != manifest.logical_attributes() {
        return Err(
            "complete pack latest logical attributes disagree with the manifest".to_string(),
        );
    }
    if MerkleFrontier::from_leaves(&leaf_hashes) != *manifest.merkle_frontier() {
        return Err(
            "complete pack leaf descriptors do not reconstruct the manifest frontier".to_string(),
        );
    }
    Ok(())
}

/// Deterministically choose a minimal exact cover of `[0, total_leaf_count)`
/// from a set of candidate pack indexes already known to belong to
/// `manifest_hash` (`docs/logical-series-identity-design.md` delivery gate
/// 3: "Exact-cover selection is deterministic").
///
/// `candidates` is `(pack_hash, decoded PackIndex)` pairs -- `pack_hash` is
/// the pack's own content address ([`PackIndex::hash`]), independent of
/// which series-scoped key it was fetched from. This function performs only
/// the *structural* checks a discovery layer can make without decoding
/// physical content: it does not call [`verify_pack_against_manifest`] and
/// never inspects `range_proof` or physical bytes. A caller that also needs
/// full membership-proof verification must run that separately per
/// candidate before calling this.
///
/// # Selection policy
///
/// Among every subset of `candidates` whose leaf ranges exactly tile
/// `[0, total_leaf_count)` with no gap and no overlap, this returns one
/// using the fewest packs; ties are broken by preferring the
/// lexicographically smaller [`ObjectHash`] at the first position the two
/// covers differ. The search is a dynamic program over the distinct range
/// endpoints named by `candidates` (never over `total_leaf_count` itself),
/// so it stays cheap even when the series is very long and only a handful
/// of packs are offered. It compares only reachable cover prefixes rather
/// than enumerating subsets, so the search is polynomial rather than
/// exponential in the number of candidates.
///
/// # Errors
///
/// Returns an error if any candidate declares a `series_hash` other than
/// `manifest_hash` or a `total_leaf_count` other than `total_leaf_count`
/// (rejecting a pack from the wrong series or the wrong series version), or
/// if no subset of `candidates` exactly tiles `[0, total_leaf_count)` (a gap,
/// or no candidates at all when `total_leaf_count > 0`). When
/// `total_leaf_count` is `0` the only exact cover is empty, and this
/// succeeds trivially without inspecting `candidates`.
pub fn select_exact_cover(
    manifest_hash: ObjectHash,
    total_leaf_count: u64,
    candidates: &[(ObjectHash, PackIndex)],
) -> Result<Vec<ObjectHash>, String> {
    if total_leaf_count == 0 {
        return Ok(Vec::new());
    }

    // Validate every candidate belongs to this exact series and series
    // version before it is allowed to participate in the cover at all.
    let mut intervals: Vec<(u64, u64, ObjectHash)> = Vec::with_capacity(candidates.len());
    for (pack_hash, pack) in candidates {
        if *pack_hash != pack.hash() {
            return Err(format!(
                "candidate pack key {pack_hash} does not match its encoded content hash {}",
                pack.hash()
            ));
        }
        if pack.series_hash() != manifest_hash {
            return Err(format!(
                "candidate pack {pack_hash} declares series_hash {} but the requested series is {manifest_hash}",
                pack.series_hash()
            ));
        }
        if pack.total_leaf_count() != total_leaf_count {
            return Err(format!(
                "candidate pack {pack_hash} declares total_leaf_count {} but the series has {total_leaf_count}",
                pack.total_leaf_count()
            ));
        }
        // `PackIndex::new_with_spans`/`decode` already guarantee leaf_start < leaf_end
        // <= total_leaf_count, but re-check defensively: this function must
        // never trust an invariant it did not itself just verify.
        if pack.leaf_start() >= pack.leaf_end() || pack.leaf_end() > total_leaf_count {
            return Err(format!(
                "candidate pack {pack_hash} has an invalid range [{}, {})",
                pack.leaf_start(),
                pack.leaf_end()
            ));
        }
        intervals.push((pack.leaf_start(), pack.leaf_end(), *pack_hash));
    }

    // Coordinate-compress to the distinct endpoints candidates actually
    // name, plus 0 and total_leaf_count, so the DP is O(candidates) rather
    // than O(total_leaf_count): a series with a billion leaves and three
    // packs must not force a billion-entry table.
    let mut points: Vec<u64> = Vec::with_capacity(intervals.len() * 2 + 2);
    points.push(0);
    points.push(total_leaf_count);
    for (start, end, _) in &intervals {
        points.push(*start);
        points.push(*end);
    }
    points.sort_unstable();
    points.dedup();
    let index_of: BTreeMap<u64, usize> = points.iter().enumerate().map(|(i, p)| (*p, i)).collect();

    let Some(&start_idx) = index_of.get(&0) else {
        return Err("no candidate pack covers leaf 0".to_string());
    };
    let Some(&end_idx) = index_of.get(&total_leaf_count) else {
        return Err(format!(
            "no candidate pack ends exactly at total_leaf_count {total_leaf_count}"
        ));
    };

    let mut outgoing: Vec<Vec<(usize, ObjectHash)>> = vec![Vec::new(); points.len()];
    for (start, end, hash) in intervals {
        let from_idx = index_of[&start];
        let to_idx = index_of[&end];
        outgoing[from_idx].push((to_idx, hash));
    }
    for choices in &mut outgoing {
        choices.sort_unstable_by_key(|(_, hash)| *hash);
    }

    // Work backward so a tie is decided by the first pack hash, then by the
    // already-finalized suffix only if that first pack is identical. Since a
    // content-addressed pack hash fixes its range, identical first hashes also
    // have identical successors.
    let mut best_count: Vec<Option<u32>> = vec![None; points.len()];
    let mut choice: Vec<Option<(usize, ObjectHash)>> = vec![None; points.len()];
    best_count[end_idx] = Some(0);
    for i in (start_idx..end_idx).rev() {
        for (to_idx, hash) in &outgoing[i] {
            let Some(suffix_count) = best_count[*to_idx] else {
                continue;
            };
            let candidate_count = suffix_count + 1;
            let better = match best_count[i] {
                None => true,
                Some(existing) if candidate_count < existing => true,
                Some(existing) if candidate_count == existing => {
                    let (_, current_hash) = choice[i].expect("count implies a recorded choice");
                    *hash < current_hash
                }
                _ => false,
            };
            if better {
                best_count[i] = Some(candidate_count);
                choice[i] = Some((*to_idx, *hash));
            }
        }
    }

    if best_count[start_idx].is_none() {
        return Err(format!(
            "no exact cover of [0, {total_leaf_count}) exists from {} candidate pack(s)",
            candidates.len()
        ));
    }
    let mut result = Vec::with_capacity(best_count[start_idx].unwrap_or(0) as usize);
    let mut current = start_idx;
    while current != end_idx {
        let (next, hash) = choice[current].expect("reachable point has a selected transition");
        result.push(hash);
        current = next;
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::super::series_leaf::encode_canonical_attributes;
    use super::super::series_manifest::PayloadKind;
    use super::super::series_merkle::{MerkleFrontier, generate_range_proof, merkle_root};
    use super::*;

    fn h(s: &str) -> ObjectHash {
        ObjectHash::of_bytes(s.as_bytes())
    }

    fn build_series(labels: &[&str]) -> (Vec<ObjectHash>, SeriesManifest, ObjectHash) {
        let leaves: Vec<ObjectHash> = labels.iter().map(|s| h(s)).collect();
        let manifest = SeriesManifest::new(
            PayloadKind::File,
            leaves.len() as u64 * 10,
            leaves.len() as u64,
            None,
            None,
            None,
            MerkleFrontier::from_leaves(&leaves),
        )
        .unwrap();
        let manifest_hash = manifest.hash();
        (leaves, manifest, manifest_hash)
    }

    fn build_pack(
        leaves: &[ObjectHash],
        series_hash: ObjectHash,
        start: usize,
        end: usize,
    ) -> PackIndex {
        let proof = generate_range_proof(leaves, start, end).unwrap();
        let root = merkle_root(leaves);
        let descriptors = one_leaf_per_range(leaves, start, end, 10);
        let logical_count = (end - start) as u64 * 10;
        PackIndex::new_with_spans(
            series_hash,
            start as u64,
            end as u64,
            leaves.len() as u64,
            root,
            proof,
            vec![PackObjectSpan::new(h("object-1"), 0, logical_count, 0, logical_count).unwrap()],
            logical_count,
            logical_count,
            descriptors,
        )
        .unwrap()
    }

    /// Build one [`PackLeafDescriptor`] per leaf in `[start, end)`, each with
    /// `logical_count` and no bounds or attributes -- the common case for
    /// tests that only care about pack-level invariants.
    fn one_leaf_per_range(
        leaves: &[ObjectHash],
        start: usize,
        end: usize,
        logical_count: u64,
    ) -> Vec<PackLeafDescriptor> {
        (start..end)
            .map(|index| {
                PackLeafDescriptor::new_with_leaf_hash(
                    leaves[index],
                    logical_count,
                    None,
                    None,
                    None,
                )
                .unwrap()
            })
            .collect()
    }

    fn descriptor_section_start(pack: &PackIndex) -> usize {
        PACK_MAGIC.len()
            + 32
            + 1
            + 8
            + 8
            + 8
            + 32
            + 4
            + encode_range_proof(pack.range_proof()).len()
            + 8
            + 8
            + 4
    }

    #[test]
    fn round_trips_encode_decode() {
        let (leaves, _manifest, series_hash) = build_series(&["a", "b", "c", "d", "e"]);
        let pack = build_pack(&leaves, series_hash, 1, 4);
        let bytes = pack.encode();
        let decoded = PackIndex::decode(&bytes).unwrap();
        assert_eq!(decoded, pack);
        assert_eq!(decoded.encode(), bytes);
    }

    #[test]
    fn linked_append_segment_round_trips_and_validates() {
        let (leaves, manifest, series_hash) = build_series(&["a", "b", "c"]);
        let parent = h("parent-series");
        let pack = PackIndex::new_segment_with_spans(
            series_hash,
            Some(parent),
            2,
            3,
            3,
            manifest.leaf_merkle_root(),
            generate_range_proof(&leaves, 2, 3).unwrap(),
            vec![PackObjectSpan::new(h("object"), 0, 10, 0, 10).unwrap()],
            10,
            10,
            one_leaf_per_range(&leaves, 2, 3, 10),
        )
        .unwrap();
        pack.validate_series_segment().unwrap();
        let decoded = PackIndex::decode(&pack.encode()).unwrap();
        assert_eq!(decoded, pack);
        assert_eq!(decoded.parent_series_hash(), Some(parent));
    }

    #[test]
    fn canonical_segment_shape_fails_closed() {
        let (leaves, _manifest, series_hash) = build_series(&["a", "b", "c"]);
        let partial_root = build_pack(&leaves, series_hash, 1, 3);
        assert!(partial_root.validate_series_segment().is_err());

        let linked_from_zero = PackIndex::new_segment_with_spans(
            series_hash,
            Some(h("parent")),
            0,
            3,
            3,
            merkle_root(&leaves),
            generate_range_proof(&leaves, 0, 3).unwrap(),
            vec![PackObjectSpan::new(h("object"), 0, 30, 0, 30).unwrap()],
            30,
            30,
            one_leaf_per_range(&leaves, 0, 3, 10),
        )
        .unwrap();
        assert!(linked_from_zero.validate_series_segment().is_err());
    }

    #[test]
    fn pack_round_trips_per_leaf_schema_fingerprints() {
        let leaves = vec![h("leaf-a"), h("leaf-b")];
        let manifest = SeriesManifest::new(
            PayloadKind::Table,
            20,
            2,
            None,
            None,
            None,
            MerkleFrontier::from_leaves(&leaves),
        )
        .unwrap();
        let descriptors = vec![
            PackLeafDescriptor::new_with_leaf_hash_and_schema(
                leaves[0],
                10,
                Some(h("schema-a")),
                None,
                None,
                None,
            )
            .unwrap(),
            PackLeafDescriptor::new_with_leaf_hash_and_schema(
                leaves[1],
                10,
                Some(h("schema-b")),
                None,
                None,
                None,
            )
            .unwrap(),
        ];
        let pack = PackIndex::new_with_spans(
            manifest.hash(),
            0,
            2,
            2,
            manifest.leaf_merkle_root(),
            generate_range_proof(&leaves, 0, 2).unwrap(),
            vec![
                PackObjectSpan::new(h("object-a"), 0, 10, 0, 50).unwrap(),
                PackObjectSpan::new(h("object-b"), 10, 20, 50, 100).unwrap(),
            ],
            20,
            100,
            descriptors,
        )
        .unwrap();
        let bytes = pack.encode();
        assert!(bytes.starts_with(PACK_MAGIC));
        let decoded = PackIndex::decode(&bytes).unwrap();
        assert_eq!(decoded, pack);
        assert_eq!(decoded.encode(), bytes);
        assert_eq!(
            decoded.leaf_descriptors()[0].schema_fingerprint(),
            Some(h("schema-a"))
        );
        assert_eq!(
            decoded.leaf_descriptors()[1].schema_fingerprint(),
            Some(h("schema-b"))
        );
        verify_pack_against_manifest(manifest.hash(), &manifest, &decoded, &leaves).unwrap();
    }

    #[test]
    fn descriptor_schema_presence_tampering_is_rejected_by_verification() {
        let leaves = vec![h("leaf")];
        let table_manifest = SeriesManifest::new(
            PayloadKind::Table,
            10,
            1,
            None,
            None,
            None,
            MerkleFrontier::from_leaves(&leaves),
        )
        .unwrap();
        let missing_schema = PackIndex::new_with_spans(
            table_manifest.hash(),
            0,
            1,
            1,
            table_manifest.leaf_merkle_root(),
            generate_range_proof(&leaves, 0, 1).unwrap(),
            vec![PackObjectSpan::new(h("object"), 0, 10, 0, 10).unwrap()],
            10,
            10,
            vec![PackLeafDescriptor::new_with_leaf_hash(leaves[0], 10, None, None, None).unwrap()],
        )
        .unwrap();
        let err = verify_pack_against_manifest(
            table_manifest.hash(),
            &table_manifest,
            &missing_schema,
            &leaves,
        )
        .unwrap_err();
        assert!(err.contains("requires a schema fingerprint"));

        let file_manifest = SeriesManifest::new(
            PayloadKind::File,
            10,
            1,
            None,
            None,
            None,
            MerkleFrontier::from_leaves(&leaves),
        )
        .unwrap();
        let injected_schema = PackIndex::new_with_spans(
            file_manifest.hash(),
            0,
            1,
            1,
            file_manifest.leaf_merkle_root(),
            generate_range_proof(&leaves, 0, 1).unwrap(),
            vec![PackObjectSpan::new(h("object"), 0, 10, 0, 100).unwrap()],
            10,
            100,
            vec![
                PackLeafDescriptor::new_with_leaf_hash_and_schema(
                    leaves[0],
                    10,
                    Some(h("injected-schema")),
                    None,
                    None,
                    None,
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let err = verify_pack_against_manifest(
            file_manifest.hash(),
            &file_manifest,
            &injected_schema,
            &leaves,
        )
        .unwrap_err();
        assert!(err.contains("file pack leaf descriptor"));
    }

    #[test]
    fn current_pack_index_and_proof_domains_are_stable() {
        let (leaves, _manifest, series_hash) = build_series(&["a", "b", "c", "d", "e"]);
        let pack = build_pack(&leaves, series_hash, 1, 4);
        assert_eq!(
            hex::encode(encode_range_proof(pack.range_proof())),
            "7761746572746f776e2e7365726965732d72616e67652d70726f6f662e76310a02000000000000000000000001000000000000001aea83da8ebb2fdc51fa40699b0f41d25e042a507891584889349db9a6cdaa3f04000000000000000100000000000000b1ed2124b50cdcdbae50d0eae40d70a60327480b1a823e48a8ab0f347fcab9c1"
        );
        assert!(pack.encode().starts_with(PACK_MAGIC));
        assert_eq!(pack.hash(), ObjectHash::of_bytes(&pack.encode()));
    }

    #[test]
    fn rejects_obsolete_pack_v1_magic() {
        let (leaves, _manifest, series_hash) = build_series(&["a", "b"]);
        let mut bytes = build_pack(&leaves, series_hash, 0, 2).encode();
        bytes[..b"watertown.series-pack.v1\n".len()].copy_from_slice(b"watertown.series-pack.v1\n");
        assert!(PackIndex::decode(&bytes).is_err());
    }

    #[test]
    fn rejects_obsolete_pack_v2_magic() {
        let (leaves, _manifest, series_hash) = build_series(&["a", "b"]);
        let mut bytes = build_pack(&leaves, series_hash, 0, 2).encode();
        bytes[..b"watertown.series-pack.v2\n".len()].copy_from_slice(b"watertown.series-pack.v2\n");
        assert!(PackIndex::decode(&bytes).is_err());
    }

    #[test]
    fn rejects_obsolete_pack_v3_magic() {
        let (leaves, _manifest, series_hash) = build_series(&["a", "b"]);
        let mut bytes = build_pack(&leaves, series_hash, 0, 2).encode();
        bytes[..b"watertown.series-pack.v3\n".len()].copy_from_slice(b"watertown.series-pack.v3\n");
        assert!(PackIndex::decode(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_descriptor_leaf_hash_not_authenticated_by_proof() {
        let (leaves, _manifest, series_hash) = build_series(&["a", "b"]);
        let pack = build_pack(&leaves, series_hash, 0, 2);
        let mut bytes = pack.encode();
        bytes[descriptor_section_start(&pack)] ^= 0xff;
        let err = PackIndex::decode(&bytes).unwrap_err();
        assert!(err.contains("range proof"), "unexpected error: {err}");
    }

    #[test]
    fn object_span_rejects_empty_ranges() {
        assert!(PackObjectSpan::new(h("object"), 1, 1, 0, 1).is_err());
        assert!(PackObjectSpan::new(h("object"), 0, 1, 2, 2).is_err());
    }

    #[test]
    fn new_rejects_noncontiguous_object_spans_and_wrong_totals() {
        let (leaves, _manifest, series_hash) = build_series(&["a", "b"]);
        let proof = generate_range_proof(&leaves, 0, 2).unwrap();
        let descriptors = one_leaf_per_range(&leaves, 0, 2, 10);
        let err = PackIndex::new_with_spans(
            series_hash,
            0,
            2,
            2,
            merkle_root(&leaves),
            proof.clone(),
            vec![
                PackObjectSpan::new(h("a"), 0, 10, 0, 10).unwrap(),
                PackObjectSpan::new(h("b"), 11, 20, 10, 20).unwrap(),
            ],
            20,
            20,
            descriptors.clone(),
        )
        .unwrap_err();
        assert!(err.contains("logical_start"));

        let err = PackIndex::new_with_spans(
            series_hash,
            0,
            2,
            2,
            merkle_root(&leaves),
            proof,
            vec![PackObjectSpan::new(h("a"), 0, 19, 0, 19).unwrap()],
            20,
            20,
            descriptors,
        )
        .unwrap_err();
        assert!(err.contains("logical spans cover"));
    }

    #[test]
    fn new_rejects_duplicate_hashes_with_different_lengths() {
        let (leaves, _manifest, series_hash) = build_series(&["a", "b"]);
        let object = h("same-object");
        let err = PackIndex::new_with_spans(
            series_hash,
            0,
            2,
            2,
            merkle_root(&leaves),
            generate_range_proof(&leaves, 0, 2).unwrap(),
            vec![
                PackObjectSpan::new(object, 0, 5, 0, 5).unwrap(),
                PackObjectSpan::new(object, 5, 20, 5, 20).unwrap(),
            ],
            20,
            20,
            one_leaf_per_range(&leaves, 0, 2, 10),
        )
        .unwrap_err();
        assert!(err.contains("conflicting byte lengths"));
    }

    #[test]
    fn new_rejects_file_object_with_different_logical_and_physical_lengths() {
        let (leaves, _manifest, series_hash) = build_series(&["a", "b"]);
        let err = PackIndex::new_with_spans(
            series_hash,
            0,
            2,
            2,
            merkle_root(&leaves),
            generate_range_proof(&leaves, 0, 2).unwrap(),
            vec![PackObjectSpan::new(h("object"), 0, 20, 0, 21).unwrap()],
            20,
            21,
            one_leaf_per_range(&leaves, 0, 2, 10),
        )
        .unwrap_err();
        assert!(err.contains("file pack object"));
    }

    #[test]
    fn new_rejects_wrong_physical_span_total() {
        let leaves = vec![h("leaf")];
        let descriptor = PackLeafDescriptor::new_with_leaf_hash_and_schema(
            leaves[0],
            20,
            Some(h("schema")),
            None,
            None,
            None,
        )
        .unwrap();
        let err = PackIndex::new_with_spans(
            h("series"),
            0,
            1,
            1,
            merkle_root(&leaves),
            generate_range_proof(&leaves, 0, 1).unwrap(),
            vec![PackObjectSpan::new(h("object"), 0, 20, 0, 19).unwrap()],
            20,
            20,
            vec![descriptor],
        )
        .unwrap_err();
        assert!(err.contains("physical spans cover"));
    }

    #[test]
    fn new_rejects_table_object_crossing_schema_run() {
        let leaves = vec![h("leaf-a"), h("leaf-b")];
        let descriptors = vec![
            PackLeafDescriptor::new_with_leaf_hash_and_schema(
                leaves[0],
                5,
                Some(h("schema-a")),
                None,
                None,
                None,
            )
            .unwrap(),
            PackLeafDescriptor::new_with_leaf_hash_and_schema(
                leaves[1],
                5,
                Some(h("schema-b")),
                None,
                None,
                None,
            )
            .unwrap(),
        ];
        let err = PackIndex::new_with_spans(
            h("series"),
            0,
            2,
            2,
            merkle_root(&leaves),
            generate_range_proof(&leaves, 0, 2).unwrap(),
            vec![PackObjectSpan::new(h("object"), 0, 10, 0, 100).unwrap()],
            10,
            100,
            descriptors,
        )
        .unwrap_err();
        assert!(err.contains("crosses a schema-fingerprint transition"));
    }

    #[test]
    fn verify_succeeds_for_a_true_pack() {
        let (leaves, manifest, series_hash) = build_series(&["a", "b", "c", "d", "e"]);
        let pack = build_pack(&leaves, series_hash, 1, 4);
        assert!(verify_pack_against_manifest(series_hash, &manifest, &pack, &leaves[1..4]).is_ok());
    }

    #[test]
    fn two_different_pack_layouts_verify_against_the_same_series() {
        let (leaves, manifest, series_hash) = build_series(&["a", "b", "c", "d", "e", "f"]);

        // Layout 1: split at 3.
        let pack_a1 = build_pack(&leaves, series_hash, 0, 3);
        let pack_a2 = build_pack(&leaves, series_hash, 3, 6);
        assert!(
            verify_pack_against_manifest(series_hash, &manifest, &pack_a1, &leaves[0..3]).is_ok()
        );
        assert!(
            verify_pack_against_manifest(series_hash, &manifest, &pack_a2, &leaves[3..6]).is_ok()
        );

        // Layout 2: split at 2 and 4.
        let pack_b1 = build_pack(&leaves, series_hash, 0, 2);
        let pack_b2 = build_pack(&leaves, series_hash, 2, 4);
        let pack_b3 = build_pack(&leaves, series_hash, 4, 6);
        assert!(
            verify_pack_against_manifest(series_hash, &manifest, &pack_b1, &leaves[0..2]).is_ok()
        );
        assert!(
            verify_pack_against_manifest(series_hash, &manifest, &pack_b2, &leaves[2..4]).is_ok()
        );
        assert!(
            verify_pack_against_manifest(series_hash, &manifest, &pack_b3, &leaves[4..6]).is_ok()
        );
    }

    #[test]
    fn cross_series_pack_is_rejected() {
        let (leaves_a, manifest_a, series_hash_a) = build_series(&["a", "b", "c", "d", "e"]);
        let (leaves_b, _manifest_b, series_hash_b) = build_series(&["v", "w", "x", "y", "z"]);
        // A pack built for series B, presented against series A's manifest.
        let pack_for_b = build_pack(&leaves_b, series_hash_b, 1, 4);
        assert!(
            verify_pack_against_manifest(series_hash_a, &manifest_a, &pack_for_b, &leaves_a[1..4])
                .is_err()
        );
    }

    #[test]
    fn wrong_series_hash_binding_is_rejected() {
        let (leaves, manifest, series_hash) = build_series(&["a", "b", "c", "d", "e"]);
        let pack = build_pack(&leaves, series_hash, 1, 4);
        // Corrupt the claimed series_hash by rebuilding through decode with a
        // tampered first hash byte.
        let mut bytes = pack.encode();
        let series_hash_pos = PACK_MAGIC.len();
        bytes[series_hash_pos] ^= 0xff;
        // Decoding still succeeds (decode does not know the "true" series
        // hash), but verification against the real manifest must fail.
        let pack = PackIndex::decode(&bytes).unwrap();
        assert!(
            verify_pack_against_manifest(series_hash, &manifest, &pack, &leaves[1..4]).is_err()
        );
    }

    #[test]
    fn substituted_leaves_are_rejected() {
        let (leaves, manifest, series_hash) = build_series(&["a", "b", "c", "d", "e"]);
        let pack = build_pack(&leaves, series_hash, 1, 4);
        let mut tampered = leaves[1..4].to_vec();
        tampered[0] = h("substituted");
        assert!(verify_pack_against_manifest(series_hash, &manifest, &pack, &tampered).is_err());
    }

    #[test]
    fn wrong_total_leaf_count_is_rejected_at_decode() {
        let (leaves, _manifest, series_hash) = build_series(&["a", "b", "c", "d", "e"]);
        let pack = build_pack(&leaves, series_hash, 1, 4);
        // Mutate the wire total_leaf_count field: this must invalidate the
        // embedded range proof's shape (it was computed for the original
        // total), so decode itself must reject the tampered bytes.
        let mut bytes = pack.encode();
        let total_pos = PACK_MAGIC.len() + 32 + 8 + 8;
        let mut total_bytes = [0u8; 8];
        total_bytes.copy_from_slice(&bytes[total_pos..total_pos + 8]);
        let mutated_total = u64::from_le_bytes(total_bytes) + 1;
        bytes[total_pos..total_pos + 8].copy_from_slice(&mutated_total.to_le_bytes());
        assert!(PackIndex::decode(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_maximum_leaf_count_without_hanging() {
        let (leaves, _manifest, series_hash) = build_series(&["a", "b", "c", "d", "e"]);
        let pack = build_pack(&leaves, series_hash, 1, 4);
        let mut bytes = pack.encode();
        let total_pos = PACK_MAGIC.len() + 32 + 8 + 8;
        bytes[total_pos..total_pos + 8].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(PackIndex::decode(&bytes).is_err());
    }

    #[test]
    fn new_rejects_empty_range() {
        let (leaves, _m, _series_hash) = build_series(&["a", "b", "c"]);
        let proof_err = generate_range_proof(&leaves, 2, 2);
        assert!(proof_err.is_err());
    }

    #[test]
    fn new_rejects_out_of_bounds_range() {
        let (leaves, _m, series_hash) = build_series(&["a", "b", "c"]);
        let proof = generate_range_proof(&leaves, 0, 3).unwrap();
        let root = merkle_root(&leaves);
        let err = PackIndex::new_with_spans(
            series_hash,
            0,
            4, // exceeds total_leaf_count of 3
            3,
            root,
            proof,
            vec![PackObjectSpan::new(h("object-1"), 0, 10, 0, 10).unwrap()],
            10,
            10,
            Vec::new(),
        )
        .unwrap_err();
        assert!(err.contains("leaf_end"));
    }

    #[test]
    fn new_rejects_empty_object_list() {
        let (leaves, _m, series_hash) = build_series(&["a", "b", "c"]);
        let proof = generate_range_proof(&leaves, 0, 2).unwrap();
        let root = merkle_root(&leaves);
        let err = PackIndex::new_with_spans(
            series_hash,
            0,
            2,
            3,
            root,
            proof,
            vec![],
            10,
            10,
            Vec::new(),
        )
        .unwrap_err();
        assert!(err.contains("physical object"));
    }

    #[test]
    fn new_accepts_ordered_duplicate_objects() {
        let (leaves, _m, series_hash) = build_series(&["a", "b", "c"]);
        let proof = generate_range_proof(&leaves, 0, 2).unwrap();
        let root = merkle_root(&leaves);
        let pack = PackIndex::new_with_spans(
            series_hash,
            0,
            2,
            3,
            root,
            proof,
            vec![
                PackObjectSpan::new(h("object-1"), 0, 5, 0, 5).unwrap(),
                PackObjectSpan::new(h("object-1"), 5, 10, 5, 10).unwrap(),
            ],
            10,
            10,
            one_leaf_per_range(&leaves, 0, 2, 5),
        )
        .expect("an ordered physical stream may reuse one content object");
        assert_eq!(pack.physical_object_hashes().len(), 2);
        assert_eq!(
            pack.physical_object_hashes()[0],
            pack.physical_object_hashes()[1]
        );
    }

    // -- PackLeafDescriptor --------------------------------------------------

    #[test]
    fn descriptor_new_rejects_zero_logical_count() {
        let err =
            PackLeafDescriptor::new_with_leaf_hash(h("leaf"), 0, None, None, None).unwrap_err();
        assert!(err.contains("positive"), "unexpected error: {err}");
    }

    #[test]
    fn descriptor_new_rejects_empty_some_attributes() {
        let err =
            PackLeafDescriptor::new_with_leaf_hash(h("leaf"), 1, None, None, Some(Vec::new()))
                .unwrap_err();
        assert!(err.contains("absent"), "unexpected error: {err}");
    }

    #[test]
    fn descriptor_new_rejects_noncanonical_attributes() {
        // Non-canonical (insignificant whitespace) attribute bytes are valid
        // JSON but not the canonical bytes this codec requires.
        let non_canonical = b"{ \"a\": 1 }".to_vec();
        let err =
            PackLeafDescriptor::new_with_leaf_hash(h("leaf"), 1, None, None, Some(non_canonical))
                .unwrap_err();
        assert!(err.contains("canonical"), "unexpected error: {err}");
    }

    #[test]
    fn descriptor_accepts_independent_optional_bounds_and_canonical_attributes() {
        let attrs = encode_canonical_attributes(r#"{"a":1}"#).unwrap();
        let d = PackLeafDescriptor::new_with_leaf_hash(
            h("leaf"),
            7,
            Some(10),
            Some(20),
            Some(attrs.clone()),
        )
        .unwrap();
        assert_eq!(d.logical_count(), 7);
        assert_eq!(d.min_event_time(), Some(10));
        assert_eq!(d.max_event_time(), Some(20));
        assert_eq!(d.logical_attributes(), Some(attrs.as_slice()));

        // Bounds are independently optional: only a minimum, only a
        // maximum, or neither must all succeed distinctly from "both".
        assert!(PackLeafDescriptor::new_with_leaf_hash(h("leaf"), 7, Some(10), None, None).is_ok());
        assert!(PackLeafDescriptor::new_with_leaf_hash(h("leaf"), 7, None, Some(20), None).is_ok());
        assert!(PackLeafDescriptor::new_with_leaf_hash(h("leaf"), 7, None, None, None).is_ok());
    }

    // -- PackIndex + PackLeafDescriptor integration ---------------------------

    #[test]
    fn pack_with_descriptor_bounds_and_attributes_round_trips() {
        let (leaves, _manifest, series_hash) = build_series(&["a"]);
        let proof = generate_range_proof(&leaves, 0, 1).unwrap();
        let root = merkle_root(&leaves);
        let attrs = encode_canonical_attributes(r#"{"k":"v"}"#).unwrap();
        let descriptor =
            PackLeafDescriptor::new_with_leaf_hash(leaves[0], 42, Some(-5), Some(99), Some(attrs))
                .unwrap();
        let pack = PackIndex::new_with_spans(
            series_hash,
            0,
            1,
            1,
            root,
            proof,
            vec![PackObjectSpan::new(h("object-1"), 0, 42, 0, 42).unwrap()],
            42,
            42,
            vec![descriptor],
        )
        .unwrap();
        let bytes = pack.encode();
        let decoded = PackIndex::decode(&bytes).unwrap();
        assert_eq!(decoded, pack);
        assert_eq!(decoded.encode(), bytes);
        assert!(bytes.starts_with(PACK_MAGIC));
        assert_eq!(pack.hash(), ObjectHash::of_bytes(&bytes));
    }

    #[test]
    fn new_rejects_too_few_descriptors() {
        let (leaves, _m, series_hash) = build_series(&["a", "b", "c", "d", "e"]);
        let proof = generate_range_proof(&leaves, 1, 4).unwrap();
        let root = merkle_root(&leaves);
        let err = PackIndex::new_with_spans(
            series_hash,
            1,
            4,
            5,
            root,
            proof,
            vec![PackObjectSpan::new(h("object-1"), 0, 30, 0, 30).unwrap()],
            30,
            10,
            one_leaf_per_range(&leaves, 1, 3, 10), // range needs 3 descriptors, only 2 given
        )
        .unwrap_err();
        assert!(err.contains("leaf descriptor"), "unexpected error: {err}");
    }

    #[test]
    fn new_rejects_too_many_descriptors() {
        let (leaves, _m, series_hash) = build_series(&["a", "b", "c", "d", "e"]);
        let proof = generate_range_proof(&leaves, 1, 4).unwrap();
        let root = merkle_root(&leaves);
        let err = PackIndex::new_with_spans(
            series_hash,
            1,
            4,
            5,
            root,
            proof,
            vec![PackObjectSpan::new(h("object-1"), 0, 30, 0, 30).unwrap()],
            30,
            10,
            one_leaf_per_range(&leaves, 1, 5, 10), // range needs 3 descriptors, 4 given
        )
        .unwrap_err();
        assert!(err.contains("leaf descriptor"), "unexpected error: {err}");
    }

    #[test]
    fn new_rejects_descriptor_sum_mismatch() {
        let (leaves, _m, series_hash) = build_series(&["a", "b", "c", "d", "e"]);
        let proof = generate_range_proof(&leaves, 1, 4).unwrap();
        let root = merkle_root(&leaves);
        // Three descriptors summing to 29, but logical_count declares 30.
        let descriptors = vec![
            PackLeafDescriptor::new_with_leaf_hash(leaves[1], 10, None, None, None).unwrap(),
            PackLeafDescriptor::new_with_leaf_hash(leaves[2], 10, None, None, None).unwrap(),
            PackLeafDescriptor::new_with_leaf_hash(leaves[3], 9, None, None, None).unwrap(),
        ];
        let err = PackIndex::new_with_spans(
            series_hash,
            1,
            4,
            5,
            root,
            proof,
            vec![PackObjectSpan::new(h("object-1"), 0, 30, 0, 30).unwrap()],
            30,
            10,
            descriptors,
        )
        .unwrap_err();
        assert!(err.contains("logical_count sum"), "unexpected error: {err}");
    }

    #[test]
    fn decode_rejects_zero_count_descriptor() {
        let (leaves, _m, series_hash) = build_series(&["a", "b", "c", "d", "e"]);
        let pack = build_pack(&leaves, series_hash, 1, 4);
        let mut bytes = pack.encode();
        // Overwrite the first descriptor's u64 logical_count with 0. The
        // descriptor section starts right after physical_byte_count and a
        // u32 descriptor count, so its exact offset is computed rather than
        // hardcoded, keeping this test resilient to unrelated header changes.
        let descriptor_section_start = descriptor_section_start(&pack);
        let logical_count_start = descriptor_section_start + 32;
        bytes[logical_count_start..logical_count_start + 8].copy_from_slice(&0u64.to_le_bytes());
        let err = PackIndex::decode(&bytes).unwrap_err();
        assert!(err.contains("positive"), "unexpected error: {err}");
    }

    #[test]
    fn decode_rejects_descriptor_sum_overflow() {
        let (leaves, _m, series_hash) = build_series(&["a", "b", "c", "d", "e"]);
        let pack = build_pack(&leaves, series_hash, 1, 4);
        let mut bytes = pack.encode();
        let descriptor_section_start = descriptor_section_start(&pack);
        // Set every descriptor's logical_count to u64::MAX so their sum
        // overflows u64 rather than merely mismatching.
        for i in 0..pack.leaf_descriptors.len() {
            let start = descriptor_section_start + i * MIN_DESCRIPTOR_WIRE_BYTES + 32;
            bytes[start..start + 8].copy_from_slice(&u64::MAX.to_le_bytes());
        }
        let err = PackIndex::decode(&bytes).unwrap_err();
        assert!(err.contains("overflow"), "unexpected error: {err}");
    }

    #[test]
    fn decode_rejects_descriptor_unknown_bounds_flags() {
        let (leaves, _m, series_hash) = build_series(&["a", "b", "c", "d", "e"]);
        let pack = build_pack(&leaves, series_hash, 1, 4);
        let mut bytes = pack.encode();
        let descriptor_section_start = descriptor_section_start(&pack);
        // The bounds_flags byte follows logical_count and the empty schema field.
        let flags_pos = descriptor_section_start + 32 + 8 + 4;
        bytes[flags_pos] = 0b1111_1100;
        let err = PackIndex::decode(&bytes).unwrap_err();
        assert!(err.contains("unknown"), "unexpected error: {err}");
    }

    #[test]
    fn descriptor_mutation_changes_pack_hash() {
        let (leaves, _m, series_hash) = build_series(&["a", "b", "c"]);
        let proof = generate_range_proof(&leaves, 0, 2).unwrap();
        let root = merkle_root(&leaves);
        let base_descriptors = vec![
            PackLeafDescriptor::new_with_leaf_hash(leaves[0], 5, None, None, None).unwrap(),
            PackLeafDescriptor::new_with_leaf_hash(leaves[1], 5, None, None, None).unwrap(),
        ];
        let mutated_descriptors = vec![
            PackLeafDescriptor::new_with_leaf_hash(leaves[0], 5, Some(1), None, None).unwrap(),
            PackLeafDescriptor::new_with_leaf_hash(leaves[1], 5, None, None, None).unwrap(),
        ];
        let pack_a = PackIndex::new_with_spans(
            series_hash,
            0,
            2,
            3,
            root,
            proof.clone(),
            vec![PackObjectSpan::new(h("object-1"), 0, 10, 0, 10).unwrap()],
            10,
            10,
            base_descriptors,
        )
        .unwrap();
        let pack_b = PackIndex::new_with_spans(
            series_hash,
            0,
            2,
            3,
            root,
            proof,
            vec![PackObjectSpan::new(h("object-1"), 0, 10, 0, 10).unwrap()],
            10,
            10,
            mutated_descriptors,
        )
        .unwrap();
        assert_ne!(
            pack_a.hash(),
            pack_b.hash(),
            "a descriptor-only change must change the pack's own content hash"
        );
    }

    #[test]
    fn leaf_descriptor_count_is_independent_of_physical_object_count() {
        // The accepted per-leaf descriptor model: physical object hashes are
        // an ordered stream independent of leaf boundaries, so a pack may
        // have far more leaf descriptors than physical objects (many small
        // leaves packed into one object) or far more physical objects than
        // leaf descriptors (one huge leaf spanning many objects). Neither
        // direction is coupled or cross-checked by this module.
        let (leaves, _m, series_hash) = build_series(&["a", "b", "c", "d", "e"]);

        // Five leaf descriptors, one physical object: many leaves, one object.
        let proof_many_leaves = generate_range_proof(&leaves, 0, 5).unwrap();
        let root = merkle_root(&leaves);
        let many_leaves_one_object = PackIndex::new_with_spans(
            series_hash,
            0,
            5,
            5,
            root,
            proof_many_leaves,
            vec![PackObjectSpan::new(h("single-object"), 0, 50, 0, 50).unwrap()],
            50,
            50,
            one_leaf_per_range(&leaves, 0, 5, 10),
        )
        .unwrap();
        assert_eq!(many_leaves_one_object.leaf_descriptors().len(), 5);
        assert_eq!(many_leaves_one_object.physical_object_hashes().len(), 1);

        // One leaf descriptor, three physical objects: one leaf spanning
        // (from a reader's perspective) content assembled from many objects.
        let proof_one_leaf = generate_range_proof(&leaves, 0, 1).unwrap();
        let one_leaf_many_objects = PackIndex::new_with_spans(
            series_hash,
            0,
            1,
            5,
            root,
            proof_one_leaf,
            vec![
                PackObjectSpan::new(h("object-a"), 0, 3, 0, 3).unwrap(),
                PackObjectSpan::new(h("object-b"), 3, 7, 3, 7).unwrap(),
                PackObjectSpan::new(h("object-c"), 7, 10, 7, 10).unwrap(),
            ],
            10,
            10,
            vec![PackLeafDescriptor::new_with_leaf_hash(leaves[0], 10, None, None, None).unwrap()],
        )
        .unwrap();
        assert_eq!(one_leaf_many_objects.leaf_descriptors().len(), 1);
        assert_eq!(one_leaf_many_objects.physical_object_hashes().len(), 3);
    }

    #[test]
    fn decode_rejects_bad_magic() {
        let (leaves, _m, series_hash) = build_series(&["a", "b", "c"]);
        let pack = build_pack(&leaves, series_hash, 0, 2);
        let mut bytes = pack.encode();
        bytes[0] ^= 0xff;
        assert!(PackIndex::decode(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_truncation() {
        let (leaves, _m, series_hash) = build_series(&["a", "b", "c", "d", "e"]);
        let pack = build_pack(&leaves, series_hash, 1, 4);
        let bytes = pack.encode();
        assert!(PackIndex::decode(&bytes[..bytes.len() - 4]).is_err());
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let (leaves, _m, series_hash) = build_series(&["a", "b", "c"]);
        let pack = build_pack(&leaves, series_hash, 0, 2);
        let mut bytes = pack.encode();
        bytes.push(0);
        assert!(PackIndex::decode(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_oversized_object_count_without_huge_alloc() {
        let (leaves, _m, series_hash) = build_series(&["a", "b", "c"]);
        let pack = build_pack(&leaves, series_hash, 0, 2);
        let bytes = pack.encode();
        // Splice in a hostile object_count of u32::MAX right after the
        // legitimate proof bytes, discarding the real (small) count and
        // object list, and check that decode fails on truncation rather
        // than attempting a huge allocation.
        let proof_len_pos = PACK_MAGIC.len() + 32 + 1 + 8 + 8 + 8 + 32;
        let mut proof_len_bytes = [0u8; 4];
        proof_len_bytes.copy_from_slice(&bytes[proof_len_pos..proof_len_pos + 4]);
        let proof_len = u32::from_le_bytes(proof_len_bytes) as usize;
        let mut truncated = bytes[..proof_len_pos + 4 + proof_len].to_vec();
        truncated.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(PackIndex::decode(&truncated).is_err());
    }

    #[test]
    fn pack_layout_changes_do_not_change_manifest_hash() {
        let (leaves, manifest, _series_hash) = build_series(&["a", "b", "c", "d", "e", "f"]);
        let manifest_hash_before = manifest.hash();

        // Building any number of different packs over the same leaves never
        // touches the manifest: there is no shared mutable state, and
        // `SeriesManifest` has no pack-related field.
        let _pack_1 = build_pack(&leaves, manifest.hash(), 0, 3);
        let _pack_2 = build_pack(&leaves, manifest.hash(), 0, 2);
        let _pack_3 = build_pack(&leaves, manifest.hash(), 2, 6);

        assert_eq!(manifest.hash(), manifest_hash_before);
    }

    // -- select_exact_cover ------------------------------------------------

    fn cand(pack: &PackIndex) -> (ObjectHash, PackIndex) {
        (pack.hash(), pack.clone())
    }

    #[test]
    fn exact_cover_prefers_fewest_packs() {
        let (leaves, manifest, series_hash) = build_series(&["a", "b", "c", "d", "e", "f"]);
        let whole = build_pack(&leaves, series_hash, 0, 6);
        let half_a = build_pack(&leaves, series_hash, 0, 3);
        let half_b = build_pack(&leaves, series_hash, 3, 6);

        let cover = select_exact_cover(
            manifest.hash(),
            manifest.leaf_count(),
            &[cand(&whole), cand(&half_a), cand(&half_b)],
        )
        .unwrap();
        assert_eq!(cover, vec![whole.hash()], "one pack beats two");
    }

    #[test]
    fn exact_cover_ties_break_on_lexicographically_smaller_hash() {
        let (leaves, manifest, series_hash) = build_series(&["a", "b", "c", "d", "e"]);
        // Two independently-built full-range packs differ only in their
        // named physical object, so they decode to different hashes while
        // both validly covering the whole range.
        let proof = generate_range_proof(&leaves, 0, leaves.len()).unwrap();
        let root = merkle_root(&leaves);
        let pack_x = PackIndex::new_with_spans(
            series_hash,
            0,
            leaves.len() as u64,
            leaves.len() as u64,
            root,
            proof.clone(),
            vec![PackObjectSpan::new(h("object-x"), 0, 50, 0, 50).unwrap()],
            50,
            50,
            one_leaf_per_range(&leaves, 0, leaves.len(), 10),
        )
        .unwrap();
        let pack_y = PackIndex::new_with_spans(
            series_hash,
            0,
            leaves.len() as u64,
            leaves.len() as u64,
            root,
            proof,
            vec![PackObjectSpan::new(h("object-y"), 0, 50, 0, 50).unwrap()],
            50,
            50,
            one_leaf_per_range(&leaves, 0, leaves.len(), 10),
        )
        .unwrap();
        assert_ne!(
            pack_x.hash(),
            pack_y.hash(),
            "layouts must be distinguishable"
        );
        let expected = pack_x.hash().min(pack_y.hash());

        let cover = select_exact_cover(
            manifest.hash(),
            manifest.leaf_count(),
            &[cand(&pack_x), cand(&pack_y)],
        )
        .unwrap();
        assert_eq!(cover, vec![expected]);

        // Order of the candidate slice must not matter.
        let cover_reordered = select_exact_cover(
            manifest.hash(),
            manifest.leaf_count(),
            &[cand(&pack_y), cand(&pack_x)],
        )
        .unwrap();
        assert_eq!(cover_reordered, vec![expected]);
    }

    #[test]
    fn exact_cover_compares_tie_breaks_from_the_start() {
        let (leaves, manifest, series_hash) = build_series(&["a", "b", "c", "d", "e", "f"]);
        let first_a = build_pack(&leaves, series_hash, 0, 2);
        let first_b = build_pack(&leaves, series_hash, 0, 4);

        let mut tails_a = Vec::new();
        let mut tails_b = Vec::new();
        for i in 0..32 {
            let proof_a = generate_range_proof(&leaves, 2, 6).unwrap();
            tails_a.push(
                PackIndex::new_with_spans(
                    series_hash,
                    2,
                    6,
                    6,
                    merkle_root(&leaves),
                    proof_a,
                    vec![PackObjectSpan::new(h(&format!("tail-a-{i}")), 0, 40, 0, 40).unwrap()],
                    40,
                    40,
                    one_leaf_per_range(&leaves, 2, 6, 10),
                )
                .unwrap(),
            );
            let proof_b = generate_range_proof(&leaves, 4, 6).unwrap();
            tails_b.push(
                PackIndex::new_with_spans(
                    series_hash,
                    4,
                    6,
                    6,
                    merkle_root(&leaves),
                    proof_b,
                    vec![PackObjectSpan::new(h(&format!("tail-b-{i}")), 0, 20, 0, 20).unwrap()],
                    20,
                    20,
                    one_leaf_per_range(&leaves, 4, 6, 10),
                )
                .unwrap(),
            );
        }

        let a_is_lex_first = first_a.hash() < first_b.hash();
        let (tail_a, tail_b) = tails_a
            .iter()
            .flat_map(|a| tails_b.iter().map(move |b| (a, b)))
            .find(|(a, b)| (a.hash() > b.hash()) == a_is_lex_first)
            .expect("sampled tails should include an opposing last-hash order");
        let expected = if a_is_lex_first {
            vec![first_a.hash(), tail_a.hash()]
        } else {
            vec![first_b.hash(), tail_b.hash()]
        };
        let cover = select_exact_cover(
            manifest.hash(),
            manifest.leaf_count(),
            &[cand(&first_a), cand(tail_a), cand(&first_b), cand(tail_b)],
        )
        .unwrap();
        assert_eq!(cover, expected);
    }

    #[test]
    fn exact_cover_rejects_a_gap() {
        let (leaves, manifest, series_hash) = build_series(&["a", "b", "c", "d", "e", "f"]);
        let first = build_pack(&leaves, series_hash, 0, 2);
        let last = build_pack(&leaves, series_hash, 4, 6);
        // [2, 4) is never covered.
        let err = select_exact_cover(
            manifest.hash(),
            manifest.leaf_count(),
            &[cand(&first), cand(&last)],
        )
        .unwrap_err();
        assert!(err.contains("no exact cover"), "unexpected error: {err}");
    }

    #[test]
    fn exact_cover_ignores_unusable_overlap_but_still_finds_a_valid_chain() {
        let (leaves, manifest, series_hash) = build_series(&["a", "b", "c", "d", "e", "f"]);
        // These two overlap and cannot be chained together ([0,6) then
        // [4,10) does not start where the first ends), but a third
        // candidate completes an exact, non-overlapping chain through them.
        let overlap_a = build_pack(&leaves, series_hash, 0, 6);
        let overlap_b = build_pack(&leaves, series_hash, 4, 6);
        let closer = build_pack(&leaves, series_hash, 0, 4);

        let cover = select_exact_cover(
            manifest.hash(),
            manifest.leaf_count(),
            &[cand(&overlap_a), cand(&overlap_b), cand(&closer)],
        )
        .unwrap();
        // The single whole-range pack still wins on pack count.
        assert_eq!(cover, vec![overlap_a.hash()]);

        // Without the whole-range pack, [0,4) + [4,6) is the only exact,
        // non-overlapping chain available.
        let cover_no_whole = select_exact_cover(
            manifest.hash(),
            manifest.leaf_count(),
            &[cand(&overlap_b), cand(&closer)],
        )
        .unwrap();
        assert_eq!(cover_no_whole, vec![closer.hash(), overlap_b.hash()]);
    }

    #[test]
    fn exact_cover_rejects_wrong_series() {
        let (leaves_a, manifest_a, _series_hash_a) = build_series(&["a", "b", "c"]);
        let (leaves_b, _manifest_b, series_hash_b) = build_series(&["x", "y", "z"]);
        let foreign = build_pack(&leaves_b, series_hash_b, 0, 3);
        let _ = leaves_a;
        let err = select_exact_cover(
            manifest_a.hash(),
            manifest_a.leaf_count(),
            &[cand(&foreign)],
        )
        .unwrap_err();
        assert!(err.contains("series_hash"), "unexpected error: {err}");
    }

    #[test]
    fn exact_cover_rejects_foreign_total_leaf_count() {
        let (leaves, manifest, series_hash) = build_series(&["a", "b", "c"]);
        let pack = build_pack(&leaves, series_hash, 0, 2);
        // Ask for a different total than every candidate declares.
        let err = select_exact_cover(manifest.hash(), manifest.leaf_count() + 1, &[cand(&pack)])
            .unwrap_err();
        assert!(err.contains("total_leaf_count"), "unexpected error: {err}");
    }

    #[test]
    fn exact_cover_of_empty_series_needs_no_candidates() {
        let manifest = SeriesManifest::new(
            super::super::series_manifest::PayloadKind::File,
            0,
            0,
            None,
            None,
            None,
            MerkleFrontier::empty(),
        )
        .unwrap();
        let cover = select_exact_cover(manifest.hash(), manifest.leaf_count(), &[]).unwrap();
        assert!(cover.is_empty());
    }

    #[test]
    fn exact_cover_rejects_no_candidates_for_nonempty_series() {
        let (_leaves, manifest, _series_hash) = build_series(&["a"]);
        let err = select_exact_cover(manifest.hash(), manifest.leaf_count(), &[]).unwrap_err();
        assert!(err.contains("no exact cover"), "unexpected error: {err}");
    }
}
