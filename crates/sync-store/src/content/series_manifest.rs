// SPDX-License-Identifier: Apache-2.0

//! `watertown.series.v1` root object: the fetchable identity of one logical series.
//!
//! `docs/logical-series-identity-design.md` delivery gate 2. A
//! [`SeriesManifest`] aggregates everything a `watertown.series.v1` object commits to
//! over the whole ordered run of logical leaves (see
//! [`super::series_leaf`]): the payload kind, the schema fingerprint (table
//! only), the aggregate logical row/byte count, the leaf count, aggregate
//! event-time bounds, canonical logical attributes, and the
//! [`super::series_merkle`] leaf Merkle root. Its own BLAKE3 hash --
//! [`SeriesManifest::hash`] over [`SeriesManifest::encode`] -- is both the
//! series' identity and the content address a `ManifestEntry.child_hash`
//! (once wired up in a later gate) would name.
//!
//! Packs are deliberately absent from this object: per the design doc,
//! "Physical pack index" data is derived storage metadata, excluded from the
//! logical content tree so that repacking never changes this hash. See
//! [`super::series_pack`] for the pack side.
//!
//! Fields are private and only reachable through validated construction
//! ([`SeriesManifest::new`]) or strict decode ([`SeriesManifest::decode`]),
//! so an invariant-violating value can never exist in safe code.

use super::series_leaf::validate_canonical_attributes;
use super::series_leaf::{LEAF_HAS_MAX, LEAF_HAS_MIN, LEAF_KIND_FILE, LEAF_KIND_TABLE};
use super::series_merkle::merkle_root;
use super::{Cursor, ObjectHash, push_len_prefixed};

/// Magic header for a `watertown.series.v1` root object.
///
/// `pub(crate)`: [`super::series_dispatch`] dispatches a fetched series
/// object between this (v2) and [`super::tree::SERIES_MAGIC`] (v1) by
/// inspecting the same magic bytes, so it must reference this exact constant
/// rather than risk a second, potentially-divergent literal.
pub(crate) const MANIFEST_MAGIC: &[u8] = b"watertown.series.v1\n";

/// Known `bounds_flags` bits; any other bit set is a decode error, matching
/// [`super::series_leaf`]'s and [`super::tree`]'s "unknown flag" convention.
const KNOWN_BOUNDS_FLAGS: u8 = LEAF_HAS_MIN | LEAF_HAS_MAX;

/// Which kind of logical payload a series' leaves carry.
///
/// Wire values are exactly [`super::series_leaf::LEAF_KIND_TABLE`] /
/// [`super::series_leaf::LEAF_KIND_FILE`] -- the same per-leaf `payload_kind`
/// byte -- so the series-level and leaf-level notions of "table or file" can
/// never silently diverge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadKind {
    /// Leaves are `TablePhysicalSeries` rows; a schema fingerprint is
    /// required.
    Table,
    /// Leaves are `FilePhysicalSeries` byte ranges; a schema fingerprint is
    /// forbidden.
    File,
}

impl PayloadKind {
    fn to_wire(self) -> u8 {
        match self {
            PayloadKind::Table => LEAF_KIND_TABLE,
            PayloadKind::File => LEAF_KIND_FILE,
        }
    }

    fn from_wire(byte: u8) -> Result<Self, String> {
        match byte {
            LEAF_KIND_TABLE => Ok(PayloadKind::Table),
            LEAF_KIND_FILE => Ok(PayloadKind::File),
            other => Err(format!("unknown series payload kind byte: {other}")),
        }
    }
}

/// The `watertown.series.v1` root object: one logical series' fetchable identity.
///
/// See the module docs for exactly what this commits to and why packs are
/// excluded. Construct with [`SeriesManifest::new`] (or decode existing
/// bytes with [`SeriesManifest::decode`]); both apply the same invariants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesManifest {
    payload_kind: PayloadKind,
    schema_fingerprint: Option<ObjectHash>,
    logical_count: u64,
    leaf_count: u64,
    min_event_time: Option<i64>,
    max_event_time: Option<i64>,
    logical_attributes: Option<Vec<u8>>,
    leaf_merkle_root: ObjectHash,
}

impl SeriesManifest {
    /// Construct a validated `watertown.series.v1` root object.
    ///
    /// `logical_attributes`, when given, must already be canonical logical-
    /// attribute bytes exactly as
    /// [`super::series_leaf::encode_canonical_attributes`] would produce them
    /// (recursively sorted object keys, no insignificant whitespace); pass
    /// `None`, not `Some(b"{}".to_vec())`, for "no logical attributes at
    /// all" -- an absent value and an empty object are distinct, matching
    /// the per-leaf convention.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `payload_kind` is [`PayloadKind::Table`] and `schema_fingerprint` is
    ///   `None`, or [`PayloadKind::File`] and `schema_fingerprint` is `Some`;
    /// - `logical_attributes` is `Some` but not canonical JSON object bytes,
    ///   or is `Some(&[])` (which must instead be `None`);
    /// - `leaf_count == 0` and `leaf_merkle_root` is not the empty Merkle
    ///   root, or `leaf_count > 0` and it is.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        payload_kind: PayloadKind,
        schema_fingerprint: Option<ObjectHash>,
        logical_count: u64,
        leaf_count: u64,
        min_event_time: Option<i64>,
        max_event_time: Option<i64>,
        logical_attributes: Option<Vec<u8>>,
        leaf_merkle_root: ObjectHash,
    ) -> Result<Self, String> {
        validate(
            payload_kind,
            schema_fingerprint,
            logical_count,
            leaf_count,
            &logical_attributes,
            leaf_merkle_root,
        )?;
        Ok(Self {
            payload_kind,
            schema_fingerprint,
            logical_count,
            leaf_count,
            min_event_time,
            max_event_time,
            logical_attributes,
            leaf_merkle_root,
        })
    }

    /// The series' payload kind.
    #[must_use]
    pub fn payload_kind(&self) -> PayloadKind {
        self.payload_kind
    }

    /// The table schema fingerprint, or `None` for a file series.
    #[must_use]
    pub fn schema_fingerprint(&self) -> Option<ObjectHash> {
        self.schema_fingerprint
    }

    /// The aggregate logical count: total rows (table) or total bytes
    /// (file) across every leaf.
    #[must_use]
    pub fn logical_count(&self) -> u64 {
        self.logical_count
    }

    /// The number of logical leaves in the series.
    #[must_use]
    pub fn leaf_count(&self) -> u64 {
        self.leaf_count
    }

    /// The aggregate minimum event time across every leaf, if any leaf
    /// carried one.
    #[must_use]
    pub fn min_event_time(&self) -> Option<i64> {
        self.min_event_time
    }

    /// The aggregate maximum event time across every leaf, if any leaf
    /// carried one.
    #[must_use]
    pub fn max_event_time(&self) -> Option<i64> {
        self.max_event_time
    }

    /// The canonical logical attributes bytes, if any were set.
    #[must_use]
    pub fn logical_attributes(&self) -> Option<&[u8]> {
        self.logical_attributes.as_deref()
    }

    /// The [`super::series_merkle`] root over this series' ordered leaf
    /// hashes.
    #[must_use]
    pub fn leaf_merkle_root(&self) -> ObjectHash {
        self.leaf_merkle_root
    }

    /// Serialize this object into its `watertown.series.v1` wire bytes:
    ///
    /// ```text
    /// MANIFEST_MAGIC
    /// u8      payload_kind
    /// u32 LE  schema_fingerprint length (0 or 32) + bytes
    /// u64 LE  logical_count
    /// u64 LE  leaf_count
    /// u8      bounds_flags
    /// [i64 LE min_event_time]
    /// [i64 LE max_event_time]
    /// u32 LE  logical_attributes length (0 = absent) + bytes
    /// 32      leaf_merkle_root
    /// ```
    ///
    /// These bytes *are* the object; [`SeriesManifest::hash`] is `blake3` of
    /// exactly this encoding.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64 + self.logical_attributes.as_ref().map_or(0, Vec::len));
        buf.extend_from_slice(MANIFEST_MAGIC);
        buf.push(self.payload_kind.to_wire());
        match self.schema_fingerprint {
            Some(h) => push_len_prefixed(&mut buf, h.as_bytes()),
            None => push_len_prefixed(&mut buf, &[]),
        }
        buf.extend_from_slice(&self.logical_count.to_le_bytes());
        buf.extend_from_slice(&self.leaf_count.to_le_bytes());
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
            Some(attrs) => push_len_prefixed(&mut buf, attrs),
            None => push_len_prefixed(&mut buf, &[]),
        }
        buf.extend_from_slice(self.leaf_merkle_root.as_bytes());
        buf
    }

    /// This object's content address: `blake3` of [`SeriesManifest::encode`].
    #[must_use]
    pub fn hash(&self) -> ObjectHash {
        ObjectHash::of_bytes(&self.encode())
    }

    /// Decode a `watertown.series.v1` root object (the inverse of
    /// [`SeriesManifest::encode`]), applying the same invariants as
    /// [`SeriesManifest::new`].
    ///
    /// # Errors
    ///
    /// Returns an error if the magic header is wrong, the buffer is
    /// truncated or has trailing bytes, `payload_kind` is not a known byte,
    /// the schema fingerprint field is a length other than `0` or `32`,
    /// `bounds_flags` has an unknown bit set, the logical attributes bytes
    /// (if any) are not canonical JSON object bytes, or any of
    /// [`SeriesManifest::new`]'s invariants fail.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let mut cur = Cursor::new(bytes);
        cur.expect_tag(MANIFEST_MAGIC)?;
        let payload_kind = PayloadKind::from_wire(cur.take_u8()?)?;
        let schema_bytes = cur.take_len_prefixed()?;
        let schema_fingerprint = if schema_bytes.is_empty() {
            None
        } else if schema_bytes.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(schema_bytes);
            Some(ObjectHash::from_bytes(arr))
        } else {
            return Err(format!(
                "schema fingerprint must be 0 or 32 bytes, got {}",
                schema_bytes.len()
            ));
        };
        let logical_count = cur.take_u64()?;
        let leaf_count = cur.take_u64()?;
        let flags = cur.take_u8()?;
        if flags & !KNOWN_BOUNDS_FLAGS != 0 {
            return Err(format!("unknown series bounds flags: {flags:#04x}"));
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
        let leaf_merkle_root = cur.take_hash()?;
        if !cur.is_empty() {
            return Err(format!(
                "{} trailing byte(s) after series manifest",
                cur.remaining()
            ));
        }
        Self::new(
            payload_kind,
            schema_fingerprint,
            logical_count,
            leaf_count,
            min_event_time,
            max_event_time,
            logical_attributes,
            leaf_merkle_root,
        )
    }
}

/// Shared invariant checks for [`SeriesManifest::new`] and
/// [`SeriesManifest::decode`].
fn validate(
    payload_kind: PayloadKind,
    schema_fingerprint: Option<ObjectHash>,
    logical_count: u64,
    leaf_count: u64,
    logical_attributes: &Option<Vec<u8>>,
    leaf_merkle_root: ObjectHash,
) -> Result<(), String> {
    if (leaf_count == 0) != (logical_count == 0) {
        return Err(format!(
            "logical_count and leaf_count must both be zero or both be nonzero, got \
             logical_count={logical_count} leaf_count={leaf_count}"
        ));
    }
    match (payload_kind, schema_fingerprint) {
        (PayloadKind::Table, None) => {
            return Err("a table series requires a schema fingerprint".to_string());
        }
        (PayloadKind::File, Some(_)) => {
            return Err("a file series must not carry a schema fingerprint".to_string());
        }
        _ => {}
    }
    if let Some(attrs) = logical_attributes {
        if attrs.is_empty() {
            return Err(
                "logical attributes must be None, not Some(&[]), to mean \"absent\"".to_string(),
            );
        }
        validate_canonical_attributes(attrs)?;
    }
    let empty_root = merkle_root(&[]);
    if leaf_count == 0 && leaf_merkle_root != empty_root {
        return Err(
            "leaf_count is zero but leaf_merkle_root is not the empty Merkle root".to_string(),
        );
    }
    if leaf_count > 0 && leaf_merkle_root == empty_root {
        return Err(
            "leaf_merkle_root is the empty Merkle root but leaf_count is nonzero".to_string(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(s: &str) -> ObjectHash {
        ObjectHash::of_bytes(s.as_bytes())
    }

    fn valid_table() -> SeriesManifest {
        SeriesManifest::new(
            PayloadKind::Table,
            Some(h("schema")),
            100,
            3,
            Some(10),
            Some(20),
            None,
            merkle_root(&[h("l1"), h("l2"), h("l3")]),
        )
        .unwrap()
    }

    fn valid_file() -> SeriesManifest {
        SeriesManifest::new(
            PayloadKind::File,
            None,
            4096,
            2,
            None,
            None,
            None,
            merkle_root(&[h("l1"), h("l2")]),
        )
        .unwrap()
    }

    #[test]
    fn round_trips_encode_decode() {
        for m in [valid_table(), valid_file()] {
            let bytes = m.encode();
            let decoded = SeriesManifest::decode(&bytes).unwrap();
            assert_eq!(decoded, m);
            assert_eq!(decoded.encode(), bytes);
        }
    }

    #[test]
    fn hash_is_blake3_of_encode() {
        let m = valid_table();
        assert_eq!(m.hash(), ObjectHash::of_bytes(&m.encode()));
    }

    #[test]
    fn golden_table_manifest() {
        let manifest = valid_table();
        assert_eq!(
            hex::encode(manifest.encode()),
            "7761746572746f776e2e7365726965732e76310a002000000020fdd34e632861ac10bc22672eb7d669a6b2e85f3ae40f33b6ff2459bddfd87464000000000000000300000000000000030a00000000000000140000000000000000000000377dc0ab9f3b6f23909372652bbc205a8290aa7db1b46bca7a9227edf4d9820c"
        );
        assert_eq!(
            manifest.hash().to_hex(),
            "12b8b877162d594a5c9cb9d1dc1f76d0754ea34ffd204ecb76f471261055552b"
        );
    }

    #[test]
    fn table_requires_schema() {
        let err = SeriesManifest::new(
            PayloadKind::Table,
            None,
            1,
            1,
            None,
            None,
            None,
            merkle_root(&[h("l1")]),
        )
        .unwrap_err();
        assert!(err.contains("schema"));
    }

    #[test]
    fn file_forbids_schema() {
        let err = SeriesManifest::new(
            PayloadKind::File,
            Some(h("schema")),
            1,
            1,
            None,
            None,
            None,
            merkle_root(&[h("l1")]),
        )
        .unwrap_err();
        assert!(err.contains("schema"));
    }

    #[test]
    fn zero_leaf_count_requires_empty_root() {
        let err = SeriesManifest::new(
            PayloadKind::File,
            None,
            0,
            0,
            None,
            None,
            None,
            h("not-the-empty-root"),
        )
        .unwrap_err();
        assert!(err.contains("empty"));
        // The correct pairing succeeds.
        assert!(
            SeriesManifest::new(
                PayloadKind::File,
                None,
                0,
                0,
                None,
                None,
                None,
                merkle_root(&[])
            )
            .is_ok()
        );
    }

    #[test]
    fn logical_count_and_leaf_count_zero_state_must_agree() {
        assert!(
            SeriesManifest::new(
                PayloadKind::File,
                None,
                0,
                1,
                None,
                None,
                None,
                merkle_root(&[h("leaf")]),
            )
            .is_err()
        );
        assert!(
            SeriesManifest::new(
                PayloadKind::File,
                None,
                1,
                0,
                None,
                None,
                None,
                merkle_root(&[]),
            )
            .is_err()
        );
    }

    #[test]
    fn nonzero_leaf_count_rejects_empty_root() {
        let err = SeriesManifest::new(
            PayloadKind::File,
            None,
            10,
            1,
            None,
            None,
            None,
            merkle_root(&[]),
        )
        .unwrap_err();
        assert!(err.contains("empty"));
    }

    #[test]
    fn attributes_must_be_canonical() {
        // Non-canonical (insignificant whitespace) attribute bytes are
        // rejected even though they are valid JSON.
        let non_canonical = b"{ \"a\": 1 }".to_vec();
        let err = SeriesManifest::new(
            PayloadKind::File,
            None,
            1,
            1,
            None,
            None,
            Some(non_canonical),
            merkle_root(&[h("l1")]),
        )
        .unwrap_err();
        assert!(!err.is_empty());

        let canonical =
            super::super::series_leaf::encode_canonical_attributes(r#"{"a":1}"#).unwrap();
        assert!(
            SeriesManifest::new(
                PayloadKind::File,
                None,
                1,
                1,
                None,
                None,
                Some(canonical),
                merkle_root(&[h("l1")]),
            )
            .is_ok()
        );
    }

    #[test]
    fn empty_attributes_vec_must_be_none() {
        let err = SeriesManifest::new(
            PayloadKind::File,
            None,
            1,
            1,
            None,
            None,
            Some(Vec::new()),
            merkle_root(&[h("l1")]),
        )
        .unwrap_err();
        assert!(err.contains("absent"));
    }

    #[test]
    fn decode_rejects_bad_magic() {
        let mut bytes = valid_file().encode();
        bytes[0] ^= 0xff;
        assert!(SeriesManifest::decode(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let mut bytes = valid_file().encode();
        bytes.push(0);
        assert!(SeriesManifest::decode(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_truncation() {
        let bytes = valid_table().encode();
        assert!(SeriesManifest::decode(&bytes[..bytes.len() - 4]).is_err());
    }

    #[test]
    fn decode_rejects_unknown_payload_kind() {
        let mut bytes = valid_file().encode();
        // payload_kind is the first byte right after the magic.
        bytes[MANIFEST_MAGIC.len()] = 0xaa;
        assert!(SeriesManifest::decode(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_unknown_bounds_flags() {
        let m = valid_file();
        let bytes = m.encode();
        // Locate the bounds_flags byte: magic + kind(1) + schema len(4) +
        // schema bytes(0, file series) + logical_count(8) + leaf_count(8).
        let flags_pos = MANIFEST_MAGIC.len() + 1 + 4 + 8 + 8;
        assert_eq!(bytes[flags_pos], 0);
        let mut mutated = bytes.clone();
        mutated[flags_pos] = 0b1000_0000;
        assert!(SeriesManifest::decode(&mutated).is_err());
    }

    #[test]
    fn decode_rejects_bad_schema_fingerprint_length() {
        // Hand-build a table manifest whose schema fingerprint field claims
        // 5 bytes instead of 0 or 32.
        let mut buf = Vec::new();
        buf.extend_from_slice(MANIFEST_MAGIC);
        buf.push(LEAF_KIND_TABLE);
        buf.extend_from_slice(&5u32.to_le_bytes());
        buf.extend_from_slice(&[0u8; 5]);
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.push(0);
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(merkle_root(&[]).as_bytes());
        assert!(SeriesManifest::decode(&buf).is_err());
    }

    #[test]
    fn decode_rejects_oversized_attribute_length_without_huge_alloc() {
        // A hostile object declaring a multi-gigabyte attributes length but
        // carrying no body must fail on truncation, not attempt a huge
        // allocation: `take_len_prefixed` slices the existing buffer rather
        // than allocating from the declared length.
        let mut buf = Vec::new();
        buf.extend_from_slice(MANIFEST_MAGIC);
        buf.push(LEAF_KIND_FILE);
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.push(0);
        buf.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(SeriesManifest::decode(&buf).is_err());
    }

    #[test]
    fn different_bounds_or_attributes_change_hash() {
        let base = valid_table();
        let different_bounds = SeriesManifest::new(
            PayloadKind::Table,
            Some(h("schema")),
            100,
            3,
            Some(11),
            Some(20),
            None,
            merkle_root(&[h("l1"), h("l2"), h("l3")]),
        )
        .unwrap();
        assert_ne!(base.hash(), different_bounds.hash());

        let with_attrs = SeriesManifest::new(
            PayloadKind::Table,
            Some(h("schema")),
            100,
            3,
            Some(10),
            Some(20),
            Some(b"{}".to_vec()),
            merkle_root(&[h("l1"), h("l2"), h("l3")]),
        )
        .unwrap();
        assert_ne!(base.hash(), with_attrs.hash());
    }

    #[test]
    fn append_changes_leaf_root_and_hash() {
        let before = SeriesManifest::new(
            PayloadKind::File,
            None,
            10,
            2,
            None,
            None,
            None,
            merkle_root(&[h("l1"), h("l2")]),
        )
        .unwrap();
        let after = SeriesManifest::new(
            PayloadKind::File,
            None,
            15,
            3,
            None,
            None,
            None,
            merkle_root(&[h("l1"), h("l2"), h("l3")]),
        )
        .unwrap();
        assert_ne!(before.hash(), after.hash());
    }

    #[test]
    fn independently_built_manifests_over_the_same_leaves_match() {
        // A pack-layout-only difference never reaches `SeriesManifest` at
        // all: it has no field a pack could touch. This pins that two
        // independently constructed manifests over the same leaves hash
        // identically, standing in for "repacking does not change series
        // identity" at this layer.
        let a = valid_table();
        let b = SeriesManifest::new(
            PayloadKind::Table,
            Some(h("schema")),
            100,
            3,
            Some(10),
            Some(20),
            None,
            merkle_root(&[h("l1"), h("l2"), h("l3")]),
        )
        .unwrap();
        assert_eq!(a.hash(), b.hash());
    }
}
