// SPDX-License-Identifier: Apache-2.0

//! Content-addressed object model (the SPACE and spine layers).
//!
//! This module implements the pure, lineage-independent hashing primitives
//! described in `docs/content-addressed-pond-design.md`.  It deals only in
//! bytes and hashes; it knows nothing about Delta Lake, partitions, or
//! transactions.  Three object kinds are modeled here:
//!
//! - **blob** -- the exact stored bytes of one file version.  A blob's
//!   identity is simply `blake3(bytes)`; there is no dedicated type because a
//!   blob *is* its bytes.  Use [`ObjectHash::of_bytes`].
//! - **tree** -- a directory as its sorted entries, each carrying the node
//!   metadata that directory records for its child (see [`tree`]).
//! - **commit** -- one transaction, the only object carrying lineage (see
//!   [`commit`]).
//!
//! # Metadata is content
//!
//! A version's metadata -- when it was created, the event-time range it
//! covers, its extended attributes -- is data about an immutable version, not
//! an annotation on the side, so a tree entry commits to it along with the
//! child's hash (see [`VersionMeta`]).  Two consequences follow.  A replica,
//! which adopts the source's metadata verbatim, has an identical root tree
//! hash; but two ponds written independently from the same bytes do *not*,
//! because their versions were created at different times.  Blobs stay purely
//! byte-addressed, so identical bytes are still stored and transferred once no
//! matter how many entries describe them.
//!
//! # The store invariant
//!
//! Every object's name is the BLAKE3 hash of its serialized bytes
//! (`name == blake3(bytes)`).  Raw blobs are hashed untagged so a file
//! version's content hash equals the value already recorded in the
//! filesystem.  Structured objects (tree, commit) embed a short magic
//! header in their serialization so their hashes cannot collide with a raw
//! blob that happens to share bytes.
//!
//! # Encoding stability (Decision D2)
//!
//! The byte encodings below are the simplest reasonable choice and are *not*
//! permanently frozen.  Because the project permits a clean reset, the
//! formats can be revised later by resetting all participating ponds.  Any
//! two ponds that sync must share the same encoding version, since hashes do
//! not match across versions.

mod capsule;
mod commit;
mod legacy_capsule;
mod manifest;
mod node_merkle;
mod series_dispatch;
mod series_leaf;
mod series_manifest;
mod series_merkle;
mod series_pack;
mod series_pack_builder;
mod tree;

pub use capsule::{
    CAPSULE_FORMAT_V1, CAPSULE_FORMAT_V2, CAPSULE_FORMAT_V3, CAPSULE_FORMAT_V4,
    CapsuleDynamicMetadata, CapsuleEntry, CapsuleLeaf, CapsuleManifest, CapsuleNode, CapsuleObject,
    CapsulePayloadKind, CapsuleSource, CapsuleVerifyReport, capsule_leaf_hash,
    capsule_manifest_bytes, capsule_root, capsule_series_root, capsule_series_root_v3,
    decode_capsule_manifest, encode_capsule_attributes, read_capsule_manifest,
    verify_capsule_directory, verify_capsule_payload_directory, verify_capsule_payloads,
    verify_incremental_capsule_payload_directory,
};
pub use commit::{Commit, ContentModelVersion, Provenance};
pub use legacy_capsule::{
    LEGACY_CAPSULE_FORMAT, LEGACY_CAPSULE_FORMAT_V1, LEGACY_CAPSULE_FORMAT_V2,
    LEGACY_NATIVE_FORMAT_DP_COMMIT_3, LegacyCapsuleDynamicMetadata, LegacyCapsuleEntry,
    LegacyCapsuleManifest, LegacyCapsuleNode, LegacyCapsuleObject, LegacyCapsulePayloadKind,
    LegacyCapsuleSource, LegacyCapsuleVerifyReport, LegacyCapsuleVersion,
    decode_legacy_capsule_manifest, legacy_capsule_manifest_bytes, legacy_capsule_root,
    read_legacy_capsule_manifest, verify_legacy_capsule_directory,
    verify_legacy_capsule_payload_directory,
};
pub use manifest::{ManifestEntry, decode_manifest, encode_manifest, manifest_hash};
pub use node_merkle::{NodeMerkle, rebuild_root as node_merkle_rebuild_root};
pub use series_dispatch::{FetchedSeriesObject, decode_fetched_series_object};
pub use series_leaf::{
    IncrementalFileLeafHasher, IncrementalTableLeafHasher, canonicalize_schema,
    encode_canonical_attributes, encode_canonical_batch_rows, encode_canonical_schema,
    file_leaf_hash, file_leaf_hash_canonical, schema_fingerprint, table_leaf_hash,
    table_leaf_hash_canonical,
};
pub use series_manifest::{PayloadKind, SeriesManifest, SeriesManifestRevision};
pub use series_merkle::{RangeProof, generate_range_proof, merkle_root, verify_range_proof};
pub use series_pack::{
    PackIndex, PackIndexRevision, PackLeafDescriptor, effective_leaf_schema_fingerprint,
    select_exact_cover, verify_pack_against_manifest,
};
pub use series_pack_builder::{
    BuiltSeriesPack, FileLeafInput, FilePackLayout, TableLeafInput, TablePackLayout,
    build_file_pack, build_table_pack, encode_table_leaf_parquet,
};
pub use tree::{
    TreeEntry, VersionMeta, decode_recipe, decode_series, decode_tree, encode_recipe,
    encode_series, encode_tree, recipe_hash, series_hash, tree_hash,
};

use std::fmt;

/// A 32-byte BLAKE3 content address naming an immutable object.
///
/// An `ObjectHash` is the name of a blob, tree, or commit.  Equality is byte
/// equality of the 32-byte digest.  The hex form (lowercase, 64 chars) is the
/// canonical textual representation and round-trips via [`ObjectHash::to_hex`]
/// and [`ObjectHash::from_hex`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectHash([u8; 32]);

impl ObjectHash {
    /// Wrap raw digest bytes that are already a BLAKE3 output.
    #[must_use]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The 32 raw digest bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Hash an arbitrary byte slice as an untagged blob.
    ///
    /// This is the identity of a file version, a symlink target, or a dynamic
    /// node's config bytes -- any object whose serialized form is exactly its
    /// raw bytes.
    #[must_use]
    pub fn of_bytes(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    /// Lowercase 64-character hex encoding of the digest.
    #[must_use]
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for b in &self.0 {
            s.push(char::from_digit(u32::from(b >> 4), 16).expect("nibble is valid hex digit"));
            s.push(char::from_digit(u32::from(b & 0x0f), 16).expect("nibble is valid hex digit"));
        }
        s
    }

    /// Parse a 64-character lowercase or uppercase hex string into a hash.
    ///
    /// # Errors
    ///
    /// Returns an error string if the input is not exactly 64 hex digits.
    pub fn from_hex(s: &str) -> Result<Self, String> {
        if s.len() != 64 {
            return Err(format!("expected 64 hex chars, got {}", s.len()));
        }
        let mut out = [0u8; 32];
        let bytes = s.as_bytes();
        for (i, slot) in out.iter_mut().enumerate() {
            let hi = hex_val(bytes[i * 2])?;
            let lo = hex_val(bytes[i * 2 + 1])?;
            *slot = (hi << 4) | lo;
        }
        Ok(Self(out))
    }
}

impl fmt::Display for ObjectHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

fn hex_val(c: u8) -> Result<u8, String> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        other => Err(format!("invalid hex digit: {:?}", other as char)),
    }
}

/// Append a `u32`-length-prefixed byte field to `buf`.
///
/// Shared by the tree and commit encoders to frame variable-length fields
/// (names, pond ids, free-form provenance strings) unambiguously.
pub(crate) fn push_len_prefixed(buf: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("field length exceeds u32::MAX");
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(bytes);
}

/// Append a `u64`-length-prefixed byte field to `buf`.
///
/// Used by [`series_leaf`] for the fields `docs/logical-series-identity-design.md`
/// specifies as `u64`-framed (the canonical row/file payload and each
/// variable-width scalar), which can exceed what a `u32` length can address.
pub(crate) fn push_len_prefixed_u64(buf: &mut Vec<u8>, bytes: &[u8]) {
    let len = u64::try_from(bytes.len()).expect("field length exceeds u64::MAX");
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(bytes);
}

/// A minimal forward cursor for decoding the content wire formats.
///
/// Shared by the tree, series, and commit decoders so the framing logic
/// (length-prefixed fields, fixed-width hashes, trailing-byte rejection) lives
/// in exactly one place, mirroring the shared [`push_len_prefixed`] encoder.
pub(crate) struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    /// Bound a pre-allocation to what the remaining buffer could plausibly
    /// hold, so a malicious or corrupt object cannot force a multi-gigabyte
    /// allocation via an oversized element count.
    ///
    /// `min_elem_bytes` is the smallest number of bytes a single element can
    /// occupy on the wire. The returned capacity never exceeds
    /// `remaining / min_elem_bytes`, which is an upper bound on the number of
    /// elements actually present; a genuinely large object simply grows the
    /// `Vec` as it decodes.
    pub(crate) fn bounded_capacity(&self, count: usize, min_elem_bytes: usize) -> usize {
        debug_assert!(min_elem_bytes > 0, "min_elem_bytes must be non-zero");
        count.min(self.remaining() / min_elem_bytes.max(1))
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.remaining() < n {
            return Err(format!(
                "truncated: need {n} byte(s), have {}",
                self.remaining()
            ));
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    pub(crate) fn expect_tag(&mut self, tag: &[u8]) -> Result<(), String> {
        let got = self.take(tag.len())?;
        if got != tag {
            return Err(format!(
                "bad magic header: expected {expected:?}, found {found:?}",
                expected = String::from_utf8_lossy(tag),
                found = String::from_utf8_lossy(got)
            ));
        }
        Ok(())
    }

    pub(crate) fn take_u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn take_hash(&mut self) -> Result<ObjectHash, String> {
        let slice = self.take(32)?;
        let mut arr = [0u8; 32];
        arr.copy_from_slice(slice);
        Ok(ObjectHash::from_bytes(arr))
    }

    pub(crate) fn take_u32(&mut self) -> Result<u32, String> {
        let slice = self.take(4)?;
        let mut arr = [0u8; 4];
        arr.copy_from_slice(slice);
        Ok(u32::from_le_bytes(arr))
    }

    pub(crate) fn take_i64(&mut self) -> Result<i64, String> {
        let slice = self.take(8)?;
        let mut arr = [0u8; 8];
        arr.copy_from_slice(slice);
        Ok(i64::from_le_bytes(arr))
    }

    pub(crate) fn take_u64(&mut self) -> Result<u64, String> {
        let slice = self.take(8)?;
        let mut arr = [0u8; 8];
        arr.copy_from_slice(slice);
        Ok(u64::from_le_bytes(arr))
    }

    pub(crate) fn take_len_prefixed(&mut self) -> Result<&'a [u8], String> {
        let len = self.take_u32()? as usize;
        self.take(len)
    }

    pub(crate) fn take_len_prefixed_string(&mut self) -> Result<String, String> {
        let bytes = self.take_len_prefixed()?;
        String::from_utf8(bytes.to_vec()).map_err(|e| format!("invalid utf-8: {e}"))
    }

    /// Consume and return all remaining bytes, leaving the cursor at the end.
    /// Used for trailing variable-length payloads (for example, a recipe's
    /// config bytes).
    pub(crate) fn take_rest(&mut self) -> &'a [u8] {
        let out = &self.buf[self.pos..];
        self.pos = self.buf.len();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn of_bytes_matches_blake3() {
        let h = ObjectHash::of_bytes(b"hello");
        assert_eq!(h.as_bytes(), blake3::hash(b"hello").as_bytes());
    }

    #[test]
    fn hex_round_trips() {
        let h = ObjectHash::of_bytes(b"watertown");
        let hex = h.to_hex();
        assert_eq!(hex.len(), 64);
        assert_eq!(ObjectHash::from_hex(&hex).unwrap(), h);
    }

    #[test]
    fn hex_accepts_uppercase() {
        let h = ObjectHash::of_bytes(b"x");
        let upper = h.to_hex().to_uppercase();
        assert_eq!(ObjectHash::from_hex(&upper).unwrap(), h);
    }

    #[test]
    fn hex_rejects_bad_input() {
        assert!(ObjectHash::from_hex("abc").is_err());
        assert!(ObjectHash::from_hex(&"z".repeat(64)).is_err());
    }

    #[test]
    fn distinct_inputs_differ() {
        assert_ne!(ObjectHash::of_bytes(b"a"), ObjectHash::of_bytes(b"b"));
    }
}
