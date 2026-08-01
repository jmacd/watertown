// SPDX-License-Identifier: Apache-2.0

//! Node manifest: the one place node identity is recorded for a commit.
//!
//! A tree entry is `(name, entry_type, child_hash)` and carries no `node_id`,
//! so the pure content objects (trees, blobs, series) dedup across ponds.  But
//! a consumer that pulls a pond and wants to update it incrementally must know
//! *which* node each position is, so a rename stays a rename and a node's
//! version history is preserved.  The node manifest supplies exactly that, out
//! of band from the pure content: one row per node, listing the source's real
//! `node_id` alongside its parent, name, type, and content address.
//!
//! The manifest is itself a content-addressed object, referenced by the commit
//! via `node_manifest_hash` (so it dedups across commits when the structure is
//! unchanged) and folded into lineage there.  A read-only mirror adopts these
//! ids verbatim, becoming row-identical to the source and turning incremental
//! pull into a `node_id`-keyed diff.  See
//! `docs/content-addressed-pond-design.md` Sections 4.5 and 8.5.2 (Decision
//! D8).

use tinyfs::EntryType;

use super::tree::{push_version_metas, take_version_metas};
use super::{Cursor, ObjectHash, VersionMeta, push_len_prefixed};

/// Magic header distinguishing a serialized node manifest from a raw blob (D2).
const MANIFEST_MAGIC: &[u8] = b"dp.manifest.2\n";

/// Minimum on-wire size of a single manifest entry: three length-prefixed
/// strings (4-byte length each, all empty), a 1-byte entry-type discriminant,
/// and a 32-byte child hash. Used to bound decode pre-allocation against a
/// hostile element count.
const MANIFEST_ENTRY_MIN_BYTES: usize = 4 + 4 + 4 + 1 + 32 + 4;

/// One node's identity record in a manifest.
///
/// The `node_id` is the source's real `NodeID`; `parent_node_id` is its
/// directory's `NodeID` (empty for the root, which has no parent).  `name`,
/// `entry_type`, and `child_hash` mirror the node's tree entry so a consumer
/// can locate the matching content object without consulting the tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestEntry {
    /// The source's `node_id` for this node.
    pub node_id: String,
    /// The `node_id` of this node's parent directory, or empty for the root.
    pub parent_node_id: String,
    /// The node's name within its parent (empty for the root).
    pub name: String,
    /// The node's entry type.
    pub entry_type: EntryType,
    /// The node's content address (the same `child_hash` its tree entry
    /// carries; for the root, its `tree_hash`).
    pub child_hash: ObjectHash,
    /// Node metadata for this node's live versions, oldest first -- the same
    /// list its tree entry carries, mirrored here so an incremental pull can
    /// diff and reapply it from the `node_id`-keyed manifest alone.
    pub versions: Vec<VersionMeta>,
}

impl ManifestEntry {
    /// Construct a manifest entry.
    #[must_use]
    pub fn new(
        node_id: impl Into<String>,
        parent_node_id: impl Into<String>,
        name: impl Into<String>,
        entry_type: EntryType,
        child_hash: ObjectHash,
        versions: Vec<VersionMeta>,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            parent_node_id: parent_node_id.into(),
            name: name.into(),
            entry_type,
            child_hash,
            versions,
        }
    }

    /// Construct a manifest entry whose node metadata is unknown.
    #[must_use]
    pub fn bare(
        node_id: impl Into<String>,
        parent_node_id: impl Into<String>,
        name: impl Into<String>,
        entry_type: EntryType,
        child_hash: ObjectHash,
    ) -> Self {
        Self::new(
            node_id,
            parent_node_id,
            name,
            entry_type,
            child_hash,
            Vec::new(),
        )
    }
}

/// Serialize a manifest's entries into the canonical wire format.
///
/// The layout is:
///
/// ```text
/// MANIFEST_MAGIC
/// u32 LE  entry count
/// repeated, entries sorted by node_id bytes ascending:
///   u32 LE  node_id length + node_id bytes (UTF-8)
///   u32 LE  parent_node_id length + parent_node_id bytes (UTF-8)
///   u32 LE  name length + name bytes (UTF-8)
///   u8      entry_type discriminant
///   32      child_hash bytes
///   u32 LE  version count, then that many VersionMeta records
/// ```
///
/// Entries are sorted by `node_id` so the encoding is canonical regardless of
/// input order; the returned bytes *are* the manifest object, and its
/// [`manifest_hash`] is `blake3` of these bytes.
///
/// # Errors
///
/// Returns an error if two entries share a `node_id` (each node appears once;
/// an ambiguous manifest must not be silently hashed).
pub fn encode_manifest(entries: &[ManifestEntry]) -> Result<Vec<u8>, String> {
    let mut sorted: Vec<&ManifestEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.node_id.as_bytes().cmp(b.node_id.as_bytes()));

    for pair in sorted.windows(2) {
        if pair[0].node_id == pair[1].node_id {
            return Err(format!(
                "duplicate node_id in manifest: {:?}",
                pair[0].node_id
            ));
        }
    }

    let mut buf = Vec::with_capacity(MANIFEST_MAGIC.len() + 4 + entries.len() * 96);
    buf.extend_from_slice(MANIFEST_MAGIC);
    let count = u32::try_from(sorted.len()).expect("entry count exceeds u32::MAX");
    buf.extend_from_slice(&count.to_le_bytes());
    for entry in sorted {
        push_len_prefixed(&mut buf, entry.node_id.as_bytes());
        push_len_prefixed(&mut buf, entry.parent_node_id.as_bytes());
        push_len_prefixed(&mut buf, entry.name.as_bytes());
        buf.push(entry.entry_type as u8);
        buf.extend_from_slice(entry.child_hash.as_bytes());
        push_version_metas(&mut buf, &entry.versions);
    }
    Ok(buf)
}

/// Compute a manifest's content address: `blake3` over [`encode_manifest`].
///
/// # Errors
///
/// Propagates the duplicate-`node_id` error from [`encode_manifest`].
pub fn manifest_hash(entries: &[ManifestEntry]) -> Result<ObjectHash, String> {
    Ok(ObjectHash::of_bytes(&encode_manifest(entries)?))
}

/// Decode a manifest object back into its entries (the inverse of
/// [`encode_manifest`]).
///
/// The entries are returned in the canonical sorted-by-`node_id` order they
/// were serialized in.
///
/// # Errors
///
/// Returns an error if the magic header is wrong, the buffer is truncated, an
/// entry type byte is not a valid [`EntryType`] discriminant, a field is not
/// valid UTF-8, or there are trailing bytes after the declared entry count.
pub fn decode_manifest(bytes: &[u8]) -> Result<Vec<ManifestEntry>, String> {
    let mut cur = Cursor::new(bytes);
    cur.expect_tag(MANIFEST_MAGIC)?;
    let count = cur.take_u32()? as usize;
    let mut entries = Vec::with_capacity(cur.bounded_capacity(count, MANIFEST_ENTRY_MIN_BYTES));
    for _ in 0..count {
        let node_id = cur.take_len_prefixed_string()?;
        let parent_node_id = cur.take_len_prefixed_string()?;
        let name = cur.take_len_prefixed_string()?;
        let entry_type = EntryType::try_from(cur.take_u8()?)?;
        let child_hash = cur.take_hash()?;
        let versions = take_version_metas(&mut cur)?;
        entries.push(ManifestEntry::new(
            node_id,
            parent_node_id,
            name,
            entry_type,
            child_hash,
            versions,
        ));
    }
    if !cur.is_empty() {
        return Err(format!(
            "{} trailing byte(s) after manifest",
            cur.remaining()
        ));
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(s: &str) -> ObjectHash {
        ObjectHash::of_bytes(s.as_bytes())
    }

    fn file(node_id: &str, parent: &str, name: &str) -> ManifestEntry {
        ManifestEntry::bare(
            node_id,
            parent,
            name,
            EntryType::FilePhysicalVersion,
            h(name),
        )
    }

    #[test]
    fn manifest_hash_is_order_independent() {
        let a = vec![file("n2", "root", "b"), file("n1", "root", "a")];
        let b = vec![file("n1", "root", "a"), file("n2", "root", "b")];
        assert_eq!(manifest_hash(&a).unwrap(), manifest_hash(&b).unwrap());
    }

    #[test]
    fn manifest_hash_changes_with_child_hash() {
        let base = vec![file("n1", "root", "a")];
        let changed = vec![ManifestEntry::bare(
            "n1",
            "root",
            "a",
            EntryType::FilePhysicalVersion,
            h("DIFFERENT"),
        )];
        assert_ne!(
            manifest_hash(&base).unwrap(),
            manifest_hash(&changed).unwrap()
        );
    }

    #[test]
    fn manifest_hash_changes_with_rename() {
        // Same node_id and content, different name -- a rename must change the
        // manifest hash so the consumer detects it.
        let before = vec![file("n1", "root", "old")];
        let after = vec![ManifestEntry::bare(
            "n1",
            "root",
            "new",
            EntryType::FilePhysicalVersion,
            h("old"),
        )];
        assert_ne!(
            manifest_hash(&before).unwrap(),
            manifest_hash(&after).unwrap()
        );
    }

    #[test]
    fn manifest_hash_changes_with_reparent() {
        let before = vec![file("n1", "dirA", "x")];
        let after = vec![file("n1", "dirB", "x")];
        assert_ne!(
            manifest_hash(&before).unwrap(),
            manifest_hash(&after).unwrap()
        );
    }

    #[test]
    fn duplicate_node_ids_rejected() {
        let dup = vec![file("n1", "root", "a"), file("n1", "root", "b")];
        assert!(manifest_hash(&dup).is_err());
    }

    #[test]
    fn decode_round_trips_encode() {
        let entries = vec![
            ManifestEntry::bare("root", "", "", EntryType::DirectoryPhysical, h("roottree")),
            file("n2", "root", "b"),
            file("n1", "root", "a"),
            ManifestEntry::bare("n3", "root", "d", EntryType::DirectoryPhysical, h("dir")),
            ManifestEntry::bare("n4", "root", "s", EntryType::Symlink, h("target")),
            ManifestEntry::bare("n5", "n3", "v", EntryType::TablePhysicalSeries, h("ser")),
            ManifestEntry::bare("n6", "n3", "dyn", EntryType::TableDynamic, h("recipe")),
        ];
        let bytes = encode_manifest(&entries).unwrap();
        let decoded = decode_manifest(&bytes).unwrap();
        let mut expected = entries.clone();
        expected.sort_by(|a, b| a.node_id.as_bytes().cmp(b.node_id.as_bytes()));
        assert_eq!(decoded, expected);
        // Re-encoding the decoded entries reproduces the same bytes.
        assert_eq!(encode_manifest(&decoded).unwrap(), bytes);
    }

    #[test]
    fn decode_handles_empty() {
        let bytes = encode_manifest(&[]).unwrap();
        assert!(decode_manifest(&bytes).unwrap().is_empty());
    }

    #[test]
    fn root_entry_has_empty_parent_and_name() {
        let entries = vec![ManifestEntry::bare(
            "root",
            "",
            "",
            EntryType::DirectoryPhysical,
            h("roottree"),
        )];
        let bytes = encode_manifest(&entries).unwrap();
        let decoded = decode_manifest(&bytes).unwrap();
        assert_eq!(decoded[0].parent_node_id, "");
        assert_eq!(decoded[0].name, "");
    }

    #[test]
    fn decode_rejects_bad_magic_and_trailing() {
        let mut bytes = encode_manifest(&[file("n1", "root", "a")]).unwrap();
        let mut bad_magic = bytes.clone();
        bad_magic[0] ^= 0xff;
        assert!(decode_manifest(&bad_magic).is_err());
        bytes.push(0);
        assert!(decode_manifest(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_truncation() {
        let bytes = encode_manifest(&[file("n1", "root", "a"), file("n2", "root", "b")]).unwrap();
        assert!(decode_manifest(&bytes[..bytes.len() - 4]).is_err());
    }

    #[test]
    fn decode_rejects_oversized_count_without_huge_alloc() {
        // A hostile manifest declaring ~4 billion entries but carrying no body
        // must fail with a truncation error rather than pre-allocating
        // gigabytes: `bounded_capacity` caps the reserve at remaining/min_entry.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MANIFEST_MAGIC);
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_manifest(&bytes).is_err());
    }

    #[test]
    fn manifest_hash_is_blake3_of_encode() {
        let entries = vec![file("n1", "root", "a")];
        assert_eq!(
            manifest_hash(&entries).unwrap(),
            ObjectHash::of_bytes(&encode_manifest(&entries).unwrap())
        );
    }

    #[test]
    fn length_prefix_prevents_field_ambiguity() {
        // Moving a character across the node_id/parent boundary must change the
        // hash, proving the framing is unambiguous.
        let a = vec![ManifestEntry::bare(
            "ab",
            "c",
            "n",
            EntryType::FilePhysicalVersion,
            h("x"),
        )];
        let b = vec![ManifestEntry::bare(
            "a",
            "bc",
            "n",
            EntryType::FilePhysicalVersion,
            h("x"),
        )];
        assert_ne!(manifest_hash(&a).unwrap(), manifest_hash(&b).unwrap());
    }
}
