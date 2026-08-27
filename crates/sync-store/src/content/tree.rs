// SPDX-License-Identifier: Apache-2.0

//! Tree objects: a directory hashed as its sorted entries, content only.
//!
//! A tree commits to a directory's immediate children -- nothing else.  The
//! recursive fold (a subdirectory contributes its own `tree_hash` as its
//! `child_hash`) is what gives the design its load-bearing property: equal
//! `tree_hash` means an identical subtree in a single comparison, and any
//! change to any descendant changes every ancestor hash on the path to the
//! root.  See `docs/content-addressed-pond-design.md` Section 4.2.
//!
//! # `child_hash` by node kind (Section 9)
//!
//! The hash an entry contributes to its parent depends on the node kind:
//!
//! | Node kind                         | `child_hash`                                   |
//! |-----------------------------------|------------------------------------------------|
//! | file (physical version)           | `blake3(version bytes)` -- the blob hash       |
//! | series / multi-version file       | [`series_hash`] over all version blob hashes   |
//! | directory                         | the subtree's [`tree_hash`]                    |
//! | symlink                           | `blake3(target path)` -- [`ObjectHash::of_bytes`] |
//! | dynamic dir / `table:dynamic`     | `blake3(stored config bytes)` -- the recipe    |
//!
//! For files and directories the `child_hash` is simply the blob or subtree
//! hash already in hand, so no helper is needed.  For symlinks and dynamic
//! nodes the bytes (target path, config) are hashed untagged via
//! [`ObjectHash::of_bytes`].  Series use [`series_hash`].

use tinyfs::EntryType;

use super::{Cursor, ObjectHash, push_len_prefixed};

/// Magic header distinguishing a serialized tree from a raw blob (D2).
///
/// Version 2 adds per-version [`VersionMeta`] to each entry: a directory now
/// records what it knows about its children, mirroring a real filesystem where
/// the node's metadata lives beside the name that refers to it.
const TREE_MAGIC: &[u8] = b"dp.tree.2\n";

/// Magic header for the cumulative series hash (D2).
///
/// `pub(crate)`: [`super::series_dispatch`] dispatches a fetched series
/// object between this (v1) and [`super::series_manifest::MANIFEST_MAGIC`]
/// (v2) by inspecting the same magic bytes, so it must reference this exact
/// constant rather than risk a second, potentially-divergent literal.
pub(crate) const SERIES_MAGIC: &[u8] = b"dp.series.1\n";

/// Magic header for a dynamic-node recipe object (D2/D4).
const RECIPE_MAGIC: &[u8] = b"dp.recipe.1\n";

/// Minimum on-wire size of a single tree entry: a length-prefixed name (4-byte
/// length + 0 bytes for an empty name), a 1-byte entry-type discriminant, a
/// 32-byte child hash, and a 4-byte version count (zero versions). Used to
/// bound decode pre-allocation against a hostile element count.
const TREE_ENTRY_MIN_BYTES: usize = 4 + 1 + 32 + 4;

/// Minimum on-wire size of one version's metadata: the presence-flag byte
/// alone, with every field absent.
const VERSION_META_MIN_BYTES: usize = 1;

/// Size of an [`ObjectHash`] on the wire (a series version hash).
const HASH_BYTES: usize = 32;

/// Presence bit for [`VersionMeta::min_event_time`].
const META_HAS_MIN: u8 = 0b0000_0001;
/// Presence bit for [`VersionMeta::max_event_time`].
const META_HAS_MAX: u8 = 0b0000_0010;
/// Presence bit for [`VersionMeta::extended_attributes`].
const META_HAS_ATTRS: u8 = 0b0000_0100;
/// Presence bit for [`VersionMeta::timestamp`].
const META_HAS_MTIME: u8 = 0b0000_1000;

/// Every defined [`VersionMeta`] presence bit; any other bit set on the wire is
/// from a newer encoding this build cannot faithfully round-trip.
const META_KNOWN_FLAGS: u8 = META_HAS_MIN | META_HAS_MAX | META_HAS_ATTRS | META_HAS_MTIME;

/// Per-version node metadata a directory records about one of its children.
///
/// The content-addressed model reduces a node to a `child_hash`, on the
/// implicit assumption that everything else about it is either structural
/// (name, type, parent) or a pure function of its bytes (blake3, size, a
/// parquet footer's min/max). Real node metadata is neither: an mtime is a fact
/// about *when the node was written*, and a raw JSON-lines blob's event-time
/// range depends on the ingest configuration of the pond that created it --
/// which key holds the event time, in which unit. A replica receiving only
/// bytes cannot recover either, so both must travel as state.
///
/// Historically they did: before the content-addressed rewrite, replication
/// shipped whole `OplogEntry` rows and these columns rode along for free. This
/// struct is exactly that row's replicable metadata, restored to the wire --
/// which is also why it lives on the *directory entry* rather than inside the
/// node's own content object. A filesystem's directory holds names that refer
/// to nodes, and the metadata belongs beside the name; keeping it out of the
/// blob and series objects leaves those purely content-addressed, so identical
/// bytes still dedup no matter what metadata describes them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VersionMeta {
    /// When this version was written, in microseconds since the Unix epoch
    /// (the node's mtime). A replica adopts the source's value rather than
    /// minting its own, exactly as `rsync -t` or `cp -p` preserve mtime.
    pub timestamp: Option<i64>,
    /// Smallest event time observed in this version's data, in microseconds.
    pub min_event_time: Option<i64>,
    /// Largest event time observed in this version's data, in microseconds.
    pub max_event_time: Option<i64>,
    /// The version's extended attributes as their stored JSON object (it
    /// carries, among other things, the series' timestamp column name).
    ///
    /// Producers must serialize this canonically -- the stored form comes from
    /// a `HashMap`, whose key order is nondeterministic, and this string is
    /// hashed. Two ponds that emitted different key orders for equal attributes
    /// would never converge.
    pub extended_attributes: Option<String>,
}

impl VersionMeta {
    /// Whether this version knows a complete event-time range.
    #[must_use]
    pub fn bounds(&self) -> Option<(i64, i64)> {
        self.min_event_time.zip(self.max_event_time)
    }

    /// Whether nothing at all is known about this version.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.timestamp.is_none()
            && self.min_event_time.is_none()
            && self.max_event_time.is_none()
            && self.extended_attributes.is_none()
    }
}

/// One entry in a tree: a named child with its type, content address, and the
/// node metadata this directory records for it.
///
/// `(name, entry_type, child_hash)` is the child's structural and content
/// identity; `versions` is what the directory knows about the node that name
/// refers to -- one [`VersionMeta`] per live version, oldest first. A
/// single-version file has one, a series has one per version, a directory has
/// none. Because it is part of the entry, a metadata-only change moves this
/// directory's `tree_hash` and every ancestor hash, so replication notices and
/// repairs it without any node's own content object changing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEntry {
    /// The child's name within this directory.
    pub name: String,
    /// The child's entry type.
    pub entry_type: EntryType,
    /// The child's content address (see the module table for how it is
    /// derived per node kind).
    pub child_hash: ObjectHash,
    /// Node metadata for the child's live versions, oldest first.
    pub versions: Vec<VersionMeta>,
}

impl TreeEntry {
    /// Construct a tree entry carrying node metadata for its child's versions.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        entry_type: EntryType,
        child_hash: ObjectHash,
        versions: Vec<VersionMeta>,
    ) -> Self {
        Self {
            name: name.into(),
            entry_type,
            child_hash,
            versions,
        }
    }

    /// Construct a tree entry whose child metadata is unknown.
    #[must_use]
    pub fn bare(name: impl Into<String>, entry_type: EntryType, child_hash: ObjectHash) -> Self {
        Self::new(name, entry_type, child_hash, Vec::new())
    }
}

/// Serialize one version's metadata: a presence-flag byte then only the fields
/// that are present.
pub(crate) fn push_version_meta(buf: &mut Vec<u8>, meta: &VersionMeta) {
    let mut flags = 0u8;
    if meta.min_event_time.is_some() {
        flags |= META_HAS_MIN;
    }
    if meta.max_event_time.is_some() {
        flags |= META_HAS_MAX;
    }
    if meta.extended_attributes.is_some() {
        flags |= META_HAS_ATTRS;
    }
    if meta.timestamp.is_some() {
        flags |= META_HAS_MTIME;
    }
    buf.push(flags);
    if let Some(min) = meta.min_event_time {
        buf.extend_from_slice(&min.to_le_bytes());
    }
    if let Some(max) = meta.max_event_time {
        buf.extend_from_slice(&max.to_le_bytes());
    }
    if let Some(attrs) = &meta.extended_attributes {
        push_len_prefixed(buf, attrs.as_bytes());
    }
    if let Some(mtime) = meta.timestamp {
        buf.extend_from_slice(&mtime.to_le_bytes());
    }
}

/// Serialize a whole per-version metadata list: a count then each entry.
pub(crate) fn push_version_metas(buf: &mut Vec<u8>, versions: &[VersionMeta]) {
    let count = u32::try_from(versions.len()).expect("version count exceeds u32::MAX");
    buf.extend_from_slice(&count.to_le_bytes());
    for meta in versions {
        push_version_meta(buf, meta);
    }
}

/// Deserialize one version's metadata (the inverse of [`push_version_meta`]).
///
/// # Errors
///
/// Returns an error if the buffer is truncated or a presence flag outside
/// [`META_KNOWN_FLAGS`] is set -- an unknown field cannot be skipped, because
/// its width is unknown, and silently dropping node state is what this encoding
/// exists to prevent.
pub(crate) fn take_version_meta(cur: &mut Cursor<'_>) -> Result<VersionMeta, String> {
    let flags = cur.take_u8()?;
    if flags & !META_KNOWN_FLAGS != 0 {
        return Err(format!("unknown version metadata flags: {flags:#04x}"));
    }
    let min_event_time = if flags & META_HAS_MIN != 0 {
        Some(cur.take_i64()?)
    } else {
        None
    };
    let max_event_time = if flags & META_HAS_MAX != 0 {
        Some(cur.take_i64()?)
    } else {
        None
    };
    let extended_attributes = if flags & META_HAS_ATTRS != 0 {
        Some(cur.take_len_prefixed_string()?)
    } else {
        None
    };
    let timestamp = if flags & META_HAS_MTIME != 0 {
        Some(cur.take_i64()?)
    } else {
        None
    };
    Ok(VersionMeta {
        timestamp,
        min_event_time,
        max_event_time,
        extended_attributes,
    })
}

/// Deserialize a per-version metadata list (the inverse of
/// [`push_version_metas`]).
///
/// # Errors
///
/// Propagates truncation and unknown-flag errors from [`take_version_meta`].
pub(crate) fn take_version_metas(cur: &mut Cursor<'_>) -> Result<Vec<VersionMeta>, String> {
    let count = cur.take_u32()? as usize;
    let mut versions = Vec::with_capacity(cur.bounded_capacity(count, VERSION_META_MIN_BYTES));
    for _ in 0..count {
        versions.push(take_version_meta(cur)?);
    }
    Ok(versions)
}

/// Serialize a directory's entries into the canonical tree wire format.
///
/// The layout is:
///
/// ```text
/// TREE_MAGIC
/// u32 LE  entry count
/// repeated, entries sorted by name bytes ascending:
///   u32 LE  name length
///   name bytes (UTF-8)
///   u8      entry_type discriminant
///   32      child_hash bytes
///   u32 LE  version count
///   repeated, one per live version, oldest first:
///     u8    VersionMeta presence flags
///     i64 LE  min_event_time, if present
///     i64 LE  max_event_time, if present
///     u32 LE length + UTF-8 extended_attributes, if present
///     i64 LE  timestamp (mtime), if present
/// ```
///
/// The returned bytes *are* the tree object; its [`tree_hash`] is
/// `blake3` of these bytes.
///
/// # Errors
///
/// Returns an error if two entries share a name (a directory cannot hold two
/// children with the same name, and an ambiguous tree must not be silently
/// hashed).
pub fn encode_tree(entries: &[TreeEntry]) -> Result<Vec<u8>, String> {
    let mut sorted: Vec<&TreeEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));

    for pair in sorted.windows(2) {
        if pair[0].name == pair[1].name {
            return Err(format!("duplicate entry name in tree: {:?}", pair[0].name));
        }
    }

    let mut buf = Vec::with_capacity(TREE_MAGIC.len() + 4 + entries.len() * 48);
    buf.extend_from_slice(TREE_MAGIC);
    let count = u32::try_from(sorted.len()).expect("entry count exceeds u32::MAX");
    buf.extend_from_slice(&count.to_le_bytes());
    for entry in sorted {
        push_len_prefixed(&mut buf, entry.name.as_bytes());
        buf.push(entry.entry_type as u8);
        buf.extend_from_slice(entry.child_hash.as_bytes());
        push_version_metas(&mut buf, &entry.versions);
    }
    Ok(buf)
}

/// Compute the recursive content hash of a directory from its entries.
///
/// This is `blake3` over the [`encode_tree`] serialization.  Because each
/// subdirectory entry carries its own `tree_hash` as `child_hash`, equal
/// results mean identical subtrees.
///
/// # Errors
///
/// Propagates the duplicate-name error from [`encode_tree`].
pub fn tree_hash(entries: &[TreeEntry]) -> Result<ObjectHash, String> {
    Ok(ObjectHash::of_bytes(&encode_tree(entries)?))
}

/// Encode a multi-version series into its content-object bytes.
///
/// A series entry must commit to its *whole* history so the hash is stable
/// across appends: appending a version extends the input deterministically.
/// The input is the ordered list of per-version blob hashes (oldest first):
///
/// ```text
/// SERIES_MAGIC
/// u32 LE  version count
/// repeated: 32 bytes per version blob hash, in order
/// ```
///
/// The series object is deliberately *pure content*: the per-version metadata
/// a replica needs travels on the directory entry that names this series (see
/// [`VersionMeta`]), not here, so two ponds holding identical bytes share this
/// hash regardless of what metadata describes them.
///
/// The returned bytes *are* the series object; its [`series_hash`] is
/// `blake3` of these bytes.  This is the simple-start encoding of the "stable
/// bao root over all versions" the design calls for (D2, not frozen).
#[must_use]
pub fn encode_series(version_hashes: &[ObjectHash]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(SERIES_MAGIC.len() + 4 + version_hashes.len() * HASH_BYTES);
    buf.extend_from_slice(SERIES_MAGIC);
    let count = u32::try_from(version_hashes.len()).expect("version count exceeds u32::MAX");
    buf.extend_from_slice(&count.to_le_bytes());
    for h in version_hashes {
        buf.extend_from_slice(h.as_bytes());
    }
    buf
}

/// Compute the cumulative content-and-history hash of a multi-version series.
///
/// This is `blake3` over the [`encode_series`] serialization.
#[must_use]
pub fn series_hash(version_hashes: &[ObjectHash]) -> ObjectHash {
    ObjectHash::of_bytes(&encode_series(version_hashes))
}

/// Decode a tree object back into its entries (the inverse of [`encode_tree`]).
///
/// The entries are returned in the canonical sorted-by-name order they were
/// serialized in.
///
/// # Errors
///
/// Returns an error if the magic header is wrong, the buffer is truncated, an
/// entry type byte is not a valid [`EntryType`] discriminant, a name is not
/// valid UTF-8, or there are trailing bytes after the declared entry count.
pub fn decode_tree(bytes: &[u8]) -> Result<Vec<TreeEntry>, String> {
    let mut cur = Cursor::new(bytes);
    cur.expect_tag(TREE_MAGIC)?;
    let count = cur.take_u32()? as usize;
    let mut entries = Vec::with_capacity(cur.bounded_capacity(count, TREE_ENTRY_MIN_BYTES));
    for _ in 0..count {
        let name = cur.take_len_prefixed_string()?;
        let entry_type = EntryType::try_from(cur.take_u8()?)?;
        let child_hash = cur.take_hash()?;
        let versions = take_version_metas(&mut cur)?;
        entries.push(TreeEntry::new(name, entry_type, child_hash, versions));
    }
    if !cur.is_empty() {
        return Err(format!("{} trailing byte(s) after tree", cur.remaining()));
    }
    Ok(entries)
}

/// Decode a series object back into its ordered version blob hashes (the
/// inverse of [`encode_series`]).
///
/// # Errors
///
/// Returns an error if the magic header is wrong, the buffer is truncated, or
/// there are trailing bytes after the declared version count.
pub fn decode_series(bytes: &[u8]) -> Result<Vec<ObjectHash>, String> {
    let mut cur = Cursor::new(bytes);
    cur.expect_tag(SERIES_MAGIC)?;
    let count = cur.take_u32()? as usize;
    let mut hashes = Vec::with_capacity(cur.bounded_capacity(count, HASH_BYTES));
    for _ in 0..count {
        hashes.push(cur.take_hash()?);
    }
    if !cur.is_empty() {
        return Err(format!("{} trailing byte(s) after series", cur.remaining()));
    }
    Ok(hashes)
}

/// Encode a dynamic node's recipe -- its factory type plus configuration bytes
/// -- into its content-object bytes.
///
/// The layout is:
///
/// ```text
/// RECIPE_MAGIC
/// u32 LE  factory_type length
/// factory_type bytes (UTF-8)
/// config bytes (to the end of the buffer)
/// ```
///
/// Unlike the earlier definition (config bytes alone), the recipe object
/// commits to *both* the factory type and its config, so two dynamic nodes
/// that share config bytes but invoke different factories hash differently and
/// a consumer can reconstruct which factory to instantiate (design Section 9 /
/// Decision D4).  The config bytes are taken as-is, byte-for-byte, with no
/// canonicalization (D2).
#[must_use]
pub fn encode_recipe(factory_type: &str, config: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(RECIPE_MAGIC.len() + 4 + factory_type.len() + config.len());
    buf.extend_from_slice(RECIPE_MAGIC);
    push_len_prefixed(&mut buf, factory_type.as_bytes());
    buf.extend_from_slice(config);
    buf
}

/// Compute a dynamic node's recipe hash: `blake3` over [`encode_recipe`].
#[must_use]
pub fn recipe_hash(factory_type: &str, config: &[u8]) -> ObjectHash {
    ObjectHash::of_bytes(&encode_recipe(factory_type, config))
}

/// Decode a recipe object into its `(factory_type, config)` parts (the inverse
/// of [`encode_recipe`]).
///
/// # Errors
///
/// Returns an error if the magic header is wrong, the buffer is truncated, or
/// the factory type is not valid UTF-8.
pub fn decode_recipe(bytes: &[u8]) -> Result<(String, Vec<u8>), String> {
    let mut cur = Cursor::new(bytes);
    cur.expect_tag(RECIPE_MAGIC)?;
    let factory_type = cur.take_len_prefixed_string()?;
    let config = cur.take_rest().to_vec();
    Ok((factory_type, config))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(s: &str) -> ObjectHash {
        ObjectHash::of_bytes(s.as_bytes())
    }

    fn file(name: &str, content: &str) -> TreeEntry {
        TreeEntry::bare(name, EntryType::FilePhysicalVersion, h(content))
    }

    #[test]
    fn tree_hash_is_deterministic() {
        let a = vec![file("b", "2"), file("a", "1")];
        let b = vec![file("a", "1"), file("b", "2")];
        // Order of input entries must not matter; sorting is canonical.
        assert_eq!(tree_hash(&a).unwrap(), tree_hash(&b).unwrap());
    }

    #[test]
    fn tree_hash_changes_with_child_content() {
        let base = vec![file("a", "1"), file("b", "2")];
        let changed = vec![file("a", "1"), file("b", "CHANGED")];
        assert_ne!(tree_hash(&base).unwrap(), tree_hash(&changed).unwrap());
    }

    #[test]
    fn tree_hash_changes_with_entry_type() {
        let as_file = vec![file("a", "1")];
        let as_dir = vec![TreeEntry::bare("a", EntryType::DirectoryPhysical, h("1"))];
        assert_ne!(tree_hash(&as_file).unwrap(), tree_hash(&as_dir).unwrap());
    }

    #[test]
    fn recursive_fold_detects_swapped_subtrees() {
        // Two sibling subtrees with the same local entry names but different
        // descendants. Folding the children in must make a swap detectable.
        let left = tree_hash(&[file("x", "left-data")]).unwrap();
        let right = tree_hash(&[file("x", "right-data")]).unwrap();

        let normal = vec![
            TreeEntry::bare("dirA", EntryType::DirectoryPhysical, left),
            TreeEntry::bare("dirB", EntryType::DirectoryPhysical, right),
        ];
        let swapped = vec![
            TreeEntry::bare("dirA", EntryType::DirectoryPhysical, right),
            TreeEntry::bare("dirB", EntryType::DirectoryPhysical, left),
        ];
        assert_ne!(tree_hash(&normal).unwrap(), tree_hash(&swapped).unwrap());
    }

    #[test]
    fn duplicate_names_rejected() {
        let dup = vec![file("a", "1"), file("a", "2")];
        assert!(tree_hash(&dup).is_err());
    }

    #[test]
    fn empty_tree_hashes() {
        // An empty directory has a well-defined, stable hash.
        let e1 = tree_hash(&[]).unwrap();
        let e2 = tree_hash(&[]).unwrap();
        assert_eq!(e1, e2);
        // And it differs from a non-empty tree.
        assert_ne!(e1, tree_hash(&[file("a", "1")]).unwrap());
    }

    #[test]
    fn series_hash_is_order_sensitive_and_stable() {
        let v1 = h("v1");
        let v2 = h("v2");
        assert_eq!(series_hash(&[v1, v2]), series_hash(&[v1, v2]));
        assert_ne!(series_hash(&[v1, v2]), series_hash(&[v2, v1]));
        // Appending a version changes the cumulative hash.
        assert_ne!(series_hash(&[v1]), series_hash(&[v1, v2]));
    }

    #[test]
    fn series_hash_differs_from_blob() {
        let v1 = h("v1");
        // A one-version series must not collide with the raw blob hash.
        assert_ne!(series_hash(&[v1]), v1);
    }

    #[test]
    fn decode_tree_round_trips_encode() {
        let entries = vec![
            file("b", "2"),
            file("a", "1"),
            TreeEntry::bare("d", EntryType::DirectoryPhysical, h("dir")),
            TreeEntry::bare("c", EntryType::Symlink, h("target")),
        ];
        let bytes = encode_tree(&entries).unwrap();
        let decoded = decode_tree(&bytes).unwrap();
        // Decoded entries come back in canonical sorted-by-name order.
        let mut expected = entries.clone();
        expected.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
        assert_eq!(decoded, expected);
        // And re-encoding the decoded entries reproduces the same bytes.
        assert_eq!(encode_tree(&decoded).unwrap(), bytes);
    }

    #[test]
    fn decode_tree_handles_empty() {
        let bytes = encode_tree(&[]).unwrap();
        assert!(decode_tree(&bytes).unwrap().is_empty());
    }

    #[test]
    fn decode_tree_rejects_bad_magic_and_trailing() {
        let mut bytes = encode_tree(&[file("a", "1")]).unwrap();
        let mut bad_magic = bytes.clone();
        bad_magic[0] ^= 0xff;
        assert!(decode_tree(&bad_magic).is_err());
        bytes.push(0);
        assert!(decode_tree(&bytes).is_err());
    }

    #[test]
    fn decode_series_round_trips_encode() {
        let versions = [h("v1"), h("v2"), h("v3")];
        let bytes = encode_series(&versions);
        assert_eq!(decode_series(&bytes).unwrap(), versions);
    }

    #[test]
    fn decode_series_rejects_truncation() {
        let bytes = encode_series(&[h("v1"), h("v2")]);
        assert!(decode_series(&bytes[..bytes.len() - 4]).is_err());
    }

    #[test]
    fn decode_series_rejects_oversized_count_without_huge_alloc() {
        // A hostile object declaring ~4 billion versions but carrying no body
        // must fail with a truncation error rather than pre-allocating
        // gigabytes: `bounded_capacity` caps the reserve at remaining/32.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(SERIES_MAGIC);
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_series(&bytes).is_err());
    }

    #[test]
    fn decode_tree_rejects_oversized_count_without_huge_alloc() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(TREE_MAGIC);
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_tree(&bytes).is_err());
    }

    #[test]
    fn series_hash_is_blake3_of_encode_series() {
        // The encoded bytes ARE the series object; its hash is blake3 of them.
        let versions = [h("v1"), h("v2"), h("v3")];
        assert_eq!(
            series_hash(&versions),
            ObjectHash::of_bytes(&encode_series(&versions))
        );
        // The encoding round-trips structurally: magic + count + 32B per hash.
        let bytes = encode_series(&versions);
        assert_eq!(&bytes[..SERIES_MAGIC.len()], SERIES_MAGIC);
        assert_eq!(bytes.len(), SERIES_MAGIC.len() + 4 + versions.len() * 32);
    }

    #[test]
    fn decode_recipe_round_trips_encode() {
        let config = b"format: csv\npath: /tmp/x\n";
        let bytes = encode_recipe("sql-derived-series", config);
        let (factory, decoded) = decode_recipe(&bytes).unwrap();
        assert_eq!(factory, "sql-derived-series");
        assert_eq!(decoded, config);
    }

    #[test]
    fn recipe_hash_commits_to_factory_not_just_config() {
        // Identical config under different factories must hash differently --
        // the whole point of folding the factory into the recipe object (D4).
        let config = b"shared config";
        assert_ne!(
            recipe_hash("factory-a", config),
            recipe_hash("factory-b", config)
        );
        // And it is blake3 of the encoded recipe bytes.
        assert_eq!(
            recipe_hash("factory-a", config),
            ObjectHash::of_bytes(&encode_recipe("factory-a", config))
        );
    }

    #[test]
    fn recipe_config_may_be_empty() {
        let bytes = encode_recipe("dynamic-dir", b"");
        let (factory, config) = decode_recipe(&bytes).unwrap();
        assert_eq!(factory, "dynamic-dir");
        assert!(config.is_empty());
    }

    #[test]
    fn decode_recipe_rejects_bad_magic() {
        assert!(decode_recipe(b"not a recipe").is_err());
    }

    fn full_meta() -> VersionMeta {
        VersionMeta {
            timestamp: Some(1_700_000_000_000_000),
            min_event_time: Some(-42),
            max_event_time: Some(i64::MAX),
            extended_attributes: Some(r#"{"watertown.timestamp_column":"Timestamp"}"#.to_string()),
        }
    }

    #[test]
    fn version_meta_round_trips_every_combination() {
        let full = full_meta();
        // Every subset of the presence flags must survive a round trip, so a
        // partially-known node keeps exactly what it knew and nothing more.
        for mask in 0u8..16 {
            let meta = VersionMeta {
                timestamp: (mask & 1 != 0).then_some(full.timestamp.unwrap()),
                min_event_time: (mask & 2 != 0).then_some(full.min_event_time.unwrap()),
                max_event_time: (mask & 4 != 0).then_some(full.max_event_time.unwrap()),
                extended_attributes: (mask & 8 != 0)
                    .then(|| full.extended_attributes.clone().unwrap()),
            };
            let mut buf = Vec::new();
            push_version_meta(&mut buf, &meta);
            let mut cur = Cursor::new(&buf);
            assert_eq!(take_version_meta(&mut cur).unwrap(), meta, "mask {mask}");
        }
    }

    #[test]
    fn empty_version_meta_costs_one_byte() {
        let mut buf = Vec::new();
        push_version_meta(&mut buf, &VersionMeta::default());
        assert_eq!(buf.len(), VERSION_META_MIN_BYTES);
        assert!(VersionMeta::default().is_empty());
        assert!(!full_meta().is_empty());
    }

    #[test]
    fn take_version_meta_rejects_unknown_flags() {
        // A newer encoding's field cannot be skipped -- its width is unknown --
        // and silently dropping node state is the failure this encoding exists
        // to prevent, so decoding must fail loudly.
        let buf = vec![META_KNOWN_FLAGS | 0b1000_0000];
        let mut cur = Cursor::new(&buf);
        let err = take_version_meta(&mut cur).unwrap_err();
        assert!(err.contains("unknown version metadata flags"), "{err}");
    }

    #[test]
    fn tree_round_trips_version_metadata() {
        let entries = vec![
            TreeEntry::new(
                "series",
                EntryType::FilePhysicalSeries,
                h("ser"),
                vec![full_meta(), VersionMeta::default()],
            ),
            TreeEntry::bare("dir", EntryType::DirectoryPhysical, h("dir")),
        ];
        let decoded = decode_tree(&encode_tree(&entries).unwrap()).unwrap();
        let mut expected = entries.clone();
        expected.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
        assert_eq!(decoded, expected);
    }

    #[test]
    fn tree_hash_changes_with_version_metadata_alone() {
        // The whole point: metadata drift must move the hash even though the
        // child_hash is untouched, or replication can never notice or repair it.
        let bare = vec![TreeEntry::bare("a", EntryType::FilePhysicalVersion, h("1"))];
        let with_meta = vec![TreeEntry::new(
            "a",
            EntryType::FilePhysicalVersion,
            h("1"),
            vec![full_meta()],
        )];
        assert_ne!(tree_hash(&bare).unwrap(), tree_hash(&with_meta).unwrap());

        let mut other = full_meta();
        other.timestamp = Some(full_meta().timestamp.unwrap() + 1);
        let mtime_only = vec![TreeEntry::new(
            "a",
            EntryType::FilePhysicalVersion,
            h("1"),
            vec![other],
        )];
        assert_ne!(
            tree_hash(&with_meta).unwrap(),
            tree_hash(&mtime_only).unwrap()
        );
    }
}
