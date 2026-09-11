// SPDX-License-Identifier: Apache-2.0

//! Persistent path-compressed Merkle map for node identity metadata.
//!
//! Keys are `blake3(node_id)`. A leaf stores the complete identity record for
//! one node; an interior Patricia branch stores the first key bit on which its
//! two children differ. Unchanged child hashes are reused verbatim, so one
//! point mutation creates only the leaf and compressed branch path it touches.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use tinyfs::EntryType;

use super::tree::{push_version_metas, take_version_metas};
use super::{Cursor, ManifestEntry, ObjectHash, push_len_prefixed};

const NODE_MAGIC: &[u8] = b"watertown.manifest-map-node.v2\n";
const ROOT_MAGIC: &[u8] = b"watertown.manifest-root.v2\n";
const CHANGE_MAGIC: &[u8] = b"watertown.manifest-change.v2\n";
const NODE_LEAF: u8 = 0;
const NODE_BRANCH: u8 = 1;

/// One directory child recorded inside its parent's identity-map record.
///
/// The child record repeats its own name and type. Keeping the parent listing
/// here permits point updates and subtree deletion without scanning the whole
/// identity map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestRecordChild {
    /// Child node identity.
    pub node_id: String,
    /// Name within the parent directory.
    pub name: String,
    /// Child entry type.
    pub entry_type: EntryType,
}

impl ManifestRecordChild {
    /// Construct a directory-child record.
    #[must_use]
    pub fn new(node_id: impl Into<String>, name: impl Into<String>, entry_type: EntryType) -> Self {
        Self {
            node_id: node_id.into(),
            name: name.into(),
            entry_type,
        }
    }
}

/// Complete value stored at one node-id key in the persistent manifest map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestRecord {
    /// Existing public identity/content metadata.
    pub entry: ManifestEntry,
    /// Direct children for a physical directory, sorted by node id.
    pub children: Vec<ManifestRecordChild>,
}

impl ManifestRecord {
    /// Construct and validate a manifest record.
    pub fn new(
        entry: ManifestEntry,
        mut children: Vec<ManifestRecordChild>,
    ) -> Result<Self, String> {
        children.sort_by(|left, right| {
            left.node_id
                .as_bytes()
                .cmp(right.node_id.as_bytes())
                .then_with(|| left.name.as_bytes().cmp(right.name.as_bytes()))
                .then_with(|| (left.entry_type as u8).cmp(&(right.entry_type as u8)))
        });
        for pair in children.windows(2) {
            if pair[0].node_id == pair[1].node_id {
                return Err(format!(
                    "manifest directory {} repeats child node {}",
                    entry.node_id, pair[0].node_id
                ));
            }
        }
        let mut names = BTreeSet::new();
        for child in &children {
            if child.node_id.is_empty() || child.name.is_empty() {
                return Err(format!(
                    "manifest directory {} has an empty child identity or name",
                    entry.node_id
                ));
            }
            if !names.insert(child.name.as_str()) {
                return Err(format!(
                    "manifest directory {} repeats child name {:?}",
                    entry.node_id, child.name
                ));
            }
        }
        if entry.entry_type != EntryType::DirectoryPhysical && !children.is_empty() {
            return Err(format!(
                "non-directory manifest node {} carries children",
                entry.node_id
            ));
        }
        if entry.node_id.is_empty() {
            return Err("manifest node_id is empty".to_string());
        }
        Ok(Self { entry, children })
    }

    /// Node id used as the map key.
    #[must_use]
    pub fn node_id(&self) -> &str {
        &self.entry.node_id
    }

    /// Encode the record independently of its Patricia leaf wrapper.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        encode_record_into(self, &mut bytes);
        bytes
    }

    /// Decode a standalone record.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let mut cursor = Cursor::new(bytes);
        let record = decode_record_from(&mut cursor)?;
        if !cursor.is_empty() {
            return Err(format!(
                "{} trailing byte(s) after manifest record",
                cursor.remaining()
            ));
        }
        if record.encode() != bytes {
            return Err("manifest record is not canonically encoded".to_string());
        }
        Ok(record)
    }
}

/// Net change to one manifest-map key.
///
/// Carrying both sides lets a consumer apply deletes, moves, and renames using
/// only changed records plus bounded parent-chain lookups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestChange {
    /// Parent-snapshot value, absent for creation.
    pub before: Option<ManifestRecord>,
    /// New-snapshot value, absent for deletion.
    pub after: Option<ManifestRecord>,
}

impl ManifestChange {
    /// Construct one non-empty, same-key change.
    pub fn new(
        before: Option<ManifestRecord>,
        after: Option<ManifestRecord>,
    ) -> Result<Self, String> {
        if before.is_none() && after.is_none() {
            return Err("manifest change has neither before nor after".to_string());
        }
        if let (Some(before), Some(after)) = (&before, &after) {
            if before.node_id() != after.node_id() {
                return Err(format!(
                    "manifest change crosses node ids {} and {}",
                    before.node_id(),
                    after.node_id()
                ));
            }
            if before == after {
                return Err(format!(
                    "manifest change for {} is a no-op",
                    before.node_id()
                ));
            }
        }
        Ok(Self { before, after })
    }

    /// Node id changed.
    #[must_use]
    pub fn node_id(&self) -> &str {
        self.after
            .as_ref()
            .or(self.before.as_ref())
            .expect("validated manifest change")
            .node_id()
    }

    /// Sort changes by node id and reject duplicates/non-canonical values.
    pub fn canonicalize(mut changes: Vec<Self>) -> Result<Vec<Self>, String> {
        changes.sort_by(|left, right| left.node_id().as_bytes().cmp(right.node_id().as_bytes()));
        for change in &changes {
            let _ = Self::new(change.before.clone(), change.after.clone())?;
        }
        for pair in changes.windows(2) {
            if pair[0].node_id() == pair[1].node_id() {
                return Err(format!(
                    "manifest changes repeat node id {}",
                    pair[0].node_id()
                ));
            }
        }
        Ok(changes)
    }
}

/// One encoded Patricia node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestMapNode {
    /// One complete key/value record.
    Leaf {
        /// `blake3(record.entry.node_id)`.
        key: [u8; 32],
        /// Node identity/content metadata.
        record: ManifestRecord,
    },
    /// Compressed binary branch at `bit`.
    Branch {
        /// First differing key bit, 0-based and most-significant-first.
        bit: u16,
        /// Common key prefix before `bit`; bit `bit` and all lower bits are 0.
        prefix: [u8; 32],
        /// Child whose key bit at `bit` is zero.
        left: ObjectHash,
        /// Child whose key bit at `bit` is one.
        right: ObjectHash,
    },
}

impl ManifestMapNode {
    /// Canonical node bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(NODE_MAGIC);
        match self {
            Self::Leaf { key, record } => {
                bytes.push(NODE_LEAF);
                bytes.extend_from_slice(key);
                encode_record_into(record, &mut bytes);
            }
            Self::Branch {
                bit,
                prefix,
                left,
                right,
            } => {
                bytes.push(NODE_BRANCH);
                bytes.extend_from_slice(&bit.to_le_bytes());
                bytes.extend_from_slice(prefix);
                bytes.extend_from_slice(left.as_bytes());
                bytes.extend_from_slice(right.as_bytes());
            }
        }
        bytes
    }

    /// Content address of [`Self::encode`].
    #[must_use]
    pub fn hash(&self) -> ObjectHash {
        ObjectHash::of_bytes(&self.encode())
    }

    /// Strict decode with canonical Patricia invariants.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let mut cursor = Cursor::new(bytes);
        cursor.expect_tag(NODE_MAGIC)?;
        let node = match cursor.take_u8()? {
            NODE_LEAF => {
                let key = cursor.take_array::<32>()?;
                let record = decode_record_from(&mut cursor)?;
                let expected = manifest_key(record.node_id());
                if key != expected {
                    return Err(format!(
                        "manifest leaf key {} does not match node id {}",
                        hex::encode(key),
                        record.node_id()
                    ));
                }
                Self::Leaf { key, record }
            }
            NODE_BRANCH => {
                let bit = cursor.take_u16()?;
                if bit >= 256 {
                    return Err(format!("manifest branch bit {bit} is out of range"));
                }
                let prefix = cursor.take_array::<32>()?;
                if prefix_after_bit_is_nonzero(&prefix, usize::from(bit)) {
                    return Err(format!(
                        "manifest branch prefix has nonzero bits at or below bit {bit}"
                    ));
                }
                let left = cursor.take_hash()?;
                let right = cursor.take_hash()?;
                if left == right {
                    return Err("manifest branch has identical children".to_string());
                }
                Self::Branch {
                    bit,
                    prefix,
                    left,
                    right,
                }
            }
            other => return Err(format!("unknown manifest map node tag {other}")),
        };
        if !cursor.is_empty() {
            return Err(format!(
                "{} trailing byte(s) after manifest map node",
                cursor.remaining()
            ));
        }
        if node.encode() != bytes {
            return Err("manifest map node is not canonically encoded".to_string());
        }
        Ok(node)
    }

    fn representative_key(&self) -> [u8; 32] {
        match self {
            Self::Leaf { key, .. } => *key,
            Self::Branch { prefix, .. } => *prefix,
        }
    }
}

/// Incremental Patricia editor backed by a caller-supplied immutable loader.
pub struct ManifestMapEditor<F>
where
    F: FnMut(ObjectHash) -> Result<Vec<u8>, String>,
{
    root: Option<ObjectHash>,
    loader: F,
    cache: HashMap<ObjectHash, ManifestMapNode>,
    created: BTreeMap<ObjectHash, Vec<u8>>,
}

impl<F> ManifestMapEditor<F>
where
    F: FnMut(ObjectHash) -> Result<Vec<u8>, String>,
{
    /// Open an editor at `root`; `None` creates an empty map.
    pub fn new(root: Option<ObjectHash>, loader: F) -> Self {
        Self {
            root,
            loader,
            cache: HashMap::new(),
            created: BTreeMap::new(),
        }
    }

    /// Current root, absent only while the map is empty.
    #[must_use]
    pub fn root(&self) -> Option<ObjectHash> {
        self.root
    }

    /// Point lookup by unhashed node id.
    pub fn lookup(&mut self, node_id: &str) -> Result<Option<ManifestRecord>, String> {
        let key = manifest_key(node_id);
        let Some(mut cursor) = self.root else {
            return Ok(None);
        };
        loop {
            match self.load(cursor)? {
                ManifestMapNode::Leaf {
                    key: leaf_key,
                    record,
                } => return Ok((leaf_key == key).then_some(record)),
                ManifestMapNode::Branch {
                    bit,
                    prefix,
                    left,
                    right,
                } => {
                    let bit = usize::from(bit);
                    if !prefix_matches(&prefix, bit, &key) {
                        return Ok(None);
                    }
                    cursor = if bit_at(&key, bit) == 0 { left } else { right };
                }
            }
        }
    }

    /// Insert or replace one record.
    pub fn upsert(&mut self, record: ManifestRecord) -> Result<(), String> {
        let key = manifest_key(record.node_id());
        let root = match self.root {
            Some(root) => self.insert_at(root, key, record)?,
            None => self.store(ManifestMapNode::Leaf { key, record }),
        };
        self.root = Some(root);
        Ok(())
    }

    /// Remove one node id. Returns whether it was present.
    pub fn remove(&mut self, node_id: &str) -> Result<bool, String> {
        let Some(root) = self.root else {
            return Ok(false);
        };
        let key = manifest_key(node_id);
        let (root, removed) = self.remove_at(root, &key)?;
        self.root = root;
        Ok(removed)
    }

    /// Finish, returning the root and every newly encoded node object.
    pub fn finish(mut self) -> Result<(ObjectHash, BTreeMap<ObjectHash, Vec<u8>>), String> {
        let root = self
            .root
            .ok_or_else(|| "manifest map cannot finish empty".to_string())?;
        let mut reachable_created = BTreeMap::new();
        let mut stack = vec![root];
        let mut seen = BTreeSet::new();
        while let Some(hash) = stack.pop() {
            if !seen.insert(hash) {
                continue;
            }
            let Some(bytes) = self.created.get(&hash).cloned() else {
                // An unchanged immutable subtree was already durable before
                // this edit. Its descendants cannot contain newly created
                // nodes, so the changed-object walk prunes here.
                continue;
            };
            let node = self.load(hash)?;
            let _ = reachable_created.insert(hash, bytes);
            if let ManifestMapNode::Branch { left, right, .. } = node {
                stack.push(right);
                stack.push(left);
            }
        }
        Ok((root, reachable_created))
    }

    fn load(&mut self, hash: ObjectHash) -> Result<ManifestMapNode, String> {
        if let Some(node) = self.cache.get(&hash) {
            return Ok(node.clone());
        }
        let bytes = if let Some(bytes) = self.created.get(&hash) {
            bytes.clone()
        } else {
            (self.loader)(hash)?
        };
        if ObjectHash::of_bytes(&bytes) != hash {
            return Err(format!("manifest map object {hash} has mismatched bytes"));
        }
        let node = ManifestMapNode::decode(&bytes)?;
        let _ = self.cache.insert(hash, node.clone());
        Ok(node)
    }

    fn store(&mut self, node: ManifestMapNode) -> ObjectHash {
        let bytes = node.encode();
        let hash = ObjectHash::of_bytes(&bytes);
        let _ = self.created.entry(hash).or_insert(bytes);
        let _ = self.cache.insert(hash, node);
        hash
    }

    fn insert_at(
        &mut self,
        current_hash: ObjectHash,
        key: [u8; 32],
        record: ManifestRecord,
    ) -> Result<ObjectHash, String> {
        let current = self.load(current_hash)?;
        let representative = current.representative_key();
        match current {
            ManifestMapNode::Leaf {
                key: existing_key,
                record: existing,
            } => {
                if existing_key == key {
                    if existing == record {
                        return Ok(current_hash);
                    }
                    return Ok(self.store(ManifestMapNode::Leaf { key, record }));
                }
                let differing = first_differing_bit(&existing_key, &key)
                    .expect("different keys have a differing bit");
                let new_leaf = self.store(ManifestMapNode::Leaf { key, record });
                Ok(self.make_branch(differing, key, current_hash, representative, new_leaf))
            }
            ManifestMapNode::Branch {
                bit,
                prefix,
                left,
                right,
            } => {
                let bit_index = usize::from(bit);
                if let Some(differing) = first_differing_bit_before(&prefix, &key, bit_index) {
                    let new_leaf = self.store(ManifestMapNode::Leaf { key, record });
                    return Ok(self.make_branch(
                        differing,
                        key,
                        current_hash,
                        representative,
                        new_leaf,
                    ));
                }
                let next = if bit_at(&key, bit_index) == 0 {
                    self.insert_at(left, key, record)?
                } else {
                    self.insert_at(right, key, record)?
                };
                if bit_at(&key, bit_index) == 0 {
                    if next == left {
                        Ok(current_hash)
                    } else {
                        Ok(self.store(ManifestMapNode::Branch {
                            bit,
                            prefix,
                            left: next,
                            right,
                        }))
                    }
                } else if next == right {
                    Ok(current_hash)
                } else {
                    Ok(self.store(ManifestMapNode::Branch {
                        bit,
                        prefix,
                        left,
                        right: next,
                    }))
                }
            }
        }
    }

    fn make_branch(
        &mut self,
        bit: usize,
        new_key: [u8; 32],
        existing_hash: ObjectHash,
        existing_key: [u8; 32],
        new_hash: ObjectHash,
    ) -> ObjectHash {
        let prefix = canonical_prefix(&new_key, bit);
        let (left, right) = if bit_at(&new_key, bit) == 0 {
            debug_assert_eq!(bit_at(&existing_key, bit), 1);
            (new_hash, existing_hash)
        } else {
            debug_assert_eq!(bit_at(&existing_key, bit), 0);
            (existing_hash, new_hash)
        };
        self.store(ManifestMapNode::Branch {
            bit: u16::try_from(bit).expect("manifest bit fits u16"),
            prefix,
            left,
            right,
        })
    }

    fn remove_at(
        &mut self,
        current_hash: ObjectHash,
        key: &[u8; 32],
    ) -> Result<(Option<ObjectHash>, bool), String> {
        match self.load(current_hash)? {
            ManifestMapNode::Leaf {
                key: existing_key, ..
            } => {
                if &existing_key == key {
                    Ok((None, true))
                } else {
                    Ok((Some(current_hash), false))
                }
            }
            ManifestMapNode::Branch {
                bit,
                prefix,
                left,
                right,
            } => {
                let bit_index = usize::from(bit);
                if !prefix_matches(&prefix, bit_index, key) {
                    return Ok((Some(current_hash), false));
                }
                if bit_at(key, bit_index) == 0 {
                    let (new_left, removed) = self.remove_at(left, key)?;
                    if !removed {
                        return Ok((Some(current_hash), false));
                    }
                    match new_left {
                        None => Ok((Some(right), true)),
                        Some(new_left) => Ok((
                            Some(self.store(ManifestMapNode::Branch {
                                bit,
                                prefix,
                                left: new_left,
                                right,
                            })),
                            true,
                        )),
                    }
                } else {
                    let (new_right, removed) = self.remove_at(right, key)?;
                    if !removed {
                        return Ok((Some(current_hash), false));
                    }
                    match new_right {
                        None => Ok((Some(left), true)),
                        Some(new_right) => Ok((
                            Some(self.store(ManifestMapNode::Branch {
                                bit,
                                prefix,
                                left,
                                right: new_right,
                            })),
                            true,
                        )),
                    }
                }
            }
        }
    }
}

/// Build a canonical map from a complete record set.
pub fn build_manifest_map(
    records: &[ManifestRecord],
) -> Result<(ObjectHash, BTreeMap<ObjectHash, Vec<u8>>), String> {
    let mut editor = ManifestMapEditor::new(None, |_hash| {
        Err("manifest map builder attempted to load an absent object".to_string())
    });
    let mut ordered = records.to_vec();
    ordered.sort_by(|left, right| left.node_id().as_bytes().cmp(right.node_id().as_bytes()));
    for pair in ordered.windows(2) {
        if pair[0].node_id() == pair[1].node_id() {
            return Err(format!("duplicate manifest node id {}", pair[0].node_id()));
        }
    }
    for record in ordered {
        editor.upsert(record)?;
    }
    editor.finish()
}

/// Encode the fixed-size root pointer stored in the reserved local index node.
#[must_use]
pub fn encode_manifest_root(root: ObjectHash) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(ROOT_MAGIC.len() + 32);
    bytes.extend_from_slice(ROOT_MAGIC);
    bytes.extend_from_slice(root.as_bytes());
    bytes
}

/// Decode the reserved local index-node root pointer.
pub fn decode_manifest_root(bytes: &[u8]) -> Result<ObjectHash, String> {
    let mut cursor = Cursor::new(bytes);
    cursor.expect_tag(ROOT_MAGIC)?;
    let root = cursor.take_hash()?;
    if !cursor.is_empty() {
        return Err(format!(
            "{} trailing byte(s) after manifest root",
            cursor.remaining()
        ));
    }
    Ok(root)
}

/// Encode one net manifest change.
#[must_use]
pub(crate) fn encode_manifest_change(change: &ManifestChange) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CHANGE_MAGIC);
    match &change.before {
        Some(record) => {
            bytes.push(1);
            push_len_prefixed(&mut bytes, &record.encode());
        }
        None => bytes.push(0),
    }
    match &change.after {
        Some(record) => {
            bytes.push(1);
            push_len_prefixed(&mut bytes, &record.encode());
        }
        None => bytes.push(0),
    }
    bytes
}

/// Decode one net manifest change.
pub(crate) fn decode_manifest_change(bytes: &[u8]) -> Result<ManifestChange, String> {
    let mut cursor = Cursor::new(bytes);
    cursor.expect_tag(CHANGE_MAGIC)?;
    let before = match cursor.take_u8()? {
        0 => None,
        1 => Some(ManifestRecord::decode(cursor.take_len_prefixed()?)?),
        other => return Err(format!("invalid manifest-change before flag {other}")),
    };
    let after = match cursor.take_u8()? {
        0 => None,
        1 => Some(ManifestRecord::decode(cursor.take_len_prefixed()?)?),
        other => return Err(format!("invalid manifest-change after flag {other}")),
    };
    if !cursor.is_empty() {
        return Err(format!(
            "{} trailing byte(s) after manifest change",
            cursor.remaining()
        ));
    }
    let change = ManifestChange::new(before, after)?;
    if encode_manifest_change(&change) != bytes {
        return Err("manifest change is not canonically encoded".to_string());
    }
    Ok(change)
}

/// Hash a node id into the Patricia key space.
#[must_use]
pub fn manifest_key(node_id: &str) -> [u8; 32] {
    *blake3::hash(node_id.as_bytes()).as_bytes()
}

fn encode_record_into(record: &ManifestRecord, bytes: &mut Vec<u8>) {
    let entry = &record.entry;
    push_len_prefixed(bytes, entry.node_id.as_bytes());
    push_len_prefixed(bytes, entry.parent_node_id.as_bytes());
    push_len_prefixed(bytes, entry.name.as_bytes());
    bytes.push(entry.entry_type as u8);
    bytes.extend_from_slice(entry.child_hash.as_bytes());
    push_version_metas(bytes, &entry.versions);
    let child_count =
        u32::try_from(record.children.len()).expect("manifest child count exceeds u32::MAX");
    bytes.extend_from_slice(&child_count.to_le_bytes());
    for child in &record.children {
        push_len_prefixed(bytes, child.node_id.as_bytes());
        push_len_prefixed(bytes, child.name.as_bytes());
        bytes.push(child.entry_type as u8);
    }
}

fn decode_record_from(cursor: &mut Cursor<'_>) -> Result<ManifestRecord, String> {
    let node_id = cursor.take_len_prefixed_string()?;
    let parent_node_id = cursor.take_len_prefixed_string()?;
    let name = cursor.take_len_prefixed_string()?;
    let entry_type = EntryType::try_from(cursor.take_u8()?)?;
    let child_hash = cursor.take_hash()?;
    let versions = take_version_metas(cursor)?;
    let child_count = cursor.take_u32()? as usize;
    let mut children = Vec::with_capacity(cursor.bounded_capacity(child_count, 9));
    for _ in 0..child_count {
        children.push(ManifestRecordChild {
            node_id: cursor.take_len_prefixed_string()?,
            name: cursor.take_len_prefixed_string()?,
            entry_type: EntryType::try_from(cursor.take_u8()?)?,
        });
    }
    ManifestRecord::new(
        ManifestEntry::new(
            node_id,
            parent_node_id,
            name,
            entry_type,
            child_hash,
            versions,
        ),
        children,
    )
}

fn bit_at(key: &[u8; 32], bit: usize) -> u8 {
    (key[bit / 8] >> (7 - bit % 8)) & 1
}

fn canonical_prefix(key: &[u8; 32], bit: usize) -> [u8; 32] {
    let mut prefix = *key;
    for index in bit..256 {
        prefix[index / 8] &= !(1 << (7 - index % 8));
    }
    prefix
}

fn prefix_after_bit_is_nonzero(prefix: &[u8; 32], bit: usize) -> bool {
    (bit..256).any(|index| bit_at(prefix, index) != 0)
}

fn prefix_matches(prefix: &[u8; 32], bit: usize, key: &[u8; 32]) -> bool {
    (0..bit).all(|index| bit_at(prefix, index) == bit_at(key, index))
}

fn first_differing_bit(left: &[u8; 32], right: &[u8; 32]) -> Option<usize> {
    (0..256).find(|&bit| bit_at(left, bit) != bit_at(right, bit))
}

fn first_differing_bit_before(prefix: &[u8; 32], key: &[u8; 32], limit: usize) -> Option<usize> {
    (0..limit).find(|&bit| bit_at(prefix, bit) != bit_at(key, bit))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(value: &str) -> ObjectHash {
        ObjectHash::of_bytes(value.as_bytes())
    }

    fn record(node_id: &str, value: &str) -> ManifestRecord {
        ManifestRecord::new(
            ManifestEntry::bare(
                node_id,
                "root",
                node_id,
                EntryType::FilePhysicalVersion,
                hash(value),
            ),
            vec![],
        )
        .unwrap()
    }

    #[test]
    fn map_is_insertion_order_independent_and_incremental() {
        let records = vec![record("a", "1"), record("b", "2"), record("c", "3")];
        let (root, objects) = build_manifest_map(&records).unwrap();
        let reversed = records.iter().cloned().rev().collect::<Vec<_>>();
        let (other_root, _) = build_manifest_map(&reversed).unwrap();
        assert_eq!(root, other_root);

        let mut editor = ManifestMapEditor::new(Some(root), |hash| {
            objects
                .get(&hash)
                .cloned()
                .ok_or_else(|| format!("missing {hash}"))
        });
        assert_eq!(editor.lookup("b").unwrap(), Some(record("b", "2")));
        editor.upsert(record("b", "changed")).unwrap();
        let (changed_root, changed_objects) = editor.finish().unwrap();
        assert_ne!(changed_root, root);
        assert!(
            changed_objects.len() < objects.len(),
            "one point update must reuse unchanged subtrees"
        );
    }

    #[test]
    fn finish_does_not_traverse_reused_subtrees() {
        let records = (0..256)
            .map(|index| record(&format!("node-{index:04}"), &format!("value-{index}")))
            .collect::<Vec<_>>();
        let (root, objects) = build_manifest_map(&records).unwrap();
        let mut loads = 0usize;
        let changed = {
            let mut editor = ManifestMapEditor::new(Some(root), |hash| {
                loads += 1;
                objects
                    .get(&hash)
                    .cloned()
                    .ok_or_else(|| format!("missing {hash}"))
            });
            editor.upsert(record("node-0128", "changed-value")).unwrap();
            editor.finish().unwrap()
        };
        assert_ne!(changed.0, root);
        assert!(
            loads < 64,
            "one point update loaded {loads} nodes from a 256-record map"
        );
        assert!(
            changed.1.len() < 64,
            "one point update created {} nodes",
            changed.1.len()
        );
    }

    #[test]
    fn remove_collapses_path_and_restores_prior_root() {
        let base = vec![record("a", "1"), record("b", "2")];
        let (base_root, base_objects) = build_manifest_map(&base).unwrap();
        let mut editor = ManifestMapEditor::new(Some(base_root), |hash| {
            base_objects
                .get(&hash)
                .cloned()
                .ok_or_else(|| format!("missing {hash}"))
        });
        editor.upsert(record("c", "3")).unwrap();
        let (with_c, created) = editor.finish().unwrap();
        let mut all = base_objects;
        all.extend(created);
        let mut editor = ManifestMapEditor::new(Some(with_c), |hash| {
            all.get(&hash)
                .cloned()
                .ok_or_else(|| format!("missing {hash}"))
        });
        assert!(editor.remove("c").unwrap());
        assert_eq!(editor.finish().unwrap().0, base_root);
    }

    #[test]
    fn malformed_nodes_fail_closed() {
        let record = record("a", "1");
        let node = ManifestMapNode::Leaf {
            key: manifest_key("a"),
            record,
        };
        let mut bytes = node.encode();
        bytes.push(0);
        assert!(ManifestMapNode::decode(&bytes).is_err());
    }
}
