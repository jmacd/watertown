// SPDX-License-Identifier: Apache-2.0

//! Node-keyed Merkle over the node manifest: an `O(change * log n)` commit root.
//!
//! `docs/incremental-content-tree-design.md` Section 4.2 replaces the flat
//! [`manifest_hash`](super::manifest_hash) -- which re-encodes and re-hashes
//! every manifest entry on every commit (`O(#nodes)`) -- with the root of a
//! **node-keyed Merkle** over the same entries.  A point update (one node
//! changed, added, or deleted) recomputes only the root-to-leaf path of the
//! touched key, so the commit hot path pays for the *change*, not the pond.
//!
//! # Construction
//!
//! This is a **sparse binary Merkle tree** of fixed depth
//! [`DEPTH`](self::DEPTH) (= 256).  A node's key is `blake3(node_id)`; its 256
//! bits, most-significant-first, spell the root-to-leaf path (bit `0` = left,
//! bit `1` = right).  Because the key is a uniform hash the tree is balanced in
//! expectation, so a populated path is `O(log n)` deep in practice while the
//! structure stays independent of insertion order.
//!
//! Hashing is domain-separated so leaf, interior, and empty pre-images cannot
//! collide:
//!
//! ```text
//! empty leaf slot:  EMPTY[DEPTH] = blake3(0x02)
//! empty subtree:    EMPTY[d]     = blake3(0x01 || EMPTY[d+1] || EMPTY[d+1])
//! present leaf:     blake3(0x00 || key(32) || value(32))
//! interior node:    blake3(0x01 || left(32) || right(32))
//! ```
//!
//! The `EMPTY[d]` ladder collapses the 2^256 empty positions into one constant
//! per level, so an empty subtree is never materialized.
//!
//! # Two implementations, one root
//!
//! Following the transparency-log tile writer (which pairs an incremental
//! writer with an `O(n)` rebuilder and asserts byte-identical output), this
//! module ships:
//!
//! - [`NodeMerkle`] -- the **incremental updater** for the commit hot path.  It
//!   stores every populated node hash, so [`NodeMerkle::set`] touches only the
//!   one path from the changed leaf to the root and [`NodeMerkle::root`] is
//!   `O(1)`.
//! - [`rebuild_root`] -- the whole-set **`O(n)` rebuilder / oracle**, used to
//!   build or repair the structure from a manifest and as the test oracle.
//!
//! Both compute byte-for-byte the same root for the same key/value set; the
//! `equivalence_over_random_mutations` test asserts this across long random
//! insert / update / delete sequences.

use std::collections::HashMap;

use blake3::Hasher;

use super::{ManifestEntry, ObjectHash, push_len_prefixed};

/// Depth of the sparse tree: one level per bit of the 256-bit `blake3` key.
const DEPTH: usize = 256;

/// Domain-separation prefix for a present leaf hash.
const TAG_LEAF: u8 = 0x00;
/// Domain-separation prefix for an interior node hash.
const TAG_NODE: u8 = 0x01;
/// Domain-separation prefix for the empty leaf slot.
const TAG_EMPTY: u8 = 0x02;

/// Hash of a present leaf at any depth: `blake3(0x00 || key || value)`.
fn leaf_hash(key: &[u8; 32], value: &[u8; 32]) -> [u8; 32] {
    let mut h = Hasher::new();
    h.update(&[TAG_LEAF]);
    h.update(key);
    h.update(value);
    *h.finalize().as_bytes()
}

/// Hash of an interior node: `blake3(0x01 || left || right)`.
fn node_hash(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut h = Hasher::new();
    h.update(&[TAG_NODE]);
    h.update(left);
    h.update(right);
    *h.finalize().as_bytes()
}

/// The `EMPTY[d]` ladder: `EMPTY[DEPTH]` is the empty leaf slot, and each higher
/// level is the interior hash of two copies of the level below.  Computed once
/// per process.
fn empty_hashes() -> &'static [[u8; 32]; DEPTH + 1] {
    use std::sync::OnceLock;
    static EMPTY: OnceLock<[[u8; 32]; DEPTH + 1]> = OnceLock::new();
    EMPTY.get_or_init(|| {
        let mut table = [[0u8; 32]; DEPTH + 1];
        let mut h = Hasher::new();
        h.update(&[TAG_EMPTY]);
        table[DEPTH] = *h.finalize().as_bytes();
        for d in (0..DEPTH).rev() {
            table[d] = node_hash(&table[d + 1], &table[d + 1]);
        }
        table
    })
}

/// The bit at `depth` of `key`, most-significant-first (`0` = left child).
fn bit_at(key: &[u8; 32], depth: usize) -> u8 {
    (key[depth / 8] >> (7 - (depth % 8))) & 1
}

/// The identity of a node in the sparse tree: its depth and the `depth`-bit
/// prefix of the keys beneath it, with all lower bits cleared so the prefix is
/// canonical for that node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct NodeKey {
    depth: u16,
    prefix: [u8; 32],
}

impl NodeKey {
    /// The node at `depth` on the path of `key` (top `depth` bits of `key`,
    /// lower bits cleared).
    fn on_path(key: &[u8; 32], depth: usize) -> Self {
        let mut prefix = *key;
        // Clear every bit at index >= depth.
        for (i, byte) in prefix.iter_mut().enumerate() {
            let bit_base = i * 8;
            if bit_base >= depth {
                *byte = 0;
            } else if bit_base + 8 > depth {
                let keep = depth - bit_base; // number of high bits to keep
                let mask = 0xffu8 << (8 - keep);
                *byte &= mask;
            }
        }
        Self {
            depth: u16::try_from(depth).expect("depth <= 256 fits u16"),
            prefix,
        }
    }
}

/// A node-keyed sparse Merkle tree over `(node_id, value)` pairs.
///
/// Values are opaque 32-byte digests; the manifest wrapper
/// ([`NodeMerkle::set_entry`] / [`rebuild_root`]) derives them from a
/// [`ManifestEntry`].  Keys are hashed (`blake3(node_id)`) into the 256-bit
/// path space, so the caller passes plain `node_id` strings.
#[derive(Debug, Clone, Default)]
pub struct NodeMerkle {
    /// Populated node hashes (interior and leaf), keyed by their [`NodeKey`].
    /// Empty subtrees are absent, standing in as their `EMPTY[d]` constant.
    nodes: HashMap<NodeKey, [u8; 32]>,
    /// Present leaves: hashed key -> value digest.  Tracks membership so a
    /// re-`set` upserts and a `remove` of an absent key is a no-op.
    leaves: HashMap<[u8; 32], [u8; 32]>,
}

impl NodeMerkle {
    /// An empty tree.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of present leaves (nodes recorded in the manifest).
    #[must_use]
    pub fn len(&self) -> usize {
        self.leaves.len()
    }

    /// Whether the tree has no present leaves.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.leaves.is_empty()
    }

    /// The current Merkle root.  `O(1)`: the root node hash is memoized.
    #[must_use]
    pub fn root(&self) -> ObjectHash {
        let root = self
            .nodes
            .get(&NodeKey::on_path(&[0u8; 32], 0))
            .copied()
            .unwrap_or(empty_hashes()[0]);
        ObjectHash::from_bytes(root)
    }

    /// Insert or update the value for `node_id`, recomputing only its path.
    pub fn set(&mut self, node_id: &str, value: [u8; 32]) {
        let key = key_of(node_id);
        self.leaves.insert(key, value);
        self.recompute_path(&key, Some(value));
    }

    /// Remove `node_id` if present (a tombstone applied to the merged view),
    /// recomputing only its path.  A no-op if the key is absent.
    pub fn remove(&mut self, node_id: &str) {
        let key = key_of(node_id);
        if self.leaves.remove(&key).is_none() {
            return;
        }
        self.recompute_path(&key, None);
    }

    /// Upsert a whole manifest entry (deriving its value digest).
    pub fn set_entry(&mut self, entry: &ManifestEntry) {
        self.set(&entry.node_id, entry_value(entry));
    }

    /// Fold the leaf slot for `key` up to the root, updating every node on the
    /// path.  `value` is `Some` for an upsert and `None` for a removal.
    fn recompute_path(&mut self, key: &[u8; 32], value: Option<[u8; 32]>) {
        let empty = empty_hashes();
        // Leaf slot at DEPTH.
        let mut cur = match value {
            Some(v) => leaf_hash(key, &v),
            None => empty[DEPTH],
        };
        self.store(NodeKey::on_path(key, DEPTH), cur, empty[DEPTH]);
        // Carry up, combining with the sibling subtree at each level.
        for depth in (0..DEPTH).rev() {
            let child_depth = depth + 1;
            let mut sibling_key = *key;
            flip_bit(&mut sibling_key, depth);
            let sibling = self
                .nodes
                .get(&NodeKey::on_path(&sibling_key, child_depth))
                .copied()
                .unwrap_or(empty[child_depth]);
            cur = if bit_at(key, depth) == 0 {
                node_hash(&cur, &sibling)
            } else {
                node_hash(&sibling, &cur)
            };
            self.store(NodeKey::on_path(key, depth), cur, empty[depth]);
        }
    }

    /// Record a node hash, or drop it when it equals the empty-subtree constant
    /// so empty regions never occupy the map.
    fn store(&mut self, node: NodeKey, hash: [u8; 32], empty: [u8; 32]) {
        if hash == empty {
            self.nodes.remove(&node);
        } else {
            self.nodes.insert(node, hash);
        }
    }
}

/// The hashed path key for a `node_id`: `blake3(node_id)`.
fn key_of(node_id: &str) -> [u8; 32] {
    *blake3::hash(node_id.as_bytes()).as_bytes()
}

/// Flip the bit at `depth` of `key` (to address the sibling subtree).
fn flip_bit(key: &mut [u8; 32], depth: usize) {
    key[depth / 8] ^= 1 << (7 - (depth % 8));
}

/// The leaf value digest for a manifest entry: everything a consumer must see
/// change on a rename, reparent, retype, or content change.  The `node_id` is
/// the key, not part of the value.
///
/// `blake3(entry_type || len-prefixed name || len-prefixed parent_node_id ||
/// child_hash || version metadata)`; the length prefixes keep the
/// `name`/`parent` boundary unambiguous, mirroring [`super::encode_manifest`].
///
/// The per-version metadata is part of the value because a metadata-only change
/// leaves `child_hash` untouched: the node's content is the same bytes, only
/// what is known *about* those bytes changed. Omitting it would make such a
/// change invisible to the node-keyed diff, so a replica holding stale metadata
/// would never be told to repair it.
fn entry_value(entry: &ManifestEntry) -> [u8; 32] {
    let mut buf = Vec::with_capacity(1 + 8 + entry.name.len() + entry.parent_node_id.len() + 32);
    buf.push(entry.entry_type as u8);
    push_len_prefixed(&mut buf, entry.name.as_bytes());
    push_len_prefixed(&mut buf, entry.parent_node_id.as_bytes());
    buf.extend_from_slice(entry.child_hash.as_bytes());
    super::tree::push_version_metas(&mut buf, &entry.versions);
    *blake3::hash(&buf).as_bytes()
}

/// Rebuild the node-keyed Merkle root from a full manifest in one pass
/// (`O(n)`), the oracle counterpart to the incremental [`NodeMerkle`].
///
/// # Errors
///
/// Returns an error if two entries share a `node_id` (the same guard
/// [`super::encode_manifest`] applies): an ambiguous manifest must not be
/// silently hashed.
pub fn rebuild_root(entries: &[ManifestEntry]) -> Result<ObjectHash, String> {
    let mut pairs: Vec<([u8; 32], [u8; 32])> = entries
        .iter()
        .map(|e| (key_of(&e.node_id), entry_value(e)))
        .collect();
    pairs.sort_by_key(|a| a.0);
    for pair in pairs.windows(2) {
        if pair[0].0 == pair[1].0 {
            // Either a duplicate node_id or (astronomically unlikely) a key
            // collision; both make the manifest ambiguous.
            return Err("duplicate node_id key in node-keyed Merkle".to_string());
        }
    }
    Ok(ObjectHash::from_bytes(build(0, &pairs)))
}

/// Recursively fold a sorted-by-key slice of `(key, value)` pairs into the
/// subtree hash at `depth`.  The empty side of any split collapses to its
/// `EMPTY[d]` constant, so the result is byte-identical to what the incremental
/// [`NodeMerkle`] memoizes.
fn build(depth: usize, pairs: &[([u8; 32], [u8; 32])]) -> [u8; 32] {
    let empty = empty_hashes();
    match pairs.len() {
        0 => empty[depth],
        1 if depth == DEPTH => leaf_hash(&pairs[0].0, &pairs[0].1),
        _ => {
            debug_assert!(
                depth < DEPTH,
                "more than one key shares a full 256-bit path"
            );
            // Keys are sorted, so the left (bit 0) group is a prefix.
            let split = pairs.partition_point(|(k, _)| bit_at(k, depth) == 0);
            let (left, right) = pairs.split_at(split);
            node_hash(&build(depth + 1, left), &build(depth + 1, right))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tinyfs::EntryType;

    fn h(s: &str) -> ObjectHash {
        ObjectHash::of_bytes(s.as_bytes())
    }

    fn entry(node_id: &str, parent: &str, name: &str, child: &str) -> ManifestEntry {
        ManifestEntry::bare(
            node_id,
            parent,
            name,
            EntryType::FilePhysicalVersion,
            h(child),
        )
    }

    /// A tiny deterministic PRNG (xorshift64*) so the equivalence test needs no
    /// external crate and is fully reproducible.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    #[test]
    fn empty_root_is_the_empty_constant() {
        let m = NodeMerkle::new();
        assert!(m.is_empty());
        assert_eq!(m.root(), ObjectHash::from_bytes(empty_hashes()[0]));
        assert_eq!(m.root(), rebuild_root(&[]).unwrap());
    }

    #[test]
    fn insert_then_remove_returns_to_empty() {
        let empty = NodeMerkle::new().root();
        let mut m = NodeMerkle::new();
        m.set_entry(&entry("n1", "root", "a", "a"));
        assert_ne!(m.root(), empty);
        m.remove("n1");
        assert!(m.is_empty());
        assert_eq!(m.root(), empty);
    }

    #[test]
    fn root_is_order_independent() {
        let a = [
            entry("n2", "root", "b", "b"),
            entry("n1", "root", "a", "a"),
            entry("n3", "root", "c", "c"),
        ];
        let mut forward = NodeMerkle::new();
        for e in &a {
            forward.set_entry(e);
        }
        let mut reverse = NodeMerkle::new();
        for e in a.iter().rev() {
            reverse.set_entry(e);
        }
        assert_eq!(forward.root(), reverse.root());
        assert_eq!(forward.root(), rebuild_root(&a).unwrap());
    }

    #[test]
    fn rename_reparent_retype_and_content_all_change_root() {
        let base = entry("n1", "dirA", "old", "content");
        let mut m = NodeMerkle::new();
        m.set_entry(&base);
        let r0 = m.root();

        // Rename.
        m.set_entry(&entry("n1", "dirA", "new", "content"));
        let r_rename = m.root();
        assert_ne!(r_rename, r0);

        // Reparent (back to original name).
        m.set_entry(&entry("n1", "dirB", "old", "content"));
        let r_reparent = m.root();
        assert_ne!(r_reparent, r0);

        // Content change.
        m.set_entry(&entry("n1", "dirA", "old", "different"));
        let r_content = m.root();
        assert_ne!(r_content, r0);

        // Retype.
        m.set("n1", {
            let mut e = entry("n1", "dirA", "old", "content");
            e.entry_type = EntryType::DirectoryPhysical;
            entry_value(&e)
        });
        assert_ne!(m.root(), r0);
    }

    #[test]
    fn incremental_matches_rebuild_after_each_op() {
        // A short scripted sequence, checked against the oracle at every step.
        let mut m = NodeMerkle::new();
        let mut state: std::collections::BTreeMap<String, ManifestEntry> =
            std::collections::BTreeMap::new();

        let ops: &[(&str, &str, &str, &str, bool)] = &[
            ("n1", "root", "a", "va", true),
            ("n2", "root", "b", "vb", true),
            ("n3", "n1", "c", "vc", true),
            ("n2", "root", "b", "vb2", true), // update
            ("n1", "root", "a", "", false),   // delete
            ("n4", "n3", "d", "vd", true),
            ("n3", "n1", "c", "", false), // delete
        ];
        for (id, parent, name, child, present) in ops {
            if *present {
                let e = entry(id, parent, name, child);
                m.set_entry(&e);
                state.insert((*id).to_string(), e);
            } else {
                m.remove(id);
                state.remove(*id);
            }
            let live: Vec<ManifestEntry> = state.values().cloned().collect();
            assert_eq!(m.root(), rebuild_root(&live).unwrap());
        }
        assert!(!m.is_empty());
    }

    #[test]
    fn equivalence_over_random_mutations() {
        // Hammer the incremental updater with a long random insert/update/delete
        // sequence and assert its root stays byte-identical to a from-scratch
        // rebuild of the live set after every mutation.  This is the Phase 3
        // guarantee: the two implementations agree bit-for-bit.
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut m = NodeMerkle::new();
        let mut state: std::collections::BTreeMap<String, ManifestEntry> =
            std::collections::BTreeMap::new();
        // A small key space forces frequent updates and deletes of live keys,
        // and occasional shared path prefixes.
        const KEYS: u64 = 40;

        for step in 0..4000u64 {
            let id = format!("node-{}", rng.below(KEYS));
            // 0,1 => delete; 2..=9 => upsert (bias toward growth).
            if rng.below(10) < 2 {
                m.remove(&id);
                state.remove(&id);
            } else {
                let parent = format!("p{}", rng.below(8));
                let name = format!("name{}", rng.below(1000));
                let child = format!("c{}", rng.below(1_000_000));
                let e = entry(&id, &parent, &name, &child);
                m.set_entry(&e);
                state.insert(id, e);
            }
            let live: Vec<ManifestEntry> = state.values().cloned().collect();
            assert_eq!(m.len(), live.len(), "len mismatch at step {step}");
            assert_eq!(
                m.root(),
                rebuild_root(&live).unwrap(),
                "root mismatch at step {step}"
            );
        }
    }

    #[test]
    fn rebuild_rejects_duplicate_node_ids() {
        let dup = [entry("n1", "root", "a", "a"), entry("n1", "root", "b", "b")];
        assert!(rebuild_root(&dup).is_err());
    }

    #[test]
    fn on_path_clears_lower_bits() {
        // A node at depth d must ignore every bit at index >= d.
        let mut key = [0xffu8; 32];
        key[0] = 0xff;
        let a = NodeKey::on_path(&key, 4);
        let mut key2 = [0x00u8; 32];
        key2[0] = 0xf0; // top 4 bits set, matching key's top nibble
        let b = NodeKey::on_path(&key2, 4);
        assert_eq!(a, b);
    }
}
