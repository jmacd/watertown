// SPDX-License-Identifier: Apache-2.0

//! Ordered RFC-6962-shaped BLAKE3 Merkle tree over logical-series leaf
//! hashes, and contiguous range membership proofs over it.
//!
//! `docs/logical-series-identity-design.md` delivery gate 2. This module
//! knows nothing about `watertown.series.v2` root objects, `watertown.series-pack.v2` pack
//! indexes, Parquet, or Bao; it is pure, order-sensitive Merkle math over an
//! already-computed ordered list of leaf hashes (each produced by
//! [`super::series_leaf::table_leaf_hash`] or
//! [`super::series_leaf::file_leaf_hash`]). It is deliberately **not** the
//! same construction as [`super::node_merkle`]: that module is a *sparse*,
//! key-addressed tree over the unordered node manifest, sized for point
//! updates; this one is a *dense*, position-addressed tree over an ordered
//! append-only sequence, sized for contiguous range proofs. The two must
//! never be confused, so this module mints its own domain tags rather than
//! reusing [`super::node_merkle`]'s.
//!
//! # Construction
//!
//! For `n` ordered leaf hashes `d[0..n)`, the root `MTH(d)` is defined
//! exactly as in [RFC 6962 §2.1](https://www.rfc-editor.org/rfc/rfc6962)
//! ("Merkle Tree"), with BLAKE3 in place of SHA-256 and project-owned domain
//! tags in place of RFC 6962's single leading `0x00`/`0x01` byte:
//!
//! ```text
//! MTH({})           = blake3(DOMAIN || TAG_EMPTY)
//! MTH({d0})         = blake3(DOMAIN || TAG_LEAF || d0)
//! MTH(d[0..n)), n>1 = blake3(DOMAIN || TAG_NODE || MTH(d[0..k)) || MTH(d[k..n)))
//!                     where k is the largest power of two strictly less than n
//! ```
//!
//! `DOMAIN` is the constant [`MERKLE_DOMAIN`]. Splitting at the largest power
//! of two less than `n` (rather than, say, `n / 2`) is what RFC 6962 calls
//! out as necessary for the *consistency-proof* structure to be well-defined
//! across appends; the immediate visible consequence used by this module's
//! "promote, don't duplicate" invariant is that an unpaired rightmost leaf
//! never gets hashed against a copy of itself. For example at `n = 3`, `k =
//! 2`, so the tree is `blake3(.. || MTH({d0,d1}) || MTH({d2}))` and
//! `MTH({d2})` is exactly `blake3(DOMAIN || TAG_LEAF || d2)` -- the lone
//! leaf's own leaf-wrapped hash, never paired with itself.
//!
//! Leaf and interior preimages can never collide because they carry
//! different domain tags ([`TAG_LEAF`] vs [`TAG_NODE`]), and the empty root
//! cannot collide with either because empty preimages never occur inside a
//! leaf or interior hash (a leaf or node hash is always exactly 33 or 65
//! bytes after its domain prefix, while [`TAG_EMPTY`] alone is a fixed
//! shorter preimage under its own tag).
//!
//! # Range proofs
//!
//! A *range proof* lets a verifier who has recomputed the ordered leaf hashes
//! for `[start, end)` -- and knows only `total`, the full series' leaf count
//! -- recompute `MTH(d[0..total))` without holding any leaf outside the
//! range. The proof is the list of sibling subtree hashes for every node in
//! the canonical RFC 6962 recursion (starting from the whole tree and
//! splitting at each node's own largest-power-of-two-less-than-count split
//! point) that falls *entirely outside* `[start, end)`; nodes entirely
//! inside are recomputed from the verifier's own leaves, and nodes that
//! straddle the boundary are split further instead of appearing in the
//! proof. This list, and its traversal order, are a pure function of
//! `(total, start, end)` alone -- computed by [`expected_positions`] without
//! looking at any hash -- so decoding validates a proof's shape (rejecting
//! gaps, overlaps, missing/extra/duplicate/reordered nodes, and wrong
//! ranges/counts) before a single hash is combined. Only a value
//! *substituted* at a structurally-correct position can escape that check;
//! it is caught instead by the final root comparison the caller performs
//! (see [`verify_range_proof`]).
//!
//! An arbitrary `[start, end)` range is generally **not** itself one node of
//! the canonical tree (only ranges aligned to a recursive split boundary
//! are), so there is no meaningful notion of "the range's own standalone
//! root" independent of its position. What [`verify_range_proof`] returns --
//! and what a [`super::series_pack`] pack index's declared range root must
//! equal -- is always the reconstructed **whole-series** root.

use super::{Cursor, ObjectHash};

/// Domain prefix for every hash this module computes. Distinct from
/// [`super::series_leaf`]'s and [`super::node_merkle`]'s own tags so no two
/// modules' preimages can ever collide even if, by coincidence, they were fed
/// identical bytes.
const MERKLE_DOMAIN: &[u8] = b"watertown.series-merkle.v1\n";

/// Domain tag for the empty tree (`n = 0`).
const TAG_EMPTY: u8 = 0x00;
/// Domain tag wrapping a single leaf hash into a tree leaf node.
const TAG_LEAF: u8 = 0x01;
/// Domain tag for an interior node combining two child hashes.
const TAG_NODE: u8 = 0x02;

/// Magic header for an encoded [`RangeProof`].
const PROOF_MAGIC: &[u8] = b"watertown.series-range-proof.v1\n";

/// Minimum on-wire size of one proof node: two `u64` fields and a 32-byte
/// hash. Used to bound decode pre-allocation against a hostile node count.
const PROOF_NODE_MIN_BYTES: usize = 8 + 8 + 32;

/// `blake3(MERKLE_DOMAIN || TAG_EMPTY)`: the root of zero leaves.
fn empty_root() -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(MERKLE_DOMAIN);
    h.update(&[TAG_EMPTY]);
    *h.finalize().as_bytes()
}

/// `blake3(MERKLE_DOMAIN || TAG_LEAF || leaf)`: one leaf hash wrapped into a
/// tree leaf node.
fn leaf_node_hash(leaf: &[u8; 32]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(MERKLE_DOMAIN);
    h.update(&[TAG_LEAF]);
    h.update(leaf);
    *h.finalize().as_bytes()
}

/// `blake3(MERKLE_DOMAIN || TAG_NODE || left || right)`: an interior node.
fn interior_hash(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(MERKLE_DOMAIN);
    h.update(&[TAG_NODE]);
    h.update(left);
    h.update(right);
    *h.finalize().as_bytes()
}

/// The RFC 6962 split point for a node covering `n > 1` leaves: the largest
/// power of two strictly less than `n`.
fn split_point(n: u64) -> u64 {
    debug_assert!(n > 1, "split_point is only defined for n > 1");
    1u64 << (63 - (n - 1).leading_zeros())
}

/// `MTH(leaves)`: the RFC-6962-shaped root hash of an ordered run of leaf
/// hashes, computed directly (no proof).
fn subtree_hash(leaves: &[ObjectHash]) -> [u8; 32] {
    match leaves.len() {
        0 => empty_root(),
        1 => leaf_node_hash(leaves[0].as_bytes()),
        n => {
            let k = split_point(n as u64) as usize;
            let left = subtree_hash(&leaves[..k]);
            let right = subtree_hash(&leaves[k..]);
            interior_hash(&left, &right)
        }
    }
}

/// Compute the Merkle root over an ordered list of logical-series leaf
/// hashes (see the module docs for the exact construction).
///
/// `merkle_root(&[])` is the well-defined empty-tree root, distinct from
/// every non-empty root because [`TAG_EMPTY`] never appears in a leaf or
/// interior preimage.
#[must_use]
pub fn merkle_root(leaves: &[ObjectHash]) -> ObjectHash {
    ObjectHash::from_bytes(subtree_hash(leaves))
}

/// One sibling subtree entry in a [`RangeProof`]: the canonical tree node
/// covering `[start, start + count)`, entirely outside the proven range, and
/// its hash.
///
/// Kept private: a proof is used only through [`generate_range_proof`],
/// [`verify_range_proof`], and its wire encoding: the individual nodes are an
/// implementation detail of the recursive fold, not a stable public type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProofNode {
    start: u64,
    count: u64,
    hash: ObjectHash,
}

/// A membership proof binding a contiguous logical-leaf range to the root of
/// the whole ordered series.
///
/// Opaque by design (see [`ProofNode`]): construct one with
/// [`generate_range_proof`], check one with [`verify_range_proof`], and move
/// one across the wire with its `Encode`/`Decode`-style free functions in
/// [`super::series_pack`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RangeProof {
    nodes: Vec<ProofNode>,
}

/// Recursively list the canonical RFC 6962 tree nodes, covering
/// `[0, total)`, that fall entirely outside `[start, end)` -- in exactly the
/// traversal order [`generate_range_proof`] and [`verify_range_proof`] visit
/// them. This is a pure function of `(total, start, end)`: it never touches a
/// hash, so it is exactly what a decoder can check a proof's *shape* against
/// before any hash is combined.
fn expected_positions(total: u64, start: u64, end: u64) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    collect_positions(0, total, start, end, &mut out);
    out
}

fn collect_positions(
    node_start: u64,
    node_count: u64,
    start: u64,
    end: u64,
    out: &mut Vec<(u64, u64)>,
) {
    let node_end = node_start + node_count;
    if node_end <= start || node_start >= end {
        out.push((node_start, node_count));
        return;
    }
    if node_start >= start && node_end <= end {
        // Entirely inside the range: the verifier recomputes this from its
        // own leaves, so no proof node is needed.
        return;
    }
    // Straddles the boundary: split exactly as MTH does and recurse.
    debug_assert!(node_count > 1, "a single leaf cannot straddle a boundary");
    let k = split_point(node_count);
    collect_positions(node_start, k, start, end, out);
    collect_positions(node_start + k, node_count - k, start, end, out);
}

/// Generate a [`RangeProof`] for `leaves[start..end)` against the whole
/// ordered `leaves` (the prover holds every leaf, so no proof node lookups
/// are needed -- only [`verify_range_proof`] needs one supplied).
///
/// # Errors
///
/// Returns an error if the range is empty (`start >= end`) or out of bounds
/// (`end > leaves.len()`).
pub fn generate_range_proof(
    leaves: &[ObjectHash],
    start: usize,
    end: usize,
) -> Result<RangeProof, String> {
    let total = leaves.len();
    if start >= end {
        return Err(format!("range must be nonempty: start={start} end={end}"));
    }
    if end > total {
        return Err(format!("range end {end} exceeds total leaf count {total}"));
    }
    let mut nodes = Vec::new();
    generate_node(
        0,
        total as u64,
        start as u64,
        end as u64,
        leaves,
        &mut nodes,
    );
    Ok(RangeProof { nodes })
}

/// Mirrors [`collect_positions`]'s recursion, but also computes and emits
/// each outside node's hash (the prover has every leaf, so this never needs
/// an externally-supplied value).
fn generate_node(
    node_start: u64,
    node_count: u64,
    start: u64,
    end: u64,
    leaves: &[ObjectHash],
    out: &mut Vec<ProofNode>,
) -> [u8; 32] {
    let node_end = node_start + node_count;
    let slice = &leaves[node_start as usize..node_end as usize];
    if node_end <= start || node_start >= end {
        let hash = subtree_hash(slice);
        out.push(ProofNode {
            start: node_start,
            count: node_count,
            hash: ObjectHash::from_bytes(hash),
        });
        return hash;
    }
    if node_start >= start && node_end <= end {
        return subtree_hash(slice);
    }
    let k = split_point(node_count);
    let left = generate_node(node_start, k, start, end, leaves, out);
    let right = generate_node(node_start + k, node_count - k, start, end, leaves, out);
    interior_hash(&left, &right)
}

/// Verify that `range_leaves` (recomputed leaf hashes for `[start, end)`)
/// together with `proof` fold, against a series of `total` leaves, to a
/// single whole-series root -- and return that root.
///
/// This never claims that `range_leaves` are correct for any particular
/// physical content; it only proves that *if* `range_leaves` are the true
/// ordered leaf hashes for `[start, end)`, then the returned value is the
/// true `MTH` root of the whole `total`-leaf series. The caller (see
/// [`super::series_pack::verify_pack_against_manifest`]) is responsible for
/// comparing that returned root against an independently-known series root
/// (never trusting a value the same untrusted input also supplied).
///
/// # Errors
///
/// Returns an error if the range is empty or out of bounds, if
/// `range_leaves.len()` does not equal `end - start`, or if `proof`'s shape
/// does not exactly match [`expected_positions`] for `(total, start, end)`
/// (rejecting gaps, overlaps, missing/extra/duplicate/reordered nodes, and
/// wrong ranges/counts). It does not, and cannot, detect a proof node whose
/// `(start, count)` is correct but whose hash has been substituted for
/// another subtree's; that is caught only by the caller's root comparison.
pub fn verify_range_proof(
    total: usize,
    start: usize,
    end: usize,
    range_leaves: &[ObjectHash],
    proof: &RangeProof,
) -> Result<ObjectHash, String> {
    if start >= end {
        return Err(format!("range must be nonempty: start={start} end={end}"));
    }
    if end > total {
        return Err(format!("range end {end} exceeds total leaf count {total}"));
    }
    if range_leaves.len() != end - start {
        return Err(format!(
            "expected {} leaf hash(es) for range, got {}",
            end - start,
            range_leaves.len()
        ));
    }
    validate_range_proof_shape(proof, total as u64, start as u64, end as u64)?;
    let mut iter = proof.nodes.iter();
    let root = fold_node(
        0,
        total as u64,
        start as u64,
        end as u64,
        start as u64,
        range_leaves,
        &mut iter,
    )?;
    if iter.next().is_some() {
        return Err("trailing proof node(s) after verification".to_string());
    }
    Ok(ObjectHash::from_bytes(root))
}

/// Check that `proof`'s node `(start, count)` sequence matches
/// [`expected_positions`] exactly, in order, with no extras or omissions.
/// Shared by [`decode_range_proof`] (strict decode), [`verify_range_proof`]
/// (defense in depth for a [`RangeProof`] built or mutated without going
/// through decode), and [`super::series_pack::PackIndex::new`] (so a pack
/// index directly constructed, rather than decoded, gets the identical
/// shape check).
///
/// `pub(crate)`: used outside this module by [`super::series_pack`].
pub(crate) fn validate_range_proof_shape(
    proof: &RangeProof,
    total: u64,
    start: u64,
    end: u64,
) -> Result<(), String> {
    let expected = expected_positions(total, start, end);
    if expected.len() != proof.nodes.len() {
        return Err(format!(
            "expected {} proof node(s) for range [{start}, {end}) of {total}, got {}",
            expected.len(),
            proof.nodes.len()
        ));
    }
    for (i, (exp, got)) in expected.iter().zip(proof.nodes.iter()).enumerate() {
        if exp.0 != got.start || exp.1 != got.count {
            return Err(format!(
                "proof node {i}: expected (start={}, count={}), got (start={}, count={})",
                exp.0, exp.1, got.start, got.count
            ));
        }
    }
    Ok(())
}

/// Mirrors [`collect_positions`]'s recursion; combines a proof node's
/// supplied hash for outside subtrees with a direct recomputation for inside
/// subtrees, recursing further at a straddling boundary.
#[allow(clippy::too_many_arguments)]
fn fold_node(
    node_start: u64,
    node_count: u64,
    start: u64,
    end: u64,
    range_offset: u64,
    range_leaves: &[ObjectHash],
    proof_iter: &mut std::slice::Iter<'_, ProofNode>,
) -> Result<[u8; 32], String> {
    let node_end = node_start + node_count;
    if node_end <= start || node_start >= end {
        let node = proof_iter
            .next()
            .ok_or_else(|| "missing proof node".to_string())?;
        if node.start != node_start || node.count != node_count {
            return Err(format!(
                "proof node mismatch: expected (start={node_start}, count={node_count}), \
                 got (start={}, count={})",
                node.start, node.count
            ));
        }
        return Ok(*node.hash.as_bytes());
    }
    if node_start >= start && node_end <= end {
        let lo = (node_start - range_offset) as usize;
        let hi = (node_end - range_offset) as usize;
        return Ok(subtree_hash(&range_leaves[lo..hi]));
    }
    let k = split_point(node_count);
    let left = fold_node(
        node_start,
        k,
        start,
        end,
        range_offset,
        range_leaves,
        proof_iter,
    )?;
    let right = fold_node(
        node_start + k,
        node_count - k,
        start,
        end,
        range_offset,
        range_leaves,
        proof_iter,
    )?;
    Ok(interior_hash(&left, &right))
}

/// Serialize a [`RangeProof`] into its wire bytes:
///
/// ```text
/// PROOF_MAGIC
/// u32 LE  node count
/// repeated: u64 LE start, u64 LE count, 32 bytes hash
/// ```
#[must_use]
pub(crate) fn encode_range_proof(proof: &RangeProof) -> Vec<u8> {
    let mut buf =
        Vec::with_capacity(PROOF_MAGIC.len() + 4 + proof.nodes.len() * PROOF_NODE_MIN_BYTES);
    buf.extend_from_slice(PROOF_MAGIC);
    let count = u32::try_from(proof.nodes.len()).expect("proof node count exceeds u32::MAX");
    buf.extend_from_slice(&count.to_le_bytes());
    for node in &proof.nodes {
        buf.extend_from_slice(&node.start.to_le_bytes());
        buf.extend_from_slice(&node.count.to_le_bytes());
        buf.extend_from_slice(node.hash.as_bytes());
    }
    buf
}

/// Decode a [`RangeProof`] (the inverse of [`encode_range_proof`]), strictly
/// validating its shape against `(total, start, end)` -- the same context a
/// [`super::series_pack`] pack index carries alongside its embedded proof
/// bytes.
///
/// # Errors
///
/// Returns an error if the range is empty or out of bounds, the magic header
/// is wrong, the buffer is truncated or has trailing bytes, or the decoded
/// node `(start, count)` sequence does not exactly match
/// [`expected_positions`] for `(total, start, end)` -- rejecting gaps,
/// overlaps, missing/extra/duplicate nodes, reordering, and wrong
/// ranges/counts, all before any hash is combined. It does not, and cannot,
/// detect a structurally-correct node carrying a substituted hash; only
/// [`verify_range_proof`]'s final root comparison catches that.
pub(crate) fn decode_range_proof(
    bytes: &[u8],
    total: usize,
    start: usize,
    end: usize,
) -> Result<RangeProof, String> {
    if start >= end {
        return Err(format!("range must be nonempty: start={start} end={end}"));
    }
    if end > total {
        return Err(format!("range end {end} exceeds total leaf count {total}"));
    }
    let mut cur = Cursor::new(bytes);
    cur.expect_tag(PROOF_MAGIC)?;
    let count = cur.take_u32()? as usize;
    let mut nodes = Vec::with_capacity(cur.bounded_capacity(count, PROOF_NODE_MIN_BYTES));
    for _ in 0..count {
        let node_start = cur.take_u64()?;
        let node_count = cur.take_u64()?;
        let hash = cur.take_hash()?;
        nodes.push(ProofNode {
            start: node_start,
            count: node_count,
            hash,
        });
    }
    if !cur.is_empty() {
        return Err(format!(
            "{} trailing byte(s) after range proof",
            cur.remaining()
        ));
    }
    let proof = RangeProof { nodes };
    validate_range_proof_shape(&proof, total as u64, start as u64, end as u64)?;
    Ok(proof)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Frozen golden vectors (see `golden_empty_root` / `golden_small_roots`
    // below). Computed once, directly from this module's own construction,
    // via `blake3::hash`-equivalent hashing of `leaves(&["a"])`,
    // `leaves(&["a","b"])`, etc.; pinned as literal hex so a later
    // accidental change to domain tags, split-point math, or hash order is
    // caught by a plain `cargo test`, not only by comparative assertions.
    const GOLDEN_EMPTY_ROOT_HEX: &str =
        "27470c01030ce200198e6c61a5e26b39d940227a21dd79589491973df76eb2e9";
    const GOLDEN_N1_HEX: &str = "1aea83da8ebb2fdc51fa40699b0f41d25e042a507891584889349db9a6cdaa3f";
    const GOLDEN_N2_HEX: &str = "2433d4c461ad75f4a095f814bc697ae7e2e0cd2b89ae54e5049e8014f43ca814";
    const GOLDEN_N3_HEX: &str = "24c34c9d29eec5fe3a0b8345ed692b6f5190e585a2a57143a247970fa304bfa4";
    const GOLDEN_N4_HEX: &str = "311d64c75eeb33cb86b52d5809f813baae4a52ab31aec00d8074c1089d4924e6";

    fn h(s: &str) -> ObjectHash {
        ObjectHash::of_bytes(s.as_bytes())
    }

    fn leaves(labels: &[&str]) -> Vec<ObjectHash> {
        labels.iter().map(|s| h(s)).collect()
    }

    // -- root construction -----------------------------------------------

    #[test]
    fn empty_root_is_stable_and_distinct() {
        let root = merkle_root(&[]);
        assert_eq!(root, merkle_root(&[]));
        assert_ne!(root, merkle_root(&leaves(&["a"])));
    }

    #[test]
    fn single_leaf_root_is_leaf_wrapped_not_raw() {
        let leaf = h("a");
        let root = merkle_root(&[leaf]);
        assert_ne!(
            root, leaf,
            "a leaf hash must be wrapped, not passed through"
        );
    }

    #[test]
    fn even_and_odd_roots_are_distinct_and_stable() {
        let two = merkle_root(&leaves(&["a", "b"]));
        let three = merkle_root(&leaves(&["a", "b", "c"]));
        let four = merkle_root(&leaves(&["a", "b", "c", "d"]));
        assert_ne!(two, three);
        assert_ne!(three, four);
        assert_eq!(two, merkle_root(&leaves(&["a", "b"])));
        assert_eq!(three, merkle_root(&leaves(&["a", "b", "c"])));
    }

    #[test]
    fn root_is_order_sensitive() {
        let forward = merkle_root(&leaves(&["a", "b", "c"]));
        let reversed = merkle_root(&leaves(&["c", "b", "a"]));
        assert_ne!(forward, reversed);
    }

    #[test]
    fn append_changes_root() {
        let before = merkle_root(&leaves(&["a", "b", "c"]));
        let after = merkle_root(&leaves(&["a", "b", "c", "d"]));
        assert_ne!(before, after);
    }

    #[test]
    fn unpaired_rightmost_leaf_is_promoted_not_duplicated() {
        // n = 3: root must be interior(MTH({a,b}), MTH({c})), i.e. leaf `c`
        // wrapped once -- not interior(MTH({a,b}), MTH({c,c})).
        let all = leaves(&["a", "b", "c"]);
        let expected_root = {
            let left = subtree_hash(&all[..2]);
            let right = subtree_hash(&all[2..3]);
            ObjectHash::from_bytes(interior_hash(&left, &right))
        };
        assert_eq!(merkle_root(&all), expected_root);

        // A tree that *did* duplicate the last leaf would differ.
        let duplicated_c = {
            let left = subtree_hash(&all[..2]);
            let right = interior_hash(all[2].as_bytes(), all[2].as_bytes());
            ObjectHash::from_bytes(interior_hash(&left, &right))
        };
        assert_ne!(merkle_root(&all), duplicated_c);
    }

    // -- golden vectors -----------------------------------------------------
    // Hard-coded hex roots for a fixed set of leaf labels, frozen so an
    // accidental change to domain tags, split-point math, or hashing order
    // is caught even if every *comparative* test above still passes.

    #[test]
    fn golden_empty_root() {
        // Frozen cross-process vector: the empty root must never change
        // silently. Computed once from this exact construction and pinned
        // here; a deliberate change to domain tags or hashing must update
        // this constant explicitly.
        assert_eq!(merkle_root(&[]).to_hex(), GOLDEN_EMPTY_ROOT_HEX);
    }

    #[test]
    fn golden_small_roots() {
        // Frozen roots for n=1..=4 over fixed leaf labels, so an accidental
        // change to split-point math or hash order is caught even if every
        // comparative test above still passes.
        assert_eq!(merkle_root(&leaves(&["a"])).to_hex(), GOLDEN_N1_HEX);
        assert_eq!(merkle_root(&leaves(&["a", "b"])).to_hex(), GOLDEN_N2_HEX);
        assert_eq!(
            merkle_root(&leaves(&["a", "b", "c"])).to_hex(),
            GOLDEN_N3_HEX
        );
        assert_eq!(
            merkle_root(&leaves(&["a", "b", "c", "d"])).to_hex(),
            GOLDEN_N4_HEX
        );
    }

    #[test]
    fn split_point_handles_the_full_u64_range() {
        assert_eq!(split_point(2), 1);
        assert_eq!(split_point(1u64 << 63), 1u64 << 62);
        assert_eq!(split_point((1u64 << 63) + 1), 1u64 << 63);
        assert_eq!(split_point(u64::MAX), 1u64 << 63);
    }

    // -- range proofs ---------------------------------------------------

    fn all_hashes(labels: &[&str]) -> Vec<ObjectHash> {
        leaves(labels)
    }

    #[test]
    fn full_range_proof_has_no_nodes() {
        let all = all_hashes(&["a", "b", "c", "d", "e"]);
        let proof = generate_range_proof(&all, 0, all.len()).unwrap();
        assert!(proof.nodes.is_empty());
        let root = verify_range_proof(all.len(), 0, all.len(), &all, &proof).unwrap();
        assert_eq!(root, merkle_root(&all));
    }

    #[test]
    fn prefix_range_proof_verifies() {
        let all = all_hashes(&["a", "b", "c", "d", "e", "f", "g"]);
        let (start, end) = (0, 3);
        let proof = generate_range_proof(&all, start, end).unwrap();
        let root = verify_range_proof(all.len(), start, end, &all[start..end], &proof).unwrap();
        assert_eq!(root, merkle_root(&all));
    }

    #[test]
    fn suffix_range_proof_verifies() {
        let all = all_hashes(&["a", "b", "c", "d", "e", "f", "g"]);
        let (start, end) = (4, 7);
        let proof = generate_range_proof(&all, start, end).unwrap();
        let root = verify_range_proof(all.len(), start, end, &all[start..end], &proof).unwrap();
        assert_eq!(root, merkle_root(&all));
    }

    #[test]
    fn middle_range_proof_verifies() {
        let all = all_hashes(&["a", "b", "c", "d", "e"]);
        let (start, end) = (1, 4);
        let proof = generate_range_proof(&all, start, end).unwrap();
        let root = verify_range_proof(all.len(), start, end, &all[start..end], &proof).unwrap();
        assert_eq!(root, merkle_root(&all));
    }

    #[test]
    fn single_leaf_range_proof_verifies() {
        let all = all_hashes(&["a", "b", "c", "d", "e"]);
        for i in 0..all.len() {
            let proof = generate_range_proof(&all, i, i + 1).unwrap();
            let root = verify_range_proof(all.len(), i, i + 1, &all[i..i + 1], &proof).unwrap();
            assert_eq!(root, merkle_root(&all));
        }
    }

    #[test]
    fn every_contiguous_range_verifies_for_several_sizes() {
        for n in 1..=11usize {
            let labels: Vec<String> = (0..n).map(|i| format!("leaf-{i}")).collect();
            let all: Vec<ObjectHash> = labels.iter().map(|s| h(s)).collect();
            let root = merkle_root(&all);
            for start in 0..n {
                for end in (start + 1)..=n {
                    let proof = generate_range_proof(&all, start, end).unwrap();
                    let got = verify_range_proof(n, start, end, &all[start..end], &proof).unwrap();
                    assert_eq!(got, root, "n={n} start={start} end={end}");
                }
            }
        }
    }

    // -- hostile / mutation tests -----------------------------------------

    #[test]
    fn wrong_total_is_rejected() {
        let all = all_hashes(&["a", "b", "c", "d", "e"]);
        let proof = generate_range_proof(&all, 1, 4).unwrap();
        // Claiming a different total leaf count must fail the shape check.
        assert!(verify_range_proof(6, 1, 4, &all[1..4], &proof).is_err());
        assert!(verify_range_proof(4, 1, 4, &all[1..4], &proof).is_err());
    }

    #[test]
    fn wrong_range_is_rejected() {
        let all = all_hashes(&["a", "b", "c", "d", "e"]);
        let proof = generate_range_proof(&all, 1, 4).unwrap();
        // Shifted range over the same proof must fail (different shape).
        assert!(verify_range_proof(5, 0, 3, &all[0..3], &proof).is_err());
        assert!(verify_range_proof(5, 2, 5, &all[2..5], &proof).is_err());
    }

    #[test]
    fn empty_range_is_rejected() {
        let all = all_hashes(&["a", "b", "c"]);
        assert!(generate_range_proof(&all, 2, 2).is_err());
        assert!(generate_range_proof(&all, 3, 2).is_err());
    }

    #[test]
    fn out_of_bounds_range_is_rejected() {
        let all = all_hashes(&["a", "b", "c"]);
        assert!(generate_range_proof(&all, 0, 4).is_err());
    }

    #[test]
    fn wrong_range_leaf_count_is_rejected() {
        let all = all_hashes(&["a", "b", "c", "d", "e"]);
        let proof = generate_range_proof(&all, 1, 4).unwrap();
        assert!(verify_range_proof(5, 1, 4, &all[1..3], &proof).is_err());
        assert!(verify_range_proof(5, 1, 4, &all[0..4], &proof).is_err());
    }

    #[test]
    fn substituted_leaf_changes_root() {
        let all = all_hashes(&["a", "b", "c", "d", "e"]);
        let proof = generate_range_proof(&all, 1, 4).unwrap();
        let mut tampered = all[1..4].to_vec();
        tampered[1] = h("not-c");
        let root = verify_range_proof(5, 1, 4, &tampered, &proof).unwrap();
        assert_ne!(root, merkle_root(&all));
    }

    #[test]
    fn reordered_range_leaves_change_root() {
        let all = all_hashes(&["a", "b", "c", "d", "e"]);
        let proof = generate_range_proof(&all, 1, 4).unwrap();
        let mut reordered = all[1..4].to_vec();
        reordered.swap(0, 1);
        let root = verify_range_proof(5, 1, 4, &reordered, &proof).unwrap();
        assert_ne!(root, merkle_root(&all));
    }

    #[test]
    fn substituted_proof_node_hash_changes_root() {
        let all = all_hashes(&["a", "b", "c", "d", "e", "f", "g", "h"]);
        let mut proof = generate_range_proof(&all, 2, 5).unwrap();
        assert!(!proof.nodes.is_empty());
        proof.nodes[0].hash = h("substituted");
        let root = verify_range_proof(8, 2, 5, &all[2..5], &proof).unwrap();
        assert_ne!(root, merkle_root(&all));
    }

    #[test]
    fn cross_series_proof_is_rejected_by_root_mismatch() {
        let series_a = all_hashes(&["a", "b", "c", "d", "e"]);
        let series_b = all_hashes(&["v", "w", "x", "y", "z"]);
        let proof = generate_range_proof(&series_a, 1, 4).unwrap();
        // Same shape (same total/start/end), different series: verification
        // succeeds structurally but returns a different root, which the
        // caller must reject by comparing against series_a's actual root.
        let root = verify_range_proof(5, 1, 4, &series_b[1..4], &proof).unwrap();
        assert_ne!(root, merkle_root(&series_a));
    }

    #[test]
    fn decode_reorders_are_rejected() {
        let all = all_hashes(&["a", "b", "c", "d", "e", "f", "g", "h"]);
        let proof = generate_range_proof(&all, 1, 6).unwrap();
        assert!(proof.nodes.len() >= 2, "need at least two nodes to reorder");
        let mut bytes = encode_range_proof(&proof);
        // Swap the first two encoded nodes (each PROOF_NODE_MIN_BYTES wide,
        // right after the magic + count header).
        let header = PROOF_MAGIC.len() + 4;
        let (a, rest) = bytes[header..].split_at_mut(PROOF_NODE_MIN_BYTES);
        let (b, _) = rest.split_at_mut(PROOF_NODE_MIN_BYTES);
        a.swap_with_slice(b);
        assert!(decode_range_proof(&bytes, 8, 1, 6).is_err());
    }

    #[test]
    fn decode_rejects_missing_node() {
        let all = all_hashes(&["a", "b", "c", "d", "e", "f", "g", "h"]);
        let proof = generate_range_proof(&all, 1, 6).unwrap();
        assert!(!proof.nodes.is_empty());
        let shrunk = RangeProof {
            nodes: proof.nodes[..proof.nodes.len() - 1].to_vec(),
        };
        let bytes = encode_range_proof(&shrunk);
        assert!(decode_range_proof(&bytes, 8, 1, 6).is_err());
    }

    #[test]
    fn decode_rejects_extra_node() {
        let all = all_hashes(&["a", "b", "c", "d", "e", "f", "g", "h"]);
        let proof = generate_range_proof(&all, 1, 6).unwrap();
        let mut extended = proof.nodes.clone();
        extended.push(ProofNode {
            start: 6,
            count: 1,
            hash: h("extra"),
        });
        let extended = RangeProof { nodes: extended };
        let bytes = encode_range_proof(&extended);
        assert!(decode_range_proof(&bytes, 8, 1, 6).is_err());
    }

    #[test]
    fn decode_rejects_duplicate_node() {
        let all = all_hashes(&["a", "b", "c", "d", "e", "f", "g", "h"]);
        let proof = generate_range_proof(&all, 1, 6).unwrap();
        assert!(!proof.nodes.is_empty());
        let mut duplicated = proof.nodes.clone();
        let dup = duplicated[0];
        duplicated.insert(0, dup);
        let duplicated = RangeProof { nodes: duplicated };
        let bytes = encode_range_proof(&duplicated);
        assert!(decode_range_proof(&bytes, 8, 1, 6).is_err());
    }

    #[test]
    fn decode_rejects_wrong_range_or_total() {
        let all = all_hashes(&["a", "b", "c", "d", "e", "f", "g", "h"]);
        let proof = generate_range_proof(&all, 1, 6).unwrap();
        let bytes = encode_range_proof(&proof);
        assert!(decode_range_proof(&bytes, 8, 1, 6).is_ok());
        assert!(decode_range_proof(&bytes, 9, 1, 6).is_err());
        assert!(decode_range_proof(&bytes, 8, 0, 6).is_err());
        assert!(decode_range_proof(&bytes, 8, 1, 7).is_err());
    }

    #[test]
    fn decode_rejects_bad_magic() {
        let all = all_hashes(&["a", "b", "c"]);
        let proof = generate_range_proof(&all, 0, 2).unwrap();
        let mut bytes = encode_range_proof(&proof);
        bytes[0] ^= 0xff;
        assert!(decode_range_proof(&bytes, 3, 0, 2).is_err());
    }

    #[test]
    fn decode_rejects_truncation() {
        let all = all_hashes(&["a", "b", "c", "d", "e"]);
        let proof = generate_range_proof(&all, 1, 4).unwrap();
        let bytes = encode_range_proof(&proof);
        assert!(!bytes.is_empty());
        assert!(decode_range_proof(&bytes[..bytes.len() - 4], 5, 1, 4).is_err());
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let all = all_hashes(&["a", "b", "c", "d", "e"]);
        let proof = generate_range_proof(&all, 1, 4).unwrap();
        let mut bytes = encode_range_proof(&proof);
        bytes.push(0);
        assert!(decode_range_proof(&bytes, 5, 1, 4).is_err());
    }

    #[test]
    fn decode_rejects_oversized_count_without_huge_alloc() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(PROOF_MAGIC);
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_range_proof(&bytes, 1_000_000_000, 0, 999_999_999).is_err());
    }

    #[test]
    fn round_trip_encode_decode() {
        let all = all_hashes(&["a", "b", "c", "d", "e", "f", "g"]);
        let proof = generate_range_proof(&all, 2, 6).unwrap();
        let bytes = encode_range_proof(&proof);
        let decoded = decode_range_proof(&bytes, 7, 2, 6).unwrap();
        assert_eq!(decoded, proof);
        assert_eq!(encode_range_proof(&decoded), bytes);
    }

    #[test]
    fn two_pack_layouts_verify_against_the_same_root() {
        // Two different exact covers of the same series (one pack per
        // layout, split differently) must both verify against the same
        // whole-series root.
        let all = all_hashes(&["a", "b", "c", "d", "e", "f"]);
        let root = merkle_root(&all);

        // Layout 1: two packs, split at 3.
        let proof_a1 = generate_range_proof(&all, 0, 3).unwrap();
        let proof_a2 = generate_range_proof(&all, 3, 6).unwrap();
        assert_eq!(
            verify_range_proof(6, 0, 3, &all[0..3], &proof_a1).unwrap(),
            root
        );
        assert_eq!(
            verify_range_proof(6, 3, 6, &all[3..6], &proof_a2).unwrap(),
            root
        );

        // Layout 2: three packs, split at 2 and 4.
        let proof_b1 = generate_range_proof(&all, 0, 2).unwrap();
        let proof_b2 = generate_range_proof(&all, 2, 4).unwrap();
        let proof_b3 = generate_range_proof(&all, 4, 6).unwrap();
        assert_eq!(
            verify_range_proof(6, 0, 2, &all[0..2], &proof_b1).unwrap(),
            root
        );
        assert_eq!(
            verify_range_proof(6, 2, 4, &all[2..4], &proof_b2).unwrap(),
            root
        );
        assert_eq!(
            verify_range_proof(6, 4, 6, &all[4..6], &proof_b3).unwrap(),
            root
        );
    }
}
