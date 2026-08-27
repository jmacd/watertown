// SPDX-License-Identifier: Apache-2.0

//! Commit objects: the spine, and the only place lineage lives.
//!
//! A commit wraps one transaction.  It names the `root_tree_hash` (the top of
//! the SPACE tree), the `parent_commit_hash` (making the single-writer chain a
//! hash chain), and the provenance.  Blobs and trees are pure content; *all*
//! provenance is isolated here so subtree hashes stay comparable across ponds.
//! See `docs/content-addressed-pond-design.md` Sections 4.3 and 5.3.

use super::{Cursor, ObjectHash, push_len_prefixed};

/// Magic header distinguishing a serialized commit from a raw blob (D2).
///
/// Bumped to `.4` for the logical-series-v2 reset
/// (`docs/logical-series-identity-design.md`): a fresh pond is v2-only after
/// reset (no v1/v2 mixed-writer support, no migration), and a commit now
/// carries an explicit [`ContentModelVersion`] tag alongside the magic so a
/// decoder never has to *infer* which content model a commit's
/// `root_tree_hash` was built under. An earlier commit (`.3` or older) cannot
/// be decoded by this version, which is intentional under the clean-reset
/// encoding policy (D2) and the reset decision above: old pre-reset pond
/// history is not expected to remain openable.
const COMMIT_MAGIC: &[u8] = b"dp.commit.4\n";

/// The one prior commit magic this reset retired (D2, `docs/logical-series-identity-design.md`):
/// `dp.commit.3` was one field shorter (no `content_model_version` byte).
/// Recognized here purely so [`Commit::decode`] can name the real cause --
/// "this pond predates the reset" -- instead of a generic "bad magic header"
/// that gives no actionable next step.
const PRE_RESET_COMMIT_MAGIC: &[u8] = b"dp.commit.3\n";

/// Which content-addressing model a commit's `root_tree_hash` (and the rest
/// of its tree) was built under.
///
/// A typed, validated wire byte (not an unchecked integer): [`Self::decode`]
/// rejects any byte it does not recognize, so a corrupt or forward-
/// incompatible tag is a loud decode error rather than silently
/// misinterpreted content.
///
/// There is currently exactly one variant because the reset decision
/// (`docs/logical-series-identity-design.md`) means there is no v1-writing
/// path to keep distinguishing: every production commit encodes the v2
/// logical-series model. The type still exists (rather than a bare `dp.commit.4`
/// bump) so a future content-model change has an explicit, checked tag to
/// bump instead of silently repurposing the magic string again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentModelVersion {
    /// The v2 logical-series identity model: series nodes commit to a
    /// `dp.series.2` manifest over persisted logical leaf hashes (BLAKE3 over
    /// canonical logical bytes), not to a physical-blob Merkle tree.
    LogicalSeriesV2,
}

impl ContentModelVersion {
    fn to_wire(self) -> u8 {
        match self {
            ContentModelVersion::LogicalSeriesV2 => 1,
        }
    }

    fn from_wire(byte: u8) -> Result<Self, String> {
        match byte {
            1 => Ok(ContentModelVersion::LogicalSeriesV2),
            other => Err(format!("unknown content model version byte: {other}")),
        }
    }
}

/// The lineage and audit metadata recorded on a commit.
///
/// This is the only content in the object model that depends on `pond_id`,
/// sequence, or wall-clock time.  Keeping it isolated in the commit is the
/// inversion the whole design rests on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Provenance {
    /// The UUID of the pond that produced this commit.
    pub pond_id: String,
    /// The pond-local transaction sequence number.
    pub seq: i64,
    /// Commit time in microseconds since the Unix epoch.
    pub time_micros: i64,
    /// A human-meaningful author identifier.
    pub author: String,
    /// The original request that produced the transaction (for example, the
    /// CLI invocation), recorded verbatim for audit.
    pub request: String,
}

/// One commit: a transaction's content root plus its lineage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    /// Which content model `root_tree_hash` was built under; see
    /// [`ContentModelVersion`].
    pub content_model_version: ContentModelVersion,
    /// The hash of this commit's root directory tree (top of SPACE).
    pub root_tree_hash: ObjectHash,
    /// The previous commit on this pond's linear chain, or `None` for the
    /// genesis commit.
    pub parent_commit_hash: Option<ObjectHash>,
    /// The content hash of this commit's node manifest object -- `blake3` of the
    /// encoded manifest -- and therefore the address a consumer fetches the
    /// manifest by.  A consumer adopts these ids to mirror the source
    /// row-for-row (Section 4.5, Decision D8).
    pub node_manifest_hash: ObjectHash,
    /// The root of the node-keyed Merkle over the same manifest (Section 4.2).
    /// A commitment that recomputes along touched paths only, so it can be
    /// verified incrementally and, in a later phase, drive incremental manifest
    /// transfer.  Distinct from `node_manifest_hash`, which is the monolithic
    /// manifest object's byte address.
    pub node_manifest_root: ObjectHash,
    /// Lineage and audit metadata.
    pub provenance: Provenance,
}

impl Commit {
    /// Construct a commit.
    #[must_use]
    pub fn new(
        content_model_version: ContentModelVersion,
        root_tree_hash: ObjectHash,
        parent_commit_hash: Option<ObjectHash>,
        node_manifest_hash: ObjectHash,
        node_manifest_root: ObjectHash,
        provenance: Provenance,
    ) -> Self {
        Self {
            content_model_version,
            root_tree_hash,
            parent_commit_hash,
            node_manifest_hash,
            node_manifest_root,
            provenance,
        }
    }

    /// Serialize the commit into its canonical wire format.
    ///
    /// The layout is:
    ///
    /// ```text
    /// COMMIT_MAGIC
    /// u8      content_model_version
    /// 32      root_tree_hash
    /// u8      parent present flag (0 or 1)
    /// 32      parent_commit_hash    (only if the flag is 1)
    /// 32      node_manifest_hash
    /// 32      node_manifest_root
    /// u32 LE + bytes   pond_id
    /// i64 LE  seq
    /// i64 LE  time_micros
    /// u32 LE + bytes   author
    /// u32 LE + bytes   request
    /// ```
    ///
    /// The returned bytes *are* the commit object; its [`Commit::hash`] is
    /// `blake3` of these bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(COMMIT_MAGIC.len() + 161);
        buf.extend_from_slice(COMMIT_MAGIC);
        buf.push(self.content_model_version.to_wire());
        buf.extend_from_slice(self.root_tree_hash.as_bytes());
        match &self.parent_commit_hash {
            Some(parent) => {
                buf.push(1);
                buf.extend_from_slice(parent.as_bytes());
            }
            None => buf.push(0),
        }
        buf.extend_from_slice(self.node_manifest_hash.as_bytes());
        buf.extend_from_slice(self.node_manifest_root.as_bytes());
        push_len_prefixed(&mut buf, self.provenance.pond_id.as_bytes());
        buf.extend_from_slice(&self.provenance.seq.to_le_bytes());
        buf.extend_from_slice(&self.provenance.time_micros.to_le_bytes());
        push_len_prefixed(&mut buf, self.provenance.author.as_bytes());
        push_len_prefixed(&mut buf, self.provenance.request.as_bytes());
        buf
    }

    /// The content address of this commit (`blake3` of [`Commit::encode`]).
    ///
    /// This hash is both the head of the SPACE tree (via `root_tree_hash`) and
    /// the leaf payload of the TIME transparency log.
    #[must_use]
    pub fn hash(&self) -> ObjectHash {
        ObjectHash::of_bytes(&self.encode())
    }

    /// Decode a commit from its canonical wire format (the inverse of
    /// [`Commit::encode`]).
    ///
    /// # Errors
    ///
    /// Returns an error if the magic header is wrong, `content_model_version`
    /// is not a recognized byte (see [`ContentModelVersion::from_wire`]), or
    /// the buffer is truncated or otherwise malformed. If the bytes carry the
    /// known pre-reset `dp.commit.3` magic specifically, the error clearly
    /// diagnoses that a destructive reset (re-initializing the pond) is
    /// required rather than reporting a generic bad-magic-header failure --
    /// no in-place migration from that layout is provided (D2, the reset
    /// decision in `docs/logical-series-identity-design.md`).
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.starts_with(PRE_RESET_COMMIT_MAGIC) {
            return Err(format!(
                "pre-reset commit magic {magic:?} found: this pond predates the logical-series-v2 \
                 reset and cannot be opened by this version -- no in-place migration is provided; \
                 re-initialize the pond (e.g. `pond init` into a fresh directory) and restore from \
                 a remote with `pond remote add` + `pond pull`, per \
                 docs/logical-series-identity-design.md",
                magic = String::from_utf8_lossy(PRE_RESET_COMMIT_MAGIC).trim_end()
            ));
        }
        let mut cur = Cursor::new(bytes);
        cur.expect_tag(COMMIT_MAGIC)?;
        let content_model_version = ContentModelVersion::from_wire(cur.take_u8()?)?;
        let root_tree_hash = cur.take_hash()?;
        let parent_commit_hash = match cur.take_u8()? {
            0 => None,
            1 => Some(cur.take_hash()?),
            other => return Err(format!("invalid parent flag {other}")),
        };
        let node_manifest_hash = cur.take_hash()?;
        let node_manifest_root = cur.take_hash()?;
        let pond_id = cur.take_len_prefixed_string()?;
        let seq = cur.take_i64()?;
        let time_micros = cur.take_i64()?;
        let author = cur.take_len_prefixed_string()?;
        let request = cur.take_len_prefixed_string()?;
        if !cur.is_empty() {
            return Err(format!("{} trailing byte(s) after commit", cur.remaining()));
        }
        Ok(Self {
            content_model_version,
            root_tree_hash,
            parent_commit_hash,
            node_manifest_hash,
            node_manifest_root,
            provenance: Provenance {
                pond_id,
                seq,
                time_micros,
                author,
                request,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prov() -> Provenance {
        Provenance {
            pond_id: "pond-uuid".to_string(),
            seq: 7,
            time_micros: 1_700_000_000_000_000,
            author: "jmacd".to_string(),
            request: "pond copy host:///x /y".to_string(),
        }
    }

    fn model() -> ContentModelVersion {
        ContentModelVersion::LogicalSeriesV2
    }

    fn root() -> ObjectHash {
        ObjectHash::of_bytes(b"root-tree")
    }

    fn manifest() -> ObjectHash {
        ObjectHash::of_bytes(b"node-manifest")
    }

    fn mroot() -> ObjectHash {
        ObjectHash::of_bytes(b"node-manifest-merkle-root")
    }

    #[test]
    fn commit_hash_is_deterministic() {
        let c1 = Commit::new(model(), root(), None, manifest(), mroot(), prov());
        let c2 = Commit::new(model(), root(), None, manifest(), mroot(), prov());
        assert_eq!(c1.hash(), c2.hash());
    }

    #[test]
    fn parent_changes_hash() {
        let no_parent = Commit::new(model(), root(), None, manifest(), mroot(), prov());
        let parent = ObjectHash::of_bytes(b"parent-commit");
        let with_parent = Commit::new(model(), root(), Some(parent), manifest(), mroot(), prov());
        assert_ne!(no_parent.hash(), with_parent.hash());
    }

    #[test]
    fn provenance_changes_hash() {
        let base = Commit::new(model(), root(), None, manifest(), mroot(), prov());
        let mut other = prov();
        other.seq = 8;
        let changed = Commit::new(model(), root(), None, manifest(), mroot(), other);
        assert_ne!(base.hash(), changed.hash());
    }

    #[test]
    fn root_tree_changes_hash() {
        let base = Commit::new(model(), root(), None, manifest(), mroot(), prov());
        let changed = Commit::new(
            model(),
            ObjectHash::of_bytes(b"other-root"),
            None,
            manifest(),
            mroot(),
            prov(),
        );
        assert_ne!(base.hash(), changed.hash());
    }

    #[test]
    fn manifest_changes_hash() {
        // The node manifest is part of lineage, so changing it (even with the
        // same content tree) must change the commit hash.
        let base = Commit::new(model(), root(), None, manifest(), mroot(), prov());
        let changed = Commit::new(
            model(),
            root(),
            None,
            ObjectHash::of_bytes(b"other-manifest"),
            mroot(),
            prov(),
        );
        assert_ne!(base.hash(), changed.hash());
    }

    #[test]
    fn manifest_root_changes_hash() {
        // The node-keyed Merkle root is a distinct commitment; changing it (with
        // the same manifest object hash) must change the commit hash.
        let base = Commit::new(model(), root(), None, manifest(), mroot(), prov());
        let changed = Commit::new(
            model(),
            root(),
            None,
            manifest(),
            ObjectHash::of_bytes(b"other-manifest-root"),
            prov(),
        );
        assert_ne!(base.hash(), changed.hash());
    }

    #[test]
    fn length_prefix_prevents_field_ambiguity() {
        // Moving a character across the author/request boundary must change
        // the hash, proving the framing is unambiguous.
        let mut a = prov();
        a.author = "ab".to_string();
        a.request = "c".to_string();
        let mut b = prov();
        b.author = "a".to_string();
        b.request = "bc".to_string();
        let ca = Commit::new(model(), root(), None, manifest(), mroot(), a);
        let cb = Commit::new(model(), root(), None, manifest(), mroot(), b);
        assert_ne!(ca.hash(), cb.hash());
    }

    #[test]
    fn commit_hash_differs_from_root_blob() {
        let c = Commit::new(model(), root(), None, manifest(), mroot(), prov());
        assert_ne!(c.hash(), root());
    }

    #[test]
    fn decode_round_trips_encode() {
        let parent = ObjectHash::of_bytes(b"parent-commit");
        for c in [
            Commit::new(model(), root(), None, manifest(), mroot(), prov()),
            Commit::new(model(), root(), Some(parent), manifest(), mroot(), prov()),
        ] {
            let bytes = c.encode();
            let decoded = Commit::decode(&bytes).expect("decode");
            assert_eq!(decoded, c);
            assert_eq!(decoded.hash(), c.hash());
        }
    }

    #[test]
    fn decode_rejects_bad_magic() {
        let mut bytes = Commit::new(model(), root(), None, manifest(), mroot(), prov()).encode();
        bytes[0] ^= 0xff;
        assert!(Commit::decode(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let mut bytes = Commit::new(model(), root(), None, manifest(), mroot(), prov()).encode();
        bytes.push(0);
        assert!(Commit::decode(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_truncation() {
        let bytes = Commit::new(model(), root(), None, manifest(), mroot(), prov()).encode();
        assert!(Commit::decode(&bytes[..bytes.len() - 4]).is_err());
    }

    #[test]
    fn decode_rejects_unknown_content_model_version() {
        // The byte immediately after COMMIT_MAGIC is the content model
        // version; a decoder must reject an unrecognized value rather than
        // silently defaulting or misinterpreting the rest of the buffer as a
        // different model's fields.
        let mut bytes = Commit::new(model(), root(), None, manifest(), mroot(), prov()).encode();
        let version_byte = COMMIT_MAGIC.len();
        bytes[version_byte] = 0xaa;
        let err = Commit::decode(&bytes).expect_err("unknown content model version must error");
        assert!(
            err.contains("content model version"),
            "error should name the field: {err}"
        );
    }

    #[test]
    fn decode_rejects_old_dp_commit_3_magic() {
        // dp.commit.3 (pre-reset, one field shorter: no content_model_version
        // byte) must not decode under dp.commit.4, per the reset decision
        // (docs/logical-series-identity-design.md): old pre-reset pond
        // history is not expected to remain openable. Item 8: the error must
        // clearly diagnose that a destructive reset is required, not merely
        // report a generic bad-magic-header failure.
        let mut bytes = Commit::new(model(), root(), None, manifest(), mroot(), prov()).encode();
        bytes[..b"dp.commit.3\n".len()].copy_from_slice(b"dp.commit.3\n");
        let err = Commit::decode(&bytes).expect_err("pre-reset dp.commit.3 magic must not decode");
        assert!(
            err.contains("pre-reset") && err.contains("re-initialize"),
            "error must clearly diagnose that a destructive reset is required: {err}"
        );
    }

    #[test]
    fn content_model_version_changes_hash() {
        // The content model tag is part of the commit's own bytes, so two
        // commits that differ only in it must not collide -- checked by
        // comparing the actual `ObjectHash`es (what `Commit::hash` returns,
        // and what a transparency log / object store would key on), not
        // merely the raw byte buffers (which trivially differ by
        // construction and would prove nothing about hashing).
        //
        // There is only one variant today, so this test exercises the wire
        // byte directly rather than constructing a second `ContentModelVersion`.
        let commit = Commit::new(model(), root(), None, manifest(), mroot(), prov());
        let base_bytes = commit.encode();
        let base_hash = commit.hash();
        assert_eq!(
            base_hash,
            ObjectHash::of_bytes(&base_bytes),
            "Commit::hash must equal ObjectHash::of_bytes(encode()) -- sanity-checking the \
             comparison mechanism itself"
        );

        let mut other_model_byte = base_bytes.clone();
        // There is only one valid wire byte (1) today; flipping it to an
        // adjacent still-plausible-looking value proves the tag is hashed,
        // even though that buffer would itself fail to decode.
        let version_byte = COMMIT_MAGIC.len();
        other_model_byte[version_byte] = 2;
        assert_ne!(
            base_bytes, other_model_byte,
            "sanity: the byte flip took effect"
        );

        let other_hash = ObjectHash::of_bytes(&other_model_byte);
        assert_ne!(
            base_hash, other_hash,
            "flipping only the content-model-version wire byte must change the commit's object \
             hash, or two commits differing solely in content model could collide"
        );
    }
}
