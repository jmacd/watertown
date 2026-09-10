// SPDX-License-Identifier: Apache-2.0

//! Native-v2 commit objects: snapshot root, lineage, and bounded change delta.

use super::manifest_map::{ManifestChange, decode_manifest_change, encode_manifest_change};
use super::{
    ContentObjectKind, Cursor, ObjectDescriptor, ObjectHash, PackDescriptor, push_len_prefixed,
};

const COMMIT_MAGIC: &[u8] = b"watertown.commit.v2\n";

/// Content-addressing model named by a commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentModelVersion {
    /// Persistent node-identity Merkle map plus immutable publication objects.
    PublicationV2,
}

impl ContentModelVersion {
    fn to_wire(self) -> u8 {
        match self {
            Self::PublicationV2 => 1,
        }
    }

    fn from_wire(value: u8) -> Result<Self, String> {
        match value {
            1 => Ok(Self::PublicationV2),
            other => Err(format!("unknown content model version byte: {other}")),
        }
    }
}

/// Lineage and audit metadata isolated from shareable content objects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Provenance {
    /// Pond that produced this commit.
    pub pond_id: String,
    /// Pond-local transaction sequence.
    pub seq: i64,
    /// Commit time in microseconds since the Unix epoch.
    pub time_micros: i64,
    /// Human-meaningful author identifier.
    pub author: String,
    /// Original request that produced the transaction.
    pub request: String,
}

/// One native-v2 snapshot commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    /// Content model used by this commit.
    pub content_model_version: ContentModelVersion,
    /// Hash of the root directory tree.
    pub root_tree_hash: ObjectHash,
    /// Prior commit in the local linear chain.
    pub parent_commit_hash: Option<ObjectHash>,
    /// Root of the persistent node-identity Merkle map.
    pub manifest_root: ObjectHash,
    /// Net identity-map changes made by this transaction.
    pub manifest_changes: Vec<ManifestChange>,
    /// Immutable payload/metadata objects created or reintroduced by this
    /// transaction. The commit object itself is added by publication.
    pub introduced_objects: Vec<ObjectDescriptor>,
    /// Pack advertisements derived for series changed by this transaction.
    pub introduced_packs: Vec<PackDescriptor>,
    /// Lineage and audit metadata.
    pub provenance: Provenance,
}

impl Commit {
    /// Construct a commit without a delta (primarily for codec/unit callers).
    #[must_use]
    pub fn new(
        content_model_version: ContentModelVersion,
        root_tree_hash: ObjectHash,
        parent_commit_hash: Option<ObjectHash>,
        manifest_root: ObjectHash,
        provenance: Provenance,
    ) -> Self {
        Self {
            content_model_version,
            root_tree_hash,
            parent_commit_hash,
            manifest_root,
            manifest_changes: Vec::new(),
            introduced_objects: Vec::new(),
            introduced_packs: Vec::new(),
            provenance,
        }
    }

    /// Construct and canonicalize a commit carrying its bounded transaction
    /// delta.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_delta(
        content_model_version: ContentModelVersion,
        root_tree_hash: ObjectHash,
        parent_commit_hash: Option<ObjectHash>,
        manifest_root: ObjectHash,
        manifest_changes: Vec<ManifestChange>,
        mut introduced_objects: Vec<ObjectDescriptor>,
        mut introduced_packs: Vec<PackDescriptor>,
        provenance: Provenance,
    ) -> Result<Self, String> {
        let manifest_changes = ManifestChange::canonicalize(manifest_changes)?;
        introduced_objects.sort_unstable();
        introduced_objects.dedup();
        introduced_packs.sort_unstable();
        introduced_packs.dedup();
        Ok(Self {
            content_model_version,
            root_tree_hash,
            parent_commit_hash,
            manifest_root,
            manifest_changes,
            introduced_objects,
            introduced_packs,
            provenance,
        })
    }

    /// Canonical wire bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(COMMIT_MAGIC);
        bytes.push(self.content_model_version.to_wire());
        bytes.extend_from_slice(self.root_tree_hash.as_bytes());
        match self.parent_commit_hash {
            Some(parent) => {
                bytes.push(1);
                bytes.extend_from_slice(parent.as_bytes());
            }
            None => bytes.push(0),
        }
        bytes.extend_from_slice(self.manifest_root.as_bytes());

        let change_count =
            u32::try_from(self.manifest_changes.len()).expect("change count exceeds u32::MAX");
        bytes.extend_from_slice(&change_count.to_le_bytes());
        for change in &self.manifest_changes {
            push_len_prefixed(&mut bytes, &encode_manifest_change(change));
        }

        let object_count =
            u32::try_from(self.introduced_objects.len()).expect("object count exceeds u32::MAX");
        bytes.extend_from_slice(&object_count.to_le_bytes());
        for object in &self.introduced_objects {
            bytes.extend_from_slice(object.hash.as_bytes());
            bytes.push(object.kind.to_wire());
        }

        let pack_count =
            u32::try_from(self.introduced_packs.len()).expect("pack count exceeds u32::MAX");
        bytes.extend_from_slice(&pack_count.to_le_bytes());
        for pack in &self.introduced_packs {
            bytes.extend_from_slice(pack.series_hash.as_bytes());
            bytes.extend_from_slice(pack.pack_hash.as_bytes());
        }

        push_len_prefixed(&mut bytes, self.provenance.pond_id.as_bytes());
        bytes.extend_from_slice(&self.provenance.seq.to_le_bytes());
        bytes.extend_from_slice(&self.provenance.time_micros.to_le_bytes());
        push_len_prefixed(&mut bytes, self.provenance.author.as_bytes());
        push_len_prefixed(&mut bytes, self.provenance.request.as_bytes());
        bytes
    }

    /// BLAKE3 address of [`Self::encode`].
    #[must_use]
    pub fn hash(&self) -> ObjectHash {
        ObjectHash::of_bytes(&self.encode())
    }

    /// Strictly decode one native-v2 commit.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let mut cursor = Cursor::new(bytes);
        cursor.expect_tag(COMMIT_MAGIC)?;
        let content_model_version = ContentModelVersion::from_wire(cursor.take_u8()?)?;
        let root_tree_hash = cursor.take_hash()?;
        let parent_commit_hash = match cursor.take_u8()? {
            0 => None,
            1 => Some(cursor.take_hash()?),
            other => return Err(format!("invalid parent flag {other}")),
        };
        let manifest_root = cursor.take_hash()?;

        let change_count = cursor.take_u32()? as usize;
        let mut manifest_changes = Vec::with_capacity(cursor.bounded_capacity(change_count, 8));
        for _ in 0..change_count {
            manifest_changes.push(decode_manifest_change(cursor.take_len_prefixed()?)?);
        }

        let object_count = cursor.take_u32()? as usize;
        let mut introduced_objects = Vec::with_capacity(cursor.bounded_capacity(object_count, 33));
        for _ in 0..object_count {
            introduced_objects.push(ObjectDescriptor {
                hash: cursor.take_hash()?,
                kind: ContentObjectKind::from_wire(cursor.take_u8()?)?,
            });
        }

        let pack_count = cursor.take_u32()? as usize;
        let mut introduced_packs = Vec::with_capacity(cursor.bounded_capacity(pack_count, 64));
        for _ in 0..pack_count {
            introduced_packs.push(PackDescriptor {
                series_hash: cursor.take_hash()?,
                pack_hash: cursor.take_hash()?,
            });
        }

        let pond_id = cursor.take_len_prefixed_string()?;
        let seq = cursor.take_i64()?;
        let time_micros = cursor.take_i64()?;
        let author = cursor.take_len_prefixed_string()?;
        let request = cursor.take_len_prefixed_string()?;
        if !cursor.is_empty() {
            return Err(format!(
                "{} trailing byte(s) after commit",
                cursor.remaining()
            ));
        }
        let decoded = Self::new_with_delta(
            content_model_version,
            root_tree_hash,
            parent_commit_hash,
            manifest_root,
            manifest_changes,
            introduced_objects,
            introduced_packs,
            Provenance {
                pond_id,
                seq,
                time_micros,
                author,
                request,
            },
        )?;
        if decoded.encode() != bytes {
            return Err("commit is not canonically encoded".to_string());
        }
        Ok(decoded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(value: &str) -> ObjectHash {
        ObjectHash::of_bytes(value.as_bytes())
    }

    fn provenance() -> Provenance {
        Provenance {
            pond_id: "pond".to_string(),
            seq: 7,
            time_micros: 9,
            author: "author".to_string(),
            request: "request".to_string(),
        }
    }

    fn commit(parent: Option<ObjectHash>) -> Commit {
        Commit::new(
            ContentModelVersion::PublicationV2,
            hash("root"),
            parent,
            hash("manifest"),
            provenance(),
        )
    }

    #[test]
    fn codec_round_trips_and_hashes_exact_bytes() {
        for commit in [commit(None), commit(Some(hash("parent")))] {
            let bytes = commit.encode();
            assert_eq!(Commit::decode(&bytes).unwrap(), commit);
            assert_eq!(commit.hash(), ObjectHash::of_bytes(&bytes));
        }
    }

    #[test]
    fn roots_parent_and_provenance_change_identity() {
        let base = commit(None);
        assert_ne!(base.hash(), commit(Some(hash("parent"))).hash());
        let mut changed = base.clone();
        changed.root_tree_hash = hash("other-root");
        assert_ne!(base.hash(), changed.hash());
        let mut changed = base.clone();
        changed.manifest_root = hash("other-manifest");
        assert_ne!(base.hash(), changed.hash());
        let mut changed = base.clone();
        changed.provenance.seq += 1;
        assert_ne!(base.hash(), changed.hash());
    }

    #[test]
    fn decoder_rejects_bad_magic_truncation_and_trailing_bytes() {
        let bytes = commit(None).encode();
        let mut bad = bytes.clone();
        bad[0] ^= 1;
        assert!(Commit::decode(&bad).is_err());
        assert!(Commit::decode(&bytes[..bytes.len() - 1]).is_err());
        let mut trailing = bytes;
        trailing.push(0);
        assert!(Commit::decode(&trailing).is_err());
    }
}
