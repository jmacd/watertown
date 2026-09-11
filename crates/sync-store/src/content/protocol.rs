// SPDX-License-Identifier: Apache-2.0

//! Canonical native-v2 publication records, object descriptors, and receipts.

use uuid::Uuid;

use super::manifest_map::{ManifestChange, decode_manifest_change, encode_manifest_change};
use super::{Cursor, ObjectHash, push_len_prefixed};

/// Native format written by the low-cost publication protocol.
pub const NATIVE_FORMAT_V2: &str = "watertown.native.v2";

const PUBLICATION_MAGIC: &[u8] = b"watertown.publication.v2\n";
const RECEIPT_MAGIC: &[u8] = b"watertown.object-receipt.v3\n";
const RECEIPT_AUTH_DOMAIN: &[u8] = b"watertown.object-receipt-auth.v3\n";

/// Canonical semantic class recorded for one immutable object.
///
/// The kind is part of the authenticated commit and publication records. It is
/// intentionally not part of payload receipt identity: identical bytes can be
/// referenced in more than one semantic role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ContentObjectKind {
    /// Untagged file/table/symlink payload bytes.
    RawBlob,
    /// Canonical directory tree.
    Tree,
    /// Persistent node-identity Merkle-map node.
    ManifestNode,
    /// Native logical-series manifest.
    SeriesManifest,
    /// Native commit object.
    Commit,
    /// Dynamic-node recipe.
    Recipe,
}

impl ContentObjectKind {
    pub(crate) fn to_wire(self) -> u8 {
        match self {
            Self::RawBlob => 0,
            Self::Tree => 1,
            Self::ManifestNode => 2,
            Self::SeriesManifest => 3,
            Self::Commit => 4,
            Self::Recipe => 5,
        }
    }

    pub(crate) fn from_wire(value: u8) -> Result<Self, String> {
        match value {
            0 => Ok(Self::RawBlob),
            1 => Ok(Self::Tree),
            2 => Ok(Self::ManifestNode),
            3 => Ok(Self::SeriesManifest),
            4 => Ok(Self::Commit),
            5 => Ok(Self::Recipe),
            other => Err(format!("unknown content object kind byte: {other}")),
        }
    }

    /// Stable diagnostic name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RawBlob => "raw_blob",
            Self::Tree => "tree",
            Self::ManifestNode => "manifest_node",
            Self::SeriesManifest => "series_manifest",
            Self::Commit => "commit",
            Self::Recipe => "recipe",
        }
    }
}

/// One immutable object named by a publication or commit delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectDescriptor {
    /// BLAKE3 address of the exact object bytes.
    pub hash: ObjectHash,
    /// Canonical semantic class authenticated by the receipt.
    pub kind: ContentObjectKind,
}

impl ObjectDescriptor {
    /// Construct an object descriptor.
    #[must_use]
    pub fn new(hash: ObjectHash, kind: ContentObjectKind) -> Self {
        Self { hash, kind }
    }
}

/// One immutable pack advertisement introduced by a publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PackDescriptor {
    /// Logical series the pack advertises.
    pub series_hash: ObjectHash,
    /// BLAKE3 address of the canonical pack-index bytes.
    pub pack_hash: ObjectHash,
}

impl PackDescriptor {
    /// Construct a pack descriptor.
    #[must_use]
    pub fn new(series_hash: ObjectHash, pack_hash: ObjectHash) -> Self {
        Self {
            series_hash,
            pack_hash,
        }
    }
}

/// Canonical immutable record describing exactly one successful push.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationRecord {
    /// Native format marker.
    pub format: String,
    /// Pond whose ref is advanced.
    pub pond_id: Uuid,
    /// Ref advanced by this publication.
    pub ref_name: String,
    /// Visible snapshot commit.
    pub snapshot_tip: ObjectHash,
    /// Persistent node-identity Merkle-map root.
    pub manifest_root: ObjectHash,
    /// Prior publication record, absent only for initial publication.
    pub parent_publication_record: Option<ObjectHash>,
    /// Immutable objects introduced by this push, sorted and deduplicated.
    pub introduced_objects: Vec<ObjectDescriptor>,
    /// Pack advertisements introduced by this push, sorted and deduplicated.
    pub introduced_packs: Vec<PackDescriptor>,
    /// Net node-identity changes from the parent snapshot to this snapshot.
    pub manifest_changes: Vec<ManifestChange>,
}

impl PublicationRecord {
    /// Construct and canonicalize a publication record.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pond_id: Uuid,
        ref_name: impl Into<String>,
        snapshot_tip: ObjectHash,
        manifest_root: ObjectHash,
        parent_publication_record: Option<ObjectHash>,
        introduced_objects: Vec<ObjectDescriptor>,
        introduced_packs: Vec<PackDescriptor>,
        manifest_changes: Vec<ManifestChange>,
    ) -> Result<Self, String> {
        let ref_name = ref_name.into();
        if ref_name.is_empty() {
            return Err("publication ref name is empty".to_string());
        }
        let mut introduced_objects = introduced_objects;
        introduced_objects.sort_unstable();
        introduced_objects.dedup();

        let mut introduced_packs = introduced_packs;
        introduced_packs.sort_unstable();
        introduced_packs.dedup();

        let manifest_changes = ManifestChange::canonicalize(manifest_changes)?;
        Ok(Self {
            format: NATIVE_FORMAT_V2.to_string(),
            pond_id,
            ref_name,
            snapshot_tip,
            manifest_root,
            parent_publication_record,
            introduced_objects,
            introduced_packs,
            manifest_changes,
        })
    }

    /// Canonical wire bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(PUBLICATION_MAGIC);
        push_len_prefixed(&mut bytes, self.format.as_bytes());
        bytes.extend_from_slice(self.pond_id.as_bytes());
        push_len_prefixed(&mut bytes, self.ref_name.as_bytes());
        bytes.extend_from_slice(self.snapshot_tip.as_bytes());
        bytes.extend_from_slice(self.manifest_root.as_bytes());
        match self.parent_publication_record {
            Some(parent) => {
                bytes.push(1);
                bytes.extend_from_slice(parent.as_bytes());
            }
            None => bytes.push(0),
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
        let change_count =
            u32::try_from(self.manifest_changes.len()).expect("change count exceeds u32::MAX");
        bytes.extend_from_slice(&change_count.to_le_bytes());
        for change in &self.manifest_changes {
            let encoded = encode_manifest_change(change);
            push_len_prefixed(&mut bytes, &encoded);
        }
        bytes
    }

    /// Content address of [`Self::encode`].
    #[must_use]
    pub fn hash(&self) -> ObjectHash {
        ObjectHash::of_bytes(&self.encode())
    }

    /// Strictly decode and revalidate canonical publication bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let mut cursor = Cursor::new(bytes);
        cursor.expect_tag(PUBLICATION_MAGIC)?;
        let format = cursor.take_len_prefixed_string()?;
        if format != NATIVE_FORMAT_V2 {
            return Err(format!("unsupported publication format {format:?}"));
        }
        let pond_id_bytes = cursor.take_array::<16>()?;
        let pond_id = Uuid::from_bytes(pond_id_bytes);
        let ref_name = cursor.take_len_prefixed_string()?;
        let snapshot_tip = cursor.take_hash()?;
        let manifest_root = cursor.take_hash()?;
        let parent_publication_record = match cursor.take_u8()? {
            0 => None,
            1 => Some(cursor.take_hash()?),
            other => return Err(format!("invalid publication parent flag {other}")),
        };
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
        let change_count = cursor.take_u32()? as usize;
        let mut manifest_changes = Vec::with_capacity(cursor.bounded_capacity(change_count, 8));
        for _ in 0..change_count {
            manifest_changes.push(decode_manifest_change(cursor.take_len_prefixed()?)?);
        }
        if !cursor.is_empty() {
            return Err(format!(
                "{} trailing byte(s) after publication record",
                cursor.remaining()
            ));
        }
        let decoded = Self::new(
            pond_id,
            ref_name,
            snapshot_tip,
            manifest_root,
            parent_publication_record,
            introduced_objects,
            introduced_packs,
            manifest_changes,
        )?;
        if decoded.encode() != bytes {
            return Err("publication record is not canonically encoded".to_string());
        }
        Ok(decoded)
    }
}

/// Authenticated identity receipt for one canonical immutable payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectReceipt {
    /// Payload BLAKE3 address.
    pub payload_hash: ObjectHash,
    /// Exact byte length.
    pub byte_length: u64,
    /// Provider ETag, when returned.
    pub e_tag: Option<String>,
    /// Provider version identifier, when returned.
    pub version: Option<String>,
}

impl ObjectReceipt {
    /// Construct a validated receipt.
    pub fn new(
        payload_hash: ObjectHash,
        byte_length: u64,
        e_tag: Option<String>,
        version: Option<String>,
    ) -> Result<Self, String> {
        for (field, value) in [("e_tag", e_tag.as_deref()), ("version", version.as_deref())] {
            if value.is_some_and(str::is_empty) {
                return Err(format!("receipt {field} must be None rather than empty"));
            }
        }
        Ok(Self {
            payload_hash,
            byte_length,
            e_tag,
            version,
        })
    }

    fn encode_body(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(RECEIPT_MAGIC);
        bytes.extend_from_slice(self.payload_hash.as_bytes());
        bytes.extend_from_slice(&self.byte_length.to_le_bytes());
        encode_optional_string(&mut bytes, self.e_tag.as_deref());
        encode_optional_string(&mut bytes, self.version.as_deref());
        bytes
    }

    /// Canonical authenticated receipt bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = self.encode_body();
        let mut hasher = blake3::Hasher::new();
        hasher.update(RECEIPT_AUTH_DOMAIN);
        hasher.update(&bytes);
        bytes.extend_from_slice(hasher.finalize().as_bytes());
        bytes
    }

    /// Strictly decode and authenticate receipt bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < RECEIPT_MAGIC.len() + 32 + 8 + 4 + 4 + 32 {
            return Err("object receipt is truncated".to_string());
        }
        let (body, authentication) = bytes.split_at(bytes.len() - 32);
        let mut hasher = blake3::Hasher::new();
        hasher.update(RECEIPT_AUTH_DOMAIN);
        hasher.update(body);
        if hasher.finalize().as_bytes() != authentication {
            return Err("object receipt authentication mismatch".to_string());
        }
        let mut cursor = Cursor::new(body);
        cursor.expect_tag(RECEIPT_MAGIC)?;
        let payload_hash = cursor.take_hash()?;
        let byte_length = cursor.take_u64()?;
        let e_tag = decode_optional_string(&mut cursor)?;
        let version = decode_optional_string(&mut cursor)?;
        if !cursor.is_empty() {
            return Err(format!(
                "{} trailing byte(s) in object receipt body",
                cursor.remaining()
            ));
        }
        let decoded = Self::new(payload_hash, byte_length, e_tag, version)?;
        if decoded.encode() != bytes {
            return Err("object receipt is not canonically encoded".to_string());
        }
        Ok(decoded)
    }
}

fn encode_optional_string(bytes: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => push_len_prefixed(bytes, value.as_bytes()),
        None => push_len_prefixed(bytes, &[]),
    }
}

fn decode_optional_string(cursor: &mut Cursor<'_>) -> Result<Option<String>, String> {
    let bytes = cursor.take_len_prefixed()?;
    if bytes.is_empty() {
        Ok(None)
    } else {
        String::from_utf8(bytes.to_vec())
            .map(Some)
            .map_err(|error| format!("invalid UTF-8 receipt metadata: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use tinyfs::EntryType;

    use super::super::ManifestEntry;
    use super::super::manifest_map::{ManifestRecord, ManifestRecordChild};
    use super::*;

    fn hash(value: &str) -> ObjectHash {
        ObjectHash::of_bytes(value.as_bytes())
    }

    #[test]
    fn receipt_round_trips_and_authenticates() {
        let receipt =
            ObjectReceipt::new(hash("payload"), 7, Some("\"etag\"".to_string()), None).unwrap();
        let bytes = receipt.encode();
        assert_eq!(ObjectReceipt::decode(&bytes).unwrap(), receipt);
        let mut corrupt = bytes;
        corrupt[10] ^= 1;
        assert!(ObjectReceipt::decode(&corrupt).is_err());
    }

    #[test]
    fn publication_round_trips_canonically() {
        let record = ManifestRecord::new(
            ManifestEntry::bare(
                "node",
                "root",
                "file",
                EntryType::FilePhysicalVersion,
                hash("payload"),
            ),
            Vec::<ManifestRecordChild>::new(),
        )
        .unwrap();
        let publication = PublicationRecord::new(
            Uuid::nil(),
            "main",
            hash("tip"),
            hash("manifest"),
            None,
            vec![ObjectDescriptor::new(
                hash("payload"),
                ContentObjectKind::RawBlob,
            )],
            vec![],
            vec![ManifestChange::new(None, Some(record)).unwrap()],
        )
        .unwrap();
        let bytes = publication.encode();
        assert_eq!(PublicationRecord::decode(&bytes).unwrap(), publication);
        assert_eq!(publication.hash(), ObjectHash::of_bytes(&bytes));
    }

    #[test]
    fn publication_allows_one_payload_in_multiple_semantic_roles() {
        let payload = hash("shared");
        let publication = PublicationRecord::new(
            Uuid::nil(),
            "main",
            hash("tip"),
            hash("manifest"),
            None,
            vec![
                ObjectDescriptor::new(payload, ContentObjectKind::RawBlob),
                ObjectDescriptor::new(payload, ContentObjectKind::Recipe),
            ],
            vec![],
            vec![],
        )
        .unwrap();
        assert_eq!(publication.introduced_objects.len(), 2);
        assert_eq!(
            PublicationRecord::decode(&publication.encode()).unwrap(),
            publication
        );
    }
}
