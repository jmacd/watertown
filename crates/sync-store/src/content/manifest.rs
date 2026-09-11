// SPDX-License-Identifier: Apache-2.0

//! In-memory node identity records shared by manifest-map, rebuild, and
//! capsule code.
//!
//! Native-v2 persistence is implemented by [`super::manifest_map`]. There is
//! intentionally no monolithic flat-manifest wire object.

use tinyfs::EntryType;

use super::{ObjectHash, VersionMeta};

/// One node's identity and current content metadata.
///
/// The persistent manifest map keys this record by `blake3(node_id)`. The
/// parent, name, type, hash, and version metadata are the value, so rename,
/// move, retype, content, and metadata changes all update the keyed leaf.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestEntry {
    /// Source node identity.
    pub node_id: String,
    /// Parent directory node identity, empty for the root.
    pub parent_node_id: String,
    /// Name within the parent, empty for the root.
    pub name: String,
    /// Node entry type.
    pub entry_type: EntryType,
    /// Current content address.
    pub child_hash: ObjectHash,
    /// Live version metadata, oldest first.
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

    /// Construct an entry without version metadata.
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
