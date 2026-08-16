// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Graft pin records -- the content-addressed reference a pond keeps to the
//! foreign tip it has grafted in (Section 8.5.2).
//!
//! A cross-pond import mounts a foreign pond's tree under its own pond_id but
//! deliberately OMITS that mount node from the content-tree fold, so the graft
//! is non-transitive: a downstream consumer that mirrors this pond never
//! re-replicates the foreign pond's closure.
//!
//! Omitting the mount node, however, would leave nothing in the content graph
//! recording WHICH foreign tip was grafted.  To make the graft a first-class,
//! content-addressed reference, each import also writes a small YAML pin file
//! at `/sys/grafts/<name>`.  That file is ordinary pond-owned content: it is
//! covered by this pond's commit hash and replicates to consumers through the
//! normal tree/push/pull path.  A consumer therefore learns the pinned foreign
//! tip without fetching the foreign pond's content -- an inert reference,
//! exactly like a git submodule pointer whose submodule has not been checked
//! out.

use serde::{Deserialize, Serialize};

/// Filesystem directory holding graft pin records.  Each child is a YAML file
/// named `<remote-name>` containing a [`GraftPin`].
pub const SYS_GRAFTS_DIR: &str = "/sys/grafts";

/// The content-addressed reference a pond keeps to a grafted foreign tip.
///
/// Written at `/sys/grafts/<name>` in the same transaction as a cross-pond
/// import. The
/// `pinned_tip` is the foreign pond's tip commit hash at import time; the
/// foreign content it commits to is NOT fetched into this pond (the graft is
/// non-transitive), so the reference is intentionally dangling for any
/// consumer that has not separately replicated the foreign pond.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraftPin {
    /// The foreign pond's id (UUID string) whose tip is grafted.
    pub foreign_pond_id: String,

    /// The in-pond path at which the foreign pond is mounted.
    pub mount_path: String,

    /// Hex-encoded tip commit hash of the foreign pond at import time.
    pub pinned_tip: String,
}

impl GraftPin {
    /// Path of the pin file for a remote named `name`.
    #[must_use]
    pub fn pin_path(name: &str) -> String {
        format!("{SYS_GRAFTS_DIR}/{name}")
    }

    /// Serialize this pin to YAML bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_yaml(&self) -> Result<String, serde_yaml::Error> {
        serde_yaml::to_string(self)
    }

    /// Parse a pin from YAML bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes are not valid pin YAML.
    pub fn from_yaml_bytes(bytes: &[u8]) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_slice(bytes)
    }
}

/// Split an absolute, non-root graft mount path into its parent and leaf.
///
/// # Errors
///
/// Returns an error when the path is relative, root, or has no leaf name.
pub fn split_mount_path(path: &str) -> Result<(&str, &str), String> {
    if !path.starts_with('/') {
        return Err(format!("mount path `{path}` must be absolute"));
    }
    if path == "/" {
        return Err("graft mount path cannot be `/`".to_string());
    }
    let trimmed = path.trim_end_matches('/');
    let last_slash = trimmed
        .rfind('/')
        .ok_or_else(|| format!("mount path `{path}` has no leaf"))?;
    let parent = if last_slash == 0 {
        "/"
    } else {
        &trimmed[..last_slash]
    };
    let leaf = &trimmed[last_slash + 1..];
    if leaf.is_empty() {
        return Err(format!("mount path `{path}` has empty leaf"));
    }
    Ok((parent, leaf))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pin_path_format() {
        assert_eq!(GraftPin::pin_path("upstreamA"), "/sys/grafts/upstreamA");
    }

    #[test]
    fn yaml_roundtrip() {
        let pin = GraftPin {
            foreign_pond_id: "01998f3a-0000-7000-8000-000000000001".to_string(),
            mount_path: "/imports/A".to_string(),
            pinned_tip: "a".repeat(64),
        };
        let yaml = pin.to_yaml().expect("serialize");
        let back = GraftPin::from_yaml_bytes(yaml.as_bytes()).expect("parse");
        assert_eq!(pin, back);
    }

    #[test]
    fn rejects_unknown_fields() {
        let yaml = "foreign_pond_id: x\nmount_path: /imports/A\npinned_tip: y\nbogus: 1\n";
        assert!(GraftPin::from_yaml_bytes(yaml.as_bytes()).is_err());
    }

    #[test]
    fn splits_mount_paths() {
        assert_eq!(split_mount_path("/imports").unwrap(), ("/", "imports"));
        assert_eq!(
            split_mount_path("/imports/upstream/").unwrap(),
            ("/imports", "upstream")
        );
        assert!(split_mount_path("/").is_err());
        assert!(split_mount_path("imports/upstream").is_err());
    }
}
