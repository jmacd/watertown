// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Version selection for ListingTable queries
//!
//! Determines which file versions to include when querying data.

use log::debug;

/// Version selection for ListingTable
#[derive(Clone, Debug, Hash, PartialEq, Eq, Default)]
pub enum VersionSelection {
    /// All versions (replaces SeriesTable)
    #[default]
    AllVersions,
    /// Latest version only (replaces TinyFsTableProvider)
    LatestVersion,
    /// Specific version (replaces NodeVersionTable)
    SpecificVersion(u64),
}

impl VersionSelection {
    /// Centralized debug logging for version selection
    /// Eliminates duplicate debug logging patterns throughout the codebase
    pub fn log_debug(&self, node_id: &tinyfs::NodeID) {
        match self {
            VersionSelection::AllVersions => {
                debug!("Version selection: ALL versions for node {node_id}");
            }
            VersionSelection::LatestVersion => {
                debug!("Version selection: LATEST version for node {node_id}");
            }
            VersionSelection::SpecificVersion(version) => {
                debug!("Version selection: SPECIFIC version {version} for node {node_id}");
            }
        }
    }

    /// Convert to cache key string
    /// Used for TableProvider caching to avoid schema inference overhead
    #[must_use]
    pub fn to_cache_string(&self) -> String {
        match self {
            VersionSelection::AllVersions => "all".to_string(),
            VersionSelection::LatestVersion => "latest".to_string(),
            VersionSelection::SpecificVersion(version) => format!("v{version}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_selection_cache_keys_are_distinct() {
        assert_eq!(VersionSelection::AllVersions.to_cache_string(), "all");
        assert_eq!(VersionSelection::LatestVersion.to_cache_string(), "latest");
        assert_eq!(
            VersionSelection::SpecificVersion(42).to_cache_string(),
            "v42"
        );
    }
}
