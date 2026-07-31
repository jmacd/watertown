// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Table Provider Configuration Options
//!
//! This module provides configuration types for creating DataFusion TableProviders
//! in a flexible, provider-agnostic way.

use crate::VersionSelection;
use tinyfs::FileID;

/// Configuration options for table provider creation
///
/// Follows anti-duplication principles: single configurable function instead of multiple variants.
/// Used by both tlogfs and in-memory testing implementations.
#[derive(Default, Clone)]
pub struct TableProviderOptions {
    /// Version selection strategy (LatestVersion, AllVersions, or SpecificVersion)
    pub version_selection: VersionSelection,

    /// Multiple file URLs/paths to combine into a single table
    /// If empty, will use the node_id/part_id pattern (existing behavior)
    pub additional_urls: Vec<String>,

    /// Per-version prune for append-only series: only the version Parquets a
    /// bounded read can reach are listed, instead of the whole version history.
    ///
    /// This is a conservative *superset* filter, never a correctness filter: a
    /// version with no recorded `max_event_time` is always retained, and the
    /// caller still applies its own time predicate. [`tinyfs::SeriesReadBounds::NONE`]
    /// (the default) lists every live version, unchanged.
    pub bounds: tinyfs::SeriesReadBounds,
}

/// Cache key for TableProvider instances
///
/// Used to avoid recreating TableProviders for the same file/version combination.
/// Enables efficient schema inference caching across queries.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableProviderKey {
    /// The file being accessed
    pub file_id: FileID,

    /// Version selection strategy
    pub version_selection: VersionSelection,

    /// Per-version series prune in effect. Part of the key because a bounded
    /// provider lists only a subset of the version Parquets: serving one to an
    /// unbounded request (or to a differently-bounded one) would silently drop
    /// rows.
    pub bounds: tinyfs::SeriesReadBounds,
}

impl TableProviderKey {
    /// Create a new cache key for an unbounded (full-history) read.
    #[must_use]
    pub fn new(file_id: FileID, version_selection: VersionSelection) -> Self {
        Self::with_bounds(file_id, version_selection, tinyfs::SeriesReadBounds::NONE)
    }

    /// Create a new cache key for a read pruned by `bounds`.
    #[must_use]
    pub fn with_bounds(
        file_id: FileID,
        version_selection: VersionSelection,
        bounds: tinyfs::SeriesReadBounds,
    ) -> Self {
        Self {
            file_id,
            version_selection,
            bounds,
        }
    }

    /// Convert to string for HashMap-based caching
    ///
    /// Format: "file_id:version_selection", with a `:b<lo>/<gt>` suffix only
    /// when a prune is in effect. Unbounded keys keep their historical spelling
    /// so existing cache entries stay addressable.
    ///
    /// Example: "12345:latest", "12345:latest:b1700000000000000/-"
    #[must_use]
    pub fn to_cache_string(&self) -> String {
        let base = format!(
            "{}:{}",
            self.file_id,
            self.version_selection.to_cache_string()
        );
        if self.bounds == tinyfs::SeriesReadBounds::NONE {
            return base;
        }
        let part = |v: Option<i64>| v.map_or_else(|| "-".to_string(), |n| n.to_string());
        format!(
            "{}:b{}/{}",
            base,
            part(self.bounds.event_time_lo),
            part(self.bounds.version_gt)
        )
    }
}
