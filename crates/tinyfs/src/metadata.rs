// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::EntryType;
use crate::error::Result;
use async_trait::async_trait;

/// Consolidated metadata for filesystem nodes
#[derive(Debug, Clone, PartialEq)]
pub struct NodeMetadata {
    /// Node version (incremented on each modification)
    pub version: u64,

    /// File size in bytes (None for directories and symlinks)
    pub size: Option<u64>,

    /// BLAKE3 checksum (Some for all files, None for directories/symlinks)
    pub blake3: Option<String>,

    /// BAO-tree outboard data (Some for files with bao_outboard, None otherwise)
    pub bao_outboard: Option<Vec<u8>>,

    /// Entry type (file, directory, symlink with file format variants)
    pub entry_type: EntryType,

    /// Entry timestamp
    pub timestamp: i64,
}

/// Common metadata interface for all filesystem nodes
/// This trait provides a uniform way to access metadata across different node types
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait Metadata: Send + Sync {
    /// Get consolidated metadata for this node
    async fn metadata(&self) -> Result<NodeMetadata>;
}
