// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

pub mod arrow;
pub mod async_helpers;
pub mod bao_validating_reader;
pub mod caching_persistence;
pub mod chained_reader;
pub mod context;
mod dir;
mod entry_type;
mod error;
mod file;
mod fs;
pub mod hostmount;
pub mod memory;
mod metadata;
mod node;
mod path;
pub mod persistence;
mod symlink;
pub mod testing;
pub mod transaction_guard;
pub mod tree_format;
mod wd;

// Public exports - Core filesystem API
pub use context::{ExportHint, FactoryContext, PondMetadata, ProviderContext};
pub use dir::{Directory, DirectoryEntry, Handle as DirHandle, Pathed};
pub use file::{
    AsyncReadSeek, File, FileMetadataWriter, Handle as FileHandle, QueryableFile, SeriesReadBounds,
};
pub use fs::FS;
pub use node::{
    FileID, INDEX_NODE_NAME, INDEX_NODE_UUID, LOG_NODE_UUID, Node, NodeID, NodePath, NodeType,
    PartID, ROOT_UUID, index_node_uuid, local_pond_uuid, log_node_uuid, set_local_pond_uuid,
};
pub use wd::{CopyDestination, Lookup, Visitor, WD};

// Buffer utilities for tests and special cases
// WARNING: These load entire files into memory - use sparingly
pub use async_helpers::buffer_helpers;
pub use caching_persistence::CachingPersistence;
pub use entry_type::EntryType;
pub use error::{Error, Result, ResultExt};
pub use hostmount::HostmountPersistence;
pub use memory::persistence::MemoryPersistence;
pub use metadata::{Metadata, NodeMetadata};
pub use persistence::{FileVersionInfo, PersistenceLayer};
pub use symlink::{Handle as SymlinkHandle, Symlink};
pub use transaction_guard::TransactionGuard;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod metadata_impl_tests;
