#![allow(tail_expr_drop_order)]
// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! TLogFS - A filesystem implementation using Delta Lake for storage
//!
//! Set RUST_LOG environment variable to control logging:
//! - RUST_LOG=off (default) - silent
//! - RUST_LOG=info - basic operations  
//! - RUST_LOG=debug - detailed diagnostics
//! - RUST_LOG=tlogfs=debug - debug only tlogfs crate

/// Core schema and data structures
pub mod schema;

/// Factory implementations
pub mod factories;

/// Large file storage utilities
pub mod large_files;

/// Persistence layer implementation
pub mod persistence;

// Re-export bao_outboard from utilities for backwards compatibility
pub use utilities::bao_outboard;

// Transaction guard implementation
pub mod transaction_guard;

// File writer implementation with clean write path
pub mod file_writer;

// DataFusion query interfaces
pub mod query;

// Arrow-backed filesystem object implementations
pub mod directory;
pub mod file;
pub mod symlink;

// Error types
pub mod error;

// Transaction metadata - required for all commits
pub mod txn_metadata;

// Data taxonomy for sensitive configuration fields
pub mod data_taxonomy;

// Schema validation utilities
pub mod schema_validation;
pub mod series_identity;

/// Extract State from provider::FactoryContext
///
/// Helper to downcast the persistence layer back to concrete State.
pub fn extract_state(pctx: &FactoryContext) -> Result<persistence::State, TLogFSError> {
    pctx.context
        .persistence
        .as_any()
        .downcast_ref::<persistence::State>()
        .ok_or_else(|| {
            TLogFSError::Internal(
                "Persistence layer is not tlogfs::State - cannot access Delta table".to_string(),
            )
        })
        .cloned()
}

// TinyFS ObjectStore implementation for DataFusion ListingTable integration
// Re-export from provider crate where it's now generically implemented
pub use provider::TinyFsObjectStore;

// Re-export key types
pub use error::TLogFSError;
pub use file::OpLogFileWriter;
pub use persistence::CollapsibleSeries;
pub use persistence::ExternalAddAction;
pub use persistence::OpLogPersistence;
pub use schema::{DirectoryEntry, OplogEntry};
// Backward compatibility alias
#[allow(deprecated)]
pub use schema::VersionedDirectoryEntry;
pub use transaction_guard::TransactionGuard;
pub use txn_metadata::{PondTxnMetadata, PondUserMetadata};

// Re-export query interfaces for DataFusion integration
pub use query::{execute_sql_on_file, get_file_schema};

// Re-export factory types from provider for easy access
pub use provider::DYNAMIC_FACTORIES;
pub use provider::FactoryContext;
pub use provider::{ConfigFile, DynamicFactory, FactoryRegistry, PondMetadata};

// Note: Macros are #[macro_export] so they're automatically available at crate::macro_name
// No need to re-export them here

// Test utilities for DRY test patterns
#[cfg(test)]
pub mod test_utils;

#[cfg(test)]
mod tests;
