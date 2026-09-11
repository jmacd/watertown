// SPDX-License-Identifier: Apache-2.0

//! Errors returned by the store crate.

use std::io;

/// Errors returned from [`crate::Store`] operations.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The store path was invalid (could not be turned into a file:// URL).
    #[error("invalid store path: {0}")]
    InvalidPath(String),

    /// IO error from the local filesystem.
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// Arrow-related error (RecordBatch construction, schema mismatch).
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Delta Lake error.
    #[error("delta error: {0}")]
    Delta(#[from] deltalake::DeltaTableError),

    /// DataFusion error (during query execution).
    #[error("datafusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    /// JSON serialization/deserialization error (for control-table style data).
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// A logical invariant was violated (returned instead of panicking).
    #[error("invariant violation: {0}")]
    Invariant(String),

    /// A partially initialized content remote has not recorded its identity.
    #[error("remote has no recorded pond_id")]
    MissingPondId,
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, StoreError>;
