// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

// Error types for TLogFS operations
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum TLogFSError {
    #[error("Delta Lake error: {0}")]
    Delta(#[from] deltalake::DeltaTableError),

    #[error("Clap error: {0}")]
    Clap(#[from] clap::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("TinyFS error: {0}")]
    TinyFS(#[from] tinyfs::Error),

    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("Path not found: {path}")]
    NodeNotFound { path: PathBuf }, // @@@ PathNotFound

    #[error("Path exists: {path}")]
    PathExists { path: PathBuf },

    #[error("Transaction error: {message}")]
    Transaction { message: String },

    #[error("Missing data")]
    Missing,

    #[error("Partition not found: part_id={part_id}, node_id={node_id}. {hint}")]
    PartitionNotFound {
        part_id: String,
        node_id: String,
        hint: String,
    },

    #[error(
        "Legacy partition layout at {path}: data table is partitioned by [{found}] but D5 \
         requires [pond_id, part_id]. Re-initialize the pond (e.g. `pond init` into a fresh \
         directory and restore from your remote with `pond remote add` + `pond pull`); \
         no in-place migration is provided."
    )]
    LegacyPartitionLayout { path: PathBuf, found: String },

    #[error("Commit error: {message}")]
    Commit { message: String },

    #[error("Restore error: {message}")]
    Restore { message: String },

    #[error("Arrow error: {0}")]
    ArrowSchema(#[from] arrow_schema::ArrowError),

    #[error("Arrow error: {0}")]
    ArrowMessage(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_arrow::Error),

    #[error("Large file not found: {blake3} at path {path}")]
    LargeFileNotFound {
        blake3: String,
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("Large file integrity check failed: expected {expected}, got {actual}")]
    LargeFileIntegrityError { expected: String, actual: String },

    #[error("Content integrity check failed: expected {expected}, got {actual}")]
    ContentIntegrityError { expected: String, actual: String },

    #[error("Content missing BLAKE3 hash for verification")]
    ContentMissingHash,

    #[error(
        "row-rewriting collapse is unsupported for logical-series-v2 (docs/logical-series-identity-design.md): {reason}"
    )]
    CollapseUnsupported { reason: String },

    /// A `TablePhysicalSeries` append carried nonempty Parquet bytes (a
    /// real, decodable schema/footer) that decoded to zero rows. This is
    /// rejected at the write choke point rather than allowed through and
    /// left for steward's fold to discover: per
    /// `docs/logical-series-identity-design.md`'s nonempty-leaf invariant,
    /// a table leaf must contain at least one row. Unlike a
    /// `FilePhysicalSeries`, a `TablePhysicalSeries` has no genuinely
    /// leafless append at all (see
    /// [`TLogFSError::SeriesTableRequiresSchemaBearingFirstVersion`]), so
    /// there is no alternative zero-byte form to fall back to here either:
    /// write real, nonempty Parquet content with at least one row.
    #[error(
        "table series append for node {node_id} version {version} carries {byte_count} byte(s) \
         of Parquet content but decodes to zero rows -- a table series append must contain \
         at least one logical row"
    )]
    SeriesZeroRowAppend {
        node_id: String,
        version: i64,
        byte_count: i64,
    },

    /// A genuinely empty (zero-byte, leafless) append is only legitimate as
    /// a `FilePhysicalSeries` node's very first version -- release blocker
    /// item 1's zero-leaf-series-materialization support
    /// (`docs/logical-series-identity-design.md`) depends on that being a
    /// valid, materializable state. A leafless append to an ALREADY
    /// existing series (`version > 1`) is a different, unsupported case:
    /// steward's source-side content-tree fold (`build_series_manifest`)
    /// intentionally ignores every leafless version's own attributes when
    /// aggregating a series' identity-bearing `VersionMeta` (ordinary,
    /// tested behavior), so a trailing metadata-only version's attribute
    /// changes would never be visible to a destination fold at all, and
    /// its mtime bump is invisible to the incremental v2 planner (which
    /// only emits work when the leaf count grows or the node doesn't exist
    /// yet at the destination) -- silently leaving a replica's node
    /// permanently stale. Rejected here, before commit, rather than
    /// allowed to reach a state no writer can safely reproduce or
    /// replicate.
    #[error(
        "leafless (zero-byte) append for node {node_id} at version {version} is only supported \
         as a series' very first version (creating a legitimately empty series); a later \
         leafless append would change series-level attributes/mtime independently of the last \
         logical leaf, which cannot be reproduced by the v2 replication fold -- append real \
         content instead, or omit this version's metadata-only change"
    )]
    SeriesLeaflessAppendAfterFirstVersion { node_id: String, version: i64 },

    /// A `TablePhysicalSeries` append carried zero physical bytes (no
    /// Parquet content at all, hence no schema). Unlike a
    /// `FilePhysicalSeries`, this is rejected at *every* version, including
    /// the very first: [`sync_store::content::SeriesManifest::new`]
    /// unconditionally requires a schema fingerprint for
    /// [`sync_store::content::PayloadKind::Table`] regardless of
    /// `leaf_count`, and a table series can only ever obtain one from a
    /// real, nonempty Parquet append (see [`Self::SeriesZeroRowAppend`]),
    /// so a genuinely empty table series can never be folded into a valid
    /// `watertown.series.v1` manifest -- steward's own source-side fold
    /// (`build_series_manifest`) would fail this node with an opaque
    /// "table series requires a schema fingerprint" error at the next
    /// commit that touches it. Reject clearly here instead, before the row
    /// is ever durable.
    #[error(
        "table series append for node {node_id} at version {version} carries zero bytes (no \
         Parquet content, hence no schema) -- a table series can never be represented without a \
         schema fingerprint, so an empty TablePhysicalSeries cannot be created this way; write \
         real, nonempty Parquet content with at least one row for its first version instead"
    )]
    SeriesTableRequiresSchemaBearingFirstVersion { node_id: String, version: i64 },

    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<String> for TLogFSError {
    fn from(s: String) -> Self {
        TLogFSError::Internal(s)
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for TLogFSError {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Self {
        TLogFSError::Internal(e.to_string())
    }
}

impl From<TLogFSError> for provider::Error {
    fn from(e: TLogFSError) -> Self {
        provider::Error::TLogFS(e.to_string())
    }
}
