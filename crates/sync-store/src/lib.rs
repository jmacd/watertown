// SPDX-License-Identifier: Apache-2.0

//! Delta-Lake-backed key/value store with per-partition content checksums.
//!
//! See `../README.md` for context.
//!
//! # Quick tour
//!
//! - [`Store`]: the Delta-Lake-backed table holding versioned key/value rows.
//! - [`Op`]: a single Put or Delete in a batch.
//! - [`StoreError`] / [`Result`]: the error type used throughout.
//! - [`checksum::PartitionChecksum`]: the strategy trait for computing
//!   per-partition content checksums; two impls
//!   ([`checksum::Merkle`], [`checksum::Homomorphic`]).
//!
//! # Schema
//!
//! See [`schema`] for the column layout.  Briefly: each row is one
//! version of one item, partitioned by `partition_key`, with a
//! BLAKE3-of-value field on every row to support per-partition content
//! checksums.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod azure_registration;
pub mod checksum;
pub mod content;
pub mod content_remote;
mod error;
pub mod metered_store;
mod s3_registration;
pub mod schema;
mod store;
pub mod testing;
pub mod tlog;

pub use azure_registration::{AZURE_SCHEMES, register_azure_handlers};
pub use s3_registration::register_s3_handlers;

pub use content::{
    Commit, ManifestEntry, NodeMerkle, ObjectHash, Provenance, TreeEntry, VersionMeta,
    decode_series, decode_tree, node_merkle_rebuild_root, series_hash, tree_hash,
};
pub use content_remote::ContentRemote;
pub use error::{Result, StoreError};
pub use metered_store::{
    MeteredStore, Observation, StorageMeter, current_meter, observed, with_meter,
};
pub use store::{AddPath, CompactMetrics, Op, RemovePath, Store};
pub use tlog::{
    Checkpoint, CheckpointError, LogHash, TileLog, TransparencyLog, checkpoint_history_path,
    commit_leaf_hash, verify_consistency, verify_inclusion,
};
