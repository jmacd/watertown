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
pub mod pack_keys;
mod recovery_recipe;
mod s3_registration;
pub mod schema;
mod store;
pub mod testing;
pub mod tlog;

pub use azure_registration::{AZURE_SCHEMES, register_azure_handlers};
pub use s3_registration::register_s3_handlers;

pub use content::{
    CAPSULE_FORMAT_V1, CAPSULE_FORMAT_V2, CapsuleEntry, CapsuleLeaf, CapsuleManifest, CapsuleNode,
    CapsuleObject, CapsulePayloadKind, CapsuleSource, CapsuleVerifyReport, Commit,
    IncrementalFileLeafHasher, IncrementalTableLeafHasher, LEGACY_CAPSULE_FORMAT,
    LEGACY_CAPSULE_FORMAT_V1, LEGACY_CAPSULE_FORMAT_V2, LEGACY_NATIVE_FORMAT_DP_COMMIT_3,
    LegacyCapsuleDynamicMetadata, LegacyCapsuleEntry, LegacyCapsuleManifest, LegacyCapsuleNode,
    LegacyCapsuleObject, LegacyCapsulePayloadKind, LegacyCapsuleSource, LegacyCapsuleVerifyReport,
    LegacyCapsuleVersion, ManifestEntry, NodeMerkle, ObjectHash, Provenance, TreeEntry,
    VersionMeta, canonicalize_schema, capsule_leaf_hash, capsule_manifest_bytes, capsule_root,
    capsule_series_root, decode_capsule_manifest, decode_legacy_capsule_manifest, decode_manifest,
    decode_recipe, decode_series, decode_tree, encode_canonical_attributes,
    encode_canonical_batch_rows, encode_canonical_schema, encode_capsule_attributes,
    file_leaf_hash, legacy_capsule_manifest_bytes, legacy_capsule_root, node_merkle_rebuild_root,
    read_capsule_manifest, read_legacy_capsule_manifest, schema_fingerprint, series_hash,
    table_leaf_hash, tree_hash, verify_capsule_directory, verify_capsule_payload_directory,
    verify_incremental_capsule_payload_directory, verify_legacy_capsule_directory,
    verify_legacy_capsule_payload_directory,
};
pub use content_remote::{ContentRemote, RecoveryRecipePublishOutcome};
pub use error::{Result, StoreError};
pub use metered_store::{
    MeterBinding, MeteredStore, Observation, RemoteKey, StorageMeter, bind_meter, observed_under,
};
pub use recovery_recipe::{
    recovery_recipe_dp_commit_3, recovery_recipe_dp_commit_3_hash,
    recovery_recipe_legacy_migration, recovery_recipe_legacy_migration_hash,
    recovery_recipe_legacy_migration_v2, recovery_recipe_legacy_migration_v2_hash,
    recovery_recipe_watertown_commit_v1, recovery_recipe_watertown_commit_v1_hash,
};
pub use store::{AddPath, CompactMetrics, Op, RemovePath, Store, raw_object_store_at_url};
pub use tlog::{
    Checkpoint, CheckpointError, LogHash, TileLog, TransparencyLog, checkpoint_history_path,
    commit_leaf_hash, verify_consistency, verify_inclusion,
};
