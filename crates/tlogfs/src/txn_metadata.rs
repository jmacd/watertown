// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Transaction metadata that MUST be provided for all commits
//!
//! This module defines type-safe transaction metadata that ensures every
//! Delta Lake commit includes the original command information needed for
//! replication and backup/restore operations.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid7::Uuid;

#[derive(Debug, Clone, Deserialize, Serialize, Eq, PartialEq)]
pub struct PondUserMetadata {
    /// Unique transaction ID (UUID v7) for Steward recovery/debugging
    /// Links this transaction to control table records
    pub txn_id: Uuid,

    /// Original CLI arguments that created this transaction
    pub args: Vec<String>,
}

impl PondUserMetadata {
    #[must_use]
    pub fn new(args: Vec<String>) -> Self {
        Self {
            txn_id: uuid7::uuid7(),
            args,
        }
    }
}

/// Transaction metadata that MUST be included in every Delta Lake commit
#[derive(Debug, Clone, Deserialize, Serialize, Eq, PartialEq)]
pub struct PondTxnMetadata {
    /// The write sequence number.
    pub txn_seq: i64,

    /// User-level metadata.
    pub user: PondUserMetadata,

    /// Pond identity UUID string. Stamped by the persistence layer at commit time.
    /// Included in Delta commit metadata for quick inspection without scanning rows.
    #[serde(default)]
    pub pond_id: String,
}

impl PondTxnMetadata {
    #[must_use]
    pub fn new(txn_seq: i64, user: PondUserMetadata) -> Self {
        Self {
            txn_seq,
            user,
            pond_id: String::new(),
        }
    }

    /// Convert to Delta Lake commit metadata format, injecting txn_seq
    ///
    /// The `txn_seq` parameter comes from `begin()`, ensuring sequence is
    /// specified once at transaction start, not at commit.
    ///
    /// Returns a HashMap ready to be passed to Delta Lake's commit operation
    #[must_use]
    pub fn to_delta_metadata(&self) -> HashMap<String, serde_json::Value> {
        let pond_txn = serde_json::to_value(self).expect("Failed to serialize PondTxnMetadata");

        HashMap::from([("pond_txn".to_string(), pond_txn)])
    }

    /// Convert to Delta commit metadata for a **maintenance** commit.
    ///
    /// Maintenance commits (reclaim's row deletes) are content-preserving and
    /// represent no transaction of their own: they are the tail of a
    /// transaction that has already committed and been recorded. Stamping them
    /// under a distinct key keeps them attributable for forensics while making
    /// them invisible to anything that reads transactions, which is what
    /// `pond_txn` means.
    ///
    /// This matters because [`Self::from_delta_metadata`] is how
    /// `reconstruct_txn_history` decides what a transaction *was*: a
    /// maintenance commit carrying `pond_txn` would be replayed as a second
    /// (duplicate) lifecycle record at a sequence that already has one. Seq
    /// recovery on open is unaffected -- it takes the per-pond MAX over all
    /// `pond_txn` commits, and the originating transaction's own commit still
    /// carries that seq.
    #[must_use]
    pub fn to_delta_maintenance_metadata(&self) -> HashMap<String, serde_json::Value> {
        let meta = serde_json::to_value(self).expect("Failed to serialize PondTxnMetadata");

        HashMap::from([("pond_maintenance".to_string(), meta)])
    }

    /// Extract from Delta Lake commit metadata (for reading backups)
    ///
    /// Returns None if pond_txn field is missing or malformed.
    /// Note: txn_seq is stored in metadata but not returned (caller already knows it from context)
    #[must_use]
    pub fn from_delta_metadata(metadata: &HashMap<String, serde_json::Value>) -> Option<Self> {
        let pond_txn = metadata.get("pond_txn")?;
        let delta_metadata: Self = serde_json::from_value(pond_txn.clone()).ok()?;
        Some(delta_metadata)
    }

    /// Extract txn_seq from Delta Lake commit metadata
    ///
    /// This is used when reopening an existing pond to determine the last transaction sequence.
    #[must_use]
    pub fn extract_txn_seq(metadata: &HashMap<String, serde_json::Value>) -> Option<i64> {
        let pond_txn = metadata.get("pond_txn")?;
        let delta_metadata: PondTxnMetadata = serde_json::from_value(pond_txn.clone()).ok()?;
        Some(delta_metadata.txn_seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_roundtrip() {
        let original = PondTxnMetadata::new(
            4,
            PondUserMetadata::new(vec![
                "mknod".to_string(),
                "hydrovu".to_string(),
                "/etc/hydrovu".to_string(),
            ]),
        );

        let delta_metadata = original.to_delta_metadata();
        let recovered =
            PondTxnMetadata::from_delta_metadata(&delta_metadata).expect("Should recover metadata");

        assert_eq!(original, recovered);
    }

    #[test]
    fn test_metadata_format() {
        let metadata = PondTxnMetadata::new(
            5,
            PondUserMetadata::new(vec!["mkdir".to_string(), "/etc".to_string()]),
        );

        let delta_metadata = metadata.to_delta_metadata();

        assert!(delta_metadata.contains_key("pond_txn"));

        let pond_txn = &delta_metadata["pond_txn"];
        // txn_id is now under 'user' after refactoring
        assert!(pond_txn["user"]["txn_id"].is_string());
        assert_eq!(pond_txn["txn_seq"], 5);
    }
}
