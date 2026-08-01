// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `pond fsck` -- local, offline filesystem-check.
//!
//! Computes a single deterministic **root hash** that commits to the live
//! content of every `pond_id` in the data table (including cross-pond
//! imports).  Post-Decision-D9 the structural check is the pond's own
//! content tree -- the same lineage-independent `tree_hash` that drives
//! replication (Decision D9, step 5b: "checksum subsumption").  There is no
//! longer a separate `row_leaf_digest` partition Merkle:
//!
//! ```text
//! root                                  <- tree_hash over per-pond roots
//! |- pond_id -> root_tree_hash          (that pond's content tree)
//! |  |- directory (part_id) -> tree_hash
//! |  |  |- child_hash of each entry (recursive)
//! |  |  +- ...
//! |  +- ...
//! +- ...
//! ```
//!
//! A directory *is* a partition, and its `tree_hash` commits recursively to
//! everything the partition holds, so two replicas are byte-identical in
//! content iff their roots match -- the same guarantee remote verification
//! uses (see [`crate::content_tree::compute_content_tree`]), reachable with a
//! single hex string.  The reserved INDEX/LOG nodes are fold-excluded, so the
//! structural root is a pure content check independent of the disposable
//! caches.
//!
//! ## Content-checksum pass
//!
//! The row Merkle commits to each file's *recorded* `blake3`, but not to
//! the bytes it points at.  When [`FsckOptions::verify_content`] is set
//! (the default), fsck additionally:
//!
//! - re-hashes inline file content via [`OplogEntry::verified_content`], and
//! - re-reads every external `_large_files/blake3=<hash>.parquet` blob
//!   through [`ChunkedReader`], which validates the blob's embedded hash
//!   chain; for non-series version/table entries it also confirms the
//!   reassembled bytes hash to the recorded `blake3`.
//!
//! Content failures are collected into [`FsckReport::errors`] rather than
//! aborting, so a single corrupt blob does not hide the rest of the report.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::execution::context::SessionContext;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use sync_store::{ObjectHash, TreeEntry, tree_hash};
use tinyfs::EntryType;
use tlogfs::large_files::find_large_file_path;
use tlogfs::schema::OplogEntry;
use utilities::chunked_files::ChunkedReader;

use crate::content_tree::build_content_tree_for_table;
use crate::{Ship, StewardError, get_data_path};

/// Options controlling an [`fsck`] run.
#[derive(Debug, Clone, Copy)]
pub struct FsckOptions {
    /// Re-read and re-hash every file's content (inline and external
    /// large-file blobs) to confirm the bytes match their recorded
    /// `blake3`.  When `false` (`--quick`), only the structural row
    /// Merkle / root checksum is computed.
    pub verify_content: bool,
}

impl Default for FsckOptions {
    fn default() -> Self {
        Self {
            verify_content: true,
        }
    }
}

/// The content `tree_hash` of one directory `(pond_id, part_id)`.
///
/// A partition is a directory; its `tree_hash` commits recursively to every
/// entry it holds, so this subsumes the retired per-partition row checksum
/// (Decision D9, step 5b).
#[derive(Debug, Clone)]
pub struct PartitionDigest {
    /// Owning pond UUID (the pond that created the rows).
    pub pond_id: String,
    /// Partition UUID -- the directory node's own `node_id`.
    pub part_id: String,
    /// Number of live child entries in this directory.
    pub rows: usize,
    /// The directory's recursive content `tree_hash`.
    pub tree_hash: ObjectHash,
}

/// A single content-verification failure found by [`fsck`].
#[derive(Debug, Clone)]
pub struct FsckError {
    /// Owning pond UUID of the offending row.
    pub pond_id: String,
    /// Partition UUID of the offending row.
    pub part_id: String,
    /// Node UUID of the offending row.
    pub node_id: String,
    /// Version of the offending row.
    pub version: i64,
    /// Human-readable description of the failure.
    pub detail: String,
}

/// Result of an [`fsck`] run.
#[derive(Debug, Clone)]
pub struct FsckReport {
    /// Single root hash committing to the live content of every pond.
    /// Two replicas are content-identical iff their roots match (and both
    /// pass content verification).
    pub root: ObjectHash,
    /// Per-directory content digests, sorted by `(pond_id, part_id)`.
    pub partitions: Vec<PartitionDigest>,
    /// Total live child entries folded into the root.
    pub rows_checked: usize,
    /// Number of external large-file blobs re-read and validated.
    pub blobs_checked: usize,
    /// Number of inline-content rows re-hashed.
    pub inline_checked: usize,
    /// Content-verification failures (empty on a clean pond).
    pub errors: Vec<FsckError>,
}

impl FsckReport {
    /// `true` iff no content-verification errors were found.
    #[must_use]
    pub fn ok(&self) -> bool {
        self.errors.is_empty()
    }

    /// Hex-encoded root hash.
    #[must_use]
    pub fn root_hex(&self) -> String {
        self.root.to_hex()
    }
}

/// Run a filesystem check over `ship`'s data table.
///
/// Builds the per-directory content `tree_hash`es and the single content
/// root (over every `pond_id`), and -- unless [`FsckOptions::verify_content`]
/// is `false` -- re-validates every file's content against its recorded
/// `blake3`.  Returns an error only for failures that prevent the check from
/// running at all (e.g. the data table cannot be read); per-file content
/// mismatches are reported in [`FsckReport::errors`].
pub async fn fsck(ship: &Ship, opts: FsckOptions) -> Result<FsckReport, StewardError> {
    let table = ship.data_persistence().table().clone();
    let data_dir = get_data_path(ship.pond_path());

    let ctx = SessionContext::new();
    let _previous = ctx
        .register_table("fsck_live", Arc::new(table.clone()))
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;

    // The set of ponds whose live content this check commits to: the local
    // pond plus every cross-pond import mirrored row-for-row.  Each has its
    // own root directory row and content tree.
    let pond_batches = ctx
        .sql("SELECT DISTINCT pond_id FROM fsck_live")
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?
        .collect()
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
    let mut pond_ids: BTreeSet<String> = BTreeSet::new();
    for batch in &pond_batches {
        #[derive(serde::Deserialize)]
        struct PondRow {
            pond_id: String,
        }
        let rows: Vec<PondRow> = serde_arrow::from_record_batch(batch)
            .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
        for row in rows {
            let _ = pond_ids.insert(row.pond_id);
        }
    }

    // Structural check: per-pond content tree.  A directory is a partition,
    // and its recursive `tree_hash` (fold-excluding the reserved INDEX/LOG
    // nodes) subsumes the retired per-partition row checksum (step 5b).
    let mut partitions: Vec<PartitionDigest> = Vec::new();
    let mut pond_root_entries: Vec<TreeEntry> = Vec::with_capacity(pond_ids.len());
    let mut rows_checked = 0usize;

    for pond_id in &pond_ids {
        let index = build_content_tree_for_table(table.clone(), pond_id).await?;

        // Each physical directory's own `tree_hash`: the root's is
        // `root_tree_hash`; every other directory appears exactly once as a
        // child entry whose `child_hash` is its `tree_hash`.
        let mut dir_hash: BTreeMap<(String, String), ObjectHash> = BTreeMap::new();
        let _ = dir_hash.insert(index.root_key.clone(), index.root_tree_hash);
        for children in index.dirs.values() {
            for child in children {
                if let Some(key) = &child.child_dir_key {
                    let _ = dir_hash.insert(key.clone(), child.child_hash);
                }
            }
        }

        let mut dir_keys: Vec<&(String, String)> = index.dirs.keys().collect();
        dir_keys.sort();
        for key in dir_keys {
            let th = dir_hash.get(key).copied().ok_or_else(|| {
                StewardError::DeltaLake(format!(
                    "fsck: directory {}/{} has no computed tree_hash",
                    key.0, key.1
                ))
            })?;
            let rows = index.dirs.get(key).map(Vec::len).unwrap_or(0);
            rows_checked += rows;
            partitions.push(PartitionDigest {
                pond_id: key.0.clone(),
                part_id: key.1.clone(),
                rows,
                tree_hash: th,
            });
        }

        pond_root_entries.push(TreeEntry::bare(
            pond_id.clone(),
            EntryType::DirectoryPhysical,
            index.root_tree_hash,
        ));
    }

    let root = tree_hash(&pond_root_entries).map_err(StewardError::DeltaLake)?;

    // Content-verification pass: re-hash inline content and re-read every
    // external large-file blob.  Independent of the structural root above,
    // so a corrupt blob is reported without hiding the rest of the check.
    let mut errors: Vec<FsckError> = Vec::new();
    let mut blobs_checked = 0usize;
    let mut inline_checked = 0usize;

    if opts.verify_content {
        let batches = ctx
            .sql("SELECT * FROM fsck_live ORDER BY pond_id, part_id, node_id, version")
            .await
            .map_err(|e| StewardError::DeltaLake(e.to_string()))?
            .collect()
            .await
            .map_err(|e| StewardError::DeltaLake(e.to_string()))?;

        for batch in &batches {
            let rows: Vec<OplogEntry> = serde_arrow::from_record_batch(batch)
                .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
            for row in rows {
                if let Err(detail) =
                    verify_row_content(&row, &data_dir, &mut blobs_checked, &mut inline_checked)
                        .await
                {
                    errors.push(FsckError {
                        pond_id: row.pond_id.clone(),
                        part_id: row.part_id.to_string(),
                        node_id: row.node_id.to_string(),
                        version: row.version,
                        detail,
                    });
                }
            }
        }
    }

    Ok(FsckReport {
        root,
        partitions,
        rows_checked,
        blobs_checked,
        inline_checked,
        errors,
    })
}

/// Verify one row's content against its recorded `blake3`.  Returns
/// `Err(detail)` describing the first failure; `Ok(())` when the row
/// either has no content to check or verifies cleanly.
async fn verify_row_content(
    row: &OplogEntry,
    data_dir: &Path,
    blobs_checked: &mut usize,
    inline_checked: &mut usize,
) -> Result<(), String> {
    if row.is_large_file() {
        let blake3 = row
            .blake3
            .as_deref()
            .ok_or_else(|| "large-file row missing blake3".to_string())?;

        let path = find_large_file_path(data_dir, blake3)
            .await
            .map_err(|e| format!("locating blob for blake3={blake3}: {e}"))?
            .ok_or_else(|| format!("large-file blob missing for blake3={blake3}"))?;

        let bytes = tokio::fs::read(&path)
            .await
            .map_err(|e| format!("reading blob {}: {e}", path.display()))?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
            .map_err(|e| format!("opening blob parquet for blake3={blake3}: {e}"))?
            .build()
            .map_err(|e| format!("building blob reader for blake3={blake3}: {e}"))?;

        let mut blob_batches = Vec::new();
        for b in reader {
            blob_batches
                .push(b.map_err(|e| format!("reading blob batch for blake3={blake3}: {e}"))?);
        }

        // ChunkedReader validates the blob's embedded chunk-hash chain
        // and reassembles the content bytes.
        let mut sink: Vec<u8> = Vec::new();
        ChunkedReader::new(blob_batches)
            .read_to_writer(&mut sink)
            .await
            .map_err(|e| format!("blob integrity check failed for blake3={blake3}: {e}"))?;
        *blobs_checked += 1;

        // For non-series entries the recorded blake3 is blake3(content),
        // so tie the reassembled bytes back to the row's recorded hash.
        // FilePhysicalSeries stores a cumulative bao root that cannot be
        // compared to a single version's content, so rely on the blob's
        // internal validation above.
        if row.file_type != EntryType::FilePhysicalSeries {
            let actual = blake3::hash(&sink).to_hex().to_string();
            if actual != blake3 {
                return Err(format!(
                    "blob content hash {actual} does not match recorded blake3 {blake3}"
                ));
            }
        }
        Ok(())
    } else if row.content.is_some() && row.file_type.is_file() {
        let _verified = row.verified_content().map_err(|e| e.to_string())?;
        *inline_checked += 1;
        Ok(())
    } else {
        Ok(())
    }
}
