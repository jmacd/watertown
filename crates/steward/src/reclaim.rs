// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Reclamation: the second half of version collapse.
//!
//! Collapse merges a window of series versions into one run row but leaves the
//! superseded rows in the table.  Those rows are already invisible to every
//! reader (they go through [`tlogfs::schema::live_series_entries`]), yet each
//! one still *references* its `_large_files` blob, so nothing is reclaimed
//! until the rows themselves are deleted.  Reclamation therefore runs in two
//! ordered steps:
//!
//! 1. Delete superseded series rows from the data Delta table.
//! 2. Mark-sweep `_large_files`, deleting every blob whose blake3 no longer
//!    appears in any remaining row.
//!
//! The order matters and cannot be inverted: `pond fsck`'s content pass reads
//! *every* row and requires each large-file blob to exist, so sweeping a blob
//! while its row survives turns a healthy pond into a failing one.
//!
//! Deleting superseded rows is Merkle-neutral: steward's content fold already
//! prunes them, so a pond's `root_tree_hash` is unchanged by reclamation and
//! mirrors -- which never received these rows -- stay converged.  That claim is
//! *checked*, not assumed: the content root is snapshotted before and after the
//! deletes and asserted identical, and the check runs BEFORE the blob sweep.
//! The ordering is the point -- deleted rows are still recoverable from Delta
//! history or a mirror, but a swept blob is gone.  Verifying while recovery is
//! still possible turns a silent, unrecoverable data loss into a loud, fixable
//! one.
//!
//! Reclamation is deliberately *not* a separate command or flag.  It is what
//! makes collapse actually free space, so it runs as part of the same
//! `pond maintain` pass.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use datafusion::prelude::SessionContext;
use deltalake::DeltaTable;
use deltalake::kernel::transaction::CommitProperties;
use log::debug;

use std::sync::Arc;
use tlogfs::schema::{CollapseRange, live_series_versions};

use crate::StewardError;

/// Series rows are grouped by this key before supersession is evaluated.
type SeriesKey = (String, String, String);

/// What a reclamation pass freed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimStats {
    /// Superseded series rows deleted from the data table.
    pub rows_deleted: usize,
    /// `_large_files` blobs deleted because no row referenced them.
    pub blobs_removed: usize,
    /// Bytes reclaimed from those blobs.
    pub bytes_freed: u64,
}

impl ReclaimStats {
    /// True when the pass changed nothing, so callers can stay quiet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows_deleted == 0 && self.blobs_removed == 0
    }
}

impl std::fmt::Display for ReclaimStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "reclaim: {} superseded row(s) deleted, {} blob(s) freed ({} bytes)",
            self.rows_deleted, self.blobs_removed, self.bytes_freed
        )
    }
}

/// The projection reclamation needs: enough to evaluate supersession, and the
/// identity columns needed to name a row in a delete predicate.
#[derive(serde::Deserialize)]
struct SeriesRow {
    pond_id: String,
    part_id: String,
    node_id: String,
    version: i64,
    collapsed_from: Option<i64>,
    collapsed_through: Option<i64>,
}

/// Every row of a physical series, projected to the columns above.
///
/// Both physical series kinds are collapsible and so both can hold superseded
/// rows; no other entry type has a supersession relation at all.
const SERIES_ROWS_SQL: &str = "SELECT pond_id, part_id, node_id, version, collapsed_from, \
     collapsed_through FROM reclaim_scan \
     WHERE file_type IN ('file:physical:series', 'table:physical:series')";

/// Hashes still referenced by *some* row, across every pond_id.
const REFERENCED_SQL: &str = "SELECT DISTINCT blake3 FROM reclaim_scan WHERE blake3 IS NOT NULL";

#[derive(serde::Deserialize)]
struct ReferencedHash {
    blake3: Option<String>,
}

/// Cap on a single delete predicate's length.  Superseded versions are named
/// explicitly (a range predicate would be wrong -- a run's own version can fall
/// inside a *later* run's range), so a first pass over a pond with years of
/// accumulated debt can produce a very long list.  Deletes are chunked instead
/// of building one enormous expression.
const PREDICATE_BUDGET: usize = 60_000;

/// Delete superseded series rows, then sweep unreferenced `_large_files`.
///
/// `pond_path` is the pond root (the parent of `_large_files`).  Returns the
/// new [`DeltaTable`] handle, since each delete produces a fresh table state.
///
/// # Safety
///
/// The caller **must** hold the pond write lock for the whole call.  The sweep
/// deletes any blob no committed row references, which is indistinguishable
/// from a blob a concurrent transaction has written but not yet committed.
///
/// # Errors
///
/// Returns an error if the data table cannot be scanned, a delete fails, or
/// `_large_files` cannot be traversed.
pub async fn reclaim_superseded(
    table: DeltaTable,
    pond_path: &Path,
    local_pond_id: &str,
    app_metadata: HashMap<String, serde_json::Value>,
) -> Result<(DeltaTable, ReclaimStats), StewardError> {
    let mut stats = ReclaimStats::default();

    let dead = find_superseded(&table).await?;
    let mut table = table;
    if !dead.is_empty() {
        // Snapshot the content root BEFORE deleting.  Reclamation must be
        // content-preserving; this is what turns that from a comment into a
        // checked invariant.
        let pre = crate::content_tree::compute_content_tree_for_table(table.clone(), local_pond_id)
            .await
            .map_err(|e| {
                StewardError::ControlTable(format!("reclaim: pre-delete content snapshot: {e}"))
            })?
            .root_tree_hash;

        table = delete_rows(table, &dead, app_metadata, &mut stats).await?;

        let post =
            crate::content_tree::compute_content_tree_for_table(table.clone(), local_pond_id)
                .await
                .map_err(|e| {
                    StewardError::ControlTable(format!(
                        "reclaim: post-delete content snapshot: {e}"
                    ))
                })?
                .root_tree_hash;

        // Return BEFORE sweeping.  A wrong delete has removed rows that Delta
        // history still holds and that a mirror can restore; sweeping their
        // blobs is what would make the loss permanent.
        if pre != post {
            return Err(StewardError::ControlTable(format!(
                "reclaim altered content root: pre={} post={} -- {} superseded row(s) were \
                 deleted that a reader could still see, so supersession was computed wrong. \
                 The `_large_files` sweep was SKIPPED, so no content is lost yet: recover the \
                 rows from the data table's Delta history or re-pull from a mirror before \
                 running maintain again.",
                pre.to_hex(),
                post.to_hex(),
                stats.rows_deleted,
            )));
        }
    }

    // Only now that the rows are gone can their blobs be unreferenced.
    let referenced = referenced_hashes(&table).await?;
    let swept = tlogfs::large_files::sweep_unreferenced(pond_path, &referenced).await?;
    stats.blobs_removed = swept.removed;
    stats.bytes_freed = swept.bytes_freed;

    if !stats.is_empty() {
        debug!("[MAINTAIN] {stats}");
    }
    Ok((table, stats))
}

/// Register `table` as `reclaim_scan` and run `sql`, deserializing the rows.
async fn query_rows<T: serde::de::DeserializeOwned>(
    table: &DeltaTable,
    sql: &str,
) -> Result<Vec<T>, StewardError> {
    let ctx = SessionContext::new();
    let _previous = ctx
        .register_table("reclaim_scan", Arc::new(table.clone()))
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
    let batches = ctx
        .sql(sql)
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?
        .collect()
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;

    let mut rows = Vec::new();
    for batch in &batches {
        let decoded: Vec<T> = serde_arrow::from_record_batch(batch)
            .map_err(|e| StewardError::DeltaLake(format!("reclaim: decode scan: {e}")))?;
        rows.extend(decoded);
    }
    Ok(rows)
}

/// Group every series row by node and return the versions no reader can see.
///
/// Supersession is evaluated by [`live_series_versions`] -- the single
/// definition shared with the read path -- and the dead set is its complement.
/// It is never inferred from a watermark: a merged run carries a fresh highest
/// version while standing for content in the middle of the stream, so both
/// "everything below K" and "any version inside a run's range" would delete
/// live runs.
async fn find_superseded(table: &DeltaTable) -> Result<HashMap<SeriesKey, Vec<i64>>, StewardError> {
    let rows: Vec<SeriesRow> = query_rows(table, SERIES_ROWS_SQL).await?;

    let mut by_node: HashMap<SeriesKey, Vec<(i64, CollapseRange)>> = HashMap::new();
    for row in rows {
        by_node
            .entry((row.pond_id, row.part_id, row.node_id))
            .or_default()
            .push((
                row.version,
                CollapseRange::new(row.version, row.collapsed_from, row.collapsed_through),
            ));
    }

    let mut dead = HashMap::new();
    for (key, entries) in by_node {
        let live: HashSet<i64> = live_series_versions(&entries).into_iter().collect();
        if live.len() == entries.len() {
            continue; // nothing collapsed on this node
        }
        let victims: Vec<i64> = entries
            .iter()
            .map(|(version, _)| *version)
            .filter(|version| !live.contains(version))
            .collect();
        if !victims.is_empty() {
            let _previous = dead.insert(key, victims);
        }
    }
    Ok(dead)
}

/// Delete the named rows in as few Delta commits as the predicate budget allows.
async fn delete_rows(
    table: DeltaTable,
    dead: &HashMap<SeriesKey, Vec<i64>>,
    app_metadata: HashMap<String, serde_json::Value>,
    stats: &mut ReclaimStats,
) -> Result<DeltaTable, StewardError> {
    let mut table = table;
    let mut clauses: Vec<String> = Vec::new();
    let mut budget = 0usize;

    for ((pond_id, part_id, node_id), versions) in dead {
        for chunk in versions.chunks(256) {
            let list = chunk
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            let clause = format!(
                "(pond_id = '{pond_id}' AND part_id = '{part_id}' AND node_id = '{node_id}' \
                 AND version IN ({list}))"
            );
            budget += clause.len();
            clauses.push(clause);
            if budget >= PREDICATE_BUDGET {
                table = flush_delete(table, &mut clauses, &app_metadata, stats).await?;
                budget = 0;
            }
        }
    }
    flush_delete(table, &mut clauses, &app_metadata, stats).await
}

/// Commit one delete for the accumulated clauses, clearing them.
async fn flush_delete(
    table: DeltaTable,
    clauses: &mut Vec<String>,
    app_metadata: &HashMap<String, serde_json::Value>,
    stats: &mut ReclaimStats,
) -> Result<DeltaTable, StewardError> {
    if clauses.is_empty() {
        return Ok(table);
    }
    let predicate = clauses.join(" OR ");
    clauses.clear();

    let (new_table, metrics) = table
        .delete()
        .with_predicate(predicate)
        .with_commit_properties(CommitProperties::default().with_metadata(app_metadata.clone()))
        .await
        .map_err(|e| StewardError::DeltaLake(format!("reclaim: delete superseded rows: {e}")))?;
    stats.rows_deleted += metrics.num_deleted_rows;
    Ok(new_table)
}

/// Every blake3 still named by a row, across all ponds.
///
/// Blobs are content-addressed, so one file can back rows in many nodes and
/// many ponds (cross-pond imports mirror rows verbatim).  The referenced set
/// must therefore be global; a per-node view would sweep blobs still in use.
/// Inline rows carry a blake3 too, and including them is harmless -- it only
/// ever *retains* a blob.
async fn referenced_hashes(table: &DeltaTable) -> Result<HashSet<String>, StewardError> {
    let rows: Vec<ReferencedHash> = query_rows(table, REFERENCED_SQL).await?;
    Ok(rows.into_iter().filter_map(|r| r.blake3).collect())
}

#[cfg(test)]
mod tests {
    use crate::{Ship, get_data_path};
    use tempfile::tempdir;
    use tlogfs::{PondTxnMetadata, PondUserMetadata};
    use tokio::io::AsyncWriteExt;

    /// Deleting superseded rows must not move the pond's `root_tree_hash`.
    ///
    /// This is the assumption reclamation rests on: mirrors never received the
    /// superseded rows, so a pond that deletes them must still fold to the same
    /// content root, or replication silently diverges.  It is also what makes
    /// the deletes safe -- if supersession were computed wrong, reclamation
    /// would remove rows a reader can still see and then sweep their blobs.
    ///
    /// The check must bracket the DELETES ALONE.  It cannot be observed through
    /// `Ship::collapse_versions`, because collapse commits its merged rows as an
    /// ordinary write and legitimately does move the content root; so this test
    /// collapses and reclaims as separate steps, exactly as `Ship::reclaim`
    /// brackets them internally.
    #[tokio::test]
    async fn deleting_superseded_rows_preserves_the_content_root() {
        let tmp = tempdir().expect("tempdir");
        let pond = tmp.path().join("pond");
        let mut ship = Ship::create_pond(pond.clone(), "reclaim-merkle")
            .await
            .expect("create pond");

        let meta = PondUserMetadata::new(vec!["test".into(), "reclaim-merkle".into()]);
        for i in 0..12u64 {
            let bytes = filler(i);
            ship.write_transaction(&meta, async move |fs| {
                let root = fs.root().await?;
                let mut w = root
                    .async_writer_path_with_type(
                        "/events.series",
                        tinyfs::EntryType::FilePhysicalSeries,
                    )
                    .await?;
                w.write_all(&bytes)
                    .await
                    .map_err(|e| crate::StewardError::Aborted(format!("write: {e}")))?;
                w.shutdown()
                    .await
                    .map_err(|e| crate::StewardError::Aborted(format!("close: {e}")))?;
                Ok(())
            })
            .await
            .expect("append version");
        }

        // Collapse ONLY -- no reclaim.  The pond now carries superseded rows,
        // which is precisely the state reclamation acts on.
        let tx = ship.begin_write(&meta).await.expect("begin write");
        {
            let state = tx.state().expect("state");
            let candidates = state
                .list_collapsible_series(1)
                .await
                .expect("list collapsible");
            assert!(!candidates.is_empty(), "series should be collapsible");
            for id in candidates {
                let stats = state.collapse_file_series(id, 1).await.expect("collapse");
                assert!(stats.collapsed, "collapse should have merged a window");
            }
        }
        let _ = tx.commit().await.expect("commit collapse");

        let pond_id = ship.control_table().pond_id_uuid().to_string();
        let table = ship.data_persistence().table().clone();

        let before = crate::content_tree::compute_content_tree_for_table(table.clone(), &pond_id)
            .await
            .expect("content root before reclaim")
            .root_tree_hash;

        let mut txn_meta = PondTxnMetadata::new(ship.last_write_seq(), meta.clone());
        txn_meta.pond_id = pond_id.clone();
        let (new_table, stats) = super::reclaim_superseded(
            table,
            &get_data_path(&pond),
            &pond_id,
            txn_meta.to_delta_maintenance_metadata(),
        )
        .await
        .expect("reclaim must not trip its own content invariant");

        assert!(
            stats.rows_deleted > 0,
            "the collapse must have left superseded rows for this to prove anything"
        );

        let after = crate::content_tree::compute_content_tree_for_table(new_table, &pond_id)
            .await
            .expect("content root after reclaim")
            .root_tree_hash;

        assert_eq!(
            before.to_hex(),
            after.to_hex(),
            "deleting {} superseded row(s) moved the content root; a mirror that never \
             received them would now diverge",
            stats.rows_deleted,
        );
    }

    /// Distinct, incompressible bytes per version, so each lands as its own
    /// `_large_files` blob instead of deduplicating or inlining.
    pub(super) fn filler(seed: u64) -> Vec<u8> {
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        (0..96 * 1024)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 24) as u8
            })
            .collect()
    }
}
