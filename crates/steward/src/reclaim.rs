// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Reclamation for the reserved native-v2 manifest-index node.
//!
//! User `FilePhysicalSeries` and `TablePhysicalSeries` rows are append-only in
//! native-v2. They are never collapsed and therefore are never candidates for
//! row deletion here. The sole production writer of a collapsed row is the
//! internal `.pond-node-index`, whose fixed-size root pointer replaces its
//! previous pointer on each content commit. Those rows are excluded from every
//! content fold and can be deleted without inspecting user series or scanning
//! `_large_files`.
//!
//! This intentionally does not advertise or perform user-series blob
//! reclamation. Pack maintenance is additive physical repacking under
//! `_packs/`; the original Oplog rows and their payloads remain canonical.

use std::collections::{HashMap, HashSet};

use datafusion::prelude::SessionContext;
use deltalake::DeltaTable;
use deltalake::kernel::transaction::CommitProperties;
use log::debug;

use std::sync::Arc;
use tlogfs::schema::{CollapseRange, live_series_versions};

use crate::StewardError;

/// Series rows are grouped by this key before supersession is evaluated.
type SeriesKey = (String, String, String);

/// What one reserved-index reclamation pass removed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimStats {
    /// Superseded `.pond-node-index` rows deleted from the data table.
    pub rows_deleted: usize,
}

impl ReclaimStats {
    /// True when the pass changed nothing, so callers can stay quiet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows_deleted == 0
    }
}

impl std::fmt::Display for ReclaimStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "reclaim: {} superseded internal index row(s) deleted",
            self.rows_deleted
        )
    }
}

/// The projection reclamation needs: enough to evaluate supersession, and the
/// identity columns needed to name a row in a delete predicate.
#[derive(Debug, serde::Deserialize)]
struct SeriesRow {
    pond_id: String,
    part_id: String,
    node_id: String,
    version: i64,
    collapsed_from: Option<i64>,
    collapsed_through: Option<i64>,
}

/// Every reserved manifest-index row, across the local pond and imported
/// foreign ponds.
///
/// The exact node and root-partition identities are the safety boundary. No
/// user-created node can acquire [`tinyfs::INDEX_NODE_UUID`], and the index is
/// excluded from every pond's content manifest.
const INDEX_ROWS_SQL: &str = "SELECT pond_id, part_id, node_id, version, collapsed_from, \
     collapsed_through FROM reclaim_scan \
     WHERE file_type = 'file:physical:series' \
     AND node_id = '00000000-0000-7700-8000-000000000000' \
     AND part_id = '00000000-0000-7100-8000-000000000000'";

/// Cap on a single delete predicate's length.  Superseded versions are named
/// explicitly (a range predicate would be wrong -- a run's own version can fall
/// inside a *later* run's range), so a first pass over a pond with years of
/// accumulated debt can produce a very long list.  Deletes are chunked instead
/// of building one enormous expression.
const PREDICATE_BUDGET: usize = 60_000;

/// Delete superseded reserved manifest-index rows.
///
/// Returns the new [`DeltaTable`] handle, since each delete produces a fresh
/// table state.
///
/// # Errors
///
/// Returns an error if the data table cannot be scanned or a delete fails.
pub async fn reclaim_internal_index_rows(
    table: DeltaTable,
    app_metadata: HashMap<String, serde_json::Value>,
) -> Result<(DeltaTable, ReclaimStats), StewardError> {
    let mut stats = ReclaimStats::default();

    let dead = find_superseded(&table).await?;
    let mut table = table;
    if !dead.is_empty() {
        table = delete_rows(table, &dead, app_metadata, &mut stats).await?;
    }

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
    let rows: Vec<SeriesRow> = query_rows(table, INDEX_ROWS_SQL).await?;

    let mut by_node: HashMap<SeriesKey, Vec<(i64, CollapseRange)>> = HashMap::new();
    for row in rows {
        if row.node_id != tinyfs::INDEX_NODE_UUID || row.part_id != tinyfs::ROOT_UUID {
            return Err(StewardError::ControlTable(format!(
                "reclaim query returned non-index row pond={} part={} node={}",
                row.pond_id, row.part_id, row.node_id
            )));
        }
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

#[cfg(test)]
mod tests {
    use crate::Ship;
    use tempfile::tempdir;
    use tlogfs::PondUserMetadata;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn deleting_only_reserved_index_rows_preserves_user_series() {
        let tmp = tempdir().expect("tempdir");
        let pond = tmp.path().join("pond");
        let mut ship = Ship::create_pond(pond.clone(), "reclaim-merkle")
            .await
            .expect("create pond");

        let meta = PondUserMetadata::new(vec!["test".into(), "reclaim-merkle".into()]);
        let mut expected = Vec::new();
        for index in 0..12u8 {
            let bytes = vec![b'a' + index; 4096];
            expected.extend_from_slice(&bytes);
            ship.write_transaction(&meta, async move |fs| {
                let root = fs.root().await?;
                let mut w = root
                    .async_writer_path_with_type(
                        "/events.series",
                        tinyfs::EntryType::FilePhysicalSeries,
                    )
                    .await?;
                w.write_all(&bytes).await?;
                w.shutdown().await?;
                Ok(())
            })
            .await
            .expect("append version");
        }

        let before = crate::content_tree::compute_content_tree(&ship)
            .await
            .expect("content root before reclaim")
            .root_tree_hash;

        let first = ship
            .collapse_versions(1000)
            .await
            .expect("reserved-index reclaim");
        assert_eq!(
            first.reclaimed.rows_deleted, 11,
            "twelve commits leave eleven superseded internal index pointers"
        );
        let after = crate::content_tree::compute_content_tree(&ship)
            .await
            .expect("content root after reclaim")
            .root_tree_hash;
        assert_eq!(before, after);

        let tx = ship
            .begin_read(&PondUserMetadata::new(vec!["read".into()]))
            .await
            .expect("begin read");
        let root = tx.root().await.expect("root");
        assert_eq!(
            root.read_file_path_to_vec("/events.series")
                .await
                .expect("read series"),
            expected
        );
        _ = tx.commit().await.expect("close read");

        let second = ship
            .collapse_versions(1000)
            .await
            .expect("idempotent reserved-index reclaim");
        assert_eq!(second.reclaimed.rows_deleted, 0);
    }
}
