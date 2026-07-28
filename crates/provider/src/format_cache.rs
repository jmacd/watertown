// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Format provider cache -- per-version Parquet cache for format providers.
//!
//! Format providers (oteljson, csv, excelhtml) parse raw file bytes into Arrow
//! RecordBatches on every read.  This module caches the parsed output of each
//! individual file version as a Parquet file on disk in `{POND}/cache/`.
//!
//! Key properties:
//! - Per-version caching: each version is independently immutable (blake3 hash),
//!   so there is nothing to invalidate.
//! - Incremental: only uncached versions are parsed; cached versions are free.
//! - Throwaway: `rm -rf {POND}/cache/` is always safe.
//! - Returns `ListingTable` over cached Parquet files for full DataFusion pushdown.

use arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::execution::context::SessionContext;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tinyfs::FileVersionInfo;

use crate::version_cache::{LiveVersions, SidecarDir, SidecarNaming};

/// Result type for format cache operations
type Result<T> = std::result::Result<T, crate::error::Error>;

/// Directory for a file's cached format conversions.
///
/// Returns `{cache_dir}/{scheme}_{node_id}/`
#[must_use]
pub fn cache_node_dir(cache_dir: &Path, scheme: &str, node_id: &tinyfs::NodeID) -> PathBuf {
    cache_dir.join(format!("{}_{}", scheme, node_id))
}

/// This node's cached versions, as a [`SidecarDir`].
///
/// [`SidecarNaming::NodeOwned`]: the directory holds exactly one node's
/// versions, so every Parquet in it belongs to that node.
#[must_use]
pub fn node_sidecars(cache_dir: &Path, scheme: &str, node_id: &tinyfs::NodeID) -> SidecarDir {
    SidecarDir::new(
        cache_node_dir(cache_dir, scheme, node_id),
        SidecarNaming::NodeOwned,
    )
}

/// Path for a single version's cached Parquet file.
///
/// Returns `{cache_dir}/{scheme}_{node_id}/v{version}_{blake3}.parquet`
#[must_use]
pub fn cache_version_path(
    cache_dir: &Path,
    scheme: &str,
    node_id: &tinyfs::NodeID,
    version: &FileVersionInfo,
) -> PathBuf {
    node_sidecars(cache_dir, scheme, node_id).sidecar_path(node_id, version)
}

/// Check which of a node's live versions are missing from the cache.
///
/// Adds only; never deletes. Use it when `versions` may be a SUBSET of the
/// node's live versions -- notably the dynamic-file path, which synthesizes a
/// single version from metadata because such nodes have no persistence records.
/// Reconciling against a subset would delete the sidecars of every version not
/// in it. Callers holding the full live set should use
/// [`reconcile_cached_versions`] instead, so the cache stays a projection of
/// what is live rather than growing forever.
#[must_use]
pub fn find_uncached_versions(
    cache_dir: &Path,
    scheme: &str,
    node_id: &tinyfs::NodeID,
    versions: &[FileVersionInfo],
) -> Vec<FileVersionInfo> {
    let live = LiveVersions::from_persistence(*node_id, versions.to_vec());
    node_sidecars(cache_dir, scheme, node_id).missing(&live)
}

/// Make a node's cache directory a projection of its live versions: delete the
/// sidecars of versions that are no longer live, and report which live versions
/// still need writing.
///
/// This is the collection step the format cache never had. Every other property
/// in this module's header held -- per-version files are immutable, so there was
/// nothing to INVALIDATE -- but nothing ever removed them, so the directory grew
/// by one Parquet for every version ever written and was never bounded. Version
/// collapse makes that worse than mere disk cost: it replaces a run of versions
/// with one merged version carrying the same content, so the superseded
/// sidecars are duplicates of data the merged one already holds.
///
/// Reads are what keep it bounded, because reads are what happen. Deleting is
/// safe: a sidecar is derived data, regenerable from the source version.
///
/// `versions` MUST be the node's complete live set, as returned by
/// `list_file_versions`. Passing a subset deletes the rest.
pub fn reconcile_cached_versions(
    cache_dir: &Path,
    scheme: &str,
    node_id: &tinyfs::NodeID,
    versions: &[FileVersionInfo],
) -> Result<Vec<FileVersionInfo>> {
    let live = LiveVersions::from_persistence(*node_id, versions.to_vec());
    let rec = node_sidecars(cache_dir, scheme, node_id).reconcile(&live)?;
    if !rec.removed.is_empty() {
        log::debug!(
            "[SWEEP] format cache: removed {} superseded sidecar(s) for {}_{}",
            rec.removed.len(),
            scheme,
            node_id
        );
    }
    Ok(rec.missing)
}

/// Write a single version's format output to cache as Parquet.
///
/// Streams batches from the format provider to disk without collecting them,
/// and writes atomically so a crash cannot leave a truncated file that a later
/// run would mistake for a complete cached version.
pub async fn cache_write_version(
    cache_dir: &Path,
    scheme: &str,
    node_id: &tinyfs::NodeID,
    version: &FileVersionInfo,
    schema: SchemaRef,
    stream: crate::version_cache::BatchStream,
) -> Result<PathBuf> {
    node_sidecars(cache_dir, scheme, node_id)
        .write_sidecar(node_id, version, schema, stream)
        .await
}

/// Extract a series version's recorded `max_event_time` (epoch µs) from its
/// extended metadata, if present. Non-series versions and versions written
/// without event-time bounds return `None` (and are always retained by an
/// event-time bound -- a missing bound must never drop data).
#[must_use]
pub fn version_max_event_time(v: &FileVersionInfo) -> Option<i64> {
    v.extended_metadata
        .as_ref()?
        .get("max_event_time")?
        .parse::<i64>()
        .ok()
}

/// Build a `ListingTable` over only the cached version Parquets retained by
/// `bounds` (per-version event-time / watermark prune), so a bounded reader
/// scans only the hot files instead of all cached history.
///
/// When `bounds` is [`tinyfs::SeriesReadBounds::NONE`] this covers every live
/// version. When the bounds retain no version, an empty table over the merged
/// cache schema is returned (0 rows), never a silent full scan.
///
/// `versions` must be the node's LIVE versions. The scan names their files
/// explicitly rather than listing the cache directory, which accumulates a
/// Parquet per version ever written: version collapse replaces a run of
/// versions with one merged version carrying the same content, so a listing
/// would return both the merged version and the superseded ones it stands for,
/// silently double-counting every row.
pub async fn listing_table_from_cache_bounded(
    cache_dir: &Path,
    scheme: &str,
    node_id: &tinyfs::NodeID,
    versions: &[FileVersionInfo],
    bounds: &tinyfs::SeriesReadBounds,
    _ctx: &SessionContext,
) -> Result<Arc<dyn TableProvider>> {
    let live = LiveVersions::from_persistence(*node_id, versions.to_vec());
    node_sidecars(cache_dir, scheme, node_id)
        .cached_set(&live, |v| {
            bounds.retains(version_max_event_time(v), v.version as i64)
        })
        .table_provider()
        .await
}

/// The cached Parquets of the live versions of EVERY node matched by a glob,
/// as one explicit [`CachedSet`].
///
/// Replaces a symlink farm. The former mechanism built
/// `{cache_dir}/{scheme}_glob_{hash}/` full of symlinks to per-node cache files
/// and then pointed a `ListingTable` at the DIRECTORY. That worked only while
/// three separate conventions all held: the caller wiped the directory first,
/// the versions handed in were live, and nothing else wrote there. Two call
/// sites performed that ritual by hand, and the wipe-then-repopulate step raced
/// any concurrent reader of the same pattern, which takes no lock.
///
/// Naming the files makes the guarantee structural instead of ritual, and it is
/// the same mechanism the rollup uses to accumulate several source nodes into
/// one scan. Schema evolution is preserved: [`CachedSet::table_provider`] merges
/// schemas across exactly these files, so a column that appears only in some
/// members is present and NULL-filled elsewhere (the UNION-ALL-BY-NAME property
/// the glob directory existed to provide).
///
/// Each `versions` slice must be that node's LIVE versions, as returned by
/// `list_file_versions`, which already hides collapse-superseded ones.
#[must_use]
pub fn glob_cached_set(
    cache_dir: &Path,
    scheme: &str,
    nodes: &[(tinyfs::NodeID, Vec<FileVersionInfo>)],
) -> crate::version_cache::CachedSet {
    glob_cached_set_bounded(cache_dir, scheme, nodes, &tinyfs::SeriesReadBounds::NONE)
}

/// [`glob_cached_set`], pruned by a per-version event-time bound.
///
/// The multi-node analogue of [`listing_table_from_cache_bounded`], and the
/// reason the rollup needs no per-version partial cache: an incremental rebuild
/// of recent buckets scans only the source versions that can reach them, which
/// is the same "skip old inputs" property the partials directory provided, from
/// metadata tlogfs already records.
#[must_use]
pub fn glob_cached_set_bounded(
    cache_dir: &Path,
    scheme: &str,
    nodes: &[(tinyfs::NodeID, Vec<FileVersionInfo>)],
    bounds: &tinyfs::SeriesReadBounds,
) -> crate::version_cache::CachedSet {
    // Falls back to the cache root only for schema recovery when no member is
    // cached yet, which is the same "no data to describe" case the glob
    // directory reported as an error.
    let mut set = crate::version_cache::CachedSet::empty_in(cache_dir.to_path_buf());
    for (node_id, versions) in nodes {
        let live = LiveVersions::from_persistence(*node_id, versions.clone());
        set.extend(
            node_sidecars(cache_dir, scheme, node_id).cached_set(&live, |v| {
                bounds.retains(version_max_event_time(v), v.version as i64)
            }),
        );
    }
    set
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures::stream::Stream;
    use std::pin::Pin;
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ]))
    }

    fn test_batch(schema: &SchemaRef, timestamps: &[i64], values: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(StringArray::from(
                    values.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn test_version(version: u64, blake3: &str) -> FileVersionInfo {
        FileVersionInfo {
            version,
            timestamp: 0,
            size: 100,
            blake3: Some(blake3.to_string()),
            entry_type: tinyfs::EntryType::FilePhysicalVersion,
            extended_metadata: None,
        }
    }

    /// A series version stamped with a recorded `max_event_time` (epoch µs),
    /// as `list_file_versions` exposes for `FilePhysicalSeries` files.
    fn test_series_version(version: u64, blake3: &str, max_event_time: i64) -> FileVersionInfo {
        let mut meta = std::collections::HashMap::new();
        let _ = meta.insert("max_event_time".to_string(), max_event_time.to_string());
        FileVersionInfo {
            version,
            timestamp: 0,
            size: 100,
            blake3: Some(blake3.to_string()),
            entry_type: tinyfs::EntryType::FilePhysicalSeries,
            extended_metadata: Some(meta),
        }
    }

    fn test_node_id() -> tinyfs::NodeID {
        tinyfs::NodeID::new(uuid7::uuid7().to_string())
    }

    /// Reconciling retires the sidecars of versions that are no longer live and
    /// reports nothing missing when every live version is already cached.
    ///
    /// This is the collection step the format cache lacked entirely: its files
    /// are immutable, so there was never anything to invalidate, but nothing
    /// removed them either and the directory grew by one Parquet per version
    /// ever written. Collapse is what makes that a correctness concern and not
    /// just disk: it replaces versions 1..3 with a single merged version, and
    /// the superseded sidecars hold the very rows the merged one now carries.
    #[tokio::test]
    async fn reconcile_retires_sidecars_a_collapse_superseded() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path();
        let node_id = test_node_id();
        let schema = test_schema();

        let before = vec![
            test_version(1, "h1"),
            test_version(2, "h2"),
            test_version(3, "h3"),
        ];
        for v in &before {
            let batch = test_batch(&schema, &[v.version as i64], &["x"]);
            let stream: crate::version_cache::BatchStream =
                Box::pin(futures::stream::once(async move { Ok(batch) }));
            let _ = cache_write_version(cache_dir, "csv", &node_id, v, schema.clone(), stream)
                .await
                .unwrap();
        }
        let dir = cache_node_dir(cache_dir, "csv", &node_id);
        let count = |d: &Path| std::fs::read_dir(d).unwrap().count();
        assert_eq!(count(&dir), 3, "three versions cached");

        // A collapse merges 1..3 into one new, higher-numbered version with the
        // same content. `list_file_versions` would now return only version 4.
        let after = vec![test_version(4, "merged")];
        let missing = reconcile_cached_versions(cache_dir, "csv", &node_id, &after).unwrap();

        assert_eq!(
            missing.len(),
            1,
            "the merged version is not cached yet, so it must be reported missing"
        );
        assert_eq!(missing[0].version, 4);
        assert_eq!(
            count(&dir),
            0,
            "all three superseded sidecars must be gone; a cache that only adds \
             is how they survived to be double-counted"
        );

        // Cache the merged version, then reconcile again: steady state is a
        // no-op, neither deleting a live sidecar nor reporting it missing.
        let batch = test_batch(&schema, &[4], &["x"]);
        let stream: crate::version_cache::BatchStream =
            Box::pin(futures::stream::once(async move { Ok(batch) }));
        let _ = cache_write_version(cache_dir, "csv", &node_id, &after[0], schema, stream)
            .await
            .unwrap();
        let missing = reconcile_cached_versions(cache_dir, "csv", &node_id, &after).unwrap();
        assert!(
            missing.is_empty(),
            "live cached version must not be missing"
        );
        assert_eq!(count(&dir), 1, "live sidecar must survive reconciliation");
    }

    #[test]
    fn test_cache_node_dir() {
        let cache_dir = Path::new("/tmp/pond/cache");
        let node_id = test_node_id();
        let dir = cache_node_dir(cache_dir, "oteljson", &node_id);
        let dir_str = dir.to_str().unwrap();
        assert!(dir_str.starts_with("/tmp/pond/cache/oteljson_"));
        assert!(dir_str.contains(&node_id.to_string()));
    }

    #[test]
    fn test_cache_version_path() {
        let cache_dir = Path::new("/tmp/pond/cache");
        let node_id = test_node_id();
        let version = test_version(3, "abcdef1234567890");
        let path = cache_version_path(cache_dir, "csv", &node_id, &version);
        let filename = path.file_name().unwrap().to_str().unwrap();
        assert!(filename.starts_with("v3_abcdef1234567890"));
        assert!(filename.ends_with(".parquet"));
    }

    #[test]
    fn test_find_uncached_versions_all_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path();
        let node_id = test_node_id();
        let versions = vec![test_version(1, "aaa"), test_version(2, "bbb")];
        let uncached = find_uncached_versions(cache_dir, "oteljson", &node_id, &versions);
        assert_eq!(uncached.len(), 2);
    }

    #[test]
    fn test_find_uncached_versions_some_cached() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path();
        let node_id = test_node_id();
        let versions = vec![test_version(1, "aaa"), test_version(2, "bbb")];

        // Pre-create the cache dir and v1's parquet file
        let v1_path = cache_version_path(cache_dir, "oteljson", &node_id, &versions[0]);
        std::fs::create_dir_all(v1_path.parent().unwrap()).unwrap();
        std::fs::write(&v1_path, b"fake parquet").unwrap();

        let uncached = find_uncached_versions(cache_dir, "oteljson", &node_id, &versions);
        assert_eq!(uncached.len(), 1);
        assert_eq!(uncached[0].version, 2);
    }

    #[tokio::test]
    async fn test_cache_write_version() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path();
        let node_id = test_node_id();
        let version = test_version(1, "deadbeef");

        let schema = test_schema();
        let batch = test_batch(&schema, &[1000, 2000, 3000], &["a", "b", "c"]);

        let stream: Pin<
            Box<dyn Stream<Item = std::result::Result<RecordBatch, crate::error::Error>> + Send>,
        > = Box::pin(futures::stream::once(async move { Ok(batch) }));

        let path = cache_write_version(cache_dir, "oteljson", &node_id, &version, schema, stream)
            .await
            .unwrap();

        assert!(path.exists());
        assert!(path.to_str().unwrap().ends_with("v1_deadbeef.parquet"));

        // Verify the tmp file was cleaned up
        let tmp_path = path.with_extension("parquet.tmp");
        assert!(!tmp_path.exists());
    }

    #[tokio::test]
    async fn test_listing_table_from_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path();
        let node_id = test_node_id();

        let schema = test_schema();

        // Write two versions
        let mut versions = Vec::new();
        for i in 1_i64..=2 {
            let version = test_version(i as u64, &format!("hash{}", i));
            versions.push(version.clone());
            let batch = test_batch(&schema, &[i * 1000], &[&format!("val{}", i)]);
            let stream: Pin<
                Box<
                    dyn Stream<Item = std::result::Result<RecordBatch, crate::error::Error>> + Send,
                >,
            > = Box::pin(futures::stream::once({
                let batch = batch;
                async move { Ok(batch) }
            }));
            let _ =
                cache_write_version(cache_dir, "csv", &node_id, &version, schema.clone(), stream)
                    .await
                    .unwrap();
        }

        // Build ListingTable and verify
        let ctx = SessionContext::new();
        let table = listing_table_from_cache_bounded(
            cache_dir,
            "csv",
            &node_id,
            &versions,
            &tinyfs::SeriesReadBounds::NONE,
            &ctx,
        )
        .await
        .unwrap();

        // Should have the correct schema
        let table_schema = table.schema();
        assert_eq!(table_schema.fields().len(), 2);
        assert_eq!(table_schema.field(0).name(), "timestamp");
        assert_eq!(table_schema.field(1).name(), "value");
    }

    /// `listing_table_from_cache_bounded` includes only the version Parquets
    /// retained by the bounds: event-time prunes old versions (retaining any
    /// without a recorded bound), version_gt prunes at/below the watermark, and
    /// pruning everything yields an empty (0-row) table -- never a full scan.
    #[tokio::test]
    async fn test_listing_table_from_cache_bounded_prunes() {
        use datafusion::arrow::array::Int64Array as DfInt64Array;

        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path();
        let node_id = test_node_id();
        let schema = test_schema();

        // Three series versions, each one row, with distinct recorded event times.
        // v1 old (t=1000), v2 mid (t=2000), v3 new (t=3000).
        let versions = vec![
            test_series_version(1, "h1", 1000),
            test_series_version(2, "h2", 2000),
            test_series_version(3, "h3", 3000),
        ];
        for v in &versions {
            let batch = test_batch(&schema, &[v.version as i64], &["x"]);
            let stream: Pin<
                Box<
                    dyn Stream<Item = std::result::Result<RecordBatch, crate::error::Error>> + Send,
                >,
            > = Box::pin(futures::stream::once(async move { Ok(batch) }));
            let _ = cache_write_version(cache_dir, "jsonlogs", &node_id, v, schema.clone(), stream)
                .await
                .unwrap();
        }

        // Count rows a bounded listing table exposes.
        async fn count_rows(table: Arc<dyn TableProvider>) -> i64 {
            let ctx = SessionContext::new();
            let _ = ctx.register_table("t", table).unwrap();
            let batches = ctx
                .sql("SELECT COUNT(*) AS c FROM t")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<DfInt64Array>()
                .unwrap()
                .value(0)
        }

        let ctx = SessionContext::new();

        // NONE -> all three versions.
        let all = listing_table_from_cache_bounded(
            cache_dir,
            "jsonlogs",
            &node_id,
            &versions,
            &tinyfs::SeriesReadBounds::NONE,
            &ctx,
        )
        .await
        .unwrap();
        assert_eq!(count_rows(all).await, 3);

        // event_time_lo=2500 -> only v3 (max_event_time >= 2500).
        let hot = listing_table_from_cache_bounded(
            cache_dir,
            "jsonlogs",
            &node_id,
            &versions,
            &tinyfs::SeriesReadBounds::from_event_time_lo(2500),
            &ctx,
        )
        .await
        .unwrap();
        assert_eq!(count_rows(hot).await, 1);

        // version_gt=1 -> v2 and v3.
        let watermarked = listing_table_from_cache_bounded(
            cache_dir,
            "jsonlogs",
            &node_id,
            &versions,
            &tinyfs::SeriesReadBounds::from_version_gt(1),
            &ctx,
        )
        .await
        .unwrap();
        assert_eq!(count_rows(watermarked).await, 2);

        // event_time_lo above every recorded bound -> empty (0 rows), not a full scan.
        let empty = listing_table_from_cache_bounded(
            cache_dir,
            "jsonlogs",
            &node_id,
            &versions,
            &tinyfs::SeriesReadBounds::from_event_time_lo(9999),
            &ctx,
        )
        .await
        .unwrap();
        assert_eq!(count_rows(empty).await, 0);
    }

    /// A version without a recorded `max_event_time` is always retained by an
    /// event-time bound -- a missing bound must never silently drop data.
    #[tokio::test]
    async fn test_bounded_retains_versions_without_event_time() {
        use datafusion::arrow::array::Int64Array as DfInt64Array;
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path();
        let node_id = test_node_id();
        let schema = test_schema();

        // v1 has no recorded bound; v2 is old.
        let versions = vec![
            test_version(1, "h1"), // extended_metadata: None
            test_series_version(2, "h2", 1000),
        ];
        for v in &versions {
            let batch = test_batch(&schema, &[v.version as i64], &["x"]);
            let stream: Pin<
                Box<
                    dyn Stream<Item = std::result::Result<RecordBatch, crate::error::Error>> + Send,
                >,
            > = Box::pin(futures::stream::once(async move { Ok(batch) }));
            let _ = cache_write_version(cache_dir, "jsonlogs", &node_id, v, schema.clone(), stream)
                .await
                .unwrap();
        }

        let ctx = SessionContext::new();
        let table = listing_table_from_cache_bounded(
            cache_dir,
            "jsonlogs",
            &node_id,
            &versions,
            &tinyfs::SeriesReadBounds::from_event_time_lo(5000),
            &ctx,
        )
        .await
        .unwrap();
        let _ = ctx.register_table("t", table).unwrap();
        let batches = ctx
            .sql("SELECT COUNT(*) AS c FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<DfInt64Array>()
            .unwrap()
            .value(0);
        // v2 (t=1000 < 5000) is pruned; v1 (no bound) is retained -> 1 row.
        assert_eq!(c, 1);
    }

    #[tokio::test]
    async fn glob_cached_set_scans_every_matched_node() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path();

        let schema = test_schema();

        // Simulate two different nodes (different files matched by a glob)
        let node1 = test_node_id();
        let node2 = test_node_id();
        let v1 = test_version(1, "hash_a");
        let v2 = test_version(1, "hash_b");

        for (node_id, version, ts, val) in [
            (&node1, &v1, vec![100], vec!["foo"]),
            (&node2, &v2, vec![200], vec!["bar"]),
        ] {
            let batch = test_batch(&schema, &ts, &val);
            let stream: Pin<
                Box<
                    dyn Stream<Item = std::result::Result<RecordBatch, crate::error::Error>> + Send,
                >,
            > = Box::pin(futures::stream::once({
                let batch = batch;
                async move { Ok(batch) }
            }));
            let _ = cache_write_version(cache_dir, "csv", node_id, version, schema.clone(), stream)
                .await
                .unwrap();
        }

        // One explicit scan over both nodes' live versions.
        let nodes = vec![(node1, vec![v1.clone()]), (node2, vec![v2.clone()])];
        let ctx = SessionContext::new();
        let table = glob_cached_set(cache_dir, "csv", &nodes)
            .table_provider()
            .await
            .unwrap();

        let table_schema = table.schema();
        assert_eq!(table_schema.fields().len(), 2);

        // Execute a query to confirm both files are read
        let _ = ctx.register_table("source", table).unwrap();
        let df = ctx.sql("SELECT COUNT(*) as cnt FROM source").await.unwrap();
        let batches = df.collect().await.unwrap();
        let cnt = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(cnt, 2); // One row from each node
    }

    /// Verify that the glob scan merges schemas across files
    /// with different columns (UNION ALL BY NAME semantics).  This catches the
    /// bug where schema inference from a single file drops columns that only
    /// exist in later files (e.g., sensors added over time).
    #[tokio::test]
    async fn glob_cached_set_merges_schemas_across_members() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path();

        // File 1: schema has (timestamp, sensor_a)
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("sensor_a", DataType::Utf8, true),
        ]));

        // File 2: schema has (timestamp, sensor_a, sensor_b)
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("sensor_a", DataType::Utf8, true),
            Field::new("sensor_b", DataType::Utf8, true),
        ]));

        let node1 = test_node_id();
        let node2 = test_node_id();
        let v1 = test_version(1, "hash_merge_a");
        let v2 = test_version(1, "hash_merge_b");

        // Write file 1 with schema1
        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(StringArray::from(vec![Some("a1")])),
            ],
        )
        .unwrap();
        let stream1: Pin<
            Box<dyn Stream<Item = std::result::Result<RecordBatch, crate::error::Error>> + Send>,
        > = Box::pin(futures::stream::once({
            let b = batch1;
            async move { Ok(b) }
        }));
        let _ = cache_write_version(cache_dir, "csv", &node1, &v1, schema1.clone(), stream1)
            .await
            .unwrap();

        // Write file 2 with schema2
        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![
                Arc::new(Int64Array::from(vec![200])),
                Arc::new(StringArray::from(vec![Some("a2")])),
                Arc::new(StringArray::from(vec![Some("b2")])),
            ],
        )
        .unwrap();
        let stream2: Pin<
            Box<dyn Stream<Item = std::result::Result<RecordBatch, crate::error::Error>> + Send>,
        > = Box::pin(futures::stream::once({
            let b = batch2;
            async move { Ok(b) }
        }));
        let _ = cache_write_version(cache_dir, "csv", &node2, &v2, schema2.clone(), stream2)
            .await
            .unwrap();

        // Merged schema must carry all 3 columns across the two members.
        let nodes = vec![(node1, vec![v1.clone()]), (node2, vec![v2.clone()])];
        let ctx = SessionContext::new();
        let table = glob_cached_set(cache_dir, "csv", &nodes)
            .table_provider()
            .await
            .unwrap();

        let table_schema = table.schema();
        let field_names: Vec<&str> = table_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert!(
            field_names.contains(&"timestamp"),
            "merged schema must contain timestamp"
        );
        assert!(
            field_names.contains(&"sensor_a"),
            "merged schema must contain sensor_a"
        );
        assert!(
            field_names.contains(&"sensor_b"),
            "merged schema must contain sensor_b (from second file)"
        );

        // Query the data -- sensor_b should be NULL for file 1's row
        let _ = ctx.register_table("source", table).unwrap();
        let df = ctx
            .sql("SELECT timestamp, sensor_a, sensor_b FROM source ORDER BY timestamp")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);

        use arrow::array::Array;

        let ts_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts_col.value(0), 100);
        assert_eq!(ts_col.value(1), 200);

        let sb_col = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(sb_col.is_null(0), "sensor_b should be NULL for file 1");
        assert_eq!(sb_col.value(1), "b2");
    }
}
