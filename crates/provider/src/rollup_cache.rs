// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Rollup partial-aggregate cache -- per-version Parquet cache of decomposable
//! temporal-reduce partials.
//!
//! `temporal-reduce` downsamples a source series into per-resolution
//! aggregations.  Without caching, every read recomputes a full
//! `GROUP BY date_bin` over the entire source history, so per-build cost grows
//! without bound as the pond ages.
//!
//! This module caches the decomposable partials (`Sum`, `Count`, `Min`, `Max`,
//! ...) for each individual input version as a Parquet file on disk under
//! `{POND}/cache/`.  At read time a `ListingTable` is built over the cached
//! partials and a cheap cross-version merge (`GROUP BY time_bucket`)
//! reconstructs the final aggregation.
//!
//! This is the aggregation-tier analogue of [`crate::format_cache`], which
//! caches the *parsed leaves*.  Together they make both the parse and the
//! aggregation incremental.
//!
//! Key properties (identical discipline to the format cache):
//! - Per-version caching: each input version is independently immutable
//!   (blake3 hash), so there is nothing to invalidate.  One new ingest version
//!   produces exactly one new partial.
//! - Incremental: only uncached versions are computed; cached versions are free.
//! - Throwaway: `rm -rf {POND}/cache/` is always safe.
//! - Config-namespaced: a different aggregation set / time column / resolution
//!   list yields a fresh `cfg_hash` namespace rather than mixing semantics.

use datafusion::catalog::TableProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Result type for rollup cache operations.
type Result<T> = std::result::Result<T, crate::error::Error>;

/// Compute a stable namespace hash over the parts of a temporal-reduce config
/// that change the *meaning* of the cached partials: the aggregation set, the
/// time column, and the resolution list.
///
/// A config change yields a fresh namespace rather than mixing incompatible
/// partials into one directory.  Changing config invalidates semantics, not
/// content, so a new namespace is the correct response; the old one is reaped
/// by normal cache pruning.
///
/// The caller passes a canonical string built from those config fields.  The
/// hash uses the same `DefaultHasher` convention as
/// the former `format_cache::pattern_hash`; stability across binaries is not
/// required because the cache is throwaway.
#[must_use]
pub fn cfg_hash(canonical: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    canonical.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Directory holding a node's cached rollup partials for one config namespace.
///
/// Returns `{cache_dir}/rollup_{cfg_hash}_{node_id}/`.
#[must_use]
pub fn cache_node_dir(cache_dir: &Path, cfg_hash: &str, node_id: &tinyfs::NodeID) -> PathBuf {
    cache_dir.join(format!("rollup_{}_{}", cfg_hash, node_id))
}

// A rollup glob dir is a `SidecarNaming::NodeScoped` [`crate::version_cache::SidecarDir`]:
// one partial file per (source node, input version). The source pattern can
// match many rotated input files, each its own node with its own versions, so
// the directory is shared and reconciliation there must be node scoped. The
// naming, writing, reconciling and reading of those partials lives in
// `version_cache`; this module keeps only what is genuinely rollup specific
// (the sequentiality frontier, sealed runs, and namespace drops).

/// Directory holding all per-source-version partials for one temporal-reduce
/// node and config namespace: `{cache_dir}/rollup_{cfg_hash}_{tr_node_id}/`.
#[must_use]
pub fn glob_dir(cache_dir: &Path, cfg_hash: &str, tr_node_id: &tinyfs::NodeID) -> PathBuf {
    cache_node_dir(cache_dir, cfg_hash, tr_node_id)
}

/// Wrap an already-resolved partials directory path as a sidecar dir.
#[must_use]
pub fn partials_dir_at(glob_dir: &Path) -> crate::version_cache::SidecarDir {
    crate::version_cache::SidecarDir::new(
        glob_dir.to_path_buf(),
        crate::version_cache::SidecarNaming::NodeScoped,
    )
}

/// The partials directory of one temporal-reduce node, as a sidecar dir.
#[must_use]
pub fn partials_dir(
    cache_dir: &Path,
    cfg_hash: &str,
    tr_node_id: &tinyfs::NodeID,
) -> crate::version_cache::SidecarDir {
    crate::version_cache::SidecarDir::new(
        glob_dir(cache_dir, cfg_hash, tr_node_id),
        crate::version_cache::SidecarNaming::NodeScoped,
    )
}

/// Drop a node's entire rollup-cache namespace for one config, forcing all
/// partials to be recomputed on next read.  Used by the `--rebuild` recovery
/// path.  Idempotent: a missing directory is not an error.
pub fn drop_node_namespace(
    cache_dir: &Path,
    cfg_hash: &str,
    node_id: &tinyfs::NodeID,
) -> Result<()> {
    let dir = cache_node_dir(cache_dir, cfg_hash, node_id);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).map_err(crate::error::Error::Io)?;
    }
    let mdir = merged_dir(cache_dir, cfg_hash, node_id);
    if mdir.exists() {
        std::fs::remove_dir_all(&mdir).map_err(crate::error::Error::Io)?;
    }
    Ok(())
}

/// Drop every rollup-cache namespace under `cache_dir`, forcing all partials
/// for all temporal-reduce nodes to be recomputed on next read.  This is the
/// global `--rebuild` recovery primitive.  The format cache and any other cache
/// content are left untouched.  Idempotent: a missing `cache_dir` is not an
/// error.
pub fn drop_all(cache_dir: &Path) -> Result<usize> {
    if !cache_dir.exists() {
        return Ok(0);
    }
    let mut dropped = 0;
    for entry in std::fs::read_dir(cache_dir).map_err(crate::error::Error::Io)? {
        let entry = entry.map_err(crate::error::Error::Io)?;
        let path = entry.path();
        let is_rollup = path.is_dir()
            && entry
                .file_name()
                .to_str()
                .is_some_and(|n| n.starts_with("rollup_") || n.starts_with("merged_"));
        if is_rollup {
            std::fs::remove_dir_all(&path).map_err(crate::error::Error::Io)?;
            dropped += 1;
        }
    }
    Ok(dropped)
}

/// Path of a source node's sequentiality-frontier sidecar within the glob dir:
/// `{glob_dir}/{source_node}.frontier`.  The file holds the maximum sealed
/// `time_bucket` value (in the finest-interval timestamp unit) observed across
/// every cached version of that source node.
#[must_use]
pub fn frontier_path(glob_dir: &Path, source_node_id: &tinyfs::NodeID) -> PathBuf {
    glob_dir.join(format!("{}.frontier", source_node_id))
}

/// Read a source node's persisted sequentiality frontier.
///
/// Returns `Ok(None)` only when the frontier file is genuinely absent, which is
/// the self-heal path for a fresh or `--rebuild`-cleared cache. A file that is
/// present but unparseable is a corrupt control artifact and is a hard error:
/// silently treating it as absent would disable the double-count guard for
/// overlapping non-series sources, allowing a re-snapshot to be summed twice.
pub fn read_frontier(glob_dir: &Path, source_node_id: &tinyfs::NodeID) -> Result<Option<i64>> {
    let path = frontier_path(glob_dir, source_node_id);
    let contents = match std::fs::read_to_string(&path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(crate::error::Error::Io(e)),
    };
    contents.trim().parse::<i64>().map(Some).map_err(|e| {
        crate::error::Error::CacheCorrupt(format!(
            "frontier file '{}' is present but unparseable ({}); the rollup cache \
             is corrupt. Re-run the export with --rebuild to recompute it from scratch.",
            path.display(),
            e
        ))
    })
}

/// Persist a source node's frontier (atomic write via `.tmp` then rename).
pub fn write_frontier(
    glob_dir: &Path,
    source_node_id: &tinyfs::NodeID,
    frontier: i64,
) -> Result<()> {
    std::fs::create_dir_all(glob_dir).map_err(crate::error::Error::Io)?;
    let final_path = frontier_path(glob_dir, source_node_id);
    let tmp_path = final_path.with_extension("frontier.tmp");
    std::fs::write(&tmp_path, frontier.to_string()).map_err(crate::error::Error::Io)?;
    std::fs::rename(&tmp_path, &final_path).map_err(crate::error::Error::Io)?;
    Ok(())
}

// --- Merged-output cache: per-resolution materialized merge result -----------
//
// The partial cache above makes the partial COMPUTATION incremental, but the
// cross-version merge (`GROUP BY date_bin` over every cached partial) still runs
// in full on every build. This merged-output cache materializes the merged
// buckets to one Parquet file per output resolution, so a rebuild recomputes
// only the suffix of buckets touched by newly-added source versions and reuses
// the sealed prefix unchanged.
//
// It lives in a directory SEPARATE from the partials glob dir so the partials
// `ListingTable` (which unions every `.parquet` under the glob dir) never
// mistakes a merged-output file for a partial.
//
// Integrity: a blake3 digest of the Parquet bytes is written to a sidecar. On
// read the digest is re-verified. An absent or incompletely published file
// self-heals via a full remerge; a present file whose bytes do not match the
// digest is a hard error rather than silently serving tampered aggregates.

/// Directory holding the merged-output cache for one temporal-reduce node and
/// config namespace: `{cache_dir}/merged_{cfg_hash}_{node_id}/`.
#[must_use]
pub fn merged_dir(cache_dir: &Path, cfg_hash: &str, node_id: &tinyfs::NodeID) -> PathBuf {
    cache_dir.join(format!("merged_{}_{}", cfg_hash, node_id))
}

/// List the per-source-version partial member files (`*.parquet`) currently
/// present in a glob dir. These are the partials merged into every resolution's
/// output; the returned paths are sorted for deterministic diffing.
pub fn list_glob_members(glob_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let rd = match std::fs::read_dir(glob_dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(crate::error::Error::Io(e)),
    };
    for entry in rd {
        let path = entry.map_err(crate::error::Error::Io)?.path();
        if path.extension().is_some_and(|ext| ext == "parquet") {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

// --- Sealed-runs merged cache (Phase 2: watermark + immutable runs) ----------
//
// Phase 1's merged cache is a single mutable Parquet whose sealed prefix is
// rewritten on every build (O(history) I/O). Phase 2 replaces it with a
// directory of immutable, append-only *sealed run* files plus one recomputed
// *hot* file, separated by a watermark derived from `allowed_lateness`:
//
//   {merged_dir}/res{secs}/
//       run-00000000.parquet   immutable sealed run (buckets [lo, hi))
//       run-00000001.parquet   ...
//       hot.parquet            every bucket at or above sealed_hi, recomputed
//                              each build
//       manifest.json          watermark + run ranges + coverage + digests
//
// Buckets below sealed_hi are frozen once and never rescanned, so per-build cost
// is bounded to the hot window. That window holds the allowed-lateness tail plus
// whatever has frozen since the last seal, which SEAL_TARGET_BYTES bounds. The read provider is a
// `ListingTable` over the whole res dir (runs ⧺ hot); consumers apply their own
// `ORDER BY`.

/// Minimum accumulated size, in bytes, before frozen buckets are sealed into a
/// run file.
///
/// Sealing used to trigger on the watermark advancing, which tied the file count
/// to how often a build ran rather than to how much data existed: a pond rebuilt
/// every minute grew a run file every minute, each a few kilobytes. Gating on
/// size decouples the two, so the count tracks data volume.
///
/// Deliberately modest. The cost of raising it is that the hot window -- which is
/// recomputed in full on every build -- carries the unsealed remainder, so the
/// target bounds per-build work. 1 MiB keeps that recompute trivial while still
/// collapsing hundreds of builds into one run. Compaction is the right tool for
/// making files genuinely large; this only has to stop the bleeding.
pub const SEAL_TARGET_BYTES: u64 = 1024 * 1024;

/// Maximum number of sealed runs to leave live in one resolution before
/// compaction merges regardless of size class.
///
/// This bounds read fan-out: every query opens every live run plus hot, so the
/// count is what a reader pays. Size-tiered merging keeps the number near
/// `log(data)` on its own; this is the backstop for ragged inputs.
pub const MAX_LIVE_RUNS: usize = 50;

/// One immutable sealed run file and the half-open output-bucket range it
/// covers, in epoch seconds. `lo_secs` is `None` for the genesis run (unbounded
/// below); `hi_secs` is exclusive.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SealedRun {
    pub name: String,
    pub lo_secs: Option<i64>,
    pub hi_secs: i64,
    pub digest: String,
    /// On-disk size of the run file in bytes, recorded when it was written.
    /// Lets compaction choose merge candidates without stat-ing every run.
    pub bytes: u64,
}

/// On-disk format of the sealed run + hot files. Bumped when their column
/// layout or the manifest's build semantics change so an older cache is
/// discarded and rebuilt rather than misread. `partials-v2` stores the
/// *mergeable partials* (sum/count/min/max) with output columns reconstructed at
/// read time (design §3 / Phase 3 step 1) AND may derive a coarser resolution by
/// folding the next-finer resolution's runs (`source_digest`; Phase 3 step 2);
/// legacy caches deserialize with an empty `format` and are wiped + rebuilt.
pub const SEALED_FORMAT: &str = "partials-v3";

/// Manifest describing the sealed-runs cache for one output resolution. Its
/// serialized bytes are the export-hint digest, so it must serialize
/// deterministically (fixed field order; `covered` is an ordered set).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct SealedManifest {
    /// On-disk file format (see [`SEALED_FORMAT`]). Absent in legacy manifests,
    /// which deserialize to `""` and are treated as a format mismatch (rebuild).
    #[serde(default)]
    pub format: String,
    /// The `allowed_lateness` (seconds) this cache was built under. A build with
    /// a different value discards and rebuilds the res dir.
    pub allowed_lateness_secs: i64,
    /// Exclusive upper bound (epoch seconds) of the sealed region: every output
    /// bucket with start `< sealed_hi_secs` lives in some run. `None` before any
    /// bucket has been sealed.
    pub sealed_hi_secs: Option<i64>,
    /// Next sealed-run sequence number for file naming.
    pub next_seq: u64,
    /// Sealed run files in ascending bucket order.
    pub runs: Vec<SealedRun>,
    /// blake3 digest of `hot.parquet`.
    pub hot_digest: Option<String>,
    /// On-disk size of `hot.parquet` as of the last build, in bytes.
    ///
    /// This is the seal trigger. `hot` spans `[sealed_hi, inf)`, which strictly
    /// contains the frozen span `[sealed_hi, watermark)` that a seal would
    /// write, so this is a conservative upper bound on how much a seal would
    /// produce: below the target, sealing cannot be worthwhile, and we can skip
    /// it without writing anything to find out.
    #[serde(default)]
    pub hot_bytes: u64,
    /// Partial member file names this cache reflects (freshness key; identical
    /// role to the Phase 1 coverage sidecar). Used only by the finest resolution,
    /// which folds the shared finest partials directly; empty for coarser
    /// resolutions, which key freshness on `source_digest` instead.
    pub covered: BTreeSet<String>,
    /// For a coarser resolution built by folding the next-finer resolution's
    /// runs (Phase 3 step 2): the finer resolution's manifest digest this cache
    /// was folded from. `None` for the finest resolution (which folds the shared
    /// partials and uses `covered`). A change in the finer digest triggers a
    /// coarse rebuild/advance.
    #[serde(default)]
    pub source_digest: Option<String>,
}

/// Per-resolution sealed-runs directory: `{merged_dir}/res{interval_secs}/`.
#[must_use]
pub fn sealed_res_dir(merged_dir: &Path, interval_secs: u64) -> PathBuf {
    merged_dir.join(format!("res{}", interval_secs))
}

fn sealed_manifest_path(res_dir: &Path) -> PathBuf {
    res_dir.join("manifest.json")
}

/// Path of the recomputed hot-window Parquet in a res dir.
#[must_use]
pub fn hot_path(res_dir: &Path) -> PathBuf {
    res_dir.join("hot.parquet")
}

/// Path of a sealed run file by name in a res dir.
#[must_use]
pub fn run_path(res_dir: &Path, name: &str) -> PathBuf {
    res_dir.join(name)
}

/// Read the sealed-runs manifest. `Ok(None)` when absent (fresh cache). A
/// present-but-unparseable manifest is a hard error rather than a silent
/// rebuild, matching the frontier/merged-cache corruption discipline.
pub fn read_sealed_manifest(res_dir: &Path) -> Result<Option<SealedManifest>> {
    let path = sealed_manifest_path(res_dir);
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(crate::error::Error::Io(e)),
    };
    serde_json::from_slice(&bytes).map(Some).map_err(|e| {
        crate::error::Error::CacheCorrupt(format!(
            "sealed-runs manifest '{}' is present but unparseable ({}); the rollup \
             cache is corrupt. Re-run the export with --rebuild to recompute it.",
            path.display(),
            e
        ))
    })
}

/// Compute the export-hint digest of a manifest without writing it (used on the
/// reuse path, where the res dir is served unchanged).
pub fn sealed_manifest_digest(manifest: &SealedManifest) -> Result<String> {
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|e| crate::error::Error::Arrow(format!("serialize sealed manifest: {}", e)))?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

/// Persist the sealed-runs manifest atomically (tmp + rename) and return the
/// blake3 digest of its serialized bytes, which callers use as the export-hint
/// digest for the whole resolution.
pub async fn write_sealed_manifest(res_dir: &Path, manifest: &SealedManifest) -> Result<String> {
    tokio::fs::create_dir_all(res_dir).await?;
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|e| crate::error::Error::Arrow(format!("serialize sealed manifest: {}", e)))?;
    let digest = blake3::hash(&bytes).to_hex().to_string();
    let path = sealed_manifest_path(res_dir);
    let tmp = PathBuf::from(format!("{}.tmp", path.display()));
    tokio::fs::write(&tmp, &bytes).await?;
    tokio::fs::rename(&tmp, &path).await?;
    Ok(digest)
}

/// Delete run files that compaction superseded.
///
/// MUST be called only after the manifest naming their replacement has been
/// durably written. Publishing the merged run first and deleting its inputs
/// second means a crash in between leaves orphans rather than a hole, and
/// orphans are harmless precisely because reads enumerate the manifest instead
/// of listing the directory. Doing it the other way round would lose data.
///
/// Failures are logged and ignored: a leftover file costs disk, not
/// correctness, and the next compaction is free to try again.
pub async fn remove_superseded(files: &[PathBuf]) {
    for f in files {
        if let Err(e) = tokio::fs::remove_file(f).await
            && e.kind() != std::io::ErrorKind::NotFound
        {
            log::warn!(
                "rollup compaction: could not remove superseded run {}: {e}",
                f.display()
            );
        }
    }
}

/// Remove an entire res dir (used when `allowed_lateness` changes or the
/// coverage shrinks, forcing a rebuild from the retained partials).
pub fn wipe_sealed_res_dir(res_dir: &Path) -> Result<()> {
    match std::fs::remove_dir_all(res_dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(crate::error::Error::Io(e)),
    }
}

/// Build the read provider for a res dir: a `ListingTable` over exactly the
/// sealed runs named by `manifest`, plus the hot file. A manifest member
/// missing on disk is a hard error (corrupt cache), never a silent hole in the
/// series.
///
/// The scan names its files EXPLICITLY rather than listing `res_dir`. The two
/// are not the same set: a `.parquet` can sit in the directory without being
/// referenced by the manifest, and every such orphan is a duplicate copy of
/// some bucket range that a directory-shaped scan sums a second time. Orphans
/// arise in normal operation, not only after a crash -- [`crate::version_cache::write_parquet_atomic`]
/// renames a new run into place BEFORE the manifest recording it is written, so
/// any interruption in that window leaves one behind, and compaction (which must
/// publish a merged segment before deleting its inputs) widens that window by
/// design. This is the same defect that let a directory-listed format cache
/// double-count the versions a collapse superseded; the fix is the same one.
pub async fn listing_table_for_res_dir(
    res_dir: &Path,
    manifest: &SealedManifest,
    ts_column: &str,
) -> Result<Arc<dyn TableProvider>> {
    let mut files: Vec<PathBuf> = Vec::with_capacity(manifest.runs.len() + 1);
    for run in &manifest.runs {
        let p = run_path(res_dir, &run.name);
        if !p.exists() {
            return Err(crate::error::Error::CacheCorrupt(format!(
                "sealed run '{}' recorded in the manifest is missing on disk; the \
                 rollup cache is corrupt. Re-run the export with --rebuild.",
                p.display()
            )));
        }
        files.push(p);
    }
    let hot = hot_path(res_dir);
    if !hot.exists() {
        return Err(crate::error::Error::CacheCorrupt(format!(
            "sealed-runs hot file '{}' is missing; the rollup cache is corrupt. \
             Re-run the export with --rebuild.",
            hot.display()
        )));
    }
    files.push(hot);

    listing_table_for_files(&files, res_dir, ts_column).await
}

/// Build a `ListingTable` over exactly `files`, in the order given.
///
/// The explicit list is the point: a directory-shaped table would also pick up
/// orphans, which is the defect [`listing_table_for_res_dir`] exists to avoid.
/// Compaction needs the same guarantee over a subset -- it reads only the runs
/// it is merging -- so both go through here.
pub async fn listing_table_for_files(
    files: &[PathBuf],
    fallback_dir: &Path,
    ts_column: &str,
) -> Result<Arc<dyn TableProvider>> {
    // Schema from the same explicit member list, so an orphan cannot contribute
    // a column either. `files` always holds at least the hot file, so the
    // empty-list directory fallback inside `merge_parquet_schemas` is unreachable.
    let merged_schema = crate::version_cache::merge_parquet_schemas(files, fallback_dir).await?;
    let mut paths = Vec::with_capacity(files.len());
    for p in files {
        paths.push(ListingTableUrl::parse(format!("file://{}", p.display()))?);
    }
    // Every run and the hot file is written by a query ending in
    // `ORDER BY time_bucket`, so each file is individually sorted ascending on
    // the timestamp column, and the runs' bucket ranges are disjoint. Declaring
    // that file-level ordering lets the physical planner satisfy a consumer's
    // `ORDER BY {ts}` with a streaming `SortPreservingMergeExec` (k-way merge of
    // the already-sorted runs + hot) instead of a `SortExec` that buffers the
    // whole reduced series in memory -- the O(1)-memory read path from the design
    // §3. Parquet statistics (collected by default) give the planner the per-file
    // min/max it needs to order the file groups.
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension(".parquet")
        .with_collect_stat(true)
        .with_file_sort_order(vec![vec![
            datafusion::prelude::col(ts_column).sort(true, false),
        ]]);
    let config = ListingTableConfig::new_with_multi_paths(paths)
        .with_listing_options(listing_options)
        .with_schema(merged_schema);
    let table = ListingTable::try_new(config)?;
    Ok(Arc::new(table))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::version_cache::{BatchStream, write_parquet_atomic};
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::SchemaRef;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::context::SessionContext;
    use std::sync::Arc;
    use tinyfs::FileVersionInfo;

    fn partials_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("time_bucket", DataType::Int64, false),
            Field::new("__p_sum_0", DataType::Float64, true),
            Field::new("__p_count_1", DataType::Int64, true),
        ]))
    }

    fn partials_batch(
        schema: &SchemaRef,
        buckets: &[i64],
        sums: &[f64],
        counts: &[i64],
    ) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(buckets.to_vec())),
                Arc::new(Float64Array::from(sums.to_vec())),
                Arc::new(Int64Array::from(counts.to_vec())),
            ],
        )
        .unwrap()
    }

    fn test_version(version: u64, blake3: Option<&str>) -> FileVersionInfo {
        FileVersionInfo {
            version,
            timestamp: 0,
            size: 100,
            blake3: blake3.map(str::to_string),
            entry_type: tinyfs::EntryType::FilePhysicalVersion,
            extended_metadata: None,
        }
    }

    fn test_node_id() -> tinyfs::NodeID {
        tinyfs::NodeID::new(uuid7::uuid7().to_string())
    }

    #[test]
    fn test_cfg_hash_stable_and_distinct() {
        let a = cfg_hash("avg|timestamp|1h,1d");
        let b = cfg_hash("avg|timestamp|1h,1d");
        let c = cfg_hash("avg|timestamp|1h,2d");
        assert_eq!(a, b, "same config must hash identically");
        assert_ne!(a, c, "different resolution list must change the namespace");
        assert_eq!(a.len(), 16);
    }

    #[test]
    fn test_cache_node_dir_layout() {
        let cache_dir = Path::new("/tmp/pond/cache");
        let node_id = test_node_id();
        let dir = cache_node_dir(cache_dir, "deadbeef", &node_id);
        let dir_str = dir.to_str().unwrap();
        assert!(dir_str.starts_with("/tmp/pond/cache/rollup_deadbeef_"));
        assert!(dir_str.contains(&node_id.to_string()));
    }

    /// Partials of several input versions merge to exactly the single-pass
    /// GROUP BY result, including a bucket straddling two versions -- and,
    /// after a version collapse, the superseded versions' partials are gone
    /// rather than summed a second time.
    #[tokio::test]
    async fn test_write_and_list_partials_merge() {
        use crate::version_cache::LiveVersions;

        let tmp = tempfile::tempdir().unwrap();
        let node_id = test_node_id();
        let dir = partials_dir(tmp.path(), "cfg", &node_id);
        let schema = partials_schema();

        // Version 1: bucket 0 sum=10 count=2 ; bucket 1 sum=5 count=1
        let v1 = test_version(1, Some("v1hash"));
        let b1 = partials_batch(&schema, &[0, 1], &[10.0, 5.0], &[2, 1]);
        let s1: BatchStream = Box::pin(futures::stream::iter(vec![Ok(b1)]));
        _ = dir
            .write_sidecar(&node_id, &v1, schema.clone(), s1)
            .await
            .unwrap();

        // Version 2: bucket 1 (boundary straddle) sum=7 count=1 ; bucket 2 sum=4 count=1
        let v2 = test_version(2, Some("v2hash"));
        let b2 = partials_batch(&schema, &[1, 2], &[7.0, 4.0], &[1, 1]);
        let s2: BatchStream = Box::pin(futures::stream::iter(vec![Ok(b2)]));
        _ = dir
            .write_sidecar(&node_id, &v2, schema.clone(), s2)
            .await
            .unwrap();

        // Incrementality: both versions now cached, nothing stale.
        let live = LiveVersions::from_persistence(node_id, vec![v1.clone(), v2.clone()]);
        let reconciled = dir.reconcile(&live).unwrap();
        assert!(reconciled.missing.is_empty());
        assert!(reconciled.removed.is_empty());

        let merged = merge_partials(&dir.cached_set(&live, |_| true)).await;
        assert_eq!(merged, vec![(0, 10.0, 2), (1, 12.0, 2), (2, 4.0, 1)]);

        // Now collapse v1+v2 into a single live v3 covering the same rows. The
        // superseded partials must be removed, not merged on top of v3's --
        // that addition is the double-count pinned by testsuite 733.
        let v3 = test_version(3, Some("v3hash"));
        let live = LiveVersions::from_persistence(node_id, vec![v3.clone()]);
        let reconciled = dir.reconcile(&live).unwrap();
        assert_eq!(reconciled.removed.len(), 2);
        assert_eq!(reconciled.missing.len(), 1);

        let b3 = partials_batch(&schema, &[0, 1, 2], &[10.0, 12.0, 4.0], &[2, 2, 1]);
        let s3: BatchStream = Box::pin(futures::stream::iter(vec![Ok(b3)]));
        _ = dir
            .write_sidecar(&node_id, &v3, schema.clone(), s3)
            .await
            .unwrap();

        let merged = merge_partials(&dir.cached_set(&live, |_| true)).await;
        assert_eq!(merged, vec![(0, 10.0, 2), (1, 12.0, 2), (2, 4.0, 1)]);
    }

    /// Fold a cached set the way the read path does, returning
    /// `(time_bucket, sum, count)` rows.
    async fn merge_partials(set: &crate::version_cache::CachedSet) -> Vec<(i64, f64, i64)> {
        let ctx = SessionContext::new();
        let table = set.table_provider().await.unwrap();
        _ = ctx.register_table("partials", table).unwrap();
        let df = ctx
            .sql(
                "SELECT time_bucket, SUM(\"__p_sum_0\") AS s, SUM(\"__p_count_1\") AS c \
                 FROM partials GROUP BY time_bucket ORDER BY time_bucket",
            )
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let merged = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        let bucket = merged
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let s = merged
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let c = merged
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        (0..merged.num_rows())
            .map(|i| (bucket.value(i), s.value(i), c.value(i)))
            .collect()
    }

    /// A stray Parquet in the res dir must not contribute rows: the manifest,
    /// not the directory listing, defines what the scan reads.
    ///
    /// Orphans are ordinary, not exotic. `write_parquet_atomic` renames a run
    /// into place before the manifest naming it is written, so an interruption
    /// in that window leaves one; compaction must publish a merged segment
    /// before deleting the inputs it replaces, so it holds that window open on
    /// purpose. A directory-shaped scan sums such a file a second time, which is
    /// exactly how a directory-listed format cache double-counted the versions a
    /// collapse superseded.
    #[tokio::test]
    async fn listing_table_ignores_a_parquet_the_manifest_does_not_name() {
        let tmp = tempfile::tempdir().unwrap();
        let res_dir = tmp.path().join("res60");
        let schema = partials_schema();

        async fn put(path: &Path, schema: SchemaRef, batch: RecordBatch) -> String {
            let s: BatchStream = Box::pin(futures::stream::iter(vec![Ok(batch)]));
            write_parquet_atomic(path, schema, s).await.unwrap()
        }

        // One sealed run covering buckets [0, 120) and the open hot file.
        let run_name = "run-00000000.parquet";
        let run_digest = put(
            &run_path(&res_dir, run_name),
            schema.clone(),
            partials_batch(&schema, &[0, 60], &[10.0, 20.0], &[1, 2]),
        )
        .await;
        let hot_digest = put(
            &hot_path(&res_dir),
            schema.clone(),
            partials_batch(&schema, &[120], &[40.0], &[4]),
        )
        .await;

        // The orphan: a duplicate of the sealed run under a name the manifest
        // never records, as an interrupted seal or an in-flight compaction
        // would leave behind.
        _ = put(
            &run_path(&res_dir, "run-00000001.parquet"),
            schema.clone(),
            partials_batch(&schema, &[0, 60], &[10.0, 20.0], &[1, 2]),
        )
        .await;

        let manifest = SealedManifest {
            format: SEALED_FORMAT.to_string(),
            allowed_lateness_secs: 0,
            sealed_hi_secs: Some(120),
            next_seq: 1,
            runs: vec![SealedRun {
                name: run_name.to_string(),
                lo_secs: None,
                hi_secs: 120,
                digest: run_digest,
                bytes: 0,
            }],
            hot_digest: Some(hot_digest),
            hot_bytes: 0,
            covered: BTreeSet::new(),
            source_digest: None,
        };

        let provider = listing_table_for_res_dir(&res_dir, &manifest, "time_bucket")
            .await
            .unwrap();
        let ctx = SessionContext::new();
        _ = ctx.register_table("merged", provider).unwrap();
        let batches = ctx
            .sql(
                "SELECT time_bucket, SUM(\"__p_sum_0\") AS s, SUM(\"__p_count_1\") AS c \
                 FROM merged GROUP BY time_bucket ORDER BY time_bucket",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let merged = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        let bucket = merged
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let s = merged
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let c = merged
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let got: Vec<(i64, f64, i64)> = (0..merged.num_rows())
            .map(|i| (bucket.value(i), s.value(i), c.value(i)))
            .collect();

        // Assert the SUMS, not an average: the orphan doubles sum and count
        // together, so avg is invariant under this bug and would pass either way.
        assert_eq!(
            got,
            vec![(0, 10.0, 1), (60, 20.0, 2), (120, 40.0, 4)],
            "an unreferenced Parquet in the res dir was scanned; the read must \
             enumerate the manifest, not list the directory"
        );
    }

    #[tokio::test]
    async fn test_drop_node_namespace() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path();
        let node_id = test_node_id();
        let schema = partials_schema();
        let v1 = test_version(1, Some("v1hash"));
        let b1 = partials_batch(&schema, &[0], &[1.0], &[1]);
        let s1: BatchStream = Box::pin(futures::stream::iter(vec![Ok(b1)]));
        _ = partials_dir(cache_dir, "cfg", &node_id)
            .write_sidecar(&node_id, &v1, schema.clone(), s1)
            .await
            .unwrap();
        assert!(cache_node_dir(cache_dir, "cfg", &node_id).exists());
        drop_node_namespace(cache_dir, "cfg", &node_id).unwrap();
        assert!(!cache_node_dir(cache_dir, "cfg", &node_id).exists());
        // Idempotent.
        drop_node_namespace(cache_dir, "cfg", &node_id).unwrap();
    }

    #[tokio::test]
    async fn test_merged_cache_drop_all_removes_merged_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let node_id = test_node_id();
        // Materialize a sealed-runs res dir so drop_all has a merged_* dir to
        // remove.
        let res_dir = sealed_res_dir(&merged_dir(tmp.path(), "cfg", &node_id), 60);
        let manifest = SealedManifest {
            allowed_lateness_secs: 86400,
            ..Default::default()
        };
        let _ = write_sealed_manifest(&res_dir, &manifest).await.unwrap();
        assert!(res_dir.exists());
        let dropped = drop_all(tmp.path()).unwrap();
        assert!(dropped >= 1);
        assert!(!res_dir.exists());
    }
}
