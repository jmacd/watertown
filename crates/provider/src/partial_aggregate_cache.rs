// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Partial-aggregate cache -- on-disk storage for `temporal-reduce` levels.
//!
//! **Naming.** This module stores *partial aggregates*: per-bucket `sum`,
//! `count`, `min`, `max` from which the output columns are reconstructed at
//! read time. "Rollup" is the ALGORITHM that computes and folds them (see
//! `factory::temporal_reduce`), not the thing stored here. The two are kept
//! distinct deliberately: the on-disk artifact is the partial-aggregate cache,
//! and the rollup is how it gets filled.
//!
//! Note that the on-disk directory prefix is still `merged_{cfg_hash}_{node}`.
//! That name is a compatibility contract with every deployed pond, so it is not
//! spelled like either concept and must not be changed casually.
//!
//! `temporal-reduce` downsamples a source series into per-resolution
//! aggregations. Without caching, every read recomputes a full
//! `GROUP BY date_bin` over the entire source history, so per-build cost grows
//! without bound as the pond ages.
//!
//! Each output resolution is stored as a small set of files under
//! `{POND}/cache/`: immutable *segments* covering closed bucket ranges, plus one
//! recomputed `hot.parquet` for the open window above the watermark. They hold
//! decomposable partials (`Sum`, `Count`, `Min`, `Max`, ...), so a read is a
//! cheap `GROUP BY time_bucket` merge and the output columns are reconstructed
//! at read time. A `manifest.json` names the members and the range each covers.
//!
//! This is the aggregation-tier analogue of [`crate::format_cache`], which
//! caches the *parsed leaves*. Together they make both the parse and the
//! aggregation incremental.
//!
//! Key properties:
//! - **Keyed by time range, not by source version.** The manifest records which
//!   bucket range each segment covers and which source CONTENT (blake3) the
//!   level reflects. Version numbers are not stable -- a tlogfs collapse
//!   rewrites N versions into one merged version with a new, higher number and
//!   identical content -- so keying on them made unchanged data look changed.
//! - **File count tracks data, not build frequency.** Sealing is gated on
//!   accumulated bytes ([`SEAL_TARGET_BYTES`]) and segments are compacted by the
//!   same size-tiered policy tlogfs uses ([`crate::size_tier`]), so a pond
//!   rebuilt every minute does not grow a file every minute.
//! - **Self-correcting.** Late data unseals the segments covering it and lets
//!   the ordinary seal path recompute them; nothing here is a dead end
//!   requiring `--rebuild`.
//! - **Throwaway.** `rm -rf {POND}/cache/` is always safe.
//! - **Config-namespaced.** A different aggregation set / time column /
//!   resolution list yields a fresh `cfg_hash` namespace rather than mixing
//!   semantics.

use datafusion::catalog::TableProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Result type for partial-aggregate cache operations.
type Result<T> = std::result::Result<T, crate::error::Error>;

/// Compute a stable namespace hash over the parts of a temporal-reduce config
/// that change the *meaning* of the cached aggregates: the aggregation set, the
/// time column, and the resolution list.
///
/// A config change yields a fresh namespace rather than mixing incompatible
/// aggregates into one directory.  Changing config invalidates semantics, not
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

/// Drop every partial-aggregate-cache namespace under `cache_dir`, forcing every
/// temporal-reduce level to be recomputed from source on next read.  This is the
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
        let is_partial_aggregate = path.is_dir()
            && entry
                .file_name()
                .to_str()
                .is_some_and(|n| n.starts_with("merged_"));
        if is_partial_aggregate {
            std::fs::remove_dir_all(&path).map_err(crate::error::Error::Io)?;
            dropped += 1;
        }
    }
    Ok(dropped)
}

// --- Merged-output cache: per-resolution materialized aggregation ------------
//
// Materializes each output resolution's merged buckets on disk, so a rebuild
// recomputes only the buckets a change can reach and reuses the rest.
//
// Integrity: a blake3 digest of each Parquet's bytes is recorded in the
// manifest. A file the manifest names but that is absent is a hard error --
// segments are published before the manifest that names them, so a missing one
// means the directory was damaged, not that a build was interrupted. The hot
// file is the exception: it is mutated in place, so its digest is re-verified
// against the manifest on every build and a disagreement rebuilds the
// resolution rather than serving a gap. See `read_verified_segment_manifest`.

/// Directory holding the merged-output cache for one temporal-reduce node and
/// config namespace: `{cache_dir}/merged_{cfg_hash}_{node_id}/`.
#[must_use]
pub fn merged_dir(cache_dir: &Path, cfg_hash: &str, node_id: &tinyfs::NodeID) -> PathBuf {
    cache_dir.join(format!("merged_{}_{}", cfg_hash, node_id))
}

// --- Segments: watermark + immutable segments ------------------------------
//
// Each resolution is a directory of immutable *segment* files plus one
// recomputed *hot* file, separated by a watermark derived from
// `allowed_lateness`:
//
//   {merged_dir}/res{secs}/
//       seg-00000000.parquet   immutable segment (buckets [lo, hi))
//       seg-00000001.parquet   ...
//       hot.parquet            every bucket at or above sealed_hi, recomputed
//                              each build
//       manifest.json          watermark + segment ranges + source content + digests
//
// Buckets below sealed_hi are frozen once and never rescanned, so per-build cost
// is bounded to the hot window: the allowed-lateness tail plus whatever has
// frozen since the last seal, which SEAL_TARGET_BYTES bounds. The alternative --
// one mutable Parquet whose sealed prefix is rewritten every build -- costs
// O(history) I/O per build.
//
// Because a segment is identified by the bucket range it covers, late data below the
// watermark is answerable: the segments holding those buckets are found by lookup,
// dropped, and recomputed by the ordinary seal path.
//
// The read provider names the manifest's members explicitly; it never lists the
// directory, so a superseded file left behind by an interrupted compaction
// cannot be double-counted. Consumers apply their own `ORDER BY`.

/// Minimum accumulated size, in bytes, before frozen buckets are sealed into a
/// segment file.
///
/// Sealing used to trigger on the watermark advancing, which tied the file count
/// to how often a build ran rather than to how much data existed: a pond rebuilt
/// every minute grew a segment file every minute, each a few kilobytes. Gating on
/// size decouples the two, so the count tracks data volume.
///
/// Deliberately modest. The cost of raising it is that the hot window -- which is
/// recomputed in full on every build -- carries the unsealed remainder, so the
/// target bounds per-build work. 1 MiB keeps that recompute trivial while still
/// collapsing hundreds of builds into one segment. Compaction is the right tool for
/// making files genuinely large; this only has to stop the bleeding.
pub const SEAL_TARGET_BYTES: u64 = 1024 * 1024;

/// The event-time span covered by one source version, in epoch MICROSECONDS.
///
/// Microseconds because that is the unit tlogfs records on `OplogEntry` and
/// surfaces as `min_event_time` / `max_event_time`, so for series sources this
/// is free -- no scan. Output-bucket ranges elsewhere in this module are epoch
/// SECONDS; conversion between the two must round outward (floor a lower bound,
/// ceil an upper one) so a rounding step can never exclude a contributing row.
///
/// `max` is INCLUSIVE: it is the largest event time present, not a half-open
/// end. Ranges elsewhere in this module are half-open, so the distinction is
/// called out here rather than assumed.
///
/// Only `min_us` drives the dirty range -- unsealing is governed by a single
/// watermark, so there is nothing an upper bound could exclude. `max_us` earns
/// its place in the comparison: a version whose bounds were absent and are
/// later recorded (an ingest fix, or a tlogfs upgrade backfilling them) keeps
/// its blake3 but changes its range, and that must read as changed so the
/// pessimistic full-axis range it had is replaced by the real one.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceRange {
    pub min_us: i64,
    pub max_us: i64,
}

impl SourceRange {
    /// A version whose event-time bounds were never recorded, covering the whole
    /// axis.
    ///
    /// Not a special case to test for downstream: any dirty range containing it
    /// becomes unbounded, so the build reads everything -- pessimistic, still
    /// correct. Non-series sources land here, which is why the pruning benefit
    /// is confined to series data, where the bounds are recorded for free.
    pub const UNKNOWN: Self = Self {
        min_us: i64::MIN,
        max_us: i64::MAX,
    };
}

/// Maximum number of segments to leave live in one resolution before
/// compaction merges regardless of size class.
///
/// This bounds read fan-out: every query opens every live segment plus hot, so the
/// count is what a reader pays. Size-tiered merging keeps the number near
/// `log(data)` on its own; this is the backstop for ragged inputs.
pub const MAX_LIVE_SEGMENTS: usize = 50;

/// One immutable segment file and the half-open output-bucket range it
/// covers, in epoch seconds. `lo_secs` is `None` for the genesis segment (unbounded
/// below); `hi_secs` is exclusive.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Segment {
    pub name: String,
    pub lo_secs: Option<i64>,
    pub hi_secs: i64,
    pub digest: String,
    /// On-disk size of the segment file in bytes, recorded when it was written.
    /// Lets compaction choose merge candidates without stat-ing every segment.
    pub bytes: u64,
}

/// On-disk format of the segment + hot files. Bumped when their column
/// layout or the manifest's build semantics change so an older cache is
/// discarded and rebuilt rather than misread. `segments-v3` stores the
/// *mergeable partials* (sum/count/min/max) with output columns reconstructed at
/// read time, keys the finest resolution on source CONTENT (`sources`) rather
/// than on per-version partial filenames, and may derive a coarser resolution by
/// folding the next-finer resolution's segments (`source_digest`). Older caches
/// carry a different (or empty) `format` and are wiped + rebuilt.
pub const SEALED_FORMAT: &str = "segments-v3";

/// Manifest describing the segment cache for one output resolution. Its
/// serialized bytes are the export-hint digest, so it must serialize
/// deterministically (fixed field order; `sources` is an ordered map).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct SegmentManifest {
    /// On-disk file format (see [`SEALED_FORMAT`]). Absent in legacy manifests,
    /// which deserialize to `""` and are treated as a format mismatch (rebuild).
    #[serde(default)]
    pub format: String,
    /// The `allowed_lateness` (seconds) this cache was built under. A build with
    /// a different value discards and rebuilds the res dir.
    pub allowed_lateness_secs: i64,
    /// Exclusive upper bound (epoch seconds) of the sealed region: every output
    /// bucket with start `< sealed_hi_secs` lives in some segment. `None` before any
    /// bucket has been sealed.
    pub sealed_hi_secs: Option<i64>,
    /// Next segment sequence number for file naming.
    pub next_seq: u64,
    /// Sealed segment files in ascending bucket order.
    pub segments: Vec<Segment>,
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
    /// The source content this cache reflects: blake3 of each LIVE source
    /// version, mapped to the event-time range it covers.
    ///
    /// Recorded by EVERY resolution, not just the finest. The finest uses it to
    /// decide reuse-vs-incremental directly. A coarser resolution keys reuse on
    /// `source_digest`, but still needs this to bound how far back to unseal
    /// when that digest is stale: its finer level's in-call change may already
    /// have been consumed by an earlier build of a different output file, so
    /// "the finer level reports no change" does not mean this level is current.
    /// See `build_level_from_finer`.
    ///
    /// Keying on CONTENT rather than on storage artifacts is the point. The
    /// previous key was a set of partial FILENAMES, one per source version, and
    /// version numbers are not stable: a tlogfs collapse rewrites N versions
    /// into one merged version with a new, higher number and identical content.
    /// Every guard this cache used to need -- the sequentiality frontier, the
    /// series/non-series split, the backfill hard errors -- existed to defend a
    /// key that changed when the data had not. A blake3 does not.
    ///
    /// Bounded: tlogfs collapse bounds the number of live versions, whereas a
    /// directory holding one file per version ever written was unbounded.
    pub sources: BTreeMap<String, SourceRange>,
    /// For a coarser resolution built by folding the next-finer resolution's
    /// segments (Phase 3 step 2): the finer resolution's manifest digest this cache
    /// was folded from. `None` for the finest resolution (which aggregates the
    /// sources directly and uses `sources`). A change in the finer digest
    /// triggers a coarse rebuild/advance.
    #[serde(default)]
    pub source_digest: Option<String>,
}

/// Per-resolution segment directory: `{merged_dir}/res{interval_secs}/`.
#[must_use]
pub fn segment_res_dir(merged_dir: &Path, interval_secs: u64) -> PathBuf {
    merged_dir.join(format!("res{}", interval_secs))
}

fn segment_manifest_path(res_dir: &Path) -> PathBuf {
    res_dir.join("manifest.json")
}

/// Path of the recomputed hot-window Parquet in a res dir.
#[must_use]
pub fn hot_path(res_dir: &Path) -> PathBuf {
    res_dir.join("hot.parquet")
}

/// Path of a segment file by name in a res dir.
#[must_use]
pub fn segment_path(res_dir: &Path, name: &str) -> PathBuf {
    res_dir.join(name)
}

/// Read the segment manifest. `Ok(None)` when absent (fresh cache). A
/// present-but-unparseable manifest is a hard error rather than a silent
/// rebuild, matching the frontier/merged-cache corruption discipline.
pub fn read_segment_manifest(res_dir: &Path) -> Result<Option<SegmentManifest>> {
    let path = segment_manifest_path(res_dir);
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(crate::error::Error::Io(e)),
    };
    serde_json::from_slice(&bytes).map(Some).map_err(|e| {
        crate::error::Error::CacheCorrupt(format!(
            "segment manifest '{}' is present but unparseable ({}); the \
             partial-aggregate cache is corrupt. Re-run the export with \
             --rebuild to recompute it.",
            path.display(),
            e
        ))
    })
}

/// [`read_segment_manifest`], discarding a manifest that disagrees with the hot
/// file on disk.
///
/// `hot.parquet` is the one file in the layout that is MUTATED IN PLACE: each
/// build renames a freshly merged hot over the previous one, and only then
/// writes the manifest recording the watermark that hot was built for. A crash
/// in that window leaves a hot holding only `bucket >= new_wm` alongside a
/// manifest still claiming `sealed_hi = old_wm`, so the buckets between them
/// live in no member of the cache. Nothing else detects this: the read path
/// checks only that the named files EXIST, and if the sources are unchanged the
/// next build reuses the manifest and serves the hole indefinitely.
///
/// Treating the disagreement as "no usable manifest" makes it self-healing --
/// the caller rebuilds the resolution from source -- at the cost of one blake3
/// of hot per build, which is bounded by the seal target and is rewritten by
/// that same build anyway.
///
/// Segments need no such check: they are immutable, published under a fresh
/// name BEFORE the manifest that names them, and deleted only after it is
/// durable, so a crash can only orphan a file that no manifest references.
pub fn read_verified_segment_manifest(res_dir: &Path) -> Result<Option<SegmentManifest>> {
    let Some(m) = read_segment_manifest(res_dir)? else {
        return Ok(None);
    };
    let hot = hot_path(res_dir);
    match (&m.hot_digest, hot.exists()) {
        (Some(recorded), true) => {
            let actual = crate::version_cache::file_blake3(&hot)?;
            if &actual == recorded {
                Ok(Some(m))
            } else {
                log::warn!(
                    "[ROLLUP] hot file '{}' does not match the digest recorded in \
                     the manifest; rebuilding this resolution (a build was \
                     interrupted between publishing hot and its manifest)",
                    hot.display()
                );
                Ok(None)
            }
        }
        // No hot yet, or a manifest that predates the digest: nothing to check
        // against, and the caller's existence checks still apply.
        _ => Ok(Some(m)),
    }
}

/// Compute the export-hint digest of a manifest without writing it (used on the
/// reuse path, where the res dir is served unchanged).
pub fn segment_manifest_digest(manifest: &SegmentManifest) -> Result<String> {
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|e| crate::error::Error::Arrow(format!("serialize sealed manifest: {}", e)))?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

/// Persist the segment manifest atomically (tmp + rename) and return the
/// blake3 digest of its serialized bytes, which callers use as the export-hint
/// digest for the whole resolution.
pub async fn write_segment_manifest(res_dir: &Path, manifest: &SegmentManifest) -> Result<String> {
    tokio::fs::create_dir_all(res_dir).await?;
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|e| crate::error::Error::Arrow(format!("serialize sealed manifest: {}", e)))?;
    let digest = blake3::hash(&bytes).to_hex().to_string();
    let path = segment_manifest_path(res_dir);
    let tmp = PathBuf::from(format!("{}.tmp", path.display()));
    tokio::fs::write(&tmp, &bytes).await?;
    tokio::fs::rename(&tmp, &path).await?;
    Ok(digest)
}

/// Delete segment files that compaction superseded.
///
/// MUST be called only after the manifest naming their replacement has been
/// durably written. Publishing the merged segment first and deleting its inputs
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
                "rollup compaction: could not remove superseded segment {}: {e}",
                f.display()
            );
        }
    }
}

/// Remove an entire res dir, forcing this resolution to be rebuilt from source
/// (used when `allowed_lateness` or the on-disk format changes).
pub fn wipe_segment_res_dir(res_dir: &Path) -> Result<()> {
    match std::fs::remove_dir_all(res_dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(crate::error::Error::Io(e)),
    }
}

/// Build the read provider for a res dir: a `ListingTable` over exactly the
/// segments named by `manifest`, plus the hot file. A manifest member
/// missing on disk is a hard error (corrupt cache), never a silent hole in the
/// series.
///
/// The scan names its files EXPLICITLY rather than listing `res_dir`. The two
/// are not the same set: a `.parquet` can sit in the directory without being
/// referenced by the manifest, and every such orphan is a duplicate copy of
/// some bucket range that a directory-shaped scan sums a second time. Orphans
/// arise in normal operation, not only after a crash -- [`crate::version_cache::write_parquet_atomic`]
/// renames a new segment into place BEFORE the manifest recording it is written, so
/// any interruption in that window leaves one behind, and compaction (which must
/// publish a merged segment before deleting its inputs) widens that window by
/// design. This is the same defect that let a directory-listed format cache
/// double-count the versions a collapse superseded; the fix is the same one.
pub async fn listing_table_for_res_dir(
    res_dir: &Path,
    manifest: &SegmentManifest,
    ts_column: &str,
) -> Result<Arc<dyn TableProvider>> {
    let mut files: Vec<PathBuf> = Vec::with_capacity(manifest.segments.len() + 1);
    for seg in &manifest.segments {
        let p = segment_path(res_dir, &seg.name);
        if !p.exists() {
            return Err(crate::error::Error::CacheCorrupt(format!(
                "segment '{}' recorded in the manifest is missing on disk; the \
                 partial-aggregate cache is corrupt. Re-run the export with --rebuild.",
                p.display()
            )));
        }
        files.push(p);
    }
    let hot = hot_path(res_dir);
    if !hot.exists() {
        return Err(crate::error::Error::CacheCorrupt(format!(
            "hot file '{}' is missing; the partial-aggregate cache is corrupt. \
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
/// Compaction needs the same guarantee over a subset -- it reads only the segments
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
    // Every segment and the hot file is written by a query ending in
    // `ORDER BY time_bucket`, so each file is individually sorted ascending on
    // the timestamp column, and the segments' bucket ranges are disjoint. Declaring
    // that file-level ordering lets the physical planner satisfy a consumer's
    // `ORDER BY {ts}` with a streaming `SortPreservingMergeExec` (k-way merge of
    // the already-sorted segments + hot) instead of a `SortExec` that buffers the
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

    /// The on-disk namespace is a compatibility contract with every deployed
    /// pond: `merged_{cfg_hash}_{node_id}`. Renaming this module and its
    /// functions to "partial-aggregate" must not move a single byte of it --
    /// a perturbed `cfg_hash` orphans every existing cache directory, and the
    /// symptom is not an error but a silent full recompute that then looks
    /// like the cache simply never helped.
    ///
    /// `test_cfg_hash_stable_and_distinct` only checks self-consistency, so it
    /// passes happily after such a change. This pins the actual bytes.
    ///
    /// It also pins something the rename did not introduce: `cfg_hash` uses
    /// `DefaultHasher`, whose output std explicitly does NOT guarantee across
    /// toolchains. If a Rust upgrade ever changes it, every deployed pond
    /// silently re-derives its whole cache. This test is the tripwire.
    #[test]
    fn merged_dir_naming_is_pinned() {
        assert_eq!(
            cfg_hash("avg|timestamp|1h,1d"),
            "f25d1ac9a335b8ea",
            "cfg_hash bytes are a deployed-pond compatibility contract"
        );
        let node = tinyfs::NodeID::new("0192f00d-0000-7000-8000-00000000abcd".to_string());
        assert_eq!(
            merged_dir(Path::new("/pond/cache"), "cafef00d12345678", &node)
                .to_string_lossy()
                .as_ref(),
            format!("/pond/cache/merged_cafef00d12345678_{node}").as_str(),
            "directory prefix must stay `merged_`"
        );
    }

    /// A stray Parquet in the res dir must not contribute rows: the manifest,
    /// not the directory listing, defines what the scan reads.
    ///
    /// Orphans are ordinary, not exotic. `write_parquet_atomic` renames a segment
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

        // One segment covering buckets [0, 120) and the open hot file.
        let seg_name = "seg-00000000.parquet";
        let seg_digest = put(
            &segment_path(&res_dir, seg_name),
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

        // The orphan: a duplicate of the segment under a name the manifest
        // never records, as an interrupted seal or an in-flight compaction
        // would leave behind.
        _ = put(
            &segment_path(&res_dir, "seg-00000001.parquet"),
            schema.clone(),
            partials_batch(&schema, &[0, 60], &[10.0, 20.0], &[1, 2]),
        )
        .await;

        let manifest = SegmentManifest {
            format: SEALED_FORMAT.to_string(),
            allowed_lateness_secs: 0,
            sealed_hi_secs: Some(120),
            next_seq: 1,
            segments: vec![Segment {
                name: seg_name.to_string(),
                lo_secs: None,
                hi_secs: 120,
                digest: seg_digest,
                bytes: 0,
            }],
            hot_digest: Some(hot_digest),
            hot_bytes: 0,
            sources: BTreeMap::new(),
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

    /// The hot file is mutated in place BEFORE the manifest recording the
    /// watermark it was built for. A crash in that window leaves the two
    /// disagreeing and the buckets between the old and new watermarks in no
    /// member of the cache -- served forever, because an unchanged source makes
    /// the next build reuse. The manifest must therefore be rejected when hot
    /// does not match the digest it recorded.
    #[tokio::test]
    async fn a_hot_file_that_disagrees_with_its_manifest_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let res_dir = tmp.path().to_path_buf();
        std::fs::create_dir_all(&res_dir).unwrap();
        std::fs::write(hot_path(&res_dir), b"hot bytes").unwrap();
        let digest = crate::version_cache::file_blake3(&hot_path(&res_dir)).unwrap();

        let m = SegmentManifest {
            format: SEALED_FORMAT.to_string(),
            hot_digest: Some(digest),
            ..Default::default()
        };
        _ = write_segment_manifest(&res_dir, &m).await.unwrap();

        assert!(
            read_verified_segment_manifest(&res_dir).unwrap().is_some(),
            "a matching hot file keeps the manifest usable"
        );

        // Simulate the crash: hot advanced, the manifest did not.
        std::fs::write(hot_path(&res_dir), b"a newer hot, published first").unwrap();
        assert!(
            read_verified_segment_manifest(&res_dir).unwrap().is_none(),
            "a hot file that does not match its manifest must force a rebuild"
        );
        // The manifest itself is still readable: this is a rebuild, not corruption.
        assert!(read_segment_manifest(&res_dir).unwrap().is_some());
    }

    /// An absent manifest and an unparseable one mean opposite things, and the
    /// difference must survive the read: absent is a cold cache (build it), while
    /// present-but-garbage means something wrote or truncated a file this code
    /// owns. Collapsing the latter into `Ok(None)` would look like a harmless
    /// rebuild while quietly discarding a res dir whose segments are still
    /// referenced -- corruption that repairs itself into data loss.
    ///
    /// The check that matters is on `read_verified_segment_manifest`, the
    /// wrapper the build path actually calls: it converts a hot-file
    /// disagreement into `Ok(None)` (a legitimate rebuild), so it must NOT do
    /// the same to a corrupt manifest.
    #[tokio::test]
    async fn an_unparseable_manifest_is_corruption_not_a_cold_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let res_dir = tmp.path().to_path_buf();
        std::fs::create_dir_all(&res_dir).unwrap();

        // Absent: a cold cache, not an error.
        assert!(
            read_segment_manifest(&res_dir).unwrap().is_none(),
            "no manifest yet is a cold cache"
        );

        // Truncated mid-write, as an interrupted writer would leave it. The
        // atomic tmp+rename in `write_segment_manifest` is what prevents this,
        // so reaching it means that guarantee was broken from outside.
        std::fs::write(
            res_dir.join("manifest.json"),
            b"{\"format\":\"partials-v2\",",
        )
        .unwrap();

        let err = read_segment_manifest(&res_dir)
            .expect_err("a present but unparseable manifest is corruption");
        assert!(
            matches!(err, crate::error::Error::CacheCorrupt(_)),
            "must be CacheCorrupt, not an IO or serde error: {err:?}"
        );
        assert!(
            err.to_string().contains("--rebuild"),
            "the message must tell the operator how to recover: {err}"
        );

        let err = read_verified_segment_manifest(&res_dir)
            .expect_err("the verifying wrapper must not swallow corruption into a rebuild");
        assert!(
            matches!(err, crate::error::Error::CacheCorrupt(_)),
            "{err:?}"
        );
    }

    /// The mirror of `listing_table_ignores_a_parquet_the_manifest_does_not_name`:
    /// a file the manifest does not name is ignored, but a file it DOES name and
    /// that is absent is a hard error. Both rules exist to make the manifest the
    /// single authority on membership -- an extra file must not be summed, and a
    /// missing one must not become a silent hole in the series.
    ///
    /// Skipping the missing member would be the more dangerous half: the gap
    /// lands in whatever bucket range that segment held, the manifest still
    /// agrees with itself, and an unchanged source makes every later build reuse
    /// it, so the hole is served indefinitely.
    #[tokio::test]
    async fn listing_table_rejects_a_manifest_member_missing_from_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let res_dir = tmp.path().join("res60");
        let schema = partials_schema();
        let s: BatchStream = Box::pin(futures::stream::iter(vec![Ok(partials_batch(
            &schema,
            &[120],
            &[40.0],
            &[4],
        ))]));
        let hot_digest = write_parquet_atomic(&hot_path(&res_dir), schema.clone(), s)
            .await
            .unwrap();

        // The manifest names a segment that was never written (or was deleted
        // out from under the cache).
        let manifest = SegmentManifest {
            format: SEALED_FORMAT.to_string(),
            allowed_lateness_secs: 0,
            sealed_hi_secs: Some(120),
            next_seq: 1,
            segments: vec![Segment {
                name: "seg-00000000.parquet".to_string(),
                lo_secs: None,
                hi_secs: 120,
                digest: "0".repeat(64),
                bytes: 0,
            }],
            hot_digest: Some(hot_digest),
            hot_bytes: 0,
            sources: BTreeMap::new(),
            source_digest: None,
        };

        let err = listing_table_for_res_dir(&res_dir, &manifest, "time_bucket")
            .await
            .expect_err("a manifest member missing on disk must not be skipped");
        assert!(
            matches!(err, crate::error::Error::CacheCorrupt(_)),
            "must be CacheCorrupt so the operator sees a corrupt cache rather \
             than a short read: {err:?}"
        );
        assert!(
            err.to_string().contains("seg-00000000.parquet"),
            "the message must name the missing member: {err}"
        );
    }

    /// The hot file is a mandatory member of every res dir -- it holds the open
    /// window `[sealed_hi, inf)`, i.e. the most recent data. Reading the sealed
    /// segments alone would succeed and return a series that simply stops at the
    /// watermark, which looks like "no recent data" rather than like a fault.
    #[tokio::test]
    async fn listing_table_rejects_a_res_dir_with_no_hot_file() {
        let tmp = tempfile::tempdir().unwrap();
        let res_dir = tmp.path().join("res60");
        let schema = partials_schema();
        let seg_name = "seg-00000000.parquet";
        let s: BatchStream = Box::pin(futures::stream::iter(vec![Ok(partials_batch(
            &schema,
            &[0, 60],
            &[10.0, 20.0],
            &[1, 2],
        ))]));
        let seg_digest = write_parquet_atomic(&segment_path(&res_dir, seg_name), schema.clone(), s)
            .await
            .unwrap();

        let manifest = SegmentManifest {
            format: SEALED_FORMAT.to_string(),
            allowed_lateness_secs: 0,
            sealed_hi_secs: Some(120),
            next_seq: 1,
            segments: vec![Segment {
                name: seg_name.to_string(),
                lo_secs: None,
                hi_secs: 120,
                digest: seg_digest,
                bytes: 0,
            }],
            hot_digest: None,
            hot_bytes: 0,
            sources: BTreeMap::new(),
            source_digest: None,
        };

        let err = listing_table_for_res_dir(&res_dir, &manifest, "time_bucket")
            .await
            .expect_err("a res dir without hot must not read as a truncated series");
        assert!(
            matches!(err, crate::error::Error::CacheCorrupt(_)),
            "{err:?}"
        );
        assert!(
            err.to_string().contains("hot"),
            "the message must identify the missing hot file: {err}"
        );
    }

    #[tokio::test]
    async fn test_merged_cache_drop_all_removes_merged_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let node_id = test_node_id();
        // Materialize a segment res dir so drop_all has a merged_* dir to
        // remove.
        let res_dir = segment_res_dir(&merged_dir(tmp.path(), "cfg", &node_id), 60);
        let manifest = SegmentManifest {
            allowed_lateness_secs: 86400,
            ..Default::default()
        };
        let _ = write_segment_manifest(&res_dir, &manifest).await.unwrap();
        assert!(res_dir.exists());
        let dropped = drop_all(tmp.path()).unwrap();
        assert!(dropped >= 1);
        assert!(!res_dir.exists());
    }
}
