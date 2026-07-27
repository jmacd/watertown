// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Per-version Parquet sidecar caches.
//!
//! Both [`crate::format_cache`] and [`crate::rollup_cache`] maintain the same
//! thing: a directory holding one Parquet sidecar per *version* of a node,
//! keyed by the version's content hash, written once and reused until the node
//! changes. That shape was copy-pasted rather than named, and the cost was a
//! silent corruption -- when the double-counting fix landed in one copy it
//! could not reach the other (testsuite 731 vs 733).
//!
//! This module names it once. The design goal is that the bug class is not
//! merely fixed but *unwritable*, via three barriers:
//!
//! 1. [`LiveVersions`] -- a version list is not a bare `Vec`; it is a
//!    node-scoped set that its constructor documents as live. "Did the caller
//!    pass the right list?" becomes a type question rather than a review one.
//!
//! 2. [`SidecarDir::reconcile`] -- reconciliation both ADDS missing sidecars
//!    and REMOVES stale ones, so the directory is a pure projection of the live
//!    set. Caches that only ever add are how a superseded version's sidecar
//!    survives to be read a second time.
//!
//! 3. [`CachedSet`] -- the only route to a `TableProvider`. It names its files
//!    explicitly and can only be obtained from a [`LiveVersions`]. There is
//!    deliberately no `fn(&Path) -> TableProvider` in this module, because
//!    every instance of the bug has been exactly that function.
//!
//! Barriers 2 and 3 are independent on purpose: with reconciliation alone a
//! stray directory listing would still be correct, and with explicit paths
//! alone the disk would still leak.
//!
//! ## Why collapse breaks the naive version
//!
//! Before version collapse these two sets were always equal:
//!
//! > {every sidecar ever written} == {sidecars of currently live versions}
//!
//! Collapse merges a run of versions into one new version that *stands in for*
//! them without deleting them, so the equality no longer holds. A cache that
//! lists its directory then returns both the merged sidecar and the superseded
//! ones it replaces -- double-counting every row in the collapsed window, and
//! again on each later collapse.

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use futures::{Stream, StreamExt};
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use tinyfs::FileVersionInfo;

type Result<T> = std::result::Result<T, crate::error::Error>;

/// A batch stream of the shape both caches write.
pub type BatchStream =
    Pin<Box<dyn Stream<Item = std::result::Result<RecordBatch, crate::error::Error>> + Send>>;

/// The set of versions of one node that are currently **live** -- i.e. not
/// superseded by a collapse run.
///
/// Exists so that "these are the live versions" is carried by the type rather
/// than assumed. Every cache entry point takes this instead of a bare slice,
/// which is what stops a historical or unfiltered list from reaching a cache.
///
/// The live set is decided in tlogfs (`live_series_entries`) and surfaces here
/// through `list_file_versions`; this type does not re-derive it, it only
/// prevents an arbitrary `Vec` from being mistaken for it.
#[derive(Debug, Clone)]
pub struct LiveVersions {
    node_id: tinyfs::NodeID,
    versions: Vec<FileVersionInfo>,
}

impl LiveVersions {
    /// Build from the result of a `list_file_versions` call.
    ///
    /// Named for its obligation: `versions` must be the node's live set, as
    /// returned by the persistence layer, not a filtered or remembered subset.
    /// Pruning for a query's bounds happens later, in [`SidecarDir::cached_set`],
    /// so that reconciliation still sees the whole live set and does not delete
    /// sidecars merely because this query did not need them.
    #[must_use]
    pub fn from_persistence(node_id: tinyfs::NodeID, versions: Vec<FileVersionInfo>) -> Self {
        Self { node_id, versions }
    }

    #[must_use]
    pub fn node_id(&self) -> &tinyfs::NodeID {
        &self.node_id
    }

    #[must_use]
    pub fn as_slice(&self) -> &[FileVersionInfo] {
        &self.versions
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.versions.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.versions.len()
    }
}

/// How sidecar files are named within a directory.
///
/// The distinction is load-bearing for deletion: in a [`Self::NodeScoped`]
/// directory, files belonging to *other* nodes are legitimately present, so
/// reconciliation must never treat them as stale.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SidecarNaming {
    /// `v{version}_{key}.parquet`. The directory belongs to a single node, so
    /// every Parquet in it is that node's.
    NodeOwned,
    /// `{node_id}_v{version}_{key}.parquet`. The directory is shared by every
    /// node a pattern matched -- one rotated log file per node, each with its
    /// own versions.
    NodeScoped,
}

/// Cache key for a version: its blake3 content hash, or -- for dynamic inputs
/// that carry no blake3 -- the node's short id, which is itself content
/// derived (e.g. a git blob OID).
#[must_use]
fn version_key(node_id: &tinyfs::NodeID, version: &FileVersionInfo) -> String {
    match version.blake3.as_deref() {
        Some(hash) => hash.to_string(),
        None => node_id.to_short_string(),
    }
}

/// A directory of per-version Parquet sidecars.
#[derive(Debug, Clone)]
pub struct SidecarDir {
    dir: PathBuf,
    naming: SidecarNaming,
}

/// What [`SidecarDir::reconcile`] found and did.
#[derive(Debug, Default)]
pub struct Reconciliation {
    /// Live versions with no sidecar yet; the caller must write these.
    pub missing: Vec<FileVersionInfo>,
    /// Sidecars deleted because their version is no longer live.
    pub removed: Vec<PathBuf>,
}

impl SidecarDir {
    #[must_use]
    pub fn new(dir: PathBuf, naming: SidecarNaming) -> Self {
        Self { dir, naming }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.dir
    }

    /// Path of one version's sidecar.
    #[must_use]
    pub fn sidecar_path(&self, node_id: &tinyfs::NodeID, version: &FileVersionInfo) -> PathBuf {
        let key = version_key(node_id, version);
        let name = match self.naming {
            SidecarNaming::NodeOwned => format!("v{}_{}.parquet", version.version, key),
            SidecarNaming::NodeScoped => {
                format!("{}_v{}_{}.parquet", node_id, version.version, key)
            }
        };
        self.dir.join(name)
    }

    /// Whether `path` is a sidecar this node owns.
    ///
    /// For [`SidecarNaming::NodeScoped`] this is the guard that keeps
    /// reconciliation from deleting a sibling node's sidecars out of a shared
    /// directory.
    fn owned_by(&self, node_id: &tinyfs::NodeID, path: &Path) -> bool {
        if path.extension().and_then(|e| e.to_str()) != Some("parquet") {
            return false; // ignore .tmp and anything else
        }
        match self.naming {
            SidecarNaming::NodeOwned => true,
            SidecarNaming::NodeScoped => path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(&format!("{}_v", node_id))),
        }
    }

    /// Live versions that have no sidecar on disk yet.
    #[must_use]
    pub fn missing(&self, live: &LiveVersions) -> Vec<FileVersionInfo> {
        live.as_slice()
            .iter()
            .filter(|v| !self.sidecar_path(live.node_id(), v).exists())
            .cloned()
            .collect()
    }

    /// Sidecars of this node that no longer correspond to a live version.
    ///
    /// After a version collapse these are the superseded versions' sidecars:
    /// still on disk, no longer part of the series, and counted a second time
    /// by anything that lists the directory.
    pub fn stale(&self, live: &LiveVersions) -> Result<Vec<PathBuf>> {
        if !self.dir.exists() {
            return Ok(Vec::new());
        }
        let keep: std::collections::HashSet<PathBuf> = live
            .as_slice()
            .iter()
            .map(|v| self.sidecar_path(live.node_id(), v))
            .collect();

        let mut stale = Vec::new();
        for entry in std::fs::read_dir(&self.dir).map_err(crate::error::Error::Io)? {
            let path = entry.map_err(crate::error::Error::Io)?.path();
            if self.owned_by(live.node_id(), &path) && !keep.contains(&path) {
                stale.push(path);
            }
        }
        stale.sort();
        Ok(stale)
    }

    /// Make the directory a projection of `live`: delete this node's stale
    /// sidecars and report which live versions still need writing.
    ///
    /// Deleting is safe because a sidecar is derived data, regenerable from the
    /// source version. Doing it here rather than leaving it to a separate
    /// sweep is the point: a cache that only adds silently accumulates the
    /// superseded sidecars that produce double-counted reads.
    pub fn reconcile(&self, live: &LiveVersions) -> Result<Reconciliation> {
        let mut removed = Vec::new();
        for path in self.stale(live)? {
            std::fs::remove_file(&path).map_err(crate::error::Error::Io)?;
            log::debug!("[SWEEP] sidecar cache: removed stale {}", path.display());
            removed.push(path);
        }
        Ok(Reconciliation {
            missing: self.missing(live),
            removed,
        })
    }

    /// The live versions' sidecars that exist on disk, as an explicit file set.
    ///
    /// `retain` prunes for a query's bounds; it does not affect what
    /// [`Self::reconcile`] considers live.
    #[must_use]
    pub fn cached_set(
        &self,
        live: &LiveVersions,
        retain: impl Fn(&FileVersionInfo) -> bool,
    ) -> CachedSet {
        let files = live
            .as_slice()
            .iter()
            .filter(|v| retain(v))
            .map(|v| self.sidecar_path(live.node_id(), v))
            .filter(|p| p.exists())
            .collect();
        CachedSet {
            files,
            fallback_dir: self.dir.clone(),
        }
    }

    /// Write one version's sidecar, streaming batches to disk.
    ///
    /// Atomic (`.tmp` then rename), so a crash never leaves a truncated file
    /// that a later run would mistake for a complete cached version.
    pub async fn write_sidecar(
        &self,
        node_id: &tinyfs::NodeID,
        version: &FileVersionInfo,
        schema: SchemaRef,
        stream: BatchStream,
    ) -> Result<PathBuf> {
        tokio::fs::create_dir_all(&self.dir).await?;
        let final_path = self.sidecar_path(node_id, version);
        write_parquet_stream(&final_path, schema, stream).await?;
        log::debug!("[SAVE] sidecar cache: wrote {}", final_path.display());
        Ok(final_path)
    }
}

/// An explicit set of cached Parquet files for a node's live versions.
///
/// The only way to reach a `TableProvider` in this module. It carries file
/// paths, never a directory to list, so a superseded version's sidecar cannot
/// re-enter a read.
#[derive(Debug, Clone)]
pub struct CachedSet {
    files: Vec<PathBuf>,
    /// Used only to recover a schema when no live sidecar is present yet.
    fallback_dir: PathBuf,
}

impl CachedSet {
    #[must_use]
    pub fn files(&self) -> &[PathBuf] {
        &self.files
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// Absorb another node's cached set from the same directory.
    ///
    /// A [`SidecarNaming::NodeScoped`] directory is read as one table spanning
    /// every source node, so the read side unions the per-node sets rather than
    /// listing the directory. Each set is still derived from its own node's
    /// live versions, which is what keeps a superseded sidecar out.
    pub fn extend(&mut self, other: CachedSet) {
        self.files.extend(other.files);
    }

    /// An empty set that falls back to `dir` for schema recovery.
    #[must_use]
    pub fn empty_in(dir: PathBuf) -> Self {
        Self {
            files: Vec::new(),
            fallback_dir: dir,
        }
    }

    /// Build a `ListingTable` over exactly these files.
    ///
    /// Schemas are merged across them (UNION-ALL-BY-NAME), so a column that
    /// appears only in newer versions is present and back-filled with NULLs for
    /// older ones. The merge is scoped to the retained files for the same
    /// reason the scan is: a superseded version's schema must not shape the read.
    pub async fn table_provider(&self) -> Result<Arc<dyn TableProvider>> {
        let merged_schema = merge_parquet_schemas(&self.files, &self.fallback_dir).await?;

        if self.files.is_empty() {
            // Everything pruned, or nothing cached yet: an empty table over the
            // cache schema, so the query yields 0 rows rather than erroring or
            // falling back to scanning a directory.
            let table = datafusion::datasource::MemTable::try_new(merged_schema, vec![vec![]])?;
            return Ok(Arc::new(table));
        }

        let mut paths = Vec::with_capacity(self.files.len());
        for p in &self.files {
            paths.push(ListingTableUrl::parse(format!("file://{}", p.display()))?);
        }
        let listing_options =
            ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");
        let config = ListingTableConfig::new_with_multi_paths(paths)
            .with_listing_options(listing_options)
            .with_schema(merged_schema);
        Ok(Arc::new(ListingTable::try_new(config)?))
    }
}

/// Stream `stream` into a Parquet file at `path`, atomically.
pub async fn write_parquet_stream(
    path: &Path,
    schema: SchemaRef,
    mut stream: BatchStream,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp_path = path.with_extension("parquet.tmp");
    let file = tokio::fs::File::create(&tmp_path).await?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(parquet::basic::ZstdLevel::default()))
        .build();
    let mut writer = AsyncArrowWriter::try_new(file, schema, Some(props))
        .map_err(|e| crate::error::Error::Arrow(e.to_string()))?;

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        writer
            .write(&batch)
            .await
            .map_err(|e| crate::error::Error::Arrow(e.to_string()))?;
    }
    let _metadata = writer
        .close()
        .await
        .map_err(|e| crate::error::Error::Arrow(e.to_string()))?;

    tokio::fs::rename(&tmp_path, path).await?;
    Ok(())
}

/// Read and merge the Arrow schemas of an explicit list of Parquet files.
///
/// When `files` is empty there is no live sidecar to describe the data, so fall
/// back to whatever `fallback_dir` holds -- the caller still needs a usable
/// schema for an empty table.
pub async fn merge_parquet_schemas(files: &[PathBuf], fallback_dir: &Path) -> Result<SchemaRef> {
    let mut schemas = Vec::with_capacity(files.len());
    for path in files {
        schemas.push(read_parquet_schema(path).await?);
    }
    if schemas.is_empty() {
        return merge_parquet_schemas_in_dir(fallback_dir).await;
    }
    Ok(Arc::new(Schema::try_merge(schemas).map_err(|e| {
        crate::error::Error::Arrow(format!("Failed to merge parquet schemas: {e}"))
    })?))
}

/// Merge the schemas of every `.parquet` directly under `dir`.
///
/// Only for schema recovery and for genuinely directory-shaped caches (the
/// symlink glob dir, which is rebuilt from the live set on every query). Never
/// use it to decide which files a read scans.
pub async fn merge_parquet_schemas_in_dir(dir: &Path) -> Result<SchemaRef> {
    let mut schemas = Vec::new();
    if dir.exists() {
        let mut entries = tokio::fs::read_dir(dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
                schemas.push(read_parquet_schema(&path).await?);
            }
        }
    }
    if schemas.is_empty() {
        return Err(crate::error::Error::Arrow(format!(
            "No cached parquet files found in '{}'",
            dir.display()
        )));
    }
    Ok(Arc::new(Schema::try_merge(schemas).map_err(|e| {
        crate::error::Error::Arrow(format!("Failed to merge parquet schemas: {e}"))
    })?))
}

async fn read_parquet_schema(path: &Path) -> Result<Schema> {
    let file = tokio::fs::File::open(path).await?;
    let reader = parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(file)
        .await
        .map_err(|e| {
            crate::error::Error::Arrow(format!(
                "Failed to read parquet metadata from '{}': {e}",
                path.display()
            ))
        })?;
    Ok(reader.schema().as_ref().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tinyfs::EntryType;

    fn node(n: u64) -> tinyfs::NodeID {
        tinyfs::NodeID::new(format!("00000000-0000-7000-8000-{n:012x}"))
    }

    fn version(v: u64, blake3: Option<&str>) -> FileVersionInfo {
        FileVersionInfo {
            version: v,
            timestamp: 1_700_000_000_000_000,
            size: 128,
            blake3: blake3.map(str::to_string),
            entry_type: EntryType::FilePhysicalSeries,
            extended_metadata: None,
        }
    }

    fn touch(path: &Path) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"x").unwrap();
    }

    #[test]
    fn missing_reports_live_versions_without_sidecars() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = SidecarDir::new(tmp.path().to_path_buf(), SidecarNaming::NodeOwned);
        let live = LiveVersions::from_persistence(
            node(1),
            vec![version(1, Some("aaa")), version(2, Some("bbb"))],
        );

        assert_eq!(dir.missing(&live).len(), 2);
        touch(&dir.sidecar_path(live.node_id(), &live.as_slice()[0]));
        let missing = dir.missing(&live);
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].version, 2);
    }

    /// The collapse case: versions 1..=3 are superseded by a merged version 4,
    /// so `list_file_versions` now returns only v4. The three superseded
    /// sidecars must be swept, or a directory listing counts their rows twice.
    #[test]
    fn reconcile_removes_sidecars_of_superseded_versions() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = SidecarDir::new(tmp.path().to_path_buf(), SidecarNaming::NodeOwned);
        let n = node(1);

        let old: Vec<FileVersionInfo> = (1..=3)
            .map(|v| version(v, Some(&format!("hash{v}"))))
            .collect();
        for v in &old {
            touch(&dir.sidecar_path(&n, v));
        }

        // After collapse the live set is a single merged run.
        let merged = version(4, Some("merged"));
        let live = LiveVersions::from_persistence(n, vec![merged.clone()]);

        let rec = dir.reconcile(&live).unwrap();
        assert_eq!(rec.removed.len(), 3, "all superseded sidecars swept");
        assert_eq!(rec.missing.len(), 1, "the merged run still needs writing");
        for v in &old {
            assert!(!dir.sidecar_path(&n, v).exists());
        }
    }

    /// A shared glob dir holds one sidecar per (source node, version). Sweeping
    /// node A must not touch node B: the rollup cache's dir is shared by every
    /// file a pattern matched.
    #[test]
    fn reconcile_is_node_scoped_in_a_shared_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = SidecarDir::new(tmp.path().to_path_buf(), SidecarNaming::NodeScoped);
        let (a, b) = (node(1), node(2));

        let a_old = version(1, Some("a1"));
        let b_old = version(1, Some("b1"));
        touch(&dir.sidecar_path(&a, &a_old));
        touch(&dir.sidecar_path(&b, &b_old));

        // Node A collapsed; node B is untouched and still live at v1.
        let live_a = LiveVersions::from_persistence(a, vec![version(2, Some("a-merged"))]);
        let rec = dir.reconcile(&live_a).unwrap();

        assert_eq!(rec.removed.len(), 1);
        assert!(
            !dir.sidecar_path(&a, &a_old).exists(),
            "A's stale sidecar go"
        );
        assert!(
            dir.sidecar_path(&b, &b_old).exists(),
            "B's sidecar must survive a sweep scoped to A"
        );
    }

    #[test]
    fn reconcile_ignores_non_parquet_and_tmp_files() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = SidecarDir::new(tmp.path().to_path_buf(), SidecarNaming::NodeOwned);
        let n = node(1);
        let live = LiveVersions::from_persistence(n, vec![version(1, Some("aaa"))]);
        touch(&dir.sidecar_path(&n, &live.as_slice()[0]));

        let tmp_file = tmp.path().join("v9_partial.parquet.tmp");
        let frontier = tmp.path().join("frontier.json");
        touch(&tmp_file);
        touch(&frontier);

        let rec = dir.reconcile(&live).unwrap();
        assert!(rec.removed.is_empty());
        assert!(tmp_file.exists(), "in-flight writes are not garbage");
        assert!(frontier.exists(), "sidecar sweep must not eat bookkeeping");
    }

    #[test]
    fn cached_set_names_only_live_present_files() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = SidecarDir::new(tmp.path().to_path_buf(), SidecarNaming::NodeOwned);
        let n = node(1);
        let v1 = version(1, Some("aaa"));
        let v2 = version(2, Some("bbb"));

        // A superseded sidecar on disk that is NOT in the live set.
        touch(&dir.sidecar_path(&n, &version(9, Some("dead"))));
        touch(&dir.sidecar_path(&n, &v1));

        let live = LiveVersions::from_persistence(n, vec![v1.clone(), v2.clone()]);
        let set = dir.cached_set(&live, |_| true);

        // v2 has no file yet, and the dead sidecar is not reachable at all.
        assert_eq!(set.files().len(), 1);
        assert!(set.files()[0].ends_with("v1_aaa.parquet"));
    }

    #[test]
    fn cached_set_respects_the_retain_predicate() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = SidecarDir::new(tmp.path().to_path_buf(), SidecarNaming::NodeOwned);
        let n = node(1);
        let v1 = version(1, Some("aaa"));
        let v2 = version(2, Some("bbb"));
        touch(&dir.sidecar_path(&n, &v1));
        touch(&dir.sidecar_path(&n, &v2));

        let live = LiveVersions::from_persistence(n, vec![v1, v2]);
        let set = dir.cached_set(&live, |v| v.version > 1);
        assert_eq!(set.files().len(), 1);
        assert!(set.files()[0].ends_with("v2_bbb.parquet"));
    }

    #[test]
    fn sidecar_naming_layouts() {
        let n = node(7);
        let v = version(3, Some("abc"));
        let owned = SidecarDir::new(PathBuf::from("/c"), SidecarNaming::NodeOwned);
        let scoped = SidecarDir::new(PathBuf::from("/c"), SidecarNaming::NodeScoped);

        assert_eq!(
            owned.sidecar_path(&n, &v).file_name().unwrap(),
            "v3_abc.parquet"
        );
        assert_eq!(
            scoped.sidecar_path(&n, &v).file_name().unwrap().to_str(),
            Some(format!("{n}_v3_abc.parquet").as_str())
        );
    }

    #[test]
    fn dynamic_versions_key_on_node_id() {
        let n = node(7);
        let v = version(1, None);
        let dir = SidecarDir::new(PathBuf::from("/c"), SidecarNaming::NodeOwned);
        let name = dir.sidecar_path(&n, &v);
        let name = name.file_name().unwrap().to_str().unwrap();
        assert!(name.starts_with("v1_"));
        assert!(name.contains(&n.to_short_string()));
    }
}
