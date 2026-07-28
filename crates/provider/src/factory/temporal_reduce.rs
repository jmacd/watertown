// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Temporal reduce dynamic factory for TLogFS
//!
//! This factory creates temporal downsampling views from existing data sources,
//! providing time-bucketed aggregations similar to the original reduce module but
//! using the modern TLogFS dynamic factory system.
//!
//! ## Factory Type: `temporal-reduce`
//!
//! Creates a dynamic directory with resolution-based files, where each resolution
//! represents a different temporal downsampling level (e.g., 1h, 6h, 1d).
//!
//! ## Configuration
//!
//! ```yaml
//! entries:
//!   - name: "single_site"
//!     factory: "temporal-reduce"
//!     config:
//!       in_pattern: "series:///combined/*"  # URL pattern to match source files
//!       out_pattern: "$0"           # Output basename pattern using captured groups
//!       time_column: "timestamp"    # Column containing timestamp
//!       resolutions: [1h, 2h, 4h, 12h, 24h]
//!       aggregations:
//!         - type: "avg"
//!           # Glob patterns supported: * matches any characters
//!           # "Vulink*.Temperature.C" matches "Vulink1.Temperature.C", "Vulink2.Temperature.C", etc.
//!           columns: ["Vulink*.Temperature.C", "AT500_Surface.Temperature.C"]
//!         - type: "min"
//!           columns: ["Vulink*.Temperature.C", "AT500_Surface.Temperature.C"]
//!         - type: "max"  # columns optional - applies to all numeric columns
//! ```
//!
//! ## Generated Structure
//!
//! The factory creates a directory with files named by resolution:
//! - `res=1h.series` - 1 hour aggregated data
//! - `res=6h.series` - 6 hour aggregated data  
//! - `res=1d.series` - 1 day aggregated data
//!
//! Each file contains time-bucketed aggregations using SQL GROUP BY operations.
//!
//! ## Multi-file glob patterns
//!
//! When `in_pattern` contains a glob (e.g., `oteljson:///ingest/casparwater*.json`)
//! that matches multiple files all mapping to the same `out_pattern`, the factory
//! delegates to `SqlDerivedFile` which expands the glob and creates a UNION ALL
//! across all matching files.  This allows temporal-reduce to aggregate across
//! many rotated log files or ingested data fragments in a single pass.
//!
//! **Caveat -- schema inference shortcut:** The column schema is discovered from
//! the lexicographically last matching file (the newest for timestamped names).
//! This works well when columns are only added over time (the newest file has
//! the most complete schema).  If files have **incompatible schemas** (e.g.,
//! columns renamed or removed), schema discovery may be incorrect.
//! Use `timeseries-join` for heterogeneous-schema merging, which performs proper
//! per-file schema discovery and alignment.

use crate::factory::sql_derived::{SqlDerivedConfig, SqlDerivedFile};
use crate::register_dynamic_factory;
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::{self, Stream};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tinyfs::ResultExt;
use tinyfs::{
    DirHandle, Directory, EntryType, Node, NodeMetadata, NodeType, Result as TinyFSResult,
};

/// Aggregation types supported by the temporal reduce factory
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggregationType {
    Avg,
    Min,
    Max,
    Count,
    Sum,
}

impl AggregationType {
    /// Convert to SQL function name
    fn to_sql(&self) -> &'static str {
        match self {
            AggregationType::Avg => "AVG",
            AggregationType::Min => "MIN",
            AggregationType::Max => "MAX",
            AggregationType::Count => "COUNT",
            AggregationType::Sum => "SUM",
        }
    }
}

/// Configuration for a single aggregation operation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregationConfig {
    /// Type of aggregation (avg, min, max, count, sum)
    #[serde(rename = "type")]
    pub agg_type: AggregationType,

    /// Columns to apply the aggregation to
    /// If not specified, applies to all numeric columns
    /// Use ["*"] for count operations
    #[serde(default)]
    pub columns: Option<Vec<String>>,
}

/// Configuration for the temporal-reduce factory
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemporalReduceConfig {
    /// Input URL pattern to match source files (supports glob patterns)
    pub in_pattern: crate::Url,

    /// Output basename pattern using captured groups (e.g., "$0", "$1") - no URL scheme
    pub out_pattern: String,

    /// Name of the timestamp column
    pub time_column: String,

    /// List of temporal resolutions (e.g., "1h", "6h", "1d")
    /// Parsed using humantime::parse_duration
    pub resolutions: Vec<String>,

    /// Aggregation operations to perform
    pub aggregations: Vec<AggregationConfig>,

    /// Optional list of table transform factory paths to apply to input TableProvider.
    /// Transforms are applied in order before SQL execution.
    /// Each transform is a path like "/etc/hydro_rename"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transforms: Option<Vec<String>>,

    /// Maximum allowed lateness for the segment rollup cache: how far behind
    /// the newest bucket a late/backfilled sample may still land before its
    /// output bucket is treated as sealed. Buckets older than
    /// `newest_bucket - allowed_lateness` are frozen into immutable segment files and
    /// never recomputed, which bounds the rollup's working set to this window
    /// instead of all history. Parsed with `humantime::parse_duration`; defaults
    /// to one day. Data arriving older than the sealed watermark is not an
    /// error: the segments covering it are unsealed and recomputed from source.
    /// Changing this value invalidates the merged (segment) cache, which is
    /// rebuilt from the cached source versions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_lateness: Option<String>,

    /// Minimum accumulated size before frozen buckets are sealed into an
    /// immutable segment file. Accepts a byte count; defaults to
    /// [`crate::rollup_cache::SEAL_TARGET_BYTES`].
    ///
    /// Unlike `allowed_lateness`, changing this does NOT invalidate the cache:
    /// a segment already sealed is correct at any target, so the new value simply
    /// governs the next seal. Exposed mainly so the tradeoff can be measured
    /// against real data -- larger means fewer files but a heavier hot-window
    /// recompute each build.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seal_target_bytes: Option<u64>,

    /// Maximum number of segment files to leave live per resolution before
    /// compaction merges them regardless of size class. Defaults to
    /// [`crate::rollup_cache::MAX_LIVE_SEGMENTS`]. Like `seal_target_bytes`, changing
    /// it invalidates nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_live_segments: Option<usize>,
}

/// The two knobs governing how the sealed layout advances, carried together so
/// they stay in step: `lateness_secs` decides WHICH buckets may freeze, and
/// `target_bytes` decides WHEN enough have frozen to be worth a file.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SealPolicy {
    /// Maximum allowed lateness in seconds; recorded in the manifest because a
    /// change to it invalidates the cache.
    pub lateness_secs: i64,
    /// Seal threshold in bytes; deliberately NOT recorded, because a change to
    /// it invalidates nothing.
    pub target_bytes: u64,
    /// Compaction backstop: how many segments may stay live before adjacent
    /// segments are merged regardless of size class. Also not recorded, for the same
    /// reason.
    pub max_segments: usize,
}

/// Default maximum allowed lateness when `allowed_lateness` is unset: one day.
const DEFAULT_ALLOWED_LATENESS: Duration = Duration::from_secs(24 * 60 * 60);

impl TemporalReduceConfig {
    /// Resolve the configured maximum allowed lateness as whole seconds,
    /// defaulting to [`DEFAULT_ALLOWED_LATENESS`]. Parsing failures are a hard
    /// error so a malformed duration surfaces instead of silently defaulting.
    fn allowed_lateness_secs(&self) -> TinyFSResult<i64> {
        let dur = match &self.allowed_lateness {
            Some(s) => humantime::parse_duration(s).map_err(|e| {
                tinyfs::Error::Other(format!(
                    "temporal-reduce: invalid allowed_lateness '{}': {}",
                    s, e
                ))
            })?,
            None => DEFAULT_ALLOWED_LATENESS,
        };
        Ok(dur.as_secs() as i64)
    }

    /// Resolve the seal threshold in bytes, defaulting to
    /// [`crate::rollup_cache::SEAL_TARGET_BYTES`].
    fn seal_target_bytes(&self) -> u64 {
        self.seal_target_bytes
            .unwrap_or(crate::rollup_cache::SEAL_TARGET_BYTES)
    }

    /// Resolve the compaction backstop, defaulting to
    /// [`crate::rollup_cache::MAX_LIVE_SEGMENTS`]. Clamped to at least 1 so a
    /// nonsense setting cannot make compaction spin.
    fn max_live_segments(&self) -> usize {
        self.max_live_segments
            .unwrap_or(crate::rollup_cache::MAX_LIVE_SEGMENTS)
            .max(1)
    }
}

/// Convert Duration to SQL interval string compatible with DuckDB
/// Deferred SQL file that generates temporal reduction SQL when schema is discovered
/// This avoids the marker-based approach by deferring SQL generation until
/// the source schema can be properly discovered
/// Result of building one resolution level of the segment cache. The
/// `provider` is the `ListingTable` over that level's segments + hot (in
/// mergeable-partials form), which the next-coarser level folds and which the
/// final level wraps in a read-time reconstruction view. `digest` is the level's
/// manifest digest (the coarser level records it as its `source_digest`);
/// `changed` feeds the export hint and the coarser level's unsealing;
/// `rebuilt` is true when this level was
/// wiped and rebuilt from scratch (a non-append change), which forces the next
/// coarser level to rebuild too.
struct LevelBuild {
    provider: Arc<dyn datafusion::catalog::TableProvider>,
    digest: String,
    changed: LevelChange,
    rebuilt: bool,
}

/// Which output buckets a level changed this build.
///
/// This used to be a bare `Option<i64>` carrying three different meanings on
/// the same `None`: "nothing changed" (reuse), "everything changed" (rebuild),
/// and "the dirty range is unbounded" (a source version with no recorded event
/// range). That conflation is harmless for the export hint, which treats every
/// `None` as "rewrite all", but it is not harmless for the coarser level, which
/// must unseal EVERYTHING in the last two cases and NOTHING in the first.
/// Spelling the three out keeps a reader of one from silently behaving like a
/// reader of another.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LevelChange {
    /// Nothing changed; the level was served from cache untouched.
    Nothing,
    /// Every bucket at or after this one (epoch seconds, bucket-aligned).
    Since(i64),
    /// The whole axis: a rebuild, or a dirty range with no lower bound.
    Everything,
}

impl LevelChange {
    /// The dirty point to hand `unseal_from`, whose `None` means "the whole
    /// axis". Returns `None` for [`LevelChange::Nothing`] too, so callers must
    /// check that case before calling.
    fn dirty_lo(self) -> Option<i64> {
        match self {
            LevelChange::Since(s) => Some(s),
            LevelChange::Nothing | LevelChange::Everything => None,
        }
    }

    /// The export hint's `changed_since`, where `None` means "rewrite every
    /// partition". Unchanged levels are reported as `None` as well; the hint's
    /// digest is what lets the exporter skip them.
    fn export_hint(self) -> Option<i64> {
        match self {
            LevelChange::Since(s) => Some(s),
            LevelChange::Nothing | LevelChange::Everything => None,
        }
    }

    /// The lower -- i.e. more inclusive -- of two dirty ranges.
    ///
    /// [`LevelChange::Everything`] absorbs anything, [`LevelChange::Nothing`] is
    /// the identity, and two bounded ranges keep the earlier point. Used where a
    /// level has more than one independent reason to believe its sealed history
    /// is stale and must honour all of them at once.
    fn min(self, other: LevelChange) -> LevelChange {
        match (self, other) {
            (LevelChange::Everything, _) | (_, LevelChange::Everything) => LevelChange::Everything,
            (LevelChange::Nothing, o) => o,
            (s, LevelChange::Nothing) => s,
            (LevelChange::Since(a), LevelChange::Since(b)) => LevelChange::Since(a.min(b)),
        }
    }
}

pub struct TemporalReduceSqlFile {
    config: TemporalReduceConfig,
    duration: Duration,
    source_path: String, // Representative source file for schema discovery (no wildcards)
    /// URL to use as the SqlDerived pattern source.  When the in_pattern glob
    /// matched multiple files that map to the same output, this is the original
    /// glob URL so that SqlDerivedFile can expand/UNION all matching files.
    /// For single-match cases this equals source_url().
    pattern_url: String,
    context: crate::FactoryContext,
    // Lazy-initialized actual SQL file
    inner: Arc<tokio::sync::Mutex<Option<SqlDerivedFile>>>,
    // CACHE: Discovered columns to avoid repeated schema discovery
    // This prevents the O(N) cascade where discover_source_columns() triggers
    // as_table_provider() on each layer for every export operation
    discovered_columns: Arc<tokio::sync::Mutex<Option<Vec<String>>>>,
}

impl TemporalReduceSqlFile {
    #[must_use]
    pub fn new(
        config: TemporalReduceConfig,
        duration: Duration,
        _source_node: Node,
        source_path: String,
        pattern_url: String,
        context: crate::FactoryContext,
    ) -> Self {
        Self {
            config,
            duration,
            source_path,
            pattern_url,
            context,
            inner: Arc::new(tokio::sync::Mutex::new(None)),
            discovered_columns: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// Build the full source URL string, preserving the in_pattern's URL scheme.
    ///
    /// For format providers (oteljson://, csv://, etc.) this returns the original URL.
    /// For builtin schemes (series://, table://, file://) it re-applies the scheme to
    /// the source_path. This ensures that schema discovery and SQL execution both use
    /// the correct format provider for the source data.
    fn source_url(&self) -> String {
        // Use full_scheme() to preserve compression and entry type suffixes
        // (e.g., "csv+gzip", "file+series", "file+table").
        let scheme = self.config.in_pattern.full_scheme();
        if self.source_path.starts_with('/') {
            format!("{}://{}", scheme, self.source_path)
        } else {
            format!("{}:///{}", scheme, self.source_path)
        }
    }

    /// Discover source columns by accessing the source node directly
    /// CACHED: Only performs discovery once per TemporalReduceSqlFile instance
    async fn discover_source_columns(&self) -> TinyFSResult<Vec<String>> {
        // Check cache first - avoid repeated schema discovery cascade
        let mut columns_guard = self.discovered_columns.lock().await;

        if let Some(cached_columns) = &*columns_guard {
            log::debug!(
                "[GO] CACHE HIT (race): Using cached {} columns for temporal reduce source '{}'",
                cached_columns.len(),
                self.source_path
            );
            return Ok(cached_columns.clone());
        }

        // Use the Provider API to create a table provider, respecting the in_pattern URL scheme.
        // This is critical for format providers like oteljson://, csv://, excelhtml:// where the
        // raw file bytes need format-specific parsing to produce a queryable table.
        // Previously this went directly through as_queryable() on the raw file node, which
        // assumes Parquet format and fails for non-Parquet data files.
        let source_url = self.source_url();
        log::debug!(
            "TemporalReduceFile: discovering columns via Provider API with URL '{}'",
            source_url
        );

        let fs = self.context.context.filesystem();
        let mut provider =
            crate::Provider::with_context(Arc::new(fs), Arc::new(self.context.context.clone()));
        if let Ok(root) = self.context.root().await {
            provider = provider.with_root(root);
        }
        let datafusion_ctx = datafusion::prelude::SessionContext::new();

        let table_provider = provider
            .create_table_provider(&source_url, &datafusion_ctx)
            .await
            .map_other_context(format!(
                "Failed to create table provider for source '{}'",
                source_url
            ))?;

        // Get schema and extract all column names, filtering out only the timestamp column
        // We include all columns (numeric and non-numeric) and let the aggregation functions
        // handle what they can aggregate - SQL will naturally ignore non-aggregatable columns
        let schema = table_provider.schema();
        let columns: Vec<String> = schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .filter(|name| name != &self.config.time_column)
            .collect();

        // Fail fast if no columns are discovered (following Watertown's fail-fast architectural principles)
        if columns.is_empty() {
            return Err(tinyfs::Error::Other(format!(
                "Schema discovery failed: no columns found in source file '{}'. \
                Table provider returned schema with {} total fields. \
                This indicates a problem with the table provider configuration or the source URL scheme.",
                self.source_path,
                schema.fields().len()
            )));
        }

        // Cache the discovered columns for future calls
        *columns_guard = Some(columns.clone());
        log::debug!(
            "[SAVE] CACHED: Stored {} discovered columns for temporal reduce source '{}'",
            columns.len(),
            self.source_path
        );

        Ok(columns)
    }

    /// Generate SQL with discovered schema
    async fn generate_sql_with_discovered_schema(
        &self,
        pattern_name: &str,
    ) -> TinyFSResult<String> {
        let modified_config = self.filled_config().await?;

        // Now call the existing generate_temporal_sql function with filled-in columns
        let sql = generate_temporal_sql(
            &modified_config,
            self.duration,
            &self.source_path,
            &self.context,
            pattern_name,
        )
        .await?;
        log::debug!(
            "[SEARCH] TEMPORAL-REDUCE SQL for {}: \n{}",
            self.source_path,
            sql
        );
        Ok(sql)
    }

    /// Resolve the config's aggregation column patterns against the discovered
    /// source schema, returning a config whose every aggregation has concrete
    /// column lists. This is the shared input to both the single-pass SQL and
    /// the rollup partial/merge SQL.
    async fn filled_config(&self) -> TinyFSResult<TemporalReduceConfig> {
        // Discover available columns
        let discovered_columns = self.discover_source_columns().await?;
        log::debug!(
            "TemporalReduceFile: discovered {} columns: {:?}",
            discovered_columns.len(),
            discovered_columns
        );

        // Create a modified config with discovered columns filled in
        let mut modified_config = self.config.clone();

        for agg in &mut modified_config.aggregations {
            match agg.columns.take() {
                None => {
                    // Use all discovered columns for this aggregation
                    agg.columns = Some(discovered_columns.clone());
                    log::debug!(
                        "TemporalReduceFile: filled {} aggregation with {} columns",
                        agg.agg_type.to_sql(),
                        discovered_columns.len()
                    );
                }
                Some(patterns) => {
                    // Special case: COUNT with ["*"] means COUNT(*), not glob expansion
                    if matches!(agg.agg_type, AggregationType::Count)
                        && patterns.len() == 1
                        && patterns[0] == "*"
                    {
                        agg.columns = Some(vec!["*".to_string()]);
                        log::debug!("TemporalReduceFile: COUNT aggregation preserved as COUNT(*)");
                        continue;
                    }

                    // Apply glob matching to filter discovered columns
                    let mut matched_columns = Vec::new();
                    for pattern in &patterns {
                        for column in &discovered_columns {
                            if match_column_pattern(column, pattern) {
                                // Avoid duplicates
                                if !matched_columns.contains(column) {
                                    matched_columns.push(column.clone());
                                }
                            }
                        }
                    }

                    // Replace the patterns with the matched columns
                    agg.columns = Some(matched_columns.clone());
                    log::debug!(
                        "TemporalReduceFile: {} aggregation matched {} patterns to {} columns",
                        agg.agg_type.to_sql(),
                        patterns.len(),
                        matched_columns.len()
                    );
                }
            }
        }

        Ok(modified_config)
    }

    /// Attempt to serve this resolution from the incremental rollup cache,
    /// returning `Ok(None)` to fall back to the single-pass delegate when the
    /// rollup preconditions are not met.
    ///
    /// The rollup computes decomposable partials once per immutable source
    /// version, caches them on disk, and merges them by time bucket. A new
    /// ingest version therefore costs O(new rows) instead of O(history).
    ///
    /// Preconditions for the rollup path:
    /// - a pond cache directory is configured;
    /// - the input has no transforms, so partials can be computed directly from
    ///   the parsed source rows;
    /// - the input scheme is a format provider whose parsed leaves are cached
    ///   per version by the format cache, which the partials read.
    async fn try_rollup_table_provider(
        &self,
        id: tinyfs::FileID,
        context: &tinyfs::ProviderContext,
    ) -> TinyFSResult<Option<Arc<dyn datafusion::catalog::TableProvider>>> {
        let Some(cache_dir) = context.cache_dir().map(std::path::Path::to_path_buf) else {
            return Ok(None);
        };

        if self
            .config
            .transforms
            .as_ref()
            .is_some_and(|t| !t.is_empty())
        {
            return Ok(None);
        }

        let scheme = self.config.in_pattern.scheme();
        let Some(format_provider) = crate::FormatRegistry::get_provider(scheme) else {
            return Ok(None);
        };

        let source_files = self.resolve_source_files().await?;
        if source_files.is_empty() {
            return Ok(None);
        }

        // The finest resolution aggregates the sources directly; every coarser
        // resolution folds the next-finer resolution's segments, so no level
        // rescans raw history more than once. The cache namespace is therefore
        // shared across all resolutions of the same site.
        let resolution_durations = parse_nesting_resolutions(&self.config.resolutions)?;
        let Some(finest_interval) = resolution_durations.first().copied() else {
            return Ok(None);
        };

        let filled = self.filled_config().await?;
        let pieces = AggSqlPieces::build(&filled)?;
        let cfg_hash = crate::rollup_cache::cfg_hash(&rollup_cfg_canonical(
            &filled,
            finest_interval,
            &self.pattern_url,
        ));

        // Key the cache by the parent site partition, which is shared by every
        // resolution file of this site, so all resolutions share one namespace.
        let site_node_id = id.part_id().to_node_id();

        let fs = self.context.context.filesystem();
        let mut provider_api =
            crate::Provider::with_context(Arc::new(fs), Arc::new(self.context.context.clone()));
        if let Ok(root) = self.context.root().await {
            provider_api = provider_api.with_root(root);
        }

        // Collect every source node's LIVE versions, plus the content identity
        // of the set: blake3 -> event-time range. That map is the freshness key
        // (design §5.3); nothing here is keyed by a version number or filename.
        let mut source_nodes: Vec<(tinyfs::NodeID, Vec<tinyfs::FileVersionInfo>)> =
            Vec::with_capacity(source_files.len());
        let mut now: std::collections::BTreeMap<String, crate::rollup_cache::SourceRange> =
            std::collections::BTreeMap::new();

        for node_path in &source_files {
            let file_url_str = node_file_url(scheme, node_path);
            let file_url = crate::Url::parse(&file_url_str).map_other()?;

            let (source_node_id, versions) = provider_api
                .ensure_url_cached(&file_url, format_provider.as_ref(), &cache_dir)
                .await
                .map_other()?;

            // The rollup sums every live version of a source. Series entry types
            // (FilePhysicalSeries, TablePhysicalSeries) store append-only deltas
            // that are concatenated on read, so their versions are disjoint and
            // summing them is exactly a single-pass GROUP BY over the whole
            // series. A non-series file stores a full RE-SNAPSHOT per version,
            // so two live versions share rows and summing them double-counts.
            //
            // This used to be defended by a per-source "sequentiality frontier":
            // scan each new version's bucket span, persist the high-water mark,
            // and fail if a later version reached below it. That is a
            // data-dependent test for a condition fixed by the entry type -- and
            // for a re-snapshot it fires on the second version regardless, since
            // every snapshot re-covers all of history. Testing the structure
            // says the same thing without the scan, the sidecar file, or the
            // chance of a stale frontier disagreeing with the data.
            let is_series = matches!(
                node_path.id().entry_type(),
                EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries
            );
            if !is_series && versions.len() > 1 {
                return Err(tinyfs::Error::Other(format!(
                    "temporal-reduce rollup: source '{}' is a non-series file with \
                     {} live versions. Each version is a full re-snapshot, so they \
                     overlap and cannot be aggregated together without \
                     double-counting. Use a series entry type for incrementally \
                     appended data, or reduce a single-version source.",
                    node_path.path().display(),
                    versions.len()
                )));
            }

            for v in &versions {
                let _ = now.insert(source_version_key(&source_node_id, v), source_range(v));
            }
            source_nodes.push((source_node_id, versions));
        }

        let ctx = &context.datafusion_session;

        // Let a consumer's full-history `ORDER BY {ts}` over the reduced series
        // stream via a k-way `SortPreservingMergeExec` of the segments + hot
        // file (each declared pre-sorted in `listing_table_for_res_dir`) instead
        // of a `SortExec` that buffers the whole series in memory. DataFusion only
        // splits a ListingTable's files into per-partition, statistics-ordered
        // groups (a prerequisite for that merge) when this execution option is
        // set; it is off by default. The rollup provider is built and later
        // queried on this same shared session, so enabling it here reaches the
        // export/sitegen read path (design §3 O(1)-memory read).
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .split_file_groups_by_statistics = true;

        let sanitized_id: String = site_node_id
            .to_string()
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        let source_table = format!("__rollup_source_{}_{}", cfg_hash, sanitized_id);

        let ts = filled.time_column.clone();

        // Segment cache (Phase 2) + hierarchical rollup (Phase 3
        // step 2). Each output resolution freezes buckets below a watermark into
        // immutable append-only segments and recomputes only the open hot window, so
        // per-build cost is bounded to ~allowed_lateness instead of all history.
        //
        // Resolutions form a nesting chain (finest .. coarsest). The finest
        // resolution aggregates the SOURCE directly; every coarser resolution is
        // built by folding the *next-finer resolution's* segments + hot
        // (exponentially less input than rescanning the source), which is exact
        // because the stored partials are associative and the intervals nest. We build the chain finest-first up to this file's
        // resolution, so a consumer of any resolution transparently materializes
        // the finer levels it depends on. Each level reuses its cache when
        // unchanged, so this is cheap after the first cold build.
        let policy = SealPolicy {
            lateness_secs: filled.allowed_lateness_secs()?,
            target_bytes: filled.seal_target_bytes(),
            max_segments: filled.max_live_segments(),
        };
        let merged_dir = crate::rollup_cache::merged_dir(&cache_dir, &cfg_hash, &site_node_id);

        // Levels to build: finest .. this file's resolution (inclusive).
        let levels: Vec<Duration> = resolution_durations
            .iter()
            .copied()
            .filter(|d| *d <= self.duration)
            .collect();

        let mut finer: Option<(String, Arc<dyn datafusion::catalog::TableProvider>)> = None;
        let mut finer_rebuilt = false;
        // The finer level's dirty point, so a backfill propagates up the chain
        // instead of stopping at the finest resolution.
        let mut finer_changed: LevelChange = LevelChange::Nothing;
        let mut final_level: Option<LevelBuild> = None;

        for (idx, level) in levels.iter().copied().enumerate() {
            let res_dir = crate::rollup_cache::segment_res_dir(&merged_dir, level.as_secs());
            let built = if idx == 0 {
                // Finest resolution: aggregate the sources directly.
                self.build_level_from_source(
                    ctx,
                    &pieces,
                    &ts,
                    level,
                    &res_dir,
                    &cache_dir,
                    scheme,
                    &source_nodes,
                    &now,
                    &source_table,
                    policy,
                )
                .await?
            } else {
                // Coarser resolution: fold the next-finer resolution's segments+hot.
                let (finer_digest, finer_provider) =
                    finer.take().expect("non-finest level has a finer level");
                let finer_table = format!(
                    "__rollup_finer_{}_{}_{}",
                    cfg_hash,
                    sanitized_id,
                    level.as_secs()
                );
                let _ = ctx
                    .register_table(finer_table.as_str(), finer_provider)
                    .map_other()?;
                let out = self
                    .build_level_from_finer(
                        ctx,
                        &pieces,
                        &ts,
                        level,
                        &res_dir,
                        &finer_table,
                        &finer_digest,
                        finer_rebuilt,
                        finer_changed,
                        &now,
                        policy,
                    )
                    .await;
                let _ = ctx.deregister_table(finer_table.as_str()).map_other()?;
                out?
            };
            finer_rebuilt = built.rebuilt;
            finer_changed = built.changed;
            finer = Some((built.digest.clone(), built.provider.clone()));
            final_level = Some(built);
        }

        let final_level = final_level.expect("at least the finest level is always built");
        let (table_provider, digest, changed_since) = (
            final_level.provider,
            final_level.digest,
            final_level.changed.export_hint(),
        );

        // The segments + hot file store mergeable partials
        // (SEALED_FORMAT = partials-v2); reconstruct the output columns at read
        // time so consumers see identical output (same names, order, values,
        // including Avg = Sum / Count) while the on-disk segments stay associatively
        // foldable for the coarser-from-finer rollup (design §3 / Phase 3). The
        // reconstruction is a passthrough-projection view over the partials
        // listing table: the timestamp column flows through unchanged so the
        // scan's declared ordering (the streaming, O(1)-memory read path) is
        // preserved. The listing provider is embedded in the view's logical plan,
        // so the view resolves in any consumer session, then deregistered here to
        // avoid leaking a table into the shared session.
        let read_name = format!(
            "__rollup_reconstruct_{}_{}_{}",
            cfg_hash,
            sanitized_id,
            self.duration.as_secs()
        );
        let _ = ctx
            .register_table(read_name.as_str(), table_provider)
            .map_other()?;
        let recon_sql = pieces.reconstruct_sql(&ts, &read_name);
        // Deregister before propagating: read_name is deterministic in
        // cfg_hash/node/resolution, so leaving it behind on the shared session
        // turns one transient planning failure into a permanent duplicate-name
        // error for this resolution.
        let recon_plan = {
            let out = ctx
                .sql(&recon_sql)
                .await
                .map_other_context("rollup reconstruction planning failed");
            let _ = ctx.deregister_table(read_name.as_str()).map_other()?;
            out?.logical_plan().clone()
        };
        let table_provider: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(
            datafusion::catalog::view::ViewTable::new(recon_plan, Some(recon_sql)),
        );

        let cache_key = crate::TableProviderKey::new(id, crate::VersionSelection::LatestVersion)
            .to_cache_string();
        context.set_table_provider_cache(cache_key, table_provider.clone())?;

        // Publish an export hint so the sitegen export layer can skip rewriting
        // output partitions whose buckets did not change this build. The digest
        // is the manifest digest (stable when content is unchanged); the export
        // reuses partitions strictly before `changed_since`, or rewrites all when
        // it is None (full rebuild or unchanged reuse).
        context.set_export_hint(
            &id,
            tinyfs::ExportHint {
                digest,
                changed_since,
            },
        )?;

        Ok(Some(table_provider))
    }

    /// Newest output-bucket start (epoch seconds) over `table`, re-binning its
    /// `bucket_col` to `output_interval` -- the event-time frontier that drives
    /// the watermark. A single bounded MAX scan; no per-bucket accumulator.
    /// `None` when the table holds no rows. `bucket_col` is `time_bucket` for the
    /// shared finest partials or the output `ts` column for a finer resolution's
    /// segments+hot.
    async fn max_output_bucket_secs(
        &self,
        ctx: &datafusion::prelude::SessionContext,
        table: &str,
        bucket_col: &str,
        output_interval: Duration,
    ) -> TinyFSResult<Option<i64>> {
        let interval = duration_to_sql_interval(output_interval);
        let bin = date_bin_expr(&interval, bucket_col);
        let sql = format!(
            "SELECT CAST(MAX(EXTRACT(EPOCH FROM {bin})) AS BIGINT) AS hi_secs \
             FROM {table}"
        );
        let batches = ctx
            .sql(&sql)
            .await
            .map_other_context("max-output-bucket SQL planning failed")?
            .collect()
            .await
            .map_other_context("max-output-bucket SQL execution failed")?;
        for batch in &batches {
            if batch.num_rows() == 0 {
                continue;
            }
            if let Some(hi) = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
            {
                use arrow::array::Array;
                if !hi.is_null(0) {
                    return Ok(Some(hi.value(0)));
                }
            }
        }
        Ok(None)
    }

    /// Advance the sealed layout in `res_dir` by folding `input_table` (its
    /// bucket-timestamp column is `in_bucket_col`) into `output_interval`: seal
    /// buckets that have crossed the watermark into a new immutable segment, then
    /// recompute the open hot window. Mutates and persists nothing but the segment /
    /// hot files; updates `m.sealed_hi_secs`, `m.segments`, `m.next_seq`,
    /// `m.hot_digest`. Bounded to the newly-frozen span + the hot window. Shared
    /// by the finest fold (input = shared partials, `time_bucket`) and every
    /// coarser fold (input = the next-finer segments+hot, `ts`).
    async fn seal_and_recompute(
        &self,
        ctx: &datafusion::prelude::SessionContext,
        pieces: &AggSqlPieces,
        ts: &str,
        output_interval: Duration,
        res_dir: &std::path::Path,
        input_table: &str,
        in_bucket_col: &str,
        m: &mut crate::rollup_cache::SegmentManifest,
        policy: SealPolicy,
    ) -> TinyFSResult<Vec<std::path::PathBuf>> {
        // Event-time frontier: newest output bucket over the input (epoch
        // seconds). Bounded MAX scan, no per-bucket state.
        let data_hi_secs = self
            .max_output_bucket_secs(ctx, input_table, in_bucket_col, output_interval)
            .await?;
        let interval_secs = output_interval.as_secs() as i64;
        let watermark = data_hi_secs
            .map(|hi| (hi - m.allowed_lateness_secs).div_euclid(interval_secs) * interval_secs);

        // Seal buckets [sealed_hi, watermark) into a new immutable segment once
        // enough has accumulated to be worth freezing. Bounded to the
        // newly-frozen span.
        //
        // The size gate is what keeps the segment count proportional to data rather
        // than to build frequency. `hot` currently spans [sealed_hi, inf), which
        // contains the frozen span [sealed_hi, wm) a seal would write, so
        // `hot_bytes` from the previous build is an upper bound on the seal's
        // size: under target, there is nothing worth sealing and we skip without
        // writing a candidate to find out. The buckets are not lost -- they stay
        // in `hot`, which is recomputed from the input below, and are sealed in
        // one larger segment on a later build.
        if let Some(wm) = watermark
            && m.sealed_hi_secs.is_none_or(|sh| wm > sh)
            && m.hot_bytes >= policy.target_bytes
        {
            let seal_sql = pieces.merge_partials_sql(
                output_interval,
                ts,
                input_table,
                in_bucket_col,
                m.sealed_hi_secs,
                Some(wm),
            );
            let name = format!("seg-{:08}.parquet", m.next_seq);
            let seg_file = crate::rollup_cache::segment_path(res_dir, &name);
            let (seg_digest, rows) = self.write_merge_to(ctx, &seal_sql, &seg_file).await?;
            if rows > 0 {
                let bytes = tokio::fs::metadata(&seg_file).await.map_other()?.len();
                m.segments.push(crate::rollup_cache::Segment {
                    name,
                    lo_secs: m.sealed_hi_secs,
                    hi_secs: wm,
                    digest: seg_digest,
                    bytes,
                });
                m.next_seq += 1;
            } else {
                // Empty span (a gap with no data): advance the watermark without
                // recording an empty segment file.
                tokio::fs::remove_file(&seg_file).await.map_other()?;
            }
            m.sealed_hi_secs = Some(wm);
        }

        // Recompute the open hot window [sealed_hi, inf) from the current input --
        // where new appends and within-window late data land. Bounded to
        // ~allowed_lateness worth of buckets.
        let hot_sql = pieces.merge_partials_sql(
            output_interval,
            ts,
            input_table,
            in_bucket_col,
            m.sealed_hi_secs,
            None,
        );
        let hot_file = crate::rollup_cache::hot_path(res_dir);
        let (hot_digest, _rows) = self.write_merge_to(ctx, &hot_sql, &hot_file).await?;
        m.hot_digest = Some(hot_digest);
        m.hot_bytes = tokio::fs::metadata(&hot_file)
            .await
            .map(|md| md.len())
            .unwrap_or(0);

        self.compact_segments(ctx, pieces, ts, output_interval, res_dir, m, policy)
            .await
    }

    /// Merge adjacent segments until the size-tiered policy is satisfied,
    /// returning the segment files the merges superseded.
    ///
    /// Sealing alone only stops the count growing per build; without compaction
    /// it still grows without bound as data accumulates, and every query opens
    /// every live segment. This is what actually bounds read fan-out.
    ///
    /// The window is chosen by [`crate::size_tier::choose_merge_window`] -- the
    /// same function tlogfs uses to collapse series versions, not a second
    /// implementation of the same idea. Its two properties are what make this
    /// affordable: only same-size-class neighbours merge, so a large accumulated
    /// segment is not rewritten to absorb a few kilobytes; and the ragged-input
    /// backstop merges the CHEAPEST window rather than the oldest, because after
    /// any previous merge the oldest segment IS the large one.
    ///
    /// Superseded files are NOT deleted here. They are returned so the caller can
    /// delete them only after the manifest naming their replacement is durable;
    /// see [`crate::rollup_cache::remove_superseded`].
    #[allow(clippy::too_many_arguments)]
    async fn compact_segments(
        &self,
        ctx: &datafusion::prelude::SessionContext,
        pieces: &AggSqlPieces,
        ts: &str,
        output_interval: Duration,
        res_dir: &std::path::Path,
        m: &mut crate::rollup_cache::SegmentManifest,
        policy: SealPolicy,
    ) -> TinyFSResult<Vec<std::path::PathBuf>> {
        let mut superseded: Vec<std::path::PathBuf> = Vec::new();
        loop {
            let sizes: Vec<u64> = m.segments.iter().map(|r| r.bytes).collect();
            let Some(window) = crate::size_tier::choose_merge_window(&sizes, policy.max_segments)
            else {
                break;
            };

            let inputs: Vec<std::path::PathBuf> = m.segments[window.clone()]
                .iter()
                .map(|r| crate::rollup_cache::segment_path(res_dir, &r.name))
                .collect();
            let table = crate::rollup_cache::listing_table_for_files(&inputs, res_dir, ts)
                .await
                .map_other()?;

            // Re-merge the partials of just these segments. Merging partials is
            // associative, so folding a window of segments into one gives exactly
            // what folding the original inputs would have; this is the same
            // property that lets a coarser resolution be built from a finer one.
            let table_name = format!("__rollup_compact_{}", m.next_seq);
            _ = ctx.register_table(&table_name, table).map_other()?;
            let sql = pieces.merge_partials_sql(output_interval, ts, &table_name, ts, None, None);
            let name = format!("seg-{:08}.parquet", m.next_seq);
            let out = crate::rollup_cache::segment_path(res_dir, &name);
            let merged = self.write_merge_to(ctx, &sql, &out).await;
            _ = ctx.deregister_table(&table_name).map_other()?;
            let (digest, rows) = merged?;

            if rows == 0 {
                // Cannot happen for non-empty segments (an empty segment is never
                // recorded), but if it did, dropping the inputs would delete
                // data. Leave the layout untouched and stop.
                tokio::fs::remove_file(&out).await.map_other()?;
                log::warn!(
                    "rollup compaction: merge of {} segments produced no rows; leaving them alone",
                    inputs.len()
                );
                break;
            }

            let bytes = tokio::fs::metadata(&out).await.map_other()?.len();
            let replacement = crate::rollup_cache::Segment {
                name,
                lo_secs: m.segments[window.start].lo_secs,
                hi_secs: m.segments[window.end - 1].hi_secs,
                digest,
                bytes,
            };
            _ = m.segments.splice(window, [replacement]);
            m.next_seq += 1;
            superseded.extend(inputs);
        }
        Ok(superseded)
    }

    /// Build the finest resolution level by aggregating the SOURCE directly.
    ///
    /// Freshness is keyed on source CONTENT: `sources` maps the blake3 of each
    /// live source version to the event-time range it covers. Unchanged means
    /// Reuse; any difference means Incremental over the dirty range; no
    /// compatible manifest means Rebuild.
    ///
    /// There is no per-version partial cache. The cached source Parquets are
    /// aggregated through a view, pruned to the versions whose recorded
    /// `max_event_time` reaches the range being rebuilt. That prune is what the
    /// partials were really buying -- "do not rescan old versions" -- and it
    /// comes from data tlogfs already records, without a second file population
    /// and without a cache keyed by a version number.
    #[allow(clippy::too_many_arguments)]
    async fn build_level_from_source(
        &self,
        ctx: &datafusion::prelude::SessionContext,
        pieces: &AggSqlPieces,
        ts: &str,
        output_interval: Duration,
        res_dir: &std::path::Path,
        cache_dir: &std::path::Path,
        scheme: &str,
        source_nodes: &[(tinyfs::NodeID, Vec<tinyfs::FileVersionInfo>)],
        now: &std::collections::BTreeMap<String, crate::rollup_cache::SourceRange>,
        source_table: &str,
        policy: SealPolicy,
    ) -> TinyFSResult<LevelBuild> {
        let manifest =
            match crate::rollup_cache::read_verified_segment_manifest(res_dir).map_other()? {
                Some(m)
                    if m.allowed_lateness_secs == policy.lateness_secs
                        && m.format == crate::rollup_cache::SEALED_FORMAT =>
                {
                    Some(m)
                }
                Some(_) => {
                    crate::rollup_cache::wipe_segment_res_dir(res_dir).map_other()?;
                    None
                }
                None => None,
            };

        enum Plan {
            Reuse,
            /// Rebuild every output bucket at or after this event time (epoch
            /// microseconds). `None` means the whole axis.
            Incremental {
                dirty_lo_us: Option<i64>,
            },
            Rebuild,
        }

        let plan = match &manifest {
            Some(m) if m.sources == *now => Plan::Reuse,
            Some(m) => Plan::Incremental {
                // The dirty range is driven by the SYMMETRIC difference, not
                // just by additions. A digest that vanished matters as much as
                // one that appeared: it means a version was collapsed away or
                // deleted, and the buckets it contributed to must be recomputed
                // from whatever now covers them.
                dirty_lo_us: dirty_lo_us(&m.sources, now),
            },
            None => Plan::Rebuild,
        };

        if let Plan::Reuse = plan {
            let m = manifest.as_ref().expect("reuse implies a manifest");
            let provider = crate::rollup_cache::listing_table_for_res_dir(res_dir, m, ts)
                .await
                .map_other()?;
            let digest = crate::rollup_cache::segment_manifest_digest(m).map_other()?;
            return Ok(LevelBuild {
                provider,
                digest,
                changed: LevelChange::Nothing,
                rebuilt: false,
            });
        }

        let rebuilt = matches!(plan, Plan::Rebuild);
        let (mut m, mut unsealed, changed) = match plan {
            Plan::Incremental { dirty_lo_us } => {
                let mut m = manifest.expect("incremental implies a manifest");
                // Floor to the BUCKET, not just to the second. `sealed_hi_secs`
                // is a bucket boundary everywhere else, and every consumer
                // compares it against a bucket START (`merge_partials_sql`
                // filters `date_bin(...) >= lo`). An unaligned watermark would
                // leave the bucket containing the dirty point in neither a
                // segment (they stop at the aligned edge below it) nor the hot
                // window (it starts strictly above it), silently dropping that
                // bucket -- including the very rows that made it dirty. It
                // would also prune source versions at the unaligned point,
                // dropping rows in [bucket_start, dirty_lo) that the bucket
                // still needs.
                let dirty_lo_secs = dirty_lo_us
                    .map(floor_us_to_secs)
                    .map(|s| floor_secs_to_bucket(s, output_interval));
                // Late data is ordinary work now, not a --rebuild error. Buckets
                // at or after the dirty point must be recomputed; those below
                // sealed_hi live in immutable segments, so the segments covering
                // them are UNSEALED -- dropped from the manifest and folded back
                // into the hot window, which the seal path below recomputes from
                // source and re-seals.
                //
                // This works only because segments are keyed by the bucket range
                // they cover, so "which segments hold this instant" is a lookup.
                // Under the old version-keyed layout there was no way to find
                // them again, which is exactly why it had to error instead.
                let unsealed = unseal_from(&mut m, dirty_lo_secs, res_dir)?;
                let changed = match dirty_lo_secs {
                    Some(lo) => LevelChange::Since(lo),
                    // An unbounded dirty range is the whole axis, not "nothing".
                    None => LevelChange::Everything,
                };
                (m, unsealed, changed)
            }
            _ => {
                crate::rollup_cache::wipe_segment_res_dir(res_dir).map_other()?;
                (
                    crate::rollup_cache::SegmentManifest {
                        format: crate::rollup_cache::SEALED_FORMAT.to_string(),
                        allowed_lateness_secs: policy.lateness_secs,
                        ..Default::default()
                    },
                    Vec::new(),
                    LevelChange::Everything,
                )
            }
        };

        // Everything the build must be able to see: the hot window
        // [sealed_hi, inf) plus the dirty range. Pruning below this is what
        // keeps an incremental build off the whole history; pruning ABOVE it
        // would silently drop contributing rows, so take the lower of the two.
        let read_lo_us = match (m.sealed_hi_secs, changed.dirty_lo()) {
            (Some(sealed_hi), Some(dirty_lo)) => Some(secs_to_us(sealed_hi.min(dirty_lo))),
            // No watermark means every bucket is hot: read everything.
            (None, _) => None,
            (Some(sealed_hi), None) => Some(secs_to_us(sealed_hi)),
        };

        // Aggregate the sources into partial columns on the fly. This view is
        // what `seal_and_recompute` folds, so it plays exactly the role the
        // partials directory used to -- as a query, not as a file population.
        let source_set = bounded_source_set(cache_dir, scheme, source_nodes, read_lo_us);
        // This build is about to record `now` as the coverage of what it wrote.
        // A live version whose sidecar vanished (a concurrent reconciler deleted
        // it; neither side locks) would contribute no rows, and the manifest
        // would then claim it was covered -- an undercount that agrees with
        // itself forever, because the next build sees `sources == now` and
        // reuses. Fail the build instead; the retry re-caches and succeeds.
        if !source_set.missing().is_empty() {
            return Err(tinyfs::Error::Other(format!(
                "temporal-reduce rollup: {} live source version(s) have no cached \
                 Parquet (first: {}). This is a lost race with cache \
                 reconciliation, not a data error; re-run the read.",
                source_set.missing().len(),
                source_set.missing()[0].display(),
            )));
        }
        let provider = source_set.table_provider().await.map_other()?;
        let available: std::collections::HashSet<String> = provider
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        if ctx.table_exist(source_table).unwrap_or(false) {
            _ = ctx.deregister_table(source_table).map_other()?;
        }
        _ = ctx.register_table(source_table, provider).map_other()?;

        let partials_view = format!("{}_partials", source_table);
        let partial_sql = pieces.partial_sql(output_interval, ts, source_table, &available);
        let build = async {
            _ = ctx
                .sql(&format!(
                    "CREATE OR REPLACE VIEW {partials_view} AS {partial_sql}"
                ))
                .await
                .map_other_context("rollup source partial view")?;
            self.seal_and_recompute(
                ctx,
                pieces,
                ts,
                output_interval,
                res_dir,
                &partials_view,
                "time_bucket",
                &mut m,
                policy,
            )
            .await
        }
        .await;
        _ = ctx
            .sql(&format!("DROP VIEW IF EXISTS {partials_view}"))
            .await;
        let superseded = build?;

        m.sources = now.clone();
        m.source_digest = None;

        let digest = crate::rollup_cache::write_segment_manifest(res_dir, &m)
            .await
            .map_other()?;
        // Only now that the manifest naming the merged segments is durable.
        unsealed.extend(superseded);
        crate::rollup_cache::remove_superseded(&unsealed).await;
        let provider = crate::rollup_cache::listing_table_for_res_dir(res_dir, &m, ts)
            .await
            .map_other()?;
        Ok(LevelBuild {
            provider,
            digest,
            changed,
            rebuilt,
        })
    }

    /// Build a coarser resolution level by folding the next-finer level's segments +
    /// hot (`finer_table`, bucket column `ts`) into `output_interval` (Phase 3
    /// step 2). Freshness is keyed on the finer level's manifest digest
    /// (`finer_digest`) plus whether it was rebuilt:
    ///  - finer reused and its digest unchanged → Reuse;
    ///  - finer appended (not rebuilt) → advance this level (bounded seal + hot);
    ///  - finer rebuilt, or no compatible manifest → rebuild this level.
    ///
    /// No beyond-window hard-fail is needed here: this level's watermark aligns
    /// down at least as far as the finer level's (coarser interval, same
    /// lateness), so `coarse_sealed_hi ≤ finer_sealed_hi`.
    ///
    /// `finer_changed` is what the finer level changed. Late data is NOT a
    /// rebuild -- it takes the finer level's incremental path -- so
    /// `finer_rebuilt` alone would leave this level's segments below its own
    /// watermark holding pre-backfill values forever. This level therefore
    /// unseals from the same point, or unseals entirely when the finer level's
    /// dirty range was unbounded.
    ///
    /// `finer_changed` alone is not sufficient, however: it reports only what
    /// moved in THIS call, and a sibling output file's earlier build may already
    /// have consumed the finer level's change. This level therefore ALSO
    /// compares `now` against the source set it last folded and unseals from
    /// whichever of the two points is lower.
    #[allow(clippy::too_many_arguments)]
    async fn build_level_from_finer(
        &self,
        ctx: &datafusion::prelude::SessionContext,
        pieces: &AggSqlPieces,
        ts: &str,
        output_interval: Duration,
        res_dir: &std::path::Path,
        finer_table: &str,
        finer_digest: &str,
        finer_rebuilt: bool,
        finer_changed: LevelChange,
        now: &std::collections::BTreeMap<String, crate::rollup_cache::SourceRange>,
        policy: SealPolicy,
    ) -> TinyFSResult<LevelBuild> {
        let manifest =
            match crate::rollup_cache::read_verified_segment_manifest(res_dir).map_other()? {
                Some(m)
                    if m.allowed_lateness_secs == policy.lateness_secs
                        && m.format == crate::rollup_cache::SEALED_FORMAT =>
                {
                    Some(m)
                }
                Some(_) => {
                    crate::rollup_cache::wipe_segment_res_dir(res_dir).map_other()?;
                    None
                }
                None => None,
            };

        // Reuse: the finer level was reused (not rebuilt) and its digest matches
        // what this level last folded, so nothing downstream changed.
        if !finer_rebuilt
            && let Some(m) = &manifest
            && m.source_digest.as_deref() == Some(finer_digest)
        {
            let provider = crate::rollup_cache::listing_table_for_res_dir(res_dir, m, ts)
                .await
                .map_other()?;
            let digest = crate::rollup_cache::segment_manifest_digest(m).map_other()?;
            return Ok(LevelBuild {
                provider,
                digest,
                changed: LevelChange::Nothing,
                rebuilt: false,
            });
        }

        // Advance in place when the finer level only appended (its sealed history
        // below our watermark is immutable, so our segments stay valid);
        // otherwise rebuild from empty.
        let can_advance = !finer_rebuilt && manifest.is_some();
        let (mut m, mut unsealed, changed, rebuilt) = if can_advance {
            let mut m = manifest.expect("can_advance implies a manifest");
            // This level has TWO independent reasons to distrust its sealed
            // history, and must honour the lower of them.
            //
            // (1) `finer_changed`: what the finer level moved in THIS call.
            //
            // (2) The source content this level last folded versus what is live
            //     now. This is not implied by (1). Each output file
            //     (`res=1h.series`, `res=1d.series`) is a separate build over
            //     the same cache, and the finer level's change is consumed by
            //     whichever build runs first: by the time the coarse file is
            //     built, the finest level legitimately reports `Nothing` -- it
            //     is unchanged since a moment ago -- while THIS level has never
            //     folded the backfill. The same gap opens whenever a previous
            //     build failed between the finer level's manifest write and
            //     this one's.
            //
            //     Keying only on (1) would advance in place, recompute only the
            //     hot window `[sealed_hi, inf)`, and then record the new finer
            //     digest -- marking itself fresh forever while every sealed
            //     bucket below the watermark keeps its pre-backfill value.
            //
            // Computing (2) from the same symmetric difference the finest level
            // uses is what makes each level's freshness self-sufficient: it does
            // not depend on which files are built, in what order, or on how many
            // finer-level changes have been consumed since this level last ran.
            let source_changed = if m.sources == *now {
                LevelChange::Nothing
            } else {
                match dirty_lo_us(&m.sources, now) {
                    Some(us) => LevelChange::Since(floor_secs_to_bucket(
                        floor_us_to_secs(us),
                        output_interval,
                    )),
                    // An unbounded dirty range is the whole axis, not "nothing".
                    None => LevelChange::Everything,
                }
            };
            // Align to OUR (coarser) bucket: a watermark must be a bucket
            // boundary or the bucket straddling it belongs to neither the
            // sealed nor the hot side. See `floor_secs_to_bucket`.
            let dirty = match finer_changed {
                LevelChange::Since(lo) => {
                    LevelChange::Since(floor_secs_to_bucket(lo, output_interval))
                }
                other => other,
            }
            .min(source_changed);
            // Late data at the finest level lowered ITS watermark; ours must
            // follow, or our segments keep their pre-backfill values forever.
            let unsealed = match dirty {
                // Nothing below our watermark moved, so our segments stand.
                LevelChange::Nothing => Vec::new(),
                LevelChange::Since(lo) => unseal_from(&mut m, Some(lo), res_dir)?,
                // Unbounded: unseal everything.
                LevelChange::Everything => unseal_from(&mut m, None, res_dir)?,
            };
            // Post-unseal, so it already reflects any segments folded back in.
            // No watermark left means the whole axis is hot.
            let changed = match m.sealed_hi_secs {
                Some(hi) => LevelChange::Since(hi),
                None => LevelChange::Everything,
            };
            (m, unsealed, changed, false)
        } else {
            crate::rollup_cache::wipe_segment_res_dir(res_dir).map_other()?;
            (
                crate::rollup_cache::SegmentManifest {
                    format: crate::rollup_cache::SEALED_FORMAT.to_string(),
                    allowed_lateness_secs: policy.lateness_secs,
                    ..Default::default()
                },
                Vec::new(),
                LevelChange::Everything,
                true,
            )
        };

        let superseded = self
            .seal_and_recompute(
                ctx,
                pieces,
                ts,
                output_interval,
                res_dir,
                finer_table,
                ts,
                &mut m,
                policy,
            )
            .await?;
        m.sources = now.clone();
        m.source_digest = Some(finer_digest.to_string());

        let digest = crate::rollup_cache::write_segment_manifest(res_dir, &m)
            .await
            .map_other()?;
        // Only now that the manifest naming the merged segments is durable.
        unsealed.extend(superseded);
        crate::rollup_cache::remove_superseded(&unsealed).await;
        let provider = crate::rollup_cache::listing_table_for_res_dir(res_dir, &m, ts)
            .await
            .map_other()?;
        Ok(LevelBuild {
            provider,
            digest,
            changed,
            rebuilt,
        })
    }

    /// Plan and execute `sql`, streaming the result to `path` atomically. Returns
    /// the written file's blake3 digest and its row count. The row count lets the
    /// caller drop empty segments (gaps with no data) rather than record an
    /// empty segment file, while still advancing the watermark.
    async fn write_merge_to(
        &self,
        ctx: &datafusion::prelude::SessionContext,
        sql: &str,
        path: &std::path::Path,
    ) -> TinyFSResult<(String, u64)> {
        let stream = ctx
            .sql(sql)
            .await
            .map_other_context("rollup sealed-cache SQL planning failed")?
            .execute_stream()
            .await
            .map_other_context("rollup sealed-cache SQL execution failed")?;
        let schema = stream.schema();
        let rows = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let rows_w = rows.clone();
        let mapped = stream.map(move |r| {
            r.inspect(|batch| {
                let _ = rows_w.fetch_add(
                    batch.num_rows() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            })
            .map_err(|e| crate::error::Error::Arrow(e.to_string()))
        });
        let digest = crate::version_cache::write_parquet_atomic(path, schema, Box::pin(mapped))
            .await
            .map_other()?;
        Ok((digest, rows.load(std::sync::atomic::Ordering::Relaxed)))
    }

    /// Resolve this partition's `pattern_url` to concrete source file
    /// NodePaths, deduplicated by node id. Directories and non-file nodes are
    /// skipped.
    async fn resolve_source_files(&self) -> TinyFSResult<Vec<tinyfs::NodePath>> {
        // Resolve the partition-scoped pattern_url, not the original in_pattern
        // glob. pattern_url is the concrete file for a 1:1 source->output
        // mapping and the original glob only when several files share one
        // output, exactly matching the non-rollup SqlDerived source in
        // ensure_inner. Using in_pattern here would pull in sibling files from
        // other output partitions whose schemas differ, so the per-version
        // partial SQL would reference columns absent from those siblings.
        let url = crate::Url::parse(&self.pattern_url)
            .map_other_context(format!("Invalid pattern URL '{}'", self.pattern_url))?;
        let tinyfs_path = percent_encoding::percent_decode_str(url.path())
            .decode_utf8()
            .map_other_context(format!("Invalid UTF-8 in URL path '{}'", url.path()))?
            .to_string();

        let tinyfs_root = self
            .context
            .root()
            .await
            .map_other_context("Failed to get root")?;

        let is_exact =
            !tinyfs_path.contains('*') && !tinyfs_path.contains('?') && !tinyfs_path.contains('[');

        let matches: Vec<tinyfs::NodePath> = if is_exact {
            match tinyfs_root.resolve_path(&tinyfs_path).await {
                Ok((_wd, tinyfs::Lookup::Found(np))) => vec![np],
                _ => Vec::new(),
            }
        } else {
            tinyfs_root
                .collect_matches(&tinyfs_path)
                .await
                .map_other_context(format!("Failed to resolve pattern '{}'", tinyfs_path))?
                .into_iter()
                .map(|(np, _captures)| np)
                .collect()
        };

        use std::collections::HashSet;
        let mut seen = HashSet::new();
        let mut files = Vec::new();
        for np in matches {
            let file_id = np.id();
            if !file_id.entry_type().is_file() {
                continue;
            }
            if seen.insert(file_id.node_id()) {
                files.push(np);
            }
        }
        Ok(files)
    }

    async fn ensure_inner(&self) -> TinyFSResult<()> {
        crate::factory::lazy_sql_file::ensure_inner_series(&self.inner, &self.context, || async {
            // Create unique pattern name based on source path to avoid collisions
            // when multiple temporal reduce files are active in the same session
            // CRITICAL: Lowercase to match DataFusion's case-insensitive table name handling
            // Replace ALL non-alphanumeric characters with underscore so the name is
            // a valid unquoted SQL identifier (spaces, parens, slashes, etc.).
            let sanitized: String = self
                .source_path
                .chars()
                .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                .collect();
            let pattern_name = format!("source_{}", sanitized).to_lowercase();

            // Generate the SQL query with schema discovery, using the unique pattern name
            log::debug!(
                "[SEARCH] TEMPORAL-REDUCE: Generating SQL query for source path: {}",
                self.source_path
            );
            let sql_query = self
                .generate_sql_with_discovered_schema(&pattern_name)
                .await?;
            log::debug!(
                "[SEARCH] TEMPORAL-REDUCE: Generated SQL query: {}",
                sql_query
            );

            // Use the pattern_url for the SqlDerived source.  When the in_pattern
            // glob matched multiple files mapping to the same output, pattern_url
            // retains the glob so SqlDerivedFile can expand and UNION ALL the
            // matching files.  For single-match cases it is the concrete file URL.
            let source_url = crate::Url::parse(&self.pattern_url)
                .map_other_context(format!("Invalid pattern URL '{}'", self.pattern_url))?;

            let sql_config = SqlDerivedConfig::new(
                {
                    let mut patterns = HashMap::new();
                    _ = patterns.insert(pattern_name.clone(), source_url);
                    patterns
                },
                Some(sql_query.clone()),
            )
            .with_transforms(self.config.transforms.clone());

            log::debug!(
                "[SEARCH] TEMPORAL-REDUCE SqlDerivedConfig for '{}': query=\n{}",
                self.source_path,
                sql_query
            );

            Ok(sql_config)
        })
        .await
    }

    #[must_use]
    pub fn create_handle(self) -> tinyfs::FileHandle {
        tinyfs::FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }
}

crate::factory::lazy_sql_file::impl_lazy_sql_derived_file_metadata!(TemporalReduceSqlFile);

#[async_trait::async_trait]
impl tinyfs::QueryableFile for TemporalReduceSqlFile {
    async fn as_table_provider(
        &self,
        id: tinyfs::FileID,
        context: &tinyfs::ProviderContext,
    ) -> tinyfs::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        let cache_key = crate::TableProviderKey::new(id, crate::VersionSelection::LatestVersion)
            .to_cache_string();
        if let Some(cached) = context.get_table_provider_cache(&cache_key) {
            return Ok(cached);
        }

        // Prefer the incremental rollup cache. A rollup error is propagated
        // rather than masked by the single-pass delegate, so any rollup bug
        // surfaces instead of silently degrading to O(history) reads.
        if let Some(provider) = self.try_rollup_table_provider(id, context).await? {
            return Ok(provider);
        }

        log::debug!(
            "DELEGATING TemporalReduceSqlFile to inner SqlDerivedFile: id={}",
            id
        );
        self.ensure_inner().await?;
        let inner_guard = self.inner.lock().await;
        let inner = inner_guard
            .as_ref()
            .expect("inner initialized by ensure_inner");
        inner.as_table_provider(id, context).await
    }
}

/// Build the full source URL string for a matched source file under a format
/// provider scheme. Mirrors `SqlDerivedFile::node_file_url`.
fn node_file_url(scheme: &str, node_path: &tinyfs::NodePath) -> String {
    format!("{}://{}", scheme, node_path.path().display())
}

/// Canonical string over the parts of a temporal-reduce config that change the
/// meaning of the cached partials: the source pattern, the finest resolution at
/// which partials are stored, the time column, and the schema-filled
/// aggregation set. Feeds [`crate::rollup_cache::cfg_hash`].
///
/// The output resolution is deliberately excluded: partials are stored once at
/// the finest resolution and shared across every coarser resolution of the same
/// site, which re-bin them at merge time.
fn rollup_cfg_canonical(
    filled: &TemporalReduceConfig,
    finest_interval: Duration,
    pattern_url: &str,
) -> String {
    use std::fmt::Write;
    let mut s = String::new();
    let _ = write!(
        s,
        "src={};finest={}s;ts={};",
        pattern_url,
        finest_interval.as_secs(),
        filled.time_column
    );
    for agg in &filled.aggregations {
        let _ = write!(s, "agg={}:", agg.agg_type.to_sql());
        if let Some(cols) = &agg.columns {
            for col in cols {
                let _ = write!(s, "{},", col);
            }
        }
        s.push(';');
    }
    s
}

/// Parse the resolution list and validate that it nests: sorted ascending, each
/// coarser interval is an exact integer multiple of the next-finer one. Returns
/// the durations sorted finest-first.
///
/// Nesting guarantees a finer bucket never splits across a coarser boundary, so
/// every coarser resolution can be derived by re-binning the finest partials
/// rather than rescanning raw history.
fn parse_nesting_resolutions(resolutions: &[String]) -> TinyFSResult<Vec<Duration>> {
    let mut durations = Vec::with_capacity(resolutions.len());
    for res_str in resolutions {
        let d = humantime::parse_duration(res_str)
            .map_other_context(format!("Invalid resolution '{}'", res_str))?;
        if d.as_nanos() == 0 {
            return Err(tinyfs::Error::Other(format!(
                "Invalid resolution '{}': must be non-zero",
                res_str
            )));
        }
        durations.push(d);
    }
    durations.sort();
    for win in durations.windows(2) {
        let finer = win[0].as_nanos();
        let coarser = win[1].as_nanos();
        if coarser % finer != 0 {
            return Err(tinyfs::Error::Other(format!(
                "Non-nesting resolutions: {:?} is not an integer multiple of {:?}; \
                 each coarser resolution must be an integer multiple of the next finer one \
                 so cascaded rollups stay exact",
                win[1], win[0]
            )));
        }
    }
    Ok(durations)
}

fn duration_to_sql_interval(duration: Duration) -> String {
    let total_seconds = duration.as_secs();

    // Convert to appropriate SQL interval
    if total_seconds.is_multiple_of(86400) {
        // Days
        format!("INTERVAL {} DAY", total_seconds / 86400)
    } else if total_seconds.is_multiple_of(3600) {
        // Hours
        format!("INTERVAL {} HOUR", total_seconds / 3600)
    } else if total_seconds.is_multiple_of(60) {
        // Minutes
        format!("INTERVAL {} MINUTE", total_seconds / 60)
    } else {
        // Seconds
        format!("INTERVAL {} SECOND", total_seconds)
    }
}

/// Match a column name against a glob pattern using tinyfs glob matching logic
///
/// Supports simple patterns with a single `*` wildcard:
/// - `"Vulink*.Temperature.C"` matches `"Vulink1.Temperature.C"`, `"Vulink2.Temperature.C"`, etc.
/// - `"Vulink*"` matches any column starting with "Vulink"
/// - `"*.Temperature.C"` matches any column ending with ".Temperature.C"
/// - `"*"` matches any column name
///
/// # Design
/// This reuses the same wildcard matching logic from `tinyfs::glob::WildcardComponent::Wildcard`
/// without requiring the full path parsing infrastructure. Column names aren't paths, so we
/// apply the pattern matching directly to the string.
///
/// # Limitations
/// - Only supports a single `*` wildcard per pattern (no `**` or multiple `*`)
/// - Multiple wildcards fall back to exact string matching
/// - No bracket expressions or other advanced glob features
fn match_column_pattern(column: &str, pattern: &str) -> bool {
    // If no wildcard, do exact match
    if !pattern.contains('*') {
        return column == pattern;
    }

    // Parse the pattern as a single component (not a path)
    // We can reuse the Wildcard variant logic directly
    let asterisk_count = pattern.chars().filter(|&c| c == '*').count();

    // Only support single wildcard patterns for simplicity
    if asterisk_count != 1 {
        return column == pattern; // Fall back to exact match
    }

    let wildcard_idx = pattern.find('*').expect("wildcard checked");
    let prefix = if wildcard_idx > 0 {
        Some(&pattern[..wildcard_idx])
    } else {
        None
    };

    let suffix = if wildcard_idx < pattern.len() - 1 {
        Some(&pattern[wildcard_idx + 1..])
    } else {
        None
    };

    // Match logic from WildcardComponent::Wildcard
    let prefix_str = prefix.unwrap_or("");
    let suffix_str = suffix.unwrap_or("");

    column.starts_with(prefix_str)
        && column.ends_with(suffix_str)
        && column.len() >= prefix_str.len() + suffix_str.len()
}

/// Decomposable partial-aggregate kinds stored in the inner CTE.
///
/// Every supported output aggregation is reconstructed from one or more of
/// these associative partials so that the same machinery can later merge
/// partials across input versions and across cascaded resolutions. `Avg` is
/// never stored directly; it is lowered to `Sum` and `Count` and reconstructed
/// as `Sum / Count` in the final SELECT.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum PartialKind {
    Sum,
    Count,
    Min,
    Max,
    CountStar,
}

impl PartialKind {
    fn tag(self) -> &'static str {
        match self {
            PartialKind::Sum => "sum",
            PartialKind::Count => "count",
            PartialKind::Min => "min",
            PartialKind::Max => "max",
            PartialKind::CountStar => "cstar",
        }
    }

    /// SQL expression computing this partial over raw source rows, grouped into
    /// a time bucket. Aliased to the partial's internal column name.
    fn raw_expr(self, column: &str, alias: &str) -> String {
        match self {
            PartialKind::Sum => format!("SUM(\"{column}\") AS \"{alias}\""),
            PartialKind::Count => format!("COUNT(\"{column}\") AS \"{alias}\""),
            PartialKind::Min => format!("MIN(\"{column}\") AS \"{alias}\""),
            PartialKind::Max => format!("MAX(\"{column}\") AS \"{alias}\""),
            PartialKind::CountStar => format!("COUNT(*) AS \"{alias}\""),
        }
    }

    /// Like [`raw_expr`], but emits a typed placeholder when the source column
    /// is absent from this input version. An oteljson source builds a per-file
    /// wide schema from the metric names present in that file, so a metric that
    /// appears or is renamed over history is missing from older versions. The
    /// single-pass query never sees this because it reads the unioned glob where
    /// the absent column reads as NULL, but a per-version partial query plans
    /// against one version's schema and would fail on `SUM("missing")`. The
    /// placeholder keeps the partial output schema identical across versions,
    /// which the cross-version merge listing table requires, and contributes
    /// nothing on merge: NULL sums/mins/maxes are ignored and a zero count adds
    /// nothing.
    fn raw_expr_or_absent(self, column: &str, alias: &str, present: bool) -> String {
        if present || matches!(self, PartialKind::CountStar) {
            return self.raw_expr(column, alias);
        }
        match self {
            PartialKind::Sum | PartialKind::Min | PartialKind::Max => {
                format!("CAST(NULL AS DOUBLE) AS \"{alias}\"")
            }
            PartialKind::Count => format!("CAST(0 AS BIGINT) AS \"{alias}\""),
            PartialKind::CountStar => format!("COUNT(*) AS \"{alias}\""),
        }
    }

    /// SQL expression merging an already-computed partial across partitions
    /// (input versions, cascaded levels), grouped by `time_bucket`. The merge
    /// is associative: sums and counts add, mins/maxes extend. The result keeps
    /// the same alias so the reconstruction SELECT is identical whether it runs
    /// over raw-grouped or merged partials.
    fn merge_expr(self, alias: &str) -> String {
        match self {
            // SUM is nullable in DataFusion; COUNT is not. Single-pass COUNT
            // columns are non-nullable, so restore that with COALESCE here to
            // keep the merged output schema identical (counts can never be
            // NULL: a bucket only exists because it had rows).
            PartialKind::Count | PartialKind::CountStar => {
                format!("COALESCE(SUM(\"{alias}\"), 0) AS \"{alias}\"")
            }
            PartialKind::Sum => format!("SUM(\"{alias}\") AS \"{alias}\""),
            PartialKind::Min => format!("MIN(\"{alias}\") AS \"{alias}\""),
            PartialKind::Max => format!("MAX(\"{alias}\") AS \"{alias}\""),
        }
    }
}

/// A single decomposable partial column in the bucketed partials table.
struct PartialDef {
    kind: PartialKind,
    /// Source column name, or `"*"` for `COUNT(*)`.
    column: String,
    /// Internal partial column alias, e.g. `__p_sum_0`.
    alias: String,
}

/// The decomposed form of a temporal aggregation: the set of decomposable
/// partial columns plus the output-column reconstructions that reference them.
///
/// The same pieces drive three SQL forms:
/// - the single-pass query ([`AggSqlPieces::full_sql`]) used when no rollup
///   cache is available (identical to a direct `AVG/MIN/MAX/...` GROUP BY);
/// - the per-version partial query ([`AggSqlPieces::partial_sql`]) whose output
///   is cached once per immutable input version;
/// - the cross-version merge query ([`AggSqlPieces::merge_sql`]) that combines
///   cached partials back into the final output.
struct AggSqlPieces {
    partials: Vec<PartialDef>,
    /// Output column expressions, in the original aggregation/column order,
    /// each referencing one or more partial aliases.
    reconstruct_exprs: Vec<String>,
}

impl AggSqlPieces {
    /// Build the decomposed pieces from the (schema-filled) config.
    fn build(config: &TemporalReduceConfig) -> TinyFSResult<Self> {
        let mut aliases: HashMap<(PartialKind, String), String> = HashMap::new();
        let mut partials: Vec<PartialDef> = Vec::new();
        let mut reconstruct_exprs: Vec<String> = Vec::new();

        // Register a partial for (kind, column), deduplicating so e.g. Avg and
        // Sum on the same column share a single SUM partial. Returns its alias.
        fn register(
            kind: PartialKind,
            column: &str,
            aliases: &mut HashMap<(PartialKind, String), String>,
            partials: &mut Vec<PartialDef>,
        ) -> String {
            let key = (kind, column.to_string());
            if let Some(existing) = aliases.get(&key) {
                return existing.clone();
            }
            let alias = format!("__p_{}_{}", kind.tag(), partials.len());
            partials.push(PartialDef {
                kind,
                column: column.to_string(),
                alias: alias.clone(),
            });
            _ = aliases.insert(key, alias.clone());
            alias
        }

        for agg in &config.aggregations {
            let columns = agg.columns.as_ref().ok_or_else(|| {
                // This should never happen since
                // TemporalReduceSqlFile.generate_sql_with_discovered_schema()
                // fills in None columns before building the pieces.
                tinyfs::Error::Other(
                    "Internal error: AggSqlPieces::build called with None columns".to_string(),
                )
            })?;

            for column in columns {
                if column == "*" && matches!(agg.agg_type, AggregationType::Count) {
                    // Special case: count(*) becomes "timestamp.count".
                    let out_alias = "timestamp.count";
                    let p = register(PartialKind::CountStar, "*", &mut aliases, &mut partials);
                    reconstruct_exprs.push(format!("\"{p}\" AS \"{out_alias}\""));
                    continue;
                }

                // Generate alias in format: scope.parameter.unit.agg
                let out_alias = format!("{}.{}", column, agg.agg_type.to_sql().to_lowercase());
                let expr = match agg.agg_type {
                    AggregationType::Avg => {
                        // Avg is not associative; store Sum and Count partials
                        // and reconstruct Avg = Sum / Count. NULLIF guards the
                        // empty bucket so the result is NULL, matching AVG.
                        let sum = register(PartialKind::Sum, column, &mut aliases, &mut partials);
                        let count =
                            register(PartialKind::Count, column, &mut aliases, &mut partials);
                        format!(
                            "CAST(\"{sum}\" AS DOUBLE) / NULLIF(\"{count}\", 0) AS \"{out_alias}\""
                        )
                    }
                    AggregationType::Sum => {
                        let p = register(PartialKind::Sum, column, &mut aliases, &mut partials);
                        format!("\"{p}\" AS \"{out_alias}\"")
                    }
                    AggregationType::Count => {
                        let p = register(PartialKind::Count, column, &mut aliases, &mut partials);
                        format!("\"{p}\" AS \"{out_alias}\"")
                    }
                    AggregationType::Min => {
                        let p = register(PartialKind::Min, column, &mut aliases, &mut partials);
                        format!("\"{p}\" AS \"{out_alias}\"")
                    }
                    AggregationType::Max => {
                        let p = register(PartialKind::Max, column, &mut aliases, &mut partials);
                        format!("\"{p}\" AS \"{out_alias}\"")
                    }
                };
                reconstruct_exprs.push(expr);
            }
        }

        Ok(Self {
            partials,
            reconstruct_exprs,
        })
    }

    /// Partial expressions computed over raw source rows (one row per bucket).
    fn raw_partial_exprs(&self) -> Vec<String> {
        self.partials
            .iter()
            .map(|p| p.kind.raw_expr(&p.column, &p.alias))
            .collect()
    }

    /// Partial expressions for one input version, substituting typed
    /// placeholders for source columns absent from that version's schema.
    fn raw_partial_exprs_for(&self, available: &std::collections::HashSet<String>) -> Vec<String> {
        self.partials
            .iter()
            .map(|p| {
                let present = p.column == "*" || available.contains(&p.column);
                p.kind.raw_expr_or_absent(&p.column, &p.alias, present)
            })
            .collect()
    }

    /// Partial expressions merging cached partials across partitions.
    fn merge_partial_exprs(&self) -> Vec<String> {
        self.partials
            .iter()
            .map(|p| p.kind.merge_expr(&p.alias))
            .collect()
    }
}

/// Time-bucketing `date_bin` expression for an interval, binning from epoch.
///
/// DATE_TRUNC only supports single calendar units and discards the multiplier,
/// so DATE_TRUNC('hour', ts) is the same whether the config says 1h or 4h.
/// date_bin() properly handles multi-unit intervals like INTERVAL '4 HOUR'.
fn date_bin_expr(interval: &str, ts: &str) -> String {
    format!("date_bin({interval}, {ts}, TIMESTAMP '1970-01-01T00:00:00')")
}

/// Generate the single-pass SQL query for temporal aggregation.
///
/// The query groups the source into time buckets, computes the decomposable
/// partials (`Sum`, `Count`, `Min`, `Max`, `COUNT(*)`), and reconstructs the
/// requested output columns, including `Avg = Sum / Count`. Output column
/// names, ordering, and values are identical to a direct `AVG/MIN/MAX/...`
/// GROUP BY. This form is used when no rollup cache is available.
async fn generate_temporal_sql(
    config: &TemporalReduceConfig,
    interval: Duration,
    _source_path: &str,
    _context: &crate::FactoryContext,
    pattern_name: &str,
) -> TinyFSResult<String> {
    let pieces = AggSqlPieces::build(config)?;
    Ok(pieces.full_sql(interval, &config.time_column, pattern_name))
}

impl AggSqlPieces {
    /// Single-pass query: group raw rows into buckets, compute partials, and
    /// reconstruct the output columns in one statement.
    fn full_sql(&self, interval: Duration, ts: &str, table: &str) -> String {
        let interval = duration_to_sql_interval(interval);
        let bin = date_bin_expr(&interval, ts);
        // COALESCE forces DataFusion to infer a non-nullable timestamp schema,
        // even though the fallback is never used.
        format!(
            r#"
        WITH time_buckets AS (
          SELECT 
            {bin} AS time_bucket,
            {partial_exprs}
          FROM {table}
          WHERE {ts} IS NOT NULL
          GROUP BY {bin}
        )
        SELECT 
          COALESCE(CAST(time_bucket AS TIMESTAMP), CAST(0 AS TIMESTAMP)) AS {ts},
          {select_exprs}
        FROM time_buckets
        ORDER BY time_bucket
        "#,
            partial_exprs = self.raw_partial_exprs().join(",\n            "),
            select_exprs = self.reconstruct_exprs.join(",\n          "),
        )
    }

    /// Per-version partial query: group one immutable input version into
    /// buckets and emit `time_bucket` plus the partial columns. The output is
    /// cached once per version; it carries no reconstruction so the partials
    /// stay mergeable across versions.
    fn partial_sql(
        &self,
        interval: Duration,
        ts: &str,
        version_table: &str,
        available: &std::collections::HashSet<String>,
    ) -> String {
        let interval = duration_to_sql_interval(interval);
        let bin = date_bin_expr(&interval, ts);
        format!(
            r#"
        SELECT 
          {bin} AS time_bucket,
          {partial_exprs}
        FROM {version_table}
        WHERE {ts} IS NOT NULL
        GROUP BY {bin}
        "#,
            partial_exprs = self.raw_partial_exprs_for(available).join(",\n          "),
        )
    }

    /// Cross-version merge query: merge cached per-version partials by
    /// `time_bucket`, re-binning to `output_interval`, then reconstruct the
    /// output columns. The reconstruction is identical to the single-pass form,
    /// so a straddling boundary bucket present in two versions is recombined
    /// exactly.
    ///
    /// Partials are stored at the finest resolution; `output_interval` may be a
    /// coarser nesting multiple, in which case `date_bin` re-buckets the finer
    /// partials into the coarser bucket. When `output_interval` equals the
    /// partial interval the re-bin is an identity because the partial
    /// `time_bucket` values are already aligned to that interval.
    ///
    /// `lower_bound` / `upper_bound` scope the aggregate to a bucket window: when
    /// `lower_bound` is `Some(lo)`, only partials whose output bucket is `>= lo`
    /// are fed to the `GROUP BY`; when `upper_bound` is `Some(hi)`, only those
    /// `< hi` (exclusive). Both are in **epoch seconds** (matching
    /// `min_output_bucket`'s `lo_secs`), so the comparison is independent of the
    /// partials' timestamp unit. Bounding the aggregate keeps the hash table to
    /// one accumulator per bucket in the window instead of one per bucket over
    /// all history (design §1a / Phase 1), and lets Phase 2 compute a segment
    /// `[lo, hi)` and the open hot window `[hi, ∞)` as two bounded merges. The
    /// predicate is pushed *into* the partials scan (a `WHERE` before the
    /// `GROUP BY`) so it prunes input rows rather than aggregation output. It is
    /// exact because every partial row in one output bucket shares the same
    /// `date_bin` value, so no output bucket straddles a bound. `None`/`None`
    /// aggregates all history (cold rebuild after a cache miss).
    ///
    /// Retained as the reference definition of the reconstructed output and as
    /// the equivalence oracle in tests; production now writes mergeable partials
    /// via [`merge_partials_sql`] and reconstructs at read time via
    /// [`reconstruct_sql`], whose composition is exactly this query.
    #[cfg(test)]
    fn merge_sql(
        &self,
        output_interval: Duration,
        ts: &str,
        partials_table: &str,
        lower_bound: Option<i64>,
        upper_bound: Option<i64>,
    ) -> String {
        let interval = duration_to_sql_interval(output_interval);
        let bin = date_bin_expr(&interval, "time_bucket");
        let mut preds: Vec<String> = Vec::new();
        if let Some(lo) = lower_bound {
            preds.push(format!("CAST(EXTRACT(EPOCH FROM {bin}) AS BIGINT) >= {lo}"));
        }
        if let Some(hi) = upper_bound {
            preds.push(format!("CAST(EXTRACT(EPOCH FROM {bin}) AS BIGINT) < {hi}"));
        }
        let where_clause = if preds.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", preds.join(" AND "))
        };
        format!(
            r#"
        WITH merged AS (
          SELECT 
            {bin} AS time_bucket,
            {merge_exprs}
          FROM {partials_table}
          {where_clause}
          GROUP BY {bin}
        )
        SELECT 
          COALESCE(CAST(time_bucket AS TIMESTAMP), CAST(0 AS TIMESTAMP)) AS {ts},
          {select_exprs}
        FROM merged
        ORDER BY time_bucket
        "#,
            merge_exprs = self.merge_partial_exprs().join(",\n            "),
            select_exprs = self.reconstruct_exprs.join(",\n          "),
        )
    }

    /// Comma-separated list of the stored partial columns (`__p_*` aliases), in
    /// declaration order. This is the on-disk column set of a segment / hot
    /// file under [`crate::rollup_cache::SEALED_FORMAT`] = `partials-v1`, and the
    /// input columns the read-time reconstruction ([`reconstruct_sql`]) consumes.
    fn partial_column_list(&self) -> String {
        self.partials
            .iter()
            .map(|p| format!("\"{}\"", p.alias))
            .collect::<Vec<_>>()
            .join(",\n          ")
    }

    /// Like [`merge_sql`], but the final projection emits the *merged partial*
    /// columns instead of the reconstructed output columns. This is what the
    /// Phase 2 segments and hot file store (design §3 / Phase 3 step 1): keeping
    /// the mergeable partials (sum/count/min/max) on disk — rather than a
    /// reconstructed, non-associative `Avg` — lets a coarser resolution correctly
    /// fold a finer resolution's segments (Phase 3 step 2) and preserves exact
    /// cross-version/cross-segment merge semantics. Output columns are rebuilt at read
    /// time by [`reconstruct_sql`]. Bounds behave exactly as in [`merge_sql`].
    ///
    /// `in_bucket_col` is the input table's bucket-timestamp column: `time_bucket`
    /// when folding the shared finest partials (finest resolution), or the output
    /// timestamp column (`ts`) when folding a finer resolution's segments+hot, whose
    /// stored bucket column is that `ts`. Both are TIMESTAMP-typed and already
    /// aligned to the finer interval, so `date_bin` re-buckets them exactly into
    /// the coarser output interval (nesting guarantees no split).
    fn merge_partials_sql(
        &self,
        output_interval: Duration,
        ts: &str,
        partials_table: &str,
        in_bucket_col: &str,
        lower_bound: Option<i64>,
        upper_bound: Option<i64>,
    ) -> String {
        let interval = duration_to_sql_interval(output_interval);
        let bin = date_bin_expr(&interval, in_bucket_col);
        let mut preds: Vec<String> = Vec::new();
        if let Some(lo) = lower_bound {
            preds.push(format!("CAST(EXTRACT(EPOCH FROM {bin}) AS BIGINT) >= {lo}"));
        }
        if let Some(hi) = upper_bound {
            preds.push(format!("CAST(EXTRACT(EPOCH FROM {bin}) AS BIGINT) < {hi}"));
        }
        let where_clause = if preds.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", preds.join(" AND "))
        };
        format!(
            r#"
        WITH merged AS (
          SELECT 
            {bin} AS time_bucket,
            {merge_exprs}
          FROM {partials_table}
          {where_clause}
          GROUP BY {bin}
        )
        SELECT 
          COALESCE(CAST(time_bucket AS TIMESTAMP), CAST(0 AS TIMESTAMP)) AS {ts},
          {partial_cols}
        FROM merged
        ORDER BY time_bucket
        "#,
            merge_exprs = self.merge_partial_exprs().join(",\n            "),
            partial_cols = self.partial_column_list(),
        )
    }

    /// Read-time reconstruction over a table of stored partials: reproduce the
    /// output columns (identical names, order, and values to [`merge_sql`]'s
    /// projection, including `Avg = Sum / Count`) from the merged partial columns
    /// written by [`merge_partials_sql`]. `partials_table` is the registered
    /// listing table over the segments + hot file; `ts` passes the timestamp
    /// column through unchanged so the scan's declared ordering (the streaming
    /// read path) is preserved.
    fn reconstruct_sql(&self, ts: &str, partials_table: &str) -> String {
        format!(
            r#"
        SELECT 
          "{ts}",
          {select_exprs}
        FROM {partials_table}
        "#,
            select_exprs = self.reconstruct_exprs.join(",\n          "),
        )
    }
}
pub struct TemporalReduceDirectory {
    config: TemporalReduceConfig,
    context: crate::FactoryContext,
    parsed_resolutions: Vec<(String, Duration)>,
}

impl TemporalReduceDirectory {
    pub fn new(config: TemporalReduceConfig, context: crate::FactoryContext) -> TinyFSResult<Self> {
        // Parse all resolutions upfront, validating that they nest so cascaded
        // rollups stay exact.
        _ = parse_nesting_resolutions(&config.resolutions)?;
        let mut parsed_resolutions = Vec::new();

        for res_str in &config.resolutions {
            let duration = humantime::parse_duration(res_str)
                .map_other_context(format!("Invalid resolution '{}'", res_str))?;
            parsed_resolutions.push((res_str.clone(), duration));
        }

        Ok(Self {
            config,
            context,
            parsed_resolutions,
        })
    }

    /// Discover source files using the in_pattern and generate output names using out_pattern
    async fn discover_source_files(&self) -> TinyFSResult<Vec<(String, String)>> {
        let pattern = self.config.in_pattern.path();
        log::debug!(
            "TemporalReduceDirectory::discover_source_files - scanning pattern {}",
            pattern
        );

        let mut source_files = Vec::new();

        // Use context.root() to respect effective_root for cross-pond imports
        let root = self.context.root().await?;

        // Use collect_matches to find source files with the given pattern
        match root.collect_matches(pattern).await {
            Ok(matches) => {
                for (node_path, captured) in matches {
                    let source_path = node_path.path.to_string_lossy().to_string();

                    // Generate output name using out_pattern and captured groups
                    let output_name =
                        self.substitute_pattern(&self.config.out_pattern, &captured)?;
                    log::debug!(
                        "TemporalReduceDirectory::discover_source_files - found match {} -> output {}",
                        source_path,
                        output_name
                    );
                    source_files.push((source_path, output_name));
                }
            }
            Err(e) => {
                log::error!(
                    "TemporalReduceDirectory::discover_source_files - failed to match pattern {}: {}",
                    pattern,
                    e
                );
                return Err(tinyfs::Error::Other(format!(
                    "Failed to match source pattern: {}",
                    e
                )));
            }
        }

        let count = source_files.len();
        log::debug!(
            "TemporalReduceDirectory::discover_source_files - discovered {} source files",
            count
        );
        Ok(source_files)
    }

    /// Substitute pattern placeholders like $0, $1 with captured groups
    fn substitute_pattern(&self, pattern: &str, captured: &[String]) -> TinyFSResult<String> {
        let mut result = pattern.to_string();

        for (i, capture) in captured.iter().enumerate() {
            let placeholder = format!("${}", i);
            result = result.replace(&placeholder, capture);
        }

        Ok(result)
    }

    /// Get source node by path from discovered source files
    async fn get_source_node_by_path(&self, source_path: &str) -> TinyFSResult<Node> {
        // Use context.root() to respect effective_root for cross-pond imports
        let root = self.context.root().await?;

        let matches = root.collect_matches(source_path).await?;

        if matches.is_empty() {
            return Err(tinyfs::Error::NotFound(std::path::PathBuf::from(
                "Source file not found",
            )));
        }

        let (node_path, _) = matches.into_iter().next().expect("checked");
        Ok(node_path.node)
    }

    /// Create a site directory node with consistent logic (eliminates duplication)
    fn create_site_directory_node(
        &self,
        site_name: String,
        source_path: String,
        source_node: Node,
        pattern_url: String,
    ) -> Node {
        let site_directory = TemporalReduceSiteDirectory::new(
            site_name.clone(),
            source_path.clone(),
            source_node,
            pattern_url,
            self.config.clone(),
            self.context.clone(),
            self.parsed_resolutions.clone(),
        );

        // Create deterministic FileID for this site directory
        // Use site_name + in_pattern (not source_path) so the ID is stable
        // regardless of which representative file was picked.
        let mut id_bytes = Vec::new();
        id_bytes.extend_from_slice(site_name.as_bytes());
        id_bytes.extend_from_slice(self.config.in_pattern.to_string().as_bytes());
        id_bytes.extend_from_slice(b"temporal-reduce-site-directory");
        // Use this temporal reduce directory's NodeID as the PartID for children
        let parent_part_id = tinyfs::PartID::from_node_id(self.context.file_id.node_id());
        let file_id = tinyfs::FileID::from_content(
            parent_part_id,
            EntryType::DirectoryDynamic,
            &id_bytes,
            self.context.file_id.pond_id(),
        );

        Node::new(file_id, NodeType::Directory(site_directory.create_handle()))
    }

    /// Create a DirHandle from this temporal reduce directory
    #[must_use]
    pub fn create_handle(self) -> DirHandle {
        DirHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }
}

#[async_trait]
impl Directory for TemporalReduceDirectory {
    async fn get(&self, name: &str) -> TinyFSResult<Option<Node>> {
        // Discover all source files first
        let source_files = self.discover_source_files().await?;

        // Group source files by output_name, collecting all paths per output.
        // The last path (lexicographically) is used as the representative for
        // schema discovery.  For rotating log files with timestamps in the
        // filename this picks the newest file, which has the most complete
        // schema (columns are only ever added over time).
        let mut sites: HashMap<String, Vec<String>> = HashMap::new();
        for (source_path, output_name) in source_files {
            sites.entry(output_name).or_default().push(source_path);
        }

        // Look for the requested site directory name
        if let Some(source_paths) = sites.get(name) {
            // Use the lexicographically last path as the schema representative.
            // For timestamped filenames (casparwater-2022-*.json, ...-2025-*.json)
            // this picks the newest file with the most complete schema.
            let representative_path = source_paths.iter().max().unwrap_or(&source_paths[0]);

            // Get the source node for this site (any representative file works)
            let source_node = self.get_source_node_by_path(representative_path).await?;

            // Build the pattern URL.  When multiple source files map to the
            // same output name, use the original in_pattern glob so that
            // SqlDerivedFile expands and UNIONs all matching files.  When
            // there is a 1:1 mapping, use the concrete source URL.
            let pattern_url = if source_paths.len() > 1 {
                self.config.in_pattern.to_string()
            } else {
                let scheme = self.config.in_pattern.full_scheme();
                if representative_path.starts_with('/') {
                    format!("{}://{}", scheme, representative_path)
                } else {
                    format!("{}:///{}", scheme, representative_path)
                }
            };

            // Create the site directory using shared helper
            let node_ref = self.create_site_directory_node(
                name.to_string(),
                representative_path.clone(),
                source_node,
                pattern_url,
            );

            return Ok(Some(node_ref));
        }

        Ok(None)
    }

    async fn insert(&mut self, _name: String, _id: Node) -> TinyFSResult<()> {
        Err(tinyfs::Error::Other(
            "temporal-reduce directory is read-only".to_string(),
        ))
    }

    async fn remove(&mut self, _name: &str) -> TinyFSResult<Option<Node>> {
        Err(tinyfs::Error::Other(
            "temporal-reduce directory is read-only".to_string(),
        ))
    }

    async fn entries(
        &self,
    ) -> TinyFSResult<Pin<Box<dyn Stream<Item = TinyFSResult<tinyfs::DirectoryEntry>> + Send>>>
    {
        // Discover all source files - fail fast on error
        let source_files = self.discover_source_files().await?;

        // Group source files by output_name, collecting all paths per output
        let mut sites: HashMap<String, Vec<String>> = HashMap::new();
        for (source_path, output_name) in source_files {
            sites.entry(output_name).or_default().push(source_path);
        }

        // Create DirectoryEntry for each unique output_name (site)
        let mut entries = Vec::new();
        for (site_name, _source_path) in sites {
            // Get the actual node to extract its real node_id - fail fast on error
            let node = self.get(&site_name).await?.ok_or_else(|| {
                tinyfs::Error::Other(format!(
                    "Failed to create temporal-reduce site '{}'",
                    site_name
                ))
            })?;
            let dir_entry = tinyfs::DirectoryEntry::new(
                site_name.clone(),
                node.id.node_id(),
                EntryType::DirectoryDynamic,
                0,
            );
            entries.push(Ok(dir_entry));
        }

        let stream = stream::iter(entries);
        Ok(Box::pin(stream))
    }
}

#[async_trait]
impl tinyfs::Metadata for TemporalReduceDirectory {
    async fn metadata(&self) -> TinyFSResult<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: None,
            blake3: None,
            bao_outboard: None,
            entry_type: EntryType::DirectoryDynamic,
            timestamp: 0,
        })
    }
}

/// Intermediate directory for a specific site/output in temporal reduce
/// This creates the resolution files (res=1h.series, res=2h.series, etc.) for a single site
pub struct TemporalReduceSiteDirectory {
    site_name: String,
    source_path: String,
    source_node: Node,
    /// URL to use as the SqlDerived pattern source (may contain glob for multi-file cases).
    pattern_url: String,
    config: TemporalReduceConfig,
    context: crate::FactoryContext,
    parsed_resolutions: Vec<(String, Duration)>,
}

impl TemporalReduceSiteDirectory {
    #[must_use]
    pub fn new(
        site_name: String,
        source_path: String,
        source_node: Node,
        pattern_url: String,
        config: TemporalReduceConfig,
        context: crate::FactoryContext,
        parsed_resolutions: Vec<(String, Duration)>,
    ) -> Self {
        Self {
            site_name,
            source_path,
            source_node,
            pattern_url,
            config,
            context,
            parsed_resolutions,
        }
    }

    #[must_use]
    pub fn create_handle(self) -> DirHandle {
        DirHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }

    /// Create a temporal SQL file for this site and resolution
    async fn create_temporal_sql_file(
        &self,
        duration: Duration,
    ) -> TinyFSResult<tinyfs::FileHandle> {
        let sql_file = TemporalReduceSqlFile::new(
            self.config.clone(),
            duration,
            self.source_node.clone(),
            self.source_path.clone(),
            self.pattern_url.clone(),
            self.context.clone(),
        );

        Ok(tinyfs::FileHandle::new(Arc::new(tokio::sync::Mutex::new(
            Box::new(sql_file),
        ))))
    }
}

#[async_trait]
impl Directory for TemporalReduceSiteDirectory {
    async fn entries(
        &self,
    ) -> TinyFSResult<Pin<Box<dyn Stream<Item = TinyFSResult<tinyfs::DirectoryEntry>> + Send>>>
    {
        let mut entries = vec![];

        // Create DirectoryEntry for each resolution without creating the file yet
        for (res_str, _duration) in &self.parsed_resolutions {
            let filename = format!("res={}.series", res_str);

            // Create deterministic FileID for this entry - note: we already have the EntryType from dir_entry below
            // Use pattern_url (not source_path) so the ID is stable regardless
            // of which representative file was picked from a multi-file glob.
            let mut id_bytes = Vec::new();
            id_bytes.extend_from_slice(self.site_name.as_bytes());
            id_bytes.extend_from_slice(self.pattern_url.as_bytes());
            id_bytes.extend_from_slice(res_str.as_bytes());
            id_bytes.extend_from_slice(filename.as_bytes());
            id_bytes.extend_from_slice(b"temporal-reduce-site-entry");
            // Use this temporal reduce site directory's NodeID as the PartID for children
            let parent_part_id = tinyfs::PartID::from_node_id(self.context.file_id.node_id());
            let file_id = tinyfs::FileID::from_content(
                parent_part_id,
                EntryType::TableDynamic,
                &id_bytes,
                self.context.file_id.pond_id(),
            );

            let dir_entry = tinyfs::DirectoryEntry::new(
                filename.clone(),
                file_id.node_id(),
                EntryType::TableDynamic,
                0,
            );
            entries.push(Ok(dir_entry));
        }

        let stream = stream::iter(entries);
        Ok(Box::pin(stream))
    }

    async fn get(&self, name: &str) -> TinyFSResult<Option<Node>> {
        // Check if the requested name matches any of our resolution files
        for (res_str, duration) in &self.parsed_resolutions {
            let filename = format!("res={}.series", res_str);
            if filename == name {
                let sql_file = self.create_temporal_sql_file(*duration).await?;

                // Create deterministic FileID for this temporal-reduce entry
                // Use pattern_url (not source_path) so the ID is stable regardless
                // of which representative file was picked from a multi-file glob.
                let mut id_bytes = Vec::new();
                id_bytes.extend_from_slice(self.site_name.as_bytes());
                id_bytes.extend_from_slice(self.pattern_url.as_bytes());
                id_bytes.extend_from_slice(res_str.as_bytes());
                id_bytes.extend_from_slice(filename.as_bytes());
                id_bytes.extend_from_slice(b"temporal-reduce-site-entry");
                // Use this temporal reduce site directory's NodeID as the PartID for children
                let parent_part_id = tinyfs::PartID::from_node_id(self.context.file_id.node_id());
                let file_id = tinyfs::FileID::from_content(
                    parent_part_id,
                    EntryType::TableDynamic,
                    &id_bytes,
                    self.context.file_id.pond_id(),
                );

                let node_ref = Node::new(file_id, NodeType::File(sql_file));
                return Ok(Some(node_ref));
            }
        }
        Ok(None)
    }

    async fn insert(&mut self, _name: String, _node: Node) -> TinyFSResult<()> {
        // Temporal reduce site directories are read-only
        Err(tinyfs::Error::Other(
            "Temporal reduce site directories are read-only".to_string(),
        ))
    }

    async fn remove(&mut self, _name: &str) -> TinyFSResult<Option<Node>> {
        // Temporal reduce site directories are read-only
        Err(tinyfs::Error::Other(
            "Temporal reduce site directories are read-only".to_string(),
        ))
    }
}

#[async_trait]
impl tinyfs::Metadata for TemporalReduceSiteDirectory {
    async fn metadata(&self) -> TinyFSResult<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: None,
            blake3: None,
            bao_outboard: None,
            entry_type: EntryType::DirectoryDynamic,
            timestamp: 0,
        })
    }
}

/// Create a temporal reduce directory from configuration and context
fn create_temporal_reduce_directory(
    config: Value,
    context: crate::FactoryContext,
) -> TinyFSResult<DirHandle> {
    let temporal_config: TemporalReduceConfig =
        crate::factory::config_util::config_from_value(config, "Invalid temporal-reduce config")?;

    let directory = TemporalReduceDirectory::new(temporal_config, context)?;
    Ok(directory.create_handle())
}

/// Validate temporal reduce configuration
fn validate_temporal_reduce_config(config: &[u8]) -> TinyFSResult<Value> {
    let (config_value, temporal_config) = crate::factory::config_util::parse_yaml_config::<
        TemporalReduceConfig,
    >(config, "Invalid temporal-reduce config")?;

    // Additional validation: resolutions must parse and nest (each coarser
    // interval an integer multiple of the next finer) so cascaded rollups stay
    // exact.
    _ = parse_nesting_resolutions(&temporal_config.resolutions)?;

    // Surface a malformed allowed_lateness at config time, not mid-build.
    _ = temporal_config.allowed_lateness_secs()?;

    Ok(config_value)
}

// Register the temporal-reduce factory
register_dynamic_factory!(
    name: "temporal-reduce",
    description: "Create temporal downsampling views with configurable resolutions and aggregations",
    directory: create_temporal_reduce_directory,
    validate: validate_temporal_reduce_config
);

/// Content identity of one live source version: its blake3, qualified by node so
/// two nodes with byte-identical versions stay distinct entries.
///
/// A version with no recorded blake3 falls back to node+version, which is
/// unstable across a collapse and so degrades to "looks changed" -- a rebuild,
/// never a stale reuse.
fn source_version_key(node_id: &tinyfs::NodeID, v: &tinyfs::FileVersionInfo) -> String {
    match &v.blake3 {
        Some(h) => format!("{}:{}", node_id, h),
        None => format!("{}:v{}", node_id, v.version),
    }
}

/// Event-time range covered by one source version (epoch microseconds, `max`
/// inclusive), from the bounds tlogfs records on a series version.
///
/// A version without recorded bounds gets the whole axis. That is not a special
/// case to test for later: it makes any dirty range containing it unbounded, so
/// the build falls back to reading everything -- correct, merely not pruned.
fn source_range(v: &tinyfs::FileVersionInfo) -> crate::rollup_cache::SourceRange {
    let md = v.extended_metadata.as_ref();
    let get = |k: &str| {
        md.and_then(|m| m.get(k))
            .and_then(|s| s.parse::<i64>().ok())
    };
    match (get("min_event_time"), get("max_event_time")) {
        (Some(min_us), Some(max_us)) => crate::rollup_cache::SourceRange { min_us, max_us },
        _ => crate::rollup_cache::SourceRange::UNKNOWN,
    }
}

/// Oldest event time (epoch µs) touched by the difference between the cached
/// source set and the current one, or `None` if the difference reaches the
/// beginning of time (or a version with no recorded range).
///
/// The SYMMETRIC difference matters: a digest that disappeared invalidates the
/// buckets it fed just as much as a new one does.
fn dirty_lo_us(
    before: &std::collections::BTreeMap<String, crate::rollup_cache::SourceRange>,
    after: &std::collections::BTreeMap<String, crate::rollup_cache::SourceRange>,
) -> Option<i64> {
    let mut lo: Option<i64> = None;
    let mut note = |r: &crate::rollup_cache::SourceRange| {
        lo = Some(match lo {
            Some(cur) => cur.min(r.min_us),
            None => r.min_us,
        });
    };
    for (k, r) in after {
        if before.get(k) != Some(r) {
            note(r);
        }
    }
    for (k, r) in before {
        if !after.contains_key(k) {
            note(r);
        }
    }
    match lo {
        Some(v) if v == i64::MIN => None,
        other => other,
    }
}

/// Fold every sealed segment that covers a bucket at or after `dirty_lo_secs`
/// back into the hot window, returning their files (to delete once the manifest
/// that no longer names them is durable).
///
/// This is how late data is handled. A backfill below the watermark used to be a
/// hard error demanding `--rebuild`, because segments were keyed by which source
/// versions produced them and there was no way to ask "which segments hold this
/// instant". Segments are keyed by bucket range now, so that question is a
/// lookup: drop the ones that answer it, lower `sealed_hi` to the oldest drop,
/// and the ordinary seal path recomputes those buckets from source and re-seals
/// them. No merge-in-place, no separate late-data path.
///
/// `dirty_lo_secs = None` means the dirty range reaches the beginning of time, so
/// everything unseals -- which is a rebuild, expressed in the same terms.
fn unseal_from(
    m: &mut crate::rollup_cache::SegmentManifest,
    dirty_lo_secs: Option<i64>,
    res_dir: &std::path::Path,
) -> TinyFSResult<Vec<std::path::PathBuf>> {
    // Nothing sealed reaches the dirty range: no unsealing to do.
    let Some(sealed_hi) = m.sealed_hi_secs else {
        return Ok(Vec::new());
    };
    if dirty_lo_secs.is_some_and(|lo| sealed_hi <= lo) {
        return Ok(Vec::new());
    }

    // A segment covers [lo, hi); it is dirty iff it holds any bucket >= dirty_lo.
    let dirty = |r: &crate::rollup_cache::Segment| match dirty_lo_secs {
        Some(lo) => r.hi_secs > lo,
        None => true,
    };
    let (dropped, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut m.segments)
        .into_iter()
        .partition(|r| dirty(r));
    m.segments = kept;

    // The new watermark is the dirty point, lowered further to the bottom edge
    // of any segment straddling it -- a segment is dropped whole, so everything
    // it covered is hot again. A `None` bottom edge reached the beginning of
    // time, so nothing stays sealed.
    //
    // This is computed from the dirty point rather than from the dropped
    // segments alone, because the watermark can legitimately have advanced over
    // a span that produced no segment file: sealing an empty range (a gap in the
    // data) records the watermark and writes nothing. Deriving the new watermark
    // only from dropped files would leave it above the dirty point with no
    // segment covering the difference, silently losing those buckets.
    let mut new_hi: Option<i64> = dirty_lo_secs;
    let mut open_ended = dirty_lo_secs.is_none();
    for r in &dropped {
        match r.lo_secs {
            Some(lo) => new_hi = Some(new_hi.map_or(lo, |cur: i64| cur.min(lo))),
            None => open_ended = true,
        }
    }
    m.sealed_hi_secs = if open_ended { None } else { new_hi };

    // Those bytes are hot again, so account for them: the seal gate reads
    // `hot_bytes` to decide whether freezing is worthwhile, and the next build
    // rewrites exactly this much.
    m.hot_bytes = m
        .hot_bytes
        .saturating_add(dropped.iter().map(|r| r.bytes).sum());

    Ok(dropped
        .iter()
        .map(|r| crate::rollup_cache::segment_path(res_dir, &r.name))
        .collect())
}

/// The cached source Parquets to aggregate, pruned to versions that can reach
/// `read_lo_us` (epoch µs). `None` reads every live version.
fn bounded_source_set(
    cache_dir: &std::path::Path,
    scheme: &str,
    source_nodes: &[(tinyfs::NodeID, Vec<tinyfs::FileVersionInfo>)],
    read_lo_us: Option<i64>,
) -> crate::version_cache::CachedSet {
    let bounds = read_lo_us.map_or(tinyfs::SeriesReadBounds::NONE, |lo| {
        tinyfs::SeriesReadBounds::from_event_time_lo(lo)
    });
    crate::format_cache::glob_cached_set_bounded(cache_dir, scheme, source_nodes, &bounds)
}

/// Epoch microseconds -> epoch seconds, rounding DOWN (toward -inf, so it holds
/// for pre-epoch times too). Every µs->s conversion here widens the range it
/// bounds, so a rounding error can only cause extra work, never dropped data.
fn floor_us_to_secs(us: i64) -> i64 {
    us.div_euclid(1_000_000)
}

/// Epoch seconds -> the start of the output bucket containing them, rounding
/// DOWN. `sealed_hi_secs` and every segment edge are bucket boundaries, and the
/// read path filters on a bucket START, so any value used as a watermark must
/// be aligned or the bucket straddling it belongs to neither the sealed nor the
/// hot side.
fn floor_secs_to_bucket(secs: i64, interval: Duration) -> i64 {
    let i = interval.as_secs() as i64;
    if i <= 0 { secs } else { secs.div_euclid(i) * i }
}

fn secs_to_us(secs: i64) -> i64 {
    secs.saturating_mul(1_000_000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FactoryRegistry;
    use arrow::array::{Float64Array, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use tinyfs::{EntryType, FileID, NodeType};

    use crate::factory::test_support::{
        create_parquet_file, create_test_environment, test_context,
    };

    fn cfg_with_aggs(aggregations: Vec<AggregationConfig>) -> TemporalReduceConfig {
        TemporalReduceConfig {
            in_pattern: crate::Url::parse("series:///sources/*").unwrap(),
            out_pattern: "$0".to_string(),
            time_column: "timestamp".to_string(),
            resolutions: vec!["1d".to_string()],
            aggregations,
            transforms: None,
            allowed_lateness: None,
            // Seal on every advance, as before the size gate existed: these
            // tests assert seal MECHANICS on datasets far under any real target.
            seal_target_bytes: Some(0),
            max_live_segments: None,
        }
    }

    fn agg(t: AggregationType, cols: &[&str]) -> AggregationConfig {
        AggregationConfig {
            agg_type: t,
            columns: Some(cols.iter().map(|c| c.to_string()).collect()),
        }
    }

    fn lowering_context() -> crate::FactoryContext {
        // generate_temporal_sql does not read the context; any valid one works.
        let provider_context = crate::factory::test_support::create_provider_context();
        test_context(&provider_context, FileID::root())
    }

    /// Phase 1: Avg must be lowered to decomposable Sum/Count partials and
    /// reconstructed as Sum/Count in the final SELECT. No bare AVG(...) is
    /// emitted, and the output column alias is preserved.
    #[tokio::test]
    async fn test_avg_lowered_to_sum_over_count() {
        let config = cfg_with_aggs(vec![agg(AggregationType::Avg, &["temperature"])]);
        let ctx = lowering_context();
        let sql = generate_temporal_sql(&config, Duration::from_secs(86400), "x", &ctx, "source_t")
            .await
            .unwrap();

        assert!(
            !sql.contains("AVG("),
            "Avg must not be emitted directly: {sql}"
        );
        assert!(
            sql.contains("SUM(\"temperature\")"),
            "expected Sum partial: {sql}"
        );
        assert!(
            sql.contains("COUNT(\"temperature\")"),
            "expected Count partial: {sql}"
        );
        assert!(
            sql.contains("AS \"temperature.avg\""),
            "expected reconstructed avg alias: {sql}"
        );
        assert!(
            sql.contains("NULLIF("),
            "Avg reconstruction must guard empty buckets: {sql}"
        );
    }

    /// Avg and Sum on the same column share a single SUM partial (dedup).
    #[tokio::test]
    async fn test_partials_deduplicated_across_aggregations() {
        let config = cfg_with_aggs(vec![
            agg(AggregationType::Avg, &["temperature"]),
            agg(AggregationType::Sum, &["temperature"]),
        ]);
        let ctx = lowering_context();
        let sql = generate_temporal_sql(&config, Duration::from_secs(86400), "x", &ctx, "source_t")
            .await
            .unwrap();

        assert_eq!(
            sql.matches("SUM(\"temperature\")").count(),
            1,
            "Sum partial must be computed once and reused: {sql}"
        );
        assert!(sql.contains("AS \"temperature.avg\""), "{sql}");
        assert!(sql.contains("AS \"temperature.sum\""), "{sql}");
    }

    /// Min/Max/Count(*) map to their decomposable partials with preserved aliases.
    #[tokio::test]
    async fn test_min_max_count_partials() {
        let config = cfg_with_aggs(vec![
            agg(AggregationType::Min, &["temperature"]),
            agg(AggregationType::Max, &["temperature"]),
            agg(AggregationType::Count, &["*"]),
        ]);
        let ctx = lowering_context();
        let sql = generate_temporal_sql(&config, Duration::from_secs(86400), "x", &ctx, "source_t")
            .await
            .unwrap();

        assert!(sql.contains("MIN(\"temperature\")"), "{sql}");
        assert!(sql.contains("MAX(\"temperature\")"), "{sql}");
        assert!(sql.contains("COUNT(*)"), "{sql}");
        assert!(sql.contains("AS \"temperature.min\""), "{sql}");
        assert!(sql.contains("AS \"temperature.max\""), "{sql}");
        assert!(sql.contains("AS \"timestamp.count\""), "{sql}");
    }

    /// SQL-split correctness: running the per-version partial query and then the
    /// cross-version merge query over a SINGLE input version must reproduce the
    /// single-pass query's output exactly (byte-for-byte per column). With one
    /// version each bucket has exactly one partial row, so the merge is an
    /// identity over the partials and no float reassociation occurs.
    #[tokio::test]
    async fn test_partial_then_merge_equals_full_single_pass() {
        use arrow::array::{Float64Array, TimestampMillisecondArray};
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        // Two daily buckets of sub-daily samples with distinct values so avg,
        // min, max, sum, and count all exercise non-trivial reconstruction.
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("temperature", DataType::Float64, true),
            Field::new("salinity", DataType::Float64, true),
        ]));
        let day = 86_400_000i64;
        let ts = vec![0, day / 4, day / 2, day, day + day / 4, day + day / 2];
        let temp = vec![20.0, 21.5, 19.0, 25.0, 24.0, 26.5];
        let sal = vec![30.0, 31.0, 32.0, 33.0, 34.0, 35.0];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(ts)),
                Arc::new(Float64Array::from(temp)),
                Arc::new(Float64Array::from(sal)),
            ],
        )
        .unwrap();

        let config = cfg_with_aggs(vec![
            agg(AggregationType::Avg, &["temperature"]),
            agg(AggregationType::Min, &["temperature"]),
            agg(AggregationType::Max, &["temperature"]),
            agg(AggregationType::Sum, &["salinity"]),
            agg(AggregationType::Count, &["*"]),
        ]);
        let pieces = AggSqlPieces::build(&config).unwrap();
        let interval = Duration::from_secs(86_400);

        let ctx = SessionContext::new();
        let src = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
        _ = ctx.register_table("src", Arc::new(src)).unwrap();

        // Single-pass output.
        let full_sql = pieces.full_sql(interval, "timestamp", "src");
        let full = ctx.sql(&full_sql).await.unwrap().collect().await.unwrap();

        // Per-version partials -> register -> merge.
        let available: std::collections::HashSet<String> =
            ["temperature".to_string(), "salinity".to_string()]
                .into_iter()
                .collect();
        let partial_sql = pieces.partial_sql(interval, "timestamp", "src", &available);
        let partials = ctx
            .sql(&partial_sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let partials_schema = partials[0].schema();
        let partials_table = MemTable::try_new(partials_schema, vec![partials]).unwrap();
        _ = ctx
            .register_table("partials", Arc::new(partials_table))
            .unwrap();
        let merge_sql = pieces.merge_sql(interval, "timestamp", "partials", None, None);
        let merged = ctx.sql(&merge_sql).await.unwrap().collect().await.unwrap();

        let full = arrow::compute::concat_batches(&full[0].schema(), &full).unwrap();
        let merged = arrow::compute::concat_batches(&merged[0].schema(), &merged).unwrap();

        assert_eq!(
            full.schema().fields(),
            merged.schema().fields(),
            "merge output schema must match single-pass"
        );
        assert_eq!(full.num_rows(), merged.num_rows());
        for col in 0..full.num_columns() {
            assert_eq!(
                full.column(col).to_data(),
                merged.column(col).to_data(),
                "column {} ({}) differs between single-pass and partial+merge",
                col,
                full.schema().field(col).name()
            );
        }
    }

    /// A source column that is absent from an older input version must not break
    /// the per-version partial query: an oteljson source builds a per-file wide
    /// schema from the metric names present in that file, so a metric that is
    /// added or renamed over history is missing from earlier versions. The
    /// partial query for such a version emits typed placeholders for the absent
    /// column, keeping the partial schema identical across versions so the merge
    /// can union them, and the merged output matches a single-pass query over
    /// the union of both versions.
    #[tokio::test]
    async fn test_partial_tolerates_column_absent_in_older_version() {
        use arrow::array::{Float64Array, TimestampMillisecondArray};
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        let day = 86_400_000i64;

        // Older version: only "temperature" exists; "salinity" had not yet
        // appeared in the source.
        let v0_schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("temperature", DataType::Float64, true),
        ]));
        let v0 = RecordBatch::try_new(
            v0_schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![0, day / 4])),
                Arc::new(Float64Array::from(vec![20.0, 22.0])),
            ],
        )
        .unwrap();

        // Newer version: both columns present.
        let v1_schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("temperature", DataType::Float64, true),
            Field::new("salinity", DataType::Float64, true),
        ]));
        let v1 = RecordBatch::try_new(
            v1_schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![day, day + day / 4])),
                Arc::new(Float64Array::from(vec![25.0, 24.0])),
                Arc::new(Float64Array::from(vec![33.0, 34.0])),
            ],
        )
        .unwrap();

        let config = cfg_with_aggs(vec![
            agg(AggregationType::Avg, &["temperature"]),
            agg(AggregationType::Sum, &["salinity"]),
            agg(AggregationType::Min, &["salinity"]),
            agg(AggregationType::Max, &["salinity"]),
            agg(AggregationType::Count, &["*"]),
        ]);
        let pieces = AggSqlPieces::build(&config).unwrap();
        let interval = Duration::from_secs(86_400);
        let ctx = SessionContext::new();

        // Per-version partials, each planned against that version's own schema.
        let mut all_partials = Vec::new();
        for (i, (batch, schema)) in [(v0, v0_schema), (v1, v1_schema)].into_iter().enumerate() {
            let table = format!("v{i}");
            let available: std::collections::HashSet<String> =
                schema.fields().iter().map(|f| f.name().clone()).collect();
            let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
            _ = ctx.register_table(table.as_str(), Arc::new(mem)).unwrap();
            let partial_sql = pieces.partial_sql(interval, "timestamp", &table, &available);
            let mut part = ctx
                .sql(&partial_sql)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            all_partials.append(&mut part);
        }

        // All partial batches must share one schema so the merge can union them.
        let partials_schema = all_partials[0].schema();
        for p in &all_partials {
            assert_eq!(
                p.schema(),
                partials_schema,
                "per-version partial schemas must be identical across versions"
            );
        }
        let partials_table = MemTable::try_new(partials_schema, vec![all_partials]).unwrap();
        _ = ctx
            .register_table("partials", Arc::new(partials_table))
            .unwrap();
        let merge_sql = pieces.merge_sql(interval, "timestamp", "partials", None, None);
        let merged = ctx.sql(&merge_sql).await.unwrap().collect().await.unwrap();
        let merged = arrow::compute::concat_batches(&merged[0].schema(), &merged).unwrap();

        // Two daily buckets: day 0 (temperature only) and day 1 (both columns).
        assert_eq!(merged.num_rows(), 2);

        let merged_schema = merged.schema();
        let names: Vec<&str> = merged_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let sal_sum_idx = names.iter().position(|n| *n == "salinity.sum").unwrap();
        let sal_sum = merged
            .column(sal_sum_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        use arrow::array::Array;
        // Day 0 had no salinity column at all -> NULL; day 1 -> 33 + 34 = 67.
        assert!(sal_sum.is_null(0), "day-0 salinity sum must be NULL");
        assert_eq!(sal_sum.value(1), 67.0);
    }

    /// each with 3 days of hourly data, then validates that temporal-reduce correctly:
    /// - Discovers all source files matching the pattern
    /// - Creates site subdirectories for each matched file
    /// - Generates daily aggregated files with correct avg/min/max values
    /// - Produces exactly 3 rows (one per day) for each site
    #[tokio::test]
    async fn test_temporal_reduce_multi_site_daily_aggregation() {
        let _ = env_logger::try_init();

        let (fs, provider_context) = create_test_environment().await;

        // Create source series files with 3 days of hourly data
        {
            let root = fs.root().await.unwrap();
            _ = root.create_dir_path("/sources").await.unwrap();

            // Schema: timestamp + temperature + salinity
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("temperature", DataType::Float64, false),
                Field::new("salinity", DataType::Float64, false),
            ]));

            // Create 3 series files, each with 3 days of hourly data
            // Day boundaries: Day 0 = 0-86399999ms, Day 1 = 86400000-172799999ms, Day 2 = 172800000-259199999ms
            for site_num in 1..=3 {
                let filename = format!("/sources/site{}.series", site_num);
                let base_temp = 15.0 + (site_num as f64) * 5.0; // site1=20, site2=25, site3=30
                let base_salinity = 30.0 + (site_num as f64) * 2.0; // site1=32, site2=34, site3=36

                let mut all_timestamps = Vec::new();
                let mut all_temps = Vec::new();
                let mut all_salinities = Vec::new();

                // 3 days x 24 hours = 72 data points per site
                for day in 0..3 {
                    for hour in 0..24 {
                        let timestamp_ms = (day * 86400 + hour * 3600) * 1000; // milliseconds
                        all_timestamps.push(timestamp_ms);
                        all_temps.push(base_temp + hour as f64); // temperature increases by hour
                        all_salinities.push(base_salinity + hour as f64 * 0.1);
                    }
                }

                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampMillisecondArray::from(all_timestamps)),
                        Arc::new(Float64Array::from(all_temps)),
                        Arc::new(Float64Array::from(all_salinities)),
                    ],
                )
                .unwrap();

                // Write the series file using create_parquet_file helper
                let mut buf = Vec::new();
                {
                    let mut writer = parquet::arrow::arrow_writer::ArrowWriter::try_new(
                        &mut buf,
                        schema.clone(),
                        None,
                    )
                    .unwrap();
                    writer.write(&batch).unwrap();
                    let _ = writer.close().unwrap();
                }

                let _ = create_parquet_file(&fs, &filename, buf, EntryType::TablePhysicalSeries)
                    .await
                    .unwrap();
            }
        }

        // Create temporal-reduce directory and verify aggregations
        {
            // Create temporal-reduce configuration
            let config = TemporalReduceConfig {
                in_pattern: crate::Url::parse("series:///sources/*.series").unwrap(),
                out_pattern: "$0".to_string(), // Preserve filename as site directory name
                time_column: "timestamp".to_string(),
                resolutions: vec!["1d".to_string()], // Daily aggregation
                aggregations: vec![
                    AggregationConfig {
                        agg_type: AggregationType::Avg,
                        columns: Some(vec!["temperature".to_string(), "salinity".to_string()]),
                    },
                    AggregationConfig {
                        agg_type: AggregationType::Min,
                        columns: Some(vec!["temperature".to_string()]),
                    },
                    AggregationConfig {
                        agg_type: AggregationType::Max,
                        columns: Some(vec!["temperature".to_string()]),
                    },
                ],
                transforms: None,
                allowed_lateness: None,
                // Seal on every advance, as before the size gate existed: these
                // tests assert seal MECHANICS on datasets far under any real target.
                seal_target_bytes: Some(0),
                max_live_segments: None,
            };

            let context = test_context(&provider_context, FileID::root());
            let temporal_dir = TemporalReduceDirectory::new(config, context).unwrap();
            let temporal_handle = temporal_dir.create_handle();

            // Verify we have 3 site directories
            // Note: The pattern "*.series" wildcard captures only the part matching "*" (e.g., "site1")
            // The out_pattern "$0" uses the first capture group, so directory names are just "site1", "site2", "site3"
            use futures::StreamExt;
            let mut entries_stream = temporal_handle.entries().await.unwrap();
            let mut site_names = Vec::new();
            while let Some(entry_result) = entries_stream.next().await {
                let entry = entry_result.unwrap();
                site_names.push(entry.name.clone());
            }
            site_names.sort();

            // The glob pattern "/sources/*.series" captures just the part matching * (without .series)
            // so we expect "site1", "site2", "site3" as directory names
            assert_eq!(
                site_names,
                vec!["site1", "site2", "site3"],
                "Should find all 3 site directories (without .series extension)"
            );

            // Verify each site has correct daily aggregations
            for site_num in 1..=3 {
                let site_name = format!("site{}", site_num);
                let base_temp = 15.0 + (site_num as f64) * 5.0;

                // Get the site directory
                let site_node = temporal_handle.get(&site_name).await.unwrap().unwrap();
                let site_dir = match &site_node.node_type {
                    NodeType::Directory(dir) => dir,
                    _ => panic!("Expected directory for {}", site_name),
                };

                // Get the daily aggregation file
                let daily_file_node = site_dir.get("res=1d.series").await.unwrap().unwrap();
                let file_id = daily_file_node.id;

                // Read and verify the aggregated data using QueryableFile interface
                if let NodeType::File(file_handle) = &daily_file_node.node_type {
                    let file_arc = file_handle.get_file().await;
                    let file_guard = file_arc.lock().await;

                    // Get table provider to query the data
                    let queryable_file = file_guard
                        .as_queryable()
                        .expect("Temporal-reduce should create QueryableFile");

                    let table_provider = queryable_file
                        .as_table_provider(file_id, &provider_context)
                        .await
                        .unwrap();

                    // Execute query to get results
                    let ctx = &provider_context.datafusion_session;
                    let table_name = format!("temporal_{}", site_num);
                    _ = ctx.register_table(&table_name, table_provider).unwrap();

                    let query = format!("SELECT * FROM {} ORDER BY timestamp", table_name);
                    let dataframe = ctx.sql(&query).await.unwrap();
                    let batches = dataframe.collect().await.unwrap();

                    // Should have exactly 3 rows (one per day)
                    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    assert_eq!(
                        total_rows, 3,
                        "Site {} should have exactly 3 daily aggregated rows",
                        site_num
                    );

                    // Verify schema has expected columns
                    assert!(!batches.is_empty(), "Should have at least one batch");
                    let result_schema = &batches[0].schema();
                    assert!(
                        result_schema.column_with_name("timestamp").is_some(),
                        "Should have timestamp column"
                    );
                    assert!(
                        result_schema.column_with_name("temperature.avg").is_some(),
                        "Should have temperature.avg column"
                    );
                    assert!(
                        result_schema.column_with_name("salinity.avg").is_some(),
                        "Should have salinity.avg column"
                    );
                    assert!(
                        result_schema.column_with_name("temperature.min").is_some(),
                        "Should have temperature.min column"
                    );
                    assert!(
                        result_schema.column_with_name("temperature.max").is_some(),
                        "Should have temperature.max column"
                    );

                    // Verify values for first day (day 0)
                    // Each day has 24 hours: hour 0-23
                    // Expected avg temperature: base_temp + (0+1+2+...+23)/24 = base_temp + 11.5
                    // Expected min temperature: base_temp + 0 = base_temp
                    // Expected max temperature: base_temp + 23 = base_temp + 23
                    let first_batch = &batches[0];
                    let temp_avg_col = first_batch
                        .column_by_name("temperature.avg")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();

                    let temp_min_col = first_batch
                        .column_by_name("temperature.min")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();

                    let temp_max_col = first_batch
                        .column_by_name("temperature.max")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();

                    let expected_avg = base_temp + 11.5; // Average of 0..23
                    let expected_min = base_temp;
                    let expected_max = base_temp + 23.0;

                    assert!(
                        (temp_avg_col.value(0) - expected_avg).abs() < 0.01,
                        "Site {} day 0: expected avg={}, got={}",
                        site_num,
                        expected_avg,
                        temp_avg_col.value(0)
                    );
                    assert_eq!(
                        temp_min_col.value(0),
                        expected_min,
                        "Site {} day 0: min temperature mismatch",
                        site_num
                    );
                    assert_eq!(
                        temp_max_col.value(0),
                        expected_max,
                        "Site {} day 0: max temperature mismatch",
                        site_num
                    );

                    log::info!(
                        "[OK] Site {} verified: {} rows, avg={:.2}, min={:.2}, max={:.2}",
                        site_num,
                        total_rows,
                        temp_avg_col.value(0),
                        temp_min_col.value(0),
                        temp_max_col.value(0)
                    );
                } else {
                    panic!("Expected file node for {}/res=1d.series", site_name);
                }
            }
        }

        log::info!("[OK] Temporal-reduce integration test completed successfully");
        log::info!("   - Created 3 series files with 72 hourly records each (3 days)");
        log::info!("   - Verified temporal-reduce discovered all 3 sites");
        log::info!("   - Validated daily aggregations: 3 rows per site");
        log::info!("   - Confirmed avg/min/max calculations are correct");
    }

    #[test]
    fn test_factory_registration() {
        let factory = FactoryRegistry::get_factory("temporal-reduce");
        assert!(factory.is_some());
        let factory = factory.unwrap();
        assert_eq!(factory.name, "temporal-reduce");
        assert!(factory.description.contains("temporal downsampling"));
    }

    /// Test that source_url() preserves the URL scheme from in_pattern.
    /// This is critical: format providers (oteljson://, csv://) must not be
    /// silently rewritten to series:// or file://.
    #[tokio::test]
    async fn test_source_url_preserves_scheme() {
        let (fs, provider_context) = create_test_environment().await;

        // Create a dummy source node (we only need it for TemporalReduceSqlFile construction)
        let root = fs.root().await.unwrap();
        _ = root.create_dir_path("/ingest").await.unwrap();
        use tokio::io::AsyncWriteExt;
        let mut w = root
            .async_writer_path_with_type("/ingest/test.json", EntryType::FilePhysicalVersion)
            .await
            .unwrap();
        w.write_all(b"test").await.unwrap();
        w.flush().await.unwrap();
        w.shutdown().await.unwrap();
        let node_path = root.get_node_path("/ingest/test.json").await.unwrap();

        let make_file = |scheme: &str, path: &str| {
            let in_pattern = crate::Url::parse(&format!("{}://{}", scheme, path)).unwrap();
            let pattern_url = in_pattern.to_string();
            let config = TemporalReduceConfig {
                in_pattern,
                out_pattern: "out".to_string(),
                time_column: "timestamp".to_string(),
                resolutions: vec!["1h".to_string()],
                aggregations: vec![],
                transforms: None,
                allowed_lateness: None,
                // Seal on every advance, as before the size gate existed: these
                // tests assert seal MECHANICS on datasets far under any real target.
                seal_target_bytes: Some(0),
                max_live_segments: None,
            };
            TemporalReduceSqlFile::new(
                config,
                Duration::from_secs(3600),
                node_path.node.clone(),
                path.to_string(),
                pattern_url,
                test_context(&provider_context, FileID::root()),
            )
        };

        // Format providers must preserve their scheme
        let oteljson_file = make_file("oteljson", "/ingest/test.json");
        assert_eq!(
            oteljson_file.source_url(),
            "oteljson:///ingest/test.json",
            "oteljson:// scheme must be preserved"
        );

        let csv_file = make_file("csv", "/data/metrics.csv");
        assert_eq!(
            csv_file.source_url(),
            "csv:///data/metrics.csv",
            "csv:// scheme must be preserved"
        );

        let excelhtml_file = make_file("excelhtml", "/data/report.html");
        assert_eq!(
            excelhtml_file.source_url(),
            "excelhtml:///data/report.html",
            "excelhtml:// scheme must be preserved"
        );

        // Builtin schemes: series and table are now entry type suffixes.
        // series:///path parses to format=file + entry_type=series,
        // so full_scheme() returns "file+series" (the canonical form).
        let series_file = make_file("series", "/combined/site1.series");
        assert_eq!(
            series_file.source_url(),
            "file+series:///combined/site1.series",
            "series entry type must be preserved via full_scheme()"
        );

        let table_file = make_file("table", "/tables/lookup");
        assert_eq!(
            table_file.source_url(),
            "file+table:///tables/lookup",
            "table entry type must be preserved via full_scheme()"
        );

        let file_file = make_file("file", "/raw/data.parquet");
        assert_eq!(
            file_file.source_url(),
            "file:///raw/data.parquet",
            "file:// scheme must be preserved"
        );
    }

    /// Integration test: temporal-reduce with CSV format provider (non-Parquet source).
    ///
    /// This validates the fix for the bug where temporal-reduce hardcoded series://
    /// in ensure_inner() and used direct as_queryable() in discover_source_columns(),
    /// causing "Corrupt footer" errors when the source was a non-Parquet format like
    /// CSV, OTelJSON, or ExcelHTML.
    #[tokio::test]
    async fn test_temporal_reduce_with_csv_format_provider() {
        let _ = env_logger::try_init();

        let (fs, provider_context) = create_test_environment().await;

        // Create a raw CSV file (entry type = data, NOT table:series)
        // This is exactly the scenario that broke: raw data files with a format provider URL
        {
            let root = fs.root().await.unwrap();
            _ = root.create_dir_path("/ingest").await.unwrap();

            // Generate CSV data: timestamp, temperature, humidity
            // 48 rows: 2 days of hourly readings
            // Use RFC3339 timestamps so CSV parser infers Timestamp type (not Int64)
            // Use fractional values so CSV parser infers Float64 (not Int64)
            let mut csv_data = String::from("timestamp,temperature,humidity\n");
            for day in 0..2 {
                for hour in 0..24 {
                    let total_secs = day * 86400 + hour * 3600;
                    let h = total_secs / 3600;
                    let m = (total_secs % 3600) / 60;
                    let s = total_secs % 60;
                    // Format as ISO 8601: 1970-01-01T00:00:00 + offset
                    let d = day + 1; // 1-indexed day
                    let ts = format!("1970-01-{:02}T{:02}:{:02}:{:02}", d, h % 24, m, s);
                    let temp = 20.5 + hour as f64;
                    let humidity = 50.5 + hour as f64 * 0.5;
                    csv_data.push_str(&format!("{},{},{}\n", ts, temp, humidity));
                }
            }

            // Write as raw data file (FilePhysicalVersion, NOT TablePhysicalSeries)
            use tokio::io::AsyncWriteExt;
            let mut w = root
                .async_writer_path_with_type("/ingest/weather.csv", EntryType::FilePhysicalVersion)
                .await
                .unwrap();
            w.write_all(csv_data.as_bytes()).await.unwrap();
            w.flush().await.unwrap();
            w.shutdown().await.unwrap();
        }

        // Create temporal-reduce with csv:// in_pattern
        {
            let config = TemporalReduceConfig {
                in_pattern: crate::Url::parse("csv:///ingest/weather.csv").unwrap(),
                out_pattern: "weather".to_string(),
                time_column: "timestamp".to_string(),
                resolutions: vec!["1d".to_string()],
                aggregations: vec![
                    AggregationConfig {
                        agg_type: AggregationType::Avg,
                        columns: Some(vec!["temperature".to_string(), "humidity".to_string()]),
                    },
                    AggregationConfig {
                        agg_type: AggregationType::Min,
                        columns: Some(vec!["temperature".to_string()]),
                    },
                    AggregationConfig {
                        agg_type: AggregationType::Max,
                        columns: Some(vec!["temperature".to_string()]),
                    },
                ],
                transforms: None,
                allowed_lateness: None,
                // Seal on every advance, as before the size gate existed: these
                // tests assert seal MECHANICS on datasets far under any real target.
                seal_target_bytes: Some(0),
                max_live_segments: None,
            };

            let context = test_context(&provider_context, FileID::root());
            let temporal_dir = TemporalReduceDirectory::new(config, context).unwrap();
            let temporal_handle = temporal_dir.create_handle();

            // Verify directory structure: should have "weather" subdirectory
            use futures::StreamExt;
            let mut entries_stream = temporal_handle.entries().await.unwrap();
            let mut site_names = Vec::new();
            while let Some(entry_result) = entries_stream.next().await {
                let entry = entry_result.unwrap();
                site_names.push(entry.name.clone());
            }
            assert_eq!(
                site_names,
                vec!["weather"],
                "Should create 'weather' site directory from out_pattern"
            );

            // Navigate to the site directory
            let weather_node = temporal_handle.get("weather").await.unwrap().unwrap();
            let weather_dir = match &weather_node.node_type {
                NodeType::Directory(dir) => dir,
                _ => panic!("Expected directory for 'weather'"),
            };

            // Get the daily aggregation file
            let daily_node = weather_dir.get("res=1d.series").await.unwrap().unwrap();
            let file_id = daily_node.id;

            // Query the aggregated data -- this is where the bug manifested as "Corrupt footer"
            if let NodeType::File(file_handle) = &daily_node.node_type {
                let file_arc = file_handle.get_file().await;
                let file_guard = file_arc.lock().await;

                let queryable = file_guard.as_queryable().expect("Should be queryable");

                let table_provider = queryable
                    .as_table_provider(file_id, &provider_context)
                    .await
                    .expect("Table provider creation should succeed with csv:// source");

                // Query: verify results
                let ctx = &provider_context.datafusion_session;
                _ = ctx.register_table("csv_reduced", table_provider).unwrap();
                let df = ctx
                    .sql("SELECT * FROM csv_reduced ORDER BY timestamp")
                    .await
                    .unwrap();
                let batches = df.collect().await.unwrap();

                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(total_rows, 2, "Should have 2 aggregated rows (2 days)");

                // Debug: print schema and row count
                log::info!(
                    "CSV reduced: {} rows, schema: {:?}",
                    total_rows,
                    batches[0].schema()
                );

                // Verify schema
                let schema = &batches[0].schema();
                assert!(schema.column_with_name("timestamp").is_some());
                assert!(schema.column_with_name("temperature.avg").is_some());
                assert!(schema.column_with_name("humidity.avg").is_some());
                assert!(schema.column_with_name("temperature.min").is_some());
                assert!(schema.column_with_name("temperature.max").is_some());

                // Verify day 0 values: avg temp = 20.5 + avg(0..23) = 20.5 + 11.5 = 32.0
                let temp_avg = batches[0]
                    .column_by_name("temperature.avg")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                assert!(
                    (temp_avg.value(0) - 32.0).abs() < 0.01,
                    "Day 0 avg temperature: expected 32.0, got {}",
                    temp_avg.value(0)
                );

                let temp_min = batches[0]
                    .column_by_name("temperature.min")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                assert_eq!(temp_min.value(0), 20.5, "Day 0 min temperature");

                let temp_max = batches[0]
                    .column_by_name("temperature.max")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                assert_eq!(temp_max.value(0), 43.5, "Day 0 max temperature");

                log::info!("[OK] CSV format provider temporal-reduce test passed");
                log::info!("   - Raw CSV data read via csv:// scheme (not Parquet)");
                log::info!("   - 48 hourly rows aggregated to 2 daily rows");
                log::info!("   - avg/min/max values verified correct");
            } else {
                panic!("Expected file node for weather/res=1d.series");
            }
        }
    }

    /// Phase 2: the incremental rollup cache must produce the same aggregated
    /// values as the single-pass delegate, and a second read over an unchanged
    /// source must reuse the cached partials without recomputing any.
    #[tokio::test]
    async fn test_rollup_cache_matches_single_pass_and_is_incremental() {
        let _ = env_logger::try_init();

        let persistence = tinyfs::MemoryPersistence::default();
        let fs = tinyfs::FS::new(persistence.clone())
            .await
            .expect("create FS");

        // Two CSV source files matched by a glob, one day of hourly data each.
        {
            let root = fs.root().await.unwrap();
            _ = root.create_dir_path("/ingest").await.unwrap();

            for day in 0..2u32 {
                let mut csv_data = String::from("timestamp,temperature,humidity\n");
                for hour in 0..24u32 {
                    let d = day + 1;
                    let ts = format!("1970-01-{:02}T{:02}:00:00", d, hour);
                    let temp = 20.5 + hour as f64;
                    let humidity = 50.5 + hour as f64 * 0.5;
                    csv_data.push_str(&format!("{},{},{}\n", ts, temp, humidity));
                }

                use tokio::io::AsyncWriteExt;
                let path = format!("/ingest/weather-{}.csv", day + 1);
                let mut w = root
                    .async_writer_path_with_type(&path, EntryType::FilePhysicalVersion)
                    .await
                    .unwrap();
                w.write_all(csv_data.as_bytes()).await.unwrap();
                w.flush().await.unwrap();
                w.shutdown().await.unwrap();
            }
        }

        let config = || TemporalReduceConfig {
            in_pattern: crate::Url::parse("csv:///ingest/weather-*.csv").unwrap(),
            out_pattern: "weather".to_string(),
            time_column: "timestamp".to_string(),
            resolutions: vec!["1d".to_string()],
            aggregations: vec![
                agg(AggregationType::Avg, &["temperature", "humidity"]),
                agg(AggregationType::Min, &["temperature"]),
                agg(AggregationType::Max, &["temperature"]),
            ],
            transforms: None,
            allowed_lateness: None,
            // Seal on every advance, as before the size gate existed: these
            // tests assert seal MECHANICS on datasets far under any real target.
            seal_target_bytes: Some(0),
            max_live_segments: None,
        };

        let make_ctx = |cache: Option<std::path::PathBuf>| {
            let session = Arc::new(datafusion::prelude::SessionContext::new());
            let _ = crate::register_tinyfs_object_store(&session, persistence.clone())
                .expect("register object store");
            let ctx = crate::ProviderContext::new(session, Arc::new(persistence.clone()));
            match cache {
                Some(dir) => ctx.with_cache_dir(dir),
                None => ctx,
            }
        };

        // Collect the daily aggregation under a given provider context. Returns
        // the (avg_temp, avg_humidity, min_temp, max_temp) tuples per day.
        async fn collect_daily(
            fs: &tinyfs::FS,
            provider_context: &crate::ProviderContext,
            config: TemporalReduceConfig,
        ) -> Vec<(f64, f64, f64, f64)> {
            let _ = fs;
            let context = test_context(provider_context, FileID::root());
            let temporal_dir = TemporalReduceDirectory::new(config, context).unwrap();
            let temporal_handle = temporal_dir.create_handle();

            let weather_node = temporal_handle.get("weather").await.unwrap().unwrap();
            let weather_dir = match &weather_node.node_type {
                NodeType::Directory(dir) => dir,
                _ => panic!("expected directory"),
            };
            let daily_node = weather_dir.get("res=1d.series").await.unwrap().unwrap();
            let file_id = daily_node.id;

            let NodeType::File(file_handle) = &daily_node.node_type else {
                panic!("expected file node");
            };
            let file_arc = file_handle.get_file().await;
            let file_guard = file_arc.lock().await;
            let queryable = file_guard.as_queryable().expect("queryable");
            let table_provider = queryable
                .as_table_provider(file_id, provider_context)
                .await
                .expect("table provider");

            let ctx = &provider_context.datafusion_session;
            _ = ctx.register_table("reduced", table_provider).unwrap();
            let batches = ctx
                .sql("SELECT \"temperature.avg\", \"humidity.avg\", \"temperature.min\", \"temperature.max\" FROM reduced ORDER BY timestamp")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();

            let mut rows = Vec::new();
            for b in &batches {
                let col = |name: &str| {
                    b.column_by_name(name)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .clone()
                };
                let ta = col("temperature.avg");
                let ha = col("humidity.avg");
                let tmin = col("temperature.min");
                let tmax = col("temperature.max");
                for i in 0..b.num_rows() {
                    rows.push((ta.value(i), ha.value(i), tmin.value(i), tmax.value(i)));
                }
            }
            rows
        }

        /// Every cache file with its content digest, under `filter`ed dirs.
        ///
        /// Incrementality is asserted on this rather than on a file COUNT: a
        /// count cannot tell reuse apart from recompute-and-overwrite, which is
        /// exactly the failure worth catching now that the cache is a fixed
        /// handful of files instead of one per source version.
        fn cache_fingerprint(
            cache_dir: &std::path::Path,
            keep: &dyn Fn(&std::path::Path) -> bool,
        ) -> Vec<(String, String)> {
            let mut out = Vec::new();
            let mut stack = vec![cache_dir.to_path_buf()];
            while let Some(dir) = stack.pop() {
                let Ok(entries) = std::fs::read_dir(&dir) else {
                    continue;
                };
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        stack.push(path);
                    } else if keep(&path) {
                        let bytes = std::fs::read(&path).unwrap();
                        out.push((
                            path.strip_prefix(cache_dir).unwrap().display().to_string(),
                            blake3::hash(&bytes).to_hex().to_string(),
                        ));
                    }
                }
            }
            out.sort();
            out
        }

        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().to_path_buf();

        // Rollup path (cache enabled).
        let cache_ctx = make_ctx(Some(cache_dir.clone()));
        let rollup_rows = collect_daily(&fs, &cache_ctx, config()).await;

        // Single-pass path (no cache) for the same config.
        let nocache_ctx = make_ctx(None);
        let single_rows = collect_daily(&fs, &nocache_ctx, config()).await;

        assert_eq!(rollup_rows.len(), 2, "two daily rows");
        assert_eq!(
            rollup_rows, single_rows,
            "rollup output must equal single-pass output"
        );

        // Known expected values for day 0 (hours 0..23).
        let (ta0, _ha0, tmin0, tmax0) = rollup_rows[0];
        assert!((ta0 - 32.0).abs() < 1e-9, "day0 avg temp");
        assert_eq!(tmin0, 20.5, "day0 min temp");
        assert_eq!(tmax0, 43.5, "day0 max temp");

        // The cache names the content of both source files, and holds only the
        // merged levels -- no per-source-version file population.
        let all = |_: &std::path::Path| true;
        let after_first = cache_fingerprint(&cache_dir, &all);
        let manifests = walk_find(&cache_dir, &|n| n == "manifest.json");
        assert_eq!(
            manifests.len(),
            1,
            "one manifest, for the single resolution"
        );
        let m = crate::rollup_cache::read_segment_manifest(manifests[0].parent().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(m.sources.len(), 2, "one source entry per source file");

        // A second read over an unchanged source, with a fresh session but the
        // same cache directory, must REUSE: identical output, and not one cache
        // byte rewritten.
        let cache_ctx2 = make_ctx(Some(cache_dir.clone()));
        let rollup_rows2 = collect_daily(&fs, &cache_ctx2, config()).await;
        assert_eq!(rollup_rows2, rollup_rows, "incremental read matches");
        assert_eq!(
            cache_fingerprint(&cache_dir, &all),
            after_first,
            "second read over unchanged sources must not rewrite the cache"
        );
    }

    /// Phase 3: cascading rollups. Multiple nesting resolutions of one site
    /// share a single set of finest-resolution partials; each resolution
    /// re-bins those partials and matches its single-pass output, and raw is
    /// scanned only once regardless of how many resolutions are read.
    #[tokio::test]
    async fn test_rollup_cascading_resolutions_share_finest_partials() {
        let _ = env_logger::try_init();

        let persistence = tinyfs::MemoryPersistence::default();
        let fs = tinyfs::FS::new(persistence.clone())
            .await
            .expect("create FS");

        {
            let root = fs.root().await.unwrap();
            _ = root.create_dir_path("/ingest").await.unwrap();
            for day in 0..2u32 {
                let mut csv_data = String::from("timestamp,temperature\n");
                for hour in 0..24u32 {
                    let d = day + 1;
                    let ts = format!("1970-01-{:02}T{:02}:00:00", d, hour);
                    let temp = 20.5 + hour as f64 + day as f64 * 100.0;
                    csv_data.push_str(&format!("{},{}\n", ts, temp));
                }
                use tokio::io::AsyncWriteExt;
                let path = format!("/ingest/weather-{}.csv", day + 1);
                let mut w = root
                    .async_writer_path_with_type(&path, EntryType::FilePhysicalVersion)
                    .await
                    .unwrap();
                w.write_all(csv_data.as_bytes()).await.unwrap();
                w.flush().await.unwrap();
                w.shutdown().await.unwrap();
            }
        }

        let config = || TemporalReduceConfig {
            in_pattern: crate::Url::parse("csv:///ingest/weather-*.csv").unwrap(),
            out_pattern: "weather".to_string(),
            time_column: "timestamp".to_string(),
            resolutions: vec!["1h".to_string(), "6h".to_string(), "1d".to_string()],
            aggregations: vec![
                agg(AggregationType::Avg, &["temperature"]),
                agg(AggregationType::Min, &["temperature"]),
                agg(AggregationType::Max, &["temperature"]),
            ],
            transforms: None,
            allowed_lateness: None,
            // Seal on every advance, as before the size gate existed: these
            // tests assert seal MECHANICS on datasets far under any real target.
            seal_target_bytes: Some(0),
            max_live_segments: None,
        };

        let make_ctx = |cache: Option<std::path::PathBuf>| {
            let session = Arc::new(datafusion::prelude::SessionContext::new());
            let _ = crate::register_tinyfs_object_store(&session, persistence.clone())
                .expect("register object store");
            let ctx = crate::ProviderContext::new(session, Arc::new(persistence.clone()));
            match cache {
                Some(dir) => ctx.with_cache_dir(dir),
                None => ctx,
            }
        };

        async fn collect_resolution(
            provider_context: &crate::ProviderContext,
            config: TemporalReduceConfig,
            res_file: &str,
            table: &str,
        ) -> Vec<(f64, f64, f64)> {
            let context = test_context(provider_context, FileID::root());
            let temporal_dir = TemporalReduceDirectory::new(config, context).unwrap();
            let temporal_handle = temporal_dir.create_handle();

            let weather_node = temporal_handle.get("weather").await.unwrap().unwrap();
            let weather_dir = match &weather_node.node_type {
                NodeType::Directory(dir) => dir,
                _ => panic!("expected directory"),
            };
            let node = weather_dir.get(res_file).await.unwrap().unwrap();
            let file_id = node.id;
            let NodeType::File(file_handle) = &node.node_type else {
                panic!("expected file node");
            };
            let file_arc = file_handle.get_file().await;
            let file_guard = file_arc.lock().await;
            let queryable = file_guard.as_queryable().expect("queryable");
            let table_provider = queryable
                .as_table_provider(file_id, provider_context)
                .await
                .expect("table provider");

            let ctx = &provider_context.datafusion_session;
            _ = ctx.register_table(table, table_provider).unwrap();
            let batches = ctx
                .sql(&format!(
                    "SELECT \"temperature.avg\", \"temperature.min\", \"temperature.max\" FROM {} ORDER BY timestamp",
                    table
                ))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();

            let mut rows = Vec::new();
            for b in &batches {
                let col = |name: &str| {
                    b.column_by_name(name)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .clone()
                };
                let avg = col("temperature.avg");
                let min = col("temperature.min");
                let max = col("temperature.max");
                for i in 0..b.num_rows() {
                    rows.push((avg.value(i), min.value(i), max.value(i)));
                }
            }
            rows
        }

        /// Every cache file with its content digest, under `filter`ed dirs.
        ///
        /// Incrementality is asserted on this rather than on a file COUNT: a
        /// count cannot tell reuse apart from recompute-and-overwrite, which is
        /// exactly the failure worth catching now that the cache is a fixed
        /// handful of files instead of one per source version.
        fn cache_fingerprint(
            cache_dir: &std::path::Path,
            keep: &dyn Fn(&std::path::Path) -> bool,
        ) -> Vec<(String, String)> {
            let mut out = Vec::new();
            let mut stack = vec![cache_dir.to_path_buf()];
            while let Some(dir) = stack.pop() {
                let Ok(entries) = std::fs::read_dir(&dir) else {
                    continue;
                };
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        stack.push(path);
                    } else if keep(&path) {
                        let bytes = std::fs::read(&path).unwrap();
                        out.push((
                            path.strip_prefix(cache_dir).unwrap().display().to_string(),
                            blake3::hash(&bytes).to_hex().to_string(),
                        ));
                    }
                }
            }
            out.sort();
            out
        }

        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().to_path_buf();
        let cache_ctx = make_ctx(Some(cache_dir.clone()));

        // Read all three resolutions from the cascading rollup path.
        let hourly = collect_resolution(&cache_ctx, config(), "res=1h.series", "r_1h").await;
        // The finest level (res3600) is the only one that touches raw source.
        let finest = |p: &std::path::Path| p.to_string_lossy().contains("res3600");
        let finest_after_1h = cache_fingerprint(&cache_dir, &finest);
        assert!(
            !finest_after_1h.is_empty(),
            "the finest resolution must be materialized"
        );
        let six_hourly = collect_resolution(&cache_ctx, config(), "res=6h.series", "r_6h").await;
        let daily = collect_resolution(&cache_ctx, config(), "res=1d.series", "r_1d").await;

        // Raw is scanned once. The coarser resolutions fold the finest level's
        // output, so they must leave it byte-identical -- if a coarser read
        // rewrote it, the cascade would be rescanning source per resolution.
        assert_eq!(
            cache_fingerprint(&cache_dir, &finest),
            finest_after_1h,
            "coarser resolutions must reuse the finest level untouched"
        );

        // Each cascaded resolution matches its single-pass output.
        let single_1h =
            collect_resolution(&make_ctx(None), config(), "res=1h.series", "s_1h").await;
        let single_6h =
            collect_resolution(&make_ctx(None), config(), "res=6h.series", "s_6h").await;
        let single_1d =
            collect_resolution(&make_ctx(None), config(), "res=1d.series", "s_1d").await;

        assert_eq!(hourly.len(), 48, "48 hourly buckets");
        assert_eq!(six_hourly.len(), 8, "8 six-hourly buckets");
        assert_eq!(daily.len(), 2, "2 daily buckets");
        assert_eq!(hourly, single_1h, "1h cascade == single-pass");
        assert_eq!(six_hourly, single_6h, "6h cascade == single-pass");
        assert_eq!(daily, single_1d, "1d cascade == single-pass");
    }

    /// Phase 3 step 2: requesting only the coarsest resolution transparently
    /// materializes the finer levels it folds from. The finest level folds the
    /// shared partials; each coarser level folds the next-finer level's sealed
    /// segments + hot (not the raw partials), so every intermediate `res{secs}` dir
    /// must exist on disk with its own manifest + hot file, and the coarse
    /// manifest must record the finer digest it folded (`source_digest`).
    #[tokio::test]
    async fn test_rollup_cascade_materializes_finer_levels_on_disk() {
        let _ = env_logger::try_init();

        let persistence = tinyfs::MemoryPersistence::default();
        let fs = tinyfs::FS::new(persistence.clone())
            .await
            .expect("create FS");
        {
            let root = fs.root().await.unwrap();
            _ = root.create_dir_path("/ingest").await.unwrap();
        }
        // Three days of hourly data so the coarse watermark advances and seals.
        for day in 1..=3u32 {
            append_day_series(&fs, "/ingest/weather.csv", day).await;
        }

        let config = TemporalReduceConfig {
            in_pattern: crate::Url::parse("csv:///ingest/weather.csv").unwrap(),
            out_pattern: "weather".to_string(),
            time_column: "timestamp".to_string(),
            resolutions: vec!["1h".to_string(), "6h".to_string(), "1d".to_string()],
            aggregations: vec![agg(AggregationType::Max, &["temperature"])],
            transforms: None,
            allowed_lateness: Some("1d".to_string()),
            // Seal on every advance, as before the size gate existed: these
            // tests assert seal MECHANICS on datasets far under any real target.
            seal_target_bytes: Some(0),
            max_live_segments: None,
        };

        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().to_path_buf();
        let ctx = reduce_ctx(&persistence, &cache_dir);

        // Read ONLY the coarsest resolution. This must build 1h -> 6h -> 1d.
        let daily = daily_max(&ctx, config).await.unwrap();
        assert_eq!(daily.len(), 3, "3 daily buckets");

        // Every level's res dir must exist with a manifest + hot file.
        let manifests = walk_find(&cache_dir, &|n| n == "manifest.json");
        let mut res_secs: Vec<u64> = manifests
            .iter()
            .filter_map(|p| {
                p.parent()
                    .and_then(|d| d.file_name())
                    .and_then(|n| n.to_str())
                    .and_then(|n| n.strip_prefix("res"))
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .collect();
        res_secs.sort_unstable();
        assert_eq!(
            res_secs,
            vec![3600, 21600, 86400],
            "cascade must materialize the 1h, 6h and 1d levels on disk"
        );
        let hots = walk_find(&cache_dir, &|n| n == "hot.parquet");
        assert_eq!(hots.len(), 3, "each level recomputes its own hot window");

        // The finest level aggregates the sources (source_digest == None, and
        // `sources` names their content); each coarser level records the finer
        // manifest digest it folded instead.
        for m_path in &manifests {
            let res_dir = m_path.parent().unwrap();
            let secs: u64 = res_dir
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_prefix("res"))
                .and_then(|s| s.parse().ok())
                .unwrap();
            let m = crate::rollup_cache::read_segment_manifest(res_dir)
                .unwrap()
                .unwrap();
            if secs == 3600 {
                assert!(
                    m.source_digest.is_none(),
                    "finest level aggregates sources, not a finer level"
                );
                assert!(
                    !m.sources.is_empty(),
                    "finest level tracks the content of its live source versions"
                );
            } else {
                assert!(
                    m.source_digest.is_some(),
                    "coarse level {secs}s must record the finer digest it folded"
                );
            }
        }
    }

    fn range(min_us: i64, max_us: i64) -> crate::rollup_cache::SourceRange {
        crate::rollup_cache::SourceRange { min_us, max_us }
    }

    fn source_map(
        entries: &[(&str, crate::rollup_cache::SourceRange)],
    ) -> std::collections::BTreeMap<String, crate::rollup_cache::SourceRange> {
        entries
            .iter()
            .map(|(k, r)| ((*k).to_string(), *r))
            .collect()
    }

    /// The dirty point is driven by the SYMMETRIC difference of the source sets.
    /// A version that DISAPPEARED -- collapsed away or deleted -- invalidates the
    /// buckets it fed just as surely as a new one does, so keying only on
    /// additions would serve output still containing its rows.
    #[test]
    fn test_dirty_lo_us_covers_both_directions() {
        let a = range(1_000, 2_000);
        let b = range(5_000, 6_000);
        let c = range(9_000, 9_500);

        let before = source_map(&[("a", a), ("b", b)]);

        assert_eq!(
            dirty_lo_us(&before, &before),
            None,
            "unchanged: no dirty range"
        );

        // Pure append: only the new version's span is dirty.
        let appended = source_map(&[("a", a), ("b", b), ("c", c)]);
        assert_eq!(dirty_lo_us(&before, &appended), Some(9_000));

        // Removal: the vanished version's span is dirty even though nothing
        // was added.
        let removed = source_map(&[("a", a)]);
        assert_eq!(dirty_lo_us(&before, &removed), Some(5_000));

        // A collapse -- several versions replaced by one merged version with the
        // same content -- is dirty over the whole span it replaces, and no
        // further back.
        let collapsed = source_map(&[("merged", range(1_000, 6_000))]);
        assert_eq!(dirty_lo_us(&before, &collapsed), Some(1_000));

        // A version with no recorded range makes the dirty range unbounded, so
        // the build reads everything rather than pruning something it cannot
        // prove is unaffected.
        let unknown = source_map(&[
            ("a", a),
            ("b", b),
            ("c", crate::rollup_cache::SourceRange::UNKNOWN),
        ]);
        assert_eq!(dirty_lo_us(&before, &unknown), None);
    }

    /// Late data unseals: segments covering the dirty point are dropped and the
    /// watermark falls back to the bottom of the oldest one dropped, so the
    /// ordinary seal path recomputes those buckets from source.
    #[test]
    fn test_unseal_from_drops_segments_covering_the_dirty_point() {
        let seg = |name: &str, lo: Option<i64>, hi: i64| crate::rollup_cache::Segment {
            name: name.to_string(),
            lo_secs: lo,
            hi_secs: hi,
            digest: "d".to_string(),
            bytes: 100,
        };
        let base = crate::rollup_cache::SegmentManifest {
            format: crate::rollup_cache::SEALED_FORMAT.to_string(),
            sealed_hi_secs: Some(300),
            segments: vec![
                seg("seg-0", None, 100),
                seg("seg-1", Some(100), 200),
                seg("seg-2", Some(200), 300),
            ],
            hot_bytes: 7,
            ..Default::default()
        };
        let dir = std::path::Path::new("/tmp/unused");

        // Dirty above the watermark: nothing sealed is affected.
        let mut m = base.clone();
        assert!(unseal_from(&mut m, Some(400), dir).unwrap().is_empty());
        assert_eq!(m.segments.len(), 3);
        assert_eq!(m.sealed_hi_secs, Some(300));

        // Dirty inside the middle segment: it and everything after it are
        // dropped, and the watermark falls to that segment's bottom -- not to
        // the dirty point, because a segment is dropped whole.
        let mut m = base.clone();
        let dropped = unseal_from(&mut m, Some(150), dir).unwrap();
        assert_eq!(dropped.len(), 2, "seg-1 and seg-2");
        assert_eq!(m.segments.len(), 1);
        assert_eq!(m.sealed_hi_secs, Some(100));
        assert_eq!(m.hot_bytes, 207, "unsealed bytes are hot again");

        // An unbounded dirty range unseals everything.
        let mut m = base.clone();
        let dropped = unseal_from(&mut m, None, dir).unwrap();
        assert_eq!(dropped.len(), 3);
        assert!(m.segments.is_empty());
        assert_eq!(m.sealed_hi_secs, None);

        // A watermark advanced over an empty span records no segment file. The
        // watermark must still fall back to the dirty point, or the buckets
        // between them would be neither sealed nor recomputed -- silently lost.
        let mut m = crate::rollup_cache::SegmentManifest {
            sealed_hi_secs: Some(500),
            segments: vec![seg("seg-0", None, 100)],
            ..Default::default()
        };
        let dropped = unseal_from(&mut m, Some(300), dir).unwrap();
        assert!(dropped.is_empty(), "no segment covers [300, 500)");
        assert_eq!(m.sealed_hi_secs, Some(300));

        // The watermark this produces is exactly the dirty point, so the dirty
        // point MUST already be bucket-aligned -- see `floor_secs_to_bucket` and
        // the caller. An unaligned one would leave the bucket containing it in
        // neither a segment (they stop at the aligned edge below) nor the hot
        // window (it starts strictly at or above the watermark, because the read
        // filters on a bucket START), dropping that bucket and the very rows
        // that made it dirty.
        let interval = Duration::from_secs(100);
        let mut m = crate::rollup_cache::SegmentManifest {
            sealed_hi_secs: Some(500),
            segments: vec![seg("seg-0", None, 100)],
            ..Default::default()
        };
        let unaligned = 350;
        let _ = unseal_from(&mut m, Some(floor_secs_to_bucket(unaligned, interval)), dir).unwrap();
        assert_eq!(
            m.sealed_hi_secs,
            Some(300),
            "the watermark must fall to the START of the bucket holding the \
             dirty point, not to the dirty point itself"
        );
    }

    /// Watermarks are bucket boundaries; flooring must round toward -inf so it
    /// holds for pre-epoch times too.
    #[test]
    fn test_floor_secs_to_bucket_rounds_down() {
        let hour = Duration::from_secs(3600);
        assert_eq!(floor_secs_to_bucket(3600, hour), 3600, "already aligned");
        assert_eq!(floor_secs_to_bucket(3601, hour), 3600);
        assert_eq!(floor_secs_to_bucket(7199, hour), 3600);
        // Pre-epoch: -1s is in the bucket starting at -3600, not at 0.
        assert_eq!(floor_secs_to_bucket(-1, hour), -3600);
        assert_eq!(floor_secs_to_bucket(-3600, hour), -3600);
        // A zero interval cannot divide; pass the value through rather than panic.
        assert_eq!(floor_secs_to_bucket(42, Duration::from_secs(0)), 42);
    }

    /// Phase 3: non-nesting resolution sets are rejected so cascaded rollups can
    /// never silently miscompute a straddling bucket.
    #[test]
    fn test_non_nesting_resolutions_rejected() {
        // 90m is not an integer multiple of 60m, and 60m does not divide 90m.
        let err = parse_nesting_resolutions(&["1h".to_string(), "90m".to_string()]);
        assert!(err.is_err(), "non-nesting resolutions must be rejected");

        // Nesting sets are accepted and returned finest-first.
        let ok = parse_nesting_resolutions(&["1d".to_string(), "1h".to_string(), "6h".to_string()])
            .unwrap();
        assert_eq!(ok.first().copied(), Some(Duration::from_secs(3600)));
    }

    /// A non-series source with more than one live version is a hard error
    /// rather than a silent double-count: each version is a full re-snapshot, so
    /// summing them counts the overlapping rows twice. A single CSV is written
    /// twice; the second version is a full rewrite that also prepends a day.
    ///
    /// This used to be phrased as a *sequentiality* check -- scan each version's
    /// bucket span and fail if it reached below a persisted frontier -- which
    /// tested the data for a property fixed by the entry type, and needed a
    /// sidecar file to remember. The structural test reports the same case, and
    /// reports it before reading a single row.
    #[tokio::test]
    async fn test_rollup_rejects_multi_version_non_series_source() {
        let _ = env_logger::try_init();

        let persistence = tinyfs::MemoryPersistence::default();
        let fs = tinyfs::FS::new(persistence.clone())
            .await
            .expect("create FS");

        async fn write_csv(root: &tinyfs::WD, path: &str, days: std::ops::Range<u32>) {
            use tokio::io::AsyncWriteExt;
            let mut csv_data = String::from("timestamp,temperature\n");
            for day in days {
                for hour in 0..24u32 {
                    let ts = format!("1970-01-{:02}T{:02}:00:00", day, hour);
                    let temp = 20.5 + hour as f64;
                    csv_data.push_str(&format!("{},{}\n", ts, temp));
                }
            }
            let mut w = root
                .async_writer_path_with_type(path, EntryType::FilePhysicalVersion)
                .await
                .unwrap();
            w.write_all(csv_data.as_bytes()).await.unwrap();
            w.flush().await.unwrap();
            w.shutdown().await.unwrap();
        }

        {
            let root = fs.root().await.unwrap();
            _ = root.create_dir_path("/ingest").await.unwrap();
            // Version 1: day 2 only.
            write_csv(&root, "/ingest/weather.csv", 2..3).await;
            // Version 2: full rewrite that prepends day 1, backfilling a sealed
            // bucket below the version-1 frontier.
            write_csv(&root, "/ingest/weather.csv", 1..3).await;
        }

        let config = TemporalReduceConfig {
            in_pattern: crate::Url::parse("csv:///ingest/weather.csv").unwrap(),
            out_pattern: "weather".to_string(),
            time_column: "timestamp".to_string(),
            resolutions: vec!["1d".to_string()],
            aggregations: vec![agg(AggregationType::Avg, &["temperature"])],
            transforms: None,
            allowed_lateness: None,
            // Seal on every advance, as before the size gate existed: these
            // tests assert seal MECHANICS on datasets far under any real target.
            seal_target_bytes: Some(0),
            max_live_segments: None,
        };

        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().to_path_buf();

        let session = Arc::new(datafusion::prelude::SessionContext::new());
        let _ = crate::register_tinyfs_object_store(&session, persistence.clone())
            .expect("register object store");
        let provider_context = crate::ProviderContext::new(session, Arc::new(persistence.clone()))
            .with_cache_dir(cache_dir.clone());

        let context = test_context(&provider_context, FileID::root());
        let temporal_dir = TemporalReduceDirectory::new(config, context).unwrap();
        let temporal_handle = temporal_dir.create_handle();

        let weather_node = temporal_handle.get("weather").await.unwrap().unwrap();
        let NodeType::Directory(weather_dir) = &weather_node.node_type else {
            panic!("expected directory");
        };
        let daily_node = weather_dir.get("res=1d.series").await.unwrap().unwrap();
        let file_id = daily_node.id;
        let NodeType::File(file_handle) = &daily_node.node_type else {
            panic!("expected file node");
        };
        let file_arc = file_handle.get_file().await;
        let file_guard = file_arc.lock().await;
        let queryable = file_guard.as_queryable().expect("queryable");

        let result = queryable
            .as_table_provider(file_id, &provider_context)
            .await;

        let err = result.expect_err("overlapping re-snapshots must be a hard error");
        let msg = err.to_string();
        assert!(
            msg.contains("non-series") && msg.contains("double-counting"),
            "error must name the overlap and why it is refused, got: {}",
            msg
        );
    }

    /// A FilePhysicalSeries source appends disjoint deltas, so a later version
    /// carrying EARLIER timestamps (late / out-of-order data, e.g. a collector
    /// flushing a buffered sample after a newer one) must merge correctly rather
    /// than being refused. The overlap guard applies only to non-series sources;
    /// series versions are disjoint deltas and the GROUP BY time_bucket merge is
    /// order-independent.
    #[tokio::test]
    async fn test_rollup_series_tolerates_late_data() {
        let _ = env_logger::try_init();

        let persistence = tinyfs::MemoryPersistence::default();
        let fs = tinyfs::FS::new(persistence.clone())
            .await
            .expect("create FS");

        // Each appended version is a self-contained CSV (header + that day's
        // hourly rows); the format cache parses every series version
        // independently, so the deltas stay disjoint by timestamp.
        async fn append_day(root: &tinyfs::WD, path: &str, day: u32) {
            use tokio::io::AsyncWriteExt;
            let mut csv_data = String::from("timestamp,temperature\n");
            for hour in 0..24u32 {
                let ts = format!("1970-01-{:02}T{:02}:00:00", day, hour);
                // Encode the day into the value so per-day aggregates differ.
                let temp = 20.5 + hour as f64 + day as f64 * 100.0;
                csv_data.push_str(&format!("{},{}\n", ts, temp));
            }
            let mut w = root
                .async_writer_path_with_type(path, EntryType::FilePhysicalSeries)
                .await
                .unwrap();
            w.write_all(csv_data.as_bytes()).await.unwrap();
            w.flush().await.unwrap();
            w.shutdown().await.unwrap();
        }

        {
            let root = fs.root().await.unwrap();
            _ = root.create_dir_path("/ingest").await.unwrap();
            // Version 1: day 2. Version 2: day 1 -- earlier timestamps arriving
            // after a newer bucket was already sealed in version 1.
            append_day(&root, "/ingest/weather.csv", 2).await;
            append_day(&root, "/ingest/weather.csv", 1).await;
        }

        let config = TemporalReduceConfig {
            in_pattern: crate::Url::parse("csv:///ingest/weather.csv").unwrap(),
            out_pattern: "weather".to_string(),
            time_column: "timestamp".to_string(),
            resolutions: vec!["1d".to_string()],
            aggregations: vec![
                agg(AggregationType::Avg, &["temperature"]),
                agg(AggregationType::Min, &["temperature"]),
                agg(AggregationType::Max, &["temperature"]),
            ],
            transforms: None,
            allowed_lateness: None,
            // Seal on every advance, as before the size gate existed: these
            // tests assert seal MECHANICS on datasets far under any real target.
            seal_target_bytes: Some(0),
            max_live_segments: None,
        };

        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().to_path_buf();

        let session = Arc::new(datafusion::prelude::SessionContext::new());
        let _ = crate::register_tinyfs_object_store(&session, persistence.clone())
            .expect("register object store");
        let provider_context = crate::ProviderContext::new(session, Arc::new(persistence.clone()))
            .with_cache_dir(cache_dir.clone());

        let context = test_context(&provider_context, FileID::root());
        let temporal_dir = TemporalReduceDirectory::new(config, context).unwrap();
        let temporal_handle = temporal_dir.create_handle();

        let weather_node = temporal_handle.get("weather").await.unwrap().unwrap();
        let NodeType::Directory(weather_dir) = &weather_node.node_type else {
            panic!("expected directory");
        };
        let daily_node = weather_dir.get("res=1d.series").await.unwrap().unwrap();
        let file_id = daily_node.id;
        let NodeType::File(file_handle) = &daily_node.node_type else {
            panic!("expected file node");
        };
        let file_arc = file_handle.get_file().await;
        let file_guard = file_arc.lock().await;
        let queryable = file_guard.as_queryable().expect("queryable");
        let table_provider = queryable
            .as_table_provider(file_id, &provider_context)
            .await
            .expect("late series data must merge, not error");
        drop(file_guard);

        let ctx = &provider_context.datafusion_session;
        _ = ctx.register_table("reduced", table_provider).unwrap();
        let batches = ctx
            .sql(
                "SELECT \"temperature.avg\", \"temperature.min\", \"temperature.max\" \
                 FROM reduced ORDER BY timestamp",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let mut rows = Vec::new();
        for b in &batches {
            let col = |name: &str| {
                b.column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .clone()
            };
            let avg = col("temperature.avg");
            let min = col("temperature.min");
            let max = col("temperature.max");
            for i in 0..b.num_rows() {
                rows.push((avg.value(i), min.value(i), max.value(i)));
            }
        }

        // hours 0..23 -> mean hour 11.5, so avg = 120.5 + day*100 here is
        // 20.5 + 11.5 + day*100. Day 1 sorts first despite arriving last.
        assert_eq!(rows.len(), 2, "expected two daily buckets, got {:?}", rows);
        let approx = |a: f64, b: f64| (a - b).abs() < 1e-9;
        assert!(
            approx(rows[0].0, 132.0) && approx(rows[0].1, 120.5) && approx(rows[0].2, 143.5),
            "day 1 (late-arriving) aggregates wrong: {:?}",
            rows[0]
        );
        assert!(
            approx(rows[1].0, 232.0) && approx(rows[1].1, 220.5) && approx(rows[1].2, 243.5),
            "day 2 aggregates wrong: {:?}",
            rows[1]
        );
    }

    /// Merged-output cache across the three shapes of a rebuild: seed, APPEND a
    /// newer version, and BACKFILL one older than everything already built.
    ///
    /// The backfill is the point. It was a hard error suggesting `--rebuild`
    /// until segments were keyed by the bucket range they cover; now the
    /// segments holding the backfilled buckets are unsealed and recomputed, and
    /// the output is simply correct.
    #[tokio::test]
    async fn test_merged_cache_incremental_splice_append_and_backfill() {
        let _ = env_logger::try_init();

        let persistence = tinyfs::MemoryPersistence::default();
        let fs = tinyfs::FS::new(persistence.clone())
            .await
            .expect("create FS");

        async fn append_day(root: &tinyfs::WD, path: &str, day: u32) {
            use tokio::io::AsyncWriteExt;
            let mut csv = String::from("timestamp,temperature\n");
            for hour in 0..24u32 {
                let ts = format!("1970-01-{:02}T{:02}:00:00", day, hour);
                let temp = 20.5 + hour as f64 + day as f64 * 100.0;
                csv.push_str(&format!("{},{}\n", ts, temp));
            }
            let mut w = root
                .async_writer_path_with_type(path, EntryType::FilePhysicalSeries)
                .await
                .unwrap();
            w.write_all(csv.as_bytes()).await.unwrap();
            w.flush().await.unwrap();
            w.shutdown().await.unwrap();
        }

        {
            let root = fs.root().await.unwrap();
            _ = root.create_dir_path("/ingest").await.unwrap();
            // Version 1: day 2.
            append_day(&root, "/ingest/weather.csv", 2).await;
        }

        let config = || TemporalReduceConfig {
            in_pattern: crate::Url::parse("csv:///ingest/weather.csv").unwrap(),
            out_pattern: "weather".to_string(),
            time_column: "timestamp".to_string(),
            resolutions: vec!["1d".to_string()],
            aggregations: vec![
                agg(AggregationType::Avg, &["temperature"]),
                agg(AggregationType::Min, &["temperature"]),
                agg(AggregationType::Max, &["temperature"]),
            ],
            transforms: None,
            allowed_lateness: None,
            // Seal on every advance, as before the size gate existed: these
            // tests assert seal MECHANICS on datasets far under any real target.
            seal_target_bytes: Some(0),
            max_live_segments: None,
        };

        let make_ctx = |cache: Option<std::path::PathBuf>| {
            let session = Arc::new(datafusion::prelude::SessionContext::new());
            let _ = crate::register_tinyfs_object_store(&session, persistence.clone())
                .expect("register object store");
            let ctx = crate::ProviderContext::new(session, Arc::new(persistence.clone()));
            match cache {
                Some(dir) => ctx.with_cache_dir(dir),
                None => ctx,
            }
        };

        async fn collect_daily(
            provider_context: &crate::ProviderContext,
            config: TemporalReduceConfig,
        ) -> (Vec<(f64, f64, f64)>, Option<tinyfs::ExportHint>) {
            let context = test_context(provider_context, FileID::root());
            let temporal_dir = TemporalReduceDirectory::new(config, context).unwrap();
            let temporal_handle = temporal_dir.create_handle();
            let weather_node = temporal_handle.get("weather").await.unwrap().unwrap();
            let NodeType::Directory(weather_dir) = &weather_node.node_type else {
                panic!("expected directory");
            };
            let daily_node = weather_dir.get("res=1d.series").await.unwrap().unwrap();
            let file_id = daily_node.id;
            let NodeType::File(file_handle) = &daily_node.node_type else {
                panic!("expected file node");
            };
            let file_arc = file_handle.get_file().await;
            let file_guard = file_arc.lock().await;
            let queryable = file_guard.as_queryable().expect("queryable");
            let table_provider = queryable
                .as_table_provider(file_id, provider_context)
                .await
                .expect("table provider");
            drop(file_guard);
            let hint = provider_context.get_export_hint(&file_id);
            let ctx = &provider_context.datafusion_session;
            _ = ctx.register_table("reduced", table_provider).unwrap();
            let batches = ctx
                .sql(
                    "SELECT \"temperature.avg\", \"temperature.min\", \"temperature.max\" \
                     FROM reduced ORDER BY timestamp",
                )
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let mut rows = Vec::new();
            for b in &batches {
                let col = |name: &str| {
                    b.column_by_name(name)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .clone()
                };
                let avg = col("temperature.avg");
                let min = col("temperature.min");
                let max = col("temperature.max");
                for i in 0..b.num_rows() {
                    rows.push((avg.value(i), min.value(i), max.value(i)));
                }
            }
            (rows, hint)
        }

        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().to_path_buf();
        let approx = |a: f64, b: f64| (a - b).abs() < 1e-9;

        // Build 1: only day 2 -> seeds the merged cache.
        let (rows1, hint1) = collect_daily(&make_ctx(Some(cache_dir.clone())), config()).await;
        assert_eq!(rows1.len(), 1, "one daily bucket after build 1");
        assert!(approx(rows1[0].0, 232.0), "day2 avg wrong: {:?}", rows1);
        // First build is a cache miss: the hint carries a digest but no
        // changed_since watermark, so the export does a full deterministic write.
        let hint1 = hint1.expect("build 1 publishes an export hint");
        assert!(hint1.changed_since.is_none(), "build 1 is a full rebuild");

        // Build 2 (APPEND): add day 3 -> splice cached day2 prefix + fresh day3.
        {
            let root = fs.root().await.unwrap();
            append_day(&root, "/ingest/weather.csv", 3).await;
        }
        let (rows2, hint2) = collect_daily(&make_ctx(Some(cache_dir.clone())), config()).await;
        assert_eq!(
            rows2.len(),
            2,
            "append splice: two buckets, got {:?}",
            rows2
        );
        assert!(
            approx(rows2[0].0, 232.0) && approx(rows2[1].0, 332.0),
            "append splice values wrong: {:?}",
            rows2
        );
        // Append splice changes buckets from day 3 onward, so the hint reports a
        // changed_since watermark at day 3 (epoch seconds) and a new digest.
        let hint2 = hint2.expect("build 2 publishes an export hint");
        assert_ne!(
            hint1.digest, hint2.digest,
            "append changes the merged digest"
        );
        // `changed_since` is derived from the event-time range tlogfs records on
        // each source version. MemoryPersistence records none, so every version
        // reports an unknown range and the dirty range is the whole axis -- the
        // export rewrites everything. Correct, merely unoptimized; `dirty_lo_us`
        // is unit-tested directly on known ranges. What matters here is that the
        // unknown case degrades to MORE work, never to stale output.
        assert!(
            hint2.changed_since.is_none(),
            "sources with no recorded event range must widen the dirty range, \
             not narrow it: {:?}",
            hint2.changed_since
        );

        // Build 3 (BACKFILL): add day 1, older than everything already built and
        // potentially below the sealed watermark. This used to be a hard error
        // demanding --rebuild, because segments were keyed by the source versions
        // that produced them and there was no way to find which held day 1.
        // Segments are keyed by bucket range now, so the affected ones are simply
        // unsealed and recomputed.
        {
            let root = fs.root().await.unwrap();
            append_day(&root, "/ingest/weather.csv", 1).await;
        }
        let (rows3, hint3) = collect_daily(&make_ctx(Some(cache_dir.clone())), config()).await;
        assert_eq!(rows3.len(), 3, "backfill adds a bucket, got {:?}", rows3);
        assert!(
            approx(rows3[0].0, 132.0) && approx(rows3[1].0, 232.0) && approx(rows3[2].0, 332.0),
            "backfill values wrong (a double-count would inflate these): {:?}",
            rows3
        );
        let hint3 = hint3.expect("build 3 publishes an export hint");
        assert_ne!(hint2.digest, hint3.digest, "backfill changes the digest");
    }

    /// Regression: partials are shared by every resolution, so when a later
    /// build appends a source version the FIRST resolution read writes the new
    /// shared partial. A coarser resolution read afterward must still reflect
    /// the appended data. Before the coverage-based freshness check, the coarser
    /// resolution saw no partial written by its own invocation and served a
    /// stale merged cache, freezing coarse charts while the finest stayed fresh.
    #[tokio::test]
    async fn test_merged_cache_coarse_resolution_reflects_sibling_append() {
        let _ = env_logger::try_init();

        let persistence = tinyfs::MemoryPersistence::default();
        let fs = tinyfs::FS::new(persistence.clone())
            .await
            .expect("create FS");

        async fn append_day(root: &tinyfs::WD, path: &str, day: u32) {
            use tokio::io::AsyncWriteExt;
            let mut csv = String::from("timestamp,temperature\n");
            for hour in 0..24u32 {
                let ts = format!("1970-01-{:02}T{:02}:00:00", day, hour);
                let temp = 20.5 + hour as f64 + day as f64 * 100.0;
                csv.push_str(&format!("{},{}\n", ts, temp));
            }
            let mut w = root
                .async_writer_path_with_type(path, EntryType::FilePhysicalSeries)
                .await
                .unwrap();
            w.write_all(csv.as_bytes()).await.unwrap();
            w.flush().await.unwrap();
            w.shutdown().await.unwrap();
        }

        {
            let root = fs.root().await.unwrap();
            _ = root.create_dir_path("/ingest").await.unwrap();
            append_day(&root, "/ingest/weather.csv", 1).await;
        }

        let config = || TemporalReduceConfig {
            in_pattern: crate::Url::parse("csv:///ingest/weather.csv").unwrap(),
            out_pattern: "weather".to_string(),
            time_column: "timestamp".to_string(),
            resolutions: vec!["1h".to_string(), "1d".to_string()],
            aggregations: vec![agg(AggregationType::Max, &["temperature"])],
            transforms: None,
            allowed_lateness: None,
            // Seal on every advance, as before the size gate existed: these
            // tests assert seal MECHANICS on datasets far under any real target.
            seal_target_bytes: Some(0),
            max_live_segments: None,
        };

        let make_ctx = |cache: std::path::PathBuf| {
            let session = Arc::new(datafusion::prelude::SessionContext::new());
            let _ = crate::register_tinyfs_object_store(&session, persistence.clone())
                .expect("register object store");
            crate::ProviderContext::new(session, Arc::new(persistence.clone()))
                .with_cache_dir(cache)
        };

        async fn collect_res(
            provider_context: &crate::ProviderContext,
            config: TemporalReduceConfig,
            res_file: &str,
        ) -> Vec<f64> {
            let context = test_context(provider_context, FileID::root());
            let temporal_dir = TemporalReduceDirectory::new(config, context).unwrap();
            let temporal_handle = temporal_dir.create_handle();
            let weather_node = temporal_handle.get("weather").await.unwrap().unwrap();
            let NodeType::Directory(weather_dir) = &weather_node.node_type else {
                panic!("expected directory");
            };
            let node = weather_dir.get(res_file).await.unwrap().unwrap();
            let file_id = node.id;
            let NodeType::File(file_handle) = &node.node_type else {
                panic!("expected file node");
            };
            let file_arc = file_handle.get_file().await;
            let file_guard = file_arc.lock().await;
            let queryable = file_guard.as_queryable().expect("queryable");
            let table_provider = queryable
                .as_table_provider(file_id, provider_context)
                .await
                .expect("table provider");
            drop(file_guard);
            let ctx = &provider_context.datafusion_session;
            let table = format!("t_{}", res_file.replace(['=', '.'], "_"));
            _ = ctx.register_table(&table, table_provider).unwrap();
            let batches = ctx
                .sql(&format!(
                    "SELECT \"temperature.max\" FROM {} ORDER BY timestamp",
                    table
                ))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let mut rows = Vec::new();
            for b in &batches {
                let col = b
                    .column_by_name("temperature.max")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .clone();
                for i in 0..b.num_rows() {
                    rows.push(col.value(i));
                }
            }
            rows
        }

        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().to_path_buf();

        // Build 1: seed BOTH resolutions' merged caches with day 1 only.
        let daily1 = collect_res(&make_ctx(cache_dir.clone()), config(), "res=1d.series").await;
        let hourly1 = collect_res(&make_ctx(cache_dir.clone()), config(), "res=1h.series").await;
        assert_eq!(daily1.len(), 1, "one daily bucket after build 1");
        assert_eq!(hourly1.len(), 24, "24 hourly buckets after build 1");

        // Append day 2 as a new source version.
        {
            let root = fs.root().await.unwrap();
            append_day(&root, "/ingest/weather.csv", 2).await;
        }

        // Build 2: read the FINEST resolution first. This writes the new shared
        // finest partial and refreshes only the 1h merged cache.
        let ctx2 = make_ctx(cache_dir.clone());
        let hourly2 = collect_res(&ctx2, config(), "res=1h.series").await;
        assert_eq!(hourly2.len(), 48, "48 hourly buckets after append");

        // The COARSE resolution is read next. Its own invocation writes no
        // partial, yet it must reflect the appended day 2.
        let daily2 = collect_res(&ctx2, config(), "res=1d.series").await;
        assert_eq!(
            daily2.len(),
            2,
            "coarse resolution must reflect the sibling-written append, got {:?}",
            daily2
        );
        let approx = |a: f64, b: f64| (a - b).abs() < 1e-9;
        assert!(
            approx(daily2[0], 143.5) && approx(daily2[1], 243.5),
            "coarse daily max wrong after append: {:?}",
            daily2
        );
    }

    // ---- Phase 2 segment / allowed_lateness tests ---------------------

    async fn append_day_series(fs: &tinyfs::FS, path: &str, day: u32) {
        use tokio::io::AsyncWriteExt;
        let root = fs.root().await.unwrap();
        let mut csv = String::from("timestamp,temperature\n");
        for hour in 0..24u32 {
            let ts = format!("1970-01-{:02}T{:02}:00:00", day, hour);
            let temp = 20.5 + hour as f64 + day as f64 * 100.0;
            csv.push_str(&format!("{},{}\n", ts, temp));
        }
        let mut w = root
            .async_writer_path_with_type(path, EntryType::FilePhysicalSeries)
            .await
            .unwrap();
        w.write_all(csv.as_bytes()).await.unwrap();
        w.flush().await.unwrap();
        w.shutdown().await.unwrap();
    }

    fn lateness_config(lateness: Option<&str>) -> TemporalReduceConfig {
        TemporalReduceConfig {
            in_pattern: crate::Url::parse("csv:///ingest/weather.csv").unwrap(),
            out_pattern: "weather".to_string(),
            time_column: "timestamp".to_string(),
            resolutions: vec!["1d".to_string()],
            // Max ALONE is invariant under both double-counting and dropping a
            // duplicate: max(x, x) == x. It cannot distinguish "every row was
            // read once" from "the sealed window was summed twice", which is
            // exactly how the original double-count survived review (see
            // testsuite 733, where the blind statistic was avg). Sum is carried
            // alongside it so these tests can assert a total.
            aggregations: vec![
                agg(AggregationType::Max, &["temperature"]),
                agg(AggregationType::Sum, &["temperature"]),
            ],
            transforms: None,
            allowed_lateness: lateness.map(str::to_string),
            // Seal on every advance, as before the size gate existed: these
            // tests assert seal MECHANICS on datasets far under any real target.
            seal_target_bytes: Some(0),
            max_live_segments: None,
        }
    }

    fn reduce_ctx(
        persistence: &tinyfs::MemoryPersistence,
        cache_dir: &std::path::Path,
    ) -> crate::ProviderContext {
        let session = Arc::new(datafusion::prelude::SessionContext::new());
        let _ = crate::register_tinyfs_object_store(&session, persistence.clone())
            .expect("register object store");
        crate::ProviderContext::new(session, Arc::new(persistence.clone()))
            .with_cache_dir(cache_dir.to_path_buf())
    }

    async fn daily_max(
        provider_context: &crate::ProviderContext,
        config: TemporalReduceConfig,
    ) -> Result<Vec<f64>, String> {
        let context = test_context(provider_context, FileID::root());
        let temporal_dir = TemporalReduceDirectory::new(config, context).unwrap();
        let temporal_handle = temporal_dir.create_handle();
        let weather_node = temporal_handle.get("weather").await.unwrap().unwrap();
        let NodeType::Directory(weather_dir) = &weather_node.node_type else {
            panic!("expected directory");
        };
        let daily_node = weather_dir.get("res=1d.series").await.unwrap().unwrap();
        let file_id = daily_node.id;
        let NodeType::File(file_handle) = &daily_node.node_type else {
            panic!("expected file node");
        };
        let file_arc = file_handle.get_file().await;
        let file_guard = file_arc.lock().await;
        let queryable = file_guard.as_queryable().expect("queryable");
        let table_provider = match queryable.as_table_provider(file_id, provider_context).await {
            Ok(p) => p,
            Err(e) => return Err(e.to_string()),
        };
        drop(file_guard);
        let ctx = &provider_context.datafusion_session;
        _ = ctx.register_table("reduced", table_provider).unwrap();
        let batches = ctx
            .sql("SELECT \"temperature.max\" FROM reduced ORDER BY timestamp")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for b in &batches {
            let col = b
                .column_by_name("temperature.max")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .clone();
            for i in 0..b.num_rows() {
                rows.push(col.value(i));
            }
        }
        _ = ctx.deregister_table("reduced").unwrap();
        Ok(rows)
    }

    /// Each day appended by [`append_day_series`] holds 24 hourly rows of
    /// `20.5 + hour + day * 100`, so its total is `768 + 2400 * day`. A rollup
    /// that counted the day twice would report double this; one that dropped a
    /// segment would report less. Unlike the daily max, this moves.
    fn expected_day_sum(day: u32) -> f64 {
        768.0 + 2400.0 * f64::from(day)
    }

    /// Like [`daily_max`] but reads an arbitrary output column, for aggregations
    /// other than Max (e.g. verifying `temperature.avg` reconstructs at read).
    async fn daily_max_col(
        provider_context: &crate::ProviderContext,
        config: TemporalReduceConfig,
        column: &str,
    ) -> Result<Vec<f64>, String> {
        let context = test_context(provider_context, FileID::root());
        let temporal_dir = TemporalReduceDirectory::new(config, context).unwrap();
        let temporal_handle = temporal_dir.create_handle();
        let weather_node = temporal_handle.get("weather").await.unwrap().unwrap();
        let NodeType::Directory(weather_dir) = &weather_node.node_type else {
            panic!("expected directory");
        };
        let daily_node = weather_dir.get("res=1d.series").await.unwrap().unwrap();
        let file_id = daily_node.id;
        let NodeType::File(file_handle) = &daily_node.node_type else {
            panic!("expected file node");
        };
        let file_arc = file_handle.get_file().await;
        let file_guard = file_arc.lock().await;
        let queryable = file_guard.as_queryable().expect("queryable");
        let table_provider = match queryable.as_table_provider(file_id, provider_context).await {
            Ok(p) => p,
            Err(e) => return Err(e.to_string()),
        };
        drop(file_guard);
        let ctx = &provider_context.datafusion_session;
        _ = ctx.register_table("reduced", table_provider).unwrap();
        let batches = ctx
            .sql(&format!(
                "SELECT \"{column}\" FROM reduced ORDER BY timestamp"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for b in &batches {
            let col = b
                .column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .clone();
            for i in 0..b.num_rows() {
                rows.push(col.value(i));
            }
        }
        _ = ctx.deregister_table("reduced").unwrap();
        Ok(rows)
    }

    fn walk_find(dir: &std::path::Path, pred: &dyn Fn(&str) -> bool) -> Vec<std::path::PathBuf> {
        let mut out = Vec::new();
        let Ok(entries) = std::fs::read_dir(dir) else {
            return out;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walk_find(&path, pred));
            } else if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && pred(name)
            {
                out.push(path);
            }
        }
        out
    }

    /// Phase 3 step 1 invariant: segment + hot files store the *mergeable
    /// partials* (`__p_*`: sum/count/min/max), not the reconstructed output
    /// columns. This is what makes a segment associatively foldable (so a coarser
    /// resolution can later be built from a finer one without corrupting `Avg`),
    /// while consumers still read reconstructed output via the read-time view.
    #[tokio::test]
    async fn test_sealed_runs_store_partials_not_output() {
        let _ = env_logger::try_init();
        let persistence = tinyfs::MemoryPersistence::default();
        let fs = tinyfs::FS::new(persistence.clone()).await.unwrap();
        {
            let root = fs.root().await.unwrap();
            _ = root.create_dir_path("/ingest").await.unwrap();
        }
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().to_path_buf();

        // An Avg aggregation is the discriminating case: reconstructed `avg` is
        // not foldable, but the stored sum+count partials are.
        let config = || TemporalReduceConfig {
            in_pattern: crate::Url::parse("csv:///ingest/weather.csv").unwrap(),
            out_pattern: "weather".to_string(),
            time_column: "timestamp".to_string(),
            resolutions: vec!["1d".to_string()],
            aggregations: vec![agg(AggregationType::Avg, &["temperature"])],
            transforms: None,
            allowed_lateness: Some("1d".to_string()),
            // Seal on every advance, as before the size gate existed: these
            // tests assert seal MECHANICS on datasets far under any real target.
            seal_target_bytes: Some(0),
            max_live_segments: None,
        };

        // Grow history so the watermark advances and buckets seal into segments.
        for day in 1..=4u32 {
            append_day_series(&fs, "/ingest/weather.csv", day).await;
            let context = test_context(&reduce_ctx(&persistence, &cache_dir), FileID::root());
            let temporal_dir = TemporalReduceDirectory::new(config(), context).unwrap();
            let temporal_handle = temporal_dir.create_handle();
            let weather_node = temporal_handle.get("weather").await.unwrap().unwrap();
            let NodeType::Directory(weather_dir) = &weather_node.node_type else {
                panic!("expected directory");
            };
            let daily_node = weather_dir.get("res=1d.series").await.unwrap().unwrap();
            let file_id = daily_node.id;
            let NodeType::File(file_handle) = &daily_node.node_type else {
                panic!("expected file node");
            };
            let file_arc = file_handle.get_file().await;
            let file_guard = file_arc.lock().await;
            let queryable = file_guard.as_queryable().expect("queryable");
            let pctx = reduce_ctx(&persistence, &cache_dir);
            // Rebuild against a fresh cache_dir-backed context each iteration.
            let _ = queryable.as_table_provider(file_id, &pctx).await;
        }

        let segs = walk_find(&cache_dir, &|n| {
            n.starts_with("seg-") && n.ends_with(".parquet")
        });
        assert!(!segs.is_empty(), "expected at least one segment file");

        // Inspect the on-disk schema of a segment file.
        let inspect = datafusion::prelude::SessionContext::new();
        inspect
            .register_parquet(
                "seg",
                segs[0].to_string_lossy().as_ref(),
                datafusion::prelude::ParquetReadOptions::default(),
            )
            .await
            .unwrap();
        let schema = inspect.table("seg").await.unwrap().schema().clone();
        let cols: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

        assert!(
            cols.iter().any(|c| c == "timestamp"),
            "segment must carry the timestamp column, got {cols:?}"
        );
        assert!(
            cols.iter().any(|c| c.starts_with("__p_sum_")),
            "segment must store the sum partial, got {cols:?}"
        );
        assert!(
            cols.iter().any(|c| c.starts_with("__p_count_")),
            "segment must store the count partial, got {cols:?}"
        );
        assert!(
            !cols.iter().any(|c| c == "temperature.avg"),
            "segment must NOT store the reconstructed output column, got {cols:?}"
        );

        // And the read-time view still reconstructs the output column correctly.
        let rows = daily_max_col(
            &reduce_ctx(&persistence, &cache_dir),
            config(),
            "temperature.avg",
        )
        .await
        .unwrap();
        let approx = |a: f64, b: f64| (a - b).abs() < 1e-9;
        assert_eq!(rows.len(), 4);
        for (i, day) in (1..=4u32).enumerate() {
            // Daily mean of temps 20.5+hour+day*100 over hours 0..24.
            let expected = 20.5 + 11.5 + day as f64 * 100.0;
            assert!(approx(rows[i], expected), "day {day} avg wrong: {rows:?}");
        }
    }

    /// As history grows by append-only versions, the watermark advances and
    /// output buckets that fall out of the allowed_lateness window are frozen
    /// into immutable `seg-*.parquet` files; the query output still spans all
    /// days. This is the mechanism that bounds per-build memory to the hot
    /// window instead of all history.
    #[tokio::test]
    async fn test_sealed_runs_advance_as_history_grows() {
        let _ = env_logger::try_init();
        let persistence = tinyfs::MemoryPersistence::default();
        let fs = tinyfs::FS::new(persistence.clone()).await.unwrap();
        {
            let root = fs.root().await.unwrap();
            _ = root.create_dir_path("/ingest").await.unwrap();
        }
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().to_path_buf();

        // Append days 1..=4 as successive newer versions, rebuilding each time.
        for day in 1..=4u32 {
            append_day_series(&fs, "/ingest/weather.csv", day).await;
            let rows = daily_max(
                &reduce_ctx(&persistence, &cache_dir),
                lateness_config(Some("1d")),
            )
            .await
            .expect("append within lateness window must succeed");
            assert_eq!(rows.len(), day as usize, "day {day}: one bucket per day");
        }

        // The watermark (data_hi - 1d) has advanced past the earliest days, so
        // at least one segment file must exist on disk.
        let segs = walk_find(&cache_dir, &|n| {
            n.starts_with("seg-") && n.ends_with(".parquet")
        });
        assert!(
            !segs.is_empty(),
            "expected segment files after history advanced, found none under {}",
            cache_dir.display()
        );
        // A hot file and a manifest must also be present.
        let hots = walk_find(&cache_dir, &|n| n == "hot.parquet");
        assert!(!hots.is_empty(), "expected a hot.parquet");
        let manifests = walk_find(&cache_dir, &|n| n == "manifest.json");
        assert_eq!(manifests.len(), 1, "one manifest for the single resolution");

        // Final output still spans all four days with correct maxima.
        let rows = daily_max(
            &reduce_ctx(&persistence, &cache_dir),
            lateness_config(Some("1d")),
        )
        .await
        .unwrap();
        let approx = |a: f64, b: f64| (a - b).abs() < 1e-9;
        assert_eq!(rows.len(), 4);
        for (i, day) in (1..=4u32).enumerate() {
            assert!(
                approx(rows[i], 43.5 + day as f64 * 100.0),
                "day {day} max wrong: {:?}",
                rows
            );
        }
    }

    /// Compaction bounds the number of live segment files, and the merged segments
    /// answer exactly what the originals did.
    ///
    /// Seals on every advance and sets a tiny backstop so that many small segments
    /// accumulate and the ragged-input path fires -- the situation a real pond
    /// reaches over months, reproduced in a few dozen buckets. Without
    /// compaction the segment count grows forever and every query opens all of them.
    ///
    /// The load-bearing assertion is not the file count but the VALUES: merging
    /// partials is associative, so folding a window of segments into one must give
    /// bit-for-bit the same answer as leaving them apart. If it did not, the
    /// symptom would be a silently wrong aggregate, which is why the check is on
    /// every day's maximum rather than on the shape of the cache.
    #[tokio::test]
    async fn compaction_bounds_segment_count_without_changing_answers() {
        let _ = env_logger::try_init();
        let persistence = tinyfs::MemoryPersistence::default();
        let fs = tinyfs::FS::new(persistence.clone()).await.unwrap();
        {
            let root = fs.root().await.unwrap();
            _ = root.create_dir_path("/ingest").await.unwrap();
        }
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().to_path_buf();

        const DAYS: u32 = 16;
        let compacting = || {
            let mut c = lateness_config(Some("1d"));
            c.seal_target_bytes = Some(0); // seal on every advance
            c.max_live_segments = Some(2); // force the backstop constantly
            c
        };

        for day in 1..=DAYS {
            append_day_series(&fs, "/ingest/weather.csv", day).await;
            let rows = daily_max(&reduce_ctx(&persistence, &cache_dir), compacting())
                .await
                .expect("append within lateness window must succeed");
            assert_eq!(rows.len(), day as usize, "day {day}: one bucket per day");
        }

        // Every day is still answered correctly after repeated merges.
        let rows = daily_max(&reduce_ctx(&persistence, &cache_dir), compacting())
            .await
            .unwrap();
        let approx = |a: f64, b: f64| (a - b).abs() < 1e-9;
        assert_eq!(rows.len(), DAYS as usize, "all days survive compaction");
        for (i, day) in (1..=DAYS).enumerate() {
            assert!(
                approx(rows[i], 43.5 + f64::from(day) * 100.0),
                "day {day} max wrong after compaction: {rows:?}"
            );
        }

        // The load-bearing assertion: a merge that duplicated or dropped rows
        // leaves the daily max untouched but moves the daily sum.
        let sums = daily_max_col(
            &reduce_ctx(&persistence, &cache_dir),
            compacting(),
            "temperature.sum",
        )
        .await
        .unwrap();
        assert_eq!(
            sums.len(),
            DAYS as usize,
            "one sum per day after compaction"
        );
        for (i, day) in (1..=DAYS).enumerate() {
            assert!(
                approx(sums[i], expected_day_sum(day)),
                "day {day} sum wrong after compaction (double-counted or dropped): \
                 got {}, want {}",
                sums[i],
                expected_day_sum(day)
            );
        }

        // The segment count is bounded well below one-per-sealed-advance.
        let manifests = walk_find(&cache_dir, &|n| n == "manifest.json");
        assert_eq!(manifests.len(), 1, "one manifest for the single resolution");
        let m: Value = serde_json::from_slice(&std::fs::read(&manifests[0]).unwrap()).unwrap();
        let live = m["segments"].as_array().expect("segments array").len();
        // Tight on purpose: the backstop merges until the count is back within
        // max_live_segments, so anything above it means compaction did not run to
        // completion. A loose bound like `live < DAYS` would pass even with
        // compaction disabled entirely, since sealing alone yields one run per
        // day -- which is exactly the growth this phase exists to stop.
        assert!(
            live <= 2,
            "compaction left {live} runs; max_live_segments was 2 (after {DAYS} days)"
        );

        // Superseded inputs were actually deleted, not merely unlinked from the
        // manifest -- otherwise compaction would trade read cost for unbounded
        // disk. Every segment file on disk must be one the manifest names.
        let named: std::collections::BTreeSet<String> = m["segments"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["name"].as_str().unwrap().to_string())
            .collect();
        let on_disk = walk_find(&cache_dir, &|n| {
            n.starts_with("seg-") && n.ends_with(".parquet")
        });
        for f in &on_disk {
            let n = f.file_name().unwrap().to_str().unwrap();
            assert!(named.contains(n), "orphaned run left on disk: {n}");
        }
        assert_eq!(on_disk.len(), live, "disk and manifest must agree");
    }

    /// Under the DEFAULT seal target, four days of a handful of rows each never
    /// accumulate enough bytes to be worth freezing, so no segment file is written
    /// at all -- yet the query still returns every day correctly, because the
    /// unsealed buckets simply stay in the recomputed hot window.
    ///
    /// This is the point of the size gate. Sealing used to fire whenever the
    /// watermark moved, so a pond rebuilt on a timer grew one small file per
    /// build regardless of how little data had arrived. The companion test
    /// `test_sealed_runs_advance_as_history_grows` pins the mechanics with the
    /// gate disabled; this one pins the policy, and together they show that
    /// whether a bucket is sealed or hot is invisible to the answer.
    #[tokio::test]
    async fn small_data_seals_nothing_yet_still_reads_correctly() {
        let _ = env_logger::try_init();
        let persistence = tinyfs::MemoryPersistence::default();
        let fs = tinyfs::FS::new(persistence.clone()).await.unwrap();
        {
            let root = fs.root().await.unwrap();
            _ = root.create_dir_path("/ingest").await.unwrap();
        }
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().to_path_buf();

        // Same shape as the mechanics test, but leaving seal_target_bytes unset
        // so the real default (SEAL_TARGET_BYTES) applies.
        let default_target = || {
            let mut c = lateness_config(Some("1d"));
            c.seal_target_bytes = None;
            c
        };

        for day in 1..=4u32 {
            append_day_series(&fs, "/ingest/weather.csv", day).await;
            let rows = daily_max(&reduce_ctx(&persistence, &cache_dir), default_target())
                .await
                .expect("append within lateness window must succeed");
            assert_eq!(rows.len(), day as usize, "day {day}: one bucket per day");
        }

        let segs = walk_find(&cache_dir, &|n| {
            n.starts_with("seg-") && n.ends_with(".parquet")
        });
        assert!(
            segs.is_empty(),
            "a few rows per day is far under {} bytes, so nothing should have \
             been sealed; found {:?}",
            crate::rollup_cache::SEAL_TARGET_BYTES,
            segs
        );

        // Unsealed does not mean unavailable: every day is still answered, from
        // the hot window alone.
        let rows = daily_max(&reduce_ctx(&persistence, &cache_dir), default_target())
            .await
            .unwrap();
        let approx = |a: f64, b: f64| (a - b).abs() < 1e-9;
        assert_eq!(rows.len(), 4, "all four days still present without any run");
        for (i, day) in (1..=4u32).enumerate() {
            assert!(
                approx(rows[i], 43.5 + day as f64 * 100.0),
                "day {day} max wrong: {rows:?}"
            );
        }

        // And the watermark still advanced: the buckets are frozen-but-unsealed,
        // waiting to be written as one larger run rather than four tiny ones.
        let manifests = walk_find(&cache_dir, &|n| n == "manifest.json");
        assert_eq!(manifests.len(), 1, "one manifest for the single resolution");
        let m: Value = serde_json::from_slice(&std::fs::read(&manifests[0]).unwrap()).unwrap();
        assert!(
            m["segments"].as_array().is_some_and(Vec::is_empty),
            "manifest must record no segments: {m}"
        );
    }

    /// A backfill that lands INSIDE the allowed_lateness window (nothing older
    /// than it has been sealed yet) is accepted and merged, in contrast to the
    /// beyond-window backfill that hard-fails. Here a generous 10-day window
    /// keeps everything hot, so day 1 arriving last still merges cleanly.
    #[tokio::test]
    async fn test_within_window_backfill_succeeds() {
        let _ = env_logger::try_init();
        let persistence = tinyfs::MemoryPersistence::default();
        let fs = tinyfs::FS::new(persistence.clone()).await.unwrap();
        {
            let root = fs.root().await.unwrap();
            _ = root.create_dir_path("/ingest").await.unwrap();
        }
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().to_path_buf();

        // Seed with day 5, then backfill day 1 (four days earlier) under a
        // 10-day lateness window: the watermark is far below day 1, so nothing
        // is sealed and the backfill is within the hot window.
        append_day_series(&fs, "/ingest/weather.csv", 5).await;
        let r1 = daily_max(
            &reduce_ctx(&persistence, &cache_dir),
            lateness_config(Some("10d")),
        )
        .await
        .unwrap();
        assert_eq!(r1.len(), 1);

        append_day_series(&fs, "/ingest/weather.csv", 1).await;
        let rows = daily_max(
            &reduce_ctx(&persistence, &cache_dir),
            lateness_config(Some("10d")),
        )
        .await
        .expect("within-window backfill must succeed");
        let approx = |a: f64, b: f64| (a - b).abs() < 1e-9;
        assert_eq!(rows.len(), 2, "day1 + day5, got {:?}", rows);
        assert!(
            approx(rows[0], 143.5) && approx(rows[1], 543.5),
            "within-window backfill values wrong: {:?}",
            rows
        );
    }

    /// Regression: a coarse level must unseal on a backfill even when the finer
    /// level's change was already consumed by a SIBLING output file's build.
    ///
    /// Each output file is a separate build over the shared cache, and
    /// `res=1h.series` is read before `res=1d.series` here. The 1h build
    /// unseals and absorbs the backfill; the 1d build that follows therefore
    /// sees the 1h level report `Nothing` -- it genuinely did not move in that
    /// call -- while the 1d level has never folded the backfilled day.
    ///
    /// Keying the 1d level's unsealing on the finer level's in-call change alone
    /// made it advance in place, recompute only its hot window, and record the
    /// new finer digest, marking itself fresh forever with the backfilled day
    /// missing from its sealed history. It now also compares the live source set
    /// against the one it last folded, so the staleness is visible to it
    /// directly.
    #[tokio::test]
    async fn test_coarse_unseals_backfill_consumed_by_sibling_build() {
        let _ = env_logger::try_init();
        let persistence = tinyfs::MemoryPersistence::default();
        let fs = tinyfs::FS::new(persistence.clone()).await.unwrap();
        {
            let root = fs.root().await.unwrap();
            _ = root.create_dir_path("/ingest").await.unwrap();
        }
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().to_path_buf();

        // Both resolutions, so the 1d level is built by folding the 1h level.
        // Zero lateness plus a zero seal target puts everything below the
        // newest bucket into sealed segments, which is where the defect hides:
        // a hot-window recompute alone cannot repair a sealed bucket.
        let config = || TemporalReduceConfig {
            resolutions: vec!["1h".to_string(), "1d".to_string()],
            ..lateness_config(Some("0s"))
        };

        async fn sums(
            provider_context: &crate::ProviderContext,
            config: TemporalReduceConfig,
            res_file: &str,
        ) -> Vec<f64> {
            let context = test_context(provider_context, FileID::root());
            let temporal_dir = TemporalReduceDirectory::new(config, context).unwrap();
            let temporal_handle = temporal_dir.create_handle();
            let weather_node = temporal_handle.get("weather").await.unwrap().unwrap();
            let NodeType::Directory(weather_dir) = &weather_node.node_type else {
                panic!("expected directory");
            };
            let node = weather_dir.get(res_file).await.unwrap().unwrap();
            let file_id = node.id;
            let NodeType::File(file_handle) = &node.node_type else {
                panic!("expected file node");
            };
            let file_arc = file_handle.get_file().await;
            let file_guard = file_arc.lock().await;
            let queryable = file_guard.as_queryable().expect("queryable");
            let table_provider = queryable
                .as_table_provider(file_id, provider_context)
                .await
                .expect("table provider");
            drop(file_guard);
            let ctx = &provider_context.datafusion_session;
            let table = format!("t_{}", res_file.replace(['=', '.'], "_"));
            _ = ctx.register_table(&table, table_provider).unwrap();
            let batches = ctx
                .sql(&format!(
                    "SELECT \"temperature.sum\" FROM {} ORDER BY timestamp",
                    table
                ))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let mut rows = Vec::new();
            for b in &batches {
                let col = b
                    .column_by_name("temperature.sum")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .clone();
                for i in 0..b.num_rows() {
                    rows.push(col.value(i));
                }
            }
            _ = ctx.deregister_table(&table).unwrap();
            rows
        }

        // Seed both levels with days 3..=5, so the 1d level accumulates sealed
        // segments well above the day the backfill will land in.
        for day in 3..=5u32 {
            append_day_series(&fs, "/ingest/weather.csv", day).await;
        }
        let seeded = sums(
            &reduce_ctx(&persistence, &cache_dir),
            config(),
            "res=1d.series",
        )
        .await;
        assert_eq!(seeded.len(), 3, "days 3..=5 seeded, got {:?}", seeded);
        let _ = sums(
            &reduce_ctx(&persistence, &cache_dir),
            config(),
            "res=1h.series",
        )
        .await;

        // Backfill day 1: older than every sealed bucket in either level.
        append_day_series(&fs, "/ingest/weather.csv", 1).await;

        // The FINEST resolution is read first and consumes the change.
        let ctx = reduce_ctx(&persistence, &cache_dir);
        let hourly = sums(&ctx, config(), "res=1h.series").await;
        assert_eq!(
            hourly.len(),
            24 * 4,
            "four days of hourly buckets after the backfill, got {}",
            hourly.len()
        );

        // The COARSE resolution is read second, in the same session. Its finer
        // level now reports no change, but it has never folded day 1.
        let daily = sums(&ctx, config(), "res=1d.series").await;
        assert_eq!(
            daily.len(),
            4,
            "coarse level dropped the backfilled day, which its sealed segments \
             cannot repair by recomputing the hot window alone: {:?}",
            daily
        );
        let approx = |a: f64, b: f64| (a - b).abs() < 1e-6;
        for (i, day) in [1u32, 3, 4, 5].iter().copied().enumerate() {
            assert!(
                approx(daily[i], expected_day_sum(day)),
                "coarse day {} sum wrong (a double-count inflates, a lost \
                 segment deflates): got {}, want {}",
                day,
                daily[i],
                expected_day_sum(day)
            );
        }
    }

    /// Changing `allowed_lateness` invalidates the sealed layout: the manifest
    /// records the value it was built under, and a build with a different value
    /// wipes the res dir and rebuilds from the retained partials. The rebuilt
    /// manifest reflects the new lateness and the output is still correct.
    #[tokio::test]
    async fn test_allowed_lateness_change_resets_cache() {
        let _ = env_logger::try_init();
        let persistence = tinyfs::MemoryPersistence::default();
        let fs = tinyfs::FS::new(persistence.clone()).await.unwrap();
        {
            let root = fs.root().await.unwrap();
            _ = root.create_dir_path("/ingest").await.unwrap();
        }
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().to_path_buf();

        // Build days 1..=3 under a 1-day window (seals early runs).
        for day in 1..=3u32 {
            append_day_series(&fs, "/ingest/weather.csv", day).await;
            _ = daily_max(
                &reduce_ctx(&persistence, &cache_dir),
                lateness_config(Some("1d")),
            )
            .await
            .unwrap();
        }
        let manifests = walk_find(&cache_dir, &|n| n == "manifest.json");
        assert_eq!(manifests.len(), 1);
        let m: Value = serde_json::from_slice(&std::fs::read(&manifests[0]).unwrap()).unwrap();
        assert_eq!(m["allowed_lateness_secs"], 86400, "1d window recorded");

        // Rebuild the SAME data with a 3-day window: mismatch -> reset + rebuild.
        let rows = daily_max(
            &reduce_ctx(&persistence, &cache_dir),
            lateness_config(Some("3d")),
        )
        .await
        .expect("lateness change resets and rebuilds cleanly");
        let approx = |a: f64, b: f64| (a - b).abs() < 1e-9;
        assert_eq!(rows.len(), 3, "all three days after reset, got {:?}", rows);
        for (i, day) in (1..=3u32).enumerate() {
            assert!(
                approx(rows[i], 43.5 + day as f64 * 100.0),
                "day {day} max wrong after reset: {:?}",
                rows
            );
        }
        let m2: Value = serde_json::from_slice(&std::fs::read(&manifests[0]).unwrap()).unwrap();
        assert_eq!(
            m2["allowed_lateness_secs"], 259200,
            "manifest reflects the new 3d window after reset"
        );
    }

    /// Read-path memory bound: the segments + hot file are declared sorted on
    /// the timestamp column, so a full-history `ORDER BY timestamp` over a
    /// multi-run reduced series is satisfied by a streaming k-way merge, never a
    /// `SortExec` that buffers the whole series. This is the O(1)-memory read
    /// path from design §3. We assert the physical plan of a fresh consumer
    /// session (target_partitions high enough that each file is its own
    /// partition) contains a `SortPreservingMergeExec` and no `SortExec`.
    #[tokio::test]
    async fn test_sealed_read_uses_sort_preserving_merge() {
        let _ = env_logger::try_init();
        let persistence = tinyfs::MemoryPersistence::default();
        let fs = tinyfs::FS::new(persistence.clone()).await.unwrap();
        {
            let root = fs.root().await.unwrap();
            _ = root.create_dir_path("/ingest").await.unwrap();
        }
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().to_path_buf();

        // Grow history to several days under a 1-day window so multiple sealed
        // runs plus a hot file coexist.
        for day in 1..=6u32 {
            append_day_series(&fs, "/ingest/weather.csv", day).await;
            _ = daily_max(
                &reduce_ctx(&persistence, &cache_dir),
                lateness_config(Some("1d")),
            )
            .await
            .unwrap();
        }
        // At least one segment plus the hot file, i.e. two separately-ordered
        // inputs -- which is what the k-way merge has to reconcile. Only one run
        // survives here: MemoryPersistence records no event-time bounds, so every
        // source version reports an unknown range, every build's dirty range is
        // the whole axis, and the sealed segments are unsealed and re-sealed as
        // one. Under tlogfs the bounds are recorded and segments accumulate.
        let segs = walk_find(&cache_dir, &|n| {
            n.starts_with("seg-") && n.ends_with(".parquet")
        });
        assert!(
            !segs.is_empty(),
            "test needs a segment plus hot to exercise the merge, got none"
        );

        // Acquire the reduced table provider (built during the last rollup).
        let provider_context = reduce_ctx(&persistence, &cache_dir);
        let context = test_context(&provider_context, FileID::root());
        let temporal_dir =
            TemporalReduceDirectory::new(lateness_config(Some("1d")), context).unwrap();
        let temporal_handle = temporal_dir.create_handle();
        let weather_node = temporal_handle.get("weather").await.unwrap().unwrap();
        let NodeType::Directory(weather_dir) = &weather_node.node_type else {
            panic!("expected directory");
        };
        let daily_node = weather_dir.get("res=1d.series").await.unwrap().unwrap();
        let file_id = daily_node.id;
        let NodeType::File(file_handle) = &daily_node.node_type else {
            panic!("expected file node");
        };
        let file_arc = file_handle.get_file().await;
        let file_guard = file_arc.lock().await;
        let queryable = file_guard.as_queryable().expect("queryable");
        let table_provider = queryable
            .as_table_provider(file_id, &provider_context)
            .await
            .unwrap();
        drop(file_guard);

        // Fresh consumer session, independent of the build session, with enough
        // target partitions that each segment + hot lands in its own scan
        // partition, and statistics-based file-group splitting enabled (as the
        // rollup build enables on the shared production session) so the declared
        // per-file ordering is honored.
        let mut config = datafusion::prelude::SessionConfig::new().with_target_partitions(8);
        config
            .options_mut()
            .execution
            .split_file_groups_by_statistics = true;
        let consumer = datafusion::prelude::SessionContext::new_with_config(config);
        _ = consumer.register_table("reduced", table_provider).unwrap();
        let plan = consumer
            .sql("SELECT * FROM reduced ORDER BY \"timestamp\"")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan_str = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        assert!(
            !plan_str.contains("SortExec"),
            "full-history ORDER BY must NOT buffer via a global SortExec; plan was:\n{plan_str}"
        );
        // The ORDER BY must be satisfied by streaming: either a k-way
        // `SortPreservingMergeExec` across per-file partitions, or directly by the
        // scan's declared `output_ordering` when the disjoint, individually-sorted
        // run/hot files collapse into a single ordered file group. Both are
        // O(1)-memory (no whole-series buffering).
        assert!(
            plan_str.contains("SortPreservingMergeExec") || plan_str.contains("output_ordering"),
            "full-history ORDER BY must stream (SortPreservingMergeExec or scan output_ordering); plan was:\n{plan_str}"
        );

        // The plan must still produce correct, fully-ordered output.
        let batches = consumer
            .sql("SELECT \"temperature.max\" FROM reduced ORDER BY \"timestamp\"")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for b in &batches {
            let col = b
                .column_by_name("temperature.max")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .clone();
            for i in 0..b.num_rows() {
                rows.push(col.value(i));
            }
        }
        let approx = |a: f64, b: f64| (a - b).abs() < 1e-9;
        assert_eq!(rows.len(), 6, "six daily buckets");
        for (i, day) in (1..=6u32).enumerate() {
            assert!(
                approx(rows[i], 43.5 + day as f64 * 100.0),
                "day {day} max wrong: {:?}",
                rows
            );
        }
    }
}
