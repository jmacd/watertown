// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Shared export logic for writing pond series/tables as Hive-partitioned parquet.
//!
//! This module contains the reusable core of `pond export` -- the DataFusion
//! `COPY TO ... PARTITIONED BY` pipeline, output scanning, and temporal bounds
//! extraction. It is used by both the CLI `export` command and the `sitegen`
//! factory.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Schema information for templates (matches original format)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TemplateSchema {
    pub fields: Vec<TemplateField>,
}

/// Field information for templates (matches original format)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TemplateField {
    pub name: String,       // Field name
    pub instrument: String, // Instrument path (parsed from field name)
    pub unit: String,       // Unit (parsed from field name)
    pub agg: String,        // Aggregation type (parsed from field name)
}

/// Metadata about an exported file
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExportOutput {
    /// Relative path to exported file
    pub file: PathBuf,
    /// UTC timestamp for start of data (seconds)
    pub start_time: Option<i64>,
    /// UTC timestamp for end of data (seconds)
    pub end_time: Option<i64>,
}

/// Export leaf containing files and schema information
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExportLeaf {
    pub files: Vec<ExportOutput>,
    pub schema: TemplateSchema,
}

/// One reconciled output partition recorded in a series export manifest.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ManifestPartition {
    /// Path relative to the export base output dir, e.g.
    /// `WellDepth/res=1m/year=2025/month=7/data.parquet`.
    pub file: PathBuf,
    /// blake3 of the partition file's bytes, verified before any reuse.
    pub digest: String,
    pub start_time: Option<i64>,
    pub end_time: Option<i64>,
}

/// Per-series export manifest written alongside a series' partition tree. It
/// records which merged-output content produced the partitions so a later build
/// can skip rewriting partitions whose source buckets did not change.
///
/// The manifest travels with the build output dir: `run.sh` seeds each new
/// `build-<ts>/` from the previous `current/`, so the export reads the previous
/// build's manifest as its reconciliation seed.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct SeriesExportManifest {
    /// The merged-content digest that produced these partitions, from the
    /// reduction provider's export hint. Equal digest across builds means the
    /// entire series output is unchanged.
    pub digest: Option<String>,
    pub partitions: Vec<ManifestPartition>,
}

/// File name of the per-series export manifest inside a series export dir.
pub const MANIFEST_FILE: &str = ".export-manifest.json";

/// Maximum number of Hive partitions written by a single `COPY ... PARTITIONED
/// BY` statement.
///
/// DataFusion's partitioned write holds one open Parquet writer per partition
/// for the whole statement and never closes one early, even though our source
/// query is `ORDER BY` the timestamp and therefore visits partitions in order.
/// Each open writer costs on the order of a megabyte of column and page
/// buffers, so peak memory tracks *partition count*, not data volume. Measured
/// on a 1-minute series whose partitioned form is ~100 MB of data:
///
/// | partitioning        | partitions | peak     |
/// |---------------------|-----------:|---------:|
/// | `year`              |          4 |    50 MB |
/// | `year,month`        |         48 |   185 MB |
/// | `year,month,day`    |       1412 |  2042 MB |
///
/// Chunking the rewrite bounds that cost, at the price of one enumeration pass
/// over the timestamp column plus one source scan per chunk. The budget is
/// deliberately conservative: wider schemas hold more per-writer buffers, and
/// the export runs alongside the rest of a site build.
const MAX_OPEN_PARTITIONS: usize = 128;

/// Hierarchical metadata structure for export results
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum ExportSet {
    Empty,
    Files(ExportLeaf),
    Map(HashMap<String, Box<ExportSet>>),
}

impl ExportSet {
    /// Construct hierarchical export set from capture groups, outputs, and schema
    #[must_use]
    pub fn construct_with_schema(
        inputs: Vec<(Vec<String>, ExportOutput)>,
        schema: TemplateSchema,
    ) -> Self {
        let mut eset = ExportSet::Empty;
        for (captures, output) in inputs {
            eset.insert_with_schema(&captures, output, &schema);
        }
        eset
    }

    /// Construct hierarchical export set from capture groups and outputs (legacy compatibility)
    #[must_use]
    pub fn construct(inputs: Vec<(Vec<String>, ExportOutput)>) -> Self {
        let empty_schema = TemplateSchema { fields: vec![] };
        Self::construct_with_schema(inputs, empty_schema)
    }

    /// Insert output at path specified by capture groups with schema
    fn insert_with_schema(
        &mut self,
        captures: &[String],
        output: ExportOutput,
        schema: &TemplateSchema,
    ) {
        if captures.is_empty() {
            if let ExportSet::Empty = self {
                *self = ExportSet::Files(ExportLeaf {
                    files: vec![],
                    schema: schema.clone(),
                });
            }
            if let ExportSet::Files(leaf) = self {
                leaf.files.push(output);
            }
            return;
        }

        if let ExportSet::Empty = self {
            *self = ExportSet::Map(HashMap::new());
        }
        if let ExportSet::Map(map) = self {
            _ = map
                .entry(captures[0].clone())
                .and_modify(|e| {
                    e.insert_with_schema(&captures[1..], output.clone(), schema);
                })
                .or_insert_with(|| {
                    let mut x = ExportSet::Empty;
                    x.insert_with_schema(&captures[1..], output.clone(), schema);
                    Box::new(x)
                });
        }
    }

    /// Legacy insert method for backwards compatibility
    pub fn insert(&mut self, captures: &[String], output: ExportOutput) {
        let empty_schema = TemplateSchema { fields: vec![] };
        self.insert_with_schema(captures, output, &empty_schema);
    }

    /// Filter export set to only include entries matching the given capture path
    #[must_use]
    pub fn filter_by_captures(&self, target_captures: &[String]) -> ExportSet {
        match self {
            ExportSet::Empty => ExportSet::Empty,
            ExportSet::Files(_leaf) => {
                if target_captures.is_empty() {
                    self.clone()
                } else {
                    ExportSet::Empty
                }
            }
            ExportSet::Map(map) => {
                if target_captures.is_empty() {
                    ExportSet::Empty
                } else {
                    let first_capture = &target_captures[0];
                    if let Some(nested_set) = map.get(first_capture) {
                        nested_set.filter_by_captures(&target_captures[1..])
                    } else {
                        ExportSet::Empty
                    }
                }
            }
        }
    }
}

/// Merge two ExportSets, preserving per-target schemas
#[must_use]
pub fn merge_export_sets(base: ExportSet, other: ExportSet) -> ExportSet {
    match (base, other) {
        (ExportSet::Empty, other) => other,
        (base, ExportSet::Empty) => base,
        (ExportSet::Files(mut base_leaf), ExportSet::Files(other_leaf)) => {
            base_leaf.files.extend(other_leaf.files);
            if base_leaf.schema.fields.is_empty() && !other_leaf.schema.fields.is_empty() {
                base_leaf.schema = other_leaf.schema;
            }
            ExportSet::Files(base_leaf)
        }
        (ExportSet::Map(mut base_map), ExportSet::Map(other_map)) => {
            for (key, other_set) in other_map {
                _ = base_map
                    .entry(key)
                    .and_modify(|existing| {
                        let merged = merge_export_sets(*existing.clone(), *other_set.clone());
                        **existing = merged;
                    })
                    .or_insert(other_set);
            }
            ExportSet::Map(base_map)
        }
        // Incompatible variants -- prefer the non-empty one
        (base, _) => base,
    }
}

// ---------------------------------------------------------------------------
// Export a series file to Hive-partitioned parquet
// ---------------------------------------------------------------------------

/// Export a queryable pond file (series/table) to Hive-partitioned parquet.
///
/// This is the shared core of `pond export` -- registers the file as a
/// DataFusion table, runs `COPY TO ... PARTITIONED BY`, scans the output
/// directory, and extracts temporal bounds from the Hive partition paths.
///
/// Returns `(Vec<(captures, ExportOutput)>, TemplateSchema)`.
pub async fn export_series_to_parquet(
    root: &tinyfs::WD,
    pond_path: &str,
    export_dir: &Path,
    temporal_parts: &[String],
    captures: &[String],
    base_output_dir: &Path,
    provider_ctx: &tinyfs::ProviderContext,
) -> Result<(Vec<(Vec<String>, ExportOutput)>, TemplateSchema)> {
    log::debug!(
        "export_series_to_parquet: pond_path={}, export_dir={}",
        pond_path,
        export_dir.display()
    );

    // Register the series as a DataFusion table
    let (_, lookup) = root
        .resolve_path(pond_path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to resolve path '{}': {}", pond_path, e))?;

    let node_path = match lookup {
        tinyfs::Lookup::Found(np) => np,
        _ => return Err(anyhow::anyhow!("Path not found: {}", pond_path)),
    };

    let file_handle = node_path
        .as_file()
        .await
        .map_err(|e| anyhow::anyhow!("'{}' is not a file: {}", pond_path, e))?;

    let file_arc = file_handle.handle.get_file().await;
    let file_guard = file_arc.lock().await;

    let queryable = file_guard
        .as_queryable()
        .ok_or_else(|| anyhow::anyhow!("'{}' is not a queryable file", pond_path))?;

    let node_id = node_path.id();
    let table_provider = queryable
        .as_table_provider(node_id, provider_ctx)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get table provider: {}", e))?;

    drop(file_guard);

    // An incremental reduction provider publishes a hint while building the
    // table provider above; it lets the export reuse partitions whose buckets
    // did not change. Absent (non-reduction sources), the export does a full
    // deterministic rewrite.
    let hint = provider_ctx.get_export_hint(&node_id);

    export_table_provider_to_parquet(
        table_provider,
        pond_path,
        export_dir,
        temporal_parts,
        captures,
        base_output_dir,
        provider_ctx,
        "timestamp",
        hint.as_ref(),
    )
    .await
}

/// Export an already-resolved TableProvider to Hive-partitioned Parquet.
///
/// This is the core export function. `export_series_to_parquet` resolves a
/// pond path to a TableProvider and calls this. Format-provider-based exports
/// (e.g., `jsonlogs:///`) can construct a TableProvider via
/// `Provider::create_provider_for_url()` and call this directly.
///
/// `timestamp_column` names the column used for temporal partitioning SQL
/// (e.g., `date_part('year', <timestamp_column>) as year`).
pub async fn export_table_provider_to_parquet(
    table_provider: std::sync::Arc<dyn datafusion::catalog::TableProvider>,
    source_label: &str,
    export_dir: &Path,
    temporal_parts: &[String],
    captures: &[String],
    base_output_dir: &Path,
    provider_ctx: &tinyfs::ProviderContext,
    timestamp_column: &str,
    hint: Option<&tinyfs::ExportHint>,
) -> Result<(Vec<(Vec<String>, ExportOutput)>, TemplateSchema)> {
    log::debug!(
        "export_table_provider_to_parquet: source={}, export_dir={}, ts_col={}",
        source_label,
        export_dir.display(),
        timestamp_column,
    );

    let manifest_path = export_dir.join(MANIFEST_FILE);
    let seed_manifest = read_series_manifest(&manifest_path)?;

    // Whole-series reuse: the reduction provider reports the merged output is
    // byte-identical to what produced the seeded partitions, so nothing is
    // rewritten. Every seeded partition is verified against its recorded digest
    // first -- a mismatch is a hard failure, never a silent serve of wrong data.
    if let (Some(h), Some(seed)) = (hint, seed_manifest.as_ref())
        && seed.digest.as_deref() == Some(h.digest.as_str())
        && !seed.partitions.is_empty()
    {
        let mut results = Vec::new();
        for p in &seed.partitions {
            let abs = base_output_dir.join(&p.file);
            verify_partition_digest(&abs, &p.digest, source_label)?;
            results.push((
                captures.to_vec(),
                ExportOutput {
                    file: p.file.clone(),
                    start_time: p.start_time,
                    end_time: p.end_time,
                },
            ));
        }
        let schema = read_parquet_schema(export_dir)?;
        log::debug!(
            "export reuse: {} unchanged partitions for {}",
            results.len(),
            source_label
        );
        return Ok((results, schema));
    }

    // Reconcile mode reuses partitions strictly before the changed-bucket
    // watermark; it requires both a seed manifest and a Some(lo) watermark.
    // Otherwise the output is fully rebuilt with deterministic file names.
    let reuse_before: Option<i64> = match (hint, seed_manifest.as_ref()) {
        (Some(h), Some(_)) => h.changed_since,
        _ => None,
    };
    let can_reuse = reuse_before.is_some() && seed_manifest.is_some();

    let seed_by_file: HashMap<PathBuf, &ManifestPartition> = seed_manifest
        .as_ref()
        .map(|m| m.partitions.iter().map(|p| (p.file.clone(), p)).collect())
        .unwrap_or_default();

    if !can_reuse {
        // Full rewrite: clear the dir so no stale partitions or prior
        // UUID-named COPY output remain. An export is a full regenerate from
        // source, so prior outputs are unconditionally stale.
        if export_dir.exists() {
            std::fs::remove_dir_all(export_dir).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to clear stale export dir '{}': {}",
                    export_dir.display(),
                    e
                )
            })?;
        }
    }
    std::fs::create_dir_all(export_dir)?;

    let ctx = &provider_ctx.datafusion_session;
    let unique_table_name = format!(
        "series_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("ok")
            .as_nanos()
    );
    _ = ctx
        .register_table(
            datafusion::sql::TableReference::bare(unique_table_name.as_str()),
            table_provider,
        )
        .map_err(|e| anyhow::anyhow!("Failed to register table: {}", e))?;

    let export_dir_rel = export_dir
        .strip_prefix(base_output_dir)
        .map_err(|e| anyhow::anyhow!("export_dir not under base_output_dir: {}", e))?
        .to_path_buf();

    // Full-rewrite fast path: no prior output is reusable, so write every
    // partition in a single `COPY ... PARTITIONED BY` (one source scan) rather
    // than rescanning the source once per partition. The per-partition loop
    // below is reserved for the small changed-partition tail of an incremental
    // reconcile, where most partitions are reused and only a few are rewritten.
    if !can_reuse {
        let (results, manifest) = full_rewrite_partitioned(
            ctx,
            &unique_table_name,
            temporal_parts,
            timestamp_column,
            export_dir,
            base_output_dir,
            captures,
            source_label,
            hint,
        )
        .await?;

        _ = ctx.deregister_table(datafusion::sql::TableReference::bare(
            unique_table_name.as_str(),
        ));
        write_series_manifest(&manifest_path, &manifest)?;

        if results.is_empty() {
            return Err(anyhow::anyhow!(
                "No files were exported for: {}",
                source_label
            ));
        }
        let schema = read_parquet_schema(export_dir)?;
        return Ok((results, schema));
    }

    let partitions = distinct_partitions(
        ctx,
        &unique_table_name,
        temporal_parts,
        timestamp_column,
        source_label,
    )
    .await?;

    let mut manifest = SeriesExportManifest {
        digest: hint.map(|h| h.digest.clone()),
        partitions: Vec::new(),
    };
    let mut results: Vec<(Vec<String>, ExportOutput)> = Vec::new();
    let mut kept: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
    let mut reused = 0usize;
    let mut written = 0usize;

    for part in &partitions {
        let rel_dir = partition_rel_dir(part);
        let file_rel = export_dir_rel.join(&rel_dir).join("data.parquet");
        let abs_file = base_output_dir.join(&file_rel);
        let (start_time, end_time) = extract_timestamps_from_path(&file_rel)?;

        // A partition is unchanged iff its whole time window ends at or before
        // the changed-bucket watermark, so no changed bucket falls inside it.
        let reusable =
            can_reuse && matches!((reuse_before, end_time), (Some(lo), Some(end)) if end <= lo);

        if reusable
            && let Some(seed_part) = seed_by_file.get(&file_rel)
            && abs_file.exists()
            && verify_partition_digest(&abs_file, &seed_part.digest, source_label).is_ok()
        {
            manifest.partitions.push((*seed_part).clone());
            results.push((
                captures.to_vec(),
                ExportOutput {
                    file: file_rel.clone(),
                    start_time: seed_part.start_time,
                    end_time: seed_part.end_time,
                },
            ));
            _ = kept.insert(file_rel);
            reused += 1;
            continue;
        }

        let digest = export_one_partition(
            ctx,
            &unique_table_name,
            timestamp_column,
            part,
            &abs_file,
            source_label,
        )
        .await?;
        manifest.partitions.push(ManifestPartition {
            file: file_rel.clone(),
            digest,
            start_time,
            end_time,
        });
        results.push((
            captures.to_vec(),
            ExportOutput {
                file: file_rel.clone(),
                start_time,
                end_time,
            },
        ));
        _ = kept.insert(file_rel);
        written += 1;
    }

    // Drop any seeded partition file the current data no longer produces.
    if can_reuse {
        for p in seed_manifest.iter().flat_map(|m| &m.partitions) {
            if !kept.contains(&p.file) {
                let abs = base_output_dir.join(&p.file);
                if abs.exists() {
                    std::fs::remove_file(&abs).map_err(|e| {
                        anyhow::anyhow!(
                            "Failed to remove stale partition '{}': {}",
                            abs.display(),
                            e
                        )
                    })?;
                }
            }
        }
    }

    _ = ctx.deregister_table(datafusion::sql::TableReference::bare(
        unique_table_name.as_str(),
    ));

    write_series_manifest(&manifest_path, &manifest)?;

    log::debug!(
        "export {}: {} partitions ({} reused, {} written)",
        source_label,
        results.len(),
        reused,
        written
    );

    let schema = read_parquet_schema(export_dir)?;

    if results.is_empty() {
        return Err(anyhow::anyhow!(
            "No files were exported for: {}",
            source_label
        ));
    }

    Ok((results, schema))
}

/// Build the Hive-style relative directory for a partition tuple, e.g.
/// `[(year,2025),(month,7)]` -> `year=2025/month=7`. An empty tuple yields an
/// empty path, so the single partition file lands directly in the export dir.
fn partition_rel_dir(part: &[(String, i64)]) -> PathBuf {
    let mut dir = PathBuf::new();
    for (name, value) in part {
        dir.push(format!("{}={}", name, value));
    }
    dir
}

/// Enumerate the distinct temporal-partition tuples present in the source,
/// ordered chronologically. A null timestamp is a hard failure: it would map to
/// an invalid `year=0` partition, so the caller must never emit one.
async fn distinct_partitions(
    ctx: &datafusion::prelude::SessionContext,
    table: &str,
    temporal_parts: &[String],
    timestamp_column: &str,
    source_label: &str,
) -> Result<Vec<Vec<(String, i64)>>> {
    if temporal_parts.is_empty() {
        return Ok(vec![vec![]]);
    }

    let cols = temporal_parts
        .iter()
        .map(|p| {
            format!(
                "CAST(date_part('{}', \"{}\") AS BIGINT) AS {}",
                p, timestamp_column, p
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let order = (1..=temporal_parts.len())
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT DISTINCT {cols} FROM \"{table}\" ORDER BY {order}");

    let batches = ctx
        .sql(&sql)
        .await
        .map_err(|e| anyhow::anyhow!("partition enumeration failed for '{}': {}", source_label, e))?
        .collect()
        .await
        .map_err(|e| {
            anyhow::anyhow!("partition enumeration failed for '{}': {}", source_label, e)
        })?;

    let mut out = Vec::new();
    for batch in &batches {
        use arrow::array::Array;
        let arrays: Vec<&arrow::array::Int64Array> = (0..temporal_parts.len())
            .map(|i| {
                batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "partition column {} is not Int64 for '{}'",
                            i,
                            source_label
                        )
                    })
            })
            .collect::<Result<_>>()?;
        for row in 0..batch.num_rows() {
            let mut tuple = Vec::with_capacity(temporal_parts.len());
            for (i, name) in temporal_parts.iter().enumerate() {
                if arrays[i].is_null(row) {
                    return Err(anyhow::anyhow!(
                        "Null timestamp partition value for '{}' in '{}'. \
                         This indicates nullable timestamp data.",
                        name,
                        source_label
                    ));
                }
                tuple.push((name.clone(), arrays[i].value(row)));
            }
            out.push(tuple);
        }
    }
    Ok(out)
}

/// Export a single partition to a deterministic file via a temp-then-rename so
/// a hardlinked seed file (from `build-<ts>/` seeded off `current/`) is never
/// modified in place. Returns the blake3 digest of the written file.
async fn export_one_partition(
    ctx: &datafusion::prelude::SessionContext,
    table: &str,
    timestamp_column: &str,
    part: &[(String, i64)],
    abs_file: &Path,
    source_label: &str,
) -> Result<String> {
    if let Some(parent) = abs_file.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = abs_file.with_extension(format!("parquet.tmp-{}", std::process::id()));

    let mut where_clause = String::new();
    for (i, (name, value)) in part.iter().enumerate() {
        if i > 0 {
            where_clause.push_str(" AND ");
        }
        where_clause.push_str(&format!(
            "CAST(date_part('{}', \"{}\") AS BIGINT) = {}",
            name, timestamp_column, value
        ));
    }
    let where_sql = if where_clause.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_clause)
    };
    let copy_sql = format!(
        "COPY (SELECT * FROM \"{table}\"{where_sql} ORDER BY \"{ts}\") TO '{out}' STORED AS PARQUET",
        ts = timestamp_column,
        out = tmp.to_string_lossy()
    );
    log::debug!("export partition SQL: {}", copy_sql);
    let df = ctx
        .sql(&copy_sql)
        .await
        .map_err(|e| anyhow::anyhow!("COPY failed for '{}': {}", source_label, e))?;
    let _ = df
        .collect()
        .await
        .map_err(|e| anyhow::anyhow!("COPY stream failed for '{}': {}", source_label, e))?;

    std::fs::rename(&tmp, abs_file).map_err(|e| {
        anyhow::anyhow!(
            "Failed to publish partition '{}': {}",
            abs_file.display(),
            e
        )
    })?;
    Ok(crate::version_cache::file_blake3(abs_file)?)
}

/// Full-rewrite export via `COPY ... PARTITIONED BY` (one source scan per chunk).
///
/// This is the fast path taken when no prior output can be reused (first build,
/// missing seed manifest, or an unbounded `changed_since`). It writes the
/// partitions in as few DataFusion passes as the [`MAX_OPEN_PARTITIONS`] writer
/// budget allows -- as opposed to `export_one_partition`, which rescans the
/// source once per partition and is only appropriate for the small
/// changed-partition tail of an incremental reconcile. DataFusion emits one
/// UUID-named parquet per Hive partition; each is renamed to the deterministic
/// `data.parquet` so a later build can reuse it by path, and its blake3 digest
/// is recorded in the returned manifest.
///
/// Chunking does not change the output: each chunk selects a disjoint set of
/// partitions, so every partition is written exactly once, by exactly one
/// statement, from the same rows it would have received from a single COPY.
async fn full_rewrite_partitioned(
    ctx: &datafusion::prelude::SessionContext,
    table: &str,
    temporal_parts: &[String],
    timestamp_column: &str,
    export_dir: &Path,
    base_output_dir: &Path,
    captures: &[String],
    source_label: &str,
    hint: Option<&tinyfs::ExportHint>,
) -> Result<(Vec<(Vec<String>, ExportOutput)>, SeriesExportManifest)> {
    // Enumerate the partitions up front so the rewrite can be split when there
    // are more of them than one statement should hold writers for.
    let partitions = if temporal_parts.is_empty() {
        Vec::new()
    } else {
        distinct_partitions(ctx, table, temporal_parts, timestamp_column, source_label).await?
    };

    if partitions.len() > MAX_OPEN_PARTITIONS {
        let chunks: Vec<&[Vec<(String, i64)>]> = partitions.chunks(MAX_OPEN_PARTITIONS).collect();
        log::debug!(
            "full-rewrite export: {} partitions for '{}' exceed the {}-writer budget; \
             rewriting in {} chunks",
            partitions.len(),
            source_label,
            MAX_OPEN_PARTITIONS,
            chunks.len()
        );
        for (i, chunk) in chunks.iter().enumerate() {
            // Each chunk writes to its own directory and is then moved into
            // place. Writing every chunk straight into `export_dir` would rely
            // on DataFusion appending rather than replacing the directory
            // contents; staging keeps the result independent of that.
            let chunk_dir = export_dir.join(format!(".chunk-{i}"));
            if chunk_dir.exists() {
                std::fs::remove_dir_all(&chunk_dir)?;
            }
            std::fs::create_dir_all(&chunk_dir)?;
            run_partitioned_copy(
                ctx,
                table,
                temporal_parts,
                timestamp_column,
                &chunk_dir,
                Some(chunk),
                source_label,
            )
            .await?;
            merge_chunk_dir(&chunk_dir, export_dir, source_label)?;
        }
    } else {
        run_partitioned_copy(
            ctx,
            table,
            temporal_parts,
            timestamp_column,
            export_dir,
            None,
            source_label,
        )
        .await?;
    }

    // DataFusion wrote one UUID-named parquet per Hive partition. Rename each to
    // the deterministic `data.parquet` and record its digest + temporal bounds.
    let produced = discover_exported_files(export_dir, base_output_dir)?;
    let mut manifest = SeriesExportManifest {
        digest: hint.map(|h| h.digest.clone()),
        partitions: Vec::new(),
    };
    let mut results: Vec<(Vec<String>, ExportOutput)> = Vec::new();
    for out in produced {
        let src_abs = base_output_dir.join(&out.file);
        let file_rel = out
            .file
            .parent()
            .map(|d| d.join("data.parquet"))
            .unwrap_or_else(|| PathBuf::from("data.parquet"));
        let dst_abs = base_output_dir.join(&file_rel);
        if src_abs != dst_abs {
            if dst_abs.exists() {
                return Err(anyhow::anyhow!(
                    "Partition '{}' produced more than one parquet file for '{}'; \
                     deterministic naming requires exactly one file per partition.",
                    file_rel.display(),
                    source_label
                ));
            }
            std::fs::rename(&src_abs, &dst_abs).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to normalize partition '{}': {}",
                    dst_abs.display(),
                    e
                )
            })?;
        }
        let digest = crate::version_cache::file_blake3(&dst_abs)?;
        manifest.partitions.push(ManifestPartition {
            file: file_rel.clone(),
            digest,
            start_time: out.start_time,
            end_time: out.end_time,
        });
        results.push((
            captures.to_vec(),
            ExportOutput {
                file: file_rel,
                start_time: out.start_time,
                end_time: out.end_time,
            },
        ));
    }
    results.sort_by(|a, b| a.1.file.cmp(&b.1.file));
    manifest.partitions.sort_by(|a, b| a.file.cmp(&b.file));
    Ok((results, manifest))
}

/// Build the predicate selecting exactly the partitions in `chunk`.
///
/// Each partition contributes one conjunction of `date_part(...) = value`
/// equalities -- the same form `export_one_partition` uses for a single
/// partition -- and the chunk is their disjunction. Chunks partition the
/// enumerated tuples, so the predicates are mutually exclusive and jointly
/// cover every partition.
fn partition_chunk_predicate(chunk: &[Vec<(String, i64)>], timestamp_column: &str) -> String {
    chunk
        .iter()
        .map(|part| {
            let conj = part
                .iter()
                .map(|(name, value)| {
                    format!(
                        "CAST(date_part('{}', \"{}\") AS BIGINT) = {}",
                        name, timestamp_column, value
                    )
                })
                .collect::<Vec<_>>()
                .join(" AND ");
            format!("({conj})")
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

/// Run one `COPY ... PARTITIONED BY` into `target_dir`.
///
/// `chunk` restricts the statement to a subset of partitions; `None` writes the
/// whole source in one pass.
async fn run_partitioned_copy(
    ctx: &datafusion::prelude::SessionContext,
    table: &str,
    temporal_parts: &[String],
    timestamp_column: &str,
    target_dir: &Path,
    chunk: Option<&[Vec<(String, i64)>]>,
    source_label: &str,
) -> Result<()> {
    let temporal_columns = temporal_parts
        .iter()
        .map(|p| {
            format!(
                "CAST(date_part('{}', \"{}\") AS BIGINT) AS {}",
                p, timestamp_column, p
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let where_sql = match chunk {
        Some(c) => format!(" WHERE {}", partition_chunk_predicate(c, timestamp_column)),
        None => String::new(),
    };
    let select = if temporal_parts.is_empty() {
        format!("SELECT * FROM \"{table}\"{where_sql} ORDER BY \"{timestamp_column}\"")
    } else {
        format!(
            "SELECT *, {temporal_columns} FROM \"{table}\"{where_sql} ORDER BY \"{timestamp_column}\""
        )
    };
    let mut copy_sql = format!(
        "COPY ({select}) TO '{}' STORED AS PARQUET",
        target_dir.to_string_lossy()
    );
    if !temporal_parts.is_empty() {
        copy_sql.push_str(&format!(" PARTITIONED BY ({})", temporal_parts.join(", ")));
    }
    log::debug!("full-rewrite export SQL: {}", copy_sql);
    let df = ctx
        .sql(&copy_sql)
        .await
        .map_err(|e| anyhow::anyhow!("COPY failed for '{}': {}", source_label, e))?;
    _ = df
        .collect()
        .await
        .map_err(|e| anyhow::anyhow!("COPY stream failed for '{}': {}", source_label, e))?;
    Ok(())
}

/// Move a chunk's partition tree into the export directory and remove the
/// staging directory.
///
/// Chunks write disjoint partitions, so no destination file can already exist.
/// A collision means two chunks claimed the same partition -- a correctness bug
/// in the chunking, not a condition to resolve by overwriting -- so it is a hard
/// failure.
fn merge_chunk_dir(chunk_dir: &Path, export_dir: &Path, source_label: &str) -> Result<()> {
    fn move_tree(dir: &Path, chunk_root: &Path, export_dir: &Path, label: &str) -> Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() {
                move_tree(&path, chunk_root, export_dir, label)?;
                continue;
            }
            let rel = path
                .strip_prefix(chunk_root)
                .map_err(|e| anyhow::anyhow!("chunk path outside its staging dir: {}", e))?;
            let dst = export_dir.join(rel);
            if dst.exists() {
                return Err(anyhow::anyhow!(
                    "Chunked export produced partition '{}' twice for '{}'; \
                     chunks must cover disjoint partitions.",
                    rel.display(),
                    label
                ));
            }
            if let Some(parent) = dst.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::rename(&path, &dst).map_err(|e| {
                anyhow::anyhow!("Failed to publish chunk file '{}': {}", dst.display(), e)
            })?;
        }
        Ok(())
    }

    move_tree(chunk_dir, chunk_dir, export_dir, source_label)?;
    std::fs::remove_dir_all(chunk_dir)?;
    Ok(())
}

/// Verify a partition file's bytes match a recorded digest, hard-failing on a
/// mismatch so a corrupted or externally modified partition is never reused.
fn verify_partition_digest(abs_file: &Path, expected: &str, source_label: &str) -> Result<()> {
    let actual = crate::version_cache::file_blake3(abs_file)?;
    if actual != expected {
        return Err(anyhow::anyhow!(
            "Export partition '{}' for '{}' does not match its recorded digest \
             (expected {}, found {}); the export output is corrupt or was modified \
             without the pond's knowledge. Re-run with a clean build dir to rebuild it.",
            abs_file.display(),
            source_label,
            expected,
            actual
        ));
    }
    Ok(())
}

/// Read a per-series export manifest, if present. A present-but-unparseable
/// manifest is a hard failure rather than a silent full rebuild.
fn read_series_manifest(path: &Path) -> Result<Option<SeriesExportManifest>> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let m: SeriesExportManifest = serde_json::from_slice(&bytes).map_err(|e| {
                anyhow::anyhow!(
                    "Export manifest '{}' is corrupt or unparseable: {}. \
                     Re-run with a clean build dir to rebuild it.",
                    path.display(),
                    e
                )
            })?;
            Ok(Some(m))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Write a per-series export manifest atomically via temp-then-rename.
fn write_series_manifest(path: &Path, manifest: &SeriesExportManifest) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension(format!("json.tmp-{}", std::process::id()));
    let bytes = serde_json::to_vec_pretty(manifest)?;
    std::fs::write(&tmp, &bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Output scanning
// ---------------------------------------------------------------------------

/// Discover all parquet files in the export directory, returning relative paths
/// from `base_path` with temporal bounds extracted from Hive partition dirs.
pub fn discover_exported_files(export_path: &Path, base_path: &Path) -> Result<Vec<ExportOutput>> {
    let mut files = Vec::new();

    fn collect_parquet_files(
        dir: &Path,
        base_path: &Path,
        files: &mut Vec<ExportOutput>,
    ) -> Result<()> {
        if !dir.exists() {
            return Ok(());
        }

        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                collect_parquet_files(&path, base_path, files)?;
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                let relative_path = path
                    .strip_prefix(base_path)
                    .map_err(|e| anyhow::anyhow!("Failed to compute relative path: {}", e))?
                    .to_path_buf();

                // Reject invalid partitions
                let path_str = relative_path.to_string_lossy();
                if path_str.contains("year=0") || path_str.contains("month=0") {
                    return Err(anyhow::anyhow!(
                        "Invalid temporal partition detected: '{}'. \
                        This indicates nullable timestamp data.",
                        relative_path.display()
                    ));
                }

                let (start_time, end_time) = extract_timestamps_from_path(&relative_path)?;

                files.push(ExportOutput {
                    file: relative_path,
                    start_time,
                    end_time,
                });
            }
        }
        Ok(())
    }

    collect_parquet_files(export_path, base_path, &mut files)?;
    files.sort_by(|a, b| a.file.cmp(&b.file));
    Ok(files)
}

// ---------------------------------------------------------------------------
// Temporal bounds extraction
// ---------------------------------------------------------------------------

/// Extract start/end timestamps from Hive-style partition path components.
///
/// e.g. `Temperature/res=1h/year=2025/month=7/data.parquet`
/// -> `(Some(1751328000), Some(1753920000))`  (2025-07-01 -> 2025-08-01)
pub fn extract_timestamps_from_path(relative_path: &Path) -> Result<(Option<i64>, Option<i64>)> {
    let components = relative_path.components();
    let mut temporal_parts = HashMap::new();
    let mut parsed_parts = Vec::new();

    for component in components {
        if let Some(dir_name) = component.as_os_str().to_str()
            && dir_name.contains('=')
        {
            let parts: Vec<&str> = dir_name.split('=').collect();
            if parts.len() == 2 {
                let part_name = parts[0];
                let part_value_str = parts[1];

                if ![
                    "year", "quarter", "month", "day", "hour", "minute", "second",
                ]
                .contains(&part_name)
                {
                    continue;
                }

                let part_value = part_value_str.parse::<i32>().map_err(|e| {
                    anyhow::anyhow!(
                        "Invalid temporal partition value '{}={}' in '{}': {}",
                        part_name,
                        part_value_str,
                        relative_path.display(),
                        e
                    )
                })?;

                _ = temporal_parts.insert(part_name, part_value);
                parsed_parts.push(part_name);
            }
        }
    }

    if parsed_parts.is_empty() {
        return Ok((None, None));
    }

    // Fill defaults for missing temporal parts.
    // Quarter -> derive month from quarter value (start of quarter).
    if temporal_parts.contains_key("quarter") && !temporal_parts.contains_key("month") {
        let q = *temporal_parts.get("quarter").expect("checked");
        let start_month = (q - 1) * 3 + 1; // Q1->1, Q2->4, Q3->7, Q4->10
        _ = temporal_parts.insert("month", start_month);
    }
    if !temporal_parts.contains_key("month") {
        _ = temporal_parts.insert("month", 1);
    }
    if !temporal_parts.contains_key("day") {
        _ = temporal_parts.insert("day", 1);
    }
    if !temporal_parts.contains_key("hour") {
        _ = temporal_parts.insert("hour", 0);
    }
    if !temporal_parts.contains_key("minute") {
        _ = temporal_parts.insert("minute", 0);
    }
    if !temporal_parts.contains_key("second") {
        _ = temporal_parts.insert("second", 0);
    }

    let start_time = build_utc_timestamp(&temporal_parts)?;

    if let Some(last_part) = parsed_parts.last() {
        let end_time = calculate_end_time(&temporal_parts, last_part)?;
        Ok((Some(start_time), Some(end_time)))
    } else {
        Ok((Some(start_time), None))
    }
}

/// Build UTC epoch seconds from temporal part values.
pub fn build_utc_timestamp(parts: &HashMap<&str, i32>) -> Result<i64> {
    use chrono::{TimeZone, Utc};

    let year = *parts
        .get("year")
        .ok_or_else(|| anyhow::anyhow!("Missing year"))?;
    let month = *parts.get("month").unwrap_or(&1) as u32;
    let day = *parts.get("day").unwrap_or(&1) as u32;
    let hour = *parts.get("hour").unwrap_or(&0) as u32;
    let minute = *parts.get("minute").unwrap_or(&0) as u32;
    let second = *parts.get("second").unwrap_or(&0) as u32;

    if year <= 0 {
        return Err(anyhow::anyhow!("Invalid year {}", year));
    }
    if !(1..=12).contains(&month) {
        return Err(anyhow::anyhow!("Invalid month {}", month));
    }

    Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()
        .map(|dt| dt.timestamp())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Invalid date: {}-{:02}-{:02} {:02}:{:02}:{:02}",
                year,
                month,
                day,
                hour,
                minute,
                second
            )
        })
}

/// Calculate end time by incrementing the finest temporal granularity.
pub fn calculate_end_time(parts: &HashMap<&str, i32>, last_part: &str) -> Result<i64> {
    use chrono::{Datelike, TimeZone, Utc};

    let year = *parts
        .get("year")
        .ok_or_else(|| anyhow::anyhow!("Missing year"))?;
    let month = *parts.get("month").unwrap_or(&1) as u32;
    let day = *parts.get("day").unwrap_or(&1) as u32;
    let hour = *parts.get("hour").unwrap_or(&0) as u32;
    let minute = *parts.get("minute").unwrap_or(&0) as u32;
    let second = *parts.get("second").unwrap_or(&0) as u32;

    if year <= 0 {
        return Err(anyhow::anyhow!("Invalid year {}", year));
    }

    let start_dt = Utc
        .with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Invalid date: {}-{:02}-{:02} {:02}:{:02}:{:02}",
                year,
                month,
                day,
                hour,
                minute,
                second
            )
        })?;

    let end_dt = match last_part {
        "year" => start_dt
            .with_year(start_dt.year() + 1)
            .ok_or_else(|| anyhow::anyhow!("Failed to add 1 year"))?,
        "quarter" => {
            // Advance by 3 months from the start-of-quarter.
            let new_month = start_dt.month() + 3;
            if new_month > 12 {
                start_dt
                    .with_year(start_dt.year() + 1)
                    .and_then(|dt| dt.with_month(new_month - 12))
                    .ok_or_else(|| anyhow::anyhow!("Failed to advance quarter past December"))?
            } else {
                start_dt
                    .with_month(new_month)
                    .ok_or_else(|| anyhow::anyhow!("Failed to advance quarter"))?
            }
        }
        "month" => {
            if start_dt.month() == 12 {
                start_dt
                    .with_year(start_dt.year() + 1)
                    .and_then(|dt| dt.with_month(1))
                    .ok_or_else(|| anyhow::anyhow!("Failed to roll over year from December"))?
            } else {
                start_dt
                    .with_month(start_dt.month() + 1)
                    .ok_or_else(|| anyhow::anyhow!("Failed to add 1 month"))?
            }
        }
        "day" => start_dt + chrono::Duration::days(1),
        "hour" => start_dt + chrono::Duration::hours(1),
        "minute" => start_dt + chrono::Duration::minutes(1),
        "second" => start_dt + chrono::Duration::seconds(1),
        _ => return Err(anyhow::anyhow!("Unknown temporal part: {}", last_part)),
    };

    Ok(end_dt.timestamp())
}

// ---------------------------------------------------------------------------
// Schema extraction
// ---------------------------------------------------------------------------

/// Read the Arrow schema from the first parquet file in a directory and
/// transform it into `TemplateSchema`.
pub fn read_parquet_schema(export_dir: &Path) -> Result<TemplateSchema> {
    let first_parquet = find_first_parquet_file(export_dir)?;

    let file = std::fs::File::open(&first_parquet).map_err(|e| {
        anyhow::anyhow!(
            "Failed to open parquet file {}: {}",
            first_parquet.display(),
            e
        )
    })?;

    let reader_builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        file,
    )
    .map_err(|e| {
        anyhow::anyhow!(
            "Failed to read parquet schema from {}: {}",
            first_parquet.display(),
            e
        )
    })?;

    let arrow_schema = reader_builder.schema();
    transform_arrow_to_template_schema(arrow_schema)
}

fn find_first_parquet_file(dir: &Path) -> Result<PathBuf> {
    fn find_recursive(dir: &Path) -> Option<PathBuf> {
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .ok()?
            .filter_map(|e| e.ok())
            .collect();
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let path = entry.path();
            if path.is_file() && path.extension().is_some_and(|ext| ext == "parquet") {
                return Some(path);
            } else if path.is_dir()
                && let Some(found) = find_recursive(&path)
            {
                return Some(found);
            }
        }
        None
    }

    find_recursive(dir)
        .ok_or_else(|| anyhow::anyhow!("No parquet files found in {}", dir.display()))
}

/// Transform Arrow schema to template schema format.
pub fn transform_arrow_to_template_schema(
    arrow_schema: &arrow::datatypes::Schema,
) -> Result<TemplateSchema> {
    let mut fields = Vec::new();

    for field in arrow_schema.fields() {
        let field_name_lower = field.name().to_lowercase();
        if matches!(field_name_lower.as_str(), "timestamp" | "rtimestamp") {
            continue;
        }

        let template_field = parse_field_name(field.name())?;
        fields.push(template_field);
    }

    Ok(TemplateSchema { fields })
}

/// Parse field name into components.
///
/// Supported formats (most specific to least):
///   4+ parts: `"instrument.name.unit.agg"` -> full decomposition
///   3 parts:  `"name.unit.agg"` -> no instrument
///   2 parts:  `"name.agg"` -> no instrument, no unit
///   1 part:   `"name"` -> bare field name (no aggregation)
pub fn parse_field_name(field_name: &str) -> Result<TemplateField> {
    if field_name == "timestamp.count" {
        return Ok(TemplateField {
            instrument: "system".to_string(),
            name: "timestamp".to_string(),
            unit: "count".to_string(),
            agg: "count".to_string(),
        });
    }

    let mut parts: Vec<&str> = field_name.split('.').collect();

    match parts.len() {
        0 => Err(anyhow::anyhow!("field name: empty field name")),
        1 => {
            // Bare field name, no aggregation
            Ok(TemplateField {
                instrument: String::new(),
                name: parts[0].to_string(),
                unit: String::new(),
                agg: String::new(),
            })
        }
        2 => {
            // name.agg (e.g., "septicstation_temperature.avg")
            let agg = parts.pop().expect("ok").to_string();
            let name = parts.pop().expect("ok").to_string();
            Ok(TemplateField {
                instrument: String::new(),
                name,
                unit: String::new(),
                agg,
            })
        }
        3 => {
            // name.unit.agg (e.g., "temperature.C.avg")
            let agg = parts.pop().expect("ok").to_string();
            let unit = parts.pop().expect("ok").to_string();
            let name = parts.pop().expect("ok").to_string();
            Ok(TemplateField {
                instrument: String::new(),
                name,
                unit,
                agg,
            })
        }
        _ => {
            // instrument.name.unit.agg (e.g., "TempProbe.temperature.C.avg")
            let agg = parts.pop().expect("ok").to_string();
            let unit = parts.pop().expect("ok").to_string();
            let name = parts.pop().expect("ok").to_string();
            let instrument = parts.join(".");
            Ok(TemplateField {
                instrument,
                name,
                unit,
                agg,
            })
        }
    }
}

/// Count files in an ExportSet.
#[must_use]
pub fn count_export_set_files(export_set: &ExportSet) -> usize {
    match export_set {
        ExportSet::Empty => 0,
        ExportSet::Files(leaf) => leaf.files.len(),
        ExportSet::Map(map) => map.values().map(|v| count_export_set_files(v)).sum(),
    }
}

/// Helper to print export set structure for debugging.
pub fn print_export_set(export_set: &ExportSet, indent: &str) {
    match export_set {
        ExportSet::Empty => {
            log::debug!("{}(no files)", indent);
        }
        ExportSet::Files(leaf) => {
            log::debug!("{}[FILE] {} exported files:", indent, leaf.files.len());
            for file_output in &leaf.files {
                let start_str = file_output
                    .start_time
                    .and_then(|s| chrono::DateTime::from_timestamp(s, 0))
                    .map(|dt| format!("{}", dt.format("%Y-%m-%d %H:%M:%S")))
                    .unwrap_or_else(|| "N/A".to_string());
                let end_str = file_output
                    .end_time
                    .and_then(|s| chrono::DateTime::from_timestamp(s, 0))
                    .map(|dt| format!("{}", dt.format("%Y-%m-%d %H:%M:%S")))
                    .unwrap_or_else(|| "N/A".to_string());
                log::debug!(
                    "{}  [FILE] {} ([TIME] {} -> {})",
                    indent,
                    file_output.file.display(),
                    start_str,
                    end_str
                );
            }
        }
        ExportSet::Map(map) => {
            log::debug!("{}[IDX]  {} capture groups:", indent, map.len());
            for (key, nested_set) in map {
                log::debug!("{}  [DIR] '{}' wildcard capture:", indent, key);
                print_export_set(nested_set, &format!("{}    ", indent));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_extract_timestamps_year_month() {
        let path = PathBuf::from("Temperature/res=1h/year=2025/month=7/data.parquet");
        let (start, end) = extract_timestamps_from_path(&path).unwrap();

        let expected_start = chrono::Utc
            .with_ymd_and_hms(2025, 7, 1, 0, 0, 0)
            .unwrap()
            .timestamp();
        let expected_end = chrono::Utc
            .with_ymd_and_hms(2025, 8, 1, 0, 0, 0)
            .unwrap()
            .timestamp();

        assert_eq!(start, Some(expected_start));
        assert_eq!(end, Some(expected_end));
    }

    #[test]
    fn test_extract_timestamps_no_temporal() {
        let path = PathBuf::from("Temperature/res=1h/data.parquet");
        let (start, end) = extract_timestamps_from_path(&path).unwrap();
        assert_eq!(start, None);
        assert_eq!(end, None);
    }

    #[test]
    fn test_december_rollover() {
        let mut parts = HashMap::new();
        _ = parts.insert("year", 2024);
        _ = parts.insert("month", 12);
        _ = parts.insert("day", 1);
        _ = parts.insert("hour", 0);
        _ = parts.insert("minute", 0);
        _ = parts.insert("second", 0);

        let end_time = calculate_end_time(&parts, "month").unwrap();
        let expected = chrono::Utc
            .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp();
        assert_eq!(end_time, expected);
    }

    #[test]
    fn test_year_rollover() {
        let mut parts = HashMap::new();
        _ = parts.insert("year", 2024);
        _ = parts.insert("month", 1);
        _ = parts.insert("day", 1);
        _ = parts.insert("hour", 0);
        _ = parts.insert("minute", 0);
        _ = parts.insert("second", 0);

        let end_time = calculate_end_time(&parts, "year").unwrap();
        let expected = chrono::Utc
            .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp();
        assert_eq!(end_time, expected);
    }

    #[test]
    fn test_parse_field_name() {
        let f = parse_field_name("north.temperature.celsius.avg").unwrap();
        assert_eq!(f.instrument, "north");
        assert_eq!(f.name, "temperature");
        assert_eq!(f.unit, "celsius");
        assert_eq!(f.agg, "avg");
    }

    #[test]
    fn test_parse_field_name_dotted_instrument() {
        let f = parse_field_name("bay.area.sensor.temperature.celsius.avg").unwrap();
        assert_eq!(f.instrument, "bay.area.sensor");
        assert_eq!(f.name, "temperature");
        assert_eq!(f.unit, "celsius");
        assert_eq!(f.agg, "avg");
    }

    #[test]
    fn test_extract_timestamps_year_quarter() {
        // quarter=3 -> month=7 (July), advance by 3 months -> October
        let path = PathBuf::from("pumps/data/res=6h/year=2025/quarter=3/data.parquet");
        let (start, end) = extract_timestamps_from_path(&path).unwrap();

        let expected_start = chrono::Utc
            .with_ymd_and_hms(2025, 7, 1, 0, 0, 0)
            .unwrap()
            .timestamp();
        let expected_end = chrono::Utc
            .with_ymd_and_hms(2025, 10, 1, 0, 0, 0)
            .unwrap()
            .timestamp();

        assert_eq!(start, Some(expected_start));
        assert_eq!(end, Some(expected_end));
    }

    #[test]
    fn test_extract_timestamps_year_quarter_q4() {
        // quarter=4 -> month=10 (October), advance by 3 months -> January next year
        let path = PathBuf::from("pumps/data/res=6h/year=2025/quarter=4/data.parquet");
        let (start, end) = extract_timestamps_from_path(&path).unwrap();

        let expected_start = chrono::Utc
            .with_ymd_and_hms(2025, 10, 1, 0, 0, 0)
            .unwrap()
            .timestamp();
        let expected_end = chrono::Utc
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp();

        assert_eq!(start, Some(expected_start));
        assert_eq!(end, Some(expected_end));
    }

    #[test]
    fn test_extract_timestamps_year_only() {
        let path = PathBuf::from("pumps/data/res=1d/year=2025/data.parquet");
        let (start, end) = extract_timestamps_from_path(&path).unwrap();

        let expected_start = chrono::Utc
            .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp();
        let expected_end = chrono::Utc
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp();

        assert_eq!(start, Some(expected_start));
        assert_eq!(end, Some(expected_end));
    }

    // -----------------------------------------------------------------------
    // Deterministic per-partition export + manifest reconciliation (P3)
    // -----------------------------------------------------------------------

    use crate::factory::test_support::create_provider_context;
    use std::sync::Arc;

    fn mem_table(rows: &[(i64, f64)]) -> Arc<dyn datafusion::catalog::TableProvider> {
        use arrow::array::{Float64Array, TimestampSecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
        ]));
        let ts: Vec<i64> = rows.iter().map(|r| r.0).collect();
        let val: Vec<f64> = rows.iter().map(|r| r.1).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampSecondArray::from(ts)),
                Arc::new(Float64Array::from(val)),
            ],
        )
        .unwrap();
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
    }

    fn ts(y: i32, mo: u32, d: u32) -> i64 {
        chrono::Utc
            .with_ymd_and_hms(y, mo, d, 0, 0, 0)
            .unwrap()
            .timestamp()
    }

    fn inode(p: &Path) -> u64 {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(p).unwrap().ino()
    }

    #[tokio::test]
    async fn test_export_full_build_deterministic_names_and_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path();
        let export_dir = base.join("WellDepth/res=1m");
        let ctx = create_provider_context();
        let parts = vec!["year".to_string(), "month".to_string()];

        let table = mem_table(&[(ts(2025, 6, 15), 1.0), (ts(2025, 7, 10), 2.0)]);
        let (results, _schema) = export_table_provider_to_parquet(
            table,
            "welldepth",
            &export_dir,
            &parts,
            &["x".into()],
            base,
            &ctx,
            "timestamp",
            None,
        )
        .await
        .unwrap();

        let jun = export_dir.join("year=2025/month=6/data.parquet");
        let jul = export_dir.join("year=2025/month=7/data.parquet");
        assert!(jun.exists(), "june partition file missing");
        assert!(jul.exists(), "july partition file missing");
        assert_eq!(results.len(), 2);

        let manifest = read_series_manifest(&export_dir.join(MANIFEST_FILE))
            .unwrap()
            .unwrap();
        assert_eq!(manifest.partitions.len(), 2);
        assert!(manifest.digest.is_none());
        for p in &manifest.partitions {
            assert_eq!(
                p.digest,
                crate::version_cache::file_blake3(&base.join(&p.file)).unwrap()
            );
        }
    }

    #[test]
    fn partition_chunk_predicate_is_a_disjunction_of_partition_equalities() {
        let chunk = vec![
            vec![("year".to_string(), 2025), ("month".to_string(), 6)],
            vec![("year".to_string(), 2025), ("month".to_string(), 7)],
        ];
        let sql = partition_chunk_predicate(&chunk, "timestamp");
        assert_eq!(
            sql,
            "(CAST(date_part('year', \"timestamp\") AS BIGINT) = 2025 AND \
             CAST(date_part('month', \"timestamp\") AS BIGINT) = 6) OR \
             (CAST(date_part('year', \"timestamp\") AS BIGINT) = 2025 AND \
             CAST(date_part('month', \"timestamp\") AS BIGINT) = 7)"
        );
    }

    /// A full rewrite whose partition count exceeds the writer budget is split
    /// into several `COPY` statements. The split must be invisible: every
    /// partition still appears exactly once, holding exactly its own rows, and
    /// no staging directory survives to confuse partition discovery.
    #[tokio::test]
    async fn test_export_chunks_large_partition_counts_without_changing_output() {
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path();
        let export_dir = base.join("WellDepth/res=1m");
        let ctx = create_provider_context();
        let parts = vec!["year".to_string(), "month".to_string(), "day".to_string()];

        // One row per day, over enough days to force multiple chunks.
        let days = MAX_OPEN_PARTITIONS * 2 + 5;
        let start = ts(2024, 1, 1);
        let rows: Vec<(i64, f64)> = (0..days)
            .map(|i| (start + (i as i64) * 86_400, i as f64))
            .collect();

        let (results, _schema) = export_table_provider_to_parquet(
            mem_table(&rows),
            "welldepth",
            &export_dir,
            &parts,
            &["x".into()],
            base,
            &ctx,
            "timestamp",
            None,
        )
        .await
        .unwrap();

        assert_eq!(results.len(), days, "every day must yield one partition");

        // No staging directory may remain: discovery walks the whole tree and
        // would otherwise pick up chunk files under a bogus partition path.
        for entry in std::fs::read_dir(&export_dir).unwrap() {
            let name = entry.unwrap().file_name();
            assert!(
                !name.to_string_lossy().starts_with(".chunk-"),
                "chunk staging dir left behind: {:?}",
                name
            );
        }

        // Each partition holds exactly the single row belonging to that day,
        // which is only true if the chunk predicates were disjoint and total.
        let manifest = read_series_manifest(&export_dir.join(MANIFEST_FILE))
            .unwrap()
            .unwrap();
        assert_eq!(manifest.partitions.len(), days);
        for (i, row) in rows.iter().enumerate() {
            let date = chrono::DateTime::from_timestamp(row.0, 0)
                .unwrap()
                .date_naive();
            use chrono::Datelike;
            let file = export_dir
                .join(format!("year={}", date.format("%Y")))
                .join(format!("month={}", date.month()))
                .join(format!("day={}", date.day()))
                .join("data.parquet");
            assert!(file.exists(), "missing partition for day {i}: {file:?}");

            let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                std::fs::File::open(&file).unwrap(),
            )
            .unwrap();
            assert_eq!(
                reader.metadata().file_metadata().num_rows(),
                1,
                "partition {file:?} should hold exactly its own row"
            );
        }
    }

    #[tokio::test]
    async fn test_export_whole_series_reuse_and_tamper_hardfail() {
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path();
        let export_dir = base.join("WellDepth/res=1m");
        let ctx = create_provider_context();
        let parts = vec!["year".to_string(), "month".to_string()];
        let rows = [(ts(2025, 6, 15), 1.0), (ts(2025, 7, 10), 2.0)];
        let hint = tinyfs::ExportHint {
            digest: "DIGEST-A".into(),
            changed_since: None,
        };

        let (_r, _s) = export_table_provider_to_parquet(
            mem_table(&rows),
            "wd",
            &export_dir,
            &parts,
            &["x".into()],
            base,
            &ctx,
            "timestamp",
            Some(&hint),
        )
        .await
        .unwrap();
        let jul = export_dir.join("year=2025/month=7/data.parquet");
        let jul_ino = inode(&jul);

        let (results, _s) = export_table_provider_to_parquet(
            mem_table(&rows),
            "wd",
            &export_dir,
            &parts,
            &["x".into()],
            base,
            &ctx,
            "timestamp",
            Some(&hint),
        )
        .await
        .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(inode(&jul), jul_ino, "reuse must not rewrite the file");

        std::fs::write(&jul, b"corrupt").unwrap();
        let err = export_table_provider_to_parquet(
            mem_table(&rows),
            "wd",
            &export_dir,
            &parts,
            &["x".into()],
            base,
            &ctx,
            "timestamp",
            Some(&hint),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("does not match its recorded digest"),
            "expected digest hard-fail, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_export_partial_reconcile_reuses_unchanged_partitions() {
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path();
        let export_dir = base.join("WellDepth/res=1m");
        let ctx = create_provider_context();
        let parts = vec!["year".to_string(), "month".to_string()];

        let (_r, _s) = export_table_provider_to_parquet(
            mem_table(&[(ts(2025, 6, 15), 1.0), (ts(2025, 7, 10), 2.0)]),
            "wd",
            &export_dir,
            &parts,
            &["x".into()],
            base,
            &ctx,
            "timestamp",
            Some(&tinyfs::ExportHint {
                digest: "D1".into(),
                changed_since: None,
            }),
        )
        .await
        .unwrap();
        let jun = export_dir.join("year=2025/month=6/data.parquet");
        let jul = export_dir.join("year=2025/month=7/data.parquet");
        let jun_ino = inode(&jun);
        let jul_ino = inode(&jul);

        // July gains a point; changed_since = 2025-07-01. June ends at
        // 2025-07-01 <= lo so it is reused; July is rewritten.
        let lo = ts(2025, 7, 1);
        let (results, _s) = export_table_provider_to_parquet(
            mem_table(&[
                (ts(2025, 6, 15), 1.0),
                (ts(2025, 7, 10), 2.0),
                (ts(2025, 7, 25), 3.0),
            ]),
            "wd",
            &export_dir,
            &parts,
            &["x".into()],
            base,
            &ctx,
            "timestamp",
            Some(&tinyfs::ExportHint {
                digest: "D2".into(),
                changed_since: Some(lo),
            }),
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(
            inode(&jun),
            jun_ino,
            "unchanged June partition must be reused"
        );
        assert_ne!(
            inode(&jul),
            jul_ino,
            "changed July partition must be rewritten"
        );

        let manifest = read_series_manifest(&export_dir.join(MANIFEST_FILE))
            .unwrap()
            .unwrap();
        assert_eq!(manifest.digest.as_deref(), Some("D2"));
    }
}
