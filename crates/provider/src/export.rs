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
use futures::StreamExt;
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

    if !can_reuse {
        // A full export regenerates every partition, so prior outputs are
        // unconditionally stale.
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

    let source_schema = table_provider.schema();
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
            source_schema,
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

    let (results, manifest, reused, written) = reconcile_partitioned(
        ctx,
        &unique_table_name,
        temporal_parts,
        timestamp_column,
        export_dir,
        base_output_dir,
        captures,
        source_label,
        hint.expect("reconcile requires an export hint"),
        seed_manifest
            .as_ref()
            .expect("reconcile requires a seed manifest"),
        reuse_before.expect("reconcile requires a changed-since watermark"),
        source_schema,
    )
    .await?;

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

async fn reconcile_partitioned(
    ctx: &datafusion::prelude::SessionContext,
    table: &str,
    temporal_parts: &[String],
    timestamp_column: &str,
    export_dir: &Path,
    base_output_dir: &Path,
    captures: &[String],
    source_label: &str,
    hint: &tinyfs::ExportHint,
    seed: &SeriesExportManifest,
    changed_since: i64,
    source_schema: arrow::datatypes::SchemaRef,
) -> Result<(
    Vec<(Vec<String>, ExportOutput)>,
    SeriesExportManifest,
    usize,
    usize,
)> {
    let mut reusable_by_file: HashMap<PathBuf, &ManifestPartition> = HashMap::new();
    let mut rewrite_from = Some(changed_since);

    for partition in &seed.partitions {
        let abs = base_output_dir.join(&partition.file);
        if matches!(partition.end_time, Some(end) if end <= changed_since) {
            if !abs.exists() {
                return Err(anyhow::anyhow!(
                    "Reusable export partition '{}' for '{}' is missing",
                    abs.display(),
                    source_label
                ));
            }
            verify_partition_digest(&abs, &partition.digest, source_label)?;
            _ = reusable_by_file.insert(partition.file.clone(), partition);
            continue;
        }

        rewrite_from = match (rewrite_from, partition.start_time) {
            (Some(current), Some(start)) => Some(current.min(start)),
            _ => None,
        };
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

    stream_partitioned_export(
        ctx,
        table,
        temporal_parts,
        timestamp_column,
        export_dir,
        source_label,
        source_schema,
        rewrite_from,
    )
    .await?;

    let produced = discover_exported_files(export_dir, base_output_dir)?;
    let mut manifest = SeriesExportManifest {
        digest: Some(hint.digest.clone()),
        partitions: Vec::with_capacity(produced.len()),
    };
    let mut results = Vec::with_capacity(produced.len());
    let mut reused = 0;
    let mut written = 0;
    for output in produced {
        let partition = if let Some(seed_partition) = reusable_by_file.get(&output.file) {
            reused += 1;
            (*seed_partition).clone()
        } else {
            written += 1;
            ManifestPartition {
                digest: crate::version_cache::file_blake3(&base_output_dir.join(&output.file))?,
                file: output.file.clone(),
                start_time: output.start_time,
                end_time: output.end_time,
            }
        };
        manifest.partitions.push(partition);
        results.push((captures.to_vec(), output));
    }
    results.sort_by(|a, b| a.1.file.cmp(&b.1.file));
    manifest.partitions.sort_by(|a, b| a.file.cmp(&b.file));
    Ok((results, manifest, reused, written))
}

/// Full-rewrite export from one ordered source execution.
///
/// DataFusion's partitioned `COPY` holds one writer open per partition and
/// requires a separate partition-enumeration execution to bound that set.
/// Instead, this path orders rows by their partition tuple and timestamp, then
/// closes each Parquet writer before opening the next. DataFusion may spill the
/// sort, while export memory remains independent of partition count.
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
    source_schema: arrow::datatypes::SchemaRef,
) -> Result<(Vec<(Vec<String>, ExportOutput)>, SeriesExportManifest)> {
    stream_partitioned_export(
        ctx,
        table,
        temporal_parts,
        timestamp_column,
        export_dir,
        source_label,
        source_schema,
        None,
    )
    .await?;

    let produced = discover_exported_files(export_dir, base_output_dir)?;
    let mut manifest = SeriesExportManifest {
        digest: hint.map(|h| h.digest.clone()),
        partitions: Vec::new(),
    };
    let mut results: Vec<(Vec<String>, ExportOutput)> = Vec::new();
    for out in produced {
        let digest = crate::version_cache::file_blake3(&base_output_dir.join(&out.file))?;
        manifest.partitions.push(ManifestPartition {
            file: out.file.clone(),
            digest,
            start_time: out.start_time,
            end_time: out.end_time,
        });
        results.push((
            captures.to_vec(),
            ExportOutput {
                file: out.file,
                start_time: out.start_time,
                end_time: out.end_time,
            },
        ));
    }
    results.sort_by(|a, b| a.1.file.cmp(&b.1.file));
    manifest.partitions.sort_by(|a, b| a.file.cmp(&b.file));
    Ok((results, manifest))
}

async fn stream_partitioned_export(
    ctx: &datafusion::prelude::SessionContext,
    table: &str,
    temporal_parts: &[String],
    timestamp_column: &str,
    export_dir: &Path,
    source_label: &str,
    source_schema: arrow::datatypes::SchemaRef,
    event_time_lo: Option<i64>,
) -> Result<()> {
    let partition_aliases = temporal_parts
        .iter()
        .enumerate()
        .map(|(i, _)| format!("__watertown_export_partition_{i}"))
        .collect::<Vec<_>>();
    let temporal_columns = temporal_parts
        .iter()
        .zip(&partition_aliases)
        .map(|(part, alias)| {
            format!(
                "CAST(date_part('{}', \"{}\") AS BIGINT) AS \"{}\"",
                part, timestamp_column, alias
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let where_sql = event_time_lo.map_or_else(String::new, |lo| {
        format!(" WHERE \"{timestamp_column}\" >= to_timestamp_seconds({lo})")
    });
    let sql = if temporal_parts.is_empty() {
        format!("SELECT * FROM \"{table}\"{where_sql} ORDER BY \"{timestamp_column}\"")
    } else {
        let partition_order = (1..=temporal_parts.len())
            .map(|i| (source_schema.fields().len() + i).to_string())
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "SELECT *, {temporal_columns} FROM \"{table}\"{where_sql} \
             ORDER BY {partition_order}, \"{timestamp_column}\""
        )
    };
    log::debug!("full-rewrite export query: {}", sql);
    let mut stream = ctx
        .sql(&sql)
        .await
        .map_err(|e| anyhow::anyhow!("export planning failed for '{}': {}", source_label, e))?
        .execute_stream()
        .await
        .map_err(|e| anyhow::anyhow!("export execution failed for '{}': {}", source_label, e))?;

    let source_columns = source_schema.fields().len();
    let mut active: Option<ActivePartitionWriter> = None;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|e| {
            anyhow::anyhow!("export execution failed for '{}': {}", source_label, e)
        })?;
        let partition_columns = (0..temporal_parts.len())
            .map(|i| {
                batch
                    .column(source_columns + i)
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
            .collect::<Result<Vec<_>>>()?;
        let source_batch = arrow::record_batch::RecordBatch::try_new(
            source_schema.clone(),
            batch.columns()[..source_columns].to_vec(),
        )?;

        let mut start = 0;
        while start < batch.num_rows() {
            let part = partition_tuple(&partition_columns, temporal_parts, start, source_label)?;
            let mut end = start + 1;
            while end < batch.num_rows()
                && partition_tuple(&partition_columns, temporal_parts, end, source_label)? == part
            {
                end += 1;
            }
            if active.as_ref().map(|writer| &writer.part) != Some(&part) {
                if let Some(writer) = active.take() {
                    writer.finish(source_label)?;
                }
                active = Some(ActivePartitionWriter::create(
                    export_dir,
                    part,
                    source_schema.clone(),
                    source_label,
                )?);
            }
            active
                .as_mut()
                .expect("partition writer was just created")
                .writer
                .write(&source_batch.slice(start, end - start))
                .map_err(|e| {
                    anyhow::anyhow!("Parquet write failed for '{}': {}", source_label, e)
                })?;
            start = end;
        }
    }

    if let Some(writer) = active {
        writer.finish(source_label)?;
    }
    Ok(())
}

fn partition_tuple(
    columns: &[&arrow::array::Int64Array],
    names: &[String],
    row: usize,
    source_label: &str,
) -> Result<Vec<(String, i64)>> {
    use arrow::array::Array;

    columns
        .iter()
        .zip(names)
        .map(|(column, name)| {
            if column.is_null(row) {
                return Err(anyhow::anyhow!(
                    "Null timestamp partition value for '{}' in '{}'. \
                     This indicates nullable timestamp data.",
                    name,
                    source_label
                ));
            }
            Ok((name.clone(), column.value(row)))
        })
        .collect()
}

struct ActivePartitionWriter {
    part: Vec<(String, i64)>,
    final_path: PathBuf,
    temp_path: PathBuf,
    writer: parquet::arrow::ArrowWriter<std::fs::File>,
}

impl ActivePartitionWriter {
    fn create(
        export_dir: &Path,
        part: Vec<(String, i64)>,
        schema: arrow::datatypes::SchemaRef,
        source_label: &str,
    ) -> Result<Self> {
        let final_path = export_dir
            .join(partition_rel_dir(&part))
            .join("data.parquet");
        if let Some(parent) = final_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        if final_path.exists() {
            return Err(anyhow::anyhow!(
                "Partition '{}' was encountered more than once for '{}'; \
                 ordered export must make each partition contiguous.",
                final_path.display(),
                source_label
            ));
        }
        let temp_path = final_path.with_extension(format!("parquet.tmp-{}", std::process::id()));
        let file = std::fs::File::create(&temp_path)?;
        let writer = parquet::arrow::ArrowWriter::try_new(file, schema, None)?;
        Ok(Self {
            part,
            final_path,
            temp_path,
            writer,
        })
    }

    fn finish(self, source_label: &str) -> Result<()> {
        _ = self
            .writer
            .close()
            .map_err(|e| anyhow::anyhow!("Parquet close failed for '{}': {}", source_label, e))?;
        std::fs::rename(&self.temp_path, &self.final_path).map_err(|e| {
            anyhow::anyhow!(
                "Failed to publish partition '{}' for '{}': {}",
                self.final_path.display(),
                source_label,
                e
            )
        })?;
        Ok(())
    }
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
    use std::any::Any;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    #[derive(Debug)]
    struct ScanCountingTableProvider {
        inner: Arc<dyn datafusion::catalog::TableProvider>,
        scans: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl datafusion::catalog::TableProvider for ScanCountingTableProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            self.inner.schema()
        }

        fn table_type(&self) -> datafusion::datasource::TableType {
            self.inner.table_type()
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&datafusion::logical_expr::Expr],
        ) -> datafusion::error::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>>
        {
            self.inner.supports_filters_pushdown(filters)
        }

        async fn scan(
            &self,
            state: &dyn datafusion::catalog::Session,
            projection: Option<&Vec<usize>>,
            filters: &[datafusion::logical_expr::Expr],
            limit: Option<usize>,
        ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            _ = self.scans.fetch_add(1, Ordering::Relaxed);
            self.inner.scan(state, projection, filters, limit).await
        }
    }

    fn scan_counting_table(
        inner: Arc<dyn datafusion::catalog::TableProvider>,
    ) -> (
        Arc<dyn datafusion::catalog::TableProvider>,
        Arc<AtomicUsize>,
    ) {
        let scans = Arc::new(AtomicUsize::new(0));
        (
            Arc::new(ScanCountingTableProvider {
                inner,
                scans: scans.clone(),
            }),
            scans,
        )
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

    /// A full rewrite with many partitions keeps one writer open at a time.
    /// Every partition must appear exactly once and hold exactly its own rows.
    #[tokio::test]
    async fn test_export_streams_large_partition_counts_without_changing_output() {
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path();
        let export_dir = base.join("WellDepth/res=1m");
        let ctx = create_provider_context();
        let parts = vec!["year".to_string(), "month".to_string(), "day".to_string()];

        // This exceeds the former 128-writer budget by more than twofold.
        let days = 261;
        let start = ts(2024, 1, 1);
        let rows: Vec<(i64, f64)> = (0..days)
            .map(|i| (start + (i as i64) * 86_400, i as f64))
            .collect();

        let (table, scans) = scan_counting_table(mem_table(&rows));
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

        assert_eq!(results.len(), days, "every day must yield one partition");
        assert_eq!(
            scans.load(Ordering::Relaxed),
            1,
            "full rewrite must plan exactly one source scan"
        );

        // No temporary output may remain for discovery to mistake as data.
        for entry in std::fs::read_dir(&export_dir).unwrap() {
            let name = entry.unwrap().file_name();
            assert!(
                !name.to_string_lossy().starts_with(".chunk-"),
                "staging dir left behind: {:?}",
                name
            );
        }

        // Each partition holds exactly the single row belonging to that day,
        // which is only true if streamed partition routing was exact.
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

        let (table, scans) = scan_counting_table(mem_table(&rows));
        let (results, _s) = export_table_provider_to_parquet(
            table,
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
        assert_eq!(
            scans.load(Ordering::Relaxed),
            0,
            "unchanged export must not plan a source scan"
        );

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

        // July gains a point after the dirty watermark. June ends before the
        // watermark and is reused; July is rewritten from its partition start
        // so the unchanged July 10 row is retained.
        let lo = ts(2025, 7, 20);
        let (table, scans) = scan_counting_table(mem_table(&[
            (ts(2025, 6, 15), 1.0),
            (ts(2025, 7, 10), 2.0),
            (ts(2025, 7, 25), 3.0),
        ]));
        let (results, _s) = export_table_provider_to_parquet(
            table,
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
        assert_eq!(
            scans.load(Ordering::Relaxed),
            1,
            "reconcile must plan one bounded dirty-tail source scan"
        );
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(&jul).unwrap(),
        )
        .unwrap();
        assert_eq!(
            reader.metadata().file_metadata().num_rows(),
            2,
            "dirty partition rewrite must include rows before the watermark"
        );

        let manifest = read_series_manifest(&export_dir.join(MANIFEST_FILE))
            .unwrap()
            .unwrap();
        assert_eq!(manifest.digest.as_deref(), Some("D2"));
    }
}
