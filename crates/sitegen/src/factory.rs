// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Sitegen factory -- registered as an executable factory, invoked via `pond run`.
//!
//! ```bash
//! pond run /etc/site.yaml build ./dist
//! ```
//!
//! The execute function:
//! 1. Parses the site.yaml config from the pond
//! 2. Runs export stages to produce ExportContext per stage
//! 3. Expands the route tree into page jobs
//! 4. Renders each page: markdown -> shortcodes -> HTML -> layout
//! 5. Writes the complete site to the output directory

use crate::config::SiteConfig;
use crate::layouts::{self, LayoutContext};
use crate::markdown::{preprocess_shortcodes, render_markdown};
use crate::routes::{self, ContentContext, ContentPage};
use crate::shortcodes::{self, ExportContext, Health, PondStatus, ShortcodeContext, classify};
use crate::status_summary;
use log::{debug, info, warn};
use provider::{ExecutionContext, FactoryContext, register_executable_factory};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tinyfs::ResultExt;

// ---------------------------------------------------------------------------
// Clap command parsing
// ---------------------------------------------------------------------------

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
struct SitegenCommand {
    #[command(subcommand)]
    command: SitegenSubcommand,
}

#[derive(Debug, Subcommand)]
enum SitegenSubcommand {
    /// Build the complete static site
    Build {
        /// Output directory for generated files
        output_dir: String,
        /// Quick mode: skip data exports and subsites (content pages only)
        #[arg(long)]
        quick: bool,
    },
}

// ---------------------------------------------------------------------------
// Factory registration
// ---------------------------------------------------------------------------

fn validate_config(config: &[u8]) -> tinyfs::Result<Value> {
    // Try parsing as standalone SiteConfig first
    if let Ok(config) = serde_yaml::from_slice::<SiteConfig>(config) {
        return serde_json::to_value(&config).map_other_context("Config serialization error");
    }

    // Fall back: extract sitegen config from a multi-doc pond config file.
    // Looks for a document with `kind: mknod` and `spec.factory: sitegen`,
    // then uses `spec.config` as the SiteConfig.
    extract_sitegen_from_pond_config(config)
}

/// Extract the sitegen factory config from a multi-document pond config file.
///
/// Scans YAML documents for one with `kind: mknod` and `spec.factory: sitegen`,
/// then parses `spec.config` as a `SiteConfig`. This lets `host+sitegen://`
/// read directly from `config/site.yaml` without a separate config file.
fn extract_sitegen_from_pond_config(config: &[u8]) -> tinyfs::Result<Value> {
    let config_str = std::str::from_utf8(config).map_other_context("Non-UTF8 config")?;

    for doc in serde_yaml::Deserializer::from_str(config_str) {
        let value = match Value::deserialize(doc) {
            Ok(v) => v,
            Err(_) => continue,
        };

        if value.get("kind").and_then(|v| v.as_str()) != Some("mknod") {
            continue;
        }

        let factory = value
            .get("spec")
            .and_then(|s| s.get("factory"))
            .and_then(|f| f.as_str());

        if factory != Some("sitegen") {
            continue;
        }

        if let Some(inner_config) = value.get("spec").and_then(|s| s.get("config")) {
            let site_config: SiteConfig =
                serde_json::from_value(inner_config.clone()).map_err(|e| {
                    tinyfs::Error::Other(format!("Invalid sitegen config in pond YAML: {}", e))
                })?;

            return serde_json::to_value(&site_config)
                .map_other_context("Config serialization error");
        }
    }

    Err(tinyfs::Error::Other(
        "No sitegen factory config found in YAML (expected standalone SiteConfig or pond config with factory: sitegen)".to_string(),
    ))
}

async fn initialize(_config: Value, _context: FactoryContext) -> Result<(), tinyfs::Error> {
    Ok(())
}

async fn execute(
    config: Value,
    context: FactoryContext,
    ctx: ExecutionContext,
) -> Result<(), tinyfs::Error> {
    let config: SiteConfig = serde_json::from_value(config).map_other_context("Invalid config")?;

    // Parse command
    let args: Vec<String> = std::iter::once("factory".to_string())
        .chain(ctx.args().iter().cloned())
        .collect();
    let cmd = SitegenCommand::try_parse_from(args).map_other()?;

    match cmd.command {
        SitegenSubcommand::Build { output_dir, quick } => {
            let output_path = std::path::PathBuf::from(&output_dir);
            // Ensure the output root exists before any write.  `sitegen build
            // <dir>` owns its target directory; creating it here makes the
            // build self-contained (callers need not pre-create it) and
            // avoids a transient ENOENT on the first write.
            std::fs::create_dir_all(&output_path).map_err(|e| {
                tinyfs::Error::Other(format!("mkdir output dir {:?}: {}", output_path, e))
            })?;
            let root = context.root().await?;
            let provider_ctx = &context.context;

            if quick {
                info!("Quick mode: skipping exports and subsites");
                // Clear exports and subsites so build_site_from_root skips them
                let mut quick_config = config;
                quick_config.exports.clear();
                quick_config.subsites.clear();

                build_site_from_root(
                    &quick_config,
                    &root,
                    provider_ctx,
                    &output_path,
                    &quick_config.site.base_url,
                )
                .await?;

                write_shared_assets(&output_path)?;
                write_theme_css(&output_path, &quick_config.theme)?;
            } else {
                // Build the main site
                build_site_from_root(
                    &config,
                    &root,
                    provider_ctx,
                    &output_path,
                    &config.site.base_url,
                )
                .await?;

                // Write shared build assets (base CSS, JS, vendor) once
                write_shared_assets(&output_path)?;

                // Write per-site theme overrides
                write_theme_css(&output_path, &config.theme)?;

                // Build subsites from imported ponds
                for subsite in &config.subsites {
                    info!("Building subsite '{}'...", subsite.name);

                    // Read sub-site config from the imported pond
                    let config_full_path = format!("{}{}", subsite.path, subsite.config);
                    let config_bytes = root
                        .read_file_path_to_vec(&config_full_path)
                        .await
                        .map_err(|e| {
                            tinyfs::Error::Other(format!(
                                "Cannot read subsite config '{}': {}",
                                config_full_path, e
                            ))
                        })?;
                    let mut sub_config: SiteConfig = serde_yaml::from_slice(&config_bytes)
                        .map_err(|e| {
                            tinyfs::Error::Other(format!(
                                "Invalid subsite config '{}': {}",
                                config_full_path, e
                            ))
                        })?;

                    // Override base_url if specified, rewriting sidebar links
                    // to match the new mount point.
                    if let Some(ref base_url) = subsite.base_url {
                        let old_base = sub_config.site.base_url.clone();
                        sub_config.site.base_url = base_url.clone();
                        if old_base != *base_url {
                            sub_config.rewrite_sidebar_urls(&old_base, base_url);
                        }
                    }

                    // Derive output subdirectory from base_url relative to parent.
                    // e.g. parent base="/staging/", subsite base="/staging/noyo-harbor/"
                    // → subdir = "noyo-harbor"
                    let parent_base = config.site.base_url.trim_matches('/');
                    let sub_base = subsite
                        .base_url
                        .as_deref()
                        .unwrap_or(&subsite.name)
                        .trim_matches('/');
                    let subdir = sub_base
                        .strip_prefix(parent_base)
                        .unwrap_or(sub_base)
                        .trim_start_matches('/');
                    let subsite_output = output_path.join(subdir);
                    std::fs::create_dir_all(&subsite_output).map_err(|e| {
                        tinyfs::Error::Other(format!("mkdir {:?}: {}", subsite_output, e))
                    })?;

                    // Create WD scoped to the mount point
                    let subsite_wd = root.open_dir_path(&subsite.path).await.map_err(|e| {
                        tinyfs::Error::Other(format!(
                            "Cannot open subsite path '{}': {}",
                            subsite.path, e
                        ))
                    })?;
                    let subsite_root = subsite_wd.as_root();

                    build_site_from_root(
                        &sub_config,
                        &subsite_root,
                        provider_ctx,
                        &subsite_output,
                        &config.site.base_url,
                    )
                    .await?;

                    // Write per-subsite theme overrides
                    write_theme_css(&subsite_output, &sub_config.theme)?;

                    info!("Subsite '{}' built at {:?}", subsite.name, subsite_output);
                }
            }

            Ok(())
        }
    }
}

/// Build a single site from a WD root.
///
/// Runs the full sitegen pipeline: export stages, content stages, route
/// expansion, page rendering, and static asset copying. Does NOT write
/// shared build assets (CSS/JS/vendor) or theme.css -- those are handled
/// by the caller.
///
/// asset references (style.css, chart.js, vendor/). For standalone sites
/// this equals the config's base_url. For subsites it's the parent's
/// base_url.
async fn build_site_from_root(
    config: &SiteConfig,
    root: &tinyfs::WD,
    provider_ctx: &tinyfs::ProviderContext,
    output_dir: &std::path::Path,
    root_base_url: &str,
) -> Result<(), tinyfs::Error> {
    // Run export stages
    let exports = run_export_stages(config, root, provider_ctx, output_dir).await?;

    // Run content stages
    let content = run_content_stages(config, root).await?;

    // Run pond status grid queries (no-op when not configured).
    // We compute these once per site build and pass them through to
    // every page; the shortcode renders nothing when the option is
    // None, so unrelated pages aren't affected.
    let pond_statuses = run_status_grid_queries(config, root, provider_ctx).await?;
    let generated_at = chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S UTC")
        .to_string();

    // Pre-read all files from the pond into a map
    let mut file_cache: BTreeMap<String, String> = BTreeMap::new();

    // Collect all page paths from route expansion
    let expansion = routes::expand_routes(config, &exports, &content);
    let jobs = expansion.jobs;
    for job in &jobs {
        if !file_cache.contains_key(&job.page_source) {
            let data = root
                .read_file_path_to_vec(&job.page_source)
                .await
                .map_err(|e| {
                    tinyfs::Error::Other(format!("Cannot read page '{}': {}", job.page_source, e))
                })?;
            let text = String::from_utf8(data).map_err(|e| {
                tinyfs::Error::Other(format!("Non-UTF8 page '{}': {}", job.page_source, e))
            })?;
            file_cache.insert(job.page_source.clone(), text);
        }
    }

    // Collect partials
    for path in config.partials.values() {
        if !file_cache.contains_key(path) {
            match root.read_file_path_to_vec(path).await {
                Ok(data) => {
                    if let Ok(text) = String::from_utf8(data) {
                        file_cache.insert(path.clone(), text);
                    }
                }
                Err(e) => {
                    warn!("Cannot read partial '{}': {}", path, e);
                }
            }
        }
    }

    // Collect static assets via glob
    let mut static_assets: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for asset in &config.static_assets {
        let matches = root.collect_matches(&asset.pattern).await?;
        if matches.is_empty() {
            warn!("No files matched static pattern '{}'", asset.pattern);
        }
        for (node_path, _captures) in &matches {
            let path_str = node_path.path.to_string_lossy().to_string();
            if let std::collections::btree_map::Entry::Vacant(entry) =
                static_assets.entry(path_str.clone())
            {
                match root.read_file_path_to_vec(&path_str).await {
                    Ok(data) => {
                        entry.insert(data);
                    }
                    Err(e) => {
                        warn!("Cannot read static asset '{}': {}", path_str, e);
                    }
                }
            }
        }
    }

    // Simple lookup closure
    let read_pond_file = |path: &str| -> Result<String, String> {
        file_cache
            .get(path)
            .cloned()
            .ok_or_else(|| format!("File not in cache: {}", path))
    };

    generate_site(
        config,
        &exports,
        &content,
        &read_pond_file,
        output_dir,
        root_base_url,
        pond_statuses,
        &generated_at,
    )
    .map_other()?;

    // Copy static assets to output, preserving directory structure
    copy_static_assets(&static_assets, output_dir)?;

    Ok(())
}

/// Run export stages: glob-match pond files, export as Hive-partitioned parquet,
/// scan output for real files with start_time/end_time.
///
/// Uses `provider::export::export_series_to_parquet` -- the same DataFusion
/// `COPY TO ... PARTITIONED BY` pipeline used by `pond export`.
///
/// # Automatic Partitioning
///
/// For each stage, the export:
///
/// 1. Discovers all resolutions from glob matches (looking for `res=Xh` in
///    capture groups).
/// 2. Computes per-resolution partition plans using
///    `partitions::compute_partitions()` -- each resolution gets the finest
///    temporal partition that satisfies the <= 2 file invariant for its
///    maximum viewport width.
/// 3. Exports each match with its resolution-specific temporal partition.
async fn run_export_stages(
    config: &SiteConfig,
    root: &tinyfs::WD,
    provider_ctx: &tinyfs::ProviderContext,
    output_dir: &std::path::Path,
) -> Result<BTreeMap<String, ExportContext>, tinyfs::Error> {
    let data_dir = output_dir.join("data");
    let mut exports = BTreeMap::new();

    for stage in &config.exports {
        // Detect format-provider URL patterns vs bare pond glob paths
        if stage.pattern.contains("://") {
            let (by_key, columns) =
                run_format_provider_export(stage, root, provider_ctx, &data_dir, config).await?;
            exports.insert(stage.name.clone(), ExportContext { by_key, columns });
        } else {
            let (by_key, columns) =
                run_queryable_file_export(stage, root, &data_dir, provider_ctx, config).await?;
            exports.insert(stage.name.clone(), ExportContext { by_key, columns });
        }
    }

    Ok(exports)
}

/// Resolve configured data-explorer datasets against the export stages.
///
/// Each `SiteConfig.explore.datasets` entry names an export stage; this
/// flattens that stage's files across all `$0` keys and pairs them with the
/// stage's unioned column names so the explorer can register the dataset and
/// show its schema without a DESCRIBE round-trip. Datasets whose `export`
/// stage produced no files are skipped (empty datasets are not useful).
fn resolve_explore_datasets(
    config: &SiteConfig,
    exports: &BTreeMap<String, ExportContext>,
) -> Vec<shortcodes::ResolvedExploreDataset> {
    let Some(explore) = &config.explore else {
        return vec![];
    };
    let mut resolved = Vec::new();
    for ds in &explore.datasets {
        let Some(ctx) = exports.get(&ds.export) else {
            warn!(
                "explore dataset '{}' references unknown export stage '{}'; skipping",
                ds.table, ds.export
            );
            continue;
        };
        let files: Vec<shortcodes::ExportedFile> = ctx.by_key.values().flatten().cloned().collect();
        if files.is_empty() {
            continue;
        }
        resolved.push(shortcodes::ResolvedExploreDataset {
            table: ds.table.clone(),
            label: ds.label.clone(),
            files,
            columns: ctx.columns.clone(),
        });
    }
    resolved
}

/// Export via format-provider URL pattern (e.g., `jsonlogs:///path/*.jsonl`).
///
/// Uses the provider API to expand the pattern, create table providers
/// through the format provider registry, and export each matched file
/// as Hive-partitioned Parquet.
async fn run_format_provider_export(
    stage: &crate::config::ExportStage,
    root: &tinyfs::WD,
    provider_ctx: &tinyfs::ProviderContext,
    data_dir: &std::path::Path,
    config: &SiteConfig,
) -> Result<(BTreeMap<String, Vec<shortcodes::ExportedFile>>, Vec<String>), tinyfs::Error> {
    let fs = provider_ctx.filesystem();
    let provider = provider::Provider::with_context(Arc::new(fs), Arc::new(provider_ctx.clone()))
        .with_root(root.clone());

    // Use UrlPatternMatcher to expand the pattern and get matched files
    let matcher = provider::UrlPatternMatcher::new(provider);
    let matched_files = match matcher.match_pattern(&stage.pattern, provider_ctx).await {
        Ok(files) => files,
        Err(e) => {
            // No matches is not fatal for log exports (logs may not exist yet)
            warn!(
                "Export stage '{}': pattern '{}' matched no files: {}",
                stage.name, stage.pattern, e
            );
            return Ok((BTreeMap::new(), vec![]));
        }
    };

    info!(
        "Export stage '{}': {} matches for '{}'",
        stage.name,
        matched_files.len(),
        stage.pattern,
    );

    let ctx = &provider_ctx.datafusion_session;
    let url = provider::Url::parse(&stage.pattern).map_err(|e| {
        tinyfs::Error::Other(format!("Invalid URL pattern '{}': {}", stage.pattern, e))
    })?;
    let scheme = url.scheme();
    let timestamp_column = &stage.timestamp_column;

    // For format-provider exports: skip temporal partitioning.
    // Raw log data (all-Utf8 columns) needs casting before date_part works.
    // Export as a single parquet file per source file.
    let temporal_parts: Vec<String> = vec![];

    let mut by_key: BTreeMap<String, Vec<shortcodes::ExportedFile>> = BTreeMap::new();
    let mut columns: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let base = config.site.base_url.trim_end_matches('/');

    for matched in &matched_files {
        let path_str = matched.path().to_string_lossy().to_string();
        let key = matched.captures.first().cloned().unwrap_or_default();

        let rel = series_path_to_data_rel(&path_str);
        let export_dir = data_dir.join(&rel);

        // Create table provider via format provider
        let file_url_str = format!("{}://{}", scheme, path_str);
        let table_provider = matcher
            .provider()
            .create_table_provider(&file_url_str, ctx)
            .await
            .map_err(|e| {
                tinyfs::Error::Other(format!(
                    "Failed to create table provider for '{}': {}",
                    file_url_str, e
                ))
            })?;

        let (export_outputs, schema) = provider::export::export_table_provider_to_parquet(
            table_provider,
            &path_str,
            &export_dir,
            &temporal_parts,
            &matched.captures,
            data_dir,
            provider_ctx,
            timestamp_column,
            None,
        )
        .await
        .map_other_context(format!("export '{}'", path_str))?;

        for field in &schema.fields {
            columns.insert(field.name.clone());
        }

        for (_caps, export_output) in &export_outputs {
            let mut temporal = BTreeMap::new();
            for component in export_output.file.components() {
                let s = component.as_os_str().to_string_lossy();
                if let Some((k, v)) = s.split_once('=')
                    && temporal_parts.contains(&k.to_string())
                {
                    temporal.insert(k.to_string(), v.to_string());
                }
            }

            by_key
                .entry(key.clone())
                .or_default()
                .push(shortcodes::ExportedFile {
                    path: path_str.clone(),
                    file: format!("{}/data/{}", base, export_output.file.to_string_lossy()),
                    captures: matched.captures.clone(),
                    temporal,
                    start_time: export_output.start_time.unwrap_or(0),
                    end_time: export_output.end_time.unwrap_or(0),
                });
        }

        info!(
            "  {} -> {} partitioned files ({:?})",
            path_str,
            export_outputs.len(),
            temporal_parts,
        );
    }

    Ok((by_key, columns.into_iter().collect()))
}

/// Run the pond status grid queries (when `SiteConfig.status_grid` is
/// set), producing one `PondStatus` per matching journal `.jsonl` file.
///
/// Returns `Ok(None)` when the feature isn't configured -- the
/// shortcode renders nothing in that case.  An empty `Vec` means the
/// feature *is* configured but discovery turned up zero files
/// (e.g. journal-ingest hasn't run yet); the shortcode renders an
/// empty-state.
async fn run_status_grid_queries(
    config: &SiteConfig,
    root: &tinyfs::WD,
    provider_ctx: &tinyfs::ProviderContext,
) -> Result<Option<Vec<PondStatus>>, tinyfs::Error> {
    let Some(grid_cfg) = &config.status_grid else {
        return Ok(None);
    };

    // Parse the configured journal_pattern URL once to extract the
    // scheme and the path glob.  We use TinyFS collect_matches() (not
    // UrlPatternMatcher) for discovery so we can match unit_globs
    // against basenames before paying for a TableProvider per file.
    let pattern_url = provider::Url::parse(&grid_cfg.journal_pattern).map_err(|e| {
        tinyfs::Error::Other(format!(
            "status_grid: invalid journal_pattern '{}': {}",
            grid_cfg.journal_pattern, e
        ))
    })?;
    let scheme = pattern_url.scheme().to_string();
    let path_pattern = pattern_url.path().to_string();

    let matches = match root.collect_matches(&path_pattern).await {
        Ok(m) => m,
        Err(e) => {
            warn!(
                "status_grid: collect_matches('{}') failed: {}",
                path_pattern, e
            );
            return Ok(Some(Vec::new()));
        }
    };

    // Filter basenames against unit_globs (simple `*` wildcard).
    let unit_globs: Vec<&str> = grid_cfg.unit_globs.iter().map(String::as_str).collect();
    let matches_unit = |unit: &str| -> bool {
        if unit_globs.is_empty() {
            return true;
        }
        unit_globs.iter().any(|g| simple_glob_match(g, unit))
    };

    let fs = provider_ctx.filesystem();
    // Format-provider URLs (jsonlogs://) only need a root-bound
    // Provider; builtin schemes (series://) require a full
    // ProviderContext.  We build both: `journal_provider` for the
    // jsonlogs:// per-unit journal lookup, and `perf_provider` for
    // the series:// per-pond perf-series lookup when configured.
    let journal_provider = provider::Provider::new(Arc::new(fs.clone())).with_root(root.clone());
    let journal_matcher = provider::UrlPatternMatcher::new(journal_provider);
    let perf_provider =
        provider::Provider::with_context(Arc::new(fs), Arc::new(provider_ctx.clone()))
            .with_root(root.clone());
    let ctx = &provider_ctx.datafusion_session;
    let tail_lines = grid_cfg.tail_lines;
    let perf_pattern = grid_cfg.perf_pattern.as_deref();
    let now_us = chrono::Utc::now().timestamp_micros();

    // Event-time lower bound (epoch µs) for the journal series read: prune
    // versions whose newest event predates `now - hot_window` so per-render
    // memory stays bounded regardless of retained history. A malformed
    // configured duration is a hard error.
    let hot_window_us = grid_cfg.hot_window_secs()? * 1_000_000;
    let window_bounds = tinyfs::SeriesReadBounds::from_event_time_lo(now_us - hot_window_us);

    let mut statuses: Vec<PondStatus> = Vec::new();

    for (np, _captures) in &matches {
        let basename = np.basename();
        let unit = basename.trim_end_matches(".jsonl").to_string();
        if !matches_unit(&unit) {
            continue;
        }

        let path_str = np.path().to_string_lossy().to_string();
        let file_url = format!("{}://{}", scheme, path_str);
        // Per-file unique table name so concurrent registrations don't collide
        // and stale registrations don't leak through.
        let table_name = status_table_name(&unit);

        // With a filesystem cache available, maintain a durable per-unit summary
        // via a version watermark (bounded per-build scan, correct for fields
        // older than the hot window). Without a cache (e.g. memory ponds/tests),
        // fall back to the bounded hot-window read.
        let status_res = if let Some(cache_dir) = provider_ctx.cache_dir() {
            maintain_unit_summary(
                &journal_matcher,
                ctx,
                root,
                cache_dir,
                &file_url,
                &path_str,
                &unit,
                &table_name,
                tail_lines,
                now_us,
                hot_window_us,
            )
            .await
        } else {
            render_unit_windowed(
                &journal_matcher,
                ctx,
                &file_url,
                &unit,
                &table_name,
                window_bounds,
                tail_lines,
            )
            .await
        };
        let mut status = match status_res {
            Ok(s) => s,
            Err(e) => {
                warn!("status_grid: unit '{}' failed: {}", unit, e);
                continue;
            }
        };

        // Enrich with perf-series data when configured.  Failures are
        // logged at warn level and leave the perf fields as None,
        // which in turn keeps `health` at Unknown -- the page still
        // renders, the card just lacks a colour.
        if let Some(pattern) = perf_pattern
            && let Some(pond_name) = extract_pond_name(&unit)
        {
            let perf_url = pattern.replace("{pond}", pond_name);
            populate_perf_fields(&perf_provider, ctx, &perf_url, &mut status).await;
        }

        status.health = classify(
            now_us,
            status.last_ok_us,
            status.last_err_us,
            status.timer_active,
            status.timer_interval_s,
            status.run_wall_s,
        );

        statuses.push(status);
    }

    statuses.sort_by(|a, b| a.unit.cmp(&b.unit));
    Ok(Some(statuses))
}

/// Per-unit journal table name, sanitized to a valid SQL identifier.
fn status_table_name(unit: &str) -> String {
    format!(
        "status_grid_{}",
        unit.replace(
            [
                '@', '.', '-', ':', '/', '\\', ' ', '+', '=', ',', ';', '(', ')',
            ],
            "_"
        )
    )
}

/// Create a bounded journal `TableProvider` for `file_url` and register it under
/// `table_name` in `ctx`.
async fn register_bounded_journal(
    journal_matcher: &provider::UrlPatternMatcher,
    ctx: &datafusion::prelude::SessionContext,
    file_url: &str,
    table_name: &str,
    bounds: tinyfs::SeriesReadBounds,
) -> Result<(), tinyfs::Error> {
    let tp = journal_matcher
        .provider()
        .create_table_provider_bounded(file_url, ctx, bounds)
        .await
        .map_err(|e| {
            tinyfs::Error::Other(format!("create_table_provider('{}'): {}", file_url, e))
        })?;
    ctx.register_table(datafusion::sql::TableReference::bare(table_name), tp)
        .map_err(|e| tinyfs::Error::Other(format!("register_table('{}'): {}", table_name, e)))?;
    Ok(())
}

/// Fallback render path (no filesystem cache): read the bounded hot window and
/// build the card directly from that scan. Correct for fields within the
/// window; used for memory ponds / tests where no durable summary is possible.
async fn render_unit_windowed(
    journal_matcher: &provider::UrlPatternMatcher,
    ctx: &datafusion::prelude::SessionContext,
    file_url: &str,
    unit: &str,
    table_name: &str,
    window_bounds: tinyfs::SeriesReadBounds,
    tail_lines: usize,
) -> Result<PondStatus, tinyfs::Error> {
    register_bounded_journal(journal_matcher, ctx, file_url, table_name, window_bounds).await?;
    let status = query_pond_status(ctx, table_name, unit, tail_lines).await;
    let _ = ctx.deregister_table(datafusion::sql::TableReference::bare(table_name));
    status
}

/// Maintain the durable per-unit summary via a version watermark, then render
/// the card from it.
///
/// - First run (`processed_version == 0`): a one-time backfill bounded by the
///   hot window keeps memory bounded on the initial seed.
/// - Steady state: fold only versions strictly above the watermark (one or a
///   few per build), so the per-build scan is bounded while the summary keeps
///   fields older than the window (a unit silent since an error keeps its card).
#[allow(clippy::too_many_arguments)]
async fn maintain_unit_summary(
    journal_matcher: &provider::UrlPatternMatcher,
    ctx: &datafusion::prelude::SessionContext,
    root: &tinyfs::WD,
    cache_dir: &std::path::Path,
    file_url: &str,
    path_str: &str,
    unit: &str,
    table_name: &str,
    tail_lines: usize,
    now_us: i64,
    hot_window_us: i64,
) -> Result<PondStatus, tinyfs::Error> {
    let versions = root
        .list_file_versions(path_str)
        .await
        .map_err(|e| tinyfs::Error::Other(format!("list_file_versions('{}'): {}", path_str, e)))?;
    let max_version = versions.iter().map(|v| v.version).max().unwrap_or(0);

    let prior = match status_summary::load(cache_dir, unit) {
        Ok(Some(s)) => s,
        Ok(None) => status_summary::UnitSummary::default(),
        Err(e) => {
            // Corrupt sidecar: re-seed rather than break the dashboard.
            warn!("status_grid: {} (re-seeding from backfill)", e);
            status_summary::UnitSummary::default()
        }
    };

    if max_version <= prior.processed_version {
        // No new versions since the last fold -- render from the durable summary.
        return Ok(summary_to_pond_status(&prior, unit));
    }

    let bounds = if prior.processed_version == 0 {
        // One-time backfill: bound the seed read by the hot window so the initial
        // O(N) build doesn't materialize all of history at once.
        tinyfs::SeriesReadBounds::from_event_time_lo(now_us - hot_window_us)
    } else {
        // Steady state: only versions the summary hasn't seen yet.
        tinyfs::SeriesReadBounds::from_version_gt(prior.processed_version as i64)
    };

    register_bounded_journal(journal_matcher, ctx, file_url, table_name, bounds).await?;
    let fold = fold_unit_versions(ctx, table_name, tail_lines).await;
    let _ = ctx.deregister_table(datafusion::sql::TableReference::bare(table_name));
    let fold = fold?;

    let merged = status_summary::merge(prior, fold, max_version, tail_lines);
    if let Err(e) = status_summary::save(cache_dir, unit, &merged) {
        warn!("status_grid: save summary for '{}' failed: {}", unit, e);
    }
    Ok(summary_to_pond_status(&merged, unit))
}

/// Build a `PondStatus` (journal-derived fields only; perf/health filled by the
/// caller) from a durable summary.
fn summary_to_pond_status(summary: &status_summary::UnitSummary, unit: &str) -> PondStatus {
    let peak_rss_bytes = summary
        .last_peak_msg
        .as_deref()
        .and_then(parse_peak_memory_bytes);
    // Summary tail is newest-first; the card renders oldest -> newest.
    let mut tail = summary.tail.clone();
    tail.sort_by_key(|a| a.ts_us);
    let tail_messages = tail.into_iter().map(|t| t.msg).collect();

    PondStatus {
        unit: unit.to_string(),
        last_seen_us: summary.last_seen_us,
        last_ok_us: summary.last_ok_us,
        last_ok_msg: summary.last_ok_msg.clone(),
        last_err_us: summary.last_err_us,
        last_err_msg: summary.last_err_msg.clone(),
        peak_rss_bytes,
        tail_messages,
        timer_active: None,
        last_run_seconds_ago: None,
        timer_interval_s: None,
        run_wall_s: None,
        health: Health::Unknown,
    }
}

/// Extract the bare pond name (the `{pond}` placeholder substitution)
/// from a systemd unit basename.  Strips the systemd template prefixes
/// `user-pond-selfmon@` and `user-pond@`, then the `.service` suffix.
/// Returns `None` for any unit that doesn't match the expected shape
/// -- the caller leaves `Health::Unknown` rather than guessing.
fn extract_pond_name(unit: &str) -> Option<&str> {
    let stripped = unit
        .strip_prefix("user-pond-selfmon@")
        .or_else(|| unit.strip_prefix("user-pond@"))
        .or_else(|| unit.strip_prefix("pond-selfmon@"))
        .or_else(|| unit.strip_prefix("pond@"))?;
    Some(stripped.strip_suffix(".service").unwrap_or(stripped))
}

/// Look up the per-pond perf series and fill in `timer_active`,
/// `last_run_seconds_ago`, `timer_interval_s` on `status`.  All
/// failures are warned and swallowed; missing perf data is not a
/// build error.  Registers the table under a per-unit name so
/// concurrent invocations don't collide.
async fn populate_perf_fields(
    perf_provider: &provider::Provider,
    ctx: &datafusion::prelude::SessionContext,
    perf_url: &str,
    status: &mut PondStatus,
) {
    use datafusion::arrow::array::{Array, Int64Array};

    let table_provider = match perf_provider.create_table_provider(perf_url, ctx).await {
        Ok(tp) => tp,
        Err(e) => {
            warn!(
                "status_grid: perf provider for '{}' failed: {}",
                perf_url, e
            );
            return;
        }
    };

    let perf_table = format!(
        "status_grid_perf_{}",
        status.unit.replace(
            [
                '@', '.', '-', ':', '/', '\\', ' ', '+', '=', ',', ';', '(', ')',
            ],
            "_"
        )
    );
    if let Err(e) = ctx.register_table(
        datafusion::sql::TableReference::bare(perf_table.as_str()),
        table_provider,
    ) {
        warn!(
            "status_grid: perf register_table('{}') failed: {}",
            perf_table, e
        );
        return;
    }

    let sql = format!(
        "SELECT \"timer.active\" AS timer_active, \
                \"last_run.seconds_ago\" AS last_run_seconds_ago, \
                \"timer.interval_s\" AS timer_interval_s \
         FROM {} \
         ORDER BY timestamp DESC LIMIT 1",
        perf_table
    );
    let result = async {
        let df = ctx.sql(&sql).await?;
        df.collect().await
    }
    .await;

    // `run.wall_s` was added to measure-pond.sh after the other perf
    // fields, so a perf series written by an older probe lacks the
    // column entirely and this SELECT would fail schema resolution.
    // Query it separately and best-effort so that during a deploy the
    // established liveness fields above are never lost -- a missing
    // `run.wall_s` just falls the staleness threshold back to
    // `2 * timer_interval_s`.
    let run_wall_result = async {
        let df = ctx
            .sql(&format!(
                "SELECT \"run.wall_s\" AS run_wall_s \
                 FROM {} \
                 ORDER BY timestamp DESC LIMIT 1",
                perf_table
            ))
            .await?;
        df.collect().await
    }
    .await;

    let _ = ctx.deregister_table(datafusion::sql::TableReference::bare(perf_table.as_str()));

    let batches = match result {
        Ok(b) => b,
        Err(e) => {
            warn!("status_grid: perf query for '{}' failed: {}", perf_url, e);
            return;
        }
    };

    let scalar_i64 =
        |batches: &[datafusion::arrow::array::RecordBatch], col: usize| -> Option<i64> {
            batches.iter().find_map(|b| {
                if b.num_rows() == 0 {
                    return None;
                }
                let arr = b.column(col).as_any().downcast_ref::<Int64Array>()?;
                if arr.is_null(0) {
                    None
                } else {
                    Some(arr.value(0))
                }
            })
        };

    let get_i64 = |col: usize| -> Option<i64> { scalar_i64(&batches, col) };

    if let Some(v) = get_i64(0) {
        status.timer_active = Some(v != 0);
    }
    status.last_run_seconds_ago = get_i64(1);
    status.timer_interval_s = get_i64(2).filter(|&v| v > 0);
    status.run_wall_s = run_wall_result
        .ok()
        .and_then(|b| scalar_i64(&b, 0))
        .filter(|&v| v > 0);
}

/// Run the per-unit DataFusion queries for the status grid.
///
/// `table_name` must already be registered in `ctx` as the journal
/// table for this unit (single-file `jsonlogs://` provider).  The
/// table schema has all-Utf8 columns including `__REALTIME_TIMESTAMP`
/// (microseconds since epoch as a string) and `MESSAGE`.
async fn query_pond_status(
    ctx: &datafusion::prelude::SessionContext,
    table_name: &str,
    unit: &str,
    tail_lines: usize,
) -> Result<PondStatus, tinyfs::Error> {
    use datafusion::arrow::array::{Array, Int64Array, StringArray};

    let to_err = |e: datafusion::error::DataFusionError, q: &str| -> tinyfs::Error {
        tinyfs::Error::Other(format!("status_grid sql ({}): {}", q, e))
    };

    // Combined status query.  We extract:
    //   * last_seen_us       -- MAX(ts) overall
    //   * last_ok_us/msg     -- last `Run summary ... outcome=ok` line
    //   * last_err_us/msg    -- last `Run summary ... outcome=err` line
    //   * last_peak_msg      -- last `Peak memory usage: ...` line
    //
    // The earlier `Started %` / `Stopped %` / `Failed %` patterns came
    // from systemd itself and are emitted under `_SYSTEMD_UNIT=init.scope`
    // (not the unit's own journal), so they never appeared in a
    // per-unit `.jsonl` and the fields were always blank.  The
    // `Run summary` line is emitted by pond on every run and is the
    // authoritative outcome marker.
    //
    // Each `_msg` column is paired to the SAME row as its matching max
    // timestamp via `first_value(... ORDER BY ts DESC)`.  A prior version
    // took `MAX(MESSAGE)` independently of `MAX(ts)`, which returned the
    // lexicographically-greatest message rather than the message of the
    // most-recent matching line -- so a card could show an old/ wrong
    // summary.  `NULLS LAST` on the DESC ordering keeps the non-matching
    // rows (NULL sort key) after the real ones, so `first_value` lands on
    // the newest matching row; when no rows match, the column is NULL.
    let status_sql = format!(
        "SELECT \
            MAX(ts) AS last_seen, \
            MAX(CASE WHEN MESSAGE LIKE '%Run summary %outcome=ok%' THEN ts END) AS last_ok_us, \
            first_value(CASE WHEN MESSAGE LIKE '%Run summary %outcome=ok%' THEN MESSAGE END \
                ORDER BY CASE WHEN MESSAGE LIKE '%Run summary %outcome=ok%' THEN ts END DESC NULLS LAST) AS last_ok_msg, \
            MAX(CASE WHEN MESSAGE LIKE '%Run summary %outcome=err%' THEN ts END) AS last_err_us, \
            first_value(CASE WHEN MESSAGE LIKE '%Run summary %outcome=err%' THEN MESSAGE END \
                ORDER BY CASE WHEN MESSAGE LIKE '%Run summary %outcome=err%' THEN ts END DESC NULLS LAST) AS last_err_msg, \
            first_value(CASE WHEN MESSAGE LIKE '%Peak memory usage:%' THEN MESSAGE END \
                ORDER BY CASE WHEN MESSAGE LIKE '%Peak memory usage:%' THEN ts END DESC NULLS LAST) AS last_peak_msg \
         FROM (SELECT CAST(\"__REALTIME_TIMESTAMP\" AS BIGINT) AS ts, \"MESSAGE\" AS MESSAGE FROM {}) t",
        table_name
    );

    let df = ctx
        .sql(&status_sql)
        .await
        .map_err(|e| to_err(e, "status"))?;
    let batches = df.collect().await.map_err(|e| to_err(e, "status"))?;

    let get_i64 =
        |batches: &[datafusion::arrow::record_batch::RecordBatch], col: usize| -> Option<i64> {
            batches.iter().find_map(|b| {
                if b.num_rows() == 0 {
                    return None;
                }
                let arr = b.column(col).as_any().downcast_ref::<Int64Array>()?;
                if arr.is_null(0) {
                    None
                } else {
                    Some(arr.value(0))
                }
            })
        };
    let get_str =
        |batches: &[datafusion::arrow::record_batch::RecordBatch], col: usize| -> Option<String> {
            batches.iter().find_map(|b| {
                if b.num_rows() == 0 {
                    return None;
                }
                let arr = b.column(col).as_any().downcast_ref::<StringArray>()?;
                if arr.is_null(0) {
                    None
                } else {
                    Some(arr.value(0).to_string())
                }
            })
        };

    let last_seen_us = get_i64(&batches, 0);
    let last_ok_us = get_i64(&batches, 1);
    let last_ok_msg = get_str(&batches, 2);
    let last_err_us = get_i64(&batches, 3);
    let last_err_msg = get_str(&batches, 4);
    let last_peak_msg = get_str(&batches, 5);
    let peak_rss_bytes = last_peak_msg.as_deref().and_then(parse_peak_memory_bytes);

    // Tail query: most recent N messages, oldest -> newest in display.
    //
    // Filter out the verbose memory-profile diagnostic that pond emits
    // on every run -- lines like `=== Large Allocations ...`,
    // `=== Total: N large allocations ===`, and `  #N: NN.NN MB @ 0x...`.
    // These crowd out the meaningful Run summary / error lines and make
    // every healthy pond look like it's crashing.
    let tail_sql = format!(
        "SELECT MESSAGE FROM (\
            SELECT CAST(\"__REALTIME_TIMESTAMP\" AS BIGINT) AS ts, \"MESSAGE\" AS MESSAGE FROM {} \
            WHERE \"MESSAGE\" NOT LIKE '=== Large Allocations%' \
              AND \"MESSAGE\" NOT LIKE '=== Total:%large allocations%' \
              AND \"MESSAGE\" NOT LIKE '  #%MB @ 0x%' \
            ORDER BY ts DESC LIMIT {}\
         ) t ORDER BY ts ASC",
        table_name, tail_lines
    );
    let df = ctx.sql(&tail_sql).await.map_err(|e| to_err(e, "tail"))?;
    let batches = df.collect().await.map_err(|e| to_err(e, "tail"))?;
    let mut tail_messages: Vec<String> = Vec::new();
    for b in &batches {
        if b.num_columns() == 0 {
            continue;
        }
        if let Some(arr) = b.column(0).as_any().downcast_ref::<StringArray>() {
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    tail_messages.push(arr.value(i).to_string());
                }
            }
        }
    }

    Ok(PondStatus {
        unit: unit.to_string(),
        last_seen_us,
        last_ok_us,
        last_ok_msg,
        last_err_us,
        last_err_msg,
        peak_rss_bytes,
        tail_messages,
        timer_active: None,
        last_run_seconds_ago: None,
        timer_interval_s: None,
        run_wall_s: None,
        health: Health::Unknown,
    })
}

/// Fold the currently-registered journal table (already bounded to the versions
/// above the watermark, or the backfill window) into a
/// [`status_summary::FoldResult`].
///
/// Mirrors [`query_pond_status`] but returns the peak timestamp and per-line
/// tail timestamps so successive folds can be merged across build boundaries.
async fn fold_unit_versions(
    ctx: &datafusion::prelude::SessionContext,
    table_name: &str,
    tail_lines: usize,
) -> Result<status_summary::FoldResult, tinyfs::Error> {
    use datafusion::arrow::array::{Array, Int64Array, StringArray};

    let to_err = |e: datafusion::error::DataFusionError, q: &str| -> tinyfs::Error {
        tinyfs::Error::Other(format!("status_grid fold sql ({}): {}", q, e))
    };

    // Same pairing logic as query_pond_status, plus last_peak_us so folds merge
    // by timestamp.
    let status_sql = format!(
        "SELECT \
            MAX(ts) AS last_seen, \
            MAX(CASE WHEN MESSAGE LIKE '%Run summary %outcome=ok%' THEN ts END) AS last_ok_us, \
            first_value(CASE WHEN MESSAGE LIKE '%Run summary %outcome=ok%' THEN MESSAGE END \
                ORDER BY CASE WHEN MESSAGE LIKE '%Run summary %outcome=ok%' THEN ts END DESC NULLS LAST) AS last_ok_msg, \
            MAX(CASE WHEN MESSAGE LIKE '%Run summary %outcome=err%' THEN ts END) AS last_err_us, \
            first_value(CASE WHEN MESSAGE LIKE '%Run summary %outcome=err%' THEN MESSAGE END \
                ORDER BY CASE WHEN MESSAGE LIKE '%Run summary %outcome=err%' THEN ts END DESC NULLS LAST) AS last_err_msg, \
            MAX(CASE WHEN MESSAGE LIKE '%Peak memory usage:%' THEN ts END) AS last_peak_us, \
            first_value(CASE WHEN MESSAGE LIKE '%Peak memory usage:%' THEN MESSAGE END \
                ORDER BY CASE WHEN MESSAGE LIKE '%Peak memory usage:%' THEN ts END DESC NULLS LAST) AS last_peak_msg \
         FROM (SELECT CAST(\"__REALTIME_TIMESTAMP\" AS BIGINT) AS ts, \"MESSAGE\" AS MESSAGE FROM {}) t",
        table_name
    );

    let df = ctx
        .sql(&status_sql)
        .await
        .map_err(|e| to_err(e, "status"))?;
    let batches = df.collect().await.map_err(|e| to_err(e, "status"))?;

    let get_i64 = |col: usize| -> Option<i64> {
        batches.iter().find_map(|b| {
            if b.num_rows() == 0 {
                return None;
            }
            let arr = b.column(col).as_any().downcast_ref::<Int64Array>()?;
            if arr.is_null(0) {
                None
            } else {
                Some(arr.value(0))
            }
        })
    };
    let get_str = |col: usize| -> Option<String> {
        batches.iter().find_map(|b| {
            if b.num_rows() == 0 {
                return None;
            }
            let arr = b.column(col).as_any().downcast_ref::<StringArray>()?;
            if arr.is_null(0) {
                None
            } else {
                Some(arr.value(0).to_string())
            }
        })
    };

    // Tail query: newest N (ts, MESSAGE), newest-first, excluding the verbose
    // memory-profile diagnostic lines (see query_pond_status).
    let tail_sql = format!(
        "SELECT ts, MESSAGE FROM (\
            SELECT CAST(\"__REALTIME_TIMESTAMP\" AS BIGINT) AS ts, \"MESSAGE\" AS MESSAGE FROM {} \
            WHERE \"MESSAGE\" NOT LIKE '=== Large Allocations%' \
              AND \"MESSAGE\" NOT LIKE '=== Total:%large allocations%' \
              AND \"MESSAGE\" NOT LIKE '  #%MB @ 0x%' \
            ORDER BY ts DESC LIMIT {}\
         ) t ORDER BY ts DESC",
        table_name, tail_lines
    );
    let df = ctx.sql(&tail_sql).await.map_err(|e| to_err(e, "tail"))?;
    let tail_batches = df.collect().await.map_err(|e| to_err(e, "tail"))?;
    let mut tail: Vec<status_summary::TailLine> = Vec::new();
    for b in &tail_batches {
        if b.num_columns() < 2 {
            continue;
        }
        let ts_arr = b.column(0).as_any().downcast_ref::<Int64Array>();
        let msg_arr = b.column(1).as_any().downcast_ref::<StringArray>();
        if let (Some(ts_arr), Some(msg_arr)) = (ts_arr, msg_arr) {
            for i in 0..b.num_rows() {
                if !ts_arr.is_null(i) && !msg_arr.is_null(i) {
                    tail.push(status_summary::TailLine {
                        ts_us: ts_arr.value(i),
                        msg: msg_arr.value(i).to_string(),
                    });
                }
            }
        }
    }

    Ok(status_summary::FoldResult {
        last_seen_us: get_i64(0),
        last_ok_us: get_i64(1),
        last_ok_msg: get_str(2),
        last_err_us: get_i64(3),
        last_err_msg: get_str(4),
        last_peak_us: get_i64(5),
        last_peak_msg: get_str(6),
        tail,
    })
}

/// Parse "Peak memory usage: 12.34 MB" / "Peak memory usage: 1.5 GB"
/// (and KB/B variants) into a byte count.  Returns `None` for any
/// unrecognized shape -- callers fall back to "unknown" in the UI.
fn parse_peak_memory_bytes(msg: &str) -> Option<u64> {
    let rest = msg.strip_prefix("Peak memory usage:")?.trim();
    let mut it = rest.split_whitespace();
    let num_str = it.next()?;
    let unit = it.next().unwrap_or("B");
    let num: f64 = num_str.parse().ok()?;
    let mult: f64 = match unit.to_ascii_uppercase().as_str() {
        "B" | "BYTES" => 1.0,
        "K" | "KB" | "KIB" => 1024.0,
        "M" | "MB" | "MIB" => 1024.0 * 1024.0,
        "G" | "GB" | "GIB" => 1024.0 * 1024.0 * 1024.0,
        "T" | "TB" | "TIB" => 1024.0_f64.powi(4),
        _ => return None,
    };
    let bytes = (num * mult).round();
    if bytes.is_finite() && bytes >= 0.0 {
        Some(bytes as u64)
    } else {
        None
    }
}

/// Minimal shell-style glob matcher (supports `*` only) used to filter
/// journal basenames against `status_grid.unit_globs`.  Sufficient for
/// systemd template-unit names like `user-pond@*.service`; we
/// intentionally avoid pulling in a full glob crate just for this.
fn simple_glob_match(pattern: &str, s: &str) -> bool {
    // Iterative two-pointer scan with backtracking on `*`.
    let p = pattern.as_bytes();
    let t = s.as_bytes();
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star, mut match_idx): (Option<usize>, usize) = (None, 0);
    while ti < t.len() {
        if pi < p.len() && p[pi] == b'*' {
            star = Some(pi);
            match_idx = ti;
            pi += 1;
        } else if pi < p.len() && p[pi] == t[ti] {
            pi += 1;
            ti += 1;
        } else if let Some(sp) = star {
            pi = sp + 1;
            match_idx += 1;
            ti = match_idx;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == b'*' {
        pi += 1;
    }
    pi == p.len()
}

/// Export queryable pond files (original path: bare glob patterns).
async fn run_queryable_file_export(
    stage: &crate::config::ExportStage,
    root: &tinyfs::WD,
    data_dir: &std::path::Path,
    provider_ctx: &tinyfs::ProviderContext,
    config: &SiteConfig,
) -> Result<(BTreeMap<String, Vec<shortcodes::ExportedFile>>, Vec<String>), tinyfs::Error> {
    let matches = root.collect_matches(&stage.pattern).await?;
    info!(
        "Export stage '{}': {} matches for '{}'",
        stage.name,
        matches.len(),
        stage.pattern
    );

    let target_points = stage.target_points;
    let display = crate::partitions::DisplayConfig { target_points };

    // Discover resolutions from matches
    let mut resolutions = Vec::new();
    for (_node_path, captures) in &matches {
        if let Some(res) = crate::partitions::extract_resolution_from_captures(captures)
            && !resolutions.contains(&res)
        {
            resolutions.push(res);
        }
    }

    // Compute per-resolution partition plans
    let partition_plans = if resolutions.is_empty() {
        warn!(
            "Export stage '{}': no resolutions found in captures. \
             Using default temporal partition ['year'].",
            stage.name
        );
        None
    } else {
        // max_width = coarsest_resolution * target_points
        // This is the widest viewport the coarsest resolution will serve.
        let mut max_res_secs = 0u64;
        for res in &resolutions {
            if let Some(secs) = crate::partitions::parse_duration_secs(res) {
                max_res_secs = max_res_secs.max(secs);
            }
        }
        let max_width_secs = max_res_secs * target_points;

        let (partitions, warnings) =
            crate::partitions::compute_partitions(&resolutions, &display, max_width_secs);

        for w in &warnings {
            warn!("Export stage '{}': {}", stage.name, w);
        }

        info!(
            "Export stage '{}': auto-partitioning {} resolutions ({:?})",
            stage.name,
            partitions.len(),
            resolutions,
        );
        for (res, plan) in &partitions {
            info!(
                "  {} -> {:?} (partition ~= {}s, max {} pts/file)",
                res, plan.temporal, plan.partition_width_secs, plan.max_points_per_file
            );
        }

        Some(partitions)
    };

    let mut by_key: BTreeMap<String, Vec<shortcodes::ExportedFile>> = BTreeMap::new();
    let mut columns: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    for (node_path, captures) in &matches {
        let path_str = node_path.path.to_string_lossy().to_string();
        let key = captures.first().cloned().unwrap_or_default();

        let rel = series_path_to_data_rel(&path_str);
        let export_dir = data_dir.join(&rel);

        // Look up resolution-specific temporal partition
        let temporal_parts: Vec<String> = if let Some(ref partitions) = partition_plans {
            if let Some(res) = crate::partitions::extract_resolution_from_captures(captures) {
                if let Some(plan) = partitions.get(&res) {
                    plan.temporal.clone()
                } else {
                    warn!(
                        "  {} -- resolution '{}' not in partition plan, using ['year']",
                        path_str, res
                    );
                    vec!["year".to_string()]
                }
            } else {
                vec!["year".to_string()]
            }
        } else {
            // No resolutions discovered -- single partition
            vec!["year".to_string()]
        };

        let (export_outputs, schema) = provider::export::export_series_to_parquet(
            root,
            &path_str,
            &export_dir,
            &temporal_parts,
            captures,
            data_dir,
            provider_ctx,
        )
        .await
        .map_err(|e| {
            tinyfs::Error::Other(format!("export_series_to_parquet '{}': {}", path_str, e))
        })?;

        for field in &schema.fields {
            columns.insert(field.name.clone());
        }

        for (_caps, export_output) in &export_outputs {
            let mut temporal = BTreeMap::new();
            for component in export_output.file.components() {
                let s = component.as_os_str().to_string_lossy();
                if let Some((k, v)) = s.split_once('=')
                    && temporal_parts.contains(&k.to_string())
                {
                    temporal.insert(k.to_string(), v.to_string());
                }
            }

            let base = config.site.base_url.trim_end_matches('/');
            by_key
                .entry(key.clone())
                .or_default()
                .push(shortcodes::ExportedFile {
                    path: path_str.clone(),
                    file: format!("{}/data/{}", base, export_output.file.to_string_lossy()),
                    captures: captures.clone(),
                    temporal,
                    start_time: export_output.start_time.unwrap_or(0),
                    end_time: export_output.end_time.unwrap_or(0),
                });
        }

        info!(
            "  {} -> {} partitioned files ({:?})",
            path_str,
            export_outputs.len(),
            temporal_parts,
        );
    }

    Ok((by_key, columns.into_iter().collect()))
}

/// Run content stages: glob-match markdown data files, parse frontmatter for metadata.
///
/// Each content stage globs for data files in the pond, reads their frontmatter
/// to extract title/weight/slug/hidden, and returns a `ContentContext` with pages
/// sorted by (weight, title).
async fn run_content_stages(
    config: &SiteConfig,
    root: &tinyfs::WD,
) -> Result<BTreeMap<String, ContentContext>, tinyfs::Error> {
    let mut content = BTreeMap::new();

    for stage in &config.content {
        let matches = root.collect_matches(&stage.pattern).await?;
        info!(
            "Content stage '{}': {} matches for '{}'",
            stage.name,
            matches.len(),
            stage.pattern
        );

        let mut pages = Vec::new();
        for (node_path, _captures) in &matches {
            let path_str = node_path.path.to_string_lossy().to_string();

            // Read the file to extract frontmatter
            let data = root.read_file_path_to_vec(&path_str).await.map_err(|e| {
                tinyfs::Error::Other(format!("Cannot read content file '{}': {}", path_str, e))
            })?;
            let text = String::from_utf8(data).map_err(|e| {
                tinyfs::Error::Other(format!("Non-UTF8 content file '{}': {}", path_str, e))
            })?;

            let (fm_yaml, _body) = split_frontmatter(&text);
            let fm: Frontmatter = if fm_yaml.is_empty() {
                Frontmatter {
                    title: slug_from_path(&path_str),
                    layout: default_layout(),
                    weight: default_weight(),
                    slug: None,
                    hidden: false,
                    section: None,
                    date: None,
                    summary: None,
                    image: None,
                }
            } else {
                serde_yaml::from_str(&fm_yaml).map_err(|e| {
                    tinyfs::Error::Other(format!(
                        "Bad frontmatter in content file '{}': {}",
                        path_str, e
                    ))
                })?
            };

            let slug = fm.slug.unwrap_or_else(|| slug_from_path(&path_str));

            pages.push(ContentPage {
                title: if fm.title.is_empty() {
                    slug.clone()
                } else {
                    fm.title
                },
                slug,
                weight: fm.weight,
                hidden: fm.hidden,
                section: fm.section,
                source_path: path_str,
                date: fm.date,
                summary: fm.summary,
                image: fm.image,
                url_path: String::new(),
            });
        }

        // Sort by (weight, title) for stable ordering
        pages.sort_by(|a, b| a.weight.cmp(&b.weight).then_with(|| a.title.cmp(&b.title)));

        info!(
            "Content stage '{}': {} pages resolved",
            stage.name,
            pages.len()
        );

        content.insert(stage.name.clone(), ContentContext { pages });
    }

    Ok(content)
}

/// Derive a URL slug from a pond path (filename without extension).
///
/// e.g. "/pages/water-system.md" -> "water-system"
fn slug_from_path(path: &str) -> String {
    let filename = path.rsplit('/').next().unwrap_or(path);
    filename
        .strip_suffix(".md")
        .or_else(|| filename.strip_suffix(".markdown"))
        .unwrap_or(filename)
        .to_string()
}

/// Copy static assets to the output directory, preserving path structure.
///
/// Each key in `assets` is a pond path like "/img/photo.jpg". The leading
/// "/" is stripped so the file is written to `output_dir/img/photo.jpg`.
fn copy_static_assets(
    assets: &BTreeMap<String, Vec<u8>>,
    output_dir: &Path,
) -> Result<(), tinyfs::Error> {
    for (path, data) in assets {
        // Strip leading slash to get a relative path
        let rel = path.strip_prefix('/').unwrap_or(path);
        let out_path = output_dir.join(rel);
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent).map_other_context(format!("mkdir {:?}", parent))?;
        }
        std::fs::write(&out_path, data).map_other_context(format!("write {:?}", out_path))?;
        debug!("copied static asset: {}", rel);
    }
    Ok(())
}

/// Write shared build assets (base CSS, JS, vendor) to the output directory.
///
/// These are compiled into the binary via `include_str!` so sitegen always
/// produces a self-contained site without requiring files in the pond.
/// Written once at the top-level output directory; subsites reference them
/// via root-relative paths.
fn write_shared_assets(output_dir: &Path) -> Result<(), tinyfs::Error> {
    static STYLE_CSS: &str = include_str!("../assets/style.css");
    static CHART_JS: &str = include_str!("../assets/chart.js");
    static DUCKDB_SHARED_JS: &str = include_str!("../assets/duckdb-shared.js");
    static EXPLORE_JS: &str = include_str!("../assets/explore.js");
    static VEGA_SHARED_JS: &str = include_str!("../assets/vega-shared.js");
    static OVERLAY_JS: &str = include_str!("../assets/overlay.js");
    static LOG_VIEWER_JS: &str = include_str!("../assets/log-viewer.js");
    static LIMITERS_JS: &str = include_str!("../assets/limiters.js");
    static RELATIVE_TIME_JS: &str = include_str!("../assets/relative-time.js");

    for (name, content) in [
        ("style.css", STYLE_CSS),
        ("chart.js", CHART_JS),
        ("duckdb-shared.js", DUCKDB_SHARED_JS),
        ("explore.js", EXPLORE_JS),
        ("vega-shared.js", VEGA_SHARED_JS),
        ("overlay.js", OVERLAY_JS),
        ("log-viewer.js", LOG_VIEWER_JS),
        ("limiters.js", LIMITERS_JS),
        ("relative-time.js", RELATIVE_TIME_JS),
    ] {
        let path = output_dir.join(name);
        std::fs::write(&path, content.as_bytes()).map_other_context(format!("write {:?}", path))?;
        debug!("wrote shared asset: {}", name);
    }

    // Copy vendor files (DuckDB-WASM, Observable Plot, D3) for offline use.
    // These are downloaded by vendor/download.sh and are not embedded in the
    // binary due to their size (~35MB, dominated by the DuckDB WASM binary).
    copy_vendor_assets(output_dir)?;

    Ok(())
}

/// Write per-site theme.css with CSS custom property overrides.
///
/// Each site (top-level and each subsite) gets its own theme.css.
/// If no theme overrides are configured, writes an empty comment
/// so the `<link>` does not 404.
fn write_theme_css(
    output_dir: &Path,
    theme: &std::collections::BTreeMap<String, String>,
) -> Result<(), tinyfs::Error> {
    let css = if theme.is_empty() {
        "/* No theme overrides */\n".to_string()
    } else {
        let mut css = String::from("/* Site theme overrides */\n:root {\n");
        for (key, value) in theme {
            css.push_str(&format!("  --{}: {};\n", key, value));
        }
        css.push_str("}\n");
        css
    };

    let path = output_dir.join("theme.css");
    std::fs::write(&path, css.as_bytes()).map_other_context(format!("write {:?}", path))?;
    debug!("wrote theme.css ({} overrides)", theme.len());
    Ok(())
}

/// Vendor files required for the generated site to function offline.
/// These are downloaded by vendor/download.sh.
const VENDOR_FILES: &[&str] = &[
    "duckdb-browser.mjs",
    "duckdb-browser-eh.worker.js",
    "duckdb-eh.wasm",
    "vega-bundle.mjs",
];

/// Pre-compressed variants of vendor files (brotli + gzip).
/// These are optional — generated by download.sh when brotli/gzip are
/// available. Caddy or nginx can serve these via precompressed/gzip_static
/// directives for significant bandwidth savings.
const VENDOR_FILES_COMPRESSED: &[&str] = &[
    "duckdb-browser.mjs.br",
    "duckdb-browser.mjs.gz",
    "duckdb-browser-eh.worker.js.br",
    "duckdb-browser-eh.worker.js.gz",
    "duckdb-eh.wasm.br",
    "duckdb-eh.wasm.gz",
    "vega-bundle.mjs.br",
    "vega-bundle.mjs.gz",
];

/// Copy vendor files from the vendor/dist/ directory to the output site.
///
/// Searches for vendor files in these locations (first match wins):
///   1. WATERTOWN_VENDOR environment variable
///   2. vendor/dist/ relative to the sitegen crate (development)
///   3. /usr/local/share/watertown/vendor/ (Docker / installed)
///   4. ~/.cache/watertown/vendor/ (user cache)
///
/// If no vendor directory is found, warns but does not error -- the site
/// will not function without network access in that case.
fn copy_vendor_assets(output_dir: &Path) -> Result<(), tinyfs::Error> {
    let vendor_dir = find_vendor_dir();

    let vendor_dir = match vendor_dir {
        Some(d) => d,
        None => {
            log::warn!(
                "Vendor files not found. Generated site requires network access. \
                 Run: cd crates/sitegen/vendor && bash download.sh"
            );
            return Ok(());
        }
    };

    let out_vendor = output_dir.join("vendor");
    std::fs::create_dir_all(&out_vendor).map_other_context(format!("mkdir {:?}", out_vendor))?;

    // Copy required vendor files (must all exist)
    for name in VENDOR_FILES {
        let src = vendor_dir.join(name);
        let dst = out_vendor.join(name);
        std::fs::copy(&src, &dst).map_err(|e| {
            tinyfs::Error::Other(format!("copy vendor {:?} -> {:?}: {}", src, dst, e))
        })?;
    }

    // Copy pre-compressed variants if they exist (optional)
    let mut compressed_count = 0;
    for name in VENDOR_FILES_COMPRESSED {
        let src = vendor_dir.join(name);
        if src.exists() {
            let dst = out_vendor.join(name);
            std::fs::copy(&src, &dst).map_err(|e| {
                tinyfs::Error::Other(format!("copy vendor {:?} -> {:?}: {}", src, dst, e))
            })?;
            compressed_count += 1;
        }
    }

    info!(
        "Copied {} vendor files ({} compressed) to {}/vendor/",
        VENDOR_FILES.len(),
        compressed_count,
        output_dir.display()
    );
    Ok(())
}

fn find_vendor_dir() -> Option<std::path::PathBuf> {
    // 1. Explicit env var
    if let Ok(path) = std::env::var("WATERTOWN_VENDOR") {
        let p = std::path::PathBuf::from(path);
        if p.join("duckdb-eh.wasm").exists() {
            return Some(p);
        }
    }

    // 2. Development: vendor/dist/ relative to the crate source
    //    (works when running from the repo checkout)
    let crate_vendor = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("vendor/dist");
    if crate_vendor.join("duckdb-eh.wasm").exists() {
        return Some(crate_vendor);
    }

    // 3. Installed location (Docker image, system install)
    let system = std::path::Path::new("/usr/local/share/watertown/vendor");
    if system.join("duckdb-eh.wasm").exists() {
        return Some(system.to_path_buf());
    }

    // 3b. FHS install location (cargo-deb-built .deb installs here,
    //     since /usr/share/ is the standard path for arch-independent
    //     read-only data on Debian/Ubuntu).
    let fhs = std::path::Path::new("/usr/share/watertown/vendor");
    if fhs.join("duckdb-eh.wasm").exists() {
        return Some(fhs.to_path_buf());
    }

    // 4. User cache
    if let Ok(home) = std::env::var("HOME") {
        let cache = std::path::PathBuf::from(home).join(".cache/watertown/vendor");
        if cache.join("duckdb-eh.wasm").exists() {
            return Some(cache);
        }
    }

    None
}

/// Convert a pond series path to a relative directory path for export.
///
/// e.g. "/reduced/single_param/Temperature/res=1h.series"
///    -> "single_param/Temperature/res=1h"
///
/// The first path component (e.g. "reduced") is stripped, and the
/// `.series` extension is removed. The result is used as a subdirectory
/// under the `data/` output directory.
fn series_path_to_data_rel(pond_path: &str) -> String {
    let stripped = pond_path.trim_start_matches('/');
    let parts: Vec<&str> = stripped.splitn(2, '/').collect();
    let rel = if parts.len() > 1 { parts[1] } else { stripped };
    rel.strip_suffix(".series").unwrap_or(rel).to_string()
}

register_executable_factory!(
    name: "sitegen",
    description: "Static site generator -- Markdown + Maud templates powered by Maudit",
    validate: validate_config,
    initialize: initialize,
    execute: execute
);

// ---------------------------------------------------------------------------
// Site generation
// ---------------------------------------------------------------------------

/// Frontmatter parsed from the top of each markdown page.
#[derive(Debug, Deserialize)]
struct Frontmatter {
    #[serde(default)]
    title: String,
    #[serde(default = "default_layout")]
    layout: String,
    /// Sort weight for content pages (lower = higher in nav). Default: 100
    #[serde(default = "default_weight")]
    weight: i32,
    /// URL slug override. If absent, derived from filename.
    #[serde(default)]
    slug: Option<String>,
    /// If true, page renders but does not appear in navigation.
    #[serde(default)]
    hidden: bool,
    /// Navigation section for grouping (e.g., "About", "Blog").
    #[serde(default)]
    section: Option<String>,
    /// Publication date (ISO 8601, e.g. "2024-06-15").
    #[serde(default)]
    date: Option<String>,
    /// Short summary for blog card display.
    #[serde(default)]
    summary: Option<String>,
    /// Image URL for blog card hero image.
    #[serde(default)]
    image: Option<String>,
}

fn default_layout() -> String {
    "default".to_string()
}

fn default_weight() -> i32 {
    100
}

/// Generate the complete static site.
#[allow(clippy::too_many_arguments)]
fn generate_site(
    config: &SiteConfig,
    exports: &BTreeMap<String, ExportContext>,
    content: &BTreeMap<String, ContentContext>,
    read_pond_file: &dyn Fn(&str) -> Result<String, String>,
    output_dir: &Path,
    root_base_url: &str,
    pond_statuses: Option<Vec<PondStatus>>,
    generated_at: &str,
) -> Result<(), GenerateError> {
    // Compute feed URL if site_url is configured
    let feed_url = if config.site.site_url.is_some() {
        let base = config.site.base_url.trim_end_matches('/');
        Some(format!("{}/feed.xml", base))
    } else {
        None
    };

    // Expand the route tree into page jobs
    let expansion = routes::expand_routes(config, exports, content);
    let jobs = expansion.jobs;
    let content_urls = expansion.content_urls;
    info!("Route expansion: {} pages to generate", jobs.len());

    // Build collections for navigation shortcodes
    let collections = routes::build_collections(exports, content);

    // Resolve data-explorer datasets once: map each configured explore dataset
    // to its export stage's flattened files + unioned columns. Shared across all
    // pages (the explore shortcode picks them up via ShortcodeContext).
    let explore_datasets = resolve_explore_datasets(config, exports);

    // Site-relative explorer page URL (base_url-prefixed) for the chart
    // cross-link. Emitted on chart pages as `data-explore-url`.
    let explore_url: Option<String> = config
        .explore
        .as_ref()
        .and_then(|e| e.url.as_deref())
        .filter(|u| !u.is_empty())
        .map(|u| {
            // Follow the asset-path convention: base_url ends with "/" and the
            // configured value is joined relative to it. Tolerate a leading
            // slash on the config value so "/explore/" and "explore/" both work.
            let rel = u.strip_prefix('/').unwrap_or(u);
            format!("{}{}", config.site.base_url, rel)
        });

    // Build content_pages map for content_nav shortcode.
    // Includes both explicit content pages and synthetic entries from
    // template routes so that export-based pages are navigable.
    // Stamp each page's `url_path` with the canonical URL discovered
    // during route expansion so shortcodes/feeds build correct links
    // regardless of where a content stage is mounted.
    let mut content_pages: BTreeMap<String, Vec<ContentPage>> = content
        .iter()
        .map(|(name, ctx)| {
            let urls = content_urls.get(name);
            let pages: Vec<ContentPage> = ctx
                .pages
                .iter()
                .map(|p| {
                    let mut p = p.clone();
                    p.url_path = urls
                        .and_then(|m| m.get(&p.source_path).cloned())
                        .unwrap_or_else(|| {
                            log::warn!(
                                "Content page '{}' in collection '{}' is not bound to any content route; falling back to '/{}' for links",
                                p.source_path,
                                name,
                                p.slug
                            );
                            format!("/{}", p.slug)
                        });
                    p
                })
                .collect();
            (name.clone(), pages)
        })
        .collect();
    for (name, pages) in routes::build_template_content_pages(config, exports) {
        content_pages.entry(name).or_default().extend(pages);
    }

    // Render each page
    for job in &jobs {
        // Render sidebar per-page so nav_list can highlight the active link
        let base = config.site.base_url.trim_end_matches('/');
        let current_path = if base.is_empty() || base == "/" {
            format!("/{}", job.output_path)
        } else {
            format!("{}/{}", base, job.output_path)
        };
        let sidebar_html = render_partial(
            config,
            "sidebar",
            read_pond_file,
            &collections,
            &content_pages,
            &current_path,
            &pond_statuses,
            generated_at,
        );
        let raw_md = read_pond_file(&job.page_source)
            .map_err(|e| GenerateError(format!("Cannot read '{}': {}", job.page_source, e)))?;

        let (fm_yaml, body) = split_frontmatter(&raw_md);
        let fm: Frontmatter = if fm_yaml.is_empty() {
            Frontmatter {
                title: job.page_source.clone(),
                layout: default_layout(),
                weight: default_weight(),
                slug: None,
                hidden: false,
                section: None,
                date: None,
                summary: None,
                image: None,
            }
        } else {
            serde_yaml::from_str(&fm_yaml).map_err(|e| {
                GenerateError(format!("Bad frontmatter in '{}': {}", job.page_source, e))
            })?
        };

        // Interpolate capture groups into the title
        let title = interpolate_captures(&fm.title, &job.captures, &config.labels);

        // Build shortcode context for this page
        let sc_ctx = Arc::new(ShortcodeContext {
            captures: job.captures.clone(),
            datafiles: job.datafiles.clone(),
            collections: collections.clone(),
            content_pages: content_pages.clone(),
            site_title: config.site.title.clone(),
            current_path: current_path.clone(),
            breadcrumbs: job.breadcrumbs.clone(),
            base_url: config.site.base_url.clone(),
            sidebar_sections: config.sidebar.clone(),
            metric_registry: config.metric_registry.clone(),
            metric_captions: config.metric_captions.clone(),
            pond_statuses: pond_statuses.clone(),
            generated_at: generated_at.to_string(),
            default_range: config.site.default_range.clone(),
            labels: config.labels.clone(),
            explore_datasets: explore_datasets.clone(),
            explore_url: explore_url.clone(),
            annotations: job.annotations.clone(),
        });

        // Rewrite {{ $0 }} -> {{ cap0 }}, nav-list -> nav_list, etc.
        let preprocessed = shortcodes::preprocess_variables(&body);

        // Expand shortcodes
        let sc = shortcodes::register_shortcodes(sc_ctx);
        let expanded =
            preprocess_shortcodes(&preprocessed, &sc, Some(&job.page_source)).map_err(|e| {
                GenerateError(format!("Shortcode error in '{}': {}", job.page_source, e))
            })?;

        // Render markdown -> HTML
        let content_html = render_markdown(&expanded);

        // Rewrite absolute URLs to include base_url prefix
        let content_html =
            crate::markdown::rewrite_absolute_urls(&content_html, &config.site.base_url);

        // Wrap in layout
        let full_html = layouts::apply_layout(
            &fm.layout,
            &LayoutContext {
                title: &title,
                site_title: &config.site.title,
                base_url: &config.site.base_url,
                root_base_url,
                content: &content_html,
                sidebar: sidebar_html.as_deref(),
                date: fm.date.as_deref(),
                feed_url: feed_url.as_deref(),
                github_url: config.site.github_url.as_deref(),
                footer: config.footer.as_deref(),
                header: config.header.as_deref(),
            },
        );

        // Write output file
        let out_path = output_dir.join(&job.output_path);
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| GenerateError(format!("mkdir {:?}: {}", parent, e)))?;
        }
        std::fs::write(&out_path, full_html.as_bytes())
            .map_err(|e| GenerateError(format!("write {:?}: {}", out_path, e)))?;

        debug!("wrote {}", job.output_path);
    }

    // Generate RSS feed if site_url is configured
    if config.site.site_url.is_some() {
        generate_feed(config, &content_pages, output_dir)?;
    } else {
        info!("RSS feed skipped: site.site_url not configured");
    }

    info!(
        "Site generation complete: {} pages to {:?}",
        jobs.len(),
        output_dir
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// RSS Feed Generation
// ---------------------------------------------------------------------------

/// Generate an RSS 2.0 feed from blog content pages.
///
/// Filters content pages by section (default "Blog"), sorts by date
/// descending, and writes `feed.xml` to the output directory.
fn generate_feed(
    config: &SiteConfig,
    content_pages: &BTreeMap<String, Vec<ContentPage>>,
    output_dir: &Path,
) -> Result<(), GenerateError> {
    let site_url = config
        .site
        .site_url
        .as_deref()
        .ok_or_else(|| GenerateError("RSS feed requires site.site_url".to_string()))?
        .trim_end_matches('/');

    let feed_config = config.feed.as_ref();
    let section = feed_config.map(|f| f.section.as_str()).unwrap_or("Blog");
    let description = feed_config
        .and_then(|f| f.description.as_deref())
        .unwrap_or(&config.site.title);

    // Find the content stage to use
    let content_name = feed_config
        .and_then(|f| f.content.as_deref())
        .or_else(|| content_pages.keys().next().map(|s| s.as_str()));

    let pages = match content_name {
        Some(name) => match content_pages.get(name) {
            Some(pages) => pages,
            None => {
                warn!("RSS feed: content stage '{}' not found, skipping", name);
                return Ok(());
            }
        },
        None => {
            warn!("RSS feed: no content stages configured, skipping");
            return Ok(());
        }
    };

    // Filter to the target section, exclude hidden pages, require dates
    let mut blog_posts: Vec<&ContentPage> = pages
        .iter()
        .filter(|p| !p.hidden)
        .filter(|p| p.section.as_deref() == Some(section))
        .filter(|p| p.date.is_some())
        .collect();

    // Sort by date descending (newest first)
    blog_posts.sort_by(|a, b| b.date.cmp(&a.date));

    let base = config.site.base_url.trim_end_matches('/');
    let feed_link = format!("{}{}/feed.xml", site_url, base);

    let items: Vec<rss::Item> = blog_posts
        .iter()
        .map(|page| {
            // Use the canonical URL recorded by route expansion so that
            // posts mounted under a nested route (e.g. /blog/<slug>)
            // get the correct link, falling back to the bare slug for
            // pages outside the route tree.
            let path = if page.url_path.is_empty() {
                format!("/{}", page.slug)
            } else {
                page.url_path.clone()
            };
            let link = format!("{}{}{}.html", site_url, base, path);
            let pub_date = page.date.as_deref().and_then(iso_date_to_rfc2822);

            let mut item = rss::Item::default();
            item.set_title(page.title.clone());
            item.set_link(link.clone());
            item.set_guid(rss::Guid {
                value: link,
                permalink: true,
            });
            if let Some(ref summary) = page.summary {
                item.set_description(summary.clone());
            }
            if let Some(date) = pub_date {
                item.set_pub_date(date);
            }
            item
        })
        .collect();

    let channel = rss::Channel {
        title: config.site.title.clone(),
        link: site_url.to_string(),
        description: description.to_string(),
        items,
        ..Default::default()
    };

    let feed_path = output_dir.join("feed.xml");
    let xml = channel.to_string();
    std::fs::write(&feed_path, xml.as_bytes())
        .map_err(|e| GenerateError(format!("write {:?}: {}", feed_path, e)))?;

    info!(
        "RSS feed: {} items written to feed.xml (section='{}')",
        blog_posts.len(),
        section
    );
    let _ = feed_link;
    Ok(())
}

/// Convert an ISO 8601 date (YYYY-MM-DD) to RFC 2822 format for RSS pubDate.
///
/// Uses noon UTC as the time component since blog posts only have date precision.
fn iso_date_to_rfc2822(iso: &str) -> Option<String> {
    let date = chrono::NaiveDate::parse_from_str(iso, "%Y-%m-%d").ok()?;
    let datetime = date.and_hms_opt(12, 0, 0)?;
    let utc = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(datetime, chrono::Utc);
    Some(utc.to_rfc2822())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn render_partial(
    config: &SiteConfig,
    name: &str,
    read_pond_file: &dyn Fn(&str) -> Result<String, String>,
    collections: &BTreeMap<String, Vec<String>>,
    content_pages: &BTreeMap<String, Vec<ContentPage>>,
    current_path: &str,
    pond_statuses: &Option<Vec<PondStatus>>,
    generated_at: &str,
) -> Option<String> {
    let path = config.partials.get(name)?;
    match read_pond_file(path) {
        Ok(md) => {
            // Build a shortcode context for the partial -- includes current_path
            // so nav_list can highlight the active page link.
            let sc_ctx = Arc::new(ShortcodeContext {
                captures: vec![],
                datafiles: vec![],
                collections: collections.clone(),
                content_pages: content_pages.clone(),
                site_title: config.site.title.clone(),
                current_path: current_path.to_string(),
                breadcrumbs: vec![],
                base_url: config.site.base_url.clone(),
                sidebar_sections: config.sidebar.clone(),
                metric_registry: config.metric_registry.clone(),
                metric_captions: config.metric_captions.clone(),
                pond_statuses: pond_statuses.clone(),
                generated_at: generated_at.to_string(),
                default_range: config.site.default_range.clone(),
                labels: config.labels.clone(),
                explore_datasets: vec![],
                explore_url: None,
                annotations: vec![],
            });
            let preprocessed = shortcodes::preprocess_variables(&md);
            let sc = shortcodes::register_shortcodes(sc_ctx);
            let expanded =
                preprocess_shortcodes(&preprocessed, &sc, Some(path)).unwrap_or(preprocessed);
            Some(render_markdown(&expanded))
        }
        Err(e) => {
            warn!("Cannot read partial '{}' from '{}': {}", name, path, e);
            None
        }
    }
}

fn interpolate_captures(
    text: &str,
    captures: &[String],
    labels: &std::collections::BTreeMap<String, String>,
) -> String {
    let mut result = text.to_string();
    for (i, val) in captures.iter().enumerate() {
        let display = labels.get(val).map(String::as_str).unwrap_or(val);
        result = result.replace(&format!("{{{{ ${} }}}}", i), display);
    }
    result
}

fn split_frontmatter(content: &str) -> (String, String) {
    let trimmed = content.trim_start();
    if !trimmed.starts_with("---") {
        return (String::new(), content.to_string());
    }
    let after = &trimmed[3..];
    if let Some(end) = after.find("\n---") {
        (
            after[..end].trim().to_string(),
            after[end + 4..].to_string(),
        )
    } else {
        (String::new(), content.to_string())
    }
}

#[derive(Debug)]
struct GenerateError(String);

impl std::fmt::Display for GenerateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for GenerateError {}

#[cfg(test)]
mod tests {
    use super::*;

    /// Register an in-memory journal table (`__REALTIME_TIMESTAMP` + `MESSAGE`,
    /// both Utf8, mirroring the jsonlogs schema) from `(ts_us, message)` rows.
    async fn register_journal_table(
        ctx: &datafusion::prelude::SessionContext,
        table: &str,
        rows: &[(i64, &str)],
    ) {
        use datafusion::arrow::array::StringArray;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("__REALTIME_TIMESTAMP", DataType::Utf8, true),
            Field::new("MESSAGE", DataType::Utf8, true),
        ]));
        let ts: StringArray = rows.iter().map(|(t, _)| Some(t.to_string())).collect();
        let msg: StringArray = rows.iter().map(|(_, m)| Some(m.to_string())).collect();
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ts), Arc::new(msg)]).unwrap();
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table(
            datafusion::sql::TableReference::bare(table),
            Arc::new(provider),
        )
        .unwrap();
    }

    /// The `_msg` fields must be paired to the row holding the matching max
    /// timestamp, not the lexicographically-greatest message. Rows are seeded so
    /// the newest ok/err lines sort *before* older ones lexicographically, which
    /// would trip the old independent-`MAX(MESSAGE)` bug.
    #[tokio::test]
    async fn status_msg_paired_to_latest_timestamp() {
        let ctx = datafusion::prelude::SessionContext::new();
        register_journal_table(
            &ctx,
            "jt",
            &[
                // older ok, lexicographically GREATER (starts with 'Z')
                (100, "Run summary Z older outcome=ok"),
                // newest ok, lexicographically smaller (starts with 'A')
                (200, "Run summary A newest outcome=ok"),
                // older err, lexicographically greater
                (150, "Run summary Z older outcome=err"),
                // newest err, lexicographically smaller
                (250, "Run summary A newest outcome=err"),
                // two peak lines; newest should win
                (300, "Peak memory usage: 10.0 MB"),
                (120, "Peak memory usage: 99.0 MB"),
                // an unrelated newer line -> last_seen must reflect overall max
                (400, "some unrelated log line"),
            ],
        )
        .await;

        let s = query_pond_status(&ctx, "jt", "unit@x.service", 10)
            .await
            .expect("query_pond_status");

        assert_eq!(s.last_seen_us, Some(400));
        assert_eq!(s.last_ok_us, Some(200));
        assert_eq!(
            s.last_ok_msg.as_deref(),
            Some("Run summary A newest outcome=ok")
        );
        assert_eq!(s.last_err_us, Some(250));
        assert_eq!(
            s.last_err_msg.as_deref(),
            Some("Run summary A newest outcome=err")
        );
        // Newest peak line (ts=300) wins over the lexicographically/numerically
        // larger-valued older one (ts=120).
        assert_eq!(s.peak_rss_bytes, Some((10.0 * 1024.0 * 1024.0) as u64));
    }

    /// A unit with no ok/err/peak lines leaves those fields None (empty-set
    /// aggregates), while `last_seen` still reflects the max timestamp.
    #[tokio::test]
    async fn status_absent_fields_are_none() {
        let ctx = datafusion::prelude::SessionContext::new();
        register_journal_table(&ctx, "jt2", &[(500, "just a heartbeat line")]).await;

        let s = query_pond_status(&ctx, "jt2", "unit@y.service", 10)
            .await
            .expect("query_pond_status");

        assert_eq!(s.last_seen_us, Some(500));
        assert_eq!(s.last_ok_us, None);
        assert_eq!(s.last_ok_msg, None);
        assert_eq!(s.last_err_us, None);
        assert_eq!(s.last_err_msg, None);
        assert_eq!(s.peak_rss_bytes, None);
    }

    #[test]
    fn extract_pond_name_user_pond() {
        assert_eq!(
            extract_pond_name("user-pond@water-staging.service"),
            Some("water-staging")
        );
    }

    #[test]
    fn extract_pond_name_user_pond_selfmon() {
        assert_eq!(
            extract_pond_name("user-pond-selfmon@watershop-selfmon.service"),
            Some("watershop-selfmon")
        );
    }

    #[test]
    fn extract_pond_name_pond_without_user_prefix() {
        // System-scope variants (no `user-` prefix) should also work
        // for forward compatibility with non-user systemd contexts.
        assert_eq!(
            extract_pond_name("pond@noyo-prod.service"),
            Some("noyo-prod")
        );
        assert_eq!(
            extract_pond_name("pond-selfmon@something.service"),
            Some("something")
        );
    }

    #[test]
    fn extract_pond_name_unknown_returns_none() {
        assert_eq!(extract_pond_name("caddy.service"), None);
        assert_eq!(extract_pond_name("init.scope"), None);
        assert_eq!(extract_pond_name(""), None);
    }

    #[test]
    fn extract_pond_name_handles_missing_service_suffix() {
        // Some emitters may strip the `.service` suffix already; the
        // helper should still return the bare pond name.
        assert_eq!(
            extract_pond_name("user-pond@water-staging"),
            Some("water-staging")
        );
    }

    /// End-to-end incremental fold across two "builds" (design §6 acceptance):
    /// a unit that erred in build #1 and then only emits unrelated heartbeat
    /// lines must keep its `last_err_msg` in build #2, even though the steady-
    /// state version window no longer contains the error line.
    #[tokio::test]
    async fn incremental_fold_keeps_silent_unit_error_card() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path();
        let unit = "user-pond@water-staging.service";
        let tail_lines = 5;

        // ---- Build #1: backfill window contains the ok + err lines. ----
        let ctx = datafusion::prelude::SessionContext::new();
        register_journal_table(
            &ctx,
            "jt_b1",
            &[
                (100, "Run summary A outcome=ok"),
                (200, "Run summary B outcome=err"),
                (250, "Peak memory usage: 12.0 MB"),
            ],
        )
        .await;
        let fold1 = fold_unit_versions(&ctx, "jt_b1", tail_lines).await.unwrap();
        let prior = status_summary::load(cache_dir, unit)
            .unwrap()
            .unwrap_or_default();
        // First fold seeds from an empty prior; watermark advances to 1.
        let merged1 = status_summary::merge(prior, fold1, 1, tail_lines);
        status_summary::save(cache_dir, unit, &merged1).unwrap();
        assert_eq!(merged1.last_err_us, Some(200));
        assert_eq!(
            merged1.last_err_msg.as_deref(),
            Some("Run summary B outcome=err")
        );

        // ---- Build #2: steady-state version window has ONLY newer heartbeat
        // lines (the error scrolled out of the version_gt window). ----
        let ctx2 = datafusion::prelude::SessionContext::new();
        register_journal_table(
            &ctx2,
            "jt_b2",
            &[(400, "just a heartbeat"), (500, "another heartbeat")],
        )
        .await;
        let fold2 = fold_unit_versions(&ctx2, "jt_b2", tail_lines)
            .await
            .unwrap();
        let prior2 = status_summary::load(cache_dir, unit).unwrap().unwrap();
        assert_eq!(prior2.processed_version, 1);
        let merged2 = status_summary::merge(prior2, fold2, 2, tail_lines);
        status_summary::save(cache_dir, unit, &merged2).unwrap();

        // The durable summary still carries the error from build #1...
        assert_eq!(merged2.last_err_us, Some(200));
        assert_eq!(
            merged2.last_err_msg.as_deref(),
            Some("Run summary B outcome=err")
        );
        // ...and the ok/peak fields, while last_seen advanced to the newest line.
        assert_eq!(merged2.last_ok_us, Some(100));
        assert_eq!(merged2.last_peak_us, Some(250));
        assert_eq!(merged2.last_seen_us, Some(500));
        assert_eq!(merged2.processed_version, 2);

        // Rendered card reflects the preserved error.
        let status = summary_to_pond_status(&merged2, unit);
        assert_eq!(status.last_err_us, Some(200));
        assert_eq!(
            status.last_err_msg.as_deref(),
            Some("Run summary B outcome=err")
        );
        assert_eq!(status.peak_rss_bytes, Some((12.0 * 1024.0 * 1024.0) as u64));
        // Tail merged across builds, newest-first render is oldest->newest.
        assert_eq!(
            status.tail_messages.last().map(String::as_str),
            Some("another heartbeat")
        );
    }

    #[test]
    fn test_split_frontmatter() {
        let (fm, body) = split_frontmatter("---\ntitle: Hi\n---\n\n# Body");
        assert_eq!(fm, "title: Hi");
        assert!(body.contains("# Body"));
    }

    #[test]
    fn test_split_frontmatter_none() {
        let (fm, body) = split_frontmatter("# Just markdown");
        assert!(fm.is_empty());
        assert_eq!(body, "# Just markdown");
    }

    #[test]
    fn test_interpolate_captures() {
        let labels = std::collections::BTreeMap::new();
        assert_eq!(
            interpolate_captures("{{ $0 }} data", &["Temperature".to_string()], &labels),
            "Temperature data"
        );
    }

    #[test]
    fn test_interpolate_captures_with_label() {
        let mut labels = std::collections::BTreeMap::new();
        labels.insert("DO".to_string(), "Dissolved Oxygen".to_string());
        assert_eq!(
            interpolate_captures("{{ $0 }}", &["DO".to_string()], &labels),
            "Dissolved Oxygen"
        );
        // A capture with no label entry falls back to the raw value.
        assert_eq!(
            interpolate_captures("{{ $0 }}", &["Salinity".to_string()], &labels),
            "Salinity"
        );
    }

    #[test]
    fn test_interpolate_empty() {
        let labels = std::collections::BTreeMap::new();
        assert_eq!(interpolate_captures("Static", &[], &labels), "Static");
    }

    #[test]
    fn test_slug_from_path() {
        assert_eq!(slug_from_path("/pages/water-system.md"), "water-system");
        assert_eq!(slug_from_path("/content/history.md"), "history");
        assert_eq!(slug_from_path("readme.markdown"), "readme");
        assert_eq!(slug_from_path("/site/index.md"), "index");
    }

    #[test]
    fn test_frontmatter_with_weight() {
        let yaml = "title: Water\nweight: 10\nhidden: true";
        let fm: Frontmatter = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(fm.title, "Water");
        assert_eq!(fm.weight, 10);
        assert!(fm.hidden);
        assert!(fm.slug.is_none());
        assert_eq!(fm.layout, "default");
    }

    #[test]
    fn test_iso_date_to_rfc2822() {
        let result = iso_date_to_rfc2822("2025-03-10").unwrap();
        assert!(
            result.contains("10 Mar 2025"),
            "Expected RFC 2822: {}",
            result
        );
        assert!(result.contains("12:00:00"), "Expected noon: {}", result);
    }

    #[test]
    fn test_iso_date_to_rfc2822_invalid() {
        assert!(iso_date_to_rfc2822("not-a-date").is_none());
        assert!(iso_date_to_rfc2822("").is_none());
    }

    #[test]
    fn test_generate_feed() {
        use crate::config::{FeedConfig, SiteMeta};
        use tempfile::TempDir;

        let output_dir = TempDir::new().unwrap();
        let config = SiteConfig {
            site: SiteMeta {
                title: "Test Blog".to_string(),
                base_url: "/".to_string(),
                site_url: Some("https://example.com".to_string()),
                github_url: None,
                default_range: None,
            },
            content: vec![],
            exports: vec![],
            routes: vec![],
            partials: BTreeMap::new(),
            static_assets: vec![],
            sidebar: vec![],
            feed: Some(FeedConfig {
                section: "Blog".to_string(),
                content: Some("pages".to_string()),
                description: Some("A test blog".to_string()),
            }),
            theme: std::collections::BTreeMap::new(),
            subsites: vec![],
            footer: None,
            header: None,
            metric_registry: std::collections::BTreeMap::new(),
            metric_captions: std::collections::BTreeMap::new(),
            labels: std::collections::BTreeMap::new(),
            explore: None,
            status_grid: None,
        };

        let mut content_pages: BTreeMap<String, Vec<ContentPage>> = BTreeMap::new();
        content_pages.insert(
            "pages".to_string(),
            vec![
                ContentPage {
                    title: "First Post".to_string(),
                    slug: "first-post".to_string(),
                    weight: 100,
                    hidden: false,
                    section: Some("Blog".to_string()),
                    source_path: "/content/first-post.md".to_string(),
                    date: Some("2025-01-15".to_string()),
                    summary: Some("My first post".to_string()),
                    image: None,
                    url_path: "/blog/first-post".to_string(),
                },
                ContentPage {
                    title: "Second Post".to_string(),
                    slug: "second-post".to_string(),
                    weight: 100,
                    hidden: false,
                    section: Some("Blog".to_string()),
                    source_path: "/content/second-post.md".to_string(),
                    date: Some("2025-02-20".to_string()),
                    summary: None,
                    image: None,
                    url_path: "/blog/second-post".to_string(),
                },
                ContentPage {
                    title: "Hidden Post".to_string(),
                    slug: "hidden".to_string(),
                    weight: 100,
                    hidden: true,
                    section: Some("Blog".to_string()),
                    source_path: "/content/hidden.md".to_string(),
                    date: Some("2025-03-01".to_string()),
                    summary: None,
                    image: None,
                    url_path: "/blog/hidden".to_string(),
                },
                ContentPage {
                    title: "Not Blog".to_string(),
                    slug: "about".to_string(),
                    weight: 100,
                    hidden: false,
                    section: Some("Main".to_string()),
                    source_path: "/content/about.md".to_string(),
                    date: None,
                    summary: None,
                    image: None,
                    url_path: "/about".to_string(),
                },
            ],
        );

        generate_feed(&config, &content_pages, output_dir.path()).unwrap();

        let feed_path = output_dir.path().join("feed.xml");
        assert!(feed_path.exists(), "feed.xml should be created");

        let xml = std::fs::read_to_string(&feed_path).unwrap();
        assert!(
            xml.contains("<title>Test Blog</title>"),
            "Channel title: {}",
            xml
        );
        assert!(xml.contains("https://example.com"), "Channel link: {}", xml);
        assert!(xml.contains("A test blog"), "Channel description: {}", xml);
        assert!(xml.contains("First Post"), "First post title: {}", xml);
        assert!(xml.contains("Second Post"), "Second post title: {}", xml);
        assert!(
            !xml.contains("Hidden Post"),
            "Hidden post excluded: {}",
            xml
        );
        assert!(!xml.contains("Not Blog"), "Non-blog excluded: {}", xml);
        assert!(
            xml.contains("https://example.com/blog/first-post.html"),
            "First post link uses url_path: {}",
            xml
        );
        assert!(
            xml.contains("https://example.com/blog/second-post.html"),
            "Second post link uses url_path: {}",
            xml
        );
        assert!(xml.contains("My first post"), "First post summary: {}", xml);
        // Second Post should appear before First Post (date desc)
        let second_pos = xml.find("Second Post").unwrap();
        let first_pos = xml.find("First Post").unwrap();
        assert!(second_pos < first_pos, "Newest post should be first");
    }

    #[test]
    fn test_generate_feed_no_site_url() {
        use crate::config::SiteMeta;
        use tempfile::TempDir;

        let output_dir = TempDir::new().unwrap();
        let config = SiteConfig {
            site: SiteMeta {
                title: "Test".to_string(),
                base_url: "/".to_string(),
                site_url: None,
                github_url: None,
                default_range: None,
            },
            content: vec![],
            exports: vec![],
            routes: vec![],
            partials: BTreeMap::new(),
            static_assets: vec![],
            sidebar: vec![],
            feed: None,
            theme: std::collections::BTreeMap::new(),
            subsites: vec![],
            footer: None,
            header: None,
            metric_registry: std::collections::BTreeMap::new(),
            metric_captions: std::collections::BTreeMap::new(),
            labels: std::collections::BTreeMap::new(),
            explore: None,
            status_grid: None,
        };
        let content: BTreeMap<String, Vec<ContentPage>> = BTreeMap::new();

        // Should error because site_url is missing
        let result = generate_feed(&config, &content, output_dir.path());
        assert!(result.is_err());
    }
}
