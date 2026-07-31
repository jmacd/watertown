// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! TableProvider creation for Watertown
//!
//! This module provides the core logic for creating DataFusion TableProviders from FileID references.
//! It abstracts away persistence implementation details by accepting ProviderContext instead of State.

use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use log::debug;
use std::sync::Arc;
use tinyfs::{FileID, ProviderContext};

use crate::Result;
use crate::{
    TableProviderKey, TableProviderOptions, VersionSelection,
    transform::temporal_filter::TemporalFilteredListingTable,
};

/// Create a TableProvider from a FileID with configurable options
///
/// This is the core table creation function that:
/// 1. Checks cache for existing providers (if no additional_urls)
/// 2. Creates ListingTableConfig from URL pattern(s)
/// 3. Infers schema using DataFusion (merges across versions, skips 0-byte files)
/// 4. Wraps in TemporalFilteredListingTable for data quality filtering
/// 5. Caches result for future queries (if no additional_urls)
///
/// # Arguments
/// * `file_id` - FileID containing node_id and part_id for partition pruning
/// * `context` - ProviderContext for session access, caching, and temporal bounds
/// * `options` - Configuration (version_selection, additional_urls)
///
/// # Returns
/// Arc<dyn TableProvider> ready for DataFusion query execution
///
/// # Example
/// ```ignore
/// use provider::{create_table_provider, TableProviderOptions, VersionSelection};
/// use tinyfs::{FileID, ProviderContext};
///
/// let options = TableProviderOptions {
///     version_selection: VersionSelection::Latest,
///     additional_urls: vec![],
/// };
/// let provider = create_table_provider(file_id, &context, options).await?;
/// ```
pub async fn create_table_provider(
    file_id: FileID,
    context: &ProviderContext,
    options: TableProviderOptions,
) -> Result<Arc<dyn TableProvider>> {
    debug!(
        "create_table_provider called for file_id: {}",
        file_id.node_id()
    );

    // Use centralized debug logging to eliminate duplication
    options.version_selection.log_debug(&file_id.node_id());

    // Check cache first (only for simple cases without additional_urls)
    if options.additional_urls.is_empty() {
        let cache_key = TableProviderKey::with_bounds(
            file_id,
            options.version_selection.clone(),
            options.bounds,
        )
        .to_cache_string();

        if let Some(cached_provider) = context.get_table_provider_cache(&cache_key) {
            debug!(
                "[GO] CACHE HIT: Returning cached TableProvider for file_id: {}",
                file_id.node_id()
            );
            return Ok(cached_provider);
        } else {
            debug!(
                "[SAVE] CACHE MISS: Creating new TableProvider for file_id: {}",
                file_id.node_id()
            );
        }
    } else {
        debug!("[WARN] CACHE BYPASS: additional_urls present, creating fresh TableProvider");
    }

    // Bounded reads list only the version Parquets the bounds can reach,
    // instead of the whole version history. This shares one predicate with the
    // read path and the format cache (`tinyfs::SeriesReadBounds::retains`), so
    // all three prune identically.
    //
    // The prune is a conservative superset: versions with no recorded
    // `max_event_time` are retained, and the caller still applies its own time
    // predicate. When it would retain nothing, the newest live version is kept
    // so the (empty) result still carries a real schema without rescanning
    // history.
    let pruned_urls: Option<Vec<String>> =
        if options.additional_urls.is_empty() && options.bounds != tinyfs::SeriesReadBounds::NONE {
            let versions = context.persistence.list_file_versions(file_id).await?;
            let mut urls: Vec<String> = versions
                .iter()
                .filter(|v| {
                    options.bounds.retains(
                        crate::format_cache::version_max_event_time(v),
                        v.version as i64,
                    )
                })
                .map(|v| crate::TinyFsPathBuilder::url_specific_version(&file_id, v.version))
                .collect();
            if urls.is_empty() {
                if let Some(newest) = versions.iter().max_by_key(|v| v.version) {
                    urls.push(crate::TinyFsPathBuilder::url_specific_version(
                        &file_id,
                        newest.version,
                    ));
                }
            }
            // No versions at all: nothing to prune, fall through to the pattern.
            if urls.is_empty() { None } else { Some(urls) }
        } else {
            None
        };

    // Create ListingTable URL(s) - either from options.additional_urls or pattern generation
    let (config, debug_info) = if let Some(urls) = &pruned_urls {
        let mut table_urls = Vec::with_capacity(urls.len());
        for url_str in urls {
            table_urls.push(ListingTableUrl::parse(url_str)?);
        }

        let file_format = Arc::new(ParquetFormat::default());
        let listing_options = ListingOptions::new(file_format);
        let config = ListingTableConfig::new_with_multi_paths(table_urls)
            .with_listing_options(listing_options);
        (config, format!("bounded: {} version URL(s)", urls.len()))
    } else if options.additional_urls.is_empty() {
        // Default behavior: single URL from pattern
        let url_pattern = options.version_selection.to_url_pattern(&file_id);
        let table_url = ListingTableUrl::parse(&url_pattern)?;

        let file_format = Arc::new(ParquetFormat::default());
        let listing_options = ListingOptions::new(file_format);
        let config = ListingTableConfig::new(table_url).with_listing_options(listing_options);
        (config, format!("single URL: {}", url_pattern))
    } else {
        // Multiple URLs provided via options - use only the provided URLs, not the default pattern
        let mut table_urls = Vec::new();

        // Add only the additional URLs (no default pattern when explicit URLs are provided)
        for url_str in &options.additional_urls {
            table_urls.push(ListingTableUrl::parse(url_str)?);
        }

        let file_format = Arc::new(ParquetFormat::default());
        let listing_options = ListingOptions::new(file_format);
        let config = ListingTableConfig::new_with_multi_paths(table_urls.clone())
            .with_listing_options(listing_options);

        let urls_str: Vec<String> = table_urls.iter().map(|u| u.to_string()).collect();
        (config, format!("multiple URLs: [{}]", urls_str.join(", ")))
    };

    debug!("Creating table provider with {debug_info}");

    // Use DataFusion's schema inference - this will automatically:
    // 1. Iterate through all versions of the file
    // 2. Skip 0-byte files (temporal override metadata-only versions)
    // 3. Merge schemas from all valid Parquet versions
    // 4. Provide the unified schema
    let config_with_schema = config
        .infer_schema(&context.datafusion_session.state())
        .await?;

    let listing_table = ListingTable::try_new(config_with_schema)?;

    // Determine temporal bounds for TemporalFilteredListingTable wrapper
    //
    // NOTE: Temporal bounds are per-FileSeries metadata for data quality filtering
    // (set via `pond set-temporal-bounds`). They filter out garbage data outside the
    // file's valid time range.
    //
    // CURRENT: Only applied to single-file queries via TemporalFilteredListingTable
    // FUTURE: Multi-file queries should have per-file bounds enforced at Parquet scan level
    let (min_time, max_time) = if options.additional_urls.is_empty() {
        // Single-file case: query temporal bounds from persistence layer
        debug!(
            "Querying temporal bounds from persistence for file_id={}",
            file_id
        );

        // Use tinyfs persistence layer through context
        let temporal_overrides = context.persistence.get_temporal_bounds(file_id).await?;

        temporal_overrides.unwrap_or_else(|| {
            debug!(
                "No temporal bounds for FileSeries {} - using unbounded (no data quality filtering)",
                file_id
            );
            (i64::MIN, i64::MAX)
        })
    } else {
        // Multi-file case: no temporal filtering at this level
        // TODO: Implement per-file temporal bounds at Parquet reader level
        debug!(
            "Multi-file query ({} URLs) - temporal bounds enforcement deferred to Parquet reader (not yet implemented)",
            options.additional_urls.len()
        );
        (i64::MIN, i64::MAX)
    };

    debug!("Creating TemporalFilteredListingTable with bounds: {min_time} to {max_time}");

    let table_provider = Arc::new(TemporalFilteredListingTable::new(
        listing_table,
        min_time,
        max_time,
    ));

    log::debug!(
        "[LIST] CREATED TableProvider: file_id={}, temporal_bounds=({}, {}), urls={}",
        file_id,
        min_time,
        max_time,
        debug_info
    );

    // Cache the result (only for simple cases without additional_urls)
    if options.additional_urls.is_empty() {
        let cache_key = TableProviderKey::with_bounds(
            file_id,
            options.version_selection.clone(),
            options.bounds,
        )
        .to_cache_string();

        context.set_table_provider_cache(cache_key, table_provider.clone())?;
        debug!("[SAVE] CACHED: Stored TableProvider for file_id: {file_id}");
    }

    Ok(table_provider)
}

// [OK] Thin convenience wrappers for backward compatibility (no logic duplication)
// Following anti-duplication guidelines: use main function with default options

/// Create a table provider with default options (all versions)
/// Thin wrapper around create_table_provider() with default options
pub async fn create_listing_table_provider(
    file_id: FileID,
    context: &ProviderContext,
) -> Result<Arc<dyn TableProvider>> {
    let options = TableProviderOptions {
        version_selection: VersionSelection::AllVersions,
        additional_urls: vec![],
        bounds: tinyfs::SeriesReadBounds::NONE,
    };
    create_table_provider(file_id, context, options).await
}

/// Create a table provider for the latest version only
/// Thin wrapper around create_table_provider() with Latest version selection
pub async fn create_latest_table_provider(
    file_id: FileID,
    context: &ProviderContext,
) -> Result<Arc<dyn TableProvider>> {
    let options = TableProviderOptions {
        version_selection: VersionSelection::LatestVersion,
        additional_urls: vec![],
        bounds: tinyfs::SeriesReadBounds::NONE,
    };
    create_table_provider(file_id, context, options).await
}
