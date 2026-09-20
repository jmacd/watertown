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
use datafusion::error::DataFusionError;
use log::debug;
use std::collections::HashSet;
use std::sync::Arc;
use tinyfs::{FileID, ProviderContext};

use crate::Result;
use crate::{TableProviderKey, TableProviderOptions, VersionSelection};

/// Create a TableProvider from a FileID with configurable options
///
/// This is the core table creation function that:
/// 1. Checks cache for existing providers (if no additional_urls)
/// 2. Creates ListingTableConfig from URL pattern(s)
/// 3. Infers schema using DataFusion (merges across versions, skips 0-byte files)
/// 4. Caches result for future queries (if no additional_urls)
///
/// # Arguments
/// * `file_id` - FileID containing node_id and part_id for partition pruning
/// * `context` - ProviderContext for session access and caching
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
///     version_selection: VersionSelection::LatestVersion,
///     additional_urls: vec![],
/// };
/// let provider = create_table_provider(file_id, &context, options).await?;
/// ```
/// The tinyfs URLs of exactly those version Parquets of `file_id` that a read
/// bounded by `bounds` can reach, or `None` when the node has no versions to
/// prune (the caller should fall back to the whole-version pattern).
///
/// This shares one predicate with the read path and the format cache
/// (`tinyfs::SeriesReadBounds::retains`), so all three prune identically.
///
/// The prune is a conservative superset, never a correctness filter: a version
/// with no recorded `max_event_time` is retained, and the caller still applies
/// its own time predicate. When it would retain nothing, the newest live
/// version is kept so an empty result still carries a real schema and an idle
/// pass does not fall back to rescanning all history.
pub async fn pruned_version_urls(
    file_id: FileID,
    context: &ProviderContext,
    bounds: tinyfs::SeriesReadBounds,
) -> Result<Option<Vec<String>>> {
    let versions = context.persistence.list_file_versions(file_id).await?;
    let mut urls: Vec<String> = versions
        .iter()
        .filter(|v| {
            bounds.retains(
                crate::format_cache::version_max_event_time(v),
                v.version as i64,
            )
        })
        .map(|v| crate::TinyFsPathBuilder::url_specific_version(&file_id, v.version))
        .collect();
    if urls.is_empty()
        && let Some(newest) = versions.iter().max_by_key(|v| v.version)
    {
        urls.push(crate::TinyFsPathBuilder::url_specific_version(
            &file_id,
            newest.version,
        ));
    }
    Ok(if urls.is_empty() { None } else { Some(urls) })
}

struct SelectedVersion {
    url: String,
    schema_fingerprint: Option<String>,
}

async fn selected_versions(
    file_id: FileID,
    context: &ProviderContext,
    selection: &VersionSelection,
    bounds: tinyfs::SeriesReadBounds,
) -> Result<Vec<SelectedVersion>> {
    let versions = context.persistence.list_file_versions(file_id).await?;
    let mut selected: Vec<_> = versions.iter().filter(|version| version.size > 0).collect();

    match selection {
        VersionSelection::AllVersions => {}
        VersionSelection::LatestVersion => {
            if let Some(latest) = selected.iter().max_by_key(|version| version.version) {
                selected = vec![*latest];
            }
        }
        VersionSelection::SpecificVersion(target) => {
            selected.retain(|version| version.version == *target);
        }
    }
    let schema_fallback = selected
        .iter()
        .max_by_key(|version| version.version)
        .copied();
    if bounds != tinyfs::SeriesReadBounds::NONE {
        selected.retain(|version| {
            bounds.retains(
                crate::format_cache::version_max_event_time(version),
                version.version as i64,
            )
        });
        if selected.is_empty()
            && let Some(fallback) = schema_fallback
        {
            selected.push(fallback);
        }
    }

    Ok(selected
        .into_iter()
        .map(|version| SelectedVersion {
            url: crate::TinyFsPathBuilder::url_specific_version(&file_id, version.version),
            schema_fingerprint: version
                .extended_metadata
                .as_ref()
                .and_then(|metadata| metadata.get("series_schema_fingerprint"))
                .cloned(),
        })
        .collect())
}

async fn listing_config_with_merged_schema(
    context: &ProviderContext,
    table_urls: Vec<ListingTableUrl>,
    schema_fingerprints: &[Option<String>],
) -> Result<ListingTableConfig> {
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()));
    let mut schemas = Vec::with_capacity(table_urls.len());
    let mut seen_fingerprints = HashSet::new();
    for (table_url, fingerprint) in table_urls.iter().zip(schema_fingerprints) {
        if let Some(fingerprint) = fingerprint
            && !seen_fingerprints.insert(fingerprint)
        {
            continue;
        }
        let inferred = ListingTableConfig::new(table_url.clone())
            .with_listing_options(listing_options.clone())
            .infer_schema(&context.datafusion_session.state())
            .await?;
        let schema = inferred.file_schema.ok_or_else(|| {
            DataFusionError::Plan(format!("Could not infer schema for {table_url}"))
        })?;
        schemas.push(schema.as_ref().clone());
    }
    let schema = arrow::datatypes::Schema::try_merge(schemas)
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
    Ok(ListingTableConfig::new_with_multi_paths(table_urls)
        .with_listing_options(listing_options)
        .with_schema(Arc::new(schema)))
}

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
    let coherence = context.persistence.coherence_state();
    let provider_generation = coherence.as_ref().map(|state| state.generation());

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

    let selected_versions = if options.additional_urls.is_empty() {
        Some(selected_versions(file_id, context, &options.version_selection, options.bounds).await?)
    } else {
        None
    };

    // Use explicit version URLs so DataFusion's list-files cache cannot hide a
    // version completed after an earlier provider was built.
    let (table_urls, schema_fingerprints, debug_info) = if let Some(versions) = &selected_versions {
        if versions.is_empty() {
            return Err(crate::Error::TinyFs(tinyfs::Error::not_found(format!(
                "No readable versions found for {file_id}"
            ))));
        }
        let mut table_urls = Vec::with_capacity(versions.len());
        let mut schema_fingerprints = Vec::with_capacity(versions.len());
        for version in versions {
            table_urls.push(ListingTableUrl::parse(&version.url)?);
            schema_fingerprints.push(version.schema_fingerprint.clone());
        }

        (
            table_urls,
            schema_fingerprints,
            format!("{} explicit version URL(s)", versions.len()),
        )
    } else {
        // Multiple URLs provided via options - use only the provided URLs, not the default pattern
        let mut table_urls = Vec::new();

        // Add only the additional URLs (no default pattern when explicit URLs are provided)
        for url_str in &options.additional_urls {
            table_urls.push(ListingTableUrl::parse(url_str)?);
        }

        let urls_str: Vec<String> = table_urls.iter().map(|u| u.to_string()).collect();
        let schema_fingerprints = vec![None; table_urls.len()];
        (
            table_urls,
            schema_fingerprints,
            format!("multiple URLs: [{}]", urls_str.join(", ")),
        )
    };

    debug!("Creating table provider with {debug_info}");

    // Use DataFusion's schema inference - this will automatically:
    // 1. Iterate through all versions of the file
    // 2. Skip 0-byte files (temporal override metadata-only versions)
    // 3. Merge schemas from all valid Parquet versions
    // 4. Provide the unified schema
    let config_with_schema =
        listing_config_with_merged_schema(context, table_urls, &schema_fingerprints).await?;
    if let (Some(coherence), Some(generation)) = (&coherence, provider_generation) {
        coherence.ensure_generation(generation)?;
    }

    let table_provider: Arc<dyn TableProvider> = tinyfs::coherent_table_provider(
        Arc::new(ListingTable::try_new(config_with_schema)?),
        coherence,
        provider_generation,
    );

    log::debug!("[LIST] CREATED TableProvider: file_id={file_id}, urls={debug_info}");

    // Cache the result (only for simple cases without additional_urls)
    if options.additional_urls.is_empty() {
        let cache_key = TableProviderKey::with_bounds(
            file_id,
            options.version_selection.clone(),
            options.bounds,
        )
        .to_cache_string();

        context.set_table_provider_cache_at(
            cache_key,
            table_provider.clone(),
            provider_generation,
        )?;
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
