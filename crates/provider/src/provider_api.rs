// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Layer 3: Provider API - URL-based TableProvider creation
//!
//! Bridges FormatProviders (Layer 2) with DataFusion TableProviders.
//! Handles:
//! - URL pattern expansion (glob support)
//! - Format provider lookup by scheme
//! - Multi-file union for patterns
//! - Table registration in DataFusion
//!
//! This enables using `csv:///path/*.csv` in factory configs like timeseries_join.

use crate::{Error, FileProvider, FormatProvider, FormatRegistry, Result, Url};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use std::future::Future;
use std::sync::Arc;

/// Provider for creating DataFusion TableProviders from URL patterns
///
/// Supports:
/// - Single file: `csv:///data/file.csv`
/// - Glob patterns: `csv:///data/**/*.csv`
/// - Compression: `csv+gzip:///data/*.csv.gz`
/// - Format options: `csv:///data/file.csv?delimiter=;`
/// - Builtin types: `file:///path`, `series:///path`, `table:///path`
pub struct Provider {
    /// TinyFS for file access
    fs: Arc<tinyfs::FS>,
    /// Optional ProviderContext for builtin type support (requires active transaction)
    provider_context: Option<Arc<tinyfs::ProviderContext>>,
    /// Optional root WD that respects effective_root (cross-pond imports).
    /// When set, path resolution uses this instead of fs.root().
    root_wd: Option<tinyfs::WD>,
}

impl Provider {
    /// Create a new Provider with TinyFS (without ProviderContext)
    /// This will only support external format providers (csv, oteljson, etc.)
    #[must_use]
    pub fn new(fs: Arc<tinyfs::FS>) -> Self {
        Self {
            fs,
            provider_context: None,
            root_wd: None,
        }
    }

    /// Create a new Provider with ProviderContext (supports all types)
    /// Use this when you have an active transaction and want to query builtin types
    #[must_use]
    pub fn with_context(
        fs: Arc<tinyfs::FS>,
        provider_context: Arc<tinyfs::ProviderContext>,
    ) -> Self {
        Self {
            fs,
            provider_context: Some(provider_context),
            root_wd: None,
        }
    }

    /// Set the root WD for path resolution (respects effective_root).
    #[must_use]
    pub fn with_root(mut self, root: tinyfs::WD) -> Self {
        self.root_wd = Some(root);
        self
    }

    /// Get the root WD, using the effective_root if set, otherwise fs.root().
    async fn root(&self) -> tinyfs::Result<tinyfs::WD> {
        if let Some(ref wd) = self.root_wd {
            Ok(wd.clone())
        } else {
            self.fs.root().await
        }
    }

    /// Create a TableProvider from a URL pattern
    ///single-file URL
    ///
    /// Uses global format registry to look up format providers by scheme.
    /// Streams data into MemTable.
    ///
    /// NOTE: Does NOT support wildcard patterns. Callers must expand patterns
    /// using TinyFS collect_matches() and call this method for each matched file.
    ///
    /// # Arguments
    ///
    /// * `url_str` - URL like `csv:///data/file.csv` (NO wildcards)
    /// * `ctx` - DataFusion session context for registration
    ///
    /// # Returns
    ///
    /// TableProvider that can be registered and queried
    pub async fn create_table_provider(
        &self,
        url_str: &str,
        ctx: &SessionContext,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        self.create_table_provider_bounded(url_str, ctx, tinyfs::SeriesReadBounds::NONE)
            .await
    }

    /// As [`Provider::create_table_provider`], but for append-only series read
    /// the supplied [`tinyfs::SeriesReadBounds`] prune versions (event-time
    /// lower bound and/or version watermark) so a bounded reader never
    /// materializes old history. The bounds apply to the no-cache MemTable
    /// path (`async_reader_bounded`), the cached-`ListingTable` path and the
    /// builtin pond path (in both, only the retained version Parquets are
    /// listed).
    pub async fn create_table_provider_bounded(
        &self,
        url_str: &str,
        _ctx: &SessionContext,
        bounds: tinyfs::SeriesReadBounds,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        let url = Url::parse(url_str)?;

        // Check for wildcards - caller should expand patterns
        let path = url.path();
        if path.contains('*') || path.contains('?') {
            return Err(Error::InvalidUrl(format!(
                "URL '{}' contains wildcards. Caller must expand patterns using TinyFS collect_matches() and call create_table_provider() for each file.",
                url_str
            )));
        }

        let scheme = url.scheme();

        // Host files explicitly typed as table/series with the default
        // (Parquet) format -- `host+table:///snapshot.parquet`,
        // `host+series:///readings.parquet` (both have scheme `file`) -- are
        // raw bytes on the host filesystem, but the URL asserts they are
        // queryable Parquet.  Read them directly as Parquet rather than
        // routing through the tinyfs builtin path, which classifies host
        // files as raw `FilePhysicalVersion` and rejects them as
        // "not queryable".
        //
        // The `scheme == "file"` guard is essential: a non-default format
        // such as `host+csv+series:///data.csv` also carries entry_type
        // `series` but must be parsed by its format provider (CSV here), NOT
        // read as Parquet.
        if url.is_host()
            && scheme == "file"
            && matches!(url.entry_type(), Some("table") | Some("series"))
        {
            return self.create_host_parquet_table_provider(&url).await;
        }

        // Check if this is a builtin TinyFS type (file, series, table, data)
        if matches!(scheme, "file" | "series" | "table" | "data") {
            // Explicit `+series`/`+table` cast over a node that is not natively
            // queryable (a data archetype, e.g. a git-ingested FileDynamic
            // Parquet file) reinterprets its bytes as Parquet.  This is the pond
            // analogue of the host cast above and requires no ProviderContext.
            // See docs/url-entry-type-casting.md.
            if matches!(url.entry_type(), Some("table") | Some("series"))
                && let Some(provider) = self.try_cast_data_node_to_parquet(&url).await?
            {
                return Ok(provider);
            }

            let provider_context = self.provider_context.as_ref().ok_or_else(|| {
                Error::InvalidUrl(format!(
                    "Builtin type '{}' requires ProviderContext. Use Provider::with_context()",
                    scheme
                ))
            })?;
            return self
                .create_builtin_table_provider(&url, provider_context, bounds)
                .await;
        }

        // External format providers (csv, oteljson, excelhtml, etc.)
        let format_provider = FormatRegistry::get_provider(scheme)
            .ok_or_else(|| Error::InvalidUrl(format!("Unknown format: {}", scheme)))?;

        // Try cache path if ProviderContext with cache_dir is available
        if let Some(ref provider_context) = self.provider_context
            && let Some(cache_dir) = provider_context.cache_dir()
        {
            return self
                .create_cached_table_from_url(
                    &url,
                    format_provider.as_ref(),
                    cache_dir,
                    &provider_context.datafusion_session,
                    bounds,
                )
                .await;
        }

        // No cache available -- fall back to MemTable
        self.create_memtable_from_url(&url, format_provider.as_ref(), bounds)
            .await
    }

    /// Create a `MemTable` from a host filesystem file read as Parquet.
    ///
    /// Used for `host+table:///x.parquet` / `host+series:///x.parquet`: the
    /// file lives on the host filesystem (raw bytes, no tinyfs entry-type
    /// metadata), but the URL asserts it is Parquet.  We read the bytes
    /// through the host file provider and decode them with the Parquet
    /// reader, which validates the format (a non-Parquet file yields a
    /// clear "not a valid Parquet file" error rather than the opaque
    /// "not queryable" error from the tinyfs builtin path).
    async fn create_host_parquet_table_provider(
        &self,
        url: &Url,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use tokio::io::AsyncReadExt;

        // Read the whole host file into memory (host files are local and
        // `pond cat`-sized).  `open_host_url` reads the local filesystem
        // directly (with optional `+gzip`/`+zstd` decompression), so this
        // does not depend on `self.fs` being a host-mount.
        let mut reader = crate::open_host_url(url).await?;
        let mut buf = Vec::new();
        let _ = reader.read_to_end(&mut buf).await.map_err(|e| {
            Error::InvalidUrl(format!("Failed to read host file '{}': {}", url.path(), e))
        })?;

        let builder =
            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(buf)).map_err(|e| {
                Error::InvalidUrl(format!(
                    "Host file '{}' is not a valid Parquet file: {}",
                    url.path(),
                    e
                ))
            })?;
        let schema = builder.schema().clone();
        let batch_reader = builder.build().map_err(|e| {
            Error::InvalidUrl(format!(
                "Failed to read Parquet from host file '{}': {}",
                url.path(),
                e
            ))
        })?;

        let mut batches = Vec::new();
        for batch in batch_reader {
            batches.push(batch.map_err(|e| {
                Error::InvalidUrl(format!(
                    "Failed to decode Parquet batch from host file '{}': {}",
                    url.path(),
                    e
                ))
            })?);
        }

        let table = MemTable::try_new(schema, vec![batches])?;
        Ok(Arc::new(table))
    }

    /// If `url` resolves to a node that is not natively queryable (a data
    /// archetype such as a git-ingested `FileDynamic` Parquet file), read its
    /// bytes and decode them as a Parquet `MemTable` (the `+series`/`+table`
    /// cast).
    ///
    /// Returns `Ok(None)` **only** when the node is natively queryable, so
    /// the caller can fall through to the normal `as_queryable()` path.
    /// Every other condition -- resolve error, missing node, file-handle
    /// error, parquet decode error -- propagates as `Err`.  Silent
    /// fallthrough on "missing path" was hiding real lookup errors as a
    /// misleading "requires ProviderContext" downstream (the next handler
    /// requires a `ProviderContext`, which the contextless cast caller --
    /// e.g. hydrovu resume -- does not have).
    ///
    /// Requires no `ProviderContext`: the cast reads bytes via the node's
    /// `async_reader()`, exactly like the host cast.
    async fn try_cast_data_node_to_parquet(
        &self,
        url: &Url,
    ) -> Result<Option<Arc<dyn datafusion::catalog::TableProvider>>> {
        use tinyfs::Lookup;

        // Wildcards are already rejected upstream by `create_table_provider`
        // (the only caller); this is dead-code-defensive.
        let path = url.path();

        let root = self.root().await?;
        let (_, lookup_result) = root.resolve_path(path).await.map_err(|e| {
            Error::InvalidUrl(format!(
                "Failed to resolve path '{}' for {}://  cast: {}",
                path,
                url.scheme(),
                e
            ))
        })?;
        let node_path = match lookup_result {
            Lookup::Found(np) => np,
            _ => {
                return Err(Error::InvalidUrl(format!(
                    "File not found for {}:// cast: {}",
                    url.scheme(),
                    path
                )));
            }
        };

        // Capability-based gate: cast only nodes that cannot serve themselves
        // as a TableProvider (data archetype).  Natively queryable
        // series/table nodes are returned as `Ok(None)` so the caller falls
        // through to the standard `as_queryable()` path -- this is the
        // *only* legitimate fallthrough.
        let is_queryable = {
            let file_handle = node_path
                .as_file()
                .await
                .map_err(|e| Error::InvalidUrl(format!("Failed to get file handle: {}", e)))?;
            let file_arc = file_handle.handle.get_file().await;
            let file_guard = file_arc.lock().await;
            file_guard.as_queryable().is_some()
        };
        if is_queryable {
            return Ok(None);
        }

        let provider = read_pond_node_as_parquet(&node_path).await?;
        Ok(Some(provider))
    }

    /// Create TableProvider from builtin TinyFS file using QueryableFile trait
    async fn create_builtin_table_provider(
        &self,
        url: &Url,
        provider_context: &tinyfs::ProviderContext,
        bounds: tinyfs::SeriesReadBounds,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        use tinyfs::Lookup;

        let root = self.root().await?;
        let path = url.path();

        let (_, lookup_result) = root
            .resolve_path(path)
            .await
            .map_err(|e| Error::InvalidUrl(format!("Failed to resolve path '{}': {}", path, e)))?;

        let node_path = match lookup_result {
            Lookup::Found(np) => np,
            _ => return Err(Error::InvalidUrl(format!("File not found: {}", path))),
        };

        let file_handle = node_path
            .as_file()
            .await
            .map_err(|e| Error::InvalidUrl(format!("Failed to get file handle: {}", e)))?;

        let file_arc = file_handle.handle.get_file().await;
        let file_guard = file_arc.lock().await;

        let queryable_file = file_guard.as_queryable().ok_or_else(|| {
            Error::InvalidUrl(format!(
                "File '{}' is not queryable (type: {:?})",
                path,
                node_path.id().entry_type()
            ))
        })?;

        let table_provider = queryable_file
            .as_table_provider_bounded(node_path.id(), provider_context, bounds)
            .await
            .map_err(|e| Error::InvalidUrl(format!("Failed to create table provider: {}", e)))?;

        drop(file_guard);
        Ok(table_provider)
    }

    /// Expand pattern and invoke callback for each matched file
    ///
    /// For patterns with wildcards, expands using TinyFS and creates a TableProvider
    /// for each match, calling the callback with the provider and file path.
    /// For single files, calls callback once.
    ///
    /// # Arguments
    ///
    /// * `url_str` - URL pattern (e.g., `excelhtml:///data/*.htm` or `csv:///file.csv`)
    /// * `callback` - Called for each match with (TableProvider, file_path)
    ///
    /// # Returns
    ///
    /// Number of files matched
    pub async fn for_each_match<F, Fut>(&self, url_str: &str, mut callback: F) -> Result<usize>
    where
        F: FnMut(Arc<dyn datafusion::catalog::TableProvider>, String) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let url = Url::parse(url_str)?;
        let path = url.path();
        let has_wildcards = path.contains('*') || path.contains('?');

        if has_wildcards {
            // Expand pattern
            let root = self.root().await?;
            let matches = root.collect_matches(path).await.map_err(|e| {
                Error::InvalidUrl(format!("Pattern expansion failed for '{}': {}", path, e))
            })?;

            if matches.is_empty() {
                return Err(Error::InvalidUrl(format!(
                    "No files match pattern: {}",
                    url_str
                )));
            }

            let ctx = SessionContext::new();
            for (node_path, _captures) in &matches {
                let file_path = node_path.path();
                let file_url_str = format!("{}://{}", url.scheme(), file_path.display());

                let table_provider = self.create_table_provider(&file_url_str, &ctx).await?;
                callback(table_provider, file_path.display().to_string()).await?;
            }

            Ok(matches.len())
        } else {
            // Single file
            let ctx = SessionContext::new();
            let table_provider = self.create_table_provider(url_str, &ctx).await?;
            callback(table_provider, path.to_string()).await?;
            Ok(1)
        }
    }

    /// Create a MemTable by streaming a single file
    async fn create_memtable_from_url(
        &self,
        url: &Url,
        format_provider: &dyn FormatProvider,
        bounds: tinyfs::SeriesReadBounds,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        // Open file with decompression, pruning series versions per the
        // supplied bounds (event-time lower bound and/or version watermark).
        let reader = self.fs.open_url_bounded(url, bounds).await?;

        // Stream data with format provider
        let (schema, mut stream) = format_provider.open_stream(reader, url).await?;

        // Collect all batches
        let mut batches = Vec::new();
        while let Some(result) = stream.next().await {
            batches.push(result?);
        }

        // Create MemTable
        let table = MemTable::try_new(schema, vec![batches])?;
        Ok(Arc::new(table))
    }

    /// Create a cached table from a format URL.
    ///
    /// For each version of the source file, checks if a cached Parquet file
    /// exists.  Uncached versions are parsed via the format provider and
    /// streamed to cache Parquet files.  Returns a `ListingTable` over the
    /// cache directory, pruned to the versions retained by `bounds` (so a
    /// bounded series reader scans only the hot version Parquets).
    async fn create_cached_table_from_url(
        &self,
        url: &Url,
        format_provider: &dyn FormatProvider,
        cache_dir: &std::path::Path,
        ctx: &SessionContext,
        bounds: tinyfs::SeriesReadBounds,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        let scheme = url.scheme();
        let (node_id, versions) = self
            .ensure_url_cached(url, format_provider, cache_dir)
            .await?;

        // Return ListingTable over the cached version Parquet files retained by
        // the bounds (all versions when bounds are NONE).
        crate::format_cache::listing_table_from_cache_bounded(
            cache_dir, scheme, &node_id, &versions, &bounds, ctx,
        )
        .await
    }

    /// Populate the per-node cache for a URL, returning the `NodeID` and its
    /// version list so callers can build glob symlinks or listing tables.
    ///
    /// Resolves the URL to a `FileID`, lists all versions, and caches any
    /// uncached versions by streaming them through the format provider.
    pub(crate) async fn ensure_url_cached(
        &self,
        url: &Url,
        format_provider: &dyn FormatProvider,
        cache_dir: &std::path::Path,
    ) -> Result<(tinyfs::NodeID, Vec<tinyfs::FileVersionInfo>)> {
        use tinyfs::Lookup;

        let scheme = url.scheme();
        let path = url.path();

        // Resolve URL path to FileID
        let root = self.root().await?;
        let (_, lookup_result) = root
            .resolve_path(path)
            .await
            .map_err(|e| Error::InvalidUrl(format!("Failed to resolve path '{}': {}", path, e)))?;

        let node_path = match lookup_result {
            Lookup::Found(np) => np,
            _ => return Err(Error::InvalidUrl(format!("File not found: {}", path))),
        };

        let file_id = node_path.id();
        let node_id = file_id.node_id();

        // Dynamic files (e.g., git-ingest blobs) are ephemeral and have no
        // persistence records.  Read them directly via their File handle and
        // synthesize a single version from metadata.
        if file_id.entry_type().is_dynamic() {
            let file_node = node_path
                .as_file()
                .await
                .map_err(|e| Error::InvalidUrl(format!("Dynamic node is not a file: {}", e)))?;

            let metadata = file_node
                .metadata()
                .await
                .map_err(|e| Error::InvalidUrl(format!("Failed to get metadata: {}", e)))?;

            let version_info = tinyfs::FileVersionInfo {
                version: metadata.version,
                timestamp: metadata.timestamp,
                size: metadata.size.unwrap_or(0),
                blake3: None,
                entry_type: metadata.entry_type,
                extended_metadata: None,
            };

            // Check if already cached
            let uncached = crate::format_cache::find_uncached_versions(
                cache_dir,
                scheme,
                &node_id,
                std::slice::from_ref(&version_info),
            );

            if !uncached.is_empty() {
                log::info!(
                    "[SAVE] Format cache: writing dynamic file {}://{}",
                    scheme,
                    path
                );

                // Read content via async_reader
                let reader = file_node.async_reader().await.map_err(|e| {
                    Error::InvalidUrl(format!("Failed to read dynamic file: {}", e))
                })?;

                let reader: std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>> = reader;

                // Apply decompression if needed
                let reader = crate::format::compression::decompress(reader, url.compression())?;

                // Parse through format provider
                let (schema, stream) = format_provider.open_stream(reader, url).await?;

                // Stream to cache Parquet
                let _ = crate::format_cache::cache_write_version(
                    cache_dir,
                    scheme,
                    &node_id,
                    &version_info,
                    schema,
                    stream,
                )
                .await?;
            }

            return Ok((node_id, vec![version_info]));
        }

        let persistence = self
            .provider_context
            .as_ref()
            .expect("cache path requires ProviderContext");

        // List all versions
        let versions = persistence
            .persistence
            .list_file_versions(file_id)
            .await
            .map_err(|e| Error::InvalidUrl(format!("Failed to list versions: {}", e)))?;

        if versions.is_empty() {
            return Err(Error::InvalidUrl(format!(
                "No versions found for file: {}",
                path
            )));
        }

        // Reconcile, not merely check: `versions` is this node's complete live
        // set, so this is the one place that can also retire the sidecars of
        // versions a collapse superseded. Without it the cache only ever grew.
        let uncached =
            crate::format_cache::reconcile_cached_versions(cache_dir, scheme, &node_id, &versions)?;

        if uncached.is_empty() {
            log::debug!(
                "[GO] Format cache: all {} versions cached for {}://{}",
                versions.len(),
                scheme,
                path
            );
        } else {
            log::info!(
                "[SAVE] Format cache: writing {} of {} versions for {}://{}",
                uncached.len(),
                versions.len(),
                scheme,
                path
            );

            for version in &uncached {
                // Read this specific version's raw bytes
                let bytes = persistence
                    .persistence
                    .read_file_version(file_id, version.version)
                    .await
                    .map_err(|e| {
                        Error::InvalidUrl(format!(
                            "Failed to read version {}: {}",
                            version.version, e
                        ))
                    })?;

                // Wrap bytes as AsyncRead for format provider
                let reader: std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>> =
                    Box::pin(std::io::Cursor::new(bytes));

                // Apply decompression if needed
                let reader = crate::format::compression::decompress(reader, url.compression())?;

                // Parse through format provider
                let (schema, stream) = format_provider.open_stream(reader, url).await?;

                // Stream to cache Parquet -- not collected into memory
                let _ = crate::format_cache::cache_write_version(
                    cache_dir, scheme, &node_id, version, schema, stream,
                )
                .await?;
            }
        }

        Ok((node_id, versions))
    }

    /// Create a single `TableProvider` for a URL that may contain wildcards.
    ///
    /// For external format schemes (`csv`, `oteljson`, ...) with an available
    /// format cache, individual file versions are cached as Parquet and a
    /// single streaming `ListingTable` is returned -- avoiding full
    /// materialization.
    ///
    /// For builtin types (`file`/`series`/`table`) or when no cache is
    /// available, falls back to UNION ALL BY NAME (which materializes).
    pub async fn create_provider_for_url(
        &self,
        url_str: &str,
        ctx: &SessionContext,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        let url = Url::parse(url_str)?;
        let path = url.path();
        let has_wildcards = path.contains('*') || path.contains('?');

        if !has_wildcards {
            // Single file -- delegate directly
            return self.create_table_provider(url_str, ctx).await;
        }

        let scheme = url.scheme();

        // For external formats with cache: glob-cache path
        if !matches!(scheme, "file" | "series" | "table" | "data")
            && let Some(ref provider_context) = self.provider_context
            && let Some(cache_dir) = provider_context.cache_dir()
        {
            let format_provider = FormatRegistry::get_provider(scheme)
                .ok_or_else(|| Error::InvalidUrl(format!("Unknown format: {}", scheme)))?;

            // Expand pattern via TinyFS
            let root = self.root().await?;
            let matches = root.collect_matches(path).await.map_err(|e| {
                Error::InvalidUrl(format!("Pattern expansion failed for '{}': {}", path, e))
            })?;

            if matches.is_empty() {
                return Err(Error::InvalidUrl(format!(
                    "No files match pattern: {}",
                    url_str
                )));
            }

            if matches.len() == 1 {
                // Single match -- per-node ListingTable
                let (node_path, _) = &matches[0];
                let file_url_str = format!("{}://{}", scheme, node_path.path().display());
                return self.create_table_provider(&file_url_str, ctx).await;
            }

            // Multi-file: cache each, then scan every live version explicitly.
            let mut nodes = Vec::with_capacity(matches.len());
            for (node_path, _) in &matches {
                let file_url_str = format!("{}://{}", scheme, node_path.path().display());
                let file_url = Url::parse(&file_url_str)?;

                nodes.push(
                    self.ensure_url_cached(&file_url, format_provider.as_ref(), cache_dir)
                        .await?,
                );
            }

            let provider = crate::format_cache::glob_cached_set(cache_dir, scheme, &nodes)
                .table_provider()
                .await?;

            log::debug!(
                "[OK] Glob cache ListingTable for {} files (pattern '{}')",
                matches.len(),
                url_str,
            );

            return Ok(provider);
        }

        // Fallback: individual providers + UNION ALL BY NAME
        let mut table_providers = Vec::new();
        let _ = self
            .for_each_match(url_str, |tp, file_path| {
                log::debug!("Matched file: {}", file_path);
                table_providers.push(tp);
                async { Ok(()) }
            })
            .await?;

        if table_providers.len() == 1 {
            return Ok(table_providers.into_iter().next().expect("len == 1"));
        }

        // Multiple files without cache -- materialize via UNION ALL BY NAME
        let temp_ctx = SessionContext::new();
        for (i, tp) in table_providers.iter().enumerate() {
            let _ = temp_ctx
                .register_table(format!("t{}", i), tp.clone())
                .map_err(|e| {
                    Error::SessionContext(format!("Failed to register table t{}: {}", i, e))
                })?;
        }

        let union_sql = (0..table_providers.len())
            .map(|i| format!("SELECT * FROM t{}", i))
            .collect::<Vec<_>>()
            .join(" UNION ALL BY NAME ");

        log::debug!("Executing fallback union query: {}", union_sql);

        let df = temp_ctx.sql(&union_sql).await?;
        let batches = df.collect().await?;
        let schema = batches
            .first()
            .map(|b| b.schema())
            .ok_or_else(|| Error::SessionContext("No batches in union result".to_string()))?;
        let mem_table = MemTable::try_new(schema, vec![batches])?;
        Ok(Arc::new(mem_table))
    }
}

/// Read a non-queryable data-archetype node (e.g. a git-ingested `FileDynamic`
/// Parquet archive) by streaming its bytes and decoding them as a Parquet
/// `MemTable`.  This is the shared implementation behind both the pond
/// `+series`/`+table` cast (`Provider::try_cast_data_node_to_parquet`) and the
/// timeseries-join builtin path (`SqlDerivedFile::cast_data_node_to_parquet_provider`).
/// Requires no `ProviderContext`.  See `docs/url-entry-type-casting.md`.
pub(crate) async fn read_pond_node_as_parquet(
    node_path: &tinyfs::NodePath,
) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tokio::io::AsyncReadExt;

    let file_handle = node_path
        .as_file()
        .await
        .map_err(|e| Error::InvalidUrl(format!("Failed to get file handle for cast: {}", e)))?;
    let mut reader = file_handle
        .handle
        .async_reader()
        .await
        .map_err(|e| Error::InvalidUrl(format!("Failed to open reader for cast: {}", e)))?;
    let mut buf = Vec::new();
    let _ = reader.read_to_end(&mut buf).await.map_err(|e| {
        Error::InvalidUrl(format!(
            "Failed to read '{}' for Parquet cast: {}",
            node_path.path().display(),
            e
        ))
    })?;

    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(buf)).map_err(|e| {
            Error::InvalidUrl(format!(
                "File '{}' is not a valid Parquet file: {}",
                node_path.path().display(),
                e
            ))
        })?;
    let schema = builder.schema().clone();
    let batch_reader = builder.build().map_err(|e| {
        Error::InvalidUrl(format!(
            "Failed to read Parquet from '{}': {}",
            node_path.path().display(),
            e
        ))
    })?;

    let mut batches = Vec::new();
    for batch in batch_reader {
        batches.push(batch.map_err(|e| {
            Error::InvalidUrl(format!(
                "Failed to decode Parquet batch from '{}': {}",
                node_path.path().display(),
                e
            ))
        })?);
    }

    let table = MemTable::try_new(schema, vec![batches])?;
    Ok(Arc::new(table))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::factory::test_factory::{InfiniteCsvConfig, InfiniteCsvFile};
    use tinyfs::{File, MemoryPersistence};

    async fn create_test_fs() -> Arc<tinyfs::FS> {
        let persistence = MemoryPersistence::default();
        Arc::new(tinyfs::FS::new(persistence).await.unwrap())
    }

    async fn create_csv_file(fs: &tinyfs::FS, path: &str, config: InfiniteCsvConfig) -> Result<()> {
        let root = fs.root().await?;

        // Create parent directories
        if let Some(parent) = std::path::Path::new(path).parent()
            && parent.to_str() != Some("/")
            && !parent.as_os_str().is_empty()
        {
            let path_parts: Vec<_> = parent
                .components()
                .filter_map(|c| match c {
                    std::path::Component::Normal(name) => name.to_str(),
                    _ => None,
                })
                .collect();

            let mut current_path = String::from("/");
            for part in path_parts {
                if !current_path.ends_with('/') {
                    current_path.push('/');
                }
                current_path.push_str(part);
                let _ = root.create_dir_path(&current_path).await;
            }
        }

        // Generate CSV data
        let infinite_file = InfiniteCsvFile::new(config);
        let mut reader = infinite_file.async_reader().await?;

        use tokio::io::AsyncReadExt;
        let mut csv_data = Vec::new();
        let _ = reader.read_to_end(&mut csv_data).await?;

        // Write to TinyFS
        let mut file_writer = root
            .async_writer_path_with_type(path, tinyfs::EntryType::FileDynamic)
            .await?;
        use tokio::io::AsyncWriteExt;
        file_writer.write_all(&csv_data).await?;
        file_writer.flush().await?;
        file_writer.shutdown().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_provider_create_table_single_file() {
        let fs = create_test_fs().await;

        // Create CSV file
        let config = InfiniteCsvConfig {
            row_count: 50,
            batch_size: 1024,
            columns: vec!["id".to_string(), "value".to_string()],
        };
        create_csv_file(&fs, "/data/test.csv", config)
            .await
            .unwrap();

        // Create provider (uses global registry with CSV pre-registered)
        let provider = Provider::new(fs.clone());

        // Create TableProvider from URL
        let ctx = SessionContext::new();
        let table = provider
            .create_table_provider("csv:///data/test.csv", &ctx)
            .await
            .unwrap();

        // Register and query
        let _ = ctx.register_table("test", table).unwrap();
        let df = ctx.sql("SELECT COUNT(*) as count FROM test").await.unwrap();
        let results = df.collect().await.unwrap();

        assert!(!results.is_empty());
    }

    #[tokio::test]
    async fn test_provider_with_compression() {
        let fs = create_test_fs().await;

        // Create CSV file (we'll simulate compression in URL)
        let config = InfiniteCsvConfig {
            row_count: 30,
            batch_size: 1024,
            columns: vec!["timestamp".to_string(), "temperature".to_string()],
        };
        create_csv_file(&fs, "/sensors/temp.csv", config)
            .await
            .unwrap();

        let provider = Provider::new(fs.clone());

        let ctx = SessionContext::new();
        let table = provider
            .create_table_provider("csv:///sensors/temp.csv?batch_size=10", &ctx)
            .await
            .unwrap();

        let _ = ctx.register_table("sensors", table).unwrap();
        let df = ctx.sql("SELECT COUNT(*) FROM sensors").await.unwrap();
        let results = df.collect().await.unwrap();

        assert!(!results.is_empty());
    }

    #[test]
    fn test_provider_uses_linkme_registry() {
        // Test that Provider uses linkme format registry
        let csv_provider = FormatRegistry::get_provider("csv");

        // CSV should be registered via linkme
        assert!(csv_provider.is_some());
        assert_eq!(csv_provider.unwrap().name(), "csv");
    }

    #[tokio::test]
    async fn test_layer_3_complete() -> Result<()> {
        // Full Layer 3 integration test: URL -> Provider -> FormatProvider -> TableProvider
        let fs = create_test_fs().await;

        // Create test CSV file in memory
        create_csv_file(
            &fs,
            "data.csv",
            InfiniteCsvConfig {
                row_count: 2,
                batch_size: 1024,
                columns: vec!["id".to_string(), "name".to_string()],
            },
        )
        .await?;

        let provider = Provider::new(fs);
        let ctx = SessionContext::new();

        // Create table from CSV URL
        let table = provider
            .create_table_provider("csv:///data.csv", &ctx)
            .await?;

        // Register and query
        let _ = ctx.register_table("users", table)?;
        let df = ctx.sql("SELECT COUNT(*) as cnt FROM users").await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_glob_pattern_expansion() -> Result<()> {
        // Test that Provider correctly rejects glob patterns
        // Glob expansion should be handled by the factory layer
        let fs = create_test_fs().await;

        // Create multiple CSV files
        for i in 1..=3 {
            create_csv_file(
                &fs,
                &format!("data{}.csv", i),
                InfiniteCsvConfig {
                    row_count: 5,
                    batch_size: 100,
                    columns: vec!["id".to_string(), "value".to_string()],
                },
            )
            .await?;
        }

        let provider = Provider::new(fs);
        let ctx = SessionContext::new();

        // Provider should reject glob patterns - factory handles iteration
        let result = provider
            .create_table_provider("csv:///data*.csv", &ctx)
            .await;
        assert!(result.is_err(), "Provider should reject glob patterns");

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("contains wildcards"),
            "Error should explain wildcards are not allowed"
        );
        assert!(
            err_msg.contains("collect_matches()"),
            "Error should guide to use collect_matches"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_host_table_reads_parquet_directly() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;

        // Write a real Parquet file to the host filesystem.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snapshot.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        {
            let file = std::fs::File::create(&path).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
            writer.write(&batch).unwrap();
            let _ = writer.close().unwrap();
        }

        // A plain pond FS is enough: host+table reads the host file directly.
        let fs = create_test_fs().await;
        let provider = Provider::new(fs);
        let ctx = SessionContext::new();

        let url = format!("host+table://{}", path.display());
        let table = provider
            .create_table_provider(&url, &ctx)
            .await
            .expect("host+table parquet must be queryable");
        let _ = ctx.register_table("source", table).unwrap();

        let results = ctx
            .sql("SELECT count(*) AS n, sum(id) AS s FROM source")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batch = &results[0];
        let n = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let s = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 3);
        assert_eq!(s, 6);
    }

    #[tokio::test]
    async fn test_host_table_rejects_non_parquet() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.txt");
        std::fs::write(&path, b"this is not parquet").unwrap();

        let fs = create_test_fs().await;
        let provider = Provider::new(fs);
        let ctx = SessionContext::new();

        let url = format!("host+table://{}", path.display());
        let err = provider
            .create_table_provider(&url, &ctx)
            .await
            .expect_err("non-Parquet host+table must error");
        assert!(
            err.to_string().contains("not a valid Parquet file"),
            "error should be a clear Parquet error, got: {}",
            err
        );
    }

    /// Regression: `host+csv+series://` carries entry_type `series` but a
    /// non-default format scheme (`csv`); it must route to the CSV format
    /// provider, NOT the Parquet host path.  (Caught by testsuite 301.)
    #[tokio::test]
    async fn test_host_csv_series_does_not_use_parquet_path() {
        let dir = tempfile::tempdir().unwrap();
        // Even pointing at a real Parquet file, csv+series must NOT take the
        // Parquet path -- the `csv` format scheme wins over the entry type.
        let path = dir.path().join("data.parquet");
        std::fs::write(&path, b"PAR1 not really").unwrap();

        let fs = create_test_fs().await;
        let provider = Provider::new(fs);
        let ctx = SessionContext::new();

        let url = format!("host+csv+series://{}", path.display());
        let err = provider
            .create_table_provider(&url, &ctx)
            .await
            .expect_err("csv+series over a plain test FS cannot read the host path");
        // The error must NOT be the Parquet-path error: that would mean the
        // routing incorrectly treated this CSV URL as Parquet.
        assert!(
            !err.to_string().contains("not a valid Parquet file"),
            "host+csv+series must not take the Parquet path, got: {}",
            err
        );
    }

    /// `read_pond_node_as_parquet` decodes a pond node's raw bytes as Parquet
    /// with NO ProviderContext.  This is the byte path behind the
    /// `series://`/`table://` cast over a git-ingested data archetype, used by
    /// hydrovu find_youngest to read archive max(timestamp).
    #[tokio::test]
    async fn test_read_pond_node_as_parquet_contextless() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use tinyfs::Lookup;
        use tokio::io::AsyncWriteExt;

        // Build real Parquet bytes (timestamp in seconds, like the archives).
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![100i64, 200, 1_770_000_000]))],
        )
        .unwrap();
        let mut parquet_bytes = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut parquet_bytes, schema, None).unwrap();
            writer.write(&batch).unwrap();
            let _ = writer.close().unwrap();
        }

        // Write those bytes into a pond node as a FileDynamic, mirroring
        // git-ingest archives.
        let fs = create_test_fs().await;
        let root = fs.root().await.unwrap();
        let _ = root.create_dir_path("/hydrovu-archive").await;
        {
            let mut w = root
                .async_writer_path_with_type(
                    "/hydrovu-archive/dev.series",
                    tinyfs::EntryType::FileDynamic,
                )
                .await
                .unwrap();
            w.write_all(&parquet_bytes).await.unwrap();
            w.flush().await.unwrap();
            w.shutdown().await.unwrap();
        }

        let node_path = match root
            .resolve_path("/hydrovu-archive/dev.series")
            .await
            .unwrap()
        {
            (_, Lookup::Found(np)) => np,
            _ => panic!("archive node not found"),
        };

        // No ProviderContext is involved -- bytes are read directly.
        let table = read_pond_node_as_parquet(&node_path)
            .await
            .expect("data node bytes must decode as Parquet");

        let ctx = SessionContext::new();
        let _ = ctx.register_table("arch", table).unwrap();
        let results = ctx
            .sql("SELECT max(timestamp) AS m FROM arch")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let m = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(m, 1_770_000_000);
    }

    /// A `series://`/`table://` cast over a path that does not exist must
    /// hard-fail with a clear "File not found" error, NOT silently fall
    /// through to the next handler (which, in the contextless cast caller --
    /// e.g. hydrovu resume -- would emit a misleading "requires
    /// ProviderContext" message and hide the real bug).
    #[tokio::test]
    async fn test_series_cast_over_missing_path_hard_fails() {
        let fs = create_test_fs().await;
        let provider = Provider::new(fs);
        let ctx = SessionContext::new();

        let err = provider
            .create_table_provider("series:///does/not/exist.series", &ctx)
            .await
            .expect_err("missing path under series:// cast must error");
        let msg = err.to_string();
        assert!(
            msg.contains("File not found") || msg.contains("Failed to resolve path"),
            "missing-path error should name the lookup failure, got: {msg}"
        );
        assert!(
            !msg.contains("requires ProviderContext"),
            "missing-path error must not be masked as 'requires ProviderContext', got: {msg}"
        );
    }
}
