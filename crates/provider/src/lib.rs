// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Provider: URL-Based file access and factory infrastructure

pub mod export;
pub mod factory;
pub mod registry;
pub mod transform;

mod error;
mod format;
pub mod format_cache;
mod format_registry;
pub mod partial_aggregate_cache;
mod provider_api;
pub mod size_tier;
mod sql_transform;
mod table_creation;
mod table_provider_options;
mod tinyfs_object_store;
mod tinyfs_path;
mod url_pattern_matcher;
pub mod version_cache;
mod version_selection;

pub use error::{Error, Result};
pub use factory::dynamic_dir::{DynamicDirConfig, DynamicDirDirectory, DynamicDirEntry};
pub use factory::sql_derived::SqlDerivedConfig;
pub use format::FormatProvider;
pub use format_registry::{FORMAT_PROVIDERS, FormatProviderEntry, FormatRegistry};
pub use provider_api::Provider;
pub use registry::{
    ConfigFile, DYNAMIC_FACTORIES, DynamicFactory, ExecutionContext, ExecutionMode, FactoryCommand,
    FactoryRegistry, QueryableFile, SchemeKind, SchemeRegistry,
};
pub use sql_transform::transform_sql;
pub use table_creation::{
    create_latest_table_provider, create_listing_table_provider, create_table_provider,
    pruned_version_urls,
};
pub use table_provider_options::{TableProviderKey, TableProviderOptions};
pub use tinyfs::{FactoryContext, PondMetadata, ProviderContext};
pub use tinyfs_object_store::{TinyFsObjectStore, register_tinyfs_object_store};
pub use tinyfs_path::TinyFsPathBuilder;
pub use url_pattern_matcher::{MatchedFile, UrlPatternMatcher};
pub use version_selection::VersionSelection;

use futures::StreamExt;
use std::pin::Pin;
use tokio::io::AsyncRead;

/// URL for accessing files with format conversion and optional compression
///
/// Format: `[host+]scheme[+compression]:///path/pattern[?query_params]`
///
/// - `host+`: Optional prefix indicating the path refers to the host filesystem
///   rather than a pond-internal path. When present, no pond/POND env is needed.
/// - `scheme`: Format name (csv, oteljson, etc.) - indicates FormatProvider
///   Compression is encoded as a `+suffix` on the scheme (e.g., `csv+gzip`)
///   Supported compressions: zstd, gzip, bzip2
/// - `path`: Filesystem path (host) or TinyFS path/glob pattern (pond)
/// - `query`: Format-specific options (delimiter, batch_size, etc.)
///
/// The URL host component (authority) must be empty.
///
/// Examples:
/// - `csv:///data/file.csv?delimiter=;`           -- pond CSV file
/// - `host+oteljson:///tmp/metrics.json`           -- host OtelJSON file
/// - `host+csv+gzip:///tmp/data.csv.gz`            -- host gzipped CSV file
/// - `host+csv+gzip+series:///tmp/data.csv.gz`     -- host gzipped CSV as time-series
/// - `oteljson+zstd:///logs/**/*.json.zstd`        -- pond compressed OtelJSON
/// - `host+table:///tmp/snapshot.parquet`           -- host file as table entry type
/// - `host+series:///tmp/readings.parquet`          -- host file as series entry type
#[derive(Debug, Clone)]
pub struct Url {
    inner: url::Url,
    /// The base format scheme (e.g., "csv", "oteljson"), without host prefix, compression, or entry type
    format_scheme: String,
    /// The full scheme including compression and entry type suffixes (e.g., "csv+gzip+series")
    /// Precomputed from the decomposed fields for efficient &str access.
    full_scheme_str: String,
    /// Optional compression extracted from scheme suffix (e.g., "gzip", "zstd")
    compression: Option<String>,
    /// Optional entry type extracted from scheme suffix (e.g., "table", "series")
    entry_type: Option<String>,
    /// Whether the URL targets the host filesystem (leading `host+` prefix)
    is_host: bool,
}

impl PartialEq for Url {
    fn eq(&self, other: &Self) -> bool {
        self.format_scheme == other.format_scheme
            && self.compression == other.compression
            && self.entry_type == other.entry_type
            && self.is_host == other.is_host
            && self.inner.path() == other.inner.path()
            && self.inner.query() == other.inner.query()
    }
}

impl Eq for Url {}

impl serde::Serialize for Url {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for Url {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Url::parse(&s).map_err(serde::de::Error::custom)
    }
}

impl std::fmt::Display for Url {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Reconstruct canonical form: [host+]scheme[+compression][+entrytype]:///path[?query]
        if self.is_host {
            write!(f, "host+")?;
        }
        write!(f, "{}", self.format_scheme)?;
        if let Some(ref c) = self.compression {
            write!(f, "+{}", c)?;
        }
        if let Some(ref et) = self.entry_type {
            write!(f, "+{}", et)?;
        }
        write!(f, "://")?;
        write!(f, "{}", self.inner.path())?;
        if let Some(query) = self.inner.query() {
            write!(f, "?{}", query)?;
        }
        Ok(())
    }
}

impl Url {
    /// Known compression suffixes that can appear after `+` in the scheme
    const KNOWN_COMPRESSIONS: &[&str] = &["zstd", "gzip", "bzip2"];

    /// Known entry type suffixes that can appear after `+` in the scheme
    const KNOWN_ENTRY_TYPES: &[&str] = &["table", "series"];

    /// Parse URL from string using standard url crate
    ///
    /// Scheme segments are parsed left-to-right:
    ///   `host+oteljson+gzip+series:///path`
    ///    |     |         |     |
    ///    |     |         |     +-- entry type (known: table, series)
    ///    |     |         +------- compression (known: zstd, gzip, bzip2)
    ///    |     +----------------- format provider (csv, oteljson, etc.)
    ///    +----------------------- source: host filesystem
    ///
    /// Returns error if fragment, port, username, password, or non-empty host are present.
    ///
    /// If the string doesn't contain "://", defaults to "file://" scheme.
    /// The actual file type is determined by EntryType when the file is accessed.
    pub fn parse(url_str: &str) -> Result<Self> {
        // Strip leading `host+` before handing to the url crate.
        // We do this at the string level so the url crate never sees `host+oteljson`
        // as an opaque compound scheme.
        let (is_host, effective_url) = if let Some(rest) = url_str.strip_prefix("host+") {
            (true, rest.to_string())
        } else {
            (false, url_str.to_string())
        };

        // If no scheme is present, default to "file" scheme
        let normalized_url = if !effective_url.contains("://") {
            format!("file://{}", effective_url)
        } else {
            effective_url
        };

        let inner = url::Url::parse(&normalized_url)?;

        if inner.fragment().is_some() {
            return Err(Error::InvalidUrl("fragment not allowed".into()));
        }
        if inner.port().is_some() {
            return Err(Error::InvalidUrl("port not allowed".into()));
        }
        if !inner.username().is_empty() {
            return Err(Error::InvalidUrl("username not allowed".into()));
        }
        if inner.password().is_some() {
            return Err(Error::InvalidUrl("password not allowed".into()));
        }

        // Reject non-empty host -- the host position is reserved
        if let Some(host) = inner.host_str().filter(|h| !h.is_empty()) {
            return Err(Error::InvalidUrl(format!(
                "non-empty host '{}' is not allowed in provider URLs; \
                 use host+ prefix instead (e.g., host+csv:///path)",
                host
            )));
        }

        // Classify scheme segments: split on '+' and assign each to its category.
        // The first segment is the format scheme UNLESS it is itself a known entry
        // type (e.g., `table:///path` or `series:///path`), in which case format
        // defaults to "file" and the segment is treated as the entry type.
        let raw_scheme = inner.scheme().to_string();
        let segments: Vec<&str> = raw_scheme.split('+').collect();

        let format_scheme;
        let mut compression = None;
        let mut entry_type = None;
        let suffix_start;

        if Self::KNOWN_ENTRY_TYPES.contains(&segments[0]) {
            // Bare entry type as scheme: `table:///path` -> format="file", entry_type="table"
            format_scheme = "file".to_string();
            entry_type = Some(segments[0].to_string());
            suffix_start = 1;
        } else if Self::KNOWN_COMPRESSIONS.contains(&segments[0]) {
            // Bare compression as scheme doesn't make sense
            return Err(Error::InvalidUrl(format!(
                "compression '{}' cannot be the primary scheme; use format+compression syntax (e.g., csv+gzip:///path)",
                segments[0]
            )));
        } else {
            format_scheme = segments[0].to_string();
            suffix_start = 1;
        }

        for &segment in &segments[suffix_start..] {
            if Self::KNOWN_COMPRESSIONS.contains(&segment) {
                if compression.is_some() {
                    return Err(Error::InvalidUrl(format!(
                        "duplicate compression in scheme '{}': '{}' conflicts with earlier compression",
                        raw_scheme, segment
                    )));
                }
                compression = Some(segment.to_string());
            } else if Self::KNOWN_ENTRY_TYPES.contains(&segment) {
                if entry_type.is_some() {
                    return Err(Error::InvalidUrl(format!(
                        "duplicate entry type in scheme '{}': '{}' conflicts with earlier entry type",
                        raw_scheme, segment
                    )));
                }
                entry_type = Some(segment.to_string());
            } else {
                return Err(Error::InvalidUrl(format!(
                    "unknown scheme suffix '+{}' in '{}'; expected compression ({}) or entry type ({})",
                    segment,
                    raw_scheme,
                    Self::KNOWN_COMPRESSIONS.join(", "),
                    Self::KNOWN_ENTRY_TYPES.join(", ")
                )));
            }
        }

        // Precompute the full scheme string from decomposed fields.
        // This is the canonical form: format[+compression][+entrytype]
        let mut full_scheme_str = format_scheme.clone();
        if let Some(ref c) = compression {
            full_scheme_str.push('+');
            full_scheme_str.push_str(c);
        }
        if let Some(ref et) = entry_type {
            full_scheme_str.push('+');
            full_scheme_str.push_str(et);
        }

        Ok(Self {
            inner,
            format_scheme,
            full_scheme_str,
            compression,
            entry_type,
            is_host,
        })
    }

    /// Get the base format scheme (e.g., "csv", "oteljson")
    ///
    /// This returns only the format portion, without any compression suffix.
    /// For `csv+gzip:///path`, this returns `"csv"`.
    #[must_use]
    pub fn scheme(&self) -> &str {
        &self.format_scheme
    }

    /// Get the full scheme including compression and entry type suffixes (e.g., "csv+gzip+series")
    ///
    /// This is reconstructed from the decomposed segments, ensuring consistency
    /// even when the input used shorthand forms (e.g., `series:///` -> `file+series`).
    /// Excludes the `host+` prefix.
    #[must_use]
    pub fn full_scheme(&self) -> &str {
        &self.full_scheme_str
    }

    /// Get optional compression from scheme suffix (e.g., "gzip", "zstd")
    ///
    /// Compression is encoded as `+suffix` on the scheme:
    /// - `csv+gzip:///path` -> `Some("gzip")`
    /// - `csv:///path` -> `None`
    #[must_use]
    pub fn compression(&self) -> Option<&str> {
        self.compression.as_deref()
    }

    /// Get optional entry type from scheme suffix (e.g., "table", "series")
    ///
    /// Entry type is encoded as `+suffix` on the scheme:
    /// - `host+table:///path` -> `Some("table")`
    /// - `host+csv+series:///path` -> `Some("series")`
    /// - `csv:///path` -> `None`
    #[must_use]
    pub fn entry_type(&self) -> Option<&str> {
        self.entry_type.as_deref()
    }

    /// Whether this URL targets the host filesystem (has `host+` prefix)
    #[must_use]
    pub fn is_host(&self) -> bool {
        self.is_host
    }

    /// Get TinyFS path or pattern (also used for host filesystem paths)
    #[must_use]
    pub fn path(&self) -> &str {
        self.inner.path()
    }

    /// Parse query parameters into strongly-typed struct using serde_qs
    ///
    /// Example:
    /// ```ignore
    /// #[derive(Deserialize)]
    /// struct CsvOptions {
    ///     delimiter: char,
    ///     has_header: bool,
    /// }
    ///
    /// let url = Url::parse("csv:///file.csv?delimiter=;&has_header=false")?;
    /// let options: CsvOptions = url.query_params()?;
    /// ```
    pub fn query_params<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        match self.inner.query() {
            None => serde_qs::from_str("")
                .map_err(|e| Error::InvalidUrl(format!("query parameter parsing failed: {}", e))),
            Some(query) => serde_qs::from_str(query)
                .map_err(|e| Error::InvalidUrl(format!("query parameter parsing failed: {}", e))),
        }
    }
}

/// Trait for URL access to Tinyfs.
#[async_trait::async_trait]
pub trait FileProvider {
    /// Open a URL with optional decompression
    async fn open_url(&self, url: &Url) -> Result<Pin<Box<dyn AsyncRead + Send>>>;

    /// Open a URL with optional decompression, passing [`tinyfs::SeriesReadBounds`]
    /// so append-only series nodes prune versions below an event-time bound
    /// and/or at-or-below a version watermark. The default ignores the bounds
    /// and delegates to [`FileProvider::open_url`].
    async fn open_url_bounded(
        &self,
        url: &Url,
        _bounds: tinyfs::SeriesReadBounds,
    ) -> Result<Pin<Box<dyn AsyncRead + Send>>> {
        self.open_url(url).await
    }
}

#[async_trait::async_trait]
impl FileProvider for tinyfs::FS {
    /// Open a file for reading with optional decompression (Layer 1)
    async fn open_url(&self, url: &Url) -> Result<Pin<Box<dyn AsyncRead + Send>>> {
        self.open_url_bounded(url, tinyfs::SeriesReadBounds::NONE)
            .await
    }

    async fn open_url_bounded(
        &self,
        url: &Url,
        bounds: tinyfs::SeriesReadBounds,
    ) -> Result<Pin<Box<dyn AsyncRead + Send>>> {
        // Get reader from TinyFS
        let reader = self
            .root()
            .await?
            .async_reader_path_bounded(url.path(), bounds)
            .await?;

        // Cast AsyncReadSeek to AsyncRead before decompression
        let reader: Pin<Box<dyn AsyncRead + Send>> = reader as Pin<Box<dyn AsyncRead + Send>>;

        // Wrap with decompression if needed (uses Layer 1 compression module)
        format::compression::decompress(reader, url.compression())
    }
}

/// Open a host filesystem file as an AsyncRead with optional decompression.
///
/// This does NOT require a pond -- it reads directly from the local filesystem.
/// Used by `pond cat host+format:///path` to process local files through
/// format providers (e.g., oteljson, csv).
pub async fn open_host_url(url: &Url) -> Result<Pin<Box<dyn AsyncRead + Send>>> {
    if !url.is_host() {
        return Err(Error::InvalidUrl(
            "open_host_url requires a host+ URL".into(),
        ));
    }

    let path = url.path();
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|e| Error::InvalidUrl(format!("Failed to open host file '{}': {}", path, e)))?;

    let reader: Pin<Box<dyn AsyncRead + Send>> = Box::pin(file);

    // Wrap with decompression if needed
    format::compression::decompress(reader, url.compression())
}

/// Create a DataFusion MemTable from a host filesystem URL.
///
/// This is the standalone pipeline for `pond cat host+format:///path`:
///   host file -> decompress -> format_provider -> MemTable
///
/// No pond or TinyFS required.
pub async fn create_memtable_from_host_url(
    url: &Url,
) -> Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
    let format_provider = FormatRegistry::get_provider(url.scheme()).ok_or_else(|| {
        Error::InvalidUrl(format!(
            "Unknown format '{}' in host URL. Known formats: csv, oteljson, excelhtml",
            url.scheme()
        ))
    })?;

    let reader = open_host_url(url).await?;

    let (schema, mut stream) = format_provider.open_stream(reader, url).await?;

    let mut batches = Vec::new();
    while let Some(result) = stream.next().await {
        batches.push(result?);
    }

    let table = datafusion::datasource::MemTable::try_new(schema, vec![batches]).map_err(|e| {
        Error::InvalidUrl(format!("Failed to create MemTable from host file: {}", e))
    })?;

    Ok(std::sync::Arc::new(table))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_host_oteljson_url() {
        let url = Url::parse("host+oteljson:///tmp/traces.json").unwrap();
        assert!(url.is_host());
        assert_eq!(url.scheme(), "oteljson");
        assert_eq!(url.path(), "/tmp/traces.json");
        assert_eq!(url.compression(), None);
        assert_eq!(url.to_string(), "host+oteljson:///tmp/traces.json");
    }

    #[test]
    fn test_host_csv_gzip_url() {
        let url = Url::parse("host+csv+gzip:///data/logs.csv.gz").unwrap();
        assert!(url.is_host());
        assert_eq!(url.scheme(), "csv");
        assert_eq!(url.compression(), Some("gzip"));
        assert_eq!(url.path(), "/data/logs.csv.gz");
        assert_eq!(url.to_string(), "host+csv+gzip:///data/logs.csv.gz");
    }

    #[test]
    fn test_host_csv_no_compression() {
        let url = Url::parse("host+csv:///tmp/data.csv").unwrap();
        assert!(url.is_host());
        assert_eq!(url.scheme(), "csv");
        assert_eq!(url.compression(), None);
        assert_eq!(url.path(), "/tmp/data.csv");
    }

    #[test]
    fn test_non_host_url_unchanged() {
        let url = Url::parse("csv:///pond/data.csv").unwrap();
        assert!(!url.is_host());
        assert_eq!(url.scheme(), "csv");
        assert_eq!(url.path(), "/pond/data.csv");
        assert_eq!(url.to_string(), "csv:///pond/data.csv");
    }

    #[test]
    fn test_bare_path_not_host() {
        let url = Url::parse("/some/path").unwrap();
        assert!(!url.is_host());
        assert_eq!(url.scheme(), "file");
    }

    #[test]
    fn test_host_url_equality() {
        let a = Url::parse("host+oteljson:///tmp/a.json").unwrap();
        let b = Url::parse("host+oteljson:///tmp/a.json").unwrap();
        let c = Url::parse("oteljson:///tmp/a.json").unwrap();
        assert_eq!(a, b);
        assert_ne!(a, c); // host vs non-host differ
    }

    #[test]
    fn test_host_url_with_query_params() {
        let url = Url::parse("host+csv:///tmp/data.csv?delimiter=;").unwrap();
        assert!(url.is_host());
        assert_eq!(url.scheme(), "csv");
        assert_eq!(url.path(), "/tmp/data.csv");
    }

    // --- Entry type tests ---

    #[test]
    fn test_host_table_entry_type() {
        let url = Url::parse("host+table:///tmp/snapshot.parquet").unwrap();
        assert!(url.is_host());
        assert_eq!(url.scheme(), "file");
        assert_eq!(url.entry_type(), Some("table"));
        assert_eq!(url.compression(), None);
        assert_eq!(url.path(), "/tmp/snapshot.parquet");
        assert_eq!(url.to_string(), "host+file+table:///tmp/snapshot.parquet");
    }

    #[test]
    fn test_host_series_entry_type() {
        let url = Url::parse("host+series:///tmp/readings.parquet").unwrap();
        assert!(url.is_host());
        assert_eq!(url.scheme(), "file");
        assert_eq!(url.entry_type(), Some("series"));
        assert_eq!(url.path(), "/tmp/readings.parquet");
    }

    #[test]
    fn test_csv_series_entry_type() {
        let url = Url::parse("csv+series:///data/timeseries.csv").unwrap();
        assert!(!url.is_host());
        assert_eq!(url.scheme(), "csv");
        assert_eq!(url.entry_type(), Some("series"));
        assert_eq!(url.compression(), None);
        assert_eq!(url.to_string(), "csv+series:///data/timeseries.csv");
    }

    #[test]
    fn test_host_csv_gzip_series() {
        let url = Url::parse("host+csv+gzip+series:///tmp/data.csv.gz").unwrap();
        assert!(url.is_host());
        assert_eq!(url.scheme(), "csv");
        assert_eq!(url.compression(), Some("gzip"));
        assert_eq!(url.entry_type(), Some("series"));
        assert_eq!(url.to_string(), "host+csv+gzip+series:///tmp/data.csv.gz");
    }

    #[test]
    fn test_entry_type_display_roundtrip() {
        let original = "host+csv+gzip+series:///tmp/data.csv.gz";
        let url = Url::parse(original).unwrap();
        assert_eq!(url.to_string(), original);
    }

    #[test]
    fn test_no_entry_type_when_absent() {
        let url = Url::parse("csv+gzip:///data/file.csv.gz").unwrap();
        assert_eq!(url.entry_type(), None);
        assert_eq!(url.compression(), Some("gzip"));
    }

    #[test]
    fn test_duplicate_compression_rejected() {
        let result = Url::parse("csv+gzip+zstd:///path");
        assert!(result.is_err());
    }

    #[test]
    fn test_duplicate_entry_type_rejected() {
        let result = Url::parse("csv+table+series:///path");
        assert!(result.is_err());
    }

    #[test]
    fn test_unknown_suffix_rejected() {
        let result = Url::parse("csv+banana:///path");
        assert!(result.is_err());
    }

    #[test]
    fn test_entry_type_equality() {
        let a = Url::parse("host+csv+series:///tmp/a.csv").unwrap();
        let b = Url::parse("host+csv+series:///tmp/a.csv").unwrap();
        let c = Url::parse("host+csv+table:///tmp/a.csv").unwrap();
        let d = Url::parse("host+csv:///tmp/a.csv").unwrap();
        assert_eq!(a, b);
        assert_ne!(a, c); // different entry type
        assert_ne!(a, d); // entry type vs none
    }

    // --- SchemeRegistry tests ---

    #[test]
    fn test_scheme_registry_no_conflicts() {
        let conflicts = SchemeRegistry::find_conflicts();
        assert!(
            conflicts.is_empty(),
            "Scheme name conflicts detected: {:?}",
            conflicts
        );
    }

    #[test]
    fn test_scheme_registry_classify_builtins() {
        assert_eq!(SchemeRegistry::classify("file"), Some(SchemeKind::Builtin));
        assert_eq!(
            SchemeRegistry::classify("series"),
            Some(SchemeKind::Builtin)
        );
        assert_eq!(SchemeRegistry::classify("table"), Some(SchemeKind::Builtin));
        assert_eq!(SchemeRegistry::classify("data"), Some(SchemeKind::Builtin));
    }

    #[test]
    fn test_scheme_registry_classify_format_providers() {
        // csv, oteljson, excelhtml are registered format providers
        assert_eq!(SchemeRegistry::classify("csv"), Some(SchemeKind::Format));
        assert_eq!(
            SchemeRegistry::classify("oteljson"),
            Some(SchemeKind::Format)
        );
        assert_eq!(
            SchemeRegistry::classify("excelhtml"),
            Some(SchemeKind::Format)
        );
    }

    #[test]
    fn test_scheme_registry_classify_factories() {
        // Test factories registered within the provider crate itself
        // External factories (sitegen, hydrovu, remote) are tested in cmd
        // since they're only linked there
        assert_eq!(
            SchemeRegistry::classify("sql-derived-table"),
            Some(SchemeKind::Factory)
        );
        assert_eq!(
            SchemeRegistry::classify("dynamic-dir"),
            Some(SchemeKind::Factory)
        );
    }

    #[test]
    fn test_scheme_registry_classify_unknown() {
        assert_eq!(SchemeRegistry::classify("banana"), None);
        assert_eq!(SchemeRegistry::classify("nosuch"), None);
    }
}
