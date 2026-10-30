// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::env;
use std::path::Path;
use std::path::PathBuf;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use log::debug;
use tinyfs::EntryType;

/// Context needed to create and operate on a Ship
///
/// This captures the pond location and command metadata together,
/// representing everything needed to initialize a Ship with proper transaction tracking.
#[derive(Debug, Clone)]
pub struct ShipContext {
    /// Optional pond path override (None means use POND env var)
    pub pond_path: Option<PathBuf>,
    /// Optional host directory root for host+ URL operations.
    /// When Some, host+ URL paths are resolved relative to this directory.
    /// When None, host+ URLs use absolute paths only.
    pub host_root: Option<PathBuf>,
    /// Overlay mount specifications for factory nodes on hostmount
    pub mount_specs: Vec<tinyfs::hostmount::MountSpec>,
    /// Original command line arguments for transaction metadata
    pub original_args: Vec<String>,
}

impl ShipContext {
    /// Create a new ShipContext from CLI parsing
    #[must_use]
    pub fn new<P: AsRef<Path>, Q: AsRef<Path>>(
        pond_path: Option<P>,
        host_root: Option<Q>,
        mount_specs: Vec<tinyfs::hostmount::MountSpec>,
        original_args: Vec<String>,
    ) -> Self {
        Self {
            pond_path: pond_path.map(|p| p.as_ref().to_path_buf()),
            host_root: host_root.map(|p| p.as_ref().to_path_buf()),
            mount_specs,
            original_args,
        }
    }

    /// Create a ShipContext for pond-only operations (no host root).
    ///
    /// Convenience over the public field constructor; used by unit and
    /// integration tests. Production always has a host root from CLI args, so
    /// the `pond` binary itself never calls this.
    #[allow(dead_code)]
    #[must_use]
    pub fn pond_only<P: AsRef<Path>>(pond_path: Option<P>, original_args: Vec<String>) -> Self {
        Self {
            pond_path: pond_path.map(|p| p.as_ref().to_path_buf()),
            host_root: None,
            mount_specs: Vec::new(),
            original_args,
        }
    }

    /// Resolve the actual pond path using the override or environment variable
    pub fn resolve_pond_path(&self) -> Result<PathBuf> {
        get_pond_path_with_override(self.pond_path.clone())
    }

    /// The CLI argument vector with the binary path (argv[0]) stripped, for
    /// recording in transaction metadata.  This is the real command the
    /// user ran, so `pond log` can faithfully show what each transaction
    /// did instead of a hand-built, often-incomplete label.
    #[must_use]
    pub fn command_args(&self) -> Vec<String> {
        self.original_args.iter().skip(1).cloned().collect()
    }

    /// Transaction metadata carrying the real CLI command (see
    /// [`command_args`](Self::command_args)).  Write commands should pass
    /// this to `write_transaction` / `begin_write` so the audit log is
    /// accurate.
    #[must_use]
    pub fn command_metadata(&self) -> steward::PondUserMetadata {
        steward::PondUserMetadata::new(self.command_args())
    }

    /// Create a Ship for an existing pond (read-only operations)
    pub async fn open_pond(&self) -> Result<steward::Steward> {
        let pond_path = self.resolve_pond_path()?;
        steward::Steward::open_pond(&pond_path)
            .await
            .map_err(|e| anyhow!("Failed to initialize ship: {}", e))
    }

    /// Initialize a new pond (for init command only)
    pub async fn create_pond(&self, birthplace: &str) -> Result<steward::Steward> {
        let pond_path = self.resolve_pond_path()?;
        steward::Steward::create_pond(&pond_path, birthplace)
            .await
            .map_err(|e| anyhow!("Failed to initialize pond: {}", e))
    }

    /// Create pond infrastructure for restoration from bundles
    ///
    /// Unlike `create_pond()`, this creates the pond structure WITHOUT recording
    /// the initial transaction #1. The first bundle will create txn_seq=1 with
    /// the original command metadata from the source pond.
    ///
    /// REQUIRED: preserve_metadata must be provided with the source pond's identity.
    /// Restoration means cloning another pond, not creating a new one.
    ///
    /// Currently only used by tests; kept here so test code can construct
    /// restoration ponds via the same `ShipContext` entry point as production code.
    #[allow(dead_code)]
    pub async fn create_pond_for_restoration(
        &self,
        preserve_metadata: steward::PondMetadata,
    ) -> Result<steward::Steward> {
        let pond_path = self.resolve_pond_path()?;
        steward::Steward::create_pond_for_restoration(&pond_path, preserve_metadata)
            .await
            .map_err(|e| anyhow!("Failed to create pond for restoration: {}", e))
    }

    /// Open a host filesystem steward for host+ URL operations.
    ///
    /// The root path is determined by:
    /// 1. `-d <dir>` flag (if provided)
    /// 2. Otherwise defaults to `/` (absolute paths only)
    ///
    /// This creates a lightweight HostSteward that provides read-only
    /// filesystem access without any Delta Lake or transaction overhead.
    /// If `--hostmount` specs were provided, they are passed through to
    /// the steward for overlay processing.
    pub fn open_host(&self) -> Result<steward::Steward> {
        let root_path = self
            .explicit_host_root()?
            .unwrap_or_else(|| PathBuf::from("/"));

        debug!("Opening host steward at root: {:?}", root_path);
        Ok(steward::Steward::open_host(
            root_path,
            self.mount_specs.clone(),
        ))
    }

    /// Resolve an explicitly supplied `-d` host root without falling back to `/`.
    ///
    /// Exec sandbox host inputs require this explicit operator authorization;
    /// a pond-stored factory configuration must never gain ambient host access.
    pub fn explicit_host_root(&self) -> Result<Option<PathBuf>> {
        let Some(dir) = &self.host_root else {
            return Ok(None);
        };
        let resolved = if dir.is_absolute() {
            dir.clone()
        } else {
            env::current_dir()
                .map_err(|e| anyhow!("Failed to get current directory: {}", e))?
                .join(dir)
        };
        Ok(Some(resolved))
    }
}

/// Result of classifying a CLI argument as pond or host-targeted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetContext {
    /// Argument targets the pond filesystem (default).
    /// The string is the pond-local path/pattern.
    Pond(String),
    /// Argument targets the host filesystem via `host+` prefix.
    /// The string is the host path/pattern (extracted from the URL).
    Host(String),
}

/// Classify a CLI argument string as pond or host-targeted.
///
/// Uses `provider::Url::parse` for URL classification and string-level
/// detection for bare paths and glob patterns (e.g., `**/*` with no scheme).
/// Invalid URLs return errors rather than falling back to pond paths.
///
/// # Examples
///
/// ```text
/// "**/*"                          -> Pond("**/*")
/// "/data/file.csv"                -> Pond("/data/file.csv")
/// "host+file:///tmp/data/**/*"    -> Host("/tmp/data/**/*")
/// "host+csv:///tmp/data.csv"      -> Host("/tmp/data.csv")
/// "csv:///pond/data.csv"          -> Pond("csv:///pond/data.csv")
///     ```
pub fn classify_target(arg: &str) -> Result<TargetContext> {
    // If the argument contains "://", parse it as a proper URL.
    // This handles both `host+scheme:///path` and `scheme:///path`.
    if arg.contains("://") {
        let url =
            provider::Url::parse(arg).map_err(|e| anyhow!("Invalid target URL '{arg}': {e}"))?;
        if url.is_host() {
            let path = url.path();
            if path.is_empty() {
                Ok(TargetContext::Host("/".to_string()))
            } else {
                Ok(TargetContext::Host(path.to_string()))
            }
        } else {
            // Non-host URL (e.g., csv:///path) -- stays in pond context
            Ok(TargetContext::Pond(arg.to_string()))
        }
    } else if let Some(rest) = arg.strip_prefix("host+") {
        // `host+` without `://` -- bare host path
        Ok(TargetContext::Host(rest.to_string()))
    } else {
        // Bare path or glob pattern -- pond context
        Ok(TargetContext::Pond(arg.to_string()))
    }
}

/// Get the pond path with an optional override, falling back to POND environment variable.
/// Converts relative paths to absolute paths using the current working directory.
pub fn get_pond_path_with_override(override_path: Option<PathBuf>) -> Result<PathBuf> {
    let path = if let Some(path) = override_path {
        path
    } else {
        env::var("POND")
            .map_err(|_| anyhow!("POND environment variable not set"))
            .map(PathBuf::from)?
    };

    // Convert relative paths to absolute paths
    // This is required because Url::from_directory_path() only accepts absolute paths
    let absolute_path = if path.is_absolute() {
        path
    } else {
        env::current_dir()
            .map_err(|e| anyhow!("Failed to get current directory: {}", e))?
            .join(&path)
    };

    Ok(absolute_path)
}

/// Run a read-only operation inside a steward read transaction.
///
/// Centralizes the begin-read / commit / abort boilerplate shared by the
/// read-only commands (`show`, `list`, `cat`, `describe`, `temporal`, ...). The
/// caller opens the appropriate steward (`open_pond` or `open_host`) and passes
/// it in; this begins a read transaction, runs `f` with mutable access to it,
/// commits on success, and aborts (rolls back) on error. Aborting a read
/// transaction has no control-table side effects; it just avoids the
/// "dropped without commit" warning.
///
/// The returned value must not borrow the transaction, so commands compute any
/// owned result inside the closure and render it after this returns.
pub async fn with_read_transaction<'s, F, T>(
    ship: &'s mut steward::Steward,
    args: Vec<String>,
    f: F,
) -> Result<T>
where
    F: for<'a> AsyncFnOnce(&'a mut steward::Transaction<'s>) -> Result<T>,
{
    let mut tx = ship
        .begin_read(&steward::PondUserMetadata::new(args))
        .await
        .map_err(|e| anyhow!("Failed to begin read transaction: {}", e))?;

    let result = match f(&mut tx).await {
        Ok(value) => value,
        Err(e) => {
            _ = tx.abort(&e).await;
            return Err(e);
        }
    };

    _ = tx
        .commit()
        .await
        .map_err(|e| anyhow!("Failed to commit read transaction: {}", e))?;

    Ok(result)
}

/// Run a write operation inside a steward write transaction.
///
/// The write counterpart to [`with_read_transaction`]. Begins a write
/// transaction, runs `f` with mutable access to it (so callers can reach the
/// transaction guard for provider context via `tx.state()`, not just `&FS`),
/// commits on success, and aborts (rolls back) on error.
///
/// This differs from [`steward::Steward::write_transaction`], which only
/// exposes `&FS` and discards the closure's return value; use this helper when
/// the command needs the full [`steward::Transaction`] or must return a value
/// computed inside the transaction.
///
/// The returned value must not borrow the transaction, so commands compute any
/// owned result inside the closure and render it after this returns.
pub async fn with_write_transaction<'s, F, T>(
    ship: &'s mut steward::Steward,
    args: Vec<String>,
    f: F,
) -> Result<T>
where
    F: for<'a> AsyncFnOnce(&'a mut steward::Transaction<'s>) -> Result<T>,
{
    let mut tx = ship
        .begin_write(&steward::PondUserMetadata::new(args))
        .await
        .map_err(|e| anyhow!("Failed to begin write transaction: {}", e))?;

    let result = match f(&mut tx).await {
        Ok(value) => value,
        Err(e) => {
            _ = tx.abort(&e).await;
            return Err(e);
        }
    };

    _ = tx
        .commit()
        .await
        .map_err(|e| anyhow!("Failed to commit write transaction: {}", e))?;

    Ok(result)
}

/// Number of hex characters to show when shortening UUID7 values
/// This matches the rightmost block (12 characters) of a UUID
const UUID_SHORT_LENGTH: usize = 12;

/// Helper function to format node IDs in a friendly way with square brackets
/// For UUID7 strings, shows last 12 hex characters (rightmost block)
/// For hex strings, shows appropriate number of digits based on magnitude
/// Always wraps the result in square brackets for consistency
#[must_use]
pub fn format_node_id(node_id: &impl std::fmt::Display) -> String {
    let node_id = node_id.to_string();
    // @@@ YUCK Use Uuid, fail, do not fallback
    // Check if this looks like a UUID7 (contains hyphens)
    let id_str = if node_id.contains('-') {
        // UUID7 format - take last UUID_SHORT_LENGTH hex characters (rightmost block)
        // This avoids timestamp collisions when UUIDs are generated rapidly
        let hex_only: String = node_id.chars().filter(|c| c.is_ascii_hexdigit()).collect();
        let len = hex_only.len();
        if len >= UUID_SHORT_LENGTH {
            hex_only[len - UUID_SHORT_LENGTH..].to_string()
        } else {
            hex_only
        }
    } else {
        // Non-UUID hex format — show last UUID_SHORT_LENGTH hex characters
        let hex_only: String = node_id.chars().filter(|c| c.is_ascii_hexdigit()).collect();
        let len = hex_only.len();
        if len >= UUID_SHORT_LENGTH {
            hex_only[len - UUID_SHORT_LENGTH..].to_string()
        } else {
            hex_only
        }
    };

    format!("[{}]", id_str)
}

/// Helper function to format file sizes
#[must_use]
pub fn format_file_size(size: u64) -> String {
    if size >= 1024 * 1024 {
        format!("{:.1}MB", size as f64 / (1024.0 * 1024.0))
    } else if size >= 1024 {
        format!("{:.1}KB", size as f64 / 1024.0)
    } else {
        format!("{}B", size)
    }
}

/// Helper struct to store file information for Watertown-specific output
#[derive(Debug)]
pub struct FileInfo {
    pub path: String,
    pub node_id: tinyfs::FileID,
    pub metadata: tinyfs::NodeMetadata,
    pub symlink_target: Option<String>,
}

impl FileInfo {
    /// Format in Watertown-specific style showing meaningful metadata
    #[must_use]
    pub fn format_watertown_style(&self) -> String {
        self.format_with_options(false)
    }

    /// Format with full entry type name shown
    #[must_use]
    pub fn format_long_style(&self) -> String {
        self.format_with_options(true)
    }

    fn format_with_options(&self, long: bool) -> String {
        let type_symbol = match self.metadata.entry_type {
            EntryType::DirectoryPhysical | EntryType::DirectoryDynamic => "[DIR]",
            EntryType::Symlink => "[LINK]",
            EntryType::FilePhysicalVersion
            | EntryType::FilePhysicalSeries
            | EntryType::FileDynamic => "[FILE]",
            EntryType::TablePhysicalVersion => "[TBL]",
            EntryType::TablePhysicalSeries | EntryType::TableDynamic => "[SER]",
        };

        let entry_type_str = if long {
            let name = match self.metadata.entry_type {
                EntryType::DirectoryPhysical => "directory",
                EntryType::DirectoryDynamic => "dynamic-directory",
                EntryType::Symlink => "symlink",
                EntryType::FilePhysicalVersion => "data",
                EntryType::FilePhysicalSeries => "data:series",
                EntryType::FileDynamic => "data:dynamic",
                EntryType::TablePhysicalVersion => "table",
                EntryType::TablePhysicalSeries => "table:series",
                EntryType::TableDynamic => "table:dynamic",
            };
            format!(" {:<17}", name)
        } else {
            String::new()
        };

        let size_str = if self.metadata.entry_type.is_directory() {
            "-".to_string()
        } else {
            format_file_size(self.metadata.size.unwrap_or(0))
        };

        let timestamp_us = self.metadata.timestamp;
        let dt = chrono::DateTime::from_timestamp(
            timestamp_us / 1_000_000,
            ((timestamp_us % 1_000_000) * 1000) as u32,
        )
        .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).expect("ok"));

        let time_str = dt.format("%Y-%m-%d %H:%M:%S").to_string();

        // Show two provenance tags: the node's own id (unique per entry, for
        // identity) and the owning pond's id (shared within a pond, which
        // reveals the origin pond for cross-pond imports). This mirrors the
        // FileID Display form `[node]@[pond]`.
        let node_id_str = format!(
            "{}@{}",
            format_node_id(&self.node_id.node_id()),
            format_node_id(&self.node_id.pond_id())
        );

        let version_str = format!("v{}", self.metadata.version);

        let symlink_part = if let Some(target) = &self.symlink_target {
            format!(" -> {}", target)
        } else {
            String::new()
        };

        format!(
            "{}{} {:>8} {:>8} {} {} {}{}\n",
            type_symbol,
            entry_type_str,
            size_str,
            node_id_str,
            version_str,
            time_str,
            self.path,
            symlink_part
        )
    }
}

/// Visitor implementation to collect file information
pub struct FileInfoVisitor {
    show_all: bool,
}

impl FileInfoVisitor {
    #[must_use]
    pub fn new(show_all: bool) -> Self {
        Self { show_all }
    }
}

#[async_trait]
impl tinyfs::Visitor<FileInfo> for FileInfoVisitor {
    async fn visit(
        &mut self,
        node: tinyfs::NodePath,
        _captured: &[String],
    ) -> tinyfs::Result<FileInfo> {
        let path = node.path().to_string_lossy().to_string();

        // Skip hidden files unless --all is specified
        let basename = node.basename();
        if !self.show_all && basename.starts_with('.') && basename != "." && basename != ".." {
            return Err(tinyfs::Error::Other("Hidden file skipped".to_string()));
        }

        // Extract metadata that we can access from the node
        let node_id = node.id();

        if let Some(file_handle) = node.into_file().await {
            // Get consolidated metadata from the file handle
            let metadata = file_handle.metadata().await?;
            let entry_type_str = metadata.entry_type.as_str();
            let size_val = metadata.size.unwrap_or(0);

            let final_type_str = metadata.entry_type.as_str();
            let version = metadata.version;
            debug!(
                "FileInfoVisitor: Successfully got metadata - entry_type={entry_type_str}, version={version}, size={size_val}"
            );

            debug!("FileInfoVisitor: Final FileInfo will have node_type={final_type_str}");

            Ok(FileInfo {
                path,
                metadata,
                symlink_target: None,
                node_id,
            })
        } else if let Some(dir_handle) = node.into_dir().await {
            let metadata = dir_handle.handle.metadata().await?;
            Ok(FileInfo {
                path,
                metadata,
                symlink_target: None,
                node_id,
            })
        } else if let Some(symlink_handle) = node.into_symlink().await {
            let target = symlink_handle.handle.readlink().await.unwrap_or_default();
            let metadata = symlink_handle.handle.metadata().await?;

            Ok(FileInfo {
                path,
                metadata,
                symlink_target: Some(target.to_string_lossy().to_string()),
                node_id,
            })
        } else {
            Err(tinyfs::Error::Other("Unknown node type".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_get_pond_path_with_override_absolute_path() {
        // Absolute path should be returned as-is
        let absolute_path = PathBuf::from("/some/absolute/path");
        let result = get_pond_path_with_override(Some(absolute_path.clone())).unwrap();
        assert_eq!(result, absolute_path);
        assert!(result.is_absolute());
    }

    #[test]
    fn test_get_pond_path_with_override_relative_path() {
        // Relative path should be converted to absolute
        let relative_path = PathBuf::from("pond");
        let result = get_pond_path_with_override(Some(relative_path.clone())).unwrap();

        // Result should be absolute
        assert!(
            result.is_absolute(),
            "Expected absolute path, got: {:?}",
            result
        );

        // Result should end with the relative path component
        assert!(
            result.ends_with("pond"),
            "Expected path to end with 'pond', got: {:?}",
            result
        );

        // Result should be prefixed with current working directory
        let cwd = env::current_dir().unwrap();
        assert_eq!(result, cwd.join("pond"));
    }

    #[test]
    fn test_get_pond_path_with_override_relative_nested_path() {
        // Nested relative path should be converted to absolute
        let relative_path = PathBuf::from("some/nested/pond");
        let result = get_pond_path_with_override(Some(relative_path)).unwrap();

        assert!(result.is_absolute());
        assert!(result.ends_with("some/nested/pond"));

        let cwd = env::current_dir().unwrap();
        assert_eq!(result, cwd.join("some/nested/pond"));
    }

    #[test]
    fn test_get_pond_path_with_override_dot_relative_path() {
        // Relative path with ./ prefix should be converted to absolute
        let relative_path = PathBuf::from("./my_pond");
        let result = get_pond_path_with_override(Some(relative_path)).unwrap();

        assert!(
            result.is_absolute(),
            "Expected absolute path, got: {:?}",
            result
        );

        let cwd = env::current_dir().unwrap();
        assert_eq!(result, cwd.join("./my_pond"));
    }

    #[test]
    fn test_get_pond_path_with_override_parent_relative_path() {
        // Relative path with ../ should be converted to absolute
        let relative_path = PathBuf::from("../sibling/pond");
        let result = get_pond_path_with_override(Some(relative_path)).unwrap();

        assert!(
            result.is_absolute(),
            "Expected absolute path, got: {:?}",
            result
        );

        let cwd = env::current_dir().unwrap();
        assert_eq!(result, cwd.join("../sibling/pond"));
    }

    // --- classify_target tests ---

    #[test]
    fn test_classify_bare_glob_is_pond() {
        assert_eq!(
            classify_target("**/*").expect("valid target"),
            TargetContext::Pond("**/*".into())
        );
    }

    #[test]
    fn test_classify_absolute_path_is_pond() {
        assert_eq!(
            classify_target("/data/file.csv").expect("valid target"),
            TargetContext::Pond("/data/file.csv".into())
        );
    }

    #[test]
    fn test_classify_host_file_url() {
        assert_eq!(
            classify_target("host+file:///tmp/data/**/*").expect("valid target"),
            TargetContext::Host("/tmp/data/**/*".into())
        );
    }

    #[test]
    fn test_classify_host_csv_url() {
        assert_eq!(
            classify_target("host+csv:///tmp/data.csv").expect("valid target"),
            TargetContext::Host("/tmp/data.csv".into())
        );
    }

    #[test]
    fn test_classify_host_empty_path() {
        assert_eq!(
            classify_target("host+file:///").expect("valid target"),
            TargetContext::Host("/".into())
        );
    }

    #[test]
    fn test_classify_host_no_scheme_separator() {
        // host+ without :// -- bare path after host+
        assert_eq!(
            classify_target("host+somepath").expect("valid target"),
            TargetContext::Host("somepath".into())
        );
    }

    #[test]
    fn test_classify_pond_url_no_host_prefix() {
        assert_eq!(
            classify_target("csv:///pond/data.csv").expect("valid target"),
            TargetContext::Pond("csv:///pond/data.csv".into())
        );
    }

    #[test]
    fn test_classify_invalid_urls_fail_instead_of_becoming_pond_paths() {
        for target in [
            "hostfs+xls://../../caspar_water_bank/sbmc_register.xls",
            "host+xls://../../caspar_water_bank/sbmc_register.xls",
            "csv:///data.csv#fragment",
        ] {
            let error = classify_target(target)
                .expect_err("invalid URL")
                .to_string();
            assert!(error.contains("Invalid target URL"), "{error}");
        }
    }
}
