// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::common::ShipContext;
use anyhow::{Result, anyhow};
use std::future::Future;
use std::pin::Pin;
use tinyfs::ResultExt;

/// Direction of copy operation
#[derive(Debug, PartialEq)]
enum CopyDirection {
    /// Copying from host filesystem into pond
    In,
    /// Copying from pond to host filesystem
    Out,
}

/// Parsed source for copy IN operations.
/// Extracted from source URLs like `host:///path`, `host+table:///path`, etc.
#[derive(Debug, Clone)]
struct CopySource {
    /// Absolute path on host filesystem
    host_path: String,
    /// Entry type to store in pond (derived from URL entry_type suffix)
    entry_type: tinyfs::EntryType,
}

/// Check if a CLI argument targets the host filesystem.
/// Recognizes both `host+scheme:///path` and `host:///path` forms.
fn is_host_target(s: &str) -> bool {
    s.starts_with("host+") || s.starts_with("host:///")
}

/// Parse a source URL into host path and entry type.
///
/// Supports:
/// - `host:///path`            -> data (FilePhysicalVersion)
/// - `host+file:///path`       -> data (FilePhysicalVersion)
/// - `host+table:///path`      -> table (TablePhysicalVersion)
/// - `host+series:///path`     -> series (TablePhysicalSeries)
fn parse_copy_source(source: &str) -> Result<CopySource> {
    // Normalize host:///path to host+file:///path for URL parsing
    let url_str = if let Some(rest) = source.strip_prefix("host:///") {
        format!("host+file:///{}", rest)
    } else {
        source.to_string()
    };

    let url = provider::Url::parse(&url_str)
        .map_err(|e| anyhow!("Invalid source URL '{}': {}", source, e))?;

    if !url.is_host() {
        return Err(anyhow!(
            "Source '{}' must target host filesystem. Use host:///path for data, \
             host+table:///path for parquet tables, or host+series:///path for time-series.",
            source
        ));
    }

    let host_path = url.path().to_string();
    let entry_type = match url.entry_type() {
        Some("table") => tinyfs::EntryType::TablePhysicalVersion,
        Some("series") => tinyfs::EntryType::TablePhysicalSeries,
        _ => tinyfs::EntryType::FilePhysicalVersion,
    };

    Ok(CopySource {
        host_path,
        entry_type,
    })
}

/// Extract the host filesystem path from a destination URL.
/// Supports `host:///path` form.
fn extract_host_dest_path(dest: &str) -> Result<String> {
    if let Some(path) = dest.strip_prefix("host://") {
        Ok(path.to_string())
    } else if dest.starts_with("host+") {
        let url = provider::Url::parse(dest)
            .map_err(|e| anyhow!("Invalid destination URL '{}': {}", dest, e))?;
        Ok(url.path().to_string())
    } else {
        Err(anyhow!(
            "Destination '{}' does not have host:// prefix",
            dest
        ))
    }
}

/// Determine copy direction based on sources and destination.
///
/// - If dest has host:// prefix -> copying OUT (pond -> host)
/// - If sources have host:// or host+ prefix -> copying IN (host -> pond)
/// - Both or neither is an error.
fn determine_copy_direction(sources: &[String], dest: &str) -> Result<CopyDirection> {
    let dest_is_host = is_host_target(dest);
    let sources_have_host = sources.iter().any(|s| is_host_target(s));

    match (dest_is_host, sources_have_host) {
        (true, true) => Err(anyhow!(
            "Invalid copy: cannot use host:// prefix on both sources and destination"
        )),
        (true, false) => Ok(CopyDirection::Out),
        (false, true) => {
            // Verify ALL sources have host prefix for consistency
            for source in sources {
                if !is_host_target(source) {
                    return Err(anyhow!(
                        "Invalid copy: when copying from host, ALL sources must have host:// or host+ prefix"
                    ));
                }
            }
            Ok(CopyDirection::In)
        }
        (false, false) => Err(anyhow!(
            "Copy requires host:// or host+ prefix on either sources or destination.\n\
             Copy IN:  pond copy host:///path/file.csv /dest\n\
             Copy OUT: pond copy /pond/path host:///output/dir\n\
             Entry types: host+table:///path (parquet table), host+series:///path (time-series)"
        )),
    }
}

/// Copy multiple source files into a destination directory.
///
/// Reads through `source_wd` (tinyfs hostmount) and writes to `dest_wd` (tinyfs pond).
async fn copy_files_to_directory(
    sources: &[CopySource],
    source_wd: &tinyfs::WD,
    dest_wd: &tinyfs::WD,
) -> Result<(), Box<dyn std::error::Error>> {
    for source in sources {
        // Extract filename from source path
        let source_filename = std::path::Path::new(&source.host_path)
            .file_name()
            .ok_or("Invalid file path")?
            .to_str()
            .ok_or("Invalid filename")?;

        copy_single_file(
            source_wd,
            &source.host_path,
            dest_wd,
            source_filename,
            source.entry_type,
        )
        .await?;
    }
    Ok(())
}

/// Copy a single file from source WD to destination WD.
///
/// Reads through `source_wd` (tinyfs layer) -- for host sources this means
/// reading through the hostmount persistence layer, not direct tokio::fs.
async fn copy_single_file(
    source_wd: &tinyfs::WD,
    source_path: &str,
    dest_wd: &tinyfs::WD,
    filename: &str,
    entry_type: tinyfs::EntryType,
) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::io::AsyncWriteExt;

    let entry_type_str = format!("{:?}", entry_type);

    log::debug!(
        "copy_single_file source_path: {source_path}, dest_filename: {filename}, entry_type: {entry_type_str}"
    );

    // Open source reader through tinyfs (hostmount or tlogfs)
    let mut source_reader = source_wd
        .async_reader_path(source_path)
        .await
        .map_err(|e| format!("Failed to open source '{}': {}", source_path, e))?;

    // For table and series files, we need to validate it's actually a parquet file
    // Table files: store as-is, Series files: also infer temporal bounds after writing
    if entry_type == tinyfs::EntryType::TablePhysicalSeries
        || entry_type == tinyfs::EntryType::TablePhysicalVersion
    {
        // Check if this is actually a parquet file by reading just the magic bytes
        let mut magic = [0u8; 4];
        use tokio::io::AsyncReadExt;
        let _ = source_reader
            .read_exact(&mut magic)
            .await
            .map_err(|e| format!("Failed to read magic bytes from '{}': {}", source_path, e))?;

        if &magic != b"PAR1" {
            return Err(format!(
                "File '{}' is not a valid parquet file (missing PAR1 magic bytes). \
                Use host:///path (without +table or +series) for non-parquet files. \
                To query CSV files, use 'pond cat host+csv:///path' with the csv:// URL scheme.",
                source_path
            )
            .into());
        }

        // Rewind to beginning for streaming copy
        use tokio::io::AsyncSeekExt;
        let _ = source_reader
            .seek(std::io::SeekFrom::Start(0))
            .await
            .map_err(|e| format!("Failed to rewind source reader: {}", e))?;

        // Create writer and stream the file content efficiently
        let mut dest_writer = dest_wd
            .async_writer_path_with_type(filename, entry_type)
            .await
            .map_err(|e| format!("Failed to create destination writer: {}", e))?;

        // Stream the file bytes without loading into memory
        let _ = tokio::io::copy(&mut source_reader, &mut dest_writer)
            .await
            .map_err(|e| format!("Failed to stream file content: {}", e))?;

        // For series files, infer temporal bounds (reads only footer)
        // This also calls shutdown() internally
        if entry_type == tinyfs::EntryType::TablePhysicalSeries {
            let (min_time, max_time, ts_col) = dest_writer
                .infer_temporal_bounds()
                .await
                .map_err(|e| format!("Failed to infer temporal bounds: {}", e))?;

            log::debug!(
                "Inferred temporal bounds for {}: min={}, max={}, ts_column={}",
                filename,
                min_time,
                max_time,
                ts_col
            );
        } else {
            dest_writer
                .shutdown()
                .await
                .map_err(|e| format!("Failed to complete file write: {}", e))?;
        }
    } else {
        // Non-table files: unified streaming copy through tinyfs
        let mut dest_writer = dest_wd
            .async_writer_path_with_type(filename, entry_type)
            .await
            .map_err(|e| format!("Failed to create destination writer: {}", e))?;

        // Stream copy through tinyfs readers/writers
        _ = tokio::io::copy(&mut source_reader, &mut dest_writer)
            .await
            .map_err(|e| format!("Failed to stream file content: {}", e))?;

        dest_writer
            .shutdown()
            .await
            .map_err(|e| format!("Failed to complete file write: {}", e))?;
    }

    log::debug!("Copied {source_path} to directory as {filename}");
    Ok(())
}

/// Recursively copy directory from host filesystem to pond, preserving structure.
///
/// Both sides go through tinyfs: the host side uses hostmount persistence,
/// the pond side uses tlogfs persistence. This is the Phase 3B-2 dual-steward
/// pattern -- two tinyfs instances open simultaneously.
async fn copy_directory_recursive(
    ship_context: &ShipContext,
    host_source_path: &str,
    pond_dest: &str,
) -> Result<()> {
    // Phase 3B-2: Open BOTH host and pond stewards concurrently
    let mut host_ship = ship_context.open_host()?;
    let host_tx = host_ship
        .begin_read(&steward::PondUserMetadata::new(vec![
            "copy-recursive-source".to_string(),
        ]))
        .await
        .map_err(|e| anyhow!("Failed to begin host transaction: {}", e))?;
    let host_root = host_tx.root().await?;

    // Navigate to the source directory in the hostmount
    let host_source_wd = host_root
        .open_dir_path(host_source_path)
        .await
        .map_err(|e| anyhow!("Source '{}' is not a directory: {}", host_source_path, e))?;

    let mut pond_ship = ship_context.open_pond().await?;
    let pond_dest = pond_dest.to_string();

    pond_ship
        .write_transaction(
            &ship_context.command_metadata(),
            async |fs| {
                let root = fs.root().await?;

                // Navigate to or create destination directory
                let dest_wd = if pond_dest != "/" {
                    let clean_dest = pond_dest.trim_end_matches('/');
                    root.create_dir_path(clean_dest).await?
                } else {
                    root
                };

                // Dual traversal helper: reads from host WD, writes to pond WD
                fn copy_dir_contents<'a>(
                    host_wd: &'a tinyfs::WD,
                    pond_wd: tinyfs::WD,
                ) -> Pin<
                    Box<dyn Future<Output = Result<(u32, u32), steward::StewardError>> + Send + 'a>,
                > {
                    Box::pin(async move {
                        use futures::StreamExt;
                        let mut dir_count = 0u32;
                        let mut file_count = 0u32;

                        // List entries through hostmount tinyfs (not tokio::fs::read_dir)
                        let mut entries_stream = host_wd.entries().await.map_err(|e| {
                            tinyfs::Error::Other(format!(
                                "Failed to list host directory entries: {}",
                                e
                            ))
                        })?;

                        while let Some(entry_result) = entries_stream.next().await {
                            let entry = entry_result.map_err(|e| {
                                tinyfs::Error::Other(format!("Failed to read entry: {}", e))
                            })?;

                            let name = &entry.name;

                            if entry.entry_type.is_directory() {
                                // Navigate into subdirectory on host side
                                let host_child_wd =
                                    host_wd.open_dir_path(name).await.map_err(|e| {
                                        tinyfs::Error::Other(format!(
                                            "Failed to open host subdirectory '{}': {}",
                                            name, e
                                        ))
                                    })?;

                                // Create or open directory in pond
                                let name_for_lookup = name.clone();
                                let child_pond_wd = pond_wd
                                    .in_path(name, |parent_wd, lookup| async move {
                                        match lookup {
                                            tinyfs::Lookup::NotFound(_, ref name) => {
                                                parent_wd.create_dir_path(name).await
                                            }
                                            tinyfs::Lookup::Found(_) => {
                                                parent_wd.open_dir_path(&name_for_lookup).await
                                            }
                                            tinyfs::Lookup::Empty(_) => {
                                                Err(tinyfs::Error::empty_path())
                                            }
                                        }
                                    })
                                    .await?;

                                dir_count += 1;

                                // Recurse into subdirectory
                                let (sub_dirs, sub_files) =
                                    copy_dir_contents(&host_child_wd, child_pond_wd).await?;
                                dir_count += sub_dirs;
                                file_count += sub_files;
                            } else if entry.entry_type.is_file() {
                                // Copy file: read from host WD, write to pond WD
                                let ext = std::path::Path::new(name.as_str())
                                    .extension()
                                    .and_then(|e| e.to_str());
                                let entry_type = match ext {
                                    Some("series") => tinyfs::EntryType::TablePhysicalSeries,
                                    Some("table") => tinyfs::EntryType::TablePhysicalVersion,
                                    _ => tinyfs::EntryType::FilePhysicalVersion,
                                };

                                copy_single_file(host_wd, name, &pond_wd, name, entry_type)
                                    .await
                                    .map_err(|e| {
                                        tinyfs::Error::Other(format!(
                                            "Failed to copy file '{}': {}",
                                            name, e
                                        ))
                                    })?;

                                file_count += 1;
                            }
                        }

                        Ok((dir_count, file_count))
                    })
                }

                let (dirs, files) = copy_dir_contents(&host_source_wd, dest_wd).await?;
                log::info!(
                    "Recursively copied {} directories and {} files from {:?} to {:?}",
                    dirs,
                    files,
                    host_source_path,
                    pond_dest
                );
                Ok(())
            },
        )
        .await
        .map_err(|e| anyhow!("Recursive copy failed: {}", e))?;

    // Commit host transaction (no-op for hostmount)
    let _ = host_tx
        .commit()
        .await
        .map_err(|e| anyhow!("Failed to commit host transaction: {}", e))?;

    Ok(())
}

/// Copy files INTO the pond from host filesystem
///
/// Phase 3B-2: Dual-steward pattern. Opens BOTH a host steward (for reading
/// source files through hostmount tinyfs) and a pond steward (for writing
/// to the pond through tlogfs tinyfs). All I/O goes through the tinyfs layer.
///
/// Source URLs encode the entry type:
/// - `host:///path` or `host+file:///path` -> raw data
/// - `host+table:///path` -> queryable parquet table
/// - `host+series:///path` -> time-series parquet with temporal bounds
///
/// Uses scoped transactions for automatic commit/rollback handling.
async fn copy_in(ship_context: &ShipContext, sources: &[String], dest: &str) -> Result<()> {
    log::debug!("COPY_IN: dual-steward-v4.0");

    // Validate arguments
    if sources.is_empty() {
        return Err(anyhow!("At least one source file must be specified"));
    }

    // Parse all source URLs to get host paths and entry types
    let parsed_sources: Vec<CopySource> = sources
        .iter()
        .map(|s| parse_copy_source(s))
        .collect::<Result<Vec<_>>>()?;

    // Phase 3B-2: Open host steward for reading sources through hostmount
    let mut host_ship = ship_context.open_host()?;
    let host_tx = host_ship
        .begin_read(&steward::PondUserMetadata::new(vec![
            "copy-in-source".to_string(),
        ]))
        .await
        .map_err(|e| anyhow!("Failed to begin host transaction: {}", e))?;
    let host_root = host_tx.root().await?;

    // Check if we have a single source that is a directory on the host
    // Do this via hostmount tinyfs, not tokio::fs::metadata
    if parsed_sources.len() == 1 {
        let source_path = &parsed_sources[0].host_path;
        if host_root.open_dir_path(source_path).await.is_ok() {
            // Commit host_tx before delegating (copy_directory_recursive opens its own)
            let _ = host_tx
                .commit()
                .await
                .map_err(|e| anyhow!("Failed to commit host transaction: {}", e))?;

            log::info!(
                "Detected directory copy from host: {} -> {}",
                source_path,
                dest
            );
            return copy_directory_recursive(ship_context, source_path, dest).await;
        }
    }

    // Open pond steward for writing
    let mut pond_ship = ship_context.open_pond().await?;
    let dest = dest.to_string();

    // Use scoped write transaction on the pond side
    pond_ship.write_transaction(
        &ship_context.command_metadata(),
        async |pond_fs| {
            let pond_root = pond_fs.root().await?;

            let copy_result = pond_root.resolve_copy_destination(&dest).await;
            match copy_result {
                Ok((dest_wd, dest_type)) => {
                    match dest_type {
                        tinyfs::CopyDestination::Directory | tinyfs::CopyDestination::ExistingDirectory => {
                            // Destination is a directory - copy files into it
                            copy_files_to_directory(&parsed_sources, &host_root, &dest_wd).await
                                .map_other_context("Copy to directory failed")?;
                            Ok(())
                        }
                        tinyfs::CopyDestination::ExistingFile => {
                            // Destination is an existing file -- overwrite it (Unix cp semantics).
                            if parsed_sources.len() == 1 {
                                let source = &parsed_sources[0];
                                let dest_filename = std::path::Path::new(dest.as_str())
                                    .file_name()
                                    .and_then(|f| f.to_str())
                                    .unwrap_or(&dest);
                                copy_single_file(&host_root, &source.host_path, &dest_wd, dest_filename, source.entry_type).await
                                    .map_other_context("Failed to overwrite file")?;
                                log::info!("Overwrote {}", dest);
                                Ok(())
                            } else {
                                Err(tinyfs::Error::Other(
                                    format!("When copying multiple files, destination '{}' must be a directory", dest)
                                ).into())
                            }
                        }
                        tinyfs::CopyDestination::NewPath(name) => {
                            // Destination doesn't exist
                            if parsed_sources.len() == 1 {
                                let source = &parsed_sources[0];
                                copy_single_file(&host_root, &source.host_path, &dest_wd, &name, source.entry_type).await
                                    .map_other_context("Failed to copy file")?;
                                log::debug!("Copied {} to {}", source.host_path, name);
                                Ok(())
                            } else {
                                Err(tinyfs::Error::Other(
                                    format!("When copying multiple files, destination '{}' must be an existing directory", dest)
                                ).into())
                            }
                        }
                    }
                }
                Err(e) => {
                    // Check if this is a trailing slash case where we need to create a directory
                    if dest.ends_with('/') {
                        let clean_dest = dest.trim_end_matches('/');
                        _ = pond_root.create_dir_path(clean_dest).await?;
                        let (dest_wd, _) = pond_root.resolve_copy_destination(clean_dest).await?;
                        copy_files_to_directory(&parsed_sources, &host_root, &dest_wd).await
                            .map_other_context("Copy to created directory failed")?;
                        Ok(())
                    } else {
                        Err(tinyfs::Error::Other(format!("Failed to resolve destination '{}': {}", dest, e)).into())
                    }
                }
            }
        }
    ).await.map_err(|e| anyhow!("Copy operation failed: {}", e))?;

    // Commit host transaction (no-op for hostmount)
    let _ = host_tx
        .commit()
        .await
        .map_err(|e| anyhow!("Failed to commit host transaction: {}", e))?;

    log::info!("[OK] File(s) copied into pond successfully");
    Ok(())
}

/// Copy files OUT of the pond to host filesystem
///
/// Phase 3B: Dual-steward pattern. Opens BOTH a pond steward (for reading
/// source files through tlogfs tinyfs) and a host steward (for writing
/// destination files through hostmount tinyfs). All I/O goes through the
/// tinyfs layer.
///
/// Supports glob patterns and maintains directory structure.
async fn copy_out(
    ship_context: &ShipContext,
    sources: &[String],
    dest: &str,
    options: &CopyOptions,
) -> Result<()> {
    use crate::common::FileInfoVisitor;

    // Parse the host:// prefix from destination
    let host_dest = extract_host_dest_path(dest)?;

    log::debug!(
        "COPY_OUT: dual-steward, sources={:?}, dest={}",
        sources,
        host_dest
    );

    // Phase 3B: Open host steward for writing destination files through hostmount
    let mut host_ship = ship_context.open_host()?;
    let host_tx = host_ship
        .begin_write(&steward::PondUserMetadata::new(vec![
            "copy-out-dest".to_string(),
        ]))
        .await
        .map_err(|e| anyhow!("Failed to begin host transaction: {}", e))?;
    let host_root = host_tx.root().await?;

    // Open pond steward for reading source files
    let mut pond_ship = ship_context.open_pond().await?;
    let pond_tx = pond_ship
        .begin_read(&steward::PondUserMetadata::new(vec![
            "copy-out".to_string(),
            dest.to_string(),
        ]))
        .await
        .map_err(|e| anyhow!("Failed to begin pond transaction: {}", e))?;
    let pond_root = (*pond_tx).root().await?;
    let provider_ctx = pond_tx.provider_context()?;

    // Collect all matching files from all patterns
    let mut all_files = Vec::new();

    for pattern in sources {
        let mut visitor = FileInfoVisitor::new(true); // Show all files
        let results = pond_root
            .visit_with_visitor(pattern, &mut visitor)
            .await
            .map_err(|e| anyhow!("Failed to match pattern '{}': {}", pattern, e))?;

        log::debug!("Pattern '{}' matched {} files", pattern, results.len());
        all_files.extend(results);
    }

    if all_files.is_empty() {
        _ = pond_tx.commit().await?;
        let _ = host_tx
            .commit()
            .await
            .map_err(|e| anyhow!("Failed to commit host transaction: {}", e))?;
        return Err(anyhow!("No files matched the specified patterns"));
    }

    let file_count = all_files.len();
    log::info!("Found {} file(s) to export", file_count);

    // Export each file
    for file_info in all_files {
        // Skip directories
        if file_info.metadata.entry_type.is_directory() {
            continue;
        }

        // Compute output path: dest/relative_path
        // Remove leading slash from pond path to make it relative
        let pond_path = &file_info.path;
        let relative_path = if let Some(prefix) = &options.strip_prefix {
            // Normalize prefix: ensure it starts with / and doesn't end with /
            let prefix = if prefix.starts_with('/') {
                prefix.to_string()
            } else {
                format!("/{}", prefix)
            };
            let prefix = prefix.trim_end_matches('/');
            if let Some(stripped) = pond_path.strip_prefix(prefix) {
                stripped.trim_start_matches('/').to_string()
            } else {
                log::warn!(
                    "Path '{}' does not start with strip-prefix '{}', using full path",
                    pond_path,
                    prefix
                );
                pond_path.trim_start_matches('/').to_string()
            }
        } else {
            pond_path.trim_start_matches('/').to_string()
        };
        let output_path = format!("{}/{}", host_dest.trim_end_matches('/'), relative_path);

        // Create parent directories on host through tinyfs
        if let Some(parent) = std::path::Path::new(&output_path).parent()
            && parent != std::path::Path::new("")
            && parent != std::path::Path::new("/")
        {
            _ = host_root
                .create_dir_all(parent)
                .await
                .map_err(|e| anyhow!("Failed to create directory {:?}: {}", parent, e))?;
        }

        log::debug!("Exporting {} to {}", file_info.path, output_path);

        // Determine export method based on file type
        if file_info.metadata.entry_type.is_series_file()
            || file_info.metadata.entry_type.is_table_file()
        {
            // Export as parquet via DataFusion SQL -> AsyncArrowWriter -> host tinyfs
            export_queryable_file_as_parquet(
                &pond_root,
                &host_root,
                &file_info.path,
                &output_path,
                &provider_ctx,
            )
            .await?;
        } else {
            // Export raw bytes: pond reader -> host writer
            export_raw_file(&pond_root, &host_root, &file_info.path, &output_path).await?;
        }
    }

    // Commit both transactions
    _ = pond_tx.commit().await?;
    let _ = host_tx
        .commit()
        .await
        .map_err(|e| anyhow!("Failed to commit host transaction: {}", e))?;

    log::info!("[OK] {} file(s) exported successfully", file_count);
    Ok(())
}

/// Export a file:series or file:table as parquet
///
/// Uses AsyncArrowWriter to stream RecordBatches from DataFusion SQL
/// directly to the host filesystem through hostmount tinyfs.
async fn export_queryable_file_as_parquet(
    pond_root: &tinyfs::WD,
    host_root: &tinyfs::WD,
    pond_path: &str,
    host_output_path: &str,
    provider_ctx: &tinyfs::ProviderContext,
) -> Result<()> {
    use datafusion::physical_plan::SendableRecordBatchStream;
    use futures::stream::StreamExt;
    use parquet::arrow::AsyncArrowWriter;
    use tokio::io::AsyncWriteExt;

    // Use default SELECT * query sorted by timestamp
    let sql_query = "SELECT * FROM source ORDER BY timestamp";

    let stream: SendableRecordBatchStream =
        tlogfs::execute_sql_on_file(pond_root, pond_path, sql_query, provider_ctx)
            .await
            .map_err(|e| anyhow!("Failed to execute SQL query on '{}': {}", pond_path, e))?;

    // Get the first batch to determine schema
    let mut stream = stream;
    let first_batch = match stream.next().await {
        Some(Ok(batch)) => batch,
        Some(Err(e)) => return Err(anyhow!("Failed to read first batch: {}", e)),
        None => {
            log::debug!("Empty file, creating empty output");
            // Create empty file on host through tinyfs
            let mut writer = host_root
                .async_writer_path(host_output_path)
                .await
                .map_err(|e| {
                    anyhow!("Failed to create output file '{}': {}", host_output_path, e)
                })?;
            writer
                .shutdown()
                .await
                .map_err(|e| anyhow!("Failed to finalize empty file: {}", e))?;
            return Ok(());
        }
    };

    let schema = first_batch.schema();

    // Create async parquet writer on host filesystem through tinyfs
    let host_writer = host_root
        .async_writer_path(host_output_path)
        .await
        .map_err(|e| anyhow!("Failed to create output file '{}': {}", host_output_path, e))?;

    let props = utilities::parquet_policy::writer_properties(
        &schema,
        parquet::file::properties::WriterProperties::builder(),
    )?;
    let mut writer = AsyncArrowWriter::try_new(host_writer, schema.clone(), Some(props))
        .map_err(|e| anyhow!("Failed to create parquet writer: {}", e))?;

    // Write first batch
    utilities::parquet_policy::validate_batch(&first_batch)?;
    writer
        .write(&first_batch)
        .await
        .map_err(|e| anyhow!("Failed to write batch to parquet: {}", e))?;

    // Stream remaining batches
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(|e| anyhow!("Failed to read batch from stream: {}", e))?;
        utilities::parquet_policy::validate_batch(&batch)?;
        writer
            .write(&batch)
            .await
            .map_err(|e| anyhow!("Failed to write batch to parquet: {}", e))?;
    }

    let metadata = writer
        .close()
        .await
        .map_err(|e| anyhow!("Failed to close parquet writer: {}", e))?;
    utilities::parquet_policy::validate_metadata(&schema, &metadata)?;

    log::debug!("Exported parquet file to {}", host_output_path);
    Ok(())
}

/// Export a regular file as raw bytes
///
/// Streams raw bytes from the pond through tinyfs reader to the host
/// filesystem through tinyfs writer. No intermediate buffering.
async fn export_raw_file(
    pond_root: &tinyfs::WD,
    host_root: &tinyfs::WD,
    pond_path: &str,
    host_output_path: &str,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    let mut reader = pond_root
        .async_reader_path(pond_path)
        .await
        .map_err(|e| anyhow!("Failed to open file '{}': {}", pond_path, e))?;

    let mut writer = host_root
        .async_writer_path(host_output_path)
        .await
        .map_err(|e| anyhow!("Failed to create output file '{}': {}", host_output_path, e))?;

    // Stream content from pond to host through tinyfs
    _ = tokio::io::copy(&mut reader, &mut writer)
        .await
        .map_err(|e| anyhow!("Failed to copy file content: {}", e))?;

    writer
        .shutdown()
        .await
        .map_err(|e| anyhow!("Failed to complete file write: {}", e))?;

    log::debug!("Exported raw file to {}", host_output_path);
    Ok(())
}

/// Options for copy operations.
///
/// Uses `Default` so callers only specify what they care about.
/// New flags go here -- existing callers never break.
#[derive(Debug, Default)]
pub struct CopyOptions {
    /// Strip a leading path prefix from pond paths when copying OUT.
    /// e.g. strip_prefix="/hydrovu" copies /hydrovu/devices/123/foo.series
    /// to <dest>/devices/123/foo.series instead of <dest>/hydrovu/devices/123/foo.series
    pub strip_prefix: Option<String>,
}

/// Main copy command dispatcher
///
/// Determines copy direction and dispatches to appropriate handler:
/// - If dest has "host://" prefix -> copy OUT (pond -> host)
/// - If sources have "host://" or "host+" prefix -> copy IN (host -> pond)
///
/// Entry type for copy IN is determined by the source URL:
/// - `host:///path` -> data (raw bytes)
/// - `host+table:///path` -> queryable parquet table
/// - `host+series:///path` -> time-series parquet with temporal bounds
pub async fn copy_command(
    ship_context: &ShipContext,
    sources: &[String],
    dest: &str,
    options: &CopyOptions,
) -> Result<()> {
    // Validate arguments
    if sources.is_empty() {
        return Err(anyhow!("At least one source file must be specified"));
    }

    // Determine copy direction
    let direction = determine_copy_direction(sources, dest)?;

    match direction {
        CopyDirection::In => {
            log::debug!("Copy direction: IN (host -> pond)");
            if options.strip_prefix.is_some() {
                log::warn!("--strip-prefix is ignored when copying in (only applies to copy out)");
            }
            copy_in(ship_context, sources, dest).await
        }
        CopyDirection::Out => {
            log::debug!("Copy direction: OUT (pond -> host)");
            copy_out(ship_context, sources, dest, options).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CopyOptions, copy_command};
    use crate::commands::init::init_command;
    use crate::common::ShipContext;
    use anyhow::Result;
    use arrow_array::{Float64Array, Int64Array, RecordBatch, record_batch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tinyfs::ResultExt;
    use tinyfs::arrow::ParquetExt;
    use tokio::fs::File;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_util::bytes::Bytes;

    /// Test setup helper - creates pond and host files for copy testing
    struct TestSetup {
        pond_path: std::path::PathBuf,
        host_files_dir: std::path::PathBuf,
        ship_context: ShipContext,
        _temp_dir: TempDir,
    }

    impl TestSetup {
        async fn new() -> Result<Self> {
            // Create a temporary directory for the test pond
            let temp_dir = TempDir::new()?;
            // Use a unique subdirectory to avoid any conflicts
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let pond_path = temp_dir.path().join(format!("test_pond_{}", timestamp));
            let host_files_dir = temp_dir.path().join("host_files");

            // Create host files directory
            tokio::fs::create_dir_all(&host_files_dir).await?;

            // Create ship context for initialization
            let init_args = vec!["pond".to_string(), "init".to_string()];
            let ship_context = ShipContext::pond_only(Some(&pond_path), init_args.clone());

            // Initialize the pond
            init_command(&ship_context, "test-host").await?;

            Ok(TestSetup {
                pond_path,
                host_files_dir,
                ship_context,
                _temp_dir: temp_dir,
            })
        }

        /// Create a text file in the host filesystem
        async fn create_host_text_file(
            &self,
            filename: &str,
            content: &str,
        ) -> Result<std::path::PathBuf> {
            let file_path = self.host_files_dir.join(filename);
            let mut file = File::create(&file_path).await?;
            file.write_all(content.as_bytes()).await?;
            file.shutdown().await?;
            Ok(file_path)
        }

        /// Create a CSV file in the host filesystem
        async fn create_host_csv_file(&self, filename: &str) -> Result<std::path::PathBuf> {
            let content = "timestamp,value,doubled_value\n2024-01-01T00:00:00Z,42.0,84.0\n2024-01-01T01:00:00Z,43.5,87.0\n";
            self.create_host_text_file(filename, content).await
        }

        /// Create a series-format parquet file in the host filesystem with proper temporal data
        async fn create_host_series_parquet_file(
            &self,
            filename: &str,
        ) -> Result<std::path::PathBuf> {
            let file_path = self.host_files_dir.join(filename);

            let schema = Schema::new(vec![
                Field::new("timestamp", DataType::Int64, false),
                Field::new("value", DataType::Float64, false),
                Field::new("doubled_value", DataType::Float64, false),
            ]);

            let timestamp_array = Int64Array::from(vec![1704067200000_i64, 1704070800000_i64]); // 2024-01-01 timestamps
            let value_array = Float64Array::from(vec![42.0_f64, 43.5_f64]);
            let doubled_value_array = Float64Array::from(vec![84.0_f64, 87.0_f64]);

            let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![
                    Arc::new(timestamp_array),
                    Arc::new(value_array),
                    Arc::new(doubled_value_array),
                ],
            )?;

            let file = std::fs::File::create(&file_path)?;
            let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
            writer.write(&batch)?;
            _ = writer.close()?;

            Ok(file_path)
        }

        /// Copy files into the pond with default options
        async fn copy_in(&self, sources: Vec<String>, dest: &str) -> Result<()> {
            copy_command(&self.ship_context, &sources, dest, &CopyOptions::default()).await
        }

        /// Copy files out of the pond with default options
        async fn copy_out_to(&self, patterns: Vec<String>, host_dest: &str) -> Result<()> {
            copy_command(
                &self.ship_context,
                &patterns,
                &format!("host://{}", host_dest),
                &CopyOptions::default(),
            )
            .await
        }

        /// Verify a file exists in the pond by trying to read it
        async fn verify_file_exists(&self, path: &str) -> Result<bool> {
            let mut ship = steward::Ship::open_pond(&self.pond_path).await?;
            let tx = ship
                .begin_read(&steward::PondUserMetadata::new(vec!["verify".to_string()]))
                .await?;
            let fs = &*tx;
            let root = fs.root().await?;

            let exists = match root.async_reader_path(path).await {
                Ok(_) => true,
                Err(tinyfs::Error::NotFound(_)) => false,
                Err(e) => return Err(e.into()),
            };

            _ = tx.commit().await?;
            Ok(exists)
        }

        /// Read file content from the pond using TinyFS
        async fn read_pond_file_content(&self, path: &str) -> Result<Vec<u8>> {
            let mut ship = steward::Ship::open_pond(&self.pond_path).await?;
            let tx = ship
                .begin_read(&steward::PondUserMetadata::new(vec!["read".to_string()]))
                .await?;
            let fs = &*tx;
            let root = fs.root().await?;

            let mut content = Vec::new();
            {
                let mut reader = root.async_reader_path(path).await?;
                _ = reader.read_to_end(&mut content).await?;
            }

            _ = tx.commit().await?;
            Ok(content)
        }

        /// Verify file metadata by reading the node metadata directly
        async fn verify_file_metadata(
            &self,
            path: &str,
            _expected_type: tinyfs::EntryType,
        ) -> Result<()> {
            let mut ship = steward::Ship::open_pond(&self.pond_path).await?;
            let tx = ship
                .begin_read(&steward::PondUserMetadata::new(vec![
                    "metadata".to_string(),
                ]))
                .await?;
            let fs = &*tx;
            let root = fs.root().await?;

            // Try to read the file to verify it exists and get the entry type info
            // (The exact metadata API is not exposed in the same way, but we can verify by successful read)
            let _reader = root.async_reader_path(path).await.map_err(|e| {
                anyhow::anyhow!("File {} does not exist or wrong type: {}", path, e)
            })?;

            _ = tx.commit().await?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_copy_single_text_file() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create host text file
        let content = "Hello, Watertown!\nThis is a test file.\n";
        let host_file = setup.create_host_text_file("test.txt", content).await?;

        // Copy to pond as data file
        setup
            .copy_in(
                vec![format!("host:///{}", host_file.display())],
                "copied_test.txt",
            )
            .await?;

        // Verify file exists and has correct content
        assert!(
            setup.verify_file_exists("copied_test.txt").await?,
            "File should exist in pond"
        );

        let pond_content = setup.read_pond_file_content("copied_test.txt").await?;
        assert_eq!(
            pond_content,
            content.as_bytes(),
            "File content should match"
        );

        // Verify metadata (simplified check)
        setup
            .verify_file_metadata("copied_test.txt", tinyfs::EntryType::FilePhysicalVersion)
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_parquet_as_series() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create host parquet file with proper temporal data for series
        let host_file = setup
            .create_host_series_parquet_file("test_series.parquet")
            .await?;

        // Copy to pond as series file (entry type encoded in URL)
        setup
            .copy_in(
                vec![format!("host+series:///{}", host_file.display())],
                "copied_series.parquet",
            )
            .await?;

        // Verify file exists in the pond
        assert!(
            setup.verify_file_exists("copied_series.parquet").await?,
            "Series file should exist in pond"
        );

        // Verify content is preserved by reading the parquet file
        let pond_content = setup
            .read_pond_file_content("copied_series.parquet")
            .await?;

        // Verify PAR1 magic bytes (parquet format)
        assert!(
            pond_content.len() > 4,
            "Series file should be larger than 4 bytes"
        );
        assert_eq!(
            &pond_content[0..4],
            b"PAR1",
            "Series file should start with PAR1 magic bytes"
        );

        // Verify we can read it as parquet and get the expected temporal data

        let bytes = Bytes::from(pond_content);
        let mut arrow_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| anyhow::anyhow!("Failed to create arrow reader: {}", e))?
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build arrow reader: {}", e))?;

        let batch = arrow_reader
            .next()
            .ok_or_else(|| anyhow::anyhow!("No batch found in series file"))?
            .map_err(|e| anyhow::anyhow!("Failed to read batch: {}", e))?;

        // Verify the schema matches our test data
        let schema = batch.schema();
        assert_eq!(schema.fields().len(), 3, "Series should have 3 fields");
        assert_eq!(
            schema.field(0).name(),
            "timestamp",
            "First field should be timestamp"
        );
        assert_eq!(
            schema.field(1).name(),
            "value",
            "Second field should be value"
        );
        assert_eq!(
            schema.field(2).name(),
            "doubled_value",
            "Third field should be doubled_value"
        );

        // Verify the actual data values - series temporal data
        assert_eq!(batch.num_rows(), 2, "Should have 2 rows");

        // Check timestamp column (Int64 milliseconds since epoch)
        let timestamp_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("Expected Int64Array for timestamp"))?;
        assert_eq!(timestamp_col.value(0), 1704067200000_i64); // 2024-01-01T00:00:00Z
        assert_eq!(timestamp_col.value(1), 1704070800000_i64); // 2024-01-01T01:00:00Z

        // Check value columns
        let value_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| anyhow::anyhow!("Expected Float64Array for value"))?;
        assert_eq!(value_col.value(0), 42.0);
        assert_eq!(value_col.value(1), 43.5);

        let doubled_value_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| anyhow::anyhow!("Expected Float64Array for doubled_value"))?;
        assert_eq!(doubled_value_col.value(0), 84.0);
        assert_eq!(doubled_value_col.value(1), 87.0);

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_single_csv_as_data() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create host CSV file
        let host_file = setup.create_host_csv_file("test.csv").await?;

        // Copy to pond as data file (CSV is raw data, not a parquet table)
        setup
            .copy_in(
                vec![format!("host:///{}", host_file.display())],
                "copied_data.csv",
            )
            .await?;

        // Verify file exists
        assert!(
            setup.verify_file_exists("copied_data.csv").await?,
            "File should exist in pond"
        );

        // Verify content is preserved
        let pond_content = setup.read_pond_file_content("copied_data.csv").await?;
        let expected_content = "timestamp,value,doubled_value\n2024-01-01T00:00:00Z,42.0,84.0\n2024-01-01T01:00:00Z,43.5,87.0\n";
        assert_eq!(
            String::from_utf8(pond_content)?,
            expected_content,
            "CSV content should be preserved"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_multiple_files_to_directory() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create multiple host files
        let file1 = setup
            .create_host_text_file("file1.txt", "Content of file 1")
            .await?;
        let file2 = setup
            .create_host_text_file("file2.txt", "Content of file 2")
            .await?;
        let file3 = setup.create_host_csv_file("data.csv").await?;

        // Copy all to a directory (use trailing slash to ensure directory creation)
        setup
            .copy_in(
                vec![
                    format!("host:///{}", file1.display()),
                    format!("host:///{}", file2.display()),
                    format!("host:///{}", file3.display()),
                ],
                "uploaded/",
            )
            .await?;

        // Verify all files exist in the directory
        assert!(
            setup.verify_file_exists("uploaded/file1.txt").await?,
            "file1.txt should exist in directory"
        );
        assert!(
            setup.verify_file_exists("uploaded/file2.txt").await?,
            "file2.txt should exist in directory"
        );
        assert!(
            setup.verify_file_exists("uploaded/data.csv").await?,
            "data.csv should exist in directory"
        );

        // Verify content
        let content1 = setup.read_pond_file_content("uploaded/file1.txt").await?;
        assert_eq!(String::from_utf8(content1)?, "Content of file 1");

        let content2 = setup.read_pond_file_content("uploaded/file2.txt").await?;
        assert_eq!(String::from_utf8(content2)?, "Content of file 2");

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_csv_with_data_format() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create CSV file and copy as data format (not table, since CSV isn't parquet)
        let host_file = setup.create_host_csv_file("test.csv").await?;

        // Copy as data (CSV files are stored as raw bytes)
        setup
            .copy_in(
                vec![format!("host:///{}", host_file.display())],
                "formatted_as_data.csv",
            )
            .await?;

        // Verify it was stored successfully
        assert!(
            setup.verify_file_exists("formatted_as_data.csv").await?,
            "File should exist in pond"
        );

        Ok(())
    }

    /// Create a parquet series file directly in the pond - extension for TestSetup
    impl TestSetup {
        async fn create_parquet_series_in_pond(&self, path: &str) -> Result<()> {
            let mut ship = steward::Ship::open_pond(&self.pond_path).await?;

            let path = path.to_string();
            ship.write_transaction(
                &steward::PondUserMetadata::new(vec![
                    "test".to_string(),
                    "create_series".to_string(),
                ]),
                async move |fs| {
                    let root = fs.root().await?;

                    // Create Arrow RecordBatch with temporal data using record_batch! macro
                    let batch = record_batch!(
                        ("timestamp", Int64, [1000_i64, 2000_i64, 3000_i64]),
                        ("value", Float64, [10.5_f64, 20.5_f64, 30.5_f64])
                    )
                    .map_other_context("Arrow error")?;

                    // Write as file:series using ParquetExt with temporal metadata extraction
                    let _ = root
                        .create_series_from_batch(&path, &batch, Some("timestamp"))
                        .await?;

                    Ok(())
                },
            )
            .await?;

            Ok(())
        }
    }

    #[tokio::test]
    async fn test_copy_error_handling() -> Result<()> {
        let setup = TestSetup::new().await?;
        let opts = CopyOptions::default();

        // Test copying non-existent file (with host prefix)
        let result = copy_command(
            &setup.ship_context,
            &["host:///nonexistent/file.txt".to_string()],
            "should_fail.txt",
            &opts,
        )
        .await;
        assert!(
            result.is_err(),
            "Should fail when copying non-existent file"
        );

        // Test copying with empty sources
        let result = copy_command(&setup.ship_context, &[], "should_fail.txt", &opts).await;
        assert!(result.is_err(), "Should fail with empty sources");

        // Test copying without host prefix (no direction can be determined)
        let host_file = setup.create_host_text_file("test.txt", "test").await?;
        let result = copy_command(
            &setup.ship_context,
            &[host_file.to_string_lossy().to_string()],
            "test.txt",
            &opts,
        )
        .await;
        assert!(
            result.is_err(),
            "Should fail without host:// prefix on source or dest"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_single_file_to_new_path() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create host file
        let host_file = setup
            .create_host_text_file("original.txt", "Original content")
            .await?;

        // Copy to new filename (not directory)
        setup
            .copy_in(
                vec![format!("host:///{}", host_file.display())],
                "renamed_file.txt",
            )
            .await?;

        // Verify the file exists with the new name
        assert!(
            setup.verify_file_exists("renamed_file.txt").await?,
            "File should exist with new name"
        );

        let content = setup.read_pond_file_content("renamed_file.txt").await?;
        assert_eq!(String::from_utf8(content)?, "Original content");

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_out_parquet_series_to_host() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create directories first
        let mut ship = steward::Ship::open_pond(&setup.pond_path).await?;
        ship.write_transaction(
            &steward::PondUserMetadata::new(vec!["test".to_string(), "setup".to_string()]),
            async |fs| {
                let root = fs.root().await?;
                _ = root.create_dir_path("/data").await?;
                _ = root.create_dir_path("/data/nested").await?;
                Ok(())
            },
        )
        .await?;

        // Create multiple parquet series files in the pond
        let series_paths = vec![
            "/data/sensor1.series",
            "/data/sensor2.series",
            "/data/nested/sensor3.series",
        ];

        for series_path in &series_paths {
            setup.create_parquet_series_in_pond(series_path).await?;
        }

        // Create temporary output directory
        let output_dir = tempfile::tempdir()?;
        let output_path = output_dir.path().to_string_lossy().to_string();

        // Copy out using pattern
        setup
            .copy_out_to(vec!["/data/**/*.series".to_string()], &output_path)
            .await?;

        // Verify all files were exported with correct directory structure
        for series_path in &series_paths {
            let relative_path = series_path.trim_start_matches('/');
            let exported_file = output_dir.path().join(relative_path);

            assert!(
                exported_file.exists(),
                "Exported file should exist: {:?}",
                exported_file
            );

            // Verify it's a valid parquet file by reading it
            let file = std::fs::File::open(&exported_file)?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

            let mut row_count = 0;
            for batch_result in reader {
                let batch = batch_result?;
                row_count += batch.num_rows();
            }

            assert!(row_count > 0, "Exported parquet file should contain data");
            log::debug!(
                "[OK] Verified exported file: {:?} ({} rows)",
                exported_file,
                row_count
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_out_single_series_to_host() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create a single parquet series file
        setup
            .create_parquet_series_in_pond("/test_sensor.series")
            .await?;

        // Create temporary output directory
        let output_dir = tempfile::tempdir()?;
        let output_path = output_dir.path().to_string_lossy().to_string();

        // Copy out single file
        setup
            .copy_out_to(vec!["/test_sensor.series".to_string()], &output_path)
            .await?;

        // Verify file was exported
        let exported_file = output_dir.path().join("test_sensor.series");
        assert!(
            exported_file.exists(),
            "Exported file should exist: {:?}",
            exported_file
        );

        // Verify parquet content
        let file = std::fs::File::open(&exported_file)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

        let batches: Result<Vec<_>, _> = reader.collect();
        let batches = batches?;

        assert!(!batches.is_empty(), "Should have at least one batch");

        // Verify schema has expected columns
        let schema = batches[0].schema();
        assert!(
            schema.column_with_name("timestamp").is_some(),
            "Should have timestamp column"
        );
        assert!(
            schema.column_with_name("value").is_some(),
            "Should have value column"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_out_requires_host_prefix() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create a parquet series file
        setup.create_parquet_series_in_pond("/test.series").await?;

        // Try to copy without host:// prefix on either side - should fail
        let result = copy_command(
            &setup.ship_context,
            &["/test.series".to_string()],
            "/tmp/output",
            &CopyOptions::default(),
        )
        .await;
        assert!(
            result.is_err(),
            "Should fail when neither source nor dest has host:// prefix"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_roundtrip_out_then_in() -> Result<()> {
        // Setup: Create first pond with test files
        let setup1 = TestSetup::new().await?;

        // Create directories first
        let mut ship1 = steward::Ship::open_pond(&setup1.pond_path).await?;
        ship1
            .write_transaction(
                &steward::PondUserMetadata::new(vec!["test".to_string(), "setup".to_string()]),
                async |fs| {
                    let root = fs.root().await?;
                    _ = root.create_dir_path("/data").await?;
                    _ = root.create_dir_path("/data/subdir").await?;
                    Ok(())
                },
            )
            .await?;

        // Create test files in first pond
        let test_files = vec![
            "/data/file1.series",
            "/data/file2.series",
            "/data/subdir/file3.series",
        ];

        for file_path in &test_files {
            setup1.create_parquet_series_in_pond(file_path).await?;
        }

        // Step 1: Copy OUT to host filesystem
        let output_dir = tempfile::tempdir()?;
        let output_path = output_dir.path().to_string_lossy().to_string();

        setup1
            .copy_out_to(vec!["/data/**/*.series".to_string()], &output_path)
            .await?;

        // Verify files were exported
        let exported_files: Vec<_> = test_files
            .iter()
            .map(|p| {
                let relative = p.trim_start_matches('/');
                output_dir.path().join(relative)
            })
            .collect();

        for exported_file in &exported_files {
            assert!(
                exported_file.exists(),
                "Exported file should exist: {:?}",
                exported_file
            );
        }

        // Step 2: Create NEW pond and copy IN from host
        let setup2 = TestSetup::new().await?;

        // Copy the entire directory structure back into the new pond
        setup2
            .copy_in(vec![format!("host://{}/data", output_path)], "/imported")
            .await?;

        // Step 3: Verify all files exist in second pond with correct structure
        for original_path in &test_files {
            let relative = original_path.trim_start_matches("/data/");
            let imported_path = format!("/imported/{}", relative);

            assert!(
                setup2.verify_file_exists(&imported_path).await?,
                "Imported file should exist: {}",
                imported_path
            );

            // Read content from second pond
            let content = setup2.read_pond_file_content(&imported_path).await?;

            // Verify content is non-empty (contains parquet data)
            assert!(!content.is_empty(), "Imported file should not be empty");

            // Verify parquet magic bytes (PAR1)
            assert_eq!(&content[0..4], b"PAR1", "Should be valid parquet file");

            log::debug!(
                "[OK] Round-trip verified for: {} ({} bytes)",
                imported_path,
                content.len()
            );
        }

        log::info!(
            "[OK] Round-trip test complete: {} files copied OUT and IN successfully",
            test_files.len()
        );
        Ok(())
    }

    /// Test: pond copy -d /tmp host:///file.txt /
    /// Verifies that the -d flag (host_root) makes host paths relative to the root.
    /// With -d set to the host_files_dir, `host:///file.txt` resolves to
    /// `host_files_dir/file.txt`.
    #[tokio::test]
    async fn test_copy_with_directory_flag() -> Result<()> {
        // Create a temporary directory for the test pond
        let temp_dir = TempDir::new()?;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let pond_path = temp_dir.path().join(format!("test_pond_{}", timestamp));
        let host_files_dir = temp_dir.path().join("host_files");
        tokio::fs::create_dir_all(&host_files_dir).await?;

        // Create a file in the host_files_dir
        let content = "Hello from -d flag test!\n";
        let file_path = host_files_dir.join("file.txt");
        let mut f = File::create(&file_path).await?;
        f.write_all(content.as_bytes()).await?;
        f.shutdown().await?;

        // Create ShipContext WITH host_root set (simulates -d flag)
        let init_args = vec!["pond".to_string(), "init".to_string()];
        let ship_context = ShipContext::new(
            Some(&pond_path),
            Some(&host_files_dir), // -d host_files_dir
            Vec::new(),
            init_args.clone(),
        );

        // Initialize the pond
        init_command(&ship_context, "test-host").await?;

        // Copy using relative path: host:///file.txt resolves to host_files_dir/file.txt
        copy_command(
            &ship_context,
            &["host:///file.txt".to_string()],
            "/",
            &CopyOptions::default(),
        )
        .await?;

        // Verify the file exists in the pond
        let mut ship = steward::Ship::open_pond(&pond_path).await?;
        let tx = ship
            .begin_read(&steward::PondUserMetadata::new(vec!["verify".to_string()]))
            .await?;
        let fs = &*tx;
        let root = fs.root().await?;

        let mut pond_content = Vec::new();
        {
            let mut reader = root.async_reader_path("/file.txt").await?;
            _ = reader.read_to_end(&mut pond_content).await?;
        }
        _ = tx.commit().await?;

        assert_eq!(
            String::from_utf8(pond_content)?,
            content,
            "File content should match original"
        );

        Ok(())
    }
}
