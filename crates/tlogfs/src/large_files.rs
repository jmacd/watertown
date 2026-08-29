// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::error::TLogFSError;
use log::{debug, info};
use parquet::basic::Compression;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;
use tokio::io::AsyncWrite;

/// Threshold for storing files separately: 64 KiB (configurable)
pub const LARGE_FILE_THRESHOLD: usize = 64 * 1024;

/// Threshold for creating subdirectories: when more than this many files exist
pub const DIRECTORY_SPLIT_THRESHOLD: usize = 100;

/// Number of bits (4 hex digits) for directory prefix
pub const PREFIX_BITS: usize = 16;

/// Options for large file storage
#[derive(Clone, Debug)]
pub struct LargeFileOptions {
    /// Compression codec for parquet files
    pub compression: Compression,
    /// Maximum rows per row group (None = parquet default)
    /// Setting to 1 puts each chunk in its own row group for isolated corruption detection
    pub max_row_group_size: Option<usize>,
}

impl Default for LargeFileOptions {
    fn default() -> Self {
        Self {
            compression: Compression::ZSTD(parquet::basic::ZstdLevel::default()),
            max_row_group_size: None,
        }
    }
}

impl LargeFileOptions {
    /// Create options optimized for corruption testing:
    /// - No compression (so we can corrupt specific bytes)
    /// - 1 row per row group (so each chunk is isolated)
    #[cfg(test)]
    #[must_use]
    pub fn uncompressed() -> Self {
        Self {
            compression: Compression::UNCOMPRESSED,
            max_row_group_size: Some(1),
        }
    }
}

/// Get large file path with hierarchical directory structure (writes to new .parquet format)
/// Returns the path where the file should be stored, handling directory migration automatically
pub async fn large_file_path<P: AsRef<Path>>(
    pond_path: P,
    blake3: &str,
) -> std::io::Result<PathBuf> {
    let large_files_dir = pond_path.as_ref().to_path_buf().join("_large_files");

    // Check if we need hierarchical structure
    if should_use_hierarchical_structure(&large_files_dir).await? {
        // Ensure migration is complete
        migrate_to_hierarchical_structure(&large_files_dir).await?;

        // Use hierarchical path with .parquet extension
        let prefix = &blake3[0..4]; // First 4 hex digits (16 bits)
        Ok(large_files_dir
            .join(format!("blake3_{}={}", PREFIX_BITS, prefix))
            .join(format!("blake3={}.parquet", blake3)))
    } else {
        // Use flat structure with .parquet extension
        Ok(large_files_dir.join(format!("blake3={}.parquet", blake3)))
    }
}

/// Check if we should use hierarchical directory structure
async fn should_use_hierarchical_structure(large_files_dir: &PathBuf) -> std::io::Result<bool> {
    // If hierarchical directories already exist, use hierarchical structure
    if has_hierarchical_directories(large_files_dir).await? {
        return Ok(true);
    }

    // Count flat files to see if we need to migrate
    let flat_file_count = count_flat_files(large_files_dir).await?;
    Ok(flat_file_count > DIRECTORY_SPLIT_THRESHOLD)
}

/// Check if hierarchical directories already exist
async fn has_hierarchical_directories(large_files_dir: &PathBuf) -> std::io::Result<bool> {
    if !large_files_dir.exists() {
        return Ok(false);
    }

    let mut entries = tokio::fs::read_dir(large_files_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_dir() {
            let filename = entry.file_name();
            let name = filename.to_string_lossy();
            if name.starts_with(&format!("blake3_{}=", PREFIX_BITS)) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Count files in flat structure (blake3=* files directly in _large_files/)
async fn count_flat_files(large_files_dir: &PathBuf) -> std::io::Result<usize> {
    if !large_files_dir.exists() {
        return Ok(0);
    }

    let mut count = 0;
    let mut entries = tokio::fs::read_dir(large_files_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file() {
            let filename = entry.file_name();
            let name = filename.to_string_lossy();
            if name.starts_with("blake3=") {
                count += 1;
            }
        }
    }
    Ok(count)
}

/// Migrate flat structure to hierarchical structure (idempotent)
async fn migrate_to_hierarchical_structure(large_files_dir: &PathBuf) -> std::io::Result<()> {
    if !large_files_dir.exists() {
        return Ok(());
    }

    // Collect all flat files that need migration
    let mut flat_files = Vec::new();
    let mut entries = tokio::fs::read_dir(large_files_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file() {
            let filename = entry.file_name();
            let name = filename.to_string_lossy();
            if name.starts_with("blake3=") {
                // Extract BLAKE3 from filename: "blake3=<blake3>"
                if let Some(blake3) = name.strip_prefix("blake3=") {
                    flat_files.push((entry.path(), blake3.to_string()));
                }
            }
        }
    }

    // Migrate each file to hierarchical structure
    for (old_path, blake3) in flat_files {
        let prefix = &blake3[0..4]; // First 4 hex digits
        let subdir = large_files_dir.join(format!("blake3_{}={}", PREFIX_BITS, prefix));
        let new_path = subdir.join(format!("blake3={}", blake3));

        // Create subdirectory if it doesn't exist
        tokio::fs::create_dir_all(&subdir).await?;

        // Move file to new location (idempotent - only if source exists and target doesn't)
        if old_path.exists() && !new_path.exists() {
            tokio::fs::rename(&old_path, &new_path).await?;
        }
    }

    Ok(())
}

/// Find large file path (for reading) - parquet format with blake3 hash
pub async fn find_large_file_path<P: AsRef<Path>>(
    pond_path: P,
    blake3: &str,
) -> std::io::Result<Option<PathBuf>> {
    let large_files_dir = pond_path.as_ref().join("_large_files");

    let prefix = &blake3[0..4];

    // Try hierarchical structure
    let hierarchical_parquet = large_files_dir
        .join(format!("blake3_{}={}", PREFIX_BITS, prefix))
        .join(format!("blake3={}.parquet", blake3));
    if hierarchical_parquet.exists() {
        return Ok(Some(hierarchical_parquet));
    }

    // Try flat structure
    let flat_parquet = large_files_dir.join(format!("blake3={}.parquet", blake3));
    if flat_parquet.exists() {
        return Ok(Some(flat_parquet));
    }

    Ok(None)
}

/// Read the full content of an externalized large file by its BLAKE3 hash,
/// reconstructing it from the parquet-chunked storage under `_large_files/`.
///
/// Shared by [`crate::persistence::OpLogPersistence::read_large_file_bytes`]
/// (public read-back API) and write-time logical-leaf stamping (which must
/// read back externalized bytes for `Large` content refs to compute
/// `logical_leaf_hash`), so the lookup/read logic lives in exactly one place.
pub async fn read_external_bytes<P: AsRef<Path>>(
    pond_path: P,
    blake3: &str,
) -> Result<Vec<u8>, TLogFSError> {
    use tokio::io::AsyncReadExt;
    let path = find_large_file_path(pond_path.as_ref(), blake3)
        .await
        .map_err(|e| TLogFSError::ArrowMessage(format!("locate large file {blake3}: {e}")))?
        .ok_or_else(|| TLogFSError::LargeFileNotFound {
            blake3: blake3.to_string(),
            path: format!("_large_files/blake3={blake3}"),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "large file not found"),
        })?;
    let mut reader =
        ParquetFileReader::new(path.clone())
            .await
            .map_err(|e| TLogFSError::LargeFileNotFound {
                blake3: blake3.to_string(),
                path: path.display().to_string(),
                source: e,
            })?;
    let mut buf = Vec::new();
    let _ = reader
        .read_to_end(&mut buf)
        .await
        .map_err(|e| TLogFSError::ArrowMessage(format!("read large file {blake3}: {e}")))?;
    Ok(buf)
}

/// Check if content should be stored as large file
#[must_use]
pub fn should_store_as_large_file(content: &[u8]) -> bool {
    let content_len = content.len();
    let is_large = content_len >= LARGE_FILE_THRESHOLD;
    debug!(
        "should_store_as_large_file: content_len={content_len}, threshold={LARGE_FILE_THRESHOLD}, is_large={is_large}"
    );
    is_large
}

/// Result of hybrid writer finalization
#[derive(Clone)]
pub struct HybridWriterResult {
    pub content: Vec<u8>,
    pub blake3: String,
    pub size: usize,
    /// Bao-tree incremental hash state for FilePhysicalSeries
    /// This captures the full bao-tree state computed incrementally during write,
    /// enabling correct cumulative_size for large files where content is stored externally.
    pub bao_state: utilities::bao_outboard::IncrementalHashState,
}

/// Hybrid writer that implements AsyncWrite with incremental hashing and spillover
pub struct HybridWriter {
    /// Temporary file for streaming writes (created lazily)
    temp_file: Option<File>,
    /// Path to temporary file
    temp_path: Option<PathBuf>,
    /// Incremental BLAKE3 hasher
    hasher: blake3::Hasher,
    /// Incremental bao-tree hasher for FilePhysicalSeries cumulative checksums
    bao_hasher: utilities::bao_outboard::IncrementalHashState,
    /// Total bytes written
    total_written: usize,
    /// Target pond directory for final file
    pond_path: PathBuf,
    /// Future for creating temp file
    create_future: Option<Pin<Box<dyn Future<Output = std::io::Result<(File, PathBuf)>> + Send>>>,
    /// Options for large file storage
    options: LargeFileOptions,
}

impl HybridWriter {
    pub fn new<P: AsRef<Path>>(pond_path: P) -> Self {
        Self::with_options(pond_path, LargeFileOptions::default())
    }

    pub fn with_options<P: AsRef<Path>>(pond_path: P, options: LargeFileOptions) -> Self {
        Self {
            temp_file: None,
            temp_path: None,
            hasher: blake3::Hasher::new(),
            bao_hasher: utilities::bao_outboard::IncrementalHashState::new(),
            total_written: 0,
            pond_path: pond_path.as_ref().into(),
            create_future: None,
            options,
        }
    }

    /// Create a HybridWriter that resumes bao-tree computation from a previous SeriesOutboard
    ///
    /// This is used when appending to a FilePhysicalSeries. The bao_hasher is initialized
    /// from the previous frontier and verified pending bytes, so the resulting bao_state will
    /// have the correct cumulative hash for the entire series (prev content + new content).
    ///
    /// # Arguments
    /// * `pond_path` - Path to the pond directory
    /// * `prev` - The previous version's SeriesOutboard
    /// * `verified_pending` - The tail bytes from previous content that form the pending block.
    ///   Must be exactly `prev.cumulative_size % BLOCK_SIZE` bytes, read from previous versions.
    ///
    /// # Errors
    /// Returns an error if the previous state is invalid or pending bytes length is wrong.
    pub fn resume_from<P: AsRef<Path>>(
        pond_path: P,
        prev: &utilities::bao_outboard::SeriesOutboard,
        verified_pending: &[u8],
    ) -> std::io::Result<Self> {
        Self::resume_from_with_options(
            pond_path,
            prev,
            verified_pending,
            LargeFileOptions::default(),
        )
    }

    /// Create a HybridWriter that resumes from previous state with custom options
    pub fn resume_from_with_options<P: AsRef<Path>>(
        pond_path: P,
        prev: &utilities::bao_outboard::SeriesOutboard,
        verified_pending: &[u8],
        options: LargeFileOptions,
    ) -> std::io::Result<Self> {
        // Resume bao_hasher from previous frontier and verified pending bytes
        let bao_hasher = utilities::bao_outboard::IncrementalHashState::resume(
            &prev.incremental.frontier,
            prev.cumulative_size,
            verified_pending,
        )
        .map_err(|e| std::io::Error::other(format!("Failed to resume bao state: {}", e)))?;

        Ok(Self {
            temp_file: None,
            temp_path: None,
            hasher: blake3::Hasher::new(),
            bao_hasher,
            total_written: 0,
            pond_path: pond_path.as_ref().into(),
            create_future: None,
            options,
        })
    }

    pub fn total_written(&self) -> usize {
        self.total_written
    }

    /// Get the temp file path if it exists (for metadata extraction before finalize)
    pub fn temp_file_path(&self) -> Option<&PathBuf> {
        self.temp_path.as_ref()
    }

    /// Finalize the writer and return content strategy decision
    pub async fn finalize(self) -> std::io::Result<HybridWriterResult> {
        // Flush and sync temp file if it exists
        if let Some(mut temp_file) = self.temp_file {
            use tokio::io::AsyncWriteExt;
            temp_file.flush().await?;
            temp_file.sync_all().await?;
        }

        // Finalize hash computation
        let blake3 = self.hasher.finalize().to_hex().to_string();

        let total_written = self.total_written;
        debug!(
            "HybridWriter finalize: {total_written} bytes, threshold={LARGE_FILE_THRESHOLD}, blake3={blake3}"
        );

        let content = if self.total_written >= LARGE_FILE_THRESHOLD {
            debug!("Large file: converting to parquet format");

            // Large file: convert temp file to parquet format using ChunkedWriter
            let temp_path = self
                .temp_path
                .ok_or_else(|| std::io::Error::other("No temp file created for large file"))?;

            // Open temp file for reading
            let temp_file_reader = File::open(&temp_path).await?;

            // Create ChunkedWriter to process the file
            // Use pond_txn_id=0 and path=blake3 as identifiers (tlogfs doesn't track pond_txn_id at this level)
            let chunked_writer =
                utilities::chunked_files::ChunkedWriter::new(0, blake3.clone(), temp_file_reader);

            // Write to record batches
            let result = chunked_writer
                .write_to_batches()
                .await
                .map_err(|e| std::io::Error::other(format!("ChunkedWriter error: {}", e)))?;

            let large_files_dir = self.pond_path.join("_large_files");
            tokio::fs::create_dir_all(&large_files_dir).await?;

            let final_path = large_file_path(&self.pond_path, &blake3).await?;

            // Ensure the parent directory exists (hierarchical structure may
            // return a path like _large_files/blake3_16=XXXX/blake3=....parquet
            // where the subdirectory hasn't been created yet for new files).
            if let Some(parent) = final_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }

            // Write batches to parquet file
            let parquet_file = File::create(&final_path).await?;
            let schema = utilities::chunked_files::arrow_schema();

            // Write parquet file using ArrowWriter with configured options
            let mut props_builder = parquet::file::properties::WriterProperties::builder()
                .set_compression(self.options.compression);

            // Optionally set max_row_group_size (used in tests for per-chunk isolation)
            if let Some(max_size) = self.options.max_row_group_size {
                props_builder = props_builder.set_max_row_group_size(max_size);
            }

            let props = props_builder.build();

            let mut writer = parquet::arrow::AsyncArrowWriter::try_new(
                parquet_file,
                schema.clone(),
                Some(props),
            )
            .map_err(|e| {
                std::io::Error::other(format!("Failed to create parquet writer: {}", e))
            })?;

            for batch in result.batches {
                writer
                    .write(&batch)
                    .await
                    .map_err(|e| std::io::Error::other(format!("Failed to write batch: {}", e)))?;
            }

            let _metadata = writer.close().await.map_err(|e| {
                std::io::Error::other(format!("Failed to close parquet writer: {}", e))
            })?;

            // Sync the file after write
            let file = File::open(&final_path).await?;
            file.sync_all().await?;

            // Clean up temp file
            tokio::fs::remove_file(&temp_path).await?;

            info!("Successfully wrote large file parquet to {:?}", final_path);
            Vec::new() // Empty vec indicates external storage
        } else if self.temp_path.is_some() {
            debug!("Small file: reading temp file into memory");
            // Small file: read temp file into memory and delete it
            let temp_path = self.temp_path.expect("temp_path is_some");
            let content = tokio::fs::read(&temp_path).await?;
            tokio::fs::remove_file(&temp_path).await?;
            content
        } else {
            // No data written
            Vec::new()
        };

        Ok(HybridWriterResult {
            content,
            blake3,
            size: self.total_written,
            bao_state: self.bao_hasher,
        })
    }
}

impl AsyncWrite for HybridWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let this = &mut *self;

        // Lazy initialization: create temp file on first write
        if this.temp_file.is_none() && this.create_future.is_none() {
            let pond_path = this.pond_path.clone();
            this.create_future = Some(Box::pin(async move {
                let temp_dir = pond_path.join("_large_files");
                tokio::fs::create_dir_all(&temp_dir).await?;

                // Create unique temp file using process ID and timestamp
                let temp_path = temp_dir.join(format!(
                    "tmp_{}_{}",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("system time is after UNIX_EPOCH")
                        .as_nanos()
                ));
                let temp_file = File::create(&temp_path).await?;

                Ok((temp_file, temp_path))
            }));
        }

        // Poll the creation future if it exists
        if let Some(future) = this.create_future.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Ready(Ok((file, path))) => {
                    this.temp_file = Some(file);
                    this.temp_path = Some(path);
                    this.create_future = None;
                    debug!("Created streaming temp file: {:?}", this.temp_path);
                }
                Poll::Ready(Err(e)) => {
                    this.create_future = None;
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // Now write to the temp file
        if let Some(ref mut temp_file) = this.temp_file {
            // Write to file first
            let result = Pin::new(temp_file).poll_write(cx, buf);

            // Only update hashers and counter if write succeeded
            if let Poll::Ready(Ok(n)) = result {
                // Hash exactly what was written (might be less than buf.len())
                let written_bytes = &buf[..n];
                let _ = this.hasher.update(written_bytes);
                // Also feed into bao-tree hasher for FilePhysicalSeries cumulative checksums
                this.bao_hasher.ingest(written_bytes);
                this.total_written += n;
            }

            result
        } else {
            Poll::Ready(Err(std::io::Error::other("Writer in invalid state")))
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if let Some(ref mut temp_file) = self.temp_file {
            Pin::new(temp_file).poll_flush(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if let Some(ref mut temp_file) = self.temp_file {
            Pin::new(temp_file).poll_shutdown(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

/// Streaming reader for parquet-encoded large files
///
/// Reads chunks on-demand without loading entire file into memory.
/// Verification is optional and happens at 16KB block granularity using bao-tree.
pub struct ParquetFileReader {
    /// Path to the parquet file
    file_path: PathBuf,
    /// Total size of the reconstructed file
    total_size: u64,
    /// Current position in the logical file
    position: u64,
    /// Current chunk being read (chunk_id, chunk_data)
    current_chunk: Option<LoadedChunk>,
    /// Position within current chunk
    chunk_position: usize,
    /// Pending chunk load future (stored to allow proper polling)
    pending_load: Option<PendingChunkLoad>,
}

/// A loaded chunk with optional verification data
struct LoadedChunk {
    chunk_id: i64,
    data: Vec<u8>,
}

/// State for async chunk loading
struct PendingChunkLoad {
    chunk_id: i64,
    future: Pin<Box<dyn Future<Output = std::io::Result<LoadedChunk>> + Send>>,
}

impl ParquetFileReader {
    /// Create a new streaming reader for a parquet file
    pub async fn new(file_path: PathBuf) -> std::io::Result<Self> {
        use futures::StreamExt;

        // Open parquet file and read metadata to get total size
        let file = File::open(&file_path).await?;

        let builder = parquet::arrow::ParquetRecordBatchStreamBuilder::new(file)
            .await
            .map_err(|e| std::io::Error::other(format!("Failed to open parquet: {}", e)))?;

        let mut stream = builder
            .build()
            .map_err(|e| std::io::Error::other(format!("Failed to build parquet reader: {}", e)))?;

        let total_size = if let Some(first_batch) = stream.next().await {
            let batch = first_batch
                .map_err(|e| std::io::Error::other(format!("Failed to read first batch: {}", e)))?;

            if batch.num_rows() == 0 {
                return Err(std::io::Error::other("Empty parquet file"));
            }

            // Get total_size from column 8 (after pond_id insertion at position 3)
            let total_sizes = batch
                .column(8)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .ok_or_else(|| std::io::Error::other("Invalid total_size column type"))?;

            total_sizes.value(0) as u64
        } else {
            return Err(std::io::Error::other("Empty parquet file"));
        };

        Ok(Self {
            file_path,
            total_size,
            position: 0,
            current_chunk: None,
            chunk_position: 0,
            pending_load: None,
        })
    }

    /// Get total size of the file
    #[must_use]
    pub fn total_size(&self) -> u64 {
        self.total_size
    }
}

/// Load and verify a chunk from parquet file using SeriesOutboard
///
/// Verification parses the stored SeriesOutboard and verifies:
/// 1. Chunk size matches stored version_size
/// 2. For chunk 0: verifies content hashes to stored cumulative_blake3
/// 3. Stored outboard parses correctly
///
/// Full cumulative verification requires sequential reading with state tracking.
/// This function provides per-chunk validation suitable for random access.
async fn load_chunk_from_parquet(
    file_path: PathBuf,
    chunk_id: i64,
) -> std::io::Result<LoadedChunk> {
    use futures::StreamExt;
    use utilities::bao_outboard::SeriesOutboard;

    let file = File::open(&file_path).await?;
    let mut reader = parquet::arrow::ParquetRecordBatchStreamBuilder::new(file)
        .await
        .map_err(|e| std::io::Error::other(format!("Failed to open parquet: {}", e)))?
        .build()
        .map_err(|e| std::io::Error::other(format!("Failed to build parquet reader: {}", e)))?;

    while let Some(batch_result) = reader.next().await {
        let batch = batch_result
            .map_err(|e| std::io::Error::other(format!("Failed to read batch: {}", e)))?;

        let chunk_ids = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .ok_or_else(|| std::io::Error::other("Invalid chunk_id column type"))?;

        for row in 0..batch.num_rows() {
            if chunk_ids.value(row) == chunk_id {
                // Load chunk data, hash, and outboard for verification
                let chunk_hashes = batch
                    .column(5)
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .ok_or_else(|| std::io::Error::other("Invalid chunk_hash column type"))?;

                let chunk_outboards = batch
                    .column(6)
                    .as_any()
                    .downcast_ref::<arrow_array::BinaryArray>()
                    .ok_or_else(|| std::io::Error::other("Invalid chunk_outboard column type"))?;

                let chunk_datas = batch
                    .column(7)
                    .as_any()
                    .downcast_ref::<arrow_array::BinaryArray>()
                    .ok_or_else(|| std::io::Error::other("Invalid chunk_data column type"))?;

                let expected_hash_hex = chunk_hashes.value(row);
                let stored_outboard_bytes = chunk_outboards.value(row);
                let chunk_data = chunk_datas.value(row);

                // Parse the stored SeriesOutboard
                let stored_outboard =
                    SeriesOutboard::from_bytes(stored_outboard_bytes).map_err(|e| {
                        std::io::Error::other(format!(
                            "Chunk {} has invalid SeriesOutboard: {}",
                            chunk_id, e
                        ))
                    })?;

                // Verify chunk size matches stored version_size
                if chunk_data.len() as u64 != stored_outboard.version_size {
                    return Err(std::io::Error::other(format!(
                        "Chunk {} size mismatch: data has {} bytes, outboard says {}",
                        chunk_id,
                        chunk_data.len(),
                        stored_outboard.version_size
                    )));
                }

                // Verify stored hash matches outboard's cumulative_blake3
                let stored_hash = blake3::Hash::from_bytes(stored_outboard.cumulative_blake3);
                let stored_hash_hex = stored_hash.to_hex().to_string();
                if stored_hash_hex != expected_hash_hex {
                    return Err(std::io::Error::other(format!(
                        "Chunk {} hash mismatch: chunk_hash={}, outboard.cumulative_blake3={}",
                        chunk_id, expected_hash_hex, stored_hash_hex
                    )));
                }

                // For chunk 0, we can fully verify since it starts at offset 0
                // (or at the series starting offset, which we don't have here)
                if chunk_id == 0 {
                    let computed = SeriesOutboard::first_version(chunk_data);
                    let computed_hash = blake3::Hash::from_bytes(computed.cumulative_blake3);
                    if computed_hash != stored_hash {
                        return Err(std::io::Error::other(format!(
                            "Chunk 0 verification failed: expected {}, computed {}",
                            stored_hash_hex,
                            computed_hash.to_hex()
                        )));
                    }
                }
                // For chunk N > 0, full verification requires previous chunk's state
                // which we don't have in this random-access context.
                // The size and hash consistency checks above catch most corruption.

                debug!(
                    "Verified chunk {} ({} bytes, cumulative_hash={})",
                    chunk_id,
                    chunk_data.len(),
                    &stored_hash_hex[..16.min(stored_hash_hex.len())]
                );

                return Ok(LoadedChunk {
                    chunk_id,
                    data: chunk_data.to_vec(),
                });
            }
        }
    }

    Err(std::io::Error::other(format!(
        "Chunk {} not found in parquet file",
        chunk_id
    )))
}

impl tokio::io::AsyncRead for ParquetFileReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = &mut *self;

        // Check if we're at EOF
        if this.position >= this.total_size {
            return Poll::Ready(Ok(()));
        }

        // Calculate which chunk we need
        let chunk_size = utilities::chunked_files::CHUNK_SIZE_DEFAULT;
        let chunk_id = (this.position / chunk_size as u64) as i64;

        // Check if we have a pending load for this chunk
        if let Some(ref mut pending) = this.pending_load {
            if pending.chunk_id == chunk_id {
                // Poll the existing future
                match pending.future.as_mut().poll(cx) {
                    Poll::Ready(Ok(chunk)) => {
                        this.current_chunk = Some(chunk);
                        this.chunk_position = (this.position % chunk_size as u64) as usize;
                        this.pending_load = None;
                    }
                    Poll::Ready(Err(e)) => {
                        this.pending_load = None;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            } else {
                // Different chunk requested, cancel old load
                this.pending_load = None;
            }
        }

        // Check if we need to load a different chunk
        let need_new_chunk = match &this.current_chunk {
            None => true,
            Some(chunk) => chunk.chunk_id != chunk_id,
        };

        if need_new_chunk && this.pending_load.is_none() {
            // Start loading the chunk
            let file_path = this.file_path.clone();
            let future = Box::pin(load_chunk_from_parquet(file_path, chunk_id));
            this.pending_load = Some(PendingChunkLoad { chunk_id, future });

            // Poll it immediately
            if let Some(ref mut pending) = this.pending_load {
                match pending.future.as_mut().poll(cx) {
                    Poll::Ready(Ok(chunk)) => {
                        this.current_chunk = Some(chunk);
                        this.chunk_position = (this.position % chunk_size as u64) as usize;
                        this.pending_load = None;
                    }
                    Poll::Ready(Err(e)) => {
                        this.pending_load = None;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
        }

        // Read from current chunk
        if let Some(chunk) = &this.current_chunk {
            let available = chunk.data.len() - this.chunk_position;
            let to_read = std::cmp::min(available, buf.remaining());
            let to_read = std::cmp::min(to_read, (this.total_size - this.position) as usize);

            if to_read > 0 {
                buf.put_slice(&chunk.data[this.chunk_position..this.chunk_position + to_read]);
                this.chunk_position += to_read;
                this.position += to_read as u64;
            }
        }

        Poll::Ready(Ok(()))
    }
}

impl tokio::io::AsyncSeek for ParquetFileReader {
    fn start_seek(mut self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        let new_pos = match position {
            std::io::SeekFrom::Start(pos) => pos as i64,
            std::io::SeekFrom::End(offset) => self.total_size as i64 + offset,
            std::io::SeekFrom::Current(offset) => self.position as i64 + offset,
        };

        if new_pos < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid seek to negative position",
            ));
        }

        let new_pos = new_pos as u64;
        if new_pos > self.total_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Seek beyond end of file",
            ));
        }

        self.position = new_pos;

        // Invalidate current chunk if we seeked to a different chunk
        let chunk_size = utilities::chunked_files::CHUNK_SIZE_DEFAULT;
        let new_chunk_id = (new_pos / chunk_size as u64) as i64;

        if let Some(chunk) = &self.current_chunk {
            if chunk.chunk_id != new_chunk_id {
                self.current_chunk = None;
                self.pending_load = None; // Cancel any pending load
            } else {
                // Same chunk, just update position within chunk
                self.chunk_position = (new_pos % chunk_size as u64) as usize;
            }
        }

        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}

/// Outcome of a [`sweep_unreferenced`] pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SweepStats {
    /// Blobs deleted because no row referenced their hash.
    pub removed: usize,
    /// Bytes reclaimed from the deleted blobs.
    pub bytes_freed: u64,
}

/// Delete every `_large_files` blob whose blake3 is absent from `referenced`.
///
/// Blobs are content-addressed, so one file can back many rows across many
/// nodes and ponds; reclamation must therefore be a mark-sweep over the whole
/// table's referenced hashes, never a per-row delete.
///
/// # Safety
///
/// The caller **must** hold the pond write lock. An uncommitted transaction can
/// have already written a blob that no committed row references yet, and this
/// sweep cannot distinguish that from garbage.
///
/// Only files named `blake3=<hash>.parquet` are considered, so in-flight temp
/// files are never touched.
///
/// # Errors
///
/// Returns an error if `_large_files` cannot be traversed or a blob cannot be
/// removed.
pub async fn sweep_unreferenced<P: AsRef<Path>>(
    pond_path: P,
    referenced: &std::collections::HashSet<String>,
) -> std::io::Result<SweepStats> {
    let root = pond_path.as_ref().join("_large_files");
    if !root.exists() {
        return Ok(SweepStats::default());
    }

    // `_large_files` is either flat or one level of `blake3_16=` shards.
    let mut dirs = vec![root];
    let mut stats = SweepStats::default();
    while let Some(dir) = dirs.pop() {
        let mut entries = tokio::fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            if file_type.is_dir() {
                dirs.push(entry.path());
                continue;
            }
            let name = entry.file_name();
            let Some(hash) = name
                .to_str()
                .and_then(|n| n.strip_prefix("blake3="))
                .and_then(|n| n.strip_suffix(".parquet"))
            else {
                continue;
            };
            if referenced.contains(hash) {
                continue;
            }
            let size = entry.metadata().await.map(|m| m.len()).unwrap_or(0);
            tokio::fs::remove_file(entry.path()).await?;
            stats.removed += 1;
            stats.bytes_freed += size;
            debug!("swept unreferenced large-file blob blake3={hash} ({size} bytes)");
        }
    }
    Ok(stats)
}
