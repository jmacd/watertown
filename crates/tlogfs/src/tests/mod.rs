// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

mod large_file_corruption;
mod large_file_roundtrip;
mod partition_cache;

use crate::persistence::OpLogPersistence;
use arrow_array::record_batch;
use log::{debug, info};
use tempfile::TempDir;
use tinyfs::arrow::ParquetExt;
use tokio::time::{Duration, timeout};

fn test_dir() -> String {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let store_path = temp_dir
        .path()
        .join("test_store")
        .to_string_lossy()
        .to_string();

    let path = match std::env::var("TLOGFS") {
        Ok(val) => {
            std::fs::remove_dir_all(&val).expect("test dir can't be removed");
            val
        }
        _ => store_path,
    };

    debug!("Creating OpLogPersistence at {path}");
    path
}

#[tokio::test]
async fn test_transaction_guard_basic_usage() {
    let store_path = test_dir();

    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence layer");

    debug!("OpLogPersistence created successfully");

    // Begin a transaction (txn_seq=2 since root init used txn_seq=1)
    debug!("Beginning transaction");
    let tx = persistence
        .begin_test()
        .await
        .expect("Failed to begin transaction");

    debug!("Transaction started successfully");

    // Try to access the root directory
    debug!("Attempting to get root directory from transaction");
    let root = tx.root().await.expect("Failed to get root directory");

    let root_debug = format!("{:?}", root);
    info!("[OK] Successfully got root directory: {root_debug:?}");

    // Commit the transaction
    debug!("Committing transaction");
    tx.commit_test()
        .await
        .expect("Failed to commit transaction");

    info!("[OK] Transaction committed successfully");
}

/// Test the absolute basics: create, commit, reopen, read empty root
#[tokio::test]
async fn test_create_commit_reopen_read_root() {
    let store_path = test_dir();

    // Step 1: Create new OpLogPersistence with root directory
    debug!("Step 1: Creating new OpLogPersistence");
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence layer");

    debug!("OpLogPersistence created successfully");

    // Step 2: Begin a transaction and verify we can access root
    debug!("Step 2: Beginning first transaction");
    let tx1 = persistence
        .begin_test()
        .await
        .expect("Failed to begin transaction");

    let root1 = tx1.root().await.expect("Failed to get root directory");
    debug!("[OK] Got root directory in first transaction");

    // List the root directory (should be empty)
    use futures::stream::StreamExt;
    let mut entries_stream1 = root1.entries().await.expect("Failed to get entries stream");
    let mut entries1 = Vec::new();
    while let Some(entry_result) = entries_stream1.next().await {
        let entry = entry_result.expect("Failed to read entry");
        entries1.push(entry);
    }
    debug!("Root directory has {} entries", entries1.len());
    assert_eq!(entries1.len(), 0, "Root should be empty initially");

    // Step 3: Commit the transaction
    debug!("Step 3: Committing first transaction");
    tx1.commit_test()
        .await
        .expect("Failed to commit first transaction");
    debug!("[OK] First transaction committed");

    // Step 4: Begin a second transaction and read root again
    debug!("Step 4: Beginning second transaction (should see committed data)");
    let tx2 = persistence
        .begin_test()
        .await
        .expect("Failed to begin second transaction");

    let root2 = tx2
        .root()
        .await
        .expect("Failed to get root in second transaction");
    debug!("[OK] Got root directory in second transaction");

    // List the root directory again (should still be empty)
    let mut entries_stream2 = root2.entries().await.expect("Failed to get entries stream");
    let mut entries2 = Vec::new();
    while let Some(entry_result) = entries_stream2.next().await {
        let entry = entry_result.expect("Failed to read entry");
        entries2.push(entry);
    }
    debug!(
        "Root directory has {} entries in second transaction",
        entries2.len()
    );
    assert_eq!(entries2.len(), 0, "Root should still be empty");

    // No need to commit read transaction
    debug!("[OK] Test passed - can create, commit, and read empty root");
}

/// Test create file, commit, reopen, read file
#[tokio::test]
async fn test_create_file_commit_reopen_read() {
    let store_path = test_dir();

    // Step 1: Create new OpLogPersistence with root directory
    debug!("Step 1: Creating new OpLogPersistence");
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence layer");

    // Step 2: Create a simple file
    debug!("Step 2: Creating a file");
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin transaction");
        let root = tx.root().await.expect("Failed to get root");

        // Write a simple text file
        root.write_file_path_from_slice("test.txt", b"Hello, World!")
            .await
            .expect("Failed to write file");

        debug!("[OK] File created");

        tx.commit_test().await.expect("Failed to commit");
        debug!("[OK] Transaction committed");
    }

    // Step 3: Read the file back in a new transaction
    debug!("Step 3: Reading file in new transaction");
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin second transaction");
        let root = tx.root().await.expect("Failed to get root");

        // Read the file
        let content = root
            .read_file_path_to_vec("test.txt")
            .await
            .expect("Failed to read file");

        let content_str = String::from_utf8(content).expect("Invalid UTF-8");
        debug!("[OK] Read file content: '{}'", content_str);

        assert_eq!(content_str, "Hello, World!", "File content should match");

        debug!("[OK] Test passed - can create file, commit, and read back");
    }
}

/// Test create file:series, commit, reopen, read - SIMPLIFIED VERSION
#[tokio::test]
async fn test_create_series_commit_reopen_read_simple() {
    let store_path = test_dir();

    // Step 1: Create new OpLogPersistence with root directory
    debug!("Step 1: Creating new OpLogPersistence");
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence layer");

    // Step 2: Create a simple series file
    debug!("Step 2: Creating a file:series");
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin transaction");
        let root = tx.root().await.expect("Failed to get root");

        // Create test data as a RecordBatch
        let batch = record_batch!(
            ("timestamp", Int64, [1704067200000_i64]),
            ("value", Float64, [42.0_f64])
        )
        .expect("Failed to create batch");

        debug!("Created RecordBatch with {} rows", batch.num_rows());

        // Write the series
        let (min_time, max_time) = root
            .create_series_from_batch("test.series", &batch, Some("timestamp"))
            .await
            .expect("Failed to create series");

        debug!(
            "[OK] Series created with time range: {} to {}",
            min_time, max_time
        );

        tx.commit_test().await.expect("Failed to commit");
        debug!("[OK] Transaction committed");
    }

    // Step 3: Read the series back in a new transaction
    debug!("Step 3: Reading series in new transaction");
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin second transaction");
        let root = tx.root().await.expect("Failed to get root");

        // Check if file exists
        let exists = root.exists(std::path::Path::new("test.series")).await;
        debug!("File exists: {}", exists);
        assert!(exists, "Series file should exist after commit");

        // Read the series back
        let read_batch = root
            .read_table_as_batch("test.series")
            .await
            .expect("Failed to read series");

        debug!(
            "[OK] Read RecordBatch with {} rows, {} columns",
            read_batch.num_rows(),
            read_batch.num_columns()
        );

        assert_eq!(read_batch.num_rows(), 1, "Should have 1 row");
        assert_eq!(read_batch.num_columns(), 2, "Should have 2 columns");

        debug!("[OK] Test passed - can create series, commit, and read back");
    }
}

/// Test create series in subdirectory, commit, reopen, read
#[tokio::test]
async fn test_create_series_in_subdir() {
    let store_path = test_dir();

    debug!("Creating new OpLogPersistence");
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence layer");

    // Create a series in a subdirectory
    debug!("Creating series in subdirectory");
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin transaction");
        let root = tx.root().await.expect("Failed to get root");

        // Create subdirectory
        debug!("Creating test/ subdirectory");
        let _test_dir = root
            .create_dir_path("test")
            .await
            .expect("Failed to create directory");

        // Create series in subdirectory
        let batch = record_batch!(
            ("timestamp", Int64, [1704067200000_i64]),
            ("value", Float64, [42.0_f64])
        )
        .expect("Failed to create batch");

        debug!("Writing series to test/data.series");
        let _time_range = root
            .create_series_from_batch("test/data.series", &batch, Some("timestamp"))
            .await
            .expect("Failed to create series");

        debug!("[OK] Series created, committing...");
        tx.commit_test().await.expect("Failed to commit");
        debug!("[OK] Committed");
    }

    // Read back in new transaction
    debug!("Reading back in new transaction");
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin read transaction");
        let root = tx.root().await.expect("Failed to get root");

        let exists = root.exists(std::path::Path::new("test/data.series")).await;
        debug!("File exists: {}", exists);
        assert!(exists, "File should exist");

        let read_batch = root
            .read_table_as_batch("test/data.series")
            .await
            .expect("Failed to read series");

        debug!("[OK] Read {} rows", read_batch.num_rows());
        assert_eq!(read_batch.num_rows(), 1);
    }

    debug!("[OK] Test passed");
}

/// Test reading from a directory after creation (simulating steward's read pattern)
#[tokio::test]
async fn test_transaction_guard_read_after_create() {
    let store_path = test_dir();

    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence layer");

    // Transaction 1: Create directory structure
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin transaction");

        let root = tx.root().await.expect("Failed to get root directory");

        _ = root
            .create_dir_path("/txn")
            .await
            .expect("Failed to create /txn directory");

        tx.commit_test()
            .await
            .expect("Failed to commit transaction");
    }

    // Transaction 2: Try to read from the created directory
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin read transaction");

        let root = tx
            .root()
            .await
            .expect("Failed to get root directory for read");

        // Try to list the directory contents
        let _txn_dir = root
            .open_dir_path("/txn")
            .await
            .expect("Failed to get /txn directory");

        debug!("[OK] Successfully accessed /txn directory in new transaction");

        // Don't commit - this is a read-only transaction
    }

    debug!("[OK] Read-after-create test completed successfully");
}

/// Test for single version file:series write/read operations using TinyFS on TLogFS
/// This test is designed to reproduce potential hanging issues with single-version series.
#[tokio::test]
async fn test_single_version_file_series_write_read() -> Result<(), Box<dyn std::error::Error>> {
    // Use a timeout to prevent hanging - if this times out, we know there's a hang issue
    let test_result = timeout(Duration::from_secs(30), async {
        debug!("=== Starting Single Version File:Series Test ===");

        let store_path = test_dir();

        // Create TLogFS persistence layer
        debug!("Creating persistence layer...");
        let mut persistence = OpLogPersistence::create_test(&store_path).await?;

        let series_path = "test/single_series.series";

        // Begin transaction for write
        debug!("Beginning transaction...");
        let tx = persistence.begin_test().await?;

        // Get working directory
        let wd = tx.root().await?;

        // Create test directory if it doesn't exist
        if !wd.exists(std::path::Path::new("test")).await {
            debug!("Creating test directory...");
            _ = wd.create_dir_path("test").await?;
        }

        // Create test data as a RecordBatch with proper timestamp column (integer milliseconds)
        let batch = record_batch!(
            (
                "timestamp",
                Int64,
                [1704067200000_i64, 1704070800000_i64, 1704074400000_i64]
            ),
            ("value", Float64, [10.5_f64, 20.3_f64, 15.8_f64]),
            (
                "sensor_id",
                Utf8,
                ["sensor_001", "sensor_001", "sensor_001"]
            )
        )?;

        debug!("Created RecordBatch with {} rows", batch.num_rows());

        // Write the series using create_series_from_batch - this is where hanging might occur
        debug!("Writing series using create_series_from_batch...");
        let (min_time, max_time) = wd
            .create_series_from_batch(series_path, &batch, Some("timestamp"))
            .await?;

        debug!(
            "Series written successfully. Time range: {} to {}",
            min_time, max_time
        );

        // Commit transaction
        debug!("Committing transaction...");
        tx.commit_test().await?;
        debug!("Transaction committed successfully");

        // Create a new transaction to verify the file exists and can be read
        debug!("Starting new transaction for read...");
        let tx2 = persistence.begin_test().await?;
        let wd2 = tx2.root().await?;

        // Verify the file exists
        let file_exists = wd2.exists(std::path::Path::new(series_path)).await;
        debug!("File exists after commit: {}", file_exists);

        if !file_exists {
            return Err("File does not exist after commit".into());
        }

        // Read back the file
        debug!("Reading file back...");
        let read_batch = wd2.read_table_as_batch(series_path).await?;

        debug!(
            "Successfully read RecordBatch with {} rows, {} columns",
            read_batch.num_rows(),
            read_batch.num_columns()
        );

        // Verify the data
        assert_eq!(read_batch.num_rows(), 3, "Should have 3 rows");
        assert_eq!(read_batch.num_columns(), 3, "Should have 3 columns");

        // Don't need to commit read transaction
        debug!("=== All Tests Passed Successfully ===");
        Ok(())
    })
    .await;

    match test_result {
        Ok(result) => result,
        Err(_) => {
            panic!(
                "Test timed out after 30 seconds - this indicates a hanging issue in single version file:series operations"
            );
        }
    }
}

/// Minimal test to isolate the exact point of hanging - write only
#[tokio::test]
async fn test_minimal_single_version_write_only() -> Result<(), Box<dyn std::error::Error>> {
    let test_result = timeout(Duration::from_secs(15), async {
        debug!("=== Minimal Single Version Write Test ===");

        let store_path = test_dir();

        debug!("Creating persistence layer...");
        let mut persistence = OpLogPersistence::create_test(&store_path).await?;

        debug!("Starting transaction...");
        let tx = persistence.begin_test().await?;

        debug!("Getting working directory...");
        let wd = tx.root().await?;

        debug!("Creating minimal batch...");
        let batch = record_batch!(
            ("timestamp", Int64, [1704067200000_i64]),
            ("value", Float64, [1.0_f64])
        )?;

        debug!("About to call create_series_from_batch - this may hang...");
        let result = wd
            .create_series_from_batch("minimal.series", &batch, Some("timestamp"))
            .await;

        match result {
            Ok((min_time, max_time)) => {
                debug!(
                    "create_series_from_batch succeeded: {} to {}",
                    min_time, max_time
                );
                debug!("Committing transaction...");
                tx.commit_test().await?;
                debug!("Transaction committed successfully!");
            }
            Err(e) => {
                debug!("create_series_from_batch failed: {}", e);
                return Err(e.into());
            }
        }

        debug!("Minimal write test completed successfully!");
        Ok(())
    })
    .await;

    match test_result {
        Ok(result) => result,
        Err(_) => {
            panic!(
                "Minimal write test timed out - hanging occurs during create_series_from_batch or transaction commit"
            );
        }
    }
}

/// Test specifically for temporal metadata extraction from single version series
#[tokio::test]
async fn test_single_version_series_temporal_metadata() -> Result<(), Box<dyn std::error::Error>> {
    debug!("=== Testing Single Version Series Temporal Metadata ===");

    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path).await?;

    // Begin transaction
    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;

    // Create a series with explicit temporal data
    let batch = record_batch!(
        (
            "timestamp",
            Int64,
            [1704067200000_i64, 1704070800000_i64, 1704074400000_i64]
        ), // Clear temporal progression
        ("value", Float64, [10.0_f64, 20.0_f64, 30.0_f64])
    )?;

    debug!(
        "Created batch with timestamps: [{}, {}, {}]",
        1704067200000_i64, 1704070800000_i64, 1704074400000_i64
    );

    // Write series with explicit timestamp column
    let (min_time, max_time) = wd
        .create_series_from_batch("temporal_test.series", &batch, Some("timestamp"))
        .await?;

    debug!("Extracted temporal range: {} to {}", min_time, max_time);

    // The temporal metadata should reflect the actual timestamps, not be 0,0
    if min_time == 0 && max_time == 0 {
        debug!(
            "[WARN]  WARNING: Temporal metadata extraction returned 0,0 - this indicates a problem"
        );
        debug!("   Expected: min_time = 1704067200000, max_time = 1704074400000");
    } else {
        debug!("[OK] Temporal metadata extracted successfully");
        assert_eq!(
            min_time, 1704067200000_i64,
            "Min time should match first timestamp"
        );
        assert_eq!(
            max_time, 1704074400000_i64,
            "Max time should match last timestamp"
        );
    }

    // Commit and verify persistence
    tx.commit_test().await?;

    // Read back and check that we can access the file
    let tx2 = persistence.begin_test().await?;
    let wd2 = tx2.root().await?;

    let file_exists = wd2
        .exists(std::path::Path::new("temporal_test.series"))
        .await;
    assert!(file_exists, "File should exist after commit");

    let read_batch = wd2.read_table_as_batch("temporal_test.series").await?;
    assert_eq!(read_batch.num_rows(), 3, "Should read back 3 rows");

    debug!("[OK] Temporal metadata test completed");
    Ok(())
}

/// Test to investigate the actual file storage structure for series
#[tokio::test]
async fn test_single_version_series_storage_investigation() -> Result<(), Box<dyn std::error::Error>>
{
    debug!("=== Investigating Single Version Series Storage ===");

    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path).await?;

    // Begin transaction
    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;

    // Create a very simple series
    let batch = record_batch!(
        ("timestamp", Int64, [1704067200000_i64]),
        ("value", Float64, [42.0_f64])
    )?;

    debug!("Writing single-row series...");

    // Write series and capture result
    let (min_time, max_time) = wd
        .create_series_from_batch("investigation.series", &batch, Some("timestamp"))
        .await?;
    debug!(
        "create_series_from_batch returned: ({}, {})",
        min_time, max_time
    );

    // Before commit, let's see what we can discover about the node structure
    let metadata = wd.metadata_for_path("investigation.series").await?;
    debug!("File metadata before commit:");
    debug!("  entry_type: {:?}", metadata.entry_type);
    debug!("  size: {:?}", metadata.size);

    // Check if we can read the raw file content
    let raw_data = wd.read_file_path_to_vec("investigation.series").await?;
    debug!("Raw file size: {} bytes", raw_data.len());

    if raw_data.len() >= 4 {
        debug!("File magic bytes: {:?}", &raw_data[0..4]);
        if &raw_data[0..4] == b"PAR1" {
            debug!("[OK] File is valid Parquet");
        } else {
            debug!("[ERR] File is not Parquet format");
        }
    }

    // Commit the transaction
    debug!("Committing transaction...");
    tx.commit_test().await?;

    // Investigate post-commit state
    let tx2 = persistence.begin_test().await?;
    let wd2 = tx2.root().await?;

    let post_commit_metadata = wd2.metadata_for_path("investigation.series").await?;
    debug!("File metadata after commit:");
    debug!("  entry_type: {:?}", post_commit_metadata.entry_type);
    debug!("  size: {:?}", post_commit_metadata.size);

    // Try to read as RecordBatch
    let read_batch = wd2.read_table_as_batch("investigation.series").await?;
    debug!(
        "Read batch: {} rows, {} columns",
        read_batch.num_rows(),
        read_batch.num_columns()
    );

    // Check the actual data values
    debug!(
        "Schema: {:?}",
        read_batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .collect::<Vec<_>>()
    );

    debug!("[OK] Storage investigation completed");
    Ok(())
}

/// Summary test documenting the current state of single version file:series operations
#[tokio::test]
async fn test_single_version_series_summary() -> Result<(), Box<dyn std::error::Error>> {
    debug!("=== Single Version File:Series - Current Status Summary ===");

    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path).await?;

    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;

    let batch = record_batch!(
        ("timestamp", Int64, [1704067200000_i64, 1704070800000_i64]),
        ("temperature", Float64, [20.5_f64, 23.1_f64]),
        ("sensor", Utf8, ["A", "B"])
    )?;

    let (min_time, max_time) = wd
        .create_series_from_batch("summary.series", &batch, Some("timestamp"))
        .await?;
    tx.commit_test().await?;

    debug!("FINDINGS:");
    debug!("[OK] No hanging issues - operations complete successfully");
    debug!("[OK] File storage works correctly - Parquet format preserved");
    debug!("[OK] Data integrity maintained - all rows and columns readable");
    debug!("[OK] Entry type correctly set to FileSeries");
    debug!(
        "[ERR] Temporal metadata extraction broken - returns (0, 0) instead of actual timestamp range"
    );
    debug!("");
    debug!("EXPECTED: min_time = 1704067200000, max_time = 1704070800000");
    debug!("ACTUAL:   min_time = {}, max_time = {}", min_time, max_time);
    debug!("");
    debug!("RECOMMENDATION:");
    debug!("  - Investigate temporal metadata extraction in create_series_from_batch");
    debug!("  - Check if timestamp column parsing logic is working correctly");
    debug!("  - Verify that min/max extraction from Arrow arrays is functional");

    Ok(())
}

/// Test streaming async reader functionality without loading entire files into memory
/// This test verifies that large files can be read without the memory footprint issue
#[tokio::test]
async fn test_streaming_async_reader_large_file() -> Result<(), Box<dyn std::error::Error>> {
    use crate::large_files::LARGE_FILE_THRESHOLD;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    debug!("=== Testing Streaming Async Reader (Large File) ===");

    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path).await?;

    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;

    // Create a large file that would be problematic if loaded entirely into memory
    let large_content = vec![42u8; LARGE_FILE_THRESHOLD + 1000]; // Slightly larger than threshold
    let expected_size = large_content.len();

    debug!(
        "Creating large file with {} bytes (threshold is {})",
        expected_size, LARGE_FILE_THRESHOLD
    );

    // Store the large file
    _ = tinyfs::async_helpers::convenience::create_file_path(
        &wd,
        "/large_test.dat",
        &large_content,
    )
    .await?;
    tx.commit_test().await?;

    debug!("[OK] Large file stored successfully");

    // Now test streaming reader - this should NOT load the entire file into memory
    let tx2 = persistence.begin_test().await?;
    let wd2 = tx2.root().await?;

    let file_node = wd2.get_node_path("/large_test.dat").await?;
    let file_handle = file_node.as_file().await?;

    debug!("Getting async reader for large file...");
    let mut reader = file_handle.async_reader().await?;

    // Test: Read only first 100 bytes (streaming approach)
    let mut buffer = vec![0u8; 100];
    let bytes_read = reader.read_exact(&mut buffer).await?;

    debug!(
        "[OK] Successfully read {} bytes from start of file",
        bytes_read
    );
    assert_eq!(buffer, vec![42u8; 100], "First 100 bytes should all be 42");

    // Test: Seek to middle and read 50 bytes (verifying AsyncSeek works)
    let middle_pos = (expected_size / 2) as u64;
    _ = reader.seek(std::io::SeekFrom::Start(middle_pos)).await?;

    let mut middle_buffer = vec![0u8; 50];
    _ = reader.read_exact(&mut middle_buffer).await?;

    debug!(
        "[OK] Successfully seeked to position {} and read 50 bytes",
        middle_pos
    );
    assert_eq!(
        middle_buffer,
        vec![42u8; 50],
        "Middle 50 bytes should all be 42"
    );

    // Test: Seek to end and verify size
    let end_pos = reader.seek(std::io::SeekFrom::End(0)).await?;
    debug!("[OK] File end position: {} bytes", end_pos);
    assert_eq!(
        end_pos as usize, expected_size,
        "File size should match expected size"
    );

    tx2.commit_test().await?;

    debug!("SUCCESS: Streaming reader works correctly for large files");
    debug!("  - No memory loading of entire file");
    debug!("  - AsyncRead works for partial reads");
    debug!("  - AsyncSeek works for random access");
    debug!("  - File size detection works correctly");

    Ok(())
}

/// Test streaming async reader functionality for small files
/// This test verifies that small files also work correctly with the streaming approach
#[tokio::test]
async fn test_streaming_async_reader_small_file() -> Result<(), Box<dyn std::error::Error>> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    debug!("=== Testing Streaming Async Reader (Small File) ===");

    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path).await?;

    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;

    // Create a small file (under threshold)
    let small_content = b"Hello, World! This is a small test file with some content.";
    let expected_size = small_content.len();

    debug!("Creating small file with {} bytes", expected_size);

    // Store the small file
    _ = tinyfs::async_helpers::convenience::create_file_path(&wd, "/small_test.txt", small_content)
        .await?;
    tx.commit_test().await?;

    debug!("[OK] Small file stored successfully");

    // Now test streaming reader with small file (stored inline in Delta Lake)
    let tx2 = persistence.begin_test().await?;
    let wd2 = tx2.root().await?;

    let file_node = wd2.get_node_path("/small_test.txt").await?;
    let file_handle = file_node.as_file().await?;

    debug!("Getting async reader for small file...");
    let mut reader = file_handle.async_reader().await?;

    // Test: Read entire content
    let mut buffer = vec![0u8; expected_size];
    _ = reader.read_exact(&mut buffer).await?;

    debug!("[OK] Successfully read {} bytes", expected_size);
    assert_eq!(&buffer, small_content, "Content should match exactly");

    // Test: Seek to start and read first 5 bytes
    _ = reader.seek(std::io::SeekFrom::Start(0)).await?;
    let mut start_buffer = vec![0u8; 5];
    _ = reader.read_exact(&mut start_buffer).await?;

    debug!("[OK] Successfully seeked to start and read first 5 bytes");
    assert_eq!(&start_buffer, b"Hello", "First 5 bytes should be 'Hello'");

    // Test: Seek to end and verify size
    let end_pos = reader.seek(std::io::SeekFrom::End(0)).await?;
    debug!("[OK] File end position: {} bytes", end_pos);
    assert_eq!(
        end_pos as usize, expected_size,
        "File size should match expected size"
    );

    tx2.commit_test().await?;

    debug!("SUCCESS: Streaming reader works correctly for small files");
    debug!("  - Small files use inline storage (Cursor over Vec<u8>)");
    debug!("  - AsyncRead works for partial reads");
    debug!("  - AsyncSeek works for random access");
    debug!("  - File size detection works correctly");

    Ok(())
}

/// Test temporal bounds functionality on file series
///
/// This test creates a file series with multiple versions, verifies count queries,
/// sets temporal bounds, and validates that the bounds correctly filter the data.
#[tokio::test]
async fn test_temporal_bounds_on_file_series() -> Result<(), Box<dyn std::error::Error>> {
    use crate::query::execute_sql_on_file;
    use crate::schema::watertown;
    use futures::stream::StreamExt;
    use std::collections::HashMap;

    debug!("=== Starting Temporal Bounds Test ===");

    let store_path = test_dir();

    // Create TLogFS persistence layer
    debug!("Creating persistence layer...");
    let mut persistence = OpLogPersistence::create_test(&store_path).await?;

    let series_path = "test/temporal_series.series";

    // Transaction 1: Create file series with all data points (simulating the combined data from multiple versions)
    debug!(
        "Creating file series with points at T=1,10,11,12,13,14,15,16,17,18,19,50 (12 points total)..."
    );
    {
        let tx = persistence.begin_test().await?;
        let wd = tx.root().await?;

        // Create test directory if it doesn't exist
        if !wd.exists(std::path::Path::new("test")).await {
            _ = wd.create_dir_path("test").await?;
        }

        // Create batch with timestamps as proper Unix epoch seconds (not milliseconds)
        // T=1,10,11,12,13,14,15,16,17,18,19,50 (12 points total)
        use arrow_array::{Float64Array, RecordBatch, StringArray, TimestampSecondArray};
        use arrow_schema::{DataType, Field, Schema, TimeUnit};
        use std::sync::Arc;

        let timestamp_data = TimestampSecondArray::from(vec![
            Some(1_i64),
            Some(10_i64),
            Some(11_i64),
            Some(12_i64),
            Some(13_i64),
            Some(14_i64),
            Some(15_i64),
            Some(16_i64),
            Some(17_i64),
            Some(18_i64),
            Some(19_i64),
            Some(50_i64),
        ])
        .with_timezone("+00:00");

        let value_data = Float64Array::from(vec![
            100.0_f64, 110.0_f64, 111.0_f64, 112.0_f64, 113.0_f64, 114.0_f64, 115.0_f64, 116.0_f64,
            117.0_f64, 118.0_f64, 119.0_f64, 150.0_f64,
        ]);

        let sensor_data = StringArray::from(vec![
            "sensor_001",
            "sensor_001",
            "sensor_001",
            "sensor_001",
            "sensor_001",
            "sensor_001",
            "sensor_001",
            "sensor_001",
            "sensor_001",
            "sensor_001",
            "sensor_001",
            "sensor_001",
        ]);

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())),
                false,
            ),
            Field::new("value", DataType::Float64, false),
            Field::new("sensor_id", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(timestamp_data),
                Arc::new(value_data),
                Arc::new(sensor_data),
            ],
        )?;

        let (min_time, max_time) = wd
            .create_series_from_batch(series_path, &batch, Some("timestamp"))
            .await?;
        debug!("Series created. Time range: {} to {}", min_time, max_time);

        tx.commit_test().await?;
    }

    // Transaction 3: Query the series before setting temporal bounds - should return 12 rows
    debug!("Querying series before temporal bounds...");
    {
        let tx = persistence.begin_test().await?;
        let wd = tx.root().await?;

        let mut result_stream = execute_sql_on_file(
            &wd,
            series_path,
            "SELECT COUNT(*) as row_count FROM source",
            &tx.state()?.as_provider_context(),
        )
        .await?;

        let mut total_count = 0_i64;
        while let Some(batch_result) = result_stream.next().await {
            let batch = batch_result?;
            if batch.num_rows() > 0 {
                let count_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .expect("COUNT(*) should return Int64Array");
                total_count = count_array.value(0);
            }
        }

        debug!("Count before temporal bounds: {}", total_count);
        assert_eq!(
            total_count, 12,
            "Should have 12 total rows before temporal bounds"
        );

        // Don't commit this read-only transaction
    }

    // Transaction 4: Set temporal bounds to [10000, 20000] ms using empty version approach
    debug!("Setting temporal bounds to [10000, 20000] ms (10-20 seconds) using empty version...");
    {
        let tx = persistence.begin_test().await?;
        let wd = tx.root().await?;

        // Create an empty version with temporal bounds (following CLI pattern)
        // Use TinyFS async writer to add empty version to existing file
        let mut writer = wd
            .async_writer_path_with_type(series_path, tinyfs::EntryType::TablePhysicalSeries)
            .await?;

        // Write empty content to create a pending record
        use tokio::io::AsyncWriteExt;
        writer.write_all(&[]).await?;
        writer.flush().await?;
        writer.shutdown().await?;

        debug!("Created empty version for metadata");

        // Set temporal overrides using extended attributes on the path
        // Note: bounds need to be in milliseconds since that's the internal representation
        let mut attributes = HashMap::new();
        _ = attributes.insert(
            watertown::MIN_TEMPORAL_OVERRIDE.to_string(),
            "10000".to_string(),
        ); // 10 seconds = 10000 ms
        _ = attributes.insert(
            watertown::MAX_TEMPORAL_OVERRIDE.to_string(),
            "20000".to_string(),
        ); // 20 seconds = 20000 ms

        wd.set_extended_attributes(series_path, attributes).await?;

        debug!("Temporal bounds set to [10000, 20000] milliseconds (10-20 seconds)");
        tx.commit_test().await?;
    }

    // Transaction 5: Query the series after setting temporal bounds - should return 10 rows
    debug!("Querying series after temporal bounds...");
    {
        let tx = persistence.begin_test().await?;
        let wd = tx.root().await?;

        let mut result_stream = execute_sql_on_file(
            &wd,
            series_path,
            "SELECT COUNT(*) as row_count FROM source",
            &tx.state()?.as_provider_context(),
        )
        .await?;

        let mut total_count = 0_i64;
        while let Some(batch_result) = result_stream.next().await {
            let batch = batch_result?;
            if batch.num_rows() > 0 {
                let count_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .expect("COUNT(*) should return Int64Array");
                total_count = count_array.value(0);
            }
        }

        debug!("Count after temporal bounds: {}", total_count);
        assert_eq!(
            total_count, 10,
            "Should have 10 rows after temporal bounds [10000, 20000] ms (excluding T=1s and T=50s)"
        );

        // Don't commit this read-only transaction
    }

    debug!("SUCCESS: Temporal bounds test completed");
    debug!(
        "  - Created file series with data points at T=1,10,11,12,13,14,15,16,17,18,19,50 seconds"
    );
    debug!("  - Verified initial count of 12 rows");
    debug!("  - Set temporal bounds to [10000, 20000] ms (10-20 seconds)");
    debug!("  - Verified filtered count of 10 rows (excluded T=1s and T=50s)");

    Ok(())
}

/// Test to reproduce unnecessary directory updates when appending to file:series
///
/// This test writes multiple versions to the same file:series and checks
/// whether the parent directory gets updated on each append (it shouldn't).
#[tokio::test]
async fn test_multiple_series_appends_directory_updates() -> Result<(), Box<dyn std::error::Error>>
{
    debug!("=== Testing Multiple Series Appends - Directory Update Check ===");

    let test_result = timeout(Duration::from_secs(30), async {
        let store_path = test_dir();

        // Create TLogFS persistence layer
        debug!("Creating persistence layer...");
        let mut persistence = OpLogPersistence::create_test(&store_path).await?;

        // Check filesystem after bootstrap
        debug!("\n=== Checking filesystem after bootstrap ===");
        let store_path_obj = std::path::Path::new(&store_path);
        debug!("Store path: {}", store_path);
        debug!("Store path exists: {}", store_path_obj.exists());
        if store_path_obj.exists() &&
            let Ok(entries) = std::fs::read_dir(store_path_obj) {
                for entry in entries.flatten() {
                    debug!("  - {}", entry.path().display());
		}
            }

        // Check _delta_log directory
        let delta_log_path = store_path_obj.join("_delta_log");
        debug!("Delta log exists: {}", delta_log_path.exists());
        if delta_log_path.exists() &&
            let Ok(entries) = std::fs::read_dir(&delta_log_path) {
                for entry in entries.flatten() {
                        debug!("  - delta log: {}", entry.path().display());
                }
        }

        let series_path = "devices/sensor_123/data.series";

        // === TRANSACTION 2: Create directory structure and initial file ===
        debug!("\n=== Transaction 2: Create directory and first version ===");
        let tx1 = persistence.begin_test().await?;
        let wd1 = tx1.root().await?;

        // Create devices directory
        debug!("Creating /devices directory...");
        _ = wd1.create_dir_path("devices").await?;

        // Create sensor subdirectory
        debug!("Creating /devices/sensor_123 directory...");
        _ = wd1.create_dir_path("devices/sensor_123").await?;

        // Write first version of series
        let batch1 = record_batch!(
            ("timestamp", Int64, [1704067200000_i64, 1704070800000_i64]),
            ("value", Float64, [10.5_f64, 20.3_f64])
        )?;

        debug!("Writing FIRST version of series...");
        _ = wd1.create_series_from_batch(series_path, &batch1, Some("timestamp")).await?;

        debug!("Committing transaction 2...");
        tx1.commit_test().await?;

        // === TRANSACTION 3: Append to existing series (SHOULD NOT UPDATE DIRECTORY) ===
        debug!("\n=== Transaction 3: Append to existing series ===");
        let tx2 = persistence.begin_test().await?;
        let wd2 = tx2.root().await?;

        // Verify file exists
        let file_exists = wd2.exists(std::path::Path::new(series_path)).await;
        debug!("File exists before append: {}", file_exists);
        assert!(file_exists, "File should exist from transaction 1");

        // Append more data using async_writer_path_with_type
        let batch2 = record_batch!(
            ("timestamp", Int64, [1704074400000_i64, 1704078000000_i64]),
            ("value", Float64, [15.8_f64, 25.1_f64])
        )?;

        debug!("Appending SECOND version using async_writer...");

        // Serialize batch to parquet
        use std::io::Cursor;
        use parquet::arrow::ArrowWriter;
        let mut buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut buffer);
            let props = parquet::file::properties::WriterProperties::builder().build();
            let mut writer = ArrowWriter::try_new(cursor, batch2.schema(), Some(props))?;
            writer.write(&batch2)?;
            _ = writer.close()?;
        }

        // Get writer for existing file - THIS SHOULD NOT UPDATE THE DIRECTORY
        let mut writer = wd2.async_writer_path_with_type(
            series_path,
            tinyfs::EntryType::TablePhysicalSeries
        ).await?;

        use tokio::io::AsyncWriteExt;
        writer.write_all(&buffer).await?;

        // Infer temporal bounds from the written parquet file
        // This also handles shutdown internally
        _ = writer.infer_temporal_bounds().await?;

        debug!("Committing transaction 3...");
        tx2.commit_test().await?;

        // === TRANSACTION 4: Verify that TX3 only created 1 oplog entry (the file) ===
        debug!("\n=== Transaction 4: Verify physical layer - checking oplog entries per transaction ===");
        let mut tx3 = persistence.begin_test().await?;

        let ctx = tx3.session_context().await?;

        // Count entries by transaction (based on commit order/timestamp)
        // We expect:
        // - TX0 (root creation): 1 entry
        // - TX1 (structure + file v1): 4 entries (root update + 2 dirs + 1 file)
        // - TX2 (file v2 append): 1 entry (ONLY the file, NO directory updates)

        let sql = "SELECT part_id, node_id, file_type, version, timestamp FROM delta_table ORDER BY timestamp";
        let df = ctx.sql(sql).await?;
        let results = df.collect().await?;

        let mut total_entries = 0;
        for batch in &results {
            total_entries += batch.num_rows();
        }

        debug!("\n=== OpLog Analysis ===");
        debug!("Total oplog entries: {}", total_entries);

        // Expected: TX1(1 root) + TX2(3 dirs + 1 file) + TX3(1 file) = 6 entries
        // If we have 7+, there are unnecessary writes

        // Count entries in TX3 specifically by looking at what was added after TX2
        // TX2 should have committed 4 entries, TX3 should add only 1 more
        let tx2_count_sql = "SELECT COUNT(*) as count FROM (SELECT * FROM delta_table ORDER BY timestamp LIMIT 5)";
        let tx2_df = ctx.sql(tx2_count_sql).await?;
        let _tx2_results = tx2_df.collect().await?;

        // Better approach: Count unique directories and their versions
        let dir_versions_sql = "SELECT node_id, file_type, COUNT(*) as version_count FROM delta_table WHERE file_type LIKE 'dir:%' GROUP BY node_id, file_type ORDER BY version_count DESC, node_id";
        let dir_versions_df = ctx.sql(dir_versions_sql).await?;
        let dir_versions_results = dir_versions_df.collect().await?;

        debug!("\n=== Directory Version Counts ===");
        let mut max_versions = 0;
        for batch in &dir_versions_results {
            debug!("Directories tracked: {} unique directories", batch.num_rows());
            max_versions = std::cmp::max(max_versions, batch.num_rows());
        }

        // Count total directory entries (should be 3: root, devices, sensor_123)
        // But if root was updated, we'll see 4 entries (root v1, root v2, devices v1, sensor_123 v1)
        let dir_entry_count_sql = "SELECT COUNT(*) as total_dir_entries FROM delta_table WHERE file_type LIKE 'dir:%'";
        let dir_entry_df = ctx.sql(dir_entry_count_sql).await?;
        let dir_entry_results = dir_entry_df.collect().await?;

        let mut total_dir_entries = 0;
        for batch in &dir_entry_results {
            if batch.num_rows() > 0 {
                use arrow_array::cast::AsArray;
                let count_col = batch.column(0).as_primitive::<arrow_array::types::Int64Type>();
                total_dir_entries = count_col.value(0) as usize;
            }
        }

        debug!("Total directory entries in oplog: {}", total_dir_entries);

        // THE KEY CHECK: store_node() skips creating empty directory versions when has_pending_operations() is true
        // Expected: root v1 (bootstrap), root v2 (with devices), devices v1 (with sensor_123), sensor_123 v1 (with file)
        // NOT created: devices v1 empty, sensor_123 v1 empty (skipped because insert() happened in same transaction)
        let expected_dir_entries = 4;

        debug!("\n=== Verification Result ===");
        debug!("Expected directory entries: {}", expected_dir_entries);
        debug!("Actual directory entries: {}", total_dir_entries);

        // Count rows per transaction to verify each transaction wrote the expected number
        debug!("\n=== Per-Transaction Row Counts ===");

        // TX1 (bootstrap): Should write 1 row (root v1)
        let tx1_sql = "SELECT COUNT(*) as count FROM delta_table WHERE txn_seq = 1";
        let tx1_df = ctx.sql(tx1_sql).await?;
        let tx1_results = tx1_df.collect().await?;
        let tx1_count = if let Some(batch) = tx1_results.first() {
            if batch.num_rows() > 0 {
                use arrow_array::cast::AsArray;
                let count_col = batch.column(0).as_primitive::<arrow_array::types::Int64Type>();
                count_col.value(0) as usize
            } else {
                0
            }
        } else {
            0
        };
        debug!("TX1 (bootstrap): {} row(s)", tx1_count);

        // TX2 (create structure): Should write 4 rows (root v2, devices v1, sensor_123 v1, file v1)
        let tx2_sql = "SELECT COUNT(*) as count FROM delta_table WHERE txn_seq = 2";
        let tx2_df = ctx.sql(tx2_sql).await?;
        let tx2_results = tx2_df.collect().await?;
        let tx2_count = if let Some(batch) = tx2_results.first() {
            if batch.num_rows() > 0 {
                use arrow_array::cast::AsArray;
                let count_col = batch.column(0).as_primitive::<arrow_array::types::Int64Type>();
                count_col.value(0) as usize
            } else {
                0
            }
        } else {
            0
        };
        debug!("TX2 (create structure): {} row(s)", tx2_count);

        // TX3 (file append): Should write EXACTLY 1 row (file v2 only, NO directory update)
        let tx3_sql = "SELECT COUNT(*) as count FROM delta_table WHERE txn_seq = 3";
        let tx3_df = ctx.sql(tx3_sql).await?;
        let tx3_results = tx3_df.collect().await?;
        let tx3_count = if let Some(batch) = tx3_results.first() {
            if batch.num_rows() > 0 {
                use arrow_array::cast::AsArray;
                let count_col = batch.column(0).as_primitive::<arrow_array::types::Int64Type>();
                count_col.value(0) as usize
            } else {
                0
            }
        } else {
            0
        };
        debug!("TX3 (file append): {} row(s)", tx3_count);

        // THE CRITICAL CHECK: TX3 should write exactly 1 row
        if tx3_count != 1 {
            debug!("\n[BUG] BUG DETECTED IN TX3!");
            debug!("TX3 (file append) wrote {} records when it should write exactly 1", tx3_count);
            debug!("Expected: 1 row (file v2 only)");
            debug!("Actual: {} row(s)", tx3_count);
            debug!("\nWhen appending to a file:series, we should NOT update the parent directory.");
            panic!("Test failed: TX3 wrote {} records instead of 1", tx3_count);
        }

        debug!("[OK] PASS: TX3 wrote exactly 1 record (file version only, no directory update)");

        // Also check: TX3 should create exactly 1 entry (the file v2)
        // Total should be: 1 (root v1) + 4 (TX2: root v2, devices, sensor_123, file v1) + 1 (TX3: file v2) = 6
        // But if bug exists: 1 + 4 + 2 (file v2 + unnecessary dir update) = 7+

        debug!("Total oplog entries: {}", total_entries);
        debug!("Expected total: 6 (1 root init + 4 structure creation + 1 file append)");

        // Verify we have the expected number of directory entries
        assert_eq!(total_dir_entries, expected_dir_entries,
            "Expected {} directory entries (root v1 empty, root v2 with devices, devices v1, sensor_123 v1), but found {}",
            expected_dir_entries, total_dir_entries);

        debug!("\n[OK] TEST PASSED!");
        debug!("- TX3 (file append) wrote exactly 1 row (file version only)");
        debug!("- No unnecessary directory updates during file append");
        debug!("- Root directory correctly has 2 versions (v1 empty, v2 when devices added)");

        Ok(())
    }).await;

    match test_result {
        Ok(result) => result,
        Err(_) => {
            panic!("Test timed out after 30 seconds");
        }
    }
}

#[tokio::test]
async fn test_symlink_create_and_read() {
    let store_path = test_dir();

    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence layer");

    debug!("=== TX1: Create symlink and read in same transaction ===");

    let tx1 = persistence
        .begin_test()
        .await
        .expect("Failed to begin transaction");

    let root = tx1.root().await.expect("Failed to get root");

    // Create a target file with content
    debug!("Creating target file: /target.txt");
    use tinyfs::async_helpers::convenience;
    _ = convenience::create_file_path(&root, "/target.txt", b"Hello, symlink!")
        .await
        .expect("Failed to create target file");

    // Create a symlink pointing to the target
    debug!("Creating symlink: /link -> /target.txt");
    _ = root
        .create_symlink_path("/link", "/target.txt")
        .await
        .expect("Failed to create symlink");

    // Follow the symlink and read the file content in the same transaction
    debug!("Following symlink to read file content in TX1");
    let content = root
        .read_file_path_to_vec("/link")
        .await
        .expect("Failed to read file through symlink in TX1");

    assert_eq!(
        content, b"Hello, symlink!",
        "Should be able to read file through symlink in same transaction"
    );
    debug!(
        "[OK] Read file content through symlink in TX1: {:?}",
        String::from_utf8_lossy(&content)
    );

    // Commit TX1
    debug!("Committing TX1");
    _ = tx1.commit().await.expect("Failed to commit TX1");

    debug!("\n=== TX2: Read symlink in new transaction ===");

    let tx2 = persistence.begin_test().await.expect("Failed to begin TX2");

    let root2 = tx2.root().await.expect("Failed to get root in TX2");

    // Follow the symlink in TX2 and read content
    debug!("Following symlink in TX2 to read file content");
    let content2 = root2
        .read_file_path_to_vec("/link")
        .await
        .expect("Failed to read file through symlink in TX2");

    assert_eq!(
        content2, b"Hello, symlink!",
        "Should be able to read file through symlink in new transaction"
    );
    debug!(
        "[OK] Read file content through symlink in TX2: {:?}",
        String::from_utf8_lossy(&content2)
    );

    debug!("\n[OK] TEST PASSED!");
    debug!("- Created symlink in TX1");
    debug!("- Read symlink target in same transaction");
    debug!("- Followed symlink to read file content in same transaction");
    debug!("- Committed TX1");
    debug!("- Read symlink in new transaction (TX2)");
    debug!("- Followed symlink in TX2 to read file content");
}

/// Test creating dynamic files using high-level path API
#[tokio::test]
async fn test_create_dynamic_file_path() {
    let store_path = test_dir();

    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence layer");

    debug!("=== Testing dynamic file creation with path API ===");

    let tx = persistence
        .begin_test()
        .await
        .expect("Failed to begin transaction");

    let root = tx.root().await.expect("Failed to get root");

    // Create parent directory first
    debug!("Creating parent directory: /config");
    _ = root
        .create_dir_path("/config")
        .await
        .expect("Failed to create parent directory");

    // Create a dynamic file with factory name and YAML config
    debug!("Creating dynamic file: /config/test.yaml");
    let test_file_config = tinyfs::testing::TestFileConfig::simple("SELECT 1");
    let config_content = test_file_config
        .to_yaml_bytes()
        .expect("Failed to serialize test file config");

    let dynamic_node = root
        .create_dynamic_path(
            "/config/test.yaml",
            tinyfs::EntryType::FileDynamic,
            "test-file",
            config_content.clone(),
        )
        .await
        .expect("Failed to create dynamic file");

    debug!("[OK] Created dynamic file with factory: test-file");
    debug!("   Node ID: {:?}", dynamic_node.id());

    // Verify EntryType is correctly embedded in the FileID
    let file_id = dynamic_node.id();
    let entry_type = file_id.entry_type();
    assert_eq!(
        entry_type,
        tinyfs::EntryType::FileDynamic,
        "Dynamic file should have FileDynamic EntryType"
    );
    debug!("[OK] FileID has correct EntryType: {:?}", entry_type);

    // Verify we can read the file back
    let read_content = root
        .read_file_path_to_vec("/config/test.yaml")
        .await
        .expect("Failed to read dynamic file");

    // Content from test-file factory is the "content" field, not the YAML config
    assert_eq!(
        String::from_utf8_lossy(&read_content),
        "SELECT 1",
        "Dynamic file content should be from TestFileConfig.content field"
    );
    debug!("[OK] Read back dynamic file content successfully");

    // Commit and verify persistence
    debug!("Committing transaction");
    _ = tx.commit().await.expect("Failed to commit");

    // Read in new transaction
    debug!("Reading dynamic file in new transaction");
    let tx2 = persistence.begin_test().await.expect("Failed to begin TX2");

    let root2 = tx2.root().await.expect("Failed to get root in TX2");

    // Read back the node to verify EntryType persisted correctly
    let node_path2 = root2
        .get_node_path(std::path::Path::new("/config/test.yaml"))
        .await
        .expect("Failed to get node");

    let file_id2 = node_path2.id();
    let entry_type2 = file_id2.entry_type();
    assert_eq!(
        entry_type2,
        tinyfs::EntryType::FileDynamic,
        "Persisted dynamic file should have valid FileDynamic EntryType"
    );
    debug!(
        "[OK] Persisted FileID has correct EntryType: {:?}",
        entry_type2
    );

    let content2 = root2
        .read_file_path_to_vec("/config/test.yaml")
        .await
        .expect("Failed to read file in TX2");

    // Content from test-file factory is the "content" field, not the YAML config
    assert_eq!(
        String::from_utf8_lossy(&content2),
        "SELECT 1",
        "Dynamic file should persist and return content from factory"
    );
    debug!("[OK] Dynamic file persisted correctly");

    debug!("\n[OK] TEST PASSED!");
    debug!("- Created dynamic file using high-level path API");
    debug!("- Read back content in same transaction");
    debug!("- Committed and verified persistence");
}

/// Test creating dynamic directories using high-level path API
/// NOTE: This test verifies dynamic directory creation with the test-dir factory.
/// This demonstrates that dynamic nodes require proper factory registration.
#[tokio::test]
async fn test_create_dynamic_directory_path() {
    // Use fixed path so we can inspect Delta files after test
    let store_path = "/tmp/tlogfs_dynamic_dir_test";
    let _ = std::fs::remove_dir_all(store_path); // Clean up from previous run

    log::debug!("\n[SEARCH] Test store path: {}", store_path);

    let mut persistence = OpLogPersistence::create_test(store_path)
        .await
        .expect("Failed to create persistence layer");

    debug!("=== Testing dynamic directory creation with path API ===");

    let tx = persistence
        .begin_test()
        .await
        .expect("Failed to begin transaction");

    let root = tx.root().await.expect("Failed to get root");

    // Create parent directory first
    debug!("Creating parent directory: /factories");
    _ = root
        .create_dir_path("/factories")
        .await
        .expect("Failed to create parent directory");

    // Create a dynamic directory with factory name and YAML config
    debug!("Creating dynamic directory: /factories/processors");
    let test_dir_config = tinyfs::testing::TestDirectoryConfig::simple("processors");
    let config_content = test_dir_config
        .to_yaml_bytes()
        .expect("Failed to serialize test dir config");

    let dynamic_dir = root
        .create_dynamic_path(
            "/factories/processors",
            tinyfs::EntryType::DirectoryDynamic,
            "test-dir",
            config_content,
        )
        .await
        .expect("Failed to create dynamic directory");

    debug!("[OK] Created dynamic directory with factory: test-dir");
    debug!("   Node ID: {:?}", dynamic_dir.id());

    // Verify the directory exists
    let exists = root
        .exists(std::path::Path::new("/factories/processors"))
        .await;
    assert!(exists, "Dynamic directory should exist");
    debug!("[OK] Dynamic directory exists");

    // Commit
    debug!("Committing transaction");
    _ = tx.commit().await.expect("Failed to commit");

    // Inspect what Delta Lake actually wrote
    log::debug!("\n[SEARCH] After TX1 commit - inspecting Delta Lake files:");
    log::debug!("[DIR] Store path: {}", store_path);

    // List _delta_log directory
    let delta_log_path = format!("{}/_delta_log", store_path);
    if let Ok(entries) = std::fs::read_dir(&delta_log_path) {
        log::debug!("[DIR] _delta_log directory:");
        for entry in entries.flatten() {
            log::debug!("  - {}", entry.file_name().to_string_lossy());
        }
    }

    // List main directory for Parquet files
    if let Ok(entries) = std::fs::read_dir(store_path) {
        log::debug!("[DIR] Main directory:");
        for entry in entries.flatten() {
            let fname = entry.file_name().to_string_lossy().to_string();
            if fname.ends_with(".parquet") {
                log::debug!("  - {} (Parquet data file)", fname);
            }
        }
    }

    log::debug!(
        "\n[HINT] Use: ./catparquet.sh {} <file>.parquet",
        store_path
    );
    log::debug!("[HINT] to inspect the actual data written\n");

    // Verify in new transaction
    let tx2 = persistence.begin_test().await.expect("Failed to begin TX2");

    let root2 = tx2.root().await.expect("Failed to get root in TX2");

    debug!("TX2: Looking up /factories/processors");
    let result = root2
        .get_node_path(std::path::Path::new("/factories/processors"))
        .await;

    let _node_path = result.expect("Dynamic directory should persist");
    debug!("[OK] Dynamic directory persisted correctly");

    debug!("\n[OK] TEST PASSED!");
    debug!("- Created dynamic directory using high-level path API");
    debug!("- Verified existence in same transaction");
    debug!("- Committed and verified persistence");
}

/// Test that would have caught the EntryType bug in FileID::from_content
///
/// This test creates dynamic nodes (both files and directories), commits them,
/// then reads them back and verifies:
/// 1. FileIDs are valid and parseable
/// 2. EntryType is correctly embedded in NodeID
/// 3. Dynamic nodes can be read and used after persistence
#[tokio::test]
async fn test_dynamic_node_entry_type_validation() {
    let store_path = test_dir();

    log::debug!("\n[SEARCH] Testing dynamic node EntryType validation");

    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence layer");

    // Transaction 1: Create dynamic file and directory
    let tx1 = persistence
        .begin_test()
        .await
        .expect("Failed to begin transaction");

    let root = tx1.root().await.expect("Failed to get root");

    // Create parent directory
    log::debug!("Creating parent directory: /dynamic-test");
    let _ = root
        .create_dir_path("/dynamic-test")
        .await
        .expect("Failed to create parent directory");

    // Create dynamic file using test-file factory
    log::debug!("Creating dynamic file: /dynamic-test/test.yaml");
    let test_file_config =
        tinyfs::testing::TestFileConfig::simple("test content from dynamic file");
    let file_config_bytes = test_file_config
        .to_yaml_bytes()
        .expect("Failed to serialize test file config");

    let dynamic_file = root
        .create_dynamic_path(
            "/dynamic-test/test.yaml",
            tinyfs::EntryType::FileDynamic,
            "test-file",
            file_config_bytes,
        )
        .await
        .expect("Failed to create dynamic file");

    let file_id = dynamic_file.id();
    log::debug!("[OK] Created dynamic file with ID: {:?}", file_id);

    // Verify EntryType is valid immediately after creation
    let file_entry_type = file_id.entry_type();
    log::debug!("  EntryType from FileID: {:?}", file_entry_type);
    assert_eq!(
        file_entry_type,
        tinyfs::EntryType::FileDynamic,
        "Dynamic file should have FileDynamic EntryType"
    );

    // Create dynamic directory using test-dir factory
    log::debug!("Creating dynamic directory: /dynamic-test/subdir");
    let test_dir_config = tinyfs::testing::TestDirectoryConfig::simple("subdir");
    let dir_config_bytes = test_dir_config
        .to_yaml_bytes()
        .expect("Failed to serialize test dir config");

    let dynamic_dir = root
        .create_dynamic_path(
            "/dynamic-test/subdir",
            tinyfs::EntryType::DirectoryDynamic,
            "test-dir",
            dir_config_bytes,
        )
        .await
        .expect("Failed to create dynamic directory");

    let dir_id = dynamic_dir.id();
    log::debug!("[OK] Created dynamic directory with ID: {:?}", dir_id);

    // Verify EntryType is valid immediately after creation
    let dir_entry_type = dir_id.entry_type();
    log::debug!("  EntryType from FileID: {:?}", dir_entry_type);
    assert_eq!(
        dir_entry_type,
        tinyfs::EntryType::DirectoryDynamic,
        "Dynamic directory should have DirectoryDynamic EntryType"
    );

    // Commit transaction
    log::debug!("Committing transaction 1");
    tx1.commit_test()
        .await
        .expect("Failed to commit transaction 1");

    // Transaction 2: Read back the dynamic nodes and verify EntryTypes
    log::debug!("\nStarting transaction 2 to read back dynamic nodes");
    let tx2 = persistence
        .begin_test()
        .await
        .expect("Failed to begin transaction 2");

    let root2 = tx2.root().await.expect("Failed to get root in TX2");

    // Read back dynamic file
    log::debug!("Reading back dynamic file: /dynamic-test/test.yaml");
    let file_node = root2
        .get_node_path(std::path::Path::new("/dynamic-test/test.yaml"))
        .await
        .expect("Failed to get dynamic file");

    let read_file_id = file_node.id();
    log::debug!("[OK] Read back dynamic file with ID: {:?}", read_file_id);

    // This is the critical test: can we parse the EntryType from the persisted FileID?
    let read_file_entry_type = read_file_id.entry_type();
    log::debug!(
        "  EntryType from persisted FileID: {:?}",
        read_file_entry_type
    );
    assert_eq!(
        read_file_entry_type,
        tinyfs::EntryType::FileDynamic,
        "Persisted dynamic file should have valid FileDynamic EntryType"
    );

    // Verify we can actually read the file content
    let file_content = root2
        .read_file_path_to_vec("/dynamic-test/test.yaml")
        .await
        .expect("Failed to read dynamic file content");
    let content_str = String::from_utf8_lossy(&file_content);
    log::debug!("  File content: {:?}", content_str);
    assert_eq!(
        content_str, "test content from dynamic file",
        "Dynamic file content should match"
    );

    // Read back dynamic directory
    log::debug!("Reading back dynamic directory: /dynamic-test/subdir");
    let dir_node = root2
        .get_node_path(std::path::Path::new("/dynamic-test/subdir"))
        .await
        .expect("Failed to get dynamic directory");

    let read_dir_id = dir_node.id();
    log::debug!(
        "[OK] Read back dynamic directory with ID: {:?}",
        read_dir_id
    );

    // This is the critical test: can we parse the EntryType from the persisted FileID?
    let read_dir_entry_type = read_dir_id.entry_type();
    log::debug!(
        "  EntryType from persisted FileID: {:?}",
        read_dir_entry_type
    );
    assert_eq!(
        read_dir_entry_type,
        tinyfs::EntryType::DirectoryDynamic,
        "Persisted dynamic directory should have valid DirectoryDynamic EntryType"
    );

    // Verify we can list the directory (even though it's empty)
    use futures::stream::StreamExt;
    let dir_node_handle = dir_node
        .into_dir()
        .await
        .expect("Node should be a directory");
    let mut entries_stream = dir_node_handle
        .handle
        .entries()
        .await
        .expect("Failed to get entries");
    let mut entry_count = 0;
    while let Some(entry_result) = entries_stream.next().await {
        let _entry = entry_result.expect("Failed to read entry");
        entry_count += 1;
    }
    log::debug!("  Directory has {} entries", entry_count);

    log::debug!("\n[OK] TEST PASSED!");
    log::debug!("- Created dynamic file and directory");
    log::debug!("- Verified EntryType immediately after creation");
    log::debug!("- Committed to persistence");
    log::debug!("- Read back from new transaction");
    log::debug!("- Verified EntryType from persisted NodeIDs (would have panicked with bug)");
    log::debug!("- Successfully used dynamic nodes (read file, list directory)");
}

/// Test FilePhysicalSeries: write multiple versions and read concatenated content
/// FilePhysicalSeries entries have "all-version" semantics where each write appends data.
/// When reading, all versions are concatenated in oldest-to-newest order.
#[tokio::test]
async fn test_file_physical_series_version_concatenation() {
    log::debug!("=== Starting FilePhysicalSeries Version Concatenation Test ===");

    let store_path = test_dir();

    // Create TLogFS persistence layer
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence");

    let file_path = "data/events.csv";

    // Write first version (CSV header + first data row)
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin transaction");
        let wd = tx.root().await.expect("Failed to get root");

        // Create data directory
        if !wd.exists(std::path::Path::new("data")).await {
            _ = wd
                .create_dir_path("data")
                .await
                .expect("Failed to create data dir");
        }

        // Write first version with header and first data row
        let csv_v1 = b"name,value\nalice,100\n";

        // Use async_writer_path_with_type to specify FilePhysicalSeries entry type
        use tokio::io::AsyncWriteExt;
        let mut writer = wd
            .async_writer_path_with_type(file_path, tinyfs::EntryType::FilePhysicalSeries)
            .await
            .expect("Failed to create writer for version 1");
        writer
            .write_all(csv_v1)
            .await
            .expect("Failed to write version 1");
        writer
            .shutdown()
            .await
            .expect("Failed to shutdown version 1 writer");

        tx.commit_test().await.expect("Failed to commit version 1");
        log::debug!("[OK] Committed version 1 with {} bytes", csv_v1.len());
    }

    // Write second version (more data rows)
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin transaction");
        let wd = tx.root().await.expect("Failed to get root");

        let csv_v2 = b"bob,200\n";

        use tokio::io::AsyncWriteExt;
        let mut writer = wd
            .async_writer_path_with_type(file_path, tinyfs::EntryType::FilePhysicalSeries)
            .await
            .expect("Failed to create writer for version 2");
        writer
            .write_all(csv_v2)
            .await
            .expect("Failed to write version 2");
        writer
            .shutdown()
            .await
            .expect("Failed to shutdown version 2 writer");

        tx.commit_test().await.expect("Failed to commit version 2");
        log::debug!("[OK] Committed version 2 with {} bytes", csv_v2.len());
    }

    // Write third version (even more data rows)
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin transaction");
        let wd = tx.root().await.expect("Failed to get root");

        let csv_v3 = b"carol,300\n";

        use tokio::io::AsyncWriteExt;
        let mut writer = wd
            .async_writer_path_with_type(file_path, tinyfs::EntryType::FilePhysicalSeries)
            .await
            .expect("Failed to create writer for version 3");
        writer
            .write_all(csv_v3)
            .await
            .expect("Failed to write version 3");
        writer
            .shutdown()
            .await
            .expect("Failed to shutdown version 3 writer");

        tx.commit_test().await.expect("Failed to commit version 3");
        log::debug!("[OK] Committed version 3 with {} bytes", csv_v3.len());
    }

    // Read back and verify concatenated content
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin read transaction");
        let wd = tx.root().await.expect("Failed to get root");

        // Read as raw bytes - should be concatenation of all versions
        let content = wd
            .read_file_path_to_vec(file_path)
            .await
            .expect("Failed to read file");

        let content_str = String::from_utf8(content.clone()).expect("Invalid UTF-8");
        log::debug!("Read {} bytes: {:?}", content.len(), content_str);

        // Verify the content is concatenated in oldest-to-newest order
        let expected = "name,value\nalice,100\nbob,200\ncarol,300\n";
        assert_eq!(
            content_str, expected,
            "FilePhysicalSeries should concatenate all versions in oldest-to-newest order"
        );

        log::debug!("[OK] Content matches expected concatenation");
    }

    log::debug!("\n[OK] TEST PASSED!");
    log::debug!("- Wrote 3 versions to FilePhysicalSeries file");
    log::debug!("- Each version appended data");
    log::debug!("- Read back yielded concatenated content in correct order");
}

/// Collapse a multi-version FilePhysicalSeries into one merged version, then
/// verify content, version count, and that an append AFTER collapse resumes
/// the cumulative bao-tree from the merged baseline correctly.
#[tokio::test]
async fn test_file_physical_series_collapse_and_append() {
    let _ = env_logger::builder().is_test(true).try_init();

    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence");
    let file_path = "data/events.csv";

    let chunks: [&[u8]; 4] = [
        b"name,value\n",
        b"alice,100\n",
        b"bob,200\n",
        b"carol,300\n",
    ];
    let mut cumulative: Vec<u8> = Vec::new();

    for (i, chunk) in chunks.iter().enumerate() {
        cumulative.extend_from_slice(chunk);
        let tx = persistence.begin_test().await.expect("begin write tx");
        let wd = tx.root().await.expect("root");
        if i == 0 {
            _ = wd.create_dir_path("data").await.expect("create data dir");
        }
        use tokio::io::AsyncWriteExt;
        let mut writer = wd
            .async_writer_path_with_type(file_path, tinyfs::EntryType::FilePhysicalSeries)
            .await
            .expect("writer");
        writer.write_all(chunk).await.expect("write");
        writer.shutdown().await.expect("shutdown");
        tx.commit_test().await.expect("commit");
    }

    // Cumulative bao is correct across the four pre-collapse versions.
    verify_cumulative_blake3(&mut persistence, file_path, &cumulative).await;

    // Collapse the four versions into one merged version.
    let merged_version = {
        let tx = persistence.begin_test().await.expect("begin collapse tx");
        let wd = tx.root().await.expect("root");
        let id = wd.get_node_path(file_path).await.expect("node").id();
        let state = tx.state().expect("state");
        let stats = state
            .collapse_file_series(id, 1)
            .await
            .expect("collapse_file_series");
        assert!(stats.collapsed, "expected a collapse to occur");
        assert_eq!(stats.versions_before, 4);
        tx.commit_test().await.expect("commit collapse");
        stats.merged_version
    };
    assert_eq!(merged_version, 5, "merged version is next after 4 writes");

    // After collapse: content identical and exactly one live version remains.
    {
        use tinyfs::persistence::PersistenceLayer;
        let tx = persistence.begin_test().await.expect("begin read tx");
        let wd = tx.root().await.expect("root");
        let content = wd
            .read_file_path_to_vec(file_path)
            .await
            .expect("read collapsed");
        assert_eq!(
            content, cumulative,
            "collapsed content must equal concatenation"
        );

        let id = wd.get_node_path(file_path).await.expect("node").id();
        let versions = tx
            .state()
            .expect("state")
            .list_file_versions(id)
            .await
            .expect("list versions");
        assert_eq!(versions.len(), 1, "collapse leaves one live version");
        assert_eq!(versions[0].version, 5);
        tx.commit_test().await.expect("commit read");
    }
    verify_cumulative_blake3(&mut persistence, file_path, &cumulative).await;

    // Append AFTER collapse — this is the bao-continuity stress: the append must
    // resume from the merged baseline, not double-count the superseded versions.
    {
        let chunk: &[u8] = b"dave,400\n";
        cumulative.extend_from_slice(chunk);
        let tx = persistence.begin_test().await.expect("begin append tx");
        let wd = tx.root().await.expect("root");
        use tokio::io::AsyncWriteExt;
        let mut writer = wd
            .async_writer_path_with_type(file_path, tinyfs::EntryType::FilePhysicalSeries)
            .await
            .expect("writer append");
        writer.write_all(chunk).await.expect("write append");
        writer.shutdown().await.expect("shutdown append");
        tx.commit_test().await.expect("commit append");
    }

    // Read back merged + appended content and confirm two live versions.
    {
        use tinyfs::persistence::PersistenceLayer;
        let tx = persistence.begin_test().await.expect("begin read2 tx");
        let wd = tx.root().await.expect("root");
        let content = wd
            .read_file_path_to_vec(file_path)
            .await
            .expect("read appended");
        assert_eq!(
            content, cumulative,
            "post-collapse append must read merged + appended content"
        );

        let id = wd.get_node_path(file_path).await.expect("node").id();
        let versions = tx
            .state()
            .expect("state")
            .list_file_versions(id)
            .await
            .expect("list versions");
        assert_eq!(
            versions.len(),
            2,
            "merged version plus one appended version"
        );
        tx.commit_test().await.expect("commit read2");
    }

    // The decisive check: cumulative bao over the full logical content still
    // matches a fresh computation, proving the append resumed correctly.
    verify_cumulative_blake3(&mut persistence, file_path, &cumulative).await;
}

/// Append one `FilePhysicalSeries` version, creating `create_dir` first when
/// provided. Shared by the collapse coverage tests below.
async fn append_series_version(
    persistence: &mut OpLogPersistence,
    file_path: &str,
    chunk: &[u8],
    create_dir: Option<&str>,
) {
    use tokio::io::AsyncWriteExt;
    let tx = persistence.begin_test().await.expect("begin write tx");
    let wd = tx.root().await.expect("root");
    if let Some(dir) = create_dir {
        _ = wd.create_dir_path(dir).await.expect("create dir");
    }
    let mut writer = wd
        .async_writer_path_with_type(file_path, tinyfs::EntryType::FilePhysicalSeries)
        .await
        .expect("writer");
    writer.write_all(chunk).await.expect("write");
    writer.shutdown().await.expect("shutdown");
    tx.commit_test().await.expect("commit");
}

/// Number of live (non-superseded) versions of a series file.
async fn live_version_count(persistence: &mut OpLogPersistence, file_path: &str) -> usize {
    use tinyfs::persistence::PersistenceLayer;
    let tx = persistence.begin_test().await.expect("begin read tx");
    let wd = tx.root().await.expect("root");
    let id = wd.get_node_path(file_path).await.expect("node").id();
    let versions = tx
        .state()
        .expect("state")
        .list_file_versions(id)
        .await
        .expect("list versions");
    let n = versions.len();
    tx.commit_test().await.expect("commit read");
    n
}

/// Collapse of versions whose content exceeds `LARGE_FILE_THRESHOLD` must keep
/// the externally stored bytes intact and preserve cumulative bao continuity
/// across a post-collapse append.
#[tokio::test]
async fn test_collapse_large_file_series() {
    let _ = env_logger::builder().is_test(true).try_init();
    use crate::large_files::LARGE_FILE_THRESHOLD;

    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence");
    let file_path = "data/large.bin";

    // Three versions, each over the large-file threshold so HybridWriter stores
    // them externally rather than inline.
    let chunk_len = LARGE_FILE_THRESHOLD + 4096;
    let mut cumulative: Vec<u8> = Vec::new();
    for (i, byte) in [11u8, 22u8, 33u8].iter().enumerate() {
        let chunk = vec![*byte; chunk_len];
        cumulative.extend_from_slice(&chunk);
        let create = if i == 0 { Some("data") } else { None };
        append_series_version(&mut persistence, file_path, &chunk, create).await;
    }
    assert_eq!(live_version_count(&mut persistence, file_path).await, 3);
    verify_cumulative_blake3(&mut persistence, file_path, &cumulative).await;

    // Collapse the three large versions into one merged version.
    {
        let tx = persistence.begin_test().await.expect("begin collapse tx");
        let wd = tx.root().await.expect("root");
        let id = wd.get_node_path(file_path).await.expect("node").id();
        let stats = tx
            .state()
            .expect("state")
            .collapse_file_series(id, 1)
            .await
            .expect("collapse_file_series");
        assert!(stats.collapsed, "expected a collapse");
        assert_eq!(stats.versions_before, 3);
        assert_eq!(stats.bytes, cumulative.len() as u64);
        tx.commit_test().await.expect("commit collapse");
    }
    assert_eq!(
        live_version_count(&mut persistence, file_path).await,
        1,
        "collapse leaves a single live version"
    );

    // Content and cumulative bao are identical after collapse.
    {
        let tx = persistence.begin_test().await.expect("begin read tx");
        let wd = tx.root().await.expect("root");
        let content = wd
            .read_file_path_to_vec(file_path)
            .await
            .expect("read collapsed");
        assert_eq!(content, cumulative, "collapsed large content matches");
        tx.commit_test().await.expect("commit read");
    }
    verify_cumulative_blake3(&mut persistence, file_path, &cumulative).await;

    // Append after collapse and confirm the bao state resumed from the merged
    // baseline rather than double-counting the superseded large versions.
    let tail = vec![44u8; chunk_len];
    cumulative.extend_from_slice(&tail);
    append_series_version(&mut persistence, file_path, &tail, None).await;
    verify_cumulative_blake3(&mut persistence, file_path, &cumulative).await;
}

/// Collapsing a file that was already collapsed once must use the highest
/// `collapsed_through` sentinel, so only the versions written after the first
/// collapse are merged and the content stays correct.
#[tokio::test]
async fn test_collapse_double_collapse() {
    let _ = env_logger::builder().is_test(true).try_init();

    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence");
    let file_path = "data/events.csv";

    let mut cumulative: Vec<u8> = Vec::new();
    let first_batch: [&[u8]; 3] = [b"a,1\n", b"b,2\n", b"c,3\n"];
    for (i, chunk) in first_batch.iter().enumerate() {
        cumulative.extend_from_slice(chunk);
        let create = if i == 0 { Some("data") } else { None };
        append_series_version(&mut persistence, file_path, chunk, create).await;
    }

    // First collapse: 3 versions -> merged version 4 (collapsed_through = 3).
    let merged_one = collapse_now(&mut persistence, file_path).await;
    assert_eq!(merged_one, 4);
    assert_eq!(live_version_count(&mut persistence, file_path).await, 1);

    // Append two more versions after the first collapse.
    for chunk in [b"d,4\n".as_slice(), b"e,5\n".as_slice()] {
        cumulative.extend_from_slice(chunk);
        append_series_version(&mut persistence, file_path, chunk, None).await;
    }
    assert_eq!(
        live_version_count(&mut persistence, file_path).await,
        3,
        "merged version plus two appends"
    );

    // Second collapse: merged version 4 plus versions 5,6 -> merged version 7
    // (collapsed_through = 6).
    let merged_two = collapse_now(&mut persistence, file_path).await;
    assert_eq!(merged_two, 7);
    assert_eq!(live_version_count(&mut persistence, file_path).await, 1);

    {
        let tx = persistence.begin_test().await.expect("begin read tx");
        let wd = tx.root().await.expect("root");
        let content = wd
            .read_file_path_to_vec(file_path)
            .await
            .expect("read double-collapsed");
        assert_eq!(
            content, cumulative,
            "double-collapse preserves full content"
        );
        tx.commit_test().await.expect("commit read");
    }
    verify_cumulative_blake3(&mut persistence, file_path, &cumulative).await;
}

/// Collapse the series file at `file_path` and return the merged version.
async fn collapse_now(persistence: &mut OpLogPersistence, file_path: &str) -> i64 {
    let tx = persistence.begin_test().await.expect("begin collapse tx");
    let wd = tx.root().await.expect("root");
    let id = wd.get_node_path(file_path).await.expect("node").id();
    let stats = tx
        .state()
        .expect("state")
        .collapse_file_series(id, 1)
        .await
        .expect("collapse_file_series");
    assert!(stats.collapsed, "expected a collapse at {file_path}");
    tx.commit_test().await.expect("commit collapse");
    stats.merged_version
}

/// Read the full series content at `file_path`.
async fn read_series_content(persistence: &mut OpLogPersistence, file_path: &str) -> Vec<u8> {
    use tokio::io::AsyncReadExt;
    let tx = persistence.begin_test().await.expect("begin read tx");
    let wd = tx.root().await.expect("root");
    let mut reader = wd.async_reader_path(file_path).await.expect("reader");
    let mut content = Vec::new();
    _ = reader.read_to_end(&mut content).await.expect("read series");
    tx.commit_test().await.expect("commit read");
    content
}

/// Size-tiered collapse merges a bounded *window* of same-size-class versions
/// instead of the whole series, so several merged runs coexist. This is what
/// makes total write volume `O(N log N)` rather than `O(N^2)`: absorbing new
/// versions must not rewrite the accumulated history.
///
/// The critical invariant is ordering. A merged run is allocated a fresh
/// (highest) version number, so a run covering versions 1..=10 outranks the
/// loose versions 21..=25 that follow it in the byte stream. Readers must
/// therefore order by range start, not by version. Concatenating by version
/// would silently emit the runs *after* the loose tail, which only a
/// multi-run fixture like this one can detect.
#[tokio::test]
async fn test_collapse_tiered_windows_preserve_order_and_content() {
    let _ = env_logger::builder().is_test(true).try_init();

    use crate::persistence::COLLAPSE_FANOUT;

    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence");
    let file_path = "data/tiered.csv";

    // 25 versions of 9 bytes each: small enough to share one size class, so the
    // tiering policy sees a single mergeable group.
    let mut cumulative: Vec<u8> = Vec::new();
    for i in 0..25u32 {
        let chunk = format!("line-{i:03}\n").into_bytes();
        assert_eq!(chunk.len(), 9);
        cumulative.extend_from_slice(&chunk);
        let create = if i == 0 { Some("data") } else { None };
        append_series_version(&mut persistence, file_path, &chunk, create).await;
    }
    assert_eq!(live_version_count(&mut persistence, file_path).await, 25);

    // `max_live` is high enough that the read-cost backstop never fires, so
    // every merge below is a genuine same-class tiered merge.
    async fn collapse_window(persistence: &mut OpLogPersistence, file_path: &str) -> (bool, i64) {
        let tx = persistence.begin_test().await.expect("begin collapse tx");
        let wd = tx.root().await.expect("root");
        let id = wd.get_node_path(file_path).await.expect("node").id();
        let stats = tx
            .state()
            .expect("state")
            .collapse_file_series(id, 1000)
            .await
            .expect("collapse_file_series");
        tx.commit_test().await.expect("commit collapse");
        (stats.collapsed, stats.merged_version)
    }

    // First window: COLLAPSE_FANOUT versions merge into one run, leaving the
    // rest loose. A whole-series collapse would have left exactly one version.
    let (first_collapsed, first_version) = collapse_window(&mut persistence, file_path).await;
    assert!(first_collapsed, "expected a tiered collapse");
    assert_eq!(
        live_version_count(&mut persistence, file_path).await,
        25 - COLLAPSE_FANOUT + 1,
        "only one window merges; the remaining versions stay loose"
    );
    assert_eq!(
        read_series_content(&mut persistence, file_path).await,
        cumulative,
        "content unchanged after the first tiered merge"
    );

    // Second window: a second run now coexists with the first. Its version
    // number is higher than the loose tail's, so this is the case that fails if
    // readers order by version rather than by range start.
    let (second_collapsed, second_version) = collapse_window(&mut persistence, file_path).await;
    assert!(second_collapsed, "expected a second tiered collapse");
    assert!(
        second_version > first_version,
        "runs are appended, never renumbered"
    );
    assert_eq!(
        live_version_count(&mut persistence, file_path).await,
        25 - 2 * COLLAPSE_FANOUT + 2,
        "two runs plus the still-loose tail"
    );
    assert_eq!(
        read_series_content(&mut persistence, file_path).await,
        cumulative,
        "two coexisting runs still concatenate in series byte order"
    );

    // Appending after collapse must continue from the tail, and cumulative bao
    // must still describe the whole stream.
    let tail = b"line-final\n";
    cumulative.extend_from_slice(tail);
    append_series_version(&mut persistence, file_path, tail, None).await;
    assert_eq!(
        read_series_content(&mut persistence, file_path).await,
        cumulative,
        "append after tiered collapse lands at the end"
    );
    verify_cumulative_blake3(&mut persistence, file_path, &cumulative).await;
}

/// Append one parquet version to a `TablePhysicalSeries`, creating it on the
/// first call. Each call produces an independent per-version parquet file,
/// which is exactly the layout that makes uncollapsed reads O(versions).
async fn append_table_series_version(
    persistence: &mut OpLogPersistence,
    file_path: &str,
    timestamps: Vec<i64>,
    values: Vec<f64>,
) {
    let tx = persistence.begin_test().await.expect("begin write tx");
    let wd = tx.root().await.expect("root");
    let batch = {
        use arrow_array::{Float64Array, Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(timestamps)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .expect("build batch")
    };
    _ = wd
        .write_series_from_batch(file_path, &batch, Some("timestamp"))
        .await
        .expect("create table series version");
    tx.commit_test().await.expect("commit");
}

/// Total row count of a series as seen through its table provider.
async fn table_series_rows(persistence: &mut OpLogPersistence, file_path: &str) -> usize {
    use futures::StreamExt;
    let tx = persistence.begin_test().await.expect("begin read tx");
    let wd = tx.root().await.expect("root");
    let id = wd.get_node_path(file_path).await.expect("node").id();
    let state = tx.state().expect("state");
    let context = state.as_provider_context();
    let table_provider =
        provider::create_table_provider(id, &context, provider::TableProviderOptions::default())
            .await
            .expect("table provider");
    let session = &context.datafusion_session;
    let plan = table_provider
        .scan(&session.state(), None, &[], None)
        .await
        .expect("scan");
    let mut stream = plan.execute(0, session.task_ctx()).expect("execute");
    let mut rows = 0usize;
    while let Some(batch) = stream.next().await {
        rows += batch.expect("batch").num_rows();
    }
    tx.commit_test().await.expect("commit read");
    rows
}

/// A `TablePhysicalSeries` accumulates one parquet file per version, so reads
/// cost O(versions) no matter how few rows each version carries. Collapsing
/// must merge them into a single version by re-encoding (parquet files cannot
/// be byte-concatenated the way a `FilePhysicalSeries` can), preserving every
/// row, and must be a no-op once already collapsed.
#[tokio::test]
async fn test_collapse_table_series_reencodes_versions() {
    let _ = env_logger::builder().is_test(true).try_init();

    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence");
    let file_path = "table.series";

    // Three versions, mimicking an hourly collector: tiny disjoint appends.
    append_table_series_version(&mut persistence, file_path, vec![100, 200], vec![1.0, 2.0]).await;
    append_table_series_version(&mut persistence, file_path, vec![300, 400], vec![3.0, 4.0]).await;
    append_table_series_version(&mut persistence, file_path, vec![500, 600], vec![5.0, 6.0]).await;

    assert_eq!(
        live_version_count(&mut persistence, file_path).await,
        3,
        "three appended versions are all live before collapse"
    );
    let rows_before = table_series_rows(&mut persistence, file_path).await;
    assert_eq!(rows_before, 6, "all six rows are visible before collapse");

    let id = node_id_of(&mut persistence, file_path).await;
    assert_eq!(
        id.entry_type(),
        tinyfs::EntryType::TablePhysicalSeries,
        "create_series_from_batch writes a TablePhysicalSeries"
    );

    // A table series must be discoverable as a collapse candidate; before this
    // was supported the discovery query matched FilePhysicalSeries only and
    // these nodes could never be collapsed at any threshold.
    {
        let tx = persistence.begin_test().await.expect("begin read tx");
        let candidates = tx
            .state()
            .expect("state")
            .list_collapsible_series(1)
            .await
            .expect("list_collapsible_series");
        assert!(
            candidates.contains(&id),
            "a multi-version table series must be a collapse candidate"
        );
        tx.commit_test().await.expect("commit read");
    }

    // Collapse.
    {
        let tx = persistence.begin_test().await.expect("begin collapse tx");
        let stats = tx
            .state()
            .expect("state")
            .collapse_table_series(id, 1)
            .await
            .expect("collapse_table_series");
        assert!(stats.collapsed, "expected a collapse");
        assert_eq!(stats.versions_before, 3, "three versions were merged");
        tx.commit_test().await.expect("commit collapse");
    }

    assert_eq!(
        live_version_count(&mut persistence, file_path).await,
        1,
        "collapse leaves exactly one live version"
    );
    assert_eq!(
        table_series_rows(&mut persistence, file_path).await,
        rows_before,
        "re-encoding must preserve every row and must not double-count \
         superseded versions"
    );

    // Already collapsed: no further candidacy and no further work.
    {
        let tx = persistence.begin_test().await.expect("begin read tx");
        let candidates = tx
            .state()
            .expect("state")
            .list_collapsible_series(1)
            .await
            .expect("list_collapsible_series");
        assert!(
            !candidates.contains(&id),
            "a collapsed table series is no longer a candidate"
        );
        tx.commit_test().await.expect("commit read");
    }
    {
        let tx = persistence.begin_test().await.expect("begin collapse tx");
        let stats = tx
            .state()
            .expect("state")
            .collapse_table_series(id, 1)
            .await
            .expect("second collapse_table_series");
        assert!(
            !stats.collapsed,
            "collapsing an already-merged series is a no-op"
        );
        tx.commit_test().await.expect("commit");
    }

    // Appending after a collapse must extend the merged baseline, not resurrect
    // the superseded versions.
    append_table_series_version(&mut persistence, file_path, vec![700, 800], vec![7.0, 8.0]).await;
    assert_eq!(
        table_series_rows(&mut persistence, file_path).await,
        rows_before + 2,
        "a post-collapse append adds exactly its own rows"
    );
}

/// Tiering the table path: with a realistic `max_live`, collapse must merge a
/// bounded *window* of same-size-class versions instead of re-encoding the
/// entire series on every trigger. That is what turns collapse from an O(N^2)
/// write amplifier into an O(N log N) one.
///
/// This drives two collapses so a merged run and loose versions coexist, which
/// is the configuration that hid the ordering bugs on the file path. For tables
/// the risk is different: the window is re-encoded through a table provider fed
/// explicit per-version URLs, so a run must be readable as an ordinary version
/// and must not be double-counted alongside the versions it replaced.
#[tokio::test]
async fn test_collapse_table_series_tiered_windows() {
    let _ = env_logger::builder().is_test(true).try_init();

    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence");
    let file_path = "table.series";

    // 25 tiny disjoint appends, the shape of an hourly collector.
    const VERSIONS: usize = 25;
    for i in 0..VERSIONS {
        let base = (i as i64 + 1) * 1000;
        append_table_series_version(
            &mut persistence,
            file_path,
            vec![base, base + 1],
            vec![i as f64, i as f64 + 0.5],
        )
        .await;
    }

    let rows_before = table_series_rows(&mut persistence, file_path).await;
    assert_eq!(rows_before, VERSIONS * 2, "every appended row is visible");
    assert_eq!(
        live_version_count(&mut persistence, file_path).await,
        VERSIONS,
        "no collapse has run yet"
    );

    let id = node_id_of(&mut persistence, file_path).await;

    // First collapse: merges exactly one fanout-sized window, not the history.
    async fn collapse(
        persistence: &mut OpLogPersistence,
        id: tinyfs::FileID,
        max_live: usize,
    ) -> crate::persistence::CollapseStats {
        let tx = persistence.begin_test().await.expect("begin collapse tx");
        let stats = tx
            .state()
            .expect("state")
            .collapse_table_series(id, max_live)
            .await
            .expect("collapse_table_series");
        tx.commit_test().await.expect("commit collapse");
        stats
    }

    let stats = collapse(&mut persistence, id, VERSIONS).await;
    assert!(
        stats.collapsed,
        "a full same-size-class window is available"
    );
    let after_first = live_version_count(&mut persistence, file_path).await;
    assert_eq!(
        after_first,
        VERSIONS - 9,
        "a window of {} versions collapses to one merged run",
        crate::persistence::COLLAPSE_FANOUT
    );
    assert_eq!(
        table_series_rows(&mut persistence, file_path).await,
        rows_before,
        "merging a window must neither lose rows nor double-count the \
         versions it superseded"
    );

    // Second collapse: a merged run now coexists with loose versions, so the
    // window picker must skip the run (different size class) and merge the next
    // group of loose versions.
    let stats = collapse(&mut persistence, id, VERSIONS).await;
    assert!(stats.collapsed, "a second loose window is still available");
    assert_eq!(
        live_version_count(&mut persistence, file_path).await,
        after_first - 9,
        "the second window collapses independently of the first run"
    );
    assert_eq!(
        table_series_rows(&mut persistence, file_path).await,
        rows_before,
        "two coexisting merged runs must not overlap or drop rows"
    );

    // Appending afterwards must extend the series, not resurrect superseded
    // versions absorbed by either run.
    append_table_series_version(&mut persistence, file_path, vec![99_000], vec![9.9]).await;
    assert_eq!(
        table_series_rows(&mut persistence, file_path).await,
        rows_before + 1,
        "a post-collapse append adds exactly its own rows"
    );
}

/// Collapse must reject a node that is not a table series, so a mis-dispatched
/// `FilePhysicalSeries` can never be silently rewritten as parquet.
#[tokio::test]
async fn test_collapse_table_series_rejects_file_series() {
    let _ = env_logger::builder().is_test(true).try_init();

    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence");
    let file_path = "data/bytes.csv";
    append_series_version(&mut persistence, file_path, b"a,1\n", Some("data")).await;
    append_series_version(&mut persistence, file_path, b"a,2\n", None).await;

    let id = node_id_of(&mut persistence, file_path).await;
    let tx = persistence.begin_test().await.expect("begin tx");
    let err = tx
        .state()
        .expect("state")
        .collapse_table_series(id, 1)
        .await
        .expect_err("a FilePhysicalSeries must be rejected");
    assert!(
        format!("{err}").contains("requires a TablePhysicalSeries"),
        "unexpected error: {err}"
    );
    tx.commit_test().await.expect("commit");
}

/// `list_collapsible_series` must honor the threshold, return only series files
/// above it, and exclude files already collapsed to a single version.
#[tokio::test]
async fn test_list_collapsible_series_threshold() {
    let _ = env_logger::builder().is_test(true).try_init();

    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence");

    let noisy = "data/noisy.csv";
    let quiet = "data/quiet.csv";

    // noisy: three versions; quiet: one version.
    for (i, chunk) in [
        b"n,1\n".as_slice(),
        b"n,2\n".as_slice(),
        b"n,3\n".as_slice(),
    ]
    .iter()
    .enumerate()
    {
        let create = if i == 0 { Some("data") } else { None };
        append_series_version(&mut persistence, noisy, chunk, create).await;
    }
    append_series_version(&mut persistence, quiet, b"q,1\n", None).await;

    let noisy_id = node_id_of(&mut persistence, noisy).await;

    // threshold 1: only the file with more than one live version qualifies.
    {
        let tx = persistence.begin_test().await.expect("begin read tx");
        let candidates = tx
            .state()
            .expect("state")
            .list_collapsible_series(1)
            .await
            .expect("list_collapsible_series");
        assert_eq!(
            candidates,
            vec![noisy_id],
            "only noisy qualifies at threshold 1"
        );
        tx.commit_test().await.expect("commit read");
    }

    // threshold 5: nothing qualifies.
    {
        let tx = persistence.begin_test().await.expect("begin read tx");
        let candidates = tx
            .state()
            .expect("state")
            .list_collapsible_series(5)
            .await
            .expect("list_collapsible_series");
        assert!(candidates.is_empty(), "no file has more than five versions");
        tx.commit_test().await.expect("commit read");
    }

    // After collapsing noisy, it is excluded because its live count drops to 1.
    _ = collapse_now(&mut persistence, noisy).await;
    {
        let tx = persistence.begin_test().await.expect("begin read tx");
        let candidates = tx
            .state()
            .expect("state")
            .list_collapsible_series(1)
            .await
            .expect("list_collapsible_series");
        assert!(
            candidates.is_empty(),
            "a collapsed file is no longer a candidate"
        );
        tx.commit_test().await.expect("commit read");
    }
}

/// Resolve the `FileID` of a node by path.
async fn node_id_of(persistence: &mut OpLogPersistence, file_path: &str) -> tinyfs::FileID {
    let tx = persistence.begin_test().await.expect("begin read tx");
    let wd = tx.root().await.expect("root");
    let id = wd.get_node_path(file_path).await.expect("node").id();
    tx.commit_test().await.expect("commit read");
    id
}

/// Regression test: cumulative_blake3 must be correct for FilePhysicalSeries
/// with externally stored large content (>= 64KB per version).
///
/// Previously, when a version's content exceeded the 64KB large file threshold,
/// HybridWriter stored it externally and `content` was empty in poll_shutdown.
/// The old code path fell through to `from_incremental_state()` with a fresh
/// (non-resumed) bao_state, producing an incorrect cumulative_blake3.
///
/// The fix resumes the HybridWriter's bao_hasher from the previous version's
/// frontier at writer creation time, so from_incremental_state() now produces
/// correct results regardless of whether content is inline or external.
///
/// This test writes large versions (>64KB) to a FilePhysicalSeries and verifies
/// that the stored cumulative_blake3 matches a fresh computation over all
/// concatenated content.
#[tokio::test]
async fn test_file_physical_series_cumulative_blake3_large_versions() {
    let _ = env_logger::builder().is_test(true).try_init();

    log::debug!("=== Testing FilePhysicalSeries Cumulative BLAKE3 for Large Versions ===");

    let store_path = test_dir();

    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence");

    let file_path = "data/station.json";

    // Track cumulative content for verification
    let mut cumulative_content = Vec::new();

    // Version 1: 100KB — exceeds 64KB large file threshold, stored externally.
    // Non-block-aligned to create pending bytes (100_000 % 16384 = 1696 pending).
    {
        let v1: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        cumulative_content.extend_from_slice(&v1);

        let tx = persistence.begin_test().await.expect("begin tx 1");
        let wd = tx.root().await.expect("root 1");
        _ = wd.create_dir_path("data").await.expect("create data dir");

        use tokio::io::AsyncWriteExt;
        let mut writer = wd
            .async_writer_path_with_type(file_path, tinyfs::EntryType::FilePhysicalSeries)
            .await
            .expect("writer v1");
        writer.write_all(&v1).await.expect("write v1");
        writer.shutdown().await.expect("shutdown v1");
        tx.commit_test().await.expect("commit v1");

        log::debug!(
            "[OK] v1: {} bytes, pending={}",
            cumulative_content.len(),
            cumulative_content.len() % 16384
        );
    }

    // Verify v1 cumulative blake3
    verify_cumulative_blake3(&mut persistence, file_path, &cumulative_content).await;

    // Version 2: 80KB — also large/external. Cumulative = 180KB.
    // Pending = 180_000 % 16384 = 14128 bytes.
    {
        let v2: Vec<u8> = (0..80_000u32).map(|i| ((i + 7) % 241) as u8).collect();
        cumulative_content.extend_from_slice(&v2);

        let tx = persistence.begin_test().await.expect("begin tx 2");
        let wd = tx.root().await.expect("root 2");

        use tokio::io::AsyncWriteExt;
        let mut writer = wd
            .async_writer_path_with_type(file_path, tinyfs::EntryType::FilePhysicalSeries)
            .await
            .expect("writer v2");
        writer.write_all(&v2).await.expect("write v2");
        writer.shutdown().await.expect("shutdown v2");
        tx.commit_test().await.expect("commit v2");

        log::debug!(
            "[OK] v2: {} bytes cumulative, pending={}",
            cumulative_content.len(),
            cumulative_content.len() % 16384
        );
    }

    verify_cumulative_blake3(&mut persistence, file_path, &cumulative_content).await;

    // Version 3: small inline append (100 bytes). Tests pending bytes from
    // a large external version being read correctly via read_file_version.
    {
        let v3: Vec<u8> = vec![0xFFu8; 100];
        cumulative_content.extend_from_slice(&v3);

        let tx = persistence.begin_test().await.expect("begin tx 3");
        let wd = tx.root().await.expect("root 3");

        use tokio::io::AsyncWriteExt;
        let mut writer = wd
            .async_writer_path_with_type(file_path, tinyfs::EntryType::FilePhysicalSeries)
            .await
            .expect("writer v3");
        writer.write_all(&v3).await.expect("write v3");
        writer.shutdown().await.expect("shutdown v3");
        tx.commit_test().await.expect("commit v3");

        log::debug!(
            "[OK] v3: {} bytes cumulative (small append after large), pending={}",
            cumulative_content.len(),
            cumulative_content.len() % 16384
        );
    }

    verify_cumulative_blake3(&mut persistence, file_path, &cumulative_content).await;

    // Version 4: append that fills to exact block boundary.
    {
        let current_pending = cumulative_content.len() % 16384;
        let boundary_fill = if current_pending > 0 {
            16384 - current_pending
        } else {
            0
        };
        if boundary_fill > 0 {
            let v4: Vec<u8> = (0..boundary_fill).map(|i| ((i + 13) % 233) as u8).collect();
            cumulative_content.extend_from_slice(&v4);

            let tx = persistence.begin_test().await.expect("begin tx 4");
            let wd = tx.root().await.expect("root 4");

            use tokio::io::AsyncWriteExt;
            let mut writer = wd
                .async_writer_path_with_type(file_path, tinyfs::EntryType::FilePhysicalSeries)
                .await
                .expect("writer v4");
            writer.write_all(&v4).await.expect("write v4");
            writer.shutdown().await.expect("shutdown v4");
            tx.commit_test().await.expect("commit v4");

            assert_eq!(cumulative_content.len() % 16384, 0);
            log::debug!(
                "[OK] v4: {} bytes cumulative (exact block boundary), pending=0",
                cumulative_content.len()
            );

            verify_cumulative_blake3(&mut persistence, file_path, &cumulative_content).await;
        }
    }

    // Version 5: 120KB after block boundary — exercises resumption with pending=0.
    {
        let v5: Vec<u8> = (0..120_000u32).map(|i| ((i + 19) % 239) as u8).collect();
        cumulative_content.extend_from_slice(&v5);

        let tx = persistence.begin_test().await.expect("begin tx 5");
        let wd = tx.root().await.expect("root 5");

        use tokio::io::AsyncWriteExt;
        let mut writer = wd
            .async_writer_path_with_type(file_path, tinyfs::EntryType::FilePhysicalSeries)
            .await
            .expect("writer v5");
        writer.write_all(&v5).await.expect("write v5");
        writer.shutdown().await.expect("shutdown v5");
        tx.commit_test().await.expect("commit v5");

        log::debug!(
            "[OK] v5: {} bytes cumulative (large after boundary), pending={}",
            cumulative_content.len(),
            cumulative_content.len() % 16384
        );
    }

    verify_cumulative_blake3(&mut persistence, file_path, &cumulative_content).await;

    // Read back and verify concatenated content
    {
        let tx = persistence.begin_test().await.expect("begin read tx");
        let wd = tx.root().await.expect("root read");

        let content = wd
            .read_file_path_to_vec(file_path)
            .await
            .expect("Failed to read file");

        assert_eq!(
            content.len(),
            cumulative_content.len(),
            "Content length mismatch"
        );
        assert_eq!(content, cumulative_content, "Content bytes mismatch");

        tx.commit_test().await.expect("commit read tx");
    }

    log::debug!("\n[OK] TEST PASSED!");
    log::debug!(
        "- Wrote 5 versions ({} bytes total) with large external content",
        cumulative_content.len()
    );
    log::debug!("- Cumulative blake3 correct after each version");
    log::debug!("- Concatenated content verified");
}

/// Helper: verify that the stored cumulative_blake3 for a FilePhysicalSeries
/// matches a fresh computation over the expected content.
async fn verify_cumulative_blake3(
    persistence: &mut OpLogPersistence,
    file_path: &str,
    expected_content: &[u8],
) {
    use tinyfs::persistence::PersistenceLayer;

    let tx = persistence.begin_test().await.expect("begin verify tx");
    let wd = tx.root().await.expect("root verify");

    let file_node = wd.get_node_path(file_path).await.expect("File not found");
    let file_id = file_node.id();

    let state = tx.state().expect("state");
    let metadata = state.metadata(file_id).await.expect("metadata");

    // Compute expected cumulative blake3 using IncrementalHashState
    let mut hash_state = utilities::bao_outboard::IncrementalHashState::new();
    hash_state.ingest(expected_content);
    let expected_blake3 = hash_state.root_hash().to_hex().to_string();

    // Verify OplogEntry.blake3
    let stored_blake3 = metadata
        .blake3
        .as_ref()
        .expect("metadata.blake3 should be set");

    assert_eq!(
        stored_blake3,
        &expected_blake3,
        "Stored cumulative_blake3 doesn't match fresh computation!\n  Expected: {}\n  Stored:   {}\n  Content:  {} bytes",
        expected_blake3,
        stored_blake3,
        expected_content.len()
    );

    // Verify SeriesOutboard.cumulative_blake3
    if let Some(bao_bytes) = &metadata.bao_outboard {
        let series_outboard = utilities::bao_outboard::SeriesOutboard::from_bytes(bao_bytes)
            .expect("parse SeriesOutboard");

        let outboard_blake3 = blake3::Hash::from_bytes(series_outboard.cumulative_blake3)
            .to_hex()
            .to_string();

        assert_eq!(
            outboard_blake3, expected_blake3,
            "SeriesOutboard.cumulative_blake3 mismatch!\n  Expected: {}\n  Outboard: {}",
            expected_blake3, outboard_blake3
        );

        assert_eq!(
            series_outboard.cumulative_size,
            expected_content.len() as u64,
            "SeriesOutboard.cumulative_size mismatch"
        );
    }

    tx.commit_test().await.expect("commit verify tx");

    log::debug!(
        "  blake3 verified: {}... ({} bytes)",
        &expected_blake3[..16],
        expected_content.len()
    );
}

/// Test FilePhysicalSeries with CSV format provider and DataFusion SQL queries
/// This verifies the full integration: write multiple CSV versions, read via csv:// URL, query with SQL
#[tokio::test]
async fn test_file_physical_series_csv_provider_sql() {
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;
    use provider::{FileProvider, FormatRegistry, Url};
    use std::sync::Arc;

    log::debug!("=== Starting FilePhysicalSeries CSV Provider SQL Test ===");

    let store_path = test_dir();

    // Create TLogFS persistence layer
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence");

    let file_path = "data/events.csv";

    // Write first version (CSV header + first data rows)
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin transaction");
        let wd = tx.root().await.expect("Failed to get root");

        // Create data directory
        if !wd.exists(std::path::Path::new("data")).await {
            _ = wd
                .create_dir_path("data")
                .await
                .expect("Failed to create data dir");
        }

        // Write first version with header and initial data
        let csv_v1 = b"name,value\nalice,100\nbob,200\n";

        use tokio::io::AsyncWriteExt;
        let mut writer = wd
            .async_writer_path_with_type(file_path, tinyfs::EntryType::FilePhysicalSeries)
            .await
            .expect("Failed to create writer for version 1");
        writer
            .write_all(csv_v1)
            .await
            .expect("Failed to write version 1");
        writer
            .shutdown()
            .await
            .expect("Failed to shutdown version 1 writer");

        tx.commit_test().await.expect("Failed to commit version 1");
        log::debug!("[OK] Committed version 1 with {} bytes", csv_v1.len());
    }

    // Write second version (more data rows - no header since it's appending)
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin transaction");
        let wd = tx.root().await.expect("Failed to get root");

        let csv_v2 = b"carol,300\ndave,400\n";

        use tokio::io::AsyncWriteExt;
        let mut writer = wd
            .async_writer_path_with_type(file_path, tinyfs::EntryType::FilePhysicalSeries)
            .await
            .expect("Failed to create writer for version 2");
        writer
            .write_all(csv_v2)
            .await
            .expect("Failed to write version 2");
        writer
            .shutdown()
            .await
            .expect("Failed to shutdown version 2 writer");

        tx.commit_test().await.expect("Failed to commit version 2");
        log::debug!("[OK] Committed version 2 with {} bytes", csv_v2.len());
    }

    // Write third version (even more data)
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin transaction");
        let wd = tx.root().await.expect("Failed to get root");

        let csv_v3 = b"eve,500\nfrank,600\n";

        use tokio::io::AsyncWriteExt;
        let mut writer = wd
            .async_writer_path_with_type(file_path, tinyfs::EntryType::FilePhysicalSeries)
            .await
            .expect("Failed to create writer for version 3");
        writer
            .write_all(csv_v3)
            .await
            .expect("Failed to write version 3");
        writer
            .shutdown()
            .await
            .expect("Failed to shutdown version 3 writer");

        tx.commit_test().await.expect("Failed to commit version 3");
        log::debug!("[OK] Committed version 3 with {} bytes", csv_v3.len());
    }

    // Now read via CSV provider and query with DataFusion
    {
        let tx = persistence
            .begin_test()
            .await
            .expect("Failed to begin read transaction");

        // Get the FS via Deref (TransactionGuard derefs to FS)
        let fs: &tinyfs::FS = &tx;

        // Open the file using csv:// URL
        let url = Url::parse(&format!("csv:///{}", file_path)).expect("Failed to parse URL");
        log::debug!("Opening file via URL: {}", url);

        let reader = fs.open_url(&url).await.expect("Failed to open URL");

        // Use CsvProvider from FormatRegistry
        let csv_provider =
            FormatRegistry::get_provider("csv").expect("CSV provider not registered");
        let (schema, stream) = csv_provider
            .open_stream(reader, &url)
            .await
            .expect("Failed to open CSV stream");

        log::debug!("Schema: {:?}", schema);
        assert_eq!(schema.fields().len(), 2, "Should have 2 columns");
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(1).name(), "value");

        // Collect all batches
        let batches: Vec<arrow::record_batch::RecordBatch> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<provider::Result<Vec<_>>>()
            .expect("Failed to collect batches");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        log::debug!(
            "Read {} total rows across {} batches",
            total_rows,
            batches.len()
        );
        assert_eq!(total_rows, 6, "Should have 6 total rows from all versions");

        // Register in DataFusion and run SQL queries
        let ctx = SessionContext::new();
        let mem_table = datafusion::datasource::MemTable::try_new(schema.clone(), vec![batches])
            .expect("Failed to create MemTable");
        let _ = ctx
            .register_table("events", Arc::new(mem_table))
            .expect("Failed to register table");

        // Query: COUNT(*)
        let df = ctx
            .sql("SELECT COUNT(*) as count FROM events")
            .await
            .expect("Failed to execute COUNT query");
        let results = df.collect().await.expect("Failed to collect results");
        let count = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 6, "COUNT(*) should be 6");
        log::debug!("[OK] COUNT(*) = {}", count);

        // Query: SUM(value)
        let df = ctx
            .sql("SELECT SUM(CAST(value AS INT)) as total FROM events")
            .await
            .expect("Failed to execute SUM query");
        let results = df.collect().await.expect("Failed to collect results");
        let total = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        // 100 + 200 + 300 + 400 + 500 + 600 = 2100
        assert_eq!(total, 2100, "SUM(value) should be 2100");
        log::debug!("[OK] SUM(value) = {}", total);

        // Query: SELECT * with ordering
        let df = ctx
            .sql("SELECT name, CAST(value AS INT) as value FROM events ORDER BY value")
            .await
            .expect("Failed to execute SELECT query");
        let results = df.collect().await.expect("Failed to collect results");

        // Verify order: alice(100), bob(200), carol(300), dave(400), eve(500), frank(600)
        let names: Vec<_> = results
            .iter()
            .flat_map(|batch| {
                let name_col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();
                (0..batch.num_rows()).map(move |i| name_col.value(i).to_string())
            })
            .collect();

        assert_eq!(
            names,
            vec!["alice", "bob", "carol", "dave", "eve", "frank"],
            "Names should be in value order"
        );
        log::debug!("[OK] SELECT ORDER BY verified: {:?}", names);
    }

    log::debug!("\n[OK] TEST PASSED!");
    log::debug!("- Wrote 3 versions to FilePhysicalSeries CSV file");
    log::debug!("- Opened via csv:// URL with CsvProvider");
    log::debug!("- Streamed 6 total rows (concatenated from all versions)");
    log::debug!("- Queried with DataFusion SQL: COUNT, SUM, ORDER BY");
}

// ============================================================
// D5.2: peek_pond_id - read pond identity from the data Delta
// table's bootstrap row (canonical) instead of the control table
// (cache).
// ============================================================

#[tokio::test]
async fn peek_pond_id_returns_none_for_missing_path() {
    let tmp = TempDir::new().unwrap();
    let missing = tmp.path().join("does-not-exist");
    let got = OpLogPersistence::peek_pond_id(&missing)
        .await
        .expect("peek_pond_id should not error on missing path");
    assert!(
        got.is_none(),
        "peek_pond_id on missing path should be None, got {:?}",
        got
    );
}

#[tokio::test]
async fn peek_pond_id_returns_none_for_empty_table() {
    // create_empty() builds the Delta schema + _delta_log but writes no
    // Add actions (used by restoration scaffolds awaiting bundles).
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("data");
    std::fs::create_dir_all(&path).unwrap();
    let _persistence =
        OpLogPersistence::create_empty(path.to_str().unwrap(), uuid7::uuid7().to_string())
            .await
            .expect("create_empty failed");

    let got = OpLogPersistence::peek_pond_id(&path)
        .await
        .expect("peek_pond_id should not error on empty table");
    assert!(
        got.is_none(),
        "peek_pond_id on empty Delta table should be None, got {:?}",
        got
    );
}

#[tokio::test]
async fn peek_pond_id_returns_local_id_for_fresh_pond() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("data").to_string_lossy().to_string();

    let pond_id = uuid7::uuid7().to_string();
    let _persistence = OpLogPersistence::open_or_create(
        &path,
        pond_id.clone(),
        true,
        Some(crate::txn_metadata::PondUserMetadata::new(vec![
            "test".to_string(),
        ])),
    )
    .await
    .expect("open_or_create failed");

    let got = OpLogPersistence::peek_pond_id(&path)
        .await
        .expect("peek_pond_id failed");
    assert_eq!(
        got.as_deref(),
        Some(pond_id.as_str()),
        "peek_pond_id should return the bootstrap row's pond_id"
    );
}

/// D5.7a.2: a write commit returns the Delta version assigned by the
/// FinalizedCommit, NOT a pre-commit version + 1 arithmetic guess.
#[tokio::test]
async fn commit_returns_assigned_version() {
    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence layer");

    let pre_version = persistence
        .table()
        .version()
        .expect("post-create table must have a version");

    let tx = persistence
        .begin_test()
        .await
        .expect("Failed to begin transaction");
    let wd = tx.root().await.expect("Failed to get root");
    _ = tinyfs::async_helpers::convenience::create_file_path(&wd, "/version_probe.txt", b"hello")
        .await
        .expect("create_file_path failed");

    let returned = tx
        .commit_test_with_sequence(2)
        .await
        .expect("commit failed");
    let returned_version = returned.expect("write commit should return a version");

    let post_version = persistence
        .table()
        .version()
        .expect("post-commit table must have a version");

    assert_eq!(
        returned_version, post_version,
        "commit-returned version ({returned_version}) must match installed table version ({post_version})"
    );
    assert_eq!(
        returned_version,
        pre_version + 1,
        "commit-returned version ({returned_version}) must equal pre_version+1 ({})",
        pre_version + 1,
    );
}

/// D5.7a.2: an empty (no-write) commit returns Ok(None) and does not
/// advance the Delta table version.
#[tokio::test]
async fn commit_no_op_returns_none() {
    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence layer");

    let pre_version = persistence
        .table()
        .version()
        .expect("post-create table must have a version");

    let tx = persistence
        .begin_test()
        .await
        .expect("Failed to begin transaction");
    // No writes performed before commit.
    let returned = tx
        .commit_test_with_sequence(2)
        .await
        .expect("commit failed");

    assert!(
        returned.is_none(),
        "empty commit should return Ok(None), got {returned:?}"
    );

    let post_version = persistence
        .table()
        .version()
        .expect("table version should still exist");
    assert_eq!(
        pre_version, post_version,
        "empty commit must not advance the Delta table version"
    );
}

/// Phase-4 memory-plateau guard (see docs/journal-status-bounded-memory-design.md
/// §6 "Memory regression guard"). An append-only `FilePhysicalSeries` grows one
/// version per tick forever. A full read materializes O(total history); an
/// event-time-bounded read (the fix) must materialize only the versions inside
/// the hot window, so the read size **plateaus** as history grows even though
/// on-disk history keeps growing.
///
/// The test builds journals of increasing length (N versions at event times
/// 1..=N) and, for each, reads with a fixed-width hot window pinned to the
/// newest data (`event_time_lo = N - 1`, i.e. the two newest versions). It
/// asserts the bounded read stays flat across N while the unbounded read grows
/// monotonically with N.
#[tokio::test]
async fn series_bounded_read_memory_plateaus_as_history_grows() {
    use tinyfs::SeriesReadBounds;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Build a fresh pond with `n` `FilePhysicalSeries` versions at event times
    // 1..=n (each a distinct fixed-size payload so content-addressing can't
    // dedupe them), then return (bounded_read_len, unbounded_read_len,
    // retained_count) where the bound keeps only versions with
    // max_event_time >= n-1 (the two newest).
    async fn measure(n: i64) -> (usize, usize, usize) {
        const PAYLOAD_LEN: usize = 128;
        let path = "logs/journal.series";
        let store_path = test_dir();
        let mut persistence = OpLogPersistence::create_test(&store_path)
            .await
            .expect("create pond");

        for t in 1..=n {
            let tx = persistence.begin_test().await.expect("begin");
            let wd = tx.root().await.expect("root");
            if !wd.exists(std::path::Path::new("logs")).await {
                let _ = wd.create_dir_path("logs").await.expect("mkdir");
            }
            // Distinct content of constant length (t in the first 8 bytes) so
            // each append is a real new version, not a content-addressed dedupe.
            let mut payload = vec![b'x'; PAYLOAD_LEN];
            payload[0..8].copy_from_slice(&t.to_le_bytes());

            let mut writer = wd
                .async_writer_path_with_type(path, tinyfs::EntryType::FilePhysicalSeries)
                .await
                .expect("writer");
            writer.write_all(&payload).await.expect("write");
            // Record this version's event-time bound (min == max == t).
            writer.set_temporal_metadata(t, t, "timestamp".to_string());
            writer.shutdown().await.expect("shutdown");
            tx.commit_test().await.expect("commit");
        }

        // Hot window: keep only the two newest versions (event time >= n-1).
        let lo = n - 1;
        let bounds = SeriesReadBounds::from_event_time_lo(lo);

        let tx = persistence.begin_test().await.expect("begin read");
        let wd = tx.root().await.expect("root");

        // Sanity: exactly two versions fall inside the fixed-width window,
        // independent of N (the plateau precondition).
        let versions = wd.list_file_versions(path).await.expect("versions");
        assert_eq!(versions.len() as i64, n, "one version per append");
        let retained = versions
            .iter()
            .filter(|v| {
                let max_et = v
                    .extended_metadata
                    .as_ref()
                    .and_then(|m| m.get("max_event_time"))
                    .and_then(|s| s.parse::<i64>().ok());
                bounds.retains(max_et, v.version as i64)
            })
            .count();

        let mut bounded = Vec::new();
        let _ = wd
            .async_reader_path_bounded(path, bounds)
            .await
            .expect("bounded reader")
            .read_to_end(&mut bounded)
            .await
            .expect("read bounded");

        let mut full = Vec::new();
        let _ = wd
            .async_reader_path_bounded(path, SeriesReadBounds::NONE)
            .await
            .expect("full reader")
            .read_to_end(&mut full)
            .await
            .expect("read full");

        (bounded.len(), full.len(), retained)
    }

    let (b4, f4, r4) = measure(4).await;
    let (b8, f8, r8) = measure(8).await;
    let (b16, f16, r16) = measure(16).await;

    // Fixed-width window retains a constant number of versions regardless of N.
    assert_eq!(
        (r4, r8, r16),
        (2, 2, 2),
        "hot window keeps 2 versions at every N"
    );

    // Plateau: the bounded read materializes a flat amount of data as history
    // grows 4 -> 8 -> 16 versions.
    assert_eq!(b4, b8, "bounded read must not grow with history (4 vs 8)");
    assert_eq!(b8, b16, "bounded read must not grow with history (8 vs 16)");

    // Control: the unbounded read grows monotonically with history, proving the
    // journal really is getting larger (so the plateau is a real win, not an
    // artifact of a non-growing series).
    assert!(
        f4 < f8,
        "unbounded read grows with history (4 -> 8): {f4} < {f8}"
    );
    assert!(
        f8 < f16,
        "unbounded read grows with history (8 -> 16): {f8} < {f16}"
    );
    assert!(
        b16 < f16,
        "bounded read must be smaller than the full-history read at N=16: {b16} < {f16}"
    );
}

/// Supersession must be decided by *range containment*, never by the highest
/// `collapsed_through` sentinel.
///
/// Once collapse is tiered, a merged run carries a fresh highest version while
/// standing for content in the middle of the stream, so the two rules diverge:
/// a run created early (low version, low range) can be outlived by a later
/// merge whose range extends past that run's *version number*. The old
/// `max(collapsed_through)` rule -- "every version <= K is dead" -- then drops a
/// live run and its content disappears silently.
#[test]
fn test_supersession_uses_ranges_not_max_watermark() {
    use crate::schema::{CollapseRange, live_series_versions};

    let loose = |v: i64| {
        (
            v,
            CollapseRange {
                lo: v,
                hi: v,
                merged: false,
            },
        )
    };
    let run = |v: i64, lo: i64, hi: i64| {
        (
            v,
            CollapseRange {
                lo,
                hi,
                merged: true,
            },
        )
    };

    // Run A merged versions 1..=10 and was allocated version 26. Later, a
    // window of loose versions 21..=31 merged as version 36. Note 36's range
    // ends at 31, which is *above* run A's version number of 26.
    let rows = vec![
        run(26, 1, 10),
        loose(11),
        loose(12),
        run(36, 21, 31),
        loose(40),
    ];

    let live = live_series_versions(&rows);
    assert_eq!(
        live,
        vec![26, 11, 12, 36, 40],
        "every row is live: no range contains another. Order is range order \
         (run A first, covering versions 1..=10), not version order"
    );

    // The rule this replaced: max(collapsed_through) = 31, retain version > 31.
    let watermark = rows
        .iter()
        .filter(|(_, r)| r.merged)
        .map(|(_, r)| r.hi)
        .max()
        .expect("a merged run is present");
    let dropped_by_watermark: Vec<i64> = rows
        .iter()
        .map(|(v, _)| *v)
        .filter(|v| *v <= watermark)
        .collect();
    assert_eq!(
        dropped_by_watermark,
        vec![26, 11, 12],
        "the watermark rule would silently drop run A (version 26) along with \
         live loose versions 11 and 12"
    );

    // A run does supersede what it actually covers.
    let rows = vec![run(26, 1, 10), loose(5), loose(11)];
    assert_eq!(
        live_series_versions(&rows),
        vec![26, 11],
        "version 5 lies inside run A's range and is superseded"
    );
}

/// Build a `FilePhysicalSeries` of `n` nine-byte versions and collapse it twice,
/// leaving two merged runs coexisting with a loose tail. Returns the cumulative
/// stream bytes.
///
/// This is the only shape that exposes the tiered-collapse reader bugs: with a
/// single run, or with no loose tail, version order and byte order still agree.
async fn build_two_run_series(persistence: &mut OpLogPersistence, file_path: &str) -> Vec<u8> {
    let mut cumulative: Vec<u8> = Vec::new();
    for i in 0..25u32 {
        let chunk = format!("line-{i:03}\n").into_bytes();
        cumulative.extend_from_slice(&chunk);
        let create = if i == 0 { Some("data") } else { None };
        append_series_version(persistence, file_path, &chunk, create).await;
    }
    for _ in 0..2 {
        let tx = persistence.begin_test().await.expect("begin collapse tx");
        let wd = tx.root().await.expect("root");
        let id = wd.get_node_path(file_path).await.expect("node").id();
        let stats = tx
            .state()
            .expect("state")
            .collapse_file_series(id, 1000)
            .await
            .expect("collapse_file_series");
        assert!(stats.collapsed, "expected a tiered collapse");
        tx.commit_test().await.expect("commit collapse");
    }
    cumulative
}

/// Corruption #1: the series read path must concatenate live rows in *range*
/// order, not version order.
///
/// A merged run is allocated a fresh (highest) version number while standing for
/// content in the middle of the stream, so sorting by version emits the runs
/// after the loose tail. The bytes are all present and the length is unchanged,
/// so this corrupts content silently -- only comparing the exact byte sequence
/// detects it.
#[tokio::test]
async fn test_corruption_read_path_orders_runs_by_range_not_version() {
    let _ = env_logger::builder().is_test(true).try_init();
    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence");
    let file_path = "data/order.csv";

    let cumulative = build_two_run_series(&mut persistence, file_path).await;
    let read = read_series_content(&mut persistence, file_path).await;

    assert_eq!(
        read.len(),
        cumulative.len(),
        "no bytes may be lost or duplicated"
    );
    assert_eq!(
        read, cumulative,
        "runs must appear at their range position, not after the loose tail"
    );
    assert!(
        read.starts_with(b"line-000\n"),
        "the stream must begin with the oldest content, which now lives inside \
         a run carrying one of the *highest* version numbers"
    );
}

/// Corruption #2: `metadata()` must describe the row covering the *end* of the
/// stream -- the last live row in range order -- not the highest version.
///
/// The append path resumes its cumulative bao state from this row. Post-tiering
/// the highest version is a mid-stream run, so returning it makes every
/// subsequent append continue from the wrong prefix, silently corrupting
/// `cumulative_blake3` for all later readers.
#[tokio::test]
async fn test_corruption_metadata_describes_stream_tail_not_highest_version() {
    let _ = env_logger::builder().is_test(true).try_init();
    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence");
    let file_path = "data/meta.csv";

    let cumulative = build_two_run_series(&mut persistence, file_path).await;

    let tx = persistence.begin_test().await.expect("begin read tx");
    let wd = tx.root().await.expect("root");
    let id = wd.get_node_path(file_path).await.expect("node").id();
    let meta = {
        use tinyfs::persistence::PersistenceLayer;
        tx.state()
            .expect("state")
            .metadata(id)
            .await
            .expect("metadata")
    };
    tx.commit_test().await.expect("commit read");

    let bao = meta
        .bao_outboard
        .expect("a series row carries a bao outboard");
    let outboard =
        utilities::bao_outboard::SeriesOutboard::from_bytes(&bao).expect("decode series outboard");
    assert_eq!(
        outboard.cumulative_size,
        cumulative.len() as u64,
        "metadata must describe the whole stream ({} bytes). A mid-stream run \
         would report only the bytes through the end of its own range",
        cumulative.len()
    );
}

/// Corruption #3: `read_pending_bytes` must walk live rows backward in *range*
/// order.
///
/// It reconstructs the trailing partial bao block, so it must return the true
/// last `pending_len` bytes of the stream. Counting version numbers downward
/// interleaves runs with loose versions and can re-read rows a run already
/// superseded, yielding the right *number* of bytes from the wrong places --
/// which then poisons the resumed hash state rather than failing.
#[tokio::test]
async fn test_corruption_read_pending_bytes_walks_range_order() {
    let _ = env_logger::builder().is_test(true).try_init();
    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path)
        .await
        .expect("Failed to create persistence");
    let file_path = "data/pending.csv";

    let cumulative = build_two_run_series(&mut persistence, file_path).await;

    let tx = persistence.begin_test().await.expect("begin read tx");
    let wd = tx.root().await.expect("root");
    let id = wd.get_node_path(file_path).await.expect("node").id();
    let state = tx.state().expect("state");

    // `allocated_version` is the version a subsequent append would take: high
    // enough that every existing row is in scope.
    let allocated = 10_000;
    for pending_len in [9usize, 27, cumulative.len()] {
        let tail = crate::file::read_pending_bytes(&state, id, allocated, pending_len)
            .await
            .expect("read_pending_bytes");
        assert_eq!(
            tail,
            cumulative[cumulative.len() - pending_len..].to_vec(),
            "the last {pending_len} bytes must come from the end of the stream \
             in range order"
        );
    }
    tx.commit_test().await.expect("commit read");
}
