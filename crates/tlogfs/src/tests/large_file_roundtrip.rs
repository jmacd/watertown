// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Large file round-trip tests for tlogfs
//!
//! These tests verify that large files (stored as chunked parquet) are correctly
//! reconstructed when read back, catching bugs where the raw parquet file is
//! returned instead of the reconstructed content.

use crate::large_files::LARGE_FILE_THRESHOLD;
use crate::persistence::OpLogPersistence;
use log::debug;
use tempfile::TempDir;

fn test_dir() -> (TempDir, String) {
    let temp_dir = TempDir::new().unwrap();
    let store_path = temp_dir.path().join("store").display().to_string();
    (temp_dir, store_path)
}

/// Test that large files are correctly reconstructed from chunked parquet storage
///
/// This test catches the bug where:
/// 1. Large files are stored as chunked parquet (compressed)
/// 2. list_file_versions() reports the original content size
/// 3. read_file_version() must reconstruct the original content (not return raw parquet)
///
/// The bug would manifest as:
/// - list_file_versions() says file is 100KB
/// - read_file_version() returns 60KB (compressed parquet)
/// - Size mismatch causes Parquet reader to fail with "Range exceeds file size"
#[tokio::test]
async fn test_large_file_content_reconstruction() -> Result<(), Box<dyn std::error::Error>> {
    let _ = env_logger::builder().is_test(true).try_init();

    debug!("=== Testing Large File Content Reconstruction ===");

    let (_temp_dir, store_path) = test_dir();
    let mut persistence = OpLogPersistence::create_test_uncompressed(&store_path).await?;

    // Create content that will definitely be stored as large file (> 64KB)
    // Use a pattern that's easily verifiable but somewhat compressible
    let content_size = LARGE_FILE_THRESHOLD + 10000; // ~74KB
    let mut original_content = Vec::with_capacity(content_size);

    // Pattern: repeating sequence that's recognizable but compressible
    for i in 0..content_size {
        original_content.push((i % 256) as u8);
    }

    debug!("Original content size: {} bytes", original_content.len());
    assert!(original_content.len() >= LARGE_FILE_THRESHOLD);

    // Store the file
    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;

    _ = tinyfs::async_helpers::convenience::create_file_path(
        &wd,
        "/large_test.dat",
        &original_content,
    )
    .await?;
    tx.commit_test().await?;

    debug!("[OK] Large file stored successfully");

    // Read the file back through the TinyFS API
    let tx2 = persistence.begin_test().await?;
    let wd2 = tx2.root().await?;
    let reconstructed_content = wd2.read_file_path_to_vec("/large_test.dat").await?;

    debug!(
        "Reconstructed content size: {} bytes",
        reconstructed_content.len()
    );

    // CRITICAL: The file read must return the ORIGINAL content
    assert_eq!(
        reconstructed_content.len(),
        original_content.len(),
        "read_file_path_to_vec() must reconstruct original content, not return raw parquet"
    );

    // Verify content matches byte-for-byte
    assert_eq!(
        reconstructed_content, original_content,
        "Reconstructed content must match original exactly"
    );

    debug!("[OK] Content reconstruction successful!");
    debug!("   Original: {} bytes", original_content.len());
    debug!("   Reconstructed: {} bytes", reconstructed_content.len());
    debug!("   Content verified: identical");

    tx2.commit_test().await?;

    Ok(())
}

/// Test that the size mismatch bug would be caught by DataFusion
///
/// This simulates what DataFusion does:
/// 1. Calls list() to get file sizes
/// 2. Tries to read file using get_range()
/// 3. Expects to receive exactly the size reported in list()
#[tokio::test]
async fn test_large_file_size_consistency() -> Result<(), Box<dyn std::error::Error>> {
    let _ = env_logger::builder().is_test(true).try_init();

    debug!("=== Testing Large File Size Consistency ===");

    let (_temp_dir, store_path) = test_dir();
    let mut persistence = OpLogPersistence::create_test_uncompressed(&store_path).await?;

    // Create a large file with easily verifiable content
    let content_size = LARGE_FILE_THRESHOLD + 50000; // ~114KB
    let original_content: Vec<u8> = (0..content_size).map(|i| (i % 128) as u8).collect();

    debug!("Creating test file with {} bytes", original_content.len());

    // Store the file
    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;
    _ = tinyfs::async_helpers::convenience::create_file_path(
        &wd,
        "/consistency_test.dat",
        &original_content,
    )
    .await?;
    tx.commit_test().await?;

    // Read file back through TinyFS API
    let tx2 = persistence.begin_test().await?;
    let wd2 = tx2.root().await?;

    let read_content = wd2.read_file_path_to_vec("/consistency_test.dat").await?;
    let actual_size = read_content.len();

    debug!("Original size: {}", original_content.len());
    debug!("Read back size: {}", actual_size);

    // CRITICAL: These MUST match or DataFusion gets confused
    assert_eq!(
        actual_size,
        original_content.len(),
        "Size mismatch would cause DataFusion Parquet reader to fail!\n\
         This is the bug we're testing for."
    );

    // Verify content is correct
    assert_eq!(read_content, original_content);

    debug!("[OK] Size consistency verified!");
    debug!("   Original size: {} bytes", original_content.len());
    debug!("   Read size: {} bytes", actual_size);
    debug!("   Match: [OK]");

    tx2.commit_test().await?;
    Ok(())
}

#[tokio::test]
async fn test_large_file_version_range() -> Result<(), Box<dyn std::error::Error>> {
    use tinyfs::PersistenceLayer;

    let (_temp_dir, store_path) = test_dir();
    let mut persistence = OpLogPersistence::create_test_uncompressed(&store_path).await?;
    let content_size = LARGE_FILE_THRESHOLD + 50000;
    let original_content: Vec<u8> = (0..content_size).map(|i| (i % 251) as u8).collect();

    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;
    _ = tinyfs::async_helpers::convenience::create_file_path(
        &wd,
        "/range_test.dat",
        &original_content,
    )
    .await?;
    tx.commit_test().await?;

    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;
    let id = wd.get_node_path("/range_test.dat").await?.id();
    let state = tx.state()?;
    let version = state.list_file_versions(id).await?[0].version;

    let middle = (content_size / 2) as u64;
    let middle_range = middle..middle + 257;
    let middle_bytes = state
        .read_file_version_range(id, version, middle_range.clone())
        .await?;
    assert_eq!(
        middle_bytes.as_ref(),
        &original_content[middle_range.start as usize..middle_range.end as usize]
    );

    let tail_start = content_size as u64 - 97;
    let tail = state
        .read_file_version_range(id, version, tail_start..content_size as u64 + 1000)
        .await?;
    assert_eq!(tail.as_ref(), &original_content[tail_start as usize..]);

    tx.commit_test().await?;
    Ok(())
}

/// Test large file with multiple reads (simulating DataFusion schema inference + data read)
///
/// DataFusion typically:
/// 1. Reads file footer for metadata
/// 2. Reads file again for data
/// Both reads must return consistent sizes
#[tokio::test]
async fn test_large_file_multiple_reads() -> Result<(), Box<dyn std::error::Error>> {
    let _ = env_logger::builder().is_test(true).try_init();

    debug!("=== Testing Large File Multiple Reads ===");

    let (_temp_dir, store_path) = test_dir();
    let mut persistence = OpLogPersistence::create_test_uncompressed(&store_path).await?;

    let content_size = LARGE_FILE_THRESHOLD + 20000;
    let original_content: Vec<u8> = (0..content_size).map(|i| ((i * 7) % 256) as u8).collect();

    // Store file
    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;
    _ = tinyfs::async_helpers::convenience::create_file_path(
        &wd,
        "/multi_read_test.dat",
        &original_content,
    )
    .await?;
    tx.commit_test().await?;

    // Read file multiple times through TinyFS API (simulating DataFusion's multiple passes)
    let tx2 = persistence.begin_test().await?;
    let wd2 = tx2.root().await?;

    // First read (schema inference)
    let read1 = wd2.read_file_path_to_vec("/multi_read_test.dat").await?;
    assert_eq!(read1.len(), original_content.len());
    assert_eq!(read1, original_content);

    // Second read (data read)
    let read2 = wd2.read_file_path_to_vec("/multi_read_test.dat").await?;
    assert_eq!(read2.len(), original_content.len());
    assert_eq!(read2, original_content);

    // Third read (verify consistency)
    let read3 = wd2.read_file_path_to_vec("/multi_read_test.dat").await?;
    assert_eq!(read3.len(), original_content.len());
    assert_eq!(read3, original_content);

    debug!("[OK] Multiple reads successful and consistent!");
    debug!(
        "   All {} reads returned {} bytes",
        3,
        original_content.len()
    );

    tx2.commit_test().await?;
    Ok(())
}
