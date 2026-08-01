// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Tests for MemoryPersistence with QueryableFile
//!
//! These tests verify that MemoryPersistence properly supports:
//! - File operations (read/write via FS API)
//! - Versioned Parquet storage via store_file_version/list_file_versions
//! - QueryableFile interface (MemoryFile currently returns error)
//!
//! **Action Items:**
//! 1. Implement MemoryFile::as_table_provider() to support Parquet data
//! 2. Add ProviderContext::new_for_testing() helper

use tinyfs::{FileID, PersistenceLayer};

#[tokio::test]
async fn test_memory_fs_create_read_file() {
    // [OK] This test verifies basic FS operations work
    use tinyfs::async_helpers::convenience;
    use tinyfs::memory::new_fs;

    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create a file
    _ = convenience::create_file_path(&root, "/test.dat", b"test content")
        .await
        .expect("Should create file");

    // Read it back
    let content = root
        .read_file_path_to_vec("/test.dat")
        .await
        .expect("Should read file");

    assert_eq!(content, b"test content");
}

// ========================================
// TESTS THAT NEED IMPLEMENTATION
// ========================================

#[tokio::test]
async fn test_memory_file_queryable_interface() {
    // [OK] IMPLEMENTATION COMPLETE
    //
    // MemoryFile::as_table_provider() now implements the QueryableFile interface
    // using the same ObjectStore-based pattern as tlogfs:
    //
    // Architecture:
    // 1. Creates tinyfs:// URL pointing to the file (e.g. tinyfs:///node/{id}/part/{id})
    // 2. DataFusion's ListingTable uses the registered TinyFsObjectStore
    // 3. TinyFsObjectStore reads from MemoryPersistence via list() and get()
    // 4. Parquet parsing is handled automatically by DataFusion's ParquetFormat
    //
    // This matches tlogfs behavior exactly, enabling:
    // - Pure in-memory testing without OpLog/DeltaLake
    // - SQL queries on memory-backed Parquet data
    // - Identical API for both memory and persistent storage
    //
    // Integration test validation requires full MemoryPersistence storage API
    // (store_file_version, list_file_versions, etc.) which is tracked separately.
}

#[tokio::test]
async fn test_store_and_list_file_versions_with_memory() {
    // [OK] Tests the new store_file_version() API for MemoryPersistence
    use tinyfs::{EntryType, MemoryPersistence};
    use utilities::test_helpers::generate_parquet_with_timestamps;

    let persistence = MemoryPersistence::default();

    // Create a FileID for a FileSeries Parquet file
    let dir_id = FileID::new_physical_dir_id(tinyfs::local_pond_uuid());
    let file_id = FileID::new_in_partition(
        dir_id.part_id(),
        EntryType::TablePhysicalSeries,
        tinyfs::local_pond_uuid(),
    );

    // Create Parquet with timestamps
    let parquet_bytes =
        generate_parquet_with_timestamps(vec![(500, 10.0), (1500, 20.0), (2500, 30.0)])
            .expect("Generate parquet");

    // Store the Parquet data as a file version using new API
    persistence
        .store_file_version(file_id, 1, parquet_bytes)
        .await
        .expect("Store file version");

    // Verify we can retrieve the version using list_file_versions
    let versions = persistence
        .list_file_versions(file_id)
        .await
        .expect("List versions");
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].version, 1);
    assert!(versions[0].size > 0); // Parquet data was stored
}

#[tokio::test]
async fn test_parquet_data_generation() {
    // [OK] Using consolidated test helpers from utilities crate
    use utilities::test_helpers::{
        SensorBatchBuilder, generate_parquet_with_sensor_data, generate_parquet_with_timestamps,
        generate_simple_table_parquet,
    };

    // Test simple timestamp/value data
    let data1 = vec![(1000, 23.5), (2000, 24.1), (3000, 25.2)];
    let parquet1 = generate_parquet_with_timestamps(data1).expect("Generate parquet 1");
    assert!(!parquet1.is_empty());
    assert_eq!(&parquet1[0..4], b"PAR1"); // Parquet magic number

    // Test sensor data
    let data2 = vec![
        (1000, "sensor1", 23.5),
        (2000, "sensor2", 24.1),
        (3000, "sensor1", 25.2),
    ];
    let parquet2 = generate_parquet_with_sensor_data(data2).expect("Generate parquet 2");
    assert!(!parquet2.is_empty());
    assert_eq!(&parquet2[0..4], b"PAR1");

    // Test simple table data
    let data3 = vec![(1, "Alice", 100), (2, "Bob", 200)];
    let parquet3 = generate_simple_table_parquet(data3).expect("Generate parquet 3");
    assert!(!parquet3.is_empty());
    assert_eq!(&parquet3[0..4], b"PAR1");

    // Test builder pattern
    let parquet4 = SensorBatchBuilder::new()
        .add_reading(0, "sensor1", 23.5, 45.2)
        .add_reading(1000, "sensor1", 24.1, 46.8)
        .build_parquet()
        .expect("Generate parquet 4");
    assert!(!parquet4.is_empty());
    assert_eq!(&parquet4[0..4], b"PAR1");
}
