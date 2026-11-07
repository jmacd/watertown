// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Logfile Ingestion Factory Tests
//!
//! The canonical logfile-ingest factory implementation is in the provider crate:
//! `provider::factory::logfile_ingest`
//!
//! This module contains integration tests that exercise the factory with
//! OpLogPersistence (the production persistence layer).

// Re-export the config type from provider for convenience in tests
pub use provider::factory::logfile_ingest::LogfileIngestConfig;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_config() {
        let config = LogfileIngestConfig {
            archived_pattern: "/var/log/test-*.json".to_string(),
            active_pattern: "/var/log/test.json".to_string(),
            pond_path: "logs/test".to_string(),
            timestamp_field: None,
            timestamp_unit: Default::default(),
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_config_empty_pond_path() {
        let config = LogfileIngestConfig {
            archived_pattern: "/var/log/test-*.json".to_string(),
            active_pattern: "/var/log/test.json".to_string(),
            pond_path: "".to_string(),
            timestamp_field: None,
            timestamp_unit: Default::default(),
        };

        assert!(config.validate().is_err());
    }
}

#[cfg(test)]
mod integration_tests {
    //! Integration tests for logfile snapshotting and incremental verification
    //!
    //! These tests simulate a growing logfile and verify that:
    //! 1. Initial snapshots capture the full file correctly
    //! 2. Appends are detected and ingested incrementally
    //! 3. Incremental checksum verification (bao-tree) works correctly
    //! 4. Content is correctly copied into the pond

    use super::LogfileIngestConfig;
    use provider::{ExecutionContext, FactoryContext};
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tinyfs::persistence::PersistenceLayer;
    use tokio::fs;

    /// Test fixture for simulating a growing logfile
    struct LogfileSimulator {
        _temp_dir: TempDir,
        log_path: PathBuf,
        content: Vec<u8>,
    }

    impl LogfileSimulator {
        /// Create a new simulator with an empty logfile
        async fn new() -> std::io::Result<Self> {
            let temp_dir = TempDir::new()?;
            let log_path = temp_dir.path().join("app.log");

            // Create empty file
            fs::write(&log_path, b"").await?;

            Ok(Self {
                _temp_dir: temp_dir,
                log_path,
                content: Vec::new(),
            })
        }

        /// Append a line to the logfile
        async fn append_line(&mut self, line: &str) -> std::io::Result<()> {
            let mut line_bytes = line.as_bytes().to_vec();
            line_bytes.push(b'\n');

            self.content.extend_from_slice(&line_bytes);

            // Append to file
            use tokio::io::AsyncWriteExt;
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(&self.log_path)
                .await?;
            file.write_all(&line_bytes).await?;
            file.flush().await?;

            Ok(())
        }

        /// Get the current size of the logfile
        fn size(&self) -> u64 {
            self.content.len() as u64
        }

        /// Get the full content for verification
        fn content(&self) -> &[u8] {
            &self.content
        }

        /// Get the path to the logfile
        fn path(&self) -> &PathBuf {
            &self.log_path
        }

        /// Compute blake3 hash of current content
        fn blake3(&self) -> String {
            blake3::hash(&self.content).to_hex().to_string()
        }
    }

    /// Test fixture for pond state
    struct PondSimulator {
        _temp_dir: TempDir,
        store_path: String,
        pond_id: String,
        /// FileID of the config node (created in new())
        config_file_id: tinyfs::FileID,
    }

    impl PondSimulator {
        /// Create a new pond with a config file for logfile-ingest factory
        async fn new(config: &LogfileIngestConfig) -> std::io::Result<Self> {
            let temp_dir = TempDir::new()?;
            let store_path = temp_dir
                .path()
                .join("pond.db")
                .to_string_lossy()
                .to_string();

            // Initialize pond persistence using tlogfs OpLogPersistence
            let mut persistence = crate::persistence::OpLogPersistence::create_test(&store_path)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            let pond_id = persistence.pond_id().to_string();

            // Create config file in the pond and associate with factory
            let config_file_id = {
                let tx = persistence
                    .begin_test()
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                let state = tx
                    .state()
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                let fs = tinyfs::FS::new(state)
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                let root = fs
                    .root()
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                // Create /system/run/ directory structure
                _ = root
                    .create_dir_path("/system")
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                _ = root
                    .create_dir_path("/system/run")
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                // Serialize config to YAML
                let config_yaml = serde_yaml::to_string(&config)
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                // Create config file with factory association
                let config_node = root
                    .create_dynamic_path(
                        "/system/run/logfile-ingest.yaml",
                        tinyfs::EntryType::FileDynamic,
                        "logfile-ingest",
                        config_yaml.as_bytes().to_vec(),
                    )
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                let file_id = config_node.id();

                tx.commit_test()
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                file_id
            };

            Ok(Self {
                _temp_dir: temp_dir,
                store_path,
                pond_id,
                config_file_id,
            })
        }

        /// Get store path
        fn store_path(&self) -> &str {
            &self.store_path
        }

        /// Get pond identity
        fn pond_id(&self) -> &str {
            &self.pond_id
        }

        /// Execute the logfile-ingest factory within a transaction
        /// This properly opens a transaction, creates the context, executes, and commits
        async fn execute_factory(&self, config: &LogfileIngestConfig) -> std::io::Result<()> {
            let mut persistence = crate::persistence::OpLogPersistence::open(
                self.store_path(),
                self.pond_id().to_string(),
            )
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

            let tx = persistence
                .begin_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            // Get State from transaction - it implements PersistenceLayer
            let state = tx
                .state()
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            // Create ProviderContext from State
            let provider_ctx = state.as_provider_context();

            // Create FactoryContext with real config FileID
            let factory_ctx = FactoryContext::new(provider_ctx, self.config_file_id);

            // Execute the factory using the provider crate's implementation
            let exec_ctx = ExecutionContext::pond_readwriter(vec![]);
            provider::factory::logfile_ingest::execute(
                serde_json::to_value(config).map_err(|e| std::io::Error::other(e.to_string()))?,
                factory_ctx,
                exec_ctx,
            )
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

            // Commit the transaction to persist changes
            tx.commit_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            Ok(())
        }

        /// Verify a file exists in the pond with expected properties
        async fn verify_file(
            &self,
            pond_path: &str,
            filename: &str,
            expected_size: u64,
            expected_blake3: &str,
            expected_content: &[u8],
        ) -> std::io::Result<()> {
            let mut persistence = crate::persistence::OpLogPersistence::open(
                self.store_path(),
                self.pond_id().to_string(),
            )
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

            let tx = persistence
                .begin_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let root = tx
                .root()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let file_path = format!("{}/{}", pond_path, filename);
            let file_node = root
                .get_node_path(&file_path)
                .await
                .map_err(|e| std::io::Error::other(format!("File not found: {}", e)))?;
            let file = file_node
                .as_file()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            // Read content
            use tokio::io::AsyncReadExt;
            let mut content = Vec::new();
            let mut reader = file
                .async_reader()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            let _ = reader.read_to_end(&mut content).await?;

            tx.commit_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            // Verify size
            assert_eq!(
                content.len() as u64,
                expected_size,
                "File size mismatch: expected {}, got {}",
                expected_size,
                content.len()
            );

            // Verify blake3
            let actual_blake3 = blake3::hash(&content).to_hex().to_string();
            assert_eq!(actual_blake3, expected_blake3, "Blake3 hash mismatch");

            // Verify byte-by-byte content
            assert_eq!(
                content.as_slice(),
                expected_content,
                "Content mismatch: bytes differ"
            );

            Ok(())
        }
    }

    #[tokio::test]
    async fn test_single_logfile_growth_and_snapshots() {
        // Setup: Create a simulated logfile first
        let mut logfile = LogfileSimulator::new().await.unwrap();

        // Configuration for logfile ingestion (needs logfile path)
        let config = LogfileIngestConfig {
            active_pattern: logfile.path().to_string_lossy().to_string(),
            archived_pattern: "".to_string(), // No archived files for this test
            pond_path: "logs/test_app".to_string(),
            timestamp_field: None,
            timestamp_unit: Default::default(),
        };

        // Create pond with the config (creates real config file with FileID)
        let pond = PondSimulator::new(&config).await.unwrap();

        // Step 1: Append initial content to logfile
        logfile
            .append_line("2025-01-03 00:00:00 INFO Starting application")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:00:01 INFO Initialized database")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:00:02 INFO Listening on port 8080")
            .await
            .unwrap();

        let size_v1 = logfile.size();
        let blake3_v1 = logfile.blake3();

        log::info!("Logfile v1: {} bytes, blake3={}", size_v1, &blake3_v1[..16]);

        // Step 2: First ingestion - should capture entire file as v1
        pond.execute_factory(&config).await.unwrap();

        // Verify: File should exist in pond with correct content
        pond.verify_file(
            "logs/test_app",
            "app.log",
            size_v1,
            &blake3_v1,
            logfile.content(),
        )
        .await
        .unwrap();

        log::info!("[OK] Initial snapshot successful");

        // Step 3: Append more content to logfile
        logfile
            .append_line("2025-01-03 00:01:00 INFO Processed 100 requests")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:02:00 INFO Processed 200 requests")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:03:00 WARN Connection timeout for client 127.0.0.1")
            .await
            .unwrap();

        let size_v2 = logfile.size();
        let blake3_v2 = logfile.blake3();
        let appended_bytes = size_v2 - size_v1;

        log::info!(
            "Logfile v2: {} bytes (+{} appended), blake3={}",
            size_v2,
            appended_bytes,
            &blake3_v2[..16]
        );

        // Step 4: Second ingestion - should detect append and create v2
        pond.execute_factory(&config).await.unwrap();

        // Verify: File should have updated content
        pond.verify_file(
            "logs/test_app",
            "app.log",
            size_v2,
            &blake3_v2,
            logfile.content(),
        )
        .await
        .unwrap();

        log::info!("[OK] Incremental append successful");

        // Step 5: Append even more content
        logfile
            .append_line("2025-01-03 00:04:00 INFO Processed 300 requests")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:05:00 ERROR Database connection failed")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:05:01 INFO Reconnected to database")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:06:00 INFO Processed 400 requests")
            .await
            .unwrap();

        let size_v3 = logfile.size();
        let blake3_v3 = logfile.blake3();
        let appended_bytes_v3 = size_v3 - size_v2;

        log::info!(
            "Logfile v3: {} bytes (+{} appended), blake3={}",
            size_v3,
            appended_bytes_v3,
            &blake3_v3[..16]
        );

        // Step 6: Third ingestion - should detect second append and create v3
        pond.execute_factory(&config).await.unwrap();

        // Verify: File should have latest content
        pond.verify_file(
            "logs/test_app",
            "app.log",
            size_v3,
            &blake3_v3,
            logfile.content(),
        )
        .await
        .unwrap();

        log::info!("[OK] Second incremental append successful");
        log::info!("[OK] All snapshots and verifications passed!");
    }

    #[tokio::test]
    async fn test_logfile_no_change_detection() {
        // Test that running ingestion multiple times without file changes is idempotent
        let mut logfile = LogfileSimulator::new().await.unwrap();

        let config = LogfileIngestConfig {
            active_pattern: logfile.path().to_string_lossy().to_string(),
            archived_pattern: "".to_string(),
            pond_path: "logs/test_app".to_string(),
            timestamp_field: None,
            timestamp_unit: Default::default(),
        };

        // Create pond with config (gets real FileID)
        let pond = PondSimulator::new(&config).await.unwrap();

        // Append some content
        logfile
            .append_line("2025-01-03 00:00:00 INFO Test line 1")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:00:01 INFO Test line 2")
            .await
            .unwrap();

        let size = logfile.size();
        let blake3 = logfile.blake3();

        // First ingestion
        pond.execute_factory(&config).await.unwrap();

        // Second ingestion - no changes
        pond.execute_factory(&config).await.unwrap();

        // Third ingestion - still no changes
        pond.execute_factory(&config).await.unwrap();

        // Verify: File should still have same content
        pond.verify_file("logs/test_app", "app.log", size, &blake3, logfile.content())
            .await
            .unwrap();

        log::info!(
            "[OK] Idempotency test passed - multiple ingestions without changes work correctly"
        );
    }

    #[tokio::test]
    async fn test_logfile_rotation() {
        //! Test log rotation scenario:
        //! 1. Active file grows, we snapshot it
        //! 2. Active file rotates (becomes archived with timestamp, new active file starts)
        //! 3. Factory should:
        //!    - Rename pond's active file to match the archived filename (preserving version history)
        //!    - Create a new file for the new active content
        //!
        //! Assumption: We snapshot at least once before a second rotation happens,
        //! so there is only one reasonable candidate for matching.

        // Setup: Create temp directory for log files
        let temp_dir = TempDir::new().unwrap();
        let active_path = temp_dir.path().join("app.log");
        let archived_pattern = temp_dir
            .path()
            .join("app-*.log")
            .to_string_lossy()
            .to_string();

        // Create initial active file with some content
        let initial_content = b"2025-01-03 00:00:00 INFO Starting application\n\
                                2025-01-03 00:00:01 INFO Initialized database\n\
                                2025-01-03 00:00:02 INFO Listening on port 8080\n";
        fs::write(&active_path, initial_content).await.unwrap();

        let initial_blake3 = blake3::hash(initial_content).to_hex().to_string();
        let initial_size = initial_content.len() as u64;

        // Configuration
        let config = LogfileIngestConfig {
            active_pattern: active_path.to_string_lossy().to_string(),
            archived_pattern: archived_pattern.clone(),
            pond_path: "logs/test_app".to_string(),
            timestamp_field: None,
            timestamp_unit: Default::default(),
        };

        // Create pond
        let pond = PondSimulator::new(&config).await.unwrap();

        // Step 1: First ingestion - capture initial active file
        pond.execute_factory(&config).await.unwrap();

        pond.verify_file(
            "logs/test_app",
            "app.log",
            initial_size,
            &initial_blake3,
            initial_content,
        )
        .await
        .unwrap();

        log::info!(
            "[OK] Step 1: Initial snapshot captured ({} bytes)",
            initial_size
        );

        // Step 2: Simulate file growth before rotation
        let growth_content = b"2025-01-03 00:01:00 INFO Processed 100 requests\n\
                               2025-01-03 00:02:00 INFO Processed 200 requests\n";
        let mut grown_content = initial_content.to_vec();
        grown_content.extend_from_slice(growth_content);

        fs::write(&active_path, &grown_content).await.unwrap();

        let grown_blake3 = blake3::hash(&grown_content).to_hex().to_string();
        let grown_size = grown_content.len() as u64;

        // Capture the growth
        pond.execute_factory(&config).await.unwrap();

        pond.verify_file(
            "logs/test_app",
            "app.log",
            grown_size,
            &grown_blake3,
            &grown_content,
        )
        .await
        .unwrap();

        log::info!("[OK] Step 2: File growth captured ({} bytes)", grown_size);

        // Step 3: Simulate rotation
        // - Active file becomes archived with timestamp
        // - New active file starts fresh
        let archived_path = temp_dir.path().join("app-2025-01-03T00-03-00.log");
        fs::rename(&active_path, &archived_path).await.unwrap();

        // New active file with fresh content
        let new_active_content = b"2025-01-03 00:03:00 INFO Log rotated - starting fresh\n\
                                   2025-01-03 00:03:01 INFO New log session begins\n";
        fs::write(&active_path, new_active_content).await.unwrap();

        let new_active_blake3 = blake3::hash(new_active_content).to_hex().to_string();
        let new_active_size = new_active_content.len() as u64;

        log::info!(
            "Rotation: archived {} bytes to {}, new active {} bytes",
            grown_size,
            archived_path.file_name().unwrap().to_string_lossy(),
            new_active_size
        );

        // Step 4: Ingestion after rotation should:
        // - Detect that active file shrunk (rotation occurred)
        // - Find the archived file that matches the pond's tracked content
        // - Rename pond file from "app.log" to "app-2025-01-03T00-03-00.log"
        // - Create new "app.log" with the fresh content
        pond.execute_factory(&config).await.unwrap();

        // Verify: The archived file in pond should have the pre-rotation content
        // (with all version history preserved from when it was the active file)
        pond.verify_file(
            "logs/test_app",
            "app-2025-01-03T00-03-00.log",
            grown_size,
            &grown_blake3,
            &grown_content,
        )
        .await
        .unwrap();

        log::info!("[OK] Step 4a: Archived file preserved with correct content");

        // Verify: The new active file should have the new content
        pond.verify_file(
            "logs/test_app",
            "app.log",
            new_active_size,
            &new_active_blake3,
            new_active_content,
        )
        .await
        .unwrap();

        log::info!("[OK] Step 4b: New active file created with fresh content");

        // Step 5: Continue growing the new active file to ensure incremental append still works
        let more_content = b"2025-01-03 00:04:00 INFO Post-rotation processing continues\n";
        let mut final_active_content = new_active_content.to_vec();
        final_active_content.extend_from_slice(more_content);

        fs::write(&active_path, &final_active_content)
            .await
            .unwrap();

        let final_active_blake3 = blake3::hash(&final_active_content).to_hex().to_string();
        let final_active_size = final_active_content.len() as u64;

        pond.execute_factory(&config).await.unwrap();

        pond.verify_file(
            "logs/test_app",
            "app.log",
            final_active_size,
            &final_active_blake3,
            &final_active_content,
        )
        .await
        .unwrap();

        log::info!("[OK] Step 5: Post-rotation append captured");

        // Verify archived file is still intact
        pond.verify_file(
            "logs/test_app",
            "app-2025-01-03T00-03-00.log",
            grown_size,
            &grown_blake3,
            &grown_content,
        )
        .await
        .unwrap();

        log::info!("[OK] Archived file still intact after subsequent operations");
        log::info!("[OK] All rotation tests passed!");
    }

    /// Extended verification helper that checks both content AND metadata blake3
    struct Blake3Verifier {
        store_path: String,
        pond_id: String,
    }

    impl Blake3Verifier {
        fn new(store_path: &str, pond_id: String) -> Self {
            Self {
                store_path: store_path.to_string(),
                pond_id,
            }
        }

        /// Verify that a file's cumulative blake3 matches expected value.
        /// This checks BOTH:
        /// 1. The content hashes to the expected blake3
        /// 2. The stored OplogEntry.blake3 field matches
        /// 3. The SeriesOutboard.cumulative_blake3 matches (for FilePhysicalSeries)
        async fn verify_file_blake3(
            &self,
            pond_path: &str,
            filename: &str,
            expected_content: &[u8],
        ) -> std::io::Result<()> {
            let mut persistence =
                crate::persistence::OpLogPersistence::open(&self.store_path, self.pond_id.clone())
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

            let tx = persistence
                .begin_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let root = tx
                .root()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let file_path = format!("{}/{}", pond_path, filename);
            let file_node = root
                .get_node_path(&file_path)
                .await
                .map_err(|e| std::io::Error::other(format!("File not found: {}", e)))?;

            let file_id = file_node.id();

            // 1. Read content and verify it matches expected
            use tokio::io::AsyncReadExt;
            let file = file_node
                .as_file()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            let mut content = Vec::new();
            let mut reader = file
                .async_reader()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            let _ = reader.read_to_end(&mut content).await?;

            assert_eq!(
                content.as_slice(),
                expected_content,
                "Content bytes don't match expected"
            );

            // 2. Compute expected cumulative blake3 using IncrementalHashState
            // IMPORTANT: We use IncrementalHashState::root_hash(), NOT blake3::hash()!
            // For FilePhysicalSeries, the cumulative_blake3 is the bao-tree incremental
            // root hash, which differs from blake3::hash() for exactly 1 block (16KB).
            // This is intentional - it ensures consistency with append operations.
            let mut hash_state = utilities::bao_outboard::IncrementalHashState::new();
            hash_state.ingest(expected_content);
            let expected_blake3 = hash_state.root_hash().to_hex().to_string();

            // 3. Query OplogEntry records to verify stored blake3
            let state = tx
                .state()
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            // Use metadata() which extracts the cumulative blake3 from
            // SeriesOutboard for FilePhysicalSeries entries.
            let metadata = state
                .metadata(file_id)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            // 4. Verify metadata.blake3 (cumulative for FilePhysicalSeries)
            let stored_blake3 = metadata
                .blake3
                .as_ref()
                .expect("metadata.blake3 should be set for FilePhysicalSeries");

            assert_eq!(
                stored_blake3, &expected_blake3,
                "metadata.blake3 mismatch!\n  Expected: {}\n  Stored:   {}",
                expected_blake3, stored_blake3
            );

            // 5. Verify SeriesOutboard.cumulative_blake3
            if let Some(bao_bytes) = &metadata.bao_outboard {
                let series_outboard =
                    utilities::bao_outboard::SeriesOutboard::from_bytes(bao_bytes).map_err(
                        |e| std::io::Error::other(format!("Failed to parse SeriesOutboard: {}", e)),
                    )?;

                let outboard_blake3 = blake3::Hash::from_bytes(series_outboard.cumulative_blake3)
                    .to_hex()
                    .to_string();

                assert_eq!(
                    outboard_blake3, expected_blake3,
                    "SeriesOutboard.cumulative_blake3 mismatch!\n  Expected: {}\n  Outboard: {}",
                    expected_blake3, outboard_blake3
                );

                // Also verify cumulative_size matches content length
                assert_eq!(
                    series_outboard.cumulative_size,
                    expected_content.len() as u64,
                    "SeriesOutboard.cumulative_size mismatch"
                );
            }

            tx.commit_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            Ok(())
        }

        /// Get version count for a file
        async fn get_version_count(
            &self,
            pond_path: &str,
            filename: &str,
        ) -> std::io::Result<usize> {
            let mut persistence =
                crate::persistence::OpLogPersistence::open(&self.store_path, self.pond_id.clone())
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

            let tx = persistence
                .begin_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let root = tx
                .root()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let file_path = format!("{}/{}", pond_path, filename);
            let file_node = root
                .get_node_path(&file_path)
                .await
                .map_err(|e| std::io::Error::other(format!("File not found: {}", e)))?;

            let state = tx
                .state()
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            let entries = state
                .query_records(file_node.id())
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            tx.commit_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            Ok(entries.len())
        }
    }

    #[tokio::test]
    async fn test_blake3_validation_small_versions_then_rotation() {
        //! Comprehensive blake3 validation test:
        //! 1. Create small file (v1) - verify cumulative blake3
        //! 2. Append small content (v2) - verify cumulative blake3
        //! 3. Append more content (v3) - verify cumulative blake3
        //! 4. Rotate: rename active to archived, create new active
        //! 5. Verify archived file still has correct cumulative blake3
        //! 6. Verify new active file has its own blake3
        //! 7. Append to new active (v2 of new file) - verify cumulative blake3

        let temp_dir = TempDir::new().unwrap();
        let active_path = temp_dir.path().join("app.log");
        let archived_pattern = temp_dir
            .path()
            .join("app-*.log")
            .to_string_lossy()
            .to_string();

        // === Phase 1: Small Version 1 ===
        let v1_content = b"2025-01-03 00:00:00 INFO Starting\n";
        fs::write(&active_path, v1_content).await.unwrap();

        let config = LogfileIngestConfig {
            active_pattern: active_path.to_string_lossy().to_string(),
            archived_pattern: archived_pattern.clone(),
            pond_path: "logs/blake3_test".to_string(),
            timestamp_field: None,
            timestamp_unit: Default::default(),
        };

        let pond = PondSimulator::new(&config).await.unwrap();
        let verifier = Blake3Verifier::new(pond.store_path(), pond.pond_id.clone());

        pond.execute_factory(&config).await.unwrap();
        verifier
            .verify_file_blake3("logs/blake3_test", "app.log", v1_content)
            .await
            .unwrap();
        log::info!(
            "[OK] v1 ({} bytes): cumulative blake3 correct",
            v1_content.len()
        );

        // === Phase 2: Append small content (v2) ===
        let v2_append = b"2025-01-03 00:01:00 INFO Processing\n";
        let mut v2_cumulative = v1_content.to_vec();
        v2_cumulative.extend_from_slice(v2_append);
        fs::write(&active_path, &v2_cumulative).await.unwrap();

        pond.execute_factory(&config).await.unwrap();
        verifier
            .verify_file_blake3("logs/blake3_test", "app.log", &v2_cumulative)
            .await
            .unwrap();
        let v2_versions = verifier
            .get_version_count("logs/blake3_test", "app.log")
            .await
            .unwrap();
        assert_eq!(v2_versions, 2, "Should have 2 versions after append");
        log::info!(
            "[OK] v2 ({} bytes, {} versions): cumulative blake3 correct",
            v2_cumulative.len(),
            v2_versions
        );

        // === Phase 3: Append more content (v3) ===
        let v3_append = b"2025-01-03 00:02:00 INFO More work\n2025-01-03 00:03:00 INFO Done\n";
        let mut v3_cumulative = v2_cumulative.clone();
        v3_cumulative.extend_from_slice(v3_append);
        fs::write(&active_path, &v3_cumulative).await.unwrap();

        pond.execute_factory(&config).await.unwrap();
        verifier
            .verify_file_blake3("logs/blake3_test", "app.log", &v3_cumulative)
            .await
            .unwrap();
        let v3_versions = verifier
            .get_version_count("logs/blake3_test", "app.log")
            .await
            .unwrap();
        assert_eq!(v3_versions, 3, "Should have 3 versions");
        log::info!(
            "[OK] v3 ({} bytes, {} versions): cumulative blake3 correct",
            v3_cumulative.len(),
            v3_versions
        );

        // === Phase 4: Rotation ===
        let archived_filename = "app-2025-01-03T00-04-00.log";
        let archived_path = temp_dir.path().join(archived_filename);
        fs::rename(&active_path, &archived_path).await.unwrap();

        // New active file
        let new_v1_content = b"2025-01-03 00:04:00 INFO Log rotated\n";
        fs::write(&active_path, new_v1_content).await.unwrap();

        pond.execute_factory(&config).await.unwrap();

        // === Phase 5: Verify archived file ===
        verifier
            .verify_file_blake3("logs/blake3_test", archived_filename, &v3_cumulative)
            .await
            .unwrap();
        let archived_versions = verifier
            .get_version_count("logs/blake3_test", archived_filename)
            .await
            .unwrap();
        assert_eq!(
            archived_versions, 3,
            "Archived file should preserve 3 versions"
        );
        log::info!(
            "[OK] Archived file ({} bytes, {} versions): cumulative blake3 preserved",
            v3_cumulative.len(),
            archived_versions
        );

        // === Phase 6: Verify new active file ===
        verifier
            .verify_file_blake3("logs/blake3_test", "app.log", new_v1_content)
            .await
            .unwrap();
        let new_v1_versions = verifier
            .get_version_count("logs/blake3_test", "app.log")
            .await
            .unwrap();
        assert_eq!(new_v1_versions, 1, "New active should have 1 version");
        log::info!(
            "[OK] New active v1 ({} bytes, {} version): blake3 correct",
            new_v1_content.len(),
            new_v1_versions
        );

        // === Phase 7: Append to new active ===
        let new_v2_append = b"2025-01-03 00:05:00 INFO Post-rotation work\n";
        let mut new_v2_cumulative = new_v1_content.to_vec();
        new_v2_cumulative.extend_from_slice(new_v2_append);
        fs::write(&active_path, &new_v2_cumulative).await.unwrap();

        pond.execute_factory(&config).await.unwrap();
        verifier
            .verify_file_blake3("logs/blake3_test", "app.log", &new_v2_cumulative)
            .await
            .unwrap();
        let new_v2_versions = verifier
            .get_version_count("logs/blake3_test", "app.log")
            .await
            .unwrap();
        assert_eq!(new_v2_versions, 2, "New active should have 2 versions");
        log::info!(
            "[OK] New active v2 ({} bytes, {} versions): cumulative blake3 correct",
            new_v2_cumulative.len(),
            new_v2_versions
        );

        // Final verification: archived file unchanged
        verifier
            .verify_file_blake3("logs/blake3_test", archived_filename, &v3_cumulative)
            .await
            .unwrap();
        log::info!("[OK] Archived file still correct after new active changes");

        log::info!("\n[OK] All blake3 validation tests passed!");
    }

    #[tokio::test]
    async fn test_blake3_validation_large_versions() {
        //! Test blake3 validation with larger versions that span multiple bao-tree blocks.
        //! Block size is 16KB, so we test with content > 16KB to exercise incremental hashing.

        let temp_dir = TempDir::new().unwrap();
        let active_path = temp_dir.path().join("large.log");
        let archived_pattern = temp_dir
            .path()
            .join("large-*.log")
            .to_string_lossy()
            .to_string();

        // === Phase 1: Create 20KB file (> 1 block) ===
        let v1_content: Vec<u8> = (0..20_000)
            .map(|i| format!("Line {}: {:0>100}\n", i, i))
            .flat_map(|s| s.into_bytes())
            .take(20_000)
            .collect();
        fs::write(&active_path, &v1_content).await.unwrap();

        let config = LogfileIngestConfig {
            active_pattern: active_path.to_string_lossy().to_string(),
            archived_pattern: archived_pattern.clone(),
            pond_path: "logs/large_test".to_string(),
            timestamp_field: None,
            timestamp_unit: Default::default(),
        };

        let pond = PondSimulator::new(&config).await.unwrap();
        let verifier = Blake3Verifier::new(pond.store_path(), pond.pond_id.clone());

        pond.execute_factory(&config).await.unwrap();
        verifier
            .verify_file_blake3("logs/large_test", "large.log", &v1_content)
            .await
            .unwrap();
        log::info!(
            "[OK] Large v1 ({} bytes, {} blocks): blake3 correct",
            v1_content.len(),
            v1_content.len() / 16384 + 1
        );

        // === Phase 2: Append 18KB (crosses block boundary) ===
        let v2_append: Vec<u8> = (0..18_000)
            .map(|i| format!("Append {}: {:0>90}\n", i, i))
            .flat_map(|s| s.into_bytes())
            .take(18_000)
            .collect();
        let mut v2_cumulative = v1_content.clone();
        v2_cumulative.extend_from_slice(&v2_append);
        fs::write(&active_path, &v2_cumulative).await.unwrap();

        pond.execute_factory(&config).await.unwrap();
        verifier
            .verify_file_blake3("logs/large_test", "large.log", &v2_cumulative)
            .await
            .unwrap();
        log::info!(
            "[OK] Large v2 ({} bytes, {} blocks): cumulative blake3 correct",
            v2_cumulative.len(),
            v2_cumulative.len() / 16384 + 1
        );

        // === Phase 3: Rotation with large file ===
        let archived_filename = "large-2025-01-03.log";
        let archived_path = temp_dir.path().join(archived_filename);
        fs::rename(&active_path, &archived_path).await.unwrap();

        // New active with 25KB content
        let new_v1_content: Vec<u8> = (0..25_000)
            .map(|i| format!("New {}: {:0>95}\n", i, i))
            .flat_map(|s| s.into_bytes())
            .take(25_000)
            .collect();
        fs::write(&active_path, &new_v1_content).await.unwrap();

        pond.execute_factory(&config).await.unwrap();

        verifier
            .verify_file_blake3("logs/large_test", archived_filename, &v2_cumulative)
            .await
            .unwrap();
        log::info!(
            "[OK] Archived large ({} bytes): blake3 preserved",
            v2_cumulative.len()
        );

        verifier
            .verify_file_blake3("logs/large_test", "large.log", &new_v1_content)
            .await
            .unwrap();
        log::info!(
            "[OK] New large v1 ({} bytes, {} blocks): blake3 correct",
            new_v1_content.len(),
            new_v1_content.len() / 16384 + 1
        );

        log::info!("\n[OK] All large file blake3 validation tests passed!");
    }

    #[tokio::test]
    async fn test_blake3_validation_block_boundary_append() {
        //! Test appending content that crosses exactly at block boundaries (16KB).
        //! This exercises edge cases in incremental hashing.

        let temp_dir = TempDir::new().unwrap();
        let active_path = temp_dir.path().join("boundary.log");

        // Create exactly 16KB (1 block) for v1
        let v1_content: Vec<u8> = vec![b'A'; 16 * 1024];
        fs::write(&active_path, &v1_content).await.unwrap();

        let config = LogfileIngestConfig {
            active_pattern: active_path.to_string_lossy().to_string(),
            archived_pattern: "".to_string(),
            pond_path: "logs/boundary_test".to_string(),
            timestamp_field: None,
            timestamp_unit: Default::default(),
        };

        let pond = PondSimulator::new(&config).await.unwrap();
        let verifier = Blake3Verifier::new(pond.store_path(), pond.pond_id.clone());

        pond.execute_factory(&config).await.unwrap();
        verifier
            .verify_file_blake3("logs/boundary_test", "boundary.log", &v1_content)
            .await
            .unwrap();
        log::info!("[OK] Exactly 1 block (16384 bytes): blake3 correct");

        // Append exactly 16KB more (now 2 full blocks)
        let v2_append: Vec<u8> = vec![b'B'; 16 * 1024];
        let mut v2_cumulative = v1_content.clone();
        v2_cumulative.extend_from_slice(&v2_append);
        fs::write(&active_path, &v2_cumulative).await.unwrap();

        pond.execute_factory(&config).await.unwrap();
        verifier
            .verify_file_blake3("logs/boundary_test", "boundary.log", &v2_cumulative)
            .await
            .unwrap();
        log::info!("[OK] Exactly 2 blocks (32768 bytes): cumulative blake3 correct");

        // Append 1 byte (partial 3rd block)
        let v3_append: Vec<u8> = vec![b'C'; 1];
        let mut v3_cumulative = v2_cumulative.clone();
        v3_cumulative.extend_from_slice(&v3_append);
        fs::write(&active_path, &v3_cumulative).await.unwrap();

        pond.execute_factory(&config).await.unwrap();
        verifier
            .verify_file_blake3("logs/boundary_test", "boundary.log", &v3_cumulative)
            .await
            .unwrap();
        log::info!("[OK] 2 blocks + 1 byte (32769 bytes): cumulative blake3 correct");

        // Fill to exactly 3 blocks
        let v4_append: Vec<u8> = vec![b'D'; 16 * 1024 - 1];
        let mut v4_cumulative = v3_cumulative.clone();
        v4_cumulative.extend_from_slice(&v4_append);
        fs::write(&active_path, &v4_cumulative).await.unwrap();

        pond.execute_factory(&config).await.unwrap();
        verifier
            .verify_file_blake3("logs/boundary_test", "boundary.log", &v4_cumulative)
            .await
            .unwrap();
        log::info!("[OK] Exactly 3 blocks (49152 bytes): cumulative blake3 correct");

        log::info!("\n[OK] All block boundary blake3 validation tests passed!");
    }

    // =========================================================================
    // SCENARIO-BASED TEST FIXTURE
    // =========================================================================
    //
    // This test fixture exercises various rotation scenarios and independently
    // verifies that the final state of the host filesystem matches the pond.
    // It does NOT trust self-reported blake3 checksums - it reads files and
    // computes fresh checksums on a separate code path.

    /// Actions that can be performed on the host filesystem
    #[derive(Debug, Clone)]
    enum HostAction {
        /// Append content to the active file
        AppendToActive(Vec<u8>),
        /// Rotate: rename active to archived name, create new empty active
        Rotate { archived_name: String },
        /// Run the factory (take a snapshot)
        Snapshot,
    }

    /// Final state of a file for verification
    #[derive(Debug)]
    struct FileState {
        filename: String,
        content: Vec<u8>,
        blake3: String,
    }

    /// Scenario-based test runner that independently verifies final state
    struct ScenarioRunner {
        temp_dir: TempDir,
        active_path: PathBuf,
        pond: PondSimulator,
        config: LogfileIngestConfig,
        /// Current content of the active file on host
        active_content: Vec<u8>,
        /// Archived files on host: filename -> content
        archived_files: std::collections::HashMap<String, Vec<u8>>,
    }

    impl ScenarioRunner {
        async fn new(test_name: &str) -> std::io::Result<Self> {
            let temp_dir = TempDir::new()?;
            let active_path = temp_dir.path().join("app.log");
            let archived_pattern = temp_dir
                .path()
                .join("app-*.log")
                .to_string_lossy()
                .to_string();

            // Create empty active file
            fs::write(&active_path, b"").await?;

            let config = LogfileIngestConfig {
                active_pattern: active_path.to_string_lossy().to_string(),
                archived_pattern: archived_pattern.clone(),
                pond_path: format!("logs/{}", test_name),
                timestamp_field: None,
                timestamp_unit: Default::default(),
            };

            let pond = PondSimulator::new(&config).await?;

            Ok(Self {
                temp_dir,
                active_path,
                pond,
                config,
                active_content: Vec::new(),
                archived_files: std::collections::HashMap::new(),
            })
        }

        /// Execute a single action
        async fn execute_action(&mut self, action: &HostAction) -> std::io::Result<()> {
            match action {
                HostAction::AppendToActive(content) => {
                    self.active_content.extend_from_slice(content);
                    fs::write(&self.active_path, &self.active_content).await?;
                    log::info!(
                        "  -> Appended {} bytes to active (total: {} bytes)",
                        content.len(),
                        self.active_content.len()
                    );
                }
                HostAction::Rotate { archived_name } => {
                    let archived_path = self.temp_dir.path().join(archived_name);

                    // Move active -> archived
                    fs::rename(&self.active_path, &archived_path).await?;
                    let _ = self
                        .archived_files
                        .insert(archived_name.clone(), self.active_content.clone());

                    // Create new empty active
                    self.active_content = Vec::new();
                    fs::write(&self.active_path, b"").await?;

                    log::info!(
                        "  -> Rotated: active ({} bytes) -> {}",
                        self.archived_files[archived_name].len(),
                        archived_name
                    );
                }
                HostAction::Snapshot => {
                    self.pond.execute_factory(&self.config).await?;
                    log::info!("  -> Snapshot taken");
                }
            }
            Ok(())
        }

        /// Execute a sequence of actions
        async fn run_scenario(&mut self, actions: &[HostAction]) -> std::io::Result<()> {
            for (i, action) in actions.iter().enumerate() {
                log::info!("Step {}: {:?}", i + 1, action);
                self.execute_action(action).await?;
            }
            Ok(())
        }

        /// Get expected final state of all files on host
        fn expected_host_state(&self) -> Vec<FileState> {
            let mut states = Vec::new();

            // Active file (if non-empty)
            if !self.active_content.is_empty() {
                let mut hasher = utilities::bao_outboard::IncrementalHashState::new();
                hasher.ingest(&self.active_content);
                states.push(FileState {
                    filename: "app.log".to_string(),
                    content: self.active_content.clone(),
                    blake3: hasher.root_hash().to_hex().to_string(),
                });
            }

            // Archived files
            for (filename, content) in &self.archived_files {
                let mut hasher = utilities::bao_outboard::IncrementalHashState::new();
                hasher.ingest(content);
                states.push(FileState {
                    filename: filename.clone(),
                    content: content.clone(),
                    blake3: hasher.root_hash().to_hex().to_string(),
                });
            }

            states
        }

        /// INDEPENDENTLY verify that pond matches host filesystem
        /// This reads actual bytes and computes fresh checksums - does NOT trust metadata
        async fn verify_final_state(&self) -> std::io::Result<()> {
            log::info!("\n=== INDEPENDENT VERIFICATION ===");

            let expected_states = self.expected_host_state();
            log::info!("Expected {} files in pond", expected_states.len());

            let mut persistence = crate::persistence::OpLogPersistence::open(
                self.pond.store_path(),
                self.pond.pond_id().to_string(),
            )
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

            let tx = persistence
                .begin_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let root = tx
                .root()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            // Verify each expected file exists in pond with correct content
            for expected in &expected_states {
                let file_path = format!("{}/{}", self.config.pond_path, expected.filename);

                // Read actual content from pond
                let file_node = root.get_node_path(&file_path).await.map_err(|e| {
                    std::io::Error::other(format!(
                        "File {} not found in pond: {}",
                        expected.filename, e
                    ))
                })?;

                let file_id = file_node.id();

                let file = file_node
                    .as_file()
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                use tokio::io::AsyncReadExt;
                let mut actual_content = Vec::new();
                let mut reader = file
                    .async_reader()
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                let _ = reader.read_to_end(&mut actual_content).await?;

                // Compute fresh blake3 on actual content (independent code path)
                let mut hasher = utilities::bao_outboard::IncrementalHashState::new();
                hasher.ingest(&actual_content);
                let actual_blake3 = hasher.root_hash().to_hex().to_string();

                // Verify size
                if actual_content.len() != expected.content.len() {
                    return Err(std::io::Error::other(format!(
                        "SIZE MISMATCH for {}: expected {} bytes, got {} bytes\n  expected content: {:?}\n  actual content:   {:?}",
                        expected.filename,
                        expected.content.len(),
                        actual_content.len(),
                        String::from_utf8_lossy(&expected.content),
                        String::from_utf8_lossy(&actual_content)
                    )));
                }

                // Verify blake3 (computed independently, not from metadata)
                if actual_blake3 != expected.blake3 {
                    return Err(std::io::Error::other(format!(
                        "BLAKE3 MISMATCH for {}:\n  expected: {}\n  actual:   {}\n  expected content ({} bytes): {:?}\n  actual content ({} bytes):   {:?}",
                        expected.filename,
                        expected.blake3,
                        actual_blake3,
                        expected.content.len(),
                        String::from_utf8_lossy(&expected.content),
                        actual_content.len(),
                        String::from_utf8_lossy(&actual_content)
                    )));
                }

                // Verify byte-for-byte content
                if actual_content != expected.content {
                    // Find first differing byte
                    let diff_pos = actual_content
                        .iter()
                        .zip(expected.content.iter())
                        .position(|(a, b)| a != b)
                        .unwrap_or(actual_content.len().min(expected.content.len()));

                    return Err(std::io::Error::other(format!(
                        "CONTENT MISMATCH for {} at byte {}: expected {:02x}, got {:02x}",
                        expected.filename,
                        diff_pos,
                        expected.content.get(diff_pos).copied().unwrap_or(0),
                        actual_content.get(diff_pos).copied().unwrap_or(0)
                    )));
                }

                // CRITICAL: Verify TinyFS metadata blake3 matches computed blake3
                // This ensures multi-version FilePhysicalSeries has correct checksums.
                // Use metadata() which returns the cumulative blake3 from SeriesOutboard.
                let state = tx
                    .state()
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                let file_metadata: tinyfs::NodeMetadata = state
                    .metadata(file_id)
                    .await
                    .map_err(|e: tinyfs::Error| std::io::Error::other(e.to_string()))?;

                let stored_blake3 = file_metadata.blake3.as_ref().ok_or_else(|| {
                    std::io::Error::other(format!(
                        "METADATA MISSING: {} has no blake3",
                        expected.filename
                    ))
                })?;

                if stored_blake3 != &expected.blake3 {
                    return Err(std::io::Error::other(format!(
                        "METADATA BLAKE3 MISMATCH for {}:\n  expected (computed): {}\n  stored (TinyFS):     {}\n  version: {}",
                        expected.filename, expected.blake3, stored_blake3, file_metadata.version
                    )));
                }

                // Also verify SeriesOutboard.cumulative_blake3
                if let Some(bao_bytes) = &file_metadata.bao_outboard {
                    let series_outboard = utilities::bao_outboard::SeriesOutboard::from_bytes(
                        bao_bytes,
                    )
                    .map_err(|e| {
                        std::io::Error::other(format!(
                            "Failed to parse SeriesOutboard for {}: {}",
                            expected.filename, e
                        ))
                    })?;

                    let outboard_blake3 =
                        blake3::Hash::from_bytes(series_outboard.cumulative_blake3)
                            .to_hex()
                            .to_string();

                    if outboard_blake3 != expected.blake3 {
                        return Err(std::io::Error::other(format!(
                            "OUTBOARD BLAKE3 MISMATCH for {}:\n  expected: {}\n  outboard: {}",
                            expected.filename, expected.blake3, outboard_blake3
                        )));
                    }

                    // Verify cumulative_size matches content length
                    if series_outboard.cumulative_size != expected.content.len() as u64 {
                        return Err(std::io::Error::other(format!(
                            "OUTBOARD SIZE MISMATCH for {}: expected {} bytes, outboard says {}",
                            expected.filename,
                            expected.content.len(),
                            series_outboard.cumulative_size
                        )));
                    }
                }

                log::info!(
                    "  [OK] {} ({} bytes, blake3={}...)",
                    expected.filename,
                    expected.content.len(),
                    &expected.blake3[..16]
                );
            }

            // Verify no extra files in pond
            let pond_dir = root
                .open_dir_path(&self.config.pond_path)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            use futures::StreamExt;
            let mut entries_stream = pond_dir
                .entries()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let mut pond_entries: Vec<String> = Vec::new();
            while let Some(entry_result) = entries_stream.next().await {
                let entry = entry_result.map_err(|e| std::io::Error::other(e.to_string()))?;
                pond_entries.push(entry.name);
            }

            let expected_names: std::collections::HashSet<_> =
                expected_states.iter().map(|s| s.filename.clone()).collect();

            for pond_name in &pond_entries {
                if !expected_names.contains(pond_name) {
                    return Err(std::io::Error::other(format!(
                        "UNEXPECTED FILE in pond: {} (not on host)",
                        pond_name
                    )));
                }
            }

            tx.commit_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            log::info!("=== VERIFICATION PASSED ===\n");
            Ok(())
        }
    }

    #[test]
    fn test_blake3_computation_sanity() {
        //! Verify our blake3 computation matches expectations
        let content = b"Day 3 logs\n";

        // Method 1: IncrementalHashState (what test fixture uses)
        let mut hasher = utilities::bao_outboard::IncrementalHashState::new();
        hasher.ingest(content);
        let incremental_hash = hasher.root_hash().to_hex().to_string();

        // Method 2: Direct blake3::hash (standard)
        let direct_hash = blake3::hash(content).to_hex().to_string();

        log::info!("Content: {:?} ({} bytes)", content, content.len());
        log::info!("IncrementalHashState: {}", incremental_hash);
        log::info!("blake3::hash:         {}", direct_hash);

        // For small content (< 16KB), these should be identical
        assert_eq!(
            incremental_hash, direct_hash,
            "Small content hash mismatch between incremental and direct"
        );

        // The expected hash from test failure was: f88090e6b17b04f8214a08f6f73f09a6e932a3d74ae70ef459a27dadb8e56310
        // Let's verify this is what we compute
        assert_eq!(
            incremental_hash, "f88090e6b17b04f8214a08f6f73f09a6e932a3d74ae70ef459a27dadb8e56310",
            "Hash doesn't match expected value for 'Day 3 logs\\n'"
        );
    }

    #[tokio::test]
    async fn test_scenario_simple_growth() {
        //! Scenario: Simple file growth without rotation
        //! append -> snapshot -> append -> snapshot -> append -> snapshot

        let mut runner = ScenarioRunner::new("scenario_simple_growth").await.unwrap();

        runner
            .run_scenario(&[
                HostAction::AppendToActive(b"Line 1\n".to_vec()),
                HostAction::Snapshot,
                HostAction::AppendToActive(b"Line 2\n".to_vec()),
                HostAction::Snapshot,
                HostAction::AppendToActive(b"Line 3\nLine 4\nLine 5\n".to_vec()),
                HostAction::Snapshot,
            ])
            .await
            .unwrap();

        runner.verify_final_state().await.unwrap();
        log::info!("[OK] Scenario: simple growth passed");
    }

    #[tokio::test]
    async fn test_scenario_rotation_fully_captured() {
        //! Scenario: Rotation where all content was captured before rotation
        //! (The "happy path" - no missed bytes)

        let mut runner = ScenarioRunner::new("scenario_rotation_captured")
            .await
            .unwrap();

        runner
            .run_scenario(&[
                HostAction::AppendToActive(b"Initial content\n".to_vec()),
                HostAction::Snapshot,
                HostAction::AppendToActive(b"More content before rotation\n".to_vec()),
                HostAction::Snapshot, // Capture everything before rotation
                HostAction::Rotate {
                    archived_name: "app-2025-01-03.log".to_string(),
                },
                HostAction::AppendToActive(b"New log session\n".to_vec()),
                HostAction::Snapshot,
            ])
            .await
            .unwrap();

        runner.verify_final_state().await.unwrap();
        log::info!("[OK] Scenario: rotation fully captured passed");
    }

    #[tokio::test]
    async fn test_scenario_rotation_with_missed_bytes() {
        //! Scenario: Rotation where growth happened AFTER last snapshot
        //! This is the bug case we identified:
        //! - snapshot at 100 bytes
        //! - file grows to 200 bytes (NO SNAPSHOT)
        //! - rotation occurs
        //! - next snapshot must capture the missed 100 bytes in archived file

        let mut runner = ScenarioRunner::new("scenario_rotation_missed")
            .await
            .unwrap();

        runner
            .run_scenario(&[
                HostAction::AppendToActive(b"Initial snapshot content\n".to_vec()),
                HostAction::Snapshot, // Capture initial state
                HostAction::AppendToActive(
                    b"This content will be MISSED before rotation!\n".to_vec(),
                ),
                // NO SNAPSHOT HERE - simulates growth between snapshot intervals
                HostAction::Rotate {
                    archived_name: "app-2025-01-03.log".to_string(),
                },
                HostAction::AppendToActive(b"New log after rotation\n".to_vec()),
                HostAction::Snapshot, // This should: rename pond file, append missed bytes, create new active
            ])
            .await
            .unwrap();

        runner.verify_final_state().await.unwrap();
        log::info!("[OK] Scenario: rotation with missed bytes passed");
    }

    #[tokio::test]
    async fn test_scenario_multiple_rotations() {
        //! Scenario: Multiple rotations over time

        let mut runner = ScenarioRunner::new("scenario_multi_rotation")
            .await
            .unwrap();

        runner
            .run_scenario(&[
                // First file lifecycle
                HostAction::AppendToActive(b"Day 1 logs\n".to_vec()),
                HostAction::Snapshot,
                HostAction::AppendToActive(b"Day 1 more logs\n".to_vec()),
                HostAction::Snapshot,
                HostAction::Rotate {
                    archived_name: "app-day1.log".to_string(),
                },
                // Second file lifecycle
                HostAction::AppendToActive(b"Day 2 logs\n".to_vec()),
                HostAction::Snapshot,
                HostAction::Rotate {
                    archived_name: "app-day2.log".to_string(),
                },
                // Third file (current active)
                HostAction::AppendToActive(b"Day 3 logs\n".to_vec()),
                HostAction::Snapshot,
            ])
            .await
            .unwrap();

        runner.verify_final_state().await.unwrap();
        log::info!("[OK] Scenario: multiple rotations passed");
    }

    #[tokio::test]
    async fn test_scenario_large_file_rotation() {
        //! Scenario: Large files (> 16KB block size) with rotation

        let mut runner = ScenarioRunner::new("scenario_large_rotation")
            .await
            .unwrap();

        // Create content > 16KB
        let large_content_1: Vec<u8> = (0..20_000)
            .map(|i| format!("Log line {}: some data here\n", i))
            .flat_map(|s| s.into_bytes())
            .take(20_000)
            .collect();

        let large_content_2: Vec<u8> = (0..15_000)
            .map(|i| format!("More log {}: additional data\n", i))
            .flat_map(|s| s.into_bytes())
            .take(15_000)
            .collect();

        runner
            .run_scenario(&[
                HostAction::AppendToActive(large_content_1.clone()),
                HostAction::Snapshot,
                HostAction::AppendToActive(large_content_2.clone()),
                // Miss some content before rotation
                HostAction::Rotate {
                    archived_name: "app-large.log".to_string(),
                },
                HostAction::AppendToActive(b"New session after large file rotation\n".to_vec()),
                HostAction::Snapshot,
            ])
            .await
            .unwrap();

        runner.verify_final_state().await.unwrap();
        log::info!("[OK] Scenario: large file rotation passed");
    }

    #[tokio::test]
    async fn test_scenario_block_boundary_rotation() {
        //! Scenario: Rotation at exact block boundary (16KB)

        let mut runner = ScenarioRunner::new("scenario_block_boundary")
            .await
            .unwrap();

        // Exactly 16KB
        let exactly_one_block = vec![b'A'; 16 * 1024];

        runner
            .run_scenario(&[
                HostAction::AppendToActive(exactly_one_block.clone()),
                HostAction::Snapshot,
                // Append exactly one more block
                HostAction::AppendToActive(vec![b'B'; 16 * 1024]),
                // Miss the second block before rotation
                HostAction::Rotate {
                    archived_name: "app-boundary.log".to_string(),
                },
                HostAction::AppendToActive(b"After boundary rotation\n".to_vec()),
                HostAction::Snapshot,
            ])
            .await
            .unwrap();

        runner.verify_final_state().await.unwrap();
        log::info!("[OK] Scenario: block boundary rotation passed");
    }
}
