// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::common::{FileInfoVisitor, ShipContext, TargetContext, classify_target};
use anyhow::Result;

/// Normalize a pattern for the list command.
///
/// Handles common user expectations:
/// - "/" becomes "/*" (list root directory entries)
/// - "/path/to/dir/" (trailing slash) becomes "/path/to/dir/*" (list directory contents)
fn normalize_pattern(pattern: &str) -> String {
    let trimmed = pattern.trim();

    // "/" alone should list root directory entries
    if trimmed == "/" {
        return "/*".to_string();
    }

    // Trailing slash means "list contents of this directory"
    if trimmed.ends_with('/') {
        return format!("{}*", trimmed);
    }

    trimmed.to_string()
}

/// List files with a closure for handling output
pub async fn list_command<F>(
    ship_context: &ShipContext,
    pattern: &str,
    show_all: bool,
    long: bool,
    mut handler: F,
) -> Result<()>
where
    F: FnMut(&str),
{
    // Classify the pattern to determine pond vs host context
    let target = classify_target(pattern);

    let (normalized_pattern, mut ship) = match target {
        TargetContext::Host(ref host_path) => {
            let np = normalize_pattern(host_path);
            let s = ship_context.open_host()?;
            (np, s)
        }
        TargetContext::Pond(_) => {
            let np = normalize_pattern(pattern);
            let s = ship_context.open_pond().await?;
            (np, s)
        }
    };

    // Use transaction for consistent filesystem access
    let mut file_results = crate::common::with_read_transaction(
        &mut ship,
        vec!["list".to_string(), normalized_pattern.clone()],
        async |tx| {
            let root = tx.root().await?;

            // Use FileInfoVisitor to collect file information - always allow all files at visitor level
            let mut visitor = FileInfoVisitor::new(true); // Always allow all at visitor level
            root.visit_with_visitor(&normalized_pattern, &mut visitor)
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to list files matching '{}' from data filesystem: {}",
                        normalized_pattern,
                        e
                    )
                })
        },
    )
    .await?;

    // Filter hidden files if show_all is false
    if !show_all {
        file_results.retain(|file_info| {
            let basename = std::path::Path::new(&file_info.path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");
            !basename.starts_with('.') || basename == "." || basename == ".."
        });
    }

    // Sort results by path for consistent output
    file_results.sort_by(|a, b| a.path.cmp(&b.path));

    for file_info in file_results {
        if long {
            handler(&file_info.format_long_style());
        } else {
            handler(&file_info.format_watertown_style());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::init::init_command;
    use crate::common::ShipContext;
    use log::debug;

    struct TestSetup {
        ship_context: ShipContext,
        _temp_dir: tempfile::TempDir,
    }

    impl TestSetup {
        async fn new() -> Result<Self> {
            let temp_dir = tempfile::tempdir()?;
            let pond_path = temp_dir.path().join("test_pond");

            // Create ship context for initialization
            let init_args = vec!["pond".to_string(), "init".to_string()];
            let ship_context = ShipContext::pond_only(Some(&pond_path), init_args.clone());

            // Initialize the pond
            init_command(&ship_context, "test-host").await?;

            Ok(Self {
                ship_context,
                _temp_dir: temp_dir,
            })
        }

        async fn create_pond_file(
            &self,
            path: &str,
            content: &str,
            entry_type: tinyfs::EntryType,
        ) -> Result<()> {
            use tokio::io::AsyncWriteExt;

            let mut ship = self.ship_context.open_pond().await?;
            let tx = ship
                .begin_write(&steward::PondUserMetadata::new(vec![
                    "test_setup".to_string(),
                    path.to_string(),
                ]))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to begin transaction: {}", e))?;

            let result = {
                let fs = &*tx;
                let root = fs.root().await?;
                let mut writer = root.async_writer_path_with_type(path, entry_type).await?;
                writer.write_all(content.as_bytes()).await?;
                writer.flush().await?;
                writer.shutdown().await
            };

            // Check shutdown result BEFORE committing
            result.map_err(|e| anyhow::anyhow!("Failed to write file content: {}", e))?;

            let _ = tx
                .commit()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to commit transaction: {}", e))?;
            Ok(())
        }

        async fn create_pond_directory(&self, path: &str) -> Result<()> {
            let mut ship = self.ship_context.open_pond().await?;
            let tx = ship
                .begin_write(&steward::PondUserMetadata::new(vec![
                    "test_setup".to_string(),
                    path.to_string(),
                ]))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to begin transaction: {}", e))?;

            let result = {
                let fs = &*tx;
                let root = fs.root().await?;
                root.create_dir_path(path).await.map(|_| ())
            };

            _ = tx
                .commit()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to commit transaction: {}", e))?;
            result.map_err(|e| anyhow::anyhow!("Failed to create directory: {}", e))?;
            Ok(())
        }

        async fn create_pond_dynamic_directory(&self, path: &str, mtime: i64) -> Result<()> {
            let mut ship = self.ship_context.open_pond().await?;
            let tx = ship
                .begin_write(&steward::PondUserMetadata::new(vec![
                    "test_setup".to_string(),
                    path.to_string(),
                ]))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to begin transaction: {}", e))?;

            let result = {
                let fs = &*tx;
                let root = fs.root().await?;
                root.create_dynamic_path_with_mtime(
                    path,
                    tinyfs::EntryType::DirectoryDynamic,
                    "dynamic-dir",
                    br#"entries:
  - name: child
    factory: test-dir
    config:
      name: child
"#
                    .to_vec(),
                    Some(mtime),
                )
                .await
            };

            _ = result.map_err(|e| anyhow::anyhow!("Failed to create dynamic directory: {}", e))?;
            _ = tx
                .commit()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to commit transaction: {}", e))?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_list_single_file() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create test file in pond
        setup
            .create_pond_file(
                "test.txt",
                "hello world",
                tinyfs::EntryType::FilePhysicalVersion,
            )
            .await
            .expect("Failed to create pond file");

        let mut results = Vec::new();
        list_command(&setup.ship_context, "test.txt", false, false, |output| {
            results.push(output.to_string())
        })
        .await
        .expect("List command failed");

        assert_eq!(results.len(), 1);
        assert!(results[0].contains("test.txt"));
        assert!(results[0].contains("[FILE]")); // FileData emoji
    }

    #[tokio::test]
    async fn test_list_multiple_files() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create multiple test files in pond
        setup
            .create_pond_file(
                "file1.txt",
                "content1",
                tinyfs::EntryType::FilePhysicalVersion,
            )
            .await
            .expect("Failed to create pond file1");
        setup
            .create_pond_file(
                "file2.csv",
                "header,value\nrow,1",
                tinyfs::EntryType::TablePhysicalVersion,
            )
            .await
            .expect("Failed to create pond file2");
        setup
            .create_pond_file(
                "file3.parquet",
                "data file",
                tinyfs::EntryType::FilePhysicalVersion,
            )
            .await
            .expect("Failed to create pond file3");

        // First, let's try listing all files to see what's actually there
        let mut all_results = Vec::new();
        list_command(&setup.ship_context, "*", false, false, |output| {
            all_results.push(output.to_string())
        })
        .await
        .expect("List all command failed");

        debug!("All files found with '*': {:?}", all_results);

        let mut results = Vec::new();
        list_command(&setup.ship_context, "file*", false, false, |output| {
            results.push(output.to_string())
        })
        .await
        .expect("List command failed");

        debug!("Files found with 'file*': {:?}", results);

        assert_eq!(results.len(), 3);

        // Results should be sorted by path
        assert!(results[0].contains("file1.txt"));
        assert!(results[1].contains("file2.csv"));
        assert!(results[2].contains("file3.parquet"));
    }

    #[tokio::test]
    async fn test_list_directory() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create directory and file in pond
        setup
            .create_pond_directory("testdir")
            .await
            .expect("Failed to create pond directory");
        setup
            .create_pond_file(
                "testdir/nested.txt",
                "nested content",
                tinyfs::EntryType::FilePhysicalVersion,
            )
            .await
            .expect("Failed to create nested pond file");

        let mut results = Vec::new();
        list_command(&setup.ship_context, "testdir", false, false, |output| {
            results.push(output.to_string())
        })
        .await
        .expect("List command failed");

        assert_eq!(results.len(), 1);
        assert!(results[0].contains("testdir"));
        assert!(results[0].contains("[DIR]")); // Directory emoji
    }

    #[tokio::test]
    async fn test_list_long_preserves_dynamic_directory_mtime() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");
        setup
            .create_pond_dynamic_directory("analysis", 1_785_725_641_564_536)
            .await
            .expect("Failed to create dynamic directory");

        let mut results = Vec::new();
        list_command(&setup.ship_context, "analysis", false, true, |output| {
            results.push(output.to_string())
        })
        .await
        .expect("List command failed");

        assert_eq!(results.len(), 1);
        assert!(results[0].contains("dynamic-directory"));
        assert!(results[0].contains("2026-08-03 02:54:01"));
        assert!(!results[0].contains("1970-01-01 00:00:00"));
    }

    #[tokio::test]
    async fn test_list_recursive_pattern() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create nested directory structure - need to create parent directories first
        setup
            .create_pond_directory("dir1")
            .await
            .expect("Failed to create dir1");
        setup
            .create_pond_directory("dir1/subdir")
            .await
            .expect("Failed to create dir1/subdir");
        setup
            .create_pond_file(
                "dir1/subdir/file1.txt",
                "content1",
                tinyfs::EntryType::FilePhysicalVersion,
            )
            .await
            .expect("Failed to create nested pond file1");

        setup
            .create_pond_directory("dir2")
            .await
            .expect("Failed to create dir2");
        setup
            .create_pond_file(
                "dir2/file2.txt",
                "content2",
                tinyfs::EntryType::FilePhysicalVersion,
            )
            .await
            .expect("Failed to create nested pond file2");

        setup
            .create_pond_file(
                "file3.txt",
                "content3",
                tinyfs::EntryType::FilePhysicalVersion,
            )
            .await
            .expect("Failed to create root pond file");

        let mut results = Vec::new();
        list_command(&setup.ship_context, "**/*.txt", false, false, |output| {
            results.push(output.to_string())
        })
        .await
        .expect("List command failed");

        assert_eq!(results.len(), 3);
        assert!(results.iter().any(|r| r.contains("dir1/subdir/file1.txt")));
        assert!(results.iter().any(|r| r.contains("dir2/file2.txt")));
        assert!(results.iter().any(|r| r.contains("file3.txt")));
    }

    #[tokio::test]
    async fn test_list_hidden_files() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create hidden and regular files
        setup
            .create_pond_file(
                ".hidden.txt",
                "hidden content",
                tinyfs::EntryType::FilePhysicalVersion,
            )
            .await
            .expect("Failed to create hidden pond file");
        setup
            .create_pond_file(
                "visible.txt",
                "visible content",
                tinyfs::EntryType::FilePhysicalVersion,
            )
            .await
            .expect("Failed to create visible pond file");

        // Test without show_all - should only see visible file
        let mut results = Vec::new();
        list_command(&setup.ship_context, "*", false, false, |output| {
            results.push(output.to_string())
        })
        .await
        .expect("List command failed");

        assert_eq!(results.len(), 1);
        assert!(results[0].contains("visible.txt"));

        // Test with show_all - should see both files
        let mut results_all = Vec::new();
        list_command(&setup.ship_context, "*", true, false, |output| {
            results_all.push(output.to_string())
        })
        .await
        .expect("List command with show_all failed");

        assert_eq!(results_all.len(), 2);
        assert!(results_all.iter().any(|r| r.contains(".hidden.txt")));
        assert!(results_all.iter().any(|r| r.contains("visible.txt")));
    }

    #[tokio::test]
    async fn test_list_no_matches() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create a file that won't match the pattern
        setup
            .create_pond_file(
                "file.txt",
                "content",
                tinyfs::EntryType::FilePhysicalVersion,
            )
            .await
            .expect("Failed to create pond file");

        let mut results = Vec::new();
        list_command(
            &setup.ship_context,
            "*.nonexistent",
            false,
            false,
            |output| results.push(output.to_string()),
        )
        .await
        .expect("List command failed");

        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_normalize_pattern() {
        // "/" should become "/*" (list root contents)
        assert_eq!(normalize_pattern("/"), "/*");

        // Trailing slash should append "*"
        assert_eq!(normalize_pattern("/data/"), "/data/*");
        assert_eq!(normalize_pattern("subdir/"), "subdir/*");

        // Normal patterns should be unchanged
        assert_eq!(normalize_pattern("*"), "*");
        assert_eq!(normalize_pattern("**/*"), "**/*");
        assert_eq!(normalize_pattern("/data/*.csv"), "/data/*.csv");
        assert_eq!(normalize_pattern("file.txt"), "file.txt");

        // Whitespace should be trimmed
        assert_eq!(normalize_pattern("  /  "), "/*");
        assert_eq!(normalize_pattern(" /data/ "), "/data/*");
    }

    #[tokio::test]
    async fn test_list_root_with_slash() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create files and directories at root level
        setup
            .create_pond_file(
                "rootfile.txt",
                "content",
                tinyfs::EntryType::FilePhysicalVersion,
            )
            .await
            .expect("Failed to create root file");

        setup
            .create_pond_directory("mydir")
            .await
            .expect("Failed to create directory");

        setup
            .create_pond_file(
                "mydir/nested.txt",
                "nested content",
                tinyfs::EntryType::FilePhysicalVersion,
            )
            .await
            .expect("Failed to create nested file");

        // Test "pond list /" - should list root entries only (not recurse)
        let mut results = Vec::new();
        list_command(&setup.ship_context, "/", false, false, |output| {
            results.push(output.to_string())
        })
        .await
        .expect("List with '/' should succeed");

        debug!("Results for '/': {:?}", results);

        // Should see root file and directory, but NOT nested file
        assert!(results.iter().any(|r| r.contains("rootfile.txt")));
        assert!(results.iter().any(|r| r.contains("mydir")));
        assert!(!results.iter().any(|r| r.contains("nested.txt")));
    }

    #[tokio::test]
    async fn test_list_trailing_slash() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        // Create directory structure
        setup
            .create_pond_directory("data")
            .await
            .expect("Failed to create directory");

        setup
            .create_pond_file(
                "data/file1.csv",
                "content1",
                tinyfs::EntryType::FilePhysicalVersion,
            )
            .await
            .expect("Failed to create file");

        setup
            .create_pond_file(
                "data/file2.csv",
                "content2",
                tinyfs::EntryType::FilePhysicalVersion,
            )
            .await
            .expect("Failed to create file");

        // Test "pond list /data/" - should list directory contents
        let mut results = Vec::new();
        list_command(&setup.ship_context, "/data/", false, false, |output| {
            results.push(output.to_string())
        })
        .await
        .expect("List with trailing slash should succeed");

        debug!("Results for '/data/': {:?}", results);

        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|r| r.contains("file1.csv")));
        assert!(results.iter().any(|r| r.contains("file2.csv")));
    }
}
