// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use super::super::memory::new_fs;
use crate::error::Error;
use crate::path::normalize;
use crate::path::strip_root;

use std::path::PathBuf;

use crate::async_helpers::convenience;

#[tokio::test]
async fn test_pending_file_metadata_does_not_deadlock() {
    use crate::EntryType;
    use crate::node::{FileID, NodeType, PartID, local_pond_uuid};
    use crate::persistence::PersistenceLayer;
    use std::time::Duration;

    let persistence = crate::memory::MemoryPersistence::default();
    let id = FileID::new_in_partition(
        PartID::root(),
        EntryType::FilePhysicalVersion,
        local_pond_uuid(),
    );
    let node = persistence.create_file_node(id).await.unwrap();
    persistence.store_node(&node).await.unwrap();

    let file = match &node.node_type {
        NodeType::File(file) => file,
        _ => panic!("expected file"),
    };
    let _writer = file.async_writer().await.unwrap();

    let metadata = tokio::time::timeout(Duration::from_millis(250), persistence.metadata(id))
        .await
        .expect("metadata lookup deadlocked")
        .unwrap();
    assert_eq!(metadata.version, 0);
    assert_eq!(metadata.size, Some(0));
    assert_eq!(metadata.entry_type, EntryType::FilePhysicalVersion);
}

#[tokio::test]
async fn test_memory_writer_propagates_store_failure_and_recovers() {
    use crate::EntryType;
    use crate::node::{FileID, NodeType, PartID, local_pond_uuid};
    use crate::persistence::PersistenceLayer;
    use tokio::io::AsyncWriteExt;

    let persistence = crate::memory::MemoryPersistence::default();
    let id = FileID::new_in_partition(
        PartID::root(),
        EntryType::FilePhysicalVersion,
        local_pond_uuid(),
    );
    let node = persistence.create_file_node(id).await.unwrap();
    persistence.store_node(&node).await.unwrap();
    let file = match &node.node_type {
        NodeType::File(file) => file,
        _ => panic!("expected file"),
    };

    persistence.fail_next_store();
    let mut writer = file.async_writer().await.unwrap();
    writer.write_all(b"not persisted").await.unwrap();
    let error = writer.shutdown().await.unwrap_err();
    assert!(error.to_string().contains("injected memory persistence"));
    assert!(
        writer
            .shutdown()
            .await
            .unwrap_err()
            .to_string()
            .contains("injected")
    );
    assert!(persistence.list_file_versions(id).await.unwrap().is_empty());
    drop(writer);

    let mut retry = file.async_writer().await.unwrap();
    retry.write_all(b"persisted").await.unwrap();
    retry.shutdown().await.unwrap();

    let versions = persistence.list_file_versions(id).await.unwrap();
    assert_eq!(versions.len(), 1);
    assert_eq!(
        persistence
            .read_file_version(id, versions[0].version)
            .await
            .unwrap(),
        b"persisted"
    );
}

#[tokio::test]
async fn test_memory_file_version_timestamps_are_microseconds() {
    use crate::EntryType;
    use crate::node::{FileID, PartID, local_pond_uuid};
    use crate::persistence::PersistenceLayer;

    let persistence = crate::memory::MemoryPersistence::default();
    let id = FileID::new_in_partition(
        PartID::root(),
        EntryType::FilePhysicalVersion,
        local_pond_uuid(),
    );
    let before = chrono::Utc::now().timestamp_micros();
    persistence
        .store_file_version(id, 1, b"content".to_vec())
        .await
        .unwrap();
    let after = chrono::Utc::now().timestamp_micros();

    let versions = persistence.list_file_versions(id).await.unwrap();
    assert_eq!(versions.len(), 1);
    assert!((before..=after).contains(&versions[0].timestamp));
}

#[tokio::test]
async fn test_create_file() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create a file in the root directory
    _ = convenience::create_file_path(&root, "/newfile", b"content")
        .await
        .unwrap();

    let content = root.read_file_path_to_vec("/newfile").await.unwrap();

    assert_eq!(content, b"content");
}

#[tokio::test]
async fn test_create_symlink() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create a file
    _ = convenience::create_file_path(&root, "/targetfile", b"target content")
        .await
        .unwrap();

    // Create a symlink to the file
    _ = root
        .create_symlink_path("/linkfile", "/targetfile")
        .await
        .unwrap();
}

#[tokio::test]
async fn test_follow_symlink() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create a file
    _ = convenience::create_file_path(&root, "/targetfile", b"target content")
        .await
        .unwrap();

    // Create a symlink to the file
    _ = root
        .create_symlink_path("/linkfile", "/targetfile")
        .await
        .unwrap();

    // Follow the symlink and verify it reaches the target
    let content = root.read_file_path_to_vec("/linkfile").await.unwrap();
    assert_eq!(content, b"target content");
}

#[tokio::test]
async fn test_normalize() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();
    let a_node = root.create_dir_path("/a").await.unwrap();
    let b_node = root.create_dir_path("/a/b").await.unwrap();

    // Create node stack with actual NodeRefs
    let node_stack = [root.node_path(), a_node.node_path(), b_node.node_path()];

    // Test 1: ../a/../b should normalize to "b" with the a_node as parent
    let (stacklen, path) = normalize("../a/../b", &node_stack).unwrap();
    assert_eq!(stacklen, 2);
    assert_eq!(path, PathBuf::from("b"));

    // Test 2: Multiple parent dirs
    let (stacklen, path) = normalize("../../file.txt", &node_stack).unwrap();
    assert_eq!(stacklen, 1);
    assert_eq!(path, PathBuf::from("file.txt"));

    // Test 3: Current dir components should be ignored
    let (stacklen, path) = normalize("./a/./b", &node_stack).unwrap();
    assert_eq!(stacklen, 3);
    assert_eq!(path, PathBuf::from("a/b"));

    // Test 4: Too many parent dirs should fail
    let result = normalize("../../../too-far", &node_stack);
    assert_eq!(result, Err(Error::parent_path_invalid("../../../too-far")));

    // Test 5: No parent dirs means use current node
    let (stacklen, path) = normalize("just/a/path", &node_stack).unwrap();
    assert_eq!(stacklen, 3);
    assert_eq!(path, PathBuf::from("just/a/path"));
}

#[tokio::test]
async fn test_relative_symlink() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create directories
    _ = root.create_dir_path("/a").await.unwrap();
    _ = root.create_dir_path("/c").await.unwrap();

    // Create the target file
    _ = convenience::create_file_path(&root, "/c/d", b"relative symlink target")
        .await
        .unwrap();

    // Create a symlink with a relative path
    _ = root.create_symlink_path("/a/b", "../c/d").await.unwrap();
    _ = root.create_symlink_path("/a/e", "/c/d").await.unwrap();

    // Follow the symlink and verify it reaches the target
    let content = root.read_file_path_to_vec("/a/b").await.unwrap();
    assert_eq!(content, b"relative symlink target");

    // Open directory "/a" directly
    let wd_a = root.open_dir_path("/a").await.unwrap();

    // Attempting to resolve "b" from within "/a" should fail
    // because the symlink target "../c/d" requires backtracking
    let result = wd_a.read_file_path_to_vec("b").await;
    assert_eq!(result, Err(Error::parent_path_invalid("../c/d")));

    // Absolute symlink resolves via the effective root (which is the
    // actual root, inherited from the root WD).
    let content = wd_a.read_file_path_to_vec("e").await.unwrap();
    assert_eq!(content, b"relative symlink target");
}

#[tokio::test]
async fn test_open_dir_path() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create a directory and a file
    let _ = root.create_dir_path("/testdir").await.unwrap();
    _ = convenience::create_file_path(&root, "/testfile", b"content")
        .await
        .unwrap();

    // Successfully open a directory
    let wd = root.open_dir_path("/testdir").await.unwrap();

    // Create a file inside the opened directory
    _ = convenience::create_file_path(&wd, "file_in_dir", b"inner content")
        .await
        .unwrap();

    // Verify we can read the file through the original path
    let content = root
        .read_file_path_to_vec("/testdir/file_in_dir")
        .await
        .unwrap();
    assert_eq!(content, b"inner content");

    // Trying to open a file as directory should fail
    assert_eq!(
        root.open_dir_path("/testfile").await,
        Err(Error::not_a_directory("/testfile"))
    );

    // Trying to open a non-existent path should fail
    assert_eq!(
        root.open_dir_path("/nonexistent").await,
        Err(Error::not_found("/nonexistent"))
    );
}

#[tokio::test]
async fn test_symlink_loop() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create directories to work with
    _ = root.create_dir_path("/dir1").await.unwrap();
    _ = root.create_dir_path("/dir2").await.unwrap();

    // Create a circular symlink reference:
    // /dir1/link1 -> /dir2/link2
    // /dir2/link2 -> /dir1/link1
    _ = root
        .create_symlink_path("/dir1/link1", "../dir2/link2")
        .await
        .unwrap();
    _ = root
        .create_symlink_path("/dir2/link2", "../dir1/link1")
        .await
        .unwrap();

    // Attempt to access through the symlink loop
    let result = root.read_file_path_to_vec("/dir1/link1").await;

    // Verify we get a SymlinkLoop error
    assert_eq!(result, Err(Error::symlink_loop("../dir2/link2")));

    // Test a more complex loop
    _ = root.create_dir_path("/loop").await.unwrap();
    _ = root
        .create_symlink_path("/loop/a", "/loop/b")
        .await
        .unwrap();
    _ = root
        .create_symlink_path("/loop/b", "/loop/c")
        .await
        .unwrap();
    _ = root
        .create_symlink_path("/loop/c", "/loop/d")
        .await
        .unwrap();
    _ = root
        .create_symlink_path("/loop/d", "/loop/e")
        .await
        .unwrap();
    _ = root
        .create_symlink_path("/loop/e", "/loop/f")
        .await
        .unwrap();
    _ = root
        .create_symlink_path("/loop/f", "/loop/g")
        .await
        .unwrap();
    _ = root
        .create_symlink_path("/loop/g", "/loop/h")
        .await
        .unwrap();
    _ = root
        .create_symlink_path("/loop/h", "/loop/i")
        .await
        .unwrap();
    _ = root
        .create_symlink_path("/loop/i", "/loop/j")
        .await
        .unwrap();
    _ = root
        .create_symlink_path("/loop/j", "/loop/a")
        .await
        .unwrap();

    // This should exceed the SYMLINK_LOOP_LIMIT (10)
    let result = root.read_file_path_to_vec("/loop/a").await;
    assert_eq!(result, Err(Error::symlink_loop("/loop/b")));
}

#[tokio::test]
async fn test_symlink_to_nonexistent() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create a symlink pointing to a non-existent target
    _ = root
        .create_symlink_path("/broken_link", "/nonexistent_target")
        .await
        .unwrap();

    // Attempt to follow the symlink
    let result = root.read_file_path_to_vec("/broken_link").await;

    // Should fail with NotFound error
    assert_eq!(result, Err(Error::not_found("/nonexistent_target")));

    // Test with relative path to non-existent target
    _ = root.create_dir_path("/dir").await.unwrap();
    _ = root
        .create_symlink_path("/dir/broken_rel", "../nonexistent_file")
        .await
        .unwrap();

    let result = root.read_file_path_to_vec("/dir/broken_rel").await;
    assert_eq!(result, Err(Error::not_found("../nonexistent_file")));

    // Test with a chain of symlinks where the last one is broken
    _ = root.create_symlink_path("/link1", "/link2").await.unwrap();
    _ = root
        .create_symlink_path("/link2", "/nonexistent_file")
        .await
        .unwrap();

    let result = root.read_file_path_to_vec("/link1").await;
    assert_eq!(result, Err(Error::not_found("/nonexistent_file")));
}

#[tokio::test]
async fn test_strip_root() {
    // Test with absolute path
    let path = PathBuf::from("/a/b/c");
    let stripped = strip_root(path);
    assert_eq!(stripped, PathBuf::from("a/b/c"));

    // Test with relative path (should remain unchanged)
    let path = PathBuf::from("a/b/c");
    let stripped = strip_root(path);
    assert_eq!(stripped, PathBuf::from("a/b/c"));

    // Test with multiple root components
    let path = PathBuf::from("//a/b");
    let stripped = strip_root(path);
    assert_eq!(stripped, PathBuf::from("a/b"));

    // Test with just a root component
    let path = PathBuf::from("/");
    let stripped = strip_root(path);
    assert_eq!(stripped, PathBuf::from(""));
}

#[tokio::test]
async fn test_visit_glob_matching() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create test directory structure
    _ = root.create_dir_path("/a").await.unwrap();
    _ = root.create_dir_path("/a/b").await.unwrap();
    _ = root.create_dir_path("/a/b/c").await.unwrap();
    _ = root.create_dir_path("/a/d").await.unwrap();
    _ = convenience::create_file_path(&root, "/a/file1.txt", b"content1")
        .await
        .unwrap();
    _ = convenience::create_file_path(&root, "/a/file2.txt", b"content2")
        .await
        .unwrap();
    _ = convenience::create_file_path(&root, "/a/other.dat", b"data")
        .await
        .unwrap();
    _ = convenience::create_file_path(&root, "/a/b/file3.txt", b"content3")
        .await
        .unwrap();
    _ = convenience::create_file_path(&root, "/a/b/c/file4.txt", b"content4")
        .await
        .unwrap();
    _ = convenience::create_file_path(&root, "/a/d/file5.txt", b"content5")
        .await
        .unwrap();

    // Test case 1: Simple direct match
    let mut visitor = FileContentVisitor::new();
    _ = root
        .visit_with_visitor("/a/file1.txt", &mut visitor)
        .await
        .unwrap();
    assert_eq!(visitor.contents, vec![b"content1"]);

    // Test case 2: Multiple match
    let mut visitor = FileContentVisitor::new();
    _ = root
        .visit_with_visitor("/a/file*.txt", &mut visitor)
        .await
        .unwrap();
    assert_eq!(visitor.contents, vec![b"content1", b"content2"]);

    // Test case 3: Multiple ** match
    let mut visitor = FileContentVisitor::new();
    _ = root
        .visit_with_visitor("/**/*.txt", &mut visitor)
        .await
        .unwrap();
    // Convert to sets for order-independent comparison
    let actual_set: std::collections::HashSet<_> = visitor.contents.into_iter().collect();
    let expected_set: std::collections::HashSet<_> = vec![
        b"content1".to_vec(),
        b"content2".to_vec(),
        b"content3".to_vec(),
        b"content4".to_vec(),
        b"content5".to_vec(),
    ]
    .into_iter()
    .collect();
    assert_eq!(actual_set, expected_set);

    // Test case 4: Single ** match
    let mut visitor = FileContentVisitor::new();
    _ = root
        .visit_with_visitor("/**/file4.txt", &mut visitor)
        .await
        .unwrap();
    assert_eq!(visitor.contents, vec![b"content4"]);

    // Test case 5: Single ** match
    let mut visitor = FileContentVisitor::new();
    _ = root
        .visit_with_visitor("/*/*.dat", &mut visitor)
        .await
        .unwrap();
    assert_eq!(visitor.contents, vec![b"data"]);
}

/// Visitor for collecting file contents
struct FileContentVisitor {
    contents: Vec<Vec<u8>>,
}

impl FileContentVisitor {
    fn new() -> Self {
        Self {
            contents: Vec::new(),
        }
    }
}

#[tokio::test]
async fn test_create_dynamic_file_path() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create parent directory
    _ = root.create_dir_path("/config").await.unwrap();

    // Create a dynamic file using the path-based API
    let config_content = b"factory_type: test\nvalue: 42";

    let dynamic_node = root
        .create_dynamic_path(
            "/config/test.yaml",
            crate::EntryType::FileDynamic,
            "test-factory",
            config_content.to_vec(),
        )
        .await
        .unwrap();

    // Verify the node was created
    assert_eq!(
        dynamic_node.id().entry_type(),
        crate::EntryType::FileDynamic
    );
    assert!(matches!(
        &dynamic_node.node.node_type,
        crate::NodeType::File(_)
    ));
    assert_eq!(
        dynamic_node.node.node_type.entry_type().await.unwrap(),
        crate::EntryType::FileDynamic
    );

    // Verify we can resolve it
    let (_, lookup) = root.resolve_path("/config/test.yaml").await.unwrap();
    match lookup {
        crate::Lookup::Found(node_path) => {
            assert_eq!(node_path.id().entry_type(), crate::EntryType::FileDynamic);
            assert_eq!(node_path.path(), std::path::Path::new("/config/test.yaml"));
        }
        _ => panic!("Expected to find the dynamic file"),
    }
}

#[tokio::test]
async fn test_create_dynamic_directory_path() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create a dynamic directory using the path-based API
    let config_content = b"query: SELECT * FROM data";

    let dynamic_node = root
        .create_dynamic_path(
            "/virtual_data",
            crate::EntryType::DirectoryDynamic,
            "sql-derived",
            config_content.to_vec(),
        )
        .await
        .unwrap();

    // Verify the node was created
    assert_eq!(
        dynamic_node.id().entry_type(),
        crate::EntryType::DirectoryDynamic
    );
    assert!(matches!(
        &dynamic_node.node.node_type,
        crate::NodeType::Directory(_)
    ));
    assert_eq!(
        dynamic_node.node.node_type.entry_type().await.unwrap(),
        crate::EntryType::DirectoryDynamic
    );

    // Verify we can resolve it
    let (_, lookup) = root.resolve_path("/virtual_data").await.unwrap();
    match lookup {
        crate::Lookup::Found(node_path) => {
            assert_eq!(
                node_path.id().entry_type(),
                crate::EntryType::DirectoryDynamic
            );
            assert_eq!(node_path.path(), std::path::Path::new("/virtual_data"));
        }
        _ => panic!("Expected to find the dynamic directory"),
    }
}

/// Test FilePhysicalSeries version concatenation at the MemoryFile level
/// This verifies that when entry_type is FilePhysicalSeries, async_reader()
/// returns a ChainedReader that concatenates all versions oldest-to-newest.
#[tokio::test]
async fn test_memory_file_physical_series_version_concatenation() {
    use crate::EntryType;
    use crate::file::File;
    use crate::memory::{MemoryFile, MemoryPersistence};
    use crate::node::{FileID, PartID, local_pond_uuid};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Create persistence layer
    let persistence = MemoryPersistence::default();
    let id = FileID::new_in_partition(
        PartID::root(),
        EntryType::FilePhysicalSeries,
        local_pond_uuid(),
    );

    // Create MemoryFile with FilePhysicalSeries entry type
    let memory_file = MemoryFile::new(id, persistence.clone(), EntryType::FilePhysicalSeries);

    // Write multiple versions using async_writer - these represent appended data
    {
        let mut writer = memory_file.async_writer().await.unwrap();
        writer.write_all(b"First line\n").await.unwrap();
        writer.shutdown().await.unwrap();
    }
    {
        let mut writer = memory_file.async_writer().await.unwrap();
        writer.write_all(b"Second line\n").await.unwrap();
        writer.shutdown().await.unwrap();
    }
    {
        let mut writer = memory_file.async_writer().await.unwrap();
        writer.write_all(b"Third line\n").await.unwrap();
        writer.shutdown().await.unwrap();
    }

    // Read via async_reader - should concatenate all versions
    let mut reader = memory_file.async_reader().await.unwrap();
    let mut content = String::new();
    let _ = reader.read_to_string(&mut content).await.unwrap();

    // Verify versions are concatenated oldest-to-newest
    assert_eq!(content, "First line\nSecond line\nThird line\n");
}

/// Test that FilePhysicalVersion entry type reads only current content (not versions)
#[tokio::test]
async fn test_memory_file_physical_version_single_content() {
    use crate::EntryType;
    use crate::file::File;
    use crate::memory::{MemoryFile, MemoryPersistence};
    use crate::node::{FileID, PartID, local_pond_uuid};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Create persistence layer
    let persistence = MemoryPersistence::default();
    let id = FileID::new_in_partition(
        PartID::root(),
        EntryType::FilePhysicalVersion,
        local_pond_uuid(),
    );

    // Create MemoryFile with FilePhysicalVersion entry type
    let memory_file = MemoryFile::new(id, persistence.clone(), EntryType::FilePhysicalVersion);

    // Write two versions using async_writer
    {
        let mut writer = memory_file.async_writer().await.unwrap();
        writer.write_all(b"Version 1").await.unwrap();
        writer.shutdown().await.unwrap();
    }
    {
        let mut writer = memory_file.async_writer().await.unwrap();
        writer.write_all(b"Version 2").await.unwrap();
        writer.shutdown().await.unwrap();
    }

    // Read via async_reader - FilePhysicalVersion returns latest content (not concatenated)
    let mut reader = memory_file.async_reader().await.unwrap();
    let mut content = Vec::new();
    let _ = reader.read_to_end(&mut content).await.unwrap();

    // FilePhysicalVersion reads latest version's content
    assert_eq!(content, b"Version 2");
}

/// Test FilePhysicalSeries using high-level async_writer API with automatic version allocation
#[tokio::test]
async fn test_memory_file_series_async_writer_with_versions() {
    use crate::EntryType;
    use crate::FS;
    use crate::memory::MemoryPersistence;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Create filesystem with memory persistence
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await.unwrap();
    let root = fs.root().await.unwrap();

    // Write 3 versions using proper tinyfs API (async_writer_path_with_type)
    // This API creates the file on first call and adds versions on subsequent calls
    let file_path = "test.series";

    // Write first version
    {
        let mut writer = root
            .async_writer_path_with_type(file_path, EntryType::FilePhysicalSeries)
            .await
            .unwrap();
        writer.write_all(b"First line\n").await.unwrap();
        writer.shutdown().await.unwrap();
    }

    // Write second version
    {
        let mut writer = root
            .async_writer_path_with_type(file_path, EntryType::FilePhysicalSeries)
            .await
            .unwrap();
        writer.write_all(b"Second line\n").await.unwrap();
        writer.shutdown().await.unwrap();
    }

    // Write third version
    {
        let mut writer = root
            .async_writer_path_with_type(file_path, EntryType::FilePhysicalSeries)
            .await
            .unwrap();
        writer.write_all(b"Third line\n").await.unwrap();
        writer.shutdown().await.unwrap();
    }

    // Read via async_reader - should concatenate all versions
    let mut reader = root.async_reader_path(file_path).await.unwrap();
    let mut content = String::new();
    let _ = reader.read_to_string(&mut content).await.unwrap();

    // Verify versions are concatenated oldest-to-newest
    assert_eq!(content, "First line\nSecond line\nThird line\n");

    // Verify bao_outboard was computed and is available in metadata
    let (_, lookup) = root.resolve_path(file_path).await.unwrap();
    match lookup {
        crate::Lookup::Found(node_path) => {
            let file_node = node_path.into_file().await.unwrap();
            let metadata = file_node.handle.metadata().await.unwrap();
            assert!(
                metadata.bao_outboard.is_some(),
                "Latest version should have bao_outboard"
            );
            assert_eq!(metadata.version, 3, "Should be at version 3");
        }
        _ => panic!("File should exist"),
    }
}

#[tokio::test]
async fn test_memory_persistence_version_range_and_reader() {
    use crate::memory::MemoryPersistence;
    use crate::node::PartID;
    use crate::{EntryType, FileID, PersistenceLayer};
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let persistence = MemoryPersistence::default();
    let id = FileID::new_in_partition(
        PartID::root(),
        EntryType::FilePhysicalVersion,
        crate::local_pond_uuid(),
    );
    persistence
        .store_file_version(id, 1, b"0123456789".to_vec())
        .await
        .unwrap();

    assert_eq!(
        persistence
            .read_file_version_range(id, 1, 3..7)
            .await
            .unwrap(),
        b"3456".as_slice()
    );
    assert_eq!(
        persistence
            .read_file_version_range(id, 1, 8..20)
            .await
            .unwrap(),
        b"89".as_slice()
    );

    let mut reader = persistence.open_file_version(id, 1).await.unwrap();
    _ = reader.seek(std::io::SeekFrom::Start(5)).await.unwrap();
    let mut tail = Vec::new();
    _ = reader.read_to_end(&mut tail).await.unwrap();
    assert_eq!(tail, b"56789");
}

#[async_trait::async_trait]
impl crate::wd::Visitor<Vec<u8>> for FileContentVisitor {
    async fn visit(
        &mut self,
        node: crate::node::NodePath,
        _captured: &[String],
    ) -> crate::error::Result<Vec<u8>> {
        let file_node = node.as_file().await?;
        let reader = file_node.async_reader().await?;
        let content = crate::async_helpers::buffer_helpers::read_all_to_vec(reader)
            .await
            .map_err(|e| Error::Other(format!("Failed to read file content: {}", e)))?;
        self.contents.push(content.clone());
        Ok(content)
    }
}
