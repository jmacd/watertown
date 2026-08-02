// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Tests for the hostmount persistence layer and FS/WD integration.

use crate::hostmount::HostmountPersistence;
use crate::node::FileID;
use crate::persistence::PersistenceLayer;
use crate::{EntryType, FS, NodeType};
use futures::StreamExt;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Create a temp directory with some test content
fn create_test_tree() -> TempDir {
    let dir = TempDir::new().expect("create temp dir");

    // Create files
    std::fs::write(dir.path().join("hello.txt"), b"Hello, World!").unwrap();
    std::fs::write(dir.path().join("data.bin"), b"\x00\x01\x02\x03").unwrap();

    // Create subdirectory with files
    std::fs::create_dir(dir.path().join("subdir")).unwrap();
    std::fs::write(dir.path().join("subdir/nested.txt"), b"nested content").unwrap();

    // Create a dotfile (should be hidden by entries())
    std::fs::write(dir.path().join(".hidden"), b"secret").unwrap();

    dir
}

#[tokio::test]
async fn test_persistence_new_valid_directory() {
    let dir = create_test_tree();
    let persistence = HostmountPersistence::new(dir.path().to_path_buf());
    assert!(persistence.is_ok());
}

#[tokio::test]
async fn test_persistence_new_nonexistent() {
    let result = HostmountPersistence::new("/nonexistent/path/foo".into());
    assert!(result.is_err());
}

#[tokio::test]
async fn test_persistence_new_file_not_dir() {
    let dir = create_test_tree();
    let result = HostmountPersistence::new(dir.path().join("hello.txt"));
    assert!(result.is_err());
}

#[tokio::test]
async fn test_load_root_node() {
    let dir = create_test_tree();
    let persistence = HostmountPersistence::new(dir.path().to_path_buf()).unwrap();
    let root = persistence.load_node(FileID::root()).await.unwrap();
    assert!(matches!(root.node_type, NodeType::Directory(_)));
    assert_eq!(root.id, FileID::root());
}

#[tokio::test]
async fn test_load_unknown_id_errors() {
    let dir = create_test_tree();
    let persistence = HostmountPersistence::new(dir.path().to_path_buf()).unwrap();
    let fake_id = FileID::new_in_partition(
        crate::node::PartID::root(),
        EntryType::FilePhysicalVersion,
        crate::local_pond_uuid(),
    );
    let result = persistence.load_node(fake_id).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_metadata_root() {
    let dir = create_test_tree();
    let persistence = HostmountPersistence::new(dir.path().to_path_buf()).unwrap();
    let meta = persistence.metadata(FileID::root()).await.unwrap();
    assert_eq!(meta.entry_type, EntryType::DirectoryPhysical);
    assert!(meta.size.is_none()); // directories have no size
}

#[tokio::test]
async fn test_symlinks_not_supported() {
    let dir = create_test_tree();
    let persistence = HostmountPersistence::new(dir.path().to_path_buf()).unwrap();
    let result = persistence
        .create_symlink_node(FileID::root(), std::path::Path::new("/tmp"), None)
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_dynamic_nodes_not_supported() {
    let dir = create_test_tree();
    let persistence = HostmountPersistence::new(dir.path().to_path_buf()).unwrap();
    let result = persistence
        .create_dynamic_node(FileID::root(), "test", vec![], None)
        .await;
    assert!(result.is_err());
}

// --- FS + WD integration tests ---------------------------------------

async fn create_test_fs() -> (TempDir, FS) {
    let dir = create_test_tree();
    let persistence = HostmountPersistence::new(dir.path().to_path_buf()).unwrap();
    let fs = FS::new(persistence).await.unwrap();
    (dir, fs)
}

#[tokio::test]
async fn test_fs_root() {
    let (_dir, fs) = create_test_fs().await;
    let root = fs.root().await;
    assert!(root.is_ok());
}

#[tokio::test]
async fn test_list_root_entries() {
    let (_dir, fs) = create_test_fs().await;
    let root = fs.root().await.unwrap();

    let entries: Vec<_> = {
        let mut stream = root.entries().await.unwrap();
        let mut items = Vec::new();
        while let Some(entry) = stream.next().await {
            items.push(entry.unwrap());
        }
        items
    };

    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();

    // Should include visible files/dirs, sorted
    assert!(
        names.contains(&"hello.txt"),
        "expected hello.txt, got {:?}",
        names
    );
    assert!(
        names.contains(&"data.bin"),
        "expected data.bin, got {:?}",
        names
    );
    assert!(
        names.contains(&"subdir"),
        "expected subdir, got {:?}",
        names
    );

    // Should NOT include dotfiles
    assert!(
        !names.contains(&".hidden"),
        "dotfile .hidden should be excluded"
    );
}

#[tokio::test]
async fn test_read_file() {
    let (_dir, fs) = create_test_fs().await;
    let root = fs.root().await.unwrap();

    let mut contents = Vec::new();
    let mut reader = root.async_reader_path("hello.txt").await.unwrap();
    let _ = reader.read_to_end(&mut contents).await.unwrap();

    assert_eq!(contents, b"Hello, World!");
}

#[tokio::test]
async fn test_read_nested_file() {
    let (_dir, fs) = create_test_fs().await;
    let root = fs.root().await.unwrap();

    let mut contents = Vec::new();
    let mut reader = root.async_reader_path("subdir/nested.txt").await.unwrap();
    let _ = reader.read_to_end(&mut contents).await.unwrap();

    assert_eq!(contents, b"nested content");
}

#[tokio::test]
async fn test_read_file_to_vec() {
    let (_dir, fs) = create_test_fs().await;
    let root = fs.root().await.unwrap();

    let contents = root.read_file_path_to_vec("hello.txt").await.unwrap();
    assert_eq!(contents, b"Hello, World!");
}

#[tokio::test]
async fn test_write_file() {
    let (dir, fs) = create_test_fs().await;
    let root = fs.root().await.unwrap();

    // Write a new file through tinyfs
    let (_np, mut writer) = root
        .create_file_path_streaming_with_type("output.txt", EntryType::FilePhysicalVersion)
        .await
        .unwrap();
    writer.write_all(b"written via tinyfs").await.unwrap();
    writer.shutdown().await.unwrap();

    // Verify the file exists on the host filesystem
    let host_content = std::fs::read(dir.path().join("output.txt")).unwrap();
    assert_eq!(host_content, b"written via tinyfs");
}

#[tokio::test]
async fn test_create_directory() {
    let (dir, fs) = create_test_fs().await;
    let root = fs.root().await.unwrap();

    let _ = root.create_dir_path("newdir").await.unwrap();

    // Verify exists on host
    assert!(dir.path().join("newdir").is_dir());
}

#[tokio::test]
async fn test_create_nested_directory() {
    let (dir, fs) = create_test_fs().await;
    let root = fs.root().await.unwrap();

    let _ = root.create_dir_all("a/b/c").await.unwrap();

    // Verify exists on host
    assert!(dir.path().join("a/b/c").is_dir());
}

#[tokio::test]
async fn test_file_not_found() {
    let (_dir, fs) = create_test_fs().await;
    let root = fs.root().await.unwrap();

    let result = root.read_file_path_to_vec("nonexistent.txt").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_directory_already_exists() {
    let (_dir, fs) = create_test_fs().await;
    let root = fs.root().await.unwrap();

    // subdir already exists
    let result = root.create_dir_path("subdir").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_deterministic_file_ids() {
    let dir = create_test_tree();

    // Create two FS instances from the same root
    let persistence1 = HostmountPersistence::new(dir.path().to_path_buf()).unwrap();
    let fs1 = FS::new(persistence1).await.unwrap();
    let root1 = fs1.root().await.unwrap();

    let persistence2 = HostmountPersistence::new(dir.path().to_path_buf()).unwrap();
    let fs2 = FS::new(persistence2).await.unwrap();
    let root2 = fs2.root().await.unwrap();

    // Look up the same file in both -- should get identical FileIDs
    let np1 = root1
        .get("hello.txt")
        .await
        .unwrap()
        .expect("hello.txt not found in fs1");
    let np2 = root2
        .get("hello.txt")
        .await
        .unwrap()
        .expect("hello.txt not found in fs2");

    assert_eq!(np1.id(), np2.id(), "Same file should get same FileID");
}

#[tokio::test]
async fn test_entries_are_sorted() {
    let dir = TempDir::new().unwrap();
    // Create files in non-alphabetical order
    std::fs::write(dir.path().join("charlie.txt"), b"c").unwrap();
    std::fs::write(dir.path().join("alpha.txt"), b"a").unwrap();
    std::fs::write(dir.path().join("bravo.txt"), b"b").unwrap();

    let persistence = HostmountPersistence::new(dir.path().to_path_buf()).unwrap();
    let fs = FS::new(persistence).await.unwrap();
    let root = fs.root().await.unwrap();

    let entries: Vec<_> = {
        let mut stream = root.entries().await.unwrap();
        let mut items = Vec::new();
        while let Some(entry) = stream.next().await {
            items.push(entry.unwrap());
        }
        items
    };

    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(names, vec!["alpha.txt", "bravo.txt", "charlie.txt"]);
}

#[tokio::test]
async fn test_path_traversal_blocked() {
    let dir = create_test_tree();
    let persistence = HostmountPersistence::new(dir.path().to_path_buf()).unwrap();
    let fs = FS::new(persistence).await.unwrap();
    let root = fs.root().await.unwrap();

    // Attempting to read "../" should fail (path traversal)
    let result = root.read_file_path_to_vec("../../../etc/passwd").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_file_metadata() {
    let (_dir, fs) = create_test_fs().await;
    let root = fs.root().await.unwrap();

    let np = root
        .get("hello.txt")
        .await
        .unwrap()
        .expect("hello.txt not found");
    let meta = np.node.node_type.entry_type().await.unwrap();
    assert_eq!(meta, EntryType::FilePhysicalVersion);
}

#[tokio::test]
async fn test_subdir_entry_type() {
    let (_dir, fs) = create_test_fs().await;
    let root = fs.root().await.unwrap();

    let entries: Vec<_> = {
        let mut stream = root.entries().await.unwrap();
        let mut items = Vec::new();
        while let Some(entry) = stream.next().await {
            items.push(entry.unwrap());
        }
        items
    };

    let subdir_entry = entries.iter().find(|e| e.name == "subdir").unwrap();
    assert_eq!(subdir_entry.entry_type, EntryType::DirectoryPhysical);

    let file_entry = entries.iter().find(|e| e.name == "hello.txt").unwrap();
    assert_eq!(file_entry.entry_type, EntryType::FilePhysicalVersion);
}
