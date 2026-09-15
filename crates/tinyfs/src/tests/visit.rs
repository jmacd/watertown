// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use futures::stream::{self, Stream};
use std::collections::BTreeSet;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Barrier, Notify};

use super::super::memory::new_fs;
use crate::async_helpers::convenience;
use crate::dir::Directory;
use crate::dir::DirectoryEntry;
use crate::dir::Handle as DirectoryHandle;
use crate::error;
use crate::fs::FS;
use crate::node::Node;
use crate::node::NodeType;

/// A directory implementation that derives its contents from wildcard matches
pub struct VisitDirectory {
    fs: Arc<FS>,
    pattern: String,
}

impl VisitDirectory {
    pub fn new<P: AsRef<str>>(fs: Arc<FS>, pattern: P) -> Self {
        Self {
            fs,
            pattern: pattern.as_ref().to_string(),
        }
    }

    pub fn new_handle<P: AsRef<str>>(fs: Arc<FS>, pattern: P) -> DirectoryHandle {
        DirectoryHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(Self::new(
            fs, pattern,
        )))))
    }
}

/// A visitor that creates filename and node ref pairs for directory iteration
struct FilenameCollector {
    items: Vec<(String, Node)>,
}

impl FilenameCollector {
    fn new() -> Self {
        Self { items: Vec::new() }
    }
}

#[async_trait::async_trait]
impl crate::wd::Visitor<(String, Node)> for FilenameCollector {
    async fn visit(
        &mut self,
        node: crate::node::NodePath,
        captured: &[String],
    ) -> error::Result<(String, Node)> {
        let filename = if captured.is_empty() {
            node.basename()
        } else {
            captured.join("_")
        };
        let result = (filename, node.node.clone());
        self.items.push(result.clone());
        Ok(result)
    }
}

/// A visitor that creates filename and file content pairs
struct FileContentVisitor {
    results: Vec<(String, Vec<u8>)>,
}

impl FileContentVisitor {
    fn new() -> Self {
        Self {
            results: Vec::new(),
        }
    }
}

#[async_trait::async_trait]
impl crate::wd::Visitor<(String, Vec<u8>)> for FileContentVisitor {
    async fn visit(
        &mut self,
        node: crate::node::NodePath,
        _captured: &[String],
    ) -> error::Result<(String, Vec<u8>)> {
        let file_node = node.as_file().await.unwrap();
        let reader = file_node.async_reader().await.unwrap();
        let content = crate::async_helpers::buffer_helpers::read_all_to_vec(reader)
            .await
            .unwrap();
        let result = (node.basename(), content);
        self.results.push(result.clone());
        Ok(result)
    }
}

/// A visitor that collects just the basename of files
struct BasenameVisitor {
    results: Vec<String>,
}

impl BasenameVisitor {
    fn new() -> Self {
        Self {
            results: Vec::new(),
        }
    }
}

#[async_trait::async_trait]
impl crate::wd::Visitor<String> for BasenameVisitor {
    async fn visit(
        &mut self,
        node: crate::node::NodePath,
        _captured: &[String],
    ) -> error::Result<String> {
        let result = node.basename();
        self.results.push(result.clone());
        Ok(result)
    }
}

struct FailingVisitor;

#[async_trait::async_trait]
impl crate::wd::Visitor<()> for FailingVisitor {
    async fn visit(
        &mut self,
        _node: crate::node::NodePath,
        _captured: &[String],
    ) -> error::Result<()> {
        Err(error::Error::Other("visitor failed".to_string()))
    }
}

struct BarrierVisitor {
    barrier: Arc<Barrier>,
}

#[async_trait::async_trait]
impl crate::wd::Visitor<()> for BarrierVisitor {
    async fn visit(
        &mut self,
        _node: crate::node::NodePath,
        _captured: &[String],
    ) -> error::Result<()> {
        _ = self.barrier.wait().await;
        Ok(())
    }
}

struct CancelOnceDirectory {
    calls: AtomicUsize,
    entered: Arc<Notify>,
}

impl CancelOnceDirectory {
    fn new_handle(entered: Arc<Notify>) -> DirectoryHandle {
        DirectoryHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(Self {
            calls: AtomicUsize::new(0),
            entered,
        }))))
    }
}

#[async_trait::async_trait]
impl Directory for CancelOnceDirectory {
    async fn get(&self, _name: &str) -> error::Result<Option<Node>> {
        Ok(None)
    }

    async fn insert(&mut self, name: String, _id: Node) -> error::Result<()> {
        Err(error::Error::immutable(name))
    }

    async fn remove(&mut self, name: &str) -> error::Result<Option<Node>> {
        Err(error::Error::immutable(name))
    }

    async fn entries(
        &self,
    ) -> error::Result<Pin<Box<dyn Stream<Item = error::Result<DirectoryEntry>> + Send>>> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.entered.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(Box::pin(stream::empty()))
    }
}

#[async_trait::async_trait]
impl crate::Metadata for CancelOnceDirectory {
    async fn metadata(&self) -> error::Result<crate::NodeMetadata> {
        Ok(crate::NodeMetadata {
            version: 1,
            size: None,
            blake3: None,
            bao_outboard: None,
            entry_type: crate::EntryType::DirectoryDynamic,
            timestamp: 0,
        })
    }
}

#[async_trait::async_trait]
impl Directory for VisitDirectory {
    async fn get(&self, name: &str) -> error::Result<Option<Node>> {
        let mut visitor = FilenameCollector::new();
        let root_node = self.fs.root().await?;
        let result = root_node
            .visit_with_visitor(&self.pattern, &mut visitor)
            .await;
        // @@@ Why do I have to do this, problem with visitor pattern.
        let _ = result?;
        Ok(visitor
            .items
            .into_iter()
            .find(|(n, _)| n == name)
            .map(|(_, r)| r))
    }

    async fn insert(&mut self, name: String, _id: Node) -> error::Result<()> {
        Err(error::Error::immutable(name))
    }

    async fn remove(&mut self, name: &str) -> error::Result<Option<Node>> {
        Err(error::Error::immutable(name))
    }

    async fn entries(
        &self,
    ) -> error::Result<Pin<Box<dyn Stream<Item = error::Result<DirectoryEntry>> + Send>>> {
        // For now, fall back to the original visitor pattern implementation
        // TODO: Use derived manager for better performance and caching
        let mut visitor = FilenameCollector::new();
        let root = self.fs.root().await?;
        let result = root.visit_with_visitor(&self.pattern, &mut visitor).await;
        // @@@
        _ = result?;
        let items: Vec<_> = visitor
            .items
            .into_iter()
            .map(|(name, _node_ref)| {
                // Convert Node to DirectoryEntry (without awaiting - use placeholder)
                // Use a deterministic FileID for testing (part_id=root for test simplicity)
                let file_id = crate::FileID::from_content(
                    crate::PartID::root(),
                    crate::EntryType::FileDynamic,
                    format!("visit:{}", name).as_bytes(),
                    crate::local_pond_uuid(),
                );
                let dir_entry = DirectoryEntry::new(
                    name.clone(),
                    file_id.node_id(),
                    crate::EntryType::FileDynamic,
                    0,
                );
                Ok(dir_entry)
            })
            .collect();
        Ok(Box::pin(stream::iter(items)))
    }
}

#[async_trait::async_trait]
impl crate::Metadata for VisitDirectory {
    async fn metadata(&self) -> error::Result<crate::NodeMetadata> {
        Ok(crate::NodeMetadata {
            version: 1,
            size: None,
            blake3: None,
            bao_outboard: None,
            entry_type: crate::EntryType::DirectoryDynamic,
            timestamp: 0, // TODO
        })
    }
}

#[tokio::test]
async fn test_visit_directory() {
    // Create a filesystem with some test files
    let fs = new_fs().await;
    let fs_arc = Arc::new(fs);
    let root = fs_arc.root().await.unwrap();

    // Create test files in various locations
    _ = root.create_dir_path("/away").await.unwrap();
    _ = root.create_dir_path("/in").await.unwrap();
    _ = root.create_dir_path("/in/a").await.unwrap();
    _ = convenience::create_file_path(&root, "/in/a/1.txt", b"Content A")
        .await
        .unwrap();

    _ = root.create_dir_path("/in/a/b").await.unwrap();
    _ = convenience::create_file_path(&root, "/in/a/b/1.txt", b"Content A-B")
        .await
        .unwrap();

    _ = root.create_dir_path("/in/a/c").await.unwrap();
    _ = convenience::create_file_path(&root, "/in/a/c/1.txt", b"Content A-C")
        .await
        .unwrap();

    _ = root.create_dir_path("/in/b").await.unwrap();
    _ = root.create_dir_path("/in/b/a").await.unwrap();
    _ = convenience::create_file_path(&root, "/in/b/a/1.txt", b"Content B-A")
        .await
        .unwrap();

    // Test the visitor pattern directly first
    let mut visitor = FilenameCollector::new();
    let _result = root.visit_with_visitor("/in/**/1.txt", &mut visitor).await;

    // If the direct visitor works, proceed with VisitDirectory
    if visitor.items.is_empty() {
        panic!("Direct visitor pattern failed - no items found for /in/**/1.txt");
    }

    // Create a virtual directory that matches all "1.txt" files in any subfolder
    _ = root
        .create_node_path("/away/visit-test", || {
            Ok(NodeType::Directory(VisitDirectory::new_handle(
                fs_arc.clone(),
                "/in/**/1.txt",
            )))
        })
        .await
        .unwrap();

    // Access the visit directory and check its contents
    let visit_dir = root.open_dir_path("/away/visit-test").await.unwrap();

    // Test accessing files through the visit directory
    let result1 = root
        .read_file_path_to_vec("/away/visit-test/a")
        .await
        .unwrap();
    assert_eq!(result1, b"Content A");

    let result2 = root
        .read_file_path_to_vec("/away/visit-test/a_b")
        .await
        .unwrap();
    assert_eq!(result2, b"Content A-B");

    let result3 = root
        .read_file_path_to_vec("/away/visit-test/a_c")
        .await
        .unwrap();
    assert_eq!(result3, b"Content A-C");

    let reader4 = root
        .async_reader_path("/away/visit-test/b_a")
        .await
        .unwrap();
    let result4 = crate::async_helpers::buffer_helpers::read_all_to_vec(reader4)
        .await
        .unwrap();
    assert_eq!(result4, b"Content B-A");

    // Test iterator functionality of VisitDirectory
    let mut entries = BTreeSet::new();
    let mut entry_stream = visit_dir.entries().await.unwrap();
    use futures::StreamExt;
    while let Some(result) = entry_stream.next().await {
        let dir_entry = result.unwrap();
        let np = visit_dir.get(&dir_entry.name).await.unwrap().unwrap();
        let file_node = np.as_file().await.unwrap();
        let reader = file_node.async_reader().await.unwrap();
        let content = crate::async_helpers::buffer_helpers::read_all_to_vec(reader)
            .await
            .unwrap();
        _ = entries.insert((np.basename(), content));
    }

    let expected = BTreeSet::from([
        ("a".to_string(), b"Content A".to_vec()),
        ("a_b".to_string(), b"Content A-B".to_vec()),
        ("a_c".to_string(), b"Content A-C".to_vec()),
        ("b_a".to_string(), b"Content B-A".to_vec()),
    ]);

    assert_eq!(entries, expected);
}

#[tokio::test]
async fn test_visit_directory_loop() {
    let fs = new_fs().await;
    let fs_arc = Arc::new(fs);
    let root = fs_arc.root().await.unwrap();

    _ = root.create_dir_path("/loop").await.unwrap();
    _ = convenience::create_file_path(&root, "/loop/test.txt", b"Test content")
        .await
        .unwrap();
    _ = root
        .create_node_path("/loop/visit", || {
            Ok(NodeType::Directory(VisitDirectory::new_handle(
                fs_arc.clone(),
                "/loop/**",
            )))
        })
        .await
        .unwrap();

    // Should see a VisitLoop error.
    let mut visitor = crate::wd::CollectingVisitor::new();
    let result = root
        .visit_with_visitor("/loop/visit/**", &mut visitor)
        .await;

    match result {
        Err(error::Error::VisitLoop(p)) => {
            assert_eq!(p, Path::new("/loop"));
        }
        _ => panic!(
            "Expected VisitLoop error but got a different error: {:?}",
            result
        ),
    }

    let matches = root.collect_matches("/loop/test.txt").await.unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].0.path(), Path::new("/loop/test.txt"));
}

#[tokio::test]
async fn test_visit_recovers_after_visitor_error() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();
    _ = root.create_dir_path("/recover").await.unwrap();
    _ = convenience::create_file_path(&root, "/recover/file.txt", b"content")
        .await
        .unwrap();

    let mut failing = FailingVisitor;
    let error = root
        .visit_with_visitor("/recover/*", &mut failing)
        .await
        .unwrap_err();
    assert_eq!(error, error::Error::Other("visitor failed".to_string()));

    let matches = root.collect_matches("/recover/*").await.unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].0.path(), Path::new("/recover/file.txt"));
}

#[tokio::test]
async fn test_concurrent_visits_do_not_share_active_nodes() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();
    _ = root.create_dir_path("/shared").await.unwrap();
    _ = convenience::create_file_path(&root, "/shared/file.txt", b"content")
        .await
        .unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let mut first = BarrierVisitor {
        barrier: barrier.clone(),
    };
    let mut second = BarrierVisitor { barrier };
    let (first_result, second_result) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            root.visit_with_visitor("/shared/*", &mut first),
            root.visit_with_visitor("/shared/*", &mut second)
        )
    })
    .await
    .expect("concurrent traversals should both reach the shared directory");

    assert!(first_result.is_ok(), "first traversal: {first_result:?}");
    assert!(second_result.is_ok(), "second traversal: {second_result:?}");
}

#[tokio::test]
async fn test_cancelled_visit_releases_active_nodes() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();
    let entered = Arc::new(Notify::new());
    _ = root
        .create_node_path("/cancel", || {
            Ok(NodeType::Directory(CancelOnceDirectory::new_handle(
                entered.clone(),
            )))
        })
        .await
        .unwrap();

    let visit_root = root.clone();
    let task = tokio::spawn(async move {
        let mut visitor = crate::wd::CollectingVisitor::new();
        visit_root
            .visit_with_visitor("/cancel/*", &mut visitor)
            .await
    });
    entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    let matches = root.collect_matches("/cancel/*").await.unwrap();
    assert!(matches.is_empty());
}

#[tokio::test]
async fn test_visit_with_symlinks() {
    let fs = new_fs().await;
    let root = fs.root().await.unwrap();

    // Create a directory structure with a symlink
    _ = root.create_dir_path("/a").await.unwrap();
    _ = root.create_dir_path("/a/123456").await.unwrap();
    _ = convenience::create_file_path(&root, "/a/123456/b.txt", b"Symlink test content")
        .await
        .unwrap();

    // Create a symlink from /a/name -> "123456"
    _ = root.create_symlink_path("/a/name", "123456").await.unwrap();

    // Test visiting with different patterns that should all find b.txt through the symlink

    // Pattern 1: Generic pattern that would find all .txt files
    let mut basename_visitor1 = BasenameVisitor::new();
    _ = root
        .visit_with_visitor("/**/*.txt", &mut basename_visitor1)
        .await
        .unwrap();
    let results1 = basename_visitor1.results;
    assert!(
        results1.contains(&"b.txt".to_string()),
        "Should find b.txt with generic pattern /**/*.txt"
    );

    // Pattern 2: Pattern explicitly going through the symlink
    let mut visitor = FileContentVisitor::new();
    _ = root
        .visit_with_visitor("/a/name/*.txt", &mut visitor)
        .await
        .unwrap();
    let results2 = visitor.results;
    assert!(
        results2.contains(&("b.txt".to_string(), b"Symlink test content".to_vec())),
        "Should find b.txt through the symlink with /a/name/*.txt"
    );

    // Pattern 3: Another pattern using a wildcard with the symlink parent
    let mut basename_visitor3 = BasenameVisitor::new();
    _ = root
        .visit_with_visitor("/*/name/*.txt", &mut basename_visitor3)
        .await
        .unwrap();
    let results3 = basename_visitor3.results;
    assert!(
        results3.contains(&"b.txt".to_string()),
        "Should find b.txt through the symlink with /*/name/*.txt"
    );
}
