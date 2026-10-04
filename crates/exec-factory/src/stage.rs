// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Host-filesystem staging and diffing.
//!
//! These functions operate purely on `std::path::Path` and real files --
//! no TinyFS, no subprocess -- so the "does the diff logic do the right
//! thing" question can be tested without a pond or a sandbox. The
//! TinyFS <-> host bridging lives in `factory.rs`.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

/// One output spec: either an exact relative file path, or a directory
/// prefix (trailing `/` in config) under which any file counts as an
/// output.
#[derive(Debug, Clone)]
pub enum OutputSpec {
    File(PathBuf),
    DirPrefix(PathBuf),
}

/// Parse a pond-absolute path (e.g. `/data/billing/journal.ledger`) into the
/// path relative to a staging root (e.g. `data/billing/journal.ledger`).
/// Pond paths are always absolute; staging mirrors them without the leading
/// `/` so they land inside the staging `tempdir()` rather than at the host
/// filesystem root.
#[must_use]
pub fn pond_path_to_relative(pond_path: &str) -> PathBuf {
    PathBuf::from(pond_path.trim_start_matches('/'))
}

/// The inverse of [`pond_path_to_relative`].
#[must_use]
pub fn relative_to_pond_path(relative: &Path) -> String {
    format!("/{}", relative.to_string_lossy())
}

#[must_use]
pub fn parse_output_spec(spec: &str) -> OutputSpec {
    if let Some(prefix) = spec.strip_suffix('/') {
        OutputSpec::DirPrefix(pond_path_to_relative(prefix))
    } else {
        OutputSpec::File(pond_path_to_relative(spec))
    }
}

/// Recursively list every regular file under `dir` (relative paths from
/// `dir`). Returns an empty vec if `dir` doesn't exist yet -- a directory
/// output spec that the program hasn't created anything under yet is not
/// an error.
fn walk_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        for entry in std::fs::read_dir(&current)? {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                stack.push(path);
            } else if file_type.is_file() {
                out.push(path);
            }
            // Symlinks are deliberately ignored: committing their target
            // bytes would be surprising, and the sandbox shouldn't let a
            // program create them pointing outside staging anyway.
        }
    }
    Ok(out)
}

/// Resolve every path currently matching `outputs` (relative to `staging`),
/// as absolute host paths.
fn resolve_output_paths(staging: &Path, outputs: &[OutputSpec]) -> io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for spec in outputs {
        match spec {
            OutputSpec::File(rel) => {
                let abs = staging.join(rel);
                if abs.is_file() {
                    paths.push(abs);
                }
            }
            OutputSpec::DirPrefix(rel) => {
                paths.extend(walk_files(&staging.join(rel))?);
            }
        }
    }
    Ok(paths)
}

/// BLAKE3 hash of a file's current contents.
fn hash_file(path: &Path) -> io::Result<blake3::Hash> {
    let bytes = std::fs::read(path)?;
    Ok(blake3::hash(&bytes))
}

/// Pre-exec hashes of every path currently matching `outputs`, keyed by path
/// relative to `staging`.
pub fn snapshot_outputs(
    staging: &Path,
    outputs: &[OutputSpec],
) -> io::Result<BTreeMap<PathBuf, blake3::Hash>> {
    let mut snapshot = BTreeMap::new();
    for abs in resolve_output_paths(staging, outputs)? {
        let rel = abs
            .strip_prefix(staging)
            .expect("resolve_output_paths returns paths under staging")
            .to_path_buf();
        let _ = snapshot.insert(rel, hash_file(&abs)?);
    }
    Ok(snapshot)
}

/// Result of comparing a post-exec snapshot of `outputs` against the
/// pre-exec one from [`snapshot_outputs`].
#[derive(Debug, Default)]
pub struct OutputDiff {
    /// Relative path (under staging) -> new file content, for every path
    /// that is new or whose content hash changed.
    pub changed: BTreeMap<PathBuf, Vec<u8>>,
    /// Relative paths that existed pre-exec and are now gone. Per the design
    /// doc, callers must treat a non-empty `deleted` as an error, not apply
    /// it -- deletions are not committed in v1.
    pub deleted: Vec<PathBuf>,
}

/// Compare the current state of `outputs` under `staging` against `before`.
pub fn diff_outputs(
    staging: &Path,
    outputs: &[OutputSpec],
    before: &BTreeMap<PathBuf, blake3::Hash>,
) -> io::Result<OutputDiff> {
    let mut diff = OutputDiff::default();
    let mut seen = std::collections::BTreeSet::new();

    for abs in resolve_output_paths(staging, outputs)? {
        let rel = abs
            .strip_prefix(staging)
            .expect("resolve_output_paths returns paths under staging")
            .to_path_buf();
        let _ = seen.insert(rel.clone());
        let after_hash = hash_file(&abs)?;
        let unchanged = before.get(&rel).is_some_and(|h| *h == after_hash);
        if !unchanged {
            let _ = diff.changed.insert(rel, std::fs::read(&abs)?);
        }
    }

    for rel in before.keys() {
        if !seen.contains(rel) {
            diff.deleted.push(rel.clone());
        }
    }

    Ok(diff)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, content: &str) {
        std::fs::create_dir_all(path.parent().expect("has parent")).expect("mkdir");
        std::fs::write(path, content).expect("write");
    }

    #[test]
    fn detects_new_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let outputs = vec![parse_output_spec("reports/")];
        let before = snapshot_outputs(dir.path(), &outputs).expect("snapshot");
        assert!(before.is_empty());

        write(&dir.path().join("reports/a.txt"), "hello");
        let diff = diff_outputs(dir.path(), &outputs, &before).expect("diff");
        assert_eq!(diff.changed.len(), 1);
        assert!(diff.deleted.is_empty());
        assert_eq!(
            diff.changed.get(Path::new("reports/a.txt")).expect("present"),
            b"hello"
        );
    }

    #[test]
    fn ignores_unchanged_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(&dir.path().join("reports/a.txt"), "hello");
        let outputs = vec![parse_output_spec("reports/")];
        let before = snapshot_outputs(dir.path(), &outputs).expect("snapshot");

        let diff = diff_outputs(dir.path(), &outputs, &before).expect("diff");
        assert!(diff.changed.is_empty());
        assert!(diff.deleted.is_empty());
    }

    #[test]
    fn detects_modified_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(&dir.path().join("reports/a.txt"), "hello");
        let outputs = vec![parse_output_spec("reports/")];
        let before = snapshot_outputs(dir.path(), &outputs).expect("snapshot");

        write(&dir.path().join("reports/a.txt"), "goodbye");
        let diff = diff_outputs(dir.path(), &outputs, &before).expect("diff");
        assert_eq!(diff.changed.len(), 1);
        assert_eq!(
            diff.changed.get(Path::new("reports/a.txt")).expect("present"),
            b"goodbye"
        );
    }

    #[test]
    fn detects_deletion() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(&dir.path().join("reports/a.txt"), "hello");
        let outputs = vec![parse_output_spec("reports/")];
        let before = snapshot_outputs(dir.path(), &outputs).expect("snapshot");

        std::fs::remove_file(dir.path().join("reports/a.txt")).expect("remove");
        let diff = diff_outputs(dir.path(), &outputs, &before).expect("diff");
        assert!(diff.changed.is_empty());
        assert_eq!(diff.deleted, vec![PathBuf::from("reports/a.txt")]);
    }

    #[test]
    fn exact_file_output_spec_works_without_directory_walk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let outputs = vec![parse_output_spec("out.txt")];
        let before = snapshot_outputs(dir.path(), &outputs).expect("snapshot");
        assert!(before.is_empty());

        write(&dir.path().join("out.txt"), "content");
        let diff = diff_outputs(dir.path(), &outputs, &before).expect("diff");
        assert_eq!(diff.changed.len(), 1);
    }

    #[test]
    fn pond_path_round_trip() {
        let rel = pond_path_to_relative("/data/billing/journal.ledger");
        assert_eq!(rel, PathBuf::from("data/billing/journal.ledger"));
        assert_eq!(
            relative_to_pond_path(&rel),
            "/data/billing/journal.ledger"
        );
    }
}
