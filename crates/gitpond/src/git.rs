// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Git operations using gix (pure Rust).
//!
//! Manages bare repo init/open/fetch, and provides lazy tree
//! navigation helpers for the dynamic directory implementation.

use std::path::{Path, PathBuf};
use tinyfs::ResultExt;

/// Kind of entry in a git tree
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    File,
    Symlink,
    Directory,
}

/// A single child entry from a git tree (single-level listing).
#[derive(Debug, Clone)]
pub struct TreeChild {
    pub name: String,
    pub oid: String,
    pub kind: EntryKind,
}

/// Get the path to the bare repo for a given factory node
#[must_use]
pub fn bare_repo_path(pond_path: &Path, node_id: &str) -> PathBuf {
    pond_path.join("git").join(format!("{}.git", node_id))
}

/// Initialize or open a bare repo, fetch the remote, and return the
/// commit SHA for the configured ref.
pub fn fetch_and_resolve(
    repo_path: &Path,
    url: &str,
    git_ref: &str,
) -> Result<String, tinyfs::Error> {
    let repo = if repo_path.exists() {
        let repo = open_bare_repo(repo_path)?;
        fetch_remote(&repo)?;
        repo
    } else {
        clone_bare_repo(repo_path, url)?
    };

    resolve_local_ref(&repo, git_ref)
}

/// Resolve a git ref to the root tree OID (ref -> commit -> tree).
pub fn resolve_tree_at_ref(repo_path: &Path, git_ref: &str) -> Result<String, tinyfs::Error> {
    let repo = open_repo(repo_path)?;
    let commit_sha = resolve_local_ref(&repo, git_ref)?;

    let oid =
        gix::ObjectId::from_hex(commit_sha.as_bytes()).map_other_context("Invalid commit SHA")?;

    let commit = repo
        .find_object(oid)
        .map_other_context("Failed to find commit")?
        .try_into_commit()
        .map_other_context("Object is not a commit")?;

    let tree_id = commit.tree_id().map_other_context("Failed to get tree")?;

    Ok(tree_id.to_hex().to_string())
}

/// Navigate from a tree OID through a prefix path to a subtree.
///
/// For example, `navigate_to_prefix(repo, tree_oid, "site/content")`
/// walks tree -> "site" subtree -> "content" subtree.
pub fn navigate_to_prefix(
    repo: &gix::Repository,
    tree_oid_hex: &str,
    prefix: &str,
) -> Result<String, tinyfs::Error> {
    let mut current_oid = parse_oid(tree_oid_hex)?;

    for segment in prefix.split('/') {
        if segment.is_empty() {
            continue;
        }

        let tree_obj = repo
            .find_object(current_oid)
            .map_other_context("Failed to find tree")?;
        let tree = tree_obj
            .try_into_tree()
            .map_other_context("Object is not a tree")?;

        let mut found = false;
        for entry_ref in tree.iter() {
            let entry = entry_ref.map_other_context("Failed to read tree entry")?;
            let name =
                std::str::from_utf8(entry.filename()).map_other_context("Non-UTF8 filename")?;
            if name == segment {
                if entry.mode().kind() != gix::object::tree::EntryKind::Tree {
                    return Err(tinyfs::Error::Other(format!(
                        "Prefix segment '{}' is not a directory",
                        segment
                    )));
                }
                current_oid = entry.oid().to_owned();
                found = true;
                break;
            }
        }

        if !found {
            return Err(tinyfs::Error::NotFound(
                format!("Prefix segment '{}' not found in tree", segment).into(),
            ));
        }
    }

    Ok(current_oid.to_hex().to_string())
}

/// List the immediate children of a git tree (single level, lazy).
pub fn list_tree_children(
    repo_path: &Path,
    tree_oid_hex: &str,
) -> Result<Vec<TreeChild>, tinyfs::Error> {
    let repo = open_repo(repo_path)?;
    let oid = parse_oid(tree_oid_hex)?;

    let tree_obj = repo
        .find_object(oid)
        .map_other_context("Failed to find tree")?;
    let tree = tree_obj
        .try_into_tree()
        .map_other_context("Object is not a tree")?;

    let mut children = Vec::new();
    for entry_ref in tree.iter() {
        let entry = entry_ref.map_other_context("Failed to read tree entry")?;

        let name = std::str::from_utf8(entry.filename()).map_other_context("Non-UTF8 filename")?;

        let oid_hex = entry.oid().to_hex().to_string();

        let kind = match entry.mode().kind() {
            gix::object::tree::EntryKind::Tree => EntryKind::Directory,
            gix::object::tree::EntryKind::Link => EntryKind::Symlink,
            gix::object::tree::EntryKind::Blob | gix::object::tree::EntryKind::BlobExecutable => {
                EntryKind::File
            }
            _ => {
                log::debug!(
                    "Skipping unsupported entry type at {}: mode={:#o}",
                    name,
                    entry.mode().value()
                );
                continue;
            }
        };

        children.push(TreeChild {
            name: name.to_string(),
            oid: oid_hex,
            kind,
        });
    }

    Ok(children)
}

/// Look up a single child by name in a git tree.
pub fn get_tree_child(
    repo_path: &Path,
    tree_oid_hex: &str,
    name: &str,
) -> Result<Option<TreeChild>, tinyfs::Error> {
    let repo = open_repo(repo_path)?;
    let oid = parse_oid(tree_oid_hex)?;

    let tree_obj = repo
        .find_object(oid)
        .map_other_context("Failed to find tree")?;
    let tree = tree_obj
        .try_into_tree()
        .map_other_context("Object is not a tree")?;

    for entry_ref in tree.iter() {
        let entry = entry_ref.map_other_context("Failed to read tree entry")?;

        let entry_name =
            std::str::from_utf8(entry.filename()).map_other_context("Non-UTF8 filename")?;

        if entry_name != name {
            continue;
        }

        let oid_hex = entry.oid().to_hex().to_string();

        let kind = match entry.mode().kind() {
            gix::object::tree::EntryKind::Tree => EntryKind::Directory,
            gix::object::tree::EntryKind::Link => EntryKind::Symlink,
            gix::object::tree::EntryKind::Blob | gix::object::tree::EntryKind::BlobExecutable => {
                EntryKind::File
            }
            _ => return Ok(None),
        };

        return Ok(Some(TreeChild {
            name: name.to_string(),
            oid: oid_hex,
            kind,
        }));
    }

    Ok(None)
}

/// Read the content of a blob by OID from a repo path.
pub fn read_blob(repo_path: &Path, oid_hex: &str) -> Result<Vec<u8>, tinyfs::Error> {
    let repo = open_repo(repo_path)?;
    let oid = parse_oid(oid_hex)?;

    let object = repo
        .find_object(oid)
        .map_other_context("Failed to find object")?;

    Ok(object.data.to_vec())
}

/// Read a symlink target from a repo path (blob content as UTF-8).
pub fn read_symlink_target_from_repo(
    repo_path: &Path,
    oid_hex: &str,
) -> Result<String, tinyfs::Error> {
    let data = read_blob(repo_path, oid_hex)?;
    String::from_utf8(data).map_other_context("Symlink target is not valid UTF-8")
}

// --- Helpers ---

fn parse_oid(hex: &str) -> Result<gix::ObjectId, tinyfs::Error> {
    gix::ObjectId::from_hex(hex.as_bytes()).map_other_context("Invalid OID")
}

/// Open a bare repo (public for use by tree.rs).
pub fn open_repo(path: &Path) -> Result<gix::Repository, tinyfs::Error> {
    gix::open(path).map_err(|e| {
        tinyfs::Error::Other(format!(
            "Failed to open bare repo at {}: {}",
            path.display(),
            e
        ))
    })
}

fn open_bare_repo(path: &Path) -> Result<gix::Repository, tinyfs::Error> {
    open_repo(path)
}

fn clone_bare_repo(path: &Path, url: &str) -> Result<gix::Repository, tinyfs::Error> {
    log::info!("Cloning bare repo from {} to {}", url, path.display());

    let mut prep =
        gix::prepare_clone_bare(url, path).map_other_context("Failed to prepare clone")?;

    let (repo, _outcome) = prep
        .fetch_only(gix::progress::Discard, &gix::interrupt::IS_INTERRUPTED)
        .map_other_context("Clone fetch failed")?;

    Ok(repo)
}

fn fetch_remote(repo: &gix::Repository) -> Result<(), tinyfs::Error> {
    log::info!("Fetching from origin");

    let remote = repo
        .find_remote("origin")
        .map_other_context("Failed to find remote 'origin'")?;
    let remote = if remote.refspecs(gix::remote::Direction::Fetch).is_empty() {
        remote
            .with_refspecs(
                ["+refs/heads/*:refs/remotes/origin/*"],
                gix::remote::Direction::Fetch,
            )
            .map_other_context("Failed to configure default fetch refspec")?
    } else {
        remote
    };

    let _outcome = remote
        .connect(gix::remote::Direction::Fetch)
        .map_other_context("Failed to connect to remote")?
        .prepare_fetch(gix::progress::Discard, Default::default())
        .map_other_context("Failed to prepare fetch")?
        .receive(gix::progress::Discard, &gix::interrupt::IS_INTERRUPTED)
        .map_other_context("Fetch failed")?;

    log::info!("Fetch complete");
    Ok(())
}

fn resolve_local_ref(repo: &gix::Repository, git_ref: &str) -> Result<String, tinyfs::Error> {
    let candidates = [
        format!("refs/remotes/origin/{}", git_ref),
        format!("refs/heads/{}", git_ref),
        format!("refs/tags/{}", git_ref),
        git_ref.to_string(),
    ];

    for candidate in &candidates {
        if let Ok(reference) = repo.find_reference(candidate.as_str())
            && let Ok(id) = reference.into_fully_peeled_id()
        {
            log::debug!("Resolved '{}' via '{}' -> {}", git_ref, candidate, id);
            return Ok(id.to_hex().to_string());
        }
    }

    if git_ref.len() >= 40
        && let Ok(oid) = gix::ObjectId::from_hex(git_ref.as_bytes())
        && repo.find_object(oid).is_ok()
    {
        return Ok(git_ref.to_string());
    }

    Err(tinyfs::Error::Other(format!(
        "Failed to resolve ref '{}': not found in local refs",
        git_ref
    )))
}
