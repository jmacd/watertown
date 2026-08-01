// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Content-tree comparison (design Section 6.2): "comparison falls out of the
//! tree."  Two ponds (or two subtrees) are identical iff their `root_tree_hash`
//! values match.  When they differ, this module descends by `child_hash` to the
//! divergent subtrees, doing work proportional to the difference rather than to
//! the size of either tree.
//!
//! Because the content tree is content-only (all lineage lives in the commit),
//! the comparison is independent of `pond_id`, sequence, and wall-clock time:
//! a clone and its origin compare equal, and two independently built ponds with
//! the same content compare equal.

use std::collections::BTreeMap;

use crate::content_tree::{ChildRef, ContentTreeIndex, NodeKey, build_content_tree_for_table};
use crate::{Ship, StewardError};

/// How a single path differs between two content trees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffKind {
    /// The path exists only in the right-hand pond.
    Added,
    /// The path exists only in the left-hand pond.
    Removed,
    /// The path exists in both with the same entry kind but different content.
    Modified,
    /// The path exists in both but with a different entry kind (for example, a
    /// directory on one side and a file on the other).
    TypeChanged,
}

/// One divergence between two content trees, identified by its slash-rooted
/// path (for example, `/sub/a.txt`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentDiff {
    /// The path that differs, rooted at `/`.
    pub path: String,
    /// The nature of the divergence.
    pub kind: DiffKind,
}

/// The result of comparing two content trees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentComparison {
    /// `true` when both roots hash identically (the trees are equal).
    pub equal: bool,
    /// The minimal set of divergent paths, in lexicographic order.  Empty when
    /// `equal` is `true`.
    pub differences: Vec<ContentDiff>,
}

/// Compare the live content trees of two ponds (design Section 6.2 / Goal 2).
///
/// Returns whether their roots are equal and, if not, the divergent paths found
/// by descending only into subtrees whose `child_hash` differs.
///
/// # Errors
///
/// Returns an error if either pond's data table cannot be read or folded.
pub async fn compare_content_trees(
    left: &Ship,
    right: &Ship,
) -> Result<ContentComparison, StewardError> {
    let left_index = build_content_tree_for_table(
        left.data_persistence().table().clone(),
        left.data_persistence().pond_id(),
    )
    .await?;
    let right_index = build_content_tree_for_table(
        right.data_persistence().table().clone(),
        right.data_persistence().pond_id(),
    )
    .await?;

    if left_index.root_tree_hash == right_index.root_tree_hash {
        return Ok(ContentComparison {
            equal: true,
            differences: Vec::new(),
        });
    }

    let mut differences = Vec::new();
    diff_dir(
        "",
        &left_index.root_key,
        &left_index,
        &right_index.root_key,
        &right_index,
        &mut differences,
    );

    Ok(ContentComparison {
        equal: false,
        differences,
    })
}

/// Index a directory's children by name for set-style comparison.
fn by_name<'a>(index: &'a ContentTreeIndex, key: &NodeKey) -> BTreeMap<&'a str, &'a ChildRef> {
    index
        .dirs
        .get(key)
        .map(|children| {
            children
                .iter()
                .map(|c| (c.name.as_str(), c))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default()
}

/// Recursively diff two physical directories whose tree hashes already differ.
///
/// `prefix` is the slash-rooted path of the directory itself (empty for the
/// root).  Only subtrees that differ are descended into; an entry whose
/// `child_hash` *and* node metadata both match prunes the whole subtree.
///
/// Metadata counts as content here.  A version's metadata -- when it was
/// written, the event-time range it covers -- is data about that immutable
/// version, committed to by the directory entry, so two entries that agree on
/// bytes but disagree on metadata are `Modified`.  Pruning on `child_hash`
/// alone would let the roots differ while the descent reported nothing.
fn diff_dir(
    prefix: &str,
    left_key: &NodeKey,
    left: &ContentTreeIndex,
    right_key: &NodeKey,
    right: &ContentTreeIndex,
    out: &mut Vec<ContentDiff>,
) {
    let left_children = by_name(left, left_key);
    let right_children = by_name(right, right_key);

    // Union of names in lexicographic order (BTreeMap keys are sorted).
    let mut names: Vec<&str> = left_children.keys().copied().collect();
    for name in right_children.keys() {
        if !left_children.contains_key(name) {
            names.push(name);
        }
    }
    names.sort_unstable();

    for name in names {
        let path = format!("{prefix}/{name}");
        match (left_children.get(name), right_children.get(name)) {
            (Some(_), None) => out.push(ContentDiff {
                path,
                kind: DiffKind::Removed,
            }),
            (None, Some(_)) => out.push(ContentDiff {
                path,
                kind: DiffKind::Added,
            }),
            (Some(lc), Some(rc)) => {
                if lc.child_hash == rc.child_hash && lc.versions == rc.versions {
                    continue; // identical subtree and metadata -- prune.
                }
                if lc.entry_type != rc.entry_type {
                    out.push(ContentDiff {
                        path,
                        kind: DiffKind::TypeChanged,
                    });
                    continue;
                }
                match (&lc.child_dir_key, &rc.child_dir_key) {
                    // Both physical directories with differing subtrees:
                    // descend for path-level detail.
                    (Some(lk), Some(rk)) if lc.child_hash != rc.child_hash => {
                        diff_dir(&path, lk, left, rk, right, out);
                    }
                    // Same kind, differing content -- either differing bytes or
                    // identical bytes with differing node metadata.
                    _ => out.push(ContentDiff {
                        path,
                        kind: DiffKind::Modified,
                    }),
                }
            }
            (None, None) => unreachable!("name came from one of the two child sets"),
        }
    }
}
