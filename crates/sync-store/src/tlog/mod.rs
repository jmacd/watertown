// SPDX-License-Identifier: Apache-2.0

//! The transparency log (TIME): a SHA-256 RFC 6962 Merkle index over the
//! linear commit spine.
//!
//! The log is not a second structure -- it is the existing append-only sequence
//! of commits from the content graph, ordered by `seq`, indexed for proofs.
//! Design Section 7 fixes the seam precisely: the leaf payload is a commit
//! object (its BLAKE3 `root_tree_hash` plus provenance), and the log hashes
//! that leaf's serialization with **SHA-256**.  Internally the system never
//! leaves BLAKE3; SHA-256 appears only here, at the publishing boundary, so
//! off-the-shelf witnesses and verifiers can co-sign.
//!
//! ```text
//! content graph (SPACE, BLAKE3)        transparency log (TIME, SHA-256)
//!         commit_N  --------- leaf N ---------> hash_leaf(commit_N.encode())
//!            |                                          |
//!     root_tree_hash                            RFC 6962 Merkle over leaves
//! ```
//!
//! Tile materialization and checkpoint emission are implemented (design D5):
//! see [`tiles`] for the C2SP `tlog-tiles` layout and [`checkpoint`] for the
//! `tlog-checkpoint` note body.  Signing the checkpoint remains deferred with
//! the rest of the trust root (design Section 10).

mod checkpoint;
mod merkle;
mod tiles;

pub use checkpoint::{
    CHECKPOINT_FILE, CHECKPOINT_HISTORY_FILE, Checkpoint, CheckpointError, checkpoint_history_path,
    checkpoint_path,
};
pub use merkle::{
    LogHash, ProofError, TransparencyLog, empty_root, hash_children, hash_leaf, verify_consistency,
    verify_inclusion,
};
pub use tiles::{TILE_HEIGHT_BITS, TILE_WIDTH, TileLog, data_tile_rel, n_path};

use crate::content::Commit;

/// The transparency-log leaf payload for a commit: its canonical content-graph
/// encoding (`root_tree_hash` + parent + provenance).  This is the exact byte
/// string the log's SHA-256 leaf hash is taken over, binding the TIME log to
/// the SPACE root through the commit object.
#[must_use]
pub fn commit_leaf_data(commit: &Commit) -> Vec<u8> {
    commit.encode()
}

/// The RFC 6962 leaf hash of a commit: `SHA-256(0x00 || commit.encode())`.
#[must_use]
pub fn commit_leaf_hash(commit: &Commit) -> LogHash {
    hash_leaf(&commit_leaf_data(commit))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content::{ContentModelVersion, ObjectHash, Provenance};

    fn commit(seq: i64, root_byte: u8) -> Commit {
        Commit::new(
            ContentModelVersion::LogicalSeriesV2,
            ObjectHash::of_bytes(&[root_byte]),
            None,
            ObjectHash::of_bytes(b"manifest"),
            ObjectHash::of_bytes(b"manifest-root"),
            Provenance {
                pond_id: "pond".to_string(),
                seq,
                time_micros: 1_700_000_000_000_000 + seq,
                author: String::new(),
                request: format!("write {seq}"),
            },
        )
    }

    #[test]
    fn commit_leaf_hash_matches_encoded_leaf() {
        let c = commit(1, 0xAB);
        assert_eq!(commit_leaf_hash(&c), hash_leaf(&c.encode()));
    }

    #[test]
    fn commit_spine_log_proves_inclusion() {
        let commits: Vec<Commit> = (0..5).map(|i| commit(i, i as u8)).collect();
        let mut log = TransparencyLog::new();
        for c in &commits {
            let _ = log.append_leaf_hash(commit_leaf_hash(c));
        }
        let root = log.root();
        for (i, c) in commits.iter().enumerate() {
            let proof = log.inclusion_proof(i).expect("proof");
            assert!(verify_inclusion(
                &commit_leaf_hash(c),
                i,
                commits.len(),
                &proof,
                &root,
            ));
        }
    }
}
