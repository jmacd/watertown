// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Content-graph push: send a pond's reachable object closure plus its tip
//! commit to a [`ContentRemote`] (design Section 8, Decisions D6 and D7).
//!
//! This is the producer side of the single delta-managed content-addressed
//! remote.  A push is one atomic Delta commit on the remote that writes the
//! new object rows and advances the tip ref together; this module assembles
//! the objects to send and the tip to point at.
//!
//! The objects are: the inline tree closure from
//! [`materialize_content_objects`], the node manifest that commit references,
//! plus the tip commit object reproduced verbatim from the persisted commit
//! spine.  Large blobs (>64KB) live externally under `_large_files`, recorded
//! only by hash (Decision D7); this module reads each one's bytes locally and
//! sends it as a blob object keyed by its content hash, so the closure is
//! complete.

use provider::factory::rate_limit::LimitUnit;
use sync_store::ContentRemote;
use sync_store::content::ObjectHash;

use crate::content_tree::materialize_content_objects;
use crate::limiter::LimiterSet;
use crate::{Ship, StewardError};

/// The result of a successful content-graph push.
#[derive(Debug, Clone)]
pub struct ContentPushOutcome {
    /// The ref advanced on the remote.
    pub ref_name: String,
    /// The tip commit hash the ref now points at.
    pub tip: ObjectHash,
    /// Number of objects written to the remote in this push.
    pub objects_pushed: usize,
    /// The remote `txn_seq` allocated for this push.
    pub remote_txn_seq: i64,
}

/// Push the pond's current content closure and tip commit to `remote` under
/// `ref_name`.
///
/// The tip is the last content-changing commit (the highest seq that stamped a
/// content-graph spine).  Content-preserving transactions such as compaction
/// record no spine and are skipped, so a push right after compaction resolves
/// the same tip as before it.  The tip's encoded object bytes are taken from the
/// persisted commit spine and verified to hash to the recorded commit hash
/// before being sent, so the remote tip can never disagree with the object it
/// names.
///
/// The full inline closure is sent every time.  A re-put of an object the
/// remote already holds is idempotent, so this is correct though not minimal;
/// the local missing-set optimization against the last-pushed tip is a later
/// refinement.
///
/// # Errors
///
/// Returns an error if the pond has no content-changing commit to push,
/// if the persisted commit object does not hash to the recorded commit hash,
/// if any external large blob is missing or its bytes do not hash to the
/// recorded key, or if reading the content tree or writing to the remote fails.
pub async fn push_content_to_remote(
    ship: &Ship,
    remote: &mut ContentRemote,
    ref_name: &str,
) -> Result<ContentPushOutcome, StewardError> {
    let mut unlimited = LimiterSet::unlimited();
    push_content_to_remote_limited(ship, remote, ref_name, &mut unlimited).await
}

/// [`push_content_to_remote`], governed by `limits`.
///
/// Every remote interaction is charged before it happens and recorded after it
/// succeeds, so a push that fails costs nothing and a push is never started
/// that the budget cannot cover.  Two dimensions are spent:
///
/// - [`LimitUnit::Ops`] -- one per remote request: each `has_blob` probe, each
///   `put_blob`, and the final batched `push_commit`.
/// - [`LimitUnit::Bytes`] -- the blob bytes streamed to the remote, plus the
///   inline object bytes in the commit batch.
///
/// `limits` is bound by the caller (see [`LimiterSet::open`]) because binding
/// needs mutable pond access while the push itself does not; charging is pure,
/// so the borrow ends before this call.  The caller is also responsible for
/// [`LimiterSet::commit`] afterwards -- this function never writes the control
/// table, so a failed push leaves no partial accounting beyond what was
/// actually spent.
///
/// # Errors
///
/// In addition to [`push_content_to_remote`]'s errors, returns
/// [`StewardError::RateLimited`] when a budget is exhausted.  The pond's data
/// commit has already happened and is durable; a push is a mirror operation
/// that is safe to retry, and because the closure is recomputed each push and
/// `has_blob` skips what the remote already holds, a retry resumes rather than
/// restarts (Decision L8).
pub async fn push_content_to_remote_limited(
    ship: &Ship,
    remote: &mut ContentRemote,
    ref_name: &str,
    limits: &mut LimiterSet,
) -> Result<ContentPushOutcome, StewardError> {
    let seq = ship
        .control_table()
        .latest_spine_seq()
        .await?
        .ok_or_else(|| {
            StewardError::Content("no content-changing commit to push (empty pond)".to_string())
        })?;

    let commit_hash_hex = ship
        .control_table()
        .commit_hash_at(seq)
        .await?
        .ok_or_else(|| {
            StewardError::Content(format!("no commit spine recorded at write seq {seq}"))
        })?;
    let commit_object_hex = ship
        .control_table()
        .commit_object_at(seq)
        .await?
        .ok_or_else(|| {
            StewardError::Content(format!("no commit object recorded at write seq {seq}"))
        })?;

    let tip = ObjectHash::from_hex(&commit_hash_hex)
        .map_err(|e| StewardError::Content(format!("invalid commit hash: {e}")))?;
    let commit_bytes = hex::decode(&commit_object_hex)
        .map_err(|e| StewardError::Content(format!("invalid commit object hex: {e}")))?;

    let recomputed = ObjectHash::of_bytes(&commit_bytes);
    if recomputed != tip {
        return Err(StewardError::Content(format!(
            "commit object hashes to {} but the spine records tip {}",
            recomputed.to_hex(),
            tip.to_hex()
        )));
    }

    let materialized = materialize_content_objects(ship).await?;

    let mut objects: Vec<(ObjectHash, Vec<u8>)> = Vec::with_capacity(materialized.inline.len() + 2);
    for (hash, bytes) in materialized.inline {
        objects.push((hash, bytes));
    }
    // Large blobs (>64KB) live externally under `_large_files/` and are recorded
    // only by hash (Decision D7).  They are NOT inlined as `objects` rows: a
    // multi-gigabyte value would bloat the remote Delta table.  Instead each is
    // streamed local->remote into the remote's content-addressed blob store,
    // keyed by its content hash, never loading the whole blob into memory.  Skip
    // blobs the remote already holds so re-pushes stay cheap.
    for hash in &materialized.external_blobs {
        limits.check(LimitUnit::Ops, 1)?;
        let present = remote
            .has_blob(*hash)
            .await
            .map_err(|e| StewardError::Content(e.to_string()))?;
        limits.record(LimitUnit::Ops, 1);
        if present {
            continue;
        }
        // Opening the reader is local and free, and it yields the exact number
        // of raw content bytes that `put_blob` will stream (the parquet
        // wrapper is not sent).  Charging that figure means the budget is
        // spent in the same units the remote bills in, with no extra stat and
        // no remote round-trip to learn the size.
        let reader = ship
            .data_persistence()
            .open_large_file_reader_by_hash(&hash.to_hex())
            .await
            .map_err(|e| StewardError::Content(format!("open external blob: {e}")))?;
        let size = reader.total_size();
        limits.check(LimitUnit::Bytes, size)?;
        limits.check(LimitUnit::Ops, 1)?;
        remote
            .put_blob(*hash, reader)
            .await
            .map_err(|e| StewardError::Content(format!("stream external blob: {e}")))?;
        limits.record(LimitUnit::Bytes, size);
        limits.record(LimitUnit::Ops, 1);
    }
    // The node manifest the commit references (Section 4.5); a consumer fetches
    // it to adopt the source's node_ids.  Verify it hashes to the commit's
    // recorded manifest hash so the tip can never name a manifest the remote
    // lacks or disagrees with.
    let (manifest_hash, manifest_bytes) = materialized.manifest.ok_or_else(|| {
        StewardError::Content("materialized objects carry no node manifest".to_string())
    })?;
    let commit = sync_store::content::Commit::decode(&commit_bytes)
        .map_err(|e| StewardError::Content(format!("decode commit object: {e}")))?;
    if commit.node_manifest_hash != manifest_hash {
        return Err(StewardError::Content(format!(
            "node manifest hashes to {} but the commit names {}",
            manifest_hash.to_hex(),
            commit.node_manifest_hash.to_hex()
        )));
    }
    objects.push((manifest_hash, manifest_bytes));
    objects.push((tip, commit_bytes));

    // The batched commit is one remote request carrying every inline object.
    let inline_bytes: u64 = objects.iter().map(|(_, b)| b.len() as u64).sum();
    limits.check(LimitUnit::Bytes, inline_bytes)?;
    limits.check(LimitUnit::Ops, 1)?;
    let remote_txn_seq = remote
        .push_commit(&objects, ref_name, tip)
        .await
        .map_err(|e| StewardError::Content(e.to_string()))?;
    limits.record(LimitUnit::Bytes, inline_bytes);
    limits.record(LimitUnit::Ops, 1);

    Ok(ContentPushOutcome {
        ref_name: ref_name.to_string(),
        tip,
        objects_pushed: objects.len(),
        remote_txn_seq,
    })
}
