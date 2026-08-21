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
/// Charging happens beneath this function, in the object store the remote is
/// built on, so what is spent is what physically crossed the wire:
///
/// - [`provider::factory::rate_limit::LimitUnit::Ops`] -- one per storage
///   request, including the log reads and parquet writes a Delta commit
///   performs internally.
/// - [`provider::factory::rate_limit::LimitUnit::Bytes`] -- bytes in **both**
///   directions, because a provider bills egress and that is the direction a
///   runaway read spends.
///
/// An earlier version charged one op per `ContentRemote` call here; a traced
/// push showed that undercounted requests by ~600x and ignored every received
/// byte.  See [`sync_store::metered_store`].
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
    // The budget is bound to the remote's own URL, so the store charges it by
    // identity however many tasks the Delta layer spreads the work across.
    let url = remote.url();
    crate::storage_meter::metered_op(&url, limits, push_content_inner(ship, remote, ref_name)).await
}

/// Open the remote at `url` and push to it, with both charged to `limits`.
///
/// Prefer this to opening a remote and then calling
/// [`push_content_to_remote_limited`].  Opening a Delta table is not a local
/// act: it lists the log, reads every commit since the last checkpoint, and
/// reads the checkpoint itself.  Measured against MinIO, the open was the
/// larger half of a tick's traffic -- so a budget that starts at the push
/// governs the smaller half and lets the rest through for free, which is
/// precisely the accounting error this whole mechanism exists to prevent.
pub async fn open_and_push_to_remote_limited(
    ship: &Ship,
    url: &str,
    storage_options: std::collections::HashMap<String, String>,
    ref_name: &str,
    limits: &mut LimiterSet,
) -> Result<ContentPushOutcome, StewardError> {
    crate::storage_meter::metered_op(
        url,
        limits,
        // Boxed only to keep this future off the caller's stack: it holds an
        // open remote plus the whole push, and the pull path shares a thread
        // with it.
        Box::pin(async move {
            let mut remote = ContentRemote::open_at_url(url, storage_options)
                .await
                .map_err(|e| StewardError::Aborted(format!("open remote {}: {}", url, e)))?;
            push_content_inner(ship, &mut remote, ref_name).await
        }),
    )
    .await
}

/// The push itself.
///
/// No charging appears in this function on purpose.  Every request it makes
/// passes through a metered object store (see [`sync_store::metered_store`]),
/// so the hundreds of requests a Delta commit actually performs are counted
/// instead of the single call that started them.
async fn push_content_inner(
    ship: &Ship,
    remote: &mut ContentRemote,
    ref_name: &str,
) -> Result<ContentPushOutcome, StewardError> {
    let commit_log = crate::content_tree::read_log_leaves(
        ship.data_persistence().table().clone(),
        &ship.control_table().pond_id_uuid().to_string(),
    )
    .await?;
    let commit_bytes = commit_log.last().cloned().ok_or_else(|| {
        StewardError::Content("no content-changing commit to push (empty pond)".to_string())
    })?;
    let tip = sync_store::content::Commit::decode(&commit_bytes)
        .map_err(|e| StewardError::Content(format!("decode commit-log tip: {e}")))?
        .hash();

    let mut materialized = materialize_content_objects(ship).await?;

    let mut objects: Vec<(ObjectHash, Vec<u8>)> = Vec::with_capacity(materialized.inline.len() + 2);
    for (hash, bytes) in std::mem::take(&mut materialized.inline) {
        objects.push((hash, bytes));
    }
    // Large blobs (>64KB) live externally under `_large_files/` and are recorded
    // only by hash (Decision D7).  They are NOT inlined as `objects` rows: a
    // multi-gigabyte value would bloat the remote Delta table.  Instead each is
    // streamed local->remote into the remote's content-addressed blob store,
    // keyed by its content hash, never loading the whole blob into memory.  Skip
    // blobs the remote already holds so re-pushes stay cheap.
    // Ask which blobs the remote already holds ONCE, as a listing, rather than
    // with a HEAD per blob.  Per-blob probing costs a billed request for every
    // blob in the pond's accumulated history on every push, including pushes
    // that upload nothing -- measured at ~180 requests per push on a staging
    // pond, or ~4300 a day at an hourly cadence, to re-confirm blobs that had
    // not changed.  That is a cost proportional to history rather than to work,
    // and it is exactly the kind of quiet spending the budgets exist to catch.
    // (It was in fact the budget that caught it.)
    let present_blobs = if materialized.external_blobs.is_empty() {
        // Nothing to ask about, so do not spend a request asking.
        std::collections::HashSet::new()
    } else {
        remote
            .list_blobs()
            .await
            .map_err(|e| StewardError::Content(e.to_string()))?
    };

    for hash in &materialized.external_blobs {
        if present_blobs.contains(hash) {
            continue;
        }
        // Opening the reader is local and free.  The bytes it streams are
        // charged by the object store as they cross the wire, which also
        // catches whatever framing the provider adds on top of them.
        let reader = ship
            .data_persistence()
            .open_large_file_reader_by_hash(&hash.to_hex())
            .await
            .map_err(|e| StewardError::Content(format!("open external blob: {e}")))?;
        remote
            .put_blob(*hash, reader)
            .await
            .map_err(|e| StewardError::Content(format!("stream external blob: {e}")))?;
    }
    // The node manifest the commit references (Section 4.5); a consumer fetches
    // it to adopt the source's node_ids.  Verify it hashes to the commit's
    // recorded manifest hash so the tip can never name a manifest the remote
    // lacks or disagrees with.
    let (manifest_hash, manifest_bytes) = materialized.manifest.take().ok_or_else(|| {
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
    for bytes in commit_log {
        let commit = sync_store::content::Commit::decode(&bytes)
            .map_err(|e| StewardError::Content(format!("decode commit-log leaf: {e}")))?;
        let hash = commit.hash();
        objects.push((hash, bytes));
    }
    if !objects.iter().any(|(hash, _)| *hash == tip) {
        return Err(StewardError::Content(
            "commit-log does not contain the advertised tip".to_string(),
        ));
    }

    // The batched commit is one call carrying every inline object; what it
    // costs is whatever the resulting Delta transaction actually performs.
    let remote_txn_seq = remote
        .push_commit(&objects, ref_name, tip)
        .await
        .map_err(|e| StewardError::Content(e.to_string()))?;
    let objects_pushed = objects.len();

    Ok(ContentPushOutcome {
        ref_name: ref_name.to_string(),
        tip,
        objects_pushed,
        remote_txn_seq,
    })
}
