// SPDX-License-Identifier: Apache-2.0

//! Native-v2 low-cost publication producer.
//!
//! Publication order is immutable payloads/receipts, immutable packs,
//! immutable per-push record, then one dedicated Delta active-row CAS.
//! Ordinary series commits carry one linked suffix pack per changed series;
//! only first publication to an empty remote performs a full current-snapshot
//! materialization and emits whole-range root packs.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use sync_store::content::{
    Commit, ContentObjectKind, ManifestChange, ManifestMapNode, ObjectDescriptor, ObjectHash,
    PackDescriptor, PackIndex, PublicationRecord, SeriesManifest,
    verify_complete_pack_against_manifest,
};
use sync_store::{
    ContentRemote, PublicationExpectation, PublicationFailurePoint, PublicationState,
};
use tinyfs::EntryType;

use crate::content_tree::{build_initial_pack_index, materialize_content_objects};
use crate::limiter::LimiterSet;
use crate::{Ship, StewardError};

/// Result of one successful publication or converged retry.
#[derive(Debug, Clone)]
pub struct ContentPushOutcome {
    /// Ref made current.
    pub ref_name: String,
    /// Visible snapshot commit.
    pub tip: ObjectHash,
    /// Manifest root made current.
    pub manifest_root: ObjectHash,
    /// Immutable publication-record head.
    pub publication_record: ObjectHash,
    /// Active-row generation.
    pub generation: i64,
    /// Number of canonical payload keys physically created by this call.
    pub objects_pushed: usize,
    /// Compatibility name for the final visibility sequence; equal to
    /// [`Self::generation`] in native v2.
    pub remote_txn_seq: i64,
    /// Exact state callers persist as the structured acknowledgement.
    pub state: PublicationState,
}

#[derive(Debug)]
pub(crate) struct Snapshot {
    pub(crate) tip: ObjectHash,
    pub(crate) commit: Commit,
}

#[derive(Debug, Default)]
struct PublicationDelta {
    objects: BTreeSet<ObjectDescriptor>,
    packs: BTreeSet<PackDescriptor>,
    changes: BTreeMap<String, ManifestChange>,
    bytes: BTreeMap<ObjectHash, Vec<u8>>,
    pack_bytes: BTreeMap<ObjectHash, Vec<u8>>,
}

#[derive(Debug)]
struct CollectedIncrementalDelta {
    delta: PublicationDelta,
    commits: Vec<(ObjectHash, Commit)>,
}

/// Resolve the current tip and manifest root from the pond-local immutable
/// commit object.
pub(crate) async fn current_snapshot(ship: &Ship) -> Result<Snapshot, StewardError> {
    let tip = crate::content_tree::log_tip_commit_hash(
        ship.data_persistence().table().clone(),
        ship.data_persistence().store_path(),
        &ship.control_table().pond_id_uuid().to_string(),
    )
    .await?
    .ok_or_else(|| {
        StewardError::Content("no content-changing commit to push (empty pond)".to_string())
    })?;
    let bytes = crate::local_content::LocalContentStore::new(ship.pond_path()).read(tip)?;
    let commit = Commit::decode(&bytes)
        .map_err(|error| StewardError::Content(format!("decode local tip {tip}: {error}")))?;
    if commit.hash() != tip {
        return Err(StewardError::Content(format!(
            "local tip object hashes to {}, expected {tip}",
            commit.hash()
        )));
    }
    Ok(Snapshot { tip, commit })
}

/// Whether a valid structured producer acknowledgement exactly matches the
/// current local snapshot.
///
/// This performs local reads only and is designed to run before credentials,
/// limiters, or a remote are prepared.
pub async fn remote_ref_is_acknowledged(
    ship: &Ship,
    url: &str,
    ref_name: &str,
) -> Result<bool, StewardError> {
    let snapshot = current_snapshot(ship).await?;
    let pond_id = ship.control_table().pond_id_uuid();
    let Some(acknowledgement) =
        crate::read_push_ack(ship.control_table(), url, pond_id, ref_name).await?
    else {
        return Ok(false);
    };
    let state = acknowledgement.state(url, pond_id, ref_name)?;
    Ok(state.snapshot_tip == snapshot.tip && state.manifest_root == snapshot.commit.manifest_root)
}

/// Convenience check for the ordinary `main` publication ref.
pub async fn remote_tip_is_acknowledged(ship: &Ship, url: &str) -> Result<bool, StewardError> {
    remote_ref_is_acknowledged(ship, url, "main").await
}

/// Publish the current snapshot without a limiter.
pub async fn push_content_to_remote(
    ship: &Ship,
    remote: &mut ContentRemote,
    ref_name: &str,
) -> Result<ContentPushOutcome, StewardError> {
    let mut unlimited = LimiterSet::unlimited();
    push_content_to_remote_limited(ship, remote, ref_name, &mut unlimited).await
}

/// Publish the current snapshot under physical object-store limits.
pub async fn push_content_to_remote_limited(
    ship: &Ship,
    remote: &mut ContentRemote,
    ref_name: &str,
    limits: &mut LimiterSet,
) -> Result<ContentPushOutcome, StewardError> {
    let url = remote.url();
    crate::storage_meter::metered_op(
        &url,
        limits,
        push_content_inner(ship, remote, ref_name, &url),
    )
    .await
}

/// Open and publish with the remote open itself covered by the limiter.
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
        Box::pin(async move {
            let mut remote = ContentRemote::open_at_url(url, storage_options)
                .await
                .map_err(|error| StewardError::Aborted(format!("open remote {url}: {error}")))?;
            push_content_inner(ship, &mut remote, ref_name, url).await
        }),
    )
    .await
}

async fn push_content_inner(
    ship: &Ship,
    remote: &mut ContentRemote,
    ref_name: &str,
    acknowledgement_url: &str,
) -> Result<ContentPushOutcome, StewardError> {
    let snapshot = current_snapshot(ship).await?;
    let pond_id = ship.control_table().pond_id_uuid();
    if remote.pond_id() != pond_id {
        return Err(StewardError::Content(format!(
            "remote pond {} does not match local pond {pond_id}",
            remote.pond_id()
        )));
    }

    let local_ack =
        crate::read_push_ack(ship.control_table(), acknowledgement_url, pond_id, ref_name)
            .await?
            .map(|acknowledgement| acknowledgement.state(acknowledgement_url, pond_id, ref_name))
            .transpose()?;
    let remote_state = remote
        .current_publication(ref_name)
        .await
        .map_err(|error| StewardError::Content(format!("read publication state: {error}")))?;

    if let Some(current) = remote_state.as_ref() {
        if let Some(acknowledged) = &local_ack {
            if !crate::same_publication_identity(current, acknowledged) {
                authenticate_remote_continuation(ship, remote, current, acknowledged).await?;
            }
        } else if current.snapshot_tip != snapshot.tip
            || current.manifest_root != snapshot.commit.manifest_root
        {
            authenticate_remote_history(ship, remote, current, true).await?;
        }
    } else if local_ack.is_some() {
        return Err(StewardError::Content(
            "remote publication row disappeared after local acknowledgement".to_string(),
        ));
    }

    if let Some(state) = &remote_state
        && state.snapshot_tip == snapshot.tip
        && state.manifest_root == snapshot.commit.manifest_root
    {
        if local_ack.is_some() {
            authenticate_remote_head(remote, state).await?;
        } else {
            authenticate_remote_history(ship, remote, state, false).await?;
        }
        validate_local_publication_state(ship, state)?;
        return Ok(outcome_for_existing(state.clone()));
    }

    let mut delta = match remote_state.as_ref() {
        None => initial_publication_delta(ship, &snapshot).await?,
        Some(baseline) => {
            validate_local_publication_state(ship, baseline)?;
            collect_incremental_delta(ship, &snapshot, baseline.snapshot_tip)
                .await?
                .delta
        }
    };

    let local_store = crate::local_content::LocalContentStore::new(ship.pond_path());
    let mut physical_objects = 0usize;
    remote
        .publication_stage(PublicationFailurePoint::BeforeObjects)
        .map_err(|error| StewardError::Content(error.to_string()))?;
    let mut processed_objects = 0usize;
    for descriptor in delta.objects.iter().copied() {
        let cached = match delta.bytes.get(&descriptor.hash) {
            Some(bytes) => Some(bytes.clone()),
            None => local_store.read_optional(descriptor.hash)?,
        };
        let outcome = if let Some(bytes) = cached {
            remote
                .put_immutable_object(descriptor, &bytes)
                .await
                .map_err(|error| {
                    StewardError::Content(format!(
                        "publish immutable {} object {}: {error}",
                        descriptor.kind.as_str(),
                        descriptor.hash
                    ))
                })?
        } else if descriptor.kind == ContentObjectKind::RawBlob {
            let reader = ship
                .data_persistence()
                .open_large_file_reader_by_hash(&descriptor.hash.to_hex())
                .await
                .map_err(|error| {
                    StewardError::Content(format!(
                        "open local large payload {}: {error}",
                        descriptor.hash
                    ))
                })?;
            remote
                .put_immutable_object_stream(descriptor, reader)
                .await
                .map_err(|error| {
                    StewardError::Content(format!(
                        "stream immutable object {}: {error}",
                        descriptor.hash
                    ))
                })?
        } else {
            return Err(StewardError::Content(format!(
                "local immutable {} object {} is absent",
                descriptor.kind.as_str(),
                descriptor.hash
            )));
        };
        physical_objects += usize::from(outcome.payload_created);
        processed_objects += 1;
        remote
            .publication_stage(PublicationFailurePoint::AfterObject(processed_objects))
            .map_err(|error| StewardError::Content(error.to_string()))?;
    }
    remote
        .publication_stage(PublicationFailurePoint::AfterObjects)
        .map_err(|error| StewardError::Content(error.to_string()))?;

    for descriptor in delta.packs.iter().copied() {
        let bytes = match delta.pack_bytes.remove(&descriptor.pack_hash) {
            Some(bytes) => Some(bytes),
            None => local_store.read_optional(descriptor.pack_hash)?,
        }
        .ok_or_else(|| {
            StewardError::Content(format!(
                "local pack object {} is absent",
                descriptor.pack_hash
            ))
        })?;
        let _ = remote
            .put_immutable_pack(descriptor, &bytes)
            .await
            .map_err(|error| {
                StewardError::Content(format!(
                    "publish immutable pack {}: {error}",
                    descriptor.pack_hash
                ))
            })?;
    }
    remote
        .publication_stage(PublicationFailurePoint::AfterPacks)
        .map_err(|error| StewardError::Content(error.to_string()))?;

    let parent_record = remote_state.as_ref().map(|state| state.publication_record);
    let record = PublicationRecord::new(
        pond_id,
        ref_name,
        snapshot.tip,
        snapshot.commit.manifest_root,
        parent_record,
        delta.objects.into_iter().collect(),
        delta.packs.into_iter().collect(),
        delta.changes.into_values().collect(),
    )
    .map_err(StewardError::Content)?;
    let record_hash = record.hash();
    let _ = remote
        .put_publication_record(&record)
        .await
        .map_err(|error| {
            StewardError::Content(format!("publish immutable publication record: {error}"))
        })?;
    remote
        .publication_stage(PublicationFailurePoint::AfterRecord)
        .map_err(|error| StewardError::Content(error.to_string()))?;

    let (expectation, generation) = match remote_state.as_ref() {
        Some(state) => (
            PublicationExpectation::Existing {
                generation: state.generation,
                publication_record: state.publication_record,
            },
            state.generation.checked_add(1).ok_or_else(|| {
                StewardError::Content("publication generation overflow".to_string())
            })?,
        ),
        None => (PublicationExpectation::Missing, 1),
    };
    let state = PublicationState::new(
        pond_id,
        ref_name,
        snapshot.tip,
        snapshot.commit.manifest_root,
        record_hash,
        generation,
        chrono::Utc::now().timestamp_micros(),
    )
    .map_err(|error| StewardError::Content(error.to_string()))?;
    let state = remote
        .compare_and_swap_publication(expectation, state)
        .await
        .map_err(|error| StewardError::Content(format!("advance publication ref: {error}")))?;
    remote
        .publication_stage(PublicationFailurePoint::AfterRef)
        .map_err(|error| StewardError::Content(error.to_string()))?;

    Ok(ContentPushOutcome {
        ref_name: ref_name.to_string(),
        tip: state.snapshot_tip,
        manifest_root: state.manifest_root,
        publication_record: state.publication_record,
        generation: state.generation,
        objects_pushed: physical_objects,
        remote_txn_seq: state.generation,
        state,
    })
}

fn outcome_for_existing(state: PublicationState) -> ContentPushOutcome {
    ContentPushOutcome {
        ref_name: state.ref_name.clone(),
        tip: state.snapshot_tip,
        manifest_root: state.manifest_root,
        publication_record: state.publication_record,
        generation: state.generation,
        objects_pushed: 0,
        remote_txn_seq: state.generation,
        state,
    }
}

async fn authenticate_remote_continuation(
    ship: &Ship,
    remote: &ContentRemote,
    current: &PublicationState,
    acknowledged: &PublicationState,
) -> Result<(), StewardError> {
    if current.pond_id != acknowledged.pond_id
        || current.ref_name != acknowledged.ref_name
        || current.format != acknowledged.format
        || current.generation <= acknowledged.generation
    {
        return Err(StewardError::Content(format!(
            "remote publication baseline changed concurrently: local acknowledgement is \
             generation {} record {}, remote is generation {} record {}",
            acknowledged.generation,
            acknowledged.publication_record,
            current.generation,
            current.publication_record
        )));
    }
    validate_local_publication_state(ship, acknowledged)?;
    let current_snapshot = local_snapshot_for_publication_state(ship, current)?;
    let local_interval =
        collect_incremental_delta(ship, &current_snapshot, acknowledged.snapshot_tip).await?;
    let expected_hops = current.generation - acknowledged.generation;
    if expected_hops <= 0
        || usize::try_from(expected_hops)
            .ok()
            .is_none_or(|hops| hops > local_interval.commits.len())
    {
        return Err(StewardError::Content(format!(
            "publication generation delta {expected_hops} exceeds the authenticated local commit \
             interval of {} commit(s)",
            local_interval.commits.len()
        )));
    }
    let mut next = current.publication_record;
    let mut recovered_newest_first = Vec::new();
    let mut reached_boundary = false;
    for hop in 0..=local_interval.commits.len() {
        let record = remote
            .get_publication_record(next)
            .await
            .map_err(|error| {
                StewardError::Content(format!(
                    "read publication record {next} while recovering producer acknowledgement: \
                     {error}"
                ))
            })?
            .ok_or_else(|| {
                StewardError::Content(format!(
                    "publication record {next} is absent while recovering producer acknowledgement"
                ))
            })?;
        if record.hash() != next
            || record.pond_id != current.pond_id
            || record.ref_name != current.ref_name
            || record.format != current.format
        {
            return Err(StewardError::Content(format!(
                "publication record {next} has mismatched producer/ref identity"
            )));
        }
        if hop == 0
            && (record.snapshot_tip != current.snapshot_tip
                || record.manifest_root != current.manifest_root)
        {
            return Err(StewardError::Content(
                "remote publication head disagrees with its immutable record".to_string(),
            ));
        }
        if next == acknowledged.publication_record {
            if i64::try_from(hop).ok() != Some(expected_hops)
                || record.snapshot_tip != acknowledged.snapshot_tip
                || record.manifest_root != acknowledged.manifest_root
            {
                return Err(StewardError::Content(
                    "remote publication history does not match the acknowledged generation and \
                     content roots"
                        .to_string(),
                ));
            }
            reached_boundary = true;
            break;
        }
        recovered_newest_first.push((next, record.clone()));
        next = record.parent_publication_record.ok_or_else(|| {
            StewardError::Content(format!(
                "remote publication history does not descend from acknowledged record {}",
                acknowledged.publication_record
            ))
        })?;
    }
    if !reached_boundary {
        return Err(StewardError::Content(format!(
            "remote publication history does not descend from acknowledged record {}",
            acknowledged.publication_record
        )));
    }

    if i64::try_from(recovered_newest_first.len()).ok() != Some(expected_hops) {
        return Err(StewardError::Content(format!(
            "remote publication lineage contains {} recovered record(s), expected {expected_hops}",
            recovered_newest_first.len()
        )));
    }

    authenticate_record_intervals(
        ship,
        remote,
        acknowledged.snapshot_tip,
        recovered_newest_first
            .iter()
            .rev()
            .map(|(_, record)| record),
        true,
    )
    .await
}

async fn authenticate_remote_history(
    ship: &Ship,
    remote: &ContentRemote,
    current: &PublicationState,
    verify_availability: bool,
) -> Result<(), StewardError> {
    let current_snapshot = local_snapshot_for_publication_state(ship, current)?;
    let local_history = collect_local_history_to_genesis(ship, &current_snapshot)?;
    if usize::try_from(current.generation)
        .ok()
        .is_none_or(|generation| generation > local_history.len())
    {
        return Err(StewardError::Content(format!(
            "remote publication generation {} exceeds the authenticated local history of {} \
             commit(s)",
            current.generation,
            local_history.len()
        )));
    }

    let mut newest_first = Vec::new();
    let mut next = Some(current.publication_record);
    let mut seen = HashSet::new();
    while let Some(hash) = next {
        if newest_first.len() >= local_history.len() || !seen.insert(hash) {
            return Err(StewardError::Content(
                "remote publication history is cyclic or longer than authenticated local commit \
                 history"
                    .to_string(),
            ));
        }
        let record = read_remote_record(remote, hash, current).await?;
        if newest_first.is_empty()
            && (record.snapshot_tip != current.snapshot_tip
                || record.manifest_root != current.manifest_root)
        {
            return Err(StewardError::Content(
                "remote publication head disagrees with its immutable record".to_string(),
            ));
        }
        next = record.parent_publication_record;
        newest_first.push((hash, record));
    }
    if i64::try_from(newest_first.len()).ok() != Some(current.generation) {
        return Err(StewardError::Content(format!(
            "remote publication lineage contains {} record(s), but active generation is {}",
            newest_first.len(),
            current.generation
        )));
    }

    newest_first.reverse();
    let (_, genesis) = newest_first.first().ok_or_else(|| {
        StewardError::Content("remote publication history has no genesis record".to_string())
    })?;
    authenticate_genesis_record(ship, remote, genesis, verify_availability).await?;
    authenticate_record_intervals(
        ship,
        remote,
        genesis.snapshot_tip,
        newest_first.iter().skip(1).map(|(_, record)| record),
        verify_availability,
    )
    .await
}

async fn authenticate_remote_head(
    remote: &ContentRemote,
    state: &PublicationState,
) -> Result<(), StewardError> {
    let record = read_remote_record(remote, state.publication_record, state).await?;
    if record.snapshot_tip != state.snapshot_tip || record.manifest_root != state.manifest_root {
        return Err(StewardError::Content(
            "remote publication head disagrees with its immutable record".to_string(),
        ));
    }
    let _ = remote
        .immutable_object_size(state.snapshot_tip)
        .await
        .map_err(|error| {
            StewardError::Content(format!(
                "authenticate remote tip commit {}: {error}",
                state.snapshot_tip
            ))
        })?
        .ok_or_else(|| {
            StewardError::Content(format!(
                "remote publication tip commit {} is absent",
                state.snapshot_tip
            ))
        })?;
    Ok(())
}

async fn read_remote_record(
    remote: &ContentRemote,
    hash: ObjectHash,
    state: &PublicationState,
) -> Result<PublicationRecord, StewardError> {
    let record = remote
        .get_publication_record(hash)
        .await
        .map_err(|error| StewardError::Content(format!("read publication record {hash}: {error}")))?
        .ok_or_else(|| StewardError::Content(format!("publication record {hash} is absent")))?;
    if record.hash() != hash
        || record.pond_id != state.pond_id
        || record.ref_name != state.ref_name
        || record.format != state.format
    {
        return Err(StewardError::Content(format!(
            "publication record {hash} has mismatched producer/ref identity"
        )));
    }
    Ok(record)
}

fn collect_local_history_to_genesis(
    ship: &Ship,
    snapshot: &Snapshot,
) -> Result<Vec<(ObjectHash, Commit)>, StewardError> {
    let store = crate::local_content::LocalContentStore::new(ship.pond_path());
    let mut newest_first = Vec::new();
    let mut next = Some(snapshot.tip);
    let mut seen = HashSet::new();
    while let Some(hash) = next {
        if !seen.insert(hash) {
            return Err(StewardError::Content(format!(
                "cycle in local commit ancestry at {hash}"
            )));
        }
        let bytes = store.read(hash)?;
        let commit = Commit::decode(&bytes).map_err(|error| {
            StewardError::Content(format!("decode local commit {hash}: {error}"))
        })?;
        if commit.hash() != hash {
            return Err(StewardError::Content(format!(
                "local commit {hash} hashes to {}",
                commit.hash()
            )));
        }
        next = commit.parent_commit_hash;
        newest_first.push((hash, commit));
    }
    newest_first.reverse();
    Ok(newest_first)
}

async fn authenticate_record_intervals<'a>(
    ship: &Ship,
    remote: &ContentRemote,
    mut parent_tip: ObjectHash,
    records: impl IntoIterator<Item = &'a PublicationRecord>,
    verify_availability: bool,
) -> Result<(), StewardError> {
    for record in records {
        if record.snapshot_tip == parent_tip {
            return Err(StewardError::Content(format!(
                "publication record {} does not advance its parent snapshot",
                record.hash()
            )));
        }
        let snapshot = Snapshot {
            tip: record.snapshot_tip,
            commit: local_snapshot_for_record(ship, record)?,
        };
        let expected = collect_incremental_delta(ship, &snapshot, parent_tip).await?;
        let recovered = publication_delta_from_record(record)?;
        require_matching_delta(&expected.delta, &recovered)?;
        if verify_availability {
            require_remote_delta_objects(remote, &recovered).await?;
        }
        parent_tip = record.snapshot_tip;
    }
    Ok(())
}

fn local_snapshot_for_record(
    ship: &Ship,
    record: &PublicationRecord,
) -> Result<Commit, StewardError> {
    let state = PublicationState::new(
        record.pond_id,
        &record.ref_name,
        record.snapshot_tip,
        record.manifest_root,
        record.hash(),
        1,
        0,
    )
    .map_err(|error| StewardError::Content(error.to_string()))?;
    Ok(local_snapshot_for_publication_state(ship, &state)?.commit)
}

fn publication_delta_from_record(
    record: &PublicationRecord,
) -> Result<PublicationDelta, StewardError> {
    let mut delta = PublicationDelta::default();
    delta
        .objects
        .extend(record.introduced_objects.iter().copied());
    delta.packs.extend(record.introduced_packs.iter().copied());
    merge_manifest_changes(
        &mut delta.changes,
        record.manifest_changes.iter().cloned(),
        "publication record",
    )?;
    Ok(delta)
}

async fn authenticate_genesis_record(
    ship: &Ship,
    remote: &ContentRemote,
    record: &PublicationRecord,
    verify_availability: bool,
) -> Result<(), StewardError> {
    if record.parent_publication_record.is_some() {
        return Err(StewardError::Content(
            "publication genesis record unexpectedly has a parent".to_string(),
        ));
    }
    let commit = local_snapshot_for_record(ship, record)?;
    let store = crate::local_content::LocalContentStore::new(ship.pond_path());
    let mut expected = PublicationDelta::default();
    let _ = expected.objects.insert(ObjectDescriptor::new(
        record.snapshot_tip,
        ContentObjectKind::Commit,
    ));
    let mut stack = vec![commit.manifest_root];
    let mut seen_nodes = HashSet::new();
    let mut manifest_records = Vec::new();
    let mut series = BTreeMap::<ObjectHash, SeriesManifest>::new();
    while let Some(hash) = stack.pop() {
        if !seen_nodes.insert(hash) {
            continue;
        }
        let bytes = store.read(hash)?;
        let node = ManifestMapNode::decode(&bytes).map_err(|error| {
            StewardError::Content(format!("decode local manifest-map node {hash}: {error}"))
        })?;
        let _ = expected
            .objects
            .insert(ObjectDescriptor::new(hash, ContentObjectKind::ManifestNode));
        match node {
            ManifestMapNode::Branch { left, right, .. } => {
                stack.push(right);
                stack.push(left);
            }
            ManifestMapNode::Leaf {
                record: manifest_record,
                ..
            } => {
                let entry = &manifest_record.entry;
                let kind = match entry.entry_type {
                    EntryType::DirectoryPhysical => Some(ContentObjectKind::Tree),
                    EntryType::FilePhysicalVersion
                    | EntryType::TablePhysicalVersion
                    | EntryType::Symlink => Some(ContentObjectKind::RawBlob),
                    EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries => {
                        let bytes = store.read(entry.child_hash)?;
                        let manifest = SeriesManifest::decode(&bytes).map_err(|error| {
                            StewardError::Content(format!(
                                "decode local series manifest {}: {error}",
                                entry.child_hash
                            ))
                        })?;
                        if manifest.hash() != entry.child_hash {
                            return Err(StewardError::Content(format!(
                                "local series manifest hashes to {}, expected {}",
                                manifest.hash(),
                                entry.child_hash
                            )));
                        }
                        let _ = series.insert(entry.child_hash, manifest);
                        Some(ContentObjectKind::SeriesManifest)
                    }
                    EntryType::DirectoryDynamic
                    | EntryType::FileDynamic
                    | EntryType::TableDynamic => Some(ContentObjectKind::Recipe),
                };
                if let Some(kind) = kind {
                    let _ = expected
                        .objects
                        .insert(ObjectDescriptor::new(entry.child_hash, kind));
                }
                manifest_records.push(manifest_record);
            }
        }
    }
    let root = manifest_records
        .iter()
        .find(|candidate| candidate.entry.node_id == tinyfs::ROOT_UUID)
        .ok_or_else(|| {
            StewardError::Content("local genesis manifest has no root record".to_string())
        })?;
    if root.entry.child_hash != commit.root_tree_hash {
        return Err(StewardError::Content(
            "local genesis manifest root record disagrees with its commit tree root".to_string(),
        ));
    }
    for manifest_record in manifest_records {
        let change =
            ManifestChange::new(None, Some(manifest_record)).map_err(StewardError::Content)?;
        let _ = expected
            .changes
            .insert(change.node_id().to_string(), change);
    }

    for (series_hash, manifest) in series {
        if manifest.leaf_count() == 0 {
            continue;
        }
        let mut candidates = record
            .introduced_packs
            .iter()
            .copied()
            .filter(|descriptor| descriptor.series_hash == series_hash);
        let descriptor = candidates.next().ok_or_else(|| {
            StewardError::Content(format!(
                "publication genesis record has no complete pack for series {series_hash}"
            ))
        })?;
        if candidates.next().is_some() {
            return Err(StewardError::Content(format!(
                "publication genesis record has multiple packs for series {series_hash}"
            )));
        }
        let bytes = remote
            .get_immutable_pack(descriptor)
            .await
            .map_err(|error| {
                StewardError::Content(format!(
                    "read publication genesis pack {}: {error}",
                    descriptor.pack_hash
                ))
            })?
            .ok_or_else(|| {
                StewardError::Content(format!(
                    "publication genesis pack {} is absent",
                    descriptor.pack_hash
                ))
            })?;
        let pack = PackIndex::decode(&bytes).map_err(|error| {
            StewardError::Content(format!(
                "decode publication genesis pack {}: {error}",
                descriptor.pack_hash
            ))
        })?;
        verify_complete_pack_against_manifest(series_hash, &manifest, &pack).map_err(|error| {
            StewardError::Content(format!(
                "publication genesis pack {} does not authenticate series {series_hash}: {error}",
                descriptor.pack_hash
            ))
        })?;
        let _ = expected.packs.insert(descriptor);
        for span in pack.object_spans() {
            let _ = expected.objects.insert(ObjectDescriptor::new(
                span.object_hash(),
                ContentObjectKind::RawBlob,
            ));
        }
    }
    let recovered = publication_delta_from_record(record)?;
    require_matching_delta(&expected, &recovered)?;
    if verify_availability {
        require_remote_delta_objects(remote, &recovered).await?;
    }
    Ok(())
}

fn validate_local_publication_state(
    ship: &Ship,
    state: &PublicationState,
) -> Result<(), StewardError> {
    let _ = local_snapshot_for_publication_state(ship, state)?;
    Ok(())
}

fn local_snapshot_for_publication_state(
    ship: &Ship,
    state: &PublicationState,
) -> Result<Snapshot, StewardError> {
    let store = crate::local_content::LocalContentStore::new(ship.pond_path());
    let bytes = store.read(state.snapshot_tip).map_err(|error| {
        StewardError::Content(format!(
            "remote publication tip {} is not authenticated in local history: cannot read the \
             local commit: {error}",
            state.snapshot_tip
        ))
    })?;
    let commit = Commit::decode(&bytes).map_err(|error| {
        StewardError::Content(format!(
            "decode local publication commit {}: {error}",
            state.snapshot_tip
        ))
    })?;
    if commit.hash() != state.snapshot_tip || commit.manifest_root != state.manifest_root {
        return Err(StewardError::Content(format!(
            "remote publication tip {} does not match the local commit and manifest root",
            state.snapshot_tip
        )));
    }
    Ok(Snapshot {
        tip: state.snapshot_tip,
        commit,
    })
}

async fn initial_publication_delta(
    ship: &Ship,
    snapshot: &Snapshot,
) -> Result<PublicationDelta, StewardError> {
    let materialized = materialize_content_objects(ship).await?;
    if materialized.manifest_root != Some(snapshot.commit.manifest_root) {
        return Err(StewardError::Content(format!(
            "full materialization root {:?} disagrees with tip root {}",
            materialized.manifest_root, snapshot.commit.manifest_root
        )));
    }
    let mut delta = PublicationDelta::default();
    for (hash, object) in materialized.inline {
        for kind in object.kinds {
            let _ = delta.objects.insert(ObjectDescriptor::new(hash, kind));
        }
        let _ = delta.bytes.insert(hash, object.bytes);
    }
    for hash in materialized.external_blobs {
        let _ = delta
            .objects
            .insert(ObjectDescriptor::new(hash, ContentObjectKind::RawBlob));
    }
    let tip_bytes =
        crate::local_content::LocalContentStore::new(ship.pond_path()).read(snapshot.tip)?;
    let _ = delta.objects.insert(ObjectDescriptor::new(
        snapshot.tip,
        ContentObjectKind::Commit,
    ));
    let _ = delta.bytes.insert(snapshot.tip, tip_bytes);
    for record in materialized.manifest_records {
        let change = ManifestChange::new(None, Some(record)).map_err(StewardError::Content)?;
        let _ = delta.changes.insert(change.node_id().to_string(), change);
    }
    for material in materialized.series_material {
        if let Some(pack) = build_initial_pack_index(&material)? {
            let bytes = pack.encode();
            let pack_hash = ObjectHash::of_bytes(&bytes);
            let descriptor = PackDescriptor::new(material.series_hash, pack_hash);
            let _ = delta.packs.insert(descriptor);
            let _ = delta.pack_bytes.insert(pack_hash, bytes);
        }
    }
    Ok(delta)
}

async fn collect_incremental_delta(
    ship: &Ship,
    snapshot: &Snapshot,
    baseline_tip: ObjectHash,
) -> Result<CollectedIncrementalDelta, StewardError> {
    if baseline_tip == snapshot.tip {
        return Ok(CollectedIncrementalDelta {
            delta: PublicationDelta::default(),
            commits: Vec::new(),
        });
    }
    let store = crate::local_content::LocalContentStore::new(ship.pond_path());
    let mut cursor = snapshot.tip;
    let mut newest_first = Vec::new();
    let mut seen = HashSet::new();
    while cursor != baseline_tip {
        if !seen.insert(cursor) {
            return Err(StewardError::Content(format!(
                "cycle in local commit ancestry at {cursor}"
            )));
        }
        let bytes = store.read(cursor).map_err(|error| {
            StewardError::Content(format!(
                "unknown publication baseline {baseline_tip}: cannot read descendant commit \
                 {cursor}: {error}"
            ))
        })?;
        let commit = Commit::decode(&bytes).map_err(|error| {
            StewardError::Content(format!("decode local commit {cursor}: {error}"))
        })?;
        if commit.hash() != cursor {
            return Err(StewardError::Content(format!(
                "local commit {cursor} hashes to {}",
                commit.hash()
            )));
        }
        let parent = commit.parent_commit_hash.ok_or_else(|| {
            StewardError::Content(format!(
                "remote baseline {baseline_tip} is not an ancestor of local tip {}",
                snapshot.tip
            ))
        })?;
        newest_first.push((cursor, bytes, commit));
        cursor = parent;
    }
    newest_first.reverse();

    let mut delta = PublicationDelta::default();
    let mut commits = Vec::with_capacity(newest_first.len());
    for (hash, bytes, commit) in newest_first {
        let _ = delta
            .objects
            .insert(ObjectDescriptor::new(hash, ContentObjectKind::Commit));
        let _ = delta.bytes.insert(hash, bytes);
        delta
            .objects
            .extend(commit.introduced_objects.iter().copied());
        delta.packs.extend(commit.introduced_packs.iter().copied());
        merge_manifest_changes(
            &mut delta.changes,
            commit.manifest_changes.iter().cloned(),
            "local manifest",
        )?;
        commits.push((hash, commit));
    }
    for pack in &delta.packs {
        let bytes = store.read(pack.pack_hash)?;
        let _ = delta.pack_bytes.insert(pack.pack_hash, bytes);
    }
    Ok(CollectedIncrementalDelta { delta, commits })
}

fn merge_manifest_changes(
    changes: &mut BTreeMap<String, ManifestChange>,
    additions: impl IntoIterator<Item = ManifestChange>,
    context: &str,
) -> Result<(), StewardError> {
    for change in additions {
        let node_id = change.node_id().to_string();
        match changes.get_mut(&node_id) {
            Some(existing) => {
                if existing.after != change.before {
                    return Err(StewardError::Content(format!(
                        "{context} deltas are discontinuous at node {node_id}"
                    )));
                }
                existing.after = change.after;
                if existing.before == existing.after {
                    let _ = changes.remove(&node_id);
                }
            }
            None => {
                let _ = changes.insert(node_id, change);
            }
        }
    }
    Ok(())
}

fn require_matching_delta(
    expected: &PublicationDelta,
    recovered: &PublicationDelta,
) -> Result<(), StewardError> {
    if expected.objects != recovered.objects {
        return Err(StewardError::Content(format!(
            "recovered publication introduced object inventory does not exactly match the local \
             acknowledged commit interval (missing {}, extra {})",
            expected.objects.difference(&recovered.objects).count(),
            recovered.objects.difference(&expected.objects).count()
        )));
    }
    if expected.packs != recovered.packs {
        return Err(StewardError::Content(format!(
            "recovered publication introduced pack inventory does not exactly match the local \
             acknowledged commit interval (missing {}, extra {})",
            expected.packs.difference(&recovered.packs).count(),
            recovered.packs.difference(&expected.packs).count()
        )));
    }
    if expected.changes != recovered.changes {
        return Err(StewardError::Content(
            "recovered publication manifest changes do not exactly match the local acknowledged \
             commit interval"
                .to_string(),
        ));
    }
    Ok(())
}

async fn require_remote_delta_objects(
    remote: &ContentRemote,
    delta: &PublicationDelta,
) -> Result<(), StewardError> {
    let hashes = delta
        .objects
        .iter()
        .map(|descriptor| descriptor.hash)
        .collect::<BTreeSet<_>>();
    for hash in hashes {
        let _ = remote
            .immutable_object_size(hash)
            .await
            .map_err(|error| {
                StewardError::Content(format!(
                    "authenticate recovered immutable object {hash}: {error}"
                ))
            })?
            .ok_or_else(|| {
                StewardError::Content(format!(
                    "recovered publication names missing immutable object {hash}"
                ))
            })?;
    }
    for descriptor in &delta.packs {
        let _ = remote
            .get_immutable_pack(*descriptor)
            .await
            .map_err(|error| {
                StewardError::Content(format!(
                    "authenticate recovered immutable pack {}: {error}",
                    descriptor.pack_hash
                ))
            })?
            .ok_or_else(|| {
                StewardError::Content(format!(
                    "recovered publication names missing immutable pack {}",
                    descriptor.pack_hash
                ))
            })?;
    }
    Ok(())
}
