// SPDX-License-Identifier: Apache-2.0

//! Explicit publication of locally maintained whole-range series packs.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use sync_store::content::{
    ContentObjectKind, ManifestMapNode, ObjectDescriptor, ObjectHash, PackIndex, PayloadKind,
    SeriesManifest, verify_complete_pack_against_manifest,
};
use sync_store::{ContentRemote, PublicationState};
use tinyfs::EntryType;

use crate::limiter::LimiterSet;
use crate::{Ship, StewardError};

/// One local whole-range pack selected for explicit remote publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsolidatedPackSelection {
    /// Current immutable series manifest.
    pub series_hash: ObjectHash,
    /// Local immutable pack index.
    pub pack_hash: ObjectHash,
    /// Distinct physical objects named by this pack.
    pub objects: usize,
    /// Sum of those objects' bytes.
    pub bytes: u64,
}

/// Result of one explicit consolidated-pack publication.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConsolidatedPackPublishOutcome {
    /// Selected current series and packs, in series-hash order.
    pub selections: Vec<ConsolidatedPackSelection>,
    /// Distinct physical objects selected across all packs.
    pub objects_selected: usize,
    /// Distinct physical bytes selected across all packs.
    pub bytes_selected: u64,
    /// Canonical remote payload objects physically created by this call.
    pub objects_created: usize,
    /// Canonical remote payload bytes physically created by this call.
    pub bytes_created: u64,
    /// Immutable pack indexes physically created by this call.
    pub packs_created: usize,
    /// Fixed-key consolidated locators physically created by this call.
    pub locators_created: usize,
}

struct PlannedPack {
    selection: ConsolidatedPackSelection,
    manifest: SeriesManifest,
    index: PackIndex,
}

struct ConsolidatedPackPlan {
    snapshot_tip: ObjectHash,
    manifest_root: ObjectHash,
    packs: Vec<PlannedPack>,
    object_lengths: BTreeMap<ObjectHash, u64>,
}

fn expected_payload_kind(entry_type: EntryType) -> Result<PayloadKind, StewardError> {
    match entry_type {
        EntryType::FilePhysicalSeries => Ok(PayloadKind::File),
        EntryType::TablePhysicalSeries => Ok(PayloadKind::Table),
        other => Err(StewardError::Content(format!(
            "local consolidated pack references non-series entry type {other:?}"
        ))),
    }
}

fn read_series_manifest(
    store: &crate::local_content::LocalContentStore,
    hash: ObjectHash,
    entry_type: EntryType,
) -> Result<SeriesManifest, StewardError> {
    let bytes = store.read(hash)?;
    let manifest = SeriesManifest::decode(&bytes).map_err(|error| {
        StewardError::Content(format!("decode current series manifest {hash}: {error}"))
    })?;
    if manifest.hash() != hash {
        return Err(StewardError::Content(format!(
            "current series manifest hashes to {}, expected {hash}",
            manifest.hash()
        )));
    }
    let expected = expected_payload_kind(entry_type)?;
    if manifest.payload_kind() != expected {
        return Err(StewardError::Content(format!(
            "current series {hash} declares {:?}, but its manifest entry is {entry_type:?}",
            manifest.payload_kind()
        )));
    }
    Ok(manifest)
}

fn current_series_manifests(
    ship: &Ship,
    root: ObjectHash,
) -> Result<HashMap<ObjectHash, (EntryType, SeriesManifest)>, StewardError> {
    let store = crate::local_content::LocalContentStore::new(ship.pond_path());
    let mut current = HashMap::new();
    let mut stack = vec![root];
    let mut seen = HashSet::new();
    while let Some(hash) = stack.pop() {
        if !seen.insert(hash) {
            continue;
        }
        let bytes = store.read(hash)?;
        let node = ManifestMapNode::decode(&bytes).map_err(|error| {
            StewardError::Content(format!("decode local manifest-map node {hash}: {error}"))
        })?;
        match node {
            ManifestMapNode::Branch { left, right, .. } => {
                stack.push(right);
                stack.push(left);
            }
            ManifestMapNode::Leaf { record, .. }
                if matches!(
                    record.entry.entry_type,
                    EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries
                ) =>
            {
                let series_hash = record.entry.child_hash;
                let manifest = read_series_manifest(&store, series_hash, record.entry.entry_type)?;
                if let Some((prior_type, prior_manifest)) =
                    current.insert(series_hash, (record.entry.entry_type, manifest.clone()))
                    && (prior_type != record.entry.entry_type || prior_manifest != manifest)
                {
                    return Err(StewardError::Content(format!(
                        "current manifest map binds series {series_hash} inconsistently"
                    )));
                }
            }
            ManifestMapNode::Leaf { .. } => {}
        }
    }
    Ok(current)
}

async fn build_plan(ship: &Ship) -> Result<ConsolidatedPackPlan, StewardError> {
    let snapshot = crate::content_push::current_snapshot(ship).await?;
    let current = current_series_manifests(ship, snapshot.commit.manifest_root)?;
    let local = crate::pack_store::all_local_pack_indexes(ship.pond_path()).await?;
    if local.is_empty() {
        return Err(StewardError::Content(
            "no verified local consolidated packs are available; run `pond maintain \
             --collapse-versions N` first"
                .to_string(),
        ));
    }

    let mut grouped = BTreeMap::<ObjectHash, Vec<(ObjectHash, PackIndex)>>::new();
    for (series_hash, pack_hash, index) in local {
        if !current.contains_key(&series_hash) {
            return Err(StewardError::Content(format!(
                "local pack {pack_hash} belongs to stale series {series_hash}; rerun `pond \
                 maintain --collapse-versions N` until stale pack generations are pruned"
            )));
        }
        grouped
            .entry(series_hash)
            .or_default()
            .push((pack_hash, index));
    }

    let mut packs = Vec::with_capacity(grouped.len());
    let mut object_lengths = BTreeMap::<ObjectHash, u64>::new();
    for (series_hash, mut candidates) in grouped {
        candidates.sort_by_key(|(pack_hash, _)| *pack_hash);
        if candidates.len() != 1 {
            return Err(StewardError::Content(format!(
                "current series {series_hash} has {} local pack advertisements; rerun \
                 maintenance until exactly one selected pack remains",
                candidates.len()
            )));
        }
        let (pack_hash, index) = candidates.pop().expect("checked one candidate");
        if index.hash() != pack_hash {
            return Err(StewardError::Content(format!(
                "local pack index hashes to {}, expected {pack_hash}",
                index.hash()
            )));
        }
        let (entry_type, manifest) = current
            .get(&series_hash)
            .expect("group was checked against current map");
        let expected = expected_payload_kind(*entry_type)?;
        if manifest.payload_kind() != expected {
            return Err(StewardError::Content(format!(
                "series {series_hash} payload kind changed during local plan construction"
            )));
        }
        verify_complete_pack_against_manifest(series_hash, manifest, &index).map_err(|error| {
            StewardError::Content(format!(
                "local pack {pack_hash} does not verify against current series {series_hash}: \
                 {error}"
            ))
        })?;

        let mut pack_objects = BTreeSet::new();
        let mut pack_bytes = 0u64;
        for span in index.object_spans() {
            let hash = span.object_hash();
            let actual = crate::pack_store::verify_pack_object(ship.pond_path(), hash).await?;
            if actual != span.physical_len() {
                return Err(StewardError::Content(format!(
                    "local pack {pack_hash} declares physical object {hash} length {}, but the \
                     verified object has {actual} bytes",
                    span.physical_len()
                )));
            }
            match object_lengths.entry(hash) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    let _ = entry.insert(actual);
                }
                std::collections::btree_map::Entry::Occupied(entry) if *entry.get() != actual => {
                    return Err(StewardError::Content(format!(
                        "physical object {hash} has conflicting lengths {} and {actual}",
                        entry.get()
                    )));
                }
                std::collections::btree_map::Entry::Occupied(_) => {}
            }
            if pack_objects.insert(hash) {
                pack_bytes = pack_bytes.checked_add(actual).ok_or_else(|| {
                    StewardError::Content("selected pack byte count overflow".to_string())
                })?;
            }
        }
        packs.push(PlannedPack {
            selection: ConsolidatedPackSelection {
                series_hash,
                pack_hash,
                objects: pack_objects.len(),
                bytes: pack_bytes,
            },
            manifest: manifest.clone(),
            index,
        });
    }
    Ok(ConsolidatedPackPlan {
        snapshot_tip: snapshot.tip,
        manifest_root: snapshot.commit.manifest_root,
        packs,
        object_lengths,
    })
}

async fn verified_remote_state(
    remote: &ContentRemote,
    ref_name: &str,
    local_pond_id: uuid::Uuid,
    plan: &ConsolidatedPackPlan,
) -> Result<PublicationState, StewardError> {
    if remote.pond_id() != local_pond_id {
        return Err(StewardError::Content(format!(
            "remote pond {} does not match local pond {local_pond_id}",
            remote.pond_id()
        )));
    }
    let state = remote
        .current_publication(ref_name)
        .await
        .map_err(|error| StewardError::Content(format!("read publication state: {error}")))?
        .ok_or_else(|| {
            StewardError::Content(format!(
                "remote ref {ref_name:?} has no current native-v2 publication"
            ))
        })?;
    if state.snapshot_tip != plan.snapshot_tip || state.manifest_root != plan.manifest_root {
        return Err(StewardError::Content(format!(
            "remote ref {ref_name:?} is at tip {} manifest {}, but local consolidated packs were \
             planned for tip {} manifest {}; push the exact local snapshot first",
            state.snapshot_tip, state.manifest_root, plan.snapshot_tip, plan.manifest_root
        )));
    }
    let record = remote
        .get_publication_record(state.publication_record)
        .await
        .map_err(|error| {
            StewardError::Content(format!("read current publication record: {error}"))
        })?
        .ok_or_else(|| {
            StewardError::Content(format!(
                "remote current publication record {} is absent",
                state.publication_record
            ))
        })?;
    if record.pond_id != local_pond_id
        || record.ref_name != ref_name
        || record.snapshot_tip != state.snapshot_tip
        || record.manifest_root != state.manifest_root
    {
        return Err(StewardError::Content(
            "remote current publication record does not match its active row".to_string(),
        ));
    }
    Ok(state)
}

async fn publish_plan(
    ship: &Ship,
    remote: &mut ContentRemote,
    ref_name: &str,
    plan: ConsolidatedPackPlan,
) -> Result<ConsolidatedPackPublishOutcome, StewardError> {
    let pond_id = ship.control_table().pond_id_uuid();
    let baseline = verified_remote_state(remote, ref_name, pond_id, &plan).await?;
    let bytes_selected = plan.object_lengths.values().try_fold(0u64, |sum, value| {
        sum.checked_add(*value)
            .ok_or_else(|| StewardError::Content("selected byte count overflow".to_string()))
    })?;
    let mut outcome = ConsolidatedPackPublishOutcome {
        selections: plan.packs.iter().map(|pack| pack.selection).collect(),
        objects_selected: plan.object_lengths.len(),
        bytes_selected,
        ..ConsolidatedPackPublishOutcome::default()
    };

    let mut known_present = HashSet::new();
    for (&hash, &length) in &plan.object_lengths {
        let reader = crate::pack_store::open_pack_object_reader(ship.pond_path(), hash)
            .await?
            .ok_or_else(|| {
                StewardError::Content(format!(
                    "verified local pack object {hash} disappeared before upload"
                ))
            })?;
        let write = remote
            .put_immutable_object_stream(
                ObjectDescriptor::new(hash, ContentObjectKind::RawBlob),
                reader,
            )
            .await
            .map_err(|error| {
                StewardError::Content(format!("publish consolidated object {hash}: {error}"))
            })?;
        if write.payload_created {
            outcome.objects_created += 1;
            outcome.bytes_created = outcome
                .bytes_created
                .checked_add(write.payload_bytes_created)
                .ok_or_else(|| {
                    StewardError::Content("created consolidated byte count overflow".to_string())
                })?;
        }
        if write.payload_created && write.payload_bytes_created != length {
            return Err(StewardError::Content(format!(
                "remote reported {} created bytes for {hash}, local object has {length}",
                write.payload_bytes_created
            )));
        }
        let _ = known_present.insert(hash);
    }

    for pack in &plan.packs {
        let current = verified_remote_state(remote, ref_name, pond_id, &plan).await?;
        if current != baseline {
            return Err(StewardError::Content(
                "remote publication changed while consolidated packs were uploading; no further \
                 locator was installed"
                    .to_string(),
            ));
        }
        let write = remote
            .publish_consolidated_pack_with_known_present(
                pack.selection.series_hash,
                &pack.manifest,
                &pack.index,
                &known_present,
            )
            .await
            .map_err(|error| {
                StewardError::Content(format!(
                    "publish consolidated pack {} for series {}: {error}",
                    pack.selection.pack_hash, pack.selection.series_hash
                ))
            })?;
        if write.pack_hash != pack.selection.pack_hash {
            return Err(StewardError::Content(format!(
                "remote published pack {} but local selection was {}",
                write.pack_hash, pack.selection.pack_hash
            )));
        }
        outcome.packs_created += usize::from(write.pack_created);
        outcome.locators_created += usize::from(write.locator_created);
    }
    Ok(outcome)
}

/// Publish every verified local consolidated pack to an already-open remote.
pub async fn publish_local_consolidated_packs(
    ship: &Ship,
    remote: &mut ContentRemote,
    ref_name: &str,
) -> Result<ConsolidatedPackPublishOutcome, StewardError> {
    let plan = build_plan(ship).await?;
    publish_plan(ship, remote, ref_name, plan).await
}

/// Open and explicitly publish local consolidated packs with the open and all
/// remote operations covered by `limits`.
pub async fn open_and_publish_local_consolidated_packs_limited(
    ship: &Ship,
    url: &str,
    storage_options: HashMap<String, String>,
    ref_name: &str,
    limits: &mut LimiterSet,
) -> Result<ConsolidatedPackPublishOutcome, StewardError> {
    crate::storage_meter::metered_op(
        url,
        limits,
        Box::pin(async move {
            let plan = build_plan(ship).await?;
            let mut remote = ContentRemote::open_at_url(url, storage_options)
                .await
                .map_err(|error| StewardError::Aborted(format!("open remote {url}: {error}")))?;
            publish_plan(ship, &mut remote, ref_name, plan).await
        }),
    )
    .await
}
