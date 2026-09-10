// SPDX-License-Identifier: Apache-2.0

//! Remote-age cost regression for the native-v2 publication consumer.

use std::collections::{BTreeMap, HashMap, HashSet};

use async_trait::async_trait;
use steward::{
    BlobReader, ContentSource, FetchedGraph, FetchedObject, FetchedSeriesV2, Ship, StewardError,
    fetch_object_graph, fetch_object_graph_since, push_content_to_remote,
};
use sync_store::content::{
    Commit, ContentModelVersion, ContentObjectKind, ManifestChange, ManifestEntry, ManifestRecord,
    ManifestRecordChild, MerkleFrontier, ObjectDescriptor, ObjectHash, PackDescriptor, PackIndex,
    PackLeafDescriptor, PackObjectSpan, PayloadKind, Provenance, PublicationRecord, SeriesManifest,
    TreeEntry, build_manifest_map, encode_tree, generate_append_range_proof, generate_range_proof,
};
use sync_store::testing::in_memory_remote_url;
use sync_store::{
    AccessClass, ContentRemote, PublicationExpectation, PublicationState, RemoteKey,
    access_summary_under,
};
use tinyfs::EntryType;
use tlogfs::PondUserMetadata;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

struct SnapshotObjects {
    root_tree: ObjectHash,
    manifest_root: ObjectHash,
    records: Vec<ManifestRecord>,
    objects: BTreeMap<ObjectHash, (ContentObjectKind, Vec<u8>)>,
}

struct NoInventorySource<'a> {
    inner: &'a dyn ContentSource,
}

#[async_trait]
impl ContentSource for NoInventorySource<'_> {
    fn pond_id(&self) -> Uuid {
        self.inner.pond_id()
    }

    async fn get_tip(&self, ref_name: &str) -> Result<Option<ObjectHash>, StewardError> {
        self.inner.get_tip(ref_name).await
    }

    async fn get_publication_state(
        &self,
        ref_name: &str,
    ) -> Result<Option<PublicationState>, StewardError> {
        self.inner.get_publication_state(ref_name).await
    }

    async fn get_publication_record(
        &self,
        hash: ObjectHash,
    ) -> Result<Option<PublicationRecord>, StewardError> {
        self.inner.get_publication_record(hash).await
    }

    async fn get_publication_pack(
        &self,
        descriptor: PackDescriptor,
    ) -> Result<Option<Vec<u8>>, StewardError> {
        self.inner.get_publication_pack(descriptor).await
    }

    async fn get_object(&self, hash: ObjectHash) -> Result<Option<Vec<u8>>, StewardError> {
        self.inner.get_object(hash).await
    }

    async fn object_size(&self, hash: ObjectHash) -> Result<Option<u64>, StewardError> {
        self.inner.object_size(hash).await
    }

    async fn get_objects(
        &self,
        hashes: &[ObjectHash],
    ) -> Result<HashMap<ObjectHash, Vec<u8>>, StewardError> {
        self.inner.get_objects(hashes).await
    }

    async fn has_blob(&self, hash: ObjectHash) -> Result<bool, StewardError> {
        self.inner.has_blob(hash).await
    }

    async fn list_blobs(&self) -> Result<HashSet<ObjectHash>, StewardError> {
        Err(StewardError::Content(
            "ordinary native publication must not list a blob inventory".to_string(),
        ))
    }

    async fn get_blob_reader(&self, hash: ObjectHash) -> Result<Option<BlobReader>, StewardError> {
        self.inner.get_blob_reader(hash).await
    }

    async fn get_series_pack(
        &self,
        series_hash: ObjectHash,
    ) -> Result<Option<PackDescriptor>, StewardError> {
        self.inner.get_series_pack(series_hash).await
    }

    async fn get_consolidated_series_pack(
        &self,
        series_hash: ObjectHash,
    ) -> Result<Option<PackDescriptor>, StewardError> {
        self.inner.get_consolidated_series_pack(series_hash).await
    }

    async fn list_pack_hashes(
        &self,
        _series_hash: ObjectHash,
    ) -> Result<HashSet<ObjectHash>, StewardError> {
        Err(StewardError::Content(
            "ordinary native publication must not list a pack prefix".to_string(),
        ))
    }

    async fn get_pack_index(
        &self,
        series_hash: ObjectHash,
        pack_hash: ObjectHash,
    ) -> Result<Option<Vec<u8>>, StewardError> {
        self.inner.get_pack_index(series_hash, pack_hash).await
    }
}

fn meta(label: &str) -> PondUserMetadata {
    PondUserMetadata::new(vec!["publication-cost".to_string(), label.to_string()])
}

async fn append_file_series(ship: &mut Ship, first: usize, count: usize) {
    let payloads = (first..first + count)
        .map(|index| format!("leaf-{index:08}\n").into_bytes())
        .collect::<Vec<_>>();
    ship.write_transaction(&meta("append-series"), async move |transaction| {
        let root = transaction.root().await?;
        for payload in payloads {
            let mut writer = root
                .async_writer_path_with_type("/observations.series", EntryType::FilePhysicalSeries)
                .await?;
            writer.write_all(&payload).await?;
            writer.shutdown().await?;
        }
        Ok(())
    })
    .await
    .expect("append file series");
}

async fn write_changing_file(ship: &mut Ship, value: usize) {
    let bytes = format!("unrelated-{value:08}").into_bytes();
    ship.write_transaction(&meta("unrelated"), async move |transaction| {
        let root = transaction.root().await?;
        let mut writer = root
            .async_writer_path_with_type("/unrelated.txt", EntryType::FilePhysicalVersion)
            .await?;
        writer.write_all(&bytes).await?;
        writer.shutdown().await?;
        Ok(())
    })
    .await
    .expect("write unrelated file");
}

fn only_series(graph: &FetchedGraph) -> &FetchedSeriesV2 {
    let mut series = graph.objects.values().filter_map(|object| match object {
        FetchedObject::SeriesV2(series) => Some(series.as_ref()),
        _ => None,
    });
    let only = series.next().expect("one fetched series");
    assert!(series.next().is_none(), "fixture has one series");
    only
}

fn snapshot(payload: &[u8]) -> SnapshotObjects {
    let payload_hash = ObjectHash::of_bytes(payload);
    let tree_bytes = encode_tree(&[TreeEntry::bare(
        "value",
        EntryType::FilePhysicalVersion,
        payload_hash,
    )])
    .unwrap();
    let root_tree = ObjectHash::of_bytes(&tree_bytes);
    let records = vec![
        ManifestRecord::new(
            ManifestEntry::bare(
                tinyfs::ROOT_UUID,
                "",
                "",
                EntryType::DirectoryPhysical,
                root_tree,
            ),
            vec![ManifestRecordChild::new(
                "00000000-0000-7600-8000-000000000123",
                "value",
                EntryType::FilePhysicalVersion,
            )],
        )
        .unwrap(),
        ManifestRecord::new(
            ManifestEntry::bare(
                "00000000-0000-7600-8000-000000000123",
                tinyfs::ROOT_UUID,
                "value",
                EntryType::FilePhysicalVersion,
                payload_hash,
            ),
            vec![],
        )
        .unwrap(),
    ];
    let (manifest_root, manifest_nodes) = build_manifest_map(&records).unwrap();
    let mut objects = BTreeMap::new();
    let _ = objects.insert(payload_hash, (ContentObjectKind::RawBlob, payload.to_vec()));
    let _ = objects.insert(root_tree, (ContentObjectKind::Tree, tree_bytes));
    for (hash, bytes) in manifest_nodes {
        let _ = objects.insert(hash, (ContentObjectKind::ManifestNode, bytes));
    }
    SnapshotObjects {
        root_tree,
        manifest_root,
        records,
        objects,
    }
}

async fn put_objects(remote: &ContentRemote, objects: &SnapshotObjects) {
    for (hash, (kind, bytes)) in &objects.objects {
        let _ = remote
            .put_immutable_object(ObjectDescriptor::new(*hash, *kind), bytes)
            .await
            .unwrap();
    }
}

async fn measure_same_change(age: i64) -> (sync_store::AccessSummary, sync_store::AccessSummary) {
    let url = in_memory_remote_url(&format!("publication-age-{age}"));
    let pond = Uuid::new_v4();
    let mut remote =
        ContentRemote::create_at_url(&url, pond, BTreeMap::new().into_iter().collect())
            .await
            .unwrap();
    let base = snapshot(b"base");
    put_objects(&remote, &base).await;

    let mut parent_commit = None;
    let mut parent_record = None;
    let mut baseline: Option<PublicationState> = None;
    for generation in 1..=age {
        let commit = Commit::new(
            ContentModelVersion::PublicationV2,
            base.root_tree,
            parent_commit,
            base.manifest_root,
            Provenance {
                pond_id: pond.to_string(),
                seq: generation,
                time_micros: generation,
                author: "cost-test".to_string(),
                request: "age remote".to_string(),
            },
        );
        let commit_hash = commit.hash();
        let commit_bytes = commit.encode();
        let _ = remote
            .put_immutable_object(
                ObjectDescriptor::new(commit_hash, ContentObjectKind::Commit),
                &commit_bytes,
            )
            .await
            .unwrap();
        let mut introduced = vec![ObjectDescriptor::new(
            commit_hash,
            ContentObjectKind::Commit,
        )];
        let changes = if generation == 1 {
            introduced.extend(
                base.objects
                    .iter()
                    .map(|(hash, (kind, _))| ObjectDescriptor::new(*hash, *kind)),
            );
            base.records
                .iter()
                .cloned()
                .map(|record| ManifestChange::new(None, Some(record)).unwrap())
                .collect()
        } else {
            Vec::new()
        };
        let record = PublicationRecord::new(
            pond,
            "main",
            commit_hash,
            base.manifest_root,
            parent_record,
            introduced,
            vec![],
            changes,
        )
        .unwrap();
        let _ = remote.put_publication_record(&record).await.unwrap();
        let state = PublicationState::new(
            pond,
            "main",
            commit_hash,
            base.manifest_root,
            record.hash(),
            generation,
            generation,
        )
        .unwrap();
        let expectation = match baseline.as_ref() {
            Some(previous) => PublicationExpectation::Existing {
                generation: previous.generation,
                publication_record: previous.publication_record,
            },
            None => PublicationExpectation::Missing,
        };
        baseline = Some(
            remote
                .compare_and_swap_publication(expectation, state)
                .await
                .unwrap(),
        );
        parent_commit = Some(commit_hash);
        parent_record = Some(record.hash());
    }
    let baseline = baseline.unwrap();

    let changed = snapshot(b"changed");
    put_objects(&remote, &changed).await;
    let changes = base
        .records
        .iter()
        .cloned()
        .zip(changed.records.iter().cloned())
        .map(|(before, after)| ManifestChange::new(Some(before), Some(after)).unwrap())
        .collect::<Vec<_>>();
    let commit = Commit::new_with_delta(
        ContentModelVersion::PublicationV2,
        changed.root_tree,
        Some(baseline.snapshot_tip),
        changed.manifest_root,
        changes.clone(),
        changed
            .objects
            .iter()
            .map(|(hash, (kind, _))| ObjectDescriptor::new(*hash, *kind))
            .collect(),
        vec![],
        Provenance {
            pond_id: pond.to_string(),
            seq: age + 1,
            time_micros: age + 1,
            author: "cost-test".to_string(),
            request: "same change".to_string(),
        },
    )
    .unwrap();
    let commit_hash = commit.hash();
    let commit_bytes = commit.encode();
    let _ = remote
        .put_immutable_object(
            ObjectDescriptor::new(commit_hash, ContentObjectKind::Commit),
            &commit_bytes,
        )
        .await
        .unwrap();
    let mut introduced = changed
        .objects
        .iter()
        .map(|(hash, (kind, _))| ObjectDescriptor::new(*hash, *kind))
        .collect::<Vec<_>>();
    introduced.push(ObjectDescriptor::new(
        commit_hash,
        ContentObjectKind::Commit,
    ));
    let record = PublicationRecord::new(
        pond,
        "main",
        commit_hash,
        changed.manifest_root,
        Some(baseline.publication_record),
        introduced,
        vec![],
        changes,
    )
    .unwrap();
    let _ = remote.put_publication_record(&record).await.unwrap();
    let state = PublicationState::new(
        pond,
        "main",
        commit_hash,
        changed.manifest_root,
        record.hash(),
        age + 1,
        age + 1,
    )
    .unwrap();
    let _ = remote
        .compare_and_swap_publication(
            PublicationExpectation::Existing {
                generation: baseline.generation,
                publication_record: baseline.publication_record,
            },
            state,
        )
        .await
        .unwrap();
    assert_eq!(remote.publication_active_file_count().unwrap(), 1);
    drop(remote);

    let key = RemoteKey::new(&url);
    let before = access_summary_under(&key);
    let opened = ContentRemote::open_at_url(&url, Default::default())
        .await
        .unwrap();
    let graph = fetch_object_graph_since(&opened, "main", Some(baseline.snapshot_tip))
        .await
        .unwrap();
    assert_eq!(graph.tip, Some(commit_hash));
    assert_eq!(graph.manifest_changes.len(), 2);
    let after = access_summary_under(&key);
    let delta = after.saturating_sub(&before);
    assert_eq!(
        delta.class(AccessClass::DeltaObjects),
        sync_store::AccessTotals::default()
    );
    assert_eq!(
        delta.class(AccessClass::DeltaCommits),
        sync_store::AccessTotals::default()
    );
    assert_eq!(
        delta.class(AccessClass::DeltaRefs),
        sync_store::AccessTotals::default()
    );
    assert_eq!(delta.object_point_queries, 0);
    assert_eq!(delta.object_batch_queries, 0);
    assert_eq!(
        delta.class(AccessClass::Fallback),
        sync_store::AccessTotals::default(),
        "native-v2 consumer must not use an unclassified inventory fallback"
    );
    let before_full = access_summary_under(&key);
    let graph = fetch_object_graph_since(&opened, "main", None)
        .await
        .unwrap();
    assert_eq!(graph.tip, Some(commit_hash));
    assert!(graph.manifest_complete);
    let full = access_summary_under(&key).saturating_sub(&before_full);
    (delta, full)
}

#[tokio::test]
async fn identical_consumer_change_cost_is_independent_of_remote_age_1_100_1000() {
    let (one, full_one) = measure_same_change(1).await;
    let (hundred, full_hundred) = measure_same_change(100).await;
    let (thousand, full_thousand) = measure_same_change(1_000).await;
    assert_eq!(one.total().ops, hundred.total().ops);
    assert_eq!(one.total().ops, thousand.total().ops);
    let totals = [
        one.total().bytes,
        hundred.total().bytes,
        thousand.total().bytes,
    ];
    let variance = totals.iter().max().unwrap() - totals.iter().min().unwrap();
    assert!(
        variance <= 1024,
        "bounded metadata encoding variance exceeded 1 KiB: {totals:?}"
    );
    for class in [
        AccessClass::ContentObjects,
        AccessClass::PublicationRecords,
        AccessClass::PublicationDeltaLog,
        AccessClass::PublicationDeltaData,
    ] {
        assert_eq!(one.class(class).ops, hundred.class(class).ops, "{class:?}");
        assert_eq!(one.class(class).ops, thousand.class(class).ops, "{class:?}");
        let bytes = [
            one.class(class).bytes,
            hundred.class(class).bytes,
            thousand.class(class).bytes,
        ];
        assert!(
            bytes.iter().max().unwrap() - bytes.iter().min().unwrap() <= 1024,
            "{class:?} bytes varied with age: {bytes:?}"
        );
    }
    assert_eq!(full_one.total().ops, full_hundred.total().ops);
    assert_eq!(full_one.total().ops, full_thousand.total().ops);
    let full_totals = [
        full_one.total().bytes,
        full_hundred.total().bytes,
        full_thousand.total().bytes,
    ];
    assert!(
        full_totals.iter().max().unwrap() - full_totals.iter().min().unwrap() <= 1024,
        "full current-snapshot bytes varied with remote age: {full_totals:?}"
    );
}

#[derive(Debug)]
struct SeriesAppendCost {
    pack_bytes: u64,
    pack_descriptors: usize,
    publication_record_bytes: usize,
    producer_pack_create_bytes: u64,
    producer_pack_create_count: u64,
    consumer_pack_bytes: u64,
    consumer_pack_ops: u64,
}

async fn measure_one_leaf_series_append(prior_leaves: usize) -> SeriesAppendCost {
    let url = in_memory_remote_url(&format!(
        "series-publication-age-{prior_leaves}-{}",
        Uuid::new_v4()
    ));
    let pond_id = Uuid::new_v4();
    let mut remote =
        ContentRemote::create_at_url(&url, pond_id, BTreeMap::new().into_iter().collect())
            .await
            .expect("create remote");

    let prefix_leaf_hashes = (0..prior_leaves)
        .map(|index| ObjectHash::of_bytes(format!("leaf-{index}").as_bytes()))
        .collect::<Vec<_>>();
    let prefix_frontier = MerkleFrontier::from_leaves(&prefix_leaf_hashes);
    let prefix_manifest = SeriesManifest::new(
        PayloadKind::File,
        prior_leaves as u64,
        prior_leaves as u64,
        None,
        None,
        None,
        prefix_frontier.clone(),
    )
    .expect("prefix manifest");
    let prefix_series_hash = prefix_manifest.hash();
    let prefix_payload = vec![0x41; prior_leaves];
    let prefix_payload_hash = ObjectHash::of_bytes(&prefix_payload);
    let prefix_pack = PackIndex::new_segment_with_spans(
        prefix_series_hash,
        None,
        0,
        prior_leaves as u64,
        prior_leaves as u64,
        prefix_manifest.leaf_merkle_root(),
        generate_range_proof(&prefix_leaf_hashes, 0, prior_leaves).expect("prefix range proof"),
        vec![
            PackObjectSpan::new(
                prefix_payload_hash,
                0,
                prior_leaves as u64,
                0,
                prior_leaves as u64,
            )
            .expect("prefix span"),
        ],
        prior_leaves as u64,
        prior_leaves as u64,
        prefix_leaf_hashes
            .iter()
            .map(|hash| {
                PackLeafDescriptor::new_with_leaf_hash(*hash, 1, None, None, None)
                    .expect("prefix descriptor")
            })
            .collect(),
    )
    .expect("prefix pack");

    let prefix_tree_bytes = encode_tree(&[TreeEntry::bare(
        "observations.series",
        EntryType::FilePhysicalSeries,
        prefix_series_hash,
    )])
    .expect("prefix tree");
    let prefix_tree_hash = ObjectHash::of_bytes(&prefix_tree_bytes);
    let series_node = "00000000-0000-7600-8000-000000000777";
    let prefix_records = vec![
        ManifestRecord::new(
            ManifestEntry::bare(
                tinyfs::ROOT_UUID,
                "",
                "",
                EntryType::DirectoryPhysical,
                prefix_tree_hash,
            ),
            vec![ManifestRecordChild::new(
                series_node,
                "observations.series",
                EntryType::FilePhysicalSeries,
            )],
        )
        .expect("prefix root record"),
        ManifestRecord::new(
            ManifestEntry::bare(
                series_node,
                tinyfs::ROOT_UUID,
                "observations.series",
                EntryType::FilePhysicalSeries,
                prefix_series_hash,
            ),
            vec![],
        )
        .expect("prefix series record"),
    ];
    let (prefix_manifest_root, prefix_manifest_nodes) =
        build_manifest_map(&prefix_records).expect("prefix manifest map");
    for (hash, bytes, kind) in [
        (
            prefix_payload_hash,
            prefix_payload,
            ContentObjectKind::RawBlob,
        ),
        (
            prefix_series_hash,
            prefix_manifest.encode(),
            ContentObjectKind::SeriesManifest,
        ),
        (prefix_tree_hash, prefix_tree_bytes, ContentObjectKind::Tree),
    ] {
        let _ = remote
            .put_immutable_object(ObjectDescriptor::new(hash, kind), &bytes)
            .await
            .expect("publish prefix object");
    }
    for (hash, bytes) in prefix_manifest_nodes {
        let _ = remote
            .put_immutable_object(
                ObjectDescriptor::new(hash, ContentObjectKind::ManifestNode),
                &bytes,
            )
            .await
            .expect("publish prefix manifest node");
    }
    let prefix_pack_descriptor = PackDescriptor::new(prefix_series_hash, prefix_pack.hash());
    let _ = remote
        .put_immutable_pack(prefix_pack_descriptor, &prefix_pack.encode())
        .await
        .expect("publish prefix pack");
    let prefix_commit = Commit::new(
        ContentModelVersion::PublicationV2,
        prefix_tree_hash,
        None,
        prefix_manifest_root,
        Provenance {
            pond_id: pond_id.to_string(),
            seq: 1,
            time_micros: 1,
            author: "series-cost".to_string(),
            request: "prefix".to_string(),
        },
    );
    let prefix_commit_hash = prefix_commit.hash();
    let prefix_commit_bytes = prefix_commit.encode();
    let _ = remote
        .put_immutable_object(
            ObjectDescriptor::new(prefix_commit_hash, ContentObjectKind::Commit),
            &prefix_commit_bytes,
        )
        .await
        .expect("publish prefix commit");
    let prefix_objects = vec![
        ObjectDescriptor::new(prefix_payload_hash, ContentObjectKind::RawBlob),
        ObjectDescriptor::new(prefix_series_hash, ContentObjectKind::SeriesManifest),
        ObjectDescriptor::new(prefix_tree_hash, ContentObjectKind::Tree),
        ObjectDescriptor::new(prefix_commit_hash, ContentObjectKind::Commit),
    ];
    let prefix_record = PublicationRecord::new(
        pond_id,
        "main",
        prefix_commit_hash,
        prefix_manifest_root,
        None,
        prefix_objects,
        vec![prefix_pack_descriptor],
        prefix_records
            .iter()
            .cloned()
            .map(|record| ManifestChange::new(None, Some(record)).expect("prefix change"))
            .collect(),
    )
    .expect("prefix publication");
    let _ = remote
        .put_publication_record(&prefix_record)
        .await
        .expect("publish prefix record");
    let baseline = PublicationState::new(
        pond_id,
        "main",
        prefix_commit_hash,
        prefix_manifest_root,
        prefix_record.hash(),
        1,
        1,
    )
    .expect("prefix state");
    let _ = remote
        .compare_and_swap_publication(PublicationExpectation::Missing, baseline.clone())
        .await
        .expect("advance prefix state");

    let suffix_leaf_hash = ObjectHash::of_bytes(b"appended-leaf");
    let all_leaf_hashes = prefix_leaf_hashes
        .iter()
        .copied()
        .chain(std::iter::once(suffix_leaf_hash))
        .collect::<Vec<_>>();
    let current_frontier = prefix_frontier
        .extended(&[suffix_leaf_hash])
        .expect("append frontier");
    let current_manifest = SeriesManifest::new(
        PayloadKind::File,
        prior_leaves as u64 + 1,
        prior_leaves as u64 + 1,
        None,
        None,
        None,
        current_frontier,
    )
    .expect("current manifest");
    let current_series_hash = current_manifest.hash();
    let suffix_payload = vec![0x42];
    let suffix_payload_hash = ObjectHash::of_bytes(&suffix_payload);
    let suffix_pack = PackIndex::new_segment_with_spans(
        current_series_hash,
        Some(prefix_series_hash),
        prior_leaves as u64,
        prior_leaves as u64 + 1,
        prior_leaves as u64 + 1,
        current_manifest.leaf_merkle_root(),
        generate_append_range_proof(&prefix_frontier, 1).expect("append proof"),
        vec![PackObjectSpan::new(suffix_payload_hash, 0, 1, 0, 1).expect("suffix span")],
        1,
        1,
        vec![
            PackLeafDescriptor::new_with_leaf_hash(suffix_leaf_hash, 1, None, None, None)
                .expect("suffix descriptor"),
        ],
    )
    .expect("suffix pack");
    assert_eq!(
        sync_store::content::merkle_root(&all_leaf_hashes),
        current_manifest.leaf_merkle_root()
    );
    let current_tree_bytes = encode_tree(&[TreeEntry::bare(
        "observations.series",
        EntryType::FilePhysicalSeries,
        current_series_hash,
    )])
    .expect("current tree");
    let current_tree_hash = ObjectHash::of_bytes(&current_tree_bytes);
    let current_records = vec![
        ManifestRecord::new(
            ManifestEntry::bare(
                tinyfs::ROOT_UUID,
                "",
                "",
                EntryType::DirectoryPhysical,
                current_tree_hash,
            ),
            vec![ManifestRecordChild::new(
                series_node,
                "observations.series",
                EntryType::FilePhysicalSeries,
            )],
        )
        .expect("current root record"),
        ManifestRecord::new(
            ManifestEntry::bare(
                series_node,
                tinyfs::ROOT_UUID,
                "observations.series",
                EntryType::FilePhysicalSeries,
                current_series_hash,
            ),
            vec![],
        )
        .expect("current series record"),
    ];
    let (current_manifest_root, current_manifest_nodes) =
        build_manifest_map(&current_records).expect("current manifest map");
    let current_commit = Commit::new_with_delta(
        ContentModelVersion::PublicationV2,
        current_tree_hash,
        Some(prefix_commit_hash),
        current_manifest_root,
        prefix_records
            .iter()
            .cloned()
            .zip(current_records.iter().cloned())
            .map(|(before, after)| {
                ManifestChange::new(Some(before), Some(after)).expect("current change")
            })
            .collect(),
        Vec::new(),
        vec![PackDescriptor::new(current_series_hash, suffix_pack.hash())],
        Provenance {
            pond_id: pond_id.to_string(),
            seq: 2,
            time_micros: 2,
            author: "series-cost".to_string(),
            request: "one-leaf suffix".to_string(),
        },
    )
    .expect("current commit");
    let current_commit_hash = current_commit.hash();

    let key = RemoteKey::new(&url);
    let before_push = access_summary_under(&key);
    let mut introduced = Vec::new();
    for (hash, bytes, kind) in [
        (
            suffix_payload_hash,
            suffix_payload,
            ContentObjectKind::RawBlob,
        ),
        (
            current_series_hash,
            current_manifest.encode(),
            ContentObjectKind::SeriesManifest,
        ),
        (
            current_tree_hash,
            current_tree_bytes,
            ContentObjectKind::Tree,
        ),
        (
            current_commit_hash,
            current_commit.encode(),
            ContentObjectKind::Commit,
        ),
    ] {
        let descriptor = ObjectDescriptor::new(hash, kind);
        let _ = remote
            .put_immutable_object(descriptor, &bytes)
            .await
            .expect("publish suffix object");
        introduced.push(descriptor);
    }
    for (hash, bytes) in current_manifest_nodes {
        let descriptor = ObjectDescriptor::new(hash, ContentObjectKind::ManifestNode);
        let _ = remote
            .put_immutable_object(descriptor, &bytes)
            .await
            .expect("publish current manifest node");
        introduced.push(descriptor);
    }
    let suffix_descriptor = PackDescriptor::new(current_series_hash, suffix_pack.hash());
    let _ = remote
        .put_immutable_pack(suffix_descriptor, &suffix_pack.encode())
        .await
        .expect("publish suffix pack");
    let record = PublicationRecord::new(
        pond_id,
        "main",
        current_commit_hash,
        current_manifest_root,
        Some(prefix_record.hash()),
        introduced,
        vec![suffix_descriptor],
        prefix_records
            .iter()
            .cloned()
            .zip(current_records.iter().cloned())
            .map(|(before, after)| {
                ManifestChange::new(Some(before), Some(after)).expect("publication change")
            })
            .collect(),
    )
    .expect("suffix publication");
    let _ = remote
        .put_publication_record(&record)
        .await
        .expect("publish suffix record");
    let update = PublicationState::new(
        pond_id,
        "main",
        current_commit_hash,
        current_manifest_root,
        record.hash(),
        2,
        2,
    )
    .expect("suffix state");
    let _ = remote
        .compare_and_swap_publication(
            PublicationExpectation::Existing {
                generation: 1,
                publication_record: prefix_record.hash(),
            },
            update.clone(),
        )
        .await
        .expect("advance suffix state");
    let producer_cost = access_summary_under(&key).saturating_sub(&before_push);
    assert_eq!(
        record.introduced_packs.len(),
        1,
        "one changed series publishes one segment descriptor"
    );
    let descriptor = record.introduced_packs[0];
    let pack_bytes = remote
        .get_immutable_pack(descriptor)
        .await
        .expect("read pack")
        .expect("pack exists");
    let pack = PackIndex::decode(&pack_bytes).expect("decode suffix pack");
    assert_eq!(pack.leaf_start(), prior_leaves as u64);
    assert_eq!(pack.leaf_end(), prior_leaves as u64 + 1);
    assert_eq!(pack.leaf_descriptors().len(), 1);
    assert_eq!(pack.physical_object_hashes().len(), 1);
    assert!(pack.parent_series_hash().is_some());

    let before_pull = access_summary_under(&key);
    let no_inventory = NoInventorySource { inner: &remote };
    let incremental = fetch_object_graph_since(&no_inventory, "main", Some(baseline.snapshot_tip))
        .await
        .expect("fetch one-leaf suffix");
    let metadata_cost = access_summary_under(&key).saturating_sub(&before_pull);
    let fetched = only_series(&incremental);
    assert_eq!(fetched.leaf_start, prior_leaves as u64);
    assert_eq!(fetched.leaf_hashes.len(), 1);
    assert_eq!(fetched.packs.len(), 1);
    assert_eq!(
        fetched.physical_object_hashes,
        vec![suffix_payload_hash],
        "incremental consumer plans only the suffix payload"
    );

    let physical = producer_cost.physical_creates(AccessClass::ContentPacks);
    SeriesAppendCost {
        pack_bytes: pack_bytes.len() as u64,
        pack_descriptors: pack.leaf_descriptors().len(),
        publication_record_bytes: record.encode().len(),
        producer_pack_create_bytes: physical.bytes,
        producer_pack_create_count: physical.ops,
        consumer_pack_bytes: metadata_cost.class(AccessClass::ContentPacks).bytes,
        consumer_pack_ops: metadata_cost.class(AccessClass::ContentPacks).ops,
    }
}

#[tokio::test]
async fn one_leaf_series_append_cost_is_bounded_after_1_100_1000_leaves() {
    let one = measure_one_leaf_series_append(1).await;
    let hundred = measure_one_leaf_series_append(100).await;
    let thousand = measure_one_leaf_series_append(1_000).await;
    let costs = [&one, &hundred, &thousand];

    assert!(
        costs.iter().all(|cost| cost.pack_descriptors == 1),
        "ordinary append packs must describe only the one-leaf suffix: {costs:?}"
    );
    assert!(
        costs
            .iter()
            .all(|cost| cost.producer_pack_create_count == 1),
        "each new series state creates exactly one immutable segment pack: {costs:?}"
    );
    let bounded_variance = |values: [u64; 3], label: &str| {
        let variance = values.iter().max().unwrap() - values.iter().min().unwrap();
        assert!(
            variance <= 4 * 1024,
            "{label} varied by {variance} bytes with prior leaf count: {values:?}"
        );
    };
    bounded_variance(
        [one.pack_bytes, hundred.pack_bytes, thousand.pack_bytes],
        "encoded suffix pack",
    );
    bounded_variance(
        [
            one.producer_pack_create_bytes,
            hundred.producer_pack_create_bytes,
            thousand.producer_pack_create_bytes,
        ],
        "producer pack create",
    );
    bounded_variance(
        [
            one.consumer_pack_bytes,
            hundred.consumer_pack_bytes,
            thousand.consumer_pack_bytes,
        ],
        "incremental consumer pack reads",
    );
    assert_eq!(
        one.publication_record_bytes, hundred.publication_record_bytes,
        "publication record bytes must not encode the historical leaf prefix"
    );
    assert_eq!(
        one.publication_record_bytes, thousand.publication_record_bytes,
        "publication record bytes must not encode the historical leaf prefix"
    );
    assert_eq!(one.consumer_pack_ops, hundred.consumer_pack_ops);
    assert_eq!(one.consumer_pack_ops, thousand.consumer_pack_ops);
}

fn build_consolidated_pack(series: &FetchedSeriesV2) -> PackIndex {
    assert_eq!(series.leaf_start, 0, "requires a complete fetched chain");
    let mut descriptors = Vec::<PackLeafDescriptor>::new();
    let mut spans = Vec::<PackObjectSpan>::new();
    let mut logical_offset = 0u64;
    let mut physical_offset = 0u64;
    for (_, pack) in &series.packs {
        descriptors.extend(pack.leaf_descriptors().iter().cloned());
        for span in pack.object_spans() {
            spans.push(
                PackObjectSpan::new(
                    span.object_hash(),
                    logical_offset + span.logical_start(),
                    logical_offset + span.logical_end(),
                    physical_offset + span.physical_start(),
                    physical_offset + span.physical_end(),
                )
                .expect("offset pack span"),
            );
        }
        logical_offset += pack.logical_count();
        physical_offset += pack.physical_byte_count();
    }
    PackIndex::new_with_spans(
        series.manifest_hash,
        0,
        series.manifest.leaf_count(),
        series.manifest.leaf_count(),
        series.manifest.leaf_merkle_root(),
        generate_range_proof(&series.leaf_hashes, 0, series.leaf_hashes.len())
            .expect("whole-range proof"),
        spans,
        logical_offset,
        physical_offset,
        descriptors,
    )
    .expect("consolidated pack")
}

#[tokio::test]
async fn fresh_clone_walks_only_the_series_chain_and_consolidation_can_terminate_it() {
    let pond_dir = tempfile::tempdir().expect("pond tempdir");
    let mut producer = Ship::create_pond(pond_dir.path().join("producer"), "series-chain")
        .await
        .expect("create producer");
    append_file_series(&mut producer, 0, 1).await;
    let url = in_memory_remote_url(&format!("series-chain-{}", Uuid::new_v4()));
    let pond_id = producer.control_table().pond_id_uuid();
    let mut remote =
        ContentRemote::create_at_url(&url, pond_id, BTreeMap::new().into_iter().collect())
            .await
            .expect("create remote");
    let _ = push_content_to_remote(&producer, &mut remote, "main")
        .await
        .expect("publish first leaf");
    for index in 1..6 {
        append_file_series(&mut producer, index, 1).await;
        let _ = push_content_to_remote(&producer, &mut remote, "main")
            .await
            .expect("publish linked append");
    }

    let key = RemoteKey::new(&url);
    let before = access_summary_under(&key);
    let no_inventory = NoInventorySource { inner: &remote };
    let graph = fetch_object_graph(&no_inventory, "main")
        .await
        .expect("fresh linked clone");
    let initial_cost = access_summary_under(&key).saturating_sub(&before);
    let series = only_series(&graph);
    assert_eq!(series.leaf_hashes.len(), 6);
    assert_eq!(series.packs.len(), 6);
    assert_eq!(
        initial_cost.class(AccessClass::PublicationRecords).ops,
        1,
        "fresh clone reads only the active publication record, not unrelated history"
    );
    let pack_cost_before_unrelated = initial_cost.class(AccessClass::ContentPacks);

    for generation in 0..25 {
        write_changing_file(&mut producer, generation).await;
        let _ = push_content_to_remote(&producer, &mut remote, "main")
            .await
            .expect("publish unrelated generation");
    }
    let before = access_summary_under(&key);
    let no_inventory = NoInventorySource { inner: &remote };
    let graph_after_unrelated = fetch_object_graph(&no_inventory, "main")
        .await
        .expect("fresh clone after unrelated generations");
    let unrelated_cost = access_summary_under(&key).saturating_sub(&before);
    let series_after_unrelated = only_series(&graph_after_unrelated);
    assert_eq!(series_after_unrelated.leaf_hashes.len(), 6);
    assert_eq!(series_after_unrelated.packs.len(), 6);
    assert_eq!(
        unrelated_cost.class(AccessClass::ContentPacks),
        pack_cost_before_unrelated,
        "series-chain reads depend on current segments, not unrelated publication generations"
    );
    assert_eq!(
        unrelated_cost.class(AccessClass::PublicationRecords).ops,
        1,
        "fresh clone must not scan publication history"
    );

    let series_hash = series_after_unrelated.manifest_hash;
    let packs_before = remote
        .diagnostic_list_pack_hashes(series_hash)
        .await
        .expect("list segment packs");
    let consolidated = build_consolidated_pack(series_after_unrelated);
    let known_present = consolidated
        .physical_object_hashes()
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let consolidated_hash = remote
        .publish_consolidated_pack_with_known_present(
            series_hash,
            &series_after_unrelated.manifest,
            &consolidated,
            &known_present,
        )
        .await
        .expect("publish consolidated pack")
        .pack_hash;
    assert!(!packs_before.contains(&consolidated_hash));
    let packs_after = remote
        .diagnostic_list_pack_hashes(series_hash)
        .await
        .expect("list packs after consolidation");
    assert_eq!(packs_after.len(), packs_before.len() + 1);

    let no_inventory = NoInventorySource { inner: &remote };
    let consolidated_graph = fetch_object_graph(&no_inventory, "main")
        .await
        .expect("fresh clone through consolidated locator");
    let consolidated_series = only_series(&consolidated_graph);
    assert_eq!(consolidated_series.packs.len(), 1);
    assert_eq!(consolidated_series.leaf_hashes.len(), 6);
    assert!(
        consolidated_series.packs[0]
            .1
            .parent_series_hash()
            .is_none(),
        "consolidated pack terminates traversal"
    );

    let consumer_dir = tempfile::tempdir().expect("consumer tempdir");
    let mut consumer = Ship::create_pond(consumer_dir.path().join("consumer"), "series-clone")
        .await
        .expect("create consumer");
    let _ = steward::rebuild_pond(&mut consumer, &no_inventory, &consolidated_graph)
        .await
        .expect("materialize consolidated fresh clone");

    let before_rename = remote
        .current_publication("main")
        .await
        .expect("read publication before rename")
        .expect("publication before rename");
    producer
        .write_transaction(&meta("rename-series"), async move |transaction| {
            let root = transaction.root().await?;
            root.rename_entry("observations.series", "moved.series")
                .await?;
            Ok(())
        })
        .await
        .expect("rename series");
    let _ = push_content_to_remote(&producer, &mut remote, "main")
        .await
        .expect("publish rename");
    let no_inventory = NoInventorySource { inner: &remote };
    let renamed = fetch_object_graph_since(&no_inventory, "main", Some(before_rename.snapshot_tip))
        .await
        .expect("fetch renamed series after consolidation");
    let renamed_series = only_series(&renamed);
    assert_eq!(
        renamed_series.packs.len(),
        1,
        "incremental fetch must ignore the whole-range consolidated locator"
    );
    assert_eq!(renamed_series.leaf_start, 5);
    let _ = steward::rebuild_pond(&mut consumer, &no_inventory, &renamed)
        .await
        .expect("apply renamed series");
}

async fn linked_remote(
    label: &str,
    leaves: usize,
) -> (
    tempfile::TempDir,
    tempfile::TempDir,
    Ship,
    ContentRemote,
    std::path::PathBuf,
) {
    let pond_dir = tempfile::tempdir().expect("pond tempdir");
    let mut producer = Ship::create_pond(pond_dir.path().join("producer"), label)
        .await
        .expect("create producer");
    append_file_series(&mut producer, 0, 1).await;
    let remote_dir = tempfile::tempdir().expect("remote tempdir");
    let remote_root = remote_dir.path().join("remote");
    let mut remote =
        ContentRemote::create_at(&remote_root, producer.control_table().pond_id_uuid())
            .await
            .expect("create remote");
    let _ = push_content_to_remote(&producer, &mut remote, "main")
        .await
        .expect("publish first leaf");
    for index in 1..leaves {
        append_file_series(&mut producer, index, 1).await;
        let _ = push_content_to_remote(&producer, &mut remote, "main")
            .await
            .expect("publish linked leaf");
    }
    (pond_dir, remote_dir, producer, remote, remote_root)
}

fn immutable_pack_count(remote_root: &std::path::Path) -> usize {
    std::fs::read_dir(remote_root.join("_content/v2/packs"))
        .expect("read pack directory")
        .filter_map(Result::ok)
        .filter(|entry| {
            entry.file_type().is_ok_and(|kind| kind.is_file())
                && entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with("blake3="))
        })
        .count()
}

#[tokio::test]
async fn missing_or_malformed_series_segments_fail_closed() {
    {
        let (_pond_dir, _remote_dir, _producer, remote, remote_root) =
            linked_remote("missing-link", 3).await;
        let graph = fetch_object_graph(&remote, "main")
            .await
            .expect("read intact chain");
        let series = only_series(&graph);
        let parent_series = series.packs[0].1.series_hash();
        std::fs::remove_file(remote_root.join(format!(
            "_content/v2/packs/by-series/blake3={}",
            parent_series.to_hex()
        )))
        .expect("remove historical locator");
        let error = fetch_object_graph(&remote, "main")
            .await
            .expect_err("missing historical locator must fail");
        assert!(
            error
                .to_string()
                .contains("no canonical pack-segment locator"),
            "unexpected missing-link error: {error}"
        );
    }

    {
        let (_pond_dir, _remote_dir, _producer, remote, remote_root) =
            linked_remote("missing-segment", 3).await;
        let graph = fetch_object_graph(&remote, "main")
            .await
            .expect("read intact chain");
        let missing_pack = only_series(&graph).packs[0].0;
        std::fs::remove_file(remote_root.join(format!(
            "_content/v2/packs/blake3={}",
            missing_pack.to_hex()
        )))
        .expect("remove historical pack");
        let error = fetch_object_graph(&remote, "main")
            .await
            .expect_err("missing historical segment must fail");
        assert!(
            error.to_string().contains("is absent"),
            "unexpected missing-segment error: {error}"
        );
    }

    {
        let (_pond_dir, _remote_dir, _producer, remote, remote_root) =
            linked_remote("malformed-segment", 2).await;
        let graph = fetch_object_graph(&remote, "main")
            .await
            .expect("read intact chain");
        let series = only_series(&graph);
        let current_hash = series.manifest_hash;
        let current_pack_hash = series.packs.last().expect("current pack").0;
        let mut malformed = std::fs::read(remote_root.join(format!(
            "_content/v2/packs/blake3={}",
            current_pack_hash.to_hex()
        )))
        .expect("read current pack");
        let leaf_start_offset = b"watertown.series-pack.v4\n".len() + 32 + 1 + 32;
        malformed[leaf_start_offset..leaf_start_offset + 8].copy_from_slice(&0u64.to_le_bytes());
        let malformed_hash = ObjectHash::of_bytes(&malformed);
        std::fs::write(
            remote_root.join(format!(
                "_content/v2/packs/blake3={}",
                malformed_hash.to_hex()
            )),
            malformed,
        )
        .expect("write malformed pack");
        std::fs::write(
            remote_root.join(format!(
                "_content/v2/packs/by-series/blake3={}",
                current_hash.to_hex()
            )),
            malformed_hash.as_bytes(),
        )
        .expect("redirect locator to malformed pack");
        let error = fetch_object_graph(&remote, "main")
            .await
            .expect_err("malformed range/proof must fail");
        assert!(
            error.to_string().contains("decode pack")
                || error.to_string().contains("append series segment"),
            "unexpected malformed-segment error: {error}"
        );
    }
}

#[tokio::test]
async fn interrupted_series_pack_publication_retries_without_duplicate_canonical_bytes() {
    let (_pond_dir, _remote_dir, mut producer, mut remote, remote_root) =
        linked_remote("series-retry", 1).await;
    let visible_before = remote
        .current_publication("main")
        .await
        .expect("read visible state")
        .expect("visible state");
    append_file_series(&mut producer, 1, 1).await;
    remote.inject_publication_failure(sync_store::PublicationFailurePoint::AfterPacks);
    let error = push_content_to_remote(&producer, &mut remote, "main")
        .await
        .expect_err("inject failure after segment publication");
    assert!(error.to_string().contains("injected publication failure"));
    assert_eq!(
        remote.current_publication("main").await.unwrap(),
        Some(visible_before),
        "failure before the active-row CAS keeps the old state visible"
    );
    let object_count = remote.immutable_object_count().await.unwrap();
    let pack_count = immutable_pack_count(&remote_root);
    let _ = fetch_object_graph(&remote, "main")
        .await
        .expect("old visible state remains fetchable");

    let retried = push_content_to_remote(&producer, &mut remote, "main")
        .await
        .expect("retry converges");
    assert_eq!(remote.immutable_object_count().await.unwrap(), object_count);
    assert_eq!(immutable_pack_count(&remote_root), pack_count);
    let graph = fetch_object_graph(&remote, "main")
        .await
        .expect("retried state is fetchable");
    assert_eq!(graph.tip, Some(retried.tip));
}
