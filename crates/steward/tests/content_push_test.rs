// SPDX-License-Identifier: Apache-2.0

//! Native-v2 publication protocol regression tests.

use steward::{
    Ship, fetch_object_graph, push_content_to_remote, read_push_ack, remote_tip_is_acknowledged,
    write_push_ack,
};
use sync_store::content::{
    Commit, ContentObjectKind, ObjectHash, PublicationRecord, encode_recipe,
};
use sync_store::{
    ContentRemote, PublicationExpectation, PublicationFailurePoint, PublicationState,
};
use tempfile::tempdir;
use tinyfs::EntryType;
use tinyfs::async_helpers::convenience::create_file_path;
use tlogfs::PondUserMetadata;
use tokio::io::AsyncWriteExt;

fn meta(label: &str) -> PondUserMetadata {
    PondUserMetadata::new(vec!["test".into(), label.into()])
}

async fn write_file(ship: &mut Ship, path: &str, bytes: &[u8]) {
    let path = path.to_string();
    let bytes = bytes.to_vec();
    ship.write_transaction(&meta("write"), async move |transaction| {
        let root = transaction.root().await?;
        if root.exists(&path).await {
            let mut writer = root
                .async_writer_path_with_type(&path, EntryType::FilePhysicalVersion)
                .await?;
            writer.write_all(&bytes).await?;
            writer.shutdown().await?;
        } else {
            let _ = create_file_path(&root, &path, &bytes).await?;
        }
        Ok(())
    })
    .await
    .expect("write transaction");
}

async fn delete_file(ship: &mut Ship, path: &str) {
    let path = path.to_string();
    ship.write_transaction(&meta("delete"), async move |transaction| {
        let root = transaction.root().await?;
        root.remove_entry(path.trim_start_matches('/')).await?;
        Ok(())
    })
    .await
    .expect("delete transaction");
}

async fn append_file_series(ship: &mut Ship, path: &str, values: &[&[u8]]) {
    let path = path.to_string();
    let values = values
        .iter()
        .map(|value| value.to_vec())
        .collect::<Vec<_>>();
    ship.write_transaction(&meta("series"), async move |transaction| {
        let root = transaction.root().await?;
        for value in values {
            let mut writer = root
                .async_writer_path_with_type(&path, EntryType::FilePhysicalSeries)
                .await?;
            writer.write_all(&value).await?;
            writer.shutdown().await?;
        }
        Ok(())
    })
    .await
    .expect("series transaction");
}

async fn new_pond(label: &str) -> (tempfile::TempDir, Ship) {
    let directory = tempdir().expect("tempdir");
    let ship = Ship::create_pond(directory.path().join("pond"), label)
        .await
        .expect("create pond");
    (directory, ship)
}

async fn new_remote(ship: &Ship) -> (tempfile::TempDir, ContentRemote) {
    let directory = tempdir().expect("remote tempdir");
    let pond_id = ship.control_table().pond_id_uuid();
    let remote = ContentRemote::create_at(directory.path().join("remote"), pond_id)
        .await
        .expect("create remote");
    (directory, remote)
}

async fn local_tip_commit(ship: &Ship) -> (ObjectHash, Commit) {
    let seq = ship
        .control_table()
        .latest_spine_seq()
        .await
        .expect("read latest spine")
        .expect("local commit spine");
    let encoded = ship
        .control_table()
        .commit_object_at(seq)
        .await
        .expect("read local commit")
        .expect("local commit object");
    let bytes = hex::decode(encoded).expect("decode local commit hex");
    let commit = Commit::decode(&bytes).expect("decode local commit");
    (commit.hash(), commit)
}

async fn stage_unacknowledged_publication(
    ship: &Ship,
    remote: &mut ContentRemote,
) -> (ObjectHash, Commit) {
    remote.inject_publication_failure(PublicationFailurePoint::AfterRecord);
    let error = push_content_to_remote(ship, remote, "main")
        .await
        .expect_err("stage immutable delta without advancing the publication row");
    assert!(error.to_string().contains("injected publication failure"));
    local_tip_commit(ship).await
}

#[tokio::test]
async fn initial_publication_exposes_a_fetchable_bounded_state() {
    let (_pond, mut ship) = new_pond("initial-v2").await;
    write_file(&mut ship, "/a.txt", b"alpha").await;
    write_file(&mut ship, "/b.txt", b"beta").await;
    let (_remote_dir, mut remote) = new_remote(&ship).await;

    let outcome = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("initial push");
    let state = remote
        .current_publication("main")
        .await
        .expect("state")
        .expect("published row");
    assert_eq!(state, outcome.state);
    assert_eq!(state.generation, 1);
    assert_eq!(remote.publication_version(), 1);

    let record = remote
        .get_publication_record(state.publication_record)
        .await
        .expect("record read")
        .expect("record exists");
    assert_eq!(record.hash(), state.publication_record);
    assert_eq!(record.snapshot_tip, state.snapshot_tip);
    assert_eq!(record.manifest_root, state.manifest_root);
    assert!(record.parent_publication_record.is_none());

    let tip_bytes = remote
        .get_immutable_object(state.snapshot_tip)
        .await
        .expect("tip read")
        .expect("tip exists");
    let commit = Commit::decode(&tip_bytes).expect("decode tip");
    assert_eq!(commit.manifest_root, state.manifest_root);
    assert!(
        remote
            .get_immutable_object(commit.root_tree_hash)
            .await
            .expect("root read")
            .is_some()
    );
    let graph = fetch_object_graph(&remote, "main")
        .await
        .expect("visible state is fetchable");
    assert_eq!(graph.tip, Some(state.snapshot_tip));
}

#[tokio::test]
async fn retry_and_historical_reintroduction_create_no_duplicate_payload() {
    let (_pond, mut ship) = new_pond("retry-v2").await;
    write_file(&mut ship, "/a.txt", b"alpha").await;
    let (_remote_dir, mut remote) = new_remote(&ship).await;

    let first = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("first push");
    let object_count = remote.immutable_object_count().await.expect("object count");
    let publication_version = remote.publication_version();
    let retry = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("retry");
    assert_eq!(retry.tip, first.tip);
    assert_eq!(retry.objects_pushed, 0);
    assert_eq!(remote.publication_version(), publication_version);
    assert_eq!(remote.immutable_object_count().await.unwrap(), object_count);

    write_file(&mut ship, "/a.txt", b"beta").await;
    let _ = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("publish beta");
    write_file(&mut ship, "/a.txt", b"alpha").await;
    let reintroduced = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("reintroduce alpha");
    let record = remote
        .get_publication_record(reintroduced.publication_record)
        .await
        .unwrap()
        .unwrap();
    let alpha = ObjectHash::of_bytes(b"alpha");
    assert!(
        record
            .introduced_objects
            .iter()
            .any(|object| object.hash == alpha),
        "historical content is still a logical addition to this push"
    );
    assert_eq!(
        remote.get_immutable_object(alpha).await.unwrap().unwrap(),
        b"alpha"
    );
}

#[tokio::test]
async fn raw_payload_magic_prefixes_keep_raw_blob_receipts() {
    let (_pond, mut ship) = new_pond("raw-magic-prefixes").await;
    let payloads: [(&str, &[u8]); 7] = [
        ("/tree.bin", b"watertown.tree.not-a-tree"),
        ("/legacy-tree.bin", b"dp.tree.not-a-tree"),
        ("/series.bin", b"watertown.series.not-a-series"),
        ("/recipe.bin", b"watertown.recipe.not-a-recipe"),
        ("/legacy-recipe.bin", b"dp.recipe.not-a-recipe"),
        ("/commit.bin", b"watertown.commit.not-a-commit"),
        (
            "/manifest.bin",
            b"watertown.manifest-map-node.not-a-manifest",
        ),
    ];
    for (path, bytes) in payloads {
        write_file(&mut ship, path, bytes).await;
    }
    let (_remote_dir, mut remote) = new_remote(&ship).await;
    let _ = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("initial publication must classify arbitrary files as raw blobs");

    for (path, _) in payloads {
        delete_file(&mut ship, path).await;
    }
    let _ = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("publish deletions");

    for (path, bytes) in payloads {
        write_file(&mut ship, path, bytes).await;
    }
    let _ = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("reintroduced typed raw descriptors must agree with immutable receipts");
}

#[tokio::test]
async fn identical_payload_bytes_can_serve_raw_and_structured_roles() {
    let (_pond, mut ship) = new_pond("shared-payload-roles").await;
    let factory = "shared-recipe-factory";
    let config = b"key: shared-value\n".to_vec();
    let recipe = encode_recipe(factory, &config);
    let recipe_hash = ObjectHash::of_bytes(&recipe);
    let raw_recipe = recipe.clone();
    ship.write_transaction(&meta("shared-roles"), async move |transaction| {
        let root = transaction.root().await?;
        let _ = create_file_path(&root, "/recipe.bin", &raw_recipe).await?;
        let _ = root
            .create_dynamic_path("/dynamic", EntryType::FileDynamic, factory, config)
            .await?;
        Ok(())
    })
    .await
    .expect("write raw and dynamic nodes with identical payload bytes");

    let (_remote_dir, mut remote) = new_remote(&ship).await;
    let pushed = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("publish payload used in two semantic roles");
    let record = remote
        .get_publication_record(pushed.publication_record)
        .await
        .expect("read publication record")
        .expect("publication record exists");
    assert!(
        record
            .introduced_objects
            .contains(&sync_store::content::ObjectDescriptor::new(
                recipe_hash,
                ContentObjectKind::RawBlob,
            ),)
    );
    assert!(
        record
            .introduced_objects
            .contains(&sync_store::content::ObjectDescriptor::new(
                recipe_hash,
                ContentObjectKind::Recipe,
            ),)
    );
    assert_eq!(
        remote
            .get_immutable_object(recipe_hash)
            .await
            .expect("read shared payload")
            .expect("shared payload exists"),
        recipe
    );
}

#[tokio::test]
async fn create_then_delete_between_pushes_squashes_to_no_manifest_change() {
    let (_pond, mut ship) = new_pond("squash-create-delete").await;
    write_file(&mut ship, "/stable.txt", b"stable").await;
    let (_remote_dir, mut remote) = new_remote(&ship).await;
    let first = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("initial push");

    write_file(&mut ship, "/transient.txt", b"temporary").await;
    delete_file(&mut ship, "/transient.txt").await;
    let next = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("publish create/delete window");
    assert_ne!(next.tip, first.tip);
    let record = remote
        .get_publication_record(next.publication_record)
        .await
        .expect("read publication")
        .expect("publication exists");
    assert!(
        record.manifest_changes.is_empty(),
        "a snapshot restored to its prior manifest must publish no net manifest changes"
    );
    let _ = fetch_object_graph(&remote, "main")
        .await
        .expect("resulting publication is fetchable");
}

#[tokio::test]
async fn identical_series_content_accepts_alternative_segmentation() {
    let (_pond, mut ship) = new_pond("series-segmentation").await;
    append_file_series(&mut ship, "/incremental.series", &[b"x"]).await;
    let (_remote_dir, mut remote) = new_remote(&ship).await;
    let _ = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("publish first segment");

    append_file_series(&mut ship, "/incremental.series", &[b"y"]).await;
    let _ = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("publish linked second segment");

    append_file_series(&mut ship, "/one-transaction.series", &[b"x", b"y"]).await;
    let outcome = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("publish equivalent whole-range segmentation");
    let graph = fetch_object_graph(&remote, "main")
        .await
        .expect("fetch publication with shared series identity");
    assert_eq!(graph.tip, Some(outcome.tip));
}

#[tokio::test]
async fn physical_create_and_byte_counters_prove_retry_uniqueness() {
    let (_pond, mut ship) = new_pond("physical-counters-v2").await;
    write_file(&mut ship, "/payload.txt", b"counter payload").await;
    let pond_id = ship.control_table().pond_id_uuid();
    let url = sync_store::testing::in_memory_remote_url("publication-physical-counters");
    let mut remote = ContentRemote::create_at_url(&url, pond_id, Default::default())
        .await
        .expect("create remote");
    let key = sync_store::RemoteKey::new(&url);
    let before = sync_store::access_summary_under(&key);
    let first = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("first push");
    let first_cost = sync_store::access_summary_under(&key).saturating_sub(&before);
    assert_eq!(
        first_cost
            .physical_creates(sync_store::AccessClass::ContentObjects)
            .ops,
        first.objects_pushed as u64
    );
    assert!(
        first_cost
            .physical_creates(sync_store::AccessClass::ContentObjects)
            .bytes
            > 0
    );

    let before_retry = sync_store::access_summary_under(&key);
    let retry = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("retry");
    let retry_cost = sync_store::access_summary_under(&key).saturating_sub(&before_retry);
    assert_eq!(retry.objects_pushed, 0);
    assert_eq!(
        retry_cost.physical_creates(sync_store::AccessClass::ContentObjects),
        sync_store::AccessTotals::default()
    );
}

#[tokio::test]
async fn small_change_publishes_only_changed_paths_and_payloads() {
    let (_pond, mut ship) = new_pond("small-change-v2").await;
    for index in 0..40 {
        write_file(
            &mut ship,
            &format!("/stable-{index}.txt"),
            format!("stable-{index}").as_bytes(),
        )
        .await;
    }
    let (_remote_dir, mut remote) = new_remote(&ship).await;
    let first = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("initial push");

    write_file(&mut ship, "/changed.txt", b"one small change").await;
    let second = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("incremental push");
    let record = remote
        .get_publication_record(second.publication_record)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        record.parent_publication_record,
        Some(first.publication_record)
    );
    assert!(
        record.introduced_objects.len() < 20,
        "one file change should create only its payload, changed trees, commit, and a short \
         Patricia path; got {} objects",
        record.introduced_objects.len()
    );
    assert!(
        record.manifest_changes.len() <= 3,
        "new file plus root identity should be a bounded change set"
    );
}

#[tokio::test]
async fn failures_before_and_after_visibility_converge_without_broken_refs() {
    let (_pond, mut ship) = new_pond("failure-v2").await;
    write_file(&mut ship, "/initial.txt", b"initial").await;
    let (_remote_dir, mut remote) = new_remote(&ship).await;
    let mut prior = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("initial push")
        .state;

    let points = [
        PublicationFailurePoint::BeforeObjects,
        PublicationFailurePoint::AfterObject(1),
        PublicationFailurePoint::AfterObjects,
        PublicationFailurePoint::AfterPacks,
        PublicationFailurePoint::AfterRecord,
        PublicationFailurePoint::AfterRef,
    ];
    for (index, point) in points.into_iter().enumerate() {
        write_file(
            &mut ship,
            &format!("/change-{index}.txt"),
            format!("change-{index}").as_bytes(),
        )
        .await;
        remote.inject_publication_failure(point);
        let failure = push_content_to_remote(&ship, &mut remote, "main")
            .await
            .expect_err("injected failure");
        assert!(failure.to_string().contains("injected publication failure"));

        let visible = remote
            .current_publication("main")
            .await
            .unwrap()
            .expect("visible state");
        if point == PublicationFailurePoint::AfterRef {
            assert_ne!(visible.snapshot_tip, prior.snapshot_tip);
        } else {
            assert_eq!(visible, prior, "failure before CAS must preserve old state");
        }
        let _ = fetch_object_graph(&remote, "main")
            .await
            .expect("every visible state remains fetchable");

        let retried = push_content_to_remote(&ship, &mut remote, "main")
            .await
            .expect("retry converges");
        assert_eq!(
            retried.generation,
            prior.generation + 1,
            "one logical update advances exactly one generation"
        );
        prior = retried.state;
    }
}

#[tokio::test]
async fn structured_ack_skips_and_restores_exact_publication_identity() {
    let (_pond, mut ship) = new_pond("ack-v2").await;
    write_file(&mut ship, "/a.txt", b"alpha").await;
    let (_remote_dir, mut remote) = new_remote(&ship).await;
    let url = "file:///operator-attached-name";
    let outcome = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("publish");

    assert!(!remote_tip_is_acknowledged(&ship, url).await.unwrap());
    write_push_ack(ship.control_table_mut(), url, &outcome.state)
        .await
        .expect("write ack");
    assert!(remote_tip_is_acknowledged(&ship, url).await.unwrap());
    let acknowledgement = read_push_ack(
        ship.control_table(),
        url,
        ship.control_table().pond_id_uuid(),
        "main",
    )
    .await
    .unwrap()
    .expect("structured ack");
    let restored = acknowledgement
        .state(url, ship.control_table().pond_id_uuid(), "main")
        .unwrap();
    assert_eq!(restored.snapshot_tip, outcome.tip);
    assert_eq!(restored.manifest_root, outcome.manifest_root);
    assert_eq!(restored.publication_record, outcome.publication_record);
    assert_eq!(restored.generation, outcome.generation);
    let key = sync_store::RemoteKey::new(url);
    let before = sync_store::access_summary_under(&key);
    assert!(remote_tip_is_acknowledged(&ship, url).await.unwrap());
    assert_eq!(
        sync_store::access_summary_under(&key)
            .saturating_sub(&before)
            .total(),
        sync_store::AccessTotals::default(),
        "an acknowledged no-op must perform no remote operation or transfer"
    );
}

#[tokio::test]
async fn missed_producer_ack_recovers_through_a_later_local_commit() {
    let (_pond, mut ship) = new_pond("producer-ack-recovery").await;
    let (_remote_dir, mut remote) = new_remote(&ship).await;
    let url = remote.url();

    write_file(&mut ship, "/a.txt", b"A").await;
    let a = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("publish A");
    write_push_ack(ship.control_table_mut(), &url, &a.state)
        .await
        .expect("ack A");

    write_file(&mut ship, "/b.txt", b"B").await;
    let b = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("publish B without persisting its ack");

    write_file(&mut ship, "/c.txt", b"C").await;
    let c = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("recover B as the baseline and publish only B to C");
    assert_eq!(c.generation, b.generation + 1);
    assert_eq!(
        remote.current_publication("main").await.unwrap(),
        Some(c.state.clone())
    );
    let record = remote
        .get_publication_record(c.publication_record)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.parent_publication_record, Some(b.publication_record));
    assert!(
        record.introduced_objects.len() < 20,
        "recovery must publish only the bounded B-to-C delta, got {} objects",
        record.introduced_objects.len()
    );
}

#[tokio::test]
async fn missed_producer_ack_rejects_inventory_shifted_between_publications() {
    let (_pond, mut ship) = new_pond("producer-ack-shifted-inventory").await;
    let (_remote_dir, mut remote) = new_remote(&ship).await;
    let url = remote.url();

    write_file(&mut ship, "/a.txt", b"A").await;
    let a = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("publish A");
    write_push_ack(ship.control_table_mut(), &url, &a.state)
        .await
        .expect("ack A");

    write_file(&mut ship, "/b.txt", b"B").await;
    let (b_tip, b_commit) = stage_unacknowledged_publication(&ship, &mut remote).await;
    write_file(&mut ship, "/c.txt", b"C").await;
    let (c_tip, c_commit) = stage_unacknowledged_publication(&ship, &mut remote).await;

    let mut b_objects = b_commit.introduced_objects.clone();
    b_objects.push(sync_store::content::ObjectDescriptor::new(
        b_tip,
        ContentObjectKind::Commit,
    ));
    let shifted = b_objects
        .iter()
        .copied()
        .find(|descriptor| descriptor.hash != b_tip)
        .expect("B has a non-commit introduced object");
    b_objects.retain(|descriptor| *descriptor != shifted);
    let rogue_b = PublicationRecord::new(
        a.state.pond_id,
        "main",
        b_tip,
        b_commit.manifest_root,
        Some(a.publication_record),
        b_objects,
        b_commit.introduced_packs.clone(),
        b_commit.manifest_changes.clone(),
    )
    .unwrap();
    let _ = remote.put_publication_record(&rogue_b).await.unwrap();
    let rogue_b_state = PublicationState::new(
        a.state.pond_id,
        "main",
        b_tip,
        b_commit.manifest_root,
        rogue_b.hash(),
        a.generation + 1,
        1,
    )
    .unwrap();
    let _ = remote
        .compare_and_swap_publication(
            PublicationExpectation::Existing {
                generation: a.generation,
                publication_record: a.publication_record,
            },
            rogue_b_state.clone(),
        )
        .await
        .unwrap();

    let mut c_objects = c_commit.introduced_objects.clone();
    c_objects.push(sync_store::content::ObjectDescriptor::new(
        c_tip,
        ContentObjectKind::Commit,
    ));
    c_objects.push(shifted);
    let rogue_c = PublicationRecord::new(
        a.state.pond_id,
        "main",
        c_tip,
        c_commit.manifest_root,
        Some(rogue_b.hash()),
        c_objects,
        c_commit.introduced_packs.clone(),
        c_commit.manifest_changes.clone(),
    )
    .unwrap();
    let _ = remote.put_publication_record(&rogue_c).await.unwrap();
    let rogue_c_state = PublicationState::new(
        a.state.pond_id,
        "main",
        c_tip,
        c_commit.manifest_root,
        rogue_c.hash(),
        rogue_b_state.generation + 1,
        2,
    )
    .unwrap();
    let _ = remote
        .compare_and_swap_publication(
            PublicationExpectation::Existing {
                generation: rogue_b_state.generation,
                publication_record: rogue_b_state.publication_record,
            },
            rogue_c_state,
        )
        .await
        .unwrap();

    write_file(&mut ship, "/d.txt", b"D").await;
    let error = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect_err("each publication interval must authenticate independently");
    assert!(
        error
            .to_string()
            .contains("introduced object inventory does not exactly match"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn missing_producer_ack_authenticates_existing_remote_history() {
    let (_pond, mut ship) = new_pond("producer-no-ack-history").await;
    let (_remote_dir, mut remote) = new_remote(&ship).await;
    write_file(&mut ship, "/a.txt", b"A").await;
    write_file(&mut ship, "/b.txt", b"B").await;
    let published = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("publish without ack");

    let recovered = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("authenticate existing genesis and converge");
    assert_eq!(recovered.state, published.state);
    assert_eq!(recovered.objects_pushed, 0);
}

#[tokio::test]
async fn missing_producer_ack_rejects_missing_remote_record() {
    let (_pond, mut ship) = new_pond("producer-no-ack-missing-record").await;
    let (_remote_dir, mut remote) = new_remote(&ship).await;
    write_file(&mut ship, "/a.txt", b"A").await;
    let (tip, commit) = local_tip_commit(&ship).await;
    let missing_record = ObjectHash::of_bytes(b"missing-record");
    let forged = PublicationState::new(
        ship.control_table().pond_id_uuid(),
        "main",
        tip,
        commit.manifest_root,
        missing_record,
        1,
        2,
    )
    .unwrap();
    let _ = remote
        .compare_and_swap_publication(PublicationExpectation::Missing, forged)
        .await
        .unwrap();

    let error = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect_err("missing record must not be trusted without a local ack");
    assert!(error.to_string().contains("is absent"));
}

#[tokio::test]
async fn missing_producer_ack_rejects_forged_genesis_inventory() {
    let (_pond, mut ship) = new_pond("producer-no-ack-forged-genesis").await;
    let (_remote_dir, mut remote) = new_remote(&ship).await;
    write_file(&mut ship, "/a.txt", b"A").await;
    let (tip, commit) = stage_unacknowledged_publication(&ship, &mut remote).await;
    let mut introduced = commit.introduced_objects.clone();
    introduced.push(sync_store::content::ObjectDescriptor::new(
        tip,
        ContentObjectKind::Commit,
    ));
    let _ = introduced.pop();
    let forged_record = PublicationRecord::new(
        ship.control_table().pond_id_uuid(),
        "main",
        tip,
        commit.manifest_root,
        None,
        introduced,
        commit.introduced_packs.clone(),
        commit.manifest_changes.clone(),
    )
    .unwrap();
    let _ = remote.put_publication_record(&forged_record).await.unwrap();
    let forged = PublicationState::new(
        ship.control_table().pond_id_uuid(),
        "main",
        tip,
        commit.manifest_root,
        forged_record.hash(),
        1,
        2,
    )
    .unwrap();
    let _ = remote
        .compare_and_swap_publication(PublicationExpectation::Missing, forged)
        .await
        .unwrap();

    let error = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect_err("forged genesis inventory must not be trusted");
    assert!(
        error
            .to_string()
            .contains("introduced object inventory does not exactly match"),
        "{error}"
    );
}

#[tokio::test]
async fn missed_producer_ack_rejects_omitted_introduced_object_inventory() {
    let (_pond, mut ship) = new_pond("producer-ack-omitted-object").await;
    let (_remote_dir, mut remote) = new_remote(&ship).await;
    let url = remote.url();

    write_file(&mut ship, "/a.txt", b"A").await;
    let a = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("publish A");
    write_push_ack(ship.control_table_mut(), &url, &a.state)
        .await
        .expect("ack A");

    write_file(&mut ship, "/b.txt", b"B").await;
    let (b_tip, b_commit) = stage_unacknowledged_publication(&ship, &mut remote).await;
    let mut introduced_objects = b_commit.introduced_objects.clone();
    introduced_objects.push(sync_store::content::ObjectDescriptor::new(
        b_tip,
        ContentObjectKind::Commit,
    ));
    introduced_objects.retain(|descriptor| descriptor.hash != b_tip);
    let rogue = PublicationRecord::new(
        a.state.pond_id,
        "main",
        b_tip,
        b_commit.manifest_root,
        Some(a.publication_record),
        introduced_objects,
        b_commit.introduced_packs.clone(),
        b_commit.manifest_changes.clone(),
    )
    .unwrap();
    let _ = remote.put_publication_record(&rogue).await.unwrap();
    let rogue_state = PublicationState::new(
        a.state.pond_id,
        "main",
        b_tip,
        b_commit.manifest_root,
        rogue.hash(),
        a.generation + 1,
        1,
    )
    .unwrap();
    let _ = remote
        .compare_and_swap_publication(
            PublicationExpectation::Existing {
                generation: a.generation,
                publication_record: a.publication_record,
            },
            rogue_state,
        )
        .await
        .unwrap();

    write_file(&mut ship, "/c.txt", b"C").await;
    let error = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect_err("omitted remote inventory must not become the next baseline");
    assert!(
        error
            .to_string()
            .contains("introduced object inventory does not exactly match"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn missed_producer_ack_rejects_mutated_manifest_changes() {
    let (_pond, mut ship) = new_pond("producer-ack-mutated-change").await;
    let (_remote_dir, mut remote) = new_remote(&ship).await;
    let url = remote.url();

    write_file(&mut ship, "/a.txt", b"A").await;
    let a = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("publish A");
    write_push_ack(ship.control_table_mut(), &url, &a.state)
        .await
        .expect("ack A");

    write_file(&mut ship, "/b.txt", b"B").await;
    let (b_tip, b_commit) = stage_unacknowledged_publication(&ship, &mut remote).await;
    assert!(
        !b_commit.manifest_changes.is_empty(),
        "B must carry a manifest change for this regression"
    );
    let mut introduced_objects = b_commit.introduced_objects.clone();
    introduced_objects.push(sync_store::content::ObjectDescriptor::new(
        b_tip,
        ContentObjectKind::Commit,
    ));
    let rogue = PublicationRecord::new(
        a.state.pond_id,
        "main",
        b_tip,
        b_commit.manifest_root,
        Some(a.publication_record),
        introduced_objects,
        b_commit.introduced_packs.clone(),
        Vec::new(),
    )
    .unwrap();
    let _ = remote.put_publication_record(&rogue).await.unwrap();
    let rogue_state = PublicationState::new(
        a.state.pond_id,
        "main",
        b_tip,
        b_commit.manifest_root,
        rogue.hash(),
        a.generation + 1,
        1,
    )
    .unwrap();
    let _ = remote
        .compare_and_swap_publication(
            PublicationExpectation::Existing {
                generation: a.generation,
                publication_record: a.publication_record,
            },
            rogue_state,
        )
        .await
        .unwrap();

    write_file(&mut ship, "/c.txt", b"C").await;
    let error = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect_err("mutated remote changes must not become the next baseline");
    assert!(
        error
            .to_string()
            .contains("manifest changes do not exactly match"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn missed_producer_ack_rejects_unrelated_publication_lineage() {
    let (_pond, mut ship) = new_pond("producer-ack-divergence").await;
    let (_remote_dir, mut remote) = new_remote(&ship).await;
    let url = remote.url();

    write_file(&mut ship, "/a.txt", b"A").await;
    let a = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("publish A");
    write_push_ack(ship.control_table_mut(), &url, &a.state)
        .await
        .expect("ack A");
    write_file(&mut ship, "/b.txt", b"B").await;
    let b = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("publish B");

    let rogue = PublicationRecord::new(
        b.state.pond_id,
        "main",
        b.tip,
        b.manifest_root,
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let rogue_hash = rogue.hash();
    let _ = remote.put_publication_record(&rogue).await.unwrap();
    let rogue_state = PublicationState::new(
        b.state.pond_id,
        "main",
        b.tip,
        b.manifest_root,
        rogue_hash,
        b.generation + 1,
        1,
    )
    .unwrap();
    let _ = remote
        .compare_and_swap_publication(
            PublicationExpectation::Existing {
                generation: b.generation,
                publication_record: b.publication_record,
            },
            rogue_state,
        )
        .await
        .unwrap();

    write_file(&mut ship, "/c.txt", b"C").await;
    let error = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect_err("unrelated publication chain must be rejected");
    assert!(
        error
            .to_string()
            .contains("does not descend from acknowledged record")
            || error
                .to_string()
                .contains("exceeds the authenticated local commit interval"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn missed_producer_ack_rejects_remote_tip_outside_local_ancestry() {
    let (_pond, mut ship) = new_pond("producer-ack-foreign-tip").await;
    let (_remote_dir, mut remote) = new_remote(&ship).await;
    let url = remote.url();

    write_file(&mut ship, "/a.txt", b"A").await;
    let a = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("publish A");
    write_push_ack(ship.control_table_mut(), &url, &a.state)
        .await
        .expect("ack A");

    let foreign_tip = ObjectHash::of_bytes(b"not-a-local-commit");
    let foreign_root = ObjectHash::of_bytes(b"not-a-local-manifest");
    let rogue = PublicationRecord::new(
        a.state.pond_id,
        "main",
        foreign_tip,
        foreign_root,
        Some(a.publication_record),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let rogue_hash = rogue.hash();
    let _ = remote.put_publication_record(&rogue).await.unwrap();
    let rogue_state = PublicationState::new(
        a.state.pond_id,
        "main",
        foreign_tip,
        foreign_root,
        rogue_hash,
        a.generation + 1,
        1,
    )
    .unwrap();
    let _ = remote
        .compare_and_swap_publication(
            PublicationExpectation::Existing {
                generation: a.generation,
                publication_record: a.publication_record,
            },
            rogue_state,
        )
        .await
        .unwrap();

    write_file(&mut ship, "/c.txt", b"C").await;
    let error = push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect_err("remote tip outside local ancestry must be rejected");
    assert!(
        error
            .to_string()
            .contains("not authenticated in local history"),
        "unexpected error: {error}"
    );
}
