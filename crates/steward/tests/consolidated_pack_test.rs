// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use steward::{
    FetchedObject, Ship, fetch_object_graph, publish_local_consolidated_packs,
    push_content_to_remote,
};
use sync_store::content::{ObjectHash, PackIndex};
use sync_store::testing::in_memory_remote_url;
use sync_store::{ContentRemote, PublicationFailurePoint};
use tempfile::TempDir;
use tinyfs::EntryType;
use tlogfs::PondUserMetadata;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

fn meta(label: &str) -> PondUserMetadata {
    PondUserMetadata::new(vec!["consolidated-pack-test".into(), label.into()])
}

async fn append_series(ship: &mut Ship, index: usize) {
    let bytes = format!("leaf-{index:04}\n").into_bytes();
    ship.write_transaction(&meta("append"), async move |transaction| {
        let root = transaction.root().await?;
        let mut writer = root
            .async_writer_path_with_type("/events.series", EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(&bytes).await?;
        writer.shutdown().await?;
        Ok(())
    })
    .await
    .expect("append series");
}

fn only_series_hash(graph: &steward::FetchedGraph) -> ObjectHash {
    let mut found = graph.objects.values().filter_map(|object| match object {
        FetchedObject::SeriesV2(series) => Some(series.manifest_hash),
        _ => None,
    });
    let hash = found.next().expect("one series");
    assert!(found.next().is_none());
    hash
}

struct Fixture {
    _pond_dir: TempDir,
    ship: Ship,
    remote: ContentRemote,
    series_hash: ObjectHash,
}

async fn fixture(label: &str) -> Fixture {
    let pond_dir = TempDir::new().expect("pond tempdir");
    let mut ship = Ship::create_pond(pond_dir.path().join("pond"), label)
        .await
        .expect("create pond");
    let url = in_memory_remote_url(&format!("{label}-{}", Uuid::new_v4()));
    let mut remote = ContentRemote::create_at_url(
        &url,
        ship.control_table().pond_id_uuid(),
        BTreeMap::new().into_iter().collect(),
    )
    .await
    .expect("create remote");
    for index in 0..3 {
        append_series(&mut ship, index).await;
        let _ = push_content_to_remote(&ship, &mut remote, "main")
            .await
            .expect("publish append");
    }
    let graph = fetch_object_graph(&remote, "main")
        .await
        .expect("fetch linked series");
    let series_hash = only_series_hash(&graph);
    assert_eq!(
        graph
            .objects
            .get(&series_hash)
            .and_then(|object| match object {
                FetchedObject::SeriesV2(series) => Some(series.packs.len()),
                _ => None,
            }),
        Some(3)
    );
    let report = ship
        .collapse_versions(1)
        .await
        .expect("create local consolidated pack");
    assert_eq!(report.series_repacked, 1);
    Fixture {
        _pond_dir: pond_dir,
        ship,
        remote,
        series_hash,
    }
}

fn local_pack_path(pond: &Path, series_hash: ObjectHash) -> PathBuf {
    let directory = steward::get_data_path(pond)
        .join(sync_store::pack_keys::PACK_INDEX_ROOT)
        .join(sync_store::pack_keys::series_dir_name(series_hash));
    std::fs::read_dir(&directory)
        .expect("read local pack directory")
        .filter_map(Result::ok)
        .find_map(|entry| {
            sync_store::pack_keys::parse_pack_file_name(entry.file_name().to_str()?)
                .ok()
                .map(|_| entry.path())
        })
        .expect("one local pack index")
}

fn local_pack(pond: &Path, series_hash: ObjectHash) -> PackIndex {
    PackIndex::decode(
        &std::fs::read(local_pack_path(pond, series_hash)).expect("read local pack index"),
    )
    .expect("decode local pack")
}

fn local_object_path(pond: &Path, hash: ObjectHash) -> PathBuf {
    steward::get_data_path(pond)
        .join(sync_store::pack_keys::PACKS_ROOT)
        .join("objects")
        .join(hash.to_hex())
}

#[tokio::test]
async fn explicit_publication_is_retry_idempotent_and_fresh_clone_uses_one_pack() {
    let mut fixture = fixture("consolidated-success").await;
    let before_objects = fixture.remote.immutable_object_count().await.unwrap();
    let first = publish_local_consolidated_packs(&fixture.ship, &mut fixture.remote, "main")
        .await
        .expect("publish consolidated pack");
    assert_eq!(first.selections.len(), 1);
    assert_eq!(first.selections[0].series_hash, fixture.series_hash);
    assert_eq!(first.objects_selected, first.selections[0].objects);
    assert_eq!(first.bytes_selected, first.selections[0].bytes);
    assert_eq!(first.packs_created, 1);
    assert_eq!(first.locators_created, 1);

    let graph = fetch_object_graph(&fixture.remote, "main")
        .await
        .expect("fresh clone through consolidated locator");
    let packs = match graph.objects.get(&fixture.series_hash) {
        Some(FetchedObject::SeriesV2(series)) => series.packs.len(),
        other => panic!("expected fetched series, got {other:?}"),
    };
    assert_eq!(packs, 1);

    let after_first = fixture.remote.immutable_object_count().await.unwrap();
    assert!(after_first >= before_objects);
    let retry = publish_local_consolidated_packs(&fixture.ship, &mut fixture.remote, "main")
        .await
        .expect("retry consolidated publication");
    assert_eq!(retry.selections, first.selections);
    assert_eq!(retry.objects_created, 0);
    assert_eq!(retry.bytes_created, 0);
    assert_eq!(retry.packs_created, 0);
    assert_eq!(retry.locators_created, 0);
    assert_eq!(
        fixture.remote.immutable_object_count().await.unwrap(),
        after_first
    );
}

#[tokio::test]
async fn wrong_remote_identity_and_missing_publication_fail_before_locator() {
    let fixture = fixture("consolidated-identity").await;
    let wrong_url = in_memory_remote_url(&format!("wrong-identity-{}", Uuid::new_v4()));
    let mut wrong = ContentRemote::create_at_url(
        &wrong_url,
        Uuid::new_v4(),
        BTreeMap::new().into_iter().collect(),
    )
    .await
    .expect("create wrong remote");
    let error = publish_local_consolidated_packs(&fixture.ship, &mut wrong, "main")
        .await
        .expect_err("wrong pond identity must fail");
    assert!(error.to_string().contains("does not match local pond"));
    assert!(
        wrong
            .consolidated_pack_for_series(fixture.series_hash)
            .await
            .unwrap()
            .is_none()
    );

    let empty_url = in_memory_remote_url(&format!("missing-state-{}", Uuid::new_v4()));
    let mut empty = ContentRemote::create_at_url(
        &empty_url,
        fixture.ship.control_table().pond_id_uuid(),
        BTreeMap::new().into_iter().collect(),
    )
    .await
    .expect("create empty remote");
    let error = publish_local_consolidated_packs(&fixture.ship, &mut empty, "main")
        .await
        .expect_err("missing current publication must fail");
    assert!(
        error
            .to_string()
            .contains("has no current native-v2 publication")
    );
}

#[tokio::test]
async fn remote_must_already_publish_the_exact_local_snapshot() {
    let mut fixture = fixture("consolidated-stale-remote").await;
    append_series(&mut fixture.ship, 3).await;
    let first = fixture
        .ship
        .collapse_versions(1)
        .await
        .expect("maintain current series");
    assert_eq!(first.series_repacked, 1);
    let _ = fixture
        .ship
        .collapse_versions(1)
        .await
        .expect("prune stale local pack generation");

    let error = publish_local_consolidated_packs(&fixture.ship, &mut fixture.remote, "main")
        .await
        .expect_err("remote behind local snapshot must fail");
    assert!(
        error
            .to_string()
            .contains("push the exact local snapshot first")
    );
}

#[tokio::test]
async fn missing_or_corrupt_local_pack_and_object_fail_closed() {
    {
        let mut fixture = fixture("missing-local-pack").await;
        std::fs::remove_file(local_pack_path(
            fixture.ship.pond_path(),
            fixture.series_hash,
        ))
        .expect("remove local pack");
        let error = publish_local_consolidated_packs(&fixture.ship, &mut fixture.remote, "main")
            .await
            .expect_err("missing local pack must fail");
        assert!(
            error
                .to_string()
                .contains("no verified local consolidated packs")
        );
    }
    {
        let mut fixture = fixture("corrupt-local-pack").await;
        std::fs::write(
            local_pack_path(fixture.ship.pond_path(), fixture.series_hash),
            b"corrupt",
        )
        .expect("corrupt local pack");
        let error = publish_local_consolidated_packs(&fixture.ship, &mut fixture.remote, "main")
            .await
            .expect_err("corrupt local pack must fail");
        assert!(
            error.to_string().contains("content-address mismatch")
                || error.to_string().contains("decode pack")
        );
    }
    {
        let mut fixture = fixture("missing-local-object").await;
        let pack = local_pack(fixture.ship.pond_path(), fixture.series_hash);
        let object = *pack
            .physical_object_hashes()
            .first()
            .expect("pack physical object");
        std::fs::remove_file(local_object_path(fixture.ship.pond_path(), object))
            .expect("remove local object");
        let error = publish_local_consolidated_packs(&fixture.ship, &mut fixture.remote, "main")
            .await
            .expect_err("missing local object must fail");
        assert!(error.to_string().contains("missing physical object"));
    }
    {
        let mut fixture = fixture("corrupt-local-object").await;
        let pack = local_pack(fixture.ship.pond_path(), fixture.series_hash);
        let object = *pack
            .physical_object_hashes()
            .first()
            .expect("pack physical object");
        std::fs::write(
            local_object_path(fixture.ship.pond_path(), object),
            b"corrupt",
        )
        .expect("corrupt local object");
        let error = publish_local_consolidated_packs(&fixture.ship, &mut fixture.remote, "main")
            .await
            .expect_err("corrupt local object must fail");
        assert!(error.to_string().contains("hashes to"));
    }
}

#[tokio::test]
async fn failure_after_pack_before_locator_preserves_chain_and_retry_converges() {
    let mut fixture = fixture("consolidated-failure").await;
    let local_pack = local_pack(fixture.ship.pond_path(), fixture.series_hash);
    let pack_hash = local_pack.hash();
    fixture
        .remote
        .inject_publication_failure(PublicationFailurePoint::BeforeConsolidatedLocator);
    let error = publish_local_consolidated_packs(&fixture.ship, &mut fixture.remote, "main")
        .await
        .expect_err("inject failure before locator");
    assert!(error.to_string().contains("injected publication failure"));
    assert!(
        fixture
            .remote
            .consolidated_pack_for_series(fixture.series_hash)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        fixture
            .remote
            .diagnostic_list_pack_hashes(fixture.series_hash)
            .await
            .unwrap()
            .contains(&pack_hash),
        "pack bytes are durable before locator publication"
    );
    let graph = fetch_object_graph(&fixture.remote, "main")
        .await
        .expect("old linked chain remains valid");
    let series = match graph.objects.get(&fixture.series_hash) {
        Some(FetchedObject::SeriesV2(series)) => series,
        other => panic!("expected fetched series, got {other:?}"),
    };
    assert_eq!(series.packs.len(), 3);

    let retry = publish_local_consolidated_packs(&fixture.ship, &mut fixture.remote, "main")
        .await
        .expect("retry after locator failure");
    assert_eq!(retry.packs_created, 0);
    assert_eq!(retry.locators_created, 1);
    let graph = fetch_object_graph(&fixture.remote, "main")
        .await
        .expect("fresh clone after retry");
    let series = match graph.objects.get(&fixture.series_hash) {
        Some(FetchedObject::SeriesV2(series)) => series,
        other => panic!("expected fetched series, got {other:?}"),
    };
    assert_eq!(series.packs.len(), 1);
}
