// SPDX-License-Identifier: Apache-2.0

use sync_store::content::{ContentObjectKind, ObjectDescriptor, ObjectHash, PublicationRecord};
use sync_store::{ContentRemote, PublicationExpectation, PublicationState};
use tempfile::tempdir;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

#[tokio::test]
async fn native_v2_remote_reopens_with_fetchable_state() {
    let directory = tempdir().unwrap();
    let root = directory.path().join("remote");
    let pond = Uuid::new_v4();
    let mut remote = ContentRemote::create_at(&root, pond).await.unwrap();
    let commit_bytes = b"synthetic immutable commit".to_vec();
    let tip = ObjectHash::of_bytes(&commit_bytes);
    remote
        .put_immutable_object(
            ObjectDescriptor::new(tip, ContentObjectKind::RawBlob),
            &commit_bytes,
        )
        .await
        .unwrap();
    let manifest = ObjectHash::of_bytes(b"manifest");
    let record =
        PublicationRecord::new(pond, "main", tip, manifest, None, vec![], vec![], vec![]).unwrap();
    remote.put_publication_record(&record).await.unwrap();
    let state = PublicationState::new(pond, "main", tip, manifest, record.hash(), 1, 1).unwrap();
    remote
        .compare_and_swap_publication(PublicationExpectation::Missing, state.clone())
        .await
        .unwrap();
    drop(remote);

    let reopened = ContentRemote::open_at(&root, pond).await.unwrap();
    assert_eq!(
        reopened.current_publication("main").await.unwrap(),
        Some(state)
    );
    assert_eq!(
        reopened.get_immutable_object(tip).await.unwrap(),
        Some(commit_bytes)
    );
    assert_eq!(
        reopened
            .get_publication_record(record.hash())
            .await
            .unwrap(),
        Some(record)
    );
}

#[tokio::test]
async fn streamed_payload_is_unique_and_readable() {
    let directory = tempdir().unwrap();
    let remote = ContentRemote::create_at(directory.path().join("remote"), Uuid::new_v4())
        .await
        .unwrap();
    let bytes = vec![0x5a; 6 * 1024 * 1024 + 17];
    let hash = ObjectHash::of_bytes(&bytes);
    let descriptor = ObjectDescriptor::new(hash, ContentObjectKind::RawBlob);
    let first = remote
        .put_immutable_object_stream(descriptor, &bytes[..])
        .await
        .unwrap();
    assert!(first.payload_created);
    let retry = remote
        .put_immutable_object_stream(descriptor, tokio::io::empty())
        .await
        .unwrap();
    assert!(
        !retry.payload_created,
        "valid receipt skips the supplied reader"
    );
    assert_eq!(remote.immutable_object_count().await.unwrap(), 1);
    let mut reader = remote
        .get_immutable_object_reader(hash)
        .await
        .unwrap()
        .unwrap();
    let mut actual = Vec::new();
    reader.read_to_end(&mut actual).await.unwrap();
    assert_eq!(actual, bytes);
}

#[tokio::test]
async fn stale_stream_upload_cleanup_is_explicit_and_scoped() {
    let directory = tempdir().unwrap();
    let root = directory.path().join("remote");
    let remote = ContentRemote::create_at(&root, Uuid::new_v4())
        .await
        .unwrap();
    let upload_dir = root.join("_content/v2/fallback/uploads");
    std::fs::create_dir_all(&upload_dir).unwrap();
    let orphan = upload_dir.join("interrupted-upload");
    std::fs::write(&orphan, b"orphaned bytes").unwrap();
    let unrelated = root.join("_content/v2/unrelated");
    std::fs::write(&unrelated, b"keep").unwrap();

    let outcome = remote
        .cleanup_stale_uploads(chrono::Utc::now() + chrono::Duration::seconds(1))
        .await
        .unwrap();
    assert_eq!(outcome.objects_deleted, 1);
    assert_eq!(outcome.bytes_deleted, b"orphaned bytes".len() as u64);
    assert!(!orphan.exists());
    assert!(unrelated.exists());
}

#[tokio::test]
async fn diagnostic_pack_scan_ignores_series_locators() {
    let directory = tempdir().unwrap();
    let remote = ContentRemote::create_at(directory.path().join("remote"), Uuid::new_v4())
        .await
        .unwrap();
    let series = ObjectHash::of_bytes(b"series");
    let pack_dir = directory.path().join("remote/_content/v2/packs/by-series");
    std::fs::create_dir_all(&pack_dir).unwrap();
    std::fs::write(
        pack_dir.join(format!("blake3={}", series.to_hex())),
        ObjectHash::of_bytes(b"pack").as_bytes(),
    )
    .unwrap();

    assert!(
        remote
            .diagnostic_list_pack_hashes(series)
            .await
            .unwrap()
            .is_empty()
    );
}
