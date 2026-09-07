// SPDX-License-Identifier: Apache-2.0

//! Integration tests for [`ContentRemote`]: the delta-managed
//! content-addressed remote (design Section 8, Decision D6).

use sync_store::content::{Commit, ContentModelVersion, ObjectHash, Provenance};
use sync_store::{ContentRemote, Store};
use tempfile::TempDir;
use uuid::Uuid;

fn pid() -> Uuid {
    Uuid::from_u128(0xc0_0000_0000_0000_0000_0000_0000_0000)
}

fn obj(bytes: &[u8]) -> (ObjectHash, Vec<u8>) {
    (ObjectHash::of_bytes(bytes), bytes.to_vec())
}

fn commit(parent: Option<ObjectHash>, seq: i64) -> (ObjectHash, Vec<u8>) {
    let commit = Commit::new(
        ContentModelVersion::LogicalSeriesV2,
        ObjectHash::of_bytes(b"root"),
        parent,
        ObjectHash::of_bytes(b"manifest"),
        ObjectHash::of_bytes(b"manifest-root"),
        Provenance {
            pond_id: pid().to_string(),
            seq,
            time_micros: seq,
            author: "test".to_string(),
            request: "test".to_string(),
        },
    );
    (commit.hash(), commit.encode())
}

async fn added_partition_rows(store: &Store, version: i64, partition: &str) -> (usize, usize) {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let (adds, _) = store.actions_at_version(version).await.unwrap();
    let partition_path = format!("partition_key={partition}/");
    let matching: Vec<_> = adds
        .iter()
        .filter(|add| add.path.contains(&partition_path))
        .collect();
    let mut rows = 0;
    for add in &matching {
        let bytes = store
            .object_store()
            .get(&object_store::path::Path::from(add.path.clone()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        rows += ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap()
            .map(|batch| batch.unwrap().num_rows())
            .sum::<usize>();
    }
    (matching.len(), rows)
}

/// A pushed object reads back by hash, and the tip ref reads back the tip.
#[tokio::test]
async fn push_then_read_objects_and_tip() {
    let dir = TempDir::new().unwrap();
    let mut remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    let (h_blob, blob) = obj(b"hello blob");
    let (h_tree, tree) = obj(b"tree bytes");
    let (h_commit, commit) = obj(b"commit bytes");

    let seq = remote
        .push_commit(
            &[
                (h_blob, blob.clone()),
                (h_tree, tree.clone()),
                (h_commit, commit.clone()),
            ],
            "main",
            h_commit,
        )
        .await
        .unwrap();
    assert_eq!(seq, 1, "first push allocates txn_seq 1");

    assert_eq!(remote.get_object(h_blob).await.unwrap(), Some(blob));
    assert_eq!(remote.get_object(h_tree).await.unwrap(), Some(tree));
    assert_eq!(remote.get_object(h_commit).await.unwrap(), Some(commit));
    assert!(remote.has_object(h_blob).await.unwrap());
    assert_eq!(remote.get_tip("main").await.unwrap(), Some(h_commit));
}

/// A hash that was never pushed is absent; an unknown ref has no tip.
#[tokio::test]
async fn absent_object_and_ref_return_none() {
    let dir = TempDir::new().unwrap();
    let remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    let missing = ObjectHash::of_bytes(b"never pushed");
    assert_eq!(remote.get_object(missing).await.unwrap(), None);
    assert!(!remote.has_object(missing).await.unwrap());
    assert_eq!(remote.get_tip("main").await.unwrap(), None);
}

/// The tip ref advances in the SAME commit that adds the new objects: after
/// a second push the tip moves and the new object is present, while earlier
/// objects remain. Re-putting an already-present object hash is idempotent.
#[tokio::test]
async fn second_push_advances_tip_atomically() {
    let dir = TempDir::new().unwrap();
    let mut remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    let (h_c1, c1) = obj(b"commit-1");
    let (h_shared, shared) = obj(b"shared-blob");
    let s1 = remote
        .push_commit(&[(h_c1, c1), (h_shared, shared.clone())], "main", h_c1)
        .await
        .unwrap();

    let (h_c2, c2) = obj(b"commit-2");
    let (h_new, new) = obj(b"new-blob");
    // Re-include the shared blob; it must remain readable and not corrupt.
    let s2 = remote
        .push_commit(
            &[(h_c2, c2), (h_new, new.clone()), (h_shared, shared.clone())],
            "main",
            h_c2,
        )
        .await
        .unwrap();
    assert_eq!(s2, s1 + 1, "txn_seq is monotonic");

    assert_eq!(remote.get_tip("main").await.unwrap(), Some(h_c2));
    assert_eq!(remote.get_object(h_new).await.unwrap(), Some(new));
    assert_eq!(remote.get_object(h_shared).await.unwrap(), Some(shared));
    assert!(
        remote.has_object(h_c1).await.unwrap(),
        "old commit retained"
    );
}

/// Item 2 (`docs/logical-series-identity-design.md`): `push_objects` writes
/// new physical objects durably WITHOUT touching any ref. A caller that
/// stops right there (simulating a crash before the ref-advance step) must
/// see the old ref (or no ref) still intact and fully resolvable, while the
/// new objects are already durable for a later retry to find and skip
/// re-uploading.
#[tokio::test]
async fn push_objects_writes_durably_without_advancing_any_ref() {
    let dir = TempDir::new().unwrap();
    let mut remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    // Establish an initial tip the "old" ref must remain pointed at.
    let (h_c1, c1) = obj(b"commit-1");
    remote
        .push_commit(&[(h_c1, c1)], "main", h_c1)
        .await
        .unwrap();
    assert_eq!(remote.get_tip("main").await.unwrap(), Some(h_c1));

    // A second push's new objects land durably...
    let (h_c2, c2) = obj(b"commit-2");
    let (h_new, new) = obj(b"new-blob-for-commit-2");
    remote
        .push_objects(&[(h_c2, c2.clone()), (h_new, new.clone())])
        .await
        .unwrap();

    // ...but simulating a crash right here -- before any ref-advance --
    // must leave "main" exactly where it was: still resolvable, still
    // naming a fully present, fetchable closure (the old commit).
    assert_eq!(
        remote.get_tip("main").await.unwrap(),
        Some(h_c1),
        "ref must not move until advance_ref is called explicitly"
    );
    assert!(
        remote.has_object(h_c1).await.unwrap(),
        "old tip's object must remain reachable through the unmoved ref"
    );

    // The new objects are nonetheless already durable, so a retried push can
    // find and skip them rather than re-uploading.
    assert_eq!(remote.get_object(h_c2).await.unwrap(), Some(c2));
    assert_eq!(remote.get_object(h_new).await.unwrap(), Some(new));
}

/// Item 2: `advance_ref` alone moves only the ref, in its own commit, after
/// objects were separately made durable by `push_objects`. Exercises the
/// full split (objects, then ref) end to end and confirms the resulting
/// state is indistinguishable from an equivalent single `push_commit`.
#[tokio::test]
async fn advance_ref_moves_the_ref_after_objects_are_already_durable() {
    let dir = TempDir::new().unwrap();
    let mut remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    let (h_c1, c1) = obj(b"only-commit");
    let objects_seq = remote.push_objects(&[(h_c1, c1.clone())]).await.unwrap();
    assert_eq!(
        remote.get_tip("main").await.unwrap(),
        None,
        "no ref exists yet -- only objects were written"
    );

    let ref_seq = remote.advance_ref("main", h_c1).await.unwrap();
    assert!(
        ref_seq > objects_seq,
        "the ref-advance commit must be sequenced strictly after the object commit"
    );
    assert_eq!(remote.get_tip("main").await.unwrap(), Some(h_c1));
    assert_eq!(remote.get_object(h_c1).await.unwrap(), Some(c1));
}

#[tokio::test]
async fn objects_and_commit_index_are_visible_in_the_same_delta_version() {
    let dir = TempDir::new().unwrap();
    let mut remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();
    let (commit_hash, commit_bytes) = commit(None, 1);
    let (blob_hash, blob_bytes) = obj(b"blob");
    let before = remote.delta_version();

    remote
        .push_objects_with_commit_index(
            &[
                (commit_hash, commit_bytes.clone()),
                (blob_hash, blob_bytes.clone()),
            ],
            &[(commit_hash, commit_bytes.clone())],
        )
        .await
        .unwrap();

    let written_version = remote.delta_version();
    assert_eq!(
        written_version,
        before + 1,
        "objects and commit index require exactly one Delta commit"
    );
    assert_eq!(
        remote.get_object(blob_hash).await.unwrap(),
        Some(blob_bytes)
    );
    let index = remote.list_commit_index().await.unwrap();
    assert_eq!(index.len(), 1);
    assert_eq!(index.get(&commit_hash).unwrap().encode(), commit_bytes);

    let store = Store::open(dir.path()).await.unwrap();
    let (adds, _) = store.actions_at_version(written_version).await.unwrap();
    assert!(
        adds.iter()
            .any(|add| add.path.contains("partition_key=objects/")),
        "the transaction must add an objects-partition parquet"
    );
    assert!(
        adds.iter()
            .any(|add| add.path.contains("partition_key=commits/")),
        "the same transaction must add a commits-partition parquet"
    );
}

#[tokio::test]
async fn commit_index_requires_the_identical_ordinary_object() {
    let dir = TempDir::new().unwrap();
    let mut remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();
    let (commit_hash, commit_bytes) = commit(None, 1);
    let before = remote.delta_version();

    let error = remote
        .push_objects_with_commit_index(&[], &[(commit_hash, commit_bytes)])
        .await
        .expect_err("an index-only commit must be rejected");

    assert!(error.to_string().contains("has no matching object row"));
    assert_eq!(
        remote.delta_version(),
        before,
        "invalid input must not create a Delta transaction"
    );
    assert!(remote.list_commit_index().await.unwrap().is_empty());
}

#[tokio::test]
async fn commit_index_physically_appends_only_missing_hashes() {
    let dir = TempDir::new().unwrap();
    let mut remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();
    let first = commit(None, 1);
    let second = commit(Some(first.0), 2);
    let third = commit(Some(second.0), 3);
    let first_log = vec![first.clone(), second.clone()];
    let complete_log = vec![first, second, third];

    remote
        .push_objects_with_commit_index(&first_log, &first_log)
        .await
        .unwrap();
    let first_version = remote.delta_version();

    remote
        .push_objects_with_commit_index(&complete_log, &complete_log)
        .await
        .unwrap();
    let second_version = remote.delta_version();

    remote
        .push_objects_with_commit_index(&complete_log, &complete_log)
        .await
        .unwrap();
    let third_version = remote.delta_version();

    let store = Store::open(dir.path()).await.unwrap();
    assert_eq!(
        added_partition_rows(&store, first_version, "commits").await,
        (1, 2),
        "first upgraded push must physically backfill both commit rows"
    );
    assert_eq!(
        added_partition_rows(&store, second_version, "commits").await,
        (1, 1),
        "the next full-log push must physically append only the new commit"
    );
    assert_eq!(
        added_partition_rows(&store, third_version, "commits").await,
        (0, 0),
        "an unchanged full-log push must create no commits-partition Add"
    );
    assert_eq!(
        remote.list_commit_index().await.unwrap().len(),
        3,
        "the live map alone is insufficient proof, but must remain complete"
    );
}

#[tokio::test]
async fn commit_index_rejects_invalid_keys_hashes_and_codecs() {
    async fn read_error(dir: &TempDir) -> String {
        let remote = ContentRemote::open_at(dir.path(), pid()).await.unwrap();
        remote
            .list_commit_index()
            .await
            .expect_err("corrupt commit index must fail")
            .to_string()
    }

    let invalid_key_dir = TempDir::new().unwrap();
    let _ = ContentRemote::create_at(invalid_key_dir.path(), pid())
        .await
        .unwrap();
    let (_, valid_bytes) = commit(None, 1);
    let mut store = Store::open(invalid_key_dir.path()).await.unwrap();
    store
        .put(pid(), "commits", "not-a-hash", valid_bytes)
        .await
        .unwrap();
    drop(store);
    assert!(
        read_error(&invalid_key_dir)
            .await
            .contains("commit index key")
    );
    let mut remote = ContentRemote::open_at(invalid_key_dir.path(), pid())
        .await
        .unwrap();
    let before = remote.delta_version();
    let new_commit = commit(None, 2);
    let error = remote
        .push_objects_with_commit_index(
            std::slice::from_ref(&new_commit),
            std::slice::from_ref(&new_commit),
        )
        .await
        .expect_err("a push must authenticate the existing index before writing");
    assert!(error.to_string().contains("commit index key"));
    assert_eq!(
        remote.delta_version(),
        before,
        "corrupt existing index must prevent the whole write batch"
    );

    let wrong_hash_dir = TempDir::new().unwrap();
    let _ = ContentRemote::create_at(wrong_hash_dir.path(), pid())
        .await
        .unwrap();
    let (_, valid_bytes) = commit(None, 1);
    let mut store = Store::open(wrong_hash_dir.path()).await.unwrap();
    store
        .put(
            pid(),
            "commits",
            &ObjectHash::of_bytes(b"wrong").to_hex(),
            valid_bytes,
        )
        .await
        .unwrap();
    drop(store);
    assert!(read_error(&wrong_hash_dir).await.contains("has hash"));

    let invalid_codec_dir = TempDir::new().unwrap();
    let _ = ContentRemote::create_at(invalid_codec_dir.path(), pid())
        .await
        .unwrap();
    let invalid_bytes = b"not a commit".to_vec();
    let invalid_hash = ObjectHash::of_bytes(&invalid_bytes);
    let mut store = Store::open(invalid_codec_dir.path()).await.unwrap();
    store
        .put(pid(), "commits", &invalid_hash.to_hex(), invalid_bytes)
        .await
        .unwrap();
    drop(store);
    assert!(
        read_error(&invalid_codec_dir)
            .await
            .contains("decode commit index value")
    );
}

/// A large-file blob round-trips through the external blob store by hash: it
/// is absent before the put, present after, and reads back byte-for-byte.
#[tokio::test]
async fn put_blob_round_trips_by_hash() {
    use tokio::io::AsyncReadExt;

    let dir = TempDir::new().unwrap();
    let remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    let bytes = b"a large external blob's contents".to_vec();
    let hash = ObjectHash::of_bytes(&bytes);

    assert!(!remote.has_blob(hash).await.unwrap(), "absent before put");
    remote.put_blob(hash, &bytes[..]).await.unwrap();
    assert!(remote.has_blob(hash).await.unwrap(), "present after put");

    let mut reader = remote.get_blob_reader(hash).await.unwrap().unwrap();
    let mut read_back = Vec::new();
    reader.read_to_end(&mut read_back).await.unwrap();
    assert_eq!(read_back, bytes, "blob reads back byte-for-byte");
}

/// A blob spanning many multipart chunks (well past the 5MB part size and the
/// in-flight-part cap) round-trips byte-for-byte.  This exercises the streaming
/// upload path where `put_blob` applies backpressure via `wait_for_capacity`
/// across multiple part uploads, rather than the single-part small-blob case.
#[tokio::test]
async fn put_blob_round_trips_large_multipart() {
    use tokio::io::AsyncReadExt;

    let dir = TempDir::new().unwrap();
    let remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    // ~37MB: crosses several 5MB part boundaries and exceeds the 16-part
    // in-flight cap, so the capacity wait is actually taken.  Non-trivial byte
    // pattern so a torn or reordered part would fail the round-trip compare.
    let mut bytes = vec![0u8; 37 * 1024 * 1024 + 12345];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = (i.wrapping_mul(2_654_435_761) >> 13) as u8;
    }
    let hash = ObjectHash::of_bytes(&bytes);

    assert!(!remote.has_blob(hash).await.unwrap(), "absent before put");
    remote.put_blob(hash, &bytes[..]).await.unwrap();
    assert!(remote.has_blob(hash).await.unwrap(), "present after put");

    let mut reader = remote.get_blob_reader(hash).await.unwrap().unwrap();
    let mut read_back = Vec::new();
    reader.read_to_end(&mut read_back).await.unwrap();
    assert_eq!(read_back.len(), bytes.len(), "length preserved");
    assert_eq!(read_back, bytes, "large blob reads back byte-for-byte");
}

/// A blob whose bytes do not hash to the claimed key is rejected AND never
/// stored: `put_blob` verifies the content during its single streaming pass and
/// aborts the multipart upload before it becomes visible, so no value is ever
/// left under a key it does not equal. `has_blob` therefore stays false, and a
/// retry cannot be silently short-circuited by a phantom object.
#[tokio::test]
async fn put_blob_rejects_and_does_not_store_hash_mismatch() {
    let dir = TempDir::new().unwrap();
    let remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    let bytes = b"the real large-file blob contents";
    let claimed = ObjectHash::of_bytes(b"a completely different value");
    assert_ne!(
        claimed,
        ObjectHash::of_bytes(bytes),
        "claimed key must not be the bytes' true hash"
    );

    let err = remote
        .put_blob(claimed, &bytes[..])
        .await
        .expect_err("mismatched blob must be rejected");
    assert!(
        format!("{err}").contains("has hash") && format!("{err}").contains("expected hash"),
        "unexpected error: {err}"
    );

    assert!(
        !remote.has_blob(claimed).await.unwrap(),
        "nothing may be stored under the claimed (wrong) key"
    );
    assert!(
        remote.get_blob_reader(claimed).await.unwrap().is_none(),
        "no readable blob may exist under the claimed key"
    );
}

/// A remote survives close/reopen: objects and tip persist via Delta.
#[tokio::test]
async fn reopen_preserves_objects_and_tip() {
    let dir = TempDir::new().unwrap();
    let (h_commit, commit) = obj(b"durable commit");
    {
        let mut remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();
        remote
            .push_commit(&[(h_commit, commit.clone())], "main", h_commit)
            .await
            .unwrap();
    }
    let remote = ContentRemote::open_at(dir.path(), pid()).await.unwrap();
    assert_eq!(remote.get_object(h_commit).await.unwrap(), Some(commit));
    assert_eq!(remote.get_tip("main").await.unwrap(), Some(h_commit));
}
