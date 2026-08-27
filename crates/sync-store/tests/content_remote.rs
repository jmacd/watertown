// SPDX-License-Identifier: Apache-2.0

//! Integration tests for [`ContentRemote`]: the delta-managed
//! content-addressed remote (design Section 8, Decision D6).

use sync_store::ContentRemote;
use sync_store::content::ObjectHash;
use tempfile::TempDir;
use uuid::Uuid;

fn pid() -> Uuid {
    Uuid::from_u128(0xc0_0000_0000_0000_0000_0000_0000_0000)
}

fn obj(bytes: &[u8]) -> (ObjectHash, Vec<u8>) {
    (ObjectHash::of_bytes(bytes), bytes.to_vec())
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
