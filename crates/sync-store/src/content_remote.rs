// SPDX-License-Identifier: Apache-2.0

//! [`ContentRemote`]: native-v2 immutable payloads plus bounded transactional
//! publication state.
//!
//! Canonical content lives at raw object-store keys beneath `_content/v2/`.
//! Every payload uses conditional create and an authenticated immutable
//! receipt. Per-push publication records name only that push's additions.
//! Series packs are content-addressed linked suffix segments reached through
//! fixed-key per-series locators; explicit consolidated locators may select a
//! whole-range pack without rewriting old segments or listing the pack prefix.
//! `_publication/` is a separate Delta table containing one active row per
//! `(pond_id, ref_name)`; its generation/record-head CAS is the final
//! visibility operation. The generic [`Store`] retained at the remote root is
//! metadata/capsule infrastructure only and is not an ordinary payload, commit,
//! or ref representation.

use std::collections::HashMap;
use std::path::Path;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use object_store::{ObjectMeta, PutMode, PutOptions, PutResult};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::content::{
    CapsuleManifest, ObjectDescriptor, ObjectHash, ObjectReceipt, PackDescriptor,
    PublicationRecord, capsule_manifest_bytes, capsule_root, decode_capsule_manifest,
    verify_capsule_payload_directory, verify_capsule_payloads,
    verify_incremental_capsule_payload_directory,
};
use crate::error::{Result, StoreError};
use crate::publication::{PublicationExpectation, PublicationState, PublicationTable};
use crate::store::Store;

/// Partition holding remote metadata; the source pond_id is stored here under
/// the nil pond partition so a consumer can discover it without knowing it.
const META_PARTITION: &str = "meta";
const POND_ID_KEY: &str = "pond_id";
const V2_OBJECT_PREFIX: &str = "_content/v2/objects";
const V2_RECEIPT_PREFIX: &str = "_content/v2/receipts";
const V2_PUBLICATION_PREFIX: &str = "_content/v2/publications";
const V2_PACK_PREFIX: &str = "_content/v2/packs";
const V2_UPLOAD_PREFIX: &str = "_content/v2/fallback/uploads";
const CAPSULE_PREFIX: &str = "recovery";
const CAPSULE_HISTORY_LIMIT: usize = 3;
const RECIPE_NATIVE_FORMAT: &str = "watertown.commit.v1";
const CAPSULE_README: &str = include_str!("../recovery/watertown.commit.v1/CAPSULE-README.md");
const CAPSULE_FORMAT: &str = include_str!("../recovery/watertown.commit.v1/CAPSULE-FORMAT.md");
const CAPSULE_TOOL: &str = include_str!("../recovery/watertown.commit.v1/capsule.py");
const CAPSULE_PARQUET_SCHEMA: &str =
    include_str!("../recovery/watertown.commit.v1/parquet_schema.py");
const CAPSULE_REQUIREMENTS: &str =
    include_str!("../recovery/watertown.commit.v1/capsule-requirements.lock");
const CAPSULE_RECOVER: &str = include_str!("../recovery/watertown.commit.v1/recover.sh");

/// Result of publishing one verified recovery-capsule generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapsulePublishOutcome {
    /// Capsule manifest root and generation identifier.
    pub root: ObjectHash,
    /// Number of previously absent payload objects uploaded.
    pub payloads_uploaded: usize,
    /// Total distinct payload objects referenced by the generation.
    pub payloads_total: usize,
}

/// Result of installing the static recovery recipe for the current native format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryRecipePublishOutcome {
    /// Domain-separated identity of the exact bootstrap bytes.
    pub recipe_hash: ObjectHash,
    /// Whether this call created the immutable versioned recipe object.
    pub versioned_created: bool,
    /// Whether this call created the discoverable top-level bootstrap.
    pub discoverable_created: bool,
}

/// Physical effect of one immutable native-v2 create attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImmutableWriteOutcome {
    /// Whether this call established the canonical payload key.
    pub payload_created: bool,
    /// Whether this call established the canonical receipt.
    pub receipt_created: bool,
    /// Payload bytes physically created by this call.
    pub payload_bytes_created: u64,
}

/// Physical effect of explicitly publishing one consolidated series pack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsolidatedPackWriteOutcome {
    /// Content address of the immutable pack index.
    pub pack_hash: ObjectHash,
    /// Whether this call created the immutable pack bytes.
    pub pack_created: bool,
    /// Whether this call installed the fixed-key consolidated locator.
    pub locator_created: bool,
}

/// Physical effect of one explicit stale-upload cleanup.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UploadCleanupOutcome {
    /// Staging objects deleted.
    pub objects_deleted: usize,
    /// Staging bytes deleted.
    pub bytes_deleted: u64,
}

/// Deterministic one-shot publication failure used by retry/atomicity tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationFailurePoint {
    /// Before the first payload create.
    BeforeObjects,
    /// After this many payload objects have been processed.
    AfterObject(usize),
    /// After every payload object, before packs.
    AfterObjects,
    /// After packs, before the immutable publication record.
    AfterPacks,
    /// After the publication record, before the active-row CAS.
    AfterRecord,
    /// Immediately after the active-row CAS became visible.
    AfterRef,
    /// After a consolidated pack is durable, before its locator is installed.
    BeforeConsolidatedLocator,
}

#[derive(Clone, PartialEq, Eq)]
enum CapsuleRefVersion {
    Missing,
    Present { root: ObjectHash },
}

const CAPSULE_PUBLISH_LOCK_STALE_MICROS: i64 = 15 * 60 * 1_000_000;
const CAPSULE_GC_PLAN_FORMAT: &str = "pondcapsule.gc-plan.1";
const CAPSULE_GC_PLAN_DOMAIN: &[u8] = b"pondcapsule.gc-plan-root.1\n";

/// One exact remote object selected by a capsule garbage-collection plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapsuleGcObject {
    /// Full object-store key beneath the attached remote.
    pub key: String,
    /// Size observed when the plan was created.
    pub size: u64,
}

/// Immutable reviewed plan for deleting unreachable capsule generations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapsuleGcPlan {
    /// Plan wire-format identifier.
    pub format: String,
    /// Exact attached remote URL.
    pub remote_url: String,
    /// Native remote pond identity.
    pub remote_pond_id: String,
    /// Current capsule root at snapshot time.
    pub latest_root: String,
    /// Retained roots, newest first.
    pub retained_roots: Vec<String>,
    /// Plan creation time.
    pub created_at_micros: i64,
    /// Earliest permitted apply time.
    pub not_before_micros: i64,
    /// Exact objects to delete, sorted by key.
    pub deletions: Vec<CapsuleGcObject>,
}

impl CapsuleGcPlan {
    /// Validate the plan's canonical invariants.
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.format != CAPSULE_GC_PLAN_FORMAT {
            return Err(format!(
                "unsupported capsule GC plan format {:?}",
                self.format
            ));
        }
        if self.remote_url.is_empty() || self.remote_pond_id.is_empty() {
            return Err("capsule GC plan remote identity is empty".to_string());
        }
        if self.retained_roots.first() != Some(&self.latest_root) {
            return Err("capsule GC plan latest root is not first in retention".to_string());
        }
        for root in &self.retained_roots {
            if root.bytes().any(|byte| byte.is_ascii_uppercase()) {
                return Err("capsule GC plan root is not lowercase hexadecimal".to_string());
            }
            let _ = ObjectHash::from_hex(root)?;
        }
        if self.not_before_micros < self.created_at_micros {
            return Err("capsule GC plan not-before precedes creation".to_string());
        }
        let mut prior = None;
        for deletion in &self.deletions {
            validate_capsule_gc_key(&deletion.key)?;
            if prior.is_some_and(|key: &str| key >= deletion.key.as_str()) {
                return Err("capsule GC plan deletions are not uniquely sorted".to_string());
            }
            prior = Some(deletion.key.as_str());
        }
        Ok(())
    }
}

/// Encode a validated capsule GC plan as canonical JSON.
pub fn capsule_gc_plan_bytes(plan: &CapsuleGcPlan) -> std::result::Result<Vec<u8>, String> {
    plan.validate()?;
    serde_json::to_vec(plan).map_err(|error| format!("encode capsule GC plan: {error}"))
}

/// Decode a canonical capsule GC plan.
pub fn decode_capsule_gc_plan(bytes: &[u8]) -> std::result::Result<CapsuleGcPlan, String> {
    let plan: CapsuleGcPlan = serde_json::from_slice(bytes)
        .map_err(|error| format!("decode capsule GC plan: {error}"))?;
    let canonical = capsule_gc_plan_bytes(&plan)?;
    if canonical != bytes {
        return Err("capsule GC plan is not canonically encoded".to_string());
    }
    Ok(plan)
}

/// Compute the reviewed plan hash.
pub fn capsule_gc_plan_hash(plan: &CapsuleGcPlan) -> std::result::Result<ObjectHash, String> {
    let bytes = capsule_gc_plan_bytes(plan)?;
    let mut hasher = blake3::Hasher::new();
    let _ = hasher.update(CAPSULE_GC_PLAN_DOMAIN);
    let _ = hasher.update(&bytes);
    Ok(ObjectHash::from_bytes(*hasher.finalize().as_bytes()))
}

/// The delta-managed content-addressed remote for one source pond.
///
/// All rows are written under the source pond's `pond_id`, matching the
/// store's per-`pond_id` physical partitioning.  Object hashes are
/// content-only and lineage-independent, so two ponds with identical content
/// produce identical object bytes under identical keys.  A node's content
/// includes the metadata its directory entry commits to, so a pond and its
/// replica share keys while two independently written ponds share only their
/// blobs.
pub struct ContentRemote {
    store: Store,
    publication: PublicationTable,
    pond_id: Uuid,
    failure_point: Option<PublicationFailurePoint>,
}

impl ContentRemote {
    /// Multipart providers require non-final parts of at least 5 MiB.
    const MULTIPART_PART_SIZE: usize = 5 * 1024 * 1024;
    /// Maximum in-flight multipart part uploads allowed while streaming a large
    /// blob to the remote (see [`Self::put_blob`]).  Bounds staged upload memory
    /// to this many [`Self::MULTIPART_PART_SIZE`] parts when the reader outpaces
    /// the network.
    const MAX_INFLIGHT_UPLOAD_PARTS: usize = 16;

    /// Create a fresh remote at `path`.  Errors if a Delta table already
    /// exists there.
    pub async fn create_at(path: impl AsRef<Path>, pond_id: Uuid) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let store = Store::create(&path).await?;
        let publication = PublicationTable::create(&path).await?;
        let mut me = Self {
            store,
            publication,
            pond_id,
            failure_point: None,
        };
        me.write_pond_id().await?;
        Ok(me)
    }

    /// Open an existing remote at `path`.
    pub async fn open_at(path: impl AsRef<Path>, pond_id: Uuid) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let store = Store::open(&path).await?;
        let publication = PublicationTable::open(&path).await?;
        Ok(Self {
            store,
            publication,
            pond_id,
            failure_point: None,
        })
    }

    /// Create a fresh remote at `url` with `storage_options` (e.g. S3 creds),
    /// recording `pond_id`.  Errors if a table already exists.
    pub async fn create_at_url(
        url: &str,
        pond_id: Uuid,
        storage_options: std::collections::HashMap<String, String>,
    ) -> Result<Self> {
        let store = Store::create_at_url(url, storage_options.clone()).await?;
        let publication = PublicationTable::create_at_url(url, storage_options).await?;
        let mut me = Self {
            store,
            publication,
            pond_id,
            failure_point: None,
        };
        me.write_pond_id().await?;
        Ok(me)
    }

    /// Open an existing remote at `url`, discovering its source pond_id from
    /// the recorded metadata.
    pub async fn open_at_url(
        url: &str,
        storage_options: std::collections::HashMap<String, String>,
    ) -> Result<Self> {
        let store = Store::open_at_url(url, storage_options.clone()).await?;
        let publication = PublicationTable::open_at_url(url, storage_options).await?;
        let bytes = store
            .get(Uuid::nil(), META_PARTITION, POND_ID_KEY)
            .await?
            .ok_or_else(|| StoreError::Invariant("remote has no recorded pond_id".to_string()))?;
        let s = String::from_utf8(bytes)
            .map_err(|e| StoreError::Invariant(format!("pond_id not utf8: {e}")))?;
        let pond_id =
            Uuid::parse_str(&s).map_err(|e| StoreError::Invariant(format!("bad pond_id: {e}")))?;
        Ok(Self {
            store,
            publication,
            pond_id,
            failure_point: None,
        })
    }

    async fn write_pond_id(&mut self) -> Result<()> {
        let _ = self
            .store
            .put(
                Uuid::nil(),
                META_PARTITION,
                POND_ID_KEY,
                self.pond_id.to_string().into_bytes(),
            )
            .await?;
        Ok(())
    }

    /// The URL this remote lives at, which is the identity its storage budget
    /// is bound to.
    pub fn url(&self) -> String {
        self.store.url()
    }

    /// The pond whose objects this remote holds.
    pub fn pond_id(&self) -> Uuid {
        self.pond_id
    }

    /// Install a one-shot deterministic failure for publication tests.
    pub fn inject_publication_failure(&mut self, point: PublicationFailurePoint) {
        self.failure_point = Some(point);
    }

    /// Trigger and clear a matching one-shot publication failure.
    pub fn publication_stage(&mut self, point: PublicationFailurePoint) -> Result<()> {
        if self.failure_point == Some(point) {
            self.failure_point = None;
            return Err(StoreError::Invariant(format!(
                "injected publication failure at {point:?}"
            )));
        }
        Ok(())
    }

    fn v2_object_path(hash: ObjectHash) -> object_store::path::Path {
        object_store::path::Path::from(format!("{V2_OBJECT_PREFIX}/blake3={}", hash.to_hex()))
    }

    fn v2_receipt_path(hash: ObjectHash) -> object_store::path::Path {
        object_store::path::Path::from(format!("{V2_RECEIPT_PREFIX}/blake3={}", hash.to_hex()))
    }

    fn v2_publication_path(hash: ObjectHash) -> object_store::path::Path {
        object_store::path::Path::from(format!("{V2_PUBLICATION_PREFIX}/blake3={}", hash.to_hex()))
    }

    fn v2_pack_path(hash: ObjectHash) -> object_store::path::Path {
        object_store::path::Path::from(format!("{V2_PACK_PREFIX}/blake3={}", hash.to_hex()))
    }

    fn v2_series_pack_path(series_hash: ObjectHash) -> object_store::path::Path {
        object_store::path::Path::from(format!(
            "{V2_PACK_PREFIX}/by-series/blake3={}",
            series_hash.to_hex()
        ))
    }

    fn v2_consolidated_series_pack_path(series_hash: ObjectHash) -> object_store::path::Path {
        object_store::path::Path::from(format!(
            "{V2_PACK_PREFIX}/consolidated/by-series/blake3={}",
            series_hash.to_hex()
        ))
    }

    /// Read the fixed-size current publication row for one ref.
    pub async fn current_publication(&self, ref_name: &str) -> Result<Option<PublicationState>> {
        self.publication.get(self.pond_id, ref_name).await
    }

    /// Atomically replace exactly one active publication row.
    pub async fn compare_and_swap_publication(
        &mut self,
        expectation: PublicationExpectation,
        next: PublicationState,
    ) -> Result<PublicationState> {
        if next.pond_id != self.pond_id {
            return Err(StoreError::Invariant(format!(
                "publication pond {} does not match remote pond {}",
                next.pond_id, self.pond_id
            )));
        }
        self.publication.compare_and_swap(expectation, next).await
    }

    /// Current dedicated publication-table version.
    #[must_use]
    pub fn publication_version(&self) -> i64 {
        self.publication.version()
    }

    /// Active Parquet files in the dedicated publication table.
    pub fn publication_active_file_count(&self) -> Result<usize> {
        self.publication.active_file_count()
    }

    /// Write one canonical immutable payload and receipt.
    ///
    /// The payload bytes are verified against `descriptor.hash` before any
    /// request. `PutMode::Create` establishes the sole canonical key. A retry
    /// with a valid receipt reads only that small receipt plus payload metadata.
    /// If a prior create was interrupted before its receipt, the payload is
    /// downloaded and hashed once before the receipt is established.
    pub async fn put_immutable_object(
        &self,
        descriptor: ObjectDescriptor,
        bytes: &[u8],
    ) -> Result<ImmutableWriteOutcome> {
        let actual = ObjectHash::of_bytes(bytes);
        if actual != descriptor.hash {
            return Err(StoreError::Invariant(format!(
                "refusing {} object {} whose bytes hash to {}",
                descriptor.kind.as_str(),
                descriptor.hash,
                actual
            )));
        }
        if let Some(receipt) = self.read_receipt(descriptor.hash).await? {
            self.verify_receipt_identity(descriptor, bytes.len() as u64, &receipt)
                .await?;
            return Ok(ImmutableWriteOutcome {
                payload_created: false,
                receipt_created: false,
                payload_bytes_created: 0,
            });
        }
        let path = Self::v2_object_path(descriptor.hash);
        match self
            .store
            .object_store()
            .put_opts(
                &path,
                bytes.to_vec().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
        {
            Ok(result) => {
                crate::metered_store::record_physical_create(
                    &crate::RemoteKey::new(&self.store.url()),
                    crate::AccessClass::ContentObjects,
                    bytes.len() as u64,
                );
                let receipt = receipt_from_put(descriptor, bytes.len() as u64, &result)?;
                let receipt_created = self.create_receipt(&receipt).await?;
                Ok(ImmutableWriteOutcome {
                    payload_created: true,
                    receipt_created,
                    payload_bytes_created: bytes.len() as u64,
                })
            }
            Err(object_store::Error::AlreadyExists { .. }) => {
                let receipt = self.read_receipt(descriptor.hash).await?;
                let receipt_created = match receipt {
                    Some(receipt) => {
                        self.verify_receipt_identity(descriptor, bytes.len() as u64, &receipt)
                            .await?;
                        false
                    }
                    None => {
                        let result =
                            self.store
                                .object_store()
                                .get(&path)
                                .await
                                .map_err(|error| {
                                    StoreError::Invariant(format!(
                                        "read receipt-less object {}: {error}",
                                        descriptor.hash
                                    ))
                                })?;
                        let metadata = result.meta.clone();
                        let existing = result.bytes().await.map_err(|error| {
                            StoreError::Invariant(format!(
                                "collect receipt-less object {}: {error}",
                                descriptor.hash
                            ))
                        })?;
                        let existing_hash = ObjectHash::of_bytes(&existing);
                        if existing_hash != descriptor.hash
                            || existing.len() != bytes.len()
                            || existing.as_ref() != bytes
                        {
                            return Err(StoreError::Invariant(format!(
                                "receipt-less object {} has conflicting bytes",
                                descriptor.hash
                            )));
                        }
                        let receipt =
                            receipt_from_meta(descriptor, existing.len() as u64, &metadata)?;
                        self.create_receipt(&receipt).await?
                    }
                };
                Ok(ImmutableWriteOutcome {
                    payload_created: false,
                    receipt_created,
                    payload_bytes_created: 0,
                })
            }
            Err(error) => Err(StoreError::Invariant(format!(
                "create immutable {} object {}: {error}",
                descriptor.kind.as_str(),
                descriptor.hash
            ))),
        }
    }

    /// Stream a potentially large canonical payload without buffering it.
    ///
    /// A valid receipt short-circuits before the reader is consumed. For a new
    /// payload, bytes are hash-verified in a unique staging object and promoted
    /// with atomic `copy_if_not_exists`; the staging key is deleted before the
    /// method returns. The canonical key therefore still has create-once
    /// semantics even on backends whose multipart API has no `PutMode`.
    pub async fn put_immutable_object_stream<R>(
        &self,
        descriptor: ObjectDescriptor,
        reader: R,
    ) -> Result<ImmutableWriteOutcome>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        if let Some(receipt) = self.read_receipt(descriptor.hash).await? {
            self.verify_receipt_identity(descriptor, receipt.byte_length, &receipt)
                .await?;
            return Ok(ImmutableWriteOutcome {
                payload_created: false,
                receipt_created: false,
                payload_bytes_created: 0,
            });
        }

        let canonical = Self::v2_object_path(descriptor.hash);
        match self.store.object_store().head(&canonical).await {
            Ok(_) => {
                let receipt = self
                    .verify_receiptless_object(descriptor, &canonical)
                    .await?;
                let created = self.create_receipt(&receipt).await?;
                return Ok(ImmutableWriteOutcome {
                    payload_created: false,
                    receipt_created: created,
                    payload_bytes_created: 0,
                });
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => {
                return Err(StoreError::Invariant(format!(
                    "head immutable object {}: {error}",
                    descriptor.hash
                )));
            }
        }

        let staging =
            object_store::path::Path::from(format!("{V2_UPLOAD_PREFIX}/{}", uuid::Uuid::new_v4()));
        self.put_hashed_object(staging.clone(), descriptor.hash, None, reader)
            .await?;
        let staged_meta = match self.store.object_store().head(&staging).await {
            Ok(metadata) => metadata,
            Err(error) => {
                let cleanup = self.store.object_store().delete(&staging).await;
                return Err(StoreError::Invariant(match cleanup {
                    Ok(()) => format!("head staged immutable object {}: {error}", descriptor.hash),
                    Err(cleanup_error) => format!(
                        "head staged immutable object {}: {error}; staging cleanup failed: \
                         {cleanup_error}",
                        descriptor.hash
                    ),
                }));
            }
        };

        let copied = match self
            .store
            .object_store()
            .copy_if_not_exists(&staging, &canonical)
            .await
        {
            Ok(()) => true,
            Err(object_store::Error::AlreadyExists { .. }) => false,
            Err(error) => {
                let cleanup = self.store.object_store().delete(&staging).await;
                return Err(StoreError::Invariant(match cleanup {
                    Ok(()) => format!(
                        "promote immutable object {} with copy-if-absent: {error}",
                        descriptor.hash
                    ),
                    Err(cleanup_error) => format!(
                        "promote immutable object {}: {error}; staging cleanup failed: \
                         {cleanup_error}",
                        descriptor.hash
                    ),
                }));
            }
        };
        self.store
            .object_store()
            .delete(&staging)
            .await
            .map_err(|error| {
                StoreError::Invariant(format!(
                    "delete staged immutable object {}: {error}",
                    descriptor.hash
                ))
            })?;

        let receipt = if copied {
            crate::metered_store::record_physical_create(
                &crate::RemoteKey::new(&self.store.url()),
                crate::AccessClass::ContentObjects,
                staged_meta.size as u64,
            );
            let metadata = self
                .store
                .object_store()
                .head(&canonical)
                .await
                .map_err(|error| {
                    StoreError::Invariant(format!(
                        "head promoted immutable object {}: {error}",
                        descriptor.hash
                    ))
                })?;
            receipt_from_meta(descriptor, metadata.size as u64, &metadata)?
        } else {
            self.verify_receiptless_object(descriptor, &canonical)
                .await?
        };
        let receipt_created = self.create_receipt(&receipt).await?;
        Ok(ImmutableWriteOutcome {
            payload_created: copied,
            receipt_created,
            payload_bytes_created: if copied { receipt.byte_length } else { 0 },
        })
    }

    /// Delete upload-staging objects older than `older_than`.
    ///
    /// These keys are never publication roots. They can survive only when a
    /// process exits after multipart upload and before normal cleanup. The
    /// age boundary protects uploads that may still be in flight.
    pub async fn cleanup_stale_uploads(
        &self,
        older_than: chrono::DateTime<chrono::Utc>,
    ) -> Result<UploadCleanupOutcome> {
        let prefix = object_store::path::Path::from(V2_UPLOAD_PREFIX);
        let mut listed = self.store.object_store().list(Some(&prefix));
        let mut stale = Vec::new();
        while let Some(metadata) = listed.next().await {
            let metadata = metadata.map_err(|error| {
                StoreError::Invariant(format!("list immutable upload staging: {error}"))
            })?;
            if metadata.last_modified <= older_than {
                stale.push(metadata);
            }
        }

        let mut outcome = UploadCleanupOutcome::default();
        for metadata in stale {
            self.store
                .object_store()
                .delete(&metadata.location)
                .await
                .map_err(|error| {
                    StoreError::Invariant(format!(
                        "delete stale immutable upload staging {}: {error}",
                        metadata.location
                    ))
                })?;
            outcome.objects_deleted += 1;
            outcome.bytes_deleted = outcome.bytes_deleted.saturating_add(metadata.size);
        }
        Ok(outcome)
    }

    /// Read and hash-verify one native-v2 immutable payload.
    pub async fn get_immutable_object(&self, hash: ObjectHash) -> Result<Option<Vec<u8>>> {
        let result = match self
            .store
            .object_store()
            .get(&Self::v2_object_path(hash))
            .await
        {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => {
                return Err(StoreError::Invariant(format!(
                    "get immutable object {hash}: {error}"
                )));
            }
        };
        let bytes = result.bytes().await.map_err(|error| {
            StoreError::Invariant(format!("collect immutable object {hash}: {error}"))
        })?;
        let actual = ObjectHash::of_bytes(&bytes);
        if actual != hash {
            return Err(StoreError::Invariant(format!(
                "immutable object {hash} hashes to {actual}"
            )));
        }
        Ok(Some(bytes.to_vec()))
    }

    /// Receipt-authenticated payload length, or `None` if neither payload nor
    /// receipt exists.
    pub async fn immutable_object_size(&self, hash: ObjectHash) -> Result<Option<u64>> {
        match self.read_receipt(hash).await? {
            Some(receipt) => {
                let metadata = self
                    .store
                    .object_store()
                    .head(&Self::v2_object_path(hash))
                    .await
                    .map_err(|error| {
                        StoreError::Invariant(format!("head receipt-backed object {hash}: {error}"))
                    })?;
                verify_receipt_metadata(&receipt, &metadata)?;
                Ok(Some(receipt.byte_length))
            }
            None => match self
                .store
                .object_store()
                .head(&Self::v2_object_path(hash))
                .await
            {
                Ok(_) => Err(StoreError::Invariant(format!(
                    "immutable object {hash} is visible without a receipt"
                ))),
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(error) => Err(StoreError::Invariant(format!(
                    "head immutable object {hash}: {error}"
                ))),
            },
        }
    }

    /// Open a streaming reader over one native-v2 immutable payload.
    pub async fn get_immutable_object_reader(
        &self,
        hash: ObjectHash,
    ) -> Result<Option<Box<dyn tokio::io::AsyncRead + Unpin + Send>>> {
        let result = match self
            .store
            .object_store()
            .get(&Self::v2_object_path(hash))
            .await
        {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => {
                return Err(StoreError::Invariant(format!(
                    "get immutable object stream {hash}: {error}"
                )));
            }
        };
        let stream = futures::TryStreamExt::map_err(result.into_stream(), std::io::Error::other);
        Ok(Some(Box::new(tokio_util::io::StreamReader::new(stream))))
    }

    /// Publish a canonical immutable pack index.
    pub async fn put_immutable_pack(
        &self,
        descriptor: PackDescriptor,
        bytes: &[u8],
    ) -> Result<bool> {
        let (created, pack) = self.store_immutable_pack(descriptor, bytes).await?;
        if pack.validate_series_segment().is_ok() {
            self.ensure_series_pack_locator(descriptor).await?;
        }
        Ok(created)
    }

    async fn store_immutable_pack(
        &self,
        descriptor: PackDescriptor,
        bytes: &[u8],
    ) -> Result<(bool, crate::content::PackIndex)> {
        let actual = ObjectHash::of_bytes(bytes);
        if actual != descriptor.pack_hash {
            return Err(StoreError::Invariant(format!(
                "pack bytes hash to {actual}, expected {}",
                descriptor.pack_hash
            )));
        }
        let pack = crate::content::PackIndex::decode(bytes)
            .map_err(|error| StoreError::Invariant(format!("decode pack: {error}")))?;
        if pack.series_hash() != descriptor.series_hash {
            return Err(StoreError::Invariant(format!(
                "pack {} names series {}, expected {}",
                descriptor.pack_hash,
                pack.series_hash(),
                descriptor.series_hash
            )));
        }
        let created = self
            .create_content_addressed_small(
                &Self::v2_pack_path(descriptor.pack_hash),
                descriptor.pack_hash,
                bytes,
                crate::AccessClass::ContentPacks,
                "pack",
            )
            .await?;
        Ok((created, pack))
    }

    /// Read the immutable ordinary append-segment locator for one series.
    ///
    /// This fixed-size point read intentionally ignores an optional
    /// consolidated locator so an incremental consumer can continue to fetch
    /// only its publication-window suffix.
    pub async fn canonical_pack_for_series(
        &self,
        series_hash: ObjectHash,
    ) -> Result<Option<PackDescriptor>> {
        self.read_series_pack_locator(
            series_hash,
            &Self::v2_series_pack_path(series_hash),
            "canonical",
        )
        .await
    }

    /// Read the optional full-range consolidated locator for one series.
    ///
    /// Fresh clones prefer this fixed-size point lookup when present. An
    /// incremental consumer deliberately uses [`Self::canonical_pack_for_series`]
    /// instead so maintenance cannot turn a suffix fetch back into a
    /// whole-series metadata read.
    pub async fn consolidated_pack_for_series(
        &self,
        series_hash: ObjectHash,
    ) -> Result<Option<PackDescriptor>> {
        self.read_series_pack_locator(
            series_hash,
            &Self::v2_consolidated_series_pack_path(series_hash),
            "consolidated",
        )
        .await
    }

    async fn read_series_pack_locator(
        &self,
        series_hash: ObjectHash,
        path: &object_store::path::Path,
        kind: &str,
    ) -> Result<Option<PackDescriptor>> {
        let bytes = match self.store.object_store().get(path).await {
            Ok(result) => result
                .bytes()
                .await
                .map_err(|error| {
                    StoreError::Invariant(format!(
                        "read {kind} pack locator for series {series_hash}: {error}"
                    ))
                })?
                .to_vec(),
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => {
                return Err(StoreError::Invariant(format!(
                    "get {kind} pack locator for series {series_hash}: {error}"
                )));
            }
        };
        let raw: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
            StoreError::Invariant(format!(
                "{kind} pack locator for series {series_hash} is {} bytes, expected 32",
                bytes.len()
            ))
        })?;
        Ok(Some(PackDescriptor::new(
            series_hash,
            ObjectHash::from_bytes(raw),
        )))
    }

    async fn ensure_series_pack_locator(&self, descriptor: PackDescriptor) -> Result<()> {
        let path = Self::v2_series_pack_path(descriptor.series_hash);
        match self
            .store
            .object_store()
            .put_opts(
                &path,
                descriptor.pack_hash.as_bytes().to_vec().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = self
                    .canonical_pack_for_series(descriptor.series_hash)
                    .await?
                    .ok_or_else(|| {
                        StoreError::Invariant(format!(
                            "canonical pack locator for series {} disappeared",
                            descriptor.series_hash
                        ))
                    })?;
                if self.get_immutable_pack(existing).await?.is_none() {
                    return Err(StoreError::Invariant(format!(
                        "canonical pack locator for series {} names missing pack {}",
                        descriptor.series_hash, existing.pack_hash
                    )));
                }
                if existing.pack_hash != descriptor.pack_hash {
                    log::debug!(
                        "series {} already has verified canonical pack {}; retaining it instead of \
                         alternative segmentation {}",
                        descriptor.series_hash,
                        existing.pack_hash,
                        descriptor.pack_hash
                    );
                }
                Ok(())
            }
            Err(error) => Err(StoreError::Invariant(format!(
                "create canonical pack locator for series {}: {error}",
                descriptor.series_hash
            ))),
        }
    }

    async fn ensure_consolidated_series_pack_locator(
        &self,
        descriptor: PackDescriptor,
    ) -> Result<bool> {
        let path = Self::v2_consolidated_series_pack_path(descriptor.series_hash);
        match self
            .store
            .object_store()
            .put_opts(
                &path,
                descriptor.pack_hash.as_bytes().to_vec().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = self
                    .read_series_pack_locator(descriptor.series_hash, &path, "consolidated")
                    .await?
                    .ok_or_else(|| {
                        StoreError::Invariant(format!(
                            "consolidated pack locator for series {} disappeared",
                            descriptor.series_hash
                        ))
                    })?;
                if self.get_immutable_pack(existing).await?.is_none() {
                    return Err(StoreError::Invariant(format!(
                        "consolidated pack locator for series {} names missing pack {}",
                        descriptor.series_hash, existing.pack_hash
                    )));
                }
                if existing.pack_hash != descriptor.pack_hash {
                    return Err(StoreError::Invariant(format!(
                        "series {} already has consolidated pack {}, refusing conflicting pack {}",
                        descriptor.series_hash, existing.pack_hash, descriptor.pack_hash
                    )));
                }
                Ok(false)
            }
            Err(error) => Err(StoreError::Invariant(format!(
                "create consolidated pack locator for series {}: {error}",
                descriptor.series_hash
            ))),
        }
    }

    /// Fetch and verify one native-v2 immutable pack index.
    pub async fn get_immutable_pack(&self, descriptor: PackDescriptor) -> Result<Option<Vec<u8>>> {
        let bytes = self
            .read_content_addressed_small(
                &Self::v2_pack_path(descriptor.pack_hash),
                descriptor.pack_hash,
                "pack",
            )
            .await?;
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let pack = crate::content::PackIndex::decode(&bytes)
            .map_err(|error| StoreError::Invariant(format!("decode pack: {error}")))?;
        if pack.series_hash() != descriptor.series_hash {
            return Err(StoreError::Invariant(format!(
                "pack {} names series {}, expected {}",
                descriptor.pack_hash,
                pack.series_hash(),
                descriptor.series_hash
            )));
        }
        Ok(Some(bytes))
    }

    /// Publish one canonical immutable per-push record.
    pub async fn put_publication_record(&self, record: &PublicationRecord) -> Result<bool> {
        if record.pond_id != self.pond_id {
            return Err(StoreError::Invariant(format!(
                "publication record pond {} does not match remote pond {}",
                record.pond_id, self.pond_id
            )));
        }
        let bytes = record.encode();
        let hash = record.hash();
        self.create_content_addressed_small(
            &Self::v2_publication_path(hash),
            hash,
            &bytes,
            crate::AccessClass::PublicationRecords,
            "publication record",
        )
        .await
    }

    /// Fetch and strictly decode one immutable publication record.
    pub async fn get_publication_record(
        &self,
        hash: ObjectHash,
    ) -> Result<Option<PublicationRecord>> {
        let bytes = self
            .read_content_addressed_small(
                &Self::v2_publication_path(hash),
                hash,
                "publication record",
            )
            .await?;
        bytes
            .map(|bytes| {
                PublicationRecord::decode(&bytes).map_err(|error| {
                    StoreError::Invariant(format!("decode publication record {hash}: {error}"))
                })
            })
            .transpose()
    }

    /// Count canonical native-v2 payload keys without including receipts,
    /// records, packs, or Delta files.
    pub async fn immutable_object_count(&self) -> Result<u64> {
        let mut stream = self
            .store
            .object_store()
            .list(Some(&object_store::path::Path::from(V2_OBJECT_PREFIX)));
        let mut count = 0u64;
        while let Some(item) = stream.next().await {
            let metadata = item.map_err(|error| {
                StoreError::Invariant(format!("list immutable objects: {error}"))
            })?;
            if metadata
                .location
                .filename()
                .is_some_and(|name| name.starts_with("blake3="))
            {
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

    async fn create_content_addressed_small(
        &self,
        path: &object_store::path::Path,
        hash: ObjectHash,
        bytes: &[u8],
        class: crate::AccessClass,
        label: &str,
    ) -> Result<bool> {
        if ObjectHash::of_bytes(bytes) != hash {
            return Err(StoreError::Invariant(format!(
                "{label} bytes do not hash to {hash}"
            )));
        }
        match self
            .store
            .object_store()
            .put_opts(
                path,
                bytes.to_vec().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
        {
            Ok(_) => {
                crate::metered_store::record_physical_create(
                    &crate::RemoteKey::new(&self.store.url()),
                    class,
                    bytes.len() as u64,
                );
                Ok(true)
            }
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = self
                    .read_content_addressed_small(path, hash, label)
                    .await?
                    .ok_or_else(|| {
                        StoreError::Invariant(format!(
                            "{label} {hash} disappeared after AlreadyExists"
                        ))
                    })?;
                if existing != bytes {
                    return Err(StoreError::Invariant(format!(
                        "existing {label} {hash} has conflicting bytes"
                    )));
                }
                Ok(false)
            }
            Err(error) => Err(StoreError::Invariant(format!(
                "create {label} {hash}: {error}"
            ))),
        }
    }

    async fn read_content_addressed_small(
        &self,
        path: &object_store::path::Path,
        hash: ObjectHash,
        label: &str,
    ) -> Result<Option<Vec<u8>>> {
        let result = match self.store.object_store().get(path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => {
                return Err(StoreError::Invariant(format!(
                    "read {label} {hash}: {error}"
                )));
            }
        };
        let bytes = result
            .bytes()
            .await
            .map_err(|error| StoreError::Invariant(format!("collect {label} {hash}: {error}")))?;
        let actual = ObjectHash::of_bytes(&bytes);
        if actual != hash {
            return Err(StoreError::Invariant(format!(
                "{label} {hash} hashes to {actual}"
            )));
        }
        Ok(Some(bytes.to_vec()))
    }

    async fn verify_receiptless_object(
        &self,
        descriptor: ObjectDescriptor,
        path: &object_store::path::Path,
    ) -> Result<ObjectReceipt> {
        use futures::TryStreamExt;

        let result = self.store.object_store().get(path).await.map_err(|error| {
            StoreError::Invariant(format!(
                "read receipt-less object {}: {error}",
                descriptor.hash
            ))
        })?;
        let metadata = result.meta.clone();
        let mut stream = result.into_stream();
        let mut hasher = blake3::Hasher::new();
        let mut length = 0u64;
        while let Some(chunk) = stream.try_next().await.map_err(|error| {
            StoreError::Invariant(format!(
                "stream receipt-less object {}: {error}",
                descriptor.hash
            ))
        })? {
            let _ = hasher.update(&chunk);
            length = length.checked_add(chunk.len() as u64).ok_or_else(|| {
                StoreError::Invariant(format!(
                    "receipt-less object {} exceeds u64::MAX",
                    descriptor.hash
                ))
            })?;
        }
        let actual = ObjectHash::from_bytes(*hasher.finalize().as_bytes());
        if actual != descriptor.hash || length != metadata.size as u64 {
            return Err(StoreError::Invariant(format!(
                "receipt-less object {} has hash {} length {}, metadata length {}",
                descriptor.hash, actual, length, metadata.size
            )));
        }
        receipt_from_meta(descriptor, length, &metadata)
    }

    async fn read_receipt(&self, hash: ObjectHash) -> Result<Option<ObjectReceipt>> {
        let path = Self::v2_receipt_path(hash);
        let result = match self.store.object_store().get(&path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => {
                return Err(StoreError::Invariant(format!(
                    "read receipt for {hash}: {error}"
                )));
            }
        };
        let bytes = result.bytes().await.map_err(|error| {
            StoreError::Invariant(format!("collect receipt for {hash}: {error}"))
        })?;
        let receipt = ObjectReceipt::decode(&bytes).map_err(|error| {
            StoreError::Invariant(format!("malformed receipt for {hash}: {error}"))
        })?;
        if receipt.payload_hash != hash {
            return Err(StoreError::Invariant(format!(
                "receipt path for {hash} names payload {}",
                receipt.payload_hash
            )));
        }
        Ok(Some(receipt))
    }

    async fn create_receipt(&self, receipt: &ObjectReceipt) -> Result<bool> {
        let path = Self::v2_receipt_path(receipt.payload_hash);
        let bytes = receipt.encode();
        match self
            .store
            .object_store()
            .put_opts(
                &path,
                bytes.clone().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
        {
            Ok(_) => {
                crate::metered_store::record_physical_create(
                    &crate::RemoteKey::new(&self.store.url()),
                    crate::AccessClass::ContentReceipts,
                    bytes.len() as u64,
                );
                Ok(true)
            }
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = self
                    .read_receipt(receipt.payload_hash)
                    .await?
                    .ok_or_else(|| {
                        StoreError::Invariant(format!(
                            "receipt {} disappeared after AlreadyExists",
                            receipt.payload_hash
                        ))
                    })?;
                if existing != *receipt {
                    return Err(StoreError::Invariant(format!(
                        "receipt {} conflicts with established identity",
                        receipt.payload_hash
                    )));
                }
                Ok(false)
            }
            Err(error) => Err(StoreError::Invariant(format!(
                "create receipt {}: {error}",
                receipt.payload_hash
            ))),
        }
    }

    async fn verify_receipt_identity(
        &self,
        descriptor: ObjectDescriptor,
        expected_length: u64,
        receipt: &ObjectReceipt,
    ) -> Result<()> {
        if receipt.payload_hash != descriptor.hash || receipt.byte_length != expected_length {
            return Err(StoreError::Invariant(format!(
                "receipt for {} does not match requested hash/length",
                descriptor.hash
            )));
        }
        let metadata = self
            .store
            .object_store()
            .head(&Self::v2_object_path(descriptor.hash))
            .await
            .map_err(|error| {
                StoreError::Invariant(format!(
                    "head receipt-backed object {}: {error}",
                    descriptor.hash
                ))
            })?;
        verify_receipt_metadata(receipt, &metadata)
    }

    fn recovery_recipe_versioned_path(hash: ObjectHash) -> object_store::path::Path {
        object_store::path::Path::from(format!(
            "{CAPSULE_PREFIX}/recipes/{RECIPE_NATIVE_FORMAT}/{}/README.sh",
            hash.to_hex()
        ))
    }

    fn recovery_recipe_discoverable_path() -> object_store::path::Path {
        object_store::path::Path::from(format!("{CAPSULE_PREFIX}/README.sh"))
    }

    async fn create_exact_object(
        &self,
        path: &object_store::path::Path,
        bytes: &[u8],
        label: &str,
    ) -> Result<bool> {
        match self
            .store
            .object_store()
            .put_opts(
                path,
                bytes.to_vec().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = self
                    .store
                    .object_store()
                    .get(path)
                    .await
                    .map_err(|error| {
                        StoreError::Invariant(format!("read existing {label}: {error}"))
                    })?
                    .bytes()
                    .await
                    .map_err(|error| {
                        StoreError::Invariant(format!("collect existing {label}: {error}"))
                    })?;
                if existing.as_ref() != bytes {
                    return Err(StoreError::Invariant(format!(
                        "existing {label} differs from the reviewed recovery recipe"
                    )));
                }
                Ok(false)
            }
            Err(error) => Err(StoreError::Invariant(format!("create {label}: {error}"))),
        }
    }

    async fn read_recovery_recipe(
        &self,
        path: &object_store::path::Path,
        label: &str,
    ) -> Result<Vec<u8>> {
        self.store
            .object_store()
            .get(path)
            .await
            .map_err(|error| StoreError::Invariant(format!("read {label}: {error}")))?
            .bytes()
            .await
            .map_err(|error| StoreError::Invariant(format!("collect {label}: {error}")))
            .map(|bytes| bytes.to_vec())
    }

    /// Verify the discoverable bootstrap against the immutable object named
    /// by the domain-separated hash of its own exact bytes.
    async fn inspect_discoverable_recovery_recipe(&self) -> Result<ObjectHash> {
        let discoverable_path = Self::recovery_recipe_discoverable_path();
        let discoverable = self
            .read_recovery_recipe(&discoverable_path, "discoverable recovery recipe")
            .await?;
        let discoverable_hash = crate::recovery_recipe::recovery_recipe_hash(&discoverable);
        let immutable_path = Self::recovery_recipe_versioned_path(discoverable_hash);
        let immutable = self
            .read_recovery_recipe(
                &immutable_path,
                "immutable copy of discoverable recovery recipe",
            )
            .await?;
        if immutable != discoverable {
            return Err(StoreError::Invariant(format!(
                "discoverable recovery recipe differs from its immutable copy {discoverable_hash}"
            )));
        }
        Ok(discoverable_hash)
    }

    /// Create the discoverable bootstrap when absent. An existing bootstrap
    /// remains immutable and is accepted only when its own hash-addressed
    /// immutable copy already exists with byte-identical content.
    async fn ensure_discoverable_recovery_recipe(&self, current: &[u8]) -> Result<bool> {
        let discoverable_path = Self::recovery_recipe_discoverable_path();
        match self
            .store
            .object_store()
            .put_opts(
                &discoverable_path,
                current.to_vec().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let _ = self.inspect_discoverable_recovery_recipe().await?;
                Ok(false)
            }
            Err(error) => Err(StoreError::Invariant(format!(
                "create discoverable recovery recipe: {error}"
            ))),
        }
    }

    /// Install the reviewed `watertown.commit.v1` recovery recipe exactly once.
    ///
    /// The immutable hash-addressed object is created before the discoverable
    /// top-level bootstrap. An existing discoverable bootstrap may contain a
    /// different recipe version, but only when its exact bytes are already
    /// backed by their own hash-addressed immutable copy. Differing objects
    /// are never overwritten.
    pub async fn publish_recovery_recipe_watertown_commit_v1(
        &self,
    ) -> Result<RecoveryRecipePublishOutcome> {
        let bytes = crate::recovery_recipe_watertown_commit_v1();
        let recipe_hash = crate::recovery_recipe_watertown_commit_v1_hash();
        let versioned_created = self
            .create_exact_object(
                &Self::recovery_recipe_versioned_path(recipe_hash),
                &bytes,
                "versioned recovery recipe",
            )
            .await?;
        let discoverable_created = self.ensure_discoverable_recovery_recipe(&bytes).await?;
        Ok(RecoveryRecipePublishOutcome {
            recipe_hash,
            versioned_created,
            discoverable_created,
        })
    }

    /// Ensure the current recovery recipe is installed at both required paths.
    pub async fn ensure_recovery_recipe_watertown_commit_v1(&self) -> Result<ObjectHash> {
        self.publish_recovery_recipe_watertown_commit_v1()
            .await
            .map(|outcome| outcome.recipe_hash)
    }

    /// Verify the current immutable recipe and the discoverable bootstrap
    /// against its own immutable hash-addressed copy.
    pub async fn inspect_recovery_recipe_watertown_commit_v1(&self) -> Result<ObjectHash> {
        let expected = crate::recovery_recipe_watertown_commit_v1();
        let hash = crate::recovery_recipe_watertown_commit_v1_hash();
        let current = self
            .read_recovery_recipe(
                &Self::recovery_recipe_versioned_path(hash),
                "current versioned recovery recipe",
            )
            .await?;
        if current != expected {
            return Err(StoreError::Invariant(format!(
                "current versioned recovery recipe differs from recipe {hash}"
            )));
        }
        let _ = self.inspect_discoverable_recovery_recipe().await?;
        Ok(hash)
    }

    /// Read the tip commit hash for `ref_name`, or `None` if the ref does not
    /// exist.
    pub async fn get_tip(&self, ref_name: &str) -> Result<Option<ObjectHash>> {
        Ok(self
            .current_publication(ref_name)
            .await?
            .map(|state| state.snapshot_tip))
    }

    /// Read the bytes of the object with the given hash, or `None` if absent.
    pub async fn get_object(&self, hash: ObjectHash) -> Result<Option<Vec<u8>>> {
        self.get_immutable_object(hash).await
    }

    /// Read exactly the requested native-v2 objects.
    pub async fn get_objects(&self, hashes: &[ObjectHash]) -> Result<HashMap<ObjectHash, Vec<u8>>> {
        let mut objects = HashMap::new();
        let mut unique = hashes.to_vec();
        unique.sort_unstable();
        unique.dedup();
        for hash in unique {
            if let Some(bytes) = self.get_immutable_object(hash).await? {
                let _ = objects.insert(hash, bytes);
            }
        }
        Ok(objects)
    }

    /// True if the object with the given hash is present on the remote.
    pub async fn has_object(&self, hash: ObjectHash) -> Result<bool> {
        Ok(self.get_object(hash).await?.is_some())
    }

    fn blob_path(hash: ObjectHash) -> object_store::path::Path {
        Self::v2_object_path(hash)
    }

    /// True if the external blob `hash` is already present in the remote blob
    /// store, so a producer can skip re-uploading it.
    ///
    /// Prefer [`Self::list_blobs`] when asking about more than a couple of
    /// blobs: this is one billed request per call.
    pub async fn has_blob(&self, hash: ObjectHash) -> Result<bool> {
        match self.store.object_store().head(&Self::blob_path(hash)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(StoreError::Invariant(format!("blob head: {e}"))),
        }
    }

    /// The current Delta table version, so a caller can prove an operation
    /// (such as [`Self::publish_pack`]) advanced no commit.
    pub fn delta_version(&self) -> i64 {
        self.store.delta_version()
    }

    /// Explicit diagnostic scan of all immutable v2 pack indexes for one
    /// logical series. Ordinary publication and pull never call this.
    pub async fn diagnostic_list_pack_hashes(
        &self,
        series_hash: ObjectHash,
    ) -> Result<std::collections::HashSet<ObjectHash>> {
        use futures::StreamExt;

        let prefix = object_store::path::Path::from(V2_PACK_PREFIX);
        let mut stream = self.store.object_store().list(Some(&prefix));
        let mut out = std::collections::HashSet::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(|e| StoreError::Invariant(format!("list packs: {e}")))?;
            if meta
                .location
                .as_ref()
                .starts_with(&format!("{V2_PACK_PREFIX}/by-series/"))
                || meta
                    .location
                    .as_ref()
                    .starts_with(&format!("{V2_PACK_PREFIX}/consolidated/"))
            {
                continue;
            }
            let Some(name) = meta.location.filename() else {
                continue;
            };
            let Some(hex) = name.strip_prefix("blake3=") else {
                continue;
            };
            let hash = ObjectHash::from_hex(hex).map_err(StoreError::Invariant)?;
            let Some(bytes) = self
                .read_content_addressed_small(&meta.location, hash, "pack")
                .await?
            else {
                continue;
            };
            let pack = crate::content::PackIndex::decode(&bytes).map_err(|error| {
                StoreError::Invariant(format!("decode diagnostic pack {hash}: {error}"))
            })?;
            if pack.series_hash() == series_hash {
                let _ = out.insert(hash);
            }
        }
        Ok(out)
    }

    /// Fetch one immutable native-v2 pack index.
    pub async fn get_pack_index_bytes(
        &self,
        series_hash: ObjectHash,
        pack_hash: ObjectHash,
    ) -> Result<Option<Vec<u8>>> {
        self.get_immutable_pack(PackDescriptor::new(series_hash, pack_hash))
            .await
    }

    async fn has_physical_object(&self, hash: ObjectHash) -> Result<bool> {
        Ok(self.immutable_object_size(hash).await?.is_some())
    }

    /// [`Self::publish_pack_with_known_present`] with an empty known-present
    /// set: every declared physical object is checked exactly, with no proof
    /// assumed. Use this whenever the caller does not itself know (from
    /// having just durably written them) which of a pack's physical objects
    /// are already present.
    pub async fn publish_pack(
        &self,
        series_hash: ObjectHash,
        pack_index: &crate::content::PackIndex,
        physical_blobs: &[(ObjectHash, Vec<u8>)],
    ) -> Result<ObjectHash> {
        self.publish_pack_with_known_present(
            series_hash,
            pack_index,
            physical_blobs,
            &std::collections::HashSet::new(),
        )
        .await
    }

    /// Publish one pack index and make it discoverable, uploading any of its
    /// declared physical blobs that `physical_blobs` supplies and that the
    /// remote does not already hold.
    ///
    /// `known_present` names physical object hashes the caller already proved
    /// durable in this operation. Every other declared hash is checked through
    /// its receipt-backed native-v2 key before the pack is created.
    ///
    /// Publication order is otherwise unchanged from [`Self::publish_pack`]:
    /// physical objects first (the design doc's "Physical pack index"
    /// section), pack index last, so a pack index never becomes visible
    /// while any physical object it names is missing.
    ///
    /// This never installs a fixed-key locator and never touches Delta
    /// publication state. Whole-range locator installation is reserved for
    /// [`Self::publish_consolidated_pack_with_known_present`].
    ///
    /// The write is idempotent and cheap to retry: if an advertisement
    /// already exists at `pack_index`'s own content-addressed key, this
    /// returns immediately without re-checking physical objects or
    /// re-writing the (necessarily identical) bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if `pack_index.series_hash()` does not equal
    /// `series_hash` (refusing to publish a pack under the wrong series'
    /// directory), or if any of `pack_index.physical_object_hashes()` not
    /// covered by `known_present` is still absent from the store after
    /// uploading every blob `physical_blobs` provides.
    pub async fn publish_pack_with_known_present(
        &self,
        series_hash: ObjectHash,
        pack_index: &crate::content::PackIndex,
        physical_blobs: &[(ObjectHash, Vec<u8>)],
        known_present: &std::collections::HashSet<ObjectHash>,
    ) -> Result<ObjectHash> {
        if pack_index.series_hash() != series_hash {
            return Err(StoreError::Invariant(format!(
                "refusing to publish pack under series={} for a pack index declaring series_hash={} (cross-series index)",
                series_hash.to_hex(),
                pack_index.series_hash().to_hex()
            )));
        }
        let bytes = pack_index.encode();
        let pack_hash = ObjectHash::of_bytes(&bytes);
        debug_assert_eq!(
            pack_hash,
            pack_index.hash(),
            "PackIndex::hash must equal blake3(encode())"
        );

        let mut uploaded: std::collections::HashSet<ObjectHash> = std::collections::HashSet::new();
        for (hash, blob_bytes) in physical_blobs {
            if uploaded.contains(hash)
                || known_present.contains(hash)
                || self.has_blob(*hash).await?
            {
                continue;
            }
            self.put_blob(*hash, &blob_bytes[..]).await?;
            let _ = uploaded.insert(*hash);
        }

        let mut checked: std::collections::HashSet<ObjectHash> = std::collections::HashSet::new();
        for hash in pack_index.physical_object_hashes() {
            if known_present.contains(hash) || !checked.insert(*hash) {
                continue;
            }
            if !self.has_physical_object(*hash).await? {
                return Err(StoreError::Invariant(format!(
                    "cannot publish pack {}: physical object {hash} is not present in the store",
                    pack_hash.to_hex()
                )));
            }
        }

        let descriptor = PackDescriptor::new(series_hash, pack_hash);
        let _ = self.store_immutable_pack(descriptor, &bytes).await?;
        Ok(pack_hash)
    }

    /// Publish an already-verified whole-range pack and then install its
    /// immutable consolidated locator.
    ///
    /// `manifest` must hash to `series_hash`, and the pack must verify as its
    /// complete parentless range. The caller first makes every hash in
    /// `known_present` durable through this remote. Any remaining referenced
    /// object is checked here. Pack bytes are created before the one-shot
    /// failure boundary and locator, so failure leaves the previous locator
    /// state valid and retry is physically idempotent.
    pub async fn publish_consolidated_pack_with_known_present(
        &mut self,
        series_hash: ObjectHash,
        manifest: &crate::content::SeriesManifest,
        pack_index: &crate::content::PackIndex,
        known_present: &std::collections::HashSet<ObjectHash>,
    ) -> Result<ConsolidatedPackWriteOutcome> {
        if manifest.hash() != series_hash {
            return Err(StoreError::Invariant(format!(
                "consolidated series manifest hashes to {}, expected {series_hash}",
                manifest.hash()
            )));
        }
        if pack_index.series_hash() != series_hash {
            return Err(StoreError::Invariant(format!(
                "refusing to publish consolidated pack under series={} for a pack declaring {}",
                series_hash,
                pack_index.series_hash()
            )));
        }
        crate::content::verify_complete_pack_against_manifest(series_hash, manifest, pack_index)
            .map_err(StoreError::Invariant)?;
        for hash in pack_index.physical_object_hashes() {
            if !known_present.contains(hash) && !self.has_physical_object(*hash).await? {
                return Err(StoreError::Invariant(format!(
                    "cannot publish consolidated pack: physical object {hash} is absent"
                )));
            }
        }

        let bytes = pack_index.encode();
        let pack_hash = ObjectHash::of_bytes(&bytes);
        let descriptor = PackDescriptor::new(series_hash, pack_hash);
        let (pack_created, _) = self.store_immutable_pack(descriptor, &bytes).await?;
        self.publication_stage(PublicationFailurePoint::BeforeConsolidatedLocator)?;
        let locator_created = self
            .ensure_consolidated_series_pack_locator(descriptor)
            .await?;
        Ok(ConsolidatedPackWriteOutcome {
            pack_hash,
            pack_created,
            locator_created,
        })
    }

    /// Stream a large blob's raw bytes from `reader` into the remote blob store,
    /// keyed by `hash`.  Chunks flow through a bounded buffer to a multipart
    /// upload; the bytes are hashed as they pass so a value can never be stored
    /// under a key it does not equal.  Never collects the whole blob in memory.
    ///
    /// Backpressure is applied so a fast local reader cannot outrun a slow
    /// upload.  Keeping the multipart handle here, rather than delegating the
    /// trailing-part flush to `WriteMultipart::finish`, also guarantees every
    /// failed or refused part can be followed by an explicit abort.
    pub async fn put_blob<R>(&self, hash: ObjectHash, mut reader: R) -> Result<()>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        let _ = self
            .put_immutable_object_stream(
                ObjectDescriptor::new(hash, crate::content::ContentObjectKind::RawBlob),
                &mut reader,
            )
            .await?;
        Ok(())
    }

    async fn put_hashed_object<R>(
        &self,
        path: object_store::path::Path,
        hash: ObjectHash,
        expected_size: Option<u64>,
        mut reader: R,
    ) -> Result<()>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        use tokio::io::AsyncReadExt;
        if expected_size == Some(0) {
            let mut probe = [0u8; 1];
            let count = reader.read(&mut probe).await.map_err(|error| {
                StoreError::Invariant(format!("read empty streamed object: {error}"))
            })?;
            let empty_hash = ObjectHash::of_bytes(&[]);
            if count != 0 || hash != empty_hash {
                return Err(StoreError::Invariant(format!(
                    "streamed object is not the declared empty payload {hash}"
                )));
            }
            self.store
                .object_store()
                .put(&path, Vec::new().into())
                .await
                .map_err(|error| {
                    StoreError::Invariant(format!("publish empty streamed object: {error}"))
                })?;
            return Ok(());
        }
        let upload = self
            .store
            .object_store()
            .put_multipart(&path)
            .await
            .map_err(|e| StoreError::Invariant(format!("blob put_multipart: {e}")))?;
        let mut upload = upload;
        let mut parts = FuturesUnordered::new();
        let mut hasher = blake3::Hasher::new();
        let mut size = 0u64;

        loop {
            if parts.len() >= Self::MAX_INFLIGHT_UPLOAD_PARTS {
                match parts.next().await {
                    Some(Ok(())) => {}
                    Some(Err(error)) => {
                        drop(parts);
                        let abort = upload.abort().await;
                        return Err(StoreError::Invariant(match abort {
                            Ok(()) => format!("blob upload part: {error}"),
                            Err(abort_error) => {
                                format!("blob upload part: {error}; abort failed: {abort_error}")
                            }
                        }));
                    }
                    None => {}
                }
            }

            let mut part = vec![0u8; Self::MULTIPART_PART_SIZE];
            let mut filled = 0;
            while filled < part.len() {
                let n = match reader.read(&mut part[filled..]).await {
                    Ok(n) => n,
                    Err(error) => {
                        drop(parts);
                        let abort = upload.abort().await;
                        return Err(StoreError::Invariant(match abort {
                            Ok(()) => format!("blob read: {error}"),
                            Err(abort_error) => {
                                format!("blob read: {error}; abort failed: {abort_error}")
                            }
                        }));
                    }
                };
                if n == 0 {
                    break;
                }
                hasher.update(&part[filled..filled + n]);
                size = match size.checked_add(n as u64) {
                    Some(size) => size,
                    None => {
                        drop(parts);
                        let abort = upload.abort().await;
                        return Err(StoreError::Invariant(match abort {
                            Ok(()) => "streamed object exceeds u64::MAX".to_string(),
                            Err(abort_error) => format!(
                                "streamed object exceeds u64::MAX; abort failed: {abort_error}"
                            ),
                        }));
                    }
                };
                filled += n;
            }
            if filled == 0 {
                break;
            }
            part.truncate(filled);
            parts.push(upload.put_part(part.into()));
        }

        // Verify the streamed content matches its claimed key BEFORE completing
        // the multipart upload.  A multipart object only becomes visible on
        // `finish()`, so aborting here discards the staged parts and a value is
        // never stored under a key it does not equal -- no temporary key needed.
        let computed = ObjectHash::from_bytes(*hasher.finalize().as_bytes());
        if computed != hash || expected_size.is_some_and(|expected| expected != size) {
            drop(parts);
            upload
                .abort()
                .await
                .map_err(|e| StoreError::Invariant(format!("blob abort: {e}")))?;
            return Err(StoreError::Invariant(format!(
                "streamed object has hash {} and size {size}, expected hash {} and size {:?}",
                computed.to_hex(),
                hash.to_hex(),
                expected_size
            )));
        }

        while let Some(result) = parts.next().await {
            if let Err(error) = result {
                drop(parts);
                let abort = upload.abort().await;
                return Err(StoreError::Invariant(match abort {
                    Ok(()) => format!("blob upload part: {error}"),
                    Err(abort_error) => {
                        format!("blob upload part: {error}; abort failed: {abort_error}")
                    }
                }));
            }
        }
        if let Err(error) = upload.complete().await {
            let abort = upload.abort().await;
            return Err(StoreError::Invariant(match abort {
                Ok(()) => format!("blob complete: {error}"),
                Err(abort_error) => {
                    format!("blob complete: {error}; abort failed: {abort_error}")
                }
            }));
        }
        Ok(())
    }

    /// Open a streaming reader over a large blob's raw bytes by hash, or `None`
    /// if absent.  The body streams from object storage chunk by chunk; the
    /// caller re-hashes as it consumes so a multi-gigabyte blob never lands in a
    /// single buffer.  Unlike an in-memory fetch, integrity is the consumer's
    /// responsibility -- it must verify the streamed bytes hash to `hash`.
    pub async fn get_blob_reader(
        &self,
        hash: ObjectHash,
    ) -> Result<Option<Box<dyn tokio::io::AsyncRead + Unpin + Send>>> {
        self.get_immutable_object_reader(hash).await
    }

    /// Publish a complete portable recovery-capsule generation.
    ///
    /// Plain payload objects are written first, followed by the manifest and
    /// generated recovery artifacts. `recovery/refs/latest` is updated last,
    /// so interruption leaves the previous verified generation current.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest is invalid, the supplied payload
    /// closure differs from the manifest, any payload bytes disagree with
    /// their key/declared size, or object-store publication fails.
    pub async fn publish_capsule(
        &self,
        manifest: &CapsuleManifest,
        payloads: &std::collections::BTreeMap<ObjectHash, Vec<u8>>,
    ) -> Result<CapsulePublishOutcome> {
        let root = capsule_root(manifest).map_err(StoreError::Invariant)?;
        let manifest_bytes = capsule_manifest_bytes(manifest).map_err(StoreError::Invariant)?;
        let declared = manifest.payload_objects().map_err(StoreError::Invariant)?;
        verify_capsule_payloads(manifest, payloads).map_err(StoreError::Invariant)?;
        if declared.len() != payloads.len() {
            return Err(StoreError::Invariant(format!(
                "capsule declares {} payloads but publisher supplied {}",
                declared.len(),
                payloads.len()
            )));
        }

        self.with_capsule_publish_lock(async {
            let expected_ref = self.capsule_ref_version().await?;
            let mut uploaded = 0usize;
            for object in &declared {
                let bytes = payloads.get(&object.hash).ok_or_else(|| {
                    StoreError::Invariant(format!(
                        "capsule payload {} was not supplied",
                        object.hash
                    ))
                })?;
                if ObjectHash::of_bytes(bytes) != object.hash {
                    return Err(StoreError::Invariant(format!(
                        "capsule payload bytes do not hash to {}",
                        object.hash
                    )));
                }
                if u64::try_from(bytes.len()).ok() != Some(object.size) {
                    return Err(StoreError::Invariant(format!(
                        "capsule payload {} has size {}, expected {}",
                        object.hash,
                        bytes.len(),
                        object.size
                    )));
                }
                let path = Self::capsule_payload_path(object.hash);
                self.store
                    .object_store()
                    .put(&path, bytes.clone().into())
                    .await
                    .map_err(|error| {
                        StoreError::Invariant(format!(
                            "publish capsule payload {}: {error}",
                            object.hash
                        ))
                    })?;
                uploaded += 1;
            }

            self.finish_capsule_publication_locked(
                root,
                manifest_bytes,
                &declared,
                uploaded,
                expected_ref,
            )
            .await
        })
        .await
    }

    /// Publish a capsule whose payload closure is staged as
    /// `blake3=<hash>` files in `objects_dir`.
    ///
    /// Payload files are verified before publication and loaded one at a time,
    /// avoiding retention of the complete capsule closure in memory.
    pub async fn publish_capsule_directory(
        &self,
        manifest: &CapsuleManifest,
        objects_dir: &Path,
    ) -> Result<CapsulePublishOutcome> {
        let root = capsule_root(manifest).map_err(StoreError::Invariant)?;
        let manifest_bytes = capsule_manifest_bytes(manifest).map_err(StoreError::Invariant)?;
        let declared = manifest.payload_objects().map_err(StoreError::Invariant)?;
        verify_capsule_payload_directory(manifest, objects_dir).map_err(StoreError::Invariant)?;

        self.with_capsule_publish_lock(async {
            let expected_ref = self.capsule_ref_version().await?;
            let mut uploaded = 0usize;
            for object in &declared {
                let source = objects_dir.join(format!("blake3={}", object.hash.to_hex()));
                let file = tokio::fs::File::open(&source).await.map_err(|error| {
                    StoreError::Invariant(format!(
                        "open staged capsule payload {}: {error}",
                        object.hash
                    ))
                })?;
                let path = Self::capsule_payload_path(object.hash);
                self.put_hashed_object(path, object.hash, Some(object.size), file)
                    .await
                    .map_err(|error| {
                        StoreError::Invariant(format!(
                            "publish capsule payload {}: {error}",
                            object.hash
                        ))
                    })?;
                uploaded += 1;
            }

            self.finish_capsule_publication_locked(
                root,
                manifest_bytes,
                &declared,
                uploaded,
                expected_ref,
            )
            .await
        })
        .await
    }

    /// Publish a capsule that inherits unstaged payloads from `prior`.
    ///
    /// Every unstaged descriptor must occur identically in the current remote
    /// generation, and every reused remote payload is streamed and verified
    /// against its declared hash and size. Newly staged payloads are streamed
    /// and verified as usual. Publication is refused if `prior` is no longer
    /// the current generation.
    pub async fn publish_capsule_incremental(
        &self,
        manifest: &CapsuleManifest,
        objects_dir: &Path,
        prior: &CapsuleManifest,
    ) -> Result<CapsulePublishOutcome> {
        let root = capsule_root(manifest).map_err(StoreError::Invariant)?;
        let manifest_bytes = capsule_manifest_bytes(manifest).map_err(StoreError::Invariant)?;
        let declared = manifest.payload_objects().map_err(StoreError::Invariant)?;
        let prior_root = capsule_root(prior).map_err(StoreError::Invariant)?;
        verify_incremental_capsule_payload_directory(manifest, prior, objects_dir)
            .map_err(StoreError::Invariant)?;
        let prior_objects = prior
            .payload_objects()
            .map_err(StoreError::Invariant)?
            .into_iter()
            .map(|object| (object.hash, object.size))
            .collect::<std::collections::HashMap<_, _>>();
        self.with_capsule_publish_lock(async {
            let _ = self.require_current_capsule(prior_root).await?;

            let mut uploaded = 0usize;
            for object in &declared {
                let source = objects_dir.join(format!("blake3={}", object.hash.to_hex()));
                let staged = tokio::fs::try_exists(&source).await.map_err(|error| {
                    StoreError::Invariant(format!(
                        "inspect staged capsule payload {}: {error}",
                        object.hash
                    ))
                })?;
                if prior_objects.get(&object.hash) == Some(&object.size) {
                    self.verify_remote_capsule_payload(object.hash, object.size)
                        .await?;
                    continue;
                }
                if !staged {
                    return Err(StoreError::Invariant(format!(
                        "unstaged capsule payload {} is not inherited from the prior generation",
                        object.hash
                    )));
                }
                let file = tokio::fs::File::open(&source).await.map_err(|error| {
                    StoreError::Invariant(format!(
                        "open staged capsule payload {}: {error}",
                        object.hash
                    ))
                })?;
                self.put_hashed_object(
                    Self::capsule_payload_path(object.hash),
                    object.hash,
                    Some(object.size),
                    file,
                )
                .await
                .map_err(|error| {
                    StoreError::Invariant(format!(
                        "publish capsule payload {}: {error}",
                        object.hash
                    ))
                })?;
                uploaded += 1;
            }

            let expected_ref = self.require_current_capsule(prior_root).await?;
            self.finish_capsule_publication_locked(
                root,
                manifest_bytes,
                &declared,
                uploaded,
                expected_ref,
            )
            .await
        })
        .await
    }

    async fn capsule_ref_version(&self) -> Result<CapsuleRefVersion> {
        let result = match self
            .store
            .object_store()
            .get(&Self::capsule_latest_path())
            .await
        {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(CapsuleRefVersion::Missing),
            Err(error) => {
                return Err(StoreError::Invariant(format!(
                    "get capsule latest ref version: {error}"
                )));
            }
        };
        let bytes = result.bytes().await.map_err(|error| {
            StoreError::Invariant(format!("read capsule latest ref version: {error}"))
        })?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|error| StoreError::Invariant(format!("capsule ref is not UTF-8: {error}")))?
            .trim_end();
        let root = ObjectHash::from_hex(text).map_err(StoreError::Invariant)?;
        Ok(CapsuleRefVersion::Present { root })
    }

    async fn require_current_capsule(&self, expected: ObjectHash) -> Result<CapsuleRefVersion> {
        let current = self.capsule_ref_version().await?;
        let CapsuleRefVersion::Present { root, .. } = &current else {
            return Err(StoreError::Invariant(
                "incremental capsule publication requires a current generation".to_string(),
            ));
        };
        if *root != expected {
            return Err(StoreError::Invariant(format!(
                "capsule generation changed during incremental publication: expected {expected}, current {}",
                root
            )));
        }
        Ok(current)
    }

    async fn finish_capsule_publication_locked(
        &self,
        root: ObjectHash,
        manifest_bytes: Vec<u8>,
        declared: &[crate::content::CapsuleObject],
        uploaded: usize,
        expected_ref: CapsuleRefVersion,
    ) -> Result<CapsulePublishOutcome> {
        let manifest_path = Self::capsule_manifest_path(root);
        self.store
            .object_store()
            .put(&manifest_path, manifest_bytes.into())
            .await
            .map_err(|error| StoreError::Invariant(format!("publish capsule manifest: {error}")))?;

        let object_list = capsule_object_list(root, declared);
        let checksums = capsule_checksums(declared);
        let artifacts = [
            ("objects.list", object_list.into_bytes()),
            ("checksums", checksums.into_bytes()),
            ("RUNBOOK.txt", capsule_runbook(root).into_bytes()),
            ("download-az.sh", capsule_az_script(root).into_bytes()),
            ("download-mc.sh", capsule_mc_script(root).into_bytes()),
            ("CAPSULE-README.md", CAPSULE_README.as_bytes().to_vec()),
            ("CAPSULE-FORMAT.md", CAPSULE_FORMAT.as_bytes().to_vec()),
            ("capsule.py", CAPSULE_TOOL.as_bytes().to_vec()),
            (
                "parquet_schema.py",
                CAPSULE_PARQUET_SCHEMA.as_bytes().to_vec(),
            ),
            (
                "capsule-requirements.lock",
                CAPSULE_REQUIREMENTS.as_bytes().to_vec(),
            ),
            ("recover.sh", CAPSULE_RECOVER.as_bytes().to_vec()),
        ];
        for (name, bytes) in artifacts {
            let path = Self::capsule_generation_path(root, name);
            self.store
                .object_store()
                .put(&path, bytes.into())
                .await
                .map_err(|error| {
                    StoreError::Invariant(format!("publish capsule artifact {name}: {error}"))
                })?;
        }

        self.finish_capsule_refs_locked(root, expected_ref).await?;

        Ok(CapsulePublishOutcome {
            root,
            payloads_uploaded: uploaded,
            payloads_total: declared.len(),
        })
    }

    async fn finish_capsule_refs_locked(
        &self,
        root: ObjectHash,
        expected_ref: CapsuleRefVersion,
    ) -> Result<()> {
        let current = self.capsule_ref_version().await?;
        if current != expected_ref {
            return Err(StoreError::Invariant(
                "capsule latest ref changed before publication lock was acquired".to_string(),
            ));
        }
        let prior_history = self.capsule_history_bytes().await?;
        self.merge_capsule_history(root).await?;
        // Ref last: readers cannot discover this root before every generation
        // dependency and recovery artifact is durable.
        let latest = self
            .store
            .object_store()
            .put(
                &Self::capsule_latest_path(),
                format!("{}\n", root.to_hex()).into_bytes().into(),
            )
            .await;
        if let Err(error) = latest {
            let publish_error =
                StoreError::Invariant(format!("publish capsule latest ref: {error}"));
            return match self.restore_capsule_history(prior_history).await {
                Ok(()) => Err(publish_error),
                Err(restore_error) => Err(StoreError::Invariant(format!(
                    "{publish_error}; restore capsule history after failed latest ref: \
                     {restore_error}"
                ))),
            };
        }
        Ok(())
    }

    async fn acquire_capsule_publish_lock(&self) -> Result<Vec<u8>> {
        let path = Self::capsule_publish_lock_path();
        let created = chrono::Utc::now().timestamp_micros();
        let token = format!("{created}\n{}\n", Uuid::new_v4()).into_bytes();
        let result = self
            .store
            .object_store()
            .put_opts(
                &path,
                token.clone().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await;
        match result {
            Ok(_) => Ok(token),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = self
                    .store
                    .object_store()
                    .get(&path)
                    .await
                    .map_err(|error| {
                        StoreError::Invariant(format!("read capsule publication lock: {error}"))
                    })?
                    .bytes()
                    .await
                    .map_err(|error| {
                        StoreError::Invariant(format!("collect capsule publication lock: {error}"))
                    })?;
                let text = std::str::from_utf8(&existing).map_err(|error| {
                    StoreError::Invariant(format!("capsule publication lock is not UTF-8: {error}"))
                })?;
                let timestamp = text
                    .lines()
                    .next()
                    .ok_or_else(|| {
                        StoreError::Invariant(
                            "capsule publication lock has no timestamp".to_string(),
                        )
                    })?
                    .parse::<i64>()
                    .map_err(|error| {
                        StoreError::Invariant(format!(
                            "capsule publication lock timestamp is invalid: {error}"
                        ))
                    })?;
                let age = created.saturating_sub(timestamp);
                if age > CAPSULE_PUBLISH_LOCK_STALE_MICROS {
                    return Err(StoreError::Invariant(format!(
                        "stale capsule publication lock is {age} microseconds old; inspect and remove it explicitly"
                    )));
                }

                Err(StoreError::Invariant(
                    "another capsule publication holds the remote lock".to_string(),
                ))
            }
            Err(error) => Err(StoreError::Invariant(format!(
                "acquire capsule publication lock: {error}"
            ))),
        }
    }

    async fn with_capsule_publish_lock<T>(
        &self,
        operation: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        let lock = self.acquire_capsule_publish_lock().await?;
        let result = operation.await;
        let release = self.release_capsule_publish_lock(&lock).await;
        match (result, release) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(release_error)) => Err(StoreError::Invariant(format!(
                "{error}; release capsule publication lock: {release_error}"
            ))),
        }
    }

    async fn release_capsule_publish_lock(&self, token: &[u8]) -> Result<()> {
        let path = Self::capsule_publish_lock_path();
        let current = self
            .store
            .object_store()
            .get(&path)
            .await
            .map_err(|error| {
                StoreError::Invariant(format!("read capsule publication lock: {error}"))
            })?
            .bytes()
            .await
            .map_err(|error| {
                StoreError::Invariant(format!("collect capsule publication lock: {error}"))
            })?;
        if current.as_ref() != token {
            return Err(StoreError::Invariant(
                "capsule publication lock ownership changed".to_string(),
            ));
        }
        self.store
            .object_store()
            .delete(&path)
            .await
            .map_err(|error| {
                StoreError::Invariant(format!("release capsule publication lock: {error}"))
            })
    }

    async fn merge_capsule_history(&self, root: ObjectHash) -> Result<()> {
        let mut history = self.capsule_roots().await?;
        history.retain(|prior| *prior != root);
        history.insert(0, root);
        history.truncate(CAPSULE_HISTORY_LIMIT);
        let history_bytes = history
            .iter()
            .map(ObjectHash::to_hex)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        self.store
            .object_store()
            .put(
                &Self::capsule_history_path(),
                history_bytes.into_bytes().into(),
            )
            .await
            .map_err(|error| {
                StoreError::Invariant(format!("publish capsule history ref: {error}"))
            })?;
        Ok(())
    }

    async fn capsule_history_bytes(&self) -> Result<Option<Vec<u8>>> {
        match self
            .store
            .object_store()
            .get(&Self::capsule_history_path())
            .await
        {
            Ok(result) => result
                .bytes()
                .await
                .map(|bytes| Some(bytes.to_vec()))
                .map_err(|error| StoreError::Invariant(format!("read capsule history: {error}"))),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(StoreError::Invariant(format!(
                "get capsule history: {error}"
            ))),
        }
    }

    async fn restore_capsule_history(&self, prior: Option<Vec<u8>>) -> Result<()> {
        let path = Self::capsule_history_path();
        match prior {
            Some(bytes) => self
                .store
                .object_store()
                .put(&path, bytes.into())
                .await
                .map(|_| ())
                .map_err(|error| {
                    StoreError::Invariant(format!("restore capsule history ref: {error}"))
                }),
            None => self
                .store
                .object_store()
                .delete(&path)
                .await
                .map_err(|error| {
                    StoreError::Invariant(format!("remove new capsule history ref: {error}"))
                }),
        }
    }

    /// Read and validate the latest capsule manifest, if one is published.
    pub async fn latest_capsule(&self) -> Result<Option<(ObjectHash, CapsuleManifest)>> {
        let reference = match self
            .store
            .object_store()
            .get(&Self::capsule_latest_path())
            .await
        {
            Ok(result) => result
                .bytes()
                .await
                .map_err(|error| StoreError::Invariant(format!("read capsule ref: {error}")))?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => {
                return Err(StoreError::Invariant(format!(
                    "get capsule latest ref: {error}"
                )));
            }
        };
        let root_text = std::str::from_utf8(&reference)
            .map_err(|error| StoreError::Invariant(format!("capsule ref is not UTF-8: {error}")))?
            .trim_end();
        let root = ObjectHash::from_hex(root_text).map_err(StoreError::Invariant)?;
        let manifest = self.capsule_manifest(root).await?;
        Ok(Some((root, manifest)))
    }

    /// Read and validate one retained capsule manifest by root.
    pub async fn capsule_manifest(&self, root: ObjectHash) -> Result<CapsuleManifest> {
        let bytes = self
            .store
            .object_store()
            .get(&Self::capsule_manifest_path(root))
            .await
            .map_err(|error| StoreError::Invariant(format!("get capsule manifest: {error}")))?
            .bytes()
            .await
            .map_err(|error| StoreError::Invariant(format!("read capsule manifest: {error}")))?;
        let manifest = decode_capsule_manifest(&bytes).map_err(StoreError::Invariant)?;
        let computed = capsule_root(&manifest).map_err(StoreError::Invariant)?;
        if computed != root {
            return Err(StoreError::Invariant(format!(
                "capsule manifest hashes to {computed}, latest ref names {root}"
            )));
        }
        Ok(manifest)
    }

    /// Retained verified capsule roots, newest first.
    pub async fn capsule_roots(&self) -> Result<Vec<ObjectHash>> {
        let bytes = match self.capsule_history_bytes().await? {
            Some(bytes) => bytes,
            None => return Ok(Vec::new()),
        };
        let text = std::str::from_utf8(&bytes).map_err(|error| {
            StoreError::Invariant(format!("capsule history is not UTF-8: {error}"))
        })?;
        let mut roots = Vec::new();
        for line in text.lines() {
            if line.is_empty() {
                return Err(StoreError::Invariant(
                    "capsule history contains an empty root".to_string(),
                ));
            }
            if line.bytes().any(|byte| byte.is_ascii_uppercase()) {
                return Err(StoreError::Invariant(
                    "capsule history root is not lowercase hexadecimal".to_string(),
                ));
            }
            let root = ObjectHash::from_hex(line).map_err(StoreError::Invariant)?;
            if roots.contains(&root) {
                return Err(StoreError::Invariant(format!(
                    "capsule history repeats root {root}"
                )));
            }
            roots.push(root);
        }
        if roots.len() > CAPSULE_HISTORY_LIMIT {
            return Err(StoreError::Invariant(format!(
                "capsule history contains {} roots, limit is {CAPSULE_HISTORY_LIMIT}",
                roots.len()
            )));
        }
        Ok(roots)
    }

    /// Snapshot retained generations and plan deletion of unreachable capsule
    /// objects after `grace_micros`.
    pub async fn plan_capsule_gc(
        &self,
        remote_url: &str,
        grace_micros: i64,
    ) -> Result<CapsuleGcPlan> {
        if grace_micros < 0 {
            return Err(StoreError::Invariant(
                "capsule GC grace period cannot be negative".to_string(),
            ));
        }
        let (latest_root, retained_roots, reachable) = self.capsule_retention_snapshot().await?;
        let created_at_micros = chrono::Utc::now().timestamp_micros();
        let not_before_micros = created_at_micros.checked_add(grace_micros).ok_or_else(|| {
            StoreError::Invariant("capsule GC not-before exceeds i64::MAX".to_string())
        })?;
        let plan = CapsuleGcPlan {
            format: CAPSULE_GC_PLAN_FORMAT.to_string(),
            remote_url: remote_url.to_string(),
            remote_pond_id: self.pond_id().to_string(),
            latest_root: latest_root.to_hex(),
            retained_roots: retained_roots.iter().map(ObjectHash::to_hex).collect(),
            created_at_micros,
            not_before_micros,
            deletions: self
                .capsule_gc_candidates(&retained_roots, &reachable)
                .await?,
        };
        plan.validate().map_err(StoreError::Invariant)?;
        Ok(plan)
    }

    /// Revalidate a capsule GC plan without deleting anything.
    pub async fn verify_capsule_gc_plan(
        &self,
        remote_url: &str,
        plan: &CapsuleGcPlan,
    ) -> Result<()> {
        let candidates = self
            .validated_capsule_gc_candidates(remote_url, plan)
            .await?;
        for deletion in &plan.deletions {
            if candidates.get(&deletion.key) != Some(&deletion.size) {
                return Err(StoreError::Invariant(format!(
                    "capsule GC target {:?} no longer matches the plan",
                    deletion.key
                )));
            }
        }
        Ok(())
    }

    /// Apply a reviewed capsule GC plan after its grace period.
    ///
    /// Missing targets are treated as already applied, allowing safe retry
    /// after a partial provider failure.
    pub async fn apply_capsule_gc_plan(
        &self,
        remote_url: &str,
        plan: &CapsuleGcPlan,
        reviewed_hash: ObjectHash,
    ) -> Result<usize> {
        let actual_hash = capsule_gc_plan_hash(plan).map_err(StoreError::Invariant)?;
        if actual_hash != reviewed_hash {
            return Err(StoreError::Invariant(format!(
                "capsule GC plan hashes to {actual_hash}, reviewed hash is {reviewed_hash}"
            )));
        }
        let now = chrono::Utc::now().timestamp_micros();
        if now < plan.not_before_micros {
            return Err(StoreError::Invariant(format!(
                "capsule GC grace period has not elapsed (now={now}, not_before={})",
                plan.not_before_micros
            )));
        }
        let lock = self.acquire_capsule_publish_lock().await?;
        let apply = async {
            let candidates = self
                .validated_capsule_gc_candidates(remote_url, plan)
                .await?;
            let mut deleted = 0usize;
            for deletion in &plan.deletions {
                match candidates.get(&deletion.key) {
                    Some(size) if *size == deletion.size => {
                        self.store
                            .object_store()
                            .delete(&object_store::path::Path::from(deletion.key.clone()))
                            .await
                            .map_err(|error| {
                                StoreError::Invariant(format!(
                                    "delete capsule GC target {:?}: {error}",
                                    deletion.key
                                ))
                            })?;
                        deleted += 1;
                    }
                    Some(size) => {
                        return Err(StoreError::Invariant(format!(
                            "capsule GC target {:?} has size {size}, expected {}",
                            deletion.key, deletion.size
                        )));
                    }
                    None => {}
                }
            }
            Ok(deleted)
        }
        .await;
        let release = self.release_capsule_publish_lock(&lock).await;
        match (apply, release) {
            (Ok(deleted), Ok(())) => Ok(deleted),
            (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(release_error)) => Err(StoreError::Invariant(format!(
                "{error}; release capsule GC lock: {release_error}"
            ))),
        }
    }

    async fn validated_capsule_gc_candidates(
        &self,
        remote_url: &str,
        plan: &CapsuleGcPlan,
    ) -> Result<std::collections::HashMap<String, u64>> {
        plan.validate().map_err(StoreError::Invariant)?;
        if plan.remote_url != remote_url || plan.remote_pond_id != self.pond_id().to_string() {
            return Err(StoreError::Invariant(
                "capsule GC plan targets a different remote".to_string(),
            ));
        }
        let (latest_root, retained_roots, reachable) = self.capsule_retention_snapshot().await?;
        let plan_roots = plan
            .retained_roots
            .iter()
            .map(|root| ObjectHash::from_hex(root).map_err(StoreError::Invariant))
            .collect::<Result<Vec<_>>>()?;
        if latest_root.to_hex() != plan.latest_root || retained_roots != plan_roots {
            return Err(StoreError::Invariant(
                "capsule retention changed after the GC plan was created".to_string(),
            ));
        }
        Ok(self
            .capsule_gc_candidates(&retained_roots, &reachable)
            .await?
            .into_iter()
            .map(|object| (object.key, object.size))
            .collect())
    }

    async fn capsule_retention_snapshot(
        &self,
    ) -> Result<(
        ObjectHash,
        Vec<ObjectHash>,
        std::collections::HashSet<ObjectHash>,
    )> {
        let latest_root = self
            .latest_capsule()
            .await?
            .ok_or_else(|| {
                StoreError::Invariant("capsule GC requires a current generation".to_string())
            })?
            .0;
        let retained_roots = self.capsule_roots().await?;
        if retained_roots.first() != Some(&latest_root) {
            return Err(StoreError::Invariant(
                "capsule history does not begin with latest".to_string(),
            ));
        }
        let mut reachable = std::collections::HashSet::new();
        for root in &retained_roots {
            let manifest = self.capsule_manifest(*root).await?;
            for object in manifest.payload_objects().map_err(StoreError::Invariant)? {
                let _ = reachable.insert(object.hash);
            }
        }
        Ok((latest_root, retained_roots, reachable))
    }

    async fn capsule_gc_candidates(
        &self,
        retained_roots: &[ObjectHash],
        reachable: &std::collections::HashSet<ObjectHash>,
    ) -> Result<Vec<CapsuleGcObject>> {
        let prefix = object_store::path::Path::from(format!("{CAPSULE_PREFIX}/"));
        let mut listing = self.store.object_store().list(Some(&prefix));
        let mut deletions = Vec::new();
        while let Some(result) = listing.next().await {
            let metadata = result.map_err(|error| {
                StoreError::Invariant(format!("list capsule GC objects: {error}"))
            })?;
            let key = metadata.location.to_string();
            let delete = if let Some(hash) =
                parse_capsule_payload_key(&key).map_err(StoreError::Invariant)?
            {
                !reachable.contains(&hash)
            } else if let Some(root) =
                parse_capsule_manifest_key(&key).map_err(StoreError::Invariant)?
            {
                !retained_roots.contains(&root)
            } else if let Some(root) =
                parse_capsule_generation_key(&key).map_err(StoreError::Invariant)?
            {
                !retained_roots.contains(&root)
            } else {
                false
            };
            if delete {
                deletions.push(CapsuleGcObject {
                    key,
                    size: metadata.size,
                });
            }
        }
        deletions.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(deletions)
    }

    fn capsule_payload_path(hash: ObjectHash) -> object_store::path::Path {
        object_store::path::Path::from(format!("{CAPSULE_PREFIX}/objects/blake3={}", hash.to_hex()))
    }

    async fn verify_remote_capsule_payload(
        &self,
        expected_hash: ObjectHash,
        expected_size: u64,
    ) -> Result<()> {
        let result = self
            .store
            .object_store()
            .get(&Self::capsule_payload_path(expected_hash))
            .await
            .map_err(|error| {
                StoreError::Invariant(format!(
                    "read inherited capsule payload {expected_hash}: {error}"
                ))
            })?;
        let mut stream = result.into_stream();
        let mut hasher = blake3::Hasher::new();
        let mut size = 0u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| {
                StoreError::Invariant(format!(
                    "stream inherited capsule payload {expected_hash}: {error}"
                ))
            })?;
            size = size.checked_add(chunk.len() as u64).ok_or_else(|| {
                StoreError::Invariant(format!(
                    "inherited capsule payload {expected_hash} exceeds u64::MAX"
                ))
            })?;
            hasher.update(&chunk);
        }
        let computed_hash = ObjectHash::from_bytes(*hasher.finalize().as_bytes());
        if computed_hash != expected_hash || size != expected_size {
            return Err(StoreError::Invariant(format!(
                "inherited capsule payload {expected_hash} has hash {computed_hash} and size \
                 {size}, expected hash {expected_hash} and size {expected_size}"
            )));
        }
        Ok(())
    }

    #[cfg(test)]
    async fn list_capsule_payloads(&self) -> Result<std::collections::HashSet<ObjectHash>> {
        let prefix = object_store::path::Path::from(format!("{CAPSULE_PREFIX}/objects"));
        let mut stream = self.store.object_store().list(Some(&prefix));
        let mut hashes = std::collections::HashSet::new();
        while let Some(result) = stream.next().await {
            let metadata = result.map_err(|error| {
                StoreError::Invariant(format!("list capsule payloads: {error}"))
            })?;
            let Some(name) = metadata.location.filename() else {
                continue;
            };
            let Some(hex) = name.strip_prefix("blake3=") else {
                continue;
            };
            if let Ok(hash) = ObjectHash::from_hex(hex) {
                let _ = hashes.insert(hash);
            }
        }
        Ok(hashes)
    }

    fn capsule_manifest_path(root: ObjectHash) -> object_store::path::Path {
        object_store::path::Path::from(format!("{CAPSULE_PREFIX}/manifests/{}.json", root.to_hex()))
    }

    fn capsule_generation_path(root: ObjectHash, name: &str) -> object_store::path::Path {
        object_store::path::Path::from(format!(
            "{CAPSULE_PREFIX}/generations/{}/{name}",
            root.to_hex()
        ))
    }

    fn capsule_latest_path() -> object_store::path::Path {
        object_store::path::Path::from(format!("{CAPSULE_PREFIX}/refs/latest"))
    }

    fn capsule_history_path() -> object_store::path::Path {
        object_store::path::Path::from(format!("{CAPSULE_PREFIX}/refs/history"))
    }

    fn capsule_publish_lock_path() -> object_store::path::Path {
        object_store::path::Path::from(format!("{CAPSULE_PREFIX}/locks/publish"))
    }
}

fn validate_capsule_gc_key(key: &str) -> std::result::Result<(), String> {
    if key.starts_with('/') || key.split('/').any(|part| part.is_empty() || part == "..") {
        return Err(format!("unsafe capsule GC key {key:?}"));
    }
    if parse_capsule_payload_key(key)?.is_some()
        || parse_capsule_manifest_key(key)?.is_some()
        || parse_capsule_generation_key(key)?.is_some()
    {
        return Ok(());
    }
    Err(format!("capsule GC key is outside managed objects {key:?}"))
}

fn parse_capsule_payload_key(key: &str) -> std::result::Result<Option<ObjectHash>, String> {
    let prefix = format!("{CAPSULE_PREFIX}/objects/blake3=");
    let Some(hex) = key.strip_prefix(&prefix) else {
        return Ok(None);
    };
    if hex.contains('/') {
        return Err(format!("malformed capsule payload key {key:?}"));
    }
    ObjectHash::from_hex(hex).map(Some)
}

fn parse_capsule_manifest_key(key: &str) -> std::result::Result<Option<ObjectHash>, String> {
    let prefix = format!("{CAPSULE_PREFIX}/manifests/");
    let Some(name) = key.strip_prefix(&prefix) else {
        return Ok(None);
    };
    let hex = name
        .strip_suffix(".json")
        .ok_or_else(|| format!("malformed capsule manifest key {key:?}"))?;
    if hex.contains('/') {
        return Err(format!("malformed capsule manifest key {key:?}"));
    }
    ObjectHash::from_hex(hex).map(Some)
}

fn parse_capsule_generation_key(key: &str) -> std::result::Result<Option<ObjectHash>, String> {
    let prefix = format!("{CAPSULE_PREFIX}/generations/");
    let Some(rest) = key.strip_prefix(&prefix) else {
        return Ok(None);
    };
    let (hex, artifact) = rest
        .split_once('/')
        .ok_or_else(|| format!("malformed capsule generation key {key:?}"))?;
    if !matches!(
        artifact,
        "objects.list"
            | "checksums"
            | "RUNBOOK.txt"
            | "download-az.sh"
            | "download-mc.sh"
            | "CAPSULE-README.md"
            | "CAPSULE-FORMAT.md"
            | "capsule.py"
            | "parquet_schema.py"
            | "capsule-requirements.lock"
            | "recover.sh"
    ) {
        return Err(format!("unexpected capsule generation artifact {key:?}"));
    }
    ObjectHash::from_hex(hex).map(Some)
}

fn capsule_object_list(root: ObjectHash, objects: &[crate::content::CapsuleObject]) -> String {
    let root = root.to_hex();
    let mut output = format!("{CAPSULE_PREFIX}/manifests/{root}.json\n");
    for object in objects {
        output.push_str(&format!(
            "{CAPSULE_PREFIX}/objects/blake3={}\n",
            object.hash.to_hex()
        ));
    }
    for name in [
        "CAPSULE-README.md",
        "CAPSULE-FORMAT.md",
        "capsule.py",
        "parquet_schema.py",
        "capsule-requirements.lock",
        "recover.sh",
    ] {
        output.push_str(&format!("{CAPSULE_PREFIX}/generations/{root}/{name}\n"));
    }
    output
}

fn capsule_checksums(objects: &[crate::content::CapsuleObject]) -> String {
    let mut output = String::new();
    for object in objects {
        output.push_str(&format!(
            "{}  {CAPSULE_PREFIX}/objects/blake3={}\n",
            object.hash.to_hex(),
            object.hash.to_hex()
        ));
    }
    output
}

fn capsule_runbook(root: ObjectHash) -> String {
    format!(
        "Watertown recovery capsule {}\n\n\
         1. Download and review download-az.sh or download-mc.sh.\n\
         2. Authenticate with managed identity or your normal client environment.\n\
         3. Run the reviewed script; it embeds no credentials.\n\
         4. Authenticate the watertown.commit.v1 recovery kit through its independently \
            supplied README.sh hash.\n\
         5. Compare every downloaded recovery aid named in CAPSULE-README.md byte for \
            byte with the authenticated kit copies.\n\
         6. Read CAPSULE-README.md in the downloaded capsule.\n\
         7. Recover without Pond: sh /trusted/recovery-kit/recover.sh \
            <download-directory> <new-output-directory>\n\
         8. Read <new-output-directory>/README.txt and inventory.json.\n\
         9. Never delete the source namespace as part of recovery.\n",
        root.to_hex()
    )
}

fn capsule_az_script(root: ObjectHash) -> String {
    format!(
        "#!/bin/sh\nset -eu\n\
         : \"${{AZURE_CONTAINER:?set AZURE_CONTAINER}}\"\n\
         DEST=${{DEST:-capsule-{root}}}\n\
        case \"$DEST\" in ''|'/'|'.'|'..'|-*) printf '%s\\n' 'unsafe destination' >&2; exit 2;; esac\n\
        if [ -e \"$DEST\" ]; then printf '%s\\n' \"destination already exists: $DEST\" >&2; exit 2; fi\n\
        mkdir -p \"$DEST/recovery/generations/{root}\" \"$DEST/recovery/manifests\" \"$DEST/recovery/objects\" \"$DEST/recovery/refs\"\n\
         printf '%s\\n' '{root}' > \"$DEST/recovery/refs/latest\"\n\
         az storage blob download --auth-mode login --container-name \"$AZURE_CONTAINER\" --name \"recovery/generations/{root}/objects.list\" --file \"$DEST/recovery/generations/{root}/objects.list\" --overwrite\n\
         while IFS= read -r key; do\n\
           case \"$key\" in\n\
             \"recovery/manifests/{root}.json\") target=\"$DEST/$key\" ;;\n\
             recovery/objects/blake3=*) digest=${{key#recovery/objects/blake3=}}; case \"$digest\" in *[!0-9a-f]*|'') exit 1;; esac; [ \"${{#digest}}\" -eq 64 ] || exit 1; target=\"$DEST/$key\" ;;\n\
             \"recovery/generations/{root}/CAPSULE-README.md\") target=\"$DEST/CAPSULE-README.md\" ;;\n\
             \"recovery/generations/{root}/CAPSULE-FORMAT.md\") target=\"$DEST/CAPSULE-FORMAT.md\" ;;\n\
             \"recovery/generations/{root}/capsule.py\") target=\"$DEST/capsule.py\" ;;\n\
             \"recovery/generations/{root}/parquet_schema.py\") target=\"$DEST/parquet_schema.py\" ;;\n\
             \"recovery/generations/{root}/capsule-requirements.lock\") target=\"$DEST/capsule-requirements.lock\" ;;\n\
             \"recovery/generations/{root}/recover.sh\") target=\"$DEST/recover.sh\" ;;\n\
             *) exit 1 ;;\n\
           esac\n\
           mkdir -p \"$(dirname \"$target\")\"\n\
           az storage blob download --auth-mode login --container-name \"$AZURE_CONTAINER\" --name \"$key\" --file \"$target\" --overwrite\n\
         done < \"$DEST/recovery/generations/{root}/objects.list\"\n",
        root = root.to_hex()
    )
}

fn capsule_mc_script(root: ObjectHash) -> String {
    format!(
        "#!/bin/sh\nset -eu\n\
         : \"${{MC_SOURCE:?set MC_SOURCE to alias/bucket-or-prefix}}\"\n\
         DEST=${{DEST:-capsule-{root}}}\n\
        case \"$DEST\" in ''|'/'|'.'|'..'|-*) printf '%s\\n' 'unsafe destination' >&2; exit 2;; esac\n\
        if [ -e \"$DEST\" ]; then printf '%s\\n' \"destination already exists: $DEST\" >&2; exit 2; fi\n\
        mkdir -p \"$DEST/recovery/generations/{root}\" \"$DEST/recovery/manifests\" \"$DEST/recovery/objects\" \"$DEST/recovery/refs\"\n\
         printf '%s\\n' '{root}' > \"$DEST/recovery/refs/latest\"\n\
         mc cp \"$MC_SOURCE/recovery/generations/{root}/objects.list\" \"$DEST/recovery/generations/{root}/objects.list\"\n\
         while IFS= read -r key; do\n\
           case \"$key\" in\n\
             \"recovery/manifests/{root}.json\") target=\"$DEST/$key\" ;;\n\
             recovery/objects/blake3=*) digest=${{key#recovery/objects/blake3=}}; case \"$digest\" in *[!0-9a-f]*|'') exit 1;; esac; [ \"${{#digest}}\" -eq 64 ] || exit 1; target=\"$DEST/$key\" ;;\n\
             \"recovery/generations/{root}/CAPSULE-README.md\") target=\"$DEST/CAPSULE-README.md\" ;;\n\
             \"recovery/generations/{root}/CAPSULE-FORMAT.md\") target=\"$DEST/CAPSULE-FORMAT.md\" ;;\n\
             \"recovery/generations/{root}/capsule.py\") target=\"$DEST/capsule.py\" ;;\n\
             \"recovery/generations/{root}/parquet_schema.py\") target=\"$DEST/parquet_schema.py\" ;;\n\
             \"recovery/generations/{root}/capsule-requirements.lock\") target=\"$DEST/capsule-requirements.lock\" ;;\n\
             \"recovery/generations/{root}/recover.sh\") target=\"$DEST/recover.sh\" ;;\n\
             *) exit 1 ;;\n\
           esac\n\
           mkdir -p \"$(dirname \"$target\")\"\n\
           mc cp \"$MC_SOURCE/$key\" \"$target\"\n\
         done < \"$DEST/recovery/generations/{root}/objects.list\"\n",
        root = root.to_hex()
    )
}

fn receipt_from_put(
    descriptor: ObjectDescriptor,
    byte_length: u64,
    result: &PutResult,
) -> Result<ObjectReceipt> {
    ObjectReceipt::new(
        descriptor.hash,
        byte_length,
        result.e_tag.clone(),
        result.version.clone(),
    )
    .map_err(StoreError::Invariant)
}

fn receipt_from_meta(
    descriptor: ObjectDescriptor,
    byte_length: u64,
    metadata: &ObjectMeta,
) -> Result<ObjectReceipt> {
    ObjectReceipt::new(
        descriptor.hash,
        byte_length,
        metadata.e_tag.clone(),
        metadata.version.clone(),
    )
    .map_err(StoreError::Invariant)
}

fn verify_receipt_metadata(receipt: &ObjectReceipt, metadata: &ObjectMeta) -> Result<()> {
    if metadata.size != receipt.byte_length {
        return Err(StoreError::Invariant(format!(
            "receipt-backed object {} has size {}, expected {}",
            receipt.payload_hash, metadata.size, receipt.byte_length
        )));
    }
    if receipt
        .e_tag
        .as_ref()
        .is_some_and(|expected| metadata.e_tag.as_ref() != Some(expected))
    {
        return Err(StoreError::Invariant(format!(
            "receipt-backed object {} ETag changed",
            receipt.payload_hash
        )));
    }
    if receipt
        .version
        .as_ref()
        .is_some_and(|expected| metadata.version.as_ref() != Some(expected))
    {
        return Err(StoreError::Invariant(format!(
            "receipt-backed object {} version changed",
            receipt.payload_hash
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::content::{
        CapsuleEntry, CapsuleLeaf, CapsuleNode, CapsuleObject, CapsulePayloadKind, CapsuleSource,
        capsule_leaf_hash, capsule_series_root, verify_capsule_directory,
    };
    use tempfile::tempdir;
    use tinyfs::EntryType;

    #[tokio::test]
    async fn immutable_payload_retry_is_physically_unique() {
        let dir = tempdir().unwrap();
        let remote = ContentRemote::create_at(dir.path().join("remote"), Uuid::new_v4())
            .await
            .unwrap();
        let bytes = b"immutable payload".to_vec();
        let descriptor = ObjectDescriptor::new(
            ObjectHash::of_bytes(&bytes),
            crate::content::ContentObjectKind::RawBlob,
        );
        let first = remote
            .put_immutable_object(descriptor, &bytes)
            .await
            .unwrap();
        assert!(first.payload_created);
        assert!(first.receipt_created);
        let retry = remote
            .put_immutable_object(descriptor, &bytes)
            .await
            .unwrap();
        assert!(!retry.payload_created);
        assert!(!retry.receipt_created);
        assert_eq!(remote.immutable_object_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn concurrent_immutable_create_converges_to_one_payload() {
        let dir = tempdir().unwrap();
        let remote = ContentRemote::create_at(dir.path().join("remote"), Uuid::new_v4())
            .await
            .unwrap();
        let bytes = b"concurrent payload".to_vec();
        let descriptor = ObjectDescriptor::new(
            ObjectHash::of_bytes(&bytes),
            crate::content::ContentObjectKind::RawBlob,
        );
        let (left, right) = tokio::join!(
            remote.put_immutable_object(descriptor, &bytes),
            remote.put_immutable_object(descriptor, &bytes)
        );
        let left = left.unwrap();
        let right = right.unwrap();
        assert_eq!(
            usize::from(left.payload_created) + usize::from(right.payload_created),
            1
        );
        assert_eq!(remote.immutable_object_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn concurrent_publishers_fail_on_a_conflicting_canonical_key() {
        let dir = tempdir().unwrap();
        let remote = ContentRemote::create_at(dir.path().join("remote"), Uuid::new_v4())
            .await
            .unwrap();
        let expected = b"expected payload".to_vec();
        let descriptor = ObjectDescriptor::new(
            ObjectHash::of_bytes(&expected),
            crate::content::ContentObjectKind::RawBlob,
        );
        remote
            .store
            .object_store()
            .put(
                &ContentRemote::v2_object_path(descriptor.hash),
                b"conflicting publisher bytes".to_vec().into(),
            )
            .await
            .unwrap();
        let (left, right) = tokio::join!(
            remote.put_immutable_object(descriptor, &expected),
            remote.put_immutable_object(descriptor, &expected)
        );
        assert!(left.unwrap_err().to_string().contains("conflicting bytes"));
        assert!(right.unwrap_err().to_string().contains("conflicting bytes"));
    }

    #[tokio::test]
    async fn receiptless_payload_is_verified_once_and_receipted() {
        let dir = tempdir().unwrap();
        let remote = ContentRemote::create_at(dir.path().join("remote"), Uuid::new_v4())
            .await
            .unwrap();
        let bytes = b"interrupted create".to_vec();
        let descriptor = ObjectDescriptor::new(
            ObjectHash::of_bytes(&bytes),
            crate::content::ContentObjectKind::RawBlob,
        );
        remote
            .store
            .object_store()
            .put_opts(
                &ContentRemote::v2_object_path(descriptor.hash),
                bytes.clone().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
            .unwrap();
        let outcome = remote
            .put_immutable_object(descriptor, &bytes)
            .await
            .unwrap();
        assert!(!outcome.payload_created);
        assert!(outcome.receipt_created);
        assert!(
            remote
                .read_receipt(descriptor.hash)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn malformed_receipt_and_conflicting_existing_payload_fail_closed() {
        let dir = tempdir().unwrap();
        let remote = ContentRemote::create_at(dir.path().join("remote"), Uuid::new_v4())
            .await
            .unwrap();
        let bytes = b"valid payload".to_vec();
        let descriptor = ObjectDescriptor::new(
            ObjectHash::of_bytes(&bytes),
            crate::content::ContentObjectKind::RawBlob,
        );
        remote
            .store
            .object_store()
            .put(
                &ContentRemote::v2_object_path(descriptor.hash),
                bytes.clone().into(),
            )
            .await
            .unwrap();
        remote
            .store
            .object_store()
            .put(
                &ContentRemote::v2_receipt_path(descriptor.hash),
                b"malformed".to_vec().into(),
            )
            .await
            .unwrap();
        let error = remote
            .put_immutable_object(descriptor, &bytes)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("malformed receipt"));

        let other = b"expected bytes".to_vec();
        let other_descriptor = ObjectDescriptor::new(
            ObjectHash::of_bytes(&other),
            crate::content::ContentObjectKind::RawBlob,
        );
        remote
            .store
            .object_store()
            .put(
                &ContentRemote::v2_object_path(other_descriptor.hash),
                b"conflicting bytes".to_vec().into(),
            )
            .await
            .unwrap();
        let error = remote
            .put_immutable_object(other_descriptor, &other)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("conflicting bytes"));
    }

    #[tokio::test]
    async fn url_remote_persists_and_discovers_pond_id() {
        let dir = tempdir().unwrap();
        let url = format!("file://{}/remote", dir.path().display());
        let pond = Uuid::new_v4();
        let _ = ContentRemote::create_at_url(&url, pond, Default::default())
            .await
            .unwrap();
        let opened = ContentRemote::open_at_url(&url, Default::default())
            .await
            .unwrap();
        assert_eq!(opened.pond_id(), pond);
    }

    #[tokio::test]
    async fn static_recovery_recipe_is_immutable_and_idempotent() {
        let dir = tempdir().unwrap();
        let remote = ContentRemote::create_at(dir.path().join("remote"), Uuid::new_v4())
            .await
            .unwrap();
        let first = remote
            .publish_recovery_recipe_watertown_commit_v1()
            .await
            .unwrap();
        assert!(first.versioned_created);
        assert!(first.discoverable_created);
        assert_eq!(
            remote
                .inspect_recovery_recipe_watertown_commit_v1()
                .await
                .unwrap(),
            first.recipe_hash
        );

        let retry = remote
            .publish_recovery_recipe_watertown_commit_v1()
            .await
            .unwrap();
        assert_eq!(retry.recipe_hash, first.recipe_hash);
        assert!(!retry.versioned_created);
        assert!(!retry.discoverable_created);
    }

    #[tokio::test]
    async fn older_backed_discoverable_recipe_is_accepted_and_left_unchanged() {
        let dir = tempdir().unwrap();
        let remote = ContentRemote::create_at(dir.path().join("remote"), Uuid::new_v4())
            .await
            .unwrap();
        let older = b"#!/bin/sh\n# independently reviewed earlier recipe\n".to_vec();
        let older_hash = crate::recovery_recipe::recovery_recipe_hash(&older);
        remote
            .store
            .object_store()
            .put(
                &ContentRemote::recovery_recipe_versioned_path(older_hash),
                older.clone().into(),
            )
            .await
            .unwrap();
        remote
            .store
            .object_store()
            .put(
                &ContentRemote::recovery_recipe_discoverable_path(),
                older.clone().into(),
            )
            .await
            .unwrap();

        let published = remote
            .publish_recovery_recipe_watertown_commit_v1()
            .await
            .unwrap();
        assert!(published.versioned_created);
        assert!(!published.discoverable_created);
        assert_eq!(
            published.recipe_hash,
            crate::recovery_recipe_watertown_commit_v1_hash()
        );
        assert_eq!(
            remote
                .inspect_recovery_recipe_watertown_commit_v1()
                .await
                .unwrap(),
            published.recipe_hash
        );
        assert_eq!(
            remote
                .ensure_recovery_recipe_watertown_commit_v1()
                .await
                .unwrap(),
            published.recipe_hash
        );
        assert_eq!(
            remote
                .store
                .object_store()
                .get(&ContentRemote::recovery_recipe_discoverable_path())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            older.as_slice()
        );
    }

    #[tokio::test]
    async fn unbacked_discoverable_recipe_is_rejected_after_current_install() {
        let dir = tempdir().unwrap();
        let remote = ContentRemote::create_at(dir.path().join("remote"), Uuid::new_v4())
            .await
            .unwrap();
        let unbacked = b"#!/bin/sh\n# no immutable copy\n".to_vec();
        remote
            .store
            .object_store()
            .put(
                &ContentRemote::recovery_recipe_discoverable_path(),
                unbacked.into(),
            )
            .await
            .unwrap();

        let error = remote
            .publish_recovery_recipe_watertown_commit_v1()
            .await
            .expect_err("an unbacked discoverable recipe must fail");
        assert!(
            error.to_string().contains("immutable copy"),
            "error must identify the missing immutable copy: {error}"
        );
        let current_hash = crate::recovery_recipe_watertown_commit_v1_hash();
        assert!(
            remote
                .store
                .object_store()
                .head(&ContentRemote::recovery_recipe_versioned_path(current_hash))
                .await
                .is_ok(),
            "the current immutable recipe must be installed before discoverable validation"
        );
        assert!(
            remote
                .ensure_recovery_recipe_watertown_commit_v1()
                .await
                .is_err()
        );
        assert!(
            remote
                .inspect_recovery_recipe_watertown_commit_v1()
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn mismatched_discoverable_immutable_copy_is_rejected() {
        let dir = tempdir().unwrap();
        let remote = ContentRemote::create_at(dir.path().join("remote"), Uuid::new_v4())
            .await
            .unwrap();
        let discoverable = b"#!/bin/sh\n# reviewed recipe\n".to_vec();
        let discoverable_hash = crate::recovery_recipe::recovery_recipe_hash(&discoverable);
        remote
            .store
            .object_store()
            .put(
                &ContentRemote::recovery_recipe_versioned_path(discoverable_hash),
                b"different immutable bytes".to_vec().into(),
            )
            .await
            .unwrap();
        remote
            .store
            .object_store()
            .put(
                &ContentRemote::recovery_recipe_discoverable_path(),
                discoverable.into(),
            )
            .await
            .unwrap();

        let error = remote
            .publish_recovery_recipe_watertown_commit_v1()
            .await
            .expect_err("a mismatched immutable copy must fail");
        assert!(
            error
                .to_string()
                .contains("differs from its immutable copy"),
            "error must identify the immutable mismatch: {error}"
        );
        assert!(
            remote
                .inspect_recovery_recipe_watertown_commit_v1()
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn corrupted_current_versioned_recipe_is_rejected() {
        let dir = tempdir().unwrap();
        let remote = ContentRemote::create_at(dir.path().join("remote"), Uuid::new_v4())
            .await
            .unwrap();
        let current_hash = crate::recovery_recipe_watertown_commit_v1_hash();
        remote
            .store
            .object_store()
            .put(
                &ContentRemote::recovery_recipe_versioned_path(current_hash),
                b"corrupt current recipe".to_vec().into(),
            )
            .await
            .unwrap();

        assert!(
            remote
                .publish_recovery_recipe_watertown_commit_v1()
                .await
                .is_err()
        );
        assert!(
            remote
                .ensure_recovery_recipe_watertown_commit_v1()
                .await
                .is_err()
        );
        assert!(
            remote
                .inspect_recovery_recipe_watertown_commit_v1()
                .await
                .is_err()
        );

        let discoverable_remote =
            ContentRemote::create_at(dir.path().join("discoverable-remote"), Uuid::new_v4())
                .await
                .unwrap();
        discoverable_remote
            .publish_recovery_recipe_watertown_commit_v1()
            .await
            .unwrap();
        discoverable_remote
            .store
            .object_store()
            .put(
                &ContentRemote::recovery_recipe_discoverable_path(),
                b"corrupt current discoverable recipe".to_vec().into(),
            )
            .await
            .unwrap();
        assert!(
            discoverable_remote
                .publish_recovery_recipe_watertown_commit_v1()
                .await
                .is_err()
        );
        assert!(
            discoverable_remote
                .inspect_recovery_recipe_watertown_commit_v1()
                .await
                .is_err()
        );
    }

    fn test_capsule(payload: &[u8]) -> (CapsuleManifest, BTreeMap<ObjectHash, Vec<u8>>) {
        let payload_hash = ObjectHash::of_bytes(payload);
        let leaf = CapsuleLeaf {
            logical_hash: capsule_leaf_hash(
                CapsulePayloadKind::File,
                None,
                payload.len() as u64,
                payload,
                None,
                None,
                None,
            )
            .unwrap(),
            logical_count: payload.len() as u64,
            source_timestamp: 1_700_000_000_000_000,
            min_event_time: None,
            max_event_time: None,
            logical_attributes: None,
            schema_fingerprint: None,
        };
        let manifest = CapsuleManifest::new(
            CapsuleSource {
                pond_id: Uuid::nil().to_string(),
                birthplace: "test".to_string(),
                source_tip: ObjectHash::of_bytes(b"tip"),
                exported_at_micros: 1_700_000_000_000_000,
                tool_version: "test".to_string(),
            },
            vec![
                CapsuleEntry {
                    path: "/".to_string(),
                    entry_type: EntryType::DirectoryPhysical,
                    source_node_id: "root".to_string(),
                    node: CapsuleNode::Directory,
                },
                CapsuleEntry {
                    path: "/data".to_string(),
                    entry_type: EntryType::FilePhysicalVersion,
                    source_node_id: "data".to_string(),
                    node: CapsuleNode::Physical {
                        payload_kind: CapsulePayloadKind::File,
                        schema_fingerprint: None,
                        logical_root: capsule_series_root(
                            CapsulePayloadKind::File,
                            None,
                            std::slice::from_ref(&leaf),
                        ),
                        objects: vec![CapsuleObject {
                            hash: payload_hash,
                            size: payload.len() as u64,
                        }],
                        leaves: vec![leaf],
                    },
                },
            ],
        )
        .unwrap();
        (manifest, BTreeMap::from([(payload_hash, payload.to_vec())]))
    }

    fn append_capsule_file(
        prior: &CapsuleManifest,
        path: &str,
        payload: &[u8],
    ) -> (CapsuleManifest, CapsuleObject) {
        let object = CapsuleObject {
            hash: ObjectHash::of_bytes(payload),
            size: payload.len() as u64,
        };
        let leaf = CapsuleLeaf {
            logical_hash: capsule_leaf_hash(
                CapsulePayloadKind::File,
                None,
                object.size,
                payload,
                None,
                None,
                None,
            )
            .unwrap(),
            logical_count: object.size,
            source_timestamp: 1_700_000_000_000_001,
            min_event_time: None,
            max_event_time: None,
            logical_attributes: None,
            schema_fingerprint: None,
        };
        let mut entries = prior.entries.clone();
        entries.push(CapsuleEntry {
            path: path.to_string(),
            entry_type: EntryType::FilePhysicalVersion,
            source_node_id: path.to_string(),
            node: CapsuleNode::Physical {
                payload_kind: CapsulePayloadKind::File,
                schema_fingerprint: None,
                logical_root: capsule_series_root(
                    CapsulePayloadKind::File,
                    None,
                    std::slice::from_ref(&leaf),
                ),
                objects: vec![object.clone()],
                leaves: vec![leaf],
            },
        });
        let mut source = prior.source.clone();
        source.source_tip = ObjectHash::of_bytes(path.as_bytes());
        (
            CapsuleManifest::new(source, entries).expect("extended capsule"),
            object,
        )
    }

    #[tokio::test]
    async fn capsule_publication_is_verified_reference_last_and_idempotent() {
        let dir = tempdir().unwrap();
        let remote_path = dir.path().join("remote");
        let remote = ContentRemote::create_at(&remote_path, Uuid::new_v4())
            .await
            .unwrap();
        let (manifest, payloads) = test_capsule(b"portable");

        let first = remote.publish_capsule(&manifest, &payloads).await.unwrap();
        assert_eq!(first.payloads_uploaded, 1);
        assert_eq!(first.payloads_total, 1);
        let (latest_root, latest) = remote.latest_capsule().await.unwrap().unwrap();
        assert_eq!(latest_root, first.root);
        assert_eq!(latest, manifest);
        assert_eq!(remote.capsule_roots().await.unwrap(), vec![first.root]);
        let report = verify_capsule_directory(&remote_path).unwrap();
        assert_eq!(report.root, first.root);
        assert_eq!(report.entries, 2);
        assert_eq!(report.payload_objects, 1);
        assert_eq!(report.logical_count, 8);
        let generation = remote_path
            .join("recovery/generations")
            .join(first.root.to_hex());
        assert_eq!(
            std::fs::read_to_string(generation.join("CAPSULE-README.md")).unwrap(),
            CAPSULE_README
        );
        assert_eq!(
            std::fs::read_to_string(generation.join("CAPSULE-FORMAT.md")).unwrap(),
            CAPSULE_FORMAT
        );
        assert_eq!(
            std::fs::read_to_string(generation.join("capsule.py")).unwrap(),
            CAPSULE_TOOL
        );
        assert_eq!(
            std::fs::read_to_string(generation.join("parquet_schema.py")).unwrap(),
            CAPSULE_PARQUET_SCHEMA
        );
        assert_eq!(
            std::fs::read_to_string(generation.join("capsule-requirements.lock")).unwrap(),
            CAPSULE_REQUIREMENTS
        );
        assert_eq!(
            std::fs::read_to_string(generation.join("recover.sh")).unwrap(),
            CAPSULE_RECOVER
        );
        let object_list = std::fs::read_to_string(generation.join("objects.list")).unwrap();
        for name in [
            "CAPSULE-README.md",
            "CAPSULE-FORMAT.md",
            "capsule.py",
            "parquet_schema.py",
            "capsule-requirements.lock",
            "recover.sh",
        ] {
            assert!(object_list.contains(&format!(
                "recovery/generations/{}/{name}",
                first.root.to_hex()
            )));
        }

        let payload_hash = manifest.payload_objects().unwrap()[0].hash;
        std::fs::write(
            remote_path
                .join("recovery/objects")
                .join(format!("blake3={}", payload_hash.to_hex())),
            b"corrupt",
        )
        .unwrap();
        let second = remote.publish_capsule(&manifest, &payloads).await.unwrap();
        assert_eq!(second.root, first.root);
        assert_eq!(
            second.payloads_uploaded, 1,
            "explicit full publication repairs every declared payload"
        );
        assert_eq!(
            remote.latest_capsule().await.unwrap().unwrap().0,
            first.root
        );
        verify_capsule_directory(&remote_path).expect("full republish repairs corrupt payload");
    }

    #[tokio::test]
    async fn refused_capsule_does_not_replace_latest() {
        let dir = tempdir().unwrap();
        let remote = ContentRemote::create_at(dir.path().join("remote"), Uuid::new_v4())
            .await
            .unwrap();
        let (manifest, payloads) = test_capsule(b"good");
        let published = remote.publish_capsule(&manifest, &payloads).await.unwrap();

        let mut corrupt = payloads;
        *corrupt.values_mut().next().unwrap() = b"bad".to_vec();
        assert!(remote.publish_capsule(&manifest, &corrupt).await.is_err());
        assert_eq!(
            remote.latest_capsule().await.unwrap().unwrap().0,
            published.root
        );

        let (mut invalid_manifest, valid_payloads) = test_capsule(b"logically-invalid");
        let CapsuleNode::Physical {
            payload_kind,
            schema_fingerprint,
            logical_root,
            leaves,
            ..
        } = &mut invalid_manifest.entries[1].node
        else {
            panic!("physical test entry");
        };
        leaves[0].logical_hash = ObjectHash::of_bytes(b"wrong logical leaf");
        *logical_root = capsule_series_root(*payload_kind, *schema_fingerprint, leaves);
        assert!(
            remote
                .publish_capsule(&invalid_manifest, &valid_payloads)
                .await
                .is_err()
        );
        assert_eq!(
            remote.latest_capsule().await.unwrap().unwrap().0,
            published.root
        );
    }

    #[tokio::test]
    async fn failed_latest_write_restores_capsule_history() {
        let dir = tempdir().unwrap();
        let remote_path = dir.path().join("remote");
        let remote = ContentRemote::create_at(&remote_path, Uuid::new_v4())
            .await
            .unwrap();
        let (first_manifest, first_payloads) = test_capsule(b"first");
        let first = remote
            .publish_capsule(&first_manifest, &first_payloads)
            .await
            .unwrap();
        let prior_history = remote.capsule_roots().await.unwrap();

        let latest_path = remote_path.join("recovery/refs/latest");
        std::fs::remove_file(&latest_path).unwrap();
        std::fs::create_dir(&latest_path).unwrap();
        std::fs::write(latest_path.join("block-replacement"), b"occupied").unwrap();

        let (second_manifest, second_payloads) = test_capsule(b"second");
        let error = remote
            .publish_capsule(&second_manifest, &second_payloads)
            .await
            .expect_err("latest ref replacement must fail");
        assert!(error.to_string().contains("publish capsule latest ref"));
        assert_eq!(prior_history, vec![first.root]);
        assert_eq!(remote.capsule_roots().await.unwrap(), prior_history);
    }

    #[test]
    fn capsule_download_scripts_reject_unexpected_object_list_paths() {
        let root = ObjectHash::of_bytes(b"capsule");
        for script in [capsule_az_script(root), capsule_mc_script(root)] {
            assert!(script.contains("\"recovery/manifests/"));
            assert!(script.contains("recovery/objects/blake3=*"));
            assert!(script.contains("*[!0-9a-f]*|'') exit 1"));
            assert!(script.contains("\"${#digest}\" -eq 64"));
            assert!(script.contains("CAPSULE-README.md"));
            assert!(script.contains("CAPSULE-FORMAT.md"));
            assert!(script.contains("capsule.py"));
            assert!(script.contains("parquet_schema.py"));
            assert!(script.contains("capsule-requirements.lock"));
            assert!(script.contains("recover.sh"));
            assert!(script.contains("target=\"$DEST/capsule.py\""));
            assert!(script.contains("destination already exists"));
            assert!(script.contains("*) exit 1"));
        }
    }

    #[tokio::test]
    async fn capsule_history_retains_three_newest_roots() {
        let dir = tempdir().unwrap();
        let remote = ContentRemote::create_at(dir.path().join("remote"), Uuid::new_v4())
            .await
            .unwrap();
        let mut published = Vec::new();
        for payload in [b"one".as_slice(), b"two", b"three", b"four"] {
            let (manifest, payloads) = test_capsule(payload);
            published.push(
                remote
                    .publish_capsule(&manifest, &payloads)
                    .await
                    .unwrap()
                    .root,
            );
        }
        assert_eq!(
            remote.capsule_roots().await.unwrap(),
            vec![published[3], published[2], published[1]]
        );
        assert_eq!(
            remote.latest_capsule().await.unwrap().unwrap().0,
            published[3]
        );

        let plan = remote.plan_capsule_gc("file:///backup", 0).await.unwrap();
        assert_eq!(plan.latest_root, published[3].to_hex());
        assert_eq!(plan.retained_roots.len(), 3);
        let obsolete_root = published[0].to_hex();
        let mut expected_deletions = [
            "CAPSULE-FORMAT.md",
            "CAPSULE-README.md",
            "RUNBOOK.txt",
            "capsule-requirements.lock",
            "capsule.py",
            "parquet_schema.py",
            "checksums",
            "download-az.sh",
            "download-mc.sh",
            "objects.list",
            "recover.sh",
        ]
        .map(|name| format!("recovery/generations/{obsolete_root}/{name}"))
        .to_vec();
        expected_deletions.push(format!("recovery/manifests/{obsolete_root}.json"));
        expected_deletions.push(format!(
            "recovery/objects/blake3={}",
            ObjectHash::of_bytes(b"one").to_hex()
        ));
        expected_deletions.sort();
        assert_eq!(
            plan.deletions
                .iter()
                .map(|object| object.key.clone())
                .collect::<Vec<_>>(),
            expected_deletions
        );
        remote
            .verify_capsule_gc_plan("file:///backup", &plan)
            .await
            .unwrap();
        let bytes = capsule_gc_plan_bytes(&plan).unwrap();
        assert_eq!(decode_capsule_gc_plan(&bytes).unwrap(), plan);
        let plan_hash = capsule_gc_plan_hash(&plan).unwrap();
        assert_ne!(plan_hash, ObjectHash::of_bytes(b""));
        let deletion_count = expected_deletions.len();
        assert_eq!(
            remote
                .apply_capsule_gc_plan("file:///backup", &plan, plan_hash)
                .await
                .unwrap(),
            deletion_count
        );
        assert_eq!(
            remote
                .apply_capsule_gc_plan("file:///backup", &plan, plan_hash)
                .await
                .unwrap(),
            0
        );
        verify_capsule_directory(&dir.path().join("remote")).unwrap();

        let (newer, payloads) = test_capsule(b"five");
        let _ = remote.publish_capsule(&newer, &payloads).await.unwrap();
        assert!(
            remote
                .verify_capsule_gc_plan("file:///backup", &plan)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn incremental_capsule_requires_every_inherited_remote_payload() {
        let dir = tempdir().unwrap();
        let remote_path = dir.path().join("remote");
        let remote = ContentRemote::create_at(&remote_path, Uuid::new_v4())
            .await
            .unwrap();
        let (first, first_payloads) = test_capsule(b"first");
        let first_root = remote
            .publish_capsule(&first, &first_payloads)
            .await
            .unwrap()
            .root;

        let (second, second_object) = append_capsule_file(&first, "/second", b"second");
        let staging = tempdir().unwrap();
        std::fs::write(
            staging
                .path()
                .join(format!("blake3={}", second_object.hash.to_hex())),
            b"second",
        )
        .unwrap();
        let second_root = remote
            .publish_capsule_incremental(&second, staging.path(), &first)
            .await
            .unwrap()
            .root;
        assert_ne!(second_root, first_root);

        let (mut malicious, malicious_object) =
            append_capsule_file(&second, "/malicious", b"malicious");
        let CapsuleNode::Physical {
            payload_kind,
            schema_fingerprint,
            logical_root,
            leaves,
            ..
        } = &mut malicious.entries[1].node
        else {
            panic!("physical inherited entry");
        };
        leaves[0].logical_hash = ObjectHash::of_bytes(b"forged logical leaf");
        *logical_root = capsule_series_root(*payload_kind, *schema_fingerprint, leaves);
        let malicious_staging = tempdir().unwrap();
        std::fs::write(
            malicious_staging
                .path()
                .join(format!("blake3={}", malicious_object.hash.to_hex())),
            b"malicious",
        )
        .unwrap();
        assert!(
            remote
                .publish_capsule_incremental(&malicious, malicious_staging.path(), &second)
                .await
                .is_err()
        );
        assert_eq!(
            remote.latest_capsule().await.unwrap().unwrap().0,
            second_root
        );

        let inherited = first.payload_objects().unwrap()[0].hash;
        let inherited_path = remote_path
            .join("recovery/objects")
            .join(format!("blake3={}", inherited.to_hex()));
        std::fs::write(&inherited_path, b"xxxxx").unwrap();
        let (third, third_object) = append_capsule_file(&second, "/third", b"third");
        let staging = tempdir().unwrap();
        std::fs::write(
            staging
                .path()
                .join(format!("blake3={}", third_object.hash.to_hex())),
            b"third",
        )
        .unwrap();
        let error = remote
            .publish_capsule_incremental(&third, staging.path(), &second)
            .await
            .expect_err("corrupt inherited payload must prevent publication");
        assert!(
            error.to_string().contains("inherited capsule payload"),
            "unexpected error: {error}"
        );
        assert_eq!(
            remote.latest_capsule().await.unwrap().unwrap().0,
            second_root
        );

        std::fs::write(&inherited_path, b"first").unwrap();
        std::fs::remove_file(inherited_path).unwrap();
        assert!(
            remote
                .publish_capsule_incremental(&third, staging.path(), &second)
                .await
                .is_err()
        );
        assert_eq!(
            remote.latest_capsule().await.unwrap().unwrap().0,
            second_root
        );
    }

    #[tokio::test]
    async fn stale_capsule_ref_version_cannot_replace_winner() {
        let dir = tempdir().unwrap();
        let remote = ContentRemote::create_at(dir.path().join("remote"), Uuid::new_v4())
            .await
            .unwrap();
        let expected = remote.capsule_ref_version().await.unwrap();
        let (winner, _) = test_capsule(b"winner");
        let winner_root = capsule_root(&winner).unwrap();
        let winner_declared = winner.payload_objects().unwrap();
        remote
            .with_capsule_publish_lock(remote.finish_capsule_publication_locked(
                winner_root,
                capsule_manifest_bytes(&winner).unwrap(),
                &winner_declared,
                0,
                expected.clone(),
            ))
            .await
            .unwrap();

        let (loser, _) = test_capsule(b"loser");
        let loser_root = capsule_root(&loser).unwrap();
        let loser_declared = loser.payload_objects().unwrap();
        assert!(
            remote
                .with_capsule_publish_lock(remote.finish_capsule_publication_locked(
                    loser_root,
                    capsule_manifest_bytes(&loser).unwrap(),
                    &loser_declared,
                    0,
                    expected,
                ))
                .await
                .is_err()
        );
        assert_eq!(
            remote.latest_capsule().await.unwrap().unwrap().0,
            winner_root
        );
        assert_eq!(remote.capsule_roots().await.unwrap()[0], winner_root);
    }

    #[tokio::test]
    async fn active_capsule_publication_lock_refuses_upload_before_any_remote_write() {
        let dir = tempdir().unwrap();
        let remote = ContentRemote::create_at(dir.path().join("remote"), Uuid::new_v4())
            .await
            .unwrap();
        let token = format!(
            "{}\n{}\n",
            chrono::Utc::now().timestamp_micros(),
            Uuid::new_v4()
        );
        remote
            .store
            .object_store()
            .put(
                &ContentRemote::capsule_publish_lock_path(),
                token.into_bytes().into(),
            )
            .await
            .unwrap();
        let (manifest, payloads) = test_capsule(b"locked");
        assert!(remote.publish_capsule(&manifest, &payloads).await.is_err());
        assert!(remote.latest_capsule().await.unwrap().is_none());
        assert!(
            remote.list_capsule_payloads().await.unwrap().is_empty(),
            "a publisher must acquire the GC-shared lock before uploading payloads"
        );
    }
}
