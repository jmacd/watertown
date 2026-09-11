// SPDX-License-Identifier: Apache-2.0

//! Structured local publication and consumer acknowledgements.

use serde::{Deserialize, Serialize};
use sync_store::PublicationState;
use sync_store::content::{NATIVE_FORMAT_V2, ObjectHash};
use uuid::Uuid;

use crate::{ControlTable, StewardError};
use tlogfs::{PondTxnMetadata, PondUserMetadata};

const PUSH_ACK_PREFIX: &str = "publication_ack:v2:";
const PULL_ACK_PREFIX: &str = "consumer_ack:v2:";
const PULL_IDENTITY_PREFIX: &str = "consumer_identity:v2:";
const ACK_FORMAT: &str = "watertown.publication-ack.v2";

/// Whether two publication rows name the same durable publication identity.
///
/// `updated_at` is metadata rather than identity; all content, lineage, and
/// generation fields must match exactly.
#[must_use]
pub fn same_publication_identity(left: &PublicationState, right: &PublicationState) -> bool {
    left.pond_id == right.pond_id
        && left.ref_name == right.ref_name
        && left.format == right.format
        && left.snapshot_tip == right.snapshot_tip
        && left.manifest_root == right.manifest_root
        && left.publication_record == right.publication_record
        && left.generation == right.generation
}

/// Durable local acknowledgement keyed by remote URL, pond id, and ref.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationAcknowledgement {
    /// Ack encoding.
    pub format: String,
    /// Exact attached remote URL.
    pub remote_url: String,
    /// Source pond identity.
    pub pond_id: String,
    /// Ref name.
    pub ref_name: String,
    /// Native remote format.
    pub native_format: String,
    /// Acknowledged snapshot tip.
    pub snapshot_tip: String,
    /// Acknowledged manifest root.
    pub manifest_root: String,
    /// Acknowledged publication record.
    pub publication_record: String,
    /// Acknowledged generation.
    pub generation: i64,
}

impl PublicationAcknowledgement {
    /// Construct from a verified remote state.
    #[must_use]
    pub fn from_state(remote_url: &str, state: &PublicationState) -> Self {
        Self {
            format: ACK_FORMAT.to_string(),
            remote_url: remote_url.to_string(),
            pond_id: state.pond_id.to_string(),
            ref_name: state.ref_name.clone(),
            native_format: state.format.clone(),
            snapshot_tip: state.snapshot_tip.to_hex(),
            manifest_root: state.manifest_root.to_hex(),
            publication_record: state.publication_record.to_hex(),
            generation: state.generation,
        }
    }

    /// Validate identity and decode the acknowledged state.
    pub fn state(
        &self,
        remote_url: &str,
        pond_id: Uuid,
        ref_name: &str,
    ) -> Result<PublicationState, StewardError> {
        if self.format != ACK_FORMAT
            || self.remote_url != remote_url
            || self.pond_id != pond_id.to_string()
            || self.ref_name != ref_name
            || self.native_format != NATIVE_FORMAT_V2
            || self.generation <= 0
        {
            return Err(StewardError::ControlTable(format!(
                "publication acknowledgement identity/format mismatch for remote {remote_url:?} \
                 pond {pond_id} ref {ref_name:?}"
            )));
        }
        let snapshot_tip = ObjectHash::from_hex(&self.snapshot_tip).map_err(|error| {
            StewardError::ControlTable(format!("invalid acknowledgement snapshot_tip: {error}"))
        })?;
        let manifest_root = ObjectHash::from_hex(&self.manifest_root).map_err(|error| {
            StewardError::ControlTable(format!("invalid acknowledgement manifest_root: {error}"))
        })?;
        let publication_record =
            ObjectHash::from_hex(&self.publication_record).map_err(|error| {
                StewardError::ControlTable(format!(
                    "invalid acknowledgement publication_record: {error}"
                ))
            })?;
        PublicationState::new(
            pond_id,
            ref_name,
            snapshot_tip,
            manifest_root,
            publication_record,
            self.generation,
            0,
        )
        .map_err(|error| StewardError::ControlTable(error.to_string()))
    }
}

/// Read the producer acknowledgement for one remote/ref.
pub async fn read_push_ack(
    control: &ControlTable,
    remote_url: &str,
    pond_id: Uuid,
    ref_name: &str,
) -> Result<Option<PublicationAcknowledgement>, StewardError> {
    read_ack(control, PUSH_ACK_PREFIX, remote_url, pond_id, ref_name).await
}

/// Persist the producer acknowledgement after a successful visible CAS.
pub async fn write_push_ack(
    control: &mut ControlTable,
    remote_url: &str,
    state: &PublicationState,
) -> Result<(), StewardError> {
    write_ack(control, PUSH_ACK_PREFIX, remote_url, state, false).await
}

/// Read the consumer acknowledgement for one remote/ref.
pub async fn read_pull_ack(
    control: &ControlTable,
    remote_url: &str,
    pond_id: Uuid,
    ref_name: &str,
) -> Result<Option<PublicationAcknowledgement>, StewardError> {
    read_ack(control, PULL_ACK_PREFIX, remote_url, pond_id, ref_name).await
}

/// Persist the consumer acknowledgement after a snapshot has been applied.
pub async fn write_pull_ack(
    control: &mut ControlTable,
    remote_url: &str,
    state: &PublicationState,
) -> Result<(), StewardError> {
    write_ack(control, PULL_ACK_PREFIX, remote_url, state, true).await
}

/// Read the consumer acknowledgement when the source pond identity is not
/// otherwise available locally.
pub async fn read_pull_ack_for_remote(
    control: &ControlTable,
    remote_url: &str,
    ref_name: &str,
) -> Result<Option<PublicationAcknowledgement>, StewardError> {
    let Some(pond_id) = control
        .raw_config_get(&pull_identity_key(remote_url, ref_name))
        .await?
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let pond_id = Uuid::parse_str(&pond_id).map_err(|error| {
        StewardError::ControlTable(format!(
            "invalid consumer acknowledgement pond identity for remote {remote_url:?}: {error}"
        ))
    })?;
    read_pull_ack(control, remote_url, pond_id, ref_name).await
}

/// Clear producer and consumer acknowledgements for one attached identity.
pub async fn clear_acknowledgements(
    control: &mut ControlTable,
    remote_url: &str,
    pond_id: Uuid,
    ref_name: &str,
) -> Result<(), StewardError> {
    let control_path = control.inner().path().to_path_buf();
    let lock_meta = PondTxnMetadata::new(
        0,
        PondUserMetadata::new(vec!["publication-ack".to_string(), "clear".to_string()]),
    );
    let _lock = crate::write_lock::WriteLockGuard::try_acquire_named(
        &control_path,
        "publication-ack.lock",
        &lock_meta,
    )?;
    let mut fresh = ControlTable::open(&control_path).await?;
    set_raw_value(
        &mut fresh,
        &ack_key(PUSH_ACK_PREFIX, remote_url, pond_id, ref_name),
        "",
    )
    .await?;
    if let Some(source_pond_id) = fresh
        .raw_config_get(&pull_identity_key(remote_url, ref_name))
        .await?
        .filter(|value| !value.is_empty())
    {
        let source_pond_id = Uuid::parse_str(&source_pond_id).map_err(|error| {
            StewardError::ControlTable(format!(
                "invalid consumer acknowledgement pond identity for remote {remote_url:?}: {error}"
            ))
        })?;
        set_raw_value(
            &mut fresh,
            &ack_key(PULL_ACK_PREFIX, remote_url, source_pond_id, ref_name),
            "",
        )
        .await?;
    }
    set_raw_value(&mut fresh, &pull_identity_key(remote_url, ref_name), "").await?;
    *control = fresh;
    Ok(())
}

async fn read_ack(
    control: &ControlTable,
    prefix: &str,
    remote_url: &str,
    pond_id: Uuid,
    ref_name: &str,
) -> Result<Option<PublicationAcknowledgement>, StewardError> {
    let key = ack_key(prefix, remote_url, pond_id, ref_name);
    let Some(encoded) = control
        .raw_config_get(&key)
        .await?
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let acknowledgement: PublicationAcknowledgement =
        serde_json::from_str(&encoded).map_err(|error| {
            StewardError::ControlTable(format!(
                "decode structured publication acknowledgement {key}: {error}"
            ))
        })?;
    let _ = acknowledgement.state(remote_url, pond_id, ref_name)?;
    Ok(Some(acknowledgement))
}

async fn write_ack(
    control: &mut ControlTable,
    prefix: &str,
    remote_url: &str,
    state: &PublicationState,
    write_pull_identity: bool,
) -> Result<(), StewardError> {
    let acknowledgement = PublicationAcknowledgement::from_state(remote_url, state);
    let encoded = serde_json::to_string(&acknowledgement).map_err(|error| {
        StewardError::ControlTable(format!("encode publication acknowledgement: {error}"))
    })?;
    let key = ack_key(prefix, remote_url, state.pond_id, &state.ref_name);
    let control_path = control.inner().path().to_path_buf();
    let lock_meta = PondTxnMetadata::new(
        0,
        PondUserMetadata::new(vec!["publication-ack".to_string(), "write".to_string()]),
    );
    let _lock = crate::write_lock::WriteLockGuard::try_acquire_named(
        &control_path,
        "publication-ack.lock",
        &lock_meta,
    )?;
    let mut fresh = ControlTable::open(&control_path).await?;
    let mut write_value = true;
    if let Some((_, current)) = fresh.raw_config_entry(&key).await?
        && !current.is_empty()
    {
        let current = decode_ack(&key, &current)?;
        let current_state = current.state(remote_url, state.pond_id, &state.ref_name)?;
        if current_state.generation > state.generation {
            return Err(StewardError::ControlTable(format!(
                "refusing stale publication acknowledgement generation {} after durable \
                 generation {} for remote {remote_url:?} ref {:?}",
                state.generation, current_state.generation, state.ref_name
            )));
        }
        if current_state.generation == state.generation {
            if same_publication_identity(&current_state, state) {
                write_value = false;
            } else {
                return Err(StewardError::ControlTable(format!(
                    "conflicting publication acknowledgement at generation {} for remote \
                     {remote_url:?} ref {:?}",
                    state.generation, state.ref_name
                )));
            }
        }
    }
    if write_value {
        set_raw_value(&mut fresh, &key, &encoded).await?;
    }
    if write_pull_identity {
        let identity_key = pull_identity_key(remote_url, &state.ref_name);
        let identity = state.pond_id.to_string();
        if let Some((_, existing)) = fresh.raw_config_entry(&identity_key).await?
            && !existing.is_empty()
            && existing != identity
        {
            return Err(StewardError::ControlTable(format!(
                "consumer acknowledgement identity for remote {remote_url:?} ref {:?} is \
                 already bound to pond {existing}; clear it explicitly before binding pond \
                 {identity}",
                state.ref_name
            )));
        }
        set_raw_value(&mut fresh, &identity_key, &identity).await?;
    }
    *control = fresh;
    Ok(())
}

fn decode_ack(key: &str, encoded: &str) -> Result<PublicationAcknowledgement, StewardError> {
    serde_json::from_str(encoded).map_err(|error| {
        StewardError::ControlTable(format!(
            "decode structured publication acknowledgement {key}: {error}"
        ))
    })
}

async fn set_raw_value(
    control: &mut ControlTable,
    key: &str,
    value: &str,
) -> Result<(), StewardError> {
    let current = control.raw_config_entry(key).await?;
    if current
        .as_ref()
        .is_some_and(|(_, current)| current == value)
    {
        return Ok(());
    }
    control
        .raw_config_set_after(
            key,
            value,
            current.map_or(i64::MIN, |(timestamp, _)| timestamp),
        )
        .await
}

fn ack_key(prefix: &str, remote_url: &str, pond_id: Uuid, ref_name: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    let _ = hasher.update(remote_url.as_bytes());
    let _ = hasher.update(&[0]);
    let _ = hasher.update(pond_id.as_bytes());
    let _ = hasher.update(&[0]);
    let _ = hasher.update(ref_name.as_bytes());
    format!("{prefix}{}", hasher.finalize().to_hex())
}

fn pull_identity_key(remote_url: &str, ref_name: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    let _ = hasher.update(remote_url.as_bytes());
    let _ = hasher.update(&[0]);
    let _ = hasher.update(ref_name.as_bytes());
    format!("{PULL_IDENTITY_PREFIX}{}", hasher.finalize().to_hex())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acknowledgement_fails_closed_on_identity_mismatch() {
        let pond = Uuid::new_v4();
        let state = PublicationState::new(
            pond,
            "main",
            ObjectHash::of_bytes(b"tip"),
            ObjectHash::of_bytes(b"manifest"),
            ObjectHash::of_bytes(b"record"),
            1,
            1,
        )
        .unwrap();
        let acknowledgement = PublicationAcknowledgement::from_state("gs://example/remote", &state);
        assert!(
            acknowledgement
                .state("gs://example/other", pond, "main")
                .is_err()
        );
        assert_eq!(
            acknowledgement
                .state("gs://example/remote", pond, "main")
                .unwrap()
                .snapshot_tip,
            state.snapshot_tip
        );
    }

    #[tokio::test]
    async fn pull_identity_index_recovers_the_remote_pond_key() {
        let directory = tempfile::tempdir().unwrap();
        let local = crate::PondMetadata::default();
        let mut control = ControlTable::create(directory.path(), &local)
            .await
            .unwrap();
        let remote_pond = Uuid::new_v4();
        assert_ne!(remote_pond, control.pond_id_uuid());
        let state = PublicationState::new(
            remote_pond,
            "main",
            ObjectHash::of_bytes(b"tip"),
            ObjectHash::of_bytes(b"manifest"),
            ObjectHash::of_bytes(b"record"),
            1,
            1,
        )
        .unwrap();
        write_pull_ack(&mut control, "gs://example/remote", &state)
            .await
            .unwrap();

        let recovered = read_pull_ack_for_remote(&control, "gs://example/remote", "main")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            recovered
                .state("gs://example/remote", remote_pond, "main")
                .unwrap()
                .snapshot_tip,
            state.snapshot_tip
        );

        let version = control.table().version();
        write_pull_ack(&mut control, "gs://example/remote", &state)
            .await
            .unwrap();
        assert_eq!(
            control.table().version(),
            version,
            "rewriting an identical pull acknowledgement must be a control-table no-op"
        );
    }

    #[tokio::test]
    async fn stale_control_handle_cannot_roll_back_acknowledgement_generation() {
        let directory = tempfile::tempdir().unwrap();
        let local = crate::PondMetadata::default();
        let mut older = ControlTable::create(directory.path(), &local)
            .await
            .unwrap();
        let mut newer = ControlTable::open(directory.path()).await.unwrap();
        let pond = older.pond_id_uuid();
        let state = |generation, label: &[u8]| {
            PublicationState::new(
                pond,
                "main",
                ObjectHash::of_bytes(label),
                ObjectHash::of_bytes(&[label, b"-manifest"].concat()),
                ObjectHash::of_bytes(&[label, b"-record"].concat()),
                generation,
                generation,
            )
            .unwrap()
        };
        let first = state(1, b"first");
        let second = state(2, b"second");

        write_push_ack(&mut newer, "file:///remote", &second)
            .await
            .unwrap();
        let error = write_push_ack(&mut older, "file:///remote", &first)
            .await
            .expect_err("stale handle must not append an older generation");
        assert!(error.to_string().contains("refusing stale"));

        let reopened = ControlTable::open(directory.path()).await.unwrap();
        let durable = read_push_ack(&reopened, "file:///remote", pond, "main")
            .await
            .unwrap()
            .unwrap()
            .state("file:///remote", pond, "main")
            .unwrap();
        assert!(same_publication_identity(&durable, &second));
    }

    #[tokio::test]
    async fn equal_generation_with_conflicting_identity_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let local = crate::PondMetadata::default();
        let mut control = ControlTable::create(directory.path(), &local)
            .await
            .unwrap();
        let pond = control.pond_id_uuid();
        let first = PublicationState::new(
            pond,
            "main",
            ObjectHash::of_bytes(b"first"),
            ObjectHash::of_bytes(b"first-manifest"),
            ObjectHash::of_bytes(b"first-record"),
            7,
            1,
        )
        .unwrap();
        let conflict = PublicationState::new(
            pond,
            "main",
            ObjectHash::of_bytes(b"conflict"),
            ObjectHash::of_bytes(b"conflict-manifest"),
            ObjectHash::of_bytes(b"conflict-record"),
            7,
            2,
        )
        .unwrap();
        write_push_ack(&mut control, "file:///remote", &first)
            .await
            .unwrap();
        let error = write_push_ack(&mut control, "file:///remote", &conflict)
            .await
            .expect_err("equal generation conflict must fail");
        assert!(error.to_string().contains("conflicting"));
    }

    #[tokio::test]
    async fn concurrent_ack_writers_converge_on_the_newer_generation() {
        let directory = tempfile::tempdir().unwrap();
        let local = crate::PondMetadata::default();
        let mut first_handle = ControlTable::create(directory.path(), &local)
            .await
            .unwrap();
        let mut second_handle = ControlTable::open(directory.path()).await.unwrap();
        let pond = first_handle.pond_id_uuid();
        let older = PublicationState::new(
            pond,
            "main",
            ObjectHash::of_bytes(b"older"),
            ObjectHash::of_bytes(b"older-manifest"),
            ObjectHash::of_bytes(b"older-record"),
            1,
            1,
        )
        .unwrap();
        let newer = PublicationState::new(
            pond,
            "main",
            ObjectHash::of_bytes(b"newer"),
            ObjectHash::of_bytes(b"newer-manifest"),
            ObjectHash::of_bytes(b"newer-record"),
            2,
            2,
        )
        .unwrap();

        let (older_result, newer_result) = tokio::join!(
            write_push_ack(&mut first_handle, "file:///remote", &older),
            write_push_ack(&mut second_handle, "file:///remote", &newer),
        );
        assert!(
            older_result.is_ok() || newer_result.is_ok(),
            "one concurrent writer must acquire the acknowledgement lock"
        );
        write_push_ack(&mut second_handle, "file:///remote", &newer)
            .await
            .expect("retrying the newer generation must converge");

        let reopened = ControlTable::open(directory.path()).await.unwrap();
        let durable = read_push_ack(&reopened, "file:///remote", pond, "main")
            .await
            .unwrap()
            .unwrap()
            .state("file:///remote", pond, "main")
            .unwrap();
        assert!(same_publication_identity(&durable, &newer));
    }
}
