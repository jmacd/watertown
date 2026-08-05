// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Reading a storage profile out of the pond and turning it into the options
//! and handler registration a remote needs.
//!
//! See `docs/storage-profile-design.md`.  This is the consumer half of the
//! `storage-*` factories, exactly as [`crate::limiter`] is the consumer half
//! of `rate-limit`.
//!
//! # Why this exists (Decision A8)
//!
//! Before profiles, the URL scheme drove every storage decision by string
//! comparison in eight places, and one of them --
//! [`crate::RemoteAttachment::to_storage_options`] -- returns an **empty map**
//! for any URL that is not `s3://`.  A non-S3 attachment would therefore
//! discard every credential it was given and fail as an opaque authentication
//! error with the configuration looking correct.
//!
//! A profile knows its provider, which is the thing a URL prefix was being
//! made to approximate.  [`ResolvedStorage`] answers both questions -- which
//! handlers to register, and which options to build -- without inspecting the
//! URL at all.
//!
//! # Resolution happens once, at the call site (Decision A5)
//!
//! Reading a profile needs the pond, and the transfer path must stay
//! synchronous and `Ship`-free: making option-building `async` would thread
//! `Ship` through everything and risk the async-recursion cycle the limiter
//! hit (`guard.commit` -> `run_post_commit_remotes` -> open -> `begin_read` ->
//! `commit`).  So callers [`ResolvedStorage::open`] once, up front, and then
//! use the pure methods.  This mirrors how a `LimiterSet` is bound.

use crate::PondUserMetadata;
use crate::Ship;
use provider::factory::storage_minio::StorageMinioConfig;
use std::collections::HashMap;
use std::fmt;

/// Why a storage profile could not be used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageProfileError {
    /// The node does not exist, or could not be read.
    NotFound { path: String, reason: String },
    /// The node exists but is not a usable storage profile.
    NotAProfile { path: String, reason: String },
    /// The profile's provider does not serve the attachment's URL scheme.
    SchemeMismatch {
        path: String,
        kind: &'static str,
        url: String,
    },
    /// A `${env:...}` reference could not be resolved on this replica.
    Unresolvable { path: String, reason: String },
    /// The legacy inline connection path failed.
    Inline { reason: String },
}

impl fmt::Display for StorageProfileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound { path, reason } => write!(
                f,
                "storage profile `{path}` not found: {reason}. A remote naming a profile that \
                 does not exist is refused rather than run without credentials."
            ),
            Self::NotAProfile { path, reason } => {
                write!(f, "`{path}` is not a usable storage profile: {reason}")
            }
            Self::SchemeMismatch { path, kind, url } => write!(
                f,
                "storage profile `{path}` is a `{kind}` profile, which does not serve the URL \
                 `{url}`"
            ),
            Self::Unresolvable { path, reason } => write!(
                f,
                "storage profile `{path}` could not be resolved in this environment: {reason}"
            ),
            Self::Inline { reason } => write!(f, "{reason}"),
        }
    }
}

impl std::error::Error for StorageProfileError {}

/// A storage profile read from the pond.
///
/// Holds the profile **document**, not resolved credentials: `${env:...}`
/// references are resolved by [`Self::to_storage_options`] at use time, per
/// replica (Decision A6).  Keeping the unresolved form here is what lets a
/// replicated attachment authenticate as whichever host is using it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedStorage {
    /// A `storage-minio` profile.
    Minio {
        path: String,
        config: Box<StorageMinioConfig>,
    },
}

impl ResolvedStorage {
    /// Read and parse the profile node at `path`.
    pub async fn open(ship: &mut Ship, path: &str) -> Result<Self, StorageProfileError> {
        let bytes = read_node_bytes(ship, path).await?;
        Self::from_bytes(path, &bytes)
    }

    /// Parse an already-read profile document.
    ///
    /// Split out from [`Self::open`] so the parse and scheme rules can be
    /// exercised without a pond.
    pub fn from_bytes(path: &str, bytes: &[u8]) -> Result<Self, StorageProfileError> {
        // Only one kind exists today.  When `storage-azure` lands it is
        // dispatched here, on the document's shape -- the node does not record
        // which factory produced it, so each kind's parse must be strict
        // enough to reject the others.  `deny_unknown_fields` on every config
        // is what makes that safe.
        match provider::factory::storage_minio::config_from_bytes(bytes) {
            Ok(config) => Ok(Self::Minio {
                path: path.to_string(),
                config: Box::new(config),
            }),
            Err(reason) => Err(StorageProfileError::NotAProfile {
                path: path.to_string(),
                reason,
            }),
        }
    }

    /// The profile's kind, as written in `pond apply`.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Minio { .. } => "storage-minio",
        }
    }

    /// The pond path this profile was read from.
    pub fn path(&self) -> &str {
        match self {
            Self::Minio { path, .. } => path,
        }
    }

    /// A one-line summary for `pond status`.  Never includes a credential,
    /// resolved or otherwise.
    pub fn describe(&self) -> String {
        match self {
            Self::Minio { config, .. } => format!("minio, {}", config.endpoint),
        }
    }

    /// Whether this provider serves `url`.
    ///
    /// Checked at attach time so a mismatched pairing is a configuration
    /// error, not a confusing failure on the first push.
    pub fn serves_scheme(&self, url: &str) -> bool {
        match self {
            Self::Minio { .. } => url.starts_with("s3://") || url.starts_with("s3a://"),
        }
    }

    /// Refuse unless this profile serves `url`.
    pub fn check_scheme(&self, url: &str) -> Result<(), StorageProfileError> {
        if self.serves_scheme(url) {
            return Ok(());
        }
        Err(StorageProfileError::SchemeMismatch {
            path: self.path().to_string(),
            kind: self.kind(),
            url: url.to_string(),
        })
    }

    /// Register this provider's delta-rs handlers.
    ///
    /// Replaces the `url.starts_with("s3://")` tests that gated
    /// `register_s3_handlers` at seven call sites (Decision A8).  Idempotent,
    /// as the underlying registration is.
    pub fn register_handlers(&self) {
        match self {
            Self::Minio { .. } => sync_store::register_s3_handlers(),
        }
    }

    /// The `object_store` options for this profile, with `${env:...}`
    /// references resolved in the **current** process environment.
    pub fn to_storage_options(&self) -> Result<HashMap<String, String>, StorageProfileError> {
        match self {
            Self::Minio { path, config } => {
                config
                    .to_storage_options()
                    .map_err(|reason| StorageProfileError::Unresolvable {
                        path: path.clone(),
                        reason,
                    })
            }
        }
    }
}

/// Read a node's bytes through a short-lived read transaction.
///
/// Mirrors `limiter::read_node_bytes`; the transaction is closed either way,
/// since its outcome does not change the profile's.
/// Read a profile node's **raw**, pre-expansion config.
///
/// Deliberately not the node's file content.  `FactoryRegistry::create_file`
/// env-expands a stored config before the factory builds the node, so the
/// rendered content holds *resolved* values -- exactly what a profile must not
/// hand to a consumer.  Binding therefore reads the stored config, where the
/// `${env:...}` references survive, so each replica resolves as itself
/// (Decision A6).
async fn read_node_bytes(ship: &mut Ship, path: &str) -> Result<Vec<u8>, StorageProfileError> {
    let meta = PondUserMetadata::new(vec!["internal".to_string(), "storage-open".to_string()]);
    let tx = ship
        .begin_read(&meta)
        .await
        .map_err(|e| StorageProfileError::NotFound {
            path: path.to_string(),
            reason: format!("begin read: {e}"),
        })?;

    let result = async {
        let root = tx.root().await.map_err(|e| StorageProfileError::NotFound {
            path: path.to_string(),
            reason: format!("cannot open pond root: {e}"),
        })?;

        let (_, lookup) =
            root.resolve_path(path)
                .await
                .map_err(|e| StorageProfileError::NotFound {
                    path: path.to_string(),
                    reason: e.to_string(),
                })?;

        let node = match lookup {
            tinyfs::Lookup::Found(node) => node,
            _ => {
                return Err(StorageProfileError::NotFound {
                    path: path.to_string(),
                    reason: "no such node".to_string(),
                });
            }
        };

        match tx.get_dynamic_node_config(node.id()).await {
            Ok(Some((_factory, config))) => Ok(config),
            Ok(None) => Err(StorageProfileError::NotAProfile {
                path: path.to_string(),
                reason: "not a factory node; a storage profile is created by \
                         `kind: mknod` with a `storage-*` factory"
                    .to_string(),
            }),
            Err(e) => Err(StorageProfileError::NotFound {
                path: path.to_string(),
                reason: format!("read config: {e}"),
            }),
        }
    }
    .await;

    // The read transaction must be closed either way; its outcome does not
    // change the profile's.
    let _ = tx.commit().await;
    result
}

/// Register the right handlers and build the storage options for
/// `attachment`, whichever authoring style it uses.
///
/// This is the single call that replaces the `url.starts_with("s3://")` test
/// plus `to_storage_options` pair repeated at every transfer site
/// (Decision A8).
///
/// - **With a profile:** the provider comes from the profile's kind, and the
///   URL is only *checked* against it, never used to infer it.
/// - **Without one:** exactly today's behavior, unchanged. The legacy path
///   stays S3-only on purpose; extending it would keep alive the scheme
///   sniffing that profiles exist to replace.
pub async fn prepare_storage(
    ship: &mut Ship,
    attachment: &crate::RemoteAttachment,
) -> Result<HashMap<String, String>, StorageProfileError> {
    if let Some(path) = attachment.storage.as_deref() {
        let profile = ResolvedStorage::open(ship, path).await?;
        profile.check_scheme(&attachment.url)?;
        profile.register_handlers();
        return profile.to_storage_options();
    }

    if attachment.url.starts_with("s3://") {
        sync_store::register_s3_handlers();
    }
    attachment
        .to_storage_options()
        .map_err(|e| StorageProfileError::Inline {
            reason: e.to_string(),
        })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    const DOC: &[u8] =
        b"endpoint: http://watershop:9000\naccess_key_id: ${env:PATH}\nsecret_access_key: ${env:PATH}\n";

    #[test]
    fn a_minio_document_parses() {
        let p = ResolvedStorage::from_bytes("/sys/storage/minio", DOC).expect("parse");
        assert_eq!(p.kind(), "storage-minio");
        assert_eq!(p.path(), "/sys/storage/minio");
        assert!(p.describe().contains("watershop:9000"));
    }

    /// A profile must never leak a credential into an operator-facing string,
    /// which is what `pond status` prints.
    #[test]
    fn describe_names_no_credential() {
        let p = ResolvedStorage::from_bytes("/sys/storage/minio", DOC).expect("parse");
        let d = p.describe();
        assert!(!d.contains("access_key"), "{d}");
        assert!(!d.contains("secret"), "{d}");
        assert!(!d.contains("${env"), "{d}");
    }

    /// The whole point of A8: which handlers and which options come from the
    /// profile, and a mismatched URL is refused rather than half-working.
    #[test]
    fn scheme_compatibility_is_checked() {
        let p = ResolvedStorage::from_bytes("/sys/storage/minio", DOC).expect("parse");
        p.check_scheme("s3://bucket").expect("s3 is served");
        p.check_scheme("s3a://bucket").expect("s3a is served");

        for url in ["az://container", "file:///tmp/x", "pond:///tmp/x"] {
            assert!(
                matches!(
                    p.check_scheme(url),
                    Err(StorageProfileError::SchemeMismatch { .. })
                ),
                "{url} must be refused"
            );
        }
    }

    #[test]
    fn a_malformed_document_is_not_a_profile() {
        let err = ResolvedStorage::from_bytes("/sys/storage/x", b"not: a: profile\n")
            .expect_err("must refuse");
        assert!(matches!(err, StorageProfileError::NotAProfile { .. }));
    }

    /// A profile that stores a literal credential must not be usable, even if
    /// it somehow got written -- the rule is enforced on the way in and on the
    /// way out.
    #[test]
    fn a_literal_credential_is_refused_on_read() {
        let err = ResolvedStorage::from_bytes(
            "/sys/storage/minio",
            b"endpoint: http://x:9000\naccess_key_id: ${env:PATH}\nsecret_access_key: hunter2\n",
        )
        .expect_err("must refuse");
        assert!(format!("{err}").contains("secret_access_key"), "{err}");
    }

    #[test]
    fn options_resolve_at_use_time() {
        let p = ResolvedStorage::from_bytes("/sys/storage/minio", DOC).expect("parse");
        let opts = p.to_storage_options().expect("resolve");
        let path = std::env::var("PATH").expect("PATH is set");
        assert_eq!(
            opts.get("secret_access_key").map(String::as_str),
            Some(path.as_str())
        );
        assert_eq!(opts.get("allow_http").map(String::as_str), Some("true"));
    }

    #[test]
    fn an_unresolvable_reference_reports_the_profile_path() {
        let p = ResolvedStorage::from_bytes(
            "/sys/storage/minio",
            b"endpoint: http://x:9000\naccess_key_id: ${env:NOT_SET_XYZZY_A}\nsecret_access_key: ${env:NOT_SET_XYZZY_B}\n",
        )
        .expect("parse");
        let err = p.to_storage_options().expect_err("must fail");
        assert!(matches!(err, StorageProfileError::Unresolvable { .. }));
        assert!(format!("{err}").contains("/sys/storage/minio"), "{err}");
    }
}
