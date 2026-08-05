// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `storage-minio` factory: declares how to reach a MinIO deployment.
//!
//! See `docs/storage-profile-design.md`.  Like `rate-limit`, a storage profile
//! is a **leaf config node**: nothing to compute, nothing to execute.  It
//! exists so that "this is the MinIO on watershop" is stated once, in the
//! pond, and referenced by path from every remote that talks to it.
//!
//! ```yaml
//! kind: mknod
//! metadata:
//!   path: /sys/storage/minio
//! spec:
//!   factory: storage-minio
//!   config:
//!     endpoint: ${env:S3_ENDPOINT}
//!     access_key_id: ${env:S3_ACCESS_KEY}
//!     secret_access_key: ${env:S3_SECRET_KEY}
//! ```
//!
//! # What is deliberately absent
//!
//! There is no `allow_http` field (Decision A2/A3).  A `storage-minio` profile
//! permits plain HTTP **by definition** -- that is the entire reason the kind
//! exists as distinct from a hypothetical `storage-s3`.  An operator running
//! MinIO behind TLS writes an `https://` endpoint; permitting HTTP does not
//! require using it.  This is where the old `S3_ALLOW_HTTP` environment
//! variable went: it stopped being a value and became the choice of `kind`.
//!
//! Path-style addressing is likewise a property of the kind rather than, as in
//! the inline attachment path, a side effect of some other field happening to
//! be non-empty (`steward::remote_config::RemoteAttachment::to_storage_options`).
//!
//! # Credentials are references, never values (Decision A1)
//!
//! Every credential field MUST be an `${env:...}` reference, rejected at
//! `pond apply` time otherwise.  A profile node is replicated exactly as a
//! remote attachment is, and it is *more* inviting to inspect -- a node named
//! `/sys/storage/minio` is something an operator will `pond cat`.  Requiring
//! references is what makes that safe, and it is why [`render`] does not need
//! to redact anything: the stored document contains no secrets to redact.
//!
//! That matters for a second reason.  Consumers bind a profile by reading this
//! node's **bytes** (`steward::storage_profile`), so `render` must produce
//! canonical YAML that parses back into [`StorageMinioConfig`].  A redacting
//! renderer would break that round trip, and a node whose displayed form
//! differed from its enforced form is precisely the drift `rate_limit::render`
//! was written to avoid.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use tinyfs::FileHandle;
use tinyfs::Result as TinyFSResult;
use tinyfs::ResultExt;

/// MinIO's own default, and what `object_store` signs with when a region is
/// not supplied.  MinIO ignores the region for placement but SigV4 requires
/// one, so this is a genuine default rather than a guess.
pub const DEFAULT_REGION: &str = "us-east-1";

// ============================================================================
// Configuration
// ============================================================================

/// On-disk config for a `storage-minio` node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageMinioConfig {
    /// MinIO endpoint URL, e.g. `http://watershop:9000`.  Required: a MinIO
    /// without an endpoint is not a MinIO.
    pub endpoint: String,

    /// Region used for SigV4 signing.  Defaults to [`DEFAULT_REGION`], which
    /// [`render`] makes explicit so `pond cat` shows what is in effect.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,

    /// Access key id.  Must be an `${env:...}` reference (Decision A1).
    pub access_key_id: String,

    /// Secret access key.  Must be an `${env:...}` reference (Decision A1).
    pub secret_access_key: String,
}

/// Why a `storage-minio` config was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageMinioError {
    /// A required field was empty.
    Missing { field: &'static str },
    /// The endpoint is not a parseable URL, or not http/https.
    BadEndpoint { endpoint: String, reason: String },
    /// A credential field was a literal rather than an `${env:...}` reference.
    LiteralCredential { field: &'static str },
}

impl fmt::Display for StorageMinioError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing { field } => write!(f, "`{field}` is required and must not be empty"),
            Self::BadEndpoint { endpoint, reason } => {
                write!(f, "invalid endpoint `{endpoint}`: {reason}")
            }
            Self::LiteralCredential { field } => write!(
                f,
                "`{field}` must be an environment reference such as ${{env:S3_SECRET_KEY}}, \
                 not a literal: a storage profile is replicated to every backup, so a literal \
                 credential would be exposed on all replicas. Set it in the environment and \
                 reference it here."
            ),
        }
    }
}

impl std::error::Error for StorageMinioError {}

impl StorageMinioConfig {
    /// The region actually used, applying [`DEFAULT_REGION`].
    pub fn region(&self) -> &str {
        match self.region.as_deref() {
            Some(r) if !r.is_empty() => r,
            _ => DEFAULT_REGION,
        }
    }

    /// Check every rule this kind enforces.
    ///
    /// Used on the config **as stored in the pond**, where both halves apply.
    /// The two creation-time entry points each check one half, because neither
    /// sees a document that satisfies both: see [`Self::validate_structure`]
    /// and [`Self::validate_references`].
    pub fn validate(&self) -> Result<(), StorageMinioError> {
        self.validate_structure()?;
        self.validate_references()
    }

    /// Check the rules that concern *values*: required fields, and a
    /// well-formed endpoint.
    ///
    /// This runs on the **env-expanded** config, which is the only form in
    /// which the values are knowable.
    pub fn validate_structure(&self) -> Result<(), StorageMinioError> {
        if self.endpoint.trim().is_empty() {
            return Err(StorageMinioError::Missing { field: "endpoint" });
        }
        if self.access_key_id.trim().is_empty() {
            return Err(StorageMinioError::Missing {
                field: "access_key_id",
            });
        }
        if self.secret_access_key.trim().is_empty() {
            return Err(StorageMinioError::Missing {
                field: "secret_access_key",
            });
        }

        // An endpoint containing an unresolved reference cannot be parsed
        // here, and must not be: resolution is per-replica at use time
        // (Decision A6).  Only a literal endpoint is checked.
        if !utilities::env_substitution::has_env_refs(&self.endpoint) {
            let parsed =
                url::Url::parse(&self.endpoint).map_err(|e| StorageMinioError::BadEndpoint {
                    endpoint: self.endpoint.clone(),
                    reason: e.to_string(),
                })?;
            if !matches!(parsed.scheme(), "http" | "https") {
                return Err(StorageMinioError::BadEndpoint {
                    endpoint: self.endpoint.clone(),
                    reason: format!("scheme must be http or https, got `{}`", parsed.scheme()),
                });
            }
        }

        Ok(())
    }

    /// Check that every credential is still an `${env:...}` reference
    /// (Decision A1).
    ///
    /// This runs on the **raw** config, before expansion -- expansion is
    /// precisely what turns a reference into a literal, so after it the
    /// distinction this rule is about no longer exists.
    pub fn validate_references(&self) -> Result<(), StorageMinioError> {
        for (field, value) in [
            ("access_key_id", &self.access_key_id),
            ("secret_access_key", &self.secret_access_key),
        ] {
            if !utilities::env_substitution::has_env_refs(value) {
                return Err(StorageMinioError::LiteralCredential { field });
            }
        }
        Ok(())
    }

    /// The `object_store` options this profile implies, with `${env:...}`
    /// references resolved.
    ///
    /// Resolution happens **here**, at use time, so each replica
    /// authenticates as itself (Decision A6).
    pub fn to_storage_options(&self) -> Result<HashMap<String, String>, String> {
        let resolve = |v: &str| {
            utilities::env_substitution::substitute_env_vars(v).map_err(|e| e.to_string())
        };

        let mut out = HashMap::new();
        let _ = out.insert("region".to_string(), resolve(self.region())?);
        let _ = out.insert("endpoint".to_string(), resolve(&self.endpoint)?);
        let _ = out.insert("access_key_id".to_string(), resolve(&self.access_key_id)?);
        let _ = out.insert(
            "secret_access_key".to_string(),
            resolve(&self.secret_access_key)?,
        );
        // Both are properties of the kind, not of any field's emptiness.
        let _ = out.insert("allow_http".to_string(), "true".to_string());
        let _ = out.insert(
            "virtual_hosted_style_request".to_string(),
            "false".to_string(),
        );
        Ok(out)
    }
}

/// Parse a `storage-minio` node's stored config bytes.
///
/// Shared by the factory and by `steward::storage_profile`, so both agree on
/// exactly one interpretation of a node's config -- the same discipline as
/// `rate_limit::spec_from_config_bytes`.
pub fn config_from_bytes(config: &[u8]) -> Result<StorageMinioConfig, String> {
    let text = std::str::from_utf8(config).map_err(|e| format!("invalid UTF-8: {e}"))?;
    let cfg: StorageMinioConfig =
        serde_yaml::from_str(text).map_err(|e| format!("invalid storage-minio config: {e}"))?;
    cfg.validate().map_err(|e| e.to_string())?;
    Ok(cfg)
}

// ============================================================================
// Factory
// ============================================================================

/// The placeholder shown in place of a credential value.
pub const REDACTED: &str = "<redacted>";

/// Render the profile as the node's readable content: endpoint and region as
/// configured, credentials replaced by [`REDACTED`].
///
/// Redaction is required, not cosmetic.  This runs on the **env-expanded**
/// config -- `FactoryRegistry::create_file` expands before the factory is
/// called -- so the values in hand are the resolved secrets.  Consumers do not
/// read this text; they bind from the raw stored config
/// (`steward::storage_profile`), which still holds the references.  So nothing
/// depends on this output parsing back, and it is free to be safe to print.
pub fn render(cfg: &StorageMinioConfig) -> Vec<u8> {
    let normalized = StorageMinioConfig {
        endpoint: cfg.endpoint.clone(),
        region: Some(cfg.region().to_string()),
        access_key_id: REDACTED.to_string(),
        secret_access_key: REDACTED.to_string(),
    };
    let body = serde_yaml::to_string(&normalized)
        .unwrap_or_else(|e| format!("# failed to render config: {e}\n"));
    format!(
        "# storage-minio: {} (plain HTTP permitted, path-style addressing)\n{}",
        cfg.endpoint, body
    )
    .into_bytes()
}

fn create_storage_minio_handle(
    config: Value,
    _context: crate::FactoryContext,
) -> TinyFSResult<FileHandle> {
    let cfg: StorageMinioConfig =
        serde_json::from_value(config).map_other_context("Invalid storage-minio config")?;
    // Structure only: `config` arrives expanded, so the reference rule is not
    // checkable here.  It is enforced at creation time on the raw form.
    cfg.validate_structure()
        .map_err(|e| tinyfs::Error::Other(format!("Invalid storage-minio config: {e}")))?;
    Ok(crate::ConfigFile::new(render(&cfg)).create_handle())
}

/// Validate the **env-expanded** config: structure only.
///
/// The credential-reference rule cannot be checked here, because expansion has
/// already replaced every reference with its value.  It is checked instead by
/// [`validate_storage_minio_raw_config`].
fn validate_storage_minio_config(config: &[u8]) -> TinyFSResult<Value> {
    let config_str = std::str::from_utf8(config).map_other_context("Invalid UTF-8")?;
    let cfg: StorageMinioConfig =
        serde_yaml::from_str(config_str).map_other_context("Invalid storage-minio config")?;
    cfg.validate_structure()
        .map_err(|e| tinyfs::Error::Other(format!("Invalid storage-minio config: {e}")))?;
    serde_json::to_value(&cfg).map_other_context("Failed to serialize storage-minio config")
}

/// Validate the **raw** config, which is the form that gets stored: every
/// credential must still be an `${env:...}` reference (Decision A1).
fn validate_storage_minio_raw_config(config: &[u8]) -> TinyFSResult<()> {
    let config_str = std::str::from_utf8(config).map_other_context("Invalid UTF-8")?;
    let cfg: StorageMinioConfig =
        serde_yaml::from_str(config_str).map_other_context("Invalid storage-minio config")?;
    cfg.validate_references()
        .map_err(|e| tinyfs::Error::Other(format!("Invalid storage-minio config: {e}")))
}

crate::register_dynamic_factory!(
    name: "storage-minio",
    description: "Declare how to reach a MinIO deployment (endpoint + credentials), referenced by path from remotes",
    file: create_storage_minio_handle,
    validate: validate_storage_minio_config,
    validate_raw: validate_storage_minio_raw_config
);

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> StorageMinioConfig {
        StorageMinioConfig {
            endpoint: "http://watershop:9000".to_string(),
            region: None,
            access_key_id: "${env:S3_ACCESS_KEY}".to_string(),
            secret_access_key: "${env:S3_SECRET_KEY}".to_string(),
        }
    }

    #[test]
    fn a_well_formed_profile_validates() {
        cfg().validate().expect("should validate");
    }

    /// Decision A1.  This is the rule the whole design leans on: because a
    /// stored profile can only contain references, `pond cat` is safe and
    /// `render` needs no redaction.
    #[test]
    fn literal_credentials_are_refused() {
        let mut c = cfg();
        c.secret_access_key = "hunter2".to_string();
        assert_eq!(
            c.validate(),
            Err(StorageMinioError::LiteralCredential {
                field: "secret_access_key"
            })
        );

        let mut c = cfg();
        c.access_key_id = "pondwriter".to_string();
        assert_eq!(
            c.validate(),
            Err(StorageMinioError::LiteralCredential {
                field: "access_key_id"
            })
        );
    }

    #[test]
    fn required_fields_are_required() {
        for (field, mutate) in [
            (
                "endpoint",
                (|c: &mut StorageMinioConfig| c.endpoint = String::new())
                    as fn(&mut StorageMinioConfig),
            ),
            ("access_key_id", |c: &mut StorageMinioConfig| {
                c.access_key_id = "   ".to_string()
            }),
            ("secret_access_key", |c: &mut StorageMinioConfig| {
                c.secret_access_key = String::new()
            }),
        ] {
            let mut c = cfg();
            mutate(&mut c);
            assert_eq!(c.validate(), Err(StorageMinioError::Missing { field }));
        }
    }

    #[test]
    fn a_non_http_endpoint_is_refused() {
        let mut c = cfg();
        c.endpoint = "s3://watershop".to_string();
        assert!(matches!(
            c.validate(),
            Err(StorageMinioError::BadEndpoint { .. })
        ));
    }

    /// An endpoint that is itself a reference cannot be parsed at apply time,
    /// and must not be: resolution is per-replica at use time (A6).
    #[test]
    fn a_referenced_endpoint_is_not_parsed_at_apply_time() {
        let mut c = cfg();
        c.endpoint = "${env:S3_ENDPOINT}".to_string();
        c.validate().expect("should defer to use time");
    }

    #[test]
    fn there_is_no_allow_http_field_to_get_wrong() {
        let err = serde_yaml::from_str::<StorageMinioConfig>(
            "endpoint: http://x:9000\naccess_key_id: ${env:A}\nsecret_access_key: ${env:B}\nallow_http: true\n",
        )
        .expect_err("allow_http must not be accepted");
        assert!(format!("{err}").contains("allow_http"), "{err}");
    }

    #[test]
    fn the_region_default_is_explicit_in_the_rendered_form() {
        let rendered = render(&cfg());
        let text = String::from_utf8(rendered).expect("utf8");
        assert!(text.contains("region: us-east-1"), "{text}");
    }

    /// `render` runs on the expanded config, so its input is the resolved
    /// secret.  Whatever else it shows, it must not show that.
    #[test]
    fn rendered_output_names_no_credential() {
        let resolved = StorageMinioConfig {
            endpoint: "http://watershop:9000".to_string(),
            region: None,
            access_key_id: "AKIAREALKEY".to_string(),
            secret_access_key: "s3cr3t-value".to_string(),
        };
        let text = String::from_utf8(render(&resolved)).expect("utf8");
        assert!(!text.contains("AKIAREALKEY"), "{text}");
        assert!(!text.contains("s3cr3t-value"), "{text}");
        assert!(text.contains(REDACTED), "{text}");
        // The non-secret connection facts stay visible: the point of the node
        // is to say where the pond talks to.
        assert!(text.contains("http://watershop:9000"), "{text}");
    }

    /// The redacted view must not be mistakable for a usable profile: feeding
    /// it back in fails the reference rule rather than silently authenticating
    /// as `<redacted>`.
    #[test]
    fn the_rendered_view_is_not_a_usable_profile() {
        let err = config_from_bytes(&render(&cfg())).expect_err("must not parse back");
        assert!(err.contains("access_key_id"), "{err}");
    }

    #[test]
    fn config_from_bytes_rejects_a_literal_credential() {
        let err = config_from_bytes(
            b"endpoint: http://x:9000\naccess_key_id: ${env:A}\nsecret_access_key: hunter2\n",
        )
        .expect_err("must reject");
        assert!(err.contains("secret_access_key"), "{err}");
    }

    /// Resolution happens at use time (A6), and the kind's fixed properties
    /// come from the kind rather than from another field being non-empty.
    ///
    /// Uses `PATH` rather than setting a variable: `std::env::set_var` is
    /// `unsafe` and this workspace denies `unsafe` blocks.
    #[test]
    fn storage_options_resolve_references_and_fix_the_kind_properties() {
        let path = std::env::var("PATH").expect("PATH is set");
        let c = StorageMinioConfig {
            endpoint: "http://watershop:9000".to_string(),
            region: None,
            access_key_id: "${env:PATH}".to_string(),
            secret_access_key: "${env:PATH}".to_string(),
        };
        let opts = c.to_storage_options().expect("resolve");
        assert_eq!(
            opts.get("access_key_id").map(String::as_str),
            Some(path.as_str())
        );
        assert_eq!(
            opts.get("secret_access_key").map(String::as_str),
            Some(path.as_str())
        );
        assert_eq!(opts.get("region").map(String::as_str), Some("us-east-1"));
        assert_eq!(
            opts.get("endpoint").map(String::as_str),
            Some("http://watershop:9000")
        );
        assert_eq!(opts.get("allow_http").map(String::as_str), Some("true"));
        assert_eq!(
            opts.get("virtual_hosted_style_request").map(String::as_str),
            Some("false")
        );
    }

    #[test]
    fn an_unresolvable_reference_is_an_error_not_an_empty_credential() {
        let c = StorageMinioConfig {
            endpoint: "http://x:9000".to_string(),
            region: None,
            access_key_id: "${env:DEFINITELY_NOT_SET_KEY_XYZZY}".to_string(),
            secret_access_key: "${env:DEFINITELY_NOT_SET_SECRET_XYZZY}".to_string(),
        };
        assert!(c.to_storage_options().is_err());
    }

    #[test]
    fn the_factory_is_registered_as_a_leaf_config_node() {
        let f = crate::FactoryRegistry::get_factory("storage-minio")
            .expect("storage-minio factory registered");
        assert!(f.create_file.is_some());
        // Nothing to execute, no directory to build -- as `rate-limit` is.
        assert!(f.create_directory.is_none());
        assert!(f.execute.is_none());
    }

    #[test]
    fn the_factory_name_conflicts_with_nothing() {
        assert!(crate::SchemeRegistry::find_conflicts().is_empty());
    }

    #[test]
    fn factory_validation_rejects_a_profile_missing_credentials() {
        assert!(
            validate_storage_minio_config(
                b"endpoint: http://x:9000\naccess_key_id: ${env:A}\nsecret_access_key: ${env:B}\n"
            )
            .is_ok()
        );
        assert!(validate_storage_minio_config(b"endpoint: http://x:9000\n").is_err());
    }
}
