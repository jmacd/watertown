// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! D4 remote attachment data types -- the on-disk YAML schema (`/sys/remotes/<name>`)
//! and the per-pond runtime mode stored in the control table's raw_config map.
//!
//! These types live in [`crate::steward`] rather than `crates/cmd` because
//! both the CLI verbs (`pond remote add/list`, `pond push/pull`) AND the
//! post-commit auto-push dispatcher (in [`crate::guard`]) need to read
//! attachment YAML and dispatch by mode.

use provider::factory::rate_limit::LimitUnit;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use thiserror::Error;

/// Errors from parsing or interpreting remote-related metadata.
#[derive(Debug, Error)]
pub enum RemoteConfigError {
    #[error("invalid remote mode `{0}` (expected `push`, `pull`, or `both`)")]
    InvalidMode(String),

    #[error("remote attachment YAML parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("environment variable substitution failed in remote config: {0}")]
    EnvSubst(String),

    #[error(
        "remote attachment `limits` has unknown dimension key `{key}`: {reason}. \
         The key names the unit this transfer spends and is checked against the \
         limiter node's configured `unit`."
    )]
    InvalidLimitDimension { key: String, reason: String },

    #[error("remote attachment `limits.{key}` must name a pond path, got `{value}`")]
    InvalidLimitPath { key: String, value: String },

    #[error("remote attachment `storage` must name a pond path, got `{value}`")]
    InvalidStoragePath { value: String },

    #[error(
        "remote attachment names storage profile `{path}` but also sets inline connection          field(s): {fields}. A profile replaces them; keeping both would leave it ambiguous          which storage is actually in use. Remove the inline field(s), or drop `storage`."
    )]
    StorageConflict { path: String, fields: String },
}

/// Portable, on-disk YAML config for one remote attachment.  Stored at
/// `/sys/remotes/<name>` and serialized as YAML.
///
/// Credential fields (`access_key_id`, `secret_access_key`) hold
/// `${env:VAR}` references rather than literal secrets (enforced for
/// `secret_access_key` at `pond remote add` time).  The reference text is
/// what gets replicated to a backup -- the secret itself is resolved from
/// the local process environment per replica at use time (see
/// [`RemoteAttachment::to_storage_options`]).  This config carries no
/// per-pond watermarks.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteAttachment {
    /// Canonical remote URL: `file:///path` or `s3://bucket/prefix`.
    pub url: String,

    /// AWS region (S3 only; ignored for `file://`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub region: String,

    /// S3 access key id (S3 only).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub access_key_id: String,

    /// S3 secret access key (S3 only).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub secret_access_key: String,

    /// Custom S3 endpoint (e.g., MinIO, R2).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub endpoint: String,

    /// Allow plain HTTP (required for local MinIO).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_http: bool,

    /// Limiters governing transfers to this remote, keyed by the unit the
    /// push/pull path spends: `bytes` and/or `ops`.  Each value is the pond
    /// path of a `rate-limit` factory node, whose configured `unit` must agree
    /// with the key (Decision L7/L10).
    ///
    /// **The key is the caller's declaration, written by the operator.**
    /// `limits.bytes` binds as [`LimitUnit::Bytes`] and the node's `unit:` is
    /// verified against it, so a limiter node retuned from `MiB/day` to
    /// `iops/second` is rejected rather than silently charged in the wrong
    /// dimension.
    ///
    /// An absent key means that dimension is ungoverned; an empty map is
    /// today's unlimited behavior and serializes away entirely, so existing
    /// `/sys/remotes/<name>` documents are byte-identical.
    ///
    /// ```yaml
    /// limits:
    ///   bytes: /sys/limits/backup-bytes
    ///   ops:   /sys/limits/backup-ops
    /// ```
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub limits: BTreeMap<String, String>,

    /// Storage profile describing *how* to reach [`Self::url`], as the pond
    /// path of a `storage-*` factory node (Decision A2).
    ///
    /// Mutually exclusive with the inline connection fields above, and the
    /// conflict is a hard error rather than a precedence rule (Decision A4):
    /// an attachment that names a profile while still carrying a stale inline
    /// endpoint has no unambiguous intent, and silently preferring one would
    /// produce a working pond talking to the wrong storage.
    ///
    /// Absent by default and serializes away entirely, so every existing
    /// `/sys/remotes/<name>` document stays byte-identical.
    ///
    /// ```yaml
    /// url: s3://water-staging
    /// storage: /sys/storage/minio
    /// ```
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<String>,
}

impl RemoteAttachment {
    /// Resolve a single config field, expanding any `${env:VAR}` references
    /// against the local process environment.  Plain literal values (no
    /// `${env:` marker) pass through untouched, so legacy configs that
    /// predate the env-reference model are unaffected.
    fn resolve_field(value: &str) -> Result<String, RemoteConfigError> {
        if utilities::env_substitution::has_env_refs(value) {
            utilities::env_substitution::substitute_env_vars(value)
                .map_err(|e| RemoteConfigError::EnvSubst(e.to_string()))
        } else {
            Ok(value.to_string())
        }
    }

    /// Build the storage options map passed to `Remote::open_at_url`.
    /// Returns an empty map for `file://` URLs.
    ///
    /// `${env:VAR}` references in the credential/endpoint fields are
    /// expanded here, at use time, against the local environment.  This is
    /// why secrets need never be persisted: the YAML stores `${env:VAR}`,
    /// the live process supplies the value.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteConfigError::EnvSubst`] when a `${env:VAR}` reference
    /// names a variable that is unset (and provides no `:-default`), rather
    /// than silently forwarding the unresolved placeholder to S3.
    pub fn to_storage_options(&self) -> Result<HashMap<String, String>, RemoteConfigError> {
        let mut out = HashMap::new();
        if self.url.starts_with("s3://") {
            if !self.region.is_empty() {
                let _ = out.insert("region".to_string(), Self::resolve_field(&self.region)?);
            }
            if !self.access_key_id.is_empty() {
                let _ = out.insert(
                    "access_key_id".to_string(),
                    Self::resolve_field(&self.access_key_id)?,
                );
            }
            if !self.secret_access_key.is_empty() {
                let _ = out.insert(
                    "secret_access_key".to_string(),
                    Self::resolve_field(&self.secret_access_key)?,
                );
            }
            if !self.endpoint.is_empty() {
                let _ = out.insert("endpoint".to_string(), Self::resolve_field(&self.endpoint)?);
                let _ = out.insert(
                    "virtual_hosted_style_request".to_string(),
                    "false".to_string(),
                );
            }
            if self.allow_http {
                let _ = out.insert("allow_http".to_string(), "true".to_string());
            }
        }
        Ok(out)
    }

    /// Parse from raw YAML bytes (as stored under `/sys/remotes/<name>`).
    ///
    /// The `limits` map is validated here rather than at use time, so a
    /// misspelled dimension (`limits: {byte: ...}`) is a loud configuration
    /// error at attach and at read, never a silently unenforced policy.
    pub fn from_yaml_bytes(bytes: &[u8]) -> Result<Self, RemoteConfigError> {
        let parsed: Self = serde_yaml::from_slice(bytes)?;
        let _ = parsed.resolved_limits()?;
        parsed.check_storage_profile()?;
        Ok(parsed)
    }

    /// Names of the inline connection fields this attachment sets.
    ///
    /// Used to enforce Decision A4; also what an operator needs listed back
    /// when the conflict is reported.
    fn inline_connection_fields(&self) -> Vec<&'static str> {
        let mut set = Vec::new();
        if !self.region.is_empty() {
            set.push("region");
        }
        if !self.access_key_id.is_empty() {
            set.push("access_key_id");
        }
        if !self.secret_access_key.is_empty() {
            set.push("secret_access_key");
        }
        if !self.endpoint.is_empty() {
            set.push("endpoint");
        }
        if self.allow_http {
            set.push("allow_http");
        }
        set
    }

    /// Enforce Decision A4: a profile and inline connection fields cannot
    /// both be present, and a profile path must be absolute.
    pub fn check_storage_profile(&self) -> Result<(), RemoteConfigError> {
        let Some(path) = self.storage.as_deref() else {
            return Ok(());
        };
        if !path.starts_with('/') {
            return Err(RemoteConfigError::InvalidStoragePath {
                value: path.to_string(),
            });
        }
        let inline = self.inline_connection_fields();
        if !inline.is_empty() {
            return Err(RemoteConfigError::StorageConflict {
                path: path.to_string(),
                fields: inline.join(", "),
            });
        }
        Ok(())
    }

    /// The configured limiters as (declared dimension, pond path) pairs.
    ///
    /// The map key is the dimension the transfer path intends to spend; it is
    /// resolved to a [`LimitUnit`] here and passed to `Limiter::open` as the
    /// caller's declaration, where it is checked against the node's configured
    /// `unit`.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteConfigError::InvalidLimitDimension`] for a key that is
    /// not a known dimension, and [`RemoteConfigError::InvalidLimitPath`] for
    /// a value that is not an absolute pond path.
    pub fn resolved_limits(&self) -> Result<Vec<(LimitUnit, String)>, RemoteConfigError> {
        let mut out = Vec::with_capacity(self.limits.len());
        for (key, path) in &self.limits {
            let unit =
                LimitUnit::parse(key).map_err(|e| RemoteConfigError::InvalidLimitDimension {
                    key: key.clone(),
                    reason: e.to_string(),
                })?;
            if !path.starts_with('/') {
                return Err(RemoteConfigError::InvalidLimitPath {
                    key: key.clone(),
                    value: path.clone(),
                });
            }
            out.push((unit, path.clone()));
        }
        Ok(out)
    }
}

/// Operating mode for a remote attachment.  Stored in the control table's
/// raw_config map under key `remote_mode:<name>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteMode {
    /// Local writes are pushed to the remote (default for `add`).
    Push,
    /// Local pulls from the remote; no writes are pushed back.
    Pull,
    /// Both directions enabled.
    Both,
}

impl RemoteMode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            RemoteMode::Push => "push",
            RemoteMode::Pull => "pull",
            RemoteMode::Both => "both",
        }
    }

    /// Parse from the persisted string form.  Returns an error on
    /// unrecognized values so the operator notices typos.
    pub fn parse(s: &str) -> Result<Self, RemoteConfigError> {
        match s {
            "push" => Ok(RemoteMode::Push),
            "pull" => Ok(RemoteMode::Pull),
            "both" => Ok(RemoteMode::Both),
            other => Err(RemoteConfigError::InvalidMode(other.to_string())),
        }
    }

    /// `true` when this remote should be pushed-to on post-commit and
    /// manual `pond push`.
    #[must_use]
    pub fn pushes(self) -> bool {
        matches!(self, RemoteMode::Push | RemoteMode::Both)
    }

    /// `true` when this remote should be pulled-from on manual `pond pull`.
    #[must_use]
    pub fn pulls(self) -> bool {
        matches!(self, RemoteMode::Pull | RemoteMode::Both)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_roundtrip() {
        for m in [RemoteMode::Push, RemoteMode::Pull, RemoteMode::Both] {
            assert_eq!(RemoteMode::parse(m.as_str()).unwrap(), m);
        }
        assert!(RemoteMode::parse("bogus").is_err());
    }

    #[test]
    fn mode_predicates() {
        assert!(RemoteMode::Push.pushes());
        assert!(!RemoteMode::Push.pulls());
        assert!(RemoteMode::Pull.pulls());
        assert!(!RemoteMode::Pull.pushes());
        assert!(RemoteMode::Both.pushes());
        assert!(RemoteMode::Both.pulls());
    }

    #[test]
    fn yaml_roundtrip_file_url() {
        let a = RemoteAttachment {
            url: "file:///tmp/x".to_string(),
            region: String::new(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            endpoint: String::new(),
            allow_http: false,
            limits: BTreeMap::new(),
            storage: None,
        };
        let s = serde_yaml::to_string(&a).unwrap();
        // Only `url:` should be present for file://.
        assert!(s.contains("url: file:///tmp/x"));
        assert!(!s.contains("region"));
        assert!(!s.contains("access_key_id"));
        let b = RemoteAttachment::from_yaml_bytes(s.as_bytes()).unwrap();
        assert_eq!(b.url, a.url);
    }

    #[test]
    fn storage_options_empty_for_file() {
        let a = RemoteAttachment {
            url: "file:///tmp/x".to_string(),
            region: "us-east-1".to_string(),
            access_key_id: "k".to_string(),
            secret_access_key: "s".to_string(),
            endpoint: String::new(),
            allow_http: true,
            limits: BTreeMap::new(),
            storage: None,
        };
        // File URLs ignore all S3 options.
        assert!(a.to_storage_options().unwrap().is_empty());
    }

    #[test]
    fn storage_options_populated_for_s3() {
        let a = RemoteAttachment {
            url: "s3://bucket/prefix".to_string(),
            region: "us-east-2".to_string(),
            access_key_id: "k".to_string(),
            secret_access_key: "s".to_string(),
            endpoint: "http://minio:9000".to_string(),
            allow_http: true,
            limits: BTreeMap::new(),
            storage: None,
        };
        let opts = a.to_storage_options().unwrap();
        assert_eq!(opts.get("region").map(String::as_str), Some("us-east-2"));
        assert_eq!(opts.get("access_key_id").map(String::as_str), Some("k"));
        assert_eq!(opts.get("secret_access_key").map(String::as_str), Some("s"));
        assert_eq!(
            opts.get("endpoint").map(String::as_str),
            Some("http://minio:9000")
        );
        assert_eq!(
            opts.get("virtual_hosted_style_request").map(String::as_str),
            Some("false")
        );
        assert_eq!(opts.get("allow_http").map(String::as_str), Some("true"));
    }

    #[test]
    fn storage_options_expand_env_refs() {
        let a = RemoteAttachment {
            url: "s3://bucket/prefix".to_string(),
            region: "us-east-2".to_string(),
            access_key_id: "literal-key".to_string(),
            // `:-default` resolves without mutating the process environment.
            secret_access_key: "${env:POND_RC_TEST_UNSET:-resolved-secret}".to_string(),
            endpoint: String::new(),
            allow_http: false,
            limits: BTreeMap::new(),
            storage: None,
        };
        let opts = a.to_storage_options().unwrap();
        // Literal values pass through; ${env:VAR} is resolved locally.
        assert_eq!(
            opts.get("access_key_id").map(String::as_str),
            Some("literal-key")
        );
        assert_eq!(
            opts.get("secret_access_key").map(String::as_str),
            Some("resolved-secret")
        );
    }

    #[test]
    fn storage_options_unset_env_ref_errors() {
        let a = RemoteAttachment {
            url: "s3://bucket/prefix".to_string(),
            region: String::new(),
            access_key_id: String::new(),
            // Uniquely-named var with no default: guaranteed unset.
            secret_access_key: "${env:POND_RC_TEST_DEFINITELY_UNSET_XYZ}".to_string(),
            endpoint: String::new(),
            allow_http: false,
            limits: BTreeMap::new(),
            storage: None,
        };
        // An unset env reference must surface as an error, never silently
        // forward the unresolved placeholder to S3.
        assert!(matches!(
            a.to_storage_options(),
            Err(RemoteConfigError::EnvSubst(_))
        ));
    }

    /// An attachment with no limits must serialize byte-identically to a
    /// pre-limiter one, so existing /sys/remotes/<name> documents -- which are
    /// replicated -- are untouched by this schema addition.
    #[test]
    fn absent_limits_serialize_away_entirely() {
        let a = RemoteAttachment {
            url: "file:///tmp/x".to_string(),
            region: String::new(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            endpoint: String::new(),
            allow_http: false,
            limits: BTreeMap::new(),
            storage: None,
        };
        let s = serde_yaml::to_string(&a).unwrap();
        assert_eq!(s, "url: file:///tmp/x\n");
    }

    /// Old YAML must keep parsing under the new binary: `limits` defaults.
    #[test]
    fn yaml_without_limits_still_parses() {
        let a = RemoteAttachment::from_yaml_bytes(b"url: file:///tmp/x\n").unwrap();
        assert!(a.limits.is_empty());
        assert!(a.resolved_limits().unwrap().is_empty());
    }

    #[test]
    fn limits_roundtrip_and_resolve_to_declared_dimensions() {
        let yaml = "url: file:///tmp/x\nlimits:\n  bytes: /sys/limits/b\n  ops: /sys/limits/o\n";
        let a = RemoteAttachment::from_yaml_bytes(yaml.as_bytes()).unwrap();
        assert_eq!(
            a.resolved_limits().unwrap(),
            vec![
                (LimitUnit::Bytes, "/sys/limits/b".to_string()),
                (LimitUnit::Ops, "/sys/limits/o".to_string()),
            ]
        );
        // The map round-trips, so `pond apply` of a dumped attachment is a
        // no-op rather than a silent policy drop.
        let again =
            RemoteAttachment::from_yaml_bytes(serde_yaml::to_string(&a).unwrap().as_bytes())
                .unwrap();
        assert_eq!(again.limits, a.limits);
    }

    /// A misspelled dimension is a configuration error, not an unenforced
    /// policy: the whole point of the limiter is that it cannot fail open by
    /// accident.
    #[test]
    fn unknown_limit_dimension_is_rejected_at_parse() {
        let yaml = "url: file:///tmp/x\nlimits:\n  byte: /sys/limits/b\n";
        assert!(matches!(
            RemoteAttachment::from_yaml_bytes(yaml.as_bytes()),
            Err(RemoteConfigError::InvalidLimitDimension { .. })
        ));
    }

    #[test]
    fn relative_limiter_path_is_rejected() {
        let yaml = "url: file:///tmp/x\nlimits:\n  bytes: sys/limits/b\n";
        assert!(matches!(
            RemoteAttachment::from_yaml_bytes(yaml.as_bytes()),
            Err(RemoteConfigError::InvalidLimitPath { .. })
        ));
    }
}

#[cfg(test)]
mod storage_profile_tests {
    use super::*;

    #[test]
    fn a_profile_alone_is_accepted() {
        let yaml = "url: s3://bucket\nstorage: /sys/storage/minio\n";
        let a = RemoteAttachment::from_yaml_bytes(yaml.as_bytes()).expect("parse");
        assert_eq!(a.storage.as_deref(), Some("/sys/storage/minio"));
    }

    /// Decision A4.  An attachment naming a profile while still carrying a
    /// stale inline endpoint has no unambiguous intent; silently preferring
    /// one would produce a working pond talking to the wrong storage.
    #[test]
    fn a_profile_plus_an_inline_field_is_refused() {
        for inline in [
            "endpoint: http://x:9000",
            "region: us-east-1",
            "access_key_id: ${env:A}",
            "secret_access_key: ${env:B}",
            "allow_http: true",
        ] {
            let yaml = format!("url: s3://bucket\nstorage: /sys/storage/minio\n{inline}\n");
            let err = RemoteAttachment::from_yaml_bytes(yaml.as_bytes())
                .expect_err("must refuse conflicting authoring styles");
            assert!(
                matches!(err, RemoteConfigError::StorageConflict { .. }),
                "{inline}: {err}"
            );
        }
    }

    #[test]
    fn a_relative_profile_path_is_refused() {
        let yaml = "url: s3://bucket\nstorage: sys/storage/minio\n";
        let err = RemoteAttachment::from_yaml_bytes(yaml.as_bytes()).expect_err("must refuse");
        assert!(matches!(err, RemoteConfigError::InvalidStoragePath { .. }));
    }

    /// The inline path is untouched: existing documents must keep working
    /// exactly as they did, and must serialize byte-identically.
    #[test]
    fn an_inline_attachment_is_unaffected() {
        // Field order follows the struct, not the source document.
        let yaml = "url: s3://bucket\naccess_key_id: ${env:A}\nendpoint: http://x:9000\n";
        let a = RemoteAttachment::from_yaml_bytes(yaml.as_bytes()).expect("parse");
        assert!(a.storage.is_none());
        assert_eq!(serde_yaml::to_string(&a).unwrap(), yaml);
    }

    #[test]
    fn an_absent_profile_serializes_away_entirely() {
        let a = RemoteAttachment {
            url: "file:///tmp/x".to_string(),
            region: String::new(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            endpoint: String::new(),
            allow_http: false,
            limits: BTreeMap::new(),
            storage: None,
        };
        assert_eq!(serde_yaml::to_string(&a).unwrap(), "url: file:///tmp/x\n");
    }
}
