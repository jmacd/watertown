// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `storage-azure` factory: declares how to reach an Azure Blob Storage
//! account.
//!
//! See `docs/storage-profile-design.md` §3.2.  Like [`super::storage_minio`],
//! a storage profile is a **leaf config node**: nothing to compute, nothing to
//! execute.  It exists so that "this is our Azure account" is stated once, in
//! the pond, and referenced by path from every remote that talks to it.
//!
//! ```yaml
//! kind: mknod
//! metadata:
//!   path: /sys/storage/azure
//! spec:
//!   factory: storage-azure
//!   config:
//!     account_name: ${env:AZURE_STORAGE_ACCOUNT}
//!     account_key: ${env:AZURE_STORAGE_KEY}
//! ```
//!
//! # Exactly one credential shape (Decision A4)
//!
//! Azure accepts an account key, a SAS token, or a service principal, and this
//! kind requires **exactly one**.  Zero is an error because a profile that
//! authenticates with nothing is a failure deferred to first push.  Two is an
//! error because a profile holding both a key and a SAS token has no
//! unambiguous intent, and silently preferring one would be the precedence
//! rule Decision A4 exists to forbid -- an operator who rotated the key and
//! left the token behind would keep authenticating with the stale one and have
//! no way to see it.
//!
//! This is also the concrete reason `storage-azure` is its own factory rather
//! than optional fields on a shared struct (Decision A3): "exactly one of three
//! groups" is not something a flat field set shared with MinIO can state.
//!
//! [`StorageAzureConfig::credential`] is the only way to read the credential,
//! so the exactly-one rule is enforced wherever it is used rather than
//! remembered at each site.
//!
//! # Credentials are references, never values (Decision A1)
//!
//! Every credential field MUST be an `${env:...}` reference, rejected at
//! `pond apply` time otherwise.  A profile node replicates to every backup, so
//! a literal would be exposed on all of them -- and an Azure account key grants
//! full control of the account.  `account_name` is exempt: it is an identifier
//! that appears in every URL, not a secret, and requiring it to be indirect
//! would obscure which account a profile names without protecting anything.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use tinyfs::FileHandle;
use tinyfs::Result as TinyFSResult;
use tinyfs::ResultExt;

/// The factory name this kind registers under.  Named once here so consumers
/// that dispatch on it (`steward::storage_profile`) cannot drift from the
/// registration below.
pub const FACTORY_NAME: &str = "storage-azure";

// ============================================================================
// Configuration
// ============================================================================

/// An Azure AD service principal: the credential shape used when a pond
/// authenticates as an application rather than with a shared secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServicePrincipal {
    /// Application (client) id.  Must be an `${env:...}` reference.
    pub client_id: String,
    /// Client secret.  Must be an `${env:...}` reference.
    pub client_secret: String,
    /// Directory (tenant) id.  Must be an `${env:...}` reference.
    pub tenant_id: String,
}

/// On-disk config for a `storage-azure` node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageAzureConfig {
    /// Storage account name, e.g. `casparwater`.  Required: an Azure profile
    /// without an account names nothing.  Not a credential (see module docs).
    pub account_name: String,

    /// Per-request HTTP timeout accepted by `object_store` (for example `2m`).
    /// Azure multipart uploads can otherwise exceed the 30-second default on a
    /// constrained uplink when several parts are in flight.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,

    /// Shared account key.  Must be an `${env:...}` reference (Decision A1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_key: Option<String>,

    /// Shared access signature token.  Must be an `${env:...}` reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sas_token: Option<String>,

    /// Azure AD application credentials.  Each field must be an `${env:...}`
    /// reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_principal: Option<ServicePrincipal>,
}

/// The credential a profile authenticates with, once the exactly-one rule has
/// been applied.
///
/// Borrowed rather than owned so obtaining it copies no secret.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AzureCredential<'a> {
    /// A shared account key.
    AccountKey(&'a str),
    /// A shared access signature token.
    SasToken(&'a str),
    /// An Azure AD application.
    ServicePrincipal(&'a ServicePrincipal),
}

impl AzureCredential<'_> {
    /// The shape's name, for operator-facing output.  Never the value.
    #[must_use]
    pub fn shape(&self) -> &'static str {
        match self {
            Self::AccountKey(_) => "account_key",
            Self::SasToken(_) => "sas_token",
            Self::ServicePrincipal(_) => "service_principal",
        }
    }
}

/// Why a `storage-azure` config was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageAzureError {
    /// A required field was empty.
    Missing { field: &'static str },
    /// No credential shape was given.
    NoCredential,
    /// More than one credential shape was given.
    AmbiguousCredential { shapes: Vec<&'static str> },
    /// A credential field was a literal rather than an `${env:...}` reference.
    LiteralCredential { field: &'static str },
}

impl fmt::Display for StorageAzureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing { field } => write!(f, "`{field}` is required and must not be empty"),
            Self::NoCredential => write!(
                f,
                "exactly one credential is required: set one of `account_key`, `sas_token`, or \
                 `service_principal`. A profile with none would authenticate with nothing and \
                 fail on first push rather than at apply."
            ),
            Self::AmbiguousCredential { shapes } => write!(
                f,
                "exactly one credential is allowed, but {} were given: {}. Preferring one \
                 silently would hide a stale credential -- remove the ones that should not be \
                 used.",
                shapes.len(),
                shapes.join(", ")
            ),
            Self::LiteralCredential { field } => write!(
                f,
                "`{field}` must be an environment reference such as \
                 ${{env:AZURE_STORAGE_KEY}}, not a literal: a storage profile is replicated to \
                 every backup, so a literal credential would be exposed on all replicas. Set it \
                 in the environment and reference it here."
            ),
        }
    }
}

impl std::error::Error for StorageAzureError {}

impl StorageAzureConfig {
    /// The single credential this profile authenticates with.
    ///
    /// The only way to read a credential, so the exactly-one rule (Decision A4)
    /// is enforced at every use rather than being a check someone has to
    /// remember to have run.
    pub fn credential(&self) -> Result<AzureCredential<'_>, StorageAzureError> {
        let mut found: Vec<AzureCredential<'_>> = Vec::new();
        if let Some(k) = self.account_key.as_deref().filter(|s| !s.trim().is_empty()) {
            found.push(AzureCredential::AccountKey(k));
        }
        if let Some(t) = self.sas_token.as_deref().filter(|s| !s.trim().is_empty()) {
            found.push(AzureCredential::SasToken(t));
        }
        if let Some(sp) = self.service_principal.as_ref() {
            found.push(AzureCredential::ServicePrincipal(sp));
        }

        match found.len() {
            1 => Ok(found[0]),
            0 => Err(StorageAzureError::NoCredential),
            _ => Err(StorageAzureError::AmbiguousCredential {
                shapes: found.iter().map(AzureCredential::shape).collect(),
            }),
        }
    }

    /// Check every rule this kind enforces.
    ///
    /// Used on the config **as stored in the pond**, where both halves apply.
    /// The two creation-time entry points each check one half, because neither
    /// sees a document that satisfies both: see [`Self::validate_structure`]
    /// and [`Self::validate_references`].
    pub fn validate(&self) -> Result<(), StorageAzureError> {
        self.validate_structure()?;
        self.validate_references()
    }

    /// Check the rules that concern *values*: a named account, and exactly one
    /// non-empty credential shape.
    ///
    /// This runs on the **env-expanded** config, which is the only form in
    /// which emptiness is knowable -- a reference to an unset variable is not
    /// distinguishable from a set one until it is expanded.
    pub fn validate_structure(&self) -> Result<(), StorageAzureError> {
        if self.account_name.trim().is_empty() {
            return Err(StorageAzureError::Missing {
                field: "account_name",
            });
        }
        if self.timeout.as_ref().is_some_and(|v| v.trim().is_empty()) {
            return Err(StorageAzureError::Missing { field: "timeout" });
        }
        if let Some(sp) = self.service_principal.as_ref() {
            for (field, value) in [
                ("service_principal.client_id", &sp.client_id),
                ("service_principal.client_secret", &sp.client_secret),
                ("service_principal.tenant_id", &sp.tenant_id),
            ] {
                if value.trim().is_empty() {
                    return Err(StorageAzureError::Missing { field });
                }
            }
        }
        let _ = self.credential()?;
        Ok(())
    }

    /// Check that every credential is still an `${env:...}` reference
    /// (Decision A1).
    ///
    /// This runs on the **raw** config, before expansion -- expansion is
    /// precisely what turns a reference into a literal, so after it the
    /// distinction this rule is about no longer exists.
    ///
    /// Each present field is checked independently of the exactly-one rule: a
    /// document with two literal credentials should say both are literals, not
    /// pick one complaint.  `account_name` is not checked; it is an identifier,
    /// not a secret.
    pub fn validate_references(&self) -> Result<(), StorageAzureError> {
        let sp = self.service_principal.as_ref();
        let fields: [(&'static str, Option<&str>); 5] = [
            ("account_key", self.account_key.as_deref()),
            ("sas_token", self.sas_token.as_deref()),
            (
                "service_principal.client_id",
                sp.map(|s| s.client_id.as_str()),
            ),
            (
                "service_principal.client_secret",
                sp.map(|s| s.client_secret.as_str()),
            ),
            (
                "service_principal.tenant_id",
                sp.map(|s| s.tenant_id.as_str()),
            ),
        ];
        for (field, value) in fields {
            let Some(value) = value else { continue };
            if value.trim().is_empty() {
                continue;
            }
            if !utilities::env_substitution::has_env_refs(value) {
                return Err(StorageAzureError::LiteralCredential { field });
            }
        }
        Ok(())
    }

    /// The `object_store` options this profile implies, with `${env:...}`
    /// references resolved.
    ///
    /// Resolution happens **here**, at use time, so each replica authenticates
    /// as itself (Decision A6).  Only the keys belonging to the one credential
    /// shape are emitted: handing `object_store` a key and a token together
    /// would reintroduce, inside the builder, exactly the ambiguity
    /// [`Self::credential`] refuses.
    pub fn to_storage_options(&self) -> Result<HashMap<String, String>, String> {
        let resolve = |v: &str| {
            utilities::env_substitution::substitute_env_vars(v).map_err(|e| e.to_string())
        };

        let credential = self.credential().map_err(|e| e.to_string())?;

        let mut out = HashMap::new();
        let _ = out.insert("account_name".to_string(), resolve(&self.account_name)?);
        if let Some(timeout) = self.timeout.as_deref() {
            let _ = out.insert("azure_timeout".to_string(), resolve(timeout)?);
        }
        match credential {
            AzureCredential::AccountKey(k) => {
                let _ = out.insert("account_key".to_string(), resolve(k)?);
            }
            AzureCredential::SasToken(t) => {
                let _ = out.insert("sas_token".to_string(), resolve(t)?);
            }
            AzureCredential::ServicePrincipal(sp) => {
                let _ = out.insert("client_id".to_string(), resolve(&sp.client_id)?);
                let _ = out.insert("client_secret".to_string(), resolve(&sp.client_secret)?);
                let _ = out.insert("tenant_id".to_string(), resolve(&sp.tenant_id)?);
            }
        }
        Ok(out)
    }
}

/// Parse a `storage-azure` node's stored config bytes.
///
/// Shared by the factory and by `steward::storage_profile`, so both agree on
/// exactly one interpretation of a node's config -- the same discipline as
/// [`super::storage_minio::config_from_bytes`].
pub fn config_from_bytes(config: &[u8]) -> Result<StorageAzureConfig, String> {
    let text = std::str::from_utf8(config).map_err(|e| format!("invalid UTF-8: {e}"))?;
    let cfg: StorageAzureConfig =
        serde_yaml::from_str(text).map_err(|e| format!("invalid storage-azure config: {e}"))?;
    cfg.validate().map_err(|e| e.to_string())?;
    Ok(cfg)
}

// ============================================================================
// Factory
// ============================================================================

/// The placeholder shown in place of a credential value.
pub const REDACTED: &str = "<redacted>";

/// Render the profile as the node's readable content: the account name and the
/// credential *shape*, with every value replaced by [`REDACTED`].
///
/// Redaction is required, not cosmetic.  This runs on the **env-expanded**
/// config -- `FactoryRegistry::create_file` expands before the factory is
/// called -- so the values in hand are the resolved secrets.  Consumers do not
/// read this text; they bind from the raw stored config
/// (`steward::storage_profile`), which still holds the references.
///
/// The shape is named because which credential is in use is an operational
/// fact worth seeing in `pond cat`: it is what an operator rotating a key needs
/// to know, and it reveals nothing about the value.
#[must_use]
pub fn render(cfg: &StorageAzureConfig) -> Vec<u8> {
    let shape = cfg.credential().map_or("none", |c| c.shape());
    let normalized = StorageAzureConfig {
        account_name: cfg.account_name.clone(),
        timeout: cfg.timeout.clone(),
        account_key: cfg.account_key.as_ref().map(|_| REDACTED.to_string()),
        sas_token: cfg.sas_token.as_ref().map(|_| REDACTED.to_string()),
        service_principal: cfg.service_principal.as_ref().map(|_| ServicePrincipal {
            client_id: REDACTED.to_string(),
            client_secret: REDACTED.to_string(),
            tenant_id: REDACTED.to_string(),
        }),
    };
    let body = serde_yaml::to_string(&normalized)
        .unwrap_or_else(|e| format!("# failed to render config: {e}\n"));
    format!(
        "# storage-azure: account {} (authenticating with {shape})\n{body}",
        cfg.account_name
    )
    .into_bytes()
}

fn create_storage_azure_handle(
    config: Value,
    _context: crate::FactoryContext,
) -> TinyFSResult<FileHandle> {
    let cfg: StorageAzureConfig =
        serde_json::from_value(config).map_other_context("Invalid storage-azure config")?;
    // Structure only: `config` arrives expanded, so the reference rule is not
    // checkable here.  It is enforced at creation time on the raw form.
    cfg.validate_structure()
        .map_err(|e| tinyfs::Error::Other(format!("Invalid storage-azure config: {e}")))?;
    Ok(crate::ConfigFile::new(render(&cfg)).create_handle())
}

/// Validate the **env-expanded** config: structure only.
///
/// The credential-reference rule cannot be checked here, because expansion has
/// already replaced every reference with its value.  It is checked instead by
/// [`validate_storage_azure_raw_config`].
fn validate_storage_azure_config(config: &[u8]) -> TinyFSResult<Value> {
    let config_str = std::str::from_utf8(config).map_other_context("Invalid UTF-8")?;
    let cfg: StorageAzureConfig =
        serde_yaml::from_str(config_str).map_other_context("Invalid storage-azure config")?;
    cfg.validate_structure()
        .map_err(|e| tinyfs::Error::Other(format!("Invalid storage-azure config: {e}")))?;
    serde_json::to_value(&cfg).map_other_context("Failed to serialize storage-azure config")
}

/// Validate the **raw** config, which is the form that gets stored: every
/// credential must still be an `${env:...}` reference (Decision A1).
///
/// The exactly-one rule is checked here too.  It is a property of which fields
/// are present, not of their values, so it is knowable before expansion -- and
/// catching it here means `pond apply` refuses an ambiguous profile instead of
/// storing one that only fails once something tries to use it.
fn validate_storage_azure_raw_config(config: &[u8]) -> TinyFSResult<()> {
    let config_str = std::str::from_utf8(config).map_other_context("Invalid UTF-8")?;
    let cfg: StorageAzureConfig =
        serde_yaml::from_str(config_str).map_other_context("Invalid storage-azure config")?;
    cfg.validate_references()
        .map_err(|e| tinyfs::Error::Other(format!("Invalid storage-azure config: {e}")))?;
    let _ = cfg
        .credential()
        .map_err(|e| tinyfs::Error::Other(format!("Invalid storage-azure config: {e}")))?;
    Ok(())
}

crate::register_dynamic_factory!(
    name: FACTORY_NAME,
    description: "Declare how to reach an Azure Blob Storage account (account + one credential shape), referenced by path from remotes",
    file: create_storage_azure_handle,
    validate: validate_storage_azure_config,
    validate_raw: validate_storage_azure_raw_config
);

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn with_key() -> StorageAzureConfig {
        StorageAzureConfig {
            account_name: "casparwater".to_string(),
            timeout: None,
            account_key: Some("${env:AZURE_STORAGE_KEY}".to_string()),
            sas_token: None,
            service_principal: None,
        }
    }

    fn principal() -> ServicePrincipal {
        ServicePrincipal {
            client_id: "${env:AZURE_CLIENT_ID}".to_string(),
            client_secret: "${env:AZURE_CLIENT_SECRET}".to_string(),
            tenant_id: "${env:AZURE_TENANT_ID}".to_string(),
        }
    }

    #[test]
    fn each_credential_shape_is_accepted_alone() {
        assert_eq!(with_key().credential().expect("key").shape(), "account_key");

        let sas = StorageAzureConfig {
            account_key: None,
            sas_token: Some("${env:AZURE_STORAGE_SAS}".to_string()),
            ..with_key()
        };
        assert_eq!(sas.credential().expect("sas").shape(), "sas_token");

        let sp = StorageAzureConfig {
            account_key: None,
            service_principal: Some(principal()),
            ..with_key()
        };
        assert_eq!(sp.credential().expect("sp").shape(), "service_principal");
    }

    /// Zero credentials must fail at apply, not at first push: a profile that
    /// authenticates with nothing is a deferred outage.
    #[test]
    fn no_credential_is_refused() {
        let cfg = StorageAzureConfig {
            account_key: None,
            ..with_key()
        };
        assert_eq!(cfg.credential(), Err(StorageAzureError::NoCredential));
        assert!(cfg.validate_structure().is_err());
    }

    /// Two credentials have no unambiguous intent.  Preferring one silently is
    /// the precedence rule Decision A4 forbids -- and the error must name both,
    /// so the operator can see which one they forgot to remove.
    #[test]
    fn two_credentials_are_refused_and_both_are_named() {
        let cfg = StorageAzureConfig {
            sas_token: Some("${env:AZURE_STORAGE_SAS}".to_string()),
            ..with_key()
        };
        let err = cfg.credential().expect_err("ambiguous");
        let msg = format!("{err}");
        assert!(msg.contains("account_key"), "{msg}");
        assert!(msg.contains("sas_token"), "{msg}");
    }

    #[test]
    fn all_three_credentials_are_refused() {
        let cfg = StorageAzureConfig {
            sas_token: Some("${env:AZURE_STORAGE_SAS}".to_string()),
            service_principal: Some(principal()),
            ..with_key()
        };
        assert!(matches!(
            cfg.credential(),
            Err(StorageAzureError::AmbiguousCredential { .. })
        ));
    }

    /// A literal credential must be refused on the way in.  A profile
    /// replicates to every backup, so a literal cannot be withdrawn once
    /// written.
    #[test]
    fn a_literal_credential_is_refused() {
        let cfg = StorageAzureConfig {
            account_key: Some("hunter2".to_string()),
            ..with_key()
        };
        assert_eq!(
            cfg.validate_references(),
            Err(StorageAzureError::LiteralCredential {
                field: "account_key"
            })
        );
    }

    /// Every field of a service principal is a credential, including the ones
    /// that look like identifiers.
    #[test]
    fn a_literal_service_principal_field_is_refused() {
        let cfg = StorageAzureConfig {
            account_key: None,
            service_principal: Some(ServicePrincipal {
                client_secret: "hunter2".to_string(),
                ..principal()
            }),
            ..with_key()
        };
        let err = cfg.validate_references().expect_err("literal");
        assert!(format!("{err}").contains("client_secret"), "{err}");
    }

    /// `account_name` is an identifier, not a secret: requiring it to be
    /// indirect would hide which account a profile names and protect nothing.
    #[test]
    fn a_literal_account_name_is_allowed() {
        cfg_validate_ok(&with_key());
    }

    fn cfg_validate_ok(cfg: &StorageAzureConfig) {
        cfg.validate_references().expect("references");
    }

    #[test]
    fn options_carry_only_the_chosen_shape() {
        // `PATH` stands in for a credential variable: the point is that the
        // value arrives from the environment at use time, not which variable
        // it came from.
        let cfg = StorageAzureConfig {
            account_key: Some("${env:PATH}".to_string()),
            ..with_key()
        };
        let opts = cfg.to_storage_options().expect("resolve");
        let path = std::env::var("PATH").expect("PATH is set");
        assert_eq!(
            opts.get("account_name").map(String::as_str),
            Some("casparwater")
        );
        assert_eq!(
            opts.get("account_key").map(String::as_str),
            Some(path.as_str())
        );
        assert!(
            !opts.contains_key("sas_token") && !opts.contains_key("client_id"),
            "an unused shape must not reach the builder: {opts:?}"
        );
    }

    #[test]
    fn timeout_is_forwarded_as_an_azure_client_option() {
        let cfg = StorageAzureConfig {
            account_key: Some("${env:PATH}".to_string()),
            timeout: Some("2m".to_string()),
            ..with_key()
        };
        let opts = cfg.to_storage_options().expect("options");
        assert_eq!(opts.get("azure_timeout").map(String::as_str), Some("2m"));
    }

    /// Options resolve at use time so each replica authenticates as itself
    /// (Decision A6).
    #[test]
    fn an_unresolvable_reference_fails_at_use_time() {
        let cfg = StorageAzureConfig {
            account_key: Some("${env:NOT_SET_XYZZY_AZ}".to_string()),
            ..with_key()
        };
        assert!(cfg.to_storage_options().is_err());
    }

    /// The rendered node is what an operator will `pond cat`.  It must name the
    /// shape in use and no value.
    #[test]
    fn render_names_the_shape_and_no_secret() {
        // `render` runs on the expanded config, so the value in hand is the
        // resolved secret -- which is exactly why it must be redacted.
        let expanded = StorageAzureConfig {
            account_key: Some("super-secret-value".to_string()),
            ..with_key()
        };
        let text = String::from_utf8(render(&expanded)).expect("utf8");
        assert!(text.contains("casparwater"), "{text}");
        assert!(text.contains("account_key"), "{text}");
        assert!(!text.contains("super-secret-value"), "{text}");
        assert!(text.contains(REDACTED), "{text}");
    }

    #[test]
    fn config_from_bytes_accepts_a_well_formed_profile() {
        let cfg = config_from_bytes(
            b"account_name: casparwater\naccount_key: ${env:AZURE_STORAGE_KEY}\n",
        )
        .expect("parse");
        assert_eq!(cfg.account_name, "casparwater");
    }

    /// `deny_unknown_fields` is what makes each profile kind's parse strict;
    /// a MinIO document must not be readable as an Azure one.
    #[test]
    fn a_minio_document_is_not_an_azure_profile() {
        let err = config_from_bytes(
            b"endpoint: http://watershop:9000\naccess_key_id: ${env:K}\nsecret_access_key: ${env:S}\n",
        )
        .expect_err("must refuse");
        assert!(err.contains("storage-azure"), "{err}");
    }

    #[test]
    fn raw_validation_refuses_an_ambiguous_profile_at_apply() {
        let err = validate_storage_azure_raw_config(
            b"account_name: casparwater\naccount_key: ${env:K}\nsas_token: ${env:T}\n",
        )
        .expect_err("must refuse");
        assert!(format!("{err}").contains("exactly one"), "{err}");
    }

    #[test]
    fn the_factory_is_registered_under_its_name() {
        let f = crate::FactoryRegistry::get_factory(FACTORY_NAME)
            .expect("storage-azure factory registered");
        assert_eq!(f.name, FACTORY_NAME);
    }
}
