// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Register Azure Blob Storage handlers with delta-rs WITHOUT depending on the
//! `deltalake-azure` crate, for the same version-pinning reason
//! [`crate::register_s3_handlers`] avoids `deltalake-aws` at deltalake 0.30.
//!
//! Call [`register_azure_handlers`] once at process startup BEFORE opening or
//! creating any `az://` URL.  In practice nothing calls it directly: a storage
//! profile registers the handlers its own kind needs
//! (`steward::storage_profile::ResolvedStorage::register_handlers`), which is
//! how provider selection stopped being a URL-prefix comparison.

use deltalake::logstore::{
    LogStore, LogStoreFactory, ObjectStoreFactory, ObjectStoreRef, StorageConfig, default_logstore,
    logstore_factories, object_store_factories,
};
use deltalake::{DeltaResult, DeltaTableError, Path};
use object_store::ObjectStoreScheme;
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

#[derive(Clone, Default, Debug)]
struct AzureStoreFactory {}

impl ObjectStoreFactory for AzureStoreFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        config: &StorageConfig,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let mut builder = MicrosoftAzureBuilder::new().with_url(url.to_string());
        for (key, value) in config.raw.iter() {
            if let Ok(config_key) = AzureConfigKey::from_str(&key.to_ascii_lowercase()) {
                builder = builder.with_config(config_key, value.clone());
            }
        }
        let (_, path) =
            ObjectStoreScheme::parse(url).map_err(|e| DeltaTableError::GenericError {
                source: Box::new(e),
            })?;
        let prefix = Path::parse(path)?;
        let store = builder.build().map_err(|e| DeltaTableError::GenericError {
            source: Box::new(e),
        })?;
        Ok((Arc::new(store), prefix))
    }
}

#[derive(Clone, Default, Debug)]
struct AzureLogStoreFactory {}

impl LogStoreFactory for AzureLogStoreFactory {
    fn with_options(
        &self,
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(
            prefixed_store,
            root_store,
            location,
            options,
        ))
    }
}

/// Every URL scheme Azure Blob Storage is reachable under.
///
/// `az` and `azure` address the Blob endpoint; `abfs`/`abfss` are the Data Lake
/// Gen2 spellings of the same store.  All four are registered because a URL
/// copied from the Azure portal or from Hadoop-flavoured documentation may use
/// any of them, and a scheme that resolves to no handler fails with a message
/// about the scheme rather than about the storage.
pub const AZURE_SCHEMES: &[&str] = &["az", "azure", "abfs", "abfss"];

/// Register Azure handlers for every scheme in [`AZURE_SCHEMES`].
///
/// Idempotent across multiple calls, as the underlying registry insert is, so
/// binding several Azure profiles is safe.
pub fn register_azure_handlers() {
    let object_factory = Arc::new(AzureStoreFactory::default());
    let log_factory = Arc::new(AzureLogStoreFactory::default());

    for scheme in AZURE_SCHEMES {
        let url = Url::parse(&format!("{scheme}://")).expect("valid scheme URL");
        object_store_factories().insert(url.clone(), object_factory.clone());
        logstore_factories().insert(url, log_factory.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Registration must be safe to repeat: profiles register their handlers
    /// at every bind, not once at startup.
    #[test]
    fn registering_twice_is_harmless() {
        register_azure_handlers();
        register_azure_handlers();

        for scheme in AZURE_SCHEMES {
            let url = Url::parse(&format!("{scheme}://")).expect("valid scheme URL");
            assert!(
                object_store_factories().get(&url).is_some(),
                "{scheme} should resolve to a handler"
            );
        }
    }
}
