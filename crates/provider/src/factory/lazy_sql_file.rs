// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Shared delegation for dynamic factory files that lazily build an inner
//! [`SqlDerivedFile`](crate::factory::sql_derived::SqlDerivedFile).
//!
//! Factories use an inner `SqlDerivedFile` for shared source resolution and,
//! where applicable, generated user-facing SQL execution.
//! They share byte-identical `File` and `Metadata` implementations; this
//! module generates those while each built-in owns its typed query plan.

use crate::FactoryContext;
use crate::factory::sql_derived::{SqlDerivedConfig, SqlDerivedFile, SqlDerivedMode};
use std::future::Future;
use tinyfs::Result as TinyFSResult;
use tokio::sync::Mutex;

/// Lazily build the inner [`SqlDerivedFile`] in [`SqlDerivedMode::Series`] mode.
///
/// Centralizes the lock/check/create/store skeleton shared by the
/// `temporal-reduce`, `timeseries-join`, and `timeseries-pivot` factories: it
/// locks `inner`, returns early if already initialized, otherwise runs
/// `build_config` to produce the factory-specific [`SqlDerivedConfig`], builds
/// the `SqlDerivedFile`, and stores it.
///
/// `build_config` is awaited while holding the lock, matching the factories'
/// existing behavior (SQL generation may itself perform schema discovery).
pub(crate) async fn ensure_inner_series<F, Fut>(
    inner: &Mutex<Option<SqlDerivedFile>>,
    context: &FactoryContext,
    build_config: F,
) -> TinyFSResult<()>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = TinyFSResult<SqlDerivedConfig>>,
{
    let mut inner_guard = inner.lock().await;
    if inner_guard.is_some() {
        return Ok(());
    }

    let sql_config = build_config().await?;
    let sql_file = SqlDerivedFile::new(sql_config, context.clone(), SqlDerivedMode::Series)?;
    *inner_guard = Some(sql_file);
    Ok(())
}

/// Generate `tinyfs::File` and `tinyfs::Metadata` implementations that delegate
/// to a lazily-built inner `SqlDerivedFile`.
///
/// The target type MUST provide:
/// - a field `inner: std::sync::Arc<tokio::sync::Mutex<Option<SqlDerivedFile>>>`
/// - an async method `ensure_inner(&self) -> tinyfs::Result<()>` that populates
///   `inner` on first call.
macro_rules! impl_lazy_sql_derived_file_metadata {
    ($ty:ty) => {
        #[async_trait::async_trait]
        impl tinyfs::File for $ty {
            async fn async_reader(
                &self,
            ) -> tinyfs::Result<std::pin::Pin<Box<dyn tinyfs::AsyncReadSeek>>> {
                self.ensure_inner().await?;
                let inner_guard = self.inner.lock().await;
                let inner = inner_guard
                    .as_ref()
                    .expect("inner initialized by ensure_inner");
                inner.async_reader().await
            }

            async fn async_writer(
                &self,
            ) -> tinyfs::Result<std::pin::Pin<Box<dyn tinyfs::FileMetadataWriter>>> {
                self.ensure_inner().await?;
                let inner_guard = self.inner.lock().await;
                let inner = inner_guard
                    .as_ref()
                    .expect("inner initialized by ensure_inner");
                inner.async_writer().await
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn as_queryable(&self) -> Option<&dyn tinyfs::QueryableFile> {
                Some(self)
            }
        }

        #[async_trait::async_trait]
        impl tinyfs::Metadata for $ty {
            async fn metadata(&self) -> tinyfs::Result<tinyfs::NodeMetadata> {
                // Lightweight metadata without expensive schema discovery, so
                // list operations stay fast; schema discovery is deferred until
                // actual content access (as_table_provider, async_reader, ...).
                Ok(tinyfs::NodeMetadata {
                    version: 1,
                    size: None,
                    blake3: None,
                    bao_outboard: None,
                    entry_type: tinyfs::EntryType::TableDynamic,
                    timestamp: 0,
                })
            }
        }
    };
}

pub(crate) use impl_lazy_sql_derived_file_metadata;
