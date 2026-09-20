// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Backend-independent persistence contract assertions.
//!
//! These helpers exercise semantics available through TinyFS rather than
//! backend implementation details. A backend test supplies an active
//! filesystem snapshot and its matching provider context, then separately
//! tests lifecycle operations such as commit, abort, and reopen.

use std::io::SeekFrom;
use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::{EntryType, FileID, ProviderContext, Result, WD, coherent_table_provider};

pub const BYTE_SERIES_PATH: &str = "/persistence-contract/bytes.series";
pub const FIRST_VERSION_CONTENT: &[u8] = b"first-version";
pub const SECOND_VERSION_CONTENT: &[u8] = b"second-version";

#[derive(Debug, Clone, Copy)]
pub struct ActiveTransactionArtifact {
    pub file_id: FileID,
    pub first_version: u64,
    pub second_version: u64,
}

/// Assert the shared read/write contract for an active persistence snapshot.
///
/// This covers:
/// - visibility through independently acquired TinyFS handles;
/// - transaction-global exclusion of concurrent writers;
/// - immediate read-after-write after writer shutdown;
/// - append-only version ordering and exact version reads;
/// - seekable and ranged reads, including clamping and invalid ranges; and
/// - generation-aware provider-cache invalidation and stale-provider rejection.
pub async fn assert_active_transaction_read_write(
    root: &WD,
    context: &ProviderContext,
) -> Result<ActiveTransactionArtifact> {
    let _ = root.create_dir_all("/persistence-contract").await?;
    let second_root = context.filesystem().root().await?;

    let mut first_writer = root
        .create_file_writer_with_type(BYTE_SERIES_PATH, EntryType::FilePhysicalSeries)
        .await?;
    first_writer
        .write_all(FIRST_VERSION_CONTENT)
        .await
        .map_err(|error| crate::Error::Other(format!("write first contract version: {error}")))?;

    let duplicate_error = match second_root
        .async_writer_path_with_type(BYTE_SERIES_PATH, EntryType::FilePhysicalSeries)
        .await
    {
        Ok(_) => panic!("a second handle must not open a writer for the same file"),
        Err(error) => error,
    };
    assert!(
        duplicate_error
            .to_string()
            .contains("already being written"),
        "unexpected duplicate-writer error: {duplicate_error}"
    );

    first_writer
        .shutdown()
        .await
        .map_err(|error| crate::Error::Other(format!("finish first contract version: {error}")))?;
    drop(first_writer);

    assert_eq!(
        second_root.read_file_path_to_vec(BYTE_SERIES_PATH).await?,
        FIRST_VERSION_CONTENT,
        "a completed write must be immediately visible through another TinyFS handle"
    );

    let generation_before_append = context
        .persistence
        .coherence_state()
        .expect("contract backends must expose coherence state")
        .generation();
    let cached: Arc<dyn TableProvider> = Arc::new(
        MemTable::try_new(Arc::new(Schema::empty()), vec![vec![]]).expect("empty in-memory table"),
    );
    context.set_table_provider_cache("persistence-contract".to_string(), cached.clone())?;
    assert!(
        context
            .get_table_provider_cache("persistence-contract")
            .is_some(),
        "provider cache entry must be visible in its creation generation"
    );
    let generation_bound_provider = coherent_table_provider(
        cached,
        context.persistence.coherence_state(),
        Some(generation_before_append),
    );

    let mut second_writer = second_root
        .async_writer_path_with_type(BYTE_SERIES_PATH, EntryType::FilePhysicalSeries)
        .await?;
    second_writer
        .write_all(SECOND_VERSION_CONTENT)
        .await
        .map_err(|error| crate::Error::Other(format!("write second contract version: {error}")))?;
    second_writer
        .shutdown()
        .await
        .map_err(|error| crate::Error::Other(format!("finish second contract version: {error}")))?;
    drop(second_writer);

    let coherence = context
        .persistence
        .coherence_state()
        .expect("contract backends must expose coherence state");
    assert!(
        coherence.generation() > generation_before_append,
        "a completed write must advance the persistence generation"
    );
    assert!(
        context
            .get_table_provider_cache("persistence-contract")
            .is_none(),
        "a provider cached before a write must not survive that write"
    );
    let stale_error = generation_bound_provider
        .supports_filters_pushdown(&[])
        .expect_err("a provider from an earlier generation must be stale");
    assert!(
        stale_error.to_string().contains("provider is stale"),
        "unexpected stale-provider error: {stale_error}"
    );

    let expected_series = [FIRST_VERSION_CONTENT, SECOND_VERSION_CONTENT].concat();
    assert_eq!(
        root.read_file_path_to_vec(BYTE_SERIES_PATH).await?,
        expected_series,
        "a series read must concatenate every live version in order"
    );

    let file_id = root.get_node_path(BYTE_SERIES_PATH).await?.id();
    let versions = root.list_file_versions(BYTE_SERIES_PATH).await?;
    assert_eq!(
        versions.len(),
        2,
        "the contract writes exactly two versions"
    );
    assert!(
        versions[0].version < versions[1].version,
        "versions must be returned oldest to newest"
    );
    assert!(
        versions[0].timestamp <= versions[1].timestamp,
        "version timestamps must be nondecreasing"
    );
    assert_eq!(
        root.read_file_version(BYTE_SERIES_PATH, versions[0].version)
            .await?,
        FIRST_VERSION_CONTENT
    );
    assert_eq!(
        root.read_file_version(BYTE_SERIES_PATH, versions[1].version)
            .await?,
        SECOND_VERSION_CONTENT
    );

    let mut reader = context
        .persistence
        .open_file_version(file_id, versions[1].version)
        .await?;
    _ = reader
        .seek(SeekFrom::Start(1))
        .await
        .map_err(|error| crate::Error::Other(format!("seek contract version: {error}")))?;
    let mut middle = [0_u8; 4];
    _ = reader
        .read_exact(&mut middle)
        .await
        .map_err(|error| crate::Error::Other(format!("read contract version: {error}")))?;
    assert_eq!(&middle, b"econ");

    assert_eq!(
        context
            .persistence
            .read_file_version_range(file_id, versions[1].version, 7..u64::MAX)
            .await?
            .as_ref(),
        b"version",
        "range ends beyond EOF must be clamped"
    );
    assert!(
        context
            .persistence
            .read_file_version_range(file_id, versions[1].version, 15..16)
            .await
            .is_err(),
        "a range starting beyond EOF must fail"
    );

    Ok(ActiveTransactionArtifact {
        file_id,
        first_version: versions[0].version,
        second_version: versions[1].version,
    })
}
