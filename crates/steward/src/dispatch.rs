// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Steward and Transaction dispatch enums
//!
//! These enums wrap the concrete steward implementations (Pond, Host) and
//! forward all method calls via match dispatch. Commands use `Steward` and
//! `Transaction` as their interface, never referencing the concrete types
//! directly.

use crate::{
    RecoveryResult, StewardError, StewardTransactionGuard,
    control_table::ControlTable,
    host::{HostSteward, HostTransaction},
    maintenance::MaintenanceReport,
    ship::Ship,
};
use std::ops::{AsyncFnOnce, Deref};
use std::path::Path;
use std::sync::Arc;
use tinyfs::FS;
use tlogfs::{PondMetadata, PondUserMetadata};

// ---------------------------------------------------------------------------
// Steward enum -- wraps the concrete steward implementations
// ---------------------------------------------------------------------------

/// Dispatch enum for steward implementations.
///
/// Currently only supports Pond (tlogfs-backed) steward. Host steward
/// will be added as a second variant when the hostmount persistence
/// layer is ready.
pub enum Steward {
    /// Full tlogfs-backed steward with Delta Lake transactions and control table
    Pond(Box<Ship>),
    /// Lightweight host filesystem steward (no transactions, no control table)
    Host(HostSteward),
}

impl Steward {
    // -- Pond-specific constructors (wrap Ship) --

    /// Initialize a completely new pond.
    pub async fn create_pond<P: AsRef<Path>>(
        pond_path: P,
        birthplace: impl Into<String>,
    ) -> Result<Self, StewardError> {
        Ok(Steward::Pond(Box::new(
            Ship::create_pond(pond_path, birthplace).await?,
        )))
    }

    /// Open an existing pond.
    pub async fn open_pond<P: AsRef<Path>>(pond_path: P) -> Result<Self, StewardError> {
        Ok(Steward::Pond(Box::new(Ship::open_pond(pond_path).await?)))
    }

    /// Create pond infrastructure for bundle restoration.
    pub async fn create_pond_for_restoration<P: AsRef<Path>>(
        pond_path: P,
        preserve_metadata: PondMetadata,
    ) -> Result<Self, StewardError> {
        Ok(Steward::Pond(Box::new(
            Ship::create_pond_for_restoration(pond_path, preserve_metadata).await?,
        )))
    }

    /// Create a fresh pond as a replica carrying the given `pond_id`.
    ///
    /// Wraps [`Ship::create_replica`]: the resulting pond is initialized with a
    /// minimal root v1 under `pond_id` so a content-addressed mirror pull
    /// ([`crate::rebuild_pond`]) has a root to diff onto.  Used by
    /// `pond restore` to bootstrap a whole-pond replica of a source pond
    /// published to a remote.
    pub async fn create_replica<P: AsRef<Path>>(
        pond_path: P,
        pond_id: uuid::Uuid,
    ) -> Result<Self, StewardError> {
        Ok(Steward::Pond(Box::new(
            Ship::create_replica(pond_path, pond_id).await?,
        )))
    }

    /// Create a host filesystem steward rooted at the given directory.
    ///
    /// If mount specs are provided, the host filesystem will be overlaid
    /// with factory-mounted nodes at the specified paths.
    #[must_use]
    pub fn open_host(
        root_path: std::path::PathBuf,
        mount_specs: Vec<tinyfs::hostmount::MountSpec>,
    ) -> Self {
        Steward::Host(HostSteward::new(root_path, mount_specs))
    }

    // -- Transaction lifecycle --

    /// Begin a read transaction.
    pub async fn begin_read(
        &mut self,
        meta: &PondUserMetadata,
    ) -> Result<Transaction<'_>, StewardError> {
        match self {
            Steward::Pond(ship) => Ok(Transaction::Pond(ship.begin_read(meta).await?)),
            Steward::Host(host) => Ok(Transaction::Host(host.begin(meta).await?)),
        }
    }

    /// Begin a write transaction.
    pub async fn begin_write(
        &mut self,
        meta: &PondUserMetadata,
    ) -> Result<Transaction<'_>, StewardError> {
        match self {
            Steward::Pond(ship) => Ok(Transaction::Pond(ship.begin_write(meta).await?)),
            Steward::Host(host) => Ok(Transaction::Host(host.begin(meta).await?)),
        }
    }

    /// Execute operations within a scoped write transaction.
    ///
    /// Runs the async closure with `&FS` access, auto-commits on `Ok`,
    /// auto-aborts on `Err`.
    pub async fn write_transaction<F>(
        &mut self,
        meta: &PondUserMetadata,
        f: F,
    ) -> Result<(), StewardError>
    where
        F: for<'a> AsyncFnOnce(&'a FS) -> Result<(), StewardError>,
    {
        match self {
            Steward::Pond(ship) => ship.write_transaction(meta, f).await,
            Steward::Host(host) => {
                let tx = host.begin(meta).await?;
                f(&tx).await
            }
        }
    }

    // -- Control table access (pond-specific) --

    /// Get a reference to the control table.
    ///
    /// # Panics
    ///
    /// Panics if called on a Host steward (the host filesystem has no control table).
    #[must_use]
    pub fn control_table(&self) -> &ControlTable {
        match self {
            Steward::Pond(ship) => ship.control_table(),
            Steward::Host(_) => panic!("control_table() called on Host steward"),
        }
    }

    /// Get a mutable reference to the control table.
    ///
    /// # Panics
    ///
    /// Panics if called on a Host steward (the host filesystem has no control table).
    pub fn control_table_mut(&mut self) -> &mut ControlTable {
        match self {
            Steward::Pond(ship) => ship.control_table_mut(),
            Steward::Host(_) => panic!("control_table_mut() called on Host steward"),
        }
    }

    // -- Recovery (pond-specific) --

    /// Check if recovery is needed.
    pub async fn check_recovery_needed(&mut self) -> Result<(), StewardError> {
        match self {
            Steward::Pond(ship) => ship.check_recovery_needed().await,
            Steward::Host(_) => Ok(()), // No recovery concept for hostmount
        }
    }

    /// Run recovery.
    pub async fn recover(&mut self) -> Result<RecoveryResult, StewardError> {
        match self {
            Steward::Pond(ship) => ship.recover().await,
            Steward::Host(_) => Ok(RecoveryResult {
                recovered_count: 0,
                was_needed: false,
            }),
        }
    }

    // -- Maintenance (pond-specific) --

    /// Run maintenance (checkpoint + vacuum) on both data and control tables.
    ///
    /// When `force` is true, checkpoint is always created.
    /// When `compact` is true, also merges small parquet files into larger ones.
    /// Returns a default (empty) report for Host stewards.
    pub async fn maintain(
        &mut self,
        force: bool,
        compact: bool,
    ) -> Result<MaintenanceReport, StewardError> {
        match self {
            Steward::Pond(ship) => ship.maintain(force, compact).await,
            Steward::Host(_) => Ok(MaintenanceReport::default()),
        }
    }

    /// Report which series collapse would merge at `threshold`, and what
    /// merging them would cost, without merging anything.
    ///
    /// Returns an empty survey for Host stewards, which hold no series.
    ///
    /// # Errors
    /// Returns an error if the read transaction or discovery query fails.
    pub async fn survey_collapsible_series(
        &mut self,
        threshold: usize,
    ) -> Result<Vec<tlogfs::CollapsibleSeries>, StewardError> {
        match self {
            Steward::Pond(ship) => ship.survey_collapsible_series(threshold).await,
            Steward::Host(_) => Ok(Vec::new()),
        }
    }

    /// Report which native v2 series pack maintenance would repack at
    /// `threshold`, and the bounded physical layout it would publish,
    /// without writing anything.
    ///
    /// Returns an empty survey for Host stewards, which hold no series.
    ///
    /// # Errors
    /// Returns an error if the discovery read transaction, a series'
    /// manifest fold, or this pond's local pack-index enumeration fails.
    pub async fn survey_pack_maintenance(
        &mut self,
        threshold: usize,
    ) -> Result<Vec<crate::PackMaintenanceCandidate>, StewardError> {
        match self {
            Steward::Pond(ship) => ship.survey_pack_maintenance(threshold).await,
            Steward::Host(_) => Ok(Vec::new()),
        }
    }

    /// Map every node in the pond to its path, from stored directory rows.
    ///
    /// Host stewards hold no pond tree, so they map nothing.
    ///
    /// # Errors
    /// Returns an error if the read transaction or the directory scan fails.
    pub async fn node_paths(
        &mut self,
    ) -> Result<std::collections::HashMap<tinyfs::FileID, String>, StewardError> {
        match self {
            Steward::Pond(ship) => ship.node_paths().await,
            Steward::Host(_) => Ok(std::collections::HashMap::new()),
        }
    }

    /// Collapse multi-version `data:series` files whose live version count
    /// exceeds `threshold` into a single merged version.
    ///
    /// Returns a default (empty) report for Host stewards.
    pub async fn collapse_versions(
        &mut self,
        threshold: usize,
    ) -> Result<crate::CollapseReport, StewardError> {
        match self {
            Steward::Pond(ship) => ship.collapse_versions(threshold).await,
            Steward::Host(_) => Ok(crate::CollapseReport::default()),
        }
    }

    // -- Access to the underlying Ship (for pond-specific operations) --

    /// Get the underlying Ship if this is a Pond steward.
    ///
    /// Use this for pond-specific operations that are not part of the
    /// common Steward interface (e.g., `replay_transaction`,
    /// `query_oplog_records`).
    #[must_use]
    pub fn as_pond(&self) -> Option<&Ship> {
        match self {
            Steward::Pond(ship) => Some(ship),
            Steward::Host(_) => None,
        }
    }

    /// Get the underlying Ship mutably if this is a Pond steward.
    pub fn as_pond_mut(&mut self) -> Option<&mut Ship> {
        match self {
            Steward::Pond(ship) => Some(ship),
            Steward::Host(_) => None,
        }
    }

    /// Get the underlying HostSteward if this is a Host steward.
    #[must_use]
    pub fn as_host(&self) -> Option<&HostSteward> {
        match self {
            Steward::Host(host) => Some(host),
            Steward::Pond(_) => None,
        }
    }

    /// Get the underlying HostSteward mutably if this is a Host steward.
    pub fn as_host_mut(&mut self) -> Option<&mut HostSteward> {
        match self {
            Steward::Host(host) => Some(host),
            Steward::Pond(_) => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Transaction enum -- wraps the concrete transaction guard implementations
// ---------------------------------------------------------------------------

/// Dispatch enum for transaction guard implementations.
///
/// Wraps the concrete transaction guards and forwards all method calls.
/// Implements `Deref<Target=FS>` for filesystem access.
///
/// The `Pond` variant is significantly larger than `Host` because
/// `StewardTransactionGuard` carries the full delta-lake transaction
/// state plus an optional process-exclusion lock guard.  Boxing isn't
/// worth the extra allocation: `Pond` is the dominant variant in
/// practice, and the size delta is amortized across a single
/// transaction's lifetime.
#[allow(clippy::large_enum_variant)]
pub enum Transaction<'a> {
    /// Full tlogfs transaction with control table tracking
    Pond(StewardTransactionGuard<'a>),
    /// Lightweight host filesystem transaction (no real commit/rollback)
    Host(HostTransaction),
}

impl<'a> Transaction<'a> {
    // -- Backend-agnostic access --

    /// Get a `ProviderContext` for this transaction.
    pub fn provider_context(&self) -> Result<tinyfs::ProviderContext, tlogfs::TLogFSError> {
        match self {
            Transaction::Pond(guard) => guard.provider_context(),
            Transaction::Host(host) => Ok(host.provider_context()),
        }
    }

    /// Get or create a DataFusion SessionContext.
    pub async fn session_context(
        &mut self,
    ) -> Result<Arc<datafusion::execution::context::SessionContext>, tlogfs::TLogFSError> {
        match self {
            Transaction::Pond(guard) => guard.session_context().await,
            Transaction::Host(host) => Ok(host.session_context()),
        }
    }

    /// Look up the factory name associated with a filesystem node.
    pub async fn get_factory_for_node(
        &self,
        id: tinyfs::FileID,
    ) -> Result<Option<String>, tlogfs::TLogFSError> {
        match self {
            Transaction::Pond(guard) => guard.get_factory_for_node(id).await,
            Transaction::Host(host) => host
                .get_factory_for_node(id)
                .await
                .map_err(tlogfs::TLogFSError::TinyFS),
        }
    }

    /// Get the factory name and config bytes for a dynamic node.
    ///
    /// Works for both file-based and directory-based dynamic nodes.
    pub async fn get_dynamic_node_config(
        &self,
        id: tinyfs::FileID,
    ) -> Result<Option<(String, Vec<u8>)>, tlogfs::TLogFSError> {
        match self {
            Transaction::Pond(guard) => guard.get_dynamic_node_config(id).await,
            Transaction::Host(host) => host
                .get_dynamic_node_config(id)
                .await
                .map_err(tlogfs::TLogFSError::TinyFS),
        }
    }

    // -- Access to the underlying guard (for pond-specific operations) --

    /// Get the underlying `StewardTransactionGuard` if this is a Pond transaction.
    ///
    /// Use this for pond-specific operations that are not part of the common
    /// `Transaction` interface (e.g., `control_table`, `get_commit_history`,
    /// `store_path`, `query_records`, `initialize_root_directory`).
    #[must_use]
    pub fn as_pond(&self) -> Option<&StewardTransactionGuard<'a>> {
        match self {
            Transaction::Pond(guard) => Some(guard),
            Transaction::Host(_) => None,
        }
    }

    /// Get the underlying `StewardTransactionGuard` mutably if this is a Pond transaction.
    pub fn as_pond_mut(&mut self) -> Option<&mut StewardTransactionGuard<'a>> {
        match self {
            Transaction::Pond(guard) => Some(guard),
            Transaction::Host(_) => None,
        }
    }

    /// Get the underlying `HostTransaction` if this is a Host transaction.
    #[must_use]
    pub fn as_host(&self) -> Option<&HostTransaction> {
        match self {
            Transaction::Host(host) => Some(host),
            Transaction::Pond(_) => None,
        }
    }

    /// Get the underlying `HostTransaction` mutably if this is a Host transaction.
    pub fn as_host_mut(&mut self) -> Option<&mut HostTransaction> {
        match self {
            Transaction::Host(host) => Some(host),
            Transaction::Pond(_) => None,
        }
    }

    // -- Lifecycle --

    /// Commit the transaction.
    ///
    /// On a `Pond` write commit, returns `Ok(Some(version))` with the
    /// data-FS Delta version number. Returns `Ok(None)` for a read or a
    /// write that produced no changes. `Host` transactions have no
    /// versioning concept and always return `Ok(None)`.
    pub async fn commit(self) -> Result<Option<i64>, StewardError> {
        match self {
            Transaction::Pond(guard) => guard.commit().await,
            Transaction::Host(_) => Ok(None), // No-op: host writes are immediate
        }
    }

    /// Abort the transaction and record it as failed.
    pub async fn abort(self, error: impl std::fmt::Display) -> StewardError {
        match self {
            Transaction::Pond(guard) => guard.abort(error).await,
            Transaction::Host(_) => {
                // No abort mechanism on host filesystem; return the error as-is
                StewardError::Aborted(error.to_string())
            }
        }
    }
}

impl<'a> Deref for Transaction<'a> {
    type Target = FS;

    fn deref(&self) -> &Self::Target {
        match self {
            Transaction::Pond(guard) => guard,
            Transaction::Host(host) => host,
        }
    }
}
