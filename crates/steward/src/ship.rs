// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Ship - Transaction coordinator managing data filesystem and control table

use crate::{
    RecoveryResult, StewardError, StewardTransactionGuard,
    control_table::{ControlTable, TransactionType},
    get_control_path, get_data_path,
    maintenance::{self, MaintenanceReport},
};
use anyhow::Result;
use log::{debug, info, warn};
use std::ops::AsyncFnOnce;
use std::path::{Path, PathBuf};
use tlogfs::{OpLogPersistence, PondMetadata, PondTxnMetadata, PondUserMetadata};

/// Ship manages the data filesystem (OpLogPersistence) and control table (ControlTable)
/// providing transaction coordination, audit logging, and post-commit action sequencing
pub struct Ship {
    data_persistence: OpLogPersistence,
    control_table: ControlTable,
    last_write_seq: i64,
    pond_path: PathBuf,
}

/// Outcome of a producer-side compaction transaction ([`Ship::compact`]).
#[derive(Debug, Clone, Copy)]
pub struct CompactOutcome {
    /// The transaction sequence allocated for this compaction.
    pub txn_seq: i64,
    /// Whether any files were actually merged.  `false` is a clean no-op:
    /// only a `Completed` record was written, the committed sequence is
    /// unchanged, and there is nothing to push.
    pub had_data: bool,
    /// Number of new merged parquet files Delta wrote (real compaction only).
    pub files_added: u64,
    /// Number of small parquet files Delta marked Removed (real compaction only).
    pub files_removed: u64,
    /// Data Delta version after the optimize commit (real compaction only).
    pub data_delta_version: i64,
}

/// Outcome of a pack-only physical maintenance sweep
/// ([`Ship::collapse_versions`]).
///
/// Every field describes *physical* pack state only: no Oplog append row,
/// `watertown.series.v2` manifest, tree/commit root, Delta version, or txn sequence
/// is ever touched by this operation (`docs/logical-series-identity-design.md`).
#[derive(Debug, Clone, Copy, Default)]
pub struct CollapseReport {
    /// Number of native v2 series (`FilePhysicalSeries`/`TablePhysicalSeries`)
    /// discovered as coarse candidates (more live versions than the
    /// requested threshold), before per-series bounded-layout candidacy is
    /// evaluated.
    pub candidates: usize,
    /// Number of series actually repacked into a new, more-bounded
    /// physical pack this run.
    pub series_repacked: usize,
    /// Number of series already at their achievable bounded floor (or
    /// never exceeding the threshold once evaluated) -- an idempotent
    /// no-op, proof that repeated maintenance settles.
    pub already_bounded: usize,
    /// Number of series that carry no persisted v2 leaf identity at all
    /// (pre-v2/legacy): pack-only maintenance does not cover these; not an
    /// error, just out of scope.
    pub unsupported_legacy: usize,
    /// New physical pack objects durably written this run (content-addressed;
    /// an object already present from a previous run does not count again).
    pub pack_objects_written: usize,
    /// Bytes of new physical pack objects durably written this run.
    pub pack_bytes_written: u64,
    /// Orphaned physical pack objects removed by this run's GC sweep (no
    /// longer referenced by any retained local pack advertisement).
    pub pack_objects_removed: usize,
    /// Bytes reclaimed by this run's pack-object GC sweep.
    pub pack_bytes_freed: u64,
    /// What the reclamation pass that follows pack maintenance actually
    /// freed (the pre-existing row/blob reclamation mechanism, unrelated
    /// to pack-object GC above).
    pub reclaimed: crate::reclaim::ReclaimStats,
}

impl std::fmt::Display for CollapseReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "pack maintenance: {} candidate(s), {} repacked, {} already bounded, \
             {} unsupported legacy series",
            self.candidates, self.series_repacked, self.already_bounded, self.unsupported_legacy
        )?;
        write!(
            f,
            "\n  packs: {} object(s) written ({} byte(s)), {} object(s) removed ({} byte(s) freed)",
            self.pack_objects_written,
            self.pack_bytes_written,
            self.pack_objects_removed,
            self.pack_bytes_freed
        )?;
        if !self.reclaimed.is_empty() {
            write!(f, "\n  {}", self.reclaimed)?;
        }
        Ok(())
    }
}

impl Ship {
    /// Initialize a completely new pond with proper transaction #1.
    ///
    /// `birthplace` is a user-asserted, immutable label for where the pond
    /// was created (recorded in the pond's identity metadata).
    ///
    /// Use `open_pond()` to work with ponds that already exist.
    pub async fn create_pond<P: AsRef<Path>>(
        pond_path: P,
        birthplace: impl Into<String>,
    ) -> Result<Self, StewardError> {
        Self::init_pond(pond_path, birthplace.into(), None).await
    }

    /// Shared root-initialization path for fresh ponds and replicas.  When
    /// `preserve_metadata` is `Some`, the pond adopts that identity (replica
    /// of a source pond); otherwise a fresh pond_id is minted.
    async fn init_pond<P: AsRef<Path>>(
        pond_path: P,
        birthplace: String,
        preserve_metadata: Option<PondMetadata>,
    ) -> Result<Self, StewardError> {
        let meta = PondUserMetadata::new(vec!["pond".to_string(), "init".to_string()]);

        // Create infrastructure (includes root directory initialization with txn_seq=1)
        // Pass metadata so root transaction has proper audit trail
        let mut ship = Self::create_infrastructure(
            pond_path,
            true,
            Some(meta.clone()),
            preserve_metadata,
            Some(birthplace),
        )
        .await?;

        // After create_infrastructure with create_new=true, tlogfs has already
        // created and committed transaction #1 (root init). Sync Ship's counter.
        let root_seq = ship.data_persistence.last_txn_seq();
        ship.last_write_seq = root_seq;

        debug!(
            "Synced last_write_seq={} from data_persistence after root init",
            root_seq
        );

        let txn_metadata = PondTxnMetadata::new(root_seq, meta);

        // Record begin for the root transaction (already committed by tlogfs)
        ship.control_table
            .record_begin(
                &txn_metadata,
                None, // Root has no based_on_seq
                TransactionType::Write,
            )
            .await?;

        // Record data_committed for the root-init transaction.  The root
        // directory's OplogEntry is written at the data Delta version that
        // `create_infrastructure` just produced (version 0 is `CREATE TABLE`
        // with no data; the root row lands at the next version).  Recording
        // the REAL version + checksums (rather than the historical
        // `data_delta_version=0` / empty-checksums placeholder) is what makes
        // `Remote::push` replicate the pond_init bundle, so a bootstrapped
        // replica holds the root-dir v1 row and `pond verify` matches
        // (P2-VERIFY-BOOTSTRAP-DRIFT).
        let root_version = ship.data_persistence.table().version().unwrap_or(0);
        // Partition checksums are retired (Decision D9, step 5b): the content
        // root recorded in the commit-log node subsumes them.
        ship.control_table
            .record_data_committed(
                &txn_metadata,
                TransactionType::Write,
                root_version,
                0,    // Duration unknown/not tracked
                None, // genesis: the first content-changing commit writes the first commit-log leaf (Decision D9)
            )
            .await?;

        // Record completed for the root transaction
        ship.control_table
            .record_completed(
                &txn_metadata,
                TransactionType::Write,
                0, // Duration unknown/not tracked
            )
            .await?;

        debug!(
            "Recorded root initialization transaction with txn_seq={}",
            root_seq
        );

        // Report pond identity (already set by create_infrastructure)
        let metadata = ship.control_table.pond_metadata().clone();
        info!(
            "Pond created with ID: {} at {} by {} (birthplace: {})",
            metadata.pond_id,
            chrono::DateTime::from_timestamp_micros(metadata.birth_timestamp)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            metadata.birth_username,
            metadata.birthplace
        );

        // Set default factory modes for primary pond
        // "remote" factory runs in "push" mode (automatic post-commit backup)
        ship.control_table
            .set_factory_mode("remote", "push")
            .await?;
        debug!("Set remote factory mode to 'push' for primary pond");

        // Pond is now ready with control table showing txn_seq=1
        Ok(ship)
    }

    /// Create pond infrastructure for bundle restoration (replication/backup restore).
    ///
    /// Unlike `create_pond("test-host")`, this creates the pond structure WITHOUT recording
    /// the initial transaction #1. The first bundle will create txn_seq=1 with
    /// the original command metadata from the source pond.
    ///
    /// Use this ONLY when restoring from bundles. Use `create_pond("test-host")` for normal initialization.
    ///
    /// # Arguments
    /// * `pond_path` - Path to the pond directory
    /// * `preserve_metadata` - Optional pond metadata from source (for replicas)
    pub async fn create_pond_for_restoration<P: AsRef<Path>>(
        pond_path: P,
        preserve_metadata: PondMetadata, // Required - restoring means preserving identity
    ) -> Result<Self, StewardError> {
        let control_path = get_control_path(pond_path.as_ref());
        let data_path = get_data_path(pond_path.as_ref());

        debug!("Creating pond for restoration: {:?}", pond_path.as_ref());

        // Create directories
        std::fs::create_dir_all(&data_path)?;
        std::fs::create_dir_all(&control_path)?;

        let control_path_str = control_path.to_string_lossy().to_string();
        let data_path_str = data_path.to_string_lossy().to_string();

        // Create ONLY the control table with preserved metadata
        debug!(
            "Creating control table for restoration with preserved pond identity: {}",
            preserve_metadata.pond_id
        );
        let control_table = ControlTable::create(&control_path_str, &preserve_metadata).await?;

        // Create an empty data Delta table structure (schema + _delta_log/, but no data)
        // Bundles will be replayed through replay_transaction() which writes to this table
        debug!("Creating empty data table structure at {}", data_path_str);
        let data_persistence =
            OpLogPersistence::create_empty(&data_path_str, preserve_metadata.pond_id.to_string())
                .await
                .map_err(StewardError::DataInit)?;

        debug!(
            "Created restoration-ready pond (empty data table, bundles will populate via replay)"
        );

        Ok(Ship {
            data_persistence,
            control_table,
            last_write_seq: 0, // No transactions yet - bundles will provide them
            pond_path: pond_path.as_ref().to_path_buf(),
        })
    }

    /// Create a fresh pond as a replica with the given `pond_id`.
    ///
    /// Convenience wrapper around [`Ship::init_pond`] that synthesizes
    /// default birth metadata (timestamp = now, hostname = `"unknown"`,
    /// username from `$USER`/`$USERNAME` or `"unknown"`).  Birth metadata on
    /// a replica is informational only -- the canonical replica identity is
    /// the `pond_id`, which must match the source pond it replicates.
    ///
    /// Since D6 the resulting pond is initialized with a minimal root v1 under
    /// the source `pond_id` so the content-addressed mirror pull
    /// ([`crate::rebuild_pond`]) has a root to diff onto.  It is NOT suitable
    /// for a bootstrap path that requires an empty data table so a source's
    /// replicated seq=1 root does not collide with a local root; use
    /// [`Ship::create_pond_for_restoration`] for that.
    pub async fn create_replica<P: AsRef<Path>>(
        pond_path: P,
        pond_id: uuid::Uuid,
    ) -> Result<Self, StewardError> {
        // Convert uuid::Uuid -> uuid7::Uuid by reusing the 16-byte
        // representation; both crates wrap the same wire format.
        let pond_id_uuid7 = uuid7::Uuid::from(*pond_id.as_bytes());
        let metadata = PondMetadata {
            pond_id: pond_id_uuid7,
            ..PondMetadata::default()
        };
        // Initialize a minimal root v1 under the source pond_id so the
        // content mirror pull (rebuild_pond) has a root to diff onto.
        Self::init_pond(pond_path, String::new(), Some(metadata)).await
    }

    /// Open an existing, pre-initialized pond.
    pub async fn open_pond<P: AsRef<Path>>(pond_path: P) -> Result<Self, StewardError> {
        Self::create_infrastructure(pond_path, false, None, None, None).await
    }

    /// Internal method to create just the filesystem infrastructure.
    ///
    /// This creates the data and control directories and initializes tlogfs instances,
    /// but does NOT create any transactions. It's used internally by both
    /// initialize_new_pond() and open_existing_pond().
    ///
    /// When `create_new=true`, `root_metadata` should contain (txn_id, cli_args) to
    /// properly record the command that created the pond.
    ///
    /// # Arguments
    /// * `pond_path` - Path to the pond directory
    /// * `create_new` - Whether to create a new pond vs open existing
    /// * `root_metadata` - Optional (txn_id, cli_args) for initial transaction
    /// * `preserve_metadata` - Optional pond metadata to preserve (for replicas)
    async fn create_infrastructure<P: AsRef<Path>>(
        pond_path: P,
        create_new: bool,
        txn_metadata: Option<PondUserMetadata>,
        preserve_metadata: Option<PondMetadata>,
        birthplace: Option<String>,
    ) -> Result<Self, StewardError> {
        let data_path = get_data_path(pond_path.as_ref());
        let control_path = get_control_path(pond_path.as_ref());

        debug!("opening pond: {:?}", pond_path.as_ref());

        // Create directories if they don't exist
        std::fs::create_dir_all(&data_path)?;
        std::fs::create_dir_all(&control_path)?;

        let data_path_str = data_path.to_string_lossy().to_string();
        let control_path_str = control_path.to_string_lossy().to_string();

        // Determine pond_id and initialize control table.
        //
        // D5.2: pond identity is canonically owned by the data Delta table's
        // bootstrap row (the root directory's OplogEntry, whose pond_id
        // partition value is the local pond's id).  The control table still
        // caches it (KEY_STORE_ID under BOOTSTRAP_POND_ID) for convenience,
        // but on disagreement the data table wins and the control table
        // cache is auto-healed to match.
        let (pond_id, control_table) = if create_new {
            // Creating new pond - use the preserved pond_id if restoring as a
            // replica, otherwise mint a fresh identity.  Either way the root
            // initialization below stamps it into the data table (canonical).
            let metadata = match preserve_metadata {
                Some(m) => m,
                None => PondMetadata {
                    birthplace: birthplace.unwrap_or_default(),
                    ..PondMetadata::default()
                },
            };
            let pond_id = metadata.pond_id.to_string();
            debug!(
                "Creating control table with pond identity: {}",
                metadata.pond_id
            );
            let ct = ControlTable::create(&control_path_str, &metadata).await?;
            (pond_id, ct)
        } else {
            // Opening existing pond - prefer the data table's bootstrap row,
            // fall back to (or cross-check against) the control table cache.
            let data_pond_id = OpLogPersistence::peek_pond_id(&data_path)
                .await
                .map_err(StewardError::DataInit)?;
            let mut ct = ControlTable::open(&control_path_str).await?;
            let ct_pond_id = ct.pond_metadata().pond_id.to_string();

            let pond_id = match data_pond_id {
                Some(from_data) => {
                    if from_data != ct_pond_id {
                        log::warn!(
                            "Pond identity disagreement: data table holds pond_id={} \
                             but control table caches pond_id={}; using data table \
                             value (canonical per D5.2) and healing control cache",
                            from_data,
                            ct_pond_id
                        );
                        // Auto-heal: rewrite the control cache to match the
                        // canonical data-table value.  Birth metadata (timestamp,
                        // birthplace, username) is preserved from the existing
                        // control cache - those fields live only there.
                        let healed = PondMetadata {
                            pond_id: from_data.parse::<uuid7::Uuid>().map_err(|e| {
                                StewardError::ControlTable(format!(
                                    "data table pond_id {} is not a valid UUID: {}",
                                    from_data, e
                                ))
                            })?,
                            birth_timestamp: ct.pond_metadata().birth_timestamp,
                            birthplace: ct.pond_metadata().birthplace.clone(),
                            birth_username: ct.pond_metadata().birth_username.clone(),
                        };
                        ct.set_pond_metadata(&healed).await?;
                    }
                    from_data
                }
                None => {
                    // Data table is empty (restoration scaffold awaiting bundles)
                    // or pre-D5 layout (already refused by open_or_create below).
                    // Either way, fall back to the control table cache.
                    debug!(
                        "Data table has no committed rows yet; using control \
                         table pond_id={} (restoration scaffold)",
                        ct_pond_id
                    );
                    ct_pond_id
                }
            };
            (pond_id, ct)
        };

        debug!("initializing data FS {data_path_str} with pond_id={pond_id}");

        // Initialize data filesystem - automatically creates root directory if create_new=true
        let data_persistence =
            OpLogPersistence::open_or_create(&data_path_str, pond_id, create_new, txn_metadata)
                .await
                .map_err(StewardError::DataInit)?;

        // Initialize last_write_seq from tlogfs (which loads from Delta metadata)
        // tlogfs is the authoritative source - it reads from actual Delta commits
        let last_seq = data_persistence.last_txn_seq();

        debug!(
            "Initialized last_seq={} from data_persistence (create_new={})",
            last_seq, create_new
        );

        Ok(Ship {
            data_persistence,
            control_table,
            last_write_seq: last_seq,
            pond_path: pond_path.as_ref().to_path_buf(),
        })
    }

    /// Execute operations within a scoped write transaction.
    ///
    /// Runs the async closure with `&FS` access, auto-commits on `Ok`,
    /// auto-aborts on `Err`. The closure's error type must convert into
    /// `StewardError` (which `tinyfs::Error` and `tlogfs::TLogFSError` do
    /// via `#[from]` impls -- just use `?`).
    pub async fn write_transaction<F>(
        &mut self,
        meta: &PondUserMetadata,
        f: F,
    ) -> Result<(), StewardError>
    where
        F: for<'a> AsyncFnOnce(&'a tinyfs::FS) -> Result<(), StewardError>,
    {
        debug!("Beginning write transaction {:?}", meta);
        let tx = self.begin_write(meta).await?;
        let result = f(&tx).await;
        match result {
            Ok(()) => {
                _ = tx.commit().await?;
                Ok(())
            }
            Err(e) => {
                let error_msg = format!("{}", e);
                debug!("Transaction failed: {error_msg}");
                Err(tx.abort_preserving(e).await)
            }
        }
    }

    /// Replay a transaction from backup with original sequence number
    ///
    /// This is specifically for pond restoration - it takes bundle files that already
    /// contain the Delta commit log with the correct txn_seq, and applies them directly.
    ///
    /// Unlike transact() which creates NEW transactions, this replays EXISTING transactions
    /// from backups, preserving their original sequence numbers.
    ///
    /// # Arguments
    /// * `txn_seq` - Original transaction sequence number from source pond
    /// * `args` - Original command arguments
    /// * `f` - Function to apply bundle files (receives transaction guard + state)
    ///
    /// # Returns
    /// Result from the function execution
    pub async fn replay_transaction<F, R>(
        &mut self,
        txn_meta: &PondTxnMetadata,
        f: F,
    ) -> Result<R, StewardError>
    where
        F: for<'a> FnOnce(
            &'a StewardTransactionGuard<'a>,
            &'a tinyfs::FS,
        ) -> std::pin::Pin<
            Box<dyn Future<Output = Result<R, StewardError>> + Send + 'a>,
        >,
    {
        debug!(
            "Replaying transaction txn_seq={} {:?}",
            txn_meta.txn_seq, txn_meta.user.args
        );

        // Begin transaction replay with specific sequence number
        let tx = self.begin_transaction_replay(txn_meta).await?;

        let result = f(&tx, &tx).await;

        match result {
            Ok(value) => {
                // Success - commit using steward guard (ensures proper sequencing)
                _ = tx.commit().await?;
                Ok(value)
            }
            Err(e) => {
                let error_msg = format!("{}", e);
                debug!("Transaction replay failed {error_msg}");
                Err(tx.abort_preserving(e).await)
            }
        }
    }

    /// Get a reference to the control table for querying pond settings
    #[must_use]
    pub fn control_table(&self) -> &ControlTable {
        &self.control_table
    }

    /// Get a mutable reference to the control table for modifying pond settings
    pub fn control_table_mut(&mut self) -> &mut ControlTable {
        &mut self.control_table
    }

    /// On-disk pond directory (the parent of `data/` and `control/`).
    /// Used by sync-remote integration for the steward adapter's `path()`.
    #[must_use]
    pub fn pond_path(&self) -> &Path {
        &self.pond_path
    }

    /// Return the durable write freeze, if this pond is frozen.
    pub fn write_freeze(&self) -> Result<Option<crate::WriteFreeze>, StewardError> {
        crate::write_lock::read_write_freeze(&get_control_path(&self.pond_path))
    }

    /// Atomically freeze data writes after all earlier writers have completed.
    ///
    /// The same process-level lock used by every data-write path serializes
    /// marker creation. If a writer is active, this fails with
    /// [`StewardError::PondLocked`] and the operator must retry.
    pub async fn freeze_writes(
        &mut self,
        meta: &PondUserMetadata,
        reason: String,
    ) -> Result<(crate::WriteFreeze, bool), StewardError> {
        let reason = reason.trim();
        if reason.is_empty() {
            return Err(StewardError::Aborted(
                "write freeze requires a non-empty reason".to_string(),
            ));
        }

        let control_dir = get_control_path(&self.pond_path);
        let txn_meta = PondTxnMetadata::new(self.last_write_seq + 1, meta.clone());
        let _write_lock = crate::write_lock::WriteLockGuard::try_acquire(&control_dir, &txn_meta)?;
        if let Some(existing) = crate::write_lock::read_write_freeze(&control_dir)? {
            return Ok((existing, false));
        }

        // Reload after acquiring the lock so a writer that completed between
        // opening Ship and acquiring exclusion cannot leave stale state here.
        let fresh_control = ControlTable::open(&control_dir).await?;
        let pond_id = fresh_control.get_pond_metadata().pond_id.to_string();
        let data_path = get_data_path(&self.pond_path);
        let data_url = url::Url::from_directory_path(&data_path)
            .or_else(|_| url::Url::from_file_path(&data_path))
            .map_err(|_| {
                StewardError::Aborted(format!(
                    "cannot form a URL from pond data path {}",
                    data_path.display()
                ))
            })?;
        let fresh_data = deltalake::open_table(data_url)
            .await
            .map_err(|error| StewardError::DeltaLake(error.to_string()))?;
        let source_tip = crate::content_tree::log_tip_commit_hash(fresh_data, &pond_id)
            .await?
            .map(|tip| tip.to_hex());
        let freeze = crate::WriteFreeze::new(pond_id, source_tip, reason.to_string());
        let created = crate::write_lock::create_write_freeze(&control_dir, &freeze)?;
        Ok((freeze, created))
    }

    /// Remove the durable write freeze while holding process-level exclusion.
    pub fn unfreeze_writes(
        &mut self,
        meta: &PondUserMetadata,
    ) -> Result<Option<crate::WriteFreeze>, StewardError> {
        let control_dir = get_control_path(&self.pond_path);
        let txn_meta = PondTxnMetadata::new(self.last_write_seq + 1, meta.clone());
        let _write_lock = crate::write_lock::WriteLockGuard::try_acquire(&control_dir, &txn_meta)?;
        crate::write_lock::remove_write_freeze(&control_dir)
    }

    /// Delete old control lifecycle rows under freeze-aware write exclusion.
    pub async fn prune_control_history(
        &mut self,
        horizon: i64,
        meta: &PondUserMetadata,
    ) -> Result<usize, StewardError> {
        let control_dir = get_control_path(&self.pond_path);
        let txn_meta = PondTxnMetadata::new(self.last_write_seq, meta.clone());
        let _write_lock =
            crate::write_lock::WriteLockGuard::try_acquire_for_write(&control_dir, &txn_meta)?;
        self.control_table.prune_below(horizon).await
    }

    /// Borrow the data persistence layer.  The sync-remote adapter uses
    /// this to read the underlying Delta table for `actions_at_version`
    /// and `read_data_file`.
    #[must_use]
    pub fn data_persistence(&self) -> &OpLogPersistence {
        &self.data_persistence
    }

    /// Mutable view of the data persistence layer.  The sync-remote
    /// adapter uses this to commit foreign Delta actions in
    /// `apply_pulled_bundle` and to drop pond rows.
    pub fn data_persistence_mut(&mut self) -> &mut OpLogPersistence {
        &mut self.data_persistence
    }

    /// Highest write txn_seq this Ship has allocated locally (0 if
    /// none).  The sync-remote `pond push` driver reads this to know
    /// the upper bound for its push loop.
    #[must_use]
    pub fn last_write_seq(&self) -> i64 {
        self.last_write_seq
    }

    /// Bump the in-memory `last_write_seq` allocator to at least `seq`.
    ///
    /// Used by `ShipRemoteSteward::apply_pulled_bundle` in MIRROR mode
    /// to keep Ship's allocator ahead of any sequence numbers that a
    /// pulled bundle injected into the data store.  Does nothing when
    /// `seq <= self.last_write_seq`.
    pub fn sync_last_write_seq(&mut self, seq: i64) {
        if seq > self.last_write_seq {
            debug!(
                "Advancing last_write_seq {} -> {} after pulled bundle apply",
                self.last_write_seq, seq
            );
            self.last_write_seq = seq;
        }
    }

    /// Begin a coordinated transaction with the given options
    pub async fn begin_read(
        &mut self,
        meta: &PondUserMetadata,
    ) -> Result<StewardTransactionGuard<'_>, StewardError> {
        self.begin_txn(false, meta).await
    }

    pub async fn begin_write(
        &mut self,
        meta: &PondUserMetadata,
    ) -> Result<StewardTransactionGuard<'_>, StewardError> {
        self.begin_txn(true, meta).await
    }

    /// Begin a write transaction whose commit will not trigger post-commit
    /// factory execution or remote auto-push (see
    /// [`StewardTransactionGuard::suppressing_post_commit`]).
    ///
    /// Restricted to the crate: used only by the staged capsule importer,
    /// which recreates `/system/run/*` and `/sys/remotes/*` namespace content
    /// verbatim from a capsule but must keep it inert until the operator
    /// explicitly seals and unsuppresses the target pond.
    pub(crate) async fn begin_write_suppressed(
        &mut self,
        meta: &PondUserMetadata,
    ) -> Result<StewardTransactionGuard<'_>, StewardError> {
        Ok(self.begin_txn(true, meta).await?.suppressing_post_commit())
    }

    async fn begin_txn(
        &mut self,
        is_write: bool,
        meta: &PondUserMetadata,
    ) -> Result<StewardTransactionGuard<'_>, StewardError> {
        debug!("Beginning steward transaction {meta:?}");

        // Compute the transaction sequence we *would* use, but do NOT
        // commit the increment yet: if the write-lock acquisition below
        // fails, we must leave `last_write_seq` untouched so the caller
        // can retry without skipping a sequence number.
        let (txn_seq, based_on_seq) = if is_write {
            (self.last_write_seq + 1, None)
        } else {
            let seq = self.last_write_seq;
            (seq, Some(seq))
        };

        let txn_meta = PondTxnMetadata::new(txn_seq, meta.clone());

        let transaction_type = if is_write {
            TransactionType::Write
        } else {
            TransactionType::Read
        };
        debug!(
            "Transaction {} allocated sequence {txn_seq} (type={:?}, based_on_seq={:?})",
            meta.txn_id, transaction_type, based_on_seq,
        );

        // For writes, acquire the process-level exclusion lock BEFORE
        // touching control or data tables.  A conflict here short-circuits
        // cleanly without polluting the control log.  Read transactions
        // do not take this lock (or any control-table record) — they're
        // safe under Delta's read-time-travel semantics.
        let write_lock = if is_write {
            let control_dir = get_control_path(&self.pond_path);
            Some(crate::write_lock::WriteLockGuard::try_acquire_for_write(
                &control_dir,
                &txn_meta,
            )?)
        } else {
            None
        };

        // Record transaction begin in control table — writes only.
        // Reads no longer leave audit rows; the per-read Delta write
        // was pure overhead now that exclusion is enforced by the lock.
        if is_write
            && let Err(error) = self
                .control_table
                .record_begin(&txn_meta, based_on_seq, transaction_type)
                .await
        {
            return Err(StewardError::ControlTable(format!(
                "Failed to record transaction begin: {error}"
            )));
        }

        // Begin Data FS transaction guard with metadata
        let data_tx_result = if is_write {
            self.data_persistence.begin_write(&txn_meta).await
        } else {
            self.data_persistence.begin_read(&txn_meta).await
        };
        let data_tx = match data_tx_result {
            Ok(data_tx) => data_tx,
            Err(error) => {
                if is_write
                    && let Err(record_error) = self
                        .control_table
                        .record_failed(&txn_meta, transaction_type, error.to_string(), 0)
                        .await
                {
                    log::error!("Failed to record data transaction begin failure: {record_error}");
                }
                return Err(StewardError::DataInit(error));
            }
        };
        if is_write {
            self.last_write_seq = txn_seq;
        }

        // Limiter usage queued by an earlier push rides out on this write
        // (Decision L12).  Spending happens during the post-commit push, which
        // cannot write the pond without recursing, so the samples wait in the
        // control table for a write that was going to happen anyway.  Read
        // transactions carry nothing.
        let pending_usage = if is_write {
            crate::limiter_usage::read_pending(&self.control_table).await
        } else {
            Vec::new()
        };

        // Create steward transaction guard with sequence tracking
        // Pass pond_path so guard can reload OpLogPersistence for post-commit
        let guard = StewardTransactionGuard::new(
            data_tx,
            &txn_meta,
            transaction_type,
            &mut self.control_table,
            &mut self.last_write_seq,
            &self.pond_path,
            write_lock,
        );

        if !pending_usage.is_empty() {
            // The rows go through this transaction, so they are durable only
            // if it commits -- which is also when the queue is drained.  A
            // failure here is logged and dropped: refusing a user's write
            // because a metric could not be recorded would trade a monitoring
            // gap for an outage.
            match crate::limiter_usage::emit(&guard, &pending_usage).await {
                Ok(()) => guard.note_emitted_usage(pending_usage.len()),
                Err(e) => log::warn!("limiter usage: failed to emit into the pond: {e}"),
            }
        }

        Ok(guard)
    }

    /// Replay a transaction from backup with a specific sequence number
    ///
    /// This is used during pond restoration to replay transactions with their ORIGINAL
    /// sequence numbers from the source pond. Unlike begin_transaction() which allocates
    /// a new sequence, this method uses the provided sequence number directly.
    ///
    /// # Critical Invariant
    /// The provided txn_seq MUST equal last_write_seq + 1, or restoration will fail.
    /// Bundles must be applied in strict sequential order.
    ///
    /// # Arguments
    /// * `txn_seq` - The original transaction sequence number from the source pond
    /// * `args` - The original command arguments that created this transaction
    /// * `variables` - Any template variables from the original transaction
    ///
    /// # Returns
    /// A transaction guard configured for the specific sequence number
    pub async fn begin_transaction_replay(
        &mut self,
        txn_meta: &PondTxnMetadata,
    ) -> Result<StewardTransactionGuard<'_>, StewardError> {
        let txn_id = txn_meta.user.txn_id;
        let txn_seq = txn_meta.txn_seq;
        debug!(
            "Replaying transaction at txn_seq={} {:?}",
            txn_seq, txn_meta.user.args
        );

        // Validate sequence - must be exactly next expected sequence
        let expected_seq = self.last_write_seq + 1;
        if txn_meta.txn_seq != expected_seq {
            return Err(StewardError::ControlTable(format!(
                "Transaction replay sequence mismatch: bundle has txn_seq={} but replica expects {}. \
                Bundles must be applied in sequential order without gaps.",
                txn_seq, expected_seq
            )));
        }

        debug!("Transaction replay {txn_id:?} using sequence {txn_seq} (type=write, replay=true)",);

        // Acquire process-level write lock (same exclusion contract as
        // begin_write).  Replay is a write transaction and must not race
        // with another writer in a sibling process.
        let control_dir = get_control_path(&self.pond_path);
        let write_lock =
            crate::write_lock::WriteLockGuard::try_acquire_for_write(&control_dir, txn_meta)?;

        // Record transaction begin in control table
        self.control_table
            .record_begin(
                txn_meta,
                None, // Replayed @@@ transactions are not "based on" anything
                TransactionType::Write,
            )
            .await
            .map_err(|e| {
                StewardError::ControlTable(format!("Failed to record transaction begin: {}", e))
            })?;

        // Begin Data FS transaction guard with metadata

        // For now, we still need to begin a transaction to get State access
        let data_tx = match self.data_persistence.begin_write(txn_meta).await {
            Ok(data_tx) => data_tx,
            Err(error) => {
                if let Err(record_error) = self
                    .control_table
                    .record_failed(txn_meta, TransactionType::Write, error.to_string(), 0)
                    .await
                {
                    log::error!(
                        "Failed to record replay data transaction begin failure: {record_error}"
                    );
                }
                return Err(StewardError::DataInit(error));
            }
        };
        self.last_write_seq = txn_seq;

        // Create steward transaction guard with sequence tracking
        Ok(StewardTransactionGuard::new(
            data_tx,
            txn_meta,
            TransactionType::Write,
            &mut self.control_table,
            &mut self.last_write_seq,
            self.pond_path.clone(),
            Some(write_lock),
        ))
    }

    /// Commit a steward transaction guard with proper sequencing
    ///
    /// Returns the data-FS Delta version of a successful write commit
    /// (`Ok(Some(v))`); returns `Ok(None)` for a read transaction or a
    /// write that produced no changes.
    pub async fn commit_transaction(
        &mut self,
        guard: StewardTransactionGuard<'_>,
    ) -> Result<Option<i64>, StewardError> {
        // Check if this is a write transaction before consuming the guard
        let is_write = guard.is_write_transaction();

        // Commit the guard (this releases the borrow on control_table)
        let commit_result = guard.commit().await?;

        // After a successful write, run automatic maintenance (checkpoint + vacuum)
        if is_write && commit_result.is_some() {
            match self.maintain(false, false).await {
                Ok(report) if report.data.is_some() || report.control.is_some() => {
                    debug!("Post-commit maintenance: {}", report);
                }
                Ok(_) => {}
                Err(error) => {
                    warn!("Post-commit maintenance skipped: {error}");
                }
            }
        }

        Ok(commit_result)
    }

    /// Run maintenance (checkpoint + vacuum) on both data and control tables.
    ///
    /// When `force` is true, checkpoint is always created (for explicit commands).
    /// When `compact` is true, the data table's own-pond partitions are
    /// compacted as a RECORDED, pushable transaction via [`Self::compact`]
    /// (so the merge replicates to remotes), and the control table is
    /// optimized best-effort (it is never pushed).
    pub async fn maintain(
        &mut self,
        force: bool,
        compact: bool,
    ) -> Result<MaintenanceReport, StewardError> {
        let mut report = MaintenanceReport::default();

        // Data table compaction goes through the recorded transaction path
        // so it is replicated (Begin/DataCommitted(Compact)).  The
        // checkpoint/vacuum that follow are best-effort and unrecorded.
        let compact_outcome = if compact {
            Some(self.compact().await?)
        } else {
            None
        };

        // Checkpoint, log cleanup, vacuum, and control optimization mutate
        // local storage too. Hold freeze-aware exclusion across all of them.
        let control_dir = get_control_path(&self.pond_path);
        let txn_meta = PondTxnMetadata::new(
            self.last_write_seq,
            PondUserMetadata::new(vec!["pond".to_string(), "maintain-storage".to_string()]),
        );
        let _write_lock =
            crate::write_lock::WriteLockGuard::try_acquire_for_write(&control_dir, &txn_meta)?;

        // Maintain data table (checkpoint + vacuum only; compaction handled
        // above).  Default retention is None (the table's 30-day default),
        // safe for replicated ponds whose commit log feeds remote diffing.
        // High-churn, fully-pushed instances (selfmon) can bound the data
        // _delta_log by setting maintenance.data_log_retention_minutes.
        let data_retention = self
            .control_table
            .get_setting(maintenance::KEY_DATA_LOG_RETENTION_MINUTES)
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|m| *m >= 0)
            .map(chrono::Duration::minutes);
        let data_table = self.data_persistence.table().clone();
        let (new_data_table, mut data_result) =
            maintenance::maintain_table(data_table, "data", force, false, data_retention).await?;
        self.data_persistence.set_table(new_data_table);
        if let Some(oc) = compact_outcome {
            data_result.compacted = oc.had_data;
            data_result.compact_files_added = oc.files_added;
            data_result.compact_files_removed = oc.files_removed;
        }
        report.data = Some(data_result);

        // Maintain control table (best-effort optimize allowed; never pushed).
        // It is never replicated, so its delta log is cleaned aggressively to a
        // short retention rather than the 30-day table default.
        let control_minutes = self
            .control_table
            .get_setting(maintenance::KEY_CONTROL_LOG_RETENTION_MINUTES)
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|m| *m >= 0)
            .unwrap_or(maintenance::CONTROL_LOG_RETENTION_MINUTES);
        let control_table = self.control_table.table().clone();
        let (new_control_table, control_result) = maintenance::maintain_table(
            control_table,
            "control",
            force,
            compact,
            Some(chrono::Duration::minutes(control_minutes)),
        )
        .await?;
        self.control_table.set_table(new_control_table);
        report.control = Some(control_result);

        Ok(report)
    }

    /// Report which series collapse would merge at `threshold`, and what
    /// merging them would cost, without merging anything.
    ///
    /// Runs only phase 1 of [`Ship::collapse_versions`] -- the same discovery,
    /// under a read transaction that takes no control-table records and
    /// consumes no sequence number -- so asking is free and leaves no trace.
    /// Sharing the discovery is the point: a preview that computed candidacy
    /// its own way could disagree with the collapse it claims to predict.
    ///
    /// # Errors
    /// Returns an error if the read transaction or the discovery query fails.
    pub async fn survey_collapsible_series(
        &mut self,
        threshold: usize,
    ) -> Result<Vec<tlogfs::CollapsibleSeries>, StewardError> {
        let meta = PondUserMetadata::new(vec![
            "pond".to_string(),
            "maintain".to_string(),
            "--dry-run".to_string(),
        ]);
        let tx = self.begin_read(&meta).await?;
        let found = {
            let state = tx.state()?;
            state.survey_collapsible_series(threshold).await?
        };
        _ = tx.commit().await?;
        Ok(found)
    }

    /// Map every node in the pond to its path, from stored directory rows.
    ///
    /// Reporting-only: it instantiates no node, so it cannot fail because some
    /// unrelated dynamic node's config does not expand.
    ///
    /// # Errors
    /// Returns an error if the read transaction or the directory scan fails.
    pub async fn node_paths(
        &mut self,
    ) -> Result<std::collections::HashMap<tinyfs::FileID, String>, StewardError> {
        let meta = PondUserMetadata::new(vec!["pond".to_string(), "node-paths".to_string()]);
        let tx = self.begin_read(&meta).await?;
        let result = async {
            let state = tx.state()?;
            Ok::<_, StewardError>(state.node_paths().await?)
        }
        .await;
        // Close the read transaction on both paths: a preview that leaves an
        // incomplete transaction behind has modified the pond it promised not
        // to touch.
        _ = tx.commit().await?;
        result
    }

    /// Report which native v2 series pack maintenance would repack at
    /// `threshold`, and the bounded physical layout it would publish,
    /// without writing anything.
    ///
    /// Shares its discovery (through `pack_maintenance`, a private module)
    /// with [`Ship::collapse_versions`], so a preview can never disagree
    /// with what a real run would do. Mutates nothing: only reads a coarse
    /// candidate list under a read transaction (no control-table records,
    /// no sequence number consumed) and this pond's own local `_packs`
    /// state.
    ///
    /// # Errors
    /// Returns an error if the discovery read transaction, a series'
    /// manifest fold, or this pond's local pack-index enumeration fails.
    pub async fn survey_pack_maintenance(
        &mut self,
        threshold: usize,
    ) -> Result<Vec<crate::PackMaintenanceCandidate>, StewardError> {
        crate::pack_maintenance::survey_pack_maintenance(self, threshold).await
    }

    /// Run pack-only physical maintenance: repack every native v2 series
    /// whose current physical representation exceeds `threshold` physical
    /// objects into a smaller, bounded set of content-addressed physical
    /// pack objects, published to this pond's own `data/_packs` namespace
    /// (`docs/logical-series-identity-design.md`).
    ///
    /// **This never rewrites or deletes an Oplog append row, never changes
    /// a `watertown.series.v2` manifest/tree/commit root, Delta version, or txn
    /// sequence, and never changes logical metadata.** Every physical
    /// object this publishes is durably written and content-addressed
    /// *before* the [`sync_store::content::PackIndex`] naming it is
    /// published, so a crash mid-repack can never expose an advertisement
    /// naming a missing object, and re-running is deterministic: a series
    /// already repacked to its achievable bounded floor is reported as
    /// already-bounded rather than repacked again (see
    /// [`CollapseReport::already_bounded`]).
    ///
    /// Reclamation (the pre-existing row/blob sweep, [`Ship::reclaim`])
    /// still runs unconditionally afterward, exactly as before: a pond can
    /// carry debt from before reclamation existed, from a crash between
    /// maintenance and reclaim, or from a blob staged by a transaction
    /// that never committed, and reclamation is otherwise only reachable
    /// through this method.
    ///
    /// # Errors
    /// Returns an error if discovery fails, if a series' recomputed leaf
    /// hash disagrees with its persisted value (corrupt row), if the
    /// assembled pack fails its own self-verification against the series'
    /// untouched manifest, if this pond's local pack-index state is
    /// malformed or names a cross-series pack (pack maintenance refuses to
    /// proceed rather than risk deleting data GC could not safely reason
    /// about), or if reclamation fails.
    pub async fn collapse_versions(
        &mut self,
        threshold: usize,
    ) -> Result<CollapseReport, StewardError> {
        let meta = PondUserMetadata::new(vec![
            "pond".to_string(),
            "maintain".to_string(),
            "--collapse-versions".to_string(),
        ]);

        // Coarse candidate count for reporting -- the exact same discovery
        // `pack_maintenance::run_pack_maintenance` reruns under its own
        // write lock below; a plain read here, taking no control-table
        // records and consuming no sequence number.
        let candidates = self.survey_collapsible_series(threshold).await?;

        let maintenance =
            crate::pack_maintenance::run_pack_maintenance(self, threshold, &meta).await?;

        let mut report = CollapseReport {
            candidates: candidates.len(),
            series_repacked: maintenance.series_repacked,
            already_bounded: maintenance.already_bounded,
            unsupported_legacy: maintenance.unsupported_legacy,
            pack_objects_written: maintenance.pack_objects_written,
            pack_bytes_written: maintenance.pack_bytes_written,
            pack_objects_removed: maintenance.pack_objects_removed,
            pack_bytes_freed: maintenance.pack_bytes_freed,
            reclaimed: crate::reclaim::ReclaimStats::default(),
        };

        // Reclaim unconditionally, whether or not any series needed
        // repacking: a pond can carry debt from before reclamation
        // existed, from a crash between maintenance and reclaim, or from a
        // blob staged by a transaction that never committed. Reclamation
        // is otherwise only reachable through this method, so gating it
        // behind pack maintenance finding work would make it permanently
        // unreachable for an already fully-bounded pond -- exactly the
        // ponds least in need of further pack work but still eligible for
        // reclamation.
        report.reclaimed = self.reclaim(&meta).await?;

        Ok(report)
    }

    /// Delete rows no reader can see and sweep the blobs they held down.
    ///
    /// Content-preserving and therefore invisible to replication: steward's
    /// content fold already prunes superseded rows, so `root_tree_hash` is
    /// unchanged and mirrors -- which never received them -- stay converged.
    /// `reclaim_superseded` asserts that rather than assuming it, and skips the
    /// irreversible blob sweep if it does not hold.
    ///
    /// Consuming no sequence of its own is what makes reclamation a tail of the
    /// collapse rather than a transaction: it introduces no new logical
    /// content, so there is nothing for a mirror to receive.  Its delete
    /// commits are therefore stamped as MAINTENANCE (`pond_maintenance`), not
    /// as `pond_txn`.  Stamping them with the collapse's `txn_seq` would make
    /// `reconstruct_txn_history` -- which yields one transaction per `pond_txn`
    /// commit -- emit N+1 entries for that one sequence, so `pond rebuild`
    /// would replay N+1 duplicate lifecycle records into the control table.
    /// Seq recovery on open is unaffected: it takes the per-pond MAX over
    /// `pond_txn` commits, and the collapse's own commit still carries it.
    async fn reclaim(
        &mut self,
        meta: &PondUserMetadata,
    ) -> Result<crate::reclaim::ReclaimStats, StewardError> {
        let pond_id = self.control_table.pond_id_uuid();
        let control_dir = get_control_path(&self.pond_path);
        let mut txn_meta = PondTxnMetadata::new(self.last_write_seq, meta.clone());
        txn_meta.pond_id = pond_id.to_string();

        // The sweep deletes every blob no committed row names, which it cannot
        // distinguish from a blob a concurrent writer has staged but not yet
        // committed.  Exclusion is the guarantee that makes it safe.
        let _write_lock =
            crate::write_lock::WriteLockGuard::try_acquire_for_write(&control_dir, &txn_meta)?;

        let (new_table, stats) = crate::reclaim::reclaim_superseded(
            self.data_persistence.table().clone(),
            &get_data_path(&self.pond_path),
            &pond_id.to_string(),
            txn_meta.to_delta_maintenance_metadata(),
        )
        .await?;
        self.data_persistence.set_table(new_table);
        Ok(stats)
    }

    /// Compact this pond's own data partitions as a RECORDED, pushable
    /// transaction.
    ///
    /// Unlike `maintain(force, /*compact=*/true)`'s historical behavior --
    /// a best-effort Delta optimize that was NOT recorded in the control
    /// table and therefore never replicated -- this writes a
    /// `Begin` / `DataCommitted(commit_kind=Compact)` transaction so that
    /// `Remote::push` emits a Compact bundle.  Downstream consumers can
    /// then use that bundle as a restart baseline (`pond restart-from-compact`),
    /// and `Remote::maintain` retention can prune the superseded Write
    /// bundles.  This closes the mirror retention-recovery loop end to end.
    ///
    /// Compaction must not change logical content: the per-partition Merkle
    /// checksums are snapshotted before and after the Delta optimize and
    /// asserted identical (a mismatch is a bug and aborts the txn with a
    /// `Failed` record).  A no-op compaction (nothing to merge) writes a
    /// `Completed` record only, leaves the committed sequence unchanged, and
    /// returns `had_data == false`.
    ///
    /// Like `Remote::restart_pond_from_compact`, the bundle built from this
    /// transaction is treated as a full snapshot of the pond at the compacted
    /// version; the optimize target size (128 MB) merges each partition's
    /// small files into one, so a producer that accumulates many small writes
    /// per partition yields a complete baseline.
    pub async fn compact(&mut self) -> Result<CompactOutcome, StewardError> {
        use chrono::Utc;

        let pond_id = self.control_table.pond_id_uuid();

        // 1. Allocate seq, acquire the write lock, advance, write Begin.
        let txn_seq = self.last_write_seq + 1;
        let meta = PondUserMetadata::new(vec![
            "pond".to_string(),
            "maintain".to_string(),
            "--compact".to_string(),
        ]);
        let txn_meta = PondTxnMetadata::new(txn_seq, meta);
        let started = Utc::now().timestamp_micros();

        let control_dir = get_control_path(&self.pond_path);
        let _write_lock =
            crate::write_lock::WriteLockGuard::try_acquire_for_write(&control_dir, &txn_meta)?;
        self.last_write_seq = txn_seq;

        self.control_table
            .record_begin(&txn_meta, None, TransactionType::Write)
            .await
            .map_err(|e| StewardError::ControlTable(format!("compact: record begin: {}", e)))?;

        // 2. Pre-compaction content root.  Compaction must be
        //    content-preserving, so the pond's `root_tree_hash` must be
        //    byte-identical before and after the optimize (Decision D9, step
        //    5b: the content root subsumes the retired partition checksums).
        let pre = crate::content_tree::compute_content_tree_for_table(
            self.data_persistence.table().clone(),
            &pond_id.to_string(),
        )
        .await
        .map_err(|e| StewardError::ControlTable(format!("compact: pre snapshot: {}", e)))?
        .root_tree_hash;

        // 3. Run the Delta optimize, scoped to our own pond_id partitions.
        //    Stamp the compaction's txn_seq into the commit's `pond_txn`
        //    metadata so `OpLogPersistence::open` recovers the right
        //    `last_txn_seq` on the next pond open (the optimize commit
        //    becomes the most recent Delta commit).
        let mut commit_meta = txn_meta.clone();
        commit_meta.pond_id = pond_id.to_string();
        let app_metadata = commit_meta.to_delta_metadata();
        let stats = match maintenance::compact_pond_partitions(
            self.data_persistence.table().clone(),
            pond_id,
            app_metadata,
        )
        .await
        {
            Ok((new_table, stats)) => {
                self.data_persistence.set_table(new_table);
                stats
            }
            Err(e) => {
                // The optimize produced no data Delta commit, so this seq is
                // not consumed on disk (`OpLogPersistence::last_txn_seq` is
                // still `txn_seq - 1`, and a reopen would recover `txn_seq - 1`
                // from the data history).  Roll the in-memory allocator back to
                // match so it stays in lockstep with `data_persistence`; the
                // next write will reuse this seq.  The terminal `Failed` record
                // closes the dangling `Begin`.
                self.last_write_seq = txn_seq - 1;
                let reason = format!("optimize failed: {}", e);
                self.record_compact_failed(&txn_meta, started, reason).await;
                return Err(StewardError::ControlTable(format!("compact: {}", e)));
            }
        };

        // 4. No-op: nothing merged.  Write Completed (terminal for a
        //    write-no-op) and do not advance the committed sequence.
        if stats.is_noop() {
            let duration_ms = ((Utc::now().timestamp_micros() - started) / 1000).max(0);
            self.control_table
                .record_completed(&txn_meta, TransactionType::Write, duration_ms)
                .await
                .map_err(|e| {
                    StewardError::ControlTable(format!("compact: record completed: {}", e))
                })?;
            // A no-op optimize commits no data, so `data_persistence`'s
            // `last_txn_seq` was never advanced to `txn_seq` and a reopen would
            // recover `txn_seq - 1` from the data history.  Roll the in-memory
            // allocator back to match, keeping `last_write_seq` in lockstep with
            // `data_persistence.last_txn_seq` (otherwise the next read/write
            // transaction on this Ship would fail the strict +1 sequence check).
            self.last_write_seq = txn_seq - 1;
            debug!("Compaction no-op at seq={} (nothing to merge)", txn_seq);
            return Ok(CompactOutcome {
                txn_seq,
                had_data: false,
                files_added: 0,
                files_removed: 0,
                data_delta_version: 0,
            });
        }

        // 5. Post-compaction content root + invariant: content must be
        //    unchanged.
        let post = crate::content_tree::compute_content_tree_for_table(
            self.data_persistence.table().clone(),
            &pond_id.to_string(),
        )
        .await
        .map_err(|e| StewardError::ControlTable(format!("compact: post snapshot: {}", e)))?
        .root_tree_hash;
        if let Err(e) = assert_compaction_invariant(pre, post) {
            let reason = format!("{}", e);
            self.record_compact_failed(&txn_meta, started, reason).await;
            return Err(e);
        }

        // 6. Record DataCommitted(Compact) at the new data Delta version.
        //    DataCommitted is the terminal record for a data write in the
        //    watertown control schema (no trailing Completed).
        let new_version = self.data_persistence.table().version().unwrap_or(-1);
        // Keep the in-memory persistence allocator in step with the
        // committed compaction seq (the optimize commit already carries it
        // on disk via `pond_txn`), so a subsequent read/write in this same
        // process validates against the right sequence.
        self.data_persistence
            .sync_last_txn_seq(&pond_id.to_string(), txn_seq);
        // Content-graph spine (Decision D9): compaction is content-preserving
        // (the invariant above asserts `root_tree_hash` is unchanged), so it is
        // transparent to the content graph and appends NO commit-log leaf --
        // like `git gc`/repack adding no commits.  Push/pull resolve the tip
        // from the last content-changing commit, whose root already matches, so
        // a compaction seq needs no spine of its own.
        let commit_spine = None;
        let duration_ms = ((Utc::now().timestamp_micros() - started) / 1000).max(0);
        // Partition checksums are retired (step 5b); the content invariant
        // above is the compaction integrity check.
        self.control_table
            .record_compact_committed(&txn_meta, new_version, duration_ms, commit_spine)
            .await
            .map_err(|e| StewardError::ControlTable(format!("compact: record committed: {}", e)))?;

        // Reconcile the transparency log against the pond-resident commit-log
        // node.  Compaction added no leaf, so this is a no-op unless a prior
        // commit's tile export was left lagging.
        crate::content_tree::materialize_tlog(
            &self.pond_path,
            self.data_persistence.table().clone(),
            pond_id,
        )
        .await;

        info!(
            "Compaction committed (seq={}, version={}, +{}/-{} files)",
            txn_seq, new_version, stats.files_added, stats.files_removed
        );
        Ok(CompactOutcome {
            txn_seq,
            had_data: true,
            files_added: stats.files_added,
            files_removed: stats.files_removed,
            data_delta_version: new_version,
        })
    }

    /// Best-effort `Failed` record for an aborted compaction transaction,
    /// so the `Begin` does not linger as an incomplete transaction.
    async fn record_compact_failed(
        &mut self,
        txn_meta: &PondTxnMetadata,
        started_micros: i64,
        reason: String,
    ) {
        let duration_ms = ((chrono::Utc::now().timestamp_micros() - started_micros) / 1000).max(0);
        if let Err(e) = self
            .control_table
            .record_failed(txn_meta, TransactionType::Write, reason, duration_ms)
            .await
        {
            warn!("compact: failed to record Failed terminal: {}", e);
        }
    }

    /// Check if recovery is needed by querying the control table
    /// Returns Ok(()) if no recovery needed, otherwise returns RecoveryNeeded error
    pub async fn check_recovery_needed(&mut self) -> Result<(), StewardError> {
        debug!("Checking if recovery is needed via control table");

        let incomplete = self.control_table.find_incomplete_transactions().await?;

        if let Some((txn_meta, data_fs_version)) = incomplete.first() {
            info!(
                "Recovery needed: incomplete transaction seq={}, id={}",
                txn_meta.txn_seq, txn_meta.user.txn_id
            );

            info!(
                "Transaction details: args={:?}, data_fs_version={}",
                txn_meta.user.args, data_fs_version
            );

            // Return RecoveryNeeded error with complete metadata
            return Err(StewardError::RecoveryNeeded {
                txn_meta: txn_meta.clone(),
            });
        }

        debug!("No recovery needed");
        Ok(())
    }

    /// Perform crash recovery by detecting incomplete transactions
    pub async fn recover(&mut self) -> Result<RecoveryResult, StewardError> {
        info!("Starting crash recovery process via control table");

        let incomplete = self.control_table.find_incomplete_transactions().await?;

        if incomplete.is_empty() {
            info!("No incomplete transactions found");
            return Ok(RecoveryResult {
                recovered_count: 0,
                was_needed: false,
            });
        }

        info!("Found {} incomplete transaction(s):", incomplete.len());
        for (txn_meta, data_fs_version) in &incomplete {
            info!(
                "  - Transaction seq={}, id={}, data_fs_version={}",
                txn_meta.txn_seq, txn_meta.user.txn_id, data_fs_version
            );
        }

        // Mark each incomplete transaction as completed
        // In the future, this will trigger replication coordination:
        // 1. Query data FS commit log for the transaction
        // 2. Bundle files that were created/modified
        // 3. Initiate replication to other nodes
        // 4. Then mark transaction as completed in control table

        for (txn_meta, _data_fs_version) in &incomplete {
            info!(
                "Marking transaction seq={}, id={} as recovered",
                txn_meta.txn_seq, txn_meta.user.txn_id
            );
            // Post-D5.7a.1: only write transactions leave Begin records,
            // so any incomplete transaction we recover here must be a
            // write that was interrupted (or a legacy Read-Begin from
            // an upgrade-in-place pond; `record_completed` is shape-
            // compatible with both).
            self.control_table
                .record_completed(txn_meta, TransactionType::Write, 0)
                .await?;
        }

        info!(
            "Recovery completed - marked {} transaction(s) as recovered",
            incomplete.len()
        );
        Ok(RecoveryResult {
            recovered_count: incomplete.len() as u64,
            was_needed: true,
        })
    }

    /// Initialize a complete pond following the proper initialization pattern
    pub async fn initialize_pond<P: AsRef<Path>>(
        pond_path: P,
        txn_meta: &PondUserMetadata,
    ) -> Result<Self, StewardError> {
        // Step 1: Set up filesystem infrastructure (creates root with txn_seq=1)
        let mut ship = Self::create_infrastructure(
            pond_path,
            true,
            Some(txn_meta.clone()),
            None, // No preserved metadata for fresh pond
            None, // birthplace unset for this legacy constructor
        )
        .await?;

        // Record root initialization in control table
        let txn_meta = PondTxnMetadata::new(1, txn_meta.clone());
        ship.control_table
            .record_begin(
                &txn_meta,
                None, // Hmm, or 0 @@@
                TransactionType::Write,
            )
            .await?;

        // Step 2: Use scoped transaction with init arguments
        // @@@ a litle awkward that we have two clones of PondTxnMetadata.
        ship.write_transaction(&txn_meta.user, async |fs| {
            // Step 3: Create initial pond directory structure (this generates actual filesystem operations)
            let data_root = fs.root().await?;
            _ = data_root.create_dir_path("/data").await?;

            // Transaction automatically commits on Ok return
            Ok(())
        })
        .await?;

        ship.control_table
            // D5.7a: bootstrap data_delta_version=0 record is never pushed.
            .record_data_committed(&txn_meta, TransactionType::Write, 0, 0, None)
            .await?;

        ship.control_table
            .record_completed(&txn_meta, TransactionType::Write, 0)
            .await?;

        ship.last_write_seq = 1;

        debug!("Pond fully initialized with transaction #1");
        Ok(ship)
    }

    /// Query OpLog records for a specific transaction sequence
    ///
    /// This is a testing API that provides access to the underlying OpLog records
    /// for verification purposes. Returns tuples of (node_id_hex, part_id_hex, version).
    ///
    /// # Arguments
    /// * `txn_seq` - The transaction sequence number to query, or None for all records
    ///
    /// # Returns
    /// Vector of tuples containing (node_id, part_id, version) as hex strings and i64
    pub async fn query_oplog_records(
        &self,
        txn_seq: Option<i64>,
    ) -> Result<Vec<(String, String, i64)>, StewardError> {
        self.data_persistence
            .query_oplog_by_txn_seq(txn_seq)
            .await
            .map_err(StewardError::DataInit)
    }
}

// Implement Debug for Ship
impl std::fmt::Debug for Ship {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ship")
            .field("pond_path", &self.pond_path)
            .field("has_data_persistence", &true)
            .finish()
    }
}

/// Verify that `pre` and `post` describe the same logical content: the pond's
/// `root_tree_hash` must be byte-identical.  Producer-side compaction
/// ([`Ship::compact`]) merges parquet files without changing any logical
/// entry, so this MUST hold; a violation indicates a bug (e.g. optimize
/// touched foreign rows or dropped data) and aborts the compaction
/// transaction.  Because the content root is lineage-independent, this is the
/// same guarantee replication relies on (Decision D9, step 5b).
fn assert_compaction_invariant(
    pre: sync_store::ObjectHash,
    post: sync_store::ObjectHash,
) -> Result<(), StewardError> {
    if pre != post {
        return Err(StewardError::ControlTable(format!(
            "compaction altered content root: pre={} post={}",
            pre.to_hex(),
            post.to_hex(),
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_ship_creation() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        // Use production initialization code (same as pond init)
        let ship = Ship::create_pond(&pond_path, "test-host")
            .await
            .expect("Failed to initialize pond");

        // Verify directories were created
        let data_path = get_data_path(&pond_path);
        let control_path = get_control_path(&pond_path);

        assert!(data_path.exists(), "Data directory should exist");
        assert!(control_path.exists(), "Control directory should exist");

        // Test that pond path is stored correctly
        assert_eq!(ship.pond_path, pond_path);

        // Test that we can open the same pond (like production commands do)
        let _opened_ship = Ship::open_pond(&pond_path)
            .await
            .expect("Should be able to open existing pond");
    }

    /// D5.2: when the data table's bootstrap row holds pond_id A and the
    /// control-table cache holds a different pond_id B, opening the pond
    /// must use A (data is canonical) and auto-heal the cache to A.
    #[tokio::test]
    async fn ship_open_prefers_data_pond_id_over_control_cache() {
        let temp_dir = tempdir().expect("temp dir");
        let pond_path = temp_dir.path().join("pond");

        // Create a fresh pond; both tables will agree on the same pond_id.
        let ship = Ship::create_pond(&pond_path, "test-host")
            .await
            .expect("create_pond failed");
        let canonical_id = ship.control_table.pond_metadata().pond_id.to_string();
        drop(ship);

        // Verify the data table's bootstrap row matches.
        let data_path = get_data_path(&pond_path);
        let from_data = OpLogPersistence::peek_pond_id(&data_path)
            .await
            .expect("peek_pond_id failed")
            .expect("data table should hold the local pond_id");
        assert_eq!(from_data, canonical_id);

        // Tamper the control table: rewrite its cached pond_id to a
        // different value while leaving the data table untouched.
        let tampered_meta = PondMetadata::default();
        let tampered_id = tampered_meta.pond_id.to_string();
        assert_ne!(tampered_id, canonical_id);
        {
            let mut opened = Ship::open_pond(&pond_path).await.expect("open_pond failed");
            opened
                .control_table
                .set_pond_metadata(&tampered_meta)
                .await
                .expect("set_pond_metadata failed");
        }

        // Reopen.  The opened ship should prefer the data-table pond_id
        // and auto-heal the control cache back to it.
        let reopened = Ship::open_pond(&pond_path).await.expect("reopen failed");
        let after_id = reopened.control_table.pond_metadata().pond_id.to_string();
        assert_eq!(
            after_id, canonical_id,
            "data-table pond_id must win on disagreement (got {} after tampering with {})",
            after_id, tampered_id
        );

        // The healing is persisted: a third open should see the canonical
        // pond_id in the control cache without any warning.
        drop(reopened);
        let again = Ship::open_pond(&pond_path)
            .await
            .expect("third open failed");
        assert_eq!(
            again.control_table.pond_metadata().pond_id.to_string(),
            canonical_id
        );
    }

    #[tokio::test]
    async fn test_ship_commit_transaction() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        // Use the same constructor as production (pond init)
        let mut ship = Ship::create_pond(&pond_path, "test-host")
            .await
            .expect("Failed to initialize pond");

        // Begin a second transaction with test arguments using scoped transaction
        let args = vec!["test".to_string(), "arg1".to_string(), "arg2".to_string()];
        let meta = PondUserMetadata::new(args);
        ship.write_transaction(&meta, async |fs| {
            // Do some filesystem operation to ensure the transaction has operations to commit
            let root = fs.root().await?;
            _ = tinyfs::async_helpers::convenience::create_file_path(
                &root,
                "/test.txt",
                b"test content",
            )
            .await?;
            Ok(())
        })
        .await
        .expect("Failed to execute scoped transaction");
    }

    #[tokio::test]
    async fn test_normal_commit_transaction() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        // Use production initialization (pond init) - this creates version 0
        let mut ship = Ship::create_pond(&pond_path, "test-host")
            .await
            .expect("Failed to initialize pond");

        // Check that recovery is not needed after init
        ship.check_recovery_needed()
            .await
            .expect("Recovery should not be needed after init");

        // Begin a second transaction with arguments using scoped transaction
        let args = vec!["test".to_string(), "arg1".to_string(), "arg2".to_string()];
        let meta = PondUserMetadata::new(args.clone());
        ship.write_transaction(&meta, async |fs| {
            // Do some operation on data filesystem
            let data_root = fs.root().await?;
            _ = tinyfs::async_helpers::convenience::create_file_path(
                &data_root,
                "/test.txt",
                b"test content",
            )
            .await?;
            Ok(())
        })
        .await
        .expect("Failed to execute scoped transaction");

        // Check that recovery is still not needed after successful commit
        let recovery_check = ship.check_recovery_needed().await;
        match recovery_check {
            Ok(()) => {
                // This is expected for a successful commit with no recovery needed
            }
            Err(StewardError::RecoveryNeeded { txn_meta }) => {
                panic!(
                    "Recovery should not be needed after successful commit, but got recovery needed for seq={}, id={}: {:?}",
                    txn_meta.txn_seq, txn_meta.user.txn_id, txn_meta.user.args
                );
            }
            Err(e) => {
                panic!("Unexpected error during recovery check: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_crash_recovery_scenario() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        // FIRST: Create a properly initialized pond and simulate a crash scenario
        {
            let mut ship = Ship::create_pond(&pond_path, "test-host")
                .await
                .expect("Failed to create ship");

            // Use coordinated transaction to simulate crash between data commit and control commit
            let meta = PondUserMetadata::new(vec![
                "copy".to_string(),
                "file1.txt".to_string(),
                "file2.txt".to_string(),
            ]);

            // Step 1-3: Begin transaction, modify, commit data FS
            let mut tx = ship
                .begin_write(&meta)
                .await
                .expect("Failed to begin transaction");

            // Get data FS root from the transaction guard
            let data_root = tx.root().await.expect("Failed to get data root");

            // Modify data during the transaction
            _ = tinyfs::async_helpers::convenience::create_file_path(
                &data_root,
                "/file1.txt",
                b"content1",
            )
            .await
            .expect("Failed to create file");

            // For testing purposes, we need to manually commit without using the steward commit logic
            // This simulates a crash where the data transaction commits but control metadata is missing

            // Extract the raw transaction guard for direct commit (testing only)
            let raw_tx = tx
                .take_transaction()
                .expect("Transaction guard should be available");

            // Commit the transaction (metadata was already provided at begin)
            let _committed_version = raw_tx
                .commit()
                .await
                .expect("Failed to commit transaction")
                .0
                .expect("Transaction should have committed with operations");

            // SIMULATE CRASH HERE - don't call commit_control_metadata()
            // This leaves data committed but control metadata missing

            debug!("[OK] Simulated crash after data commit");
        } // Ship drops here, simulating crash

        // SECOND: Create a new ship (simulating restart) and test recovery
        {
            let mut ship = Ship::open_pond(&pond_path)
                .await
                .expect("Failed to open pond after crash");

            // Recovery should be needed because control metadata is missing
            let recovery_result = match ship.check_recovery_needed().await {
                Err(StewardError::RecoveryNeeded { txn_meta }) => {
                    debug!(
                        "[OK] Detected recovery needed for seq={}, txn_id: {}",
                        txn_meta.txn_seq, txn_meta.user.txn_id
                    );
                    debug!("[OK] Recovery metadata: {:?}", txn_meta);
                    // Perform actual recovery
                    ship.recover().await.expect("Recovery should succeed")
                }
                Ok(()) => panic!("Recovery should have been needed after crash"),
                Err(e) => panic!("Unexpected error during recovery check: {:?}", e),
            };

            assert!(
                recovery_result.was_needed,
                "Recovery should have been needed"
            );
            assert_eq!(
                recovery_result.recovered_count, 1,
                "Should have recovered one transaction"
            );

            // After recovery, no further recovery should be needed
            ship.check_recovery_needed()
                .await
                .expect("Recovery should not be needed after successful recovery");

            // Verify data survived the crash and recovery (use read transaction)
            let tx = ship
                .begin_read(&PondUserMetadata::new(vec!["verify".to_string()]))
                .await
                .expect("Failed to begin read transaction");
            let data_root = tx.root().await.expect("Failed to get data root");
            let file_content = data_root
                .read_file_path_to_vec("/file1.txt")
                .await
                .expect("File should exist after recovery");
            assert_eq!(file_content, b"content1");
            _ = tx
                .commit()
                .await
                .expect("Failed to commit read transaction");

            debug!("[OK] Recovery completed successfully");
        }
    }

    #[tokio::test]
    async fn test_multiple_transaction_recovery() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        let mut ship = Ship::create_pond(&pond_path, "test-host")
            .await
            .expect("Failed to create ship");

        // Commit several transactions normally using scoped pattern
        for i in 1..=3 {
            let args = vec!["test".to_string(), format!("operation{}", i)];
            let meta = PondUserMetadata::new(args);
            ship.write_transaction(&meta, async |fs| {
                let data_root = fs.root().await?;
                _ = tinyfs::async_helpers::convenience::create_file_path(
                    &data_root,
                    &format!("/file{}.txt", i),
                    format!("content{}", i).as_bytes(),
                )
                .await?;
                Ok(())
            })
            .await
            .expect("Failed to execute scoped transaction");
        }

        // The key test is that recovery works with existing completed transactions
        let recovery_result = ship.recover().await.expect("Failed to recover");

        // Since all transactions were completed normally, no recovery should be needed
        assert_eq!(
            recovery_result.recovered_count, 0,
            "No recovery should be needed for completed transactions"
        );
        assert!(
            !recovery_result.was_needed,
            "Recovery should not have been needed"
        );

        ship.check_recovery_needed()
            .await
            .expect("Recovery should not be needed after recovery");
    }

    #[tokio::test]
    async fn test_recovery_command_interface() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        let mut ship = Ship::create_pond(&pond_path, "test-host")
            .await
            .expect("Failed to create ship");

        // Test recover when no recovery is needed
        let recovery_result = ship.recover().await.expect("Failed to execute recovery");
        assert_eq!(recovery_result.recovered_count, 0);
        assert!(!recovery_result.was_needed);
    }

    #[tokio::test]
    async fn test_crash_recovery_with_metadata_extraction() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        // Step 1: Initialize pond using production code (pond init)
        {
            let _ship = Ship::create_pond(&pond_path, "test-host")
                .await
                .expect("Failed to initialize pond");
        }

        // Step 2: Create a transaction with metadata that commits to data FS but crashes before control FS
        {
            // Use production code to open existing pond
            let mut ship = Ship::open_pond(&pond_path)
                .await
                .expect("Failed to open existing pond");

            // Simulate a crash scenario using coordinated transaction approach
            let copy_args = vec![
                "pond".to_string(),
                "copy".to_string(),
                "source.txt".to_string(),
                "dest.txt".to_string(),
            ];

            let mut tx = {
                // Pass copy_args to begin_transaction so they're recorded in control table
                let tx = ship
                    .begin_write(&PondUserMetadata::new(copy_args.clone()))
                    .await
                    .expect("Failed to begin transaction");

                // Get data FS root from the transaction guard
                let data_root = tx.root().await.expect("Failed to get data root");

                // Do actual file operation
                _ = tinyfs::async_helpers::convenience::create_file_path(
                    &data_root,
                    "/dest.txt",
                    b"copied content",
                )
                .await
                .expect("Failed to create dest file");

                tx
            };

            // For testing purposes, we need to manually commit without using the steward commit logic
            // This simulates a crash where the data transaction commits but control metadata is missing

            // Extract the raw transaction guard for direct commit (testing only)
            let raw_tx = tx
                .take_transaction()
                .expect("Transaction guard should be available");

            // Commit the transaction (metadata was already provided at begin)
            let _committed_version = raw_tx
                .commit()
                .await
                .expect("Failed to commit transaction")
                .0
                .expect("Transaction should have committed with operations");

            // SIMULATE CRASH: Don't record control metadata
            debug!("[OK] Simulated crash after data commit");
        }

        // Step 3: Recovery after crash using production code
        {
            // Use production code to open existing pond
            let mut ship = Ship::open_pond(&pond_path)
                .await
                .expect("Failed to open existing pond for recovery");

            // Check that recovery is needed
            let check_result = ship.check_recovery_needed().await;
            assert!(
                check_result.is_err(),
                "Should detect that recovery is needed"
            );

            let recovered_txn_meta =
                if let Err(StewardError::RecoveryNeeded { txn_meta }) = check_result {
                    debug!(
                        "Should need recovery for seq={}, txn_id: {}",
                        txn_meta.txn_seq, txn_meta.user.txn_id
                    );
                    debug!("Recovery metadata: {:?}", txn_meta);
                    txn_meta
                } else {
                    panic!("Expected RecoveryNeeded error, got: {:?}", check_result);
                };

            // Perform recovery
            let recovery_result = ship.recover().await.expect("Recovery should succeed");
            assert_eq!(
                recovery_result.recovered_count, 1,
                "Should recover exactly 1 transaction"
            );
            assert!(
                recovery_result.was_needed,
                "Recovery should have been needed"
            );

            // Verify recovery is no longer needed
            ship.check_recovery_needed()
                .await
                .expect("Recovery should not be needed after successful recovery");

            // Verify the recovered transaction descriptor.  In the
            // post-D2 lean schema the control table no longer persists
            // CLI args, so the recovered metadata carries the original
            // txn_seq and txn_id but an empty args vec.  Recovery
            // tooling that needs the original command must source it
            // from the data Delta commit metadata (`pond_txn`).
            assert!(
                recovered_txn_meta.user.args.is_empty(),
                "Post-D2 control table does not persist CLI args; \
                 recovered args must be empty, got {:?}",
                recovered_txn_meta.user.args
            );
            assert_eq!(
                recovered_txn_meta.txn_seq, 2,
                "Recovered txn_seq must match the incomplete write (root=1, this=2)"
            );

            // Verify the data file still exists (use read transaction)
            let tx = ship
                .begin_read(&PondUserMetadata::new(vec!["verify".to_string()]))
                .await
                .expect("Failed to begin read transaction");
            let data_root = tx.root().await.expect("Failed to get data root");
            let reader = data_root
                .async_reader_path("/dest.txt")
                .await
                .expect("File should exist after recovery");
            let file_content = tinyfs::buffer_helpers::read_all_to_vec(reader)
                .await
                .expect("Failed to read file content");
            assert_eq!(file_content, b"copied content");
            _ = tx
                .commit()
                .await
                .expect("Failed to commit read transaction");
        }
    }

    #[tokio::test]
    async fn test_no_recovery_needed_for_consistent_state() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        let mut ship = Ship::create_pond(&pond_path, "test-host")
            .await
            .expect("Failed to create ship");

        // Do normal complete transactions using scoped pattern
        for i in 1..=3 {
            let args = vec![
                "pond".to_string(),
                "mkdir".to_string(),
                format!("/dir{}", i),
            ];
            let meta = PondUserMetadata::new(args);
            ship.write_transaction(&meta, async |fs| {
                let data_root = fs.root().await?;
                _ = data_root.create_dir_path(&format!("/dir{}", i)).await?;
                Ok(())
            })
            .await
            .expect("Failed to execute scoped transaction");
        }

        // Check that no recovery is needed
        ship.check_recovery_needed()
            .await
            .expect("No recovery should be needed for consistent state");

        // Run recovery anyway - should be no-op
        let recovery_result = ship
            .recover()
            .await
            .expect("Recovery should succeed even when not needed");
        assert_eq!(recovery_result.recovered_count, 0);
        assert!(!recovery_result.was_needed);

        // State should still be consistent
        ship.check_recovery_needed()
            .await
            .expect("State should remain consistent after no-op recovery");
    }

    #[tokio::test]
    async fn test_new_transaction_api() {
        // Test the new begin_transaction API to ensure it has proper sequencing
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond_new_api");

        // Initialize a new pond
        let mut ship = Ship::create_pond(&pond_path, "test-host")
            .await
            .expect("Failed to create pond");

        // Test: Use begin_transaction - this demonstrates the API
        let tx = ship
            .begin_write(&PondUserMetadata::new(vec!["test".to_string()]))
            .await
            .expect("Failed to begin transaction");

        // Perform some work
        let root = tx.root().await.expect("Failed to get root");
        _ = tinyfs::async_helpers::convenience::create_file_path(
            &root,
            "/test_file.txt",
            b"test content",
        )
        .await
        .expect("Failed to create file");

        // The commit step demonstrates the borrow checker challenge
        // ship.commit_transaction(tx).await - this would fail due to borrow checker
        // Instead, we rely on the steward guard's commit method which has the proper sequencing
        _ = tx
            .commit()
            .await
            .expect("Failed to commit steward transaction");

        debug!("[OK] New transaction API works with proper sequencing via steward guard commit");
    }

    #[tokio::test]
    async fn test_transaction_sequence_numbering() {
        // Test that transaction sequences are allocated correctly without conflicts
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond_sequences");

        // Initialize pond - this creates root directory with txn_seq=1
        let mut ship = Ship::create_pond(&pond_path, "test-host")
            .await
            .expect("Failed to create pond");

        // Verify initial sequence is 1 (root directory was created with txn_seq=1)
        let initial_seq = ship.last_write_seq;
        assert_eq!(
            initial_seq, 1,
            "Initial sequence should be 1 after root directory creation"
        );

        // First user transaction should get txn_seq=2
        let meta = PondUserMetadata::new(vec!["first".to_string()]);
        ship.write_transaction(&meta, async |fs| {
            let root = fs.root().await?;
            _ = tinyfs::async_helpers::convenience::create_file_path(
                &root,
                "/file1.txt",
                b"content1",
            )
            .await?;
            Ok(())
        })
        .await
        .expect("First transaction should succeed");

        // Verify sequence advanced to 2
        let after_first = ship.last_write_seq;

        assert_eq!(
            after_first, 2,
            "After first user transaction, sequence should be 2"
        );

        // Second user transaction should get txn_seq=3
        let meta = PondUserMetadata::new(vec!["second".to_string()]);
        ship.write_transaction(&meta, async |fs| {
            let root = fs.root().await?;
            _ = tinyfs::async_helpers::convenience::create_file_path(
                &root,
                "/file2.txt",
                b"content2",
            )
            .await?;
            Ok(())
        })
        .await
        .expect("Second transaction should succeed");

        // Verify sequence advanced to 3
        let after_second = ship.last_write_seq;
        assert_eq!(
            after_second, 3,
            "After second user transaction, sequence should be 3"
        );

        // Verify no conflicts: root=1, first=2, second=3
        debug!("Transaction sequence test passed: root=1, first_user=2, second_user=3");
    }

    #[tokio::test]
    async fn test_root_directory_version_and_control_table() {
        // Test that root initialization creates v1 and records it in control table
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_root_version");

        // Initialize pond
        let ship = Ship::create_pond(&pond_path, "test-host")
            .await
            .expect("Failed to create pond");

        // Verify that last_write_seq is 1 after root initialization
        let last_seq = ship.last_write_seq;
        assert_eq!(last_seq, 1, "After root init, last_write_seq should be 1");

        // Verify control table has the root transaction recorded
        let control_last_seq = ship
            .control_table
            .get_last_write_sequence()
            .await
            .expect("Failed to query control table");
        assert_eq!(
            control_last_seq, 1,
            "Control table should show last write sequence as 1"
        );

        // Reopen the pond to verify persistence
        drop(ship);
        let ship2 = Ship::open_pond(&pond_path)
            .await
            .expect("Failed to reopen pond");

        let reopened_seq = ship2.last_write_seq;
        assert_eq!(
            reopened_seq, 1,
            "After reopening, last_write_seq should still be 1"
        );

        debug!("[OK] Root directory initialization is recorded in control table with txn_seq=1");
    }

    #[tokio::test]
    async fn test_directory_tree_single_version_per_node() {
        // Test that creating a tree of directories in one transaction creates exactly one version per node
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_dir_tree");

        // Initialize pond
        let mut ship = Ship::create_pond(&pond_path, "test-host")
            .await
            .expect("Failed to create pond");

        // Create a tree of directories in a single transaction
        let meta = PondUserMetadata::new(vec!["create_tree".to_string()]);
        ship.write_transaction(&meta, async |fs| {
            let root = fs.root().await?;

            // Create nested directory structure: /a/b/c and /a/d/e
            _ = root.create_dir_path("/a").await?;
            _ = root.create_dir_path("/a/b").await?;
            _ = root.create_dir_path("/a/b/c").await?;
            _ = root.create_dir_path("/a/d").await?;
            _ = root.create_dir_path("/a/d/e").await?;

            Ok(())
        })
        .await
        .expect("Failed to create directory tree");

        // Verify the transaction sequence advanced properly
        let after_tree = ship.last_write_seq;

        assert_eq!(
            after_tree, 2,
            "After creating directory tree, sequence should be 2"
        );

        // Query OpLog records for transaction 2 to verify version numbers
        let records = ship
            .query_oplog_records(Some(2))
            .await
            .expect("Failed to query OpLog records");

        debug!("Found {} records in txn_seq=2", records.len());
        for (node_id, part_id, version) in records.iter() {
            debug!(
                "  Record: node_id={}, part_id={}, version={}",
                node_id, part_id, version
            );
        }

        // Group records by node_id to count versions per node
        use std::collections::HashMap;
        let mut node_versions: HashMap<String, Vec<i64>> = HashMap::new();

        for (node_id, _part_id, version) in records.iter() {
            node_versions
                .entry(node_id.clone())
                .or_default()
                .push(*version);
        }

        // Verify each node has exactly one version in transaction 2
        for (node_id, versions) in node_versions.iter() {
            assert_eq!(
                versions.len(),
                1,
                "Node {} should have exactly 1 version in txn_seq=2, but has {} versions: {:?}",
                node_id,
                versions.len(),
                versions
            );

            // Verify version numbers: root gets v2, new nodes get v1
            // Node IDs are stored in full UUID format, but root is identified by starting with "00000000"
            if node_id == tinyfs::ROOT_UUID {
                assert_eq!(
                    versions[0], 2,
                    "Root directory ({}) should have v2 in second transaction",
                    node_id
                );
            } else if node_id == tinyfs::INDEX_NODE_UUID {
                // The reserved node-manifest index node is created on this first
                // write transaction, so it carries v1 (design
                // `docs/incremental-content-tree-design.md` Phase 2).
                assert_eq!(
                    versions[0], 1,
                    "Index node ({}) should have v1 when first created",
                    node_id
                );
            } else if node_id == tinyfs::LOG_NODE_UUID {
                // The reserved commit-log node is created on this first
                // content-changing write, so it carries v1 (Decision D9).
                assert_eq!(
                    versions[0], 1,
                    "Log node ({}) should have v1 when first created",
                    node_id
                );
            } else {
                assert_eq!(
                    versions[0], 1,
                    "New directory {} should have v1, got v{}",
                    node_id, versions[0]
                );
            }
        }

        // Verify we created the expected number of directories
        // Root (updated) + /a + /a/b + /a/b/c + /a/d + /a/d/e = 6 nodes, plus
        // the reserved node-manifest index node and the reserved commit-log
        // node, both created on this first write = 8.
        assert_eq!(
            node_versions.len(),
            8,
            "Should have 8 nodes in txn_seq=2 (root + 5 new dirs + index node + log node), got: {:?}",
            node_versions.keys().collect::<Vec<_>>()
        );

        debug!(
            "[OK] Directory tree creation produces exactly one version per node with correct numbering"
        );
    }

    #[tokio::test]
    async fn test_index_node_phase2() {
        // Phase 2 (docs/incremental-content-tree-design.md): the reserved
        // node-manifest index node is created on the first write, updated as a
        // single collapsed live version thereafter, carries the full node
        // manifest, is excluded from the fold, and is hidden from listings.
        use sync_store::content::decode_manifest;

        const INDEX_NAME: &str = ".pond-node-index";

        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_index_node");
        let mut ship = Ship::create_pond(&pond_path, "test-host")
            .await
            .expect("Failed to create pond");

        // First write: create two directories.
        let meta = PondUserMetadata::new(vec!["w1".to_string()]);
        ship.write_transaction(&meta, async |fs| {
            let root = fs.root().await?;
            _ = root.create_dir_path("/a").await?;
            _ = root.create_dir_path("/b").await?;
            Ok(())
        })
        .await
        .expect("first write");

        // Second write: add a nested directory.
        let meta = PondUserMetadata::new(vec!["w2".to_string()]);
        ship.write_transaction(&meta, async |fs| {
            let root = fs.root().await?;
            _ = root.create_dir_path("/a/c").await?;
            Ok(())
        })
        .await
        .expect("second write");

        // Read the index node and the current content-tree report.
        let report = crate::content_tree::compute_content_tree(&ship)
            .await
            .expect("content tree");

        let meta = PondUserMetadata::new(vec!["read".to_string()]);
        let tx = ship.begin_read(&meta).await.expect("begin read");
        let root = tx.root().await.expect("root");

        // The index node is addressable by name for the write path...
        let index_bytes = root
            .read_file_path_to_vec(INDEX_NAME)
            .await
            .expect("index node exists and is readable");

        // ...but hidden from user-facing enumeration.
        let matches = root.collect_matches("/*").await.expect("list root");
        let names: Vec<String> = matches.iter().map(|(np, _)| np.basename()).collect();
        assert!(
            !names.iter().any(|n| n == INDEX_NAME),
            "index node must not appear in directory listings, got: {names:?}"
        );
        assert!(names.iter().any(|n| n == "a"));
        assert!(names.iter().any(|n| n == "b"));
        _ = tx.commit().await.expect("commit read");

        // The manifest carries every real node exactly once, and never itself.
        let manifest = decode_manifest(&index_bytes).expect("decode manifest");
        assert!(
            !manifest
                .iter()
                .any(|e| e.node_id == tinyfs::INDEX_NODE_UUID),
            "index node must be excluded from its own manifest (fold exclusion)"
        );
        // root + a + b + a/c = 4 real nodes; the index node is excluded.
        assert_eq!(
            manifest.len(),
            4,
            "manifest lists exactly the four real nodes, got: {:?}",
            manifest.iter().map(|e| &e.name).collect::<Vec<_>>()
        );
        assert!(manifest.iter().any(|e| e.name == "a"));
        assert!(manifest.iter().any(|e| e.name == "b"));
        assert!(manifest.iter().any(|e| e.name == "c"));

        // The manifest's root entry equals the fold's root_tree_hash: writing
        // the index node never perturbs the excluded-from-fold root hash.
        let root_entry = manifest
            .iter()
            .find(|e| e.parent_node_id.is_empty())
            .expect("root manifest entry");
        assert_eq!(
            root_entry.child_hash, report.root_tree_hash,
            "manifest root hash matches the content-tree root_tree_hash"
        );

        // The index node stays at a single live version: pack maintenance that
        // targets anything with more than one live version finds no candidate
        // (the directories are single-version, and the index node collapses on
        // every write), so it is never a pack maintenance candidate.
        let report = ship.collapse_versions(1).await.expect("collapse sweep");
        assert_eq!(
            report.candidates, 0,
            "index node self-collapses and is not a pack maintenance candidate"
        );
        assert_eq!(report.series_repacked, 0);
    }

    /// `Ship::collapse_versions` now performs real, pack-only physical
    /// maintenance instead of gating (`docs/logical-series-identity-design.md`):
    /// once a series exceeds the requested threshold's physical object
    /// count, it is repacked into a bounded physical pack published under
    /// this pond's own `data/_packs`, while every Oplog row, the series'
    /// `watertown.series.v2` manifest, and the Delta write sequence are left
    /// completely untouched. A threshold nobody exceeds still no-ops
    /// cleanly, and repeated maintenance settles (idempotent).
    #[tokio::test]
    async fn test_collapse_versions_repacks_once_a_real_candidate_exists() {
        use tinyfs::ResultExt;
        use tokio::io::AsyncWriteExt;

        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond_collapse");
        let mut ship = Ship::create_pond(&pond_path, "test-host")
            .await
            .expect("Failed to create pond");

        let file_path = "/data/events.csv";
        let chunks: [&[u8]; 4] = [b"name,value\n", b"alice,1\n", b"bob,2\n", b"carol,3\n"];
        let mut cumulative: Vec<u8> = Vec::new();

        for (i, chunk) in chunks.iter().enumerate() {
            cumulative.extend_from_slice(chunk);
            let meta = PondUserMetadata::new(vec![format!("write{i}")]);
            let chunk = chunk.to_vec();
            let first = i == 0;
            ship.write_transaction(&meta, async move |fs| {
                let root = fs.root().await?;
                if first {
                    _ = root.create_dir_path("data").await?;
                }
                let mut writer = root
                    .async_writer_path_with_type(file_path, tinyfs::EntryType::FilePhysicalSeries)
                    .await?;
                writer.write_all(&chunk).await.map_other()?;
                writer.shutdown().await.map_other()?;
                Ok(())
            })
            .await
            .expect("series write transaction");
        }

        // A sweep below the version count finds nothing and opens no write txn.
        let seq_before = ship.last_write_seq;
        let none = ship
            .collapse_versions(10)
            .await
            .expect("collapse with high threshold");
        assert_eq!(
            none.candidates, 0,
            "threshold above version count is a no-op"
        );
        assert_eq!(none.series_repacked, 0);
        assert_eq!(
            ship.last_write_seq, seq_before,
            "a no-op pack maintenance sweep must not consume a sequence number"
        );

        // Root/Delta state before repacking, so we can prove it is untouched.
        let root_before = crate::content_tree::compute_content_tree(&ship)
            .await
            .expect("content tree before repack")
            .root_tree_hash;
        let delta_version_before = ship.data_persistence().table().version();

        // Pack-only maintenance: the series has 4 live versions/physical
        // objects, exceeding threshold 1, and is repacked into one bounded
        // physical pack (the whole tiny payload fits under the file pack's
        // byte cap).
        let seq_before_repack = ship.last_write_seq;
        let report = ship
            .collapse_versions(1)
            .await
            .expect("pack maintenance must succeed (exit 0), never gate");
        assert_eq!(report.candidates, 1);
        assert_eq!(
            report.series_repacked, 1,
            "the one over-threshold series must be repacked"
        );
        assert_eq!(report.unsupported_legacy, 0);
        assert!(
            report.pack_objects_written > 0,
            "repacking must publish at least one new physical pack object"
        );
        assert_eq!(
            ship.last_write_seq, seq_before_repack,
            "pack maintenance touches only local `_packs/` disk state, never a Delta \
             transaction, so it must not consume a sequence number"
        );

        // Delta/root state is provably untouched: no logical row, manifest,
        // tree, or commit root changed.
        let root_after = crate::content_tree::compute_content_tree(&ship)
            .await
            .expect("content tree after repack")
            .root_tree_hash;
        assert_eq!(
            root_after, root_before,
            "pack-only maintenance must never change the root tree hash"
        );
        assert_eq!(
            ship.data_persistence().table().version(),
            delta_version_before,
            "pack-only maintenance must never advance the Delta version"
        );

        // Content is byte-identical: reads still come from the untouched
        // Oplog rows, not from any pack.
        let meta = PondUserMetadata::new(vec!["read".to_string()]);
        let tx = ship.begin_read(&meta).await.expect("begin read");
        let root = tx.root().await.expect("root");
        let content = root
            .read_file_path_to_vec(file_path)
            .await
            .expect("read content");
        assert_eq!(
            content, cumulative,
            "content is unchanged by pack-only maintenance"
        );
        _ = tx.commit().await.expect("commit read");

        // Idempotent: a second run at the same threshold settles -- the
        // series is now already at its achievable bounded floor and is not
        // repacked again, and no further physical objects are written.
        let again = ship
            .collapse_versions(1)
            .await
            .expect("repeat pack maintenance");
        assert_eq!(
            again.candidates, 1,
            "still a coarse candidate by version count"
        );
        assert_eq!(
            again.series_repacked, 0,
            "an already-bounded series must not be repacked again"
        );
        assert_eq!(
            again.already_bounded, 1,
            "the just-repacked series settles as already bounded"
        );
        assert_eq!(
            again.pack_objects_written, 0,
            "a settled repeat run writes no further physical objects"
        );
    }

    /// The lower-level `collapse_file_series` helper
    /// (`tlogfs::persistence::State::collapse_file_series`) is unsupported
    /// for logical-series-v2 (a merged row cannot carry a single persisted
    /// logical leaf hash for the versions it would absorb -- BLOCKER 3):
    /// called directly it must reject rather than merge, content and version
    /// count must stay untouched, and the series must remain fully usable
    /// afterward (further ordinary appends still work).
    ///
    /// The real, supported way to collapse history for a logical-series-v2
    /// node is `WD::async_writer_path_collapsing_with_type`, used by
    /// content-pull replication to mirror a source pond's collapse: it is
    /// exercised here too, proving a genuine merged-history write still
    /// round-trips byte-identical content through the normal read path.
    #[tokio::test]
    async fn test_collapse_file_series_low_level_is_rejected_and_pull_mirror_collapse_works() {
        use tinyfs::ResultExt;
        use tokio::io::AsyncWriteExt;

        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond_collapse_low_level");
        let mut ship = Ship::create_pond(&pond_path, "test-host")
            .await
            .expect("Failed to create pond");

        let file_path = "/data/events.csv";
        let chunks: [&[u8]; 4] = [b"name,value\n", b"alice,1\n", b"bob,2\n", b"carol,3\n"];
        let mut cumulative: Vec<u8> = Vec::new();

        for (i, chunk) in chunks.iter().enumerate() {
            cumulative.extend_from_slice(chunk);
            let meta = PondUserMetadata::new(vec![format!("write{i}")]);
            let chunk = chunk.to_vec();
            let first = i == 0;
            ship.write_transaction(&meta, async move |fs| {
                let root = fs.root().await?;
                if first {
                    _ = root.create_dir_path("data").await?;
                }
                let mut writer = root
                    .async_writer_path_with_type(file_path, tinyfs::EntryType::FilePhysicalSeries)
                    .await?;
                writer.write_all(&chunk).await.map_other()?;
                writer.shutdown().await.map_other()?;
                Ok(())
            })
            .await
            .expect("series write transaction");
        }

        let file_id = {
            let meta = PondUserMetadata::new(vec!["read-id".to_string()]);
            let tx = ship.begin_read(&meta).await.expect("begin read");
            let candidates = {
                let state = tx.state().expect("state");
                state
                    .list_collapsible_series(1)
                    .await
                    .expect("list collapsible series")
            };
            _ = tx.commit().await.expect("commit read");
            assert_eq!(candidates.len(), 1, "exactly one series should qualify");
            candidates[0]
        };

        let meta = PondUserMetadata::new(vec!["low-level-collapse".to_string()]);
        let tx = ship.begin_write(&meta).await.expect("begin write");
        let err = {
            let state = tx.state().expect("state");
            state
                .collapse_file_series(file_id, 1)
                .await
                .expect_err("collapse_file_series must be unsupported for logical-series-v2")
        };
        assert!(
            matches!(err, tlogfs::TLogFSError::CollapseUnsupported { .. }),
            "expected CollapseUnsupported, got {err:?}"
        );
        // Abort: the rejected call must not have queued any row rewrite.
        drop(tx);

        // Content and version count are exactly as the four appends left them.
        let meta = PondUserMetadata::new(vec!["read".to_string()]);
        let tx = ship.begin_read(&meta).await.expect("begin read");
        let root = tx.root().await.expect("root");
        let content = root
            .read_file_path_to_vec(file_path)
            .await
            .expect("read content");
        assert_eq!(
            content, cumulative,
            "a rejected low-level collapse must not change content"
        );
        _ = tx.commit().await.expect("commit read");

        // The supported replacement: a pull-mirror collapsing write
        // (`async_writer_path_collapsing_with_type`) supersedes every earlier
        // version with one fresh baseline. Verify it round-trips correctly
        // and that the series stays discoverable/appendable afterward.
        let meta = PondUserMetadata::new(vec!["pull-mirror-collapse".to_string()]);
        let cumulative_for_write = cumulative.clone();
        ship.write_transaction(&meta, async move |fs| {
            let root = fs.root().await?;
            let mut writer = root
                .async_writer_path_collapsing_with_type(
                    file_path,
                    tinyfs::EntryType::FilePhysicalSeries,
                )
                .await?;
            writer.write_all(&cumulative_for_write).await.map_other()?;
            writer.shutdown().await.map_other()?;
            Ok(())
        })
        .await
        .expect("pull-mirror collapsing write");

        let meta = PondUserMetadata::new(vec!["read-after-mirror".to_string()]);
        let tx = ship.begin_read(&meta).await.expect("begin read");
        let root = tx.root().await.expect("root");
        let content = root
            .read_file_path_to_vec(file_path)
            .await
            .expect("read pull-mirrored content");
        assert_eq!(
            content, cumulative,
            "pull-mirror collapsing write preserves byte-identical content"
        );
        _ = tx.commit().await.expect("commit read");

        // The merged version now supersedes every prior version, so only one
        // live version remains and the series is no longer a collapse
        // candidate at threshold 1.
        let meta = PondUserMetadata::new(vec!["read-candidates".to_string()]);
        let tx = ship.begin_read(&meta).await.expect("begin read");
        let candidates = {
            let state = tx.state().expect("state");
            state
                .list_collapsible_series(1)
                .await
                .expect("list collapsible series")
        };
        _ = tx.commit().await.expect("commit read");
        assert!(
            candidates.is_empty(),
            "a single merged live version must not itself be a collapse candidate"
        );

        // The series remains normally appendable after a pull-mirror collapse.
        let meta = PondUserMetadata::new(vec!["append-after-mirror".to_string()]);
        ship.write_transaction(&meta, async move |fs| {
            let root = fs.root().await?;
            let mut writer = root
                .async_writer_path_with_type(file_path, tinyfs::EntryType::FilePhysicalSeries)
                .await?;
            writer.write_all(b"dave,4\n").await.map_other()?;
            writer.shutdown().await.map_other()?;
            Ok(())
        })
        .await
        .expect("append after pull-mirror collapse");

        let meta = PondUserMetadata::new(vec!["read-final".to_string()]);
        let tx = ship.begin_read(&meta).await.expect("begin read");
        let root = tx.root().await.expect("root");
        let content = root
            .read_file_path_to_vec(file_path)
            .await
            .expect("read final content");
        let mut expected = cumulative.clone();
        expected.extend_from_slice(b"dave,4\n");
        assert_eq!(
            content, expected,
            "an ordinary append after a pull-mirror collapse is fully live"
        );
        _ = tx.commit().await.expect("commit read");
    }

    #[tokio::test]
    async fn test_collapse_versions_multiple_files() {
        use tinyfs::ResultExt;
        use tokio::io::AsyncWriteExt;

        async fn write_version(
            ship: &mut Ship,
            path: &'static str,
            chunk: &'static [u8],
            make_dir: bool,
        ) {
            let meta = PondUserMetadata::new(vec!["write".to_string()]);
            ship.write_transaction(&meta, async move |fs| {
                let root = fs.root().await?;
                if make_dir {
                    _ = root.create_dir_path("data").await?;
                }
                let mut writer = root
                    .async_writer_path_with_type(path, tinyfs::EntryType::FilePhysicalSeries)
                    .await?;
                writer.write_all(chunk).await.map_other()?;
                writer.shutdown().await.map_other()?;
                Ok(())
            })
            .await
            .expect("series write");
        }

        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond_multi_collapse");
        let mut ship = Ship::create_pond(&pond_path, "test-host")
            .await
            .expect("Failed to create pond");

        let noisy = "/data/noisy.csv";
        let quiet = "/data/quiet.csv";

        // noisy: three versions; quiet: a single version.
        let noisy_chunks: [&[u8]; 3] = [b"n,1\n", b"n,2\n", b"n,3\n"];
        let mut noisy_full: Vec<u8> = Vec::new();
        for (i, chunk) in noisy_chunks.iter().enumerate() {
            noisy_full.extend_from_slice(chunk);
            write_version(&mut ship, noisy, chunk, i == 0).await;
        }
        write_version(&mut ship, quiet, b"q,1\n", false).await;

        // Pack maintenance repacks only the over-threshold series (noisy);
        // quiet has a single version and is never even a coarse candidate.
        let report = ship
            .collapse_versions(1)
            .await
            .expect("pack maintenance must succeed");
        assert_eq!(report.candidates, 1);
        assert_eq!(report.series_repacked, 1);

        // Both files' content is untouched: pack maintenance never rewrites
        // Oplog rows.
        let meta = PondUserMetadata::new(vec!["read".to_string()]);
        let tx = ship.begin_read(&meta).await.expect("begin read");
        let root = tx.root().await.expect("root");
        let noisy_content = root.read_file_path_to_vec(noisy).await.expect("read noisy");
        assert_eq!(
            noisy_content, noisy_full,
            "noisy is unchanged by pack-only maintenance"
        );
        let quiet_content = root.read_file_path_to_vec(quiet).await.expect("read quiet");
        assert_eq!(quiet_content, b"q,1\n", "quiet file is unchanged");
        _ = tx.commit().await.expect("commit read");

        // A repeated run settles: noisy is now already bounded and is not
        // repacked again.
        let again = ship
            .collapse_versions(1)
            .await
            .expect("repeated pack maintenance must settle");
        assert_eq!(again.candidates, 1);
        assert_eq!(
            again.series_repacked, 0,
            "a repeated run must not repack an already-bounded series"
        );
        assert_eq!(again.already_bounded, 1);
    }

    fn count_log_files(log_dir: &Path, ext: &str) -> usize {
        std::fs::read_dir(log_dir)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some(ext))
                    .count()
            })
            .unwrap_or(0)
    }

    /// The control table is never replicated, so its `_delta_log` must be
    /// cleaned aggressively rather than kept for the 30-day Delta default.
    /// Many small write transactions accumulate commit JSONs; a forced maintain
    /// with a short retention must collapse them to the latest checkpoint while
    /// leaving the pond fully readable.
    #[tokio::test]
    async fn test_control_log_cleanup_bounds_delta_log() {
        let temp_dir = tempdir().expect("temp dir");
        let pond_path = temp_dir.path().join("test_pond");
        let mut ship = Ship::create_pond(&pond_path, "test-host")
            .await
            .expect("create pond");

        // Drive enough write transactions to accumulate control commit JSONs
        // well past a checkpoint boundary.
        for i in 0..16 {
            let meta = PondUserMetadata::new(vec![format!("commit-{i}")]);
            ship.write_transaction(&meta, async move |fs| {
                let root = fs.root().await?;
                _ = tinyfs::async_helpers::convenience::create_file_path(
                    &root,
                    &format!("/file-{i}.txt"),
                    format!("content-{i}").as_bytes(),
                )
                .await?;
                Ok(())
            })
            .await
            .expect("write transaction");
        }

        let control_log_dir = get_control_path(&pond_path).join("_delta_log");
        let json_before = count_log_files(&control_log_dir, "json");
        assert!(
            json_before > 10,
            "expected many control commit JSONs, got {json_before}"
        );

        // Let existing log files age past the tiny retention used below so the
        // checkpoint-aligned cleanup considers them expired.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let control_table = ship.control_table.table().clone();
        let pre_version = control_table.version().unwrap_or(-1);
        let (cleaned_table, result) = maintenance::maintain_table(
            control_table,
            "control",
            true,
            false,
            Some(chrono::Duration::milliseconds(10)),
        )
        .await
        .expect("test control maintenance");

        assert!(result.checkpoint_created, "forced maintain must checkpoint");
        assert!(
            result.logs_cleaned > 0,
            "expected expired control logs to be cleaned"
        );
        assert_eq!(
            cleaned_table.version().unwrap_or(-1),
            pre_version,
            "cleanup must not change the table version"
        );

        let json_after = count_log_files(&control_log_dir, "json");
        assert!(
            json_after < json_before,
            "control commit JSONs should shrink: before={json_before} after={json_after}"
        );

        // The pond must still open and replay cleanly after log cleanup.
        drop(ship);
        let reopened = Ship::open_pond(&pond_path).await.expect("reopen pond");
        drop(reopened);
    }
}
