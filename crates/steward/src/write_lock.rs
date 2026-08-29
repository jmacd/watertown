// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Process-level write exclusion for a single pond.
//!
//! `WriteLockGuard` wraps an OS advisory file lock (via [`fs2::FileExt`])
//! on `{control_dir}/write.lock`.  At most one process can hold the
//! exclusive lock at a time; concurrent attempts return
//! [`StewardError::PondLocked`] with the holder's PID, start time, and
//! txn_id parsed from the lockfile body.
//!
//! The lock is released automatically when the file descriptor closes,
//! which happens on normal `Drop`, panic unwind, and process death
//! (including `kill -9`).  We never explicitly unlock; the lockfile
//! itself persists with stale contents and is truncated/rewritten by
//! the next acquirer.
//!
//! Reads are intentionally not locked.  See `docs/d5.7-resume.md` for
//! the design discussion.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use tlogfs::PondTxnMetadata;

use crate::StewardError;

const WRITE_FREEZE_FILE: &str = "write.freeze";
const WRITE_FREEZE_FORMAT: &str = "watertown.write-freeze.v1";

/// Durable declaration that a pond must reject all data writes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteFreeze {
    /// Marker format identifier.
    pub format: String,
    /// Pond identity at the time of the freeze.
    pub pond_id: String,
    /// Exact content tip protected by the freeze.
    pub source_tip: Option<String>,
    /// UTC time at which the marker was created.
    pub frozen_at: DateTime<Utc>,
    /// Process that created the marker.
    pub frozen_by_pid: u32,
    /// Operator-supplied reason for the freeze.
    pub reason: String,
}

impl WriteFreeze {
    pub(crate) fn new(pond_id: String, source_tip: Option<String>, reason: String) -> Self {
        Self {
            format: WRITE_FREEZE_FORMAT.to_string(),
            pond_id,
            source_tip,
            frozen_at: Utc::now(),
            frozen_by_pid: std::process::id(),
            reason,
        }
    }
}

/// RAII handle that holds an exclusive advisory lock on `write.lock`
/// for the lifetime of a write transaction.  Dropping the guard
/// closes the underlying file descriptor, which releases the lock.
#[derive(Debug)]
pub(crate) struct WriteLockGuard {
    // Held until drop to keep the FD (and thus the kernel lock) open.
    #[allow(dead_code)]
    file: File,
    path: PathBuf,
}

impl WriteLockGuard {
    /// Acquire the write lock and reject a persisted write freeze.
    pub(crate) fn try_acquire_for_write(
        control_dir: &Path,
        txn_meta: &PondTxnMetadata,
    ) -> Result<Self, StewardError> {
        let guard = Self::try_acquire(control_dir, txn_meta)?;
        if let Some(freeze) = read_write_freeze(control_dir)? {
            return Err(StewardError::PondWriteFrozen {
                path: write_freeze_path(control_dir),
                details: format!(
                    "frozen_at={}, source_tip={}, reason={}",
                    freeze.frozen_at.to_rfc3339(),
                    freeze.source_tip.as_deref().unwrap_or("<none>"),
                    freeze.reason
                ),
            });
        }
        Ok(guard)
    }

    /// Attempt to acquire the write lock for `control_dir`.
    ///
    /// On success, the lockfile body is replaced with the current
    /// holder's PID, start timestamp, and `txn_id`.
    ///
    /// On conflict, returns [`StewardError::PondLocked`] populated
    /// with whatever holder info could be parsed from the existing
    /// lockfile body (fields are `None` if the file is empty, missing,
    /// or malformed — the lock is still rejected either way).
    pub(crate) fn try_acquire(
        control_dir: &Path,
        txn_meta: &PondTxnMetadata,
    ) -> Result<Self, StewardError> {
        let path = control_dir.join("write.lock");

        // Open without O_TRUNC: if another process holds the lock we
        // must not stomp their body before we discover the conflict.
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        match FileExt::try_lock_exclusive(&file) {
            Ok(()) => {
                let _pos = file.seek(SeekFrom::Start(0))?;
                file.set_len(0)?;
                file.write_all(format_lock_body(txn_meta).as_bytes())?;
                file.flush()?;
                Ok(Self { file, path })
            }
            Err(err) if is_would_block(&err) => {
                let holder = read_holder_info(&path).unwrap_or_default();
                Err(StewardError::PondLocked {
                    path,
                    holder_pid: holder.pid,
                    holder_since: holder.since,
                    holder_txn_id: holder.txn_id,
                })
            }
            Err(err) => Err(StewardError::Io(err)),
        }
    }

    /// Path to the lockfile (useful for user-facing error messages).
    #[cfg(test)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

pub(crate) fn read_write_freeze(control_dir: &Path) -> Result<Option<WriteFreeze>, StewardError> {
    let path = write_freeze_path(control_dir);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(StewardError::Io(error)),
    };
    let freeze: WriteFreeze =
        serde_json::from_slice(&bytes).map_err(|error| StewardError::InvalidWriteFreeze {
            path: path.clone(),
            reason: error.to_string(),
        })?;
    if freeze.format != WRITE_FREEZE_FORMAT {
        return Err(StewardError::InvalidWriteFreeze {
            path,
            reason: format!("unsupported format `{}`", freeze.format),
        });
    }
    Ok(Some(freeze))
}

pub(crate) fn create_write_freeze(
    control_dir: &Path,
    freeze: &WriteFreeze,
) -> Result<bool, StewardError> {
    if read_write_freeze(control_dir)?.is_some() {
        return Ok(false);
    }

    let path = write_freeze_path(control_dir);
    let temporary = control_dir.join(format!(".write.freeze.{}.tmp", uuid::Uuid::new_v4()));
    let mut bytes = serde_json::to_vec_pretty(freeze)?;
    bytes.push(b'\n');
    let result = (|| -> Result<(), StewardError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, &path)?;
        sync_directory(control_dir)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result.map(|()| true)
}

pub(crate) fn remove_write_freeze(control_dir: &Path) -> Result<Option<WriteFreeze>, StewardError> {
    let Some(freeze) = read_write_freeze(control_dir)? else {
        return Ok(None);
    };
    std::fs::remove_file(write_freeze_path(control_dir))?;
    sync_directory(control_dir)?;
    Ok(Some(freeze))
}

fn write_freeze_path(control_dir: &Path) -> PathBuf {
    control_dir.join(WRITE_FREEZE_FILE)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), StewardError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<(), StewardError> {
    Ok(())
}

impl Drop for WriteLockGuard {
    fn drop(&mut self) {
        // The kernel releases the lock when `self.file` closes; no
        // explicit unlock_exclusive() needed.  We intentionally leave
        // the lockfile on disk so the next acquirer can discover the
        // last holder's identity via its (now stale) body before
        // overwriting it.  Trying to delete here would race with a
        // concurrent acquirer creating it fresh.
        let _ = &self.path; // suppress unused-field warning
    }
}

/// `WouldBlock` is the kind returned by `fs2` when another process
/// holds the lock.  On some platforms the precise OS error code is
/// `EWOULDBLOCK`/`EAGAIN` which both map to `ErrorKind::WouldBlock`.
fn is_would_block(err: &std::io::Error) -> bool {
    err.kind() == std::io::ErrorKind::WouldBlock
}

#[derive(Debug, Default, Clone)]
struct HolderInfo {
    pid: Option<u32>,
    since: Option<DateTime<Utc>>,
    txn_id: Option<String>,
}

fn read_holder_info(path: &Path) -> std::io::Result<HolderInfo> {
    let mut s = String::new();
    let _read = File::open(path)?.read_to_string(&mut s)?;
    let mut info = HolderInfo::default();
    for line in s.lines() {
        if let Some((k, v)) = line.split_once('=') {
            match k.trim() {
                "pid" => info.pid = v.trim().parse().ok(),
                "started" => {
                    info.since = DateTime::parse_from_rfc3339(v.trim())
                        .ok()
                        .map(|d| d.with_timezone(&Utc));
                }
                "txn_id" => info.txn_id = Some(v.trim().to_string()),
                _ => {}
            }
        }
    }
    Ok(info)
}

fn format_lock_body(txn_meta: &PondTxnMetadata) -> String {
    format!(
        "pid={}\nstarted={}\ntxn_id={}\n",
        std::process::id(),
        Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        txn_meta.user.txn_id,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tlogfs::PondUserMetadata;

    fn meta() -> PondTxnMetadata {
        PondTxnMetadata::new(1, PondUserMetadata::new(vec!["test".to_string()]))
    }

    #[test]
    fn acquire_succeeds_when_uncontended() {
        let dir = TempDir::new().expect("tempdir");
        let g =
            WriteLockGuard::try_acquire(dir.path(), &meta()).expect("first acquire should succeed");
        assert!(g.path().ends_with("write.lock"));
        assert!(g.path().exists());
    }

    #[test]
    fn acquire_writes_holder_body() {
        let dir = TempDir::new().expect("tempdir");
        let m = meta();
        let _g = WriteLockGuard::try_acquire(dir.path(), &m).expect("acquire");
        let body = std::fs::read_to_string(dir.path().join("write.lock")).expect("read lockfile");
        assert!(body.contains(&format!("pid={}", std::process::id())));
        assert!(body.contains(&format!("txn_id={}", m.user.txn_id)));
        assert!(body.contains("started="));
    }

    #[test]
    fn second_acquire_in_same_process_fails_with_pondlocked() {
        let dir = TempDir::new().expect("tempdir");
        let _g1 = WriteLockGuard::try_acquire(dir.path(), &meta()).expect("first");
        let err = WriteLockGuard::try_acquire(dir.path(), &meta()).expect_err("second should fail");
        match err {
            StewardError::PondLocked {
                holder_pid,
                holder_txn_id,
                ..
            } => {
                assert_eq!(holder_pid, Some(std::process::id()));
                assert!(holder_txn_id.is_some());
            }
            other => panic!("expected PondLocked, got {other:?}"),
        }
    }

    #[test]
    fn acquire_succeeds_after_previous_guard_dropped() {
        let dir = TempDir::new().expect("tempdir");
        {
            let _g1 = WriteLockGuard::try_acquire(dir.path(), &meta()).expect("first");
            // drops at end of block
        }
        let _g2 = WriteLockGuard::try_acquire(dir.path(), &meta())
            .expect("re-acquire after drop should succeed");
    }

    #[test]
    fn write_freeze_round_trips_and_blocks_write_acquisition() {
        let dir = TempDir::new().expect("tempdir");
        let freeze = WriteFreeze::new(
            "pond-id".to_string(),
            Some("tip".to_string()),
            "format migration".to_string(),
        );
        {
            let _guard = WriteLockGuard::try_acquire(dir.path(), &meta()).expect("admin lock");
            assert!(create_write_freeze(dir.path(), &freeze).expect("create freeze"));
            assert!(!create_write_freeze(dir.path(), &freeze).expect("idempotent freeze"));
        }

        assert_eq!(
            read_write_freeze(dir.path()).expect("read freeze"),
            Some(freeze.clone())
        );
        assert!(matches!(
            WriteLockGuard::try_acquire_for_write(dir.path(), &meta()),
            Err(StewardError::PondWriteFrozen { .. })
        ));

        {
            let _guard = WriteLockGuard::try_acquire(dir.path(), &meta()).expect("admin lock");
            assert_eq!(
                remove_write_freeze(dir.path()).expect("remove freeze"),
                Some(freeze)
            );
            assert!(
                remove_write_freeze(dir.path())
                    .expect("idempotent remove")
                    .is_none()
            );
        }
        let _guard = WriteLockGuard::try_acquire_for_write(dir.path(), &meta())
            .expect("write after unfreeze");
    }
}
