// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Shared logic for appending new bytes from a host-filesystem file onto an
//! existing `tinyfs::EntryType::FilePhysicalSeries` pond path.
//!
//! Two factories need exactly the same three steps: (1) read the pond's
//! committed cumulative state for a series path, (2) verify a host file's
//! tracked prefix still matches that state -- using the stored bao-tree
//! frontier so this costs at most one `BLOCK_SIZE` read, not a full
//! re-hash -- and (3) commit only the new suffix as the next version.
//! `logfile_ingest` (tailing an actively-written log file) and `exec-factory`
//! (committing a sandboxed program's additions to a staged file) both do
//! this, so this module is the one place the logic lives instead of each
//! factory re-deriving it independently.

use crate::FactoryContext;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use tinyfs::ResultExt;
use tinyfs::{EntryType, NodeMetadata};
use utilities::bao_outboard::{BLOCK_SIZE, IncrementalHashState, SeriesOutboard};

/// The pond's committed state for a `FilePhysicalSeries` path: the
/// cumulative BLAKE3 hash, the total bytes already committed across all
/// versions (`cumulative_size` -- not just the latest version's size), and,
/// when available, the bao-tree frontier that lets [`verify_prefix_matches`]
/// resume hashing from the last committed block instead of re-reading the
/// whole prefix.
#[derive(Debug, Clone)]
pub struct PondSeriesState {
    pub blake3: String,
    pub cumulative_size: u64,
    pub frontier: Option<Vec<(u32, [u8; 32], u64)>>,
}

impl PondSeriesState {
    /// Derive a [`PondSeriesState`] from tinyfs node metadata. `label` is
    /// only used to name the path in error messages.
    pub fn from_metadata(metadata: &NodeMetadata, label: &str) -> Result<Self, tinyfs::Error> {
        let blake3 = metadata.blake3.clone().ok_or_else(|| {
            tinyfs::Error::Other(format!("pond file {label} missing required blake3 hash"))
        })?;

        // metadata.size is just the latest version's size, not cumulative;
        // the bao_outboard's SeriesOutboard carries the real cumulative_size
        // plus the frontier needed for cheap incremental verification.
        let (cumulative_size, frontier) = if let Some(bao_outboard) = &metadata.bao_outboard {
            match SeriesOutboard::from_bytes(bao_outboard) {
                Ok(series) => (series.cumulative_size, Some(series.incremental.frontier)),
                Err(_) => (metadata.size.unwrap_or(0), None),
            }
        } else {
            (metadata.size.unwrap_or(0), None)
        };

        Ok(Self {
            blake3,
            cumulative_size,
            frontier,
        })
    }
}

/// Load the committed [`PondSeriesState`] for `pond_path`, or `None` if the
/// path doesn't exist yet (the next write will be the series' first
/// version).
pub async fn load_series_state(
    context: &FactoryContext,
    root: &tinyfs::WD,
    pond_path: &str,
) -> Result<Option<PondSeriesState>, tinyfs::Error> {
    if !root.exists(pond_path).await {
        return Ok(None);
    }
    // `resolve_path` returns (parent WD, Lookup) -- the target node's ID is
    // inside the Lookup, not the returned WD (which is the *containing*
    // directory). Reading `wd.node_path().id()` here would silently use the
    // parent directory's ID instead of the file's.
    let (_wd, lookup) = root.resolve_path(pond_path).await?;
    let file_id = match lookup {
        tinyfs::Lookup::Found(node_path) => node_path.id(),
        tinyfs::Lookup::Empty(node_path) => node_path.id(),
        tinyfs::Lookup::NotFound(_, _) => {
            // root.exists() said true above, so this would be a race; treat
            // it the same as "doesn't exist yet".
            return Ok(None);
        }
    };
    let metadata = context.context.persistence.metadata(file_id).await?;
    Ok(Some(PondSeriesState::from_metadata(&metadata, pond_path)?))
}

/// Verify that `host_path`'s tracked prefix (its first `state.cumulative_size`
/// bytes) still matches the pond's committed cumulative hash. When a
/// frontier is stored, this reads only the trailing partial block instead
/// of the whole prefix; otherwise it falls back to hashing the whole
/// prefix (legacy entries without a bao_outboard). This is a precondition
/// fallback, not a verification fallback: a genuine hash mismatch is still
/// surfaced as `matches == false`.
///
/// Returns `(matches, host_root_hex)`; the host root hash is returned so
/// callers can include it in diagnostics.
pub fn verify_prefix_matches(
    host_path: &Path,
    state: &PondSeriesState,
) -> Result<(bool, String), tinyfs::Error> {
    let cumulative_size = state.cumulative_size;
    if cumulative_size == 0 {
        // Nothing tracked yet; any host content is a fresh prefix.
        return Ok((true, String::new()));
    }

    let host_root = match &state.frontier {
        Some(frontier) => {
            let block = BLOCK_SIZE as u64;
            let pending_start = (cumulative_size / block) * block;
            let pending_len = (cumulative_size % block) as usize;

            let mut file = std::fs::File::open(host_path).map_other()?;
            let _ = file.seek(SeekFrom::Start(pending_start)).map_other()?;
            let mut verified_pending = vec![0u8; pending_len];
            file.read_exact(&mut verified_pending).map_other()?;

            let resumed =
                IncrementalHashState::resume(frontier, cumulative_size, &verified_pending)
                    .map_other()?;
            resumed.root_hash().to_hex().to_string()
        }
        None => {
            let mut file = std::fs::File::open(host_path).map_other()?;
            let mut prefix_content = vec![0u8; cumulative_size as usize];
            file.read_exact(&mut prefix_content).map_other()?;
            let mut hasher = IncrementalHashState::new();
            hasher.ingest(&prefix_content);
            hasher.root_hash().to_hex().to_string()
        }
    };

    let matches = host_root == state.blake3;
    Ok((matches, host_root))
}

/// Read exactly `host_path`'s bytes from `prior_cumulative_size` to end of
/// file. A TOCTOU-safe exact read (never `read_to_end`): if the file is
/// smaller than `prior_cumulative_size` this is a clear "file shrank" error
/// instead of a subtraction underflow.
pub fn read_new_suffix(
    host_path: &Path,
    prior_cumulative_size: u64,
) -> Result<Vec<u8>, tinyfs::Error> {
    let file_len = std::fs::metadata(host_path).map_other()?.len();
    let new_len = file_len.checked_sub(prior_cumulative_size).ok_or_else(|| {
        tinyfs::Error::Other(format!(
            "{} shrank from {prior_cumulative_size} to {file_len} bytes",
            host_path.display()
        ))
    })?;

    let mut file = std::fs::File::open(host_path).map_other()?;
    let _ = file
        .seek(SeekFrom::Start(prior_cumulative_size))
        .map_other()?;
    let mut new_content = vec![0u8; new_len as usize];
    file.read_exact(&mut new_content).map_other()?;
    Ok(new_content)
}

/// Commit `suffix` as the next `FilePhysicalSeries` version at `pond_path`,
/// creating parent directories as needed. Writes only `suffix` -- never the
/// whole file -- since series versions are concatenated on read and the
/// prior versions are already committed. A no-op if `suffix` is empty.
pub async fn commit_series_append(
    root: &tinyfs::WD,
    pond_path: &str,
    suffix: &[u8],
) -> Result<(), tinyfs::Error> {
    if suffix.is_empty() {
        return Ok(());
    }
    use tokio::io::AsyncWriteExt;

    if let Some((parent, _)) = pond_path.rsplit_once('/')
        && !parent.is_empty()
    {
        let _ = root.create_dir_all(parent).await?;
    }

    let mut writer = root
        .async_writer_path_with_type(pond_path, EntryType::FilePhysicalSeries)
        .await?;
    writer.write_all(suffix).await.map_other()?;
    writer.shutdown().await.map_other()?;
    Ok(())
}
