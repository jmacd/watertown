// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Production, pack-only physical maintenance for `pond maintain
//! --collapse-versions N` (`docs/logical-series-identity-design.md`).
//!
//! This module never rewrites or deletes an Oplog append row, never
//! changes a `dp.series.2` manifest/tree/commit root, Delta version, or txn
//! sequence, and never changes logical metadata. What it *does* do is
//! discover native v2 series whose current physical representation is
//! fragmented past a requested threshold and publish a smaller, bounded set
//! of content-addressed physical pack objects (plus one [`PackIndex`]
//! advertisement) to the local pond's own `data/_packs` namespace
//! ([`crate::pack_store`]), so a remote or `pond://` reader can select the
//! bounded layout instead of the original one-object-per-append stream --
//! exactly the same acceptance path gate 7's initial pack publication
//! already uses (see [`crate::content_tree::build_initial_pack_index`]),
//! just built from a repack instead of a 1:1 mapping of existing objects.
//!
//! # Why this is safe
//!
//! A pack advertisement is purely additive: it is one more way to *read* a
//! series' already-committed logical content, never a replacement for the
//! Oplog rows themselves. [`crate::content_tree::build_series_manifest`]
//! (the exact same fold every push/verify path uses) is called on the
//! series' live rows to get the untouched, canonical `dp.series.2`
//! manifest; every leaf hash a fresh pack claims is recomputed from the
//! real, live-fetched content and checked against that persisted leaf hash
//! before it is trusted (requirement 3's "recompute/verify"); and the
//! assembled [`PackIndex`] is self-verified against that same manifest
//! ([`sync_store::content::verify_pack_against_manifest`]) before it is
//! ever published. If any of that disagrees, this module fails loudly
//! rather than publish a pack that could mislead a reader.
//!
//! # Streaming, not buffering
//!
//! Existing gate-5 builders ([`sync_store::content::build_file_pack`]/
//! [`sync_store::content::build_table_pack`]) are prototypes: they accept
//! already-fully-materialized leaf inputs and return every physical
//! object's bytes in one `Vec`. This module does not call them. Instead:
//! - [`repack_file_series`] streams each leaf's bytes in fixed-size chunks
//!   (inline bytes fed directly; externalized bytes read via
//!   [`tlogfs::OpLogPersistence::open_large_file_reader_by_hash`]'s
//!   streaming reader) through both an incremental leaf hasher
//!   ([`sync_store::content::IncrementalFileLeafHasher`]) and a
//!   bounded byte accumulator that flushes and durably writes one physical
//!   object the moment it reaches its cap -- never holding more than one
//!   target object's bytes at a time, and never the whole series.
//! - [`repack_table_series`] holds at most one already-decoded leaf's rows
//!   plus one bounded, not-yet-flushed target object's row slices at a
//!   time (via zero-copy [`arrow_array::RecordBatch::slice`]), exactly the
//!   allowance the task spec makes for the table path ("at most one
//!   bounded output object/batch set, never whole series").
//!
//! Physical object boundaries are independent of logical leaf boundaries
//! in both paths: a leaf may span several physical objects, or several
//! leaves may share one, with leaf boundaries preserved only in each
//! pack's [`sync_store::content::PackLeafDescriptor`]s -- exactly as
//! `docs/logical-series-identity-design.md`'s pack design already
//! specifies and as existing dual readers
//! ([`crate::content_pull::fetch_and_verify_file_pack`]) already assume.
//!
//! # Publication order and crash safety
//!
//! Every physical object this module writes is content-addressed and
//! published via [`crate::pack_store::write_pack_object`] *before* the
//! [`PackIndex`] naming it is published via
//! [`crate::pack_store::publish_pack_index`] -- so a crash mid-repack
//! leaves only orphaned (harmless, unreferenced) physical objects and no
//! advertisement naming a missing one. Re-running is deterministic: the
//! same live rows always recompute the same leaf hashes, the same bounded
//! layout, and therefore the same content-addressed objects and the same
//! pack index bytes, so a repeat run's writes are idempotent no-ops (see
//! [`crate::pack_store::write_pack_object`]/[`crate::pack_store::publish_pack_index`]'s
//! own content-addressed idempotence) and discovery settles: a series
//! already repacked to its achievable bounded floor is never re-flagged
//! (see [`discover_candidates`]'s candidacy test).

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use tinyfs::{EntryType, FileID};
use tokio::io::AsyncReadExt;

use sync_store::content::{ObjectHash, PackIndex, PackLeafDescriptor, SeriesManifest};

use crate::{StewardError, content_pull, content_tree, pack_store};

/// Maximum bytes any one File-payload physical pack object may hold.
/// Physical object boundaries are independent of leaf boundaries: a leaf
/// may span several objects, or several leaves may share one object (see
/// the module docs).
const FILE_PACK_MAX_BYTES_PER_OBJECT: u64 = 4 * 1024 * 1024;

/// Maximum rows any one Table-payload physical pack object may hold.
const TABLE_PACK_MAX_ROWS_PER_OBJECT: u64 = 100_000;

/// Soft byte safeguard for a Table-payload physical pack object: a pending
/// object is flushed early, short of the row cap, once its accumulated
/// batches' estimated in-memory size reaches this -- so a table with very
/// wide or large rows still gets bounded physical objects rather than one
/// enormous 100_000-row object.
const TABLE_PACK_MAX_BYTES_PER_OBJECT: u64 = 8 * 1024 * 1024;

/// Version of the deterministic table repack layout these constants (row
/// cap, byte safeguard, and the packing algorithm itself) describe.
/// Recorded in a [`pack_store::TableLayoutMarker`] sidecar alongside a
/// freshly published table pack so a later discovery pass can recognize
/// "this exact pack was already produced by the current deterministic
/// maintenance layout" without re-decoding it, rather than re-deriving an
/// inherently approximate `ceil(rows / cap)` estimate that the byte
/// safeguard can make inaccurate (see [`discover_candidates`]). Bump this
/// whenever the row cap, byte safeguard, or packing algorithm changes, so a
/// stale marker is never mistaken for a match against new constants.
const TABLE_PACK_LAYOUT_VERSION: u32 = 1;

/// One physical read/write chunk size for streaming an externalized file
/// leaf's bytes -- bounded, arbitrary, and unrelated to any pack layout
/// cap.
const FILE_STREAM_CHUNK_BYTES: usize = 256 * 1024;

/// Why one native v2 series was, or was not, repacked this run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackCandidateOutcome {
    /// Its current physical fanout exceeds the requested threshold and a
    /// smaller bounded layout is achievable; a real (non-dry-run) call
    /// repacks it.
    NeedsRepack,
    /// Already at its achievable bounded floor (or never exceeded the
    /// threshold) -- an idempotent no-op, reported so a dry run can show
    /// settling.
    AlreadyBounded,
    /// A pre-v2 series: no persisted logical-leaf identity on any row.
    /// Pack-only maintenance does not cover a series that never received
    /// v2 leaf stamping. Not an error -- it simply is not the kind of
    /// physical layout this maintenance operates on.
    UnsupportedLegacy,
    /// Actually repacked and published a new, more-bounded pack this run.
    Repacked,
}

/// One native v2 series pack maintenance discovered, with enough detail to
/// report a dry run and, if [`PackCandidateOutcome::NeedsRepack`], to
/// actually repack it.
#[derive(Debug, Clone)]
pub struct PackMaintenanceCandidate {
    /// The series node's identity.
    pub file_id: FileID,
    /// The series' `dp.series.2` manifest hash, or `None` for
    /// [`PackCandidateOutcome::UnsupportedLegacy`] (a pre-v2 series has no
    /// v2 manifest to hash).
    pub series_hash: Option<ObjectHash>,
    /// Whether this is a `FilePhysicalSeries` or `TablePhysicalSeries`.
    pub entry_type: EntryType,
    /// Number of logical leaves (0 for [`PackCandidateOutcome::UnsupportedLegacy`]).
    pub leaf_count: u64,
    /// This series' logical payload size: bytes for a `FilePhysicalSeries`,
    /// rows for a `TablePhysicalSeries` (0 for
    /// [`PackCandidateOutcome::UnsupportedLegacy`]).
    pub logical_count: u64,
    /// How many physical objects the currently-selected pack cover (or, in
    /// its absence, the original per-append stream) requires today.
    pub current_physical_objects: usize,
    /// How many physical objects a bounded repack would produce.
    pub proposed_physical_objects: usize,
    /// What happened, or would happen, to this series.
    pub outcome: PackCandidateOutcome,
}

impl std::fmt::Display for PackMaintenanceCandidate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let unit = match self.entry_type {
            EntryType::TablePhysicalSeries => "row(s)",
            _ => "byte(s)",
        };
        write!(
            f,
            "node {} ({:?}): {} leaf/leaves, {} logical {unit}, {} physical object(s)",
            self.file_id.node_id(),
            self.entry_type,
            self.leaf_count,
            self.logical_count,
            self.current_physical_objects,
        )?;
        match self.outcome {
            PackCandidateOutcome::NeedsRepack => write!(
                f,
                " -> {} proposed (needs repack)",
                self.proposed_physical_objects
            ),
            PackCandidateOutcome::Repacked => {
                write!(f, " -> {} (repacked)", self.proposed_physical_objects)
            }
            PackCandidateOutcome::AlreadyBounded => write!(f, " (already bounded)"),
            PackCandidateOutcome::UnsupportedLegacy => {
                write!(f, " (pre-v2, unsupported by pack maintenance)")
            }
        }
    }
}

/// What one [`run_pack_maintenance`] call actually did.
#[derive(Debug, Clone, Default)]
pub(crate) struct PackMaintenanceReport {
    pub(crate) candidates: Vec<PackMaintenanceCandidate>,
    pub(crate) series_repacked: usize,
    pub(crate) already_bounded: usize,
    pub(crate) unsupported_legacy: usize,
    pub(crate) pack_objects_written: usize,
    pub(crate) pack_bytes_written: u64,
    pub(crate) pack_objects_removed: usize,
    pub(crate) pack_bytes_freed: u64,
    /// Obsolete `series=<oldhash>` directories deleted this run because
    /// their series hash was not among any currently-live series' manifest
    /// (requirement 2), and had already survived one full maintenance
    /// generation marked stale (finding 4's concurrent-reader grace
    /// period, [`pack_store::prune_obsolete_series_dirs`]).
    pub(crate) series_dirs_removed: usize,
    /// Obsolete `series=<oldhash>` directories newly marked stale this run
    /// (first generation as no-longer-live; not yet deleted).
    pub(crate) series_dirs_marked_stale: usize,
    /// Superseded pack advertisements deleted this run because they had
    /// already survived one full maintenance generation marked stale
    /// (finding 4, [`pack_store::retain_selected_pack_only`]).
    pub(crate) pack_advertisements_removed: usize,
    /// Superseded pack advertisements newly marked stale this run (first
    /// generation as non-selected; not yet deleted).
    pub(crate) pack_advertisements_marked_stale: usize,
    /// Orphan layout marker sidecars cleaned up this run (finding 5).
    pub(crate) orphan_markers_removed: usize,
}

/// One native v2 series' live state, already folded into its manifest --
/// enough to decide candidacy and, if selected, to repack (a real repack
/// fetches this one series' full content-bearing rows fresh, right before
/// streaming it -- see [`run_pack_maintenance`] -- rather than this struct
/// ever holding a series' payload bytes; see the module docs' "Streaming,
/// not buffering" section and requirement 4).
struct DiscoveredSeries {
    file_id: FileID,
    series_hash: ObjectHash,
    entry_type: EntryType,
    manifest: SeriesManifest,
    current_physical_objects: usize,
    proposed_physical_objects: usize,
    /// The existing full-range local pack advertisement's own hash, if any
    /// was already found on disk during discovery (see
    /// [`current_pack_fanout`]) -- the one worth retaining for a
    /// [`PackCandidateOutcome::AlreadyBounded`] series that never gets a
    /// fresh repack this run (requirement 2).
    existing_pack_hash: Option<ObjectHash>,
}

/// Discovery's per-series verdict, before any mutation.
enum Discovery {
    /// No persisted v2 leaf identity on any row: pack-only maintenance does
    /// not apply.
    Legacy(FileID),
    /// A native v2 series already at its achievable bounded floor.
    Bounded(DiscoveredSeries),
    /// A native v2 series whose current physical fanout exceeds the
    /// threshold and can be reduced.
    NeedsRepack(DiscoveredSeries),
}

fn layout_cap(entry_type: EntryType) -> u64 {
    match entry_type {
        EntryType::TablePhysicalSeries => TABLE_PACK_MAX_ROWS_PER_OBJECT,
        _ => FILE_PACK_MAX_BYTES_PER_OBJECT,
    }
}

/// `ceil(n / d)`, saturating into `usize`; `0` when `n` is `0`.
fn ceil_div(n: u64, d: u64) -> usize {
    if n == 0 || d == 0 {
        return 0;
    }
    let q = n / d;
    let r = n % d;
    let total = if r == 0 { q } else { q + 1 };
    usize::try_from(total).unwrap_or(usize::MAX)
}

fn candidate_from_series(
    series: &DiscoveredSeries,
    outcome: PackCandidateOutcome,
) -> PackMaintenanceCandidate {
    PackMaintenanceCandidate {
        file_id: series.file_id,
        series_hash: Some(series.series_hash),
        entry_type: series.entry_type,
        leaf_count: series.manifest.leaf_count(),
        logical_count: series.manifest.logical_count(),
        current_physical_objects: series.current_physical_objects,
        proposed_physical_objects: series.proposed_physical_objects,
        outcome,
    }
}

fn candidate_from_discovery(discovery: &Discovery) -> PackMaintenanceCandidate {
    match discovery {
        Discovery::Legacy(file_id) => PackMaintenanceCandidate {
            file_id: *file_id,
            series_hash: None,
            entry_type: file_id.entry_type(),
            leaf_count: 0,
            logical_count: 0,
            current_physical_objects: 0,
            proposed_physical_objects: 0,
            outcome: PackCandidateOutcome::UnsupportedLegacy,
        },
        Discovery::Bounded(series) => {
            candidate_from_series(series, PackCandidateOutcome::AlreadyBounded)
        }
        Discovery::NeedsRepack(series) => {
            candidate_from_series(series, PackCandidateOutcome::NeedsRepack)
        }
    }
}

/// The current best (fewest-object) full-range `[0, leaf_count)` pack cover
/// already advertised on local disk for `series_hash` (along with that
/// pack's own hash, so a caller can check its layout marker), or
/// `manifest.leaf_count()` with no hash (the original unbounded
/// one-object-per-leaf layout) if none is found.
///
/// Only full-range packs are considered: a partial-range pack cannot stand
/// in for "the series' current physical representation" on its own.
///
/// # Errors
///
/// Propagates [`pack_store::list_local_pack_hashes`]'s and
/// [`pack_store::read_and_verify_pack_index`]'s errors -- a malformed local
/// advertisement must fail discovery rather than be silently skipped.
async fn current_pack_fanout(
    pond_root: &Path,
    series_hash: ObjectHash,
    manifest: &SeriesManifest,
) -> Result<(usize, Option<ObjectHash>), StewardError> {
    let mut best: Option<(usize, ObjectHash)> = None;
    for pack_hash in pack_store::list_local_pack_hashes(pond_root, series_hash).await? {
        let Some(index) =
            pack_store::read_and_verify_pack_index(pond_root, series_hash, pack_hash).await?
        else {
            continue;
        };
        if index.leaf_start() == 0
            && index.leaf_end() == manifest.leaf_count()
            && index.total_leaf_count() == manifest.leaf_count()
        {
            let n = index.physical_object_hashes().len();
            best = Some(match best {
                Some((b, bh)) if b <= n => (b, bh),
                _ => (n, pack_hash),
            });
        }
    }
    match best {
        Some((n, hash)) => Ok((n, Some(hash))),
        None => Ok((
            usize::try_from(manifest.leaf_count()).unwrap_or(usize::MAX),
            None,
        )),
    }
}

/// Whether `pack_hash` (assumed already established as the current best
/// full-range cover for a `TablePhysicalSeries`) carries a layout marker
/// matching this build's exact deterministic table layout constants --
/// i.e. it was already produced by *this* maintenance layout and, however
/// many objects it happens to contain (the byte safeguard can make that
/// more than a bare `ceil(rows / row_cap)` estimate), re-repacking it would
/// be a deterministic no-op. Used so discovery settles even though the
/// byte safeguard makes the row-count-only estimate an unreliable way to
/// recognize an already-bounded table pack (requirement 3).
async fn table_pack_is_current_deterministic_layout(
    pond_root: &Path,
    series_hash: ObjectHash,
    pack_hash: ObjectHash,
) -> Result<bool, StewardError> {
    let Some(marker) =
        pack_store::read_table_layout_marker(pond_root, series_hash, pack_hash).await
    else {
        return Ok(false);
    };
    Ok(marker.layout_version == TABLE_PACK_LAYOUT_VERSION
        && marker.row_cap == TABLE_PACK_MAX_ROWS_PER_OBJECT
        && marker.byte_safeguard_cap == TABLE_PACK_MAX_BYTES_PER_OBJECT)
}

/// One native v2 (or wholly-legacy) series' full survey result, computed
/// once against the `threshold=0` coarse discovery query (every series
/// with at least one live version) -- this is the single canonical pass
/// [`survey_all_v2_series`] performs; both [`classify_discovery`] (an
/// operational-`threshold` filter/classification over it) and
/// [`live_v2_series_hashes`] (the pond-wide live-series-hash set) are
/// derived from the *same* survey, so `run_pack_maintenance` never has to
/// re-run the coarse SQL query or re-fetch/re-fold any series' metadata a
/// second time just to get the other view of it (finding 6).
struct V2SeriesSurvey {
    file_id: FileID,
    /// Live versions that would collapse into one, from the coarse query
    /// -- compared against an operational `threshold` in
    /// [`classify_discovery`] exactly as the old per-threshold SQL
    /// `HAVING COUNT(*) > threshold` clause did.
    live_versions: usize,
    kind: V2SeriesSurveyKind,
}

enum V2SeriesSurveyKind {
    /// No persisted v2 leaf identity on any row: pack-only maintenance
    /// does not apply.
    Legacy,
    /// Boxed: `SeriesManifest` makes this variant much larger than
    /// `Legacy`, and a `Vec<V2SeriesSurvey>` may hold many entries at
    /// once, so indirection here avoids inflating every element (legacy
    /// or not) to the larger variant's size.
    V2(Box<V2SeriesDetails>),
}

struct V2SeriesDetails {
    entry_type: EntryType,
    series_hash: ObjectHash,
    manifest: SeriesManifest,
    current_physical_objects: usize,
    proposed_physical_objects: usize,
    existing_pack_hash: Option<ObjectHash>,
    already_deterministic: bool,
}

/// Survey every native v2 (and wholly-legacy) series pond-wide, exactly
/// once, against the coarse `threshold=0` discovery query -- i.e. every
/// series with at least one live version, independent of any operational
/// repack `threshold`. This is the single full pass both
/// [`discover_candidates`] and [`all_live_v2_series_hashes`] used to run
/// *separately* (once at the operational `threshold`, once again at `0`);
/// `run_pack_maintenance` now calls this once and derives both views from
/// its result (finding 6: avoid duplicate full discovery).
///
/// Metadata only: no leaf's inline `content` bytes are read or buffered
/// here for any candidate (requirement 4). A real repack re-fetches this
/// one series' full content-bearing rows fresh, immediately before
/// streaming it, one series at a time -- see [`run_pack_maintenance`].
///
/// # Errors
///
/// Returns an error if the coarse discovery query fails, if a series'
/// live rows cannot be read, if a series carries a genuine v1/v2 mix that
/// [`content_tree::build_series_manifest`] rejects as corrupt (as opposed
/// to a wholly pre-v2 series, reported as [`V2SeriesSurveyKind::Legacy`]
/// instead), or if this pond's own local pack advertisements cannot be
/// read.
async fn survey_all_v2_series(ship: &mut crate::Ship) -> Result<Vec<V2SeriesSurvey>, StewardError> {
    let coarse = ship.survey_collapsible_series(0).await?;
    let table = ship.data_persistence().table().clone();
    let pond_id = ship.data_persistence().pond_id().to_string();
    let pond_root = ship.pond_path().to_path_buf();

    let mut out = Vec::with_capacity(coarse.len());
    for candidate in coarse {
        let entry_type = candidate.file_id.entry_type();
        if !matches!(
            entry_type,
            EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries
        ) {
            // The coarse discovery SQL itself only ever selects these two
            // kinds; this is a defensive check, not an expected path.
            continue;
        }
        let file_id = candidate.file_id;
        let live_versions = candidate.live_versions;
        let node_id = file_id.node_id().to_string();
        let ordered_meta =
            content_tree::read_series_live_metadata_ordered(table.clone(), &pond_id, &node_id)
                .await?;

        // A genuinely pre-v2 series carries no `logical_leaf_hash` on *any*
        // row (v2 leaf stamping never ran) -- not corruption, simply a
        // physical layout this maintenance does not yet cover.
        // `build_series_manifest` cannot even be called on it: it treats
        // any nonempty leafless row as corrupt, which is the right call
        // for a genuinely mixed v1/v2 series but not for one that is
        // wholly pre-v2.
        let has_any_v2_identity = ordered_meta.iter().any(|v| v.logical_leaf_hash.is_some());
        if !has_any_v2_identity {
            out.push(V2SeriesSurvey {
                file_id,
                live_versions,
                kind: V2SeriesSurveyKind::Legacy,
            });
            continue;
        }

        let (manifest, _meta) = content_tree::build_series_manifest(entry_type, &ordered_meta)?;
        let series_hash = manifest.hash();
        let (current_physical_objects, best_pack_hash) =
            current_pack_fanout(&pond_root, series_hash, &manifest).await?;
        let cap = layout_cap(entry_type);
        let proposed_physical_objects = ceil_div(manifest.logical_count(), cap).max(1);

        // For a table series, a matching layout marker on the current best
        // full-range pack is a stronger, exact signal than the row-count
        // estimate above (which the byte safeguard can make inaccurate):
        // it means *this* deterministic layout already produced that exact
        // pack, so re-repacking it would be a no-op no matter how its
        // actual object count compares to the estimate (requirement 3).
        let already_deterministic = match (entry_type, best_pack_hash) {
            (EntryType::TablePhysicalSeries, Some(pack_hash)) => {
                table_pack_is_current_deterministic_layout(&pond_root, series_hash, pack_hash)
                    .await?
            }
            _ => false,
        };

        out.push(V2SeriesSurvey {
            file_id,
            live_versions,
            kind: V2SeriesSurveyKind::V2(Box::new(V2SeriesDetails {
                entry_type,
                series_hash,
                manifest,
                current_physical_objects,
                proposed_physical_objects: if already_deterministic {
                    current_physical_objects
                } else {
                    proposed_physical_objects
                },
                existing_pack_hash: best_pack_hash,
                already_deterministic,
            })),
        });
    }
    Ok(out)
}

/// Filter/classify one [`survey_all_v2_series`] pass down to exactly what
/// the old per-`threshold` coarse SQL query (`HAVING COUNT(*) > threshold`)
/// plus per-series candidacy check used to produce: only a series whose
/// `live_versions` exceeds `threshold` is even reported, and among those, a
/// v2 series needs a repack only if it is not already at its deterministic
/// bounded floor and still exceeds `threshold` in *physical* object count.
fn classify_discovery(survey: &[V2SeriesSurvey], threshold: usize) -> Vec<Discovery> {
    survey
        .iter()
        .filter(|entry| entry.live_versions > threshold)
        .map(|entry| match &entry.kind {
            V2SeriesSurveyKind::Legacy => Discovery::Legacy(entry.file_id),
            V2SeriesSurveyKind::V2(details) => {
                let series = DiscoveredSeries {
                    file_id: entry.file_id,
                    series_hash: details.series_hash,
                    entry_type: details.entry_type,
                    manifest: details.manifest.clone(),
                    current_physical_objects: details.current_physical_objects,
                    proposed_physical_objects: details.proposed_physical_objects,
                    existing_pack_hash: details.existing_pack_hash,
                };
                // The second clause is what makes discovery settle: a
                // series already at its achievable bounded floor is never
                // re-flagged merely for still nominally exceeding
                // `threshold` object-count, since there is nothing left
                // this maintenance could do about it.
                if !details.already_deterministic
                    && details.current_physical_objects > threshold
                    && details.current_physical_objects > details.proposed_physical_objects
                {
                    Discovery::NeedsRepack(series)
                } else {
                    Discovery::Bounded(series)
                }
            }
        })
        .collect()
}

/// Every native v2 series' manifest hash currently live in the pond,
/// pond-wide, independent of any repack `threshold` -- the set a
/// `series=<hash>` directory's hash must belong to in order to be kept
/// (requirement 2). Includes every entry from [`survey_all_v2_series`]'s
/// `threshold=0` pass, so a series with as few as one live leaf still
/// counts as live even though it would never be a repack candidate at a
/// higher operational threshold: a pack advertisement is pruned only for
/// truly no longer being the current representation of *any* live series,
/// never merely for being under some fragmentation threshold.
fn live_v2_series_hashes(survey: &[V2SeriesSurvey]) -> HashSet<ObjectHash> {
    survey
        .iter()
        .filter_map(|entry| match &entry.kind {
            V2SeriesSurveyKind::V2(details) => Some(details.series_hash),
            V2SeriesSurveyKind::Legacy => None,
        })
        .collect()
}

/// Discover every native v2 series pack maintenance would consider at
/// `threshold`: [`survey_all_v2_series`]'s single canonical pass, filtered
/// and classified by [`classify_discovery`]. Kept as its own entry point
/// for [`survey_pack_maintenance`]'s standalone preview use (a caller that
/// only wants this view, not the live-hash-set view too, does not need to
/// know the survey/classification split exists).
///
/// # Errors
/// Propagates [`survey_all_v2_series`]'s errors.
async fn discover_candidates(
    ship: &mut crate::Ship,
    threshold: usize,
) -> Result<Vec<Discovery>, StewardError> {
    let survey = survey_all_v2_series(ship).await?;
    Ok(classify_discovery(&survey, threshold))
}

/// Report which native v2 series pack maintenance would repack at
/// `threshold`, and the bounded layout it would publish, without writing
/// anything. Shares [`discover_candidates`] with [`run_pack_maintenance`]
/// so a preview cannot disagree with what a real run would do.
///
/// # Errors
/// Propagates [`discover_candidates`]'s errors.
pub(crate) async fn survey_pack_maintenance(
    ship: &mut crate::Ship,
    threshold: usize,
) -> Result<Vec<PackMaintenanceCandidate>, StewardError> {
    let discovered = discover_candidates(ship, threshold).await?;
    Ok(discovered.iter().map(candidate_from_discovery).collect())
}

/// What one repack call actually wrote.
struct RepackOutcome {
    objects_written: usize,
    bytes_written: u64,
    /// The number of physical objects the published pack actually
    /// contains (not merely those newly written this run -- an
    /// already-present, deduplicated object still counts). Used to report
    /// the *actual* achieved layout rather than a `ceil()` estimate
    /// (requirement 3).
    final_physical_objects: usize,
    /// The freshly published pack's own content-addressed hash, so the
    /// caller can select it as the one advertisement worth retaining for
    /// this series (requirement 2) and, for a table series, tag it with a
    /// layout marker (requirement 3).
    pack_hash: ObjectHash,
}

/// What one series' streaming repack (before its index is published, so
/// before its pack hash is known) actually wrote -- [`RepackOutcome`]
/// minus `pack_hash`.
struct StreamOutcome {
    objects_written: usize,
    bytes_written: u64,
    final_physical_objects: usize,
}

/// Bounded byte accumulator for a File-payload physical pack object:
/// buffers at most [`FILE_PACK_MAX_BYTES_PER_OBJECT`] bytes at a time,
/// durably writing (content-addressed, idempotent) and clearing its buffer
/// the moment that cap is reached. Physical object boundaries this
/// produces are entirely independent of logical leaf boundaries.
struct FileObjectAccumulator {
    buf: Vec<u8>,
    cap: usize,
    /// Unconditional running sum of every flushed physical object's actual
    /// byte length -- *including* an object that was already present
    /// (deduplicated) and so did not need writing. This, not
    /// `bytes_written` (new bytes only), is the series' true
    /// `physical_byte_count`: the real total size of the pack's actual
    /// final physical objects, which for a table series can differ from
    /// the original Oplog blob sizes it was built from (requirement 1).
    total_bytes: u64,
}

impl FileObjectAccumulator {
    fn new(cap: usize) -> Self {
        Self {
            buf: Vec::new(),
            cap: cap.max(1),
            total_bytes: 0,
        }
    }

    /// Feed the next slice of the concatenated logical byte stream, in
    /// order, flushing zero or more full-capacity physical objects as
    /// `cap` is reached. Never buffers more than one object's worth of
    /// bytes.
    async fn feed(
        &mut self,
        pond_root: &Path,
        mut chunk: &[u8],
        physical_object_hashes: &mut Vec<ObjectHash>,
        objects_written: &mut usize,
        bytes_written: &mut u64,
    ) -> Result<(), StewardError> {
        while !chunk.is_empty() {
            let space = self.cap - self.buf.len();
            let take = space.min(chunk.len());
            self.buf.extend_from_slice(&chunk[..take]);
            chunk = &chunk[take..];
            if self.buf.len() == self.cap {
                self.flush(
                    pond_root,
                    physical_object_hashes,
                    objects_written,
                    bytes_written,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn flush(
        &mut self,
        pond_root: &Path,
        physical_object_hashes: &mut Vec<ObjectHash>,
        objects_written: &mut usize,
        bytes_written: &mut u64,
    ) -> Result<(), StewardError> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let bytes = std::mem::take(&mut self.buf);
        let len = bytes.len() as u64;
        let (hash, wrote) = pack_store::write_pack_object(pond_root, &bytes).await?;
        physical_object_hashes.push(hash);
        // Unconditional: this object's actual bytes are part of the pack's
        // final physical footprint whether or not this run had to write
        // them anew.
        self.total_bytes += len;
        if wrote {
            *objects_written += 1;
            *bytes_written += len;
        }
        Ok(())
    }

    /// Flush whatever partial object remains at end of stream.
    async fn finish(
        &mut self,
        pond_root: &Path,
        physical_object_hashes: &mut Vec<ObjectHash>,
        objects_written: &mut usize,
        bytes_written: &mut u64,
    ) -> Result<(), StewardError> {
        self.flush(
            pond_root,
            physical_object_hashes,
            objects_written,
            bytes_written,
        )
        .await
    }
}

/// Stream a `FilePhysicalSeries`' live leaves into a bounded physical pack,
/// recomputing and verifying every leaf hash against its persisted value as
/// it streams (never trusting the persisted `logical_leaf_hash` blindly),
/// and never holding more than one bounded physical object's bytes (plus
/// the one leaf/chunk currently in flight) in memory at a time.
///
/// `ordered` is this one series' *metadata-only* live rows (see
/// [`content_tree::read_series_live_metadata_ordered`]) -- their
/// `content` field is always `None` regardless of whether a version's row
/// actually carries inline bytes. This function fetches each leaf's own
/// content lazily, one leaf at a time, via
/// [`content_tree::read_series_version_inline_content`], immediately
/// before that leaf is streamed, and lets it drop at the end of that loop
/// iteration -- never buffering more than one leaf's inline content
/// (finding 2), and never held by [`DiscoveredSeries`] itself
/// (requirement 4).
///
/// # Errors
///
/// Returns an error if a leaf-bearing version is missing its
/// `logical_count`, if its content cannot be fetched or streamed, if its
/// recomputed leaf hash does not match its persisted `logical_leaf_hash`
/// (corrupt row), or if the assembled [`PackIndex`] fails its own
/// self-verification against `series.manifest`.
async fn repack_file_series(
    ship: &crate::Ship,
    pond_root: &Path,
    table: &deltalake::DeltaTable,
    pond_id: &str,
    series: &DiscoveredSeries,
    ordered: &[content_tree::SeriesVersionData],
) -> Result<(PackIndex, StreamOutcome), StewardError> {
    let node_id = series.file_id.node_id().to_string();
    let leaf_versions: Vec<&content_tree::SeriesVersionData> = ordered
        .iter()
        .filter(|v| v.logical_leaf_hash.is_some())
        .collect();

    let mut whole_series_leaf_hashes = Vec::with_capacity(leaf_versions.len());
    let mut leaf_descriptors = Vec::with_capacity(leaf_versions.len());
    let mut physical_object_hashes: Vec<ObjectHash> = Vec::new();
    let mut objects_written = 0usize;
    let mut bytes_written = 0u64;
    let cap = usize::try_from(FILE_PACK_MAX_BYTES_PER_OBJECT).unwrap_or(usize::MAX);
    let mut accumulator = FileObjectAccumulator::new(cap);

    for v in &leaf_versions {
        let expected_leaf_hash = v.logical_leaf_hash.expect("filtered for Some above");
        let logical_count = v.logical_count.ok_or_else(|| {
            StewardError::Content(
                "leaf-bearing file series version has a logical_leaf_hash but no logical_count"
                    .to_string(),
            )
        })?;
        let logical_count = u64::try_from(logical_count).map_err(|_| {
            StewardError::Content("series version logical_count is negative".to_string())
        })?;
        let attrs = content_tree::canonical_leaf_attributes(v)?;
        let mut hasher = sync_store::content::IncrementalFileLeafHasher::new(
            logical_count,
            v.meta.min_event_time,
            v.meta.max_event_time,
            attrs.as_deref(),
        )
        .map_err(StewardError::Content)?;

        // Fetch this one leaf's inline content lazily, immediately before
        // streaming it, and let it drop at the end of this loop iteration
        // -- never a whole series' worth of inline leaves at once
        // (finding 2). `None` means this version's row was externalized,
        // so its bytes are streamed from `_large_files` instead, exactly
        // as before.
        let inline_content = content_tree::read_series_version_inline_content(
            table.clone(),
            pond_id,
            &node_id,
            v.version,
        )
        .await?;

        match inline_content {
            Some(bytes) => {
                hasher.write(&bytes).map_err(StewardError::Content)?;
                accumulator
                    .feed(
                        pond_root,
                        &bytes,
                        &mut physical_object_hashes,
                        &mut objects_written,
                        &mut bytes_written,
                    )
                    .await?;
            }
            None => {
                let mut reader = ship
                    .data_persistence()
                    .open_large_file_reader_by_hash(&v.blob_hash.to_hex())
                    .await
                    .map_err(|e| {
                        StewardError::Content(format!(
                            "stream externalized file leaf {}: {e}",
                            v.blob_hash
                        ))
                    })?;
                let mut buf = vec![0u8; FILE_STREAM_CHUNK_BYTES];
                loop {
                    let n = reader.read(&mut buf).await?;
                    if n == 0 {
                        break;
                    }
                    hasher.write(&buf[..n]).map_err(StewardError::Content)?;
                    accumulator
                        .feed(
                            pond_root,
                            &buf[..n],
                            &mut physical_object_hashes,
                            &mut objects_written,
                            &mut bytes_written,
                        )
                        .await?;
                }
            }
        }

        let recomputed = hasher.finish().map_err(StewardError::Content)?;
        if recomputed != expected_leaf_hash {
            return Err(StewardError::Content(format!(
                "file leaf for node {} recomputed hash {recomputed} does not match its persisted \
                 leaf hash {expected_leaf_hash} (corrupt row)",
                series.file_id.node_id()
            )));
        }

        let descriptor = PackLeafDescriptor::new(
            logical_count,
            v.meta.min_event_time,
            v.meta.max_event_time,
            attrs,
        )
        .map_err(StewardError::Content)?;
        whole_series_leaf_hashes.push(recomputed);
        leaf_descriptors.push(descriptor);
    }

    accumulator
        .finish(
            pond_root,
            &mut physical_object_hashes,
            &mut objects_written,
            &mut bytes_written,
        )
        .await?;

    // The unconditional sum of every actually-flushed physical object's
    // real byte length -- including any object that was already present
    // (deduplicated) and so is not part of `bytes_written` -- never the
    // original Oplog leaves' `blob_size` (requirement 1). For the file
    // path these two happen to agree today (a straight byte
    // concatenation), but this is computed from the pack's own actual
    // output, not derived from input sizes, so it stays correct even if
    // that ever changes.
    let physical_byte_count = accumulator.total_bytes;
    let final_physical_objects = physical_object_hashes.len();

    let index = finish_pack_index(
        series,
        whole_series_leaf_hashes,
        physical_object_hashes,
        leaf_descriptors,
        physical_byte_count,
    )?;

    Ok((
        index,
        StreamOutcome {
            objects_written,
            bytes_written,
            final_physical_objects,
        },
    ))
}

/// Encode `pending`'s accumulated rows into one bounded physical Parquet
/// object and durably write it, clearing `pending` for the next one.
async fn flush_table_object(
    pond_root: &Path,
    schema: &Arc<Schema>,
    pending: &mut Vec<RecordBatch>,
    physical_object_hashes: &mut Vec<ObjectHash>,
    objects_written: &mut usize,
    bytes_written: &mut u64,
    physical_byte_count: &mut u64,
) -> Result<(), StewardError> {
    if pending.is_empty() {
        return Ok(());
    }
    let bytes = sync_store::content::encode_table_leaf_parquet(schema, pending)
        .map_err(StewardError::Content)?;
    pending.clear();
    let len = bytes.len() as u64;
    let (hash, wrote) = pack_store::write_pack_object(pond_root, &bytes).await?;
    physical_object_hashes.push(hash);
    // Unconditional: this object's actual encoded Parquet bytes are part of
    // the pack's real final physical footprint whether or not this run had
    // to write them anew -- this, not the original leaves' `blob_size`, is
    // `physical_byte_count` (requirement 1); a table repack re-encodes
    // Parquet, so the two can differ.
    *physical_byte_count += len;
    if wrote {
        *objects_written += 1;
        *bytes_written += len;
    }
    Ok(())
}

/// Stream a `TablePhysicalSeries`' live leaves into a bounded physical
/// pack.
///
/// `ordered` is this one series' *metadata-only* live rows (see
/// [`content_tree::read_series_live_metadata_ordered`]); each leaf's own
/// content is instead fetched lazily, one leaf at a time, via
/// [`content_tree::read_series_version_inline_content`] (finding 2),
/// immediately before that leaf's Parquet bytes are decoded via
/// [`content_pull::decode_table_object`], the same decoder
/// `fetch_and_verify_table_pack` already trusts. Every leaf's decoded
/// batches are cast, column by column, to one shared canonical `Schema`
/// `Arc` with `arrow-cast` (finding 3: a `Dictionary<K, V>` column is cast
/// to plain `V`, not merely reasserted), so leaves whose Parquet
/// physically encodes an equivalent logical column differently can still
/// be accumulated into one physical object, then handed to
/// [`sync_store::content::TableLeafInput::new`], which itself recomputes
/// and verifies the leaf hash from that decoded content -- checked here
/// against the persisted `logical_leaf_hash` before the leaf's rows are
/// ever accumulated into a pending physical object. Pending rows are held
/// as zero-copy [`RecordBatch::slice`]s, bounded by both a row cap
/// ([`TABLE_PACK_MAX_ROWS_PER_OBJECT`]) and an estimated in-memory byte
/// safeguard ([`TABLE_PACK_MAX_BYTES_PER_OBJECT`]).
///
/// # Errors
///
/// Returns an error if the series manifest carries no schema fingerprint,
/// if a leaf's Parquet bytes cannot be read, fetched, or decoded, if a
/// decoded leaf's schema fingerprint does not match the manifest's, if a
/// leaf's decoded columns cannot be cast to the run's canonical schema, if
/// a leaf's recomputed hash does not match its persisted
/// `logical_leaf_hash` (corrupt row), or if the assembled [`PackIndex`]
/// fails its own self-verification against `series.manifest`.
async fn repack_table_series(
    ship: &crate::Ship,
    pond_root: &Path,
    table: &deltalake::DeltaTable,
    pond_id: &str,
    series: &DiscoveredSeries,
    ordered: &[content_tree::SeriesVersionData],
) -> Result<(PackIndex, StreamOutcome), StewardError> {
    let node_id = series.file_id.node_id().to_string();
    let leaf_versions: Vec<&content_tree::SeriesVersionData> = ordered
        .iter()
        .filter(|v| v.logical_leaf_hash.is_some())
        .collect();

    let schema_fingerprint = series.manifest.schema_fingerprint().ok_or_else(|| {
        StewardError::Content(format!(
            "table series {} manifest has no schema_fingerprint",
            series.file_id.node_id()
        ))
    })?;

    let mut whole_series_leaf_hashes = Vec::with_capacity(leaf_versions.len());
    let mut leaf_descriptors = Vec::with_capacity(leaf_versions.len());
    let mut physical_object_hashes: Vec<ObjectHash> = Vec::new();
    let mut physical_byte_count: u64 = 0;
    let mut objects_written = 0usize;
    let mut bytes_written = 0u64;

    let mut canonical_schema: Option<Arc<Schema>> = None;
    let mut pending_batches: Vec<RecordBatch> = Vec::new();
    let mut pending_rows: u64 = 0;
    // Incremental, proportional estimate of `pending_batches`' real
    // in-memory footprint: `RecordBatch::get_array_memory_size()` reports
    // a zero-copy slice's *entire underlying buffer* size, not a
    // proportional share of it, so summing that over every pending slice
    // (as this used to) drastically overestimates real memory and
    // triggers the byte safeguard far earlier than warranted (requirement
    // 3's "zero-copy sliced batches don't overestimate retained memory").
    // Instead, each pushed slice contributes its proportional share
    // (`whole batch's memory * rows taken / rows in whole batch`) of its
    // *un-sliced* source batch, accumulated here and reset on flush.
    let mut pending_bytes_estimate: u64 = 0;

    for v in &leaf_versions {
        let expected_leaf_hash = v.logical_leaf_hash.expect("filtered for Some above");
        // Fetch this one leaf's inline content lazily, immediately before
        // decoding it, and let it drop once decoded -- never a whole
        // series' worth of inline leaves at once (finding 2). Owned
        // outright (no clone needed): unlike the old whole-series
        // `ordered` slice, this per-leaf fetch already hands back an
        // owned `Vec<u8>` the caller may consume directly (finding 3's
        // "remove table's extra raw bytes clone").
        let raw_bytes: Vec<u8> = match content_tree::read_series_version_inline_content(
            table.clone(),
            pond_id,
            &node_id,
            v.version,
        )
        .await?
        {
            Some(bytes) => bytes,
            None => ship
                .data_persistence()
                .read_large_file_bytes(&v.blob_hash.to_hex())
                .await
                .map_err(|e| {
                    StewardError::Content(format!(
                        "read externalized table leaf {}: {e}",
                        v.blob_hash
                    ))
                })?,
        };
        let (decoded_schema, decoded_batches) =
            content_pull::decode_table_object(bytes::Bytes::from(raw_bytes), schema_fingerprint)
                .await?;
        let schema = match &canonical_schema {
            Some(schema) => Arc::clone(schema),
            None => {
                // The canonical logical schema (finding 3): every field's
                // `DataType` normalized via
                // `sync_store::content::canonicalize_schema` (a
                // `Dictionary<K, V>` column becomes plain `V`), so a run
                // whose leaves mix dictionary- and plain-encoded Parquet
                // for the same logical column can still be accumulated
                // into one physical object -- rather than the first
                // leaf's raw (possibly dictionary-encoded) physical
                // schema being forced, unchanged, onto every later leaf.
                let schema = sync_store::content::canonicalize_schema(&decoded_schema)
                    .map_err(StewardError::Content)?;
                canonical_schema = Some(Arc::clone(&schema));
                schema
            }
        };

        // Cast every decoded column to the canonical schema's field type
        // with `arrow-cast` (a real typed cast, e.g. `Dictionary<K, Utf8>`
        // -> `Utf8`), never merely reinterpreted/reasserted the way
        // `RecordBatch::try_new` alone would -- that used to fail outright
        // for a leaf whose physical encoding legitimately differed from
        // the first leaf's, even though both share this run's one
        // `schema_fingerprint`.
        let mut normalized_batches = Vec::with_capacity(decoded_batches.len());
        for batch in &decoded_batches {
            let mut columns = Vec::with_capacity(batch.num_columns());
            for (i, field) in schema.fields().iter().enumerate() {
                let array = batch.column(i);
                let cast = arrow_cast::cast(array, field.data_type()).map_err(|e| {
                    StewardError::Content(format!(
                        "cast decoded table leaf column {:?} to canonical type {:?}: {e}",
                        field.name(),
                        field.data_type()
                    ))
                })?;
                columns.push(cast);
            }
            let normalized = RecordBatch::try_new(Arc::clone(&schema), columns).map_err(|e| {
                StewardError::Content(format!("normalize decoded table leaf schema: {e}"))
            })?;
            normalized_batches.push(normalized);
        }

        let attrs = content_tree::canonical_leaf_attributes(v)?;
        let leaf_input = sync_store::content::TableLeafInput::new(
            Arc::clone(&schema),
            normalized_batches,
            v.meta.min_event_time,
            v.meta.max_event_time,
            attrs,
        )
        .map_err(StewardError::Content)?;

        if leaf_input.leaf_hash() != expected_leaf_hash {
            return Err(StewardError::Content(format!(
                "table leaf for node {} recomputed hash {} does not match its persisted leaf \
                 hash {expected_leaf_hash} (corrupt row)",
                series.file_id.node_id(),
                leaf_input.leaf_hash(),
            )));
        }

        whole_series_leaf_hashes.push(leaf_input.leaf_hash());
        leaf_descriptors.push(leaf_input.descriptor().clone());

        for batch in leaf_input.batches() {
            let total = batch.num_rows();
            // Computed once per whole (un-sliced) batch: the proportional
            // share each row of it contributes to `pending_bytes_estimate`
            // below.
            let batch_mem = batch.get_array_memory_size() as u64;
            let mut offset = 0usize;
            while offset < total {
                let space = TABLE_PACK_MAX_ROWS_PER_OBJECT - pending_rows;
                let space_usize = usize::try_from(space).unwrap_or(usize::MAX);
                let take = space_usize.min(total - offset);
                if take > 0 {
                    pending_batches.push(batch.slice(offset, take));
                    pending_rows += take as u64;
                    pending_bytes_estimate += batch_mem * (take as u64) / (total as u64).max(1);
                    offset += take;
                }
                if pending_rows == TABLE_PACK_MAX_ROWS_PER_OBJECT
                    || pending_bytes_estimate >= TABLE_PACK_MAX_BYTES_PER_OBJECT
                {
                    flush_table_object(
                        pond_root,
                        &schema,
                        &mut pending_batches,
                        &mut physical_object_hashes,
                        &mut objects_written,
                        &mut bytes_written,
                        &mut physical_byte_count,
                    )
                    .await?;
                    pending_rows = 0;
                    pending_bytes_estimate = 0;
                }
            }
        }
    }

    let schema = canonical_schema.ok_or_else(|| {
        StewardError::Content(format!(
            "table series {} produced no leaf schema during repack (empty leaf set)",
            series.file_id.node_id()
        ))
    })?;
    flush_table_object(
        pond_root,
        &schema,
        &mut pending_batches,
        &mut physical_object_hashes,
        &mut objects_written,
        &mut bytes_written,
        &mut physical_byte_count,
    )
    .await?;

    let final_physical_objects = physical_object_hashes.len();

    let index = finish_pack_index(
        series,
        whole_series_leaf_hashes,
        physical_object_hashes,
        leaf_descriptors,
        physical_byte_count,
    )?;

    Ok((
        index,
        StreamOutcome {
            objects_written,
            bytes_written,
            final_physical_objects,
        },
    ))
}

/// Assemble and self-verify a full-range `[0, leaf_count)` [`PackIndex`]
/// from a repack's recomputed leaf hashes/descriptors and its freshly
/// written physical objects -- mirrors
/// [`content_tree::build_initial_pack_index`]'s exact construction and
/// self-check pattern, just fed from a repack's own accumulated values
/// instead of persisted per-version physical objects.
///
/// # Errors
///
/// Returns an error if the produced leaf count does not match the
/// manifest's, if building the range proof or [`PackIndex`] itself is
/// rejected, or if the freshly built pack fails its own self-check against
/// `series.manifest` -- any of which would mean an internal bug in this
/// repack, not user error, and must not be published.
fn finish_pack_index(
    series: &DiscoveredSeries,
    whole_series_leaf_hashes: Vec<ObjectHash>,
    physical_object_hashes: Vec<ObjectHash>,
    leaf_descriptors: Vec<PackLeafDescriptor>,
    physical_byte_count: u64,
) -> Result<PackIndex, StewardError> {
    let total_leaf_count = whole_series_leaf_hashes.len() as u64;
    if total_leaf_count != series.manifest.leaf_count() {
        return Err(StewardError::Content(format!(
            "repack for node {} produced {total_leaf_count} leaf hash(es) but the manifest \
             declares leaf_count {}",
            series.file_id.node_id(),
            series.manifest.leaf_count()
        )));
    }

    let range_proof = sync_store::content::generate_range_proof(
        &whole_series_leaf_hashes,
        0,
        whole_series_leaf_hashes.len(),
    )
    .map_err(StewardError::Content)?;
    let range_root = series.manifest.leaf_merkle_root();

    let index = PackIndex::new(
        series.series_hash,
        0,
        total_leaf_count,
        total_leaf_count,
        range_root,
        range_proof,
        physical_object_hashes,
        series.manifest.logical_count(),
        physical_byte_count,
        leaf_descriptors,
    )
    .map_err(StewardError::Content)?;

    sync_store::content::verify_pack_against_manifest(
        series.series_hash,
        &series.manifest,
        &index,
        &whole_series_leaf_hashes,
    )
    .map_err(StewardError::Content)?;

    Ok(index)
}

/// Repack one series (dispatching on payload kind) and publish its pack
/// index. Physical objects are written during the streaming repack itself,
/// strictly before the index that names them is published here --
/// satisfies the "objects-first, index-last" atomicity requirement.
///
/// `ordered` is this one series' *metadata-only* live rows (bounded, no
/// payload bytes); each leaf's own content is instead fetched lazily, one
/// leaf at a time, during the streaming repack itself (finding 2; see
/// [`repack_file_series`]/[`repack_table_series`]).
///
/// For a `TablePhysicalSeries`, also writes a [`pack_store::TableLayoutMarker`]
/// sidecar naming this build's exact deterministic layout constants, so a
/// later discovery pass can recognize this exact pack as already produced
/// by the current maintenance layout without re-decoding it or trusting an
/// inherently approximate row-count estimate (requirement 3). The marker
/// is published *after* the index it describes: it is a discovery-time
/// optimization hint, never load-bearing for correctness, so its own
/// durability need not precede the index's.
///
/// # Errors
///
/// Propagates [`repack_file_series`]/[`repack_table_series`]'s and
/// [`pack_store::publish_pack_index`]'s errors.
async fn repack_series(
    ship: &crate::Ship,
    pond_root: &Path,
    table: &deltalake::DeltaTable,
    pond_id: &str,
    series: &DiscoveredSeries,
    ordered: &[content_tree::SeriesVersionData],
) -> Result<RepackOutcome, StewardError> {
    let (index, outcome) = match series.entry_type {
        EntryType::FilePhysicalSeries => {
            repack_file_series(ship, pond_root, table, pond_id, series, ordered).await?
        }
        EntryType::TablePhysicalSeries => {
            repack_table_series(ship, pond_root, table, pond_id, series, ordered).await?
        }
        other => {
            return Err(StewardError::Content(format!(
                "pack maintenance cannot repack entry type {other:?}"
            )));
        }
    };
    let pack_hash = pack_store::publish_pack_index(pond_root, series.series_hash, &index).await?;

    if series.entry_type == EntryType::TablePhysicalSeries {
        pack_store::write_table_layout_marker(
            pond_root,
            series.series_hash,
            pack_hash,
            pack_store::TableLayoutMarker {
                layout_version: TABLE_PACK_LAYOUT_VERSION,
                row_cap: TABLE_PACK_MAX_ROWS_PER_OBJECT,
                byte_safeguard_cap: TABLE_PACK_MAX_BYTES_PER_OBJECT,
            },
        )
        .await?;
    }

    Ok(RepackOutcome {
        objects_written: outcome.objects_written,
        bytes_written: outcome.bytes_written,
        final_physical_objects: outcome.final_physical_objects,
        pack_hash,
    })
}

/// Run real pack-only maintenance at `threshold`: repack every
/// over-threshold native v2 series discovered, prune obsolete local pack
/// advertisements (requirement 2), then sweep any physical pack object no
/// currently-retained local pack advertisement references.
///
/// Acquires the same pond write lock
/// ([`crate::write_lock::WriteLockGuard`]) [`crate::Ship::reclaim`] uses,
/// serializing this local-disk mutation with reclamation's -- both are
/// called in turn (never nested) from [`crate::Ship::collapse_versions`].
/// Neither this function nor reclaim touches the Delta table's own
/// transaction log at all: only local disk under `_packs/` is mutated
/// here, so no Delta root/version/txn-sequence state is at stake.
///
/// # Ordering (crash safety and requirement 2's "never prune before the
/// replacement index is durable")
///
/// 1. Every candidate needing a repack is streamed and published first:
///    its physical objects are written and fsynced, then its
///    [`PackIndex`] is published and fsynced ("objects-first, index-last"
///    -- see [`repack_series`]), before anything is ever pruned.
/// 2. Only once every repack above has successfully published its own
///    durable index does this compute the pond-wide set of currently-live
///    series manifest hashes ([`live_v2_series_hashes`], derived from the
///    single [`survey_all_v2_series`] pass taken at the very start of this
///    call -- finding 6: never re-surveyed a second time) and delete any
///    `series=<hash>` directory whose hash is not in that set
///    ([`pack_store::prune_obsolete_series_dirs`]) -- each such directory's
///    every advertisement is itself re-validated content-addressedly
///    before deletion, failing loudly rather than silently on anything
///    malformed (requirement 2's "fail safe on malformed state").
/// 3. For every series still live, any extra advertisement beyond the one
///    just-published or previously-selected pack is collapsed away
///    ([`pack_store::retain_selected_pack_only`]), so a series settles to
///    exactly one advertisement rather than accumulating one per repack.
/// 4. Finally, every physical object no surviving advertisement
///    references is swept -- this is the same object-level GC that always
///    ran here, now operating over a much smaller retained advertisement
///    set thanks to steps 2-3, which is what keeps disk growth from
///    becoming quadratic in the number of append/repack cycles.
///
/// One series's live version *metadata* is fetched fresh, immediately
/// before that series is streamed (bounded, no payload bytes); each
/// leaf's own content is then fetched lazily, one leaf at a time, during
/// the streaming repack itself and dropped once that leaf is consumed --
/// never more than one series' metadata, nor more than one leaf's content,
/// held at a time, and never any of it for a series that turns out not to
/// need a repack at all (finding 2; see [`discover_candidates`]'s own
/// metadata-only discovery stage).
///
/// # Errors
///
/// Returns an error if the write lock cannot be acquired, if discovery or
/// any repack fails, if the live-series survey fails, if pruning
/// encounters a malformed advertisement, or if the final GC sweep's
/// pack-index enumeration fails (a malformed/cross-series advertisement
/// fails this outright rather than risk sweeping something it still
/// depends on).
pub(crate) async fn run_pack_maintenance(
    ship: &mut crate::Ship,
    threshold: usize,
    meta: &tlogfs::PondUserMetadata,
) -> Result<PackMaintenanceReport, StewardError> {
    let control_dir = crate::get_control_path(ship.pond_path());
    let mut txn_meta = tlogfs::PondTxnMetadata::new(ship.last_write_seq(), meta.clone());
    txn_meta.pond_id = ship.control_table().pond_id_uuid().to_string();
    let _write_lock = crate::write_lock::WriteLockGuard::try_acquire(&control_dir, &txn_meta)?;

    let survey = survey_all_v2_series(ship).await?;
    let discovered = classify_discovery(&survey, threshold);
    let pond_root = ship.pond_path().to_path_buf();
    let table = ship.data_persistence().table().clone();
    let pond_id = ship.data_persistence().pond_id().to_string();

    let mut report = PackMaintenanceReport::default();
    // The one pack advertisement worth retaining for each series touched
    // this run, whether freshly repacked or already bounded -- fed to
    // `retain_selected_pack_only` below, after pruning.
    let mut selected_packs: Vec<(ObjectHash, ObjectHash)> = Vec::new();

    for entry in discovered {
        match entry {
            Discovery::Legacy(_) => {
                report.unsupported_legacy += 1;
                report.candidates.push(candidate_from_discovery(&entry));
            }
            Discovery::Bounded(ref series) => {
                report.already_bounded += 1;
                if let Some(pack_hash) = series.existing_pack_hash {
                    selected_packs.push((series.series_hash, pack_hash));
                }
                report.candidates.push(candidate_from_series(
                    series,
                    PackCandidateOutcome::AlreadyBounded,
                ));
            }
            Discovery::NeedsRepack(ref series) => {
                // Fetch this one series' live version *metadata* fresh,
                // right before streaming it, and let it drop at the end
                // of this loop iteration -- never buffered alongside any
                // other series' metadata, and never carrying any leaf's
                // payload bytes at all (finding 2): each leaf's own
                // content is instead fetched lazily, one leaf at a time,
                // inside `repack_series` itself.
                let node_id = series.file_id.node_id().to_string();
                let ordered = content_tree::read_series_live_metadata_ordered(
                    table.clone(),
                    &pond_id,
                    &node_id,
                )
                .await?;

                let outcome =
                    repack_series(ship, &pond_root, &table, &pond_id, series, &ordered).await?;
                report.series_repacked += 1;
                report.pack_objects_written += outcome.objects_written;
                report.pack_bytes_written += outcome.bytes_written;
                selected_packs.push((series.series_hash, outcome.pack_hash));
                let mut candidate = candidate_from_series(series, PackCandidateOutcome::Repacked);
                candidate.proposed_physical_objects = outcome.final_physical_objects;
                report.candidates.push(candidate);
            }
        }
    }

    // Requirement 2: prune every `series=<hash>` directory whose hash is
    // not among any series currently live pond-wide (independent of this
    // run's `threshold`), only now that every repack above has already
    // published its own durable replacement index.
    let live_series_hashes = live_v2_series_hashes(&survey);
    let prune = pack_store::prune_obsolete_series_dirs(&pond_root, &live_series_hashes).await?;
    report.series_dirs_removed = prune.removed;
    report.series_dirs_marked_stale = prune.marked_stale;

    // Collapse any series that still carries more than one advertisement
    // (an old repack's leftover, or a pre-existing duplicate) down to just
    // the one selected above.
    for (series_hash, pack_hash) in selected_packs {
        let retention =
            pack_store::retain_selected_pack_only(&pond_root, series_hash, pack_hash).await?;
        report.pack_advertisements_removed += retention.removed;
        report.pack_advertisements_marked_stale += retention.marked_stale;
        report.orphan_markers_removed += retention.orphan_markers_removed;
    }

    // GC: every physical object referenced by any retained, valid local
    // pack advertisement is a root. `all_local_pack_indexes` fails loud on
    // any malformed/cross-series entry rather than silently treat it as
    // naming no objects (requirement 5's correctness-over-cleanup rule).
    // Runs after every repack above has already published its own index,
    // and after pruning/retention have already removed every advertisement
    // this run decided not to keep, so a freshly written pack's objects
    // are already referenced -- and a pruned/retired pack's objects are
    // already unreferenced -- by the time this looks.
    let mut referenced: HashSet<ObjectHash> = HashSet::new();
    for (_, _, index) in pack_store::all_local_pack_indexes(&pond_root).await? {
        referenced.extend(index.physical_object_hashes().iter().copied());
    }
    let sweep = pack_store::sweep_unreferenced_pack_objects(&pond_root, &referenced).await?;
    report.pack_objects_removed = sweep.removed;
    report.pack_bytes_freed = sweep.bytes_freed;

    Ok(report)
}
