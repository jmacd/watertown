// SPDX-License-Identifier: Apache-2.0

//! Tile materialization for the transparency log (design Decision D5).
//!
//! The RFC 6962 SHA-256 Merkle tree over the commit spine (see [`super`]) is
//! persisted as **tiles** in the C2SP `tlog-tiles` layout: fixed-size,
//! immutable, statically servable blocks of hashes.  A verifier fetches a
//! handful of tiles plus the signed [`Checkpoint`](super::Checkpoint) and
//! recomputes an O(log n) inclusion or consistency proof, never walking the
//! whole log.
//!
//! # Layout
//!
//! A tile at level `L` (>= 0) and index `N` holds up to [`TILE_WIDTH`] hashes
//! taken from tree level `8 * L`; each hash covers a complete subtree of
//! `2^(8L)` leaves.  Level 0 therefore holds the leaf hashes themselves.  Tiles
//! live under a log directory as:
//!
//! ```text
//! tile/<L>/<N>          full tile   (TILE_WIDTH hashes)
//! tile/<L>/<N>.p/<W>    partial tile (W < TILE_WIDTH hashes, rightmost only)
//! checkpoint            the tlog-checkpoint note body
//! ```
//!
//! `<N>` is encoded in base-1000 groups (`x001/x234/567`) per the spec so no
//! single directory holds more than 1000 entries.  A full tile is immutable:
//! once written it is never rewritten.  Only the rightmost (partial) tile at
//! each level changes as the log grows.
//!
//! # Complexity
//!
//! The incremental writer ([`TileLog::append_leaf_hashes`]) folds only the
//! touched right spine: appending a leaf carries up the completed-subtree chain
//! (`O(1)` amortized hashing) and recomputes the tree head in `O(log n)`,
//! without ever reloading the existing leaves.  Full tiles are immutable and
//! written exactly once.  The whole-tree [`TileLog::materialize`] recomputes
//! everything from a leaf set (`O(n)`) and is kept for building or repairing a
//! log; both paths produce byte-identical tiles.

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};

use super::checkpoint::Checkpoint;
use super::merkle::{LogHash, empty_root, hash_children, hash_leaf, mth};

/// The number of hashes in a full tile (C2SP tile height 8, so `2^8`).
pub const TILE_WIDTH: u64 = 256;
/// Bits of tree level spanned by one tile level (C2SP tile height 8).
pub const TILE_HEIGHT_BITS: u32 = 8;
/// The byte length of one SHA-256 log hash.
const HASH_LEN: usize = 32;

/// A materialized, C2SP `tlog-tiles` view of the commit-spine transparency log.
///
/// Bound to a log directory (`{POND}/tlog`) and the log's origin identifier.
/// Appending leaves rewrites the affected tiles and re-emits the checkpoint.
#[derive(Debug, Clone)]
pub struct TileLog {
    dir: PathBuf,
    origin: String,
}

impl TileLog {
    /// Bind a tile log to directory `dir` with the given `origin` identifier.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>, origin: impl Into<String>) -> Self {
        Self {
            dir: dir.into(),
            origin: origin.into(),
        }
    }

    /// The log directory.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The current tree size (leaf count) recorded in the checkpoint, or 0 if
    /// the log has not been materialized yet.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint exists but cannot be read or parsed.
    pub fn size(&self) -> Result<u64, super::CheckpointError> {
        Ok(Checkpoint::read(&self.dir)?.map(|c| c.size).unwrap_or(0))
    }

    /// Read every leaf hash from the level-0 tiles, in log order.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint or any expected level-0 tile is
    /// missing or malformed.
    pub fn load_leaves(&self) -> Result<Vec<LogHash>, super::CheckpointError> {
        let size = self.size()?;
        let mut leaves = Vec::with_capacity(size as usize);
        let full = size / TILE_WIDTH;
        for n in 0..full {
            self.read_tile_hashes(0, n, TILE_WIDTH as usize, &mut leaves)?;
        }
        let rem = (size % TILE_WIDTH) as usize;
        if rem > 0 {
            self.read_tile_hashes(0, full, rem, &mut leaves)?;
        }
        Ok(leaves)
    }

    /// Append leaves given by their raw leaf data, re-materialize the tiles, and
    /// emit a fresh checkpoint.  Returns the new checkpoint.
    ///
    /// Each entry of `leaf_data` is hashed with the RFC 6962 leaf rule
    /// (`SHA-256(0x00 || data)`) before being appended.
    ///
    /// # Errors
    ///
    /// Returns an error if existing leaves cannot be loaded or any tile /
    /// checkpoint write fails.
    pub fn append_leaf_data<I, B>(&self, leaf_data: I) -> Result<Checkpoint, super::CheckpointError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        let hashes = leaf_data.into_iter().map(|d| hash_leaf(d.as_ref()));
        self.append_leaf_hashes(hashes)
    }

    /// Append pre-computed leaf hashes incrementally and emit a fresh
    /// checkpoint.  Returns the new checkpoint.
    ///
    /// This folds only the touched right spine of the tree: each appended leaf
    /// does `O(1)` amortized hashing (a carry up the completed-subtree chain),
    /// and the new tree head costs `O(log n)`.  Existing leaves are never
    /// reloaded and full tiles are never rewritten.  The result is
    /// byte-identical to [`Self::materialize`] over the whole leaf sequence.
    ///
    /// # Errors
    ///
    /// Returns an error if the current size cannot be read or any tile /
    /// checkpoint write fails.
    pub fn append_leaf_hashes<I>(&self, hashes: I) -> Result<Checkpoint, super::CheckpointError>
    where
        I: IntoIterator<Item = LogHash>,
    {
        let old = self.size()?;
        let mut session = TileSession::open(self.dir.clone(), old);
        let mut index = old;
        for h in hashes {
            session.push_leaf(index, h)?;
            index += 1;
        }
        let new = index;
        let root = if new == 0 {
            empty_root()
        } else {
            session.tree_head(new)?
        };
        session.flush()?;

        let checkpoint = Checkpoint::new(self.origin.clone(), new, root);
        checkpoint.write(&self.dir)?;
        checkpoint.append_to_history(&self.dir)?;
        Ok(checkpoint)
    }

    /// Write all tiles for the leaf sequence `leaves` and emit the checkpoint,
    /// recomputing every tile from scratch (`O(n)`).
    ///
    /// This is the whole-tree (re)builder: use it to materialize a log from a
    /// known leaf set or to repair one.  The incremental
    /// [`Self::append_leaf_hashes`] is the hot path for live commits and
    /// produces byte-identical output.  Full tiles already present on disk are
    /// left untouched (they are immutable); the rightmost partial tile at each
    /// level is rewritten atomically.  The checkpoint rename is the log's commit
    /// point.
    ///
    /// # Errors
    ///
    /// Returns any I/O error from writing a tile or the checkpoint.
    pub fn materialize(&self, leaves: &[LogHash]) -> io::Result<Checkpoint> {
        let t = leaves.len() as u64;
        let root = if t == 0 { empty_root() } else { mth(leaves) };

        // `t` is loop-invariant; the walk terminates when a level has no
        // complete subtree (`available == 0`).  Skip entirely for the empty log.
        let mut level: u8 = 0;
        if t > 0 {
            loop {
                let shift = TILE_HEIGHT_BITS * u32::from(level);
                let sub: u64 = if shift >= 64 { u64::MAX } else { 1u64 << shift };
                let available = if sub > t { 0 } else { t / sub };
                if available == 0 {
                    break;
                }
                let num_tiles = available.div_ceil(TILE_WIDTH);
                for tile_n in 0..num_tiles {
                    let first = tile_n * TILE_WIDTH;
                    let width = std::cmp::min(TILE_WIDTH, available - first);
                    self.write_tile(level, tile_n, first, width, sub, leaves)?;
                }
                level += 1;
            }
        }
        let checkpoint = Checkpoint::new(self.origin.clone(), t, root);
        checkpoint.write(&self.dir)?;
        checkpoint.append_to_history(&self.dir)?;
        Ok(checkpoint)
    }

    /// Compute the hashes for one tile and write it (skipping existing full
    /// tiles, which are immutable).
    fn write_tile(
        &self,
        level: u8,
        tile_n: u64,
        first_node: u64,
        width: u64,
        sub: u64,
        leaves: &[LogHash],
    ) -> io::Result<()> {
        let full = width == TILE_WIDTH;
        let target = self.dir.join(data_tile_rel(level, tile_n, width as usize));

        // A completed full tile is immutable: never recompute or rewrite it.
        if full && target.exists() {
            return Ok(());
        }

        let mut buf = Vec::with_capacity(width as usize * HASH_LEN);
        for i in 0..width {
            let node = first_node + i;
            let start = (node * sub) as usize;
            let end = start + sub as usize;
            buf.extend_from_slice(mth(&leaves[start..end]).as_bytes());
        }
        // Write the current representation FIRST, then drop superseded partial
        // variants of this tile index.  Deleting only after the replacement is
        // durable keeps at least one width >= the committed size on disk at
        // every instant, so a crash here never strands the committed prefix.
        write_atomic(&target, &buf)?;
        let keep = if full { None } else { Some(width as usize) };
        prune_partial_variants(&self.dir, level, tile_n, keep)
    }

    /// Read `count` hashes from tile `(level, n)` into `out`.
    fn read_tile_hashes(
        &self,
        level: u8,
        n: u64,
        count: usize,
        out: &mut Vec<LogHash>,
    ) -> Result<(), super::CheckpointError> {
        let bytes = read_tile_at_least(&self.dir, level, n, count)?;
        if bytes.len() < count * HASH_LEN {
            return Err(super::CheckpointError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "tile (level {level}, index {n}) holds {} bytes, expected at least {}",
                    bytes.len(),
                    count * HASH_LEN
                ),
            )));
        }
        for chunk in bytes.as_chunks::<HASH_LEN>().0.iter().take(count) {
            out.push(LogHash::from_bytes(*chunk));
        }
        Ok(())
    }
}

/// The C2SP `tlog-tiles` index-path encoding of `n`: base-1000 groups, the most
/// significant groups prefixed with `x` and the last group zero-padded to three
/// digits (e.g. `567`, `x001/x234/567`).
#[must_use]
pub fn n_path(mut n: u64) -> String {
    let mut s = format!("{:03}", n % 1000);
    n /= 1000;
    while n > 0 {
        s = format!("x{:03}/{s}", n % 1000);
        n /= 1000;
    }
    s
}

/// The path of a data tile relative to the log directory: `tile/<L>/<N>` for a
/// full tile, `tile/<L>/<N>.p/<W>` for a partial one.
#[must_use]
pub fn data_tile_rel(level: u8, n: u64, width: usize) -> String {
    let base = format!("tile/{level}/{}", n_path(n));
    if width as u64 == TILE_WIDTH {
        base
    } else {
        format!("{base}.p/{width}")
    }
}

/// Atomically write `bytes` to `path`: create parent directories, write a
/// sibling temp file, then rename it into place so a crash never leaves a
/// half-written tile or checkpoint.
///
/// # Errors
///
/// Returns any I/O error from directory creation, writing, or renaming.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = match path.file_name() {
        Some(name) => {
            let mut n = name.to_os_string();
            n.push(".tmp");
            path.with_file_name(n)
        }
        None => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot write to a path with no file name",
            ));
        }
    };
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)
}

/// Read the bytes of tile `(level, n)`, resolving whichever on-disk
/// representation holds at least `count` hashes.
///
/// Every stored tile entry is a completed-subtree hash (a leaf hash at level 0),
/// so a tile of width `w' >= count` shares the same first `count` hashes as the
/// width-`count` representation.  Resolving the widest-sufficient tile therefore
/// lets a reader tolerate a rightmost partial tile that a crash left *wider*
/// than the committed checkpoint records: `flush`/`write_tile` write the new,
/// wider representation before deleting the superseded narrower one, so at every
/// instant at least one representation with width `>= committed_size` exists,
/// even if a crash interrupts the delete or precedes the checkpoint rename.
///
/// Preference order (all correct; ordered for the fewest bytes read in the
/// steady state): the immutable full tile, then the exact-width partial (the
/// steady-state name), then the narrowest partial variant wide enough.
///
/// # Errors
///
/// Returns `NotFound` if no representation with `>= count` hashes exists, or any
/// other I/O error encountered while reading a candidate.
fn read_tile_at_least(dir: &Path, level: u8, n: u64, count: usize) -> io::Result<Vec<u8>> {
    let need = count * HASH_LEN;

    // A full tile is immutable and 256 wide, so it satisfies any request.
    let full = dir.join(data_tile_rel(level, n, TILE_WIDTH as usize));
    match std::fs::read(&full) {
        Ok(bytes) if bytes.len() >= need => return Ok(bytes),
        Ok(_) => {}
        Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
        Err(_) => {}
    }

    // The exact-width partial: the canonical steady-state name.
    if count > 0 && (count as u64) < TILE_WIDTH {
        let exact = dir.join(data_tile_rel(level, n, count));
        match std::fs::read(&exact) {
            Ok(bytes) if bytes.len() >= need => return Ok(bytes),
            Ok(_) => {}
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
            Err(_) => {}
        }
    }

    // Otherwise scan partial variants for the narrowest width >= count (a
    // crash-left wider partial, superseding an interrupted delete).
    let pdir = dir.join(format!("tile/{level}/{}.p", n_path(n)));
    let mut best: Option<(usize, PathBuf)> = None;
    if let Ok(entries) = std::fs::read_dir(&pdir) {
        for entry in entries.flatten() {
            let Some(width) = entry
                .file_name()
                .to_str()
                .and_then(|s| s.parse::<usize>().ok())
            else {
                continue;
            };
            if width >= count && best.as_ref().is_none_or(|(bw, _)| width < *bw) {
                best = Some((width, entry.path()));
            }
        }
    }
    if let Some((_, path)) = best {
        let bytes = std::fs::read(&path)?;
        if bytes.len() >= need {
            return Ok(bytes);
        }
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!(
            "no tile representation for level {level} index {n} holding >= {count} hashes under {}",
            dir.display()
        ),
    ))
}

/// Delete partial variants (`tile/<L>/<N>.p/<W>`) of tile `(level, n)` other
/// than `keep`.  Called *after* the replacement tile is durably written, so a
/// crash mid-prune only ever leaves a superset of the representations needed to
/// read the committed size (readers pick the widest-sufficient via
/// [`read_tile_at_least`]).  Passing `keep = None` removes every partial variant
/// (used when the tile has just been completed as a full tile).
fn prune_partial_variants(dir: &Path, level: u8, n: u64, keep: Option<usize>) -> io::Result<()> {
    let pdir = dir.join(format!("tile/{level}/{}.p", n_path(n)));
    let entries = match std::fs::read_dir(&pdir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for entry in entries.flatten() {
        let width = entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<usize>().ok());
        if width == keep {
            continue;
        }
        let path = entry.path();
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// The number of stored-hash tile levels tracked (`256^9 == 2^72` leaves, far
/// beyond any realistic log).  Tile level `L` holds hashes at tree level `8L`.
const MAX_TILE_LEVELS: usize = 9;

/// An in-progress incremental update to a tile log.
///
/// A session buffers the tiles it reads or writes, folds each completed subtree
/// up the tree exactly once, and flushes the touched tiles at the end.  Its
/// invariant mirrors the C2SP layout: the stored hash at tree level `8(L+1)`,
/// index `m` is the perfect fold of the 256 hashes in tile `(L, m)`, so filling
/// a tile at level `L` produces exactly one stored hash at level `L + 1`.
struct TileSession {
    dir: PathBuf,
    /// Cached tile buffers, keyed by (tile level, tile index); each is a
    /// contiguous run of 32-byte hashes.
    tiles: HashMap<(u8, u64), Vec<u8>>,
    /// Tiles modified this session, to be written on [`Self::flush`].
    dirty: HashSet<(u8, u64)>,
    /// Committed stored-hash count per level at open time (`old >> 8L`), used to
    /// bound reads of pre-existing tiles.
    count_open: [u64; MAX_TILE_LEVELS],
    /// Current stored-hash count per level, used to size tiles on flush.
    count: [u64; MAX_TILE_LEVELS],
}

impl TileSession {
    /// Open a session over `dir` for a log currently holding `old` leaves.
    fn open(dir: PathBuf, old: u64) -> Self {
        let mut count_open = [0u64; MAX_TILE_LEVELS];
        for (level, slot) in count_open.iter_mut().enumerate() {
            let shift = TILE_HEIGHT_BITS * level as u32;
            *slot = if shift >= 64 { 0 } else { old >> shift };
        }
        Self {
            dir,
            tiles: HashMap::new(),
            dirty: HashSet::new(),
            count_open,
            count: count_open,
        }
    }

    /// Append one leaf at absolute index `index`, carrying any completed
    /// subtrees up the tree.
    fn push_leaf(&mut self, index: u64, leaf: LogHash) -> io::Result<()> {
        self.set_stored(0, index, leaf)?;
        if index % TILE_WIDTH == TILE_WIDTH - 1 {
            self.cascade(0, index / TILE_WIDTH)?;
        }
        Ok(())
    }

    /// A tile `(level, tile_index)` just filled: fold it into one stored hash at
    /// the next level, then recurse if that fill completes the next tile too.
    fn cascade(&mut self, level: u8, tile_index: u64) -> io::Result<()> {
        let next_level = level + 1;
        if next_level as usize >= MAX_TILE_LEVELS {
            return Ok(());
        }
        let folded = fold_perfect(&self.full_tile_hashes(level, tile_index)?);
        // The fold of tile (level, tile_index) is the stored hash at
        // next_level whose stored-index equals tile_index.
        self.set_stored(next_level, tile_index, folded)?;
        if tile_index % TILE_WIDTH == TILE_WIDTH - 1 {
            self.cascade(next_level, tile_index / TILE_WIDTH)?;
        }
        Ok(())
    }

    /// Set the stored hash at `(level, index)`, loading the backing tile if this
    /// is its first touch and marking it dirty.
    fn set_stored(&mut self, level: u8, index: u64, hash: LogHash) -> io::Result<()> {
        let tile_index = index / TILE_WIDTH;
        self.ensure_loaded(level, tile_index)?;
        let pos = (index % TILE_WIDTH) as usize;
        let buf = self.tiles.get_mut(&(level, tile_index)).expect("loaded");
        if buf.len() < (pos + 1) * HASH_LEN {
            buf.resize((pos + 1) * HASH_LEN, 0);
        }
        buf[pos * HASH_LEN..(pos + 1) * HASH_LEN].copy_from_slice(hash.as_bytes());
        let _ = self.dirty.insert((level, tile_index));
        let slot = &mut self.count[level as usize];
        if index + 1 > *slot {
            *slot = index + 1;
        }
        Ok(())
    }

    /// Read the stored hash at `(level, index)` from the cache, loading the
    /// backing tile from disk if necessary.
    fn read_stored(&mut self, level: u8, index: u64) -> io::Result<LogHash> {
        let tile_index = index / TILE_WIDTH;
        self.ensure_loaded(level, tile_index)?;
        let buf = &self.tiles[&(level, tile_index)];
        let pos = (index % TILE_WIDTH) as usize;
        let mut arr = [0u8; HASH_LEN];
        arr.copy_from_slice(&buf[pos * HASH_LEN..(pos + 1) * HASH_LEN]);
        Ok(LogHash::from_bytes(arr))
    }

    /// Ensure tile `(level, tile_index)` is cached, loading its pre-existing
    /// (committed) hashes from disk on first touch.
    fn ensure_loaded(&mut self, level: u8, tile_index: u64) -> io::Result<()> {
        if self.tiles.contains_key(&(level, tile_index)) {
            return Ok(());
        }
        let available = self.count_open[level as usize].saturating_sub(tile_index * TILE_WIDTH);
        let width = std::cmp::min(TILE_WIDTH, available) as usize;
        let mut bytes = Vec::new();
        if width > 0 {
            let disk = read_tile_at_least(&self.dir, level, tile_index, width)?;
            if disk.len() < width * HASH_LEN {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "tile (level {level}, index {tile_index}) holds {} bytes, expected at least {}",
                        disk.len(),
                        width * HASH_LEN
                    ),
                ));
            }
            bytes.extend_from_slice(&disk[..width * HASH_LEN]);
        }
        let _ = self.tiles.insert((level, tile_index), bytes);
        Ok(())
    }

    /// The 256 hashes of a full tile `(level, tile_index)`, from the cache when
    /// available (the common case after a fill) or loaded from disk.
    fn full_tile_hashes(&mut self, level: u8, tile_index: u64) -> io::Result<Vec<LogHash>> {
        if !self.tiles.contains_key(&(level, tile_index)) {
            let path = self
                .dir
                .join(data_tile_rel(level, tile_index, TILE_WIDTH as usize));
            let disk = std::fs::read(&path)?;
            let _ = self.tiles.insert((level, tile_index), disk);
        }
        let buf = &self.tiles[&(level, tile_index)];
        if buf.len() < TILE_WIDTH as usize * HASH_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "expected a full tile at level {level} index {tile_index}, got {} bytes",
                    buf.len()
                ),
            ));
        }
        Ok(bytes_to_hashes(&buf[..TILE_WIDTH as usize * HASH_LEN]))
    }

    /// The RFC 6962 tree head over the first `n` leaves, folding the frontier of
    /// perfect subtrees right-to-left (`MTH = hash(left, hash(..., right))`).
    fn tree_head(&mut self, n: u64) -> io::Result<LogHash> {
        let mut subtrees = Vec::new();
        let mut pos = 0u64;
        let mut remaining = n;
        for tree_level in (0..64u32).rev() {
            let size = 1u64 << tree_level;
            if remaining & size != 0 {
                let idx = pos >> tree_level;
                subtrees.push(self.perfect_hash(tree_level, idx)?);
                pos += size;
                remaining ^= size;
            }
        }
        let mut acc = *subtrees.last().expect("n > 0 has at least one subtree");
        for left in subtrees.iter().rev().skip(1) {
            acc = hash_children(left, &acc);
        }
        Ok(acc)
    }

    /// The hash of the perfect subtree at tree level `tree_level`, index `idx`.
    ///
    /// When `tree_level` is a tile boundary (`8L`) the hash is a stored hash read
    /// straight from tile level `L`.  Otherwise it is folded from the
    /// `2^(tree_level % 8)` stored hashes one tile level below.
    fn perfect_hash(&mut self, tree_level: u32, idx: u64) -> io::Result<LogHash> {
        let tile_level = (tree_level / TILE_HEIGHT_BITS) as u8;
        let j = tree_level % TILE_HEIGHT_BITS;
        if j == 0 {
            return self.read_stored(tile_level, idx);
        }
        let base = idx << j;
        let mut children = Vec::with_capacity(1usize << j);
        for t in 0..(1u64 << j) {
            children.push(self.read_stored(tile_level, base + t)?);
        }
        Ok(fold_perfect(&children))
    }

    /// Write every dirty tile at its current width, then drop superseded partial
    /// variants.  Full tiles are written once and never revisited.
    ///
    /// The write-then-prune order is the log's crash-safety hinge: the new
    /// (wider) representation is made durable before the narrower one it
    /// supersedes is removed, and pruning happens before the checkpoint advances
    /// the committed size.  At every instant on disk there is therefore a tile
    /// of width `>= committed_size`, so a crash between flush and the checkpoint
    /// rename never strands the committed prefix (readers resolve the
    /// widest-sufficient tile via [`read_tile_at_least`]).
    fn flush(&self) -> io::Result<()> {
        let mut keys: Vec<(u8, u64)> = self.dirty.iter().copied().collect();
        keys.sort_unstable();
        for (level, tile_index) in keys {
            let available = self.count[level as usize].saturating_sub(tile_index * TILE_WIDTH);
            let width = std::cmp::min(TILE_WIDTH, available) as usize;
            let buf = &self.tiles[&(level, tile_index)];

            let path = self.dir.join(data_tile_rel(level, tile_index, width));
            write_atomic(&path, &buf[..width * HASH_LEN])?;
            let keep = if width as u64 == TILE_WIDTH {
                None
            } else {
                Some(width)
            };
            prune_partial_variants(&self.dir, level, tile_index, keep)?;
        }
        Ok(())
    }
}

/// Fold a power-of-two run of hashes into one via pairwise `hash_children`
/// (the RFC 6962 hash of a perfect subtree).
fn fold_perfect(hashes: &[LogHash]) -> LogHash {
    debug_assert!(hashes.len().is_power_of_two());
    let mut level = hashes.to_vec();
    while level.len() > 1 {
        level = level
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| hash_children(&pair[0], &pair[1]))
            .collect();
    }
    level[0]
}

/// Split a byte run into 32-byte log hashes.
fn bytes_to_hashes(bytes: &[u8]) -> Vec<LogHash> {
    bytes
        .as_chunks::<HASH_LEN>()
        .0
        .iter()
        .copied()
        .map(LogHash::from_bytes)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tlog::{TransparencyLog, verify_inclusion};

    #[test]
    fn n_path_groups_in_base_1000() {
        assert_eq!(n_path(0), "000");
        assert_eq!(n_path(7), "007");
        assert_eq!(n_path(999), "999");
        assert_eq!(n_path(1000), "x001/000");
        assert_eq!(n_path(1234), "x001/234");
        assert_eq!(n_path(1_234_567), "x001/x234/567");
    }

    #[test]
    fn data_tile_rel_marks_partial_tiles() {
        assert_eq!(data_tile_rel(0, 0, 256), "tile/0/000");
        assert_eq!(data_tile_rel(0, 1, 14), "tile/0/001.p/14");
        assert_eq!(data_tile_rel(1, 0, 1), "tile/1/000.p/1");
    }

    fn leaves(n: usize) -> Vec<LogHash> {
        (0..n)
            .map(|i| hash_leaf(format!("leaf-{i}").as_bytes()))
            .collect()
    }

    #[test]
    fn checkpoint_root_matches_reference_for_many_sizes() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Cover empty, sub-tile, exact-tile, and multi-tile-level sizes.
        for size in [0usize, 1, 2, 3, 255, 256, 257, 512, 600, 1000] {
            let sub = dir.path().join(format!("log-{size}"));
            let log = TileLog::new(&sub, "watertown/test");
            let ls = leaves(size);
            let cp = log.materialize(&ls).expect("materialize");

            let mut reference = TransparencyLog::new();
            for l in &ls {
                let _ = reference.append_leaf_hash(*l);
            }
            assert_eq!(cp.size, size as u64, "size for {size}");
            assert_eq!(cp.root, reference.root(), "root for {size}");
        }
    }

    #[test]
    fn leaves_round_trip_through_tiles() {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = TileLog::new(dir.path(), "watertown/test");
        let ls = leaves(600);
        let _ = log.materialize(&ls).expect("materialize");
        let loaded = log.load_leaves().expect("load");
        assert_eq!(loaded, ls);
    }

    #[test]
    fn incremental_append_equals_batch_and_full_tiles_are_stable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let inc = TileLog::new(dir.path().join("inc"), "watertown/test");

        // Fill exactly one full level-0 tile, then verify that appending more
        // leaves does not disturb the now-immutable full tile bytes.
        let _ = inc
            .append_leaf_data((0..256).map(|i| format!("leaf-{i}")))
            .unwrap();
        let full_tile = inc.dir().join(data_tile_rel(0, 0, 256));
        let full_before = std::fs::read(&full_tile).expect("read full tile");

        let mut last = None;
        for i in 256..300 {
            last = Some(inc.append_leaf_data([format!("leaf-{i}")]).unwrap());
        }
        let cp = last.expect("appended");
        assert_eq!(cp.size, 300);

        let full_after = std::fs::read(&full_tile).expect("read full tile");
        assert_eq!(full_before, full_after, "full tile must be immutable");

        // The full-batch materialization of the same 300 leaves agrees.
        let batch = TileLog::new(dir.path().join("batch"), "watertown/test");
        let batch_cp = batch.materialize(&leaves(300)).expect("materialize");
        assert_eq!(cp.root, batch_cp.root);
    }

    /// Collect every tile file under `dir/tile` keyed by its relative path so two
    /// logs can be compared byte-for-byte.
    fn tile_snapshot(dir: &Path) -> std::collections::BTreeMap<String, Vec<u8>> {
        let mut out = std::collections::BTreeMap::new();
        let root = dir.join("tile");
        let mut stack = vec![root.clone()];
        while let Some(d) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&d) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    let rel = path
                        .strip_prefix(&root)
                        .expect("under tile root")
                        .to_string_lossy()
                        .into_owned();
                    let _ = out.insert(rel, std::fs::read(&path).expect("read tile"));
                }
            }
        }
        out
    }

    #[test]
    fn incremental_is_byte_identical_to_materialize_across_splits() {
        let base = tempfile::tempdir().expect("tempdir");
        // Append the same leaves in several unequal chunks and require the tiles
        // and checkpoint to match a single whole-tree materialization exactly.
        let split_points = [1usize, 2, 3, 255, 256, 257, 511, 512, 513, 700, 1000];
        for &size in &split_points {
            let ls = leaves(size);

            let oracle = TileLog::new(base.path().join(format!("oracle-{size}")), "watertown/test");
            let oracle_cp = oracle.materialize(&ls).expect("materialize");

            let inc = TileLog::new(base.path().join(format!("inc-{size}")), "watertown/test");
            // Deterministic but uneven chunk sizes exercise partial-tile carries.
            let mut i = 0;
            let mut step = 1usize;
            while i < size {
                let end = std::cmp::min(size, i + step);
                let cp = inc
                    .append_leaf_hashes(ls[i..end].iter().copied())
                    .expect("append");
                assert_eq!(cp.size, end as u64, "size after chunk for {size}");
                i = end;
                step = step * 2 + 1;
            }

            let inc_cp = Checkpoint::read(inc.dir())
                .expect("read checkpoint")
                .expect("checkpoint present");
            assert_eq!(inc_cp.size, oracle_cp.size, "final size for {size}");
            assert_eq!(inc_cp.root, oracle_cp.root, "final root for {size}");
            assert_eq!(
                tile_snapshot(inc.dir()),
                tile_snapshot(oracle.dir()),
                "tile bytes must match materialize for {size}"
            );
        }
    }

    #[test]
    fn tile_leaves_prove_inclusion_against_checkpoint() {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = TileLog::new(dir.path(), "watertown/test");
        let ls = leaves(257);
        let cp = log.materialize(&ls).expect("materialize");

        // Rebuild proofs from the tile-stored leaves and verify against the
        // published checkpoint root.
        let loaded = log.load_leaves().expect("load");
        let mut reference = TransparencyLog::new();
        for l in &loaded {
            let _ = reference.append_leaf_hash(*l);
        }
        for (i, l) in loaded.iter().enumerate() {
            let proof = reference.inclusion_proof(i).expect("proof");
            assert!(
                verify_inclusion(l, i, loaded.len(), &proof, &cp.root),
                "leaf {i} should prove against checkpoint root"
            );
        }
    }

    /// Overwrite the rightmost level-0 partial tile of `dir` so that only a
    /// representation of width `wider` remains, while the checkpoint keeps
    /// recording `committed` (< `wider`).  This reproduces the on-disk state
    /// after a crash between `flush` (which writes the wider partial and prunes
    /// the narrower one) and the checkpoint rename.
    fn splice_wider_rightmost_partial(dir: &Path, committed: usize, wider: usize) {
        assert!(committed < wider && wider < TILE_WIDTH as usize);
        // Correct bytes for the width-`wider` tile come from a clean log of that
        // size; its first `committed` hashes equal the committed representation.
        let src = tempfile::tempdir().expect("tempdir");
        let wide_log = TileLog::new(src.path(), "watertown/test");
        let _ = wide_log
            .materialize(&leaves(wider))
            .expect("materialize wide");
        let wide_bytes =
            std::fs::read(src.path().join(data_tile_rel(0, 0, wider))).expect("read wide tile");

        let pdir = dir.join("tile/0/000.p");
        // Drop the committed-width partial (as a completed prune would) and drop
        // any other variants, then drop in only the wider one.
        if pdir.exists() {
            std::fs::remove_dir_all(&pdir).expect("clear partial dir");
        }
        std::fs::create_dir_all(&pdir).expect("recreate partial dir");
        std::fs::write(pdir.join(wider.to_string()), &wide_bytes).expect("write wide partial");

        // The checkpoint still records the committed size.
        let cp = Checkpoint::read(dir)
            .expect("read checkpoint")
            .expect("checkpoint present");
        assert_eq!(cp.size, committed as u64, "committed size precondition");
    }

    #[test]
    fn crash_left_wider_partial_stays_readable_and_appendable() {
        // A crash between the tile flush and the checkpoint rename can leave the
        // rightmost partial tile WIDER than the checkpoint records.  The
        // committed prefix must stay readable and the log must stay appendable.
        let dir = tempfile::tempdir().expect("tempdir");
        let log = TileLog::new(dir.path(), "watertown/test");
        let committed = 100usize;
        let _ = log.materialize(&leaves(committed)).expect("materialize");

        // Simulate the crash window: on-disk rightmost partial is width 150, the
        // checkpoint still says 100, and the width-100 partial is gone.
        splice_wider_rightmost_partial(dir.path(), committed, 150);

        // The committed prefix is still readable (reader resolves the wider tile
        // and truncates); before the fix this errored NotFound.
        assert_eq!(log.size().expect("size"), committed as u64);
        let loaded = log
            .load_leaves()
            .expect("load committed leaves after crash");
        assert_eq!(
            loaded,
            leaves(committed),
            "committed leaves survive the crash"
        );

        // And the log is still appendable: growing it lands a checkpoint whose
        // root matches a clean whole-tree materialization of the same leaves.
        let target = 300usize;
        let cp = log
            .append_leaf_data((committed..target).map(|i| format!("leaf-{i}")))
            .expect("append after crash");
        assert_eq!(cp.size, target as u64);
        let oracle = TileLog::new(dir.path().join("oracle"), "watertown/test");
        let oracle_cp = oracle.materialize(&leaves(target)).expect("materialize");
        assert_eq!(
            cp.root, oracle_cp.root,
            "post-crash append matches clean log"
        );
        assert_eq!(log.load_leaves().expect("reload"), leaves(target));
    }

    #[test]
    fn crash_left_full_tile_over_partial_checkpoint_stays_readable() {
        // The tile-completing variant: a crash after the rightmost partial was
        // promoted to a full immutable tile but before the checkpoint advanced.
        // A committed size within that tile must still read back.
        let dir = tempfile::tempdir().expect("tempdir");
        let log = TileLog::new(dir.path(), "watertown/test");
        let committed = 200usize;
        let _ = log.materialize(&leaves(committed)).expect("materialize");

        // Replace the width-200 partial with the completed full tile (256) that
        // a crash could have left, keeping the checkpoint at 200.
        let src = tempfile::tempdir().expect("tempdir");
        let full_log = TileLog::new(src.path(), "watertown/test");
        let _ = full_log
            .materialize(&leaves(256))
            .expect("materialize full");
        let full_bytes =
            std::fs::read(src.path().join(data_tile_rel(0, 0, 256))).expect("read full tile");
        std::fs::remove_dir_all(dir.path().join("tile/0/000.p")).expect("drop partial");
        std::fs::write(dir.path().join(data_tile_rel(0, 0, 256)), &full_bytes)
            .expect("write full tile");

        assert_eq!(log.size().expect("size"), committed as u64);
        assert_eq!(
            log.load_leaves().expect("load"),
            leaves(committed),
            "committed prefix reads back from the completed full tile"
        );
    }
}
