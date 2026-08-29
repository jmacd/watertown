// SPDX-License-Identifier: Apache-2.0

//! Shared key layout for the pack-advertisement namespace
//! (`docs/logical-series-identity-design.md` delivery gate 3).
//!
//! A pack index is derived storage metadata excluded from the logical
//! content tree, so it cannot be discovered through the commit/tree object
//! closure the way an inline object or a `_blobs/blob=<hash>` physical blob
//! can. Every backend instead advertises pack indexes under one namespace,
//! keyed first by the `watertown.series.v1` series hash and then by the pack's own
//! content address:
//!
//! ```text
//! _packs/series=<64-hex series_hash>/pack=<64-hex pack_hash>
//! ```
//!
//! This module holds only the pure string formatting/parsing for that
//! layout -- no I/O -- so [`crate::content_remote::ContentRemote`] (an
//! `object_store` backend) and a `pond://` local source (a plain
//! filesystem) can each resolve the *same* relative path components against
//! their own root and agree on discovery byte-for-byte. Both directions are
//! strict: a name that does not parse back to exactly the value it was
//! built from is rejected rather than guessed at, since every segment here
//! is untrusted the moment it comes from a listing.

use crate::content::ObjectHash;

/// Top-level directory holding every series' pack advertisements.
pub const PACKS_ROOT: &str = "_packs";

/// The directory name for one series' pack advertisements: `series=<hex>`.
#[must_use]
pub fn series_dir_name(series_hash: ObjectHash) -> String {
    format!("series={}", series_hash.to_hex())
}

/// The file/key name for one pack advertisement: `pack=<hex>`.
#[must_use]
pub fn pack_file_name(pack_hash: ObjectHash) -> String {
    format!("pack={}", pack_hash.to_hex())
}

/// Parse a `series=<hex>` directory name back into its [`ObjectHash`].
///
/// # Errors
///
/// Returns an error if `name` does not start with `series=` or the
/// remainder is not a valid 64-character hex hash.
pub fn parse_series_dir_name(name: &str) -> Result<ObjectHash, String> {
    let hex = name
        .strip_prefix("series=")
        .ok_or_else(|| format!("not a pack-series directory: {name:?}"))?;
    ObjectHash::from_hex(hex).map_err(|e| format!("bad series hash in {name:?}: {e}"))
}

/// Parse a `pack=<hex>` file/key name back into its [`ObjectHash`].
///
/// # Errors
///
/// Returns an error if `name` does not start with `pack=` or the remainder
/// is not a valid 64-character hex hash.
pub fn parse_pack_file_name(name: &str) -> Result<ObjectHash, String> {
    let hex = name
        .strip_prefix("pack=")
        .ok_or_else(|| format!("not a pack advertisement file: {name:?}"))?;
    ObjectHash::from_hex(hex).map_err(|e| format!("bad pack hash in {name:?}: {e}"))
}

/// Suffix marking a versioned maintenance-layout marker sidecar: a small,
/// non-content-addressed file recording the bounded-layout parameters that
/// produced one specific `pack=<hex>` advertisement, kept deliberately
/// outside the pack's own logical identity/hash so its presence/absence can
/// never change a pack's content address. See
/// `steward::pack_maintenance`'s table-repack settlement check.
const LAYOUT_MARKER_SUFFIX: &str = ".layout";

/// The file/key name for one pack's maintenance-layout marker sidecar:
/// `pack=<hex>.layout`.
#[must_use]
pub fn layout_marker_file_name(pack_hash: ObjectHash) -> String {
    format!("{}{LAYOUT_MARKER_SUFFIX}", pack_file_name(pack_hash))
}

/// Parse a `pack=<hex>.layout` marker sidecar name back into the
/// [`ObjectHash`] of the pack advertisement it describes, or `None` if
/// `name` does not have the marker suffix at all (not a parse error: the
/// caller should fall through to [`parse_pack_file_name`] in that case).
///
/// # Errors
///
/// Returns an error if `name` has the marker suffix but the remainder does
/// not parse as `pack=<hex>`.
pub fn parse_layout_marker_file_name(name: &str) -> Result<Option<ObjectHash>, String> {
    let Some(inner) = name.strip_suffix(LAYOUT_MARKER_SUFFIX) else {
        return Ok(None);
    };
    parse_pack_file_name(inner).map(Some)
}

/// Suffix marking a pack advertisement (or, under `_packs/`, a whole
/// `series=<hex>` directory) as *superseded as of a previous maintenance
/// run*: a small, non-content-addressed sentinel that implements a
/// one-generation deletion grace period (`docs/logical-series-identity-design.md`'s
/// concurrent-reader-availability requirement).
///
/// A `pond://` reader ([`crate::content_source::LocalPondSource`] in
/// `steward`, informally referenced here since this module has no
/// dependency on it) that already listed an advertisement before a
/// maintenance run started must still be able to fetch it afterward: pack
/// maintenance therefore only *marks* a just-superseded advertisement stale
/// the first time it is seen no longer selected, and only actually deletes
/// it on a *later* run that finds it already marked stale -- so any one
/// generation's worth of superseded advertisements survives one full
/// maintenance cycle, bounding growth to one extra generation rather than
/// letting it grow without bound.
const STALE_MARKER_SUFFIX: &str = ".stale";

/// The file/key name for one pack's (or, under `_packs/`, one series
/// directory's) stale-generation sentinel: `pack=<hex>.stale` (or
/// `series=<hex>.stale`, appended the same way by
/// `steward::pack_store::stale_series_marker_path`).
#[must_use]
pub fn stale_marker_file_name(pack_hash: ObjectHash) -> String {
    format!("{}{STALE_MARKER_SUFFIX}", pack_file_name(pack_hash))
}

/// Parse a `pack=<hex>.stale` sentinel name back into the [`ObjectHash`] of
/// the pack advertisement it marks, or `None` if `name` does not have the
/// stale suffix at all (not a parse error: the caller should fall through
/// to [`parse_pack_file_name`]/[`parse_layout_marker_file_name`] in that
/// case).
///
/// # Errors
///
/// Returns an error if `name` has the stale suffix but the remainder does
/// not parse as `pack=<hex>`.
pub fn parse_stale_marker_file_name(name: &str) -> Result<Option<ObjectHash>, String> {
    let Some(inner) = name.strip_suffix(STALE_MARKER_SUFFIX) else {
        return Ok(None);
    };
    parse_pack_file_name(inner).map(Some)
}

/// The stale-generation sentinel name for a whole `series=<hex>` directory
/// under `_packs/`: `series=<hex>.stale`.
#[must_use]
pub fn stale_series_marker_file_name(series_hash: ObjectHash) -> String {
    format!("{}{STALE_MARKER_SUFFIX}", series_dir_name(series_hash))
}

/// Parse a `series=<hex>.stale` sentinel name back into the [`ObjectHash`]
/// it marks, or `None` if `name` does not have the stale suffix.
///
/// # Errors
///
/// Returns an error if `name` has the stale suffix but the remainder does
/// not parse as `series=<hex>`.
pub fn parse_stale_series_marker_file_name(name: &str) -> Result<Option<ObjectHash>, String> {
    let Some(inner) = name.strip_suffix(STALE_MARKER_SUFFIX) else {
        return Ok(None);
    };
    parse_series_dir_name(inner).map(Some)
}

/// Filenames that are common, harmless filesystem/OS metadata a pack-series
/// directory may accumulate incidentally (e.g. a Finder window state file
/// dropped by browsing the pond's data directory in the Finder) -- never
/// written by this codebase, never content-addressed, and never able to
/// name or hide a pack advertisement or physical object. Listed by exact
/// name only (never a prefix/suffix match, which could be used to smuggle a
/// malicious or corrupt file past validation) so recognizing them can never
/// widen to accept anything else.
const IGNORABLE_METADATA_FILENAMES: &[&str] = &[".DS_Store", "Thumbs.db", "desktop.ini"];

/// True if `name` is a filename a pack-series (or pack-objects) directory
/// listing must silently ignore rather than reject: a crash-artifact temp
/// file (see the `write_atomic` callers in `steward::pack_store`, always
/// prefixed `.tmp-`) or one of [`IGNORABLE_METADATA_FILENAMES`].
///
/// Deliberately narrow: this must never be broadened to swallow a
/// malformed or unexpected name, since a caller doing GC or discovery relies
/// on every *other* name either parsing as a real advertisement/object or
/// failing loudly (`docs/logical-series-identity-design.md`'s
/// fail-safe-on-malformed-state rule).
#[must_use]
pub fn is_ignorable_directory_entry(name: &str) -> bool {
    name.starts_with(".tmp-") || IGNORABLE_METADATA_FILENAMES.contains(&name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(s: &str) -> ObjectHash {
        ObjectHash::of_bytes(s.as_bytes())
    }

    #[test]
    fn series_dir_name_round_trips() {
        let hash = h("some-series");
        let name = series_dir_name(hash);
        assert_eq!(parse_series_dir_name(&name).unwrap(), hash);
    }

    #[test]
    fn pack_file_name_round_trips() {
        let hash = h("some-pack");
        let name = pack_file_name(hash);
        assert_eq!(parse_pack_file_name(&name).unwrap(), hash);
    }

    #[test]
    fn parse_rejects_wrong_prefix() {
        assert!(parse_series_dir_name("pack=abc").is_err());
        assert!(parse_pack_file_name("series=abc").is_err());
        assert!(parse_series_dir_name("not-a-key").is_err());
    }

    #[test]
    fn parse_rejects_bad_hex() {
        assert!(parse_series_dir_name("series=nothex").is_err());
        assert!(parse_pack_file_name("pack=zz").is_err());
    }

    #[test]
    fn layout_marker_file_name_round_trips() {
        let hash = h("some-pack");
        let name = layout_marker_file_name(hash);
        assert_eq!(name, format!("{}.layout", pack_file_name(hash)));
        assert_eq!(parse_layout_marker_file_name(&name).unwrap(), Some(hash));
    }

    #[test]
    fn parse_layout_marker_file_name_is_none_for_a_plain_pack_file() {
        let hash = h("some-pack");
        let name = pack_file_name(hash);
        assert_eq!(parse_layout_marker_file_name(&name).unwrap(), None);
    }

    #[test]
    fn parse_layout_marker_file_name_rejects_bad_hex_with_the_suffix() {
        assert!(parse_layout_marker_file_name("pack=zz.layout").is_err());
    }

    #[test]
    fn is_ignorable_directory_entry_recognizes_known_metadata_and_temp_files() {
        assert!(is_ignorable_directory_entry(".DS_Store"));
        assert!(is_ignorable_directory_entry("Thumbs.db"));
        assert!(is_ignorable_directory_entry("desktop.ini"));
        assert!(is_ignorable_directory_entry(".tmp-1234-abcd"));
        assert!(!is_ignorable_directory_entry("pack=deadbeef"));
        assert!(!is_ignorable_directory_entry("suspicious-file"));
        assert!(!is_ignorable_directory_entry(".ds_store"));
    }
}
