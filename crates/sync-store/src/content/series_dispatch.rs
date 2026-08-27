// SPDX-License-Identifier: Apache-2.0

//! Magic-dispatch over a fetched series object: `dp.series.1` (v1, a plain
//! ordered list of version blob hashes) versus `dp.series.2` (v2, a decoded
//! [`super::series_manifest::SeriesManifest`]).
//!
//! `docs/logical-series-identity-design.md` delivery gate 4 ("Add a dual
//! reader"). A `TreeEntry.child_hash` for a `FilePhysicalSeries` or
//! `TablePhysicalSeries` node names *either* object kind, and a consumer
//! cannot know which without fetching and inspecting it; this module is the
//! single place that inspection happens, so every caller (today just
//! `steward`'s content-graph fetch) dispatches identically rather than
//! re-implementing the magic check. Old callers that only ever understood
//! `dp.series.1` keep working unmodified: [`super::decode_series`] is
//! untouched, and this module is purely additive.
//!
//! No v1 reader may silently interpret v2 identity (design doc invariant 6):
//! this dispatch fails loudly on an unrecognized magic rather than guessing,
//! and successfully dispatching to [`FetchedSeriesObject::V2`] does not by
//! itself authorize any v1 code path to keep treating the object as a v1
//! series -- see `steward`'s explicit `SeriesV2` rejection in its
//! planning/apply path (v2 native materialization is a later delivery gate).

use super::series_manifest::{MANIFEST_MAGIC, SeriesManifest};
use super::tree::SERIES_MAGIC;
use super::{ObjectHash, decode_series};

/// The decoded, dispatched form of a fetched series object -- whichever of
/// the two known encodings its magic header named.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchedSeriesObject {
    /// A `dp.series.1` object: the ordered per-version blob hashes
    /// [`super::decode_series`] already returns.
    V1(Vec<ObjectHash>),
    /// A `dp.series.2` object: a decoded [`SeriesManifest`].
    V2(SeriesManifest),
}

/// Dispatch a fetched series object's raw bytes to its decoded form by
/// inspecting its magic header.
///
/// Bytes are checked against both known magics (`dp.series.1\n` and
/// `dp.series.2\n`) before any decode is attempted, so a caller gets one
/// precise error naming the exact expectation for whichever it is, rather
/// than a decoder's own truncation/tag error for the other encoding.
///
/// # Errors
///
/// - If `bytes` starts with the v1 magic, propagates
///   [`super::decode_series`]'s error (truncation, oversized count, trailing
///   bytes) with a `"dp.series.1"`-prefixed context.
/// - If `bytes` starts with the v2 magic, propagates
///   [`SeriesManifest::decode`]'s error similarly, prefixed `"dp.series.2"`.
/// - If `bytes` starts with neither known magic (including a magic-length
///   prefix that is merely truncated), returns one combined error precisely
///   naming both expected magics and the actual leading bytes found, so a
///   corrupt or entirely foreign object is never mistaken for a truncated
///   instance of either encoding.
pub fn decode_fetched_series_object(bytes: &[u8]) -> Result<FetchedSeriesObject, String> {
    if bytes.starts_with(SERIES_MAGIC) {
        return decode_series(bytes)
            .map(FetchedSeriesObject::V1)
            .map_err(|e| format!("dp.series.1 series object: {e}"));
    }
    if bytes.starts_with(MANIFEST_MAGIC) {
        return SeriesManifest::decode(bytes)
            .map(FetchedSeriesObject::V2)
            .map_err(|e| format!("dp.series.2 series object: {e}"));
    }
    let preview_len = bytes
        .len()
        .min(SERIES_MAGIC.len().max(MANIFEST_MAGIC.len()));
    Err(format!(
        "fetched series object matches neither known magic \
         ({SERIES_MAGIC:?} for dp.series.1 or {MANIFEST_MAGIC:?} for dp.series.2); \
         got {} byte(s) starting with {:?}",
        bytes.len(),
        &bytes[..preview_len]
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content::{PayloadKind, encode_series, merkle_root};

    fn h(s: &str) -> ObjectHash {
        ObjectHash::of_bytes(s.as_bytes())
    }

    #[test]
    fn dispatches_v1() {
        let versions = vec![h("a"), h("b")];
        let bytes = encode_series(&versions);
        match decode_fetched_series_object(&bytes).expect("decode v1") {
            FetchedSeriesObject::V1(got) => assert_eq!(got, versions),
            FetchedSeriesObject::V2(_) => panic!("expected V1"),
        }
    }

    #[test]
    fn dispatches_v2() {
        let leaves = vec![h("x"), h("y"), h("z")];
        let root = merkle_root(&leaves);
        let manifest = SeriesManifest::new(PayloadKind::File, None, 30, 3, None, None, None, root)
            .expect("valid manifest");
        let bytes = manifest.encode();
        match decode_fetched_series_object(&bytes).expect("decode v2") {
            FetchedSeriesObject::V2(got) => assert_eq!(got, manifest),
            FetchedSeriesObject::V1(_) => panic!("expected V2"),
        }
    }

    #[test]
    fn rejects_unknown_magic() {
        let bytes = b"dp.something-else.1\nrest of the bytes".to_vec();
        let err = decode_fetched_series_object(&bytes).expect_err("unknown magic must fail");
        assert!(err.contains("dp.series.1"), "error should name v1: {err}");
        assert!(err.contains("dp.series.2"), "error should name v2: {err}");
    }

    #[test]
    fn rejects_empty_bytes() {
        let err = decode_fetched_series_object(&[]).expect_err("empty bytes must fail");
        assert!(err.contains("dp.series.1"));
        assert!(err.contains("dp.series.2"));
    }

    #[test]
    fn propagates_v1_decode_error_with_context() {
        // Right magic, truncated body: a real dp.series.1 error, not the
        // combined unknown-magic error.
        let versions = vec![h("a"), h("b")];
        let bytes = encode_series(&versions);
        let truncated = &bytes[..bytes.len() - 4];
        let err = decode_fetched_series_object(truncated).expect_err("truncated v1 must fail");
        assert!(
            err.starts_with("dp.series.1"),
            "expected v1-tagged error, got: {err}"
        );
    }

    #[test]
    fn propagates_v2_decode_error_with_context() {
        let leaves = vec![h("x"), h("y")];
        let root = merkle_root(&leaves);
        let manifest = SeriesManifest::new(PayloadKind::File, None, 20, 2, None, None, None, root)
            .expect("valid manifest");
        let bytes = manifest.encode();
        let truncated = &bytes[..bytes.len() - 4];
        let err = decode_fetched_series_object(truncated).expect_err("truncated v2 must fail");
        assert!(
            err.starts_with("dp.series.2"),
            "expected v2-tagged error, got: {err}"
        );
    }
}
