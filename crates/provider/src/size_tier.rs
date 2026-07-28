//! Size-tiered merge policy, shared by every accumulating segment store.
//!
//! Two places in this codebase accumulate immutable, range-ordered segments and
//! must periodically merge them to keep their count bounded: tlogfs series
//! collapse, and the rollup cache's segments. The policy for choosing WHICH
//! segments to merge is identical in both, and depends on nothing but their
//! sizes -- so it lives here once and is called from both, rather than being
//! reimplemented alongside each store.
//!
//! That is not a stylistic preference. The archived rollup design instructed
//! that a sibling module be mirrored "exactly" from an existing one; the two
//! copies then drifted, and the resulting disagreements were the source of this
//! branch's double-counting bugs. A tiering policy written twice is the same
//! hazard: the copies are meant to agree, nothing forces them to, and the
//! symptom of divergence is quiet write amplification rather than an error.

use std::ops::Range;

/// How many adjacent same-class segments merge into one.
///
/// The value trades write volume against read cost. A larger fanout writes
/// slightly less but leaves up to `F - 1` live segments per class, and reads pay
/// per live segment. At `F = 10` a store holds at most a few dozen live segments
/// across all classes while keeping write amplification to a small constant.
pub const MERGE_FANOUT: usize = 10;

/// Size class of a segment, i.e. its magnitude in units of [`MERGE_FANOUT`].
///
/// Segments merge only with segments of the same class, which is what keeps a
/// large accumulated segment from being rewritten every time a few small ones
/// arrive.
#[must_use]
pub fn size_class(bytes: u64) -> u32 {
    let mut class = 0u32;
    let mut bound = MERGE_FANOUT as u64;
    while bytes >= bound {
        class += 1;
        match bound.checked_mul(MERGE_FANOUT as u64) {
            Some(next) => bound = next,
            None => break,
        }
    }
    class
}

/// Choose the contiguous window of segments to merge, or `None` for a no-op.
///
/// `sizes` are the on-disk byte counts of the live segments, ordered oldest
/// content first; the returned half-open range indexes into that slice.
///
/// Size-tiered policy: merge the oldest group of [`MERGE_FANOUT`] *adjacent*
/// segments sharing a size class. Merging only same-class neighbours is the
/// whole point -- it is what stops a 96 MB accumulated segment from being
/// rewritten to absorb 10 KB of new data.
///
/// `max_live` is a backstop for read cost. If no same-class group exists but the
/// store has drifted past `max_live` segments, [`MERGE_FANOUT`] adjacent
/// segments are merged regardless of class so the count stays bounded.
///
/// The backstop merges the CHEAPEST such window, not the oldest. Taking the
/// oldest looks natural but is the one choice that defeats tiering: after any
/// previous merge the oldest segment IS the large accumulated one, so a backstop
/// anchored at index 0 rewrites megabytes to absorb kilobytes -- precisely the
/// `O(N^2)` behaviour size classes exist to prevent. It also fires more often
/// than it appears to, because callers may pass the same number as both the
/// candidacy threshold and `max_live`, so every candidate satisfies
/// `sizes.len() > max_live` by construction. Choosing by total bytes bounds the
/// segment count just as well while keeping write volume at the minimum any
/// window could achieve.
#[must_use]
pub fn choose_merge_window(sizes: &[u64], max_live: usize) -> Option<Range<usize>> {
    if sizes.len() < 2 {
        return None;
    }

    // Oldest same-class group of at least MERGE_FANOUT adjacent segments.
    let classes: Vec<u32> = sizes.iter().copied().map(size_class).collect();
    let mut start = 0usize;
    while start < classes.len() {
        let mut end = start + 1;
        while end < classes.len() && classes[end] == classes[start] {
            end += 1;
        }
        if end - start >= MERGE_FANOUT {
            return Some(start..start + MERGE_FANOUT);
        }
        start = end;
    }

    // Backstop: bound live segment count even when classes are ragged. Every
    // window of this width reduces the count identically, so pick the one that
    // rewrites the fewest bytes; ties resolve to the oldest.
    if sizes.len() > max_live {
        let width = MERGE_FANOUT.min(sizes.len());
        let cost = |start: usize| -> u128 {
            sizes[start..start + width]
                .iter()
                .map(|&b| u128::from(b))
                .sum()
        };
        let best = (0..=sizes.len() - width).min_by_key(|&start| cost(start))?;
        return Some(best..best + width);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_class_counts_magnitudes_of_fanout() {
        assert_eq!(size_class(0), 0);
        assert_eq!(size_class(9), 0);
        assert_eq!(size_class(10), 1);
        assert_eq!(size_class(99), 1);
        assert_eq!(size_class(100), 2);
        // Must not overflow or spin on an enormous segment.
        assert!(size_class(u64::MAX) > 0);
    }

    #[test]
    fn no_window_below_two_segments() {
        assert_eq!(choose_merge_window(&[], 1), None);
        assert_eq!(choose_merge_window(&[5], 0), None);
    }

    #[test]
    fn merges_oldest_full_group_of_one_class() {
        // 12 same-class segments: the OLDEST ten merge, leaving the newest two.
        let sizes = vec![5u64; 12];
        assert_eq!(choose_merge_window(&sizes, 100), Some(0..10));
    }

    #[test]
    fn leaves_a_large_segment_alone_when_small_ones_arrive() {
        // One big accumulated segment followed by a few small ones: no group of
        // ten shares a class and the count is under the backstop, so nothing is
        // rewritten. Merging here would rewrite the large segment to absorb
        // almost nothing, which is the O(N^2) trap.
        let mut sizes = vec![10_000_000u64];
        sizes.extend(std::iter::repeat_n(5u64, 4));
        assert_eq!(choose_merge_window(&sizes, 100), None);
    }

    #[test]
    fn backstop_picks_the_cheapest_window_not_the_oldest() {
        // Ragged classes, over the backstop: the large accumulated segment sits
        // oldest. Anchoring at 0 would rewrite it to absorb kilobytes.
        let mut sizes = vec![10_000_000u64];
        sizes.extend((0..14).map(|i| 5 + i as u64));
        let w = choose_merge_window(&sizes, 10).expect("backstop must fire");
        assert!(
            w.start > 0,
            "backstop anchored at the large oldest segment: {w:?}"
        );
        // Cheapest ten of the small tail are the ten smallest, i.e. indices 1..11.
        assert_eq!(w, 1..11);
    }

    #[test]
    fn backstop_does_not_fire_while_under_max_live() {
        let sizes = vec![10_000_000u64, 5, 6, 7];
        assert_eq!(choose_merge_window(&sizes, 100), None);
    }
}
