// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the collapse survey -- the read-only half of version
//! collapse that `pond maintain --dry-run` reports.
//!
//! A preview is only worth having if it predicts the thing it previews, so the
//! property under test is agreement: the survey must name the same candidates
//! that [`steward::Ship::collapse_versions`] would act on, and must cost
//! nothing to ask. `Ship::collapse_versions` performs pack-only physical
//! maintenance (design doc, delivery gate 7 and the pack-maintenance
//! follow-up): it never rewrites/deletes Oplog rows, never changes
//! `dp.series.2` manifests/root/version, and instead publishes bounded
//! content-addressed physical packs. The coarse survey
//! (`survey_collapsible_series`) remains a pure, side-effect-free discovery
//! query regardless; the finer-grained `survey_pack_maintenance` is the one
//! that must agree candidate-for-candidate with what a real run repacks.

use steward::Ship;
use tempfile::tempdir;
use tlogfs::PondUserMetadata;
use tokio::io::AsyncWriteExt;

fn meta(label: &str) -> PondUserMetadata {
    PondUserMetadata::new(vec!["test".into(), label.into()])
}

/// Incompressible bytes, distinct per version so no two dedup to one blob.
fn chunk(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

async fn append_series_version(ship: &mut Ship, path: &str, bytes: &[u8]) {
    let path = path.to_string();
    let bytes = bytes.to_vec();
    ship.write_transaction(&meta("series"), async move |fs| {
        let root = fs.root().await?;
        let mut writer = root
            .async_writer_path_with_type(&path, tinyfs::EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(&bytes).await?;
        writer.shutdown().await?;
        Ok(())
    })
    .await
    .expect("series transaction");
}

const VERSIONS: usize = 12;
const VERSION_LEN: usize = 96 * 1024;

async fn pond_with_series() -> (tempfile::TempDir, Ship) {
    let tmp = tempdir().expect("tempdir");
    let mut ship = Ship::create_pond(tmp.path().join("pond"), "collapse-survey")
        .await
        .expect("create pond");
    for i in 0..VERSIONS {
        let bytes = chunk(i as u64 + 7, VERSION_LEN);
        append_series_version(&mut ship, "/events.series", &bytes).await;
    }
    (tmp, ship)
}

/// The survey reports the bytes collapse would rewrite, which is what the push
/// that follows has to carry.  A figure that did not match the content would be
/// worse than no figure at all: it would be a budget estimate someone trusted.
#[tokio::test]
async fn the_survey_reports_the_real_payload() {
    let (_t, mut ship) = pond_with_series().await;

    let found = ship.survey_collapsible_series(1).await.expect("survey");
    assert_eq!(found.len(), 1, "one series should qualify: {found:?}");

    let series = &found[0];
    assert_eq!(series.live_versions, VERSIONS);
    assert_eq!(
        series.total_bytes,
        (VERSIONS * VERSION_LEN) as u64,
        "reported payload must be the content collapse would merge"
    );
}

/// Asking must be free.  The survey runs under a read transaction that takes no
/// control records and consumes no sequence, so an operator (or a monitor) can
/// ask as often as they like without moving the pond.
#[tokio::test]
async fn asking_changes_nothing() {
    let (_t, mut ship) = pond_with_series().await;

    let seq_before = ship
        .control_table()
        .latest_spine_seq()
        .await
        .expect("seq before");

    let first = ship.survey_collapsible_series(1).await.expect("survey");
    let second = ship.survey_collapsible_series(1).await.expect("resurvey");

    let seq_after = ship
        .control_table()
        .latest_spine_seq()
        .await
        .expect("seq after");

    assert_eq!(seq_before, seq_after, "a survey must not advance the pond");
    assert_eq!(
        first.len(),
        second.len(),
        "a survey must be repeatable, not self-consuming"
    );
    assert_eq!(first[0].live_versions, second[0].live_versions);
    assert_eq!(first[0].total_bytes, second[0].total_bytes);
}

/// The survey must agree with what pack-only maintenance actually does:
/// `survey_pack_maintenance` (the dry-run-safe, no-mutation preview) must
/// name the same series, as [`steward::PackCandidateOutcome::NeedsRepack`],
/// that a real `Ship::collapse_versions` call goes on to repack in the same
/// process. A preview that named a different candidate set than what
/// actually got repacked would be misinformation, not caution -- and the
/// preview call itself must be provably inert (no pond state moves) whether
/// or not it agrees.
#[tokio::test]
async fn the_survey_agrees_with_what_collapse_repacks() {
    use steward::PackCandidateOutcome;

    let (_t, mut ship) = pond_with_series().await;

    let predicted = ship
        .survey_pack_maintenance(1)
        .await
        .expect("pack maintenance survey");
    let predicted_repacks: Vec<_> = predicted
        .iter()
        .filter(|c| c.outcome == PackCandidateOutcome::NeedsRepack)
        .collect();
    assert_eq!(
        predicted_repacks.len(),
        1,
        "one series should be predicted for repack: {predicted:?}"
    );

    let seq_before = ship
        .control_table()
        .latest_spine_seq()
        .await
        .expect("seq before");

    // The dry-run survey must not have moved anything before we run for
    // real.
    let seq_after_survey = ship
        .control_table()
        .latest_spine_seq()
        .await
        .expect("seq after survey");
    assert_eq!(
        seq_before, seq_after_survey,
        "a dry-run survey must not advance the pond"
    );

    let report = ship
        .collapse_versions(1)
        .await
        .expect("pack-only maintenance must succeed for a v2 pond with a real candidate");
    assert_eq!(
        report.series_repacked,
        predicted_repacks.len(),
        "the number of series actually repacked must match what the survey predicted"
    );
    assert_eq!(report.candidates, predicted.len());

    // A repeated survey now finds nothing left to repack: the bounded layout
    // just published already satisfies the same threshold.
    let resurvey = ship
        .survey_pack_maintenance(1)
        .await
        .expect("resurvey after real maintenance");
    assert!(
        resurvey
            .iter()
            .all(|c| c.outcome != PackCandidateOutcome::NeedsRepack),
        "a repeated survey must find nothing left to repack once maintenance has settled: \
         {resurvey:?}"
    );
}

/// A threshold nothing exceeds yields nothing -- so a quiet pond's dry run
/// reports "no work" rather than manufacturing candidates.
#[tokio::test]
async fn a_high_threshold_finds_nothing() {
    let (_t, mut ship) = pond_with_series().await;

    let found = ship
        .survey_collapsible_series(VERSIONS * 10)
        .await
        .expect("survey");
    assert!(found.is_empty(), "expected no candidates, got {found:?}");
}

/// The report has to name files, and it has to do so without instantiating
/// them.  Every real pond carries dynamic nodes under `/sys` whose config
/// expands from the environment; walking the live tree to collect names made
/// the preview fail on ponds it was written to inspect.  Paths come from stored
/// directory rows instead, so an unresolvable factory is just another name.
#[tokio::test]
async fn naming_files_does_not_instantiate_them() {
    let (_t, mut ship) = pond_with_series().await;

    ship.write_transaction(&meta("unresolvable"), async move |fs| {
        let root = fs.root().await?;
        _ = root.create_dir_path("/sys").await?;
        _ = root.create_dir_path("/sys/remotes").await?;
        _ = root
            .create_dynamic_path(
                "/sys/remotes/broken.yaml",
                tinyfs::EntryType::FileDynamic,
                "no-such-factory-is-registered",
                b"endpoint: ${MISSING_ENV_VAR_THAT_IS_NOT_SET}\n".to_vec(),
            )
            .await?;
        Ok(())
    })
    .await
    .expect("stage an unresolvable dynamic node");

    let seq_before = ship
        .control_table()
        .latest_spine_seq()
        .await
        .expect("seq before");

    let paths = ship.node_paths().await.expect(
        "naming must not depend on any factory resolving: that is what broke the first attempt",
    );

    let series = ship.survey_collapsible_series(1).await.expect("survey");
    assert_eq!(
        paths.get(&series[0].file_id).map(String::as_str),
        Some("/events.series"),
        "the survey's candidate must be nameable: {paths:?}"
    );
    assert!(
        paths.values().any(|p| p == "/sys/remotes/broken.yaml"),
        "the unresolvable node should be named, not skipped: {paths:?}"
    );

    // A preview that leaves an open transaction behind has modified the pond it
    // promised only to look at -- the control table would show it as INCOMPLETE.
    let seq_after = ship
        .control_table()
        .latest_spine_seq()
        .await
        .expect("seq after");
    assert_eq!(
        seq_before, seq_after,
        "naming nodes must not advance the pond"
    );
}
