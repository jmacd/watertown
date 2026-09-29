// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use datafusion::error::Result;
use query_foundation::partial::{
    AggregatePartial, PartialKey, PartialManifest, PartialPatchReason, PartialPatchWork,
    PartialResolution, PartialStateStore, PartialWork, RawRebuildReason,
};

fn manifest(bucket_count: usize) -> Result<PartialManifest> {
    let partials = (0..bucket_count)
        .map(|index| {
            (
                PartialKey::new(index as i64 * 10, ["site-a"]),
                AggregatePartial::empty()
                    .with_value(Some(index as f64))
                    .with_value(None),
            )
        })
        .collect();
    PartialManifest::try_new("recipe-1", "source-state-1", 10, 0, partials)
}

#[test]
fn aggregate_partials_merge_nulls_without_raw_rows() {
    let left = AggregatePartial::empty()
        .with_value(Some(2.0))
        .with_value(None);
    let right = AggregatePartial::empty()
        .with_value(Some(6.0))
        .with_value(Some(10.0));
    let merged = left.merge(right);
    assert_eq!(merged.rows(), 4);
    assert_eq!(merged.non_null(), 3);
    assert_eq!(merged.sum(), 18.0);
    assert_eq!(merged.min(), Some(2.0));
    assert_eq!(merged.max(), Some(10.0));
    assert_eq!(merged.mean(), Some(6.0));
}

#[test]
fn exact_no_change_hits_have_constant_zero_work_at_every_scale() -> Result<()> {
    for bucket_count in [1, 100, 1_000] {
        let mut store = PartialStateStore::default();
        store.publish(manifest(bucket_count)?);
        let resolution = store.resolve("recipe-1", "source-state-1", 10, 0)?;
        let PartialResolution::Exact {
            manifest: resolved,
            work,
        } = resolution
        else {
            panic!("exact manifest must be reused");
        };
        assert_eq!(resolved.partials().len(), bucket_count);
        assert_eq!(work, PartialWork::default());
    }

    Ok(())
}

#[test]
fn coarser_resolutions_fold_fine_partials_and_then_hit_exactly() -> Result<()> {
    let mut store = PartialStateStore::default();
    store.publish(manifest(1_000)?);

    let resolution = store.resolve("recipe-1", "source-state-1", 100, 0)?;
    let PartialResolution::Folded {
        source_width,
        manifest: coarse,
        work,
    } = resolution
    else {
        panic!("coarse state must fold from fine partials");
    };
    assert_eq!(source_width, 10);
    assert_eq!(coarse.partials().len(), 100);
    assert_eq!(work.raw_rows_scanned, 0);
    assert_eq!(work.partials_read, 1_000);
    assert_eq!(work.partials_written, 100);
    let first = coarse
        .partials()
        .get(&PartialKey::new(0, ["site-a"]))
        .expect("first coarse bucket must exist");
    assert_eq!(first.rows(), 20);
    assert_eq!(first.non_null(), 10);
    assert_eq!(first.sum(), 45.0);

    let exact = store.resolve("recipe-1", "source-state-1", 100, 0)?;
    assert!(matches!(
        exact,
        PartialResolution::Exact {
            work: PartialWork {
                raw_rows_scanned: 0,
                partials_read: 0,
                partials_written: 0,
            },
            ..
        }
    ));

    let another = store.resolve("recipe-1", "source-state-1", 50, 0)?;
    let PartialResolution::Folded {
        source_width, work, ..
    } = another
    else {
        panic!("another resolution must reuse fine partials");
    };
    assert_eq!(source_width, 10);
    assert_eq!(work.raw_rows_scanned, 0);
    assert_eq!(work.partials_read, 1_000);
    assert_eq!(work.partials_written, 200);

    Ok(())
}

#[test]
fn cache_misses_name_the_raw_rebuild_reason() -> Result<()> {
    let mut empty = PartialStateStore::default();
    assert_eq!(
        empty.resolve("recipe-1", "source-state-1", 10, 0)?,
        PartialResolution::NeedsRaw {
            reason: RawRebuildReason::MissingState
        }
    );

    let mut store = PartialStateStore::default();
    store.publish(manifest(10)?);
    assert_eq!(
        store.resolve("recipe-2", "source-state-1", 10, 0)?,
        PartialResolution::NeedsRaw {
            reason: RawRebuildReason::RecipeChanged
        }
    );
    assert_eq!(
        store.resolve("recipe-1", "source-state-2", 10, 0)?,
        PartialResolution::NeedsRaw {
            reason: RawRebuildReason::SourceStateChanged
        }
    );
    assert_eq!(
        store.resolve("recipe-1", "source-state-1", 15, 0)?,
        PartialResolution::NeedsRaw {
            reason: RawRebuildReason::IncompatibleResolution
        }
    );
    assert_eq!(
        store.resolve("recipe-1", "source-state-1", 20, 5)?,
        PartialResolution::NeedsRaw {
            reason: RawRebuildReason::IncompatibleResolution
        }
    );

    Ok(())
}

#[test]
fn manifests_validate_identity_resolution_and_bucket_alignment() {
    let error = PartialManifest::try_new(
        "",
        "source",
        10,
        0,
        BTreeMap::<PartialKey, AggregatePartial>::new(),
    )
    .expect_err("empty recipe identity must fail");
    assert!(error.to_string().contains("recipe identity"), "{error}");

    let error = PartialManifest::try_new(
        "recipe",
        "source",
        10,
        0,
        BTreeMap::from([(PartialKey::new(1, ["site-a"]), AggregatePartial::empty())]),
    )
    .expect_err("unaligned bucket must fail");
    assert!(error.to_string().contains("not aligned"), "{error}");
}

#[test]
fn one_bucket_append_work_is_constant_at_every_retained_history_scale() -> Result<()> {
    for bucket_count in [1, 100, 1_000] {
        let mut store = PartialStateStore::default();
        store.publish(manifest(bucket_count)?);
        let key = PartialKey::new(bucket_count as i64 * 10, ["site-a"]);
        let value = AggregatePartial::empty()
            .with_value(Some(bucket_count as f64))
            .with_value(None);
        let work = store.apply_patch(
            "recipe-1",
            "source-state-1",
            "source-state-2",
            10,
            BTreeMap::from([(key, Some(value))]),
            PartialPatchReason::Append,
            2,
        )?;
        assert_eq!(
            work,
            PartialPatchWork {
                reason: PartialPatchReason::Append,
                raw_rows_scanned: 2,
                buckets_touched: 1,
                buckets_upserted: 1,
                buckets_removed: 0,
                invalidated_resolutions: 0,
            }
        );
        let PartialResolution::Exact {
            manifest: updated,
            work,
        } = store.resolve("recipe-1", "source-state-2", 10, 0)?
        else {
            panic!("updated fine state must be exact");
        };
        assert_eq!(updated.partials().len(), bucket_count + 1);
        assert_eq!(work, PartialWork::default());
    }

    Ok(())
}

#[test]
fn dirty_patch_invalidates_stale_coarse_state_and_names_rebuild_reason() -> Result<()> {
    let mut store = PartialStateStore::default();
    store.publish(manifest(100)?);
    assert!(matches!(
        store.resolve("recipe-1", "source-state-1", 100, 0)?,
        PartialResolution::Folded { .. }
    ));

    let replacement = AggregatePartial::empty().with_value(Some(999.0));
    let work = store.apply_patch(
        "recipe-1",
        "source-state-1",
        "source-state-2",
        10,
        BTreeMap::from([(PartialKey::new(0, ["site-a"]), Some(replacement))]),
        PartialPatchReason::RetroactiveRepair,
        1,
    )?;
    assert_eq!(work.reason, PartialPatchReason::RetroactiveRepair);
    assert_eq!(work.buckets_touched, 1);
    assert_eq!(work.invalidated_resolutions, 1);

    let rebuilt = store.resolve("recipe-1", "source-state-2", 100, 0)?;
    let PartialResolution::Folded { manifest, work, .. } = rebuilt else {
        panic!("stale coarse state must be rebuilt from updated fine partials");
    };
    assert_eq!(work.raw_rows_scanned, 0);
    assert_eq!(work.partials_read, 100);
    assert_eq!(
        manifest
            .partials()
            .get(&PartialKey::new(0, ["site-a"]))
            .expect("coarse bucket must exist")
            .max(),
        Some(999.0)
    );

    Ok(())
}

#[test]
fn no_data_frontier_advance_retags_all_resolutions_with_zero_bucket_work() -> Result<()> {
    let mut store = PartialStateStore::default();
    store.publish(manifest(100)?);
    _ = store.resolve("recipe-1", "source-state-1", 100, 0)?;

    let work = store.apply_patch(
        "recipe-1",
        "source-state-1",
        "source-state-2",
        10,
        BTreeMap::new(),
        PartialPatchReason::Append,
        0,
    )?;
    assert_eq!(
        work,
        PartialPatchWork {
            reason: PartialPatchReason::Append,
            raw_rows_scanned: 0,
            buckets_touched: 0,
            buckets_upserted: 0,
            buckets_removed: 0,
            invalidated_resolutions: 0,
        }
    );
    assert!(matches!(
        store.resolve("recipe-1", "source-state-2", 10, 0)?,
        PartialResolution::Exact {
            work: PartialWork {
                raw_rows_scanned: 0,
                partials_read: 0,
                partials_written: 0,
            },
            ..
        }
    ));
    assert!(matches!(
        store.resolve("recipe-1", "source-state-2", 100, 0)?,
        PartialResolution::Exact {
            work: PartialWork {
                raw_rows_scanned: 0,
                partials_read: 0,
                partials_written: 0,
            },
            ..
        }
    ));

    Ok(())
}

#[test]
fn invalid_patch_identity_and_work_fail_explicitly() -> Result<()> {
    let mut store = PartialStateStore::default();
    store.publish(manifest(1)?);
    let error = store
        .apply_patch(
            "recipe-1",
            "source-state-1",
            "source-state-1",
            10,
            BTreeMap::new(),
            PartialPatchReason::Append,
            0,
        )
        .expect_err("patch must advance source identity");
    assert!(
        error.to_string().contains("must advance source-state"),
        "{error}"
    );

    let error = store
        .apply_patch(
            "recipe-1",
            "source-state-1",
            "source-state-2",
            10,
            BTreeMap::new(),
            PartialPatchReason::Append,
            1,
        )
        .expect_err("zero-bucket patch cannot scan rows");
    assert!(
        error.to_string().contains("must not report raw rows"),
        "{error}"
    );

    Ok(())
}
