// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use datafusion::error::Result;
use query_foundation::frontier::{RepairPolicy, SettledState};
use query_foundation::locality::{
    ChangeExtent, ChangeKind, ChangeSet, FrontierChange, LocalityClass, LocalityContract,
    LogicalChange,
};
use query_foundation::orchestration::{
    FullRebuildReason, ReductionDecision, ReductionRequest, plan_incremental_reduction,
};
use query_foundation::partial::{
    AggregatePartial, PartialKey, PartialManifest, PartialPatchReason, PartialResolution,
    PartialStateStore, PartialWork, RawRebuildReason,
};
use query_foundation::statistics::TimeInterval;

fn store() -> Result<PartialStateStore> {
    let manifest = PartialManifest::try_new(
        "recipe",
        "state-1",
        10,
        0,
        BTreeMap::from([(
            PartialKey::new(0, ["site"]),
            AggregatePartial::empty().with_value(Some(1.0)),
        )]),
    )?;
    let mut store = PartialStateStore::default();
    store.publish(manifest);
    Ok(store)
}

fn locality() -> Result<LocalityContract> {
    LocalityContract::try_new(
        LocalityClass::FixedWindow {
            width: 10,
            origin: 0,
        },
        ["source"],
    )
}

fn settled(policy: RepairPolicy) -> Result<SettledState> {
    SettledState::try_new(Some(100), Some(110), policy)
}

fn request<'a>(prior: &'a str, current: &'a str) -> ReductionRequest<'a> {
    ReductionRequest {
        recipe_id: "recipe",
        prior_source_state_id: prior,
        source_state_id: current,
        width: 10,
        origin: 0,
    }
}

fn change(kind: ChangeKind, extent: ChangeExtent) -> Result<LogicalChange> {
    LogicalChange::try_new(kind, "source", "member", extent)
}

#[test]
fn no_change_reuses_exact_state_with_zero_work() -> Result<()> {
    let mut store = store()?;
    let decision = plan_incremental_reduction(
        &mut store,
        &locality()?,
        settled(RepairPolicy::within(10)?)?,
        &ChangeSet::new(),
        request("state-1", "state-1"),
    )?;
    assert!(matches!(
        decision,
        ReductionDecision::Reuse {
            resolution: PartialResolution::Exact {
                work: PartialWork {
                    raw_rows_scanned: 0,
                    partials_read: 0,
                    partials_written: 0,
                },
                ..
            }
        }
    ));

    Ok(())
}

#[test]
fn no_data_frontier_advance_retags_state_without_bucket_work() -> Result<()> {
    let mut store = store()?;
    let changes = ChangeSet::new().with_frontier_change(FrontierChange::try_new(Some(100), 105)?);
    let decision = plan_incremental_reduction(
        &mut store,
        &locality()?,
        settled(RepairPolicy::within(10)?)?,
        &changes,
        request("state-1", "state-2"),
    )?;
    let ReductionDecision::Reuse { resolution } = decision else {
        panic!("frontier-only advance must reuse state");
    };
    let PartialResolution::Exact { manifest, work } = resolution else {
        panic!("retagged state must resolve exactly");
    };
    assert_eq!(manifest.source_state_id(), "state-2");
    assert_eq!(work, PartialWork::default());

    Ok(())
}

#[test]
fn append_disorder_repair_and_membership_choose_exact_dirty_windows() -> Result<()> {
    let cases = [
        (
            ChangeKind::AddedChunk,
            TimeInterval::try_new(111, 112)?,
            PartialPatchReason::Append,
            TimeInterval::try_new(110, 119)?,
        ),
        (
            ChangeKind::AddedChunk,
            TimeInterval::try_new(105, 105)?,
            PartialPatchReason::UnsealedDisorder,
            TimeInterval::try_new(100, 109)?,
        ),
        (
            ChangeKind::AddedChunk,
            TimeInterval::try_new(95, 95)?,
            PartialPatchReason::RetroactiveRepair,
            TimeInterval::try_new(90, 99)?,
        ),
        (
            ChangeKind::WildcardMemberAdded,
            TimeInterval::try_new(105, 105)?,
            PartialPatchReason::MembershipChange,
            TimeInterval::try_new(100, 109)?,
        ),
        (
            ChangeKind::RemovedChunk,
            TimeInterval::try_new(105, 105)?,
            PartialPatchReason::MembershipChange,
            TimeInterval::try_new(100, 109)?,
        ),
    ];
    for (kind, changed, expected_reason, expected_range) in cases {
        let mut store = store()?;
        let changes = ChangeSet::new().with_change(change(kind, ChangeExtent::Bounded(changed))?);
        let decision = plan_incremental_reduction(
            &mut store,
            &locality()?,
            settled(RepairPolicy::within(10)?)?,
            &changes,
            request("state-1", "state-2"),
        )?;
        let ReductionDecision::RebuildDirty {
            ranges,
            reason,
            prior,
        } = decision
        else {
            panic!("bounded change must produce dirty rebuild");
        };
        assert_eq!(ranges, vec![expected_range]);
        assert_eq!(reason, expected_reason);
        assert!(matches!(
            prior,
            PartialResolution::Exact {
                work: PartialWork {
                    raw_rows_scanned: 0,
                    partials_read: 0,
                    partials_written: 0,
                },
                ..
            }
        ));
    }

    Ok(())
}

#[test]
fn missing_or_semantically_invalid_state_requires_named_full_rebuild() -> Result<()> {
    let mut empty = PartialStateStore::default();
    let changes = ChangeSet::new().with_change(change(
        ChangeKind::AddedChunk,
        ChangeExtent::Bounded(TimeInterval::try_new(111, 112)?),
    )?);
    assert_eq!(
        plan_incremental_reduction(
            &mut empty,
            &locality()?,
            settled(RepairPolicy::within(10)?)?,
            &changes,
            request("state-1", "state-2"),
        )?,
        ReductionDecision::RebuildAll {
            reason: FullRebuildReason::PartialState(RawRebuildReason::MissingState)
        }
    );

    let mut store = store()?;
    assert_eq!(
        plan_incremental_reduction(
            &mut store,
            &locality()?,
            settled(RepairPolicy::within(10)?)?,
            &ChangeSet::new().with_recipe_change(),
            ReductionRequest {
                recipe_id: "recipe-2",
                ..request("state-1", "state-2")
            },
        )?,
        ReductionDecision::RebuildAll {
            reason: FullRebuildReason::RecipeChanged
        }
    );

    let global = LocalityContract::try_new(LocalityClass::Global, ["source"])?;
    assert_eq!(
        plan_incremental_reduction(
            &mut store,
            &global,
            settled(RepairPolicy::within(10)?)?,
            &changes,
            request("state-1", "state-2"),
        )?,
        ReductionDecision::RebuildAll {
            reason: FullRebuildReason::GlobalChange
        }
    );

    Ok(())
}

#[test]
fn rejected_repairs_and_unexplained_identity_changes_fail() -> Result<()> {
    let changes = ChangeSet::new().with_change(change(
        ChangeKind::AddedChunk,
        ChangeExtent::Bounded(TimeInterval::try_new(95, 95)?),
    )?);
    let error = plan_incremental_reduction(
        &mut store()?,
        &locality()?,
        settled(RepairPolicy::reject())?,
        &changes,
        request("state-1", "state-2"),
    )
    .expect_err("rejected retroactive change must fail");
    assert!(
        error.to_string().contains("rejects retroactive data"),
        "{error}"
    );

    let error = plan_incremental_reduction(
        &mut store()?,
        &locality()?,
        settled(RepairPolicy::within(10)?)?,
        &ChangeSet::new(),
        request("state-1", "state-2"),
    )
    .expect_err("unexplained source identity change must fail");
    assert!(
        error
            .to_string()
            .contains("without a declared logical or frontier change"),
        "{error}"
    );

    Ok(())
}
