// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Explicit incremental fixed-window reduction decisions.

use std::collections::BTreeMap;

use datafusion::error::{DataFusionError, Result};

use crate::frontier::{ChangeDisposition, SettledState};
use crate::locality::{ChangeKind, ChangeSet, LocalityClass, LocalityContract, OutputImpact};
use crate::partial::{PartialPatchReason, PartialResolution, PartialStateStore, RawRebuildReason};
use crate::statistics::TimeInterval;

/// Complete rebuild cause when bounded state reuse is impossible.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FullRebuildReason {
    /// Semantic recipe identity changed.
    RecipeChanged,
    /// Logical schema changed.
    SchemaChanged,
    /// A global recipe cannot localize a relevant logical change.
    GlobalChange,
    /// Required partial state is unavailable or incompatible.
    PartialState(RawRebuildReason),
}

/// One explicit incremental reduction decision.
#[derive(Clone, Debug, PartialEq)]
pub enum ReductionDecision {
    /// Existing or folded partial state satisfies the request.
    Reuse {
        /// State-resolution evidence and work counters.
        resolution: PartialResolution,
    },
    /// Recompute only normalized dirty output windows from exact raw bounds.
    RebuildDirty {
        /// Exact dirty window intervals.
        ranges: Vec<TimeInterval>,
        /// Why these windows changed.
        reason: PartialPatchReason,
        /// Prior state that will receive the resulting patch.
        prior: PartialResolution,
    },
    /// Recompute complete input for an explicit reason.
    RebuildAll {
        /// Reason bounded reuse is invalid.
        reason: FullRebuildReason,
    },
}

/// Identity and resolution request for one incremental reduction.
pub struct ReductionRequest<'a> {
    /// Semantic recipe identity being requested.
    pub recipe_id: &'a str,
    /// Source state represented by existing partials.
    pub prior_source_state_id: &'a str,
    /// Exact new source state being planned.
    pub source_state_id: &'a str,
    /// Requested fixed-window width.
    pub width: i64,
    /// Requested fixed-window origin.
    pub origin: i64,
}

/// Plan reuse or exact rebuild work without executing raw input.
pub fn plan_incremental_reduction(
    store: &mut PartialStateStore,
    locality: &LocalityContract,
    settled: SettledState,
    changes: &ChangeSet,
    request: ReductionRequest<'_>,
) -> Result<ReductionDecision> {
    let patch_reason = classify_relevant_changes(locality, settled, changes)?;
    let impact = locality.affected_output_ranges(changes)?;
    match impact {
        OutputImpact::Complete => {
            let reason = if changes.recipe_changed() {
                FullRebuildReason::RecipeChanged
            } else if changes.schema_changed() {
                FullRebuildReason::SchemaChanged
            } else if matches!(locality.class(), LocalityClass::Global) {
                FullRebuildReason::GlobalChange
            } else {
                return Err(DataFusionError::Plan(
                    "complete reduction impact has no declared rebuild reason".to_owned(),
                ));
            };
            Ok(ReductionDecision::RebuildAll { reason })
        }
        OutputImpact::None => {
            let has_relevant_change = changes
                .changes()
                .iter()
                .any(|change| locality.depends_on(change.source()));
            if request.source_state_id != request.prior_source_state_id
                && changes.frontier().is_none()
                && !has_relevant_change
            {
                return Err(DataFusionError::Plan(
                    "source-state identity changed without a declared logical or frontier change"
                        .to_owned(),
                ));
            }
            let prior = store.resolve(
                request.recipe_id,
                request.prior_source_state_id,
                request.width,
                request.origin,
            )?;
            if let PartialResolution::NeedsRaw { reason } = prior {
                return Ok(ReductionDecision::RebuildAll {
                    reason: FullRebuildReason::PartialState(reason),
                });
            }
            if request.source_state_id != request.prior_source_state_id {
                _ = store.apply_patch(
                    request.recipe_id,
                    request.prior_source_state_id,
                    request.source_state_id,
                    request.width,
                    BTreeMap::new(),
                    patch_reason.unwrap_or(PartialPatchReason::Append),
                    0,
                )?;
            }
            let resolution = store.resolve(
                request.recipe_id,
                request.source_state_id,
                request.width,
                request.origin,
            )?;
            Ok(ReductionDecision::Reuse { resolution })
        }
        OutputImpact::Bounded(ranges) => {
            if request.source_state_id == request.prior_source_state_id {
                return Err(DataFusionError::Plan(
                    "dirty reduction change must advance source-state identity".to_owned(),
                ));
            }
            let prior = store.resolve(
                request.recipe_id,
                request.prior_source_state_id,
                request.width,
                request.origin,
            )?;
            if let PartialResolution::NeedsRaw { reason } = prior {
                return Ok(ReductionDecision::RebuildAll {
                    reason: FullRebuildReason::PartialState(reason),
                });
            }
            let reason = patch_reason.ok_or_else(|| {
                DataFusionError::Plan(
                    "dirty reduction ranges have no declared change reason".to_owned(),
                )
            })?;
            Ok(ReductionDecision::RebuildDirty {
                ranges,
                reason,
                prior,
            })
        }
    }
}

fn classify_relevant_changes(
    locality: &LocalityContract,
    settled: SettledState,
    changes: &ChangeSet,
) -> Result<Option<PartialPatchReason>> {
    let mut selected = None;
    for change in changes
        .changes()
        .iter()
        .filter(|change| locality.depends_on(change.source()))
    {
        let disposition = settled.classify(change.extent())?;
        let reason = if matches!(
            change.kind(),
            ChangeKind::RemovedChunk
                | ChangeKind::WildcardMemberAdded
                | ChangeKind::WildcardMemberRemoved
        ) {
            PartialPatchReason::MembershipChange
        } else {
            match disposition {
                ChangeDisposition::NoRows => continue,
                ChangeDisposition::Append { .. } => PartialPatchReason::Append,
                ChangeDisposition::UnsealedDisorder { .. } => PartialPatchReason::UnsealedDisorder,
                ChangeDisposition::Repair { .. } => PartialPatchReason::RetroactiveRepair,
            }
        };
        selected = Some(stronger_reason(selected, reason));
    }
    Ok(selected)
}

fn stronger_reason(
    current: Option<PartialPatchReason>,
    candidate: PartialPatchReason,
) -> PartialPatchReason {
    match current {
        Some(current) if reason_rank(current) >= reason_rank(candidate) => current,
        _ => candidate,
    }
}

fn reason_rank(reason: PartialPatchReason) -> u8 {
    match reason {
        PartialPatchReason::Append => 0,
        PartialPatchReason::UnsealedDisorder => 1,
        PartialPatchReason::MembershipChange => 2,
        PartialPatchReason::RetroactiveRepair => 3,
    }
}
