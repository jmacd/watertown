// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Settled-frontier classification and explicit retroactive repair policy.

use datafusion::error::{DataFusionError, Result};

use crate::locality::ChangeExtent;
use crate::statistics::TimeInterval;

/// Source policy for logical changes at or behind the settled frontier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RepairPolicy {
    max_lateness: Option<i64>,
}

impl RepairPolicy {
    /// Reject every retroactive logical change.
    #[must_use]
    pub fn reject() -> Self {
        Self { max_lateness: None }
    }

    /// Permit repair no farther than `max_lateness` event-time units behind
    /// the settled frontier.
    pub fn within(max_lateness: i64) -> Result<Self> {
        if max_lateness < 0 {
            return Err(DataFusionError::Plan(format!(
                "maximum repair lateness must not be negative, got {max_lateness}"
            )));
        }
        Ok(Self {
            max_lateness: Some(max_lateness),
        })
    }
}

/// Required handling for one changed event-time extent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChangeDisposition {
    /// The logical change contains no rows.
    NoRows,
    /// Every changed row is beyond previously observed event time.
    Append {
        /// Exact appended interval.
        changed: TimeInterval,
    },
    /// Changed rows are after the settled frontier but at or behind previously
    /// observed event time.
    UnsealedDisorder {
        /// Exact changed interval in the active unsealed region.
        changed: TimeInterval,
    },
    /// Some or all changed rows require retroactive output repair.
    Repair {
        /// Complete exact changed interval.
        changed: TimeInterval,
        /// Portion at or behind the settled frontier.
        retroactive: TimeInterval,
        /// Portion strictly after the frontier, when the change spans it.
        unsealed: Option<TimeInterval>,
    },
}

/// Source-supplied settled state and repair promise.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SettledState {
    settled_through: Option<i64>,
    observed_through: Option<i64>,
    repair_policy: RepairPolicy,
}

impl SettledState {
    /// Construct state from source-supplied settled and observed positions.
    pub fn try_new(
        settled_through: Option<i64>,
        observed_through: Option<i64>,
        repair_policy: RepairPolicy,
    ) -> Result<Self> {
        if settled_through.is_some() && observed_through.is_none() {
            return Err(DataFusionError::Plan(
                "settled frontier requires an observed-through position".to_owned(),
            ));
        }
        if let (Some(settled), Some(observed)) = (settled_through, observed_through)
            && settled > observed
        {
            return Err(DataFusionError::Plan(format!(
                "settled frontier {settled} exceeds observed-through position {observed}"
            )));
        }
        Ok(Self {
            settled_through,
            observed_through,
            repair_policy,
        })
    }

    /// Source-supplied settled frontier.
    #[must_use]
    pub fn settled_through(self) -> Option<i64> {
        self.settled_through
    }

    /// Greatest event time observed in the represented source state.
    #[must_use]
    pub fn observed_through(self) -> Option<i64> {
        self.observed_through
    }

    /// Classify one exact logical change without silently treating unknown or
    /// retroactive data as an ordinary append.
    pub fn classify(self, extent: ChangeExtent) -> Result<ChangeDisposition> {
        let changed = match extent {
            ChangeExtent::Empty => return Ok(ChangeDisposition::NoRows),
            ChangeExtent::Unknown => {
                return Err(DataFusionError::Plan(
                    "cannot classify change with unknown event-time extent against settled frontier"
                        .to_owned(),
                ));
            }
            ChangeExtent::Bounded(changed) => changed,
        };
        if self
            .observed_through
            .is_none_or(|observed| changed.min() > observed)
        {
            return Ok(ChangeDisposition::Append { changed });
        }
        let Some(frontier) = self.settled_through else {
            return Ok(ChangeDisposition::UnsealedDisorder { changed });
        };
        if changed.min() > frontier {
            return Ok(ChangeDisposition::UnsealedDisorder { changed });
        }

        let Some(max_lateness) = self.repair_policy.max_lateness else {
            return Err(DataFusionError::Plan(format!(
                "retroactive change [{}..={}] reaches settled frontier {frontier}, but repair policy rejects retroactive data",
                changed.min(),
                changed.max()
            )));
        };
        let earliest = frontier.checked_sub(max_lateness).unwrap_or(i64::MIN);
        if changed.min() < earliest {
            return Err(DataFusionError::Plan(format!(
                "retroactive change [{}..={}] exceeds repair policy: settled frontier {frontier}, earliest repairable event time {earliest}",
                changed.min(),
                changed.max()
            )));
        }
        let retroactive = TimeInterval::try_new(changed.min(), changed.max().min(frontier))?;
        let unsealed = if changed.max() > frontier {
            Some(TimeInterval::try_new(
                frontier.checked_add(1).ok_or_else(|| {
                    DataFusionError::Plan(
                        "settled frontier overflowed while classifying mixed change".to_owned(),
                    )
                })?,
                changed.max(),
            )?)
        } else {
            None
        };
        Ok(ChangeDisposition::Repair {
            changed,
            retroactive,
            unsealed,
        })
    }
}
