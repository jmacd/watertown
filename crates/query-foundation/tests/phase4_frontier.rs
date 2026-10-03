// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use datafusion::error::Result;
use query_foundation::frontier::{ChangeDisposition, RepairPolicy, SettledState};
use query_foundation::locality::ChangeExtent;
use query_foundation::statistics::TimeInterval;

fn interval(min: i64, max: i64) -> Result<TimeInterval> {
    TimeInterval::try_new(min, max)
}

#[test]
fn append_and_unsealed_disorder_are_distinct() -> Result<()> {
    let no_frontier = SettledState::try_new(None, None, RepairPolicy::reject())?;
    assert_eq!(
        no_frontier.classify(ChangeExtent::Bounded(interval(-100, 10)?))?,
        ChangeDisposition::Append {
            changed: interval(-100, 10)?
        }
    );

    let settled = SettledState::try_new(Some(100), Some(110), RepairPolicy::within(10)?)?;
    assert_eq!(
        settled.classify(ChangeExtent::Bounded(interval(111, 120)?))?,
        ChangeDisposition::Append {
            changed: interval(111, 120)?
        }
    );
    assert_eq!(
        settled.classify(ChangeExtent::Bounded(interval(105, 106)?))?,
        ChangeDisposition::UnsealedDisorder {
            changed: interval(105, 106)?
        }
    );

    Ok(())
}

#[test]
fn frontier_touching_and_mixed_changes_produce_exact_repair_intervals() -> Result<()> {
    let settled = SettledState::try_new(Some(100), Some(110), RepairPolicy::within(10)?)?;
    assert_eq!(
        settled.classify(ChangeExtent::Bounded(interval(100, 100)?))?,
        ChangeDisposition::Repair {
            changed: interval(100, 100)?,
            retroactive: interval(100, 100)?,
            unsealed: None,
        }
    );
    assert_eq!(
        settled.classify(ChangeExtent::Bounded(interval(95, 105)?))?,
        ChangeDisposition::Repair {
            changed: interval(95, 105)?,
            retroactive: interval(95, 100)?,
            unsealed: Some(interval(101, 105)?),
        }
    );
    assert_eq!(
        settled.classify(ChangeExtent::Bounded(interval(90, 90)?))?,
        ChangeDisposition::Repair {
            changed: interval(90, 90)?,
            retroactive: interval(90, 90)?,
            unsealed: None,
        }
    );

    Ok(())
}

#[test]
fn unsupported_retroactive_changes_fail_with_policy_context() -> Result<()> {
    let rejecting = SettledState::try_new(Some(100), Some(110), RepairPolicy::reject())?;
    let error = rejecting
        .classify(ChangeExtent::Bounded(interval(100, 100)?))
        .expect_err("frontier-touching change must be retroactive");
    assert!(error.to_string().contains("frontier 100"), "{error}");
    assert!(
        error.to_string().contains("rejects retroactive data"),
        "{error}"
    );

    let bounded = SettledState::try_new(Some(100), Some(110), RepairPolicy::within(10)?)?;
    let error = bounded
        .classify(ChangeExtent::Bounded(interval(89, 90)?))
        .expect_err("change beyond repair policy must fail");
    assert!(
        error
            .to_string()
            .contains("earliest repairable event time 90"),
        "{error}"
    );

    Ok(())
}

#[test]
fn empty_and_unknown_changes_are_never_silent_append_fallbacks() -> Result<()> {
    let settled = SettledState::try_new(Some(100), Some(110), RepairPolicy::within(10)?)?;
    assert_eq!(
        settled.classify(ChangeExtent::Empty)?,
        ChangeDisposition::NoRows
    );
    let error = settled
        .classify(ChangeExtent::Unknown)
        .expect_err("unknown extent must fail");
    assert!(
        error.to_string().contains("unknown event-time extent"),
        "{error}"
    );

    Ok(())
}

#[test]
fn invalid_repair_policy_fails_during_planning() {
    let error = RepairPolicy::within(-1).expect_err("negative repair bound must fail");
    assert!(
        error.to_string().contains("must not be negative"),
        "{error}"
    );

    let error = SettledState::try_new(Some(10), None, RepairPolicy::reject())
        .expect_err("settled frontier without observed position must fail");
    assert!(
        error
            .to_string()
            .contains("requires an observed-through position"),
        "{error}"
    );

    let error = SettledState::try_new(Some(11), Some(10), RepairPolicy::reject())
        .expect_err("frontier beyond observed position must fail");
    assert!(
        error.to_string().contains("exceeds observed-through"),
        "{error}"
    );
}
