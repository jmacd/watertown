// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use datafusion::error::Result;
use query_foundation::locality::{
    ChangeExtent, ChangeKind, ChangeSet, FrontierChange, LocalityClass, LocalityContract,
    LogicalChange, OutputImpact, RangeRequirement, StateContract,
};
use query_foundation::statistics::TimeInterval;

fn interval(min: i64, max: i64) -> Result<TimeInterval> {
    TimeInterval::try_new(min, max)
}

fn change(kind: ChangeKind, identity: &str, extent: ChangeExtent) -> Result<LogicalChange> {
    LogicalChange::try_new(kind, "series", identity, extent)
}

#[test]
fn change_set_distinguishes_every_logical_change_class() -> Result<()> {
    let changes = ChangeSet::new()
        .with_change(change(
            ChangeKind::AddedChunk,
            "chunk-added",
            ChangeExtent::Bounded(interval(10, 19)?),
        )?)
        .with_change(change(
            ChangeKind::RemovedChunk,
            "chunk-removed",
            ChangeExtent::Bounded(interval(0, 9)?),
        )?)
        .with_change(change(
            ChangeKind::WildcardMemberAdded,
            "member-added",
            ChangeExtent::Unknown,
        )?)
        .with_change(change(
            ChangeKind::WildcardMemberRemoved,
            "member-removed",
            ChangeExtent::Empty,
        )?)
        .with_recipe_change()
        .with_schema_change()
        .with_frontier_change(FrontierChange::try_new(Some(19), 29)?);

    assert_eq!(changes.changes().len(), 4);
    assert_eq!(changes.changes()[0].kind(), ChangeKind::AddedChunk);
    assert_eq!(changes.changes()[0].source(), "series");
    assert_eq!(changes.changes()[0].identity(), "chunk-added");
    assert_eq!(
        changes.changes()[0].extent(),
        ChangeExtent::Bounded(interval(10, 19)?)
    );
    assert!(changes.recipe_changed());
    assert!(changes.schema_changed());
    assert_eq!(
        changes.frontier(),
        Some(FrontierChange::try_new(Some(19), 29)?)
    );

    Ok(())
}

#[test]
fn row_and_timestamp_local_ranges_are_exact() -> Result<()> {
    for class in [LocalityClass::RowLocal, LocalityClass::TimestampLocal] {
        let contract = LocalityContract::try_new(class, ["series", "right"])?;
        assert_eq!(contract.state_contract(), StateContract::None);
        let requested = interval(10, 20)?;
        let required = contract.required_input_ranges(requested)?;
        assert_eq!(
            required.get("series"),
            Some(&RangeRequirement::Bounded(vec![requested]))
        );
        assert_eq!(
            required.get("right"),
            Some(&RangeRequirement::Bounded(vec![requested]))
        );

        let changes = ChangeSet::new()
            .with_change(change(
                ChangeKind::AddedChunk,
                "chunk-1",
                ChangeExtent::Bounded(interval(0, 4)?),
            )?)
            .with_change(change(
                ChangeKind::WildcardMemberAdded,
                "member-1",
                ChangeExtent::Bounded(interval(5, 9)?),
            )?);
        assert_eq!(
            contract.affected_output_ranges(&changes)?,
            OutputImpact::Bounded(vec![interval(0, 9)?])
        );
    }

    Ok(())
}

#[test]
fn fixed_windows_expand_and_normalize_dirty_ranges() -> Result<()> {
    let contract = LocalityContract::try_new(
        LocalityClass::FixedWindow {
            width: 10,
            origin: 0,
        },
        ["series"],
    )?;
    assert_eq!(contract.state_contract(), StateContract::WindowPartials);
    assert_eq!(
        contract
            .required_input_ranges(interval(10, 10)?)?
            .get("series"),
        Some(&RangeRequirement::Bounded(vec![interval(10, 19)?]))
    );

    let boundary = ChangeSet::new().with_change(change(
        ChangeKind::AddedChunk,
        "boundary",
        ChangeExtent::Bounded(interval(9, 10)?),
    )?);
    assert_eq!(
        contract.affected_output_ranges(&boundary)?,
        OutputImpact::Bounded(vec![interval(0, 19)?])
    );

    let separated = ChangeSet::new()
        .with_change(change(
            ChangeKind::AddedChunk,
            "early",
            ChangeExtent::Bounded(interval(1, 1)?),
        )?)
        .with_change(change(
            ChangeKind::AddedChunk,
            "late",
            ChangeExtent::Bounded(interval(25, 25)?),
        )?);
    assert_eq!(
        contract.affected_output_ranges(&separated)?,
        OutputImpact::Bounded(vec![interval(0, 9)?, interval(20, 29)?])
    );

    let negative = LocalityContract::try_new(
        LocalityClass::FixedWindow {
            width: 10,
            origin: 5,
        },
        ["series"],
    )?;
    let changes = ChangeSet::new().with_change(change(
        ChangeKind::AddedChunk,
        "negative",
        ChangeExtent::Bounded(interval(-1, -1)?),
    )?);
    assert_eq!(
        negative.affected_output_ranges(&changes)?,
        OutputImpact::Bounded(vec![interval(-5, 4)?])
    );

    Ok(())
}

#[test]
fn unknown_semantics_are_complete_but_empty_and_frontier_only_are_clean() -> Result<()> {
    let local = LocalityContract::try_new(LocalityClass::TimestampLocal, ["series"])?;
    let frontier_only =
        ChangeSet::new().with_frontier_change(FrontierChange::try_new(Some(10), 20)?);
    assert_eq!(
        local.affected_output_ranges(&frontier_only)?,
        OutputImpact::None
    );

    let empty = ChangeSet::new().with_change(change(
        ChangeKind::AddedChunk,
        "empty",
        ChangeExtent::Empty,
    )?);
    assert_eq!(local.affected_output_ranges(&empty)?, OutputImpact::None);

    let unrelated = ChangeSet::new().with_change(LogicalChange::try_new(
        ChangeKind::AddedChunk,
        "other-source",
        "chunk",
        ChangeExtent::Bounded(interval(0, 10)?),
    )?);
    assert_eq!(
        local.affected_output_ranges(&unrelated)?,
        OutputImpact::None
    );

    let unknown = ChangeSet::new().with_change(change(
        ChangeKind::RemovedChunk,
        "unknown",
        ChangeExtent::Unknown,
    )?);
    assert_eq!(
        local.affected_output_ranges(&unknown)?,
        OutputImpact::Complete
    );
    assert_eq!(
        local.affected_output_ranges(&ChangeSet::new().with_recipe_change())?,
        OutputImpact::Complete
    );
    assert_eq!(
        local.affected_output_ranges(&ChangeSet::new().with_schema_change())?,
        OutputImpact::Complete
    );

    let global = LocalityContract::try_new(LocalityClass::Global, ["series"])?;
    assert_eq!(global.state_contract(), StateContract::CompleteInput);
    assert_eq!(
        global.required_input_ranges(interval(0, 1)?)?.get("series"),
        Some(&RangeRequirement::Complete)
    );
    assert_eq!(
        global.affected_output_ranges(&ChangeSet::new().with_change(change(
            ChangeKind::AddedChunk,
            "chunk",
            ChangeExtent::Bounded(interval(0, 1)?),
        )?))?,
        OutputImpact::Complete
    );

    Ok(())
}

#[test]
fn invalid_contracts_and_backward_frontiers_fail_explicitly() {
    let error = LocalityContract::try_new(
        LocalityClass::FixedWindow {
            width: 0,
            origin: 0,
        },
        ["series"],
    )
    .expect_err("zero-width window must fail");
    assert!(error.to_string().contains("must be positive"), "{error}");

    let error = LocalityContract::try_new(LocalityClass::RowLocal, ["series", "series"])
        .expect_err("duplicate input identity must fail");
    assert!(
        error.to_string().contains("duplicate locality input"),
        "{error}"
    );

    let error = FrontierChange::try_new(Some(20), 19).expect_err("backward frontier must fail");
    assert!(
        error.to_string().contains("cannot move backward"),
        "{error}"
    );

    let error = LogicalChange::try_new(ChangeKind::AddedChunk, "", "chunk", ChangeExtent::Unknown)
        .expect_err("empty source identity must fail");
    assert!(
        error
            .to_string()
            .contains("source identity must not be empty"),
        "{error}"
    );
}
