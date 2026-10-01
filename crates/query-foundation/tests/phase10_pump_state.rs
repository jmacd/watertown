// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use datafusion::error::Result;
use query_foundation::plans::pump_state::{
    DepthSample, PumpEpisodeSpan, PumpPhase, PumpStateBoundary, PumpStateRecipe,
};
use query_foundation::statistics::TimeInterval;

fn sample(minute: i64, depth: f64) -> DepthSample {
    DepthSample {
        event_time: minute * 60_000_000,
        minute,
        depth,
    }
}

#[test]
fn classifies_disturbance_at_its_future_trough() -> Result<()> {
    let recipe = PumpStateRecipe::try_new(60, 0.3)?;
    let mut samples = (0..=60)
        .map(|minute| sample(minute, 45.0))
        .collect::<Vec<_>>();
    samples.extend([
        sample(61, 44.5),
        sample(62, 44.0),
        sample(63, 43.5),
        sample(64, 43.8),
        sample(65, 44.2),
        sample(66, 44.8),
    ]);

    let classified = recipe.classify(&samples)?;
    assert!(
        classified.rows[..=60]
            .iter()
            .all(|row| row.phase == PumpPhase::Static)
    );
    assert_eq!(
        classified.rows[61..]
            .iter()
            .map(|row| row.phase)
            .collect::<Vec<_>>(),
        vec![
            PumpPhase::Pumping,
            PumpPhase::Pumping,
            PumpPhase::Pumping,
            PumpPhase::Recovering,
            PumpPhase::Recovering,
            PumpPhase::Static,
        ]
    );
    assert_eq!(classified.boundary.open_episode_start(), None);
    assert_eq!(classified.boundary.observed_through(), Some(66));
    assert_eq!(
        classified.episodes,
        vec![PumpEpisodeSpan {
            start: 61,
            end: 65,
            open: false,
        }]
    );
    assert!(classified.metrics.maximum_lookback_rows <= 61);
    assert_eq!(classified.metrics.largest_episode_rows, 5);
    Ok(())
}

#[test]
fn retains_and_replaces_the_complete_open_episode() -> Result<()> {
    let recipe = PumpStateRecipe::try_new(60, 0.3)?;
    let mut samples = (0..=60)
        .map(|minute| sample(minute, 45.0))
        .collect::<Vec<_>>();
    samples.extend([
        sample(61, 44.5),
        sample(62, 44.0),
        sample(63, 43.5),
        sample(64, 43.8),
    ]);

    let first = recipe.classify(&samples)?;
    assert_eq!(first.boundary.open_episode_start(), Some(61));
    assert_eq!(
        first.rows[61..]
            .iter()
            .map(|row| row.phase)
            .collect::<Vec<_>>(),
        vec![
            PumpPhase::Pumping,
            PumpPhase::Pumping,
            PumpPhase::Pumping,
            PumpPhase::Recovering,
        ]
    );

    let append = first
        .boundary
        .plan_append(recipe, TimeInterval::try_new(65, 66)?)?;
    assert_eq!(append.source(), TimeInterval::try_new(1, 66)?);
    assert_eq!(append.replace_from(), 61);

    samples.extend([sample(65, 43.0), sample(66, 44.8)]);
    let repaired = recipe.classify(&samples[1..])?;
    assert_eq!(
        repaired.rows[60..]
            .iter()
            .map(|row| row.phase)
            .collect::<Vec<_>>(),
        vec![
            PumpPhase::Pumping,
            PumpPhase::Pumping,
            PumpPhase::Pumping,
            PumpPhase::Pumping,
            PumpPhase::Pumping,
            PumpPhase::Static,
        ],
        "a later trough must revise the previously provisional recovery row"
    );
    Ok(())
}

#[test]
fn minute_gaps_close_disturbance_islands() -> Result<()> {
    let recipe = PumpStateRecipe::try_new(60, 0.3)?;
    let samples = [
        sample(0, 45.0),
        sample(1, 44.0),
        sample(3, 43.0),
        sample(4, 45.0),
    ];
    let classified = recipe.classify(&samples)?;
    assert_eq!(
        classified
            .rows
            .iter()
            .map(|row| row.phase)
            .collect::<Vec<_>>(),
        vec![
            PumpPhase::Static,
            PumpPhase::Pumping,
            PumpPhase::Pumping,
            PumpPhase::Static,
        ]
    );
    assert_eq!(classified.metrics.largest_episode_rows, 1);
    Ok(())
}

#[test]
fn malformed_recipe_and_source_order_fail_loudly() {
    assert!(PumpStateRecipe::try_new(0, 0.3).is_err());
    assert!(PumpStateRecipe::try_new(60, f64::NAN).is_err());
    assert!(PumpStateBoundary::try_new(None, Some(1)).is_err());
    assert!(PumpStateBoundary::try_new(Some(1), Some(2)).is_err());

    let recipe = PumpStateRecipe::try_new(60, 0.3).unwrap();
    let duplicate = recipe
        .classify(&[sample(1, 45.0), sample(1, 44.0)])
        .unwrap_err();
    assert!(
        duplicate
            .to_string()
            .contains("input minutes must be strictly increasing")
    );
    let non_finite = recipe.classify(&[sample(1, f64::NAN)]).unwrap_err();
    assert!(
        non_finite
            .to_string()
            .contains("depth at minute 1 is not finite")
    );
}

#[test]
fn closed_append_work_is_independent_of_retained_history() -> Result<()> {
    let recipe = PumpStateRecipe::try_new(60, 0.3)?;
    let short = recipe.classify(
        &(0..=100)
            .map(|minute| sample(minute, 45.0))
            .collect::<Vec<_>>(),
    )?;
    let long = recipe.classify(
        &(0..=10_000)
            .map(|minute| sample(minute, 45.0))
            .collect::<Vec<_>>(),
    )?;

    let short_plan = short
        .boundary
        .plan_append(recipe, TimeInterval::try_new(101, 105)?)?;
    let long_plan = long
        .boundary
        .plan_append(recipe, TimeInterval::try_new(10_001, 10_005)?)?;
    assert_eq!(
        short_plan.source().max() - short_plan.source().min(),
        long_plan.source().max() - long_plan.source().min()
    );
    assert_eq!(short_plan.replace_from(), 101);
    assert_eq!(long_plan.replace_from(), 10_001);
    Ok(())
}

#[test]
fn retroactive_change_uses_persisted_episode_index_for_repair_anchor() -> Result<()> {
    let recipe = PumpStateRecipe::try_new(60, 0.3)?;
    let mut samples = (0..=60)
        .map(|minute| sample(minute, 45.0))
        .collect::<Vec<_>>();
    samples.extend([
        sample(61, 44.5),
        sample(62, 44.0),
        sample(63, 43.5),
        sample(64, 43.8),
        sample(65, 44.2),
        sample(66, 44.8),
    ]);
    let classified = recipe.classify(&samples)?;

    let inside = classified.boundary.plan_repair(
        recipe,
        TimeInterval::try_new(64, 64)?,
        &classified.episodes,
    )?;
    assert_eq!(inside.source(), TimeInterval::try_new(1, 66)?);
    assert_eq!(inside.replace_from(), 61);

    let adjacent = classified.boundary.plan_repair(
        recipe,
        TimeInterval::try_new(66, 66)?,
        &classified.episodes,
    )?;
    assert_eq!(adjacent.source(), TimeInterval::try_new(1, 66)?);
    assert_eq!(adjacent.replace_from(), 61);
    Ok(())
}
