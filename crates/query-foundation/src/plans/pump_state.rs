// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Stateful pump-episode classification and bounded append planning.

use std::collections::VecDeque;

use datafusion::error::{DataFusionError, Result};

use crate::statistics::TimeInterval;

/// Configuration for the adaptive well-pump classifier.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PumpStateRecipe {
    lookback_minutes: i64,
    disturbance_drop: f64,
}

impl PumpStateRecipe {
    /// Construct a classifier using a trailing ceiling and minimum drop.
    pub fn try_new(lookback_minutes: i64, disturbance_drop: f64) -> Result<Self> {
        if lookback_minutes <= 0 {
            return Err(DataFusionError::Plan(format!(
                "pump-state lookback must be positive, got {lookback_minutes}"
            )));
        }
        if !disturbance_drop.is_finite() || disturbance_drop <= 0.0 {
            return Err(DataFusionError::Plan(format!(
                "pump-state disturbance drop must be finite and positive, got {disturbance_drop}"
            )));
        }
        Ok(Self {
            lookback_minutes,
            disturbance_drop,
        })
    }

    /// Trailing ceiling width in integer minutes.
    #[must_use]
    pub fn lookback_minutes(self) -> i64 {
        self.lookback_minutes
    }

    /// Classify an ordered, one-sample-per-minute source slice.
    ///
    /// The final disturbed island is provisional: a later, deeper trough can
    /// move its pumping/recovering split. [`PumpStateBoundary`] records that
    /// island so the next append replaces it rather than treating it as final.
    pub fn classify(self, samples: &[DepthSample]) -> Result<PumpStateClassification> {
        validate_samples(samples)?;
        let mut ceiling = VecDeque::<(usize, i64, f64)>::new();
        let mut phases = vec![PumpPhase::Static; samples.len()];
        let mut episode_start = None;
        let mut episodes = Vec::new();
        let mut largest_episode_rows = 0;
        let mut maximum_lookback_rows = 0;

        for (index, sample) in samples.iter().enumerate() {
            let oldest = sample
                .minute
                .checked_sub(self.lookback_minutes)
                .unwrap_or(i64::MIN);
            while ceiling
                .front()
                .is_some_and(|(_, minute, _)| *minute < oldest)
            {
                _ = ceiling.pop_front();
            }
            while ceiling
                .back()
                .is_some_and(|(_, _, depth)| *depth <= sample.depth)
            {
                _ = ceiling.pop_back();
            }
            ceiling.push_back((index, sample.minute, sample.depth));
            maximum_lookback_rows = maximum_lookback_rows.max(ceiling.len());
            let trailing_ceiling = ceiling
                .front()
                .map(|(_, _, depth)| *depth)
                .expect("current sample is always present in trailing ceiling");
            let disturbed = sample.depth < trailing_ceiling - self.disturbance_drop;
            let continues_episode = episode_start.is_some()
                && index > 0
                && samples[index - 1].minute.checked_add(1) == Some(sample.minute);

            match (disturbed, continues_episode, episode_start) {
                (true, true, Some(_)) => {}
                (true, _, Some(start)) => {
                    classify_episode(samples, &mut phases, start, index);
                    episodes.push(PumpEpisodeSpan {
                        start: samples[start].minute,
                        end: samples[index - 1].minute,
                        open: false,
                    });
                    largest_episode_rows = largest_episode_rows.max(index - start);
                    episode_start = Some(index);
                }
                (true, _, None) => episode_start = Some(index),
                (false, _, Some(start)) => {
                    classify_episode(samples, &mut phases, start, index);
                    episodes.push(PumpEpisodeSpan {
                        start: samples[start].minute,
                        end: samples[index - 1].minute,
                        open: false,
                    });
                    largest_episode_rows = largest_episode_rows.max(index - start);
                    episode_start = None;
                }
                (false, _, None) => {}
            }
        }

        if let Some(start) = episode_start {
            classify_episode(samples, &mut phases, start, samples.len());
            episodes.push(PumpEpisodeSpan {
                start: samples[start].minute,
                end: samples
                    .last()
                    .expect("open episode requires a final sample")
                    .minute,
                open: true,
            });
            largest_episode_rows = largest_episode_rows.max(samples.len() - start);
        }

        let rows = samples
            .iter()
            .zip(phases)
            .map(|(sample, phase)| PumpStateRow {
                event_time: sample.event_time,
                minute: sample.minute,
                depth: sample.depth,
                phase,
            })
            .collect();
        Ok(PumpStateClassification {
            rows,
            episodes,
            boundary: PumpStateBoundary {
                observed_through: samples.last().map(|sample| sample.minute),
                open_episode_start: episode_start.map(|start| samples[start].minute),
            },
            metrics: PumpStateMetrics {
                source_rows: samples.len() as u64,
                maximum_lookback_rows: maximum_lookback_rows as u64,
                largest_episode_rows: largest_episode_rows as u64,
            },
        })
    }
}

fn validate_samples(samples: &[DepthSample]) -> Result<()> {
    for (index, sample) in samples.iter().enumerate() {
        if !sample.depth.is_finite() {
            return Err(DataFusionError::Execution(format!(
                "pump-state depth at minute {} is not finite",
                sample.minute
            )));
        }
        if let Some(previous) = index.checked_sub(1).map(|previous| samples[previous]) {
            if sample.minute <= previous.minute {
                return Err(DataFusionError::Execution(format!(
                    "pump-state input minutes must be strictly increasing; {} follows {}",
                    sample.minute, previous.minute
                )));
            }
            if sample.event_time <= previous.event_time {
                return Err(DataFusionError::Execution(format!(
                    "pump-state event times must be strictly increasing; {} follows {}",
                    sample.event_time, previous.event_time
                )));
            }
        }
    }
    Ok(())
}

fn classify_episode(samples: &[DepthSample], phases: &mut [PumpPhase], start: usize, end: usize) {
    let trough = (start..end)
        .min_by(|left, right| {
            samples[*left]
                .depth
                .total_cmp(&samples[*right].depth)
                .then_with(|| samples[*left].minute.cmp(&samples[*right].minute))
        })
        .expect("episode always contains at least one row");
    for (index, phase) in phases.iter_mut().enumerate().take(end).skip(start) {
        *phase = if index <= trough {
            PumpPhase::Pumping
        } else {
            PumpPhase::Recovering
        };
    }
}

/// One validated depth observation.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DepthSample {
    /// Original event time retained in the output.
    pub event_time: i64,
    /// Integer epoch-minute key used by the range window and gap detection.
    pub minute: i64,
    /// Well depth.
    pub depth: f64,
}

/// Classified physical pump state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PumpPhase {
    /// At rest near the trailing ceiling.
    Static,
    /// Disturbed and at or before the episode trough.
    Pumping,
    /// Disturbed and after the episode trough.
    Recovering,
}

/// One classified output row.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PumpStateRow {
    /// Original event time.
    pub event_time: i64,
    /// Integer epoch minute.
    pub minute: i64,
    /// Well depth.
    pub depth: f64,
    /// Classified phase.
    pub phase: PumpPhase,
}

/// Persistent boundary needed to plan the next append.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PumpStateBoundary {
    observed_through: Option<i64>,
    open_episode_start: Option<i64>,
}

impl PumpStateBoundary {
    /// Reconstruct a persisted boundary.
    pub fn try_new(observed_through: Option<i64>, open_episode_start: Option<i64>) -> Result<Self> {
        if observed_through.is_none() && open_episode_start.is_some() {
            return Err(DataFusionError::Plan(
                "open pump-state episode requires an observed-through minute".to_owned(),
            ));
        }
        if let (Some(observed), Some(open)) = (observed_through, open_episode_start)
            && open > observed
        {
            return Err(DataFusionError::Plan(format!(
                "open pump-state episode starts at {open} after observed minute {observed}"
            )));
        }
        Ok(Self {
            observed_through,
            open_episode_start,
        })
    }

    /// Greatest source minute represented by the current output.
    #[must_use]
    pub fn observed_through(self) -> Option<i64> {
        self.observed_through
    }

    /// Start of the provisional disturbed island, if one remains open.
    #[must_use]
    pub fn open_episode_start(self) -> Option<i64> {
        self.open_episode_start
    }

    /// Plan the source read and output replacement for an exact append.
    ///
    /// A closed prior output reads only the trailing ceiling context and appends
    /// new rows. An open episode rereads from its start (plus ceiling context)
    /// and replaces that provisional suffix because a new trough can move the
    /// phase split backward.
    pub fn plan_append(
        self,
        recipe: PumpStateRecipe,
        changed: TimeInterval,
    ) -> Result<PumpStateAppendPlan> {
        if let Some(observed) = self.observed_through
            && changed.min() <= observed
        {
            return Err(DataFusionError::Plan(format!(
                "pump-state append begins at {} at or before observed minute {observed}; retroactive repair requires a persisted episode index",
                changed.min()
            )));
        }
        let replace_from = self.open_episode_start.unwrap_or(changed.min());
        let context_anchor = self.open_episode_start.unwrap_or(changed.min());
        let read_from = context_anchor
            .checked_sub(recipe.lookback_minutes)
            .unwrap_or(i64::MIN);
        Ok(PumpStateAppendPlan {
            source: TimeInterval::try_new(read_from, changed.max())?,
            replace_from,
        })
    }

    /// Plan a conservative suffix repair using persisted disturbed-episode
    /// boundaries.
    ///
    /// A changed row can become a new trough and revise phase labels back to
    /// the start of the disturbed island containing it. It can also turn a
    /// static boundary into a disturbed row and join the immediately preceding
    /// island, so adjacency selects that island as the repair anchor. The
    /// source read includes the trailing-ceiling context before the anchor and
    /// continues through the previously observed suffix.
    pub fn plan_repair(
        self,
        recipe: PumpStateRecipe,
        changed: TimeInterval,
        episodes: &[PumpEpisodeSpan],
    ) -> Result<PumpStateAppendPlan> {
        let Some(observed) = self.observed_through else {
            return self.plan_append(recipe, changed);
        };
        if changed.min() > observed {
            return self.plan_append(recipe, changed);
        }
        validate_episode_index(episodes, observed, self.open_episode_start)?;
        let replace_from = episodes
            .iter()
            .filter(|episode| {
                episode.start <= changed.min()
                    && episode
                        .end
                        .checked_add(1)
                        .is_none_or(|end| end >= changed.min())
            })
            .map(|episode| episode.start)
            .next_back()
            .unwrap_or(changed.min());
        let read_from = replace_from
            .checked_sub(recipe.lookback_minutes)
            .unwrap_or(i64::MIN);
        Ok(PumpStateAppendPlan {
            source: TimeInterval::try_new(read_from, observed.max(changed.max()))?,
            replace_from,
        })
    }
}

fn validate_episode_index(
    episodes: &[PumpEpisodeSpan],
    observed: i64,
    open_episode_start: Option<i64>,
) -> Result<()> {
    let mut previous_end = None;
    for (index, episode) in episodes.iter().enumerate() {
        if episode.start > episode.end {
            return Err(DataFusionError::Plan(format!(
                "pump-state episode starts at {} after ending at {}",
                episode.start, episode.end
            )));
        }
        if episode.end > observed {
            return Err(DataFusionError::Plan(format!(
                "pump-state episode ends at {} after observed minute {observed}",
                episode.end
            )));
        }
        if previous_end.is_some_and(|end| end >= episode.start) {
            return Err(DataFusionError::Plan(
                "pump-state episode index must be strictly ordered and non-overlapping".to_owned(),
            ));
        }
        if episode.open && index + 1 != episodes.len() {
            return Err(DataFusionError::Plan(
                "only the final pump-state episode may be open".to_owned(),
            ));
        }
        previous_end = Some(episode.end);
    }
    let indexed_open = episodes
        .last()
        .filter(|episode| episode.open)
        .map(|episode| episode.start);
    if indexed_open != open_episode_start {
        return Err(DataFusionError::Plan(format!(
            "pump-state boundary open episode {open_episode_start:?} does not match episode index {indexed_open:?}"
        )));
    }
    Ok(())
}

/// One persisted disturbed-island boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PumpEpisodeSpan {
    /// Inclusive first disturbed minute.
    pub start: i64,
    /// Inclusive last currently observed disturbed minute.
    pub end: i64,
    /// Whether later rows can still revise this island's trough.
    pub open: bool,
}

/// Bounded source and output extents for one append.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PumpStateAppendPlan {
    source: TimeInterval,
    replace_from: i64,
}

impl PumpStateAppendPlan {
    /// Exact source interval needed to recompute the append.
    #[must_use]
    pub fn source(self) -> TimeInterval {
        self.source
    }

    /// First output minute replaced by the recomputation.
    #[must_use]
    pub fn replace_from(self) -> i64 {
        self.replace_from
    }
}

/// Work observations from one classification slice.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PumpStateMetrics {
    /// Source rows classified.
    pub source_rows: u64,
    /// Largest trailing-ceiling deque.
    pub maximum_lookback_rows: u64,
    /// Largest disturbed episode in the supplied slice.
    pub largest_episode_rows: u64,
}

/// Classified rows, resumable boundary, and measurable work.
#[derive(Clone, Debug, PartialEq)]
pub struct PumpStateClassification {
    /// Classified rows, including the provisional open suffix.
    pub rows: Vec<PumpStateRow>,
    /// Disturbed-island index used to anchor retroactive repairs.
    pub episodes: Vec<PumpEpisodeSpan>,
    /// Boundary state for the next append.
    pub boundary: PumpStateBoundary,
    /// Physical work observations.
    pub metrics: PumpStateMetrics,
}
