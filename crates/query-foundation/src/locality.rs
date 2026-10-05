// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Explicit logical changes and recipe range-locality contracts.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};

use crate::statistics::TimeInterval;

/// Logical membership or content change.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChangeKind {
    /// An immutable chunk entered snapshot membership.
    AddedChunk,
    /// An immutable chunk left snapshot membership.
    RemovedChunk,
    /// A wildcard member entered captured membership.
    WildcardMemberAdded,
    /// A wildcard member left captured membership.
    WildcardMemberRemoved,
}

/// Event-time extent of one logical change.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChangeExtent {
    /// The change contains no logical rows.
    Empty,
    /// The exact inclusive changed event-time interval.
    Bounded(TimeInterval),
    /// Rows changed, but their event-time extent is unavailable.
    Unknown,
}

/// One identified logical membership/content change.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalChange {
    kind: ChangeKind,
    source: Arc<str>,
    identity: Arc<str>,
    extent: ChangeExtent,
}

impl LogicalChange {
    /// Construct an identified logical change.
    pub fn try_new(
        kind: ChangeKind,
        source: impl Into<Arc<str>>,
        identity: impl Into<Arc<str>>,
        extent: ChangeExtent,
    ) -> Result<Self> {
        let source = source.into();
        let identity = identity.into();
        if source.is_empty() {
            return Err(DataFusionError::Plan(
                "change source identity must not be empty".to_owned(),
            ));
        }
        if identity.is_empty() {
            return Err(DataFusionError::Plan(
                "change member identity must not be empty".to_owned(),
            ));
        }
        Ok(Self {
            kind,
            source,
            identity,
            extent,
        })
    }

    /// Change category.
    #[must_use]
    pub fn kind(&self) -> ChangeKind {
        self.kind
    }

    /// Stable source identity.
    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Stable chunk or wildcard-member identity.
    #[must_use]
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// Changed event-time extent.
    #[must_use]
    pub fn extent(&self) -> ChangeExtent {
        self.extent
    }
}

/// Explicit settled-frontier movement, including no-data advancement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrontierChange {
    previous: Option<i64>,
    current: i64,
}

impl FrontierChange {
    /// Construct a monotonic frontier movement.
    pub fn try_new(previous: Option<i64>, current: i64) -> Result<Self> {
        if previous.is_some_and(|previous| current < previous) {
            return Err(DataFusionError::Plan(format!(
                "settled frontier cannot move backward from {} to {current}",
                previous.expect("checked Some")
            )));
        }
        Ok(Self { previous, current })
    }

    /// Previous frontier, when one existed.
    #[must_use]
    pub fn previous(self) -> Option<i64> {
        self.previous
    }

    /// New settled frontier.
    #[must_use]
    pub fn current(self) -> i64 {
        self.current
    }
}

/// Logical changes captured between two exact source states.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChangeSet {
    changes: Vec<LogicalChange>,
    recipe_changed: bool,
    schema_changed: bool,
    frontier: Option<FrontierChange>,
}

impl ChangeSet {
    /// Empty logical change set. Physical repacking alone remains empty.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an identified logical membership/content change.
    #[must_use]
    pub fn with_change(mut self, change: LogicalChange) -> Self {
        self.changes.push(change);
        self
    }

    /// Mark semantic recipe identity as changed.
    #[must_use]
    pub fn with_recipe_change(mut self) -> Self {
        self.recipe_changed = true;
        self
    }

    /// Mark the logical schema as changed.
    #[must_use]
    pub fn with_schema_change(mut self) -> Self {
        self.schema_changed = true;
        self
    }

    /// Record source-supplied settled-frontier movement.
    #[must_use]
    pub fn with_frontier_change(mut self, frontier: FrontierChange) -> Self {
        self.frontier = Some(frontier);
        self
    }

    /// Identified membership/content changes.
    #[must_use]
    pub fn changes(&self) -> &[LogicalChange] {
        &self.changes
    }

    /// Whether semantic recipe identity changed.
    #[must_use]
    pub fn recipe_changed(&self) -> bool {
        self.recipe_changed
    }

    /// Whether logical schema changed.
    #[must_use]
    pub fn schema_changed(&self) -> bool {
        self.schema_changed
    }

    /// Settled-frontier movement, including no-data advancement.
    #[must_use]
    pub fn frontier(&self) -> Option<FrontierChange> {
        self.frontier
    }
}

/// Range behavior declared by one typed recipe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalityClass {
    /// Output rows depend only on the same input rows.
    RowLocal,
    /// Output timestamps depend on the same timestamp across inputs.
    TimestampLocal,
    /// Output windows depend on complete aligned fixed-width buckets.
    FixedWindow {
        /// Positive event-time bucket width.
        width: i64,
        /// Event-time origin used for Euclidean bucket alignment.
        origin: i64,
    },
    /// Any logical input change may affect complete output.
    Global,
}

/// Persistent state required across executions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateContract {
    /// No cross-execution state is needed.
    None,
    /// Reusable partial aggregates are stored per fixed window.
    WindowPartials,
    /// Complete input is required unless another contract narrows it.
    CompleteInput,
}

/// Input range required for one source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RangeRequirement {
    /// Normalized bounded intervals.
    Bounded(Vec<TimeInterval>),
    /// Complete source input.
    Complete,
}

/// Required ranges keyed by stable logical input identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InputRanges {
    ranges: BTreeMap<Arc<str>, RangeRequirement>,
}

impl InputRanges {
    /// Requirement for one logical input.
    #[must_use]
    pub fn get(&self, input: &str) -> Option<&RangeRequirement> {
        self.ranges.get(input)
    }
}

/// Output invalidation caused by a change set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutputImpact {
    /// No logical output rows changed.
    None,
    /// Normalized dirty output intervals.
    Bounded(Vec<TimeInterval>),
    /// Complete output must be rebuilt.
    Complete,
}

/// Validated locality contract for a typed recipe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalityContract {
    class: LocalityClass,
    inputs: Arc<[Arc<str>]>,
}

impl LocalityContract {
    /// Construct a contract with stable, unique logical input identities.
    pub fn try_new<I, S>(class: LocalityClass, inputs: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<Arc<str>>,
    {
        if let LocalityClass::FixedWindow { width, .. } = class
            && width <= 0
        {
            return Err(DataFusionError::Plan(format!(
                "fixed-window width must be positive, got {width}"
            )));
        }
        let inputs = inputs.into_iter().map(Into::into).collect::<Vec<_>>();
        if inputs.is_empty() {
            return Err(DataFusionError::Plan(
                "locality contract requires at least one input".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for input in &inputs {
            if input.is_empty() {
                return Err(DataFusionError::Plan(
                    "locality input identity must not be empty".to_owned(),
                ));
            }
            if !unique.insert(input.as_ref()) {
                return Err(DataFusionError::Plan(format!(
                    "duplicate locality input identity '{input}'"
                )));
            }
        }
        Ok(Self {
            class,
            inputs: inputs.into(),
        })
    }

    /// Declared locality class.
    #[must_use]
    pub fn class(&self) -> LocalityClass {
        self.class
    }

    /// Whether this recipe consumes the named logical source.
    #[must_use]
    pub fn depends_on(&self, source: &str) -> bool {
        self.inputs.iter().any(|input| input.as_ref() == source)
    }

    /// Persistent-state requirement.
    #[must_use]
    pub fn state_contract(&self) -> StateContract {
        match self.class {
            LocalityClass::RowLocal | LocalityClass::TimestampLocal => StateContract::None,
            LocalityClass::FixedWindow { .. } => StateContract::WindowPartials,
            LocalityClass::Global => StateContract::CompleteInput,
        }
    }

    /// Required input ranges for one requested output interval.
    pub fn required_input_ranges(&self, requested: TimeInterval) -> Result<InputRanges> {
        let requirement = match self.class {
            LocalityClass::RowLocal | LocalityClass::TimestampLocal => {
                RangeRequirement::Bounded(vec![requested])
            }
            LocalityClass::FixedWindow { width, origin } => {
                RangeRequirement::Bounded(vec![expand_to_windows(requested, width, origin)?])
            }
            LocalityClass::Global => RangeRequirement::Complete,
        };
        Ok(InputRanges {
            ranges: self
                .inputs
                .iter()
                .map(|input| (Arc::clone(input), requirement.clone()))
                .collect(),
        })
    }

    /// Dirty output ranges caused by explicit logical changes.
    pub fn affected_output_ranges(&self, changes: &ChangeSet) -> Result<OutputImpact> {
        if changes.recipe_changed() || changes.schema_changed() {
            return Ok(OutputImpact::Complete);
        }
        let relevant = changes
            .changes()
            .iter()
            .filter(|change| {
                self.inputs
                    .iter()
                    .any(|input| input.as_ref() == change.source())
            })
            .collect::<Vec<_>>();
        if relevant.is_empty() {
            return Ok(OutputImpact::None);
        }
        if matches!(self.class, LocalityClass::Global) {
            return Ok(OutputImpact::Complete);
        }

        let mut intervals = Vec::new();
        for change in relevant {
            match change.extent() {
                ChangeExtent::Empty => {}
                ChangeExtent::Unknown => return Ok(OutputImpact::Complete),
                ChangeExtent::Bounded(interval) => intervals.push(match self.class {
                    LocalityClass::RowLocal | LocalityClass::TimestampLocal => interval,
                    LocalityClass::FixedWindow { width, origin } => {
                        expand_to_windows(interval, width, origin)?
                    }
                    LocalityClass::Global => unreachable!("handled above"),
                }),
            }
        }
        if intervals.is_empty() {
            Ok(OutputImpact::None)
        } else {
            Ok(OutputImpact::Bounded(normalize_intervals(intervals)?))
        }
    }
}

fn expand_to_windows(interval: TimeInterval, width: i64, origin: i64) -> Result<TimeInterval> {
    let min_delta = interval.min().checked_sub(origin).ok_or_else(|| {
        DataFusionError::Plan("fixed-window minimum alignment overflowed".to_owned())
    })?;
    let max_delta = interval.max().checked_sub(origin).ok_or_else(|| {
        DataFusionError::Plan("fixed-window maximum alignment overflowed".to_owned())
    })?;
    let min = origin
        .checked_add(
            min_delta
                .div_euclid(width)
                .checked_mul(width)
                .ok_or_else(|| {
                    DataFusionError::Plan("fixed-window minimum alignment overflowed".to_owned())
                })?,
        )
        .ok_or_else(|| {
            DataFusionError::Plan("fixed-window minimum alignment overflowed".to_owned())
        })?;
    let max_start = origin
        .checked_add(
            max_delta
                .div_euclid(width)
                .checked_mul(width)
                .ok_or_else(|| {
                    DataFusionError::Plan("fixed-window maximum alignment overflowed".to_owned())
                })?,
        )
        .ok_or_else(|| {
            DataFusionError::Plan("fixed-window maximum alignment overflowed".to_owned())
        })?;
    let max = max_start
        .checked_add(width - 1)
        .ok_or_else(|| DataFusionError::Plan("fixed-window end overflowed".to_owned()))?;
    TimeInterval::try_new(min, max)
}

fn normalize_intervals(mut intervals: Vec<TimeInterval>) -> Result<Vec<TimeInterval>> {
    intervals.sort_by_key(|interval| (interval.min(), interval.max()));
    let mut normalized: Vec<TimeInterval> = Vec::with_capacity(intervals.len());
    for interval in intervals {
        let Some(previous) = normalized.last_mut() else {
            normalized.push(interval);
            continue;
        };
        let touches = previous.max() == i64::MAX
            || interval.min() <= previous.max()
            || interval.min() == previous.max() + 1;
        if touches {
            *previous = TimeInterval::try_new(previous.min(), previous.max().max(interval.max()))?;
        } else {
            normalized.push(interval);
        }
    }
    Ok(normalized)
}
