// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Explicit fixed-window aggregate partial manifests and reuse planning.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};

/// Mergeable statistics for one aggregate bucket and group.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AggregatePartial {
    rows: u64,
    non_null: u64,
    sum: f64,
    min: Option<f64>,
    max: Option<f64>,
}

impl AggregatePartial {
    /// Empty partial.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            rows: 0,
            non_null: 0,
            sum: 0.0,
            min: None,
            max: None,
        }
    }

    /// Add one nullable value.
    #[must_use]
    pub fn with_value(mut self, value: Option<f64>) -> Self {
        self.rows += 1;
        if let Some(value) = value {
            self.non_null += 1;
            self.sum += value;
            self.min = Some(self.min.map_or(value, |current| current.min(value)));
            self.max = Some(self.max.map_or(value, |current| current.max(value)));
        }
        self
    }

    /// Merge another partial without revisiting raw rows.
    #[must_use]
    pub fn merge(self, other: Self) -> Self {
        Self {
            rows: self.rows + other.rows,
            non_null: self.non_null + other.non_null,
            sum: self.sum + other.sum,
            min: merge_min(self.min, other.min),
            max: merge_max(self.max, other.max),
        }
    }

    /// Total input rows, including null values.
    #[must_use]
    pub fn rows(self) -> u64 {
        self.rows
    }

    /// Non-null input values.
    #[must_use]
    pub fn non_null(self) -> u64 {
        self.non_null
    }

    /// Sum of non-null values.
    #[must_use]
    pub fn sum(self) -> f64 {
        self.sum
    }

    /// Minimum non-null value.
    #[must_use]
    pub fn min(self) -> Option<f64> {
        self.min
    }

    /// Maximum non-null value.
    #[must_use]
    pub fn max(self) -> Option<f64> {
        self.max
    }

    /// Mean of non-null values.
    #[must_use]
    pub fn mean(self) -> Option<f64> {
        (self.non_null != 0).then(|| self.sum / self.non_null as f64)
    }
}

fn merge_min(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn merge_max(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

/// Stable fixed-window bucket and grouping identity.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PartialKey {
    bucket_start: i64,
    group: Arc<[Arc<str>]>,
}

impl PartialKey {
    /// Construct a bucket key from canonical group values.
    #[must_use]
    pub fn new<I, S>(bucket_start: i64, group: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Arc<str>>,
    {
        Self {
            bucket_start,
            group: group.into_iter().map(Into::into).collect(),
        }
    }

    /// Inclusive bucket start.
    #[must_use]
    pub fn bucket_start(&self) -> i64 {
        self.bucket_start
    }

    /// Canonical group values.
    #[must_use]
    pub fn group(&self) -> &[Arc<str>] {
        &self.group
    }
}

/// Authoritative aggregate state for one exact recipe and source state.
#[derive(Clone, Debug, PartialEq)]
pub struct PartialManifest {
    recipe_id: Arc<str>,
    source_state_id: Arc<str>,
    width: i64,
    origin: i64,
    partials: BTreeMap<PartialKey, AggregatePartial>,
}

impl PartialManifest {
    /// Validate and capture one exact partial-state manifest.
    pub fn try_new(
        recipe_id: impl Into<Arc<str>>,
        source_state_id: impl Into<Arc<str>>,
        width: i64,
        origin: i64,
        partials: BTreeMap<PartialKey, AggregatePartial>,
    ) -> Result<Self> {
        let recipe_id = recipe_id.into();
        let source_state_id = source_state_id.into();
        if recipe_id.is_empty() {
            return Err(DataFusionError::Plan(
                "partial manifest recipe identity must not be empty".to_owned(),
            ));
        }
        if source_state_id.is_empty() {
            return Err(DataFusionError::Plan(
                "partial manifest source-state identity must not be empty".to_owned(),
            ));
        }
        if width <= 0 {
            return Err(DataFusionError::Plan(format!(
                "partial manifest width must be positive, got {width}"
            )));
        }
        for key in partials.keys() {
            validate_bucket_alignment(key, width, origin)?;
        }
        Ok(Self {
            recipe_id,
            source_state_id,
            width,
            origin,
            partials,
        })
    }

    /// Semantic recipe identity.
    #[must_use]
    pub fn recipe_id(&self) -> &str {
        &self.recipe_id
    }

    /// Exact logical source-state identity.
    #[must_use]
    pub fn source_state_id(&self) -> &str {
        &self.source_state_id
    }

    /// Fixed bucket width.
    #[must_use]
    pub fn width(&self) -> i64 {
        self.width
    }

    /// Fixed bucket origin.
    #[must_use]
    pub fn origin(&self) -> i64 {
        self.origin
    }

    /// Exact bucket/group partials.
    #[must_use]
    pub fn partials(&self) -> &BTreeMap<PartialKey, AggregatePartial> {
        &self.partials
    }

    fn fold_to(&self, width: i64, origin: i64) -> Result<Self> {
        if width <= self.width || width.rem_euclid(self.width) != 0 {
            return Err(DataFusionError::Plan(format!(
                "resolution {width} is not a coarser multiple of partial width {}",
                self.width
            )));
        }
        let origin_delta = origin.checked_sub(self.origin).ok_or_else(|| {
            DataFusionError::Plan("resolution origin alignment overflowed".to_owned())
        })?;
        if origin_delta.rem_euclid(self.width) != 0 {
            return Err(DataFusionError::Plan(format!(
                "resolution origin {origin} is incompatible with partial origin {} at width {}",
                self.origin, self.width
            )));
        }
        let mut folded = BTreeMap::new();
        for (key, partial) in &self.partials {
            let delta = key.bucket_start().checked_sub(origin).ok_or_else(|| {
                DataFusionError::Plan("coarse bucket alignment overflowed".to_owned())
            })?;
            let bucket_start = origin
                .checked_add(delta.div_euclid(width).checked_mul(width).ok_or_else(|| {
                    DataFusionError::Plan("coarse bucket alignment overflowed".to_owned())
                })?)
                .ok_or_else(|| {
                    DataFusionError::Plan("coarse bucket alignment overflowed".to_owned())
                })?;
            let coarse_key = PartialKey {
                bucket_start,
                group: Arc::clone(&key.group),
            };
            _ = folded
                .entry(coarse_key)
                .and_modify(|value: &mut AggregatePartial| *value = value.merge(*partial))
                .or_insert(*partial);
        }
        Self::try_new(
            Arc::clone(&self.recipe_id),
            Arc::clone(&self.source_state_id),
            width,
            origin,
            folded,
        )
    }
}

/// Why raw input is required instead of reusable partial state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawRebuildReason {
    /// No partial state has been published.
    MissingState,
    /// Available state belongs to another semantic recipe.
    RecipeChanged,
    /// Available state belongs to another logical source snapshot.
    SourceStateChanged,
    /// Available resolutions or origins cannot fold to the request.
    IncompatibleResolution,
}

/// Measured work used to satisfy one state request.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PartialWork {
    /// Raw source rows scanned.
    pub raw_rows_scanned: u64,
    /// Fine partial buckets read.
    pub partials_read: u64,
    /// Output partial buckets reused or produced.
    pub partials_written: u64,
}

/// Why fine partial state changed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PartialPatchReason {
    /// New event time strictly beyond previously observed input.
    Append,
    /// Out-of-order input inside the unsealed region.
    UnsealedDisorder,
    /// Explicitly permitted repair at or behind the settled frontier.
    RetroactiveRepair,
    /// Wildcard or other source membership changed.
    MembershipChange,
}

/// Measured bounded work for one fine-state patch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartialPatchWork {
    /// Explicit reason for rebuilding these buckets.
    pub reason: PartialPatchReason,
    /// Raw rows used to rebuild dirty partials.
    pub raw_rows_scanned: u64,
    /// Dirty bucket/group keys examined.
    pub buckets_touched: u64,
    /// Bucket/group partials inserted or replaced.
    pub buckets_upserted: u64,
    /// Bucket/group partials removed.
    pub buckets_removed: u64,
    /// Stale derived resolutions discarded.
    pub invalidated_resolutions: u64,
}

/// How a requested aggregate state was resolved.
#[derive(Clone, Debug, PartialEq)]
pub enum PartialResolution {
    /// Exact manifest reuse; no source or partial buckets were scanned.
    Exact {
        /// Reused manifest.
        manifest: PartialManifest,
        /// Assertable work counters.
        work: PartialWork,
    },
    /// A compatible finer manifest was folded without raw input.
    Folded {
        /// Finer source resolution.
        source_width: i64,
        /// Newly published coarse manifest.
        manifest: PartialManifest,
        /// Assertable work counters.
        work: PartialWork,
    },
    /// Raw input is required for an explicit reason.
    NeedsRaw {
        /// Cache miss/rebuild reason.
        reason: RawRebuildReason,
    },
}

/// In-memory manifest authority used to prove state-selection semantics.
#[derive(Debug, Default)]
pub struct PartialStateStore {
    manifests: BTreeMap<i64, PartialManifest>,
}

impl PartialStateStore {
    /// Publish one validated manifest as authoritative for its resolution.
    pub fn publish(&mut self, manifest: PartialManifest) {
        _ = self.manifests.insert(manifest.width(), manifest);
    }

    /// Apply an explicit dirty-bucket patch to one authoritative fine
    /// resolution and invalidate stale derived resolutions.
    pub fn apply_patch(
        &mut self,
        recipe_id: &str,
        prior_source_state_id: &str,
        new_source_state_id: &str,
        width: i64,
        updates: BTreeMap<PartialKey, Option<AggregatePartial>>,
        reason: PartialPatchReason,
        raw_rows_scanned: u64,
    ) -> Result<PartialPatchWork> {
        if new_source_state_id.is_empty() {
            return Err(DataFusionError::Plan(
                "new partial source-state identity must not be empty".to_owned(),
            ));
        }
        if new_source_state_id == prior_source_state_id {
            return Err(DataFusionError::Plan(
                "partial patch must advance source-state identity".to_owned(),
            ));
        }
        if updates.is_empty() && raw_rows_scanned != 0 {
            return Err(DataFusionError::Plan(
                "zero-bucket partial patch must not report raw rows scanned".to_owned(),
            ));
        }
        let manifest = self.manifests.get(&width).ok_or_else(|| {
            DataFusionError::Plan(format!("cannot patch missing partial resolution {width}"))
        })?;
        if manifest.recipe_id() != recipe_id {
            return Err(DataFusionError::Plan(format!(
                "cannot patch recipe '{recipe_id}' from manifest recipe '{}'",
                manifest.recipe_id()
            )));
        }
        if manifest.source_state_id() != prior_source_state_id {
            return Err(DataFusionError::Plan(format!(
                "cannot patch source state '{prior_source_state_id}' from manifest source state '{}'",
                manifest.source_state_id()
            )));
        }
        for key in updates.keys() {
            validate_bucket_alignment(key, manifest.width(), manifest.origin())?;
        }

        if updates.is_empty() {
            for manifest in self.manifests.values_mut().filter(|manifest| {
                manifest.recipe_id() == recipe_id
                    && manifest.source_state_id() == prior_source_state_id
            }) {
                manifest.source_state_id = Arc::from(new_source_state_id);
            }
            return Ok(PartialPatchWork {
                reason,
                raw_rows_scanned,
                buckets_touched: 0,
                buckets_upserted: 0,
                buckets_removed: 0,
                invalidated_resolutions: 0,
            });
        }

        let stale_resolutions = self
            .manifests
            .iter()
            .filter_map(|(resolution, manifest)| {
                (*resolution != width
                    && manifest.recipe_id() == recipe_id
                    && manifest.source_state_id() == prior_source_state_id)
                    .then_some(*resolution)
            })
            .collect::<Vec<_>>();
        for resolution in &stale_resolutions {
            _ = self.manifests.remove(resolution);
        }

        let buckets_touched = updates.len() as u64;
        let mut buckets_upserted = 0;
        let mut buckets_removed = 0;
        let manifest = self
            .manifests
            .get_mut(&width)
            .expect("validated manifest remains");
        for (key, value) in updates {
            match value {
                Some(value) => {
                    _ = manifest.partials.insert(key, value);
                    buckets_upserted += 1;
                }
                None => {
                    if manifest.partials.remove(&key).is_some() {
                        buckets_removed += 1;
                    }
                }
            }
        }
        manifest.source_state_id = Arc::from(new_source_state_id);
        Ok(PartialPatchWork {
            reason,
            raw_rows_scanned,
            buckets_touched,
            buckets_upserted,
            buckets_removed,
            invalidated_resolutions: stale_resolutions.len() as u64,
        })
    }

    /// Resolve exact state or fold the closest compatible finer resolution.
    pub fn resolve(
        &mut self,
        recipe_id: &str,
        source_state_id: &str,
        width: i64,
        origin: i64,
    ) -> Result<PartialResolution> {
        if width <= 0 {
            return Err(DataFusionError::Plan(format!(
                "requested partial width must be positive, got {width}"
            )));
        }

        if let Some(manifest) = self.manifests.get(&width)
            && manifest.recipe_id() == recipe_id
            && manifest.source_state_id() == source_state_id
            && manifest.origin() == origin
        {
            return Ok(PartialResolution::Exact {
                manifest: manifest.clone(),
                work: PartialWork::default(),
            });
        }

        let candidate = self
            .manifests
            .values()
            .filter(|manifest| {
                manifest.recipe_id() == recipe_id
                    && manifest.source_state_id() == source_state_id
                    && manifest.width() < width
                    && width.rem_euclid(manifest.width()) == 0
                    && origin
                        .checked_sub(manifest.origin())
                        .is_some_and(|delta| delta.rem_euclid(manifest.width()) == 0)
            })
            .max_by_key(|manifest| manifest.width())
            .cloned();
        if let Some(candidate) = candidate {
            let partials_read = candidate.partials().len() as u64;
            let manifest = candidate.fold_to(width, origin)?;
            let work = PartialWork {
                raw_rows_scanned: 0,
                partials_read,
                partials_written: manifest.partials().len() as u64,
            };
            self.publish(manifest.clone());
            return Ok(PartialResolution::Folded {
                source_width: candidate.width(),
                manifest,
                work,
            });
        }

        let reason = if self.manifests.is_empty() {
            RawRebuildReason::MissingState
        } else if self
            .manifests
            .values()
            .all(|manifest| manifest.recipe_id() != recipe_id)
        {
            RawRebuildReason::RecipeChanged
        } else if self.manifests.values().all(|manifest| {
            manifest.recipe_id() != recipe_id || manifest.source_state_id() != source_state_id
        }) {
            RawRebuildReason::SourceStateChanged
        } else {
            RawRebuildReason::IncompatibleResolution
        };
        Ok(PartialResolution::NeedsRaw { reason })
    }
}

fn validate_bucket_alignment(key: &PartialKey, width: i64, origin: i64) -> Result<()> {
    let delta = key
        .bucket_start()
        .checked_sub(origin)
        .ok_or_else(|| DataFusionError::Plan("partial bucket alignment overflowed".to_owned()))?;
    if delta.rem_euclid(width) != 0 {
        return Err(DataFusionError::Plan(format!(
            "partial bucket start {} is not aligned to width {width} and origin {origin}",
            key.bucket_start()
        )));
    }
    Ok(())
}
