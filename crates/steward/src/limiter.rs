// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Limiter: enforce a `rate-limit` node's policy against ephemeral state.
//!
//! See `docs/rate-limiter-design.md`.  The split of responsibilities is the
//! whole point of the design:
//!
//! - **Policy lives in the pond.**  A `rate-limit` factory node
//!   (`provider::factory::rate_limit`) declares the budget; it is versioned and
//!   replicated like any other config.
//! - **State lives in the control table.**  The sliding window is written under
//!   the raw config key `limiter:<path>`, which is per-replica and disposable
//!   (Decision L1).  Two replicas spend against different remotes from
//!   different network positions, so a replicated counter would be actively
//!   wrong; and a window is worthless after a few hours, so nothing is lost by
//!   making it disposable.
//! - **A limiter never writes the pond.**  It governs actions, it does not
//!   participate in them.
//!
//! Usage is `open` → (`check`, act, `record`)* → `commit`:
//!
//! ```ignore
//! let mut bytes = Limiter::open(&mut ship, "/sys/limits/backup-bytes", LimitUnit::Bytes).await?;
//! for blob in blobs {
//!     bytes.check(blob.len() as u64)?;   // before the action
//!     transfer(blob).await?;
//!     bytes.record(blob.len() as u64);   // after it succeeded
//! }
//! bytes.commit(ship.control_table_mut()).await?;   // exactly one control write
//! ```
//!
//! `check` before and `record` after means an action that fails is never
//! charged, and an action is never started that the budget cannot cover.
//!
//! # Cost discipline (Decision L2)
//!
//! The control table is a Delta table, so every write is a Delta commit that
//! takes the control write lock.  A limiter therefore performs **one control
//! read at `open` and at most one control write at `commit`** -- never one per
//! governed operation.  Charges accumulate in memory in between.

use provider::factory::rate_limit::{LimitUnit, RateSpec, spec_from_config_bytes};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

use crate::{ControlTable, PondUserMetadata, Ship, StewardError};

/// Raw config key prefix for limiter windows in the control table.
pub const LIMITER_KEY_PREFIX: &str = "limiter:";

/// The control key a limiter at `path` stores its window under.
#[must_use]
pub fn limiter_key(path: &str) -> String {
    format!("{LIMITER_KEY_PREFIX}{path}")
}

// ============================================================================
// Errors
// ============================================================================

/// Failures from binding or consulting a limiter.
#[derive(Debug, Clone, thiserror::Error)]
pub enum LimiterError {
    #[error("limiter `{path}` not found in the pond: {reason}")]
    NotFound { path: String, reason: String },

    #[error("`{path}` is not a usable rate-limit node: {reason}")]
    NotRateLimit { path: String, reason: String },

    #[error(
        "limiter `{path}` is configured in `{configured}` but the caller spends `{expected}`; \
         a limiter must govern the quantity its caller actually consumes"
    )]
    UnitMismatch {
        path: String,
        configured: LimitUnit,
        expected: LimitUnit,
    },

    #[error(
        "rate limit `{path}` exceeded: {used}/{limit} {unit} used in the last {window_secs}s, \
         request for {requested} {unit} denied; retry in {retry_after_secs}s"
    )]
    Exceeded {
        path: String,
        unit: LimitUnit,
        used: u64,
        limit: u64,
        requested: u64,
        window_secs: u64,
        retry_after_secs: u64,
    },

    #[error("limiter `{path}` control state error: {reason}")]
    Control { path: String, reason: String },
}

impl LimiterError {
    /// True for the one variant that means "the budget said no", as opposed to
    /// a misconfiguration.  Callers surface these differently: an exhausted
    /// budget is an expected, retryable condition; a unit mismatch is a bug in
    /// the configuration.
    #[must_use]
    pub fn is_exceeded(&self) -> bool {
        matches!(self, LimiterError::Exceeded { .. })
    }
}

impl From<LimiterError> for StewardError {
    fn from(e: LimiterError) -> Self {
        // An exhausted budget is a throttle, not a fault: the pond is healthy
        // and the action is safe to retry later.  Everything else -- a missing
        // node, a wrong unit, an unreadable window -- is a misconfiguration
        // that will not fix itself.
        if e.is_exceeded() {
            StewardError::RateLimited(e)
        } else {
            StewardError::Aborted(e.to_string())
        }
    }
}

// ============================================================================
// The sliding window
// ============================================================================

/// Number of buckets a window is divided into (Decision L5).
///
/// Bucketing bounds the serialized state by bucket count rather than by
/// operation count: a limiter charged thousands of times per push still stores
/// a fixed-size window.  The cost is up to one bucket's width of
/// over-permissiveness at the leading edge, which for a cost guard is well
/// inside the noise.
fn bucket_count(window: Duration) -> i64 {
    match window.as_secs() {
        86400 => 96, // 15-minute buckets over a day
        3600 => 60,  // 1-minute buckets over an hour
        60 => 60,    // 1-second buckets over a minute
        _ => 20,     // 50ms buckets over a second
    }
}

fn bucket_micros(window: Duration) -> i64 {
    let total = i64::try_from(window.as_micros()).unwrap_or(i64::MAX);
    (total / bucket_count(window)).max(1)
}

fn window_micros(window: Duration) -> i64 {
    i64::try_from(window.as_micros()).unwrap_or(i64::MAX)
}

/// A bucketed sliding window: bucket start (epoch micros) -> amount charged.
///
/// Persisted as an array of `[start, amount]` pairs rather than a JSON object,
/// because JSON object keys must be strings and a stringified `i64` neither
/// round-trips through `serde_json` nor reads well in a control dump.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Buckets {
    buckets: BTreeMap<i64, u64>,
}

impl Serialize for Buckets {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.buckets.iter().map(|(start, amount)| (*start, *amount)))
    }
}

impl<'de> Deserialize<'de> for Buckets {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let pairs = Vec::<(i64, u64)>::deserialize(deserializer)?;
        Ok(Buckets {
            buckets: pairs.into_iter().collect(),
        })
    }
}

impl Buckets {
    fn add(&mut self, now_us: i64, bucket_us: i64, amount: u64) {
        let start = now_us - now_us.rem_euclid(bucket_us);
        let slot = self.buckets.entry(start).or_insert(0);
        *slot = slot.saturating_add(amount);
    }

    /// Total charged within `span` ending at `now_us`.
    ///
    /// A bucket counts if any part of it lies inside the span.  Buckets
    /// stamped in the future (a backwards clock step) are counted rather than
    /// ignored, so a clock jump cannot be used to erase history.
    fn sum_since(&self, now_us: i64, span_us: i64) -> u64 {
        let cutoff = now_us.saturating_sub(span_us);
        self.buckets
            .range(cutoff..)
            .map(|(_, v)| *v)
            .fold(0_u64, u64::saturating_add)
    }

    /// Drop buckets entirely older than the window.
    fn prune(&mut self, now_us: i64, window_us: i64, bucket_us: i64) {
        let cutoff = now_us.saturating_sub(window_us).saturating_sub(bucket_us);
        self.buckets.retain(|start, _| *start >= cutoff);
    }

    fn merge(&mut self, other: &Buckets) {
        for (start, amount) in &other.buckets {
            let slot = self.buckets.entry(*start).or_insert(0);
            *slot = slot.saturating_add(*amount);
        }
    }

    /// Every charge held, regardless of age.
    fn total(&self) -> u64 {
        self.buckets
            .values()
            .fold(0_u64, |a, v| a.saturating_add(*v))
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.buckets.values().all(|v| *v == 0)
    }

    /// The oldest live bucket's expiry, used to report when budget frees up.
    fn oldest_start(&self, now_us: i64, window_us: i64) -> Option<i64> {
        let cutoff = now_us.saturating_sub(window_us);
        self.buckets
            .range(cutoff..)
            .find(|(_, v)| **v > 0)
            .map(|(start, _)| *start)
    }
}

/// The persisted form of a limiter's window.
///
/// `unit` and `window_us` are recorded alongside the buckets so that a policy
/// change is detectable: if an operator rewrites a limiter from `MiB/day` to
/// `iops/second`, the stored counts are measuring something else entirely and
/// must be discarded rather than reinterpreted.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredWindow {
    v: u32,
    unit: LimitUnit,
    window_us: i64,
    bucket_us: i64,
    #[serde(default)]
    buckets: Buckets,
}

const STORED_WINDOW_VERSION: u32 = 1;

// ============================================================================
// Limiter
// ============================================================================

/// A bound limiter: a policy from the pond plus its window from control.
#[derive(Debug, Clone)]
pub struct Limiter {
    path: String,
    spec: RateSpec,
    /// The window as loaded from control.
    loaded: Buckets,
    /// Charges made this session, not yet persisted.
    pending: Buckets,
    dirty: bool,
}

/// A limiter's current position, for status reporting (Decision L9).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimiterState {
    pub path: String,
    pub unit: LimitUnit,
    pub used: u64,
    pub limit: u64,
    pub burst: u64,
    pub window: Duration,
    /// How long until any budget frees up, if currently saturated.
    pub reset_in: Duration,
}

impl Limiter {
    /// Bind to the limiter at `path`, declaring the unit this caller spends.
    ///
    /// Resolves `path` in the pond, parses its `rate-limit` policy, **checks
    /// the declared unit against the configured one** (Decision L10), and
    /// loads the window from the control table.
    ///
    /// The unit check is the contract: a caller states the dimension it
    /// consumes and the bind fails if the node governs a different one.  Scale
    /// and period are deliberately outside the contract, so an operator can
    /// retune `10 MiB/day` to `2 GiB/hour` without touching code.
    ///
    /// A missing control key is not an error: the window starts empty and the
    /// full budget is available (Decision L6, and Open Question O1).
    pub async fn open(
        ship: &mut Ship,
        path: &str,
        expect: LimitUnit,
    ) -> Result<Self, LimiterError> {
        let config = read_node_bytes(ship, path).await?;
        let spec =
            spec_from_config_bytes(&config).map_err(|reason| LimiterError::NotRateLimit {
                path: path.to_string(),
                reason,
            })?;
        Self::bind(ship.control_table(), path, spec, expect).await
    }

    /// Bind against an already-resolved policy.  Split out from [`Self::open`]
    /// so the unit check and window load can be exercised without a pond.
    pub async fn bind(
        control: &ControlTable,
        path: &str,
        spec: RateSpec,
        expect: LimitUnit,
    ) -> Result<Self, LimiterError> {
        if spec.unit != expect {
            return Err(LimiterError::UnitMismatch {
                path: path.to_string(),
                configured: spec.unit,
                expected: expect,
            });
        }

        let raw = control
            .raw_config_get(&limiter_key(path))
            .await
            .map_err(|e| LimiterError::Control {
                path: path.to_string(),
                reason: e.to_string(),
            })?;

        let loaded = match raw {
            // No key: fresh pond, or the control table was rebuilt.  Start
            // empty rather than refusing to run (Decision L6).
            None => Buckets::default(),
            Some(text) if text.is_empty() => Buckets::default(),
            Some(text) => match serde_json::from_str::<StoredWindow>(&text) {
                // A stored window measured in a different unit or over a
                // different period is measuring something else; discard it
                // rather than reinterpret counts that no longer mean what
                // they meant when they were written.
                Ok(w)
                    if w.v == STORED_WINDOW_VERSION
                        && w.unit == spec.unit
                        && w.window_us == window_micros(spec.window) =>
                {
                    w.buckets
                }
                Ok(_) => Buckets::default(),
                Err(e) => {
                    log::warn!(
                        "limiter `{path}`: discarding unreadable control window ({e}); \
                         starting from an empty window"
                    );
                    Buckets::default()
                }
            },
        };

        Ok(Self {
            path: path.to_string(),
            spec,
            loaded,
            pending: Buckets::default(),
            dirty: false,
        })
    }

    /// The pond path this limiter was bound from.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The policy in force.
    #[must_use]
    pub fn spec(&self) -> &RateSpec {
        &self.spec
    }

    /// Would charging `amount` exceed the policy right now?
    ///
    /// Pure: no I/O and no mutation.  Call before the governed action.
    pub fn check(&self, amount: u64) -> Result<(), LimiterError> {
        self.check_at(now_micros(), amount)
    }

    /// [`Self::check`] against an explicit clock, for deterministic tests.
    pub fn check_at(&self, now_us: i64, amount: u64) -> Result<(), LimiterError> {
        let window_us = window_micros(self.spec.window);

        // A single request larger than the whole budget can never be admitted;
        // say so directly rather than reporting it as transient.
        if amount > self.spec.amount {
            return Err(self.exceeded(now_us, self.used_at(now_us, window_us), amount, window_us));
        }

        // Constraint 1: the sliding-window total.
        let used = self.used_at(now_us, window_us);
        if used.saturating_add(amount) > self.spec.amount {
            return Err(self.exceeded(now_us, used, amount, window_us));
        }

        // Constraint 2: the burst allowance.  A burst is `spec.burst` base
        // units spendable faster than the smoothed rate, so it is enforced
        // over the time it takes to earn that much at the smoothed rate.  When
        // `burst == amount` (the default) this window equals the full window
        // and the constraint coincides with constraint 1 -- i.e. a pure
        // sliding window with no extra allowance.
        if self.spec.burst < self.spec.amount {
            let burst_span = burst_span_micros(&self.spec, window_us);
            let burst_used = self.used_at(now_us, burst_span);
            if burst_used.saturating_add(amount) > self.spec.burst {
                return Err(LimiterError::Exceeded {
                    path: self.path.clone(),
                    unit: self.spec.unit,
                    used: burst_used,
                    limit: self.spec.burst,
                    requested: amount,
                    window_secs: (burst_span / 1_000_000).max(1) as u64,
                    retry_after_secs: self.retry_after_secs(now_us, burst_span),
                });
            }
        }

        Ok(())
    }

    /// Charge `amount` against the in-memory window.
    ///
    /// Call only after a successful [`Self::check`] **and** a successful
    /// action, so failed work is never billed.
    pub fn record(&mut self, amount: u64) {
        self.record_at(now_micros(), amount);
    }

    /// [`Self::record`] against an explicit clock, for deterministic tests.
    pub fn record_at(&mut self, now_us: i64, amount: u64) {
        if amount == 0 {
            return;
        }
        self.pending
            .add(now_us, bucket_micros(self.spec.window), amount);
        self.dirty = true;
    }

    /// Fold this session's charges into the persisted window.
    ///
    /// Exactly one control write, and none at all when nothing was charged
    /// (Decision L2).
    pub async fn commit(&mut self, control: &mut ControlTable) -> Result<(), LimiterError> {
        self.commit_at(control, now_micros()).await
    }

    /// [`Self::commit`] against an explicit clock, for deterministic tests.
    pub async fn commit_at(
        &mut self,
        control: &mut ControlTable,
        now_us: i64,
    ) -> Result<(), LimiterError> {
        let sample = self.commit_window_at(control, now_us).await?;
        crate::limiter_usage::queue(control, sample.as_slice()).await;
        Ok(())
    }

    /// Persist the window and report what was spent, without queueing it.
    ///
    /// Split from [`Self::commit_at`] so a [`LimiterSet`] can persist several
    /// windows and then queue all their samples in **one** control write
    /// rather than one per limiter (Decision L2).
    async fn commit_window_at(
        &mut self,
        control: &mut ControlTable,
        now_us: i64,
    ) -> Result<Option<crate::limiter_usage::UsageSample>, LimiterError> {
        if !self.dirty {
            return Ok(None);
        }
        let spent = self.spent();
        let window_us = window_micros(self.spec.window);
        let bucket_us = bucket_micros(self.spec.window);

        let mut merged = self.loaded.clone();
        merged.merge(&self.pending);
        merged.prune(now_us, window_us, bucket_us);

        let stored = StoredWindow {
            v: STORED_WINDOW_VERSION,
            unit: self.spec.unit,
            window_us,
            bucket_us,
            buckets: merged.clone(),
        };
        let text = serde_json::to_string(&stored).map_err(|e| LimiterError::Control {
            path: self.path.clone(),
            reason: format!("failed to serialize window: {e}"),
        })?;
        control
            .raw_config_set(&limiter_key(&self.path), &text)
            .await
            .map_err(|e| LimiterError::Control {
                path: self.path.clone(),
                reason: e.to_string(),
            })?;

        self.loaded = merged;
        self.pending = Buckets::default();
        self.dirty = false;

        if spent == 0 {
            return Ok(None);
        }
        // Reported after the merge, so `used` is the window total including
        // this spending -- the number a headroom alert wants.
        let state = self.state_at(now_us);
        Ok(Some(crate::limiter_usage::UsageSample {
            at_us: now_us,
            limiter: state.path,
            unit: state.unit.as_str().to_string(),
            amount: spent,
            used: state.used,
            limit: state.limit,
            window_us,
        }))
    }

    /// Units charged through this limiter since it was opened, not yet
    /// reflected in any emitted metric.
    #[must_use]
    pub fn spent(&self) -> u64 {
        self.pending.total()
    }

    /// Current position, for `pond status` (Decision L9).
    #[must_use]
    pub fn state(&self) -> LimiterState {
        self.state_at(now_micros())
    }

    /// [`Self::state`] against an explicit clock, for deterministic tests.
    #[must_use]
    pub fn state_at(&self, now_us: i64) -> LimiterState {
        let window_us = window_micros(self.spec.window);
        LimiterState {
            path: self.path.clone(),
            unit: self.spec.unit,
            used: self.used_at(now_us, window_us),
            limit: self.spec.amount,
            burst: self.spec.burst,
            window: self.spec.window,
            reset_in: Duration::from_secs(self.retry_after_secs(now_us, window_us)),
        }
    }

    fn used_at(&self, now_us: i64, span_us: i64) -> u64 {
        self.loaded
            .sum_since(now_us, span_us)
            .saturating_add(self.pending.sum_since(now_us, span_us))
    }

    fn exceeded(&self, now_us: i64, used: u64, requested: u64, window_us: i64) -> LimiterError {
        LimiterError::Exceeded {
            path: self.path.clone(),
            unit: self.spec.unit,
            used,
            limit: self.spec.amount,
            requested,
            window_secs: (window_us / 1_000_000).max(1) as u64,
            retry_after_secs: self.retry_after_secs(now_us, window_us),
        }
    }

    /// Seconds until the oldest live charge falls out of `span_us`.
    fn retry_after_secs(&self, now_us: i64, span_us: i64) -> u64 {
        let oldest = self
            .loaded
            .oldest_start(now_us, span_us)
            .into_iter()
            .chain(self.pending.oldest_start(now_us, span_us))
            .min();
        match oldest {
            None => 0,
            Some(start) => {
                let expires_at = start.saturating_add(span_us);
                let remaining = expires_at.saturating_sub(now_us).max(0);
                // Round up, and never report 0 while a charge is still live:
                // a 0 would invite a hot retry loop, which is the behavior we
                // are here to stop.
                let secs = remaining.div_euclid(1_000_000)
                    + i64::from(remaining.rem_euclid(1_000_000) != 0);
                secs.max(1) as u64
            }
        }
    }
}

/// The span over which the burst allowance is enforced: the time it takes to
/// earn `burst` base units at the smoothed rate `amount / window`.
fn burst_span_micros(spec: &RateSpec, window_us: i64) -> i64 {
    if spec.amount == 0 {
        return window_us;
    }
    let span = (window_us as i128 * spec.burst as i128) / spec.amount as i128;
    (span as i64).clamp(1, window_us)
}

/// Wall clock in epoch microseconds, matching the control table's `ts_micros`
/// convention (Open Question O3).
#[must_use]
pub fn now_micros() -> i64 {
    chrono::Utc::now().timestamp_micros()
}

// ============================================================================
// LimiterSet
// ============================================================================

/// The limiters governing one activity, one per dimension it spends in.
///
/// A single action usually costs in more than one currency -- a blob push
/// spends both bytes and requests -- and each dimension is governed by its own
/// node.  `LimiterSet` binds them together so the consumer states a dimension
/// at each charge rather than juggling `Option<Limiter>` locals.
///
/// The dimension passed to [`Self::check`] and [`Self::record`] is looked up,
/// not trusted: an unbound dimension is simply ungoverned, which is what an
/// absent `limits` key means.
///
/// Binding happens up front so a misconfiguration -- a missing node, or a node
/// governing the wrong unit -- fails before any remote I/O rather than halfway
/// through a transfer.
#[derive(Debug, Default)]
pub struct LimiterSet {
    bound: Vec<Limiter>,
}

impl LimiterSet {
    /// An empty set: every dimension ungoverned.  This is today's behavior for
    /// an attachment that configures no limits, and it costs nothing -- no
    /// pond read, no control read, no control write.
    #[must_use]
    pub fn unlimited() -> Self {
        Self::default()
    }

    /// Bind every `(dimension, pond path)` pair.
    ///
    /// Each limiter is opened with its dimension as the caller's declaration,
    /// so the node's configured `unit` is checked against what the operator
    /// said it governs (Decision L10).
    ///
    /// # Errors
    ///
    /// Returns [`LimiterError::NotFound`] if a named node is missing,
    /// [`LimiterError::NotRateLimit`] if it is not a `rate-limit` node,
    /// [`LimiterError::UnitMismatch`] if its unit disagrees with the declared
    /// dimension, and [`LimiterError::Control`] if the window cannot be read
    /// or a dimension is bound twice.
    pub async fn open(
        ship: &mut Ship,
        limits: &[(LimitUnit, String)],
    ) -> Result<Self, LimiterError> {
        let mut bound: Vec<Limiter> = Vec::with_capacity(limits.len());
        for (unit, path) in limits {
            if bound.iter().any(|l| l.spec().unit == *unit) {
                return Err(LimiterError::Control {
                    path: path.clone(),
                    reason: format!("dimension `{unit}` is bound more than once"),
                });
            }
            bound.push(Limiter::open(ship, path, *unit).await?);
        }
        Ok(Self { bound })
    }

    /// Nothing is governed, so no charge can ever be refused.
    #[must_use]
    pub fn is_unlimited(&self) -> bool {
        self.bound.is_empty()
    }

    fn get(&self, unit: LimitUnit) -> Option<&Limiter> {
        self.bound.iter().find(|l| l.spec().unit == unit)
    }

    fn get_mut(&mut self, unit: LimitUnit) -> Option<&mut Limiter> {
        self.bound.iter_mut().find(|l| l.spec().unit == unit)
    }

    /// Would spending `amount` of `unit` exceed its policy?  Pure: no I/O.
    ///
    /// An unbound dimension always admits.
    ///
    /// # Errors
    ///
    /// Returns [`LimiterError::Exceeded`] when the charge would breach the
    /// budget or the burst allowance.
    pub fn check(&self, unit: LimitUnit, amount: u64) -> Result<(), LimiterError> {
        match self.get(unit) {
            Some(l) => l.check(amount),
            None => Ok(()),
        }
    }

    /// Charge `amount` of `unit` after the action succeeded.  Pure: no I/O.
    pub fn record(&mut self, unit: LimitUnit, amount: u64) {
        if let Some(l) = self.get_mut(unit) {
            l.record(amount);
        }
    }

    /// Persist every dirty window: at most one control write per bound
    /// limiter, and none at all for a set that spent nothing.
    ///
    /// # Errors
    ///
    /// Returns [`LimiterError::Control`] if a window cannot be written.
    pub async fn commit(&mut self, control: &mut ControlTable) -> Result<(), LimiterError> {
        self.commit_at(control, now_micros()).await
    }

    /// [`Self::commit`] against an explicit clock, for deterministic tests.
    pub async fn commit_at(
        &mut self,
        control: &mut ControlTable,
        now_us: i64,
    ) -> Result<(), LimiterError> {
        let mut samples = Vec::new();
        for l in &mut self.bound {
            if let Some(sample) = l.commit_window_at(control, now_us).await? {
                samples.push(sample);
            }
        }

        // Queue for the pond in one write, so spending becomes a durable
        // metric and not just enforcement state that a control rebuild erases
        // (Decision L12).
        crate::limiter_usage::queue(control, &samples).await;
        Ok(())
    }

    /// Current position of every bound limiter, for `pond status`.
    #[must_use]
    pub fn states(&self) -> Vec<LimiterState> {
        self.bound.iter().map(Limiter::state).collect()
    }
}

/// Read a pond node's bytes at `path`.
async fn read_node_bytes(ship: &mut Ship, path: &str) -> Result<Vec<u8>, LimiterError> {
    let meta = PondUserMetadata::new(vec!["internal".to_string(), "limiter-open".to_string()]);
    let tx = ship
        .begin_read(&meta)
        .await
        .map_err(|e| LimiterError::Control {
            path: path.to_string(),
            reason: format!("begin read: {e}"),
        })?;

    let result = async {
        let root = tx.root().await.map_err(|e| LimiterError::NotFound {
            path: path.to_string(),
            reason: format!("cannot open pond root: {e}"),
        })?;
        let reader = root
            .async_reader_path(path)
            .await
            .map_err(|e| LimiterError::NotFound {
                path: path.to_string(),
                reason: e.to_string(),
            })?;
        tinyfs::buffer_helpers::read_all_to_vec(reader)
            .await
            .map_err(|e| LimiterError::NotFound {
                path: path.to_string(),
                reason: format!("read: {e}"),
            })
    }
    .await;

    // The read transaction must be closed either way; its outcome does not
    // change the limiter's.
    let _ = tx.commit().await;
    result
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use provider::factory::rate_limit::RateLimitConfig;

    const HOUR_US: i64 = 3_600 * 1_000_000;
    const DAY_US: i64 = 86_400 * 1_000_000;

    fn spec(unit: &str, limit: f64, burst: Option<f64>) -> RateSpec {
        RateLimitConfig {
            unit: unit.to_string(),
            limit,
            burst,
        }
        .resolve()
        .expect("test spec should resolve")
    }

    /// A limiter with an empty window, without touching a control table.
    fn limiter(path: &str, spec: RateSpec) -> Limiter {
        Limiter {
            path: path.to_string(),
            spec,
            loaded: Buckets::default(),
            pending: Buckets::default(),
            dirty: false,
        }
    }

    // -------- the unit contract (Decision L10) --------

    #[test]
    fn unit_mismatch_is_detected_before_any_work() {
        // Binding an ops limiter as bytes must fail: the whole point is that a
        // limiter governs the quantity its caller actually consumes.
        let s = spec("iops/second", 5.0, None);
        assert_eq!(s.unit, LimitUnit::Ops);
        let err = LimiterError::UnitMismatch {
            path: "/sys/limits/x".to_string(),
            configured: s.unit,
            expected: LimitUnit::Bytes,
        };
        let text = err.to_string();
        assert!(text.contains("/sys/limits/x"), "{text}");
        assert!(text.contains("ops"), "{text}");
        assert!(text.contains("bytes"), "{text}");
        assert!(!err.is_exceeded());
    }

    #[test]
    fn scale_and_period_are_outside_the_contract() {
        // Both are byte limiters, so both bind for a byte-spending caller even
        // though the operator has retuned the budget entirely.
        assert_eq!(spec("MiB/day", 10.0, None).unit, LimitUnit::Bytes);
        assert_eq!(spec("GiB/hour", 2.0, None).unit, LimitUnit::Bytes);
    }

    // -------- window accounting --------

    #[test]
    fn charges_accumulate_and_the_limit_binds() {
        let mut l = limiter("/l", spec("MiB/day", 10.0, None));
        let t0 = 1_000 * DAY_US;

        assert!(l.check_at(t0, 4 * 1024 * 1024).is_ok());
        l.record_at(t0, 4 * 1024 * 1024);
        assert!(l.check_at(t0, 6 * 1024 * 1024).is_ok());
        l.record_at(t0, 6 * 1024 * 1024);

        // Exactly at the limit: the next base unit is refused.
        assert_eq!(l.state_at(t0).used, 10 * 1024 * 1024);
        let err = l.check_at(t0, 1).expect_err("budget is exhausted");
        assert!(err.is_exceeded(), "{err}");
    }

    #[test]
    fn exactly_the_limit_is_admitted() {
        let l = limiter("/l", spec("MiB/day", 10.0, None));
        let t0 = 1_000 * DAY_US;
        assert!(l.check_at(t0, 10 * 1024 * 1024).is_ok());
    }

    #[test]
    fn a_request_larger_than_the_whole_budget_is_refused() {
        let l = limiter("/l", spec("MiB/day", 10.0, None));
        let t0 = 1_000 * DAY_US;
        assert!(l.check_at(t0, 10 * 1024 * 1024 + 1).is_err());
    }

    #[test]
    fn budget_frees_as_the_window_slides() {
        let mut l = limiter("/l", spec("MiB/day", 10.0, None));
        let t0 = 1_000 * DAY_US;
        l.record_at(t0, 10 * 1024 * 1024);
        assert!(l.check_at(t0, 1).is_err());

        // Half a day later, the charge is still inside the day-long window.
        assert!(l.check_at(t0 + DAY_US / 2, 1).is_err());

        // Past the full window (plus a bucket, since a bucket counts while any
        // part of it is live), the budget is back.
        let later = t0 + DAY_US + 2 * bucket_micros(Duration::from_secs(86400));
        assert!(l.check_at(later, 10 * 1024 * 1024).is_ok());
        assert_eq!(l.state_at(later).used, 0);
    }

    #[test]
    fn partial_slide_frees_partial_budget() {
        let mut l = limiter("/l", spec("MiB/hour", 10.0, None));
        let t0 = 1_000 * DAY_US;
        // Spend half the budget, then again half an hour later.
        l.record_at(t0, 5 * 1024 * 1024);
        l.record_at(t0 + HOUR_US / 2, 5 * 1024 * 1024);
        assert!(l.check_at(t0 + HOUR_US / 2, 1).is_err());

        // An hour after the first charge, only the first has expired.
        let later = t0 + HOUR_US + 2 * bucket_micros(Duration::from_secs(3600));
        assert_eq!(l.state_at(later).used, 5 * 1024 * 1024);
        assert!(l.check_at(later, 5 * 1024 * 1024).is_ok());
        assert!(l.check_at(later, 5 * 1024 * 1024 + 1).is_err());
    }

    #[test]
    fn bucket_rollover_neither_loses_nor_double_counts() {
        let s = spec("ops/minute", 60.0, None);
        let bucket = bucket_micros(s.window);
        let mut l = limiter("/l", s);
        let t0 = 1_000 * DAY_US;

        // One charge in each of 60 consecutive buckets.
        for i in 0..60 {
            l.record_at(t0 + i * bucket, 1);
        }
        assert_eq!(l.state_at(t0 + 59 * bucket).used, 60);
        assert!(l.check_at(t0 + 59 * bucket, 1).is_err());
    }

    #[test]
    fn zero_charges_are_ignored() {
        let mut l = limiter("/l", spec("MiB/day", 10.0, None));
        l.record_at(1_000 * DAY_US, 0);
        assert!(!l.dirty, "a zero charge should not dirty the window");
    }

    // -------- burst --------

    #[test]
    fn default_burst_is_a_pure_sliding_window() {
        // burst == limit: the burst constraint coincides with the window
        // constraint, so the whole budget is spendable instantaneously.
        let s = spec("MiB/day", 10.0, None);
        assert_eq!(s.burst, s.amount);
        let l = limiter("/l", s);
        assert!(l.check_at(1_000 * DAY_US, 10 * 1024 * 1024).is_ok());
    }

    #[test]
    fn a_smaller_burst_smooths_spending() {
        // 10 MiB/day with a 1 MiB burst: the daily total is still 10 MiB, but
        // no more than 1 MiB may be spent in any 1/10th of a day.
        let s = spec("MiB/day", 10.0, Some(1.0));
        assert!(s.burst < s.amount);
        let mut l = limiter("/l", s);
        let t0 = 1_000 * DAY_US;

        assert!(l.check_at(t0, 1024 * 1024).is_ok());
        l.record_at(t0, 1024 * 1024);

        // Within the burst span the allowance is spent, even though the daily
        // budget has 9 MiB left.
        let err = l.check_at(t0, 1).expect_err("burst allowance is spent");
        assert!(err.is_exceeded(), "{err}");
        assert!(l.state_at(t0).used < l.spec.amount);

        // After the burst span (a tenth of a day) it refills.
        assert!(
            l.check_at(t0 + DAY_US / 10 + 2 * 1_000_000, 1024 * 1024)
                .is_ok()
        );
    }

    #[test]
    fn burst_span_is_the_time_to_earn_the_burst() {
        let s = spec("MiB/day", 10.0, Some(1.0));
        assert_eq!(burst_span_micros(&s, DAY_US), DAY_US / 10);
        // Default burst spans the whole window, i.e. no extra constraint.
        let s = spec("MiB/day", 10.0, None);
        assert_eq!(burst_span_micros(&s, DAY_US), DAY_US);
    }

    // -------- diagnostics --------

    #[test]
    fn exceeded_error_names_the_limiter_and_a_retry_time() {
        let mut l = limiter("/sys/limits/backup-bytes", spec("MiB/hour", 10.0, None));
        let t0 = 1_000 * DAY_US;
        l.record_at(t0, 10 * 1024 * 1024);
        let err = l.check_at(t0, 1).unwrap_err();
        let text = err.to_string();
        assert!(text.contains("/sys/limits/backup-bytes"), "{text}");
        assert!(text.contains("bytes"), "{text}");
        assert!(text.contains("retry in"), "{text}");
        match err {
            LimiterError::Exceeded {
                used,
                limit,
                requested,
                retry_after_secs,
                ..
            } => {
                assert_eq!(used, 10 * 1024 * 1024);
                assert_eq!(limit, 10 * 1024 * 1024);
                assert_eq!(requested, 1);
                // Never 0 while saturated: a 0 would invite a hot retry loop.
                assert!(retry_after_secs > 0);
                assert!(retry_after_secs <= 3600);
            }
            other => panic!("expected Exceeded, got {other:?}"),
        }
    }

    #[test]
    fn state_reports_position_for_status_output() {
        let mut l = limiter("/l", spec("MiB/day", 10.0, Some(1.0)));
        let t0 = 1_000 * DAY_US;
        l.record_at(t0, 3 * 1024 * 1024);
        let st = l.state_at(t0);
        assert_eq!(st.path, "/l");
        assert_eq!(st.unit, LimitUnit::Bytes);
        assert_eq!(st.used, 3 * 1024 * 1024);
        assert_eq!(st.limit, 10 * 1024 * 1024);
        assert_eq!(st.burst, 1024 * 1024);
        assert_eq!(st.window, Duration::from_secs(86400));
    }

    #[test]
    fn an_unsaturated_limiter_reports_no_wait() {
        let l = limiter("/l", spec("MiB/day", 10.0, None));
        assert_eq!(l.state_at(1_000 * DAY_US).reset_in, Duration::ZERO);
    }

    // -------- persisted form --------

    #[test]
    fn stored_window_round_trips() {
        let s = spec("MiB/day", 10.0, None);
        let mut buckets = Buckets::default();
        buckets.add(1_000 * DAY_US, bucket_micros(s.window), 4096);
        let stored = StoredWindow {
            v: STORED_WINDOW_VERSION,
            unit: s.unit,
            window_us: window_micros(s.window),
            bucket_us: bucket_micros(s.window),
            buckets: buckets.clone(),
        };
        let text = serde_json::to_string(&stored).unwrap();
        let back: StoredWindow = serde_json::from_str(&text).unwrap();
        assert_eq!(back.buckets, buckets);
        assert_eq!(back.unit, LimitUnit::Bytes);
        assert_eq!(back.window_us, DAY_US);
    }

    #[test]
    fn stored_window_is_bounded_by_bucket_count_not_charge_count() {
        // The reason for bucketing: ten thousand charges must not produce ten
        // thousand entries, or the control write would grow without bound.
        let s = spec("MiB/day", 1024.0, None);
        let bucket = bucket_micros(s.window);
        let mut buckets = Buckets::default();
        let t0 = 1_000 * DAY_US;
        for i in 0..10_000_i64 {
            buckets.add(t0 + (i % 96) * bucket, bucket, 1);
        }
        assert!(
            buckets.buckets.len() <= 96,
            "expected <=96 buckets, got {}",
            buckets.buckets.len()
        );
    }

    #[test]
    fn pruning_drops_only_expired_buckets() {
        let s = spec("MiB/hour", 10.0, None);
        let bucket = bucket_micros(s.window);
        let mut buckets = Buckets::default();
        let t0 = 1_000 * DAY_US;
        buckets.add(t0, bucket, 100);
        buckets.add(t0 + HOUR_US, bucket, 200);
        buckets.prune(t0 + HOUR_US, HOUR_US, bucket);
        assert_eq!(buckets.sum_since(t0 + HOUR_US, HOUR_US), 200 + 100);

        // Well past the window, everything goes.
        buckets.prune(t0 + 10 * HOUR_US, HOUR_US, bucket);
        assert!(buckets.is_empty());
    }

    #[test]
    fn merge_sums_overlapping_buckets() {
        let bucket = 1_000_000;
        let t0 = 1_000 * DAY_US;
        let mut a = Buckets::default();
        a.add(t0, bucket, 5);
        let mut b = Buckets::default();
        b.add(t0, bucket, 7);
        b.add(t0 + bucket, bucket, 2);
        a.merge(&b);
        assert_eq!(a.sum_since(t0 + bucket, 10 * bucket), 14);
    }

    #[test]
    fn a_backwards_clock_cannot_erase_history() {
        // Buckets stamped ahead of `now` are still counted, so stepping the
        // clock back must not hand back budget (Open Question O3).
        let mut l = limiter("/l", spec("MiB/hour", 10.0, None));
        let t0 = 1_000 * DAY_US;
        l.record_at(t0, 10 * 1024 * 1024);
        assert!(l.check_at(t0 - HOUR_US, 1).is_err());
    }

    #[test]
    fn limiter_key_is_namespaced() {
        assert_eq!(limiter_key("/sys/limits/x"), "limiter:/sys/limits/x");
        assert!(limiter_key("/x").starts_with(LIMITER_KEY_PREFIX));
    }

    #[test]
    fn bucket_geometry_matches_the_design() {
        assert_eq!(bucket_count(Duration::from_secs(86400)), 96);
        assert_eq!(bucket_micros(Duration::from_secs(86400)), DAY_US / 96);
        assert_eq!(bucket_count(Duration::from_secs(3600)), 60);
        assert_eq!(bucket_micros(Duration::from_secs(60)), 1_000_000);
        assert_eq!(bucket_micros(Duration::from_secs(1)), 50_000);
    }
}
