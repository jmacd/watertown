// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Charging a [`LimiterSet`] for physical storage traffic.
//!
//! This is the steward-side half of [`sync_store::metered_store`]: that module
//! knows when a request happens but not what a budget is; this one knows what
//! a budget is but must not know about object stores.  The trait between them
//! is deliberately two methods wide.
//!
//! # Why the budget is moved rather than borrowed
//!
//! A meter must be `'static` to be bound to a remote, but a `LimiterSet` is
//! owned by the caller and must be returned so it can be committed to the
//! control table.  So [`metered_op`] takes the set, shares it for the duration
//! of the operation, and puts it back afterwards -- the caller keeps its
//! `&mut` and never sees the difference.
//!
//! # Why a guard, not a scope
//!
//! The budget is bound to the *remote*, by URL, for as long as the guard
//! lives (see [`sync_store::bind_meter`]).  Nothing needs to wrap the work:
//! requests are charged because of where they go, not because of what was
//! executing when they were made.  That is what makes the accounting survive
//! the tasks the Delta layer spawns, and it is why a caller can hold the guard
//! across many small calls -- which is what
//! [`crate::metered_source::MeteredSource`] does.

use std::future::Future;
use std::sync::{Arc, Mutex};

use provider::factory::rate_limit::LimitUnit;
use sync_store::{RemoteKey, StorageMeter};

use crate::StewardError;
use crate::limiter::{LimiterError, LimiterSet};

/// Adapts a shared [`LimiterSet`] to the storage layer's meter.
struct LimiterMeter {
    limits: Arc<Mutex<LimiterSet>>,
    /// The refusal that stopped the operation, if one did.
    ///
    /// The storage layer can only return an `object_store` error, so the
    /// typed [`LimiterError`] would otherwise be flattened into a string by
    /// the time it reached the caller.  Keeping it here preserves
    /// [`StewardError::RateLimited`], which is what tells an operator that
    /// nothing is broken -- a budget simply said no.
    refusal: Mutex<Option<LimiterError>>,
}

impl std::fmt::Debug for LimiterMeter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LimiterMeter")
    }
}

impl StorageMeter for LimiterMeter {
    fn check(&self, ops: u64, bytes: u64) -> Result<(), String> {
        let limits = self
            .limits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (unit, amount) in [(LimitUnit::Ops, ops), (LimitUnit::Bytes, bytes)] {
            if amount == 0 {
                continue;
            }
            if let Err(e) = limits.check(unit, amount) {
                let message = e.to_string();
                *self
                    .refusal
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(e);
                return Err(message);
            }
        }
        Ok(())
    }

    fn record(&self, ops: u64, bytes: u64) {
        let mut limits = self
            .limits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (unit, amount) in [(LimitUnit::Ops, ops), (LimitUnit::Bytes, bytes)] {
            if amount != 0 {
                limits.record(unit, amount);
            }
        }
    }
}

/// A budget bound to one remote for the duration of some work.
///
/// While this value lives, every request any store makes to that remote is
/// charged to `limits` -- including requests made from tasks the storage layer
/// spawned, which is the failure mode an ambient meter had.
pub struct MeterGuard {
    key: RemoteKey,
    shared: Arc<Mutex<LimiterSet>>,
    meter: Arc<LimiterMeter>,
    /// Unbinds the budget when the guard is dropped.
    binding: Option<sync_store::MeterBinding>,
    /// Physical traffic already seen for this remote when the guard was made,
    /// so the guard can report what happened during its own lifetime by
    /// difference.
    observed_at_start: (u64, u64),
}

impl MeterGuard {
    /// Govern the remote at `url` with `limits`, leaving the caller's set
    /// empty until [`Self::finish`] returns it.
    pub fn new(url: &str, limits: &mut LimiterSet) -> Self {
        let key = RemoteKey::new(url);
        let shared = Arc::new(Mutex::new(std::mem::replace(
            limits,
            LimiterSet::unlimited(),
        )));
        let meter = Arc::new(LimiterMeter {
            limits: Arc::clone(&shared),
            refusal: Mutex::new(None),
        });
        let before = sync_store::observed_under(&key);
        let binding = sync_store::bind_meter(&key, Arc::clone(&meter) as Arc<dyn StorageMeter>);

        // Arrears were observed before this guard existed but are charged to
        // it, so the window they are measured against has to start early
        // enough to contain them.  Otherwise settling a debt would read as a
        // charge with no matching measurement.
        let (owed_ops, owed_bytes) = binding.arrears();
        Self {
            key,
            shared,
            meter,
            binding: Some(binding),
            observed_at_start: (
                before.0.saturating_sub(owed_ops),
                before.1.saturating_sub(owed_bytes),
            ),
        }
    }

    /// The refusal that stopped the work, if a budget has said no.
    ///
    /// Peeks rather than takes: a wrapper asks after every failed call, and
    /// the same refusal may be reported to more than one of them.
    pub fn refusal(&self) -> Option<StewardError> {
        self.meter
            .refusal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .map(StewardError::RateLimited)
    }

    /// Return the spending to `limits` and report the refusal, if any, that
    /// stopped the work.
    ///
    /// The spending comes back whether the work succeeded or not, so the
    /// caller can commit the usage either way.
    pub fn finish(mut self, limits: &mut LimiterSet) -> Option<StewardError> {
        // Unbind first: traffic after this point belongs to whoever comes
        // next, and must not land in the numbers reported below.
        drop(self.binding.take());

        let after = sync_store::observed_under(&self.key);
        let observed_ops = after.0.saturating_sub(self.observed_at_start.0);
        let observed_bytes = after.1.saturating_sub(self.observed_at_start.1);

        let refusal = self
            .meter
            .refusal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(self.meter);

        // Put the spending back where the caller left it.  A stream that
        // outlived the guard would still hold a reference; take the contents
        // in place rather than failing, because losing the accounting is
        // worse than losing the allocation.
        *limits = match Arc::try_unwrap(self.shared) {
            Ok(m) => m
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            Err(still_shared) => std::mem::replace(
                &mut *still_shared
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
                LimiterSet::unlimited(),
            ),
        };

        // Recorded after the spending is returned, so the sample the limiter
        // emits carries both numbers for the same operation.
        limits.record_observed(LimitUnit::Ops, observed_ops);
        limits.record_observed(LimitUnit::Bytes, observed_bytes);

        refusal.map(StewardError::RateLimited)
    }
}

/// Run `fut` with every request it makes to the remote at `url` charged to
/// `limits`.
///
/// `limits` is left holding everything the operation spent, whether it
/// succeeded or not, so the caller can commit the usage either way.  A failure
/// caused by a budget is returned as [`StewardError::RateLimited`] rather than
/// as the opaque storage error the object store produced.
pub async fn metered_op<F, T, E>(url: &str, limits: &mut LimiterSet, fut: F) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
    E: From<StewardError>,
{
    let guard = MeterGuard::new(url, limits);
    let outcome = fut.await;
    let refusal = guard.finish(limits);

    match outcome {
        Err(_) if refusal.is_some() => Err(E::from(refusal.expect("checked as Some in the guard"))),
        other => other,
    }
}
