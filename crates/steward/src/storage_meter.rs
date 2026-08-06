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
//! A meter must be `'static` to live in a task-local, but a `LimiterSet` is
//! owned by the caller and must be returned so it can be committed to the
//! control table.  So [`metered_op`] takes the set, shares it for the duration
//! of the operation, and puts it back afterwards -- the caller keeps its
//! `&mut` and never sees the difference.

use std::future::Future;
use std::sync::{Arc, Mutex};

use provider::factory::rate_limit::LimitUnit;
use sync_store::StorageMeter;

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

/// Run `fut` with every physical storage request charged to `limits`.
///
/// `limits` is left holding everything the operation spent, whether it
/// succeeded or not, so the caller can commit the usage either way.  A failure
/// caused by a budget is returned as [`StewardError::RateLimited`] rather than
/// as the opaque storage error the object store produced.
pub async fn metered_op<F, T>(limits: &mut LimiterSet, fut: F) -> Result<T, StewardError>
where
    F: Future<Output = Result<T, StewardError>>,
{
    let shared = Arc::new(Mutex::new(std::mem::replace(
        limits,
        LimiterSet::unlimited(),
    )));
    let meter = Arc::new(LimiterMeter {
        limits: Arc::clone(&shared),
        refusal: Mutex::new(None),
    });

    let outcome = sync_store::with_meter(meter.clone(), fut).await;

    let refusal = meter
        .refusal
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    drop(meter);

    // Put the spending back where the caller left it.  A stream that outlived
    // the scope would still hold a reference; take the contents in place
    // rather than failing, because losing the accounting is worse than losing
    // the allocation.
    *limits = match Arc::try_unwrap(shared) {
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

    match outcome {
        Err(_) if refusal.is_some() => Err(StewardError::RateLimited(
            refusal.expect("checked as Some in the guard"),
        )),
        other => other,
    }
}
