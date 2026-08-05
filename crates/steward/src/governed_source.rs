// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! [`GovernedSource`]: a rate-limiting decorator over a [`ContentSource`], so a
//! pull spends a budget the same way a push does.
//!
//! # Why a decorator and not threaded arguments
//!
//! A push charges its limiters inline, because the push path is one function
//! that knows every byte before it sends them.  A pull is not shaped that way:
//! the fetch walk, the mirror rebuild, and the cross-pond import each descend
//! through several layers before reaching the wire, and external blobs stream
//! from a reader handed back to a caller three frames away.  Threading a
//! `&mut LimiterSet` through all of that would mean every one of those
//! signatures has to keep carrying it, and a new pull path added later would
//! silently be ungoverned by default.
//!
//! [`ContentSource`] is already exactly the set of operations that touch the
//! remote.  Wrapping it puts enforcement at that boundary instead: every
//! existing pull path is governed without changing its signature, and any path
//! added later is governed because it has no other way to reach the remote.
//!
//! # What a pull can and cannot promise
//!
//! A push checks the exact cost *before* paying it, because it holds the bytes
//! it is about to send.  A pull learns a transfer's size only by performing it
//! -- [`BlobReader`] is an opaque stream with no length, and an object's size
//! is known once its bytes arrive.  So a pull enforces the weaker but still
//! bounding rule: **refuse to begin a transfer once the budget is spent**, and
//! charge each transfer once it lands.  A pull can therefore overshoot its
//! budget, but by at most one object or one read chunk (8 MiB) -- not by the
//! unbounded amount that going ungoverned allows.  Blobs are charged as they
//! stream, so a runaway multi-gigabyte download is cut off partway through
//! rather than after it completes.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sync_store::content::ObjectHash;
use uuid::Uuid;

use provider::factory::rate_limit::LimitUnit;

use crate::limiter::LimiterSet;
use crate::{BlobReader, ContentSource, StewardError};

/// Wraps a [`ContentSource`] so every remote operation is charged to a
/// [`LimiterSet`].
///
/// Ops are charged per *remote round-trip*.  Reads served from the snapshot
/// taken by [`ContentSource::preload_objects`] are not round-trips and are not
/// charged as ops; their bytes are still charged, because those bytes did cross
/// the wire during the preload.
pub struct GovernedSource<'a> {
    inner: &'a dyn ContentSource,
    limits: Arc<Mutex<LimiterSet>>,
    /// True between `preload_objects` and `clear_object_cache`, when
    /// `get_object` is an in-memory lookup rather than a remote request.
    preloaded: Mutex<bool>,
}

impl<'a> GovernedSource<'a> {
    /// Govern `inner` with `limits`.
    #[must_use]
    pub fn new(inner: &'a dyn ContentSource, limits: LimiterSet) -> Self {
        Self {
            inner,
            limits: Arc::new(Mutex::new(limits)),
            preloaded: Mutex::new(false),
        }
    }

    /// Take the limiters back so the caller can commit their windows.
    ///
    /// # Panics
    ///
    /// Panics if a previous charge panicked while holding the lock.
    #[must_use]
    pub fn into_limits(self) -> LimiterSet {
        // The only other holder is a `GovernedBlobReader`, and every reader is
        // dropped when its blob finishes streaming, well before a pull commits.
        Arc::try_unwrap(self.limits)
            .unwrap_or_else(|_| unreachable!("a blob reader outlived the pull that opened it"))
            .into_inner()
            .expect("limiter lock poisoned by a panic during a charge")
    }

    /// Refuse if `unit` has nothing left to spend.
    ///
    /// This is the pull-side check: not "does `amount` fit" (unknowable before
    /// the transfer) but "is there any budget at all".  A zero charge is
    /// admitted by a spent budget, so `1` is the smallest charge that a spent
    /// budget refuses.
    fn check_any_left(&self, unit: LimitUnit) -> Result<(), StewardError> {
        self.limits
            .lock()
            .expect("limiter lock poisoned by a panic during a charge")
            .check(unit, 1)
            .map_err(StewardError::RateLimited)
    }

    fn record(&self, unit: LimitUnit, amount: u64) {
        if amount == 0 {
            return;
        }
        self.limits
            .lock()
            .expect("limiter lock poisoned by a panic during a charge")
            .record(unit, amount);
    }

    /// Charge one remote round-trip, refusing first if the ops budget is spent.
    fn begin_op(&self) -> Result<(), StewardError> {
        self.check_any_left(LimitUnit::Ops)?;
        self.record(LimitUnit::Ops, 1);
        Ok(())
    }

    fn is_preloaded(&self) -> bool {
        *self
            .preloaded
            .lock()
            .expect("preload flag poisoned by a panic")
    }

    fn set_preloaded(&self, value: bool) {
        *self
            .preloaded
            .lock()
            .expect("preload flag poisoned by a panic") = value;
    }
}

#[async_trait]
impl ContentSource for GovernedSource<'_> {
    fn pond_id(&self) -> Uuid {
        // Local metadata already held by the open source: no transfer, no
        // charge.
        self.inner.pond_id()
    }

    async fn get_tip(&self, ref_name: &str) -> Result<Option<ObjectHash>, StewardError> {
        self.begin_op()?;
        self.inner.get_tip(ref_name).await
    }

    async fn get_object(&self, hash: ObjectHash) -> Result<Option<Vec<u8>>, StewardError> {
        // After a preload this is an in-memory lookup, so it is not an
        // operation against the remote -- but its bytes were fetched by the
        // preload, so they are still charged.
        if self.is_preloaded() {
            self.check_any_left(LimitUnit::Bytes)?;
        } else {
            self.begin_op()?;
            self.check_any_left(LimitUnit::Bytes)?;
        }
        let result = self.inner.get_object(hash).await?;
        if let Some(bytes) = &result {
            self.record(LimitUnit::Bytes, bytes.len() as u64);
        }
        Ok(result)
    }

    async fn has_blob(&self, hash: ObjectHash) -> Result<bool, StewardError> {
        self.begin_op()?;
        self.inner.has_blob(hash).await
    }

    async fn list_blobs(&self) -> Result<std::collections::HashSet<ObjectHash>, StewardError> {
        // One listing, one charge -- the point of asking this way.
        self.begin_op()?;
        self.inner.list_blobs().await
    }

    async fn get_blob_reader(&self, hash: ObjectHash) -> Result<Option<BlobReader>, StewardError> {
        self.begin_op()?;
        self.check_any_left(LimitUnit::Bytes)?;
        let Some(reader) = self.inner.get_blob_reader(hash).await? else {
            return Ok(None);
        };
        Ok(Some(Box::new(GovernedBlobReader {
            inner: reader,
            limits: Arc::clone(&self.limits),
        })))
    }

    async fn preload_objects(&self) -> Result<(), StewardError> {
        self.begin_op()?;
        self.inner.preload_objects().await?;
        self.set_preloaded(true);
        Ok(())
    }

    fn clear_object_cache(&self) {
        self.set_preloaded(false);
        self.inner.clear_object_cache();
    }
}

/// A [`BlobReader`] that charges the byte budget as it streams, and stops once
/// the budget is spent.
///
/// This is where a pull's finest-grained enforcement lives.  A large blob is
/// the one transfer big enough to blow a budget on its own, and it is also the
/// only one whose size is unknown in advance, so it is charged per read and cut
/// off mid-stream when the budget runs out.
struct GovernedBlobReader {
    inner: BlobReader,
    limits: Arc<Mutex<LimiterSet>>,
}

impl tokio::io::AsyncRead for GovernedBlobReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::task::Poll;

        let this = self.get_mut();

        // Refuse to start another chunk on a spent budget.  Checking before the
        // read is what bounds the overshoot to a single chunk.
        {
            let limits = this
                .limits
                .lock()
                .map_err(|_| std::io::Error::other("limiter lock poisoned"))?;
            if let Err(e) = limits.check(LimitUnit::Bytes, 1) {
                return Poll::Ready(Err(std::io::Error::other(e.to_string())));
            }
        }

        let before = buf.filled().len();
        let poll = std::pin::Pin::new(&mut this.inner).poll_read(cx, buf);
        if matches!(poll, Poll::Ready(Ok(()))) {
            let read = buf.filled().len().saturating_sub(before) as u64;
            if read > 0
                && let Ok(mut limits) = this.limits.lock()
            {
                limits.record(LimitUnit::Bytes, read);
            }
        }
        poll
    }
}
