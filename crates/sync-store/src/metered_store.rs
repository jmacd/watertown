// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Charging a budget for what remote storage *physically* costs.
//!
//! # Why this layer exists
//!
//! Budgets used to be charged where a transfer was *described* rather than
//! where it was *performed*: `content_push` charged one op per
//! `ContentRemote` call, so a `push_commit` carrying 131 objects was "1 op".
//! A traced `water-prod push origin` against MinIO measured the gap:
//!
//! | | charged | physical |
//! |---|---|---|
//! | ops | 2 | 1198 (1186 GET, 6 LIST, 5 PUT, 1 HEAD) |
//! | bytes | 90.1 KiB (sent only) | 1.74 MiB (0.26 up, 1.48 **down**) |
//!
//! Three things were wrong, and all three are properties of the *layer*, not
//! of any particular charge:
//!
//! 1. One logical call is many requests.  A Delta commit writes parquet and
//!    log objects and re-reads the log to find its next version.
//! 2. Only the sent direction was counted.  Bytes *received* are what Azure
//!    and R2 bill as egress -- the direction that caused the incident these
//!    budgets exist to prevent.
//! 3. Only annotated paths were counted at all.  Maintenance, compaction and
//!    every incidental log read spent silently, because no one had written a
//!    charge there.
//!
//! Metering the [`ObjectStore`] fixes all three at once: it is the narrowest
//! waist every provider passes through, so a request is counted because it
//! *happened*, not because a caller remembered to declare it.  Nothing new
//! needs annotating when a code path is added.
//!
//! # The meter is ambient, not threaded
//!
//! delta-rs builds stores through a process-wide factory registry keyed by URL
//! scheme, so a store cannot be handed the budget of the operation that will
//! use it.  Instead the *operation* publishes its meter into a task-local for
//! the duration of the call (see [`with_meter`]), and the store charges
//! whatever is current.  Work outside any scope -- tests, local ponds, an
//! ungoverned remote -- finds no meter and is not charged, which is why this
//! wrapper is safe to install unconditionally.
//!
//! # What it cannot see
//!
//! Charging counts `ObjectStore` calls, which is nearly but not exactly HTTP
//! requests.  Page fetches inside one `list` are modelled from the item count
//! (S3 returns 1000 per page); multipart uploads are charged per part.  A
//! provider that internally retries a request is undercounted by the retries.
//! These are bounded, small, and in every case far closer than the ~600x
//! understatement they replace.

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::{
    Error as ObjectStoreError, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, Result as ObjectStoreResult, UploadPart, path::Path as ObjectPath,
};
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock};

/// How many items a listing returns per underlying request.  S3 and Azure
/// both page at 1000, so an `n`-item listing cost `ceil(n / 1000)` requests
/// even though it arrived as one call.
const LIST_PAGE_SIZE: u64 = 1000;

/// A budget that physical storage traffic is charged against.
///
/// Implemented in `steward` over a `LimiterSet`; kept abstract here because
/// `sync-store` sits below the crate that knows what a limiter is.
pub trait StorageMeter: Send + Sync + fmt::Debug {
    /// Refuse before a request is made.  `Err(reason)` aborts the request and
    /// surfaces `reason` to the caller.
    fn check(&self, ops: u64, bytes: u64) -> Result<(), String>;

    /// Record what a request actually cost, after it happened.
    fn record(&self, ops: u64, bytes: u64);
}

tokio::task_local! {
    static METER: Arc<dyn StorageMeter>;
}

/// Run `f` with `meter` charged for every metered store operation it performs.
///
/// Nested scopes replace rather than stack: the innermost meter is the one
/// charged, which is what a caller means by governing a sub-operation.
pub async fn with_meter<F, T>(meter: Arc<dyn StorageMeter>, f: F) -> T
where
    F: Future<Output = T>,
{
    // Nested scopes -- a metered source used inside a larger metered
    // operation -- must not queue behind themselves.  The task-local says the
    // caller already holds the gate.
    if METER.try_with(|_| ()).is_ok() {
        let _installed = ProcessMeter::install(Arc::clone(&meter));
        return METER.scope(meter, f).await;
    }
    let _gate = GOVERNED.lock().await;
    let _installed = ProcessMeter::install(Arc::clone(&meter));
    METER.scope(meter, f).await
}

/// Serializes governed operations within a process.
///
/// [`PROCESS_METER`] is a single slot, so two governed operations running at
/// once would charge each other's budgets.  Rather than let that be a latent
/// hazard, only one runs at a time -- which is what a pond process does
/// anyway: it replicates to one remote, then the next.  Making the invariant
/// hold by construction is what lets the process meter be trusted.
static GOVERNED: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The meter installed for the whole process, as a backstop for the
/// task-local one.
///
/// # Why a task-local is not enough
///
/// Measured against MinIO: a governed tick made 463 requests through this
/// wrapper and only 26 of them found a task-local meter.  The Delta layer
/// hands work to tasks it spawns, and a `tokio::task_local` does not cross
/// that boundary -- so the great majority of a push was charged to nothing at
/// all while appearing, from the budget's side, simply not to have happened.
///
/// A process-wide install closes that gap, and is honest about what a budget
/// governs here: one pond process replicates to one remote at a time, so
/// "this process is pushing" and "this operation is pushing" are the same
/// statement.  The task-local is kept as the first lookup because when it is
/// present it is exact.
static PROCESS_METER: RwLock<Option<Arc<dyn StorageMeter>>> = RwLock::new(None);

/// Restores whatever process meter was installed before, so nested scopes
/// (a source wrapper inside a larger operation) unwind in order.
struct ProcessMeter(Option<Arc<dyn StorageMeter>>);

impl ProcessMeter {
    fn install(meter: Arc<dyn StorageMeter>) -> Self {
        let mut slot = PROCESS_METER
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self(slot.replace(meter))
    }
}

impl Drop for ProcessMeter {
    fn drop(&mut self) {
        let mut slot = PROCESS_METER
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *slot = self.0.take();
    }
}

/// The meter governing the current work, if any.
///
/// The task-local first, because it is exact; the process meter second,
/// because work that escaped the task is still work a budget must pay for.
#[must_use]
pub fn current_meter() -> Option<Arc<dyn StorageMeter>> {
    if let Ok(meter) = METER.try_with(Arc::clone) {
        return Some(meter);
    }
    PROCESS_METER
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

/// Physical traffic seen by every metered store in this process.
///
/// # Why this is not the meter
///
/// The meter is ambient: it governs whichever task installed it, and it is
/// therefore only as reliable as the task boundary.  Work handed to a spawned
/// task -- which the Delta layer does freely -- escapes it silently, and a
/// budget that is quietly not charged is indistinguishable from a budget that
/// is not being spent.
///
/// These counters are bound to the store instead.  A request increments them
/// because it happened, with no ambient state involved, so comparing them
/// against what a budget charged makes such a blind spot visible in the pond
/// rather than only in a provider's trace.
#[derive(Debug, Default)]
pub struct Observation {
    ops: AtomicU64,
    bytes: AtomicU64,
}

impl Observation {
    /// Requests seen since the process started.
    #[must_use]
    pub fn ops(&self) -> u64 {
        self.ops.load(Ordering::Relaxed)
    }

    /// Bytes transferred, in either direction, since the process started.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    fn add(&self, ops: u64, bytes: u64) {
        let _ = self.ops.fetch_add(ops, Ordering::Relaxed);
        let _ = self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

/// Process-wide totals, shared by every [`MeteredStore`].
///
/// One accumulator rather than one per store: a budget governs a remote, and
/// a remote is reached through however many store instances the Delta layer
/// decides to build.  Callers take a difference across an operation (see
/// `steward::storage_meter::MeterGuard`), which is why an ever-increasing
/// total is enough.
static OBSERVED: LazyLock<Arc<Observation>> = LazyLock::new(Arc::default);

/// The process-wide observation counters.
#[must_use]
pub fn observed() -> Arc<Observation> {
    Arc::clone(&OBSERVED)
}

/// Refuse up front if the budget cannot cover this request.
fn check(meter: Option<&Arc<dyn StorageMeter>>, ops: u64, bytes: u64) -> ObjectStoreResult<()> {
    let Some(meter) = meter else {
        return Ok(());
    };
    meter
        .check(ops, bytes)
        .map_err(|reason| ObjectStoreError::Generic {
            store: "metered",
            source: reason.into(),
        })
}

/// Charge a request that has been attempted.
///
/// Recorded regardless of whether the request succeeded: a request that
/// reached the provider and failed is still billed by the provider, and a
/// failing operation retried on a timer is exactly the runaway shape these
/// budgets exist to stop.  Charging only successes would make the worst case
/// free.
fn record(meter: Option<&Arc<dyn StorageMeter>>, ops: u64, bytes: u64) {
    // Observed first and unconditionally: what happened is recorded whether or
    // not anything was watching.
    OBSERVED.add(ops, bytes);
    if let Some(meter) = meter {
        meter.record(ops, bytes);
    }
}

/// An [`ObjectStore`] that charges the ambient [`StorageMeter`] for the
/// requests and bytes it actually performs.
pub struct MeteredStore {
    inner: Arc<dyn ObjectStore>,
}

impl MeteredStore {
    /// Wrap `inner` so its traffic is charged to whichever meter is current.
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

impl fmt::Debug for MeteredStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MeteredStore({:?})", self.inner)
    }
}

impl fmt::Display for MeteredStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MeteredStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for MeteredStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        let meter = current_meter();
        let bytes = payload.content_length() as u64;
        check(meter.as_ref(), 1, bytes)?;
        let result = self.inner.put_opts(location, payload, opts).await;
        record(meter.as_ref(), 1, bytes);
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        let meter = current_meter();
        check(meter.as_ref(), 1, 0)?;
        let upload = self.inner.put_multipart_opts(location, opts).await;
        record(meter.as_ref(), 1, 0);
        Ok(Box::new(MeteredUpload {
            inner: upload?,
            meter,
        }))
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        let meter = current_meter();
        // A read's size is not knowable before it is made, so admission asks
        // only whether any budget remains; the bytes are charged as they
        // arrive.  This is the same shape the pull path already used.
        check(meter.as_ref(), 1, 0)?;
        let result = self.inner.get_opts(location, options).await;
        record(meter.as_ref(), 1, 0);
        let mut result = result?;
        // Count bytes off the wire as they stream, not `meta.size`: a ranged
        // read transfers less than the object, and a stream abandoned early
        // transfers less than it promised.
        if let GetResultPayload::Stream(stream) = result.payload {
            let meter = meter.clone();
            result.payload = GetResultPayload::Stream(
                stream
                    .inspect(move |chunk| {
                        if let Ok(bytes) = chunk {
                            record(meter.as_ref(), 0, bytes.len() as u64);
                        }
                    })
                    .boxed(),
            );
        }
        Ok(result)
    }

    async fn delete(&self, location: &ObjectPath) -> ObjectStoreResult<()> {
        let meter = current_meter();
        check(meter.as_ref(), 1, 0)?;
        let result = self.inner.delete(location).await;
        record(meter.as_ref(), 1, 0);
        result
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let meter = current_meter();
        // `list` is not async and cannot refuse, so the first page is charged
        // here and further pages are charged as items arrive.
        record(meter.as_ref(), 1, 0);
        let mut seen: u64 = 0;
        self.inner
            .list(prefix)
            .inspect(move |item| {
                if item.is_ok() {
                    seen += 1;
                    if seen.is_multiple_of(LIST_PAGE_SIZE) {
                        record(meter.as_ref(), 1, 0);
                    }
                }
            })
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> ObjectStoreResult<ListResult> {
        let meter = current_meter();
        check(meter.as_ref(), 1, 0)?;
        let result = self.inner.list_with_delimiter(prefix).await;
        let pages = result.as_ref().map_or(1, |r| {
            (r.objects.len() as u64).div_ceil(LIST_PAGE_SIZE).max(1)
        });
        record(meter.as_ref(), pages, 0);
        result
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> ObjectStoreResult<()> {
        let meter = current_meter();
        check(meter.as_ref(), 1, 0)?;
        let result = self.inner.copy(from, to).await;
        record(meter.as_ref(), 1, 0);
        result
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> ObjectStoreResult<()> {
        let meter = current_meter();
        check(meter.as_ref(), 1, 0)?;
        let result = self.inner.copy_if_not_exists(from, to).await;
        record(meter.as_ref(), 1, 0);
        result
    }
}

/// A multipart upload whose parts are charged individually, because each part
/// is its own request and its own bytes.
#[derive(Debug)]
struct MeteredUpload {
    inner: Box<dyn MultipartUpload>,
    meter: Option<Arc<dyn StorageMeter>>,
}

#[async_trait]
impl MultipartUpload for MeteredUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let bytes = data.content_length() as u64;
        record(self.meter.as_ref(), 1, bytes);
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> ObjectStoreResult<PutResult> {
        record(self.meter.as_ref(), 1, 0);
        self.inner.complete().await
    }

    async fn abort(&mut self) -> ObjectStoreResult<()> {
        record(self.meter.as_ref(), 1, 0);
        self.inner.abort().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    struct Counter {
        ops: Mutex<u64>,
        bytes: Mutex<u64>,
        refuse: bool,
    }

    impl StorageMeter for Counter {
        fn check(&self, _ops: u64, _bytes: u64) -> Result<(), String> {
            if self.refuse {
                return Err("budget spent".to_string());
            }
            Ok(())
        }

        fn record(&self, ops: u64, bytes: u64) {
            *self.ops.lock().unwrap() += ops;
            *self.bytes.lock().unwrap() += bytes;
        }
    }

    fn store() -> MeteredStore {
        MeteredStore::new(Arc::new(InMemory::new()))
    }

    /// The point of the whole module: a read is charged for the bytes that
    /// come *back*, which the old logical charging never counted at all.
    #[tokio::test]
    async fn a_read_charges_the_bytes_it_receives() {
        let meter = Arc::new(Counter::default());
        let store = store();
        let path = ObjectPath::from("obj");

        with_meter(meter.clone(), async {
            store
                .put(&path, PutPayload::from_static(b"0123456789"))
                .await
                .unwrap();
            let got = store.get(&path).await.unwrap();
            got.bytes().await.unwrap();
        })
        .await;

        // one put + one get
        assert_eq!(*meter.ops.lock().unwrap(), 2);
        // ten bytes up, ten bytes back down
        assert_eq!(*meter.bytes.lock().unwrap(), 20);
    }

    /// A spent budget refuses the request rather than performing it.
    #[tokio::test]
    async fn a_spent_budget_refuses() {
        let meter = Arc::new(Counter {
            refuse: true,
            ..Counter::default()
        });
        let store = store();

        let err = with_meter(meter, async {
            store
                .put(&ObjectPath::from("obj"), PutPayload::from_static(b"x"))
                .await
                .unwrap_err()
        })
        .await;

        assert!(err.to_string().contains("budget spent"), "{err}");
    }

    /// Unmetered work still functions: the wrapper is installed
    /// unconditionally, so an ungoverned pond must not fail or panic.
    #[tokio::test]
    async fn no_meter_is_not_an_error() {
        // Held for the same reason a governed operation holds it: the process
        // meter is one slot, so "no meter is installed" is only a fact while
        // nothing else is installing one.  A pond process satisfies this by
        // doing one thing at a time; a test binary has to say so.
        let _gate = GOVERNED.lock().await;
        let store = store();
        let path = ObjectPath::from("obj");
        store
            .put(&path, PutPayload::from_static(b"hi"))
            .await
            .unwrap();
        assert_eq!(
            &store.get(&path).await.unwrap().bytes().await.unwrap()[..],
            b"hi"
        );
    }

    /// Every request counts, not just the annotated ones -- a delete and a
    /// list are charged though no caller declares them.
    #[tokio::test]
    async fn incidental_requests_are_charged() {
        let meter = Arc::new(Counter::default());
        let store = store();
        let path = ObjectPath::from("obj");

        with_meter(meter.clone(), async {
            store
                .put(&path, PutPayload::from_static(b"x"))
                .await
                .unwrap();
            let _ = store.list(None).collect::<Vec<_>>().await;
            store.delete(&path).await.unwrap();
        })
        .await;

        assert_eq!(*meter.ops.lock().unwrap(), 3);
    }
}
