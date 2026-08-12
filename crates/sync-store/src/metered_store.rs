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
//! # A store is bound to its budget by identity
//!
//! delta-rs builds stores through a process-wide factory registry, so a store
//! cannot be *handed* the budget of the operation that will use it.  But the
//! factory is handed the remote's URL, and a budget governs a remote -- so the
//! store can look its budget up by the identity it already has.  That is what
//! [`RemoteKey`] and [`bind_meter`] do.
//!
//! An earlier version resolved the budget from ambient state instead: the
//! operation published its meter into a task-local and the store charged
//! whatever was current.  It was wrong, and measurably so.  A
//! `tokio::task_local` does not survive `tokio::spawn`, and the Delta layer
//! spawns freely: of 463 requests one governed tick made through this wrapper,
//! only 26 found the meter.  The remaining 94% were charged to nothing, which
//! from the budget's side is indistinguishable from not having happened.
//!
//! Identity has no such failure mode.  A store knows which remote it speaks to
//! for its whole life, on whatever task, so attribution is a property of the
//! wiring rather than of what happens to be executing.  It also makes
//! concurrency ordinary rather than dangerous -- two remotes pushed at once
//! charge two budgets, where a single ambient slot forced them to take turns.
//!
//! # Nothing spends for free
//!
//! Traffic that reaches a remote while no budget is bound to it is *not*
//! discarded: it accrues as arrears against that remote's key, and the next
//! binding is charged for it (see [`bind_meter`]).  So a code path that
//! touches a remote outside any guard makes the next guarded operation more
//! likely to be refused, rather than spending invisibly.
//!
//! Arrears live as long as the process.  A pond process is one tick and pushes
//! at the end of it, so in practice everything a tick spends is swept into
//! that tick's commit; arrears outstanding when a process exits are lost,
//! which is a real limit of this mechanism and not a claim it makes.
//!
//! # What it cannot see
//!
//! Charging counts `ObjectStore` calls, which is nearly but not exactly HTTP
//! requests.  Page fetches inside one `list` are modelled from the item count
//! (S3 returns 1000 per page); multipart uploads are charged per part.  A
//! provider that internally retries a request is undercounted by the retries.
//!
//! Measured against MinIO with a fresh process per tick
//! (`testsuite/measure-remote-cost.sh`), the budget charged 574 requests where
//! the server's own trace recorded 586: a residue of about 2%, from the
//! modelling above rather than from anything escaping attribution.  That is
//! the accuracy this layer claims, against the ~600x understatement it
//! replaced.

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::{
    Error as ObjectStoreError, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, Result as ObjectStoreResult, UploadPart, path::Path as ObjectPath,
};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock};
use url::Url;

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

/// Which remote a store speaks to.
///
/// A budget governs a remote, and a remote is a URL.  The object-store factory
/// is handed that URL when it builds the store, so the store can carry it for
/// life and resolve its budget by identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RemoteKey(String);

impl RemoteKey {
    /// The key identifying the remote at `url`.
    ///
    /// Parsed when possible so equivalent spellings agree, and taken verbatim
    /// otherwise: a URL this crate cannot parse still identifies a remote
    /// consistently, which is all a key has to do.
    #[must_use]
    pub fn new(url: &str) -> Self {
        let text = match Url::parse(url) {
            Ok(mut u) => {
                u.set_query(None);
                u.set_fragment(None);
                u.to_string()
            }
            Err(_) => url.to_string(),
        };
        Self(text.trim_end_matches('/').to_string())
    }

    /// The key as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether this key is `other` or lies beneath it.
    ///
    /// A remote is configured as a bucket or container URL while stores get
    /// built for the tables underneath it, so a budget bound to the remote has
    /// to cover its descendants.
    #[must_use]
    pub fn is_under(&self, other: &Self) -> bool {
        self.0 == other.0
            || (self.0.starts_with(&other.0) && self.0.as_bytes().get(other.0.len()) == Some(&b'/'))
    }
}

impl fmt::Display for RemoteKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// Lets the registry be probed with a `&str` while being keyed by `RemoteKey`,
// so walking up a URL's path costs no allocation.
impl std::borrow::Borrow<str> for RemoteKey {
    fn borrow(&self) -> &str {
        &self.0
    }
}

/// The budget bound to each remote.
static METERS: LazyLock<RwLock<HashMap<RemoteKey, Arc<dyn StorageMeter>>>> =
    LazyLock::new(RwLock::default);

/// Traffic that reached a remote while no budget was bound to it, owed by the
/// next binding.
static ARREARS: LazyLock<RwLock<HashMap<RemoteKey, (u64, u64)>>> = LazyLock::new(RwLock::default);

/// Physical traffic seen per remote, whatever was or was not charged for it.
static OBSERVED: LazyLock<RwLock<HashMap<RemoteKey, Arc<Observation>>>> =
    LazyLock::new(RwLock::default);

/// Bind `meter` to the remote at `key` until the returned value is dropped.
///
/// Every request any store makes to that remote, or to anything beneath it, is
/// charged here -- on whatever task, in whatever spawned corner of the Delta
/// layer it happens.
///
/// Nested bindings on one key replace and then restore, so a wrapper inside a
/// larger operation charges the inner budget while it lives, which is what a
/// caller means by governing a sub-operation.
#[must_use = "the budget is only bound while the binding is held"]
pub fn bind_meter(key: &RemoteKey, meter: Arc<dyn StorageMeter>) -> MeterBinding {
    let previous = METERS
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(key.clone(), Arc::clone(&meter));

    // Charge whatever reached this remote while nothing was bound.  Work that
    // escaped a budget is carried, not forgiven: otherwise a path that spends
    // outside a guard spends for free, which is the failure this whole module
    // exists to make impossible.
    let arrears = take_arrears(key);
    if arrears != (0, 0) {
        log::warn!(
            "[WARN] {key}: charging {} ops / {} bytes spent with no budget bound",
            arrears.0,
            arrears.1
        );
        meter.record(arrears.0, arrears.1);
    }

    MeterBinding {
        key: key.clone(),
        previous,
        arrears,
    }
}

/// A budget bound to a remote, unbound when dropped.
pub struct MeterBinding {
    key: RemoteKey,
    previous: Option<Arc<dyn StorageMeter>>,
    arrears: (u64, u64),
}

impl MeterBinding {
    /// Traffic this binding was charged for that happened before it existed.
    ///
    /// Reported so a caller comparing what it charged against what was
    /// observed can account for it, rather than reading arrears as a
    /// discrepancy.
    #[must_use]
    pub fn arrears(&self) -> (u64, u64) {
        self.arrears
    }
}

impl fmt::Debug for MeterBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MeterBinding({})", self.key)
    }
}

impl Drop for MeterBinding {
    fn drop(&mut self) {
        let mut meters = METERS
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match self.previous.take() {
            Some(m) => {
                let _ = meters.insert(self.key.clone(), m);
            }
            None => {
                let _ = meters.remove(&self.key);
            }
        }
    }
}

/// The budget governing the remote at `key`, if one is bound.
///
/// Resolves the key itself first and then each enclosing prefix, because a
/// store is built for a table beneath the remote a budget names.  The walk is
/// bounded by the depth of a URL path.
fn meter_for(key: &RemoteKey) -> Option<Arc<dyn StorageMeter>> {
    let meters = METERS
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if meters.is_empty() {
        return None;
    }
    let mut candidate: &str = key.as_str();
    loop {
        if let Some(meter) = meters.get(candidate) {
            return Some(Arc::clone(meter));
        }
        match candidate.rfind('/') {
            Some(0) | None => return None,
            Some(cut) => candidate = &candidate[..cut],
        }
    }
}

/// Take everything owed on `key` and anything beneath it.
fn take_arrears(key: &RemoteKey) -> (u64, u64) {
    let mut arrears = ARREARS
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let owed: Vec<RemoteKey> = arrears
        .keys()
        .filter(|k| k.is_under(key))
        .cloned()
        .collect();
    let mut total = (0u64, 0u64);
    for k in owed {
        if let Some((ops, bytes)) = arrears.remove(&k) {
            total.0 = total.0.saturating_add(ops);
            total.1 = total.1.saturating_add(bytes);
        }
    }
    total
}

/// Note traffic to `key` that no budget claimed.
fn owe(key: &RemoteKey, ops: u64, bytes: u64) {
    let mut arrears = ARREARS
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let entry = arrears.entry(key.clone()).or_insert((0, 0));
    entry.0 = entry.0.saturating_add(ops);
    entry.1 = entry.1.saturating_add(bytes);
}

/// Physical traffic to `key` and everything beneath it since the process
/// started.
///
/// # Why this is not the meter
///
/// These counters are incremented because a request *happened*: the store adds
/// to them before it looks a budget up, keyed by the URL it was built for.
/// Charging, by contrast, depends on that lookup finding something.  The two
/// are produced by different mechanisms, so comparing them makes a
/// misattributed store visible in the pond -- as observed traffic that no
/// budget was charged for -- rather than only in a provider's trace.
#[must_use]
pub fn observed_under(key: &RemoteKey) -> (u64, u64) {
    let observed = OBSERVED
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    observed
        .iter()
        .filter(|(k, _)| k.is_under(key))
        .fold((0u64, 0u64), |acc, (_, o)| {
            (
                acc.0.saturating_add(o.ops()),
                acc.1.saturating_add(o.bytes()),
            )
        })
}

/// The counters for `key`, created on first use.
fn observation(key: &RemoteKey) -> Arc<Observation> {
    if let Some(o) = OBSERVED
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(key)
    {
        return Arc::clone(o);
    }
    Arc::clone(
        OBSERVED
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(key.clone())
            .or_default(),
    )
}

/// Physical traffic seen for one remote.
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
fn record(key: &RemoteKey, meter: Option<&Arc<dyn StorageMeter>>, ops: u64, bytes: u64) {
    // Observed first and unconditionally: what happened is recorded whether or
    // not anything was watching, and under the key of the store that did it
    // rather than the budget that claimed it.
    observation(key).add(ops, bytes);
    match meter {
        Some(meter) => meter.record(ops, bytes),
        // Nothing claimed this traffic, so it is owed rather than forgiven.
        None => owe(key, ops, bytes),
    }
}

/// An [`ObjectStore`] that charges the budget bound to its remote for the
/// requests and bytes it actually performs.
pub struct MeteredStore {
    inner: Arc<dyn ObjectStore>,
    /// The remote this store speaks to, fixed when it was built.  Attribution
    /// follows from this rather than from what is executing, which is what
    /// makes it survive every task boundary the Delta layer introduces.
    key: RemoteKey,
}

impl MeteredStore {
    /// Wrap `inner` so its traffic is charged to the budget bound to `key`.
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>, key: RemoteKey) -> Self {
        Self { inner, key }
    }

    /// The budget governing this store right now, if any.
    fn meter(&self) -> Option<Arc<dyn StorageMeter>> {
        meter_for(&self.key)
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
        let meter = self.meter();
        let bytes = payload.content_length() as u64;
        check(meter.as_ref(), 1, bytes)?;
        let result = self.inner.put_opts(location, payload, opts).await;
        record(&self.key, meter.as_ref(), 1, bytes);
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        let meter = self.meter();
        check(meter.as_ref(), 1, 0)?;
        let upload = self.inner.put_multipart_opts(location, opts).await;
        record(&self.key, meter.as_ref(), 1, 0);
        Ok(Box::new(MeteredUpload {
            inner: upload?,
            key: self.key.clone(),
            meter,
        }))
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        let meter = self.meter();
        // A read's size is not knowable before it is made, so admission asks
        // only whether any budget remains; the bytes are charged as they
        // arrive.  This is the same shape the pull path already used.
        check(meter.as_ref(), 1, 0)?;
        let result = self.inner.get_opts(location, options).await;
        record(&self.key, meter.as_ref(), 1, 0);
        let mut result = result?;
        // Count bytes off the wire as they stream, not `meta.size`: a ranged
        // read transfers less than the object, and a stream abandoned early
        // transfers less than it promised.
        if let GetResultPayload::Stream(stream) = result.payload {
            let meter = meter.clone();
            // The key travels with the stream: a read that outlives the call
            // is still that remote's traffic, and there is no ambient state
            // left to ask.
            let key = self.key.clone();
            result.payload = GetResultPayload::Stream(
                stream
                    .inspect(move |chunk| {
                        if let Ok(bytes) = chunk {
                            record(&key, meter.as_ref(), 0, bytes.len() as u64);
                        }
                    })
                    .boxed(),
            );
        }
        Ok(result)
    }

    async fn delete(&self, location: &ObjectPath) -> ObjectStoreResult<()> {
        let meter = self.meter();
        check(meter.as_ref(), 1, 0)?;
        let result = self.inner.delete(location).await;
        record(&self.key, meter.as_ref(), 1, 0);
        result
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let meter = self.meter();
        // `list` is not async and cannot refuse, so the first page is charged
        // here and further pages are charged as items arrive.
        record(&self.key, meter.as_ref(), 1, 0);
        let mut seen: u64 = 0;
        let key = self.key.clone();
        self.inner
            .list(prefix)
            .inspect(move |item| {
                if item.is_ok() {
                    seen += 1;
                    if seen.is_multiple_of(LIST_PAGE_SIZE) {
                        record(&key, meter.as_ref(), 1, 0);
                    }
                }
            })
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> ObjectStoreResult<ListResult> {
        let meter = self.meter();
        check(meter.as_ref(), 1, 0)?;
        let result = self.inner.list_with_delimiter(prefix).await;
        let pages = result.as_ref().map_or(1, |r| {
            (r.objects.len() as u64).div_ceil(LIST_PAGE_SIZE).max(1)
        });
        record(&self.key, meter.as_ref(), pages, 0);
        result
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> ObjectStoreResult<()> {
        let meter = self.meter();
        check(meter.as_ref(), 1, 0)?;
        let result = self.inner.copy(from, to).await;
        record(&self.key, meter.as_ref(), 1, 0);
        result
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> ObjectStoreResult<()> {
        let meter = self.meter();
        check(meter.as_ref(), 1, 0)?;
        let result = self.inner.copy_if_not_exists(from, to).await;
        record(&self.key, meter.as_ref(), 1, 0);
        result
    }
}

/// A multipart upload whose parts are charged individually, because each part
/// is its own request and its own bytes.
#[derive(Debug)]
struct MeteredUpload {
    inner: Box<dyn MultipartUpload>,
    key: RemoteKey,
    meter: Option<Arc<dyn StorageMeter>>,
}

#[async_trait]
impl MultipartUpload for MeteredUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let bytes = data.content_length() as u64;
        if let Err(error) = check(self.meter.as_ref(), 1, bytes) {
            return Box::pin(async move { Err(error) });
        }
        record(&self.key, self.meter.as_ref(), 1, bytes);
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> ObjectStoreResult<PutResult> {
        check(self.meter.as_ref(), 1, 0)?;
        record(&self.key, self.meter.as_ref(), 1, 0);
        self.inner.complete().await
    }

    async fn abort(&mut self) -> ObjectStoreResult<()> {
        // Cleanup must remain possible after the request budget is exhausted:
        // refusing an abort can leave staged multipart data accruing storage
        // charges.  Account for the request, but deliberately do not admit it.
        record(&self.key, self.meter.as_ref(), 1, 0);
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

    #[derive(Debug)]
    struct ByteBudget {
        limit: u64,
        used: Mutex<u64>,
    }

    impl StorageMeter for ByteBudget {
        fn check(&self, _ops: u64, bytes: u64) -> Result<(), String> {
            let used = *self.used.lock().unwrap();
            if used.saturating_add(bytes) > self.limit {
                return Err(format!(
                    "byte budget exceeded: {used}/{} used, request for {bytes} denied",
                    self.limit
                ));
            }
            Ok(())
        }

        fn record(&self, _ops: u64, bytes: u64) {
            let mut used = self.used.lock().unwrap();
            *used = used.saturating_add(bytes);
        }
    }

    #[derive(Debug)]
    struct RequestBudget {
        limit: u64,
        used: Mutex<u64>,
    }

    impl StorageMeter for RequestBudget {
        fn check(&self, ops: u64, _bytes: u64) -> Result<(), String> {
            let used = *self.used.lock().unwrap();
            if used.saturating_add(ops) > self.limit {
                return Err(format!(
                    "request budget exceeded: {used}/{} used, request for {ops} denied",
                    self.limit
                ));
            }
            Ok(())
        }

        fn record(&self, ops: u64, _bytes: u64) {
            let mut used = self.used.lock().unwrap();
            *used = used.saturating_add(ops);
        }
    }

    /// A store on its own remote, so tests cannot charge each other.  This is
    /// the property the previous design lacked: attribution was ambient, so
    /// two tests running at once shared one slot and had to be serialized.
    fn store(name: &str) -> (MeteredStore, RemoteKey) {
        let key = RemoteKey::new(&format!("mem://{name}"));
        (
            MeteredStore::new(Arc::new(InMemory::new()), key.clone()),
            key,
        )
    }

    /// The point of the whole module: a read is charged for the bytes that
    /// come *back*, which the old logical charging never counted at all.
    #[tokio::test]
    async fn a_read_charges_the_bytes_it_receives() {
        let meter = Arc::new(Counter::default());
        let (store, key) = store("read-charges-bytes");
        let path = ObjectPath::from("obj");

        let binding = bind_meter(&key, meter.clone());
        store
            .put(&path, PutPayload::from_static(b"0123456789"))
            .await
            .unwrap();
        let got = store.get(&path).await.unwrap();
        got.bytes().await.unwrap();
        drop(binding);

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
        let (store, key) = store("spent-budget-refuses");

        let _binding = bind_meter(&key, meter);
        let err = store
            .put(&ObjectPath::from("obj"), PutPayload::from_static(b"x"))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("budget spent"), "{err}");
    }

    /// Every multipart part is a separate physical request, so it must be
    /// admitted before reaching the provider.  Checking only when the upload
    /// is opened lets one large blob overrun the entire budget before the next
    /// object-store call notices.
    #[tokio::test]
    async fn a_multipart_upload_stops_before_exceeding_its_byte_budget() {
        const MIB: u64 = 1024 * 1024;

        let meter = Arc::new(ByteBudget {
            limit: 6 * MIB,
            used: Mutex::new(0),
        });
        let (store, key) = store("multipart-byte-budget");
        let path = ObjectPath::from("large");
        let binding = bind_meter(&key, meter.clone());
        let upload = store.put_multipart(&path).await.unwrap();
        let mut writer = object_store::WriteMultipart::new(upload);

        // WriteMultipart emits 5 MiB parts.  The first fits; admitting the
        // second would exceed the 6 MiB budget and must fail before upload.
        writer.write(&vec![0u8; 12 * MIB as usize]);
        let err = writer
            .finish()
            .await
            .expect_err("second part must be denied");
        drop(binding);

        assert!(err.to_string().contains("byte budget exceeded"), "{err}");
        assert_eq!(*meter.used.lock().unwrap(), 5 * MIB);
        assert!(
            store.head(&path).await.is_err(),
            "a refused multipart upload must not publish a partial object"
        );
    }

    /// Completing a multipart upload is itself a physical request.  It must
    /// not slip past an exhausted IOPS budget, but the subsequent abort must
    /// still reach the provider so staged parts do not become a storage cost.
    #[tokio::test]
    async fn multipart_completion_is_limited_but_cleanup_is_not_refused() {
        let meter = Arc::new(RequestBudget {
            limit: 2,
            used: Mutex::new(0),
        });
        let (store, key) = store("multipart-request-budget");
        let path = ObjectPath::from("large");
        let binding = bind_meter(&key, meter.clone());
        let mut upload = store.put_multipart(&path).await.unwrap();

        upload
            .put_part(PutPayload::from_static(b"part"))
            .await
            .unwrap();
        let err = upload
            .complete()
            .await
            .expect_err("completion must exceed the request budget");
        assert!(err.to_string().contains("request budget exceeded"), "{err}");

        upload.abort().await.expect("cleanup must remain possible");
        drop(binding);

        assert_eq!(
            *meter.used.lock().unwrap(),
            3,
            "initiation, part, and mandatory cleanup are charged"
        );
        assert!(
            store.head(&path).await.is_err(),
            "a refused completion must not publish the object"
        );
    }

    /// Unmetered work still functions: the wrapper is installed
    /// unconditionally, so an ungoverned pond must not fail or panic.
    #[tokio::test]
    async fn no_meter_is_not_an_error() {
        let (store, _key) = store("no-meter");
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
        let (store, key) = store("incidental");
        let path = ObjectPath::from("obj");

        let binding = bind_meter(&key, meter.clone());
        store
            .put(&path, PutPayload::from_static(b"x"))
            .await
            .unwrap();
        let _ = store.list(None).collect::<Vec<_>>().await;
        store.delete(&path).await.unwrap();
        drop(binding);

        assert_eq!(*meter.ops.lock().unwrap(), 3);
    }

    /// A budget bound to a remote governs the tables beneath it, because that
    /// is what a store is actually built for: the config names a bucket, the
    /// Delta layer opens `bucket/table`.
    #[tokio::test]
    async fn a_budget_governs_the_tables_beneath_it() {
        let meter = Arc::new(Counter::default());
        let bucket = RemoteKey::new("mem://beneath");
        let table = MeteredStore::new(
            Arc::new(InMemory::new()),
            RemoteKey::new("mem://beneath/table/_delta_log"),
        );

        let binding = bind_meter(&bucket, meter.clone());
        table
            .put(&ObjectPath::from("obj"), PutPayload::from_static(b"x"))
            .await
            .unwrap();
        drop(binding);

        assert_eq!(*meter.ops.lock().unwrap(), 1);
    }

    /// A neighbouring remote's budget is not charged.  Matching is on path
    /// boundaries, so `mem://neighbour-two` does not fall under
    /// `mem://neighbour`.
    #[tokio::test]
    async fn a_neighbour_is_not_charged() {
        let meter = Arc::new(Counter::default());
        let mine = RemoteKey::new("mem://neighbour");
        let (theirs, _) = store("neighbour-two");

        let binding = bind_meter(&mine, meter.clone());
        theirs
            .put(&ObjectPath::from("obj"), PutPayload::from_static(b"x"))
            .await
            .unwrap();
        drop(binding);

        assert_eq!(*meter.ops.lock().unwrap(), 0);
    }

    /// Traffic that happens with no budget bound is charged to the next one.
    ///
    /// Without this, any path that reaches a remote outside a guard spends for
    /// free -- which is precisely the shape of the runaway these budgets exist
    /// to stop.
    #[tokio::test]
    async fn traffic_outside_a_budget_is_owed_to_the_next_one() {
        let (store, key) = store("arrears");
        let path = ObjectPath::from("obj");

        // Ungoverned: nothing is bound.
        store
            .put(&path, PutPayload::from_static(b"12345"))
            .await
            .unwrap();

        let meter = Arc::new(Counter::default());
        let binding = bind_meter(&key, meter.clone());
        assert_eq!(binding.arrears(), (1, 5));
        drop(binding);

        assert_eq!(*meter.ops.lock().unwrap(), 1);
        assert_eq!(*meter.bytes.lock().unwrap(), 5);
    }

    /// Arrears are owed once.  A second binding after the debt is settled
    /// starts clean, so a retry loop cannot be charged the same traffic twice.
    #[tokio::test]
    async fn arrears_are_charged_once() {
        let (store, key) = store("arrears-once");
        store
            .put(&ObjectPath::from("obj"), PutPayload::from_static(b"x"))
            .await
            .unwrap();

        drop(bind_meter(&key, Arc::new(Counter::default())));

        let second = Arc::new(Counter::default());
        let binding = bind_meter(&key, second.clone());
        assert_eq!(binding.arrears(), (0, 0));
        drop(binding);
        assert_eq!(*second.ops.lock().unwrap(), 0);
    }

    /// Observation counts what happened whether or not a budget claimed it,
    /// which is what lets the two be compared.
    #[tokio::test]
    async fn observation_counts_ungoverned_traffic() {
        let (store, key) = store("observed-ungoverned");
        assert_eq!(observed_under(&key), (0, 0));

        store
            .put(&ObjectPath::from("obj"), PutPayload::from_static(b"abc"))
            .await
            .unwrap();

        assert_eq!(observed_under(&key), (1, 3));
    }
}
