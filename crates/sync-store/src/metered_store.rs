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
    Error as ObjectStoreError, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult, UploadPart, path::Path as ObjectPath,
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

    /// Atomically admit and reserve a request.
    ///
    /// Implementations backed by a shared budget should override this method
    /// so concurrent requests cannot all pass `check` against the same
    /// remaining allowance. The default is suitable for meters that are
    /// already atomic or are only used serially.
    fn check_and_record(&self, ops: u64, bytes: u64) -> Result<(), String> {
        self.check(ops, bytes)?;
        self.record(ops, bytes);
        Ok(())
    }

    /// Record traffic that crossed the provider boundary without admission.
    ///
    /// Ordinary requests use [`Self::check_and_record`]. This method remains
    /// necessary for cleanup requests and for non-streaming APIs that reveal
    /// additional provider-side pages only after they return.
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

    /// A non-sensitive label suitable for diagnostics.
    ///
    /// Userinfo, query, fragment, and path are excluded so credentials and
    /// object-like path components can never be written to logs.
    #[must_use]
    pub fn diagnostic_label(&self) -> String {
        let Ok(url) = Url::parse(&self.0) else {
            return "<redacted-remote>".to_string();
        };
        let Some(host) = url.host() else {
            return format!("{}://<remote>", url.scheme());
        };
        match url.port() {
            Some(port) => format!("{}://{}:{port}", url.scheme(), host),
            None => format!("{}://{}", url.scheme(), host),
        }
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

#[derive(Clone)]
struct MeterEntry {
    id: u64,
    meter: Arc<dyn StorageMeter>,
}

/// The budget bindings for each remote, in activation order.
static METERS: LazyLock<RwLock<HashMap<RemoteKey, Vec<MeterEntry>>>> =
    LazyLock::new(RwLock::default);
static NEXT_BINDING_ID: AtomicU64 = AtomicU64::new(1);

/// Traffic that reached a remote while no budget was bound to it, owed by the
/// next binding.
static ARREARS: LazyLock<RwLock<HashMap<RemoteKey, (u64, u64)>>> = LazyLock::new(RwLock::default);

/// Physical traffic seen per remote, whatever was or was not charged for it.
static OBSERVED: LazyLock<RwLock<HashMap<RemoteKey, Arc<Observation>>>> =
    LazyLock::new(RwLock::default);

const ACCESS_OPERATIONS: [AccessOperation; 7] = [
    AccessOperation::Get,
    AccessOperation::Head,
    AccessOperation::List,
    AccessOperation::Put,
    AccessOperation::Multipart,
    AccessOperation::Delete,
    AccessOperation::Copy,
];
const ACCESS_CLASSES: [AccessClass; 10] = [
    AccessClass::DeltaLog,
    AccessClass::DeltaObjects,
    AccessClass::DeltaCommits,
    AccessClass::DeltaRefs,
    AccessClass::DeltaMeta,
    AccessClass::PackIndexes,
    AccessClass::PackObjects,
    AccessClass::Blobs,
    AccessClass::Recovery,
    AccessClass::Other,
];

/// One physical object-store operation category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessOperation {
    /// Object body read.
    Get,
    /// Metadata-only object probe.
    Head,
    /// Prefix listing, including modeled pages.
    List,
    /// Single-request object write.
    Put,
    /// Multipart initiation, parts, completion, or cleanup.
    Multipart,
    /// Object deletion.
    Delete,
    /// Provider-side object copy.
    Copy,
}

impl AccessOperation {
    const fn index(self) -> usize {
        self as usize
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Head => "head",
            Self::List => "list",
            Self::Put => "put",
            Self::Multipart => "multipart",
            Self::Delete => "delete",
            Self::Copy => "copy",
        }
    }
}

/// A non-sensitive class of object-store path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessClass {
    /// Delta transaction-log objects.
    DeltaLog,
    /// Parquet files for the inline content-object partition.
    DeltaObjects,
    /// Parquet files for the derived commit-index partition.
    DeltaCommits,
    /// Parquet files for the content-ref partition.
    DeltaRefs,
    /// Parquet files for the remote-metadata partition.
    DeltaMeta,
    /// Pack-v3 index metadata.
    PackIndexes,
    /// Shared physical pack payloads.
    PackObjects,
    /// External content-addressed blobs.
    Blobs,
    /// Recovery capsule and recipe objects.
    Recovery,
    /// Paths outside the recognized storage layout.
    Other,
}

impl AccessClass {
    const fn index(self) -> usize {
        self as usize
    }

    const fn name(self) -> &'static str {
        match self {
            Self::DeltaLog => "delta_log",
            Self::DeltaObjects => "delta_objects",
            Self::DeltaCommits => "delta_commits",
            Self::DeltaRefs => "delta_refs",
            Self::DeltaMeta => "delta_meta",
            Self::PackIndexes => "pack_indexes",
            Self::PackObjects => "pack_objects",
            Self::Blobs => "blobs",
            Self::Recovery => "recovery",
            Self::Other => "other",
        }
    }
}

/// Request and byte totals for one operation or path class.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct AccessTotals {
    /// Modeled provider requests.
    pub ops: u64,
    /// Request and response body bytes.
    pub bytes: u64,
}

impl AccessTotals {
    fn add(&mut self, ops: u64, bytes: u64) {
        self.ops = self.ops.saturating_add(ops);
        self.bytes = self.bytes.saturating_add(bytes);
    }

    fn merge(&mut self, other: Self) {
        self.add(other.ops, other.bytes);
    }

    fn saturating_sub(self, earlier: Self) -> Self {
        Self {
            ops: self.ops.saturating_sub(earlier.ops),
            bytes: self.bytes.saturating_sub(earlier.bytes),
        }
    }
}

/// Process-local physical access shape for a remote.
///
/// Path classes deliberately reveal no object hashes or user content. Logical
/// query counters distinguish expensive one-key Delta queries from batched
/// current-closure reads without adding provider traffic.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AccessSummary {
    total: AccessTotals,
    operations: [AccessTotals; ACCESS_OPERATIONS.len()],
    classes: [AccessTotals; ACCESS_CLASSES.len()],
    /// Single-object Delta queries issued.
    pub object_point_queries: u64,
    /// Single-object queries that returned an inline object.
    pub object_point_hits: u64,
    /// Batched exact-set Delta queries issued.
    pub object_batch_queries: u64,
    /// Distinct hashes requested across batched queries.
    pub object_batch_keys: u64,
    /// Requested hashes returned by batched queries.
    pub object_batch_hits: u64,
}

impl AccessSummary {
    /// Total physical requests and transferred bytes.
    #[must_use]
    pub fn total(&self) -> AccessTotals {
        self.total
    }

    /// Physical totals attributed to `operation`.
    #[must_use]
    pub fn operation(&self, operation: AccessOperation) -> AccessTotals {
        self.operations[operation.index()]
    }

    /// Physical totals attributed to `class`.
    #[must_use]
    pub fn class(&self, class: AccessClass) -> AccessTotals {
        self.classes[class.index()]
    }

    fn add_access(&mut self, operation: AccessOperation, class: AccessClass, ops: u64, bytes: u64) {
        self.total.add(ops, bytes);
        self.operations[operation.index()].add(ops, bytes);
        self.classes[class.index()].add(ops, bytes);
    }

    fn merge(&mut self, other: &Self) {
        self.total.merge(other.total);
        for operation in ACCESS_OPERATIONS {
            self.operations[operation.index()].merge(other.operation(operation));
        }
        for class in ACCESS_CLASSES {
            self.classes[class.index()].merge(other.class(class));
        }
        self.object_point_queries = self
            .object_point_queries
            .saturating_add(other.object_point_queries);
        self.object_point_hits = self
            .object_point_hits
            .saturating_add(other.object_point_hits);
        self.object_batch_queries = self
            .object_batch_queries
            .saturating_add(other.object_batch_queries);
        self.object_batch_keys = self
            .object_batch_keys
            .saturating_add(other.object_batch_keys);
        self.object_batch_hits = self
            .object_batch_hits
            .saturating_add(other.object_batch_hits);
    }

    /// Difference between two monotonically increasing snapshots.
    #[must_use]
    pub fn saturating_sub(&self, earlier: &Self) -> Self {
        let mut difference = Self {
            total: self.total.saturating_sub(earlier.total),
            object_point_queries: self
                .object_point_queries
                .saturating_sub(earlier.object_point_queries),
            object_point_hits: self
                .object_point_hits
                .saturating_sub(earlier.object_point_hits),
            object_batch_queries: self
                .object_batch_queries
                .saturating_sub(earlier.object_batch_queries),
            object_batch_keys: self
                .object_batch_keys
                .saturating_sub(earlier.object_batch_keys),
            object_batch_hits: self
                .object_batch_hits
                .saturating_sub(earlier.object_batch_hits),
            ..Self::default()
        };
        for operation in ACCESS_OPERATIONS {
            difference.operations[operation.index()] = self
                .operation(operation)
                .saturating_sub(earlier.operation(operation));
        }
        for class in ACCESS_CLASSES {
            difference.classes[class.index()] =
                self.class(class).saturating_sub(earlier.class(class));
        }
        difference
    }
}

impl fmt::Display for AccessSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "total_ops={} total_bytes={}",
            self.total.ops, self.total.bytes
        )?;
        for operation in ACCESS_OPERATIONS {
            let totals = self.operation(operation);
            write!(
                f,
                " {}_ops={} {}_bytes={}",
                operation.name(),
                totals.ops,
                operation.name(),
                totals.bytes
            )?;
        }
        for class in ACCESS_CLASSES {
            let totals = self.class(class);
            write!(
                f,
                " {}_ops={} {}_bytes={}",
                class.name(),
                totals.ops,
                class.name(),
                totals.bytes
            )?;
        }
        write!(
            f,
            " object_point_queries={} object_point_hits={} object_batch_queries={} object_batch_keys={} object_batch_hits={}",
            self.object_point_queries,
            self.object_point_hits,
            self.object_batch_queries,
            self.object_batch_keys,
            self.object_batch_hits
        )
    }
}

#[derive(Clone, Copy)]
struct AccessEvent {
    operation: AccessOperation,
    class: AccessClass,
}

impl AccessEvent {
    fn new(operation: AccessOperation, location: &ObjectPath, prefix: Option<&ObjectPath>) -> Self {
        Self {
            operation,
            class: classify_path(location, prefix),
        }
    }
}

fn classify_path(location: &ObjectPath, prefix: Option<&ObjectPath>) -> AccessClass {
    let mut path = location.as_ref();
    if let Some(prefix) = prefix {
        let prefix = prefix.as_ref().trim_end_matches('/');
        if path == prefix {
            path = "";
        } else if let Some(relative) = path
            .strip_prefix(prefix)
            .and_then(|suffix| suffix.strip_prefix('/'))
        {
            path = relative;
        }
    }
    if path == "_delta_log" || path.starts_with("_delta_log/") {
        return AccessClass::DeltaLog;
    }
    if path == "_packs/v3" || path.starts_with("_packs/v3/") {
        return AccessClass::PackIndexes;
    }
    if path == "_packs/objects" || path.starts_with("_packs/objects/") {
        return AccessClass::PackObjects;
    }
    if path == "_blobs" || path.starts_with("_blobs/") {
        return AccessClass::Blobs;
    }
    if path == "recovery" || path.starts_with("recovery/") {
        return AccessClass::Recovery;
    }
    for component in path.split('/') {
        match component {
            "partition_key=objects" => return AccessClass::DeltaObjects,
            "partition_key=commits" => return AccessClass::DeltaCommits,
            "partition_key=refs" => return AccessClass::DeltaRefs,
            "partition_key=meta" => return AccessClass::DeltaMeta,
            _ => {}
        }
    }
    AccessClass::Other
}

fn classify_prefix(prefix: Option<&ObjectPath>, root_prefix: Option<&ObjectPath>) -> AccessClass {
    prefix.map_or(AccessClass::Other, |path| classify_path(path, root_prefix))
}

/// Bind `meter` to the remote at `key` until the returned value is dropped.
///
/// Every request any store makes to that remote, or to anything beneath it, is
/// charged here -- on whatever task, in whatever spawned corner of the Delta
/// layer it happens.
///
/// The newest live binding on one key wins. Bindings carry identities and are
/// removed from the stack by identity, so overlapping guards remain correct
/// even when they finish out of activation order.
#[must_use = "the budget is only bound while the binding is held"]
pub fn bind_meter(key: &RemoteKey, meter: Arc<dyn StorageMeter>) -> MeterBinding {
    let id = NEXT_BINDING_ID.fetch_add(1, Ordering::Relaxed);
    METERS
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .entry(key.clone())
        .or_default()
        .push(MeterEntry {
            id,
            meter: Arc::clone(&meter),
        });

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
        if meter.check_and_record(arrears.0, arrears.1).is_err() {
            // The traffic already happened. Preserve its cost even when the
            // inherited debt exceeds the newly bound budget; the meter keeps
            // the refusal so the guarded operation cannot report success.
            meter.record(arrears.0, arrears.1);
        }
    }

    MeterBinding {
        key: key.clone(),
        id,
        arrears,
    }
}

/// A budget bound to a remote, unbound when dropped.
pub struct MeterBinding {
    key: RemoteKey,
    id: u64,
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
        let mut remove_key = false;
        if let Some(entries) = meters.get_mut(&self.key) {
            if let Some(position) = entries.iter().position(|entry| entry.id == self.id) {
                entries.remove(position);
            }
            remove_key = entries.is_empty();
        }
        if remove_key {
            let _ = meters.remove(&self.key);
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
    matching_meter(&meters, key)
}

fn matching_meter(
    meters: &HashMap<RemoteKey, Vec<MeterEntry>>,
    key: &RemoteKey,
) -> Option<Arc<dyn StorageMeter>> {
    if meters.is_empty() {
        return None;
    }
    let mut candidate: &str = key.as_str();
    loop {
        if let Some(meter) = meters.get(candidate).and_then(|entries| entries.last()) {
            return Some(Arc::clone(&meter.meter));
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

/// Carry traffic that no live budget can claim into the next binding.
///
/// This is public for a bound meter whose lifetime has ended while an
/// already-open response stream still exists. Such traffic must become
/// arrears rather than disappearing into the retired meter.
pub fn record_arrears(key: &RemoteKey, ops: u64, bytes: u64) {
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

/// Detailed physical and logical access shape for `key` and its descendants.
#[must_use]
pub fn access_summary_under(key: &RemoteKey) -> AccessSummary {
    let observed = OBSERVED
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut summary = AccessSummary::default();
    for (_, observation) in observed.iter().filter(|(k, _)| k.is_under(key)) {
        summary.merge(&observation.summary());
    }
    summary
}

pub(crate) fn record_object_point_query(key: &RemoteKey, hit: bool) {
    observation(key).add_object_point_query(hit);
}

pub(crate) fn record_object_batch_query(key: &RemoteKey, requested: usize, returned: usize) {
    observation(key).add_object_batch_query(requested, returned);
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
    summary: std::sync::Mutex<AccessSummary>,
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

    fn summary(&self) -> AccessSummary {
        self.summary
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn add(&self, event: AccessEvent, ops: u64, bytes: u64) {
        let _ = self.ops.fetch_add(ops, Ordering::Relaxed);
        let _ = self.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.summary
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .add_access(event.operation, event.class, ops, bytes);
    }

    fn add_object_point_query(&self, hit: bool) {
        let mut summary = self
            .summary
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        summary.object_point_queries = summary.object_point_queries.saturating_add(1);
        if hit {
            summary.object_point_hits = summary.object_point_hits.saturating_add(1);
        }
    }

    fn add_object_batch_query(&self, requested: usize, returned: usize) {
        let mut summary = self
            .summary
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        summary.object_batch_queries = summary.object_batch_queries.saturating_add(1);
        summary.object_batch_keys = summary.object_batch_keys.saturating_add(requested as u64);
        summary.object_batch_hits = summary.object_batch_hits.saturating_add(returned as u64);
    }
}

/// Atomically reserve the budget before traffic reaches the provider.
fn admit(
    key: &RemoteKey,
    meter: Option<&Arc<dyn StorageMeter>>,
    event: AccessEvent,
    ops: u64,
    bytes: u64,
) -> ObjectStoreResult<()> {
    if let Some(meter) = meter {
        meter
            .check_and_record(ops, bytes)
            .map_err(|reason| ObjectStoreError::Generic {
                store: "metered",
                source: reason.into(),
            })?;
    } else {
        // Close the race between resolving no meter and a concurrent bind.
        // Holding the registry read lock until either the newly-visible meter
        // is charged or arrears are recorded ensures a binding cannot install
        // itself and drain arrears in between those two events.
        let meters = METERS
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(meter) = matching_meter(&meters, key) {
            meter
                .check_and_record(ops, bytes)
                .map_err(|reason| ObjectStoreError::Generic {
                    store: "metered",
                    source: reason.into(),
                })?;
        } else {
            record_arrears(key, ops, bytes);
        }
    }
    observation(key).add(event, ops, bytes);
    Ok(())
}

/// Charge a request that has been attempted.
///
/// Recorded regardless of whether the request succeeded: a request that
/// reached the provider and failed is still billed by the provider, and a
/// failing operation retried on a timer is exactly the runaway shape these
/// budgets exist to stop.  Charging only successes would make the worst case
/// free.
fn record(
    key: &RemoteKey,
    meter: Option<&Arc<dyn StorageMeter>>,
    event: AccessEvent,
    ops: u64,
    bytes: u64,
) {
    // Observed first and unconditionally: what happened is recorded whether or
    // not anything was watching, and under the key of the store that did it
    // rather than the budget that claimed it.
    observation(key).add(event, ops, bytes);
    match meter {
        Some(meter) => meter.record(ops, bytes),
        // Nothing claimed this traffic, so it is owed rather than forgiven.
        None => record_arrears(key, ops, bytes),
    }
}

/// An [`ObjectStore`] that charges the budget bound to its remote for the
/// requests and bytes it reserves before performing.
pub struct MeteredStore {
    inner: Arc<dyn ObjectStore>,
    /// The remote this store speaks to, fixed when it was built.  Attribution
    /// follows from this rather than from what is executing, which is what
    /// makes it survive every task boundary the Delta layer introduces.
    key: RemoteKey,
    /// Prefix delta-rs applies outside this root object store.
    prefix: Option<ObjectPath>,
}

impl MeteredStore {
    /// Wrap `inner` so its traffic is charged to the budget bound to `key`.
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>, key: RemoteKey) -> Self {
        Self {
            inner,
            key,
            prefix: None,
        }
    }

    /// Wrap `inner`, classifying requests relative to delta-rs's `prefix`.
    #[must_use]
    pub fn new_with_prefix(
        inner: Arc<dyn ObjectStore>,
        key: RemoteKey,
        prefix: ObjectPath,
    ) -> Self {
        Self {
            inner,
            key,
            prefix: Some(prefix),
        }
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
        let event = AccessEvent::new(AccessOperation::Put, location, self.prefix.as_ref());
        admit(&self.key, meter.as_ref(), event, 1, bytes)?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        let meter = self.meter();
        let event = AccessEvent::new(AccessOperation::Multipart, location, self.prefix.as_ref());
        admit(&self.key, meter.as_ref(), event, 1, 0)?;
        let upload = self.inner.put_multipart_opts(location, opts).await;
        Ok(Box::new(MeteredUpload {
            inner: upload?,
            key: self.key.clone(),
            meter,
            class: event.class,
        }))
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        let meter = self.meter();
        let is_head = options.head;
        let event = AccessEvent::new(
            if is_head {
                AccessOperation::Head
            } else {
                AccessOperation::Get
            },
            location,
            self.prefix.as_ref(),
        );
        admit(&self.key, meter.as_ref(), event, 1, 0)?;
        let result = self.inner.get_opts(location, options).await;
        let result = result?;

        // GetResult::range is the exact response range, unlike meta.size for
        // a ranged request. Refuse before exposing the body so a single GET
        // cannot consume an entire object after the byte budget is exhausted.
        let response_bytes = if is_head {
            0
        } else {
            result.range.end.saturating_sub(result.range.start)
        };
        // Reserve the complete response before exposing its body. Besides
        // stopping oversized reads before they stream, this makes concurrent
        // GET admission atomic and keeps an outliving stream paid for by the
        // guard under which it was opened.
        admit(&self.key, meter.as_ref(), event, 0, response_bytes)?;
        Ok(result)
    }

    async fn delete(&self, location: &ObjectPath) -> ObjectStoreResult<()> {
        let meter = self.meter();
        let event = AccessEvent::new(AccessOperation::Delete, location, self.prefix.as_ref());
        admit(&self.key, meter.as_ref(), event, 1, 0)?;
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let key = self.key.clone();
        let event = AccessEvent {
            operation: AccessOperation::List,
            class: classify_prefix(prefix, self.prefix.as_ref()),
        };
        let inner = self.inner.list(prefix);
        futures::stream::try_unfold(
            (inner, key, event, 0u64),
            |(mut inner, key, event, item_index)| async move {
                if item_index.is_multiple_of(LIST_PAGE_SIZE) {
                    let meter = meter_for(&key);
                    admit(&key, meter.as_ref(), event, 1, 0)?;
                }
                match inner.next().await {
                    Some(item) => {
                        item.map(|item| Some((item, (inner, key, event, item_index + 1))))
                    }
                    None => Ok(None),
                }
            },
        )
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> ObjectStoreResult<ListResult> {
        let meter = self.meter();
        let event = AccessEvent {
            operation: AccessOperation::List,
            class: classify_prefix(prefix, self.prefix.as_ref()),
        };
        admit(&self.key, meter.as_ref(), event, 1, 0)?;
        let result = self.inner.list_with_delimiter(prefix).await;
        let result = result?;
        let entries = result
            .objects
            .len()
            .saturating_add(result.common_prefixes.len()) as u64;
        let pages = entries.div_ceil(LIST_PAGE_SIZE).max(1);
        let additional_pages = pages.saturating_sub(1);
        if additional_pages > 0 {
            // This API returns all pages at once. A successful admission
            // reserves them; a refusal still records their already-incurred
            // cost before returning the error.
            if let Err(error) = admit(&self.key, meter.as_ref(), event, additional_pages, 0) {
                record(&self.key, meter.as_ref(), event, additional_pages, 0);
                return Err(error);
            }
        }
        Ok(result)
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> ObjectStoreResult<()> {
        let meter = self.meter();
        let event = AccessEvent::new(AccessOperation::Copy, to, self.prefix.as_ref());
        admit(&self.key, meter.as_ref(), event, 1, 0)?;
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> ObjectStoreResult<()> {
        let meter = self.meter();
        let event = AccessEvent::new(AccessOperation::Copy, to, self.prefix.as_ref());
        admit(&self.key, meter.as_ref(), event, 1, 0)?;
        self.inner.copy_if_not_exists(from, to).await
    }
}

/// A multipart upload whose parts are charged individually, because each part
/// is its own request and its own bytes.
#[derive(Debug)]
struct MeteredUpload {
    inner: Box<dyn MultipartUpload>,
    key: RemoteKey,
    meter: Option<Arc<dyn StorageMeter>>,
    class: AccessClass,
}

#[async_trait]
impl MultipartUpload for MeteredUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let bytes = data.content_length() as u64;
        let event = AccessEvent {
            operation: AccessOperation::Multipart,
            class: self.class,
        };
        if let Err(error) = admit(&self.key, self.meter.as_ref(), event, 1, bytes) {
            return Box::pin(async move { Err(error) });
        }
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> ObjectStoreResult<PutResult> {
        let event = AccessEvent {
            operation: AccessOperation::Multipart,
            class: self.class,
        };
        admit(&self.key, self.meter.as_ref(), event, 1, 0)?;
        self.inner.complete().await
    }

    async fn abort(&mut self) -> ObjectStoreResult<()> {
        // Cleanup must remain possible after the request budget is exhausted:
        // refusing an abort can leave staged multipart data accruing storage
        // charges.  Account for the request, but deliberately do not admit it.
        let event = AccessEvent {
            operation: AccessOperation::Multipart,
            class: self.class,
        };
        record(&self.key, self.meter.as_ref(), event, 1, 0);
        self.inner.abort().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::GetRange;
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

    #[test]
    fn access_paths_are_classified_without_exposing_object_names() {
        assert_eq!(
            classify_path(&ObjectPath::from("_delta_log/0000000001.json"), None),
            AccessClass::DeltaLog
        );
        assert_eq!(
            classify_path(
                &ObjectPath::from("pond_id=p/partition_key=objects/part-secret.parquet"),
                None
            ),
            AccessClass::DeltaObjects
        );
        assert_eq!(
            classify_path(
                &ObjectPath::from("pond_id=p/partition_key=commits/part-secret.parquet"),
                None
            ),
            AccessClass::DeltaCommits
        );
        assert_eq!(
            classify_path(
                &ObjectPath::from("pond_id=p/partition_key=refs/part-secret.parquet"),
                None
            ),
            AccessClass::DeltaRefs
        );
        assert_eq!(
            classify_path(
                &ObjectPath::from("_packs/v3/series=secret/pack=secret"),
                None
            ),
            AccessClass::PackIndexes
        );
        assert_eq!(
            classify_path(&ObjectPath::from("_packs/objects/secret"), None),
            AccessClass::PackObjects
        );
        assert_eq!(
            classify_path(&ObjectPath::from("_blobs/blob=secret"), None),
            AccessClass::Blobs
        );
        assert_eq!(
            classify_path(&ObjectPath::from("recovery/capsules/secret"), None),
            AccessClass::Recovery
        );
        assert_eq!(
            classify_path(&ObjectPath::from("unrecognized/secret"), None),
            AccessClass::Other
        );
        let prefix = ObjectPath::from("table/prefix");
        assert_eq!(
            classify_path(
                &ObjectPath::from("table/prefix/_delta_log/0000000001.json"),
                Some(&prefix)
            ),
            AccessClass::DeltaLog
        );
        assert_eq!(
            classify_path(
                &ObjectPath::from("table/prefix/_packs/objects/secret"),
                Some(&prefix)
            ),
            AccessClass::PackObjects
        );
    }

    #[test]
    fn diagnostic_remote_labels_exclude_credentials_and_paths() {
        let secret_hash = "a".repeat(64);
        let key = RemoteKey::new(&format!(
            "s3://user:password@example.invalid/private/{secret_hash}?token=secret"
        ));
        let label = key.diagnostic_label();
        assert_eq!(label, "s3://example.invalid");
        assert!(!label.contains("user"));
        assert!(!label.contains("password"));
        assert!(!label.contains(&secret_hash));
        assert!(!label.contains("token"));
    }

    #[tokio::test]
    async fn access_summary_breaks_down_operations_and_path_classes() {
        let (store, key) = store("access-summary-shape");
        let path = ObjectPath::from("_delta_log/0000000001.json");
        let before = access_summary_under(&key);

        store
            .put(&path, PutPayload::from_static(b"abc"))
            .await
            .expect("put");
        let result = store.get(&path).await.expect("get");
        assert_eq!(&result.bytes().await.expect("body")[..], b"abc");
        let _ = store.head(&path).await.expect("head");
        let listed = store
            .list(Some(&ObjectPath::from("_delta_log")))
            .collect::<Vec<_>>()
            .await;
        assert_eq!(listed.len(), 1);

        let summary = access_summary_under(&key).saturating_sub(&before);
        assert_eq!(summary.total(), AccessTotals { ops: 4, bytes: 6 });
        assert_eq!(
            summary.operation(AccessOperation::Get),
            AccessTotals { ops: 1, bytes: 3 }
        );
        assert_eq!(
            summary.operation(AccessOperation::Head),
            AccessTotals { ops: 1, bytes: 0 }
        );
        assert_eq!(
            summary.operation(AccessOperation::List),
            AccessTotals { ops: 1, bytes: 0 }
        );
        assert_eq!(
            summary.operation(AccessOperation::Put),
            AccessTotals { ops: 1, bytes: 3 }
        );
        assert_eq!(
            summary.class(AccessClass::DeltaLog),
            AccessTotals { ops: 4, bytes: 6 }
        );
        assert!(summary.to_string().contains("delta_log_ops=4"));
        assert!(summary.to_string().contains("delta_commits_ops=0"));
    }

    #[test]
    fn access_summary_records_logical_object_query_shape() {
        let key = RemoteKey::new("mem://logical-query-summary");
        let before = access_summary_under(&key);

        record_object_point_query(&key, true);
        record_object_point_query(&key, false);
        record_object_batch_query(&key, 12, 10);

        let summary = access_summary_under(&key).saturating_sub(&before);
        assert_eq!(summary.object_point_queries, 2);
        assert_eq!(summary.object_point_hits, 1);
        assert_eq!(summary.object_batch_queries, 1);
        assert_eq!(summary.object_batch_keys, 12);
        assert_eq!(summary.object_batch_hits, 10);
        assert_eq!(summary.total(), AccessTotals::default());
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

    #[tokio::test]
    async fn a_read_larger_than_the_byte_budget_is_refused_before_streaming() {
        let meter = Arc::new(ByteBudget {
            limit: 5,
            used: Mutex::new(0),
        });
        let (store, key) = store("read-preflight-budget");
        let path = ObjectPath::from("obj");
        store
            .inner
            .put(&path, PutPayload::from_static(b"0123456789"))
            .await
            .unwrap();

        let _binding = bind_meter(&key, meter.clone());
        let err = store
            .get(&path)
            .await
            .expect_err("response larger than the remaining budget must be refused");

        assert!(err.to_string().contains("byte budget exceeded"), "{err}");
        assert_eq!(
            *meter.used.lock().unwrap(),
            0,
            "a body refused from its response headers must not be consumed"
        );
    }

    #[tokio::test]
    async fn an_exact_byte_budget_allows_one_read_then_refuses_the_next() {
        let meter = Arc::new(ByteBudget {
            limit: 10,
            used: Mutex::new(0),
        });
        let (store, key) = store("read-exact-budget");
        let path = ObjectPath::from("obj");
        store
            .inner
            .put(&path, PutPayload::from_static(b"0123456789"))
            .await
            .unwrap();

        let _binding = bind_meter(&key, meter.clone());
        let first = store.get(&path).await.unwrap();
        assert_eq!(
            *meter.used.lock().unwrap(),
            10,
            "the complete response must be reserved before its body is exposed"
        );
        let first = first.bytes().await.unwrap();
        assert_eq!(&first[..], b"0123456789");

        let err = store
            .get(&path)
            .await
            .expect_err("the same object must not be readable twice on a one-object budget");
        assert!(err.to_string().contains("byte budget exceeded"), "{err}");
        assert_eq!(*meter.used.lock().unwrap(), 10);
    }

    #[tokio::test]
    async fn a_ranged_read_is_admitted_by_its_response_length() {
        let meter = Arc::new(ByteBudget {
            limit: 5,
            used: Mutex::new(0),
        });
        let (store, key) = store("read-range-budget");
        let path = ObjectPath::from("obj");
        store
            .inner
            .put(&path, PutPayload::from_static(b"0123456789"))
            .await
            .unwrap();

        let _binding = bind_meter(&key, meter.clone());
        let result = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(2..7)),
                    ..GetOptions::default()
                },
            )
            .await
            .expect("the five-byte response range fits the budget");
        assert_eq!(&result.bytes().await.unwrap()[..], b"23456");
        assert_eq!(*meter.used.lock().unwrap(), 5);
    }

    #[tokio::test]
    async fn head_does_not_spend_the_byte_budget() {
        let meter = Arc::new(ByteBudget {
            limit: 0,
            used: Mutex::new(0),
        });
        let (store, key) = store("head-byte-budget");
        let path = ObjectPath::from("obj");
        store
            .inner
            .put(&path, PutPayload::from_static(b"0123456789"))
            .await
            .unwrap();

        let _binding = bind_meter(&key, meter.clone());
        let meta = store
            .get_opts(
                &path,
                GetOptions {
                    head: true,
                    ..GetOptions::default()
                },
            )
            .await
            .expect("HEAD transfers no body bytes");
        assert_eq!(meta.meta.size, 10);
        assert_eq!(*meter.used.lock().unwrap(), 0);
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

    #[tokio::test]
    async fn a_spent_request_budget_refuses_a_lazy_list() {
        let meter = Arc::new(Counter {
            refuse: true,
            ..Counter::default()
        });
        let (store, key) = store("spent-list-budget-refuses");

        let _binding = bind_meter(&key, meter);
        let results = store.list(None).collect::<Vec<_>>().await;

        assert_eq!(results.len(), 1);
        let err = results[0]
            .as_ref()
            .expect_err("list admission must fail before yielding provider results");
        assert!(err.to_string().contains("budget spent"), "{err}");
    }

    #[tokio::test]
    async fn a_lazy_list_uses_the_meter_active_when_polled() {
        let meter = Arc::new(Counter::default());
        let (store, key) = store("lazy-list-binding");
        let path = ObjectPath::from("obj");
        store
            .inner
            .put(&path, PutPayload::from_static(b"x"))
            .await
            .unwrap();

        let listing = store.list(None);
        let binding = bind_meter(&key, meter.clone());
        let results = listing.collect::<Vec<_>>().await;
        drop(binding);

        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());
        assert_eq!(
            *meter.ops.lock().unwrap(),
            1,
            "meter selection must happen when the lazy request is polled"
        );
    }

    #[tokio::test]
    async fn a_paginated_list_stops_before_an_unbudgeted_page() {
        let meter = Arc::new(RequestBudget {
            limit: 1,
            used: Mutex::new(0),
        });
        let (store, key) = store("list-page-budget");
        for index in 0..=(2 * LIST_PAGE_SIZE + 5) {
            store
                .inner
                .put(
                    &ObjectPath::from(format!("object-{index:04}")),
                    PutPayload::from_static(b"x"),
                )
                .await
                .unwrap();
        }

        let _binding = bind_meter(&key, meter.clone());
        let results = store.list(None).collect::<Vec<_>>().await;

        assert_eq!(
            results.len(),
            LIST_PAGE_SIZE as usize + 1,
            "the list must end immediately after its first refused page"
        );
        assert!(results[..LIST_PAGE_SIZE as usize].iter().all(Result::is_ok));
        let err = results[LIST_PAGE_SIZE as usize]
            .as_ref()
            .expect_err("the second modeled page must be refused");
        assert!(err.to_string().contains("request budget exceeded"), "{err}");
        assert_eq!(*meter.used.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn list_with_delimiter_refuses_unbudgeted_modeled_pages() {
        let meter = Arc::new(RequestBudget {
            limit: 1,
            used: Mutex::new(0),
        });
        let (store, key) = store("delimiter-list-page-budget");
        for index in 0..=LIST_PAGE_SIZE {
            store
                .inner
                .put(
                    &ObjectPath::from(format!("prefix-{index:04}/object")),
                    PutPayload::from_static(b"x"),
                )
                .await
                .unwrap();
        }

        let _binding = bind_meter(&key, meter.clone());
        let err = store
            .list_with_delimiter(None)
            .await
            .expect_err("the unbudgeted second modeled page must be refused");

        assert!(err.to_string().contains("request budget exceeded"), "{err}");
        assert_eq!(
            *meter.used.lock().unwrap(),
            2,
            "pages already returned by the provider must remain charged"
        );
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

    #[tokio::test]
    async fn overlapping_same_key_bindings_can_finish_out_of_order() {
        let first = Arc::new(Counter::default());
        let second = Arc::new(Counter::default());
        let third = Arc::new(Counter::default());
        let (store, key) = store("overlapping-bindings");
        let path = ObjectPath::from("obj");

        let first_binding = bind_meter(&key, first.clone());
        let second_binding = bind_meter(&key, second.clone());
        drop(first_binding);

        store
            .put(&path, PutPayload::from_static(b"x"))
            .await
            .expect("newest live binding remains active");
        assert_eq!(*first.ops.lock().unwrap(), 0);
        assert_eq!(*second.ops.lock().unwrap(), 1);

        drop(second_binding);
        store
            .put(&path, PutPayload::from_static(b"y"))
            .await
            .expect("traffic is carried as arrears after every binding ends");
        assert_eq!(
            *second.ops.lock().unwrap(),
            1,
            "dropping the newest binding must not restore an expired meter"
        );

        let third_binding = bind_meter(&key, third.clone());
        assert_eq!(
            *third.ops.lock().unwrap(),
            1,
            "the next live binding must inherit the unclaimed request"
        );
        drop(third_binding);
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
