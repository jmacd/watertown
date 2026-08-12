// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the rate limiter against a real pond: policy read
//! from the pond, window held in the control table, enforcement on a live
//! push, and usage emitted back into the pond as a durable metric.
//!
//! See `docs/rate-limiter-design.md`.

use provider::factory::rate_limit::LimitUnit;
use steward::{
    LIMITER_USAGE_SERIES, Limiter, LimiterError, LimiterSet, LimiterUsageRow, Ship, StewardError,
    push_content_to_remote_limited,
};
use sync_store::ContentRemote;
use tempfile::tempdir;
use tinyfs::async_helpers::convenience::create_file_path;
use tlogfs::PondUserMetadata;

fn meta(label: &str) -> PondUserMetadata {
    PondUserMetadata::new(vec!["test".into(), label.into()])
}

async fn new_pond(label: &str) -> (tempfile::TempDir, Ship) {
    let tmp = tempdir().expect("tempdir");
    let ship = Ship::create_pond(tmp.path().join("pond"), label)
        .await
        .expect("create pond");
    (tmp, ship)
}

async fn write_file(ship: &mut Ship, path: &str, bytes: &[u8]) {
    let bytes = bytes.to_vec();
    ship.write_transaction(&meta("write"), async move |fs| {
        let root = fs.root().await?;
        let _ = create_file_path(&root, path, &bytes).await?;
        Ok(())
    })
    .await
    .expect("write transaction");
}

/// Create a `rate-limit` node by writing its config YAML, which is exactly
/// what `pond apply` stores and what the limiter parses back out.
async fn write_limiter(ship: &mut Ship, path: &str, unit: &str, limit: f64, burst: Option<f64>) {
    let mut yaml = format!("unit: {unit}\nlimit: {limit}\n");
    if let Some(b) = burst {
        yaml.push_str(&format!("burst: {b}\n"));
    }
    write_file(ship, path, yaml.as_bytes()).await;
}

async fn read_usage_rows(ship: &mut Ship) -> Vec<LimiterUsageRow> {
    use tinyfs::arrow::parquet::ParquetExt;

    let tx = ship.begin_read(&meta("read-usage")).await.expect("read");
    let root = tx.root().await.expect("root");
    let out = if root.exists(LIMITER_USAGE_SERIES).await {
        root.read_table_as_items::<LimiterUsageRow, _>(LIMITER_USAGE_SERIES)
            .await
            .expect("read usage series")
    } else {
        Vec::new()
    };
    let _ = tx.commit().await;
    out
}

/// The policy is read from the pond and the window from control, so a limiter
/// bound twice in a row sees what the first one spent.
#[tokio::test]
async fn a_window_survives_rebinding_through_the_control_table() {
    let (_t, mut ship) = new_pond("limiter-window").await;
    write_limiter(&mut ship, "/quota", "MiB/day", 10.0, None).await;

    let mut l = Limiter::open(&mut ship, "/quota", LimitUnit::Bytes)
        .await
        .expect("open");
    l.record(4 * 1024 * 1024);
    l.record_observed(5 * 1024 * 1024);
    l.commit(ship.control_table_mut()).await.expect("commit");

    let again = Limiter::open(&mut ship, "/quota", LimitUnit::Bytes)
        .await
        .expect("reopen");
    assert_eq!(again.state().used, 4 * 1024 * 1024);
    assert_eq!(again.state().observed, 5 * 1024 * 1024);
    assert!(again.state().observed_since_us.is_some());
    assert_eq!(again.state().limit, 10 * 1024 * 1024);
}

/// Decision L10: the caller declares the dimension it spends and a limiter
/// governing a different one refuses to bind, rather than silently counting
/// bytes against an operation budget.
#[tokio::test]
async fn binding_the_wrong_dimension_is_refused() {
    let (_t, mut ship) = new_pond("limiter-unit").await;
    write_limiter(&mut ship, "/quota", "MiB/day", 10.0, None).await;

    let err = Limiter::open(&mut ship, "/quota", LimitUnit::Ops)
        .await
        .expect_err("unit mismatch");
    assert!(matches!(err, LimiterError::UnitMismatch { .. }), "{err:?}");
}

#[tokio::test]
async fn binding_a_missing_node_is_an_error_not_an_unlimited_budget() {
    let (_t, mut ship) = new_pond("limiter-missing").await;
    let err = Limiter::open(&mut ship, "/nope", LimitUnit::Bytes)
        .await
        .expect_err("missing node");
    assert!(matches!(err, LimiterError::NotFound { .. }), "{err:?}");
}

/// A limiter with room does not change the outcome of a push.
#[tokio::test]
async fn a_generous_limit_does_not_disturb_the_push() {
    let (_t, mut ship) = new_pond("limiter-push-ok").await;
    write_limiter(&mut ship, "/quota", "GiB/day", 1.0, None).await;
    write_file(&mut ship, "/a.txt", b"alpha").await;

    let pond_id = uuid::Uuid::parse_str(ship.data_persistence().pond_id()).expect("pond id");
    let mut remote = ContentRemote::create_at_url(
        &sync_store::testing::in_memory_remote_url("push-ok"),
        pond_id,
        [].into(),
    )
    .await
    .expect("create remote");

    let mut limits = LimiterSet::open(&mut ship, &[(LimitUnit::Bytes, "/quota".to_string())])
        .await
        .expect("bind");
    let outcome = push_content_to_remote_limited(&ship, &mut remote, "main", &mut limits)
        .await
        .expect("push");
    assert!(outcome.objects_pushed >= 1);

    // The push spent real bytes against the budget.
    let spent = limits.states()[0].used;
    assert!(spent > 0, "a push must charge the byte budget");
    limits
        .commit(ship.control_table_mut())
        .await
        .expect("commit");
}

/// Decision L8: an exhausted budget stops the push, and it stops it as
/// `RateLimited` -- a throttle, distinguishable from a fault.
#[tokio::test]
async fn an_exhausted_budget_stops_the_push() {
    let (_t, mut ship) = new_pond("limiter-push-deny").await;
    // One byte per day: anything at all is over budget.
    write_limiter(&mut ship, "/quota", "B/day", 1.0, None).await;
    write_file(&mut ship, "/a.txt", b"alpha").await;

    let pond_id = uuid::Uuid::parse_str(ship.data_persistence().pond_id()).expect("pond id");
    let mut remote = ContentRemote::create_at_url(
        &sync_store::testing::in_memory_remote_url("push-denied"),
        pond_id,
        [].into(),
    )
    .await
    .expect("create remote");

    let mut limits = LimiterSet::open(&mut ship, &[(LimitUnit::Bytes, "/quota".to_string())])
        .await
        .expect("bind");
    let err = push_content_to_remote_limited(&ship, &mut remote, "main", &mut limits)
        .await
        .expect_err("budget exhausted");

    assert!(
        matches!(err, StewardError::RateLimited(_)),
        "a throttle must not look like a fault: {err:?}"
    );
    let text = err.to_string();
    assert!(text.contains("/quota"), "{text}");
    assert!(text.contains("retry in"), "{text}");
}

/// A large blob is admitted one multipart part at a time.  The provider must
/// never receive the part that would cross either configured byte ceiling.
#[tokio::test]
async fn a_multipart_blob_cannot_overrun_its_window_or_burst() {
    const MIB: usize = 1024 * 1024;

    let (_t, mut ship) = new_pond("limiter-multipart-deny").await;
    write_limiter(&mut ship, "/quota", "MiB/day", 6.0, Some(6.0)).await;
    let body = vec![b'x'; 12 * MIB];
    let hash = sync_store::content::ObjectHash::of_bytes(&body);
    write_file(&mut ship, "/large.bin", &body).await;

    let pond_id = uuid::Uuid::parse_str(ship.data_persistence().pond_id()).expect("pond id");
    let mut remote = ContentRemote::create_at_url(
        &sync_store::testing::in_memory_remote_url("push-multipart-denied"),
        pond_id,
        [].into(),
    )
    .await
    .expect("create remote");

    let mut limits = LimiterSet::open(&mut ship, &[(LimitUnit::Bytes, "/quota".to_string())])
        .await
        .expect("bind");
    let err = push_content_to_remote_limited(&ship, &mut remote, "main", &mut limits)
        .await
        .expect_err("multipart blob must exceed its budget");

    assert!(matches!(err, StewardError::RateLimited(_)), "{err:?}");
    assert!(
        limits.states()[0].used <= 6 * MIB as u64,
        "denied push charged past its configured ceiling: {:?}",
        limits.states()[0]
    );
    assert!(
        !remote.has_blob(hash).await.expect("probe refused blob"),
        "a refused multipart blob must not become visible"
    );
}

/// An ungoverned dimension costs nothing at all: no pond read, no control
/// read, no control write.
#[tokio::test]
async fn an_unlimited_set_never_refuses() {
    let (_t, mut ship) = new_pond("limiter-unlimited").await;
    let limits = LimiterSet::open(&mut ship, &[]).await.expect("bind none");
    assert!(limits.is_unlimited());
    assert!(limits.check(LimitUnit::Bytes, u64::MAX).is_ok());
    assert!(limits.states().is_empty());
}

/// Decision L12: spending is queued in control, then flushed into the pond by
/// the next write transaction -- because the push that spends it cannot write
/// the pond without recursing.
#[tokio::test]
async fn usage_reaches_the_pond_on_the_next_write() {
    let (_t, mut ship) = new_pond("limiter-usage").await;
    write_limiter(&mut ship, "/quota", "MiB/day", 10.0, None).await;

    let mut l = Limiter::open(&mut ship, "/quota", LimitUnit::Bytes)
        .await
        .expect("open");
    l.record(1024);
    l.commit(ship.control_table_mut()).await.expect("commit");

    // Nothing has been written since, so the sample is still only in control.
    assert!(
        read_usage_rows(&mut ship).await.is_empty(),
        "usage must not appear before a write carries it"
    );

    // Any write at all carries it out.
    write_file(&mut ship, "/unrelated.txt", b"x").await;

    let rows = read_usage_rows(&mut ship).await;
    assert_eq!(rows.len(), 1, "expected one usage row, got {rows:?}");
    assert_eq!(rows[0].limiter, "/quota");
    assert_eq!(rows[0].unit, "bytes");
    assert_eq!(rows[0].amount, 1024, "amount is the delta spent");
    assert_eq!(rows[0].used, 1024, "used is the window total");
    assert_eq!(
        rows[0].limit,
        10 * 1024 * 1024,
        "limit travels with the row"
    );
    assert_eq!(rows[0].window_secs, 86_400);
}

/// The queue is drained by the write that emitted it, so a second write does
/// not duplicate the sample.
#[tokio::test]
async fn emitted_usage_is_not_emitted_twice() {
    let (_t, mut ship) = new_pond("limiter-usage-once").await;
    write_limiter(&mut ship, "/quota", "MiB/day", 10.0, None).await;

    let mut l = Limiter::open(&mut ship, "/quota", LimitUnit::Bytes)
        .await
        .expect("open");
    l.record(2048);
    l.commit(ship.control_table_mut()).await.expect("commit");

    write_file(&mut ship, "/one.txt", b"1").await;
    write_file(&mut ship, "/two.txt", b"2").await;

    let rows = read_usage_rows(&mut ship).await;
    assert_eq!(rows.len(), 1, "sample emitted more than once: {rows:?}");
}

/// A limiter that admitted everything but spent nothing produces no row: an
/// idle pond should not manufacture metric traffic.
#[tokio::test]
async fn spending_nothing_emits_nothing() {
    let (_t, mut ship) = new_pond("limiter-usage-idle").await;
    write_limiter(&mut ship, "/quota", "MiB/day", 10.0, None).await;

    let mut l = Limiter::open(&mut ship, "/quota", LimitUnit::Bytes)
        .await
        .expect("open");
    l.check(1024).expect("admitted");
    l.commit(ship.control_table_mut()).await.expect("commit");

    write_file(&mut ship, "/one.txt", b"1").await;
    assert!(read_usage_rows(&mut ship).await.is_empty());
}

/// Both dimensions accumulate independently and both reach the pond.
#[tokio::test]
async fn each_dimension_reports_separately() {
    let (_t, mut ship) = new_pond("limiter-usage-two").await;
    write_limiter(&mut ship, "/bytes", "MiB/day", 10.0, None).await;
    write_limiter(&mut ship, "/ops", "iops/hour", 100.0, None).await;

    let mut limits = LimiterSet::open(
        &mut ship,
        &[
            (LimitUnit::Bytes, "/bytes".to_string()),
            (LimitUnit::Ops, "/ops".to_string()),
        ],
    )
    .await
    .expect("bind");

    limits.record(LimitUnit::Bytes, 4096);
    limits.record(LimitUnit::Ops, 3);
    limits
        .commit(ship.control_table_mut())
        .await
        .expect("commit");

    write_file(&mut ship, "/one.txt", b"1").await;

    let mut rows = read_usage_rows(&mut ship).await;
    rows.sort_by(|a, b| a.limiter.cmp(&b.limiter));
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(rows[0].limiter, "/bytes");
    assert_eq!(rows[0].unit, "bytes");
    assert_eq!(rows[0].amount, 4096);
    assert_eq!(rows[1].limiter, "/ops");
    assert_eq!(rows[1].unit, "ops");
    assert_eq!(rows[1].amount, 3);
}

/// A re-push must not cost a request per blob the pond has ever retained.
///
/// Confirming blob presence with a probe per blob makes every push cost a
/// billed request for each blob in the accumulated history, even when the push
/// uploads nothing at all -- a bill proportional to how long the pond has
/// existed rather than to what it just did.  A single listing answers the same
/// question, so a no-op re-push spends the same regardless of how many blobs
/// are retained.  This is the finding the ops budget surfaced in staging; the
/// test exists so it cannot come back unnoticed.
///
/// The claim is tested as an invariance rather than against a threshold: what
/// matters is that tripling the retained blobs does not change the bill, and a
/// fixed number would only ever be a guess about what the constant should be.
#[tokio::test]
async fn a_re_push_does_not_pay_per_retained_blob() {
    let few = no_op_re_push_cost(4, "push-listing-few").await;
    let many = no_op_re_push_cost(12, "push-listing-many").await;
    assert_eq!(
        few, many,
        "a no-op re-push cost {few} requests over 4 retained blobs and {many} over 12; \
         cost that grows with retained blobs is the per-blob probing this listing replaced"
    );
}

/// Push `blobs` external blobs to a fresh remote, then push again with nothing
/// to do, and report what the second push physically cost.
async fn no_op_re_push_cost(blobs: usize, remote_name: &str) -> u64 {
    let (_t, mut ship) = new_pond(&format!("limiter-{remote_name}")).await;
    // Generous enough that nothing is refused: the point here is what is
    // *spent*, not what is denied.
    write_limiter(&mut ship, "/ops", "ops/day", 100_000.0, None).await;

    // Each file must exceed the 64 KB inline threshold to become an external
    // blob (Decision D7); distinct contents keep their hashes distinct.
    for i in 0..blobs {
        let body = vec![b'a' + u8::try_from(i).expect("small index"); 100 * 1024];
        write_file(&mut ship, &format!("/big{i}.bin"), &body).await;
    }

    let pond_id = uuid::Uuid::parse_str(ship.data_persistence().pond_id()).expect("pond id");
    let mut remote = ContentRemote::create_at_url(
        &sync_store::testing::in_memory_remote_url(remote_name),
        pond_id,
        [].into(),
    )
    .await
    .expect("create remote");

    // First push: uploads are genuine work and are expected to cost per blob.
    let mut limits = LimiterSet::open(&mut ship, &[(LimitUnit::Ops, "/ops".to_string())])
        .await
        .expect("bind");
    let _ = push_content_to_remote_limited(&ship, &mut remote, "main", &mut limits)
        .await
        .expect("first push");
    let after_first = limits.states()[0].used;
    assert!(
        after_first >= blobs as u64,
        "the first push should have uploaded {blobs} blobs, spent {after_first}"
    );
    limits
        .commit(ship.control_table_mut())
        .await
        .expect("commit");

    // Second push: the remote already holds every blob, so nothing is
    // transferred and the cost must not scale with how many are held.
    let mut limits = LimiterSet::open(&mut ship, &[(LimitUnit::Ops, "/ops".to_string())])
        .await
        .expect("rebind");
    let before_second = limits.states()[0].used;
    let _ = push_content_to_remote_limited(&ship, &mut remote, "main", &mut limits)
        .await
        .expect("second push");
    limits.states()[0].used - before_second
}
