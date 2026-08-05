// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! End-to-end tests for `pond apply` `kind: backup` / `kind: remote`
//! (Decisions L9/L11): authoring a governed attachment declaratively, in the
//! same document that creates the limiters governing it.
//!
//! The point of L11 is that YAML authoring is a *surface*, not a second
//! implementation: an attachment created by `pond apply` must be
//! indistinguishable from one created by `pond backup add`, and must be
//! governed by the limiters it names.  These tests assert both halves.
//!
//! All remotes are `file://`, so nothing here touches the network.

use cmd::commands::{
    apply_command, init_command, push_command, remote::list_remote_names,
    remote::load_remote_attachment, status_command,
};
use cmd::common::ShipContext;
use provider::factory::rate_limit::LimitUnit;
use std::sync::Once;
use steward::{PondUserMetadata, REMOTE_MODE_PREFIX, RemoteMode};
use tempfile::TempDir;
use tinyfs::EntryType;
use tokio::io::AsyncWriteExt;

static INIT_LOG: Once = Once::new();
fn init_log() {
    INIT_LOG.call_once(|| {
        let _ = env_logger::builder().is_test(true).try_init();
    });
}

fn ctx_for(pond_path: &std::path::Path, args: Vec<&str>) -> ShipContext {
    ShipContext {
        pond_path: Some(pond_path.to_path_buf()),
        host_root: None,
        mount_specs: Vec::new(),
        original_args: args.into_iter().map(String::from).collect(),
    }
}

/// Write `yaml` to a file under `dir` and run `pond apply -f` on it.
async fn apply_yaml(ctx: &ShipContext, dir: &std::path::Path, yaml: &str) -> anyhow::Result<()> {
    let path = dir.join(format!("apply-{}.yaml", uuid::Uuid::new_v4()));
    std::fs::write(&path, yaml)?;
    apply_command(ctx, &[path.to_string_lossy().to_string()]).await
}

async fn write_small_file(ctx: &ShipContext, path: &str, bytes: &[u8]) -> anyhow::Result<()> {
    let mut ship = ctx.open_pond().await?;
    ship.write_transaction(
        &PondUserMetadata::new(vec!["copy".to_string(), path.to_string()]),
        async |fs| {
            let root = fs.root().await?;
            let mut w = root
                .async_writer_path_with_type(path, EntryType::FilePhysicalVersion)
                .await?;
            w.write_all(bytes)
                .await
                .map_err(|e| steward::StewardError::Aborted(format!("write: {}", e)))?;
            w.shutdown()
                .await
                .map_err(|e| steward::StewardError::Aborted(format!("close: {}", e)))?;
            Ok(())
        },
    )
    .await?;
    Ok(())
}

/// One document creating a generous byte budget, one creating an ops budget,
/// and one attaching a backup governed by both.
fn governed_backup_yaml(url: &str, mib_per_day: u64, ops_per_hour: u64) -> String {
    format!(
        r#"version: v1
kind: mknod
metadata:
  path: /sys/limits/backup-bytes
spec:
  factory: rate-limit
  config:
    unit: MiB/day
    limit: {mib_per_day}
    burst: 1
---
version: v1
kind: mknod
metadata:
  path: /sys/limits/backup-ops
spec:
  factory: rate-limit
  config:
    unit: ops/hour
    limit: {ops_per_hour}
    burst: 1
---
version: v1
kind: backup
metadata:
  path: /sys/remotes/origin
spec:
  url: {url}
  limits:
    bytes: /sys/limits/backup-bytes
    ops: /sys/limits/backup-ops
"#
    )
}

/// A governed backup authored entirely in YAML behaves like one authored with
/// `pond backup add`: it attaches, it records its limiter bindings, and a push
/// under a generous budget succeeds.
///
/// The limiters are created by `mknod` documents in the *same* file as the
/// attachment that names them, which is the case the phase ordering exists
/// for: attaching validates the limiter paths, so the nodes must already be
/// committed.
#[tokio::test]
async fn apply_creates_a_governed_backup_in_one_document() {
    init_log();
    let tmp = TempDir::new().expect("tmp");
    let pond = tmp.path().join("pond");
    let remote = tmp.path().join("remote");
    std::fs::create_dir_all(&remote).expect("mkdir remote");
    let url = format!("file://{}", remote.display());

    let ctx = ctx_for(&pond, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");
    write_small_file(&ctx, "/hello.txt", b"hello from an applied pond")
        .await
        .expect("write");

    apply_yaml(&ctx, tmp.path(), &governed_backup_yaml(&url, 100, 10_000))
        .await
        .expect("apply governed backup");

    let mut ship = ctx.open_pond().await.expect("open");
    let names = list_remote_names(&mut ship).await.expect("list");
    assert_eq!(names, vec!["origin".to_string()], "attachment not created");

    let attachment = load_remote_attachment(&mut ship, "origin")
        .await
        .expect("load");
    assert_eq!(attachment.url, url);

    // The bindings survived the round trip through the replicated document,
    // and resolve to the dimensions the limiters were created with.
    let mut limits = attachment.resolved_limits().expect("resolve limits");
    limits.sort_by_key(|(u, _)| format!("{u}"));
    assert_eq!(
        limits,
        vec![
            (LimitUnit::Bytes, "/sys/limits/backup-bytes".to_string()),
            (LimitUnit::Ops, "/sys/limits/backup-ops".to_string()),
        ]
    );
    drop(ship);

    push_command(&ctx, Some("origin".to_string()))
        .await
        .expect("push under a generous budget must succeed");
}

/// The binding is not decorative: a backup attached by `pond apply` naming a
/// budget of one byte per day cannot push.
///
/// This is the failure the whole feature exists to prevent -- a runaway
/// process spending remote resources -- reached entirely through the
/// declarative surface.
#[tokio::test]
async fn an_applied_backup_is_actually_governed() {
    init_log();
    let tmp = TempDir::new().expect("tmp");
    let pond = tmp.path().join("pond");
    let remote = tmp.path().join("remote");
    std::fs::create_dir_all(&remote).expect("mkdir remote");
    let url = format!("file://{}", remote.display());

    let ctx = ctx_for(&pond, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");
    write_small_file(&ctx, "/hello.txt", b"hello from a throttled pond")
        .await
        .expect("write");

    // One op per hour: the attach itself spends nothing, but the first push
    // needs several remote operations.
    apply_yaml(&ctx, tmp.path(), &governed_backup_yaml(&url, 100, 1))
        .await
        .expect("apply");

    let err = push_command(&ctx, Some("origin".to_string()))
        .await
        .expect_err("push must be refused by the ops budget");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("rate limit") || msg.contains("retry in"),
        "expected a rate-limit refusal, got: {msg}"
    );
}

/// `kind: remote` requires a mount, because a pull-mode attachment has to say
/// where the imported pond lands.
#[tokio::test]
async fn a_remote_without_a_mount_is_refused() {
    init_log();
    let tmp = TempDir::new().expect("tmp");
    let pond = tmp.path().join("pond");
    let ctx = ctx_for(&pond, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");

    let err = apply_yaml(
        &ctx,
        tmp.path(),
        "version: v1\nkind: remote\nmetadata:\n  path: /sys/remotes/water\nspec:\n  url: file:///tmp/nope\n",
    )
    .await
    .expect_err("mountless remote must be refused");
    assert!(
        format!("{err:#}").contains("requires 'mount'"),
        "unexpected error: {err:#}"
    );
}

/// A backup mirrors the whole pond, so there is nothing for a mount to mean.
/// Accepting one silently would leave an operator believing a subtree was
/// being backed up when the entire pond was.
#[tokio::test]
async fn a_backup_with_a_mount_is_refused() {
    init_log();
    let tmp = TempDir::new().expect("tmp");
    let pond = tmp.path().join("pond");
    let ctx = ctx_for(&pond, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");

    let err = apply_yaml(
        &ctx,
        tmp.path(),
        "version: v1\nkind: backup\nmetadata:\n  path: /sys/remotes/origin\nspec:\n  url: file:///tmp/nope\n  mount: /sources/x\n",
    )
    .await
    .expect_err("mounted backup must be refused");
    assert!(
        format!("{err:#}").contains("not valid for kind 'backup'"),
        "unexpected error: {err:#}"
    );
}

/// An unknown limit dimension must fail at apply time, not at push time.  A
/// typo'd budget that is silently ignored is an ungoverned remote, which is
/// exactly the state this feature exists to make impossible.
#[tokio::test]
async fn an_unknown_limit_dimension_is_refused_at_apply_time() {
    init_log();
    let tmp = TempDir::new().expect("tmp");
    let pond = tmp.path().join("pond");
    let ctx = ctx_for(&pond, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");

    let err = apply_yaml(
        &ctx,
        tmp.path(),
        "version: v1\nkind: backup\nmetadata:\n  path: /sys/remotes/origin\nspec:\n  url: file:///tmp/nope\n  limits:\n    megabytes: /sys/limits/x\n",
    )
    .await
    .expect_err("unknown dimension must be refused");
    assert!(
        format!("{err:#}").contains("megabytes"),
        "unexpected error: {err:#}"
    );

    let mut ship = ctx.open_pond().await.expect("open");
    assert!(
        list_remote_names(&mut ship).await.expect("list").is_empty(),
        "a refused attachment must leave no trace"
    );
}

/// Naming a limiter that does not exist must fail loudly.  Treating a missing
/// budget as "unlimited" would turn a typo into an ungoverned remote.
#[tokio::test]
async fn a_missing_limiter_node_is_refused() {
    init_log();
    let tmp = TempDir::new().expect("tmp");
    let pond = tmp.path().join("pond");
    let remote = tmp.path().join("remote");
    std::fs::create_dir_all(&remote).expect("mkdir remote");
    let ctx = ctx_for(&pond, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");
    write_small_file(&ctx, "/hello.txt", b"x")
        .await
        .expect("write");

    // The attachment is well-formed, so it applies; the *push* is what
    // discovers the budget does not exist -- and refuses rather than
    // proceeding ungoverned.
    apply_yaml(
        &ctx,
        tmp.path(),
        &format!(
            "version: v1\nkind: backup\nmetadata:\n  path: /sys/remotes/origin\nspec:\n  url: file://{}\n  limits:\n    bytes: /sys/limits/does-not-exist\n",
            remote.display()
        ),
    )
    .await
    .expect("apply");

    let err = push_command(&ctx, Some("origin".to_string()))
        .await
        .expect_err("push with a missing limiter must be refused");
    assert!(
        format!("{err:#}").contains("does-not-exist"),
        "unexpected error: {err:#}"
    );
}

/// `bidirectional: true` selects `RemoteMode::Both` on a backup, and is
/// rejected on a `kind: remote`, matching the CLI verbs it replaces.
#[tokio::test]
async fn bidirectional_selects_both_on_a_backup_and_is_refused_on_a_remote() {
    init_log();
    let tmp = TempDir::new().expect("tmp");
    let pond = tmp.path().join("pond");
    let remote = tmp.path().join("remote");
    std::fs::create_dir_all(&remote).expect("mkdir remote");
    let ctx = ctx_for(&pond, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");

    apply_yaml(
        &ctx,
        tmp.path(),
        &format!(
            "version: v1\nkind: backup\nmetadata:\n  path: /sys/remotes/origin\nspec:\n  url: file://{}\n  bidirectional: true\n",
            remote.display()
        ),
    )
    .await
    .expect("apply bidirectional backup");

    let ship = ctx.open_pond().await.expect("open");
    let mode = ship
        .control_table()
        .raw_config_get(&format!("{REMOTE_MODE_PREFIX}origin"))
        .await
        .expect("get mode")
        .expect("mode set");
    assert_eq!(mode, RemoteMode::Both.as_str());
    drop(ship);

    let err = apply_yaml(
        &ctx,
        tmp.path(),
        "version: v1\nkind: remote\nmetadata:\n  path: /sys/remotes/water\nspec:\n  url: file:///tmp/nope\n  mount: /sources/water\n  bidirectional: true\n",
    )
    .await
    .expect_err("bidirectional remote must be refused");
    assert!(
        format!("{err:#}").contains("not valid for kind 'remote'"),
        "unexpected error: {err:#}"
    );
}

// ===========================================================================
// Storage profiles (docs/storage-profile-design.md)
// ===========================================================================

/// A profile and an inline connection field together must be refused at apply
/// time (Decision A4).  Silently preferring one would give an operator a
/// working pond talking to the wrong storage.
#[tokio::test]
async fn a_profile_plus_inline_credentials_is_refused() {
    init_log();
    let tmp = TempDir::new().expect("tmp");
    let pond = tmp.path().join("pond");
    let ctx = ctx_for(&pond, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");

    let err = apply_yaml(
        &ctx,
        tmp.path(),
        "version: v1\nkind: backup\nmetadata:\n  path: /sys/remotes/origin\nspec:\n  url: s3://bucket\n  storage: /sys/storage/minio\n  endpoint: http://elsewhere:9000\n",
    )
    .await
    .expect_err("conflicting authoring styles must be refused");
    assert!(
        format!("{err:#}").contains("endpoint"),
        "the conflicting field should be named: {err:#}"
    );
}

/// A literal credential in a profile must be refused at `pond apply` time
/// (Decision A1).  A profile node is replicated, and is more inviting to
/// `pond cat` than an attachment ever was.
#[tokio::test]
async fn a_literal_credential_in_a_profile_is_refused() {
    init_log();
    let tmp = TempDir::new().expect("tmp");
    let pond = tmp.path().join("pond");
    let ctx = ctx_for(&pond, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");

    let err = apply_yaml(
        &ctx,
        tmp.path(),
        "version: v1\nkind: mknod\nmetadata:\n  path: /sys/storage/minio\nspec:\n  factory: storage-minio\n  config:\n    endpoint: http://watershop:9000\n    access_key_id: ${env:S3_ACCESS_KEY}\n    secret_access_key: hunter2\n",
    )
    .await
    .expect_err("a literal secret must be refused");
    assert!(
        format!("{err:#}").contains("secret_access_key"),
        "unexpected error: {err:#}"
    );
}

/// A `storage-minio` profile does not serve a `file://` URL, and saying so at
/// attach time beats an opaque failure on the first push.
#[tokio::test]
async fn a_profile_that_does_not_serve_the_url_is_refused_at_attach() {
    init_log();
    let tmp = TempDir::new().expect("tmp");
    let pond = tmp.path().join("pond");
    let remote = tmp.path().join("remote");
    std::fs::create_dir_all(&remote).expect("mkdir remote");
    let ctx = ctx_for(&pond, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");

    let err = apply_yaml(
        &ctx,
        tmp.path(),
        &format!(
            "version: v1\nkind: mknod\nmetadata:\n  path: /sys/storage/minio\nspec:\n  factory: storage-minio\n  config:\n    endpoint: http://watershop:9000\n    access_key_id: ${{env:PATH}}\n    secret_access_key: ${{env:PATH}}\n---\nversion: v1\nkind: backup\nmetadata:\n  path: /sys/remotes/origin\nspec:\n  url: file://{}\n  storage: /sys/storage/minio\n",
            remote.display()
        ),
    )
    .await
    .expect_err("a minio profile must not serve file://");
    assert!(
        format!("{err:#}").contains("does not serve"),
        "unexpected error: {err:#}"
    );
}

/// Naming a profile that does not exist must fail loudly rather than fall
/// back to running without credentials.
#[tokio::test]
async fn a_missing_profile_is_refused_at_attach() {
    init_log();
    let tmp = TempDir::new().expect("tmp");
    let pond = tmp.path().join("pond");
    let ctx = ctx_for(&pond, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");

    let err = apply_yaml(
        &ctx,
        tmp.path(),
        "version: v1\nkind: backup\nmetadata:\n  path: /sys/remotes/origin\nspec:\n  url: s3://bucket\n  storage: /sys/storage/nope\n",
    )
    .await
    .expect_err("a missing profile must be refused");
    assert!(
        format!("{err:#}").contains("/sys/storage/nope"),
        "unexpected error: {err:#}"
    );

    let mut ship = ctx.open_pond().await.expect("open");
    assert!(
        list_remote_names(&mut ship).await.expect("list").is_empty(),
        "a refused attachment must leave no trace"
    );
}

/// Binding reads the *stored* config, where the `${env:...}` references
/// survive, not the node's rendered content, which is expanded and redacted.
/// This is what lets one replicated profile document authenticate each replica
/// as itself (Decision A6).
#[tokio::test]
async fn a_profile_binds_from_the_stored_reference_not_the_rendered_view() {
    init_log();
    let tmp = TempDir::new().expect("tmp");
    let pond = tmp.path().join("pond");
    let ctx = ctx_for(&pond, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");

    apply_yaml(
        &ctx,
        tmp.path(),
        "version: v1\nkind: mknod\nmetadata:\n  path: /sys/storage/minio\nspec:\n  factory: storage-minio\n  config:\n    endpoint: http://watershop:9000\n    access_key_id: ${env:PATH}\n    secret_access_key: ${env:PATH}\n",
    )
    .await
    .expect("apply profile");

    let mut ship = ctx.open_pond().await.expect("open");
    let pond = ship.as_pond_mut().expect("pond steward");
    let profile = steward::ResolvedStorage::open(pond, "/sys/storage/minio")
        .await
        .expect("read profile back");
    assert_eq!(profile.kind(), "storage-minio");
    assert!(profile.describe().contains("watershop:9000"));
    assert!(profile.serves_scheme("s3://bucket"));

    // The credential reference survived replication as text, and resolves
    // here rather than having been baked in at apply time (Decision A6).
    let opts = profile.to_storage_options().expect("resolve");
    let path = std::env::var("PATH").expect("PATH is set");
    assert_eq!(
        opts.get("secret_access_key").map(String::as_str),
        Some(path.as_str())
    );
    assert_eq!(opts.get("allow_http").map(String::as_str), Some("true"));
}

/// Phase A1: `pond status` still renders on a pond containing a profile node.
///
/// Deliberately narrow, and worth stating why.  A `file://` backup cannot name
/// a MinIO profile -- the schemes must agree -- and an `s3://` one cannot be
/// attached here, because attach performs a real push.  So the *profile-backed*
/// status line cannot be reached end-to-end without a reachable MinIO; its
/// rendering is covered instead by `format_storage_line`'s unit tests, and this
/// asserts only that a profile node in the pond does not disturb `status`.
///
/// The gap closes when the staging MinIO is restored -- the same prerequisite
/// as measuring real push bandwidth.
#[tokio::test]
async fn status_renders_on_a_pond_holding_a_profile() {
    init_log();
    let tmp = TempDir::new().expect("tmp");
    let pond = tmp.path().join("pond");
    let ctx = ctx_for(&pond, vec!["pond", "init"]);
    init_command(&ctx, "test-host").await.expect("init");

    // A `file://` backup cannot name a MinIO profile (schemes must agree), so
    // the profile is applied on its own and the status path is exercised by
    // attaching it to a remote whose URL it does serve.
    let remote = tmp.path().join("remote");
    std::fs::create_dir_all(&remote).expect("mkdir");
    apply_yaml(
        &ctx,
        tmp.path(),
        &format!(
            "version: v1\nkind: mknod\nmetadata:\n  path: /sys/storage/minio\nspec:\n  factory: storage-minio\n  config:\n    endpoint: http://watershop:9000\n    access_key_id: ${{env:PATH}}\n    secret_access_key: ${{env:PATH}}\n---\nversion: v1\nkind: backup\nmetadata:\n  path: /sys/remotes/origin\nspec:\n  url: file://{}\n",
            remote.display()
        ),
    )
    .await
    .expect("apply");

    status_command(&ctx).await.expect("status with a profile");
}
