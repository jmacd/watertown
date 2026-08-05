// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `pond status` (D6.2) -- operator-facing aggregate of the pond's
//! identity, local commit state, recovery health, and per-remote sync
//! watermarks.
//!
//! This is a fast, OFFLINE command: it reads only the local control
//! table and `/sys/remotes/*` attachments.  It never opens a remote or
//! touches the network, so it is safe to run frequently and on a pond
//! whose remotes are unreachable.  Push "lag" is computed purely from
//! whose remotes are unreachable.  Push/pull state is reported as the
//! per-ref tip commit hash last pushed/pulled (`last_pushed_tip:<url>` /
//! `last_pulled_tip:<url>`); to cross-check against what a remote actually
//! recorded, use `pond verify`.
#![allow(clippy::print_stdout)]

use crate::commands::remote::{list_remote_names, load_remote_attachment};
use crate::common::ShipContext;
use anyhow::{Result, anyhow};
use provider::factory::rate_limit::LimitUnit;
use std::time::Duration;
use steward::{LimiterState, REMOTE_MODE_PREFIX, REMOTE_MOUNT_PATH_PREFIX, RemoteMode};

/// Render the operator status report for the pond at `ship_context`.
pub async fn status_command(ship_context: &ShipContext) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;

    let metadata = ship.control_table().get_pond_metadata().clone();
    let last_write_seq = ship
        .control_table()
        .get_last_write_sequence()
        .await
        .map_err(|e| anyhow!("read last write sequence: {}", e))?;
    let incomplete = ship
        .control_table()
        .find_incomplete_transactions()
        .await
        .map_err(|e| anyhow!("scan incomplete transactions: {}", e))?;

    println!("Pond Status");
    println!("===========");
    println!();
    println!("Identity");
    println!("  Pond ID:    {}", metadata.pond_id);
    println!(
        "  Created:    {} by {}",
        format_timestamp(metadata.birth_timestamp),
        metadata.birth_username,
    );
    println!(
        "  Birthplace: {}",
        if metadata.birthplace.is_empty() {
            "(unspecified)"
        } else {
            &metadata.birthplace
        }
    );
    if let Ok(path) = ship_context.resolve_pond_path() {
        println!("  Location:   {}", path.display());
    }
    println!();

    println!("Local state");
    println!("  Last write seq:  {}", last_write_seq);
    if incomplete.is_empty() {
        println!("  Recovery:        OK (no incomplete transactions)");
    } else {
        println!(
            "  Recovery:        [WARN] {} incomplete transaction(s) -- run `pond recover`",
            incomplete.len()
        );
        for (txn_meta, _data_version) in &incomplete {
            println!(
                "                     seq={} txn_id={}",
                txn_meta.txn_seq, txn_meta.user.txn_id
            );
        }
    }
    println!();

    let names = list_remote_names(&mut ship).await?;
    println!("Remotes ({})", names.len());
    if names.is_empty() {
        println!("  (none attached)");
        return Ok(());
    }

    for name in names {
        let attachment = match load_remote_attachment(&mut ship, &name).await {
            Ok(a) => a,
            Err(e) => {
                println!("  {}  [unreadable: {}]", name, e);
                continue;
            }
        };

        let mode_str = ship
            .control_table()
            .raw_config_get(&format!("{REMOTE_MODE_PREFIX}{name}"))
            .await
            .unwrap_or_default()
            .unwrap_or_else(|| "push".to_string());
        let mode = RemoteMode::parse(&mode_str).unwrap_or(RemoteMode::Push);

        let mount = ship
            .control_table()
            .raw_config_get(&format!("{REMOTE_MOUNT_PATH_PREFIX}{name}"))
            .await
            .unwrap_or_default()
            .filter(|s| !s.is_empty());

        let last_pushed_tip = read_tip(&ship, &format!("last_pushed_tip:{}", attachment.url)).await;
        let last_pulled_tip = read_tip(&ship, &format!("last_pulled_tip:{}", attachment.url)).await;

        println!("  {}  [{}]", name, mode_str);
        println!("    url:          {}", attachment.url);
        match &mount {
            Some(p) => println!("    mount:        {}", p),
            None => println!("    mount:        / (mirror)"),
        }

        report_storage(&mut ship, &attachment).await;

        if mode.pushes() {
            match last_pushed_tip {
                Some(tip) => println!("    last pushed:  {}", tip),
                None => println!("    last pushed:  - (never pushed)"),
            }
        }

        if mode.pulls() {
            match last_pulled_tip {
                Some(tip) => println!("    last pulled:  {}", tip),
                None => println!("    last pulled:  - (never pulled)"),
            }
        }

        // Decision L9: a throttle must never be silent.  A rate-limited pond
        // looks exactly like a healthy one whose backup has quietly stopped
        // advancing, so the budget has to be on the status page next to the
        // tip it is holding back.
        report_limits(&mut ship, &attachment).await;
    }

    Ok(())
}

/// Print the storage profile backing `attachment`, or nothing if it carries
/// its connection details inline.
///
/// Worth a line for the same reason the limits are (Decision L9): the profile
/// is now the only place the endpoint and credentials are written, so "which
/// storage is this remote actually talking to" is otherwise a question that
/// requires reading a second node to answer.
async fn report_storage(ship: &mut steward::Steward, attachment: &steward::RemoteAttachment) {
    let Some(path) = attachment.storage.as_deref() else {
        return;
    };

    let Some(pond) = ship.as_pond_mut() else {
        println!(
            "    storage:      {}  [unavailable: not a pond steward]",
            path
        );
        return;
    };

    // `describe` is what makes this safe to print: it is tested to name no
    // credential, resolved or otherwise.
    let detail = match steward::ResolvedStorage::open(pond, path).await {
        Ok(profile) => Ok(profile.describe()),
        // Report rather than fail.  A broken profile means this remote cannot
        // push at all, which is exactly when `pond status` has to keep working
        // and say why.
        Err(e) => Err(e.to_string()),
    };
    println!("{}", format_storage_line(path, &detail));
}

/// `storage:      /sys/storage/minio  (minio, http://watershop:9000)`
///
/// Pure so the rendering can be tested without a pond holding a real profile,
/// which today would mean a reachable MinIO.
fn format_storage_line(path: &str, detail: &Result<String, String>) -> String {
    match detail {
        Ok(described) => format!("    storage:      {}  ({})", path, described),
        Err(reason) => format!("    storage:      {}  [{}]", path, reason),
    }
}

/// Print one line per limiter governing `attachment`, or nothing if it is
/// ungoverned.
async fn report_limits(ship: &mut steward::Steward, attachment: &steward::RemoteAttachment) {
    let limits = match attachment.resolved_limits() {
        Ok(l) if l.is_empty() => return,
        Ok(l) => l,
        Err(e) => {
            println!("    limits:       [invalid: {}]", e);
            return;
        }
    };

    println!("    limits:");
    let Some(pond) = ship.as_pond_mut() else {
        println!("      (unavailable: not a pond steward)");
        return;
    };

    for (unit, path) in limits {
        match steward::Limiter::open(pond, &path, unit).await {
            Ok(l) => println!("      {}", format_limiter_state(&l.state())),
            // Report rather than fail: `pond status` must still work on a pond
            // whose limiter config is broken -- that is precisely when an
            // operator is most likely to be running it.
            Err(e) => println!("      {} [{}]: {}", unit, path, e),
        }
    }
}

/// `bytes [/sys/limits/backup-bytes]: 4.0 MiB / 10.0 MiB (40%) per 1d`
fn format_limiter_state(s: &LimiterState) -> String {
    let render = |v: u64| match s.unit {
        LimitUnit::Bytes => format_bytes(v),
        LimitUnit::Ops => v.to_string(),
    };
    let pct = if s.limit == 0 {
        100
    } else {
        // Saturating percentage; a limit spent exactly to the last unit reads
        // 100%, not 99%.
        ((u128::from(s.used) * 100) / u128::from(s.limit)).min(100) as u64
    };

    let mut line = format!(
        "{} [{}]: {} / {} ({}%) per {}",
        s.unit,
        s.path,
        render(s.used),
        render(s.limit),
        pct,
        format_duration(s.window),
    );
    if s.used >= s.limit {
        line.push_str(&format!(
            "  [EXHAUSTED -- retry in {}]",
            format_duration(s.reset_in)
        ));
    }
    line
}

/// Binary units, matching the `unit:` grammar a limiter is configured with, so
/// the report reads back in the same terms the policy was written in.
fn format_bytes(v: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = v as f64;
    let mut idx = 0;
    while value >= 1024.0 && idx + 1 < UNITS.len() {
        value /= 1024.0;
        idx += 1;
    }
    if idx == 0 {
        format!("{v} B")
    } else {
        format!("{value:.1} {}", UNITS[idx])
    }
}

fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs == 0 {
        return "0s".to_string();
    }
    if secs.is_multiple_of(86_400) {
        format!("{}d", secs / 86_400)
    } else if secs.is_multiple_of(3_600) {
        format!("{}h", secs / 3_600)
    } else if secs.is_multiple_of(60) {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

/// Read a per-ref tip commit hash setting.  Returns `None` if the key is unset
/// or empty (the ref has never been pushed/pulled).
async fn read_tip(ship: &steward::Steward, key: &str) -> Option<String> {
    ship.control_table()
        .raw_config_get(key)
        .await
        .ok()
        .flatten()
        .filter(|v| !v.is_empty())
}

/// Format a microsecond timestamp as a human-readable UTC string.
fn format_timestamp(micros: i64) -> String {
    chrono::DateTime::from_timestamp_micros(micros)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| format!("<invalid timestamp: {}>", micros))
}

#[cfg(test)]
mod tests {

    /// The profile line names where the pond talks to, and -- because it is
    /// built from `describe` -- never what it authenticates with.
    #[test]
    fn the_storage_line_names_the_profile_and_no_credential() {
        let doc = b"endpoint: http://watershop:9000\naccess_key_id: ${env:S3_KEY}\nsecret_access_key: ${env:S3_SECRET}\n";
        let profile =
            steward::ResolvedStorage::from_bytes("/sys/storage/minio", doc).expect("parse");
        let line = format_storage_line("/sys/storage/minio", &Ok(profile.describe()));

        assert!(line.contains("/sys/storage/minio"), "{line}");
        assert!(line.contains("http://watershop:9000"), "{line}");
        assert!(!line.contains("S3_SECRET"), "{line}");
        assert!(!line.contains("secret_access_key"), "{line}");
    }

    /// A profile that cannot be read means this remote cannot push at all,
    /// which is exactly when `pond status` has to keep working and say why.
    #[test]
    fn an_unreadable_profile_is_reported_not_hidden() {
        let line = format_storage_line(
            "/sys/storage/minio",
            &Err("storage profile `/sys/storage/minio` not found".to_string()),
        );
        assert!(line.contains("not found"), "{line}");
    }
    use super::*;

    fn state(unit: LimitUnit, used: u64, limit: u64, window: Duration) -> LimiterState {
        LimiterState {
            path: "/sys/limits/backup".to_string(),
            unit,
            used,
            limit,
            burst: limit,
            window,
            reset_in: Duration::from_secs(3600),
        }
    }

    #[test]
    fn bytes_are_reported_in_the_units_the_policy_was_written_in() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.0 KiB");
        assert_eq!(format_bytes(10 * 1024 * 1024), "10.0 MiB");
        assert_eq!(format_bytes(1536 * 1024 * 1024), "1.5 GiB");
    }

    #[test]
    fn windows_read_back_as_written() {
        assert_eq!(format_duration(Duration::from_secs(86_400)), "1d");
        assert_eq!(format_duration(Duration::from_secs(3_600)), "1h");
        assert_eq!(format_duration(Duration::from_secs(60)), "1m");
        assert_eq!(format_duration(Duration::from_secs(90)), "90s");
    }

    #[test]
    fn a_healthy_limiter_reports_headroom() {
        let line = format_limiter_state(&state(
            LimitUnit::Bytes,
            4 * 1024 * 1024,
            10 * 1024 * 1024,
            Duration::from_secs(86_400),
        ));
        assert!(line.contains("4.0 MiB / 10.0 MiB"), "{line}");
        assert!(line.contains("(40%)"), "{line}");
        assert!(line.contains("per 1d"), "{line}");
        assert!(!line.contains("EXHAUSTED"), "{line}");
    }

    /// The whole point of L9: a saturated budget must be impossible to miss,
    /// because the pond otherwise looks healthy while its backup goes stale.
    #[test]
    fn an_exhausted_limiter_says_so_and_says_when() {
        let line = format_limiter_state(&state(
            LimitUnit::Bytes,
            10 * 1024 * 1024,
            10 * 1024 * 1024,
            Duration::from_secs(86_400),
        ));
        assert!(line.contains("(100%)"), "{line}");
        assert!(line.contains("EXHAUSTED"), "{line}");
        assert!(line.contains("retry in 1h"), "{line}");
    }

    /// Over-spending (a burst settling, or a policy tightened under a live
    /// window) must not render as more than 100%.
    #[test]
    fn overspend_is_clamped_and_still_flagged() {
        let line = format_limiter_state(&state(LimitUnit::Ops, 150, 100, Duration::from_secs(60)));
        assert!(line.contains("150 / 100"), "{line}");
        assert!(line.contains("(100%)"), "{line}");
        assert!(line.contains("EXHAUSTED"), "{line}");
    }

    #[test]
    fn ops_are_reported_as_plain_counts() {
        let line = format_limiter_state(&state(LimitUnit::Ops, 5, 100, Duration::from_secs(3600)));
        assert!(line.contains("ops ["), "{line}");
        assert!(line.contains("5 / 100"), "{line}");
    }
}
