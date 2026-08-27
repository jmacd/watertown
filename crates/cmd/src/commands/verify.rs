// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `pond verify [<name>]` -- compare this pond's CURRENT content tip against
//! one or more remotes' published tip via
//! [`steward::verify_content_against_remote`].
//!
//! The content-addressed remote (Decision D6) holds a single object closure
//! and one tip ref per pond -- there is no `(pond_id, seq)` frontier.  Verify
//! therefore reports the commit-graph relationship between the local tip and
//! the remote's published tip: up to date, the remote lagging behind unpushed
//! local commits, an empty remote, or a real divergence.
//!
//! When invoked without `name`, every attached remote is verified.  Each
//! remote is processed independently; one failure does not halt the others.
#![allow(clippy::print_stdout)]

use crate::commands::remote::{list_remote_names, load_remote_attachment};
use crate::common::ShipContext;
use anyhow::{Result, anyhow};
use steward::{ContentVerifyReport, ContentVerifyState, verify_content_against_remote};
use sync_store::ContentRemote;

/// Verify against `name`, or against every attached remote when `name` is
/// `None`.
pub async fn verify_command(
    ship_context: &ShipContext,
    name: Option<String>,
    exact: bool,
) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;

    let targets: Vec<String> = if let Some(n) = name {
        vec![n]
    } else {
        list_remote_names(&mut ship).await?
    };

    if targets.is_empty() {
        println!("No remotes attached; nothing to verify.");
        return Ok(());
    }

    let mut had_error = false;
    let mut had_mismatch = false;
    for name in targets {
        match verify_one(&mut ship, &name).await {
            Ok(report) => {
                print_report(&name, &report);
                if !report.ok || (exact && report.state != ContentVerifyState::UpToDate) {
                    had_mismatch = true;
                }
            }
            Err(e) => {
                log::error!("[ERR] verify {}: {}", name, e);
                had_error = true;
            }
        }
    }

    if had_error {
        Err(anyhow!("one or more verifications failed"))
    } else if had_mismatch {
        let requirement = if exact {
            "verification did not find exact matching local and remote tips"
        } else {
            "verification completed with mismatches"
        };
        Err(anyhow!("{requirement}; see report(s) above"))
    } else {
        Ok(())
    }
}

async fn verify_one(ship: &mut steward::Steward, name: &str) -> Result<ContentVerifyReport> {
    let attachment = load_remote_attachment(ship, name).await?;

    let storage_options = {
        let pond = ship
            .as_pond_mut()
            .ok_or_else(|| anyhow!("verify requires a pond steward (not a host steward)"))?;
        steward::storage_profile::prepare_storage(pond, &attachment).await?
    };
    let remote = ContentRemote::open_at_url(&attachment.url, storage_options)
        .await
        .map_err(|e| anyhow!("open remote `{}` ({}): {}", name, attachment.url, e))?;

    let ship_ref = ship
        .as_pond()
        .ok_or_else(|| anyhow!("verify requires a pond steward (not a host steward)"))?;

    let report = verify_content_against_remote(ship_ref, &remote, "main")
        .await
        .map_err(|e| anyhow!("verify against `{}` ({}): {}", name, attachment.url, e))?;

    Ok(report)
}

fn print_report(name: &str, report: &ContentVerifyReport) {
    let seq = report
        .remote_seq
        .map(|s| s.to_string())
        .unwrap_or_else(|| "-".to_string());
    let header = match &report.state {
        ContentVerifyState::BothEmpty => {
            format!("[OK] verify {}: no local commits and remote is empty", name)
        }
        ContentVerifyState::RemoteEmpty => format!(
            "[OK] verify {}: remote has no published tip yet (nothing pushed)",
            name
        ),
        ContentVerifyState::UpToDate => format!(
            "[OK] verify {}: live data matches remote tip at seq={}",
            name, seq
        ),
        ContentVerifyState::RemoteBehind { local_unpushed } => format!(
            "[OK] verify {}: remote tip at seq={} is behind local by {} commit(s); push to catch up",
            name, seq, local_unpushed
        ),
        ContentVerifyState::Diverged => format!(
            "[MISMATCH] verify {}: remote tip at seq={} is not in this pond's history (diverged)",
            name, seq
        ),
    };
    println!("{}", header);

    if let Some(local) = report.local_tip {
        println!("    local tip:  {}", local.to_hex());
    } else {
        println!("    local tip:  - (no local commits)");
    }
    match report.remote_tip {
        Some(rt) => println!("    remote tip: {}", rt.to_hex()),
        None => println!("    remote tip: - (no published ref)"),
    }
}
