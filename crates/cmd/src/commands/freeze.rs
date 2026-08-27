// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Persistent pond-wide write-freeze commands.

#![allow(clippy::print_stdout)]

use anyhow::{Result, anyhow};

use crate::common::ShipContext;

pub async fn freeze_writes_command(ship_context: &ShipContext, reason: String) -> Result<()> {
    let mut steward = ship_context.open_pond().await?;
    let ship = steward
        .as_pond_mut()
        .ok_or_else(|| anyhow!("write freeze requires a pond steward"))?;
    let (freeze, created) = ship
        .freeze_writes(&ship_context.command_metadata(), reason)
        .await
        .map_err(|error| anyhow!("freeze pond writes: {error}"))?;

    if created {
        println!(
            "[OK] Pond writes frozen (tip={}, frozen_at={}, reason={})",
            freeze.source_tip.as_deref().unwrap_or("<none>"),
            freeze.frozen_at.to_rfc3339(),
            freeze.reason
        );
    } else {
        println!(
            "[OK] Pond writes already frozen (tip={}, frozen_at={}, reason={})",
            freeze.source_tip.as_deref().unwrap_or("<none>"),
            freeze.frozen_at.to_rfc3339(),
            freeze.reason
        );
    }
    Ok(())
}

pub async fn freeze_status_command(ship_context: &ShipContext) -> Result<()> {
    let pond_path = ship_context.resolve_pond_path()?;
    match steward::read_pond_write_freeze(&pond_path)? {
        Some(freeze) => println!(
            "Pond writes are FROZEN\n  pond_id: {}\n  source_tip: {}\n  frozen_at: {}\n  frozen_by_pid: {}\n  reason: {}",
            freeze.pond_id,
            freeze.source_tip.as_deref().unwrap_or("<none>"),
            freeze.frozen_at.to_rfc3339(),
            freeze.frozen_by_pid,
            freeze.reason
        ),
        None => println!("Pond writes are enabled"),
    }
    Ok(())
}

pub async fn unfreeze_writes_command(ship_context: &ShipContext) -> Result<()> {
    let pond_path = ship_context.resolve_pond_path()?;
    match steward::unfreeze_pond_writes(&pond_path, &ship_context.command_metadata())
        .map_err(|error| anyhow!("unfreeze pond writes: {error}"))?
    {
        Some(freeze) => println!(
            "[OK] Pond writes unfrozen (previous_tip={}, previous_reason={})",
            freeze.source_tip.as_deref().unwrap_or("<none>"),
            freeze.reason
        ),
        None => println!("[OK] Pond writes were already enabled"),
    }
    Ok(())
}
