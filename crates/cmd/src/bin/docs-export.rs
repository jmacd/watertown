// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::io::Write;

fn main() -> anyhow::Result<()> {
    let metadata = cmd::documentation::inventory();
    serde_json::to_writer_pretty(std::io::stdout().lock(), &metadata)?;
    writeln!(std::io::stdout().lock())?;
    Ok(())
}
