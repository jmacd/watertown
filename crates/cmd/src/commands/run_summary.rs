// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Process-global side channel for `pond run` invocation metadata.
//!
//! `run_command` and its helpers populate the factory name once it's
//! resolved (from the oplog for pond paths or from the URL scheme for
//! host paths).  `main.rs` reads it at process exit so it can emit a
//! single structured "Run summary" line that names which factory was
//! executed and how long it took.  The dispatch layer doesn't have
//! access to this otherwise: `run_command` returns `Result<()>` to
//! match every other subcommand, and the factory name lives several
//! function-call layers deep.
//!
//! A process-global is acceptable because the pond binary is a
//! one-shot CLI: each invocation runs exactly one subcommand.

use std::sync::Mutex;
use std::time::Instant;

static FACTORY: Mutex<Option<String>> = Mutex::new(None);

/// Record the factory name that this `pond run` invocation resolved.
///
/// Idempotent: only the first call wins, so callers don't need to
/// worry about overwriting on retry paths.
pub fn record_factory(name: &str) {
    let mut slot = FACTORY.lock().expect("run_summary FACTORY mutex poisoned");
    if slot.is_none() {
        *slot = Some(name.to_string());
    }
}

/// Take and return the recorded factory name, leaving `None` behind.
#[must_use]
pub fn take_factory() -> Option<String> {
    FACTORY
        .lock()
        .expect("run_summary FACTORY mutex poisoned")
        .take()
}

/// Non-overlapping wall times for one pond run. Unreached phases are omitted.
pub(super) struct RunPhases {
    phase: &'static str,
    since: Instant,
    fields: String,
}

impl RunPhases {
    pub(super) fn new() -> Self {
        Self {
            phase: "open",
            since: Instant::now(),
            fields: String::new(),
        }
    }

    pub(super) fn enter(&mut self, next: &'static str) {
        use std::fmt::Write;
        let now = Instant::now();
        if log::log_enabled!(target: "pond::run_phases", log::Level::Debug) {
            write!(
                self.fields,
                " {}_ms={}",
                self.phase,
                (now - self.since).as_millis()
            )
            .expect("writing to a String cannot fail");
        }
        self.phase = next;
        self.since = now;
    }

    pub(super) fn report(mut self, path: &str, ok: bool) {
        self.enter("done");
        log::debug!(target: "pond::run_phases", "Run phases path={path} outcome={}{}",
            if ok { "ok" } else { "err" }, self.fields);
    }
}
