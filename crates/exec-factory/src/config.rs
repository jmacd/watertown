// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Configuration schema for the `exec` factory.
//!
//! See `docs/exec-factory-design.md` for the full design. This module is
//! deliberately simpler than that document's sketch in one respect: `inputs`
//! and `outputs` are exact pond paths or directory prefixes (trailing `/`),
//! not full glob patterns. Reinventing a glob engine was judged not worth it
//! for v1 -- tinyfs's own wildcard matcher is tied to its directory-walking
//! visitor and isn't exposed as a standalone path matcher. Prefix matching on
//! output *directories* covers the real use cases (e.g. `reports/`) without
//! that complexity; exact globs can be added later if a concrete need shows
//! up.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tinyfs::Result as TinyFSResult;
use tinyfs::ResultExt;

/// Default wall-clock timeout for ordinary (non-interactive) runs. See
/// [`ExecConfig::effective_timeout`] for how `interactive` changes this.
const DEFAULT_TIMEOUT_SECONDS: u64 = 60;

/// Configuration for one `exec` factory mount.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecConfig {
    /// Absolute path to the program to execute. Never searched on `$PATH`:
    /// the config is pond-versioned, so the exact binary invoked should be
    /// an explicit, auditable value.
    pub program: String,

    /// Literal argument vector passed to the program. Never shell-interpreted;
    /// if shell semantics are wanted, spell them out explicitly, e.g.
    /// `program: /bin/bash, args: ["-c", "..."]`.
    #[serde(default)]
    pub args: Vec<String>,

    /// Exact pond paths staged read-only into the sandbox before exec.
    pub inputs: Vec<String>,

    /// Pond paths (exact files) or directory prefixes (trailing `/`) that
    /// may be created or modified by the program. Anything written outside
    /// these paths in the staging directory is discarded, not committed.
    /// Deletions of a path that matched `outputs` before exec are always
    /// treated as an error (see design doc, "Deletions, failure, and commit
    /// semantics").
    #[serde(default)]
    pub outputs: Vec<String>,

    /// Exact pond paths (no directory prefixes) treated as **append-only**
    /// `tinyfs::EntryType::FilePhysicalSeries` files: the program sees the
    /// path's full prior content staged read-write (series versions are
    /// concatenated on read), and on clean exit only the *new* suffix bytes
    /// -- not the whole file -- are committed as the series' next version.
    /// A natural fit for append-only formats like Ledger journals. If the
    /// program leaves behind anything that isn't a byte-for-byte extension
    /// of what it started with (edited, truncated, or deleted), the whole
    /// run is rejected and the pond is left unchanged, same as any other
    /// output error.
    #[serde(default)]
    pub series_outputs: Vec<String>,

    /// Explicit environment variables set inside the sandbox. Empty by
    /// default -- nothing is inherited from the `pond` process environment.
    #[serde(default)]
    pub env: HashMap<String, String>,

    /// Allow network access inside the sandbox. `false` unless a concrete
    /// use case needs it; bookkeeping tools are local-file-only.
    #[serde(default)]
    pub network: bool,

    /// Run the program without `bwrap`'s `--new-session` isolation, so a
    /// real interactive program (a REPL, `hledger add`, etc.) keeps normal
    /// terminal job control (Ctrl-C, Ctrl-Z) instead of losing it to a
    /// detached session. This trades away `--new-session`'s defense against
    /// the sandboxed program injecting fake keystrokes back at the
    /// controlling terminal (`TIOCSTI`) -- acceptable only because running
    /// a program interactively is already an explicit, attended choice by
    /// the operator. See docs/exec-factory-design.md's interactive sessions
    /// section.
    #[serde(default)]
    pub interactive: bool,

    /// Kill the program if it hasn't exited within this many seconds.
    /// `None` resolves via [`ExecConfig::effective_timeout`]: 60s for
    /// ordinary runs, no timeout at all for `interactive: true` runs.
    /// `Some(0)` always means "no timeout", for either kind of run.
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
}

impl ExecConfig {
    /// The wall-clock timeout to enforce, or `None` for "run indefinitely".
    /// See `timeout_seconds`'s doc comment for the resolution rules.
    #[must_use]
    pub fn effective_timeout(&self) -> Option<Duration> {
        match self.timeout_seconds {
            Some(0) => None,
            Some(secs) => Some(Duration::from_secs(secs)),
            None if self.interactive => None,
            None => Some(Duration::from_secs(DEFAULT_TIMEOUT_SECONDS)),
        }
    }
}

/// Validate exec configuration from YAML bytes, per the
/// `validate_config: fn(&[u8]) -> TinyFSResult<Value>` factory contract.
pub fn validate_exec_config(config_bytes: &[u8]) -> TinyFSResult<serde_json::Value> {
    let config: ExecConfig =
        serde_yaml::from_slice(config_bytes).map_other_context("Invalid exec config")?;

    if !config.program.starts_with('/') {
        return Err(tinyfs::Error::Other(format!(
            "exec config: 'program' must be an absolute path, got '{}'",
            config.program
        )));
    }
    if config.inputs.is_empty() && config.outputs.is_empty() && config.series_outputs.is_empty() {
        return Err(tinyfs::Error::Other(
            "exec config: at least one of 'inputs', 'outputs', or 'series_outputs' must be set"
                .to_string(),
        ));
    }
    for spec in &config.series_outputs {
        if spec.ends_with('/') {
            return Err(tinyfs::Error::Other(format!(
                "exec config: 'series_outputs' entries must be exact file paths, not a \
                 directory prefix, got '{spec}'"
            )));
        }
        if config.outputs.contains(spec) {
            return Err(tinyfs::Error::Other(format!(
                "exec config: '{spec}' is listed in both 'outputs' and 'series_outputs'; \
                 each output path must pick exactly one commit semantics"
            )));
        }
    }

    serde_json::to_value(config).map_other_context("Failed to convert exec config")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_relative_program_path() {
        let yaml = b"program: cat\ninputs: [/a]\noutputs: []\n";
        let err = validate_exec_config(yaml).expect_err("relative program path should be rejected");
        assert!(format!("{err}").contains("absolute path"));
    }

    #[test]
    fn rejects_empty_inputs_and_outputs() {
        let yaml = b"program: /bin/cat\ninputs: []\noutputs: []\n";
        let err = validate_exec_config(yaml).expect_err("empty inputs+outputs should be rejected");
        assert!(format!("{err}").contains("inputs"));
    }

    #[test]
    fn accepts_minimal_valid_config() {
        let yaml = br#"
program: /bin/cat
args: ["in.txt"]
inputs: ["/data/in.txt"]
outputs: ["/data/out.txt"]
"#;
        let value = validate_exec_config(yaml).expect("valid config should parse");
        let config: ExecConfig = serde_json::from_value(value).expect("round-trip");
        assert_eq!(config.program, "/bin/cat");
        assert_eq!(
            config.effective_timeout(),
            Some(Duration::from_secs(60))
        );
        assert!(!config.network);
    }

    #[test]
    fn accepts_series_outputs_only() {
        let yaml = b"program: /bin/sh\ninputs: []\noutputs: []\nseries_outputs: [\"/accounting/journal.ledger\"]\n";
        let value = validate_exec_config(yaml).expect("series_outputs alone should be sufficient");
        let config: ExecConfig = serde_json::from_value(value).expect("round-trip");
        assert_eq!(config.series_outputs, vec!["/accounting/journal.ledger"]);
    }

    #[test]
    fn rejects_series_output_directory_prefix() {
        let yaml = b"program: /bin/sh\ninputs: []\noutputs: []\nseries_outputs: [\"/accounting/\"]\n";
        let err = validate_exec_config(yaml).expect_err("directory-style series output should be rejected");
        assert!(format!("{err}").contains("exact file paths"));
    }

    #[test]
    fn rejects_path_in_both_outputs_and_series_outputs() {
        let yaml = b"program: /bin/sh\ninputs: []\noutputs: [\"/a.txt\"]\nseries_outputs: [\"/a.txt\"]\n";
        let err = validate_exec_config(yaml).expect_err("overlapping output path should be rejected");
        assert!(format!("{err}").contains("both"));
    }

    #[test]
    fn interactive_runs_default_to_no_timeout() {
        let yaml = b"program: /bin/sh\ninputs: []\noutputs: []\nseries_outputs: [\"/a.ledger\"]\ninteractive: true\n";
        let value = validate_exec_config(yaml).expect("valid config should parse");
        let config: ExecConfig = serde_json::from_value(value).expect("round-trip");
        assert_eq!(config.effective_timeout(), None);
    }

    #[test]
    fn explicit_zero_timeout_always_disables_timeout() {
        let yaml = b"program: /bin/sh\ninputs: []\noutputs: [\"/a\"]\ntimeout_seconds: 0\n";
        let value = validate_exec_config(yaml).expect("valid config should parse");
        let config: ExecConfig = serde_json::from_value(value).expect("round-trip");
        assert!(!config.interactive);
        assert_eq!(config.effective_timeout(), None);
    }
}
