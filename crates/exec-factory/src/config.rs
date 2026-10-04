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
use tinyfs::Result as TinyFSResult;
use tinyfs::ResultExt;

fn default_timeout_seconds() -> u64 {
    60
}

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
    pub outputs: Vec<String>,

    /// Explicit environment variables set inside the sandbox. Empty by
    /// default -- nothing is inherited from the `pond` process environment.
    #[serde(default)]
    pub env: HashMap<String, String>,

    /// Allow network access inside the sandbox. `false` unless a concrete
    /// use case needs it; bookkeeping tools are local-file-only.
    #[serde(default)]
    pub network: bool,

    /// Kill the program if it hasn't exited within this many seconds.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
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
    if config.inputs.is_empty() && config.outputs.is_empty() {
        return Err(tinyfs::Error::Other(
            "exec config: at least one of 'inputs' or 'outputs' must be set".to_string(),
        ));
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
        assert_eq!(config.timeout_seconds, 60);
        assert!(!config.network);
    }
}
