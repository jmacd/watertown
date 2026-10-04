// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! The `exec` dynamic factory: stage declared pond paths into a real
//! directory, run an external program inside a sandbox, and commit the
//! program's declared output files back into the pond as one transaction.
//!
//! See `docs/exec-factory-design.md` for the full design; `config.rs` for
//! the config schema (and its one deliberate simplification vs. the design
//! doc: exact paths / directory prefixes, not glob patterns); `stage.rs` for
//! the pure host-side staging/diff helpers this module drives; `sandbox.rs`
//! for the `bwrap` invocation.

use crate::config::{ExecConfig, validate_exec_config};
use crate::sandbox::run_sandboxed;
use crate::stage::{diff_outputs, parse_output_spec, pond_path_to_relative, snapshot_outputs};
use provider::{ExecutionContext, FactoryContext, register_executable_factory};
use serde_json::Value;
use tinyfs::Result as TinyFSResult;
use tinyfs::ResultExt;

fn validate_config(config: &[u8]) -> TinyFSResult<Value> {
    validate_exec_config(config)
}

async fn initialize(_config: Value, _context: FactoryContext) -> Result<(), tinyfs::Error> {
    // Nothing to set up ahead of time: inputs are staged and outputs are
    // committed entirely within `execute`.
    Ok(())
}

/// Copy every `inputs` path from the pond into `staging`, read-only.
async fn stage_inputs(
    root: &tinyfs::WD,
    cfg: &ExecConfig,
    staging: &std::path::Path,
) -> Result<(), tinyfs::Error> {
    for pond_path in &cfg.inputs {
        let relative = pond_path_to_relative(pond_path);
        let host_path = staging.join(&relative);
        if let Some(parent) = host_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_other_context("exec: create staging directory")?;
        }
        let bytes = root
            .read_file_path_to_vec(pond_path)
            .await
            .map_err(|e| tinyfs::Error::Other(format!("exec: stage input '{pond_path}': {e}")))?;
        tokio::fs::write(&host_path, &bytes)
            .await
            .map_other_context("exec: write staged input")?;

        // Read-only so the sandboxed program can observe, but a bug (or a
        // deliberately hostile ledger plugin) can't quietly mutate an input
        // in place and have it misread as an "unchanged" output.
        let mut perms = tokio::fs::metadata(&host_path)
            .await
            .map_other_context("exec: stat staged input")?
            .permissions();
        perms.set_readonly(true);
        tokio::fs::set_permissions(&host_path, perms)
            .await
            .map_other_context("exec: chmod staged input read-only")?;
    }
    Ok(())
}

/// Commit the output diff back into the pond. All writes for one `execute`
/// call happen inside the same pond transaction that `register_executable_factory!`
/// already runs `execute` under, so either every output lands or (on any
/// error) none of them do.
async fn commit_outputs(
    root: &tinyfs::WD,
    diff: &crate::stage::OutputDiff,
) -> Result<(), tinyfs::Error> {
    for (relative, content) in &diff.changed {
        let pond_path = crate::stage::relative_to_pond_path(relative);
        if let Some(parent) = relative.parent().filter(|p| !p.as_os_str().is_empty()) {
            let parent_pond_path = crate::stage::relative_to_pond_path(parent);
            let _ = root.create_dir_all(&parent_pond_path).await.map_err(|e| {
                tinyfs::Error::Other(format!(
                    "exec: create output directory '{parent_pond_path}': {e}"
                ))
            })?;
        }
        root.write_file_path_from_slice(&pond_path, content)
            .await
            .map_err(|e| {
                tinyfs::Error::Other(format!("exec: commit output '{pond_path}': {e}"))
            })?;
    }
    Ok(())
}

pub async fn execute(
    config: Value,
    context: FactoryContext,
    _ctx: ExecutionContext,
) -> Result<(), tinyfs::Error> {
    let cfg: ExecConfig = serde_json::from_value(config).map_other_context("exec: invalid config")?;

    let staging = tempfile::tempdir()
        .map_other_context("exec: create staging directory")?;
    let root = context.root().await?;

    stage_inputs(&root, &cfg, staging.path()).await?;

    let output_specs: Vec<_> = cfg.outputs.iter().map(|s| parse_output_spec(s)).collect();
    let before = snapshot_outputs(staging.path(), &output_specs)
        .map_other_context("exec: snapshot outputs before run")?;

    let status = run_sandboxed(&cfg, staging.path())
        .await
        .map_err(|e| tinyfs::Error::Other(format!("exec: sandbox failed: {e}")))?;
    if !status.success() {
        return Err(tinyfs::Error::Other(format!(
            "exec: program '{}' exited with {status}; pond is unchanged",
            cfg.program
        )));
    }

    let diff = diff_outputs(staging.path(), &output_specs, &before)
        .map_other_context("exec: diff outputs after run")?;
    if !diff.deleted.is_empty() {
        return Err(tinyfs::Error::Other(format!(
            "exec: program '{}' deleted declared output path(s) {:?}; deletions are not \
             supported in v1, pond is unchanged",
            cfg.program, diff.deleted
        )));
    }

    commit_outputs(&root, &diff).await?;
    log::info!(
        "exec: '{}' committed {} changed output(s)",
        cfg.program,
        diff.changed.len()
    );
    Ok(())
}

register_executable_factory!(
    name: "exec",
    description: "Run an external program against declared pond paths inside a sandbox, committing its declared outputs as one transaction",
    validate: validate_config,
    initialize: initialize,
    execute: execute
);
