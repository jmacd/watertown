// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Linux sandbox invocation via `bubblewrap` (`bwrap`).
//!
//! See `docs/exec-factory-design.md` §4. `bwrap` itself is located via
//! `$PATH` (it is a trusted system sandboxing tool, not the user-supplied
//! program being sandboxed); the program being run inside the sandbox is
//! always an absolute path from `ExecConfig`, never `$PATH`-searched.

use crate::config::ExecConfig;
use std::path::Path;
use std::process::ExitStatus;
use std::time::Duration;

/// Directories bind-mounted read-only into the sandbox so a dynamically
/// linked program (and its loader) can actually run. Deliberately coarse
/// for v1 ("bind wholesale" per the design doc) rather than resolving each
/// program's exact `ldd` closure; only directories that exist on the host
/// are bound.
const READONLY_SYSTEM_DIRS: &[&str] = &["/usr", "/lib", "/lib64", "/bin", "/sbin", "/etc"];

#[derive(Debug, thiserror::Error)]
pub enum SandboxError {
    #[error(
        "bwrap not found on $PATH; the exec factory requires Linux + bubblewrap (see docs/exec-factory-design.md)"
    )]
    BwrapNotFound,
    #[error("failed to spawn sandboxed process: {0}")]
    Spawn(#[source] std::io::Error),
    #[error("sandboxed program timed out after {0:?}")]
    Timeout(Duration),
    #[error("failed to wait for sandboxed process: {0}")]
    Wait(#[source] std::io::Error),
}

/// Build the `bwrap` argument vector for running `cfg.program` with
/// `cfg.args` inside `staging`, per the design doc's §4 invocation.
fn build_bwrap_args(cfg: &ExecConfig, staging: &Path) -> Vec<String> {
    let mut args: Vec<String> = Vec::new();

    for dir in READONLY_SYSTEM_DIRS {
        if Path::new(dir).exists() {
            args.push("--ro-bind".to_string());
            args.push((*dir).to_string());
            args.push((*dir).to_string());
        }
    }

    args.push("--dev".to_string());
    args.push("/dev".to_string());
    args.push("--proc".to_string());
    args.push("/proc".to_string());

    let staging_str = staging.to_string_lossy().to_string();
    args.push("--bind".to_string());
    args.push(staging_str.clone());
    args.push(staging_str.clone());
    args.push("--chdir".to_string());
    args.push(staging_str);

    args.push("--unshare-pid".to_string());
    args.push("--die-with-parent".to_string());
    args.push("--new-session".to_string());
    if !cfg.network {
        args.push("--unshare-net".to_string());
    }

    // bwrap clears the environment by default; set a minimal PATH so the
    // program (or a shell it spawns, e.g. `bash -c 'cat ...'`) can resolve
    // further binaries by name, plus whatever the config explicitly asked
    // for.
    args.push("--setenv".to_string());
    args.push("PATH".to_string());
    args.push("/usr/local/bin:/usr/bin:/bin".to_string());
    for (key, value) in &cfg.env {
        args.push("--setenv".to_string());
        args.push(key.clone());
        args.push(value.clone());
    }

    args.push("--".to_string());
    args.push(cfg.program.clone());
    args.extend(cfg.args.iter().cloned());

    args
}

/// Run `cfg.program` inside a `bwrap` sandbox rooted at `staging`, enforcing
/// `cfg.timeout_seconds`. Returns the child's exit status on clean exit
/// (any exit code -- the caller decides what "success" means); returns
/// `SandboxError` if `bwrap` itself couldn't run or the timeout elapsed.
pub async fn run_sandboxed(cfg: &ExecConfig, staging: &Path) -> Result<ExitStatus, SandboxError> {
    let args = build_bwrap_args(cfg, staging);

    let mut child = tokio::process::Command::new("bwrap")
        .args(&args)
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                SandboxError::BwrapNotFound
            } else {
                SandboxError::Spawn(e)
            }
        })?;

    let timeout = Duration::from_secs(cfg.timeout_seconds);
    match tokio::time::timeout(timeout, child.wait()).await {
        Ok(result) => result.map_err(SandboxError::Wait),
        Err(_elapsed) => {
            // kill_on_drop will send SIGKILL when `child` is dropped; also
            // try an explicit kill so the caller's error is informative
            // rather than racing a background drop.
            let _ = child.start_kill();
            Err(SandboxError::Timeout(timeout))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> ExecConfig {
        ExecConfig {
            program: "/bin/cat".to_string(),
            args: vec!["in.txt".to_string()],
            inputs: vec!["/in.txt".to_string()],
            outputs: vec![],
            env: Default::default(),
            network: false,
            timeout_seconds: 5,
        }
    }

    #[test]
    fn bwrap_args_scope_network_and_pid_by_default() {
        let cfg = test_config();
        let args = build_bwrap_args(&cfg, Path::new("/tmp/staging-test"));
        assert!(args.contains(&"--unshare-net".to_string()));
        assert!(args.contains(&"--unshare-pid".to_string()));
        assert!(args.contains(&"--die-with-parent".to_string()));
        assert!(args.iter().any(|a| a == "/bin/cat"));
    }

    #[test]
    fn bwrap_args_allow_network_when_configured() {
        let mut cfg = test_config();
        cfg.network = true;
        let args = build_bwrap_args(&cfg, Path::new("/tmp/staging-test"));
        assert!(!args.contains(&"--unshare-net".to_string()));
    }

    // Exercises the real `bwrap` binary; only meaningful on Linux (see
    // docs/exec-factory-design.md) and ignored elsewhere so `cargo test`
    // doesn't fail on macOS dev machines without bwrap installed.
    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn runs_a_trivial_program_under_bwrap() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("in.txt"), "hello from sandbox").expect("write input");

        let cfg = ExecConfig {
            program: "/bin/sh".to_string(),
            args: vec![
                "-c".to_string(),
                "cat in.txt > out.txt".to_string(),
            ],
            inputs: vec!["/in.txt".to_string()],
            outputs: vec!["/out.txt".to_string()],
            env: Default::default(),
            network: false,
            timeout_seconds: 10,
        };

        let status = run_sandboxed(&cfg, dir.path())
            .await
            .expect("bwrap should run (install bubblewrap on this Linux host)");
        assert!(status.success());

        let out = std::fs::read_to_string(dir.path().join("out.txt")).expect("read output");
        assert_eq!(out, "hello from sandbox");
    }
}
