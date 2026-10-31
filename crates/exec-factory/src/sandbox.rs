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
use std::path::{Path, PathBuf};
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
    #[error("host input requires an explicit pond -d hostmount root")]
    MissingHostRoot,
    #[error("invalid host input '{0}': {1}")]
    InvalidHostInput(String, String),
}

/// Build the `bwrap` argument vector for running `cfg.program` with
/// `cfg.args` inside `staging`, per the design doc's §4 invocation.
fn build_bwrap_args(
    cfg: &ExecConfig,
    staging: &Path,
    host_inputs: &[(PathBuf, String)],
) -> Vec<String> {
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
    args.push("--tmpfs".to_string());
    args.push("/tmp".to_string());

    let staging_str = staging.to_string_lossy().to_string();
    args.push("--bind".to_string());
    args.push(staging_str.clone());
    args.push(staging_str.clone());
    args.push("--chdir".to_string());
    args.push(staging_str);

    for (source, target) in host_inputs {
        args.push("--ro-bind".to_string());
        args.push(source.to_string_lossy().to_string());
        args.push(target.clone());
    }

    args.push("--unshare-pid".to_string());
    args.push("--die-with-parent".to_string());
    if !cfg.interactive {
        // `--new-session` stops the sandboxed program from injecting fake
        // keystrokes back at the controlling terminal (TIOCSTI), but also
        // detaches it from normal terminal job control (Ctrl-C/Ctrl-Z no
        // longer reach it). Interactive sessions need real job control more
        // than they need that hardening -- see `ExecConfig::interactive`.
        args.push("--new-session".to_string());
    }
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
/// `cfg.effective_timeout()` (if any). Returns the child's exit status on
/// clean exit (any exit code -- the caller decides what "success" means);
/// returns `SandboxError` if `bwrap` itself couldn't run or the timeout
/// elapsed. stdin/stdout/stderr are inherited from the `pond` process by
/// default (standard `tokio::process::Command` behavior), so an
/// `interactive: true` program talking to a real terminal just works.
pub async fn run_sandboxed(
    cfg: &ExecConfig,
    staging: &Path,
    host_root: Option<&Path>,
) -> Result<ExitStatus, SandboxError> {
    let host_inputs = resolve_host_inputs(cfg, host_root)?;
    let args = build_bwrap_args(cfg, staging, &host_inputs);

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

    match cfg.effective_timeout() {
        Some(timeout) => match tokio::time::timeout(timeout, child.wait()).await {
            Ok(result) => result.map_err(SandboxError::Wait),
            Err(_elapsed) => {
                // kill_on_drop will send SIGKILL when `child` is dropped; also
                // try an explicit kill so the caller's error is informative
                // rather than racing a background drop.
                let _ = child.start_kill();
                Err(SandboxError::Timeout(timeout))
            }
        },
        None => child.wait().await.map_err(SandboxError::Wait),
    }
}

fn resolve_host_inputs(
    cfg: &ExecConfig,
    host_root: Option<&Path>,
) -> Result<Vec<(PathBuf, String)>, SandboxError> {
    if cfg.host_inputs.is_empty() {
        return Ok(Vec::new());
    }
    let root = host_root.ok_or(SandboxError::MissingHostRoot)?;
    let root = std::fs::canonicalize(root).map_err(|error| {
        SandboxError::InvalidHostInput(root.display().to_string(), error.to_string())
    })?;
    if !root.is_dir() {
        return Err(SandboxError::InvalidHostInput(
            root.display().to_string(),
            "hostmount root is not a directory".to_string(),
        ));
    }

    cfg.host_inputs
        .iter()
        .map(|mount| {
            let relative = mount
                .source
                .strip_prefix('/')
                .expect("validated absolute host input");
            let requested = root.join(relative);
            let resolved = std::fs::canonicalize(&requested).map_err(|error| {
                SandboxError::InvalidHostInput(mount.source.clone(), error.to_string())
            })?;
            if !resolved.starts_with(&root) {
                return Err(SandboxError::InvalidHostInput(
                    mount.source.clone(),
                    "path escapes the explicit hostmount root".to_string(),
                ));
            }
            Ok((resolved, mount.target.clone()))
        })
        .collect()
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
            series_outputs: vec![],
            host_inputs: vec![],
            env: Default::default(),
            network: false,
            interactive: false,
            timeout_seconds: Some(5),
        }
    }

    #[test]
    fn bwrap_args_scope_network_and_pid_by_default() {
        let cfg = test_config();
        let args = build_bwrap_args(&cfg, Path::new("/tmp/staging-test"), &[]);
        assert!(args.contains(&"--unshare-net".to_string()));
        assert!(args.contains(&"--unshare-pid".to_string()));
        assert!(args.contains(&"--die-with-parent".to_string()));
        assert!(args.contains(&"--new-session".to_string()));
        assert!(args.iter().any(|a| a == "/bin/cat"));
    }

    #[test]
    fn bwrap_args_allow_network_when_configured() {
        let mut cfg = test_config();
        cfg.network = true;
        let args = build_bwrap_args(&cfg, Path::new("/tmp/staging-test"), &[]);
        assert!(!args.contains(&"--unshare-net".to_string()));
    }

    #[test]
    fn bwrap_args_drop_new_session_when_interactive() {
        let mut cfg = test_config();
        cfg.interactive = true;
        let args = build_bwrap_args(&cfg, Path::new("/tmp/staging-test"), &[]);
        assert!(!args.contains(&"--new-session".to_string()));
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
            args: vec!["-c".to_string(), "cat in.txt > out.txt".to_string()],
            inputs: vec!["/in.txt".to_string()],
            outputs: vec!["/out.txt".to_string()],
            series_outputs: vec![],
            host_inputs: vec![],
            env: Default::default(),
            network: false,
            interactive: false,
            timeout_seconds: Some(10),
        };

        let status = run_sandboxed(&cfg, dir.path(), None)
            .await
            .expect("bwrap should run (install bubblewrap on this Linux host)");
        assert!(status.success());

        let out = std::fs::read_to_string(dir.path().join("out.txt")).expect("read output");
        assert_eq!(out, "hello from sandbox");
    }

    // Verifies the "no timeout" path (`effective_timeout() == None`) actually
    // lets a program run past what would otherwise be a very short timeout,
    // rather than accidentally always wrapping in `tokio::time::timeout`.
    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn interactive_run_has_no_enforced_timeout() {
        let dir = tempfile::tempdir().expect("tempdir");

        let cfg = ExecConfig {
            program: "/bin/sh".to_string(),
            args: vec!["-c".to_string(), "sleep 2".to_string()],
            inputs: vec![],
            outputs: vec![],
            series_outputs: vec![],
            host_inputs: vec![],
            env: Default::default(),
            network: false,
            interactive: true,
            timeout_seconds: None,
        };
        assert_eq!(cfg.effective_timeout(), None);

        let status = run_sandboxed(&cfg, dir.path(), None)
            .await
            .expect("bwrap should run (install bubblewrap on this Linux host)");
        assert!(status.success());
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn host_input_is_visible_and_read_only() {
        let host_root = tempfile::tempdir().expect("host root");
        let staging = tempfile::tempdir().expect("staging");
        std::fs::create_dir(host_root.path().join("statements")).expect("statement dir");
        std::fs::write(
            host_root.path().join("statements/bank.pdf"),
            b"statement bytes",
        )
        .expect("statement");
        let cfg = ExecConfig {
            program: "/bin/sh".to_string(),
            args: vec![
                "-c".to_string(),
                "cat /inbox/bank.pdf > copied.pdf; ! echo changed > /inbox/bank.pdf".to_string(),
            ],
            inputs: vec![],
            outputs: vec!["/copied.pdf".to_string()],
            series_outputs: vec![],
            host_inputs: vec![crate::config::HostInput {
                source: "/statements".to_string(),
                target: "/inbox".to_string(),
            }],
            env: Default::default(),
            network: false,
            interactive: false,
            timeout_seconds: Some(10),
        };

        let status = run_sandboxed(&cfg, staging.path(), Some(host_root.path()))
            .await
            .expect("host input sandbox run");
        assert!(status.success());
        assert_eq!(
            std::fs::read(staging.path().join("copied.pdf")).expect("copied output"),
            b"statement bytes"
        );
        assert_eq!(
            std::fs::read(host_root.path().join("statements/bank.pdf")).expect("host statement"),
            b"statement bytes"
        );
    }

    #[test]
    fn host_input_requires_explicit_root_and_stays_within_it() {
        let root = tempfile::tempdir().expect("host root");
        std::fs::create_dir(root.path().join("statements")).expect("statement dir");
        let mut cfg = test_config();
        cfg.host_inputs.push(crate::config::HostInput {
            source: "/statements".to_string(),
            target: "/inbox".to_string(),
        });

        assert!(matches!(
            resolve_host_inputs(&cfg, None),
            Err(SandboxError::MissingHostRoot)
        ));
        let mounts = resolve_host_inputs(&cfg, Some(root.path())).expect("resolve mount");
        assert_eq!(
            mounts[0].0,
            std::fs::canonicalize(root.path().join("statements")).expect("canonical statement dir")
        );
        assert_eq!(mounts[0].1, "/inbox");
    }
}
