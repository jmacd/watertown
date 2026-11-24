// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::{fs, process::Command};

#[test]
fn maintenance_phases_distinguish_work_noops_and_refused_prune() {
    let dir = tempfile::tempdir().unwrap();
    let logs = dir.path().join("input.log");
    let recipe = dir.path().join("ingest.yaml");
    let trace = "warn,pond::maintain_phases=debug";
    let pond = |args: &[&str], filter: &str, success: bool| {
        let output = Command::new(env!("CARGO_BIN_EXE_pond"))
            .env("POND", dir.path().join("pond"))
            .env("RUST_LOG", filter)
            .args(args)
            .output()
            .unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert_eq!(output.status.success(), success, "{args:?}: {stderr}");
        stderr
    };
    fs::write(
        &recipe,
        format!(
            "archived_pattern: '{}.*'\nactive_pattern: '{}'\npond_path: /logs\n",
            logs.display(),
            logs.display()
        ),
    )
    .unwrap();
    for args in [
        vec!["init", "--birthplace", "maintain-phases"],
        vec!["mkdir", "/logs"],
        vec![
            "mknod",
            "logfile-ingest",
            "/ingest",
            "--config-path",
            recipe.to_str().unwrap(),
        ],
    ] {
        let _ = pond(&args, "warn", true);
    }
    for content in ["one\n", "one\ntwo\n"] {
        fs::write(&logs, content).unwrap();
        let _ = pond(&["run", "/ingest", "push"], "warn", true);
    }

    let args = [
        "maintain",
        "--compact",
        "--prune",
        "--allow-no-remote",
        "--collapse-versions",
        "1",
    ];
    let first = pond(&args, trace, true);
    let warm = pond(&args, trace, true);
    let phase = |text: &str, scope, name| {
        let tag = format!("Maintain phase scope={scope} phase={name} elapsed_ms=");
        let mut matches = text.lines().filter_map(|line| line.split_once(&tag));
        let (_, fields) = matches
            .next()
            .unwrap_or_else(|| panic!("missing {tag} in {text}"));
        assert!(matches.next().is_none(), "duplicate {tag}");
        let (ms, work) = fields.split_once(" outcome=").unwrap();
        assert!(ms.parse::<u128>().is_ok());
        work.to_owned()
    };
    assert!(phase(&first, "pack", "discover").contains("selected=1"));
    assert!(phase(&first, "pack", "spool").contains("delta_scans=1 inline_bytes=8"));
    assert!(phase(&first, "pack", "repack").contains("series=1 "));
    assert!(phase(&warm, "pack", "spool").contains("delta_scans=0 inline_bytes=0"));
    assert!(phase(&warm, "pack", "repack").contains("series=0 "));
    for (scope, name) in [
        ("control", "prune"),
        ("data", "reclaim_index"),
        ("pack", "gc"),
        ("data", "root_before"),
        ("data", "compact"),
        ("control", "compact"),
        ("data", "vacuum"),
        ("control", "vacuum"),
        ("data", "checkpoint"),
        ("control", "checkpoint"),
    ] {
        assert!(phase(&first, scope, name).starts_with("ok "));
    }
    let refused = pond(&["maintain", "--prune"], trace, false);
    assert_eq!(phase(&refused, "control", "prune").trim(), "incomplete");
    assert!(!refused.contains("Maintain phase scope=data"));
    let preview = pond(
        &["maintain", "--dry-run", "--collapse-versions", "1"],
        trace,
        true,
    );
    assert!(!preview.contains("Maintain phase scope=pack"));
    assert!(!pond(&["maintain"], "info", true).contains("Maintain phase "));
}
