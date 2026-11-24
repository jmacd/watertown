// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::{fs, process::Command};

#[test]
fn measure_ingest_reports_only_the_phases_it_ran() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("metrics.jsonl");
    let config = dir.path().join("ingest.yaml");
    let node = "/system/etc/measure/test";
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
        &config,
        format!(
            "archived_pattern: '{}.*'\nactive_pattern: '{}'\npond_path: /measure\n",
            log.display(),
            log.display()
        ),
    )
    .unwrap();
    fs::write(&log, b"{\"value\":1}\n").unwrap();
    for args in [
        vec!["init", "--birthplace", "run-phases"],
        vec!["mkdir", "-p", "/system/etc/measure"],
        vec!["mkdir", "/measure"],
        vec![
            "mknod",
            "logfile-ingest",
            node,
            "--config-path",
            config.to_str().unwrap(),
        ],
    ] {
        let _ = pond(&args, "warn", true);
    }

    let run = |success| {
        let stderr = pond(
            &["run", node, "push"],
            "info,pond::run_phases=debug",
            success,
        );
        let mut records = stderr
            .lines()
            .filter_map(|line| line.split_once("Run phases "));
        let (_, record) = records.next().expect("missing run timing");
        assert!(records.next().is_none(), "duplicate run timings");
        let mut fields = record.split_whitespace();
        assert_eq!(fields.next(), Some("path=/system/etc/measure/test"));
        assert_eq!(
            fields.next(),
            Some(if success { "outcome=ok" } else { "outcome=err" })
        );
        let names: Vec<_> = fields
            .map(|field| {
                let (name, ms) = field.split_once('=').unwrap();
                assert!(ms.parse::<u128>().is_ok(), "{field}");
                name
            })
            .collect();
        let mut expected = vec!["open_ms", "begin_ms", "setup_ms", "factory_ms"];
        expected.extend(if success {
            &["local_commit_ms", "post_commit_ms"][..]
        } else {
            &["abort_ms"][..]
        });
        assert_eq!(names, expected);
    };

    run(true); // Content-changing commit.
    run(true); // No-op commit can still retry publication.
    fs::write(&log, b"{\"changed\":true}\n{\"value\":2}\n").unwrap();
    run(false); // Rewritten prefix: the factory aborts before committing.
    fs::write(&log, b"{\"value\":1}\n{\"value\":2}\n").unwrap();
    run(true); // Retry the legitimate append.
    let ordinary = pond(&["run", node, "push"], "info", true);
    assert!(ordinary.contains("Run summary") && !ordinary.contains("Run phases "));
}
