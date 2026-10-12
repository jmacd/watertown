// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Build script: captures git SHA for build identification in sitegen footer.

use std::{path::PathBuf, process::Command};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let git_sha = match git(&["rev-parse", "--short", "HEAD"]) {
        Ok(sha) => sha,
        Err(error) => {
            println!("cargo:warning=Cannot determine sitegen Git SHA: {error}");
            println!("cargo:rustc-env=WATERTOWN_GIT_SHA=unknown");
            return;
        }
    };

    println!("cargo:rustc-env=WATERTOWN_GIT_SHA={}", git_sha);

    // Submodules and worktrees have a .git pointer file, not a directory.
    watch(git_path("HEAD"));
    let head_ref = git(&["rev-parse", "--symbolic-full-name", "HEAD"])
        .expect("cannot resolve sitegen Git HEAD");
    if head_ref != "HEAD" {
        watch(git_path(&head_ref));
        let packed_refs = git_path("packed-refs");
        if packed_refs
            .try_exists()
            .expect("cannot inspect Git packed refs")
        {
            watch(packed_refs);
        }
    }
}

fn git(args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .args(args)
        .output()
        .map_err(|error| format!("cannot run git: {error}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_string());
    }
    String::from_utf8(output.stdout)
        .map(|value| value.trim().to_string())
        .map_err(|error| format!("invalid git output: {error}"))
}

fn git_path(name: &str) -> PathBuf {
    PathBuf::from(
        git(&["rev-parse", "--git-path", name]).expect("cannot resolve Git metadata path"),
    )
}

fn watch(mut path: PathBuf) {
    // A packed branch has no loose ref file. Watch its nearest existing parent
    // so creating a loose ref is detected without registering a missing input.
    while !path.try_exists().expect("cannot inspect Git metadata path") {
        assert!(path.pop(), "Git metadata path has no existing parent");
    }
    println!("cargo:rerun-if-changed={}", path.display());
}
