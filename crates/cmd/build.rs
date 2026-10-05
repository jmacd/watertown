use std::env;

fn main() {
    // Bake the package version into the binary so `pond --version` matches the
    // deb version, container tag, and GitHub release. CI sets WATERTOWN_VERSION
    // to the computed 0.<build>.<pr> string; local dev builds fall back to the
    // crate version from Cargo.toml so the version always resolves.
    println!("cargo:rerun-if-env-changed=WATERTOWN_VERSION");
    let version = env::var("WATERTOWN_VERSION")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| env::var("CARGO_PKG_VERSION").expect("CARGO_PKG_VERSION is always set"));
    println!("cargo:rustc-env=WATERTOWN_VERSION={version}");
}
