// SPDX-License-Identifier: Apache-2.0

//! Static, self-extracting recovery recipes for native backup formats.

use sha2::{Digest, Sha256};

use crate::ObjectHash;

const RECIPE_DOMAIN: &[u8] = b"pondcapsule.recipe.1\n";
const README: &str = include_str!("../recovery/dp.commit.3/README.md");
const FORMAT: &str = include_str!("../recovery/dp.commit.3/FORMAT.md");
const REQUIREMENTS: &str = include_str!("../recovery/dp.commit.3/requirements.lock");
const CAPSULE_README: &str = include_str!("../recovery/dp.commit.3/CAPSULE-README.md");
const CAPSULE_FORMAT: &str = include_str!("../recovery/dp.commit.3/CAPSULE-FORMAT.md");
const CAPSULE_REQUIREMENTS: &str =
    include_str!("../recovery/dp.commit.3/capsule-requirements.lock");
const CAPSULE_TOOL: &str = include_str!("../recovery/dp.commit.3/capsule.py");
const RECOVER: &str = include_str!("../recovery/dp.commit.3/recover.sh");
const CAPSULE_TEST: &str = include_str!("../recovery/dp.commit.3/capsule_test.py");
const DOWNLOAD_AZCOPY: &str = include_str!("../recovery/dp.commit.3/download-azcopy.sh");
const DOWNLOAD_MC: &str = include_str!("../recovery/dp.commit.3/download-mc.sh");
const EXTRACTOR: &str = include_str!("../recovery/dp.commit.3/extract.py");
const INTEGRATION_TEST: &str = include_str!("../recovery/dp.commit.3/integration_test.py");
const NATIVE_FIXTURES: &str = include_str!("../recovery/dp.commit.3/native-fixtures.json");

const V4_README: &str = include_str!("../recovery/watertown.commit.v1/README.md");
const V4_FORMAT: &str = include_str!("../recovery/watertown.commit.v1/FORMAT.md");
const V4_REQUIREMENTS: &str = include_str!("../recovery/watertown.commit.v1/requirements.lock");
const V4_CAPSULE_README: &str = include_str!("../recovery/watertown.commit.v1/CAPSULE-README.md");
const V4_CAPSULE_FORMAT: &str = include_str!("../recovery/watertown.commit.v1/CAPSULE-FORMAT.md");
const V4_CAPSULE_REQUIREMENTS: &str =
    include_str!("../recovery/watertown.commit.v1/capsule-requirements.lock");
const V4_CAPSULE_TOOL: &str = include_str!("../recovery/watertown.commit.v1/capsule.py");
const V4_RECOVER: &str = include_str!("../recovery/watertown.commit.v1/recover.sh");
const V4_CAPSULE_TEST: &str = include_str!("../recovery/watertown.commit.v1/capsule_test.py");
const V4_DOWNLOAD_AZCOPY: &str = include_str!("../recovery/watertown.commit.v1/download-azcopy.sh");
const V4_DOWNLOAD_MC: &str = include_str!("../recovery/watertown.commit.v1/download-mc.sh");
const V4_EXTRACTOR: &str = include_str!("../recovery/watertown.commit.v1/extract.py");
const V4_INTEGRATION_TEST: &str =
    include_str!("../recovery/watertown.commit.v1/integration_test.py");
const V4_NATIVE_FIXTURES: &str =
    include_str!("../recovery/watertown.commit.v1/native-fixtures.json");

const LEGACY_README: &str = include_str!("../recovery/legacy-migration/README.md");
const LEGACY_FORMAT: &str = include_str!("../recovery/legacy-migration/FORMAT.md");
const LEGACY_REQUIREMENTS: &str = include_str!("../recovery/legacy-migration/requirements.lock");
const LEGACY_INTEGRATION_REQUIREMENTS: &str =
    include_str!("../recovery/legacy-migration/integration-requirements.lock");
const LEGACY_CAPSULE_README: &str = include_str!("../recovery/legacy-migration/CAPSULE-README.md");
const LEGACY_CAPSULE_FORMAT: &str = include_str!("../recovery/legacy-migration/CAPSULE-FORMAT.md");
const LEGACY_CAPSULE_REQUIREMENTS: &str =
    include_str!("../recovery/legacy-migration/capsule-requirements.lock");
const LEGACY_CAPSULE_TOOL: &str = include_str!("../recovery/legacy-migration/capsule.py");
const LEGACY_RECOVER: &str = include_str!("../recovery/legacy-migration/recover.sh");
const LEGACY_CAPSULE_TEST: &str = include_str!("../recovery/legacy-migration/capsule_test.py");
const LEGACY_EXTRACTOR: &str = include_str!("../recovery/legacy-migration/extract.py");
const LEGACY_INTEGRATION_TEST: &str =
    include_str!("../recovery/legacy-migration/integration_test.py");
const LEGACY_NATIVE_FIXTURES: &str =
    include_str!("../recovery/legacy-migration/native-fixtures.json");

const LEGACY_V2_README: &str = include_str!("../recovery/legacy-migration-v2/README.md");
const LEGACY_V2_FORMAT: &str = include_str!("../recovery/legacy-migration-v2/FORMAT.md");
const LEGACY_V2_REQUIREMENTS: &str =
    include_str!("../recovery/legacy-migration-v2/requirements.lock");
const LEGACY_V2_INTEGRATION_REQUIREMENTS: &str =
    include_str!("../recovery/legacy-migration-v2/integration-requirements.lock");
const LEGACY_V2_CAPSULE_README: &str =
    include_str!("../recovery/legacy-migration-v2/CAPSULE-README.md");
const LEGACY_V2_CAPSULE_FORMAT: &str =
    include_str!("../recovery/legacy-migration-v2/CAPSULE-FORMAT.md");
const LEGACY_V2_CAPSULE_REQUIREMENTS: &str =
    include_str!("../recovery/legacy-migration-v2/capsule-requirements.lock");
const LEGACY_V2_CAPSULE_TOOL: &str = include_str!("../recovery/legacy-migration-v2/capsule.py");
const LEGACY_V2_RECOVER: &str = include_str!("../recovery/legacy-migration-v2/recover.sh");
const LEGACY_V2_CAPSULE_TEST: &str =
    include_str!("../recovery/legacy-migration-v2/capsule_test.py");
const LEGACY_V2_EXTRACTOR: &str = include_str!("../recovery/legacy-migration-v2/extract.py");
const LEGACY_V2_INTEGRATION_TEST: &str =
    include_str!("../recovery/legacy-migration-v2/integration_test.py");
const LEGACY_V2_NATIVE_FIXTURES: &str =
    include_str!("../recovery/legacy-migration-v2/native-fixtures.json");

const FILES: &[(&str, &str)] = &[
    ("README.md", README),
    ("FORMAT.md", FORMAT),
    ("requirements.lock", REQUIREMENTS),
    ("CAPSULE-README.md", CAPSULE_README),
    ("CAPSULE-FORMAT.md", CAPSULE_FORMAT),
    ("capsule-requirements.lock", CAPSULE_REQUIREMENTS),
    ("capsule.py", CAPSULE_TOOL),
    ("recover.sh", RECOVER),
    ("capsule_test.py", CAPSULE_TEST),
    ("download-azcopy.sh", DOWNLOAD_AZCOPY),
    ("download-mc.sh", DOWNLOAD_MC),
    ("extract.py", EXTRACTOR),
    ("integration_test.py", INTEGRATION_TEST),
    ("native-fixtures.json", NATIVE_FIXTURES),
];

/// Build the reviewable POSIX bootstrap for the `dp.commit.3` recovery kit.
#[must_use]
pub fn recovery_recipe_dp_commit_3() -> Vec<u8> {
    let mut script = String::from(
        "#!/bin/sh\n\
         set -eu\n\
         umask 077\n\
         DEST=${1:-watertown-recovery-dp.commit.3}\n\
         case \"$DEST\" in ''|'/'|'.'|'..'|-*) printf '%s\\n' 'unsafe destination' >&2; exit 2;; esac\n\
         if [ -e \"$DEST\" ]; then printf '%s\\n' \"destination already exists: $DEST\" >&2; exit 2; fi\n\
         mkdir -m 700 \"$DEST\"\n",
    );
    let mut checksums = String::new();
    for (index, (name, contents)) in FILES.iter().enumerate() {
        let delimiter = format!("__WATERTOWN_RECIPE_FILE_{index}__");
        assert!(
            !contents.lines().any(|line| line == delimiter),
            "recipe delimiter occurs in embedded file"
        );
        script.push_str(&format!(
            "cat > \"$DEST/{name}\" <<'{delimiter}'\n{contents}{delimiter}\n"
        ));
        let digest = Sha256::digest(contents.as_bytes());
        checksums.push_str(&format!("{digest:x}  {name}\n"));
    }
    script.push_str(&format!(
        "cat > \"$DEST/SHA256SUMS\" <<'__WATERTOWN_RECIPE_CHECKSUMS__'\n\
         {checksums}__WATERTOWN_RECIPE_CHECKSUMS__\n\
         printf '%s\\n' \"Recovery kit extracted to $DEST\" \
         \"Review $DEST/README.md and every extracted file before continuing.\" \
         \"Verify SHA256SUMS with sha256sum, or with shasum -a 256 on macOS.\"\n"
    ));
    script.into_bytes()
}

/// Domain-separated BLAKE3 identity of the exact `README.sh` bytes.
#[must_use]
pub fn recovery_recipe_dp_commit_3_hash() -> ObjectHash {
    let script = recovery_recipe_dp_commit_3();
    recovery_recipe_hash(&script)
}

/// Build the reviewable POSIX bootstrap for the `watertown.commit.v1` recovery kit.
#[must_use]
pub fn recovery_recipe_watertown_commit_v1() -> Vec<u8> {
    let files = [
        ("README.md", V4_README),
        ("FORMAT.md", V4_FORMAT),
        ("requirements.lock", V4_REQUIREMENTS),
        ("CAPSULE-README.md", V4_CAPSULE_README),
        ("CAPSULE-FORMAT.md", V4_CAPSULE_FORMAT),
        ("capsule-requirements.lock", V4_CAPSULE_REQUIREMENTS),
        ("capsule.py", V4_CAPSULE_TOOL),
        ("recover.sh", V4_RECOVER),
        ("capsule_test.py", V4_CAPSULE_TEST),
        ("download-azcopy.sh", V4_DOWNLOAD_AZCOPY),
        ("download-mc.sh", V4_DOWNLOAD_MC),
        ("extract.py", V4_EXTRACTOR),
        ("integration_test.py", V4_INTEGRATION_TEST),
        ("native-fixtures.json", V4_NATIVE_FIXTURES),
    ];
    let mut script = String::from(
        "#!/bin/sh\nset -eu\numask 077\nDEST=${1:-watertown-recovery-kit-v1}\n\
         case \"$DEST\" in ''|'/'|'.'|'..'|-*) printf '%s\\n' 'unsafe destination' >&2; exit 2;; esac\n\
         if [ -e \"$DEST\" ]; then printf '%s\\n' \"destination already exists: $DEST\" >&2; exit 2; fi\n\
         mkdir -m 700 \"$DEST\"\n",
    );
    let mut checksums = String::new();
    for (index, (name, contents)) in files.iter().enumerate() {
        let delimiter = format!("__WATERTOWN_V4_RECIPE_FILE_{index}__");
        assert!(!contents.lines().any(|line| line == delimiter));
        script.push_str(&format!(
            "cat > \"$DEST/{name}\" <<'{delimiter}'\n{contents}{delimiter}\n"
        ));
        checksums.push_str(&format!(
            "{:x}  {name}\n",
            Sha256::digest(contents.as_bytes())
        ));
    }
    script.push_str(&format!(
        "cat > \"$DEST/SHA256SUMS\" <<'__WATERTOWN_V4_RECIPE_CHECKSUMS__'\n\
         {checksums}__WATERTOWN_V4_RECIPE_CHECKSUMS__\n\
         printf '%s\\n' \"Recovery kit extracted to $DEST\" \
         \"Review $DEST/README.md and every extracted file before continuing.\" \
         \"Verify SHA256SUMS with sha256sum, or with shasum -a 256 on macOS.\"\n"
    ));
    script.into_bytes()
}

/// Domain-separated BLAKE3 identity of the exact `watertown.commit.v1` bootstrap.
#[must_use]
pub fn recovery_recipe_watertown_commit_v1_hash() -> ObjectHash {
    recovery_recipe_hash(&recovery_recipe_watertown_commit_v1())
}

/// Build the reviewable POSIX bootstrap for the opaque legacy-migration kit.
#[must_use]
pub fn recovery_recipe_legacy_migration() -> Vec<u8> {
    let files = [
        ("README.md", LEGACY_README),
        ("FORMAT.md", LEGACY_FORMAT),
        ("requirements.lock", LEGACY_REQUIREMENTS),
        (
            "integration-requirements.lock",
            LEGACY_INTEGRATION_REQUIREMENTS,
        ),
        ("CAPSULE-README.md", LEGACY_CAPSULE_README),
        ("CAPSULE-FORMAT.md", LEGACY_CAPSULE_FORMAT),
        ("capsule-requirements.lock", LEGACY_CAPSULE_REQUIREMENTS),
        ("capsule.py", LEGACY_CAPSULE_TOOL),
        ("recover.sh", LEGACY_RECOVER),
        ("capsule_test.py", LEGACY_CAPSULE_TEST),
        ("extract.py", LEGACY_EXTRACTOR),
        ("integration_test.py", LEGACY_INTEGRATION_TEST),
        ("native-fixtures.json", LEGACY_NATIVE_FIXTURES),
    ];
    let mut script = String::from(
        "#!/bin/sh\nset -eu\numask 077\nDEST=${1:-watertown-legacy-migration-kit}\n\
         case \"$DEST\" in ''|'/'|'.'|'..'|-*) printf '%s\\n' 'unsafe destination' >&2; exit 2;; esac\n\
         if [ -e \"$DEST\" ]; then printf '%s\\n' \"destination already exists: $DEST\" >&2; exit 2; fi\n\
         mkdir -m 700 \"$DEST\"\n",
    );
    let mut checksums = String::new();
    for (index, (name, contents)) in files.iter().enumerate() {
        let delimiter = format!("__WATERTOWN_LEGACY_MIGRATION_FILE_{index}__");
        assert!(!contents.lines().any(|line| line == delimiter));
        script.push_str(&format!(
            "cat > \"$DEST/{name}\" <<'{delimiter}'\n{contents}{delimiter}\n"
        ));
        checksums.push_str(&format!(
            "{:x}  {name}\n",
            Sha256::digest(contents.as_bytes())
        ));
    }
    script.push_str(&format!(
        "cat > \"$DEST/SHA256SUMS\" <<'__WATERTOWN_LEGACY_MIGRATION_CHECKSUMS__'\n\
         {checksums}__WATERTOWN_LEGACY_MIGRATION_CHECKSUMS__\n\
         printf '%s\\n' \"Legacy migration kit extracted to $DEST\" \
         \"Review $DEST/README.md and every extracted file before continuing.\" \
         \"Verify SHA256SUMS with sha256sum, or with shasum -a 256 on macOS.\"\n"
    ));
    script.into_bytes()
}

/// Domain-separated BLAKE3 identity of the legacy-migration bootstrap.
#[must_use]
pub fn recovery_recipe_legacy_migration_hash() -> ObjectHash {
    recovery_recipe_hash(&recovery_recipe_legacy_migration())
}

/// Build the reviewable POSIX bootstrap for the metadata-preserving opaque
/// legacy-migration kit. This is intentionally a separate immutable recipe
/// from the frozen `pondcapsule.legacy.1` extractor.
#[must_use]
pub fn recovery_recipe_legacy_migration_v2() -> Vec<u8> {
    let files = [
        ("README.md", LEGACY_V2_README),
        ("FORMAT.md", LEGACY_V2_FORMAT),
        ("requirements.lock", LEGACY_V2_REQUIREMENTS),
        (
            "integration-requirements.lock",
            LEGACY_V2_INTEGRATION_REQUIREMENTS,
        ),
        ("CAPSULE-README.md", LEGACY_V2_CAPSULE_README),
        ("CAPSULE-FORMAT.md", LEGACY_V2_CAPSULE_FORMAT),
        ("capsule-requirements.lock", LEGACY_V2_CAPSULE_REQUIREMENTS),
        ("capsule.py", LEGACY_V2_CAPSULE_TOOL),
        ("recover.sh", LEGACY_V2_RECOVER),
        ("capsule_test.py", LEGACY_V2_CAPSULE_TEST),
        ("extract.py", LEGACY_V2_EXTRACTOR),
        ("integration_test.py", LEGACY_V2_INTEGRATION_TEST),
        ("native-fixtures.json", LEGACY_V2_NATIVE_FIXTURES),
    ];
    let mut script = String::from(
        "#!/bin/sh\nset -eu\numask 077\nDEST=${1:-watertown-legacy-migration-v2-kit}\n\
         case \"$DEST\" in ''|'/'|'.'|'..'|-*) printf '%s\\n' 'unsafe destination' >&2; exit 2;; esac\n\
         if [ -e \"$DEST\" ]; then printf '%s\\n' \"destination already exists: $DEST\" >&2; exit 2; fi\n\
         mkdir -m 700 \"$DEST\"\n",
    );
    let mut checksums = String::new();
    for (index, (name, contents)) in files.iter().enumerate() {
        let delimiter = format!("__WATERTOWN_LEGACY_MIGRATION_V2_FILE_{index}__");
        assert!(!contents.lines().any(|line| line == delimiter));
        script.push_str(&format!(
            "cat > \"$DEST/{name}\" <<'{delimiter}'\n{contents}{delimiter}\n"
        ));
        checksums.push_str(&format!(
            "{:x}  {name}\n",
            Sha256::digest(contents.as_bytes())
        ));
    }
    script.push_str(&format!(
        "cat > \"$DEST/SHA256SUMS\" <<'__WATERTOWN_LEGACY_MIGRATION_V2_CHECKSUMS__'\n\
         {checksums}__WATERTOWN_LEGACY_MIGRATION_V2_CHECKSUMS__\n\
         printf '%s\\n' \"Legacy migration v2 kit extracted to $DEST\" \
         \"Review $DEST/README.md and every extracted file before continuing.\" \
         \"Verify SHA256SUMS with sha256sum, or with shasum -a 256 on macOS.\"\n"
    ));
    script.into_bytes()
}

/// Domain-separated BLAKE3 identity of the metadata-preserving legacy recipe.
#[must_use]
pub fn recovery_recipe_legacy_migration_v2_hash() -> ObjectHash {
    recovery_recipe_hash(&recovery_recipe_legacy_migration_v2())
}

pub(crate) fn recovery_recipe_hash(script: &[u8]) -> ObjectHash {
    let mut hasher = blake3::Hasher::new();
    let _ = hasher.update(RECIPE_DOMAIN);
    let _ = hasher.update(script);
    ObjectHash::from_bytes(*hasher.finalize().as_bytes())
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn bootstrap_is_deterministic_and_extracts_only_review_files() {
        assert_eq!(recovery_recipe_dp_commit_3(), recovery_recipe_dp_commit_3());
        let temporary = tempdir().unwrap();
        let script = temporary.path().join("README.sh");
        std::fs::write(&script, recovery_recipe_dp_commit_3()).unwrap();
        let destination = temporary.path().join("kit");
        let status = Command::new("sh")
            .arg(&script)
            .arg(&destination)
            .status()
            .unwrap();
        assert!(status.success());
        let mut names = std::fs::read_dir(&destination)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect::<Vec<_>>();
        names.sort();
        assert_eq!(
            names,
            [
                "CAPSULE-FORMAT.md",
                "CAPSULE-README.md",
                "FORMAT.md",
                "README.md",
                "SHA256SUMS",
                "capsule-requirements.lock",
                "capsule.py",
                "capsule_test.py",
                "download-azcopy.sh",
                "download-mc.sh",
                "extract.py",
                "integration_test.py",
                "native-fixtures.json",
                "recover.sh",
                "requirements.lock"
            ]
        );
        assert!(
            Command::new("python3")
                .arg(destination.join("extract.py"))
                .arg("--verify-fixtures")
                .arg(destination.join("native-fixtures.json"))
                .status()
                .unwrap()
                .success()
        );
        for helper in ["download-azcopy.sh", "download-mc.sh"] {
            assert!(
                Command::new("sh")
                    .arg("-n")
                    .arg(destination.join(helper))
                    .status()
                    .unwrap()
                    .success()
            );
        }
        assert!(!recovery_recipe_dp_commit_3().is_empty());
        assert_ne!(
            recovery_recipe_dp_commit_3_hash(),
            ObjectHash::of_bytes(b"")
        );
    }

    #[test]
    fn bootstrap_refuses_existing_destination() {
        let temporary = tempdir().unwrap();
        let script = temporary.path().join("README.sh");
        std::fs::write(&script, recovery_recipe_dp_commit_3()).unwrap();
        let destination = temporary.path().join("existing");
        std::fs::create_dir(&destination).unwrap();
        assert!(
            !Command::new("sh")
                .arg(&script)
                .arg(&destination)
                .status()
                .unwrap()
                .success()
        );
    }

    #[test]
    fn v4_bootstrap_is_deterministic_and_extracts_review_files() {
        assert_eq!(
            recovery_recipe_watertown_commit_v1(),
            recovery_recipe_watertown_commit_v1()
        );
        let temporary = tempdir().unwrap();
        let script = temporary.path().join("README.sh");
        std::fs::write(&script, recovery_recipe_watertown_commit_v1()).unwrap();
        let destination = temporary.path().join("kit");
        assert!(
            Command::new("sh")
                .arg(&script)
                .arg(&destination)
                .status()
                .unwrap()
                .success()
        );
        for name in [
            "README.md",
            "FORMAT.md",
            "extract.py",
            "integration_test.py",
            "recover.sh",
            "SHA256SUMS",
        ] {
            assert!(destination.join(name).is_file(), "{name}");
        }
        assert_ne!(
            recovery_recipe_watertown_commit_v1_hash(),
            recovery_recipe_dp_commit_3_hash()
        );
    }

    #[test]
    fn legacy_migration_bootstrap_is_distinct_and_extracts_review_files() {
        assert_eq!(
            recovery_recipe_legacy_migration(),
            recovery_recipe_legacy_migration()
        );
        let temporary = tempdir().unwrap();
        let script = temporary.path().join("README.sh");
        std::fs::write(&script, recovery_recipe_legacy_migration()).unwrap();
        let destination = temporary.path().join("kit");
        assert!(
            Command::new("sh")
                .arg(&script)
                .arg(&destination)
                .status()
                .unwrap()
                .success()
        );
        for name in [
            "README.md",
            "FORMAT.md",
            "extract.py",
            "capsule.py",
            "integration_test.py",
            "recover.sh",
            "SHA256SUMS",
        ] {
            assert!(destination.join(name).is_file(), "{name}");
        }
        assert_ne!(
            recovery_recipe_legacy_migration_hash(),
            recovery_recipe_dp_commit_3_hash()
        );
        assert_ne!(
            recovery_recipe_legacy_migration_hash(),
            recovery_recipe_watertown_commit_v1_hash()
        );
    }

    #[test]
    fn legacy_migration_v2_bootstrap_is_distinct_and_extracts_review_files() {
        assert_eq!(
            recovery_recipe_legacy_migration_v2(),
            recovery_recipe_legacy_migration_v2()
        );
        let temporary = tempdir().unwrap();
        let script = temporary.path().join("README.sh");
        std::fs::write(&script, recovery_recipe_legacy_migration_v2()).unwrap();
        let destination = temporary.path().join("kit");
        assert!(
            Command::new("sh")
                .arg(&script)
                .arg(&destination)
                .status()
                .unwrap()
                .success()
        );
        assert!(
            std::fs::read_to_string(destination.join("CAPSULE-FORMAT.md"))
                .unwrap()
                .contains("pondcapsule.legacy.2")
        );
        assert_ne!(
            recovery_recipe_legacy_migration_v2_hash(),
            recovery_recipe_legacy_migration_hash()
        );
    }
}
