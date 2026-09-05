// SPDX-License-Identifier: Apache-2.0

//! Static, self-extracting recovery recipe for the current native backup format.

use sha2::{Digest, Sha256};

use crate::ObjectHash;

const RECIPE_DOMAIN: &[u8] = b"pondcapsule.recipe.1\n";
const README: &str = include_str!("../recovery/watertown.commit.v1/README.md");
const FORMAT: &str = include_str!("../recovery/watertown.commit.v1/FORMAT.md");
const REQUIREMENTS: &str = include_str!("../recovery/watertown.commit.v1/requirements.lock");
const CAPSULE_README: &str = include_str!("../recovery/watertown.commit.v1/CAPSULE-README.md");
const CAPSULE_FORMAT: &str = include_str!("../recovery/watertown.commit.v1/CAPSULE-FORMAT.md");
const CAPSULE_REQUIREMENTS: &str =
    include_str!("../recovery/watertown.commit.v1/capsule-requirements.lock");
const CAPSULE_TOOL: &str = include_str!("../recovery/watertown.commit.v1/capsule.py");
const PARQUET_SCHEMA: &str = include_str!("../recovery/watertown.commit.v1/parquet_schema.py");
const RECOVER: &str = include_str!("../recovery/watertown.commit.v1/recover.sh");
const CAPSULE_TEST: &str = include_str!("../recovery/watertown.commit.v1/capsule_test.py");
const DOWNLOAD_AZCOPY: &str = include_str!("../recovery/watertown.commit.v1/download-azcopy.sh");
const DOWNLOAD_MC: &str = include_str!("../recovery/watertown.commit.v1/download-mc.sh");
const EXTRACTOR: &str = include_str!("../recovery/watertown.commit.v1/extract.py");
const INTEGRATION_TEST: &str = include_str!("../recovery/watertown.commit.v1/integration_test.py");

const FILES: &[(&str, &str)] = &[
    ("README.md", README),
    ("FORMAT.md", FORMAT),
    ("requirements.lock", REQUIREMENTS),
    ("CAPSULE-README.md", CAPSULE_README),
    ("CAPSULE-FORMAT.md", CAPSULE_FORMAT),
    ("capsule-requirements.lock", CAPSULE_REQUIREMENTS),
    ("capsule.py", CAPSULE_TOOL),
    ("parquet_schema.py", PARQUET_SCHEMA),
    ("recover.sh", RECOVER),
    ("capsule_test.py", CAPSULE_TEST),
    ("download-azcopy.sh", DOWNLOAD_AZCOPY),
    ("download-mc.sh", DOWNLOAD_MC),
    ("extract.py", EXTRACTOR),
    ("integration_test.py", INTEGRATION_TEST),
];

/// Build the reviewable POSIX bootstrap for the `watertown.commit.v1` recovery kit.
#[must_use]
pub fn recovery_recipe_watertown_commit_v1() -> Vec<u8> {
    let mut script = String::from(
        "#!/bin/sh\nset -eu\numask 077\nDEST=${1:-watertown-recovery-kit-v1}\n\
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
        checksums.push_str(&format!(
            "{:x}  {name}\n",
            Sha256::digest(contents.as_bytes())
        ));
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

/// Domain-separated BLAKE3 identity of the exact bootstrap bytes.
#[must_use]
pub fn recovery_recipe_watertown_commit_v1_hash() -> ObjectHash {
    recovery_recipe_hash(&recovery_recipe_watertown_commit_v1())
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
    fn bootstrap_is_deterministic_and_extracts_only_current_files() {
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
                "parquet_schema.py",
                "recover.sh",
                "requirements.lock",
            ]
        );
        assert_ne!(
            recovery_recipe_watertown_commit_v1_hash(),
            ObjectHash::of_bytes(b"")
        );
    }

    #[test]
    fn bootstrap_refuses_existing_destination() {
        let temporary = tempdir().unwrap();
        let script = temporary.path().join("README.sh");
        std::fs::write(&script, recovery_recipe_watertown_commit_v1()).unwrap();
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
}
