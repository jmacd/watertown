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

pub(crate) fn recovery_recipe_hash(script: &[u8]) -> ObjectHash {
    let mut hasher = blake3::Hasher::new();
    let _ = hasher.update(RECIPE_DOMAIN);
    let _ = hasher.update(script);
    ObjectHash::from_bytes(*hasher.finalize().as_bytes())
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use serde_json::Value;
    use tempfile::tempdir;
    use tinyfs::EntryType;

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
    fn native_fixtures_remain_frozen() {
        let fixtures: Value = serde_json::from_str(NATIVE_FIXTURES).unwrap();
        let hash = |start: u8| {
            let bytes: [u8; 32] = std::array::from_fn(|index| start + index as u8);
            ObjectHash::from_bytes(bytes)
        };
        let hex_value = |name: &str| {
            hex::decode(fixtures[name].as_str().unwrap()).expect("fixture must contain hex")
        };

        let mut commit = Vec::new();
        commit.extend_from_slice(b"dp.commit.3\n");
        commit.extend_from_slice(hash(0).as_bytes());
        commit.push(1);
        commit.extend_from_slice(hash(32).as_bytes());
        commit.extend_from_slice(hash(64).as_bytes());
        commit.extend_from_slice(hash(96).as_bytes());
        crate::content::push_len_prefixed(&mut commit, b"pond-x");
        commit.extend_from_slice(&(-7_i64).to_le_bytes());
        commit.extend_from_slice(&1_700_000_000_000_000_i64.to_le_bytes());
        crate::content::push_len_prefixed(&mut commit, b"alice");
        crate::content::push_len_prefixed(&mut commit, b"pond write");
        assert_eq!(commit, hex_value("commit_hex"));

        let entries = vec![
            crate::ManifestEntry::new(
                "node-file",
                "root",
                "data.bin",
                EntryType::FilePhysicalSeries,
                hash(32),
                vec![
                    crate::VersionMeta {
                        timestamp: Some(123),
                        min_event_time: Some(-2),
                        max_event_time: Some(9),
                        extended_attributes: Some(r#"{"a":1}"#.to_string()),
                    },
                    crate::VersionMeta {
                        timestamp: Some(124),
                        ..crate::VersionMeta::default()
                    },
                ],
            ),
            crate::ManifestEntry::bare("root", "", "", EntryType::DirectoryPhysical, hash(0)),
        ];
        assert_eq!(
            crate::content::encode_manifest(&entries).unwrap(),
            hex_value("manifest_hex")
        );
        assert_eq!(
            crate::node_merkle_rebuild_root(&entries).unwrap().to_hex(),
            fixtures["manifest_root_hex"].as_str().unwrap()
        );
        assert_eq!(
            crate::content::encode_tree(&[crate::TreeEntry::new(
                "data.bin",
                EntryType::FilePhysicalSeries,
                hash(32),
                entries[0].versions.clone(),
            )])
            .unwrap(),
            hex_value("tree_hex")
        );
        assert_eq!(
            crate::content::encode_series(&[hash(64), hash(96)]),
            hex_value("series_hex")
        );
        assert_eq!(
            crate::content::encode_recipe("factory-x", b"\0\xffconfig"),
            hex_value("recipe_hex")
        );
    }
}
