// SPDX-License-Identifier: Apache-2.0

//! Pond-local immutable object cache backing native-v2 commits and publication.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use sync_store::content::ObjectHash;

use crate::{StewardError, get_data_path};

const LOCAL_OBJECT_ROOT: &str = "_content/v2/objects";
const LOCAL_STATE_ROOT: &str = "_content/v2/state/manifest-root";

#[derive(Debug, Clone)]
pub(crate) struct LocalContentStore {
    root: PathBuf,
    state_root: PathBuf,
}

impl LocalContentStore {
    pub(crate) fn new(pond_path: &Path) -> Self {
        Self {
            root: get_data_path(pond_path).join(LOCAL_OBJECT_ROOT),
            state_root: get_data_path(pond_path).join(LOCAL_STATE_ROOT),
        }
    }

    pub(crate) fn read_manifest_root_cursor(
        &self,
        pond_id: &str,
    ) -> Result<Option<Vec<u8>>, StewardError> {
        let path = self.manifest_root_cursor_path(pond_id);
        match std::fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(StewardError::Content(format!(
                "read local manifest-root cursor {}: {error}",
                path.display()
            ))),
        }
    }

    pub(crate) fn write_manifest_root_cursor(
        &self,
        pond_id: &str,
        bytes: &[u8],
    ) -> Result<(), StewardError> {
        std::fs::create_dir_all(&self.state_root).map_err(|error| {
            StewardError::Content(format!(
                "create local manifest-root cursor directory {}: {error}",
                self.state_root.display()
            ))
        })?;
        let final_path = self.manifest_root_cursor_path(pond_id);
        let temporary = self
            .state_root
            .join(format!(".pending-{}", uuid::Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|error| {
                StewardError::Content(format!(
                    "create local manifest-root cursor staging file {}: {error}",
                    temporary.display()
                ))
            })?;
        file.write_all(bytes).map_err(|error| {
            StewardError::Content(format!(
                "write local manifest-root cursor staging file {}: {error}",
                temporary.display()
            ))
        })?;
        file.sync_all().map_err(|error| {
            StewardError::Content(format!(
                "sync local manifest-root cursor staging file {}: {error}",
                temporary.display()
            ))
        })?;
        drop(file);
        std::fs::rename(&temporary, &final_path).map_err(|error| {
            let _ = std::fs::remove_file(&temporary);
            StewardError::Content(format!(
                "publish local manifest-root cursor {}: {error}",
                final_path.display()
            ))
        })?;
        File::open(&self.state_root)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| {
                StewardError::Content(format!(
                    "sync local manifest-root cursor directory {}: {error}",
                    self.state_root.display()
                ))
            })
    }

    pub(crate) fn read(&self, hash: ObjectHash) -> Result<Vec<u8>, StewardError> {
        let path = self.object_path(hash);
        let bytes = std::fs::read(&path).map_err(|error| {
            StewardError::Content(format!(
                "read local immutable object {}: {error}",
                path.display()
            ))
        })?;
        let actual = ObjectHash::of_bytes(&bytes);
        if actual != hash {
            return Err(StewardError::Content(format!(
                "local immutable object {} hashes to {}, expected {}",
                path.display(),
                actual,
                hash
            )));
        }
        Ok(bytes)
    }

    pub(crate) fn read_optional(&self, hash: ObjectHash) -> Result<Option<Vec<u8>>, StewardError> {
        let path = self.object_path(hash);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(StewardError::Content(format!(
                    "read local immutable object {}: {error}",
                    path.display()
                )));
            }
        };
        let actual = ObjectHash::of_bytes(&bytes);
        if actual != hash {
            return Err(StewardError::Content(format!(
                "local immutable object {} hashes to {}, expected {}",
                path.display(),
                actual,
                hash
            )));
        }
        Ok(Some(bytes))
    }

    pub(crate) fn read_string_error(&self, hash: ObjectHash) -> Result<Vec<u8>, String> {
        self.read(hash).map_err(|error| error.to_string())
    }

    pub(crate) fn put_batch(
        &self,
        objects: &BTreeMap<ObjectHash, Vec<u8>>,
    ) -> Result<(), StewardError> {
        for (hash, bytes) in objects {
            let _ = self.put(*hash, bytes)?;
        }
        Ok(())
    }

    pub(crate) fn put(&self, hash: ObjectHash, bytes: &[u8]) -> Result<bool, StewardError> {
        let actual = ObjectHash::of_bytes(bytes);
        if actual != hash {
            return Err(StewardError::Content(format!(
                "refusing local immutable object {} whose bytes hash to {}",
                hash, actual
            )));
        }
        std::fs::create_dir_all(&self.root).map_err(|error| {
            StewardError::Content(format!(
                "create local immutable object directory {}: {error}",
                self.root.display()
            ))
        })?;
        let final_path = self.object_path(hash);
        if final_path.exists() {
            self.verify_existing(hash, bytes)?;
            return Ok(false);
        }

        let temporary = self.root.join(format!(".pending-{}", uuid::Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|error| {
                StewardError::Content(format!(
                    "create local immutable staging object {}: {error}",
                    temporary.display()
                ))
            })?;
        file.write_all(bytes).map_err(|error| {
            StewardError::Content(format!(
                "write local immutable staging object {}: {error}",
                temporary.display()
            ))
        })?;
        file.sync_all().map_err(|error| {
            StewardError::Content(format!(
                "sync local immutable staging object {}: {error}",
                temporary.display()
            ))
        })?;
        drop(file);

        match std::fs::hard_link(&temporary, &final_path) {
            Ok(()) => {
                std::fs::remove_file(&temporary).map_err(|error| {
                    StewardError::Content(format!(
                        "remove local immutable staging object {}: {error}",
                        temporary.display()
                    ))
                })?;
                File::open(&self.root)
                    .and_then(|directory| directory.sync_all())
                    .map_err(|error| {
                        StewardError::Content(format!(
                            "sync local immutable object directory {}: {error}",
                            self.root.display()
                        ))
                    })?;
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let _ = std::fs::remove_file(&temporary);
                self.verify_existing(hash, bytes)?;
                Ok(false)
            }
            Err(error) => {
                let _ = std::fs::remove_file(&temporary);
                Err(StewardError::Content(format!(
                    "publish local immutable object {}: {error}",
                    final_path.display()
                )))
            }
        }
    }

    fn verify_existing(&self, hash: ObjectHash, expected: &[u8]) -> Result<(), StewardError> {
        let existing = self.read(hash)?;
        if existing != expected {
            return Err(StewardError::Content(format!(
                "local immutable object {} has conflicting bytes",
                hash
            )));
        }
        Ok(())
    }

    fn object_path(&self, hash: ObjectHash) -> PathBuf {
        self.root.join(format!("blake3={}", hash.to_hex()))
    }

    fn manifest_root_cursor_path(&self, pond_id: &str) -> PathBuf {
        self.state_root.join(format!("pond={pond_id}"))
    }
}
