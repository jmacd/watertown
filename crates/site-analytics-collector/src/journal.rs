// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::{
    catalog::digest,
    config::{Config, validate_path},
};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;

const MAX_RECORD_BYTES: u64 = 4096;
const MAX_INVENTORY_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Clone)]
pub struct Event {
    pub received_at_us: i64,
    pub catalog_id: String,
    pub page_uri: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    schema_version: u32,
    received_at_us: i64,
    site_id: String,
    catalog_id: String,
    page_uri: String,
    segment_id: Uuid,
    record_seq: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Segment {
    pub segment_id: Uuid,
    pub predecessor: Option<Uuid>,
    pub record_count: u64,
    pub bytes: u64,
    pub sha256: String,
}

impl Segment {
    fn new(predecessor: Option<Uuid>) -> Self {
        Self {
            segment_id: Uuid::new_v4(),
            predecessor,
            record_count: 0,
            bytes: 0,
            sha256: digest(b""),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Inventory {
    schema_version: u32,
    site_id: String,
    closed: Vec<Segment>,
    active: Segment,
}

pub struct Journal {
    config: Config,
    inventory: Inventory,
    file: Option<File>,
    hasher: Sha256,
    _writer_lock: File,
    rotation_lock: File,
    pub recovered_partial: bool,
    #[cfg(test)]
    fail_at: Option<&'static str>,
}

impl Journal {
    pub fn open(config: &Config) -> io::Result<Self> {
        if !config.log_dir.try_exists()? {
            std::fs::create_dir(&config.log_dir)?;
            sync_dir(
                config
                    .log_dir
                    .parent()
                    .ok_or_else(|| invalid("log directory has no parent"))?,
            )?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if std::fs::metadata(&config.log_dir)?.permissions().mode() & 0o027 != 0 {
                // Newly provisioned directories must be made private before logs exist.
                if std::fs::read_dir(&config.log_dir)?.next().is_some() {
                    return Err(invalid("log directory must be private (mode 0700 or 0750)"));
                }
                std::fs::set_permissions(&config.log_dir, std::fs::Permissions::from_mode(0o750))?;
            }
        }
        let writer_lock = open_control(&config.log_dir.join(".writer.lock"))?;
        FileExt::try_lock_exclusive(&writer_lock)
            .map_err(|_| invalid("another collector owns this log directory"))?;
        let rotation_lock = open_control(&config.log_dir.join(".rotation.lock"))?;
        FileExt::lock_exclusive(&rotation_lock)?;
        let path = config.log_dir.join("inventory.json");
        let inventory = if path.try_exists()? {
            let mut bytes = Vec::new();
            File::open(path)?
                .take(MAX_INVENTORY_BYTES + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() as u64 > MAX_INVENTORY_BYTES {
                return Err(invalid("segment inventory exceeds supported bound"));
            }
            serde_json::from_slice::<Inventory>(&bytes)
                .map_err(|_| invalid("invalid segment inventory"))?
        } else {
            for entry in std::fs::read_dir(&config.log_dir)? {
                let name = entry?.file_name();
                if name != ".writer.lock" && name != ".rotation.lock" && name != "inventory.tmp" {
                    return Err(invalid(
                        "logs exist without an inventory; restore it before starting",
                    ));
                }
            }
            Inventory {
                schema_version: 1,
                site_id: config.site_id.clone(),
                closed: vec![],
                active: Segment::new(None),
            }
        };
        if inventory.schema_version != 1 || inventory.site_id != config.site_id {
            return Err(invalid("inventory version/site does not match collector"));
        }
        let mut journal = Self {
            config: config.clone(),
            inventory,
            file: None,
            hasher: Sha256::new(),
            _writer_lock: writer_lock,
            rotation_lock,
            recovered_partial: false,
            #[cfg(test)]
            fail_at: None,
        };
        journal.recover()?;
        FileExt::unlock(&journal.rotation_lock)?;
        Ok(journal)
    }

    fn active_path(&self) -> PathBuf {
        self.config.log_dir.join("views.jsonl")
    }

    fn archive_path(&self, id: Uuid) -> PathBuf {
        self.config.log_dir.join(format!("views-{id}.jsonl"))
    }

    fn recover(&mut self) -> io::Result<()> {
        let mut predecessor = None;
        let mut expected = BTreeSet::new();
        for segment in &self.inventory.closed {
            if segment.predecessor != predecessor || !expected.insert(segment.segment_id) {
                return Err(invalid("segment inventory has a gap or duplicate identity"));
            }
            let (scanned, _, _) = scan(
                &self.archive_path(segment.segment_id),
                segment,
                &self.config.site_id,
                false,
            )?;
            if scanned.bytes != segment.bytes || scanned.record_count != segment.record_count {
                return Err(invalid("closed segment differs from its inventory"));
            }
            predecessor = Some(segment.segment_id);
        }
        if self.inventory.active.predecessor != predecessor
            || !expected.insert(self.inventory.active.segment_id)
        {
            return Err(invalid("active segment predecessor is inconsistent"));
        }
        for entry in std::fs::read_dir(&self.config.log_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                return Err(invalid("log directory contains a non-regular file"));
            }
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| invalid("invalid log filename"))?;
            if let Some(id) = name
                .strip_prefix("views-")
                .and_then(|s| s.strip_suffix(".jsonl"))
            {
                let id = Uuid::parse_str(id).map_err(|_| invalid("invalid segment filename"))?;
                if !expected.contains(&id) {
                    return Err(invalid(
                        "unlisted segment exists; reconcile inventory before starting",
                    ));
                }
            }
        }
        let active = self.active_path();
        let pending = self.archive_path(self.inventory.active.segment_id);
        if pending.try_exists()? {
            if active.try_exists()? {
                return Err(invalid("both active and pending-rotation files exist"));
            }
            let (scanned, _, _) = scan(
                &pending,
                &self.inventory.active,
                &self.config.site_id,
                false,
            )?;
            self.inventory.active = scanned;
            self.finish_rotation()?;
        } else if active.try_exists()? {
            let (scanned, hasher, partial) =
                scan(&active, &self.inventory.active, &self.config.site_id, true)?;
            if !partial.is_empty() {
                self.ensure_capacity(partial.len() as u64)?;
                let evidence = self
                    .config
                    .log_dir
                    .join(format!("recovery-{}.bin", Uuid::new_v4()));
                let mut evidence_file = new_file(&evidence)?;
                evidence_file.write_all(&partial)?;
                evidence_file.sync_all()?;
                sync_dir(&self.config.log_dir)?;
                let file = OpenOptions::new().write(true).open(&active)?;
                file.set_len(scanned.bytes)?;
                file.sync_all()?;
                self.recovered_partial = true;
            }
            self.inventory.active = scanned;
            self.hasher = hasher;
            self.file = Some(OpenOptions::new().append(true).open(&active)?);
            // Complete unacknowledged tails must be durable before checkpointing them.
            self.file.as_ref().expect("active log").sync_all()?;
            self.save_inventory()?;
            // Restart always closes a used segment; no sequence is reused.
            if self.inventory.active.bytes > 0 || self.recovered_partial {
                self.rotate_locked()?;
            }
        } else {
            if self.inventory.active.bytes != 0
                || self.inventory.active.record_count != 0
                || self.inventory.active.sha256 != digest(b"")
            {
                return Err(invalid("acknowledged active log is missing"));
            }
            self.save_inventory()?;
            self.create_active()?;
        }
        Ok(())
    }

    fn create_active(&mut self) -> io::Result<()> {
        let file = new_file(&self.active_path())?;
        file.sync_all()?;
        sync_dir(&self.config.log_dir)?;
        self.file = Some(file);
        self.hasher = Sha256::new();
        Ok(())
    }

    fn save_inventory(&self) -> io::Result<()> {
        let bytes = serde_json::to_vec(&self.inventory)
            .map_err(|_| invalid("cannot serialize segment inventory"))?;
        if bytes.len() as u64 > MAX_INVENTORY_BYTES {
            return Err(invalid("segment inventory exceeds supported bound"));
        }
        self.ensure_capacity(bytes.len() as u64)?;
        let temp = self.config.log_dir.join("inventory.tmp");
        let mut options = OpenOptions::new();
        options.create(true).truncate(true).write(true);
        private_mode(&mut options);
        let mut file = options.open(&temp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(temp, self.config.log_dir.join("inventory.json"))?;
        sync_dir(&self.config.log_dir)
    }

    fn ensure_capacity(&self, extra: u64) -> io::Result<()> {
        let mut total = extra;
        for entry in std::fs::read_dir(&self.config.log_dir)? {
            let metadata = entry?.metadata()?;
            total = total
                .checked_add(metadata.len())
                .ok_or_else(|| invalid("log size overflow"))?;
        }
        if total > self.config.max_log_bytes {
            return Err(invalid(
                "log directory capacity reached; no logs were deleted",
            ));
        }
        Ok(())
    }

    pub fn append(&mut self, events: &[Event]) -> io::Result<Segment> {
        if events.is_empty() {
            return Ok(self.inventory.active.clone());
        }
        if self.inventory.active.bytes >= self.config.segment_bytes {
            FileExt::lock_exclusive(&self.rotation_lock)?;
            let result = self.rotate_locked();
            FileExt::unlock(&self.rotation_lock)?;
            result?;
        }
        let mut bytes = Vec::new();
        let mut count = self.inventory.active.record_count;
        for event in events {
            validate_path(&event.page_uri).map_err(invalid)?;
            if event.catalog_id.len() != 64
                || !event.catalog_id.bytes().all(|b| b.is_ascii_hexdigit())
            {
                return Err(invalid("invalid accepted event catalog identity"));
            }
            count = count
                .checked_add(1)
                .ok_or_else(|| invalid("record sequence overflow"))?;
            serde_json::to_writer(
                &mut bytes,
                &Record {
                    schema_version: 1,
                    received_at_us: event.received_at_us,
                    site_id: self.config.site_id.clone(),
                    catalog_id: event.catalog_id.clone(),
                    page_uri: event.page_uri.clone(),
                    segment_id: self.inventory.active.segment_id,
                    record_seq: count,
                },
            )
            .map_err(|_| invalid("cannot serialize accepted event"))?;
            bytes.push(b'\n');
        }
        // Reserve an additional inventory-sized write before altering the log.
        let inventory_size = serde_json::to_vec(&self.inventory)
            .map_err(|_| invalid("cannot encode inventory"))?
            .len();
        self.ensure_capacity(bytes.len() as u64 + inventory_size as u64 + 4096)?;
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| invalid("active log unavailable"))?;
        file.write_all(&bytes)?;
        self.hit("append")?;
        self.file.as_ref().expect("active log").sync_all()?;
        self.hit("log-sync")?;
        self.hasher.update(&bytes);
        self.inventory.active.record_count = count;
        self.inventory.active.bytes += bytes.len() as u64;
        self.inventory.active.sha256 = hex_digest(&self.hasher);
        self.save_inventory()?;
        self.hit("inventory")?;
        Ok(self.inventory.active.clone())
    }

    fn rotate_locked(&mut self) -> io::Result<()> {
        if let Some(file) = self.file.take() {
            file.sync_all()?;
            drop(file);
        }
        let archive = self.archive_path(self.inventory.active.segment_id);
        if archive.try_exists()? {
            return Err(invalid("immutable rotation filename already exists"));
        }
        std::fs::rename(self.active_path(), archive)?;
        sync_dir(&self.config.log_dir)?;
        self.hit("rename")?;
        self.finish_rotation()
    }

    fn finish_rotation(&mut self) -> io::Result<()> {
        let old = self.inventory.active.clone();
        self.inventory.active = Segment::new(Some(old.segment_id));
        self.inventory.closed.push(old);
        self.save_inventory()?;
        self.hit("rotation-inventory")?;
        self.create_active()?;
        self.hit("new-active")
    }

    fn hit(&self, _point: &str) -> io::Result<()> {
        #[cfg(test)]
        if self.fail_at == Some(_point) {
            return Err(invalid("injected storage failure"));
        }
        Ok(())
    }
}

fn scan(
    path: &Path,
    checkpoint: &Segment,
    site_id: &str,
    allow_partial: bool,
) -> io::Result<(Segment, Sha256, Vec<u8>)> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut hasher = Sha256::new();
    let mut scanned = Segment {
        bytes: 0,
        record_count: 0,
        sha256: digest(b""),
        ..checkpoint.clone()
    };
    let mut verified =
        checkpoint.bytes == 0 && checkpoint.record_count == 0 && checkpoint.sha256 == digest(b"");
    let mut partial = Vec::new();
    loop {
        let mut line = Vec::new();
        (&mut reader)
            .take(MAX_RECORD_BYTES + 1)
            .read_until(b'\n', &mut line)?;
        if line.is_empty() {
            break;
        }
        if line.len() as u64 > MAX_RECORD_BYTES {
            return Err(invalid("accepted log contains an oversized record"));
        }
        if !line.ends_with(b"\n") {
            if !allow_partial {
                return Err(invalid("immutable segment has an incomplete record"));
            }
            partial = line;
            break;
        }
        let record: Record = serde_json::from_slice(&line)
            .map_err(|_| invalid("invalid accepted-event record; refusing to skip it"))?;
        if record.schema_version != 1
            || record.site_id != site_id
            || record.segment_id != checkpoint.segment_id
            || record.record_seq != scanned.record_count + 1
            || record.catalog_id.len() != 64
            || !record.catalog_id.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(invalid(
                "accepted record identity/sequence does not match inventory",
            ));
        }
        validate_path(&record.page_uri).map_err(invalid)?;
        hasher.update(&line);
        scanned.bytes += line.len() as u64;
        scanned.record_count += 1;
        if scanned.bytes == checkpoint.bytes {
            verified = scanned.record_count == checkpoint.record_count
                && hex_digest(&hasher) == checkpoint.sha256;
        }
    }
    if !verified {
        return Err(invalid("acknowledged log prefix is missing or changed"));
    }
    scanned.sha256 = hex_digest(&hasher);
    Ok((scanned, hasher, partial))
}

fn hex_digest(hasher: &Sha256) -> String {
    hasher
        .clone()
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}

fn private_mode(options: &mut OpenOptions) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o640);
    }
}

fn new_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    private_mode(&mut options);
    options.open(path)
}

fn open_control(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    private_mode(&mut options);
    options.open(path)
}

fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::fixture;

    fn event(time: i64) -> Event {
        Event {
            received_at_us: time,
            catalog_id: digest(b"fixture"),
            page_uri: "/".into(),
        }
    }

    fn records(dir: &Path) -> Vec<Record> {
        let mut records = Vec::new();
        for file in std::fs::read_dir(dir).unwrap() {
            let path = file.unwrap().path();
            if path.extension().is_some_and(|e| e == "jsonl") {
                for line in BufReader::new(File::open(path).unwrap()).lines() {
                    records.push(serde_json::from_str(&line.unwrap()).unwrap());
                }
            }
        }
        records
    }

    #[test]
    fn equal_and_backward_timestamps_survive_restart_and_multiple_rotations() {
        let temp = tempfile::tempdir().unwrap();
        let config = fixture(temp.path());
        let mut log = Journal::open(&config).unwrap();
        let events = vec![event(100), event(100), event(90)];
        let first = log.append(&events).unwrap();
        assert_eq!(first.record_count, 3);
        drop(log);
        let mut log = Journal::open(&config).unwrap();
        assert_eq!(log.inventory.closed[0].segment_id, first.segment_id);
        assert_eq!(log.inventory.closed[0].record_count, 3);
        for _ in 0..40 {
            log.append(&[event(100)]).unwrap();
        }
        assert!(log.inventory.closed.len() >= 3);
        drop(log);
        let log = Journal::open(&config).unwrap();
        let rows = records(&config.log_dir);
        assert_eq!(rows.len(), 43);
        assert_eq!(rows.iter().filter(|r| r.received_at_us == 90).count(), 1);
        let unique: BTreeSet<_> = rows.iter().map(|r| (r.segment_id, r.record_seq)).collect();
        assert_eq!(unique.len(), rows.len());
        assert_eq!(log.inventory.active.record_count, 0);
    }

    #[test]
    fn acknowledged_prefix_survives_failure_at_every_publication_boundary() {
        for point in [
            "append",
            "log-sync",
            "inventory",
            "rename",
            "rotation-inventory",
            "new-active",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let config = fixture(temp.path());
            let mut log = Journal::open(&config).unwrap();
            log.append(&[event(10)]).unwrap();
            log.fail_at = Some(point);
            if ["rename", "rotation-inventory", "new-active"].contains(&point) {
                assert!(log.rotate_locked().is_err(), "{point}");
            } else {
                assert!(log.append(&[event(20)]).is_err(), "{point}");
            }
            drop(log);
            let log = Journal::open(&config).unwrap();
            let rows = records(&config.log_dir);
            assert_eq!(
                rows.iter().filter(|r| r.received_at_us == 10).count(),
                1,
                "{point}"
            );
            let unique: BTreeSet<_> = rows.iter().map(|r| (r.segment_id, r.record_seq)).collect();
            assert_eq!(unique.len(), rows.len(), "{point}");
            drop(log);
            Journal::open(&config).unwrap();
            assert_eq!(records(&config.log_dir).len(), rows.len(), "{point}");
        }
    }

    #[test]
    fn partial_tail_is_preserved_as_evidence_without_removing_acknowledged_records() {
        let temp = tempfile::tempdir().unwrap();
        let config = fixture(temp.path());
        let mut log = Journal::open(&config).unwrap();
        log.append(&[event(10)]).unwrap();
        drop(log);
        let partial = b"{\"schema_version\":1,\"received_at";
        let mut file = OpenOptions::new()
            .append(true)
            .open(config.log_dir.join("views.jsonl"))
            .unwrap();
        file.write_all(partial).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let log = Journal::open(&config).unwrap();
        assert!(log.recovered_partial);
        assert_eq!(records(&config.log_dir).len(), 1);
        let evidence: Vec<_> = std::fs::read_dir(&config.log_dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "bin"))
            .collect();
        assert_eq!(evidence.len(), 1);
        assert_eq!(std::fs::read(&evidence[0]).unwrap(), partial);
    }

    #[test]
    fn changed_or_missing_acknowledged_data_and_missing_segments_block_startup() {
        for damage in [
            "truncate",
            "modify",
            "missing-active",
            "missing-archive",
            "duplicate",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let config = fixture(temp.path());
            let mut log = Journal::open(&config).unwrap();
            log.append(&[event(10)]).unwrap();
            let active = log.active_path();
            let archive = log.archive_path(log.inventory.active.segment_id);
            if damage == "missing-archive" {
                log.rotate_locked().unwrap();
            }
            drop(log);
            match damage {
                "truncate" => OpenOptions::new()
                    .write(true)
                    .open(&active)
                    .unwrap()
                    .set_len(10)
                    .unwrap(),
                "modify" => {
                    let text = std::fs::read_to_string(&active)
                        .unwrap()
                        .replace("\"received_at_us\":10", "\"received_at_us\":11");
                    std::fs::write(&active, text).unwrap();
                }
                "missing-active" => std::fs::remove_file(&active).unwrap(),
                "missing-archive" => std::fs::remove_file(archive).unwrap(),
                "duplicate" => {
                    std::fs::copy(
                        &active,
                        config
                            .log_dir
                            .join(format!("views-{}.jsonl", Uuid::new_v4())),
                    )
                    .unwrap();
                }
                _ => unreachable!(),
            }
            assert!(Journal::open(&config).is_err(), "{damage}");
        }
    }

    #[test]
    fn capacity_failure_never_deletes_records_and_writer_lock_is_exclusive() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = fixture(temp.path());
        config.max_log_bytes = 20_000;
        let mut log = Journal::open(&config).unwrap();
        assert!(Journal::open(&config).is_err());
        let mut accepted = 0;
        while log.append(&[event(10)]).is_ok() {
            accepted += 1;
            assert!(accepted < 100);
        }
        assert!(accepted > 0);
        drop(log);
        assert!(records(&config.log_dir).len() >= accepted);
        config.max_log_bytes *= 2;
        let mut log = Journal::open(&config).unwrap();
        log.append(&[event(20)]).unwrap();
        assert_eq!(records(&config.log_dir).len(), accepted + 1);
    }

    #[test]
    fn malformed_complete_record_is_not_silently_truncated() {
        let temp = tempfile::tempdir().unwrap();
        let config = fixture(temp.path());
        let log = Journal::open(&config).unwrap();
        drop(log);
        std::fs::write(config.log_dir.join("views.jsonl"), b"not JSON\n").unwrap();
        assert!(Journal::open(&config).is_err());
        assert_eq!(
            std::fs::read(config.log_dir.join("views.jsonl")).unwrap(),
            b"not JSON\n"
        );
    }
}
