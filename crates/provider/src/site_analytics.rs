// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Version-one collector wire contract, independent of permissive log readers.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::File,
    io::{self, BufRead, BufReader, Read, Seek, Write},
    path::Path,
};
use uuid::Uuid;

pub const MAX_RECORD_BYTES: u64 = 4096;
pub const MAX_INVENTORY_BYTES: u64 = 4 * 1024 * 1024;

pub fn invalid(message: &str) -> io::Error {
    io::Error::other(format!("analytics coverage error: {message}"))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub schema_version: u32,
    pub received_at_us: i64,
    pub site_id: String,
    pub catalog_id: String,
    pub page_uri: String,
    pub segment_id: Uuid,
    pub record_seq: u64,
}

pub fn validate_site(id: &str) -> io::Result<()> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
    {
        return Err(invalid("invalid site identity"));
    }
    Ok(())
}

fn valid_digest(id: &str) -> bool {
    id.len() == 64
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub fn parse_record(line: &[u8]) -> io::Result<Record> {
    if line.len() as u64 > MAX_RECORD_BYTES || !line.ends_with(b"\n") {
        return Err(invalid("oversized or incomplete record"));
    }
    let record: Record =
        serde_json::from_slice(line).map_err(|_| invalid("invalid accepted-event JSON/schema"))?;
    validate_site(&record.site_id)?;
    let path = &record.page_uri;
    if record.schema_version != 1
        || record.record_seq == 0
        || !valid_digest(&record.catalog_id)
        || !path.starts_with('/')
        || path.len() > 1024
        || path.contains("//")
        || path.contains(['\\', '%', '?', '#'])
        || path.chars().any(|c| c.is_control() || c.is_whitespace())
        || path.split('/').any(|p| matches!(p, "." | ".."))
        || chrono::DateTime::from_timestamp_micros(record.received_at_us).is_none()
    {
        return Err(invalid("invalid accepted-event fields"));
    }
    Ok(record)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Segment {
    pub segment_id: Uuid,
    pub predecessor: Option<Uuid>,
    pub record_count: u64,
    pub bytes: u64,
    pub sha256: String,
}

impl Segment {
    pub fn filename(&self) -> String {
        format!("views-{}.jsonl", self.segment_id)
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Inventory {
    pub schema_version: u32,
    pub site_id: String,
    pub closed: Vec<Segment>,
    pub active: Segment,
}

impl Inventory {
    pub fn parse(bytes: &[u8], site: &str) -> io::Result<Self> {
        if bytes.len() as u64 > MAX_INVENTORY_BYTES {
            return Err(invalid("inventory exceeds supported bound"));
        }
        let inventory: Self =
            serde_json::from_slice(bytes).map_err(|_| invalid("invalid inventory JSON/schema"))?;
        if inventory.schema_version != 1 || inventory.site_id != site {
            return Err(invalid("inventory version/site mismatch"));
        }
        let mut previous = None;
        let mut ids = BTreeSet::new();
        for segment in inventory.segments() {
            if segment.predecessor != previous
                || !ids.insert(segment.segment_id)
                || !valid_digest(&segment.sha256)
                || (segment.record_count == 0) != (segment.bytes == 0)
            {
                return Err(invalid("inventory gap, duplicate or invalid checkpoint"));
            }
            previous = Some(segment.segment_id);
        }
        Ok(inventory)
    }

    pub fn segments(&self) -> impl Iterator<Item = &Segment> {
        self.closed.iter().chain(std::iter::once(&self.active))
    }

    pub fn extends(&self, prior: &Self) -> io::Result<()> {
        if self.site_id != prior.site_id || !self.closed.starts_with(&prior.closed) {
            return Err(invalid("closed history changed or disappeared"));
        }
        let next = self
            .segments()
            .nth(prior.closed.len())
            .ok_or_else(|| invalid("previous active segment disappeared"))?;
        if next.segment_id != prior.active.segment_id
            || next.bytes < prior.active.bytes
            || next.record_count < prior.active.record_count
            || (next.bytes == prior.active.bytes && next != &prior.active)
        {
            return Err(invalid("previous active checkpoint regressed or changed"));
        }
        Ok(())
    }
}

pub struct Snapshot {
    pub inventory: Inventory,
    // Anonymous private scratch files bound memory, and release the host lock
    // before pond I/O. Their lifetime is exactly this ingestion attempt.
    pub file: File,
    pub offsets: Vec<u64>,
}

fn regular_file(path: &Path) -> io::Result<File> {
    let source_error = |error: io::Error| {
        io::Error::new(
            error.kind(),
            format!(
                "analytics coverage error: source {} unavailable: {error}",
                path.display(),
            ),
        )
    };
    if !std::fs::symlink_metadata(path)
        .map_err(source_error)?
        .file_type()
        .is_file()
    {
        return Err(invalid("source must be a regular file, not a symlink"));
    }
    File::open(path).map_err(source_error)
}

impl Snapshot {
    pub fn capture(dir: &Path, site: &str) -> io::Result<Self> {
        validate_site(site)?;
        let lock = regular_file(&dir.join(".rotation.lock"))?;
        fs2::FileExt::try_lock_shared(&lock).map_err(|error| {
            invalid(&format!(
                "cannot acquire collector rotation/recovery lock: {error}"
            ))
        })?;
        let mut bytes = Vec::new();
        let _ = regular_file(&dir.join("inventory.json"))?
            .take(MAX_INVENTORY_BYTES + 1)
            .read_to_end(&mut bytes)?;
        let inventory = Inventory::parse(&bytes, site)?;
        let expected: BTreeSet<_> = inventory
            .closed
            .iter()
            .map(Segment::filename)
            .chain(std::iter::once("views.jsonl".into()))
            .collect();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if entry.path().extension().is_some_and(|ext| ext == "jsonl")
                && !expected.contains(&entry.file_name().to_string_lossy().to_string())
            {
                return Err(invalid(
                    "unlisted or duplicate source segment; reconcile before ingesting",
                ));
            }
        }
        let mut staged = tempfile::tempfile()?;
        let mut offsets = Vec::new();
        for segment in inventory.segments() {
            let active = segment.segment_id == inventory.active.segment_id;
            let name = if active {
                "views.jsonl".into()
            } else {
                segment.filename()
            };
            let file = regular_file(&dir.join(name))?;
            let size = file.metadata()?.len();
            if size < segment.bytes || (!active && size != segment.bytes) {
                return Err(invalid("source segment length differs from inventory"));
            }
            // Only the published durable prefix is a member of this snapshot.
            // An in-flight append (complete or partial) belongs to a later run.
            let mut reader = BufReader::new(file.take(segment.bytes));
            offsets.push(staged.stream_position()?);
            let mut hasher = Sha256::new();
            let mut count = 0u64;
            loop {
                let mut line = Vec::new();
                let _ = (&mut reader)
                    .take(MAX_RECORD_BYTES + 1)
                    .read_until(b'\n', &mut line)?;
                if line.is_empty() {
                    break;
                }
                let record = parse_record(&line)?;
                count += 1;
                if record.site_id != site
                    || record.segment_id != segment.segment_id
                    || record.record_seq != count
                {
                    return Err(invalid(
                        "segment identity, site or record sequence mismatch",
                    ));
                }
                hasher.update(&line);
                staged.write_all(&line)?;
            }
            let digest: String = hasher
                .finalize()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            if count != segment.record_count || digest != segment.sha256 {
                return Err(invalid("segment record count or checksum mismatch"));
            }
        }
        staged.rewind()?;
        Ok(Self {
            inventory,
            file: staged,
            offsets,
        })
    }
}
