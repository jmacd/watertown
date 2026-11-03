// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use super::{LogfileIngestConfig, TimestampUnit, read_pond_state};
use crate::{
    FactoryContext,
    site_analytics::{Inventory, MAX_INVENTORY_BYTES, Snapshot, invalid},
};
use std::{
    io::{Read, Seek, SeekFrom},
    path::Path,
};
use tinyfs::{EntryType, ResultExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use utilities::bao_outboard::IncrementalHashState;

const INVENTORY: &str = "_inventory.json";

pub(super) fn validate_config(config: &LogfileIngestConfig) -> tinyfs::Result<()> {
    let site = config
        .analytics_site_id
        .as_deref()
        .ok_or_else(|| tinyfs::Error::Other("analytics site is required".into()))?;
    crate::site_analytics::validate_site(site).map_other()?;
    let active = Path::new(&config.active_pattern);
    let parent = active
        .parent()
        .ok_or_else(|| invalid("source has no parent"))
        .map_other()?;
    if !active.is_absolute()
        || active.file_name().is_none_or(|name| name != "views.jsonl")
        || config.archived_pattern != parent.join("views-*.jsonl").to_string_lossy()
        || config.timestamp_field.as_deref() != Some("received_at_us")
        || config.timestamp_unit != TimestampUnit::Microseconds
    {
        return Err(tinyfs::Error::Other(
            "analytics requires absolute views.jsonl/views-*.jsonl paths in one directory and received_at_us microseconds".into(),
        ));
    }
    Ok(())
}

pub(super) async fn ingest(
    context: &FactoryContext,
    config: &LogfileIngestConfig,
) -> tinyfs::Result<()> {
    validate_config(config)?;
    let site = config.analytics_site_id.clone().expect("validated site");
    let dir = Path::new(&config.active_pattern)
        .parent()
        .expect("validated parent")
        .to_owned();
    let mut snapshot = tokio::task::spawn_blocking(move || Snapshot::capture(&dir, &site))
        .await
        .map_other()?
        .map_other()?;
    let root = context.context.filesystem().root().await?;
    let manifest = format!("{}/{}", config.pond_path, INVENTORY);
    let prior = match root.async_reader_path(&manifest).await {
        Ok(reader) => {
            let mut bytes = Vec::new();
            let _ = reader
                .take(MAX_INVENTORY_BYTES + 1)
                .read_to_end(&mut bytes)
                .await
                .map_other()?;
            Some(Inventory::parse(&bytes, &snapshot.inventory.site_id).map_other()?)
        }
        Err(tinyfs::Error::NotFound(_)) => None,
        Err(error) => return Err(error),
    };
    if let Some(prior) = &prior {
        snapshot.inventory.extends(prior).map_other()?;
    }
    let pond = read_pond_state(context, &config.pond_path).await?;
    let expected: std::collections::BTreeMap<_, _> = prior
        .iter()
        .flat_map(|inventory| inventory.segments())
        .filter(|s| s.bytes > 0)
        .map(|s| (s.filename(), s.bytes))
        .collect();
    for (name, state) in &pond {
        if name != INVENTORY && expected.get(name) != Some(&state.cumulative_size) {
            return Err(invalid("pond membership differs from its checkpoint")).map_other();
        }
    }
    if expected.keys().any(|name| !pond.contains_key(name)) {
        return Err(invalid("previously ingested pond segment is missing")).map_other();
    }

    // Validate every stored prefix before opening any pond writer. Host filenames
    // never define membership: even the active segment uses its permanent UUID.
    for (segment, offset) in snapshot.inventory.segments().zip(&snapshot.offsets) {
        if let Some(state) = pond.get(&segment.filename()) {
            let _ = snapshot.file.seek(SeekFrom::Start(*offset)).map_other()?;
            let mut reader = Read::take(&mut snapshot.file, state.cumulative_size);
            let mut hasher = IncrementalHashState::new();
            let mut buffer = [0; 65536];
            let mut read = 0;
            loop {
                let n = reader.read(&mut buffer).map_other()?;
                if n == 0 {
                    break;
                }
                hasher.ingest(&buffer[..n]);
                read += n as u64;
            }
            if read != state.cumulative_size || hasher.root_hash().to_hex().as_str() != state.blake3
            {
                return Err(invalid("known segment prefix conflicts with pond history"))
                    .map_other();
            }
        }
    }
    if prior.as_ref() == Some(&snapshot.inventory) {
        return Ok(());
    }
    let _ = root.create_dir_all(&config.pond_path).await?;
    for (segment, offset) in snapshot.inventory.segments().zip(&snapshot.offsets) {
        let start = pond
            .get(&segment.filename())
            .map_or(0, |state| state.cumulative_size);
        if start == segment.bytes {
            continue;
        }
        let _ = snapshot
            .file
            .seek(SeekFrom::Start(offset + start))
            .map_other()?;
        let path = format!("{}/{}", config.pond_path, segment.filename());
        let mut writer = root
            .async_writer_path_with_type(&path, EntryType::FilePhysicalSeries)
            .await?;
        let mut reader =
            std::io::BufReader::new(Read::take(&mut snapshot.file, segment.bytes - start));
        let mut bounds: Option<(i64, i64)> = None;
        loop {
            let mut line = Vec::new();
            let _ = std::io::BufRead::read_until(&mut reader, b'\n', &mut line).map_other()?;
            if line.is_empty() {
                break;
            }
            let record = crate::site_analytics::parse_record(&line).map_other()?;
            bounds = Some(match bounds {
                None => (record.received_at_us, record.received_at_us),
                Some((lo, hi)) => (lo.min(record.received_at_us), hi.max(record.received_at_us)),
            });
            writer.write_all(&line).await.map_other()?;
        }
        let (lo, hi) = bounds
            .ok_or_else(|| invalid("nonempty append has no records"))
            .map_other()?;
        writer.require_temporal_metadata("timestamp".into());
        writer.set_temporal_metadata(lo, hi, "timestamp".into());
        writer.shutdown().await.map_other()?;
    }
    let mut writer = root
        .async_writer_path_with_type(&manifest, EntryType::FilePhysicalVersion)
        .await?;
    let bytes = serde_json::to_vec(&snapshot.inventory).map_other()?;
    writer.write_all(&bytes).await.map_other()?;
    writer.shutdown().await.map_other()?;
    Ok(())
}
