// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Validate pre-unit JSON-log bounds against the immutable bytes. A labeled
//! member of the same source supplies the candidate axis, never the pathname.

use std::collections::BTreeSet;
use std::io::Cursor;
use std::path::Path;

use serde_json::Value;
use tinyfs::{FileID, FileVersionInfo, PersistenceLayer};

use crate::{Error, Result};

/// An unknown explicit unit or conflicting column is not a legacy omission.
pub(crate) fn candidate_column(attributes: Option<&Value>) -> Option<Option<&str>> {
    let Some(attributes) = attributes else {
        return Some(None);
    };
    let object = attributes.as_object()?;
    if object.contains_key("watertown.timestamp_unit") {
        return None;
    }
    match object.get("watertown.timestamp_column") {
        Some(value) => Some(Some(value.as_str()?)),
        None => Some(None),
    }
}

async fn rows_within(bytes: Vec<u8>, column: &str, lo: i64, hi: i64) -> Result<bool> {
    let mut saw_row = false;
    let skipped = crate::format::batch::read_clean_lines(Box::pin(Cursor::new(bytes)), |line| {
        let value = serde_json::from_str::<Value>(line).ok();
        let ts = match value.as_ref().and_then(|row| row.get(column)) {
            Some(Value::String(s)) => s.parse::<i64>().ok(),
            Some(Value::Number(n)) => n.as_i64(),
            _ => None,
        };
        saw_row = true;
        Ok(ts.is_some_and(|ts| (lo..=hi).contains(&ts)))
    })
    .await?;
    Ok(saw_row && skipped == 0)
}

pub(crate) async fn verify(
    persistence: &dyn PersistenceLayer,
    cache: &Path,
    id: FileID,
    column: &str,
    versions: &[FileVersionInfo],
) -> Result<bool> {
    let path = crate::format_cache::cache_node_dir(cache, "jsonlogs", &id.node_id())
        .join("verified-micros-v1.json");
    let previous: BTreeSet<String> = match tokio::fs::read(&path).await {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeSet::new(),
        Err(e) => return Err(e.into()),
    };
    let mut live = BTreeSet::new();
    for v in versions {
        let bound = |name| v.extended_metadata.as_ref()?.get(name)?.parse::<i64>().ok();
        let (Some(lo), Some(hi), Some(hash)) = (
            bound("min_event_time"),
            bound("max_event_time"),
            v.blake3.as_deref(),
        ) else {
            return Ok(false);
        };
        if hi < lo {
            return Ok(false);
        }
        let key = serde_json::to_vec(&("jsonlogs:micros:v1", id, v.version, hash, lo, hi, column))
            .expect("proof key is serializable");
        let key = blake3::hash(&key).to_hex().to_string();
        if !previous.contains(&key) {
            let bytes = persistence.read_file_version(id, v.version).await?;
            if blake3::hash(&bytes).to_hex().as_str() != hash {
                return Err(Error::InvalidUrl(format!(
                    "content digest mismatch: {id} v{}",
                    v.version
                )));
            }
            if !rows_within(bytes, column, lo, hi).await? {
                return Ok(false);
            }
        }
        _ = live.insert(key);
    }
    if live != previous {
        crate::version_cache::write_cache_json(&path, &live).await?;
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn requires_every_row_to_support_the_proposed_microsecond_range() {
        let valid = "{\"t\":\"-2\"}\n{\"t\":3}\n{\"t\":3}\n";
        assert!(rows_within(valid.into(), "t", -2, 3).await.unwrap());
        for invalid in [
            "{\"t\":4}",
            "{\"t\":-3}",
            "{}",
            "{\"t\":null}",
            "{\"t\":2.5}",
            "bad",
        ] {
            assert!(
                !rows_within(format!("{valid}{invalid}").into(), "t", -2, 3)
                    .await
                    .unwrap()
            );
        }
        assert!(!rows_within(vec![], "t", -2, 3).await.unwrap());
        assert_eq!(
            candidate_column(Some(&json!({"watertown.timestamp_unit":"unknown"}))),
            None
        );
        assert_eq!(
            candidate_column(Some(&json!({"watertown.timestamp_column":"other"}))),
            Some(Some("other"))
        );
    }

    #[tokio::test]
    async fn cached_proof_requires_the_same_source_and_bounds() {
        use crate::factory::test_support::{create_file_with_type, create_test_environment};
        let (fs, ctx) = create_test_environment().await;
        let bytes = br#"{"ts":"3","other":"9"}"#;
        let id = create_file_with_type(
            &fs,
            "/old",
            bytes.to_vec(),
            tinyfs::EntryType::FilePhysicalSeries,
        )
        .await
        .unwrap();
        let persistence = ctx.persistence.as_ref();
        let mut v = persistence
            .list_file_versions(id)
            .await
            .unwrap()
            .pop()
            .unwrap();
        v.blake3 = Some(blake3::hash(bytes).to_hex().to_string());
        v.extended_metadata = Some(
            [
                ("min_event_time".into(), "1".into()),
                ("max_event_time".into(), "3".into()),
            ]
            .into(),
        );
        let cache = tempfile::tempdir().unwrap();
        let check = |v: &FileVersionInfo, col: &str| {
            let v = v.clone();
            let col = col.to_owned();
            let path = cache.path();
            async move { verify(persistence, path, id, &col, &[v]).await.unwrap() }
        };
        assert!(check(&v, "ts").await);
        assert!(!check(&v, "other").await);
        let _ = v
            .extended_metadata
            .as_mut()
            .unwrap()
            .insert("max_event_time".into(), "2".into());
        assert!(!check(&v, "ts").await);
    }
}
