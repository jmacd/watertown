// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};
use provider::factory::logfile_ingest::{LogfileIngestConfig, TimestampUnit, execute};
use provider::{ExecutionContext, FactoryContext, Provider};
use site_analytics_collector::{
    config::Config,
    journal::{Event, Journal},
};
use std::{collections::BTreeMap, io::Write, path::PathBuf, sync::Arc};
use tlogfs::{OpLogPersistence, PondTxnMetadata, PondUserMetadata};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;
type Counts = BTreeMap<(i64, String), i64>;

struct Harness {
    temp: tempfile::TempDir,
    collector: Config,
    ingest: LogfileIngestConfig,
    pond_id: String,
    pond: PathBuf,
}

impl Harness {
    async fn new() -> Self {
        let temp = tempfile::tempdir().expect("private fixture directory");
        let logs = temp.path().join("logs");
        let collector = Config {
            site_id: "noyo".into(),
            origin: "https://noyocenter.org".into(),
            listen: "127.0.0.1:0".parse().expect("loopback fixture address"),
            catalog_dir: temp.path().join("catalogs"),
            log_dir: logs.clone(),
            endpoint: "/_analytics/view".into(),
            max_log_bytes: 1048576,
            segment_bytes: 4096,
        };
        let ingest = LogfileIngestConfig {
            active_pattern: logs.join("views.jsonl").to_string_lossy().into(),
            archived_pattern: logs.join("views-*.jsonl").to_string_lossy().into(),
            pond_path: "/analytics/raw".into(),
            timestamp_field: Some("received_at_us".into()),
            timestamp_unit: TimestampUnit::Microseconds,
            analytics_site_id: Some("noyo".into()),
        };
        ingest
            .validate()
            .expect("valid collector ingestion configuration");
        let pond = temp.path().join("pond");
        let pond_id = uuid7::uuid7().to_string();
        let _ = OpLogPersistence::create(
            &pond,
            pond_id.clone(),
            PondUserMetadata::new(vec!["test".into()]),
        )
        .await
        .expect("create persistent test pond");
        Self {
            temp,
            collector,
            ingest,
            pond_id,
            pond,
        }
    }

    async fn run(&self, commit: bool) -> TestResult<(Counts, u64)> {
        let mut persistence = OpLogPersistence::open(&self.pond, self.pond_id.clone()).await?;
        let metadata = PondTxnMetadata::new(
            persistence.last_txn_seq_for(&self.pond_id) + 1,
            PondUserMetadata::new(vec!["test".into()]),
        );
        let tx = persistence.begin_write(&metadata).await?;
        let state = tx.state()?;
        let context = state
            .as_provider_context()
            .with_cache_dir(self.temp.path().join("cache"));
        let root = context.filesystem().root().await?;
        execute(
            serde_json::to_value(&self.ingest)?,
            FactoryContext::new(context.clone(), root.node_path().id()),
            ExecutionContext::pond_readwriter(vec![]),
        )
        .await?;
        let id = root
            .get_node_path("/analytics/raw/_inventory.json")
            .await?
            .id();
        let version = context.persistence.metadata(id).await?.version;
        let inventory: serde_json::Value = serde_json::from_slice(
            &root
                .read_file_path_to_vec("/analytics/raw/_inventory.json")
                .await?,
        )?;
        if inventory["closed"]
            .as_array()
            .expect("closed segments")
            .iter()
            .chain(std::iter::once(&inventory["active"]))
            .all(|segment| segment["record_count"] == 0)
        {
            if commit {
                let _ = tx.commit().await?;
            }
            return Ok((Counts::new(), version));
        }
        let provider =
            Provider::with_context(Arc::new(context.filesystem()), Arc::new(context.clone()));
        let table = provider
            .create_provider_for_url(
                "site-analytics:///analytics/raw/views-*.jsonl",
                &context.datafusion_session,
            )
            .await?;
        let _ = context.datafusion_session.register_table("events", table)?;
        let batches = context
            .datafusion_session
            .sql(
                "SELECT date_trunc('hour', timestamp) AS hour, page_uri, COUNT(*) AS page_views \
             FROM events GROUP BY date_trunc('hour', timestamp), page_uri ORDER BY hour, page_uri",
            )
            .await?
            .collect()
            .await?;
        let mut counts = Counts::new();
        for batch in batches {
            let times = batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("UTC microsecond buckets");
            let paths = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("URI dimension");
            let views = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("reference COUNT output");
            for row in 0..batch.num_rows() {
                assert!(
                    counts
                        .insert(
                            (times.value(row), paths.value(row).into()),
                            views.value(row)
                        )
                        .is_none()
                );
            }
        }
        if commit {
            let _ = tx.commit().await?;
        }
        Ok((counts, version))
    }
}

fn append(journal: &mut Journal, expected: &mut Counts, time: i64, page: &str, count: usize) {
    let event = Event {
        received_at_us: time,
        page_uri: page.into(),
        catalog_id: "a".repeat(64),
    };
    let _ = journal
        .append(&vec![event; count])
        .expect("durable collector append");
    *expected
        .entry((time.div_euclid(3_600_000_000) * 3_600_000_000, page.into()))
        .or_default() += count as i64;
}

#[tokio::test]
async fn collector_to_pond_reference_handles_rotations_restarts_and_transaction_retry() {
    let harness = Harness::new().await;
    let mut journal = Journal::open(&harness.collector).unwrap();
    let mut expected = Counts::new();
    let empty = harness.run(true).await.unwrap();
    assert!(empty.0.is_empty());
    assert_eq!(harness.run(true).await.unwrap(), empty);
    append(&mut journal, &mut expected, 3_600_000_001, "/", 2);
    append(&mut journal, &mut expected, -1, "/water", 1);
    let (counts, first_version) = harness.run(true).await.unwrap();
    assert_eq!(counts, expected);
    assert_eq!(
        harness.run(true).await.unwrap(),
        (expected.clone(), first_version)
    );

    append(&mut journal, &mut expected, 3_600_000_001, "/", 1);
    append(&mut journal, &mut expected, 0, "/water", 1);
    assert_eq!(harness.run(true).await.unwrap().0, expected);
    // Several rotations happen between snapshots, including a new active file
    // larger than the previously ingested one.
    for _ in 0..5 {
        append(&mut journal, &mut expected, 7_200_000_000, "/", 25);
    }
    let before_partial = harness.run(true).await.unwrap();
    assert_eq!(before_partial.0, expected);
    std::fs::OpenOptions::new()
        .append(true)
        .open(harness.collector.log_dir.join("views.jsonl"))
        .unwrap()
        .write_all(b"{\"schema_version\":")
        .unwrap();
    assert_eq!(harness.run(true).await.unwrap(), before_partial);
    drop(journal);
    let mut journal = Journal::open(&harness.collector).unwrap();
    assert!(journal.recovered_partial);
    assert_eq!(harness.run(true).await.unwrap().0, expected);
    append(&mut journal, &mut expected, 0, "/", 1);
    assert_eq!(harness.run(false).await.unwrap().0, expected);
    assert_eq!(harness.run(true).await.unwrap().0, expected);
    assert_eq!(harness.run(true).await.unwrap().0, expected);

    let inventory: serde_json::Value = serde_json::from_slice(
        &std::fs::read(harness.collector.log_dir.join("inventory.json")).unwrap(),
    )
    .unwrap();
    let id = inventory["closed"][0]["segment_id"].as_str().unwrap();
    let archive = harness.collector.log_dir.join(format!("views-{id}.jsonl"));
    let backup = harness.temp.path().join("restore.jsonl");
    std::fs::rename(&archive, &backup).unwrap();
    assert!(
        harness.run(true).await.is_err(),
        "missing archive must fail"
    );
    std::fs::rename(&backup, &archive).unwrap();
    assert_eq!(harness.run(true).await.unwrap().0, expected);
    let duplicate = harness.collector.log_dir.join("duplicate.jsonl");
    let _ = std::fs::copy(&archive, &duplicate).unwrap();
    assert!(
        harness
            .run(true)
            .await
            .unwrap_err()
            .to_string()
            .contains("duplicate")
    );
    std::fs::remove_file(duplicate).unwrap();
    let original = std::fs::read(&archive).unwrap();
    let changed = String::from_utf8(original.clone())
        .unwrap()
        .replace("\"/water\"", "\"/other\"");
    std::fs::write(&archive, changed).unwrap();
    assert!(
        harness.run(true).await.is_err(),
        "modified archive must fail"
    );
    std::fs::write(&archive, original).unwrap();
    assert_eq!(harness.run(true).await.unwrap().0, expected);

    let lock = std::fs::File::open(harness.collector.log_dir.join(".rotation.lock")).unwrap();
    fs2::FileExt::lock_exclusive(&lock).unwrap();
    assert!(
        harness
            .run(true)
            .await
            .unwrap_err()
            .to_string()
            .contains("rotation/recovery")
    );
    fs2::FileExt::unlock(&lock).unwrap();
    assert_eq!(harness.run(true).await.unwrap().0, expected);
}

#[tokio::test]
async fn collector_ingestion_rejects_rewritten_prefix_even_with_updated_host_checksum() {
    let mut harness = Harness::new().await;
    let mut journal = Journal::open(&harness.collector).unwrap();
    let mut expected = Counts::new();
    append(&mut journal, &mut expected, 1, "/", 2);
    let prior = harness.run(true).await.unwrap();
    harness.ingest.analytics_site_id = Some("caspar-water".into());
    assert!(harness.run(true).await.is_err());
    harness.ingest.analytics_site_id = Some("noyo".into());
    assert_eq!(harness.run(true).await.unwrap(), prior);

    append(&mut journal, &mut expected, 1, "/", 1);
    let path = harness.collector.log_dir.join("views.jsonl");
    let inventory_path = harness.collector.log_dir.join("inventory.json");
    let original = std::fs::read(&path).unwrap();
    let original_inventory = std::fs::read(&inventory_path).unwrap();
    let changed = String::from_utf8(original.clone()).unwrap().replacen(
        "\"received_at_us\":1",
        "\"received_at_us\":2",
        1,
    );
    let mut inventory: serde_json::Value = serde_json::from_slice(&original_inventory).unwrap();
    inventory["active"]["sha256"] =
        site_analytics_collector::catalog::digest(changed.as_bytes()).into();
    std::fs::write(&path, changed).unwrap();
    std::fs::write(&inventory_path, serde_json::to_vec(&inventory).unwrap()).unwrap();
    assert!(
        harness
            .run(true)
            .await
            .unwrap_err()
            .to_string()
            .contains("prefix conflicts")
    );
    std::fs::write(&path, original).unwrap();
    std::fs::write(&inventory_path, original_inventory).unwrap();
    assert_eq!(harness.run(true).await.unwrap().0, expected);
}

#[tokio::test]
async fn strict_reader_rejects_bad_records_without_changing_jsonlogs() {
    use futures::TryStreamExt;
    let format = provider::FormatRegistry::get_provider("site-analytics").unwrap();
    let url = provider::Url::parse("site-analytics:///fixture.jsonl").unwrap();
    let record = serde_json::json!({
        "schema_version": 1, "received_at_us": 1, "site_id": "noyo",
        "catalog_id": "a".repeat(64), "page_uri": "/", "segment_id": uuid::Uuid::new_v4(),
        "record_seq": 1,
    })
    .to_string()
        + "\n";
    let invalid = [
        "\n".into(),
        "{}\n".into(),
        record.trim_end().into(),
        record.replace("\"record_seq\":1", "\"record_seq\":0"),
        record.replace("\"site_id\":\"noyo\"", "\"site_id\":\"bad/site\""),
        record.replace("\"received_at_us\":1", "\"received_at_us\":\"1\""),
        record.replace("\"page_uri\":\"/\"", "\"page_uri\":\"/?private=true\""),
        format!("{record}{record}"),
        format!("{record}{}\n", "x".repeat(4097)),
    ];
    for data in invalid {
        let (_, stream) = format
            .open_stream(Box::pin(std::io::Cursor::new(data)), &url)
            .await
            .unwrap();
        assert!(stream.try_collect::<Vec<_>>().await.is_err());
    }
    let mut many = String::new();
    let mut value: serde_json::Value = serde_json::from_str(&record).unwrap();
    for seq in 1..=1024 {
        value["record_seq"] = seq.into();
        many.push_str(&value.to_string());
        many.push('\n');
    }
    many.push_str("invalid\n");
    let (_, mut stream) = format
        .open_stream(Box::pin(std::io::Cursor::new(many)), &url)
        .await
        .unwrap();
    assert_eq!(stream.try_next().await.unwrap().unwrap().num_rows(), 1024);
    assert!(
        stream.try_next().await.is_err(),
        "late corruption must not disappear after a yielded batch"
    );
    let (_, stream) = format
        .open_stream(Box::pin(std::io::Cursor::new(record.clone())), &url)
        .await
        .unwrap();
    assert_eq!(
        stream.try_collect::<Vec<_>>().await.unwrap()[0].num_rows(),
        1
    );
    let permissive = provider::FormatRegistry::get_provider("jsonlogs").unwrap();
    let (_, stream) = permissive
        .open_stream(
            Box::pin(std::io::Cursor::new(format!("{record}invalid\n"))),
            &url,
        )
        .await
        .unwrap();
    assert_eq!(
        stream.try_collect::<Vec<_>>().await.unwrap()[0].num_rows(),
        1
    );
}
