// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;
use query_foundation::overlap::{OverlapPolicy, RowIdentity};
use query_foundation::snapshot::{ChunkDescriptor, DatasetSnapshot, EventTimeContract};
use query_foundation::statistics::TimeInterval;
use query_foundation::testkit::FoundationFixture;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("sensor", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

fn chunk(
    fixture: &FoundationFixture,
    id: &str,
    sequence: u64,
    logical_count: u64,
    bounds: Option<(i64, i64)>,
) -> Result<ChunkDescriptor> {
    Ok(ChunkDescriptor::new(
        id,
        sequence,
        fixture.object_descriptor(&format!("chunks/{id}.parquet"))?,
        schema(),
        logical_count,
        bounds
            .map(|(min, max)| TimeInterval::try_new(min, max))
            .transpose()?,
    ))
}

fn snapshot(chunks: Vec<ChunkDescriptor>, policy: OverlapPolicy) -> Result<DatasetSnapshot> {
    DatasetSnapshot::try_new(
        "snapshot-0001",
        schema(),
        chunks,
        Some(EventTimeContract::new("ts")),
        policy,
    )
}

#[test]
fn preserve_all_is_explicitly_declared() -> Result<()> {
    let fixture = FoundationFixture::new(schema());
    let chunks = vec![
        chunk(&fixture, "archive", 1, 10, Some((0, 10)))?,
        chunk(&fixture, "live", 2, 10, Some((10, 20)))?,
    ];
    let snapshot = DatasetSnapshot::try_new(
        "snapshot-0001",
        schema(),
        chunks,
        Some(EventTimeContract::new("ts")),
        OverlapPolicy::PreserveAll,
    )?;
    assert_eq!(snapshot.overlap(), &OverlapPolicy::PreserveAll);

    Ok(())
}

#[test]
fn require_disjoint_accepts_only_complete_nonoverlapping_evidence() -> Result<()> {
    let fixture = FoundationFixture::new(schema());
    let disjoint = snapshot(
        vec![
            chunk(&fixture, "late-published", 1, 10, Some((30, 39)))?,
            chunk(&fixture, "empty", 2, 0, None)?,
            chunk(&fixture, "early-published", 3, 10, Some((0, 9)))?,
        ],
        OverlapPolicy::RequireDisjoint,
    )?;
    assert_eq!(disjoint.overlap(), &OverlapPolicy::RequireDisjoint);

    let error = snapshot(
        vec![
            chunk(&fixture, "archive", 1, 10, Some((0, 10)))?,
            chunk(&fixture, "live", 2, 10, Some((10, 20)))?,
        ],
        OverlapPolicy::RequireDisjoint,
    )
    .expect_err("inclusive endpoint overlap must fail");
    let message = error.to_string();
    assert!(message.contains("archive"), "{message}");
    assert!(message.contains("live"), "{message}");
    assert!(message.contains("[0..=10]"), "{message}");
    assert!(message.contains("[10..=20]"), "{message}");

    let error = snapshot(
        vec![chunk(&fixture, "unknown", 1, 10, None)?],
        OverlapPolicy::RequireDisjoint,
    )
    .expect_err("missing bounds must not imply disjointness");
    assert!(error.to_string().contains("unknown"), "{error}");
    assert!(
        error.to_string().contains("without event-time bounds"),
        "{error}"
    );

    let error = DatasetSnapshot::try_new(
        "snapshot-0001",
        schema(),
        vec![chunk(&fixture, "ordinary", 1, 10, None)?],
        None,
        OverlapPolicy::RequireDisjoint,
    )
    .expect_err("disjoint ranges require an event-time contract");
    assert!(
        error
            .to_string()
            .contains("requires an event-time contract"),
        "{error}"
    );

    Ok(())
}

#[test]
fn key_policies_validate_and_expose_row_identity() -> Result<()> {
    let fixture = FoundationFixture::new(schema());
    let key = RowIdentity::try_new(["sensor", "ts"])?;
    let reject = snapshot(
        vec![chunk(&fixture, "archive", 1, 10, Some((0, 10)))?],
        OverlapPolicy::reject_duplicate_key(key.clone()),
    )?;
    assert_eq!(
        reject.overlap(),
        &OverlapPolicy::RejectDuplicateKey { key: key.clone() }
    );

    let prefer = snapshot(
        vec![
            chunk(&fixture, "archive", 1, 10, Some((0, 10)))?,
            chunk(&fixture, "live", 2, 10, Some((5, 15)))?,
        ],
        OverlapPolicy::prefer_by_sequence(key.clone()),
    )?;
    assert_eq!(prefer.overlap(), &OverlapPolicy::PreferBySequence { key });

    let missing = RowIdentity::try_new(["missing"])?;
    let error = snapshot(
        vec![chunk(&fixture, "archive", 1, 10, Some((0, 10)))?],
        OverlapPolicy::reject_duplicate_key(missing),
    )
    .expect_err("missing key column must fail");
    assert!(
        error.to_string().contains("row identity column 'missing'"),
        "{error}"
    );

    Ok(())
}

#[test]
fn invalid_row_identity_definitions_fail_immediately() {
    let error =
        RowIdentity::try_new(Vec::<String>::new()).expect_err("empty row identity must fail");
    assert!(error.to_string().contains("at least one column"), "{error}");

    let error =
        RowIdentity::try_new(["ts", "ts"]).expect_err("duplicate row identity columns must fail");
    assert!(
        error.to_string().contains("duplicate row identity column"),
        "{error}"
    );

    let error = RowIdentity::try_new([""]).expect_err("empty row identity column must fail");
    assert!(
        error.to_string().contains("column must not be empty"),
        "{error}"
    );
}
