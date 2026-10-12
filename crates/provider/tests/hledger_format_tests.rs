// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Int64Array, StringArray, UInt64Array,
};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use provider::{FormatRegistry, SchemeKind, SchemeRegistry, Url};
use std::io::Cursor;
use std::sync::Arc;

const UNIT: i128 = 1_000_000_000_000_000_000;
const JOURNAL: &str = "\
account Assets:Bank
commodity $
Y2026

01/02 * (INV-1) Example | January fee ; transaction comment
    ; customer-account: demo
    Expenses:Office    $10.10 ; source: demo.csv:1
    Assets:Bank
        ; bank comment

2026-02-01 ! Example
    * Expenses:Office    $0.20
    Assets:Bank          $-0.20
";

async fn read(text: &str, query: &str) -> provider::Result<(SchemaRef, Vec<RecordBatch>)> {
    let provider = FormatRegistry::get_provider("hledger").expect("hledger registered");
    let url = Url::parse(&format!("hledger:///books/demo.journal{query}"))?;
    let (schema, stream) = provider
        .open_stream(Box::pin(Cursor::new(text.as_bytes().to_vec())), &url)
        .await?;
    Ok((schema, stream.try_collect().await?))
}

fn strings<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
    batch
        .column_by_name(name)
        .expect("column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("text")
}

fn amounts(batch: &RecordBatch) -> &Decimal128Array {
    batch
        .column_by_name("amount")
        .expect("amount")
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("decimal")
}

#[tokio::test]
async fn hledger_typed_postings_inference_correlation_and_comments() -> provider::Result<()> {
    assert_eq!(
        SchemeRegistry::classify("hledger"),
        Some(SchemeKind::Format)
    );
    let (schema, batches) = read(JOURNAL, "?batch_size=2").await?;
    assert_eq!(batches.len(), 2);
    assert_eq!(
        schema
            .field_with_name("amount")
            .expect("amount")
            .data_type(),
        &DataType::Decimal128(38, 18)
    );
    assert_eq!(
        schema.field_with_name("date").expect("date").data_type(),
        &DataType::Date32
    );
    let first = &batches[0];
    assert_eq!(first.num_rows(), 2);
    let sequence = first
        .column_by_name("transaction_sequence")
        .expect("sequence")
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("sequence type");
    assert_eq!(sequence.values().as_ref(), &[1, 1]);
    assert_eq!(strings(first, "source").value(0), "/books/demo.journal");
    assert_eq!(strings(first, "payee").value(0), "Example");
    assert_eq!(strings(first, "code").value(0), "INV-1");
    assert_eq!(strings(first, "description").value(0).trim(), "January fee");
    assert_eq!(strings(first, "status").value(0), "cleared");
    assert_eq!(strings(first, "posting_status").value(1), "cleared");
    assert_eq!(strings(first, "account").value(1), "Assets:Bank");
    assert_eq!(strings(first, "commodity").value(1), "$");
    assert_eq!(amounts(first).value(0), 101 * UNIT / 10);
    assert_eq!(amounts(first).value(1), -101 * UNIT / 10);
    let inferred = first
        .column_by_name("amount_inferred")
        .expect("inferred")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("boolean");
    assert!(!inferred.value(0));
    assert!(inferred.value(1));
    assert_eq!(
        strings(first, "transaction_comment").value(1),
        "transaction comment\ncustomer-account: demo"
    );
    assert_eq!(
        strings(first, "posting_comment").value(0),
        "source: demo.csv:1"
    );
    assert_eq!(strings(first, "posting_comment").value(1), "bank comment");
    assert_eq!(strings(first, "tag_customer_account").value(1), "demo");
    assert_eq!(strings(first, "tag_source").value(0), "demo.csv:1");
    assert!(strings(first, "tag_source").is_null(1));
    assert!(strings(&batches[1], "tag_customer_account").is_null(0));
    assert_eq!(strings(&batches[1], "status").value(0), "pending");
    assert_eq!(strings(&batches[1], "posting_status").value(0), "cleared");
    assert_eq!(strings(&batches[1], "posting_status").value(1), "pending");
    assert_eq!(amounts(&batches[1]).value(0), UNIT / 5);
    let dates = first
        .column_by_name("date")
        .expect("date")
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("date type");
    assert_eq!(dates.value(0), dates.value(1));
    assert!(
        schema
            .fields()
            .iter()
            .all(|field| !field.name().contains("balance"))
    );
    let inferred_schema = FormatRegistry::get_provider("hledger")
        .expect("registered")
        .infer_schema(
            Box::pin(Cursor::new(JOURNAL.as_bytes().to_vec())),
            &Url::parse("hledger:///demo.journal")?,
        )
        .await?;
    assert_eq!(inferred_schema, schema);
    Ok(())
}

#[tokio::test]
async fn hledger_annotation_columns_inherit_and_remain_nullable() -> provider::Result<()> {
    let journal = "\
account Assets  ; parent-tag: not-inherited
account Assets:Bank  ; institution: demo
    ; region: local
2026-03-01 * Water service assessment
    ; party: party-0042
    ; billing-account: billing-0017
    ; service-connection: connection-0003
    ; responsibility: responsibility-0091
    ; cycle: 2026-03
    Assets:Bank  $125 ; receipt: receipt-0001, party: party-0042
    Income:Water Service  $-125
2026-03-02 Payment
    ; party: party-0043
    Assets:Bank  $1
    Income:Water Service  $-1
";
    let (schema, batches) = read(journal, "?batch_size=1").await?;
    assert_eq!(batches.len(), 4);
    assert!(schema.field_with_name("tag_parent_tag").is_err());
    for batch in &batches {
        assert_eq!(batch.schema(), schema);
    }
    let first = &batches[0];
    assert_eq!(strings(first, "tag_party").value(0), "party-0042");
    assert_eq!(
        strings(first, "tag_billing_account").value(0),
        "billing-0017"
    );
    assert_eq!(
        strings(first, "tag_service_connection").value(0),
        "connection-0003"
    );
    assert_eq!(
        strings(first, "tag_responsibility").value(0),
        "responsibility-0091"
    );
    assert_eq!(strings(first, "tag_cycle").value(0), "2026-03");
    assert_eq!(strings(first, "tag_receipt").value(0), "receipt-0001");
    assert_eq!(strings(first, "tag_institution").value(0), "demo");
    assert_eq!(strings(first, "tag_region").value(0), "local");
    assert!(strings(&batches[1], "tag_receipt").is_null(0));
    assert!(strings(&batches[1], "tag_institution").is_null(0));
    assert_eq!(strings(&batches[2], "tag_party").value(0), "party-0043");
    assert!(strings(&batches[2], "tag_cycle").is_null(0));
    let field = schema
        .field_with_name("tag_billing_account")
        .expect("annotation");
    assert!(field.is_nullable());
    assert_eq!(field.data_type(), &DataType::Utf8);
    assert_eq!(
        field.metadata().get("hledger_tag").map(String::as_str),
        Some("billing-account")
    );
    assert!(
        strings(first, "transaction_comment")
            .value(0)
            .contains("party: party-0042")
    );
    Ok(())
}

#[tokio::test]
async fn hledger_tag_syntax_matches_comment_annotation_rules() -> provider::Result<()> {
    let journal = "2026-01-01 Example ; free text project: water utility, flag:, url: https://example.test/a:b, due-date: 2026-02-01\n    A  $1 ; note: first, note: first, 9-key: value\n    B\n";
    let (_, batches) = read(journal, "").await?;
    let first = &batches[0];
    assert_eq!(strings(first, "tag_project").value(1), "water utility");
    assert_eq!(strings(first, "tag_flag").value(1), "");
    assert!(!strings(first, "tag_flag").is_null(1));
    assert_eq!(
        strings(first, "tag_url").value(1),
        "https://example.test/a:b"
    );
    assert_eq!(strings(first, "tag_due_date").value(1), "2026-02-01");
    assert_eq!(strings(first, "tag_note").value(0), "first");
    assert!(strings(first, "tag_note").is_null(1));
    assert_eq!(strings(first, "tag_9_key").value(0), "value");
    assert!(first.schema().field_with_name("tag_https").is_err());
    Ok(())
}

#[tokio::test]
async fn hledger_semicolons_in_account_names_are_not_annotations() -> provider::Result<()> {
    let (_, batches) = read(
        "account A;part  ; institution: demo\n2026-01-01 Example\n    A;part  $1\n    B ; literal\n",
        "",
    ).await?;
    let rows = &batches[0];
    assert_eq!(strings(rows, "account").value(0), "A;part");
    assert_eq!(strings(rows, "account").value(1), "B ; literal");
    assert_eq!(strings(rows, "tag_institution").value(0), "demo");
    assert!(strings(rows, "tag_institution").is_null(1));
    assert_eq!(amounts(rows).value(1), -UNIT);
    Ok(())
}

#[tokio::test]
async fn hledger_conflicting_annotations_and_column_names_fail() {
    for (journal, diagnostic) in [
        (
            "2026-01-01 Example ; party: one, party: two\n    A  $1\n    B\n",
            "conflicting values",
        ),
        (
            "2026-01-01 Example\n    ; party: one\n    ; party: two\n    A  $1\n    B\n",
            "conflicting values",
        ),
        (
            "2026-01-01 Example ; party: one\n    A  $1 ; party: two\n    B\n",
            "conflicting values",
        ),
        (
            "2026-01-01 Example\n    A  $1 ; party: one, party: two\n    B\n",
            "conflicting values",
        ),
        (
            "account A  ; party: one\n2026-01-01 Example ; party: two\n    A  $1\n    B\n",
            "conflicting values",
        ),
        (
            "account A  ; party: one\n2026-01-01 Example\n    A  $1 ; party: two\n    B\n",
            "conflicting values",
        ),
        (
            "2026-01-01 Example ; party-id: one, party_id: two\n    A  $1\n    B\n",
            "both map to column",
        ),
    ] {
        let error = read(journal, "").await.expect_err("must fail").to_string();
        assert!(error.contains(diagnostic), "{error}");
    }
}

#[tokio::test]
async fn hledger_posting_specific_annotations_can_differ_without_overrides() -> provider::Result<()>
{
    let (_, batches) = read(
        "2026-03-01 Payment ; party: payer\n    Cash  $125\n    Receivable  $-60 ; service-connection: one\n    Receivable  $-65 ; service-connection: two\n",
        "",
    ).await?;
    let rows = &batches[0];
    assert!(strings(rows, "tag_service_connection").is_null(0));
    assert_eq!(strings(rows, "tag_service_connection").value(1), "one");
    assert_eq!(strings(rows, "tag_service_connection").value(2), "two");
    assert_eq!(strings(rows, "tag_party").value(1), "payer");
    assert_eq!(strings(rows, "tag_party").value(2), "payer");
    Ok(())
}

#[tokio::test]
async fn hledger_exact_amounts_grouping_and_zero_inference() -> provider::Result<()> {
    for (amount, expected) in [
        ("$1,000.00", 1000 * UNIT),
        ("$0.000000000000000001", 1),
        ("$-0.25", -UNIT / 4),
        ("10 USD", 10 * UNIT),
        ("42", 42 * UNIT),
    ] {
        let text = format!("2026-01-01 Example\n    Assets:Cash  {amount}\n    Equity:Opening\n");
        let (_, batches) = read(&text, "").await?;
        assert_eq!(amounts(&batches[0]).value(0), expected);
        assert_eq!(amounts(&batches[0]).value(1), -expected);
    }
    let (_, batches) = read("2026-01-01 Zero\n    A  $1\n    B  $-1\n    C\n", "").await?;
    assert_eq!(amounts(&batches[0]).value(2), 0);
    assert_eq!(strings(&batches[0], "commodity").value(2), "$");
    Ok(())
}

#[tokio::test]
async fn hledger_balanced_commodities_and_ambiguous_inference() -> provider::Result<()> {
    let text = "2026-01-01 Example\n    A  1 USD\n    B  -1 USD\n    C  2 EUR\n    D  -2 EUR\n";
    let (_, batches) = read(text, "").await?;
    assert_eq!(batches[0].num_rows(), 4);
    for text in [
        "2026-01-01 Missing\n    A\n    B\n",
        "2026-01-01 Missing\n    A  $1\n    B\n    C\n",
        "2026-01-01 Mixed\n    A  1 USD\n    B  1 EUR\n    C\n",
        "2026-01-01 Unbalanced\n    A  $1\n    B  $-2\n",
        "2026-01-01 Conversion\n    A  1 EUR\n    B  -2 USD\n",
        "2026-01-01 No postings\n",
    ] {
        assert!(read(text, "").await.is_err(), "{text}");
    }
    Ok(())
}

#[tokio::test]
async fn hledger_unsupported_accounting_features_fail_explicitly() {
    for (text, feature) in [
        ("include sibling.journal\n", "include"),
        ("P 2026-01-01 EUR $2\n", "market price"),
        ("decimal-mark ,\n", "decimal-mark"),
        ("commodity $1,000.00\n", "commodity formatting"),
        ("= Expenses\n    (Budget)  $1\n", "automatic"),
        ("~ monthly\n    Expenses  $1\n    Assets\n", "periodic"),
        ("2026-01-01 Cost\n    A  1 EUR @ $2\n    B\n", "costs"),
        (
            "2026-01-01 Assertion\n    A  $1 = $1\n    B\n",
            "assertions",
        ),
        ("2026-01-01 Assignment\n    A  = $1\n    B\n", "assignments"),
        ("2026-01-01 Virtual\n    (Memo)  $1\n", "virtual"),
        (
            "2026-01-01 Virtual\n    [Memo]  $1\n    [Other]  $-1\n",
            "virtual",
        ),
        (
            "2026-01-01 Dates\n    A  $1 ; date: 2026-02-01\n    B\n",
            "date overrides",
        ),
    ] {
        let error = read(text, "").await.expect_err(feature).to_string();
        assert!(error.contains(feature), "{feature}: {error}");
        assert!(error.contains("/books/demo.journal"), "{error}");
    }
}

#[tokio::test]
async fn hledger_invalid_dates_numbers_and_options_fail_without_truncation() {
    for text in [
        "2026-02-30 Bad date\n    A  $1\n    B\n",
        "02/01 Missing year\n    A  $1\n    B\n",
        "Y999999999999999999999\n",
        "2026-01-01 Comma\n    A  $1,000\n    B\n",
        "2026-01-01 Grouping\n    A  $1,23.00\n    B\n",
        "2026-01-01 Decimal comma\n    A  $1.234,56\n    B\n",
        "2026-01-01 Decimal grouping\n    A  $1.23.45\n    B\n",
        "2026-01-01 Fraction grouping\n    A  $1.00,000\n    B\n",
        "2026-01-01 Precision\n    A  $0.0000000000000000001\n    B\n",
        "2026-01-01 Overflow\n    A  $999999999999999999999\n    B\n",
        "2026-01-01 Bad posting\n    A  rubbish\n    B\n",
        "2026-01-01 Tabs\n    A\t\t$1\n    B  $-1\n",
        "comment\n2026-01-01 Hidden\n    A  $1\n    B\n",
    ] {
        assert!(read(text, "").await.is_err(), "{text}");
    }
    for query in ["?batch_size=0", "?batch_size=bad", "?unknown=true"] {
        assert!(read(JOURNAL, query).await.is_err(), "{query}");
    }
}

#[tokio::test]
async fn hledger_standalone_comments_and_original_error_lines() -> provider::Result<()> {
    let (_, batches) = read(
        "2026-01-01 Example\n    ; transaction note\n    A  $1\n    ; posting note\n    B  ; inferred note\n",
        "",
    ).await?;
    assert_eq!(batches[0].num_rows(), 2);
    assert_eq!(strings(&batches[0], "account").value(1), "B");
    assert_eq!(
        strings(&batches[0], "transaction_comment").value(0),
        "transaction note"
    );
    assert_eq!(
        strings(&batches[0], "posting_comment").value(0),
        "posting note"
    );
    assert_eq!(
        strings(&batches[0], "posting_comment").value(1),
        "inferred note"
    );
    let error = read(
        "; removed line\n2026-01-01 Café\n    A  $1\n    B\n\n; another removed line\nunknown directive\n",
        "",
    ).await.expect_err("unknown directive").to_string();
    assert!(error.contains("line 7:"), "{error}");
    Ok(())
}

#[tokio::test]
async fn hledger_empty_and_comment_only_files_have_stable_schema() -> provider::Result<()> {
    for text in [
        "",
        "; Nothing yet\n",
        "account Assets:Bank\n",
        "comment\n2026-01-01 not a transaction\nend comment\n",
    ] {
        let (schema, batches) = read(text, "").await?;
        assert_eq!(schema.fields().len(), 14);
        assert!(batches.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn hledger_host_datafusion_financial_queries_without_pond() -> provider::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("demo.journal");
    std::fs::write(&path, JOURNAL)?;
    let table = provider::create_memtable_from_host_url(&Url::parse(&format!(
        "host+hledger://{}",
        path.display()
    ))?)
    .await?;
    let ctx = SessionContext::new();
    _ = ctx.register_table("postings", table)?;
    let result = ctx.sql("SELECT sum(amount) AS total FROM postings WHERE account LIKE 'Expenses:%' GROUP BY commodity").await?.collect().await?;
    assert_eq!(
        result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("exact total")
            .value(0),
        103 * UNIT / 10
    );
    let result = ctx.sql("SELECT source, transaction_sequence, count(*) AS n FROM postings GROUP BY source, transaction_sequence ORDER BY transaction_sequence").await?.collect().await?;
    assert_eq!(result[0].num_rows(), 2);
    assert_eq!(
        result[0]
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count")
            .value(0),
        2
    );
    Ok(())
}

#[tokio::test]
async fn hledger_cached_and_uncached_pond_globs_merge_annotation_columns() -> provider::Result<()> {
    let persistence: Arc<dyn tinyfs::PersistenceLayer> =
        Arc::new(tinyfs::MemoryPersistence::default());
    let fs = Arc::new(tinyfs::FS::from_arc(persistence.clone()));
    let root = fs.root().await?;
    root.write_file_path_from_slice("/first.journal", JOURNAL.as_bytes())
        .await?;
    root.write_file_path_from_slice(
        "/second.journal",
        b"2026-03-01 Example ; cycle: 2026-03, party: other\n    Expenses:Office  $0.003\n    Assets:Bank\n",
    )
    .await?;
    let cache = tempfile::tempdir()?;
    for use_cache in [false, true] {
        let ctx = Arc::new(SessionContext::new());
        let mut context = tinyfs::ProviderContext::new(ctx.clone(), persistence.clone());
        if use_cache {
            context = context.with_cache_dir(cache.path().to_path_buf());
        }
        let provider = provider::Provider::with_context(fs.clone(), Arc::new(context));
        for _ in 0..2 {
            let table = provider
                .create_provider_for_url("hledger:///*.journal", &ctx)
                .await?;
            _ = ctx.register_table("postings", table)?;
            let result = ctx.sql("SELECT sum(amount) AS total, count(DISTINCT source) AS files FROM postings WHERE account LIKE 'Expenses:%'").await?.collect().await?;
            assert_eq!(
                result[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .expect("exact total")
                    .value(0),
                10_303 * UNIT / 1000
            );
            assert_eq!(
                result[0]
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("file count")
                    .value(0),
                2
            );
            let result = ctx.sql("SELECT count(*) AS n FROM postings WHERE tag_cycle = '2026-03' AND tag_customer_account IS NULL AND tag_party = 'other'").await?.collect().await?;
            assert_eq!(
                result[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("count")
                    .value(0),
                2
            );
            let result = ctx.sql("SELECT count(*) AS n FROM postings WHERE tag_customer_account = 'demo' AND tag_cycle IS NULL").await?.collect().await?;
            assert_eq!(
                result[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("count")
                    .value(0),
                2
            );
            assert!(ctx.deregister_table("postings")?.is_some());
        }
    }
    Ok(())
}

#[tokio::test]
async fn hledger_cross_file_annotation_column_collisions_fail() -> provider::Result<()> {
    let persistence: Arc<dyn tinyfs::PersistenceLayer> =
        Arc::new(tinyfs::MemoryPersistence::default());
    let fs = Arc::new(tinyfs::FS::from_arc(persistence.clone()));
    let root = fs.root().await?;
    root.write_file_path_from_slice(
        "/first.journal",
        b"2026-01-01 Example ; party-id: first\n    A  $1\n    B\n",
    )
    .await?;
    root.write_file_path_from_slice(
        "/second.journal",
        b"2026-01-01 Example ; party_id: second\n    A  $1\n    B\n",
    )
    .await?;
    let cache = tempfile::tempdir()?;
    for use_cache in [false, true] {
        let ctx = Arc::new(SessionContext::new());
        let mut context = tinyfs::ProviderContext::new(ctx.clone(), persistence.clone());
        if use_cache {
            context = context.with_cache_dir(cache.path().to_path_buf());
        }
        let provider = provider::Provider::with_context(fs.clone(), Arc::new(context));
        let result = provider
            .create_provider_for_url("hledger:///*.journal", &ctx)
            .await;
        assert!(
            result.is_err(),
            "column collision must fail (cache={use_cache})"
        );
    }
    Ok(())
}
