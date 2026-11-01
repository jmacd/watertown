// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Single-file journal postings for financial queries, without running balances.

use super::batch::{BatchStream, finish_batch};
use crate::{Error, FormatProvider, Result, Url};
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, StringArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::NaiveDate;
use hledger_parser::{Commodity, Directive, Status, Transaction};
use regex::Regex;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt};

// A fixed scale keeps independently parsed journals union-compatible. Every
// conversion is exact; values needing more fractional digits are rejected.
const PRECISION: u8 = 38;
const SCALE: u32 = 18;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(feature = "documentation", derive(schemars::JsonSchema))]
pub(crate) struct Options {
    batch_size: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self { batch_size: 8192 }
    }
}

#[derive(Default)]
pub struct HledgerProvider;

impl HledgerProvider {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl FormatProvider for HledgerProvider {
    fn name(&self) -> &str {
        "hledger"
    }

    async fn infer_schema(
        &self,
        reader: Pin<Box<dyn AsyncRead + Send>>,
        url: &Url,
    ) -> Result<SchemaRef> {
        let (schema, _) = self.open_stream(reader, url).await?;
        Ok(schema)
    }

    async fn open_stream(
        &self,
        mut reader: Pin<Box<dyn AsyncRead + Send>>,
        url: &Url,
    ) -> Result<(SchemaRef, BatchStream)> {
        let options: Options = url.query_params()?;
        if options.batch_size == 0 {
            return Err(Error::InvalidUrl(
                "hledger batch_size must be positive".into(),
            ));
        }
        let mut text = String::new();
        _ = reader.read_to_string(&mut text).await?;
        let source = url.path().to_string();
        let parse_source = source.clone();
        let rows = tokio::task::spawn_blocking(move || {
            // Constructing the upstream Chumsky parser can exhaust Tokio's
            // default worker stack, even on empty input in debug builds.
            stacker::grow(16 * 1024 * 1024, || parse_journal(&text))
                .map_err(|e| Error::Hledger(format!("{parse_source}: {e}")))
        })
        .await
        .map_err(|e| Error::Hledger(format!("{source}: journal parsing task failed: {e}")))??;
        let schema = posting_schema(&rows).map_err(|e| Error::Hledger(format!("{source}: {e}")))?;
        let stream_schema = schema.clone();
        let stream = async_stream::try_stream! {
            for chunk in rows.chunks(options.batch_size) {
                yield rows_to_batch(stream_schema.clone(), &source, chunk)?;
            }
        };
        Ok((schema, Box::pin(stream)))
    }
}

fn posting_schema(rows: &[PostingRow]) -> Result<SchemaRef> {
    let mut fields = vec![
        Field::new("source", DataType::Utf8, false),
        Field::new("transaction_sequence", DataType::UInt64, false),
        Field::new("date", DataType::Date32, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("posting_status", DataType::Utf8, false),
        Field::new("code", DataType::Utf8, true),
        Field::new("payee", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("account", DataType::Utf8, false),
        Field::new("commodity", DataType::Utf8, false),
        Field::new(
            "amount",
            DataType::Decimal128(PRECISION, SCALE as i8),
            false,
        ),
        Field::new("amount_inferred", DataType::Boolean, false),
        Field::new("transaction_comment", DataType::Utf8, true),
        Field::new("posting_comment", DataType::Utf8, true),
    ];
    let mut columns = BTreeMap::<String, String>::new();
    for row in rows {
        for key in row.tags.keys() {
            let column = format!(
                "tag_{}",
                key.chars()
                    .map(|c| {
                        if c.is_ascii_alphanumeric() || c == '_' {
                            c
                        } else {
                            '_'
                        }
                    })
                    .collect::<String>()
            );
            if let Some(previous) = columns.insert(column.clone(), key.clone())
                && previous != *key
            {
                return Err(Error::Hledger(format!(
                    "annotation keys '{previous}' and '{key}' both map to column '{column}'"
                )));
            }
        }
    }
    fields.extend(columns.into_iter().map(|(column, key)| {
        Field::new(column, DataType::Utf8, true).with_metadata(std::collections::HashMap::from([(
            "hledger_tag".to_string(),
            key,
        )]))
    }));
    Ok(Arc::new(Schema::new(fields)))
}

type Tags = BTreeMap<String, String>;

struct PostingRow {
    transaction_sequence: u64,
    date: i32,
    status: &'static str,
    posting_status: &'static str,
    code: Option<String>,
    payee: String,
    description: Option<String>,
    account: String,
    commodity: String,
    amount: i128,
    amount_inferred: bool,
    transaction_comment: Option<String>,
    posting_comment: Option<String>,
    tags: Tags,
}

#[derive(Default)]
struct SourceTransaction {
    line: usize,
    comments: Vec<String>,
    posting_comments: Vec<Vec<String>>,
}

struct InspectedJournal {
    transactions: Vec<SourceTransaction>,
    accounts: BTreeMap<String, Vec<String>>,
    text: String,
    source_lines: Vec<usize>,
}

fn parse_journal(text: &str) -> Result<Vec<PostingRow>> {
    let inspected = inspect_source(text)?;
    let directives = hledger_parser::parse(&inspected.text).map_err(|errors| {
        Error::Hledger(
            errors
                .into_iter()
                .map(|error| {
                    let parsed_line = inspected
                        .text
                        .bytes()
                        .take(error.span.start)
                        .filter(|c| *c == b'\n')
                        .count();
                    let line = inspected
                        .source_lines
                        .get(parsed_line)
                        .copied()
                        .unwrap_or_else(|| text.lines().count().max(1));
                    format!("line {line}: {}", error.message)
                })
                .collect::<Vec<_>>()
                .join("; "),
        )
    })?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
        .ok_or_else(|| Error::Hledger("cannot construct date epoch".into()))?;
    let account_tags = inspected
        .accounts
        .into_iter()
        .map(|(name, comments)| {
            parse_tags(&comments)
                .map(|tags| (name.clone(), tags))
                .map_err(|e| Error::Hledger(format!("account '{name}': {e}")))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let mut rows = Vec::new();
    let mut sources = inspected.transactions.into_iter().enumerate();
    for directive in directives {
        match directive {
            Directive::Transaction(tx) => {
                let (index, source) = sources.next().ok_or_else(|| {
                    Error::Hledger("cannot correlate parsed transaction with source".into())
                })?;
                if source.posting_comments.len() != tx.postings.len() {
                    return Err(Error::Hledger(format!(
                        "line {}: cannot correlate {} source postings with {} parsed postings",
                        source.line,
                        source.posting_comments.len(),
                        tx.postings.len()
                    )));
                }
                let amounts = resolve_amounts(&tx).map_err(|e| {
                    Error::Hledger(format!(
                        "transaction {}, line {}: {e}",
                        index + 1,
                        source.line
                    ))
                })?;
                let date = i32::try_from(tx.date.signed_duration_since(epoch).num_days())
                    .map_err(|e| Error::Hledger(format!("date out of Arrow range: {e}")))?;
                let transaction_tags = parse_tags(&source.comments).map_err(|e| {
                    Error::Hledger(format!(
                        "transaction {}, line {}: {e}",
                        index + 1,
                        source.line
                    ))
                })?;
                let transaction_comment = join_comments(source.comments);
                for ((posting, amount), comments) in
                    tx.postings.iter().zip(amounts).zip(source.posting_comments)
                {
                    let account = posting.account_name.join(":");
                    let tags = (|| {
                        let mut tags = transaction_tags.clone();
                        if let Some(inherited) = account_tags.get(&account) {
                            merge_tags(&mut tags, inherited)?;
                        }
                        merge_tags(&mut tags, &parse_tags(&comments)?)?;
                        Ok::<_, Error>(tags)
                    })()
                    .map_err(|e| {
                        Error::Hledger(format!(
                            "transaction {}, line {}, posting '{}': {e}",
                            index + 1,
                            source.line,
                            account
                        ))
                    })?;
                    rows.push(PostingRow {
                        transaction_sequence: (index + 1) as u64,
                        date,
                        status: status(tx.status.as_ref()),
                        posting_status: status(posting.status.as_ref().or(tx.status.as_ref())),
                        code: tx.code.clone(),
                        payee: tx.payee.clone(),
                        description: tx.description.clone(),
                        account,
                        commodity: amount.commodity,
                        amount: amount.value,
                        amount_inferred: amount.inferred,
                        transaction_comment: transaction_comment.clone(),
                        posting_comment: join_comments(comments),
                        tags,
                    });
                }
            }
            Directive::Account(_)
            | Directive::Payee(_)
            | Directive::Tag(_)
            | Directive::Year(_)
            | Directive::Commodity(Commodity::Commodity(_)) => {}
            Directive::Include(_) => {
                return unsupported(
                    "include directives; assemble files through factory configuration",
                );
            }
            Directive::AutoPostings(_) => return unsupported("automatic posting rules"),
            Directive::PeriodicTransaction(_) => return unsupported("periodic transaction rules"),
            Directive::Price(_) => return unsupported("market price directives"),
            Directive::DecimalMark(_) => return unsupported("decimal-mark directives"),
            Directive::Commodity(Commodity::Amount(_)) => {
                return unsupported("commodity formatting directives");
            }
        }
    }
    if sources.next().is_some() {
        return Err(Error::Hledger("source transaction was not parsed".into()));
    }
    Ok(rows)
}

fn unsupported<T>(feature: &str) -> Result<T> {
    Err(Error::Hledger(format!("unsupported {feature}")))
}

fn status(value: Option<&Status>) -> &'static str {
    match value {
        Some(Status::Cleared) => "cleared",
        Some(Status::Pending) => "pending",
        None => "unmarked",
    }
}

fn join_comments(comments: Vec<String>) -> Option<String> {
    if comments.is_empty() {
        None
    } else {
        Some(comments.join("\n"))
    }
}

fn parse_tags(comments: &[String]) -> Result<Tags> {
    let mut tags = Tags::new();
    for comment in comments {
        let mut rest = comment.as_str();
        while let Some((prefix, after_colon)) = rest.split_once(':') {
            // hledger takes the last whitespace-separated word before a colon,
            // and the value up to the next comma or end of the comment line.
            let key = prefix.rsplit(char::is_whitespace).next().unwrap_or("");
            if key.is_empty() {
                rest = after_colon;
                continue;
            }
            if matches!(key, "date" | "date2") {
                return unsupported("comment-based date overrides");
            }
            let (value, next) = after_colon.split_once(',').unwrap_or((after_colon, ""));
            insert_tag(&mut tags, key, value.trim())?;
            rest = next;
        }
    }
    Ok(tags)
}

fn insert_tag(tags: &mut Tags, key: &str, value: &str) -> Result<()> {
    if let Some(previous) = tags.get(key) {
        if previous != value {
            return Err(Error::Hledger(format!(
                "conflicting values for annotation '{key}': '{previous}' versus '{value}'"
            )));
        }
    } else {
        _ = tags.insert(key.to_string(), value.to_string());
    }
    Ok(())
}

fn merge_tags(tags: &mut Tags, additional: &Tags) -> Result<()> {
    for (key, value) in additional {
        insert_tag(tags, key, value)?;
    }
    Ok(())
}

struct ResolvedAmount {
    commodity: String,
    value: i128,
    inferred: bool,
}

fn resolve_amounts(tx: &Transaction) -> Result<Vec<ResolvedAmount>> {
    if tx.postings.is_empty() {
        return Err(Error::Hledger("transaction has no postings".into()));
    }
    let mut sums = BTreeMap::<String, i128>::new();
    let mut amounts = Vec::with_capacity(tx.postings.len());
    let mut missing = None;
    for (index, posting) in tx.postings.iter().enumerate() {
        let account = posting.account_name.join(":");
        if posting.is_virtual || (account.starts_with('[') && account.ends_with(']')) {
            return unsupported("virtual postings");
        }
        if posting.price.is_some() {
            return unsupported("posting costs/conversion prices");
        }
        if posting.assertion.is_some() {
            return unsupported("balance assertions and assignments");
        }
        match &posting.amount {
            Some(amount) => {
                let quantity = amount.quantity.normalize();
                if quantity.scale() > SCALE {
                    return Err(Error::Hledger(format!(
                        "amount requires more than {SCALE} fractional digits"
                    )));
                }
                let value = quantity
                    .mantissa()
                    .checked_mul(10_i128.pow(SCALE - quantity.scale()))
                    .ok_or_else(|| Error::Hledger("amount overflow".into()))?;
                let sum = sums.entry(amount.commodity.clone()).or_default();
                *sum = sum
                    .checked_add(value)
                    .ok_or_else(|| Error::Hledger("transaction amount sum overflow".into()))?;
                amounts.push(Some(ResolvedAmount {
                    commodity: amount.commodity.clone(),
                    value,
                    inferred: false,
                }));
            }
            None => {
                if missing.replace(index).is_some() {
                    return Err(Error::Hledger(
                        "more than one omitted posting amount".into(),
                    ));
                }
                amounts.push(None);
            }
        }
    }
    let residuals: Vec<_> = sums.iter().filter(|(_, value)| **value != 0).collect();
    if let Some(index) = missing {
        let (commodity, sum) = if residuals.len() == 1 {
            residuals[0]
        } else if residuals.is_empty() && sums.len() == 1 {
            sums.iter()
                .next()
                .ok_or_else(|| Error::Hledger("missing commodity".into()))?
        } else {
            return Err(Error::Hledger(
                "omitted amount cannot be inferred as one unambiguous commodity".into(),
            ));
        };
        let value = sum
            .checked_neg()
            .ok_or_else(|| Error::Hledger("inferred amount overflow".into()))?;
        if value.unsigned_abs() >= 10_u128.pow(u32::from(PRECISION)) {
            return Err(Error::Hledger(
                "inferred amount exceeds decimal precision".into(),
            ));
        }
        amounts[index] = Some(ResolvedAmount {
            commodity: commodity.clone(),
            value,
            inferred: true,
        });
    } else if !residuals.is_empty() {
        return Err(Error::Hledger(
            "transaction does not balance per commodity; implicit conversions are unsupported"
                .into(),
        ));
    }
    amounts
        .into_iter()
        .map(|amount| amount.ok_or_else(|| Error::Hledger("unresolved posting amount".into())))
        .collect()
}

// The upstream parser can interpret standalone comments as postings and panic
// on invalid dates. Preserve comments separately and validate before parsing.
fn inspect_source(text: &str) -> Result<InspectedJournal> {
    let numbers = Regex::new(r"[0-9]*[.,]?[0-9][0-9.,]*")
        .map_err(|e| Error::Hledger(format!("cannot build number validator: {e}")))?;
    let posting_dates = Regex::new(r"\[[0-9]")
        .map_err(|e| Error::Hledger(format!("cannot build posting date validator: {e}")))?;
    let mut sources: Vec<SourceTransaction> = Vec::new();
    let mut accounts = BTreeMap::<String, Vec<String>>::new();
    let mut current_account = None;
    let mut parser_text = String::new();
    let mut source_lines = Vec::new();
    let mut current = None;
    let mut year = None;
    let mut block_comment = false;
    for (index, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if block_comment {
            if trimmed == "end comment" {
                block_comment = false;
            }
            continue;
        }
        if !line.starts_with([' ', '\t']) && trimmed == "comment" {
            block_comment = true;
            continue;
        }
        let indented = line.starts_with([' ', '\t']);
        if trimmed.is_empty() || (!indented && trimmed.starts_with(['#', '*', '%'])) {
            continue;
        }
        let start = line.trim_start();
        let account_body = if !indented && start.split_whitespace().next() == Some("account") {
            Some(start["account".len()..].trim_start())
        } else if indented && current.is_some() && !start.starts_with(';') {
            Some(
                start
                    .strip_prefix('*')
                    .or_else(|| start.strip_prefix('!'))
                    .filter(|rest| rest.starts_with(char::is_whitespace))
                    .map_or(start, str::trim_start),
            )
        } else {
            None
        };
        let (source_content, comment) = split_source_comment(line, account_body);
        let content = source_content.trim();
        let comment = comment.map(str::trim);
        if comment.is_some_and(|comment| posting_dates.is_match(comment)) {
            return unsupported("comment-based date overrides");
        }
        if !content.trim().is_empty() {
            parser_text.push_str(source_content.trim_end());
            parser_text.push('\n');
            source_lines.push(index + 1);
        }
        if !indented {
            if trimmed.starts_with(';') {
                continue;
            }
            current = None;
            current_account = None;
            if content.starts_with('=') {
                return unsupported("automatic posting rules");
            }
            if content.starts_with('~') {
                return unsupported("periodic transaction rules");
            }
            if content.split_whitespace().next() == Some("P") {
                return unsupported("market price directives");
            }
            if content.split_whitespace().next() == Some("account") {
                let name = content["account".len()..]
                    .trim()
                    .split(':')
                    .map(str::trim)
                    .collect::<Vec<_>>()
                    .join(":");
                accounts
                    .entry(name.clone())
                    .or_default()
                    .extend(comment.into_iter().map(String::from));
                current_account = Some(name);
            } else if let Some(value) = content
                .strip_prefix('Y')
                .or_else(|| content.strip_prefix("year "))
            {
                let value = value.trim();
                if value.len() != 4 || !value.bytes().all(|c| c.is_ascii_digit()) {
                    return Err(Error::Hledger(format!(
                        "line {}: year must contain four digits",
                        index + 1
                    )));
                }
                year = Some(
                    value
                        .parse::<i32>()
                        .map_err(|e| Error::Hledger(format!("invalid year: {e}")))?,
                );
            } else if content.starts_with(|c: char| c.is_ascii_digit()) {
                let token = content
                    .split_whitespace()
                    .next()
                    .ok_or_else(|| Error::Hledger("missing transaction date".into()))?;
                validate_date(token, year)
                    .map_err(|e| Error::Hledger(format!("line {}: {e}", index + 1)))?;
                sources.push(SourceTransaction {
                    line: index + 1,
                    comments: comment.into_iter().map(String::from).collect(),
                    posting_comments: Vec::new(),
                });
                current = Some(sources.len() - 1);
            }
        } else if let Some(current) = current {
            let source = &mut sources[current];
            if !content.trim().is_empty() {
                if content.contains('\t') {
                    return unsupported("tab-separated posting fields; use two spaces");
                }
                if let Some((_, amount)) = content.split_once("  ") {
                    if amount.contains(['{', '}']) {
                        return unsupported("lot cost annotations");
                    }
                    for number in numbers.find_iter(amount) {
                        let number = number.as_str();
                        if number.contains(',') && !number.contains('.') {
                            return Err(Error::Hledger(format!(
                                "line {}: comma-only amounts are ambiguous to hledger_parser; use 1000 or 1,000.00, not 1,000",
                                index + 1
                            )));
                        }
                        if number.matches('.').count() > 1
                            || number.split_once('.').is_some_and(|(_, fraction)| {
                                !fraction.bytes().all(|c| c.is_ascii_digit())
                            })
                        {
                            return Err(Error::Hledger(format!(
                                "line {}: use one decimal point with no fractional grouping",
                                index + 1
                            )));
                        }
                        let integral = number.split('.').next().unwrap_or(number);
                        if integral.contains(',') {
                            let groups: Vec<_> = integral.split(',').collect();
                            if groups[0].is_empty()
                                || groups[0].len() > 3
                                || groups[1..].iter().any(|group| group.len() != 3)
                            {
                                return Err(Error::Hledger(format!(
                                    "line {}: invalid thousands grouping",
                                    index + 1
                                )));
                            }
                        }
                        if let Some((_, fraction)) = number.split_once('.')
                            && fraction.len() > SCALE as usize
                        {
                            return Err(Error::Hledger(format!(
                                "line {}: at most {SCALE} fractional digits are supported",
                                index + 1
                            )));
                        }
                    }
                }
                source.posting_comments.push(Vec::new());
            }
            if let Some(comment) = comment {
                let comments = source
                    .posting_comments
                    .last_mut()
                    .unwrap_or(&mut source.comments);
                comments.push(comment.to_string());
            }
        } else if let Some(name) = &current_account
            && let Some(comment) = comment
        {
            accounts
                .entry(name.clone())
                .or_default()
                .push(comment.to_string());
        }
    }
    if block_comment {
        return Err(Error::Hledger("unterminated block comment".into()));
    }
    Ok(InspectedJournal {
        transactions: sources,
        accounts,
        text: parser_text,
        source_lines,
    })
}

fn split_source_comment<'a>(
    line: &'a str,
    account_body: Option<&'a str>,
) -> (&'a str, Option<&'a str>) {
    // Semicolons belong to account names until a two-space field separator.
    let offset = if let Some(body) = account_body {
        let Some((_, fields)) = body.split_once("  ") else {
            return (line, None);
        };
        line.len() - fields.len()
    } else {
        0
    };
    if let Some((before, comment)) = line[offset..].split_once(';') {
        (&line[..offset + before.len()], Some(comment))
    } else {
        (line, None)
    }
}

fn validate_date(token: &str, year: Option<i32>) -> Result<()> {
    let parts: Vec<_> = token.split(['/', '-', '.']).collect();
    let (year, month, day) = match parts.as_slice() {
        [year, month, day] if year.len() == 4 => (year.parse::<i32>().ok(), *month, *day),
        [month, day] => (year, *month, *day),
        _ => return Err(Error::Hledger("unsupported transaction date".into())),
    };
    let year = year.ok_or_else(|| {
        Error::Hledger("dates must have a four-digit year or a preceding Y/year directive".into())
    })?;
    let date = month
        .parse::<u32>()
        .ok()
        .zip(day.parse::<u32>().ok())
        .and_then(|(month, day)| NaiveDate::from_ymd_opt(year, month, day));
    if date.is_none() {
        return Err(Error::Hledger("invalid transaction date".into()));
    }
    Ok(())
}

fn rows_to_batch(schema: SchemaRef, source: &str, rows: &[PostingRow]) -> Result<RecordBatch> {
    let amounts = Decimal128Array::from_iter_values(rows.iter().map(|r| r.amount))
        .with_precision_and_scale(PRECISION, SCALE as i8)
        .map_err(|e| Error::Arrow(e.to_string()))?;
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            source,
            rows.len(),
        ))),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|r| r.transaction_sequence),
        )),
        Arc::new(Date32Array::from_iter_values(rows.iter().map(|r| r.date))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.status))),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| r.posting_status),
        )),
        Arc::new(StringArray::from_iter(
            rows.iter().map(|r| r.code.as_deref()),
        )),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| &r.payee))),
        Arc::new(StringArray::from_iter(
            rows.iter().map(|r| r.description.as_deref()),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| &r.account),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| &r.commodity),
        )),
        Arc::new(amounts),
        Arc::new(BooleanArray::from_iter(
            rows.iter().map(|r| Some(r.amount_inferred)),
        )),
        Arc::new(StringArray::from_iter(
            rows.iter().map(|r| r.transaction_comment.as_deref()),
        )),
        Arc::new(StringArray::from_iter(
            rows.iter().map(|r| r.posting_comment.as_deref()),
        )),
    ];
    for field in schema.fields().iter().skip(columns.len()) {
        let key = field.metadata().get("hledger_tag").ok_or_else(|| {
            Error::Hledger(format!(
                "annotation column '{}' has no source key",
                field.name()
            ))
        })?;
        columns.push(Arc::new(StringArray::from_iter(
            rows.iter().map(|row| row.tags.get(key).map(String::as_str)),
        )));
    }
    finish_batch(schema, columns)
}

crate::register_format_provider!(scheme: "hledger", provider: HledgerProvider::new);
