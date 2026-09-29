// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Typed same-scope composition with explicit overlap semantics.

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::common::{Column, ScalarValue};
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result};
use datafusion::functions_aggregate::expr_fn::{count, max, min};
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::{Expr, col, lit};
use datafusion::prelude::ExprFunctionExt;

use crate::overlap::{OverlapPolicy, RowIdentity};
use crate::statistics::TimeInterval;

const SOURCE_COLUMN: &str = "__watertown_combine_source";
const SEQUENCE_COLUMN: &str = "__watertown_combine_sequence";
const ROW_NUMBER_COLUMN: &str = "__watertown_combine_row_number";
const COUNT_COLUMN: &str = "__watertown_combine_count";
const FIRST_SOURCE_COLUMN: &str = "__watertown_combine_first_source";
const LAST_SOURCE_COLUMN: &str = "__watertown_combine_last_source";

/// One logical source participating in same-scope composition.
pub struct CombineInput {
    source: Arc<str>,
    sequence: u64,
    frame: DataFrame,
    event_time_ranges: Option<Arc<[TimeInterval]>>,
}

impl CombineInput {
    /// Construct an input with a stable source identity and precedence sequence.
    #[must_use]
    pub fn new(source: impl Into<Arc<str>>, sequence: u64, frame: DataFrame) -> Self {
        Self {
            source: source.into(),
            sequence,
            frame,
            event_time_ranges: None,
        }
    }

    /// Declare complete event-time ranges for disjointness validation.
    #[must_use]
    pub fn with_event_time_ranges(mut self, ranges: Vec<TimeInterval>) -> Self {
        self.event_time_ranges = Some(ranges.into());
        self
    }
}

/// Compose same-scope inputs using only the work required by `policy`.
///
/// Key policies execute only key columns to validate conflicts before
/// returning the schema-aligned output plan.
pub async fn combine_same_scope(
    inputs: Vec<CombineInput>,
    policy: &OverlapPolicy,
) -> Result<DataFrame> {
    validate_inputs(&inputs, policy)?;
    if matches!(policy, OverlapPolicy::RequireDisjoint) {
        validate_disjoint_ranges(&inputs)?;
    }

    let key = match policy {
        OverlapPolicy::RejectDuplicateKey { key } | OverlapPolicy::PreferBySequence { key } => {
            Some(key)
        }
        OverlapPolicy::PreserveAll | OverlapPolicy::RequireDisjoint => None,
    };
    if let Some(key) = key {
        validate_key_inputs(&inputs, key)?;
    }

    let output_columns = union_output_columns(&inputs);
    let union = union_inputs(inputs)?;
    match policy {
        OverlapPolicy::PreserveAll | OverlapPolicy::RequireDisjoint => {
            select_output_columns(union, &output_columns)
        }
        OverlapPolicy::RejectDuplicateKey { key } => {
            reject_duplicate_keys(union.clone(), key).await?;
            select_output_columns(union, &output_columns)
        }
        OverlapPolicy::PreferBySequence { key } => {
            reject_ambiguous_precedence(union.clone(), key).await?;
            prefer_by_sequence(union, key, &output_columns)
        }
    }
}

fn validate_inputs(inputs: &[CombineInput], policy: &OverlapPolicy) -> Result<()> {
    if inputs.is_empty() {
        return Err(DataFusionError::Plan(
            "same-scope combine requires at least one input".to_owned(),
        ));
    }
    let mut sources = BTreeSet::new();
    let mut sequences = BTreeSet::new();
    for input in inputs {
        if input.source.is_empty() {
            return Err(DataFusionError::Plan(
                "same-scope combine source identity must not be empty".to_owned(),
            ));
        }
        if !sources.insert(input.source.as_ref()) {
            return Err(DataFusionError::Plan(format!(
                "duplicate same-scope source identity '{}'",
                input.source
            )));
        }
        if !sequences.insert(input.sequence) {
            return Err(DataFusionError::Plan(format!(
                "duplicate same-scope source sequence {}",
                input.sequence
            )));
        }
        for reserved in [
            SOURCE_COLUMN,
            SEQUENCE_COLUMN,
            ROW_NUMBER_COLUMN,
            COUNT_COLUMN,
            FIRST_SOURCE_COLUMN,
            LAST_SOURCE_COLUMN,
        ] {
            if input
                .frame
                .schema()
                .field_with_unqualified_name(reserved)
                .is_ok()
            {
                return Err(DataFusionError::Plan(format!(
                    "same-scope input '{}' contains reserved column '{reserved}'",
                    input.source
                )));
            }
        }
    }
    if matches!(policy, OverlapPolicy::RequireDisjoint)
        && inputs.iter().any(|input| input.event_time_ranges.is_none())
    {
        return Err(DataFusionError::Plan(
            "RequireDisjoint combine requires complete event-time ranges for every source"
                .to_owned(),
        ));
    }
    Ok(())
}

fn validate_key_inputs(inputs: &[CombineInput], key: &RowIdentity) -> Result<()> {
    for column in key.columns() {
        let expected = inputs[0]
            .frame
            .schema()
            .field_with_unqualified_name(column)
            .map_err(|_| {
                DataFusionError::Plan(format!(
                    "same-scope source '{}' is missing row identity column '{column}'",
                    inputs[0].source
                ))
            })?
            .data_type()
            .clone();
        for input in &inputs[1..] {
            let actual = input
                .frame
                .schema()
                .field_with_unqualified_name(column)
                .map_err(|_| {
                    DataFusionError::Plan(format!(
                        "same-scope source '{}' is missing row identity column '{column}'",
                        input.source
                    ))
                })?
                .data_type();
            if actual != &expected {
                return Err(DataFusionError::Plan(format!(
                    "same-scope source '{}' row identity column '{column}' has type {actual}, expected {expected}",
                    input.source
                )));
            }
        }
    }
    Ok(())
}

fn validate_disjoint_ranges(inputs: &[CombineInput]) -> Result<()> {
    let mut ranges = inputs
        .iter()
        .flat_map(|input| {
            input
                .event_time_ranges
                .as_deref()
                .unwrap_or_default()
                .iter()
                .copied()
                .map(|range| (Arc::clone(&input.source), range))
        })
        .collect::<Vec<_>>();
    ranges.sort_by_key(|(_, range)| (range.min(), range.max()));
    let Some((mut prior_source, mut prior)) = ranges.first().cloned() else {
        return Ok(());
    };
    for (source, range) in ranges.into_iter().skip(1) {
        if range.min() <= prior.max() {
            return Err(DataFusionError::Plan(format!(
                "RequireDisjoint combine found overlapping sources '{prior_source}' [{}..={}] and '{source}' [{}..={}]",
                prior.min(),
                prior.max(),
                range.min(),
                range.max()
            )));
        }
        prior_source = source;
        prior = range;
    }
    Ok(())
}

fn union_output_columns(inputs: &[CombineInput]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    inputs
        .iter()
        .flat_map(|input| input.frame.schema().fields())
        .filter(|field| seen.insert(field.name().to_owned()))
        .map(|field| field.name().to_owned())
        .collect()
}

fn union_inputs(inputs: Vec<CombineInput>) -> Result<DataFrame> {
    let mut inputs = inputs.into_iter();
    let first = inputs.next().expect("validated non-empty combine inputs");
    let mut union = add_provenance(first)?;
    for input in inputs {
        union = union.union_by_name(add_provenance(input)?)?;
    }
    Ok(union)
}

fn add_provenance(input: CombineInput) -> Result<DataFrame> {
    input
        .frame
        .with_column(SOURCE_COLUMN, lit(input.source.to_string()))?
        .with_column(SEQUENCE_COLUMN, lit(input.sequence))
}

async fn reject_duplicate_keys(frame: DataFrame, key: &RowIdentity) -> Result<()> {
    let key_exprs = key_columns(key);
    let duplicate = frame
        .aggregate(
            key_exprs,
            vec![
                count(lit(1_i64)).alias(COUNT_COLUMN),
                min(col(SOURCE_COLUMN)).alias(FIRST_SOURCE_COLUMN),
                max(col(SOURCE_COLUMN)).alias(LAST_SOURCE_COLUMN),
            ],
        )?
        .filter(col(COUNT_COLUMN).gt(lit(1_i64)))?
        .limit(0, Some(1))?
        .collect()
        .await?;
    let Some(batch) = duplicate.first() else {
        return Ok(());
    };
    if batch.num_rows() == 0 {
        return Ok(());
    }
    let key_values = format_key(batch, key)?;
    let first_source = ScalarValue::try_from_array(batch.column(key.columns().len() + 1), 0)?;
    let last_source = ScalarValue::try_from_array(batch.column(key.columns().len() + 2), 0)?;
    Err(DataFusionError::Execution(format!(
        "duplicate logical key [{key_values}] conflicts between sources {first_source} and {last_source}"
    )))
}

async fn reject_ambiguous_precedence(frame: DataFrame, key: &RowIdentity) -> Result<()> {
    let mut groups = key_columns(key);
    groups.push(col(SEQUENCE_COLUMN));
    let duplicate = frame
        .aggregate(
            groups,
            vec![
                count(lit(1_i64)).alias(COUNT_COLUMN),
                min(col(SOURCE_COLUMN)).alias(FIRST_SOURCE_COLUMN),
            ],
        )?
        .filter(col(COUNT_COLUMN).gt(lit(1_i64)))?
        .limit(0, Some(1))?
        .collect()
        .await?;
    let Some(batch) = duplicate.first() else {
        return Ok(());
    };
    if batch.num_rows() == 0 {
        return Ok(());
    }
    let key_values = format_key(batch, key)?;
    let sequence = ScalarValue::try_from_array(batch.column(key.columns().len()), 0)?;
    let source = ScalarValue::try_from_array(batch.column(key.columns().len() + 2), 0)?;
    Err(DataFusionError::Execution(format!(
        "PreferBySequence is ambiguous for key [{key_values}] in source {source} at sequence {sequence}"
    )))
}

fn format_key(batch: &arrow::record_batch::RecordBatch, key: &RowIdentity) -> Result<String> {
    key.columns()
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let value = ScalarValue::try_from_array(batch.column(index), 0)?;
            Ok(format!("{name}={value}"))
        })
        .collect::<Result<Vec<_>>>()
        .map(|values| values.join(", "))
}

fn prefer_by_sequence(
    frame: DataFrame,
    key: &RowIdentity,
    output_columns: &[String],
) -> Result<DataFrame> {
    let rank = row_number()
        .partition_by(key_columns(key))
        .order_by(vec![col(SEQUENCE_COLUMN).sort(false, false)])
        .build()?;
    let reconciled = frame
        .with_column(ROW_NUMBER_COLUMN, rank)?
        .filter(col(ROW_NUMBER_COLUMN).eq(lit(1_u64)))?;
    select_output_columns(reconciled, output_columns)
}

fn key_columns(key: &RowIdentity) -> Vec<Expr> {
    key.columns()
        .iter()
        .map(|name| col(Column::from_name(name.as_ref())))
        .collect()
}

fn select_output_columns(frame: DataFrame, output_columns: &[String]) -> Result<DataFrame> {
    let expressions = output_columns
        .iter()
        .map(|name| col(Column::from_name(name)))
        .collect::<Vec<Expr>>();
    frame.select(expressions)
}
