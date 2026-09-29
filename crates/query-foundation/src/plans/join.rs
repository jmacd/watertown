// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Typed accumulated full-outer joins for timestamp-local series.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{Column, ScalarValue};
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result};
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::logical_expr::{Expr, JoinType, col, lit};

use crate::statistics::TimeInterval;

const ACCUMULATED_TIME: &str = "__watertown_join_accumulated_time";
const INPUT_TIME_PREFIX: &str = "__watertown_join_input_time_";

/// One independently planned input to a timestamp join.
pub struct TimestampJoinInput {
    frame: DataFrame,
    event_time: Arc<str>,
    bounds: Option<TimeInterval>,
}

impl TimestampJoinInput {
    /// Declare the event-time column for one input.
    #[must_use]
    pub fn new(frame: DataFrame, event_time: impl Into<Arc<str>>) -> Self {
        Self {
            frame,
            event_time: event_time.into(),
            bounds: None,
        }
    }

    /// Apply an exact inclusive bound before the input reaches the join.
    #[must_use]
    pub fn with_bounds(mut self, bounds: TimeInterval) -> Self {
        self.bounds = Some(bounds);
        self
    }
}

struct PreparedInput {
    frame: DataFrame,
    key: String,
    columns: Vec<String>,
}

/// Accumulate inputs through typed full-outer event-time joins.
///
/// Every input appears once in the logical plan. The accumulated event-time
/// key is coalesced after each join, so timestamps absent from the first input
/// remain available to later inputs.
pub fn accumulated_full_outer_join(
    inputs: Vec<TimestampJoinInput>,
    output_event_time: &str,
) -> Result<DataFrame> {
    let event_type = validate_inputs(&inputs, output_event_time)?;
    let mut prepared = inputs
        .into_iter()
        .enumerate()
        .map(|(index, input)| prepare_input(index, input, &event_type))
        .collect::<Result<Vec<_>>>()?
        .into_iter();
    let first = prepared.next().expect("validated join inputs");
    let mut output_columns = first.columns;
    let mut accumulator = first.frame.select(
        std::iter::once(col(Column::from_name(&first.key)).alias(ACCUMULATED_TIME))
            .chain(named_columns(&output_columns))
            .collect::<Vec<_>>(),
    )?;

    for input in prepared {
        let joined = accumulator.join(
            input.frame,
            JoinType::Full,
            &[ACCUMULATED_TIME],
            &[input.key.as_str()],
            None,
        )?;
        output_columns.extend(input.columns);
        accumulator = joined.select(
            std::iter::once(
                coalesce(vec![
                    col(Column::from_name(ACCUMULATED_TIME)),
                    col(Column::from_name(&input.key)),
                ])
                .alias(ACCUMULATED_TIME),
            )
            .chain(named_columns(&output_columns))
            .collect::<Vec<_>>(),
        )?;
    }

    accumulator.select(
        std::iter::once(col(Column::from_name(ACCUMULATED_TIME)).alias(output_event_time))
            .chain(named_columns(&output_columns))
            .collect::<Vec<_>>(),
    )
}

fn validate_inputs(inputs: &[TimestampJoinInput], output_event_time: &str) -> Result<DataType> {
    if inputs.len() < 2 {
        return Err(DataFusionError::Plan(
            "timestamp join requires at least two inputs".to_owned(),
        ));
    }
    if output_event_time.is_empty() {
        return Err(DataFusionError::Plan(
            "timestamp join output event-time name must not be empty".to_owned(),
        ));
    }

    let mut event_type = None;
    let mut outputs = BTreeMap::new();
    for (index, input) in inputs.iter().enumerate() {
        if input.event_time.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "timestamp join input {index} event-time name must not be empty"
            )));
        }
        let field = input
            .frame
            .schema()
            .field_with_unqualified_name(&input.event_time)
            .map_err(|_| {
                DataFusionError::Plan(format!(
                    "timestamp join input {index} is missing event-time column '{}'",
                    input.event_time
                ))
            })?;
        validate_event_type(field.data_type())?;
        match &event_type {
            Some(expected) if expected != field.data_type() => {
                return Err(DataFusionError::Plan(format!(
                    "timestamp join input {index} event-time column '{}' has type {}, expected {expected}",
                    input.event_time,
                    field.data_type()
                )));
            }
            None => event_type = Some(field.data_type().clone()),
            Some(_) => {}
        }

        for field in input.frame.schema().fields() {
            if field.name() == input.event_time.as_ref() {
                continue;
            }
            if field.name() == output_event_time
                || field.name() == ACCUMULATED_TIME
                || field.name().starts_with(INPUT_TIME_PREFIX)
            {
                return Err(DataFusionError::Plan(format!(
                    "timestamp join input {index} output column '{}' conflicts with a join key",
                    field.name()
                )));
            }
            if let Some(prior) = outputs.insert(field.name().to_owned(), index) {
                return Err(DataFusionError::Plan(format!(
                    "timestamp join output column '{}' appears in inputs {prior} and {index}; scope columns or combine same-scope inputs before joining",
                    field.name()
                )));
            }
        }
    }
    Ok(event_type.expect("validated join inputs"))
}

fn validate_event_type(data_type: &DataType) -> Result<()> {
    if matches!(
        data_type,
        DataType::Int64 | DataType::Date64 | DataType::Timestamp(_, _)
    ) {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "timestamp join event-time type {data_type} is unsupported"
        )))
    }
}

fn prepare_input(
    index: usize,
    input: TimestampJoinInput,
    event_type: &DataType,
) -> Result<PreparedInput> {
    let key = format!("{INPUT_TIME_PREFIX}{index}");
    let frame = match input.bounds {
        Some(bounds) => input.frame.filter(
            col(Column::from_name(input.event_time.as_ref()))
                .gt_eq(event_literal(bounds.min(), event_type)?)
                .and(
                    col(Column::from_name(input.event_time.as_ref()))
                        .lt_eq(event_literal(bounds.max(), event_type)?),
                ),
        )?,
        None => input.frame,
    };
    let columns = frame
        .schema()
        .fields()
        .iter()
        .filter(|field| field.name() != input.event_time.as_ref())
        .map(|field| field.name().to_owned())
        .collect::<Vec<_>>();
    let projection = std::iter::once(col(Column::from_name(input.event_time.as_ref())).alias(&key))
        .chain(named_columns(&columns))
        .collect::<Vec<_>>();
    Ok(PreparedInput {
        frame: frame.select(projection)?,
        key,
        columns,
    })
}

fn named_columns(names: &[String]) -> impl Iterator<Item = Expr> + '_ {
    names
        .iter()
        .map(|name| col(Column::from_name(name.as_str())))
}

fn event_literal(value: i64, data_type: &DataType) -> Result<Expr> {
    let scalar = match data_type {
        DataType::Int64 => ScalarValue::Int64(Some(value)),
        DataType::Date64 => ScalarValue::Date64(Some(value)),
        DataType::Timestamp(TimeUnit::Second, timezone) => {
            ScalarValue::TimestampSecond(Some(value), timezone.clone())
        }
        DataType::Timestamp(TimeUnit::Millisecond, timezone) => {
            ScalarValue::TimestampMillisecond(Some(value), timezone.clone())
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            ScalarValue::TimestampMicrosecond(Some(value), timezone.clone())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, timezone) => {
            ScalarValue::TimestampNanosecond(Some(value), timezone.clone())
        }
        _ => {
            return Err(DataFusionError::Plan(format!(
                "timestamp join event-time type {data_type} is unsupported"
            )));
        }
    };
    Ok(lit(scalar))
}
