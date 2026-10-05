// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Typed sparse pivot assembled from projected measurement inputs.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::Column;
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::col;

use crate::plans::join::{
    TimestampJoinInput, accumulated_full_outer_join, exact_event_time_filter,
};
use crate::plans::transform::null_pad;
use crate::statistics::TimeInterval;

/// One selected measurement and its output column.
pub struct PivotInput {
    frame: DataFrame,
    event_time: Arc<str>,
    values: Vec<(Arc<str>, Arc<str>)>,
    bounds: Option<TimeInterval>,
}

impl PivotInput {
    /// Select one value column from one independently planned input.
    #[must_use]
    pub fn new(
        frame: DataFrame,
        event_time: impl Into<Arc<str>>,
        value: impl Into<Arc<str>>,
        output: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            frame,
            event_time: event_time.into(),
            values: vec![(value.into(), output.into())],
            bounds: None,
        }
    }

    /// Select an additional value from the same independently planned input.
    ///
    /// Grouping values this way keeps one physical source scan per input.
    #[must_use]
    pub fn with_value(mut self, value: impl Into<Arc<str>>, output: impl Into<Arc<str>>) -> Self {
        self.values.push((value.into(), output.into()));
        self
    }

    /// Apply an exact inclusive event-time bound to this measurement.
    #[must_use]
    pub fn with_bounds(mut self, bounds: TimeInterval) -> Self {
        self.bounds = Some(bounds);
        self
    }
}

/// A requested measurement with no source in the captured membership.
pub struct MissingPivotColumn {
    output: Arc<str>,
    data_type: DataType,
}

impl MissingPivotColumn {
    /// Declare a typed nullable output for an absent measurement.
    #[must_use]
    pub fn new(output: impl Into<Arc<str>>, data_type: DataType) -> Self {
        Self {
            output: output.into(),
            data_type,
        }
    }
}

/// Build a sparse typed pivot without a separate timestamp-spine scan.
pub fn pivot_measurements(
    inputs: Vec<PivotInput>,
    missing: Vec<MissingPivotColumn>,
    output_event_time: &str,
) -> Result<DataFrame> {
    validate_outputs(&inputs, &missing, output_event_time)?;
    let mut projected = inputs
        .into_iter()
        .map(project_input)
        .collect::<Result<Vec<_>>>()?;
    let frame = if projected.len() == 1 {
        let (frame, event_time) = projected.pop().expect("one pivot input");
        let columns = frame
            .schema()
            .fields()
            .iter()
            .filter(|field| field.name() != event_time.as_ref())
            .map(|field| col(Column::from_name(field.name())))
            .collect::<Vec<_>>();
        frame.select(
            std::iter::once(col(Column::from_name(event_time.as_ref())).alias(output_event_time))
                .chain(columns)
                .collect::<Vec<_>>(),
        )?
    } else {
        accumulated_full_outer_join(
            projected
                .into_iter()
                .map(|(frame, event_time)| TimestampJoinInput::new(frame, event_time))
                .collect(),
            output_event_time,
        )?
    };
    null_pad(
        frame,
        missing
            .into_iter()
            .map(|column| (column.output.to_string(), column.data_type))
            .collect(),
    )
}

fn validate_outputs(
    inputs: &[PivotInput],
    missing: &[MissingPivotColumn],
    output_event_time: &str,
) -> Result<()> {
    if inputs.is_empty() {
        return Err(DataFusionError::Plan(
            "pivot requires at least one present measurement input".to_owned(),
        ));
    }
    if output_event_time.is_empty() {
        return Err(DataFusionError::Plan(
            "pivot output event-time name must not be empty".to_owned(),
        ));
    }
    let mut outputs = BTreeSet::from([output_event_time]);
    for output in inputs
        .iter()
        .flat_map(|input| input.values.iter().map(|(_, output)| output.as_ref()))
        .chain(missing.iter().map(|column| column.output.as_ref()))
    {
        if output.is_empty() {
            return Err(DataFusionError::Plan(
                "pivot output column must not be empty".to_owned(),
            ));
        }
        if !outputs.insert(output) {
            return Err(DataFusionError::Plan(format!(
                "duplicate pivot output column '{output}'"
            )));
        }
    }
    Ok(())
}

fn project_input(input: PivotInput) -> Result<(DataFrame, Arc<str>)> {
    let event_field = input
        .frame
        .schema()
        .field_with_unqualified_name(&input.event_time)
        .map_err(|_| {
            DataFusionError::Plan(format!(
                "pivot input is missing event-time column '{}'",
                input.event_time
            ))
        })?;
    let event_type = event_field.data_type().clone();
    if !matches!(
        event_type,
        DataType::Int64 | DataType::Date64 | DataType::Timestamp(_, _)
    ) {
        return Err(DataFusionError::Plan(format!(
            "pivot event-time column '{}' has unsupported type {event_type}",
            input.event_time
        )));
    }
    let mut projection = vec![col(Column::from_name(input.event_time.as_ref()))];
    for (value, output) in &input.values {
        if input.event_time == *value {
            return Err(DataFusionError::Plan(format!(
                "pivot event-time column '{}' cannot also be the value column",
                input.event_time
            )));
        }
        _ = input
            .frame
            .schema()
            .field_with_unqualified_name(value)
            .map_err(|_| {
                DataFusionError::Plan(format!("pivot input is missing value column '{value}'"))
            })?;
        projection.push(col(Column::from_name(value.as_ref())).alias(output.as_ref()));
    }
    let frame = input.frame.select(projection)?;
    let frame = match input.bounds {
        Some(bounds) => {
            exact_event_time_filter(frame, input.event_time.as_ref(), bounds, &event_type)?
        }
        None => frame,
    };
    Ok((frame, input.event_time))
}
