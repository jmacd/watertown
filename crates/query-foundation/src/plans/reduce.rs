// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Typed fixed-window aggregate plans.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::Column;
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result};
use datafusion::functions_aggregate::expr_fn::{count, max, min, sum};
use datafusion::logical_expr::{Expr, Operator, binary_expr, col, lit};

use crate::plans::join::exact_event_time_filter;
use crate::statistics::TimeInterval;

/// Validated fixed-window reduction recipe.
#[derive(Clone, Debug)]
pub struct FixedWindowReduce {
    event_time: Arc<str>,
    value: Arc<str>,
    groups: Arc<[Arc<str>]>,
    width: i64,
    origin: i64,
    bounds: Option<TimeInterval>,
}

impl FixedWindowReduce {
    /// Construct an exact integer fixed-window recipe.
    pub fn try_new<I, S>(
        event_time: impl Into<Arc<str>>,
        value: impl Into<Arc<str>>,
        groups: I,
        width: i64,
        origin: i64,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<Arc<str>>,
    {
        let event_time = event_time.into();
        let value = value.into();
        if event_time.is_empty() {
            return Err(DataFusionError::Plan(
                "reduce event-time column must not be empty".to_owned(),
            ));
        }
        if value.is_empty() {
            return Err(DataFusionError::Plan(
                "reduce value column must not be empty".to_owned(),
            ));
        }
        if event_time == value {
            return Err(DataFusionError::Plan(
                "reduce event-time and value columns must be distinct".to_owned(),
            ));
        }
        if width <= 0 {
            return Err(DataFusionError::Plan(format!(
                "reduce window width must be positive, got {width}"
            )));
        }
        let groups = groups.into_iter().map(Into::into).collect::<Vec<_>>();
        let mut unique = BTreeSet::new();
        for group in &groups {
            if group.is_empty() {
                return Err(DataFusionError::Plan(
                    "reduce group column must not be empty".to_owned(),
                ));
            }
            if group.as_ref() == event_time.as_ref() || group.as_ref() == value.as_ref() {
                return Err(DataFusionError::Plan(format!(
                    "reduce group column '{group}' conflicts with event-time or value column"
                )));
            }
            if matches!(
                group.as_ref(),
                "bucket_start" | "rows" | "non_null" | "sum" | "min" | "max"
            ) {
                return Err(DataFusionError::Plan(format!(
                    "reduce group column '{group}' conflicts with an aggregate output"
                )));
            }
            if !unique.insert(group.as_ref()) {
                return Err(DataFusionError::Plan(format!(
                    "duplicate reduce group column '{group}'"
                )));
            }
        }
        Ok(Self {
            event_time,
            value,
            groups: groups.into(),
            width,
            origin,
            bounds: None,
        })
    }

    /// Apply an exact inclusive input bound before reduction.
    #[must_use]
    pub fn with_bounds(mut self, bounds: TimeInterval) -> Self {
        self.bounds = Some(bounds);
        self
    }
}

/// Build a standard DataFusion aggregate over aligned integer event-time
/// buckets.
pub fn reduce_fixed_windows(frame: DataFrame, recipe: &FixedWindowReduce) -> Result<DataFrame> {
    let event_field = frame
        .schema()
        .field_with_unqualified_name(&recipe.event_time)
        .map_err(|_| {
            DataFusionError::Plan(format!(
                "reduce input is missing event-time column '{}'",
                recipe.event_time
            ))
        })?;
    if event_field.data_type() != &DataType::Int64 {
        return Err(DataFusionError::Plan(format!(
            "reduce event-time column '{}' must be Int64, got {}",
            recipe.event_time,
            event_field.data_type()
        )));
    }
    let value_field = frame
        .schema()
        .field_with_unqualified_name(&recipe.value)
        .map_err(|_| {
            DataFusionError::Plan(format!(
                "reduce input is missing value column '{}'",
                recipe.value
            ))
        })?;
    if value_field.data_type() != &DataType::Float64 {
        return Err(DataFusionError::Plan(format!(
            "reduce value column '{}' must be Float64, got {}",
            recipe.value,
            value_field.data_type()
        )));
    }
    for group in recipe.groups.iter() {
        _ = frame
            .schema()
            .field_with_unqualified_name(group)
            .map_err(|_| {
                DataFusionError::Plan(format!("reduce input is missing group column '{group}'"))
            })?;
    }

    let frame = match recipe.bounds {
        Some(bounds) => {
            exact_event_time_filter(frame, &recipe.event_time, bounds, &DataType::Int64)?
        }
        None => frame,
    };
    let bucket = bucket_start_expression(
        col(Column::from_name(recipe.event_time.as_ref())),
        recipe.width,
        recipe.origin,
    )
    .alias("bucket_start");
    let mut group_exprs = Vec::with_capacity(recipe.groups.len() + 1);
    group_exprs.push(bucket);
    group_exprs.extend(
        recipe
            .groups
            .iter()
            .map(|group| col(Column::from_name(group.as_ref()))),
    );
    frame.aggregate(
        group_exprs,
        vec![
            count(lit(1_i64)).alias("rows"),
            count(col(Column::from_name(recipe.value.as_ref()))).alias("non_null"),
            sum(col(Column::from_name(recipe.value.as_ref()))).alias("sum"),
            min(col(Column::from_name(recipe.value.as_ref()))).alias("min"),
            max(col(Column::from_name(recipe.value.as_ref()))).alias("max"),
        ],
    )
}

fn bucket_start_expression(event_time: Expr, width: i64, origin: i64) -> Expr {
    let delta = event_time.clone() - lit(origin);
    let remainder = binary_expr(delta, Operator::Modulo, lit(width));
    let floor_remainder = binary_expr(remainder + lit(width), Operator::Modulo, lit(width));
    event_time - floor_remainder
}
