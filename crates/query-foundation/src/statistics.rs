// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Conservative event-time statistics and filter-bound extraction.

use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, Operator};

/// Inclusive minimum and maximum event time for one immutable chunk.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimeInterval {
    min: i64,
    max: i64,
}

impl TimeInterval {
    /// Construct a non-empty inclusive interval.
    ///
    /// # Errors
    ///
    /// Returns an error when `min` is greater than `max`.
    pub fn try_new(min: i64, max: i64) -> datafusion::error::Result<Self> {
        if min > max {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "invalid event-time interval: minimum {min} exceeds maximum {max}"
            )));
        }
        Ok(Self { min, max })
    }

    /// Inclusive minimum.
    #[must_use]
    pub fn min(self) -> i64 {
        self.min
    }

    /// Inclusive maximum.
    #[must_use]
    pub fn max(self) -> i64 {
        self.max
    }

    fn can_intersect(self, bounds: QueryBounds) -> bool {
        if let Some(lower) = bounds.lower
            && (self.max < lower.value || (self.max == lower.value && !lower.inclusive))
        {
            return false;
        }
        if let Some(upper) = bounds.upper
            && (self.min > upper.value || (self.min == upper.value && !upper.inclusive))
        {
            return false;
        }
        true
    }
}

#[derive(Clone, Copy, Debug)]
struct Bound {
    value: i64,
    inclusive: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct QueryBounds {
    lower: Option<Bound>,
    upper: Option<Bound>,
}

impl QueryBounds {
    fn intersect(self, other: Self) -> Self {
        Self {
            lower: tighter_lower(self.lower, other.lower),
            upper: tighter_upper(self.upper, other.upper),
        }
    }

    pub(crate) fn retains(self, interval: TimeInterval) -> bool {
        interval.can_intersect(self)
    }
}

pub(crate) fn event_time_bounds(filters: &[Expr], column: &str) -> Option<QueryBounds> {
    filters
        .iter()
        .filter_map(|filter| expression_bounds(filter, column))
        .reduce(QueryBounds::intersect)
}

fn expression_bounds(expression: &Expr, column: &str) -> Option<QueryBounds> {
    match expression {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            match (
                expression_bounds(&binary.left, column),
                expression_bounds(&binary.right, column),
            ) {
                (Some(left), Some(right)) => Some(left.intersect(right)),
                (Some(bounds), None) | (None, Some(bounds)) => Some(bounds),
                (None, None) => None,
            }
        }
        Expr::BinaryExpr(binary) => {
            comparison_bounds(&binary.left, binary.op, &binary.right, column)
        }
        Expr::Between(between) if !between.negated && is_column(&between.expr, column) => {
            Some(QueryBounds {
                lower: scalar_i64(&between.low).map(|value| Bound {
                    value,
                    inclusive: true,
                }),
                upper: scalar_i64(&between.high).map(|value| Bound {
                    value,
                    inclusive: true,
                }),
            })
            .filter(|bounds| bounds.lower.is_some() || bounds.upper.is_some())
        }
        _ => None,
    }
}

fn comparison_bounds(
    left: &Expr,
    operator: Operator,
    right: &Expr,
    column: &str,
) -> Option<QueryBounds> {
    if is_column(left, column) {
        return bound_for_operator(operator, scalar_i64(right)?);
    }
    if is_column(right, column) {
        return bound_for_operator(reverse(operator)?, scalar_i64(left)?);
    }
    None
}

fn bound_for_operator(operator: Operator, value: i64) -> Option<QueryBounds> {
    let inclusive = matches!(operator, Operator::Eq | Operator::GtEq | Operator::LtEq);
    match operator {
        Operator::Eq => Some(QueryBounds {
            lower: Some(Bound { value, inclusive }),
            upper: Some(Bound { value, inclusive }),
        }),
        Operator::Gt | Operator::GtEq => Some(QueryBounds {
            lower: Some(Bound { value, inclusive }),
            upper: None,
        }),
        Operator::Lt | Operator::LtEq => Some(QueryBounds {
            lower: None,
            upper: Some(Bound { value, inclusive }),
        }),
        _ => None,
    }
}

fn reverse(operator: Operator) -> Option<Operator> {
    match operator {
        Operator::Eq => Some(Operator::Eq),
        Operator::Gt => Some(Operator::Lt),
        Operator::GtEq => Some(Operator::LtEq),
        Operator::Lt => Some(Operator::Gt),
        Operator::LtEq => Some(Operator::GtEq),
        _ => None,
    }
}

fn is_column(expression: &Expr, name: &str) -> bool {
    expression
        .try_as_col()
        .is_some_and(|column| column.name == name)
}

fn scalar_i64(expression: &Expr) -> Option<i64> {
    let Expr::Literal(value, _) = expression else {
        return None;
    };
    match value {
        ScalarValue::Int64(value)
        | ScalarValue::Date64(value)
        | ScalarValue::TimestampSecond(value, _)
        | ScalarValue::TimestampMillisecond(value, _)
        | ScalarValue::TimestampMicrosecond(value, _)
        | ScalarValue::TimestampNanosecond(value, _) => *value,
        _ => None,
    }
}

fn tighter_lower(left: Option<Bound>, right: Option<Bound>) -> Option<Bound> {
    match (left, right) {
        (None, bound) | (bound, None) => bound,
        (Some(left), Some(right)) if left.value > right.value => Some(left),
        (Some(left), Some(right)) if right.value > left.value => Some(right),
        (Some(left), Some(right)) => Some(Bound {
            value: left.value,
            inclusive: left.inclusive && right.inclusive,
        }),
    }
}

fn tighter_upper(left: Option<Bound>, right: Option<Bound>) -> Option<Bound> {
    match (left, right) {
        (None, bound) | (bound, None) => bound,
        (Some(left), Some(right)) if left.value < right.value => Some(left),
        (Some(left), Some(right)) if right.value < left.value => Some(right),
        (Some(left), Some(right)) => Some(Bound {
            value: left.value,
            inclusive: left.inclusive && right.inclusive,
        }),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::{col, lit};

    use super::{TimeInterval, event_time_bounds};

    #[test]
    fn honors_inclusive_and_exclusive_edges() {
        let interval = TimeInterval::try_new(0, 3).expect("valid interval");
        let exclusive = event_time_bounds(&[col("ts").gt(lit(3_i64))], "ts")
            .expect("supported event-time bound");
        let inclusive = event_time_bounds(&[col("ts").gt_eq(lit(3_i64))], "ts")
            .expect("supported event-time bound");

        assert!(!exclusive.retains(interval));
        assert!(inclusive.retains(interval));
    }

    #[test]
    fn handles_literal_on_left_side() {
        let bounds = event_time_bounds(&[lit(100_i64).lt_eq(col("ts"))], "ts")
            .expect("reversed event-time comparison");

        assert!(!bounds.retains(TimeInterval::try_new(0, 99).expect("valid interval")));
        assert!(bounds.retains(TimeInterval::try_new(100, 103).expect("valid interval")));
    }

    #[test]
    fn unsupported_disjunction_produces_no_pruning_bound() {
        let bounds = event_time_bounds(
            &[col("ts").lt(lit(4_i64)).or(col("value").gt(lit(10.0_f64)))],
            "ts",
        );

        assert!(bounds.is_none());
    }

    #[test]
    fn rejects_inverted_interval() {
        let error = TimeInterval::try_new(10, 9).expect_err("inverted interval must fail");

        assert!(error.to_string().contains("minimum 10 exceeds maximum 9"));
    }
}
