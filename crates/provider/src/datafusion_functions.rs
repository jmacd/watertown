// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use arrow::array::{Array, ArrayRef, Float64Array, Float64Builder};
use arrow::datatypes::DataType;
use datafusion::common::{Result, exec_err};
use datafusion::execution::FunctionRegistry;
use datafusion::prelude::SessionContext;
use datafusion_expr::expr_fn::create_udwf;
use datafusion_expr::{PartitionEvaluator, Volatility, WindowUDF};
use std::sync::Arc;

#[derive(Debug)]
struct CounterDeltaEvaluator;

impl PartitionEvaluator for CounterDeltaEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        let Some(values) = values
            .first()
            .and_then(|values| values.as_any().downcast_ref::<Float64Array>())
        else {
            return exec_err!("counter_delta requires one Float64 argument");
        };
        if values.len() != num_rows {
            return exec_err!(
                "counter_delta received {} values for a {num_rows}-row partition",
                values.len()
            );
        }
        for index in 0..num_rows {
            if !values.is_null(index) && !values.value(index).is_finite() {
                return exec_err!("counter_delta cannot evaluate non-finite values");
            }
        }

        let mut output = Float64Builder::with_capacity(num_rows);
        for index in 0..num_rows {
            if index == 0 || values.is_null(index) || values.is_null(index - 1) {
                output.append_null();
                continue;
            }

            let previous = values.value(index - 1);
            let current = values.value(index);
            output.append_value((current - previous).max(0.0));
        }
        Ok(Arc::new(output.finish()))
    }

    fn is_causal(&self) -> bool {
        true
    }
}

/// Returns the ordered positive-delta window function.
///
/// `counter_delta(value) OVER (ORDER BY timestamp)` returns null for the first
/// row or a null predecessor, the positive delta for an increase, and zero for
/// an unchanged or decreasing value.
#[must_use]
pub fn counter_delta_udwf() -> WindowUDF {
    create_udwf(
        "counter_delta",
        DataType::Float64,
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(|| Ok(Box::new(CounterDeltaEvaluator))),
    )
}

/// Registers Watertown's DataFusion functions in a session.
pub fn register_datafusion_functions(context: &SessionContext) -> Result<()> {
    let mut registry = context.clone();
    let _ = FunctionRegistry::register_udwf(&mut registry, Arc::new(counter_delta_udwf()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;

    #[tokio::test]
    async fn counter_delta_accumulates_only_positive_changes() {
        let context = SessionContext::new();
        register_datafusion_functions(&context).expect("register functions");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("timestamp", DataType::Int64, false),
                Field::new("value", DataType::Float64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
                Arc::new(Float64Array::from(vec![
                    Some(10.0),
                    Some(12.5),
                    Some(9.0),
                    Some(9.0),
                    None,
                    Some(14.0),
                ])),
            ],
        )
        .expect("batch");
        let _ = context
            .register_table(
                "measurements",
                Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).expect("table")),
            )
            .expect("register table");

        let batches = context
            .sql(
                "SELECT counter_delta(value) OVER (ORDER BY timestamp) AS delta \
                 FROM measurements ORDER BY timestamp",
            )
            .await
            .expect("plan")
            .collect()
            .await
            .expect("execute");
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64 results");

        assert!(values.is_null(0));
        assert_eq!(values.value(1), 2.5);
        assert_eq!(values.value(2), 0.0);
        assert_eq!(values.value(3), 0.0);
        assert!(values.is_null(4));
        assert!(values.is_null(5));
    }

    #[test]
    fn counter_delta_rejects_non_finite_values_including_first_row() {
        let mut evaluator = CounterDeltaEvaluator;
        let values: ArrayRef = Arc::new(Float64Array::from(vec![f64::INFINITY, 1.0]));
        let error = evaluator
            .evaluate_all(&[values], 2)
            .expect_err("non-finite input must fail");
        assert!(error.to_string().contains("non-finite"));
    }
}
