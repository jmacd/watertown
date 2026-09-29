// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Column transforms expressed only as DataFusion logical projections.

use std::collections::BTreeSet;

use arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, cast, col, lit};

/// One output column in a typed logical projection.
#[derive(Clone, Debug)]
pub enum ProjectionColumn {
    /// Read a source column, optionally cast it, and assign an output name.
    Source {
        /// Input column name.
        source: String,
        /// Output column name.
        output: String,
        /// Strict output cast, when required.
        data_type: Option<DataType>,
    },
    /// Produce a typed nullable column.
    Null {
        /// Output column name.
        output: String,
        /// Arrow type of the null value.
        data_type: DataType,
    },
}

impl ProjectionColumn {
    /// Preserve a source column unchanged.
    #[must_use]
    pub fn identity(name: impl Into<String>) -> Self {
        let name = name.into();
        Self::Source {
            source: name.clone(),
            output: name,
            data_type: None,
        }
    }

    /// Rename a source column.
    #[must_use]
    pub fn rename(source: impl Into<String>, output: impl Into<String>) -> Self {
        Self::Source {
            source: source.into(),
            output: output.into(),
            data_type: None,
        }
    }

    /// Strictly cast a source column and assign its output name.
    #[must_use]
    pub fn cast(source: impl Into<String>, output: impl Into<String>, data_type: DataType) -> Self {
        Self::Source {
            source: source.into(),
            output: output.into(),
            data_type: Some(data_type),
        }
    }

    /// Add a typed null output column.
    #[must_use]
    pub fn null(output: impl Into<String>, data_type: DataType) -> Self {
        Self::Null {
            output: output.into(),
            data_type,
        }
    }

    fn output(&self) -> &str {
        match self {
            Self::Source { output, .. } | Self::Null { output, .. } => output,
        }
    }
}

/// Apply a validated typed projection.
pub fn project(frame: DataFrame, columns: Vec<ProjectionColumn>) -> Result<DataFrame> {
    let mut outputs = BTreeSet::new();
    for column in &columns {
        if column.output().is_empty() {
            return Err(DataFusionError::Plan(
                "projection output name must not be empty".to_owned(),
            ));
        }
        if !outputs.insert(column.output()) {
            return Err(DataFusionError::Plan(format!(
                "duplicate projection output '{}'",
                column.output()
            )));
        }
        if let ProjectionColumn::Source { source, .. } = column {
            _ = frame.schema().field_with_unqualified_name(source)?;
        }
    }

    let expressions = columns
        .into_iter()
        .map(|column| match column {
            ProjectionColumn::Source {
                source,
                output,
                data_type,
            } => Ok(match data_type {
                Some(data_type) => cast(col(&source), data_type).alias(output),
                None => alias_when_needed(col(&source), &source, &output),
            }),
            ProjectionColumn::Null { output, data_type } => {
                let value = ScalarValue::try_new_null(&data_type)?;
                Ok(lit(value).alias(output))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    frame.select(expressions)
}

/// Rename one column while preserving all other columns and their order.
pub fn rename_column(frame: DataFrame, source: &str, output: &str) -> Result<DataFrame> {
    require_column(&frame, source)?;
    let columns = frame
        .schema()
        .fields()
        .iter()
        .map(|field| {
            if field.name() == source {
                ProjectionColumn::rename(source, output)
            } else {
                ProjectionColumn::identity(field.name())
            }
        })
        .collect();
    project(frame, columns)
}

/// Strictly cast one column while preserving its name and all other columns.
pub fn cast_column(frame: DataFrame, source: &str, data_type: DataType) -> Result<DataFrame> {
    require_column(&frame, source)?;
    let columns = frame
        .schema()
        .fields()
        .iter()
        .map(|field| {
            if field.name() == source {
                ProjectionColumn::cast(source, source, data_type.clone())
            } else {
                ProjectionColumn::identity(field.name())
            }
        })
        .collect();
    project(frame, columns)
}

/// Prefix every column except the event-time column with `scope.`.
pub fn scope_prefix(frame: DataFrame, scope: &str, event_time_column: &str) -> Result<DataFrame> {
    if scope.is_empty() {
        return Err(DataFusionError::Plan(
            "scope prefix must not be empty".to_owned(),
        ));
    }
    require_column(&frame, event_time_column)?;
    let columns = frame
        .schema()
        .fields()
        .iter()
        .map(|field| {
            if field.name() == event_time_column {
                ProjectionColumn::identity(field.name())
            } else {
                ProjectionColumn::rename(field.name(), format!("{scope}.{}", field.name()))
            }
        })
        .collect();
    project(frame, columns)
}

/// Add missing nullable columns using typed null expressions.
pub fn null_pad(frame: DataFrame, columns: Vec<(String, DataType)>) -> Result<DataFrame> {
    let mut projection = frame
        .schema()
        .fields()
        .iter()
        .map(|field| ProjectionColumn::identity(field.name()))
        .collect::<Vec<_>>();
    let mut additions = BTreeSet::new();
    for (name, data_type) in columns {
        if !additions.insert(name.clone()) {
            return Err(DataFusionError::Plan(format!(
                "duplicate null-padding column '{name}'"
            )));
        }
        match frame.schema().field_with_unqualified_name(&name) {
            Ok(field) if field.data_type() != &data_type => {
                return Err(DataFusionError::Plan(format!(
                    "existing column '{name}' has type {}, requested null-padding type {data_type}",
                    field.data_type()
                )));
            }
            Ok(_) => {}
            Err(_) => projection.push(ProjectionColumn::null(name, data_type)),
        }
    }
    project(frame, projection)
}

fn require_column(frame: &DataFrame, name: &str) -> Result<()> {
    _ = frame.schema().field_with_unqualified_name(name)?;
    Ok(())
}

fn alias_when_needed(expression: Expr, source: &str, output: &str) -> Expr {
    if source == output {
        expression
    } else {
        expression.alias(output)
    }
}
