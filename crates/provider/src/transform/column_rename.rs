// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Generic Column Rename TableProvider Implementation
//!
//! This module provides the core infrastructure for renaming columns in a TableProvider.
//! It's used by both:
//! - `column-rename` factory: User-configured rename rules
//! - `scope-prefix` factory: Auto-generated rules for scope prefixing
//!
//! The implementation wraps any TableProvider and applies column renaming via:
//! - Schema transformation (forward map: original -> renamed)
//! - Filter rewriting (reverse map: renamed -> original for pushdown)
//! - Batch column renaming in ExecutionPlan

use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use futures::stream::StreamExt;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// Parse Arrow type name to DataType
fn parse_arrow_type(type_name: &str) -> Option<DataType> {
    match type_name.to_lowercase().as_str() {
        "timestamp" => Some(DataType::Timestamp(TimeUnit::Second, Some("+00:00".into()))),
        "utf8" | "string" => Some(DataType::Utf8),
        "int64" => Some(DataType::Int64),
        "int32" => Some(DataType::Int32),
        "float64" => Some(DataType::Float64),
        "float32" => Some(DataType::Float32),
        "boolean" => Some(DataType::Boolean),
        _ => None,
    }
}

/// Column rename function: takes original column name, returns renamed column name
pub type ColumnRenameFunc = Arc<dyn Fn(&str) -> String + Send + Sync>;

/// Column cast function: takes renamed column name, returns optional Arrow data type string
pub type ColumnCastMap = Arc<HashMap<String, String>>;

/// Wraps a TableProvider to rename columns according to a function
pub struct ColumnRenameTableProvider {
    /// The underlying table provider
    inner: Arc<dyn TableProvider>,

    /// Function to rename columns (original -> renamed)
    rename_fn: ColumnRenameFunc,

    /// Optional type casts to apply: renamed_column -> type_name (e.g., "timestamp")
    cast_map: ColumnCastMap,

    /// Cached renamed schema
    schema: SchemaRef,

    /// Reverse mapping: renamed -> original (for filter pushdown)
    reverse_map: HashMap<String, String>,
}

impl ColumnRenameTableProvider {
    /// Create a new column rename wrapper
    ///
    /// # Arguments
    /// * `inner` - The underlying table provider to wrap
    /// * `rename_fn` - Function that maps original column names to renamed names
    /// * `cast_map` - Optional map of renamed columns to cast types (e.g., "timestamp" -> "timestamp")
    pub fn new(
        inner: Arc<dyn TableProvider>,
        rename_fn: ColumnRenameFunc,
        cast_map: HashMap<String, String>,
    ) -> DataFusionResult<Self> {
        let original_schema = inner.schema();
        let cast_map_arc = Arc::new(cast_map.clone());
        let (renamed_schema, reverse_map) =
            Self::create_renamed_schema(&original_schema, &rename_fn, &cast_map_arc)?;

        Ok(Self {
            inner,
            rename_fn,
            cast_map: cast_map_arc,
            schema: Arc::new(renamed_schema),
            reverse_map,
        })
    }

    /// Create a new schema with renamed columns and reverse mapping
    fn create_renamed_schema(
        original: &Schema,
        rename_fn: &ColumnRenameFunc,
        cast_map: &ColumnCastMap,
    ) -> DataFusionResult<(Schema, HashMap<String, String>)> {
        let mut reverse_map = HashMap::new();

        let renamed_fields: Vec<Field> = original
            .fields()
            .iter()
            .map(|field| {
                let original_name = field.name();
                let new_name = rename_fn(original_name);

                // Track reverse mapping for filter pushdown
                if new_name != *original_name {
                    let _ = reverse_map.insert(new_name.clone(), original_name.clone());
                }

                // Check if this column needs type casting
                let data_type = if let Some(cast_type) = cast_map.get(&new_name) {
                    parse_arrow_type(cast_type).unwrap_or_else(|| {
                        log::warn!(
                            "Unknown cast type '{}' for column '{}', keeping original type",
                            cast_type,
                            new_name
                        );
                        field.data_type().clone()
                    })
                } else {
                    field.data_type().clone()
                };

                Field::new(new_name, data_type, field.is_nullable())
            })
            .collect();

        Ok((Schema::new(renamed_fields), reverse_map))
    }

    /// Rewrite an expression to use original column names
    fn rewrite_expr(&self, expr: &Expr) -> Expr {
        use datafusion::common::Column;
        use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};

        expr.clone()
            .transform(|e| {
                Ok(if let Expr::Column(col) = &e {
                    // Map renamed column back to original
                    if let Some(original) = self.reverse_map.get(&col.name) {
                        Transformed::yes(Expr::Column(Column::new(
                            col.relation.clone(),
                            original.clone(),
                        )))
                    } else {
                        Transformed::no(e)
                    }
                } else {
                    Transformed::no(e)
                })
            })
            .data()
            .expect("Column name rewriting is infallible")
    }
}

impl fmt::Debug for ColumnRenameTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ColumnRenameTableProvider")
            .field("inner", &"<TableProvider>")
            .field("schema", &self.schema)
            .field("reverse_map", &self.reverse_map)
            .finish()
    }
}

#[async_trait]
impl TableProvider for ColumnRenameTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Rewrite filters to use original column names
        let rewritten_filters: Vec<Expr> =
            filters.iter().map(|expr| self.rewrite_expr(expr)).collect();

        // Get plan from inner provider
        let plan = self
            .inner
            .scan(state, projection, &rewritten_filters, limit)
            .await?;

        // Calculate output schema based on projection
        let output_schema = if let Some(proj) = projection {
            let fields: Vec<_> = proj.iter().map(|&i| self.schema.field(i).clone()).collect();
            Arc::new(Schema::new(fields))
        } else {
            self.schema.clone()
        };

        // Wrap execution plan to rename columns in output batches
        Ok(Arc::new(ColumnRenameExec::new(
            plan,
            output_schema,
            self.rename_fn.clone(),
            self.cast_map.clone(),
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // Rewrite filters and ask inner provider
        let rewritten: Vec<Expr> = filters.iter().map(|f| self.rewrite_expr(f)).collect();
        let refs: Vec<&Expr> = rewritten.iter().collect();
        self.inner.supports_filters_pushdown(&refs)
    }
}

/// Execution plan that renames columns in output batches
pub struct ColumnRenameExec {
    inner: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    rename_fn: ColumnRenameFunc,
    cast_map: ColumnCastMap,
    properties: datafusion::physical_plan::PlanProperties,
}

impl ColumnRenameExec {
    pub fn new(
        inner: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
        rename_fn: ColumnRenameFunc,
        cast_map: ColumnCastMap,
    ) -> Self {
        let properties = Self::compute_properties(&output_schema, &inner);
        Self {
            inner,
            output_schema,
            rename_fn,
            cast_map,
            properties,
        }
    }

    fn compute_properties(
        schema: &SchemaRef,
        inner: &Arc<dyn ExecutionPlan>,
    ) -> datafusion::physical_plan::PlanProperties {
        use datafusion::physical_expr::EquivalenceProperties;
        use datafusion::physical_plan::{Partitioning, PlanProperties};

        let inner_props = inner.properties();
        let partition_count = inner_props.output_partitioning().partition_count();
        PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(partition_count),
            inner_props.emission_type,
            inner_props.boundedness,
        )
    }
}

impl fmt::Debug for ColumnRenameExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ColumnRenameExec")
    }
}

impl DisplayAs for ColumnRenameExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ColumnRenameExec")
    }
}

impl ExecutionPlan for ColumnRenameExec {
    fn name(&self) -> &str {
        "ColumnRenameExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "ColumnRenameExec expects exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(ColumnRenameExec::new(
            Arc::clone(&children[0]),
            self.output_schema.clone(),
            self.rename_fn.clone(),
            self.cast_map.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let inner_stream = self.inner.execute(partition, context)?;
        let output_schema = self.output_schema.clone();
        let output_schema_for_adapter = output_schema.clone();
        let rename_fn = self.rename_fn.clone();
        let cast_map = self.cast_map.clone();

        // Create stream that renames columns in each batch
        let stream = inner_stream.map(move |batch_result| {
            batch_result.and_then(|batch| {
                rename_batch_columns(&batch, &output_schema, &rename_fn, &cast_map)
            })
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema_for_adapter,
            stream,
        )))
    }
}

/// Rename columns in a RecordBatch and apply type casts if needed
fn rename_batch_columns(
    batch: &RecordBatch,
    target_schema: &SchemaRef,
    _rename_fn: &ColumnRenameFunc,
    cast_map: &ColumnCastMap,
) -> DataFusionResult<RecordBatch> {
    use datafusion::arrow::compute::cast;

    // If no casts needed, just rename schema
    if cast_map.is_empty() {
        let columns = batch.columns().to_vec();
        return renamed_batch(target_schema, columns, batch.num_rows());
    }

    // Apply casts where needed
    let mut new_columns = Vec::with_capacity(batch.num_columns());

    for (i, field) in target_schema.fields().iter().enumerate() {
        let column = batch.column(i);

        // Check if this column needs casting
        if cast_map.contains_key(field.name()) {
            // Cast to target type
            let casted = cast(column, field.data_type())
                .map_err(|e| DataFusionError::ArrowError(e.into(), None))?;
            new_columns.push(casted);
        } else {
            new_columns.push(column.clone());
        }
    }

    renamed_batch(target_schema, new_columns, batch.num_rows())
}

fn renamed_batch(
    schema: &SchemaRef,
    columns: Vec<ArrayRef>,
    row_count: usize,
) -> DataFusionResult<RecordBatch> {
    RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(row_count)),
    )
    .map_err(|e| DataFusionError::ArrowError(e.into(), None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, Int64Array};
    use datafusion::arrow::datatypes::Field;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn count_star_preserves_rows_through_empty_projection() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let inner = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
        let table = Arc::new(
            ColumnRenameTableProvider::new(
                inner,
                Arc::new(|name| format!("renamed_{name}")),
                HashMap::new(),
            )
            .unwrap(),
        );
        let context = SessionContext::new();
        _ = context.register_table("renamed", table).unwrap();

        let batches = context
            .sql("SELECT COUNT(*) AS count FROM renamed")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let count = batches[0]
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count.value(0), 3);
    }
}
