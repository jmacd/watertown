// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Null-Padding Table Provider
//!
//! A wrapper TableProvider that extends a table's schema with nullable columns.
//! When the underlying table is missing specified columns, they are added as
//! nullable fields filled with NULL values.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, new_null_array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::stream::StreamExt;

/// Creates a table provider that extends the schema with nullable columns
pub fn null_padding_table(
    inner: Arc<dyn TableProvider>,
    extend_columns: HashMap<String, DataType>,
) -> DataFusionResult<Arc<dyn TableProvider>> {
    let inner_schema = inner.schema();

    // Check if any columns actually need to be added
    let has_columns_to_add = extend_columns
        .iter()
        .any(|(col_name, _)| inner_schema.column_with_name(col_name).is_none());

    if !has_columns_to_add {
        // No columns to add - just return the inner table provider
        return Ok(inner);
    }

    Ok(Arc::new(NullPaddingTableProvider::new(
        inner,
        extend_columns,
    )?))
}

/// TableProvider that adds nullable columns to match a desired schema
#[derive(Debug)]
struct NullPaddingTableProvider {
    inner: Arc<dyn TableProvider>,
    extended_schema: SchemaRef,
    /// Indices of columns that need to be added (not in inner schema)
    columns_to_add: Vec<usize>,
}

impl NullPaddingTableProvider {
    fn new(
        inner: Arc<dyn TableProvider>,
        extend_columns: HashMap<String, DataType>,
    ) -> DataFusionResult<Self> {
        let inner_schema = inner.schema();
        let mut extended_fields = inner_schema.fields().to_vec();
        let mut columns_to_add = Vec::new();

        // Check each column we want to extend
        for (col_name, data_type) in extend_columns {
            if inner_schema.column_with_name(&col_name).is_none() {
                // Column doesn't exist - need to add it
                let field = Field::new(&col_name, data_type, true);
                columns_to_add.push(extended_fields.len());
                extended_fields.push(Arc::new(field));
            }
        }

        let extended_schema = Arc::new(Schema::new(extended_fields));

        Ok(Self {
            inner,
            extended_schema,
            columns_to_add,
        })
    }
}

#[async_trait]
impl TableProvider for NullPaddingTableProvider {
    fn schema(&self) -> SchemaRef {
        self.extended_schema.clone()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        use datafusion::common::tree_node::TreeNode;

        let inner_schema = self.inner.schema();

        // Separate filters into those that reference inner columns vs padded columns
        let mut inner_filters = Vec::new();
        let mut results = Vec::new();

        for filter in filters {
            let mut references_padded = false;
            let _ = filter.apply(|expr| {
                if let Expr::Column(col) = expr
                    && inner_schema.column_with_name(&col.name).is_none()
                {
                    references_padded = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            });

            if references_padded {
                // Filter references padded columns - can't push to inner
                results.push(TableProviderFilterPushDown::Unsupported);
            } else {
                // Filter only references inner columns - ask inner provider
                inner_filters.push(*filter);
            }
        }

        // Ask inner provider about filters it can handle
        if !inner_filters.is_empty() {
            let inner_support = self.inner.supports_filters_pushdown(&inner_filters)?;

            // Merge results: padded column filters are Unsupported, others use inner's answer
            let mut inner_idx = 0;
            for i in 0..filters.len() {
                if results.len() <= i {
                    // This filter was for inner columns
                    results.push(inner_support[inner_idx].clone());
                    inner_idx += 1;
                }
            }
        }

        Ok(results)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        use datafusion::common::tree_node::TreeNode;

        // Filter out any predicates that reference padded columns
        // Only push down filters that reference columns in the inner schema
        // Note: DataFusion's optimizer already handles filter simplification at the query planning level,
        // so we don't need to use ExprSimplifier here. We just classify which filters can be pushed down.
        let inner_schema = self.inner.schema();
        let pushdown_filters: Vec<Expr> = filters
            .iter()
            .filter(|filter| {
                let mut references_padded = false;
                let _ = filter.apply(|expr| {
                    if let Expr::Column(col) = expr
                        && inner_schema.column_with_name(&col.name).is_none()
                    {
                        references_padded = true;
                        return Ok(TreeNodeRecursion::Stop);
                    }
                    Ok(TreeNodeRecursion::Continue)
                });
                !references_padded
            })
            .cloned()
            .collect();

        // Push down projection to inner table for columns that exist there.
        // Map from extended schema indices to inner schema indices.
        let num_inner_fields = inner_schema.fields().len();

        let (inner_projection, output_schema) = if let Some(proj) = projection {
            let mut inner_proj = Vec::new();

            // Extended schema has inner fields first, then padded fields
            // If idx < num_inner_fields, it's an inner column, otherwise it's padded
            for &idx in proj {
                if idx < num_inner_fields {
                    inner_proj.push(idx);
                }
            }

            // Only push down projection if we're requesting at least one column from inner
            let inner_projection = if inner_proj.is_empty() {
                Some(vec![0])
            } else {
                Some(inner_proj)
            };

            // Calculate output schema from projection
            let projected_fields: Vec<_> = proj
                .iter()
                .map(|&i| self.extended_schema.field(i).clone())
                .collect();
            let output_schema = Arc::new(Schema::new(projected_fields));

            (inner_projection, output_schema)
        } else {
            // No projection - scan all columns from inner, output full extended schema
            (None, self.extended_schema.clone())
        };

        let inner_plan = self
            .inner
            .scan(state, inner_projection.as_ref(), &pushdown_filters, limit)
            .await?;

        let cache = NullPaddingExec::compute_properties(inner_plan.properties(), output_schema);

        Ok(Arc::new(NullPaddingExec {
            inner: inner_plan,
            extended_schema: self.extended_schema.clone(),
            columns_to_add: self.columns_to_add.clone(),
            projection: projection.cloned(),
            inner_projection: inner_projection.clone(),
            cache,
        }))
    }
}

/// Physical execution plan that adds null columns
#[derive(Debug)]
struct NullPaddingExec {
    inner: Arc<dyn ExecutionPlan>,
    extended_schema: SchemaRef,
    columns_to_add: Vec<usize>,
    projection: Option<Vec<usize>>,
    /// The projection that was pushed down to the inner plan
    inner_projection: Option<Vec<usize>>,
    cache: Arc<PlanProperties>,
}

impl NullPaddingExec {
    /// Compute plan properties for the null padding execution plan
    fn compute_properties(
        inner_props: &PlanProperties,
        output_schema: SchemaRef,
    ) -> Arc<PlanProperties> {
        // We preserve partitioning, boundedness, and emission type from inner plan
        // but create new equivalence properties for the extended schema
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            inner_props.output_partitioning().clone(),
            inner_props.emission_type,
            inner_props.boundedness,
        ))
    }
}

impl DisplayAs for NullPaddingExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "NullPaddingExec: adding {} null columns",
                    self.columns_to_add.len()
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "NullPaddingExec")
            }
        }
    }
}

impl ExecutionPlan for NullPaddingExec {
    fn name(&self) -> &str {
        "NullPaddingExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "NullPaddingExec expects exactly one child".to_string(),
            ));
        }

        let cache = NullPaddingExec::compute_properties(
            children[0].properties(),
            self.properties().equivalence_properties().schema().clone(),
        );

        Ok(Arc::new(NullPaddingExec {
            inner: children[0].clone(),
            extended_schema: self.extended_schema.clone(),
            columns_to_add: self.columns_to_add.clone(),
            projection: self.projection.clone(),
            inner_projection: self.inner_projection.clone(),
            cache,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let inner_stream = self.inner.execute(partition, context)?;
        let extended_schema = self.extended_schema.clone();
        let columns_to_add = self.columns_to_add.clone();
        let projection = self.projection.clone();
        let inner_projection = self.inner_projection.clone();

        let output_schema = self.schema();

        let stream = inner_stream.map(move |batch_result| {
            batch_result.and_then(|batch| {
                extend_batch(
                    &batch,
                    &extended_schema,
                    &columns_to_add,
                    &projection,
                    &inner_projection,
                )
            })
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream,
        )))
    }
}

/// Extend a single batch with null columns and apply projection
fn extend_batch(
    batch: &RecordBatch,
    extended_schema: &SchemaRef,
    columns_to_add: &[usize],
    projection: &Option<Vec<usize>>,
    inner_projection: &Option<Vec<usize>>,
) -> DataFusionResult<RecordBatch> {
    let num_rows = batch.num_rows();

    if let Some(proj) = projection
        && proj.is_empty()
    {
        let schema = Arc::new(
            extended_schema
                .project(proj)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
        );
        return RecordBatch::try_new_with_options(
            schema,
            Vec::new(),
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
    }

    // If we have inner_projection, we need to reconstruct the full extended schema
    let columns: Vec<ArrayRef> = if let Some(inner_proj) = inner_projection {
        // Inner was projected - batch has a subset of inner columns
        // Build full extended schema: first the inner columns, then the padded columns
        let mut result_cols = Vec::with_capacity(extended_schema.fields().len());
        let num_inner_fields = extended_schema.fields().len() - columns_to_add.len();

        // Add inner columns in order - either from batch if in projection, or null if not
        for inner_idx in 0..num_inner_fields {
            let col = if let Some(batch_idx) = inner_proj.iter().position(|&i| i == inner_idx) {
                // This inner column was projected - take it from batch
                batch.column(batch_idx).clone()
            } else {
                // This inner column was not projected - fill with null
                let field = extended_schema.field(inner_idx);
                new_null_array(field.data_type(), num_rows)
            };
            result_cols.push(col);
        }

        // Add null columns for all padded fields
        for &field_idx in columns_to_add {
            let field = extended_schema.field(field_idx);
            result_cols.push(new_null_array(field.data_type(), num_rows));
        }

        result_cols
    } else {
        // No inner projection - batch has all inner columns, just add padding
        let mut cols: Vec<ArrayRef> = batch.columns().to_vec();

        // Add null arrays for missing columns
        for &field_idx in columns_to_add {
            let field = extended_schema.field(field_idx);
            let null_array = new_null_array(field.data_type(), num_rows);
            cols.push(null_array);
        }
        cols
    };

    // Create full extended batch
    let extended_batch = RecordBatch::try_new(extended_schema.clone(), columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    // Apply projection if needed
    if let Some(proj) = projection {
        extended_batch
            .project(proj)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    } else {
        Ok(extended_batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, Int32Array, StringArray};
    use arrow_array::record_batch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::*;

    #[tokio::test]
    async fn test_no_columns_to_add() -> DataFusionResult<()> {
        // Create a table with columns a, b
        let batch = record_batch!(("a", Int32, [1, 2, 3]), ("b", Float64, [1.0, 2.0, 3.0]))?;

        let schema = batch.schema();
        let table = MemTable::try_new(schema.clone(), vec![vec![batch]])?;

        // Extend with columns that already exist
        let mut columns = HashMap::new();
        let _ = columns.insert("a".to_string(), DataType::Int32);
        let _ = columns.insert("b".to_string(), DataType::Float64);
        let extended = null_padding_table(Arc::new(table), columns)?;

        // Schema should be unchanged
        assert_eq!(extended.schema().fields().len(), 2);

        // Query should work
        let ctx = SessionContext::new();
        let _ = ctx.register_table("test", extended)?;
        let df = ctx.sql("SELECT * FROM test").await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);
        assert_eq!(results[0].num_columns(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_add_one_column() -> DataFusionResult<()> {
        // Create a table with just column a
        let batch = record_batch!(("a", Int32, [1, 2, 3]))?;

        let schema = batch.schema();
        let table = MemTable::try_new(schema.clone(), vec![vec![batch]])?;

        // Extend with column b
        let mut columns = HashMap::new();
        let _ = columns.insert("b".to_string(), DataType::Float64);
        let extended = null_padding_table(Arc::new(table), columns)?;

        // Schema should have both columns
        assert_eq!(extended.schema().fields().len(), 2);
        assert_eq!(extended.schema().field(0).name(), "a");
        assert_eq!(extended.schema().field(1).name(), "b");
        assert!(extended.schema().field(1).is_nullable());

        // Query and verify
        let ctx = SessionContext::new();
        let _ = ctx.register_table("test", extended)?;
        let df = ctx.sql("SELECT a, b FROM test").await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);
        assert_eq!(results[0].num_columns(), 2);

        // Column b should be all nulls
        let b_col = results[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(b_col.is_null(0));
        assert!(b_col.is_null(1));
        assert!(b_col.is_null(2));

        Ok(())
    }

    #[tokio::test]
    async fn test_add_multiple_columns() -> DataFusionResult<()> {
        // Create a table with columns a, b
        let batch = record_batch!(("a", Int32, [1, 2]), ("b", Float64, [1.0, 2.0]))?;

        let schema = batch.schema();
        let table = MemTable::try_new(schema, vec![vec![batch]])?;

        // Extend with columns c, d, e
        let mut columns = HashMap::new();
        let _ = columns.insert("c".to_string(), DataType::Float64);
        let _ = columns.insert("d".to_string(), DataType::Float64);
        let _ = columns.insert("e".to_string(), DataType::Float64);
        let extended = null_padding_table(Arc::new(table), columns)?;

        // Schema should have 5 columns total
        assert_eq!(extended.schema().fields().len(), 5);
        // Original columns should be first
        assert_eq!(extended.schema().field(0).name(), "a");
        assert_eq!(extended.schema().field(1).name(), "b");
        // New columns c, d, e should exist (order not guaranteed with HashMap)
        assert!(extended.schema().column_with_name("c").is_some());
        assert!(extended.schema().column_with_name("d").is_some());
        assert!(extended.schema().column_with_name("e").is_some());

        // Query and verify all columns
        let ctx = SessionContext::new();
        let _ = ctx.register_table("test", extended)?;
        let df = ctx.sql("SELECT a, b, c, d, e FROM test").await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 2);
        assert_eq!(results[0].num_columns(), 5);

        // Columns c, d, e should be all nulls
        for col_idx in 2..5 {
            let col = results[0]
                .column(col_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert!(col.is_null(0));
            assert!(col.is_null(1));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_mixed_existing_and_new_columns() -> DataFusionResult<()> {
        // Create a table with columns a, c
        let batch = record_batch!(("a", Int32, [10, 20]), ("c", Float64, [100.0, 200.0]))?;

        let schema = batch.schema();
        let table = MemTable::try_new(schema, vec![vec![batch]])?;

        // Extend with [a, b, c, d] - a and c exist, b and d are new
        let mut columns = HashMap::new();
        let _ = columns.insert("a".to_string(), DataType::Int32);
        let _ = columns.insert("b".to_string(), DataType::Float64);
        let _ = columns.insert("c".to_string(), DataType::Float64);
        let _ = columns.insert("d".to_string(), DataType::Float64);
        let extended = null_padding_table(Arc::new(table), columns)?;

        // Schema should have 4 columns: a, c (existing), b, d (new)
        assert_eq!(extended.schema().fields().len(), 4);
        // Original columns should be first
        assert_eq!(extended.schema().field(0).name(), "a");
        assert_eq!(extended.schema().field(1).name(), "c");
        // New columns b, d should exist (order not guaranteed with HashMap)
        assert!(extended.schema().column_with_name("b").is_some());
        assert!(extended.schema().column_with_name("d").is_some());

        // Query and verify
        let ctx = SessionContext::new();
        let _ = ctx.register_table("test", extended)?;
        let df = ctx.sql("SELECT a, b, c, d FROM test").await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        let result = &results[0];
        let schema = result.schema();

        // Verify a values
        let a_idx = schema.column_with_name("a").unwrap().0;
        let a_col = result
            .column(a_idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(a_col.value(0), 10);
        assert_eq!(a_col.value(1), 20);

        // Verify b is null
        let b_idx = schema.column_with_name("b").unwrap().0;
        let b_col = result
            .column(b_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(b_col.is_null(0));
        assert!(b_col.is_null(1));

        // Verify c values
        let c_idx = schema.column_with_name("c").unwrap().0;
        let c_col = result
            .column(c_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(c_col.value(0), 100.0);
        assert_eq!(c_col.value(1), 200.0);

        // Verify d is null
        let d_idx = schema.column_with_name("d").unwrap().0;
        let d_col = result
            .column(d_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(d_col.is_null(0));
        assert!(d_col.is_null(1));

        Ok(())
    }

    #[tokio::test]
    async fn test_projection_with_new_columns() -> DataFusionResult<()> {
        // Create a table with column a
        let batch = record_batch!(("a", Int32, [1, 2, 3]))?;

        let schema = batch.schema();
        let table = MemTable::try_new(schema, vec![vec![batch]])?;

        // Extend with columns b, c
        let mut columns = HashMap::new();
        let _ = columns.insert("b".to_string(), DataType::Float64);
        let _ = columns.insert("c".to_string(), DataType::Float64);
        let extended = null_padding_table(Arc::new(table), columns)?;

        // Query with projection
        let ctx = SessionContext::new();
        let _ = ctx.register_table("test", extended)?;
        let df = ctx.sql("SELECT b, c FROM test").await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);
        assert_eq!(results[0].num_columns(), 2);

        // Both columns should be null
        let b_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let c_col = results[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for i in 0..3 {
            assert!(b_col.is_null(i));
            assert!(c_col.is_null(i));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_extend_list() -> DataFusionResult<()> {
        // Create a table
        let batch = record_batch!(("a", Int32, [1, 2]))?;

        let schema = batch.schema();
        let table = MemTable::try_new(schema, vec![vec![batch]])?;

        // Extend with empty map
        let columns = HashMap::new();
        let extended = null_padding_table(Arc::new(table), columns)?;

        // Schema should be unchanged
        assert_eq!(extended.schema().fields().len(), 1);

        let ctx = SessionContext::new();
        let _ = ctx.register_table("test", extended)?;
        let df = ctx.sql("SELECT * FROM test").await?;
        let results = df.collect().await?;

        assert_eq!(results[0].num_rows(), 2);
        assert_eq!(results[0].num_columns(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_string_datatype_columns() -> DataFusionResult<()> {
        // Create a table with int column
        let batch = record_batch!(("id", Int32, [1, 2]))?;

        let schema = batch.schema();
        let table = MemTable::try_new(schema, vec![vec![batch]])?;

        // Extend with String column
        let mut columns = HashMap::new();
        let _ = columns.insert("name".to_string(), DataType::Utf8);
        let extended = null_padding_table(Arc::new(table), columns)?;

        assert_eq!(extended.schema().fields().len(), 2);
        assert_eq!(extended.schema().field(1).data_type(), &DataType::Utf8);

        let ctx = SessionContext::new();
        let _ = ctx.register_table("test", extended)?;
        let df = ctx.sql("SELECT id, name FROM test").await?;
        let results = df.collect().await?;

        // name column should be null strings
        let name_col = results[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(name_col.is_null(0));
        assert!(name_col.is_null(1));

        Ok(())
    }

    #[tokio::test]
    async fn test_mixed_datatypes() -> DataFusionResult<()> {
        // Create a table with just an id column
        let batch = record_batch!(("id", Int32, [1, 2]))?;

        let schema = batch.schema();
        let table = MemTable::try_new(schema, vec![vec![batch]])?;

        // Extend with columns of different types
        let mut columns = HashMap::new();
        let _ = columns.insert("temperature".to_string(), DataType::Float64);
        let _ = columns.insert("location".to_string(), DataType::Utf8);
        let _ = columns.insert("count".to_string(), DataType::Int64);
        let extended = null_padding_table(Arc::new(table), columns)?;

        // Verify schema has all columns with correct types
        assert_eq!(extended.schema().fields().len(), 4);
        assert!(extended.schema().column_with_name("id").is_some());
        assert!(extended.schema().column_with_name("temperature").is_some());
        assert!(extended.schema().column_with_name("location").is_some());
        assert!(extended.schema().column_with_name("count").is_some());

        let schema = extended.schema();
        let temp_field = schema.column_with_name("temperature").unwrap().1;
        assert_eq!(temp_field.data_type(), &DataType::Float64);

        let loc_field = schema.column_with_name("location").unwrap().1;
        assert_eq!(loc_field.data_type(), &DataType::Utf8);

        let count_field = schema.column_with_name("count").unwrap().1;
        assert_eq!(count_field.data_type(), &DataType::Int64);

        Ok(())
    }

    /// Mock table provider that tracks what projection was requested
    #[derive(Debug)]
    struct SpyTableProvider {
        inner: Arc<MemTable>,
        last_projection: Arc<std::sync::Mutex<Option<Vec<usize>>>>,
    }

    #[async_trait::async_trait]
    impl TableProvider for SpyTableProvider {
        fn schema(&self) -> SchemaRef {
            self.inner.schema()
        }

        fn table_type(&self) -> TableType {
            self.inner.table_type()
        }

        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            // Record what projection was requested
            *self.last_projection.lock().unwrap() = projection.cloned();

            // Forward to inner
            self.inner.scan(state, projection, filters, limit).await
        }
    }

    #[tokio::test]
    async fn test_projection_pushdown() -> DataFusionResult<()> {
        // Create a table with multiple columns
        let batch = record_batch!(
            ("a", Int32, [1, 2, 3]),
            ("b", Float64, [1.0, 2.0, 3.0]),
            ("c", Utf8, ["x", "y", "z"])
        )?;

        let schema = batch.schema();
        let mem_table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch]])?);
        let last_projection = Arc::new(std::sync::Mutex::new(None));

        let spy = Arc::new(SpyTableProvider {
            inner: mem_table,
            last_projection: last_projection.clone(),
        });

        // Extend with new column d
        let mut columns = HashMap::new();
        let _ = columns.insert("d".to_string(), DataType::Int64);
        let extended = null_padding_table(spy.clone(), columns)?;

        let ctx = SessionContext::new();
        let _ = ctx.register_table("test", extended)?;

        // Test 1: Query only existing columns - should push down projection [0, 1] for a, b
        let df = ctx.sql("SELECT a, b FROM test").await?;
        let results = df.collect().await?;
        assert_eq!(results[0].num_columns(), 2);
        assert_eq!(results[0].num_rows(), 3);

        // Verify projection was pushed down to inner table
        let projection = last_projection.lock().unwrap().clone();
        assert!(projection.is_some(), "Projection should be pushed down");
        let proj = projection.unwrap();
        assert_eq!(proj.len(), 2, "Should project exactly 2 columns");
        assert!(proj.contains(&0), "Should include column 0 (a)");
        assert!(proj.contains(&1), "Should include column 1 (b)");
        assert!(
            !proj.contains(&2),
            "Should NOT include column 2 (c) - not requested"
        );

        // Test 2: Query only new padded column - should request column 0 for row count
        *last_projection.lock().unwrap() = None; // Reset
        let df = ctx.sql("SELECT d FROM test").await?;
        let results = df.collect().await?;
        assert_eq!(results[0].num_columns(), 1);
        assert_eq!(results[0].num_rows(), 3);

        // Verify minimal projection (just for row count)
        let projection = last_projection.lock().unwrap().clone();
        assert!(
            projection.is_some(),
            "Should request at least one column for row count"
        );
        assert_eq!(
            projection.unwrap(),
            vec![0],
            "Should request only first column for counting"
        );

        // Test 3: Query mix of existing and new columns - should project only existing
        *last_projection.lock().unwrap() = None; // Reset
        let df = ctx.sql("SELECT a, d FROM test").await?;
        let results = df.collect().await?;
        assert_eq!(results[0].num_columns(), 2);
        assert_eq!(results[0].num_rows(), 3);

        // Verify only existing column was pushed down
        let projection = last_projection.lock().unwrap().clone();
        assert!(projection.is_some(), "Projection should be pushed down");
        assert_eq!(
            projection.unwrap(),
            vec![0],
            "Should only request column 0 (a), not d which is padded"
        );

        // Test 4: Query all columns
        *last_projection.lock().unwrap() = None; // Reset
        let df = ctx.sql("SELECT * FROM test").await?;
        let results = df.collect().await?;
        assert_eq!(results[0].num_columns(), 4); // a, b, c, d
        assert_eq!(results[0].num_rows(), 3);

        // When SELECT * on extended table (with padded column d),
        // DataFusion will request projection [0,1,2,3] on the extended schema.
        // Since column d (index 3) doesn't exist in inner, we map to inner columns [0,1,2].
        // Our implementation passes Some(vec![0,1,2]) which is all inner columns.
        // Ideally this could be optimized to None, but Some([0,1,2]) is also correct.
        let projection = last_projection.lock().unwrap().clone();
        if let Some(proj) = projection {
            // If we do push down projection, it should be all 3 inner columns
            assert_eq!(proj.len(), 3, "Should request all 3 inner columns");
            assert!(proj.contains(&0) && proj.contains(&1) && proj.contains(&2));
        }
        // Either None or Some([0,1,2]) is acceptable for scanning all columns

        // Test 5: COUNT(*) projects no output columns through the padding plan,
        // but its input batches must retain their row counts.
        *last_projection.lock().unwrap() = None;
        let df = ctx.sql("SELECT COUNT(*) AS count FROM test").await?;
        let results = df.collect().await?;
        assert_eq!(results[0].num_columns(), 1);
        assert_eq!(results[0].num_rows(), 1);
        assert_eq!(
            results[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            3
        );
        assert_eq!(
            last_projection.lock().unwrap().clone(),
            Some(vec![0]),
            "empty output projection still needs one inner column for row count"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_filters_and_limit() -> DataFusionResult<()> {
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::file::properties::WriterProperties;
        use std::fs::File;
        use tempfile::TempDir;

        // Create a temporary directory and parquet file
        let temp_dir = TempDir::new().unwrap();
        let parquet_path = temp_dir.path().join("test.parquet");

        // Create test data
        let batch = record_batch!(("value", Int32, [1, 2, 3, 4, 5]))?;

        // Write to parquet file
        let file = File::create(&parquet_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        let _ = writer.close().unwrap();

        // Register parquet file as table with filter pushdown enabled
        let mut cfg = SessionConfig::default();
        cfg.options_mut().execution.parquet.pushdown_filters = true;
        cfg.options_mut().execution.parquet.reorder_filters = true;
        let ctx = SessionContext::new_with_config(cfg);
        ctx.register_parquet(
            "inner_table",
            parquet_path.to_str().unwrap(),
            Default::default(),
        )
        .await?;

        // Get the parquet table provider
        let parquet_provider = ctx.table_provider("inner_table").await?;

        // Extend with extra column
        let mut columns = HashMap::new();
        let _ = columns.insert("extra".to_string(), DataType::Float64);
        let extended = null_padding_table(parquet_provider, columns)?;

        let _ = ctx.register_table("test", extended)?;

        // Test with filter and limit - parquet supports filter pushdown
        let df = ctx
            .sql("SELECT value, extra FROM test WHERE value > 2 LIMIT 2")
            .await?;
        let results = df.collect().await?;

        // Should get 2 rows: [3, 4]
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        let value_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(value_col.value(0), 3);
        assert_eq!(value_col.value(1), 4);

        let extra_col = results[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(extra_col.is_null(0));
        assert!(extra_col.is_null(1));

        // Test filter on padded column - should NOT be pushed down, evaluated locally
        let df = ctx
            .sql("SELECT value, extra FROM test WHERE extra IS NULL")
            .await?;
        let results = df.collect().await?;

        // All rows should pass (extra is always NULL)
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_simplification() -> DataFusionResult<()> {
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::file::properties::WriterProperties;
        use std::fs::File;
        use tempfile::TempDir;

        // Create a temporary directory and parquet file
        let temp_dir = TempDir::new().unwrap();
        let parquet_path = temp_dir.path().join("test.parquet");

        // Create test data
        let batch = record_batch!(("value", Int32, [1, 2, 3, 4, 5]))?;

        // Write to parquet file
        let file = File::create(&parquet_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        let _ = writer.close().unwrap();

        // Register parquet file as table with filter pushdown enabled
        let mut cfg = SessionConfig::default();
        cfg.options_mut().execution.parquet.pushdown_filters = true;
        cfg.options_mut().execution.parquet.reorder_filters = true;
        let ctx = SessionContext::new_with_config(cfg);
        ctx.register_parquet(
            "inner_table",
            parquet_path.to_str().unwrap(),
            Default::default(),
        )
        .await?;

        // Get the parquet table provider
        let parquet_provider = ctx.table_provider("inner_table").await?;

        // Extend with extra nullable column using null_padding
        let mut columns = HashMap::new();
        let _ = columns.insert("extra".to_string(), DataType::Float64);
        let extended = null_padding_table(parquet_provider, columns)?;

        let _ = ctx.register_table("test", extended)?;

        // Test that filter simplification works correctly
        // DataFusion's ExprSimplifier + our filter classification handles this:
        // - "extra IS NOT NULL" on always-NULL column -> 0 rows
        // - "extra IS NULL" on always-NULL column -> all rows

        let df = ctx
            .sql("SELECT value FROM test WHERE extra IS NOT NULL")
            .await?;
        let results = df.collect().await?;
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 0,
            "IS NOT NULL on padded column should return no rows"
        );

        let df = ctx
            .sql("SELECT value FROM test WHERE extra IS NULL")
            .await?;
        let results = df.collect().await?;
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 5,
            "IS NULL on padded column should return all rows"
        );

        Ok(())
    }
}
