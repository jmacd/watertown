// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Conservative planning boundary for user-authored SQL.

use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{SQLOptions, SessionContext};
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, SortExpr as Sort, Volatility};
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{Ident, ObjectName, ObjectNamePart, Query, Visit, Visitor};
use datafusion::sql::sqlparser::dialect::GenericDialect;

use crate::locality::{LocalityClass, LocalityContract};

/// Proven representation of an external source's textual event time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventTimeEncoding {
    Rfc3339,
    Microseconds,
}

/// Explicit locality declaration attached to user SQL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserSqlDeclaration {
    locality: LocalityContract,
    sources: BTreeSet<Arc<str>>,
    event_time: Option<Arc<str>>,
    input_event_time: Option<Arc<str>>,
    input_encoding: Option<EventTimeEncoding>,
    timestamp_grouped: bool,
}

impl UserSqlDeclaration {
    /// Declare unrestricted SQL as global.
    pub fn global<I, S>(sources: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<Arc<str>>,
    {
        Self::try_new(LocalityClass::Global, sources, None)
    }

    /// Declare a single-source projection/filter, optionally sorted without
    /// limiting rows, as timestamp-local.
    pub fn timestamp_local(
        source: impl Into<Arc<str>>,
        event_time: impl Into<Arc<str>>,
    ) -> Result<Self> {
        Self::try_new(
            LocalityClass::TimestampLocal,
            [source.into()],
            Some(event_time.into()),
        )
    }

    /// Declare aggregation within complete groups of one exact timestamp.
    pub fn timestamp_grouped(
        source: impl Into<Arc<str>>,
        event_time: impl Into<Arc<str>>,
    ) -> Result<Self> {
        let mut declaration = Self::timestamp_local(source, event_time)?;
        declaration.timestamp_grouped = true;
        Ok(declaration)
    }

    /// Declare the source event-time column when SQL renames it. Int64 time is
    /// epoch microseconds; text may be parsed as RFC3339 or integer microseconds.
    pub fn with_input_event_time(mut self, input_event_time: impl Into<Arc<str>>) -> Result<Self> {
        let input_event_time = input_event_time.into();
        if self.event_time.is_none() || input_event_time.is_empty() {
            return Err(DataFusionError::Plan(
                "input event time requires a local declaration and a nonempty column".to_owned(),
            ));
        }
        self.input_event_time = Some(input_event_time);
        Ok(self)
    }

    #[must_use]
    pub fn with_input_encoding(mut self, encoding: EventTimeEncoding) -> Self {
        self.input_encoding = Some(encoding);
        self
    }

    fn try_new<I, S>(class: LocalityClass, sources: I, event_time: Option<Arc<str>>) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<Arc<str>>,
    {
        let sources = sources.into_iter().map(Into::into).collect::<Vec<_>>();
        let locality = LocalityContract::try_new(class, sources.clone())?;
        if event_time.as_ref().is_some_and(|name| name.is_empty()) {
            return Err(DataFusionError::Plan(
                "local SQL event-time column must not be empty".to_owned(),
            ));
        }
        Ok(Self {
            locality,
            sources: sources.into_iter().collect(),
            input_event_time: event_time.clone(),
            input_encoding: None,
            event_time,
            timestamp_grouped: false,
        })
    }
}

/// Planned read-only SQL and its validated locality contract.
pub struct PlannedUserSql {
    frame: DataFrame,
    locality: LocalityContract,
}

impl PlannedUserSql {
    /// Validated locality used for change impact and execution policy.
    #[must_use]
    pub fn locality(&self) -> &LocalityContract {
        &self.locality
    }

    /// Consume the wrapper and return the executable DataFrame.
    #[must_use]
    pub fn into_frame(self) -> DataFrame {
        self.frame
    }
}

/// Plan read-only user SQL without inferring a stronger locality contract.
pub async fn plan_user_sql(
    context: &SessionContext,
    sql: &str,
    declaration: UserSqlDeclaration,
) -> Result<PlannedUserSql> {
    if sql.trim().is_empty() {
        return Err(DataFusionError::Plan(
            "user SQL must not be empty".to_owned(),
        ));
    }
    let options = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);
    let frame = context.sql_with_options(sql, options).await?;
    let parsed = parse_user_query(sql)?;
    validate_sources(&parsed, &declaration.sources)?;
    if declaration.locality.class() == LocalityClass::TimestampLocal {
        validate_timestamp_local_sql(
            context,
            sql,
            &declaration.sources,
            declaration.event_time.as_deref().expect("local event time"),
            declaration
                .input_event_time
                .as_deref()
                .expect("local input event time"),
            declaration.timestamp_grouped,
            declaration.input_encoding,
        )
        .await?;
    }
    Ok(PlannedUserSql {
        frame,
        locality: declaration.locality,
    })
}

fn parse_user_query(sql: &str) -> Result<Box<Query>> {
    let dialect = GenericDialect {};
    let mut statements = DFParser::parse_sql_with_dialect(sql, &dialect)
        .map_err(|error| DataFusionError::Plan(format!("failed to inspect user SQL: {error}")))?;
    if statements.len() != 1 {
        return Err(DataFusionError::Plan(
            "user SQL must contain exactly one query".to_owned(),
        ));
    }
    let Some(DFStatement::Statement(statement)) = statements.pop_front() else {
        return Err(DataFusionError::Plan(
            "user SQL must be a standard query".to_owned(),
        ));
    };
    let datafusion::sql::sqlparser::ast::Statement::Query(query) = statement.as_ref() else {
        return Err(DataFusionError::Plan("user SQL must be a query".to_owned()));
    };
    Ok(query.clone())
}

fn validate_sources(query: &Query, declared: &BTreeSet<Arc<str>>) -> Result<()> {
    let mut visitor = SourceVisitor::default();
    if query.visit(&mut visitor).is_break() {
        return Err(DataFusionError::Internal(
            "user SQL source inspection ended unexpectedly".to_owned(),
        ));
    }
    let planned = visitor.sources;
    if &planned != declared {
        return Err(DataFusionError::Plan(format!(
            "user SQL source set {planned:?} does not match declared sources {declared:?}"
        )));
    }
    Ok(())
}

#[derive(Default)]
struct SourceVisitor {
    sources: BTreeSet<Arc<str>>,
    cte_scopes: Vec<BTreeSet<String>>,
}

impl Visitor for SourceVisitor {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        let aliases = query
            .with
            .iter()
            .flat_map(|with| &with.cte_tables)
            .map(|cte| normalize_ident(&cte.alias.name))
            .collect();
        self.cte_scopes.push(aliases);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        _ = self.cte_scopes.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        let name = normalize_object_name(relation);
        let is_cte = !name.contains('.')
            && self
                .cte_scopes
                .iter()
                .rev()
                .any(|scope| scope.contains(&name));
        if !is_cte {
            _ = self.sources.insert(name.into());
        }
        ControlFlow::Continue(())
    }
}

fn normalize_object_name(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(ident) => normalize_ident(ident),
            ObjectNamePart::Function(function) => function.to_string(),
        })
        .collect::<Vec<_>>()
        .join(".")
}

fn normalize_ident(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

async fn validate_timestamp_local_sql(
    context: &SessionContext,
    sql: &str,
    sources: &BTreeSet<Arc<str>>,
    event_time: &str,
    input_event_time: &str,
    grouped: bool,
    input_encoding: Option<EventTimeEncoding>,
) -> Result<()> {
    let validation = SessionContext::new();
    for source in sources {
        let schema = context
            .table(source.to_string())
            .await?
            .schema()
            .as_arrow()
            .clone();
        _ = validation.register_table(
            source.to_string(),
            Arc::new(datafusion::datasource::empty::EmptyTable::new(Arc::new(
                schema,
            ))),
        )?;
    }
    let options = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);
    let frame = validation.sql_with_options(sql, options).await?;
    validate_local_plan(
        frame.logical_plan(),
        event_time,
        input_event_time,
        grouped,
        input_encoding,
    )
}

fn validate_local_plan(
    plan: &LogicalPlan,
    event_time: &str,
    input_event_time: &str,
    grouped: bool,
    input_encoding: Option<EventTimeEncoding>,
) -> Result<()> {
    let mode = if grouped {
        "timestamp-grouped"
    } else {
        "timestamp-local"
    };
    let mut saw_aggregate = false;
    _ = plan.apply(|node| {
        let grouped_aggregate = if let (true, LogicalPlan::Aggregate(aggregate)) = (grouped, node) {
            if !matches!(aggregate.group_expr.as_slice(), [Expr::Column(_)])
                || !preserves_time_expression(&aggregate.group_expr[0], &aggregate.input, input_event_time, input_encoding)?
            {
                return Err(DataFusionError::Plan(format!(
                    "timestamp-grouped SQL must GROUP BY only the exact event-time column '{event_time}'"
                )));
            }
            let data_type = aggregate.group_expr[0].get_type(aggregate.input.schema())?;
            if !matches!(data_type, DataType::Int64 | DataType::Timestamp(_, _)) {
                return Err(DataFusionError::Plan(
                    "timestamp-grouped SQL must parse event time before grouping".to_owned(),
                ));
            }
            saw_aggregate = true;
            true
        } else {
            false
        };
        // A full sort commutes with time restriction; a top-k sort does not.
        if !matches!(
            node,
            LogicalPlan::Projection(_)
                | LogicalPlan::Filter(_)
                | LogicalPlan::SubqueryAlias(_)
                | LogicalPlan::TableScan(_)
        ) && !matches!(node, LogicalPlan::Sort(sort) if sort.fetch.is_none()) && !grouped_aggregate
        {
            return Err(DataFusionError::Plan(format!(
                "{mode} SQL does not support logical operator {}",
                logical_operator_name(node)
            )));
        }
        for expression in node.expressions() {
            validate_local_expression(&expression, grouped_aggregate)?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    let index = plan
        .schema()
        .index_of_column(&datafusion::common::Column::from_name(event_time))
        .map_err(|_| {
            DataFusionError::Plan(format!(
                "{mode} SQL output must retain event-time column '{event_time}'"
            ))
        })?;
    if !preserves_time_column(plan, index, input_event_time, input_encoding)? {
        return Err(DataFusionError::Plan(format!(
            "{mode} SQL must preserve input event-time column '{input_event_time}' as '{event_time}'"
        )));
    }
    if grouped {
        if !saw_aggregate {
            return Err(DataFusionError::Plan(
                "timestamp-grouped SQL requires an exact-time aggregation".to_owned(),
            ));
        }
        let field = plan.schema().field_with_unqualified_name(event_time)?;
        if !matches!(
            field.data_type(),
            DataType::Int64 | DataType::Timestamp(_, _)
        ) {
            return Err(DataFusionError::Plan(
                "timestamp-grouped SQL requires typed event-time output".to_owned(),
            ));
        }
    }
    Ok(())
}

// Trace through user projections/CTEs, stopping at the schema-only declared
// source. Checking only the last projection would allow hidden time changes.
fn preserves_time_column(
    plan: &LogicalPlan,
    index: usize,
    event_time: &str,
    encoding: Option<EventTimeEncoding>,
) -> Result<bool> {
    match plan {
        LogicalPlan::TableScan(_) => Ok(plan.schema().field(index).name() == event_time),
        LogicalPlan::Projection(projection) => preserves_time_expression(
            &projection.expr[index],
            &projection.input,
            event_time,
            encoding,
        ),
        LogicalPlan::Aggregate(aggregate) => match aggregate.group_expr.get(index) {
            Some(expression) => {
                preserves_time_expression(expression, &aggregate.input, event_time, encoding)
            }
            None => Ok(false),
        },
        LogicalPlan::Filter(filter) => {
            preserves_time_column(&filter.input, index, event_time, encoding)
        }
        LogicalPlan::Sort(sort) => preserves_time_column(&sort.input, index, event_time, encoding),
        LogicalPlan::SubqueryAlias(alias) => {
            preserves_time_column(&alias.input, index, event_time, encoding)
        }
        _ => Ok(false),
    }
}

fn preserves_time_expression(
    expression: &Expr,
    input: &LogicalPlan,
    event_time: &str,
    encoding: Option<EventTimeEncoding>,
) -> Result<bool> {
    match expression {
        Expr::Alias(alias) => preserves_time_expression(&alias.expr, input, event_time, encoding),
        Expr::Column(column) => preserves_time_column(
            input,
            input.schema().index_of_column(column)?,
            event_time,
            encoding,
        ),
        Expr::Cast(cast) => {
            let input_type = cast.expr.get_type(input.schema())?;
            let preserving_cast = &input_type == cast.field.data_type()
                || (matches!(
                    input_type,
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                ) && encoding != Some(EventTimeEncoding::Microseconds)
                    && matches!(
                        cast.field.data_type(),
                        DataType::Timestamp(TimeUnit::Nanosecond, _)
                    ));
            Ok(preserving_cast
                && preserves_time_expression(&cast.expr, input, event_time, encoding)?)
        }
        Expr::ScalarFunction(function)
            if function.func.name() == "to_timestamp_micros" && function.args.len() == 1 =>
        {
            let argument = &function.args[0];
            if encoding == Some(EventTimeEncoding::Rfc3339)
                || argument.get_type(input.schema())? != DataType::Int64
            {
                return Ok(false);
            }
            // Journal timestamps are strings containing integer microseconds.
            // Do not admit float casts, arithmetic, or timestamp downcasts.
            let argument = match argument {
                Expr::Cast(cast)
                    if matches!(
                        cast.expr.get_type(input.schema())?,
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                    ) =>
                {
                    &cast.expr
                }
                _ => argument,
            };
            preserves_time_expression(argument, input, event_time, encoding)
        }
        _ => Ok(false),
    }
}

fn validate_local_expression(expression: &Expr, allow_aggregate: bool) -> Result<()> {
    _ = expression.apply(|nested| {
        match nested {
            Expr::AggregateFunction(_) if !allow_aggregate => {
                return Err(DataFusionError::Plan(format!(
                    "timestamp-local SQL contains unsupported expression {nested}"
                )));
            }
            Expr::AggregateFunction(function)
                if function.func.signature().volatility != Volatility::Immutable =>
            {
                return Err(DataFusionError::Plan(format!(
                    "timestamp-grouped SQL aggregate '{}' is not immutable",
                    function.func.name()
                )));
            }
            Expr::AggregateFunction(function) => {
                validate_aggregate_order(function.func.name(), &function.params.order_by)?;
            }
            Expr::WindowFunction(_)
            | Expr::Exists(_)
            | Expr::InSubquery(_)
            | Expr::ScalarSubquery(_)
            | Expr::GroupingSet(_)
            | Expr::OuterReferenceColumn(_, _)
            | Expr::Unnest(_) => {
                return Err(DataFusionError::Plan(format!(
                    "timestamp-local SQL contains unsupported expression {nested}"
                )));
            }
            Expr::ScalarFunction(function)
                if function.func.signature().volatility != Volatility::Immutable =>
            {
                return Err(DataFusionError::Plan(format!(
                    "timestamp-local SQL function '{}' is not immutable",
                    function.name()
                )));
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

/// Aggregates whose result depends on the order rows reach them. Scan order
/// within a timestamp group is not stable across incremental, repacked, or
/// repartitioned reads, so these need an explicit ORDER BY to be reproducible.
const ORDER_SENSITIVE_AGGREGATES: &[&str] = &[
    "first_value",
    "last_value",
    "nth_value",
    "array_agg",
    "string_agg",
];

fn validate_aggregate_order(name: &str, order_by: &[Sort]) -> Result<()> {
    let name = name.to_ascii_lowercase();
    if name == "any_value" {
        return Err(DataFusionError::Plan(
            "timestamp-grouped SQL aggregate 'any_value' picks an arbitrary row; \
             use an order-insensitive aggregate such as MAX, or FIRST_VALUE(... ORDER BY ...)"
                .to_string(),
        ));
    }
    if order_by.is_empty() && ORDER_SENSITIVE_AGGREGATES.contains(&name.as_str()) {
        return Err(DataFusionError::Plan(format!(
            "timestamp-grouped SQL aggregate '{name}' depends on input order; \
             add an ORDER BY inside the aggregate, e.g. {name}(value ORDER BY other_column)"
        )));
    }
    Ok(())
}

fn logical_operator_name(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Projection(_) => "Projection",
        LogicalPlan::Filter(_) => "Filter",
        LogicalPlan::Window(_) => "Window",
        LogicalPlan::Aggregate(_) => "Aggregate",
        LogicalPlan::Sort(_) => "Sort",
        LogicalPlan::Join(_) => "Join",
        LogicalPlan::Repartition(_) => "Repartition",
        LogicalPlan::Union(_) => "Union",
        LogicalPlan::TableScan(_) => "TableScan",
        LogicalPlan::EmptyRelation(_) => "EmptyRelation",
        LogicalPlan::Subquery(_) => "Subquery",
        LogicalPlan::SubqueryAlias(_) => "SubqueryAlias",
        LogicalPlan::Limit(_) => "Limit",
        LogicalPlan::Statement(_) => "Statement",
        LogicalPlan::Values(_) => "Values",
        LogicalPlan::Explain(_) => "Explain",
        LogicalPlan::Analyze(_) => "Analyze",
        LogicalPlan::Extension(_) => "Extension",
        LogicalPlan::Distinct(_) => "Distinct",
        LogicalPlan::Dml(_) => "Dml",
        LogicalPlan::Ddl(_) => "Ddl",
        LogicalPlan::Copy(_) => "Copy",
        LogicalPlan::DescribeTable(_) => "DescribeTable",
        LogicalPlan::Unnest(_) => "Unnest",
        LogicalPlan::RecursiveQuery(_) => "RecursiveQuery",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::view::ViewTable;
    use datafusion::datasource::MemTable;

    fn empty_table() -> Arc<MemTable> {
        Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Int64,
                    true,
                )])),
                vec![vec![]],
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn declared_view_source_ignores_internal_anonymous_scan() {
        let context = SessionContext::new();
        _ = context.register_table("inner", empty_table()).unwrap();
        let inner = context.table("inner").await.unwrap();
        let view = Arc::new(ViewTable::new(
            inner.logical_plan().clone(),
            Some("nested typed plan".to_owned()),
        ));
        _ = context.register_table("declared", view).unwrap();

        _ = plan_user_sql(
            &context,
            "WITH filtered AS (SELECT value FROM declared) SELECT * FROM filtered",
            UserSqlDeclaration::global(["declared"]).unwrap(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn undeclared_source_is_rejected_before_provider_expansion() {
        let context = SessionContext::new();
        _ = context.register_table("declared", empty_table()).unwrap();
        _ = context.register_table("other", empty_table()).unwrap();

        let error = match plan_user_sql(
            &context,
            "SELECT value FROM other",
            UserSqlDeclaration::global(["declared"]).unwrap(),
        )
        .await
        {
            Ok(_) => panic!("undeclared source must fail"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("user SQL source set {\"other\"} does not match declared sources")
        );
    }

    #[tokio::test]
    async fn timestamp_local_validation_stops_at_declared_view_boundary() {
        let context = SessionContext::new();
        _ = context
            .register_table("left_source", empty_table())
            .unwrap();
        _ = context
            .register_table("right_source", empty_table())
            .unwrap();
        let joined = context
            .sql(
                "SELECT left_source.value \
                 FROM left_source JOIN right_source \
                 ON left_source.value = right_source.value",
            )
            .await
            .unwrap();
        let view = Arc::new(ViewTable::new(
            joined.logical_plan().clone(),
            Some("typed nested join".to_owned()),
        ));
        _ = context.register_table("declared", view).unwrap();

        _ = plan_user_sql(
            &context,
            "SELECT value FROM declared WHERE value IS NOT NULL",
            UserSqlDeclaration::timestamp_local("declared", "value").unwrap(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn timestamp_local_validation_rejects_aggregate_sql() {
        let context = SessionContext::new();
        _ = context.register_table("declared", empty_table()).unwrap();

        let error = match plan_user_sql(
            &context,
            "SELECT value, SUM(value) AS total FROM declared GROUP BY value",
            UserSqlDeclaration::timestamp_local("declared", "value").unwrap(),
        )
        .await
        {
            Ok(_) => panic!("aggregate SQL must not be timestamp-local"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("Aggregate"));
    }

    #[tokio::test]
    async fn timestamp_local_validation_rejects_a_sort_with_fetch() {
        let context = SessionContext::new();
        _ = context.register_table("declared", empty_table()).unwrap();
        let frame = context
            .sql("SELECT value FROM declared ORDER BY value")
            .await
            .unwrap();
        let LogicalPlan::Sort(mut sort) = frame.logical_plan().clone() else {
            panic!("expected a sort plan");
        };
        sort.fetch = Some(1);
        let error = validate_local_plan(&LogicalPlan::Sort(sort), "value", "value", false, None)
            .expect_err("a top-k sort must not be timestamp-local");
        assert!(error.to_string().contains("Sort"), "{error}");
    }
}
