// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Conservative planning boundary for user-authored SQL.

use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::DFSchema;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{SQLOptions, SessionContext};
use datafusion::logical_expr::{Expr, LogicalPlan, Volatility};
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{Ident, ObjectName, ObjectNamePart, Query, Visit, Visitor};
use datafusion::sql::sqlparser::dialect::GenericDialect;

use crate::locality::{LocalityClass, LocalityContract};

/// Explicit locality declaration attached to user SQL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserSqlDeclaration {
    locality: LocalityContract,
    sources: BTreeSet<Arc<str>>,
    event_time: Option<Arc<str>>,
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
            declaration.timestamp_grouped.then_some(
                declaration
                    .event_time
                    .as_deref()
                    .expect("local declaration has event time"),
            ),
        )
        .await?;
    }
    if declaration.locality.class() == LocalityClass::TimestampLocal {
        let event_time = declaration
            .event_time
            .as_deref()
            .expect("timestamp-local declaration has event time");
        _ = frame
            .schema()
            .field_with_unqualified_name(event_time)
            .map_err(|_| {
                DataFusionError::Plan(format!(
                    "timestamp-local SQL output must retain event-time column '{event_time}'"
                ))
            })?;
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
    grouped_time: Option<&str>,
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
    validate_local_plan(frame.logical_plan(), grouped_time)
}

#[cfg(test)]
fn validate_timestamp_local_plan(plan: &LogicalPlan) -> Result<()> {
    validate_local_plan(plan, None)
}

fn validate_local_plan(plan: &LogicalPlan, grouped_time: Option<&str>) -> Result<()> {
    let mode = if grouped_time.is_some() {
        "timestamp-grouped"
    } else {
        "timestamp-local"
    };
    let mut saw_aggregate = false;
    _ = plan.apply(|node| {
        let grouped_aggregate = if let (Some(event_time), LogicalPlan::Aggregate(aggregate)) = (grouped_time, node) {
            if !matches!(aggregate.group_expr.as_slice(), [Expr::Column(column)] if column.name == event_time) {
                return Err(DataFusionError::Plan(format!(
                    "timestamp-grouped SQL must GROUP BY only the exact event-time column '{event_time}'"
                )));
            }
            let field = aggregate.input.schema().field_with_unqualified_name(event_time)?;
            if !matches!(field.data_type(), DataType::Int64 | DataType::Timestamp(_, _)) {
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
        if let Some(event_time) = grouped_time {
            _ = node.schema().field_with_unqualified_name(event_time).map_err(|_| {
                DataFusionError::Plan(format!("timestamp-grouped SQL must retain '{event_time}' at every stage"))
            })?;
            if let LogicalPlan::Projection(projection) = node {
                let mut positions = projection.schema.fields().iter().enumerate()
                    .filter(|(_, field)| field.name() == event_time);
                let (index, _) = positions.next().expect("event-time field checked above");
                if positions.next().is_some() || !preserves_group_time(
                    &projection.expr[index], projection.input.schema(), event_time,
                )? {
                    return Err(DataFusionError::Plan(format!(
                        "timestamp-grouped SQL must preserve the value and name of '{event_time}'"
                    )));
                }
            }
        }
        for expression in node.expressions() {
            validate_local_expression(&expression, grouped_aggregate)?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if let Some(event_time) = grouped_time {
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

fn preserves_group_time(expression: &Expr, input: &DFSchema, event_time: &str) -> Result<bool> {
    match expression {
        Expr::Alias(alias) => preserves_group_time(&alias.expr, input, event_time),
        Expr::Column(column) => Ok(column.name == event_time),
        Expr::Cast(cast) if matches!(cast.expr.as_ref(), Expr::Column(column) if column.name == event_time) =>
        {
            let input_type = input.field_with_unqualified_name(event_time)?.data_type();
            // Parse the existing RFC3339 source representation without reducing
            // timestamp precision. Other conversions require their own proof.
            Ok(input_type == cast.field.data_type()
                || (matches!(
                    input_type,
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                ) && matches!(
                    cast.field.data_type(),
                    DataType::Timestamp(TimeUnit::Nanosecond, _)
                )))
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
        let error = validate_timestamp_local_plan(&LogicalPlan::Sort(sort))
            .expect_err("a top-k sort must not be timestamp-local");
        assert!(error.to_string().contains("Sort"), "{error}");
    }
}
