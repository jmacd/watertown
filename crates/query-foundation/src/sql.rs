// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Conservative planning boundary for user-authored SQL.

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{SQLOptions, SessionContext};
use datafusion::logical_expr::{Expr, LogicalPlan, Volatility};

use crate::locality::{LocalityClass, LocalityContract};

/// Explicit locality declaration attached to user SQL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserSqlDeclaration {
    locality: LocalityContract,
    sources: BTreeSet<Arc<str>>,
    event_time: Option<Arc<str>>,
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

    /// Declare a single-source SQL projection/filter as timestamp-local.
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
    validate_sources(frame.logical_plan(), &declaration.sources)?;
    if declaration.locality.class() == LocalityClass::TimestampLocal {
        validate_timestamp_local(frame.logical_plan())?;
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

fn validate_sources(plan: &LogicalPlan, declared: &BTreeSet<Arc<str>>) -> Result<()> {
    let mut planned = BTreeSet::new();
    _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            _ = planned.insert(Arc::<str>::from(scan.table_name.to_string()));
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if &planned != declared {
        return Err(DataFusionError::Plan(format!(
            "user SQL source set {planned:?} does not match declared sources {declared:?}"
        )));
    }
    Ok(())
}

fn validate_timestamp_local(plan: &LogicalPlan) -> Result<()> {
    _ = plan.apply(|node| {
        if !matches!(
            node,
            LogicalPlan::Projection(_)
                | LogicalPlan::Filter(_)
                | LogicalPlan::SubqueryAlias(_)
                | LogicalPlan::TableScan(_)
        ) {
            return Err(DataFusionError::Plan(format!(
                "timestamp-local SQL does not support logical operator {}",
                logical_operator_name(node)
            )));
        }
        for expression in node.expressions() {
            validate_local_expression(&expression)?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

fn validate_local_expression(expression: &Expr) -> Result<()> {
    _ = expression.apply(|nested| {
        match nested {
            Expr::AggregateFunction(_)
            | Expr::WindowFunction(_)
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
