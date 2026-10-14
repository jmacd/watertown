// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! SQL transformation utilities for pattern-based table name replacement
//!
//! Provides AST-based SQL transformation to replace table references while preserving
//! column names, string literals, and query structure.

use crate::factory::sql_derived::SqlTransformOptions;
use datafusion::sql::parser::DFParser;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{Query, Select, SetExpr, TableFactor};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use log::debug;
use std::collections::HashMap;

/// Transform SQL query by replacing table names using AST manipulation
///
/// This ensures we only replace actual table references, not column names or string literals.
/// Falls back to original SQL if parsing fails.
#[must_use]
pub fn transform_sql(original_sql: &str, options: &SqlTransformOptions) -> String {
    // If no transformations are needed, return original
    if options.table_mappings.is_none() && options.source_replacement.is_none() {
        return original_sql.to_string();
    }

    let dialect = GenericDialect {};

    // Parse the SQL
    let statements = match DFParser::parse_sql_with_dialect(original_sql, &dialect) {
        Ok(stmts) => stmts,
        Err(e) => {
            log::warn!(
                "Failed to parse SQL for transformation, falling back to original: {}",
                e
            );
            return original_sql.to_string();
        }
    };

    if statements.is_empty() {
        return original_sql.to_string();
    }

    // Get the first statement (we only expect one SELECT)
    let mut statement = statements[0].clone();

    // Apply transformations
    replace_table_names_in_statement(
        &mut statement,
        options.table_mappings.as_ref(),
        options.source_replacement.as_deref(),
    );

    // Unparse the transformed AST back to SQL
    statement.to_string()
}

fn replace_table_names_in_statement(
    statement: &mut DFStatement,
    table_mappings: Option<&HashMap<String, String>>,
    source_replacement: Option<&str>,
) {
    if let DFStatement::Statement(boxed) = statement
        && let datafusion::sql::sqlparser::ast::Statement::Query(query) = boxed.as_mut()
    {
        replace_table_names_in_query(query, table_mappings, source_replacement);
    }
}

fn replace_table_names_in_query(
    query: &mut Box<Query>,
    table_mappings: Option<&HashMap<String, String>>,
    source_replacement: Option<&str>,
) {
    // Handle CTEs (WITH clauses)
    if let Some(ref mut with) = query.with {
        for cte_table in &mut with.cte_tables {
            replace_table_names_in_query(&mut cte_table.query, table_mappings, source_replacement);
        }
    }

    // Handle main query body
    replace_table_names_in_set_expr(&mut query.body, table_mappings, source_replacement);
}

fn replace_table_names_in_set_expr(
    set_expr: &mut SetExpr,
    table_mappings: Option<&HashMap<String, String>>,
    source_replacement: Option<&str>,
) {
    match set_expr {
        SetExpr::Select(select) => {
            replace_table_names_in_select(select, table_mappings, source_replacement);
        }
        SetExpr::SetOperation { left, right, .. } => {
            // Handle UNION, INTERSECT, EXCEPT operations
            replace_table_names_in_set_expr(left, table_mappings, source_replacement);
            replace_table_names_in_set_expr(right, table_mappings, source_replacement);
        }
        SetExpr::Query(query) => {
            replace_table_names_in_query(query, table_mappings, source_replacement);
        }
        _ => {}
    }
}

fn replace_table_names_in_select(
    select: &mut Box<Select>,
    table_mappings: Option<&HashMap<String, String>>,
    source_replacement: Option<&str>,
) {
    // Replace in FROM clause
    for table_with_joins in &mut select.from {
        replace_table_name(
            &mut table_with_joins.relation,
            table_mappings,
            source_replacement,
        );

        // Replace in JOINs
        for join in &mut table_with_joins.joins {
            replace_table_name(&mut join.relation, table_mappings, source_replacement);
        }
    }
}

fn replace_table_name(
    table_factor: &mut TableFactor,
    table_mappings: Option<&HashMap<String, String>>,
    source_replacement: Option<&str>,
) {
    if let TableFactor::Table { name, alias, .. } = table_factor {
        let table_name = name.to_string();

        if let Some(mappings) = table_mappings {
            if let Some(replacement) = mappings.get(&table_name) {
                debug!(
                    "Replacing table reference '{}' with '{}' in AST",
                    table_name, replacement
                );
                use datafusion::sql::sqlparser::ast::{
                    Ident, ObjectName, ObjectNamePart, TableAlias,
                };

                // If no existing alias, add one using the original table name
                // This allows column references like "sensor_a.timestamp" to still work
                if alias.is_none() {
                    *alias = Some(TableAlias {
                        explicit: true,
                        name: Ident::new(&table_name),
                        columns: vec![],
                        at: None,
                    });
                }

                *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                    replacement.clone(),
                ))]);
            }
        } else if let Some(replacement) = source_replacement
            && table_name == "source"
        {
            debug!("Replacing 'source' with '{}' in AST", replacement);
            use datafusion::sql::sqlparser::ast::{Ident, ObjectName, ObjectNamePart};
            *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(replacement))]);
        }
    } else if let TableFactor::Derived { subquery, .. } = table_factor {
        // Recursively handle subqueries
        replace_table_names_in_query(subquery, table_mappings, source_replacement);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_source_replacement() {
        let sql = "SELECT * FROM source";
        let options = SqlTransformOptions {
            table_mappings: None,
            source_replacement: Some("actual_table".to_string()),
        };

        let result = transform_sql(sql, &options);
        assert!(result.contains("actual_table"));
        assert!(!result.contains("source"));
    }

    #[test]
    fn test_table_mappings() {
        let sql = "SELECT * FROM table_a JOIN table_b ON table_a.id = table_b.id";
        let options = SqlTransformOptions {
            table_mappings: Some(
                [
                    ("table_a".to_string(), "real_a".to_string()),
                    ("table_b".to_string(), "real_b".to_string()),
                ]
                .into(),
            ),
            source_replacement: None,
        };

        let result = transform_sql(sql, &options);
        assert!(result.contains("real_a"));
        assert!(result.contains("real_b"));
    }

    #[test]
    fn test_no_transformation_needed() {
        let sql = "SELECT * FROM source";
        let options = SqlTransformOptions::default();

        let result = transform_sql(sql, &options);
        assert_eq!(result, sql);
    }
}
