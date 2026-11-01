// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Column Rename Transform Factory
//!
//! This factory creates table-provider transformations that rename columns
//! according to rules. It is an executable factory with ExecutionMode::TableTransform.
//!
//! The factory is "executed" by other factories (like sql_derived) when they apply
//! transform chains. The execute function receives the config and returns transformation
//! logic that can be applied to a TableProvider.
//!
//! The configuration supports:
//! - Direct column name mappings: "DateTime" -> "timestamp"
//! - Regex pattern replacements: "Name (Unit)" -> "Name.Unit"

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tinyfs::Result as TinyFSResult;
use tinyfs::ResultExt;

// ============================================================================
// Configuration Types
// ============================================================================

/// A single column rename rule
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
#[cfg_attr(feature = "documentation", derive(schemars::JsonSchema))]
pub enum RenameRule {
    /// Direct column name mapping
    ///
    /// Example:
    /// ```yaml
    /// - type: direct
    ///   from: "DateTime"
    ///   to: "timestamp"
    ///   cast: "timestamp"
    /// ```
    #[serde(rename = "direct")]
    Direct {
        from: String,
        to: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        cast: Option<String>,
    },

    /// Regex pattern-based renaming
    ///
    /// Example:
    /// ```yaml
    /// - type: pattern
    ///   pattern: "^(.+) \\((.+)\\)$"
    ///   replacement: "$1.$2"
    /// ```
    /// This transforms "Temperature (C)" -> "Temperature.C"
    #[serde(rename = "pattern")]
    Pattern {
        pattern: String,
        replacement: String,
    },
}

/// Configuration for column rename transformation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "documentation", derive(schemars::JsonSchema))]
pub struct ColumnRenameConfig {
    /// List of rename rules applied in order
    pub rules: Vec<RenameRule>,
}

impl ColumnRenameConfig {
    /// Validate configuration - ensures regex patterns compile
    ///
    /// # Errors
    /// Returns error if any regex pattern is invalid
    pub fn validate(&self) -> Result<(), tinyfs::Error> {
        for rule in &self.rules {
            if let RenameRule::Pattern { pattern, .. } = rule {
                let _regex = Regex::new(pattern)
                    .map_other_context(format!("Invalid regex pattern '{}'", pattern))?;
            }
        }
        Ok(())
    }

    /// Apply rename rules to a column name
    ///
    /// Rules are applied in order. The first matching rule wins.
    /// If no rule matches, the original column name is returned.
    #[must_use]
    pub fn apply_to_column(&self, column_name: &str) -> String {
        for rule in &self.rules {
            match rule {
                RenameRule::Direct { from, to, cast: _ } => {
                    if column_name == from {
                        return to.clone();
                    }
                }
                RenameRule::Pattern {
                    pattern,
                    replacement,
                } => {
                    if let Ok(regex) = Regex::new(pattern)
                        && regex.is_match(column_name)
                    {
                        return regex
                            .replace(column_name, replacement.as_str())
                            .into_owned();
                    }
                }
            }
        }

        // No rule matched - return original
        column_name.to_string()
    }

    /// Create a forward mapping of column renames
    ///
    /// Returns HashMap: original_name -> renamed_name (only for columns that change)
    #[must_use]
    pub fn create_forward_map(&self, column_names: &[String]) -> HashMap<String, String> {
        let mut map = HashMap::new();

        for col_name in column_names {
            let renamed = self.apply_to_column(col_name);
            if renamed != *col_name {
                _ = map.insert(col_name.clone(), renamed);
            }
        }

        map
    }

    /// Create a reverse mapping of column renames
    ///
    /// Returns HashMap: renamed_name -> original_name (for filter pushdown)
    #[must_use]
    pub fn create_reverse_map(&self, column_names: &[String]) -> HashMap<String, String> {
        let mut map = HashMap::new();

        for col_name in column_names {
            let renamed = self.apply_to_column(col_name);
            if renamed != *col_name {
                _ = map.insert(renamed, col_name.clone());
            }
        }

        map
    }

    /// Build a map of columns that need type casting
    ///
    /// Returns HashMap: renamed_column_name -> cast_type_name
    #[must_use]
    pub fn build_cast_map(&self, column_names: Vec<&str>) -> HashMap<String, String> {
        let mut cast_map = HashMap::new();

        for col_name in column_names {
            for rule in &self.rules {
                if let RenameRule::Direct { from, to, cast } = rule
                    && from == col_name
                {
                    if let Some(cast_type) = cast {
                        _ = cast_map.insert(to.clone(), cast_type.clone());
                    }
                    break;
                }
            }
        }

        cast_map
    }
}

// ============================================================================
// Factory Implementation
// ============================================================================

/// Validate column rename configuration from YAML bytes
fn validate_column_rename_config(config: &[u8]) -> TinyFSResult<Value> {
    let config_str = std::str::from_utf8(config).map_other_context("Invalid UTF-8")?;

    let config: ColumnRenameConfig =
        serde_yaml::from_str(config_str).map_other_context("Invalid YAML")?;

    // Validate regex patterns compile
    config.validate().map_other()?;

    serde_json::to_value(config).map_other_context("Serialization error")
}

/// Apply column rename transformation to a TableProvider
///
/// This function is called by other factories (like sql_derived) when they apply
/// transform chains. It wraps the input TableProvider with a ColumnRenameTableProvider
/// that applies the configured rename rules and optional type casts.
async fn apply_column_rename_transform(
    context: crate::FactoryContext,
    input: std::sync::Arc<dyn datafusion::catalog::TableProvider>,
) -> Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>, tinyfs::Error> {
    // Read config directly from the file's content (version 1)
    let config_bytes = context
        .context
        .persistence
        .read_file_version(context.file_id, 1)
        .await?;

    // Parse config
    let config_str = std::str::from_utf8(&config_bytes).map_other_context("Invalid UTF-8")?;

    let rename_config: ColumnRenameConfig =
        serde_yaml::from_str(config_str).map_other_context("Invalid YAML")?;

    // Build cast map from rules with cast annotations
    let schema = input.schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let cast_map = rename_config.build_cast_map(field_names);

    // Create rename function from config
    let rename_fn = std::sync::Arc::new(move |col_name: &str| -> String {
        rename_config.apply_to_column(col_name)
    });

    // Wrap with ColumnRenameTableProvider
    let provider =
        crate::transform::column_rename::ColumnRenameTableProvider::new(input, rename_fn, cast_map)
            .map_other_context("Failed to create rename provider")?;

    Ok(std::sync::Arc::new(provider))
}

// Register the factory as a table transform factory
crate::register_table_transform_factory!(
    name: "column-rename",
    description: "Column rename transformation for table providers",
    validate: validate_column_rename_config,
    transform: apply_column_rename_transform
);

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_direct_rule() {
        let config = ColumnRenameConfig {
            rules: vec![RenameRule::Direct {
                from: "DateTime".to_string(),
                to: "timestamp".to_string(),
                cast: None,
            }],
        };

        assert_eq!(config.apply_to_column("DateTime"), "timestamp");
        assert_eq!(config.apply_to_column("OtherColumn"), "OtherColumn");
    }

    #[test]
    fn test_pattern_rule() {
        let config = ColumnRenameConfig {
            rules: vec![RenameRule::Pattern {
                pattern: r"^(.+) \((.+)\)$".to_string(),
                replacement: "$1.$2".to_string(),
            }],
        };

        assert_eq!(config.apply_to_column("Temperature (C)"), "Temperature.C");
        assert_eq!(config.apply_to_column("DO (mg/L)"), "DO.mg/L");
        assert_eq!(config.apply_to_column("SimpleColumn"), "SimpleColumn");
    }

    #[test]
    fn test_combined_rules() {
        let config = ColumnRenameConfig {
            rules: vec![
                RenameRule::Direct {
                    from: "DateTime".to_string(),
                    cast: None,
                    to: "timestamp".to_string(),
                },
                RenameRule::Pattern {
                    pattern: r"^(.+) \((.+)\)$".to_string(),
                    replacement: "$1.$2".to_string(),
                },
            ],
        };

        assert_eq!(config.apply_to_column("DateTime"), "timestamp");
        assert_eq!(config.apply_to_column("Temperature (C)"), "Temperature.C");
        assert_eq!(config.apply_to_column("Simple"), "Simple");
    }

    #[test]
    fn test_validation_valid() {
        let config = ColumnRenameConfig {
            rules: vec![RenameRule::Pattern {
                pattern: r"^(.+) \((.+)\)$".to_string(),
                replacement: "$1.$2".to_string(),
            }],
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validation_invalid_regex() {
        let config = ColumnRenameConfig {
            rules: vec![RenameRule::Pattern {
                pattern: r"(?P<unclosed".to_string(), // Invalid: unclosed group
                replacement: "$1.$2".to_string(),
            }],
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_forward_map() {
        let config = ColumnRenameConfig {
            rules: vec![
                RenameRule::Direct {
                    from: "DateTime".to_string(),
                    to: "timestamp".to_string(),
                    cast: None,
                },
                RenameRule::Pattern {
                    pattern: r"^(.+) \((.+)\)$".to_string(),
                    replacement: "$1.$2".to_string(),
                },
            ],
        };

        let columns = vec![
            "DateTime".to_string(),
            "Temperature (C)".to_string(),
            "Simple".to_string(),
        ];

        let forward_map = config.create_forward_map(&columns);

        assert_eq!(forward_map.get("DateTime"), Some(&"timestamp".to_string()));
        assert_eq!(
            forward_map.get("Temperature (C)"),
            Some(&"Temperature.C".to_string())
        );
        assert_eq!(forward_map.get("Simple"), None); // Unchanged columns not in map
    }

    #[test]
    fn test_reverse_map() {
        let config = ColumnRenameConfig {
            rules: vec![RenameRule::Direct {
                from: "DateTime".to_string(),
                to: "new_name".to_string(),
                cast: None,
            }],
        };

        let columns = vec!["DateTime".to_string(), "Other".to_string()];

        let reverse_map = config.create_reverse_map(&columns);

        assert_eq!(reverse_map.get("new_name"), Some(&"DateTime".to_string()));
        assert_eq!(reverse_map.get("Other"), None);
    }

    #[test]
    fn test_yaml_serialization() {
        let config = ColumnRenameConfig {
            rules: vec![
                RenameRule::Direct {
                    from: "DateTime".to_string(),
                    to: "timestamp".to_string(),
                    cast: None,
                },
                RenameRule::Pattern {
                    pattern: r"^(.+) \((.+)\)$".to_string(),
                    replacement: "$1.$2".to_string(),
                },
            ],
        };

        let yaml = serde_yaml::to_string(&config).unwrap();
        let parsed: ColumnRenameConfig = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(config.rules.len(), parsed.rules.len());
    }
}
