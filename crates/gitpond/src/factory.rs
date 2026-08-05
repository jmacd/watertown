// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Git-ingest factory registration and execution.
//!
//! Registers as both a dynamic directory factory (for reads) and an
//! executable factory (for `pond run pull` to fetch from the remote).

use crate::git;
use crate::tree::{AutoCloneInfo, GitRootDirectory};
use log::{debug, info};
use provider::{ExecutionContext, FactoryContext};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use tinyfs::Result as TinyFSResult;
use tinyfs::ResultExt;

use clap::{Parser, Subcommand};

/// Git-ingest factory subcommands
#[derive(Debug, Parser)]
struct GitIngestCommand {
    #[command(subcommand)]
    command: Option<GitIngestSubcommand>,
}

#[derive(Debug, Subcommand)]
enum GitIngestSubcommand {
    /// Pull changes from git (fetch + resolve)
    Pull,

    /// Push mode (no-op for git-ingest -- git is a read-only source)
    Push,

    /// Show current sync status
    Status,
}

/// Configuration for the git-ingest factory
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitIngestConfig {
    /// Git remote URL (HTTPS or SSH)
    pub url: String,

    /// Git ref to track (branch name, tag, or SHA)
    #[serde(rename = "ref")]
    pub git_ref: String,

    /// Optional path prefix filter.  When set, only files under this
    /// directory in the git tree are synced, with the prefix stripped
    /// from their paths.
    #[serde(default)]
    pub prefix: Option<String>,
}

impl GitIngestConfig {
    /// Validate the configuration
    pub fn validate(&self) -> TinyFSResult<()> {
        if self.url.is_empty() {
            return Err(tinyfs::Error::Other("url cannot be empty".to_string()));
        }
        if self.git_ref.is_empty() {
            return Err(tinyfs::Error::Other("ref cannot be empty".to_string()));
        }
        if let Some(ref prefix) = self.prefix {
            if prefix.is_empty() {
                return Err(tinyfs::Error::Other(
                    "prefix must not be empty (omit the field instead)".to_string(),
                ));
            }
            if prefix.starts_with('/') || prefix.ends_with('/') {
                return Err(tinyfs::Error::Other(
                    "prefix must not start or end with '/'".to_string(),
                ));
            }
            if prefix.contains("..") {
                return Err(tinyfs::Error::Other(
                    "prefix must not contain '..'".to_string(),
                ));
            }
        }
        Ok(())
    }
}

/// Parse command-line arguments
fn parse_command(ctx: ExecutionContext) -> Result<GitIngestCommand, tinyfs::Error> {
    let args_with_prog_name: Vec<String> = std::iter::once("factory".to_string())
        .chain(ctx.args().iter().cloned())
        .collect();

    GitIngestCommand::try_parse_from(args_with_prog_name).map_other_context("Command parse error")
}

/// Create a dynamic directory for reading the git tree.
fn create_directory(config: Value, context: FactoryContext) -> TinyFSResult<tinyfs::DirHandle> {
    let config: GitIngestConfig =
        serde_json::from_value(config).map_other_context("Invalid config")?;

    let pond_path = context
        .context
        .pond_path()
        .ok_or_else(|| {
            tinyfs::Error::Other(
                "git-ingest requires a real pond (pond_path not available)".to_string(),
            )
        })?
        .to_path_buf();

    let node_id = context.file_id.node_id().to_string();
    let repo_path = git::bare_repo_path(&pond_path, &node_id);

    let root_dir = GitRootDirectory::new(
        repo_path,
        config.git_ref,
        config.prefix,
        context.file_id,
        Some(AutoCloneInfo {
            url: config.url,
            pond_path,
        }),
    );

    Ok(root_dir.create_handle())
}

/// Initialize factory (called once per dynamic node creation)
async fn initialize(_config: Value, _context: FactoryContext) -> Result<(), tinyfs::Error> {
    Ok(())
}

/// Execute the git-ingest factory (pull/status commands)
pub async fn execute(
    config: Value,
    context: FactoryContext,
    ctx: ExecutionContext,
) -> Result<(), tinyfs::Error> {
    let config: GitIngestConfig =
        serde_json::from_value(config).map_other_context("Invalid config")?;

    let cmd = parse_command(ctx)?;

    let pond_path = context
        .context
        .pond_path()
        .ok_or_else(|| {
            tinyfs::Error::Other(
                "git-ingest requires a real pond (pond_path not available)".to_string(),
            )
        })?
        .to_path_buf();

    let node_id = context.file_id.node_id().to_string();

    match cmd.command {
        Some(GitIngestSubcommand::Push) => {
            info!("git-ingest: 'push' mode is a no-op (git is a read-only source)");
            Ok(())
        }
        Some(GitIngestSubcommand::Status) => execute_status(&pond_path, &node_id, &config),
        Some(GitIngestSubcommand::Pull) | None => execute_pull(&pond_path, &node_id, &config),
    }
}

/// Show sync status
fn execute_status(
    pond_path: &std::path::Path,
    node_id: &str,
    config: &GitIngestConfig,
) -> Result<(), tinyfs::Error> {
    let repo_path = git::bare_repo_path(pond_path, node_id);

    if repo_path.exists() {
        match git::resolve_tree_at_ref(&repo_path, &config.git_ref) {
            Ok(_) => {
                let repo = git::open_repo(&repo_path)?;
                let commit_sha = git::fetch_and_resolve(&repo_path, &config.url, &config.git_ref)
                    .unwrap_or_else(|_| "unknown".to_string());
                drop(repo);
                log::info!(
                    "git-ingest: at commit {}",
                    &commit_sha[..12.min(commit_sha.len())]
                );
            }
            Err(_) => {
                log::info!("git-ingest: repo cached but ref not yet resolved");
            }
        }
    } else {
        log::info!("git-ingest: not yet fetched");
    }
    log::info!("  url: {}", config.url);
    log::info!("  ref: {}", config.git_ref);
    if let Some(ref prefix) = config.prefix {
        log::info!("  prefix: {}", prefix);
    }
    Ok(())
}

/// Pull changes from git (fetch only -- tree is served dynamically)
fn execute_pull(
    pond_path: &std::path::Path,
    node_id: &str,
    config: &GitIngestConfig,
) -> Result<(), tinyfs::Error> {
    info!("git-ingest pull: {} (ref: {})", config.url, config.git_ref,);

    // Ensure the git cache directory exists
    let git_dir = pond_path.join("git");
    std::fs::create_dir_all(&git_dir).map_other_context("Failed to create git dir")?;

    // Fetch and resolve the ref
    let repo_path = git::bare_repo_path(pond_path, node_id);
    let commit_sha = git::fetch_and_resolve(&repo_path, &config.url, &config.git_ref)?;
    debug!("Resolved {} -> {}", config.git_ref, &commit_sha[..12]);

    // Verify prefix is valid if configured
    if let Some(ref prefix) = config.prefix {
        let repo = git::open_repo(&repo_path)?;
        let tree_oid = git::resolve_tree_at_ref(&repo_path, &config.git_ref)?;
        let _ = git::navigate_to_prefix(&repo, &tree_oid, prefix)?;
        info!("Prefix '{}' verified in tree", prefix);
    }

    info!("Fetch complete, tree will be served dynamically");
    Ok(())
}

/// Validate configuration YAML
fn validate_config(config: &[u8]) -> TinyFSResult<Value> {
    let config: GitIngestConfig =
        serde_yaml::from_slice(config).map_other_context("Invalid config YAML")?;

    config.validate()?;

    serde_json::to_value(&config).map_other_context("Failed to serialize config")
}

// --- Dual registration: dynamic directory + executable ---
//
// The standard macros only support one or the other. We manually
// construct the DynamicFactory entry to get both capabilities.

fn initialize_wrapper(
    config: Value,
    context: FactoryContext,
) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>> {
    Box::pin(async move {
        initialize(config, context)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    })
}

fn execute_wrapper(
    config: Value,
    context: FactoryContext,
    ctx: ExecutionContext,
) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>> {
    Box::pin(async move {
        execute(config, context, ctx)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    })
}

#[allow(unsafe_code)]
#[linkme::distributed_slice(provider::registry::DYNAMIC_FACTORIES)]
static FACTORY_GIT_INGEST: provider::registry::DynamicFactory =
    provider::registry::DynamicFactory {
        name: "git-ingest",
        description: "Mount a git repository branch as a dynamic directory in the pond",
        create_directory: Some(create_directory),
        create_file: None,
        validate_config,
        validate_raw_config: None,
        try_as_queryable: None,
        initialize: Some(initialize_wrapper),
        execute: Some(execute_wrapper),
        apply_table_transform: None,
    };

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_config() {
        let config = GitIngestConfig {
            url: "https://github.com/user/blog.git".to_string(),
            git_ref: "main".to_string(),
            prefix: None,
        };
        config.validate().expect("valid config should pass");
    }

    #[test]
    fn test_validate_config_empty_url() {
        let config = GitIngestConfig {
            url: String::new(),
            git_ref: "main".to_string(),
            prefix: None,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_config_empty_ref() {
        let config = GitIngestConfig {
            url: "https://github.com/user/blog.git".to_string(),
            git_ref: String::new(),
            prefix: None,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_config_yaml() {
        let yaml = b"url: https://github.com/user/blog.git\nref: main\n";
        let result = validate_config(yaml);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_config_yaml_unknown_field() {
        let yaml = b"url: https://github.com/user/blog.git\nref: main\nextra: bad\n";
        let result = validate_config(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_config_with_prefix() {
        let config = GitIngestConfig {
            url: "https://github.com/user/repo.git".to_string(),
            git_ref: "main".to_string(),
            prefix: Some("site".to_string()),
        };
        config.validate().expect("valid prefix");
    }

    #[test]
    fn test_validate_config_prefix_leading_slash() {
        let config = GitIngestConfig {
            url: "https://github.com/user/repo.git".to_string(),
            git_ref: "main".to_string(),
            prefix: Some("/site".to_string()),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_config_prefix_trailing_slash() {
        let config = GitIngestConfig {
            url: "https://github.com/user/repo.git".to_string(),
            git_ref: "main".to_string(),
            prefix: Some("site/".to_string()),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_config_prefix_dotdot() {
        let config = GitIngestConfig {
            url: "https://github.com/user/repo.git".to_string(),
            git_ref: "main".to_string(),
            prefix: Some("../etc".to_string()),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_config_prefix_empty() {
        let config = GitIngestConfig {
            url: "https://github.com/user/repo.git".to_string(),
            git_ref: "main".to_string(),
            prefix: Some(String::new()),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_config_yaml_with_prefix() {
        let yaml = b"url: https://github.com/user/repo.git\nref: main\nprefix: site/content\n";
        let result = validate_config(yaml);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_config_yaml_without_prefix() {
        let yaml = b"url: https://github.com/user/repo.git\nref: main\n";
        let result = validate_config(yaml);
        assert!(result.is_ok());
    }
}
