// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

// CLI command for creating dynamic nodes
use crate::common::ShipContext;
use anyhow::{Result, anyhow};
use log::debug;
use provider::FactoryRegistry;
use std::fs;
use utilities::env_substitution;

/// Create a dynamic node in the pond using transaction guard pattern
pub async fn mknod_command(
    ship_context: &ShipContext,
    factory_type: &str,
    path: &str,
    config_path: &str,
    overwrite: bool,
) -> Result<()> {
    debug!("Creating dynamic node in pond: {path} with factory: {factory_type}");

    // Read config file early to validate it exists and is readable
    let config_content = fs::read_to_string(config_path)
        .map_err(|e| anyhow!("Failed to read config file '{}': {}", config_path, e))?;

    // Some factories additionally constrain the *raw* form -- e.g. a storage
    // profile requires that its credentials still be references, a rule that
    // is invisible after expansion.  Check it before anything is written: a
    // literal caught on first use would already be in replicated history.
    FactoryRegistry::validate_raw_config(factory_type, config_content.as_bytes()).map_err(|e| {
        anyhow!(
            "Invalid configuration for factory '{}': {}",
            factory_type,
            e
        )
    })?;

    // Expand env references for validation and initialization only.
    // The raw content (with ${env:...} references) is stored in the
    // pond so that secrets are never persisted.
    let expanded_content =
        env_substitution::substitute_env_vars(&config_content)
            .map_err(|e| {
                anyhow!(
                    "Failed to expand environment variables in config file '{}':\n  {}\n  \
            Tip: Use ${{env:VAR}} to read environment variables, ${{env:VAR:-default}} for defaults",
                    config_path,
                    e
                )
            })?;

    // Validate using the expanded (secrets-resolved) content
    let expanded_bytes = expanded_content.as_bytes();

    // Validate the factory and configuration early
    let _validated_config = FactoryRegistry::validate_config(factory_type, expanded_bytes)
        .map_err(|e| {
            anyhow!(
                "Invalid configuration for factory '{}': {}",
                factory_type,
                e
            )
        })?;

    // Store the raw config content (with ${env:...} references intact).
    // Expansion happens at runtime in `pond run` so that
    // secrets are never persisted in the oplog.
    let stored_config_bytes = config_content.into_bytes();

    // Create ship and use scoped transaction for mknod operation
    let mut ship = ship_context.open_pond().await?;
    let path_clone = path.to_string();
    let factory_type_clone = factory_type.to_string();

    // mknod needs the transaction guard for tx.state() (provider context), so it
    // takes the full Transaction (not the &FS-only Steward::write_transaction).
    crate::common::with_write_transaction(&mut ship, ship_context.command_args(), async |tx| {
        mknod_impl(
            tx,
            tx,
            &path_clone,
            &factory_type_clone,
            stored_config_bytes.clone(),
            expanded_content.into_bytes(),
            overwrite,
        )
        .await
        .map_err(|e| anyhow!("mknod operation failed: {}", e))
    })
    .await?;

    Ok(())
}

async fn mknod_impl(
    tx: &steward::Transaction<'_>,
    fs: &tinyfs::FS,
    path: &str,
    factory_type: &str,
    config_bytes: Vec<u8>,
    expanded_config_bytes: Vec<u8>,
    overwrite: bool,
) -> Result<()> {
    let root = fs.root().await?;

    // Check what the factory supports and use the appropriate creation method
    let factory = FactoryRegistry::get_factory(factory_type)
        .ok_or_else(|| anyhow!("Unknown factory type: {}", factory_type))?;

    // Determine what type of node to create based on factory capabilities
    let entry_type = if factory.create_directory.is_some() {
        tinyfs::EntryType::DirectoryDynamic
    } else if factory.create_file.is_some()
        || factory.execute.is_some()
        || factory.apply_table_transform.is_some()
    {
        // Factory supports files - either:
        // 1. Has explicit create_file function (template factory)
        // 2. Is executable factory (config bytes ARE the file content via ConfigFile wrapper)
        // 3. Is table transform factory (config stored for use by other factories)
        //
        // If the factory provides try_as_queryable, it creates SQL-queryable files
        // and should be stored as TableDynamic so that series:// pattern resolution
        // (SqlDerivedFile) can discover them. Without this, standalone mknod nodes
        // would be invisible to timeseries-join, timeseries-pivot, temporal-reduce, etc.
        if factory.try_as_queryable.is_some() {
            tinyfs::EntryType::TableDynamic
        } else {
            tinyfs::EntryType::FileDynamic
        }
    } else {
        return Err(anyhow!(
            "Factory '{}' does not support creating directories or files",
            factory_type
        ));
    };

    let _node_path = if overwrite {
        root.create_dynamic_path_with_overwrite(
            path,
            entry_type,
            factory_type,
            config_bytes.clone(),
            overwrite,
        )
        .await?
    } else {
        root.create_dynamic_path(path, entry_type, factory_type, config_bytes.clone())
            .await
            .map_err(|e| match e {
                tinyfs::Error::AlreadyExists(_) => {
                    anyhow!(
                        "Dynamic node already exists at path '{}'. Use --overwrite to replace it.",
                        path
                    )
                }
                e => anyhow!("Failed to create dynamic node: {}", e),
            })?
    };

    // Node is created, lock is released - now run factory initialization
    // Get the parent node ID for factory context
    let parent_path = std::path::Path::new(path)
        .parent()
        .unwrap_or(std::path::Path::new("/"));
    let parent_node_path = root.resolve_path(parent_path).await?;
    let parent_node_id = match parent_node_path.1 {
        tinyfs::Lookup::Found(node) => node.id(),
        _ => {
            return Err(anyhow!(
                "Parent directory not found: {}",
                parent_path.display()
            ));
        }
    };

    // Create factory context from transaction guard
    let provider_context = tx
        .provider_context()
        .map_err(|e| anyhow!("Failed to get provider context: {}", e))?;
    let context = provider::FactoryContext::new(provider_context, parent_node_id);

    // Run factory initialization if it exists (e.g., create directories)
    // Use expanded config for initialization since it may need to connect
    // to remote services (e.g., S3 for import-mode partition discovery).
    FactoryRegistry::initialize::<tlogfs::TLogFSError>(
        factory_type,
        &expanded_config_bytes,
        context,
    )
    .await
    .map_err(|e| anyhow!("Factory initialization failed: {}", e))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::init::init_command;
    use crate::common::ShipContext;
    use std::fs;
    use std::path::PathBuf;
    use tempfile::TempDir;

    struct TestSetup {
        temp_dir: TempDir,
        ship_context: ShipContext,
    }

    impl TestSetup {
        async fn new() -> Result<Self> {
            let temp_dir = TempDir::new().expect("Failed to create temp directory");
            let pond_path = temp_dir.path().join("test_pond");

            // Create ship context for initialization
            let init_args = vec!["pond".to_string(), "init".to_string()];
            let ship_context = ShipContext::pond_only(Some(&pond_path), init_args.clone());

            // Initialize pond
            init_command(&ship_context, "test-host").await?;

            Ok(Self {
                temp_dir,
                ship_context,
            })
        }

        /// Create a factory configuration file for testing (sql-derived-table)
        fn create_factory_config(&self) -> Result<PathBuf> {
            let config_path = self.temp_dir.path().join("factory_config.yaml");
            let config_content = r#"patterns:
  source: "table:///data/*.table"
query: "SELECT * FROM source"
"#;
            fs::write(&config_path, config_content)?;
            Ok(config_path)
        }

        /// Verify that the dynamic node exists in the pond
        async fn verify_node_exists(&self, pond_path: &str) -> Result<bool> {
            let mut ship = self.ship_context.open_pond().await?;
            let tx = ship
                .begin_read(&steward::PondUserMetadata::new(vec![
                    "verify_node".to_string(),
                ]))
                .await?;

            let result = {
                let fs = &*tx;
                let root = fs
                    .root()
                    .await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                root.exists(pond_path).await
            };

            _ = tx.commit().await?;
            Ok(result)
        }
    }

    #[tokio::test]
    async fn test_mknod_nonexistent_factory() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create a dummy config file
        let config_path = setup.temp_dir.path().join("dummy_config.json");
        fs::write(&config_path, "{}")?;

        // Try to create node with unknown factory
        let result = mknod_command(
            &setup.ship_context,
            "unknown_factory",
            "/test_node",
            &config_path.to_string_lossy(),
            false,
        )
        .await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(
            error_msg.contains("Unknown factory type")
                || error_msg.contains("Invalid configuration")
        );

        // Verify node was not created
        assert!(!setup.verify_node_exists("/test_node").await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_mknod_invalid_config_file() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Try with nonexistent config file
        let result = mknod_command(
            &setup.ship_context,
            "hostmount",
            "/test_node",
            "/nonexistent/config.json",
            false,
        )
        .await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Failed to read config file"));

        Ok(())
    }

    #[tokio::test]
    async fn test_mknod_nested_path() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create parent directories first
        crate::commands::mkdir::mkdir_command(&setup.ship_context, "/deep/nested/path", true)
            .await?;

        // Create factory config
        let config_path = setup.create_factory_config()?;

        // Create node at nested path
        let result = mknod_command(
            &setup.ship_context,
            "sql-derived-table",
            "/deep/nested/path/derived",
            &config_path.to_string_lossy(),
            false,
        )
        .await;

        assert!(
            result.is_ok(),
            "mknod should succeed for nested paths: {:?}",
            result.err()
        );

        // Verify the nested node was created
        assert!(
            setup
                .verify_node_exists("/deep/nested/path/derived")
                .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_mknod_duplicate_path() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create factory config
        let config_path = setup.create_factory_config()?;

        // Create first node
        let result1 = mknod_command(
            &setup.ship_context,
            "sql-derived-table",
            "/duplicate_node",
            &config_path.to_string_lossy(),
            false,
        )
        .await;

        if let Err(ref e) = result1 {
            debug!("First mknod failed with error: {}", e);
        }
        assert!(result1.is_ok());

        // Try to create second node at same path
        let result2 = mknod_command(
            &setup.ship_context,
            "sql-derived-table",
            "/duplicate_node",
            &config_path.to_string_lossy(),
            false,
        )
        .await;

        // This should fail since the path already exists
        assert!(result2.is_err());
        let error_msg = result2.unwrap_err().to_string();
        assert!(error_msg.contains("already exists") || error_msg.contains("conflict"));

        Ok(())
    }

    #[tokio::test]
    async fn test_mknod_transaction_rollback_on_error() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create invalid config that will fail during node creation
        let config_path = setup.temp_dir.path().join("malformed_config.yaml");
        fs::write(&config_path, "{ malformed yaml")?; // Invalid YAML

        // Try to create node
        let result = mknod_command(
            &setup.ship_context,
            "hostmount",
            "/test_rollback",
            &config_path.to_string_lossy(),
            false,
        )
        .await;

        assert!(result.is_err());

        // Verify that no partial node was created (transaction rolled back)
        assert!(!setup.verify_node_exists("/test_rollback").await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_mknod_overwrite_flag_error_message() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create factory config
        let config_path = setup.create_factory_config()?;

        // Create first node
        let result1 = mknod_command(
            &setup.ship_context,
            "sql-derived-table",
            "/test_overwrite",
            &config_path.to_string_lossy(),
            false,
        )
        .await;
        assert!(result1.is_ok());

        // Try to create second node at same path without --overwrite - should show helpful error
        let result2 = mknod_command(
            &setup.ship_context,
            "sql-derived-table",
            "/test_overwrite",
            &config_path.to_string_lossy(),
            false,
        )
        .await;

        assert!(result2.is_err());
        let error_msg = result2.unwrap_err().to_string();
        assert!(error_msg.contains("Dynamic node already exists"));
        assert!(error_msg.contains("Use --overwrite to replace it"));

        Ok(())
    }

    #[tokio::test]
    async fn test_mknod_overwrite_success() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create first factory config
        let config_path1 = setup.create_factory_config()?;

        // Create first node
        let result1 = mknod_command(
            &setup.ship_context,
            "sql-derived-table",
            "/test_overwrite_success",
            &config_path1.to_string_lossy(),
            false,
        )
        .await;
        assert!(
            result1.is_ok(),
            "First mknod should succeed: {:?}",
            result1.err()
        );

        // Verify the node was created
        assert!(setup.verify_node_exists("/test_overwrite_success").await?);

        // Create second factory config with different query
        let config_path2 = setup.temp_dir.path().join("factory_config2.yaml");
        let config_content2 = r#"patterns:
  source: "table:///data/*.table"
query: "SELECT * FROM source LIMIT 10"
"#;
        fs::write(&config_path2, config_content2)?;

        // Overwrite the node with new configuration
        let result2 = mknod_command(
            &setup.ship_context,
            "sql-derived-table",
            "/test_overwrite_success",
            &config_path2.to_string_lossy(),
            true,
        )
        .await;
        assert!(
            result2.is_ok(),
            "Overwrite mknod should succeed: {:?}",
            result2.err()
        );

        // Verify the node still exists (configuration should be updated)
        assert!(setup.verify_node_exists("/test_overwrite_success").await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_dynamic_node_config_directory() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create a git-ingest config (DirectoryDynamic factory)
        let config_path = setup.temp_dir.path().join("git_config.yaml");
        let config_content = "url: https://github.com/user/repo.git\nref: main\n";
        fs::write(&config_path, config_content)?;

        // Create the directory dynamic node
        let result = mknod_command(
            &setup.ship_context,
            "git-ingest",
            "/test_git_config",
            &config_path.to_string_lossy(),
            false,
        )
        .await;
        assert!(
            result.is_ok(),
            "mknod git-ingest should succeed: {:?}",
            result.err()
        );

        // Retrieve the config via get_dynamic_node_config
        let mut ship = setup.ship_context.open_pond().await?;
        let tx = ship
            .begin_read(&steward::PondUserMetadata::new(vec![
                "test_config".to_string(),
            ]))
            .await?;

        // Resolve the node to get its FileID
        let root = tx.root().await?;
        let (_wd, lookup) = root.resolve_path("/test_git_config").await?;
        let node = match lookup {
            tinyfs::Lookup::Found(n) => n,
            _ => panic!("Node not found"),
        };

        // Get the dynamic config
        let config_result = tx.get_dynamic_node_config(node.id()).await?;
        assert!(
            config_result.is_some(),
            "get_dynamic_node_config should return config for DirectoryDynamic"
        );

        let (factory_name, config_bytes) = config_result.unwrap();
        assert_eq!(factory_name, "git-ingest");
        assert_eq!(
            String::from_utf8_lossy(&config_bytes),
            config_content,
            "Config bytes should match the stored YAML"
        );

        _ = tx.commit().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_get_dynamic_node_config_file() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create an sql-derived-table config (FileDynamic factory)
        let config_path = setup.create_factory_config()?;
        let config_content = fs::read_to_string(&config_path)?;

        // Create the file dynamic node
        let result = mknod_command(
            &setup.ship_context,
            "sql-derived-table",
            "/test_sql_config",
            &config_path.to_string_lossy(),
            false,
        )
        .await;
        assert!(
            result.is_ok(),
            "mknod sql-derived-table should succeed: {:?}",
            result.err()
        );

        // Retrieve the config via get_dynamic_node_config
        let mut ship = setup.ship_context.open_pond().await?;
        let tx = ship
            .begin_read(&steward::PondUserMetadata::new(vec![
                "test_config".to_string(),
            ]))
            .await?;

        let root = tx.root().await?;
        let (_wd, lookup) = root.resolve_path("/test_sql_config").await?;
        let node = match lookup {
            tinyfs::Lookup::Found(n) => n,
            _ => panic!("Node not found"),
        };

        let config_result = tx.get_dynamic_node_config(node.id()).await?;
        assert!(
            config_result.is_some(),
            "get_dynamic_node_config should return config for file-based factories"
        );

        let (factory_name, config_bytes) = config_result.unwrap();
        assert_eq!(factory_name, "sql-derived-table");
        assert_eq!(
            String::from_utf8_lossy(&config_bytes),
            config_content,
            "Config bytes should match the stored YAML"
        );

        _ = tx.commit().await?;

        Ok(())
    }
}
