// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Factory registration system for dynamic node creation
//!
//! This module provides the infrastructure for registering and managing factories
//! that create dynamic nodes (directories and files) in TinyFS. Factories are
//! registered at compile-time using linkme's distributed slice mechanism.

// linkme uses #[link_section] which is considered unsafe by rustc
#![allow(unsafe_code)]

use crate::FactoryContext;
use async_trait::async_trait;
use clap::Parser;
use linkme::distributed_slice;
use serde_json::Value;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tinyfs::Result as TinyFSResult;
use tinyfs::ResultExt;
use tinyfs::{AsyncReadSeek, DirHandle, EntryType, File, FileHandle, Metadata, NodeMetadata};
use tokio::sync::Mutex;

/// Execution mode for factory operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    /// Factory operates as a pond read-writer (normal mode)
    PondReadWriter,
    /// Factory operates as a control writer (special mode for system operations)
    ControlWriter,
    /// Factory applies table-provider transformations (called by other factories)
    TableTransform,
}

/// Execution context for factory commands
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionContext {
    mode: ExecutionMode,
    args: Vec<String>,
}

impl ExecutionContext {
    /// Create a control writer execution context
    #[must_use]
    pub fn control_writer(args: Vec<String>) -> Self {
        Self {
            mode: ExecutionMode::ControlWriter,
            args,
        }
    }

    /// Create a pond read-writer execution context
    #[must_use]
    pub fn pond_readwriter(args: Vec<String>) -> Self {
        Self {
            mode: ExecutionMode::PondReadWriter,
            args,
        }
    }

    /// Create a table transform execution context
    #[must_use]
    pub fn table_transform() -> Self {
        Self {
            mode: ExecutionMode::TableTransform,
            args: vec![],
        }
    }

    /// Get the execution mode
    #[must_use]
    pub fn mode(&self) -> ExecutionMode {
        self.mode
    }

    /// Get the command arguments
    #[must_use]
    pub fn args(&self) -> &[String] {
        &self.args
    }
}

/// Trait for factory commands that can be executed
pub trait FactoryCommand {
    /// Return the execution mode allowed for this command
    fn allowed(&self) -> ExecutionMode;
}

impl ExecutionContext {
    /// Parse command-line arguments into a factory command
    #[allow(clippy::print_stderr)]
    pub fn to_command<T, E>(self) -> Result<T, E>
    where
        T: Parser + FactoryCommand,
        E: From<clap::Error> + From<String>,
    {
        // Clap expects the first argument to be the program name
        let args_with_prog_name = if self.args.is_empty() {
            vec!["factory".to_string()]
        } else {
            std::iter::once("factory".to_string())
                .chain(self.args)
                .collect()
        };

        let cmd = T::try_parse_from(args_with_prog_name).map_err(|e| {
            // Print Clap's helpful error message immediately
            eprintln!("{}", e);
            E::from(e)
        })?;

        let allowed = cmd.allowed();
        if allowed == self.mode {
            Ok(cmd)
        } else {
            Err(E::from(format!(
                "incorrect execution mode: {:?} != {:?}",
                allowed, self.mode
            )))
        }
    }
}

// Re-export QueryableFile from tinyfs for backward compatibility
pub use tinyfs::QueryableFile;

/// A factory descriptor that can create dynamic nodes
///
/// Factories are registered at compile-time using the `register_dynamic_factory!`
/// and `register_executable_factory!` macros. Each factory can create either
/// directories or files (but not both), and optionally support initialization
/// and execution.
#[allow(clippy::type_complexity)]
pub struct DynamicFactory {
    /// Factory name (unique identifier)
    pub name: &'static str,
    /// Human-readable description
    pub description: &'static str,

    /// Create a directory node (mutually exclusive with create_file)
    pub create_directory:
        Option<fn(config: Value, context: FactoryContext) -> TinyFSResult<DirHandle>>,

    /// Create a file node (mutually exclusive with create_directory)
    pub create_file: Option<
        fn(
            config: Value,
            context: FactoryContext,
        ) -> Pin<Box<dyn Future<Output = TinyFSResult<FileHandle>> + Send>>,
    >,

    /// Validate and process configuration (all factories must provide this).
    ///
    /// Receives the **env-expanded** config, so it can check structure and
    /// resolved values but can never observe an `${env:...}` reference.
    pub validate_config: fn(config: &[u8]) -> TinyFSResult<Value>,

    /// Validate the **raw** config, as it will be stored in the pond, before
    /// env expansion (optional).
    ///
    /// `mknod` and `pond apply` validate the expanded config but store the raw
    /// one, so that secrets live in the environment and only references are
    /// persisted.  That split leaves one rule unexpressible in
    /// [`Self::validate_config`]: "this field must *remain* a reference".  A
    /// factory holding credentials (`storage-*`) needs it enforced at creation
    /// time -- catching it on first use is too late, because the plaintext is
    /// by then already in replicated, append-only history.
    pub validate_raw_config: Option<fn(config: &[u8]) -> TinyFSResult<()>>,

    /// Try to cast a File to QueryableFile (for SQL-based factories)
    pub try_as_queryable: Option<fn(&dyn File) -> Option<&dyn QueryableFile>>,

    /// Initialize factory after node creation (optional, runs outside lock)
    /// Returns Result<(), Box<dyn std::error::Error>> for flexibility
    pub initialize: Option<
        fn(
            config: Value,
            context: FactoryContext,
        ) -> Pin<
            Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>,
        >,
    >,

    /// Execute factory command (optional, for executable factories)
    /// Returns Result<(), Box<dyn std::error::Error>> for flexibility
    pub execute: Option<
        fn(
            config: Value,
            context: FactoryContext,
            ctx: ExecutionContext,
        ) -> Pin<
            Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>,
        >,
    >,

    /// Apply table provider transformation (optional, for transform factories)
    /// Takes a FactoryContext (with FileID to read config) and input TableProvider
    /// Returns transformed TableProvider
    pub apply_table_transform: Option<
        fn(
            context: FactoryContext,
            input: Arc<dyn datafusion::catalog::TableProvider>,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Result<
                            Arc<dyn datafusion::catalog::TableProvider>,
                            Box<dyn std::error::Error + Send + Sync>,
                        >,
                    > + Send,
            >,
        >,
    >,
}

/// Distributed slice containing all registered factories
/// linkme's distributed_slice uses #[link_section] which is considered unsafe
#[allow(unsafe_code)]
#[allow(clippy::declare_interior_mutable_const)]
#[distributed_slice]
pub static DYNAMIC_FACTORIES: [DynamicFactory];

// ---------------------------------------------------------------------------
// Unified scheme classification
// ---------------------------------------------------------------------------

/// Builtin scheme names reserved by the core URL system.
/// These are neither format providers nor factories -- they map directly
/// to TinyFS entry types or raw file access.
const BUILTIN_SCHEMES: &[&str] = &["file", "series", "table", "data"];

/// Classification of a URL scheme name across all registries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemeKind {
    /// A builtin scheme handled directly by the provider layer
    /// (`file`, `series`, `table`, `data`).
    Builtin,
    /// A registered format provider (CSV, OtelJSON, ExcelHTML, etc.).
    Format,
    /// A registered factory (sitegen, hydrovu, remote, etc.).
    Factory,
}

/// Unified scheme registry that queries both format providers and factories.
pub struct SchemeRegistry;

impl SchemeRegistry {
    /// Classify a scheme name by searching builtins, format providers, and factories.
    ///
    /// Returns `None` if the scheme is not recognized by any registry.
    #[must_use]
    pub fn classify(scheme: &str) -> Option<SchemeKind> {
        if BUILTIN_SCHEMES.contains(&scheme) {
            return Some(SchemeKind::Builtin);
        }
        if crate::FormatRegistry::get_provider(scheme).is_some() {
            return Some(SchemeKind::Format);
        }
        if FactoryRegistry::get_factory(scheme).is_some() {
            return Some(SchemeKind::Factory);
        }
        None
    }

    /// Validate that no factory name collides with a format provider name or
    /// a builtin scheme.
    ///
    /// Returns a list of conflicting names (empty means no conflicts).
    /// Call this from tests or at startup to catch registration errors.
    #[must_use]
    pub fn find_conflicts() -> Vec<String> {
        let mut conflicts = Vec::new();

        for factory in DYNAMIC_FACTORIES.iter() {
            let name = factory.name;

            if BUILTIN_SCHEMES.contains(&name) {
                conflicts.push(format!("factory '{}' conflicts with builtin scheme", name));
            }

            if crate::FormatRegistry::get_provider(name).is_some() {
                conflicts.push(format!("factory '{}' conflicts with format provider", name));
            }
        }

        conflicts
    }
}

/// Factory registry for looking up and creating dynamic nodes
pub struct FactoryRegistry;

impl FactoryRegistry {
    /// Get a factory by name
    #[must_use]
    pub fn get_factory(name: &str) -> Option<&'static DynamicFactory> {
        DYNAMIC_FACTORIES
            .iter()
            .find(|factory| factory.name == name)
    }

    /// List all available factories
    #[must_use]
    pub fn list_factories() -> &'static [DynamicFactory] {
        &DYNAMIC_FACTORIES
    }

    /// Create a dynamic directory using the specified factory
    pub fn create_directory(
        factory_name: &str,
        config: &[u8],
        context: FactoryContext,
    ) -> TinyFSResult<DirHandle> {
        let factory = Self::get_factory(factory_name)
            .ok_or_else(|| tinyfs::Error::Other(format!("Unknown factory: {}", factory_name)))?;

        // Expand ${env:VAR} placeholders before validation/deserialization.
        // Configs are stored raw; `pond run` expands them for executable
        // factories, but create_directory is called at filesystem-open time.
        let config_str =
            std::str::from_utf8(config).map_other_context("Config is not valid UTF-8")?;
        let expanded = utilities::env_substitution::substitute_env_vars(config_str)
            .map_other_context("Env expansion failed")?;

        let config_value = (factory.validate_config)(expanded.as_bytes())?;

        if let Some(create_fn) = factory.create_directory {
            create_fn(config_value, context)
        } else {
            Err(tinyfs::Error::Other(format!(
                "Factory '{}' does not support directories",
                factory_name
            )))
        }
    }

    /// Create a dynamic file using the specified factory
    pub async fn create_file(
        factory_name: &str,
        config: &[u8],
        context: FactoryContext,
    ) -> TinyFSResult<FileHandle> {
        let factory = Self::get_factory(factory_name)
            .ok_or_else(|| tinyfs::Error::Other(format!("Unknown factory: {}", factory_name)))?;

        // Expand ${env:VAR} placeholders (same as create_directory).
        let config_str =
            std::str::from_utf8(config).map_other_context("Config is not valid UTF-8")?;
        let expanded = utilities::env_substitution::substitute_env_vars(config_str)
            .map_other_context("Env expansion failed")?;

        let config_value = (factory.validate_config)(expanded.as_bytes())?;

        if let Some(create_fn) = factory.create_file {
            create_fn(config_value, context).await
        } else {
            Err(tinyfs::Error::Other(format!(
                "Factory '{}' does not support files",
                factory_name
            )))
        }
    }

    /// Validate configuration for a specific factory
    pub fn validate_config(factory_name: &str, config: &[u8]) -> TinyFSResult<Value> {
        let factory = Self::get_factory(factory_name)
            .ok_or_else(|| tinyfs::Error::Other(format!("Unknown factory: {}", factory_name)))?;

        (factory.validate_config)(config)
    }

    /// Validate the raw, pre-expansion config for factories that constrain it.
    ///
    /// A no-op for factories that declare no `validate_raw_config`.
    pub fn validate_raw_config(factory_name: &str, config: &[u8]) -> TinyFSResult<()> {
        let factory = Self::get_factory(factory_name)
            .ok_or_else(|| tinyfs::Error::Other(format!("Unknown factory: {}", factory_name)))?;

        match factory.validate_raw_config {
            Some(validate) => validate(config),
            None => Ok(()),
        }
    }

    /// Determine whether a factory creates directories or files
    pub fn factory_creates_directory(factory_name: &str) -> TinyFSResult<bool> {
        let factory = Self::get_factory(factory_name)
            .ok_or_else(|| tinyfs::Error::Other(format!("Unknown factory: {}", factory_name)))?;

        match (
            factory.create_directory.is_some(),
            factory.create_file.is_some(),
        ) {
            (true, false) => Ok(true),
            (false, true) => Ok(false),
            (true, true) => Err(tinyfs::Error::Other(format!(
                "Factory '{}' incorrectly supports both directories and files",
                factory_name
            ))),
            (false, false) => Err(tinyfs::Error::Other(format!(
                "Factory '{}' supports neither directories nor files",
                factory_name
            ))),
        }
    }

    /// Initialize a factory after node creation (runs outside lock)
    pub async fn initialize<E>(
        factory_name: &str,
        config: &[u8],
        context: FactoryContext,
    ) -> Result<(), E>
    where
        E: From<tinyfs::Error> + From<Box<dyn std::error::Error + Send + Sync>>,
    {
        let factory = Self::get_factory(factory_name).ok_or_else(|| {
            E::from(tinyfs::Error::Other(format!(
                "Unknown factory: {}",
                factory_name
            )))
        })?;

        let config_value = (factory.validate_config)(config).map_err(E::from)?;

        if let Some(initialize_fn) = factory.initialize {
            initialize_fn(config_value, context).await.map_err(E::from)
        } else {
            Ok(())
        }
    }

    /// Try to cast a File to QueryableFile by iterating through all registered factories
    ///
    /// This uses the factory registry instead of hardcoding types, making it properly extensible.
    /// Each factory that creates QueryableFile implementations registers its downcast function.
    #[must_use]
    pub fn try_as_queryable_file(file: &dyn File) -> Option<&dyn QueryableFile> {
        for factory in DYNAMIC_FACTORIES.iter() {
            if let Some(try_fn) = factory.try_as_queryable
                && let Some(queryable) = try_fn(file)
            {
                return Some(queryable);
            }
        }
        None
    }

    /// Execute a factory command
    pub async fn execute<E>(
        factory_name: &str,
        config: &[u8],
        context: FactoryContext,
        ctx: ExecutionContext,
    ) -> Result<(), E>
    where
        E: From<tinyfs::Error> + From<Box<dyn std::error::Error + Send + Sync>>,
    {
        let factory = Self::get_factory(factory_name).ok_or_else(|| {
            E::from(tinyfs::Error::Other(format!(
                "Unknown factory: {}",
                factory_name
            )))
        })?;

        let config_value = (factory.validate_config)(config).map_err(E::from)?;

        if let Some(execute_fn) = factory.execute {
            execute_fn(config_value, context, ctx)
                .await
                .map_err(E::from)
        } else {
            Err(E::from(tinyfs::Error::Other(format!(
                "Factory '{}' does not support execution",
                factory_name
            ))))
        }
    }
}

/// Convenience macro for registering a dynamic factory
#[macro_export]
macro_rules! register_dynamic_factory {
    (
        name: $name:expr,
        description: $description:expr,
        directory: $dir_fn:expr,
        validate: $validate_fn:expr
    ) => {
        paste::paste! {
            #[allow(unsafe_code)]
            #[linkme::distributed_slice($crate::registry::DYNAMIC_FACTORIES)]
            static [<FACTORY_ $name:snake:upper>]: $crate::registry::DynamicFactory = $crate::registry::DynamicFactory {
                name: $name,
                description: $description,
                create_directory: Some($dir_fn),
                create_file: None,
                validate_config: $validate_fn,
                validate_raw_config: None,
                try_as_queryable: None,
                initialize: None,
                execute: None,
                apply_table_transform: None,
            };
        }
    };

    (
        name: $name:expr,
        description: $description:expr,
        file: $file_fn:expr,
        validate: $validate_fn:expr,
        validate_raw: $validate_raw_fn:expr
    ) => {
        paste::paste! {
            fn [<file_wrapper_ $name:snake>](
                config: serde_json::Value,
                context: $crate::FactoryContext,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = tinyfs::Result<tinyfs::FileHandle>> + Send>> {
                Box::pin(async move { $file_fn(config, context) })
            }

            #[allow(unsafe_code)]
            #[linkme::distributed_slice($crate::registry::DYNAMIC_FACTORIES)]
            static [<FACTORY_ $name:snake:upper>]: $crate::registry::DynamicFactory = $crate::registry::DynamicFactory {
                name: $name,
                description: $description,
                create_directory: None,
                create_file: Some([<file_wrapper_ $name:snake>]),
                validate_config: $validate_fn,
                validate_raw_config: Some($validate_raw_fn),
                try_as_queryable: None,
                initialize: None,
                execute: None,
                apply_table_transform: None,
            };
        }
    };

    (
        name: $name:expr,
        description: $description:expr,
        file: $file_fn:expr,
        validate: $validate_fn:expr
    ) => {
        paste::paste! {
            fn [<file_wrapper_ $name:snake>](
                config: serde_json::Value,
                context: $crate::FactoryContext,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = tinyfs::Result<tinyfs::FileHandle>> + Send>> {
                Box::pin(async move { $file_fn(config, context) })
            }

            #[allow(unsafe_code)]
            #[linkme::distributed_slice($crate::registry::DYNAMIC_FACTORIES)]
            static [<FACTORY_ $name:snake:upper>]: $crate::registry::DynamicFactory = $crate::registry::DynamicFactory {
                name: $name,
                description: $description,
                create_directory: None,
                create_file: Some([<file_wrapper_ $name:snake>]),
                validate_config: $validate_fn,
                validate_raw_config: None,
                try_as_queryable: None,
                initialize: None,
                execute: None,
                apply_table_transform: None,
            };
        }
    };

    (
        name: $name:expr,
        description: $description:expr,
        file: $file_fn:expr,
        validate: $validate_fn:expr,
        try_as_queryable: $queryable_fn:expr
    ) => {
        paste::paste! {
            fn [<file_wrapper_ $name:snake>](
                config: serde_json::Value,
                context: $crate::FactoryContext,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = tinyfs::Result<tinyfs::FileHandle>> + Send>> {
                Box::pin(async move { $file_fn(config, context) })
            }

            #[allow(unsafe_code)]
            #[linkme::distributed_slice($crate::registry::DYNAMIC_FACTORIES)]
            static [<FACTORY_ $name:snake:upper>]: $crate::registry::DynamicFactory = $crate::registry::DynamicFactory {
                name: $name,
                description: $description,
                create_directory: None,
                create_file: Some([<file_wrapper_ $name:snake>]),
                validate_config: $validate_fn,
                validate_raw_config: None,
                try_as_queryable: Some($queryable_fn),
                initialize: None,
                execute: None,
                apply_table_transform: None,
            };
        }
    };
}

/// Register an executable factory (for run configurations)
#[macro_export]
macro_rules! register_executable_factory {
    (
        name: $name:expr,
        description: $description:expr,
        validate: $validate_fn:expr,
        initialize: $init_fn:expr,
        execute: $execute_fn:expr
    ) => {
        paste::paste! {
            fn [<initialize_wrapper_ $name:snake>](
                config: serde_json::Value,
                context: $crate::FactoryContext,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>> {
                Box::pin(async move {
                    $init_fn(config, context).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
                })
            }

            fn [<execute_wrapper_ $name:snake>](
                config: serde_json::Value,
                context: $crate::FactoryContext,
                ctx: $crate::registry::ExecutionContext,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>> {
                Box::pin(async move {
                    $execute_fn(config, context, ctx).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
                })
            }

            #[allow(unsafe_code)]
            #[linkme::distributed_slice($crate::registry::DYNAMIC_FACTORIES)]
            static [<FACTORY_ $name:snake:upper>]: $crate::registry::DynamicFactory = $crate::registry::DynamicFactory {
                name: $name,
                description: $description,
                create_directory: None,
                create_file: None,
                validate_config: $validate_fn,
                validate_raw_config: None,
                try_as_queryable: None,
                initialize: Some([<initialize_wrapper_ $name:snake>]),
                execute: Some([<execute_wrapper_ $name:snake>]),
                apply_table_transform: None,
            };
        }
    };
}

/// Register a table transform factory
#[macro_export]
macro_rules! register_table_transform_factory {
    (
        name: $name:expr,
        description: $description:expr,
        validate: $validate_fn:expr,
        transform: $transform_fn:expr
    ) => {
        paste::paste! {
            fn [<transform_wrapper_ $name:snake>](
                context: $crate::FactoryContext,
                input: std::sync::Arc<dyn datafusion::catalog::TableProvider>,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>, Box<dyn std::error::Error + Send + Sync>>> + Send>> {
                Box::pin(async move {
                    $transform_fn(context, input).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
                })
            }

            #[allow(unsafe_code)]
            #[linkme::distributed_slice($crate::registry::DYNAMIC_FACTORIES)]
            static [<FACTORY_ $name:snake:upper>]: $crate::registry::DynamicFactory = $crate::registry::DynamicFactory {
                name: $name,
                description: $description,
                create_directory: None,
                create_file: None,
                validate_config: $validate_fn,
                validate_raw_config: None,
                try_as_queryable: None,
                initialize: None,
                execute: None,
                apply_table_transform: Some([<transform_wrapper_ $name:snake>]),
            };
        }
    };
}

/// Config file that stores configuration as YAML
pub struct ConfigFile {
    config_yaml: Vec<u8>,
}

impl ConfigFile {
    /// Create a new config file with the given YAML content
    #[must_use]
    pub fn new(config_yaml: Vec<u8>) -> Self {
        Self { config_yaml }
    }

    /// Convert this ConfigFile into a FileHandle
    #[must_use]
    pub fn create_handle(self) -> FileHandle {
        FileHandle::new(Arc::new(Mutex::new(Box::new(self) as Box<dyn File>)))
    }
}

#[async_trait]
impl File for ConfigFile {
    async fn async_reader(&self) -> TinyFSResult<Pin<Box<dyn AsyncReadSeek>>> {
        use std::io::Cursor;
        let cursor = Cursor::new(self.config_yaml.clone());
        Ok(Box::pin(cursor))
    }

    async fn async_writer(&self) -> TinyFSResult<Pin<Box<dyn tinyfs::FileMetadataWriter>>> {
        Err(tinyfs::Error::Other(
            "Config files are read-only".to_string(),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[async_trait]
impl Metadata for ConfigFile {
    async fn metadata(&self) -> TinyFSResult<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: Some(self.config_yaml.len() as u64),
            blake3: None,
            bao_outboard: None,
            entry_type: EntryType::FileDynamic,
            timestamp: 0,
        })
    }
}
