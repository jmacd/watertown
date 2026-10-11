// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! SQL execution interface for TLogFS files
//!
//! This module provides a simple interface to execute SQL queries against TLogFS files
//! without requiring the caller to understand the underlying DataFusion setup.

use crate::error::TLogFSError;
use datafusion::physical_plan::SendableRecordBatchStream; // Import the canonical version

/// Execute a SQL query against a TLogFS file and return a streaming result
///
/// This is the main interface for executing SQL queries against any TLogFS file type
/// (FileTable, FileSeries, SqlDerivedFile, etc.). The file is automatically registered
/// as a table named "source" in the DataFusion context.
///
/// Returns a stream of RecordBatch results for efficient processing of large datasets.
///
/// # Arguments
/// * `tinyfs_wd` - TinyFS working directory for file resolution
/// * `path` - Path to the TLogFS file
/// * `sql_query` - SQL query to execute (the file will be available as "source" table)
///
/// # Returns
/// A stream of RecordBatch results from the query execution
pub async fn execute_sql_on_file(
    tinyfs_wd: &tinyfs::WD,
    path: &str,
    sql_query: &str,
    provider_context: &tinyfs::ProviderContext,
) -> Result<SendableRecordBatchStream, TLogFSError> {
    // Get SessionContext from provider context
    let ctx = &provider_context.datafusion_session;

    // Resolve path to get node_id and part_id directly (anti-duplication - no wrapper function)
    use tinyfs::Lookup;
    let (_, lookup_result) = tinyfs_wd
        .resolve_path(path)
        .await
        .map_err(TLogFSError::TinyFS)?;

    match lookup_result {
        Lookup::Found(node_path) => {
            let file_handle = node_path.as_file().await.map_err(TLogFSError::TinyFS)?;

            // Get the entry type and metadata
            //let metadata = file_handle.metadata().await.map_err(TLogFSError::TinyFS)?;

            match node_path.id().entry_type() {
                tinyfs::EntryType::TablePhysicalVersion
                | tinyfs::EntryType::TablePhysicalSeries
                | tinyfs::EntryType::TableDynamic => {
                    // Use trait dispatch instead of type checking - follows anti-duplication principles

                    //let file_arc = file_handle.handle.get_file().await;
                    //let file_guard = file_arc.lock().await;

                    // @@@ This appears to be dead code: it wasn't necessary because
                    // the node.id().part_id() is the parent partition we needed.
                    //
                    // Simple and direct: get NodeIDs without unnecessary conversions
                    // let node_id = node_path.id();
                    // let part_id = {
                    //     let parent_path = node_path.dirname();
                    //     let parent_node_path =
                    //         tinyfs_wd.resolve_path(&parent_path).await.map_err(|e| {
                    //             TLogFSError::ArrowMessage(format!(
                    //                 "Failed to resolve parent path: {}",
                    //                 e
                    //             ))
                    //         })?;
                    //     match parent_node_path.1 {
                    //         Lookup::Found(parent_node) => parent_node.id(),
                    //         _ => tinyfs::NodeID::root(),
                    //     }
                    // };

                    // Single workflow: Use QueryableFile trait dispatch instead of type checking

                    let file_arc = file_handle.handle.get_file().await;
                    let file_guard = file_arc.lock().await;

                    if let Some(queryable_file) = file_guard.as_queryable() {
                        let table_provider = queryable_file
                            .as_table_provider(node_path.id(), provider_context)
                            .await
                            .map_err(|e| TLogFSError::ArrowMessage(e.to_string()))?;
                        drop(file_guard);

                        // Deregister any stale "source" table from a previous call
                        // in the same session before registering the new one
                        let _ = ctx
                            .deregister_table(datafusion::common::TableReference::bare("source"));

                        _ = ctx
                            .register_table(
                                datafusion::common::TableReference::bare("source"),
                                table_provider,
                            )
                            .map_err(|e| {
                                TLogFSError::ArrowMessage(format!(
                                    "Failed to register table 'source': {}",
                                    e
                                ))
                            })?;
                    } else {
                        return Err(TLogFSError::ArrowMessage(
                            "File does not implement QueryableFile trait".to_string(),
                        ));
                    }

                    // Execute SQL and create the stream
                    let df = ctx.sql(sql_query).await.map_err(|e| {
                        TLogFSError::ArrowMessage(format!(
                            "Failed to execute SQL query '{}': {}",
                            sql_query, e
                        ))
                    })?;

                    let stream = df.execute_stream().await.map_err(|e| {
                        TLogFSError::ArrowMessage(format!("Failed to create result stream: {}", e))
                    })?;

                    // Deregister "source" now that the execution plan holds its
                    // own Arc to the table provider -- prevents stale bindings
                    // from leaking into later queries within the same session
                    let _ =
                        ctx.deregister_table(datafusion::common::TableReference::bare("source"));

                    Ok(stream)
                }
                _ => Err(TLogFSError::ArrowMessage(format!(
                    "Path {} points to unsupported entry type for table operations: {}",
                    path,
                    node_path.entry_type()
                ))),
            }
        }
        Lookup::NotFound(full_path, _) => Err(TLogFSError::ArrowMessage(format!(
            "File not found: {}",
            full_path.display()
        ))),
        Lookup::Empty(_) => Err(TLogFSError::ArrowMessage("Empty path provided".to_string())),
    }
}

/// Get the schema for a TLogFS file without executing any queries
///
/// # Arguments
/// * `tinyfs_wd` - Working directory handle for path resolution
/// * `path` - Path to the TLogFS file
/// * `tx` - Transaction guard for consistent access
///
/// # Returns
/// The Arrow schema for the file
pub async fn get_file_schema(
    tinyfs_wd: &tinyfs::WD,
    path: &str,
    provider_context: &tinyfs::ProviderContext,
) -> Result<arrow::datatypes::SchemaRef, TLogFSError> {
    use tinyfs::Lookup;
    let (_, lookup_result) = tinyfs_wd
        .resolve_path(path)
        .await
        .map_err(TLogFSError::TinyFS)?;

    match lookup_result {
        Lookup::Found(node_path) => {
            let file_handle = node_path.as_file().await.map_err(TLogFSError::TinyFS)?;

            match node_path.entry_type() {
                tinyfs::EntryType::TablePhysicalVersion
                | tinyfs::EntryType::TablePhysicalSeries
                | tinyfs::EntryType::TableDynamic => {
                    let file_arc = file_handle.handle.get_file().await;
                    let file_guard = file_arc.lock().await;

                    // let node_id = node_path.id().await;
                    // let part_id = {
                    //     let parent_path = node_path.dirname();
                    //     let parent_node_path =
                    //         tinyfs_wd.resolve_path(&parent_path).await.map_err(|e| {
                    //             TLogFSError::ArrowMessage(format!(
                    //                 "Failed to resolve parent path: {}",
                    //                 e
                    //             ))
                    //         })?;
                    //     match parent_node_path.1 {
                    //         Lookup::Found(parent_node) => parent_node.id().await,
                    //         _ => tinyfs::NodeID::root(),
                    //     }
                    // };

                    // Get QueryableFile and table provider
                    if let Some(queryable_file) = file_guard.as_queryable() {
                        let table_provider = queryable_file
                            .as_table_provider(node_path.id(), provider_context)
                            .await
                            .map_err(|e| TLogFSError::ArrowMessage(e.to_string()))?;
                        drop(file_guard);

                        // Get schema directly from table provider
                        Ok(table_provider.schema())
                    } else {
                        Err(TLogFSError::ArrowMessage(
                            "File does not implement QueryableFile trait".to_string(),
                        ))
                    }
                }
                _ => Err(TLogFSError::ArrowMessage(format!(
                    "Unsupported file type for schema extraction: {}",
                    node_path.entry_type()
                ))),
            }
        }
        Lookup::NotFound(full_path, _) => Err(TLogFSError::ArrowMessage(format!(
            "File not found: {}",
            full_path.display()
        ))),
        Lookup::Empty(_) => Err(TLogFSError::ArrowMessage("Empty path provided".to_string())),
    }
}
