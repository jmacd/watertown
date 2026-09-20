// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! TinyFS-backed ObjectStore implementation for DataFusion ListingTable integration.
//!
//! This module implements the object_store::ObjectStore trait to bridge TinyFS
//! storage with DataFusion's ListingTable. It maps ObjectStore paths to TinyFS
//! node IDs, enabling native predicate pushdown and streaming execution.
//!
//! Path format: "/node/{node_id}" maps to TinyFS node ID

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use crate::tinyfs_path::TinyFsPathBuilder;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use log::debug;
use object_store::{
    GetOptions, GetResult, ListResult, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result as ObjectStoreResult, path::Path as ObjectPath,
};
use tinyfs::PersistenceLayer;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

/// File series information for ObjectStore registry
#[derive(Debug, Clone)]
struct FileSeriesInfo {
    /// FileID for the file series
    file_id: tinyfs::FileID,
    /// Version information for all versions in the series
    versions: Vec<tinyfs::FileVersionInfo>,
}

/// TinyFS-backed ObjectStore implementation.
///
/// This store maps ObjectStore paths to TinyFS file handles:
/// - Path format: "/node/{node_id}"
/// - Generic over any PersistenceLayer implementation
/// - Provides file discovery and streaming access for DataFusion ListingTable
pub struct TinyFsObjectStore<P: PersistenceLayer> {
    /// Persistence layer for dynamic file discovery and version access
    persistence: P,
}

impl<P: PersistenceLayer> TinyFsObjectStore<P> {
    /// Create a new TinyFS ObjectStore from any PersistenceLayer
    #[must_use]
    pub fn new(persistence: P) -> Self {
        Self { persistence }
    }

    /// Create ObjectMeta for a specific version
    fn create_object_meta_for_version(
        &self,
        location: &ObjectPath,
        series_info: &FileSeriesInfo,
        version_num: Option<u64>,
    ) -> ObjectStoreResult<ObjectMeta> {
        match version_num {
            Some(version) => {
                // Find the specific version info
                let version_info = series_info
                    .versions
                    .iter()
                    .find(|v| v.version == version)
                    .ok_or_else(|| object_store::Error::NotFound {
                        path: location.to_string(),
                        source: format!("Version {} not found", version).into(),
                    })?;

                Ok(ObjectMeta {
                    location: location.clone(),
                    last_modified: chrono::Utc::now(), // TODO: use version timestamp
                    size: version_info.size,
                    e_tag: None,
                    version: None,
                })
            }
            None => {
                // For non-versioned access, use the latest version
                let latest_version = series_info
                    .versions
                    .iter()
                    .max_by_key(|v| v.version)
                    .ok_or_else(|| object_store::Error::NotFound {
                        path: location.to_string(),
                        source: "No versions found".into(),
                    })?;

                Ok(ObjectMeta {
                    location: location.clone(),
                    last_modified: chrono::Utc::now(),
                    size: latest_version.size,
                    e_tag: None,
                    version: None,
                })
            }
        }
    }

    /// Extract node ID from ObjectStore path
    ///
    /// Expected formats:
    /// Parse versioned path to extract series_id and version number using canonical parser
    /// Returns (series_key, version_number) where series_key is for registry lookup
    fn parse_versioned_path(&self, path: &ObjectPath) -> ObjectStoreResult<(String, Option<u64>)> {
        let path_str = path.as_ref();

        // Use canonical parser for consistency
        match parse_tinyfs_path(path_str, self.persistence.pond_uuid()) {
            Ok(parsed) => {
                // Create series key using FileID components
                let series_key = format!(
                    "node/{}/part/{}",
                    parsed.file_id.node_id(),
                    parsed.file_id.part_id()
                );
                Ok((series_key, parsed.version))
            }
            Err(err) => Err(object_store::Error::Generic {
                store: "TinyFS",
                source: err.into(),
            }),
        }
    }
}

impl<P: PersistenceLayer> fmt::Display for TinyFsObjectStore<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TinyFsObjectStore")
    }
}

impl<P: PersistenceLayer> fmt::Debug for TinyFsObjectStore<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TinyFsObjectStore")
            .field("persistence", &"<Persistence>")
            .field("node_registry", &"<NodeRegistry>")
            .finish()
    }
}

#[async_trait]
impl<P: PersistenceLayer + Clone + 'static> ObjectStore for TinyFsObjectStore<P> {
    async fn put_opts(
        &self,
        _location: &ObjectPath,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        debug!("ObjectStore put_opts called for location: {_location}");
        // TinyFS ObjectStore is read-only - data is managed through TinyFS transactions
        Err(object_store::Error::Generic {
            store: "TinyFS",
            source: "TinyFS ObjectStore is read-only. Use TinyFS transactions to write data."
                .into(),
        })
    }

    async fn put_multipart_opts(
        &self,
        _location: &ObjectPath,
        _opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn object_store::MultipartUpload>> {
        debug!("ObjectStore put_multipart_opts called for location: {_location}");
        // TinyFS ObjectStore is read-only - data is managed through TinyFS transactions
        Err(object_store::Error::Generic {
            store: "TinyFS",
            source: "TinyFS ObjectStore is read-only. Use TinyFS transactions to write data."
                .into(),
        })
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        let path = location.as_ref();
        debug!("ObjectStore get_opts called for path: {path}");
        let (series_key, version_num) = self.parse_versioned_path(location)?;
        debug!(
            "ObjectStore get_opts called for location: {location}, series_key: {series_key}, version: {version_num:?}"
        );
        let head = options.head;
        let range = format!("{:?}", options.range);
        let if_match = format!("{:?}", options.if_match);
        let if_none_match = format!("{:?}", options.if_none_match);
        debug!(
            "ObjectStore get_opts options: head={head}, range={range}, if_match={if_match}, if_none_match={if_none_match}"
        );

        // Parse the path to get node_id and part_id for dynamic discovery
        let parsed_path = parse_tinyfs_path(location.as_ref(), self.persistence.pond_uuid())
            .map_err(|err| object_store::Error::Generic {
                store: "TinyFS",
                source: err.into(),
            })?;

        // Query persistence layer dynamically for file versions
        let versions = self
            .persistence
            .list_file_versions(parsed_path.file_id)
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "TinyFS",
                source: format!("Failed to list file versions: {}", e).into(),
            })?;

        if versions.is_empty() {
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: "No file versions found".into(),
            });
        }

        // Create FileSeriesInfo dynamically
        let series_info = FileSeriesInfo {
            file_id: parsed_path.file_id,
            versions,
        };
        debug!("ObjectStore dynamically discovered file series for series_key: {series_key}");

        // Get version-specific metadata
        let object_meta =
            self.create_object_meta_for_version(location, &series_info, version_num)?;
        let size = object_meta.size;
        debug!("ObjectStore file metadata - size: {size}");
        options.check_preconditions(&object_meta)?;

        // If this is a head request, return metadata only
        if options.head {
            return Ok(GetResult {
                meta: object_meta.clone(),
                payload: object_store::GetResultPayload::Stream(futures::stream::empty().boxed()),
                range: 0..object_meta.size,
                attributes: Default::default(),
            });
        }

        // Read the specific version using persistence layer
        let version_to_read = match version_num {
            Some(v) => v,
            None => {
                // If no version specified, use the latest available version
                series_info
                    .versions
                    .iter()
                    .map(|v| v.version)
                    .max()
                    .ok_or_else(|| object_store::Error::NotFound {
                        path: location.to_string(),
                        source: "No versions available for file series".into(),
                    })?
            }
        };

        let response_range = match options.range.as_ref() {
            Some(range) => range
                .as_range(size)
                .map_err(|source| object_store::Error::Generic {
                    store: "TinyFS",
                    source: format!("Invalid range for object of size {size}: {source}").into(),
                })?,
            None => 0..size,
        };

        let mut reader = self
            .persistence
            .open_file_version(series_info.file_id, version_to_read)
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "TinyFS",
                source: format!("Failed to open version {}: {}", version_to_read, e).into(),
            })?;

        _ = reader
            .seek(std::io::SeekFrom::Start(response_range.start))
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "TinyFS",
                source: format!(
                    "Failed to seek version {version_to_read} to {}: {e}",
                    response_range.start
                )
                .into(),
            })?;

        let bytes_to_read = response_range.end - response_range.start;
        let stream = ReaderStream::new(reader.take(bytes_to_read)).map_err(move |e| {
            object_store::Error::Generic {
                store: "TinyFS",
                source: format!("Failed to stream version {version_to_read}: {e}").into(),
            }
        });

        debug!(
            "ObjectStore streaming version {version_to_read}, range {response_range:?}, logical size {size}"
        );

        Ok(GetResult {
            meta: object_meta.clone(),
            payload: object_store::GetResultPayload::Stream(stream.boxed()),
            range: response_range,
            attributes: Default::default(),
        })
    }

    async fn get_range(
        &self,
        location: &ObjectPath,
        range: Range<u64>,
    ) -> ObjectStoreResult<Bytes> {
        object_store::GetRange::Bounded(range.clone())
            .is_valid()
            .map_err(|source| object_store::Error::Generic {
                store: "TinyFS",
                source: format!("Invalid range: {source}").into(),
            })?;
        let parsed_path = parse_tinyfs_path(location.as_ref(), self.persistence.pond_uuid())
            .map_err(|err| object_store::Error::Generic {
                store: "TinyFS",
                source: err.into(),
            })?;
        let version = parsed_path
            .version
            .ok_or_else(|| object_store::Error::Generic {
                store: "TinyFS",
                source: format!(
                    "File series path '{}' must identify a specific version",
                    location
                )
                .into(),
            })?;

        debug!(
            "ObjectStore reading version {version} range {range:?} from {}",
            parsed_path.file_id
        );
        self.persistence
            .read_file_version_range(parsed_path.file_id, version, range)
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "TinyFS",
                source: format!("Failed to read version {version} range: {e}").into(),
            })
    }

    async fn delete(&self, location: &ObjectPath) -> ObjectStoreResult<()> {
        debug!("ObjectStore delete called for location: {location}");
        // TinyFS ObjectStore is read-only - data is managed through TinyFS transactions
        Err(object_store::Error::Generic {
            store: "TinyFS",
            source: "TinyFS ObjectStore is read-only. Use TinyFS transactions to delete data."
                .into(),
        })
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let persistence = self.persistence.clone();
        let prefix = prefix.map(|p| p.as_ref().to_string());
        let pond_id = self.persistence.pond_uuid();

        let prefix_str = prefix.as_ref().map(|p| p.as_ref()).unwrap_or("None");
        debug!("ObjectStore list called with prefix: {prefix_str}");

        let stream = async_stream::stream! {
            // Parse the prefix to extract both node_id and part_id for dynamic discovery
            if let Some(ref prefix_str) = prefix {
                if let Some(file_id) = extract_node_and_part_ids_from_path(prefix_str, pond_id) {
                    debug!("ObjectStore extracting file versions for file_id: {file_id}");

                    // Query persistence layer directly with FileID - no pre-registration needed!
                    // file_id already constructed by extract_node_and_part_ids_from_path
                    match persistence.list_file_versions(file_id).await {
                        Ok(versions) => {
                            let version_count = versions.len();
                            debug!("ObjectStore discovered {version_count} versions for {file_id}");

                            for version_info in versions {
                                // Filter by prefix before creating path
                                let version_path = TinyFsPathBuilder::specific_version(&file_id, version_info.version);
                                if !version_path.starts_with(prefix_str) {
                                    debug!("ObjectStore skipping version {version_path} (doesn't match prefix {prefix_str})");
                                    continue;
                                }

                                // CRITICAL: Skip 0-byte files during listing to prevent DataFusion schema inference issues
                                // 0-byte files are temporal override metadata only, not actual data files
                                if version_info.size == 0 {
                                    debug!("ObjectStore skipping 0-byte file: {version_path} (temporal override metadata only)");
                                    continue;
                                }

                                // Use clean path (DataFusion-compatible format)
                                let clean_path = version_path.clone();

                                debug!("ObjectStore discovered version: {version_path}");

                                let object_meta = ObjectMeta {
                                    location: ObjectPath::from(clean_path.clone()),
                                    last_modified: chrono::Utc::now(), // TODO: use actual timestamp from version_info
                                    size: version_info.size,
                                    e_tag: None,
                                    version: None,
                                };
                                let size = version_info.size;
                                debug!("ObjectStore yielding ObjectMeta: path={clean_path}, size={size}");
                                yield Ok(object_meta);
                            }
                        }
                        Err(e) => {
                            debug!("ObjectStore failed to list file versions for {file_id}: {e}");
                            yield Err(object_store::Error::Generic {
                                store: "TinyFS",
                                source: format!("Failed to list file versions for {}: {}", file_id, e).into(),
                            });
                        }
                    }
                } else {
                    debug!("ObjectStore could not extract node_id from prefix: {prefix_str}");
                }
            } else {
                debug!("ObjectStore list called with no prefix - no files to return");
            }
        };

        stream.boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> ObjectStoreResult<ListResult> {
        let prefix_str = prefix.map(|p| p.as_ref()).unwrap_or("None");
        debug!("ObjectStore list_with_delimiter called with prefix: {prefix_str}");
        // For simplicity, treat this the same as regular list since TinyFS
        // doesn't have a natural directory structure
        let objects: Vec<ObjectMeta> = self.list(prefix).try_collect().await?;

        let object_count = objects.len();
        debug!("ObjectStore list_with_delimiter returning {object_count} objects");
        Ok(ListResult {
            common_prefixes: vec![], // No directory structure in TinyFS
            objects,
        })
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> ObjectStoreResult<()> {
        debug!("ObjectStore copy called from: {from} to: {to}");
        // TinyFS ObjectStore is read-only - data is managed through TinyFS transactions
        Err(object_store::Error::Generic {
            store: "TinyFS",
            source: "TinyFS ObjectStore is read-only. Use TinyFS transactions to copy data.".into(),
        })
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> ObjectStoreResult<()> {
        debug!("ObjectStore copy_if_not_exists called from: {from} to: {to}");
        // TinyFS ObjectStore is read-only - data is managed through TinyFS transactions
        Err(object_store::Error::Generic {
            store: "TinyFS",
            source: "TinyFS ObjectStore is read-only. Use TinyFS transactions to copy data.".into(),
        })
    }
}

/// Canonical TinyFS path parsing result
#[derive(Debug, Clone)]
struct TinyFsPath {
    file_id: tinyfs::FileID,
    version: Option<u64>, // None = all versions, Some(n) = specific version n
}

/// Single canonical method to parse all TinyFS path formats
/// This eliminates duplication and ensures consistency across all path parsing
fn parse_tinyfs_path(path: &str, default_pond_id: uuid7::Uuid) -> Result<TinyFsPath, String> {
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    // Handle directory paths: "directory/{node_id}"
    if parts.len() == 2 && parts[0] == "directory" {
        let node_id = parts[1]
            .parse::<uuid7::Uuid>()
            .map_err(|_| format!("Invalid directory node_id UUID: {}", parts[1]))
            .map(|uuid| tinyfs::NodeID::new(uuid.to_string()))?;

        // For directories, node_id == part_id
        let file_id = tinyfs::FileID::new_from_ids(
            tinyfs::PartID::from_node_id(node_id),
            node_id,
            default_pond_id,
        );
        return Ok(TinyFsPath {
            file_id,
            version: None,
        });
    }

    // Handle file paths with pond_id:
    //   "pond/{pond_id}/part/{part_id}/node/{node_id}/version/"
    //   "pond/{pond_id}/part/{part_id}/node/{node_id}/version/{version}.parquet"
    // Also supports legacy paths without pond_id:
    //   "part/{part_id}/node/{node_id}/version/..."
    let (pond_id, part_idx) = if parts.len() >= 2 && parts[0] == "pond" {
        let pid = parts[1]
            .parse::<uuid7::Uuid>()
            .map_err(|_| format!("Invalid pond_id UUID: {}", parts[1]))?;
        (pid, 2) // part_id starts at index 2
    } else {
        (default_pond_id, 0) // legacy path, use default
    };

    // Minimum remaining: ["part", part_id, "node", node_id, "version"]
    if parts.len() < part_idx + 5
        || parts[part_idx] != "part"
        || parts[part_idx + 2] != "node"
        || parts[part_idx + 4] != "version"
    {
        return Err(format!(
            "Invalid TinyFS path format. Expected: [pond/{{pond_id}}/]part/{{part_id}}/node/{{node_id}}/version/[{{version}}.parquet], got: {}",
            path
        ));
    }

    let part_id = parts[part_idx + 1]
        .parse::<uuid7::Uuid>()
        .map_err(|_| format!("Invalid part_id UUID: {}", parts[part_idx + 1]))
        .map(|uuid| tinyfs::PartID::new(uuid.to_string()))?;

    let node_id = parts[part_idx + 3]
        .parse::<uuid7::Uuid>()
        .map_err(|_| format!("Invalid node_id UUID: {}", parts[part_idx + 3]))
        .map(|uuid| tinyfs::NodeID::new(uuid.to_string()))?;

    let version = if parts.len() == part_idx + 5 {
        None
    } else if parts.len() == part_idx + 6 {
        let version_str = parts[part_idx + 5]
            .strip_suffix(".parquet")
            .ok_or_else(|| {
                format!(
                    "Version file must end with .parquet: {}",
                    parts[part_idx + 5]
                )
            })?;
        let version_num = version_str
            .parse::<u64>()
            .map_err(|_| format!("Invalid version number: {}", version_str))?;
        Some(version_num)
    } else {
        return Err(format!("Invalid TinyFS path length: {}", path));
    };

    let file_id = tinyfs::FileID::new_from_ids(part_id, node_id, pond_id);
    Ok(TinyFsPath { file_id, version })
}

/// Extract FileID from a tinyfs:// path using canonical parser
/// Examples:
/// - "part/987fcdeb-51a2-4321-8765-432109876543/node/019945f3-031b-7e54-863d-895392f16dac/version" -> Some(file_id)
/// - "part/987fcdeb-51a2-4321-8765-432109876543/node/019945f3-031b-7e54-863d-895392f16dac/version/1.parquet" -> Some(file_id)
fn extract_node_and_part_ids_from_path(path: &str, pond_id: uuid7::Uuid) -> Option<tinyfs::FileID> {
    parse_tinyfs_path(path, pond_id)
        .ok()
        .map(|parsed| parsed.file_id)
}

/// Register TinyFsObjectStore with a SessionContext
///
/// This is a helper function to register a TinyFsObjectStore with DataFusion's SessionContext,
/// making TinyFS files accessible via the `tinyfs:///` URL scheme.
///
/// Returns the registered ObjectStore for further use if needed.
pub fn register_tinyfs_object_store<P: PersistenceLayer + Clone + 'static>(
    ctx: &datafusion::execution::context::SessionContext,
    persistence: P,
) -> Result<Arc<TinyFsObjectStore<P>>, Box<dyn std::error::Error + Send + Sync>> {
    crate::register_datafusion_functions(ctx)?;
    let object_store = Arc::new(TinyFsObjectStore::new(persistence));

    let url =
        url::Url::parse("tinyfs:///").map_err(|e| format!("Failed to parse tinyfs URL: {}", e))?;

    _ = ctx
        .runtime_env()
        .register_object_store(&url, object_store.clone());

    debug!("Registered TinyFS ObjectStore with SessionContext - ready for any TinyFS path");
    Ok(object_store)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::GetRange;
    use tinyfs::{EntryType, FileID, MemoryPersistence, PartID};

    async fn test_store() -> (TinyFsObjectStore<MemoryPersistence>, ObjectPath, Vec<u8>) {
        let persistence = MemoryPersistence::default();
        let id = FileID::new_in_partition(
            PartID::root(),
            EntryType::TablePhysicalVersion,
            tinyfs::local_pond_uuid(),
        );
        let content = b"0123456789abcdefghijklmnopqrstuvwxyz".to_vec();
        persistence
            .store_file_version(id, 1, content.clone())
            .await
            .unwrap();
        let path = ObjectPath::from(TinyFsPathBuilder::specific_version(&id, 1));
        (TinyFsObjectStore::new(persistence), path, content)
    }

    #[tokio::test]
    async fn get_range_returns_only_requested_version_bytes() {
        let (store, path, content) = test_store().await;
        let bytes = store.get_range(&path, 10..16).await.unwrap();
        assert_eq!(bytes.as_ref(), &content[10..16]);

        let bytes = store.get_range(&path, 30..100).await.unwrap();
        assert_eq!(bytes.as_ref(), &content[30..]);
    }

    #[tokio::test]
    async fn get_opts_honors_offset_and_suffix_ranges() {
        let (store, path, content) = test_store().await;

        let result = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Offset(12)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(result.range, 12..content.len() as u64);
        assert_eq!(result.bytes().await.unwrap().as_ref(), &content[12..]);

        let result = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Suffix(5)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(result.range, content.len() as u64 - 5..content.len() as u64);
        assert_eq!(
            result.bytes().await.unwrap().as_ref(),
            &content[content.len() - 5..]
        );
    }
}
