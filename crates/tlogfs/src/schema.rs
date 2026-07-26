// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

// Phase 2 TLogFS Schema Implementation - Abstraction Consolidation
use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::Result;
use deltalake::kernel::{
    DataType as DeltaDataType, PrimitiveType, StructField as DeltaStructField,
};
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tinyfs::{EntryType, FileID, NodeID, PartID};

/// Extended attributes - immutable metadata set at file creation
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ExtendedAttributes {
    /// Simple key -> String value mapping
    pub attributes: HashMap<String, String>,
}

/// Watertown system metadata key constants
pub mod watertown {
    pub const TIMESTAMP_COLUMN: &str = "watertown.timestamp_column";
    pub const MIN_TEMPORAL_OVERRIDE: &str = "watertown.min_temporal_override";
    pub const MAX_TEMPORAL_OVERRIDE: &str = "watertown.max_temporal_override";
}

impl ExtendedAttributes {
    /// Create from JSON string (for reading from OplogEntry)
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        let attributes: HashMap<String, String> = serde_json::from_str(json)?;
        Ok(Self { attributes })
    }

    /// Serialize to JSON string (for storing in OplogEntry)
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.attributes)
    }

    /// Set timestamp column name (defaults to "Timestamp" if not set)
    pub fn set_timestamp_column(&mut self, column_name: &str) -> &mut Self {
        _ = self.attributes.insert(
            watertown::TIMESTAMP_COLUMN.to_string(),
            column_name.to_string(),
        );
        self
    }

    /// Get timestamp column name with default fallback
    #[must_use]
    pub fn timestamp_column(&self) -> &str {
        self.attributes
            .get(watertown::TIMESTAMP_COLUMN)
            .map(|s| s.as_str())
            .unwrap_or("Timestamp") // Default column name
    }

    /// Set temporal overrides (convenience method for setting both min/max)
    pub fn set_temporal_overrides(
        &mut self,
        min_override: Option<i64>,
        max_override: Option<i64>,
    ) -> &mut Self {
        if let Some(min) = min_override {
            _ = self.attributes.insert(
                watertown::MIN_TEMPORAL_OVERRIDE.to_string(),
                min.to_string(),
            );
        }
        if let Some(max) = max_override {
            _ = self.attributes.insert(
                watertown::MAX_TEMPORAL_OVERRIDE.to_string(),
                max.to_string(),
            );
        }
        self
    }

    /// Get temporal overrides from extended attributes
    #[must_use]
    pub fn temporal_overrides(&self) -> Option<(i64, i64)> {
        let min = self
            .attributes
            .get(watertown::MIN_TEMPORAL_OVERRIDE)
            .and_then(|s| s.parse::<i64>().ok());
        let max = self
            .attributes
            .get(watertown::MAX_TEMPORAL_OVERRIDE)
            .and_then(|s| s.parse::<i64>().ok());

        match (min, max) {
            (Some(min), Some(max)) => Some((min, max)),
            _ => None,
        }
    }

    /// Set/get raw attributes (for future extensibility)
    pub fn set_raw(&mut self, key: &str, value: &str) -> &mut Self {
        _ = self.attributes.insert(key.to_string(), value.to_string());
        self
    }

    #[must_use]
    pub fn get_raw(&self, key: &str) -> Option<&str> {
        self.attributes.get(key).map(|s| s.as_str())
    }

    /// Create from a HashMap of arbitrary attributes
    #[must_use]
    pub fn from_map(attributes: HashMap<String, String>) -> Self {
        Self { attributes }
    }
}

/// Extract temporal range from Arrow RecordBatch
/// Returns timestamps normalized to **microseconds** for consistent storage.
///
/// This is a thin wrapper around the canonical implementation in tinyfs::arrow::parquet.
pub fn extract_temporal_range_from_batch(
    batch: &arrow::record_batch::RecordBatch,
    time_column: &str,
) -> Result<(i64, i64), crate::error::TLogFSError> {
    tinyfs::arrow::parquet::extract_temporal_bounds_from_batch(batch, time_column)
        .map_err(|e| crate::error::TLogFSError::ArrowMessage(e.to_string()))
}

/// Auto-detect timestamp column with priority order
pub fn detect_timestamp_column(
    schema: &arrow::datatypes::Schema,
) -> Result<String, crate::error::TLogFSError> {
    use arrow::datatypes::DataType;

    // Priority order for auto-detection
    let candidates = [
        "timestamp",
        "Timestamp",
        "event_time",
        "time",
        "ts",
        "datetime",
    ];

    for candidate in candidates {
        if let Some(field) = schema.column_with_name(candidate) {
            match field.1.data_type() {
                DataType::Timestamp(_, _) | DataType::Int64 => {
                    return Ok(candidate.to_string());
                }
                _ => continue,
            }
        }
    }

    Err(crate::error::TLogFSError::ArrowMessage(
        "No timestamp column found in schema".to_string(),
    ))
}

/// Compute BLAKE3 hash for any content (small or large files)
#[must_use]
pub fn compute_blake3(content: &[u8]) -> String {
    blake3::hash(content).to_hex().to_string()
}

/// Storage format for OplogEntry content
/// This field enables forward compatibility for different storage strategies
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum StorageFormat {
    /// Small files, symlinks, and dynamic nodes - content stored inline in OplogEntry
    #[serde(rename = "inline")]
    Inline,

    /// Physical directories - full directory snapshot stored in content field
    #[serde(rename = "fulldir")]
    FullDir,
}

/// Trait for converting data structures to Arrow and Delta Lake schemas
pub trait ForArrow {
    fn for_arrow() -> Vec<FieldRef>;

    /// Default implementation that converts Arrow schema to Delta Lake schema
    #[must_use]
    fn for_delta() -> Vec<DeltaStructField> {
        let afs = Self::for_arrow();

        afs.into_iter()
            .map(|af| {
                let prim_type = match af.data_type() {
                    DataType::Timestamp(TimeUnit::Microsecond, _) => PrimitiveType::Timestamp,
                    DataType::Utf8 => PrimitiveType::String,
                    DataType::Binary => PrimitiveType::Binary,
                    DataType::Int64 => PrimitiveType::Long,
                    DataType::UInt64 => PrimitiveType::Long, // UInt64 -> Long for size field
                    _ => panic!("configure this type: {:?}", af.data_type()),
                };

                DeltaStructField {
                    name: af.name().to_string(),
                    data_type: DeltaDataType::Primitive(prim_type),
                    nullable: af.is_nullable(),
                    metadata: HashMap::new(),
                }
            })
            .collect()
    }
}

/// Filesystem entry stored in the operation log
/// This represents a single filesystem operation (create, update, delete)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OplogEntry {
    /// Partition ID (parent directory for files/symlinks, self for directories)
    pub part_id: PartID,
    /// NodeID from TinyFS
    pub node_id: NodeID,
    /// Type of filesystem entry (file, directory, or symlink)
    pub file_type: EntryType, // @@@ rename one or other
    /// Timestamp when this node was modified (microseconds since Unix epoch)
    pub timestamp: i64,
    /// Per-node modification version counter (starts at 1, increments on each change)
    pub version: i64,
    /// Type-specific content:
    /// - For files: raw file data (if <= threshold) or None (if > threshold, stored externally)
    /// - For symlinks: target path
    /// - For directories: Arrow IPC encoded VersionedDirectoryEntry records
    pub content: Option<Vec<u8>>,
    /// BLAKE3 hash - semantics vary by entry type:
    ///
    /// | Entry Type            | blake3 contains                                      |
    /// |-----------------------|------------------------------------------------------|
    /// | FilePhysicalVersion   | `blake3(this_version_content)` - verifiable per-version |
    /// | FilePhysicalSeries    | `bao_root(v1||v2||...||vN)` - **CUMULATIVE**, set by `set_bao_outboard()` |
    /// | TablePhysicalVersion  | `blake3(this_version_content)` - verifiable per-version |
    /// | TablePhysicalSeries   | `blake3(this_version_content)` - per-version (parquet can't concat) |
    /// | DirectoryPhysical     | None - directories don't store blake3               |
    /// | Symlink               | None - symlinks don't store blake3                  |
    ///
    /// **CRITICAL for FilePhysicalSeries**: The blake3 field is the cumulative bao-tree
    /// root hash through ALL versions, NOT just this version's content. This means:
    /// - You CANNOT verify individual version content against this hash
    /// - Verification happens when reading concatenated series via bao-tree
    /// - The cumulative hash is extracted from `SeriesOutboard.cumulative_blake3`
    ///   when `set_bao_outboard()` is called
    pub blake3: Option<String>,
    /// File size in bytes (Some() for all files, None for directories/symlinks)
    /// NOTE: Uses i64 instead of u64 to match Delta Lake protocol (Java ecosystem legacy)
    pub size: Option<i64>,

    // NEW: Event time metadata for efficient temporal queries (FileSeries support)
    /// Minimum timestamp from data column for fast SQL range queries
    /// Some() for FileSeries entries, None for other entry types
    pub min_event_time: Option<i64>,
    /// Maximum timestamp from data column for fast SQL range queries
    /// Some() for FileSeries entries, None for other entry types
    pub max_event_time: Option<i64>,
    /// Manual override for minimum timestamp (temporal overlap resolution)
    /// Some() when user manually restricts the temporal range, None for auto-detected bounds
    pub min_override: Option<i64>,
    /// Manual override for maximum timestamp (temporal overlap resolution)
    /// Some() when user manually restricts the temporal range, None for auto-detected bounds
    pub max_override: Option<i64>,
    /// Extended attributes - immutable metadata set at file creation
    /// JSON-encoded key-value pairs for application-specific metadata
    /// For FileSeries: includes timestamp column name and other series metadata
    pub extended_attributes: Option<String>,

    /// Factory type for dynamic files/directories
    pub factory: Option<String>,

    /// Storage format for this entry's content
    /// Determines how to interpret the content field and optimize access patterns
    pub format: StorageFormat,

    /// Transaction sequence number from Steward
    /// This links OpLog records to transaction sequences for chronological ordering
    pub txn_seq: i64,

    /// Pond identity - UUID of the pond that created this record.
    /// For locally-created records, this is the local pond's UUID.
    /// For imported records (cross-pond import), this is the foreign pond's UUID.
    /// Stamped by the persistence layer at commit time; callers do not set this.
    pub pond_id: String,

    /// Bao-tree outboard data for blake3 verified streaming
    ///
    /// ALWAYS non-null for FilePhysicalVersion and FilePhysicalSeries entries.
    /// Content varies based on file type and storage mode.
    ///
    /// ## Storage Format
    /// Binary array of (left_hash, right_hash) pairs in post-order traversal.
    /// Format: Raw concatenated hash pairs, 64 bytes each (32 + 32)
    /// Size: (blocks - 1) * 64 bytes for complete file, less for partial
    /// Block size: 16KB (BlockSize::from_chunk_log(4))
    ///
    /// ## For FilePhysicalVersion
    /// Complete outboard for the version's content (VersionOutboard struct).
    /// Enables verified streaming of the individual version.
    ///
    /// ## For FilePhysicalSeries (Inline Content)
    /// Only cumulative outboard is stored (SeriesOutboard with empty version_outboard):
    /// - blake3 hash in OplogEntry is sufficient for individual version verification
    /// - Cumulative outboard enables prefix verification when appending
    ///
    /// ## For FilePhysicalSeries (Large Files)
    /// Both outboards stored (SeriesOutboard):
    /// 1. **Version-independent outboard** (offset=0): For verified streaming of
    ///    this individual version's content in isolation.
    /// 2. **Cumulative outboard**: The bao-tree state for concatenated content
    ///    from version 1 through this version.
    ///
    /// The cumulative outboard enables prefix verification when appending:
    /// new versions can verify the existing concatenated content matches
    /// the stored state before adding new data.
    pub bao_outboard: Option<Vec<u8>>,

    /// Version-collapse sentinel for `FilePhysicalSeries`.
    ///
    /// When a run of series versions is collapsed into a single merged row,
    /// that row stores `collapsed_through = Some(hi)`, meaning every version in
    /// `[collapsed_from, hi]` has been superseded by this row and must be
    /// skipped by series readers. `None` for all non-merged rows and all
    /// non-series entry types.
    pub collapsed_through: Option<i64>,

    /// Lowest series version superseded by this merged row; paired with
    /// [`Self::collapsed_through`] to form the closed range `[lo, hi]` that this
    /// row replaces.
    ///
    /// `None` on a row that also has `collapsed_through = Some(hi)` means the
    /// run starts at the beginning of the series (`lo = 0`) — this is how
    /// pre-tiering rows, which always merged the entire history, are read.
    /// `None` on a row with no `collapsed_through` simply means the row is not
    /// a merged run.
    ///
    /// Version numbers are never reassigned. A merged row is allocated a fresh
    /// (highest) version like any other append, so `version` is **not** part of
    /// the range it covers, and rows must be ordered for reading by range start
    /// rather than by `version` — see [`CollapseRange::sort_key`].
    pub collapsed_from: Option<i64>,
}

/// The closed range of series versions a row occupies (loose row) or supersedes
/// (merged collapse row).
///
/// Collapse is a *physical* regrouping of an append-only byte stream: merging
/// versions changes how bytes are stored, never their identity or order. Version
/// numbers are therefore never reassigned, and a merged row is allocated a fresh
/// (highest) version like any other append. That makes `version` useless as a
/// read ordering key for merged rows — a run covering versions 101..=200 may
/// carry version 4000 — so readers must order by [`Self::sort_key`] instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CollapseRange {
    /// First superseded version (inclusive); equals `version` for a loose row.
    pub lo: i64,
    /// Last superseded version (inclusive); equals `version` for a loose row.
    pub hi: i64,
    /// True when this row is a collapse output standing in for `[lo, hi]`.
    pub merged: bool,
}

impl CollapseRange {
    /// Range covered by `entry`. A row with `collapsed_through = Some(hi)` and
    /// no `collapsed_from` is a pre-tiering full-history merge, i.e. `[0, hi]`.
    #[must_use]
    pub fn of(entry: &OplogEntry) -> Self {
        match entry.collapsed_through {
            Some(hi) => Self {
                lo: entry.collapsed_from.unwrap_or(0),
                hi,
                merged: true,
            },
            None => Self {
                lo: entry.version,
                hi: entry.version,
                merged: false,
            },
        }
    }

    /// Ordering key placing rows in series byte order (oldest content first).
    #[must_use]
    pub fn sort_key(&self) -> i64 {
        self.lo
    }

    /// Whether `self` fully covers `other`.
    #[must_use]
    pub fn covers(&self, other: &Self) -> bool {
        self.lo <= other.lo && other.hi <= self.hi
    }
}

/// Select the live rows of a series — those not superseded by any merged
/// collapse row — ordered oldest content first.
///
/// Replaces the former `max(collapsed_through)` scalar-watermark idiom, which
/// could only express "everything up to K is dead" and so forced collapse to
/// merge the entire series history on every cycle.
///
/// Cost is `O(rows x runs)`, not `O(rows^2)`: only merged rows can supersede
/// anything, and they are few, while a single node can accumulate thousands of
/// loose versions.
#[must_use]
pub fn live_series_entries(records: &[OplogEntry]) -> Vec<&OplogEntry> {
    let runs: Vec<(usize, CollapseRange)> = records
        .iter()
        .enumerate()
        .map(|(i, e)| (i, CollapseRange::of(e)))
        .filter(|(_, r)| r.merged)
        .collect();

    let mut live: Vec<&OplogEntry> = records
        .iter()
        .enumerate()
        .filter(|(i, entry)| {
            let range = CollapseRange::of(entry);
            !runs.iter().any(|(j, run)| {
                if j == i {
                    return false; // a run never supersedes itself
                }
                // Identical ranges (should not occur) resolve to the newer row.
                run.covers(&range) && (*run != range || records[*j].version > entry.version)
            })
        })
        .map(|(_, entry)| entry)
        .collect();

    live.sort_by_key(|e| CollapseRange::of(e).sort_key());
    live
}

impl ForArrow for OplogEntry {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("part_id", DataType::Utf8, false)),
            Arc::new(Field::new("node_id", DataType::Utf8, false)),
            Arc::new(Field::new("file_type", DataType::Utf8, false)),
            Arc::new(Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            )),
            Arc::new(Field::new("version", DataType::Int64, false)),
            Arc::new(Field::new("content", DataType::Binary, true)), // Now nullable for large files
            Arc::new(Field::new("blake3", DataType::Utf8, true)), // BLAKE3 hash for large file checksums
            Arc::new(Field::new("size", DataType::Int64, true)), // File size in bytes (Int64 to match Delta Lake protocol)
            // NEW: Temporal metadata fields for FileSeries support
            Arc::new(Field::new("min_event_time", DataType::Int64, true)), // Min timestamp from data for fast queries
            Arc::new(Field::new("max_event_time", DataType::Int64, true)), // Max timestamp from data for fast queries
            Arc::new(Field::new("min_override", DataType::Int64, true)), // Manual override for temporal bounds
            Arc::new(Field::new("max_override", DataType::Int64, true)), // Manual override for temporal bounds
            Arc::new(Field::new("extended_attributes", DataType::Utf8, true)), // JSON-encoded application metadata
            Arc::new(Field::new("factory", DataType::Utf8, true)), // Factory type for dynamic files/directories
            Arc::new(Field::new("format", DataType::Utf8, false)), // Storage format - required field
            Arc::new(Field::new("txn_seq", DataType::Int64, false)), // Transaction sequence number from Steward (required)
            Arc::new(Field::new("pond_id", DataType::Utf8, false)), // Pond identity UUID (required, non-nullable)
            Arc::new(Field::new("bao_outboard", DataType::Binary, true)), // Bao-tree outboard for verified streaming
            Arc::new(Field::new("collapsed_through", DataType::Int64, true)), // Highest series version superseded by this merged row
            Arc::new(Field::new("collapsed_from", DataType::Int64, true)), // Lowest series version superseded by this merged row (None => 0)
        ]
    }
}

impl OplogEntry {
    /// Create Arrow schema for OplogEntry
    #[must_use]
    pub fn create_schema() -> Arc<arrow::datatypes::Schema> {
        Arc::new(arrow::datatypes::Schema::new(Self::for_arrow()))
    }

    /// Create entry for small file (<= threshold)
    #[must_use]
    pub fn new_small_file(
        id: FileID,
        timestamp: i64,
        version: i64,
        content: Vec<u8>,
        txn_seq: i64,
    ) -> Self {
        // INVARIANT: Dynamic EntryTypes require factory field - cannot use new_small_file
        assert!(
            !id.entry_type().is_dynamic(),
            "Cannot create OplogEntry for dynamic EntryType {:?} without factory field. Use new_dynamic_node instead.",
            id.entry_type()
        );

        let size = content.len() as u64;
        Self {
            part_id: id.part_id(),
            node_id: id.node_id(),
            file_type: id.entry_type(),
            timestamp,
            version,
            content: Some(content.clone()),
            blake3: Some(compute_blake3(&content)), // NEW: Always compute SHA256
            size: Some(size as i64),                // Cast to i64 to match Delta Lake protocol
            // Temporal metadata - None for non-series files
            min_event_time: None,
            max_event_time: None,
            min_override: None,
            max_override: None,
            extended_attributes: None,
            factory: None,
            format: StorageFormat::Inline, // Small files use inline storage
            txn_seq,
            pond_id: id.pond_id().to_string(),
            bao_outboard: None,
            collapsed_through: None,
            collapsed_from: None,
        }
    }

    /// Create entry for large file (> threshold)
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new_large_file(
        id: FileID,
        timestamp: i64,
        version: i64,
        blake3_hash: String,
        size: i64,
        txn_seq: i64,
    ) -> Self {
        // INVARIANT: Dynamic EntryTypes require factory field
        assert!(
            !id.entry_type().is_dynamic(),
            "Cannot create OplogEntry for dynamic EntryType {:?} without factory field. Use new_dynamic_node instead.",
            id.entry_type()
        );

        Self {
            part_id: id.part_id(),
            node_id: id.node_id(),
            file_type: id.entry_type(),
            timestamp,
            version,
            content: None,
            blake3: Some(blake3_hash),
            size: Some(size), // NEW: Store size explicitly
            // Temporal metadata - None for non-series files
            min_event_time: None,
            max_event_time: None,
            min_override: None,
            max_override: None,
            extended_attributes: None,
            factory: None,
            format: StorageFormat::Inline, // Large files use inline format (content is external)
            txn_seq,
            pond_id: id.pond_id().to_string(),
            bao_outboard: None,
            collapsed_through: None,
            collapsed_from: None,
        }
    }

    /// Create entry for non-file types (directories, symlinks) - always inline
    #[must_use]
    pub fn new_inline(
        id: FileID,
        timestamp: i64,
        version: i64,
        content: Vec<u8>,
        txn_seq: i64,
    ) -> Self {
        // INVARIANT: Dynamic EntryTypes require factory field
        assert!(
            !id.entry_type().is_dynamic(),
            "Cannot create OplogEntry for dynamic EntryType {:?} without factory field. Use new_dynamic_node instead.",
            id.entry_type()
        );

        Self {
            part_id: id.part_id(),
            node_id: id.node_id(),
            file_type: id.entry_type(),
            timestamp,
            version,
            content: Some(content),
            blake3: None,
            size: None, // None for directories and symlinks
            // Temporal metadata - None for directories and symlinks
            min_event_time: None,
            max_event_time: None,
            min_override: None,
            max_override: None,
            extended_attributes: None,
            factory: None,
            format: StorageFormat::Inline, // Symlinks and small metadata use inline storage
            txn_seq,
            pond_id: id.pond_id().to_string(),
            bao_outboard: None,
            collapsed_through: None,
            collapsed_from: None,
        }
    }

    /// Check if this entry represents a large file (based on content absence)
    #[must_use]
    pub fn is_large_file(&self) -> bool {
        self.content.is_none() && self.file_type.is_file()
    }

    /// Get file size (guaranteed for files, None for directories/symlinks)
    #[must_use]
    pub fn file_size(&self) -> Option<i64> {
        self.size
    }

    /// Create entry for FileSeries with temporal metadata extraction
    /// This is the specialized constructor for time series data
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new_file_series(
        id: FileID,
        timestamp: i64,
        version: i64,
        content: Vec<u8>,
        min_event_time: i64,
        max_event_time: i64,
        extended_attributes: ExtendedAttributes,
        txn_seq: i64,
    ) -> Self {
        // INVARIANT: Dynamic EntryTypes require factory field
        assert!(
            !id.entry_type().is_dynamic(),
            "Cannot create OplogEntry for dynamic EntryType {:?} without factory field. Use new_dynamic_node instead.",
            id.entry_type()
        );

        let size = content.len() as u64;
        Self {
            part_id: id.part_id(),
            node_id: id.node_id(),
            file_type: id.entry_type(),
            timestamp,
            version,
            content: Some(content.clone()),
            blake3: Some(compute_blake3(&content)),
            size: Some(size as i64), // Cast to i64 to match Delta Lake protocol
            // Temporal metadata for efficient DataFusion queries
            min_event_time: Some(min_event_time),
            max_event_time: Some(max_event_time),
            min_override: None, // No overrides by default
            max_override: None, // No overrides by default
            extended_attributes: Some(extended_attributes.to_json().unwrap_or_default()),
            factory: None,                 // Physical file, no factory
            format: StorageFormat::Inline, // Small FileSeries use inline storage
            txn_seq,
            pond_id: id.pond_id().to_string(),
            bao_outboard: None,
            collapsed_through: None,
            collapsed_from: None,
        }
    }

    /// Create entry for large FileSeries (> threshold) with temporal metadata
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new_large_file_series(
        id: FileID,
        timestamp: i64,
        version: i64,
        blake3_hash: String,
        size: i64, // Changed from u64 to i64 to match Delta Lake protocol
        min_event_time: i64,
        max_event_time: i64,
        extended_attributes: ExtendedAttributes,
        txn_seq: i64,
    ) -> Self {
        // INVARIANT: Dynamic EntryTypes require factory field
        assert!(
            !id.entry_type().is_dynamic(),
            "Cannot create OplogEntry for dynamic EntryType {:?} without factory field. Use new_dynamic_node instead.",
            id.entry_type()
        );
        assert!(
            id.entry_type().is_series_file() || id.entry_type() == EntryType::FilePhysicalSeries,
            "new_large_file_series requires a series EntryType, got {:?}",
            id.entry_type()
        );

        Self {
            part_id: id.part_id(),
            node_id: id.node_id(),
            file_type: id.entry_type(),
            timestamp,
            version,
            content: None,
            blake3: Some(blake3_hash),
            size: Some(size),
            // Temporal metadata for efficient DataFusion queries
            min_event_time: Some(min_event_time),
            max_event_time: Some(max_event_time),
            min_override: None, // No overrides by default
            max_override: None, // No overrides by default
            extended_attributes: Some(extended_attributes.to_json().unwrap_or_default()),
            factory: None,                 // Physical file, no factory
            format: StorageFormat::Inline, // Large FileSeries use inline format (content is external)
            txn_seq,
            pond_id: id.pond_id().to_string(),
            bao_outboard: None,
            collapsed_through: None,
            collapsed_from: None,
        }
    }

    /// Get extended attributes if present
    #[must_use]
    pub fn get_extended_attributes(&self) -> Option<ExtendedAttributes> {
        self.extended_attributes
            .as_ref()
            .and_then(|json| ExtendedAttributes::from_json(json).ok())
    }

    /// Check if this entry is a FileSeries with temporal metadata
    #[must_use]
    pub fn is_series_file(&self) -> bool {
        self.file_type.is_series_file()
    }

    /// Get temporal range for series files
    #[must_use]
    pub fn temporal_range(&self) -> Option<(i64, i64)> {
        match (self.min_event_time, self.max_event_time) {
            (Some(min), Some(max)) => Some((min, max)),
            _ => None,
        }
    }

    /// Get temporal overrides if set
    #[must_use]
    pub fn temporal_overrides(&self) -> Option<(i64, i64)> {
        match (self.min_override, self.max_override) {
            (Some(min), Some(max)) => Some((min, max)),
            _ => None,
        }
    }

    /// Get effective temporal range (overrides take precedence over auto-detected range)
    /// This is the key method for temporal overlap detection and resolution
    #[must_use]
    pub fn effective_temporal_range(&self) -> Option<(i64, i64)> {
        // Use overrides if available, otherwise fall back to auto-detected range
        if let Some(overrides) = self.temporal_overrides() {
            Some(overrides)
        } else {
            self.temporal_range()
        }
    }

    /// Set temporal overrides (for command-line tools)
    pub fn set_temporal_overrides(&mut self, min_override: Option<i64>, max_override: Option<i64>) {
        self.min_override = min_override;
        self.max_override = max_override;
    }

    /// Clear temporal overrides (reset to auto-detected bounds)
    pub fn clear_temporal_overrides(&mut self) {
        self.min_override = None;
        self.max_override = None;
    }

    /// Extract consolidated metadata
    #[must_use]
    pub fn metadata(&self) -> tinyfs::NodeMetadata {
        // For FilePhysicalSeries, the canonical blake3 is the cumulative hash
        // stored in SeriesOutboard, not the per-version hash in self.blake3.
        // Every written version has a SeriesOutboard computed in poll_shutdown.
        let blake3 = if self.file_type == EntryType::FilePhysicalSeries {
            if let Some(bao) = &self.bao_outboard {
                let so = utilities::bao_outboard::SeriesOutboard::from_bytes(bao)
                    .expect("FilePhysicalSeries bao_outboard must be valid SeriesOutboard");
                Some(
                    blake3::Hash::from_bytes(so.cumulative_blake3)
                        .to_hex()
                        .to_string(),
                )
            } else {
                // No bao_outboard yet — this is a pending/unwritten file.
                self.blake3.clone()
            }
        } else {
            self.blake3.clone()
        };

        tinyfs::NodeMetadata {
            version: self.version as u64,
            size: self.size.map(|s| s as u64),
            blake3,
            bao_outboard: self.bao_outboard.clone(),
            entry_type: self.file_type,
            timestamp: self.timestamp,
        }
    }

    /// Create entry for dynamic node with factory type and configuration
    #[must_use]
    pub fn new_dynamic_node(
        id: FileID,
        timestamp: i64,
        version: i64,
        factory_type: &str,
        config_content: Vec<u8>,
        txn_seq: i64,
    ) -> Self {
        // For ALL dynamic nodes (both files and directories):
        // - Config goes in content field (YAML bytes)
        // - Factory type goes in factory field
        // - No special encoding needed - just store the raw YAML
        Self {
            part_id: id.part_id(),
            node_id: id.node_id(),
            file_type: id.entry_type(),
            timestamp,
            version,
            content: Some(config_content),
            blake3: None,
            size: None,
            min_event_time: None,
            max_event_time: None,
            min_override: None,
            max_override: None,
            extended_attributes: None,
            factory: Some(factory_type.to_string()), // Factory type identifier
            format: StorageFormat::Inline,           // Config is always inline
            txn_seq,
            pond_id: id.pond_id().to_string(),
            bao_outboard: None,
            collapsed_through: None,
            collapsed_from: None,
        }
    }

    /// Check if this entry is a dynamic node (has factory type)
    #[must_use]
    pub fn is_dynamic(&self) -> bool {
        self.factory.is_some()
    }

    /// Get factory type if this is a dynamic node
    #[must_use]
    pub fn factory_type(&self) -> Option<&str> {
        self.factory.as_deref()
    }

    /// Get factory configuration content if this is a dynamic node (verified)
    /// This verifies the BLAKE3 hash before returning.
    ///
    /// # Errors
    /// Returns `ContentIntegrityError` if the hash doesn't match.
    /// Returns `ContentMissingHash` if content exists but blake3 is None.
    pub fn factory_config(&self) -> Result<Option<&[u8]>, crate::TLogFSError> {
        if self.is_dynamic() {
            self.verified_content()
        } else {
            Ok(None)
        }
    }

    /// Get the bao-tree outboard data if present
    #[must_use]
    pub fn get_bao_outboard(&self) -> Option<&[u8]> {
        self.bao_outboard.as_deref()
    }

    /// Set the bao-tree outboard data.
    ///
    /// # FilePhysicalSeries: Updates blake3 to Cumulative Hash
    ///
    /// For `FilePhysicalSeries`, this method performs a **critical side effect**:
    /// it extracts `cumulative_blake3` from the `SeriesOutboard` and overwrites
    /// the `blake3` field.
    ///
    /// **Why?** The `blake3` field for series types must be the cumulative
    /// bao-tree root hash: `bao_root(v1 || v2 || ... || vN)`. This hash cannot
    /// be computed from individual version hashes - it requires the cumulative
    /// outboard computation done during version append.
    ///
    /// The `SeriesOutboard.cumulative_blake3` field contains exactly this value,
    /// computed incrementally as versions are appended. By extracting it here,
    /// we ensure the OplogEntry's blake3 always reflects the cumulative state.
    ///
    /// **Design Decision**: We could have required callers to set blake3 separately,
    /// but that would be error-prone. Coupling the outboard storage with blake3
    /// update ensures consistency.
    pub fn set_bao_outboard(&mut self, outboard: Vec<u8>) {
        // Store the bao_outboard. For FilePhysicalSeries, the cumulative blake3
        // lives inside SeriesOutboard.cumulative_blake3 and is extracted by
        // metadata() on read. We do NOT overwrite self.blake3 here because
        // large files use self.blake3 as the storage key for lookup.
        self.bao_outboard = Some(outboard);
    }

    /// Check if this entry has bao-tree outboard data
    #[must_use]
    pub fn has_bao_outboard(&self) -> bool {
        self.bao_outboard.is_some()
    }

    /// Create a new small file entry with bao-tree outboard
    /// This is the preferred constructor for FilePhysicalSeries entries
    #[must_use]
    pub fn new_small_file_with_outboard(
        id: FileID,
        timestamp: i64,
        version: i64,
        content: Vec<u8>,
        txn_seq: i64,
        bao_outboard: Vec<u8>,
    ) -> Self {
        let mut entry = Self::new_small_file(id, timestamp, version, content, txn_seq);
        entry.bao_outboard = Some(bao_outboard);
        entry
    }

    /// Get verified content bytes, checking BLAKE3 hash if available.
    ///
    /// For content that has a blake3 hash stored, this verifies integrity before returning.
    /// For content without a hash (e.g., directory snapshots), returns content directly.
    ///
    /// # Entry Type Verification Behavior
    ///
    /// | Entry Type            | Verification                                     |
    /// |-----------------------|--------------------------------------------------|
    /// | FilePhysicalVersion   | `blake3(content) == stored_blake3` [OK]            |
    /// | FilePhysicalSeries    | **SKIPPED** - see below                          |
    /// | TablePhysicalVersion  | `blake3(content) == stored_blake3` [OK]            |
    /// | TablePhysicalSeries   | `blake3(content) == stored_blake3` [OK]            |
    /// | DirectoryPhysical     | None stored - returns content directly           |
    /// | Symlink               | None stored - returns content directly           |
    ///
    /// # Why FilePhysicalSeries Skips Verification
    ///
    /// For `FilePhysicalSeries`, the `blake3` field contains the **cumulative**
    /// bao-tree root hash: `bao_root(v1 || v2 || ... || vN)`. This is fundamentally
    /// different from `blake3(this_version_content)`.
    ///
    /// You **cannot** verify individual version content against a cumulative hash.
    /// The math doesn't work: `blake3(v2) != bao_root(v1 || v2)`.
    ///
    /// Instead, integrity verification for series content happens:
    /// 1. At write time: Content is hashed into the cumulative outboard
    /// 2. At read time: BaoValidatingReader validates concatenated content against cumulative hash
    ///
    /// # Errors
    /// Returns `ContentIntegrityError` if the hash doesn't match.
    /// Returns `ContentMissingHash` if content exists but blake3 is None for types that require it.
    pub fn verified_content(&self) -> Result<Option<&[u8]>, crate::TLogFSError> {
        let Some(content) = self.content.as_deref() else {
            return Ok(None);
        };

        // Directories and symlinks don't store blake3 - return content directly
        if matches!(
            self.file_type,
            EntryType::DirectoryPhysical | EntryType::DirectoryDynamic | EntryType::Symlink
        ) {
            return Ok(Some(content));
        }

        // FilePhysicalSeries: blake3 is CUMULATIVE (bao_root of all versions concatenated).
        // Individual version content CANNOT be verified against cumulative hash.
        // Verification happens via bao-tree when reading the concatenated series.
        // See bao-tree-design.md "Unified blake3 Field Semantics" for details.
        if self.file_type == EntryType::FilePhysicalSeries {
            return Ok(Some(content));
        }

        // For files and symlinks with content, verify blake3 if present
        if let Some(expected_hash) = &self.blake3 {
            let actual_hash = blake3::hash(content).to_hex().to_string();
            if actual_hash != *expected_hash {
                return Err(crate::TLogFSError::ContentIntegrityError {
                    expected: expected_hash.clone(),
                    actual: actual_hash,
                });
            }
        }
        // Note: Small files in the old format might not have blake3 hashes
        // In that case, we return the content without verification
        // New writes always compute blake3 via new_small_file()

        Ok(Some(content))
    }

    /// Get verified content bytes, requiring content to be present.
    /// This is a convenience wrapper around `verified_content()` that returns an error
    /// if content is None.
    ///
    /// # Errors
    /// Returns `ContentIntegrityError` if the hash doesn't match.
    /// Returns `Missing` if content is None.
    pub fn verified_content_required(&self) -> Result<&[u8], crate::TLogFSError> {
        self.verified_content()?.ok_or(crate::TLogFSError::Missing)
    }

    /// Create entry for directory full snapshot (new storage format)
    /// This constructor creates a complete directory state snapshot instead of incremental changes
    #[must_use]
    pub fn new_directory_full_snapshot(
        id: FileID,
        timestamp: i64,
        version: i64,
        content: Vec<u8>, // Serialized DirectoryEntry[]
        txn_seq: i64,
    ) -> Self {
        Self {
            part_id: id.part_id(),
            node_id: id.node_id(),
            file_type: id.entry_type(),
            timestamp,
            version,
            content: Some(content),
            blake3: None,
            size: None,
            min_event_time: None,
            max_event_time: None,
            min_override: None,
            max_override: None,
            extended_attributes: None,
            factory: None,
            format: StorageFormat::FullDir, // Full directory snapshot
            txn_seq,
            pond_id: id.pond_id().to_string(),
            bao_outboard: None,
            collapsed_through: None,
            collapsed_from: None,
        }
    }
}

/// Type alias - use tinyfs::DirectoryEntry as the canonical type
/// This avoids duplicate struct definitions and maintains a single source of truth.
pub type DirectoryEntry = tinyfs::DirectoryEntry;

/// Type alias for backward compatibility during transition
#[deprecated(note = "Use DirectoryEntry instead")]
pub type VersionedDirectoryEntry = tinyfs::DirectoryEntry;

/// ForArrow implementation for tinyfs::DirectoryEntry to enable Arrow/Parquet serialization
impl ForArrow for tinyfs::DirectoryEntry {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("child_node_id", DataType::Utf8, false)),
            Arc::new(Field::new("entry_type", DataType::Utf8, false)),
            Arc::new(Field::new("version_last_modified", DataType::Int64, false)),
            Arc::new(Field::new("pond_id", DataType::Utf8, true)),
        ]
    }
}

/// Encode DirectoryEntry records as Arrow IPC bytes for storage in OplogEntry.content
pub fn encode_directory_entries(
    entries: &[DirectoryEntry],
) -> Result<Vec<u8>, crate::error::TLogFSError> {
    use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};

    let entry_count = entries.len();
    debug!("encode_directory_entries() - encoding {entry_count} entries");
    for (i, entry) in entries.iter().enumerate() {
        let name = &entry.name;
        let child_node_id = &entry.child_node_id;
        debug!("  Entry {i}: name='{name}', child_node_id='{child_node_id}'");
    }

    // Use serde_arrow consistently for both empty and non-empty cases
    let batch = serde_arrow::to_record_batch(&DirectoryEntry::for_arrow(), &entries)?;

    let row_count = batch.num_rows();
    let col_count = batch.num_columns();
    debug!("encode_directory_entries() - created batch with {row_count} rows, {col_count} columns");

    let mut buffer = Vec::new();
    let options = IpcWriteOptions::default();
    let mut writer =
        StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options)?;
    writer.write(&batch)?;
    writer.finish()?;

    let buffer_len = buffer.len();
    debug!("encode_directory_entries() - encoded to {buffer_len} bytes");
    Ok(buffer)
}

/// Legacy function name for backward compatibility
#[deprecated(note = "Use encode_directory_entries instead")]
pub fn encode_versioned_directory_entries(
    entries: &[DirectoryEntry],
) -> Result<Vec<u8>, crate::error::TLogFSError> {
    encode_directory_entries(entries)
}

/// Decode DirectoryEntry records from Arrow IPC bytes
pub fn decode_directory_entries(
    content: &[u8],
) -> Result<Vec<DirectoryEntry>, crate::error::TLogFSError> {
    use arrow::ipc::reader::StreamReader;

    // Handle empty directories (0 bytes of content)
    if content.is_empty() {
        return Ok(Vec::new());
    }

    debug!(
        "decode_directory_entries() - processing {} bytes",
        content.len()
    );

    let mut reader = StreamReader::try_new(std::io::Cursor::new(content), None).map_err(|e| {
        crate::error::TLogFSError::ArrowMessage(format!("Failed to create IPC StreamReader: {}", e))
    })?;

    if let Some(batch_result) = reader.next() {
        let batch = batch_result.map_err(|e| {
            crate::error::TLogFSError::ArrowMessage(format!("Failed to read IPC batch: {}", e))
        })?;

        let entries: Vec<DirectoryEntry> = serde_arrow::from_record_batch(&batch)?;
        debug!(
            "decode_directory_entries() - decoded {} entries",
            entries.len()
        );
        Ok(entries)
    } else {
        Ok(Vec::new())
    }
}
