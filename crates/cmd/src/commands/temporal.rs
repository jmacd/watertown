// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;

use crate::common::ShipContext;
use log::debug;

// Note! the use of milliseconds in this file is arbitrary, and not correct.
// we should use Nanoseconds if we want to perform overlap detection on OTel
// data, and we should use Seconds if we want to cleanly support Hyrovu data,
// either way this is not ideal because the Export logic deals in seconds and
// calls the parse_timestamp_millis function here.

/// Simple overlap detection using direct time series data analysis
pub async fn detect_overlaps_command(
    ship_context: &ShipContext,
    patterns: &[String],
    verbose: bool,
    format: &str,
) -> Result<()> {
    debug!("detect_overlaps_command called with patterns and verbose flag");

    if patterns.is_empty() {
        return Err(anyhow!("At least one file pattern must be specified"));
    }

    let mut ship = ship_context.open_pond().await?;
    crate::common::with_read_transaction(
        &mut ship,
        ship_context.original_args.clone(),
        async |tx| detect_overlaps_impl(tx, patterns, verbose, format).await,
    )
    .await
}

async fn detect_overlaps_impl(
    tx: &mut steward::Transaction<'_>,
    patterns: &[String],
    verbose: bool,
    format: &str,
) -> Result<()> {
    let pattern_count = patterns.len();
    debug!("Starting simplified temporal overlap detection for {pattern_count} patterns");

    // SIMPLIFIED APPROACH: Use TLogFS factory directly to get time series data

    // Get TinyFS root for file access (guard derefs to FS)
    let tinyfs_root = Arc::new(tx.root().await?);

    // NodeTable no longer needed - TemporalFilteredListingTable handles metadata internally

    // Get the transaction's SessionContext with pre-registered ObjectStore
    // This ensures single ObjectStore per transaction, preventing registry conflicts
    let ctx = tx.session_context().await?;

    // Collect all matching file paths and their metadata
    let mut file_info = Vec::new();

    for pattern in patterns {
        debug!("Resolving pattern: {pattern}");

        // Resolve pattern to files using TinyFS pattern matching
        let matches = tinyfs_root
            .collect_matches(pattern)
            .await
            .map_err(|e| anyhow!("Failed to resolve pattern '{}': {}", pattern, e))?;

        for (node_path, _captured) in matches {
            if let Some(file_node) = node_path.into_file().await {
                let metadata = file_node
                    .metadata()
                    .await
                    .map_err(|e| anyhow!("Failed to get metadata: {}", e))?;

                if metadata.entry_type.is_series_file() {
                    let path_str = node_path.path().to_string_lossy().to_string();

                    // Use resolve_path to get both the parent directory and lookup result
                    let (_parent_wd, lookup) = tinyfs_root
                        .resolve_path(&path_str)
                        .await
                        .map_err(|e| anyhow!("Failed to resolve path {}: {}", path_str, e))?;

                    match lookup {
                        tinyfs::Lookup::Found(found_node) => {
                            let node_id = found_node.id();
                            let part_id = node_id.part_id();

                            file_info.push((path_str, node_id, part_id));
                        }
                        _ => {
                            return Err(anyhow!("File not found: {}", path_str));
                        }
                    }
                }
            }
        }
    }

    if file_info.is_empty() {
        debug!("No FileSeries files found matching the specified patterns");
        return Ok(());
    }

    let file_count = file_info.len();
    debug!("Found {file_count} files for overlap analysis");

    // Create a UNION query to get all data from all versions sorted by timestamp
    let mut union_parts = Vec::new();

    for (origin_id, (path_str, node_id, _part_id)) in file_info.iter().enumerate() {
        // Get all versions of this file from OpLog records
        let pond = tx
            .as_pond()
            .expect("temporal command requires a pond transaction");
        let records = pond
            .query_records(*node_id)
            .await
            .map_err(|e| anyhow!("Failed to get records for {}: {}", path_str, e))?;

        // Filter out empty versions (size == 0) to avoid Parquet parsing errors,
        // and rows superseded by a collapse merge row's version range.
        let live: std::collections::HashSet<i64> = tlogfs::schema::live_series_entries(&records)
            .into_iter()
            .map(|r| r.version)
            .collect();
        let versions: Vec<_> = records
            .into_iter()
            .filter(|r| {
                !matches!(r.format, tlogfs::schema::StorageFormat::Inline)
                    && r.size.unwrap_or(0) > 0
                    && live.contains(&r.version)
            })
            .collect();

        let version_count = versions.len();
        debug!("Found file: {path_str} (node: {node_id}) with {version_count} non-empty versions");

        // Create a TemporalFilteredListingTable for each version using the new approach
        debug!("Creating table providers for {path_str} with {version_count} versions");

        debug!("\nAnalyzing {}: {} versions", path_str, version_count);

        for record in versions {
            let version = record.version;
            let size = record.size.unwrap_or(0);
            debug!("Creating table provider for {path_str} version {version} (size: {size})");

            let context = tx.provider_context()?;
            let table_provider = provider::create_table_provider(
                *node_id,
                &context,
                provider::TableProviderOptions {
                    version_selection: provider::VersionSelection::SpecificVersion(version as u64),
                    additional_urls: Vec::new(),
                },
            )
            .await
            .map_err(|e| {
                anyhow!(
                    "Failed to create TemporalFilteredListingTable for {} v{}: {}",
                    path_str,
                    version,
                    e
                )
            })?;

            // Register table with unique name including version
            let table_name = format!("file_{}_{}", origin_id, version);
            _ = ctx
                .register_table(&table_name, table_provider)
                .map_err(|e| anyhow!("Failed to register table '{}': {}", table_name, e))?;

            // Query this individual table provider to get its statistics
            let stats_query = format!(
                "SELECT COUNT(*) as row_count, MIN(timestamp) as min_ts, MAX(timestamp) as max_ts FROM {}",
                table_name
            );
            let stats_df = ctx
                .sql(&stats_query)
                .await
                .map_err(|e| anyhow!("Failed to query stats for table '{}': {}", table_name, e))?;
            let stats_batches = stats_df.collect().await.map_err(|e| {
                anyhow!("Failed to collect stats for table '{}': {}", table_name, e)
            })?;

            // Extract and print the statistics
            if let Some(batch) = stats_batches.first() {
                if batch.num_rows() > 0 {
                    let row_count_col = batch.column_by_name("row_count").ok_or_else(|| {
                        anyhow::anyhow!("Expected 'row_count' column in statistics")
                    })?;
                    let min_ts_col = batch
                        .column_by_name("min_ts")
                        .ok_or_else(|| anyhow::anyhow!("Expected 'min_ts' column in statistics"))?;
                    let max_ts_col = batch
                        .column_by_name("max_ts")
                        .ok_or_else(|| anyhow::anyhow!("Expected 'max_ts' column in statistics"))?;

                    let row_count = row_count_col
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .ok_or_else(|| anyhow::anyhow!("Expected Int64 type for 'row_count'"))?
                        .value(0);

                    // Handle potential null timestamps (empty tables)
                    let min_ts_str = if min_ts_col.is_null(0) {
                        "NULL".to_string()
                    } else {
                        // @@@ This was not planned
                        match min_ts_col.data_type() {
                            arrow::datatypes::DataType::Timestamp(
                                arrow::datatypes::TimeUnit::Millisecond,
                                _,
                            ) => {
                                let min_ts = min_ts_col
                                    .as_any()
                                    .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                                    .expect("downcast failed after type match")
                                    .value(0);
                                format_timestamp(min_ts)
                            }
                            arrow::datatypes::DataType::Timestamp(
                                arrow::datatypes::TimeUnit::Second,
                                _,
                            ) => {
                                let min_ts = min_ts_col
                                    .as_any()
                                    .downcast_ref::<arrow::array::TimestampSecondArray>()
                                    .expect("downcast failed after type match")
                                    .value(0);
                                format_timestamp(min_ts * 1000)
                            }
                            _ => "UNKNOWN_TYPE".to_string(),
                        }
                    };

                    let max_ts_str = if max_ts_col.is_null(0) {
                        "NULL".to_string()
                    } else {
                        match max_ts_col.data_type() {
                            arrow::datatypes::DataType::Timestamp(
                                arrow::datatypes::TimeUnit::Millisecond,
                                _,
                            ) => {
                                let max_ts = max_ts_col
                                    .as_any()
                                    .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                                    .expect("downcast failed after type match")
                                    .value(0);
                                format_timestamp(max_ts)
                            }
                            arrow::datatypes::DataType::Timestamp(
                                arrow::datatypes::TimeUnit::Second,
                                _,
                            ) => {
                                let max_ts = max_ts_col
                                    .as_any()
                                    .downcast_ref::<arrow::array::TimestampSecondArray>()
                                    .expect("downcast failed after type match")
                                    .value(0);
                                format_timestamp(max_ts * 1000)
                            }
                            _ => "UNKNOWN_TYPE".to_string(),
                        }
                    };

                    debug!(
                        "    Version {}: {} rows, {} to {}",
                        version, row_count, min_ts_str, max_ts_str
                    );
                } else {
                    debug!("    Version {}: 0 rows (empty)", version);
                }
            }

            // Add to UNION query with origin tracking (timestamp and metadata)
            union_parts.push(format!(
                "(SELECT timestamp, {} as _node_id, {} as _version, '{}' as _file_path FROM {})",
                origin_id, version, path_str, table_name
            ));
        }
    }

    // Create simple query: just timestamp and metadata, sorted by timestamp
    let union_query = format!(
        "SELECT timestamp, _node_id, _version, _file_path FROM ({}) ORDER BY timestamp",
        union_parts.join(" UNION ALL ")
    );
    debug!("Generated simple UNION query: {union_query}");

    // Execute the query to get all data sorted by timestamp
    let dataframe = ctx
        .sql(&union_query)
        .await
        .map_err(|e| anyhow!("Failed to execute UNION query: {}", e))?;

    let all_batches = dataframe
        .collect()
        .await
        .map_err(|e| anyhow!("Failed to collect query results: {}", e))?;

    if all_batches.is_empty() {
        debug!("No data found in specified patterns");
        return Ok(());
    }

    // Create origin-to-path mapping for output
    let mut origin_to_path = HashMap::new();
    for (origin_id, (path_str, _node_id, _part_id)) in file_info.iter().enumerate() {
        _ = origin_to_path.insert(origin_id as i64, path_str.clone());
    }

    // Analyze the combined batches for overlaps
    let overlap_analysis = analyze_temporal_overlaps(&all_batches, verbose, origin_to_path)?;

    // Output results based on format
    match format {
        "summary" => print_overlap_summary(&overlap_analysis),
        "full" => print_overlap_details(&overlap_analysis),
        _ => {
            return Err(anyhow!(
                "Invalid format '{}'. Use summary, full, or json",
                format
            ));
        }
    }

    Ok(())
}

/// Data structure for overlap analysis results
#[derive(Debug)]
struct OverlapAnalysis {
    total_points: usize,
    overlapping_points: usize,
    overlap_runs: Vec<OverlapRun>,
    origin_statistics: HashMap<i64, OriginStats>,
    timeline: Vec<TimelineSegment>,
    origin_to_path: HashMap<i64, String>,
}

#[derive(Debug)]
enum TimelineSegment {
    Run {
        origin_id: i64,
        start_timestamp: i64,
        end_timestamp: i64,
        point_count: usize,
    },
    Overlap {
        start_timestamp: i64,
        end_timestamp: i64,
        points: Vec<(i64, i64)>, // (timestamp, origin_id) pairs
    },
}

#[derive(Debug)]
struct OverlapRun {
    start_timestamp: i64,
    end_timestamp: i64,
    duration_ms: i64,
    point_count: usize,
    origins_involved: Vec<i64>,
}

#[derive(Debug)]
struct OriginStats {
    point_count: usize,
    time_range: Option<(i64, i64)>,
}

/// Analyze temporal overlaps in the interleaved data
fn analyze_temporal_overlaps(
    batches: &[arrow::record_batch::RecordBatch],
    _verbose: bool,
    origin_to_path: HashMap<i64, String>,
) -> Result<OverlapAnalysis> {
    // Collect all data points across all batches
    let mut data_points = Vec::new();
    let mut origin_statistics = HashMap::new();

    for batch in batches {
        // Extract columns
        let timestamp_col = batch
            .column_by_name("timestamp")
            .ok_or_else(|| anyhow!("No timestamp column found"))?;
        let origin_col = batch
            .column_by_name("_node_id")
            .ok_or_else(|| anyhow!("No _node_id column found"))?;

        // Handle timestamp as either millisecond or second timestamp types
        let timestamps = match timestamp_col.data_type() {
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
                timestamp_col
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                    .ok_or_else(|| anyhow!("Failed to cast timestamp column"))?
                    .values()
                    .to_vec()
            }
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => {
                // Convert seconds to milliseconds for consistent internal representation
                timestamp_col
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampSecondArray>()
                    .ok_or_else(|| anyhow!("Failed to cast timestamp column"))?
                    .values()
                    .iter()
                    .map(|&ts| ts * 1000)
                    .collect() // Convert seconds to milliseconds
            }
            _ => {
                return Err(anyhow!(
                    "Unsupported timestamp column type: {:?}",
                    timestamp_col.data_type()
                ));
            }
        };

        let origin_ids = origin_col
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| anyhow!("Failed to cast _node_id column"))?
            .values()
            .to_vec();

        // Collect all data points
        for (&timestamp, &origin_id) in timestamps.iter().zip(origin_ids.iter()) {
            data_points.push((timestamp, origin_id));

            // Update origin statistics
            let stats = origin_statistics.entry(origin_id).or_insert(OriginStats {
                point_count: 0,
                time_range: None,
            });
            stats.point_count += 1;
            stats.time_range = match stats.time_range {
                Some((min, max)) => Some((min.min(timestamp), max.max(timestamp))),
                None => Some((timestamp, timestamp)),
            };
        }
    }

    // Sort all data points by timestamp to create timeline
    data_points.sort_by_key(|&(timestamp, _)| timestamp);

    // Analyze timeline for runs and overlaps
    let timeline = analyze_timeline(&data_points);

    Ok(OverlapAnalysis {
        total_points: data_points.len(),
        overlapping_points: timeline
            .iter()
            .map(|segment| match segment {
                TimelineSegment::Overlap { points, .. } => points.len(),
                _ => 0,
            })
            .sum(),
        overlap_runs: Vec::new(), // Will be populated from timeline
        origin_statistics,
        timeline,
        origin_to_path,
    })
}

/// Analyze timeline of data points to identify runs, overlaps, and gaps
fn analyze_timeline(data_points: &[(i64, i64)]) -> Vec<TimelineSegment> {
    let mut timeline = Vec::new();

    if data_points.is_empty() {
        return timeline;
    }

    let mut current_run: Option<(i64, i64, i64, usize)> = None; // (origin_id, start_ts, end_ts, count)
    let overlap_threshold_ms = 60000; // 1 minute - points within this are considered potentially overlapping

    for i in 0..data_points.len() {
        let (timestamp, origin_id) = data_points[i];

        match current_run {
            None => {
                // Start first run
                current_run = Some((origin_id, timestamp, timestamp, 1));
            }
            Some((run_origin, start_ts, _end_ts, count)) => {
                if origin_id == run_origin {
                    // Continue current run
                    current_run = Some((run_origin, start_ts, timestamp, count + 1));
                } else {
                    // Origin change - finish current run and check for overlap
                    let prev_timestamp = if i > 0 {
                        data_points[i - 1].0
                    } else {
                        timestamp
                    };

                    if timestamp - prev_timestamp <= overlap_threshold_ms {
                        // Close overlap - look ahead to see if this is a true overlap or transition
                        let mut overlap_points =
                            vec![(prev_timestamp, run_origin), (timestamp, origin_id)];

                        // Look ahead for more overlapping points
                        let mut j = i + 1;
                        while j < data_points.len() {
                            let (next_ts, next_origin) = data_points[j];
                            if next_ts - timestamp <= overlap_threshold_ms {
                                overlap_points.push((next_ts, next_origin));
                                j += 1;
                            } else {
                                break;
                            }
                        }

                        // Finish previous run
                        if count > 1 {
                            timeline.push(TimelineSegment::Run {
                                origin_id: run_origin,
                                start_timestamp: start_ts,
                                end_timestamp: prev_timestamp,
                                point_count: count,
                            });
                        }

                        // Add overlap
                        timeline.push(TimelineSegment::Overlap {
                            start_timestamp: prev_timestamp,
                            end_timestamp: overlap_points
                                .last()
                                .expect("overlap_points is non-empty")
                                .0,
                            points: overlap_points,
                        });

                        // Skip ahead past overlap
                        // TODO: Continue processing from after overlap
                        current_run = None;
                    } else {
                        // Clean transition - finish run and start new one
                        timeline.push(TimelineSegment::Run {
                            origin_id: run_origin,
                            start_timestamp: start_ts,
                            end_timestamp: prev_timestamp,
                            point_count: count,
                        });

                        current_run = Some((origin_id, timestamp, timestamp, 1));
                    }
                }
            }
        }
    }

    // Finish final run
    if let Some((origin_id, start_ts, end_ts, count)) = current_run {
        timeline.push(TimelineSegment::Run {
            origin_id,
            start_timestamp: start_ts,
            end_timestamp: end_ts,
            point_count: count,
        });
    }

    timeline
}

/// Print overlap analysis summary
#[allow(clippy::print_stdout)]
fn print_overlap_summary(analysis: &OverlapAnalysis) {
    println!("Temporal Overlap Analysis Summary");
    println!("================================");
    println!("Total data points: {}", analysis.total_points);
    println!("Overlapping points: {}", analysis.overlapping_points);

    let overlap_percentage = if analysis.total_points > 0 {
        (analysis.overlapping_points as f64 / analysis.total_points as f64) * 100.0
    } else {
        0.0
    };
    println!("Overlap percentage: {:.2}%", overlap_percentage);

    // Count different types of segments
    let mut run_count = 0;
    let mut overlap_count = 0;

    for segment in &analysis.timeline {
        match segment {
            TimelineSegment::Run { .. } => run_count += 1,
            TimelineSegment::Overlap { .. } => overlap_count += 1,
        }
    }

    println!(
        "Timeline segments: {} runs, {} overlaps",
        run_count, overlap_count
    );

    if !analysis.timeline.is_empty() {
        println!("\nTimeline Analysis:");
        for (i, segment) in analysis.timeline.iter().enumerate() {
            match segment {
                TimelineSegment::Run {
                    origin_id,
                    start_timestamp,
                    end_timestamp,
                    point_count,
                } => {
                    let start_time = format_timestamp(*start_timestamp);
                    let end_time = format_timestamp(*end_timestamp);
                    let duration_hours =
                        (*end_timestamp - *start_timestamp) as f64 / (1000.0 * 60.0 * 60.0);
                    let file_path = analysis
                        .origin_to_path
                        .get(origin_id)
                        .map(|s| s.as_str())
                        .unwrap_or("unknown");

                    println!(
                        "  {}. RUN: {} - {} to {} ({:.1} hours, {} points)",
                        i + 1,
                        file_path,
                        start_time,
                        end_time,
                        duration_hours,
                        point_count
                    );
                }
                TimelineSegment::Overlap {
                    start_timestamp,
                    end_timestamp,
                    points,
                } => {
                    let start_time = format_timestamp(*start_timestamp);
                    let end_time = format_timestamp(*end_timestamp);
                    let origins: std::collections::HashSet<i64> =
                        points.iter().map(|(_, origin)| *origin).collect();
                    let file_paths: Vec<&str> = origins
                        .iter()
                        .map(|id| {
                            analysis
                                .origin_to_path
                                .get(id)
                                .map(|s| s.as_str())
                                .unwrap_or("unknown")
                        })
                        .collect();

                    if *start_timestamp == *end_timestamp {
                        println!(
                            "  {}. OVERLAP: Single moment at {} - {} points from files {:?}",
                            i + 1,
                            start_time,
                            points.len(),
                            file_paths
                        );
                    } else {
                        println!(
                            "  {}. OVERLAP: {} to {} - {} points from files {:?}",
                            i + 1,
                            start_time,
                            end_time,
                            points.len(),
                            file_paths
                        );
                    }
                }
            }
        }
    }

    println!("\nOrigin Statistics:");
    for (origin_id, stats) in &analysis.origin_statistics {
        let file_path = analysis
            .origin_to_path
            .get(origin_id)
            .map(|s| s.as_str())
            .unwrap_or("unknown");
        if let Some((min_ts, max_ts)) = stats.time_range {
            let min_time = format_timestamp(min_ts);
            let max_time = format_timestamp(max_ts);
            let span_hours = (max_ts - min_ts) as f64 / (1000.0 * 60.0 * 60.0);

            println!(
                "  {}: {} points, {} to {} ({:.1} hours span)",
                file_path, stats.point_count, min_time, max_time, span_hours
            );
        } else {
            println!(
                "  {}: {} points, no time range",
                file_path, stats.point_count
            );
        }
    }
}

/// Print detailed overlap analysis
#[allow(clippy::print_stdout)]
fn print_overlap_details(analysis: &OverlapAnalysis) {
    print_overlap_summary(analysis);

    if !analysis.overlap_runs.is_empty() {
        println!("\nDetailed Overlap Runs:");
        for (i, run) in analysis.overlap_runs.iter().enumerate() {
            let start_time = format_timestamp(run.start_timestamp);
            let end_time = format_timestamp(run.end_timestamp);

            if run.start_timestamp == run.end_timestamp {
                println!("\nRun {}: Single point overlap", i + 1);
                println!("  Timestamp: {}", start_time);
            } else {
                println!("\nRun {}: Time range overlap", i + 1);
                println!("  Start: {}", start_time);
                println!("  End: {}", end_time);
                println!(
                    "  Duration: {:.1} hours",
                    run.duration_ms as f64 / (1000.0 * 60.0 * 60.0)
                );
            }
            println!("  Points: {}", run.point_count);
            println!("  Origins involved: {:?}", run.origins_involved);
        }
    }
}

/// Convert Unix timestamp in milliseconds to human-readable date string
fn format_timestamp(timestamp_ms: i64) -> String {
    // Convert milliseconds to seconds for DateTime
    let timestamp_secs = timestamp_ms / 1000;
    let naive_datetime = DateTime::from_timestamp(timestamp_secs, 0);

    match naive_datetime {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        None => format!("Invalid timestamp: {}", timestamp_ms),
    }
}

/// Create a metadata-only FileSeries version with temporal bounds
pub async fn set_temporal_bounds_command(
    ship_context: &ShipContext,
    target_path: String,
    min_bound: Option<String>,
    max_bound: Option<String>,
) -> Result<()> {
    if min_bound.is_none() && max_bound.is_none() {
        return Err(anyhow!("no bounds were provided"));
    }

    // Parse temporal bounds if provided
    let mut extended_attrs = HashMap::new();

    if let Some(min_str) = min_bound {
        let timestamp = parse_timestamp_millis(&min_str)?;
        debug!("Setting min temporal override: {timestamp}");
        _ = extended_attrs.insert(
            "watertown.min_temporal_override".to_string(),
            timestamp.to_string(),
        );
    }

    if let Some(max_str) = max_bound {
        let timestamp = parse_timestamp_millis(&max_str)?;
        debug!("Setting max temporal override: {timestamp}");
        _ = extended_attrs.insert(
            "watertown.max_temporal_override".to_string(),
            timestamp.to_string(),
        );
    }

    // Use the general extended attributes command
    set_extended_attributes_command(ship_context, target_path, extended_attrs).await
}

/// Set arbitrary extended attributes on a FileSeries by creating an empty version
pub async fn set_extended_attributes_command(
    ship_context: &ShipContext,
    target_path: String,
    attributes: HashMap<String, String>,
) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;
    crate::common::with_write_transaction(&mut ship, ship_context.command_args(), async |tx| {
        debug!("Setting extended attributes for target_path: {target_path}");

        // Get TinyFS working directory from the transaction
        let tinyfs_root = tx.root().await?;

        debug!("About to get async writer for target_path: {target_path}");

        // Use TinyFS async writer to either add a new version to existing file or create new FileSeries
        // This handles both cases automatically: existing file gets new version, missing file gets created
        let mut writer = tinyfs_root
            .async_writer_path_with_type(&target_path, tinyfs::EntryType::TablePhysicalSeries)
            .await
            .map_err(|e| anyhow!("Failed to get FileSeries writer: {}", e))?;

        debug!("Got FileSeries writer for target_path: {target_path}");

        // Write empty content to create a pending record
        use tokio::io::AsyncWriteExt;
        writer
            .write_all(&[])
            .await
            .map_err(|e| anyhow!("Failed to write empty content: {}", e))?;
        writer
            .flush()
            .await
            .map_err(|e| anyhow!("Failed to flush empty content: {}", e))?;

        debug!("Wrote and flushed empty content, pending record should now exist");

        // Complete the write operation to create the pending record
        writer
            .shutdown()
            .await
            .map_err(|e| anyhow!("Failed to complete empty write: {}", e))?;

        debug!("Writer shutdown complete, pending record should now exist");

        debug!("About to set extended attributes on pending version");

        // Now apply extended attributes to the pending record created by shutdown
        tinyfs_root
            .set_extended_attributes(&target_path, attributes)
            .await
            .map_err(|e| anyhow!("Failed to set extended attributes: {}", e))?;

        debug!("Applied extended attributes to pending FileSeries version");

        debug!("Completed FileSeries write with extended attributes");

        Ok(())
    })
    .await?;

    debug!("Created metadata-only FileSeries version with extended attributes");

    Ok(())
}

pub fn parse_timestamp_seconds(timestamp_str: &str) -> Result<i64> {
    parse_timestamp_millis(timestamp_str).map(|x| x / 1000)
}

/// Parse human-readable timestamp to milliseconds since Unix epoch
fn parse_timestamp_millis(timestamp_str: &str) -> Result<i64> {
    // Try multiple common timestamp formats
    let formats = [
        "%Y-%m-%d %H:%M:%S",   // "2024-01-01 00:00:00"
        "%Y-%m-%dT%H:%M:%S",   // "2024-01-01T00:00:00"
        "%Y-%m-%dT%H:%M:%SZ",  // "2024-01-01T00:00:00Z"
        "%Y-%m-%dT%H:%M:%S%z", // "2024-01-01T00:00:00+00:00"
        "%Y-%m-%d",            // "2024-01-01" (assumes 00:00:00)
    ];

    for format in &formats {
        // Try parsing as naive datetime first, then assume UTC
        if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(timestamp_str, format) {
            let utc_dt = DateTime::<Utc>::from_naive_utc_and_offset(naive_dt, Utc);
            return Ok(utc_dt.timestamp_millis());
        }

        // Try parsing as datetime with timezone
        if let Ok(dt) = DateTime::parse_from_str(timestamp_str, format) {
            return Ok(dt.timestamp_millis());
        }
    }

    // Try parsing date-only format and assume 00:00:00 UTC
    if let Ok(date) = chrono::NaiveDate::parse_from_str(timestamp_str, "%Y-%m-%d") {
        let naive_dt = date.and_hms_opt(0, 0, 0).expect("midnight is always valid");
        let utc_dt = DateTime::<Utc>::from_naive_utc_and_offset(naive_dt, Utc);
        return Ok(utc_dt.timestamp_millis());
    }

    Err(anyhow!(
        "Could not parse timestamp '{}'. Supported formats: YYYY-MM-DD HH:MM:SS, YYYY-MM-DDTHH:MM:SS[Z], YYYY-MM-DD",
        timestamp_str
    ))
}
