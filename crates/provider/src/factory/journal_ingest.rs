// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Journal Ingestion Factory
//!
//! Collects systemd journal logs by spawning `journalctl --output=json` and
//! writing entries into the pond as raw JSON Lines files, one per systemd unit.
//!
//! Features:
//! - Incremental collection via journalctl cursor (stored in pond)
//! - Auto-discovers all units from the journal
//! - Groups entries by `_SYSTEMD_UNIT`, one `FilePhysicalSeries` per unit
//! - Separate kernel ring buffer stream (`_TRANSPORT=kernel`)
//! - Entries with no unit and non-kernel transport go to `other.jsonl`

use crate::{ExecutionContext, ExecutionMode, FactoryContext, register_executable_factory};
use clap::{Parser, Subcommand};
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use tinyfs::ResultExt;
use tinyfs::{EntryType, Result as TinyFSResult};

/// Journal ingest factory subcommands
#[derive(Debug, Parser)]
struct JournalCommand {
    #[command(subcommand)]
    command: Option<JournalSubcommand>,
}

#[derive(Debug, Subcommand)]
enum JournalSubcommand {
    /// Collect new journal entries (default)
    Push,

    /// Pull mode (no-op for journal-ingest)
    Pull,

    /// Show cursor position and entry counts per unit
    Status,
}

/// Parse command-line arguments
fn parse_command(ctx: ExecutionContext) -> Result<JournalCommand, tinyfs::Error> {
    let args_with_prog_name: Vec<String> = std::iter::once("factory".to_string())
        .chain(ctx.args().iter().cloned())
        .collect();

    JournalCommand::try_parse_from(args_with_prog_name).map_other_context("Command parse error")
}

/// Configuration for the journal ingestion factory
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalIngestConfig {
    /// Destination path within the pond (relative to pond root)
    /// Example: "logs/watershop"
    pub pond_path: String,

    /// Command to invoke journalctl (default: "journalctl")
    /// Can be customized for non-standard installs
    #[serde(default = "default_journalctl_command")]
    pub journalctl_command: String,

    /// Whether to collect kernel ring buffer as a separate stream (default: true)
    #[serde(default = "default_collect_kernel")]
    pub collect_kernel: bool,

    /// JSON field name containing the timestamp (default: "__REALTIME_TIMESTAMP")
    /// The value is interpreted as microseconds since Unix epoch.
    #[serde(default = "default_timestamp_field")]
    pub timestamp_field: String,

    /// Extra command-line arguments appended to every `journalctl`
    /// invocation.  Useful for e.g. `["--merge"]` to pull both the
    /// system journal and the per-user journal of the invoking user
    /// (which is what surfaces user-scope units like
    /// `pond@water-prod.service` to the ingester).  Default: empty.
    #[serde(default)]
    pub extra_args: Vec<String>,
}

fn default_journalctl_command() -> String {
    "journalctl".to_string()
}

fn default_collect_kernel() -> bool {
    true
}

fn default_timestamp_field() -> String {
    "__REALTIME_TIMESTAMP".to_string()
}

impl JournalIngestConfig {
    /// Validate the configuration
    pub fn validate(&self) -> TinyFSResult<()> {
        if self.pond_path.is_empty() {
            return Err(tinyfs::Error::Other(
                "pond_path cannot be empty".to_string(),
            ));
        }
        if self.journalctl_command.is_empty() {
            return Err(tinyfs::Error::Other(
                "journalctl_command cannot be empty".to_string(),
            ));
        }
        Ok(())
    }
}

/// Name of the cursor file stored in the pond directory
const CURSOR_FILENAME: &str = ".journal-cursor";

/// Filename for kernel messages
const KERNEL_FILENAME: &str = "kernel.jsonl";

/// Filename for entries with no unit and non-kernel transport
const OTHER_FILENAME: &str = "other.jsonl";

/// Read the stored cursor from the pond, if it exists
async fn read_cursor(
    context: &FactoryContext,
    pond_path: &str,
) -> Result<Option<String>, tinyfs::Error> {
    let fs = context.context.filesystem();
    let root = fs.root().await?;

    let cursor_path = format!("{}/{}", pond_path, CURSOR_FILENAME);

    match root.read_file_path_to_vec(&cursor_path).await {
        Ok(data) => {
            let cursor = String::from_utf8(data).map_other_context("Invalid cursor data")?;
            let trimmed = cursor.trim().to_string();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                debug!(
                    "Read cursor: {}...{}",
                    &trimmed[..16.min(trimmed.len())],
                    &trimmed[trimmed.len().saturating_sub(8)..]
                );
                Ok(Some(trimmed))
            }
        }
        Err(tinyfs::Error::NotFound(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Write the cursor to the pond
async fn write_cursor(
    context: &FactoryContext,
    pond_path: &str,
    cursor: &str,
) -> Result<(), tinyfs::Error> {
    let fs = context.context.filesystem();
    let root = fs.root().await?;

    let cursor_path = format!("{}/{}", pond_path, CURSOR_FILENAME);

    // Write cursor as raw bytes (overwrites existing content)
    root.write_file_path_from_slice(&cursor_path, cursor.as_bytes())
        .await?;

    debug!(
        "Wrote cursor: {}...{}",
        &cursor[..16.min(cursor.len())],
        &cursor[cursor.len().saturating_sub(8)..]
    );
    Ok(())
}

/// Spawn journalctl and collect output as JSON Lines
async fn collect_journal_entries(
    config: &JournalIngestConfig,
    cursor: Option<&str>,
) -> Result<Vec<String>, tinyfs::Error> {
    let mut cmd_args = vec!["--output=json".to_string(), "--no-pager".to_string()];

    if let Some(cursor_val) = cursor {
        cmd_args.push(format!("--after-cursor={}", cursor_val));
    }

    cmd_args.extend(config.extra_args.iter().cloned());

    info!(
        "Spawning: {} {}",
        config.journalctl_command,
        cmd_args.join(" ")
    );

    let output = std::process::Command::new(&config.journalctl_command)
        .args(&cmd_args)
        .output()
        .map_other_context(format!("Failed to spawn {}", config.journalctl_command))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(tinyfs::Error::Other(format!(
            "journalctl failed (exit {}): {}",
            output.status, stderr
        )));
    }

    let stdout =
        String::from_utf8(output.stdout).map_other_context("Invalid UTF-8 from journalctl")?;

    let lines: Vec<String> = stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.to_string())
        .collect();

    info!("Collected {} journal entries", lines.len());
    Ok(lines)
}

/// Group journal entries by unit and extract the last cursor
///
/// Returns a map of (filename -> Vec<json_line>) and the last cursor seen.
fn group_entries(
    lines: &[String],
    collect_kernel: bool,
) -> Result<(BTreeMap<String, Vec<&str>>, Option<String>), tinyfs::Error> {
    let mut groups: BTreeMap<String, Vec<&str>> = BTreeMap::new();
    let mut last_cursor: Option<String> = None;
    let mut skipped = 0u64;

    for line in lines {
        let parsed: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        // Extract cursor for progress tracking
        if let Some(cursor) = parsed.get("__CURSOR").and_then(|v| v.as_str()) {
            last_cursor = Some(cursor.to_string());
        }

        // Determine destination filename
        let filename = determine_filename(&parsed, collect_kernel);

        groups.entry(filename).or_default().push(line.as_str());
    }

    if skipped > 0 {
        warn!("journal-ingest: skipped {} unparseable line(s)", skipped);
    }

    Ok((groups, last_cursor))
}

/// Per-file temporal bounds tracked during grouping.
struct FileBounds {
    min_timestamp: i64,
    max_timestamp: i64,
}

/// Extract per-file min/max timestamps from grouped entries.
///
/// Parses the configured timestamp field from each JSON line and tracks
/// bounds per output file. Returns None for files where no valid
/// timestamp was found.
fn compute_file_bounds(
    groups: &BTreeMap<String, Vec<&str>>,
    timestamp_field: &str,
) -> BTreeMap<String, FileBounds> {
    let mut bounds: BTreeMap<String, FileBounds> = BTreeMap::new();

    for (filename, lines) in groups {
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        let mut found = false;

        for line in lines {
            if let Ok(parsed) = serde_json::from_str::<Value>(line)
                && let Some(ts) = extract_timestamp(&parsed, timestamp_field)
            {
                if ts < min_ts {
                    min_ts = ts;
                }
                if ts > max_ts {
                    max_ts = ts;
                }
                found = true;
            }
        }

        if found {
            let _ = bounds.insert(
                filename.clone(),
                FileBounds {
                    min_timestamp: min_ts,
                    max_timestamp: max_ts,
                },
            );
        }
    }

    bounds
}

/// Extract a microsecond timestamp from a JSON value's field.
///
/// journalctl's __REALTIME_TIMESTAMP is a string containing
/// microseconds since epoch. We also handle numeric values.
fn extract_timestamp(entry: &Value, field: &str) -> Option<i64> {
    match entry.get(field)? {
        Value::String(s) => s.parse::<i64>().ok(),
        Value::Number(n) => n.as_i64(),
        _ => None,
    }
}

/// Determine the output filename for a journal entry
fn determine_filename(entry: &Value, collect_kernel: bool) -> String {
    // Check for kernel transport first (when collect_kernel is enabled)
    if collect_kernel
        && let Some(transport) = entry.get("_TRANSPORT").and_then(|v| v.as_str())
        && transport == "kernel"
    {
        return KERNEL_FILENAME.to_string();
    }

    // Group by _SYSTEMD_UNIT if present.  The exception is the user
    // manager service (`user@<uid>.service`) when the entry also
    // carries a `_SYSTEMD_USER_UNIT`: that combination identifies an
    // entry belonging to a user-scope unit (e.g. `pond@water-prod`)
    // when journalctl is invoked with `--merge`.  Without this
    // override every user-scope entry would land in a single
    // `user@<uid>.service.jsonl` blob, defeating per-unit analysis.
    if let Some(unit) = entry.get("_SYSTEMD_UNIT").and_then(|v| v.as_str()) {
        let is_user_manager = unit.starts_with("user@") && unit.ends_with(".service");
        if is_user_manager
            && let Some(user_unit) = entry.get("_SYSTEMD_USER_UNIT").and_then(|v| v.as_str())
        {
            let safe_name = user_unit.replace('/', "-");
            return format!("user-{}.jsonl", safe_name);
        }
        // Sanitize unit name for use as filename
        // systemd units are already mostly filename-safe, just replace / with -
        let safe_name = unit.replace('/', "-");
        return format!("{}.jsonl", safe_name);
    }

    // Check _SYSTEMD_USER_UNIT for user-level services
    if let Some(unit) = entry.get("_SYSTEMD_USER_UNIT").and_then(|v| v.as_str()) {
        let safe_name = unit.replace('/', "-");
        return format!("user-{}.jsonl", safe_name);
    }

    // Fallback: use SYSLOG_IDENTIFIER if available
    if let Some(ident) = entry.get("SYSLOG_IDENTIFIER").and_then(|v| v.as_str()) {
        let safe_name = ident.replace('/', "-");
        return format!("{}.jsonl", safe_name);
    }

    // Last resort
    OTHER_FILENAME.to_string()
}

/// Write grouped entries to the pond as FilePhysicalSeries
async fn write_entries(
    context: &FactoryContext,
    config: &JournalIngestConfig,
    groups: &BTreeMap<String, Vec<&str>>,
    file_bounds: &BTreeMap<String, FileBounds>,
) -> Result<(), tinyfs::Error> {
    let fs = context.context.filesystem();
    let root = fs.root().await?;

    // Ensure the pond directory exists
    let _ = root.create_dir_all(&config.pond_path).await?;

    for (filename, lines) in groups {
        let pond_dest = format!("{}/{}", config.pond_path, filename);

        // Build the content as newline-delimited JSON
        let mut content = String::new();
        for line in lines {
            content.push_str(line);
            content.push('\n');
        }

        info!(
            "Writing {} entries ({} bytes) to {}",
            lines.len(),
            content.len(),
            pond_dest
        );

        // Write as FilePhysicalSeries (append creates new version)
        use tokio::io::AsyncWriteExt;
        let mut writer = root
            .async_writer_path_with_type(&pond_dest, EntryType::FilePhysicalSeries)
            .await?;

        writer.write_all(content.as_bytes()).await.map_other()?;

        // Record the event-time bounds computed from the records themselves.
        // journald output is read whole rather than tailed by byte offset, and
        // every entry carries __REALTIME_TIMESTAMP, so a group without bounds
        // means the configured field does not match the records -- declare the
        // requirement and let the write fail rather than store an unbounded
        // version that forces downstream rollups to recompute all history.
        writer.require_temporal_metadata(config.timestamp_field.clone());
        if let Some(bounds) = file_bounds.get(filename) {
            writer.set_temporal_metadata(
                bounds.min_timestamp,
                bounds.max_timestamp,
                config.timestamp_field.clone(),
            );
        }

        writer.shutdown().await.map_other()?;
    }

    Ok(())
}

/// Execute status command -- show cursor and file list
#[allow(clippy::print_stdout)]
async fn execute_status(
    context: &FactoryContext,
    config: &JournalIngestConfig,
) -> Result<(), tinyfs::Error> {
    let cursor = read_cursor(context, &config.pond_path).await?;

    match cursor {
        Some(c) => println!("Cursor: {}", c),
        None => println!("Cursor: (none -- will collect from beginning)"),
    }

    // List files in the pond directory
    let fs = context.context.filesystem();
    let root = fs.root().await?;

    match root.open_dir_path(&config.pond_path).await {
        Ok(pond_dir) => {
            use futures::StreamExt;
            let mut entries = pond_dir.entries().await?;
            let mut files: Vec<String> = Vec::new();

            while let Some(entry_result) = entries.next().await {
                let entry = entry_result?;
                if !entry.entry_type.is_file() {
                    continue;
                }
                if entry.name == CURSOR_FILENAME {
                    continue;
                }
                files.push(entry.name);
            }

            files.sort();

            if files.is_empty() {
                println!("No log files collected yet.");
            } else {
                println!("Log files ({}):", files.len());
                for name in &files {
                    println!("  {}", name);
                }
            }
        }
        Err(tinyfs::Error::NotFound(_)) => {
            println!("No log files collected yet (directory does not exist).");
        }
        Err(e) => return Err(e),
    }

    Ok(())
}

/// Initialize factory (called once per node creation)
async fn initialize(_config: Value, _context: FactoryContext) -> Result<(), tinyfs::Error> {
    Ok(())
}

/// Execute the journal ingestion process
pub async fn execute(
    config: Value,
    context: FactoryContext,
    ctx: ExecutionContext,
) -> Result<(), tinyfs::Error> {
    let config: JournalIngestConfig =
        serde_json::from_value(config.clone()).map_other_context("Invalid config")?;

    let cmd = parse_command(ctx)?;

    match cmd.command {
        Some(JournalSubcommand::Pull) => {
            info!("journal-ingest: 'pull' mode is a no-op");
            return Ok(());
        }
        Some(JournalSubcommand::Status) => {
            return execute_status(&context, &config).await;
        }
        Some(JournalSubcommand::Push) | None => {
            // Default: collect entries
        }
    }

    info!(
        "Starting journal ingestion (mode: {:?})",
        ExecutionMode::PondReadWriter
    );

    // Step 1: Read cursor from pond
    let cursor = read_cursor(&context, &config.pond_path).await?;
    if let Some(ref c) = cursor {
        info!(
            "Resuming from cursor: {}...{}",
            &c[..16.min(c.len())],
            &c[c.len().saturating_sub(8)..]
        );
    } else {
        info!("No cursor found -- collecting all available journal entries");
    }

    // Step 2: Spawn journalctl and collect entries
    let lines = collect_journal_entries(&config, cursor.as_deref()).await?;

    if lines.is_empty() {
        info!("No new journal entries to collect");
        return Ok(());
    }

    // Step 3: Group entries by unit
    let (groups, last_cursor) = group_entries(&lines, config.collect_kernel)?;

    info!(
        "Grouped {} entries into {} files",
        lines.len(),
        groups.len()
    );

    for (filename, entries) in &groups {
        debug!("  {} -> {} entries", filename, entries.len());
    }

    // Step 4: Compute per-file temporal bounds
    let file_bounds = compute_file_bounds(&groups, &config.timestamp_field);

    // Step 5: Write grouped entries to pond
    write_entries(&context, &config, &groups, &file_bounds).await?;

    // Step 6: Save cursor
    if let Some(cursor_val) = &last_cursor {
        write_cursor(&context, &config.pond_path, cursor_val).await?;
        info!("Saved cursor for next run");
    }

    info!(
        "Journal ingestion complete: {} entries written to {} files",
        lines.len(),
        groups.len()
    );

    Ok(())
}

/// Validate configuration
fn validate_config(config: &[u8]) -> TinyFSResult<Value> {
    let config: JournalIngestConfig =
        serde_yaml::from_slice(config).map_other_context("Invalid config YAML")?;

    config.validate()?;

    serde_json::to_value(&config).map_other_context("Failed to serialize config")
}

// Register the factory
register_executable_factory!(
    name: "journal-ingest",
    description: "Collect systemd journal logs into per-unit JSON Lines files",
    validate: validate_config,
    initialize: initialize,
    execute: execute
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_config() {
        let config = JournalIngestConfig {
            pond_path: "logs/workshep".to_string(),
            journalctl_command: "journalctl".to_string(),
            collect_kernel: true,
            timestamp_field: "__REALTIME_TIMESTAMP".to_string(),
            extra_args: vec![],
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_config_empty_pond_path() {
        let config = JournalIngestConfig {
            pond_path: "".to_string(),
            journalctl_command: "journalctl".to_string(),
            collect_kernel: true,
            timestamp_field: "__REALTIME_TIMESTAMP".to_string(),
            extra_args: vec![],
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_config_yaml() {
        let yaml =
            b"pond_path: logs/watershop\njournalctl_command: journalctl\ncollect_kernel: true\n";
        let result = validate_config(yaml);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_config_yaml_defaults() {
        let yaml = b"pond_path: logs/workshep\n";
        let result = validate_config(yaml);
        assert!(result.is_ok());
        let val = result.unwrap();
        assert_eq!(val["journalctl_command"], "journalctl");
        assert_eq!(val["collect_kernel"], true);
        assert_eq!(val["timestamp_field"], "__REALTIME_TIMESTAMP");
    }

    #[test]
    fn test_validate_config_yaml_rejects_unknown() {
        let yaml = b"pond_path: logs/test\nunknown_field: oops\n";
        let result = validate_config(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_determine_filename_kernel() {
        let entry: Value =
            serde_json::from_str(r#"{"_TRANSPORT":"kernel","MESSAGE":"test"}"#).unwrap();
        assert_eq!(determine_filename(&entry, true), "kernel.jsonl");
        // When collect_kernel is false, kernel entries fall through
        assert_eq!(determine_filename(&entry, false), "other.jsonl");
    }

    #[test]
    fn test_determine_filename_systemd_unit() {
        let entry: Value = serde_json::from_str(
            r#"{"_SYSTEMD_UNIT":"ssh.service","MESSAGE":"test","_TRANSPORT":"syslog"}"#,
        )
        .unwrap();
        assert_eq!(determine_filename(&entry, true), "ssh.service.jsonl");
    }

    #[test]
    fn test_determine_filename_user_unit() {
        let entry: Value = serde_json::from_str(
            r#"{"_SYSTEMD_USER_UNIT":"pulseaudio.service","MESSAGE":"test","_TRANSPORT":"journal"}"#,
        )
        .unwrap();
        assert_eq!(
            determine_filename(&entry, true),
            "user-pulseaudio.service.jsonl"
        );
    }

    #[test]
    fn test_determine_filename_user_manager_with_user_unit() {
        // journalctl --merge tags user-scope entries with both
        // _SYSTEMD_UNIT=user@<uid>.service and the actual user unit
        // in _SYSTEMD_USER_UNIT.  Without the override we'd dump
        // every user-scope entry into a single user@<uid>.jsonl.
        let entry: Value = serde_json::from_str(
            r#"{"_SYSTEMD_UNIT":"user@1001.service","_SYSTEMD_USER_UNIT":"pond@water-prod.service","MESSAGE":"test","_TRANSPORT":"stdout"}"#,
        )
        .unwrap();
        assert_eq!(
            determine_filename(&entry, true),
            "user-pond@water-prod.service.jsonl"
        );
    }

    #[test]
    fn test_determine_filename_user_manager_without_user_unit() {
        // The user manager itself logs entries that lack
        // _SYSTEMD_USER_UNIT (e.g. session lifecycle); those should
        // still file under user@<uid>.service.jsonl.
        let entry: Value = serde_json::from_str(
            r#"{"_SYSTEMD_UNIT":"user@1001.service","MESSAGE":"Started session","_TRANSPORT":"journal"}"#,
        )
        .unwrap();
        assert_eq!(determine_filename(&entry, true), "user@1001.service.jsonl");
    }

    #[test]
    fn test_determine_filename_syslog_identifier() {
        let entry: Value = serde_json::from_str(
            r#"{"SYSLOG_IDENTIFIER":"dbus-daemon","MESSAGE":"test","_TRANSPORT":"syslog"}"#,
        )
        .unwrap();
        assert_eq!(determine_filename(&entry, true), "dbus-daemon.jsonl");
    }

    #[test]
    fn test_determine_filename_fallback() {
        let entry: Value =
            serde_json::from_str(r#"{"MESSAGE":"test","_TRANSPORT":"stdout"}"#).unwrap();
        assert_eq!(determine_filename(&entry, true), "other.jsonl");
    }

    #[test]
    fn test_group_entries() {
        let lines = vec![
            r#"{"_SYSTEMD_UNIT":"ssh.service","MESSAGE":"login","__CURSOR":"c1"}"#.to_string(),
            r#"{"_SYSTEMD_UNIT":"ssh.service","MESSAGE":"logout","__CURSOR":"c2"}"#.to_string(),
            r#"{"_TRANSPORT":"kernel","MESSAGE":"usb","__CURSOR":"c3"}"#.to_string(),
            r#"{"_SYSTEMD_UNIT":"cron.service","MESSAGE":"job","__CURSOR":"c4"}"#.to_string(),
        ];

        let (groups, last_cursor) = group_entries(&lines, true).unwrap();

        assert_eq!(groups.len(), 3);
        assert_eq!(groups["ssh.service.jsonl"].len(), 2);
        assert_eq!(groups["kernel.jsonl"].len(), 1);
        assert_eq!(groups["cron.service.jsonl"].len(), 1);
        assert_eq!(last_cursor.unwrap(), "c4");
    }

    #[test]
    fn test_group_entries_skips_invalid() {
        let lines = vec![
            r#"{"_SYSTEMD_UNIT":"ssh.service","MESSAGE":"ok","__CURSOR":"c1"}"#.to_string(),
            "not valid json".to_string(),
            r#"{"_SYSTEMD_UNIT":"ssh.service","MESSAGE":"ok2","__CURSOR":"c2"}"#.to_string(),
        ];

        let (groups, last_cursor) = group_entries(&lines, true).unwrap();
        assert_eq!(groups["ssh.service.jsonl"].len(), 2);
        assert_eq!(last_cursor.unwrap(), "c2");
    }

    #[test]
    fn test_group_entries_empty() {
        let lines: Vec<String> = vec![];
        let (groups, last_cursor) = group_entries(&lines, true).unwrap();
        assert!(groups.is_empty());
        assert!(last_cursor.is_none());
    }

    #[test]
    fn test_extract_timestamp_string() {
        let entry: Value =
            serde_json::from_str(r#"{"__REALTIME_TIMESTAMP":"1772431543477823","MESSAGE":"test"}"#)
                .unwrap();
        assert_eq!(
            extract_timestamp(&entry, "__REALTIME_TIMESTAMP"),
            Some(1772431543477823)
        );
    }

    #[test]
    fn test_extract_timestamp_number() {
        let entry: Value =
            serde_json::from_str(r#"{"ts":1772431543477823,"MESSAGE":"test"}"#).unwrap();
        assert_eq!(extract_timestamp(&entry, "ts"), Some(1772431543477823));
    }

    #[test]
    fn test_extract_timestamp_missing() {
        let entry: Value = serde_json::from_str(r#"{"MESSAGE":"test"}"#).unwrap();
        assert_eq!(extract_timestamp(&entry, "__REALTIME_TIMESTAMP"), None);
    }

    #[test]
    fn test_compute_file_bounds() {
        let lines = vec![
            r#"{"_SYSTEMD_UNIT":"ssh.service","MESSAGE":"login","__CURSOR":"c1","__REALTIME_TIMESTAMP":"100"}"#.to_string(),
            r#"{"_SYSTEMD_UNIT":"ssh.service","MESSAGE":"logout","__CURSOR":"c2","__REALTIME_TIMESTAMP":"300"}"#.to_string(),
            r#"{"_TRANSPORT":"kernel","MESSAGE":"usb","__CURSOR":"c3","__REALTIME_TIMESTAMP":"200"}"#.to_string(),
        ];

        let (groups, _) = group_entries(&lines, true).unwrap();
        let bounds = compute_file_bounds(&groups, "__REALTIME_TIMESTAMP");

        let ssh_bounds = bounds.get("ssh.service.jsonl").unwrap();
        assert_eq!(ssh_bounds.min_timestamp, 100);
        assert_eq!(ssh_bounds.max_timestamp, 300);

        let kernel_bounds = bounds.get("kernel.jsonl").unwrap();
        assert_eq!(kernel_bounds.min_timestamp, 200);
        assert_eq!(kernel_bounds.max_timestamp, 200);
    }

    #[test]
    fn test_compute_file_bounds_no_timestamp() {
        let lines = vec![r#"{"_SYSTEMD_UNIT":"ssh.service","MESSAGE":"test"}"#.to_string()];

        let (groups, _) = group_entries(&lines, true).unwrap();
        let bounds = compute_file_bounds(&groups, "__REALTIME_TIMESTAMP");

        // No bounds when timestamp field is missing
        assert!(!bounds.contains_key("ssh.service.jsonl"));
    }
}
