// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::config::ReportSummary;
use chrono::{DateTime, Utc};
use datafusion::arrow::array::{Array, Float64Array, TimestampMicrosecondArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::sql::TableReference;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

static REPORT_QUERY_ID: AtomicU64 = AtomicU64::new(0);

/// One timestamped numeric report sample.
#[derive(Clone, Debug)]
struct Sample {
    pub timestamp: DateTime<Utc>,
    pub value: f64,
}

/// Numeric aggregation used by rendering and tests.
#[derive(Clone, Debug, PartialEq)]
struct Summary {
    pub sample_count: usize,
    pub finite_count: usize,
    pub latest: Option<f64>,
    pub minimum: Option<f64>,
    pub maximum: Option<f64>,
    pub sum: Option<f64>,
}

/// Calculated content for a configured report section.
#[derive(Clone, Debug)]
struct ReportSection {
    pub title: String,
    pub unit: String,
    pub summary_kind: ReportSummary,
    pub link: String,
    pub chart: bool,
    pub samples: Vec<Sample>,
}

/// Whether the pond-size row is suppressed, unavailable, or measured.
#[derive(Clone, Copy, Debug)]
enum PondSize {
    NotIncluded,
    Unavailable,
    Bytes(u64),
}

/// Fully collected report prior to HTML/text rendering.
#[derive(Clone, Debug)]
struct Report {
    pub site_name: String,
    pub site_url: String,
    pub title: String,
    pub message: Option<String>,
    pub period_start: DateTime<Utc>,
    pub period_end: DateTime<Utc>,
    pub pond_size: PondSize,
    pub sections: Vec<ReportSection>,
}

/// A static chart produced with a rendered report.
#[derive(Clone, Debug)]
pub struct ReportChart {
    pub filename: String,
    pub png: Vec<u8>,
}

/// Transport-independent report artifacts.
#[derive(Clone, Debug)]
pub struct RenderedReport {
    pub html: String,
    pub plain_text: String,
    pub charts: Vec<ReportChart>,
}

/// Validate report references and values without querying a pond.
pub(crate) fn validate_reports(config: &crate::config::SiteConfig) -> Result<(), String> {
    for (name, report) in &config.reports {
        if name.trim().is_empty() {
            return Err("report names cannot be empty".to_string());
        }
        if report.title.trim().is_empty() {
            return Err(format!("report '{name}' has an empty title"));
        }
        let window = humantime::parse_duration(&report.window)
            .map_err(|error| format!("report '{name}' has an invalid window: {error}"))?;
        if window.is_zero() {
            return Err(format!("report '{name}' window must be greater than zero"));
        }
        if report.sections.is_empty() {
            return Err(format!("report '{name}' must contain at least one section"));
        }
        for (index, section) in report.sections.iter().enumerate() {
            let context = format!("report '{name}' section {index}");
            let stage = config
                .exports
                .iter()
                .find(|stage| stage.name == section.export)
                .ok_or_else(|| {
                    format!("{context} references unknown export '{}'", section.export)
                })?;
            if stage.pattern.contains("://") {
                return Err(format!(
                    "{context} export '{}' uses a format-provider pattern, which reports do not yet support",
                    section.export
                ));
            }
            if section
                .title
                .as_ref()
                .is_some_and(|title| title.trim().is_empty())
                || section.value.trim().is_empty()
                || section.href.trim().is_empty()
            {
                return Err(format!(
                    "{context} value, href, and any title override must not be empty"
                ));
            }
        }
    }
    Ok(())
}

/// Collect and render a named report from pond data.
pub async fn render_named_report(
    config: &crate::config::SiteConfig,
    root: &tinyfs::WD,
    provider_context: &tinyfs::ProviderContext,
    name: &str,
    period_end: DateTime<Utc>,
) -> Result<RenderedReport, tinyfs::Error> {
    validate_reports(config).map_err(tinyfs::Error::Other)?;
    let report_config = config
        .reports
        .get(name)
        .ok_or_else(|| tinyfs::Error::Other(format!("unknown report '{name}'")))?;
    let site_url = config.site.site_url.clone().ok_or_else(|| {
        tinyfs::Error::Other(format!(
            "report '{name}' requires site.site_url for canonical links"
        ))
    })?;
    let window = humantime::parse_duration(&report_config.window)
        .map_err(|error| tinyfs::Error::Other(format!("invalid report window: {error}")))?;
    let window = chrono::Duration::from_std(window)
        .map_err(|error| tinyfs::Error::Other(format!("report window is too large: {error}")))?;
    let period_start = period_end.checked_sub_signed(window).ok_or_else(|| {
        tinyfs::Error::Other(format!(
            "report '{name}' window exceeds the timestamp range"
        ))
    })?;
    let query_id = REPORT_QUERY_ID.fetch_add(1, Ordering::Relaxed);

    let mut sections = Vec::with_capacity(report_config.sections.len());
    for (index, section_config) in report_config.sections.iter().enumerate() {
        let stage = config
            .exports
            .iter()
            .find(|stage| stage.name == section_config.export)
            .expect("validated report export");
        let samples = collect_samples(
            root,
            provider_context,
            stage,
            &section_config.captures,
            &section_config.value,
            period_start,
            period_end,
            query_id,
            index,
        )
        .await?;
        sections.push(ReportSection {
            title: section_title(config, section_config),
            unit: section_config.unit.clone(),
            summary_kind: section_config.summary,
            link: section_config.href.clone(),
            chart: section_config.chart,
            samples,
        });
    }

    let pond_size = if report_config.include_pond_size {
        provider_context
            .pond_path()
            .and_then(directory_allocated_size)
            .map_or(PondSize::Unavailable, PondSize::Bytes)
    } else {
        PondSize::NotIncluded
    };
    render_report(&Report {
        site_name: config.site.title.clone(),
        site_url,
        title: report_config.title.clone(),
        message: report_config.message.clone(),
        period_start,
        period_end,
        pond_size,
        sections,
    })
    .map_err(tinyfs::Error::Other)
}

/// Write a rendered report as a browser-viewable offline preview.
pub fn write_preview(report: &RenderedReport, output_dir: &Path) -> Result<(), tinyfs::Error> {
    std::fs::create_dir_all(output_dir).map_err(|error| {
        tinyfs::Error::Other(format!(
            "failed to create report directory '{}': {error}",
            output_dir.display()
        ))
    })?;
    write_artifact(output_dir.join("report.html"), report.html.as_bytes())?;
    write_artifact(output_dir.join("report.txt"), report.plain_text.as_bytes())?;
    for chart in &report.charts {
        write_artifact(output_dir.join(&chart.filename), &chart.png)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn collect_samples(
    root: &tinyfs::WD,
    provider_context: &tinyfs::ProviderContext,
    stage: &crate::config::ExportStage,
    captures: &[String],
    value_column: &str,
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
    query_id: u64,
    section_index: usize,
) -> Result<Vec<Sample>, tinyfs::Error> {
    let matches = root.collect_matches(&stage.pattern).await?;
    let selected: Vec<_> = matches
        .into_iter()
        .filter(|(_, found_captures)| found_captures == captures)
        .collect();
    let [(node_path, _)] = selected.as_slice() else {
        return Err(tinyfs::Error::Other(format!(
            "report export '{}' with captures {:?} matched {} files; expected exactly one",
            stage.name,
            captures,
            selected.len()
        )));
    };
    let path = node_path.path.to_string_lossy();
    let filesystem = provider_context.filesystem();
    let provider =
        provider::Provider::with_context(Arc::new(filesystem), Arc::new(provider_context.clone()))
            .with_root(root.clone());
    let url = format!("series://{path}");
    let table_provider = provider
        .create_table_provider_bounded(
            &url,
            &provider_context.datafusion_session,
            tinyfs::SeriesReadBounds::from_event_time_lo(period_start.timestamp_micros()),
        )
        .await
        .map_err(|error| {
            tinyfs::Error::Other(format!("failed to query report source '{path}': {error}"))
        })?;
    let table_name = format!("sitegen_report_{query_id}_{section_index}");
    let table_ref = TableReference::bare(table_name.as_str());
    provider_context
        .datafusion_session
        .register_table(table_ref.clone(), table_provider)
        .map_err(|error| {
            tinyfs::Error::Other(format!(
                "failed to register report source '{path}': {error}"
            ))
        })?;
    let result = query_samples(
        &provider_context.datafusion_session,
        &table_name,
        &stage.timestamp_column,
        value_column,
        period_start,
        period_end,
    )
    .await;
    let _ = provider_context
        .datafusion_session
        .deregister_table(table_ref);
    result
}

async fn query_samples(
    context: &datafusion::prelude::SessionContext,
    table_name: &str,
    timestamp_column: &str,
    value_column: &str,
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
) -> Result<Vec<Sample>, tinyfs::Error> {
    let timestamp = quote_identifier(timestamp_column);
    let value = quote_identifier(value_column);
    let table = quote_identifier(table_name);
    let start_micros = period_start.timestamp_micros();
    let end_micros = period_end.timestamp_micros();
    let sql = format!(
        "SELECT {timestamp}, {value} FROM {table} \
         WHERE {timestamp} >= to_timestamp_micros({start_micros}) \
         AND {timestamp} < to_timestamp_micros({end_micros}) \
         ORDER BY {timestamp}"
    );
    let frame = context
        .sql(&sql)
        .await
        .map_err(|error| tinyfs::Error::Other(format!("report query failed: {error}")))?;
    let batches = frame
        .collect()
        .await
        .map_err(|error| tinyfs::Error::Other(format!("report query failed: {error}")))?;
    decode_samples(&batches)
}

fn decode_samples(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> Result<Vec<Sample>, tinyfs::Error> {
    let mut samples = Vec::new();
    for batch in batches {
        let timestamps = cast(
            batch.column(0),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .map_err(|error| tinyfs::Error::Other(format!("invalid report timestamps: {error}")))?;
        let timestamps = timestamps
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| tinyfs::Error::Other("invalid report timestamp type".to_string()))?;
        let values = cast(batch.column(1), &DataType::Float64)
            .map_err(|error| tinyfs::Error::Other(format!("invalid report values: {error}")))?;
        let values = values
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| tinyfs::Error::Other("invalid report value type".to_string()))?;
        for index in 0..batch.num_rows() {
            if timestamps.is_null(index) || values.is_null(index) {
                continue;
            }
            let timestamp =
                DateTime::from_timestamp_micros(timestamps.value(index)).ok_or_else(|| {
                    tinyfs::Error::Other("report timestamp is out of range".to_string())
                })?;
            samples.push(Sample {
                timestamp,
                value: values.value(index),
            });
        }
    }
    Ok(samples)
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn section_title(
    config: &crate::config::SiteConfig,
    section: &crate::config::ReportSectionConfig,
) -> String {
    if let Some(title) = &section.title {
        return title.clone();
    }
    let item = section
        .captures
        .first()
        .map_or(section.export.as_str(), String::as_str);
    config.labels.get(item).cloned().unwrap_or_else(|| {
        item.split(['-', '_'])
            .filter(|word| !word.is_empty())
            .map(|word| {
                let mut characters = word.chars();
                characters.next().map_or_else(String::new, |first| {
                    first.to_uppercase().chain(characters).collect()
                })
            })
            .collect::<Vec<_>>()
            .join(" ")
    })
}

fn write_artifact(path: std::path::PathBuf, data: &[u8]) -> Result<(), tinyfs::Error> {
    std::fs::write(&path, data).map_err(|error| {
        tinyfs::Error::Other(format!(
            "failed to write report artifact '{}': {error}",
            path.display()
        ))
    })
}

fn directory_allocated_size(path: &Path) -> Option<u64> {
    let mut total = 0_u64;
    let entries = std::fs::read_dir(path).ok()?;
    for entry in entries {
        let entry = entry.ok()?;
        if entry.file_type().ok()?.is_symlink() {
            continue;
        }
        let metadata = entry.metadata().ok()?;
        if metadata.is_dir() {
            total = total.checked_add(directory_allocated_size(&entry.path())?)?;
        } else if metadata.is_file() {
            total = total.checked_add(allocated_size(&metadata))?;
        }
    }
    Some(total)
}

#[cfg(unix)]
fn allocated_size(metadata: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.blocks().saturating_mul(512)
}

#[cfg(not(unix))]
fn allocated_size(metadata: &std::fs::Metadata) -> u64 {
    metadata.len()
}

/// Compute a summary, excluding non-finite values from numeric aggregations.
#[must_use]
fn calculate_summary(samples: &[Sample], summary_kind: ReportSummary) -> Summary {
    let finite: Vec<&Sample> = samples
        .iter()
        .filter(|sample| sample.value.is_finite())
        .collect();
    let latest = finite.last().map(|sample| sample.value);
    let minimum = finite.iter().map(|sample| sample.value).reduce(f64::min);
    let maximum = finite.iter().map(|sample| sample.value).reduce(f64::max);
    let sum = match summary_kind {
        ReportSummary::Range => None,
        ReportSummary::Sum => Some(finite.iter().map(|sample| sample.value).sum()),
    };
    Summary {
        sample_count: samples.len(),
        finite_count: finite.len(),
        latest,
        minimum,
        maximum,
        sum,
    }
}

/// Render email-safe HTML, plain text, and deterministic PNG line charts.
fn render_report(report: &Report) -> Result<RenderedReport, String> {
    let mut charts = Vec::new();
    let mut chart_files = Vec::with_capacity(report.sections.len());
    for (index, section) in report.sections.iter().enumerate() {
        if section.chart {
            let filename = format!("chart-{index}.png");
            let png = render_chart(&section.samples)?;
            charts.push(ReportChart {
                filename: filename.clone(),
                png,
            });
            chart_files.push(Some(filename));
        } else {
            chart_files.push(None);
        }
    }

    let site_name = escape_html(&report.site_name);
    let site_url = escape_html(&report.site_url);
    let title = escape_html(&report.title);
    let period = format_period(report.period_start, report.period_end);

    let mut html = format!(
        concat!(
            "<!doctype html><html><head><meta charset=\"utf-8\"></head>",
            "<body style=\"margin:0;padding:16px;background:#f6f8fa;font-family:Arial,sans-serif;color:#24292f;\">",
            "<table role=\"presentation\" width=\"100%\" cellspacing=\"0\" cellpadding=\"0\" style=\"max-width:720px;margin:0 auto;background:#ffffff;border:1px solid #d0d7de;\">",
            "<tr><td style=\"padding:24px;\">",
            "<h1 style=\"font-size:22px;margin:0 0 8px;\"><a href=\"{site_url}\" style=\"color:#0969da;text-decoration:none;\">{site_name}</a></h1>",
            "<p style=\"margin:0 0 16px;color:#57606a;\">{title}</p>",
            "<p style=\"margin:0 0 16px;color:#57606a;\">UTC period: {period}</p>"
        ),
        site_url = site_url,
        site_name = site_name,
        title = title,
        period = period,
    );
    if let Some(message) = &report.message {
        html.push_str(&format!(
            "<p style=\"margin:0 0 16px;\">{}</p>",
            escape_html(message)
        ));
    }
    append_pond_size_html(&mut html, report.pond_size);

    for (section, chart_file) in report.sections.iter().zip(chart_files.iter()) {
        append_section_html(&mut html, section, chart_file.as_deref(), &report.site_url)?;
    }
    html.push_str("</td></tr></table></body></html>");

    let mut plain_text = format!(
        "{}\n{}\nUTC period: {}\n",
        report.site_name, report.title, period
    );
    if let Some(message) = &report.message {
        plain_text.push_str(message);
        plain_text.push('\n');
    }
    append_pond_size_text(&mut plain_text, report.pond_size);
    for section in &report.sections {
        append_section_text(&mut plain_text, section, &report.site_url)?;
    }

    Ok(RenderedReport {
        html,
        plain_text,
        charts,
    })
}

/// Escape text and attribute values before inserting them into HTML.
#[must_use]
pub(crate) fn escape_html(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(character),
        }
    }
    escaped
}

/// Resolve a configured relative or absolute link against the configured site.
pub(crate) fn resolve_link(site_url: &str, link: &str) -> Result<String, String> {
    match url::Url::parse(link) {
        Ok(url) => Ok(url.to_string()),
        Err(url::ParseError::RelativeUrlWithoutBase) => {
            let mut base = url::Url::parse(site_url)
                .map_err(|error| format!("invalid site_url while rendering link: {error}"))?;
            if !base.path().ends_with('/') {
                base.set_path(&format!("{}/", base.path()));
            }
            base.join(link)
                .map(|url| url.to_string())
                .map_err(|error| format!("invalid section link while rendering: {error}"))
        }
        Err(error) => Err(format!("invalid section link while rendering: {error}")),
    }
}

fn append_pond_size_html(html: &mut String, pond_size: PondSize) {
    match pond_size {
        PondSize::NotIncluded => {}
        PondSize::Unavailable => html.push_str(
            "<p style=\"margin:0 0 16px;color:#57606a;\">Pond allocated size: unavailable</p>",
        ),
        PondSize::Bytes(bytes) => html.push_str(&format!(
            "<p style=\"margin:0 0 16px;color:#57606a;\">Pond allocated size: {}</p>",
            format_bytes(bytes)
        )),
    }
}

fn append_pond_size_text(plain_text: &mut String, pond_size: PondSize) {
    match pond_size {
        PondSize::NotIncluded => {}
        PondSize::Unavailable => plain_text.push_str("Pond allocated size: unavailable\n"),
        PondSize::Bytes(bytes) => {
            plain_text.push_str(&format!("Pond allocated size: {}\n", format_bytes(bytes)));
        }
    }
}

fn append_section_html(
    html: &mut String,
    section: &ReportSection,
    chart_file: Option<&str>,
    site_url: &str,
) -> Result<(), String> {
    let title = escape_html(&section.title);
    let link = escape_html(&resolve_link(site_url, &section.link)?);
    let summary = calculate_summary(&section.samples, section.summary_kind);
    html.push_str(&format!(
        "<hr style=\"border:0;border-top:1px solid #d0d7de;margin:20px 0;\"><h2 style=\"font-size:18px;margin:0 0 8px;\">{title}</h2>"
    ));
    if section.samples.is_empty() {
        html.push_str("<p style=\"margin:0 0 8px;color:#57606a;\">No data in this period.</p>");
        html.push_str("<p style=\"margin:0 0 8px;color:#57606a;\">Samples: 0</p>");
    } else if summary.finite_count == 0 {
        html.push_str(
            "<p style=\"margin:0 0 8px;color:#57606a;\">No finite numeric data in this period.</p>",
        );
        html.push_str(&format!(
            "<p style=\"margin:0 0 8px;color:#57606a;\">Samples: {}</p>",
            summary.sample_count
        ));
    } else {
        html.push_str(
            "<table role=\"presentation\" cellspacing=\"0\" cellpadding=\"0\" style=\"border-collapse:collapse;margin:0 0 8px;\">",
        );
        append_summary_rows_html(html, &summary, section.summary_kind, &section.unit);
        html.push_str("</table>");
    }
    html.push_str(&format!(
        "<p style=\"margin:0 0 12px;\"><a href=\"{link}\" style=\"color:#0969da;\">View details</a></p>"
    ));
    if let Some(chart_file) = chart_file {
        let chart_file = escape_html(chart_file);
        html.push_str(&format!(
            "<img src=\"{chart_file}\" alt=\"{title} chart\" width=\"640\" style=\"display:block;width:100%;max-width:640px;height:auto;border:0;\">"
        ));
    }
    Ok(())
}

fn append_section_text(
    plain_text: &mut String,
    section: &ReportSection,
    site_url: &str,
) -> Result<(), String> {
    let summary = calculate_summary(&section.samples, section.summary_kind);
    plain_text.push_str(&format!("\n{}\n", section.title));
    if section.samples.is_empty() {
        plain_text.push_str("No data in this period.\nSamples: 0\n");
    } else if summary.finite_count == 0 {
        plain_text.push_str(&format!(
            "No finite numeric data in this period.\nSamples: {}\n",
            summary.sample_count
        ));
    } else {
        append_summary_text(plain_text, &summary, section.summary_kind, &section.unit);
    }
    plain_text.push_str(&format!(
        "View details: {}\n",
        resolve_link(site_url, &section.link)?
    ));
    Ok(())
}

fn append_summary_rows_html(
    html: &mut String,
    summary: &Summary,
    summary_kind: ReportSummary,
    unit: &str,
) {
    let unit = escape_html(unit);
    let row = |html: &mut String, label: &str, value: Option<f64>| {
        if let Some(value) = value {
            html.push_str(&format!(
                "<tr><td style=\"padding:3px 12px 3px 0;color:#57606a;\">{label}</td><td style=\"padding:3px 0;text-align:right;\">{} {unit}</td></tr>",
                format_number(value)
            ));
        }
    };
    match summary_kind {
        ReportSummary::Range => {
            row(html, "Latest", summary.latest);
            row(html, "Minimum", summary.minimum);
            row(html, "Maximum", summary.maximum);
        }
        ReportSummary::Sum => row(html, "Sum", summary.sum),
    }
    html.push_str(&format!(
        "<tr><td style=\"padding:3px 12px 3px 0;color:#57606a;\">Samples</td><td style=\"padding:3px 0;text-align:right;\">{}</td></tr>",
        summary.sample_count
    ));
}

fn append_summary_text(
    plain_text: &mut String,
    summary: &Summary,
    summary_kind: ReportSummary,
    unit: &str,
) {
    let line = |plain_text: &mut String, label: &str, value: Option<f64>| {
        if let Some(value) = value {
            plain_text.push_str(&format!("{label}: {} {unit}\n", format_number(value)));
        }
    };
    match summary_kind {
        ReportSummary::Range => {
            line(plain_text, "Latest", summary.latest);
            line(plain_text, "Minimum", summary.minimum);
            line(plain_text, "Maximum", summary.maximum);
        }
        ReportSummary::Sum => line(plain_text, "Sum", summary.sum),
    }
    plain_text.push_str(&format!("Samples: {}\n", summary.sample_count));
}

fn format_number(value: f64) -> String {
    format!("{value:.2}")
}

fn format_period(start: DateTime<Utc>, end: DateTime<Utc>) -> String {
    format!("{} to {}", start.to_rfc3339(), end.to_rfc3339())
}

fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let bytes_float = bytes as f64;
    if bytes_float >= GIB {
        format!("{:.2} GiB", bytes_float / GIB)
    } else if bytes_float >= MIB {
        format!("{:.2} MiB", bytes_float / MIB)
    } else if bytes_float >= KIB {
        format!("{:.2} KiB", bytes_float / KIB)
    } else {
        format!("{bytes} B")
    }
}

/// Produce a simple, browser-free RGB PNG line chart.
fn render_chart(samples: &[Sample]) -> Result<Vec<u8>, String> {
    const WIDTH: usize = 640;
    const HEIGHT: usize = 240;
    const LEFT: usize = 28;
    const RIGHT: usize = 12;
    const TOP: usize = 12;
    const BOTTOM: usize = 24;

    let mut pixels = vec![255_u8; WIDTH * HEIGHT * 3];
    let plot_width = WIDTH - LEFT - RIGHT;
    let plot_height = HEIGHT - TOP - BOTTOM;

    for step in 0..=4 {
        let y = TOP + (plot_height * step / 4);
        draw_line(
            &mut pixels,
            WIDTH,
            HEIGHT,
            (LEFT, y),
            (LEFT + plot_width, y),
            [225, 229, 234],
        );
        let x = LEFT + (plot_width * step / 4);
        draw_line(
            &mut pixels,
            WIDTH,
            HEIGHT,
            (x, TOP),
            (x, TOP + plot_height),
            [225, 229, 234],
        );
    }
    draw_line(
        &mut pixels,
        WIDTH,
        HEIGHT,
        (LEFT, TOP + plot_height),
        (LEFT + plot_width, TOP + plot_height),
        [139, 148, 158],
    );
    draw_line(
        &mut pixels,
        WIDTH,
        HEIGHT,
        (LEFT, TOP),
        (LEFT, TOP + plot_height),
        [139, 148, 158],
    );

    let finite: Vec<&Sample> = samples
        .iter()
        .filter(|sample| sample.value.is_finite())
        .collect();
    if !finite.is_empty() {
        let first_time = finite
            .iter()
            .map(|sample| sample.timestamp.timestamp_micros())
            .min()
            .ok_or_else(|| "chart timestamp range is empty".to_string())?;
        let last_time = finite
            .iter()
            .map(|sample| sample.timestamp.timestamp_micros())
            .max()
            .ok_or_else(|| "chart timestamp range is empty".to_string())?;
        let mut minimum = finite
            .iter()
            .map(|sample| sample.value)
            .reduce(f64::min)
            .ok_or_else(|| "chart value range is empty".to_string())?;
        let mut maximum = finite
            .iter()
            .map(|sample| sample.value)
            .reduce(f64::max)
            .ok_or_else(|| "chart value range is empty".to_string())?;
        if minimum == maximum {
            let padding = (minimum.abs() * 0.05).max(1.0);
            minimum -= padding;
            maximum += padding;
        }

        let mut previous: Option<(usize, usize)> = None;
        for sample in samples {
            if !sample.value.is_finite() {
                previous = None;
                continue;
            }
            let timestamp = sample.timestamp.timestamp_micros();
            let x_ratio = if first_time == last_time {
                0.5
            } else {
                (timestamp as f64 - first_time as f64) / (last_time as f64 - first_time as f64)
            };
            let y_ratio = (sample.value - minimum) / (maximum - minimum);
            let x = LEFT + (x_ratio.clamp(0.0, 1.0) * plot_width as f64).round() as usize;
            let y = TOP + ((1.0 - y_ratio.clamp(0.0, 1.0)) * plot_height as f64).round() as usize;
            if let Some((previous_x, previous_y)) = previous {
                draw_line(
                    &mut pixels,
                    WIDTH,
                    HEIGHT,
                    (previous_x, previous_y),
                    (x, y),
                    [9, 105, 218],
                );
            }
            draw_point(&mut pixels, WIDTH, HEIGHT, x, y, [9, 105, 218]);
            previous = Some((x, y));
        }
    }

    let mut encoded = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut encoded, WIDTH as u32, HEIGHT as u32);
        encoder.set_color(png::ColorType::Rgb);
        encoder.set_depth(png::BitDepth::Eight);
        encoder.set_compression(png::Compression::Fast);
        let mut writer = encoder
            .write_header()
            .map_err(|error| format!("failed to write chart PNG header: {error}"))?;
        writer
            .write_image_data(&pixels)
            .map_err(|error| format!("failed to write chart PNG: {error}"))?;
    }
    Ok(encoded)
}

fn draw_line(
    pixels: &mut [u8],
    width: usize,
    height: usize,
    start: (usize, usize),
    end: (usize, usize),
    color: [u8; 3],
) {
    let mut x0 = start.0 as isize;
    let mut y0 = start.1 as isize;
    let x1 = end.0 as isize;
    let y1 = end.1 as isize;
    let dx = (x1 - x0).abs();
    let step_x = if x0 < x1 { 1 } else { -1 };
    let dy = -(y1 - y0).abs();
    let step_y = if y0 < y1 { 1 } else { -1 };
    let mut error = dx + dy;
    loop {
        if x0 >= 0 && y0 >= 0 && x0 < width as isize && y0 < height as isize {
            set_pixel(pixels, width, x0 as usize, y0 as usize, color);
        }
        if x0 == x1 && y0 == y1 {
            break;
        }
        let twice_error = 2 * error;
        if twice_error >= dy {
            error += dy;
            x0 += step_x;
        }
        if twice_error <= dx {
            error += dx;
            y0 += step_y;
        }
    }
}

fn draw_point(pixels: &mut [u8], width: usize, height: usize, x: usize, y: usize, color: [u8; 3]) {
    for offset_y in -2_isize..=2 {
        for offset_x in -2_isize..=2 {
            if offset_x * offset_x + offset_y * offset_y > 4 {
                continue;
            }
            let point_x = x as isize + offset_x;
            let point_y = y as isize + offset_y;
            if point_x >= 0 && point_y >= 0 && point_x < width as isize && point_y < height as isize
            {
                set_pixel(pixels, width, point_x as usize, point_y as usize, color);
            }
        }
    }
}

fn set_pixel(pixels: &mut [u8], width: usize, x: usize, y: usize, color: [u8; 3]) {
    let offset = (y * width + x) * 3;
    pixels[offset..offset + 3].copy_from_slice(&color);
}

#[cfg(test)]
mod tests {
    use super::{
        PondSize, Report, ReportSection, Sample, calculate_summary, escape_html, render_chart,
        render_report, resolve_link, section_title, validate_reports, write_preview,
    };
    use crate::config::ReportSummary;
    use chrono::{DateTime, Utc};
    use datafusion::arrow::array::{Float64Array, TimestampMicrosecondArray};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    fn timestamp(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(seconds, 0).expect("valid test timestamp")
    }

    #[test]
    fn escapes_html_resolves_links_and_uses_local_charts() {
        let report = Report {
            site_name: "<Caspar & Co>".to_string(),
            site_url: "https://example.test/base/".to_string(),
            title: "Report".to_string(),
            message: Some("Check <the instruments>.".to_string()),
            period_start: timestamp(0),
            period_end: timestamp(60),
            pond_size: PondSize::Unavailable,
            sections: vec![ReportSection {
                title: "Depth <unsafe>".to_string(),
                unit: "ft".to_string(),
                summary_kind: ReportSummary::Range,
                link: "details?x=1&y=2".to_string(),
                chart: true,
                samples: vec![Sample {
                    timestamp: timestamp(10),
                    value: 2.0,
                }],
            }],
        };
        let rendered = render_report(&report).expect("render report");
        assert!(rendered.html.contains("&lt;Caspar &amp; Co&gt;"));
        assert!(rendered.html.contains("Check &lt;the instruments&gt;."));
        assert!(
            rendered
                .html
                .contains("https://example.test/base/details?x=1&amp;y=2")
        );
        assert!(rendered.html.contains("src=\"chart-0.png\""));
        assert_eq!(rendered.charts[0].filename, "chart-0.png");
        assert_eq!(
            resolve_link("https://example.test/base/", "/path").expect("resolve"),
            "https://example.test/path"
        );
        assert_eq!(
            resolve_link("https://example.test/base", "path").expect("resolve"),
            "https://example.test/base/path"
        );
        assert_eq!(escape_html("\"'"), "&quot;&#39;");
    }

    #[test]
    fn computes_summaries_without_non_finite_values() {
        let samples = vec![
            Sample {
                timestamp: timestamp(1),
                value: 3.0,
            },
            Sample {
                timestamp: timestamp(2),
                value: f64::NAN,
            },
            Sample {
                timestamp: timestamp(3),
                value: 7.0,
            },
        ];
        let range = calculate_summary(&samples, ReportSummary::Range);
        assert_eq!(range.sample_count, 3);
        assert_eq!(range.finite_count, 2);
        assert_eq!(range.latest, Some(7.0));
        assert_eq!(range.minimum, Some(3.0));
        assert_eq!(range.maximum, Some(7.0));
        assert_eq!(range.sum, None);
        assert_eq!(
            calculate_summary(&samples, ReportSummary::Sum).sum,
            Some(10.0)
        );
    }

    #[test]
    fn sum_sections_render_only_the_total() {
        let samples = vec![
            Sample {
                timestamp: timestamp(1),
                value: 3.0,
            },
            Sample {
                timestamp: timestamp(2),
                value: 7.0,
            },
        ];
        let summary = calculate_summary(&samples, ReportSummary::Sum);
        let mut text = String::new();
        super::append_summary_text(&mut text, &summary, ReportSummary::Sum, "gal");
        assert!(text.contains("Sum: 10.00 gal"));
        assert!(!text.contains("Latest:"));
        assert!(!text.contains("Minimum:"));
        assert!(!text.contains("Maximum:"));
    }

    #[test]
    fn produces_pngs_for_empty_constant_and_non_finite_series() {
        for samples in [
            Vec::new(),
            vec![Sample {
                timestamp: timestamp(1),
                value: 3.0,
            }],
            vec![
                Sample {
                    timestamp: timestamp(1),
                    value: 3.0,
                },
                Sample {
                    timestamp: timestamp(2),
                    value: 3.0,
                },
            ],
            vec![Sample {
                timestamp: timestamp(1),
                value: f64::NAN,
            }],
        ] {
            let png = render_chart(&samples).expect("render chart");
            assert!(png.starts_with(b"\x89PNG\r\n\x1a\n"));
        }
    }

    #[test]
    fn validates_report_export_references() {
        let yaml = r#"
site:
  title: Test
exports:
  - name: metrics
    pattern: /reduced/*/*.series
reports:
  weekly:
    title: Weekly
    window: 7d
    sections:
      - export: missing
        captures: [depth, res=1h]
        value: depth.avg
        unit: m
        href: data/depth.html
"#;
        let config: crate::config::SiteConfig =
            serde_yaml::from_str(yaml).expect("parse report config");
        assert_eq!(
            validate_reports(&config).expect_err("unknown export must fail"),
            "report 'weekly' section 0 references unknown export 'missing'"
        );
    }

    #[test]
    fn report_sections_reuse_site_labels() {
        let yaml = r#"
site:
  title: Test
exports:
  - name: metrics
    pattern: /reduced/*/*.series
reports:
  weekly:
    title: Weekly
    window: 7d
    sections:
      - export: metrics
        captures: [well-depth, res=1h]
        value: depth.avg
        unit: m
        href: data/depth.html
labels:
  well-depth: Well level
"#;
        let config: crate::config::SiteConfig =
            serde_yaml::from_str(yaml).expect("parse report config");
        let section = &config.reports["weekly"].sections[0];
        assert_eq!(section_title(&config, section), "Well level");
    }

    #[tokio::test]
    async fn query_filters_to_the_configured_window() {
        let day = 86_400_000_000_i64;
        let end = timestamp(10 * 86_400);
        let start_micros = end.timestamp_micros() - 7 * day;
        let batch = RecordBatch::try_from_iter(vec![
            (
                "timestamp",
                Arc::new(TimestampMicrosecondArray::from(vec![
                    start_micros - 1,
                    start_micros,
                    end.timestamp_micros(),
                    end.timestamp_micros() + 1,
                ])) as _,
            ),
            (
                "value",
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])) as _,
            ),
        ])
        .expect("record batch");
        let context = SessionContext::new();
        context
            .register_batch("samples", batch)
            .expect("register samples");

        let samples = super::query_samples(
            &context,
            "samples",
            "timestamp",
            "value",
            end - chrono::Duration::days(7),
            end,
        )
        .await
        .expect("query samples");
        assert_eq!(
            samples
                .iter()
                .map(|sample| sample.value)
                .collect::<Vec<_>>(),
            vec![2.0]
        );
    }

    #[test]
    fn writes_browser_viewable_preview_artifacts() {
        let report = Report {
            site_name: "Test".to_string(),
            site_url: "https://example.test/".to_string(),
            title: "Weekly".to_string(),
            message: None,
            period_start: timestamp(0),
            period_end: timestamp(60),
            pond_size: PondSize::NotIncluded,
            sections: vec![ReportSection {
                title: "Depth".to_string(),
                unit: "m".to_string(),
                summary_kind: ReportSummary::Range,
                link: "depth.html".to_string(),
                chart: true,
                samples: vec![Sample {
                    timestamp: timestamp(30),
                    value: 1.0,
                }],
            }],
        };
        let rendered = render_report(&report).expect("render report");
        let output = tempfile::tempdir().expect("temporary directory");
        write_preview(&rendered, output.path()).expect("write preview");
        assert!(output.path().join("report.html").is_file());
        assert!(output.path().join("report.txt").is_file());
        assert!(output.path().join("chart-0.png").is_file());
    }
}
