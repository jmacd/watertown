// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use chrono::{DateTime, SecondsFormat, Utc};
use datafusion::arrow::array::{Array, Float64Array, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::TableReference;
use provider::{ExecutionContext, ExecutionMode, FactoryContext, register_executable_factory};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tinyfs::{ResultExt, SeriesReadBounds};

static MONITOR_QUERY_ID: AtomicU64 = AtomicU64::new(0);
static TEMP_FILE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct MonitorConfig {
    pond: String,
    title: String,
    output_dir: String,
    checks: Vec<CheckConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum CheckConfig {
    RateWhile(RateWhileCheckConfig),
    AnyObservedBelow(AnyObservedBelowCheckConfig),
    AverageRange(AverageRangeCheckConfig),
    AllObservedBelow(BelowCheckConfig),
    StaleAfter(StaleAfterCheckConfig),
    GradedScore(GradedScoreCheckConfig),
    StaleOrBelow(StaleOrBelowCheckConfig),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BelowCheckConfig {
    id: String,
    label: String,
    description: String,
    href: String,
    source: String,
    #[serde(default = "default_timestamp_column")]
    timestamp_column: String,
    value_column: String,
    #[serde(default)]
    unit: String,
    threshold: f64,
    window: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
enum AnyObservedBelowType {
    #[serde(rename = "any-observed-below")]
    AnyObservedBelow,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AnyObservedBelowCheckConfig {
    id: String,
    label: String,
    description: String,
    href: String,
    #[serde(rename = "type")]
    check_type: AnyObservedBelowType,
    source: String,
    #[serde(default = "default_timestamp_column")]
    timestamp_column: String,
    value_column: String,
    #[serde(default)]
    unit: String,
    threshold: f64,
    window: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
enum StaleOrBelowType {
    #[serde(rename = "stale-or-below")]
    StaleOrBelow,
}

/// Alarms if the source has produced zero samples in `window` (stale or
/// missing), or if any observed sample is below `threshold` -- combining
/// the staleness and low-value failure modes of a single instrument (e.g. a
/// pH probe) into one check instead of requiring two separate cards.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StaleOrBelowCheckConfig {
    id: String,
    label: String,
    description: String,
    href: String,
    #[serde(rename = "type")]
    check_type: StaleOrBelowType,
    source: String,
    #[serde(default = "default_timestamp_column")]
    timestamp_column: String,
    value_column: String,
    #[serde(default)]
    unit: String,
    threshold: f64,
    window: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
enum AverageRangeType {
    #[serde(rename = "average-range")]
    AverageRange,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AverageRangeCheckConfig {
    id: String,
    label: String,
    description: String,
    href: String,
    #[serde(rename = "type")]
    check_type: AverageRangeType,
    source: String,
    #[serde(default = "default_timestamp_column")]
    timestamp_column: String,
    value_column: String,
    #[serde(default)]
    unit: String,
    lower_bound: f64,
    upper_bound: f64,
    window: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
enum StaleAfterType {
    #[serde(rename = "stale-after")]
    StaleAfter,
}

/// Alarms when a source has produced zero non-null samples within `window`.
/// Unlike the other scalar checks, an empty sample set here *is* the alarm
/// condition rather than an unknown/indeterminate state: it means the data
/// source has stopped producing new data.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StaleAfterCheckConfig {
    id: String,
    label: String,
    description: String,
    href: String,
    #[serde(rename = "type")]
    check_type: StaleAfterType,
    source: String,
    #[serde(default = "default_timestamp_column")]
    timestamp_column: String,
    value_column: String,
    #[serde(default)]
    unit: String,
    window: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
enum GradedScoreType {
    #[serde(rename = "graded-score")]
    GradedScore,
}

/// Classifies the latest sample of a continuous 0.0-1.0-style score into one
/// of four quality bands (good/watch/concern/alarm) instead of a plain
/// pass/fail threshold, for signals like the leak-probability score where a
/// single alarm/healthy split loses the gradient the operator cares about.
/// `good_max < watch_max < concern_max` must hold; values above
/// `concern_max`, and a missing/no-data window, both land in the worst
/// (`alarm`) band -- absence of a recent score is treated as the worst case,
/// not as "unknown", so a quiet source can't hide a real leak behind a
/// gray card.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GradedScoreCheckConfig {
    id: String,
    label: String,
    description: String,
    href: String,
    #[serde(rename = "type")]
    check_type: GradedScoreType,
    source: String,
    #[serde(default = "default_timestamp_column")]
    timestamp_column: String,
    value_column: String,
    #[serde(default)]
    unit: String,
    good_max: f64,
    watch_max: f64,
    concern_max: f64,
    window: String,
}

trait ScalarCheck {
    fn id(&self) -> &str;
    fn label(&self) -> &str;
    fn description(&self) -> &str;
    fn href(&self) -> &str;
    fn source(&self) -> &str;
    fn timestamp_column(&self) -> &str;
    fn value_column(&self) -> &str;
    fn window(&self) -> &str;
}

macro_rules! impl_scalar_check {
    ($type:ty) => {
        impl ScalarCheck for $type {
            fn id(&self) -> &str {
                &self.id
            }

            fn label(&self) -> &str {
                &self.label
            }

            fn description(&self) -> &str {
                &self.description
            }

            fn href(&self) -> &str {
                &self.href
            }

            fn source(&self) -> &str {
                &self.source
            }

            fn timestamp_column(&self) -> &str {
                &self.timestamp_column
            }

            fn value_column(&self) -> &str {
                &self.value_column
            }

            fn window(&self) -> &str {
                &self.window
            }
        }
    };
}

impl_scalar_check!(BelowCheckConfig);
impl_scalar_check!(AnyObservedBelowCheckConfig);
impl_scalar_check!(AverageRangeCheckConfig);
impl_scalar_check!(StaleAfterCheckConfig);
impl_scalar_check!(GradedScoreCheckConfig);
impl_scalar_check!(StaleOrBelowCheckConfig);

fn default_timestamp_column() -> String {
    "timestamp".to_string()
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
enum RateWhileType {
    #[serde(rename = "rate-while")]
    RateWhile,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RateWhileCheckConfig {
    id: String,
    label: String,
    description: String,
    href: String,
    #[serde(rename = "type")]
    check_type: RateWhileType,
    measurement: MeasurementConfig,
    condition: ConditionConfig,
    alignment: AlignmentConfig,
    window: String,
    minimum_active_time: String,
    alarm: AlarmConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct MeasurementConfig {
    source: String,
    #[serde(default = "default_timestamp_column")]
    timestamp: String,
    column: String,
    accumulation: Accumulation,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Accumulation {
    PositiveDeltas,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ConditionConfig {
    source: String,
    #[serde(default = "default_timestamp_column")]
    timestamp: String,
    predicate: PredicateConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PredicateConfig {
    column: String,
    operator: PredicateOperator,
    value: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum PredicateOperator {
    Eq,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AlignmentConfig {
    method: AlignmentMethod,
    tolerance: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum AlignmentMethod {
    Previous,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AlarmConfig {
    operator: AlarmOperator,
    value: f64,
    #[serde(default)]
    unit: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum AlarmOperator {
    Lt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum CheckState {
    Healthy,
    Alarm,
    Unknown,
}

impl CheckState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Alarm => "alarm",
            Self::Unknown => "unknown",
        }
    }
}

/// A finer-grained quality tone for checks whose raw value is itself a
/// meaningful continuous score (e.g. a leak probability), not just a
/// pass/fail threshold. `state` still drives the pond-wide alarm rollup
/// (good/watch -> healthy, concern/alarm -> alarm); `tone` carries the
/// additional band for display so a card can show more than two colors.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Tone {
    Good,
    Watch,
    Concern,
    Alarm,
}

#[derive(Clone, Debug)]
struct Sample {
    timestamp: DateTime<Utc>,
    value: f64,
}

#[derive(Clone, Debug)]
struct RateSample {
    timestamp: DateTime<Utc>,
    value: f64,
    positive_delta: Option<f64>,
}

#[derive(Clone, Debug)]
struct ConditionSample {
    timestamp: DateTime<Utc>,
    value: String,
}

#[derive(Debug, Serialize)]
struct CheckStatus {
    id: String,
    label: String,
    description: String,
    href: String,
    state: CheckState,
    #[serde(skip_serializing_if = "Option::is_none")]
    tone: Option<Tone>,
    rule: &'static str,
    source: String,
    value_column: String,
    unit: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    threshold: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lower_bound: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    upper_bound: Option<f64>,
    window_seconds: u64,
    sample_count: usize,
    observed_start: Option<String>,
    observed_end: Option<String>,
    latest_value: Option<f64>,
    minimum: Option<f64>,
    maximum: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    average: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    condition_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_seconds: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    minimum_active_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    accumulated_change: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rate: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    aligned_interval_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unaligned_interval_count: Option<usize>,
}

#[derive(Debug, Serialize)]
struct MonitorStatus {
    schema_version: u32,
    pond: String,
    title: String,
    generated_at: String,
    transaction_sequence: i64,
    state: CheckState,
    checks: Vec<CheckStatus>,
}

fn validate_config(config_bytes: &[u8]) -> tinyfs::Result<Value> {
    let config: MonitorConfig =
        serde_yaml::from_slice(config_bytes).map_other_context("Invalid monitor-report config")?;
    validate(&config)?;
    serde_json::to_value(config).map_other_context("Monitor config serialization failed")
}

fn validate(config: &MonitorConfig) -> tinyfs::Result<()> {
    if config.pond.trim().is_empty() {
        return Err(tinyfs::Error::Other(
            "monitor-report pond cannot be empty".to_string(),
        ));
    }
    if config.title.trim().is_empty() {
        return Err(tinyfs::Error::Other(
            "monitor-report title cannot be empty".to_string(),
        ));
    }
    if config.output_dir.trim().is_empty() {
        return Err(tinyfs::Error::Other(
            "monitor-report output_dir cannot be empty".to_string(),
        ));
    }
    if !Path::new(&config.output_dir).is_absolute() {
        return Err(tinyfs::Error::Other(
            "monitor-report output_dir must be absolute".to_string(),
        ));
    }
    let mut ids = HashSet::new();
    for check in &config.checks {
        if check.id().is_empty()
            || !check.id().chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
            })
        {
            return Err(tinyfs::Error::Other(format!(
                "monitor-report check id '{}' must contain only ASCII letters, digits, '-' or '_'",
                check.id()
            )));
        }
        if !ids.insert(check.id()) {
            return Err(tinyfs::Error::Other(format!(
                "monitor-report check id '{}' is duplicated",
                check.id()
            )));
        }
        match check {
            CheckConfig::AllObservedBelow(check) => validate_below_check(check)?,
            CheckConfig::AnyObservedBelow(check) => validate_any_observed_below_check(check)?,
            CheckConfig::AverageRange(check) => validate_average_range_check(check)?,
            CheckConfig::RateWhile(check) => validate_rate_while_check(check)?,
            CheckConfig::StaleAfter(check) => validate_stale_after_check(check)?,
            CheckConfig::GradedScore(check) => validate_graded_score_check(check)?,
            CheckConfig::StaleOrBelow(check) => validate_stale_or_below_check(check)?,
        }
    }
    Ok(())
}

impl CheckConfig {
    fn id(&self) -> &str {
        match self {
            Self::AllObservedBelow(check) => &check.id,
            Self::AnyObservedBelow(check) => &check.id,
            Self::AverageRange(check) => &check.id,
            Self::RateWhile(check) => &check.id,
            Self::StaleAfter(check) => &check.id,
            Self::GradedScore(check) => &check.id,
            Self::StaleOrBelow(check) => &check.id,
        }
    }
}

fn validate_below_check(check: &BelowCheckConfig) -> tinyfs::Result<()> {
    validate_scalar_check(check)?;
    if !check.threshold.is_finite() {
        return Err(tinyfs::Error::Other(format!(
            "monitor-report check '{}' threshold must be finite",
            check.id
        )));
    }
    Ok(())
}

fn validate_any_observed_below_check(check: &AnyObservedBelowCheckConfig) -> tinyfs::Result<()> {
    validate_scalar_check(check)?;
    if !check.threshold.is_finite() {
        return Err(tinyfs::Error::Other(format!(
            "monitor-report check '{}' threshold must be finite",
            check.id
        )));
    }
    Ok(())
}

fn validate_average_range_check(check: &AverageRangeCheckConfig) -> tinyfs::Result<()> {
    validate_scalar_check(check)?;
    if !check.lower_bound.is_finite() || !check.upper_bound.is_finite() {
        return Err(tinyfs::Error::Other(format!(
            "monitor-report check '{}' range bounds must be finite",
            check.id
        )));
    }
    if check.lower_bound > check.upper_bound {
        return Err(tinyfs::Error::Other(format!(
            "monitor-report check '{}' lower_bound cannot exceed upper_bound",
            check.id
        )));
    }
    Ok(())
}

fn validate_stale_after_check(check: &StaleAfterCheckConfig) -> tinyfs::Result<()> {
    validate_scalar_check(check)
}

fn validate_stale_or_below_check(check: &StaleOrBelowCheckConfig) -> tinyfs::Result<()> {
    validate_scalar_check(check)?;
    if !check.threshold.is_finite() {
        return Err(tinyfs::Error::Other(format!(
            "monitor-report check '{}' threshold must be finite",
            check.id
        )));
    }
    Ok(())
}

fn validate_graded_score_check(check: &GradedScoreCheckConfig) -> tinyfs::Result<()> {
    validate_scalar_check(check)?;
    if !check.good_max.is_finite() || !check.watch_max.is_finite() || !check.concern_max.is_finite()
    {
        return Err(tinyfs::Error::Other(format!(
            "monitor-report check '{}' good_max, watch_max, and concern_max must be finite",
            check.id
        )));
    }
    if !(check.good_max < check.watch_max && check.watch_max < check.concern_max) {
        return Err(tinyfs::Error::Other(format!(
            "monitor-report check '{}' requires good_max < watch_max < concern_max",
            check.id
        )));
    }
    Ok(())
}

fn validate_scalar_check(check: &impl ScalarCheck) -> tinyfs::Result<()> {
    if check.label().trim().is_empty()
        || check.description().trim().is_empty()
        || check.href().trim().is_empty()
        || check.source().trim().is_empty()
        || check.timestamp_column().trim().is_empty()
        || check.value_column().trim().is_empty()
    {
        return Err(tinyfs::Error::Other(format!(
            "monitor-report check '{}' has an empty label, description, href, source, timestamp_column, or value_column",
            check.id()
        )));
    }
    validate_href(check.id(), check.href())?;
    let _ = validate_duration(check.id(), "window", check.window())?;
    Ok(())
}

fn validate_rate_while_check(check: &RateWhileCheckConfig) -> tinyfs::Result<()> {
    if check.label.trim().is_empty()
        || check.description.trim().is_empty()
        || check.href.trim().is_empty()
        || check.measurement.source.trim().is_empty()
        || check.measurement.timestamp.trim().is_empty()
        || check.measurement.column.trim().is_empty()
        || check.condition.source.trim().is_empty()
        || check.condition.timestamp.trim().is_empty()
        || check.condition.predicate.column.trim().is_empty()
        || check.condition.predicate.value.trim().is_empty()
    {
        return Err(tinyfs::Error::Other(format!(
            "monitor-report rate-while check '{}' has an empty label, description, href, source, timestamp, column, or predicate value",
            check.id
        )));
    }
    validate_href(&check.id, &check.href)?;
    if !check.alarm.value.is_finite() {
        return Err(tinyfs::Error::Other(format!(
            "monitor-report check '{}' alarm value must be finite",
            check.id
        )));
    }
    let window = validate_duration(&check.id, "window", &check.window)?;
    let minimum_active_time =
        validate_duration(&check.id, "minimum_active_time", &check.minimum_active_time)?;
    let _ = validate_duration(&check.id, "alignment tolerance", &check.alignment.tolerance)?;
    if minimum_active_time > window {
        return Err(tinyfs::Error::Other(format!(
            "monitor-report check '{}' minimum_active_time cannot exceed its window",
            check.id
        )));
    }
    Ok(())
}

fn validate_href(check_id: &str, href: &str) -> tinyfs::Result<()> {
    if !href.starts_with('/') || href.starts_with("//") {
        return Err(tinyfs::Error::Other(format!(
            "monitor-report check '{check_id}' href must be an absolute same-origin site path beginning with one '/'"
        )));
    }
    Ok(())
}

fn validate_duration(
    check_id: &str,
    name: &str,
    value: &str,
) -> tinyfs::Result<std::time::Duration> {
    let duration = humantime::parse_duration(value).map_err(|error| {
        tinyfs::Error::Other(format!(
            "monitor-report check '{check_id}' has an invalid {name}: {error}"
        ))
    })?;
    if duration.is_zero() {
        return Err(tinyfs::Error::Other(format!(
            "monitor-report check '{check_id}' {name} must be greater than zero"
        )));
    }
    Ok(duration)
}

async fn initialize(_config: Value, _context: FactoryContext) -> tinyfs::Result<()> {
    Ok(())
}

async fn execute(
    config: Value,
    context: FactoryContext,
    execution: ExecutionContext,
) -> tinyfs::Result<()> {
    if !matches!(
        execution.mode(),
        ExecutionMode::ControlReader | ExecutionMode::PondReadWriter
    ) {
        return Err(tinyfs::Error::Other(format!(
            "monitor-report requires ControlReader or PondReadWriter mode, got {:?}",
            execution.mode()
        )));
    }
    if execution.mode() == ExecutionMode::ControlReader && execution.args() != ["push".to_string()]
    {
        return Err(tinyfs::Error::Other(format!(
            "automatic monitor-report execution requires the 'push' mode, got {:?}",
            execution.args()
        )));
    }

    let config: MonitorConfig =
        serde_json::from_value(config).map_other_context("Invalid monitor-report config")?;
    validate(&config)?;

    let generated_at = Utc::now();
    let root = context.root().await?;
    let mut checks = Vec::with_capacity(config.checks.len());
    for check in &config.checks {
        checks.push(evaluate_check(check, &root, &context.context, generated_at).await?);
    }

    let state = overall_state(&checks);
    let status = MonitorStatus {
        schema_version: 4,
        pond: config.pond,
        title: config.title,
        generated_at: format_timestamp(generated_at),
        transaction_sequence: context.txn_seq,
        state,
        checks,
    };
    publish(&status, Path::new(&config.output_dir))?;
    log::info!(
        "Published {} monitor status for pond '{}' to {}",
        status.state.as_str(),
        status.pond,
        config.output_dir
    );
    Ok(())
}

async fn evaluate_check(
    config: &CheckConfig,
    root: &tinyfs::WD,
    provider_context: &tinyfs::ProviderContext,
    period_end: DateTime<Utc>,
) -> tinyfs::Result<CheckStatus> {
    match config {
        CheckConfig::AllObservedBelow(config) => {
            evaluate_below_check(config, root, provider_context, period_end).await
        }
        CheckConfig::AnyObservedBelow(config) => {
            evaluate_any_observed_below_check(config, root, provider_context, period_end).await
        }
        CheckConfig::AverageRange(config) => {
            evaluate_average_range_check(config, root, provider_context, period_end).await
        }
        CheckConfig::RateWhile(config) => {
            evaluate_rate_while_check(config, root, provider_context, period_end).await
        }
        CheckConfig::StaleAfter(config) => {
            evaluate_stale_after_check(config, root, provider_context, period_end).await
        }
        CheckConfig::GradedScore(config) => {
            evaluate_graded_score_check(config, root, provider_context, period_end).await
        }
        CheckConfig::StaleOrBelow(config) => {
            evaluate_stale_or_below_check(config, root, provider_context, period_end).await
        }
    }
}

async fn evaluate_below_check(
    config: &BelowCheckConfig,
    root: &tinyfs::WD,
    provider_context: &tinyfs::ProviderContext,
    period_end: DateTime<Utc>,
) -> tinyfs::Result<CheckStatus> {
    let (window_seconds, samples) =
        collect_scalar_window(config, root, provider_context, period_end).await?;
    Ok(classify_all_observed_below(
        config,
        window_seconds,
        &samples,
    ))
}

async fn evaluate_any_observed_below_check(
    config: &AnyObservedBelowCheckConfig,
    root: &tinyfs::WD,
    provider_context: &tinyfs::ProviderContext,
    period_end: DateTime<Utc>,
) -> tinyfs::Result<CheckStatus> {
    let (window_seconds, samples) =
        collect_scalar_window(config, root, provider_context, period_end).await?;
    Ok(classify_any_observed_below(
        config,
        window_seconds,
        &samples,
    ))
}

async fn evaluate_average_range_check(
    config: &AverageRangeCheckConfig,
    root: &tinyfs::WD,
    provider_context: &tinyfs::ProviderContext,
    period_end: DateTime<Utc>,
) -> tinyfs::Result<CheckStatus> {
    let (window_seconds, samples) =
        collect_scalar_window(config, root, provider_context, period_end).await?;
    classify_average_range(config, window_seconds, &samples)
}

async fn evaluate_stale_after_check(
    config: &StaleAfterCheckConfig,
    root: &tinyfs::WD,
    provider_context: &tinyfs::ProviderContext,
    period_end: DateTime<Utc>,
) -> tinyfs::Result<CheckStatus> {
    let (window_seconds, samples) =
        collect_scalar_window(config, root, provider_context, period_end).await?;
    Ok(classify_stale_after(config, window_seconds, &samples))
}

async fn evaluate_graded_score_check(
    config: &GradedScoreCheckConfig,
    root: &tinyfs::WD,
    provider_context: &tinyfs::ProviderContext,
    period_end: DateTime<Utc>,
) -> tinyfs::Result<CheckStatus> {
    let (window_seconds, samples) =
        collect_scalar_window(config, root, provider_context, period_end).await?;
    Ok(classify_graded_score(config, window_seconds, &samples))
}

async fn evaluate_stale_or_below_check(
    config: &StaleOrBelowCheckConfig,
    root: &tinyfs::WD,
    provider_context: &tinyfs::ProviderContext,
    period_end: DateTime<Utc>,
) -> tinyfs::Result<CheckStatus> {
    let (window_seconds, samples) =
        collect_scalar_window(config, root, provider_context, period_end).await?;
    Ok(classify_stale_or_below(config, window_seconds, &samples))
}

async fn collect_scalar_window(
    config: &impl ScalarCheck,
    root: &tinyfs::WD,
    provider_context: &tinyfs::ProviderContext,
    period_end: DateTime<Utc>,
) -> tinyfs::Result<(u64, Vec<Sample>)> {
    let window_text = config.window();
    let window = humantime::parse_duration(window_text).map_err(|error| {
        tinyfs::Error::Other(format!("invalid monitor window '{window_text}': {error}"))
    })?;
    let chrono_window = chrono::Duration::from_std(window)
        .map_err(|error| tinyfs::Error::Other(format!("monitor window is too large: {error}")))?;
    let period_start = period_end
        .checked_sub_signed(chrono_window)
        .ok_or_else(|| {
            tinyfs::Error::Other(format!(
                "monitor window '{window_text}' exceeds the timestamp range"
            ))
        })?;
    let samples = collect_samples(config, root, provider_context, period_start, period_end).await?;
    Ok((window.as_secs(), samples))
}

async fn evaluate_rate_while_check(
    config: &RateWhileCheckConfig,
    root: &tinyfs::WD,
    provider_context: &tinyfs::ProviderContext,
    period_end: DateTime<Utc>,
) -> tinyfs::Result<CheckStatus> {
    let window = humantime::parse_duration(&config.window).map_err(|error| {
        tinyfs::Error::Other(format!(
            "invalid monitor window '{}': {error}",
            config.window
        ))
    })?;
    let chrono_window = chrono::Duration::from_std(window)
        .map_err(|error| tinyfs::Error::Other(format!("monitor window is too large: {error}")))?;
    let period_start = period_end
        .checked_sub_signed(chrono_window)
        .ok_or_else(|| {
            tinyfs::Error::Other(format!(
                "monitor window '{}' exceeds the timestamp range",
                config.window
            ))
        })?;
    let tolerance = humantime::parse_duration(&config.alignment.tolerance).map_err(|error| {
        tinyfs::Error::Other(format!(
            "invalid monitor alignment tolerance '{}': {error}",
            config.alignment.tolerance
        ))
    })?;
    let condition_start = period_start
        .checked_sub_signed(chrono::Duration::from_std(tolerance).map_err(|error| {
            tinyfs::Error::Other(format!("monitor alignment tolerance is too large: {error}"))
        })?)
        .ok_or_else(|| {
            tinyfs::Error::Other(format!(
                "monitor alignment tolerance '{}' exceeds the timestamp range",
                config.alignment.tolerance
            ))
        })?;
    let measurements =
        collect_rate_samples(config, root, provider_context, period_start, period_end).await?;
    let conditions =
        collect_condition_samples(config, root, provider_context, condition_start, period_end)
            .await?;
    classify_rate_while(config, window.as_secs(), &measurements, &conditions)
}

async fn collect_rate_samples(
    config: &RateWhileCheckConfig,
    root: &tinyfs::WD,
    provider_context: &tinyfs::ProviderContext,
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
) -> tinyfs::Result<Vec<RateSample>> {
    let Some((table_name, table_ref)) = register_monitor_source(
        &config.id,
        &config.measurement.source,
        root,
        provider_context,
        period_start,
    )
    .await?
    else {
        return Ok(Vec::new());
    };
    let result = query_rate_samples(
        &provider_context.datafusion_session,
        &table_name,
        &config.measurement.timestamp,
        &config.measurement.column,
        period_start,
        period_end,
    )
    .await;
    finish_monitor_query(
        &config.id,
        result,
        provider_context
            .datafusion_session
            .deregister_table(table_ref),
    )
}

async fn collect_condition_samples(
    config: &RateWhileCheckConfig,
    root: &tinyfs::WD,
    provider_context: &tinyfs::ProviderContext,
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
) -> tinyfs::Result<Vec<ConditionSample>> {
    let Some((table_name, table_ref)) = register_monitor_source(
        &config.id,
        &config.condition.source,
        root,
        provider_context,
        period_start,
    )
    .await?
    else {
        return Ok(Vec::new());
    };
    let result = query_condition_samples(
        &provider_context.datafusion_session,
        &table_name,
        &config.condition.timestamp,
        &config.condition.predicate.column,
        period_start,
        period_end,
    )
    .await;
    finish_monitor_query(
        &config.id,
        result,
        provider_context
            .datafusion_session
            .deregister_table(table_ref),
    )
}

async fn register_monitor_source(
    check_id: &str,
    source: &str,
    root: &tinyfs::WD,
    provider_context: &tinyfs::ProviderContext,
    period_start: DateTime<Utc>,
) -> tinyfs::Result<Option<(String, TableReference)>> {
    let matcher_provider = provider::Provider::with_context(
        Arc::new(provider_context.filesystem()),
        Arc::new(provider_context.clone()),
    )
    .with_root(root.clone());
    match provider::UrlPatternMatcher::new(matcher_provider)
        .match_pattern(source, provider_context)
        .await
    {
        Ok(matches) if matches.is_empty() => return Ok(None),
        Ok(_) => {}
        Err(provider::Error::NoFilesMatched(_)) => return Ok(None),
        Err(error) => {
            return Err(tinyfs::Error::Other(format!(
                "monitor check '{check_id}' failed to match source '{source}': {error}"
            )));
        }
    }

    let provider = provider::Provider::with_context(
        Arc::new(provider_context.filesystem()),
        Arc::new(provider_context.clone()),
    )
    .with_root(root.clone());
    let table_provider = match provider
        .create_provider_for_url_bounded(
            source,
            &provider_context.datafusion_session,
            SeriesReadBounds::from_event_time_lo(period_start.timestamp_micros()),
        )
        .await
    {
        Ok(table_provider) => table_provider,
        Err(provider::Error::NoFilesMatched(_)) => return Ok(None),
        Err(error) => {
            return Err(tinyfs::Error::Other(format!(
                "monitor check '{check_id}' failed to open source '{source}': {error}"
            )));
        }
    };

    let query_id = MONITOR_QUERY_ID.fetch_add(1, Ordering::Relaxed);
    let table_name = format!("monitor_{query_id}");
    let table_ref = TableReference::bare(table_name.as_str());
    provider_context
        .datafusion_session
        .register_table(table_ref.clone(), table_provider)
        .map_err(|error| {
            tinyfs::Error::Other(format!(
                "monitor check '{check_id}' failed to register source: {error}"
            ))
        })?;
    Ok(Some((table_name, table_ref)))
}

fn finish_monitor_query<T>(
    check_id: &str,
    result: tinyfs::Result<T>,
    deregister_result: datafusion::common::Result<
        Option<Arc<dyn datafusion::datasource::TableProvider>>,
    >,
) -> tinyfs::Result<T> {
    let deregister_result = deregister_result.map_err(|error| {
        tinyfs::Error::Other(format!(
            "monitor check '{check_id}' failed to deregister source: {error}"
        ))
    });
    match (result, deregister_result) {
        (Ok(value), Ok(_)) => Ok(value),
        (Err(error), Ok(_)) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(query_error), Err(deregister_error)) => Err(tinyfs::Error::Other(format!(
            "{query_error}; additionally failed to deregister monitor source: {deregister_error}"
        ))),
    }
}

async fn collect_samples(
    config: &impl ScalarCheck,
    root: &tinyfs::WD,
    provider_context: &tinyfs::ProviderContext,
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
) -> tinyfs::Result<Vec<Sample>> {
    let Some((table_name, table_ref)) = register_monitor_source(
        config.id(),
        config.source(),
        root,
        provider_context,
        period_start,
    )
    .await?
    else {
        return Ok(Vec::new());
    };
    let result = query_samples(
        &provider_context.datafusion_session,
        &table_name,
        config.timestamp_column(),
        config.value_column(),
        period_start,
        period_end,
    )
    .await;
    finish_monitor_query(
        config.id(),
        result,
        provider_context
            .datafusion_session
            .deregister_table(table_ref),
    )
}

async fn query_samples(
    session: &datafusion::prelude::SessionContext,
    table_name: &str,
    timestamp_column: &str,
    value_column: &str,
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
) -> tinyfs::Result<Vec<Sample>> {
    let timestamp = quote_identifier(timestamp_column);
    let value = quote_identifier(value_column);
    let table = quote_identifier(table_name);
    let sql = format!(
        "SELECT {timestamp}, {value} FROM {table} \
         WHERE {timestamp} >= to_timestamp_micros({}) \
         AND {timestamp} < to_timestamp_micros({}) \
         AND {value} IS NOT NULL ORDER BY {timestamp}",
        period_start.timestamp_micros(),
        period_end.timestamp_micros()
    );
    let batches = session
        .sql(&sql)
        .await
        .map_err(|error| tinyfs::Error::Other(format!("monitor query failed: {error}")))?
        .collect()
        .await
        .map_err(|error| tinyfs::Error::Other(format!("monitor query failed: {error}")))?;
    decode_samples(&batches)
}

async fn query_rate_samples(
    session: &datafusion::prelude::SessionContext,
    table_name: &str,
    timestamp_column: &str,
    value_column: &str,
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
) -> tinyfs::Result<Vec<RateSample>> {
    let timestamp = quote_identifier(timestamp_column);
    let value = quote_identifier(value_column);
    let table = quote_identifier(table_name);
    let sql = format!(
        "SELECT {timestamp}, CAST({value} AS DOUBLE), \
         counter_delta(CAST({value} AS DOUBLE)) OVER (ORDER BY {timestamp}) \
         FROM {table} WHERE {timestamp} >= to_timestamp_micros({}) \
         AND {timestamp} < to_timestamp_micros({}) \
         AND {value} IS NOT NULL ORDER BY {timestamp}",
        period_start.timestamp_micros(),
        period_end.timestamp_micros()
    );
    let batches = session
        .sql(&sql)
        .await
        .map_err(|error| tinyfs::Error::Other(format!("monitor rate query failed: {error}")))?
        .collect()
        .await
        .map_err(|error| tinyfs::Error::Other(format!("monitor rate query failed: {error}")))?;
    decode_rate_samples(&batches)
}

async fn query_condition_samples(
    session: &datafusion::prelude::SessionContext,
    table_name: &str,
    timestamp_column: &str,
    value_column: &str,
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
) -> tinyfs::Result<Vec<ConditionSample>> {
    let timestamp = quote_identifier(timestamp_column);
    let value = quote_identifier(value_column);
    let table = quote_identifier(table_name);
    let sql = format!(
        "SELECT {timestamp}, {value} FROM {table} \
         WHERE {timestamp} >= to_timestamp_micros({}) \
         AND {timestamp} < to_timestamp_micros({}) \
         AND {value} IS NOT NULL ORDER BY {timestamp}",
        period_start.timestamp_micros(),
        period_end.timestamp_micros()
    );
    let batches = session
        .sql(&sql)
        .await
        .map_err(|error| tinyfs::Error::Other(format!("monitor condition query failed: {error}")))?
        .collect()
        .await
        .map_err(|error| {
            tinyfs::Error::Other(format!("monitor condition query failed: {error}"))
        })?;
    decode_condition_samples(&batches)
}

fn decode_samples(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> tinyfs::Result<Vec<Sample>> {
    let mut samples = Vec::new();
    for batch in batches {
        let timestamps = cast(
            batch.column(0),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .map_err(|error| tinyfs::Error::Other(format!("invalid monitor timestamps: {error}")))?;
        let timestamps = timestamps
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| tinyfs::Error::Other("invalid monitor timestamp type".to_string()))?;
        let values = cast(batch.column(1), &DataType::Float64)
            .map_err(|error| tinyfs::Error::Other(format!("invalid monitor values: {error}")))?;
        let values = values
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| tinyfs::Error::Other("invalid monitor value type".to_string()))?;

        for index in 0..batch.num_rows() {
            if timestamps.is_null(index) || values.is_null(index) {
                continue;
            }
            let timestamp =
                DateTime::from_timestamp_micros(timestamps.value(index)).ok_or_else(|| {
                    tinyfs::Error::Other("monitor timestamp is out of range".to_string())
                })?;
            let value = values.value(index);
            if !value.is_finite() {
                return Err(tinyfs::Error::Other(format!(
                    "monitor source contains a non-finite value at {}",
                    format_timestamp(timestamp)
                )));
            }
            samples.push(Sample { timestamp, value });
        }
    }
    Ok(samples)
}

fn decode_rate_samples(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> tinyfs::Result<Vec<RateSample>> {
    let mut samples = Vec::new();
    for batch in batches {
        let timestamps = cast(
            batch.column(0),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .map_err(|error| tinyfs::Error::Other(format!("invalid monitor timestamps: {error}")))?;
        let timestamps = timestamps
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| tinyfs::Error::Other("invalid monitor timestamp type".to_string()))?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| tinyfs::Error::Other("invalid monitor value type".to_string()))?;
        let deltas = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| tinyfs::Error::Other("invalid monitor delta type".to_string()))?;

        for index in 0..batch.num_rows() {
            if timestamps.is_null(index) || values.is_null(index) {
                continue;
            }
            let timestamp =
                DateTime::from_timestamp_micros(timestamps.value(index)).ok_or_else(|| {
                    tinyfs::Error::Other("monitor timestamp is out of range".to_string())
                })?;
            let value = values.value(index);
            let positive_delta = (!deltas.is_null(index)).then(|| deltas.value(index));
            if !value.is_finite()
                || positive_delta.is_some_and(|positive_delta| !positive_delta.is_finite())
            {
                return Err(tinyfs::Error::Other(format!(
                    "monitor source contains a non-finite value at {}",
                    format_timestamp(timestamp)
                )));
            }
            samples.push(RateSample {
                timestamp,
                value,
                positive_delta,
            });
        }
    }
    Ok(samples)
}

fn decode_condition_samples(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> tinyfs::Result<Vec<ConditionSample>> {
    let mut samples = Vec::new();
    for batch in batches {
        let timestamps = cast(
            batch.column(0),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .map_err(|error| tinyfs::Error::Other(format!("invalid monitor timestamps: {error}")))?;
        let timestamps = timestamps
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| tinyfs::Error::Other("invalid monitor timestamp type".to_string()))?;
        let values = cast(batch.column(1), &DataType::Utf8).map_err(|error| {
            tinyfs::Error::Other(format!("invalid monitor conditions: {error}"))
        })?;
        let values = values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| tinyfs::Error::Other("invalid monitor condition type".to_string()))?;

        for index in 0..batch.num_rows() {
            if timestamps.is_null(index) || values.is_null(index) {
                continue;
            }
            let timestamp =
                DateTime::from_timestamp_micros(timestamps.value(index)).ok_or_else(|| {
                    tinyfs::Error::Other("monitor timestamp is out of range".to_string())
                })?;
            samples.push(ConditionSample {
                timestamp,
                value: values.value(index).to_string(),
            });
        }
    }
    Ok(samples)
}

fn classify_all_observed_below(
    config: &BelowCheckConfig,
    window_seconds: u64,
    samples: &[Sample],
) -> CheckStatus {
    let state = if samples.is_empty() {
        CheckState::Unknown
    } else if samples.iter().all(|sample| sample.value < config.threshold) {
        CheckState::Alarm
    } else {
        CheckState::Healthy
    };
    let minimum = samples.iter().map(|sample| sample.value).reduce(f64::min);
    let maximum = samples.iter().map(|sample| sample.value).reduce(f64::max);
    CheckStatus {
        id: config.id.clone(),
        label: config.label.clone(),
        description: config.description.clone(),
        href: config.href.clone(),
        state,
        tone: None,
        rule: "all-observed-below",
        source: config.source.clone(),
        value_column: config.value_column.clone(),
        unit: config.unit.clone(),
        threshold: Some(config.threshold),
        lower_bound: None,
        upper_bound: None,
        window_seconds,
        sample_count: samples.len(),
        observed_start: samples
            .first()
            .map(|sample| format_timestamp(sample.timestamp)),
        observed_end: samples
            .last()
            .map(|sample| format_timestamp(sample.timestamp)),
        latest_value: samples.last().map(|sample| sample.value),
        minimum,
        maximum,
        average: None,
        condition_source: None,
        active_seconds: None,
        minimum_active_seconds: None,
        accumulated_change: None,
        rate: None,
        aligned_interval_count: None,
        unaligned_interval_count: None,
    }
}

fn classify_any_observed_below(
    config: &AnyObservedBelowCheckConfig,
    window_seconds: u64,
    samples: &[Sample],
) -> CheckStatus {
    let state = if samples.is_empty() {
        CheckState::Unknown
    } else if samples.iter().any(|sample| sample.value < config.threshold) {
        CheckState::Alarm
    } else {
        CheckState::Healthy
    };
    let minimum = samples.iter().map(|sample| sample.value).reduce(f64::min);
    let maximum = samples.iter().map(|sample| sample.value).reduce(f64::max);
    CheckStatus {
        id: config.id.clone(),
        label: config.label.clone(),
        description: config.description.clone(),
        href: config.href.clone(),
        state,
        tone: None,
        rule: "any-observed-below",
        source: config.source.clone(),
        value_column: config.value_column.clone(),
        unit: config.unit.clone(),
        threshold: Some(config.threshold),
        lower_bound: None,
        upper_bound: None,
        window_seconds,
        sample_count: samples.len(),
        observed_start: samples
            .first()
            .map(|sample| format_timestamp(sample.timestamp)),
        observed_end: samples
            .last()
            .map(|sample| format_timestamp(sample.timestamp)),
        latest_value: samples.last().map(|sample| sample.value),
        minimum,
        maximum,
        average: None,
        condition_source: None,
        active_seconds: None,
        minimum_active_seconds: None,
        accumulated_change: None,
        rate: None,
        aligned_interval_count: None,
        unaligned_interval_count: None,
    }
}

fn classify_stale_or_below(
    config: &StaleOrBelowCheckConfig,
    window_seconds: u64,
    samples: &[Sample],
) -> CheckStatus {
    // Combines staleness and a low-value threshold into one check: absence
    // of samples in the window is itself an alarm (same reasoning as
    // stale-after), and so is any observed value below the threshold.
    let state = if samples.is_empty()
        || samples.iter().any(|sample| sample.value < config.threshold)
    {
        CheckState::Alarm
    } else {
        CheckState::Healthy
    };
    let minimum = samples.iter().map(|sample| sample.value).reduce(f64::min);
    let maximum = samples.iter().map(|sample| sample.value).reduce(f64::max);
    CheckStatus {
        id: config.id.clone(),
        label: config.label.clone(),
        description: config.description.clone(),
        href: config.href.clone(),
        state,
        tone: None,
        rule: "stale-or-below",
        source: config.source.clone(),
        value_column: config.value_column.clone(),
        unit: config.unit.clone(),
        threshold: Some(config.threshold),
        lower_bound: None,
        upper_bound: None,
        window_seconds,
        sample_count: samples.len(),
        observed_start: samples
            .first()
            .map(|sample| format_timestamp(sample.timestamp)),
        observed_end: samples
            .last()
            .map(|sample| format_timestamp(sample.timestamp)),
        latest_value: samples.last().map(|sample| sample.value),
        minimum,
        maximum,
        average: None,
        condition_source: None,
        active_seconds: None,
        minimum_active_seconds: None,
        accumulated_change: None,
        rate: None,
        aligned_interval_count: None,
        unaligned_interval_count: None,
    }
}

fn classify_stale_after(
    config: &StaleAfterCheckConfig,
    window_seconds: u64,
    samples: &[Sample],
) -> CheckStatus {
    // Absence of samples is itself the alarm condition for a staleness
    // check: unlike the other scalar checks, "no data in the window" means
    // the source has stopped producing new data, not that nothing can be
    // concluded.
    let state = if samples.is_empty() {
        CheckState::Alarm
    } else {
        CheckState::Healthy
    };
    let minimum = samples.iter().map(|sample| sample.value).reduce(f64::min);
    let maximum = samples.iter().map(|sample| sample.value).reduce(f64::max);
    CheckStatus {
        id: config.id.clone(),
        label: config.label.clone(),
        description: config.description.clone(),
        href: config.href.clone(),
        state,
        tone: None,
        rule: "stale-after",
        source: config.source.clone(),
        value_column: config.value_column.clone(),
        unit: config.unit.clone(),
        threshold: None,
        lower_bound: None,
        upper_bound: None,
        window_seconds,
        sample_count: samples.len(),
        observed_start: samples
            .first()
            .map(|sample| format_timestamp(sample.timestamp)),
        observed_end: samples
            .last()
            .map(|sample| format_timestamp(sample.timestamp)),
        latest_value: samples.last().map(|sample| sample.value),
        minimum,
        maximum,
        average: None,
        condition_source: None,
        active_seconds: None,
        minimum_active_seconds: None,
        accumulated_change: None,
        rate: None,
        aligned_interval_count: None,
        unaligned_interval_count: None,
    }
}

fn classify_graded_score(
    config: &GradedScoreCheckConfig,
    window_seconds: u64,
    samples: &[Sample],
) -> CheckStatus {
    let latest_value = samples.last().map(|sample| sample.value);
    // A missing score is the worst band, not "unknown": a quiet leak-score
    // source should not be able to hide a real leak behind a neutral card.
    let tone = match latest_value {
        None => Tone::Alarm,
        Some(value) if value <= config.good_max => Tone::Good,
        Some(value) if value <= config.watch_max => Tone::Watch,
        Some(value) if value <= config.concern_max => Tone::Concern,
        Some(_) => Tone::Alarm,
    };
    let state = match tone {
        Tone::Good | Tone::Watch => CheckState::Healthy,
        Tone::Concern | Tone::Alarm => CheckState::Alarm,
    };
    let minimum = samples.iter().map(|sample| sample.value).reduce(f64::min);
    let maximum = samples.iter().map(|sample| sample.value).reduce(f64::max);
    CheckStatus {
        id: config.id.clone(),
        label: config.label.clone(),
        description: config.description.clone(),
        href: config.href.clone(),
        state,
        tone: Some(tone),
        rule: "graded-score",
        source: config.source.clone(),
        value_column: config.value_column.clone(),
        unit: config.unit.clone(),
        threshold: None,
        lower_bound: None,
        upper_bound: None,
        window_seconds,
        sample_count: samples.len(),
        observed_start: samples
            .first()
            .map(|sample| format_timestamp(sample.timestamp)),
        observed_end: samples
            .last()
            .map(|sample| format_timestamp(sample.timestamp)),
        latest_value,
        minimum,
        maximum,
        average: None,
        condition_source: None,
        active_seconds: None,
        minimum_active_seconds: None,
        accumulated_change: None,
        rate: None,
        aligned_interval_count: None,
        unaligned_interval_count: None,
    }
}

fn classify_average_range(
    config: &AverageRangeCheckConfig,
    window_seconds: u64,
    samples: &[Sample],
) -> tinyfs::Result<CheckStatus> {
    let average = if samples.is_empty() {
        None
    } else {
        let average = samples
            .iter()
            .enumerate()
            .fold(0.0, |average, (index, sample)| {
                average + (sample.value - average) / (index + 1) as f64
            });
        if !average.is_finite() {
            return Err(tinyfs::Error::Other(format!(
                "monitor check '{}' produced a non-finite average",
                config.id
            )));
        }
        Some(average)
    };
    let state = match average {
        None => CheckState::Unknown,
        Some(average) if average < config.lower_bound || average > config.upper_bound => {
            CheckState::Alarm
        }
        Some(_) => CheckState::Healthy,
    };
    let minimum = samples.iter().map(|sample| sample.value).reduce(f64::min);
    let maximum = samples.iter().map(|sample| sample.value).reduce(f64::max);
    Ok(CheckStatus {
        id: config.id.clone(),
        label: config.label.clone(),
        description: config.description.clone(),
        href: config.href.clone(),
        state,
        tone: None,
        rule: "average-range",
        source: config.source.clone(),
        value_column: config.value_column.clone(),
        unit: config.unit.clone(),
        threshold: None,
        lower_bound: Some(config.lower_bound),
        upper_bound: Some(config.upper_bound),
        window_seconds,
        sample_count: samples.len(),
        observed_start: samples
            .first()
            .map(|sample| format_timestamp(sample.timestamp)),
        observed_end: samples
            .last()
            .map(|sample| format_timestamp(sample.timestamp)),
        latest_value: samples.last().map(|sample| sample.value),
        minimum,
        maximum,
        average,
        condition_source: None,
        active_seconds: None,
        minimum_active_seconds: None,
        accumulated_change: None,
        rate: None,
        aligned_interval_count: None,
        unaligned_interval_count: None,
    })
}

fn classify_rate_while(
    config: &RateWhileCheckConfig,
    window_seconds: u64,
    measurements: &[RateSample],
    conditions: &[ConditionSample],
) -> tinyfs::Result<CheckStatus> {
    let tolerance = humantime::parse_duration(&config.alignment.tolerance).map_err(|error| {
        tinyfs::Error::Other(format!(
            "invalid monitor alignment tolerance '{}': {error}",
            config.alignment.tolerance
        ))
    })?;
    let minimum_active_time =
        humantime::parse_duration(&config.minimum_active_time).map_err(|error| {
            tinyfs::Error::Other(format!(
                "invalid monitor minimum_active_time '{}': {error}",
                config.minimum_active_time
            ))
        })?;
    let tolerance_micros = i64::try_from(tolerance.as_micros()).map_err(|_| {
        tinyfs::Error::Other("monitor alignment tolerance is too large".to_string())
    })?;

    let mut condition_index = 0;
    let mut active_seconds = 0.0;
    let mut accumulated_change = 0.0;
    let mut aligned_interval_count = 0;
    let mut unaligned_interval_count = 0;
    for interval in measurements.windows(2) {
        let start = interval[0].timestamp;
        let end = interval[1].timestamp;
        let elapsed_micros = end.timestamp_micros() - start.timestamp_micros();
        if elapsed_micros <= 0 {
            return Err(tinyfs::Error::Other(format!(
                "monitor check '{}' has duplicate or decreasing measurement timestamps at {}",
                config.id,
                format_timestamp(end)
            )));
        }

        while condition_index + 1 < conditions.len()
            && conditions[condition_index + 1].timestamp <= start
        {
            condition_index += 1;
        }
        let Some(condition) = conditions
            .get(condition_index)
            .filter(|condition| condition.timestamp <= start)
        else {
            unaligned_interval_count += 1;
            continue;
        };
        if start.timestamp_micros() - condition.timestamp.timestamp_micros() > tolerance_micros
            || elapsed_micros > tolerance_micros
        {
            unaligned_interval_count += 1;
            continue;
        }
        aligned_interval_count += 1;
        let predicate_matches = match config.condition.predicate.operator {
            PredicateOperator::Eq => condition.value == config.condition.predicate.value,
        };
        if !predicate_matches {
            continue;
        }

        let positive_delta = interval[1].positive_delta.ok_or_else(|| {
            tinyfs::Error::Other(format!(
                "monitor check '{}' is missing a counter delta at {}",
                config.id,
                format_timestamp(end)
            ))
        })?;
        active_seconds += elapsed_micros as f64 / 1_000_000.0;
        accumulated_change += positive_delta;
    }

    let rate = (active_seconds > 0.0).then(|| 3600.0 * accumulated_change / active_seconds);
    let state = if active_seconds < minimum_active_time.as_secs_f64() {
        CheckState::Unknown
    } else {
        match config.alarm.operator {
            AlarmOperator::Lt if rate.is_some_and(|rate| rate < config.alarm.value) => {
                CheckState::Alarm
            }
            AlarmOperator::Lt => CheckState::Healthy,
        }
    };
    let minimum = measurements
        .iter()
        .map(|sample| sample.value)
        .reduce(f64::min);
    let maximum = measurements
        .iter()
        .map(|sample| sample.value)
        .reduce(f64::max);

    Ok(CheckStatus {
        id: config.id.clone(),
        label: config.label.clone(),
        description: config.description.clone(),
        href: config.href.clone(),
        state,
        tone: None,
        rule: "rate-while",
        source: config.measurement.source.clone(),
        value_column: config.measurement.column.clone(),
        unit: config.alarm.unit.clone(),
        threshold: Some(config.alarm.value),
        lower_bound: None,
        upper_bound: None,
        window_seconds,
        sample_count: measurements.len(),
        observed_start: measurements
            .first()
            .map(|sample| format_timestamp(sample.timestamp)),
        observed_end: measurements
            .last()
            .map(|sample| format_timestamp(sample.timestamp)),
        latest_value: measurements.last().map(|sample| sample.value),
        minimum,
        maximum,
        average: None,
        condition_source: Some(config.condition.source.clone()),
        active_seconds: Some(active_seconds),
        minimum_active_seconds: Some(minimum_active_time.as_secs()),
        accumulated_change: Some(accumulated_change),
        rate,
        aligned_interval_count: Some(aligned_interval_count),
        unaligned_interval_count: Some(unaligned_interval_count),
    })
}

fn overall_state(checks: &[CheckStatus]) -> CheckState {
    if checks.is_empty() {
        CheckState::Unknown
    } else if checks.iter().any(|check| check.state == CheckState::Alarm) {
        CheckState::Alarm
    } else if checks
        .iter()
        .any(|check| check.state == CheckState::Unknown)
    {
        CheckState::Unknown
    } else {
        CheckState::Healthy
    }
}

fn publish(status: &MonitorStatus, output_dir: &Path) -> tinyfs::Result<()> {
    std::fs::create_dir_all(output_dir).map_err(|error| {
        tinyfs::Error::Other(format!(
            "failed to create monitor output directory '{}': {error}",
            output_dir.display()
        ))
    })?;
    let mut json = serde_json::to_vec_pretty(status)
        .map_other_context("Failed to serialize monitor status")?;
    json.push(b'\n');
    write_atomic(output_dir, "status.json", &json)?;
    let legacy_html = output_dir.join("index.html");
    if let Err(error) = std::fs::remove_file(&legacy_html)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        return Err(tinyfs::Error::Other(format!(
            "failed to remove legacy monitor artifact '{}': {error}",
            legacy_html.display()
        )));
    }
    File::open(output_dir)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            tinyfs::Error::Other(format!(
                "failed to sync monitor output directory '{}': {error}",
                output_dir.display()
            ))
        })
}

fn write_atomic(output_dir: &Path, filename: &str, bytes: &[u8]) -> tinyfs::Result<()> {
    let id = TEMP_FILE_ID.fetch_add(1, Ordering::Relaxed);
    let temporary = output_dir.join(format!(".{filename}.{}.{}.tmp", std::process::id(), id));
    let destination = output_dir.join(filename);
    let result = (|| -> std::io::Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, &destination)
    })();
    if let Err(error) = result {
        _ = std::fs::remove_file(&temporary);
        return Err(tinyfs::Error::Other(format!(
            "failed to publish monitor artifact '{}': {error}",
            destination.display()
        )));
    }
    Ok(())
}

fn format_timestamp(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

register_executable_factory!(
    name: "monitor-report",
    description: "Evaluate committed pond measurements and publish an atomic host status report",
    post_commit: read_only,
    validate: validate_config,
    initialize: initialize,
    execute: execute
);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, TimestampMicrosecondArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use tinyfs::arrow::ParquetExt;

    fn config() -> BelowCheckConfig {
        BelowCheckConfig {
            id: "well-depth-low".to_string(),
            label: "Well depth above 40".to_string(),
            description:
                "At least one well-depth reading must reach 40 m in the trailing three hours."
                    .to_string(),
            href: "/data/well-depth.html".to_string(),
            source: "series:///well-depth".to_string(),
            timestamp_column: "timestamp".to_string(),
            value_column: "well_depth_value".to_string(),
            unit: "m".to_string(),
            threshold: 40.0,
            window: "3h".to_string(),
        }
    }

    fn sample(seconds: i64, value: f64) -> Sample {
        Sample {
            timestamp: DateTime::from_timestamp(seconds, 0).expect("valid timestamp"),
            value,
        }
    }

    fn rate_config() -> RateWhileCheckConfig {
        RateWhileCheckConfig {
            id: "chlorine-feed-response".to_string(),
            label: "Chlorine feed responds while well pump runs".to_string(),
            description: "Chlorine level must increase while the well pump is running.".to_string(),
            href: "/data/chlorine-level.html".to_string(),
            check_type: RateWhileType::RateWhile,
            measurement: MeasurementConfig {
                source: "series:///chlorine-*".to_string(),
                timestamp: "timestamp".to_string(),
                column: "chlorine_level".to_string(),
                accumulation: Accumulation::PositiveDeltas,
            },
            condition: ConditionConfig {
                source: "series:///pump-state".to_string(),
                timestamp: "timestamp".to_string(),
                predicate: PredicateConfig {
                    column: "phase".to_string(),
                    operator: PredicateOperator::Eq,
                    value: "pumping".to_string(),
                },
            },
            alignment: AlignmentConfig {
                method: AlignmentMethod::Previous,
                tolerance: "2m".to_string(),
            },
            window: "24h".to_string(),
            minimum_active_time: "3m".to_string(),
            alarm: AlarmConfig {
                operator: AlarmOperator::Lt,
                value: 40.0,
                unit: "sensor-units-per-pump-hour".to_string(),
            },
        }
    }

    fn any_below_config() -> AnyObservedBelowCheckConfig {
        AnyObservedBelowCheckConfig {
            id: "tank-level-low".to_string(),
            label: "Tank level stays above 3 m".to_string(),
            description: "Every tank-level reading must remain at or above 3 m.".to_string(),
            href: "/data/tank-level.html".to_string(),
            check_type: AnyObservedBelowType::AnyObservedBelow,
            source: "series:///tank-level".to_string(),
            timestamp_column: "timestamp".to_string(),
            value_column: "tank_level_value".to_string(),
            unit: "m".to_string(),
            threshold: 3.0,
            window: "3h".to_string(),
        }
    }

    fn average_range_config() -> AverageRangeCheckConfig {
        AverageRangeCheckConfig {
            id: "system-pressure-range".to_string(),
            label: "System pressure stays within its operating range".to_string(),
            description: "The one-hour average must remain between 1.2 and 2.2.".to_string(),
            href: "/data/system-pressure.html".to_string(),
            check_type: AverageRangeType::AverageRange,
            source: "series:///system-pressure".to_string(),
            timestamp_column: "timestamp".to_string(),
            value_column: "system_pressure_value".to_string(),
            unit: "bar".to_string(),
            lower_bound: 1.2,
            upper_bound: 2.2,
            window: "1h".to_string(),
        }
    }

    fn stale_after_config() -> StaleAfterCheckConfig {
        StaleAfterCheckConfig {
            id: "septic-data-stale".to_string(),
            label: "Septic station is reporting data".to_string(),
            description: "At least one reading must arrive in the trailing 24 hours.".to_string(),
            href: "/septic/pumps.html".to_string(),
            check_type: StaleAfterType::StaleAfter,
            source: "oteljson:///ingest/septicstation*.json".to_string(),
            timestamp_column: "timestamp".to_string(),
            value_column: "orenco_RT_Pump1".to_string(),
            unit: String::new(),
            window: "24h".to_string(),
        }
    }

    fn rate_sample(seconds: i64, value: f64, positive_delta: Option<f64>) -> RateSample {
        RateSample {
            timestamp: DateTime::from_timestamp(seconds, 0).expect("valid timestamp"),
            value,
            positive_delta,
        }
    }

    fn condition_sample(seconds: i64, value: &str) -> ConditionSample {
        ConditionSample {
            timestamp: DateTime::from_timestamp(seconds, 0).expect("valid timestamp"),
            value: value.to_string(),
        }
    }

    #[test]
    fn all_observed_below_requires_at_least_one_sample() {
        let check = config();
        assert_eq!(
            classify_all_observed_below(&check, 10_800, &[]).state,
            CheckState::Unknown
        );
        assert_eq!(
            classify_all_observed_below(&check, 10_800, &[sample(1, 39.9), sample(10_000, 20.0)])
                .state,
            CheckState::Alarm
        );
        assert_eq!(
            classify_all_observed_below(&check, 10_800, &[sample(1, 39.9), sample(2, 40.0)]).state,
            CheckState::Healthy
        );
    }

    #[test]
    fn any_observed_below_alarms_on_one_low_sample() {
        let check = any_below_config();
        assert_eq!(
            classify_any_observed_below(&check, 10_800, &[]).state,
            CheckState::Unknown
        );
        assert_eq!(
            classify_any_observed_below(
                &check,
                10_800,
                &[sample(1, 3.1), sample(2, 2.99), sample(3, 3.2)]
            )
            .state,
            CheckState::Alarm
        );
        assert_eq!(
            classify_any_observed_below(&check, 10_800, &[sample(1, 3.0), sample(2, 3.2)]).state,
            CheckState::Healthy
        );
    }

    #[test]
    fn stale_after_alarms_when_no_samples_observed() {
        let check = stale_after_config();
        let status = classify_stale_after(&check, 86_400, &[]);
        assert_eq!(status.state, CheckState::Alarm);
        assert_eq!(status.rule, "stale-after");
        assert_eq!(status.sample_count, 0);
    }

    #[test]
    fn stale_after_is_healthy_with_any_recent_sample() {
        let check = stale_after_config();
        let status = classify_stale_after(&check, 86_400, &[sample(1, 0.0)]);
        assert_eq!(status.state, CheckState::Healthy);
        assert_eq!(status.sample_count, 1);
    }

    fn graded_score_config() -> GradedScoreCheckConfig {
        GradedScoreCheckConfig {
            id: "leak-score".to_string(),
            label: "Leak risk".to_string(),
            description: "7-day trailing leak-probability score from overnight drawdown."
                .to_string(),
            href: "/leak".to_string(),
            check_type: GradedScoreType::GradedScore,
            source: "series:///leak/leak-analysis".to_string(),
            timestamp_column: "timestamp".to_string(),
            value_column: "leak_score".to_string(),
            unit: String::new(),
            good_max: 0.25,
            watch_max: 0.5,
            concern_max: 0.75,
            window: "3d".to_string(),
        }
    }

    #[test]
    fn graded_score_bands_the_latest_sample() {
        let check = graded_score_config();
        assert_eq!(
            classify_graded_score(&check, 259_200, &[sample(1, 0.1)]).tone,
            Some(Tone::Good)
        );
        assert_eq!(
            classify_graded_score(&check, 259_200, &[sample(1, 0.4)]).tone,
            Some(Tone::Watch)
        );
        assert_eq!(
            classify_graded_score(&check, 259_200, &[sample(1, 0.6)]).tone,
            Some(Tone::Concern)
        );
        assert_eq!(
            classify_graded_score(&check, 259_200, &[sample(1, 0.9)]).tone,
            Some(Tone::Alarm)
        );
    }

    #[test]
    fn graded_score_treats_missing_data_as_the_worst_band() {
        let check = graded_score_config();
        let status = classify_graded_score(&check, 259_200, &[]);
        assert_eq!(status.tone, Some(Tone::Alarm));
        assert_eq!(status.state, CheckState::Alarm);
        assert_eq!(status.sample_count, 0);
    }

    #[test]
    fn graded_score_state_rolls_up_good_and_watch_to_healthy() {
        let check = graded_score_config();
        assert_eq!(
            classify_graded_score(&check, 259_200, &[sample(1, 0.2)]).state,
            CheckState::Healthy
        );
        assert_eq!(
            classify_graded_score(&check, 259_200, &[sample(1, 0.4)]).state,
            CheckState::Healthy
        );
        assert_eq!(
            classify_graded_score(&check, 259_200, &[sample(1, 0.6)]).state,
            CheckState::Alarm
        );
    }

    fn stale_or_below_config() -> StaleOrBelowCheckConfig {
        StaleOrBelowCheckConfig {
            id: "ph-stale-or-low".to_string(),
            label: "pH stays above 6.5".to_string(),
            description: "Every pH reading must remain at or above 6.5; a 3-hour gap counts as a failure too."
                .to_string(),
            href: "/data/ph.html".to_string(),
            check_type: StaleOrBelowType::StaleOrBelow,
            source: "oteljson:///ingest/casparwater*.json".to_string(),
            timestamp_column: "timestamp".to_string(),
            value_column: "atlasph_ph".to_string(),
            unit: String::new(),
            threshold: 6.5,
            window: "3h".to_string(),
        }
    }

    #[test]
    fn stale_or_below_alarms_when_no_samples_observed() {
        let check = stale_or_below_config();
        let status = classify_stale_or_below(&check, 10_800, &[]);
        assert_eq!(status.state, CheckState::Alarm);
        assert_eq!(status.rule, "stale-or-below");
        assert_eq!(status.sample_count, 0);
    }

    #[test]
    fn stale_or_below_alarms_on_one_low_sample() {
        let check = stale_or_below_config();
        assert_eq!(
            classify_stale_or_below(&check, 10_800, &[sample(1, 7.0), sample(2, 6.1)]).state,
            CheckState::Alarm
        );
    }

    #[test]
    fn stale_or_below_is_healthy_with_recent_samples_above_threshold() {
        let check = stale_or_below_config();
        let status = classify_stale_or_below(&check, 10_800, &[sample(1, 6.9), sample(2, 7.1)]);
        assert_eq!(status.state, CheckState::Healthy);
        assert_eq!(status.sample_count, 2);
    }

    #[test]
    fn average_range_is_inclusive_and_requires_samples() {
        let check = average_range_config();
        assert_eq!(
            classify_average_range(&check, 3_600, &[])
                .expect("empty classification")
                .state,
            CheckState::Unknown
        );
        let lower = classify_average_range(&check, 3_600, &[sample(1, 1.0), sample(2, 1.4)])
            .expect("lower bound");
        assert_eq!(lower.state, CheckState::Healthy);
        assert_eq!(lower.average, Some(1.2));
        let upper = classify_average_range(&check, 3_600, &[sample(1, 2.0), sample(2, 2.4)])
            .expect("upper bound");
        assert_eq!(upper.state, CheckState::Healthy);
        assert_eq!(upper.average, Some(2.2));
        assert_eq!(
            classify_average_range(&check, 3_600, &[sample(1, 1.0), sample(2, 1.3)])
                .expect("below range")
                .state,
            CheckState::Alarm
        );
        assert_eq!(
            classify_average_range(&check, 3_600, &[sample(1, 2.2), sample(2, 2.3)])
                .expect("above range")
                .state,
            CheckState::Alarm
        );
    }

    #[test]
    fn average_range_rejects_reversed_bounds() {
        let mut check = average_range_config();
        check.lower_bound = 2.2;
        check.upper_bound = 1.2;

        let error = validate_average_range_check(&check).expect_err("reversed bounds");
        assert!(error.to_string().contains("lower_bound cannot exceed"));
    }

    #[test]
    fn rate_while_uses_elapsed_active_time_and_ignores_decreases() {
        let status = classify_rate_while(
            &rate_config(),
            86_400,
            &[
                rate_sample(0, 10.0, None),
                rate_sample(60, 11.0, Some(1.0)),
                rate_sample(120, 12.0, Some(1.0)),
                rate_sample(180, 5.0, Some(0.0)),
                rate_sample(240, 6.0, Some(1.0)),
            ],
            &[
                condition_sample(0, "pumping"),
                condition_sample(60, "pumping"),
                condition_sample(120, "pumping"),
                condition_sample(180, "pumping"),
            ],
        )
        .expect("classify rate");

        assert_eq!(status.state, CheckState::Healthy);
        assert_eq!(status.active_seconds, Some(240.0));
        assert_eq!(status.accumulated_change, Some(3.0));
        assert_eq!(status.rate, Some(45.0));
        assert_eq!(status.aligned_interval_count, Some(4));
        assert_eq!(status.unaligned_interval_count, Some(0));
    }

    #[test]
    fn rate_while_is_unknown_without_minimum_evidence() {
        let status = classify_rate_while(
            &rate_config(),
            86_400,
            &[rate_sample(0, 10.0, None), rate_sample(60, 11.0, Some(1.0))],
            &[condition_sample(0, "pumping")],
        )
        .expect("classify rate");

        assert_eq!(status.state, CheckState::Unknown);
        assert_eq!(status.active_seconds, Some(60.0));
        assert_eq!(status.rate, Some(60.0));
    }

    #[test]
    fn monitor_yaml_is_strictly_typed() {
        let yaml = br#"
pond: water-staging
title: Water status
output_dir: /monitor
checks:
  - id: tank-level-low
    label: Tank level stays above 3 m
    description: Every tank-level reading must remain at or above 3 m.
    href: /data/tank-level.html
    type: any-observed-below
    source: oteljson:///ingest/casparwater*.json
    timestamp_column: timestamp
    value_column: tank_level_value
    unit: m
    threshold: 3
    window: 3h
  - id: system-pressure-range
    label: System pressure stays within its operating range
    description: The one-hour average must remain between 1.2 and 2.2.
    href: /data/system-pressure.html
    type: average-range
    source: oteljson:///ingest/casparwater*.json
    timestamp_column: timestamp
    value_column: system_pressure_value
    unit: bar
    lower_bound: 1.2
    upper_bound: 2.2
    window: 1h
  - id: chlorine-feed-response
    label: Chlorine feed responds while well pump runs
    description: Chlorine level must increase while the well pump is running.
    href: /data/chlorine-level.html
    type: rate-while
    measurement:
      source: oteljson:///ingest/casparwater*.json
      timestamp: timestamp
      column: chlorine_level_value
      accumulation: positive-deltas
    condition:
      source: series:///pump-state/well-pump-state
      timestamp: timestamp
      predicate:
        column: phase
        operator: eq
        value: pumping
    alignment:
      method: previous
      tolerance: 2m
    window: 24h
    minimum_active_time: 30m
    alarm:
      operator: lt
      value: 2.5
      unit: sensor-units-per-pump-hour
  - id: septic-data-stale
    label: Septic station is reporting data
    description: At least one reading must arrive in the trailing 24 hours.
    href: /septic/pumps.html
    type: stale-after
    source: oteljson:///ingest/septicstation*.json
    timestamp_column: timestamp
    value_column: orenco_RT_Pump1
    window: 24h
  - id: leak-score
    label: Leak risk
    description: 7-day trailing leak-probability score from overnight drawdown.
    href: /leak
    type: graded-score
    source: series:///leak/leak-analysis
    timestamp_column: timestamp
    value_column: leak_score
    good_max: 0.25
    watch_max: 0.5
    concern_max: 0.75
    window: 3d
  - id: ph-stale-or-low
    label: pH stays above 6.5
    description: Every pH reading must remain at or above 6.5; a 3-hour gap counts as a failure too.
    href: /data/ph.html
    type: stale-or-below
    source: oteljson:///ingest/casparwater*.json
    timestamp_column: timestamp
    value_column: atlasph_ph
    threshold: 6.5
    window: 3h
"#;
        let config: MonitorConfig = serde_yaml::from_slice(yaml).expect("rate-while config");
        validate(&config).expect("valid config");
        assert!(matches!(config.checks[0], CheckConfig::AnyObservedBelow(_)));
        assert!(matches!(config.checks[1], CheckConfig::AverageRange(_)));
        assert!(matches!(config.checks[2], CheckConfig::RateWhile(_)));
        assert!(matches!(config.checks[3], CheckConfig::StaleAfter(_)));
        assert!(matches!(config.checks[4], CheckConfig::GradedScore(_)));
        assert!(matches!(config.checks[5], CheckConfig::StaleOrBelow(_)));
    }

    #[test]
    fn publish_writes_authoritative_json_and_removes_legacy_html() {
        let output = tempfile::tempdir().expect("tempdir");
        std::fs::write(output.path().join("index.html"), "legacy").expect("legacy html");
        let status = MonitorStatus {
            schema_version: 4,
            pond: "water-staging".to_string(),
            title: "Water status".to_string(),
            generated_at: "2026-09-01T00:00:00Z".to_string(),
            transaction_sequence: 42,
            state: CheckState::Alarm,
            checks: vec![classify_all_observed_below(
                &config(),
                10_800,
                &[sample(1, 39.0), sample(2, 38.0)],
            )],
        };

        publish(&status, output.path()).expect("publish");

        let json = std::fs::read_to_string(output.path().join("status.json")).expect("json");
        assert!(!output.path().join("index.html").exists());
        assert!(json.contains("\"transaction_sequence\": 42"));
        assert!(json.contains("\"state\": \"alarm\""));
        assert!(json.contains("\"href\": \"/data/well-depth.html\""));
    }

    #[test]
    fn no_checks_is_a_valid_unknown_status() {
        let config = MonitorConfig {
            pond: "site-staging".to_string(),
            title: "Site pond status".to_string(),
            output_dir: "/monitor".to_string(),
            checks: vec![],
        };

        validate(&config).expect("no-check reporting config");
        assert_eq!(overall_state(&[]), CheckState::Unknown);
    }

    #[tokio::test]
    async fn factory_queries_committed_view_and_publishes_alarm() {
        let output = tempfile::tempdir().expect("tempdir");
        let persistence = tinyfs::MemoryPersistence::default();
        let filesystem = tinyfs::FS::new(persistence.clone())
            .await
            .expect("filesystem");
        let session = Arc::new(SessionContext::new());
        provider::register_tinyfs_object_store(&session, persistence.clone())
            .expect("object store");
        let provider_context = tinyfs::ProviderContext::new(session, Arc::new(persistence.clone()));
        let root = filesystem.root().await.expect("root");
        let now = Utc::now();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new("well_depth_value", DataType::Float64, false),
            ])),
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![
                    (now - chrono::Duration::hours(2)).timestamp_micros(),
                    (now - chrono::Duration::minutes(1)).timestamp_micros(),
                ])),
                Arc::new(Float64Array::from(vec![39.5, 38.0])),
            ],
        )
        .expect("batch");
        root.create_series_from_batch("/well-depth", &batch, Some("timestamp"))
            .await
            .expect("series");

        let config = MonitorConfig {
            pond: "water-staging".to_string(),
            title: "Water status".to_string(),
            output_dir: output.path().to_string_lossy().into_owned(),
            checks: vec![CheckConfig::AllObservedBelow(config())],
        };
        execute(
            serde_json::to_value(config).expect("config"),
            FactoryContext::new(provider_context, tinyfs::FileID::root()).with_txn_seq(7),
            ExecutionContext::control_reader(vec!["push".to_string()]),
        )
        .await
        .expect("execute monitor");

        let json = std::fs::read_to_string(output.path().join("status.json")).expect("json");
        let value: serde_json::Value = serde_json::from_str(&json).expect("status json");
        assert_eq!(value["state"], "alarm");
        assert_eq!(value["transaction_sequence"], 7);
        assert_eq!(value["checks"][0]["sample_count"], 2);
        assert_eq!(value["checks"][0]["maximum"], 39.5);
    }

    #[tokio::test]
    async fn factory_evaluates_any_below_and_average_range() {
        let output = tempfile::tempdir().expect("tempdir");
        let persistence = tinyfs::MemoryPersistence::default();
        let filesystem = tinyfs::FS::new(persistence.clone())
            .await
            .expect("filesystem");
        let session = Arc::new(SessionContext::new());
        provider::register_tinyfs_object_store(&session, persistence.clone())
            .expect("object store");
        let provider_context = tinyfs::ProviderContext::new(session, Arc::new(persistence.clone()));
        let root = filesystem.root().await.expect("root");
        let now = Utc::now();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new("tank_level_value", DataType::Float64, false),
                Field::new("system_pressure_value", DataType::Float64, false),
            ])),
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![
                    (now - chrono::Duration::minutes(30)).timestamp_micros(),
                    (now - chrono::Duration::minutes(1)).timestamp_micros(),
                ])),
                Arc::new(Float64Array::from(vec![3.2, 2.9])),
                Arc::new(Float64Array::from(vec![1.0, 1.4])),
            ],
        )
        .expect("batch");
        root.create_series_from_batch("/measurements", &batch, Some("timestamp"))
            .await
            .expect("series");

        let mut any_below = any_below_config();
        any_below.source = "series:///measurements".to_string();
        let mut average_range = average_range_config();
        average_range.source = "series:///measurements".to_string();
        let config = MonitorConfig {
            pond: "water-staging".to_string(),
            title: "Water status".to_string(),
            output_dir: output.path().to_string_lossy().into_owned(),
            checks: vec![
                CheckConfig::AnyObservedBelow(any_below),
                CheckConfig::AverageRange(average_range),
            ],
        };
        execute(
            serde_json::to_value(config).expect("config"),
            FactoryContext::new(provider_context, tinyfs::FileID::root()).with_txn_seq(8),
            ExecutionContext::control_reader(vec!["push".to_string()]),
        )
        .await
        .expect("execute monitor");

        let json = std::fs::read_to_string(output.path().join("status.json")).expect("json");
        let value: serde_json::Value = serde_json::from_str(&json).expect("status json");
        assert_eq!(value["schema_version"], 4);
        assert_eq!(value["state"], "alarm");
        assert_eq!(value["checks"][0]["rule"], "any-observed-below");
        assert_eq!(value["checks"][0]["state"], "alarm");
        assert_eq!(value["checks"][0]["minimum"], 2.9);
        assert_eq!(value["checks"][1]["rule"], "average-range");
        assert_eq!(value["checks"][1]["state"], "healthy");
        assert_eq!(value["checks"][1]["average"], 1.2);
        assert_eq!(value["checks"][1]["lower_bound"], 1.2);
        assert_eq!(value["checks"][1]["upper_bound"], 2.2);
    }

    #[tokio::test]
    async fn factory_evaluates_rate_while_from_committed_series() {
        let output = tempfile::tempdir().expect("tempdir");
        let persistence = tinyfs::MemoryPersistence::default();
        let filesystem = tinyfs::FS::new(persistence.clone())
            .await
            .expect("filesystem");
        let session = Arc::new(SessionContext::new());
        provider::register_tinyfs_object_store(&session, persistence.clone())
            .expect("object store");
        let provider_context = tinyfs::ProviderContext::new(session, Arc::new(persistence.clone()));
        let root = filesystem.root().await.expect("root");
        let now = Utc::now();
        let timestamps = (0..5)
            .map(|minutes| (now - chrono::Duration::minutes(5 - minutes)).timestamp_micros())
            .collect::<Vec<_>>();
        let measurement_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new("chlorine_level", DataType::Float64, false),
            ])),
            vec![
                Arc::new(TimestampMicrosecondArray::from(timestamps.clone())),
                Arc::new(Float64Array::from(vec![10.0, 11.0, 12.0, 5.0, 6.0])),
            ],
        )
        .expect("measurement batch");
        root.create_series_from_batch(
            "/chlorine-0001",
            &measurement_batch.slice(0, 3),
            Some("timestamp"),
        )
        .await
        .expect("first measurement series");
        root.create_series_from_batch(
            "/chlorine-0002",
            &measurement_batch.slice(3, 2),
            Some("timestamp"),
        )
        .await
        .expect("second measurement series");
        let condition_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new("phase", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(TimestampMicrosecondArray::from(timestamps)),
                Arc::new(StringArray::from(vec![
                    "pumping", "pumping", "pumping", "pumping", "pumping",
                ])),
            ],
        )
        .expect("condition batch");
        root.create_series_from_batch("/pump-state", &condition_batch, Some("timestamp"))
            .await
            .expect("condition series");

        let config = MonitorConfig {
            pond: "water-staging".to_string(),
            title: "Water status".to_string(),
            output_dir: output.path().to_string_lossy().into_owned(),
            checks: vec![CheckConfig::RateWhile(rate_config())],
        };
        execute(
            serde_json::to_value(config).expect("config"),
            FactoryContext::new(provider_context, tinyfs::FileID::root()).with_txn_seq(9),
            ExecutionContext::control_reader(vec!["push".to_string()]),
        )
        .await
        .expect("execute monitor");

        let json = std::fs::read_to_string(output.path().join("status.json")).expect("json");
        let value: serde_json::Value = serde_json::from_str(&json).expect("status json");
        assert_eq!(value["state"], "healthy");
        assert_eq!(value["checks"][0]["active_seconds"], 240.0);
        assert_eq!(value["checks"][0]["accumulated_change"], 3.0);
        assert_eq!(value["checks"][0]["rate"], 45.0);
    }

    #[tokio::test]
    async fn factory_publishes_unknown_when_source_has_no_files() {
        let output = tempfile::tempdir().expect("tempdir");
        let persistence = tinyfs::MemoryPersistence::default();
        let filesystem = tinyfs::FS::new(persistence.clone())
            .await
            .expect("filesystem");
        let session = Arc::new(SessionContext::new());
        provider::register_tinyfs_object_store(&session, persistence.clone())
            .expect("object store");
        let provider_context = tinyfs::ProviderContext::new(session, Arc::new(persistence));
        let _root = filesystem.root().await.expect("root");
        let monitor_config = MonitorConfig {
            pond: "water-staging".to_string(),
            title: "Water status".to_string(),
            output_dir: output.path().to_string_lossy().into_owned(),
            checks: vec![CheckConfig::AllObservedBelow(config())],
        };

        execute(
            serde_json::to_value(monitor_config).expect("config"),
            FactoryContext::new(provider_context, tinyfs::FileID::root()).with_txn_seq(8),
            ExecutionContext::control_reader(vec!["push".to_string()]),
        )
        .await
        .expect("execute monitor");

        let json = std::fs::read_to_string(output.path().join("status.json")).expect("json");
        let value: serde_json::Value = serde_json::from_str(&json).expect("status json");
        assert_eq!(value["state"], "unknown");
        assert_eq!(value["checks"][0]["sample_count"], 0);
    }
}
