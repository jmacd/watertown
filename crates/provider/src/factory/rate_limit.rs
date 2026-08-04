// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Rate-limit factory: declares a consumption budget as a pond node.
//!
//! See `docs/rate-limiter-design.md`.  A `rate-limit` node is a **leaf config
//! node**: it has no content to compute and nothing to execute.  It exists so
//! that a budget is declared in the pond -- versioned and replicated like any
//! other config -- and can be referenced by path from whatever it governs.
//!
//! ```yaml
//! kind: mknod
//! metadata:
//!   path: /sys/limits/backup-bytes
//! spec:
//!   factory: rate-limit
//!   config:
//!     unit: MiB/day
//!     limit: 10
//!     burst: 1
//! ```
//!
//! The **running state is deliberately not here.**  A limiter's sliding window
//! lives in the per-replica control table, not in the pond, so limiter
//! bookkeeping never becomes pond history (Decision L1).  This module owns only
//! the policy: parsing it, validating it, and rendering it.  The enforcement
//! half is `steward::limiter`.
//!
//! # Unit grammar (Decision L4b)
//!
//! `unit` is `<scale>/<period>`, and it carries three things at once: the
//! **dimension** (bytes vs. operations), the **scale** (how big one unit is),
//! and the **period** (the window `limit` applies over).  `limit` and `burst`
//! are plain YAML numbers read in terms of it (Decision L4a) -- so they stay
//! typed and templatable, and `burst`'s relationship to `unit` is unambiguous:
//! **a burst shares the scale, never the period.**
//!
//! Byte scales are binary only.  An operator who writes `MB` gets an error
//! naming `MiB` rather than a silent factor-of-1.048576 surprise in a cost
//! control; see `docs/fallback-antipattern-philosophy.md`.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::time::Duration;
use tinyfs::FileHandle;
use tinyfs::Result as TinyFSResult;
use tinyfs::ResultExt;

// ============================================================================
// Configuration types
// ============================================================================

/// On-disk config for a `rate-limit` node.
///
/// The unit is one string; the magnitudes are numbers (Decision L4a).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimitConfig {
    /// `<scale>/<period>`, e.g. `MiB/day`, `iops/second`, `B/hour`.
    /// Carries dimension, scale, and period together.
    pub unit: String,

    /// How many `unit`s per period are permitted.  `unit: MiB/day` with
    /// `limit: 10` means 10 MiB per day.
    pub limit: f64,

    /// Optional instantaneous allowance, in the **scale** component of `unit`
    /// only -- the period does not apply to a burst.  `unit: MiB/day` with
    /// `burst: 1` permits 1 MiB to be spent faster than the smoothed rate.
    ///
    /// Defaults to one period's worth of `limit`, i.e. a pure sliding window
    /// with no extra allowance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub burst: Option<f64>,
}

/// The dimension a limiter governs.
///
/// This is the entire unit contract between a limiter and its callers
/// (Decision L10): a caller declares the dimension it spends and the bind is
/// checked against it.  Scale and period are deliberately **not** part of the
/// contract -- the caller owns the dimension (a property of the code), the
/// operator owns the scale and period (a property of policy).  Requiring
/// callers to match the full unit string would make every change of budget
/// granularity a code change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LimitUnit {
    /// Counted in bytes.
    Bytes,
    /// Counted in operations (requests, messages, ...).
    Ops,
}

impl LimitUnit {
    /// The name used as a `limits` map key and in error messages.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            LimitUnit::Bytes => "bytes",
            LimitUnit::Ops => "ops",
        }
    }

    /// Parse a `limits` map key.  Unknown keys are rejected rather than
    /// ignored, so a misspelled dimension is a configuration error and not a
    /// silently unenforced policy.
    pub fn parse(s: &str) -> Result<Self, RateParseError> {
        match s {
            "bytes" => Ok(LimitUnit::Bytes),
            "ops" => Ok(LimitUnit::Ops),
            other => Err(RateParseError::UnknownDimension(other.to_string())),
        }
    }
}

impl fmt::Display for LimitUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A fully resolved rate policy: config with the unit string parsed and the
/// magnitudes converted to whole base units (bytes, or operations).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateSpec {
    /// What is counted.
    pub unit: LimitUnit,
    /// `limit * scale`, in base units.
    pub amount: u64,
    /// The period `amount` applies over.
    pub window: Duration,
    /// `burst * scale`, in base units.  Defaults to `amount`.
    pub burst: u64,
    /// The `unit` string as written, retained for diagnostics and rendering.
    pub unit_text: String,
}

impl RateSpec {
    /// Human rendering, e.g. `10 MiB/day (burst 1 MiB)`.
    #[must_use]
    pub fn describe(&self) -> String {
        format!(
            "{} {} over {} (burst {} {})",
            self.amount,
            base_unit_name(self.unit),
            humanize_duration(self.window),
            self.burst,
            base_unit_name(self.unit),
        )
    }
}

fn base_unit_name(unit: LimitUnit) -> &'static str {
    match unit {
        LimitUnit::Bytes => "bytes",
        LimitUnit::Ops => "ops",
    }
}

fn humanize_duration(d: Duration) -> String {
    let secs = d.as_secs();
    match secs {
        86400 => "1 day".to_string(),
        3600 => "1 hour".to_string(),
        60 => "1 minute".to_string(),
        1 => "1 second".to_string(),
        n => format!("{n} seconds"),
    }
}

// ============================================================================
// Unit parsing
// ============================================================================

/// Errors from interpreting a `rate-limit` configuration.
#[derive(Debug, Clone, PartialEq)]
pub enum RateParseError {
    /// The unit string is not `<scale>/<period>`.
    Malformed(String),
    /// The scale component is not a recognized byte or operation scale.
    UnknownScale(String),
    /// A decimal SI byte scale was used where a binary one is required.
    DecimalByteScale { got: String, want: String },
    /// The period component is not a recognized period.
    UnknownPeriod(String),
    /// `limit` or `burst` is negative, non-finite, or rounds to zero.
    BadMagnitude { field: &'static str, value: f64 },
    /// A `limits` map key names no known dimension.
    UnknownDimension(String),
}

impl fmt::Display for RateParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RateParseError::Malformed(s) => write!(
                f,
                "unit `{s}` must be `<scale>/<period>`, e.g. `MiB/day` or `iops/second`"
            ),
            RateParseError::UnknownScale(s) => write!(
                f,
                "unknown scale `{s}`: expected a binary byte scale (B, KiB, MiB, GiB, TiB) \
                 or an operation scale (iops, ops, op)"
            ),
            RateParseError::DecimalByteScale { got, want } => write!(
                f,
                "decimal SI byte scale `{got}` is not accepted; use the binary scale `{want}` \
                 (a cost control must not silently differ by a factor of 1024/1000)"
            ),
            RateParseError::UnknownPeriod(s) => write!(
                f,
                "unknown period `{s}`: expected second, minute, hour, or day (or s, m, h, d)"
            ),
            RateParseError::BadMagnitude { field, value } => write!(
                f,
                "`{field}` value {value} is not a positive quantity of at least one base unit"
            ),
            RateParseError::UnknownDimension(s) => write!(
                f,
                "unknown limit dimension `{s}`: expected `bytes` or `ops`"
            ),
        }
    }
}

impl std::error::Error for RateParseError {}

/// One scale: how many base units it represents, and which dimension it is in.
fn parse_scale(s: &str) -> Result<(LimitUnit, u64), RateParseError> {
    // Binary byte scales.  Matched case-sensitively: `MiB` is the spelling,
    // and accepting `mib`/`MIB` would invite the `MB` confusion back in.
    let bytes = match s {
        "B" => Some(1_u64),
        "KiB" => Some(1024),
        "MiB" => Some(1024 * 1024),
        "GiB" => Some(1024 * 1024 * 1024),
        "TiB" => Some(1024 * 1024 * 1024 * 1024),
        _ => None,
    };
    if let Some(scale) = bytes {
        return Ok((LimitUnit::Bytes, scale));
    }

    // Decimal SI byte scales get a specific error naming the binary spelling,
    // rather than the generic "unknown scale".
    let decimal = match s {
        "kB" | "KB" => Some("KiB"),
        "MB" => Some("MiB"),
        "GB" => Some("GiB"),
        "TB" => Some("TiB"),
        _ => None,
    };
    if let Some(want) = decimal {
        return Err(RateParseError::DecimalByteScale {
            got: s.to_string(),
            want: want.to_string(),
        });
    }

    // Operation scales are all synonyms for a count of one.
    match s {
        "iops" | "ops" | "op" => Ok((LimitUnit::Ops, 1)),
        other => Err(RateParseError::UnknownScale(other.to_string())),
    }
}

/// One period.  No `week`/`month`: calendar arithmetic has no place here.
fn parse_period(s: &str) -> Result<Duration, RateParseError> {
    match s {
        "second" | "seconds" | "s" => Ok(Duration::from_secs(1)),
        "minute" | "minutes" | "m" => Ok(Duration::from_secs(60)),
        "hour" | "hours" | "h" => Ok(Duration::from_secs(3600)),
        "day" | "days" | "d" => Ok(Duration::from_secs(86400)),
        other => Err(RateParseError::UnknownPeriod(other.to_string())),
    }
}

/// Parse a `<scale>/<period>` unit string into its dimension, scale, and window.
pub fn parse_unit(unit: &str) -> Result<(LimitUnit, u64, Duration), RateParseError> {
    let mut parts = unit.split('/');
    let scale_text = parts.next().unwrap_or("").trim();
    let period_text = parts.next().unwrap_or("").trim();
    if parts.next().is_some() || scale_text.is_empty() || period_text.is_empty() {
        return Err(RateParseError::Malformed(unit.to_string()));
    }

    let (dimension, scale) = parse_scale(scale_text)?;
    let window = parse_period(period_text)?;
    Ok((dimension, scale, window))
}

/// Convert a magnitude expressed in `scale` units into whole base units.
///
/// Rounds **down**, so a policy never permits more than it says.  A value that
/// rounds to zero is rejected: a limiter that permits nothing is a
/// configuration mistake, not a policy.
fn resolve_magnitude(field: &'static str, value: f64, scale: u64) -> Result<u64, RateParseError> {
    if !value.is_finite() || value <= 0.0 {
        return Err(RateParseError::BadMagnitude { field, value });
    }
    let base = (value * scale as f64).floor();
    if base < 1.0 {
        return Err(RateParseError::BadMagnitude { field, value });
    }
    Ok(base as u64)
}

impl RateLimitConfig {
    /// Resolve this config into a [`RateSpec`].
    pub fn resolve(&self) -> Result<RateSpec, RateParseError> {
        let (unit, scale, window) = parse_unit(&self.unit)?;
        let amount = resolve_magnitude("limit", self.limit, scale)?;
        // A burst shares the scale but not the period.  Absent, it is one
        // period's worth -- a pure sliding window with no extra allowance.
        let burst = match self.burst {
            Some(b) => resolve_magnitude("burst", b, scale)?,
            None => amount,
        };
        Ok(RateSpec {
            unit,
            amount,
            window,
            burst,
            unit_text: self.unit.clone(),
        })
    }
}

// ============================================================================
// Factory
// ============================================================================

/// Render the effective policy.
///
/// The output is **canonical YAML that parses back into [`RateLimitConfig`]**,
/// with `burst` made explicit.  That matters: consumers bind a limiter by
/// reading this node's bytes (`steward::limiter`), so what `pond cat
/// /sys/limits/<name>` shows is exactly what is enforced -- there is no second
/// representation that could drift from the first.  The leading comment is
/// ordinary YAML and is ignored on the way back in.
fn render(spec: &RateSpec, cfg: &RateLimitConfig) -> Vec<u8> {
    let normalized = RateLimitConfig {
        unit: cfg.unit.clone(),
        limit: cfg.limit,
        burst: Some(cfg.burst.unwrap_or(cfg.limit)),
    };
    let body = serde_yaml::to_string(&normalized)
        .unwrap_or_else(|e| format!("# failed to render config: {e}\n"));
    format!(
        "# rate-limit: {} in dimension `{}`\n{}",
        spec.describe(),
        spec.unit,
        body
    )
    .into_bytes()
}

fn create_rate_limit_handle(
    config: Value,
    _context: crate::FactoryContext,
) -> TinyFSResult<FileHandle> {
    let cfg: RateLimitConfig =
        serde_json::from_value(config).map_other_context("Invalid rate-limit config")?;
    let spec = cfg
        .resolve()
        .map_err(|e| tinyfs::Error::Other(format!("Invalid rate-limit config: {e}")))?;
    Ok(crate::ConfigFile::new(render(&spec, &cfg)).create_handle())
}

fn validate_rate_limit_config(config: &[u8]) -> TinyFSResult<Value> {
    let config_str = std::str::from_utf8(config).map_other_context("Invalid UTF-8")?;
    let cfg: RateLimitConfig =
        serde_yaml::from_str(config_str).map_other_context("Invalid rate-limit config")?;

    // Resolve eagerly so a bad unit or magnitude is rejected at `pond apply`
    // time, not on the first governed action.
    let _spec = cfg
        .resolve()
        .map_err(|e| tinyfs::Error::Other(format!("Invalid rate-limit config: {e}")))?;

    serde_json::to_value(&cfg).map_other_context("Failed to serialize rate-limit config")
}

/// Parse a `rate-limit` node's stored config bytes into a [`RateSpec`].
///
/// Shared by the factory and by consumers that bind a limiter
/// (`steward::limiter`), so both agree on exactly one interpretation of a
/// node's policy.
pub fn spec_from_config_bytes(config: &[u8]) -> Result<RateSpec, String> {
    let text = std::str::from_utf8(config).map_err(|e| format!("invalid UTF-8: {e}"))?;
    let cfg: RateLimitConfig =
        serde_yaml::from_str(text).map_err(|e| format!("invalid rate-limit config: {e}"))?;
    cfg.resolve().map_err(|e| e.to_string())
}

crate::register_dynamic_factory!(
    name: "rate-limit",
    description: "Declare a consumption budget (e.g. 10 MiB/day) that governs actions in the pond",
    file: create_rate_limit_handle,
    validate: validate_rate_limit_config
);

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(unit: &str, limit: f64, burst: Option<f64>) -> RateLimitConfig {
        RateLimitConfig {
            unit: unit.to_string(),
            limit,
            burst,
        }
    }

    // -------- unit grammar --------

    #[test]
    fn parses_byte_units() {
        let (dim, scale, window) = parse_unit("MiB/day").unwrap();
        assert_eq!(dim, LimitUnit::Bytes);
        assert_eq!(scale, 1024 * 1024);
        assert_eq!(window, Duration::from_secs(86400));

        let (dim, scale, window) = parse_unit("B/hour").unwrap();
        assert_eq!(dim, LimitUnit::Bytes);
        assert_eq!(scale, 1);
        assert_eq!(window, Duration::from_secs(3600));

        let (_, scale, window) = parse_unit("GiB/d").unwrap();
        assert_eq!(scale, 1024 * 1024 * 1024);
        assert_eq!(window, Duration::from_secs(86400));
    }

    #[test]
    fn parses_operation_units() {
        for text in ["iops/second", "ops/second", "op/s"] {
            let (dim, scale, window) = parse_unit(text).unwrap();
            assert_eq!(dim, LimitUnit::Ops, "{text}");
            assert_eq!(scale, 1, "{text}");
            assert_eq!(window, Duration::from_secs(1), "{text}");
        }
    }

    #[test]
    fn tolerates_whitespace_around_separator() {
        assert_eq!(parse_unit("MiB / day").unwrap().1, 1024 * 1024);
    }

    #[test]
    fn rejects_decimal_si_byte_scales_by_name() {
        // The whole point: `MB` must not silently mean `MiB`, and the error
        // has to say which spelling to use.
        let err = parse_unit("MB/day").unwrap_err();
        assert_eq!(
            err,
            RateParseError::DecimalByteScale {
                got: "MB".to_string(),
                want: "MiB".to_string()
            }
        );
        assert!(err.to_string().contains("MiB"));
    }

    #[test]
    fn rejects_calendar_periods() {
        assert_eq!(
            parse_unit("MiB/week").unwrap_err(),
            RateParseError::UnknownPeriod("week".to_string())
        );
        assert!(matches!(
            parse_unit("MiB/month"),
            Err(RateParseError::UnknownPeriod(_))
        ));
    }

    #[test]
    fn rejects_malformed_units() {
        for text in ["MiB", "/day", "MiB/", "", "MiB/day/week"] {
            assert!(
                matches!(parse_unit(text), Err(RateParseError::Malformed(_))),
                "expected `{text}` to be malformed"
            );
        }
    }

    #[test]
    fn rejects_unknown_scale() {
        assert_eq!(
            parse_unit("furlongs/day").unwrap_err(),
            RateParseError::UnknownScale("furlongs".to_string())
        );
    }

    // -------- magnitudes --------

    #[test]
    fn resolves_magnitudes_in_base_units() {
        let spec = cfg("MiB/day", 10.0, None).resolve().unwrap();
        assert_eq!(spec.unit, LimitUnit::Bytes);
        assert_eq!(spec.amount, 10 * 1024 * 1024);
        assert_eq!(spec.window, Duration::from_secs(86400));
    }

    #[test]
    fn fractional_limits_resolve() {
        let spec = cfg("MiB/day", 0.5, None).resolve().unwrap();
        assert_eq!(spec.amount, 524_288);
    }

    #[test]
    fn burst_shares_the_scale_not_the_period() {
        // `unit: MiB/day, burst: 1` is 1 MiB -- the `/day` does not apply.
        let spec = cfg("MiB/day", 10.0, Some(1.0)).resolve().unwrap();
        assert_eq!(spec.amount, 10 * 1024 * 1024);
        assert_eq!(spec.burst, 1024 * 1024);
    }

    #[test]
    fn absent_burst_defaults_to_one_period_of_limit() {
        let spec = cfg("iops/second", 5.0, None).resolve().unwrap();
        assert_eq!(spec.amount, 5);
        assert_eq!(spec.burst, 5);
    }

    #[test]
    fn rejects_limits_that_round_to_nothing() {
        // 0.4 of one operation is not a policy.
        assert!(matches!(
            cfg("ops/second", 0.4, None).resolve(),
            Err(RateParseError::BadMagnitude { field: "limit", .. })
        ));
        assert!(matches!(
            cfg("MiB/day", 0.0, None).resolve(),
            Err(RateParseError::BadMagnitude { field: "limit", .. })
        ));
        assert!(matches!(
            cfg("MiB/day", -1.0, None).resolve(),
            Err(RateParseError::BadMagnitude { field: "limit", .. })
        ));
        assert!(matches!(
            cfg("ops/second", 5.0, Some(0.2)).resolve(),
            Err(RateParseError::BadMagnitude { field: "burst", .. })
        ));
    }

    #[test]
    fn rounds_down_so_a_policy_never_permits_more_than_it_says() {
        // 1.9 ops/second is 1, not 2.
        assert_eq!(cfg("ops/second", 1.9, None).resolve().unwrap().amount, 1);
    }

    // -------- dimensions --------

    #[test]
    fn dimension_keys_roundtrip_and_reject_typos() {
        assert_eq!(LimitUnit::parse("bytes").unwrap(), LimitUnit::Bytes);
        assert_eq!(LimitUnit::parse("ops").unwrap(), LimitUnit::Ops);
        assert_eq!(LimitUnit::Bytes.as_str(), "bytes");
        assert_eq!(LimitUnit::Ops.as_str(), "ops");
        // A misspelled dimension must be an error, never a silently
        // unenforced policy.
        assert!(LimitUnit::parse("byte").is_err());
        assert!(LimitUnit::parse("iops").is_err());
    }

    // -------- config validation --------

    #[test]
    fn validate_accepts_the_documented_examples() {
        let yaml = "unit: MiB/day\nlimit: 10\nburst: 1\n";
        let value = validate_rate_limit_config(yaml.as_bytes()).unwrap();
        let back: RateLimitConfig = serde_json::from_value(value).unwrap();
        assert_eq!(back.unit, "MiB/day");
        assert_eq!(back.limit, 10.0);
        assert_eq!(back.burst, Some(1.0));

        let yaml = "unit: iops/second\nlimit: 5\nburst: 20\n";
        assert!(validate_rate_limit_config(yaml.as_bytes()).is_ok());
    }

    #[test]
    fn validate_rejects_bad_policy_at_config_time() {
        // A bad unit must fail here -- at `pond apply` -- not on the first
        // governed action.
        assert!(validate_rate_limit_config(b"unit: MB/day\nlimit: 10\n").is_err());
        assert!(validate_rate_limit_config(b"unit: MiB/week\nlimit: 10\n").is_err());
        assert!(validate_rate_limit_config(b"unit: MiB/day\nlimit: 0\n").is_err());
    }

    #[test]
    fn validate_rejects_unknown_and_missing_fields() {
        // `deny_unknown_fields`: a typo'd key is an error, not a default.
        assert!(validate_rate_limit_config(b"unit: MiB/day\nlimit: 10\nrate: 5\n").is_err());
        assert!(validate_rate_limit_config(b"limit: 10\n").is_err());
        assert!(validate_rate_limit_config(b"unit: MiB/day\n").is_err());
        // The magnitudes are numbers, not prose: the old embedded-amount
        // form is a type error rather than a parser special case.
        assert!(validate_rate_limit_config(b"unit: MiB/day\nlimit: 10 MiB\n").is_err());
    }

    #[test]
    fn spec_from_config_bytes_matches_resolve() {
        let yaml = b"unit: MiB/day\nlimit: 10\n";
        let spec = spec_from_config_bytes(yaml).unwrap();
        assert_eq!(spec, cfg("MiB/day", 10.0, None).resolve().unwrap());
        assert!(spec_from_config_bytes(b"unit: MB/day\nlimit: 10\n").is_err());
    }

    #[test]
    fn rendering_states_the_effective_policy() {
        let c = cfg("MiB/day", 10.0, Some(1.0));
        let spec = c.resolve().unwrap();
        let text = String::from_utf8(render(&spec, &c)).unwrap();
        assert!(text.contains("unit: MiB/day"), "{text}");
        assert!(text.contains("dimension `bytes`"), "{text}");
        assert!(text.contains("10485760 bytes over 1 day"), "{text}");
        assert!(text.contains("burst 1048576 bytes"), "{text}");
    }

    #[test]
    fn rendering_round_trips_to_the_same_spec() {
        // The node's bytes are what consumers bind against, so the rendering
        // must parse back to exactly the policy it describes -- otherwise
        // `pond cat` and enforcement could disagree.
        for c in [
            cfg("MiB/day", 10.0, Some(1.0)),
            cfg("iops/second", 5.0, None),
            cfg("GiB/hour", 0.5, None),
        ] {
            let spec = c.resolve().unwrap();
            let rendered = render(&spec, &c);
            let reparsed = spec_from_config_bytes(&rendered).unwrap();
            assert_eq!(reparsed, spec, "round trip failed for {}", c.unit);
        }
    }

    #[test]
    fn rendering_makes_the_default_burst_explicit() {
        let c = cfg("iops/second", 5.0, None);
        let spec = c.resolve().unwrap();
        let text = String::from_utf8(render(&spec, &c)).unwrap();
        assert!(text.contains("burst: 5"), "{text}");
    }

    #[test]
    fn factory_is_registered_under_its_name() {
        let f = crate::FactoryRegistry::get_factory("rate-limit")
            .expect("rate-limit factory should be registered");
        assert!(f.create_file.is_some());
        // A leaf config node: nothing to execute, no directory to build.
        assert!(f.create_directory.is_none());
        assert!(f.execute.is_none());
    }

    #[test]
    fn factory_name_conflicts_with_nothing() {
        assert!(crate::SchemeRegistry::find_conflicts().is_empty());
    }
}
