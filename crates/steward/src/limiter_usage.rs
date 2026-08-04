// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Limiter usage as a durable, monitorable metric (Decision L12).
//!
//! A limiter's window lives in the control table, which is per-replica and
//! disposable -- exactly right for enforcement, and useless for monitoring.
//! You cannot chart a number that vanishes when the control table is rebuilt,
//! and you cannot alert on a budget you cannot see approaching.  So spending
//! is *also* recorded into the pond, where it is durable and replicated.
//!
//! # Why the emission is deferred
//!
//! Spending happens during the post-commit push, and a push cannot write the
//! pond: writing would commit, which would push, which would spend, which
//! would write.  The accumulated usage is therefore parked in the control
//! table and flushed into the pond at the **start of the next write
//! transaction** -- piggybacking on a write the caller was doing anyway.
//!
//! That breaks the loop and is self-limiting: the write that emits sample *N*
//! triggers a push that queues sample *N+1*, emitted by the following write.
//! A pond that stops being written stops emitting, which is correct -- an idle
//! pond is also not spending.
//!
//! # Why samples are cleared at commit, not at read
//!
//! The rows are written through the transaction, so they land in the pond only
//! if that transaction commits.  The pending queue is therefore drained only on
//! that same commit.  An aborted write re-emits its samples next time rather
//! than dropping them, and the drain removes exactly the samples that were
//! emitted rather than blindly clearing, so anything queued in between
//! survives.

use serde::{Deserialize, Serialize};
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, TimeUnit};
use tinyfs::arrow::parquet::ParquetExt;

use crate::ControlTable;

/// Control key holding usage samples awaiting emission into the pond.
pub const LIMITER_USAGE_PENDING_KEY: &str = "limiter-usage-pending";

/// The pond series every limiter's usage is appended to.
///
/// One shared series rather than one per limiter: monitoring wants a single
/// table it can group by `limiter`, and a per-limiter series would multiply
/// pond nodes with every policy added.
pub const LIMITER_USAGE_SERIES: &str = "/sys/limits/usage";

/// The directory holding [`LIMITER_USAGE_SERIES`], created on demand.
pub const LIMITER_USAGE_DIR: &str = "/sys/limits";

/// One limiter's spending during one governed activity, queued in control
/// until a write transaction can carry it into the pond.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageSample {
    /// When the spending was committed, in epoch microseconds.  This is the
    /// moment of the activity, not of the later emission, so a chart shows
    /// when budget was actually consumed.
    pub at_us: i64,
    /// Pond path of the `rate-limit` node that governed the activity.
    pub limiter: String,
    /// The dimension spent (`bytes` or `ops`).
    pub unit: String,
    /// Units spent by this activity.  Differencing is not required: this is
    /// already a delta, so it charts directly as a rate.
    pub amount: u64,
    /// Sliding-window total after the spending, in the same units.
    pub used: u64,
    /// The configured budget at the time.  Carried per row so a chart stays
    /// honest across a retune rather than comparing old usage to a new limit.
    pub limit: u64,
    /// Window length in microseconds.
    pub window_us: i64,
}

/// A row of [`LIMITER_USAGE_SERIES`].
///
/// The pond-facing shape of [`UsageSample`]: microsecond fields become an
/// Arrow timestamp and a whole-second window, which is what a query wants.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimiterUsageRow {
    /// Event time of the spending (epoch microseconds, UTC).
    pub timestamp: i64,
    pub limiter: String,
    pub unit: String,
    pub amount: u64,
    pub used: u64,
    pub limit: u64,
    /// Window length in seconds.
    pub window_secs: u64,
}

impl tinyfs::arrow::schema::ForArrow for LimiterUsageRow {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            )),
            Arc::new(Field::new("limiter", DataType::Utf8, false)),
            Arc::new(Field::new("unit", DataType::Utf8, false)),
            Arc::new(Field::new("amount", DataType::UInt64, false)),
            Arc::new(Field::new("used", DataType::UInt64, false)),
            Arc::new(Field::new("limit", DataType::UInt64, false)),
            Arc::new(Field::new("window_secs", DataType::UInt64, false)),
        ]
    }
}

impl From<&UsageSample> for LimiterUsageRow {
    fn from(s: &UsageSample) -> Self {
        Self {
            timestamp: s.at_us,
            limiter: s.limiter.clone(),
            unit: s.unit.clone(),
            amount: s.amount,
            used: s.used,
            limit: s.limit,
            window_secs: u64::try_from(s.window_us).unwrap_or(0) / 1_000_000,
        }
    }
}

/// Read the queued samples without disturbing them.
///
/// A malformed or absent value yields an empty queue: a monitoring metric must
/// never be the reason a write transaction fails.
pub async fn read_pending(control: &ControlTable) -> Vec<UsageSample> {
    let raw = match control.raw_config_get(LIMITER_USAGE_PENDING_KEY).await {
        Ok(Some(text)) if !text.is_empty() => text,
        Ok(_) => return Vec::new(),
        Err(e) => {
            log::warn!("limiter usage: cannot read pending samples: {e}");
            return Vec::new();
        }
    };
    match serde_json::from_str::<Vec<UsageSample>>(&raw) {
        Ok(v) => v,
        Err(e) => {
            log::warn!("limiter usage: discarding unreadable pending samples: {e}");
            Vec::new()
        }
    }
}

/// Queue `samples` for emission by the next write transaction.
///
/// One control write regardless of how many limiters spent, appended to
/// whatever is already queued.
pub async fn queue(control: &mut ControlTable, samples: &[UsageSample]) {
    if samples.is_empty() {
        return;
    }
    let mut all = read_pending(control).await;
    all.extend_from_slice(samples);

    // A pond that is never written again must not grow this key without
    // bound.  Keeping the newest samples means a long-idle pond reports its
    // most recent spending rather than its oldest.
    const MAX_PENDING: usize = 4096;
    if all.len() > MAX_PENDING {
        let drop_count = all.len() - MAX_PENDING;
        log::warn!(
            "limiter usage: dropping {drop_count} oldest pending sample(s); \
             the pond has not been written in a long time"
        );
        let _ = all.drain(..drop_count);
    }

    match serde_json::to_string(&all) {
        Ok(text) => {
            if let Err(e) = control
                .raw_config_set(LIMITER_USAGE_PENDING_KEY, &text)
                .await
            {
                log::warn!("limiter usage: cannot queue samples: {e}");
            }
        }
        Err(e) => log::warn!("limiter usage: cannot serialize samples: {e}"),
    }
}

/// Drop the first `count` queued samples, which have just been committed into
/// the pond.
///
/// Removing a prefix rather than clearing the key preserves anything queued
/// after the emission began.
pub async fn drop_emitted(control: &mut ControlTable, count: usize) {
    if count == 0 {
        return;
    }
    let mut all = read_pending(control).await;
    let remaining = all.split_off(count.min(all.len()));

    let text = if remaining.is_empty() {
        String::new()
    } else {
        match serde_json::to_string(&remaining) {
            Ok(t) => t,
            Err(e) => {
                log::warn!("limiter usage: cannot re-serialize pending samples: {e}");
                return;
            }
        }
    };
    if let Err(e) = control
        .raw_config_set(LIMITER_USAGE_PENDING_KEY, &text)
        .await
    {
        log::warn!("limiter usage: cannot drain pending samples: {e}");
    }
}

/// Append `samples` to [`LIMITER_USAGE_SERIES`] through an open write
/// transaction.
///
/// # Errors
///
/// Returns the underlying tinyfs error.  Callers treat this as non-fatal:
/// failing a user's write because a metric could not be recorded would trade a
/// monitoring gap for an outage.
pub async fn emit(fs: &tinyfs::FS, samples: &[UsageSample]) -> tinyfs::Result<()> {
    if samples.is_empty() {
        return Ok(());
    }
    let rows: Vec<LimiterUsageRow> = samples.iter().map(LimiterUsageRow::from).collect();

    let root = fs.root().await?;
    let _ = root.create_dir_all(LIMITER_USAGE_DIR).await?;
    let _ = root
        .write_series_from_items(LIMITER_USAGE_SERIES, &rows, Some("timestamp"))
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(at_us: i64, amount: u64) -> UsageSample {
        UsageSample {
            at_us,
            limiter: "/sys/limits/backup-bytes".to_string(),
            unit: "bytes".to_string(),
            amount,
            used: amount,
            limit: 10 * 1024 * 1024,
            window_us: 86_400_000_000,
        }
    }

    #[test]
    fn samples_round_trip_through_json() {
        let queued = vec![sample(1, 10), sample(2, 20)];
        let text = serde_json::to_string(&queued).unwrap();
        let back: Vec<UsageSample> = serde_json::from_str(&text).unwrap();
        assert_eq!(back, queued);
    }

    /// The row carries the spending time, not the (later) emission time, so a
    /// chart shows when budget was actually consumed.
    #[test]
    fn row_preserves_the_spending_time() {
        let row = LimiterUsageRow::from(&sample(1_700_000_000_000_000, 42));
        assert_eq!(row.timestamp, 1_700_000_000_000_000);
        assert_eq!(row.amount, 42);
    }

    #[test]
    fn window_is_reported_in_whole_seconds() {
        let row = LimiterUsageRow::from(&sample(0, 1));
        assert_eq!(row.window_secs, 86_400);
    }

    /// `amount` is already a delta, so a rate chart needs no differencing and
    /// is immune to the window being reset by a control rebuild.
    #[test]
    fn amount_is_a_delta_not_a_running_total() {
        let rows: Vec<LimiterUsageRow> = [sample(1, 10), sample(2, 20)]
            .iter()
            .map(LimiterUsageRow::from)
            .collect();
        assert_eq!(rows.iter().map(|r| r.amount).sum::<u64>(), 30);
    }

    #[test]
    fn arrow_schema_names_the_event_time_column_first() {
        use tinyfs::arrow::schema::ForArrow;
        let fields = LimiterUsageRow::for_arrow();
        assert_eq!(fields[0].name(), "timestamp");
        assert!(matches!(
            fields[0].data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, _)
        ));
        assert_eq!(fields.len(), 7);
    }
}
