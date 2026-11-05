// SPDX-License-Identifier: Apache-2.0

//! Daily public projection and an explicit, independently refreshed last-good bundle.

use crate::{
    SiteConfig,
    analytics::{Catalog, validate_path},
    routes::PageJob,
};
use datafusion::{
    arrow::array::{Array, StringArray, TimestampMicrosecondArray, UInt64Array},
    logical_expr::{col, lit},
    scalar::ScalarValue,
};
use futures::StreamExt;
use maud::{DOCTYPE, html};
use provider::factory::analytics_rollup::partitions::{DAY_US, PartitionSink};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read, Write},
    path::Path,
};
use tinyfs::{Result, ResultExt};

const MAX_BYTES: u64 = 16 * 1024 * 1024;
const MAX_ROWS: usize = 100_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "documentation", derive(schemars::JsonSchema))]
pub struct AnalyticsReportConfig {
    /// Independent site/environment identity, matching the rollup and catalogs.
    pub site_id: String,
    /// Committed analytics-rollup target in this site's own pond.
    pub target: String,
    /// Private host directory of retained publisher catalogs, never raw logs.
    pub catalog_dir: String,
    /// Current publisher catalog; retired pages are never linked blindly.
    pub current_catalog: String,
    /// Private host file shared by refresh and rendering; outside the web root.
    pub state_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DayCount {
    day: String,
    page_uri: String,
    page_views: u64,
    partial: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Page {
    page_uri: String,
    title: String,
    retired: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicReport {
    schema_version: u32,
    site_id: String,
    window_start: String,
    window_end_exclusive: String,
    snapshot_published_at: String,
    coverage: String,
    daily: Vec<DayCount>,
    pages: Vec<Page>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    site_id: String,
    target: String,
    failed: bool,
    data: Option<PublicReport>,
}

fn fail(message: impl Into<String>) -> tinyfs::Error {
    tinyfs::Error::Other(format!("analytics report: {}", message.into()))
}

pub(crate) fn validate(config: &SiteConfig) -> std::result::Result<(), String> {
    let Some(report) = &config.analytics_report else {
        return Ok(());
    };
    crate::analytics::validate_site(&report.site_id)?;
    validate_path(&config.site.base_url)?;
    if !config.site.base_url.ends_with('/') {
        return Err("analytics report requires a trailing '/' in base_url".into());
    }
    if config
        .analytics
        .as_ref()
        .is_some_and(|a| a.site_id != report.site_id)
    {
        return Err("analytics collection/report site_id must agree".into());
    }
    for path in [
        &report.target,
        &report.catalog_dir,
        &report.current_catalog,
        &report.state_path,
    ] {
        if !Path::new(path).is_absolute()
            || Path::new(path)
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err("analytics report paths must be absolute, without '..'".into());
        }
    }
    Ok(())
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path, limit: u64) -> Result<T> {
    let mut bytes = Vec::new();
    let _ = std::fs::File::open(path)
        .map_other()?
        .take(limit + 1)
        .read_to_end(&mut bytes)
        .map_other()?;
    if bytes.len() as u64 > limit {
        return Err(fail("JSON exceeds supported size"));
    }
    serde_json::from_slice(&bytes).map_other()
}

fn write_state(path: &Path, state: &State) -> Result<()> {
    let bytes = serde_json::to_vec(state).map_other()?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(fail("report exceeds supported size"));
    }
    let parent = path
        .parent()
        .ok_or_else(|| fail("state needs a parent directory"))?;
    std::fs::create_dir_all(parent).map_other()?;
    let mut file = tempfile::NamedTempFile::new_in(parent).map_other()?;
    file.write_all(&bytes).map_other()?;
    file.as_file().sync_all().map_other()?;
    let _ = file.persist(path).map_other()?;
    Ok(())
}

fn day(time: i64) -> Result<String> {
    chrono::DateTime::from_timestamp_micros(time)
        .map(|date| date.format("%Y-%m-%d").to_string())
        .ok_or_else(|| fail("unsupported date"))
}

fn window(now: i64) -> Result<(i64, i64)> {
    let today = now.div_euclid(DAY_US);
    let start = today
        .checked_sub(29)
        .and_then(|d| d.checked_mul(DAY_US))
        .ok_or_else(|| fail("window overflow"))?;
    let end = today
        .checked_add(1)
        .and_then(|d| d.checked_mul(DAY_US))
        .ok_or_else(|| fail("window overflow"))?;
    Ok((start, end))
}

fn labels(config: &AnalyticsReportConfig) -> Result<BTreeMap<String, Page>> {
    let current: Catalog = read_json(Path::new(&config.current_catalog), 2 * 1024 * 1024)?;
    current.verify(&config.site_id).map_err(fail)?;
    let mut paths = std::fs::read_dir(&config.catalog_dir)
        .map_other()?
        .take(4097)
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()
        .map_other()?;
    if paths.len() > 4096 {
        return Err(fail("too many retained catalogs"));
    }
    paths.sort();
    let mut labels: BTreeMap<String, Page> = BTreeMap::new();
    let mut bytes = 0;
    for path in paths
        .into_iter()
        .filter(|path| path.extension().is_some_and(|e| e == "json"))
    {
        bytes += std::fs::metadata(&path).map_other()?.len();
        if bytes > 64 * 1024 * 1024 {
            return Err(fail("catalog history exceeds supported size"));
        }
        let catalog: Catalog = read_json(&path, 2 * 1024 * 1024)?;
        catalog.verify(&config.site_id).map_err(fail)?;
        for entry in catalog.entries {
            let page = labels.entry(entry.page_uri.clone()).or_insert(Page {
                page_uri: entry.page_uri.clone(),
                title: entry.title.clone(),
                retired: true,
            });
            if page.title != entry.title {
                page.title.clone_from(&entry.page_uri);
            }
        }
    }
    for entry in current.entries {
        let _ = labels.insert(
            entry.page_uri.clone(),
            Page {
                page_uri: entry.page_uri,
                title: entry.title,
                retired: false,
            },
        );
    }
    Ok(labels)
}

async fn project(
    config: &AnalyticsReportConfig,
    context: &tinyfs::ProviderContext,
    now: i64,
) -> Result<PublicReport> {
    let sink = PartitionSink::new(context.clone(), config.target.clone());
    let manifest = sink
        .load()
        .await?
        .ok_or_else(|| fail("no committed rollup"))?;
    if manifest.source.site_id != config.site_id {
        return Err(fail("rollup site_id mismatch"));
    }
    let labels = labels(config)?;
    let (start, end) = window(now)?;
    let timestamp = |value| {
        lit(ScalarValue::TimestampMicrosecond(
            Some(value),
            Some("UTC".into()),
        ))
    };
    let mut stream = context
        .datafusion_session
        .read_table(sink.table(&manifest).await?)
        .map_other()?
        .filter(
            col("timestamp")
                .gt_eq(timestamp(start))
                .and(col("timestamp").lt(timestamp(end))),
        )
        .map_other()?
        .execute_stream()
        .await
        .map_other()?;
    let mut counts: BTreeMap<(String, String), u64> = BTreeMap::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.map_other()?;
        let times = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| fail("invalid timestamp column"))?;
        let paths = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| fail("invalid URI column"))?;
        let views = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| fail("invalid count column"))?;
        for row in 0..batch.num_rows() {
            if times.is_null(row)
                || paths.is_null(row)
                || views.is_null(row)
                || views.value(row) == 0
            {
                return Err(fail("invalid aggregate row"));
            }
            if !labels.contains_key(paths.value(row)) {
                return Err(fail("missing retained catalog for counted page"));
            }
            let count = counts
                .entry((day(times.value(row))?, paths.value(row).into()))
                .or_default();
            *count = count
                .checked_add(views.value(row))
                .ok_or_else(|| fail("daily count overflow"))?;
            if counts.len() > MAX_ROWS {
                return Err(fail("too many public daily rows"));
            }
        }
    }
    let pages: BTreeSet<_> = counts.keys().map(|(_, uri)| uri.clone()).collect();
    let partial_from = day(manifest.published_at_us)?.min(day(now)?);
    let data = PublicReport {
        schema_version: 1,
        site_id: config.site_id.clone(),
        window_start: day(start)?,
        window_end_exclusive: day(end)?,
        snapshot_published_at: chrono::DateTime::from_timestamp_micros(manifest.published_at_us)
            .ok_or_else(|| fail("invalid publication timestamp"))?
            .to_rfc3339(),
        coverage: "unknown".into(),
        daily: counts
            .into_iter()
            .map(|((day, page_uri), page_views)| DayCount {
                partial: day >= partial_from,
                day,
                page_uri,
                page_views,
            })
            .collect(),
        pages: pages.into_iter().map(|uri| labels[&uri].clone()).collect(),
    };
    validate_data(&data, &config.site_id)?;
    if serde_json::to_vec(&data).map_other()?.len() as u64 > MAX_BYTES / 2 {
        return Err(fail("public report exceeds supported size"));
    }
    Ok(data)
}

fn load_state(config: &AnalyticsReportConfig) -> Result<State> {
    let state: State = read_json(Path::new(&config.state_path), MAX_BYTES)?;
    if state.site_id != config.site_id || state.target != config.target {
        return Err(fail("saved report belongs to another site or target"));
    }
    if !state.failed && state.data.is_none() {
        return Err(fail("successful report has no data"));
    }
    if let Some(data) = &state.data {
        validate_data(data, &config.site_id)?;
    }
    Ok(state)
}

fn validate_data(data: &PublicReport, site: &str) -> Result<()> {
    let parse = |value: &str| {
        let date = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map_err(|_| fail("invalid public date"))?;
        if date.format("%Y-%m-%d").to_string() != value {
            return Err(fail("noncanonical public date"));
        }
        Ok(date)
    };
    let start = parse(&data.window_start)?;
    let end = parse(&data.window_end_exclusive)?;
    if data.schema_version != 1
        || data.site_id != site
        || data.coverage != "unknown"
        || end.signed_duration_since(start).num_days() != 30
        || data.daily.len() > MAX_ROWS
    {
        return Err(fail("invalid public report contract"));
    }
    let _ = chrono::DateTime::parse_from_rfc3339(&data.snapshot_published_at).map_other()?;
    let mut pages = BTreeSet::new();
    for page in &data.pages {
        validate_path(&page.page_uri).map_err(fail)?;
        if page.title.len() > 1024 || !pages.insert(&page.page_uri) {
            return Err(fail("invalid public page label"));
        }
    }
    let mut keys = BTreeSet::new();
    let mut total = 0u64;
    for row in &data.daily {
        let date = parse(&row.day)?;
        if date < start
            || date >= end
            || row.page_views == 0
            || !pages.contains(&row.page_uri)
            || !keys.insert((&row.day, &row.page_uri))
        {
            return Err(fail("invalid public daily row"));
        }
        total = total
            .checked_add(row.page_views)
            .ok_or_else(|| fail("public total overflow"))?;
    }
    Ok(())
}

/// Refresh is separate from site rendering and always returns the original job failure.
pub(crate) async fn refresh(
    config: &SiteConfig,
    context: &tinyfs::ProviderContext,
    now: i64,
) -> Result<()> {
    validate(config).map_err(fail)?;
    let report = config
        .analytics_report
        .as_ref()
        .ok_or_else(|| fail("not configured"))?;
    let result = project(report, context, now).await;
    match result {
        Ok(data) => write_state(
            Path::new(&report.state_path),
            &State {
                site_id: report.site_id.clone(),
                target: report.target.clone(),
                failed: false,
                data: Some(data),
            },
        ),
        Err(error) => {
            log::error!("Analytics refresh failed for {}: {error}", report.site_id);
            let prior = match load_state(report) {
                Ok(state) => state.data,
                Err(prior_error) => {
                    log::error!(
                        "No usable last-good analytics for {}: {prior_error}",
                        report.site_id
                    );
                    None
                }
            };
            if let Err(state_error) = write_state(
                Path::new(&report.state_path),
                &State {
                    site_id: report.site_id.clone(),
                    target: report.target.clone(),
                    failed: true,
                    data: prior,
                },
            ) {
                return Err(fail(format!(
                    "{error}; recording failure also failed: {state_error}"
                )));
            }
            Err(error)
        }
    }
}

/// Render only typed public fields; private state and refresh errors are never copied.
pub(crate) fn render(
    config: &SiteConfig,
    jobs: &[PageJob],
    output: &Path,
    root_base: &str,
    now: i64,
) -> Result<()> {
    let Some(report) = &config.analytics_report else {
        return Ok(());
    };
    let mut public_root = output.canonicalize().map_other()?;
    let mount = config
        .site
        .base_url
        .strip_prefix(root_base)
        .ok_or_else(|| fail("report mount is outside the root site"))?;
    for component in Path::new(mount).components() {
        if !matches!(component, std::path::Component::Normal(_)) || !public_root.pop() {
            return Err(fail("invalid report mount"));
        }
    }
    let private_path = Path::new(&report.state_path);
    let mut ancestor = private_path
        .parent()
        .ok_or_else(|| fail("state needs a parent directory"))?;
    // Resolve existing parent aliases even before the state file is created.
    let resolved_path = loop {
        match ancestor.canonicalize() {
            Ok(parent) => {
                break parent.join(private_path.strip_prefix(ancestor).map_other()?);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                ancestor = ancestor
                    .parent()
                    .ok_or_else(|| fail("state needs an existing ancestor"))?;
            }
            Err(error) => return Err(fail(error.to_string())),
        }
    };
    if resolved_path.starts_with(&public_root)
        || private_path
            .canonicalize()
            .is_ok_and(|path| path.starts_with(&public_root))
    {
        return Err(fail("private analytics state must be outside the web root"));
    }
    let state = match load_state(report) {
        Ok(state) => Some(state),
        Err(error) => {
            log::error!("Analytics unavailable for {}: {error}", report.site_id);
            None
        }
    };
    let failed = state.as_ref().is_none_or(|state| state.failed);
    let mut data = state.and_then(|state| state.data);
    let (start, end) = window(now)?;
    let start = day(start)?;
    let end = day(end)?;
    let today = day(now)?;
    let live: BTreeSet<_> = jobs
        .iter()
        .map(|job| {
            crate::analytics::identity(&config.site.base_url, &job.output_path).map(|(uri, _)| uri)
        })
        .collect::<std::result::Result<_, _>>()
        .map_err(fail)?;
    if let Some(data) = &mut data {
        data.daily.retain(|row| row.day >= start && row.day < end);
        let used: BTreeSet<_> = data.daily.iter().map(|row| row.page_uri.as_str()).collect();
        data.pages
            .retain(|page| used.contains(page.page_uri.as_str()));
        for page in &mut data.pages {
            page.retired |= !live.contains(&page.page_uri);
        }
    }
    let mut totals: BTreeMap<&str, u64> = BTreeMap::new();
    let mut partial_days = BTreeSet::new();
    let mut page_totals: BTreeMap<&str, u64> = BTreeMap::new();
    let mut histories: BTreeMap<&str, Vec<&DayCount>> = BTreeMap::new();
    let page_labels: BTreeMap<_, _> = data
        .iter()
        .flat_map(|data| &data.pages)
        .map(|page| (page.page_uri.as_str(), page))
        .collect();
    if let Some(data) = &data {
        for row in &data.daily {
            histories.entry(&row.page_uri).or_default().push(row);
            if row.partial {
                let _ = partial_days.insert(row.day.as_str());
            }
            let total = totals.entry(&row.day).or_default();
            *total = total
                .checked_add(row.page_views)
                .ok_or_else(|| fail("site total overflow"))?;
            let total = page_totals.entry(&row.page_uri).or_default();
            *total = total
                .checked_add(row.page_views)
                .ok_or_else(|| fail("page total overflow"))?;
        }
    }
    let mut ranking = page_totals.into_iter().collect::<Vec<_>>();
    ranking.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
    let max = totals.values().copied().max().unwrap_or(1);
    let markup = html! {
        (DOCTYPE)
        html lang="en" {
            head { meta charset="utf-8"; meta name="viewport" content="width=device-width, initial-scale=1";
                title { "Reported page views | " (&config.site.title) }
                link rel="stylesheet" href=(format!("{root_base}style.css"));
            }
            body { main {
                nav { a href=(&config.site.base_url) { (&config.site.title) } }
                h1 { "Reported page views" }
                p { "Daily counts over 30 UTC days. Not unique visitors or proven human visits. Browser blocking and delivery loss are unobservable." }
                p { "Coverage: unknown. Missing page/day counts are unknown, not zero. Today is partial." }
                @if failed { p role="status" { "Analytics refresh failed or is unavailable. Showing last-good data when available." } }
                @if let Some(data) = &data {
                    p { "Aggregate snapshot published: " (&data.snapshot_published_at) ". This timestamp is not proof of complete collection." }
                    p { "Original snapshot window: " (&data.window_start) " to " (&data.window_end_exclusive) " (exclusive). Only dates within the current 30-day window are shown." }
                    a href=(format!("{}_analytics/report.json", config.site.base_url)) { "Download daily aggregate JSON" }
                    h2 { "Daily site totals" }
                    @if totals.is_empty() { p { "No reported counts in this window; coverage remains unknown." } }
                    table { thead { tr { th { "UTC day" } th { "Reported views" } th { "Relative count" } } }
                        tbody { @for (date, count) in &totals { tr {
                            td { (date) @if partial_days.contains(date) || **date == today { " (partial)" } }
                            td { (count) }
                            td { svg width="200" height="16" role="img" aria-label=(format!("{count} reported views")) {
                                rect width=((*count as f64 / max as f64 * 200.0).max(1.0)) height="16" fill="currentColor" {}
                            } }
                        } } }
                    }
                    h2 { "Pages and daily history" }
                    @for (uri, count) in ranking {
                        @let page = page_labels[uri];
                        details {
                            summary {
                                @if page.retired { (&page.title) " (retired/unavailable)" }
                                @else { a href=(&page.page_uri) { (&page.title) } }
                                " - " (count) " reported views"
                            }
                            p { (&page.page_uri) }
                            table { tbody { @for row in &histories[uri] {
                                tr { td { (&row.day) @if row.partial || row.day == today { " (partial)" } } td { (row.page_views) } }
                            } } }
                        }
                    }
                } @else { p { "Analytics unavailable." } }
            } }
        }
    };
    let json_path = output.join("_analytics/report.json");
    if let Some(data) = data {
        std::fs::create_dir_all(json_path.parent().expect("public report parent")).map_other()?;
        std::fs::write(&json_path, serde_json::to_vec_pretty(&data).map_other()?).map_other()?;
    } else {
        match std::fs::remove_file(&json_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(fail(error.to_string())),
        }
    }
    std::fs::write(output.join("analytics.html"), markup.into_string()).map_other()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytics::CatalogEntry;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use provider::factory::analytics_rollup::partitions::{
        HOUR_US, Inventory, Manifest, schema, source_id,
    };
    use std::sync::Arc;
    use tlogfs::{OpLogPersistence, PondTxnMetadata, PondUserMetadata};

    fn config(temp: &Path) -> SiteConfig {
        let mut config: SiteConfig = serde_yaml::from_str(
            "site: {title: Noyo, base_url: /noyo-harbor/}\nanalytics: {enabled: true, site_id: noyo}\n",
        ).expect("config");
        config.analytics_report = Some(AnalyticsReportConfig {
            site_id: "noyo".into(),
            target: "/rollup".into(),
            catalog_dir: temp.join("catalogs").to_string_lossy().into(),
            current_catalog: temp.join("current.json").to_string_lossy().into(),
            state_path: temp.join("private/state.json").to_string_lossy().into(),
        });
        config
    }

    fn catalogs(config: &SiteConfig) {
        let report = config.analytics_report.as_ref().expect("report");
        std::fs::create_dir_all(&report.catalog_dir).expect("catalog directory");
        let home = CatalogEntry {
            page_uri: "/noyo-harbor/".into(),
            title: "<script>title</script>".into(),
            aliases: vec!["/noyo-harbor/".into()],
        };
        let current = Catalog::new(config, vec![home.clone()]).expect("current catalog");
        let history = Catalog::new(
            config,
            vec![
                home,
                CatalogEntry {
                    page_uri: "/noyo-harbor/gone".into(),
                    title: "Retired".into(),
                    aliases: vec!["/noyo-harbor/gone".into()],
                },
            ],
        )
        .expect("history");
        std::fs::write(
            &report.current_catalog,
            serde_json::to_vec(&current).expect("JSON"),
        )
        .expect("current");
        std::fs::write(
            Path::new(&report.catalog_dir).join("retained.json"),
            serde_json::to_vec(&history).expect("JSON"),
        )
        .expect("history");
    }

    fn jobs() -> Vec<PageJob> {
        vec![PageJob {
            output_path: "index.html".into(),
            page_source: "/index.md".into(),
            captures: vec![],
            datafiles: vec![],
            annotations: vec![],
            breadcrumbs: vec![],
        }]
    }

    #[tokio::test]
    async fn daily_public_projection_keeps_last_good_freshness_and_fails_the_job() {
        let temp = tempfile::tempdir().unwrap();
        let config = config(temp.path());
        catalogs(&config);
        let now = 50 * DAY_US + 2 * HOUR_US;
        let id = uuid::Uuid::new_v4().to_string();
        let mut pond = OpLogPersistence::create(
            temp.path().join("pond"),
            id.clone(),
            PondUserMetadata::new(vec!["report".into()]),
        )
        .await
        .unwrap();
        let metadata = PondTxnMetadata::new(
            pond.last_txn_seq_for(&id) + 1,
            PondUserMetadata::new(vec!["fixture".into()]),
        );
        let tx = pond.begin_write(&metadata).await.unwrap();
        let context = tx.state().unwrap().as_provider_context();
        let sink = PartitionSink::new(context, "/rollup".into());
        let mut days = BTreeMap::new();
        for (day, rows) in [
            (20, vec![(0, "/noyo-harbor/", 99)]),
            (21, vec![(0, "/noyo-harbor/", 1)]),
            (49, vec![(0, "/noyo-harbor/gone", 4)]),
            (50, vec![(0, "/noyo-harbor/", 2), (1, "/noyo-harbor/", 3)]),
        ] {
            let batch = RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(
                        TimestampMicrosecondArray::from(
                            rows.iter()
                                .map(|(h, _, _)| day * DAY_US + h * HOUR_US)
                                .collect::<Vec<_>>(),
                        )
                        .with_timezone("UTC"),
                    ),
                    Arc::new(StringArray::from(
                        rows.iter().map(|(_, p, _)| *p).collect::<Vec<_>>(),
                    )),
                    Arc::new(UInt64Array::from(
                        rows.iter().map(|(_, _, v)| *v).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let stream = Box::pin(RecordBatchStreamAdapter::new(
                schema(),
                futures::stream::iter(vec![Ok(batch)]),
            ));
            let _ = days.insert(
                day * DAY_US,
                sink.stage_day(day * DAY_US, stream).await.unwrap().unwrap(),
            );
        }
        let source = Inventory::parse(
            serde_json::to_string(&serde_json::json!({
                "schema_version": 1, "site_id": "noyo", "closed": [], "active": {
                    "segment_id": uuid::Uuid::new_v4(), "predecessor": null, "bytes": 100,
                    "record_count": 5, "sha256": "a".repeat(64),
                },
            }))
            .unwrap()
            .as_bytes(),
            "noyo",
        )
        .unwrap();
        sink.publish(&Manifest {
            schema_version: 1,
            source_path: "/raw-not-available".into(),
            recipe_id: "daily-report-fixture".into(),
            source_state_id: source_id(&source).unwrap(),
            source,
            published_at_us: now,
            collection_coverage: "unknown".into(),
            days,
        })
        .await
        .unwrap();
        let _ = tx.commit().await.unwrap();
        let metadata = PondTxnMetadata::new(
            pond.last_txn_seq_for(&id) + 1,
            PondUserMetadata::new(vec!["refresh".into()]),
        );
        let tx = pond.begin_write(&metadata).await.unwrap();
        let context = tx.state().unwrap().as_provider_context();
        refresh(&config, &context, now).await.unwrap();
        let report = config.analytics_report.as_ref().unwrap();
        let state = load_state(report).unwrap();
        let original = state.data.unwrap();
        assert_eq!(original.daily.len(), 3);
        assert_eq!(original.daily[2].page_views, 5);
        assert!(original.daily[2].partial);
        assert!(!original.daily[1].partial);
        let output = temp.path().join("public/noyo-harbor");
        std::fs::create_dir_all(&output).unwrap();
        render(&config, &jobs(), &output, "/", now).unwrap();
        let html = std::fs::read_to_string(output.join("analytics.html")).unwrap();
        assert!(html.contains("&lt;script&gt;title&lt;/script&gt;"));
        assert!(!html.contains("<script>"));
        assert!(html.contains("href=\"/noyo-harbor/\""));
        assert!(!html.contains("href=\"/noyo-harbor/gone\""));
        assert!(html.contains("retired/unavailable"));
        let public: serde_json::Value =
            read_json(&output.join("_analytics/report.json"), MAX_BYTES).unwrap();
        assert_eq!(public.as_object().unwrap().len(), 8);
        assert_eq!(public["daily"][2].as_object().unwrap().len(), 4);
        let serialized = serde_json::to_string(&public).unwrap();
        for private in [
            "record_seq",
            "segment_id",
            "catalog_id",
            "raw-not-available",
            "source_state_id",
            "\"timestamp\"",
        ] {
            assert!(!serialized.contains(private), "{private}");
        }

        std::fs::remove_file(&report.current_catalog).unwrap();
        assert!(refresh(&config, &context, now + DAY_US).await.is_err());
        let failed = load_state(report).unwrap();
        assert!(failed.failed);
        assert_eq!(
            serde_json::to_value(failed.data).unwrap(),
            serde_json::to_value(&original).unwrap()
        );
        render(&config, &jobs(), &output, "/", now + DAY_US).unwrap();
        let public: PublicReport =
            read_json(&output.join("_analytics/report.json"), MAX_BYTES).unwrap();
        assert_eq!(
            public.daily.len(),
            2,
            "fallback never extends the public history window"
        );
        assert_eq!(public.snapshot_published_at, original.snapshot_published_at);
        assert!(public.daily[1].partial, "old partial buckets stay partial");
        let html = std::fs::read_to_string(output.join("analytics.html")).unwrap();
        assert!(html.contains("Analytics refresh failed"));

        let mut other = config.clone();
        other.analytics_report.as_mut().unwrap().site_id = "caspar-water".into();
        render(&other, &jobs(), &output, "/", now).unwrap();
        assert!(!output.join("_analytics/report.json").exists());
        assert!(
            std::fs::read_to_string(output.join("analytics.html"))
                .unwrap()
                .contains("Analytics unavailable.")
        );
        let mut first = config.clone();
        first.analytics_report.as_mut().unwrap().state_path =
            temp.path().join("first.json").to_string_lossy().into();
        assert!(refresh(&first, &context, now).await.is_err());
        assert!(
            load_state(first.analytics_report.as_ref().unwrap())
                .unwrap()
                .data
                .is_none()
        );
        render(&first, &jobs(), &output, "/", now).unwrap();
        assert!(
            std::fs::read_to_string(output.join("analytics.html"))
                .unwrap()
                .contains("Analytics unavailable.")
        );

        std::fs::write(&report.state_path, b"{broken private state").unwrap();
        render(&config, &jobs(), &output, "/", now).unwrap();
        assert!(!output.join("_analytics/report.json").exists());
        assert!(
            !std::fs::read_to_string(output.join("analytics.html"))
                .unwrap()
                .contains("broken private state")
        );
        let mut unsafe_output = config.clone();
        unsafe_output.analytics_report.as_mut().unwrap().state_path = temp
            .path()
            .join("public/state.json")
            .to_string_lossy()
            .into();
        assert!(render(&unsafe_output, &jobs(), &output, "/", now).is_err());
        let mut collision = jobs();
        let mut dashboard = jobs().pop().unwrap();
        dashboard.output_path = "analytics.html".into();
        collision.push(dashboard);
        let mut registry = crate::analytics::BuildRegistry::default();
        registry
            .record(&config, &collision, std::iter::empty::<&str>())
            .unwrap();
        assert!(registry.finish().is_err());

        catalogs(&config);
        refresh(&config, &context, now + DAY_US).await.unwrap();
        assert!(!load_state(report).unwrap().failed);
        assert_eq!(
            load_state(report)
                .unwrap()
                .data
                .unwrap()
                .snapshot_published_at,
            original.snapshot_published_at
        );
    }
}
