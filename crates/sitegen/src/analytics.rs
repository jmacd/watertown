// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Publisher-owned catalogs. A subsite is an independent analytics site.

use crate::config::{AnalyticsConfig, SiteConfig};
use crate::routes::PageJob;
use maud::{Markup, html};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

pub(crate) const SCRIPT: &str = include_str!("../assets/analytics.js");
pub(crate) const CATALOG_PATH: &str = "_analytics/catalog.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CatalogEntry {
    pub page_uri: String,
    pub title: String,
    pub aliases: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Catalog {
    pub schema_version: u32,
    pub site_id: String,
    pub entries: Vec<CatalogEntry>,
    pub catalog_id: String,
}

pub(crate) struct PageAnalytics {
    pub page_uri: String,
    pub catalog_id: String,
    pub endpoint: String,
    pub script_url: String,
}

impl PageAnalytics {
    pub fn markup(&self) -> Markup {
        html! {
            script defer src=(&self.script_url)
                data-watertown-analytics="1"
                data-page-uri=(&self.page_uri)
                data-catalog-id=(&self.catalog_id)
                data-endpoint=(&self.endpoint) {}
        }
    }
}

pub(crate) fn enabled(config: &SiteConfig) -> Option<&AnalyticsConfig> {
    config.analytics.as_ref().filter(|a| a.enabled)
}

pub(crate) fn validate(config: &SiteConfig) -> Result<(), String> {
    let Some(analytics) = enabled(config) else {
        return Ok(());
    };
    validate_site(&analytics.site_id)?;
    validate_path(&analytics.endpoint)?;
    validate_path(&config.site.base_url)?;
    if !config.site.base_url.ends_with('/') {
        return Err("analytics requires site.base_url to end with '/'".into());
    }
    for path in &analytics.exclude {
        validate_path(path)?;
    }
    Ok(())
}

pub(crate) fn validate_site(site_id: &str) -> Result<(), String> {
    if site_id.is_empty()
        || site_id.len() > 128
        || !site_id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
    {
        return Err("analytics.site_id must be 1-128 ASCII letters, digits, '-' or '_'".into());
    }
    Ok(())
}

pub(crate) fn validate_path(path: &str) -> Result<(), String> {
    if !path.starts_with('/')
        || path.contains("//")
        || path.contains(['\\', '?', '#', '%'])
        || path.chars().any(|c| c.is_control() || c.is_whitespace())
        || path.split('/').any(|part| matches!(part, "." | ".."))
        || path.len() > 1024
    {
        return Err(format!("invalid analytics path {path:?}"));
    }
    Ok(())
}

/// Aliases match the supported Caddy path/path.html/path/ serving contract.
/// No percent decoding, case folding, or general trailing-slash normalization.
pub(crate) fn identity(base: &str, output: &str) -> Result<(String, Vec<String>), String> {
    if output.starts_with('/') || output.is_empty() {
        return Err(format!("invalid analytics output path {output:?}"));
    }
    let emitted = format!("{base}{output}");
    validate_path(&emitted)?;
    let canonical = if let Some(dir) = emitted.strip_suffix("/index.html") {
        format!("{dir}/")
    } else {
        emitted.strip_suffix(".html").unwrap_or(&emitted).into()
    };
    let mut aliases = BTreeSet::from([emitted.clone(), canonical.clone()]);
    if let Some(extensionless) = emitted.strip_suffix(".html") {
        aliases.insert(extensionless.into());
    }
    if canonical != "/" && canonical.ends_with('/') {
        aliases.insert(canonical.trim_end_matches('/').into());
    }
    Ok((canonical, aliases.into_iter().collect()))
}

pub(crate) fn excluded(config: &SiteConfig, uri: &str) -> bool {
    uri.trim_end_matches('/') == format!("{}analytics", config.site.base_url)
        || enabled(config).is_some_and(|a| a.exclude.iter().any(|path| path == uri))
}

impl Catalog {
    pub fn new(config: &SiteConfig, mut entries: Vec<CatalogEntry>) -> Result<Self, String> {
        validate(config)?;
        let analytics = enabled(config).ok_or("analytics is disabled")?;
        entries.sort_by(|a, b| a.page_uri.cmp(&b.page_uri));
        let mut aliases = BTreeSet::new();
        for entry in &entries {
            for alias in &entry.aliases {
                if !aliases.insert(alias) {
                    return Err(format!("analytics page or alias collision: {alias}"));
                }
            }
        }
        // Hash only versioned publisher metadata, never build time or data exports.
        #[derive(Serialize)]
        struct Identity<'a> {
            schema_version: u32,
            site_id: &'a str,
            entries: &'a [CatalogEntry],
        }
        let bytes = serde_json::to_vec(&Identity {
            schema_version: 1,
            site_id: &analytics.site_id,
            entries: &entries,
        })
        .map_err(|e| e.to_string())?;
        Ok(Self {
            schema_version: 1,
            site_id: analytics.site_id.clone(),
            entries,
            catalog_id: digest(&bytes),
        })
    }

    pub fn page(&self, config: &SiteConfig, uri: &str) -> Option<PageAnalytics> {
        self.entries
            .iter()
            .any(|entry| entry.page_uri == uri)
            .then(|| PageAnalytics {
                page_uri: uri.into(),
                catalog_id: self.catalog_id.clone(),
                endpoint: enabled(config)
                    .expect("catalog requires enabled analytics")
                    .endpoint
                    .clone(),
                script_url: format!("{}{}", config.site.base_url, script_path()),
            })
    }

    pub fn write(&self, output: &Path) -> Result<(), String> {
        let path = output.join(CATALOG_PATH);
        std::fs::create_dir_all(path.parent().expect("catalog parent"))
            .map_err(|e| e.to_string())?;
        std::fs::write(
            path,
            serde_json::to_vec_pretty(self).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        std::fs::write(output.join(script_path()), SCRIPT).map_err(|e| e.to_string())
    }
}

fn script_path() -> String {
    format!("_analytics/view-{}.js", digest(SCRIPT.as_bytes()))
}

fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Validate combined serving paths without combining catalog ownership.
#[derive(Default)]
pub(crate) struct BuildRegistry {
    active: bool,
    sites: BTreeSet<String>,
    paths: BTreeMap<String, String>,
    endpoints: BTreeSet<String>,
    reserved: BTreeSet<String>,
    conflicts: Vec<String>,
}

impl BuildRegistry {
    pub fn record(
        &mut self,
        config: &SiteConfig,
        jobs: &[PageJob],
        assets: impl Iterator<Item = impl AsRef<str>>,
    ) -> Result<(), String> {
        if let Some(analytics) = enabled(config) {
            self.active = true;
            validate(config)?;
            if !self.sites.insert(analytics.site_id.clone()) {
                return Err(format!(
                    "duplicate analytics site_id: {}",
                    analytics.site_id
                ));
            }
            if !self.endpoints.insert(analytics.endpoint.clone()) {
                return Err(format!(
                    "analytics sites must use distinct beacon endpoints: {}",
                    analytics.endpoint
                ));
            }
            self.reserved
                .insert(format!("{}_analytics/", config.site.base_url));
        }
        for job in jobs {
            self.record_output(config, &job.output_path);
        }
        for path in assets {
            let path = path.as_ref().trim_start_matches('/');
            let extension = Path::new(path)
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("");
            if enabled(config).is_some()
                && (extension.eq_ignore_ascii_case("html") || extension.eq_ignore_ascii_case("htm"))
            {
                let (uri, _) = identity(&config.site.base_url, path)?;
                if !excluded(config, &uri) {
                    return Err(format!(
                        "analytics cannot instrument copied HTML asset '{path}'; exclude '{uri}' explicitly or render it through a page layout"
                    ));
                }
            }
            self.record_output(config, path);
        }
        Ok(())
    }

    fn record_output(&mut self, config: &SiteConfig, output: &str) {
        let aliases = if output.ends_with(".html") {
            match identity(&config.site.base_url, output) {
                Ok((_, aliases)) => aliases,
                Err(error) => {
                    self.conflicts.push(error);
                    return;
                }
            }
        } else {
            vec![format!("{}{output}", config.site.base_url)]
        };
        for alias in aliases {
            if let Some(previous) = self.paths.insert(alias.clone(), output.into()) {
                self.conflicts.push(format!(
                    "analytics serving-path collision '{alias}': '{previous}' and '{output}'"
                ));
            }
        }
    }

    pub fn finish(&self) -> Result<(), String> {
        if self.active {
            if let Some(error) = self.conflicts.first() {
                return Err(error.clone());
            }
            for prefix in &self.reserved {
                if let Some((path, _)) = self.paths.range(prefix.clone()..).next()
                    && path.starts_with(prefix)
                {
                    return Err(format!("analytics reserves output path '{path}'"));
                }
            }
            for endpoint in &self.endpoints {
                if self.paths.contains_key(endpoint) {
                    return Err(format!(
                        "analytics endpoint conflicts with emitted path '{endpoint}'"
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(id: &str, base: &str) -> SiteConfig {
        serde_yaml::from_str(&format!(
            "site:\n  title: Test\n  base_url: {base}\nanalytics:\n  enabled: true\n  site_id: {id}\n"
        )).unwrap()
    }

    fn entry(base: &str, output: &str) -> CatalogEntry {
        let (page_uri, aliases) = identity(base, output).unwrap();
        CatalogEntry {
            page_uri,
            aliases,
            title: output.into(),
        }
    }

    #[test]
    fn catalogs_are_deterministic_site_owned_and_mount_aware() {
        let root = config("caspar-water", "/");
        let entries = vec![entry("/", "water.html"), entry("/", "index.html")];
        let first = Catalog::new(&root, entries.clone()).unwrap();
        let mut reordered = entries;
        reordered.reverse();
        assert_eq!(
            first.catalog_id,
            Catalog::new(&root, reordered.clone()).unwrap().catalog_id
        );
        assert_ne!(
            first.catalog_id,
            Catalog::new(&config("noyo", "/"), reordered)
                .unwrap()
                .catalog_id
        );
        let noyo = Catalog::new(
            &config("noyo", "/noyo-harbor/"),
            vec![
                entry("/noyo-harbor/", "index.html"),
                entry("/noyo-harbor/", "sites/BDock.html"),
            ],
        )
        .unwrap();
        assert_eq!(noyo.entries[0].page_uri, "/noyo-harbor/");
        assert_eq!(noyo.entries[1].page_uri, "/noyo-harbor/sites/BDock");
        assert!(
            noyo.entries[0]
                .aliases
                .contains(&"/noyo-harbor/index.html".into())
        );
        assert!(
            noyo.entries[0]
                .aliases
                .contains(&"/noyo-harbor/index".into())
        );
        assert_eq!(
            noyo.page(&config("noyo", "/noyo-harbor/"), "/noyo-harbor/")
                .unwrap()
                .endpoint,
            "/_analytics/view"
        );
    }

    #[test]
    fn catalog_rejects_actual_serving_alias_collisions() {
        assert!(
            Catalog::new(
                &config("test", "/"),
                vec![entry("/", "water.html"), entry("/", "water/index.html"),]
            )
            .unwrap_err()
            .contains("collision")
        );
        for output in [
            "../escape.html",
            "/absolute.html",
            "x//y.html",
            "x%2fy.html",
            "x?y.html",
        ] {
            assert!(identity("/", output).is_err(), "{output}");
        }
        assert_ne!(
            entry("/", "Water.html").page_uri,
            entry("/", "water.html").page_uri
        );
    }

    #[test]
    fn paths_and_site_ids_are_validated_before_instrumentation() {
        let mut config = config("noyo", "/noyo-harbor/");
        for endpoint in [
            "https://elsewhere/view",
            "//elsewhere/view",
            "/view?secret=1",
            "/../view",
        ] {
            config.analytics.as_mut().unwrap().endpoint = endpoint.into();
            assert!(validate(&config).is_err());
        }
        config.analytics.as_mut().unwrap().endpoint = "/_analytics/view".into();
        config.analytics.as_mut().unwrap().site_id = "noyo/harbor".into();
        assert!(validate(&config).is_err());
    }

    #[test]
    fn raw_html_requires_explicit_exclusion_and_cross_site_collisions_fail() {
        let mut config = config("noyo", "/noyo-harbor/");
        assert!(
            BuildRegistry::default()
                .record(&config, &[], ["/raw.html"].iter())
                .is_err()
        );
        config
            .analytics
            .as_mut()
            .unwrap()
            .exclude
            .push("/noyo-harbor/raw".into());
        let mut registry = BuildRegistry::default();
        registry.record(&config, &[], ["/raw.html"].iter()).unwrap();
        registry.finish().unwrap();
        config.analytics.as_mut().unwrap().site_id = "other".into();
        config.analytics.as_mut().unwrap().endpoint = "/_analytics/other/view".into();
        registry.record(&config, &[], ["/raw.html"].iter()).unwrap();
        assert!(registry.finish().unwrap_err().contains("collision"));
    }

    #[test]
    fn independent_sites_cannot_share_a_beacon_endpoint() {
        let mut registry = BuildRegistry::default();
        registry
            .record(&config("parent", "/"), &[], std::iter::empty::<&str>())
            .unwrap();
        let error = registry
            .record(
                &config("noyo", "/noyo-harbor/"),
                &[],
                std::iter::empty::<&str>(),
            )
            .unwrap_err();
        assert!(error.contains("distinct beacon endpoints"), "{error}");
    }

    #[test]
    fn parent_analytics_namespace_is_reserved_across_subsites() {
        let parent = config("parent", "/");
        let mut child = config("child", "/_analytics/");
        child.analytics = None;
        let mut registry = BuildRegistry::default();
        registry
            .record(&parent, &[], std::iter::empty::<&str>())
            .unwrap();
        registry.record(&child, &[], ["index.html"].iter()).unwrap();
        assert!(registry.finish().unwrap_err().contains("reserves"));
    }
}
