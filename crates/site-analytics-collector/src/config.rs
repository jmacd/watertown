// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};
use url::Url;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub site_id: String,
    pub origin: String,
    pub listen: SocketAddr,
    pub catalog_dir: PathBuf,
    pub log_dir: PathBuf,
    pub endpoint: String,
    pub max_log_bytes: u64,
    pub segment_bytes: u64,
}

impl Config {
    pub fn validate(&self) -> Result<(), String> {
        validate_site_id(&self.site_id)?;
        let origin = Url::parse(&self.origin).map_err(|_| "invalid collector origin")?;
        if !matches!(origin.scheme(), "http" | "https")
            || origin.origin().ascii_serialization() != self.origin
        {
            return Err(
                "origin must be a canonical HTTP(S) origin without a trailing slash".into(),
            );
        }
        if !self.listen.ip().is_loopback() {
            return Err("collector must listen on a loopback address".into());
        }
        validate_path(&self.endpoint)?;
        if self.endpoint == "/_analytics/health" {
            return Err("endpoint conflicts with private health route".into());
        }
        if !self.log_dir.is_absolute() || !self.catalog_dir.is_absolute() {
            return Err("collector directories must be absolute paths".into());
        }
        if self.log_dir.starts_with(&self.catalog_dir)
            || self.catalog_dir.starts_with(&self.log_dir)
        {
            return Err("catalog and log directories must be separate".into());
        }
        if self.segment_bytes < 4096 || self.max_log_bytes < self.segment_bytes.saturating_mul(4) {
            return Err(
                "segment_bytes must be >= 4096 and max_log_bytes >= 4 * segment_bytes".into(),
            );
        }
        Ok(())
    }

    pub fn host(&self) -> &str {
        self.origin.split_once("://").expect("validated origin").1
    }
}

pub fn validate_site_id(id: &str) -> Result<(), String> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
    {
        return Err("invalid analytics site identity".into());
    }
    Ok(())
}

pub fn validate_path(path: &str) -> Result<(), String> {
    if !path.starts_with('/')
        || path.len() > 1024
        || path.contains("//")
        || path.contains(['\\', '%', '?', '#'])
        || path.chars().any(|c| c.is_control() || c.is_whitespace())
        || path.split('/').any(|s| matches!(s, "." | ".."))
    {
        return Err("invalid analytics path".into());
    }
    Ok(())
}

#[cfg(test)]
pub fn fixture(root: &std::path::Path) -> Config {
    Config {
        site_id: "noyo".into(),
        origin: "https://noyocenter.org".into(),
        listen: "127.0.0.1:0".parse().unwrap(),
        catalog_dir: root.join("catalogs"),
        log_dir: root.join("logs"),
        endpoint: "/_analytics/view".into(),
        max_log_bytes: 1024 * 1024,
        segment_bytes: 4096,
    }
}
