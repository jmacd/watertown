// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! # Sitegen -- Static site generator for Watertown
//!
//! Replaces the Tera-based TemplateFactory with a Markdown + Maud pipeline.
//! Uses pulldown-cmark for markdown rendering and Maud for HTML layouts.
//!
//! ## Usage
//!
//! ```bash
//! pond run /etc/site.yaml build ./dist
//! ```

mod config;
mod documentation;
mod email;
mod factory;
mod layouts;
pub mod markdown;
mod monitor;
pub mod partitions;
pub mod report;
mod routes;
mod shortcodes;
mod status_summary;

pub use config::SiteConfig;

#[cfg(feature = "documentation")]
#[must_use]
pub fn documentation_schemas() -> std::collections::BTreeMap<&'static str, schemars::Schema> {
    fn schema<T: schemars::JsonSchema>() -> schemars::Schema {
        schemars::generate::SchemaSettings::draft2020_12()
            .for_deserialize()
            .into_generator()
            .into_root_schema_for::<T>()
    }
    std::collections::BTreeMap::from([
        ("factory:sitegen", schema::<SiteConfig>()),
        ("factory:monitor-report", schema::<monitor::MonitorConfig>()),
        ("factory:email-report", schema::<email::EmailReportConfig>()),
        ("sitegen:frontmatter", schema::<factory::Frontmatter>()),
    ])
}

#[cfg(feature = "documentation")]
#[must_use]
pub fn documentation_shortcodes() -> Vec<String> {
    let mut shortcodes = shortcodes::register_shortcodes(std::sync::Arc::new(
        shortcodes::ShortcodeContext::default(),
    ));
    documentation::register_source(
        &mut shortcodes,
        config::DocumentationConfig {
            version: 1,
            source_root: "/".into(),
            include_roots: vec![],
        },
        std::collections::BTreeMap::new(),
    );
    shortcodes.names()
}
