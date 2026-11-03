// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::config::{Config, validate_path};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::Read,
};

const MAX_FILE_BYTES: u64 = 2 * 1024 * 1024;
const MAX_REGISTRY_BYTES: u64 = 64 * 1024 * 1024;
const MAX_CATALOGS: usize = 4096;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    page_uri: String,
    title: String,
    aliases: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Catalog {
    schema_version: u32,
    site_id: String,
    entries: Vec<Entry>,
    catalog_id: String,
}

#[derive(Clone)]
pub struct Registry {
    pages: BTreeMap<String, BTreeSet<String>>,
}

impl Registry {
    pub fn load(config: &Config) -> Result<Self, String> {
        let mut pages = BTreeMap::new();
        let mut total_bytes = 0;
        let dir = std::fs::read_dir(&config.catalog_dir)
            .map_err(|_| "cannot read private catalog registry")?;
        for entry in dir {
            let entry = entry.map_err(|_| "cannot read catalog directory entry")?;
            if entry.path().extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            if !entry
                .file_type()
                .map_err(|_| "cannot inspect catalog")?
                .is_file()
            {
                return Err("catalog must be a regular file, not a symlink".into());
            }
            let mut bytes = Vec::new();
            File::open(entry.path())
                .map_err(|_| "cannot open catalog")?
                .take(MAX_FILE_BYTES + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| "cannot read catalog")?;
            total_bytes += bytes.len() as u64;
            if bytes.len() as u64 > MAX_FILE_BYTES
                || total_bytes > MAX_REGISTRY_BYTES
                || pages.len() >= MAX_CATALOGS
            {
                return Err("catalog registry exceeds supported bounds".into());
            }
            let catalog: Catalog =
                serde_json::from_slice(&bytes).map_err(|_| "invalid catalog JSON/schema")?;
            if catalog.schema_version != 1 || catalog.site_id != config.site_id {
                return Err("catalog version/site does not match collector".into());
            }
            #[derive(Serialize)]
            struct Identity<'a> {
                schema_version: u32,
                site_id: &'a str,
                entries: &'a [Entry],
            }
            let canonical = serde_json::to_vec(&Identity {
                schema_version: 1,
                site_id: &catalog.site_id,
                entries: &catalog.entries,
            })
            .map_err(|_| "cannot encode catalog identity")?;
            if digest(&canonical) != catalog.catalog_id {
                return Err("catalog content does not match its identity".into());
            }
            let mut uris = BTreeSet::new();
            let mut aliases = BTreeSet::new();
            let mut previous: Option<&str> = None;
            for entry in &catalog.entries {
                validate_path(&entry.page_uri)?;
                if previous.is_some_and(|p| p >= entry.page_uri.as_str())
                    || entry.title.len() > 4096
                    || entry.aliases.len() > 16
                {
                    return Err("catalog ordering or entry bounds invalid".into());
                }
                previous = Some(&entry.page_uri);
                uris.insert(entry.page_uri.clone());
                for alias in &entry.aliases {
                    validate_path(alias)?;
                    if !aliases.insert(alias) {
                        return Err("catalog contains conflicting aliases".into());
                    }
                }
            }
            if pages.insert(catalog.catalog_id, uris).is_some() {
                return Err("duplicate installed catalog".into());
            }
        }
        if pages.is_empty() {
            return Err("collector requires an installed catalog".into());
        }
        Ok(Self { pages })
    }

    pub fn accepts(&self, id: &str, uri: &str) -> bool {
        self.pages.get(id).is_some_and(|pages| pages.contains(uri))
    }

    pub fn ids(&self) -> Vec<&str> {
        self.pages.keys().map(String::as_str).collect()
    }

    pub fn preserves(&self, old: &Self) -> bool {
        old.pages.keys().all(|id| self.pages.contains_key(id))
    }
}

pub fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
pub fn install_fixture(config: &Config, uri: &str) -> String {
    std::fs::create_dir_all(&config.catalog_dir).unwrap();
    let entries = vec![Entry {
        page_uri: uri.into(),
        title: "Home".into(),
        aliases: vec![uri.into()],
    }];
    // serde_json::Value uses a different key order; use the producer's struct order.
    #[derive(Serialize)]
    struct Identity<'a> {
        schema_version: u32,
        site_id: &'a str,
        entries: &'a [Entry],
    }
    let bytes = serde_json::to_vec(&Identity {
        schema_version: 1,
        site_id: &config.site_id,
        entries: &entries,
    })
    .unwrap();
    let id = digest(&bytes);
    let mut catalog: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    catalog["catalog_id"] = id.clone().into();
    std::fs::write(
        config.catalog_dir.join(format!("{id}.json")),
        serde_json::to_vec(&catalog).unwrap(),
    )
    .unwrap();
    id
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::fixture;

    #[test]
    fn catalogs_are_site_bound_and_old_catalogs_remain_accepted() {
        let temp = tempfile::tempdir().unwrap();
        let config = fixture(temp.path());
        let old = install_fixture(&config, "/noyo-harbor/");
        let first = Registry::load(&config).unwrap();
        let new = install_fixture(&config, "/");
        let next = Registry::load(&config).unwrap();
        assert!(next.preserves(&first));
        assert!(next.accepts(&old, "/noyo-harbor/"));
        assert!(next.accepts(&new, "/"));
        assert!(!next.accepts(&old, "/"));
        let mut other_site = config.clone();
        other_site.site_id = "caspar-water".into();
        assert!(Registry::load(&other_site).is_err());
        std::fs::remove_file(config.catalog_dir.join(format!("{old}.json"))).unwrap();
        assert!(!Registry::load(&config).unwrap().preserves(&first));
    }

    #[test]
    fn changed_catalog_content_cannot_reuse_an_installed_identity() {
        let temp = tempfile::tempdir().unwrap();
        let config = fixture(temp.path());
        let id = install_fixture(&config, "/");
        let path = config.catalog_dir.join(format!("{id}.json"));
        let original = std::fs::read_to_string(&path).unwrap();
        std::fs::write(&path, original.replace("Home", "Changed")).unwrap();
        assert!(Registry::load(&config).is_err());
        std::fs::write(&path, vec![b'x'; MAX_FILE_BYTES as usize + 1]).unwrap();
        assert!(Registry::load(&config).is_err());
    }
}
