// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Test utilities and fixtures for TinyFS
//!
//! This module provides reusable test factories and utilities for testing
//! dynamic node functionality across the TinyFS and TLogFS layers.

use serde::{Deserialize, Serialize};

pub mod persistence_contract;

/// Configuration for test directory factory
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TestDirectoryConfig {
    /// Name/description of this test directory
    pub name: String,
    /// Optional metadata for testing
    #[serde(default)]
    pub metadata: std::collections::HashMap<String, String>,
}

/// Configuration for test file factory  
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TestFileConfig {
    /// Content to return when reading the file
    pub content: String,
    /// Optional MIME type
    #[serde(default)]
    pub mime_type: Option<String>,
}

impl TestDirectoryConfig {
    /// Create a simple test directory config
    pub fn simple(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            metadata: std::collections::HashMap::new(),
        }
    }

    /// Serialize to YAML bytes
    pub fn to_yaml_bytes(&self) -> Result<Vec<u8>, serde_yaml::Error> {
        serde_yaml::to_string(self).map(|s| s.into_bytes())
    }

    /// Deserialize from YAML bytes
    pub fn from_yaml_bytes(bytes: &[u8]) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_slice(bytes)
    }
}

impl TestFileConfig {
    /// Create a simple test file config
    pub fn simple(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            mime_type: None,
        }
    }

    /// Serialize to YAML bytes
    pub fn to_yaml_bytes(&self) -> Result<Vec<u8>, serde_yaml::Error> {
        serde_yaml::to_string(self).map(|s| s.into_bytes())
    }

    /// Deserialize from YAML bytes
    pub fn from_yaml_bytes(bytes: &[u8]) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_directory_config_roundtrip() {
        let config = TestDirectoryConfig::simple("test-dir");
        let yaml = config.to_yaml_bytes().unwrap();
        let parsed = TestDirectoryConfig::from_yaml_bytes(&yaml).unwrap();
        assert_eq!(config, parsed);
    }

    #[test]
    fn test_file_config_roundtrip() {
        let config = TestFileConfig::simple("test content");
        let yaml = config.to_yaml_bytes().unwrap();
        let parsed = TestFileConfig::from_yaml_bytes(&yaml).unwrap();
        assert_eq!(config, parsed);
    }
}
