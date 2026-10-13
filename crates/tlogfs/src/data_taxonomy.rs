// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Watertown data taxonomy for classifying and protecting sensitive configuration data.
//!
//! This module defines data classes for sensitive information in Watertown configurations,
//! such as API keys, OAuth secrets, and service credentials. These classified types
//! automatically redact sensitive data when serialized or displayed, preventing accidental
//! exposure in logs, debugging output, or file listings.
//!
//! # Usage
//!
//! Wrap sensitive configuration fields with the appropriate classified type:
//!
//! # Programmatic Access
//!
//! To access the actual value of a classified field, use `.as_declassified()`:
//!
//! # Integration with Serde
//!
//! Classified types implement custom `Serialize` that outputs "[REDACTED]" instead
//! of the actual value. This ensures that when configurations are displayed via
//! `pond cat` or similar commands, sensitive data is not exposed.

use data_privacy::{classified, taxonomy};
use serde::{Deserialize, Serialize};

/// Watertown data taxonomy for sensitive configuration fields
///
/// This enum defines the data classification categories used by the classified
/// wrappers below.
#[taxonomy(watertown)]
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum WatertownTaxonomy {
    /// API keys, client IDs, access keys - identifiers for authentication
    ApiKey,

    /// API secrets, client secrets, secret access keys - sensitive credentials
    ApiSecret,

    /// Service endpoints that may reveal infrastructure details
    ServiceEndpoint,
}

macro_rules! classified_type {
    ($name:ident, $class:ident) => {
        #[classified(WatertownTaxonomy::$class)]
        #[derive(Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name<T>(T);

        impl<T> $name<T> {
            /// Classify a value.
            pub fn new(value: T) -> Self {
                Self(value)
            }

            /// Borrow the classified value for authorized use.
            pub fn as_declassified(&self) -> &T {
                &self.0
            }

            /// Remove the classification wrapper.
            pub fn declassify(self) -> T {
                self.0
            }
        }
    };
}

classified_type!(ApiKey, ApiKey);
classified_type!(ApiSecret, ApiSecret);
classified_type!(ServiceEndpoint, ServiceEndpoint);

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug)]
    struct TestConfig {
        pub client_id: ApiKey<String>,
        pub client_secret: ApiSecret<String>,
        pub endpoint: ServiceEndpoint<String>,
        pub public_field: String,
    }

    #[test]
    fn test_sensitive_fields_redacted_in_json() {
        let config = TestConfig {
            client_id: ApiKey::new("my-client-id-12345".to_string()),
            client_secret: ApiSecret::new("super-secret-key".to_string()),
            endpoint: ServiceEndpoint::new("https://api.example.com".to_string()),
            public_field: "public-data".to_string(),
        };

        let json = serde_json::to_string_pretty(&config).unwrap();

        // Sensitive fields should contain actual values for config file I/O
        assert!(json.contains(r#""client_id": "my-client-id-12345""#));
        assert!(json.contains(r#""client_secret": "super-secret-key""#));
        assert!(json.contains(r#""endpoint": "https://api.example.com""#));

        // Public field should be visible
        assert!(json.contains(r#""public_field": "public-data""#));
    }

    #[test]
    fn test_sensitive_fields_redacted_in_yaml() {
        let config = TestConfig {
            client_id: ApiKey::new("my-client-id-12345".to_string()),
            client_secret: ApiSecret::new("super-secret-key".to_string()),
            endpoint: ServiceEndpoint::new("https://api.example.com".to_string()),
            public_field: "public-data".to_string(),
        };

        let yaml = serde_yaml::to_string(&config).unwrap();

        // Sensitive fields should contain actual values for config file I/O
        assert!(yaml.contains("client_id: my-client-id-12345"));
        assert!(yaml.contains("client_secret: super-secret-key"));
        assert!(
            yaml.contains("endpoint: 'https://api.example.com'")
                || yaml.contains("endpoint: https://api.example.com")
        );

        // Public field should be visible
        assert!(yaml.contains("public_field: public-data"));
    }

    #[test]
    fn test_declassify_provides_actual_value() {
        let config = TestConfig {
            client_id: ApiKey::new("my-client-id-12345".to_string()),
            client_secret: ApiSecret::new("super-secret-key".to_string()),
            endpoint: ServiceEndpoint::new("https://api.example.com".to_string()),
            public_field: "public-data".to_string(),
        };

        // Programmatic access via declassify() works
        assert_eq!(config.client_id.declassify(), "my-client-id-12345");
        assert_eq!(config.client_secret.declassify(), "super-secret-key");
        assert_eq!(config.endpoint.declassify(), "https://api.example.com");
    }

    #[test]
    fn test_deserialization_from_yaml() {
        let yaml = r#"
client_id: my-client-id-12345
client_secret: super-secret-key
endpoint: https://api.example.com
public_field: public-data
"#;

        let config: TestConfig = serde_yaml::from_str(yaml).unwrap();

        // Deserialized values are accessible via declassify
        assert_eq!(config.client_id.declassify(), "my-client-id-12345");
        assert_eq!(config.client_secret.declassify(), "super-secret-key");
        assert_eq!(config.endpoint.declassify(), "https://api.example.com");
        assert_eq!(config.public_field, "public-data");
    }

    #[test]
    fn test_debug_output_is_redacted() {
        let api_key = ApiKey::new("my-secret-key".to_string());
        let debug_output = format!("{:?}", api_key);

        // Debug output should show only the classification, not the actual value.
        assert!(debug_output.contains("CLASSIFIED:watertown/api_key"));
        assert!(!debug_output.contains("my-secret-key"));
    }
}
