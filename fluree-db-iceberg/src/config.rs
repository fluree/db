//! Configuration schemas for Iceberg graph sources.
//!
//! This module defines the JSON-serializable configuration structures
//! stored in `GraphSourceRecord.config` for Iceberg graph sources.

use crate::auth::AuthConfig;
use crate::catalog::parse_table_identifier;
use crate::catalog::TableIdentifier;
use crate::error::{IcebergError, Result};
use serde::{Deserialize, Serialize};

/// Configuration for an Iceberg graph source.
///
/// This is stored as JSON in `GraphSourceRecord.config` for graph sources with
/// type `GraphSourceType::Iceberg`.
///
/// # Example JSON
///
/// ```json
/// {
///     "catalog": {
///         "type": "rest",
///         "uri": "https://polaris.example.com",
///         "auth": {
///             "type": "bearer",
///             "token": {"env_var": "POLARIS_TOKEN"}
///         }
///     },
///     "table": "openflights.airlines",
///     "io": {
///         "vended_credentials": true
///     }
/// }
/// ```
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IcebergGsConfig {
    /// Catalog configuration
    pub catalog: CatalogConfig,
    /// Table identifier
    pub table: TableConfig,
    /// Storage/IO configuration
    #[serde(default)]
    pub io: IoConfig,
    /// R2RML mapping source (format-agnostic, used in Phase 3)
    #[serde(default)]
    pub mapping: Option<MappingSource>,
}

impl IcebergGsConfig {
    /// Parse from JSON string (stored in GraphSourceRecord.config).
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json).map_err(|e| {
            IcebergError::Config(format!("Failed to parse Iceberg graph source config: {e}"))
        })
    }

    /// Serialize to JSON string.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|e| IcebergError::Config(format!("Failed to serialize config: {e}")))
    }

    /// Serialize to pretty-printed JSON string.
    pub fn to_json_pretty(&self) -> Result<String> {
        serde_json::to_string_pretty(self)
            .map_err(|e| IcebergError::Config(format!("Failed to serialize config: {e}")))
    }

    /// Get the table identifier.
    ///
    /// For `Direct` catalog configs, if no explicit `table` config is set,
    /// the table identifier is derived from the last two path segments of
    /// `table_location` (e.g., `s3://bucket/warehouse/ns/table` → `ns.table`).
    pub fn table_identifier(&self) -> Result<TableIdentifier> {
        let id_str = self.table.identifier();
        if !id_str.is_empty() {
            return parse_table_identifier(&id_str);
        }

        // For Direct mode, derive from table_location path segments
        if let CatalogConfig::Direct { table_location } = &self.catalog {
            let path = table_location
                .trim_start_matches("s3://")
                .trim_start_matches("s3a://");
            let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
            if segments.len() >= 3 {
                // segments[0] = bucket, segments[1..n-2] = warehouse path,
                // segments[n-2] = namespace, segments[n-1] = table
                let ns = segments[segments.len() - 2];
                let table = segments[segments.len() - 1];
                return Ok(TableIdentifier {
                    namespace: ns.to_string(),
                    table: table.to_string(),
                });
            }
        }

        Err(IcebergError::Config(
            "Cannot determine table identifier from config".to_string(),
        ))
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        match &self.catalog {
            CatalogConfig::Rest { uri, .. } => {
                if uri.is_empty() {
                    return Err(IcebergError::Config("catalog.uri is required".to_string()));
                }
                // Validate table identifier can be parsed
                self.table_identifier()?;
            }
            CatalogConfig::Direct { table_location } => {
                if table_location.is_empty() {
                    return Err(IcebergError::Config(
                        "catalog.table_location is required".to_string(),
                    ));
                }
                if !table_location.starts_with("s3://") && !table_location.starts_with("s3a://") {
                    return Err(IcebergError::Config(format!(
                        "Direct catalog table_location must be an S3 URI (s3:// or s3a://), got: {table_location}"
                    )));
                }
                // Validate table identifier can be derived from table_location
                self.table_identifier()?;
                // Vended credentials are not supported with Direct catalog
                if self.io.vended_credentials {
                    return Err(IcebergError::Config(
                        "Vended credentials are not supported with Direct catalog — \
                         use IAM roles or explicit S3 credentials instead"
                            .to_string(),
                    ));
                }
            }
        }

        Ok(())
    }
}

/// How to discover Iceberg table metadata.
///
/// # Variants
///
/// - `Rest` — discover metadata via an Iceberg REST catalog API (e.g., Polaris).
/// - `Direct` — metadata location is already known; the engine reads
///   `version-hint.text` from the table's metadata directory to resolve the
///   current metadata file, making this config set-and-forget.
///
/// # Serde
///
/// Uses a helper enum with `#[serde(untagged)]` to accept both the new tagged
/// format (`{"type": "rest", ...}`) and the legacy flat struct format
/// (`{"uri": "...", "auth": ...}`).
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(from = "CatalogConfigHelper", into = "CatalogConfigHelper")]
#[allow(clippy::large_enum_variant)] // Config type, not in hot paths
pub enum CatalogConfig {
    /// Discover metadata via an Iceberg REST catalog API.
    Rest {
        /// Catalog type identifier (e.g., "polaris", "rest").
        catalog_type: String,
        /// Base URI of the catalog (e.g., "https://polaris.example.com").
        uri: String,
        /// Authentication configuration.
        auth: AuthConfig,
        /// Optional Polaris warehouse identifier.
        warehouse: Option<String>,
    },

    /// Metadata location is already known (e.g., from iceberg-rust commit).
    /// The engine reads `version-hint.text` from the metadata directory
    /// to resolve the current metadata file.
    Direct {
        /// S3 prefix for the table root directory.
        /// Must contain a `metadata/` subdirectory with Iceberg metadata files.
        /// Example: "s3://bucket/warehouse/my_namespace/my_table"
        table_location: String,
    },
}

impl CatalogConfig {
    /// Create a REST catalog config with common defaults.
    pub fn rest(uri: impl Into<String>) -> Self {
        CatalogConfig::Rest {
            catalog_type: "polaris".to_string(),
            uri: uri.into(),
            auth: AuthConfig::None,
            warehouse: None,
        }
    }

    /// Create a Direct catalog config from a table location.
    ///
    /// The `table_location` should be the S3 prefix for the table root
    /// (e.g., `s3://bucket/warehouse/ns/table`). Trailing slashes are stripped.
    pub fn direct(table_location: impl Into<String>) -> Self {
        let mut loc = table_location.into();
        // Normalize: strip trailing slashes
        while loc.ends_with('/') {
            loc.pop();
        }
        CatalogConfig::Direct {
            table_location: loc,
        }
    }
}

// ---------------------------------------------------------------------------
// Serde helper: supports both tagged enum format and legacy flat struct format.
// ---------------------------------------------------------------------------

/// Internal serde helper that uses `#[serde(untagged)]` to accept both the
/// new tagged format (`{"type": "rest", ...}`) and the legacy flat struct
/// format (`{"uri": "...", "auth": ...}`).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
enum CatalogConfigHelper {
    /// New tagged format.
    Tagged(TaggedCatalogConfig),
    /// Legacy flat struct format (deserializes as Rest).
    Legacy(LegacyCatalogConfig),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
#[allow(clippy::large_enum_variant)]
enum TaggedCatalogConfig {
    Rest {
        #[serde(default = "default_catalog_type")]
        catalog_type: String,
        uri: String,
        #[serde(default)]
        auth: AuthConfig,
        #[serde(default)]
        warehouse: Option<String>,
    },
    Direct {
        table_location: String,
    },
}

/// Legacy format: flat struct with `uri` as a required field.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct LegacyCatalogConfig {
    #[serde(default = "default_catalog_type")]
    catalog_type: String,
    uri: String,
    #[serde(default)]
    auth: AuthConfig,
    #[serde(default)]
    warehouse: Option<String>,
}

fn default_catalog_type() -> String {
    "polaris".to_string()
}

impl From<CatalogConfigHelper> for CatalogConfig {
    fn from(helper: CatalogConfigHelper) -> Self {
        match helper {
            CatalogConfigHelper::Tagged(TaggedCatalogConfig::Rest {
                catalog_type,
                uri,
                auth,
                warehouse,
            })
            | CatalogConfigHelper::Legacy(LegacyCatalogConfig {
                catalog_type,
                uri,
                auth,
                warehouse,
            }) => CatalogConfig::Rest {
                catalog_type,
                uri,
                auth,
                warehouse,
            },
            CatalogConfigHelper::Tagged(TaggedCatalogConfig::Direct { table_location }) => {
                CatalogConfig::Direct { table_location }
            }
        }
    }
}

impl From<CatalogConfig> for CatalogConfigHelper {
    fn from(config: CatalogConfig) -> Self {
        match config {
            CatalogConfig::Rest {
                catalog_type,
                uri,
                auth,
                warehouse,
            } => CatalogConfigHelper::Tagged(TaggedCatalogConfig::Rest {
                catalog_type,
                uri,
                auth,
                warehouse,
            }),
            CatalogConfig::Direct { table_location } => {
                CatalogConfigHelper::Tagged(TaggedCatalogConfig::Direct { table_location })
            }
        }
    }
}

/// Table identification configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum TableConfig {
    /// Full table identifier string (e.g., "openflights.airlines")
    Identifier(String),
    /// Structured namespace + table
    Structured { namespace: String, name: String },
}

impl TableConfig {
    /// Get the canonical table identifier string.
    pub fn identifier(&self) -> String {
        match self {
            TableConfig::Identifier(id) => id.clone(),
            TableConfig::Structured { namespace, name } => format!("{namespace}.{name}"),
        }
    }
}

/// Storage I/O configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IoConfig {
    /// Whether to use vended credentials from the catalog (default: true)
    #[serde(default = "default_vended_credentials")]
    pub vended_credentials: bool,
    /// S3 region override
    #[serde(default)]
    pub s3_region: Option<String>,
    /// S3 endpoint override (for MinIO, LocalStack, etc.)
    #[serde(default)]
    pub s3_endpoint: Option<String>,
    /// Use path-style S3 URLs (for MinIO, LocalStack)
    #[serde(default)]
    pub s3_path_style: bool,
}

fn default_vended_credentials() -> bool {
    true
}

impl Default for IoConfig {
    fn default() -> Self {
        Self {
            vended_credentials: true, // Default to using vended credentials
            s3_region: None,
            s3_endpoint: None,
            s3_path_style: false,
        }
    }
}

/// R2RML mapping source (format-agnostic).
///
/// Phase 3 will use this to load mappings without depending on
/// the serialization format (Turtle, JSON-LD, etc.).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MappingSource {
    /// Storage address, URL, or inline content
    pub source: String,
    /// Media type hint (optional, inferred from source extension if omitted)
    /// Examples: "text/turtle", "application/ld+json"
    #[serde(default)]
    pub media_type: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Legacy flat format (backward compatibility) ──

    #[test]
    fn test_parse_minimal_config_legacy_format() {
        let json = r#"{
            "catalog": {
                "uri": "https://polaris.example.com"
            },
            "table": "openflights.airlines"
        }"#;

        let config: IcebergGsConfig = serde_json::from_str(json).unwrap();
        match &config.catalog {
            CatalogConfig::Rest {
                uri, catalog_type, ..
            } => {
                assert_eq!(uri, "https://polaris.example.com");
                assert_eq!(catalog_type, "polaris");
            }
            other => panic!("Expected Rest variant, got {other:?}"),
        }
        assert_eq!(config.table.identifier(), "openflights.airlines");
        assert!(config.io.vended_credentials);
        assert!(config.mapping.is_none());
    }

    #[test]
    fn test_parse_full_config_legacy_format() {
        let json = r#"{
            "catalog": {
                "uri": "https://polaris.example.com",
                "catalog_type": "rest",
                "auth": {
                    "type": "bearer",
                    "token": "my-token"
                },
                "warehouse": "my-warehouse"
            },
            "table": {
                "namespace": "db.schema",
                "name": "events"
            },
            "io": {
                "vended_credentials": false,
                "s3_region": "us-west-2",
                "s3_endpoint": "http://localhost:9000"
            },
            "mapping": {
                "source": "s3://bucket/mapping.ttl",
                "media_type": "text/turtle"
            }
        }"#;

        let config: IcebergGsConfig = serde_json::from_str(json).unwrap();
        match &config.catalog {
            CatalogConfig::Rest {
                catalog_type,
                warehouse,
                ..
            } => {
                assert_eq!(catalog_type, "rest");
                assert_eq!(warehouse, &Some("my-warehouse".to_string()));
            }
            other => panic!("Expected Rest variant, got {other:?}"),
        }
        assert_eq!(config.table.identifier(), "db.schema.events");
        assert!(!config.io.vended_credentials);
        assert_eq!(config.io.s3_region, Some("us-west-2".to_string()));
        let mapping = config.mapping.unwrap();
        assert_eq!(mapping.source, "s3://bucket/mapping.ttl");
        assert_eq!(mapping.media_type, Some("text/turtle".to_string()));
    }

    // ── New tagged format ──

    #[test]
    fn test_parse_tagged_rest_config() {
        let json = r#"{
            "catalog": {
                "type": "rest",
                "uri": "https://polaris.example.com",
                "warehouse": "wh1"
            },
            "table": "ns.table"
        }"#;

        let config: IcebergGsConfig = serde_json::from_str(json).unwrap();
        match &config.catalog {
            CatalogConfig::Rest { uri, warehouse, .. } => {
                assert_eq!(uri, "https://polaris.example.com");
                assert_eq!(warehouse, &Some("wh1".to_string()));
            }
            other => panic!("Expected Rest variant, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_tagged_direct_config() {
        let json = r#"{
            "catalog": {
                "type": "direct",
                "table_location": "s3://bucket/warehouse/ns/table"
            },
            "table": "",
            "io": {
                "vended_credentials": false
            }
        }"#;

        let config: IcebergGsConfig = serde_json::from_str(json).unwrap();
        match &config.catalog {
            CatalogConfig::Direct { table_location } => {
                assert_eq!(table_location, "s3://bucket/warehouse/ns/table");
            }
            other => panic!("Expected Direct variant, got {other:?}"),
        }
        // Table identifier derived from path
        let table_id = config.table_identifier().unwrap();
        assert_eq!(table_id.namespace, "ns");
        assert_eq!(table_id.table, "table");
    }

    // ── Validation ──

    #[test]
    fn test_validate_rest_missing_uri() {
        let config = IcebergGsConfig {
            catalog: CatalogConfig::Rest {
                catalog_type: "polaris".to_string(),
                uri: String::new(),
                auth: AuthConfig::None,
                warehouse: None,
            },
            table: TableConfig::Identifier("ns.table".to_string()),
            io: IoConfig::default(),
            mapping: None,
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("uri"));
    }

    #[test]
    fn test_validate_rest_invalid_table_id() {
        let config = IcebergGsConfig {
            catalog: CatalogConfig::rest("https://polaris.example.com"),
            table: TableConfig::Identifier("invalid".to_string()),
            io: IoConfig::default(),
            mapping: None,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_direct_empty_location() {
        let config = IcebergGsConfig {
            catalog: CatalogConfig::Direct {
                table_location: String::new(),
            },
            table: TableConfig::Identifier(String::new()),
            io: IoConfig {
                vended_credentials: false,
                ..Default::default()
            },
            mapping: None,
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("table_location"));
    }

    #[test]
    fn test_validate_direct_non_s3_uri() {
        let config = IcebergGsConfig {
            catalog: CatalogConfig::direct("https://not-s3.example.com/table"),
            table: TableConfig::Identifier(String::new()),
            io: IoConfig {
                vended_credentials: false,
                ..Default::default()
            },
            mapping: None,
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("S3 URI"));
    }

    #[test]
    fn test_validate_direct_rejects_vended_credentials() {
        let config = IcebergGsConfig {
            catalog: CatalogConfig::direct("s3://bucket/warehouse/ns/table"),
            table: TableConfig::Identifier(String::new()),
            io: IoConfig {
                vended_credentials: true,
                ..Default::default()
            },
            mapping: None,
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Vended credentials"));
    }

    // ── Roundtrip serialization ──

    #[test]
    fn test_roundtrip_rest() {
        let original = IcebergGsConfig {
            catalog: CatalogConfig::Rest {
                catalog_type: "polaris".to_string(),
                uri: "https://polaris.example.com".to_string(),
                auth: AuthConfig::None,
                warehouse: None,
            },
            table: TableConfig::Identifier("ns.table".to_string()),
            io: IoConfig::default(),
            mapping: None,
        };

        let json = original.to_json().unwrap();
        let parsed = IcebergGsConfig::from_json(&json).unwrap();
        assert_eq!(parsed.catalog, original.catalog);
        assert_eq!(parsed.table.identifier(), original.table.identifier());
    }

    #[test]
    fn test_roundtrip_direct() {
        let original = IcebergGsConfig {
            catalog: CatalogConfig::direct("s3://bucket/warehouse/ns/table"),
            table: TableConfig::Identifier(String::new()),
            io: IoConfig {
                vended_credentials: false,
                ..Default::default()
            },
            mapping: None,
        };

        let json = original.to_json().unwrap();
        let parsed = IcebergGsConfig::from_json(&json).unwrap();
        assert_eq!(parsed.catalog, original.catalog);
    }

    // ── CatalogConfig helpers ──

    #[test]
    fn test_direct_strips_trailing_slash() {
        let config = CatalogConfig::direct("s3://bucket/table/");
        match config {
            CatalogConfig::Direct { table_location } => {
                assert_eq!(table_location, "s3://bucket/table");
            }
            _ => panic!("Expected Direct"),
        }
    }

    #[test]
    fn test_catalog_config_direct_serde_roundtrip() {
        let config = CatalogConfig::direct("s3://bucket/warehouse/ns/table");
        let json = serde_json::to_string(&config).unwrap();
        let parsed: CatalogConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, parsed);
    }

    #[test]
    fn test_catalog_config_rest_backward_compat() {
        // Old flat format (no "type" field) should deserialize as Rest
        let old_json = r#"{"uri": "https://polaris.example.com"}"#;
        let parsed: CatalogConfig = serde_json::from_str(old_json).unwrap();
        assert!(matches!(parsed, CatalogConfig::Rest { .. }));
    }
}
