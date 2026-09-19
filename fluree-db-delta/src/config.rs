//! Persisted configuration of a Delta graph source.

use std::collections::BTreeMap;

use fluree_db_iceberg::config::MappingSource;
use serde::{Deserialize, Serialize};

use crate::error::{DeltaError, Result};

/// The `config` JSON of a `GraphSourceType::Delta` nameservice record.
///
/// A source names its tables by path. `rr:tableName` values in the mapping
/// resolve through [`Self::table_location`]: an explicit `tables` entry wins,
/// otherwise the name's dot-separated segments become directories under
/// `root` (`dbo.orders` → `<root>/dbo/orders`, the OneLake `Tables/` layout).
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct DeltaGsConfig {
    /// Directory the mapping's table names resolve beneath.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<String>,

    /// Explicit table name → table location, for tables outside `root` or
    /// whose directory does not follow the dotted-name layout.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tables: BTreeMap<String, String>,

    #[serde(default)]
    pub io: DeltaIoConfig,

    /// R2RML mapping source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mapping: Option<MappingSource>,

    /// Model ledger supplying policy and schema for governed reads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    /// Default-allow for policy evaluation when the request leaves it unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_allow: Option<bool>,
}

/// Object-store options. Credentials come from the ambient provider chain of
/// the target store; nothing secret is persisted here.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct DeltaIoConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3_region: Option<String>,
    /// Endpoint override (MinIO, LocalStack, …).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3_endpoint: Option<String>,
    #[serde(default)]
    pub s3_path_style: bool,
}

impl DeltaGsConfig {
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json).map_err(|e| DeltaError::Config(e.to_string()))
    }

    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|e| DeltaError::Config(e.to_string()))
    }

    pub fn validate(&self) -> Result<()> {
        if self.root.is_none() && self.tables.is_empty() {
            return Err(DeltaError::Config(
                "a Delta graph source needs a `root` location or explicit `tables`".to_string(),
            ));
        }
        for location in self.root.iter().chain(self.tables.values()) {
            validate_location(location)?;
        }
        Ok(())
    }

    /// Where `table_name` lives.
    pub fn table_location(&self, table_name: &str) -> Result<String> {
        if let Some(location) = self.tables.get(table_name) {
            return Ok(location.clone());
        }
        let root = self.root.as_deref().ok_or_else(|| {
            DeltaError::Config(format!(
                "table '{table_name}' has no `tables` entry and the source has no `root`"
            ))
        })?;
        let segments: Vec<&str> = table_name.split('.').collect();
        if segments
            .iter()
            .any(|s| s.is_empty() || *s == ".." || s.contains(['/', '\\']))
        {
            return Err(DeltaError::Config(format!(
                "table name '{table_name}' cannot be resolved beneath `root`; \
                 give it an explicit `tables` entry"
            )));
        }
        Ok(format!(
            "{}/{}",
            root.trim_end_matches('/'),
            segments.join("/")
        ))
    }
}

/// Schemes the reader can open. Local locations must also sit under the
/// operator's `FLUREE_ICEBERG_LOCAL_ROOTS` allowlist, shared with Iceberg.
pub(crate) fn validate_location(location: &str) -> Result<()> {
    if fluree_db_iceberg::is_local_location(location) {
        return fluree_db_iceberg::ensure_local_location_allowed(location)
            .map_err(|e| DeltaError::Config(e.to_string()));
    }
    match location.split_once("://") {
        Some(("s3" | "s3a", rest)) if !rest.is_empty() => Ok(()),
        _ => Err(DeltaError::Config(format!(
            "unsupported Delta table location '{location}': expected s3://, file:// \
             or an absolute path"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rooted(root: &str) -> DeltaGsConfig {
        DeltaGsConfig {
            root: Some(root.to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn dotted_names_resolve_to_directories_under_root() {
        let config = rooted("s3://bucket/lake/Tables/");
        assert_eq!(
            config.table_location("dbo.orders").unwrap(),
            "s3://bucket/lake/Tables/dbo/orders"
        );
        assert_eq!(
            config.table_location("orders").unwrap(),
            "s3://bucket/lake/Tables/orders"
        );
    }

    #[test]
    fn explicit_entry_wins_and_needs_no_root() {
        let mut config = rooted("s3://bucket/lake");
        config
            .tables
            .insert("orders".to_string(), "s3://other/x.y".to_string());
        assert_eq!(config.table_location("orders").unwrap(), "s3://other/x.y");
        config.root = None;
        assert_eq!(config.table_location("orders").unwrap(), "s3://other/x.y");
        assert!(config.table_location("missing").is_err());
    }

    #[test]
    fn names_that_would_escape_root_are_refused() {
        let config = rooted("s3://bucket/lake");
        for name in ["..", "a..b", "a/b", "../x", "", "a.", r"a\b"] {
            assert!(config.table_location(name).is_err(), "{name:?}");
        }
    }

    #[test]
    fn validate_requires_a_location_with_a_supported_scheme() {
        assert!(DeltaGsConfig::default().validate().is_err());
        assert!(rooted("s3://bucket/lake").validate().is_ok());
        for bad in ["http://host/lake", "lake", "s3://"] {
            assert!(rooted(bad).validate().is_err(), "{bad}");
        }
    }

    #[test]
    fn config_round_trips_without_empty_fields() {
        let json = rooted("s3://bucket/lake").to_json().unwrap();
        assert_eq!(
            json,
            r#"{"root":"s3://bucket/lake","io":{"s3_path_style":false}}"#
        );
        assert_eq!(
            DeltaGsConfig::from_json(&json).unwrap().root.as_deref(),
            Some("s3://bucket/lake")
        );
    }
}
