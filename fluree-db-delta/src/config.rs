//! Persisted configuration of a Delta graph source.

use std::collections::BTreeMap;

use std::sync::Arc;

use fluree_db_iceberg::auth::AuthConfig;
use fluree_db_iceberg::config::MappingSource;
use fluree_db_iceberg::{ConfigValue, SecretResolver};
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

    /// Databricks Unity Catalog. Tables without a `tables` entry are named
    /// (`catalog.schema.table`), and Unity says where each one lives and
    /// issues the credentials that read it. Excludes `root`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unity: Option<UnityConfig>,

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

/// A Unity Catalog workspace and how to authenticate to it.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UnityConfig {
    /// The workspace URL, e.g. `https://<workspace>.cloud.databricks.com`.
    pub uri: String,
    /// A bearer token, or OAuth2 client credentials of a service principal.
    pub auth: AuthConfig,
    /// Completes a mapped table name of fewer than three parts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
}

impl UnityConfig {
    /// Resolve secret references; see [`DeltaIoConfig::hydrate`].
    pub async fn hydrate(&self, resolver: Option<&Arc<dyn SecretResolver>>) -> Result<Self> {
        let mut hydrated = self.clone();
        hydrated.auth = self
            .auth
            .hydrate(resolver)
            .await
            .map_err(|e| DeltaError::Config(format!("Unity Catalog auth: {e}")))?;
        Ok(hydrated)
    }

    /// `table_name` as Unity knows it: `catalog.schema.table`.
    pub fn full_name(&self, table_name: &str) -> Result<String> {
        let parts: Vec<&str> = table_name.split('.').collect();
        let unplaceable = |missing: &str| {
            DeltaError::Config(format!(
                "table '{table_name}' needs a {missing}: write it as catalog.schema.table \
                 or give the source a default"
            ))
        };
        if parts.iter().any(|p| p.trim().is_empty()) || parts.len() > 3 {
            return Err(DeltaError::Config(format!(
                "table '{table_name}' is not a catalog.schema.table name"
            )));
        }
        let catalog = || {
            self.catalog
                .as_deref()
                .ok_or_else(|| unplaceable("catalog"))
        };
        let schema = || self.schema.as_deref().ok_or_else(|| unplaceable("schema"));
        Ok(match parts.as_slice() {
            [table] => format!("{}.{}.{table}", catalog()?, schema()?),
            [schema, table] => format!("{}.{schema}.{table}", catalog()?),
            _ => table_name.to_string(),
        })
    }

    fn validate(&self) -> Result<()> {
        fluree_db_iceberg::net::validate_public_url(&self.uri)
            .map_err(|e| DeltaError::Config(format!("Unity Catalog uri: {e}")))?;
        match &self.auth {
            AuthConfig::Bearer { .. } => Ok(()),
            AuthConfig::OAuth2ClientCredentials { token_url, .. } => {
                fluree_db_iceberg::net::validate_public_url(token_url)
                    .map_err(|e| DeltaError::Config(format!("Unity Catalog token URL: {e}")))
            }
            _ => Err(DeltaError::Config(
                "Unity Catalog needs a bearer token or OAuth2 client credentials".to_string(),
            )),
        }
    }
}

/// Where a mapped table is found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placement {
    /// A table directory, read with the process's own credentials.
    Path(String),
    /// A Unity Catalog table, by its three-part name.
    Unity(String),
}

/// Object-store options. S3 credentials always come from the ambient AWS
/// chain. Azure credentials come from the ambient chain (`AZURE_*` environment
/// variables, workload identity, managed identity) unless `azure` names a
/// service principal.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct DeltaIoConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3_region: Option<String>,
    /// Endpoint override (MinIO, LocalStack, …).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3_endpoint: Option<String>,
    #[serde(default)]
    pub s3_path_style: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub azure: Option<AzureAuth>,
}

/// Explicit Azure credentials for ADLS Gen2 and OneLake locations.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AzureAuth {
    /// Microsoft Entra service principal (OAuth2 client credentials). The
    /// token is requested for the storage audience and refreshed by the store.
    ClientSecret {
        tenant_id: String,
        client_id: String,
        /// Prefer an environment or secret reference: a literal is persisted
        /// with the graph source.
        client_secret: ConfigValue,
    },
}

impl DeltaIoConfig {
    /// Resolve secret references so [`crate::DeltaTable::open`] can read the
    /// config synchronously. Key any cache on the config as stored, not on
    /// this result, or every secret rotation re-keys it.
    pub async fn hydrate(&self, resolver: Option<&Arc<dyn SecretResolver>>) -> Result<Self> {
        let mut hydrated = self.clone();
        if let Some(AzureAuth::ClientSecret { client_secret, .. }) = &mut hydrated.azure {
            *client_secret = client_secret
                .hydrate(resolver)
                .await
                .map_err(|e| DeltaError::Config(format!("Azure client secret: {e}")))?;
        }
        Ok(hydrated)
    }
}

impl DeltaGsConfig {
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json).map_err(|e| DeltaError::Config(e.to_string()))
    }

    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|e| DeltaError::Config(e.to_string()))
    }

    pub fn validate(&self) -> Result<()> {
        if self.root.is_none() && self.tables.is_empty() && self.unity.is_none() {
            return Err(DeltaError::Config(
                "a Delta graph source needs a `root` location, explicit `tables` or a \
                 `unity` catalog"
                    .to_string(),
            ));
        }
        if let Some(unity) = &self.unity {
            if self.root.is_some() {
                return Err(DeltaError::Config(
                    "a Delta graph source resolves unlisted tables under `root` or through \
                     `unity`, not both"
                        .to_string(),
                ));
            }
            unity.validate()?;
        }
        for location in self.root.iter().chain(self.tables.values()) {
            validate_location(location)?;
        }
        if let Some(AzureAuth::ClientSecret {
            tenant_id,
            client_id,
            ..
        }) = &self.io.azure
        {
            if tenant_id.trim().is_empty() || client_id.trim().is_empty() {
                return Err(DeltaError::Config(
                    "Azure client-secret auth needs a tenant_id and a client_id".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Where `table_name` is found: its `tables` entry, else the catalog, else
    /// beneath `root`.
    pub fn placement(&self, table_name: &str) -> Result<Placement> {
        match &self.unity {
            Some(unity) if !self.tables.contains_key(table_name) => {
                unity.full_name(table_name).map(Placement::Unity)
            }
            _ => self.table_location(table_name).map(Placement::Path),
        }
    }

    /// The path of a table that is addressed by path.
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

/// Which store serves a location.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocationKind {
    Local,
    S3,
    Azure,
}

/// Azure hosts a location may name. Requiring one keeps a stored location from
/// steering credentials to an arbitrary endpoint.
const AZURE_HOST_SUFFIXES: [&str; 2] = [".dfs.core.windows.net", ".dfs.fabric.microsoft.com"];

/// Schemes the reader can open. Local locations must also sit under the
/// operator's `FLUREE_ICEBERG_LOCAL_ROOTS` allowlist, shared with Iceberg.
pub(crate) fn validate_location(location: &str) -> Result<LocationKind> {
    if fluree_db_iceberg::is_local_location(location) {
        return fluree_db_iceberg::ensure_local_location_allowed(location)
            .map(|()| LocationKind::Local)
            .map_err(|e| DeltaError::Config(e.to_string()));
    }
    match location.split_once("://") {
        Some(("s3" | "s3a", rest)) if !rest.is_empty() => Ok(LocationKind::S3),
        // `abfss://<container>@<account>.dfs.core.windows.net/<path>`, or the
        // OneLake form `abfss://<workspace>@onelake.dfs.fabric.microsoft.com/…`.
        Some(("abfss", rest)) => {
            let authority = rest.split('/').next().unwrap_or("");
            let well_formed = authority.split_once('@').is_some_and(|(container, host)| {
                !container.is_empty()
                    && AZURE_HOST_SUFFIXES
                        .iter()
                        .any(|suffix| host.len() > suffix.len() && host.ends_with(suffix))
            });
            if well_formed {
                Ok(LocationKind::Azure)
            } else {
                Err(DeltaError::Config(format!(
                    "unsupported Azure location '{location}': expected \
                     abfss://<container>@<account>.dfs.core.windows.net/<path> or \
                     abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<item>/<path>"
                )))
            }
        }
        _ => Err(DeltaError::Config(format!(
            "unsupported Delta table location '{location}': expected s3://, abfss://, \
             file:// or an absolute path"
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

    fn unity(catalog: Option<&str>, schema: Option<&str>) -> UnityConfig {
        UnityConfig {
            uri: "https://workspace.example.com".to_string(),
            auth: AuthConfig::Bearer {
                token: ConfigValue::literal("t"),
            },
            catalog: catalog.map(str::to_string),
            schema: schema.map(str::to_string),
        }
    }

    #[test]
    fn a_short_name_is_completed_from_the_sources_defaults() {
        let both = unity(Some("main"), Some("sales"));
        assert_eq!(both.full_name("orders").unwrap(), "main.sales.orders");
        assert_eq!(both.full_name("hr.people").unwrap(), "main.hr.people");
        assert_eq!(both.full_name("dev.hr.people").unwrap(), "dev.hr.people");

        let bare = unity(None, None);
        assert_eq!(bare.full_name("dev.hr.people").unwrap(), "dev.hr.people");
        for (config, name, missing) in [
            (&bare, "hr.people", "needs a catalog"),
            (&unity(Some("main"), None), "people", "needs a schema"),
            (&unity(None, Some("hr")), "people", "needs a catalog"),
        ] {
            let said = config.full_name(name).unwrap_err().to_string();
            assert!(said.contains(missing), "{said}");
        }
        for name in ["", "a..b", "a.b.c.d", ".b.c"] {
            assert!(both.full_name(name).is_err(), "{name:?} was placed");
        }
    }

    #[test]
    fn a_listed_table_is_read_by_path_and_the_rest_through_unity() {
        let config = DeltaGsConfig {
            unity: Some(unity(Some("main"), None)),
            tables: BTreeMap::from([("raw.events".to_string(), "s3://lake/events".to_string())]),
            ..Default::default()
        };
        config.validate().unwrap();
        assert_eq!(
            config.placement("raw.events").unwrap(),
            Placement::Path("s3://lake/events".to_string())
        );
        assert_eq!(
            config.placement("sales.orders").unwrap(),
            Placement::Unity("main.sales.orders".to_string())
        );
        // Without a catalog, placement is what it always was.
        assert_eq!(
            rooted("s3://lake/Tables").placement("dbo.orders").unwrap(),
            Placement::Path("s3://lake/Tables/dbo/orders".to_string())
        );
    }

    #[test]
    fn a_unity_source_is_checked_as_it_is_stored() {
        let with = |unity: UnityConfig| DeltaGsConfig {
            unity: Some(unity),
            ..Default::default()
        };
        with(unity(None, None)).validate().unwrap();

        let mut rooted_too = with(unity(None, None));
        rooted_too.root = Some("s3://lake/Tables".to_string());
        assert!(rooted_too
            .validate()
            .unwrap_err()
            .to_string()
            .contains("not both"));

        let mut unauthenticated = unity(None, None);
        unauthenticated.auth = AuthConfig::None;
        assert!(with(unauthenticated).validate().is_err());

        let mut internal_token_url = unity(None, None);
        internal_token_url.auth = AuthConfig::OAuth2ClientCredentials {
            token_url: "http://127.0.0.1/token".to_string(),
            client_id: ConfigValue::literal("app"),
            client_secret: ConfigValue::literal("s"),
            scope: None,
            audience: None,
        };
        assert!(with(internal_token_url)
            .validate()
            .unwrap_err()
            .to_string()
            .contains("token URL"));

        let mut internal = unity(None, None);
        internal.uri = "http://169.254.169.254".to_string();
        assert!(with(internal)
            .validate()
            .unwrap_err()
            .to_string()
            .contains("SSRF"));
    }

    #[test]
    fn a_unity_block_round_trips_without_its_secret_when_named_by_variable() {
        let mut config = DeltaGsConfig {
            unity: Some(unity(Some("main"), None)),
            ..Default::default()
        };
        config.unity.as_mut().unwrap().auth = AuthConfig::Bearer {
            token: ConfigValue::from_env("DATABRICKS_TOKEN"),
        };
        let json = config.to_json().unwrap();
        assert!(json.contains("\"env_var\":\"DATABRICKS_TOKEN\""), "{json}");
        let back = DeltaGsConfig::from_json(&json).unwrap();
        assert_eq!(back.unity.unwrap().catalog.as_deref(), Some("main"));
        // A source stored before catalogs existed still reads.
        assert!(DeltaGsConfig::from_json(r#"{"root":"s3://lake/Tables"}"#)
            .unwrap()
            .unity
            .is_none());
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
    fn azure_locations_must_name_a_storage_host() {
        for good in [
            "abfss://lake@acct.dfs.core.windows.net/Tables",
            "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/item-guid/Tables",
        ] {
            assert_eq!(
                validate_location(good).unwrap(),
                LocationKind::Azure,
                "{good}"
            );
        }
        for bad in [
            // fsspec form: the account would come from the environment.
            "abfss://lake/Tables",
            "abfss://lake@evil.example.com/Tables",
            "abfss://lake@.dfs.core.windows.net/Tables",
            "abfss://@acct.dfs.core.windows.net/Tables",
            "abfss://lake@acct.dfs.core.windows.net.evil.example/Tables",
            // Cleartext.
            "abfs://lake@acct.dfs.core.windows.net/Tables",
        ] {
            assert!(validate_location(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_service_principal_needs_its_identifiers_and_keeps_its_secret_out_of_debug() {
        let mut config = rooted("abfss://lake@acct.dfs.core.windows.net/Tables");
        config.io.azure = Some(AzureAuth::ClientSecret {
            tenant_id: String::new(),
            client_id: "app".to_string(),
            client_secret: ConfigValue::literal("hunter2"),
        });
        assert!(config.validate().is_err());
        if let Some(AzureAuth::ClientSecret { tenant_id, .. }) = &mut config.io.azure {
            *tenant_id = "tenant".to_string();
        }
        config.validate().unwrap();
        assert!(!format!("{config:?}").contains("hunter2"));

        let json = config.to_json().unwrap();
        let back = DeltaGsConfig::from_json(&json).unwrap();
        assert!(matches!(
            back.io.azure,
            Some(AzureAuth::ClientSecret { ref client_id, .. }) if client_id == "app"
        ));
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
