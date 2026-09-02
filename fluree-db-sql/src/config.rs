//! Graph-source configuration for a SQL source.
//!
//! Stored as the opaque `config` JSON of a `f:SqlMapping` nameservice record.
//! Everything reachable over the wire — endpoint, catalog/schema defaults,
//! credentials — lives here; the R2RML mapping itself is stored in CAS and only
//! referenced (`mapping.source` is a CID), exactly as for Iceberg sources.

use std::collections::BTreeMap;
use std::sync::Arc;

use fluree_db_iceberg::auth::AuthConfig;
use fluree_db_iceberg::config::MappingSource;
use fluree_db_iceberg::SecretResolver;
use serde::{Deserialize, Serialize};

use crate::dialect::SqlDialect;
use crate::error::{Result, SqlError};

/// Which header family the endpoint speaks. Trino renamed its headers from
/// `X-Presto-*` to `X-Trino-*` in release 351; PrestoDB still uses the old
/// names. Everything else about the protocol is identical.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WireProtocol {
    #[default]
    Trino,
    Presto,
}

impl WireProtocol {
    pub(crate) fn header(self, suffix: &str) -> String {
        match self {
            WireProtocol::Trino => format!("X-Trino-{suffix}"),
            WireProtocol::Presto => format!("X-Presto-{suffix}"),
        }
    }
}

fn default_request_timeout() -> u64 {
    120
}

fn default_user() -> String {
    "fluree".to_string()
}

/// Persisted configuration of one SQL graph source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlGsConfig {
    /// Base URL of the statement endpoint, e.g. `https://trino.example.com:8443`
    /// or `http://localhost:8080` for a sidecar. `/v1/statement` is appended.
    pub endpoint: String,

    /// How identifiers and literals are rendered. Defaults to Trino; a
    /// `fluree-sql-bridge` sidecar in front of another engine reports its own.
    #[serde(default)]
    pub dialect: SqlDialect,

    /// Header family (`X-Trino-*` vs `X-Presto-*`).
    #[serde(default)]
    pub protocol: WireProtocol,

    /// Default catalog for unqualified table names (`X-Trino-Catalog`).
    #[serde(default)]
    pub catalog: Option<String>,

    /// Default schema for unqualified table names (`X-Trino-Schema`).
    #[serde(default)]
    pub schema: Option<String>,

    /// The `X-Trino-User` value. Required by the protocol even when a bearer
    /// token identifies the caller; defaults to `fluree`.
    #[serde(default = "default_user")]
    pub user: String,

    /// Endpoint authentication. Shares the Iceberg REST catalog's shape so the
    /// same `ConfigValue` indirection (`env_var`, `secret_ref`) applies.
    #[serde(default)]
    pub auth: AuthConfig,

    /// Session properties sent as `X-Trino-Session: k=v,k=v`.
    #[serde(default)]
    pub session: BTreeMap<String, String>,

    /// Per-request HTTP timeout (each page fetch is one request).
    #[serde(default = "default_request_timeout")]
    pub request_timeout_secs: u64,

    /// The R2RML mapping (CAS CID + media type). Absent only transiently.
    #[serde(default)]
    pub mapping: Option<MappingSource>,

    /// Optional model ledger (`name:branch`) whose default graph supplies this
    /// source's view policies and class/property hierarchy (see the Iceberg
    /// config's field of the same name).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    /// Optional `default-allow` for governed requests that match no policy
    /// (see the Iceberg config's field of the same name).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_allow: Option<bool>,

    /// Tables whose subject key columns were found non-unique by the last
    /// registration or `check` probe. The pushdown lane refuses a statement
    /// over them unless `allow_duplicate_subjects` is set, because a star
    /// over a repeated subject returns wrong multiplicities.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub duplicate_subject_tables: Vec<String>,

    /// Accept duplicate subject keys: the probe still warns, queries proceed.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_duplicate_subjects: bool,
}

impl SqlGsConfig {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            dialect: SqlDialect::default(),
            protocol: WireProtocol::default(),
            catalog: None,
            schema: None,
            user: default_user(),
            auth: AuthConfig::default(),
            session: BTreeMap::new(),
            request_timeout_secs: default_request_timeout(),
            mapping: None,
            model: None,
            default_allow: None,
            duplicate_subject_tables: Vec::new(),
            allow_duplicate_subjects: false,
        }
    }

    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json)
            .map_err(|e| SqlError::Config(format!("invalid config JSON: {e}")))
    }

    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|e| SqlError::Config(format!("serialize config: {e}")))
    }

    /// Structural validation: endpoint scheme/host and non-empty user.
    pub fn validate(&self) -> Result<()> {
        crate::net::validate_endpoint(&self.endpoint)?;
        if self.user.trim().is_empty() {
            return Err(SqlError::Config("user must not be empty".to_string()));
        }
        if self.request_timeout_secs == 0 {
            return Err(SqlError::Config(
                "request_timeout_secs must be positive".to_string(),
            ));
        }
        for key in self.session.keys() {
            if key.contains(',') || key.contains('=') {
                return Err(SqlError::Config(format!(
                    "session property name '{key}' may not contain ',' or '='"
                )));
            }
        }
        Ok(())
    }

    /// Resolve every `secret_ref` in the auth block. Fields carrying no secret
    /// reference clone through untouched.
    pub async fn hydrate(&self, resolver: Option<&Arc<dyn SecretResolver>>) -> Result<Self> {
        let auth = self.auth.hydrate(resolver).await?;
        Ok(Self {
            auth,
            ..self.clone()
        })
    }

    /// The endpoint with any trailing slash removed.
    pub fn endpoint_base(&self) -> &str {
        self.endpoint.trim_end_matches('/')
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_config_round_trips_with_defaults() {
        let cfg = SqlGsConfig::from_json(r#"{"endpoint":"http://localhost:8080"}"#).unwrap();
        assert_eq!(cfg.user, "fluree");
        assert_eq!(cfg.dialect, SqlDialect::Trino);
        assert_eq!(cfg.protocol, WireProtocol::Trino);
        assert_eq!(cfg.request_timeout_secs, 120);
        assert!(cfg.mapping.is_none());
        cfg.validate().unwrap();
        let back = SqlGsConfig::from_json(&cfg.to_json().unwrap()).unwrap();
        assert_eq!(back.endpoint, "http://localhost:8080");
    }

    #[test]
    fn full_config_parses() {
        let cfg = SqlGsConfig::from_json(
            r#"{
              "endpoint": "https://trino.example.com/",
              "dialect": "postgres",
              "protocol": "presto",
              "catalog": "pg",
              "schema": "public",
              "user": "svc",
              "auth": {"type": "bearer", "token": {"env_var": "TRINO_TOKEN"}},
              "session": {"query_max_run_time": "5m"},
              "mapping": {"source": "bafy...", "media_type": "text/turtle"}
            }"#,
        )
        .unwrap();
        assert_eq!(cfg.dialect, SqlDialect::Postgres);
        assert_eq!(cfg.protocol, WireProtocol::Presto);
        assert_eq!(cfg.endpoint_base(), "https://trino.example.com");
        assert!(matches!(cfg.auth, AuthConfig::Bearer { .. }));
        assert_eq!(cfg.session["query_max_run_time"], "5m");
        cfg.validate().unwrap();
    }

    #[test]
    fn validate_rejects_bad_scheme_and_empty_user() {
        let mut cfg = SqlGsConfig::new("ftp://x");
        assert!(cfg.validate().is_err());
        cfg = SqlGsConfig::new("http://x");
        cfg.user = " ".into();
        assert!(cfg.validate().is_err());
    }
}
