//! Configuration types for graph source creation.
//!
//! This module contains builder-style configuration structs for creating
//! different types of graph sources (BM25, Vector, Iceberg, R2RML).

use fluree_db_core::ledger_id::{format_ledger_id, DEFAULT_BRANCH};
use fluree_db_query::bm25::Bm25Config;
use serde_json::Value as JsonValue;

#[cfg(feature = "iceberg")]
use fluree_db_iceberg::IcebergGsConfig;

#[cfg(feature = "vector")]
use crate::search::SearchDeploymentConfig;
#[cfg(feature = "vector")]
use fluree_db_query::vector::DistanceMetric;

// =============================================================================
// BM25 Configuration
// =============================================================================

/// Configuration for creating a BM25 full-text search index.
#[derive(Debug, Clone)]
pub struct Bm25CreateConfig {
    /// Name for the graph source (e.g., "my-search")
    pub name: String,

    /// Branch name (defaults to "main")
    pub branch: Option<String>,

    /// Source ledger alias (e.g., "docs:main")
    pub ledger: String,

    /// Indexing query that defines what to index.
    ///
    /// The query must:
    /// - Include `@id` in the select to identify documents
    /// - Select properties whose text content should be indexed
    ///
    /// Example:
    /// ```json
    /// {
    ///   "@context": {"ex": "http://example.org/"},
    ///   "where": [{"@id": "?x", "@type": "ex:Article"}],
    ///   "select": {"?x": ["@id", "ex:title", "ex:content"]}
    /// }
    /// ```
    pub query: JsonValue,

    /// BM25 k1 parameter (term frequency saturation). Default: 1.2
    pub k1: Option<f64>,

    /// BM25 b parameter (document length normalization). Default: 0.75
    pub b: Option<f64>,
}

impl Bm25CreateConfig {
    /// Create a new config with minimal required fields.
    pub fn new(name: impl Into<String>, ledger: impl Into<String>, query: JsonValue) -> Self {
        Self {
            name: name.into(),
            branch: None,
            ledger: ledger.into(),
            query,
            k1: None,
            b: None,
        }
    }

    /// Set the branch name.
    pub fn with_branch(mut self, branch: impl Into<String>) -> Self {
        self.branch = Some(branch.into());
        self
    }

    /// Set BM25 k1 parameter.
    pub fn with_k1(mut self, k1: f64) -> Self {
        self.k1 = Some(k1);
        self
    }

    /// Set BM25 b parameter.
    pub fn with_b(mut self, b: f64) -> Self {
        self.b = Some(b);
        self
    }

    /// Get the effective branch name.
    pub fn effective_branch(&self) -> &str {
        self.branch.as_deref().unwrap_or(DEFAULT_BRANCH)
    }

    /// Get the graph source ID (name:branch).
    pub fn graph_source_id(&self) -> String {
        format_ledger_id(&self.name, self.effective_branch())
    }

    /// Build BM25Config from the options.
    pub fn bm25_config(&self) -> Bm25Config {
        Bm25Config::new(self.k1.unwrap_or(1.2), self.b.unwrap_or(0.75))
    }

    /// Validate the configuration.
    ///
    /// Returns an error if any configuration values are invalid.
    ///
    /// # Validation Rules
    ///
    /// - `name` must not be empty
    /// - `ledger` must not be empty
    /// - `k1` must be positive (if specified)
    /// - `b` must be between 0 and 1 (if specified)
    /// - `query` must have a "select" clause
    pub fn validate(&self) -> crate::Result<()> {
        // Validate name
        if self.name.trim().is_empty() {
            return Err(crate::ApiError::config("Graph source name cannot be empty"));
        }

        // Validate name format (no colons allowed - reserved for alias)
        if self.name.contains(':') {
            return Err(crate::ApiError::config(
                "Graph source name cannot contain ':' (use branch for versioning)",
            ));
        }

        // Validate ledger alias
        if self.ledger.trim().is_empty() {
            return Err(crate::ApiError::config("Source ledger cannot be empty"));
        }

        // Validate k1
        if let Some(k1) = self.k1 {
            if k1 <= 0.0 {
                return Err(crate::ApiError::config(format!(
                    "k1 must be positive, got {k1}"
                )));
            }
            if k1 > 10.0 {
                // Warn but don't error - unusual but valid
                tracing::warn!(k1 = k1, "Unusually high k1 value (typical: 1.2-2.0)");
            }
        }

        // Validate b
        if let Some(b) = self.b {
            if !(0.0..=1.0).contains(&b) {
                return Err(crate::ApiError::config(format!(
                    "b must be between 0 and 1, got {b}"
                )));
            }
        }

        // Validate query structure
        if self.query.get("select").is_none() && self.query.get("selectOne").is_none() {
            return Err(crate::ApiError::config(
                "Indexing query must have a 'select' or 'selectOne' clause",
            ));
        }

        Ok(())
    }
}

// =============================================================================
// Vector Search Configuration
// =============================================================================

/// Configuration for creating a vector similarity search index.
///
/// Vector graph sources provide approximate nearest neighbor search using embedding vectors.
/// The index is built using HNSW and supports cosine, dot product,
/// and Euclidean distance metrics.
///
/// # Example
///
/// ```ignore
/// use fluree_db_api::VectorCreateConfig;
/// use fluree_db_query::vector::DistanceMetric;
///
/// let config = VectorCreateConfig::new(
///     "embeddings",
///     "docs:main",
///     json!({
///         "@context": {"ex": "http://example.org/"},
///         "where": [{"@id": "?doc", "@type": "ex:Article"}],
///         "select": {"?doc": ["@id", "ex:embedding"]}
///     }),
///     "ex:embedding",
///     768,
/// )
/// .with_metric(DistanceMetric::Cosine);
///
/// let result = fluree.create_vector_index(config).await?;
/// ```
#[cfg(feature = "vector")]
#[derive(Debug, Clone)]
pub struct VectorCreateConfig {
    /// Name for the graph source (e.g., "embeddings")
    pub name: String,

    /// Branch name (defaults to "main")
    pub branch: Option<String>,

    /// Source ledger alias (e.g., "docs:main")
    pub ledger: String,

    /// Indexing query that defines what to index.
    ///
    /// The query must:
    /// - Include `@id` in the select to identify documents
    /// - Select the embedding property
    pub query: JsonValue,

    /// Property path to the embedding vector (e.g., "ex:embedding")
    pub embedding_property: String,

    /// Expected vector dimensions (e.g., 768 for sentence transformers)
    pub dimensions: usize,

    /// Distance metric for similarity search. Default: Cosine
    pub metric: Option<DistanceMetric>,

    /// HNSW connectivity parameter (default: 16)
    /// Higher values give better recall but slower indexing
    pub connectivity: Option<usize>,

    /// Expansion factor during index construction (default: 128)
    pub expansion_add: Option<usize>,

    /// Expansion factor during search (default: 64)
    /// Higher values give better recall but slower search
    pub expansion_search: Option<usize>,

    /// Deployment configuration (embedded or remote).
    ///
    /// If `None`, defaults to embedded mode. Set to remote mode to delegate
    /// vector search to a remote search service via HTTP.
    pub deployment: Option<SearchDeploymentConfig>,
}

#[cfg(feature = "vector")]
impl VectorCreateConfig {
    /// Create a new config with minimal required fields.
    pub fn new(
        name: impl Into<String>,
        ledger: impl Into<String>,
        query: JsonValue,
        embedding_property: impl Into<String>,
        dimensions: usize,
    ) -> Self {
        Self {
            name: name.into(),
            branch: None,
            ledger: ledger.into(),
            query,
            embedding_property: embedding_property.into(),
            dimensions,
            metric: None,
            connectivity: None,
            expansion_add: None,
            expansion_search: None,
            deployment: None,
        }
    }

    /// Set the branch name.
    pub fn with_branch(mut self, branch: impl Into<String>) -> Self {
        self.branch = Some(branch.into());
        self
    }

    /// Set the distance metric.
    pub fn with_metric(mut self, metric: DistanceMetric) -> Self {
        self.metric = Some(metric);
        self
    }

    /// Set HNSW connectivity parameter.
    pub fn with_connectivity(mut self, connectivity: usize) -> Self {
        self.connectivity = Some(connectivity);
        self
    }

    /// Set expansion factor for index construction.
    pub fn with_expansion_add(mut self, expansion_add: usize) -> Self {
        self.expansion_add = Some(expansion_add);
        self
    }

    /// Set expansion factor for search.
    pub fn with_expansion_search(mut self, expansion_search: usize) -> Self {
        self.expansion_search = Some(expansion_search);
        self
    }

    /// Set the deployment configuration (embedded or remote).
    pub fn with_deployment(mut self, deployment: SearchDeploymentConfig) -> Self {
        self.deployment = Some(deployment);
        self
    }

    /// Get the effective branch name.
    pub fn effective_branch(&self) -> &str {
        self.branch.as_deref().unwrap_or(DEFAULT_BRANCH)
    }

    /// Get the graph source ID (name:branch).
    pub fn graph_source_id(&self) -> String {
        format_ledger_id(&self.name, self.effective_branch())
    }

    /// Get the effective distance metric.
    pub fn effective_metric(&self) -> DistanceMetric {
        self.metric.unwrap_or(DistanceMetric::Cosine)
    }

    /// Validate the configuration.
    ///
    /// # Validation Rules
    ///
    /// - `name` must not be empty
    /// - `name` must not contain ':'
    /// - `ledger` must not be empty
    /// - `embedding_property` must not be empty
    /// - `dimensions` must be positive
    /// - `query` must have a "select" clause
    pub fn validate(&self) -> crate::Result<()> {
        // Validate name
        if self.name.trim().is_empty() {
            return Err(crate::ApiError::config("Graph source name cannot be empty"));
        }

        if self.name.contains(':') {
            return Err(crate::ApiError::config(
                "Graph source name cannot contain ':' (use branch for versioning)",
            ));
        }

        // Validate ledger alias
        if self.ledger.trim().is_empty() {
            return Err(crate::ApiError::config("Source ledger cannot be empty"));
        }

        // Validate embedding property
        if self.embedding_property.trim().is_empty() {
            return Err(crate::ApiError::config(
                "Embedding property cannot be empty",
            ));
        }

        // Validate dimensions
        if self.dimensions == 0 {
            return Err(crate::ApiError::config(
                "Vector dimensions must be positive",
            ));
        }

        // Validate query structure
        if self.query.get("select").is_none() && self.query.get("selectOne").is_none() {
            return Err(crate::ApiError::config(
                "Indexing query must have a 'select' or 'selectOne' clause",
            ));
        }

        Ok(())
    }
}

// =============================================================================
// Iceberg Configuration
// =============================================================================

/// Configuration for creating an Iceberg graph source.
///
/// Iceberg graph sources provide access to Apache Iceberg tables stored in data lakes
/// (S3, GCS, etc.) via REST catalogs like Apache Polaris.
///
/// # Example
///
/// ```ignore
/// use fluree_db_api::IcebergCreateConfig;
///
/// let config = IcebergCreateConfig::new(
///     "openflights-gs",
///     "https://polaris.example.com",
///     "openflights.airlines",
/// )
/// .with_auth_bearer("my-token")
/// .with_warehouse("my-warehouse");
///
/// let result = fluree.create_iceberg_graph_source(config).await?;
/// ```
#[cfg(feature = "iceberg")]
#[derive(Debug, Clone)]
pub struct IcebergCreateConfig {
    /// Name for the graph source (e.g., "openflights-gs")
    pub name: String,

    /// Branch name (defaults to "main")
    pub branch: Option<String>,

    /// The reusable connection block (catalog access + IO).
    pub connection: IcebergConnectionConfig,

    /// Table identifier (e.g., "openflights.airlines"). Empty for Direct mode
    /// (derived from the table location).
    pub table_identifier: String,

    /// Optional tombstone/delete convention for materialization (which source
    /// column + value(s)/null mark a row as a delete). `None` => additive
    /// materialization (no retraction). Not a connection concern, so it lives on
    /// the create config rather than the reusable `IcebergConnectionConfig`.
    pub delete_convention: Option<fluree_db_iceberg::DeleteConvention>,

    /// Optional ordering column for latest-by-key materialization (e.g. an event
    /// timestamp or offset). `None` => last-in-scan-order wins.
    pub order_by: Option<String>,

    /// Optional model ledger (`name:branch`) governing the source: its default
    /// graph supplies view policies and the class/property hierarchy.
    pub model: Option<String>,

    /// Optional `default-allow` for governed requests that match no policy.
    pub default_allow: Option<bool>,
}

/// The reusable Iceberg connection block — catalog access + IO, with **no**
/// table or mapping.
///
/// Factored out of [`IcebergCreateConfig`] so catalog browse / metadata preview
/// can run against an **unsaved** connection during onboarding (before a graph
/// source record is created). The relationship is:
/// `IcebergCreateConfig` = `IcebergConnectionConfig` + `table_identifier`.
#[cfg(feature = "iceberg")]
#[derive(Debug, Clone)]
pub struct IcebergConnectionConfig {
    /// Catalog mode: REST or Direct S3 access.
    pub catalog_mode: CatalogMode,

    /// Storage / IO configuration (vended credentials + S3 region/endpoint/path-style).
    pub io: fluree_db_iceberg::config::IoConfig,
}

/// How the Iceberg catalog is accessed.
#[cfg(feature = "iceberg")]
#[derive(Debug, Clone)]
pub enum CatalogMode {
    /// Connect to a REST catalog at the given URI.
    Rest(Box<RestCatalogMode>),
    /// Read directly from a table location (no REST catalog): an S3 prefix,
    /// or a local path (`file://` URI / absolute path) for catalog-less
    /// tables on the local filesystem.
    Direct {
        /// Table root directory.
        /// Examples: "s3://bucket/warehouse/my_namespace/my_table",
        /// "file:///data/warehouse/my_namespace/my_table"
        table_location: String,
    },
    /// Resolve tables via the AWS Glue Data Catalog using the native AWS SDK.
    ///
    /// The Glue *database* is the table identifier's namespace. Credentials come
    /// from the ambient AWS credential chain — no REST/SigV4 or vended
    /// credentials are involved.
    Glue {
        /// AWS region. Falls back to the SDK default chain / `io.s3_region` if `None`.
        region: Option<String>,
        /// Glue catalog id for cross-account access (`None` = the caller's account).
        catalog_id: Option<String>,
    },
    /// Resolve tables via AWS S3 Tables using the native AWS SDK.
    ///
    /// Credentials come from the ambient AWS credential chain.
    S3Tables {
        /// AWS region. Falls back to the SDK default chain / `io.s3_region` if `None`.
        region: Option<String>,
        /// The S3 Tables table-bucket ARN
        /// (`arn:aws:s3tables:<region>:<account>:bucket/<name>`).
        table_bucket_arn: String,
    },
}

/// REST catalog mode configuration.
///
/// This carries only the catalog-connection fields (`catalog_uri` / `warehouse`
/// / `auth`); the table identifier lives on [`IcebergCreateConfig`] and
/// vended-credential / S3 IO settings live on
/// [`IcebergConnectionConfig::io`].
#[cfg(feature = "iceberg")]
#[derive(Debug, Clone)]
pub struct RestCatalogMode {
    /// REST catalog URI
    pub catalog_uri: String,
    /// Optional warehouse identifier
    pub warehouse: Option<String>,
    /// Authentication configuration
    pub auth: fluree_db_iceberg::auth::AuthConfig,
}

/// The catalog-mode fields every surface collects — CLI flags, the server's JSON
/// body — exactly as given. [`IcebergConnectionConfig::from_mode`] and
/// [`IcebergCreateConfig::from_mode`] parse them in ONE place, so a catalog mode
/// added there reaches every surface, with the same rules and messages.
#[cfg(feature = "iceberg")]
#[derive(Debug, Default, Clone, Copy)]
pub struct CatalogModeArgs<'a> {
    /// `rest` (also when empty), `direct`, `glue`, or `s3tables`; case-insensitive.
    pub mode: &'a str,
    /// REST catalog URI (`rest`).
    pub catalog_uri: Option<&'a str>,
    /// Table directory or warehouse root (`direct`).
    pub table_location: Option<&'a str>,
    /// AWS region of the catalog (`glue`, `s3tables`).
    pub region: Option<&'a str>,
    /// Glue catalog id for cross-account access (`glue`).
    pub catalog_id: Option<&'a str>,
    /// S3 Tables table-bucket ARN (`s3tables`).
    pub table_bucket_arn: Option<&'a str>,
    /// The REST-only settings (catalog auth, warehouse, a request for vended
    /// credentials) the caller was given, by field name. Any other mode refuses
    /// them rather than silently ignoring them.
    pub rest_only: &'a [&'static str],
}

/// Why catalog-mode arguments do not describe a source. Field names are the
/// snake_case wire names; [`CatalogModeError::message`] renders them in the
/// caller's own spelling (`--catalog-uri` on the CLI, `catalog_uri` in JSON).
#[cfg(feature = "iceberg")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogModeError {
    /// `mode` is not a catalog mode.
    UnknownMode(String),
    /// The mode needs `field`.
    MissingField {
        mode: &'static str,
        field: &'static str,
    },
    /// A catalog mode with no R2RML mapping needs the source's own `table`.
    MissingTable { mode: &'static str },
    /// `field` is a REST catalog setting, which this mode would ignore.
    NotForMode {
        mode: &'static str,
        field: &'static str,
    },
}

#[cfg(feature = "iceberg")]
impl CatalogModeError {
    /// The error message, with each field name rendered by `spell`.
    pub fn message(&self, spell: impl Fn(&str) -> String) -> String {
        match self {
            CatalogModeError::UnknownMode(mode) => format!(
                "unknown catalog mode '{mode}'. Use 'rest', 'direct', 'glue', or 's3tables'."
            ),
            CatalogModeError::MissingField { mode, field } => {
                format!("{} is required for {mode} mode", spell(field))
            }
            CatalogModeError::MissingTable { mode } => format!(
                "{} is required for {mode} mode (or provide {} to define tables via mapping)",
                spell("table"),
                spell("r2rml")
            ),
            CatalogModeError::NotForMode { mode, field } => format!(
                "{} applies to rest mode only; {mode} mode would ignore it",
                spell(field)
            ),
        }
    }
}

#[cfg(feature = "iceberg")]
impl IcebergConnectionConfig {
    /// Parse a catalog mode and its fields into a connection with default IO —
    /// the one dispatch every surface goes through (see [`CatalogModeArgs`]).
    pub fn from_mode(args: CatalogModeArgs<'_>) -> std::result::Result<Self, CatalogModeError> {
        let required = |value: Option<&str>, mode: &'static str, field: &'static str| {
            value
                .filter(|v| !v.trim().is_empty())
                .map(str::to_string)
                .ok_or(CatalogModeError::MissingField { mode, field })
        };
        let owned = |value: Option<&str>| value.map(str::to_string);
        let mode = if args.mode.is_empty() {
            "rest".to_string()
        } else {
            args.mode.to_lowercase()
        };
        let refuse_rest_only = |mode: &'static str| match args.rest_only.first().copied() {
            Some(field) => Err(CatalogModeError::NotForMode { mode, field }),
            None => Ok(()),
        };
        Ok(match mode.as_str() {
            "rest" => Self::rest(required(args.catalog_uri, "rest", "catalog_uri")?),
            "direct" => {
                refuse_rest_only("direct")?;
                Self::direct(required(args.table_location, "direct", "table_location")?)
            }
            "glue" => {
                refuse_rest_only("glue")?;
                Self::glue(owned(args.region), owned(args.catalog_id))
            }
            "s3tables" => {
                refuse_rest_only("s3tables")?;
                Self::s3_tables(
                    owned(args.region),
                    required(args.table_bucket_arn, "s3tables", "table_bucket_arn")?,
                )
            }
            _ => return Err(CatalogModeError::UnknownMode(args.mode.to_string())),
        })
    }

    /// Create a REST-catalog connection with default IO (vended credentials on).
    pub fn rest(catalog_uri: impl Into<String>) -> Self {
        Self {
            catalog_mode: CatalogMode::Rest(Box::new(RestCatalogMode {
                catalog_uri: catalog_uri.into(),
                warehouse: None,
                auth: fluree_db_iceberg::auth::AuthConfig::None,
            })),
            io: fluree_db_iceberg::config::IoConfig::default(),
        }
    }

    /// Create a Direct S3 connection (no REST catalog). Vended credentials are
    /// forced off — Direct mode uses ambient/IAM credentials.
    pub fn direct(table_location: impl Into<String>) -> Self {
        Self {
            catalog_mode: CatalogMode::Direct {
                table_location: table_location.into(),
            },
            io: fluree_db_iceberg::config::IoConfig {
                vended_credentials: false,
                ..Default::default()
            },
        }
    }

    /// Create an AWS Glue Data Catalog connection (native AWS SDK). Vended
    /// credentials are forced off — the SDK reads S3 with the ambient/IAM
    /// credential chain.
    pub fn glue(region: Option<String>, catalog_id: Option<String>) -> Self {
        Self {
            catalog_mode: CatalogMode::Glue { region, catalog_id },
            io: fluree_db_iceberg::config::IoConfig {
                vended_credentials: false,
                ..Default::default()
            },
        }
    }

    /// Create an AWS S3 Tables connection (native AWS SDK) from a table-bucket
    /// ARN. Vended credentials are forced off — the SDK reads the managed table
    /// bucket with the ambient/IAM credential chain.
    pub fn s3_tables(region: Option<String>, table_bucket_arn: impl Into<String>) -> Self {
        Self {
            catalog_mode: CatalogMode::S3Tables {
                region,
                table_bucket_arn: table_bucket_arn.into(),
            },
            io: fluree_db_iceberg::config::IoConfig {
                vended_credentials: false,
                ..Default::default()
            },
        }
    }

    /// Set bearer token authentication (REST mode only).
    pub fn with_auth_bearer(self, token: impl Into<String>) -> Self {
        self.with_auth_bearer_value(fluree_db_iceberg::ConfigValue::literal(token.into()))
    }

    /// Set bearer token authentication from a secret REFERENCE (REST mode only).
    ///
    /// `token_ref` is an opaque reference resolved at use time by the injected
    /// [`SecretResolver`](fluree_db_iceberg::SecretResolver) (see
    /// [`Fluree::with_secret_resolver`](crate::Fluree::with_secret_resolver)); the
    /// token value never appears in the stored config. Mirrors
    /// [`Self::with_auth_bearer`].
    pub fn with_auth_bearer_token_ref(self, token_ref: impl Into<String>) -> Self {
        self.with_auth_bearer_value(fluree_db_iceberg::ConfigValue::SecretRef {
            secret_ref: token_ref.into(),
        })
    }

    /// Set bearer token authentication (REST mode only), the token given as a
    /// literal, a secret reference, or the name of an environment variable of
    /// the process that reads the tables
    /// ([`ConfigValue::from_env`](fluree_db_iceberg::ConfigValue::from_env)).
    /// Only a literal is stored as the token itself.
    pub fn with_auth_bearer_value(mut self, token: fluree_db_iceberg::ConfigValue) -> Self {
        if let CatalogMode::Rest(ref mut rest) = self.catalog_mode {
            rest.auth = fluree_db_iceberg::auth::AuthConfig::Bearer { token };
        } else {
            tracing::warn!("bearer auth has no effect in Direct catalog mode");
        }
        self
    }

    /// Set Google metadata-server authentication (REST mode only).
    ///
    /// Mints and refreshes short-lived Google OAuth tokens from the GCE/GKE
    /// metadata server (Workload Identity) — for Google Iceberg REST catalogs
    /// (BigLake), where a static bearer expires after ~1h. `scopes` is optional
    /// (defaults to cloud-platform).
    pub fn with_auth_google_metadata(mut self, scopes: Option<String>) -> Self {
        if let CatalogMode::Rest(ref mut rest) = self.catalog_mode {
            rest.auth = fluree_db_iceberg::auth::AuthConfig::GoogleMetadata {
                scopes,
                metadata_url: None,
            };
        } else {
            tracing::warn!("with_auth_google_metadata has no effect in Direct catalog mode");
        }
        self
    }

    /// Set OAuth2 client credentials authentication (REST mode only).
    pub fn with_auth_oauth2(
        self,
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Self {
        self.with_auth_oauth2_secret_value(
            token_url,
            client_id,
            fluree_db_iceberg::ConfigValue::literal(client_secret.into()),
        )
    }

    /// Set OAuth2 client-credentials auth with the client secret supplied as a
    /// secret REFERENCE (REST mode only).
    ///
    /// `client_secret_ref` is an opaque reference resolved at use time by the
    /// injected [`SecretResolver`](fluree_db_iceberg::SecretResolver); the secret
    /// value never appears in the stored config. `token_url` and `client_id` are
    /// non-secret and stored literally. Scope/audience are set separately via
    /// [`Self::with_oauth2_scope`] / [`Self::with_oauth2_audience`] (call them
    /// AFTER this). Mirrors [`Self::with_auth_oauth2`].
    pub fn with_auth_oauth2_client_secret_ref(
        self,
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret_ref: impl Into<String>,
    ) -> Self {
        self.with_auth_oauth2_secret_value(
            token_url,
            client_id,
            fluree_db_iceberg::ConfigValue::SecretRef {
                secret_ref: client_secret_ref.into(),
            },
        )
    }

    /// Set OAuth2 client-credentials auth (REST mode only), the client secret
    /// given as a literal, a secret reference, or the name of an environment
    /// variable of the process that reads the tables. Scope and audience are
    /// set afterwards, as for [`Self::with_auth_oauth2`].
    pub fn with_auth_oauth2_secret_value(
        mut self,
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: fluree_db_iceberg::ConfigValue,
    ) -> Self {
        if let CatalogMode::Rest(ref mut rest) = self.catalog_mode {
            rest.auth = fluree_db_iceberg::auth::AuthConfig::OAuth2ClientCredentials {
                token_url: token_url.into(),
                client_id: fluree_db_iceberg::ConfigValue::literal(client_id.into()),
                client_secret,
                scope: None,
                audience: None,
            };
        } else {
            tracing::warn!("OAuth2 auth has no effect in Direct catalog mode");
        }
        self
    }

    /// Set the OAuth2 `scope` for client-credentials auth (REST + OAuth2 only).
    ///
    /// Mutates the existing OAuth2 auth config in place, so call this AFTER
    /// [`Self::with_auth_oauth2`]. It has no effect (and warns) if OAuth2
    /// client-credentials auth has not been configured. Required for
    /// scope-gated REST catalogs such as Snowflake Horizon / Apache Polaris,
    /// where the catalog session role is selected via
    /// `scope=session:role:<ROLE>`.
    pub fn with_oauth2_scope(mut self, scope: impl Into<String>) -> Self {
        match &mut self.catalog_mode {
            CatalogMode::Rest(rest) => {
                if let fluree_db_iceberg::auth::AuthConfig::OAuth2ClientCredentials {
                    scope: slot,
                    ..
                } = &mut rest.auth
                {
                    *slot = Some(scope.into());
                } else {
                    tracing::warn!(
                        "with_oauth2_scope has no effect unless OAuth2 client-credentials auth is set first (call with_auth_oauth2)"
                    );
                }
            }
            CatalogMode::Direct { .. } => {
                tracing::warn!("with_oauth2_scope has no effect in Direct catalog mode");
            }
            CatalogMode::Glue { .. } | CatalogMode::S3Tables { .. } => {
                tracing::warn!("with_oauth2_scope has no effect in Glue/S3Tables catalog mode");
            }
        }
        self
    }

    /// Set the OAuth2 `audience` for client-credentials auth (REST + OAuth2 only).
    ///
    /// Mutates the existing OAuth2 auth config in place, so call this AFTER
    /// [`Self::with_auth_oauth2`]. It has no effect (and warns) if OAuth2
    /// client-credentials auth has not been configured.
    pub fn with_oauth2_audience(mut self, audience: impl Into<String>) -> Self {
        match &mut self.catalog_mode {
            CatalogMode::Rest(rest) => {
                if let fluree_db_iceberg::auth::AuthConfig::OAuth2ClientCredentials {
                    audience: slot,
                    ..
                } = &mut rest.auth
                {
                    *slot = Some(audience.into());
                } else {
                    tracing::warn!(
                        "with_oauth2_audience has no effect unless OAuth2 client-credentials auth is set first (call with_auth_oauth2)"
                    );
                }
            }
            CatalogMode::Direct { .. } => {
                tracing::warn!("with_oauth2_audience has no effect in Direct catalog mode");
            }
            CatalogMode::Glue { .. } | CatalogMode::S3Tables { .. } => {
                tracing::warn!("with_oauth2_audience has no effect in Glue/S3Tables catalog mode");
            }
        }
        self
    }

    /// Set the warehouse identifier (REST mode only).
    pub fn with_warehouse(mut self, warehouse: impl Into<String>) -> Self {
        if let CatalogMode::Rest(ref mut rest) = self.catalog_mode {
            rest.warehouse = Some(warehouse.into());
        } else {
            tracing::warn!("with_warehouse has no effect in Direct catalog mode");
        }
        self
    }

    /// Enable or disable vended credentials (REST mode only).
    pub fn with_vended_credentials(mut self, enabled: bool) -> Self {
        if self.is_rest() {
            self.io.vended_credentials = enabled;
        } else {
            tracing::warn!("with_vended_credentials has no effect in Direct catalog mode");
        }
        self
    }

    /// Set S3 region.
    pub fn with_s3_region(mut self, region: impl Into<String>) -> Self {
        self.io.s3_region = Some(region.into());
        self
    }

    /// Set S3 endpoint (for MinIO, LocalStack).
    pub fn with_s3_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.io.s3_endpoint = Some(endpoint.into());
        self
    }

    /// Enable path-style S3 URLs.
    pub fn with_s3_path_style(mut self, enabled: bool) -> Self {
        self.io.s3_path_style = enabled;
        self
    }

    /// Get the catalog URI (for REST mode) or table location (for direct mode).
    pub fn catalog_uri_or_location(&self) -> &str {
        match &self.catalog_mode {
            CatalogMode::Rest(rest) => &rest.catalog_uri,
            CatalogMode::Direct { table_location } => table_location,
            CatalogMode::Glue { catalog_id, .. } => catalog_id.as_deref().unwrap_or("aws-glue"),
            CatalogMode::S3Tables {
                table_bucket_arn, ..
            } => table_bucket_arn,
        }
    }

    /// This connection's native-AWS catalog (Glue, S3 Tables) as the engine
    /// config models it, so region resolution and validation are the engine's
    /// own rules rather than a copy; `None` for REST and Direct.
    fn aws_sdk_catalog(&self) -> Option<fluree_db_iceberg::config::CatalogConfig> {
        use fluree_db_iceberg::config::CatalogConfig;
        match &self.catalog_mode {
            CatalogMode::Glue { region, catalog_id } => {
                Some(CatalogConfig::glue(region.clone(), catalog_id.clone()))
            }
            CatalogMode::S3Tables {
                region,
                table_bucket_arn,
            } => Some(CatalogConfig::s3_tables(
                region.clone(),
                table_bucket_arn.clone(),
            )),
            CatalogMode::Rest(_) | CatalogMode::Direct { .. } => None,
        }
    }

    /// The region an AWS Glue / S3 Tables catalog API is called in (see
    /// [`fluree_db_iceberg::config::CatalogConfig::aws_catalog_region`]), after
    /// validating the catalog's settings; `Ok(None)` defers to the AWS SDK's
    /// region chain, and is always the answer for REST and Direct.
    pub fn aws_catalog_region(&self) -> crate::Result<Option<String>> {
        let Some(catalog) = self.aws_sdk_catalog() else {
            return Ok(None);
        };
        let invalid = |e: fluree_db_iceberg::IcebergError| crate::ApiError::config(e.to_string());
        catalog.validate_aws_sdk_catalog().map_err(invalid)?;
        self.io.validate_s3_overrides().map_err(invalid)?;
        Ok(catalog.aws_catalog_region(&self.io).map(str::to_string))
    }

    /// The IO settings this connection's S3 reads use: for Glue / S3 Tables,
    /// `s3_region` falls back to the catalog's region (see
    /// [`fluree_db_iceberg::config::IoConfig::with_catalog_region`]).
    pub fn storage_io(&self) -> std::borrow::Cow<'_, fluree_db_iceberg::config::IoConfig> {
        let catalog_region = self
            .aws_sdk_catalog()
            .and_then(|c| c.aws_region().map(str::to_string));
        match catalog_region {
            Some(region) if self.io.s3_region.is_none() => {
                std::borrow::Cow::Owned(self.io.with_catalog_region(Some(&region)))
            }
            _ => std::borrow::Cow::Borrowed(&self.io),
        }
    }

    /// Returns `true` if this connection uses REST catalog mode.
    pub fn is_rest(&self) -> bool {
        matches!(self.catalog_mode, CatalogMode::Rest(_))
    }

    /// Returns `true` if this connection uses direct S3 catalog mode.
    pub fn is_direct(&self) -> bool {
        matches!(self.catalog_mode, CatalogMode::Direct { .. })
    }

    /// Returns `true` if this connection uses AWS Glue catalog mode.
    pub fn is_glue(&self) -> bool {
        matches!(self.catalog_mode, CatalogMode::Glue { .. })
    }

    /// Returns `true` if this connection uses AWS S3 Tables catalog mode.
    pub fn is_s3tables(&self) -> bool {
        matches!(self.catalog_mode, CatalogMode::S3Tables { .. })
    }
}

#[cfg(feature = "iceberg")]
impl IcebergCreateConfig {
    /// Create a new Iceberg graph source config for REST catalog mode.
    pub fn new(
        name: impl Into<String>,
        catalog_uri: impl Into<String>,
        table_identifier: impl Into<String>,
    ) -> Self {
        Self::from_connection(
            name,
            IcebergConnectionConfig::rest(catalog_uri),
            table_identifier,
        )
    }

    /// Create a new Iceberg graph source config for direct S3 access (no REST catalog).
    pub fn new_direct(name: impl Into<String>, table_location: impl Into<String>) -> Self {
        Self::from_connection(
            name,
            IcebergConnectionConfig::direct(table_location),
            String::new(),
        )
    }

    /// Create a graph source config over any connection (REST, Direct, AWS Glue,
    /// AWS S3 Tables). `table_identifier` is the source's own `namespace.table`
    /// (empty for Direct, whose table comes from its location). Every other field
    /// starts unset; this is the one place they are listed.
    pub fn from_connection(
        name: impl Into<String>,
        connection: IcebergConnectionConfig,
        table_identifier: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            branch: None,
            connection,
            table_identifier: table_identifier.into(),
            delete_convention: None,
            model: None,
            default_allow: None,
            order_by: None,
        }
    }

    /// Parse a catalog mode, its fields, and the source's own `table` into a
    /// create config — the one dispatch every surface goes through (see
    /// [`CatalogModeArgs`]). A catalog mode (rest, glue, s3tables) needs `table`
    /// unless the source's tables come from an R2RML mapping (`has_mapping`), in
    /// which case each `rr:tableName` names its table; Direct ignores `table`.
    pub fn from_mode(
        name: impl Into<String>,
        args: CatalogModeArgs<'_>,
        table: Option<&str>,
        has_mapping: bool,
    ) -> std::result::Result<Self, CatalogModeError> {
        let connection = IcebergConnectionConfig::from_mode(args)?;
        let mode = match &connection.catalog_mode {
            CatalogMode::Direct { .. } => {
                return Ok(Self::from_connection(name, connection, String::new()));
            }
            CatalogMode::Rest(_) => "rest",
            CatalogMode::Glue { .. } => "glue",
            CatalogMode::S3Tables { .. } => "s3tables",
        };
        let table = match table.map(str::trim).filter(|t| !t.is_empty()) {
            Some(table) => table,
            // A mapping-defined source has no table of its own; the placeholder
            // only satisfies the identifier shape and is never loaded.
            None if has_mapping => fluree_db_iceberg::config::MAPPING_DEFINED_TABLE,
            None => return Err(CatalogModeError::MissingTable { mode }),
        };
        Ok(Self::from_connection(name, connection, table))
    }

    /// Set the branch name.
    pub fn with_branch(mut self, branch: impl Into<String>) -> Self {
        self.branch = Some(branch.into());
        self
    }

    /// Set bearer token authentication (REST mode only).
    pub fn with_auth_bearer(mut self, token: impl Into<String>) -> Self {
        self.connection = self.connection.with_auth_bearer(token);
        self
    }

    /// Use the GCE/GKE metadata server for refreshable Google catalog auth
    /// (REST mode only). See [`IcebergConnectionConfig::with_auth_google_metadata`].
    pub fn with_auth_google_metadata(mut self, scopes: Option<String>) -> Self {
        self.connection = self.connection.with_auth_google_metadata(scopes);
        self
    }

    /// Set OAuth2 client credentials authentication (REST mode only).
    pub fn with_auth_oauth2(
        mut self,
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Self {
        self.connection = self
            .connection
            .with_auth_oauth2(token_url, client_id, client_secret);
        self
    }

    /// See [`IcebergConnectionConfig::with_auth_bearer_value`].
    pub fn with_auth_bearer_value(mut self, token: fluree_db_iceberg::ConfigValue) -> Self {
        self.connection = self.connection.with_auth_bearer_value(token);
        self
    }

    /// See [`IcebergConnectionConfig::with_auth_oauth2_secret_value`].
    pub fn with_auth_oauth2_secret_value(
        mut self,
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: fluree_db_iceberg::ConfigValue,
    ) -> Self {
        self.connection =
            self.connection
                .with_auth_oauth2_secret_value(token_url, client_id, client_secret);
        self
    }

    /// Set bearer authentication from a secret *reference* (REST mode only).
    /// See [`IcebergConnectionConfig::with_auth_bearer_token_ref`].
    pub fn with_auth_bearer_token_ref(mut self, token_ref: impl Into<String>) -> Self {
        self.connection = self.connection.with_auth_bearer_token_ref(token_ref);
        self
    }

    /// Set OAuth2 client-credentials authentication with the client secret as a
    /// secret *reference* (REST mode only). See
    /// [`IcebergConnectionConfig::with_auth_oauth2_client_secret_ref`].
    pub fn with_auth_oauth2_client_secret_ref(
        mut self,
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret_ref: impl Into<String>,
    ) -> Self {
        self.connection = self.connection.with_auth_oauth2_client_secret_ref(
            token_url,
            client_id,
            client_secret_ref,
        );
        self
    }

    /// Set the OAuth2 `scope` for client-credentials auth (REST + OAuth2 only).
    ///
    /// Call after [`Self::with_auth_oauth2`]. See
    /// [`IcebergConnectionConfig::with_oauth2_scope`].
    pub fn with_oauth2_scope(mut self, scope: impl Into<String>) -> Self {
        self.connection = self.connection.with_oauth2_scope(scope);
        self
    }

    /// Set the OAuth2 `audience` for client-credentials auth (REST + OAuth2 only).
    ///
    /// Call after [`Self::with_auth_oauth2`]. See
    /// [`IcebergConnectionConfig::with_oauth2_audience`].
    pub fn with_oauth2_audience(mut self, audience: impl Into<String>) -> Self {
        self.connection = self.connection.with_oauth2_audience(audience);
        self
    }

    /// Set the warehouse identifier (REST mode only).
    pub fn with_warehouse(mut self, warehouse: impl Into<String>) -> Self {
        self.connection = self.connection.with_warehouse(warehouse);
        self
    }

    /// Enable or disable vended credentials (REST mode only).
    pub fn with_vended_credentials(mut self, enabled: bool) -> Self {
        self.connection = self.connection.with_vended_credentials(enabled);
        self
    }

    /// Set S3 region.
    pub fn with_s3_region(mut self, region: impl Into<String>) -> Self {
        self.connection = self.connection.with_s3_region(region);
        self
    }

    /// Set S3 endpoint (for MinIO, LocalStack).
    pub fn with_s3_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.connection = self.connection.with_s3_endpoint(endpoint);
        self
    }

    /// Enable path-style S3 URLs.
    pub fn with_s3_path_style(mut self, enabled: bool) -> Self {
        self.connection = self.connection.with_s3_path_style(enabled);
        self
    }

    /// Set the tombstone/delete convention used during materialization.
    pub fn with_delete_convention(
        mut self,
        convention: fluree_db_iceberg::DeleteConvention,
    ) -> Self {
        self.delete_convention = Some(convention);
        self
    }

    /// Set the ordering column for latest-by-key materialization.
    pub fn with_order_by(mut self, column: impl Into<String>) -> Self {
        self.order_by = Some(column.into());
        self
    }

    /// Reference a model ledger whose default graph holds this source's view
    /// policies and `rdfs:subClassOf` / `rdfs:subPropertyOf` hierarchy.
    pub fn with_model(mut self, ledger: impl Into<String>) -> Self {
        self.model = Some(ledger.into());
        self
    }

    /// Declare the fallback for governed requests that match no policy: `true`
    /// keeps the source readable under authentication without a model.
    pub fn with_default_allow(mut self, allow: bool) -> Self {
        self.default_allow = Some(allow);
        self
    }

    /// Get the effective branch name.
    pub fn effective_branch(&self) -> &str {
        self.branch.as_deref().unwrap_or(DEFAULT_BRANCH)
    }

    /// Get the graph source ID (name:branch).
    pub fn graph_source_id(&self) -> String {
        format_ledger_id(&self.name, self.effective_branch())
    }

    /// Get the catalog URI (for REST mode) or table location (for direct mode).
    pub fn catalog_uri_or_location(&self) -> &str {
        self.connection.catalog_uri_or_location()
    }

    /// Get the table identifier string (for REST mode), or derive from location (for direct mode).
    pub fn table_identifier_display(&self) -> String {
        match &self.connection.catalog_mode {
            // REST / Glue / S3Tables all carry an explicit `namespace.table`.
            CatalogMode::Rest(_) | CatalogMode::Glue { .. } | CatalogMode::S3Tables { .. } => {
                self.table_identifier.clone()
            }
            CatalogMode::Direct { table_location } => {
                let path = table_location
                    .trim_start_matches("s3://")
                    .trim_start_matches("s3a://");
                let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
                if segments.len() >= 3 {
                    format!(
                        "{}.{}",
                        segments[segments.len() - 2],
                        segments[segments.len() - 1]
                    )
                } else {
                    table_location.clone()
                }
            }
        }
    }

    /// Convert to the internal IcebergGsConfig structure for storage.
    pub fn to_iceberg_gs_config(&self) -> IcebergGsConfig {
        use fluree_db_iceberg::config::{CatalogConfig, TableConfig};

        // Per mode: only the catalog, the table identifier and the vended flag
        // differ. Everything else is set ONCE below, so a source-level field
        // (model, delete convention, ...) cannot be dropped for one mode.
        let mut io = self.connection.io.clone();
        let (catalog, table) = match &self.connection.catalog_mode {
            CatalogMode::Rest(rest) => (
                CatalogConfig::Rest {
                    catalog_type: "polaris".to_string(),
                    uri: rest.catalog_uri.clone(),
                    auth: rest.auth.clone(),
                    warehouse: rest.warehouse.clone(),
                },
                self.table_identifier.clone(),
            ),
            CatalogMode::Direct { table_location } => {
                // Direct never uses vended credentials, regardless of the io flag.
                io.vended_credentials = false;
                (CatalogConfig::direct(table_location), String::new())
            }
            CatalogMode::Glue { region, catalog_id } => {
                // Glue never vends: S3 is read with the ambient AWS credential chain.
                io.vended_credentials = false;
                (
                    CatalogConfig::glue(region.clone(), catalog_id.clone()),
                    self.table_identifier.clone(),
                )
            }
            CatalogMode::S3Tables {
                region,
                table_bucket_arn,
            } => {
                // S3 Tables never vends: the managed table bucket is read with the
                // ambient AWS credential chain.
                io.vended_credentials = false;
                (
                    CatalogConfig::s3_tables(region.clone(), table_bucket_arn.clone()),
                    self.table_identifier.clone(),
                )
            }
        };
        IcebergGsConfig {
            catalog,
            table: TableConfig::Identifier(table),
            io,
            mapping: None,
            delete: self.delete_convention.clone(),
            order_by: self.order_by.clone(),
            model: self.model.clone(),
            default_allow: self.default_allow,
        }
    }

    /// Validate the configuration.
    pub fn validate(&self) -> crate::Result<()> {
        if self.name.trim().is_empty() {
            return Err(crate::ApiError::config("Graph source name cannot be empty"));
        }
        if self.model.as_deref().is_some_and(|m| m.trim().is_empty()) {
            return Err(crate::ApiError::config("model ledger id cannot be empty"));
        }

        if self.name.contains(':') {
            return Err(crate::ApiError::config(
                "Graph source name cannot contain ':' (use branch for versioning)",
            ));
        }

        match &self.connection.catalog_mode {
            CatalogMode::Rest(rest) => {
                if rest.catalog_uri.trim().is_empty() {
                    return Err(crate::ApiError::config("Catalog URI cannot be empty"));
                }
                if self.table_identifier.trim().is_empty() {
                    return Err(crate::ApiError::config("Table identifier cannot be empty"));
                }
                use fluree_db_iceberg::catalog::parse_table_identifier;
                parse_table_identifier(&self.table_identifier).map_err(|e| {
                    crate::ApiError::config(format!("Invalid table identifier: {e}"))
                })?;
            }
            CatalogMode::Direct { table_location } => {
                if table_location.trim().is_empty() {
                    return Err(crate::ApiError::config(
                        "Table location cannot be empty for direct catalog mode",
                    ));
                }
                let is_object_store =
                    table_location.starts_with("s3://") || table_location.starts_with("s3a://");
                // Local catalog-less tables: `file://` URIs (incl. the
                // `file:/abs` single-slash variant) or bare absolute paths.
                // Mirrors `fluree_db_iceberg::config`'s Direct validation.
                let is_local = fluree_db_iceberg::is_local_location(table_location);
                if !is_object_store && !is_local {
                    return Err(crate::ApiError::config(format!(
                        "Direct catalog table_location must be an S3 URI (s3:// or s3a://), a \
                         file:// URI, or an absolute local path, got: {table_location}"
                    )));
                }
                // Local locations are fail-closed behind the operator allowlist
                // (`FLUREE_ICEBERG_LOCAL_ROOTS`). Both validation gates enforce
                // it — this one and `fluree_db_iceberg::config` — because a
                // config can reach either first.
                fluree_db_iceberg::ensure_local_location_allowed(table_location)
                    .map_err(|e| crate::ApiError::config(e.to_string()))?;
            }
            CatalogMode::Glue { .. } | CatalogMode::S3Tables { .. } => {
                if self.table_identifier.trim().is_empty() {
                    return Err(crate::ApiError::config(
                        "Table identifier cannot be empty for AWS Glue / S3 Tables catalog mode \
                         (use namespace.table, or an R2RML mapping)",
                    ));
                }
                // Region shape, table bucket ARN, table identifier, vended flag:
                // one set of rules, owned by `fluree_db_iceberg::config`, so this
                // gate and the query-time gate cannot disagree.
                self.to_iceberg_gs_config()
                    .validate()
                    .map_err(|e| crate::ApiError::config(e.to_string()))?;
            }
        }

        // Validate the tombstone/delete convention at creation time rather than
        // deferring to the first materialize scan.
        if let Some(delete) = &self.delete_convention {
            delete
                .validate()
                .map_err(|e| crate::ApiError::config(format!("Invalid delete convention: {e}")))?;
        }

        Ok(())
    }

    /// Returns `true` if this config uses REST catalog mode.
    pub fn is_rest(&self) -> bool {
        self.connection.is_rest()
    }

    /// Returns `true` if this config uses direct S3 catalog mode.
    pub fn is_direct(&self) -> bool {
        self.connection.is_direct()
    }

    /// Returns `true` if this config uses AWS Glue catalog mode.
    pub fn is_glue(&self) -> bool {
        self.connection.is_glue()
    }

    /// Returns `true` if this config uses AWS S3 Tables catalog mode.
    pub fn is_s3tables(&self) -> bool {
        self.connection.is_s3tables()
    }
}

// =============================================================================
// R2RML Configuration
// =============================================================================

/// Configuration for creating an R2RML graph source.
///
/// R2RML graph sources combine Iceberg table access with R2RML mappings to expose
/// relational data as RDF triples. The R2RML mapping defines how table
/// rows are transformed into triples.
///
/// # Example
///
/// ```ignore
/// use fluree_db_api::R2rmlCreateConfig;
///
/// let config = R2rmlCreateConfig::new(
///     "airlines-rdf",
///     "https://polaris.example.com",
///     "openflights.airlines",
///     "fluree:file://mappings/airlines.ttl",
/// )
/// .with_auth_bearer("my-token");
///
/// let result = fluree.create_r2rml_graph_source(config).await?;
/// ```
/// How the R2RML mapping is provided.
#[cfg(feature = "iceberg")]
#[derive(Debug, Clone)]
pub enum R2rmlMappingInput {
    /// Mapping content provided inline (Turtle format).
    /// Will be stored to CAS during graph source creation.
    Content(String),
    /// Pre-existing storage address (legacy / advanced use).
    /// The mapping must already exist at this address.
    Address(String),
}

#[cfg(feature = "iceberg")]
#[derive(Debug, Clone)]
pub struct R2rmlCreateConfig {
    /// Underlying Iceberg configuration
    pub iceberg: IcebergCreateConfig,

    /// R2RML mapping input — content or pre-existing address
    pub mapping: R2rmlMappingInput,

    /// R2RML mapping media type (optional, inferred if omitted)
    pub mapping_media_type: Option<String>,
}

#[cfg(feature = "iceberg")]
impl R2rmlCreateConfig {
    /// See [`IcebergCreateConfig::with_model`].
    pub fn with_model(mut self, ledger: impl Into<String>) -> Self {
        self.iceberg.model = Some(ledger.into());
        self
    }

    /// See [`IcebergCreateConfig::with_default_allow`].
    pub fn with_default_allow(mut self, allow: bool) -> Self {
        self.iceberg.default_allow = Some(allow);
        self
    }

    /// Create a new R2RML graph source config with REST catalog and inline mapping.
    pub fn new(
        name: impl Into<String>,
        catalog_uri: impl Into<String>,
        table_identifier: impl Into<String>,
        mapping_content: impl Into<String>,
    ) -> Self {
        Self {
            iceberg: IcebergCreateConfig::new(name, catalog_uri, table_identifier),
            mapping: R2rmlMappingInput::Content(mapping_content.into()),
            mapping_media_type: None,
        }
    }

    /// Create a new R2RML graph source config with direct S3 access and inline mapping.
    pub fn new_direct(
        name: impl Into<String>,
        table_location: impl Into<String>,
        mapping_content: impl Into<String>,
    ) -> Self {
        Self {
            iceberg: IcebergCreateConfig::new_direct(name, table_location),
            mapping: R2rmlMappingInput::Content(mapping_content.into()),
            mapping_media_type: None,
        }
    }

    /// Set the branch name.
    pub fn with_branch(mut self, branch: impl Into<String>) -> Self {
        self.iceberg = self.iceberg.with_branch(branch);
        self
    }

    /// Set bearer token authentication.
    pub fn with_auth_bearer(mut self, token: impl Into<String>) -> Self {
        self.iceberg = self.iceberg.with_auth_bearer(token);
        self
    }

    /// Set Google metadata-server authentication (GKE Workload Identity), with
    /// automatic token refresh. See [`IcebergCreateConfig::with_auth_google_metadata`].
    pub fn with_auth_google_metadata(mut self, scopes: Option<String>) -> Self {
        self.iceberg = self.iceberg.with_auth_google_metadata(scopes);
        self
    }

    /// Set OAuth2 client credentials authentication.
    pub fn with_auth_oauth2(
        mut self,
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Self {
        self.iceberg = self
            .iceberg
            .with_auth_oauth2(token_url, client_id, client_secret);
        self
    }

    /// Set bearer authentication from a secret *reference*.
    /// See [`IcebergConnectionConfig::with_auth_bearer_token_ref`].
    pub fn with_auth_bearer_token_ref(mut self, token_ref: impl Into<String>) -> Self {
        self.iceberg = self.iceberg.with_auth_bearer_token_ref(token_ref);
        self
    }

    /// Set OAuth2 client-credentials authentication with the client secret as a
    /// secret *reference*. See
    /// [`IcebergConnectionConfig::with_auth_oauth2_client_secret_ref`].
    pub fn with_auth_oauth2_client_secret_ref(
        mut self,
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret_ref: impl Into<String>,
    ) -> Self {
        self.iceberg = self.iceberg.with_auth_oauth2_client_secret_ref(
            token_url,
            client_id,
            client_secret_ref,
        );
        self
    }

    /// Set the OAuth2 `scope` (delegates to the underlying Iceberg config).
    ///
    /// Call after [`Self::with_auth_oauth2`]. See
    /// [`IcebergCreateConfig::with_oauth2_scope`].
    pub fn with_oauth2_scope(mut self, scope: impl Into<String>) -> Self {
        self.iceberg = self.iceberg.with_oauth2_scope(scope);
        self
    }

    /// Set the OAuth2 `audience` (delegates to the underlying Iceberg config).
    ///
    /// Call after [`Self::with_auth_oauth2`]. See
    /// [`IcebergCreateConfig::with_oauth2_audience`].
    pub fn with_oauth2_audience(mut self, audience: impl Into<String>) -> Self {
        self.iceberg = self.iceberg.with_oauth2_audience(audience);
        self
    }

    /// Set the warehouse identifier.
    pub fn with_warehouse(mut self, warehouse: impl Into<String>) -> Self {
        self.iceberg = self.iceberg.with_warehouse(warehouse);
        self
    }

    /// Set the mapping media type (e.g., "text/turtle").
    pub fn with_mapping_media_type(mut self, media_type: impl Into<String>) -> Self {
        self.mapping_media_type = Some(media_type.into());
        self
    }

    /// Enable or disable vended credentials.
    pub fn with_vended_credentials(mut self, enabled: bool) -> Self {
        self.iceberg = self.iceberg.with_vended_credentials(enabled);
        self
    }

    /// Set S3 region.
    pub fn with_s3_region(mut self, region: impl Into<String>) -> Self {
        self.iceberg = self.iceberg.with_s3_region(region);
        self
    }

    /// Set S3 endpoint (for MinIO, LocalStack).
    pub fn with_s3_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.iceberg = self.iceberg.with_s3_endpoint(endpoint);
        self
    }

    /// Enable path-style S3 URLs.
    pub fn with_s3_path_style(mut self, enabled: bool) -> Self {
        self.iceberg = self.iceberg.with_s3_path_style(enabled);
        self
    }

    /// Set the tombstone/delete convention used during materialization.
    pub fn with_delete_convention(
        mut self,
        convention: fluree_db_iceberg::DeleteConvention,
    ) -> Self {
        self.iceberg = self.iceberg.with_delete_convention(convention);
        self
    }

    /// Set the ordering column for latest-by-key materialization.
    pub fn with_order_by(mut self, column: impl Into<String>) -> Self {
        self.iceberg = self.iceberg.with_order_by(column);
        self
    }

    /// Get the graph source ID (name:branch).
    pub fn graph_source_id(&self) -> String {
        self.iceberg.graph_source_id()
    }

    /// Convert to the internal IcebergGsConfig structure with mapping for storage.
    ///
    /// `mapping_address` is the CAS address where the mapping was stored.
    pub fn to_iceberg_gs_config(&self, mapping_address: &str) -> IcebergGsConfig {
        let mut config = self.iceberg.to_iceberg_gs_config();
        // Persist a concrete, resolved media type so the query path reuses it
        // instead of re-defaulting a `null` to JSON-LD (issue #1397). An explicit
        // media type is kept verbatim; an omitted one is filled with the resolved
        // default (Turtle for inline/CID mappings). This needs no migration:
        // `MappingSource::media_type` is already `Option<String>` with serde
        // `default`, so pre-existing `null` records still deserialize and are
        // fixed in place by the query-side default.
        let media_type = self.mapping_media_type.clone().unwrap_or_else(|| {
            fluree_db_r2rml::loader::MappingFormat::resolve(None, mapping_address)
                .media_type()
                .to_string()
        });
        config.mapping = Some(fluree_db_iceberg::config::MappingSource {
            source: mapping_address.to_string(),
            media_type: Some(media_type),
        });
        config
    }

    /// Get the mapping content (for Content variant) or None (for Address variant).
    pub fn mapping_content(&self) -> Option<&str> {
        match &self.mapping {
            R2rmlMappingInput::Content(c) => Some(c),
            R2rmlMappingInput::Address(_) => None,
        }
    }

    /// Get the mapping address (for Address variant) or None (for Content variant).
    pub fn mapping_address(&self) -> Option<&str> {
        match &self.mapping {
            R2rmlMappingInput::Address(a) => Some(a),
            R2rmlMappingInput::Content(_) => None,
        }
    }

    /// Validate the configuration.
    pub fn validate(&self) -> crate::Result<()> {
        // Validate the underlying Iceberg config
        self.iceberg.validate()?;

        // Validate mapping
        match &self.mapping {
            R2rmlMappingInput::Content(c) if c.trim().is_empty() => {
                return Err(crate::ApiError::config(
                    "R2RML mapping content cannot be empty",
                ));
            }
            R2rmlMappingInput::Address(a) if a.trim().is_empty() => {
                return Err(crate::ApiError::config(
                    "R2RML mapping address cannot be empty",
                ));
            }
            _ => {}
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_bm25_config_defaults() {
        let config = Bm25CreateConfig::new("search", "docs:main", json!({"select": ["?x"]}));

        assert_eq!(config.name, "search");
        assert_eq!(config.ledger, "docs:main");
        assert_eq!(config.effective_branch(), "main");
        assert_eq!(config.graph_source_id(), "search:main");

        let bm25 = config.bm25_config();
        assert!((bm25.k1 - 1.2).abs() < 0.001);
        assert!((bm25.b - 0.75).abs() < 0.001);
    }

    #[test]
    fn test_bm25_config_with_options() {
        let config = Bm25CreateConfig::new("search", "docs:main", json!({}))
            .with_branch("dev")
            .with_k1(1.5)
            .with_b(0.5);

        assert_eq!(config.effective_branch(), "dev");
        assert_eq!(config.graph_source_id(), "search:dev");

        let bm25 = config.bm25_config();
        assert!((bm25.k1 - 1.5).abs() < 0.001);
        assert!((bm25.b - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_bm25_config_validation_valid() {
        let config = Bm25CreateConfig::new("search", "docs:main", json!({"select": ["?x"]}));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_bm25_config_validation_empty_name() {
        let config = Bm25CreateConfig::new("", "docs:main", json!({"select": ["?x"]}));
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().to_string().contains("name"));
    }

    #[test]
    fn test_bm25_config_validation_name_with_colon() {
        let config = Bm25CreateConfig::new("search:index", "docs:main", json!({"select": ["?x"]}));
        assert!(config.validate().is_err());
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("colon") || err.contains("':'"));
    }

    #[test]
    fn test_bm25_config_validation_empty_ledger() {
        let config = Bm25CreateConfig::new("search", "", json!({"select": ["?x"]}));
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("ledger"));
    }

    #[test]
    fn test_bm25_config_validation_negative_k1() {
        let config =
            Bm25CreateConfig::new("search", "docs:main", json!({"select": ["?x"]})).with_k1(-1.0);
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().to_string().contains("k1"));
    }

    #[test]
    fn test_bm25_config_validation_invalid_b() {
        let config =
            Bm25CreateConfig::new("search", "docs:main", json!({"select": ["?x"]})).with_b(1.5);
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().to_string().contains("b"));
    }

    #[test]
    fn test_bm25_config_validation_no_select() {
        let config = Bm25CreateConfig::new("search", "docs:main", json!({"where": []}));
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("select"));
    }

    #[test]
    fn test_bm25_config_validation_select_one() {
        // selectOne is also valid
        let config = Bm25CreateConfig::new("search", "docs:main", json!({"selectOne": ["?x"]}));
        assert!(config.validate().is_ok());
    }

    #[cfg(feature = "iceberg")]
    fn oauth2_auth(config: &IcebergCreateConfig) -> &fluree_db_iceberg::auth::AuthConfig {
        match &config.connection.catalog_mode {
            CatalogMode::Rest(rest) => &rest.auth,
            CatalogMode::Direct { .. }
            | CatalogMode::Glue { .. }
            | CatalogMode::S3Tables { .. } => panic!("expected REST catalog mode"),
        }
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_iceberg_with_oauth2_scope_and_audience() {
        let config = IcebergCreateConfig::new("gs", "https://catalog.example.com", "ns.tbl")
            .with_auth_oauth2("https://catalog.example.com/v1/oauth/tokens", "", "secret")
            .with_oauth2_scope("session:role:ICEBERG_READER")
            .with_oauth2_audience("polaris");

        match oauth2_auth(&config) {
            fluree_db_iceberg::auth::AuthConfig::OAuth2ClientCredentials {
                scope,
                audience,
                ..
            } => {
                assert_eq!(scope.as_deref(), Some("session:role:ICEBERG_READER"));
                assert_eq!(audience.as_deref(), Some("polaris"));
            }
            other => panic!("expected OAuth2 auth, got {other:?}"),
        }
    }

    #[cfg(feature = "iceberg")]
    fn conn_auth(conn: &IcebergConnectionConfig) -> &fluree_db_iceberg::auth::AuthConfig {
        match &conn.catalog_mode {
            CatalogMode::Rest(rest) => &rest.auth,
            CatalogMode::Direct { .. }
            | CatalogMode::Glue { .. }
            | CatalogMode::S3Tables { .. } => panic!("expected REST catalog mode"),
        }
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn bearer_token_ref_builder_stores_secret_ref() {
        let conn = IcebergConnectionConfig::rest("https://catalog.example.com")
            .with_auth_bearer_token_ref("vault://team/bearer");
        match conn_auth(&conn) {
            fluree_db_iceberg::auth::AuthConfig::Bearer { token } => assert_eq!(
                *token,
                fluree_db_iceberg::ConfigValue::SecretRef {
                    secret_ref: "vault://team/bearer".to_string()
                }
            ),
            other => panic!("expected Bearer auth, got {other:?}"),
        }
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn oauth2_client_secret_ref_builder_stores_secret_ref_and_literal_id() {
        let conn = IcebergConnectionConfig::rest("https://catalog.example.com")
            .with_auth_oauth2_client_secret_ref(
                "https://catalog.example.com/v1/oauth/tokens",
                "svc-client",
                "vault://team/client-secret",
            );
        match conn_auth(&conn) {
            fluree_db_iceberg::auth::AuthConfig::OAuth2ClientCredentials {
                client_id,
                client_secret,
                ..
            } => {
                // client_id is non-secret and stored as a plain literal.
                assert_eq!(client_id.resolve().unwrap(), "svc-client");
                // client_secret is stored as an opaque SecretRef, never a literal.
                assert_eq!(
                    *client_secret,
                    fluree_db_iceberg::ConfigValue::SecretRef {
                        secret_ref: "vault://team/client-secret".to_string()
                    }
                );
            }
            other => panic!("expected OAuth2 auth, got {other:?}"),
        }
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn debug_of_connection_config_redacts_secrets() {
        // A future `{:?}` on the connection (in a log or error) must not leak the
        // bearer token or the OAuth2 client secret it transitively holds via the
        // `AuthConfig` -> `ConfigValue` chain.
        let bearer = IcebergConnectionConfig::rest("https://catalog.example.com")
            .with_auth_bearer("super-secret-bearer-token");
        let dbg = format!("{bearer:?}");
        assert!(
            !dbg.contains("super-secret-bearer-token"),
            "bearer token leaked in Debug: {dbg}"
        );

        let oauth = IcebergConnectionConfig::rest("https://catalog.example.com").with_auth_oauth2(
            "https://catalog.example.com/v1/oauth/tokens",
            "client-id-ok-to-show",
            "super-secret-oauth-secret",
        );
        let dbg = format!("{oauth:?}");
        assert!(
            !dbg.contains("super-secret-oauth-secret"),
            "oauth client_secret leaked in Debug: {dbg}"
        );

        // The same guarantee must hold one level up, on IcebergCreateConfig,
        // whose derived Debug prints the connection.
        let create = IcebergCreateConfig::new("gs", "https://catalog.example.com", "ns.tbl")
            .with_auth_bearer("super-secret-bearer-token");
        assert!(
            !format!("{create:?}").contains("super-secret-bearer-token"),
            "bearer token leaked in IcebergCreateConfig Debug"
        );
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_oauth2_scope_setter_warns_without_oauth2() {
        // Bearer auth set, scope setter should be a no-op (and not panic).
        let config = IcebergCreateConfig::new("gs", "https://catalog.example.com", "ns.tbl")
            .with_auth_bearer("tok")
            .with_oauth2_scope("session:role:READER");
        assert!(matches!(
            oauth2_auth(&config),
            fluree_db_iceberg::auth::AuthConfig::Bearer { .. }
        ));
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_iceberg_oauth2_scope_roundtrip_no_migration() {
        // Locks the "no migration" claim: scope/audience survive the
        // to_iceberg_gs_config -> serialize -> deserialize round-trip that the
        // persistence layer performs.
        let config = IcebergCreateConfig::new("gs", "https://catalog.example.com", "ns.tbl")
            .with_auth_oauth2(
                "https://catalog.example.com/v1/oauth/tokens",
                "client",
                "secret",
            )
            .with_oauth2_scope("session:role:ICEBERG_READER")
            .with_oauth2_audience("polaris");

        let gs = config.to_iceberg_gs_config();
        let serialized = serde_json::to_string(&gs).unwrap();
        let back: IcebergGsConfig = serde_json::from_str(&serialized).unwrap();

        match back.catalog {
            fluree_db_iceberg::config::CatalogConfig::Rest { auth, .. } => match auth {
                fluree_db_iceberg::auth::AuthConfig::OAuth2ClientCredentials {
                    scope,
                    audience,
                    ..
                } => {
                    assert_eq!(scope.as_deref(), Some("session:role:ICEBERG_READER"));
                    assert_eq!(audience.as_deref(), Some("polaris"));
                }
                other => panic!("expected OAuth2 auth after round-trip, got {other:?}"),
            },
            other => panic!("expected REST catalog after round-trip, got {other:?}"),
        }
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_r2rml_with_oauth2_scope_delegates() {
        let config = R2rmlCreateConfig::new(
            "gs",
            "https://catalog.example.com",
            "ns.tbl",
            "@prefix rr: <http://www.w3.org/ns/r2rml#> .",
        )
        .with_auth_oauth2("https://catalog.example.com/v1/oauth/tokens", "", "secret")
        .with_oauth2_scope("session:role:ICEBERG_READER")
        .with_oauth2_audience("polaris");

        match oauth2_auth(&config.iceberg) {
            fluree_db_iceberg::auth::AuthConfig::OAuth2ClientCredentials {
                scope,
                audience,
                ..
            } => {
                assert_eq!(scope.as_deref(), Some("session:role:ICEBERG_READER"));
                assert_eq!(audience.as_deref(), Some("polaris"));
            }
            other => panic!("expected OAuth2 auth, got {other:?}"),
        }
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_r2rml_persists_resolved_media_type_no_migration() {
        // Issue #1397: an omitted media type must be persisted as a concrete
        // `text/turtle` (not `null`) so the query path reuses it; an explicit
        // media type is preserved verbatim. The value survives the
        // to_iceberg_gs_config -> serialize -> deserialize round-trip with no
        // schema migration (`MappingSource::media_type` is `Option` + serde
        // `default`).
        let cid = "bagiibqexampleciddoesnotendwithanextension";
        let mapping = "@prefix rr: <http://www.w3.org/ns/r2rml#> .";

        // No explicit media type -> the resolved Turtle default is persisted.
        let config = R2rmlCreateConfig::new("gs", "https://catalog.example.com", "ns.tbl", mapping);
        let gs = config.to_iceberg_gs_config(cid);
        assert_eq!(
            gs.mapping.as_ref().and_then(|m| m.media_type.as_deref()),
            Some("text/turtle"),
            "an omitted media type must be filled with the resolved Turtle default"
        );

        // ...and survives serialize -> deserialize unchanged (no migration).
        let serialized = serde_json::to_string(&gs).unwrap();
        let back: IcebergGsConfig = serde_json::from_str(&serialized).unwrap();
        assert_eq!(
            back.mapping.as_ref().and_then(|m| m.media_type.as_deref()),
            Some("text/turtle")
        );

        // An explicit media type is preserved verbatim.
        let explicit =
            R2rmlCreateConfig::new("gs", "https://catalog.example.com", "ns.tbl", mapping)
                .with_mapping_media_type("application/ld+json");
        let gs = explicit.to_iceberg_gs_config(cid);
        assert_eq!(
            gs.mapping.as_ref().and_then(|m| m.media_type.as_deref()),
            Some("application/ld+json"),
            "an explicit media type must be preserved"
        );
    }

    // ── Catalog modes: one dispatch for every surface (CLI, server) ──

    #[cfg(feature = "iceberg")]
    const S3TABLES_ARN: &str = "arn:aws:s3tables:us-east-1:123456789012:bucket/analytics";

    #[cfg(feature = "iceberg")]
    fn mode_args(mode: &str) -> CatalogModeArgs<'_> {
        CatalogModeArgs {
            mode,
            ..Default::default()
        }
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn from_mode_parses_every_catalog_mode() {
        let rest = IcebergConnectionConfig::from_mode(CatalogModeArgs {
            catalog_uri: Some("https://polaris.example.com"),
            ..mode_args("REST")
        })
        .unwrap();
        assert!(rest.is_rest());
        // An empty mode is the default, rest.
        assert!(IcebergConnectionConfig::from_mode(CatalogModeArgs {
            catalog_uri: Some("https://polaris.example.com"),
            ..mode_args("")
        })
        .unwrap()
        .is_rest());
        let direct = IcebergConnectionConfig::from_mode(CatalogModeArgs {
            table_location: Some("s3://bucket/warehouse/ns/t"),
            ..mode_args("direct")
        })
        .unwrap();
        assert!(direct.is_direct());
        let glue = IcebergConnectionConfig::from_mode(CatalogModeArgs {
            region: Some("us-east-1"),
            catalog_id: Some("123456789012"),
            ..mode_args("Glue")
        })
        .unwrap();
        match &glue.catalog_mode {
            CatalogMode::Glue { region, catalog_id } => {
                assert_eq!(region.as_deref(), Some("us-east-1"));
                assert_eq!(catalog_id.as_deref(), Some("123456789012"));
            }
            other => panic!("expected glue, got {other:?}"),
        }
        assert!(!glue.io.vended_credentials, "glue never vends");
        let s3tables = IcebergConnectionConfig::from_mode(CatalogModeArgs {
            table_bucket_arn: Some(S3TABLES_ARN),
            ..mode_args("s3tables")
        })
        .unwrap();
        assert!(s3tables.is_s3tables());
        assert!(!s3tables.io.vended_credentials, "s3tables never vends");
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn from_mode_names_what_is_missing_in_each_surfaces_spelling() {
        let missing = |args| IcebergConnectionConfig::from_mode(args).unwrap_err();
        let cli = |field: &str| format!("--{}", field.replace('_', "-"));
        assert_eq!(
            missing(mode_args("rest")).message(cli),
            "--catalog-uri is required for rest mode"
        );
        assert_eq!(
            missing(mode_args("direct")).message(str::to_string),
            "table_location is required for direct mode"
        );
        assert_eq!(
            missing(mode_args("s3tables")).message(cli),
            "--table-bucket-arn is required for s3tables mode"
        );
        // A blank value is as missing as an absent one.
        assert!(matches!(
            missing(CatalogModeArgs {
                table_bucket_arn: Some("  "),
                ..mode_args("s3tables")
            }),
            CatalogModeError::MissingField { .. }
        ));
        assert_eq!(
            missing(mode_args("hadoop")),
            CatalogModeError::UnknownMode("hadoop".to_string())
        );
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn rest_only_settings_are_refused_by_other_modes() {
        // Catalog auth or a warehouse given to a non-REST mode would be silently
        // ignored; refuse it, naming the setting in the caller's spelling.
        let auth = ["auth_bearer"];
        let err = IcebergConnectionConfig::from_mode(CatalogModeArgs {
            rest_only: &auth,
            ..mode_args("glue")
        })
        .unwrap_err();
        assert_eq!(
            err.message(|f| format!("--{}", f.replace('_', "-"))),
            "--auth-bearer applies to rest mode only; glue mode would ignore it"
        );
        for mode in ["direct", "s3tables"] {
            let args = CatalogModeArgs {
                table_location: Some("s3://b/w/ns/t"),
                table_bucket_arn: Some(S3TABLES_ARN),
                rest_only: &["warehouse"],
                ..mode_args(mode)
            };
            assert!(matches!(
                IcebergConnectionConfig::from_mode(args),
                Err(CatalogModeError::NotForMode {
                    field: "warehouse",
                    ..
                })
            ));
        }
        // REST takes them.
        IcebergConnectionConfig::from_mode(CatalogModeArgs {
            catalog_uri: Some("https://polaris.example.com"),
            rest_only: &["auth_bearer", "warehouse"],
            ..mode_args("rest")
        })
        .unwrap();
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn create_from_mode_needs_a_table_unless_a_mapping_defines_them() {
        let glue = || mode_args("glue");
        // A mapping-defined source has no table of its own (each rr:tableName is
        // one), for glue and s3tables exactly as for rest.
        let mapped = IcebergCreateConfig::from_mode("gs", glue(), None, true).unwrap();
        assert_eq!(
            mapped.table_identifier,
            fluree_db_iceberg::config::MAPPING_DEFINED_TABLE
        );
        let named =
            IcebergCreateConfig::from_mode("gs", glue(), Some("sales.orders"), true).unwrap();
        assert_eq!(named.table_identifier, "sales.orders");
        let err = IcebergCreateConfig::from_mode("gs", glue(), Some(" "), false).unwrap_err();
        assert_eq!(
            err.message(str::to_string),
            "table is required for glue mode (or provide r2rml to define tables via mapping)"
        );
        // Direct takes its table from the location.
        let direct = IcebergCreateConfig::from_mode(
            "gs",
            CatalogModeArgs {
                table_location: Some("s3://bucket/warehouse/ns/t"),
                ..mode_args("direct")
            },
            Some("ignored.table"),
            false,
        )
        .unwrap();
        assert_eq!(direct.table_identifier, "");
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn every_catalog_mode_keeps_the_source_level_fields() {
        // The per-mode conversion builds ONE IcebergGsConfig: a source-level field
        // (governance model, delete convention, ...) cannot be dropped for a mode.
        let connections = [
            IcebergConnectionConfig::rest("https://polaris.example.com"),
            IcebergConnectionConfig::direct("s3://bucket/warehouse/ns/t"),
            IcebergConnectionConfig::glue(Some("us-east-1".into()), None),
            IcebergConnectionConfig::s3_tables(None, S3TABLES_ARN),
        ];
        for connection in connections {
            let mut config = IcebergCreateConfig::from_connection("gs", connection, "ns.t");
            config.model = Some("governance:main".to_string());
            config.default_allow = Some(false);
            config.order_by = Some("updated_at".to_string());
            config.delete_convention = Some(fluree_db_iceberg::DeleteConvention {
                column: "op".to_string(),
                deleted_values: vec![Some("D".to_string())],
            });
            let gs = config.to_iceberg_gs_config();
            assert_eq!(
                gs.model.as_deref(),
                Some("governance:main"),
                "{:?}",
                gs.catalog
            );
            assert_eq!(gs.default_allow, Some(false), "{:?}", gs.catalog);
            assert_eq!(
                gs.order_by.as_deref(),
                Some("updated_at"),
                "{:?}",
                gs.catalog
            );
            assert!(gs.delete.is_some(), "{:?}", gs.catalog);
        }
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn aws_catalog_modes_validate_with_the_engine_rules() {
        let glue = |region: &str| {
            IcebergCreateConfig::from_connection(
                "gs",
                IcebergConnectionConfig::glue(Some(region.to_string()), None),
                "sales.orders",
            )
        };
        glue("us-east-1").validate().unwrap();
        // The engine's region-shape rule reaches the API gate.
        let err = glue("us-east-1.evil.com").validate().unwrap_err();
        assert!(err.to_string().contains("not an AWS region"), "{err}");
        let arn = IcebergCreateConfig::from_connection(
            "gs",
            IcebergConnectionConfig::s3_tables(Some("eu-west-1".into()), S3TABLES_ARN),
            "sales.orders",
        );
        let err = arn.validate().unwrap_err();
        assert!(err.to_string().contains("contradicts"), "{err}");
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn connection_region_and_storage_io_resolve_like_the_engine() {
        // Glue: the catalog region, else s3_region; the reads, s3_region else region.
        let glue = IcebergConnectionConfig::glue(Some("us-west-2".into()), None);
        assert_eq!(
            glue.aws_catalog_region().unwrap().as_deref(),
            Some("us-west-2")
        );
        assert_eq!(glue.storage_io().s3_region.as_deref(), Some("us-west-2"));
        let glue = glue.with_s3_region("eu-west-1");
        assert_eq!(
            glue.aws_catalog_region().unwrap().as_deref(),
            Some("us-west-2")
        );
        assert_eq!(glue.storage_io().s3_region.as_deref(), Some("eu-west-1"));
        let bare = IcebergConnectionConfig::glue(None, None).with_s3_region("eu-west-1");
        assert_eq!(
            bare.aws_catalog_region().unwrap().as_deref(),
            Some("eu-west-1")
        );
        // S3 Tables: the ARN names the region.
        let s3tables = IcebergConnectionConfig::s3_tables(None, S3TABLES_ARN);
        assert_eq!(
            s3tables.aws_catalog_region().unwrap().as_deref(),
            Some("us-east-1")
        );
        assert_eq!(
            s3tables.storage_io().s3_region.as_deref(),
            Some("us-east-1")
        );
        // REST / Direct: no AWS catalog API; io untouched.
        let rest = IcebergConnectionConfig::rest("https://polaris.example.com");
        assert_eq!(rest.aws_catalog_region().unwrap(), None);
        assert_eq!(rest.storage_io().s3_region, None);
        // A malformed region is refused before any client is built.
        let bad = IcebergConnectionConfig::glue(Some("x/y".into()), None);
        assert!(bad.aws_catalog_region().is_err());
    }
}
