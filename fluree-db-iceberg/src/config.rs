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
    /// Optional tombstone/delete convention. When set, materialization
    /// classifies each source row as live or a delete (tombstone) and retracts
    /// tombstoned subjects from the target ledger. Absent => additive (no
    /// retraction), which is the legacy behavior.
    #[serde(default)]
    pub delete: Option<DeleteConvention>,
    /// Optional ordering column for latest-by-key materialization. When set, the
    /// rows of each subject within a refresh window are ordered by this column
    /// (numeric/timestamp columns compare by value; others lexicographically) and
    /// the **latest** row defines the subject — a whole-subject replace that
    /// clears fields dropped in the newer revision, matching a
    /// `ROW_NUMBER() … ORDER BY <col> DESC` latest-by-key view. Absent => the
    /// last row in scan order wins and live revisions are merged per predicate
    /// (legacy behavior; fields cleared in a later revision are NOT removed).
    #[serde(default)]
    pub order_by: Option<String>,
    /// Optional model ledger (`name:branch`) governing this source: its
    /// default graph supplies the view policies and the `rdfs:subClassOf` /
    /// `rdfs:subPropertyOf` hierarchy used to expand policy targets, the way a
    /// native ledger's `f:policySource` / `f:schemaSource` config references do.
    /// A virtual source has no ledger of its own to hold either.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Optional `default-allow` for governed requests that carry policy inputs
    /// but match no policy — the same tri-state as a native ledger's
    /// `f:defaultAllow` config. Lets an admin declare a source readable under
    /// authentication without attaching a model. Unset: fail-closed (`false`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_allow: Option<bool>,
}

/// Declares how a delete is encoded in the source table's append log so the
/// materializer can recognize "tombstone" rows and retract those subjects.
///
/// Append-only CDC sinks model a delete as an ordinary appended row carrying a
/// marker. A row is a tombstone when the value of `column` is one of
/// `deleted_values`. A `null` entry in `deleted_values` matches a NULL `column`
/// value — the Debezium null-payload convention (used when the table has no
/// explicit op column). Examples:
/// - `{ column: "_op", deleted_values: ["d", "delete"] }` — value-match op column.
/// - `{ column: "type", deleted_values: [null] }` — null-payload delete.
/// - `{ column: "_op", deleted_values: ["d", null] }` — either.
///
/// The subject IRI of a tombstone row is derived by the SAME R2RML subject
/// materializer as a live row, so the retracted IRI matches what was asserted.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct DeleteConvention {
    /// Source column inspected to classify a row as a tombstone.
    pub column: String,
    /// Column values that mark a row as a delete (tombstone). A `null` element
    /// matches a NULL `column` value (null-payload delete). Must be non-empty.
    #[serde(default)]
    pub deleted_values: Vec<Option<String>>,
}

impl DeleteConvention {
    /// Validate the convention is usable: a column is named and at least one
    /// delete value (possibly `null`) is declared.
    pub fn validate(&self) -> Result<()> {
        if self.column.trim().is_empty() {
            return Err(IcebergError::Config(
                "delete.column is required".to_string(),
            ));
        }
        if self.deleted_values.is_empty() {
            return Err(IcebergError::Config(
                "delete.deleted_values must list at least one value (use null for a \
                 null-payload delete)"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// Classify one row by its marker-column value (already converted to its
    /// string form, or `None` when the column is null for that row): `true` if
    /// that value — including `None` for a null — is in `deleted_values`.
    pub fn is_tombstone(&self, column_value: Option<&str>) -> bool {
        self.deleted_values
            .iter()
            .any(|d| d.as_deref() == column_value)
    }
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
                .trim_start_matches("s3a://")
                .trim_start_matches("file://");
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
    /// Checks shared by the native-AWS catalog modes (Glue, S3 Tables).
    fn validate_aws_sdk_catalog(&self, mode: &str) -> Result<()> {
        // Every table is named `<namespace>.<table>`; nothing derives from a path.
        self.table_identifier()?;
        // The catalog returns a metadata location only — never credentials — so
        // a source that requires vended credentials could never be satisfied.
        // Refuse it here, as Direct mode does, rather than read with the ambient
        // identity the config said not to use.
        if self.io.vended_credentials {
            return Err(IcebergError::Config(format!(
                "Vended credentials are not supported with the {mode} catalog — it reads S3 \
                 with the ambient AWS credential chain; set io.vended_credentials = false"
            )));
        }
        self.io.validate_s3_overrides()
    }

    /// The IO settings this source's S3 reads use. For the native-AWS catalog
    /// modes (Glue, S3 Tables) `s3_region` falls back to the catalog's own
    /// region — see [`IoConfig::with_catalog_region`]; every other mode reads
    /// with `io` as configured.
    pub fn storage_io(&self) -> std::borrow::Cow<'_, IoConfig> {
        match self.catalog.aws_region() {
            Some(region) if self.io.s3_region.is_none() => {
                std::borrow::Cow::Owned(self.io.with_catalog_region(Some(region)))
            }
            _ => std::borrow::Cow::Borrowed(&self.io),
        }
    }

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
                let is_object_store =
                    table_location.starts_with("s3://") || table_location.starts_with("s3a://");
                // `file:///abs/path` (and the `file:/abs` single-slash variant) or a
                // bare absolute path: a catalog-less LOCAL table, read from the
                // filesystem with no object store or catalog service involved.
                let is_local = crate::local_guard::is_local_location(table_location);
                if !is_object_store && !is_local {
                    return Err(IcebergError::Config(format!(
                        "Direct catalog table_location must be an S3 URI (s3:// or s3a://), a \
                         file:// URI, or an absolute local path, got: {table_location}"
                    )));
                }
                // Local locations are fail-closed: permitted only under an
                // operator-allowlisted root. Refuse at creation rather than
                // letting a disallowed path surface as a storage error later.
                crate::local_guard::ensure_local_location_allowed(table_location)?;
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
            CatalogConfig::Glue { .. } => {
                self.catalog.validate_aws_sdk_catalog()?;
                self.validate_aws_sdk_catalog("Glue")?;
            }
            CatalogConfig::S3Tables { .. } => {
                self.catalog.validate_aws_sdk_catalog()?;
                self.validate_aws_sdk_catalog("S3 Tables")?;
            }
        }

        if let Some(delete) = &self.delete {
            delete.validate()?;
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
    /// to resolve the current metadata file, falling back to a
    /// metadata-directory listing on backends that support it (local
    /// filesystem).
    Direct {
        /// Table root directory: an S3 prefix
        /// (`s3://bucket/warehouse/my_namespace/my_table`) or a LOCAL path
        /// (`file:///data/warehouse/ns/table` or a bare absolute path) for
        /// catalog-less tables on the local filesystem. Must contain a
        /// `metadata/` subdirectory with Iceberg metadata files.
        table_location: String,
    },

    /// AWS Glue Data Catalog via the native AWS SDK (`aws-sdk-glue`).
    ///
    /// Resolves the table's `metadata_location` from Glue `GetTable`, then reads
    /// metadata/manifests/data from S3 with the ambient AWS credential chain. The
    /// Glue *database* is the table identifier's namespace. No REST/SigV4/vended
    /// credentials are involved — the SDK signs its own calls.
    Glue {
        /// AWS region. Falls back to the SDK default chain / `io.s3_region` if `None`.
        #[serde(default)]
        region: Option<String>,
        /// Glue catalog id for cross-account access (`None` = the caller's account).
        #[serde(default)]
        catalog_id: Option<String>,
    },

    /// AWS S3 Tables via the native AWS SDK (`aws-sdk-s3tables`).
    ///
    /// Resolves the table's metadata location from `GetTableMetadataLocation`,
    /// then reads from the managed table bucket with the ambient AWS credential
    /// chain.
    S3Tables {
        /// AWS region. Falls back to the SDK default chain / `io.s3_region` if `None`.
        #[serde(default)]
        region: Option<String>,
        /// The S3 Tables table-bucket ARN
        /// (`arn:aws:s3tables:<region>:<account>:bucket/<name>`).
        table_bucket_arn: String,
    },
}

impl CatalogConfig {
    /// The region configured on a native-AWS catalog mode (Glue, S3 Tables);
    /// `None` for REST and Direct, which call no AWS catalog API.
    ///
    /// An S3 Tables bucket ARN names its region, so S3 Tables falls back to it.
    pub fn aws_region(&self) -> Option<&str> {
        match self {
            CatalogConfig::Glue { region, .. } => region.as_deref(),
            CatalogConfig::S3Tables {
                region,
                table_bucket_arn,
            } => region
                .as_deref()
                .or_else(|| s3tables_bucket_arn_region(table_bucket_arn).ok()),
            CatalogConfig::Rest { .. } | CatalogConfig::Direct { .. } => None,
        }
    }

    /// Validate a native-AWS catalog's own settings (Glue, S3 Tables): region
    /// shape, a non-empty Glue catalog id, a well-formed S3 Tables bucket ARN
    /// whose region agrees with `region`. No-op for REST and Direct. Shared by
    /// [`IcebergGsConfig::validate`] and the onboarding (browse/preview) path,
    /// which has a catalog but no table.
    pub fn validate_aws_sdk_catalog(&self) -> Result<()> {
        match self {
            CatalogConfig::Glue { region, catalog_id } => {
                validate_aws_region("catalog.region", region.as_deref())?;
                if catalog_id.as_deref().is_some_and(|id| id.trim().is_empty()) {
                    return Err(IcebergError::Config(
                        "Glue catalog.catalog_id must not be empty (omit it for the caller's \
                         account)"
                            .to_string(),
                    ));
                }
            }
            CatalogConfig::S3Tables {
                region,
                table_bucket_arn,
            } => {
                validate_aws_region("catalog.region", region.as_deref())?;
                let arn_region = s3tables_bucket_arn_region(table_bucket_arn)?;
                // The bucket lives in the ARN's region; a different `region` could
                // only send the catalog call to the wrong endpoint.
                if let Some(region) = region.as_deref().filter(|r| *r != arn_region) {
                    return Err(IcebergError::Config(format!(
                        "S3Tables catalog.region '{region}' contradicts the table bucket ARN's \
                         region '{arn_region}' (omit catalog.region to use the ARN's)"
                    )));
                }
            }
            CatalogConfig::Rest { .. } | CatalogConfig::Direct { .. } => {}
        }
        Ok(())
    }

    /// Whether this catalog can vend storage credentials. Only a REST catalog
    /// can; Direct has no catalog, and Glue / S3 Tables return a metadata
    /// location only, so their reads always use the ambient AWS identity.
    pub fn vends_credentials(&self) -> bool {
        matches!(self, CatalogConfig::Rest { .. })
    }

    /// A display label for the catalog in logs and errors: the REST URI, the
    /// Glue catalog id (`aws-glue` for the caller's own account), the S3 Tables
    /// bucket ARN, or the Direct table location.
    pub fn catalog_label(&self) -> &str {
        match self {
            CatalogConfig::Rest { uri, .. } => uri,
            CatalogConfig::Direct { table_location } => table_location,
            CatalogConfig::Glue { catalog_id, .. } => catalog_id.as_deref().unwrap_or("aws-glue"),
            CatalogConfig::S3Tables {
                table_bucket_arn, ..
            } => table_bucket_arn,
        }
    }

    /// The region a native-AWS catalog's API is called in — see
    /// [`aws_catalog_region`]. `None` for REST and Direct.
    pub fn aws_catalog_region<'a>(&'a self, io: &'a IoConfig) -> Option<&'a str> {
        match self {
            CatalogConfig::Glue { .. } | CatalogConfig::S3Tables { .. } => {
                aws_catalog_region(self.aws_region(), io)
            }
            CatalogConfig::Rest { .. } | CatalogConfig::Direct { .. } => None,
        }
    }

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

    /// Create an AWS Glue Data Catalog config.
    pub fn glue(region: Option<String>, catalog_id: Option<String>) -> Self {
        CatalogConfig::Glue { region, catalog_id }
    }

    /// Create an AWS S3 Tables catalog config from a table-bucket ARN.
    pub fn s3_tables(region: Option<String>, table_bucket_arn: impl Into<String>) -> Self {
        CatalogConfig::S3Tables {
            region,
            table_bucket_arn: table_bucket_arn.into(),
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
    Glue {
        #[serde(default)]
        region: Option<String>,
        #[serde(default)]
        catalog_id: Option<String>,
    },
    S3Tables {
        #[serde(default)]
        region: Option<String>,
        table_bucket_arn: String,
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
            CatalogConfigHelper::Tagged(TaggedCatalogConfig::Glue { region, catalog_id }) => {
                CatalogConfig::Glue { region, catalog_id }
            }
            CatalogConfigHelper::Tagged(TaggedCatalogConfig::S3Tables {
                region,
                table_bucket_arn,
            }) => CatalogConfig::S3Tables {
                region,
                table_bucket_arn,
            },
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
            CatalogConfig::Glue { region, catalog_id } => {
                CatalogConfigHelper::Tagged(TaggedCatalogConfig::Glue { region, catalog_id })
            }
            CatalogConfig::S3Tables {
                region,
                table_bucket_arn,
            } => CatalogConfigHelper::Tagged(TaggedCatalogConfig::S3Tables {
                region,
                table_bucket_arn,
            }),
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

impl IoConfig {
    /// These settings with `s3_region` falling back to a native-AWS catalog's
    /// region: a lake's catalog and its data normally share a region, so a user
    /// who names only the catalog's region still reads S3 there. An explicit
    /// `s3_region` wins — the data may live in another region than the catalog.
    pub fn with_catalog_region(&self, catalog_region: Option<&str>) -> IoConfig {
        IoConfig {
            s3_region: self
                .s3_region
                .clone()
                .or_else(|| catalog_region.map(str::to_string)),
            ..self.clone()
        }
    }

    /// Validate the S3 overrides that end up in an SDK endpoint: the region's
    /// shape (it becomes part of a hostname) and the endpoint's SSRF guard.
    pub fn validate_s3_overrides(&self) -> Result<()> {
        validate_aws_region("io.s3_region", self.s3_region.as_deref())?;
        if let Some(endpoint) = self.s3_endpoint.as_deref() {
            crate::net::validate_s3_endpoint(endpoint)?;
        }
        Ok(())
    }
}

/// The region a native-AWS catalog's API (Glue `GetTable`, S3 Tables
/// `GetTableMetadataLocation`) is called in: the mode's own `region`, else
/// `io.s3_region` (the same region the data reads use), else `None`, which
/// defers to the AWS SDK's region chain (`AWS_REGION`, profile, IMDS).
///
/// The one resolution both the query path and the browse/preview path use, so
/// the catalog call and the S3 reads cannot disagree about a region the user
/// gave only once.
pub fn aws_catalog_region<'a>(mode_region: Option<&'a str>, io: &'a IoConfig) -> Option<&'a str> {
    mode_region.or(io.s3_region.as_deref())
}

/// The region of an S3 Tables table-bucket ARN,
/// `arn:<partition>:s3tables:<region>:<account-id>:bucket/<name>`, or a config
/// error naming what is wrong with it. Accepts every AWS partition (`aws`,
/// `aws-cn`, `aws-us-gov`).
pub fn s3tables_bucket_arn_region(arn: &str) -> Result<&str> {
    let invalid = |why: &str| {
        IcebergError::Config(format!(
            "S3Tables table_bucket_arn must be an S3 Tables bucket ARN \
             (arn:aws:s3tables:<region>:<account-id>:bucket/<name>); {why}: {arn}"
        ))
    };
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    let [prefix, partition, service, region, account, resource] = parts.as_slice() else {
        return Err(invalid("it has too few ':'-separated parts"));
    };
    if *prefix != "arn" || !partition.starts_with("aws") || *service != "s3tables" {
        return Err(invalid("it is not an s3tables ARN"));
    }
    if account.len() != 12 || !account.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid("the account id is not 12 digits"));
    }
    if resource.strip_prefix("bucket/").is_none_or(str::is_empty) {
        return Err(invalid("the resource is not bucket/<name>"));
    }
    validate_aws_region("table_bucket_arn region", Some(region))?;
    Ok(region)
}

/// Refuse a region that is not shaped like an AWS region (`us-east-1`,
/// `us-gov-west-1`, `cn-north-1`). The AWS SDK interpolates the region into the
/// service hostname (`glue.<region>.amazonaws.com`), so a value carrying `.`,
/// `/` or `:` would point a signed request at another host.
pub fn validate_aws_region(field: &str, region: Option<&str>) -> Result<()> {
    let Some(region) = region else {
        return Ok(());
    };
    let well_formed = !region.is_empty()
        && region.len() <= 32
        && region.split('-').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        });
    if well_formed {
        Ok(())
    } else {
        Err(IcebergError::Config(format!(
            "{field} '{region}' is not an AWS region (expected e.g. us-east-1)"
        )))
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

    // ── AWS Glue / S3 Tables ──

    const S3TABLES_ARN: &str = "arn:aws:s3tables:us-east-1:123456789012:bucket/demo";

    /// A Glue / S3 Tables source as the API builds one: vended credentials off.
    fn aws_sdk_config(catalog: CatalogConfig, table: &str) -> IcebergGsConfig {
        IcebergGsConfig {
            catalog,
            table: TableConfig::Identifier(table.to_string()),
            io: IoConfig {
                vended_credentials: false,
                ..Default::default()
            },
            mapping: None,
            delete: None,
            order_by: None,
            model: None,
            default_allow: None,
        }
    }

    #[test]
    fn test_parse_tagged_glue_config() {
        let json = r#"{
            "catalog": { "type": "glue", "region": "us-east-1" },
            "table": "sales.orders",
            "io": { "vended_credentials": false }
        }"#;
        let config: IcebergGsConfig = serde_json::from_str(json).unwrap();
        match &config.catalog {
            CatalogConfig::Glue { region, catalog_id } => {
                assert_eq!(region, &Some("us-east-1".to_string()));
                assert_eq!(catalog_id, &None);
            }
            other => panic!("Expected Glue variant, got {other:?}"),
        }
        let table_id = config.table_identifier().unwrap();
        assert_eq!(table_id.namespace, "sales");
        assert_eq!(table_id.table, "orders");
        config.validate().unwrap();
    }

    #[test]
    fn test_parse_tagged_s3tables_config() {
        let json = format!(
            r#"{{
            "catalog": {{ "type": "s3tables", "region": "us-east-1", "table_bucket_arn": "{S3TABLES_ARN}" }},
            "table": "sales.orders",
            "io": {{ "vended_credentials": false }}
        }}"#
        );
        let config: IcebergGsConfig = serde_json::from_str(&json).unwrap();
        match &config.catalog {
            CatalogConfig::S3Tables {
                region,
                table_bucket_arn,
            } => {
                assert_eq!(region, &Some("us-east-1".to_string()));
                assert_eq!(table_bucket_arn, S3TABLES_ARN);
            }
            other => panic!("Expected S3Tables variant, got {other:?}"),
        }
        config.validate().unwrap();
    }

    #[test]
    fn test_aws_sdk_configs_roundtrip_with_their_tags() {
        for (catalog, tag) in [
            (CatalogConfig::glue(Some("us-east-1".into()), None), "glue"),
            (CatalogConfig::s3_tables(None, S3TABLES_ARN), "s3tables"),
        ] {
            let config = aws_sdk_config(catalog.clone(), "sales.orders");
            let json = config.to_json().unwrap();
            assert!(json.contains(&format!("\"type\":\"{tag}\"")), "{json}");
            assert_eq!(IcebergGsConfig::from_json(&json).unwrap().catalog, catalog);
        }
    }

    #[test]
    fn test_validate_aws_sdk_catalogs() {
        let glue = || CatalogConfig::glue(Some("us-east-1".into()), None);
        aws_sdk_config(glue(), "sales.orders").validate().unwrap();

        // The table identifier must be `namespace.table`.
        assert!(aws_sdk_config(glue(), "").validate().is_err());
        // An S3 Tables source needs a table-bucket ARN.
        let err = aws_sdk_config(CatalogConfig::s3_tables(None, "not-an-arn"), "ns.t")
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("ARN"), "{err}");
        // An empty catalog id is refused rather than sent to Glue.
        let err = aws_sdk_config(CatalogConfig::glue(None, Some(" ".into())), "ns.t")
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("catalog_id"), "{err}");
    }

    #[test]
    fn test_aws_sdk_catalogs_refuse_vended_credentials() {
        // Neither catalog vends: a source that requires vended credentials could
        // only be served by the ambient identity it asked not to use.
        for catalog in [
            CatalogConfig::glue(None, None),
            CatalogConfig::s3_tables(None, S3TABLES_ARN),
        ] {
            let mut config = aws_sdk_config(catalog, "sales.orders");
            config.io.vended_credentials = true;
            let err = config.validate().unwrap_err().to_string();
            assert!(
                err.contains("Vended credentials are not supported"),
                "{err}"
            );
        }
    }

    #[test]
    fn test_aws_regions_must_be_region_shaped() {
        // The SDK puts the region into the service hostname, so anything that is
        // not a plain region label is refused before a client is built.
        for bad in [
            "evil.com",
            "us-east-1.evil.com",
            "x/y",
            "us-east-1:443",
            "US-EAST-1",
            "",
            "-",
        ] {
            let config = aws_sdk_config(CatalogConfig::glue(Some(bad.into()), None), "ns.t");
            assert!(config.validate().is_err(), "region {bad:?} must be refused");
            let mut io_bad = aws_sdk_config(CatalogConfig::glue(None, None), "ns.t");
            io_bad.io.s3_region = Some(bad.into());
            assert!(
                io_bad.validate().is_err(),
                "s3_region {bad:?} must be refused"
            );
        }
        for good in ["us-east-1", "eu-central-2", "us-gov-west-1", "cn-north-1"] {
            validate_aws_region("region", Some(good)).unwrap();
        }
        validate_aws_region("region", None).unwrap();
    }

    #[test]
    fn test_s3tables_bucket_arn_is_parsed_and_names_the_region() {
        assert_eq!(
            s3tables_bucket_arn_region(S3TABLES_ARN).unwrap(),
            "us-east-1"
        );
        assert_eq!(
            s3tables_bucket_arn_region("arn:aws-cn:s3tables:cn-north-1:123456789012:bucket/b")
                .unwrap(),
            "cn-north-1"
        );
        for bad in [
            "not-an-arn",
            "arn:aws:s3:us-east-1:123456789012:bucket/b",
            "arn:aws:s3tables:us-east-1:1234:bucket/b",
            "arn:aws:s3tables:us-east-1:123456789012:table/b",
            "arn:aws:s3tables:us-east-1:123456789012:bucket/",
            "arn:aws:s3tables:evil.com:123456789012:bucket/b",
        ] {
            assert!(
                s3tables_bucket_arn_region(bad).is_err(),
                "{bad} must be refused"
            );
        }

        // With no catalog.region the ARN's region is the catalog's region...
        let arn_only = CatalogConfig::s3_tables(None, S3TABLES_ARN);
        assert_eq!(arn_only.aws_region(), Some("us-east-1"));
        assert_eq!(
            aws_sdk_config(arn_only, "ns.t")
                .storage_io()
                .s3_region
                .as_deref(),
            Some("us-east-1")
        );
        // ...and a region that contradicts the ARN is refused.
        let err = aws_sdk_config(
            CatalogConfig::s3_tables(Some("eu-west-1".into()), S3TABLES_ARN),
            "ns.t",
        )
        .validate()
        .unwrap_err();
        assert!(err.to_string().contains("contradicts"), "{err}");
    }

    #[test]
    fn test_aws_region_resolution_falls_back_both_ways() {
        let io_with = |s3_region: Option<&str>| IoConfig {
            vended_credentials: false,
            s3_region: s3_region.map(str::to_string),
            ..Default::default()
        };
        // Catalog call: the mode's region, else io.s3_region, else the SDK chain.
        let glue = CatalogConfig::glue(Some("us-west-2".into()), None);
        assert_eq!(
            glue.aws_catalog_region(&io_with(Some("eu-west-1"))),
            Some("us-west-2")
        );
        let glue_bare = CatalogConfig::glue(None, None);
        assert_eq!(
            glue_bare.aws_catalog_region(&io_with(Some("eu-west-1"))),
            Some("eu-west-1")
        );
        assert_eq!(glue_bare.aws_catalog_region(&io_with(None)), None);
        // REST / Direct call no AWS catalog API.
        assert_eq!(
            CatalogConfig::rest("https://c").aws_catalog_region(&io_with(Some("x"))),
            None
        );

        // S3 reads: an explicit s3_region wins (data may live elsewhere), else the
        // catalog's region.
        let mut config = aws_sdk_config(glue, "ns.t");
        assert_eq!(config.storage_io().s3_region.as_deref(), Some("us-west-2"));
        config.io.s3_region = Some("eu-west-1".into());
        assert_eq!(config.storage_io().s3_region.as_deref(), Some("eu-west-1"));
        // Other modes read with io as configured.
        let rest = aws_sdk_config(CatalogConfig::rest("https://c"), "ns.t");
        assert_eq!(rest.storage_io().s3_region, None);
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
            delete: None,
            order_by: None,
            model: None,
            default_allow: None,
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
            delete: None,
            order_by: None,
            model: None,
            default_allow: None,
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
            delete: None,
            order_by: None,
            model: None,
            default_allow: None,
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
            delete: None,
            order_by: None,
            model: None,
            default_allow: None,
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("S3 URI"));
    }

    #[test]
    fn test_validate_direct_local_locations_are_fail_closed() {
        // Catalog-less local tables: file:// URIs and bare absolute paths are
        // recognized Direct locations, but reading the local filesystem is
        // opt-in — without `FLUREE_ICEBERG_LOCAL_ROOTS` the config is refused
        // with a message that names the switch, rather than accepted here and
        // failing confusingly at the first scan.
        for loc in [
            "file:///data/warehouse/ns/table",
            "file:/data/warehouse/ns/table",
            "/data/warehouse/ns/table",
        ] {
            let config = IcebergGsConfig {
                catalog: CatalogConfig::direct(loc),
                table: TableConfig::Identifier(String::new()),
                io: IoConfig {
                    vended_credentials: false,
                    ..Default::default()
                },
                mapping: None,
                delete: None,
                order_by: None,
                model: None,
                default_allow: None,
            };
            if crate::local_guard::local_roots().is_none() {
                let err = config
                    .validate()
                    .expect_err("local location must be refused while the allowlist is unset")
                    .to_string();
                assert!(
                    err.contains(crate::local_guard::LOCAL_ROOTS_ENV),
                    "refusal must name the switch that enables local tables: {err}"
                );
                assert!(
                    !err.contains("must be an S3 URI"),
                    "the location is recognized as local, not rejected as malformed: {err}"
                );
            }
            // The table identifier derives from the path's last two segments,
            // same as S3 locations — independent of the allowlist.
            let id = config.table_identifier().unwrap();
            assert_eq!(id.namespace, "ns");
            assert_eq!(id.table, "table");
        }

        // Relative paths are still rejected.
        let config = IcebergGsConfig {
            catalog: CatalogConfig::direct("relative/path/table"),
            table: TableConfig::Identifier(String::new()),
            io: IoConfig {
                vended_credentials: false,
                ..Default::default()
            },
            mapping: None,
            delete: None,
            order_by: None,
            model: None,
            default_allow: None,
        };
        assert!(config.validate().is_err());
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
            delete: None,
            order_by: None,
            model: None,
            default_allow: None,
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Vended credentials"));
    }

    // ── Delete convention ──

    fn dv(values: &[Option<&str>]) -> Vec<Option<String>> {
        values
            .iter()
            .map(|v| v.map(std::string::ToString::to_string))
            .collect()
    }

    #[test]
    fn test_delete_convention_validate() {
        // value-match
        assert!(DeleteConvention {
            column: "_op".to_string(),
            deleted_values: dv(&[Some("d"), Some("delete")]),
        }
        .validate()
        .is_ok());
        // null-payload (null entry)
        assert!(DeleteConvention {
            column: "type".to_string(),
            deleted_values: dv(&[None]),
        }
        .validate()
        .is_ok());
        // empty column rejected
        assert!(DeleteConvention {
            column: String::new(),
            deleted_values: dv(&[Some("d")]),
        }
        .validate()
        .is_err());
        // no delete values rejected
        assert!(DeleteConvention {
            column: "_op".to_string(),
            deleted_values: vec![],
        }
        .validate()
        .is_err());
    }

    #[test]
    fn test_delete_convention_is_tombstone() {
        let value = DeleteConvention {
            column: "_op".to_string(),
            deleted_values: dv(&[Some("d")]),
        };
        assert!(value.is_tombstone(Some("d")));
        assert!(!value.is_tombstone(Some("c")));
        assert!(!value.is_tombstone(None)); // null is NOT a delete unless listed

        let null = DeleteConvention {
            column: "type".to_string(),
            deleted_values: dv(&[None]),
        };
        assert!(null.is_tombstone(None)); // null payload => tombstone
        assert!(!null.is_tombstone(Some("Profile")));

        // both: a value OR null marks a delete
        let both = DeleteConvention {
            column: "_op".to_string(),
            deleted_values: dv(&[Some("d"), None]),
        };
        assert!(both.is_tombstone(Some("d")));
        assert!(both.is_tombstone(None));
        assert!(!both.is_tombstone(Some("u")));
    }

    #[test]
    fn test_delete_convention_serde_default_absent() {
        // A config without `delete` deserializes with delete == None (backward compat).
        let json = r#"{"catalog":{"type":"direct","table_location":"s3://b/w/ns/t"},"table":""}"#;
        let cfg = IcebergGsConfig::from_json(json).unwrap();
        assert!(cfg.delete.is_none());
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
            delete: None,
            order_by: None,
            model: None,
            default_allow: None,
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
            delete: None,
            order_by: None,
            model: None,
            default_allow: None,
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
