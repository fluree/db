//! Delta graph source endpoints: `delta/map`, and the read-only `delta/catalog/*`
//! and `delta/r2rml/*` that lead up to it.

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::FlureeHeaders;
use crate::state::AppState;
use crate::telemetry::{create_request_span, extract_request_id, extract_trace_id};
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::Instrument;

use super::ledger::forward_write_request;

/// Request body for `POST /v1/fluree/delta/map`
#[derive(Deserialize)]
pub struct DeltaMapRequest {
    /// Graph source name
    pub name: String,
    /// Directory the mapping's `rr:tableName`s resolve beneath
    /// (`dbo.orders` → `<root>/dbo/orders`)
    pub root: Option<String>,
    /// Explicit table name → table location
    #[serde(default)]
    pub tables: BTreeMap<String, String>,
    /// R2RML mapping content (Turtle by default)
    pub r2rml: String,
    /// R2RML mapping media type
    pub r2rml_type: Option<String>,
    /// Branch name
    pub branch: Option<String>,
    pub s3_region: Option<String>,
    /// S3 endpoint override (MinIO, LocalStack, …)
    pub s3_endpoint: Option<String>,
    #[serde(default)]
    pub s3_path_style: bool,
    /// Microsoft Entra service principal for `abfss://` locations. Omit all
    /// four to use the server's ambient Azure credentials.
    pub azure_tenant_id: Option<String>,
    pub azure_client_id: Option<String>,
    /// Literal client secret (stored with the graph source)
    pub azure_client_secret: Option<String>,
    /// Name of a server environment variable holding the client secret
    pub azure_client_secret_env: Option<String>,
    #[serde(flatten)]
    pub unity: UnityConnectionRequest,
    /// Model ledger (`name:branch`) whose default graph supplies the source's
    /// view policies and class/property hierarchy.
    pub model: Option<String>,
    /// `default-allow` for governed requests that match no policy.
    pub default_allow: Option<bool>,
}

/// A Unity Catalog connection, as every Delta endpoint spells it.
#[derive(Deserialize, Default)]
pub struct UnityConnectionRequest {
    /// Databricks workspace URL. In a `map` request, tables without a `tables`
    /// entry are then named in Unity Catalog, which places them and issues
    /// their credentials.
    pub unity_uri: Option<String>,
    /// Complete a table name of fewer than three parts; scope a listing
    pub unity_catalog: Option<String>,
    pub unity_schema: Option<String>,
    /// Catalog auth, as for `iceberg/map`: a bearer token, or the client id and
    /// secret of a service principal. An `_env` field names a server
    /// environment variable listed in `FLUREE_GRAPH_SOURCE_SECRET_ENV_VARS`.
    pub auth_bearer: Option<String>,
    pub auth_bearer_env: Option<String>,
    pub oauth2_client_id: Option<String>,
    pub oauth2_client_secret: Option<String>,
    pub oauth2_client_secret_env: Option<String>,
    /// Defaults to the workspace's token endpoint
    pub oauth2_token_url: Option<String>,
    /// Defaults to `all-apis`
    pub oauth2_scope: Option<String>,
}

impl UnityConnectionRequest {
    /// `None` when the request names no catalog.
    fn into_config(self, allowed_env: &str) -> Result<Option<fluree_db_api::DeltaUnityConfig>> {
        use super::iceberg::secret_value;
        fluree_db_api::DeltaUnityFields {
            bearer: secret_value(
                "auth_bearer",
                self.auth_bearer.as_deref(),
                self.auth_bearer_env.as_deref(),
                allowed_env,
            )?,
            oauth2_client_secret: secret_value(
                "oauth2_client_secret",
                self.oauth2_client_secret.as_deref(),
                self.oauth2_client_secret_env.as_deref(),
                allowed_env,
            )?,
            uri: self.unity_uri,
            catalog: self.unity_catalog,
            schema: self.unity_schema,
            oauth2_client_id: self.oauth2_client_id,
            oauth2_token_url: self.oauth2_token_url,
            oauth2_scope: self.oauth2_scope,
        }
        .into_config()
        .map_err(ServerError::Api)
    }

    /// For an endpoint that is about the catalog, and so needs one.
    fn required(self, allowed_env: &str) -> Result<fluree_db_api::DeltaUnityConfig> {
        self.into_config(allowed_env)?
            .ok_or_else(|| ServerError::bad_request("`unity_uri` is required"))
    }
}

/// Response for `POST /v1/fluree/delta/map`
#[derive(Serialize)]
pub struct DeltaMapResponse {
    pub graph_source_id: String,
    pub mapping_source: String,
    pub triples_map_count: usize,
    pub table_count: usize,
    pub table_names: Vec<String>,
    pub mapping_validated: bool,
    /// Current Delta version of each mapped table that opened at registration.
    pub table_versions: BTreeMap<String, u64>,
    /// Mapped tables that could not be read at registration, and why.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub table_warnings: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_warnings: Vec<String>,
}

/// Map Delta tables as a graph source
///
/// POST /v1/fluree/delta/map
pub async fn delta_map(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }
    delta_map_local(state, request).await.into_response()
}

async fn delta_map_local(state: Arc<AppState>, request: Request) -> Result<impl IntoResponse> {
    let headers = FlureeHeaders::from_headers(request.headers())?;
    let body_bytes = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    let req: DeltaMapRequest = serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))?;

    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);
    let span = create_request_span(
        "delta:map",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&req.name),
        None,
        None,
    );
    async move {
        tracing::info!(status = "start", name = %req.name, "delta map requested");

        // Each of these reaches an outbound HTTP client.
        super::iceberg_ssrf::guard_connection_urls(
            req.unity.unity_uri.as_deref(),
            req.unity.oauth2_token_url.as_deref(),
            req.s3_endpoint.as_deref(),
        )?;

        let result = state
            .fluree
            .create_delta_graph_source(build_delta_config(req)?)
            .await
            .map_err(ServerError::Api)?;

        tracing::info!(
            status = "success",
            graph_source_id = %result.graph_source_id,
            "delta graph source mapped"
        );
        Ok((
            StatusCode::CREATED,
            Json(DeltaMapResponse {
                graph_source_id: result.graph_source_id,
                mapping_source: result.mapping_source,
                triples_map_count: result.triples_map_count,
                table_count: result.table_names.len(),
                table_names: result.table_names,
                mapping_validated: result.mapping_validated,
                table_versions: result.table_versions,
                table_warnings: result.table_warnings,
                model_warnings: result.model_warnings,
            }),
        ))
    }
    .instrument(span)
    .await
}

fn build_delta_config(req: DeltaMapRequest) -> Result<fluree_db_api::DeltaCreateConfig> {
    build_delta_config_allowing(req, &super::iceberg::allowed_secret_env())
}

fn build_delta_config_allowing(
    req: DeltaMapRequest,
    allowed_env: &str,
) -> Result<fluree_db_api::DeltaCreateConfig> {
    let unity = req.unity.into_config(allowed_env)?;
    let azure = fluree_db_api::DeltaAzureFields {
        tenant_id: req.azure_tenant_id,
        client_id: req.azure_client_id,
        client_secret: req.azure_client_secret,
        client_secret_env: req.azure_client_secret_env,
    }
    .into_auth()
    .map_err(ServerError::Api)?;
    Ok(fluree_db_api::DeltaCreateConfig {
        unity,
        name: req.name,
        branch: req.branch,
        root: req.root,
        tables: req.tables,
        io: fluree_db_api::DeltaIoConfig {
            s3_region: req.s3_region,
            s3_endpoint: req.s3_endpoint,
            s3_path_style: req.s3_path_style,
            azure,
        },
        mapping: fluree_db_api::R2rmlMappingInput::Content(req.r2rml),
        mapping_media_type: req.r2rml_type,
        model: req.model,
        default_allow: req.default_allow,
    })
}

// =============================================================================
// Read-only: the catalog, and a mapping before it is registered
// =============================================================================

async fn parse_body<T: serde::de::DeserializeOwned>(request: Request) -> Result<T> {
    let bytes = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    serde_json::from_slice(&bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))
}

fn span_for(operation: &str, state: &AppState, request: &Request) -> Result<tracing::Span> {
    let headers = FlureeHeaders::from_headers(request.headers())?;
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);
    Ok(create_request_span(
        operation,
        request_id.as_deref(),
        trace_id.as_deref(),
        None,
        None,
        None,
    ))
}

/// Storage options for a table Unity places; its credentials are Unity's.
#[derive(Deserialize, Default)]
pub struct UnityStorageRequest {
    pub s3_region: Option<String>,
    pub s3_endpoint: Option<String>,
    #[serde(default)]
    pub s3_path_style: bool,
}

/// Request body for `POST /v1/fluree/delta/catalog/browse`
#[derive(Deserialize)]
pub struct DeltaBrowseRequest {
    #[serde(flatten)]
    pub unity: UnityConnectionRequest,
    /// `schemas` or `tables` (the default): how far a listing of one catalog
    /// reaches. With no `unity_catalog` the catalogs are listed; with a
    /// `unity_schema` too, that schema's tables.
    #[serde(default)]
    pub depth: fluree_db_api::DeltaBrowseDepth,
}

/// Request body for `POST /v1/fluree/delta/catalog/preview` and `…/verify`
#[derive(Deserialize)]
pub struct DeltaTableRequest {
    #[serde(flatten)]
    pub unity: UnityConnectionRequest,
    /// Completed from `unity_catalog` and `unity_schema` when short
    pub table: String,
    #[serde(flatten)]
    pub storage: UnityStorageRequest,
}

#[derive(Deserialize)]
pub struct DeltaTableOverride {
    /// As the table is named in `tables`
    pub table: String,
    /// Columns of the subject, in place of the declared or chosen key
    #[serde(default)]
    pub subject_key: Option<Vec<String>>,
    #[serde(default)]
    pub class_name: Option<String>,
    #[serde(default)]
    pub subject_strategy: Option<fluree_db_api::SubjectStrategy>,
}

/// Request body for `POST /v1/fluree/delta/r2rml/generate`
#[derive(Deserialize)]
pub struct DeltaGenerateRequest {
    #[serde(flatten)]
    pub unity: UnityConnectionRequest,
    /// In output order
    pub tables: Vec<String>,
    /// The base every generated IRI derives from
    pub base_namespace: String,
    #[serde(default)]
    pub per_table_overrides: Vec<DeltaTableOverride>,
    #[serde(default)]
    pub options: fluree_db_api::GenerateOptions,
}

fn guard_unity(unity: &UnityConnectionRequest, s3_endpoint: Option<&str>) -> Result<()> {
    super::iceberg_ssrf::guard_connection_urls(
        unity.unity_uri.as_deref(),
        unity.oauth2_token_url.as_deref(),
        s3_endpoint,
    )
}

/// POST /v1/fluree/delta/catalog/browse
pub async fn delta_catalog_browse(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    async {
        let span = span_for("delta:catalog:browse", &state, &request)?;
        let req: DeltaBrowseRequest = parse_body(request).await?;
        async {
            guard_unity(&req.unity, None)?;
            let unity = req.unity.required(&super::iceberg::allowed_secret_env())?;
            let listing = state
                .fluree
                .browse_delta_unity(&unity, req.depth)
                .await
                .map_err(ServerError::Api)?;
            Ok::<_, ServerError>((StatusCode::OK, Json(listing)))
        }
        .instrument(span)
        .await
    }
    .await
    .into_response()
}

/// POST /v1/fluree/delta/catalog/preview
pub async fn delta_catalog_preview(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    async {
        let span = span_for("delta:catalog:preview", &state, &request)?;
        let req: DeltaTableRequest = parse_body(request).await?;
        async {
            guard_unity(&req.unity, None)?;
            let unity = req.unity.required(&super::iceberg::allowed_secret_env())?;
            let preview = state
                .fluree
                .preview_delta_unity_table(&unity, &req.table)
                .await
                .map_err(ServerError::Api)?;
            Ok::<_, ServerError>((StatusCode::OK, Json(preview)))
        }
        .instrument(span)
        .await
    }
    .await
    .into_response()
}

/// POST /v1/fluree/delta/catalog/verify
pub async fn delta_catalog_verify(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    async {
        let span = span_for("delta:catalog:verify", &state, &request)?;
        let req: DeltaTableRequest = parse_body(request).await?;
        async {
            guard_unity(&req.unity, req.storage.s3_endpoint.as_deref())?;
            let unity = req.unity.required(&super::iceberg::allowed_secret_env())?;
            let io = fluree_db_api::DeltaIoConfig {
                s3_region: req.storage.s3_region,
                s3_endpoint: req.storage.s3_endpoint,
                s3_path_style: req.storage.s3_path_style,
                azure: None,
            };
            let access = state
                .fluree
                .verify_delta_unity_table(&unity, &io, &req.table)
                .await
                .map_err(ServerError::Api)?;
            tracing::info!(table = %access.full_name, readable = access.readable, "delta table verified");
            Ok::<_, ServerError>((StatusCode::OK, Json(access)))
        }
        .instrument(span)
        .await
    }
    .await
    .into_response()
}

fn generate_request(
    req: DeltaGenerateRequest,
    allowed_env: &str,
) -> Result<fluree_db_api::GenerateDeltaR2rmlRequest> {
    if req.tables.is_empty() {
        return Err(ServerError::bad_request(
            "at least one table is required for generate",
        ));
    }
    Ok(fluree_db_api::GenerateDeltaR2rmlRequest {
        unity: req.unity.required(allowed_env)?,
        tables: req.tables,
        base_namespace: req.base_namespace,
        per_table_overrides: req
            .per_table_overrides
            .into_iter()
            .map(|o| {
                let table_override = fluree_db_api::TableOverride {
                    primary_key: o.subject_key,
                    class_name: o.class_name,
                    subject_strategy: o.subject_strategy,
                };
                (o.table, table_override)
            })
            .collect(),
        options: req.options,
    })
}

/// POST /v1/fluree/delta/r2rml/generate
pub async fn delta_r2rml_generate(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    async {
        let span = span_for("delta:r2rml:generate", &state, &request)?;
        let req: DeltaGenerateRequest = parse_body(request).await?;
        async {
            guard_unity(&req.unity, None)?;
            let api_req = generate_request(req, &super::iceberg::allowed_secret_env())?;
            let response = state
                .fluree
                .generate_delta_r2rml(api_req)
                .await
                .map_err(ServerError::Api)?;
            tracing::info!(
                tables = response.tables.len(),
                diagnostics = response.diagnostics.len(),
                "delta r2rml generated"
            );
            Ok::<_, ServerError>((StatusCode::OK, Json(response)))
        }
        .instrument(span)
        .await
    }
    .await
    .into_response()
}

/// A `delta/map` body, whose `name` validation has no use for.
fn validate_request(mut body: serde_json::Value) -> Result<DeltaMapRequest> {
    if let Some(fields) = body.as_object_mut() {
        fields.entry("name").or_insert_with(|| "validate".into());
    }
    serde_json::from_value(body).map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))
}

/// Check a mapping against the tables a `delta/map` of the same body would
/// read. Registers nothing.
///
/// POST /v1/fluree/delta/r2rml/validate
pub async fn delta_r2rml_validate(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    async {
        let span = span_for("delta:r2rml:validate", &state, &request)?;
        let req = validate_request(parse_body(request).await?)?;
        async {
            guard_unity(&req.unity, req.s3_endpoint.as_deref())?;
            let response = state
                .fluree
                .validate_delta_r2rml(&build_delta_config(req)?)
                .await
                .map_err(ServerError::Api)?;
            Ok::<_, ServerError>((StatusCode::OK, Json(response)))
        }
        .instrument(span)
        .await
    }
    .await
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(extra: serde_json::Value) -> DeltaMapRequest {
        let mut body = serde_json::json!({"name": "sales", "r2rml": ""});
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        serde_json::from_value(body).unwrap()
    }

    fn stored(config: &fluree_db_api::DeltaCreateConfig) -> serde_json::Value {
        serde_json::to_value(config.to_gs_config("m.ttl")).unwrap()
    }

    #[test]
    fn a_unity_request_stores_its_catalog_and_a_listed_variables_name() {
        let req = request(serde_json::json!({
            "unity_uri": "https://workspace.example.com",
            "unity_catalog": "main",
            "oauth2_client_id": "app",
            "oauth2_client_secret_env": "SP_SECRET",
        }));
        let config = build_delta_config_allowing(req, "SP_SECRET").unwrap();
        config.validate().unwrap();
        let unity = &stored(&config)["unity"];
        assert_eq!(unity["uri"], "https://workspace.example.com");
        assert_eq!(unity["catalog"], "main");
        assert_eq!(unity["auth"]["client_secret"]["env_var"], "SP_SECRET");
        assert_eq!(
            unity["auth"]["token_url"],
            "https://workspace.example.com/oidc/v1/token"
        );
    }

    #[test]
    fn a_unity_secret_may_name_only_a_listed_variable() {
        for field in ["auth_bearer_env", "oauth2_client_secret_env"] {
            let req = request(serde_json::json!({
                "unity_uri": "https://workspace.example.com",
                "oauth2_client_id": "app",
                field: "AWS_SECRET_ACCESS_KEY",
            }));
            let err = build_delta_config_allowing(req, "SP_SECRET")
                .err()
                .unwrap_or_else(|| panic!("{field} accepted"));
            assert!(
                err.to_string()
                    .contains(super::super::iceberg::SECRET_ENV_ALLOWLIST),
                "{err}"
            );
        }
    }

    #[test]
    fn a_source_by_path_is_built_as_before() {
        let config = build_delta_config_allowing(
            request(serde_json::json!({"root": "s3://lake/Tables"})),
            "",
        )
        .unwrap();
        assert!(config.unity.is_none());
        assert!(stored(&config).get("unity").is_none());
    }

    fn generate(extra: serde_json::Value) -> Result<fluree_db_api::GenerateDeltaR2rmlRequest> {
        let mut body = serde_json::json!({
            "unity_uri": "https://workspace.example.com",
            "unity_catalog": "main",
            "auth_bearer_env": "UNITY_TOKEN",
            "tables": ["sales.orders", "sales.customers"],
            "base_namespace": "https://example.org/",
        });
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        generate_request(serde_json::from_value(body).unwrap(), "UNITY_TOKEN")
    }

    #[test]
    fn a_generate_request_carries_its_tables_overrides_and_options() {
        let req = generate(serde_json::json!({
            "per_table_overrides": [
                {"table": "sales.orders", "subject_key": ["order_id", "line"], "class_name": "Purchase"},
                {"table": "sales.customers", "subject_strategy": "identifier"},
            ],
            "options": {"emit_fk_joins": false},
        }))
        .unwrap();
        assert_eq!(req.tables, ["sales.orders", "sales.customers"]);
        assert_eq!(req.unity.catalog.as_deref(), Some("main"));
        let orders = &req.per_table_overrides["sales.orders"];
        assert_eq!(
            orders.primary_key.as_deref(),
            Some(&["order_id".to_string(), "line".to_string()][..])
        );
        assert_eq!(orders.class_name.as_deref(), Some("Purchase"));
        assert_eq!(
            req.per_table_overrides["sales.customers"].subject_strategy,
            Some(fluree_db_api::SubjectStrategy::Identifier)
        );
        assert!(!req.options.emit_fk_joins);
        // An option left out keeps its default.
        assert!(req.options.keep_fk_keys_as_literals);
    }

    #[test]
    fn a_catalog_endpoint_needs_a_catalog_and_a_listed_variable() {
        assert!(generate(serde_json::json!({})).is_ok());
        let stray = generate(serde_json::json!({"unity_uri": null}));
        assert!(stray.err().unwrap().to_string().contains("workspace URL"));
        let none = generate(serde_json::json!({
            "unity_uri": null, "unity_catalog": null, "auth_bearer_env": null,
        }));
        assert!(none.err().unwrap().to_string().contains("unity_uri"));
        let unlisted = generate(serde_json::json!({"auth_bearer_env": "AWS_SECRET_ACCESS_KEY"}));
        assert!(unlisted
            .err()
            .unwrap()
            .to_string()
            .contains(super::super::iceberg::SECRET_ENV_ALLOWLIST));
    }

    #[test]
    fn a_validate_body_is_a_map_body_whose_name_is_optional() {
        let unnamed = validate_request(serde_json::json!({"root": "s3://lake/t", "r2rml": "x"}));
        assert_eq!(unnamed.unwrap().name, "validate");
        let named = validate_request(
            serde_json::json!({"name": "sales", "root": "s3://lake/t", "r2rml": "x"}),
        );
        assert_eq!(named.unwrap().name, "sales");
        assert!(validate_request(serde_json::json!({"root": "s3://lake/t"})).is_err());
    }
}
