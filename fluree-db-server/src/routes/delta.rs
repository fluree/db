//! Delta graph source endpoints: POST /v1/fluree/delta/map

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
    /// Databricks workspace URL. Tables without a `tables` entry are then
    /// named in Unity Catalog, which places them and issues their credentials.
    pub unity_uri: Option<String>,
    /// Complete a mapped table name of fewer than three parts
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
    /// Model ledger (`name:branch`) whose default graph supplies the source's
    /// view policies and class/property hierarchy.
    pub model: Option<String>,
    /// `default-allow` for governed requests that match no policy.
    pub default_allow: Option<bool>,
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
            req.unity_uri.as_deref(),
            req.oauth2_token_url.as_deref(),
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
    use super::iceberg::secret_value;
    let unity = fluree_db_api::DeltaUnityFields {
        uri: req.unity_uri,
        catalog: req.unity_catalog,
        schema: req.unity_schema,
        bearer: secret_value(
            "auth_bearer",
            req.auth_bearer.as_deref(),
            req.auth_bearer_env.as_deref(),
            allowed_env,
        )?,
        oauth2_client_id: req.oauth2_client_id,
        oauth2_client_secret: secret_value(
            "oauth2_client_secret",
            req.oauth2_client_secret.as_deref(),
            req.oauth2_client_secret_env.as_deref(),
            allowed_env,
        )?,
        oauth2_token_url: req.oauth2_token_url,
        oauth2_scope: req.oauth2_scope,
    }
    .into_config()
    .map_err(ServerError::Api)?;
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
}
