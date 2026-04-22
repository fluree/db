//! Iceberg graph source endpoints: POST /fluree/iceberg/map

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
use std::sync::Arc;
use tracing::Instrument;

use super::ledger::forward_write_request;

/// Request body for `POST /fluree/iceberg/map`
#[derive(Deserialize)]
pub struct IcebergMapRequest {
    /// Graph source name
    pub name: String,
    /// Catalog mode: "rest" (default) or "direct"
    #[serde(default = "default_mode")]
    pub mode: String,
    /// REST catalog URI
    pub catalog_uri: Option<String>,
    /// Table identifier (namespace.table)
    pub table: Option<String>,
    /// S3 table location (direct mode)
    pub table_location: Option<String>,
    /// R2RML mapping source
    pub r2rml: Option<String>,
    /// R2RML mapping media type
    pub r2rml_type: Option<String>,
    /// Branch name
    pub branch: Option<String>,
    /// Bearer token for catalog auth
    pub auth_bearer: Option<String>,
    /// OAuth2 token URL
    pub oauth2_token_url: Option<String>,
    /// OAuth2 client ID
    pub oauth2_client_id: Option<String>,
    /// OAuth2 client secret
    pub oauth2_client_secret: Option<String>,
    /// Warehouse identifier
    pub warehouse: Option<String>,
    /// Disable vended credentials
    #[serde(default)]
    pub no_vended_credentials: bool,
    /// S3 region override
    pub s3_region: Option<String>,
    /// S3 endpoint override
    pub s3_endpoint: Option<String>,
    /// Use path-style S3 URLs
    #[serde(default)]
    pub s3_path_style: bool,
}

fn default_mode() -> String {
    "rest".to_string()
}

/// Response for `POST /fluree/iceberg/map`
#[derive(Serialize)]
pub struct IcebergMapResponse {
    pub graph_source_id: String,
    pub table_identifier: String,
    pub catalog_uri: String,
    pub connection_tested: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mapping_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub triples_map_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mapping_validated: Option<bool>,
}

/// Map an Iceberg table as a graph source
///
/// POST /fluree/iceberg/map
pub async fn iceberg_map(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    iceberg_map_local(state, request).await.into_response()
}

async fn iceberg_map_local(state: Arc<AppState>, request: Request) -> Result<impl IntoResponse> {
    let headers_result = FlureeHeaders::from_headers(request.headers());
    let headers = match headers_result {
        Ok(h) => h,
        Err(e) => return Err(e),
    };

    let body_bytes = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    let req: IcebergMapRequest = serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))?;

    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "iceberg:map",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&req.name),
        None,
        None,
    );
    async move {
        tracing::info!(status = "start", name = %req.name, "iceberg map requested");

        let fluree = &state.fluree;
        let iceberg_config = build_iceberg_config(&req)?;

        let response = if let Some(ref r2rml_content) = req.r2rml {
            // R2RML mode — mapping content provided inline
            let config = fluree_db_api::R2rmlCreateConfig {
                iceberg: iceberg_config,
                mapping: fluree_db_api::R2rmlMappingInput::Content(r2rml_content.clone()),
                mapping_media_type: req.r2rml_type.clone(),
            };

            let result = fluree
                .create_r2rml_graph_source(config)
                .await
                .map_err(ServerError::Api)?;

            IcebergMapResponse {
                graph_source_id: result.graph_source_id,
                table_identifier: result.table_identifier,
                catalog_uri: result.catalog_uri,
                connection_tested: result.connection_tested,
                mapping_source: Some(result.mapping_source),
                triples_map_count: Some(result.triples_map_count),
                mapping_validated: Some(result.mapping_validated),
            }
        } else {
            // Raw Iceberg mode
            let result = fluree
                .create_iceberg_graph_source(iceberg_config)
                .await
                .map_err(ServerError::Api)?;

            IcebergMapResponse {
                graph_source_id: result.graph_source_id,
                table_identifier: result.table_identifier,
                catalog_uri: result.catalog_uri,
                connection_tested: result.connection_tested,
                mapping_source: None,
                triples_map_count: None,
                mapping_validated: None,
            }
        };

        tracing::info!(
            status = "success",
            graph_source_id = %response.graph_source_id,
            "iceberg graph source mapped"
        );
        Ok((StatusCode::CREATED, Json(response)))
    }
    .instrument(span)
    .await
}

fn build_iceberg_config(req: &IcebergMapRequest) -> Result<fluree_db_api::IcebergCreateConfig> {
    let mode = req.mode.to_lowercase();
    let mut config = match mode.as_str() {
        "rest" => {
            let catalog_uri = req
                .catalog_uri
                .as_ref()
                .ok_or_else(|| ServerError::bad_request("catalog_uri is required for rest mode"))?;
            let table = req.table.as_deref().unwrap_or_default();
            if table.is_empty() && req.r2rml.is_none() {
                return Err(ServerError::bad_request(
                    "table is required for rest mode (or provide r2rml to define tables via mapping)",
                ));
            }
            let table = if table.is_empty() {
                "default.default"
            } else {
                table
            };
            fluree_db_api::IcebergCreateConfig::new(&req.name, catalog_uri, table)
        }
        "direct" => {
            let location = req.table_location.as_ref().ok_or_else(|| {
                ServerError::bad_request("table_location is required for direct mode")
            })?;
            fluree_db_api::IcebergCreateConfig::new_direct(&req.name, location)
        }
        other => {
            return Err(ServerError::bad_request(format!(
                "unknown catalog mode '{other}'. Use 'rest' or 'direct'."
            )));
        }
    };

    if let Some(ref branch) = req.branch {
        config = config.with_branch(branch);
    }
    if let Some(ref token) = req.auth_bearer {
        config = config.with_auth_bearer(token);
    }
    if let (Some(ref url), Some(ref id), Some(ref secret)) = (
        &req.oauth2_token_url,
        &req.oauth2_client_id,
        &req.oauth2_client_secret,
    ) {
        config = config.with_auth_oauth2(url, id, secret);
    }
    if let Some(ref wh) = req.warehouse {
        config = config.with_warehouse(wh);
    }
    if req.no_vended_credentials {
        config = config.with_vended_credentials(false);
    }
    if let Some(ref region) = req.s3_region {
        config = config.with_s3_region(region);
    }
    if let Some(ref endpoint) = req.s3_endpoint {
        config = config.with_s3_endpoint(endpoint);
    }
    if req.s3_path_style {
        config = config.with_s3_path_style(true);
    }

    Ok(config)
}
