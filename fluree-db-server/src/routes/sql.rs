//! SQL graph source endpoints: POST /v1/fluree/sql/map

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

/// Request body for `POST /v1/fluree/sql/map`
#[derive(Deserialize)]
pub struct SqlMapRequest {
    /// Graph source name
    pub name: String,
    /// Statement endpoint base URL (`https://trino.example.com`, or a sidecar)
    pub endpoint: String,
    /// R2RML mapping content (Turtle by default)
    pub r2rml: String,
    /// R2RML mapping media type
    pub r2rml_type: Option<String>,
    /// Branch name
    pub branch: Option<String>,
    /// Rendering dialect: `trino` (default), `postgres`, `mysql`, `sqlite`
    pub dialect: Option<String>,
    /// Header family: `trino` (default) or `presto`
    pub protocol: Option<String>,
    /// Default catalog for unqualified table names
    pub catalog: Option<String>,
    /// Default schema for unqualified table names
    pub schema: Option<String>,
    /// `X-Trino-User` (defaults to `fluree`)
    pub user: Option<String>,
    /// Static bearer token
    pub auth_bearer: Option<String>,
    /// OAuth2 client-credentials token URL
    pub oauth2_token_url: Option<String>,
    pub oauth2_client_id: Option<String>,
    pub oauth2_client_secret: Option<String>,
    pub oauth2_scope: Option<String>,
    pub oauth2_audience: Option<String>,
    /// Session properties (`X-Trino-Session`)
    #[serde(default)]
    pub session: BTreeMap<String, String>,
    /// Model ledger (`name:branch`) whose default graph supplies the source's
    /// view policies and class/property hierarchy.
    pub model: Option<String>,
}

/// Response for `POST /v1/fluree/sql/map`
#[derive(Serialize)]
pub struct SqlMapResponse {
    pub graph_source_id: String,
    pub endpoint: String,
    pub connection_tested: bool,
    pub mapping_source: String,
    pub triples_map_count: usize,
    pub table_count: usize,
    pub table_names: Vec<String>,
    pub mapping_validated: bool,
}

/// Map a SQL endpoint as a graph source
///
/// POST /v1/fluree/sql/map
pub async fn sql_map(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }
    sql_map_local(state, request).await.into_response()
}

async fn sql_map_local(state: Arc<AppState>, request: Request) -> Result<impl IntoResponse> {
    let headers = FlureeHeaders::from_headers(request.headers())?;
    let body_bytes = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    let req: SqlMapRequest = serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))?;

    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);
    let span = create_request_span(
        "sql:map",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&req.name),
        None,
        None,
    );
    async move {
        tracing::info!(status = "start", name = %req.name, "sql map requested");

        // The endpoint reaches an outbound HTTP client: refuse the
        // link-local/metadata range before anything connects. (Loopback and
        // private hosts are legitimate — a sidecar is the common deployment.)
        fluree_db_api::validate_sql_endpoint(&req.endpoint)
            .map_err(|e| ServerError::bad_request(e.to_string()))?;
        if let Some(url) = &req.oauth2_token_url {
            super::iceberg_ssrf::guard_connection_urls(None, Some(url), None)?;
        }

        let config = build_sql_config(&req)?;
        let result = state
            .fluree
            .create_sql_graph_source(config)
            .await
            .map_err(ServerError::Api)?;

        tracing::info!(
            status = "success",
            graph_source_id = %result.graph_source_id,
            "sql graph source mapped"
        );
        Ok((
            StatusCode::CREATED,
            Json(SqlMapResponse {
                graph_source_id: result.graph_source_id,
                endpoint: result.endpoint,
                connection_tested: result.connection_tested,
                mapping_source: result.mapping_source,
                triples_map_count: result.triples_map_count,
                table_count: result.table_count,
                table_names: result.table_names,
                mapping_validated: result.mapping_validated,
            }),
        ))
    }
    .instrument(span)
    .await
}

fn build_sql_config(req: &SqlMapRequest) -> Result<fluree_db_api::SqlCreateConfig> {
    use fluree_db_api::{SqlAuthConfig, SqlDialect, WireProtocol};

    let mut config = fluree_db_api::SqlCreateConfig::new(&req.name, &req.endpoint, &req.r2rml);
    config.branch = req.branch.clone();
    config.mapping_media_type = req.r2rml_type.clone();
    config.catalog = req.catalog.clone();
    config.schema = req.schema.clone();
    config.user = req.user.clone();
    config.session = req.session.clone();
    config.model = req.model.clone();

    if let Some(d) = &req.dialect {
        config.dialect = match d.to_lowercase().as_str() {
            "trino" => SqlDialect::Trino,
            "postgres" | "postgresql" => SqlDialect::Postgres,
            "mysql" => SqlDialect::Mysql,
            "sqlite" => SqlDialect::Sqlite,
            other => {
                return Err(ServerError::bad_request(format!(
                    "unknown dialect '{other}'. Use trino, postgres, mysql or sqlite."
                )))
            }
        };
    }
    if let Some(p) = &req.protocol {
        config.protocol = match p.to_lowercase().as_str() {
            "trino" => WireProtocol::Trino,
            "presto" => WireProtocol::Presto,
            other => {
                return Err(ServerError::bad_request(format!(
                    "unknown protocol '{other}'. Use trino or presto."
                )))
            }
        };
    }

    if let (Some(url), Some(secret)) = (&req.oauth2_token_url, &req.oauth2_client_secret) {
        config.auth = SqlAuthConfig::OAuth2ClientCredentials {
            token_url: url.clone(),
            client_id: fluree_db_sql_config_value(req.oauth2_client_id.as_deref().unwrap_or("")),
            client_secret: fluree_db_sql_config_value(secret),
            scope: req.oauth2_scope.clone(),
            audience: req.oauth2_audience.clone(),
        };
    } else if let Some(token) = &req.auth_bearer {
        config.auth = SqlAuthConfig::Bearer {
            token: fluree_db_sql_config_value(token),
        };
    }
    Ok(config)
}

fn fluree_db_sql_config_value(literal: &str) -> fluree_db_api::SqlConfigValue {
    fluree_db_api::SqlConfigValue::Literal(literal.to_string())
}
