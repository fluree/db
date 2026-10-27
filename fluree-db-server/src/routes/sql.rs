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
    /// Session properties (`X-Trino-Session`)
    #[serde(default)]
    pub session: BTreeMap<String, String>,
    /// Model ledger (`name:branch`) whose default graph supplies the source's
    /// view policies and class/property hierarchy.
    pub model: Option<String>,
    /// `default-allow` for governed requests that match no policy.
    pub default_allow: Option<bool>,
    /// Accept subject keys the registration probe finds non-unique.
    #[serde(default)]
    pub allow_duplicate_subjects: Option<bool>,
    /// The endpoint's auth: the structured `auth` (exactly the stored
    /// config's shape, Google identities included), or the flat fields
    /// `auth_bearer` / `auth_bearer_env` / `oauth2_token_url` /
    /// `oauth2_client_id` / `oauth2_client_secret` / `oauth2_client_secret_env`
    /// / `oauth2_scope` / `oauth2_audience`. See
    /// [`fluree_db_api::sql_auth_from_json`].
    #[serde(flatten)]
    pub auth: serde_json::Map<String, serde_json::Value>,
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
    /// Warnings about the `model` reference (policies a virtual source cannot
    /// evaluate).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_warnings: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mapping_warnings: Vec<String>,
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

        let config = build_sql_config(&req, &fluree_db_api::SecretRules::server(&state.fluree))?;
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
                model_warnings: result.model_warnings,
                mapping_warnings: result.mapping_warnings,
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

fn build_sql_config(
    req: &SqlMapRequest,
    rules: &fluree_db_api::SecretRules,
) -> Result<fluree_db_api::SqlCreateConfig> {
    use fluree_db_api::{SqlDialect, WireProtocol};

    let mut config = fluree_db_api::SqlCreateConfig::new(&req.name, &req.endpoint, &req.r2rml);
    config.branch = req.branch.clone();
    config.mapping_media_type = req.r2rml_type.clone();
    config.catalog = req.catalog.clone();
    config.schema = req.schema.clone();
    config.user = req.user.clone();
    config.session = req.session.clone();
    config.model = req.model.clone();
    config.default_allow = req.default_allow;
    config.allow_duplicate_subjects = req.allow_duplicate_subjects.unwrap_or(false);

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

    config.auth = fluree_db_api::sql_auth_from_json(req.auth.clone(), rules)
        .map_err(super::iceberg::input_error)?;
    // The token URL reaches an outbound HTTP client.
    if let Some(url) = super::iceberg_ssrf::token_url(&config.auth) {
        super::iceberg_ssrf::guard_connection_urls(None, Some(url), None)?;
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(extra: serde_json::Value) -> SqlMapRequest {
        let mut body = serde_json::json!({
            "name": "orders", "endpoint": "https://trino.example.com", "r2rml": "",
        });
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        serde_json::from_value(body).unwrap()
    }

    fn stored_auth(req: &SqlMapRequest, listed: &str) -> Result<serde_json::Value> {
        let rules = fluree_db_api::SecretRules::listed(listed, false);
        Ok(serde_json::to_value(build_sql_config(req, &rules)?.auth).unwrap())
    }

    #[test]
    fn a_sql_secret_can_name_a_listed_variable() {
        // SQL secrets were literal-only over HTTP.
        let auth = stored_auth(
            &request(serde_json::json!({"auth_bearer_env": "TRINO_TOKEN"})),
            "TRINO_TOKEN",
        )
        .unwrap();
        assert_eq!(auth["token"]["env_var"], "TRINO_TOKEN");
        let refused = stored_auth(
            &request(
                serde_json::json!({"oauth2_token_url": "https://t.example.com/token",
                "oauth2_client_secret_env": "AWS_SECRET_ACCESS_KEY"}),
            ),
            "TRINO_TOKEN",
        );
        assert!(refused
            .unwrap_err()
            .to_string()
            .contains(fluree_db_api::SECRET_ENV_VARS_ENV));
    }

    #[test]
    fn sql_auth_is_the_same_flat_or_structured_and_takes_google() {
        let flat = stored_auth(
            &request(
                serde_json::json!({"oauth2_token_url": "https://t.example.com/token",
                "oauth2_client_id": "app", "oauth2_client_secret": "s"}),
            ),
            "",
        )
        .unwrap();
        let structured = stored_auth(
            &request(
                serde_json::json!({"auth": {"type": "oauth2_client_credentials",
                "token_url": "https://t.example.com/token", "client_id": "app",
                "client_secret": "s"}}),
            ),
            "",
        )
        .unwrap();
        assert_eq!(flat, structured);
        let google = stored_auth(
            &request(serde_json::json!({"auth": {"type": "google",
                "credentials": {"source": "metadata"}}})),
            "",
        )
        .unwrap();
        assert_eq!(google["type"], "google");
    }

    #[test]
    fn a_sql_auth_given_two_ways_or_half_given_is_refused() {
        for extra in [
            // OAuth2 used to win over the bearer silently.
            serde_json::json!({"auth_bearer": "t", "oauth2_token_url": "https://t.example.com",
                "oauth2_client_secret": "s"}),
            // A token URL alone used to store no auth.
            serde_json::json!({"oauth2_token_url": "https://t.example.com"}),
            serde_json::json!({"auth_bearr": "t"}),
        ] {
            assert!(stored_auth(&request(extra.clone()), "").is_err(), "{extra}");
        }
    }

    #[test]
    fn a_structured_token_url_is_held_to_the_ssrf_guard() {
        let e = stored_auth(
            &request(
                serde_json::json!({"auth": {"type": "oauth2_client_credentials",
                "token_url": "http://169.254.169.254/token", "client_id": "",
                "client_secret": "s"}}),
            ),
            "",
        )
        .unwrap_err();
        assert!(e.to_string().contains("SSRF guard"), "{e}");
    }
}
