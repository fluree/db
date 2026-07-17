//! Iceberg graph source endpoints: POST /v1/fluree/iceberg/map

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

use super::iceberg_ssrf::guard_connection_urls;
use super::ledger::forward_write_request;

/// Request body for `POST /v1/fluree/iceberg/map`
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
    /// OAuth2 scope (e.g. "session:role:<ROLE>" for Snowflake Horizon / Polaris)
    pub oauth2_scope: Option<String>,
    /// OAuth2 audience
    pub oauth2_audience: Option<String>,
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

/// Response for `POST /v1/fluree/iceberg/map`
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
    pub table_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_names: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mapping_validated: Option<bool>,
}

/// Map an Iceberg table as a graph source
///
/// POST /v1/fluree/iceberg/map
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

        // SSRF guard: reject request-supplied URLs that target internal hosts,
        // before any outbound HTTP client sees them (unauthenticated by default).
        guard_connection_urls(
            req.catalog_uri.as_deref(),
            req.oauth2_token_url.as_deref(),
            req.s3_endpoint.as_deref(),
        )?;

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
                table_count: Some(result.table_count),
                table_names: Some(result.table_names),
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
                table_count: None,
                table_names: None,
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
    // OAuth2 activates on oauth2_token_url + oauth2_client_secret; client_id
    // defaults to "" so Horizon / PAT callers can omit it (Snowflake Horizon's
    // `session:role:` token exchange requires an absent/empty client_id).
    if let (Some(ref url), Some(ref secret)) = (&req.oauth2_token_url, &req.oauth2_client_secret) {
        let id = req.oauth2_client_id.as_deref().unwrap_or("");
        config = config.with_auth_oauth2(url, id, secret);
        if let Some(ref scope) = req.oauth2_scope {
            config = config.with_oauth2_scope(scope);
        }
        if let Some(ref audience) = req.oauth2_audience {
            config = config.with_oauth2_audience(audience);
        }
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

// =============================================================================
// Read-only catalog browse + metadata preview (metadata-only, no graph source
// created). POST-with-read-semantics: the connection carries a secret in the
// body, so these are POSTs, but they mutate nothing.
// =============================================================================

/// The reusable Iceberg connection fields shared by browse/preview requests
/// (a subset of [`IcebergMapRequest`], minus `name`/`table`/`r2rml`).
#[derive(Deserialize)]
pub struct IcebergConnectionRequest {
    /// Catalog mode: "rest" (default) or "direct"
    #[serde(default = "default_mode")]
    pub mode: String,
    /// REST catalog URI
    pub catalog_uri: Option<String>,
    /// S3 table location (direct mode)
    pub table_location: Option<String>,
    /// Bearer token for catalog auth
    pub auth_bearer: Option<String>,
    /// OAuth2 token URL
    pub oauth2_token_url: Option<String>,
    /// OAuth2 client ID
    pub oauth2_client_id: Option<String>,
    /// OAuth2 client secret
    pub oauth2_client_secret: Option<String>,
    /// OAuth2 scope (e.g. "session:role:<ROLE>" for Snowflake Horizon / Polaris)
    pub oauth2_scope: Option<String>,
    /// OAuth2 audience
    pub oauth2_audience: Option<String>,
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

fn build_iceberg_connection(
    req: &IcebergConnectionRequest,
) -> Result<fluree_db_api::IcebergConnectionConfig> {
    use fluree_db_api::IcebergConnectionConfig;

    let mode = req.mode.to_lowercase();
    let mut conn = match mode.as_str() {
        "rest" => {
            let catalog_uri = req
                .catalog_uri
                .as_ref()
                .ok_or_else(|| ServerError::bad_request("catalog_uri is required for rest mode"))?;
            IcebergConnectionConfig::rest(catalog_uri)
        }
        "direct" => {
            let location = req.table_location.as_ref().ok_or_else(|| {
                ServerError::bad_request("table_location is required for direct mode")
            })?;
            IcebergConnectionConfig::direct(location)
        }
        other => {
            return Err(ServerError::bad_request(format!(
                "unknown catalog mode '{other}'. Use 'rest' or 'direct'."
            )));
        }
    };

    if let Some(ref token) = req.auth_bearer {
        conn = conn.with_auth_bearer(token);
    }
    // OAuth2 activates on token_url + client_secret; client_id defaults to ""
    // so Horizon / PAT callers can omit it (mirrors iceberg/map).
    if let (Some(ref url), Some(ref secret)) = (&req.oauth2_token_url, &req.oauth2_client_secret) {
        let id = req.oauth2_client_id.as_deref().unwrap_or("");
        conn = conn.with_auth_oauth2(url, id, secret);
        if let Some(ref scope) = req.oauth2_scope {
            conn = conn.with_oauth2_scope(scope);
        }
        if let Some(ref audience) = req.oauth2_audience {
            conn = conn.with_oauth2_audience(audience);
        }
    }
    if let Some(ref wh) = req.warehouse {
        conn = conn.with_warehouse(wh);
    }
    if req.no_vended_credentials {
        conn = conn.with_vended_credentials(false);
    }
    if let Some(ref region) = req.s3_region {
        conn = conn.with_s3_region(region);
    }
    if let Some(ref endpoint) = req.s3_endpoint {
        conn = conn.with_s3_endpoint(endpoint);
    }
    if req.s3_path_style {
        conn = conn.with_s3_path_style(true);
    }

    Ok(conn)
}

/// Read the request span, parse the JSON body into `T`.
async fn parse_iceberg_body<T: serde::de::DeserializeOwned>(request: Request) -> Result<T> {
    let body_bytes = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))
}

/// Request body for `POST /v1/fluree/iceberg/catalog/browse`
#[derive(Deserialize)]
pub struct IcebergBrowseRequest {
    #[serde(flatten)]
    pub connection: IcebergConnectionRequest,
    /// Browse depth: "namespaces" or "tables" (default "tables")
    pub depth: Option<String>,
}

/// Browse an Iceberg catalog (namespaces + tables). Read-only.
///
/// POST /v1/fluree/iceberg/catalog/browse
pub async fn iceberg_catalog_browse(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    iceberg_catalog_browse_local(state, request)
        .await
        .into_response()
}

async fn iceberg_catalog_browse_local(
    state: Arc<AppState>,
    request: Request,
) -> Result<impl IntoResponse> {
    use fluree_db_api::BrowseDepth;

    let headers = FlureeHeaders::from_headers(request.headers())?;
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);
    let req: IcebergBrowseRequest = parse_iceberg_body(request).await?;

    let span = create_request_span(
        "iceberg:catalog:browse",
        request_id.as_deref(),
        trace_id.as_deref(),
        None,
        None,
        None,
    );
    async move {
        guard_connection_urls(
            req.connection.catalog_uri.as_deref(),
            req.connection.oauth2_token_url.as_deref(),
            req.connection.s3_endpoint.as_deref(),
        )?;
        let conn = build_iceberg_connection(&req.connection)?;
        let depth = match req.depth.as_deref().map(str::to_lowercase).as_deref() {
            Some("namespaces") => BrowseDepth::Namespaces,
            None | Some("tables") => BrowseDepth::Tables,
            Some(other) => {
                return Err(ServerError::bad_request(format!(
                    "unknown depth '{other}'. Use 'namespaces' or 'tables'."
                )));
            }
        };

        let browse = state
            .fluree
            .browse_iceberg_catalog(conn, depth)
            .await
            .map_err(ServerError::Api)?;

        tracing::info!(
            status = "success",
            namespaces = browse.namespaces.len(),
            tables = browse.tables.len(),
            "iceberg catalog browsed"
        );
        Ok((StatusCode::OK, Json(browse)))
    }
    .instrument(span)
    .await
}

/// Request body for `POST /v1/fluree/iceberg/catalog/preview`
#[derive(Deserialize)]
pub struct IcebergPreviewRequest {
    #[serde(flatten)]
    pub connection: IcebergConnectionRequest,
    /// Table namespace (e.g. "DW")
    pub namespace: String,
    /// Table name (e.g. "DIM_STORE")
    pub name: String,
    /// Stats tier: "schema" (Tier-A) or "stats" (Tier-A + Tier-B). Default "schema".
    pub tier: Option<String>,
}

/// Preview an Iceberg table's schema (+ optional per-column stats). Read-only.
///
/// POST /v1/fluree/iceberg/catalog/preview
pub async fn iceberg_catalog_preview(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    iceberg_catalog_preview_local(state, request)
        .await
        .into_response()
}

async fn iceberg_catalog_preview_local(
    state: Arc<AppState>,
    request: Request,
) -> Result<impl IntoResponse> {
    use fluree_db_api::{StatsTier, TableIdentifier};

    let headers = FlureeHeaders::from_headers(request.headers())?;
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);
    let req: IcebergPreviewRequest = parse_iceberg_body(request).await?;

    let span = create_request_span(
        "iceberg:catalog:preview",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&format!("{}.{}", req.namespace, req.name)),
        None,
        None,
    );
    async move {
        guard_connection_urls(
            req.connection.catalog_uri.as_deref(),
            req.connection.oauth2_token_url.as_deref(),
            req.connection.s3_endpoint.as_deref(),
        )?;
        let conn = build_iceberg_connection(&req.connection)?;
        let tier = match req.tier.as_deref().map(str::to_lowercase).as_deref() {
            None | Some("schema") => StatsTier::Schema,
            Some("stats") => StatsTier::Stats,
            Some(other) => {
                return Err(ServerError::bad_request(format!(
                    "unknown tier '{other}'. Use 'schema' or 'stats'."
                )));
            }
        };
        let table = TableIdentifier::new(&req.namespace, &req.name);

        let preview = state
            .fluree
            .preview_iceberg_table(conn, table, tier)
            .await
            .map_err(ServerError::Api)?;

        tracing::info!(
            status = "success",
            table = %format!("{}.{}", req.namespace, req.name),
            columns = preview.schema.columns.len(),
            "iceberg table previewed"
        );
        Ok((StatusCode::OK, Json(preview)))
    }
    .instrument(span)
    .await
}

// =============================================================================
// Deterministic R2RML generation (metadata-only; creates no graph source).
// =============================================================================

/// One `per_table_overrides` entry (its `{namespace, name}` key + values). JSON
/// object keys must be strings, so overrides ride the wire as a list rather than
/// a struct-keyed map.
#[derive(Deserialize)]
pub struct TableOverrideEntry {
    /// Table namespace (e.g. "DW").
    pub namespace: String,
    /// Table name (e.g. "DIM_STORE").
    pub name: String,
    /// Replaces identifier_field_ids with a SINGLE-column subject key (kept for
    /// backward compatibility). Always earns a SubjectKeyUnverified diagnostic.
    #[serde(default)]
    pub primary_key: Option<String>,
    /// Replaces identifier_field_ids with a COMPOSITE (one-or-more-column) subject
    /// key. Takes precedence over `primary_key` when both are present.
    #[serde(default)]
    pub subject_key: Option<Vec<String>>,
    /// Per-table subject-key strategy: `auto` (always emit) or `identifier`
    /// (strict). `null` inherits the request-level `options.subject_strategy`.
    #[serde(default)]
    pub subject_strategy: Option<fluree_db_api::SubjectStrategy>,
    /// Overrides the derived class name / subject slug for the table.
    #[serde(default)]
    pub class_name: Option<String>,
}

/// Request body for `POST /v1/fluree/iceberg/r2rml/generate`.
#[derive(Deserialize)]
pub struct IcebergGenerateRequest {
    #[serde(flatten)]
    pub connection: IcebergConnectionRequest,
    /// Tables to map, in output order: `[{ "namespace": .., "name": .. }]`.
    pub tables: Vec<fluree_db_api::TableIdentifier>,
    /// The SINGLE base namespace all IRIs derive from.
    pub base_namespace: String,
    /// Per-table subject-key / class-name overrides.
    #[serde(default)]
    pub per_table_overrides: Vec<TableOverrideEntry>,
    /// Emit knobs (xsd_long_as_integer / emit_fk_joins / keep_fk_keys_as_literals).
    #[serde(default)]
    pub options: fluree_db_api::GenerateOptions,
    /// RESERVED for PR-4 (target-model IRI rewrite); accepted and ignored.
    #[serde(default)]
    pub target_model_ledger_id: Option<String>,
}

/// Deterministically generate an R2RML mapping over Iceberg tables. Read-only
/// (metadata-only; creates no graph source).
///
/// POST /v1/fluree/iceberg/r2rml/generate
pub async fn iceberg_r2rml_generate(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    iceberg_r2rml_generate_local(state, request)
        .await
        .into_response()
}

async fn iceberg_r2rml_generate_local(
    state: Arc<AppState>,
    request: Request,
) -> Result<impl IntoResponse> {
    use fluree_db_api::{GenerateR2rmlRequest, TableIdentifier, TableOverride};

    let headers = FlureeHeaders::from_headers(request.headers())?;
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);
    let req: IcebergGenerateRequest = parse_iceberg_body(request).await?;

    let span = create_request_span(
        "iceberg:r2rml:generate",
        request_id.as_deref(),
        trace_id.as_deref(),
        None,
        None,
        None,
    );
    async move {
        if req.tables.is_empty() {
            return Err(ServerError::bad_request(
                "at least one table is required for generate",
            ));
        }
        guard_connection_urls(
            req.connection.catalog_uri.as_deref(),
            req.connection.oauth2_token_url.as_deref(),
            req.connection.s3_endpoint.as_deref(),
        )?;
        let connection = build_iceberg_connection(&req.connection)?;
        let per_table_overrides = req
            .per_table_overrides
            .into_iter()
            .map(|e| {
                // A composite `subject_key` wins; else the single `primary_key`
                // is lifted into a one-element column list.
                let primary_key = e.subject_key.or_else(|| e.primary_key.map(|pk| vec![pk]));
                (
                    TableIdentifier::new(e.namespace, e.name),
                    TableOverride {
                        primary_key,
                        class_name: e.class_name,
                        subject_strategy: e.subject_strategy,
                    },
                )
            })
            .collect();

        let api_req = GenerateR2rmlRequest {
            connection,
            tables: req.tables,
            base_namespace: req.base_namespace,
            per_table_overrides,
            options: req.options,
            target_model_ledger_id: req.target_model_ledger_id,
        };

        let response = state
            .fluree
            .generate_r2rml(api_req)
            .await
            .map_err(ServerError::Api)?;

        tracing::info!(
            status = "success",
            tables = response.structured.table_mappings.len(),
            diagnostics = response.diagnostics.len(),
            "iceberg r2rml generated"
        );
        Ok((StatusCode::OK, Json(response)))
    }
    .instrument(span)
    .await
}

/// Request body for `POST /v1/fluree/iceberg/r2rml/validate`
#[derive(Deserialize)]
pub struct IcebergValidateRequest {
    #[serde(flatten)]
    pub connection: IcebergConnectionRequest,
    /// R2RML mapping to validate, in Turtle format.
    pub r2rml: String,
    /// Optional Iceberg snapshot id to validate against. The metadata preview
    /// resolves each table's current snapshot, so this is recorded, not enforced.
    pub snapshot: Option<i64>,
}

/// Validate an R2RML mapping against a live catalog (compile + cross-check).
/// Read-only: creates no graph source, writes nothing.
///
/// POST /v1/fluree/iceberg/r2rml/validate
pub async fn iceberg_r2rml_validate(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    iceberg_r2rml_validate_local(state, request)
        .await
        .into_response()
}

async fn iceberg_r2rml_validate_local(
    state: Arc<AppState>,
    request: Request,
) -> Result<impl IntoResponse> {
    let headers = FlureeHeaders::from_headers(request.headers())?;
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);
    let req: IcebergValidateRequest = parse_iceberg_body(request).await?;

    let span = create_request_span(
        "iceberg:r2rml:validate",
        request_id.as_deref(),
        trace_id.as_deref(),
        None,
        None,
        None,
    );
    async move {
        guard_connection_urls(
            req.connection.catalog_uri.as_deref(),
            req.connection.oauth2_token_url.as_deref(),
            req.connection.s3_endpoint.as_deref(),
        )?;
        let conn = build_iceberg_connection(&req.connection)?;

        let response = state
            .fluree
            .validate_r2rml(conn, req.r2rml, req.snapshot)
            .await
            .map_err(ServerError::Api)?;

        tracing::info!(
            status = "success",
            compiled_ok = response.compiled_ok,
            triples_maps = response.triples_map_count,
            diagnostics = response.diagnostics.len(),
            "iceberg r2rml validated"
        );
        Ok((StatusCode::OK, Json(response)))
    }
    .instrument(span)
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_deserializes_oauth2_scope_and_reaches_auth_config() {
        // Omit client_id (Horizon case); provide token_url + secret + scope.
        let body = serde_json::json!({
            "name": "gs",
            "mode": "rest",
            "catalog_uri": "https://catalog.example.com",
            "table": "ns.tbl",
            "oauth2_token_url": "https://catalog.example.com/v1/oauth/tokens",
            "oauth2_client_secret": "pat",
            "oauth2_scope": "session:role:ICEBERG_READER",
            "oauth2_audience": "polaris"
        });
        let req: IcebergMapRequest = serde_json::from_value(body).unwrap();
        assert_eq!(
            req.oauth2_scope.as_deref(),
            Some("session:role:ICEBERG_READER")
        );
        assert_eq!(req.oauth2_audience.as_deref(), Some("polaris"));

        let config = build_iceberg_config(&req).unwrap();
        let gs = config.to_iceberg_gs_config();
        let v = serde_json::to_value(&gs).unwrap();
        let auth = &v["catalog"]["auth"];

        assert_eq!(auth["type"], "oauth2_client_credentials");
        assert_eq!(auth["client_id"], ""); // defaulted to empty
        assert_eq!(auth["scope"], "session:role:ICEBERG_READER");
        assert_eq!(auth["audience"], "polaris");
    }

    #[test]
    fn request_without_secret_does_not_activate_oauth2() {
        let body = serde_json::json!({
            "name": "gs",
            "catalog_uri": "https://catalog.example.com",
            "table": "ns.tbl",
            "oauth2_token_url": "https://catalog.example.com/v1/oauth/tokens"
        });
        let req: IcebergMapRequest = serde_json::from_value(body).unwrap();
        let config = build_iceberg_config(&req).unwrap();
        let gs = config.to_iceberg_gs_config();
        let v = serde_json::to_value(&gs).unwrap();
        assert_eq!(v["catalog"]["auth"]["type"], "none");
    }

    #[test]
    fn browse_request_flattens_connection_and_builds_config() {
        // The flattened connection fields must deserialize alongside `depth`,
        // and build a REST connection carrying the OAuth2 scope.
        let body = serde_json::json!({
            "mode": "rest",
            "catalog_uri": "https://catalog.example.com",
            "warehouse": "wh1",
            "oauth2_token_url": "https://catalog.example.com/v1/oauth/tokens",
            "oauth2_client_secret": "pat",
            "oauth2_scope": "session:role:ICEBERG_READER",
            "depth": "namespaces"
        });
        let req: IcebergBrowseRequest = serde_json::from_value(body).unwrap();
        assert_eq!(req.depth.as_deref(), Some("namespaces"));
        assert_eq!(req.connection.warehouse.as_deref(), Some("wh1"));

        // Build a create config from the same connection so we can inspect the
        // serialized auth block (the server crate can't name fluree_db_iceberg
        // types directly).
        let create = fluree_db_api::IcebergCreateConfig {
            name: "gs".to_string(),
            branch: None,
            connection: build_iceberg_connection(&req.connection).unwrap(),
            table_identifier: "ns.tbl".to_string(),
        };
        assert!(create.is_rest());
        let gs = create.to_iceberg_gs_config();
        let v = serde_json::to_value(&gs).unwrap();
        assert_eq!(v["catalog"]["warehouse"], "wh1");
        assert_eq!(v["catalog"]["auth"]["type"], "oauth2_client_credentials");
        assert_eq!(v["catalog"]["auth"]["scope"], "session:role:ICEBERG_READER");
    }

    #[test]
    fn browse_request_direct_mode_builds_direct_connection() {
        let body = serde_json::json!({
            "mode": "direct",
            "table_location": "s3://bucket/warehouse/ns/table"
        });
        let req: IcebergBrowseRequest = serde_json::from_value(body).unwrap();
        assert!(req.depth.is_none());
        let conn = build_iceberg_connection(&req.connection).unwrap();
        assert!(conn.is_direct());
    }

    #[test]
    fn generate_request_flattens_connection_tables_overrides_and_options() {
        // The flattened connection fields deserialize alongside the generate
        // body: tables, base_namespace, an overrides list, options, and the
        // reserved target_model_ledger_id.
        let body = serde_json::json!({
            "mode": "rest",
            "catalog_uri": "https://catalog.example.com",
            "warehouse": "wh1",
            "oauth2_token_url": "https://catalog.example.com/v1/oauth/tokens",
            "oauth2_client_secret": "pat",
            "oauth2_scope": "session:role:ICEBERG_READER",
            "tables": [
                {"namespace": "DW", "name": "DIM_GEOGRAPHY"},
                {"namespace": "DW", "name": "DIM_SUPPLIER"}
            ],
            "base_namespace": "http://ns.fluree.dev/edw#",
            "per_table_overrides": [
                {"namespace": "DW", "name": "DIM_SUPPLIER", "primary_key": "ALT_KEY"}
            ],
            "options": {"xsd_long_as_integer": false},
            "target_model_ledger_id": "model:main"
        });
        let req: IcebergGenerateRequest = serde_json::from_value(body).unwrap();

        assert_eq!(req.tables.len(), 2);
        assert_eq!(req.tables[0].namespace, "DW");
        assert_eq!(req.tables[0].name, "DIM_GEOGRAPHY");
        assert_eq!(req.base_namespace, "http://ns.fluree.dev/edw#");

        assert_eq!(req.per_table_overrides.len(), 1);
        assert_eq!(req.per_table_overrides[0].name, "DIM_SUPPLIER");
        assert_eq!(
            req.per_table_overrides[0].primary_key.as_deref(),
            Some("ALT_KEY")
        );
        assert!(req.per_table_overrides[0].class_name.is_none());

        // Explicit `false` overrides the default; the other knobs default `true`.
        assert!(!req.options.xsd_long_as_integer);
        assert!(req.options.emit_fk_joins);
        assert!(req.options.keep_fk_keys_as_literals);

        assert_eq!(req.target_model_ledger_id.as_deref(), Some("model:main"));

        // The flattened connection builds a REST connection carrying the scope.
        let conn = build_iceberg_connection(&req.connection).unwrap();
        assert!(conn.is_rest());
    }

    #[test]
    fn generate_request_defaults_overrides_and_options() {
        // Overrides and options are optional; options default to all-`true`.
        let body = serde_json::json!({
            "mode": "rest",
            "catalog_uri": "https://catalog.example.com",
            "tables": [{"namespace": "DW", "name": "DIM_DATE"}],
            "base_namespace": "http://ns.fluree.dev/edw#"
        });
        let req: IcebergGenerateRequest = serde_json::from_value(body).unwrap();
        assert!(req.per_table_overrides.is_empty());
        assert!(req.options.xsd_long_as_integer);
        assert!(req.options.emit_fk_joins);
        assert!(req.options.keep_fk_keys_as_literals);
        assert!(req.target_model_ledger_id.is_none());
    }
}
