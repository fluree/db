//! Query endpoints: /fluree/query, /fluree/explain, /:ledger/query
//!
//! Supports both JSON-LD and SPARQL query content types:
//! - `application/json`: JSON-LD query format (JSON body with "from" field)
//! - `application/sparql-query`: SPARQL query syntax (raw SPARQL string in body)
//!
//! For SPARQL UPDATE operations, use the update endpoints instead.

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::{tracking_headers, FlureeHeaders, MaybeCredential, MaybeDataBearer};
// Note: NeedsRefresh is no longer used - replaced by FreshnessSource trait
use crate::state::AppState;
use crate::telemetry::{
    create_request_span, extract_request_id, extract_trace_id, log_query_text, set_span_error_code,
    should_log_query_text,
};
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::Json;
use fluree_db_api::dataset::GraphSelector;
use fluree_db_api::{
    DatasetSpec, FreshnessCheck, FreshnessSource, GraphDb, GraphSource, LedgerState, TimeSpec,
    TrackingTally,
};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::Instrument;

// ============================================================================
// SPARQL Protocol query parameter support (GET ?query=...)
// ============================================================================

/// Optional URL query parameters for W3C SPARQL Protocol compliance.
///
/// The SPARQL Protocol (RFC 3986) allows queries via:
///   GET /sparql?query=SELECT+...&default-graph-uri=...
///
/// When `query` is present and the request body is empty, the query parameter
/// value is used as the SPARQL query string.
#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct SparqlParams {
    /// The SPARQL query string (URL-encoded)
    pub query: Option<String>,
    /// Opt in to using the ledger's stored default context for this request.
    ///
    /// Defaults to false. For JSON-LD, this injects the context only when the
    /// query omits `@context`/`context`. For ledger-scoped SPARQL, this makes
    /// default prefixes available when the query has no explicit `PREFIX`.
    #[serde(
        default,
        alias = "default_context",
        alias = "use-default-context",
        alias = "use_default_context"
    )]
    pub default_context: bool,
    /// Optional default graph URI (part of W3C SPARQL Protocol).
    // Kept for: full SPARQL Protocol compliance — BSBM and other tools may send this param.
    // Use when: implementing default-graph-uri scoping in query execution.
    #[expect(dead_code)]
    pub default_graph_uri: Option<String>,
}

/// If a `?query=` URL parameter is present and the credential body is empty,
/// return the query param value as the SPARQL string. Otherwise fall back to
/// the credential body.
fn resolve_sparql_text(params: &SparqlParams, credential: &MaybeCredential) -> Result<String> {
    // Prefer ?query= parameter when body is empty (standard SPARQL Protocol GET)
    if let Some(ref q) = params.query {
        let body = credential.body_string().unwrap_or_default();
        if body.trim().is_empty() {
            return Ok(q.clone());
        }
    }
    // Fall back to request body
    credential.body_string()
}

/// Check if the request should be treated as SPARQL based on headers OR the
/// presence of a `?query=` URL parameter.
fn is_sparql_request(
    headers: &FlureeHeaders,
    credential: &MaybeCredential,
    params: &SparqlParams,
) -> bool {
    headers.is_sparql_query() || credential.is_sparql || params.query.is_some()
}

// ============================================================================
// Data API Auth Helpers
// ============================================================================

/// Resolve the effective request identity for policy enforcement.
///
/// Precedence:
/// 1) Signed request DID (credential)
/// 2) Bearer token identity (fluree.identity ?? sub)
fn effective_identity(credential: &MaybeCredential, bearer: &MaybeDataBearer) -> Option<String> {
    credential
        .did()
        .map(std::string::ToString::to_string)
        .or_else(|| bearer.0.as_ref().and_then(|p| p.identity.clone()))
}

/// Check if tracking is requested in query opts
fn has_tracking_opts(query_json: &JsonValue) -> bool {
    let Some(opts) = query_json.get("opts") else {
        return false;
    };

    // Check for meta (tracking) options
    // - meta: true enables all tracking
    // - meta: {time: true, ...} enables selective tracking
    // - meta: false or meta: {} should NOT enable tracking
    if let Some(meta) = opts.get("meta") {
        match meta {
            JsonValue::Bool(true) => return true,
            JsonValue::Object(obj) if !obj.is_empty() => return true,
            _ => {} // meta: false or meta: {} - don't enable tracking
        }
    }

    // max-fuel implicitly enables fuel tracking
    if opts.get("max-fuel").is_some()
        || opts.get("max_fuel").is_some()
        || opts.get("maxFuel").is_some()
    {
        return true;
    }

    false
}

/// Check if the query opts request identity-based policy enforcement.
///
/// Returns true when `opts.identity` or `opts.policy-class` is present.
/// These fields trigger policy lookup in the connection execution path;
/// the plain GraphDb path does not process them.
fn has_policy_opts(query_json: &JsonValue) -> bool {
    let Some(opts) = query_json.get("opts") else {
        return false;
    };
    opts.get("identity").is_some()
        || opts.get("policy-class").is_some()
        || opts.get("policy").is_some()
}

/// Helper to extract ledger ID from request (for JSON-LD queries)
fn get_ledger_id(
    path_ledger: Option<&str>,
    headers: &FlureeHeaders,
    body: &JsonValue,
) -> Result<String> {
    // Priority: path > header > body.from
    if let Some(ledger) = path_ledger {
        return Ok(ledger.to_string());
    }

    if let Some(ledger) = &headers.ledger {
        return Ok(ledger.clone());
    }

    if let Some(from) = body.get("from").and_then(|v| v.as_str()) {
        return Ok(from.to_string());
    }

    Err(ServerError::MissingLedger)
}

/// Inject header values into query JSON (modifies the query in place)
fn inject_headers_into_query(query: &mut JsonValue, headers: &FlureeHeaders) {
    if let Some(obj) = query.as_object_mut() {
        // Get or create opts object
        let opts = obj
            .entry("opts")
            .or_insert_with(|| JsonValue::Object(serde_json::Map::new()));

        if let Some(opts_obj) = opts.as_object_mut() {
            headers.inject_into_opts(opts_obj);
        }
    }
}

fn jsonld_query_has_context(query: &JsonValue) -> bool {
    query.get("@context").is_some() || query.get("context").is_some()
}

async fn inject_default_context_if_requested(
    state: &AppState,
    ledger_id: &str,
    query: &mut JsonValue,
    use_default_context: bool,
) -> Result<()> {
    if !use_default_context || jsonld_query_has_context(query) {
        return Ok(());
    }

    if let Some(ctx) = state
        .fluree
        .get_default_context(ledger_id)
        .await
        .map_err(ServerError::Api)?
    {
        if let Some(obj) = query.as_object_mut() {
            obj.insert("@context".to_string(), ctx);
        }
    }
    Ok(())
}

async fn attach_default_context_to_graph(
    state: &AppState,
    ledger_id: &str,
    graph: GraphDb,
    use_default_context: bool,
) -> Result<GraphDb> {
    if !use_default_context {
        return Ok(graph);
    }
    let ctx = state
        .fluree
        .get_default_context(ledger_id)
        .await
        .map_err(ServerError::Api)?;
    Ok(graph.with_default_context(ctx))
}

/// Execute a query
///
/// POST /fluree/query
/// GET /fluree/query
///
/// Supports:
/// - JSON-LD queries (JSON body with "from" field for ledger)
/// - SPARQL queries (Content-Type: application/sparql-query)
/// - Signed requests (JWS/VC format with Content-Type: application/jwt)
///   - Connection-scoped: requires FROM clause in SPARQL to specify ledger
pub async fn query(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SparqlParams>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    credential: MaybeCredential,
) -> Result<impl IntoResponse> {
    // Create request span with correlation context
    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);
    let trace_id = extract_trace_id(&credential.headers);

    // Detect input format before span creation so otel.name is set at open time
    let input_format = if is_sparql_request(&headers, &credential, &params) {
        "sparql"
    } else {
        "json-ld"
    };

    let span = create_request_span(
        "query",
        request_id.as_deref(),
        trace_id.as_deref(),
        None, // ledger ID determined later
        None, // tenant_id not yet supported
        Some(input_format),
    );
    async move {
    let span = tracing::Span::current();

    tracing::info!(status = "start", "query request received");

    // Enforce data auth if configured (Bearer token OR signed request)
    let data_auth = state.config.data_auth();
    if data_auth.mode == crate::config::DataAuthMode::Required
        && !credential.is_signed()
        && bearer.0.is_none()
    {
        set_span_error_code(&span, "error:Unauthorized");
        return Err(ServerError::unauthorized(
            "Authentication required (signed request or Bearer token)",
        ));
    }
    // SPARQL UPDATE should use the update endpoint, not query
    if headers.is_sparql_update() || credential.is_sparql_update {
        let error = ServerError::bad_request(
            "SPARQL UPDATE requests should use the /v1/fluree/update endpoint, not /v1/fluree/query",
        );
        set_span_error_code(&span, "error:BadRequest");
        tracing::warn!(error = %error, "SPARQL UPDATE sent to query endpoint");
        return Err(error);
    }

    let delimited = wants_delimited(&headers);

    // Handle SPARQL query
    if is_sparql_request(&headers, &credential, &params) {
        // Connection-scoped SPARQL returns pre-formatted JSON — delimited not supported
        if let Some(fmt) = delimited {
            return Err(ServerError::not_acceptable(format!(
                "{} format not supported for connection-scoped SPARQL queries. \
                     Use the /:ledger/query endpoint instead.",
                fmt.name().to_uppercase()
            )));
        }

        let sparql = resolve_sparql_text(&params, &credential)?;

        // Log query text according to configuration
        log_query_text(&sparql, &state.telemetry_config, &span);

        // Connection-scoped SPARQL requires a FROM/FROM NAMED clause to specify the ledger.
        //
        // NOTE: We intentionally do NOT fall back to the fluree-ledger header here.
        // Ledger-scoped SPARQL without FROM is supported via the /:ledger/query route.

        // Enforce bearer ledger scope for unsigned SPARQL requests
        if let Some(p) = bearer.0.as_ref() {
            if !credential.is_signed() {
                // Extract ledger IDs from FROM/FROM NAMED clauses.
                // Parse failure → fall through (let the engine produce a proper error).
                if let Ok(ledger_ids) = fluree_db_api::sparql_dataset_ledger_ids(&sparql) {
                    for ledger_id in &ledger_ids {
                        if !p.can_read(ledger_id) {
                            return Err(ServerError::not_found("Ledger not found"));
                        }
                    }
                }
            }
        }

        // AgentJson: connection-scoped SPARQL with agent-optimized envelope
        if headers.wants_agent_json() {
            let parsed = fluree_db_sparql::parse_sparql(&sparql);
            let from_count = parsed.ast.as_ref()
                .and_then(|ast| match &ast.body {
                    fluree_db_sparql::ast::QueryBody::Select(q) => q.dataset.as_ref(),
                    fluree_db_sparql::ast::QueryBody::Construct(q) => q.dataset.as_ref(),
                    fluree_db_sparql::ast::QueryBody::Ask(q) => q.dataset.as_ref(),
                    fluree_db_sparql::ast::QueryBody::Describe(q) => q.dataset.as_ref(),
                    fluree_db_sparql::ast::QueryBody::Update(_) => None,
                })
                .map(|d| d.default_graphs.len())
                .unwrap_or(0);
            let agent_ctx = fluree_db_api::AgentJsonContext {
                sparql_text: Some(sparql.to_string()),
                from_count,
                iso_timestamp: Some(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
                ..Default::default()
            };
            let mut config = fluree_db_api::FormatterConfig::agent_json()
                .with_agent_json_context(agent_ctx);
            if let Some(max_bytes) = headers.max_bytes() {
                config = config.with_max_bytes(max_bytes);
            }
            let result = state.fluree.query_from().sparql(&sparql).format(config).execute_formatted().await;
            return match result {
                Ok(json) => {
                    tracing::info!(status = "success", query_kind = "sparql", format = "agent-json");
                    let content_type = "application/vnd.fluree.agent+json; charset=utf-8";
                    Ok(([(axum::http::header::CONTENT_TYPE, content_type)], Json(json)).into_response())
                }
                Err(e) => {
                    let server_error = ServerError::Api(e);
                    set_span_error_code(&span, "error:InvalidQuery");
                    tracing::error!(error = %server_error, query_kind = "sparql", "query failed");
                    Err(server_error)
                }
            };
        }

        match state.fluree.query_from().sparql(&sparql).execute_formatted().await {
            Ok(result) => {
                tracing::info!(
                    status = "success",
                    query_kind = "sparql",
                    result_count = result.as_array().map(std::vec::Vec::len).unwrap_or(0)
                );
                Ok((HeaderMap::new(), Json(result)).into_response())
            }
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(&span, "error:InvalidQuery");
                tracing::error!(error = %server_error, query_kind = "sparql", "query failed");
                Err(server_error)
            }
        }
    } else {
        // Handle JSON-LD query (JSON body)
        let mut query_json: JsonValue = credential.body_json()?;

        // Log query text according to configuration (only serialize if needed)
        if should_log_query_text(&state.telemetry_config) {
            if let Ok(query_text) = serde_json::to_string(&query_json) {
                log_query_text(&query_text, &state.telemetry_config, &span);
            }
        }

        // Get ledger id
        let ledger_id = match get_ledger_id(None, &headers, &query_json) {
            Ok(ledger_id) => {
                span.record("ledger_id", ledger_id.as_str());
                ledger_id
            }
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "missing ledger ID");
                return Err(e);
            }
        };

        // Inject header values into query opts
        inject_headers_into_query(&mut query_json, &headers);

        // Enforce bearer ledger scope for unsigned requests
        if let Some(p) = bearer.0.as_ref() {
            if !credential.is_signed() && !p.can_read(&ledger_id) {
                set_span_error_code(&span, "error:Forbidden");
                // Avoid existence leak
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

        // Apply bearer identity + server-default policy-class to opts, honoring
        // the root-identity impersonation semantic (see routes::policy_auth).
        let identity = effective_identity(&credential, &bearer);
        let policy_class = data_auth.default_policy_class.as_deref();
        crate::routes::policy_auth::apply_auth_identity_to_opts(
            &state,
            &ledger_id,
            &mut query_json,
            identity.as_deref(),
            policy_class,
        )
        .await;

        inject_default_context_if_requested(
            &state,
            &ledger_id,
            &mut query_json,
            params.default_context,
        )
        .await?;

        execute_query(&state, &ledger_id, &query_json, delimited).await
    }
    }
    .instrument(span)
    .await
}

/// Execute a query with ledger in path
///
/// POST /:ledger/query
/// GET /:ledger/query
///
/// Supports:
/// - JSON-LD queries (JSON body)
/// - SPARQL queries (Content-Type: application/sparql-query)
/// - Signed requests (JWS/VC format)
///   - Ledger-scoped: FROM clause is optional (ledger from path is used)
pub async fn query_ledger(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    Query(params): Query<SparqlParams>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    credential: MaybeCredential,
) -> Result<impl IntoResponse> {
    // Create request span with correlation context
    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);
    let trace_id = extract_trace_id(&credential.headers);

    let input_format = if is_sparql_request(&headers, &credential, &params) {
        "sparql"
    } else {
        "json-ld"
    };

    let span = create_request_span(
        "query",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&ledger),
        None, // tenant_id not yet supported
        Some(input_format),
    );
    async move {
    let span = tracing::Span::current();

    tracing::info!(status = "start", "ledger query request received");

    // Enforce data auth if configured (Bearer token OR signed request)
    let data_auth = state.config.data_auth();
    if data_auth.mode == crate::config::DataAuthMode::Required
        && !credential.is_signed()
        && bearer.0.is_none()
    {
        set_span_error_code(&span, "error:Unauthorized");
        return Err(ServerError::unauthorized(
            "Authentication required (signed request or Bearer token)",
        ));
    }

    // SPARQL UPDATE should use the update endpoint, not query
    if headers.is_sparql_update() || credential.is_sparql_update {
        let error = ServerError::bad_request(
            "SPARQL UPDATE requests should use the /v1/fluree/update/<ledger...> endpoint, not /v1/fluree/query/<ledger...>",
        );
        set_span_error_code(&span, "error:BadRequest");
        tracing::warn!(error = %error, "SPARQL UPDATE sent to query endpoint");
        return Err(error);
    }

    let delimited = wants_delimited(&headers);

    // Handle SPARQL query - ledger is known from path
    if is_sparql_request(&headers, &credential, &params) {
        let sparql = resolve_sparql_text(&params, &credential)?;

        // Log query text according to configuration
        log_query_text(&sparql, &state.telemetry_config, &span);

        // Enforce bearer ledger scope for unsigned requests
        if let Some(p) = bearer.0.as_ref() {
            if !credential.is_signed() && !p.can_read(&ledger) {
                set_span_error_code(&span, "error:Forbidden");
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

        let bearer_identity = effective_identity(&credential, &bearer);
        let identity = crate::routes::policy_auth::resolve_sparql_identity(
            &state,
            &ledger,
            bearer_identity.as_deref(),
            headers.identity.as_deref(),
        )
        .await;
        return execute_sparql_ledger(
            &state,
            &ledger,
            &sparql,
            identity.as_deref(),
            delimited,
            &headers,
            params.default_context,
        )
            .await;
    }

    // Handle JSON-LD query (JSON body)
    let mut query_json: JsonValue = credential.body_json()?;

    // Log query text according to configuration (only serialize if needed)
    if should_log_query_text(&state.telemetry_config) {
        if let Ok(query_text) = serde_json::to_string(&query_json) {
            log_query_text(&query_text, &state.telemetry_config, &span);
        }
    }

    // Get ledger id (path takes precedence)
    let ledger_id = match get_ledger_id(Some(&ledger), &headers, &query_json) {
        Ok(ledger_id) => {
            span.record("ledger_id", ledger_id.as_str());
            ledger_id
        }
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "ledger ID mismatch");
            return Err(e);
        }
    };

    // Ledger-scoped endpoint: allow `from` as a named-graph selector, but reject
    // attempts to target a different ledger than the URL.
    normalize_ledger_scoped_from(&ledger_id, &mut query_json)?;

    // Inject header values into query opts
    inject_headers_into_query(&mut query_json, &headers);

    // Enforce bearer ledger scope for unsigned requests
    if let Some(p) = bearer.0.as_ref() {
        if !credential.is_signed() && !p.can_read(&ledger) {
            set_span_error_code(&span, "error:Forbidden");
            return Err(ServerError::not_found("Ledger not found"));
        }
    }

    // Apply bearer identity + server-default policy-class to opts, honoring
    // the root-identity impersonation semantic (see routes::policy_auth).
    let identity = effective_identity(&credential, &bearer);
    let policy_class = data_auth.default_policy_class.as_deref();
    crate::routes::policy_auth::apply_auth_identity_to_opts(
        &state,
        &ledger_id,
        &mut query_json,
        identity.as_deref(),
        policy_class,
    )
    .await;

    inject_default_context_if_requested(
        &state,
        &ledger_id,
        &mut query_json,
        params.default_context,
    )
    .await?;

    execute_query(&state, &ledger_id, &query_json, delimited).await
    }
    .instrument(span)
    .await
}

/// Execute a query with ledger as greedy tail segment.
///
/// POST /fluree/query/<ledger...>
/// GET /fluree/query/<ledger...>
///
/// This avoids ambiguity when ledger names contain `/`.
pub async fn query_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    params: Query<SparqlParams>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    credential: MaybeCredential,
) -> Result<impl IntoResponse> {
    query_ledger(
        State(state),
        Path(ledger),
        params,
        headers,
        bearer,
        credential,
    )
    .await
}

/// Explain a query plan with ledger in path.
///
/// POST /:ledger/explain
/// GET /:ledger/explain
///
/// Supports:
/// - JSON-LD queries (JSON body)
/// - SPARQL queries (Content-Type: application/sparql-query)
/// - Signed requests (JWS/VC format)
pub async fn explain_ledger(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    Query(params): Query<SparqlParams>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    credential: MaybeCredential,
) -> Result<Json<JsonValue>> {
    // Create request span with correlation context
    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);
    let trace_id = extract_trace_id(&credential.headers);

    let input_format = if is_sparql_request(&headers, &credential, &params) {
        "sparql"
    } else {
        "json-ld"
    };

    let span = create_request_span(
        "explain",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&ledger),
        None, // tenant_id not yet supported
        Some(input_format),
    );
    async move {
        let span = tracing::Span::current();

        tracing::info!(status = "start", "ledger explain request received");

        // Enforce data auth if configured (Bearer token OR signed request)
        let data_auth = state.config.data_auth();
        if data_auth.mode == crate::config::DataAuthMode::Required
            && !credential.is_signed()
            && bearer.0.is_none()
        {
            set_span_error_code(&span, "error:Unauthorized");
            return Err(ServerError::unauthorized(
                "Authentication required (signed request or Bearer token)",
            ));
        }

        // SPARQL UPDATE should use the update endpoint, not explain
        if headers.is_sparql_update() || credential.is_sparql_update {
            let error = ServerError::bad_request(
                "SPARQL UPDATE requests should use the /v1/fluree/update endpoint, not /v1/fluree/explain",
            );
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %error, "SPARQL UPDATE sent to explain endpoint");
            return Err(error);
        }

        // Handle SPARQL explain
        if is_sparql_request(&headers, &credential, &params) {
            let sparql = resolve_sparql_text(&params, &credential)?;
            log_query_text(&sparql, &state.telemetry_config, &span);

            // Enforce bearer ledger scope for unsigned requests
            if let Some(p) = bearer.0.as_ref() {
                if !credential.is_signed() && !p.can_read(&ledger) {
                    set_span_error_code(&span, "error:Forbidden");
                    return Err(ServerError::not_found("Ledger not found"));
                }
            }

            // For now, explain is ledger-scoped and does not support dataset clauses.
            if fluree_db_api::sparql_dataset_ledger_ids(&sparql)
                .map(|v| !v.is_empty())
                .unwrap_or(false)
            {
                return Err(ServerError::bad_request(
                    "SPARQL FROM/FROM NAMED is not supported for explain on the ledger-scoped endpoint; remove dataset clauses to explain the core plan"
                        .to_string(),
                ));
            }

            let ledger_id = ledger.clone();
            let loaded = if state.config.is_proxy_storage_mode() {
                state.fluree.ledger(&ledger_id).await.map_err(ServerError::Api)?
            } else {
                load_ledger_for_query(&state, &ledger_id, &span).await?
            };
            let db = fluree_db_api::GraphDb::from_ledger_state(&loaded);
            let result = state
                .fluree
                .explain_sparql(&db, &sparql)
                .await
                .map_err(ServerError::Api)?;

            tracing::info!(status = "success", query_kind = "sparql", "explain completed");
            return Ok(Json(result));
        }

        // Handle JSON-LD explain (JSON body)
        let mut query_json: JsonValue = credential.body_json()?;
        if should_log_query_text(&state.telemetry_config) {
            if let Ok(query_text) = serde_json::to_string(&query_json) {
                log_query_text(&query_text, &state.telemetry_config, &span);
            }
        }

        let ledger_id = match get_ledger_id(Some(&ledger), &headers, &query_json) {
            Ok(ledger_id) => {
                span.record("ledger_id", ledger_id.as_str());
                ledger_id
            }
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "ledger ID mismatch");
                return Err(e);
            }
        };

        // Ledger-scoped endpoint: allow `from` as a named-graph selector, but reject
        // attempts to target a different ledger than the URL.
        normalize_ledger_scoped_from(&ledger_id, &mut query_json)?;

        // Inject header values into query opts
        inject_headers_into_query(&mut query_json, &headers);

        // Enforce bearer ledger scope for unsigned requests
        if let Some(p) = bearer.0.as_ref() {
            if !credential.is_signed() && !p.can_read(&ledger) {
                set_span_error_code(&span, "error:Forbidden");
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

        // Apply bearer identity + server-default policy-class to opts, honoring
        // the root-identity impersonation semantic (see routes::policy_auth).
        let identity = effective_identity(&credential, &bearer);
        let policy_class = data_auth.default_policy_class.as_deref();
        crate::routes::policy_auth::apply_auth_identity_to_opts(
            &state,
            &ledger_id,
            &mut query_json,
            identity.as_deref(),
            policy_class,
        )
        .await;

        let loaded = if state.config.is_proxy_storage_mode() {
            state.fluree.ledger(&ledger_id).await.map_err(ServerError::Api)?
        } else {
            load_ledger_for_query(&state, &ledger_id, &span).await?
        };
        let db = fluree_db_api::GraphDb::from_ledger_state(&loaded);
        let result = state
            .fluree
            .explain(&db, &query_json)
            .await
            .map_err(ServerError::Api)?;

        tracing::info!(status = "success", query_kind = "jsonld", "explain completed");
        Ok(Json(result))
    }
    .instrument(span)
    .await
}

/// Explain a query plan with ledger as greedy tail segment.
///
/// POST /fluree/explain/<ledger...>
/// GET /fluree/explain/<ledger...>
///
/// This avoids ambiguity when ledger names contain `/`.
pub async fn explain_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    params: Query<SparqlParams>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    credential: MaybeCredential,
) -> Result<Json<JsonValue>> {
    explain_ledger(
        State(state),
        Path(ledger),
        params,
        headers,
        bearer,
        credential,
    )
    .await
}

/// Check if a query requires dataset features (multi-ledger, named graphs, etc.)
///
/// Dataset features that require the connection execution path:
/// - `fromNamed` / `from-named`: Named graphs in the dataset
/// - `from` as array: Multiple default graphs
/// - `from` as object with special fields: graph selector, alias, time-travel
fn requires_dataset_features(query: &JsonValue) -> bool {
    // Check for fromNamed (new) or from-named (legacy)
    if query.get("fromNamed").is_some() || query.get("from-named").is_some() {
        return true;
    }

    // Check the structure of "from"
    if let Some(from) = query.get("from") {
        // Array of sources = multiple default graphs
        if from.is_array() {
            return true;
        }

        // String with time-travel or graph fragment requires dataset parsing
        // so the server can apply time travel and/or named graph selection.
        if let Some(s) = from.as_str() {
            if s.contains('@') || s.contains('#') {
                return true;
            }
        }

        // Object form with special keys (graph, alias, t, iso, sha, etc.)
        if let Some(obj) = from.as_object() {
            // Any key other than just @id indicates dataset features
            let has_special_keys = obj.keys().any(|k| !matches!(k.as_str(), "@id"));
            if has_special_keys {
                return true;
            }
        }
    }

    false
}

fn iri_to_string(iri: &fluree_db_sparql::ast::Iri) -> String {
    use fluree_db_sparql::ast::IriValue;
    match &iri.value {
        IriValue::Full(s) => s.to_string(),
        IriValue::Prefixed { prefix, local } => {
            if prefix.is_empty() {
                format!(":{local}")
            } else {
                format!("{prefix}:{local}")
            }
        }
    }
}

fn split_graph_fragment(s: &str) -> (&str, Option<&str>) {
    match s.split_once('#') {
        Some((base, frag)) => (base, Some(frag)),
        None => (s, None),
    }
}

fn base_ledger_id(s: &str) -> Result<String> {
    let (no_frag, _frag) = split_graph_fragment(s);
    let (base, _time) = fluree_db_core::ledger_id::split_time_travel_suffix(no_frag)
        .map_err(|e| ServerError::bad_request(format!("Invalid time travel in ledger ref: {e}")))?;
    Ok(base)
}

fn looks_like_graph_selector_only(s: &str) -> bool {
    // Ledger IDs typically look like `name:branch` and do NOT include `://`.
    // Graph IRIs commonly include `://` or `urn:` and should be treated as selectors.
    matches!(s, "default" | "txn-meta")
        || s.contains("://")
        || s.starts_with("urn:")
        || (!s.contains(':') && !s.contains('@') && !s.contains('#'))
}

fn normalize_ledger_scoped_from(ledger_id: &str, query: &mut JsonValue) -> Result<()> {
    let Some(obj) = query.as_object_mut() else {
        return Ok(());
    };
    let Some(from_val) = obj.get("from").cloned() else {
        return Ok(());
    };

    match from_val {
        JsonValue::String(s) => {
            // 1) If it's a pure graph selector (e.g. txn-meta / default / graph name),
            // treat it as "graph within this ledger".
            if looks_like_graph_selector_only(&s) {
                let mut src = serde_json::Map::new();
                src.insert("@id".to_string(), JsonValue::String(ledger_id.to_string()));
                src.insert("graph".to_string(), JsonValue::String(s));
                obj.insert("from".to_string(), JsonValue::Object(src));
                return Ok(());
            }

            // 2) If it encodes ledger + optional time/fragment, require base ledger match.
            let base = base_ledger_id(&s)?;
            let base_path = base_ledger_id(ledger_id)?;
            if base != base_path {
                return Err(ServerError::bad_request(format!(
                    "Ledger mismatch: endpoint ledger is '{ledger_id}' but query 'from' targets '{s}'"
                )));
            }
        }
        JsonValue::Object(m) => {
            // Object form must name this ledger in @id (time/graph selectors ok).
            if let Some(id) = m.get("@id").and_then(|v| v.as_str()) {
                let base = base_ledger_id(id)?;
                let base_path = base_ledger_id(ledger_id)?;
                if base != base_path {
                    return Err(ServerError::bad_request(format!(
                        "Ledger mismatch: endpoint ledger is '{ledger_id}' but query 'from.@id' targets '{id}'"
                    )));
                }
            }
        }
        JsonValue::Array(_) => {
            // Allow arrays only if caller explicitly provides ledger refs per-entry.
            // (Graph-only entries are ambiguous in this endpoint.)
            // Mismatch will be enforced by the connection parsing path if present.
        }
        _ => {}
    }

    Ok(())
}

#[cfg(test)]
mod ledger_scoped_from_tests {
    use super::{normalize_ledger_scoped_from, requires_dataset_features};
    use serde_json::json;

    #[test]
    fn normalize_from_txn_meta_string_rewrites_to_object() {
        let mut q = json!({"select": ["*"], "from": "txn-meta"});
        normalize_ledger_scoped_from("myledger:main", &mut q).unwrap();
        assert_eq!(
            q.get("from").unwrap(),
            &json!({"@id": "myledger:main", "graph": "txn-meta"})
        );
        assert!(requires_dataset_features(&q));
    }

    #[test]
    fn normalize_from_different_ledger_errors() {
        let mut q = json!({"select": ["*"], "from": "other:main"});
        let err = normalize_ledger_scoped_from("myledger:main", &mut q).unwrap_err();
        assert!(err.to_string().contains("Ledger mismatch"));
    }

    #[test]
    fn requires_dataset_features_for_time_travel_and_fragment() {
        let q1 = json!({"select": ["*"], "from": "myledger:main@t:1"});
        assert!(requires_dataset_features(&q1));
        let q2 = json!({"select": ["*"], "from": "myledger:main#txn-meta"});
        assert!(requires_dataset_features(&q2));
    }
}

/// Delimited format requested by the client (TSV or CSV).
#[derive(Debug, Clone, Copy)]
enum DelimitedFormat {
    Tsv,
    Csv,
}

impl DelimitedFormat {
    fn name(self) -> &'static str {
        match self {
            DelimitedFormat::Tsv => "tsv",
            DelimitedFormat::Csv => "csv",
        }
    }
}

/// Check if the client requested a delimited format (TSV or CSV).
fn wants_delimited(headers: &FlureeHeaders) -> Option<DelimitedFormat> {
    if headers.wants_tsv() {
        Some(DelimitedFormat::Tsv)
    } else if headers.wants_csv() {
        Some(DelimitedFormat::Csv)
    } else {
        None
    }
}

/// Build an HTTP response with delimited body and appropriate Content-Type.
fn delimited_response(bytes: Vec<u8>, format: DelimitedFormat) -> Response {
    let content_type = match format {
        DelimitedFormat::Tsv => "text/tab-separated-values; charset=utf-8",
        DelimitedFormat::Csv => "text/csv; charset=utf-8",
    };
    ([(axum::http::header::CONTENT_TYPE, content_type)], bytes).into_response()
}

async fn execute_query(
    state: &AppState,
    ledger_id: &str,
    query_json: &JsonValue,
    delimited: Option<DelimitedFormat>,
) -> Result<Response> {
    // Create execution span
    let span = tracing::debug_span!(
        "query_execute",
        ledger_id = ledger_id,
        query_kind = "jsonld",
        tracker_time = tracing::field::Empty,
        tracker_fuel = tracing::field::Empty,
    );
    async move {
    let span = tracing::Span::current();

    // Check for history query: explicit "to" key indicates history mode
    // History queries must go through the dataset/connection path for correct index selection
    if query_json.get("to").is_some() {
        if let Some(fmt) = delimited {
            return Err(ServerError::not_acceptable(format!(
                "{} format not supported for history queries",
                fmt.name().to_uppercase()
            )));
        }
        return execute_history_query(state, ledger_id, query_json, &span).await;
    }

    // Check for dataset features (fromNamed, from array, from object with graph/alias/time)
    // These require the connection execution path for proper dataset handling
    if requires_dataset_features(query_json) {
        if let Some(fmt) = delimited {
            return Err(ServerError::not_acceptable(format!(
                "{} format not supported for dataset queries",
                fmt.name().to_uppercase()
            )));
        }
        return execute_dataset_query(state, ledger_id, query_json, &span).await;
    }

    // If identity-based policy enforcement is requested (opts.identity or opts.policy-class),
    // route through the connection path which performs policy lookup and enforcement.
    // The simple GraphDb path does not read opts.identity.
    if has_policy_opts(query_json) {
        if let Some(fmt) = delimited {
            return Err(ServerError::not_acceptable(format!(
                "{} format not supported for identity-scoped queries",
                fmt.name().to_uppercase()
            )));
        }
        return execute_dataset_query(state, ledger_id, query_json, &span)
            .await
            .map(IntoResponse::into_response);
    }

    // In proxy mode, use the unified Fluree methods (no local freshness checking)
    if state.config.is_proxy_storage_mode() {
        if let Some(fmt) = delimited {
            return Err(ServerError::not_acceptable(format!(
                "{} format not supported in proxy mode",
                fmt.name().to_uppercase()
            )));
        }
        return execute_query_proxy(state, ledger_id, query_json, &span).await;
    }

    // Shared storage mode: use load_ledger_for_query with freshness checking
    let ledger = load_ledger_for_query(state, ledger_id, &span).await?;
    let graph = GraphDb::from_ledger_state(&ledger);
    let fluree = &state.fluree;

    // Check if tracking is requested
    if has_tracking_opts(query_json) {
        if let Some(fmt) = delimited {
            return Err(ServerError::not_acceptable(format!(
                "{} format not supported for tracked queries",
                fmt.name().to_uppercase()
            )));
        }
        // Execute tracked query via builder
        let response = match graph
            .query(fluree.as_ref())
            .jsonld(query_json)
            .execute_tracked()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                // TrackedErrorResponse has status and error fields
                let server_error =
                    ServerError::Api(fluree_db_api::ApiError::http(e.status, e.error));
                set_span_error_code(&span, "error:InvalidQuery");
                tracing::error!(error = %server_error, "tracked query failed");
                return Err(server_error);
            }
        };

        // Record tracker fields on the execution span
        if let Some(ref time) = response.time {
            span.record("tracker_time", time.as_str());
        }
        if let Some(fuel) = response.fuel {
            span.record("tracker_fuel", fuel);
        }

        // Extract tracking info for headers
        let tally = TrackingTally {
            time: response.time.clone(),
            fuel: response.fuel,
            policy: response.policy.clone(),
        };
        let headers = tracking_headers(&tally);

        tracing::info!(status = "success", tracked = true, time = ?response.time, fuel = response.fuel);
        return Ok((headers, Json(response)).into_response());
    }

    // Delimited fast path: execute raw query and format as TSV/CSV bytes
    if let Some(fmt) = delimited {
        let result = graph
            .query(fluree.as_ref())
            .jsonld(query_json)
            .execute()
            .await
            .map_err(|e| {
                let server_error = ServerError::Api(e);
                set_span_error_code(&span, "error:InvalidQuery");
                tracing::error!(error = %server_error, "query execution failed");
                server_error
            })?;

        let row_count = result.row_count();
        let bytes = match fmt {
            DelimitedFormat::Tsv => result.to_tsv_bytes(&graph.snapshot),
            DelimitedFormat::Csv => result.to_csv_bytes(&graph.snapshot),
        }
        .map_err(|e| {
            ServerError::internal(format!(
                "{} formatting error: {}",
                fmt.name().to_uppercase(),
                e
            ))
        })?;

        tracing::info!(status = "success", format = fmt.name(), row_count);
        return Ok(delimited_response(bytes, fmt));
    }

    // Execute query via builder - formatted JSON-LD output
    let result = match graph
        .query(fluree.as_ref())
        .jsonld(query_json)
        .execute_formatted()
        .await
    {
        Ok(result) => {
            tracing::info!(
                status = "success",
                tracked = false,
                result_count = result.as_array().map(std::vec::Vec::len).unwrap_or(0)
            );
            result
        }
        Err(e) => {
            let server_error = ServerError::Api(e);
            set_span_error_code(&span, "error:InvalidQuery");
            tracing::error!(error = %server_error, "query execution failed");
            return Err(server_error);
        }
    };
    Ok((HeaderMap::new(), Json(result)).into_response())
    }
    .instrument(span)
    .await
}

/// Execute a JSON-LD query in proxy mode (uses Fluree wrapper methods)
async fn execute_query_proxy(
    state: &AppState,
    ledger_id: &str,
    query_json: &JsonValue,
    span: &tracing::Span,
) -> Result<Response> {
    // Check if tracking is requested
    if has_tracking_opts(query_json) {
        // Execute tracked query
        let response = match state
            .fluree
            .graph(ledger_id)
            .query()
            .jsonld(query_json)
            .execute_tracked()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                let server_error =
                    ServerError::Api(fluree_db_api::ApiError::http(e.status, e.error));
                set_span_error_code(span, "error:InvalidQuery");
                tracing::error!(error = %server_error, "tracked query failed (proxy)");
                return Err(server_error);
            }
        };

        // Record tracker fields on the execution span
        if let Some(ref time) = response.time {
            span.record("tracker_time", time.as_str());
        }
        if let Some(fuel) = response.fuel {
            span.record("tracker_fuel", fuel);
        }

        // Extract tracking info for headers
        let tally = TrackingTally {
            time: response.time.clone(),
            fuel: response.fuel,
            policy: response.policy.clone(),
        };
        let headers = tracking_headers(&tally);

        tracing::info!(status = "success", tracked = true, time = ?response.time, fuel = response.fuel);
        return Ok((headers, Json(response)).into_response());
    }

    // Execute query
    let result = match state
        .fluree
        .graph(ledger_id)
        .query()
        .jsonld(query_json)
        .execute_formatted()
        .await
    {
        Ok(result) => {
            tracing::info!(
                status = "success",
                tracked = false,
                result_count = result.as_array().map(std::vec::Vec::len).unwrap_or(0)
            );
            result
        }
        Err(e) => {
            let server_error = ServerError::Api(e);
            set_span_error_code(span, "error:InvalidQuery");
            tracing::error!(error = %server_error, "query execution failed (proxy)");
            return Err(server_error);
        }
    };
    Ok((HeaderMap::new(), Json(result)).into_response())
}

/// Execute a SPARQL query against a specific ledger and return result
async fn execute_sparql_ledger(
    state: &AppState,
    ledger_id: &str,
    sparql: &str,
    identity: Option<&str>,
    delimited: Option<DelimitedFormat>,
    headers: &FlureeHeaders,
    use_default_context: bool,
) -> Result<Response> {
    // Create span for peer mode loading
    let span = tracing::debug_span!(
        "sparql_execute",
        ledger_id = ledger_id,
        tracker_time = tracing::field::Empty,
        tracker_fuel = tracing::field::Empty,
    );
    async move {
        let span = tracing::Span::current();

        // If the SPARQL includes FROM/FROM NAMED, interpret them as dataset clauses
        // selecting named graphs *within this ledger* (multi-named-graph support).
        //
        // - `FROM <ledger>` selects the default graph (ledger is optional in this route).
        // - `FROM <txn-meta>` selects the txn-meta graph within this ledger.
        // - `FROM <graphIRI>` selects the named graph IRI within this ledger.
        // - `FROM NAMED <graphIRI>` makes that named graph available via GRAPH <graphIRI>.
        //
        // If a FROM IRI looks like another ledger ID, reject with a ledger mismatch error.
        let parsed = fluree_db_sparql::parse_sparql(sparql);
        let dataset_clause = parsed.ast.as_ref().and_then(|ast| match &ast.body {
            fluree_db_sparql::ast::QueryBody::Select(q) => q.dataset.as_ref(),
            fluree_db_sparql::ast::QueryBody::Construct(q) => q.dataset.as_ref(),
            fluree_db_sparql::ast::QueryBody::Ask(q) => q.dataset.as_ref(),
            fluree_db_sparql::ast::QueryBody::Describe(q) => q.dataset.as_ref(),
            fluree_db_sparql::ast::QueryBody::Update(_) => None,
        });

        let has_dataset_clause = dataset_clause
            .map(|d| !d.default_graphs.is_empty() || !d.named_graphs.is_empty() || d.to_graph.is_some())
            .unwrap_or(false);

        // Build QueryConnectionOptions from the resolved identity plus header-supplied
        // policy fields. SPARQL has no body `opts` block, so headers are the only
        // transport for `policy-class`, `policy`, `policy-values`, and `default-allow`.
        let policy_values_map = headers.policy_values_map().map_err(|e| {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "invalid fluree-policy-values header");
            e
        })?;
        let qc_opts = fluree_db_api::QueryConnectionOptions {
            identity: identity.map(String::from),
            policy_class: if headers.policy_class.is_empty() {
                None
            } else {
                Some(headers.policy_class.clone())
            },
            policy: headers.policy.clone(),
            policy_values: policy_values_map,
            default_allow: headers.default_allow,
            ..Default::default()
        };

        let wants_sparql_xml = headers.wants_sparql_results_xml();
        let wants_rdf_xml = headers.wants_rdf_xml();
        if wants_sparql_xml && wants_rdf_xml {
            return Err(ServerError::not_acceptable(
                "Conflicting Accept headers: both SPARQL Results XML and RDF/XML requested"
                    .to_string(),
            ));
        }

        // In proxy mode, use the unified Fluree method (returns pre-formatted JSON)
        if state.config.is_proxy_storage_mode() && !has_dataset_clause {
            if wants_sparql_xml {
                return Err(ServerError::not_acceptable(
                    "SPARQL Results XML is not supported in proxy mode".to_string(),
                ));
            }
            if wants_rdf_xml {
                return Err(ServerError::not_acceptable(
                    "RDF/XML is not supported in proxy mode".to_string(),
                ));
            }
            if let Some(fmt) = delimited {
                return Err(ServerError::not_acceptable(format!(
                    "{} format not supported in proxy mode",
                    fmt.name().to_uppercase()
                )));
            }
            let result = if qc_opts.has_any_policy_inputs() {
                let view = state.fluree.db_with_policy(ledger_id, &qc_opts).await
                    .inspect_err(|_| { set_span_error_code(&span, "error:QueryFailed"); })?;
                let view =
                    attach_default_context_to_graph(state, ledger_id, view, use_default_context)
                        .await?;
                view.query(state.fluree.as_ref())
                    .sparql(sparql)
                    .execute_formatted()
                    .await
                    .inspect_err(|_| { set_span_error_code(&span, "error:QueryFailed"); })?
            } else {
                let view = if use_default_context {
                    state.fluree.db_with_default_context(ledger_id).await
                } else {
                    state.fluree.db(ledger_id).await
                }
                .inspect_err(|_| {
                    set_span_error_code(&span, "error:QueryFailed");
                })?;
                view.query(state.fluree.as_ref())
                    .sparql(sparql)
                    .execute_formatted()
                    .await
                    .inspect_err(|_| {
                        set_span_error_code(&span, "error:QueryFailed");
                    })?
            };
            return Ok((HeaderMap::new(), Json(result)).into_response());
        }

        // Policy-scoped queries (identity or explicit policy inputs) without a
        // dataset clause go through the connection path (returns pre-formatted
        // JSON). Dataset-clause queries with policy are handled in the dataset
        // branch below via build_dataset_view_with_policy.
        if qc_opts.has_any_policy_inputs() && !has_dataset_clause {
            if wants_sparql_xml {
                return Err(ServerError::not_acceptable(
                    "SPARQL Results XML is not supported for identity-scoped SPARQL queries"
                        .to_string(),
                ));
            }
            if wants_rdf_xml {
                return Err(ServerError::not_acceptable(
                    "RDF/XML is not supported for identity-scoped SPARQL queries".to_string(),
                ));
            }
            if let Some(fmt) = delimited {
                return Err(ServerError::not_acceptable(format!(
                    "{} format not supported for identity-scoped SPARQL queries",
                    fmt.name().to_uppercase()
                )));
            }
            let view = state.fluree.db_with_policy(ledger_id, &qc_opts).await
                .inspect_err(|_| { set_span_error_code(&span, "error:QueryFailed"); })?;
            let view = attach_default_context_to_graph(state, ledger_id, view, use_default_context)
                .await?;
            let result = view.query(state.fluree.as_ref())
                .sparql(sparql)
                .execute_formatted()
                .await
                .inspect_err(|_| {
                    set_span_error_code(&span, "error:QueryFailed");
                })?;
            return Ok((HeaderMap::new(), Json(result)).into_response());
        }

        // Ledger-scoped SPARQL with dataset clauses (FROM/FROM NAMED): build a dataset
        // of graphs within this ledger and execute as a dataset query.
        if has_dataset_clause {
            if let Some(fmt) = delimited {
                return Err(ServerError::not_acceptable(format!(
                    "{} format not supported for SPARQL dataset clauses on the ledger endpoint",
                    fmt.name().to_uppercase()
                )));
            }

            let Some(dc) = dataset_clause else {
                // Should be unreachable given has_dataset_clause
                return Err(ServerError::bad_request("Invalid SPARQL dataset clause"));
            };

            if dc.to_graph.is_some() {
                return Err(ServerError::bad_request(
                    "SPARQL history range (FROM <...> TO <...>) is not supported on the ledger-scoped endpoint; use /fluree/query instead",
                ));
            }

            // Ensure head is fresh in shared storage mode before time-travel view loading.
            if !state.config.is_proxy_storage_mode() {
                let _ = load_ledger_for_query(state, ledger_id, &span).await?;
            }

            let base_path = base_ledger_id(ledger_id)?;

            let mut spec = DatasetSpec::new();

            let mut add_default = |raw: &str| -> Result<()> {
                // If FROM explicitly names this ledger, treat as default graph.
                if raw == ledger_id {
                    spec.default_graphs.push(GraphSource::new(ledger_id).with_graph(GraphSelector::Default));
                    return Ok(());
                }

                // If it looks like a ledger ref (name:branch or has @ / #), enforce mismatch rules.
                let looks_like_ledger_ref = raw.contains('@')
                    || raw.contains('#')
                    || (raw.contains(':') && !raw.contains("://") && !raw.starts_with("urn:"));

                if looks_like_ledger_ref {
                    let (no_frag, frag) = split_graph_fragment(raw);
                    let (base, time) = fluree_db_core::ledger_id::split_time_travel_suffix(no_frag)
                        .map_err(|e| ServerError::bad_request(format!("Invalid time travel in FROM: {e}")))?;
                    if base != base_path {
                        return Err(ServerError::bad_request(format!(
                            "Ledger mismatch: endpoint ledger is '{ledger_id}' but SPARQL FROM targets '{raw}'"
                        )));
                    }
                    let time_spec = time.map(|t| match t {
                        fluree_db_core::ledger_id::LedgerIdTimeSpec::AtT(t) => TimeSpec::AtT(t),
                        fluree_db_core::ledger_id::LedgerIdTimeSpec::AtIso(iso) => TimeSpec::AtTime(iso),
                        fluree_db_core::ledger_id::LedgerIdTimeSpec::AtCommit(c) => TimeSpec::AtCommit(c),
                    });
                    let selector = frag.map(GraphSelector::from_str).unwrap_or(GraphSelector::Default);
                    let mut src = GraphSource::new(ledger_id).with_graph(selector);
                    if let Some(ts) = time_spec {
                        src = src.with_time(ts);
                    }
                    spec.default_graphs.push(src);
                    return Ok(());
                }

                // Otherwise treat as a graph selector within this ledger.
                let selector = GraphSelector::from_str(raw);
                spec.default_graphs
                    .push(GraphSource::new(ledger_id).with_graph(selector));
                Ok(())
            };

            let mut add_named = |raw: &str| -> Result<()> {
                let looks_like_ledger_ref = raw.contains('@')
                    || raw.contains('#')
                    || (raw.contains(':') && !raw.contains("://") && !raw.starts_with("urn:"));

                if looks_like_ledger_ref {
                    let (no_frag, frag) = split_graph_fragment(raw);
                    let (base, time) = fluree_db_core::ledger_id::split_time_travel_suffix(no_frag)
                        .map_err(|e| ServerError::bad_request(format!("Invalid time travel in FROM NAMED: {e}")))?;
                    if base != base_path {
                        return Err(ServerError::bad_request(format!(
                            "Ledger mismatch: endpoint ledger is '{ledger_id}' but SPARQL FROM NAMED targets '{raw}'"
                        )));
                    }
                    let time_spec = time.map(|t| match t {
                        fluree_db_core::ledger_id::LedgerIdTimeSpec::AtT(t) => TimeSpec::AtT(t),
                        fluree_db_core::ledger_id::LedgerIdTimeSpec::AtIso(iso) => TimeSpec::AtTime(iso),
                        fluree_db_core::ledger_id::LedgerIdTimeSpec::AtCommit(c) => TimeSpec::AtCommit(c),
                    });
                    let selector = frag.map(GraphSelector::from_str).unwrap_or(GraphSelector::Default);
                    let mut src = GraphSource::new(ledger_id)
                        .with_graph(selector)
                        .with_alias(raw);
                    if let Some(ts) = time_spec {
                        src = src.with_time(ts);
                    }
                    spec.named_graphs.push(src);
                    return Ok(());
                }

                // Named graph selector within this ledger; alias must match query's GRAPH IRI.
                let selector = GraphSelector::from_str(raw);
                spec.named_graphs.push(
                    GraphSource::new(ledger_id)
                        .with_graph(selector)
                        .with_alias(raw),
                );
                Ok(())
            };

            // Default graphs: if none specified, use this ledger's default graph.
            if dc.default_graphs.is_empty() {
                spec.default_graphs
                    .push(GraphSource::new(ledger_id).with_graph(GraphSelector::Default));
            } else {
                for iri in &dc.default_graphs {
                    add_default(&iri_to_string(iri))?;
                }
            }

            for iri in &dc.named_graphs {
                add_named(&iri_to_string(iri))?;
            }

            // Tracked dataset query: if tracking headers are present, use tracked path
            if headers.has_tracking() {
                if wants_sparql_xml {
                    return Err(ServerError::not_acceptable(
                        "SPARQL Results XML is not supported for tracked queries".to_string(),
                    ));
                }
                if wants_rdf_xml {
                    return Err(ServerError::not_acceptable(
                        "RDF/XML is not supported for tracked queries".to_string(),
                    ));
                }
                let tracking_opts = headers.to_tracking_options();
                let dataset = if qc_opts.has_any_policy_inputs() {
                    state.fluree.build_dataset_view_with_policy(&spec, &qc_opts).await
                } else {
                    state.fluree.build_dataset_view(&spec).await
                }
                .map_err(ServerError::Api)?;
                let response = dataset
                    .query(state.fluree.as_ref())
                    .sparql(sparql)
                    .tracking(tracking_opts)
                    .execute_tracked()
                    .await;
                let response = match response {
                    Ok(r) => r,
                    Err(e) => {
                        let server_error =
                            ServerError::Api(fluree_db_api::ApiError::http(e.status, e.error));
                        set_span_error_code(&span, "error:QueryFailed");
                        tracing::error!(error = %server_error, "tracked SPARQL dataset query failed");
                        return Err(server_error);
                    }
                };

                let tally = TrackingTally {
                    time: response.time.clone(),
                    fuel: response.fuel,
                    policy: response.policy.clone(),
                };
                let headers = tracking_headers(&tally);

                tracing::info!(status = "success", tracked = true, time = ?response.time, fuel = response.fuel);
                return Ok((headers, Json(response)).into_response());
            }

            // SPARQL Results XML: execute and format as XML string (SELECT/ASK only)
            if wants_sparql_xml {
                let dataset = if qc_opts.has_any_policy_inputs() {
                    state.fluree.build_dataset_view_with_policy(&spec, &qc_opts).await
                } else {
                    state.fluree.build_dataset_view(&spec).await
                }
                .map_err(ServerError::Api)?;
                let xml = dataset
                    .query(state.fluree.as_ref())
                    .sparql(sparql)
                    .format(fluree_db_api::FormatterConfig::sparql_xml())
                    .execute_formatted_string()
                    .await
                    .map_err(ServerError::Api)?;
                let content_type = "application/sparql-results+xml; charset=utf-8";
                return Ok((
                    [(axum::http::header::CONTENT_TYPE, content_type)],
                    xml.into_bytes(),
                )
                    .into_response());
            }

            // RDF/XML: execute and format graph results (CONSTRUCT/DESCRIBE only)
            if wants_rdf_xml {
                let is_graph_query = matches!(
                    parsed.ast.as_ref().map(|a| &a.body),
                    Some(fluree_db_sparql::ast::QueryBody::Construct(_) |
fluree_db_sparql::ast::QueryBody::Describe(_))
                );
                if !is_graph_query {
                    return Err(ServerError::not_acceptable(
                        "RDF/XML is only available for SPARQL CONSTRUCT/DESCRIBE queries"
                            .to_string(),
                    ));
                }

                let dataset = if qc_opts.has_any_policy_inputs() {
                    state.fluree.build_dataset_view_with_policy(&spec, &qc_opts).await
                } else {
                    state.fluree.build_dataset_view(&spec).await
                }
                .map_err(ServerError::Api)?;
                let xml = dataset
                    .query(state.fluree.as_ref())
                    .sparql(sparql)
                    .format(fluree_db_api::FormatterConfig::rdf_xml())
                    .execute_formatted_string()
                    .await
                    .map_err(ServerError::Api)?;
                let content_type = "application/rdf+xml; charset=utf-8";
                return Ok((
                    [(axum::http::header::CONTENT_TYPE, content_type)],
                    xml.into_bytes(),
                )
                    .into_response());
            }

            // AgentJson: dataset query with agent-optimized envelope
            if headers.wants_agent_json() {
                let from_count = dc.default_graphs.len();
                let agent_ctx = fluree_db_api::AgentJsonContext {
                    sparql_text: Some(sparql.to_string()),
                    from_count,
                    iso_timestamp: Some(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
                    ..Default::default()
                };
                let mut config = fluree_db_api::FormatterConfig::agent_json()
                    .with_agent_json_context(agent_ctx);
                if let Some(max_bytes) = headers.max_bytes() {
                    config = config.with_max_bytes(max_bytes);
                }
                let dataset = if qc_opts.has_any_policy_inputs() {
                    state.fluree.build_dataset_view_with_policy(&spec, &qc_opts).await
                } else {
                    state.fluree.build_dataset_view(&spec).await
                }
                .map_err(ServerError::Api)?;
                let result = dataset
                    .query(state.fluree.as_ref())
                    .sparql(sparql)
                    .format(config)
                    .execute_formatted()
                    .await
                    .map_err(ServerError::Api)?;
                let content_type = "application/vnd.fluree.agent+json; charset=utf-8";
                return Ok((
                    [(axum::http::header::CONTENT_TYPE, content_type)],
                    Json(result),
                )
                    .into_response());
            }

            let dataset = if qc_opts.has_any_policy_inputs() {
                state.fluree.build_dataset_view_with_policy(&spec, &qc_opts).await
            } else {
                state.fluree.build_dataset_view(&spec).await
            }
            .map_err(ServerError::Api)?;
            let result = dataset
                .query(state.fluree.as_ref())
                .sparql(sparql)
                .execute_formatted()
                .await
                .map_err(ServerError::Api)?;

            return Ok((HeaderMap::new(), Json(result)).into_response());
        }

        // Shared storage mode: use load_ledger_for_query with freshness checking
        let ledger = load_ledger_for_query(state, ledger_id, &span)
            .await
            .inspect_err(|_| {
                set_span_error_code(&span, "error:LedgerLoad");
            })?;
        let graph = attach_default_context_to_graph(
            state,
            ledger_id,
            GraphDb::from_ledger_state(&ledger),
            use_default_context,
        )
        .await?;
        let fluree = &state.fluree;

        // Tracked SPARQL: if tracking headers are present, use tracked execution path
        if headers.has_tracking() {
            if wants_sparql_xml {
                return Err(ServerError::not_acceptable(
                    "SPARQL Results XML is not supported for tracked queries".to_string(),
                ));
            }
            if wants_rdf_xml {
                return Err(ServerError::not_acceptable(
                    "RDF/XML is not supported for tracked queries".to_string(),
                ));
            }
            if let Some(fmt) = delimited {
                return Err(ServerError::not_acceptable(format!(
                    "{} format not supported for tracked queries",
                    fmt.name().to_uppercase()
                )));
            }

            let tracking_opts = headers.to_tracking_options();
            let response = match graph
                .query(fluree.as_ref())
                .sparql(sparql)
                .tracking(tracking_opts)
                .execute_tracked()
                .await
            {
                Ok(response) => response,
                Err(e) => {
                    let server_error =
                        ServerError::Api(fluree_db_api::ApiError::http(e.status, e.error));
                    set_span_error_code(&span, "error:InvalidQuery");
                    tracing::error!(error = %server_error, "tracked SPARQL query failed");
                    return Err(server_error);
                }
            };

            // Record tracker fields on the execution span
            if let Some(ref time) = response.time {
                span.record("tracker_time", time.as_str());
            }
            if let Some(fuel) = response.fuel {
                span.record("tracker_fuel", fuel);
            }

            let tally = TrackingTally {
                time: response.time.clone(),
                fuel: response.fuel,
                policy: response.policy.clone(),
            };
            let resp_headers = tracking_headers(&tally);

            tracing::info!(status = "success", tracked = true, time = ?response.time, fuel = response.fuel);
            return Ok((resp_headers, Json(response)).into_response());
        }

        // Delimited fast path: execute raw query and format as TSV/CSV bytes
        if let Some(fmt) = delimited {
            let result = graph
                .query(fluree.as_ref())
                .sparql(sparql)
                .execute()
                .await
                .map_err(|e| {
                    set_span_error_code(&span, "error:InvalidQuery");
                    tracing::error!(error = %e, "SPARQL query execution failed");
                    ServerError::Api(e)
                })?;

            let row_count = result.row_count();
            let bytes = match fmt {
                DelimitedFormat::Tsv => result.to_tsv_bytes(&graph.snapshot),
                DelimitedFormat::Csv => result.to_csv_bytes(&graph.snapshot),
            }
            .map_err(|e| {
                ServerError::internal(format!(
                    "{} formatting error: {}",
                    fmt.name().to_uppercase(),
                    e
                ))
            })?;

            tracing::info!(status = "success", format = fmt.name(), row_count);
            return Ok(delimited_response(bytes, fmt));
        }

        // SPARQL Results XML: execute and format as XML string (SELECT/ASK only)
        if wants_sparql_xml {
            let xml = graph
                .query(fluree.as_ref())
                .sparql(sparql)
                .format(fluree_db_api::FormatterConfig::sparql_xml())
                .execute_formatted_string()
                .await
                .inspect_err(|_| {
                    set_span_error_code(&span, "error:QueryFailed");
                })
                .map_err(ServerError::Api)?;
            let content_type = "application/sparql-results+xml; charset=utf-8";
            return Ok((
                [(axum::http::header::CONTENT_TYPE, content_type)],
                xml.into_bytes(),
            )
                .into_response());
        }

        // RDF/XML: execute and format graph results (CONSTRUCT/DESCRIBE only)
        if wants_rdf_xml {
            let is_graph_query = matches!(
                parsed.ast.as_ref().map(|a| &a.body),
                Some(fluree_db_sparql::ast::QueryBody::Construct(_) |
fluree_db_sparql::ast::QueryBody::Describe(_))
            );
            if !is_graph_query {
                return Err(ServerError::not_acceptable(
                    "RDF/XML is only available for SPARQL CONSTRUCT/DESCRIBE queries".to_string(),
                ));
            }

            let xml = graph
                .query(fluree.as_ref())
                .sparql(sparql)
                .format(fluree_db_api::FormatterConfig::rdf_xml())
                .execute_formatted_string()
                .await
                .inspect_err(|_| {
                    set_span_error_code(&span, "error:QueryFailed");
                })
                .map_err(ServerError::Api)?;
            let content_type = "application/rdf+xml; charset=utf-8";
            return Ok((
                [(axum::http::header::CONTENT_TYPE, content_type)],
                xml.into_bytes(),
            )
                .into_response());
        }

        // AgentJson: execute with agent-optimized envelope format
        if headers.wants_agent_json() {
            let from_count = dataset_clause
                .map(|d| d.default_graphs.len())
                .unwrap_or(0);
            let agent_ctx = fluree_db_api::AgentJsonContext {
                sparql_text: Some(sparql.to_string()),
                from_count,
                iso_timestamp: Some(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
                ..Default::default()
            };
            let mut config = fluree_db_api::FormatterConfig::agent_json()
                .with_agent_json_context(agent_ctx);
            if let Some(max_bytes) = headers.max_bytes() {
                config = config.with_max_bytes(max_bytes);
            }
            let result = graph
                .query(fluree.as_ref())
                .sparql(sparql)
                .format(config)
                .execute_formatted()
                .await
                .inspect_err(|_| {
                    set_span_error_code(&span, "error:QueryFailed");
                })?;
            let content_type = "application/vnd.fluree.agent+json; charset=utf-8";
            return Ok((
                [(axum::http::header::CONTENT_TYPE, content_type)],
                Json(result),
            )
                .into_response());
        }

        // Execute SPARQL query via builder - formatted JSON output
        let result = graph
            .query(fluree.as_ref())
            .sparql(sparql)
            .execute_formatted()
            .await
            .inspect_err(|_| {
                set_span_error_code(&span, "error:QueryFailed");
            })?;
        Ok((HeaderMap::new(), Json(result)).into_response())
    }
    .instrument(span)
    .await
}

/// Explain a query
///
/// POST /fluree/explain
/// GET /fluree/explain
///
/// Returns the query execution plan without executing.
/// Supports signed requests (JWS/VC format).
pub async fn explain(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SparqlParams>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    credential: MaybeCredential,
) -> Result<Json<JsonValue>> {
    // Create request span with correlation context
    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);
    let trace_id = extract_trace_id(&credential.headers);

    let input_format = if is_sparql_request(&headers, &credential, &params) {
        "sparql"
    } else {
        "json-ld"
    };

    let span = create_request_span(
        "explain",
        request_id.as_deref(),
        trace_id.as_deref(),
        None, // ledger ID determined later
        None, // tenant_id not yet supported
        Some(input_format),
    );
    async move {
        let span = tracing::Span::current();

        tracing::info!(status = "start", "explain request received");

        // Enforce data auth if configured (Bearer token OR signed request)
        let data_auth = state.config.data_auth();
        if data_auth.mode == crate::config::DataAuthMode::Required
            && !credential.is_signed()
            && bearer.0.is_none()
        {
            set_span_error_code(&span, "error:Unauthorized");
            return Err(ServerError::unauthorized(
                "Authentication required (signed request or Bearer token)",
            ));
        }

        // SPARQL UPDATE should use the update endpoint, not explain
        if headers.is_sparql_update() || credential.is_sparql_update {
            let error = ServerError::bad_request(
                "SPARQL UPDATE requests should use the /v1/fluree/update endpoint, not /v1/fluree/explain",
            );
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %error, "SPARQL UPDATE sent to explain endpoint");
            return Err(error);
        }

        // Handle SPARQL explain (connection-scoped: ledger must be discoverable via header or FROM)
        if is_sparql_request(&headers, &credential, &params) {
            let sparql = resolve_sparql_text(&params, &credential)?;
            log_query_text(&sparql, &state.telemetry_config, &span);

            // Determine target ledger: header wins, otherwise require a single FROM ledger id.
            let ledger_id = if let Some(ref l) = headers.ledger {
                l.clone()
            } else {
                let ledger_ids = fluree_db_api::sparql_dataset_ledger_ids(&sparql).map_err(|e| {
                    ServerError::bad_request(format!(
                        "Unable to determine ledger for SPARQL explain; include a FROM clause (e.g., FROM <myledger:main>) or send '{}' header. Details: {}",
                        FlureeHeaders::LEDGER,
                        e
                    ))
                })?;
                if ledger_ids.len() != 1 {
                    return Err(ServerError::bad_request(
                        "SPARQL explain requires exactly one target ledger (single FROM <ledger>); multi-ledger explain is not supported"
                            .to_string(),
                    ));
                }
                ledger_ids[0].clone()
            };
            span.record("ledger_id", ledger_id.as_str());

            // Enforce bearer ledger scope for unsigned requests
            if let Some(p) = bearer.0.as_ref() {
                if !credential.is_signed() && !p.can_read(&ledger_id) {
                    set_span_error_code(&span, "error:Forbidden");
                    // Avoid existence leak
                    return Err(ServerError::not_found("Ledger not found"));
                }
            }

            let loaded = if state.config.is_proxy_storage_mode() {
                state.fluree.ledger(&ledger_id).await.map_err(ServerError::Api)?
            } else {
                load_ledger_for_query(&state, &ledger_id, &span).await?
            };
            let db = fluree_db_api::GraphDb::from_ledger_state(&loaded);
            let result = {
                match state.fluree.explain_sparql(&db, &sparql).await {
                    Ok(result) => {
                        tracing::info!(status = "success", "explain completed");
                        result
                    }
                    Err(e) => {
                        let server_error = ServerError::Api(e);
                        set_span_error_code(&span, "error:InvalidQuery");
                        tracing::error!(error = %server_error, "explain execution failed");
                        return Err(server_error);
                    }
                }
            };
            return Ok(Json(result));
        }

        // Parse body as JSON
        let mut query_json = match credential.body_json() {
            Ok(json) => json,
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "invalid JSON in request body");
                return Err(e);
            }
        };

        // Log query text according to configuration (only serialize if needed)
        if should_log_query_text(&state.telemetry_config) {
            if let Ok(query_text) = serde_json::to_string(&query_json) {
                log_query_text(&query_text, &state.telemetry_config, &span);
            }
        }

        // Get ledger id
        let ledger_id = match get_ledger_id(None, &headers, &query_json) {
            Ok(ledger_id) => {
                span.record("ledger_id", ledger_id.as_str());
                ledger_id
            }
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "missing ledger ID");
                return Err(e);
            }
        };

        // Inject header values into query opts
        inject_headers_into_query(&mut query_json, &headers);

        // Enforce bearer ledger scope for unsigned requests
        if let Some(p) = bearer.0.as_ref() {
            if !credential.is_signed() && !p.can_read(&ledger_id) {
                set_span_error_code(&span, "error:Forbidden");
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

        // Apply bearer identity + server-default policy-class to opts, honoring
        // the root-identity impersonation semantic (see routes::policy_auth).
        let identity = effective_identity(&credential, &bearer);
        let policy_class = data_auth.default_policy_class.as_deref();
        crate::routes::policy_auth::apply_auth_identity_to_opts(
            &state,
            &ledger_id,
            &mut query_json,
            identity.as_deref(),
            policy_class,
        )
        .await;

        // Execute explain
        let loaded = if state.config.is_proxy_storage_mode() {
            state.fluree.ledger(&ledger_id).await.map_err(ServerError::Api)?
        } else {
            load_ledger_for_query(&state, &ledger_id, &span).await?
        };
        let db = fluree_db_api::GraphDb::from_ledger_state(&loaded);
        let result = match state.fluree.explain(&db, &query_json).await {
            Ok(result) => {
                tracing::info!(status = "success", "explain completed");
                result
            }
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(&span, "error:InvalidQuery");
                tracing::error!(error = %server_error, "explain execution failed");
                return Err(server_error);
            }
        };

        Ok(Json(result))
    }
    .instrument(span)
    .await
}

// ===== Peer mode support =====

/// Load a ledger for query, handling peer mode freshness checking.
///
/// **Note**: This function is only used for non-proxy storage modes (file-backed Fluree).
/// In proxy mode, routes use `Fluree` wrapper methods instead.
///
/// Load a ledger for query execution
///
/// In transaction mode, simply loads the ledger via ledger_cached().
/// In peer mode with shared storage, checks if the local ledger is stale vs SSE watermarks
/// and reloads if needed using LedgerManager::reload() for coalesced reloading.
pub(crate) async fn load_ledger_for_query(
    state: &AppState,
    ledger_id: &str,
    span: &tracing::Span,
) -> Result<LedgerState> {
    let fluree = &state.fluree;

    // Get cached handle (loads if not cached)
    let handle = fluree.ledger_cached(ledger_id).await.map_err(|e| {
        set_span_error_code(span, "error:NotFound");
        tracing::error!(error = %e, "ledger not found");
        ServerError::Api(e)
    })?;

    // In transaction mode, just return the cached state
    if state.config.server_role != ServerRole::Peer {
        return Ok(handle.snapshot().await.to_ledger_state());
    }

    // In peer mode (shared storage), check freshness and potentially reload
    let peer_state = state
        .peer_state
        .as_ref()
        .expect("peer_state should exist in peer mode");

    // Check freshness using FreshnessSource trait
    // If no watermark available (SSE hasn't seen ledger), treat as current (lenient policy)
    if let Some(watermark) = peer_state.watermark(ledger_id) {
        match handle.check_freshness(&watermark).await {
            FreshnessCheck::Stale => {
                // Remote is ahead - reload ledger from shared storage
                // Uses LedgerManager::reload() which handles coalescing
                tracing::info!(
                    ledger_id = ledger_id,
                    remote_index_t = watermark.index_t,
                    "Refreshing ledger for peer query"
                );

                if let Some(mgr) = fluree.ledger_manager() {
                    mgr.reload(ledger_id).await.map_err(ServerError::Api)?;
                    state.refresh_counter.fetch_add(1, Ordering::Relaxed);
                }
            }
            FreshnessCheck::Current => {
                // Local is fresh, use cached
            }
        }
    } else {
        // No watermark = lenient policy: proceed with cached state
        tracing::debug!(
            ledger_id = ledger_id,
            "Ledger not yet seen in SSE, using cached state"
        );
    }

    Ok(handle.snapshot().await.to_ledger_state())
}

// Note: reload_ledger_coalesced has been removed in favor of LedgerManager::reload()
// which provides built-in coalescing of concurrent reload requests.

/// Execute a history query (query with explicit `to` key)
///
/// History queries must go through the connection/dataset path to properly handle:
/// - Dataset parsing with `from` and `to` keys for history time ranges
/// - Correct index selection for history mode (includes retracted data)
/// - `@op` binding population (true = assert, false = retract)
///
/// If the query doesn't have a `from` key, the ledger ID from the URL path is injected.
async fn execute_history_query(
    state: &AppState,
    ledger_id: &str,
    query_json: &JsonValue,
    span: &tracing::Span,
) -> Result<Response> {
    // Clone the query so we can potentially inject the `from` key
    let mut query = query_json.clone();

    // If query doesn't have a `from` key, inject the ledger ID from the URL path
    // This allows users to POST to /:ledger/query with just `{ "to": "...", ... }`
    if query.get("from").is_none() {
        if let Some(obj) = query.as_object_mut() {
            obj.insert("from".to_string(), JsonValue::String(ledger_id.to_string()));
        }
    }

    // Execute through the connection path which handles dataset/history parsing
    if has_tracking_opts(&query) {
        let response = match state
            .fluree
            .query_from()
            .jsonld(&query)
            .execute_tracked()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                let server_error =
                    ServerError::Api(fluree_db_api::ApiError::http(e.status, e.error));
                set_span_error_code(span, "error:InvalidQuery");
                tracing::error!(
                    error = %server_error,
                    query_kind = "history",
                    "tracked history query failed"
                );
                return Err(server_error);
            }
        };

        // Record tracker fields on the execution span
        if let Some(ref time) = response.time {
            span.record("tracker_time", time.as_str());
        }
        if let Some(fuel) = response.fuel {
            span.record("tracker_fuel", fuel);
        }

        let tally = TrackingTally {
            time: response.time.clone(),
            fuel: response.fuel,
            policy: response.policy.clone(),
        };
        let headers = tracking_headers(&tally);

        tracing::info!(
            status = "success",
            tracked = true,
            query_kind = "history",
            time = ?response.time,
            fuel = response.fuel
        );
        Ok((headers, Json(response)).into_response())
    } else {
        match state
            .fluree
            .query_from()
            .jsonld(&query)
            .execute_formatted()
            .await
        {
            Ok(result) => {
                tracing::info!(
                    status = "success",
                    query_kind = "history",
                    result_count = result.as_array().map(std::vec::Vec::len).unwrap_or(0)
                );
                Ok((HeaderMap::new(), Json(result)).into_response())
            }
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(span, "error:InvalidQuery");
                tracing::error!(
                    error = %server_error,
                    query_kind = "history",
                    "history query failed"
                );
                Err(server_error)
            }
        }
    }
}

/// Execute a dataset query (query with fromNamed, from array, or structured from object)
///
/// Dataset queries must go through the connection/dataset path to properly handle:
/// - Multiple default graphs (from array)
/// - Named graphs (fromNamed)
/// - Graph selectors (from object with graph field)
/// - Dataset-local aliases for GRAPH patterns
///
/// If the query doesn't have a `from` key, the ledger ID from the URL path is injected.
async fn execute_dataset_query(
    state: &AppState,
    ledger_id: &str,
    query_json: &JsonValue,
    span: &tracing::Span,
) -> Result<Response> {
    // Clone the query so we can potentially inject the `from` key
    let mut query = query_json.clone();

    // If query doesn't have a `from` key, inject the ledger ID from the URL path
    // This allows users to POST to /:ledger/query with just `{ "fromNamed": {...}, ... }`
    if query.get("from").is_none() {
        if let Some(obj) = query.as_object_mut() {
            obj.insert("from".to_string(), JsonValue::String(ledger_id.to_string()));
        }
    }

    // Execute through the connection path which handles dataset parsing
    if has_tracking_opts(&query) {
        let response = match state
            .fluree
            .query_from()
            .jsonld(&query)
            .execute_tracked()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                let server_error =
                    ServerError::Api(fluree_db_api::ApiError::http(e.status, e.error));
                set_span_error_code(span, "error:InvalidQuery");
                tracing::error!(
                    error = %server_error,
                    query_kind = "dataset",
                    "tracked dataset query failed"
                );
                return Err(server_error);
            }
        };

        // Record tracker fields on the execution span
        if let Some(ref time) = response.time {
            span.record("tracker_time", time.as_str());
        }
        if let Some(fuel) = response.fuel {
            span.record("tracker_fuel", fuel);
        }

        let tally = TrackingTally {
            time: response.time.clone(),
            fuel: response.fuel,
            policy: response.policy.clone(),
        };
        let headers = tracking_headers(&tally);

        tracing::info!(
            status = "success",
            tracked = true,
            query_kind = "dataset",
            time = ?response.time,
            fuel = response.fuel
        );
        Ok((headers, Json(response)).into_response())
    } else {
        match state
            .fluree
            .query_from()
            .jsonld(&query)
            .execute_formatted()
            .await
        {
            Ok(result) => {
                tracing::info!(
                    status = "success",
                    query_kind = "dataset",
                    result_count = result.as_array().map(std::vec::Vec::len).unwrap_or(0)
                );
                Ok((HeaderMap::new(), Json(result)).into_response())
            }
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(span, "error:InvalidQuery");
                tracing::error!(
                    error = %server_error,
                    query_kind = "dataset",
                    "dataset query failed"
                );
                Err(server_error)
            }
        }
    }
}
