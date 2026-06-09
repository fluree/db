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
    DatasetSpec, FreshnessCheck, FreshnessSource, GraphDb, GraphSource, LedgerState,
    QueryExecutionOptions, TimeSpec, TrackingTally,
};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::Instrument;

fn query_execution_options(state: &AppState) -> QueryExecutionOptions {
    crate::query_control::query_execution_options(state.config.query_timeout_ms)
}

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

/// Extract a representative ledger identifier from a `from` / `fromNamed`
/// value of any supported shape.
///
/// Shapes handled (see `requires_dataset_features` and `parse_dataset_spec`):
/// - string: `"ledger:main"` (optionally with an `@t:` / `#graph` suffix)
/// - array: `["a:main", "b:main"]` or `[{"@id": "a"}, ...]` — first element
/// - object `from`: `{"@id": "ledger:main@t:5", ...}` — the `@id`
/// - object `fromNamed`: `{"alias": <source>, ...}` — first map value
///
/// Returns the first concrete ledger string found, or `None` if the value
/// carries no resolvable identifier. This is used only to pick a ledger for
/// auth scoping and span recording; the full multi-graph dataset is resolved
/// later by `parse_dataset_spec` in the dataset execution path.
fn first_ledger_identifier(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(s) => Some(s.clone()),
        JsonValue::Array(items) => items.iter().find_map(first_ledger_identifier),
        JsonValue::Object(map) => map
            .get("@id")
            .and_then(JsonValue::as_str)
            .map(str::to_string)
            // `fromNamed` map form: { alias -> source }. No `@id`, so fall
            // back to the first source value that yields an identifier.
            .or_else(|| map.values().find_map(first_ledger_identifier)),
        _ => None,
    }
}

/// Collect **every** concrete ledger identifier a `from` / `fromNamed` value
/// references, across all supported shapes.
///
/// Unlike [`first_ledger_identifier`] (which returns a single representative
/// id for span recording), this enumerates all of them so the bearer
/// ledger-scope check can authorize every ledger a multi-default-graph or
/// named-graph query will actually read — not just the first. Mirrors the
/// shape handling of `first_ledger_identifier`: an object with `@id` is a
/// single source; otherwise its values are sources (`fromNamed` map form).
fn collect_ledger_identifiers(value: &JsonValue, out: &mut Vec<String>) {
    match value {
        JsonValue::String(s) => out.push(s.clone()),
        JsonValue::Array(items) => {
            for item in items {
                collect_ledger_identifiers(item, out);
            }
        }
        JsonValue::Object(map) => {
            if let Some(id) = map.get("@id").and_then(JsonValue::as_str) {
                out.push(id.to_string());
            } else {
                for v in map.values() {
                    collect_ledger_identifiers(v, out);
                }
            }
        }
        _ => {}
    }
}

/// Enforce bearer ledger-scope over **all** ledgers a query's `from` /
/// `fromNamed` references, not just the representative one.
///
/// Unsigned bearer tokens may be scoped to a subset of ledgers. A
/// multi-default-graph (`from: ["a","b"]`) or named-graph (`fromNamed`) query
/// must be rejected if it touches any ledger outside that scope — otherwise a
/// token scoped to `a` could read `b` by piggy-backing it onto the dataset.
/// Rejected with 404 (not 403) to avoid leaking ledger existence, matching the
/// single-query and multi-query-envelope responses.
///
/// No-op for signed requests and unauthenticated (no-bearer) requests; those
/// are handled by the surrounding data-auth gate and per-ledger policy.
fn enforce_bearer_dataset_scope(
    query_json: &JsonValue,
    bearer: &MaybeDataBearer,
    is_signed: bool,
    span: &tracing::Span,
) -> Result<()> {
    let Some(principal) = bearer.0.as_ref() else {
        return Ok(());
    };
    if is_signed {
        return Ok(());
    }

    let mut ids: Vec<String> = Vec::new();
    if let Some(from) = query_json.get("from") {
        collect_ledger_identifiers(from, &mut ids);
    }
    if let Some(named) = query_json
        .get("fromNamed")
        .or_else(|| query_json.get("from-named"))
    {
        collect_ledger_identifiers(named, &mut ids);
    }

    for raw in ids {
        // Strip any `@t:` / `#graph` suffix so a scoped read token still
        // authorizes time-travel / graph-fragment reads of an in-scope ledger.
        let base = base_ledger_id(&raw)?;
        if !principal.can_read(&base) {
            set_span_error_code(span, "error:Forbidden");
            return Err(ServerError::not_found("Ledger not found"));
        }
    }
    Ok(())
}

/// Helper to extract ledger ID from request (for JSON-LD queries)
fn get_ledger_id(
    path_ledger: Option<&str>,
    headers: &FlureeHeaders,
    body: &JsonValue,
) -> Result<String> {
    // Priority: path > header > body.from > body.fromNamed
    if let Some(ledger) = path_ledger {
        return Ok(ledger.to_string());
    }

    if let Some(ledger) = &headers.ledger {
        return Ok(ledger.clone());
    }

    // Accept every `from` shape the engine supports — string, array of
    // sources (multi-default-graph union), or structured object (time travel
    // / graph fragment). Earlier this only matched a bare string, so array /
    // object `from` (and `fromNamed`-only) queries were rejected with
    // `MissingLedger` before the dataset path could run (issue #1259).
    //
    // The extracted id is used only for the conservative bearer scope check
    // and span recording; per-ledger policy and routing are applied later in
    // `execute_dataset_query` via `parse_dataset_spec`, which sees the full
    // dataset spec. Strip any `@t:` / `#graph` suffix so auth scopes to the
    // base ledger.
    let from_id = body.get("from").and_then(first_ledger_identifier);
    let named_id = || {
        body.get("fromNamed")
            .or_else(|| body.get("from-named"))
            .and_then(first_ledger_identifier)
    };
    if let Some(raw) = from_id.or_else(named_id) {
        return base_ledger_id(&raw);
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
        // Connection-scoped SPARQL returns pre-formatted JSON only. The byte
        // formats (delimited, RDF/XML, SPARQL-results XML) are not negotiated on
        // this route — reject with 406 rather than silently downgrading to JSON,
        // and point callers at the ledger-scoped route which does serve them.
        if let Some(fmt) = delimited {
            return Err(ServerError::not_acceptable(format!(
                "{} format not supported for connection-scoped SPARQL queries. \
                     Use the /:ledger/query endpoint instead.",
                fmt.name().to_uppercase()
            )));
        }
        if headers.wants_rdf_xml() {
            return Err(ServerError::not_acceptable(
                "RDF/XML is not supported for connection-scoped SPARQL queries. \
                 Use the /:ledger/query endpoint instead."
                    .to_string(),
            ));
        }
        if headers.wants_sparql_results_xml() {
            return Err(ServerError::not_acceptable(
                "SPARQL Results XML is not supported for connection-scoped SPARQL queries. \
                 Use the /:ledger/query endpoint instead."
                    .to_string(),
            ));
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
            // AgentJson is a solution-table envelope; a CONSTRUCT/DESCRIBE graph
            // has no such form, so reject rather than mislabel a JSON-LD graph
            // as AgentJson (issue #1274).
            if is_graph_query(parsed.ast.as_ref()) {
                return Err(ServerError::not_acceptable(
                    "AgentJson is not available for SPARQL CONSTRUCT/DESCRIBE queries; \
                     use the default (JSON-LD) or Accept: application/rdf+xml"
                        .to_string(),
                ));
            }
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
            let content_type = "application/vnd.fluree.agent+json; charset=utf-8";

            // Tracked agent-json: keep the agent envelope as the response body
            // (so agents see the same shape), but surface fuel/time/policy via
            // x-fdb-* response headers.
            if headers.has_tracking() {
                let tracking_opts = headers.to_tracking_options();
                let response = state
                    .fluree
                    .query_from()
                    .sparql(&sparql)
                    .format(config)
                    .tracking(tracking_opts)
                    .execution_options(query_execution_options(&state))
                    .execute_tracked()
                    .await;
                return match response {
                    Ok(r) => {
                        let tally = TrackingTally {
                            time: r.time.clone(),
                            fuel: r.fuel,
                            policy: r.policy.clone(),
                        };
                        let mut resp_headers = tracking_headers(&tally);
                        resp_headers.insert(
                            axum::http::header::CONTENT_TYPE,
                            content_type.parse().expect("content-type parses"),
                        );
                        tracing::info!(
                            status = "success",
                            query_kind = "sparql",
                            format = "agent-json",
                            tracked = true,
                            time = ?r.time,
                            fuel = r.fuel,
                        );
                        Ok((resp_headers, Json(r.result)).into_response())
                    }
                    Err(e) => {
                        let server_error =
                            ServerError::Api(fluree_db_api::ApiError::http(e.status, e.error));
                        set_span_error_code(&span, "error:QueryFailed");
                        tracing::error!(error = %server_error, query_kind = "sparql", "tracked agent-json SPARQL connection query failed");
                        Err(server_error)
                    }
                };
            }

            let result = state
                .fluree
                .query_from()
                .sparql(&sparql)
                .format(config)
                .execution_options(query_execution_options(&state))
                .execute_formatted()
                .await;
            return match result {
                Ok(json) => {
                    tracing::info!(status = "success", query_kind = "sparql", format = "agent-json");
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

        // Tracked path: if fluree-track-* / fluree-max-fuel headers are present,
        // run through the tracking pipeline and emit fuel/time headers + tally body.
        if headers.has_tracking() {
            let tracking_opts = headers.to_tracking_options();
            let response = state
                .fluree
                .query_connection_sparql_tracked_with_options(
                    &sparql,
                    None,
                    Some(tracking_opts),
                    query_execution_options(&state),
                )
                .await;
            let response = match response {
                Ok(r) => r,
                Err(e) => {
                    let server_error =
                        ServerError::Api(fluree_db_api::ApiError::http(e.status, e.error));
                    set_span_error_code(&span, "error:QueryFailed");
                    tracing::error!(error = %server_error, query_kind = "sparql", "tracked SPARQL connection query failed");
                    return Err(server_error);
                }
            };
            let tally = TrackingTally {
                time: response.time.clone(),
                fuel: response.fuel,
                policy: response.policy.clone(),
            };
            let resp_headers = tracking_headers(&tally);
            tracing::info!(
                status = "success",
                query_kind = "sparql",
                tracked = true,
                time = ?response.time,
                fuel = response.fuel
            );
            return Ok((resp_headers, Json(response)).into_response());
        }

        let parsed = fluree_db_sparql::parse_sparql(&sparql);
        let (fmt_config, content_type) =
            sparql_json_response_format(parsed.ast.as_ref(), &headers);
        match state
            .fluree
            .query_from()
            .sparql(&sparql)
            .format(fmt_config)
            .execution_options(query_execution_options(&state))
            .execute_formatted()
            .await
        {
            Ok(result) => {
                tracing::info!(
                    status = "success",
                    query_kind = "sparql",
                    result_count = result.as_array().map(std::vec::Vec::len).unwrap_or(0)
                );
                Ok(([(axum::http::header::CONTENT_TYPE, content_type)], Json(result)).into_response())
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
        // ...and over every additional ledger a multi-default-graph / named-graph
        // `from`/`fromNamed` references (the single check above only covers the
        // representative id). Parity with the multi-query envelope path.
        enforce_bearer_dataset_scope(&query_json, &bearer, credential.is_signed(), &span)?;

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
    // ...and over every additional ledger a `fromNamed` (or normalized `from`)
    // references — a scoped token must not reach an out-of-scope ledger via a
    // named graph even on the ledger-scoped endpoint.
    enforce_bearer_dataset_scope(&query_json, &bearer, credential.is_signed(), &span)?;

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

            // If the SPARQL carries FROM clauses, allow them only when they
            // target the path ledger (same base) — with an optional
            // time-travel suffix. That gives `--at` callers a working plan
            // at the requested `t` while still rejecting attempts to point
            // explain at a different ledger via FROM. Multi-FROM and FROM
            // NAMED are still routed through the connection-explain path,
            // which itself enforces single-ledger.
            let from_ids = fluree_db_api::sparql_dataset_ledger_ids(&sparql)
                .unwrap_or_default();
            let has_dataset_clauses = !from_ids.is_empty();
            if has_dataset_clauses {
                let base_path = base_ledger_id(&ledger)?;
                for from in &from_ids {
                    let base = base_ledger_id(from)?;
                    if base != base_path {
                        set_span_error_code(&span, "error:BadRequest");
                        return Err(ServerError::bad_request(format!(
                            "Ledger mismatch: endpoint ledger is '{ledger}' but SPARQL FROM targets '{from}'"
                        )));
                    }
                }

                // Route through the connection-explain path so the time-travel
                // suffix on FROM <ledger@t:N> drives snapshot selection.
                let result = state
                    .fluree
                    .explain_connection_sparql(&sparql)
                    .await
                    .map_err(ServerError::Api)?;
                tracing::info!(
                    status = "success",
                    query_kind = "sparql",
                    "explain completed (dataset path)"
                );
                return Ok(Json(result));
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

        // When the body carries a time-travel `from` (or any other dataset
        // feature `normalize_ledger_scoped_from` accepted), delegate to the
        // connection-explain path so the snapshot is resolved at the
        // requested `t`. The default fast path below loads the ledger at
        // HEAD, which would silently drop time-travel — exactly the
        // explain bug the CLI was working around with a hard refusal.
        if requires_dataset_features(&query_json) {
            // Ensure the dataset has a default-graph entry pointing at this
            // ledger if the body omitted `from`. Mirrors execute_query's
            // dataset routing.
            if query_json.get("from").is_none() {
                if let Some(obj) = query_json.as_object_mut() {
                    obj.insert(
                        "from".to_string(),
                        JsonValue::String(ledger_id.to_string()),
                    );
                }
            }
            let result = state
                .fluree
                .explain_connection(&query_json)
                .await
                .map_err(ServerError::Api)?;
            tracing::info!(
                status = "success",
                query_kind = "jsonld",
                "explain completed (dataset path)"
            );
            return Ok(Json(result));
        }

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

/// Whether a parsed SPARQL query is a graph query (CONSTRUCT / DESCRIBE), whose
/// result is an RDF graph rather than a solution/binding table. Graph queries
/// have no SPARQL-results-JSON / SPARQL-results-XML / AgentJson rendering; their
/// only serializations are JSON-LD (default) and RDF/XML.
fn is_graph_query(ast: Option<&fluree_db_sparql::ast::SparqlAst>) -> bool {
    matches!(
        ast.map(|a| &a.body),
        Some(
            fluree_db_sparql::ast::QueryBody::Construct(_)
                | fluree_db_sparql::ast::QueryBody::Describe(_)
        )
    )
}

/// Pick the formatter config and response `Content-Type` for a SPARQL query that
/// returns a JSON body (i.e. not RDF/XML, SPARQL-results XML, delimited, or
/// AgentJson — those are negotiated on their own paths).
///
/// - CONSTRUCT / DESCRIBE produce a graph, which only has a JSON-LD rendering, so
///   they are always formatted as JSON-LD and labelled `application/ld+json`. The
///   formatter coerces graph results to JSON-LD regardless (issue #1274); forcing
///   the config here keeps the chosen format and the `Content-Type` in agreement.
/// - SELECT / ASK keep the SPARQL-results-JSON default unless the client opts into
///   JSON-LD with `Accept: application/ld+json` (see [`FlureeHeaders::wants_jsonld`]).
fn sparql_json_response_format(
    ast: Option<&fluree_db_sparql::ast::SparqlAst>,
    headers: &FlureeHeaders,
) -> (fluree_db_api::FormatterConfig, &'static str) {
    if is_graph_query(ast) || headers.wants_jsonld() {
        (
            fluree_db_api::FormatterConfig::jsonld(),
            "application/ld+json; charset=utf-8",
        )
    } else {
        // SPARQL-results JSON: the builder default for a SPARQL query.
        (
            fluree_db_api::FormatterConfig::sparql_json(),
            "application/json",
        )
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
            .execution_options(query_execution_options(state))
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
            .execution_options(query_execution_options(state))
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
        .execution_options(query_execution_options(state))
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
            .execution_options(query_execution_options(state))
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
        .execution_options(query_execution_options(state))
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

        // Formatter config + Content-Type for the JSON response paths below.
        // CONSTRUCT/DESCRIBE always render as JSON-LD; SELECT/ASK opt in via
        // `Accept: application/ld+json` (issue #1274). XML / delimited / AgentJson
        // are negotiated separately on their own branches and ignore this.
        let (json_fmt_config, json_content_type) =
            sparql_json_response_format(parsed.ast.as_ref(), headers);

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
                    .format(json_fmt_config.clone())
                    .execution_options(query_execution_options(&state))
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
                    .format(json_fmt_config.clone())
                    .execution_options(query_execution_options(&state))
                    .execute_formatted()
                    .await
                    .inspect_err(|_| {
                        set_span_error_code(&span, "error:QueryFailed");
                    })?
            };
            return Ok((
                [(axum::http::header::CONTENT_TYPE, json_content_type)],
                Json(result),
            )
                .into_response());
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
                .format(json_fmt_config.clone())
                .execution_options(query_execution_options(&state))
                .execute_formatted()
                .await
                .inspect_err(|_| {
                    set_span_error_code(&span, "error:QueryFailed");
                })?;
            return Ok((
                [(axum::http::header::CONTENT_TYPE, json_content_type)],
                Json(result),
            )
                .into_response());
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
                    .execution_options(query_execution_options(&state))
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
                // SPARQL Results XML serializes a solution table, not a graph;
                // reject CONSTRUCT/DESCRIBE here (406) instead of executing and
                // surfacing the formatter's 400 (issue #1274).
                if is_graph_query(parsed.ast.as_ref()) {
                    return Err(ServerError::not_acceptable(
                        "SPARQL Results XML is only available for SPARQL SELECT/ASK queries; \
                         CONSTRUCT/DESCRIBE return a graph (use the default JSON-LD or \
                         Accept: application/rdf+xml)"
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
                    .format(fluree_db_api::FormatterConfig::sparql_xml())
                    .execution_options(query_execution_options(&state))
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
                if !is_graph_query(parsed.ast.as_ref()) {
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
                    .execution_options(query_execution_options(&state))
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
                if is_graph_query(parsed.ast.as_ref()) {
                    return Err(ServerError::not_acceptable(
                        "AgentJson is not available for SPARQL CONSTRUCT/DESCRIBE queries; \
                         use the default (JSON-LD) or Accept: application/rdf+xml"
                            .to_string(),
                    ));
                }
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
                    .execution_options(query_execution_options(&state))
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
                .format(json_fmt_config.clone())
                .execution_options(query_execution_options(&state))
                .execute_formatted()
                .await
                .map_err(ServerError::Api)?;

            return Ok((
                [(axum::http::header::CONTENT_TYPE, json_content_type)],
                Json(result),
            )
                .into_response());
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
                .execution_options(query_execution_options(&state))
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
            // CSV/TSV serialize a solution table; a CONSTRUCT/DESCRIBE graph has
            // no such form, so reject with 406 rather than producing malformed
            // rows or a 500 (issue #1274).
            if is_graph_query(parsed.ast.as_ref()) {
                return Err(ServerError::not_acceptable(format!(
                    "{} is only available for SPARQL SELECT/ASK queries; \
                     CONSTRUCT/DESCRIBE return a graph (use the default JSON-LD or \
                     Accept: application/rdf+xml)",
                    fmt.name().to_uppercase()
                )));
            }
            let result = graph
                .query(fluree.as_ref())
                .sparql(sparql)
                .execution_options(query_execution_options(&state))
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
            // Graph queries have no solution-table XML form — reject with 406
            // rather than executing into the formatter's 400 (issue #1274).
            if is_graph_query(parsed.ast.as_ref()) {
                return Err(ServerError::not_acceptable(
                    "SPARQL Results XML is only available for SPARQL SELECT/ASK queries; \
                     CONSTRUCT/DESCRIBE return a graph (use the default JSON-LD or \
                     Accept: application/rdf+xml)"
                        .to_string(),
                ));
            }
            let xml = graph
                .query(fluree.as_ref())
                .sparql(sparql)
                .format(fluree_db_api::FormatterConfig::sparql_xml())
                .execution_options(query_execution_options(&state))
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
            if !is_graph_query(parsed.ast.as_ref()) {
                return Err(ServerError::not_acceptable(
                    "RDF/XML is only available for SPARQL CONSTRUCT/DESCRIBE queries".to_string(),
                ));
            }

            let xml = graph
                .query(fluree.as_ref())
                .sparql(sparql)
                .format(fluree_db_api::FormatterConfig::rdf_xml())
                .execution_options(query_execution_options(&state))
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
            if is_graph_query(parsed.ast.as_ref()) {
                return Err(ServerError::not_acceptable(
                    "AgentJson is not available for SPARQL CONSTRUCT/DESCRIBE queries; \
                     use the default (JSON-LD) or Accept: application/rdf+xml"
                        .to_string(),
                ));
            }
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
                .execution_options(query_execution_options(&state))
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
            .format(json_fmt_config)
            .execution_options(query_execution_options(&state))
            .execute_formatted()
            .await
            .inspect_err(|_| {
                set_span_error_code(&span, "error:QueryFailed");
            })?;
        Ok((
            [(axum::http::header::CONTENT_TYPE, json_content_type)],
            Json(result),
        )
            .into_response())
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
            // FROM may carry a time-travel suffix (`@t:N` / `@iso:` / `@commit:`);
            // strip it for the auth check so a scoped read token still authorizes.
            let from_ids_raw = fluree_db_api::sparql_dataset_ledger_ids(&sparql)
                .unwrap_or_default();
            let ledger_id_raw = if let Some(ref l) = headers.ledger {
                l.clone()
            } else if from_ids_raw.len() == 1 {
                from_ids_raw[0].clone()
            } else if from_ids_raw.is_empty() {
                return Err(ServerError::bad_request(format!(
                    "Unable to determine ledger for SPARQL explain; include a FROM clause (e.g., FROM <myledger:main>) or send '{}' header.",
                    FlureeHeaders::LEDGER,
                )));
            } else {
                return Err(ServerError::bad_request(
                    "SPARQL explain requires exactly one target ledger (single FROM <ledger>); multi-ledger explain is not supported"
                        .to_string(),
                ));
            };
            let ledger_id = base_ledger_id(&ledger_id_raw)?;
            span.record("ledger_id", ledger_id.as_str());

            // Enforce bearer ledger scope for unsigned requests. We compare
            // against the *base* ledger id (sans `@t:` suffix) so scoped
            // tokens authorize time-travel explains.
            if let Some(p) = bearer.0.as_ref() {
                if !credential.is_signed() && !p.can_read(&ledger_id) {
                    set_span_error_code(&span, "error:Forbidden");
                    // Avoid existence leak
                    return Err(ServerError::not_found("Ledger not found"));
                }
            }

            // If FROM carries a time-travel suffix, route through the
            // dataset-aware connection-explain path so snapshot selection
            // honors `@t:N` / ISO / commit-prefix. Otherwise keep the
            // simple HEAD-load fast path.
            if ledger_id_raw != ledger_id {
                let result = state
                    .fluree
                    .explain_connection_sparql(&sparql)
                    .await
                    .map_err(ServerError::Api)?;
                tracing::info!(status = "success", "explain completed (dataset path)");
                return Ok(Json(result));
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

        // Get ledger id (may include `@t:N` suffix when body's `from` is
        // time-travelled). Use the base for the bearer scope check so
        // scoped read tokens still authorize.
        let ledger_id_raw = match get_ledger_id(None, &headers, &query_json) {
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
        let ledger_id = base_ledger_id(&ledger_id_raw)?;

        // Inject header values into query opts
        inject_headers_into_query(&mut query_json, &headers);

        // Enforce bearer ledger scope for unsigned requests (base id only).
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

        // When the body carries time-travel `from` (or any other dataset
        // feature parse_dataset_spec recognizes), route through the
        // dataset-aware connection-explain path so snapshot selection
        // honors `@t:N` rather than silently loading HEAD.
        if requires_dataset_features(&query_json) {
            let result = match state.fluree.explain_connection(&query_json).await {
                Ok(result) => {
                    tracing::info!(status = "success", "explain completed (dataset path)");
                    result
                }
                Err(e) => {
                    let server_error = ServerError::Api(e);
                    set_span_error_code(&span, "error:InvalidQuery");
                    tracing::error!(error = %server_error, "explain execution failed");
                    return Err(server_error);
                }
            };
            return Ok(Json(result));
        }

        // Execute explain (HEAD fast path)
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
            .execution_options(query_execution_options(state))
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
            .execution_options(query_execution_options(state))
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

    // Delegate the actual execution to the connection-scoped sub-query helper —
    // the same path the multi-query dispatcher uses for each sub-query alias.
    let tracked = has_tracking_opts(&query);
    let outcome = fluree_db_api::query::multi::run_jsonld_subquery(
        state.fluree.as_ref(),
        &query,
        None,
        query_execution_options(state),
    )
    .await
    .map_err(|e| {
        let server_error = ServerError::Api(e);
        set_span_error_code(span, "error:InvalidQuery");
        tracing::error!(
            error = %server_error,
            query_kind = "dataset",
            tracked,
            "dataset query failed"
        );
        server_error
    })?;

    if let Some(tally) = outcome.tally {
        // Record tracker fields on the execution span (parity with prior behavior).
        if let Some(ref time) = tally.time {
            span.record("tracker_time", time.as_str());
        }
        if let Some(fuel) = tally.fuel {
            span.record("tracker_fuel", fuel);
        }
        let headers = tracking_headers(&tally);
        let response =
            fluree_db_api::TrackedQueryResponse::success(outcome.data, Some(tally.clone()));
        tracing::info!(
            status = "success",
            tracked = true,
            query_kind = "dataset",
            time = ?tally.time,
            fuel = tally.fuel
        );
        Ok((headers, Json(response)).into_response())
    } else {
        tracing::info!(
            status = "success",
            query_kind = "dataset",
            result_count = outcome.data.as_array().map(std::vec::Vec::len).unwrap_or(0)
        );
        Ok((HeaderMap::new(), Json(outcome.data)).into_response())
    }
}

// =============================================================================
// Multi-query envelope handler
// =============================================================================

use fluree_db_api::query::multi::MultiQueryError;
use fluree_db_api::query::multi::{
    MultiQueryBounds, MultiQueryRequest, MultiQuerySubquery, MultiQueryValidationError,
    SubqueryLanguage,
};

/// `POST /v1/fluree/multi-query`
///
/// Execute a bundle of independent queries against a single resolved
/// snapshot moment, in parallel under bounded concurrency.
///
/// Wire format documented in `fluree_db_api::query::multi`. Envelope-level
/// validation (bounds, asOf collision, opts.t rejection, history-query
/// rejection, envelope max-fuel rejection) runs before any sub-query
/// executes; per-alias outcomes (success, error, timeout) are assembled
/// into the response body's `results` / `errors` map and the top-level
/// `status` field summarizes the aggregate.
///
/// HTTP status mapping:
/// - **4xx** — envelope validation failed (bounds violation, asOf
///   collision, malformed entry, etc.). No `results` / `errors` keys.
/// - **5xx** — envelope infra failed (snapshot resolution dies; response
///   exceeds the size cap during assembly).
/// - **200** — anything else, including all-sub-queries-failed. Clients
///   branch on `body.status` (`"ok"` | `"partial"` | `"all_failed"`)
///   rather than HTTP code for per-alias outcomes.
pub async fn multi_query(
    State(state): State<Arc<AppState>>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    credential: MaybeCredential,
) -> Result<Response> {
    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);
    let trace_id = extract_trace_id(&credential.headers);
    let span = create_request_span(
        "multi_query",
        request_id.as_deref(),
        trace_id.as_deref(),
        None,
        None,
        Some("multi-query"),
    );

    async move {
        let span = tracing::Span::current();
        tracing::info!(status = "start", "multi-query request received");

        // Auth: bearer or signed credential. Mirrors single-query
        // /fluree/query top-level handler.
        let data_auth = state.config.data_auth();
        if data_auth.mode == crate::config::DataAuthMode::Required
            && !credential.is_signed()
            && bearer.0.is_none()
        {
            set_span_error_code(&span, "error:Unauthorized");
            return Err(ServerError::unauthorized(
                "data auth required: provide a bearer token or signed request",
            ));
        }

        // Negotiate output format from request headers. Multi-query
        // assembles each alias's result inside a JSON envelope, so
        // byte-/string-shaped formats (TSV/CSV/SPARQL XML/RDF/XML) are
        // rejected with 406; unknown `Fluree-Output-Format` values are
        // rejected with 400. Tag the span with the error class that
        // actually fired (not a constant) so trace dashboards filter
        // correctly.
        let envelope_format = match negotiate_multi_query_format(&headers) {
            Ok(f) => f,
            Err(err) => {
                let code = match &err {
                    ServerError::NotAcceptable(_) => "error:NotAcceptable",
                    ServerError::BadRequest(_) => "error:BadRequest",
                    _ => "error:NotAcceptable",
                };
                set_span_error_code(&span, code);
                return Err(err);
            }
        };

        // Parse envelope body via credential to honor JWS-wrapped requests.
        let body: JsonValue = credential.body_json()?;
        let mut envelope: MultiQueryRequest = serde_json::from_value(body).map_err(|e| {
            set_span_error_code(&span, "error:BadRequest");
            ServerError::bad_request(format!("invalid multi-query envelope: {e}"))
        })?;

        // Inject fluree-* headers (policy-class, policy, policy-values,
        // max-fuel, etc.) into the envelope's top-level opts *before*
        // validation. This way the envelope-level rejections (max-fuel
        // unsupported, maxConcurrency bounds) catch values supplied via
        // headers the same as they catch body opts, and the merged opts
        // carry the headers into every sub-query as defaults — parity
        // with single-query `inject_headers_into_query`.
        envelope = inject_headers_into_envelope(envelope, &headers);

        let bounds = MultiQueryBounds::DEFAULT;

        // Validation — we re-run it inside the api crate's dispatcher,
        // but pre-validating here gives us the distinct-ledger set we
        // need for the bearer-scope check before any execution starts.
        let distinct_ledgers =
            match fluree_db_api::query::multi::validate_envelope(&envelope, &bounds) {
                Ok(distinct) => distinct,
                Err(err) => {
                    set_span_error_code(&span, "error:BadRequest");
                    return Err(validation_error_to_server(&err));
                }
            };

        // Bearer ledger-scope enforcement — parity with single-query
        // /query and /query/:ledger. Unsigned bearer tokens may carry a
        // scope that limits which ledgers they can read; any envelope
        // referencing a ledger outside that scope is rejected with 404
        // (avoiding existence leak), matching the single-query response.
        if let Some(principal) = bearer.0.as_ref() {
            if !credential.is_signed() {
                for ledger_id in &distinct_ledgers {
                    if !principal.can_read(ledger_id) {
                        set_span_error_code(&span, "error:Forbidden");
                        return Err(ServerError::not_found("Ledger not found"));
                    }
                }
            }
        }

        // Per-sub-query identity / default-policy-class injection runs
        // here, not inside the api crate. apply_auth_identity_to_opts
        // depends on the server's impersonation table, which is a
        // server concern.
        //
        // Two security invariants this block enforces:
        //
        // 1. The impersonation gate sees the **final** opts.identity
        //    that would be in effect — including any value set at the
        //    envelope level or in the sub.opts override. Without the
        //    pre-merge below, an envelope-level `opts.identity` would
        //    bypass the gate entirely because
        //    `body_requests_impersonation` only inspects the query
        //    body's opts.
        //
        // 2. The gate's decision (force bearer identity, or honour body
        //    opts) is persisted into `sub.query["opts"]`, where the api
        //    crate's dispatcher gives it precedence over `envelope.opts`
        //    and `sub.opts`. The dispatcher's merge rule is
        //    `envelope ⊕ sub.opts ⊕ body opts` with body winning, so
        //    nothing downstream can clobber the forced identity by
        //    setting an unrelated key like `meta` at the envelope or
        //    sub-query level.
        let envelope_opts_owned = envelope.opts.clone();
        let effective_id = effective_identity(&credential, &bearer);
        let default_policy_class = data_auth.default_policy_class.clone();
        for sub in envelope.queries.values_mut() {
            if matches!(sub.language, SubqueryLanguage::JsonLd) {
                premerge_opts_into_subquery_body(envelope_opts_owned.as_ref(), sub);
                apply_envelope_subquery_auth(
                    &state,
                    sub,
                    effective_id.as_deref(),
                    default_policy_class.as_deref(),
                )
                .await;
            } else if matches!(sub.language, SubqueryLanguage::Sparql) {
                // SPARQL aliases are policy-enforced too: resolve identity /
                // policy-class through the same impersonation gate and stash the
                // decision in `sub.opts`, which the api crate's SPARQL path reads
                // (`run_sparql_subquery` → `connection_opts`). Without this a
                // SPARQL alias would run unrestricted while its JSON-LD twin is
                // gated.
                apply_envelope_sparql_auth(
                    &state,
                    sub,
                    envelope_opts_owned.as_ref(),
                    effective_id.as_deref(),
                    default_policy_class.as_deref(),
                )
                .await;
            }
        }

        // Hand off to the api crate: validate (again, cheaply) →
        // resolve snapshot → dispatch → assemble. Per-alias outcomes
        // are folded into the response body; only envelope-level
        // failures bubble up as `MultiQueryError`.
        let mut builder = state.fluree.multi_query().envelope(envelope).bounds(bounds);
        if let Some(cfg) = envelope_format {
            builder = builder.format(cfg);
        }
        let response = match builder.execute().await {
            Ok(r) => r,
            Err(MultiQueryError::Validation(err)) => {
                set_span_error_code(&span, "error:BadRequest");
                return Err(validation_error_to_server(&err));
            }
            Err(MultiQueryError::Snapshot(api_err)) => {
                set_span_error_code(&span, "error:SnapshotResolutionFailed");
                tracing::error!(error = %api_err, "multi-query snapshot resolution failed");
                return Err(ServerError::Api(api_err));
            }
            Err(MultiQueryError::ResponseAssembly(err)) => {
                set_span_error_code(&span, "error:ResponseTooLarge");
                return Err(ServerError::internal(err.to_string()));
            }
            Err(MultiQueryError::EnvelopeRequired) => {
                set_span_error_code(&span, "error:Internal");
                return Err(ServerError::internal(
                    "multi-query envelope was not provided to the dispatcher".to_string(),
                ));
            }
            Err(MultiQueryError::UnsupportedFormat { format }) => {
                set_span_error_code(&span, "error:NotAcceptable");
                return Err(ServerError::not_acceptable(format!(
                    "multi-query format {format:?} produces non-JSON output \
                     and cannot be used inside a multi-query envelope"
                )));
            }
        };

        tracing::info!(
            status = "success",
            query_kind = "multi",
            response_status = ?response.status,
        );
        Ok((HeaderMap::new(), Json(response)).into_response())
    }
    .instrument(span)
    .await
}

/// Map a validation error to the appropriate `ServerError`, preserving the
/// structured discriminator in the message so clients can branch on it.
fn validation_error_to_server(err: &MultiQueryValidationError) -> ServerError {
    let msg = err.to_string();
    // Validation errors are always client-fault — surface as 4xx.
    ServerError::bad_request(msg)
}

/// Negotiate the per-alias output format from the request headers.
///
/// Precedence (most specific wins):
///
/// 1. **`Fluree-Output-Format` header** — opt-in fluree-specific selector
///    that mirrors the CLI's `--format` flag (`json` | `typed-json`).
///    `Fluree-Normalize-Arrays: true` layers on top of either value, the
///    same as `fluree query --format ... --normalize-arrays`.
/// 2. **`Accept` header** — standard HTTP content negotiation.
///    `application/vnd.fluree.agent+json` selects
///    [`FormatterConfig::agent_json`] (honouring `Fluree-Max-Bytes`).
/// 3. **Default** — no format set, so the api crate's per-language
///    defaults (JSON-LD aliases → JSON-LD, SPARQL aliases → SPARQL JSON).
///
/// Byte-/string-shaped Accept values (TSV/CSV/SPARQL XML/RDF XML) are
/// explicit requests for a shape the envelope cannot satisfy — reject
/// with 406 Not Acceptable so the client gets a clear error rather than
/// a silent downgrade.
fn negotiate_multi_query_format(
    headers: &FlureeHeaders,
) -> std::result::Result<Option<fluree_db_api::FormatterConfig>, ServerError> {
    // Precedence (must match docs/api/multi-query.md): `Fluree-Output-Format`
    // is the most specific selector and wins over `Accept`. We resolve it
    // first; only if it's absent do we look at `Accept`. The byte-shape
    // Accept rejection (TSV / CSV / XML / RDF XML → 406) only fires when
    // the caller hasn't already pinned a format via `Fluree-Output-Format` —
    // otherwise a header pair like `Accept: text/csv` +
    // `Fluree-Output-Format: typed-json` would return 406 instead of
    // honouring the explicit selector.
    let output_format = header_value(&headers.raw, "fluree-output-format");
    let normalize_arrays = header_is_true(&headers.raw, "fluree-normalize-arrays");

    // Fluree-Output-Format is the CLI-facing selector. Recognised values
    // mirror `fluree query --format`: `json` (per-language default) and
    // `typed-json` (always-typed literal/IRI shape). Unknown values are
    // rejected before any sub-query runs.
    if let Some(value) = output_format {
        let lower = value.to_ascii_lowercase();
        match lower.as_str() {
            "json" => {
                if normalize_arrays {
                    return Ok(Some(
                        fluree_db_api::FormatterConfig::jsonld().with_normalize_arrays(),
                    ));
                }
                return Ok(None);
            }
            "typed-json" | "typed_json" | "typedjson" => {
                let mut config = fluree_db_api::FormatterConfig::typed_json();
                if normalize_arrays {
                    config = config.with_normalize_arrays();
                }
                return Ok(Some(config));
            }
            other => {
                return Err(ServerError::bad_request(format!(
                    "unknown Fluree-Output-Format value '{other}'; \
                     valid values for multi-query: json, typed-json"
                )));
            }
        }
    }
    // No `Fluree-Output-Format`, but `Fluree-Normalize-Arrays: true` alone
    // still flips the default JSON-LD config — same behaviour as
    // `fluree query --normalize-arrays` without `--format`.
    if normalize_arrays {
        return Ok(Some(
            fluree_db_api::FormatterConfig::jsonld().with_normalize_arrays(),
        ));
    }

    // No explicit Fluree-Output-Format — fall back to Accept negotiation.
    // Byte-shape values can't be embedded in the envelope's JSON results
    // map, so a TSV/CSV/XML/RDF XML request without an explicit selector
    // is a clear "wrong endpoint" — return 406 with a clear error.
    if headers.wants_tsv()
        || headers.wants_csv()
        || headers.wants_sparql_results_xml()
        || headers.wants_rdf_xml()
    {
        return Err(ServerError::not_acceptable(
            "multi-query envelopes can only return JSON results; \
             TSV / CSV / SPARQL XML / RDF XML are not supported here",
        ));
    }

    if headers.wants_agent_json() {
        let mut config = fluree_db_api::FormatterConfig::agent_json();
        if let Some(max_bytes) = headers.max_bytes() {
            config = config.with_max_bytes(max_bytes);
        }
        return Ok(Some(config));
    }
    Ok(None)
}

fn header_value<'a>(headers: &'a axum::http::HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

fn header_is_true(headers: &axum::http::HeaderMap, name: &str) -> bool {
    match header_value(headers, name) {
        Some(v) => v.eq_ignore_ascii_case("true") || v == "1" || v.is_empty(),
        None => false,
    }
}

/// Inject `fluree-*` headers (policy-class, policy, policy-values, etc.)
/// into the envelope's top-level `opts`. Sub-query opts merge against
/// these envelope defaults during dispatch, so the headers reach every
/// alias without per-sub-query repetition.
fn inject_headers_into_envelope(
    mut envelope: MultiQueryRequest,
    headers: &FlureeHeaders,
) -> MultiQueryRequest {
    let mut opts = envelope
        .opts
        .take()
        .unwrap_or_else(|| JsonValue::Object(serde_json::Map::new()));
    if let Some(obj) = opts.as_object_mut() {
        headers.inject_into_opts(obj);
    }
    envelope.opts = Some(opts);
    envelope
}

/// Apply the server's bearer identity / default-policy-class to a
/// single JSON-LD sub-query's `query` body before handing the envelope
/// to the api-crate dispatcher.
///
/// Per-sub-query application uses the sub-query's primary ledger (first
/// entry of `from`) as the impersonation-check context. Sub-queries
/// that span multiple ledgers fall back to the first as a conservative
/// default — same heuristic the previous server-side dispatcher used.
async fn apply_envelope_subquery_auth(
    state: &AppState,
    sub: &mut MultiQuerySubquery,
    bearer_identity: Option<&str>,
    default_policy_class: Option<&str>,
) {
    if bearer_identity.is_none() && default_policy_class.is_none() {
        return;
    }
    let primary_ledger = primary_ledger_from_jsonld(&sub.query);
    crate::routes::policy_auth::apply_auth_identity_to_opts(
        state,
        primary_ledger.as_deref().unwrap_or(""),
        &mut sub.query,
        bearer_identity,
        default_policy_class,
    )
    .await;
}

/// Run the impersonation gate for a **SPARQL** sub-query alias.
///
/// SPARQL bodies carry no `opts` block, so identity / policy inputs ride on the
/// envelope `opts` (header-injected) and the per-alias `sub.opts` override —
/// never on the query string. We reuse the **exact** JSON-LD gate
/// ([`apply_auth_identity_to_opts`]) by feeding it a synthetic
/// `{ "opts": <merged envelope ⊕ sub opts> }` object, then store the gated opts
/// back as `sub.opts`. For SPARQL the api dispatcher merges `envelope ⊕ sub.opts`
/// (there is no body layer), so a forced bearer identity in `sub.opts` wins and
/// cannot be clobbered by a user-supplied envelope/sub `identity`. The
/// per-ledger impersonation check uses the alias's first `FROM` ledger.
///
/// Reusing the JSON-LD gate (rather than re-deriving the decision) keeps SPARQL
/// and JSON-LD aliases on identical impersonation semantics by construction.
async fn apply_envelope_sparql_auth(
    state: &AppState,
    sub: &mut MultiQuerySubquery,
    envelope_opts: Option<&JsonValue>,
    bearer_identity: Option<&str>,
    default_policy_class: Option<&str>,
) {
    if bearer_identity.is_none() && default_policy_class.is_none() {
        return;
    }
    let sparql = sub.query.as_str().unwrap_or_default();
    let ledger = fluree_db_api::sparql_dataset_ledger_ids(sparql)
        .ok()
        .and_then(|ids| ids.into_iter().next())
        .unwrap_or_default();

    // Wrap the merged opts as a synthetic query body so the JSON-LD gate can
    // inspect/force identity & policy-class exactly as it does for JSON-LD.
    let merged = fluree_db_api::query::multi::merged_opts(envelope_opts, sub.opts.as_ref());
    let mut synthetic = JsonValue::Object(serde_json::Map::new());
    if let Some(opts) = merged {
        if let Some(obj) = synthetic.as_object_mut() {
            obj.insert("opts".to_string(), opts);
        }
    }
    crate::routes::policy_auth::apply_auth_identity_to_opts(
        state,
        &ledger,
        &mut synthetic,
        bearer_identity,
        default_policy_class,
    )
    .await;
    sub.opts = synthetic.get("opts").cloned();
}

/// Pre-merge envelope-level `opts` and sub-query `opts` override into
/// the sub-query body's `opts` BEFORE the impersonation gate runs.
///
/// Without this step, an envelope-level `opts.identity` (or one in the
/// per-sub-query opts override) would never reach
/// `body_requests_impersonation`, because the gate only inspects
/// `sub.query["opts"]`. The result would be a silent identity bypass:
/// the user's "request to impersonate" goes through unchecked because
/// the gate didn't see it.
///
/// Precedence (most specific wins): `sub.query["opts"]` already in the
/// body beats `sub.opts`, which beats `envelope.opts`. After this
/// merge, `sub.query["opts"]` holds the final set the gate decides
/// against. The api crate's dispatcher uses the same priority when it
/// later merges envelope / sub / body together, so the gate's decision
/// (written into `sub.query["opts"]`) survives.
fn premerge_opts_into_subquery_body(
    envelope_opts: Option<&JsonValue>,
    sub: &mut MultiQuerySubquery,
) {
    let envelope_with_sub =
        fluree_db_api::query::multi::merged_opts(envelope_opts, sub.opts.as_ref());
    let Some(envelope_with_sub) = envelope_with_sub else {
        return;
    };
    let Some(body) = sub.query.as_object_mut() else {
        return;
    };
    let body_opts = body.remove("opts");
    let final_opts =
        fluree_db_api::query::multi::merged_opts(Some(&envelope_with_sub), body_opts.as_ref());
    if let Some(opts) = final_opts {
        body.insert("opts".to_string(), opts);
    }
}

/// Extract the first ledger identifier (with any temporal suffix and
/// `#fragment` stripped) from a JSON-LD sub-query body's `from` field.
/// Used as the impersonation-check context for
/// [`apply_envelope_subquery_auth`].
fn primary_ledger_from_jsonld(query: &JsonValue) -> Option<String> {
    let from = query.as_object()?.get("from")?;
    let raw = match from {
        JsonValue::String(s) => s.clone(),
        JsonValue::Array(arr) => arr.iter().find_map(|v| match v {
            JsonValue::String(s) => Some(s.clone()),
            JsonValue::Object(obj) => obj
                .get("@id")
                .or_else(|| obj.get("id"))
                .and_then(JsonValue::as_str)
                .map(str::to_string),
            _ => None,
        })?,
        JsonValue::Object(obj) => obj
            .get("@id")
            .or_else(|| obj.get("id"))
            .and_then(JsonValue::as_str)?
            .to_string(),
        _ => return None,
    };
    let bare = raw.split('#').next().unwrap_or(&raw);
    for marker in ["@t:", "@iso:", "@commit:"] {
        if let Some(idx) = bare.find(marker) {
            return Some(bare[..idx].to_string());
        }
    }
    Some(bare.to_string())
}
