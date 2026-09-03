//! GraphQL endpoints: `/v1/fluree/graphql/*ledger` and `/v1/fluree/graphql-schema/*ledger`.
//!
//! The schema is derived from the ledger's own data, so there is nothing to
//! register: any ledger with typed subjects answers introspection and queries.
//!
//! Like SPARQL and Cypher, a GraphQL request has no body `opts` block, so
//! identity and policy travel entirely in headers.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use fluree_db_api::graphql::Limits;
use fluree_db_api::graphql::{GraphQlRequest, PreparedRequest};
use fluree_db_api::QueryExecutionOptions;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tracing::Instrument;

use crate::error::{Result, ServerError};
use crate::extract::{FlureeHeaders, MaybeCredential, MaybeDataBearer};
use crate::state::AppState;
use crate::telemetry::{create_request_span, extract_request_id, extract_trace_id};

/// `GET /v1/fluree/graphql/*ledger?query=…` — the shape GraphiQL and other
/// browser clients use.
#[derive(Debug, Default, Deserialize)]
pub struct GraphQlParams {
    pub query: Option<String>,
    /// JSON object, URL-encoded.
    pub variables: Option<String>,
    #[serde(rename = "operationName")]
    pub operation_name: Option<String>,
    /// Return `extensions.explain`. Also settable as `extensions.explain` in a
    /// JSON request body, which is where a GraphQL client would put it.
    pub explain: Option<bool>,
}

/// `POST /v1/fluree/graphql/*ledger`
///
/// Accepts `application/json` (`{query, variables, operationName}`) and
/// `application/graphql` (the document as the raw body).
pub async fn graphql_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    Query(params): Query<GraphQlParams>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    credential: MaybeCredential,
) -> Result<Response> {
    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);
    let trace_id = extract_trace_id(&credential.headers);
    let span = create_request_span(
        "graphql",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&ledger),
        None,
        None,
    );

    // One document resolves its root fields concurrently, so all of a request's
    // queries share the cancellation handle this installs: a timeout or a
    // client disconnect cancels the whole fan-out, not one field of it.
    let timeout_ms = state.config.query_timeout_ms;
    let limits = graphql_limits(&state);
    crate::query_control::run_query_task(timeout_ms, move || {
        async move {
            authorize_read(&state, &ledger, &bearer, &credential)?;
            let request = parse_request(&params, &credential, limits)?;
            let options = crate::query_control::current_query_execution_options(timeout_ms);

            // Parsed once, here: the document decides which path to take, and
            // whichever one runs reuses this parse rather than repeating it.
            let prepared = match PreparedRequest::new(&request) {
                Ok(prepared) => prepared,
                // A refused document is already a GraphQL error envelope.
                Err(envelope) => return Ok(Json(envelope).into_response()),
            };

            // A GraphQL error is part of the response body, not a transport failure:
            // the spec has clients read `errors`, and returning 4xx for an unknown
            // field would break every standard client.
            let response = if prepared.writes() {
                execute_mutation(
                    &state,
                    &ledger,
                    &headers,
                    &bearer,
                    &credential,
                    prepared,
                    options,
                )
                .await?
            } else {
                let view = policy_view(&state, &ledger, &headers, &bearer, &credential).await?;
                state
                    .fluree
                    .graphql_with_options(&view, prepared, options)
                    .await
                    .map_err(ServerError::Api)?
            };
            Ok(Json(response).into_response())
        }
        .instrument(span)
    })
    .await
}

/// `GET /v1/fluree/graphql-schema/*ledger` — the derived schema as SDL.
///
/// A separate path rather than a suffix on the query route: ledger names may
/// contain `/`, so the greedy tail would swallow a `/schema` suffix.
pub async fn graphql_schema_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    credential: MaybeCredential,
) -> Result<Response> {
    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);
    let trace_id = extract_trace_id(&credential.headers);
    let span = create_request_span(
        "graphql:schema",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&ledger),
        None,
        None,
    );

    async move {
        authorize_read(&state, &ledger, &bearer, &credential)?;
        let view = policy_view(&state, &ledger, &headers, &bearer, &credential).await?;
        // Includes mutations when the ledger's `graphql:Schema` enables them,
        // so the SDL matches what this endpoint will actually accept.
        let sdl = fluree_db_api::graphql::schema_sdl_with_mutations(&view)
            .await
            .map_err(ServerError::Api)?;
        Ok((
            [(
                axum::http::header::CONTENT_TYPE,
                "application/graphql; charset=utf-8",
            )],
            sdl,
        )
            .into_response())
    }
    .instrument(span)
    .await
}

/// Run a writing GraphQL request, committing to the ledger.
async fn execute_mutation(
    state: &Arc<AppState>,
    ledger: &str,
    headers: &FlureeHeaders,
    bearer: &MaybeDataBearer,
    credential: &MaybeCredential,
    prepared: PreparedRequest<'_>,
    options: QueryExecutionOptions,
) -> Result<JsonValue> {
    // Writing needs write authority, which reading does not imply.
    if let Some(p) = bearer.0.as_ref() {
        if !credential.is_signed() && !p.can_write(ledger) {
            return Err(ServerError::not_found("Ledger not found"));
        }
    }
    // Policy still applies: `wrap_policy` on the read view is what prunes the
    // schema, and the transaction path enforces write policy itself.
    let _ = policy_view(state, ledger, headers, bearer, credential).await?;

    let loaded = state
        .fluree
        .ledger(ledger)
        .await
        .map_err(ServerError::Api)?;
    let context = state
        .fluree
        .get_default_context(ledger)
        .await
        .map_err(ServerError::Api)?;
    let (response, _committed) = state
        .fluree
        .graphql_transact_with_options(loaded, context, prepared, options)
        .await
        .map_err(ServerError::Api)?;
    Ok(response)
}

/// The configured document bounds. `0` on either knob means "no ceiling",
/// matching how `query_timeout_ms = 0` disables the timeout.
fn graphql_limits(state: &AppState) -> Limits {
    let unlimited = Limits::unlimited();
    Limits {
        max_depth: match state.config.graphql_max_depth {
            0 => unlimited.max_depth,
            n => n,
        },
        max_complexity: match state.config.graphql_max_complexity {
            0 => unlimited.max_complexity,
            n => n,
        },
    }
}

fn authorize_read(
    state: &AppState,
    ledger: &str,
    bearer: &MaybeDataBearer,
    credential: &MaybeCredential,
) -> Result<()> {
    let data_auth = state.config.data_auth();
    if data_auth.mode == crate::config::DataAuthMode::Required
        && bearer.0.is_none()
        && !credential.is_signed()
    {
        return Err(ServerError::unauthorized("Bearer token required"));
    }
    if let Some(p) = bearer.0.as_ref() {
        if !credential.is_signed() && !p.can_read(ledger) {
            // Not 403: whether a ledger exists is itself privileged.
            return Err(ServerError::not_found("Ledger not found"));
        }
    }
    Ok(())
}

/// The ledger view, wrapped in policy when the request carries any policy input.
///
/// The default context is attached: it is what shortens IRIs into the GraphQL
/// names this ledger's users already write, so the schema depends on it.
async fn policy_view(
    state: &AppState,
    ledger: &str,
    headers: &FlureeHeaders,
    bearer: &MaybeDataBearer,
    credential: &MaybeCredential,
) -> Result<fluree_db_api::GraphDb> {
    let bearer_identity = crate::routes::query::effective_identity(credential, bearer);
    let identity = crate::routes::policy_auth::resolve_sparql_identity(
        state,
        ledger,
        bearer_identity.as_deref(),
        headers.identity.as_deref(),
    )
    .await;

    let opts = fluree_db_api::GovernanceOptions {
        identity,
        policy_class: (!headers.policy_class.is_empty()).then(|| headers.policy_class.clone()),
        policy: headers.policy.clone(),
        policy_values: headers.policy_values_map()?,
        default_allow: headers.default_allow,
    };

    let view = state
        .fluree
        .db_with_default_context(ledger)
        .await
        .map_err(ServerError::Api)?;
    if opts.has_any_policy_inputs() {
        state
            .fluree
            .wrap_policy(view, &opts, None)
            .await
            .map_err(ServerError::Api)
    } else {
        Ok(view)
    }
}

fn parse_request(
    params: &GraphQlParams,
    credential: &MaybeCredential,
    limits: Limits,
) -> Result<GraphQlRequest> {
    // A `?query=` parameter wins: that is a GET, which has no body.
    if let Some(query) = &params.query {
        let variables = match &params.variables {
            Some(raw) => Some(serde_json::from_str::<JsonValue>(raw).map_err(|e| {
                ServerError::bad_request(format!("`variables` is not valid JSON: {e}"))
            })?),
            None => None,
        };
        return Ok(GraphQlRequest {
            query: query.clone(),
            variables,
            operation_name: params.operation_name.clone(),
            explain: params.explain.unwrap_or(false),
            limits,
        });
    }

    let body = credential.body_string()?;
    // `application/graphql` sends the document as the whole body, so anything
    // that is not a JSON envelope is treated as the document itself.
    let Ok(JsonValue::Object(envelope)) = serde_json::from_str::<JsonValue>(&body) else {
        let mut request = GraphQlRequest::new(body);
        request.explain = params.explain.unwrap_or(false);
        request.limits = limits;
        return Ok(request);
    };
    let query = envelope
        .get("query")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| ServerError::bad_request("the request body has no `query`"))?;
    Ok(GraphQlRequest {
        query: query.to_string(),
        variables: envelope.get("variables").cloned(),
        operation_name: envelope
            .get("operationName")
            .and_then(JsonValue::as_str)
            .map(str::to_string),
        // The request envelope's own `extensions` is where a GraphQL client
        // puts out-of-band flags; `?explain=true` is the convenience form.
        explain: params.explain.unwrap_or(false)
            || envelope
                .get("extensions")
                .and_then(|e| e.get("explain"))
                .and_then(JsonValue::as_bool)
                .unwrap_or(false),
        limits,
    })
}
