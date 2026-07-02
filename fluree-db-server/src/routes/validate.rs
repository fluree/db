//! SHACL validation report endpoint: `GET|POST /v1/fluree/validate/*ledger`.
//!
//! The HTTP surface of `fluree_db_api::validate` — validates the current
//! state of a ledger (or one of its named graphs) against SHACL shapes and
//! returns a validation report, instead of rejecting a transaction the way
//! staging-time enforcement does.
//!
//! `GET` validates against the ledger's attached shapes with default
//! options. `POST` accepts a JSON body selecting the data graph and the
//! shapes source (see [`ValidateBody`]). Ad-hoc shapes REPLACE the attached
//! shapes unless `includeAttached` is set, matching the CLI semantics.
//!
//! Response negotiation via `Accept`:
//! - `text/turtle` → W3C `sh:ValidationReport` as Turtle
//! - `application/ld+json` → W3C `sh:ValidationReport` as JSON-LD
//! - anything else (default) → JSON summary envelope with per-result fields
//!
//! Auth matches the read endpoints (bearer `can_read` on the ledger). Note
//! that validation reads the full graph — results are not policy-filtered,
//! so access to this endpoint implies read access to the data it reports on.

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::{FlureeHeaders, MaybeDataBearer};
use crate::state::AppState;
use crate::telemetry::{
    create_request_span, extract_request_id, extract_trace_id, set_span_error_code,
};
use axum::extract::{Path, Request, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use fluree_db_api::validate::{ShapesSource, ValidateOptions, ValidateReport};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;
use tracing::Instrument;

/// Maximum request-body size (inline shapes documents).
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// POST body for the validate endpoint. All fields optional; an empty or
/// absent body validates the default graph against the attached shapes.
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ValidateBody {
    /// IRI of the named data graph to validate (default: the default graph).
    pub graph: Option<String>,
    /// Ad-hoc shapes: a JSON-LD object/array, or a string containing Turtle.
    pub shapes: Option<JsonValue>,
    /// IRI of a named graph in this ledger holding the shapes.
    pub shapes_graph: Option<String>,
    /// Union ad-hoc shapes with the attached shapes instead of replacing.
    #[serde(default)]
    pub include_attached: bool,
}

/// Validate a ledger's current state (ledger in path tail).
///
/// `GET|POST /v1/fluree/validate/<ledger...>`
pub async fn validate_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    request: Request,
) -> Response {
    // In peer mode, forward to transactor (body still intact).
    if state.config.server_role == ServerRole::Peer {
        let client = match state.forwarding_client.as_ref() {
            Some(c) => c,
            None => {
                return ServerError::internal("Forwarding client not configured").into_response()
            }
        };
        return match client.forward(request).await {
            Ok(resp) => resp,
            Err(e) => e.into_response(),
        };
    }

    let body = match read_body(request).await {
        Ok(body) => body,
        Err(e) => return e.into_response(),
    };
    validate_local(state, ledger, headers, bearer, body)
        .await
        .into_response()
}

async fn read_body(request: Request) -> Result<ValidateBody> {
    let bytes = axum::body::to_bytes(request.into_body(), MAX_BODY_BYTES)
        .await
        .map_err(|e| ServerError::bad_request(format!("failed to read request body: {e}")))?;
    if bytes.is_empty() {
        return Ok(ValidateBody::default());
    }
    serde_json::from_slice(&bytes)
        .map_err(|e| ServerError::bad_request(format!("invalid validate body: {e}")))
}

async fn validate_local(
    state: Arc<AppState>,
    alias: String,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    body: ValidateBody,
) -> Result<Response> {
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "validate",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&alias),
        None,
        None,
    );
    async move {
        let span = tracing::Span::current();
        tracing::info!(status = "start", "shacl validate requested");

        // Enforce data auth (same pattern as the query/show endpoints).
        let data_auth = state.config.data_auth();
        if data_auth.mode == crate::config::DataAuthMode::Required && bearer.0.is_none() {
            set_span_error_code(&span, "error:Unauthorized");
            return Err(ServerError::unauthorized("Bearer token required"));
        }
        if let Some(p) = bearer.0.as_ref() {
            if !p.can_read(&alias) {
                set_span_error_code(&span, "error:Forbidden");
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

        // Validation reads the local index; proxy storage mode has none.
        if state.config.is_proxy_storage_mode() {
            set_span_error_code(&span, "error:NotImplemented");
            return Err(ServerError::NotImplemented(
                "SHACL validation is not available in proxy storage mode".to_string(),
            ));
        }

        let options = ValidateOptions {
            graph: body.graph.clone(),
            shapes: shapes_source(&body)?,
            include_attached: body.include_attached,
        };

        let report = state
            .fluree
            .validate_ledger(&alias, &options)
            .await
            .map_err(|e| {
                if e.is_not_found() {
                    ServerError::not_found(format!("Ledger not found: {alias}"))
                } else {
                    ServerError::Api(e)
                }
            })?;

        tracing::info!(
            status = "success",
            conforms = report.conforms,
            results = report.results.len(),
            shapes = report.shape_count,
            "shacl validate complete"
        );
        Ok(negotiate_response(&report, &headers))
    }
    .instrument(span)
    .await
}

fn shapes_source(body: &ValidateBody) -> Result<ShapesSource> {
    if body.shapes.is_some() && body.shapes_graph.is_some() {
        return Err(ServerError::bad_request(
            "'shapes' and 'shapesGraph' are mutually exclusive",
        ));
    }
    match &body.shapes {
        Some(JsonValue::String(turtle)) => Ok(ShapesSource::InlineTurtle(turtle.clone())),
        Some(doc @ (JsonValue::Object(_) | JsonValue::Array(_))) => {
            Ok(ShapesSource::InlineJsonLd(doc.clone()))
        }
        Some(_) => Err(ServerError::bad_request(
            "'shapes' must be a JSON-LD object/array or a string of Turtle",
        )),
        None => Ok(match &body.shapes_graph {
            Some(iri) => ShapesSource::Graph(iri.clone()),
            None => ShapesSource::Attached,
        }),
    }
}

fn negotiate_response(report: &ValidateReport, headers: &FlureeHeaders) -> Response {
    let accept = headers
        .raw
        .get(http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if accept.contains("text/turtle") {
        return (
            [(http::header::CONTENT_TYPE, "text/turtle")],
            report.to_turtle(),
        )
            .into_response();
    }
    if accept.contains("application/ld+json") {
        return (
            [(http::header::CONTENT_TYPE, "application/ld+json")],
            Json(report.to_jsonld()).into_response().into_body(),
        )
            .into_response();
    }

    // Default: JSON summary envelope with IRI-resolved per-result fields.
    Json(json!({
        "conforms": report.conforms,
        "violations": report.violation_count(),
        "warnings": report.warning_count(),
        "infos": report.info_count(),
        "shapesChecked": report.shape_count,
        "results": report.results,
    }))
    .into_response()
}
