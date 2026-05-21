//! RDF export endpoint: `POST /v1/fluree/export/*ledger`.
//!
//! Returns ledger data as RDF in the requested format. The response body is
//! the raw RDF bytes; `Content-Type` reflects the format.
//!
//! **Auth bracket: admin-protected.** RDF export today reads from the binary
//! index without applying per-flake policy filtering, so it lives in the same
//! bracket as `/create`, `/drop`, and `/reindex` rather than the data-read
//! bracket of `/query` and `/show`. Adding policy-filtered streaming export
//! would let it move to read-auth in the future.

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::FlureeHeaders;
use crate::state::AppState;
use crate::telemetry::{create_request_span, extract_request_id, extract_trace_id};
use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use fluree_db_api::export::ExportFormat;
use fluree_db_api::TimeSpec;
use serde::Deserialize;
use std::sync::Arc;
use tracing::Instrument;

#[derive(Deserialize, Default)]
pub struct ExportRequest {
    /// One of: `turtle`/`ttl`, `ntriples`/`nt`, `nquads`/`n-quads`, `trig`,
    /// `jsonld`/`json-ld`/`json`. Default: `turtle`.
    pub format: Option<String>,
    /// Export all named graphs. Requires a dataset format (`trig` or `nquads`).
    #[serde(default)]
    pub all_graphs: bool,
    /// Export a single named graph by IRI. Mutually exclusive with `all_graphs`.
    pub graph: Option<String>,
    /// Override the JSON-LD prefix context. Either a bare object (`{ "ex": "..." }`)
    /// or a `{ "@context": {...} }` wrapper.
    pub context: Option<serde_json::Value>,
    /// Time spec — transaction number, ISO-8601 datetime, or commit CID prefix.
    pub at: Option<String>,
}

/// `POST /v1/fluree/export/<ledger...>`
pub async fn export_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    headers: FlureeHeaders,
    request: Request,
) -> Response {
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

    export_local(state, ledger, headers, request)
        .await
        .into_response()
}

async fn export_local(
    state: Arc<AppState>,
    ledger: String,
    headers: FlureeHeaders,
    request: Request,
) -> Result<Response> {
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "ledger:export",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&ledger),
        None,
        None,
    );
    async move {
        tracing::info!(status = "start", "ledger export requested");

        let body_bytes = axum::body::to_bytes(request.into_body(), 1024 * 1024)
            .await
            .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
        let req: ExportRequest = if body_bytes.is_empty() {
            ExportRequest::default()
        } else {
            serde_json::from_slice(&body_bytes)
                .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))?
        };

        let format = parse_format(req.format.as_deref().unwrap_or("turtle"))?;

        let mut builder = state.fluree.export(&ledger).format(format);
        if req.all_graphs {
            builder = builder.all_graphs();
        }
        if let Some(iri) = req.graph.as_deref() {
            builder = builder.graph(iri);
        }
        if let Some(at_str) = req.at.as_deref() {
            builder = builder.as_of(parse_time_spec(at_str));
        }
        if let Some(ctx) = req.context.as_ref() {
            builder = builder.context(ctx);
        }

        let mut buf: Vec<u8> = Vec::new();
        let stats = builder.write_to(&mut buf).await.map_err(ServerError::Api)?;
        tracing::info!(
            status = "success",
            triples = stats.triples_written,
            bytes = buf.len(),
            "ledger export complete"
        );

        let content_type = content_type_for(format);
        let resp = Response::builder()
            .status(200)
            .header(header::CONTENT_TYPE, content_type)
            .body(Body::from(buf))
            .map_err(|e| ServerError::internal(format!("failed to build response: {e}")))?;
        Ok(resp)
    }
    .instrument(span)
    .await
}

fn parse_format(s: &str) -> Result<ExportFormat> {
    match s.to_ascii_lowercase().as_str() {
        "turtle" | "ttl" => Ok(ExportFormat::Turtle),
        "ntriples" | "nt" => Ok(ExportFormat::NTriples),
        "nquads" | "n-quads" => Ok(ExportFormat::NQuads),
        "trig" => Ok(ExportFormat::TriG),
        "jsonld" | "json-ld" | "json" => Ok(ExportFormat::JsonLd),
        other => Err(ServerError::bad_request(format!(
            "unknown export format '{other}'"
        ))),
    }
}

fn parse_time_spec(at: &str) -> TimeSpec {
    if let Ok(t) = at.parse::<i64>() {
        TimeSpec::at_t(t)
    } else if at.contains('-') && at.contains(':') {
        TimeSpec::at_time(at.to_string())
    } else {
        TimeSpec::at_commit(at.to_string())
    }
}

fn content_type_for(format: ExportFormat) -> &'static str {
    match format {
        ExportFormat::Turtle => "text/turtle; charset=utf-8",
        ExportFormat::NTriples => "application/n-triples; charset=utf-8",
        ExportFormat::NQuads => "application/n-quads; charset=utf-8",
        ExportFormat::TriG => "application/trig; charset=utf-8",
        ExportFormat::JsonLd => "application/ld+json; charset=utf-8",
    }
}
