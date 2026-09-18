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
    /// The ledger's system graphs are excluded unless `system_graphs` is set.
    #[serde(default)]
    pub all_graphs: bool,
    /// Also emit the ledger's system graphs (`#txn-meta`, `#config`) under
    /// `all_graphs`. Diagnostic only — see `ExportBuilder::system_graphs`.
    #[serde(default)]
    pub system_graphs: bool,
    /// Emit edge annotations as raw `f:reifies*` triples instead of RDF 1.2
    /// annotation syntax. Escape hatch for consumers pinned to pre-4.2 bytes.
    #[serde(default)]
    pub raw_reifies: bool,
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
        if req.system_graphs {
            builder = builder.system_graphs();
        }
        if req.raw_reifies {
            builder = builder.raw_reifies();
        }
        if let Some(iri) = req.graph.as_deref() {
            builder = builder.graph(iri);
        }
        if let Some(at_str) = req.at.as_deref() {
            builder = builder.as_of(parse_time_spec(at_str)?);
        }
        if let Some(ctx) = req.context.as_ref() {
            builder = builder.context(ctx);
        }

        let mut buf: Vec<u8> = Vec::new();
        let stats = builder.write_to(&mut buf).await.map_err(ServerError::Api)?;
        tracing::info!(
            status = "success",
            triples = stats.triples_written,
            graphs = stats.graphs_written,
            rows_skipped = stats.rows_skipped,
            named_graphs_omitted = stats.named_graphs_omitted,
            annotations_out_of_scope = stats.annotations_out_of_scope,
            annotations_unresolved = stats.annotations_unresolved,
            bytes = buf.len(),
            "ledger export complete"
        );

        // Tell the client what the export left out. Without this, #1847's
        // complaint — "nothing in the output to suggest anything is missing" —
        // is fixed on the CLI and still true over HTTP: the same builder that
        // makes the CLI print `warning: 1 named graph not exported` returns a
        // bare 200 here. Headers rather than a body field, because the body is
        // the RDF document and must stay parseable by an ordinary RDF client.
        // Only emitted when non-zero, so a clean export is byte-for-byte what
        // it was.
        let content_type = content_type_for(format);
        let mut builder = Response::builder()
            .status(200)
            .header(header::CONTENT_TYPE, content_type);
        if stats.named_graphs_omitted > 0 {
            builder = builder.header(
                "x-fluree-export-named-graphs-omitted",
                stats.named_graphs_omitted,
            );
        }
        if stats.annotations_unresolved > 0 {
            builder = builder.header(
                "x-fluree-export-annotations-unresolved",
                stats.annotations_unresolved,
            );
        }
        if stats.annotations_out_of_scope > 0 {
            // The fourth omission class, and the one the HTTP surface was
            // missing while the CLI warned about it: the body carries
            // `~ <r>` markers whose reifiers' own properties are not in the
            // export. A clean 200 with no signal is the same "nothing
            // suggests anything is missing" problem the other three were
            // given headers to fix.
            builder = builder.header(
                "x-fluree-export-annotations-out-of-scope",
                stats.annotations_out_of_scope,
            );
        }
        if stats.rows_skipped > 0 {
            builder = builder.header("x-fluree-export-rows-skipped", stats.rows_skipped);
        }
        let resp = builder
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

/// Parse the request's `at` field.
///
/// Shares [`TimeSpec::parse_at`] with `fluree query --at` and `fluree export
/// --at` (#1805). This was a byte-identical copy of the CLI's old heuristic, so
/// `POST /export {"at": "t:2"}` sent the literal string `t:2` to the commit
/// prefix resolver exactly as the CLI did.
fn parse_time_spec(at: &str) -> Result<TimeSpec> {
    TimeSpec::parse_at(at)
        .map_err(|e| ServerError::bad_request(format!("invalid 'at' time spec: {e}")))
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
