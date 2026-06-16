//! Streaming query endpoint: `POST /v1/fluree/stream/query/*ledger`.
//!
//! Emits SELECT results incrementally as NDJSON records
//! (`application/x-ndjson`) instead of buffering the whole result into a single
//! JSON body. A wall-clock heartbeat keeps the connection alive past proxy idle
//! timeouts (e.g. CloudFront/ALB ~60s) during long-running queries, and carries
//! the live fuel total as a progress signal.
//!
//! This endpoint deliberately covers the single-ledger `GraphDb` path only.
//! Per-request identity/policy-class queries, ASK/CONSTRUCT/DESCRIBE,
//! `selectOne`, and hydration are not supported here — they return `4xx` and
//! should use the buffered `/v1/fluree/query` endpoint. Ledger-configured
//! policy is still enforced (the streaming path runs the same operators).
//!
//! The standard `/query` endpoint is untouched: this is a separate route with
//! its own handler, so the benchmark-critical buffered path never pays for the
//! streaming machinery.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::{interval, MissedTickBehavior};

use fluree_db_api::format::ndjson_stream;
use fluree_db_api::{GraphDb, OwnedStreamQuery, Tracker, TrackingOptions};
use serde_json::Value as JsonValue;

use crate::error::{Result, ServerError};
use crate::extract::{FlureeHeaders, MaybeCredential, MaybeDataBearer};
use crate::routes::query::{
    get_ledger_id, has_policy_opts, is_sparql_request, load_ledger_for_query, resolve_sparql_text,
    SparqlParams,
};
use crate::state::AppState;

/// Interval between heartbeat records when no rows are flowing. Chosen well
/// under the typical 60s proxy idle timeout.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// Backpressure depth of the producer→transport channel. A full channel
/// suspends the producer at its next `send`, pausing execution.
const STREAM_CHANNEL_DEPTH: usize = 64;

/// `POST /v1/fluree/stream/query/<ledger...>` — ledger in the greedy path tail.
pub async fn stream_query_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    Query(params): Query<SparqlParams>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    credential: MaybeCredential,
) -> Response {
    match stream_query_inner(state, ledger, params, headers, bearer, credential).await {
        Ok(response) => response,
        Err(e) => e.into_response(),
    }
}

async fn stream_query_inner(
    state: Arc<AppState>,
    ledger: String,
    params: SparqlParams,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    credential: MaybeCredential,
) -> Result<Response> {
    let span = tracing::Span::current();

    // Enforce data auth if configured (Bearer token OR signed request).
    let data_auth = state.config.data_auth();
    if data_auth.mode == crate::config::DataAuthMode::Required
        && !credential.is_signed()
        && bearer.0.is_none()
    {
        return Err(ServerError::unauthorized(
            "Authentication required (signed request or Bearer token)",
        ));
    }

    if headers.is_sparql_update() || credential.is_sparql_update {
        return Err(ServerError::bad_request(
            "SPARQL UPDATE requests should use the /v1/fluree/update endpoint",
        ));
    }

    // Resolve the query body and a fuel/time tracker. SPARQL FROM clauses and
    // unsupported query shapes are rejected later by `plan_stream_query`.
    let (input, tracker) = if is_sparql_request(&headers, &credential, &params) {
        let sparql = resolve_sparql_text(&params, &credential)?;
        if let Some(p) = bearer.0.as_ref() {
            if !credential.is_signed() && !p.can_read(&ledger) {
                return Err(ServerError::not_found("Ledger not found"));
            }
        }
        (OwnedStreamQuery::Sparql(sparql), stream_tracker(None))
    } else {
        let query_json: JsonValue = credential.body_json()?;

        // Per-request identity/policy enforcement routes through the connection
        // path, which the single-ledger streaming path does not implement.
        if has_policy_opts(&query_json) {
            return Err(ServerError::bad_request(
                "identity/policy-scoped queries are not supported on the streaming endpoint; \
                 use /v1/fluree/query",
            ));
        }

        // Path ledger must match any `from` target.
        let ledger_id = get_ledger_id(Some(&ledger), &headers, &query_json)?;
        if ledger_id != ledger {
            return Err(ServerError::bad_request(format!(
                "Ledger mismatch: endpoint ledger is '{ledger}' but query targets '{ledger_id}'"
            )));
        }

        if let Some(p) = bearer.0.as_ref() {
            if !credential.is_signed() && !p.can_read(&ledger) {
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

        let tracker = stream_tracker(Some(&query_json));
        (OwnedStreamQuery::JsonLd(query_json), tracker)
    };

    // Load the ledger (owned) and plan before committing to the 200 stream, so
    // parse errors / unsupported shapes surface as a clean 4xx.
    let ledger_state = load_ledger_for_query(state.as_ref(), &ledger, &span).await?;
    let graph = GraphDb::from_ledger_state(&ledger_state);
    let plan = state
        .fluree
        .plan_stream_query(&graph, &input)
        .await
        .map_err(ServerError::Api)?;
    drop(graph);

    let options =
        crate::query_control::current_query_execution_options(state.config.query_timeout_ms);

    // Producer task: owns the ledger state and drives execution, formatting and
    // flushing each batch as NDJSON records.
    let (tx, rx) = mpsc::channel::<Bytes>(STREAM_CHANNEL_DEPTH);
    let fluree = state.fluree.clone();
    let producer_tracker = tracker.clone();
    tokio::spawn(async move {
        fluree
            .run_stream_query(ledger_state, plan, producer_tracker, options, tx)
            .await;
    });

    tracing::info!(status = "start", ledger = %ledger, "streaming query started");
    Ok(ndjson_response(rx, tracker))
}

/// A fuel + time tracker for the streaming endpoint, honoring any `max-fuel`
/// from JSON-LD `opts`. Fuel/time are forced on so heartbeats carry a live
/// fuel total and the `end` record reports both.
fn stream_tracker(query_json: Option<&JsonValue>) -> Tracker {
    let mut opts = TrackingOptions::from_opts_value(query_json.and_then(|j| j.get("opts")));
    opts.track_fuel = true;
    opts.track_time = true;
    Tracker::new(opts)
}

/// Assemble the response body: forward producer records and inject a heartbeat
/// whenever no record has flowed for [`HEARTBEAT_INTERVAL`]. The heartbeat
/// reads the live fuel total from `tracker` (a lock-free atomic load).
fn ndjson_response(rx: mpsc::Receiver<Bytes>, tracker: Tracker) -> Response {
    let start = Instant::now();
    let mut ticker = interval(HEARTBEAT_INTERVAL);
    // Don't pile up heartbeats after a slow stretch, and don't fire one immediately.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let stream = futures::stream::unfold(
        (rx, ticker, tracker, start),
        move |(mut rx, mut ticker, tracker, start)| async move {
            loop {
                tokio::select! {
                    msg = rx.recv() => {
                        return msg.map(|bytes| {
                            (Ok::<Bytes, std::io::Error>(bytes), (rx, ticker, tracker, start))
                        });
                    }
                    _ = ticker.tick() => {
                        // The interval's first tick fires immediately; skip it so
                        // we never emit a heartbeat before the head record.
                        if start.elapsed() < HEARTBEAT_INTERVAL {
                            continue;
                        }
                        let elapsed_ms = start.elapsed().as_millis() as u64;
                        let hb = ndjson_stream::heartbeat_record(elapsed_ms, tracker.current_fuel());
                        return Some((Ok(Bytes::from(hb)), (rx, ticker, tracker, start)));
                    }
                }
            }
        },
    );

    let body = Body::from_stream(stream);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, ndjson_stream::NDJSON_CONTENT_TYPE)
        .header(header::TRANSFER_ENCODING, "chunked")
        .header(header::CACHE_CONTROL, "no-cache, no-transform")
        .body(body)
        .expect("response builder cannot fail")
}
