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
use fluree_db_api::{
    DataSetDb, GraphDb, LedgerState, OwnedStreamQuery, StreamDatasetPlan, StreamQueryPlan, Tracker,
    TrackingOptions,
};
use serde_json::Value as JsonValue;

use crate::error::{Result, ServerError};
use crate::extract::{FlureeHeaders, MaybeCredential, MaybeDataBearer};
use crate::query_control::QueryDisconnectGuard;
use crate::routes::query::{
    await_query_min_t_requirements, collect_jsonld_min_t_requirements,
    collect_sparql_min_t_requirements, effective_identity, enforce_bearer_dataset_scope,
    has_policy_opts, inject_default_context_if_requested, inject_headers_into_query,
    is_sparql_request, load_ledger_for_query, normalize_ledger_scoped_from,
    requires_dataset_features, resolve_sparql_text, SparqlParams,
};
use crate::state::AppState;

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

    // Resolve into one of two execution shapes, planned before the 200 stream
    // commits so parse errors / unsupported shapes return a clean 4xx:
    //  - Single: the lean single-ledger GraphDb path (common case).
    //  - Dataset: the connection/dataset path (policy, `from`/`fromNamed`,
    //    multi-ledger), which enforces per-request policy exactly like `/query`.
    let fluree = state.fluree.clone();
    let (stream_plan, tracker) = if is_sparql_request(&headers, &credential, &params) {
        let sparql = resolve_sparql_text(&params, &credential)?;
        if let Some(p) = bearer.0.as_ref() {
            if !credential.is_signed() && !p.can_read(&ledger) {
                return Err(ServerError::not_found("Ledger not found"));
            }
        }
        // SPARQL streaming has no per-request policy resolution yet, so refuse
        // anything that would impose identity/policy scoping (use /query):
        // server default policy class, an authenticated identity, or any of the
        // `Fluree-Identity` / `Fluree-Policy*` / `Fluree-Default-Allow` headers
        // that `/query` would fold into connection options.
        // FROM/FROM NAMED and unsupported shapes are rejected by the planner.
        if data_auth.default_policy_class.is_some()
            || effective_identity(&credential, &bearer).is_some()
            || request_carries_policy(&headers)
        {
            return Err(policy_unsupported());
        }
        // Freshness barrier parity with /query (header / `@t:` min-t).
        let min_t = collect_sparql_min_t_requirements(headers.min_t, &sparql, Some(&ledger))?;
        await_query_min_t_requirements(state.as_ref(), min_t).await?;

        let input = OwnedStreamQuery::Sparql(sparql);
        let ledger_state = load_ledger_for_query(state.as_ref(), &ledger, &span).await?;
        let plan = {
            let graph = GraphDb::from_ledger_state(&ledger_state);
            fluree
                .plan_stream_query(&graph, &input)
                .await
                .map_err(ServerError::Api)?
        };
        (
            StreamPlan::Single { ledger_state, plan },
            stream_tracker(None),
        )
    } else {
        let mut query_json: JsonValue = credential.body_json()?;

        // History queries (top-level `to`) use a distinct execution path that
        // neither streaming path implements; planning here would silently read
        // the current view. Reject — use /query.
        if query_json.get("to").is_some() {
            return Err(ServerError::bad_request(
                "history queries (`to`) are not supported on the streaming endpoint; \
                 use /v1/fluree/query",
            ));
        }

        // Mirror /query's ledger-scoped preprocessing: normalize `from` against
        // the path ledger, fold header opts in, enforce bearer scope over the
        // path ledger and every referenced graph, then apply auth-derived
        // identity + default policy class.
        normalize_ledger_scoped_from(&ledger, &mut query_json)?;
        inject_headers_into_query(&mut query_json, &headers);
        if let Some(p) = bearer.0.as_ref() {
            if !credential.is_signed() && !p.can_read(&ledger) {
                return Err(ServerError::not_found("Ledger not found"));
            }
        }
        enforce_bearer_dataset_scope(&query_json, &bearer, credential.is_signed(), &span)?;
        let identity = effective_identity(&credential, &bearer);
        crate::routes::policy_auth::apply_auth_identity_to_opts(
            state.as_ref(),
            &ledger,
            &mut query_json,
            identity.as_deref(),
            data_auth.default_policy_class.as_deref(),
        )
        .await;

        // Freshness barrier + stored-default-context injection, before planning,
        // to match /query's request controls.
        let min_t = collect_jsonld_min_t_requirements(&headers, &query_json, Some(&ledger))?;
        await_query_min_t_requirements(state.as_ref(), min_t).await?;
        inject_default_context_if_requested(
            state.as_ref(),
            &ledger,
            &mut query_json,
            params.default_context,
        )
        .await?;

        let tracker = stream_tracker(Some(&query_json));

        if requires_dataset_features(&query_json) || has_policy_opts(&query_json) {
            // Dataset path: ensure the spec carries the path ledger as a default
            // graph, build the policy-wrapped dataset, then plan against it.
            if query_json.get("from").is_none() {
                if let Some(obj) = query_json.as_object_mut() {
                    obj.insert("from".to_string(), JsonValue::String(ledger.clone()));
                }
            }
            let dataset = fluree
                .build_stream_dataset(&query_json)
                .await
                .map_err(ServerError::Api)?;
            let input = OwnedStreamQuery::JsonLd(query_json);
            let plan = fluree
                .plan_stream_query_dataset(&dataset, &input)
                .await
                .map_err(ServerError::Api)?;
            (StreamPlan::Dataset { dataset, plan }, tracker)
        } else {
            let input = OwnedStreamQuery::JsonLd(query_json);
            let ledger_state = load_ledger_for_query(state.as_ref(), &ledger, &span).await?;
            let plan = {
                let graph = GraphDb::from_ledger_state(&ledger_state);
                fluree
                    .plan_stream_query(&graph, &input)
                    .await
                    .map_err(ServerError::Api)?
            };
            (StreamPlan::Single { ledger_state, plan }, tracker)
        }
    };

    let options =
        crate::query_control::current_query_execution_options(state.config.query_timeout_ms);
    // Cancellation handle shared with the operators: a timeout timer already
    // fires on it; the disconnect guard below also fires it (ClientDisconnected)
    // when the client drops the response stream mid-execution.
    let disconnect_guard = options
        .cancellation
        .clone()
        .map(crate::query_control::QueryDisconnectGuard::new);

    let (tx, rx) = mpsc::channel::<Bytes>(STREAM_CHANNEL_DEPTH);
    let producer_tracker = tracker.clone();
    match stream_plan {
        StreamPlan::Single { ledger_state, plan } => {
            tokio::spawn(async move {
                fluree
                    .run_stream_query(ledger_state, plan, producer_tracker, options, tx)
                    .await;
            });
        }
        StreamPlan::Dataset { dataset, plan } => {
            tokio::spawn(async move {
                fluree
                    .run_stream_query_dataset(dataset, plan, producer_tracker, options, tx)
                    .await;
            });
        }
    }

    let heartbeat = (state.config.stream_heartbeat_ms > 0)
        .then(|| Duration::from_millis(state.config.stream_heartbeat_ms));

    tracing::info!(status = "start", ledger = %ledger, "streaming query started");
    Ok(ndjson_response(rx, tracker, disconnect_guard, heartbeat))
}

/// The two streaming execution shapes resolved by the handler. Constructed
/// once and matched immediately, so the inter-variant size difference is not
/// worth a heap indirection.
#[allow(clippy::large_enum_variant)]
enum StreamPlan {
    Single {
        ledger_state: LedgerState,
        plan: StreamQueryPlan,
    },
    Dataset {
        dataset: DataSetDb,
        plan: StreamDatasetPlan,
    },
}

/// True if the request carries any policy-scoping signal `/query` would fold
/// into connection options: `Fluree-Identity`, `Fluree-Policy`,
/// `Fluree-Policy-Class`, `Fluree-Policy-Values`, or `Fluree-Default-Allow`.
fn request_carries_policy(headers: &FlureeHeaders) -> bool {
    headers.identity.is_some()
        || headers.policy.is_some()
        || !headers.policy_class.is_empty()
        || headers.policy_values.is_some()
        || headers.default_allow
}

/// Error for query shapes whose policy scoping the streaming endpoint cannot
/// enforce as strongly as `/query`.
fn policy_unsupported() -> ServerError {
    ServerError::bad_request(
        "identity/policy-scoped queries are not supported on the streaming endpoint; \
         use /v1/fluree/query",
    )
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

/// Assemble the response body: forward producer records and, when `heartbeat`
/// is set, inject a heartbeat whenever no record has flowed for that interval.
/// The heartbeat reads the live fuel total from `tracker` (a lock-free atomic
/// load). `heartbeat = None` disables heartbeats entirely.
///
/// `guard` lives in the stream state: if the client drops the response body
/// mid-stream it is dropped while armed and cancels the producer; on normal
/// completion we disarm it before the stream ends.
fn ndjson_response(
    rx: mpsc::Receiver<Bytes>,
    tracker: Tracker,
    guard: Option<QueryDisconnectGuard>,
    heartbeat: Option<Duration>,
) -> Response {
    let body = match heartbeat {
        None => {
            let stream = futures::stream::unfold(
                (rx, guard),
                move |(mut rx, mut guard)| async move {
                    match rx.recv().await {
                        Some(bytes) => Some((Ok::<Bytes, std::io::Error>(bytes), (rx, guard))),
                        None => {
                            if let Some(g) = guard.as_mut() {
                                g.disarm();
                            }
                            None
                        }
                    }
                },
            );
            Body::from_stream(stream)
        }
        Some(period) => {
            let start = Instant::now();
            let mut ticker = interval(period);
            // Don't pile up heartbeats after a slow stretch.
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let stream = futures::stream::unfold(
                (rx, ticker, tracker, start, period, guard),
                move |(mut rx, mut ticker, tracker, start, period, mut guard)| async move {
                    loop {
                        tokio::select! {
                            msg = rx.recv() => {
                                match msg {
                                    Some(bytes) => {
                                        return Some((
                                            Ok::<Bytes, std::io::Error>(bytes),
                                            (rx, ticker, tracker, start, period, guard),
                                        ));
                                    }
                                    None => {
                                        // Producer finished (terminal already sent).
                                        if let Some(g) = guard.as_mut() {
                                            g.disarm();
                                        }
                                        return None;
                                    }
                                }
                            }
                            _ = ticker.tick() => {
                                // The interval's first tick fires immediately; skip
                                // it so we never emit a heartbeat before the head.
                                if start.elapsed() < period {
                                    continue;
                                }
                                let elapsed_ms = start.elapsed().as_millis() as u64;
                                let hb = ndjson_stream::heartbeat_record(
                                    elapsed_ms,
                                    tracker.current_fuel(),
                                );
                                return Some((
                                    Ok(Bytes::from(hb)),
                                    (rx, ticker, tracker, start, period, guard),
                                ));
                            }
                        }
                    }
                },
            );
            Body::from_stream(stream)
        }
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, ndjson_stream::NDJSON_CONTENT_TYPE)
        .header(header::TRANSFER_ENCODING, "chunked")
        .header(header::CACHE_CONTROL, "no-cache, no-transform")
        .body(body)
        .expect("response builder cannot fail")
}
