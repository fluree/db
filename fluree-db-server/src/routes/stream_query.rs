//! Streaming query endpoints:
//! - `POST /v1/fluree/stream/query/*ledger` — ledger-scoped
//! - `POST /v1/fluree/stream/query` — connection-scoped (ledger from the request)
//!
//! Emits SELECT results incrementally as NDJSON records
//! (`application/x-ndjson`) instead of buffering the whole result into a single
//! JSON body. A wall-clock heartbeat keeps the connection alive past proxy idle
//! timeouts (e.g. CloudFront/ALB ~60s) during long-running queries, and carries
//! the live fuel total as a progress signal.
//!
//! Policy, `from`/`fromNamed`, and multi-ledger queries route to the
//! connection/dataset path and are enforced exactly like `/query` (for SPARQL,
//! only the ledger-scoped form enforces per-request policy; the connection form
//! refuses policy signals). ASK/CONSTRUCT/DESCRIBE, `selectOne`, hydration, and
//! history (`to` / SPARQL `FROM..TO`) return `4xx` — use `/v1/fluree/query`.
//!
//! The standard `/query` endpoint is untouched: these are separate routes with
//! their own handlers, so the benchmark-critical buffered path never pays for
//! the streaming machinery.

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
    get_ledger_id, has_policy_opts, inject_default_context_if_requested, inject_headers_into_query,
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

/// `POST /v1/fluree/stream/query` — connection-scoped (no path ledger).
///
/// Ledgers come entirely from the request: JSON-LD `from`/`fromNamed` (or the
/// `Fluree-Ledger` header), or SPARQL `FROM`/`FROM NAMED`. Always the
/// connection/dataset path — there is no single-ledger shortcut.
pub async fn stream_query_connection(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SparqlParams>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    credential: MaybeCredential,
) -> Response {
    match stream_query_connection_inner(state, params, headers, bearer, credential).await {
        Ok(response) => response,
        Err(e) => e.into_response(),
    }
}

async fn stream_query_connection_inner(
    state: Arc<AppState>,
    params: SparqlParams,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    credential: MaybeCredential,
) -> Result<Response> {
    let span = tracing::Span::current();

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

    let fluree = state.fluree.clone();

    let (stream_plan, tracker) = if is_sparql_request(&headers, &credential, &params) {
        let sparql = resolve_sparql_text(&params, &credential)?;

        // Connection SPARQL has no single ledger to resolve a per-request
        // identity against, so it cannot enforce identity policy (parity with
        // /query, which runs connection SPARQL unpoliced). Rather than silently
        // ignore an *explicit* policy request, refuse the explicit policy
        // headers (Fluree-Identity / Fluree-Policy* / Fluree-Default-Allow) and
        // point at the ledger-scoped route (which does enforce SPARQL policy).
        // A plain bearer token (auth only) and the server `default_policy_class`
        // are not per-request policy requests and do not apply to SPARQL, so
        // they do not trigger a refusal here — same as /query.
        if request_carries_policy(&headers) {
            return Err(ServerError::bad_request(
                "policy-scoped SPARQL is not supported on the connection-scoped streaming \
                 endpoint; use /v1/fluree/stream/query/<ledger> or /v1/fluree/query",
            ));
        }

        // Bearer scope over every FROM/FROM NAMED ledger.
        if let Some(p) = bearer.0.as_ref() {
            if !credential.is_signed() {
                if let Ok(ledger_ids) = fluree_db_api::sparql_dataset_ledger_ids(&sparql) {
                    for lid in &ledger_ids {
                        if !p.can_read(lid) {
                            return Err(ServerError::not_found("Ledger not found"));
                        }
                    }
                }
            }
        }

        let min_t = collect_sparql_min_t_requirements(headers.min_t, &sparql, None)?;
        await_query_min_t_requirements(state.as_ref(), min_t).await?;

        let dataset = fluree
            .build_stream_dataset_for_sparql(
                &sparql,
                &fluree_db_api::QueryConnectionOptions::default(),
            )
            .await
            .map_err(ServerError::Api)?;
        let input = OwnedStreamQuery::Sparql(sparql);
        let plan = fluree
            .plan_stream_query_dataset(&dataset, &input)
            .await
            .map_err(ServerError::Api)?;
        (
            StreamPlan::Dataset { dataset, plan },
            stream_tracker_from_headers(&headers),
        )
    } else {
        let mut query_json: JsonValue = credential.body_json()?;

        if query_json.get("to").is_some() {
            return Err(ServerError::bad_request(
                "history queries (`to`) are not supported on the streaming endpoint; \
                 use /v1/fluree/query",
            ));
        }

        // Representative ledger from `from`/`fromNamed` or the Fluree-Ledger
        // header; errors if neither is present.
        let ledger_id = get_ledger_id(None, &headers, &query_json)?;

        // If only a header ledger was given, materialize it as a `from` so the
        // dataset spec is non-empty.
        if query_json.get("from").is_none() && query_json.get("fromNamed").is_none() {
            if let Some(obj) = query_json.as_object_mut() {
                obj.insert("from".to_string(), JsonValue::String(ledger_id.clone()));
            }
        }

        inject_headers_into_query(&mut query_json, &headers);
        if let Some(p) = bearer.0.as_ref() {
            if !credential.is_signed() && !p.can_read(&ledger_id) {
                return Err(ServerError::not_found("Ledger not found"));
            }
        }
        enforce_bearer_dataset_scope(&query_json, &bearer, credential.is_signed(), &span)?;
        let identity = effective_identity(&credential, &bearer);
        crate::routes::policy_auth::apply_auth_identity_to_opts(
            state.as_ref(),
            &ledger_id,
            &mut query_json,
            identity.as_deref(),
            data_auth.default_policy_class.as_deref(),
        )
        .await;
        let min_t = collect_jsonld_min_t_requirements(&headers, &query_json, Some(&ledger_id))?;
        await_query_min_t_requirements(state.as_ref(), min_t).await?;
        inject_default_context_if_requested(
            state.as_ref(),
            &ledger_id,
            &mut query_json,
            params.default_context,
        )
        .await?;

        let tracker = stream_tracker(Some(&query_json));
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
    };

    tracing::info!(status = "start", "connection streaming query started");
    Ok(finish_stream(&state, fluree, stream_plan, tracker))
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
        // Resolve identity + policy the same way /query's SPARQL path does:
        // SPARQL has no body `opts`, so policy arrives via the resolved identity
        // (bearer/header), the server default policy class, and the
        // `Fluree-Policy*` / `Fluree-Default-Allow` headers.
        let bearer_identity = effective_identity(&credential, &bearer);
        let identity = crate::routes::policy_auth::resolve_sparql_identity(
            state.as_ref(),
            &ledger,
            bearer_identity.as_deref(),
            headers.identity.as_deref(),
        )
        .await;
        let qc_opts = crate::routes::query::sparql_qc_opts(identity.as_deref(), &headers)?;

        // Detect FROM/FROM NAMED dataset clauses.
        let parsed = fluree_db_sparql::parse_sparql(&sparql);
        let dataset_clause = parsed.ast.as_ref().and_then(|ast| match &ast.body {
            fluree_db_sparql::ast::QueryBody::Select(q) => q.dataset.as_ref(),
            fluree_db_sparql::ast::QueryBody::Construct(q) => q.dataset.as_ref(),
            fluree_db_sparql::ast::QueryBody::Ask(q) => q.dataset.as_ref(),
            fluree_db_sparql::ast::QueryBody::Describe(q) => q.dataset.as_ref(),
            fluree_db_sparql::ast::QueryBody::Update(_) => None,
        });
        let has_dataset = dataset_clause
            .map(|d| {
                !d.default_graphs.is_empty() || !d.named_graphs.is_empty() || d.to_graph.is_some()
            })
            .unwrap_or(false);

        // Freshness barrier parity with /query (header / `@t:` min-t).
        let min_t = collect_sparql_min_t_requirements(headers.min_t, &sparql, Some(&ledger))?;
        await_query_min_t_requirements(state.as_ref(), min_t).await?;

        if qc_opts.has_any_policy_inputs() || has_dataset {
            // Policy and/or FROM → connection/dataset streaming path (enforces
            // policy exactly like /query). History range FROM..TO is a distinct
            // path neither streaming path implements.
            if dataset_clause.and_then(|d| d.to_graph.as_ref()).is_some() {
                return Err(ServerError::bad_request(
                    "SPARQL history range (FROM <...> TO <...>) is not supported on the \
                     streaming endpoint; use /v1/fluree/query",
                ));
            }
            let spec = if has_dataset {
                crate::routes::query::ledger_scoped_sparql_dataset_spec(
                    &ledger,
                    dataset_clause.expect("has_dataset implies a clause"),
                )?
            } else {
                let mut spec = fluree_db_api::DatasetSpec::new();
                spec.default_graphs.push(
                    fluree_db_api::GraphSource::new(&ledger)
                        .with_graph(fluree_db_api::dataset::GraphSelector::Default),
                );
                spec
            };
            // Ensure the head is fresh before view loading (shared storage).
            if !state.config.is_proxy_storage_mode() {
                let _ = load_ledger_for_query(state.as_ref(), &ledger, &span).await?;
            }
            let dataset = fluree
                .build_stream_dataset_from_spec(&spec, &qc_opts)
                .await
                .map_err(ServerError::Api)?;
            let input = OwnedStreamQuery::Sparql(sparql);
            let plan = fluree
                .plan_stream_query_dataset(&dataset, &input)
                .await
                .map_err(ServerError::Api)?;
            (
                StreamPlan::Dataset { dataset, plan },
                stream_tracker_from_headers(&headers),
            )
        } else {
            // Plain single-ledger SPARQL (no policy, no FROM).
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
                stream_tracker_from_headers(&headers),
            )
        }
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

    tracing::info!(status = "start", ledger = %ledger, "streaming query started");
    Ok(finish_stream(&state, fluree, stream_plan, tracker))
}

/// Spawn the producer for a resolved plan and assemble the NDJSON streaming
/// response (cancellation/disconnect guard + heartbeat). Shared by the
/// ledger-scoped and connection-scoped handlers.
fn finish_stream(
    state: &AppState,
    fluree: Arc<fluree_db_api::Fluree>,
    stream_plan: StreamPlan,
    tracker: Tracker,
) -> Response {
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

    ndjson_response(rx, tracker, disconnect_guard, heartbeat)
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

/// True if the request carries any policy-scoping signal: `Fluree-Identity`,
/// `Fluree-Policy`, `Fluree-Policy-Class`, `Fluree-Policy-Values`, or
/// `Fluree-Default-Allow`.
fn request_carries_policy(headers: &FlureeHeaders) -> bool {
    headers.identity.is_some()
        || headers.policy.is_some()
        || !headers.policy_class.is_empty()
        || headers.policy_values.is_some()
        || headers.default_allow
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

/// SPARQL has no body `opts`, so its `max-fuel` arrives via the
/// `Fluree-Max-Fuel` header (parity with `/query`). Fuel/time forced on.
fn stream_tracker_from_headers(headers: &FlureeHeaders) -> Tracker {
    let mut opts = headers.to_tracking_options();
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
            let stream =
                futures::stream::unfold((rx, guard), move |(mut rx, mut guard)| async move {
                    match rx.recv().await {
                        Some(bytes) => Some((Ok::<Bytes, std::io::Error>(bytes), (rx, guard))),
                        None => {
                            if let Some(g) = guard.as_mut() {
                                g.disarm();
                            }
                            None
                        }
                    }
                });
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
