//! Streaming SELECT query execution: NDJSON record output.
//!
//! This is the API-side producer behind the server's `/v1/fluree/stream/query`
//! endpoint. It shares the engine (planner + operators) with the buffered
//! [`query`](crate::Fluree::query) path and diverges only at the driver: instead
//! of collecting batches into a `Vec`, it formats and flushes each batch as an
//! NDJSON `row` record via a [`BatchSink`], so rows reach the client
//! incrementally and a long-running query can keep bytes flowing.
//!
//! Flow: the handler calls [`Fluree::plan_stream_query`] first (parse +
//! eligibility + plan) so parse errors and unsupported query shapes surface as
//! a clean `4xx` *before* the `200 OK` stream is committed; it then `spawn`s
//! [`Fluree::run_stream_query`] with an owned `LedgerState` and the plan, wiring
//! the receiver into the response body. Heartbeats are injected by the
//! transport layer (the server), not here.

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::format::iri::IriCompactor;
use crate::format::ndjson_stream;
use crate::format::sparql;
use crate::query::helpers::build_query_result;
use crate::view::{GraphDb, QueryInput};
use crate::{
    ApiError, ExecutableQuery, Fluree, LedgerState, QueryExecutionOptions, QueryResult, Result,
    Tracker, VarRegistry,
};

use fluree_db_query::execute::{
    execute_prepared_streaming, BatchSink, ContextConfig, PrepareConfig,
};

/// An owned query body for a streaming request.
///
/// Owned (not borrowed) so the producer can run inside a `spawn`ed `'static`
/// task after the request body has been consumed.
pub enum OwnedStreamQuery {
    /// SPARQL query text.
    Sparql(String),
    /// JSON-LD query document.
    JsonLd(serde_json::Value),
}

impl OwnedStreamQuery {
    fn as_input(&self) -> QueryInput<'_> {
        match self {
            OwnedStreamQuery::Sparql(s) => QueryInput::Sparql(s),
            OwnedStreamQuery::JsonLd(j) => QueryInput::JsonLd(j),
        }
    }
}

/// A parsed, validated, planned streaming query, ready to execute.
///
/// Produced by [`Fluree::plan_stream_query`] and consumed by
/// [`Fluree::run_stream_query`]. All fields are owned so the plan can move into
/// a spawned task.
pub struct StreamQueryPlan {
    vars: VarRegistry,
    parsed: fluree_db_query::ir::Query,
    executable: ExecutableQuery,
}

impl Fluree {
    /// Parse, validate, and plan a streaming SELECT query.
    ///
    /// Returns an error (for the handler to map to `4xx`) on parse failure, on
    /// SPARQL dataset clauses, or for query shapes the streaming endpoint does
    /// not support: ASK, CONSTRUCT/DESCRIBE, `selectOne`, and hydration queries
    /// (which need async database access during formatting). Those remain on
    /// the buffered `/query` endpoint.
    pub async fn plan_stream_query(
        &self,
        db: &GraphDb,
        input: &OwnedStreamQuery,
    ) -> Result<StreamQueryPlan> {
        let input = input.as_input();

        let (vars, mut parsed) = match &input {
            QueryInput::JsonLd(json) => {
                crate::query::helpers::parse_jsonld_query(
                    json,
                    &db.snapshot,
                    db.default_context.as_ref(),
                    None,
                )?
            }
            QueryInput::Sparql(sparql) => {
                self.validate_sparql_for_view(sparql)?;
                crate::query::helpers::parse_sparql_to_ir(
                    sparql,
                    &db.snapshot,
                    db.default_context.as_ref(),
                )?
            }
        };

        super::query::maybe_wrap_for_graph_source(db, &mut parsed);

        ensure_streamable(&parsed.output)?;

        let executable = self.build_executable_for_view(db, &parsed).await?;

        Ok(StreamQueryPlan {
            vars,
            parsed,
            executable,
        })
    }

    /// Execute a planned streaming query, emitting NDJSON records into `tx`.
    ///
    /// Sends the `head` record, then one `row` record per result row as batches
    /// arrive, then exactly one terminal record (`end` on success, `error` on
    /// failure). Backpressure is natural: a full channel suspends the producer
    /// at the next `tx.send`, pausing execution.
    ///
    /// Intended to be `tokio::spawn`ed by the HTTP handler. Owns the
    /// `LedgerState` (the `GraphDb` borrows it) so it outlives the request.
    pub async fn run_stream_query(
        &self,
        ledger: LedgerState,
        plan: StreamQueryPlan,
        tracker: Tracker,
        options: QueryExecutionOptions,
        tx: mpsc::Sender<Bytes>,
    ) {
        // Charge the one-time query floor for fuel-model parity with the
        // buffered `/query` path. A sub-floor `max-fuel` surfaces as an
        // immediate error terminal.
        if let Err(e) = crate::query::helpers::charge_query_floor(&tracker) {
            let _ = tx
                .send(bytes::Bytes::from(ndjson_stream::error_record(
                    &e.to_string(),
                    0,
                )))
                .await;
            return;
        }

        let graph = GraphDb::from_ledger_state(&ledger);

        // Metadata-only result: carries vars/output/context/binary_graph for the
        // formatter; batches stream separately and are never collected here.
        let meta = build_query_result(
            plan.vars,
            plan.parsed,
            Vec::new(),
            Some(graph.t),
            Some(graph.overlay.clone()),
            graph.binary_graph(),
        );

        let (var_names, head_vars) = sparql::compute_head(&meta);
        let compactor = IriCompactor::new(graph.snapshot.shared_namespaces(), &meta.context);

        // Head first: flushes an immediate first byte and starts the idle clock
        // fresh before any (potentially slow) batch pull.
        if tx
            .send(Bytes::from(ndjson_stream::head_record(&var_names)))
            .await
            .is_err()
        {
            return; // client already gone
        }

        let mut sink = NdjsonRowSink {
            result: &meta,
            compactor: &compactor,
            head_vars: &head_vars,
            var_names: &var_names,
            tx: tx.clone(),
            rows: 0,
            buf: String::new(),
        };

        let exec = self
            .execute_view_streaming(&graph, &meta.vars, &plan.executable, &tracker, &options, &mut sink)
            .await;

        let terminal = match exec {
            Ok(()) => ndjson_stream::end_record(
                sink.rows,
                meta.t,
                tracker.current_fuel(),
                tracker.tally().and_then(|t| t.time).as_deref(),
            ),
            Err(err) => ndjson_stream::error_record(&err.to_string(), sink.rows),
        };
        let _ = tx.send(Bytes::from(terminal)).await;
    }

    /// Streaming sibling of `execute_view_internal_with_r2rml`: builds the same
    /// single-ledger execution context, but drives `execute_prepared_streaming`
    /// with `sink` instead of collecting batches.
    ///
    /// SYNC: keep the context wiring here in step with
    /// `execute_view_internal_with_r2rml` in `view/query.rs`.
    async fn execute_view_streaming<S: BatchSink>(
        &self,
        db: &GraphDb,
        vars: &VarRegistry,
        executable: &ExecutableQuery,
        tracker: &Tracker,
        options: &QueryExecutionOptions,
        sink: &mut S,
    ) -> std::result::Result<(), fluree_db_query::QueryError> {
        let db_ref = db.as_graph_db_ref();
        let prepare_config = PrepareConfig::current(db.binary_store.as_ref());
        let prepared =
            fluree_db_query::execute::prepare_execution_with_config(db_ref, executable, &prepare_config)
                .await?;

        let spatial_map = db.binary_store.as_ref().map(|s| s.spatial_provider_map());
        let uses_fulltext = executable.uses_fulltext();
        let fulltext_map = if uses_fulltext {
            db.binary_store.as_ref().map(|s| s.fulltext_provider_map())
        } else {
            None
        };
        let english_lang_id = if uses_fulltext {
            db.binary_store.as_ref().and_then(|s| s.resolve_lang_id("en"))
        } else {
            None
        };

        let config = ContextConfig {
            tracker: Some(tracker),
            cancellation: options.cancellation.clone(),
            policy_enforcer: db.policy_enforcer().cloned(),
            binary_store: db.binary_store.clone(),
            binary_g_id: db.graph_id,
            dict_novelty: db.dict_novelty.clone(),
            spatial_providers: spatial_map.as_ref(),
            fulltext_providers: fulltext_map.as_ref(),
            english_lang_id,
            remote_service: self.remote_service_executor(),
            strict_bind_errors: true,
            ..Default::default()
        };

        execute_prepared_streaming(db_ref, vars, prepared, config, sink).await
    }
}



/// Reject query shapes the streaming endpoint does not support.
fn ensure_streamable(output: &fluree_db_query::ir::QueryOutput) -> Result<()> {
    if output.is_ask() {
        return Err(ApiError::query(
            "ASK queries are not supported on the streaming endpoint; use /query",
        ));
    }
    if output.construct_template().is_some() {
        return Err(ApiError::query(
            "CONSTRUCT/DESCRIBE queries are not supported on the streaming endpoint; use /query",
        ));
    }
    if output.is_select_one() {
        return Err(ApiError::query(
            "selectOne queries are not supported on the streaming endpoint; use /query",
        ));
    }
    if output.has_hydration() {
        return Err(ApiError::query(
            "hydration queries are not supported on the streaming endpoint; use /query",
        ));
    }
    Ok(())
}

/// [`BatchSink`] that formats each batch as NDJSON `row` records and flushes
/// them to the response channel. Reuses one `String` buffer across batches.
struct NdjsonRowSink<'a> {
    result: &'a QueryResult,
    compactor: &'a IriCompactor,
    head_vars: &'a [fluree_db_query::VarId],
    var_names: &'a [String],
    tx: mpsc::Sender<Bytes>,
    rows: u64,
    buf: String,
}

#[async_trait::async_trait]
impl BatchSink for NdjsonRowSink<'_> {
    async fn push(
        &mut self,
        batch: fluree_db_query::binding::Batch,
    ) -> std::result::Result<(), fluree_db_query::QueryError> {
        self.buf.clear();
        let n = sparql::stream_ndjson_rows(
            &mut self.buf,
            self.result,
            &batch,
            self.head_vars,
            self.var_names,
            self.compactor,
        )
        .map_err(|e| fluree_db_query::QueryError::Internal(format!("stream format error: {e}")))?;
        self.rows += n as u64;

        let bytes = Bytes::from(std::mem::take(&mut self.buf));
        self.tx.send(bytes).await.map_err(|_| {
            // Receiver dropped — the client disconnected. Abort execution.
            fluree_db_query::QueryError::Cancelled {
                reason: fluree_db_core::QueryCancellationReason::ClientDisconnected,
            }
        })?;
        Ok(())
    }
}
