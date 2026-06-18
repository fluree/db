//! Parallel dispatcher for multi-query envelopes.
//!
//! Schedules each alias's sub-query as its own tokio task, bounded by a
//! semaphore (concurrency cap) and an envelope-wide wall-clock deadline.
//! When the deadline fires, in-flight tasks are aborted and marked as
//! `Timeout` in the per-alias outcomes — they don't bubble up as an
//! envelope-level error.
//!
//! Two layers of timeout:
//!
//! - **Envelope deadline** — bounded by the configured limit plus
//!   optional `opts.timeoutMs` clamped to that limit.
//! - **Per-sub-query effective timeout** — `min(opts.timeoutMs, remaining)`
//!   where `remaining` is the envelope's time budget at the moment the
//!   sub-query's semaphore permit is acquired. A sub-query that waited
//!   30 s in the permit queue on a 60 s envelope gets ≤30 s of execution
//!   regardless of what its own `opts.timeoutMs` says.
//!
//! Each task assembles the merged `@context` / `opts`, applies the
//! envelope snapshot (rewriting `from` to pin per-ledger `t`), then
//! calls into the language-specific helpers
//! ([`run_jsonld_subquery`], [`run_sparql_subquery`] in
//! [`crate::query::multi::run`]). The connection-level ledger cache means
//! parallel sub-queries against the same ledger share a snapshot load.
//!
//! # Public entry point
//!
//! Most callers use the builder:
//!
//! ```ignore
//! let fluree: Arc<Fluree> = Arc::new(FlureeBuilder::memory().build_memory());
//! let response = fluree.multi_query()
//!     .envelope(envelope)
//!     .format(FormatterConfig::typed_json().with_normalize_arrays())
//!     .execute()
//!     .await?;
//! ```
//!
//! `.format(...)` is optional. Without it, JSON-LD aliases format as
//! JSON-LD and SPARQL aliases as SPARQL Results JSON. When set, the
//! format reaches each alias according to its shape:
//!
//! - `TypedJson`, `SparqlJson`, `AgentJson` — applied to **every** alias
//!   (these are cross-language shapes by design).
//! - `JsonLd` — applied to JSON-LD aliases; SPARQL aliases keep their
//!   SPARQL Results JSON default. This is what makes the CLI's
//!   `--normalize-arrays` (a JsonLd + normalize_arrays config) compose
//!   cleanly with mixed-language envelopes.
//! - `Tsv` / `Csv` / `SparqlXml` / `RdfXml` — rejected at `.execute()`
//!   with [`MultiQueryError::UnsupportedFormat`]: the envelope's `results`
//!   map can only carry JSON values.
//!
//! See [`MultiQueryBuilder::format`] for the full table.
//!
//! Downstream consumers (HTTP servers, custom dispatchers) can also
//! call [`run_envelope`] directly if they want to skip the builder.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value as JsonValue;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::Instrument;

use super::response::{assemble_response, ResponseAssemblyError};
use super::run::{run_jsonld_subquery, run_sparql_subquery, SubqueryOutput};
use super::snapshot::{
    apply_snapshot_to_jsonld, apply_snapshot_to_sparql, resolve_envelope_snapshot, EnvelopeSnapshot,
};
use crate::format::{FormatterConfig, OutputFormat};
use crate::query::multi::{
    apply_sparql_context, merged_context, merged_opts, validate_envelope, MultiQueryBounds,
    MultiQueryRequest, MultiQueryResponse, MultiQuerySubquery, MultiQueryValidationError,
    SparqlContextDirectives, SubqueryLanguage,
};
use crate::{
    ApiError, Fluree, GovernanceOptions, QueryExecutionOptions, TrackingOptions, TrackingTally,
};

// =============================================================================
// Public error type
// =============================================================================

/// Envelope-level failure surfaced by [`MultiQueryBuilder::execute`].
///
/// Per-alias failures (one sub-query errored or timed out) land inside
/// [`MultiQueryResponse::errors`] and do not surface here — the envelope
/// is still considered a success in that case (HTTP 200 from the server
/// handler).
#[derive(Debug, thiserror::Error)]
pub enum MultiQueryError {
    /// The envelope itself was malformed or violated server bounds.
    /// Maps to a 4xx HTTP response when surfaced over HTTP.
    #[error(transparent)]
    Validation(#[from] MultiQueryValidationError),
    /// Envelope-entry snapshot resolution failed. Maps to a 5xx HTTP
    /// response when surfaced over HTTP.
    #[error(transparent)]
    Snapshot(#[from] ApiError),
    /// Response assembly exceeded the configured size cap. Maps to a
    /// 5xx HTTP response when surfaced over HTTP.
    #[error(transparent)]
    ResponseAssembly(#[from] ResponseAssemblyError),
    /// Programmer error — `.envelope(…)` was not called on the builder
    /// before `.execute()`.
    #[error("multi-query envelope was not provided before execute()")]
    EnvelopeRequired,
    /// The supplied [`FormatterConfig`] produces a non-JSON output
    /// (TSV / CSV / SPARQL XML / RDF XML). A multi-query envelope
    /// embeds each alias's result inside a JSON response object, so
    /// only JSON-producing formats are valid here.
    #[error(
        "format {format:?} produces non-JSON output and cannot be used inside a multi-query envelope; \
         supported formats: JsonLd, SparqlJson, TypedJson, AgentJson"
    )]
    UnsupportedFormat { format: OutputFormat },
}

/// Whether a [`FormatterConfig`] produces JSON output that can be embedded
/// inside the multi-query envelope's `results` map.
///
/// JSON-producing formats (`JsonLd`, `SparqlJson`, `TypedJson`, `AgentJson`)
/// are accepted; anything that produces a bytes-/string-shaped payload
/// (`Tsv`, `Csv`, `SparqlXml`, `RdfXml`) is rejected — those need a
/// designed per-alias binary response story that doesn't exist in v1.
fn is_json_output_format(format: OutputFormat) -> bool {
    matches!(
        format,
        OutputFormat::JsonLd
            | OutputFormat::SparqlJson
            | OutputFormat::TypedJson
            | OutputFormat::AgentJson
    )
}

// =============================================================================
// Outcome types (used by both the dispatcher and the response assembler)
// =============================================================================

/// Per-alias outcome assembled by the dispatcher and consumed by
/// [`super::response::assemble_response`].
///
/// Crate-internal — callers reach the per-alias outcomes via the
/// [`MultiQueryResponse`] returned by the builder; the raw outcome
/// representation is an implementation detail of the dispatch path.
#[derive(Debug)]
pub(crate) struct AliasOutcome {
    pub alias: String,
    pub kind: AliasOutcomeKind,
}

/// Discriminator for a per-alias result.
#[derive(Debug)]
pub(crate) enum AliasOutcomeKind {
    Success {
        data: JsonValue,
        tally: Option<TrackingTally>,
    },
    Error {
        code: String,
        message: String,
    },
    Timeout {
        /// Effective timeout that fired, in milliseconds. May be the
        /// envelope wall deadline or the sub-query's own
        /// `opts.timeoutMs` — whichever was tighter when the permit
        /// was acquired.
        effective_timeout_ms: u64,
    },
}

// =============================================================================
// Dispatch config (resolved bounds for one envelope)
// =============================================================================

/// Resolved bounds for an envelope: configured limits combined with any
/// envelope-level opts overrides (already clamped to the configured
/// limits at validation time).
///
/// Crate-internal — callers configure bounds via
/// [`MultiQueryBuilder::bounds`]; this struct is the resolved form the
/// dispatcher uses internally.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DispatchConfig {
    pub max_concurrency: usize,
    pub envelope_timeout_ms: u64,
    /// Per-sub-query result-size ceiling in bytes, derived from
    /// [`MultiQueryBounds::max_response_size_bytes`]. Defensive
    /// belt-and-suspenders check that catches a single runaway
    /// sub-query before it contributes to envelope-wide memory
    /// pressure — the assembly-time envelope cap is the strict
    /// guarantee, this one is the per-task early-exit.
    pub max_subquery_response_bytes: usize,
}

impl DispatchConfig {
    pub(crate) fn from_envelope(envelope: &MultiQueryRequest, bounds: &MultiQueryBounds) -> Self {
        let opts = envelope.opts.as_ref().and_then(JsonValue::as_object);

        let max_concurrency = opts
            .and_then(|o| o.get("maxConcurrency"))
            .and_then(JsonValue::as_u64)
            .map(|v| (v as usize).min(bounds.max_concurrency))
            .unwrap_or(bounds.max_concurrency);

        let envelope_timeout_ms = opts
            .and_then(|o| o.get("timeoutMs"))
            .and_then(JsonValue::as_u64)
            .map(|v| v.min(bounds.max_envelope_timeout_ms))
            .unwrap_or(bounds.max_envelope_timeout_ms);

        // Per-sub-query cap defaults to the envelope cap — a single
        // alias is allowed to consume the whole budget, but no more.
        let max_subquery_response_bytes = bounds.max_response_size_bytes;

        Self {
            max_concurrency,
            envelope_timeout_ms,
            max_subquery_response_bytes,
        }
    }
}

// =============================================================================
// Builder (public DX entry point)
// =============================================================================

/// Builder for a multi-query envelope execution, started via
/// [`Fluree::multi_query`].
///
/// # Example
///
/// ```ignore
/// let fluree: Arc<Fluree> = Arc::new(FlureeBuilder::memory().build_memory());
/// let envelope: MultiQueryRequest = serde_json::from_value(body)?;
/// let response = fluree.multi_query()
///     .envelope(envelope)
///     .bounds(MultiQueryBounds::DEFAULT)
///     .execute()
///     .await?;
/// ```
#[must_use = "MultiQueryBuilder is inert until .execute() is awaited"]
pub struct MultiQueryBuilder {
    fluree: Arc<Fluree>,
    envelope: Option<MultiQueryRequest>,
    bounds: MultiQueryBounds,
    format: Option<FormatterConfig>,
    execution: QueryExecutionOptions,
}

impl MultiQueryBuilder {
    pub(crate) fn new(fluree: Arc<Fluree>) -> Self {
        Self {
            fluree,
            envelope: None,
            bounds: MultiQueryBounds::DEFAULT,
            format: None,
            execution: QueryExecutionOptions::default(),
        }
    }

    /// Set the envelope to execute. Required before [`Self::execute`].
    pub fn envelope(mut self, envelope: MultiQueryRequest) -> Self {
        self.envelope = Some(envelope);
        self
    }

    /// Override the configured limits (max sub-queries, max distinct
    /// ledgers, response size cap, etc.). Defaults to
    /// [`MultiQueryBounds::DEFAULT`].
    pub fn bounds(mut self, bounds: MultiQueryBounds) -> Self {
        self.bounds = bounds;
        self
    }

    /// Apply this [`FormatterConfig`] as the envelope-wide default
    /// output format. Matches the single-query `.format(...)` vocabulary
    /// on [`crate::query::builder::FromQueryBuilder`].
    ///
    /// # How the format reaches each alias
    ///
    /// The envelope's `results` map is always JSON, so only JSON-producing
    /// [`OutputFormat`] values are valid here. Their treatment differs by
    /// design:
    ///
    /// | [`OutputFormat`]     | JSON-LD aliases    | SPARQL aliases                                     |
    /// |----------------------|--------------------|----------------------------------------------------|
    /// | `TypedJson`          | applies            | applies (cross-language typed shape)               |
    /// | `SparqlJson`         | applies            | applies (cross-language SPARQL Results JSON shape) |
    /// | `AgentJson`          | applies            | applies (cross-language agent envelope)            |
    /// | `JsonLd`             | applies            | **skipped** — SPARQL Results JSON default kept     |
    /// | `Tsv` / `Csv` / `SparqlXml` / `RdfXml` | rejected at [`Self::execute`] with [`MultiQueryError::UnsupportedFormat`] | rejected |
    ///
    /// `JsonLd` is treated as a JSON-LD-language shape: applying it to a
    /// SPARQL `SELECT` alias would silently swap out SPARQL Results JSON
    /// for the JSON-LD shape, which is rarely what the caller wants.
    /// This is also why the CLI's `--normalize-arrays` (which builds a
    /// `JsonLd` + `normalize_arrays` config) only affects JSON-LD aliases
    /// without rejecting mixed-language envelopes.
    ///
    /// Without a call, defaults stay as today: JSON-LD aliases format
    /// as JSON-LD, SPARQL aliases format as SPARQL Results JSON.
    pub fn format(mut self, config: FormatterConfig) -> Self {
        self.format = Some(config);
        self
    }

    /// Set execution controls applied to every sub-query in the envelope.
    pub fn execution_options(mut self, options: QueryExecutionOptions) -> Self {
        self.execution = options;
        self
    }

    /// Execute the envelope and return the assembled response.
    ///
    /// Validates → resolves the snapshot → dispatches sub-queries in
    /// parallel → assembles per-alias results. Per-alias failures land
    /// inside [`MultiQueryResponse::errors`]; only envelope-level
    /// failures (validation, snapshot resolution, size cap) surface as
    /// `Err(MultiQueryError)`.
    pub async fn execute(self) -> Result<MultiQueryResponse, MultiQueryError> {
        let envelope = self.envelope.ok_or(MultiQueryError::EnvelopeRequired)?;
        if let Some(cfg) = self.format.as_ref() {
            if !is_json_output_format(cfg.format) {
                return Err(MultiQueryError::UnsupportedFormat { format: cfg.format });
            }
        }
        run_envelope(
            self.fluree,
            envelope,
            &self.bounds,
            self.format,
            self.execution,
        )
        .await
    }
}

// =============================================================================
// `Fluree::multi_query()` entry method
// =============================================================================

impl Fluree {
    /// Begin a multi-query envelope builder.
    ///
    /// Multi-query bundles N independent JSON-LD and/or SPARQL queries
    /// into one envelope that runs in parallel against a single
    /// resolved snapshot moment. See the
    /// [Multi-query envelope documentation](https://github.com/fluree/db/blob/main/docs/api/multi-query.md)
    /// for the envelope wire format.
    ///
    /// `Fluree` is used through an `Arc` so each sub-query task can
    /// share the connection-level ledger cache safely. Wrap once at
    /// construction:
    ///
    /// ```ignore
    /// let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    /// let response = fluree.multi_query()
    ///     .envelope(envelope)
    ///     .execute()
    ///     .await?;
    /// ```
    pub fn multi_query(self: &Arc<Self>) -> MultiQueryBuilder {
        MultiQueryBuilder::new(Arc::clone(self))
    }
}

// =============================================================================
// Free-standing entry point (skips the builder)
// =============================================================================

/// Run an envelope end-to-end: validate, resolve the snapshot,
/// dispatch, and assemble the response.
///
/// Crate-internal — used by [`MultiQueryBuilder::execute`]. External
/// callers should go through the builder for a stable public API
/// surface; if a use case emerges for skipping the builder, this can
/// be promoted later.
pub(crate) async fn run_envelope(
    fluree: Arc<Fluree>,
    envelope: MultiQueryRequest,
    bounds: &MultiQueryBounds,
    default_format: Option<FormatterConfig>,
    execution: QueryExecutionOptions,
) -> Result<MultiQueryResponse, MultiQueryError> {
    // Envelope wall-clock starts here so meta.elapsed_ms reflects the
    // full pipeline the client observes: validation, snapshot
    // resolution, dispatch, and assembly. Validation failure surfaces
    // as `MultiQueryError::Validation` before any response body is
    // built (the handler returns 4xx), so the elapsed reading is only
    // observable when at least validation + snapshot succeed.
    let started = Instant::now();

    // Phase instrumentation (debug!): `mq.*` markers localize where an
    // envelope spends time or stalls (validation, snapshot resolution,
    // dispatch, per-sub-query lifecycle, drain). debug! keeps them zero-cost
    // at the prod info! level while remaining available for diagnosis.
    tracing::debug!(aliases = envelope.queries.len(), "mq.run_envelope.start");

    let distinct_ledgers = validate_envelope(&envelope, bounds)?;
    tracing::debug!(
        distinct_ledgers = distinct_ledgers.len(),
        "mq.validate.ok; resolving snapshot"
    );

    let snapshot = Arc::new(
        resolve_envelope_snapshot(fluree.as_ref(), &distinct_ledgers, envelope.as_of.as_ref())
            .await
            .inspect_err(|e| tracing::warn!(error = %e, "mq.snapshot.err"))
            .map_err(MultiQueryError::Snapshot)?,
    );
    tracing::debug!(
        ledgers = snapshot.ledgers.len(),
        "mq.snapshot.resolved; dispatching"
    );

    let include_meta = envelope_meta_enabled(envelope.opts.as_ref());
    let config = DispatchConfig::from_envelope(&envelope, bounds);
    tracing::debug!(
        max_concurrency = config.max_concurrency,
        envelope_timeout_ms = config.envelope_timeout_ms,
        "mq.dispatch.config"
    );

    let outcomes = dispatch_subqueries(
        fluree,
        envelope,
        Arc::clone(&snapshot),
        config,
        default_format,
        execution,
    )
    .await;
    tracing::debug!(
        outcomes = outcomes.len(),
        "mq.dispatch.complete; assembling"
    );
    let elapsed_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64;

    let response = assemble_response(
        outcomes,
        snapshot.as_ref(),
        bounds,
        include_meta,
        elapsed_ms,
    )
    .map_err(MultiQueryError::ResponseAssembly)?;
    tracing::debug!("mq.assemble.ok");

    Ok(response)
}

// =============================================================================
// Dispatcher (parallel fan-out)
// =============================================================================

/// Dispatch every sub-query in the envelope in parallel under the
/// configured bounds, returning per-alias outcomes in submission order.
///
/// Crate-internal — exposed via [`run_envelope`] which feeds the
/// outcomes through [`assemble_response`].
async fn dispatch_subqueries(
    fluree: Arc<Fluree>,
    envelope: MultiQueryRequest,
    snapshot: Arc<EnvelopeSnapshot>,
    config: DispatchConfig,
    default_format: Option<FormatterConfig>,
    execution: QueryExecutionOptions,
) -> Vec<AliasOutcome> {
    let envelope_context = Arc::new(envelope.context.clone());
    let envelope_opts = Arc::new(envelope.opts.clone());
    let envelope_format = Arc::new(default_format);

    let semaphore = Arc::new(Semaphore::new(config.max_concurrency));
    let deadline = Instant::now() + Duration::from_millis(config.envelope_timeout_ms);

    let aliases: Vec<String> = envelope.queries.keys().cloned().collect();
    let mut set: JoinSet<(usize, AliasOutcomeKind)> = JoinSet::new();

    for (idx, (alias, sub)) in envelope.queries.into_iter().enumerate() {
        let fluree = Arc::clone(&fluree);
        let snapshot = Arc::clone(&snapshot);
        let envelope_context = Arc::clone(&envelope_context);
        let envelope_opts = Arc::clone(&envelope_opts);
        let envelope_format = Arc::clone(&envelope_format);
        let execution = execution.clone();
        let semaphore = Arc::clone(&semaphore);

        let span = tracing::debug_span!(
            "sub_query",
            alias = alias.as_str(),
            language = match sub.language {
                SubqueryLanguage::JsonLd => "jsonld",
                SubqueryLanguage::Sparql => "sparql",
            },
            effective_timeout_ms = tracing::field::Empty,
            result_status = tracing::field::Empty,
        );

        // Phase instrumentation (debug!): per-sub-query lifecycle — a
        // never-returning sub-query shows spawn + permit.acquired but no done.
        let log_alias = alias.clone();

        set.spawn(
            async move {
                tracing::debug!(alias = %log_alias, "mq.sub.spawn");
                let _permit = match semaphore.acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => {
                        return (
                            idx,
                            AliasOutcomeKind::Error {
                                code: "internal".into(),
                                message: "dispatcher semaphore closed".into(),
                            },
                        );
                    }
                };
                tracing::debug!(alias = %log_alias, "mq.sub.permit.acquired");

                // Effective timeout = min(opts.timeoutMs, remaining envelope budget),
                // computed at permit acquisition, not envelope entry.
                let remaining = deadline.saturating_duration_since(Instant::now());
                let sub_timeout = sub_query_timeout_ms(&sub, envelope_opts.as_ref().as_ref());
                let effective_ms = sub_timeout
                    .map(|t| t.min(remaining.as_millis() as u64))
                    .unwrap_or(remaining.as_millis() as u64);
                let effective = Duration::from_millis(effective_ms);
                let span = tracing::Span::current();
                span.record("effective_timeout_ms", effective_ms);

                let exec = execute_subquery(
                    fluree.as_ref(),
                    sub,
                    envelope_context.as_ref().as_ref(),
                    envelope_opts.as_ref().as_ref(),
                    envelope_format.as_ref().as_ref(),
                    snapshot.as_ref(),
                    execution,
                );
                let kind = match tokio::time::timeout(effective, exec).await {
                    Ok(Ok(output)) => {
                        // Per-sub-query post-format size check. If a
                        // single sub-query already exceeds the cap we
                        // mark it as an error and drop the data, so a
                        // runaway query doesn't sit in memory waiting
                        // for envelope assembly to reject it.
                        let bytes = serde_json::to_vec(&output.data)
                            .map(|v| v.len())
                            .unwrap_or(0);
                        if bytes > config.max_subquery_response_bytes {
                            AliasOutcomeKind::Error {
                                code: "response_too_large".into(),
                                message: format!(
                                    "sub-query result is {bytes} bytes, exceeds per-sub-query cap of {} bytes",
                                    config.max_subquery_response_bytes
                                ),
                            }
                        } else {
                            AliasOutcomeKind::Success {
                                data: output.data,
                                tally: output.tally,
                            }
                        }
                    }
                    Ok(Err(api_err)) => AliasOutcomeKind::Error {
                        code: classify_error(&api_err),
                        message: api_err.to_string(),
                    },
                    Err(_) => AliasOutcomeKind::Timeout {
                        effective_timeout_ms: effective_ms,
                    },
                };

                let status = match &kind {
                    AliasOutcomeKind::Success { .. } => "ok",
                    AliasOutcomeKind::Error { .. } => "error",
                    AliasOutcomeKind::Timeout { .. } => "timeout",
                };
                span.record("result_status", status);
                tracing::debug!(alias = %log_alias, status, "mq.sub.done");

                (idx, kind)
            }
            .instrument(span),
        );
    }

    // Drain with a hard envelope-deadline guard. When the deadline
    // fires, any tasks still running get aborted and reported as
    // Timeout.
    let mut results: Vec<Option<AliasOutcomeKind>> = (0..aliases.len()).map(|_| None).collect();
    let deadline_sleep = tokio::time::sleep_until(deadline.into());
    tokio::pin!(deadline_sleep);

    loop {
        tokio::select! {
            biased;
            joined = set.join_next() => {
                match joined {
                    Some(Ok((idx, kind))) => {
                        if idx < results.len() {
                            results[idx] = Some(kind);
                        }
                    }
                    Some(Err(join_err)) => {
                        tracing::error!(error = %join_err, "sub-query task join failed");
                    }
                    None => break,
                }
            }
            () = &mut deadline_sleep => {
                // Phase instrumentation (debug!): distinguishes "deadline
                // never fired" from "fired but aborts couldn't land" — abort
                // cannot interrupt a task parked in a sync block_on section, so
                // the drain below can still hang past the fired deadline.
                tracing::warn!(remaining = set.len(), "mq.drain.deadline_fired");
                set.abort_all();
                while let Some(joined) = set.join_next().await {
                    if let Ok((idx, kind)) = joined {
                        if idx < results.len() {
                            results[idx] = Some(kind);
                        }
                    }
                }
                tracing::warn!("mq.drain.deadline_drained");
                break;
            }
        }
    }

    // Slots still None either timed out before completion or panicked.
    // Mark with the envelope budget since that's the closest signal.
    aliases
        .into_iter()
        .enumerate()
        .map(|(idx, alias)| AliasOutcome {
            alias,
            kind: results[idx].take().unwrap_or(AliasOutcomeKind::Timeout {
                effective_timeout_ms: config.envelope_timeout_ms,
            }),
        })
        .collect()
}

/// Build the rewritten sub-query body (merged @context/opts + snapshot
/// applied), then dispatch to the language-specific helper and return
/// the raw output. Errors propagate as `ApiError` which the dispatcher
/// wraps into `AliasOutcomeKind::Error`.
///
/// Identity / policy-class injection happens **outside** this layer —
/// the caller (HTTP handler, custom dispatcher, downstream app) is
/// responsible for setting `opts.identity` / `opts.policy-class` on
/// each sub-query body or on the envelope opts before dispatch. This
/// keeps the dispatcher free of any authn/authz logic.
async fn execute_subquery(
    fluree: &Fluree,
    sub: MultiQuerySubquery,
    envelope_context: Option<&JsonValue>,
    envelope_opts: Option<&JsonValue>,
    envelope_format: Option<&FormatterConfig>,
    snapshot: &EnvelopeSnapshot,
    execution: QueryExecutionOptions,
) -> crate::Result<SubqueryOutput> {
    // Three-layer opts merge with body opts winning. The body layer is
    // pulled out **before** any merge so callers that pre-injected
    // identity / policy-class into the body (e.g. the server handler
    // after running its impersonation gate) keep their decision —
    // overwriting the body opts with `envelope ⊕ sub.opts` would let a
    // user-supplied envelope `meta: true` clobber a server-forced
    // bearer identity. Precedence (most specific wins):
    //
    //   sub.query["opts"]   ← body (server may pre-inject here)
    //   sub.opts            ← per-sub-query override
    //   envelope.opts       ← envelope defaults
    let body_opts = sub.query.as_object().and_then(|o| o.get("opts").cloned());
    let envelope_with_sub = merged_opts(envelope_opts, sub.opts.as_ref());
    let merged_opts_val = merged_opts(envelope_with_sub.as_ref(), body_opts.as_ref());
    let tracking_opts = TrackingOptions::from_opts_value(merged_opts_val.as_ref());
    let tracking_enabled = tracking_opts.track_time
        || tracking_opts.track_fuel
        || tracking_opts.track_policy
        || tracking_opts.max_fuel.is_some();

    match sub.language {
        SubqueryLanguage::JsonLd => {
            let inner_ctx = sub
                .query
                .get("@context")
                .or_else(|| sub.query.get("context"))
                .cloned();
            let merged_ctx = merged_context(envelope_context, inner_ctx.as_ref());

            let mut query_body = sub.query;
            if let Some(obj) = query_body.as_object_mut() {
                if let Some(ctx) = merged_ctx {
                    obj.insert("@context".to_string(), ctx);
                } else {
                    // Explicit reset via sub-query @context: null — strip
                    // any inherited or original entry so downstream parser
                    // sees no context.
                    obj.remove("@context");
                    obj.remove("context");
                }
                if let Some(opts) = merged_opts_val {
                    obj.insert("opts".to_string(), opts);
                }
            }

            apply_snapshot_to_jsonld(&mut query_body, snapshot);

            run_jsonld_subquery(fluree, &query_body, envelope_format.cloned(), execution).await
        }
        SubqueryLanguage::Sparql => {
            let sparql = sub.query.as_str().unwrap_or_default();

            // SPARQL has no inner JSON-LD context — directives come
            // from the envelope context only.
            let directives = envelope_context
                .map(SparqlContextDirectives::from_context)
                .unwrap_or_default();
            let with_directives = apply_sparql_context(sparql, &directives);
            let with_snapshot = apply_snapshot_to_sparql(&with_directives, snapshot);

            let tracking = if tracking_enabled {
                Some(tracking_opts)
            } else {
                None
            };
            // Envelope formats fall into two classes for SPARQL aliases:
            //
            // - **Cross-language shapes** (`TypedJson`, `SparqlJson`,
            //   `AgentJson`): the caller consciously picked a unified
            //   output shape. Apply it to SPARQL aliases too.
            // - **JSON-LD shape** (`JsonLd`, with or without
            //   `normalize_arrays`): would coerce SELECT results out of
            //   SPARQL Results JSON for no clear gain — and silently
            //   change the SPARQL alias's wire shape away from the
            //   per-language default the caller expects. Skip it; the
            //   SPARQL builder's default (SPARQL Results JSON) takes
            //   over.
            //
            // This keeps `--normalize-arrays` (which on the CLI maps to
            // `FormatterConfig::jsonld().with_normalize_arrays()`)
            // applying ONLY to JSON-LD aliases, matching the CLI's
            // documented "normalize-arrays applies to JSON-LD aliases"
            // semantics without rejecting mixed envelopes.
            let sparql_format = envelope_format
                .filter(|cfg| !matches!(cfg.format, OutputFormat::JsonLd))
                .cloned();

            // Policy enforcement for SPARQL aliases. SPARQL bodies carry no
            // `opts` block, so identity / policy-class / inline policy can only
            // reach execution through the merged envelope/sub opts assembled
            // above. Parse them into `GovernanceOptions` and thread them
            // down; `run_sparql_subquery` only diverts to the policy path when
            // an actual policy input is present. The identity here is whatever
            // the caller (HTTP handler) resolved through its impersonation gate
            // — this layer stays authn/authz-agnostic, mirroring JSON-LD.
            let policy_opts = match &merged_opts_val {
                Some(opts) => GovernanceOptions::from_json(&serde_json::json!({ "opts": opts }))
                    .map_err(|e| ApiError::query(format!("invalid sub-query opts: {e}")))?,
                None => GovernanceOptions::default(),
            };
            run_sparql_subquery(
                fluree,
                &with_snapshot,
                Some(policy_opts),
                tracking,
                sparql_format,
                execution,
            )
            .await
        }
    }
}

/// Select the per-sub-query timeout the dispatcher should enforce.
///
/// Precedence matches the opts-merge rule the dispatcher applies in
/// [`execute_subquery`]: `sub.query["opts"]` (body) wins over
/// `sub.opts`, which wins over `envelope.opts`. Without the body
/// check this helper would diverge from the merge — body-level
/// `opts.timeoutMs` would show up in the merged opts but never reach
/// `tokio::time::timeout`.
fn sub_query_timeout_ms(
    sub: &MultiQuerySubquery,
    envelope_opts: Option<&JsonValue>,
) -> Option<u64> {
    if let Some(t) = sub
        .query
        .as_object()
        .and_then(|o| o.get("opts"))
        .and_then(JsonValue::as_object)
        .and_then(|o| o.get("timeoutMs"))
        .and_then(JsonValue::as_u64)
    {
        return Some(t);
    }
    if let Some(t) = sub
        .opts
        .as_ref()
        .and_then(JsonValue::as_object)
        .and_then(|o| o.get("timeoutMs"))
        .and_then(JsonValue::as_u64)
    {
        return Some(t);
    }
    envelope_opts
        .and_then(JsonValue::as_object)
        .and_then(|o| o.get("timeoutMs"))
        .and_then(JsonValue::as_u64)
}

fn classify_error(_err: &ApiError) -> String {
    // Coarse classification — finer categorisation can come later if
    // the response shape requires it.
    "api_error".into()
}

fn envelope_meta_enabled(opts: Option<&JsonValue>) -> bool {
    let Some(opts) = opts.and_then(JsonValue::as_object) else {
        return false;
    };
    if let Some(meta) = opts.get("meta") {
        match meta {
            JsonValue::Bool(true) => return true,
            JsonValue::Object(o) if !o.is_empty() => return true,
            _ => {}
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::multi::{MultiQueryBounds, SubqueryLanguage};
    use serde_json::json;

    fn envelope_with_opts(opts: JsonValue, count: usize) -> MultiQueryRequest {
        let mut queries = indexmap::IndexMap::new();
        for i in 0..count {
            queries.insert(
                format!("q{i}"),
                MultiQuerySubquery {
                    language: SubqueryLanguage::JsonLd,
                    query: json!({ "from": "ledgerA", "select": {"?s": ["*"]}, "where": [] }),
                    opts: None,
                },
            );
        }
        MultiQueryRequest {
            context: None,
            as_of: None,
            opts: Some(opts),
            queries,
        }
    }

    #[test]
    fn dispatch_config_uses_envelope_opts_when_below_limit() {
        let env = envelope_with_opts(json!({ "maxConcurrency": 4, "timeoutMs": 5000 }), 1);
        let bounds = MultiQueryBounds::DEFAULT;
        let cfg = DispatchConfig::from_envelope(&env, &bounds);
        assert_eq!(cfg.max_concurrency, 4);
        assert_eq!(cfg.envelope_timeout_ms, 5000);
    }

    #[test]
    fn dispatch_config_clamps_to_limits() {
        let env = envelope_with_opts(json!({ "maxConcurrency": 9999, "timeoutMs": 9_999_999 }), 1);
        let bounds = MultiQueryBounds {
            max_concurrency: 8,
            max_envelope_timeout_ms: 30_000,
            ..MultiQueryBounds::DEFAULT
        };
        let cfg = DispatchConfig::from_envelope(&env, &bounds);
        assert_eq!(cfg.max_concurrency, 8);
        assert_eq!(cfg.envelope_timeout_ms, 30_000);
    }

    #[test]
    fn dispatch_config_falls_back_to_bounds_when_no_opts() {
        let env = MultiQueryRequest {
            context: None,
            as_of: None,
            opts: None,
            queries: indexmap::IndexMap::new(),
        };
        let bounds = MultiQueryBounds::DEFAULT;
        let cfg = DispatchConfig::from_envelope(&env, &bounds);
        assert_eq!(cfg.max_concurrency, bounds.max_concurrency);
        assert_eq!(cfg.envelope_timeout_ms, bounds.max_envelope_timeout_ms);
    }

    #[test]
    fn sub_query_timeout_prefers_subquery_opts() {
        let sub = MultiQuerySubquery {
            language: SubqueryLanguage::JsonLd,
            query: json!({}),
            opts: Some(json!({ "timeoutMs": 1234 })),
        };
        let env_opts = json!({ "timeoutMs": 9999 });
        let t = sub_query_timeout_ms(&sub, Some(&env_opts));
        assert_eq!(t, Some(1234));
    }

    #[test]
    fn sub_query_timeout_falls_back_to_envelope() {
        let sub = MultiQuerySubquery {
            language: SubqueryLanguage::JsonLd,
            query: json!({}),
            opts: None,
        };
        let env_opts = json!({ "timeoutMs": 5000 });
        let t = sub_query_timeout_ms(&sub, Some(&env_opts));
        assert_eq!(t, Some(5000));
    }

    #[test]
    fn sub_query_timeout_returns_none_when_neither_set() {
        let sub = MultiQuerySubquery {
            language: SubqueryLanguage::JsonLd,
            query: json!({}),
            opts: None,
        };
        let t = sub_query_timeout_ms(&sub, None);
        assert!(t.is_none());
    }

    #[test]
    fn sub_query_timeout_body_opts_win_over_sub_opts_and_envelope() {
        // Regression: the merged opts give body precedence (sub.query["opts"]
        // > sub.opts > envelope.opts), so the dispatcher's effective timeout
        // must look at the body first. Previously sub_query_timeout_ms only
        // checked sub.opts and envelope, so a body-level opts.timeoutMs
        // showed up in the merged opts but didn't actually shorten the
        // tokio::time::timeout.
        let sub = MultiQuerySubquery {
            language: SubqueryLanguage::JsonLd,
            query: json!({ "from": "x", "opts": { "timeoutMs": 250 } }),
            opts: Some(json!({ "timeoutMs": 5_000 })),
        };
        let env_opts = json!({ "timeoutMs": 60_000 });
        let t = sub_query_timeout_ms(&sub, Some(&env_opts));
        assert_eq!(t, Some(250), "body opts.timeoutMs must take precedence");
    }

    #[test]
    fn sub_query_timeout_falls_through_to_sub_opts_when_no_body_opt() {
        // No body-level timeout — sub.opts wins, envelope is the last
        // resort. Confirms the chain still works for the existing case.
        let sub = MultiQuerySubquery {
            language: SubqueryLanguage::JsonLd,
            query: json!({ "from": "x", "opts": { "meta": true } }),
            opts: Some(json!({ "timeoutMs": 1_234 })),
        };
        let env_opts = json!({ "timeoutMs": 9_999 });
        let t = sub_query_timeout_ms(&sub, Some(&env_opts));
        assert_eq!(t, Some(1_234));
    }

    // -------------------------------------------------------------------------
    // Merge-order regression: envelope ⊕ sub.opts ⊕ body opts, body wins.
    //
    // Server pre-injects authoritative bits (e.g. forced bearer identity
    // from the impersonation gate) into `sub.query["opts"]`. The
    // dispatcher MUST give those body-level opts precedence over
    // envelope.opts and sub.opts — otherwise a user-supplied envelope
    // `meta: true` or `timeoutMs: N` can silently clobber the forced
    // identity.
    // -------------------------------------------------------------------------

    /// Reimplementation of `execute_subquery`'s merge step in isolation
    /// so we can assert what ends up in `sub.query["opts"]` without
    /// actually executing a query. Mirrors the production code in this
    /// file — keep them in sync.
    fn compute_final_opts(
        envelope_opts: Option<&JsonValue>,
        sub: &MultiQuerySubquery,
    ) -> Option<JsonValue> {
        let body_opts = sub.query.as_object().and_then(|o| o.get("opts").cloned());
        let envelope_with_sub = merged_opts(envelope_opts, sub.opts.as_ref());
        merged_opts(envelope_with_sub.as_ref(), body_opts.as_ref())
    }

    #[test]
    fn merge_body_opts_win_over_envelope_opts_on_key_conflict() {
        // Server pre-injects `opts.identity = "bearer:DID"` into the
        // body. Envelope opts has `meta: true`. After merge:
        // - meta survives (envelope-only key)
        // - identity stays as the body's value (NOT clobbered by an
        //   envelope value if the user tried to spoof)
        let envelope = json!({ "meta": true, "identity": "user:spoofed" });
        let sub = MultiQuerySubquery {
            language: SubqueryLanguage::JsonLd,
            query: json!({ "from": "x", "opts": { "identity": "bearer:DID" } }),
            opts: None,
        };
        let final_opts = compute_final_opts(Some(&envelope), &sub).unwrap();
        assert_eq!(final_opts["meta"], true, "envelope meta survives");
        assert_eq!(
            final_opts["identity"], "bearer:DID",
            "body identity must win over envelope identity"
        );
    }

    #[test]
    fn merge_body_opts_win_over_sub_opts_on_key_conflict() {
        // Same precedence applies between sub.opts and body opts: a
        // server-injected identity in the body wins over a per-sub-query
        // override.
        let sub = MultiQuerySubquery {
            language: SubqueryLanguage::JsonLd,
            query: json!({ "from": "x", "opts": { "identity": "bearer:DID" } }),
            opts: Some(json!({ "identity": "sub:spoofed", "timeoutMs": 5000 })),
        };
        let final_opts = compute_final_opts(None, &sub).unwrap();
        assert_eq!(
            final_opts["identity"], "bearer:DID",
            "body identity must win over sub.opts identity"
        );
        assert_eq!(
            final_opts["timeoutMs"], 5000,
            "sub.opts timeoutMs (no conflict) survives"
        );
    }

    #[test]
    fn merge_envelope_opts_lift_to_body_when_no_conflict() {
        // Envelope opts that don't conflict with anything in body
        // should still reach the body, so existing behaviour (envelope
        // defaults apply to every sub-query) is preserved.
        let envelope = json!({ "meta": true });
        let sub = MultiQuerySubquery {
            language: SubqueryLanguage::JsonLd,
            query: json!({ "from": "x" }), // no body opts
            opts: None,
        };
        let final_opts = compute_final_opts(Some(&envelope), &sub).unwrap();
        assert_eq!(final_opts["meta"], true);
    }

    #[test]
    fn merge_sub_opts_win_over_envelope_opts() {
        let envelope = json!({ "timeoutMs": 60_000 });
        let sub = MultiQuerySubquery {
            language: SubqueryLanguage::JsonLd,
            query: json!({ "from": "x" }),
            opts: Some(json!({ "timeoutMs": 1000 })),
        };
        let final_opts = compute_final_opts(Some(&envelope), &sub).unwrap();
        assert_eq!(final_opts["timeoutMs"], 1000);
    }
}
