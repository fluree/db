//! Parallel dispatcher for multi-query envelopes.
//!
//! Schedules each alias's sub-query as its own tokio task, bounded by a
//! semaphore (concurrency cap) and an envelope-wide wall-clock deadline.
//! When the deadline fires, in-flight tasks are dropped (cancelled) and
//! marked as `Timeout` in the per-alias outcomes — they don't bubble up as
//! an envelope-level error.
//!
//! Two layers of timeout:
//! - **Envelope deadline**: bounded by server config plus optional
//!   `opts.timeoutMs` clamped to the server limit.
//! - **Per-sub-query effective timeout**: `min(opts.timeoutMs, remaining)`
//!   where `remaining` is the envelope's time budget at the moment the
//!   sub-query's semaphore permit is acquired. A sub-query that waited
//!   30s in the permit queue on a 60s envelope gets ≤30s of execution
//!   regardless of what its own `opts.timeoutMs` says.
//!
//! Each task assembles the merged `@context` / `opts`, applies the
//! envelope snapshot (rewriting `from` to pin per-ledger `t`), then calls
//! into the existing single-query helpers (`run_jsonld_subquery`,
//! `run_sparql_subquery`). The connection cache means parallel sub-queries
//! against the same ledger share a snapshot load.

use std::sync::Arc;
use std::time::{Duration, Instant};

use fluree_db_api::query::multi::{
    apply_sparql_context, merged_context, merged_opts, MultiQueryBounds, MultiQueryRequest,
    MultiQuerySubquery, SparqlContextDirectives, SubqueryLanguage,
};
use fluree_db_api::query::multi_snapshot::{
    apply_snapshot_to_jsonld, apply_snapshot_to_sparql, EnvelopeSnapshot,
};
use fluree_db_api::{TrackingOptions, TrackingTally};
use serde_json::Value as JsonValue;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::Instrument;

use crate::error::ServerError;
use crate::state::AppState;

use super::policy_auth::apply_auth_identity_to_opts;
use super::query::{run_jsonld_subquery, run_sparql_subquery, SubqueryOutput};

/// Per-alias outcome assembled into the response by task #5.
#[derive(Debug)]
pub(crate) struct AliasOutcome {
    pub alias: String,
    pub kind: AliasOutcomeKind,
}

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
        /// envelope wall deadline or the sub-query's own `opts.timeoutMs`
        /// — whichever was tighter when the permit was acquired.
        effective_timeout_ms: u64,
    },
}

/// Resolved bounds for an envelope: server limits combined with any
/// envelope-level opts overrides (already clamped to the server limits at
/// validation time).
#[derive(Debug, Clone, Copy)]
pub(crate) struct DispatchConfig {
    pub max_concurrency: usize,
    pub envelope_timeout_ms: u64,
    /// Per-sub-query result-size ceiling in bytes, derived from
    /// [`MultiQueryBounds::max_response_size_bytes`]. This is a
    /// defensive belt-and-suspenders check that catches a single
    /// runaway sub-query before it contributes to envelope-wide memory
    /// pressure — the assembly-time envelope cap is the strict
    /// guarantee, this one is the per-task early-exit.
    pub max_subquery_response_bytes: usize,
}

/// Bearer-derived identity inputs applied to every JSON-LD sub-query.
///
/// `apply_auth_identity_to_opts` injects `identity` and the server's
/// `default_policy_class` into each sub-query body's opts before
/// dispatch — same code path the single-query `/query` handler uses for
/// JSON-LD requests. The result is uniform identity behaviour across
/// the two endpoints.
///
/// **SPARQL note:** the current single-query connection-scoped SPARQL
/// path (i.e. when SPARQL declares its own `FROM <ledger>`) does not
/// thread identity either. v1 multi-query matches that for parity;
/// identity threading for connection-scoped SPARQL is a follow-up that
/// lands on both endpoints at once.
#[derive(Debug, Clone, Default)]
pub(crate) struct MultiQueryIdentityContext {
    /// Effective principal identity for this envelope. `None` when no
    /// authenticated principal is present (development mode).
    pub identity: Option<String>,
    /// Server-default policy class applied when the request doesn't
    /// override via `opts.policy-class` (JSON-LD).
    pub default_policy_class: Option<String>,
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
        // alias is allowed to consume the whole budget, but no more. This
        // keeps the worst case bounded without artificially partitioning
        // among aliases that might be cheap.
        let max_subquery_response_bytes = bounds.max_response_size_bytes;

        Self {
            max_concurrency,
            envelope_timeout_ms,
            max_subquery_response_bytes,
        }
    }
}

/// Dispatch every sub-query in the envelope in parallel under the
/// configured bounds, returning per-alias outcomes in submission order.
///
/// This function never returns an `Err`: any envelope-level failure
/// (snapshot resolution, malformed alias) belongs to the caller; per-alias
/// failures land in `AliasOutcomeKind::Error` or `::Timeout`.
pub(crate) async fn dispatch_multi_query(
    state: Arc<AppState>,
    envelope: MultiQueryRequest,
    snapshot: Arc<EnvelopeSnapshot>,
    config: DispatchConfig,
    identity_ctx: Arc<MultiQueryIdentityContext>,
) -> Vec<AliasOutcome> {
    let envelope_context = Arc::new(envelope.context.clone());
    let envelope_opts = Arc::new(envelope.opts.clone());

    let semaphore = Arc::new(Semaphore::new(config.max_concurrency));
    let deadline = Instant::now() + Duration::from_millis(config.envelope_timeout_ms);

    // Stable submission order: index → alias. The HashMap from JoinSet
    // results may arrive in any order; we re-sort at the end.
    let aliases: Vec<String> = envelope.queries.keys().cloned().collect();
    let mut set: JoinSet<(usize, AliasOutcomeKind)> = JoinSet::new();

    for (idx, (alias, sub)) in envelope.queries.into_iter().enumerate() {
        let state = Arc::clone(&state);
        let snapshot = Arc::clone(&snapshot);
        let envelope_context = Arc::clone(&envelope_context);
        let envelope_opts = Arc::clone(&envelope_opts);
        let semaphore = Arc::clone(&semaphore);
        let identity_ctx = Arc::clone(&identity_ctx);

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

        set.spawn(
            async move {
                // Acquire permit — this is where we may wait for the
                // concurrency cap to release.
                let _permit = match semaphore.acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => {
                        // Semaphore closed (impossible in practice — we
                        // hold a clone of the Arc). Surface as error.
                        return (
                            idx,
                            AliasOutcomeKind::Error {
                                code: "internal".into(),
                                message: "dispatcher semaphore closed".into(),
                            },
                        );
                    }
                };

                // Effective timeout = min(opts.timeoutMs, remaining envelope budget)
                // computed at permit-acquisition, not envelope-entry.
                let remaining = deadline.saturating_duration_since(Instant::now());
                let sub_timeout = sub_query_timeout_ms(&sub, envelope_opts.as_ref().as_ref());
                let effective_ms = sub_timeout
                    .map(|t| t.min(remaining.as_millis() as u64))
                    .unwrap_or(remaining.as_millis() as u64);
                let effective = Duration::from_millis(effective_ms);
                let span = tracing::Span::current();
                span.record("effective_timeout_ms", effective_ms);

                let exec = execute_subquery(
                    Arc::clone(&state),
                    sub,
                    envelope_context.as_ref().as_ref(),
                    envelope_opts.as_ref().as_ref(),
                    snapshot.as_ref(),
                    identity_ctx.as_ref(),
                );
                let kind = match tokio::time::timeout(effective, exec).await {
                    Ok(Ok(output)) => {
                        // Per-sub-query post-format size check. Each
                        // alias result is serialized once for sizing
                        // (a serialization the assembler would have
                        // done anyway); if a single sub-query result is
                        // already over the per-sub-query budget we mark
                        // it as an error and drop the data, so a
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
                    Ok(Err(server_err)) => AliasOutcomeKind::Error {
                        code: classify_error(&server_err),
                        message: server_err.to_string(),
                    },
                    Err(_) => AliasOutcomeKind::Timeout {
                        effective_timeout_ms: effective_ms,
                    },
                };

                match &kind {
                    AliasOutcomeKind::Success { .. } => span.record("result_status", "ok"),
                    AliasOutcomeKind::Error { .. } => span.record("result_status", "error"),
                    AliasOutcomeKind::Timeout { .. } => span.record("result_status", "timeout"),
                };

                (idx, kind)
            }
            .instrument(span),
        );
    }

    // Drain with a hard envelope-deadline guard. When the deadline fires,
    // any tasks still running get aborted and reported as Timeout.
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
                        // Task panicked or was cancelled. We can't map back
                        // to its alias here (join_next is unordered, the
                        // task didn't return its idx). Treat as an error
                        // bucket — placement happens after the loop.
                        tracing::error!(error = %join_err, "sub-query task join failed");
                    }
                    None => break,
                }
            }
            _ = &mut deadline_sleep => {
                set.abort_all();
                // Drain remaining tasks; aborted ones return JoinError, and
                // tasks that already completed return normally.
                while let Some(joined) = set.join_next().await {
                    if let Ok((idx, kind)) = joined {
                        if idx < results.len() {
                            results[idx] = Some(kind);
                        }
                    }
                }
                break;
            }
        }
    }

    // Any slot still None either timed out (deadline fired before it could
    // complete or before it acquired a permit) or panicked. Mark as Timeout
    // with the envelope budget since that's the closest information we
    // have.
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
/// applied), dispatch to the language-specific helper, and return the
/// raw output. Errors propagate to the caller as `ServerError` which the
/// dispatcher wraps into `AliasOutcomeKind::Error`.
async fn execute_subquery(
    state: Arc<AppState>,
    sub: MultiQuerySubquery,
    envelope_context: Option<&JsonValue>,
    envelope_opts: Option<&JsonValue>,
    snapshot: &EnvelopeSnapshot,
    identity_ctx: &MultiQueryIdentityContext,
) -> Result<SubqueryOutput, ServerError> {
    // Merge opts (envelope defaults, sub-query wins on key conflict).
    let merged_opts_val = merged_opts(envelope_opts, sub.opts.as_ref());
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

            // Identity / default-policy-class injection. We use the
            // sub-query's primary ledger (first entry in its `from`) as
            // the impersonation-check context — sub-queries that span
            // multiple ledgers fall back to the first as a conservative
            // default. Matches single-query `/query` behaviour where
            // ledger-id comes from a single source (path / header /
            // body) per request.
            let primary_ledger = primary_ledger_from_jsonld(&query_body);
            apply_auth_identity_to_opts(
                &state,
                primary_ledger.as_deref().unwrap_or(""),
                &mut query_body,
                identity_ctx.identity.as_deref(),
                identity_ctx.default_policy_class.as_deref(),
            )
            .await;

            apply_snapshot_to_jsonld(&mut query_body, snapshot);

            run_jsonld_subquery(&state, &query_body).await
        }
        SubqueryLanguage::Sparql => {
            let sparql = sub.query.as_str().unwrap_or_default();

            // SPARQL has no inner JSON-LD context — directives come from
            // the envelope context only.
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
            // SPARQL identity threading is a follow-up: the single-query
            // connection-scoped SPARQL path doesn't currently thread
            // identity either, so v1 multi-query matches that behavior
            // for parity. Documented in docs/api/multi-query.md as a v1
            // limitation; SPARQL identity threading via
            // QueryConnectionOptions will land alongside the same fix
            // on the single-query path.
            run_sparql_subquery(&state, &with_snapshot, tracking).await
        }
    }
}

/// Extract the first ledger identifier (with any temporal suffix
/// stripped) from a JSON-LD sub-query body's `from` field. Used as the
/// impersonation-check context for [`apply_auth_identity_to_opts`].
///
/// Sub-queries that span multiple ledgers fall back to the first entry —
/// the impersonation check is per-bearer, not per-ledger, and asserting
/// against the first ledger is conservative (a bearer that can
/// impersonate against one ledger in a multi-ledger sub-query is
/// trusted with that sub-query as a whole).
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
    // Strip a `@t:`/`@iso:`/`@commit:` suffix and any `#fragment` to
    // align with the snapshot map / impersonation-table keying.
    let bare = raw.split('#').next().unwrap_or(&raw);
    for marker in ["@t:", "@iso:", "@commit:"] {
        if let Some(idx) = bare.find(marker) {
            return Some(bare[..idx].to_string());
        }
    }
    Some(bare.to_string())
}

fn sub_query_timeout_ms(
    sub: &MultiQuerySubquery,
    envelope_opts: Option<&JsonValue>,
) -> Option<u64> {
    // Sub-query opts override envelope opts; sub-query opts.timeoutMs wins.
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

fn classify_error(err: &ServerError) -> String {
    // For v1 we report a coarse classification. Task #5 / #6 will refine
    // this when assembling the response body and HTTP status code.
    match err {
        ServerError::Api(_) => "api_error".into(),
        _ => "internal".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_api::query::multi::AsOf;
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
    fn dispatch_config_clamps_to_server_limits() {
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

    // The full async dispatcher path is exercised end-to-end by the
    // integration tests in task #6, where a real `AppState` is available.
    // Here we keep coverage to the pure helpers; that avoids standing up a
    // full server fixture for a unit test.
    fn _signatures_compile() {
        // Make sure AsOf is in scope for the public types referenced above.
        let _ = AsOf::T(0);
    }
}
