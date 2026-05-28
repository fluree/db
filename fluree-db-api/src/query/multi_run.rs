//! Per-sub-query execution helpers used by the multi-query dispatcher.
//!
//! Each helper takes a pre-merged sub-query body (envelope `@context` /
//! `opts` already folded in, snapshot already applied) and dispatches it
//! through the connection-scoped query path
//! ([`Fluree::query_from`](crate::Fluree::query_from)) which both
//! single-query and multi-query share.
//!
//! These helpers are also useful directly — e.g., a custom dispatcher or
//! a test fixture can run one sub-query at a time without involving the
//! envelope dispatcher.

use crate::{ApiError, Fluree, Result, TrackingOptions, TrackingTally};
use serde_json::Value as JsonValue;

/// Output of a single sub-query execution.
///
/// The caller is responsible for assembling either a transport response
/// (HTTP) or a per-alias entry inside a multi-query envelope response
/// (in-process / dispatcher).
#[derive(Debug)]
pub struct SubqueryOutput {
    /// JSON-formatted query result. For JSON-LD: the formatted query
    /// JSON. For SPARQL: SPARQL 1.1 Results JSON (or whichever format
    /// the connection builder produced).
    pub data: JsonValue,
    /// Tracking telemetry. Populated when `opts.meta` (JSON-LD) or the
    /// caller-supplied tracking options (SPARQL) requested tracking;
    /// `None` otherwise.
    pub tally: Option<TrackingTally>,
}

/// Execute a JSON-LD sub-query through the connection (`query_from()`)
/// path.
///
/// The caller is expected to:
///
/// - Have already merged envelope-level `@context` and `opts` into
///   `query_json` (multi-query dispatcher path) or to have processed
///   headers / policy into `opts` (single-query HTTP handler path).
/// - Have applied any envelope-level snapshot pin to the sub-query's
///   `from` (multi-query dispatcher path).
/// - Be inside whichever tracing span the caller chose to attribute this
///   execution to — this function does not create its own span.
///
/// Tracking is enabled implicitly when the query body's `opts` carry a
/// recognised tracking trigger (matches the single-query convention).
pub async fn run_jsonld_subquery(
    fluree: &Fluree,
    query_json: &JsonValue,
) -> Result<SubqueryOutput> {
    if has_tracking_opts(query_json) {
        let response = fluree
            .query_from()
            .jsonld(query_json)
            .execute_tracked()
            .await
            .map_err(|e| ApiError::http(e.status, e.error))?;
        let tally = TrackingTally {
            time: response.time.clone(),
            fuel: response.fuel,
            policy: response.policy.clone(),
        };
        Ok(SubqueryOutput {
            data: response.result,
            tally: Some(tally),
        })
    } else {
        let data = fluree
            .query_from()
            .jsonld(query_json)
            .execute_formatted()
            .await?;
        Ok(SubqueryOutput { data, tally: None })
    }
}

/// Execute a SPARQL sub-query through the connection (`query_from()`)
/// path.
///
/// The SPARQL string carries its own `FROM <ledger>` dataset clause; the
/// connection builder routes through nameservice / snapshot loading
/// without extra wiring.
///
/// `tracking` accepts the full [`TrackingOptions`] surface (selective
/// `meta` flags, `max_fuel`). `None` runs the non-tracked builder path;
/// `Some(opts)` runs the tracked path with those options applied.
pub async fn run_sparql_subquery(
    fluree: &Fluree,
    sparql: &str,
    tracking: Option<TrackingOptions>,
) -> Result<SubqueryOutput> {
    if let Some(opts) = tracking {
        let response = fluree
            .query_from()
            .sparql(sparql)
            .tracking(opts)
            .execute_tracked()
            .await
            .map_err(|e| ApiError::http(e.status, e.error))?;
        let tally = TrackingTally {
            time: response.time.clone(),
            fuel: response.fuel,
            policy: response.policy.clone(),
        };
        Ok(SubqueryOutput {
            data: response.result,
            tally: Some(tally),
        })
    } else {
        let data = fluree
            .query_from()
            .sparql(sparql)
            .execute_formatted()
            .await?;
        Ok(SubqueryOutput { data, tally: None })
    }
}

/// Does this JSON-LD query body request tracking via `opts.meta` or
/// `opts.max-fuel` (in any spelling)?
///
/// Mirrors the same shapes the single-query HTTP handler recognises so a
/// caller can hand-roll an `opts` block without learning a new vocab.
fn has_tracking_opts(query_json: &JsonValue) -> bool {
    let Some(opts) = query_json.get("opts") else {
        return false;
    };
    if let Some(meta) = opts.get("meta") {
        match meta {
            JsonValue::Bool(true) => return true,
            JsonValue::Object(obj) if !obj.is_empty() => return true,
            _ => {}
        }
    }
    opts.get("max-fuel").is_some()
        || opts.get("max_fuel").is_some()
        || opts.get("maxFuel").is_some()
}
