//! Multi-query envelope response assembly.
//!
//! Turns the dispatcher's `Vec<AliasOutcome>` into a wire-shape
//! [`MultiQueryResponse`], measuring serialized byte size as each result is
//! added so an oversized response surfaces as an envelope-level error rather
//! than blowing memory.
//!
//! Sizing is incremental and approximate: each per-alias successful result
//! is serialized once with `serde_json::to_vec`, its byte length is added to
//! a running total alongside a small fudge for envelope overhead, and the
//! assembler aborts with [`ResponseAssemblyError::ResponseSizeExceeded`]
//! once the running total crosses
//! [`MultiQueryBounds::max_response_size_bytes`]. Building a single huge
//! `JsonValue` and measuring after would defeat the purpose.

use fluree_db_api::query::multi::{
    MultiQueryBounds, MultiQueryMeta, MultiQueryResponse, MultiQueryStatus, SnapshotInfo,
};
use fluree_db_api::query::multi_snapshot::EnvelopeSnapshot;
use fluree_db_api::TrackingTally;
use serde_json::{Map as JsonMap, Value as JsonValue};

use super::multi_dispatch::{AliasOutcome, AliasOutcomeKind};

/// Envelope-level failure during response assembly. Maps to a 5xx response;
/// per-alias failures stay inside [`MultiQueryResponse::errors`] and do not
/// surface here.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ResponseAssemblyError {
    #[error(
        "assembled response would exceed server response-size cap of {limit} bytes \
         (reached {actual} bytes at alias '{alias}')"
    )]
    ResponseSizeExceeded {
        alias: String,
        actual: usize,
        limit: usize,
    },
}

/// Assemble a [`MultiQueryResponse`] from the dispatcher's per-alias
/// outcomes, the resolved envelope snapshot, and the server's response-size
/// cap.
///
/// Returns:
/// - `Ok(response, status_is_partial)` on success. The boolean exists for
///   HTTP-layer signaling but is also encoded in `response.status` so the
///   caller can pick whichever it prefers.
/// - `Err(ResponseSizeExceeded)` when the running serialized total crosses
///   `bounds.max_response_size_bytes`. The error names the alias whose
///   addition pushed the total over.
///
/// `include_meta` controls whether the response carries an aggregate
/// [`MultiQueryMeta`] block; the caller derives this from envelope opts
/// (`opts.meta` enabled at the envelope level).
pub(crate) fn assemble_response(
    outcomes: Vec<AliasOutcome>,
    snapshot: &EnvelopeSnapshot,
    bounds: &MultiQueryBounds,
    include_meta: bool,
    envelope_elapsed_ms: u64,
) -> Result<MultiQueryResponse, ResponseAssemblyError> {
    let snapshot_info = build_snapshot_info(snapshot);
    let mut results: JsonMap<String, JsonValue> = JsonMap::new();
    let mut errors: JsonMap<String, JsonValue> = JsonMap::new();

    let mut byte_total: usize = ENVELOPE_OVERHEAD_BYTES;
    let mut success_count: usize = 0;
    let mut failure_count: usize = 0;
    let mut fuel_total: Option<f64> = None;

    for outcome in outcomes {
        let alias = outcome.alias;
        match outcome.kind {
            AliasOutcomeKind::Success { data, tally } => {
                success_count += 1;
                if include_meta {
                    accumulate_fuel(&mut fuel_total, tally.as_ref());
                }

                let entry = data;
                let entry_bytes = approximate_serialized_len(&entry);
                let per_alias_overhead = PER_ENTRY_OVERHEAD_BYTES + alias.len();
                byte_total = byte_total.saturating_add(entry_bytes + per_alias_overhead);
                if byte_total > bounds.max_response_size_bytes {
                    return Err(ResponseAssemblyError::ResponseSizeExceeded {
                        alias,
                        actual: byte_total,
                        limit: bounds.max_response_size_bytes,
                    });
                }
                results.insert(alias, entry);
            }
            AliasOutcomeKind::Error { code, message } => {
                failure_count += 1;
                let entry = error_entry(&code, &message);
                let entry_bytes = approximate_serialized_len(&entry);
                let per_alias_overhead = PER_ENTRY_OVERHEAD_BYTES + alias.len();
                byte_total = byte_total.saturating_add(entry_bytes + per_alias_overhead);
                if byte_total > bounds.max_response_size_bytes {
                    return Err(ResponseAssemblyError::ResponseSizeExceeded {
                        alias,
                        actual: byte_total,
                        limit: bounds.max_response_size_bytes,
                    });
                }
                errors.insert(alias, entry);
            }
            AliasOutcomeKind::Timeout {
                effective_timeout_ms,
            } => {
                failure_count += 1;
                let entry = timeout_entry(effective_timeout_ms);
                let entry_bytes = approximate_serialized_len(&entry);
                let per_alias_overhead = PER_ENTRY_OVERHEAD_BYTES + alias.len();
                byte_total = byte_total.saturating_add(entry_bytes + per_alias_overhead);
                if byte_total > bounds.max_response_size_bytes {
                    return Err(ResponseAssemblyError::ResponseSizeExceeded {
                        alias,
                        actual: byte_total,
                        limit: bounds.max_response_size_bytes,
                    });
                }
                errors.insert(alias, entry);
            }
        }
    }

    let status = derive_status(success_count, failure_count);
    let meta = if include_meta {
        Some(MultiQueryMeta {
            fuel_total,
            elapsed_ms: Some(envelope_elapsed_ms),
        })
    } else {
        None
    };

    Ok(MultiQueryResponse {
        status,
        snapshot: snapshot_info,
        results,
        errors,
        meta,
    })
}

/// Status corresponds to the per-alias success/failure counts. Validation
/// already rejected empty envelopes, so the `(0, 0)` case is unreachable in
/// production — we still map it to `AllFailed` defensively.
fn derive_status(success_count: usize, failure_count: usize) -> MultiQueryStatus {
    match (success_count, failure_count) {
        (0, _) => MultiQueryStatus::AllFailed,
        (_, 0) => MultiQueryStatus::Ok,
        _ => MultiQueryStatus::Partial,
    }
}

fn build_snapshot_info(snapshot: &EnvelopeSnapshot) -> SnapshotInfo {
    let mut ledgers = JsonMap::new();
    // Sort ledgers by name for deterministic response shape (helps caching
    // and trace diffing across requests).
    let mut sorted: Vec<(&String, &i64)> = snapshot.ledgers.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(b.0));
    for (ledger, t) in sorted {
        ledgers.insert(ledger.clone(), JsonValue::Number((*t).into()));
    }
    SnapshotInfo {
        as_of: snapshot.as_of.clone(),
        ledgers,
    }
}

fn error_entry(code: &str, message: &str) -> JsonValue {
    let mut obj = JsonMap::new();
    obj.insert("code".into(), JsonValue::String(code.to_string()));
    obj.insert("message".into(), JsonValue::String(message.to_string()));
    JsonValue::Object(obj)
}

fn timeout_entry(effective_timeout_ms: u64) -> JsonValue {
    let mut obj = JsonMap::new();
    obj.insert("code".into(), JsonValue::String("timeout".into()));
    obj.insert(
        "message".into(),
        JsonValue::String(format!(
            "sub-query exceeded effective deadline of {effective_timeout_ms}ms"
        )),
    );
    obj.insert(
        "effective_timeout_ms".into(),
        JsonValue::Number(effective_timeout_ms.into()),
    );
    JsonValue::Object(obj)
}

fn accumulate_fuel(total: &mut Option<f64>, tally: Option<&TrackingTally>) {
    let Some(t) = tally.and_then(|t| t.fuel) else {
        return;
    };
    *total = Some(total.unwrap_or(0.0) + t);
}

/// Approximate serialized length of a JSON value without owning the bytes.
///
/// Uses `serde_json::to_vec` which is allocating but reliable. For v1 this
/// is acceptable — successful results are typically the dominant
/// contributor and we'd serialize them anyway when sending the response.
fn approximate_serialized_len(value: &JsonValue) -> usize {
    serde_json::to_vec(value).map(|v| v.len()).unwrap_or(0)
}

/// Approximate bytes consumed by the outer envelope skeleton (status,
/// snapshot block keys, brackets, separators, etc.). Conservative so we err
/// on the side of cutting off slightly earlier than the strict cap.
const ENVELOPE_OVERHEAD_BYTES: usize = 256;

/// Approximate bytes per per-alias entry beyond the entry's own JSON
/// (quoted alias key, colon, comma). The alias's character length is added
/// separately at the call site so non-ASCII alias keys are accounted for.
const PER_ENTRY_OVERHEAD_BYTES: usize = 6;

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_api::query::multi::MultiQueryBounds;
    use std::collections::HashMap;

    fn snapshot_with(pairs: &[(&str, i64)]) -> EnvelopeSnapshot {
        let mut ledgers = HashMap::new();
        for (k, v) in pairs {
            ledgers.insert((*k).to_string(), *v);
        }
        EnvelopeSnapshot {
            as_of: Some("2024-01-01T00:00:00.000Z".into()),
            ledgers,
        }
    }

    fn success(alias: &str, data: JsonValue) -> AliasOutcome {
        AliasOutcome {
            alias: alias.to_string(),
            kind: AliasOutcomeKind::Success { data, tally: None },
        }
    }

    fn err(alias: &str, code: &str, message: &str) -> AliasOutcome {
        AliasOutcome {
            alias: alias.to_string(),
            kind: AliasOutcomeKind::Error {
                code: code.to_string(),
                message: message.to_string(),
            },
        }
    }

    fn timeout(alias: &str, effective_timeout_ms: u64) -> AliasOutcome {
        AliasOutcome {
            alias: alias.to_string(),
            kind: AliasOutcomeKind::Timeout {
                effective_timeout_ms,
            },
        }
    }

    #[test]
    fn all_success_yields_ok_status() {
        let outcomes = vec![
            success("a", serde_json::json!([{"x": 1}])),
            success("b", serde_json::json!([{"x": 2}])),
        ];
        let snap = snapshot_with(&[("ledgerA", 42)]);
        let resp = assemble_response(outcomes, &snap, &MultiQueryBounds::DEFAULT, false, 5)
            .unwrap();
        assert_eq!(resp.status, MultiQueryStatus::Ok);
        assert_eq!(resp.results.len(), 2);
        assert!(resp.errors.is_empty());
    }

    #[test]
    fn all_failure_yields_all_failed_status() {
        let outcomes = vec![
            err("a", "api_error", "syntax"),
            timeout("b", 1_000),
        ];
        let snap = snapshot_with(&[("ledgerA", 42)]);
        let resp = assemble_response(outcomes, &snap, &MultiQueryBounds::DEFAULT, false, 5)
            .unwrap();
        assert_eq!(resp.status, MultiQueryStatus::AllFailed);
        assert!(resp.results.is_empty());
        assert_eq!(resp.errors.len(), 2);
        assert_eq!(resp.errors["b"]["effective_timeout_ms"], 1_000);
    }

    #[test]
    fn mixed_yields_partial_status() {
        let outcomes = vec![
            success("a", serde_json::json!([{"x": 1}])),
            err("b", "api_error", "boom"),
        ];
        let snap = snapshot_with(&[("ledgerA", 42)]);
        let resp = assemble_response(outcomes, &snap, &MultiQueryBounds::DEFAULT, false, 5)
            .unwrap();
        assert_eq!(resp.status, MultiQueryStatus::Partial);
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.errors.len(), 1);
    }

    #[test]
    fn snapshot_info_echoes_as_of_and_ledgers() {
        let outcomes = vec![success("a", serde_json::json!([]))];
        let snap = snapshot_with(&[("ledgerA", 42), ("ledgerB", 99)]);
        let resp = assemble_response(outcomes, &snap, &MultiQueryBounds::DEFAULT, false, 5)
            .unwrap();
        assert_eq!(
            resp.snapshot.as_of.as_deref(),
            Some("2024-01-01T00:00:00.000Z")
        );
        assert_eq!(resp.snapshot.ledgers["ledgerA"], 42);
        assert_eq!(resp.snapshot.ledgers["ledgerB"], 99);
    }

    #[test]
    fn snapshot_ledgers_are_sorted_for_determinism() {
        let outcomes = vec![success("a", serde_json::json!([]))];
        let snap = snapshot_with(&[("ledgerC", 3), ("ledgerA", 1), ("ledgerB", 2)]);
        let resp = assemble_response(outcomes, &snap, &MultiQueryBounds::DEFAULT, false, 5)
            .unwrap();
        let keys: Vec<&String> = resp.snapshot.ledgers.keys().collect();
        assert_eq!(keys, vec!["ledgerA", "ledgerB", "ledgerC"]);
    }

    #[test]
    fn response_size_cap_aborts_with_alias_attribution() {
        let big = serde_json::Value::String("x".repeat(2_000));
        let outcomes = vec![
            success("a", big.clone()),
            success("b", big.clone()),
            success("c", big),
        ];
        let snap = snapshot_with(&[("ledgerA", 42)]);
        let bounds = MultiQueryBounds {
            max_response_size_bytes: 2_500,
            ..MultiQueryBounds::DEFAULT
        };
        let err = assemble_response(outcomes, &snap, &bounds, false, 5).unwrap_err();
        match err {
            ResponseAssemblyError::ResponseSizeExceeded {
                alias,
                actual,
                limit,
            } => {
                assert_eq!(limit, 2_500);
                assert!(actual > 2_500);
                // The first entry (a) fits under the cap; the second (b)
                // pushes the running total over. Attribution names the
                // offending alias, not the first oversized one in absolute
                // terms.
                assert_eq!(alias, "b");
            }
        }
    }

    #[test]
    fn meta_included_when_requested_with_envelope_elapsed() {
        let outcomes = vec![AliasOutcome {
            alias: "a".into(),
            kind: AliasOutcomeKind::Success {
                data: serde_json::json!([]),
                tally: Some(TrackingTally {
                    time: Some("3ms".into()),
                    fuel: Some(123.0),
                    policy: None,
                }),
            },
        }];
        let snap = snapshot_with(&[("ledgerA", 42)]);
        let resp = assemble_response(outcomes, &snap, &MultiQueryBounds::DEFAULT, true, 10)
            .unwrap();
        let meta = resp.meta.unwrap();
        assert_eq!(meta.elapsed_ms, Some(10));
        assert_eq!(meta.fuel_total, Some(123.0));
    }

    #[test]
    fn meta_omitted_when_not_requested() {
        let outcomes = vec![success("a", serde_json::json!([]))];
        let snap = snapshot_with(&[("ledgerA", 42)]);
        let resp = assemble_response(outcomes, &snap, &MultiQueryBounds::DEFAULT, false, 5)
            .unwrap();
        assert!(resp.meta.is_none());
    }
}
