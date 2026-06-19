//! Multi-query envelope response assembly.
//!
//! Turns the dispatcher's `Vec<AliasOutcome>` into a wire-shape
//! [`MultiQueryResponse`], measuring serialized byte size as each result
//! is added so an oversized response surfaces as an envelope-level error
//! rather than blowing memory.
//!
//! Sizing is incremental and approximate: each per-alias successful
//! result is serialized once with `serde_json::to_vec`, its byte length
//! is added to a running total alongside a small fudge for envelope
//! overhead, and the assembler aborts with
//! [`ResponseAssemblyError::ResponseSizeExceeded`] once the running
//! total crosses [`MultiQueryBounds::max_response_size_bytes`].

use indexmap::IndexMap;
use serde_json::{Map as JsonMap, Value as JsonValue};

use super::dispatch::{AliasOutcome, AliasOutcomeKind};
use super::snapshot::EnvelopeSnapshot;
use crate::query::multi::{
    MultiQueryBounds, MultiQueryMeta, MultiQueryResponse, MultiQueryStatus, SnapshotInfo,
};
use crate::TrackingTally;

/// Envelope-level failure during response assembly. Maps to a 5xx HTTP
/// response when used by the server handler; per-alias failures stay
/// inside [`MultiQueryResponse::errors`] and do not surface here.
#[derive(Debug, thiserror::Error)]
pub enum ResponseAssemblyError {
    #[error(
        "assembled response would exceed response-size cap of {limit} bytes \
         (reached {actual} bytes at alias '{alias}')"
    )]
    ResponseSizeExceeded {
        alias: String,
        actual: usize,
        limit: usize,
    },
}

/// Assemble a [`MultiQueryResponse`] from the dispatcher's per-alias
/// outcomes, the resolved envelope snapshot, and the bounds.
///
/// Crate-internal — fed by `run_envelope` which the builder calls.
/// External callers reach the assembled response via the builder.
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
    // Per-alias tracking telemetry, preserved in submission order via
    // IndexMap so a client can correlate `tracking[alias]` against
    // `results[alias]` without re-sorting.
    let mut tracking: IndexMap<String, TrackingTally> = IndexMap::new();

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
                if let Some(t) = tally {
                    tracking.insert(alias.clone(), t);
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
        tracking,
        meta,
    })
}

/// Status corresponds to the per-alias success/failure counts.
/// Validation already rejected empty envelopes, so the `(0, 0)` case is
/// unreachable in production — we still map it to `AllFailed` defensively.
fn derive_status(success_count: usize, failure_count: usize) -> MultiQueryStatus {
    match (success_count, failure_count) {
        (0, _) => MultiQueryStatus::AllFailed,
        (_, 0) => MultiQueryStatus::Ok,
        _ => MultiQueryStatus::Partial,
    }
}

fn build_snapshot_info(snapshot: &EnvelopeSnapshot) -> SnapshotInfo {
    let mut ledgers = JsonMap::new();
    // Sort ledgers by name for deterministic response shape (helps
    // caching and trace diffing across requests).
    let mut sorted: Vec<(&String, &i64)> = snapshot.ledgers.iter().collect();
    sorted.sort_by_key(|&(k, _)| k);
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

/// Approximate serialized length of a JSON value without owning the
/// bytes. Uses `serde_json::to_vec` which is allocating but reliable.
fn approximate_serialized_len(value: &JsonValue) -> usize {
    serde_json::to_vec(value).map(|v| v.len()).unwrap_or(0)
}

/// Approximate bytes consumed by the outer envelope skeleton (status,
/// snapshot block keys, brackets, separators, etc.). Conservative so we
/// err on the side of cutting off slightly earlier than the strict cap.
const ENVELOPE_OVERHEAD_BYTES: usize = 256;

/// Approximate bytes per per-alias entry beyond the entry's own JSON
/// (quoted alias key, colon, comma). The alias's character length is
/// added separately at the call site so non-ASCII alias keys are
/// accounted for.
const PER_ENTRY_OVERHEAD_BYTES: usize = 6;

#[cfg(test)]
mod tests {
    use super::*;
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
        let resp =
            assemble_response(outcomes, &snap, &MultiQueryBounds::DEFAULT, false, 5).unwrap();
        assert_eq!(resp.status, MultiQueryStatus::Ok);
        assert_eq!(resp.results.len(), 2);
        assert!(resp.errors.is_empty());
    }

    #[test]
    fn all_failure_yields_all_failed_status() {
        let outcomes = vec![err("a", "api_error", "syntax"), timeout("b", 1_000)];
        let snap = snapshot_with(&[("ledgerA", 42)]);
        let resp =
            assemble_response(outcomes, &snap, &MultiQueryBounds::DEFAULT, false, 5).unwrap();
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
        let resp =
            assemble_response(outcomes, &snap, &MultiQueryBounds::DEFAULT, false, 5).unwrap();
        assert_eq!(resp.status, MultiQueryStatus::Partial);
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.errors.len(), 1);
    }

    #[test]
    fn snapshot_info_echoes_as_of_and_ledgers() {
        let outcomes = vec![success("a", serde_json::json!([]))];
        let snap = snapshot_with(&[("ledgerA", 42), ("ledgerB", 99)]);
        let resp =
            assemble_response(outcomes, &snap, &MultiQueryBounds::DEFAULT, false, 5).unwrap();
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
        let resp =
            assemble_response(outcomes, &snap, &MultiQueryBounds::DEFAULT, false, 5).unwrap();
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
                // pushes the running total over.
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
                    reasoning: None,
                }),
            },
        }];
        let snap = snapshot_with(&[("ledgerA", 42)]);
        let resp =
            assemble_response(outcomes, &snap, &MultiQueryBounds::DEFAULT, true, 10).unwrap();
        let meta = resp.meta.unwrap();
        assert_eq!(meta.elapsed_ms, Some(10));
        assert_eq!(meta.fuel_total, Some(123.0));
    }

    #[test]
    fn meta_omitted_when_not_requested() {
        let outcomes = vec![success("a", serde_json::json!([]))];
        let snap = snapshot_with(&[("ledgerA", 42)]);
        let resp =
            assemble_response(outcomes, &snap, &MultiQueryBounds::DEFAULT, false, 5).unwrap();
        assert!(resp.meta.is_none());
    }

    fn success_with_tally(alias: &str, data: JsonValue, tally: TrackingTally) -> AliasOutcome {
        AliasOutcome {
            alias: alias.to_string(),
            kind: AliasOutcomeKind::Success {
                data,
                tally: Some(tally),
            },
        }
    }

    #[test]
    fn per_alias_tracking_populated_when_sub_query_has_tally() {
        let outcomes = vec![
            success_with_tally(
                "alice",
                serde_json::json!([]),
                TrackingTally {
                    time: Some("5ms".into()),
                    fuel: Some(12.3),
                    policy: None,
                    reasoning: None,
                },
            ),
            success_with_tally(
                "brian",
                serde_json::json!([]),
                TrackingTally {
                    time: Some("3ms".into()),
                    fuel: Some(8.1),
                    policy: None,
                    reasoning: None,
                },
            ),
        ];
        let snap = snapshot_with(&[("ledgerA", 42)]);
        let resp =
            assemble_response(outcomes, &snap, &MultiQueryBounds::DEFAULT, true, 87).unwrap();
        assert_eq!(resp.tracking.len(), 2);
        let alice = &resp.tracking["alice"];
        assert_eq!(alice.time.as_deref(), Some("5ms"));
        assert_eq!(alice.fuel, Some(12.3));
        let brian = &resp.tracking["brian"];
        assert_eq!(brian.fuel, Some(8.1));
        // Envelope-level rollup still works alongside per-alias.
        let meta = resp.meta.unwrap();
        assert!(
            (meta.fuel_total.unwrap() - 20.4).abs() < 1e-9,
            "fuel_total should sum per-alias fuel"
        );
        assert_eq!(meta.elapsed_ms, Some(87));
    }

    #[test]
    fn per_alias_tracking_only_includes_aliases_that_tracked() {
        // 'alice' tracked, 'brian' did not. Tracking map has only alice.
        let outcomes = vec![
            success_with_tally(
                "alice",
                serde_json::json!([]),
                TrackingTally {
                    time: Some("5ms".into()),
                    fuel: Some(10.0),
                    policy: None,
                    reasoning: None,
                },
            ),
            success("brian", serde_json::json!([])),
        ];
        let snap = snapshot_with(&[("ledgerA", 42)]);
        let resp =
            assemble_response(outcomes, &snap, &MultiQueryBounds::DEFAULT, false, 5).unwrap();
        assert_eq!(resp.tracking.len(), 1);
        assert!(resp.tracking.contains_key("alice"));
        assert!(!resp.tracking.contains_key("brian"));
    }

    #[test]
    fn per_alias_tracking_preserves_submission_order() {
        let outcomes = vec![
            success_with_tally(
                "zulu",
                serde_json::json!([]),
                TrackingTally {
                    time: None,
                    fuel: Some(1.0),
                    policy: None,
                    reasoning: None,
                },
            ),
            success_with_tally(
                "alpha",
                serde_json::json!([]),
                TrackingTally {
                    time: None,
                    fuel: Some(2.0),
                    policy: None,
                    reasoning: None,
                },
            ),
        ];
        let snap = snapshot_with(&[("ledgerA", 42)]);
        let resp =
            assemble_response(outcomes, &snap, &MultiQueryBounds::DEFAULT, false, 5).unwrap();
        let keys: Vec<&String> = resp.tracking.keys().collect();
        assert_eq!(keys, vec!["zulu", "alpha"]);
    }

    #[test]
    fn per_alias_tracking_empty_when_no_sub_query_tracked() {
        let outcomes = vec![
            success("a", serde_json::json!([])),
            success("b", serde_json::json!([])),
        ];
        let snap = snapshot_with(&[("ledgerA", 42)]);
        let resp =
            assemble_response(outcomes, &snap, &MultiQueryBounds::DEFAULT, false, 5).unwrap();
        assert!(resp.tracking.is_empty());
    }
}
