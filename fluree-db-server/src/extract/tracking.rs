//! Tracking response headers
//!
//! When tracking is enabled (via meta/max-fuel options), the server returns
//! tracking metrics both in the response body AND as HTTP headers:
//!
//! - `x-fdb-time`: Execution time (e.g., "12.34ms")
//! - `x-fdb-fuel`: Fuel consumed (as string, e.g., "42")
//! - `x-fdb-policy`: Policy stats as base64-encoded JSON
//! - `x-fdb-policy-enforcement`: Whether policy governed the request, as JSON
//!   (only when it did); absent means the request ran unenforced
//! - `x-fdb-reasoning`: OWL2-RL materialization outcome as JSON (only when a
//!   reasoning mode ran); `"capped": true` means results may be incomplete

use axum::http::{HeaderMap, HeaderName, HeaderValue};
use base64::{engine::general_purpose::STANDARD, Engine};
use fluree_db_api::TrackingTally;

/// Header name for execution time
pub const X_FDB_TIME: &str = "x-fdb-time";
/// Header name for fuel consumed
pub const X_FDB_FUEL: &str = "x-fdb-fuel";
/// Header name for policy stats (base64-encoded JSON)
pub const X_FDB_POLICY: &str = "x-fdb-policy";
/// Header name for the policy-enforcement state (JSON)
pub const X_FDB_POLICY_ENFORCEMENT: &str = "x-fdb-policy-enforcement";
/// Header name for the reasoning materialization outcome (JSON)
pub const X_FDB_REASONING: &str = "x-fdb-reasoning";

/// Build tracking headers from a TrackingTally
///
/// Returns a HeaderMap with tracking headers set based on what's present in the tally.
/// This matches the legacy `with-tracking-headers` behavior.
pub fn tracking_headers(tally: &TrackingTally) -> HeaderMap {
    let mut headers = HeaderMap::new();

    // Add time header if present
    if let Some(ref time) = tally.time {
        if let Ok(value) = HeaderValue::from_str(time) {
            headers.insert(HeaderName::from_static(X_FDB_TIME), value);
        }
    }

    // Add fuel header if present. Format decimal fuel to up to 3 places without
    // trailing zeros (e.g. `1.234`, `1`, `0.5`).
    if let Some(fuel) = tally.fuel {
        let formatted = format_fuel(fuel);
        if let Ok(value) = HeaderValue::from_str(&formatted) {
            headers.insert(HeaderName::from_static(X_FDB_FUEL), value);
        }
    }

    // Add policy header if present (base64-encoded JSON)
    if let Some(ref policy) = tally.policy {
        if let Ok(json) = serde_json::to_string(policy) {
            let encoded = STANDARD.encode(json.as_bytes());
            if let Ok(value) = HeaderValue::from_str(&encoded) {
                headers.insert(HeaderName::from_static(X_FDB_POLICY), value);
            }
        }
    }

    // Add the policy-enforcement header when policy governed the request.
    // Plain JSON, like reasoning: two booleans, no user data, and the state a
    // caller reading CSV/TSV (which has no body to carry a tally) needs most.
    if let Some(ref enforcement) = tally.policy_enforcement {
        if let Ok(json) = serde_json::to_string(enforcement) {
            if let Ok(value) = HeaderValue::from_str(&json) {
                headers.insert(HeaderName::from_static(X_FDB_POLICY_ENFORCEMENT), value);
            }
        }
    }

    // Add reasoning header if a materialization ran. Plain JSON (not base64):
    // the payload is short, and `capped` should be greppable straight off the
    // wire. `rules_fired` and `rules` stay in the body: they name every rule,
    // which for a large program would grow the header past what proxies
    // accept.
    if let Some(ref reasoning) = tally.reasoning {
        if let Ok(mut json) = serde_json::to_value(reasoning) {
            if let Some(fields) = json.as_object_mut() {
                fields.remove("rules_fired");
                fields.remove("rules");
            }
            if let Ok(value) = HeaderValue::from_str(&json.to_string()) {
                headers.insert(HeaderName::from_static(X_FDB_REASONING), value);
            }
        }
    }

    headers
}

fn format_fuel(fuel: f64) -> String {
    let s = format!("{fuel:.3}");
    let trimmed = s.trim_end_matches('0').trim_end_matches('.');
    if trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_api::{ReasoningDataTally, ReasoningTally, RulesSourceTally};

    /// The reasoning header says what ran (the rule set and its source) and
    /// over which data, but leaves the per-rule lists, which grow with the
    /// program, to the body.
    #[test]
    fn the_reasoning_header_names_the_rule_set_without_per_rule_counts() {
        let tally = TrackingTally {
            time: None,
            fuel: None,
            policy: None,
            policy_enforcement: None,
            reasoning: Some(Box::new(ReasoningTally {
                capped: false,
                capped_reason: None,
                derived_facts: 2,
                iterations: 1,
                duration_ms: 3,
                capped_rules: Vec::new(),
                pending_rules: Vec::new(),
                evaluation_time: None,
                rules_fired: std::collections::BTreeMap::from([(
                    "http://example.org/r1".to_string(),
                    2,
                )]),
                rule_set: Some("00000000000000ab".to_string()),
                rules: vec![fluree_db_api::RuleTally {
                    id: "http://example.org/r1".to_string(),
                    hash: "00000000000000cd".to_string(),
                }],
                rules_source: Some(RulesSourceTally {
                    ledger: "rules:main".to_string(),
                    graph: "http://example.org/rules".to_string(),
                    t: 12,
                    commit_id: None,
                }),
                data: Some(ReasoningDataTally {
                    ledger: "data:main".to_string(),
                    t: 5,
                    commit_id: Some("bafy-data".to_string()),
                }),
            })),
            sql: None,
            sql_elided: None,
        };
        let headers = tracking_headers(&tally);
        let header: serde_json::Value = serde_json::from_str(
            headers
                .get(X_FDB_REASONING)
                .expect("reasoning header")
                .to_str()
                .expect("ascii"),
        )
        .expect("json");
        assert_eq!(header["rule_set"], "00000000000000ab");
        assert_eq!(header["rules_source"]["ledger"], "rules:main");
        assert_eq!(header["rules_source"]["t"], 12);
        // The data block is three short values: it rides the header.
        assert_eq!(header["data"]["ledger"], "data:main");
        assert_eq!(header["data"]["t"], 5);
        assert_eq!(header["data"]["commit_id"], "bafy-data");
        assert!(header.get("rules_fired").is_none(), "{header}");
        assert!(header.get("rules").is_none(), "{header}");
    }
}
