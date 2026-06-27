//! Tracking response headers
//!
//! When tracking is enabled (via meta/max-fuel options), the server returns
//! tracking metrics both in the response body AND as HTTP headers:
//!
//! - `x-fdb-time`: Execution time (e.g., "12.34ms")
//! - `x-fdb-fuel`: Fuel consumed (as string, e.g., "42")
//! - `x-fdb-policy`: Policy stats as base64-encoded JSON
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

    // Add reasoning header if a materialization ran. Plain JSON (not base64):
    // the payload is short, has no user data, and `capped` should be greppable
    // straight off the wire.
    if let Some(ref reasoning) = tally.reasoning {
        if let Ok(json) = serde_json::to_string(reasoning) {
            if let Ok(value) = HeaderValue::from_str(&json) {
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
