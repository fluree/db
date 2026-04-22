//! Shared helpers for applying bearer-authenticated identity and policy class
//! to incoming requests, with support for **root-identity impersonation**.
//!
//! # Impersonation semantic
//!
//! A bearer-authenticated identity that has **no** `f:policyClass` assignments
//! on the target ledger (i.e. it is unrestricted) may delegate to a body- or
//! header-supplied target identity for policy-testing purposes. The check is
//! performed via [`fluree_db_api::identity_has_no_policies`], which returns
//! true only for the `FoundNoPolicies` case — unknown identities (`NotFound`)
//! and restricted identities (`FoundWithPolicies`) both fall through to the
//! non-spoofable "force bearer identity" path.
//!
//! # When impersonation is allowed
//!
//! - Direct (non-proxy) storage mode only — proxies forward the request
//!   upstream; the upstream performs its own check.
//! - The request body / headers explicitly request a target via
//!   `opts.identity`, `opts.policy-class`, `opts.policy`, or
//!   `opts.policy-values`. (`opts.default-allow` is intentionally excluded —
//!   it only governs the absence of matching rules and never widens the
//!   bearer's resolved policies.)
//! - The bearer identity resolves to `FoundNoPolicies` on the target ledger.
//!
//! When all three hold, the server logs an `info`-level audit line and leaves
//! the body/header-supplied target in place. Otherwise the bearer identity is
//! forced into opts (current non-impersonation behavior).

use crate::state::AppState;
use serde_json::Value as JsonValue;

/// Returns `true` if the request body explicitly redirects policy evaluation
/// to a different identity or policy set than the bearer's own.
///
/// `default-allow` is intentionally excluded: it only governs the absence of
/// matching rules and never widens the bearer's resolved policies, so a
/// request that sets only `default-allow` does not require the impersonation
/// gate (and shouldn't pay for the ledger lookup).
pub(crate) fn body_requests_impersonation(query: &JsonValue) -> bool {
    let Some(opts) = query.get("opts") else {
        return false;
    };
    opts.get("identity").is_some()
        || opts.get("policy-class").is_some()
        || opts.get("policy_class").is_some()
        || opts.get("policyClass").is_some()
        || opts.get("policy").is_some()
        || opts.get("policy-values").is_some()
        || opts.get("policy_values").is_some()
        || opts.get("policyValues").is_some()
}

/// Force the bearer's `identity` and `policy-class` into `opts`, overriding any
/// client-supplied values. This is the non-impersonation path.
fn force_auth_opts(query: &mut JsonValue, identity: Option<&str>, policy_class: Option<&str>) {
    let Some(obj) = query.as_object_mut() else {
        return;
    };
    let opts = obj
        .entry("opts")
        .or_insert_with(|| JsonValue::Object(serde_json::Map::new()));
    let Some(opts_obj) = opts.as_object_mut() else {
        return;
    };

    if let Some(id) = identity {
        opts_obj.insert("identity".to_string(), JsonValue::String(id.to_string()));
    }
    if let Some(pc) = policy_class {
        opts_obj.insert(
            "policy-class".to_string(),
            JsonValue::String(pc.to_string()),
        );
    }
}

/// Check whether `bearer_identity` is allowed to impersonate on `ledger_id`.
///
/// Returns `false` in proxy mode, on lookup errors, and for any identity lookup
/// result other than `FoundNoPolicies`.
async fn bearer_can_impersonate(state: &AppState, ledger_id: &str, bearer_identity: &str) -> bool {
    if state.config.is_proxy_storage_mode() {
        return false;
    }
    let fluree = &state.fluree;
    let Ok(ledger) = fluree.ledger(ledger_id).await else {
        return false;
    };
    fluree_db_api::identity_has_no_policies(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        ledger.t(),
        bearer_identity,
    )
    .await
    .unwrap_or(false)
}

/// Apply the bearer-authenticated identity to JSON-LD `opts`, honoring the
/// root-identity impersonation semantic described at module-level.
///
/// Use this in place of the legacy `force_query_auth_opts` function on all
/// JSON-LD query, explain, and transaction routes.
pub(crate) async fn apply_auth_identity_to_opts(
    state: &AppState,
    ledger_id: &str,
    query: &mut JsonValue,
    bearer_identity: Option<&str>,
    default_policy_class: Option<&str>,
) {
    // No authenticated identity → nothing to force; body opts flow through.
    let Some(bearer_id) = bearer_identity else {
        return;
    };

    // If the body isn't trying to impersonate, fast path: force bearer identity
    // and server-default policy-class.
    if !body_requests_impersonation(query) {
        force_auth_opts(query, Some(bearer_id), default_policy_class);
        return;
    }

    if bearer_can_impersonate(state, ledger_id, bearer_id).await {
        let target = query
            .pointer("/opts/identity")
            .and_then(|v| v.as_str())
            .unwrap_or("<unspecified>");
        tracing::info!(
            bearer = %bearer_id,
            target = %target,
            ledger = %ledger_id,
            "policy impersonation: bearer delegating to target identity"
        );
        // Respect body/header-supplied opts in full. Do not layer the
        // server-default policy-class on top — an unrestricted bearer that's
        // explicitly testing policies should get exactly the policies they
        // asked for.
    } else {
        force_auth_opts(query, Some(bearer_id), default_policy_class);
    }
}

/// Resolve the effective identity for a **SPARQL** request.
///
/// Uses the bearer identity unless (1) the bearer can impersonate and (2) a
/// `fluree-identity` header is supplied, in which case the header value wins.
/// Returns `None` if no identity is available.
pub(crate) async fn resolve_sparql_identity(
    state: &AppState,
    ledger_id: &str,
    bearer_identity: Option<&str>,
    header_identity: Option<&str>,
) -> Option<String> {
    match (bearer_identity, header_identity) {
        (Some(bearer), Some(header_id)) => {
            if bearer_can_impersonate(state, ledger_id, bearer).await {
                tracing::info!(
                    bearer = %bearer,
                    target = %header_id,
                    ledger = %ledger_id,
                    "policy impersonation: bearer delegating to target identity (SPARQL)"
                );
                Some(header_id.to_string())
            } else {
                Some(bearer.to_string())
            }
        }
        (Some(bearer), None) => Some(bearer.to_string()),
        // No bearer: if a header is set and we're unauthenticated (DataAuthMode::None),
        // honor it — this matches the legacy root-no-identity behavior.
        (None, Some(header_id)) => Some(header_id.to_string()),
        (None, None) => None,
    }
}
