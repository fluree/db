//! Bind caller policy options to authority established by credential verification.
//!
//! Policy-free identities do not acquire delegation privileges. Embedded hosts
//! construct `PolicyAuthorization` directly; network hosts use the signed
//! `fluree.policy` fixed selection or request-selection capability
//! from a configured policy authority. Normal
//! bearer and signed-request identities use server-selected / ledger policies.

use crate::error::Result;
use crate::extract::{CredentialPolicy, DataPrincipal, FlureeHeaders};
use crate::state::AppState;
use fluree_db_api::{GovernanceOptions, PolicyAuthorization};
use serde_json::Value;

/// Bind verified authority before header defaults are merged into a query or write.
/// `signed_identity` must come from a verified signed request, never caller headers.
/// Signed request credentials take precedence over a bearer token, and cannot
/// inherit that token's delegated authority. Anonymous private-server requests
/// retain the direct SDK-style policy option contract.
pub(crate) fn bind_authorization(
    state: &AppState,
    mut headers: FlureeHeaders,
    principal: Option<&DataPrincipal>,
    signed_identity: Option<&str>,
) -> Result<FlureeHeaders> {
    let authorization = if let Some(did) = signed_identity {
        Some(CredentialPolicy::Fixed(
            PolicyAuthorization::from_trusted_options(GovernanceOptions {
                identity: Some(did.to_owned()),
                policy_class: state
                    .config
                    .data_auth_default_policy_class
                    .clone()
                    .map(|c| vec![c]),
                ..Default::default()
            }),
        ))
    } else {
        principal.map(|p| p.policy_authorization.clone())
    };
    if let Some(authorization) = authorization {
        // Request-selected credentials must wait until body/header merging is complete.
        if matches!(authorization, CredentialPolicy::Request) {
            headers.policy_authorization = Some(authorization);
            return Ok(headers);
        }
        let requested = governance_from_headers(headers.identity.as_deref(), &headers)?;
        let options = authorization.resolve_options(&requested)?;
        headers.identity = options.identity;
        headers.policy_class = options.policy_class.unwrap_or_default();
        headers.policy = options.policy;
        headers.policy_values = options
            .policy_values
            .map(serde_json::to_value)
            .transpose()?;
        headers.default_allow = options.default_allow;
        headers.policy_authorization = Some(authorization);
    }
    Ok(headers)
}

/// Apply after all body/header/envelope merges; canonical nulls shadow outer
/// policy defaults and source overrides cannot replace authenticated selection.
pub(crate) fn apply_authorization_to_opts(
    query: &mut Value,
    headers: &FlureeHeaders,
) -> Result<()> {
    if let Some(authorization) = &headers.policy_authorization {
        authorization.apply_to_jsonld(query)?;
    }
    Ok(())
}

/// Apply the same policy selection when an endpoint builds a view directly.
pub(crate) async fn wrap_authorized_view(
    state: &AppState,
    view: fluree_db_api::GraphDb,
    headers: &FlureeHeaders,
) -> Result<fluree_db_api::GraphDb> {
    let opts = bound_governance(headers.identity.as_deref(), headers)?;
    wrap_governed_view(state, view, &opts).await
}

/// JSON routes must use the finalized body selection, including request-selected
/// credentials and body-level narrowing, after authorization has been applied.
pub(crate) async fn wrap_jsonld_view(
    state: &AppState,
    view: fluree_db_api::GraphDb,
    query: &Value,
) -> Result<fluree_db_api::GraphDb> {
    let opts = GovernanceOptions::from_json(query)
        .map_err(|e| crate::error::ServerError::bad_request(e.to_string()))?;
    wrap_governed_view(state, view, &opts).await
}

async fn wrap_governed_view(
    state: &AppState,
    view: fluree_db_api::GraphDb,
    opts: &GovernanceOptions,
) -> Result<fluree_db_api::GraphDb> {
    if opts.has_any_policy_inputs() {
        Ok(state.fluree.wrap_policy(view, opts, None).await?)
    } else {
        Ok(state.fluree.wrap_policy_defaults(view).await?)
    }
}

/// Resolve header policy options against the authority attached by
/// `bind_authorization`. Header-only transports call this after binding; JSON
/// bodies use `apply_authorization_to_opts` after merging their header defaults.
/// Fixed credentials accept matching selections and deny narrowing; request
/// credentials resolve their final selection here, including deny when absent.
pub(crate) fn bound_governance(
    identity: Option<&str>,
    headers: &FlureeHeaders,
) -> Result<GovernanceOptions> {
    let requested = governance_from_headers(identity, headers)?;
    match &headers.policy_authorization {
        Some(authorization) => authorization.resolve_options(&requested),
        None => Ok(requested),
    }
}

/// Parse caller header options without applying credential authorization.
fn governance_from_headers(
    identity: Option<&str>,
    headers: &FlureeHeaders,
) -> Result<GovernanceOptions> {
    let policy_values_map = headers.policy_values_map()?;
    Ok(GovernanceOptions {
        identity: identity.map(String::from),
        policy_class: if headers.policy_class.is_empty() {
            None
        } else {
            Some(headers.policy_class.clone())
        },
        policy: headers.policy.clone(),
        policy_values: policy_values_map,
        default_allow: headers.default_allow,
    })
}
