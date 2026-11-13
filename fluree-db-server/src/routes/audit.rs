//! Audit-log access on ledgers whose retention suppresses history.
//!
//! On such a ledger the commit log still holds what its history no longer
//! serves: raw commits and transactions are audit data. Reading them takes
//! the `fluree.ledger.audit.*` permission, which ordinary read, history,
//! export and storage scopes do not imply. Ledgers without suppression are
//! unaffected.

use axum::http::HeaderMap;
use fluree_db_api::{ApiError, LedgerId};

use crate::error::{Result, ServerError};
use crate::state::AppState;

/// Refuse raw commit content read through `ledger` unless the caller may
/// audit each branch whose suppressed history it reaches (its own, or a
/// source branch's, whose commits its store reads too). `granted` says
/// whether the route's own principal may audit a branch. A bearer token the
/// route's extractor ignored (data auth off) still counts when it carries the
/// audit permission. A cluster replica short of the erasures it applied
/// serves raw content to no one.
pub(crate) async fn require_audit(
    state: &AppState,
    headers: &HeaderMap,
    ledger: &str,
    granted: impl Fn(&LedgerId) -> bool,
) -> Result<()> {
    require_audit_reaching(state, headers, ledger, granted, false).await
}

/// [`require_audit`] for a grant over `ledger`'s whole storage, which
/// reaches every branch's commits.
#[cfg(feature = "aws")]
pub(crate) async fn require_audit_of_every_branch(
    state: &AppState,
    headers: &HeaderMap,
    ledger: &str,
    granted: impl Fn(&LedgerId) -> bool,
) -> Result<()> {
    require_audit_reaching(state, headers, ledger, granted, true).await
}

async fn require_audit_reaching(
    state: &AppState,
    headers: &HeaderMap,
    ledger: &str,
    granted: impl Fn(&LedgerId) -> bool,
    every_branch: bool,
) -> Result<()> {
    let suppressing = state
        .fluree
        .raw_history_suppressing(ledger, every_branch)
        .await?;
    let mut bearer = None;
    for branch in suppressing {
        if granted(&branch) {
            continue;
        }
        if bearer.is_none() {
            bearer = Some(match crate::extract::extract_bearer_token(headers) {
                Some(token) => crate::extract::verify_data_principal(&token, state)
                    .await
                    .ok(),
                None => None,
            });
        }
        if bearer
            .as_ref()
            .and_then(Option::as_ref)
            .is_some_and(|principal| principal.can_audit(&branch))
        {
            continue;
        }
        let through = if branch.as_str() == ledger {
            String::new()
        } else {
            format!(" (read through {ledger})")
        };
        return Err(ServerError::Api(ApiError::http(
            403,
            format!(
                "{branch} suppresses history{through}: raw commits and transactions need \
                 the fluree.ledger.audit permission"
            ),
        )));
    }
    Ok(())
}
