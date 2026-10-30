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

/// Refuse raw commit content of `ledger` unless the ledger suppresses no
/// history or `granted` (the route's own principal) says the caller may
/// audit it. A bearer token the route's extractor ignored (data auth off)
/// still counts when it carries the audit permission. A cluster replica
/// short of the erasures it applied serves raw content to no one.
pub(crate) async fn require_audit(
    state: &AppState,
    headers: &HeaderMap,
    ledger: &str,
    granted: bool,
) -> Result<()> {
    if granted {
        return Ok(state.fluree.confirm_history_current(ledger).await?);
    }
    if !state.fluree.history_suppressed(ledger).await? {
        return Ok(());
    }
    let ledger_id = LedgerId::parse(ledger).map_err(|e| ServerError::bad_request(e.to_string()))?;
    if let Some(token) = crate::extract::extract_bearer_token(headers) {
        if let Ok(principal) = crate::extract::verify_data_principal(&token, state).await {
            if principal.can_audit(&ledger_id) {
                return Ok(());
            }
        }
    }
    Err(ServerError::Api(ApiError::http(
        403,
        format!(
            "{ledger} suppresses history: raw commits and transactions need the \
             fluree.ledger.audit permission"
        ),
    )))
}
