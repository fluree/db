//! Canonical ledger-ID-to-path helpers for storage addresses.
//!
//! We avoid putting `:` in storage paths for cross-platform portability
//! (Windows/macOS filesystem restrictions). Instead we normalize a ledger ID
//! `name[:branch]` into `name/branch` (default branch applied).

use crate::ledger_id::{split_ledger_id, LedgerIdParseError};

/// Convert a ledger ID `name[:branch]` into a portable path prefix `name/branch`.
///
/// - Applies the default branch if missing.
/// - Never includes `:` in the output.
pub fn ledger_id_to_path_prefix(ledger_id: &str) -> Result<String, LedgerIdParseError> {
    // If caller already provided a storage-path style ID (`name/branch`),
    // preserve it as-is. This is used in various places (tests, file layouts)
    // and is already portable (no ':').
    if !ledger_id.contains(':') && ledger_id.contains('/') {
        return Ok(ledger_id.to_string());
    }

    let (name, branch) = split_ledger_id(ledger_id)?;
    Ok(format!("{name}/{branch}"))
}

/// Namespace for content shared across all branches of a ledger.
///
/// Uses `@` prefix, which cannot collide with any real branch name since
/// `@` is forbidden by [`validate_branch_name`].
pub const SHARED_NAMESPACE: &str = "@shared";

/// Path prefix for content shared across all branches of a ledger.
///
/// Given `"mydb:main"` or `"mydb:feature-x"`, returns `"mydb/@shared"`.
/// Given `"mydb/main"` (path form), returns `"mydb/@shared"`.
pub fn shared_prefix_for_path(ledger_id: &str) -> String {
    let name = if !ledger_id.contains(':') && ledger_id.contains('/') {
        ledger_id.split('/').next().unwrap_or(ledger_id)
    } else {
        ledger_id.split(':').next().unwrap_or(ledger_id)
    };
    format!("{name}/{SHARED_NAMESPACE}")
}
