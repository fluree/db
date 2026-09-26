//! Canonical ledger-ID-to-path helpers for storage addresses.
//!
//! We avoid putting `:` in storage paths for cross-platform portability
//! (Windows/macOS filesystem restrictions). Instead a ledger ID
//! `name[:branch]` maps to `name/branch` (default branch applied).
//!
//! These are pure derivations: every valid spelling of an id maps to the same
//! path, so they parse leniently. They still trip in debug builds when handed
//! a non-canonical id, because the caller that skipped normalization is
//! usually also keying a cache or comparing ids with it.

use crate::ledger_id::{LedgerId, LedgerIdParseError, LedgerName};

/// Namespace for content shared across all branches of a ledger.
///
/// Uses `@` prefix, which cannot collide with any real branch name since
/// `@` is forbidden by [`validate_branch_name`](crate::validate_branch_name).
pub const SHARED_NAMESPACE: &str = "@shared";

/// Parse an id that a storage seam received as a string.
///
/// Debug builds panic on a non-canonical id so tests catch the path that
/// skipped edge normalization; release builds apply the default branch.
pub(crate) fn storage_ledger_id(
    ledger_id: &str,
    seam: &str,
) -> Result<LedgerId, LedgerIdParseError> {
    debug_assert!(
        LedgerId::expect_canonical(ledger_id, seam).is_ok(),
        "{}",
        LedgerId::expect_canonical(ledger_id, seam).unwrap_err()
    );
    LedgerId::parse(ledger_id)
}

/// Convert a ledger ID `name[:branch]` into a portable path prefix `name/branch`.
pub fn ledger_id_to_path_prefix(ledger_id: &str) -> Result<String, LedgerIdParseError> {
    Ok(LedgerId::parse(ledger_id)?.path_prefix())
}

/// Path prefix for content shared across all branches of a ledger.
///
/// Takes the ledger *name*: whole-ledger operations must not be reachable
/// with a branch-qualified id.
pub fn shared_prefix_for_path(ledger_name: &LedgerName) -> String {
    ledger_name.shared_prefix()
}
