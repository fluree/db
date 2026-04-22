//! Index-based time travel resolution
//!
//! Replaces commit-chain walking with efficient O(log n) index queries for
//! resolving `@iso:<datetime>` and `@commit:<prefix>` time travel specifiers.
//!
//! # Background
//!
//! Commit metadata is stored as queryable flakes (8-10 per commit):
//! - **Commit subject** (FLUREE_COMMIT + hex): ledger#time, ledger#t, ledger#size, ledger#flakes, etc.
//!
//! The `Flake.t` value on these flakes IS the commit transaction number,
//! so we can resolve time travel efficiently using index queries.

use std::collections::HashSet;

use chrono::{TimeZone, Utc};
use fluree_db_core::overlay::OverlayProvider;
use fluree_db_core::{
    range_bounded_with_overlay, range_with_overlay, Flake, FlakeValue, IndexType, LedgerSnapshot,
    ObjectBounds, RangeMatch, RangeOptions, RangeTest, Sid, TXN_META_GRAPH_ID,
};
use fluree_vocab::db::TIME as LEDGER_TIME;
use fluree_vocab::namespaces::{FLUREE_COMMIT, FLUREE_DB};

use crate::error::{ApiError, Result};

/// Convert epoch milliseconds to an ISO-8601 string for error messages.
fn epoch_ms_to_iso(epoch_ms: i64) -> String {
    Utc.timestamp_millis_opt(epoch_ms)
        .single()
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| epoch_ms.to_string())
}

/// Resolve an ISO-8601 datetime to a transaction number using POST index queries.
///
/// # Algorithm
///
/// 1. Query POST for first commit with `ledger#time > target_epoch_ms`
/// 2. Return that flake's `t - 1` (the previous commit's t)
/// 3. If no result, return `current_t` (target is >= all commits)
/// 4. If target < earliest commit time, return error
///
/// # Why this works
///
/// POST index orders by predicate → object → subject → t.
/// With `lower_bound > target`, we get the first time greater than target.
/// The previous t is the commit with the largest time <= target.
///
/// # Arguments
///
/// * `snapshot` - The database snapshot to query
/// * `overlay` - Optional overlay provider (novelty) for uncommitted data
/// * `target_epoch_ms` - Target timestamp in epoch milliseconds
/// * `current_t` - Current head transaction number (used as fallback)
///
/// # Errors
///
/// Returns an error if the target timestamp is before the earliest commit.
pub async fn datetime_to_t<O>(
    snapshot: &LedgerSnapshot,
    overlay: Option<&O>,
    target_epoch_ms: i64,
    current_t: i64,
) -> Result<i64>
where
    O: OverlayProvider + ?Sized,
{
    tracing::debug!(
        target_epoch_ms,
        current_t,
        "datetime_to_t: resolving ISO epoch-ms"
    );
    let time_predicate = Sid::new(FLUREE_DB, LEDGER_TIME);

    // Step 1: Check if any ledger#time flakes exist at all
    // (and get the earliest commit time)
    let probe_opts = RangeOptions::default()
        .with_to_t(current_t)
        .with_flake_limit(1);

    let probe_match = RangeMatch::predicate(time_predicate.clone());

    let earliest_flakes = if let Some(ovl) = overlay {
        range_with_overlay(
            snapshot,
            TXN_META_GRAPH_ID,
            ovl,
            IndexType::Post,
            RangeTest::Eq,
            probe_match,
            probe_opts,
        )
        .await?
    } else {
        range_with_overlay(
            snapshot,
            TXN_META_GRAPH_ID,
            &fluree_db_core::NoOverlay,
            IndexType::Post,
            RangeTest::Eq,
            probe_match,
            probe_opts,
        )
        .await?
    };

    if earliest_flakes.is_empty() {
        // No commit timestamps exist - fall back to head (matches existing behavior)
        tracing::debug!("datetime_to_t: no ledger#time flakes found; returning head");
        return Ok(current_t);
    }

    // Extract earliest timestamp (POST orders by object ascending for same predicate)
    let earliest_time = match &earliest_flakes[0].o {
        FlakeValue::Long(ms) => *ms,
        _ => return Ok(current_t), // Invalid timestamp type, fall back
    };
    tracing::debug!(earliest_time, "datetime_to_t: earliest ledger#time");

    // Check if target is before earliest commit
    if target_epoch_ms < earliest_time {
        let target_iso = epoch_ms_to_iso(target_epoch_ms);
        let earliest_iso = epoch_ms_to_iso(earliest_time);
        return Err(ApiError::internal(format!(
            "There is no data as of {target_iso} (earliest commit is at {earliest_iso})"
        )));
    }

    // Step 2: Find the first commit AFTER the target time
    // Use object bounds with lower > target_epoch_ms (exclusive)
    let search_opts = RangeOptions::default()
        .with_to_t(current_t)
        .with_flake_limit(1)
        .with_object_bounds(
            ObjectBounds::new().with_lower(FlakeValue::Long(target_epoch_ms), false), // exclusive: > target
        );

    let search_match = RangeMatch::predicate(time_predicate);

    let after_flakes = if let Some(ovl) = overlay {
        range_with_overlay(
            snapshot,
            TXN_META_GRAPH_ID,
            ovl,
            IndexType::Post,
            RangeTest::Eq,
            search_match,
            search_opts,
        )
        .await?
    } else {
        range_with_overlay(
            snapshot,
            TXN_META_GRAPH_ID,
            &fluree_db_core::NoOverlay,
            IndexType::Post,
            RangeTest::Eq,
            search_match,
            search_opts,
        )
        .await?
    };

    if after_flakes.is_empty() {
        // Target is >= all commit times, return head
        tracing::debug!("datetime_to_t: no commit after target; returning head");
        return Ok(current_t);
    }

    // The flake we found is the first commit AFTER target.
    // Its t is the transaction number of that commit.
    // The previous transaction (t - 1) is what we want.
    let after_t = after_flakes[0].t;
    let after_o = &after_flakes[0].o;
    tracing::debug!(
        after_t,
        ?after_o,
        "datetime_to_t: first commit after target"
    );

    // Clamp to 0 minimum (t=0 may be valid for genesis-as-of queries)
    let resolved_t = (after_t - 1).max(0);
    tracing::debug!(resolved_t, "datetime_to_t: resolved t");

    Ok(resolved_t)
}

/// Resolve a commit prefix to a transaction number using bounded SPOT index scan.
///
/// # Algorithm
///
/// 1. Normalize the prefix (strip `fluree:commit:` and `sha256:` if present)
/// 2. Bounded SPOT scan: `[Sid(FLUREE_COMMIT, prefix), Sid(FLUREE_COMMIT, prefix~))`
/// 3. Track unique commit subjects
/// 4. Return `flake.t` from the single match (or error on 0 / >1)
///
/// # Arguments
///
/// * `snapshot` - The database snapshot to query
/// * `overlay` - Optional overlay provider (novelty) for uncommitted data
/// * `commit_prefix` - Commit CID prefix to match (hex digest, with or without standard prefixes)
/// * `current_t` - Current head transaction number
///
/// # Errors
///
/// - If prefix is too short (< 6 chars) or too long (> 64 chars)
/// - If no commit matches the prefix
/// - If multiple commits match (ambiguous prefix)
pub async fn commit_to_t<O>(
    snapshot: &LedgerSnapshot,
    overlay: Option<&O>,
    commit_prefix: &str,
    current_t: i64,
) -> Result<i64>
where
    O: OverlayProvider + ?Sized,
{
    // Step 1: Normalize the commit prefix
    // Strip "fluree:commit:" prefix if present
    let normalized = commit_prefix
        .strip_prefix("fluree:commit:")
        .unwrap_or(commit_prefix);
    // Strip "sha256:" prefix if present
    let normalized = normalized.strip_prefix("sha256:").unwrap_or(normalized);

    // Validation: minimum 6 characters for useful prefix matching
    if normalized.len() < 6 {
        return Err(ApiError::query(format!(
            "Commit prefix must be at least 6 characters, got {}",
            normalized.len()
        )));
    }

    // SHA-256 in hex is 64 characters
    if normalized.len() > 64 {
        return Err(ApiError::query(format!(
            "Commit prefix too long ({} chars). SHA-256 in hex is 64 characters.",
            normalized.len()
        )));
    }

    // Step 2: Create bounded SPOT scan
    // Commit subjects use the FLUREE_COMMIT namespace with hex hash as name
    let start_sid = Sid::new(FLUREE_COMMIT, normalized);
    // Use tilde (~) as end bound since it sorts after all hex characters (0-9, a-f)
    let end_prefix = format!("{normalized}~");
    let end_sid = Sid::new(FLUREE_COMMIT, &end_prefix);

    let start_bound = Flake::min_for_subject(start_sid);
    let end_bound = Flake::min_for_subject(end_sid);

    // Limit flakes to bound the scan. Each commit has ~8-9 flakes,
    // so 32 is enough to detect 0/1/>1 unique subjects efficiently.
    let opts = RangeOptions::default()
        .with_to_t(current_t)
        .with_flake_limit(32);

    let flakes = if let Some(ovl) = overlay {
        range_bounded_with_overlay(
            snapshot,
            TXN_META_GRAPH_ID,
            ovl,
            IndexType::Spot,
            start_bound,
            end_bound,
            opts,
        )
        .await?
    } else {
        range_bounded_with_overlay(
            snapshot,
            TXN_META_GRAPH_ID,
            &fluree_db_core::NoOverlay,
            IndexType::Spot,
            start_bound,
            end_bound,
            opts,
        )
        .await?
    };

    // Step 3: Collect unique commit subjects
    // Filter to only those whose name actually starts with our prefix
    // (the range might include some boundary edge cases)
    let mut matching_commits: Vec<(&Sid, i64)> = Vec::new();
    let mut seen_subjects: HashSet<&Sid> = HashSet::new();

    for flake in &flakes {
        // Verify this subject is a commit in FLUREE_COMMIT and matches prefix
        if flake.s.namespace_code != FLUREE_COMMIT {
            continue;
        }
        if !flake.s.name.starts_with(normalized) {
            continue;
        }

        // Deduplicate by subject
        if seen_subjects.insert(&flake.s) {
            matching_commits.push((&flake.s, flake.t));

            // Early exit if we find an exact match
            if flake.s.name.as_ref() == normalized {
                // Exact match - this is the one
                return Ok(flake.t);
            }
        }

        // Stop if we have more than one match (ambiguous)
        if matching_commits.len() > 1 {
            break;
        }
    }

    // Step 4: Return result based on match count
    match matching_commits.len() {
        0 => Err(ApiError::query(format!(
            "No commit found with prefix: {normalized}"
        ))),
        1 => {
            let (_, t) = matching_commits[0];
            Ok(t)
        }
        _ => {
            // Multiple matches - ambiguous prefix
            let commit_ids: Vec<String> = matching_commits
                .iter()
                .take(5)
                .map(|(sid, _)| format!("fluree:commit:sha256:{}", sid.name))
                .collect();
            Err(ApiError::query(format!(
                "Ambiguous commit prefix: {}. Multiple commits match: {:?}{}",
                normalized,
                commit_ids,
                if matching_commits.len() > 5 {
                    " ..."
                } else {
                    ""
                }
            )))
        }
    }
}

/// Resolve a [`TimeSpec`](crate::TimeSpec) to a concrete transaction number.
///
/// This is the shared resolution logic used by both dataset queries and export.
pub(crate) async fn resolve_time_spec(
    ledger: &fluree_db_ledger::LedgerState,
    spec: &crate::TimeSpec,
) -> Result<i64> {
    let current_t = ledger.t();
    match spec {
        crate::TimeSpec::AtT(t) => Ok(*t),
        crate::TimeSpec::Latest => Ok(current_t),
        crate::TimeSpec::AtTime(iso) => {
            let dt = chrono::DateTime::parse_from_rfc3339(iso).map_err(|e| {
                ApiError::internal(format!(
                    "Invalid ISO-8601 timestamp for time travel: {iso} ({e})"
                ))
            })?;
            // `ledger#time` flakes store epoch milliseconds. Ceiling sub-ms precision
            // to avoid truncation off-by-one.
            let mut target_epoch_ms = dt.timestamp_millis();
            if dt.timestamp_subsec_nanos() % 1_000_000 != 0 {
                target_epoch_ms += 1;
            }
            datetime_to_t(
                &ledger.snapshot,
                Some(ledger.novelty.as_ref()),
                target_epoch_ms,
                current_t,
            )
            .await
        }
        crate::TimeSpec::AtCommit(commit_prefix) => {
            commit_to_t(
                &ledger.snapshot,
                Some(ledger.novelty.as_ref()),
                commit_prefix,
                current_t,
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_commit_prefix_normalization() {
        // Test various prefix formats
        let cases = [
            ("abc123", "abc123"),
            ("sha256:abc123", "abc123"),
            ("fluree:commit:abc123", "abc123"),
            ("fluree:commit:sha256:abc123", "abc123"),
        ];

        for (input, expected) in cases {
            let normalized = input.strip_prefix("fluree:commit:").unwrap_or(input);
            let normalized = normalized.strip_prefix("sha256:").unwrap_or(normalized);
            assert_eq!(normalized, expected, "Failed for input: {input}");
        }
    }

    #[test]
    fn test_commit_prefix_validation() {
        // Too short
        assert!(commit_prefix_valid("abc12").is_err());
        // Minimum valid
        assert!(commit_prefix_valid("abc123").is_ok());
        // Maximum valid (64 hex chars)
        assert!(commit_prefix_valid(&"a".repeat(64)).is_ok());
        // Too long
        assert!(commit_prefix_valid(&"a".repeat(65)).is_err());
    }

    fn commit_prefix_valid(prefix: &str) -> std::result::Result<(), &'static str> {
        if prefix.len() < 6 {
            Err("too short")
        } else if prefix.len() > 64 {
            Err("too long")
        } else {
            Ok(())
        }
    }
}
