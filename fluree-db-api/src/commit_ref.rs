//! A commit reference: one of several forms a caller can use to identify a
//! commit (exact CID, hex-digest prefix, or transaction number `t`).
//!
//! Each variant resolves to the canonical [`CommitId`] via the helpers in this
//! module. The resolvers scan the commit metadata recorded in the txn-meta
//! graph, so they require both a [`LedgerSnapshot`] and the current novelty
//! overlay.

use crate::{ApiError, Result};
use fluree_db_core::{CommitId, ContentId, LedgerSnapshot};
use fluree_db_novelty::Novelty;

/// How a caller identifies a commit.
///
/// Commits have a canonical content-addressed id ([`CommitId`]), but there are
/// several user-facing forms that resolve to the same id.
pub enum CommitRef {
    /// Exact CID (e.g., from API or full CID string like "bagaybqabciq...")
    Exact(CommitId),
    /// Hex digest prefix (e.g., "3dd028" — the SHA-256 hex prefix of the commit)
    Prefix(String),
    /// Transaction number (e.g., t=5)
    T(i64),
}

/// Resolve a commit hex-digest prefix to a full [`CommitId`].
///
/// Uses a bounded SPOT index scan on commit subjects (same approach as
/// `time_resolve::commit_to_t`, but returns the CID instead of `t`).
pub(crate) async fn resolve_commit_prefix(
    snapshot: &LedgerSnapshot,
    overlay: &Novelty,
    prefix: &str,
    current_t: i64,
) -> Result<CommitId> {
    use fluree_db_core::{
        range_bounded_with_overlay, Flake, IndexType, RangeOptions, Sid, TXN_META_GRAPH_ID,
    };
    use fluree_vocab::namespaces::FLUREE_COMMIT;

    // Normalize: strip standard prefixes
    let normalized = prefix.strip_prefix("fluree:commit:").unwrap_or(prefix);
    let normalized = normalized.strip_prefix("sha256:").unwrap_or(normalized);

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

    // Build scan range: [prefix, prefix~) where ~ sorts after all hex chars
    let start_sid = Sid::new(FLUREE_COMMIT, normalized);
    let end_prefix = format!("{normalized}~");
    let end_sid = Sid::new(FLUREE_COMMIT, &end_prefix);

    let start_bound = Flake::min_for_subject(start_sid);
    let end_bound = Flake::min_for_subject(end_sid);

    let opts = RangeOptions::default()
        .with_to_t(current_t)
        .with_flake_limit(32);

    let flakes = range_bounded_with_overlay(
        snapshot,
        TXN_META_GRAPH_ID,
        overlay,
        IndexType::Spot,
        start_bound,
        end_bound,
        opts,
    )
    .await?;

    // Collect unique matching commit subjects
    let mut seen = std::collections::HashSet::new();
    let mut matches: Vec<String> = Vec::new();

    for flake in &flakes {
        if flake.s.namespace_code != FLUREE_COMMIT {
            continue;
        }
        if !flake.s.name.starts_with(normalized) {
            continue;
        }
        if seen.insert(flake.s.name.as_ref()) {
            matches.push(flake.s.name.to_string());
        }
        if matches.len() > 1 {
            break;
        }
    }

    match matches.len() {
        0 => Err(ApiError::NotFound(format!(
            "No commit found with prefix: {normalized}"
        ))),
        1 => {
            // Reconstruct ContentId from hex digest
            let hex = &matches[0];
            let digest: [u8; 32] = hex::decode(hex)
                .map_err(|e| ApiError::internal(format!("Invalid hex digest: {e}")))?
                .try_into()
                .map_err(|_| ApiError::internal("Digest not 32 bytes"))?;
            Ok(ContentId::from_sha256_digest(
                fluree_db_core::CODEC_FLUREE_COMMIT,
                &digest,
            ))
        }
        _ => {
            let ids: Vec<_> = matches
                .iter()
                .take(5)
                .map(|h| &h[..7.min(h.len())])
                .collect();
            Err(ApiError::query(format!(
                "Ambiguous commit prefix '{}': matches {:?}{}",
                normalized,
                ids,
                if matches.len() > 5 { " ..." } else { "" }
            )))
        }
    }
}

/// Resolve a transaction number (`t`) to a full [`CommitId`].
///
/// Queries the POST index for commit flakes where predicate = `fluree:db/t`
/// and object = the target `t` value. The matching commit subject's hex digest
/// is then converted to a [`CommitId`].
pub(crate) async fn resolve_t_to_commit_id(
    snapshot: &LedgerSnapshot,
    overlay: &Novelty,
    target_t: i64,
    current_t: i64,
) -> Result<CommitId> {
    use fluree_db_core::{
        range_with_overlay, FlakeValue, IndexType, RangeMatch, RangeOptions, RangeTest, Sid,
        TXN_META_GRAPH_ID,
    };
    use fluree_vocab::namespaces::{FLUREE_COMMIT, FLUREE_DB};

    if target_t < 1 {
        return Err(ApiError::query(format!(
            "Transaction number must be >= 1, got {target_t}"
        )));
    }
    if target_t > current_t {
        return Err(ApiError::NotFound(format!(
            "Transaction t={target_t} not found (latest is t={current_t})"
        )));
    }

    // POST index query: predicate = fluree:db/t, object = target_t (exact match)
    let predicate = Sid::new(FLUREE_DB, fluree_vocab::db::T);
    let range_match = RangeMatch::predicate_object(predicate, FlakeValue::Long(target_t));

    let opts = RangeOptions::default()
        .with_to_t(current_t)
        .with_flake_limit(16);

    let flakes = range_with_overlay(
        snapshot,
        TXN_META_GRAPH_ID,
        overlay,
        IndexType::Post,
        RangeTest::Eq,
        range_match,
        opts,
    )
    .await?;

    // Find the flake with our exact predicate and object value
    for flake in &flakes {
        if flake.p.namespace_code != FLUREE_DB || flake.p.name.as_ref() != fluree_vocab::db::T {
            continue;
        }
        if flake.o != FlakeValue::Long(target_t) {
            continue;
        }
        // The subject is in FLUREE_COMMIT namespace with hex digest as name
        if flake.s.namespace_code != FLUREE_COMMIT {
            continue;
        }
        let hex = flake.s.name.as_ref();
        let digest: [u8; 32] = hex::decode(hex)
            .map_err(|e| ApiError::internal(format!("Invalid hex digest: {e}")))?
            .try_into()
            .map_err(|_| ApiError::internal("Digest not 32 bytes"))?;
        return Ok(ContentId::from_sha256_digest(
            fluree_db_core::CODEC_FLUREE_COMMIT,
            &digest,
        ));
    }

    Err(ApiError::NotFound(format!(
        "No commit found for t={target_t}"
    )))
}
