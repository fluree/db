//! Multi-query envelope snapshot resolution.
//!
//! [`resolve_envelope_snapshot`] runs once per envelope, before sub-query
//! dispatch. It walks the distinct ledger set (provided by validation) and
//! produces an [`EnvelopeSnapshot`] mapping each ledger to a concrete `t`,
//! honoring the envelope `asOf` (integer, ISO timestamp, or omitted).
//!
//! Sub-queries run as written: the dispatcher hands the snapshot to execution
//! ([`EnvelopeSnapshot::ledger_times`]), and the connection path reads each
//! dataset member that names no time of its own at its ledger's `t`. A member
//! keeps the name the query wrote it by. The collision check in validation
//! already ensures no sub-query names a time of its own when the envelope sets
//! `asOf`.
//!
//! The snapshot moment is recorded once at envelope-entry — the "shared time
//! resolution" the envelope contract promises. It is **not** distributed
//! atomicity: per-ledger `t` values represent each ledger's latest commit
//! at-or-before the moment, and ledger commit clocks are not synchronized.

use std::collections::{BTreeSet, HashMap};

use chrono::{DateTime, Utc};

use crate::dataset::LedgerTimes;
use crate::query::multi::AsOf;
use crate::{time_resolve, ApiError, Fluree, Result};
use fluree_db_query::QueryError;

/// Resolved per-ledger `t` map for an envelope, plus the wall-clock moment it
/// represents (echoed back to the client).
///
/// Built by [`resolve_envelope_snapshot`] before dispatch; execution reads
/// each sub-query's ledgers at its [`EnvelopeSnapshot::ledger_times`].
#[derive(Debug, Clone)]
pub struct EnvelopeSnapshot {
    /// ISO 8601 wall-clock moment that produced the per-ledger map. Echoed in
    /// the response so clients can replay against the same moment. `None` only
    /// when the envelope used an explicit integer `asOf` (which is necessarily
    /// single-ledger and has no shared wall-clock interpretation).
    pub as_of: Option<String>,
    /// Per-ledger numeric `t`. For each ledger referenced by the envelope,
    /// this is the time every member of that ledger that names no time of its
    /// own is read at.
    pub ledgers: HashMap<String, i64>,
}

impl EnvelopeSnapshot {
    /// The snapshot as execution reads it: each ledger's `t`, by ledger id.
    /// The keys are the canonical ids validation produced, so each parses; one
    /// that did not would be an error here rather than a ledger read at its
    /// head.
    pub(crate) fn ledger_times(&self) -> Result<LedgerTimes> {
        self.ledgers
            .iter()
            .map(|(ledger, t)| {
                let id = fluree_db_core::LedgerId::parse(ledger).map_err(|e| {
                    ApiError::internal(format!(
                        "snapshot ledger '{ledger}' is not a ledger id: {e}"
                    ))
                })?;
                Ok((id, *t))
            })
            .collect()
    }
}

/// Failure modes specific to snapshot resolution. The caller's mistakes keep
/// their class as an [`ApiError`]; see its `From` impl.
#[derive(Debug, thiserror::Error)]
pub enum EnvelopeSnapshotError {
    #[error("invalid ISO 8601 timestamp for asOf: {iso} ({source})")]
    InvalidIso {
        iso: String,
        source: chrono::ParseError,
    },
    #[error("failed to load ledger '{ledger}' for snapshot resolution: {source}")]
    LedgerLoad { ledger: String, source: ApiError },
    #[error("failed to resolve asOf for ledger '{ledger}': {source}")]
    PerLedgerResolve { ledger: String, source: ApiError },
}

/// An `asOf` that is malformed or names no data on a ledger is a 400, and a
/// ledger that does not exist a 404, as each is on a single query. Anything
/// else is a failure to honor a valid `asOf`.
impl From<EnvelopeSnapshotError> for ApiError {
    fn from(err: EnvelopeSnapshotError) -> Self {
        match err {
            EnvelopeSnapshotError::InvalidIso { .. } => ApiError::invalid_query(err.to_string()),
            EnvelopeSnapshotError::LedgerLoad { source, .. }
            | EnvelopeSnapshotError::PerLedgerResolve { source, .. }
                if source.is_not_found() =>
            {
                source
            }
            EnvelopeSnapshotError::PerLedgerResolve {
                ledger,
                source: ApiError::Query(QueryError::InvalidQuery(msg)),
            } => ApiError::invalid_query(format!(
                "failed to resolve asOf for ledger '{ledger}': {msg}"
            )),
            other => ApiError::internal(other.to_string()),
        }
    }
}

/// Resolve the envelope's `asOf` to a per-ledger `t` map.
///
/// Behaviour by `asOf` variant:
/// - `Some(AsOf::T(t))`: the (single) ledger pins to `t` directly. Validation
///   already rejected multi-ledger envelopes paired with an integer `asOf`.
/// - `Some(AsOf::Iso(iso))`: each distinct ledger resolves the timestamp via
///   [`time_resolve::datetime_to_t`] against its own current snapshot.
/// - `None`: server-now is captured once and each distinct ledger pins to its
///   current `t`. The `as_of` field of the returned snapshot is populated
///   with that captured moment so the response is reproducible.
///
/// Per-ledger work runs sequentially in v1; the connection-level ledger cache
/// ([`Fluree::ledger_cached`]) coalesces repeated loads of the same ledger so
/// the bounded fan-out is acceptable. Parallel resolution can come later if
/// large envelopes show pressure here.
pub async fn resolve_envelope_snapshot(
    fluree: &Fluree,
    distinct_ledgers: &BTreeSet<String>,
    as_of: Option<&AsOf>,
) -> Result<EnvelopeSnapshot> {
    let (echoed_as_of, target_iso) = match as_of {
        Some(AsOf::Iso(iso)) => (Some(iso.clone()), Some(iso.clone())),
        Some(AsOf::T(_)) => (None, None),
        None => {
            // Server-resolved "now" — captured once, applied uniformly. We
            // echo this so the client knows exactly when the envelope ran.
            let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
            (Some(now), None)
        }
    };

    let mut ledgers: HashMap<String, i64> = HashMap::with_capacity(distinct_ledgers.len());

    for ledger_id in distinct_ledgers {
        let t = match (&as_of, target_iso.as_deref()) {
            // Integer t: pin directly. The single-ledger invariant is
            // enforced at validation; if a caller bypasses validation we'd
            // pin every ledger to the same integer, which is the documented
            // worst-case but still well-defined.
            (Some(AsOf::T(t)), _) => *t,
            (_, Some(iso)) => resolve_iso_to_t(fluree, ledger_id, iso).await?,
            (None, None) => resolve_current_t(fluree, ledger_id).await?,
            // AsOf::T paired with an ISO target is unreachable.
            (Some(AsOf::Iso(_)), None) => unreachable!("ISO branch sets target_iso"),
        };
        ledgers.insert(ledger_id.clone(), t);
    }

    Ok(EnvelopeSnapshot {
        as_of: echoed_as_of,
        ledgers,
    })
}

async fn resolve_current_t(fluree: &Fluree, ledger_id: &str) -> Result<i64> {
    let handle =
        fluree
            .ledger_cached(ledger_id)
            .await
            .map_err(|e| EnvelopeSnapshotError::LedgerLoad {
                ledger: ledger_id.to_string(),
                source: e,
            })?;
    Ok(handle.t().await)
}

async fn resolve_iso_to_t(fluree: &Fluree, ledger_id: &str, iso: &str) -> Result<i64> {
    let handle =
        fluree
            .ledger_cached(ledger_id)
            .await
            .map_err(|e| EnvelopeSnapshotError::LedgerLoad {
                ledger: ledger_id.to_string(),
                source: e,
            })?;
    let view = handle.snapshot().await;
    let ledger = view.to_ledger_state();
    let current_t = ledger.t();

    let dt =
        DateTime::parse_from_rfc3339(iso).map_err(|source| EnvelopeSnapshotError::InvalidIso {
            iso: iso.to_string(),
            source,
        })?;
    // ledger#time flakes store epoch milliseconds; mirror the rounding rule
    // from load_graph_db_at so sub-millisecond ISO precision doesn't push us
    // off-by-one before the intended commit (especially around genesis).
    let mut target_epoch_ms = dt.timestamp_millis();
    if dt.timestamp_subsec_nanos() % 1_000_000 != 0 {
        target_epoch_ms += 1;
    }

    let resolved = time_resolve::datetime_to_t(
        &ledger.snapshot,
        Some(ledger.novelty.as_ref()),
        target_epoch_ms,
        current_t,
    )
    .await
    .map_err(|e| EnvelopeSnapshotError::PerLedgerResolve {
        ledger: ledger_id.to_string(),
        source: e,
    })?;

    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each snapshot id reads back as the ledger id it came from; one that
    /// does not is an error for the envelope, never a ledger left out of the
    /// times (and so read at its head).
    #[test]
    fn a_snapshot_id_that_is_no_ledger_id_fails_the_envelope() {
        let mut ledgers = HashMap::from([("mq:a".to_string(), 3)]);
        let snapshot = EnvelopeSnapshot {
            as_of: None,
            ledgers: ledgers.clone(),
        };
        let times = snapshot.ledger_times().expect("canonical ids parse");
        assert_eq!(
            times.get(&fluree_db_core::LedgerId::parse("mq:a").unwrap()),
            Some(&3)
        );

        ledgers.insert("http://ex.org/g".to_string(), 4);
        let snapshot = EnvelopeSnapshot {
            as_of: None,
            ledgers,
        };
        assert!(snapshot.ledger_times().is_err());
    }
}
