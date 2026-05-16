//! Cross-ledger resolver entry point.
//!
//! The single helper shared by every subsystem (policy / shapes /
//! schema / rules / constraints). Materialization is dispatched per
//! [`ArtifactKind`]; only `PolicyRules` is implemented in Phase 1a
//! (this slice provides the orchestration skeleton — the per-kind
//! materializer is added in slice 4).
//!
//! See `docs/design/cross-ledger-model-enforcement.md`.

use super::types::{
    check_cycle, memo_hit, reject_if_reserved_graph, ArtifactKind, GovernanceArtifact,
    ResolveCtx, ResolvedGraph,
};
use super::CrossLedgerError;
use fluree_db_core::ledger_config::GraphSourceRef;
use fluree_db_nameservice::NsRecord;
use std::sync::Arc;

/// Resolve a cross-ledger `GraphSourceRef` to a term-neutral artifact.
///
/// Performs, in order:
///
/// 1. Validate this is actually a cross-ledger ref (`f:ledger` set)
///    and that unsupported Phase-4 fields (`f:trustPolicy`,
///    `f:rollbackGuard`) are absent.
/// 2. Canonicalize the model ledger id via the nameservice
///    (`nameservice.lookup`); fail with `ModelLedgerMissing` if the
///    ledger is absent or retracted on this instance.
/// 3. Reject `#config` / `#txn-meta` selectors before any storage
///    round-trip on M.
/// 4. Capture `resolved_t` — `f:atT` pin if set, else lazy per-request
///    head-t in `ctx.resolved_ts`.
/// 5. Form the tuple `(canonical_model_ledger_id, graph_iri,
///    resolved_t)` and check `ctx.memo`. On hit, return immediately.
/// 6. Check `ctx.active` for cycles. On miss, push and call into the
///    per-kind materializer.
/// 7. On materializer success, pop `active`, insert into `memo`, and
///    return.
///
/// Materialization itself is not implemented in this slice — the
/// per-kind dispatch returns
/// `CrossLedgerError::TranslationFailed { detail: "..." }` so call
/// sites can be wired before the projector lands.
pub async fn resolve_graph_ref(
    graph_ref: &GraphSourceRef,
    kind: ArtifactKind,
    ctx: &mut ResolveCtx<'_>,
) -> Result<Arc<ResolvedGraph>, CrossLedgerError> {
    // (1) Phase-4 fields not yet supported. Surface a clear failure
    // rather than silently ignoring them.
    if graph_ref.trust_policy.is_some() {
        return Err(CrossLedgerError::TrustCheckFailed {
            ledger_id: graph_ref.ledger.clone().unwrap_or_default(),
            detail: "f:trustPolicy is not yet supported (Phase 4)".into(),
        });
    }
    if graph_ref.rollback_guard.is_some() {
        return Err(CrossLedgerError::TrustCheckFailed {
            ledger_id: graph_ref.ledger.clone().unwrap_or_default(),
            detail: "f:rollbackGuard is not yet supported (Phase 4)".into(),
        });
    }

    // Caller is expected to invoke this only for cross-ledger refs
    // (graph_ref.ledger.is_some()). A same-ledger ref would be a
    // bug at the call site, not a user-facing failure.
    let raw_ledger_ref = graph_ref.ledger.as_deref().ok_or_else(|| {
        CrossLedgerError::TranslationFailed {
            ledger_id: String::new(),
            graph_iri: graph_ref.graph_selector.clone().unwrap_or_default(),
            detail: "resolve_graph_ref called without f:ledger; same-ledger refs \
                    must use the local resolver"
                .into(),
        }
    })?;

    let graph_iri = graph_ref.graph_selector.as_deref().ok_or_else(|| {
        CrossLedgerError::TranslationFailed {
            ledger_id: raw_ledger_ref.to_string(),
            graph_iri: String::new(),
            detail: "cross-ledger references require an explicit f:graphSelector".into(),
        }
    })?;

    // (2) Canonicalize via the nameservice. Same-instance is enforced
    // implicitly: anything outside our nameservice can't be looked
    // up. (Cross-instance federation would surface here as a separate
    // resolver path; Phase next.)
    let ns_record: NsRecord = match ctx.fluree.nameservice().lookup(raw_ledger_ref).await {
        Ok(Some(record)) if !record.retracted => record,
        Ok(Some(_retracted)) => {
            return Err(CrossLedgerError::ModelLedgerMissing {
                ledger_id: raw_ledger_ref.to_string(),
            });
        }
        Ok(None) => {
            return Err(CrossLedgerError::ModelLedgerMissing {
                ledger_id: raw_ledger_ref.to_string(),
            });
        }
        Err(e) => {
            return Err(CrossLedgerError::TranslationFailed {
                ledger_id: raw_ledger_ref.to_string(),
                graph_iri: graph_iri.to_string(),
                detail: format!("nameservice lookup failed: {e}"),
            });
        }
    };
    let canonical_ledger_id = ns_record.ledger_id.clone();

    // (3) Reserved-graph guard against the *canonical* id so an alias
    // can't slip a #config selector through by typing it for a
    // different alias spelling.
    reject_if_reserved_graph(&canonical_ledger_id, graph_iri)?;

    // (4) resolved_t: pinned f:atT or lazy per-request capture.
    let resolved_t = if let Some(pinned) = graph_ref.at_t {
        // Pinned values are NOT stored in resolved_ts; only unpinned
        // captures are. This keeps the lazy-capture cache from being
        // polluted by per-resolve pins.
        pinned
    } else if let Some(t) = ctx.resolved_ts.get(&canonical_ledger_id) {
        *t
    } else {
        let head_t = ns_record.commit_t;
        ctx.resolved_ts
            .insert(canonical_ledger_id.clone(), head_t);
        head_t
    };

    let tuple = (canonical_ledger_id.clone(), graph_iri.to_string(), resolved_t);

    // (5) Memo hit — short-circuit cross-subsystem de-dup before
    // entering `active`, so two subsystems referencing the same
    // (M, graph, t) never look like a cycle to each other.
    if let Some(hit) = memo_hit(&ctx.memo, &tuple) {
        return Ok(hit);
    }

    // (6) Cycle check.
    check_cycle(&ctx.active, &tuple)?;

    ctx.active.push(tuple.clone());

    // Materialize. Slice 3 provides the dispatch shape; the actual
    // projector lands in slice 4.
    let materialize_result = materialize(
        kind,
        &canonical_ledger_id,
        graph_iri,
        resolved_t,
        ctx,
    )
    .await;

    // Always pop active, even on error, so a failure deeper in the
    // chain doesn't leave stale entries that trip cycle detection
    // for unrelated subsequent calls.
    ctx.active.pop();

    let resolved = materialize_result?;
    let arc = Arc::new(resolved);
    ctx.memo.insert(tuple, arc.clone());
    Ok(arc)
}

/// Dispatch per `ArtifactKind` to the subsystem-specific projector.
///
/// Slice 3 placeholder — every kind returns
/// `TranslationFailed { detail: "not yet implemented" }`. Slice 4
/// fills in `PolicyRules`; later phases add the rest.
async fn materialize(
    kind: ArtifactKind,
    canonical_ledger_id: &str,
    graph_iri: &str,
    _resolved_t: i64,
    _ctx: &mut ResolveCtx<'_>,
) -> Result<ResolvedGraph, CrossLedgerError> {
    let _ = canonical_ledger_id;
    let _ = graph_iri;
    let _ = kind;
    Err(CrossLedgerError::TranslationFailed {
        ledger_id: canonical_ledger_id.to_string(),
        graph_iri: graph_iri.to_string(),
        detail: format!("artifact materialization for {kind:?} not yet implemented"),
    })
}

// The `GovernanceArtifact` enum currently has only the `PolicyRules`
// variant; the suppression below keeps the materialization placeholder
// honest about constructing one even while no caller does. It's
// removed once slice 4 lands an actual producer.
#[allow(dead_code)]
fn _governance_artifact_construction_guard(wire: fluree_db_policy::PolicyArtifactWire) {
    let _ = GovernanceArtifact::PolicyRules(wire);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cross_ledger::ResolveCtx;
    use crate::FlureeBuilder;
    use fluree_db_core::ledger_config::GraphSourceRef;

    fn local_ref(graph: &str) -> GraphSourceRef {
        GraphSourceRef {
            ledger: None,
            graph_selector: Some(graph.into()),
            at_t: None,
            trust_policy: None,
            rollback_guard: None,
        }
    }

    fn cross_ref(ledger: &str, graph: &str) -> GraphSourceRef {
        GraphSourceRef {
            ledger: Some(ledger.into()),
            graph_selector: Some(graph.into()),
            at_t: None,
            trust_policy: None,
            rollback_guard: None,
        }
    }

    #[tokio::test]
    async fn missing_ledger_field_is_a_call_site_bug() {
        let fluree = FlureeBuilder::memory().build_memory();
        let mut ctx = ResolveCtx::new("d:main", &fluree);

        let err = resolve_graph_ref(
            &local_ref("http://example.org/policy"),
            ArtifactKind::PolicyRules,
            &mut ctx,
        )
        .await
        .unwrap_err();

        match err {
            CrossLedgerError::TranslationFailed { detail, .. } => {
                assert!(detail.contains("same-ledger refs"));
            }
            other => panic!("expected TranslationFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unknown_model_ledger_returns_model_ledger_missing() {
        let fluree = FlureeBuilder::memory().build_memory();
        let mut ctx = ResolveCtx::new("d:main", &fluree);

        let err = resolve_graph_ref(
            &cross_ref("nope:main", "http://example.org/policy"),
            ArtifactKind::PolicyRules,
            &mut ctx,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, CrossLedgerError::ModelLedgerMissing { ref ledger_id } if ledger_id == "nope:main"),
            "expected ModelLedgerMissing(nope:main), got {err:?}"
        );
    }

    #[tokio::test]
    async fn trust_policy_field_is_rejected_until_phase_4() {
        let fluree = FlureeBuilder::memory().build_memory();
        let mut ctx = ResolveCtx::new("d:main", &fluree);

        let mut ref_with_trust = cross_ref("m:main", "http://example.org/policy");
        ref_with_trust.trust_policy = Some(fluree_db_core::ledger_config::TrustPolicy {
            trust_mode: fluree_db_core::ledger_config::TrustMode::Trusted,
        });

        let err = resolve_graph_ref(&ref_with_trust, ArtifactKind::PolicyRules, &mut ctx)
            .await
            .unwrap_err();
        assert!(
            matches!(err, CrossLedgerError::TrustCheckFailed { ref detail, .. } if detail.contains("Phase 4")),
            "expected Phase-4 TrustCheckFailed, got {err:?}"
        );
    }
}
