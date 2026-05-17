//! Cross-ledger resolver entry point.
//!
//! The single helper shared by every subsystem (policy / shapes /
//! schema / rules / constraints). Materialization is dispatched per
//! [`ArtifactKind`]; only `PolicyRules` is implemented in Phase 1a
//! and dispatches into `policy_materializer`. Other kinds will return
//! `CrossLedgerError::TranslationFailed` until their projector lands
//! in a later phase.
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
/// 1. Reject Phase-3 and Phase-4 fields (`f:atT`, `f:trustPolicy`,
///    `f:rollbackGuard`) with [`CrossLedgerError::UnsupportedFeature`];
///    validate this is a cross-ledger ref (`f:ledger` and
///    `f:graphSelector` both set).
/// 2. Canonicalize the model ledger id via the nameservice
///    (`nameservice.lookup`); fail with `ModelLedgerMissing` if the
///    ledger is absent or retracted on this instance.
/// 3. Reject `#config` / `#txn-meta` selectors before any storage
///    round-trip on M.
/// 4. Capture `resolved_t` lazily: read `ctx.resolved_ts[model_id]`
///    on hit, else read M's head `commit_t` once and store. Pinned
///    `f:atT` is rejected at step (1) until Phase 3.
/// 5. Form the resolution key
///    `(ArtifactKind, canonical_model_ledger_id, graph_iri,
///    resolved_t)` and check, in order: (a) `ctx.memo` (per-request
///    de-dup); (b) `fluree.governance_cache()` (per-instance, shared
///    across requests and across every data ledger that references
///    the same (M, graph, t)). On hit at either layer, return —
///    cross-subsystem de-dup runs before cycle detection, and a
///    governance-cache hit is also folded into `ctx.memo` so later
///    calls in the same request short-circuit at (a).
/// 6. Check `ctx.active` for cycles. On miss, push and call into
///    the per-kind materializer.
/// 7. On materializer success, pop `active`, insert into both
///    `ctx.memo` and `fluree.governance_cache()`, and return. On
///    failure, pop `active` so a deeper failure doesn't poison
///    subsequent calls.
pub async fn resolve_graph_ref(
    graph_ref: &GraphSourceRef,
    kind: ArtifactKind,
    ctx: &mut ResolveCtx<'_>,
) -> Result<Arc<ResolvedGraph>, CrossLedgerError> {
    // (1) Phase 3 / Phase 4 fields are parsed but not yet honored. Fail
    // closed — partial behavior (e.g., accepting f:atT and opening M
    // at that t with no retention check) would silently degrade the
    // contract operators think they're getting.
    if graph_ref.at_t.is_some() {
        return Err(CrossLedgerError::UnsupportedFeature {
            feature: "f:atT",
            phase: "Phase 3",
            ledger_id: graph_ref.ledger.clone().unwrap_or_default(),
        });
    }
    if graph_ref.trust_policy.is_some() {
        return Err(CrossLedgerError::UnsupportedFeature {
            feature: "f:trustPolicy",
            phase: "Phase 4",
            ledger_id: graph_ref.ledger.clone().unwrap_or_default(),
        });
    }
    if graph_ref.rollback_guard.is_some() {
        return Err(CrossLedgerError::UnsupportedFeature {
            feature: "f:rollbackGuard",
            phase: "Phase 4",
            ledger_id: graph_ref.ledger.clone().unwrap_or_default(),
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

    // (4) resolved_t: lazy per-request capture. f:atT pins are
    // rejected at step (1) above (Phase 3, not yet implemented), so
    // every cross-ledger reference resolves against M's current head
    // for the duration of the request. Per-model entries are written
    // once and reused; subsequent unpinned references to the same M
    // in the same request hit the cache.
    let resolved_t = if let Some(t) = ctx.resolved_ts.get(&canonical_ledger_id) {
        *t
    } else {
        let head_t = ns_record.commit_t;
        ctx.resolved_ts
            .insert(canonical_ledger_id.clone(), head_t);
        head_t
    };

    let key: (ArtifactKind, String, String, i64) = (
        kind,
        canonical_ledger_id.clone(),
        graph_iri.to_string(),
        resolved_t,
    );

    // (5a) Memo hit — short-circuit cross-subsystem de-dup before
    // entering `active`, so two subsystems referencing the same
    // (kind, M, graph, t) never look like a cycle to each other.
    // ArtifactKind is part of the key: a memoized PolicyRules entry
    // never short-circuits a Shapes lookup for the same graph.
    if let Some(hit) = memo_hit(&ctx.memo, &key) {
        return Ok(hit);
    }

    // (5b) Per-instance governance cache hit — shareable across
    // requests and across every data ledger on this instance that
    // references the same (M, graph, t). Writeback below on miss.
    // The per-request memo is populated alongside so subsequent
    // resolutions in this same request short-circuit at (5a).
    if let Some(hit) = ctx.fluree.governance_cache().get(&key) {
        ctx.memo.insert(key.clone(), hit.clone());
        return Ok(hit);
    }

    // (6) Cycle check.
    check_cycle(&ctx.active, &key)?;

    ctx.active.push(key.clone());

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
    // Write back to both the per-request memo and the per-instance
    // cache. Subsequent calls in this same request hit the memo;
    // subsequent calls in *other* requests on this instance hit
    // the governance cache.
    ctx.memo.insert(key.clone(), arc.clone());
    ctx.fluree.governance_cache().insert(key, arc.clone());
    Ok(arc)
}

/// Dispatch per `ArtifactKind` to the subsystem-specific projector.
///
/// Phase 1a implements `PolicyRules`; other kinds land in later
/// phases per the design doc's phasing table.
async fn materialize(
    kind: ArtifactKind,
    canonical_ledger_id: &str,
    graph_iri: &str,
    resolved_t: i64,
    ctx: &mut ResolveCtx<'_>,
) -> Result<ResolvedGraph, CrossLedgerError> {
    match kind {
        ArtifactKind::PolicyRules => {
            let wire = super::policy_materializer::materialize_policy_rules(
                canonical_ledger_id,
                graph_iri,
                resolved_t,
                ctx.fluree,
            )
            .await?;
            Ok(ResolvedGraph {
                model_ledger_id: canonical_ledger_id.to_string(),
                graph_iri: graph_iri.to_string(),
                resolved_t,
                artifact: GovernanceArtifact::PolicyRules(wire),
            })
        }
        ArtifactKind::Constraints => {
            let wire = super::constraints_materializer::materialize_constraints(
                canonical_ledger_id,
                graph_iri,
                resolved_t,
                ctx.fluree,
            )
            .await?;
            Ok(ResolvedGraph {
                model_ledger_id: canonical_ledger_id.to_string(),
                graph_iri: graph_iri.to_string(),
                resolved_t,
                artifact: GovernanceArtifact::Constraints(wire),
            })
        }
        ArtifactKind::SchemaClosure => {
            let wire = super::schema_materializer::materialize_schema(
                canonical_ledger_id,
                graph_iri,
                resolved_t,
                ctx.fluree,
            )
            .await?;
            Ok(ResolvedGraph {
                model_ledger_id: canonical_ledger_id.to_string(),
                graph_iri: graph_iri.to_string(),
                resolved_t,
                artifact: GovernanceArtifact::SchemaClosure(wire),
            })
        }
        ArtifactKind::Shapes => {
            let wire = super::shapes_materializer::materialize_shapes(
                canonical_ledger_id,
                graph_iri,
                resolved_t,
                ctx.fluree,
            )
            .await?;
            Ok(ResolvedGraph {
                model_ledger_id: canonical_ledger_id.to_string(),
                graph_iri: graph_iri.to_string(),
                resolved_t,
                artifact: GovernanceArtifact::Shapes(wire),
            })
        }
    }
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
    async fn at_t_pin_is_rejected_until_phase_3() {
        // f:atT is parsed by the config layer but Phase 3 hasn't
        // landed the retention/pinning semantics yet. Accepting the
        // pin and using it as the resolved_t would silently bypass
        // the "no fallback to nearest-available" retention guard
        // the design doc requires, so resolve_graph_ref must reject.
        let fluree = FlureeBuilder::memory().build_memory();
        let mut ctx = ResolveCtx::new("d:main", &fluree);

        let mut pinned = cross_ref("m:main", "http://example.org/policy");
        pinned.at_t = Some(5);

        let err = resolve_graph_ref(&pinned, ArtifactKind::PolicyRules, &mut ctx)
            .await
            .unwrap_err();
        match err {
            CrossLedgerError::UnsupportedFeature {
                feature, phase, ..
            } => {
                assert_eq!(feature, "f:atT");
                assert_eq!(phase, "Phase 3");
            }
            other => panic!("expected UnsupportedFeature for f:atT, got {other:?}"),
        }
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
        match err {
            CrossLedgerError::UnsupportedFeature {
                feature, phase, ..
            } => {
                assert_eq!(feature, "f:trustPolicy");
                assert_eq!(phase, "Phase 4");
            }
            other => panic!("expected UnsupportedFeature for f:trustPolicy, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn rollback_guard_field_is_rejected_until_phase_4() {
        let fluree = FlureeBuilder::memory().build_memory();
        let mut ctx = ResolveCtx::new("d:main", &fluree);

        let mut ref_with_guard = cross_ref("m:main", "http://example.org/policy");
        ref_with_guard.rollback_guard = Some(fluree_db_core::ledger_config::RollbackGuard {
            min_t: Some(100),
        });

        let err = resolve_graph_ref(&ref_with_guard, ArtifactKind::PolicyRules, &mut ctx)
            .await
            .unwrap_err();
        match err {
            CrossLedgerError::UnsupportedFeature {
                feature, phase, ..
            } => {
                assert_eq!(feature, "f:rollbackGuard");
                assert_eq!(phase, "Phase 4");
            }
            other => panic!("expected UnsupportedFeature for f:rollbackGuard, got {other:?}"),
        }
    }
}
