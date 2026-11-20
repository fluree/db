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
    check_cycle, memo_hit, reject_if_reserved_graph, ArtifactKind, GovernanceArtifact, ModelHead,
    ResolutionKey, ResolveCtx, ResolvedGraph,
};
use super::CrossLedgerError;
use fluree_db_core::ledger_config::GraphSourceRef;
use fluree_db_core::TimeSpec;
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
/// 4. Capture M's head lazily: read `ctx.resolved_heads[model_id]`
///    on hit, else read M's head `commit_t` and commit id once and
///    store. Pinned `f:atT` is rejected at step (1) until Phase 3.
/// 5. Form the resolution key
///    `(ArtifactKind, canonical_model_ledger_id, graph_iri, head)`
///    and check, in order: (a) `ctx.memo` (per-request
///    de-dup); (b) `fluree.governance_cache()` (per-instance, shared
///    across requests and across every data ledger that references
///    the same (M, graph, head)). On hit at either layer, return —
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
    // (0) `resolve_graph_ref` is the cross-ledger dispatcher. A
    // same-ledger ref hitting this path is a caller bug — fail with
    // `TranslationFailed` carrying the offending selector so the
    // error names the actual problem, rather than degrading into a
    // downstream `UnsupportedFeature` error whose `ledger_id` would
    // be empty.
    let raw_ledger_ref =
        graph_ref
            .ledger
            .as_deref()
            .ok_or_else(|| CrossLedgerError::TranslationFailed {
                ledger_id: String::new(),
                graph_iri: graph_ref.graph_selector.clone().unwrap_or_default(),
                detail: "resolve_graph_ref called without f:ledger; same-ledger refs \
                    must use the local resolver"
                    .into(),
            })?;

    // (1) Phase 3 / Phase 4 fields are parsed but not yet honored. Fail
    // closed — partial behavior (e.g., accepting f:atT and opening M
    // at that t with no retention check) would silently degrade the
    // contract operators think they're getting. `raw_ledger_ref` is
    // now guaranteed Some, so the error messages carry a real ledger
    // id.
    if graph_ref.at_t.is_some() {
        return Err(CrossLedgerError::UnsupportedFeature {
            feature: "f:atT",
            phase: "Phase 3",
            ledger_id: raw_ledger_ref.to_string(),
        });
    }
    if graph_ref.trust_policy.is_some() {
        return Err(CrossLedgerError::UnsupportedFeature {
            feature: "f:trustPolicy",
            phase: "Phase 4",
            ledger_id: raw_ledger_ref.to_string(),
        });
    }
    if graph_ref.rollback_guard.is_some() {
        return Err(CrossLedgerError::UnsupportedFeature {
            feature: "f:rollbackGuard",
            phase: "Phase 4",
            ledger_id: raw_ledger_ref.to_string(),
        });
    }

    let graph_iri =
        graph_ref
            .graph_selector
            .as_deref()
            .ok_or_else(|| CrossLedgerError::TranslationFailed {
                ledger_id: raw_ledger_ref.to_string(),
                graph_iri: String::new(),
                detail: "cross-ledger references require an explicit f:graphSelector".into(),
            })?;

    // (2) Canonicalize via the nameservice.
    let ns_record = model_record(raw_ledger_ref, graph_iri, ctx).await?;
    let canonical_ledger_id = ns_record.ledger_id.clone();

    // (3) Reserved-graph guard against the *canonical* id so an alias
    // can't slip a #config selector through by typing it for a
    // different alias spelling.
    reject_if_reserved_graph(&canonical_ledger_id, graph_iri)?;

    // (4) M's head: lazy per-request capture. f:atT pins are
    // rejected at step (1) above (Phase 3, not yet implemented), so
    // every cross-ledger reference resolves against M's current head
    // for the duration of the request. Per-model entries are written
    // once and reused; subsequent unpinned references to the same M
    // in the same request hit the cache.
    let head = captured_head(&ns_record, ctx);

    resolve_at(
        kind,
        canonical_ledger_id.as_str(),
        graph_iri,
        head,
        true,
        ctx,
    )
    .await
}

/// Resolve the graph a query's `rulesFrom` names, in the ledger it names,
/// at its pin (the head when it has none), to the rules stored there.
///
/// It takes the path a configured cross-ledger `f:rulesSource` takes, with
/// the query's pin: the ledger is canonicalized through the nameservice and
/// refused as missing like a configured model ledger, a system graph is
/// refused, and the read is memoized and cached under the head it resolved
/// to. An unpinned read shares the request's head capture for that ledger,
/// so policy and rules read one head of it. A pinned read resolves its own
/// ([`pinned_head`]) and leaves the capture alone.
pub(crate) async fn resolve_rules_from(
    rules_from: &fluree_db_query::ir::RulesFrom,
    ctx: &mut ResolveCtx<'_>,
) -> crate::error::Result<Arc<ResolvedGraph>> {
    let graph_iri = rules_from.graph_iri();
    let ns_record = model_record(rules_from.ledger().as_str(), graph_iri, ctx).await?;
    let canonical_ledger_id = ns_record.ledger_id.clone();
    reject_if_reserved_graph(&canonical_ledger_id, graph_iri)?;
    let (head, cacheable) = match rules_from.at() {
        None | Some(TimeSpec::Latest) => (captured_head(&ns_record, ctx), true),
        Some(spec) => {
            let head = pinned_head(ctx, &canonical_ledger_id, spec).await?;
            // Without a commit the key names a `t` alone, which a later
            // rebase can give other contents: read it, but don't keep it
            // past this request.
            let cacheable = head.commit_id.is_some();
            (head, cacheable)
        }
    };
    Ok(resolve_at(
        ArtifactKind::Rules,
        canonical_ledger_id.as_str(),
        graph_iri,
        head,
        cacheable,
        ctx,
    )
    .await?)
}

/// The nameservice record of a model ledger named `raw_ledger_ref`.
/// Same-instance is enforced implicitly: anything outside our nameservice
/// can't be looked up. (Cross-instance federation would surface here as a
/// separate resolver path; Phase next.)
async fn model_record(
    raw_ledger_ref: &str,
    graph_iri: &str,
    ctx: &ResolveCtx<'_>,
) -> Result<NsRecord, CrossLedgerError> {
    match ctx.fluree.nameservice().lookup(raw_ledger_ref).await {
        Ok(Some(record)) if !record.retracted => Ok(record),
        Ok(_) => Err(CrossLedgerError::ModelLedgerMissing {
            ledger_id: raw_ledger_ref.to_string(),
        }),
        Err(e) => Err(CrossLedgerError::TranslationFailed {
            ledger_id: raw_ledger_ref.to_string(),
            graph_iri: graph_iri.to_string(),
            detail: format!("nameservice lookup failed: {e}"),
        }),
    }
}

/// The request's head for a model ledger: captured on its first reference
/// and reused by every later unpinned one.
fn captured_head(ns_record: &NsRecord, ctx: &mut ResolveCtx<'_>) -> ModelHead {
    ctx.resolved_heads
        .entry(ns_record.ledger_id.clone())
        .or_insert_with(|| ModelHead {
            t: ns_record.commit_t,
            commit_id: ns_record.commit_head_id.clone(),
        })
        .clone()
}

/// The head a pin names on a model ledger: the `t` it resolves to against
/// the ledger's current state (`@t:` as given, `@commit:` its commit's `t`,
/// `@time:` / `@recorded:` the last commit at or before that time), and the
/// commit at that `t`. A `t` past the head is refused, as reading a ledger
/// there is. The commit is left out, rather than guessed, where the history
/// does not name one commit at that `t` (a rebased ledger can hold two).
async fn pinned_head(
    ctx: &ResolveCtx<'_>,
    canonical_ledger_id: &fluree_db_core::LedgerId,
    spec: &TimeSpec,
) -> crate::error::Result<ModelHead> {
    let handle = ctx
        .fluree
        .ledger_cached(canonical_ledger_id.as_str())
        .await?;
    let view = handle.snapshot().await;
    let (head_t, head_commit) = (view.t, view.head_commit_id.clone());
    let state = view.to_ledger_state();
    let t = crate::time_resolve::resolve_time_spec(&state, spec).await?;
    if t > head_t {
        return Err(crate::error::ApiError::Ledger(
            fluree_db_ledger::LedgerError::future_time(canonical_ledger_id.as_str(), t, head_t),
        ));
    }
    if t < 1 {
        return Err(crate::error::ApiError::invalid_query(format!(
            "rulesFrom pins ledger '{canonical_ledger_id}' at t={t}; a ledger's first commit \
             is t=1"
        )));
    }
    if t == head_t {
        return Ok(ModelHead {
            t,
            commit_id: head_commit,
        });
    }
    let commit_id =
        crate::ledger_view::resolve_t_to_commit_id(&state.snapshot, &*state.novelty, t, head_t)
            .await
            .map_err(|e| tracing::debug!(t, error = %e, "rulesFrom pin names no single commit"))
            .ok();
    Ok(ModelHead { t, commit_id })
}

/// Resolve `graph_iri` of the canonical model ledger at `head` to a `kind`
/// artifact: memo, governance cache, cycle check, materializer. `cacheable`
/// false keeps the result out of the per-instance governance cache (it is
/// still memoized for the request).
async fn resolve_at(
    kind: ArtifactKind,
    canonical_ledger_id: &str,
    graph_iri: &str,
    head: ModelHead,
    cacheable: bool,
    ctx: &mut ResolveCtx<'_>,
) -> Result<Arc<ResolvedGraph>, CrossLedgerError> {
    let key: ResolutionKey = (
        kind,
        canonical_ledger_id.to_string(),
        graph_iri.to_string(),
        head.clone(),
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
    if let Some(hit) = cacheable
        .then(|| ctx.fluree.governance_cache().get(&key))
        .flatten()
    {
        ctx.memo.insert(key.clone(), hit.clone());
        return Ok(hit);
    }

    // (6) Cycle check.
    check_cycle(&ctx.active, &key)?;

    ctx.active.push(key.clone());

    // Materialize. Slice 3 provides the dispatch shape; the actual
    // projector lands in slice 4.
    let materialize_result = materialize(kind, canonical_ledger_id, graph_iri, &head, ctx).await;

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
    //
    // Benign read/write race: two concurrent requests can both
    // miss at (5b), each materialize independently, and each
    // write its own `Arc`. The cache ends up holding whichever
    // write completed last; the loser's `Arc` is dropped when
    // its request finishes. Wire artifacts are pure functions of
    // `(kind, model_ledger, graph, head)`, so the two
    // materialized values are structurally identical — duplicate
    // work, not duplicate semantics. Single-flight is a possible
    // future optimization but not a correctness requirement.
    ctx.memo.insert(key.clone(), arc.clone());
    if cacheable {
        ctx.fluree.governance_cache().insert(key, arc.clone());
    }
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
    head: &ModelHead,
    ctx: &mut ResolveCtx<'_>,
) -> Result<ResolvedGraph, CrossLedgerError> {
    match kind {
        ArtifactKind::PolicyRules => {
            let wire = super::policy_materializer::materialize_policy_rules(
                canonical_ledger_id,
                graph_iri,
                head,
                ctx,
            )
            .await?;
            Ok(ResolvedGraph {
                model_ledger_id: canonical_ledger_id.to_string(),
                graph_iri: graph_iri.to_string(),
                resolved_t: head.t,
                commit_id: head.commit_id.clone(),
                artifact: GovernanceArtifact::PolicyRules(wire),
            })
        }
        ArtifactKind::Constraints => {
            let wire = super::constraints_materializer::materialize_constraints(
                canonical_ledger_id,
                graph_iri,
                head,
                ctx,
            )
            .await?;
            Ok(ResolvedGraph {
                model_ledger_id: canonical_ledger_id.to_string(),
                graph_iri: graph_iri.to_string(),
                resolved_t: head.t,
                commit_id: head.commit_id.clone(),
                artifact: GovernanceArtifact::Constraints(wire),
            })
        }
        ArtifactKind::SchemaClosure => {
            let wire = super::schema_materializer::materialize_schema(
                canonical_ledger_id,
                graph_iri,
                head,
                ctx,
            )
            .await?;
            Ok(ResolvedGraph {
                model_ledger_id: canonical_ledger_id.to_string(),
                graph_iri: graph_iri.to_string(),
                resolved_t: head.t,
                commit_id: head.commit_id.clone(),
                artifact: GovernanceArtifact::SchemaClosure(wire),
            })
        }
        ArtifactKind::Shapes => {
            let wire = super::shapes_materializer::materialize_shapes(
                canonical_ledger_id,
                graph_iri,
                head,
                ctx,
            )
            .await?;
            Ok(ResolvedGraph {
                model_ledger_id: canonical_ledger_id.to_string(),
                graph_iri: graph_iri.to_string(),
                resolved_t: head.t,
                commit_id: head.commit_id.clone(),
                artifact: GovernanceArtifact::Shapes(wire),
            })
        }
        ArtifactKind::Rules => {
            let wire = super::rules_materializer::materialize_rules(
                canonical_ledger_id,
                graph_iri,
                head,
                ctx,
            )
            .await?;
            Ok(ResolvedGraph {
                model_ledger_id: canonical_ledger_id.to_string(),
                graph_iri: graph_iri.to_string(),
                resolved_t: head.t,
                commit_id: head.commit_id.clone(),
                artifact: GovernanceArtifact::Rules(wire),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(s: &str) -> fluree_db_core::LedgerId {
        fluree_db_core::LedgerId::parse(s).unwrap()
    }
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
        let data_id = id("d:main");
        let mut ctx = ResolveCtx::new(&data_id, &fluree);

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
        let data_id = id("d:main");
        let mut ctx = ResolveCtx::new(&data_id, &fluree);

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
        let data_id = id("d:main");
        let mut ctx = ResolveCtx::new(&data_id, &fluree);

        let mut pinned = cross_ref("m:main", "http://example.org/policy");
        pinned.at_t = Some(5);

        let err = resolve_graph_ref(&pinned, ArtifactKind::PolicyRules, &mut ctx)
            .await
            .unwrap_err();
        match err {
            CrossLedgerError::UnsupportedFeature { feature, phase, .. } => {
                assert_eq!(feature, "f:atT");
                assert_eq!(phase, "Phase 3");
            }
            other => panic!("expected UnsupportedFeature for f:atT, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn trust_policy_field_is_rejected_until_phase_4() {
        let fluree = FlureeBuilder::memory().build_memory();
        let data_id = id("d:main");
        let mut ctx = ResolveCtx::new(&data_id, &fluree);

        let mut ref_with_trust = cross_ref("m:main", "http://example.org/policy");
        ref_with_trust.trust_policy = Some(fluree_db_core::ledger_config::TrustPolicy {
            trust_mode: fluree_db_core::ledger_config::TrustMode::Trusted,
        });

        let err = resolve_graph_ref(&ref_with_trust, ArtifactKind::PolicyRules, &mut ctx)
            .await
            .unwrap_err();
        match err {
            CrossLedgerError::UnsupportedFeature { feature, phase, .. } => {
                assert_eq!(feature, "f:trustPolicy");
                assert_eq!(phase, "Phase 4");
            }
            other => panic!("expected UnsupportedFeature for f:trustPolicy, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn rollback_guard_field_is_rejected_until_phase_4() {
        let fluree = FlureeBuilder::memory().build_memory();
        let data_id = id("d:main");
        let mut ctx = ResolveCtx::new(&data_id, &fluree);

        let mut ref_with_guard = cross_ref("m:main", "http://example.org/policy");
        ref_with_guard.rollback_guard =
            Some(fluree_db_core::ledger_config::RollbackGuard { min_t: Some(100) });

        let err = resolve_graph_ref(&ref_with_guard, ArtifactKind::PolicyRules, &mut ctx)
            .await
            .unwrap_err();
        match err {
            CrossLedgerError::UnsupportedFeature { feature, phase, .. } => {
                assert_eq!(feature, "f:rollbackGuard");
                assert_eq!(phase, "Phase 4");
            }
            other => panic!("expected UnsupportedFeature for f:rollbackGuard, got {other:?}"),
        }
    }
}
