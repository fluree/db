//! SHACL validation for branch operations.
//!
//! Merge, rebase, revert, and merge preview build their commits on a path
//! parallel to the transaction pipeline, staging flakes straight into a
//! [`StagedLedger`]. This module runs the same post-stage validation the
//! transaction path runs (`apply_shacl_policy_to_staged_view`), so a branch
//! operation whose resulting state the ledger's shapes reject fails exactly
//! as a transaction producing that state would.
//!
//! A branch operation is authoring, not replay: the combination it stages
//! onto the target is new state nobody has validated. So, unlike commit
//! replay, a cross-ledger `f:shapesSource` is resolved and enforced here
//! rather than skipped. Branch operations carry no request surface (no
//! requested validation mode, identity, inline shapes, or authoring context)
//! and run the ledger's configured posture.

use crate::error::{ApiError, Result};
use crate::rebase::ConflictStrategy;
use fluree_db_core::{ConflictKey, Flake, GraphId, Sid};
use fluree_db_ledger::{LedgerState, StagedLedger};
use std::collections::{BTreeSet, HashMap};

/// Outcome of validating a branch operation's staged view.
///
/// Warn-mode violations are logged by the validator and never surface here,
/// matching the transaction path. Only reject-mode violations produce a
/// report.
#[derive(Clone, Debug, Default)]
pub(crate) struct BranchOpValidation {
    /// Formatted violation report, `None` when the view conforms.
    pub(crate) report: Option<String>,
}

impl BranchOpValidation {
    pub(crate) fn conforms(&self) -> bool {
        self.report.is_none()
    }

    /// Turn a rejected outcome into the error a transaction producing the
    /// same state would return.
    pub(crate) fn into_result(self) -> Result<()> {
        self.into_result_with(|report| report)
    }

    /// Like [`Self::into_result`], with `describe` wrapping the report in
    /// the operation's own context (which commit a rebase stopped on, say).
    pub(crate) fn into_result_with(self, describe: impl FnOnce(String) -> String) -> Result<()> {
        match self.report {
            None => Ok(()),
            #[cfg(feature = "shacl")]
            Some(report) => {
                Err(fluree_db_transact::TransactError::ShaclViolation(describe(report)).into())
            }
            // Without the feature no validator runs, so no report is ever
            // produced; keep the match total without naming a variant that
            // does not exist in this configuration.
            #[cfg(not(feature = "shacl"))]
            Some(report) => Err(crate::error::ApiError::internal(format!(
                "SHACL violation reported without the shacl feature: {}",
                describe(report)
            ))),
        }
    }
}

/// Named graphs in `graph_iris` that `state` has not registered, as
/// provisional graph id -> (graph Sid, IRI): the id the commit's registration
/// will assign. The Sid is resolved against the state's namespaces plus
/// `namespace_delta`, the codes the incoming commits introduced, so it
/// matches the Sid their flakes carry.
fn unregistered_graphs(
    state: &LedgerState,
    namespace_delta: &HashMap<u16, String>,
    graph_iris: &BTreeSet<String>,
) -> Result<HashMap<GraphId, (Sid, String)>> {
    let registry = &state.snapshot.graph_registry;
    let new: Vec<String> = graph_iris
        .iter()
        .filter(|iri| registry.graph_id_for_iri(iri).is_none())
        .cloned()
        .collect();
    if new.is_empty() {
        return Ok(HashMap::new());
    }
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&state.snapshot);
    ns.adopt_delta_for_persistence(namespace_delta)
        .map_err(|e| {
            ApiError::BranchConflict(format!(
                "the incoming commits' namespace allocations conflict with the target's: {e}"
            ))
        })?;
    let ids = registry.provisional_ids(&new);
    Ok(new
        .into_iter()
        .filter_map(|iri| {
            let g_id = *ids.get(iri.as_str())?;
            let sid = ns.lookup_sid_for_iri(&iri)?;
            Some((g_id, (sid, iri)))
        })
        .collect())
}

impl crate::Fluree {
    /// Stage `flakes` onto `state` and validate the result against the
    /// ledger's SHACL configuration and shapes.
    ///
    /// This is the one way a branch operation builds a view to commit: the
    /// staging and the validation travel together so no operation can copy
    /// one without the other. Returns the validated view and the outcome;
    /// the caller decides what a rejection means (merge and revert fail,
    /// rebase names the commit it stopped on, preview reports it).
    ///
    /// `namespace_delta` holds the namespace codes the operation introduces,
    /// which the snapshot will not carry until the commit lands; they make
    /// the operation's own terms encodable for `sh:sparql` lowering and
    /// resolvable in messages. `op` names the operation in error messages.
    pub(crate) async fn stage_validated(
        &self,
        state: LedgerState,
        flakes: Vec<Flake>,
        namespace_delta: &HashMap<u16, String>,
        graph_iris: &BTreeSet<String>,
        op: &'static str,
    ) -> Result<(StagedLedger, BranchOpValidation)> {
        let mut reverse_graph = state.snapshot.build_reverse_graph().map_err(|e| {
            ApiError::internal(format!("Failed to build reverse graph during {op}: {e}"))
        })?;
        // Graphs the incoming commits created that this state has not
        // registered, routed by the id the commit will give them.
        let new_graphs = unregistered_graphs(&state, namespace_delta, graph_iris)?;
        for (g_id, (sid, _)) in &new_graphs {
            reverse_graph.insert(sid.clone(), *g_id);
        }
        let mut view = StagedLedger::new(state, flakes, &reverse_graph)
            .map_err(|e| ApiError::internal(format!("Failed to stage flakes during {op}: {e}")))?;
        let new_graph_iris: HashMap<GraphId, String> = new_graphs
            .into_iter()
            .map(|(g_id, (_, iri))| (g_id, iri))
            .collect();
        let outcome = self
            .validate_branch_op_view(&mut view, namespace_delta, &new_graph_iris)
            .await?;
        Ok((view, outcome))
    }

    /// The merge's staging, shared by the merge itself and its preview:
    /// resolve `source_flakes` under `strategy` against `target_state`, then
    /// stage and validate the result.
    pub(crate) async fn stage_merge(
        &self,
        target_state: LedgerState,
        source_flakes: Vec<Flake>,
        conflicts: &[ConflictKey],
        strategy: &ConflictStrategy,
        namespace_delta: &HashMap<u16, String>,
        graph_iris: &BTreeSet<String>,
    ) -> Result<(StagedLedger, BranchOpValidation)> {
        let resolved = self
            .apply_two_way_strategy(source_flakes, conflicts, strategy, &target_state)
            .await?;
        self.stage_validated(target_state, resolved, namespace_delta, graph_iris, "merge")
            .await
    }

    /// Revert's staging, shared by the revert itself and its preview:
    /// resolve `inverted` under `strategy` against `target_state`, then stage
    /// and validate the result.
    ///
    /// `None` when the strategy leaves nothing to apply, such as
    /// `TakeBranch` with full overlap. There is no commit to build then, and
    /// nothing that could be rejected.
    pub(crate) async fn stage_revert(
        &self,
        target_state: LedgerState,
        inverted: Vec<Flake>,
        conflicts: &[ConflictKey],
        strategy: &ConflictStrategy,
        namespace_delta: &HashMap<u16, String>,
        graph_iris: &BTreeSet<String>,
    ) -> Result<Option<(StagedLedger, BranchOpValidation)>> {
        let staged = self
            .apply_two_way_strategy(inverted, conflicts, strategy, &target_state)
            .await?;
        if staged.is_empty() {
            return Ok(None);
        }
        self.stage_validated(target_state, staged, namespace_delta, graph_iris, "revert")
            .await
            .map(Some)
    }

    /// Validate a staged view against the target ledger's SHACL
    /// configuration and shapes. `new_graph_iris` names graphs the incoming
    /// commits create, which the target's registry does not know yet.
    #[cfg(feature = "shacl")]
    async fn validate_branch_op_view(
        &self,
        view: &mut StagedLedger,
        namespace_delta: &HashMap<u16, String>,
        new_graph_iris: &HashMap<GraphId, String>,
    ) -> Result<BranchOpValidation> {
        use crate::tx::{
            apply_shacl_policy_to_staged_view, open_cross_ledger_shapes_model,
            resolve_cross_ledger_schema_for_tx, StagedShaclContext,
        };
        use fluree_db_transact::{NamespaceRegistry, TransactError};

        if !view.has_staged() {
            return Ok(BranchOpValidation::default());
        }

        let base = view.base();
        let ledger_id = base.snapshot.ledger_id.to_string();

        // Config from the target's pre-operation state, resolved once and
        // shared by the cross-ledger resolvers and the policy pass.
        let config = crate::config_resolver::resolve_ledger_config(
            &base.snapshot,
            base.novelty.as_ref(),
            base.t(),
        )
        .await
        .map_err(|e| {
            ApiError::internal(format!(
                "failed to load ledger config for branch-operation validation: {e}"
            ))
        })?;
        let tx_config = config.clone().map(std::sync::Arc::new);

        let mut resolve_ctx = crate::cross_ledger::ResolveCtx::new(&ledger_id, self);
        let cross_ledger_shapes =
            open_cross_ledger_shapes_model(config.as_ref(), &mut resolve_ctx).await?;
        let cross_ledger_schema =
            resolve_cross_ledger_schema_for_tx(base, config.as_ref(), &mut resolve_ctx).await?;

        // The staged namespace registry: the snapshot's codes plus the ones
        // this operation brings in.
        // Sibling branches allocate codes independently, so a code or prefix
        // the source introduced can already mean something else on the
        // target. The commit builder rejects such a merge too; surfacing it
        // here, as a conflict rather than an internal error, lets a preview
        // answer instead of failing.
        let mut staged_ns = NamespaceRegistry::from_db(&base.snapshot);
        staged_ns
            .adopt_delta_for_persistence(namespace_delta)
            .map_err(|e| {
                ApiError::BranchConflict(format!(
                    "the source branch's namespace allocations conflict with the target's: {e}"
                ))
            })?;
        let cross_ledger_data_ns_map = cross_ledger_shapes
            .as_ref()
            .map(|_| crate::tx::namespace_prefix_map(&staged_ns));
        let cross_ledger_membership = cross_ledger_shapes
            .as_ref()
            .zip(cross_ledger_data_ns_map.as_ref())
            .map(|(model, ns_map)| model.membership(ns_map));

        // Graph routing for the staged flakes: every named graph they touch,
        // with its IRI for per-graph config resolution. The default graph
        // always gets the ledger-wide policy.
        let mut graph_delta: rustc_hash::FxHashMap<u16, String> = rustc_hash::FxHashMap::default();
        for (g_id, _) in view.staged_flakes_by_graph().filter(|(g_id, _)| *g_id != 0) {
            let iri = base
                .snapshot
                .graph_registry
                .iri_for_graph_id(g_id)
                .map(str::to_string)
                .or_else(|| new_graph_iris.get(&g_id).cloned());
            if let Some(iri) = iri {
                graph_delta.entry(g_id).or_insert(iri);
            }
        }

        let ctx = StagedShaclContext {
            graph_delta: Some(&graph_delta),
            tracker: None,
            cross_ledger_shapes: cross_ledger_shapes.as_ref().and_then(|m| m.wire()),
            staged_ns: Some(&staged_ns),
            uncommitted_namespaces: Some(namespace_delta),
            txn_context: None,
            inline_shape_bundle: None,
            cross_ledger_schema,
            cross_ledger_membership,
            requested_validation_mode: None,
            request_identity: None,
            origin_validated_replay: false,
        };

        // The flag says whether validation actually ran, which the
        // transaction path threads into commit provenance. A branch
        // operation only needs to know whether anything rejected it, and a
        // pass that was skipped (SHACL disabled, or no shapes) rejects
        // nothing.
        match apply_shacl_policy_to_staged_view(view, ctx, tx_config).await {
            Ok(_ran) => Ok(BranchOpValidation::default()),
            Err(TransactError::ShaclViolation(report)) => Ok(BranchOpValidation {
                report: Some(report),
            }),
            Err(e) => Err(e.into()),
        }
    }

    /// Without the `shacl` feature there is nothing to validate against.
    #[cfg(not(feature = "shacl"))]
    async fn validate_branch_op_view(
        &self,
        _view: &mut StagedLedger,
        _namespace_delta: &HashMap<u16, String>,
        _new_graph_iris: &HashMap<GraphId, String>,
    ) -> Result<BranchOpValidation> {
        Ok(BranchOpValidation::default())
    }
}
