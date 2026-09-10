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
//! rather than skipped. Branch operations carry no request surface — no
//! requested validation mode, identity, inline shapes, or authoring context
//! — and run the ledger's configured posture.

use crate::error::Result;
use fluree_db_core::{GraphId, Sid};
use fluree_db_ledger::StagedLedger;
use std::collections::HashMap;

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
        match self.report {
            None => Ok(()),
            #[cfg(feature = "shacl")]
            Some(report) => Err(fluree_db_transact::TransactError::ShaclViolation(report).into()),
            // Without the feature no validator runs, so no report is ever
            // produced; keep the match total without naming a variant that
            // does not exist in this configuration.
            #[cfg(not(feature = "shacl"))]
            Some(report) => Err(crate::error::ApiError::internal(format!(
                "SHACL violation reported without the shacl feature: {report}"
            ))),
        }
    }
}

impl crate::Fluree {
    /// Validate a branch operation's staged view against the target ledger's
    /// SHACL configuration and shapes.
    ///
    /// `reverse_graph` is the map the view was built with. `namespace_delta`
    /// holds the namespace codes the operation introduces, which the snapshot
    /// will not carry until the commit lands; they make the operation's own
    /// terms encodable for `sh:sparql` lowering and resolvable in messages.
    #[cfg(feature = "shacl")]
    pub(crate) async fn validate_branch_op_view(
        &self,
        view: &mut StagedLedger,
        reverse_graph: &HashMap<Sid, GraphId>,
        namespace_delta: &HashMap<u16, String>,
    ) -> Result<BranchOpValidation> {
        use crate::error::ApiError;
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
        let mut staged_ns = NamespaceRegistry::from_db(&base.snapshot);
        staged_ns
            .adopt_delta_for_persistence(namespace_delta)
            .map_err(|e| {
                ApiError::internal(format!(
                    "branch-operation namespace delta conflicts with the target registry: {e}"
                ))
            })?;
        let cross_ledger_data_ns_map: Option<HashMap<u16, String>> =
            cross_ledger_shapes.as_ref().map(|_| {
                staged_ns
                    .all_codes()
                    .into_iter()
                    .filter_map(|code| staged_ns.get_prefix(code).map(|p| (code, p.to_string())))
                    .collect()
            });
        let cross_ledger_membership = match (&cross_ledger_shapes, &cross_ledger_data_ns_map) {
            (Some(model), Some(ns_map)) => Some(fluree_db_shacl::CrossLedgerMembership {
                model_db: fluree_db_core::GraphDbRef::new(
                    &model.model_db.snapshot,
                    model.model_g_id,
                    model.model_db.overlay.as_ref(),
                    model.model_db.t,
                ),
                data_ns_map: ns_map,
                same_term_space: false,
            }),
            _ => None,
        };

        // Graph routing for the staged flakes: every named graph they touch,
        // with its IRI for per-graph config resolution. The default graph
        // always gets the ledger-wide policy.
        let graph_sids: HashMap<GraphId, Sid> = reverse_graph
            .iter()
            .map(|(sid, g_id)| (*g_id, sid.clone()))
            .collect();
        let mut graph_delta: rustc_hash::FxHashMap<u16, String> = rustc_hash::FxHashMap::default();
        for g_sid in view.staged_flakes().iter().filter_map(|f| f.g.as_ref()) {
            if let Some(&g_id) = reverse_graph.get(g_sid) {
                if let Some(iri) = base.snapshot.graph_registry.iri_for_graph_id(g_id) {
                    graph_delta.entry(g_id).or_insert_with(|| iri.to_string());
                }
            }
        }

        let ctx = StagedShaclContext {
            graph_delta: Some(&graph_delta),
            graph_sids: Some(&graph_sids),
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

        match apply_shacl_policy_to_staged_view(view, ctx, tx_config).await {
            Ok(()) => Ok(BranchOpValidation::default()),
            Err(TransactError::ShaclViolation(report)) => Ok(BranchOpValidation {
                report: Some(report),
            }),
            Err(e) => Err(e.into()),
        }
    }

    /// Without the `shacl` feature there is nothing to validate against.
    #[cfg(not(feature = "shacl"))]
    pub(crate) async fn validate_branch_op_view(
        &self,
        _view: &mut StagedLedger,
        _reverse_graph: &HashMap<Sid, GraphId>,
        _namespace_delta: &HashMap<u16, String>,
    ) -> Result<BranchOpValidation> {
        Ok(BranchOpValidation::default())
    }
}
