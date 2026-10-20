//! Validation for branch operations.
//!
//! Merge (fast-forward included), rebase, revert, and their previews build
//! their commits on a path parallel to the transaction pipeline, staging
//! flakes straight into a [`StagedLedger`]. This module runs the checks the
//! transaction path runs on a staged view (`check_staged_write`: SHACL and
//! uniqueness under the target's configuration, and the staged-config
//! guard), so a branch operation whose resulting state the ledger's
//! configuration rejects fails exactly as a transaction producing that state
//! would. R2 is enforced when the data is written; branch operations
//! re-check the integrity of the target's config groups (R1/R3).
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
use fluree_db_transact::TransactError;
use std::collections::{BTreeSet, HashMap};

/// Outcome of validating a branch operation's staged view.
///
/// Warn-mode violations are logged by the validator and never surface here,
/// matching the transaction path. Only a rejection produces an outcome: a
/// SHACL violation report, a uniqueness violation, or a refusal of the
/// staged-config guard.
#[derive(Debug, Default)]
pub(crate) struct BranchOpValidation {
    /// Why the operation may not apply, `None` when the view conforms.
    rejection: Option<Rejection>,
}

/// Why a branch operation's staged view may not apply.
#[derive(Debug)]
enum Rejection {
    /// A SHACL or uniqueness violation: the error a transaction producing
    /// the same state would fail with.
    Check(TransactError),
    /// The staged config conflicts with the target's: a config group the
    /// operation would split, a second value, or another branch's config
    /// graph. The target's state blocks the operation (409).
    Config(String),
}

impl BranchOpValidation {
    pub(crate) fn conforms(&self) -> bool {
        self.rejection.is_none()
    }

    /// The rejection as a report for a preview: the SHACL violation report,
    /// or the uniqueness violation's or config refusal's message. `None` when
    /// the view conforms.
    pub(crate) fn report(&self) -> Option<String> {
        self.rejection.as_ref().map(|rejection| match rejection {
            #[cfg(feature = "shacl")]
            Rejection::Check(TransactError::ShaclViolation(report)) => report.to_string(),
            Rejection::Check(other) => other.to_string(),
            Rejection::Config(message) => message.clone(),
        })
    }

    /// Turn a rejected outcome into the error a transaction producing the
    /// same state would return.
    pub(crate) fn into_result(self) -> Result<()> {
        self.into_result_with(|report| report)
    }

    /// Like [`Self::into_result`], with `describe` wrapping the refusal's
    /// message in the operation's own context (which commit a rebase stopped
    /// on, say): a SHACL report and a config refusal. A uniqueness violation
    /// is returned as its typed error, which carries no free text.
    pub(crate) fn into_result_with(self, describe: impl FnOnce(String) -> String) -> Result<()> {
        match self.rejection {
            None => Ok(()),
            #[cfg(feature = "shacl")]
            Some(Rejection::Check(TransactError::ShaclViolation(report))) => {
                let message = describe(report.message().to_string());
                let report =
                    fluree_db_transact::ShaclViolations::new(message, report.results().to_vec());
                Err(TransactError::ShaclViolation(report).into())
            }
            Some(Rejection::Check(rejection)) => {
                let _ = describe;
                Err(rejection.into())
            }
            Some(Rejection::Config(message)) => Err(ApiError::BranchConflict(describe(message))),
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
        self.stage_validated_with(state, flakes, namespace_delta, graph_iris, op, None)
            .await
    }

    /// [`Self::stage_validated`], with the target's config when the caller
    /// has already read it (a fast-forward reads it to learn whether the
    /// target governs writes), so the checks do not read it again.
    async fn stage_validated_with(
        &self,
        state: LedgerState,
        flakes: Vec<Flake>,
        namespace_delta: &HashMap<u16, String>,
        graph_iris: &BTreeSet<String>,
        op: &'static str,
        config: Option<&crate::tx::ConfigRead>,
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
            .validate_branch_op_view(&mut view, namespace_delta, &new_graph_iris, op, config)
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

    /// The target's state before a fast-forward, and whether its
    /// configuration governs writes ([`governs_writes`]).
    ///
    /// A fast-forward adopts the source's commits as they are, so the target
    /// checks them first ([`Self::validate_fast_forward`]), against its
    /// configuration as it is before the merge, exactly as it would a
    /// transaction writing the same change. The state comes from the ledger
    /// cache when it holds the target at `record`'s head, else from storage.
    pub(crate) async fn fast_forward_base<C>(
        &self,
        target_id: &fluree_db_core::LedgerId,
        target_store: &C,
        record: &fluree_db_nameservice::NsRecord,
    ) -> Result<FastForwardBase>
    where
        C: fluree_db_core::ContentStore + Clone + 'static,
    {
        let cached = match self.ledger_manager.as_ref() {
            Some(manager) => manager.get_loaded_view(target_id).await.filter(|view| {
                view.t == record.commit_t && view.head_commit_id == record.commit_head_id
            }),
            None => None,
        };
        let state = match cached {
            Some(view) => view.to_ledger_state(),
            None => {
                self.load_queryable_state_with_store(target_store.clone(), record.clone())
                    .await?
            }
        };
        let config = crate::tx::load_transaction_config(&state).await?;
        let governed = config.as_deref().is_some_and(governs_writes);
        Ok(FastForwardBase {
            state,
            governed,
            config: crate::tx::ConfigRead::resolved(config),
        })
    }

    /// Check a fast-forward onto `base` (from [`Self::fast_forward_base`]):
    /// stage the adopted commits' net change (the source's line since the
    /// target's head) onto it and run the transaction checks under its
    /// configuration. `None` when there is nothing to check: the target
    /// governs no writes and the change carries no config settings, so it
    /// fast-forwards as before.
    pub(crate) async fn validate_fast_forward(
        &self,
        base: FastForwardBase,
        source_store: &impl fluree_db_core::ContentStore,
        diff: &fluree_db_core::BranchDiff,
    ) -> Result<Option<BranchOpValidation>> {
        let (data, _keys) =
            crate::merge::collect_commit_data(source_store, &diff.source.commits, &diff.source.own)
                .await?;
        if !base.governed && !carries_config_settings(&base.state, &data) {
            return Ok(None);
        }
        let (_view, outcome) = self
            .stage_validated_with(
                base.state,
                data.flakes,
                &data.namespace_delta,
                &data.graph_iris,
                "merge",
                Some(&base.config),
            )
            .await?;
        Ok(Some(outcome))
    }

    /// Run the checks a transaction runs on its staged view
    /// ([`crate::tx::check_staged_write`]) against the target's
    /// configuration: SHACL and uniqueness, under the target's pre-operation
    /// config, and the staged-config guard's re-check of the target's config
    /// groups. `new_graph_iris` names graphs the incoming commits create,
    /// which the target's registry does not know yet; `op` names the
    /// operation in refusals.
    async fn validate_branch_op_view(
        &self,
        view: &mut StagedLedger,
        namespace_delta: &HashMap<u16, String>,
        new_graph_iris: &HashMap<GraphId, String>,
        op: &'static str,
        config: Option<&crate::tx::ConfigRead>,
    ) -> Result<BranchOpValidation> {
        use crate::config_guard::GuardScope;
        use crate::tx::{check_staged_write, StagedWriteError, WriteChecks};
        use fluree_db_transact::NamespaceRegistry;

        if !view.has_staged() {
            return Ok(BranchOpValidation::default());
        }
        if let Some(refusal) = self.system_graph_write(view, new_graph_iris, op).await? {
            return Ok(BranchOpValidation {
                rejection: Some(Rejection::Config(refusal)),
            });
        }

        let base = view.base();
        let ledger_id = base.snapshot.ledger_id.clone();
        let mut resolve_ctx = crate::cross_ledger::ResolveCtx::new(&ledger_id, self);

        // The staged namespace registry: the snapshot's codes plus the ones
        // this operation brings in (its delta then holds them, for violation
        // messages and `sh:sparql` lowering).
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
        // Graph routing for the staged flakes: every named graph they touch,
        // with its IRI for per-graph config resolution. The default graph
        // always gets the ledger-wide settings.
        let mut graph_delta: rustc_hash::FxHashMap<fluree_db_core::GraphId, String> =
            rustc_hash::FxHashMap::default();
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

        // A branch operation only needs to know whether anything rejected
        // it; whether a check ran (commit provenance for a transaction) does
        // not matter here.
        match check_staged_write(
            view,
            &mut staged_ns,
            &graph_delta,
            WriteChecks {
                config,
                ..WriteChecks::new(GuardScope::BranchOperation(op))
            },
            &mut resolve_ctx,
        )
        .await
        {
            Ok(_governed) => Ok(BranchOpValidation::default()),
            Err(StagedWriteError::Rejected(rejection)) => Ok(BranchOpValidation {
                rejection: Some(Rejection::Check(rejection)),
            }),
            Err(StagedWriteError::ConfigRefused(message)) => Ok(BranchOpValidation {
                rejection: Some(Rejection::Config(message)),
            }),
            Err(StagedWriteError::Failed(e)) => Err(e.into()),
        }
    }
}

impl crate::Fluree {
    /// Why a branch operation may not write a system graph, if it does.
    ///
    /// A merge stages what another branch wrote, so it may not change the
    /// target's own config or txn-meta graph at all (asserting or
    /// retracting): the target's system graph IRIs name ordinary graphs on
    /// every other branch, and what a branch writes there is data, written
    /// under that branch's rules, not a change to the target's config or
    /// commit metadata. Config is changed on the branch it configures.
    ///
    /// No branch operation may write another branch's system graph: config
    /// (or commit metadata) written on a branch is named for that branch
    /// (`urn:fluree:<name>:<branch>#config`) and configures that branch
    /// only, so carried to another branch it would land in an ordinary
    /// graph, never as that branch's config. A branch the target was created
    /// from (directly or not) is the exception: its system graphs are
    /// already ordinary graphs on the target, copied at the fork.
    /// Retractions (a revert cleaning such a graph up) are allowed.
    async fn system_graph_write(
        &self,
        view: &StagedLedger,
        new_graph_iris: &HashMap<GraphId, String>,
        op: &str,
    ) -> Result<Option<String>> {
        use fluree_db_core::graph_registry::{CONFIG_GRAPH_ID, TXN_META_GRAPH_ID};
        let base = view.base();
        let ledger_id = base.snapshot.ledger_id.to_string();
        if op == "merge" {
            let own = view
                .staged_flakes_by_graph()
                .map(|(g_id, _)| g_id)
                .find(|g_id| matches!(*g_id, CONFIG_GRAPH_ID | TXN_META_GRAPH_ID));
            if let Some(g_id) = own {
                let role = if g_id == CONFIG_GRAPH_ID {
                    "config"
                } else {
                    "txn-meta"
                };
                return Ok(Some(format!(
                    "the merge would change this branch's {role} graph <{}> with what the \
                     source branch wrote to it as an ordinary graph; change this branch's \
                     {role} on this branch itself",
                    base.snapshot
                        .graph_registry
                        .iri_for_graph_id(g_id)
                        .unwrap_or_default()
                )));
            }
        }
        let mut written: BTreeSet<GraphId> = BTreeSet::new();
        for (g_id, flake) in view.staged_flakes_by_graph() {
            if flake.op && g_id != fluree_db_core::DEFAULT_GRAPH_ID {
                written.insert(g_id);
            }
        }
        for g_id in written {
            let iri = new_graph_iris
                .get(&g_id)
                .map(String::as_str)
                .or_else(|| base.snapshot.graph_registry.iri_for_graph_id(g_id));
            let Some((branch, role)) =
                iri.and_then(|iri| crate::config_guard::other_branch_system_graph(iri, &ledger_id))
            else {
                continue;
            };
            if self.is_ancestor_branch(&ledger_id, branch).await? {
                continue;
            }
            return Ok(Some(format!(
                "the {op} would write to branch {branch}'s {role} graph <{}>: what is written \
                 there on a branch configures that branch only and is not carried over as \
                 {role}; make the change on this branch, or revert it on {branch} first",
                iri.unwrap_or_default()
            )));
        }
        Ok(None)
    }

    /// Whether `branch` is one `ledger_id`'s branch was created from,
    /// directly or through other branches.
    async fn is_ancestor_branch(&self, ledger_id: &str, branch: &str) -> Result<bool> {
        let mut id = fluree_db_core::LedgerId::parse(ledger_id)?;
        // A branch chain is short; the bound only guards a malformed cycle.
        for _ in 0..64 {
            let Some(record) = self.nameservice().lookup(&id).await? else {
                return Ok(false);
            };
            let Some(parent) = record.source_branch else {
                return Ok(false);
            };
            if parent == branch {
                return Ok(true);
            }
            id = fluree_db_core::LedgerId::from_parts(id.name(), &parent)?;
        }
        Ok(false)
    }
}

/// A fast-forward target's state before the merge.
pub(crate) struct FastForwardBase {
    /// The target's state at the merge's head.
    pub(crate) state: LedgerState,
    /// Whether its configuration governs writes ([`governs_writes`]).
    pub(crate) governed: bool,
    /// Its configuration, read once: the checks reuse it.
    pub(crate) config: crate::tx::ConfigRead,
}

/// Whether the adopted change carries anything a branch operation's config
/// checks can refuse: a config field ([`crate::config_vocab::is_config_field`],
/// the config reader's vocabulary) for the split-group and second-value
/// checks, or a write to the target's config or txn-meta graph or to another
/// branch's system graph of this ledger. Every such refusal needs one of
/// them (a config typed outside the config graph is refused only where it is
/// written), so a change without any (data, edge annotations included) is
/// accepted unchecked into a target that governs no writes.
fn carries_config_settings(
    target: &LedgerState,
    data: &crate::commit_data::CollectedCommitData,
) -> bool {
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&target.snapshot);
    // The adopted commits' own namespace codes, so a config graph first
    // written on the source still encodes as its flakes do. A conflicting
    // delta is the merge's to refuse; it cannot make the change config-free.
    if ns
        .adopt_delta_for_persistence(&data.namespace_delta)
        .is_err()
    {
        return true;
    }
    let own_system_graphs: Vec<Sid> = [
        fluree_db_core::config_graph_iri(&target.snapshot.ledger_id),
        fluree_db_core::graph_registry::txn_meta_graph_iri(&target.snapshot.ledger_id),
    ]
    .iter()
    .filter_map(|iri| ns.lookup_sid_for_iri(iri))
    .collect();
    let ledger_id = target.snapshot.ledger_id.to_string();
    let mut graphs_seen: std::collections::HashSet<&Sid> = std::collections::HashSet::new();
    data.flakes.iter().any(|flake| {
        crate::config_vocab::is_config_field(&flake.p)
            || flake.g.as_ref().is_some_and(|g| {
                own_system_graphs.contains(g)
                    || (graphs_seen.insert(g)
                        && ns.get_prefix(g.namespace_code).is_some_and(|prefix| {
                            let iri = format!("{prefix}{}", g.name);
                            crate::config_guard::other_branch_system_graph(&iri, &ledger_id)
                                .is_some()
                        }))
            })
    })
}

/// Whether a ledger configuration governs writes: SHACL or uniqueness
/// enabled, ledger-wide or for any graph. (Which graphs a write actually
/// touches, and each one's effective setting, is decided by the checks
/// themselves; this only says whether running them can matter.)
pub(crate) fn governs_writes(config: &fluree_db_core::ledger_config::LedgerConfig) -> bool {
    use fluree_db_core::ledger_config::{ShaclDefaults, TransactDefaults};
    let shacl = |group: &Option<ShaclDefaults>| {
        group
            .as_ref()
            .is_some_and(|shacl| shacl.enabled == Some(true))
    };
    let unique = |group: &Option<TransactDefaults>| {
        group
            .as_ref()
            .is_some_and(|transact| transact.unique_enabled == Some(true))
    };
    shacl(&config.shacl)
        || unique(&config.transact)
        || config
            .graph_overrides
            .iter()
            .any(|graph| shacl(&graph.shacl) || unique(&graph.transact))
}
