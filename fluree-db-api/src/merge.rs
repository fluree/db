//! Branch merge support.
//!
//! Merges a source branch into a target branch. Supports both fast-forward
//! merges (target HEAD is the common ancestor) and general merges with
//! conflict resolution strategies.

use crate::error::{ApiError, Result};
use crate::rebase::{current_asserted_for_key, ConflictStrategy};
use fluree_db_core::commit::codec::read_commit_envelope;
use fluree_db_core::content_kind::ContentKind;
use fluree_db_core::ledger_id::format_ledger_id;
use fluree_db_core::{collect_dag_cids, load_commit_by_id, CommonAncestor};
use fluree_db_core::{BranchedContentStore, ConflictKey, ContentId, ContentStore, Flake};
use fluree_db_ledger::{LedgerState, StagedLedger};
use fluree_db_nameservice::NsRecord;
use fluree_db_novelty::compute_delta_keys;
use fluree_db_transact::{CommitOpts, NamespaceRegistry};
use futures::stream::{self, StreamExt, TryStreamExt};
use rustc_hash::FxHashSet;
use serde::Serialize;
use tracing::Instrument;

/// One detected conflict between source and target branch state.
///
/// "Conflict" means both branches modified the `(s, p, g)` key relative to the
/// merge base **and** their resulting object sets differ. Two branches that
/// independently asserted the exact same triple are not reported here.
#[derive(Clone, Debug)]
pub(crate) struct DetectedConflict {
    /// The `(s, p, g)` key being conflicted on.
    pub(crate) key: ConflictKey,
    /// Currently-asserted flakes at this key on the source branch.
    pub(crate) source_values: Vec<Flake>,
    /// Currently-asserted flakes at this key on the target branch.
    pub(crate) target_values: Vec<Flake>,
}

/// Detect conflicts between source and target branches relative to a common
/// ancestor.
///
/// Implements the design's two-step rule:
///
/// 1. Intersect each side's delta keys (subjects/predicates/graphs touched
///    since the ancestor) — produces a candidate set.
/// 2. For each candidate, load the currently-asserted object sets at that key
///    on both branches and compare. Only keys whose object sets differ are
///    reported as conflicts; keys where both branches converged on identical
///    values are dropped.
///
/// The returned [`DetectedConflict`] list is sorted by key. The source/target
/// flake vectors are returned alongside so callers (preview details, merge
/// engine resolution) don't have to re-fetch them.
pub(crate) async fn detect_conflicts<S, T>(
    source_store: &S,
    target_store: &T,
    source_state: &LedgerState,
    target_state: &LedgerState,
    source_head: &ContentId,
    target_head: &ContentId,
    ancestor_t: i64,
) -> Result<Vec<DetectedConflict>>
where
    S: ContentStore + Clone + 'static,
    T: ContentStore + Clone + 'static,
{
    // Step 1: delta-key intersection.
    let s_delta_fut = compute_delta_keys(source_store.clone(), source_head.clone(), ancestor_t);
    let t_delta_fut = compute_delta_keys(target_store.clone(), target_head.clone(), ancestor_t);
    let (s_delta, t_delta) = tokio::try_join!(s_delta_fut, t_delta_fut)?;

    let mut candidates: Vec<ConflictKey> = s_delta.intersection(&t_delta).cloned().collect();
    candidates.sort();

    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    // Step 2: load object sets and filter to keys whose sets actually differ.
    const DETECT_CONCURRENCY: usize = 16;

    let detected: Vec<Option<DetectedConflict>> = stream::iter(candidates.into_iter())
        .map(|key| async move {
            let (source_values, target_values) = tokio::try_join!(
                current_asserted_for_key(source_state, &key),
                current_asserted_for_key(target_state, &key),
            )?;
            // Compare as sets via Flake's existing semantic equality
            // (`(s, p, o, dt, m)` — `g` is shared by construction since
            // `current_asserted_for_key` filtered by `key.g`).
            let s_set: FxHashSet<&Flake> = source_values.iter().collect();
            let t_set: FxHashSet<&Flake> = target_values.iter().collect();
            if s_set == t_set {
                Ok::<Option<DetectedConflict>, ApiError>(None)
            } else {
                Ok(Some(DetectedConflict {
                    key,
                    source_values,
                    target_values,
                }))
            }
        })
        .buffered(DETECT_CONCURRENCY)
        .try_collect()
        .await?;

    Ok(detected.into_iter().flatten().collect())
}

/// Summary report of a completed merge operation.
#[derive(Clone, Debug, Serialize)]
pub struct MergeReport {
    /// Target branch that was merged into.
    pub target: String,
    /// Source branch that was merged from.
    pub source: String,
    /// Whether this was a fast-forward merge.
    pub fast_forward: bool,
    /// New commit HEAD of the target after merge.
    pub new_head_t: i64,
    /// New commit HEAD CID of the target after merge.
    pub new_head_id: ContentId,
    /// Number of commit blobs copied to the target namespace.
    pub commits_copied: usize,
    /// Number of conflicts detected and resolved during merge.
    pub conflict_count: usize,
    /// Conflict resolution strategy used, if applicable.
    pub strategy: Option<String>,
}

impl crate::Fluree {
    /// Merge a source branch into a target branch driven by a [`MergePlan`].
    ///
    /// This is the primary plan-driven entry point. See `docs/design/merge-custom.md`
    /// for the full design.
    ///
    /// **v1 surface.** This iteration of the merge engine honors:
    /// - `plan.source` / `plan.target` branch selection
    /// - Expected commit heads (staleness guard — returns
    ///   `ApiError::BranchConflict` if either branch HEAD has moved)
    /// - `plan.base_strategy` (`take-source` / `take-target` / `take-both` /
    ///   `abort`)
    ///
    /// **Deferred to a follow-up:**
    /// - `plan.resolutions` (per-conflict actions overriding `base_strategy`)
    /// - `plan.additional_patch` (plan-level edits applied after resolutions)
    /// - `custom` action (requires JSON-LD patch compilation against the
    ///   target's namespace registry)
    ///
    /// Plans that exercise deferred features are accepted only when those
    /// fields are empty/absent; non-empty `resolutions` or `additional_patch`
    /// are rejected with `400` so callers fail loudly rather than silently
    /// getting a different merge than they asked for.
    pub async fn merge(
        &self,
        ledger_name: &str,
        plan: crate::merge_plan::MergePlan,
    ) -> Result<MergeReport> {
        plan.validate_shape()?;

        // v1: resolutions and additional_patch are not yet honored. Reject
        // requests that include them so callers know the feature is staged.
        if !plan.resolutions.is_empty() {
            return Err(ApiError::Http {
                status: 400,
                message: "MergePlan.resolutions is not yet implemented (v1 honors base_strategy only); use base_strategy or omit resolutions".into(),
            });
        }
        if plan
            .additional_patch
            .as_ref()
            .is_some_and(|p| !p.is_empty())
        {
            return Err(ApiError::Http {
                status: 400,
                message: "MergePlan.additional_patch is not yet implemented (v1 honors base_strategy only); omit additional_patch".into(),
            });
        }

        // Map BaseStrategy → ConflictStrategy. The two enums diverge in two
        // ways: BaseStrategy uses `TakeTarget` (clearer), ConflictStrategy
        // uses the legacy `TakeBranch`; and ConflictStrategy has a `Skip`
        // variant for rebase that BaseStrategy intentionally lacks.
        let strategy = match plan.base_strategy {
            crate::merge_plan::BaseStrategy::TakeSource => ConflictStrategy::TakeSource,
            crate::merge_plan::BaseStrategy::TakeTarget => ConflictStrategy::TakeBranch,
            crate::merge_plan::BaseStrategy::TakeBoth => ConflictStrategy::TakeBoth,
            crate::merge_plan::BaseStrategy::Abort => ConflictStrategy::Abort,
        };

        let span = tracing::debug_span!(
            "merge",
            ledger_name,
            source = plan.source.branch.as_str(),
            target = plan.target.branch.as_str(),
        );
        async move {
            self.merge_with_plan_inner(ledger_name, plan, strategy)
                .await
        }
        .instrument(span)
        .await
    }

    async fn merge_with_plan_inner(
        &self,
        ledger_name: &str,
        plan: crate::merge_plan::MergePlan,
        strategy: ConflictStrategy,
    ) -> Result<MergeReport> {
        // Pass expected heads through to the shared backend so the
        // `lookup → validate-expected → run merge` sequence happens against
        // a single nameservice read. Splitting the validation across two
        // separate lookups would let a branch advance between them and
        // silently merge the new HEAD instead of returning a stale-plan
        // conflict.
        self.merge_inner(
            ledger_name,
            &plan.source.branch,
            Some(&plan.target.branch),
            strategy,
            Some((plan.source.expected.clone(), plan.target.expected.clone())),
        )
        .await
    }

    /// Merge a source branch into a target branch.
    ///
    /// Supports fast-forward merges (when the target has not diverged) and
    /// general merges with conflict resolution via the given `strategy`.
    ///
    /// If `target_branch` is `None`, the source's parent branch (from its
    /// branch point) is used as the target.
    ///
    /// Most new code should prefer [`Self::merge`] (plan-driven) for the
    /// staleness guard and richer per-conflict resolution surface. This
    /// method is the legacy strategy-only entry point and remains for
    /// backward compatibility; both share the same underlying merge engine.
    pub async fn merge_branch(
        &self,
        ledger_name: &str,
        source_branch: &str,
        target_branch: Option<&str>,
        strategy: ConflictStrategy,
    ) -> Result<MergeReport> {
        let span = tracing::debug_span!("merge_branch", ledger_name, source_branch, ?target_branch);
        async move {
            self.merge_inner(ledger_name, source_branch, target_branch, strategy, None)
                .await
        }
        .instrument(span)
        .await
    }

    /// Single shared backend for both the legacy strategy-only entry point
    /// (`merge_branch`) and the plan-driven `merge`.
    ///
    /// `expected_heads`, when present, is `(source_expected, target_expected)`
    /// from a [`MergePlan`]. The expected commit IDs are checked against the
    /// records resolved by this same call's lookup — there is no second
    /// lookup, so a branch that moves between nameservice read and merge
    /// execution is detected by the commit writer's CAS, not by a racing
    /// expected-head check that read a stale record.
    async fn merge_inner(
        &self,
        ledger_name: &str,
        source_branch: &str,
        target_branch: Option<&str>,
        strategy: ConflictStrategy,
        expected_heads: Option<(ContentId, ContentId)>,
    ) -> Result<MergeReport> {
        let source_id = format_ledger_id(ledger_name, source_branch);
        let source_record = self
            .nameservice()
            .lookup(&source_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(source_id.clone()))?;

        // Resolve target: explicit or from source's parent branch.
        let source_parent = source_record.source_branch.as_deref().ok_or_else(|| {
            ApiError::InvalidBranch(format!(
                "Branch {source_branch} has no source branch; \
                     only branches created from another branch can be merged"
            ))
        })?;

        let resolved_target = target_branch.unwrap_or(source_parent);

        if source_branch == resolved_target {
            return Err(ApiError::InvalidBranch(
                "Cannot merge a branch into itself".to_string(),
            ));
        }

        let target_id = format_ledger_id(ledger_name, resolved_target);
        let target_record = self
            .nameservice()
            .lookup(&target_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(target_id.clone()))?;

        let source_head_id = source_record.commit_head_id.clone().ok_or_else(|| {
            ApiError::InvalidBranch(format!(
                "Source branch {source_branch} has no commits to merge"
            ))
        })?;
        let source_head_t = source_record.commit_t;

        // Validate expected heads against the records we just looked up.
        // This is the atomic check: no second nameservice read sneaks in
        // between the validation and the merge execution.
        if let Some((expected_source, expected_target)) = expected_heads {
            if source_head_id != expected_source {
                return Err(ApiError::BranchConflict(format!(
                    "stale source HEAD: plan expected {expected_source}, current is {source_head_id}",
                )));
            }
            match target_record.commit_head_id.as_ref() {
                Some(actual_target) if *actual_target == expected_target => {}
                Some(actual_target) => {
                    return Err(ApiError::BranchConflict(format!(
                        "stale target HEAD: plan expected {expected_target}, current is {actual_target}",
                    )));
                }
                None => {
                    return Err(ApiError::InvalidBranch(format!(
                        "Target branch {resolved_target} has no commits — cannot merge into an unborn branch",
                    )));
                }
            }
        }

        // Compute common ancestor to determine fast-forward eligibility.
        // Build branch-aware stores for both source and target.
        let source_store = LedgerState::build_branched_store(
            &self.nameservice_mode,
            &source_record,
            self.backend(),
        )
        .await?;
        let target_branched_for_ancestor: BranchedContentStore =
            if target_record.source_branch.is_some() {
                LedgerState::build_branched_store(
                    &self.nameservice_mode,
                    &target_record,
                    self.backend(),
                )
                .await?
            } else {
                BranchedContentStore::leaf(self.content_store(&target_id))
            };

        let target_head = target_record.commit_head_id.as_ref();
        let ancestor = match target_head {
            Some(target_head_id) => {
                // For sibling-branch merges (e.g., feature-a → feature-b,
                // both off main) the target HEAD lives in the target's own
                // namespace, not source's parent chain. The source-only
                // store can read source ancestry but not target's. Build a
                // union store so the ancestor walk can fan out to either
                // side's namespace — same shape `merge_preview` uses, so
                // the two stay in lock-step.
                use std::sync::Arc;
                let union_store = BranchedContentStore::with_parents(
                    Arc::new(source_store.clone()) as Arc<dyn ContentStore>,
                    vec![target_branched_for_ancestor.clone()],
                );
                Some(
                    fluree_db_core::find_common_ancestor(
                        &union_store,
                        &source_head_id,
                        target_head_id,
                    )
                    .await?,
                )
            }
            None => None,
        };

        // Fast-forward check: target HEAD must be the common ancestor.
        let is_fast_forward = match (&ancestor, target_head) {
            (Some(a), Some(tid)) => a.commit_id == *tid,
            (None, None) => true,
            _ => false,
        };

        // Disconnect target from ledger manager to prevent stale reads.
        if let Some(ref lm) = self.ledger_manager {
            lm.disconnect(&target_id).await;
        }

        let result: Result<MergeReport> =
            if is_fast_forward {
                let stop_at_t = ancestor.map(|a| a.t).unwrap_or(0);
                // Build the expected target ref from the same lookup that drove
                // the FF decision so the publish CAS-fails on any concurrent
                // advance. For an unborn target we expect `None` (the ledger has
                // no commit head yet — first FF establishes it).
                let expected_target = target_record.commit_head_id.as_ref().map(|id| {
                    fluree_db_nameservice::RefValue {
                        id: Some(id.clone()),
                        t: target_record.commit_t,
                    }
                });
                self.fast_forward_merge(
                    source_branch,
                    &source_record,
                    &source_head_id,
                    source_head_t,
                    resolved_target,
                    &target_id,
                    stop_at_t,
                    &source_store,
                    expected_target.as_ref(),
                )
                .await
            } else {
                // General merge: branches have diverged.
                let ancestor = ancestor.expect("ancestor must exist when both heads are Some");
                self.general_merge(
                    &source_id,
                    &source_record,
                    &source_head_id,
                    source_branch,
                    resolved_target,
                    &target_id,
                    &target_record,
                    &ancestor,
                    &source_store,
                    &strategy,
                )
                .await
            };

        // No nameservice rollback on error.
        //
        // Why: every merge publish path (`fast_forward_merge` → CAS publish,
        // `general_merge` → `fluree_db_transact::commit` → CAS publish)
        // either advances target HEAD atomically or returns an error
        // without publishing. There is no partial-publish state that needs
        // unwinding, and the post-publish steps (index copy, source-index
        // republish) are intentionally warn-only — they don't propagate
        // failures here.
        //
        // The legacy `reset_head` rollback is actively dangerous in this
        // architecture: it bypasses the monotonic-`t` guard and force-writes
        // a pre-merge snapshot. On a CAS-conflict error path the target
        // HEAD has been advanced by *another* writer; rolling back would
        // clobber their commit with our stale snapshot. Better to leave
        // the winning state in place and surface the conflict to the
        // caller, who can re-preview and try again.
        result
    }

    /// Fast-forward merge: copy commits from source to target and advance HEAD.
    #[allow(clippy::too_many_arguments)]
    async fn fast_forward_merge(
        &self,
        source_branch: &str,
        source_record: &NsRecord,
        source_head_id: &ContentId,
        source_head_t: i64,
        resolved_target: &str,
        target_id: &str,
        stop_at_t: i64,
        source_store: &impl ContentStore,
        expected_target: Option<&fluree_db_nameservice::RefValue>,
    ) -> Result<MergeReport> {
        // Copy commit and txn blobs from the source namespace into the target
        // namespace so the target is self-contained (no fallback reads needed).
        // We use the branched content store so that collect_dag_cids can read
        // parent commits from ancestor namespaces when checking stop_at_t.
        let commits_copied = self
            .copy_commit_chain(source_store, source_head_id, stop_at_t, target_id)
            .await?;

        // True no-op: source HEAD already equals target HEAD (source had no
        // unique commits). Skip publishing — the nameservice's monotonic
        // guard would reject `new.t == current.t` as a CAS conflict, and we
        // have no advance to record anyway.
        let already_caught_up = expected_target
            .and_then(|e| e.id.as_ref())
            .is_some_and(|id| id == source_head_id);

        if !already_caught_up {
            // Advance target's HEAD via compare-and-set against the value we
            // looked up earlier. A concurrent writer that advances target HEAD
            // between our nameservice read and this publish will fail this CAS
            // (`actual != expected`) and surface as a `BranchConflict`. The
            // legacy non-CAS `publish_commit` was monotonic-by-`t` only, so a
            // concurrent advance to a *different* commit at a lower-than-source
            // `t` would have been silently overwritten — exactly the staleness
            // race the plan-driven flow's expected-head check was supposed to
            // prevent.
            let new_ref = fluree_db_nameservice::RefValue {
                id: Some(source_head_id.clone()),
                t: source_head_t,
            };
            let cas = self
                .publisher()?
                .compare_and_set_ref(
                    target_id,
                    fluree_db_nameservice::RefKind::CommitHead,
                    expected_target,
                    &new_ref,
                )
                .await?;
            match cas {
                fluree_db_nameservice::CasResult::Updated => {}
                fluree_db_nameservice::CasResult::Conflict { actual } => {
                    return Err(ApiError::BranchConflict(format!(
                        "fast-forward merge raced a concurrent writer on target {target_id}: \
                         expected {expected_target:?}, actual {actual:?}",
                    )));
                }
            }
        }

        // Copy source's index to target namespace.
        let source_id = &source_record.ledger_id;
        if let Some(ref index_cid) = source_record.index_head_id {
            if let Err(e) = self
                .copy_index_to_branch(source_id, target_id, index_cid)
                .await
            {
                tracing::warn!(
                    %e, source = %source_id, target = %target_id,
                    "failed to copy index during merge; target will rebuild from commits"
                );
            } else if let Err(e) = self
                .publisher()?
                .publish_index(target_id, source_record.index_t, index_cid)
                .await
            {
                tracing::warn!(%e, "failed to publish index for merged target");
            }
        }

        Ok(MergeReport {
            target: resolved_target.to_string(),
            source: source_branch.to_string(),
            fast_forward: true,
            new_head_t: source_head_t,
            new_head_id: source_head_id.clone(),
            commits_copied,
            conflict_count: 0,
            strategy: None,
        })
    }

    /// General (non-fast-forward) merge: compute deltas, detect conflicts,
    /// resolve them, and create a merge commit on the target branch.
    #[allow(clippy::too_many_arguments)]
    async fn general_merge<C: ContentStore + Clone + 'static>(
        &self,
        source_id: &str,
        source_record: &NsRecord,
        source_head_id: &ContentId,
        source_branch: &str,
        resolved_target: &str,
        target_id: &str,
        target_record: &NsRecord,
        ancestor: &CommonAncestor,
        source_store: &C,
        strategy: &ConflictStrategy,
    ) -> Result<MergeReport> {
        // Skip is not supported for merge (it only makes sense for per-commit rebase).
        if *strategy == ConflictStrategy::Skip {
            return Err(ApiError::InvalidBranch(
                "Skip strategy is not supported for merge".to_string(),
            ));
        }

        let target_head_id = target_record
            .commit_head_id
            .as_ref()
            .expect("target must have head for non-fast-forward merge");
        let target_head_t = target_record.commit_t;
        let source_head_t = source_record.commit_t;

        // Build target's branch-aware store.
        let target_store: BranchedContentStore = if target_record.source_branch.is_some() {
            LedgerState::build_branched_store(&self.nameservice_mode, target_record, self.backend())
                .await?
        } else {
            BranchedContentStore::leaf(self.content_store(target_id))
        };

        // Load source and target queryable states. Both are needed:
        // - target_state for staging the merge commit
        // - source_state for refined conflict detection (object-set comparison)
        let target_state_fut =
            self.load_queryable_state_with_store(target_store.clone(), target_record.clone());
        let source_state_fut =
            self.load_queryable_state_with_store(source_store.clone(), source_record.clone());
        let (target_state, source_state) = tokio::try_join!(target_state_fut, source_state_fut)?;

        // Refined conflict detection (intersect delta keys, then compare
        // object sets). Convergent edits — both branches asserting the same
        // resulting triple — are not reported as conflicts.
        let detected = detect_conflicts(
            source_store,
            &target_store,
            &source_state,
            &target_state,
            source_head_id,
            target_head_id,
            ancestor.t,
        )
        .await?;
        let conflicts: Vec<ConflictKey> = detected.iter().map(|c| c.key.clone()).collect();
        let conflict_count = conflicts.len();

        // Abort if conflicts exist and strategy is Abort.
        if *strategy == ConflictStrategy::Abort && !conflicts.is_empty() {
            return Err(ApiError::BranchConflict(format!(
                "Merge aborted: {} conflict(s) between {} and {} with abort strategy",
                conflicts.len(),
                source_branch,
                resolved_target,
            )));
        }

        // Compute merge_t — the `t` for the merge commit. For multi-parent
        // merges this is `max(source_t, target_t) + 1`, which can exceed
        // `target_t + 1` when `source_t > target_t`. Threading this through
        // the staged view and the commit writer keeps query/validate
        // observations and the eventual commit header at the same `t`.
        let merge_t = source_head_t.max(target_head_t) + 1;

        // Collect source flakes and metadata: walk source commits from HEAD
        // to ancestor, gathering flakes, namespace deltas, and graph deltas.
        let source_data = collect_commit_data(source_store, source_head_id, ancestor.t).await?;

        // Resolve conflicts.
        let mut resolved_flakes = self
            .resolve_merge_flakes(&source_data.flakes, &conflicts, strategy, &target_state)
            .await?;

        // Restamp every merge-produced flake to `merge_t`. Source flakes
        // collected by `collect_commit_data` carry their original commit-t
        // values (e.g. `t = 5` from a source commit), and retractions
        // synthesized by `build_source_retractions` carry the placeholder
        // `t = 0`. Without restamping, the staged view's `staged_t == merge_t`
        // disagrees with the actual flake t values inside it, so query/
        // validate observations during the merge flow can drift from the
        // post-commit state observed after a reload. Stamping all flakes at
        // `merge_t` keeps the in-memory view, the commit blob, and any
        // post-reload range scan consistent at one `t`.
        for f in &mut resolved_flakes {
            f.t = merge_t;
        }

        // Stage resolved flakes onto target state. An empty flake set is valid
        // (e.g., TakeBranch drops all source flakes) — we still create the merge
        // commit to record the parent relationship and prevent future re-merges.
        let reverse_graph = target_state.snapshot.build_reverse_graph().map_err(|e| {
            ApiError::internal(format!("Failed to build reverse graph during merge: {e}"))
        })?;

        let view = StagedLedger::new_with_t(target_state, resolved_flakes, &reverse_graph, merge_t)
            .map_err(|e| ApiError::internal(format!("Failed to stage flakes during merge: {e}")))?;

        // Create merge commit with the source head as an additional parent,
        // propagating namespace and graph deltas from the source branch.
        let ns_registry = NamespaceRegistry::from_db(view.db());
        let mut commit_opts = CommitOpts::default()
            .with_merge_parents(vec![source_head_id.clone()])
            .with_merge_t(merge_t);
        if !source_data.namespace_delta.is_empty() {
            commit_opts = commit_opts.with_namespace_delta(source_data.namespace_delta);
        }
        if !source_data.graph_delta.is_empty() {
            commit_opts = commit_opts.with_graph_delta(source_data.graph_delta);
        }

        // Copy source commit chain to target namespace so the target is
        // self-contained for DAG walking. This must happen before the merge
        // commit is published.
        let commits_copied = self
            .copy_commit_chain(source_store, source_head_id, ancestor.t, target_id)
            .await?;

        let content_store = self.content_store(target_id);

        let publisher = self.publisher()?;
        let (receipt, _new_state) = fluree_db_transact::commit(
            view,
            ns_registry,
            &content_store,
            publisher,
            &self.index_config,
            commit_opts,
        )
        .await?;

        // Copy source's index to target (best-effort).
        if let Some(ref index_cid) = source_record.index_head_id {
            if let Err(e) = self
                .copy_index_to_branch(source_id, target_id, index_cid)
                .await
            {
                tracing::warn!(
                    %e, source = %source_id, target = %target_id,
                    "failed to copy source index during merge; target will rebuild"
                );
            }
        }

        Ok(MergeReport {
            target: resolved_target.to_string(),
            source: source_branch.to_string(),
            fast_forward: false,
            new_head_t: receipt.t,
            new_head_id: receipt.commit_id,
            commits_copied,
            conflict_count,
            strategy: Some(strategy.as_str().to_string()),
        })
    }

    /// Resolve flakes for a merge operation.
    ///
    /// In merge context the semantics are:
    /// - `TakeBoth`: keep all source flakes as-is (both values coexist).
    /// - `TakeSource` (incoming branch wins): keep source flakes + retract
    ///   target's conflicting values.
    /// - `TakeBranch` (target wins): drop source's conflicting flakes.
    /// - `Abort`/`Skip`: handled before this method is called.
    async fn resolve_merge_flakes(
        &self,
        flakes: &[Flake],
        conflicting_keys: &[ConflictKey],
        strategy: &ConflictStrategy,
        target_state: &LedgerState,
    ) -> Result<Vec<Flake>> {
        if conflicting_keys.is_empty() {
            return Ok(flakes.to_vec());
        }

        let conflict_set: FxHashSet<&ConflictKey> = conflicting_keys.iter().collect();

        match strategy {
            ConflictStrategy::TakeSource => {
                // Incoming branch wins: keep source flakes + retract target's values.
                // In rebase terms, this is like TakeBranch — we query the target
                // state (the "other side") for retractions.
                let retractions = self
                    .build_source_retractions(conflicting_keys, target_state)
                    .await?;
                let mut result = flakes.to_vec();
                result.extend(retractions);
                Ok(result)
            }
            ConflictStrategy::TakeBranch => {
                // Target wins: drop source's conflicting flakes.
                Ok(flakes
                    .iter()
                    .filter(|f| {
                        let key = ConflictKey::new(f.s.clone(), f.p.clone(), f.g.clone());
                        !conflict_set.contains(&key)
                    })
                    .cloned()
                    .collect())
            }
            // TakeBoth: keep all source flakes, both values coexist.
            // Abort/Skip: handled before this method is called.
            _ => Ok(flakes.to_vec()),
        }
    }

    /// Copy commit blobs (and their referenced txn blobs) from a source
    /// content store into the target's storage namespace.
    ///
    /// Collects the commit DAG from `head_id` backwards to `stop_at_t`,
    /// then iterates the resulting CIDs to copy each commit and its txn
    /// blob into the target namespace.
    async fn copy_commit_chain(
        &self,
        source_store: &impl ContentStore,
        head_id: &ContentId,
        stop_at_t: i64,
        target_ledger_id: &str,
    ) -> Result<usize> {
        let storage = self
            .admin_storage()
            .ok_or_else(|| ApiError::internal("merge requires managed storage backend"))?;

        let dag = collect_dag_cids(source_store, head_id, stop_at_t).await?;
        let mut copied = 0;

        for (_, cid) in &dag {
            let bytes = source_store.get(cid).await?;

            // Parse envelope to extract txn CID reference. The bytes were already
            // loaded by collect_dag_cids for parent discovery; this is a re-parse
            // of the same data, not a second storage read.
            let envelope = read_commit_envelope(&bytes).map_err(|e| {
                ApiError::internal(format!("failed to read commit envelope {cid}: {e}"))
            })?;

            // Write commit blob to target namespace.
            storage
                .content_write_bytes_with_hash(
                    ContentKind::Commit,
                    target_ledger_id,
                    &cid.digest_hex(),
                    &bytes,
                )
                .await?;

            // Overlap: copy the txn blob concurrently with reading the
            // next commit blob.  The two operations are independent.
            let txn_fut = async {
                let Some(ref txn_cid) = envelope.txn else {
                    return Ok(());
                };
                let txn_bytes = source_store.get(txn_cid).await?;
                storage
                    .content_write_bytes_with_hash(
                        ContentKind::Txn,
                        target_ledger_id,
                        &txn_cid.digest_hex(),
                        &txn_bytes,
                    )
                    .await?;
                Ok::<_, crate::error::ApiError>(())
            };

            txn_fut.await?;

            copied += 1;
        }

        tracing::debug!(commits = copied, "copied commit chain to target namespace");
        Ok(copied)
    }
}

/// Collected flakes and metadata from a range of commits.
struct CollectedCommitData {
    flakes: Vec<Flake>,
    namespace_delta: std::collections::HashMap<u16, String>,
    graph_delta: std::collections::HashMap<u16, String>,
}

/// Collect all flakes, namespace deltas, and graph deltas from commits
/// between `head_id` and `stop_at_t` (exclusive).
async fn collect_commit_data(
    store: &impl ContentStore,
    head_id: &ContentId,
    stop_at_t: i64,
) -> Result<CollectedCommitData> {
    let dag = collect_dag_cids(store, head_id, stop_at_t).await?;
    let mut all_flakes = Vec::new();
    let mut namespace_delta = std::collections::HashMap::new();
    let mut graph_delta = std::collections::HashMap::new();

    // dag is in newest-first order; we want oldest-first for correct ordering.
    for (_, cid) in dag.iter().rev() {
        let commit = load_commit_by_id(store, cid).await?;
        all_flakes.extend(commit.flakes);
        // Accumulate deltas: earlier commits take precedence (oldest-first).
        for (code, prefix) in commit.namespace_delta {
            namespace_delta.entry(code).or_insert(prefix);
        }
        for (g_id, iri) in commit.graph_delta {
            graph_delta.entry(g_id).or_insert(iri);
        }
    }

    Ok(CollectedCommitData {
        flakes: all_flakes,
        namespace_delta,
        graph_delta,
    })
}
