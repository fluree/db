//! Revert: create a single commit that undoes the changes from a set or range
//! of prior commits, with git-style conflict resolution.
//!
//! Three entry points cover the natural shapes of a revert request:
//! - [`crate::Fluree::revert_commit`] — the common case: undo a single commit.
//! - [`crate::Fluree::revert_commits`] — cherry-pick style: revert an explicit
//!   set of commits identified by [`CommitRef`].
//! - [`crate::Fluree::revert_range`] — git's `A..B`: revert every commit
//!   reachable from `to` but not from `from`.
//!
//! Each produces a single bundled commit with a single parent (HEAD) and
//! records the reverted commit IDs as `f:reverts` entries in `txn_meta`.
//!
//! Limitations (v1):
//! - Merge commits in the revert set are rejected; `-m`-style mainline
//!   selection is deferred.
//! - [`ConflictStrategy::TakeBoth`] and [`ConflictStrategy::Skip`] are
//!   rejected — only `Abort`, `TakeSource`, and `TakeBranch` make sense for
//!   revert.

use crate::commit_data::{collect_from_commits, CollectedCommitData};
use crate::error::{ApiError, Result};
use crate::ledger_view::{CommitRef, LedgerView};
use crate::rebase::ConflictStrategy;
use fluree_db_core::commit::{TxnMetaEntry, TxnMetaValue};
use fluree_db_core::ledger_id::format_ledger_id;
use fluree_db_core::{
    collect_dag_cids, load_commit_by_id, load_commit_envelope_by_id, trace_commits_by_id,
    BranchedContentStore, CommitId, ConflictKey, ContentStore, NonEmpty,
};
use fluree_db_ledger::{LedgerState, StagedLedger};
use fluree_db_nameservice::NsRecordSnapshot;
use fluree_db_transact::{CommitOpts, CommitReceipt, NamespaceRegistry};
use fluree_vocab::namespaces::FLUREE_DB;
use futures::TryStreamExt;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::Serialize;
use tracing::Instrument;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Summary of a completed revert.
#[derive(Clone, Debug, Serialize)]
pub struct RevertReport {
    /// Branch the revert commit was written to.
    pub branch: String,
    /// Commit IDs of every reverted commit, newest-first (the order applied).
    pub reverted_commits: Vec<CommitId>,
    /// Number of `(s, p, g)` keys that conflicted before resolution.
    pub conflict_count: usize,
    /// Conflict-resolution strategy that was applied.
    pub strategy: String,
    /// `t` of the freshly written revert commit (new HEAD).
    pub new_head_t: i64,
    /// Commit ID of the freshly written revert commit (new HEAD).
    pub new_head_id: CommitId,
}

// ---------------------------------------------------------------------------
// Orchestration
// ---------------------------------------------------------------------------

impl crate::Fluree {
    /// Revert a single commit on `branch`.
    ///
    /// Convenience wrapper around [`Self::revert_commits`] for the most common
    /// case. See that method for [`CommitRef`] resolution rules and conflict
    /// strategy semantics.
    pub async fn revert_commit(
        &self,
        ledger_name: &str,
        branch: &str,
        commit: CommitRef,
        strategy: ConflictStrategy,
    ) -> Result<RevertReport> {
        let span = tracing::debug_span!(
            "revert_commit",
            ledger_name,
            branch,
            strategy = strategy.as_str()
        );
        async move {
            self.revert_inner(ledger_name, branch, RevertSource::single(commit), strategy)
                .await
        }
        .instrument(span)
        .await
    }

    /// Revert an explicit set of commits on `branch` (cherry-pick style).
    ///
    /// Each [`CommitRef`] is resolved against the branch's current view (so
    /// `t:N`, hex prefixes, and full commit IDs all work). Reverts are applied
    /// newest-first by `t`. Each resolved commit must be reachable from the
    /// branch HEAD via parent links and must not be a merge or genesis commit.
    ///
    /// `commits` must be non-empty; an empty `Vec` returns
    /// [`ApiError::InvalidBranch`].
    ///
    /// See [`Self::revert_range`] for the git-`A..B` form.
    ///
    /// Conflict resolution mirrors [`Self::merge_branch`]:
    /// - `Abort`: bail with a structured error if any conflicts exist.
    /// - `TakeSource`: revert wins — emit inverse flakes and additionally
    ///   retract any current HEAD values on conflicting `(s, p, g)` tuples.
    /// - `TakeBranch`: HEAD wins — drop inverse flakes for conflicting tuples.
    /// - `TakeBoth` and `Skip` are rejected as not meaningful for revert.
    pub async fn revert_commits(
        &self,
        ledger_name: &str,
        branch: &str,
        commits: Vec<CommitRef>,
        strategy: ConflictStrategy,
    ) -> Result<RevertReport> {
        let span = tracing::debug_span!(
            "revert_commits",
            ledger_name,
            branch,
            strategy = strategy.as_str()
        );
        async move {
            let source = RevertSource::try_set(commits).ok_or_else(|| {
                ApiError::InvalidBranch("Revert requires at least one commit".to_string())
            })?;
            self.revert_inner(ledger_name, branch, source, strategy)
                .await
        }
        .instrument(span)
        .await
    }

    /// Revert every commit in the git-style range `from..to` on `branch`.
    ///
    /// Mirrors `git revert A..B`: `from` is **exclusive**, `to` is
    /// **inclusive**. `from` must be an ancestor of `to`, and both must be
    /// reachable from the branch HEAD. Reverts are applied newest-first.
    ///
    /// See [`Self::revert_commits`] for cherry-pick style and conflict
    /// strategy semantics.
    pub async fn revert_range(
        &self,
        ledger_name: &str,
        branch: &str,
        from: CommitRef,
        to: CommitRef,
        strategy: ConflictStrategy,
    ) -> Result<RevertReport> {
        let span = tracing::debug_span!(
            "revert_range",
            ledger_name,
            branch,
            strategy = strategy.as_str()
        );
        async move {
            self.revert_inner(ledger_name, branch, RevertSource::range(from, to), strategy)
                .await
        }
        .instrument(span)
        .await
    }

    async fn revert_inner(
        &self,
        ledger_name: &str,
        branch: &str,
        source: RevertSource,
        strategy: ConflictStrategy,
    ) -> Result<RevertReport> {
        match strategy {
            ConflictStrategy::TakeBoth => {
                return Err(ApiError::InvalidBranch(
                    "TakeBoth strategy is not supported for revert".to_string(),
                ));
            }
            ConflictStrategy::Skip => {
                return Err(ApiError::InvalidBranch(
                    "Skip strategy is not supported for revert".to_string(),
                ));
            }
            _ => {}
        }

        let ctx = self
            .build_revert_context(ledger_name, branch, source)
            .await?;

        if strategy == ConflictStrategy::Abort && !ctx.conflict_keys.is_empty() {
            return Err(ApiError::BranchConflict(format!(
                "Revert aborted: {} conflict(s) on {} with abort strategy",
                ctx.conflict_keys.len(),
                branch
            )));
        }

        let snapshot = NsRecordSnapshot::from_record(&ctx.branch_record);
        if let Some(ref lm) = self.ledger_manager {
            lm.disconnect(&ctx.branch_id).await;
        }

        let RevertContext {
            branch_id,
            branch_record,
            branch_store,
            plan,
            conflict_keys,
        } = ctx;

        let result = self
            .write_revert_commit(
                &branch_id,
                branch_record.clone(),
                &branch_store,
                &plan,
                &conflict_keys,
                &strategy,
            )
            .await;

        match result {
            Ok(outcome) => {
                let (new_head_t, new_head_id) = match outcome {
                    RevertWriteOutcome::Wrote(receipt) => (receipt.t, receipt.commit_id),
                    RevertWriteOutcome::NoOp { head_t, head_id } => (head_t, head_id),
                };
                Ok(RevertReport {
                    branch: branch.to_string(),
                    reverted_commits: plan.ordered_commits.into_vec(),
                    conflict_count: conflict_keys.len(),
                    strategy: strategy.as_str().to_string(),
                    new_head_t,
                    new_head_id,
                })
            }
            Err(e) => {
                tracing::warn!(
                    branch = %branch_id,
                    error = %e,
                    "revert failed, rolling back nameservice state"
                );
                if let Err(rollback_err) = self.nameservice().reset_head(&branch_id, snapshot).await
                {
                    tracing::error!(
                        branch = %branch_id,
                        error = %rollback_err,
                        "failed to roll back nameservice state after revert failure"
                    );
                }
                Err(e)
            }
        }
    }

    /// Resolve `source` against `branch`'s current state, walk the DAG, build
    /// the revert plan, and compute conflict keys — every step that
    /// [`Self::revert_inner`] performs *before* mutating state. Shared with the
    /// preview path.
    pub(crate) async fn build_revert_context(
        &self,
        ledger_name: &str,
        branch: &str,
        source: RevertSource,
    ) -> Result<RevertContext> {
        let branch_id = format_ledger_id(ledger_name, branch);
        let branch_record = self
            .nameservice()
            .lookup(&branch_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(branch_id.clone()))?;
        let branch_head_id = branch_record.commit_head_id.clone().ok_or_else(|| {
            ApiError::InvalidBranch(format!("Branch {branch_id} has no commits to revert"))
        })?;

        let branch_store: BranchedContentStore = if branch_record.source_branch.is_some() {
            LedgerState::build_branched_store(
                &self.nameservice_mode,
                &branch_record,
                self.backend(),
            )
            .await?
        } else {
            BranchedContentStore::leaf(self.content_store(&branch_id))
        };

        // Resolve user-supplied [`CommitRef`]s against the branch's current
        // view — same path used by `branch create --at`.
        let branch_state = self.ledger(&branch_id).await?;
        let view = LedgerView::from_state(&branch_state);
        let resolved = match source {
            RevertSource::Commits(NonEmpty { head, tail }) => {
                let head = view.resolve_commit(head).await?;
                let mut resolved_tail = Vec::with_capacity(tail.len());
                for r in tail {
                    resolved_tail.push(view.resolve_commit(r).await?);
                }
                ResolvedSource::Commits(NonEmpty {
                    head,
                    tail: resolved_tail,
                })
            }
            RevertSource::Range { from, to } => {
                let from = view.resolve_commit(from).await?;
                let to = view.resolve_commit(to).await?;
                ResolvedSource::Range { from, to }
            }
        };

        let plan = resolve_revert_plan(&branch_store, &branch_head_id, &resolved).await?;
        let conflict_keys = compute_conflict_keys(
            &branch_store,
            &branch_head_id,
            &plan.reverted_set,
            plan.oldest_t,
        )
        .await?;

        Ok(RevertContext {
            branch_id,
            branch_record,
            branch_store,
            plan,
            conflict_keys,
        })
    }

    async fn write_revert_commit<C: ContentStore + Clone + 'static>(
        &self,
        branch_id: &str,
        branch_record: fluree_db_nameservice::NsRecord,
        branch_store: &C,
        plan: &RevertPlan,
        conflict_keys: &[ConflictKey],
        strategy: &ConflictStrategy,
    ) -> Result<RevertWriteOutcome> {
        // Load reverted commits oldest-first then fold via the shared
        // accumulator: invert each flake's `op` (assertion ⇄ retraction) and
        // accumulate `namespace_delta`/`graph_delta` with earlier-wins
        // semantics, matching the merge path's `collect_commit_data`.
        let mut commits = Vec::with_capacity(plan.ordered_commits.len());
        for commit_id in plan.ordered_commits.iter().rev() {
            commits.push(load_commit_by_id(branch_store, commit_id).await?);
        }
        let CollectedCommitData {
            flakes: inverted,
            namespace_delta,
            graph_delta,
        } = collect_from_commits(commits, |f| f.invert_at(0));

        let target_state = self
            .load_queryable_state_with_store(branch_store.clone(), branch_record)
            .await?;

        let staged = self
            .apply_two_way_strategy(inverted, conflict_keys, strategy, &target_state)
            .await?;

        // If every reverted flake was a conflict and the strategy dropped
        // them all (e.g. TakeBranch with full overlap), there is nothing to
        // commit. Return a no-op outcome rather than letting `commit()`
        // reject the empty transaction.
        if staged.is_empty() {
            return Ok(RevertWriteOutcome::NoOp {
                head_t: target_state.t(),
                head_id: target_state
                    .head_commit_id
                    .clone()
                    .ok_or_else(|| ApiError::internal("branch has no head commit id"))?,
            });
        }

        let txn_meta: Vec<TxnMetaEntry> = plan
            .ordered_commits
            .iter()
            .map(|commit_id| {
                TxnMetaEntry::new(
                    FLUREE_DB,
                    fluree_vocab::db::REVERTS,
                    TxnMetaValue::string(commit_id.to_string()),
                )
            })
            .collect();

        let reverse_graph = target_state.snapshot.build_reverse_graph().map_err(|e| {
            ApiError::internal(format!("Failed to build reverse graph during revert: {e}"))
        })?;

        let view = StagedLedger::new(target_state, staged, &reverse_graph).map_err(|e| {
            ApiError::internal(format!("Failed to stage flakes during revert: {e}"))
        })?;

        let ns_registry = NamespaceRegistry::from_db(view.db());
        let mut commit_opts = CommitOpts::default().with_txn_meta(txn_meta);
        if !namespace_delta.is_empty() {
            commit_opts = commit_opts.with_namespace_delta(namespace_delta);
        }
        if !graph_delta.is_empty() {
            commit_opts = commit_opts.with_graph_delta(graph_delta);
        }

        let content_store = self.content_store(branch_id);
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

        Ok(RevertWriteOutcome::Wrote(receipt))
    }
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// Caller-supplied source of the commit list, with [`CommitRef`]s still
/// unresolved.
pub(crate) enum RevertSource {
    Commits(NonEmpty<CommitRef>),
    Range { from: CommitRef, to: CommitRef },
}

impl RevertSource {
    /// Wrap a single [`CommitRef`] as a one-element source.
    pub(crate) fn single(commit: CommitRef) -> Self {
        Self::Commits(NonEmpty::from(commit))
    }

    /// Wrap a non-empty list of [`CommitRef`]s; returns `None` if `commits`
    /// is empty so callers must validate at the boundary.
    pub(crate) fn try_set(commits: Vec<CommitRef>) -> Option<Self> {
        NonEmpty::try_from_vec(commits).map(Self::Commits)
    }

    /// Build a git-style range source.
    pub(crate) fn range(from: CommitRef, to: CommitRef) -> Self {
        Self::Range { from, to }
    }
}

/// Everything [`Fluree::revert_inner`] needs after resolution and validation
/// but before mutating state. Shared with the preview path.
pub(crate) struct RevertContext {
    pub(crate) branch_id: String,
    pub(crate) branch_record: fluree_db_nameservice::NsRecord,
    pub(crate) branch_store: BranchedContentStore,
    pub(crate) plan: RevertPlan,
    pub(crate) conflict_keys: Vec<ConflictKey>,
}

/// Source after [`CommitRef`] resolution, ready for plan computation.
enum ResolvedSource {
    Commits(NonEmpty<CommitId>),
    Range { from: CommitId, to: CommitId },
}

/// The resolved set of commits to revert.
pub(crate) struct RevertPlan {
    /// Reverted commits ordered newest-first by `t` (application order).
    pub(crate) ordered_commits: NonEmpty<CommitId>,
    /// Same set as `ordered_commits`, indexed for membership checks.
    pub(crate) reverted_set: FxHashSet<CommitId>,
    /// `t` of the oldest reverted commit. Used as the lower bound when
    /// scanning intervening commits for conflicts.
    pub(crate) oldest_t: i64,
}

/// Internal result of attempting to write the revert commit.
enum RevertWriteOutcome {
    /// Revert commit was written.
    Wrote(CommitReceipt),
    /// Conflict resolution dropped every staged flake; no commit was written.
    NoOp { head_t: i64, head_id: CommitId },
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn resolve_revert_plan<C: ContentStore + ?Sized>(
    store: &C,
    head_id: &CommitId,
    source: &ResolvedSource,
) -> Result<RevertPlan> {
    // Walk the branch's full ancestry once for reachability validation.
    // `dag` is newest-first.
    let dag = collect_dag_cids(store, head_id, 0).await?;
    let dag_index: FxHashMap<CommitId, i64> = dag.iter().cloned().map(|(t, c)| (c, t)).collect();

    // Build a list of (t, commit_id) pairs to revert. Each branch produces
    // a non-empty result (the cherry-pick branch via `NonEmpty`, the range
    // branch via an explicit empty-check on the expansion).
    let with_t: NonEmpty<(i64, CommitId)> = match source {
        ResolvedSource::Commits(commits) => {
            let mut out: Vec<(i64, CommitId)> = Vec::with_capacity(commits.len());
            let mut seen = FxHashSet::default();
            for commit_id in commits.iter() {
                if !seen.insert(commit_id.clone()) {
                    continue;
                }
                let t = dag_index.get(commit_id).copied().ok_or_else(|| {
                    ApiError::InvalidBranch(format!(
                        "Commit {commit_id} is not reachable from the branch HEAD"
                    ))
                })?;
                out.push((t, commit_id.clone()));
            }
            // De-duplication preserves at least one element since the input
            // `NonEmpty<CommitRef>` had at least one and we never drop the
            // first occurrence of each unique commit.
            NonEmpty::try_from_vec(out).expect("dedup of NonEmpty input is non-empty")
        }
        ResolvedSource::Range { from, to } => {
            let to_t = dag_index.get(to).copied().ok_or_else(|| {
                ApiError::InvalidBranch(format!(
                    "Range endpoint {to} is not reachable from the branch HEAD"
                ))
            })?;
            let from_t = dag_index.get(from).copied().ok_or_else(|| {
                ApiError::InvalidBranch(format!(
                    "Range endpoint {from} is not reachable from the branch HEAD"
                ))
            })?;
            if from_t >= to_t {
                return Err(ApiError::InvalidBranch(format!(
                    "Range start {from} is not an ancestor of {to}"
                )));
            }
            // Confirm `from` is on `to`'s ancestry path, then collect (from, to].
            let to_ancestry = collect_dag_cids(store, to, 0).await?;
            let to_set: FxHashSet<CommitId> = to_ancestry.iter().map(|(_, c)| c.clone()).collect();
            if !to_set.contains(from) {
                return Err(ApiError::InvalidBranch(format!(
                    "Range start {from} is not an ancestor of {to}"
                )));
            }
            let collected: Vec<(i64, CommitId)> = to_ancestry
                .into_iter()
                .filter(|(t, c)| c != from && *t > from_t)
                .collect();
            NonEmpty::try_from_vec(collected).ok_or_else(|| {
                ApiError::InvalidBranch("Revert range selects zero commits".to_string())
            })?
        }
    };

    for (_, commit_id) in with_t.iter() {
        let env = load_commit_envelope_by_id(store, commit_id).await?;
        match env.parents.len() {
            0 => {
                return Err(ApiError::InvalidBranch(format!(
                    "Cannot revert genesis commit {commit_id}"
                )));
            }
            1 => {}
            _ => {
                return Err(ApiError::InvalidBranch(format!(
                    "Cannot revert merge commit {commit_id}; reverting merges is not yet supported"
                )));
            }
        }
    }

    // Drop into a plain Vec for sorting (`NonEmpty` deliberately exposes no
    // mutation), then wrap the result back up. Non-emptiness is preserved
    // because sort doesn't change length.
    let mut sorted: Vec<(i64, CommitId)> = with_t.into_vec();
    sorted.sort_by_key(|entry| std::cmp::Reverse(entry.0));

    // After sorting newest-first, the last element holds the oldest `t`.
    let oldest_t = sorted
        .last()
        .map(|(t, _)| *t)
        .expect("non-empty preserved through sort");

    let commit_ids: Vec<CommitId> = sorted.into_iter().map(|(_, c)| c).collect();
    let reverted_set: FxHashSet<CommitId> = commit_ids.iter().cloned().collect();
    let ordered_commits =
        NonEmpty::try_from_vec(commit_ids).expect("non-empty preserved through map");

    Ok(RevertPlan {
        ordered_commits,
        reverted_set,
        oldest_t,
    })
}

/// Compute the conflict keys: `(s, p, g)` tuples touched by the reverted set
/// that are also touched by intervening commits not in the revert set.
async fn compute_conflict_keys<C: ContentStore + Clone + 'static>(
    store: &C,
    head_id: &CommitId,
    reverted_set: &FxHashSet<CommitId>,
    oldest_t: i64,
) -> Result<Vec<ConflictKey>> {
    // stop_at_t = oldest_t - 1 → include every commit with t >= oldest_t.
    let stop = oldest_t.saturating_sub(1);
    let stream = trace_commits_by_id(store.clone(), head_id.clone(), stop);
    futures::pin_mut!(stream);

    let mut reverted_keys: FxHashSet<ConflictKey> = FxHashSet::default();
    let mut intervening_keys: FxHashSet<ConflictKey> = FxHashSet::default();

    while let Some(commit) = stream.try_next().await? {
        let commit_id = commit.id.as_ref().expect("loaded commit must have id set");
        let dest = if reverted_set.contains(commit_id) {
            &mut reverted_keys
        } else {
            &mut intervening_keys
        };
        for f in &commit.flakes {
            dest.insert(ConflictKey::new(f.s.clone(), f.p.clone(), f.g.clone()));
        }
    }

    let mut conflicts: Vec<ConflictKey> = reverted_keys
        .intersection(&intervening_keys)
        .cloned()
        .collect();
    conflicts.sort();
    Ok(conflicts)
}
