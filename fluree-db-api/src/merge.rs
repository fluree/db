//! Branch merge support.
//!
//! Merges a source branch into a target branch. Supports both fast-forward
//! merges (target HEAD is the common ancestor) and general merges with
//! conflict resolution strategies.

use crate::commit_data::{collect_from_commits, CollectedCommitData};
use crate::error::{ApiError, Result};
use crate::ledger_manager::GuardedStagedCommit;
use crate::rebase::ConflictStrategy;
use fluree_db_core::commit::codec::read_commit_envelope;
use fluree_db_core::content_kind::ContentKind;
use fluree_db_core::ledger_id::format_ledger_id;
use fluree_db_core::{collect_dag_cids, load_commit_by_id, CommonAncestor};
use fluree_db_core::{BranchedContentStore, ConflictKey, ContentId, ContentStore};
use fluree_db_ledger::{LedgerState, StagedLedger};
use fluree_db_nameservice::{NsRecord, NsRecordSnapshot};
use fluree_db_novelty::compute_delta_keys;
use fluree_db_transact::{CommitOpts, NamespaceRegistry};
use serde::Serialize;
use tracing::Instrument;

/// Output of [`Fluree::prepare_merge`].
///
/// Bundles everything either apply path needs: receipt metadata
/// (`target`, `source`, `fast_forward`, `commits_copied`,
/// `conflict_count`, `strategy`), the `rollback_snapshot` the local
/// path uses to undo a partial nameservice publish on apply failure,
/// the target's head observed under the lock (as `expected_prev` for
/// the CAS / `AdvanceRef`), the new head the apply should land on,
/// and — when this isn't a fast-forward — the
/// [`GuardedStagedCommit`] carrying the merge commit body to write.
///
/// Callers route on `commit`:
/// - `Some(_)` → apply (locally via [`StagedCommit::apply`], or in
///   Raft mode by writing the merge commit blob + proposing
///   `AdvanceRef`).
/// - `None` → fast-forward: just advance the ref from
///   `current_head_*` to `new_head_*`.
///
/// [`StagedCommit::apply`]: fluree_db_transact::StagedCommit::apply
pub struct StagedMerge {
    /// Resolved target branch name (without ledger prefix).
    pub target: String,
    /// Source branch name (without ledger prefix).
    pub source: String,
    /// Fully-qualified target id (`"<ledger>:<target>"`).
    pub target_id: String,
    /// Fully-qualified source id (`"<ledger>:<source>"`).
    pub source_id: String,
    /// `true` when the target's HEAD was the common ancestor — the
    /// apply step just advances the ref, no new commit body to write.
    pub fast_forward: bool,
    /// Number of `(s, p, g)` conflicts the strategy resolved.
    /// Always `0` for fast-forward.
    pub conflict_count: usize,
    /// Conflict strategy carried through to the receipt. `None` for
    /// fast-forward (where no strategy was applied).
    pub strategy: Option<ConflictStrategy>,
    /// Number of source commits copied into the target's namespace
    /// during the build phase.
    pub commits_copied: usize,
    /// Target's head snapshot. The local path passes this to
    /// `RefPublisher::reset_head` to undo a partial publish on apply
    /// failure. The Raft path ignores it (no publish happens until
    /// `AdvanceRef` applies through consensus).
    pub rollback_snapshot: NsRecordSnapshot,
    /// Target's head before the merge. `expected_prev` for the CAS /
    /// `AdvanceRef` proposal.
    pub current_head_t: i64,
    /// Companion to [`Self::current_head_t`]. `None` if the target
    /// branch was empty (genesis case).
    pub current_head_id: Option<ContentId>,
    /// Target's head after the merge — for fast-forward this is the
    /// source's head; for general merge this is the merge commit's
    /// id.
    pub new_head_t: i64,
    /// Companion to [`Self::new_head_t`].
    pub new_head_id: ContentId,
    /// `Some` for general merge with a fresh merge commit to apply;
    /// `None` for fast-forward (the ref just advances to
    /// `new_head_*`).
    pub commit: Option<GuardedStagedCommit>,
    /// Source's index ref (if any). Best-effort copy after the ref
    /// advance so the target reuses the source's index. Only set for
    /// fast-forward — general merge invalidates the source's index.
    pub source_index_for_publish: Option<(ContentId, i64)>,
    /// Source ledger id used as the source for any best-effort
    /// post-apply index copy. Carried through so the apply path can
    /// address content-store namespaces without a re-lookup.
    pub source_ledger_id: String,
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
    /// Merge a source branch into a target branch.
    ///
    /// Supports fast-forward merges (when the target has not diverged) and
    /// general merges with conflict resolution via the given `strategy`.
    ///
    /// If `target_branch` is `None`, the source's parent branch (from its
    /// branch point) is used as the target.
    pub async fn merge_branch(
        &self,
        ledger_name: &str,
        source_branch: &str,
        target_branch: Option<&str>,
        strategy: ConflictStrategy,
    ) -> Result<MergeReport> {
        let span = tracing::debug_span!("merge_branch", ledger_name, source_branch, ?target_branch);
        async move {
            let staged = self
                .prepare_merge(ledger_name, source_branch, target_branch, strategy)
                .await?;
            let source_id = staged.source_id.clone();
            let target_id = staged.target_id.clone();
            let target_snapshot = staged.rollback_snapshot.clone();

            match self.apply_merge(staged).await {
                Ok(report) => Ok(report),
                Err(e) => {
                    tracing::warn!(
                        source = %source_id,
                        target = %target_id,
                        error = %e,
                        "merge failed, rolling back nameservice state"
                    );
                    if let Err(rollback_err) = self
                        .branch_admin()?
                        .reset_head(&target_id, target_snapshot)
                        .await
                    {
                        tracing::error!(
                            target = %target_id,
                            error = %rollback_err,
                            "failed to roll back target nameservice state after merge failure"
                        );
                    }
                    Err(e)
                }
            }
        }
        .instrument(span)
        .await
    }

    /// Validate and build the merge up to (but not including) the
    /// commit-blob write + ref advance. Returns a [`StagedMerge`] the
    /// caller can then apply via [`Self::apply_merge`] (local
    /// path) or by writing the merge commit blob + proposing
    /// `AdvanceRef` through consensus (Raft path).
    ///
    /// Performs all the work that's the same for both the
    /// fast-forward and general merge shapes — resolution, common
    /// ancestor, cache disconnect, copying source commits into the
    /// target namespace — and then either:
    /// - Returns a fast-forward [`StagedMerge`] with `commit: None`
    ///   and `new_head_*` set to the source's head (caller just
    ///   advances the ref).
    /// - Computes deltas, applies the conflict strategy, builds a
    ///   merge commit, and returns it inside
    ///   [`StagedMerge::commit`].
    ///
    /// Errors with [`ApiError::InvalidBranch`] for missing source
    /// parent, self-merge, empty source, or `Skip` strategy; with
    /// [`ApiError::NotFound`] for missing source/target records; with
    /// [`ApiError::BranchConflict`] when `Abort` meets real
    /// conflicts.
    pub async fn prepare_merge(
        &self,
        ledger_name: &str,
        source_branch: &str,
        target_branch: Option<&str>,
        strategy: ConflictStrategy,
    ) -> Result<StagedMerge> {
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

        let resolved_target = target_branch.unwrap_or(source_parent).to_string();

        if source_branch == resolved_target {
            return Err(ApiError::InvalidBranch(
                "Cannot merge a branch into itself".to_string(),
            ));
        }

        let target_id = format_ledger_id(ledger_name, &resolved_target);
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

        // Compute common ancestor to determine fast-forward eligibility.
        // Build a BranchedContentStore for the source so we can walk both
        // commit chains through parent namespaces.
        let source_store = LedgerState::build_branched_store(
            &self.nameservice_mode,
            &source_record,
            self.backend(),
        )
        .await?;

        let target_head = target_record.commit_head_id.clone();
        let ancestor = match target_head.as_ref() {
            Some(target_head_id) => Some(
                fluree_db_core::find_common_ancestor(
                    &source_store,
                    &source_head_id,
                    target_head_id,
                )
                .await?,
            ),
            None => None,
        };

        // Fast-forward check: target HEAD must be the common ancestor.
        let is_fast_forward = match (&ancestor, target_head.as_ref()) {
            (Some(a), Some(tid)) => a.commit_id == *tid,
            (None, None) => true,
            _ => false,
        };

        // Snapshot target nameservice state before mutations. The
        // local apply path passes this to `reset_head` to roll back
        // on apply failure.
        let rollback_snapshot = NsRecordSnapshot::from_record(&target_record);

        // Disconnect target from ledger manager to prevent stale reads.
        if let Some(ref lm) = self.ledger_manager {
            lm.disconnect(&target_id).await;
        }

        if is_fast_forward {
            self.build_merge_ff(
                source_branch,
                &resolved_target,
                source_id,
                target_id,
                &source_record,
                &source_store,
                ancestor.as_ref(),
                source_head_id,
                source_head_t,
                rollback_snapshot,
                target_head,
            )
            .await
        } else {
            let ancestor = ancestor.expect("ancestor must exist when both heads are Some");
            self.build_merge_general(
                source_branch,
                &resolved_target,
                source_id,
                target_id,
                &source_record,
                &target_record,
                &source_store,
                source_head_id,
                &ancestor,
                strategy,
                rollback_snapshot,
                target_head,
            )
            .await
        }
    }

    /// Apply a [`StagedMerge`] through the local commit pipeline.
    /// Returns the [`MergeReport`] regardless of whether the apply
    /// was a fast-forward or a general merge.
    async fn apply_merge(&self, staged: StagedMerge) -> Result<MergeReport> {
        let StagedMerge {
            target,
            source,
            target_id,
            fast_forward,
            conflict_count,
            strategy,
            commits_copied,
            current_head_t: _,
            current_head_id: _,
            new_head_t,
            new_head_id,
            commit,
            source_index_for_publish,
            source_ledger_id,
            ..
        } = staged;

        let (new_head_t, new_head_id) = match commit {
            Some(GuardedStagedCommit {
                write_guard,
                staged: staged_commit,
            }) => {
                let content_store = self.content_store(&target_id);
                let publisher = self.publisher()?;
                let (receipt, new_state) = staged_commit
                    .apply(&content_store, publisher, false)
                    .await?;

                if let Some(guard) = write_guard {
                    let needs_reindex = new_state.should_reindex(&self.index_config);
                    self.finalize_commit(guard, new_state, receipt.t, needs_reindex)
                        .await?;
                }
                (receipt.t, receipt.commit_id)
            }
            None => {
                // Fast-forward: advance target's HEAD to the source's head.
                self.publisher()?
                    .publish_commit(&target_id, new_head_t, &new_head_id)
                    .await?;
                (new_head_t, new_head_id)
            }
        };

        // Best-effort: copy source's index into the target namespace
        // (and, for fast-forward, publish the index ref too). Errors
        // here only warn — the target can rebuild from commits.
        if let Some((index_cid, index_t)) = source_index_for_publish {
            if let Err(e) = self
                .copy_index_to_branch(&source_ledger_id, &target_id, &index_cid)
                .await
            {
                tracing::warn!(
                    %e, source = %source_ledger_id, target = %target_id,
                    "failed to copy index during merge; target will rebuild from commits"
                );
            } else if fast_forward {
                if let Err(e) = self
                    .publisher()?
                    .publish_index(&target_id, index_t, &index_cid)
                    .await
                {
                    tracing::warn!(%e, "failed to publish index for merged target");
                }
            }
        }

        Ok(MergeReport {
            target,
            source,
            fast_forward,
            new_head_t,
            new_head_id,
            commits_copied,
            conflict_count,
            strategy: strategy.map(|s| s.as_str().to_string()),
        })
    }

    /// Fast-forward merge: copy commits from source to target and
    /// return a `StagedMerge` whose apply step just advances the
    /// target's HEAD ref.
    #[allow(clippy::too_many_arguments)]
    async fn build_merge_ff(
        &self,
        source_branch: &str,
        resolved_target: &str,
        source_id: String,
        target_id: String,
        source_record: &NsRecord,
        source_store: &impl ContentStore,
        ancestor: Option<&CommonAncestor>,
        source_head_id: ContentId,
        source_head_t: i64,
        rollback_snapshot: NsRecordSnapshot,
        target_head: Option<ContentId>,
    ) -> Result<StagedMerge> {
        let stop_at_t = ancestor.map(|a| a.t).unwrap_or(0);
        let commits_copied = self
            .copy_commit_chain(source_store, &source_head_id, stop_at_t, &target_id)
            .await?;

        let current_head_t = ancestor.map(|a| a.t).unwrap_or(0);
        let source_index_for_publish = source_record
            .index_head_id
            .as_ref()
            .map(|cid| (cid.clone(), source_record.index_t));

        Ok(StagedMerge {
            target: resolved_target.to_string(),
            source: source_branch.to_string(),
            target_id,
            source_ledger_id: source_record.ledger_id.clone(),
            source_id,
            fast_forward: true,
            conflict_count: 0,
            strategy: None,
            commits_copied,
            rollback_snapshot,
            current_head_t,
            current_head_id: target_head,
            new_head_t: source_head_t,
            new_head_id: source_head_id,
            commit: None,
            source_index_for_publish,
        })
    }

    /// General (non-fast-forward) merge: compute deltas, detect
    /// conflicts, resolve them, build a merge commit on the target
    /// branch — but do not write the commit blob and do not publish.
    /// Returns the staged commit inside [`StagedMerge::commit`].
    #[allow(clippy::too_many_arguments)]
    async fn build_merge_general(
        &self,
        source_branch: &str,
        resolved_target: &str,
        source_id: String,
        target_id: String,
        source_record: &NsRecord,
        target_record: &NsRecord,
        source_store: &BranchedContentStore,
        source_head_id: ContentId,
        ancestor: &CommonAncestor,
        strategy: ConflictStrategy,
        rollback_snapshot: NsRecordSnapshot,
        target_head: Option<ContentId>,
    ) -> Result<StagedMerge> {
        // Skip is not supported for merge (it only makes sense for per-commit rebase).
        if strategy == ConflictStrategy::Skip {
            return Err(ApiError::InvalidBranch(
                "Skip strategy is not supported for merge".to_string(),
            ));
        }

        let target_head_id = target_record
            .commit_head_id
            .as_ref()
            .expect("target must have head for non-fast-forward merge");

        // Compute source delta: all (s,p,g) tuples modified on source since ancestor.
        let source_delta =
            compute_delta_keys(source_store.clone(), source_head_id.clone(), ancestor.t).await?;

        // Compute target delta. Use the same branch-aware store below when
        // loading the queryable target state for staging.
        let target_store: BranchedContentStore = if target_record.source_branch.is_some() {
            LedgerState::build_branched_store(&self.nameservice_mode, target_record, self.backend())
                .await?
        } else {
            BranchedContentStore::leaf(self.content_store(&target_id))
        };
        let target_delta =
            compute_delta_keys(target_store.clone(), target_head_id.clone(), ancestor.t).await?;

        // Find conflicts: intersection of source and target delta sets.
        let conflicts: Vec<ConflictKey> =
            source_delta.intersection(&target_delta).cloned().collect();

        let conflict_count = conflicts.len();

        // Abort if conflicts exist and strategy is Abort.
        if strategy == ConflictStrategy::Abort && !conflicts.is_empty() {
            return Err(ApiError::BranchConflict(format!(
                "Merge aborted: {} conflict(s) between {} and {} with abort strategy",
                conflicts.len(),
                source_branch,
                resolved_target,
            )));
        }

        // Acquire target state under the write lock when a manager is
        // available, serializing with regular transactions on the target
        // branch. Without a manager (embedded use, no shared cache), fall
        // back to a fresh storage load — there's nothing to protect.
        let (write_guard, target_state) = self
            .lock_or_load(&target_id, target_store, target_record.clone())
            .await?;

        // Collect source flakes and metadata: walk source commits from HEAD
        // to ancestor, gathering flakes, namespace deltas, and graph deltas.
        let CollectedCommitData {
            flakes: source_flakes,
            namespace_delta,
            graph_delta,
        } = collect_commit_data(source_store, &source_head_id, ancestor.t).await?;

        // Resolve conflicts via the shared two-way strategy helper.
        let resolved_flakes = self
            .apply_two_way_strategy(source_flakes, &conflicts, &strategy, &target_state)
            .await?;

        // Stage resolved flakes onto target state. An empty flake set is valid
        // (e.g., TakeBranch drops all source flakes) — we still create the merge
        // commit to record the parent relationship and prevent future re-merges.
        let reverse_graph = target_state.snapshot.build_reverse_graph().map_err(|e| {
            ApiError::internal(format!("Failed to build reverse graph during merge: {e}"))
        })?;

        let current_head_t = target_state.t();

        let view = StagedLedger::new(target_state, resolved_flakes, &reverse_graph)
            .map_err(|e| ApiError::internal(format!("Failed to stage flakes during merge: {e}")))?;

        // Create merge commit with the source head as an additional parent,
        // propagating namespace and graph deltas from the source branch.
        let ns_registry = NamespaceRegistry::from_db(view.db());
        let mut commit_opts =
            CommitOpts::default().with_merge_parents(vec![source_head_id.clone()]);
        if !namespace_delta.is_empty() {
            commit_opts = commit_opts.with_namespace_delta(namespace_delta);
        }
        if !graph_delta.is_empty() {
            commit_opts = commit_opts.with_graph_delta(graph_delta);
        }

        // Copy source commit chain to target namespace so the target is
        // self-contained for DAG walking. This must happen before the merge
        // commit is published.
        let commits_copied = self
            .copy_commit_chain(source_store, &source_head_id, ancestor.t, &target_id)
            .await?;

        // With the lock held the staged base is authoritative — derive
        // `expected_head_ref` from it directly, no nameservice round-trip.
        let expected_head_ref =
            view.base()
                .head_commit_id
                .as_ref()
                .map(|cid| fluree_db_nameservice::RefValue {
                    id: Some(cid.clone()),
                    t: view.base().t(),
                });

        let staged_commit = fluree_db_transact::build_commit(
            view,
            ns_registry,
            expected_head_ref,
            None,
            &self.index_config,
            commit_opts,
        )
        .await?;

        let new_head_t = staged_commit.commit.t;
        let new_head_id = staged_commit
            .commit
            .id
            .clone()
            .expect("build_commit guarantees commit.id is set");

        // Source's index is best-effort copied (not published) on
        // general merge — the merge commit advances `index_t` past
        // it, but the binary index data is still useful to have in
        // the target namespace so subsequent indexing can reuse it.
        let source_index_for_publish = source_record
            .index_head_id
            .as_ref()
            .map(|cid| (cid.clone(), source_record.index_t));

        Ok(StagedMerge {
            target: resolved_target.to_string(),
            source: source_branch.to_string(),
            target_id,
            source_ledger_id: source_record.ledger_id.clone(),
            source_id,
            fast_forward: false,
            conflict_count,
            strategy: Some(strategy),
            commits_copied,
            rollback_snapshot,
            current_head_t,
            current_head_id: target_head,
            new_head_t,
            new_head_id,
            commit: Some(GuardedStagedCommit {
                write_guard,
                staged: staged_commit,
            }),
            source_index_for_publish,
        })
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

/// Collect all flakes, namespace deltas, and graph deltas from commits
/// between `head_id` and `stop_at_t` (exclusive). Walks the DAG newest-first
/// then folds via [`collect_from_commits`] in oldest-first order so that
/// earlier commits win on namespace and graph delta key collisions.
async fn collect_commit_data(
    store: &impl ContentStore,
    head_id: &ContentId,
    stop_at_t: i64,
) -> Result<CollectedCommitData> {
    let dag = collect_dag_cids(store, head_id, stop_at_t).await?;
    let mut commits = Vec::with_capacity(dag.len());
    for (_, cid) in dag.iter().rev() {
        commits.push(load_commit_by_id(store, cid).await?);
    }
    Ok(collect_from_commits(commits, std::convert::identity))
}
