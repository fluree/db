//! Branch merge support.
//!
//! Merges a source branch into a target branch. Supports both fast-forward
//! merges (target HEAD is the common ancestor) and general merges with
//! conflict resolution strategies.

use crate::error::{ApiError, Result};
use crate::rebase::ConflictStrategy;
use fluree_db_core::commit::codec::read_commit_envelope;
use fluree_db_core::content_kind::ContentKind;
use fluree_db_core::ledger_id::format_ledger_id;
use fluree_db_core::{collect_dag_cids, load_commit_by_id, CommonAncestor};
use fluree_db_core::{ConflictKey, ContentId, ContentStore, Flake};
use fluree_db_ledger::{LedgerState, StagedLedger};
use fluree_db_nameservice::{NsRecord, NsRecordSnapshot};
use fluree_db_novelty::compute_delta_keys;
use fluree_db_transact::{CommitOpts, NamespaceRegistry};
use rustc_hash::FxHashSet;
use serde::Serialize;
use tracing::Instrument;

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
            self.merge_branch_inner(ledger_name, source_branch, target_branch, strategy)
                .await
        }
        .instrument(span)
        .await
    }

    async fn merge_branch_inner(
        &self,
        ledger_name: &str,
        source_branch: &str,
        target_branch: Option<&str>,
        strategy: ConflictStrategy,
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

        // Compute common ancestor to determine fast-forward eligibility.
        // Build a BranchedContentStore for the source so we can walk both
        // commit chains through parent namespaces.
        let source_store = LedgerState::build_branched_store(
            &self.nameservice_mode,
            &source_record,
            self.backend(),
        )
        .await?;

        let target_head = target_record.commit_head_id.as_ref();
        let ancestor = match target_head {
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
        let is_fast_forward = match (&ancestor, target_head) {
            (Some(a), Some(tid)) => a.commit_id == *tid,
            (None, None) => true,
            _ => false,
        };

        // Snapshot target nameservice state before mutations.
        // If any step fails after publish_commit, we roll back.
        let target_snapshot = NsRecordSnapshot::from_record(&target_record);

        // Disconnect target from ledger manager to prevent stale reads.
        if let Some(ref lm) = self.ledger_manager {
            lm.disconnect(&target_id).await;
        }

        let result: Result<MergeReport> = if is_fast_forward {
            let stop_at_t = ancestor.map(|a| a.t).unwrap_or(0);
            self.fast_forward_merge(
                source_branch,
                &source_record,
                &source_head_id,
                source_head_t,
                resolved_target,
                &target_id,
                stop_at_t,
                &source_store,
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

        match result {
            Ok(report) => Ok(report),
            Err(e) => {
                tracing::warn!(
                    source = %source_id,
                    target = %target_id,
                    error = %e,
                    "merge failed, rolling back nameservice state"
                );
                if let Err(rollback_err) = self
                    .nameservice()
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
    ) -> Result<MergeReport> {
        // Copy commit and txn blobs from the source namespace into the target
        // namespace so the target is self-contained (no fallback reads needed).
        // We use the branched content store so that collect_dag_cids can read
        // parent commits from ancestor namespaces when checking stop_at_t.
        let commits_copied = self
            .copy_commit_chain(source_store, source_head_id, stop_at_t, target_id)
            .await?;

        // Advance target's HEAD to source's HEAD.
        self.publisher()?
            .publish_commit(target_id, source_head_t, source_head_id)
            .await?;

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

        // Compute source delta: all (s,p,g) tuples modified on source since ancestor.
        let source_delta =
            compute_delta_keys(source_store.clone(), source_head_id.clone(), ancestor.t).await?;

        // Compute target delta. Build a branched store if target is also a branch.
        let target_delta = if target_record.source_branch.is_some() {
            let target_store = LedgerState::build_branched_store(
                &self.nameservice_mode,
                target_record,
                self.backend(),
            )
            .await?;
            compute_delta_keys(target_store, target_head_id.clone(), ancestor.t).await?
        } else {
            let target_store = self.content_store(target_id);
            compute_delta_keys(target_store, target_head_id.clone(), ancestor.t).await?
        };

        // Find conflicts: intersection of source and target delta sets.
        let conflicts: Vec<ConflictKey> =
            source_delta.intersection(&target_delta).cloned().collect();

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

        // Load target state for staging the merge commit.
        let target_state =
            LedgerState::load(&self.nameservice_mode, target_id, self.backend()).await?;

        // Collect source flakes and metadata: walk source commits from HEAD
        // to ancestor, gathering flakes, namespace deltas, and graph deltas.
        let source_data = collect_commit_data(source_store, source_head_id, ancestor.t).await?;

        // Resolve conflicts.
        let resolved_flakes = self
            .resolve_merge_flakes(&source_data.flakes, &conflicts, strategy, &target_state)
            .await?;

        // Stage resolved flakes onto target state. An empty flake set is valid
        // (e.g., TakeBranch drops all source flakes) — we still create the merge
        // commit to record the parent relationship and prevent future re-merges.
        let reverse_graph = target_state.snapshot.build_reverse_graph().map_err(|e| {
            ApiError::internal(format!("Failed to build reverse graph during merge: {e}"))
        })?;

        let view = StagedLedger::new(target_state, resolved_flakes, &reverse_graph)
            .map_err(|e| ApiError::internal(format!("Failed to stage flakes during merge: {e}")))?;

        // Create merge commit with the source head as an additional parent,
        // propagating namespace and graph deltas from the source branch.
        let ns_registry = NamespaceRegistry::from_db(view.db());
        let mut commit_opts =
            CommitOpts::default().with_merge_parents(vec![source_head_id.clone()]);
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
