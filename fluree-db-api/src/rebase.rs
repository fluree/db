//! Branch rebase support
//!
//! Provides types and orchestration for replaying a branch's commits on top
//! of its source branch's current HEAD, bringing the branch up to date with
//! upstream changes.

use crate::error::{ApiError, Result};
use fluree_db_core::ledger_id::format_ledger_id;
use fluree_db_core::{
    range_with_overlay, ConflictKey, ContentId, Flake, IndexType, RangeMatch, RangeOptions,
    RangeTest, DEFAULT_GRAPH_ID,
};
use fluree_db_core::{trace_commits_by_id, Commit};
use fluree_db_ledger::{LedgerState, StagedLedger};
use fluree_db_nameservice::NsRecordSnapshot;
use fluree_db_novelty::compute_delta_keys;
use fluree_db_transact::{CommitOpts, NamespaceRegistry, StagedCommit};
use futures::TryStreamExt;
use rustc_hash::FxHashSet;
use serde::{Deserialize, Serialize};
use tracing::Instrument;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Strategy for resolving conflicts when branch and source modifications
/// overlap on the same (subject, predicate, graph) tuple.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictStrategy {
    /// Replay as-is, both values coexist (multi-cardinality). Default.
    #[default]
    TakeBoth,
    /// Fail on first conflict, no changes applied.
    Abort,
    /// Drop branch's conflicting flakes from the replayed commit (source wins).
    TakeSource,
    /// Keep branch's flakes and add retractions for source's conflicting values (branch wins).
    TakeBranch,
    /// Skip the entire commit if any flakes conflict.
    Skip,
}

impl ConflictStrategy {
    /// Parse a canonical strategy name from a string.
    ///
    /// Unlike [`Self::from_str_name`], this intentionally rejects aliases such
    /// as `ours` and `theirs` for API surfaces that require a strict wire
    /// contract.
    pub fn parse_canonical(s: &str) -> std::result::Result<Self, String> {
        match s {
            "take-both" => Ok(Self::TakeBoth),
            "abort" => Ok(Self::Abort),
            "take-source" => Ok(Self::TakeSource),
            "take-branch" => Ok(Self::TakeBranch),
            "skip" => Ok(Self::Skip),
            _ => Err(format!("Unknown conflict strategy: {s}")),
        }
    }

    /// Parse a strategy name from a string (case-insensitive).
    pub fn from_str_name(s: &str) -> Option<Self> {
        let normalized = s.to_lowercase();
        if let Ok(strategy) = Self::parse_canonical(&normalized) {
            return Some(strategy);
        }

        match normalized.as_str() {
            "takeboth" | "take_both" => Some(Self::TakeBoth),
            "takesource" | "take_source" | "theirs" => Some(Self::TakeSource),
            "takebranch" | "take_branch" | "ours" => Some(Self::TakeBranch),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::TakeBoth => "take-both",
            Self::Abort => "abort",
            Self::TakeSource => "take-source",
            Self::TakeBranch => "take-branch",
            Self::Skip => "skip",
        }
    }
}

/// Record of a conflict detected during rebase of a single commit.
#[derive(Clone, Debug, Serialize)]
pub struct RebaseConflict {
    pub original_t: i64,
    #[serde(skip)]
    pub keys: Vec<ConflictKey>,
    pub conflict_count: usize,
    pub resolution: &'static str,
}

/// Record of a commit that failed validation after replay.
#[derive(Clone, Debug, Serialize)]
pub struct RebaseFailure {
    pub original_t: i64,
    pub error: String,
}

/// Summary report of a completed rebase operation.
#[derive(Clone, Debug, Serialize)]
pub struct RebaseReport {
    pub replayed: usize,
    pub conflicts: Vec<RebaseConflict>,
    pub failures: Vec<RebaseFailure>,
    pub source_head_t: i64,
    pub source_head_id: ContentId,
    pub fast_forward: bool,
    pub total_commits: usize,
    pub skipped: usize,
}

/// Output of [`Fluree::prepare_rebase`].
///
/// Bundles everything the apply phase needs — receipt metadata
/// (counts of `replayed`/`skipped`/`conflicts`/`total_commits`,
/// `fast_forward`, source head info), the rollback snapshot and
/// write guard for local-mode bookkeeping, the ref endpoints the
/// apply will advance between, and the list of built-but-unwritten
/// commit blobs (`pending_replays`) the apply phase atomically writes
/// before the single ref advance.
///
/// Callers route on `new_head_id`:
/// - `Some(_)` → write all `pending_replays` then advance the branch
///   HEAD from `pre_rebase_head_id` to `new_head_id` (one operation
///   atomically; local: `publish_commit`; Raft: `AdvanceRef`).
/// - `None` → all source commits were skipped (`Skip` strategy on
///   every conflicting commit, or every commit's flake set was
///   empty after resolution); nothing to apply, return the report
///   with `replayed == 0`.
pub struct StagedRebase {
    /// Branch being rebased (without ledger prefix).
    pub branch: String,
    /// Fully-qualified branch id (`"<ledger>:<branch>"`).
    pub branch_id: String,
    /// Source branch name (without ledger prefix).
    pub source: String,
    /// Fully-qualified source id.
    pub source_id: String,
    /// Source's current head ref. For fast-forward, this is also
    /// what the branch's HEAD advances to (`new_head_*` reflects
    /// it).
    pub source_head_id: ContentId,
    /// Companion to [`Self::source_head_id`].
    pub source_head_t: i64,
    /// `true` when the branch is already at the common ancestor —
    /// the apply phase just fast-forwards branch HEAD to source's
    /// HEAD. `pending_replays` is empty in this case (no new commits
    /// to write).
    pub fast_forward: bool,
    /// Total number of source commits considered (replayed +
    /// skipped + had-no-effect).
    pub total_commits: usize,
    /// Number of replays the build phase produced (length of
    /// `pending_replays`).
    pub replayed: usize,
    /// Number of source commits the `Skip` strategy dropped due to
    /// conflict.
    pub skipped: usize,
    /// Per-commit conflict records (independent of strategy).
    pub conflicts: Vec<RebaseConflict>,
    /// Branch's pre-rebase head snapshot. The local apply path
    /// passes this to `RefPublisher::reset_head` to roll back a
    /// partial publish on apply failure. The Raft apply path
    /// ignores it (the `AdvanceRef` proposal either applies cleanly
    /// or doesn't apply at all).
    pub rollback_snapshot: NsRecordSnapshot,
    /// Branch's pre-rebase head (CAS `expected_prev`). `None` if
    /// the branch was at genesis (rare — usually rebase requires an
    /// existing head, but the build phase produces this either
    /// way).
    pub pre_rebase_head_id: Option<ContentId>,
    /// Companion to [`Self::pre_rebase_head_id`].
    pub pre_rebase_head_t: i64,
    /// Head ref the apply step advances the branch to. `Some` for
    /// fast-forward (= source's head) and active general rebase (=
    /// last replay's id); `None` when every source commit was
    /// skipped or had no effect.
    pub new_head_id: Option<ContentId>,
    /// Companion to [`Self::new_head_id`].
    pub new_head_t: i64,
    /// Ledger write guard held across the build → apply window so
    /// concurrent transactions on the branch are serialized. `None`
    /// when no `LedgerManager` is configured.
    pub write_guard: Option<crate::LedgerWriteGuard>,
    /// Cumulative post-build state used by `finalize_commit` after
    /// the ref advance, so the cache catches up with consensus.
    pub final_state: LedgerState,
    /// Built-but-unwritten replay commits. The apply step writes
    /// them all to the content store in order, then advances the
    /// branch HEAD from `pre_rebase_head_id` directly to the last
    /// blob's `commit_id`. Intermediate commits exist as
    /// content-addressed objects but aren't on the active head
    /// chain until that single advance commits.
    pub pending_replays: Vec<PendingReplay>,
}

/// Built-but-unwritten replay commit. Carried inside
/// [`StagedRebase::pending_replays`]; the apply phase writes each one
/// to the content store before the atomic ref advance.
pub struct PendingReplay {
    /// Commit's content id (used both as the blob key and as the
    /// new branch HEAD when this is the last entry).
    pub commit_id: ContentId,
    /// Canonical serialized bytes (`ContentId::new(Commit, bytes)
    /// == commit_id`).
    pub commit_bytes: Vec<u8>,
    /// Original source commit's `t`. Carried through for tracing /
    /// telemetry; not used by the apply step.
    pub original_t: i64,
}

// ---------------------------------------------------------------------------
// Orchestration
// ---------------------------------------------------------------------------

impl crate::Fluree {
    /// Rebase a branch onto its source branch's current HEAD.
    ///
    /// Replays the branch's unique commits on top of the source's current
    /// state, detecting and resolving conflicts according to `strategy`.
    pub async fn rebase_branch(
        &self,
        ledger_name: &str,
        branch: &str,
        strategy: ConflictStrategy,
    ) -> Result<RebaseReport> {
        let span = tracing::debug_span!(
            "rebase_branch",
            ledger_name,
            branch,
            strategy = strategy.as_str()
        );
        async move {
            let staged = self.prepare_rebase(ledger_name, branch, strategy).await?;
            let branch_id = staged.branch_id.clone();
            let rollback_snapshot = staged.rollback_snapshot.clone();

            match self.apply_rebase(staged).await {
                Ok(report) => Ok(report),
                Err(e) => {
                    tracing::warn!(
                        branch = %branch_id,
                        error = %e,
                        "rebase failed, rolling back nameservice state"
                    );
                    if let Err(rollback_err) = self
                        .branch_admin()?
                        .reset_head(&branch_id, rollback_snapshot)
                        .await
                    {
                        tracing::error!(
                            branch = %branch_id,
                            error = %rollback_err,
                            "failed to roll back nameservice state after rebase failure"
                        );
                    }
                    if let Some(ref lm) = self.ledger_manager {
                        lm.disconnect(&branch_id).await;
                    }
                    Err(e)
                }
            }
        }
        .instrument(span)
        .await
    }

    /// Validate and build the rebase up to (but not including) the
    /// content-store blob writes + ref advance. Returns a
    /// [`StagedRebase`] the caller can then apply via
    /// [`Self::apply_rebase`] (local single-step apply) or by
    /// writing the blobs + proposing a single `AdvanceRef` through
    /// consensus (Raft path).
    ///
    /// Build phase semantics:
    /// - Fast-forward (branch head is the common ancestor):
    ///   [`StagedRebase::pending_replays`] is empty,
    ///   [`StagedRebase::new_head_id`] is the source's head.
    /// - General rebase: each source commit gets replayed in turn,
    ///   each iteration's input being the previous iteration's
    ///   post-commit state. Cumulative novelty is gated against
    ///   [`IndexConfig::reindex_max_bytes`]; exceeding it returns
    ///   422 with structured remediation (reindex source then
    ///   retry, or rebase a smaller commit range). Mid-rebase
    ///   reindex is intentionally not performed (atomicity); the
    ///   post-apply `finalize_commit` reports
    ///   `needs_reindex` and the background indexer rebuilds.
    /// - All-skipped (every conflicting commit dropped by `Skip`,
    ///   or every replay produced empty flakes):
    ///   [`StagedRebase::new_head_id`] is `None`; the apply step
    ///   does nothing.
    ///
    /// Errors with [`ApiError::InvalidBranch`] when called on a
    /// root branch, [`ApiError::NotFound`] when the source can't be
    /// resolved, and [`ApiError::BranchConflict`] when `Abort`
    /// meets real conflicts.
    pub async fn prepare_rebase(
        &self,
        ledger_name: &str,
        branch: &str,
        strategy: ConflictStrategy,
    ) -> Result<StagedRebase> {
        let branch_id = format_ledger_id(ledger_name, branch);
        let branch_record = self
            .nameservice()
            .lookup(&branch_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(branch_id.clone()))?;

        // Refuse the root structurally — there's nothing to rebase onto.
        // "main" carries no special meaning here; a ledger whose root is
        // named "trunk" will be refused on `trunk`, and a non-root branch
        // named "main" can be rebased like any other.
        let source_name = branch_record.source_branch.as_ref().ok_or_else(|| {
            ApiError::InvalidBranch(format!(
                "Cannot rebase '{branch}': it is the root of ledger '{ledger_name}' \
                 (no parent to rebase onto)."
            ))
        })?;

        let source_id = format_ledger_id(ledger_name, source_name);
        let source_record = self
            .nameservice()
            .lookup(&source_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(source_id.clone()))?;

        let source_head_id = source_record.commit_head_id.clone().ok_or_else(|| {
            ApiError::internal(format!("Source branch {source_id} has no commit head"))
        })?;
        let source_head_t = source_record.commit_t;

        // Build a BranchedContentStore for reading commits across namespaces.
        let branch_store = LedgerState::build_branched_store(
            &self.nameservice_mode,
            &branch_record,
            self.backend(),
        )
        .await?;

        // Compute common ancestor by walking commit chains.
        let branch_head_id = branch_record
            .commit_head_id
            .clone()
            .ok_or_else(|| ApiError::internal(format!("Branch {branch_id} has no commit head")))?;
        let ancestor =
            fluree_db_core::find_common_ancestor(&branch_store, &branch_head_id, &source_head_id)
                .await?;

        let pre_rebase_head_t = branch_record.commit_t;
        let pre_rebase_head_id = branch_record.commit_head_id.clone();
        let rollback_snapshot = NsRecordSnapshot::from_record(&branch_record);
        let source_name_owned = source_name.to_string();

        // Fast-forward: branch has no unique commits beyond the ancestor.
        let is_fast_forward = branch_head_id == ancestor.commit_id;

        if is_fast_forward {
            // Copy the source index into the branch namespace
            // (best-effort, matches local single-node behavior).
            self.copy_source_index(&source_id, &branch_id, &source_record)
                .await;

            // Load the source's state under the branch's id so the
            // post-apply cache reflects the fast-forwarded branch.
            let mut final_state = self.ledger(&source_id).await?;
            std::sync::Arc::make_mut(&mut final_state.snapshot).ledger_id = branch_id.clone();

            let write_guard = self.lock_ledger(&branch_id).await?;

            return Ok(StagedRebase {
                branch: branch.to_string(),
                branch_id,
                source: source_name_owned,
                source_id,
                source_head_id: source_head_id.clone(),
                source_head_t,
                fast_forward: true,
                total_commits: 0,
                replayed: 0,
                skipped: 0,
                conflicts: Vec::new(),
                rollback_snapshot,
                pre_rebase_head_id,
                pre_rebase_head_t,
                new_head_id: Some(source_head_id),
                new_head_t: source_head_t,
                write_guard,
                final_state,
                pending_replays: Vec::new(),
            });
        }

        // Compute source delta: all (s,p,g) tuples modified on source since ancestor.
        // The source may itself be a branch, so use a BranchedContentStore if it has
        // a source_branch, otherwise a plain store.
        let source_delta = if source_record.source_branch.is_some() {
            let source_store = LedgerState::build_branched_store(
                &self.nameservice_mode,
                &source_record,
                self.backend(),
            )
            .await?;
            compute_delta_keys(source_store, source_head_id.clone(), ancestor.t).await?
        } else {
            let source_store = self.content_store(&source_id);
            compute_delta_keys(source_store, source_head_id.clone(), ancestor.t).await?
        };

        // Pass 1: stream branch commits to collect lightweight summaries
        // (CID, t, conflict keys) without retaining flake payloads in memory.
        let summaries = scan_branch_commits(
            branch_store.clone(),
            branch_head_id,
            ancestor.t,
            &source_delta,
        )
        .await?;
        let total_commits = summaries.len();

        // Abort upfront if any commit conflicts — no commits will be written.
        if strategy == ConflictStrategy::Abort {
            if let Some(summary) = summaries.iter().find(|s| !s.conflict_keys.is_empty()) {
                return Err(ApiError::BranchConflict(format!(
                    "Rebase aborted: {} conflict(s) at t={} with abort strategy",
                    summary.conflict_keys.len(),
                    summary.t
                )));
            }
        }

        // Copy the source index into the branch namespace before replay.
        // Gives the branch an index to start from when novelty is reindexed
        // post-rebase (best-effort).
        self.copy_source_index(&source_id, &branch_id, &source_record)
            .await;

        // Acquire the target branch's write lock when a manager is
        // available, serializing the entire replay against regular
        // transactions on the same branch.
        let write_guard = self.lock_ledger(&branch_id).await?;

        // Start replay from the source's queryable state, relabeled
        // to the branch.
        let mut current_state = self.ledger(&source_id).await?;
        std::sync::Arc::make_mut(&mut current_state.snapshot).ledger_id = branch_id.clone();

        let mut conflicts: Vec<RebaseConflict> = Vec::new();
        let mut skipped = 0;
        let mut replayed = 0;
        let mut pending_replays: Vec<PendingReplay> = Vec::new();

        for summary in &summaries {
            let has_conflicts = !summary.conflict_keys.is_empty();

            if has_conflicts && strategy == ConflictStrategy::Skip {
                conflicts.push(RebaseConflict {
                    original_t: summary.t,
                    conflict_count: summary.conflict_keys.len(),
                    keys: summary.conflict_keys.clone(),
                    resolution: strategy.as_str(),
                });
                skipped += 1;
                continue;
            }

            let commit =
                fluree_db_core::load_commit_by_id(&branch_store, &summary.commit_id).await?;

            let flakes = self
                .resolve_flakes(
                    &commit.flakes,
                    &summary.conflict_keys,
                    &strategy,
                    &current_state,
                )
                .await?;

            if has_conflicts {
                conflicts.push(RebaseConflict {
                    original_t: summary.t,
                    conflict_count: summary.conflict_keys.len(),
                    keys: summary.conflict_keys.clone(),
                    resolution: strategy.as_str(),
                });
            }

            if flakes.is_empty() {
                continue;
            }

            let staged = self
                .build_replay_commit(current_state, flakes, &commit)
                .await?;
            let commit_id = staged
                .commit
                .id
                .clone()
                .expect("build_replay_commit guarantees commit.id is set");
            let commit_bytes = staged.commit_bytes.clone();

            let (_receipt, next_state) = staged
                .finalize_state()
                .map_err(|e| ApiError::internal(format!("rebase finalize_state failed: {e}")))?;

            if next_state.at_max_novelty(&self.index_config) {
                let cumulative = next_state.novelty_size();
                let max = self.index_config.reindex_max_bytes;
                return Err(ApiError::http(
                    422,
                    format!(
                        "Rebase would accumulate {cumulative} bytes of novelty (max {max}); \
                         reindex the source branch then retry, or rebase a smaller commit range."
                    ),
                ));
            }

            pending_replays.push(PendingReplay {
                commit_id,
                commit_bytes,
                original_t: summary.t,
            });
            current_state = next_state;
            replayed += 1;
        }

        let new_head_id = pending_replays.last().map(|b| b.commit_id.clone());
        let new_head_t = current_state.t();

        Ok(StagedRebase {
            branch: branch.to_string(),
            branch_id,
            source: source_name_owned,
            source_id,
            source_head_id,
            source_head_t,
            fast_forward: false,
            total_commits,
            replayed,
            skipped,
            conflicts,
            rollback_snapshot,
            pre_rebase_head_id,
            pre_rebase_head_t,
            new_head_id,
            new_head_t,
            write_guard,
            final_state: current_state,
            pending_replays,
        })
    }

    /// Apply a [`StagedRebase`] through the local commit pipeline:
    /// write any `pending_replays` to the content store, single
    /// fast-forward `publish_commit` to advance the branch HEAD,
    /// then finalize the cache. The build phase already serialized
    /// against concurrent transactions via the held write guard.
    ///
    /// All-or-nothing: if the blob write or publish fails, nothing
    /// is on the active head chain. Orphan blobs in the content
    /// store are GC fodder later.
    async fn apply_rebase(&self, staged: StagedRebase) -> Result<RebaseReport> {
        let StagedRebase {
            branch: _,
            branch_id,
            source: _,
            source_id: _,
            source_head_id,
            source_head_t,
            fast_forward,
            total_commits,
            replayed,
            skipped,
            conflicts,
            rollback_snapshot: _,
            pre_rebase_head_id: _,
            pre_rebase_head_t: _,
            new_head_id,
            new_head_t,
            write_guard,
            final_state,
            pending_replays,
        } = staged;

        if let Some(ref last_head_id) = new_head_id {
            if !pending_replays.is_empty() {
                let content_store = self.content_store(&branch_id);
                for blob in &pending_replays {
                    content_store
                        .put_with_id(&blob.commit_id, &blob.commit_bytes)
                        .await
                        .map_err(|e| {
                            ApiError::internal(format!("rebase commit blob write failed: {e}"))
                        })?;
                }
            }
            self.publisher()?
                .publish_commit(&branch_id, new_head_t, last_head_id)
                .await?;
        }

        if let Some(guard) = write_guard {
            let needs_reindex = final_state.should_reindex(&self.index_config);
            let commit_t = final_state.t();
            self.finalize_commit(guard, final_state, commit_t, needs_reindex)
                .await?;
        }

        // Fast-forward keeps the cache disconnected so the next read
        // reloads from storage (mirrors the previous behavior of
        // `fast_forward_rebase`).
        if fast_forward {
            if let Some(ref lm) = self.ledger_manager {
                lm.disconnect(&branch_id).await;
            }
        }

        Ok(RebaseReport {
            replayed,
            conflicts,
            failures: Vec::new(),
            source_head_t,
            source_head_id,
            fast_forward,
            total_commits,
            skipped,
        })
    }

    /// Dry-run terminal for rebase: stage `flakes` on top of `state`
    /// and produce a [`StagedCommit`] representing the replay of
    /// `original_commit`, without writing the commit blob or
    /// publishing the ref. The caller composes this with either
    /// [`StagedCommit::apply`] (local single-step apply) or the
    /// blob-write + `AdvanceRef` consensus apply path.
    ///
    /// Uses `skip_sequencing=true` (no `expected_head_ref` baked in)
    /// and `skip_backpressure=true` (a single rebase can accumulate
    /// novelty well past the per-commit gate). Atomic rebase is
    /// instead bounded by a cumulative novelty check the caller
    /// applies after `staged.finalize_state()` of each step.
    pub(crate) async fn build_replay_commit(
        &self,
        state: LedgerState,
        flakes: Vec<Flake>,
        original_commit: &Commit,
    ) -> Result<StagedCommit> {
        let reverse_graph = state.snapshot.build_reverse_graph().map_err(|e| {
            ApiError::internal(format!("Failed to build reverse graph during rebase: {e}"))
        })?;

        let view = StagedLedger::new(state, flakes, &reverse_graph).map_err(|e| {
            ApiError::internal(format!("Failed to stage flakes during rebase: {e}"))
        })?;

        let ns_registry = NamespaceRegistry::from_db(view.db());
        let commit_opts = CommitOpts::default()
            .with_skip_backpressure()
            .with_skip_sequencing()
            .with_namespace_delta(original_commit.namespace_delta.clone())
            .with_graph_delta(original_commit.graph_delta.clone());

        // With skip_sequencing=true the apply path uses
        // `fast_forward_commit` (no CAS), so `expected_head_ref` is
        // intentionally `None` here. raw_txn has no place in a replay
        // commit, so `txn_id` is `None` as well.
        let staged = fluree_db_transact::build_commit(
            view,
            ns_registry,
            None,
            None,
            &self.index_config,
            commit_opts,
        )
        .await?;

        Ok(staged)
    }

    /// Filter flakes based on the conflict strategy, generating retractions
    /// for `TakeBranch`.
    async fn resolve_flakes(
        &self,
        flakes: &[Flake],
        conflicting_keys: &[ConflictKey],
        strategy: &ConflictStrategy,
        source_state: &LedgerState,
    ) -> Result<Vec<Flake>> {
        if conflicting_keys.is_empty() {
            return Ok(flakes.to_vec());
        }

        let conflict_set: FxHashSet<&ConflictKey> = conflicting_keys.iter().collect();

        match strategy {
            ConflictStrategy::TakeSource => {
                // Drop branch's conflicting flakes (source wins).
                Ok(flakes
                    .iter()
                    .filter(|f| {
                        let key = ConflictKey::new(f.s.clone(), f.p.clone(), f.g.clone());
                        !conflict_set.contains(&key)
                    })
                    .cloned()
                    .collect())
            }
            ConflictStrategy::TakeBranch => {
                // Keep branch's flakes + retract source's conflicting values.
                let retractions = self
                    .build_source_retractions(conflicting_keys, source_state)
                    .await?;
                let mut result = flakes.to_vec();
                result.extend(retractions);
                Ok(result)
            }
            // TakeBoth: keep all branch flakes, both values coexist.
            // Abort/Skip: handled before this function is called.
            _ => Ok(flakes.to_vec()),
        }
    }

    /// Apply the two-way half of [`ConflictStrategy`] to a flake set being
    /// staged.
    ///
    /// "Two-way" means: `flakes` is the **winning** side under `TakeSource`
    /// and the **losing** side under `TakeBranch`. `opposite_state` is the
    /// ledger state whose current values get retracted when `flakes` wins.
    /// This is the polarity used by merge (`flakes` = incoming source branch)
    /// and revert (`flakes` = inverse flakes from the reverted commits); the
    /// rebase replay path has the opposite polarity (`flakes` = branch
    /// commits being replayed onto `source_state`) and uses a separate
    /// resolver.
    ///
    /// Behaviour:
    /// - empty `conflicting_keys` → return `flakes` unchanged
    /// - `TakeSource` → keep `flakes` + retract `opposite_state`'s values for
    ///   each conflict key (via [`Self::build_source_retractions`])
    /// - `TakeBranch` → drop entries of `flakes` whose `(s, p, g)` is in the
    ///   conflict set
    /// - `TakeBoth` → keep `flakes` unchanged so both values coexist (merge
    ///   only; revert rejects this strategy at the entry point)
    /// - `Abort` / `Skip` → caller's responsibility to short-circuit before
    ///   invoking this method
    pub(crate) async fn apply_two_way_strategy(
        &self,
        flakes: Vec<Flake>,
        conflicting_keys: &[ConflictKey],
        strategy: &ConflictStrategy,
        opposite_state: &LedgerState,
    ) -> Result<Vec<Flake>> {
        if conflicting_keys.is_empty() {
            return Ok(flakes);
        }

        let conflict_set: FxHashSet<&ConflictKey> = conflicting_keys.iter().collect();

        match strategy {
            ConflictStrategy::TakeSource => {
                let retractions = self
                    .build_source_retractions(conflicting_keys, opposite_state)
                    .await?;
                let mut result = flakes;
                result.extend(retractions);
                Ok(result)
            }
            ConflictStrategy::TakeBranch => Ok(flakes
                .into_iter()
                .filter(|f| {
                    let key = ConflictKey::new(f.s.clone(), f.p.clone(), f.g.clone());
                    !conflict_set.contains(&key)
                })
                .collect()),
            // TakeBoth: keep all flakes (both values coexist).
            // Abort/Skip: caller handles these before this function is called.
            _ => Ok(flakes),
        }
    }

    /// Look up the source state's current flakes for the given conflict keys
    /// and generate retraction flakes (`op: false`) for each.
    pub(crate) async fn build_source_retractions(
        &self,
        conflicting_keys: &[ConflictKey],
        source_state: &LedgerState,
    ) -> Result<Vec<Flake>> {
        let mut retractions = Vec::new();

        for key in conflicting_keys {
            retractions.extend(
                current_asserted_for_key(source_state, key)
                    .await?
                    .into_iter()
                    .map(|flake| Flake {
                        op: false,
                        t: 0, // overwritten by commit
                        ..flake
                    }),
            );
        }

        Ok(retractions)
    }

    /// Copy index artifacts from source to branch (best-effort).
    async fn copy_source_index(
        &self,
        source_id: &str,
        branch_id: &str,
        source_record: &fluree_db_nameservice::NsRecord,
    ) {
        if let Some(ref index_cid) = source_record.index_head_id {
            if let Err(e) = self
                .copy_index_to_branch(source_id, branch_id, index_cid)
                .await
            {
                tracing::warn!(
                    %e, source = %source_id, branch = %branch_id,
                    "failed to copy index during rebase; branch will replay from genesis"
                );
            } else if let Some(publisher) = self.nameservice_mode.publisher() {
                if let Err(e) = publisher
                    .publish_index(branch_id, source_record.index_t, index_cid)
                    .await
                {
                    tracing::warn!(%e, "failed to publish index for rebased branch");
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Lightweight summary of a branch commit, collected during the scan pass.
/// Holds only the CID, `t`, and pre-computed conflict keys — not the full
/// flake payload, so the entire branch history fits in memory.
struct CommitSummary {
    commit_id: ContentId,
    t: i64,
    conflict_keys: Vec<ConflictKey>,
}

/// Stream branch commits HEAD→oldest, extract conflict keys, and return
/// lightweight summaries in oldest-first order. Full flake payloads are
/// dropped after conflict key extraction so only summaries remain in memory.
async fn scan_branch_commits<C: fluree_db_core::ContentStore + Clone + 'static>(
    store: C,
    head_id: ContentId,
    stop_at_t: i64,
    source_delta: &FxHashSet<ConflictKey>,
) -> Result<Vec<CommitSummary>> {
    let stream = trace_commits_by_id(store, head_id, stop_at_t);
    futures::pin_mut!(stream);

    let mut summaries = Vec::new();
    while let Some(commit) = stream.try_next().await? {
        let conflict_keys = find_conflicting_keys(&commit.flakes, source_delta);
        summaries.push(CommitSummary {
            commit_id: commit.id.expect("loaded commit should have an id"),
            t: commit.t,
            conflict_keys,
        });
    }

    summaries.reverse();
    Ok(summaries)
}

/// Find (s, p, g) keys from flakes that overlap with the source delta.
fn find_conflicting_keys(
    flakes: &[Flake],
    source_delta: &FxHashSet<ConflictKey>,
) -> Vec<ConflictKey> {
    let mut seen = FxHashSet::default();
    flakes
        .iter()
        .filter_map(|f| {
            let key = ConflictKey::new(f.s.clone(), f.p.clone(), f.g.clone());
            if source_delta.contains(&key) && seen.insert(key.clone()) {
                Some(key)
            } else {
                None
            }
        })
        .collect()
}

pub(crate) async fn current_asserted_for_key(
    state: &LedgerState,
    key: &ConflictKey,
) -> Result<Vec<Flake>> {
    let g_id = match &key.g {
        None => DEFAULT_GRAPH_ID,
        Some(g_sid) => match state
            .snapshot
            .decode_sid(g_sid)
            .and_then(|iri| state.snapshot.graph_registry.graph_id_for_iri(&iri))
        {
            Some(g_id) => g_id,
            None => return Ok(Vec::new()),
        },
    };

    let match_val = RangeMatch::subject_predicate(key.s.clone(), key.p.clone());
    let opts = RangeOptions {
        to_t: Some(state.t()),
        ..Default::default()
    };

    let flakes = range_with_overlay(
        &state.snapshot,
        g_id,
        state.novelty.as_ref(),
        IndexType::Spot,
        RangeTest::Eq,
        match_val,
        opts,
    )
    .await?;

    Ok(flakes
        .into_iter()
        .filter(|flake| flake.op && flake.g == key.g)
        .collect())
}
