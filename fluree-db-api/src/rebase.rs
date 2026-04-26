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
use fluree_db_ledger::{LedgerState, LedgerView};
use fluree_db_nameservice::NsRecordSnapshot;
use fluree_db_novelty::compute_delta_keys;
use fluree_db_transact::{CommitOpts, NamespaceRegistry};
use futures::TryStreamExt;
use rustc_hash::FxHashSet;
use serde::Serialize;
use tracing::Instrument;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Strategy for resolving conflicts when branch and source modifications
/// overlap on the same (subject, predicate, graph) tuple.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
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
            self.rebase_branch_inner(ledger_name, branch, strategy)
                .await
        }
        .instrument(span)
        .await
    }

    async fn rebase_branch_inner(
        &self,
        ledger_name: &str,
        branch: &str,
        strategy: ConflictStrategy,
    ) -> Result<RebaseReport> {
        if branch == "main" {
            return Err(ApiError::InvalidBranch(
                "Cannot rebase the main branch".to_string(),
            ));
        }

        let branch_id = format_ledger_id(ledger_name, branch);
        let branch_record = self
            .nameservice()
            .lookup(&branch_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(branch_id.clone()))?;

        let source_name = branch_record.source_branch.as_ref().ok_or_else(|| {
            ApiError::InvalidBranch(format!("Branch {branch_id} has no source branch"))
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

        // Disconnect branch from ledger manager to prevent stale reads.
        if let Some(ref lm) = self.ledger_manager {
            lm.disconnect(&branch_id).await;
        }

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

        // Fast-forward: branch has no unique commits beyond the ancestor.
        let is_fast_forward = branch_head_id == ancestor.commit_id;

        if is_fast_forward {
            return self
                .fast_forward_rebase(
                    &branch_id,
                    &source_id,
                    &source_record,
                    source_head_id,
                    source_head_t,
                )
                .await;
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

        // Snapshot the branch's nameservice state before any mutations.
        // If replay fails, we restore this snapshot to roll back.
        let pre_rebase_snapshot = NsRecordSnapshot::from_record(&branch_record);

        // Copy the source index into the branch namespace before replay.
        // This gives the branch an index to build incrementally from when
        // novelty grows too large mid-rebase.
        self.copy_source_index(&source_id, &branch_id, &source_record)
            .await;

        // Run replay and finalization; roll back on any error.
        let ctx = ReplayContext {
            branch_id: &branch_id,
            branch_record: &branch_record,
            source_id: &source_id,
            source_head_id: &source_head_id,
            source_head_t,
            branch_store: &branch_store,
            summaries: &summaries,
            strategy: &strategy,
            total_commits,
        };
        let result = self.run_replay(&ctx).await;

        match result {
            Ok(report) => {
                if let Some(ref lm) = self.ledger_manager {
                    lm.disconnect(&branch_id).await;
                }
                Ok(report)
            }
            Err(e) => {
                tracing::warn!(
                    branch = %branch_id,
                    error = %e,
                    "rebase failed, rolling back nameservice state"
                );
                if let Err(rollback_err) = self
                    .nameservice()
                    .reset_head(&branch_id, pre_rebase_snapshot)
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

    /// The actual replay loop + finalization, extracted so the caller can
    /// wrap it in a snapshot/rollback guard.
    async fn run_replay(&self, ctx: &ReplayContext<'_>) -> Result<RebaseReport> {
        let mut current_state =
            LedgerState::load(&self.nameservice_mode, ctx.source_id, self.backend()).await?;

        current_state.snapshot.ledger_id = ctx.branch_id.to_string();

        let mut report = RebaseReport {
            replayed: 0,
            conflicts: Vec::new(),
            failures: Vec::new(),
            source_head_t: ctx.source_head_t,
            source_head_id: ctx.source_head_id.clone(),
            fast_forward: false,
            total_commits: ctx.total_commits,
            skipped: 0,
        };

        for summary in ctx.summaries {
            let has_conflicts = !summary.conflict_keys.is_empty();

            if has_conflicts && *ctx.strategy == ConflictStrategy::Skip {
                report.conflicts.push(RebaseConflict {
                    original_t: summary.t,
                    conflict_count: summary.conflict_keys.len(),
                    keys: summary.conflict_keys.clone(),
                    resolution: ctx.strategy.as_str(),
                });
                report.skipped += 1;
                continue;
            }

            let commit =
                fluree_db_core::load_commit_by_id(ctx.branch_store, &summary.commit_id).await?;

            let flakes = self
                .resolve_flakes(
                    &commit.flakes,
                    &summary.conflict_keys,
                    ctx.strategy,
                    &current_state,
                )
                .await?;

            if has_conflicts {
                report.conflicts.push(RebaseConflict {
                    original_t: summary.t,
                    conflict_count: summary.conflict_keys.len(),
                    keys: summary.conflict_keys.clone(),
                    resolution: ctx.strategy.as_str(),
                });
            }

            if flakes.is_empty() {
                continue;
            }

            current_state = self.replay_commit(current_state, flakes, &commit).await?;
            report.replayed += 1;

            if current_state.should_reindex(&self.index_config) {
                current_state = self
                    .flush_rebase_novelty(ctx.branch_id, ctx.branch_record)
                    .await?;
            }
        }

        Ok(report)
    }

    async fn fast_forward_rebase(
        &self,
        branch_id: &str,
        source_id: &str,
        source_record: &fluree_db_nameservice::NsRecord,
        source_head_id: ContentId,
        source_head_t: i64,
    ) -> Result<RebaseReport> {
        self.publisher()?
            .publish_commit(branch_id, source_head_t, &source_head_id)
            .await?;

        self.copy_source_index(source_id, branch_id, source_record)
            .await;

        if let Some(ref lm) = self.ledger_manager {
            lm.disconnect(branch_id).await;
        }

        Ok(RebaseReport {
            replayed: 0,
            conflicts: Vec::new(),
            failures: Vec::new(),
            source_head_t,
            source_head_id,
            fast_forward: true,
            total_commits: 0,
            skipped: 0,
        })
    }

    /// Stage flakes and commit as a replay of the original commit.
    async fn replay_commit(
        &self,
        state: LedgerState,
        flakes: Vec<Flake>,
        original_commit: &Commit,
    ) -> Result<LedgerState> {
        let reverse_graph = state.snapshot.build_reverse_graph().map_err(|e| {
            ApiError::internal(format!("Failed to build reverse graph during rebase: {e}"))
        })?;

        let view = LedgerView::stage(state, flakes, &reverse_graph).map_err(|e| {
            ApiError::internal(format!("Failed to stage flakes during rebase: {e}"))
        })?;

        let ns_registry = NamespaceRegistry::from_db(view.db());
        let commit_opts = CommitOpts::default()
            .with_skip_backpressure()
            .with_skip_sequencing()
            .with_namespace_delta(original_commit.namespace_delta.clone())
            .with_graph_delta(original_commit.graph_delta.clone());

        let content_store = self.content_store(view.db().ledger_id.as_str());
        let publisher = self.publisher()?;
        let (_receipt, new_state) = fluree_db_transact::commit(
            view,
            ns_registry,
            &content_store,
            publisher,
            &self.index_config,
            commit_opts,
        )
        .await?;

        Ok(new_state)
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

    /// Build an inline index for the branch mid-rebase to flush novelty.
    ///
    /// Uses `rebuild_index_from_commits_with_store` with a `BranchedContentStore`
    /// so the indexer can follow the branch's commit chain through parent
    /// namespaces. Publishes the result to the nameservice, then reloads the
    /// `LedgerState` so novelty only contains commits since the new index.
    async fn flush_rebase_novelty(
        &self,
        branch_id: &str,
        branch_record: &fluree_db_nameservice::NsRecord,
    ) -> Result<LedgerState> {
        tracing::debug!(
            branch_id,
            "building inline index mid-rebase to flush novelty"
        );

        let branch_store = LedgerState::build_branched_store(
            &self.nameservice_mode,
            branch_record,
            self.backend(),
        )
        .await?;

        let record = self
            .nameservice()
            .lookup(branch_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(branch_id.to_string()))?;

        let mut indexer_config = crate::build_indexer_config(self.config());

        // Seed configured full-text properties from the branch's current
        // `f:fullTextDefaults`. Best-effort: failures leave the set empty and
        // fall back to the `@fulltext`-datatype-only path for this rebuild.
        if let Ok(state) = self.ledger(branch_id).await {
            // `state.t()` covers the novelty-only case (no prior index) where
            // `snapshot.t == 0` would drop all novelty flakes from the query.
            let to_t = state.t();
            let snapshot = &state.snapshot;
            let overlay: &dyn fluree_db_core::OverlayProvider = &*state.novelty;
            if let Ok(Some(cfg)) =
                crate::config_resolver::resolve_ledger_config(snapshot, overlay, to_t).await
            {
                indexer_config.fulltext_configured_properties =
                    crate::config_resolver::configured_fulltext_properties_for_indexer(&cfg);
            }
        }

        let index_result = fluree_db_indexer::rebuild_index_from_commits_with_store(
            branch_store,
            branch_id,
            &record,
            indexer_config,
        )
        .await
        .map_err(|e| ApiError::internal(format!("Mid-rebase index build failed: {e}")))?;

        self.publisher()?
            .publish_index(branch_id, index_result.index_t, &index_result.root_id)
            .await?;

        LedgerState::load(&self.nameservice_mode, branch_id, self.backend())
            .await
            .map_err(Into::into)
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

/// Context for the replay loop, bundling references that would otherwise
/// require 10+ parameters.
struct ReplayContext<'a> {
    branch_id: &'a str,
    branch_record: &'a fluree_db_nameservice::NsRecord,
    source_id: &'a str,
    source_head_id: &'a ContentId,
    source_head_t: i64,
    branch_store: &'a fluree_db_core::BranchedContentStore,
    summaries: &'a [CommitSummary],
    strategy: &'a ConflictStrategy,
    total_commits: usize,
}

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
