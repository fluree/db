//! Read-only preview of a revert.
//!
//! Computes what [`crate::Fluree::revert_commit`], [`Self::revert_commits`],
//! or [`Self::revert_range`] would do — the resolved set of commits, the
//! conflict keys, and whether the chosen strategy would let the operation
//! proceed — without writing a commit. Mirrors
//! [`crate::Fluree::merge_preview`] for the merge path.

use crate::error::{ApiError, Result};
use crate::ledger_view::CommitRef;
use crate::merge_preview::{DEFAULT_MAX_COMMITS, DEFAULT_MAX_CONFLICT_KEYS};
use crate::rebase::ConflictStrategy;
use crate::revert::{RevertContext, RevertSource};
use fluree_db_core::{commit_to_summary, load_commit_by_id, CommitSummary, ConflictKey};
use serde::Serialize;
use tracing::Instrument;

/// Knobs for [`crate::Fluree::revert_preview`].
///
/// `RevertPreviewOpts::default()` caps the commit list at 500 entries and
/// the conflict-key list at 200, includes conflict computation, and uses
/// `Abort` as the strategy used for the `revertable` verdict — matching the
/// safest default of the mutating revert paths.
#[derive(Clone, Debug)]
pub struct RevertPreviewOpts {
    /// `Some(n)` caps `reverted_commits` at `n`; `None` is unbounded. **Does
    /// not bound the underlying DAG walk** — the unbounded `reverted_count`
    /// is always computed.
    pub max_commits: Option<usize>,
    /// Cap on `conflicts.keys`. `None` is unbounded. **Does not bound the
    /// underlying conflict-key computation** — `conflicts.count` is always
    /// the full intersection size.
    pub max_conflict_keys: Option<usize>,
    /// When `false`, skips conflict computation entirely; the response still
    /// contains `reverted_commits` and `reverted_count` but `conflicts` will
    /// be empty.
    pub include_conflicts: bool,
    /// Strategy used to compute the `revertable` verdict. `Abort` means the
    /// preview reports `revertable = false` whenever conflicts exist;
    /// `TakeSource`/`TakeBranch` always report `revertable = true`.
    pub conflict_strategy: ConflictStrategy,
}

impl Default for RevertPreviewOpts {
    fn default() -> Self {
        Self {
            max_commits: Some(DEFAULT_MAX_COMMITS),
            max_conflict_keys: Some(DEFAULT_MAX_CONFLICT_KEYS),
            include_conflicts: true,
            conflict_strategy: ConflictStrategy::Abort,
        }
    }
}

/// Conflict summary returned by [`crate::Fluree::revert_preview`] family.
#[derive(Clone, Debug, Serialize)]
pub struct RevertConflictSummary {
    /// Total `(s, p, g)` keys that conflict (uncapped count).
    pub count: usize,
    /// Conflict keys, capped by [`RevertPreviewOpts::max_conflict_keys`].
    pub keys: Vec<ConflictKey>,
    /// `true` when `keys.len() < count`.
    pub truncated: bool,
}

impl RevertConflictSummary {
    fn empty() -> Self {
        Self {
            count: 0,
            keys: Vec::new(),
            truncated: false,
        }
    }
}

/// Read-only preview of a revert.
#[derive(Clone, Debug, Serialize)]
pub struct RevertPreview {
    /// Branch the revert would be written to.
    pub branch: String,
    /// Total commits that would be reverted (uncapped).
    pub reverted_count: usize,
    /// Newest-first summaries, capped by [`RevertPreviewOpts::max_commits`].
    pub reverted_commits: Vec<CommitSummary>,
    /// `true` if `reverted_commits.len() < reverted_count`.
    pub truncated: bool,
    /// Conflicts the revert would encounter.
    pub conflicts: RevertConflictSummary,
    /// Whether the chosen strategy would let the revert proceed.
    pub revertable: bool,
}

impl crate::Fluree {
    /// Preview reverting a single commit on `branch`.
    ///
    /// Convenience wrapper around [`Self::revert_commits_preview`] for the
    /// common single-commit case.
    pub async fn revert_commit_preview(
        &self,
        ledger_name: &str,
        branch: &str,
        commit: CommitRef,
    ) -> Result<RevertPreview> {
        self.revert_commit_preview_with(ledger_name, branch, commit, RevertPreviewOpts::default())
            .await
    }

    /// Like [`Self::revert_commit_preview`] but with explicit knobs.
    pub async fn revert_commit_preview_with(
        &self,
        ledger_name: &str,
        branch: &str,
        commit: CommitRef,
        opts: RevertPreviewOpts,
    ) -> Result<RevertPreview> {
        let span = tracing::debug_span!("revert_commit_preview", ledger_name, branch);
        async move {
            self.revert_preview_inner(ledger_name, branch, RevertSource::single(commit), opts)
                .await
        }
        .instrument(span)
        .await
    }

    /// Preview reverting an explicit set of commits on `branch`.
    pub async fn revert_commits_preview(
        &self,
        ledger_name: &str,
        branch: &str,
        commits: Vec<CommitRef>,
    ) -> Result<RevertPreview> {
        self.revert_commits_preview_with(ledger_name, branch, commits, RevertPreviewOpts::default())
            .await
    }

    /// Like [`Self::revert_commits_preview`] but with explicit knobs.
    pub async fn revert_commits_preview_with(
        &self,
        ledger_name: &str,
        branch: &str,
        commits: Vec<CommitRef>,
        opts: RevertPreviewOpts,
    ) -> Result<RevertPreview> {
        let span = tracing::debug_span!("revert_commits_preview", ledger_name, branch);
        async move {
            let source = RevertSource::try_set(commits).ok_or_else(|| {
                ApiError::InvalidBranch("Revert requires at least one commit".to_string())
            })?;
            self.revert_preview_inner(ledger_name, branch, source, opts)
                .await
        }
        .instrument(span)
        .await
    }

    /// Preview reverting a git-style range `from..to` on `branch`.
    pub async fn revert_range_preview(
        &self,
        ledger_name: &str,
        branch: &str,
        from: CommitRef,
        to: CommitRef,
    ) -> Result<RevertPreview> {
        self.revert_range_preview_with(
            ledger_name,
            branch,
            from,
            to,
            RevertPreviewOpts::default(),
        )
        .await
    }

    /// Like [`Self::revert_range_preview`] but with explicit knobs.
    pub async fn revert_range_preview_with(
        &self,
        ledger_name: &str,
        branch: &str,
        from: CommitRef,
        to: CommitRef,
        opts: RevertPreviewOpts,
    ) -> Result<RevertPreview> {
        let span = tracing::debug_span!("revert_range_preview", ledger_name, branch);
        async move {
            self.revert_preview_inner(ledger_name, branch, RevertSource::range(from, to), opts)
                .await
        }
        .instrument(span)
        .await
    }

    async fn revert_preview_inner(
        &self,
        ledger_name: &str,
        branch: &str,
        source: RevertSource,
        opts: RevertPreviewOpts,
    ) -> Result<RevertPreview> {
        match opts.conflict_strategy {
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

        let RevertContext {
            branch_store,
            plan,
            conflict_keys,
            ..
        } = self.build_revert_context(ledger_name, branch, source).await?;

        // Build per-commit summaries up to the requested cap. The full count
        // is `plan.ordered_commits.len()`; the cap only bounds the slice we
        // load and return.
        let reverted_count = plan.ordered_commits.len();
        let take_n = opts.max_commits.map_or(reverted_count, |n| n.min(reverted_count));
        let mut reverted_commits = Vec::with_capacity(take_n);
        for commit_id in plan.ordered_commits.iter().take(take_n) {
            let commit = load_commit_by_id(&branch_store, commit_id).await?;
            reverted_commits.push(commit_to_summary(&commit));
        }
        let truncated = take_n < reverted_count;

        // Conflict summary: cap keys after sort. `compute_conflict_keys`
        // already returns them sorted lexicographically.
        let conflicts = if opts.include_conflicts {
            let count = conflict_keys.len();
            let mut keys = conflict_keys;
            let truncated = match opts.max_conflict_keys {
                Some(cap) if count > cap => {
                    keys.truncate(cap);
                    true
                }
                _ => false,
            };
            RevertConflictSummary {
                count,
                keys,
                truncated,
            }
        } else {
            RevertConflictSummary::empty()
        };

        let revertable =
            opts.conflict_strategy != ConflictStrategy::Abort || conflicts.count == 0;

        Ok(RevertPreview {
            branch: branch.to_string(),
            reverted_count,
            reverted_commits,
            truncated,
            conflicts,
            revertable,
        })
    }
}
