//! Read-only branch merge preview.
//!
//! Computes the rich diff between two branches — ahead/behind commit lists,
//! conflict keys, fast-forward eligibility — using the same primitives as
//! [`crate::Fluree::merge_branch`] but without mutating any nameservice or
//! content-store state.
//!
//! The heavy lifting (per-commit summaries, DAG walking, common-ancestor
//! discovery, delta-key computation) lives in `fluree-db-core` and
//! `fluree-db-novelty`. This module orchestrates them: nameservice lookups,
//! branched-store construction for source/target, and parallel walks.

use crate::error::{ApiError, Result};
use fluree_db_core::ledger_id::format_ledger_id;
use fluree_db_core::{
    find_common_ancestor, walk_commit_summaries, BranchedContentStore, CommitSummary, ConflictKey,
    ContentId, ContentStore,
};
use fluree_db_ledger::LedgerState;
use fluree_db_novelty::compute_delta_keys;
use serde::Serialize;
use std::sync::Arc;
use tracing::Instrument;

/// Default cap on commits per side returned in [`BranchDelta::commits`].
pub const DEFAULT_MAX_COMMITS: usize = 500;

/// Default cap on conflict keys returned in [`ConflictSummary::keys`].
pub const DEFAULT_MAX_CONFLICT_KEYS: usize = 200;

/// Knobs for [`crate::Fluree::merge_preview`].
///
/// `MergePreviewOpts::default()` matches the spec: cap each commit list at
/// 500 entries, cap conflict keys at 200, and run the conflict computation.
/// Setting `max_commits` or `max_conflict_keys` to `None` explicitly opts in
/// to **unbounded** results — direct Rust callers can use this for tooling
/// that needs the full divergence. The HTTP layer always supplies a bound to
/// protect against pathologically large responses.
///
/// ### What the caps do and do not control
///
/// `max_commits` and `max_conflict_keys` cap the **size of the returned
/// lists**, not the cost of computing them:
///
/// - The `BranchDelta::count` on each side is the full unbounded divergence,
///   computed by walking every commit envelope between HEAD and the common
///   ancestor. A 1M-commit divergence costs 1M envelope reads regardless of
///   the cap.
/// - The `ConflictSummary::count` is the full intersection size; both
///   `compute_delta_keys` walks scan every flake on each side since the
///   ancestor. Pass [`include_conflicts: false`](Self::include_conflicts) to
///   skip them entirely when only counts are needed.
///
/// To bound the *I/O cost* of the walk itself, callers must pre-check the
/// divergence (e.g., refuse before invoking when `target.t - ancestor.t`
/// exceeds some threshold) or use `include_conflicts: false`.
#[derive(Clone, Debug)]
pub struct MergePreviewOpts {
    /// Per side. `Some(n)` caps the returned list at `n`; `None` is
    /// unbounded. **Does not bound the divergence walk** — see type docs.
    pub max_commits: Option<usize>,
    /// Cap on `conflicts.keys`. `None` is unbounded. **Does not bound the
    /// `compute_delta_keys` walks** — see type docs.
    pub max_conflict_keys: Option<usize>,
    /// When `false`, skips the two `compute_delta_keys` walks — the response
    /// still contains commit counts but `conflicts` will be empty. The
    /// fastest way to bound preview cost on diverged branches.
    pub include_conflicts: bool,
}

impl Default for MergePreviewOpts {
    fn default() -> Self {
        Self {
            max_commits: Some(DEFAULT_MAX_COMMITS),
            max_conflict_keys: Some(DEFAULT_MAX_CONFLICT_KEYS),
            include_conflicts: true,
        }
    }
}

/// Common ancestor of source and target HEADs.
#[derive(Clone, Debug, Serialize)]
pub struct AncestorRef {
    pub commit_id: ContentId,
    pub t: i64,
}

/// One side of a branch divergence — commits unique to that side since the
/// common ancestor.
#[derive(Clone, Debug, Serialize)]
pub struct BranchDelta {
    /// Total number of commits on this side of the divergence.
    pub count: usize,
    /// Newest-first commit summaries, capped by `max_commits`.
    pub commits: Vec<CommitSummary>,
    /// `true` when `count > commits.len()` — the list was truncated.
    pub truncated: bool,
}

/// Summary of overlapping `(s, p, g)` tuples touched by both sides since the
/// common ancestor. Empty when the merge is fast-forward (no real conflicts
/// possible) or when [`MergePreviewOpts::include_conflicts`] is `false`.
#[derive(Clone, Debug, Serialize)]
pub struct ConflictSummary {
    pub count: usize,
    pub keys: Vec<ConflictKey>,
    pub truncated: bool,
}

impl ConflictSummary {
    fn empty() -> Self {
        Self {
            count: 0,
            keys: Vec::new(),
            truncated: false,
        }
    }
}

/// Read-only diff between two branches.
#[derive(Clone, Debug, Serialize)]
pub struct MergePreview {
    pub source: String,
    pub target: String,

    /// `None` when both heads are absent (the unborn-branches edge case).
    pub ancestor: Option<AncestorRef>,

    /// Commits on `source` not on `target`.
    pub ahead: BranchDelta,
    /// Commits on `target` not on `source`.
    pub behind: BranchDelta,

    /// `true` iff `target HEAD == ancestor` (or both heads are absent).
    /// Mirrors the `is_fast_forward` check in `merge_branch_inner`.
    pub fast_forward: bool,

    /// Always populated. Empty when `fast_forward` (no conflicts possible)
    /// or when the caller opted out via [`MergePreviewOpts::include_conflicts`].
    pub conflicts: ConflictSummary,
}

impl crate::Fluree {
    /// Compute a preview of merging `source_branch` into `target_branch`.
    ///
    /// Read-only: walks both commit DAGs to the common ancestor, returns
    /// per-side commit lists and conflict keys. No nameservice or content
    /// store mutations.
    ///
    /// If `target_branch` is `None`, the source's parent branch is used,
    /// matching [`Self::merge_branch`] semantics.
    pub async fn merge_preview(
        &self,
        ledger_name: &str,
        source_branch: &str,
        target_branch: Option<&str>,
    ) -> Result<MergePreview> {
        self.merge_preview_with(
            ledger_name,
            source_branch,
            target_branch,
            MergePreviewOpts::default(),
        )
        .await
    }

    /// Like [`Self::merge_preview`] but with explicit knobs.
    pub async fn merge_preview_with(
        &self,
        ledger_name: &str,
        source_branch: &str,
        target_branch: Option<&str>,
        opts: MergePreviewOpts,
    ) -> Result<MergePreview> {
        let span =
            tracing::debug_span!("merge_preview", ledger_name, source_branch, ?target_branch);
        async move {
            self.merge_preview_inner(ledger_name, source_branch, target_branch, opts)
                .await
        }
        .instrument(span)
        .await
    }

    async fn merge_preview_inner(
        &self,
        ledger_name: &str,
        source_branch: &str,
        target_branch: Option<&str>,
        opts: MergePreviewOpts,
    ) -> Result<MergePreview> {
        // ---- Resolve records (mirrors merge_branch_inner). ----------------
        let source_id = format_ledger_id(ledger_name, source_branch);
        let source_record = self
            .nameservice()
            .lookup(&source_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(source_id.clone()))?;

        let source_parent = source_record.source_branch.as_deref().ok_or_else(|| {
            ApiError::InvalidBranch(format!(
                "Branch {source_branch} has no source branch; \
                 only branches created from another branch can be previewed"
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

        // ---- Build branched stores. ---------------------------------------
        // Source is always a branch by definition (we required source_branch above).
        let source_store = LedgerState::build_branched_store(
            &self.nameservice_mode,
            &source_record,
            self.backend(),
        )
        .await?;

        // Target may or may not be a branch — same logic as merge.rs:296-308.
        // We always wrap as a `BranchedContentStore` (using `leaf` for the
        // non-branch case) so the union store below can chain it as a parent.
        let target_branched: BranchedContentStore = if target_record.source_branch.is_some() {
            LedgerState::build_branched_store(
                &self.nameservice_mode,
                &target_record,
                self.backend(),
            )
            .await?
        } else {
            BranchedContentStore::leaf(self.content_store(&target_id))
        };

        let source_head = source_record.commit_head_id.clone();
        let target_head = target_record.commit_head_id.clone();

        // ---- Find common ancestor. ----------------------------------------
        // The ancestor walk needs to load both `source_head` and `target_head`,
        // which may live in disjoint branch namespaces (e.g., two sibling
        // branches off `main`). We construct a union view that fans out to
        // both branched stores' ancestry so either head's envelope resolves.
        let ancestor = match (&source_head, &target_head) {
            (Some(s), Some(t)) => {
                let union_store = BranchedContentStore::with_parents(
                    Arc::new(source_store.clone()) as Arc<dyn ContentStore>,
                    vec![target_branched.clone()],
                );
                Some(find_common_ancestor(&union_store, s, t).await?)
            }
            _ => None,
        };

        // ---- Fast-forward predicate (mirrors merge.rs:135-139). -----------
        let fast_forward = match (&ancestor, &target_head) {
            (Some(a), Some(tid)) => a.commit_id == *tid,
            (None, None) => true,
            _ => false,
        };

        let stop_at_t = ancestor.as_ref().map_or(0, |a| a.t);

        // ---- Walk both sides in parallel. ---------------------------------
        // `opts.max_commits == None` is a deliberate "unbounded" signal — we
        // pass it through verbatim. The HTTP layer always supplies a bound to
        // protect against unbounded responses; direct Rust callers can opt in.
        let ahead_fut = async {
            match &source_head {
                Some(head) => {
                    walk_commit_summaries(&source_store, head, stop_at_t, opts.max_commits)
                        .await
                        .map_err(ApiError::from)
                }
                None => Ok((Vec::new(), 0)),
            }
        };

        let behind_fut = async {
            match &target_head {
                Some(head) => {
                    walk_commit_summaries(&target_branched, head, stop_at_t, opts.max_commits)
                        .await
                        .map_err(ApiError::from)
                }
                None => Ok((Vec::new(), 0)),
            }
        };

        let ((ahead_summaries, ahead_count), (behind_summaries, behind_count)) =
            tokio::try_join!(ahead_fut, behind_fut)?;

        let ahead = BranchDelta {
            count: ahead_count,
            truncated: ahead_count > ahead_summaries.len(),
            commits: ahead_summaries,
        };
        let behind = BranchDelta {
            count: behind_count,
            truncated: behind_count > behind_summaries.len(),
            commits: behind_summaries,
        };

        // ---- Conflicts (only if relevant). --------------------------------
        let conflicts = if !opts.include_conflicts || fast_forward {
            ConflictSummary::empty()
        } else {
            match (&source_head, &target_head, &ancestor) {
                (Some(s_head), Some(t_head), Some(anc)) => {
                    let s_delta_fut =
                        compute_delta_keys(source_store.clone(), s_head.clone(), anc.t);
                    let t_delta_fut =
                        compute_delta_keys(target_branched.clone(), t_head.clone(), anc.t);
                    let (s_delta, t_delta) = tokio::try_join!(s_delta_fut, t_delta_fut)?;

                    // Sort lexicographically by (s, p, g) so capped responses
                    // are stable across builds and across requests — `HashSet`
                    // intersection order is otherwise unspecified.
                    let mut keys: Vec<ConflictKey> =
                        s_delta.intersection(&t_delta).cloned().collect();
                    keys.sort();
                    let count = keys.len();
                    let truncated = match opts.max_conflict_keys {
                        Some(cap) if count > cap => {
                            keys.truncate(cap);
                            true
                        }
                        _ => false,
                    };
                    ConflictSummary {
                        count,
                        keys,
                        truncated,
                    }
                }
                _ => ConflictSummary::empty(),
            }
        };

        // ---- Invariants (debug-only). -------------------------------------
        debug_assert!(ahead.commits.len() <= ahead.count);
        debug_assert!(behind.commits.len() <= behind.count);
        if fast_forward {
            debug_assert_eq!(behind.count, 0);
            debug_assert_eq!(conflicts.count, 0);
        }
        if source_head.is_some() && target_head.is_some() {
            debug_assert!(ancestor.is_some());
        }

        Ok(MergePreview {
            source: source_branch.to_string(),
            target: resolved_target.to_string(),
            ancestor: ancestor.map(|a| AncestorRef {
                commit_id: a.commit_id,
                t: a.t,
            }),
            ahead,
            behind,
            fast_forward,
            conflicts,
        })
    }
}
