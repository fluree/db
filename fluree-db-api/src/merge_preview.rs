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
use crate::format::iri::IriCompactor;
use crate::graph_commit_builder::resolve_flake;
use crate::rebase::{current_asserted_for_key, ConflictStrategy};
use fluree_db_core::ledger_id::format_ledger_id;
use fluree_db_core::{
    find_common_ancestor, walk_commit_summaries, BranchedContentStore, CommitSummary, ConflictKey,
    ContentId, ContentStore, Flake,
};
use fluree_db_ledger::LedgerState;
use fluree_db_novelty::{compute_delta_keys, compute_delta_keys_and_changes};
use futures::{stream, StreamExt, TryStreamExt};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::Serialize;
use std::sync::Arc;
use tracing::Instrument;

/// Default cap on commits per side returned in [`BranchDelta::commits`].
pub const DEFAULT_MAX_COMMITS: usize = 500;

/// Default cap on conflict keys returned in [`ConflictSummary::keys`].
pub const DEFAULT_MAX_CONFLICT_KEYS: usize = 200;

/// Default cap on change entries returned in [`ChangeSummary::entries`],
/// counted in flakes.
pub const DEFAULT_MAX_CHANGES: usize = 500;

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
    /// When `true`, include source/target flake values for each returned
    /// conflict key. Details are computed only for `conflicts.keys` after the
    /// `max_conflict_keys` cap is applied.
    pub include_conflict_details: bool,
    /// Strategy used for human-readable conflict resolution labels.
    pub conflict_strategy: ConflictStrategy,
    /// When `true`, include the aggregate netted change set the merge would
    /// apply (source side, `ancestor..source_head`) as
    /// [`MergePreview::changes`]. Costs one full commit load per commit in
    /// the source divergence; the walk is shared with the conflict
    /// computation when both are requested.
    pub include_changes: bool,
    /// Cap on change entries returned in [`ChangeSummary::entries`], counted
    /// in **flakes** (not subjects) and cut at subject boundaries — a
    /// subject's diff is never split across the cap, so a single subject
    /// larger than the cap is returned whole (bounded overshoot). `Some(0)`
    /// is a valid "diff stats" mode: exact counts, no payload, and no
    /// source-state load for IRI resolution. `None` is unbounded. Caps the
    /// response size only — counts stay exact and the replay walk is
    /// unaffected.
    pub max_changes: Option<usize>,
    /// Resume cursor for paging [`ChangeSummary::entries`]: return only
    /// subjects whose full IRI sorts strictly after this value. Pass the
    /// previous response's [`ChangeSummary::next_cursor`]. Each page re-pays
    /// the full replay + netting cost. Requires `include_changes`.
    pub changes_after_subject: Option<String>,
}

impl Default for MergePreviewOpts {
    fn default() -> Self {
        Self {
            max_commits: Some(DEFAULT_MAX_COMMITS),
            max_conflict_keys: Some(DEFAULT_MAX_CONFLICT_KEYS),
            include_conflicts: true,
            include_conflict_details: false,
            conflict_strategy: ConflictStrategy::default(),
            include_changes: false,
            max_changes: Some(DEFAULT_MAX_CHANGES),
            changes_after_subject: None,
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
    /// Strategy used to annotate conflict details. Omitted when conflicts were
    /// not computed or no conflict keys were returned.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy: Option<String>,
    /// Per-key source/target values for the returned conflict keys.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub details: Vec<ConflictDetail>,
}

/// A conflict key with the source and target flakes that touched it.
#[derive(Clone, Debug, Serialize)]
pub struct ConflictDetail {
    pub key: ConflictKey,
    pub source_values: Vec<crate::ResolvedFlake>,
    pub target_values: Vec<crate::ResolvedFlake>,
    pub resolution: ConflictResolutionPreview,
}

/// Human-readable strategy annotation for a single conflict row.
#[derive(Clone, Debug, Serialize)]
pub struct ConflictResolutionPreview {
    pub source_action: &'static str,
    pub target_action: &'static str,
    pub outcome: &'static str,
}

impl ConflictSummary {
    fn empty() -> Self {
        Self {
            count: 0,
            keys: Vec::new(),
            truncated: false,
            strategy: None,
            details: Vec::new(),
        }
    }
}

/// Aggregate netted change set a merge would apply — the source side's
/// `ancestor..source_head` flakes folded per fact, with
/// internally-cancelling assert/retract pairs removed.
///
/// ### Netting contract
///
/// Per fact (full identity: subject, predicate, object, datatype, graph,
/// language tag, list index), the net op is the newest in-range op; a fact
/// survives only when its oldest and newest in-range ops agree. Intermediate
/// churn (create-then-delete, delete-then-restore) never appears. This is
/// the **net commit effect** — what replaying the range applies, minus
/// pairs that cancel — so a re-assert of a value that already existed
/// before the range still nets as an assert (the merge does apply it).
///
/// The change set is strategy-independent: it is the raw source-vs-ancestor
/// delta, *before* conflict resolution. Under a non-default strategy,
/// conflicting keys resolve per the separately-returned
/// [`ConflictSummary::details`].
#[derive(Clone, Debug, Serialize)]
pub struct ChangeSummary {
    /// Exact net assert count across the full divergence. Never truncated.
    pub assert_count: usize,
    /// Exact net retract count across the full divergence. Never truncated.
    pub retract_count: usize,
    /// Exact number of distinct subjects touched by net changes.
    pub subject_count: usize,
    /// Net changes grouped by subject, subjects ordered by full IRI.
    /// Bounded by [`MergePreviewOpts::max_changes`] (counting flakes, cut at
    /// subject boundaries) and offset by
    /// [`MergePreviewOpts::changes_after_subject`].
    pub entries: Vec<SubjectChange>,
    /// `true` when subjects after this page were withheld — either by the
    /// flake cap or because `max_changes = 0` suppressed the payload.
    pub truncated: bool,
    /// Cursor for the next page when `truncated` by the flake cap: the last
    /// returned subject IRI, to be passed as `changes_after_subject`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

impl ChangeSummary {
    fn empty() -> Self {
        Self {
            assert_count: 0,
            retract_count: 0,
            subject_count: 0,
            entries: Vec::new(),
            truncated: false,
            next_cursor: None,
        }
    }
}

/// Net changes for one subject.
#[derive(Clone, Debug, Serialize)]
pub struct SubjectChange {
    /// Full (expanded) subject IRI — also the pagination sort key.
    pub subject: String,
    /// Net additions on this subject.
    pub asserts: Vec<crate::ResolvedFlake>,
    /// Net removals on this subject.
    pub retracts: Vec<crate::ResolvedFlake>,
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

    /// Whether the selected strategy can be applied without aborting.
    pub mergeable: bool,

    /// Aggregate netted change set. Present iff
    /// [`MergePreviewOpts::include_changes`] was set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changes: Option<ChangeSummary>,
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
        if opts.include_conflict_details && !opts.include_conflicts {
            return Err(ApiError::InvalidBranch(
                "include_conflict_details requires include_conflicts=true".to_string(),
            ));
        }
        if opts.conflict_strategy == ConflictStrategy::Abort && !opts.include_conflicts {
            return Err(ApiError::InvalidBranch(
                "strategy=abort requires include_conflicts=true for mergeable preview".to_string(),
            ));
        }
        if opts.conflict_strategy == ConflictStrategy::Skip {
            return Err(ApiError::InvalidBranch(
                "Skip strategy is not supported for merge preview".to_string(),
            ));
        }
        if opts.changes_after_subject.is_some() && !opts.include_changes {
            return Err(ApiError::InvalidBranch(
                "changes_after_subject requires include_changes=true".to_string(),
            ));
        }

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

        // ---- Source-side replay (conflicts and/or changes). ---------------
        // Conflicts and the netted change set both fold the source commit
        // chain since the ancestor; when both are requested one walk serves
        // both. Conflicts are only possible on non-fast-forward merges;
        // changes are meaningful regardless (fast-forward is the common
        // merge-request review case).
        let need_conflicts = opts.include_conflicts
            && !fast_forward
            && source_head.is_some()
            && target_head.is_some()
            && ancestor.is_some();
        let need_changes = opts.include_changes;

        let source_fut = async {
            match &source_head {
                Some(s_head) if need_changes => {
                    let (keys, net) = compute_delta_keys_and_changes(
                        source_store.clone(),
                        s_head.clone(),
                        stop_at_t,
                    )
                    .await?;
                    Ok::<_, ApiError>((Some(keys), Some(net)))
                }
                Some(s_head) if need_conflicts => {
                    let keys =
                        compute_delta_keys(source_store.clone(), s_head.clone(), stop_at_t).await?;
                    Ok((Some(keys), None))
                }
                _ => Ok((None, None)),
            }
        };
        let target_fut = async {
            match (&target_head, &ancestor) {
                (Some(t_head), Some(anc)) if need_conflicts => {
                    let keys =
                        compute_delta_keys(target_branched.clone(), t_head.clone(), anc.t).await?;
                    Ok::<_, ApiError>(Some(keys))
                }
                _ => Ok(None),
            }
        };
        let ((source_delta, net_flakes), target_delta) = tokio::try_join!(source_fut, target_fut)?;

        // ---- Conflicts (only if relevant). --------------------------------
        let (conflict_keys, conflict_count, conflicts_truncated) =
            match (need_conflicts, &source_delta, &target_delta) {
                (true, Some(s_delta), Some(t_delta)) => {
                    // Sort lexicographically by (s, p, g) so capped responses
                    // are stable across builds and across requests — `HashSet`
                    // intersection order is otherwise unspecified.
                    let mut keys: Vec<ConflictKey> =
                        s_delta.intersection(t_delta).cloned().collect();
                    keys.sort();
                    let count = keys.len();
                    let truncated = match opts.max_conflict_keys {
                        Some(cap) if count > cap => {
                            keys.truncate(cap);
                            true
                        }
                        _ => false,
                    };
                    (keys, count, truncated)
                }
                _ => (Vec::new(), 0, false),
            };

        // ---- Load states needed for IRI resolution. -----------------------
        // Conflict details need both sides; the change payload needs only the
        // source side (its head state carries every namespace code the
        // divergence introduced). Stats-only mode (`max_changes = 0`) skips
        // the load entirely.
        let want_details = opts.include_conflict_details && !conflict_keys.is_empty();
        let want_change_payload = need_changes
            && net_flakes.as_ref().is_some_and(|n| !n.is_empty())
            && opts.max_changes != Some(0);

        let (source_state, target_state) = if want_details {
            let source_state_fut =
                self.load_queryable_state_with_store(source_store.clone(), source_record.clone());
            let target_state_fut = self
                .load_queryable_state_with_store(target_branched.clone(), target_record.clone());
            let (s, t) = tokio::try_join!(source_state_fut, target_state_fut)?;
            (Some(s), Some(t))
        } else if want_change_payload {
            let s = self
                .load_queryable_state_with_store(source_store.clone(), source_record.clone())
                .await?;
            (Some(s), None)
        } else {
            (None, None)
        };

        let conflicts = if !need_conflicts {
            ConflictSummary::empty()
        } else {
            let details = if want_details {
                build_conflict_details(
                    &conflict_keys,
                    source_state.as_ref().expect("loaded for details"),
                    target_state.as_ref().expect("loaded for details"),
                    &opts.conflict_strategy,
                )
                .await?
            } else {
                Vec::new()
            };

            ConflictSummary {
                count: conflict_count,
                keys: conflict_keys,
                truncated: conflicts_truncated,
                strategy: if conflict_count > 0 {
                    Some(opts.conflict_strategy.as_str().to_string())
                } else {
                    None
                },
                details,
            }
        };

        // ---- Changes (only if requested). ----------------------------------
        let changes = if need_changes {
            // No compactor in stats-only mode even when conflict details
            // loaded the source state — `max_changes = 0` means no payload.
            let compactor = if want_change_payload {
                source_state
                    .as_ref()
                    .map(|s| IriCompactor::from_namespaces(s.snapshot.shared_namespaces()))
            } else {
                None
            };
            let summary = match net_flakes {
                Some(net) if !net.is_empty() => build_change_summary(
                    net,
                    compactor,
                    opts.max_changes,
                    opts.changes_after_subject.as_deref(),
                )?,
                _ => ChangeSummary::empty(),
            };
            Some(summary)
        } else {
            None
        };

        let mergeable = opts.conflict_strategy != ConflictStrategy::Abort || conflicts.count == 0;

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
            mergeable,
            changes,
        })
    }
}

/// Group netted flakes by subject, order deterministically, and apply the
/// cursor + flake cap. Counts are computed over the full net set before any
/// truncation. `compactor: None` is stats-only mode — exact counts, no
/// entries.
fn build_change_summary(
    net_flakes: Vec<Flake>,
    compactor: Option<IriCompactor>,
    max_changes: Option<usize>,
    after_subject: Option<&str>,
) -> Result<ChangeSummary> {
    let assert_count = net_flakes.iter().filter(|f| f.op).count();
    let retract_count = net_flakes.len() - assert_count;

    let Some(compactor) = compactor else {
        // Stats-only: distinct subjects countable without IRI resolution
        // (Sid ↔ IRI is bijective under one namespace table).
        let subjects: FxHashSet<&fluree_db_core::Sid> = net_flakes.iter().map(|f| &f.s).collect();
        return Ok(ChangeSummary {
            assert_count,
            retract_count,
            subject_count: subjects.len(),
            entries: Vec::new(),
            truncated: !net_flakes.is_empty(),
            next_cursor: None,
        });
    };

    // Group by Sid first — a cheap Arc-clone key with no IRI resolution — so
    // each distinct subject is decoded exactly once rather than once per flake.
    // Sorting by the decoded IRI then gives the deterministic subject order
    // that pagination cursors rely on.
    let mut by_sid: FxHashMap<fluree_db_core::Sid, Vec<Flake>> = FxHashMap::default();
    for f in net_flakes {
        by_sid.entry(f.s.clone()).or_default().push(f);
    }
    let subject_count = by_sid.len();

    let mut by_subject: Vec<(String, Vec<Flake>)> = by_sid
        .into_iter()
        .map(|(sid, flakes)| Ok((compactor.decode_sid(&sid)?, flakes)))
        .collect::<Result<_>>()?;
    by_subject.sort_by(|(a, _), (b, _)| a.cmp(b));

    let mut entries = Vec::new();
    let mut emitted_flakes = 0usize;
    let mut truncated = false;

    for (subject, mut flakes) in by_subject
        .into_iter()
        .skip_while(|(s, _)| after_subject.is_some_and(|a| s.as_str() <= a))
    {
        // Cut at subject boundaries: stop before a subject that would cross
        // the cap — unless it's the first subject of the page, which is
        // returned whole (bounded overshoot) so progress is always possible.
        if let Some(cap) = max_changes {
            if !entries.is_empty() && emitted_flakes + flakes.len() > cap {
                truncated = true;
                break;
            }
        }
        emitted_flakes += flakes.len();

        // Deterministic order within a subject.
        flakes.sort_by(|a, b| {
            a.p.cmp(&b.p)
                .then_with(|| a.o.cmp(&b.o))
                .then_with(|| a.dt.cmp(&b.dt))
                .then_with(|| a.m.cmp(&b.m))
        });

        let mut asserts = Vec::new();
        let mut retracts = Vec::new();
        for f in &flakes {
            let resolved = resolve_flake(&compactor, f)?;
            if f.op {
                asserts.push(resolved);
            } else {
                retracts.push(resolved);
            }
        }
        entries.push(SubjectChange {
            subject,
            asserts,
            retracts,
        });
    }

    let next_cursor = if truncated {
        entries.last().map(|e| e.subject.clone())
    } else {
        None
    };

    Ok(ChangeSummary {
        assert_count,
        retract_count,
        subject_count,
        entries,
        truncated,
        next_cursor,
    })
}

async fn build_conflict_details(
    keys: &[ConflictKey],
    source_state: &LedgerState,
    target_state: &LedgerState,
    strategy: &ConflictStrategy,
) -> Result<Vec<ConflictDetail>> {
    let source_compactor = IriCompactor::from_namespaces(source_state.snapshot.shared_namespaces());
    let target_compactor = IriCompactor::from_namespaces(target_state.snapshot.shared_namespaces());
    let resolution = resolution_for_strategy(strategy);

    stream::iter(keys.iter().cloned())
        .map(|key| {
            let source_compactor = &source_compactor;
            let target_compactor = &target_compactor;
            let resolution = resolution.clone();
            async move {
                let (source_flakes, target_flakes) = tokio::try_join!(
                    current_asserted_for_key(source_state, &key),
                    current_asserted_for_key(target_state, &key),
                )?;
                let source_values = resolve_flake_list(&source_flakes, source_compactor)?;
                let target_values = resolve_flake_list(&target_flakes, target_compactor)?;

                Ok::<_, ApiError>(ConflictDetail {
                    key,
                    source_values,
                    target_values,
                    resolution,
                })
            }
        })
        .buffered(8)
        .try_collect()
        .await
}

fn resolve_flake_list(
    flakes: &[Flake],
    compactor: &IriCompactor,
) -> Result<Vec<crate::ResolvedFlake>> {
    flakes
        .iter()
        .map(|flake| resolve_flake(compactor, flake))
        .collect()
}

fn resolution_for_strategy(strategy: &ConflictStrategy) -> ConflictResolutionPreview {
    match strategy {
        ConflictStrategy::TakeBoth => ConflictResolutionPreview {
            source_action: "kept",
            target_action: "kept",
            outcome: "both-values-kept",
        },
        ConflictStrategy::TakeSource => ConflictResolutionPreview {
            source_action: "kept",
            target_action: "retracted",
            outcome: "source-wins",
        },
        ConflictStrategy::TakeBranch => ConflictResolutionPreview {
            source_action: "dropped",
            target_action: "kept",
            outcome: "target-wins",
        },
        ConflictStrategy::Abort => ConflictResolutionPreview {
            source_action: "unchanged",
            target_action: "unchanged",
            outcome: "merge-aborts",
        },
        ConflictStrategy::Skip => unreachable!("skip strategy is rejected before previewing"),
    }
}
