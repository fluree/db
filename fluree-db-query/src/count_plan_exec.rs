//! Count-only plan executor — evaluates a `CountPlan` against a `BinaryIndexStore`.
//!
//! The executor wraps as a `FastPathOperator` closure. During `open()`:
//! 1. Call `fast_path_store(ctx)` — return `Ok(None)` if not binary-index (triggers fallback)
//! 2. Resolve all `Ref` predicates to `p_id`s
//! 3. Recursively evaluate the plan tree using existing iterator primitives
//! 4. Return single count batch
//!
//! See `count_plan.rs` for the IR definition and planner.

use crate::count_plan::{
    ChainFold, CountPlan, CountPlanRoot, KeySetNode, ScalarNode, StreamNode, TailWeight,
};
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, collect_subjects_for_predicate_set, collect_subjects_for_predicate_sorted,
    collect_subjects_with_object_in_set, count_rows_for_predicate_psot, fast_path_store,
    intersect_many_sorted, leaf_entries_for_predicate, normalize_pred_sid,
    sum_post_object_counts_filtered, FastPathOperator, ObjectFilterMode, PostObjectGroupCountIter,
    PsotObjectFilterCountIter, PsotSubjectCountIter, PsotSubjectWeightedSumIter,
};
use crate::operator::BoxedOperator;
use fluree_db_binary_index::{BinaryIndexStore, RunSortOrder};
use fluree_db_core::GraphId;
use rustc_hash::{FxHashMap, FxHashSet};
use std::sync::Arc;

/// Create a `FastPathOperator` that executes a `CountPlan`.
pub(crate) fn count_plan_operator(
    plan: CountPlan,
    fallback: Option<BoxedOperator>,
) -> BoxedOperator {
    let out_var = plan.out_var;
    Box::new(FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let g_id = ctx.binary_g_id;

            match execute_plan(&plan.root, store, g_id)? {
                Some(count) => {
                    let count_i64 = i64::try_from(count)
                        .map_err(|_| QueryError::execution("COUNT(*) exceeds i64 in count plan"))?;
                    Ok(Some(build_count_batch(out_var, count_i64)?))
                }
                None => Ok(None), // Fall through to general pipeline.
            }
        },
        fallback,
        "count-plan",
    ))
}

// ===========================================================================
// Plan evaluation
// ===========================================================================

fn execute_plan(
    root: &CountPlanRoot,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
) -> Result<Option<u64>> {
    match root {
        CountPlanRoot::Scalar(scalar) => execute_scalar(scalar, store, g_id),
        CountPlanRoot::Chain(chain) => execute_chain(chain, store, g_id),
    }
}

// ---------------------------------------------------------------------------
// Scalar evaluation
// ---------------------------------------------------------------------------

fn execute_scalar(
    node: &ScalarNode,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
) -> Result<Option<u64>> {
    match node {
        ScalarNode::TotalRowCount { pred } => {
            let sid = normalize_pred_sid(store, pred)?;
            let Some(p_id) = store.sid_to_p_id(&sid) else {
                return Ok(Some(0));
            };
            Ok(Some(count_rows_for_predicate_psot(store, g_id, p_id)?))
        }

        ScalarNode::Sum { source } => {
            let total = sum_stream(source, store, g_id, None, None)?;
            Ok(total)
        }

        ScalarNode::SumExcluding { source, excluded } => {
            let exclude_sorted = execute_keyset_as_sorted(excluded, store, g_id)?;
            let exclude_sorted = match exclude_sorted {
                Some(s) => s,
                None => return Ok(None),
            };
            let total = sum_stream(source, store, g_id, Some(&exclude_sorted), None)?;
            Ok(total)
        }

        ScalarNode::SumFiltered { source, filter } => {
            let filter_sorted = execute_keyset_as_sorted(filter, store, g_id)?;
            let filter_sorted = match filter_sorted {
                Some(s) => s,
                None => return Ok(None),
            };
            let total = sum_stream(source, store, g_id, None, Some(&filter_sorted))?;
            Ok(total)
        }

        ScalarNode::PostObjectFilteredSum {
            pred,
            object_filter,
        } => {
            let sid = normalize_pred_sid(store, pred)?;
            let Some(p_id) = store.sid_to_p_id(&sid) else {
                return Ok(Some(0));
            };
            let filter_sorted = execute_keyset_as_sorted(object_filter, store, g_id)?;
            let filter_sorted = match filter_sorted {
                Some(s) => s,
                None => return Ok(None),
            };
            if filter_sorted.is_empty() {
                return Ok(Some(0));
            }
            sum_post_object_counts_filtered(store, g_id, p_id, &filter_sorted)
        }

        ScalarNode::TotalMinusPostObjectFilteredSum {
            pred,
            excluded_objects,
        } => {
            let sid = normalize_pred_sid(store, pred)?;
            let Some(p_id) = store.sid_to_p_id(&sid) else {
                return Ok(Some(0));
            };
            let total = count_rows_for_predicate_psot(store, g_id, p_id)?;
            let excluded_sorted = execute_keyset_as_sorted(excluded_objects, store, g_id)?;
            let excluded_sorted = match excluded_sorted {
                Some(s) => s,
                None => return Ok(None),
            };
            if excluded_sorted.is_empty() {
                return Ok(Some(total));
            }
            let Some(in_set) =
                sum_post_object_counts_filtered(store, g_id, p_id, &excluded_sorted)?
            else {
                return Ok(None);
            };
            Ok(Some(total.saturating_sub(in_set)))
        }
    }
}

// ---------------------------------------------------------------------------
// Stream evaluation — produces (key, count) pairs, summed with optional filters
// ---------------------------------------------------------------------------

/// Sum all `(key, count)` from a stream, optionally filtering by exclude/include sets.
///
/// For MINUS/EXISTS modifiers on star/single-triple shapes, we pre-compute a sorted
/// exclusion or inclusion list and use a running index pointer for O(1) amortized
/// per-subject filtering (matching `fast_minus_count_all.rs` and `fast_exists_count_all.rs`).
fn sum_stream(
    node: &StreamNode,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    exclude_sorted: Option<&[u64]>,
    include_sorted: Option<&[u64]>,
) -> Result<Option<u64>> {
    match node {
        StreamNode::SubjectCountScan { pred } => {
            let sid = normalize_pred_sid(store, pred)?;
            let Some(p_id) = store.sid_to_p_id(&sid) else {
                return Ok(Some(0));
            };
            let mut iter = PsotSubjectCountIter::new(store, g_id, p_id)?;
            let mut excl_idx: usize = 0;
            let mut incl_idx: usize = 0;
            let mut total: u128 = 0;
            while let Some((s, count)) = iter.next_group()? {
                if is_excluded(s, exclude_sorted, &mut excl_idx) {
                    continue;
                }
                if !is_included(s, include_sorted, &mut incl_idx) {
                    continue;
                }
                total = total.saturating_add(count as u128);
            }
            Ok(Some(total.min(u64::MAX as u128) as u64))
        }

        StreamNode::StarJoin { children } => {
            sum_star_join(children, store, g_id, exclude_sorted, include_sorted)
        }

        StreamNode::OptionalJoin {
            required,
            optional_groups,
        } => sum_optional_join(
            required,
            optional_groups,
            store,
            g_id,
            exclude_sorted,
            include_sorted,
        ),

        StreamNode::AntiJoin { source, excluded } => {
            let exclude_list = execute_keyset_as_sorted(excluded, store, g_id)?;
            let exclude_list = match exclude_list {
                Some(s) => s,
                None => return Ok(None),
            };
            // Merge with any existing sorted exclusion list.
            let merged = merge_sorted_lists(exclude_sorted, &exclude_list);
            sum_stream(source, store, g_id, Some(&merged), include_sorted)
        }

        StreamNode::SemiJoin { source, filter } => {
            let filter_list = execute_keyset_as_sorted(filter, store, g_id)?;
            let filter_list = match filter_list {
                Some(s) => s,
                None => return Ok(None),
            };
            // Intersect with any existing sorted inclusion list.
            let merged = match include_sorted {
                Some(existing) => intersect_sorted_pair(existing, &filter_list),
                None => filter_list,
            };
            sum_stream(source, store, g_id, exclude_sorted, Some(&merged))
        }
    }
}

/// Check if `key` is in the sorted exclusion list, advancing the index pointer.
#[inline]
fn is_excluded(key: u64, sorted: Option<&[u64]>, idx: &mut usize) -> bool {
    let Some(list) = sorted else { return false };
    while *idx < list.len() && list[*idx] < key {
        *idx += 1;
    }
    *idx < list.len() && list[*idx] == key
}

/// Check if `key` is in the sorted inclusion list, advancing the index pointer.
/// Returns true if there is no inclusion list (no filter) or key is present.
#[inline]
fn is_included(key: u64, sorted: Option<&[u64]>, idx: &mut usize) -> bool {
    let Some(list) = sorted else { return true };
    while *idx < list.len() && list[*idx] < key {
        *idx += 1;
    }
    *idx < list.len() && list[*idx] == key
}

/// Merge two sorted lists into a deduplicated sorted union.
fn merge_sorted_lists(existing: Option<&[u64]>, new: &[u64]) -> Vec<u64> {
    let Some(existing) = existing else {
        return new.to_vec();
    };
    let mut result = Vec::with_capacity(existing.len() + new.len());
    let (mut i, mut j) = (0, 0);
    while i < existing.len() && j < new.len() {
        match existing[i].cmp(&new[j]) {
            std::cmp::Ordering::Less => {
                result.push(existing[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                result.push(new[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                result.push(existing[i]);
                i += 1;
                j += 1;
            }
        }
    }
    result.extend_from_slice(&existing[i..]);
    result.extend_from_slice(&new[j..]);
    result
}

/// Intersect two sorted lists into a sorted intersection.
fn intersect_sorted_pair(a: &[u64], b: &[u64]) -> Vec<u64> {
    let mut result = Vec::with_capacity(a.len().min(b.len()));
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                result.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    result
}

/// N-way merge-join on subject count iterators, multiplying counts per key.
///
/// Uses sorted-list-based exclusion/inclusion with running index pointers for O(1)
/// amortized per-subject filtering (matching `fast_minus_count_all::count_property_join_all`).
///
/// Formula: `Σ_{s in all, not excluded, included} Π_i count_i(s)`
fn sum_star_join(
    children: &[StreamNode],
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    exclude_sorted: Option<&[u64]>,
    include_sorted: Option<&[u64]>,
) -> Result<Option<u64>> {
    // All children must be SubjectCountScan for the streaming N-way merge.
    let mut iters: Vec<PsotSubjectCountIter<'_>> = Vec::with_capacity(children.len());
    for child in children {
        let StreamNode::SubjectCountScan { pred } = child else {
            return Ok(None);
        };
        let sid = normalize_pred_sid(store, pred)?;
        let Some(p_id) = store.sid_to_p_id(&sid) else {
            return Ok(Some(0));
        };
        iters.push(PsotSubjectCountIter::new(store, g_id, p_id)?);
    }

    let mut curr: Vec<Option<(u64, u64)>> = Vec::with_capacity(iters.len());
    for it in &mut iters {
        curr.push(it.next_group()?);
    }

    let mut excl_idx: usize = 0;
    let mut incl_idx: usize = 0;
    let mut total: u128 = 0;

    loop {
        if curr.iter().any(std::option::Option::is_none) {
            break;
        }

        let max_s = curr.iter().filter_map(|c| c.map(|(s, _)| s)).max().unwrap();

        if curr.iter().all(|c| c.map(|(s, _)| s) == Some(max_s)) {
            let skip = is_excluded(max_s, exclude_sorted, &mut excl_idx)
                || !is_included(max_s, include_sorted, &mut incl_idx);

            if !skip {
                let product: u128 = curr.iter().map(|c| c.unwrap().1 as u128).product();
                total = total.saturating_add(product);
            }

            for (i, it) in iters.iter_mut().enumerate() {
                curr[i] = it.next_group()?;
            }
        } else {
            for (i, it) in iters.iter_mut().enumerate() {
                if let Some((s_id, _)) = curr[i] {
                    if s_id < max_s {
                        curr[i] = it.next_group()?;
                    }
                }
            }
        }
    }

    Ok(Some(total.min(u64::MAX as u128) as u64))
}

/// Fully streaming merge-join with OPTIONAL semantics.
///
/// Interleaves optional group cursor advancement with the required N-way merge,
/// matching the star-join + OPTIONAL multiplicity algorithm.
/// No HashMap materialization for required or optional streams.
///
/// Formula: `Σ_s req_product(s) × Π_g max(1, Π_i opt_gi(s))`
fn sum_optional_join(
    required: &StreamNode,
    optional_groups: &[Vec<StreamNode>],
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    exclude_sorted: Option<&[u64]>,
    include_sorted: Option<&[u64]>,
) -> Result<Option<u64>> {
    // Collect required iterators (single scan or star join children).
    let mut req_iters: Vec<PsotSubjectCountIter<'_>> = Vec::new();
    match required {
        StreamNode::SubjectCountScan { pred } => {
            let sid = normalize_pred_sid(store, pred)?;
            let Some(p_id) = store.sid_to_p_id(&sid) else {
                return Ok(Some(0));
            };
            req_iters.push(PsotSubjectCountIter::new(store, g_id, p_id)?);
        }
        StreamNode::StarJoin { children } => {
            for child in children {
                let StreamNode::SubjectCountScan { pred } = child else {
                    return Ok(None);
                };
                let sid = normalize_pred_sid(store, pred)?;
                let Some(p_id) = store.sid_to_p_id(&sid) else {
                    return Ok(Some(0));
                };
                req_iters.push(PsotSubjectCountIter::new(store, g_id, p_id)?);
            }
        }
        _ => return Ok(None),
    }

    // Optional groups: each group is a same-subject star; multiplier is max(1, Π counts).
    // An optional predicate that is absent in the store makes the entire group `always_one`.
    struct OptGroup<'a> {
        always_one: bool,
        iters: Vec<PsotSubjectCountIter<'a>>,
        cur: Vec<Option<(u64, u64)>>,
    }

    let mut opt_groups: Vec<OptGroup<'_>> = Vec::with_capacity(optional_groups.len());
    for grp in optional_groups {
        let mut always_one = false;
        let mut iters: Vec<PsotSubjectCountIter<'_>> = Vec::with_capacity(grp.len());
        for node in grp {
            let StreamNode::SubjectCountScan { pred } = node else {
                return Ok(None);
            };
            let sid = normalize_pred_sid(store, pred)?;
            let Some(p_id) = store.sid_to_p_id(&sid) else {
                // Absent optional predicate => group never matches => multiplier 1.
                always_one = true;
                iters.clear();
                break;
            };
            iters.push(PsotSubjectCountIter::new(store, g_id, p_id)?);
        }
        let mut cur: Vec<Option<(u64, u64)>> = Vec::with_capacity(iters.len());
        for it in &mut iters {
            cur.push(it.next_group()?);
        }
        opt_groups.push(OptGroup {
            always_one,
            iters,
            cur,
        });
    }

    // Prime required cursors.
    let mut req_cur: Vec<Option<(u64, u64)>> = Vec::with_capacity(req_iters.len());
    for it in &mut req_iters {
        req_cur.push(it.next_group()?);
    }

    let mut excl_idx: usize = 0;
    let mut incl_idx: usize = 0;
    let mut total: u128 = 0;

    loop {
        if req_cur.iter().any(std::option::Option::is_none) {
            break;
        }

        let max_s = req_cur
            .iter()
            .filter_map(|c| c.map(|(s, _)| s))
            .max()
            .unwrap();

        if req_cur.iter().all(|c| c.map(|(s, _)| s) == Some(max_s)) {
            let skip = is_excluded(max_s, exclude_sorted, &mut excl_idx)
                || !is_included(max_s, include_sorted, &mut incl_idx);

            if !skip {
                // Required product at this subject.
                let mut product: u128 = req_cur.iter().map(|c| c.unwrap().1 as u128).product();

                // Multiply OPTIONAL group factors for this subject (streaming).
                for g in &mut opt_groups {
                    if g.always_one {
                        continue;
                    }
                    let mut g_prod: u128 = 1;
                    for i in 0..g.iters.len() {
                        // Advance optional cursor to >= max_s.
                        while let Some((sid2, _)) = g.cur[i] {
                            if sid2 < max_s {
                                g.cur[i] = g.iters[i].next_group()?;
                                continue;
                            }
                            break;
                        }
                        let c = match g.cur[i] {
                            Some((sid2, c)) if sid2 == max_s => {
                                g.cur[i] = g.iters[i].next_group()?;
                                c
                            }
                            _ => 0u64,
                        };
                        if c == 0 {
                            g_prod = 0;
                            break;
                        }
                        g_prod = g_prod.saturating_mul(c as u128);
                    }
                    let mult = if g_prod == 0 { 1u128 } else { g_prod };
                    product = product.saturating_mul(mult);
                }

                total = total.saturating_add(product);
            } else {
                // Still need to advance optional cursors past this subject
                // to keep them in sync even when the required subject is skipped.
                for g in &mut opt_groups {
                    if g.always_one {
                        continue;
                    }
                    for i in 0..g.iters.len() {
                        while let Some((sid2, _)) = g.cur[i] {
                            if sid2 < max_s {
                                g.cur[i] = g.iters[i].next_group()?;
                                continue;
                            }
                            break;
                        }
                        if let Some((sid2, _)) = g.cur[i] {
                            if sid2 == max_s {
                                g.cur[i] = g.iters[i].next_group()?;
                            }
                        }
                    }
                }
            }

            // Advance required iterators.
            for (i, it) in req_iters.iter_mut().enumerate() {
                req_cur[i] = it.next_group()?;
            }
        } else {
            // Advance smaller required subjects up to the current max.
            for (i, it) in req_iters.iter_mut().enumerate() {
                if let Some((s_id, _)) = req_cur[i] {
                    if s_id < max_s {
                        req_cur[i] = it.next_group()?;
                    }
                }
            }
        }
    }

    Ok(Some(total.min(u64::MAX as u128) as u64))
}

// ---------------------------------------------------------------------------
// KeySet evaluation — produces materialized sorted lists or hash sets
// ---------------------------------------------------------------------------

/// Primary keyset evaluator: returns a sorted `Vec<u64>`.
///
/// All callers in Phase B use sorted lists for streaming merge-skip/merge-keep.
fn execute_keyset_as_sorted(
    node: &KeySetNode,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
) -> Result<Option<Vec<u64>>> {
    match node {
        KeySetNode::SubjectsSorted { pred } | KeySetNode::SubjectSet { pred } => {
            let sid = normalize_pred_sid(store, pred)?;
            let Some(p_id) = store.sid_to_p_id(&sid) else {
                return Ok(Some(Vec::new()));
            };
            Ok(Some(collect_subjects_for_predicate_sorted(
                store, g_id, p_id,
            )?))
        }
        KeySetNode::SubjectsWithObjectIn { pred, object_set } => {
            // Need a hash set for the object filter, then sort the result.
            let obj_set = execute_keyset_as_hash_set(object_set, store, g_id)?;
            let obj_set = match obj_set {
                Some(s) => s,
                None => return Ok(None),
            };
            let sid = normalize_pred_sid(store, pred)?;
            let Some(p_id) = store.sid_to_p_id(&sid) else {
                return Ok(Some(Vec::new()));
            };
            let Some(mut subjects) =
                collect_subjects_with_object_in_set(store, g_id, p_id, &obj_set)?
            else {
                return Ok(None);
            };
            subjects.sort_unstable();
            subjects.dedup();
            Ok(Some(subjects))
        }
        KeySetNode::IntersectSorted { children } => {
            let mut lists: Vec<Vec<u64>> = Vec::with_capacity(children.len());
            for child in children {
                let sorted = execute_keyset_as_sorted(child, store, g_id)?;
                let sorted = match sorted {
                    Some(s) => s,
                    None => return Ok(None),
                };
                lists.push(sorted);
            }
            Ok(Some(intersect_many_sorted(lists)))
        }
    }
}

/// Hash-set evaluator for keysets — used only by `SubjectsWithObjectIn` which
/// needs `collect_subjects_with_object_in_set(&FxHashSet)`.
fn execute_keyset_as_hash_set(
    node: &KeySetNode,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
) -> Result<Option<FxHashSet<u64>>> {
    match node {
        KeySetNode::SubjectSet { pred } => {
            let sid = normalize_pred_sid(store, pred)?;
            let Some(p_id) = store.sid_to_p_id(&sid) else {
                return Ok(Some(FxHashSet::default()));
            };
            Ok(Some(collect_subjects_for_predicate_set(store, g_id, p_id)?))
        }
        _ => {
            // Fall back: get sorted, convert to set.
            let sorted = execute_keyset_as_sorted(node, store, g_id)?;
            match sorted {
                Some(v) => Ok(Some(v.into_iter().collect())),
                None => Ok(None),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Chain evaluation
// ---------------------------------------------------------------------------

/// Execute a chain fold: right-to-left accumulation over a linear chain.
///
/// For a chain `?v0 <p1> ?v1 . ?v1 <p2> ?v2 . ... ?v_{N-1} <pN> ?vN`:
///
/// **Step 1** — Build initial weights keyed by `v_{N-1}` (subjects of pN).
///   - `TailWeight::None`: `weights[v_{N-1}] = count_pN(v_{N-1})`
///   - `TailWeight::Optional { tail_pred }`: For each `v_{N-1}`, compute
///     `Σ_{vN in pN(v_{N-1})} max(1, count_tail(vN))` via `PsotSubjectWeightedSumIter`.
///   - `TailWeight::Minus { tail_pred }`: Count only pN objects NOT in `subjects(tail_pred)`
///     via `PsotObjectFilterCountIter`.
///   - `TailWeight::Exists { tail_pred }`: Count only pN objects IN `subjects(tail_pred)`
///     via `PsotObjectFilterCountIter`.
///
/// **Step 2** — Fold right-to-left through `p_{N-1}` … `p2` via `PsotSubjectWeightedSumIter`.
///
/// **Step 3** — Final merge: `POST(p1)` objects × weights.
fn execute_chain(
    chain: &ChainFold,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
) -> Result<Option<u64>> {
    assert!(
        chain.predicates.len() >= 2,
        "chain must have at least 2 predicates"
    );

    let n = chain.predicates.len();

    // Resolve all predicate Refs to p_ids.
    let mut p_ids: Vec<u32> = Vec::with_capacity(n);
    for pred in &chain.predicates {
        let sid = normalize_pred_sid(store, pred)?;
        let Some(p_id) = store.sid_to_p_id(&sid) else {
            // Missing predicate in an inner join chain → 0.
            return Ok(Some(0));
        };
        p_ids.push(p_id);
    }

    // Generic chain speed trick (this is what made the 2026-03-09 numbers good):
    //
    // Do NOT scan PSOT(p2) across all subjects. Instead:
    // - Stream POST(p1) grouped by object b (these are the only b values that matter)
    // - For each b, *seek* into PSOT(p2) to sum weights over its objects
    //
    // This is especially important when p2 is huge (e.g. rdf:type) but p1's object
    // domain is much smaller.

    enum P2WeightMode<'a> {
        /// Each edge counts as 1 (used when the chain ends at p2 with no tail modifier).
        CountEdges { non_iri_weight: u64 },
        /// Weight is looked up by object key, with a default for missing/non-IRI objects.
        LookupRef {
            weights: &'a FxHashMap<u64, u64>,
            default_weight: u64,
            non_iri_weight: u64,
        },
        /// Owned version of [`LookupRef`] (used for 2-hop tail modifiers).
        LookupOwned {
            weights: FxHashMap<u64, u64>,
            default_weight: u64,
            non_iri_weight: u64,
        },
        /// Owned version of [`InSetRef`] (used for 2-hop tail modifiers).
        InSetOwned {
            set: FxHashSet<u64>,
            non_iri_weight: u64,
        },
        /// Owned version of [`NotInSetRef`] (used for 2-hop tail modifiers).
        NotInSetOwned {
            set: FxHashSet<u64>,
            non_iri_weight: u64,
        },
    }

    impl P2WeightMode<'_> {
        #[inline]
        fn weight_row(&self, o_type: u16, o_key: u64) -> u64 {
            let is_iri = o_type == fluree_db_core::o_type::OType::IRI_REF.as_u16();
            match self {
                P2WeightMode::CountEdges { non_iri_weight } => {
                    if is_iri {
                        1
                    } else {
                        *non_iri_weight
                    }
                }
                P2WeightMode::LookupRef {
                    weights,
                    default_weight,
                    non_iri_weight,
                } => {
                    if is_iri {
                        weights.get(&o_key).copied().unwrap_or(*default_weight)
                    } else {
                        *non_iri_weight
                    }
                }
                P2WeightMode::LookupOwned {
                    weights,
                    default_weight,
                    non_iri_weight,
                } => {
                    if is_iri {
                        weights.get(&o_key).copied().unwrap_or(*default_weight)
                    } else {
                        *non_iri_weight
                    }
                }
                P2WeightMode::InSetOwned {
                    set,
                    non_iri_weight,
                } => {
                    if is_iri && set.contains(&o_key) {
                        1
                    } else {
                        *non_iri_weight
                    }
                }
                P2WeightMode::NotInSetOwned {
                    set,
                    non_iri_weight,
                } => {
                    if is_iri {
                        (!set.contains(&o_key)) as u64
                    } else {
                        *non_iri_weight
                    }
                }
            }
        }
    }

    struct PsotSeekSumCursor<'a, 'm> {
        store: &'a BinaryIndexStore,
        p_id: u32,
        leaves: &'a [fluree_db_binary_index::format::branch::LeafEntry],
        leaf_pos: usize,
        leaflet_idx: usize,
        row: usize,
        handle: Option<Box<dyn fluree_db_binary_index::read::leaf_access::LeafHandle>>,
        batch: Option<fluree_db_binary_index::ColumnBatch>,
        mixed: bool,
        /// True when the current leaflet is a pure non-IRI_REF leaflet —
        /// every row gets the `non_iri_weight` without checking `o_key`.
        all_non_iri: bool,
        mode: P2WeightMode<'m>,
    }

    impl<'a, 'm> PsotSeekSumCursor<'a, 'm> {
        fn new(
            store: &'a BinaryIndexStore,
            g_id: GraphId,
            p_id: u32,
            mode: P2WeightMode<'m>,
        ) -> Self {
            let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
            Self {
                store,
                p_id,
                leaves,
                leaf_pos: 0,
                leaflet_idx: 0,
                row: 0,
                handle: None,
                batch: None,
                mixed: false,
                all_non_iri: false,
                mode,
            }
        }

        fn load_next_batch(&mut self, target_b: u64) -> Result<Option<()>> {
            use fluree_db_binary_index::format::run_record_v2::read_ordered_key_v2;
            loop {
                if self.handle.is_none() {
                    if self.leaf_pos >= self.leaves.len() {
                        return Ok(None);
                    }
                    let leaf_entry = &self.leaves[self.leaf_pos];
                    self.leaf_pos += 1;
                    self.leaflet_idx = 0;
                    self.row = 0;
                    self.batch = None;
                    self.handle = Some(
                        self.store
                            .open_leaf_handle(
                                &leaf_entry.leaf_cid,
                                leaf_entry.sidecar_cid.as_ref(),
                                false,
                            )
                            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?,
                    );
                }

                let handle = self.handle.as_ref().unwrap();
                let dir = handle.dir();

                while self.leaflet_idx < dir.entries.len() {
                    let entry = &dir.entries[self.leaflet_idx];
                    let idx = self.leaflet_idx;
                    self.leaflet_idx += 1;
                    if entry.row_count == 0 || entry.p_const != Some(self.p_id) {
                        continue;
                    }

                    // Skip leaflets that cannot contain the target subject by inspecting last key.
                    let last = read_ordered_key_v2(RunSortOrder::Psot, &entry.last_key);
                    let last_b = last.s_id.as_u64();
                    if last_b < target_b {
                        continue;
                    }

                    let mixed = entry.o_type_const.is_none();
                    let iri_only =
                        entry.o_type_const == Some(fluree_db_core::o_type::OType::IRI_REF.as_u16());
                    let non_iri_only = !mixed && !iri_only;

                    // Choose projection based on whether we need per-row o_type.
                    // - mixed: need o_type
                    // - non-IRI homogeneous: we don't need o_type, but we also don't need o_key (kept simple)
                    let projection = if mixed {
                        crate::fast_path_common::projection_sid_otype_okey()
                    } else {
                        crate::fast_path_common::projection_sid_okey()
                    };

                    let batch = if let Some(cache) = self.store.leaflet_cache() {
                        use fluree_db_binary_index::read::column_loader::load_columns_cached_via_handle;
                        let idx_u32: u32 = idx.try_into().map_err(|_| {
                            QueryError::Internal("leaflet idx exceeds u32".to_string())
                        })?;
                        load_columns_cached_via_handle(
                            handle.as_ref(),
                            idx,
                            RunSortOrder::Psot,
                            cache,
                            handle.leaf_id(),
                            idx_u32,
                        )
                        .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?
                    } else {
                        handle
                            .load_columns(idx, &projection, RunSortOrder::Psot)
                            .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?
                    };

                    self.row = 0;
                    self.batch = Some(batch);
                    self.mixed = mixed;
                    self.all_non_iri = non_iri_only;
                    return Ok(Some(()));
                }

                self.handle = None;
            }
        }

        /// Return `Some(sum)` if `target_b` exists, otherwise `None`.
        fn seek_sum_for_subject(&mut self, target_b: u64) -> Result<Option<u64>> {
            let mut found = false;
            let mut sum: u64 = 0;

            loop {
                if self.batch.is_none() && self.load_next_batch(target_b)?.is_none() {
                    return Ok(found.then_some(sum));
                }
                let batch = self.batch.as_ref().unwrap();

                if self.row >= batch.row_count {
                    self.batch = None;
                    continue;
                }

                if !found {
                    // Advance to first row with s_id >= target_b.
                    while self.row < batch.row_count && batch.s_id.get(self.row) < target_b {
                        let cur = batch.s_id.get(self.row);
                        while self.row < batch.row_count && batch.s_id.get(self.row) == cur {
                            self.row += 1;
                        }
                    }
                    if self.row >= batch.row_count {
                        self.batch = None;
                        continue;
                    }
                    let b = batch.s_id.get(self.row);
                    if b > target_b {
                        return Ok(None);
                    }
                    found = true;
                } else if batch.s_id.get(self.row) > target_b {
                    return Ok(Some(sum));
                }

                // We are at `target_b`.
                while self.row < batch.row_count && batch.s_id.get(self.row) == target_b {
                    let (o_type, o_key) = if self.all_non_iri {
                        // Any non-IRI type will do; weight_row will take the non-iri branch.
                        (fluree_db_core::o_type::OType::XSD_STRING.as_u16(), 0)
                    } else if self.mixed {
                        (batch.o_type.get(self.row), batch.o_key.get(self.row))
                    } else {
                        (
                            fluree_db_core::o_type::OType::IRI_REF.as_u16(),
                            batch.o_key.get(self.row),
                        )
                    };

                    let w = self.mode.weight_row(o_type, o_key);
                    if w > 0 {
                        sum = sum.checked_add(w).ok_or_else(|| {
                            QueryError::execution("COUNT(*) overflow in chain join")
                        })?;
                    }
                    self.row += 1;
                }

                if self.row < batch.row_count {
                    return Ok(Some(sum));
                }

                // Subject group may continue in the next leaflet/batch.
                self.batch = None;
            }
        }
    }

    // 1) Build weights for v2 (object of p2), by folding the tail (p3..pN).
    let mut v2_weights: FxHashMap<u64, u64> = FxHashMap::default();
    if n == 2 {
        // No tail fold needed: p2 is the last predicate, and we handle tail_weight directly.
    } else {
        // Step 1: initial weights keyed by v_{N-1} (subjects of pN), with tail modifier.
        match &chain.tail_weight {
            TailWeight::None => {
                let mut iter = PsotSubjectCountIter::new(store, g_id, p_ids[n - 1])?;
                while let Some((s, count)) = iter.next_group()? {
                    if count > 0 {
                        v2_weights.insert(s, count);
                    }
                }
            }
            TailWeight::Optional { tail_pred } => {
                let tail_sid = normalize_pred_sid(store, tail_pred)?;
                if let Some(tail_p_id) = store.sid_to_p_id(&tail_sid) {
                    let mut mult_map: FxHashMap<u64, u64> = FxHashMap::default();
                    let mut iter = PsotSubjectCountIter::new(store, g_id, tail_p_id)?;
                    while let Some((c, count)) = iter.next_group()? {
                        mult_map.insert(c, count.max(1));
                    }
                    let Some(mut ws_iter) =
                        PsotSubjectWeightedSumIter::new(store, g_id, p_ids[n - 1], &mult_map, 1)?
                    else {
                        return Ok(None);
                    };
                    while let Some((s, sum)) = ws_iter.next_group()? {
                        if sum > 0 {
                            v2_weights.insert(s, sum);
                        }
                    }
                } else {
                    let mut iter = PsotSubjectCountIter::new(store, g_id, p_ids[n - 1])?;
                    while let Some((s, count)) = iter.next_group()? {
                        if count > 0 {
                            v2_weights.insert(s, count);
                        }
                    }
                }
            }
            TailWeight::Minus { tail_pred } => {
                let tail_sid = normalize_pred_sid(store, tail_pred)?;
                if let Some(tail_p_id) = store.sid_to_p_id(&tail_sid) {
                    let excluded = collect_subjects_for_predicate_set(store, g_id, tail_p_id)?;
                    let Some(mut iter) = PsotObjectFilterCountIter::new(
                        store,
                        g_id,
                        p_ids[n - 1],
                        &excluded,
                        ObjectFilterMode::NotInSet,
                    )?
                    else {
                        return Ok(None);
                    };
                    while let Some((s, count)) = iter.next_group()? {
                        if count > 0 {
                            v2_weights.insert(s, count);
                        }
                    }
                } else {
                    let mut iter = PsotSubjectCountIter::new(store, g_id, p_ids[n - 1])?;
                    while let Some((s, count)) = iter.next_group()? {
                        if count > 0 {
                            v2_weights.insert(s, count);
                        }
                    }
                }
            }
            TailWeight::Exists { tail_pred } => {
                let tail_sid = normalize_pred_sid(store, tail_pred)?;
                let Some(tail_p_id) = store.sid_to_p_id(&tail_sid) else {
                    return Ok(Some(0));
                };
                let included = collect_subjects_for_predicate_set(store, g_id, tail_p_id)?;
                if included.is_empty() {
                    return Ok(Some(0));
                }
                let Some(mut iter) = PsotObjectFilterCountIter::new(
                    store,
                    g_id,
                    p_ids[n - 1],
                    &included,
                    ObjectFilterMode::InSet,
                )?
                else {
                    return Ok(None);
                };
                while let Some((s, count)) = iter.next_group()? {
                    if count > 0 {
                        v2_weights.insert(s, count);
                    }
                }
            }
        }

        if v2_weights.is_empty() {
            return Ok(Some(0));
        }

        // Fold right-to-left through predicates p_{N-1}..p3, producing weights keyed by v2.
        // (Critically: we stop at i==2; p2 is handled via sparse seeks driven by POST(p1).)
        for i in (2..n - 1).rev() {
            let Some(mut iter) =
                PsotSubjectWeightedSumIter::new(store, g_id, p_ids[i], &v2_weights, 0)?
            else {
                return Ok(None);
            };
            let mut new_weights: FxHashMap<u64, u64> = FxHashMap::default();
            while let Some((b, sum)) = iter.next_group()? {
                if sum > 0 {
                    new_weights.insert(b, sum);
                }
            }
            if new_weights.is_empty() {
                return Ok(Some(0));
            }
            v2_weights = new_weights;
        }
    }

    // 2) Stream POST(p1) grouped by object `b`, and for each `b` seek PSOT(p2) to compute w(b).
    let Some(mut it1) = PostObjectGroupCountIter::new(store, g_id, p_ids[0])? else {
        return Ok(None);
    };

    // Configure how each p2 edge contributes to w(b).
    let p2_mode: P2WeightMode<'_> = if n == 2 {
        match &chain.tail_weight {
            TailWeight::None => P2WeightMode::CountEdges { non_iri_weight: 1 },
            TailWeight::Optional { tail_pred } => {
                let tail_sid = normalize_pred_sid(store, tail_pred)?;
                if let Some(tail_p_id) = store.sid_to_p_id(&tail_sid) {
                    let mut mult_map: FxHashMap<u64, u64> = FxHashMap::default();
                    let mut iter = PsotSubjectCountIter::new(store, g_id, tail_p_id)?;
                    while let Some((c, count)) = iter.next_group()? {
                        mult_map.insert(c, count.max(1));
                    }
                    P2WeightMode::LookupOwned {
                        weights: mult_map,
                        default_weight: 1,
                        non_iri_weight: 1,
                    }
                } else {
                    P2WeightMode::CountEdges { non_iri_weight: 1 }
                }
            }
            TailWeight::Minus { tail_pred } => {
                let tail_sid = normalize_pred_sid(store, tail_pred)?;
                if let Some(tail_p_id) = store.sid_to_p_id(&tail_sid) {
                    let excluded = collect_subjects_for_predicate_set(store, g_id, tail_p_id)?;
                    P2WeightMode::NotInSetOwned {
                        set: excluded,
                        non_iri_weight: 1,
                    }
                } else {
                    P2WeightMode::CountEdges { non_iri_weight: 1 }
                }
            }
            TailWeight::Exists { tail_pred } => {
                let tail_sid = normalize_pred_sid(store, tail_pred)?;
                let Some(tail_p_id) = store.sid_to_p_id(&tail_sid) else {
                    return Ok(Some(0));
                };
                let included = collect_subjects_for_predicate_set(store, g_id, tail_p_id)?;
                if included.is_empty() {
                    return Ok(Some(0));
                }
                P2WeightMode::InSetOwned {
                    set: included,
                    non_iri_weight: 0,
                }
            }
        }
    } else {
        P2WeightMode::LookupRef {
            weights: &v2_weights,
            default_weight: 0,
            non_iri_weight: 0,
        }
    };

    let mut p2_cursor = PsotSeekSumCursor::new(store, g_id, p_ids[1], p2_mode);

    let mut total: u64 = 0;
    while let Some((b, n1)) = it1.next_group()? {
        let Some(w) = p2_cursor.seek_sum_for_subject(b)? else {
            continue;
        };
        if w == 0 {
            continue;
        }
        let add = n1
            .checked_mul(w)
            .ok_or_else(|| QueryError::execution("COUNT(*) overflow in chain join"))?;
        total = total
            .checked_add(add)
            .ok_or_else(|| QueryError::execution("COUNT(*) overflow in chain join"))?;
    }

    Ok(Some(total))
}
