//! MINUS operator - anti-join semantics
//!
//! Implements SPARQL MINUS semantics (set difference):
//! - For each input row, execute the MINUS patterns with empty seed (fresh scope)
//! - If any result matches the input row on shared variables, filter out that input row
//! - Return rows that don't match anything in the MINUS subtree
//!
//! Key semantics:
//! - MINUS executes with **empty bindings** (fresh scope) - does NOT see outer variables
//! - Matching is done on **shared variables only** (vars in both outer and MINUS)
//! - Empty MINUS results do NOT remove input rows (guard: ignore empty solutions)

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::execute::build_where_operators_seeded;
use crate::ir::Pattern;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::seed::EmptyOperator;
use crate::temporal_mode::PlanningContext;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::StatsView;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Hash key for MINUS rows where ALL shared variables are matchable (bound).
///
/// Wraps the ordered shared-var bindings for O(1) hash-probe lookup.
/// Only used for fully-bound minus rows; rows with unbound shared vars go
/// into the wildcard fallback list.
#[derive(Clone)]
struct MinusKey(Vec<Binding>);

impl PartialEq for MinusKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for MinusKey {}

impl Hash for MinusKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for b in &self.0 {
            b.hash(state);
        }
    }
}

/// MINUS operator - anti-join semantics (set difference)
///
/// Executes the MINUS patterns once with an empty seed (fresh scope), materializes
/// all results into a hash set for O(1) lookup, then filters input rows.
///
/// Uses a partitioned approach:
/// - `minus_hash`: HashSet of MinusKey for minus rows where ALL shared vars are
///   matchable — enables O(1) per-input-row lookup in the common case.
/// - `minus_wildcards`: Vec for minus rows with >= 1 unbound shared var (rare,
///   from OPTIONAL inside MINUS) — checked via linear scan.
///
/// Complexity: O(N + M) in the common case (all bound), vs O(N * M) before.
pub struct MinusOperator {
    /// Child operator providing input solutions
    child: BoxedOperator,
    /// MINUS patterns to execute
    minus_patterns: Vec<Pattern>,
    /// Shared variables (appear in both child schema and MINUS patterns)
    shared_vars: Vec<VarId>,
    /// Output schema (same as child)
    schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
    /// Optional stats for nested query optimization (Arc for cheap cloning in nested operators)
    stats: Option<Arc<StatsView>>,
    /// Planning context captured at planner-time. Used when building the
    /// MINUS subtree so it inherits the same temporal mode.
    planning: PlanningContext,
    /// Hash set of minus rows where ALL shared vars are matchable (common case)
    minus_hash: HashSet<MinusKey>,
    /// Minus rows with >= 1 unbound shared var (wildcard rows, rare)
    /// Each entry is a Vec of Option<Binding>: Some(b) for matchable, None for unbound
    minus_wildcards: Vec<Vec<Option<Binding>>>,
}

impl MinusOperator {
    /// Create a new MINUS operator
    ///
    /// # Arguments
    ///
    /// * `child` - Input solutions operator
    /// * `minus_patterns` - Patterns to execute for anti-join matching
    /// * `stats` - Optional stats for nested query optimization (Arc for cheap cloning)
    pub fn new(
        child: BoxedOperator,
        minus_patterns: Vec<Pattern>,
        stats: Option<Arc<StatsView>>,
        planning: PlanningContext,
    ) -> Self {
        let schema: Arc<[VarId]> = Arc::from(child.schema().to_vec().into_boxed_slice());
        let child_vars: HashSet<VarId> = child.schema().iter().copied().collect();

        // Compute variables referenced in MINUS patterns
        let mut minus_vars: HashSet<VarId> = HashSet::new();
        collect_vars_from_patterns(&minus_patterns, &mut minus_vars);

        // Shared vars are the intersection
        let shared_vars: Vec<VarId> = child_vars.intersection(&minus_vars).copied().collect();

        Self {
            child,
            minus_patterns,
            shared_vars,
            schema,
            state: OperatorState::Created,
            stats,
            planning,
            minus_hash: HashSet::new(),
            minus_wildcards: Vec::new(),
        }
    }

    /// Build the hash set and wildcard list from materialized minus batches.
    fn build_hash_index(&mut self, batches: Vec<Batch>) {
        for batch in &batches {
            for row_idx in 0..batch.len() {
                let mut key_bindings = Vec::with_capacity(self.shared_vars.len());
                let mut has_wildcard = false;

                for &var in &self.shared_vars {
                    let binding = batch.column(var).map(|col| &col[row_idx]);
                    match binding {
                        Some(b) if b.is_matchable() => {
                            key_bindings.push(Some(b.clone()));
                        }
                        _ => {
                            key_bindings.push(None);
                            has_wildcard = true;
                        }
                    }
                }

                if has_wildcard {
                    self.minus_wildcards.push(key_bindings);
                } else {
                    // All shared vars are matchable — unwrap the Options into a MinusKey
                    let key = MinusKey(
                        key_bindings
                            .into_iter()
                            .map(|opt| opt.expect("checked: no wildcard"))
                            .collect(),
                    );
                    self.minus_hash.insert(key);
                }
            }
        }
    }

    /// Check if an input row is eliminated by the MINUS.
    ///
    /// Per W3C SPARQL §8.3, MINUS removes an input row µ when there exists a
    /// MINUS row µ' such that:
    ///   1. µ and µ' are **compatible**: for every variable in dom(µ) ∩ dom(µ'),
    ///      µ(v) = µ'(v).
    ///   2. dom(µ) ∩ dom(µ') ≠ ∅: at least one shared variable is bound in both.
    ///
    /// Uses hash probe for the common case (all shared vars matchable on both sides),
    /// with linear scan fallback for wildcard rows.
    fn input_row_eliminated(&self, input_batch: &Batch, row_idx: usize) -> bool {
        // Extract input shared-var bindings
        let mut input_bindings = Vec::with_capacity(self.shared_vars.len());
        let mut input_has_wildcard = false;

        for &var in &self.shared_vars {
            let binding = input_batch.column(var).map(|col| &col[row_idx]);
            match binding {
                Some(b) if b.is_matchable() => {
                    input_bindings.push(Some(b.clone()));
                }
                _ => {
                    input_bindings.push(None);
                    input_has_wildcard = true;
                }
            }
        }

        if !input_has_wildcard {
            // Common case: all input shared vars are matchable.
            // Hash probe against minus_hash — O(1).
            let probe = MinusKey(
                input_bindings
                    .iter()
                    .map(|opt| opt.clone().expect("checked: no wildcard"))
                    .collect(),
            );
            if self.minus_hash.contains(&probe) {
                return true;
            }

            // Check wildcard minus rows — O(W), W typically 0.
            // A wildcard minus row matches if every matchable position equals the input.
            for wc_row in &self.minus_wildcards {
                if wildcard_matches(&input_bindings, wc_row) {
                    return true;
                }
            }
        } else {
            // Rare: input has unbound shared var(s).
            // Must linear-scan both sets for partial matching.

            // Check minus_hash entries
            for entry in &self.minus_hash {
                if matches_partial(&input_bindings, &entry.0) {
                    return true;
                }
            }

            // Check wildcard minus rows
            for wc_row in &self.minus_wildcards {
                if wildcard_matches(&input_bindings, wc_row) {
                    return true;
                }
            }
        }

        false
    }
}

/// Check if an input row matches a wildcard minus row.
///
/// Both vecs are ordered by shared_vars. `minus_row` has `None` for unbound positions.
/// Match fires if: compatible on all positions AND at least one position is bound on both sides.
fn wildcard_matches(input: &[Option<Binding>], minus_row: &[Option<Binding>]) -> bool {
    let mut has_shared_bound = false;

    for (i_opt, m_opt) in input.iter().zip(minus_row.iter()) {
        if let (Some(i), Some(m)) = (i_opt, m_opt) {
            // Both bound: must be equal
            if i != m {
                return false;
            }
            has_shared_bound = true;
        }
        // One or both unbound: trivially compatible, doesn't count as shared bound
    }

    has_shared_bound
}

/// Check if a partially-bound input matches a fully-bound minus entry.
///
/// `input` has `None` for unbound positions, `minus_bindings` are all matchable.
fn matches_partial(input: &[Option<Binding>], minus_bindings: &[Binding]) -> bool {
    let mut has_shared_bound = false;

    for (i_opt, m) in input.iter().zip(minus_bindings.iter()) {
        match i_opt {
            Some(i) => {
                if i != m {
                    return false;
                }
                has_shared_bound = true;
            }
            None => {
                // Input unbound: trivially compatible
            }
        }
    }

    has_shared_bound
}

/// Check if two rows match on shared variables (free function for testing).
///
/// Preserves the original `rows_match` semantics for unit tests.
#[cfg(test)]
fn rows_match(
    shared_vars: &[VarId],
    input_batch: &Batch,
    input_row_idx: usize,
    minus_batch: &Batch,
    minus_row_idx: usize,
) -> bool {
    let mut has_shared_bound = false;

    let compatible = shared_vars.iter().all(|&var| {
        let input_binding = input_batch.column(var).map(|col| &col[input_row_idx]);
        let minus_binding = minus_batch.column(var).map(|col| &col[minus_row_idx]);

        match (input_binding, minus_binding) {
            (Some(i), Some(m)) if i.is_matchable() && m.is_matchable() => {
                has_shared_bound = true;
                i == m
            }
            _ => true,
        }
    });

    compatible && has_shared_bound
}

#[async_trait]
impl Operator for MinusOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        // Materialize the MINUS subtree once with an empty seed (fresh scope).
        // MINUS is always uncorrelated — the subtree doesn't see outer variables.
        if !self.shared_vars.is_empty() {
            #[expect(clippy::box_default)]
            let seed: BoxedOperator = Box::new(EmptyOperator::new());
            let mut minus_op = build_where_operators_seeded(
                Some(seed),
                &self.minus_patterns,
                self.stats.clone(),
                None,
                &self.planning,
            )?;

            minus_op.open(ctx).await?;

            let mut batches = Vec::new();
            while let Some(batch) = minus_op.next_batch(ctx).await? {
                if !batch.is_empty() {
                    batches.push(batch);
                }
            }

            minus_op.close();

            // Build hash index from materialized batches
            self.build_hash_index(batches);
        }

        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        loop {
            // Get next batch from child
            let input_batch = match self.child.next_batch(ctx).await? {
                Some(b) if !b.is_empty() => b,
                Some(_) => continue, // Skip empty batches
                None => {
                    self.state = OperatorState::Exhausted;
                    return Ok(None);
                }
            };

            // If no shared variables, MINUS can't match anything - return input unchanged
            if self.shared_vars.is_empty() {
                return Ok(Some(input_batch));
            }

            // If MINUS subtree produced no results, nothing can be removed
            if self.minus_hash.is_empty() && self.minus_wildcards.is_empty() {
                return Ok(Some(input_batch));
            }

            // For each input row, hash-probe against materialized MINUS results
            let mut keep_rows: Vec<bool> = vec![true; input_batch.len()];

            for (row_idx, keep) in keep_rows.iter_mut().enumerate() {
                if self.input_row_eliminated(&input_batch, row_idx) {
                    *keep = false;
                }
            }

            // Build output batch with only kept rows
            let kept_count = keep_rows.iter().filter(|&&k| k).count();
            if kept_count == 0 {
                // All rows filtered out, try next input batch
                continue;
            }

            if kept_count == input_batch.len() {
                // All rows kept, return unchanged
                return Ok(Some(input_batch));
            }

            // Build filtered batch
            let mut columns: Vec<Vec<Binding>> = (0..self.schema.len())
                .map(|_| Vec::with_capacity(kept_count))
                .collect();

            for (row_idx, keep) in keep_rows.iter().enumerate() {
                if *keep {
                    for (col, var) in columns.iter_mut().zip(self.schema.iter()) {
                        if let Some(input_col) = input_batch.column(*var) {
                            col.push(input_col[row_idx].clone());
                        }
                    }
                }
            }

            return Ok(Some(Batch::new(self.schema.clone(), columns)?));
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Could be 0 to child rows, return child estimate as upper bound
        self.child.estimated_rows()
    }

    async fn drain_count(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<u64>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }
        let mut count: u64 = 0;
        loop {
            match self.child.next_batch(ctx).await? {
                Some(batch) if !batch.is_empty() => {
                    if self.shared_vars.is_empty()
                        || (self.minus_hash.is_empty() && self.minus_wildcards.is_empty())
                    {
                        // No shared vars or empty MINUS: all rows survive.
                        count = count.checked_add(batch.len() as u64).ok_or_else(|| {
                            QueryError::execution("COUNT(*) overflow in MINUS drain_count")
                        })?;
                    } else {
                        for row_idx in 0..batch.len() {
                            if !self.input_row_eliminated(&batch, row_idx) {
                                count = count.checked_add(1).ok_or_else(|| {
                                    QueryError::execution("COUNT(*) overflow in MINUS drain_count")
                                })?;
                            }
                        }
                    }
                }
                Some(_) => continue,
                None => break,
            }
        }
        self.state = OperatorState::Exhausted;
        Ok(Some(count))
    }
}

/// Collect all variables referenced in a list of patterns
fn collect_vars_from_patterns(patterns: &[Pattern], vars: &mut HashSet<VarId>) {
    for p in patterns {
        for v in p.variables() {
            vars.insert(v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::triple::{Ref, Term, TriplePattern};
    use fluree_db_core::Sid;

    #[test]
    fn test_shared_vars_computation() {
        // Create a child with schema [?s, ?name]
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let child: BoxedOperator = Box::new(TestEmptyWithSchema {
            schema: child_schema,
        });

        // MINUS pattern references ?s and ?age
        let minus_patterns = vec![Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)), // ?s - shared
            Ref::Sid(Sid::new(100, "age")),
            Term::Var(VarId(2)), // ?age - not shared
        ))];

        let op = MinusOperator::new(
            child,
            minus_patterns,
            None,
            crate::temporal_mode::PlanningContext::current(),
        );

        // Only ?s should be shared
        assert_eq!(op.shared_vars.len(), 1);
        assert!(op.shared_vars.contains(&VarId(0)));
    }

    #[test]
    fn test_minus_schema_preserved() {
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let child: BoxedOperator = Box::new(TestEmptyWithSchema {
            schema: child_schema.clone(),
        });

        let op = MinusOperator::new(
            child,
            vec![],
            None,
            crate::temporal_mode::PlanningContext::current(),
        );

        // Output schema should match child schema
        assert_eq!(op.schema(), &*child_schema);
    }

    // Helper struct for testing
    struct TestEmptyWithSchema {
        schema: Arc<[VarId]>,
    }

    #[async_trait]
    impl Operator for TestEmptyWithSchema {
        fn schema(&self) -> &[VarId] {
            &self.schema
        }

        async fn open(&mut self, _ctx: &ExecutionContext<'_>) -> Result<()> {
            Ok(())
        }

        async fn next_batch(&mut self, _ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
            Ok(None)
        }

        fn close(&mut self) {}
    }

    /// Helper: build a MinusOperator with a given child schema and shared vars
    fn make_minus_with_shared(shared: Vec<VarId>) -> MinusOperator {
        let child_schema: Arc<[VarId]> = Arc::from(shared.clone().into_boxed_slice());
        let child: BoxedOperator = Box::new(TestEmptyWithSchema {
            schema: child_schema.clone(),
        });
        MinusOperator {
            child,
            minus_patterns: vec![],
            shared_vars: shared,
            schema: child_schema,
            state: OperatorState::Created,
            stats: None,
            planning: crate::temporal_mode::PlanningContext::current(),
            minus_hash: HashSet::new(),
            minus_wildcards: Vec::new(),
        }
    }

    /// Helper: build a 1-row Batch with given bindings
    fn batch_1row(schema: &[VarId], bindings: Vec<Binding>) -> Batch {
        let arc_schema: Arc<[VarId]> = Arc::from(schema.to_vec().into_boxed_slice());
        let columns: Vec<Vec<Binding>> = bindings.into_iter().map(|b| vec![b]).collect();
        Batch::new(arc_schema, columns).unwrap()
    }

    #[test]
    fn rows_match_both_bound_equal() {
        let shared = vec![VarId(0)];
        let sid = Sid::new(100, "x");
        let input = batch_1row(&[VarId(0)], vec![Binding::sid(sid.clone())]);
        let minus = batch_1row(&[VarId(0)], vec![Binding::sid(sid)]);
        assert!(rows_match(&shared, &input, 0, &minus, 0));
    }

    #[test]
    fn rows_match_both_bound_unequal() {
        let shared = vec![VarId(0)];
        let input = batch_1row(&[VarId(0)], vec![Binding::sid(Sid::new(100, "x"))]);
        let minus = batch_1row(&[VarId(0)], vec![Binding::sid(Sid::new(200, "y"))]);
        assert!(!rows_match(&shared, &input, 0, &minus, 0));
    }

    #[test]
    fn rows_match_input_unbound_trivially_compatible() {
        // Input has Unbound, MINUS has a value — trivially compatible
        // but no shared bound variables → match should NOT fire
        let shared = vec![VarId(0)];
        let input = batch_1row(&[VarId(0)], vec![Binding::Unbound]);
        let minus = batch_1row(&[VarId(0)], vec![Binding::sid(Sid::new(100, "x"))]);
        assert!(
            !rows_match(&shared, &input, 0, &minus, 0),
            "no shared bound var → match must not fire"
        );
    }

    #[test]
    fn rows_match_minus_unbound_trivially_compatible() {
        // MINUS has Unbound, input has a value — trivially compatible
        // but no shared bound variables → match should NOT fire
        let shared = vec![VarId(0)];
        let input = batch_1row(&[VarId(0)], vec![Binding::sid(Sid::new(100, "x"))]);
        let minus = batch_1row(&[VarId(0)], vec![Binding::Unbound]);
        assert!(
            !rows_match(&shared, &input, 0, &minus, 0),
            "no shared bound var → match must not fire"
        );
    }

    #[test]
    fn rows_match_both_unbound() {
        let shared = vec![VarId(0)];
        let input = batch_1row(&[VarId(0)], vec![Binding::Unbound]);
        let minus = batch_1row(&[VarId(0)], vec![Binding::Unbound]);
        assert!(
            !rows_match(&shared, &input, 0, &minus, 0),
            "both unbound → no shared bound var → no match"
        );
    }

    #[test]
    fn rows_match_poisoned_trivially_compatible() {
        // Poisoned (from failed OPTIONAL) is not in domain
        let shared = vec![VarId(0)];
        let input = batch_1row(&[VarId(0)], vec![Binding::Poisoned]);
        let minus = batch_1row(&[VarId(0)], vec![Binding::sid(Sid::new(100, "x"))]);
        assert!(
            !rows_match(&shared, &input, 0, &minus, 0),
            "poisoned is not matchable → no shared bound var"
        );
    }

    #[test]
    fn rows_match_multi_var_one_unbound_one_equal() {
        // Two shared vars: var0 is bound+equal, var1 has input Unbound
        // Compatible (unbound is trivially ok) AND has shared bound (var0)
        let shared = vec![VarId(0), VarId(1)];
        let sid = Sid::new(100, "x");
        let input = batch_1row(
            &[VarId(0), VarId(1)],
            vec![Binding::sid(sid.clone()), Binding::Unbound],
        );
        let minus = batch_1row(
            &[VarId(0), VarId(1)],
            vec![Binding::sid(sid), Binding::sid(Sid::new(200, "y"))],
        );
        assert!(
            rows_match(&shared, &input, 0, &minus, 0),
            "var0 is shared+equal, var1 unbound in input → compatible + has_shared_bound"
        );
    }

    #[test]
    fn rows_match_multi_var_one_equal_one_unequal() {
        // Two shared vars: var0 bound+equal, var1 bound+unequal → NOT compatible
        let shared = vec![VarId(0), VarId(1)];
        let sid = Sid::new(100, "x");
        let input = batch_1row(
            &[VarId(0), VarId(1)],
            vec![Binding::sid(sid.clone()), Binding::sid(Sid::new(300, "a"))],
        );
        let minus = batch_1row(
            &[VarId(0), VarId(1)],
            vec![Binding::sid(sid), Binding::sid(Sid::new(400, "b"))],
        );
        assert!(
            !rows_match(&shared, &input, 0, &minus, 0),
            "var1 disagrees → incompatible"
        );
    }

    // === Hash-probe specific tests ===

    #[test]
    fn hash_index_fully_bound_rows_go_to_hash() {
        let mut op = make_minus_with_shared(vec![VarId(0), VarId(1)]);
        let batch = batch_1row(
            &[VarId(0), VarId(1)],
            vec![
                Binding::sid(Sid::new(100, "x")),
                Binding::sid(Sid::new(200, "y")),
            ],
        );
        op.build_hash_index(vec![batch]);
        assert_eq!(op.minus_hash.len(), 1);
        assert!(op.minus_wildcards.is_empty());
    }

    #[test]
    fn hash_index_unbound_rows_go_to_wildcards() {
        let mut op = make_minus_with_shared(vec![VarId(0), VarId(1)]);
        let batch = batch_1row(
            &[VarId(0), VarId(1)],
            vec![Binding::sid(Sid::new(100, "x")), Binding::Unbound],
        );
        op.build_hash_index(vec![batch]);
        assert!(op.minus_hash.is_empty());
        assert_eq!(op.minus_wildcards.len(), 1);
    }

    #[test]
    fn input_row_eliminated_hash_hit() {
        let mut op = make_minus_with_shared(vec![VarId(0)]);
        let sid = Sid::new(100, "x");
        let minus_batch = batch_1row(&[VarId(0)], vec![Binding::sid(sid.clone())]);
        op.build_hash_index(vec![minus_batch]);

        let input = batch_1row(&[VarId(0)], vec![Binding::sid(sid)]);
        assert!(op.input_row_eliminated(&input, 0));
    }

    #[test]
    fn input_row_eliminated_hash_miss() {
        let mut op = make_minus_with_shared(vec![VarId(0)]);
        let minus_batch = batch_1row(&[VarId(0)], vec![Binding::sid(Sid::new(100, "x"))]);
        op.build_hash_index(vec![minus_batch]);

        let input = batch_1row(&[VarId(0)], vec![Binding::sid(Sid::new(200, "y"))]);
        assert!(!op.input_row_eliminated(&input, 0));
    }

    #[test]
    fn input_row_eliminated_wildcard_minus_match() {
        // Minus row has unbound var1, input is fully bound.
        // Shared vars: [var0, var1]. Minus has var0=x, var1=unbound.
        // Input has var0=x, var1=anything → should match (var0 equal, var1 wildcard).
        let mut op = make_minus_with_shared(vec![VarId(0), VarId(1)]);
        let sid = Sid::new(100, "x");
        let minus_batch = batch_1row(
            &[VarId(0), VarId(1)],
            vec![Binding::sid(sid.clone()), Binding::Unbound],
        );
        op.build_hash_index(vec![minus_batch]);

        let input = batch_1row(
            &[VarId(0), VarId(1)],
            vec![Binding::sid(sid), Binding::sid(Sid::new(200, "y"))],
        );
        assert!(
            op.input_row_eliminated(&input, 0),
            "wildcard minus row should match fully-bound input"
        );
    }

    #[test]
    fn input_row_eliminated_input_unbound_vs_hash() {
        // Input has unbound var, minus is fully bound.
        // var0: input=x, minus=x (match). var1: input=unbound → compatible.
        // Has shared bound (var0) → eliminated.
        let mut op = make_minus_with_shared(vec![VarId(0), VarId(1)]);
        let sid = Sid::new(100, "x");
        let minus_batch = batch_1row(
            &[VarId(0), VarId(1)],
            vec![Binding::sid(sid.clone()), Binding::sid(Sid::new(200, "y"))],
        );
        op.build_hash_index(vec![minus_batch]);

        let input = batch_1row(
            &[VarId(0), VarId(1)],
            vec![Binding::sid(sid), Binding::Unbound],
        );
        assert!(
            op.input_row_eliminated(&input, 0),
            "input unbound var → partial match should work"
        );
    }

    #[test]
    fn input_row_eliminated_all_unbound_no_match() {
        // Both sides all unbound → no shared bound var → no elimination.
        let mut op = make_minus_with_shared(vec![VarId(0)]);
        let minus_batch = batch_1row(&[VarId(0)], vec![Binding::Unbound]);
        op.build_hash_index(vec![minus_batch]);

        let input = batch_1row(&[VarId(0)], vec![Binding::Unbound]);
        assert!(
            !op.input_row_eliminated(&input, 0),
            "both unbound → no shared bound var → no elimination"
        );
    }
}
