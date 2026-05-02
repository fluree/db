//! Filter bounds pushdown utilities
//!
//! Extracts range constraints from FILTER expressions for pushdown
//! to scan/join operators, enabling index-level filtering.

use crate::ir::{Expression, Pattern};
use crate::planner::extract_object_bounds_for_var;
use crate::sort::compare_flake_values;
use crate::ir::triple::TriplePattern;
use crate::var_registry::VarId;
use fluree_db_core::{FlakeValue, ObjectBounds};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

/// Extract object bounds from a list of filter expressions for pushdown optimization.
///
/// Returns both the bounds map and the indices of filters that were fully consumed by pushdown.
pub fn extract_bounds_from_filters(
    triples: &[TriplePattern],
    filters: &[Expression],
) -> (HashMap<VarId, ObjectBounds>, Vec<usize>) {
    let mut bounds: HashMap<VarId, ObjectBounds> = HashMap::new();
    let mut consumed_indices: Vec<usize> = Vec::new();

    let object_vars: HashSet<VarId> = triples.iter().filter_map(|t| t.o.as_var()).collect();
    if object_vars.is_empty() || filters.is_empty() {
        return (bounds, consumed_indices);
    }

    for (idx, expr) in filters.iter().enumerate() {
        let mut filter_vars_matched = 0;
        for &var in &object_vars {
            if let Some(new_bounds) = extract_object_bounds_for_var(expr, var) {
                filter_vars_matched += 1;
                bounds
                    .entry(var)
                    .and_modify(|existing| *existing = merge_object_bounds(existing, &new_bounds))
                    .or_insert(new_bounds);
            }
        }

        let filter_vars_total = count_filter_vars(expr);
        if filter_vars_matched > 0 && filter_vars_total == filter_vars_matched {
            consumed_indices.push(idx);
        }
    }

    (bounds, consumed_indices)
}

/// Extract object bounds from lookahead FILTER patterns for pushdown optimization
///
/// Examines the patterns following the current triple segment to find FILTER patterns
/// that impose range constraints on object variables. These bounds can be pushed down
/// to the scan operator, NestedLoopJoinOperator, or PropertyJoinOperator for index-level filtering.
///
/// # Arguments
///
/// * `triples` - The triple patterns in this segment (used to identify object variables)
/// * `remaining` - The patterns following this segment (searched for FILTERs)
///
/// Extract object bounds and track which filters were fully consumed by pushdown
///
/// Returns both the bounds map and a set of indices (relative to `remaining`) of
/// FILTER patterns that were fully consumed and should be skipped.
///
/// A filter is considered "fully consumed" if:
/// - It's a range-safe filter (no OR, NOT, complex functions)
/// - All its constraints apply to object variables in the current triple segment
pub fn extract_lookahead_bounds_with_consumption(
    triples: &[TriplePattern],
    remaining: &[Pattern],
) -> (HashMap<VarId, ObjectBounds>, Vec<usize>) {
    let mut bounds: HashMap<VarId, ObjectBounds> = HashMap::new();
    let mut consumed_indices: Vec<usize> = Vec::new();

    // Collect all object variables from all triples in this segment
    let object_vars: HashSet<VarId> = triples.iter().filter_map(|t| t.o.as_var()).collect();

    if object_vars.is_empty() {
        return (bounds, consumed_indices);
    }

    // Process all contiguous FILTER patterns in remaining
    for (idx, pattern) in remaining.iter().enumerate() {
        match pattern {
            Pattern::Filter(expr) => {
                // Check if this filter can be fully consumed
                let mut filter_vars_matched = 0;

                // Try to extract bounds for each object variable
                for &var in &object_vars {
                    if let Some(new_bounds) = extract_object_bounds_for_var(expr, var) {
                        filter_vars_matched += 1;
                        // Merge with existing bounds for this var (intersection)
                        bounds
                            .entry(var)
                            .and_modify(|existing| {
                                *existing = merge_object_bounds(existing, &new_bounds);
                            })
                            .or_insert(new_bounds);
                    }
                }

                // Count how many unique variables the filter references
                let filter_vars_total = count_filter_vars(expr);

                // Filter is fully consumed if:
                // 1. We extracted bounds for at least one variable
                // 2. All filter variables are object variables in our triples
                // (For simple comparisons like ?scoreV > 0.9, this means the filter is fully pushed)
                if filter_vars_matched > 0 && filter_vars_total == filter_vars_matched {
                    consumed_indices.push(idx);
                }
            }
            // Stop at non-FILTER patterns - they might change bindings
            _ => break,
        }
    }

    (bounds, consumed_indices)
}

/// Count unique variables referenced in a filter expression
pub fn count_filter_vars(expr: &Expression) -> usize {
    // Use the existing variables() method on Expression
    let vars: HashSet<VarId> = expr.referenced_vars().into_iter().collect();
    vars.len()
}

/// Merge two ObjectBounds, taking the tighter constraint for each bound
///
/// For lower bounds, takes the higher value (more restrictive).
/// For upper bounds, takes the lower value (more restrictive).
/// When values are equal, takes the exclusive bound (more restrictive).
pub fn merge_object_bounds(a: &ObjectBounds, b: &ObjectBounds) -> ObjectBounds {
    ObjectBounds {
        lower: merge_lower_bound(a.lower.as_ref(), b.lower.as_ref()),
        upper: merge_upper_bound(a.upper.as_ref(), b.upper.as_ref()),
    }
}

/// Merge lower bounds - take the higher value (tighter constraint)
pub fn merge_lower_bound(
    a: Option<&(FlakeValue, bool)>,
    b: Option<&(FlakeValue, bool)>,
) -> Option<(FlakeValue, bool)> {
    match (a, b) {
        (None, None) => None,
        (Some(a), None) => Some(a.clone()),
        (None, Some(b)) => Some(b.clone()),
        (Some((va, inc_a)), Some((vb, inc_b))) => {
            // Compare values - take the higher one (tighter lower bound)
            match compare_flake_values(va, vb) {
                Ordering::Less => Some((vb.clone(), *inc_b)),
                Ordering::Greater => Some((va.clone(), *inc_a)),
                Ordering::Equal => {
                    // Same value - take exclusive if either is exclusive (tighter)
                    Some((va.clone(), *inc_a && *inc_b))
                }
            }
        }
    }
}

/// Merge upper bounds - take the lower value (tighter constraint)
pub fn merge_upper_bound(
    a: Option<&(FlakeValue, bool)>,
    b: Option<&(FlakeValue, bool)>,
) -> Option<(FlakeValue, bool)> {
    match (a, b) {
        (None, None) => None,
        (Some(a), None) => Some(a.clone()),
        (None, Some(b)) => Some(b.clone()),
        (Some((va, inc_a)), Some((vb, inc_b))) => {
            // Compare values - take the lower one (tighter upper bound)
            match compare_flake_values(va, vb) {
                Ordering::Less => Some((va.clone(), *inc_a)),
                Ordering::Greater => Some((vb.clone(), *inc_b)),
                Ordering::Equal => {
                    // Same value - take exclusive if either is exclusive (tighter)
                    Some((va.clone(), *inc_a && *inc_b))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::FilterValue;
    use crate::ir::triple::{Ref, Term};
    use fluree_db_core::Sid;

    use crate::ir::Function;

    fn make_pattern(s_var: VarId, p_name: &str, o_var: VarId) -> TriplePattern {
        TriplePattern::new(
            Ref::Var(s_var),
            Ref::Sid(Sid::new(100, p_name)),
            Term::Var(o_var),
        )
    }

    #[test]
    fn test_extract_lookahead_bounds_simple_range() {
        // Triple pattern: ?s :age ?age
        // Filter: ?age > 18 && ?age < 65
        let triples = vec![make_pattern(VarId(0), "age", VarId(1))];
        let remaining = vec![Pattern::Filter(Expression::and(vec![
            Expression::gt(
                Expression::Var(VarId(1)),
                Expression::Const(FilterValue::Long(18)),
            ),
            Expression::lt(
                Expression::Var(VarId(1)),
                Expression::Const(FilterValue::Long(65)),
            ),
        ]))];

        let (bounds, _consumed) = extract_lookahead_bounds_with_consumption(&triples, &remaining);

        // Should have bounds for VarId(1) (the object variable)
        assert!(bounds.contains_key(&VarId(1)));
        let obj_bounds = bounds.get(&VarId(1)).unwrap();

        // Check lower bound: > 18 (exclusive)
        assert!(obj_bounds.lower.is_some());
        let (lower_val, lower_inclusive) = obj_bounds.lower.as_ref().unwrap();
        assert_eq!(*lower_val, FlakeValue::Long(18));
        assert!(!lower_inclusive); // exclusive (>)

        // Check upper bound: < 65 (exclusive)
        assert!(obj_bounds.upper.is_some());
        let (upper_val, upper_inclusive) = obj_bounds.upper.as_ref().unwrap();
        assert_eq!(*upper_val, FlakeValue::Long(65));
        assert!(!upper_inclusive); // exclusive (<)
    }

    #[test]
    fn test_extract_lookahead_bounds_no_filter() {
        // Triple pattern with no following filter
        let triples = vec![make_pattern(VarId(0), "name", VarId(1))];
        let remaining: Vec<Pattern> = vec![];

        let (bounds, _consumed) = extract_lookahead_bounds_with_consumption(&triples, &remaining);

        // Should have no bounds
        assert!(bounds.is_empty());
    }

    #[test]
    fn test_merge_lower_bound_takes_higher_value() {
        // Merging > 10 and > 20 should result in > 20
        let a = Some((FlakeValue::Long(10), false));
        let b = Some((FlakeValue::Long(20), false));

        let merged = merge_lower_bound(a.as_ref(), b.as_ref());

        assert!(merged.is_some());
        let (val, _inclusive) = merged.unwrap();
        assert_eq!(val, FlakeValue::Long(20));
    }

    #[test]
    fn test_merge_lower_bound_equal_values_exclusive_wins() {
        // Merging >= 10 and > 10 should result in > 10 (exclusive is tighter)
        let a = Some((FlakeValue::Long(10), true)); // >= 10
        let b = Some((FlakeValue::Long(10), false)); // > 10

        let merged = merge_lower_bound(a.as_ref(), b.as_ref());

        assert!(merged.is_some());
        let (val, inclusive) = merged.unwrap();
        assert_eq!(val, FlakeValue::Long(10));
        assert!(
            !inclusive,
            "Should be exclusive when merging inclusive + exclusive"
        );
    }

    #[test]
    fn test_merge_upper_bound_takes_lower_value() {
        // Merging < 100 and < 50 should result in < 50
        let a = Some((FlakeValue::Long(100), false));
        let b = Some((FlakeValue::Long(50), false));

        let merged = merge_upper_bound(a.as_ref(), b.as_ref());

        assert!(merged.is_some());
        let (val, _inclusive) = merged.unwrap();
        assert_eq!(val, FlakeValue::Long(50));
    }

    #[test]
    fn test_merge_object_bounds_full() {
        // Test full merge of ObjectBounds
        let a = ObjectBounds {
            lower: Some((FlakeValue::Long(10), false)),
            upper: Some((FlakeValue::Long(100), true)),
        };
        let b = ObjectBounds {
            lower: Some((FlakeValue::Long(20), true)),
            upper: Some((FlakeValue::Long(80), false)),
        };

        let merged = merge_object_bounds(&a, &b);

        // Lower: max(10, 20) = 20
        let (lower_val, _) = merged.lower.as_ref().unwrap();
        assert_eq!(*lower_val, FlakeValue::Long(20));

        // Upper: min(100, 80) = 80
        let (upper_val, _) = merged.upper.as_ref().unwrap();
        assert_eq!(*upper_val, FlakeValue::Long(80));
    }

    #[test]
    fn test_extract_bounds_sandwich() {
        // Triple: ?s :age ?age
        // Filter: (< 10 ?age 20) — sandwich pattern
        let triples = vec![make_pattern(VarId(0), "age", VarId(1))];
        let filter = Expression::Call {
            func: Function::Lt,
            args: vec![
                Expression::Const(FilterValue::Long(10)),
                Expression::Var(VarId(1)),
                Expression::Const(FilterValue::Long(20)),
            ],
        };

        let (bounds, consumed) = extract_bounds_from_filters(&triples, &[filter]);

        // Should have bounds for VarId(1) (the object variable)
        assert!(bounds.contains_key(&VarId(1)));
        let obj_bounds = bounds.get(&VarId(1)).unwrap();

        // Lower: > 10 (exclusive)
        let (lower_val, lower_inclusive) = obj_bounds.lower.as_ref().expect("should have lower");
        assert_eq!(*lower_val, FlakeValue::Long(10));
        assert!(!lower_inclusive);

        // Upper: < 20 (exclusive)
        let (upper_val, upper_inclusive) = obj_bounds.upper.as_ref().expect("should have upper");
        assert_eq!(*upper_val, FlakeValue::Long(20));
        assert!(!upper_inclusive);

        // Filter should be consumed (single var, fully captured)
        assert_eq!(consumed, vec![0]);
    }
}
