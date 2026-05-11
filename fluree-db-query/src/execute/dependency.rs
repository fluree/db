//! Variable dependency tracking for projection pushdown.
//!
//! Computes which variables the query output depends on by working backward
//! from the final output through ORDER BY, post-binds, HAVING, aggregates,
//! and GROUP BY. Variables without downstream dependencies are dead and can
//! be projected away early.

use crate::ir::{Grouping, Query};
use crate::var_registry::VarId;
use std::collections::HashSet;

/// Per-operator required variable sets.
///
/// Each field holds the variables that the operator's output must contain
/// for all downstream consumers to function correctly.  The sets are
/// computed once (backward from SELECT) and consulted by each operator to
/// trim dead columns from its output.
#[derive(Debug)]
pub struct VariableDeps {
    pub required_where_vars: Vec<VarId>,
    pub required_groupby_vars: Vec<VarId>,
    pub required_aggregate_vars: Vec<VarId>,
    pub required_having_vars: Vec<VarId>,
    pub required_bind_vars: Vec<Vec<VarId>>,
    pub required_sort_vars: Vec<VarId>,
}

/// Compute per-operator downstream dependency sets.
///
/// Works backward from SELECT through ORDER BY, post-binds, HAVING,
/// aggregates, and GROUP BY, recording the dependency set at each stage
/// boundary.
///
/// Returns `None` when trimming is not applicable:
/// - `Wildcard` / `Boolean` select mode (all WHERE vars are needed)
/// - Empty select list (no explicit projection)
/// - `Construct` without a template
pub fn compute_variable_deps(query: &Query) -> Option<VariableDeps> {
    // ---- backward walk ----

    // Seed deps from the query output requirements.
    // Wildcard/Boolean return None from `variables()`, disabling trimming.
    // Selection::Hydration contributes its root variable via bound_var.
    let mut deps: HashSet<VarId> = query.output.referenced_vars()?;

    // ORDER BY vars must survive to the sort operator.
    for spec in &query.ordering {
        deps.insert(spec.var);
    }
    let required_sort_vars: Vec<VarId> = deps.iter().copied().collect();

    // Post-aggregation binds (reverse order): trace expression inputs.
    // Record deps BEFORE processing each bind backward, since that
    // represents what the bind's output must contain for downstream.
    let binds = query
        .grouping
        .as_ref()
        .and_then(Grouping::aggregation)
        .map(|agg| agg.binds.as_slice())
        .unwrap_or(&[]);
    let mut required_bind_vars: Vec<Vec<VarId>> = Vec::with_capacity(binds.len());
    for (var, expr) in binds.iter().rev() {
        // Record what this bind's output must contain.
        required_bind_vars.push(deps.iter().copied().collect());
        // Then trace backward through the bind expression.
        if deps.remove(var) {
            deps.extend(expr.referenced_vars());
        }
    }
    // Reverse so indices match the forward (execution) order of binds.
    required_bind_vars.reverse();

    // Record what HAVING's output must contain (before tracing HAVING backward).
    let required_having_vars: Vec<VarId> = deps.iter().copied().collect();

    // HAVING expression variables: needed in HAVING's input but not
    // necessarily in its output (HAVING evaluates before trimming).
    if let Some(having_expr) = query.grouping.as_ref().and_then(Grouping::having) {
        deps.extend(having_expr.referenced_vars());
    }

    // Record what Aggregate's output must contain (before tracing aggregates backward).
    let required_aggregate_vars: Vec<VarId> = deps.iter().copied().collect();

    // Aggregates: replace output vars with input vars.
    for spec in query.grouping.iter().flat_map(Grouping::aggregates) {
        if deps.remove(&spec.output_var) {
            if let Some(input_var) = spec.input_var {
                deps.insert(input_var);
            }
        }
    }

    // Record what GROUP BY's output must contain (before tracing GROUP BY backward).
    let required_groupby_vars: Vec<VarId> = deps.iter().copied().collect();

    // GROUP BY keys must survive.
    if let Some(Grouping::Explicit { group_by, .. }) = &query.grouping {
        deps.extend(group_by.iter().copied());
    }

    // deps now contains the full set of WHERE-produced variables needed downstream.
    let required_where_vars: Vec<VarId> = deps.iter().copied().collect();

    Some(VariableDeps {
        required_where_vars,
        required_groupby_vars,
        required_aggregate_vars,
        required_having_vars,
        required_bind_vars,
        required_sort_vars,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::triple::{Ref, Term, TriplePattern};
    use crate::ir::{
        AggregateFn, AggregateSpec, Aggregation, ConstructTemplate, Expression, FlakeValue,
        Pattern, Query, QueryOutput, ReasoningConfig,
    };
    use crate::parse::SelectMode;
    use crate::sort::SortSpec;
    use fluree_db_core::Sid;
    use fluree_graph_json_ld::ParsedContext;
    use std::collections::HashMap;

    fn make_query(select: Vec<VarId>, patterns: Vec<Pattern>, select_mode: SelectMode) -> Query {
        let output = match select_mode {
            SelectMode::Many => QueryOutput::select_all(select),
            SelectMode::One => QueryOutput::select_one(select),
            SelectMode::Construct => QueryOutput::Construct(ConstructTemplate::new(Vec::new())),
            SelectMode::Ask => QueryOutput::Ask,
        };
        Query {
            context: ParsedContext::default(),
            orig_context: None,
            output,
            patterns,
            reasoning: ReasoningConfig::default(),
            grouping: None,
            ordering: Vec::new(),
            limit: None,
            offset: None,
            post_values: None,
        }
    }

    fn make_wildcard_query(patterns: Vec<Pattern>) -> Query {
        Query {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::wildcard(),
            patterns,
            reasoning: ReasoningConfig::default(),
            grouping: None,
            ordering: Vec::new(),
            limit: None,
            offset: None,
            post_values: None,
        }
    }

    fn make_tp(s: VarId, p: &str, o: VarId) -> TriplePattern {
        TriplePattern::new(Ref::Var(s), Ref::Sid(Sid::new(100, p)), Term::Var(o))
    }

    #[test]
    fn none_for_wildcard() {
        let query = make_wildcard_query(vec![]);
        assert!(compute_variable_deps(&query).is_none());
    }

    #[test]
    fn none_for_boolean() {
        let query = make_query(vec![], vec![], SelectMode::Ask);
        assert!(compute_variable_deps(&query).is_none());
    }

    #[test]
    fn none_for_construct_without_template() {
        let query = make_query(vec![], vec![], SelectMode::Construct);
        assert!(compute_variable_deps(&query).is_none());
    }

    #[test]
    fn none_for_empty_select() {
        let query = make_query(vec![], vec![], SelectMode::Many);
        assert!(compute_variable_deps(&query).is_none());
    }

    #[test]
    fn simple_select_where_vars() {
        let query = make_query(vec![VarId(1), VarId(2)], vec![], SelectMode::Many);
        let deps = compute_variable_deps(&query).unwrap();
        let where_set: HashSet<VarId> = deps.required_where_vars.into_iter().collect();
        assert_eq!(where_set, HashSet::from([VarId(1), VarId(2)]));
    }

    #[test]
    fn order_by_adds_where_vars() {
        let mut query = make_query(vec![VarId(1)], vec![], SelectMode::Many);
        query.ordering = vec![SortSpec::asc(VarId(3))];
        let deps = compute_variable_deps(&query).unwrap();
        assert!(deps.required_where_vars.contains(&VarId(1)));
        assert!(deps.required_where_vars.contains(&VarId(3)));
    }

    #[test]
    fn aggregate_replaces_output_with_input_in_where_vars() {
        // SELECT ?city (AVG(?age) AS ?avg) ... GROUP BY ?city
        let mut query = make_query(vec![VarId(2), VarId(3)], vec![], SelectMode::Many);
        query.grouping = Some(Grouping::Explicit {
            group_by: fluree_db_core::NonEmpty::try_from_vec(vec![VarId(2)]).unwrap(),
            aggregation: Some(Aggregation {
                aggregates: fluree_db_core::NonEmpty::try_from_vec(vec![AggregateSpec {
                    function: AggregateFn::Avg,
                    input_var: Some(VarId(1)),
                    output_var: VarId(3),
                    distinct: false,
                }])
                .unwrap(),
                binds: Vec::new(),
            }),
            having: None,
        });

        let deps = compute_variable_deps(&query).unwrap();
        // ?city (group key) and ?age (aggregate input) are WHERE dependencies
        assert!(deps.required_where_vars.contains(&VarId(2)));
        assert!(deps.required_where_vars.contains(&VarId(1)));
        // ?avg (aggregate output) is NOT a WHERE dependency
        assert!(!deps.required_where_vars.contains(&VarId(3)));
    }

    #[test]
    fn post_bind_traces_where_vars() {
        // SELECT ?x (AVG(?n) AS ?avg) (CEIL(?avg) AS ?ceil)
        // GROUP BY ?x — the post-bind ?ceil = CEIL(?avg) references the
        // aggregate output ?avg, which traces back to the WHERE var ?n.
        let mut query = make_query(vec![VarId(0), VarId(3)], vec![], SelectMode::Many);
        query.grouping = Some(Grouping::Explicit {
            group_by: fluree_db_core::NonEmpty::try_from_vec(vec![VarId(0)]).unwrap(),
            aggregation: Some(Aggregation {
                aggregates: fluree_db_core::NonEmpty::try_from_vec(vec![AggregateSpec {
                    function: AggregateFn::Avg,
                    input_var: Some(VarId(1)),
                    output_var: VarId(2),
                    distinct: false,
                }])
                .unwrap(),
                binds: vec![(
                    VarId(3),
                    Expression::Call {
                        func: crate::ir::Function::Ceil,
                        args: vec![Expression::Var(VarId(2))],
                    },
                )],
            }),
            having: None,
        });

        let deps = compute_variable_deps(&query).unwrap();
        // ?x (group key) and ?n (aggregate input traced from ?avg) are WHERE deps.
        assert!(deps.required_where_vars.contains(&VarId(0)));
        assert!(deps.required_where_vars.contains(&VarId(1)));
        // ?ceil (post-bind output) is not a WHERE dep — it's computed.
        assert!(!deps.required_where_vars.contains(&VarId(3)));
    }

    #[test]
    fn having_adds_where_vars() {
        let mut query = make_query(vec![VarId(0)], vec![], SelectMode::Many);
        query.grouping = Some(Grouping::Explicit {
            group_by: fluree_db_core::NonEmpty::try_from_vec(vec![VarId(0)]).unwrap(),
            aggregation: None,
            having: Some(Expression::gt(
                Expression::Var(VarId(1)),
                Expression::Const(FlakeValue::Long(10)),
            )),
        });

        let deps = compute_variable_deps(&query).unwrap();
        assert!(deps.required_where_vars.contains(&VarId(0)));
        assert!(deps.required_where_vars.contains(&VarId(1)));
    }

    #[test]
    fn construct_uses_template_vars() {
        let query = Query {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::Construct(ConstructTemplate::new(vec![make_tp(
                VarId(0),
                "name",
                VarId(1),
            )])),
            patterns: vec![],
            reasoning: ReasoningConfig::default(),
            grouping: None,
            ordering: Vec::new(),
            limit: None,
            offset: None,
            post_values: None,
        };

        let deps = compute_variable_deps(&query).unwrap();
        assert!(deps.required_where_vars.contains(&VarId(0)));
        assert!(deps.required_where_vars.contains(&VarId(1)));
    }

    // ---- hydration tests ----

    fn make_query_with_selections(columns: Vec<crate::ir::Column>) -> Query {
        Query {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::Select {
                projection: crate::ir::Projection::Tuple(columns),
                restriction: None,
            },
            patterns: vec![],
            reasoning: ReasoningConfig::default(),
            grouping: None,
            ordering: Vec::new(),
            limit: None,
            offset: None,
            post_values: None,
        }
    }

    #[test]
    fn hydration_adds_root_var() {
        // SELECT ?name + hydration rooted at ?s
        // The formatter needs both ?name (var selection) and ?s (root var).
        let query = make_query_with_selections(vec![
            crate::ir::Column::Var(VarId(1)),
            crate::ir::Column::Hydration(crate::ir::HydrationSpec::new(
                crate::ir::Root::Var(VarId(0)),
                crate::ir::NestedSelectSpec::Explicit {
                    forward: vec![],
                    reverse: HashMap::new(),
                },
            )),
        ]);

        let deps = compute_variable_deps(&query).unwrap();
        assert!(deps.required_where_vars.contains(&VarId(0))); // root var
        assert!(deps.required_where_vars.contains(&VarId(1))); // var selection
    }

    #[test]
    fn hydration_root_already_in_select() {
        // Var selection ?s + hydration rooted at ?s — only ?s needed.
        let query = make_query_with_selections(vec![
            crate::ir::Column::Var(VarId(0)),
            crate::ir::Column::Hydration(crate::ir::HydrationSpec::new(
                crate::ir::Root::Var(VarId(0)),
                crate::ir::NestedSelectSpec::Explicit {
                    forward: vec![],
                    reverse: HashMap::new(),
                },
            )),
        ]);

        let deps = compute_variable_deps(&query).unwrap();
        assert!(deps.required_where_vars.contains(&VarId(0)));
        assert_eq!(deps.required_where_vars.len(), 1);
    }

    #[test]
    fn hydration_sid_root_no_extra_vars() {
        // SELECT ?name + hydration rooted at an IRI constant.
        // Sid root binds no variable — only ?name needed.
        let query = make_query_with_selections(vec![
            crate::ir::Column::Var(VarId(1)),
            crate::ir::Column::Hydration(crate::ir::HydrationSpec::new(
                crate::ir::Root::Sid(Sid::new(100, "alice")),
                crate::ir::NestedSelectSpec::Explicit {
                    forward: vec![],
                    reverse: HashMap::new(),
                },
            )),
        ]);

        let deps = compute_variable_deps(&query).unwrap();
        assert!(deps.required_where_vars.contains(&VarId(1)));
        assert_eq!(deps.required_where_vars.len(), 1);
    }

    #[test]
    fn hydration_two_roots_both_added() {
        // Two hydration columns with distinct variable roots: both root vars
        // must contribute to required_where_vars so the executor produces
        // bindings for each.
        let query = make_query_with_selections(vec![
            crate::ir::Column::Hydration(crate::ir::HydrationSpec::new(
                crate::ir::Root::Var(VarId(0)),
                crate::ir::NestedSelectSpec::Explicit {
                    forward: vec![],
                    reverse: HashMap::new(),
                },
            )),
            crate::ir::Column::Hydration(crate::ir::HydrationSpec::new(
                crate::ir::Root::Var(VarId(1)),
                crate::ir::NestedSelectSpec::Explicit {
                    forward: vec![],
                    reverse: HashMap::new(),
                },
            )),
        ]);

        let deps = compute_variable_deps(&query).unwrap();
        assert!(deps.required_where_vars.contains(&VarId(0)));
        assert!(deps.required_where_vars.contains(&VarId(1)));
        assert_eq!(deps.required_where_vars.len(), 2);
    }

    // ---- per-operator pipeline deps tests ----

    #[test]
    fn variable_deps_with_order_by() {
        // SELECT ?name WHERE { ... } ORDER BY ?age
        let mut query = make_query(vec![VarId(0)], vec![], SelectMode::Many);
        query.ordering = vec![SortSpec::asc(VarId(1))];

        let deps = compute_variable_deps(&query).unwrap();
        // required_sort_vars needs both ?name and ?age
        assert!(deps.required_sort_vars.contains(&VarId(0)));
        assert!(deps.required_sort_vars.contains(&VarId(1)));
        // required_where_vars same as sort (no post-WHERE ops between sort and WHERE)
        assert!(deps.required_where_vars.contains(&VarId(0)));
        assert!(deps.required_where_vars.contains(&VarId(1)));
    }

    #[test]
    fn variable_deps_with_group_by_and_aggregate() {
        // SELECT ?city (AVG(?age) AS ?avg) WHERE { ... } GROUP BY ?city
        let mut query = make_query(vec![VarId(0), VarId(2)], vec![], SelectMode::Many);
        query.grouping = Some(Grouping::Explicit {
            group_by: fluree_db_core::NonEmpty::try_from_vec(vec![VarId(0)]).unwrap(),
            aggregation: Some(Aggregation {
                aggregates: fluree_db_core::NonEmpty::try_from_vec(vec![AggregateSpec {
                    function: AggregateFn::Avg,
                    input_var: Some(VarId(1)),
                    output_var: VarId(2),
                    distinct: false,
                }])
                .unwrap(),
                binds: Vec::new(),
            }),
            having: None,
        });

        let deps = compute_variable_deps(&query).unwrap();

        // required_aggregate_vars = what Aggregate's OUTPUT must contain = SELECT vars
        assert!(deps.required_aggregate_vars.contains(&VarId(0)));
        assert!(deps.required_aggregate_vars.contains(&VarId(2)));

        // required_groupby_vars = what GROUP BY's OUTPUT must contain
        // = after tracing aggregates backward: ?avg→?age, so {?city, ?age}
        assert!(deps.required_groupby_vars.contains(&VarId(0)));
        assert!(deps.required_groupby_vars.contains(&VarId(1)));
        assert!(!deps.required_groupby_vars.contains(&VarId(2)));

        // required_where_vars = groupby deps + GROUP BY keys = {?city, ?age}
        assert!(deps.required_where_vars.contains(&VarId(0)));
        assert!(deps.required_where_vars.contains(&VarId(1)));
        assert!(!deps.required_where_vars.contains(&VarId(2)));
    }

    #[test]
    fn variable_deps_with_post_bind() {
        // SELECT ?x (AVG(?n) AS ?avg) (CEIL(?avg) AS ?ceil)
        // GROUP BY ?x ORDER BY ?ceil — bind on aggregate output, sorted on bind output.
        let mut query = make_query(vec![VarId(0), VarId(3)], vec![], SelectMode::Many);
        query.grouping = Some(Grouping::Explicit {
            group_by: fluree_db_core::NonEmpty::try_from_vec(vec![VarId(0)]).unwrap(),
            aggregation: Some(Aggregation {
                aggregates: fluree_db_core::NonEmpty::try_from_vec(vec![AggregateSpec {
                    function: AggregateFn::Avg,
                    input_var: Some(VarId(1)),
                    output_var: VarId(2),
                    distinct: false,
                }])
                .unwrap(),
                binds: vec![(
                    VarId(3),
                    Expression::Call {
                        func: crate::ir::Function::Ceil,
                        args: vec![Expression::Var(VarId(2))],
                    },
                )],
            }),
            having: None,
        });
        query.ordering = vec![SortSpec::asc(VarId(3))];

        let deps = compute_variable_deps(&query).unwrap();

        // required_sort_vars needs ?x and ?ceil
        assert!(deps.required_sort_vars.contains(&VarId(0)));
        assert!(deps.required_sort_vars.contains(&VarId(3)));

        // required_bind_vars[0] is what bind 0's output must contain.
        // Same as required_sort_vars since there's only one bind.
        assert_eq!(deps.required_bind_vars.len(), 1);
        assert!(deps.required_bind_vars[0].contains(&VarId(0)));
        assert!(deps.required_bind_vars[0].contains(&VarId(3)));

        // required_where_vars: bind ?ceil→?avg, aggregate ?avg→?n, so {?x, ?n}.
        assert!(deps.required_where_vars.contains(&VarId(0)));
        assert!(deps.required_where_vars.contains(&VarId(1)));
        assert!(!deps.required_where_vars.contains(&VarId(2)));
        assert!(!deps.required_where_vars.contains(&VarId(3)));
    }

    #[test]
    fn variable_deps_with_having() {
        // SELECT ?city (COUNT(?p) AS ?cnt) WHERE { ... }
        // GROUP BY ?city HAVING (?cnt > 5)
        let mut query = make_query(vec![VarId(0), VarId(2)], vec![], SelectMode::Many);
        query.grouping = Some(Grouping::Explicit {
            group_by: fluree_db_core::NonEmpty::try_from_vec(vec![VarId(0)]).unwrap(),
            aggregation: Some(Aggregation {
                aggregates: fluree_db_core::NonEmpty::try_from_vec(vec![AggregateSpec {
                    function: AggregateFn::Count,
                    input_var: Some(VarId(1)),
                    output_var: VarId(2),
                    distinct: false,
                }])
                .unwrap(),
                binds: Vec::new(),
            }),
            having: Some(Expression::gt(
                Expression::Var(VarId(2)),
                Expression::Const(FlakeValue::Long(5)),
            )),
        });

        let deps = compute_variable_deps(&query).unwrap();

        // required_having_vars = what HAVING's OUTPUT must contain = SELECT vars
        // HAVING expression vars are NOT needed in output (only in input)
        assert!(deps.required_having_vars.contains(&VarId(0)));
        assert!(deps.required_having_vars.contains(&VarId(2)));

        // required_aggregate_vars = what Aggregate's OUTPUT must contain
        // = required_having_vars ∪ HAVING expr vars (HAVING needs them from its child)
        assert!(deps.required_aggregate_vars.contains(&VarId(0)));
        assert!(deps.required_aggregate_vars.contains(&VarId(2)));

        // required_groupby_vars = what GROUP BY's OUTPUT must contain
        // = after tracing aggregates backward: ?cnt→?p, so {?city, ?p}
        assert!(deps.required_groupby_vars.contains(&VarId(0)));
        assert!(deps.required_groupby_vars.contains(&VarId(1)));
        assert!(!deps.required_groupby_vars.contains(&VarId(2)));

        // required_where_vars = groupby deps + GROUP BY keys = {?city, ?p}
        assert!(deps.required_where_vars.contains(&VarId(0)));
        assert!(deps.required_where_vars.contains(&VarId(1)));
        assert!(!deps.required_where_vars.contains(&VarId(2)));
    }
}
