//! Variable dependency tracking for projection pushdown.
//!
//! Computes which variables the query output depends on by working backward
//! from the final output through ORDER BY, post-binds, HAVING, aggregates,
//! and GROUP BY. Variables without downstream dependencies are dead and can
//! be projected away early.

use crate::options::QueryOptions;
use crate::parse::ParsedQuery;
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
pub fn compute_variable_deps(query: &ParsedQuery, options: &QueryOptions) -> Option<VariableDeps> {
    // ---- backward walk ----

    // Seed deps from the query output requirements.
    // Wildcard/Boolean return None from `variables()`, disabling trimming.
    let mut deps: HashSet<VarId> = query.output.variables()?;

    // Graph crawl formatter reads the root variable from result batches
    // in addition to the SELECT variables (for mixed-select mode).
    if let Some(root_var) = query
        .graph_select
        .as_ref()
        .and_then(super::super::ir::GraphSelectSpec::root_var)
    {
        deps.insert(root_var);
    }

    // ORDER BY vars must survive to the sort operator.
    for spec in &options.order_by {
        deps.insert(spec.var);
    }
    let required_sort_vars: Vec<VarId> = deps.iter().copied().collect();

    // Post-binds (reverse order): trace expression inputs.
    // Record deps BEFORE processing each bind backward, since that
    // represents what the bind's output must contain for downstream.
    let mut required_bind_vars: Vec<Vec<VarId>> = Vec::with_capacity(options.post_binds.len());
    for (var, expr) in options.post_binds.iter().rev() {
        // Record what this bind's output must contain.
        required_bind_vars.push(deps.iter().copied().collect());
        // Then trace backward through the bind expression.
        if deps.remove(var) {
            deps.extend(expr.variables());
        }
    }
    // Reverse so indices match the forward (execution) order of post_binds.
    required_bind_vars.reverse();

    // Record what HAVING's output must contain (before tracing HAVING backward).
    let required_having_vars: Vec<VarId> = deps.iter().copied().collect();

    // HAVING expression variables: needed in HAVING's input but not
    // necessarily in its output (HAVING evaluates before trimming).
    if let Some(ref having_expr) = options.having {
        deps.extend(having_expr.variables());
    }

    // Record what Aggregate's output must contain (before tracing aggregates backward).
    let required_aggregate_vars: Vec<VarId> = deps.iter().copied().collect();

    // Aggregates: replace output vars with input vars.
    for spec in &options.aggregates {
        if deps.remove(&spec.output_var) {
            if let Some(input_var) = spec.input_var {
                deps.insert(input_var);
            }
        }
    }

    // Record what GROUP BY's output must contain (before tracing GROUP BY backward).
    let required_groupby_vars: Vec<VarId> = deps.iter().copied().collect();

    // GROUP BY keys must survive.
    deps.extend(options.group_by.iter().copied());

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
    use crate::aggregate::{AggregateFn, AggregateSpec};
    use crate::ir::{Expression, FilterValue, Pattern};
    use crate::options::QueryOptions;
    use crate::parse::{ConstructTemplate, ParsedQuery, QueryOutput, SelectMode};
    use crate::sort::SortSpec;
    use crate::ir::triple::{Ref, Term, TriplePattern};
    use fluree_db_core::Sid;
    use fluree_graph_json_ld::ParsedContext;

    fn make_query(
        select: Vec<VarId>,
        patterns: Vec<Pattern>,
        select_mode: SelectMode,
    ) -> ParsedQuery {
        let output = match select_mode {
            SelectMode::Many => QueryOutput::select(select),
            SelectMode::One => QueryOutput::select_one(select),
            SelectMode::Wildcard => QueryOutput::Wildcard,
            SelectMode::Construct => QueryOutput::Construct(ConstructTemplate::new(Vec::new())),
            SelectMode::Boolean => QueryOutput::Boolean,
        };
        ParsedQuery {
            context: ParsedContext::default(),
            orig_context: None,
            output,
            patterns,
            options: QueryOptions::default(),
            graph_select: None,
            post_values: None,
        }
    }

    fn make_tp(s: VarId, p: &str, o: VarId) -> TriplePattern {
        TriplePattern::new(Ref::Var(s), Ref::Sid(Sid::new(100, p)), Term::Var(o))
    }

    #[test]
    fn none_for_wildcard() {
        let query = make_query(vec![], vec![], SelectMode::Wildcard);
        assert!(compute_variable_deps(&query, &QueryOptions::default()).is_none());
    }

    #[test]
    fn none_for_boolean() {
        let query = make_query(vec![], vec![], SelectMode::Boolean);
        assert!(compute_variable_deps(&query, &QueryOptions::default()).is_none());
    }

    #[test]
    fn none_for_construct_without_template() {
        let query = make_query(vec![], vec![], SelectMode::Construct);
        assert!(compute_variable_deps(&query, &QueryOptions::default()).is_none());
    }

    #[test]
    fn none_for_empty_select() {
        let query = make_query(vec![], vec![], SelectMode::Many);
        assert!(compute_variable_deps(&query, &QueryOptions::default()).is_none());
    }

    #[test]
    fn simple_select_where_vars() {
        let query = make_query(vec![VarId(1), VarId(2)], vec![], SelectMode::Many);
        let deps = compute_variable_deps(&query, &QueryOptions::default()).unwrap();
        let where_set: HashSet<VarId> = deps.required_where_vars.into_iter().collect();
        assert_eq!(where_set, HashSet::from([VarId(1), VarId(2)]));
    }

    #[test]
    fn order_by_adds_where_vars() {
        let query = make_query(vec![VarId(1)], vec![], SelectMode::Many);
        let options = QueryOptions::new().with_order_by(vec![SortSpec::asc(VarId(3))]);
        let deps = compute_variable_deps(&query, &options).unwrap();
        assert!(deps.required_where_vars.contains(&VarId(1)));
        assert!(deps.required_where_vars.contains(&VarId(3)));
    }

    #[test]
    fn aggregate_replaces_output_with_input_in_where_vars() {
        // SELECT ?city (AVG(?age) AS ?avg) ... GROUP BY ?city
        let query = make_query(vec![VarId(2), VarId(3)], vec![], SelectMode::Many);
        let options = QueryOptions::new()
            .with_group_by(vec![VarId(2)])
            .with_aggregates(vec![AggregateSpec {
                function: AggregateFn::Avg,
                input_var: Some(VarId(1)),
                output_var: VarId(3),
                distinct: false,
            }]);

        let deps = compute_variable_deps(&query, &options).unwrap();
        // ?city (group key) and ?age (aggregate input) are WHERE dependencies
        assert!(deps.required_where_vars.contains(&VarId(2)));
        assert!(deps.required_where_vars.contains(&VarId(1)));
        // ?avg (aggregate output) is NOT a WHERE dependency
        assert!(!deps.required_where_vars.contains(&VarId(3)));
    }

    #[test]
    fn post_bind_traces_where_vars() {
        // SELECT ?x (CEIL(?avg) AS ?ceil)
        // post_bind: ?ceil = CEIL(?avg)
        let query = make_query(vec![VarId(0), VarId(2)], vec![], SelectMode::Many);
        let options = QueryOptions {
            post_binds: vec![(
                VarId(2),
                Expression::Call {
                    func: crate::ir::Function::Ceil,
                    args: vec![Expression::Var(VarId(1))],
                },
            )],
            ..Default::default()
        };

        let deps = compute_variable_deps(&query, &options).unwrap();
        assert!(deps.required_where_vars.contains(&VarId(0)));
        // ?avg (input to post-bind) is a WHERE dependency, ?ceil is not (it's computed)
        assert!(deps.required_where_vars.contains(&VarId(1)));
        assert!(!deps.required_where_vars.contains(&VarId(2)));
    }

    #[test]
    fn having_adds_where_vars() {
        let query = make_query(vec![VarId(0)], vec![], SelectMode::Many);
        let options = QueryOptions::new()
            .with_group_by(vec![VarId(0)])
            .with_having(Expression::gt(
                Expression::Var(VarId(1)),
                Expression::Const(FilterValue::Long(10)),
            ));

        let deps = compute_variable_deps(&query, &options).unwrap();
        assert!(deps.required_where_vars.contains(&VarId(0)));
        assert!(deps.required_where_vars.contains(&VarId(1)));
    }

    #[test]
    fn construct_uses_template_vars() {
        let query = ParsedQuery {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::Construct(ConstructTemplate::new(vec![make_tp(
                VarId(0),
                "name",
                VarId(1),
            )])),
            patterns: vec![],
            options: QueryOptions::default(),
            graph_select: None,
            post_values: None,
        };

        let deps = compute_variable_deps(&query, &QueryOptions::default()).unwrap();
        assert!(deps.required_where_vars.contains(&VarId(0)));
        assert!(deps.required_where_vars.contains(&VarId(1)));
    }

    // ---- graph_select tests ----

    #[test]
    fn graph_select_adds_root_var() {
        // SELECT ?name WHERE { ... } with graph_select rooted at ?s
        // The formatter needs both ?name (select var) and ?s (root var).
        let mut query = make_query(vec![VarId(1)], vec![], SelectMode::Many);
        query.graph_select = Some(crate::ir::GraphSelectSpec::new(
            crate::ir::Root::Var(VarId(0)),
            vec![],
        ));

        let deps = compute_variable_deps(&query, &QueryOptions::default()).unwrap();
        assert!(deps.required_where_vars.contains(&VarId(0))); // root var
        assert!(deps.required_where_vars.contains(&VarId(1))); // select var
    }

    #[test]
    fn graph_select_root_already_in_select() {
        // SELECT ?s WHERE { ... } with graph_select rooted at ?s
        // Root var is the same as the select var — no extra var needed.
        let mut query = make_query(vec![VarId(0)], vec![], SelectMode::Many);
        query.graph_select = Some(crate::ir::GraphSelectSpec::new(
            crate::ir::Root::Var(VarId(0)),
            vec![],
        ));

        let deps = compute_variable_deps(&query, &QueryOptions::default()).unwrap();
        assert!(deps.required_where_vars.contains(&VarId(0)));
        assert_eq!(deps.required_where_vars.len(), 1);
    }

    #[test]
    fn graph_select_sid_root_no_extra_vars() {
        // SELECT ?name WHERE { ... } with graph_select rooted at an IRI constant
        // Sid root doesn't reference a variable — only select vars needed.
        let mut query = make_query(vec![VarId(1)], vec![], SelectMode::Many);
        query.graph_select = Some(crate::ir::GraphSelectSpec::new(
            crate::ir::Root::Sid(Sid::new(100, "alice")),
            vec![],
        ));

        let deps = compute_variable_deps(&query, &QueryOptions::default()).unwrap();
        assert!(deps.required_where_vars.contains(&VarId(1)));
        assert_eq!(deps.required_where_vars.len(), 1);
    }

    #[test]
    fn graph_select_wildcard_output_disables_trimming() {
        // SELECT * WHERE { ... } with graph_select — Wildcard disables trimming.
        let mut query = make_query(vec![], vec![], SelectMode::Wildcard);
        query.graph_select = Some(crate::ir::GraphSelectSpec::new(
            crate::ir::Root::Var(VarId(0)),
            vec![],
        ));

        assert!(compute_variable_deps(&query, &QueryOptions::default()).is_none());
    }

    // ---- per-operator pipeline deps tests ----

    #[test]
    fn variable_deps_with_order_by() {
        // SELECT ?name WHERE { ... } ORDER BY ?age
        let query = make_query(vec![VarId(0)], vec![], SelectMode::Many);
        let options = QueryOptions::new().with_order_by(vec![SortSpec::asc(VarId(1))]);

        let deps = compute_variable_deps(&query, &options).unwrap();
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
        let query = make_query(vec![VarId(0), VarId(2)], vec![], SelectMode::Many);
        let options = QueryOptions::new()
            .with_group_by(vec![VarId(0)])
            .with_aggregates(vec![AggregateSpec {
                function: AggregateFn::Avg,
                input_var: Some(VarId(1)),
                output_var: VarId(2),
                distinct: false,
            }]);

        let deps = compute_variable_deps(&query, &options).unwrap();

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
        // SELECT ?x ?ceil WHERE { ... } BIND(CEIL(?avg) AS ?ceil) ORDER BY ?ceil
        let query = make_query(vec![VarId(0), VarId(2)], vec![], SelectMode::Many);
        let options = QueryOptions {
            post_binds: vec![(
                VarId(2),
                Expression::Call {
                    func: crate::ir::Function::Ceil,
                    args: vec![Expression::Var(VarId(1))],
                },
            )],
            order_by: vec![SortSpec::asc(VarId(2))],
            ..Default::default()
        };

        let deps = compute_variable_deps(&query, &options).unwrap();

        // required_sort_vars needs ?x and ?ceil
        assert!(deps.required_sort_vars.contains(&VarId(0)));
        assert!(deps.required_sort_vars.contains(&VarId(2)));

        // required_bind_vars[0] represents what bind 0's OUTPUT must contain.
        // That's the same as required_sort_vars since bind 0 is the only bind.
        assert_eq!(deps.required_bind_vars.len(), 1);
        assert!(deps.required_bind_vars[0].contains(&VarId(0)));
        assert!(deps.required_bind_vars[0].contains(&VarId(2)));

        // required_where_vars: bind traces ?ceil→?avg, so {?x, ?avg}
        assert!(deps.required_where_vars.contains(&VarId(0)));
        assert!(deps.required_where_vars.contains(&VarId(1)));
        assert!(!deps.required_where_vars.contains(&VarId(2)));
    }

    #[test]
    fn variable_deps_with_having() {
        // SELECT ?city (COUNT(?p) AS ?cnt) WHERE { ... }
        // GROUP BY ?city HAVING (?cnt > 5)
        let query = make_query(vec![VarId(0), VarId(2)], vec![], SelectMode::Many);
        let options = QueryOptions::new()
            .with_group_by(vec![VarId(0)])
            .with_aggregates(vec![AggregateSpec {
                function: AggregateFn::Count,
                input_var: Some(VarId(1)),
                output_var: VarId(2),
                distinct: false,
            }])
            .with_having(Expression::gt(
                Expression::Var(VarId(2)),
                Expression::Const(FilterValue::Long(5)),
            ));

        let deps = compute_variable_deps(&query, &options).unwrap();

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
