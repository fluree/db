//! V4 — GROUP BY / aggregate projection-scope validation.
//!
//! SPARQL 1.1 §11 / §18.2.4 and the `SelectClause` grammar note: when a
//! query groups — an explicit `GROUP BY`, or an aggregate anywhere in its
//! SELECT, HAVING or ORDER BY (implicit single group, §18.2.4.1) — every
//! projected variable must be a **group key** (a bare `GROUP BY ?v`, or the
//! alias of a `GROUP BY (expr AS ?v)`) or appear only **inside an
//! aggregate**; and `SELECT *` is not permitted with `GROUP BY`.
//!
//! Leniencies (deliberate, to avoid over-rejection):
//! - `GROUP BY (?v)` — a bracketed bare variable — counts as the key `?v`.
//! - An alias assigned by an *earlier* item in the same SELECT clause is
//!   usable in later projection expressions (`SELECT (SUM(?x) AS ?s)
//!   (?s + 1 AS ?t)` is legal — the Extend chain binds `?s` first).
//! - `HAVING` / `ORDER BY` expressions are not checked here: a non-key
//!   variable they read means `SAMPLE(?v)` (§18.2.4.1), which lowering
//!   supplies.

use std::collections::HashSet;

use crate::ast::expr::Expression;
use crate::ast::pattern::GraphPattern;
use crate::ast::query::{
    GroupByClause, GroupCondition, OrderExpr, SelectVariable, SelectVariables, SolutionModifiers,
};
use crate::diag::{DiagCode, Diagnostic, Label};
use crate::span::SourceSpan;

/// Check a SELECT clause's projection against its grouping.
///
/// `clause_span` anchors the `SELECT *`-with-`GROUP BY` error (the star
/// itself has no span of its own).
pub(super) fn check_projection_scope(
    variables: &SelectVariables,
    modifiers: &SolutionModifiers,
    clause_span: SourceSpan,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let group_by = modifiers.group_by.as_ref();
    let items = match variables {
        SelectVariables::Star => {
            // `SELECT *` cannot contain an aggregate, so only an explicit
            // GROUP BY makes it invalid.
            if let Some(group_by) = group_by {
                diagnostics.push(
                    Diagnostic::error(
                        DiagCode::SelectStarWithGroupBy,
                        "SELECT * is not allowed with GROUP BY",
                        clause_span,
                    )
                    .with_label(Label::new(group_by.span, "grouped here"))
                    .with_help(
                        "Project the group keys and aggregates explicitly, \
                         e.g. SELECT ?key (COUNT(*) AS ?n).",
                    ),
                );
            }
            return;
        }
        SelectVariables::Explicit(items) => items,
    };

    if !modifiers.level_groups(variables) {
        return; // Not a grouped query.
    }

    // Allowed bare variables: group keys, plus aliases assigned by earlier
    // items in this SELECT clause.
    let mut allowed: HashSet<&str> = HashSet::new();
    if let Some(group_by) = group_by {
        for condition in &group_by.conditions {
            match condition {
                GroupCondition::Var(v) => {
                    allowed.insert(v.name.as_ref());
                }
                GroupCondition::Expr { expr, alias, .. } => {
                    if let Some(alias) = alias {
                        allowed.insert(alias.name.as_ref());
                    } else if let Expression::Var(v) = expr.unwrap_bracketed() {
                        // `GROUP BY (?v)` — treat as the key `?v`.
                        allowed.insert(v.name.as_ref());
                    }
                    // An unaliased key *expression* contributes no variable:
                    // `GROUP BY (?a + ?b)` does not license projecting `?a`.
                }
            }
        }
    }

    for item in items {
        match item {
            SelectVariable::Var(v) => {
                if !allowed.contains(v.name.as_ref()) {
                    diagnostics.push(ungrouped_error(v.name.as_ref(), v.span, group_by));
                }
            }
            SelectVariable::Expr { expr, alias, .. } => {
                let mut reported: HashSet<&str> = HashSet::new();
                for v in expr.unaggregated_variables() {
                    if !allowed.contains(v.name.as_ref()) && reported.insert(v.name.as_ref()) {
                        diagnostics.push(ungrouped_error(v.name.as_ref(), v.span, group_by));
                    }
                }
                // Later projection expressions may use this alias.
                allowed.insert(alias.name.as_ref());
            }
        }
    }
}

/// V6 — SELECT `AS` alias validity (SPARQL 1.1 §19.8 grammar note 13).
///
/// The variable assigned in `(expr AS ?v)` must not be (a) assigned by an
/// earlier item in the same SELECT clause, nor (b) already in scope in the
/// query's WHERE pattern (§18.2.1 in-scope definition — this is what makes
/// `SELECT (1 AS ?X) { SELECT (2 AS ?X) {} }` illegal: the sub-SELECT
/// projects `?X` into the outer pattern's scope).
pub(super) fn check_select_aliases(
    variables: &SelectVariables,
    pattern: &GraphPattern,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let SelectVariables::Explicit(items) = variables else {
        return; // SELECT * assigns nothing.
    };
    if !items
        .iter()
        .any(|item| matches!(item, SelectVariable::Expr { .. }))
    {
        return; // No AS assignments — skip the in-scope walk.
    }

    let mut in_scope_vars = Vec::new();
    pattern.add_in_scope_variables(&mut in_scope_vars);
    let in_scope: HashSet<&str> = in_scope_vars.iter().map(|v| v.name.as_ref()).collect();

    let mut assigned: HashSet<&str> = HashSet::new();
    for item in items {
        let SelectVariable::Expr { alias, .. } = item else {
            continue;
        };
        if assigned.contains(alias.name.as_ref()) {
            diagnostics.push(
                Diagnostic::error(
                    DiagCode::SelectAliasAlreadyBound,
                    format!(
                        "variable ?{} is assigned more than once in the SELECT clause",
                        alias.name
                    ),
                    alias.span,
                )
                .with_help(
                    "Each (expr AS ?v) in a SELECT clause must assign a distinct \
                     variable (SPARQL 1.1 §19.8).",
                ),
            );
        } else if in_scope.contains(alias.name.as_ref()) {
            diagnostics.push(
                Diagnostic::error(
                    DiagCode::SelectAliasAlreadyBound,
                    format!(
                        "SELECT alias ?{} is already in scope in the WHERE pattern",
                        alias.name
                    ),
                    alias.span,
                )
                .with_help(
                    "The variable assigned in (expr AS ?v) must not already be \
                     in scope (SPARQL 1.1 §19.8). Alias to a fresh variable name.",
                ),
            );
        }
        assigned.insert(alias.name.as_ref());
    }
}

/// SPARQL 1.2 negative-syntax rule: an aggregate call may not appear
/// inside another aggregate's argument (`COUNT(COUNT(*))`). Checked over
/// the SELECT projection and the GROUP BY / HAVING / ORDER BY expressions
/// of a (sub-)query.
pub(super) fn check_nested_aggregates(
    variables: &SelectVariables,
    modifiers: &SolutionModifiers,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let mut check = |expr: &Expression| {
        expr.walk(&mut |e| {
            if let Expression::Aggregate {
                expr: Some(arg),
                span,
                ..
            } = e
            {
                if arg.contains_aggregate() {
                    diagnostics.push(
                        Diagnostic::error(
                            DiagCode::NestedAggregate,
                            "aggregate function calls cannot be nested",
                            *span,
                        )
                        .with_help(
                            "Compute the inner aggregate in a sub-SELECT and \
                             aggregate over its result instead.",
                        ),
                    );
                }
            }
        });
    };

    if let SelectVariables::Explicit(items) = variables {
        for item in items {
            if let SelectVariable::Expr { expr, .. } = item {
                check(expr);
            }
        }
    }
    if let Some(group_by) = &modifiers.group_by {
        for condition in &group_by.conditions {
            if let GroupCondition::Expr { expr, .. } = condition {
                check(expr);
            }
        }
    }
    if let Some(having) = &modifiers.having {
        for condition in &having.conditions {
            check(condition);
        }
    }
    if let Some(order_by) = &modifiers.order_by {
        for condition in &order_by.conditions {
            if let OrderExpr::Expr(expr) = &condition.expr {
                check(expr);
            }
        }
    }
}

fn ungrouped_error(name: &str, span: SourceSpan, group_by: Option<&GroupByClause>) -> Diagnostic {
    let mut diag = Diagnostic::error(
        DiagCode::UngroupedVariableInProjection,
        format!(
            "variable ?{name} is projected but is neither a GROUP BY key \
             nor aggregated"
        ),
        span,
    )
    .with_help(
        "In a grouped query every projected variable must be a GROUP BY \
         key or appear only inside an aggregate such as COUNT()/SUM() \
         (SPARQL 1.1 §11).",
    );
    if let Some(group_by) = group_by {
        diag = diag.with_label(Label::new(group_by.span, "grouped here"));
    } else {
        diag = diag.with_note(
            "An aggregate in the SELECT, HAVING or ORDER BY clause groups the \
             whole solution into a single implicit group.",
        );
    }
    diag
}

/// An aggregate — in the SELECT, HAVING or ORDER BY clause — whose argument
/// reads an alias assigned by the same SELECT clause.
///
/// The alias is bound by the SELECT's `Extend`, which runs after aggregation
/// (SPARQL 1.1 §18.2.4.4), so the aggregate would see it unbound: `COUNT(?alias)`
/// would be 0 for every group. That answer is never what the query means, so
/// the shape is rejected with a pointer to aggregating the expression itself.
pub(super) fn check_aggregate_over_select_alias(
    variables: &SelectVariables,
    modifiers: &SolutionModifiers,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let SelectVariables::Explicit(items) = variables else {
        return;
    };
    let aliases: HashSet<&str> = items
        .iter()
        .filter_map(|item| match item {
            SelectVariable::Expr { alias, .. } => Some(alias.name.as_ref()),
            SelectVariable::Var(_) => None,
        })
        .collect();
    if aliases.is_empty() {
        return;
    }

    let mut reported: HashSet<String> = HashSet::new();
    let mut check = |expr: &Expression| {
        expr.walk(&mut |e| {
            if let Expression::Aggregate {
                expr: Some(arg), ..
            } = e
            {
                for v in arg.variables() {
                    let name: &str = v.name.as_ref();
                    if aliases.contains(name) && reported.insert(name.to_string()) {
                        diagnostics.push(
                            Diagnostic::error(
                                DiagCode::AggregateOverSelectAlias,
                                format!(
                                    "?{name} is assigned by this SELECT clause, after \
                                     aggregation, so it cannot be aggregated at the same level"
                                ),
                                v.span,
                            )
                            .with_help(
                                "Aggregate the expression itself (e.g. COUNT(IF(...)) \
                                 instead of COUNT(?alias)), or compute the alias in a \
                                 sub-SELECT and aggregate over its result.",
                            ),
                        );
                    }
                }
            }
        });
    };

    for item in items {
        if let SelectVariable::Expr { expr, .. } = item {
            check(expr);
        }
    }
    if let Some(having) = &modifiers.having {
        for condition in &having.conditions {
            check(condition);
        }
    }
    if let Some(order_by) = &modifiers.order_by {
        for condition in &order_by.conditions {
            if let OrderExpr::Expr(expr) = &condition.expr {
                check(expr);
            }
        }
    }
}
