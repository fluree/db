//! Rule validation: the monotonicity rule, range restriction, and the
//! fail-closed filter-operand checks.
//!
//! Every rejection is an error naming the rule and the construct. A rule that
//! quietly derives less than it was written to is the worst outcome — see
//! docs/design/rules-engine.md.

use super::{HeadTerm, RuleHead};
use crate::error::{QueryError, Result};
use crate::ir::{Expression, Function, Pattern, Query, Ref, Term};
use crate::parse::ast::{UnresolvedExpression, UnresolvedFilterValue, UnresolvedPattern};
use crate::var_registry::{VarId, VarRegistry};
use fluree_db_core::value::FlakeValue;
use fluree_db_core::LedgerSnapshot;
use serde_json::Value as JsonValue;
use std::collections::HashSet;

fn reject(rule: &str, what: &str) -> QueryError {
    QueryError::InvalidQuery(format!("datalog rule {rule}: {what}"))
}

/// Reject non-monotonic constructs anywhere in the body, and IRI filter
/// operands whose namespace the ledger has never seen (they could never match
/// a stored term, so an exclusion filter on them would keep every row).
pub(super) fn validate_body(query: &Query, snapshot: &LedgerSnapshot, rule: &str) -> Result<()> {
    if query.grouping.is_some() {
        return Err(reject(
            rule,
            "the body uses GROUP BY / aggregates, which are not allowed in a rule body \
             (a fixpoint cannot evaluate them monotonically)",
        ));
    }
    // Same reasoning as the subquery arm below, for the rule's own body:
    // `build_rule` clears these modifiers before execution, so without a check
    // a `CONSTRUCT … WHERE { … } LIMIT 1` would run as though the LIMIT were
    // not written — the silent-drop this engine exists to stop.
    if let Some(what) = query
        .limit
        .map(|_| "LIMIT")
        .or_else(|| query.offset.map(|_| "OFFSET"))
    {
        return Err(reject(
            rule,
            &format!(
                "the body uses {what}, which is not allowed in a rule body: which solutions a \
                 slice keeps depends on how much the fixpoint has derived so far, so the rule \
                 would derive different facts depending on round order"
            ),
        ));
    }
    walk_patterns(&query.patterns, snapshot, rule)?;
    if let Some(values) = &query.post_values {
        walk_patterns(std::slice::from_ref(values), snapshot, rule)?;
    }
    Ok(())
}

fn walk_patterns(patterns: &[Pattern], snapshot: &LedgerSnapshot, rule: &str) -> Result<()> {
    for pattern in patterns {
        match pattern {
            Pattern::Optional(_) => {
                return Err(non_monotonic(rule, "OPTIONAL"));
            }
            Pattern::Minus(_) => {
                return Err(non_monotonic(rule, "MINUS"));
            }
            Pattern::NotExists(_) => {
                return Err(non_monotonic(rule, "NOT EXISTS (`not-exists`)"));
            }
            Pattern::Service(_) => {
                return Err(reject(
                    rule,
                    "the body uses SERVICE, which is not allowed in a rule body \
                     (rules derive over local data only)",
                ));
            }
            Pattern::Filter(expr) => check_expr(expr, snapshot, rule, false)?,
            Pattern::Bind { expr, .. } => check_expr(expr, snapshot, rule, false)?,
            Pattern::Unwind { list, .. } => check_expr(list, snapshot, rule, false)?,
            Pattern::Union(branches) => {
                for branch in branches {
                    walk_patterns(branch, snapshot, rule)?;
                }
            }
            Pattern::Exists(inner)
            | Pattern::Graph {
                patterns: inner, ..
            }
            | Pattern::DefaultGraphSource { patterns: inner }
            | Pattern::EdgeAnnotation { body: inner, .. }
            | Pattern::AnnotationTarget { body: inner, .. } => {
                walk_patterns(inner, snapshot, rule)?;
            }
            Pattern::Subquery(sub) => {
                // A subquery carries its own modifiers, and two of them break a
                // fixpoint. GROUP BY / aggregates are non-monotonic for the
                // same reason the top-level check rejects them. LIMIT and
                // OFFSET are worse than unsupported: which solutions survive a
                // slice depends on how much has been derived so far, so the
                // rule's output changes with round order and the result is then
                // cached. DISTINCT and ORDER BY are safe — neither changes the
                // solution SET as derivation grows, and heads dedup anyway.
                if sub.grouping.is_some() {
                    return Err(reject(
                        rule,
                        "a subquery in the body uses GROUP BY / aggregates, which are not \
                         allowed in a rule body (a fixpoint cannot evaluate them monotonically)",
                    ));
                }
                if let Some(what) = sub
                    .limit
                    .map(|_| "LIMIT")
                    .or_else(|| sub.offset.map(|_| "OFFSET"))
                {
                    return Err(reject(
                        rule,
                        &format!(
                            "a subquery in the body uses {what}, which is not allowed in a rule \
                             body: which solutions a slice keeps depends on how much the fixpoint \
                             has derived so far, so the rule would derive different facts \
                             depending on round order"
                        ),
                    ));
                }
                walk_patterns(&sub.patterns, snapshot, rule)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn non_monotonic(rule: &str, construct: &str) -> QueryError {
    reject(
        rule,
        &format!(
            "the body uses {construct}, which is not allowed in a rule body: a fixpoint \
             cannot evaluate negation or left joins soundly without stratification, so \
             the rule is rejected rather than run over a wrong answer"
        ),
    )
}

/// Walk a filter/bind expression for constructs a fixpoint cannot evaluate.
///
/// Two things are rejected. A negated EXISTS is negation, and it spells itself
/// more than one way: `NOT EXISTS { … }` lowers to `Exists { negated: true }`,
/// while `!EXISTS { … }` lowers to `Not` wrapping a plain `Exists`. `negated`
/// carries the parity of the `Not` wrappers seen so far, so both spellings —
/// and any odd nesting of `Not` around either — are caught, while `!!EXISTS`
/// stays allowed because it is monotone. And an `IRI(…)` constant naming a
/// namespace the ledger has never seen is fail-open in `!=` form.
///
/// The match is deliberately exhaustive rather than falling through on a
/// wildcard: every variant that can hold a sub-expression or a pattern is
/// visited, so a construct cannot hide from the walk inside a container the
/// walker forgot, which is exactly how `!EXISTS` slipped past the first cut of
/// this check. A new `Expression` variant is a compile error here until
/// someone decides what it means in a rule body.
fn check_expr(
    expr: &Expression,
    snapshot: &LedgerSnapshot,
    rule: &str,
    negated: bool,
) -> Result<()> {
    match expr {
        Expression::Exists {
            patterns,
            negated: inner,
        } => {
            if negated ^ *inner {
                return Err(non_monotonic(rule, "NOT EXISTS inside FILTER"));
            }
            walk_patterns(patterns, snapshot, rule)
        }
        Expression::Call { func, args } => {
            if matches!(func, Function::Iri) {
                if let [Expression::Const(FlakeValue::String(iri))] = args.as_slice() {
                    if snapshot.encode_iri_strict(iri).is_none() {
                        return Err(reject(
                            rule,
                            &format!(
                                "filter operand <{iri}> names a namespace this ledger has never \
                                 seen, so it can never equal a stored term; the rule is rejected \
                                 rather than run with a filter that cannot match (quote the \
                                 operand if you meant a string literal)"
                            ),
                        ));
                    }
                }
            }
            let inner_negated = negated ^ matches!(func, Function::Not);
            for arg in args {
                check_expr(arg, snapshot, rule, inner_negated)?;
            }
            Ok(())
        }
        // A pattern comprehension is a correlated subquery evaluated per row;
        // it has no JSON-LD or SPARQL rule surface today (it is Cypher's), and
        // its patterns would need the monotonicity walk before it could.
        Expression::PatternComprehension { .. } => Err(reject(
            rule,
            "the body uses a pattern comprehension, which is not allowed in a rule body",
        )),
        Expression::Map(entries) => entries
            .iter()
            .try_for_each(|(_, v)| check_expr(v, snapshot, rule, negated)),
        Expression::ListComprehension {
            list, filter, map, ..
        } => {
            check_expr(list, snapshot, rule, negated)?;
            for part in [filter, map].into_iter().flatten() {
                check_expr(part, snapshot, rule, negated)?;
            }
            Ok(())
        }
        Expression::Reduce {
            init, list, body, ..
        } => {
            check_expr(init, snapshot, rule, negated)?;
            check_expr(list, snapshot, rule, negated)?;
            check_expr(body, snapshot, rule, negated)
        }
        Expression::ListPredicate {
            list, predicate, ..
        } => {
            check_expr(list, snapshot, rule, negated)?;
            check_expr(predicate, snapshot, rule, negated)
        }
        Expression::Member { target, .. } => check_expr(target, snapshot, rule, negated),
        // Leaves: nothing to walk. `Resolved` is runtime-only and never
        // reaches a rule body at parse time.
        Expression::Var(_) | Expression::Const(_) | Expression::Resolved(_) => Ok(()),
    }
}

/// Every variable the body can BIND (deduplicated, in first-seen order).
///
/// `produced_vars`, not `referenced_vars`: the question range restriction asks
/// is which variables a matched row actually carries a value for, and
/// `referenced_vars` includes operands a pattern only reads — filter operands
/// most of all. A head variable mentioned solely inside a `FILTER` passed the
/// check under `referenced_vars`, then derived nothing at runtime with no
/// diagnostic. `produced_vars` returns nothing for `Filter`, `Minus`, `Exists`
/// and `NotExists`, only the target of a `Bind`, and only the SELECT list of a
/// subquery — the predicate this check's name claims.
///
/// The result is also the body's output projection, so it must not be narrower
/// than the head needs: every head variable is a produced variable by the time
/// `check_range_restriction` has passed.
pub(super) fn body_vars(query: &Query) -> Vec<VarId> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    let mut push = |ids: Vec<VarId>| {
        for id in ids {
            if seen.insert(id) {
                out.push(id);
            }
        }
    };
    for pattern in &query.patterns {
        push(pattern.produced_vars());
    }
    if let Some(values) = &query.post_values {
        push(values.produced_vars());
    }
    out
}

/// Range restriction: a head may only use variables the body binds.
pub(super) fn check_range_restriction(
    heads: &[RuleHead],
    body_vars: &[VarId],
    vars: &VarRegistry,
    rule: &str,
) -> Result<()> {
    let bound: HashSet<VarId> = body_vars.iter().copied().collect();
    let mut unbound: Vec<&str> = Vec::new();
    for head in heads {
        for term in [&head.subject, &head.predicate, &head.object] {
            if let HeadTerm::Var(v) = term {
                if !bound.contains(v) {
                    let name = vars.try_name(*v).unwrap_or("?");
                    if !unbound.contains(&name) {
                        unbound.push(name);
                    }
                }
            }
        }
    }
    if unbound.is_empty() {
        return Ok(());
    }
    Err(reject(
        rule,
        &format!(
            "the insert pattern uses {} which the where clause never binds; every \
             variable used in `insert` must also appear in `where` (a where/insert \
             typo is the usual cause)",
            unbound
                .iter()
                .map(|v| format!("`{v}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    ))
}

/// Fail-closed checks on the JSON-LD rule's filters before lowering.
///
/// - An unquoted `prefix:name` operand whose prefix the rule's `@context`
///   does not define would lower as a string and never equal an IRI (`=`
///   derives nothing, `!=` keeps every row).
/// - An unquoted bare word is ambiguous between a string and a local name;
///   against an IRI-bound variable a string comparison fails invisibly in both
///   directions, so it is rejected with both rewrites.
pub(super) fn lint_unresolved_filters(patterns: &[UnresolvedPattern], rule: &str) -> Result<()> {
    for pattern in patterns {
        match pattern {
            UnresolvedPattern::Filter(expr)
            | UnresolvedPattern::Bind { expr, .. }
            | UnresolvedPattern::Unwind { expr, .. } => lint_unresolved_expr(expr, rule)?,
            UnresolvedPattern::Optional(inner)
            | UnresolvedPattern::Minus(inner)
            | UnresolvedPattern::Exists(inner)
            | UnresolvedPattern::NotExists(inner)
            | UnresolvedPattern::Graph {
                patterns: inner, ..
            }
            | UnresolvedPattern::EdgeAnnotation { body: inner, .. }
            | UnresolvedPattern::AnnotationTarget { body: inner, .. } => {
                lint_unresolved_filters(inner, rule)?;
            }
            UnresolvedPattern::Union(branches) => {
                for branch in branches {
                    lint_unresolved_filters(branch, rule)?;
                }
            }
            UnresolvedPattern::Subquery(sub) => lint_unresolved_filters(&sub.patterns, rule)?,
            _ => {}
        }
    }
    Ok(())
}

fn lint_unresolved_expr(expr: &UnresolvedExpression, rule: &str) -> Result<()> {
    match expr {
        UnresolvedExpression::Const(UnresolvedFilterValue::Curie(atom)) => Err(reject(
            rule,
            &format!(
                "filter operand `{atom}` uses a prefix the rule's @context does not define; \
                 an unresolved operand can never equal an IRI, so the rule is rejected \
                 rather than run with a filter that cannot match. Define the prefix in \
                 the rule's @context, or write \"{atom}\" (quoted) for a string literal"
            ),
        )),
        UnresolvedExpression::Const(UnresolvedFilterValue::Bare(word)) => Err(reject(
            rule,
            &format!(
                "filter operand `{word}` is a bare word, which is ambiguous between a \
                 string and a local name (against an IRI-bound variable a string \
                 comparison fails invisibly in both directions). Write \"{word}\" \
                 (quoted) to compare against the string, or a prefixed IRI such as \
                 ex:{word} to compare against the IRI"
            ),
        )),
        UnresolvedExpression::And(items) | UnresolvedExpression::Or(items) => {
            items.iter().try_for_each(|e| lint_unresolved_expr(e, rule))
        }
        UnresolvedExpression::Not(inner) => lint_unresolved_expr(inner, rule),
        UnresolvedExpression::In { expr, values, .. } => {
            lint_unresolved_expr(expr, rule)?;
            values
                .iter()
                .try_for_each(|e| lint_unresolved_expr(e, rule))
        }
        UnresolvedExpression::Call { args, .. } => {
            args.iter().try_for_each(|e| lint_unresolved_expr(e, rule))
        }
        _ => Ok(()),
    }
}

/// Warn when a filter compares a variable that occurs only in IRI positions
/// (subject or predicate) against a quoted string. RDFterm-equal makes that a
/// clean `false`, so the rule derives nothing with no other signal.
pub(super) fn warn_iri_vs_literal(query: &Query, vars: &VarRegistry, rule: &str) {
    let mut iri_position: HashSet<VarId> = HashSet::new();
    let mut literal_capable: HashSet<VarId> = HashSet::new();
    collect_positions(&query.patterns, &mut iri_position, &mut literal_capable);
    let iri_only: HashSet<VarId> = iri_position.difference(&literal_capable).copied().collect();
    if iri_only.is_empty() {
        return;
    }
    for pattern in &query.patterns {
        if let Pattern::Filter(expr) = pattern {
            warn_expr(expr, &iri_only, vars, rule);
        }
    }
}

fn collect_positions(
    patterns: &[Pattern],
    iri_position: &mut HashSet<VarId>,
    literal_capable: &mut HashSet<VarId>,
) {
    for pattern in patterns {
        match pattern {
            Pattern::Triple(tp) => {
                if let Ref::Var(v) = &tp.s {
                    iri_position.insert(*v);
                }
                if let Ref::Var(v) = &tp.p {
                    iri_position.insert(*v);
                }
                if let Term::Var(v) = &tp.o {
                    literal_capable.insert(*v);
                }
            }
            Pattern::EdgeAnnotation { edge, body, .. }
            | Pattern::AnnotationTarget { edge, body, .. } => {
                collect_positions(
                    std::slice::from_ref(&Pattern::Triple(edge.clone())),
                    iri_position,
                    literal_capable,
                );
                collect_positions(body, iri_position, literal_capable);
            }
            Pattern::Union(branches) => {
                for branch in branches {
                    collect_positions(branch, iri_position, literal_capable);
                }
            }
            Pattern::Graph { patterns, .. } | Pattern::DefaultGraphSource { patterns } => {
                collect_positions(patterns, iri_position, literal_capable);
            }
            _ => {}
        }
    }
}

fn warn_expr(expr: &Expression, iri_only: &HashSet<VarId>, vars: &VarRegistry, rule: &str) {
    if let Expression::Call { func, args } = expr {
        if matches!(func, Function::Eq | Function::Ne) {
            if let [a, b] = args.as_slice() {
                let pair = match (a, b) {
                    (Expression::Var(v), Expression::Const(FlakeValue::String(s)))
                    | (Expression::Const(FlakeValue::String(s)), Expression::Var(v)) => {
                        Some((*v, s))
                    }
                    _ => None,
                };
                if let Some((v, s)) = pair {
                    if iri_only.contains(&v) {
                        tracing::warn!(
                            rule,
                            var = vars.try_name(v).unwrap_or("?"),
                            operand = %s,
                            "datalog rule filter compared an IRI against a literal: a quoted \
                             operand is a string literal and never equals an IRI — if the \
                             operand was meant to name an IRI, write it unquoted as a \
                             prefixed or absolute IRI (`ex:knows`, not `\"ex:knows\"`)"
                        );
                    }
                }
            }
        }
        for arg in args {
            warn_expr(arg, iri_only, vars, rule);
        }
    }
}

/// Keyword keys a rule body may use, by object shape. The standard query
/// parser ignores an unknown `@`-key on a node pattern (the pattern simply
/// never matches), which in a rule means "derive nothing, silently" — the
/// failure mode #1558 reports. A rule is rejected instead.
const NODE_KEYWORDS: &[&str] = &[
    "@id",
    "@type",
    "@context",
    "@annotation",
    "@edge",
    "@reifies",
];
const VALUE_OBJECT_KEYWORDS: &[&str] = &["@value", "@type", "@language", "@annotation", "@edge"];

/// Walk the raw `where` JSON and reject any `@`-prefixed key the query parser
/// does not give a meaning to in that position.
pub(super) fn reject_unknown_keyword_keys(value: &JsonValue, label: &str) -> Result<()> {
    match value {
        JsonValue::Object(map) => {
            let allowed: &[&str] = if map.contains_key("@value") {
                VALUE_OBJECT_KEYWORDS
            } else {
                NODE_KEYWORDS
            };
            for (key, child) in map {
                if key.starts_with('@') && !allowed.contains(&key.as_str()) {
                    return Err(QueryError::InvalidQuery(format!(
                        "datalog rule {label}: `{key}` is not a where-pattern keyword the \
                         rule engine understands (node patterns take {}; value objects take {}); \
                         the rule is rejected rather than run with the key ignored",
                        NODE_KEYWORDS.join(", "),
                        VALUE_OBJECT_KEYWORDS.join(", ")
                    )));
                }
                if key != "@context" {
                    reject_unknown_keyword_keys(child, label)?;
                }
            }
            Ok(())
        }
        JsonValue::Array(items) => items
            .iter()
            .try_for_each(|item| reject_unknown_keyword_keys(item, label)),
        _ => Ok(()),
    }
}
