//! SPARQL lowering hooks for policy queries and datalog rules
//!
//! `f:query` policy conditions and `f:rule` datalog rules can be stored with
//! the `f:sparql` datatype. The consumers of those literals live in
//! `fluree-db-query`, which cannot depend on `fluree-db-sparql` (the parser
//! depends on the query crate for lowering). This module implements the
//! [`fluree_db_query::lang_support`] hooks using the real SPARQL parser and
//! registers them process-wide.
//!
//! [`ensure_sparql_support_registered`] is called from the policy builder and
//! the query entry points; it is idempotent and effectively free after the
//! first call.

use fluree_db_core::{FlakeValue, LedgerSnapshot};
use fluree_db_query::ir::{Expression, Function, Pattern, Ref, Term, TriplePattern};
use fluree_db_query::lang_support::{
    register_sparql_support, CompareOp, RuleFilter, RuleTerm, RuleTriplePattern, RuleValue,
    SparqlRuleParts, SparqlSupport,
};
use fluree_db_query::{ir::QueryOutput, VarRegistry};
use fluree_db_sparql::{parse_sparql, QueryBody, Severity, SparqlAst};
use std::sync::Arc;

/// Register the SPARQL lowering hooks with `fluree-db-query`. Idempotent.
pub(crate) fn ensure_sparql_support_registered() {
    register_sparql_support(SparqlSupport {
        lower_policy_query,
        lower_rule,
    });
}

/// Parse SPARQL source, returning the AST or a joined error message.
///
/// Unlike the query endpoints (which surface structured diagnostics), policy
/// and rule extraction only need a message — failures land in logs and
/// fail-closed error paths.
fn parse_to_ast(source: &str) -> Result<SparqlAst, String> {
    let output = parse_sparql(source);
    let ast = match output.ast {
        Some(ast) => ast,
        None => {
            let msg = output
                .diagnostics
                .iter()
                .filter(|d| d.severity == Severity::Error)
                .map(|d| d.message.clone())
                .next()
                .unwrap_or_else(|| "SPARQL parse error".to_string());
            return Err(msg);
        }
    };

    // Same capability validation as the query endpoints.
    let capabilities = fluree_db_sparql::Capabilities::default();
    if let Some(err) = fluree_db_sparql::validate(&ast, &capabilities)
        .into_iter()
        .find(|d| d.severity == Severity::Error)
    {
        return Err(err.message);
    }

    Ok(ast)
}

/// Validate a SPARQL policy query source at policy-build time.
///
/// Checks parse success and the ASK/SELECT form requirement without
/// lowering (no term interning against a snapshot). Used by the policy
/// builder to preserve the "deny on unparseable f:query" behavior.
pub(crate) fn validate_sparql_policy_source(source: &str) -> Result<(), String> {
    let ast = parse_to_ast(source)?;
    match &ast.body {
        QueryBody::Ask(_) | QueryBody::Select(_) => Ok(()),
        QueryBody::Construct(_) | QueryBody::Describe(_) => {
            Err("SPARQL policy queries must be ASK or SELECT (got CONSTRUCT/DESCRIBE)".to_string())
        }
        QueryBody::Update(_) => {
            Err("SPARQL policy queries must be ASK or SELECT (got an update)".to_string())
        }
    }
}

/// Lower a SPARQL ASK/SELECT policy query to WHERE patterns
/// (hook for [`fluree_db_query::lang_support`]).
fn lower_policy_query(
    source: &str,
    snapshot: &LedgerSnapshot,
    vars: &mut VarRegistry,
) -> Result<Vec<Pattern>, String> {
    let ast = parse_to_ast(source)?;
    match &ast.body {
        QueryBody::Ask(_) | QueryBody::Select(_) => {}
        _ => {
            return Err(
                "SPARQL policy queries must be ASK or SELECT (got CONSTRUCT/DESCRIBE/update)"
                    .to_string(),
            )
        }
    }

    let query = fluree_db_sparql::lower_sparql(&ast, snapshot, vars).map_err(|e| e.to_string())?;

    // Policy evaluation is an existence check over WHERE solutions; grouping
    // (GROUP BY / aggregates / HAVING) would change which solutions exist, so
    // reject it rather than silently mis-evaluate.
    if query.grouping.is_some() {
        return Err("GROUP BY / aggregates are not supported in SPARQL policy queries".to_string());
    }

    let mut patterns = query.patterns;
    // A trailing VALUES clause is an inner-join constraint; keep it.
    if let Some(values) = query.post_values {
        patterns.push(values);
    }
    Ok(patterns)
}

/// Lower a SPARQL `CONSTRUCT ... WHERE ...` rule to datalog rule parts
/// (hook for [`fluree_db_query::lang_support`]).
///
/// The datalog engine executes a restricted pattern language: basic graph
/// patterns plus simple comparison FILTERs. Anything else (OPTIONAL, UNION,
/// property paths, BIND, subqueries, ...) is rejected with a descriptive
/// error rather than approximated.
fn lower_rule(source: &str, snapshot: &LedgerSnapshot) -> Result<SparqlRuleParts, String> {
    let ast = parse_to_ast(source)?;
    if !matches!(&ast.body, QueryBody::Construct(_)) {
        return Err(
            "SPARQL rules must be CONSTRUCT ... WHERE ... queries; the CONSTRUCT template \
             is the rule head (insert) and the WHERE clause is the rule body"
                .to_string(),
        );
    }

    let mut vars = VarRegistry::new();
    let query =
        fluree_db_sparql::lower_sparql(&ast, snapshot, &mut vars).map_err(|e| e.to_string())?;

    if query.grouping.is_some() {
        return Err("GROUP BY / aggregates are not supported in SPARQL rules".to_string());
    }

    let mut where_patterns = Vec::new();
    let mut filters = Vec::new();
    for pattern in &query.patterns {
        match pattern {
            Pattern::Triple(tp) => where_patterns.push(triple_to_rule_pattern(tp, &vars)?),
            Pattern::Filter(expr) => filters.push(expr_to_rule_filter(expr, &vars)?),
            other => {
                return Err(format!(
                    "SPARQL rule WHERE clause contains an unsupported construct ({}); \
                     only basic graph patterns and comparison FILTERs are supported",
                    pattern_kind(other)
                ))
            }
        }
    }

    let template = match &query.output {
        QueryOutput::Construct(t) => &t.patterns,
        _ => return Err("SPARQL rule is missing a CONSTRUCT template".to_string()),
    };
    let insert_patterns = template
        .iter()
        .map(|tp| triple_to_rule_pattern(tp, &vars))
        .collect::<Result<Vec<_>, _>>()?;

    if where_patterns.is_empty() {
        return Err("SPARQL rule WHERE clause has no triple patterns".to_string());
    }
    if insert_patterns.is_empty() {
        return Err("SPARQL rule CONSTRUCT template has no triple patterns".to_string());
    }

    Ok(SparqlRuleParts {
        where_patterns,
        filters,
        insert_patterns,
    })
}

/// Short human-readable label for an unsupported pattern kind.
fn pattern_kind(pattern: &Pattern) -> &'static str {
    match pattern {
        Pattern::Triple(_) => "triple",
        Pattern::Filter(_) => "filter",
        Pattern::Optional(_) => "OPTIONAL",
        Pattern::Union(_) => "UNION",
        Pattern::Bind { .. } => "BIND",
        Pattern::Values { .. } => "VALUES",
        Pattern::Minus(_) => "MINUS",
        Pattern::Exists(_) => "EXISTS",
        Pattern::NotExists(_) => "NOT EXISTS",
        Pattern::PropertyPath(_) => "property path",
        _ => "unsupported pattern",
    }
}

fn ref_to_rule_term(r: &Ref, vars: &VarRegistry) -> Result<RuleTerm, String> {
    match r {
        Ref::Var(v) => Ok(RuleTerm::Var(Arc::from(vars.name(*v)))),
        Ref::Sid(sid) => Ok(RuleTerm::Sid(sid.clone())),
        Ref::Iri(iri) => Err(format!(
            "unresolved IRI <{iri}> in SPARQL rule (not registered on this ledger)"
        )),
    }
}

fn term_to_rule_term(t: &Term, vars: &VarRegistry) -> Result<RuleTerm, String> {
    match t {
        Term::Var(v) => Ok(RuleTerm::Var(Arc::from(vars.name(*v)))),
        Term::Sid(sid) => Ok(RuleTerm::Sid(sid.clone())),
        Term::Iri(iri) => Err(format!(
            "unresolved IRI <{iri}> in SPARQL rule (not registered on this ledger)"
        )),
        Term::Value(fv) => Ok(RuleTerm::Value(flake_value_to_rule_value(fv)?)),
    }
}

fn flake_value_to_rule_value(fv: &FlakeValue) -> Result<RuleValue, String> {
    match fv {
        FlakeValue::String(s) => Ok(RuleValue::String(s.to_string())),
        FlakeValue::Long(l) => Ok(RuleValue::Long(*l)),
        FlakeValue::Double(d) => Ok(RuleValue::Double(*d)),
        FlakeValue::Boolean(b) => Ok(RuleValue::Boolean(*b)),
        FlakeValue::Ref(sid) => Ok(RuleValue::Ref(sid.clone())),
        other => Err(format!(
            "unsupported literal type in SPARQL rule: {other:?} \
             (supported: string, integer, double, boolean, IRI)"
        )),
    }
}

fn triple_to_rule_pattern(
    tp: &TriplePattern,
    vars: &VarRegistry,
) -> Result<RuleTriplePattern, String> {
    Ok(RuleTriplePattern {
        subject: ref_to_rule_term(&tp.s, vars)?,
        predicate: ref_to_rule_term(&tp.p, vars)?,
        object: term_to_rule_term(&tp.o, vars)?,
    })
}

fn expr_to_rule_term(e: &Expression, vars: &VarRegistry) -> Result<RuleTerm, String> {
    match e {
        Expression::Var(v) => Ok(RuleTerm::Var(Arc::from(vars.name(*v)))),
        Expression::Const(fv) => Ok(RuleTerm::Value(flake_value_to_rule_value(fv)?)),
        _ => Err(
            "SPARQL rule FILTER operands must be variables or constants (no nested expressions)"
                .to_string(),
        ),
    }
}

fn expr_to_rule_filter(e: &Expression, vars: &VarRegistry) -> Result<RuleFilter, String> {
    let Expression::Call { func, args } = e else {
        return Err(
            "SPARQL rule FILTER must be a comparison (=, !=, <, <=, >, >=) or a boolean \
             combination (&&, ||, !) of comparisons"
                .to_string(),
        );
    };

    let compare_op = |op: CompareOp| -> Result<RuleFilter, String> {
        if args.len() != 2 {
            return Err("comparison in SPARQL rule FILTER must have two operands".to_string());
        }
        Ok(RuleFilter::Compare {
            op,
            left: expr_to_rule_term(&args[0], vars)?,
            right: expr_to_rule_term(&args[1], vars)?,
        })
    };

    match func {
        Function::Eq => compare_op(CompareOp::Equal),
        Function::Ne => compare_op(CompareOp::NotEqual),
        Function::Lt => compare_op(CompareOp::LessThan),
        Function::Le => compare_op(CompareOp::LessThanOrEqual),
        Function::Gt => compare_op(CompareOp::GreaterThan),
        Function::Ge => compare_op(CompareOp::GreaterThanOrEqual),
        Function::And => Ok(RuleFilter::And(
            args.iter()
                .map(|a| expr_to_rule_filter(a, vars))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Function::Or => Ok(RuleFilter::Or(
            args.iter()
                .map(|a| expr_to_rule_filter(a, vars))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Function::Not => {
            if args.len() != 1 {
                return Err("NOT in SPARQL rule FILTER must have one operand".to_string());
            }
            Ok(RuleFilter::Not(Box::new(expr_to_rule_filter(
                &args[0], vars,
            )?)))
        }
        other => Err(format!(
            "unsupported function {other:?} in SPARQL rule FILTER \
             (supported: =, !=, <, <=, >, >=, &&, ||, !)"
        )),
    }
}
