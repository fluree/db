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

use fluree_db_core::LedgerSnapshot;
use fluree_db_query::ir::Pattern;
use fluree_db_query::lang_support::{register_sparql_support, SparqlRuleParts, SparqlSupport};
use fluree_db_query::{ir::QueryOutput, VarRegistry};
use fluree_db_sparql::{parse_sparql, QueryBody, Severity, SparqlAst};

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

/// Lower a SPARQL `CONSTRUCT ... WHERE ...` rule (hook for
/// [`fluree_db_query::lang_support`]).
///
/// The rule engine executes the lowered WHERE clause through the normal
/// query executor and derives the CONSTRUCT template for each solution;
/// validation of what a rule body may contain (monotonic constructs only)
/// lives in `fluree_db_query::datalog_rules`, so this hook only parses and
/// lowers.
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
    if !matches!(query.output, QueryOutput::Construct(_)) {
        return Err("SPARQL rule is missing a CONSTRUCT template".to_string());
    }
    Ok(SparqlRuleParts { query, vars })
}
