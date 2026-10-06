//! Cypher lowering hooks for policy queries
//!
//! `f:query` policy conditions can be stored with the `f:cypher` datatype.
//! The consumer (`fluree_db_query::policy::QueryPolicyExecutor`) lives in
//! `fluree-db-query`, which cannot depend on `fluree-db-cypher` (the parser
//! depends on the query crate for lowering). This module implements the
//! [`fluree_db_query::lang_support`] Cypher hook using the real parser and
//! registers it process-wide, mirroring [`crate::sparql_lang`].
//!
//! Contract: a Cypher policy condition is a **read-only** query evaluated as
//! an existence check (at least one result row = the condition holds).
//! `$this` / `$identity` (and custom policy values) are supplied as Cypher
//! parameters carrying IRI strings — compare with `id(n)` / `elementId(n)`.
//! An unbound identity substitutes as `null`, which never compares equal.
//!
//! Bare identifiers lower without a ledger `@vocab` (the executor has no
//! ledger-context handle): labels and property names resolve exactly like
//! data written without `@vocab` (namespace-0 names), and full IRIs are
//! written in backticks. Ledgers configured with `@vocab` should use
//! backtick-quoted full IRIs in conditions.

use fluree_db_core::LedgerSnapshot;
use fluree_db_cypher::ast::Statement;
use fluree_db_cypher::{
    lower_cypher, parse_cypher, substitute_params, validate, Capabilities, CypherAst, Severity,
};
use fluree_db_query::ir::Pattern;
use fluree_db_query::lang_support::{register_cypher_support, CypherSupport};
use fluree_db_query::VarRegistry;

/// Register the Cypher lowering hook with `fluree-db-query`. Idempotent.
pub(crate) fn ensure_cypher_support_registered() {
    register_cypher_support(CypherSupport { lower_policy_query });
}

/// Parse Cypher source, returning the AST or a joined error message.
fn parse_to_ast(source: &str) -> Result<CypherAst, String> {
    let output = parse_cypher(source);
    let ast = match output.ast {
        Some(ast) => ast,
        None => {
            let msg = output
                .diagnostics
                .iter()
                .filter(|d| d.severity == Severity::Error)
                .map(|d| d.message.clone())
                .next()
                .unwrap_or_else(|| "Cypher parse error".to_string());
            return Err(msg);
        }
    };

    let capabilities = Capabilities::default();
    if let Some(err) = validate(&ast, &capabilities)
        .into_iter()
        .find(|d| d.severity == Severity::Error)
    {
        return Err(err.message);
    }

    Ok(ast)
}

/// Reject anything but a plain read query — policy conditions must not
/// write, and procedure calls / schema commands have no existence semantics.
fn require_read_only(ast: &CypherAst) -> Result<(), String> {
    match &ast.statement {
        Statement::Query(_) => Ok(()),
        Statement::Update(_) => Err(
            "Cypher policy queries must be read-only MATCH...RETURN queries \
             (got CREATE/MERGE/SET/DELETE)"
                .to_string(),
        ),
        Statement::Schema(_) | Statement::CallProcedure(_) => Err(
            "Cypher policy queries must be read-only MATCH...RETURN queries \
             (got a schema command or procedure call)"
                .to_string(),
        ),
    }
}

/// Validate a Cypher policy query source at policy-build time.
///
/// Checks parse success and the read-only requirement without substituting
/// parameters or lowering (no term interning against a snapshot). Used by
/// the policy builder to preserve the "deny on unparseable f:query"
/// behavior.
pub(crate) fn validate_cypher_policy_source(source: &str) -> Result<(), String> {
    let ast = parse_to_ast(source)?;
    require_read_only(&ast)
}

/// Lower a Cypher read query to WHERE patterns
/// (hook for [`fluree_db_query::lang_support`]).
fn lower_policy_query(
    source: &str,
    snapshot: &LedgerSnapshot,
    vars: &mut VarRegistry,
    params: &serde_json::Map<String, serde_json::Value>,
) -> Result<Vec<Pattern>, String> {
    let mut ast = parse_to_ast(source)?;
    require_read_only(&ast)?;

    // `$this` / `$identity` / custom policy values substitute as literals
    // before lowering; a `$param` the executor didn't supply is an error
    // (fails closed to deny).
    substitute_params(&mut ast, params).map_err(|e| e.to_string())?;

    let query = lower_cypher(&ast, snapshot, vars).map_err(|e| e.to_string())?;

    // Policy evaluation is an existence check over solutions; grouping
    // (aggregates) would change which solutions exist, so reject it rather
    // than silently mis-evaluate.
    if query.grouping.is_some() {
        return Err("aggregates are not supported in Cypher policy queries".to_string());
    }

    let mut patterns = query.patterns;
    if let Some(values) = query.post_values {
        patterns.push(values);
    }
    Ok(patterns)
}
