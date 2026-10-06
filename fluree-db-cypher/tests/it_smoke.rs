//! Smoke tests: scaffolding compiles and the parse entry point returns.

use fluree_db_cypher::parse_cypher;

#[test]
fn parse_smoke_match_return() {
    let out = parse_cypher("MATCH (n:Person) RETURN n");
    assert!(!out.has_errors(), "diagnostics: {:?}", out.diagnostics);
    assert!(out.ast.is_some());
}

#[test]
fn parse_smoke_empty_returns_error() {
    let out = parse_cypher("");
    assert!(out.has_errors());
}

#[test]
fn parse_xor_expression() {
    let out = parse_cypher("MATCH (n:Person) WHERE n.a = 1 XOR n.b = 2 RETURN n");
    assert!(!out.has_errors(), "diagnostics: {:?}", out.diagnostics);
    assert!(out.ast.is_some());
}

#[test]
fn parse_modulus_expression() {
    let out = parse_cypher("MATCH (n:Person) WHERE n.id % 2 = 0 RETURN n");
    assert!(!out.has_errors(), "diagnostics: {:?}", out.diagnostics);
    assert!(out.ast.is_some());
}

/// Regression: a long `XOR` chain must stay linear. The old structural
/// desugaring cloned the left operand twice per operator, so this 2000-term
/// chain expanded to a ~2^2000-node AST and never finished parsing. With a
/// first-class `BinOp::Xor` node it parses in microseconds.
#[test]
fn parse_long_xor_chain_is_linear() {
    let terms: Vec<&str> = vec!["true"; 2000];
    let query = format!("RETURN {}", terms.join(" XOR "));
    let out = parse_cypher(&query);
    assert!(!out.has_errors(), "diagnostics: {:?}", out.diagnostics);
    assert!(out.ast.is_some());
}

/// Regression: deeply-nested input must return a diagnostic, not overflow the
/// stack (a Rust stack overflow aborts the whole process — an unauthenticated
/// DoS, since `parse_cypher` runs on the request handler thread). The depth
/// guard trips long before the stack is exhausted. Each shape exercises a
/// distinct recursion path: parens re-enter `parse_or`, the unary layers
/// self-recurse, and `CALL { … }` re-enters `parse_statement`.
#[test]
fn deep_paren_nesting_errors() {
    let parens = format!("RETURN {}1{}", "(".repeat(50_000), ")".repeat(50_000));
    let out = parse_cypher(&parens);
    assert!(out.has_errors(), "deep parens should error");
    assert!(out.ast.is_none());
}

#[test]
fn deep_not_nesting_errors() {
    let nots = format!("RETURN {}true", "NOT ".repeat(50_000));
    assert!(parse_cypher(&nots).has_errors(), "deep NOT should error");
}

#[test]
fn deep_unary_minus_nesting_errors() {
    let negs = format!("RETURN {}1", "-".repeat(50_000));
    assert!(
        parse_cypher(&negs).has_errors(),
        "deep unary minus should error"
    );
}

#[test]
fn deep_call_nesting_errors() {
    let calls = format!(
        "{}RETURN 1{}",
        "CALL { ".repeat(50_000),
        " }".repeat(50_000)
    );
    assert!(parse_cypher(&calls).has_errors(), "deep CALL should error");
}

/// Regression: a long `UNION` chain *inside* `CALL { … }` recurses through the
/// `parse_call_body ↔ parse_call_union_tail` cycle, which bypasses
/// `parse_statement`. Without its own depth guard that cycle would recurse
/// unbounded and overflow the stack (an unauthenticated DoS). The guard must
/// trip and return a diagnostic instead.
#[test]
fn deep_call_union_nesting_errors() {
    let branches: Vec<&str> = vec!["RETURN 1"; 50_000];
    let body = branches.join(" UNION ");
    let query = format!("CALL {{ {body} }} RETURN 1");
    assert!(
        parse_cypher(&query).has_errors(),
        "deep CALL-UNION chain should error, not overflow"
    );
}

/// The depth guard must not reject ordinary, modestly-nested queries.
#[test]
fn moderate_nesting_is_accepted() {
    let parens = format!("RETURN {}1{}", "(".repeat(32), ")".repeat(32));
    let out = parse_cypher(&parens);
    assert!(!out.has_errors(), "diagnostics: {:?}", out.diagnostics);
    assert!(out.ast.is_some());
}

/// Keyword tokens are accepted as binding names in `AS` position — a
/// deliberate leniency over strict openCypher (which requires backticking
/// reserved words). `RETURN … AS end` was previously rejected at parse.
#[test]
fn keyword_as_alias_parses() {
    for query in [
        "RETURN 1 AS end",
        "RETURN 1 AS count",
        "RETURN 1 AS order",
        "MATCH (n) RETURN n.name AS limit, n.age AS skip",
        "UNWIND [1, 2, 3] AS end RETURN end",
    ] {
        let out = parse_cypher(query);
        assert!(
            !out.has_errors(),
            "{query} — diagnostics: {:?}",
            out.diagnostics
        );
        assert!(out.ast.is_some(), "{query} produced no AST");
    }
}

/// A keyword bound as an alias must remain referenceable downstream as a plain
/// variable — the expression parser treats a stray keyword token in operand
/// position as a variable name.
#[test]
fn keyword_alias_referenced_downstream_parses() {
    for query in [
        "WITH 1 AS end RETURN end",
        "MATCH (n) WITH count(*) AS count WHERE count > 5 RETURN count",
        "UNWIND [1, 2] AS end RETURN end + 1",
    ] {
        let out = parse_cypher(query);
        assert!(
            !out.has_errors(),
            "{query} — diagnostics: {:?}",
            out.diagnostics
        );
        assert!(out.ast.is_some(), "{query} produced no AST");
    }
}

/// The keyword-as-variable fallback must not shadow the real primary meaning of
/// `count(*)`, `exists { … }`, and `all(x IN … )` — those still parse as their
/// dedicated constructs when followed by their delimiter.
#[test]
fn keyword_primaries_still_parse_as_constructs() {
    for query in [
        "MATCH (n) RETURN count(*)",
        "MATCH (n) RETURN count(DISTINCT n)",
        "MATCH (n) WHERE all(x IN [1, 2, 3] WHERE x > 0) RETURN n",
        "MATCH (n) WHERE exists { (n)-[:KNOWS]->() } RETURN n",
    ] {
        let out = parse_cypher(query);
        assert!(
            !out.has_errors(),
            "{query} — diagnostics: {:?}",
            out.diagnostics
        );
        assert!(out.ast.is_some(), "{query} produced no AST");
    }
}
