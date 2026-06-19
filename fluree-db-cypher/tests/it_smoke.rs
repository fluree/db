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
