//! Query-surface parity tests for the PR-2 SPARQL semantic-validation
//! passes (V3 blank-node scope, V4 projection scope, V5 BIND scope,
//! V6 SELECT aliases, SPARQL 1.2 nested-aggregate / duplicated-VALUES),
//! per `docs/contributing/sparql-compliance.md` § Query Surface Parity.
//!
//! Classification: these are validation-only (reject-more) fixes — no new
//! IR or engine capability. Two of the rules carry cross-surface
//! semantics, and the JSON-LD analytical surface deliberately DIVERGES:
//!
//! - **Ungrouped projection under `groupBy`** — SPARQL rejects per spec
//!   (§11); Fluree's JSON-LD surface accepts it and projects the
//!   non-key variable as a per-group LIST (a long-standing Fluree
//!   feature). Pinned below so a future "shared checker" refactor cannot
//!   silently break it.
//! - **`bind` on an already-bound variable** — SPARQL rejects per §10.1;
//!   the JSON-LD surface accepts the shape (Fluree-owned syntax, no
//!   spec obligation). Its acceptance is pinned below as the divergence
//!   record.

use crate::support::{self, context_ex_schema, genesis_ledger, MemoryFluree, MemoryLedger};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

async fn seed_people(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let ctx = context_ex_schema();
    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:alice", "schema:name": "Alice", "schema:age": 50},
            {"@id": "ex:brian", "schema:name": "Brian", "schema:age": 50},
            {"@id": "ex:cam",   "schema:name": "Cam",   "schema:age": 34}
        ]
    });
    fluree
        .insert(ledger0, &insert)
        .await
        .expect("seed insert should succeed")
        .ledger
}

// =============================================================================
// V4 — GROUP BY projection scope
// =============================================================================

/// SPARQL surface: projecting an ungrouped, unaggregated variable is now a
/// hard error (W3C group06/agg09 class).
#[tokio::test]
async fn sparql_groupby_ungrouped_projection_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/grouping-parity-v4:main").await;

    let err = support::query_sparql(
        &fluree,
        &ledger,
        "PREFIX schema: <http://schema.org/> \
         SELECT ?age ?name WHERE { ?s schema:age ?age ; schema:name ?name } GROUP BY ?age",
    )
    .await
    .expect_err("ungrouped projected variable must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("neither a GROUP BY key nor aggregated"),
        "unexpected error: {msg}"
    );
}

/// SPARQL surface: SELECT * with GROUP BY is now a hard error (W3C test_43).
#[tokio::test]
async fn sparql_select_star_with_groupby_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/grouping-parity-star:main").await;

    let err = support::query_sparql(&fluree, &ledger, "SELECT * WHERE { ?s ?p ?o } GROUP BY ?s")
        .await
        .expect_err("SELECT * with GROUP BY must be rejected");
    assert!(
        err.to_string()
            .contains("SELECT * is not allowed with GROUP BY"),
        "unexpected error: {err}"
    );
}

/// SPARQL surface: a spec-valid grouped query still executes.
#[tokio::test]
async fn sparql_groupby_key_and_aggregate_still_works() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/grouping-parity-ok:main").await;

    let result = support::query_sparql(
        &fluree,
        &ledger,
        "PREFIX schema: <http://schema.org/> \
         SELECT ?age (COUNT(?s) AS ?n) WHERE { ?s schema:age ?age } GROUP BY ?age ORDER BY ?age",
    )
    .await
    .expect("valid grouped query");
    let rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(rows, json!([[34, 1], [50, 2]]));
}

/// JSON-LD surface (reviewed divergence): the analytical query surface
/// ACCEPTS an ungrouped selected variable under `groupBy` and projects it
/// as a per-group list. Fluree owns this syntax; the SPARQL-side rejection
/// must not leak into it.
#[tokio::test]
async fn jsonld_groupby_ungrouped_select_projects_grouped_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/grouping-parity-jsonld:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?age", "?name"],
        "where": [{"@id": "?s", "schema:age": "?age", "schema:name": "?name"}],
        "groupBy": ["?age"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("JSON-LD grouped-list projection is a supported Fluree feature");
    let rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    // ?name is not a group key: it comes back as a per-group list.
    let rows = rows.as_array().expect("rows");
    assert_eq!(rows.len(), 2, "one row per ?age group: {rows:?}");
    let group50 = rows
        .iter()
        .find(|r| {
            r.as_array()
                .and_then(|c| c.first())
                .and_then(serde_json::Value::as_i64)
                == Some(50)
        })
        .expect("age-50 group");
    let mut names: Vec<String> = group50.as_array().expect("cols")[1]
        .as_array()
        .expect("grouped ?name must be a list")
        .iter()
        .map(|v| v.as_str().unwrap_or_default().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Alice", "Brian"]);
}

// =============================================================================
// V5 — BIND scope
// =============================================================================

/// SPARQL surface: BIND to a variable already in scope in the group is now
/// a hard error (W3C syntax-BINDscope6 class).
#[tokio::test]
async fn sparql_bind_target_already_in_scope_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/grouping-parity-v5:main").await;

    let err = support::query_sparql(
        &fluree,
        &ledger,
        "PREFIX schema: <http://schema.org/> \
         SELECT ?s ?age WHERE { ?s schema:age ?age . BIND((?age + 1) AS ?age) }",
    )
    .await
    .expect_err("BIND to an in-scope variable must be rejected");
    assert!(
        err.to_string().contains("already in scope"),
        "unexpected error: {err}"
    );
}

/// SPARQL surface: BIND to a fresh variable still executes.
#[tokio::test]
async fn sparql_bind_fresh_variable_still_works() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/grouping-parity-v5ok:main").await;

    let result = support::query_sparql(
        &fluree,
        &ledger,
        "PREFIX schema: <http://schema.org/> \
         SELECT ?name ?next WHERE { ?s schema:name ?name ; schema:age ?age . \
         BIND((?age + 1) AS ?next) } ORDER BY ?name",
    )
    .await
    .expect("BIND to a fresh variable");
    let rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(rows, json!([["Alice", 51], ["Brian", 51], ["Cam", 35]]));
}

/// JSON-LD surface (reviewed divergence): the analytical `bind` on an
/// already-bound variable is ACCEPTED by the Fluree-owned syntax — no
/// validation error — and behaves as a join/constraint on the existing
/// binding rather than a rebind: a conflicting expression yields zero
/// rows, a consistent one keeps them. Pinned so the SPARQL-side V5
/// rejection does not silently change JSON-LD behavior — any tightening
/// here needs its own decision.
#[tokio::test]
async fn jsonld_bind_on_bound_variable_accepted_as_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/grouping-parity-v5jsonld:main").await;
    let ctx = context_ex_schema();

    // Conflicting rebind (?age = ?age + 1 never holds): accepted, 0 rows.
    let conflicting = json!({
        "@context": ctx,
        "select": ["?name", "?age"],
        "where": [
            {"@id": "?s", "schema:name": "?name", "schema:age": "?age"},
            ["bind", "?age", ["expr", ["+", "?age", 1]]]
        ]
    });
    let result = support::query_jsonld(&fluree, &ledger, &conflicting)
        .await
        .expect("JSON-LD bind on a bound variable is accepted (divergence)");
    let rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(rows.as_array().map(Vec::len), Some(0), "{rows:?}");

    // Consistent rebind (?age = ?age + 0 always holds): all rows survive.
    let consistent = json!({
        "@context": ctx,
        "select": ["?name", "?age"],
        "where": [
            {"@id": "?s", "schema:name": "?name", "schema:age": "?age"},
            ["bind", "?age", ["expr", ["+", "?age", 0]]]
        ]
    });
    let result = support::query_jsonld(&fluree, &ledger, &consistent)
        .await
        .expect("JSON-LD bind on a bound variable is accepted (divergence)");
    let rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(rows.as_array().map(Vec::len), Some(3), "{rows:?}");
}

// =============================================================================
// V6 — SELECT aliases, V3 — blank-node scope, SPARQL 1.2 checks
// =============================================================================

/// SPARQL surface: duplicate AS alias / alias already in scope are now hard
/// errors (W3C test_45 / test_65 class).
#[tokio::test]
async fn sparql_select_alias_violations_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/grouping-parity-v6:main").await;

    let err = support::query_sparql(&fluree, &ledger, "SELECT (1 AS ?x) (2 AS ?x) WHERE {}")
        .await
        .expect_err("duplicate SELECT alias must be rejected");
    assert!(
        err.to_string().contains("assigned more than once"),
        "unexpected error: {err}"
    );

    let err = support::query_sparql(
        &fluree,
        &ledger,
        "PREFIX schema: <http://schema.org/> \
         SELECT ((?age + 1) AS ?age) WHERE { ?s schema:age ?age }",
    )
    .await
    .expect_err("SELECT alias shadowing a pattern variable must be rejected");
    assert!(
        err.to_string().contains("already in scope"),
        "unexpected error: {err}"
    );
}

/// SPARQL surface: blank-node label reuse across BGP scopes is now a hard
/// error (W3C blabel-cross-* class). No JSON-LD analogue — JSON-LD has no
/// blank-node label syntax in WHERE patterns (SPARQL-surface-only rule).
#[tokio::test]
async fn sparql_blank_node_cross_scope_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/grouping-parity-v3:main").await;

    let err = support::query_sparql(
        &fluree,
        &ledger,
        "PREFIX schema: <http://schema.org/> \
         SELECT ?v WHERE { _:a schema:age ?v OPTIONAL { _:a schema:name ?n } }",
    )
    .await
    .expect_err("blank-node label reuse across scopes must be rejected");
    assert!(
        err.to_string().contains("basic graph pattern"),
        "unexpected error: {err}"
    );
}

/// SPARQL surface: SPARQL 1.2 negative-syntax checks surface as hard
/// errors through the API. Nested aggregates are not expressible in the
/// JSON-LD aggregate syntax (single-function S-expressions); the JSON-LD
/// `values` clause is keyed by variable name (a JSON map/array), where a
/// duplicate is a syntax impossibility — both SPARQL-surface-only.
#[tokio::test]
async fn sparql_12_syntax_checks_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/grouping-parity-s12:main").await;

    let err = support::query_sparql(&fluree, &ledger, "SELECT (COUNT(COUNT(*)) AS ?c) WHERE {}")
        .await
        .expect_err("nested aggregate must be rejected");
    assert!(
        err.to_string().contains("cannot be nested"),
        "unexpected error: {err}"
    );

    let err = support::query_sparql(
        &fluree,
        &ledger,
        "SELECT * WHERE { VALUES (?a ?a) { (1 1) } }",
    )
    .await
    .expect_err("duplicated VALUES variable must be rejected");
    assert!(
        err.to_string().contains("listed more than once"),
        "unexpected error: {err}"
    );
}
