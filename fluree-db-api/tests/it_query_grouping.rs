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

/// JSON-LD surface, V5/V6 analogue: a `bind` — or a scalar select alias,
/// which desugars to one — onto an already-bound variable is REJECTED.
///
/// This replaces a pin that recorded the opposite. That pin's comment said
/// "any tightening here needs its own decision", which read as a product
/// decision reserving the behaviour; the archaeology says otherwise. The
/// comment came from `e1f1380eb6`, whose body describes the two JSON-LD pins
/// as "deliberately NOT changed by this PR" — PR scope, not intent — and
/// describes this one as "empirically behaving" as a join/constraint, i.e.
/// observed rather than designed. The behaviour underneath it is
/// `BindOperator`'s clobber prevention, whose every line arrived wholesale in
/// `5985d0f011` "fluree v4 baseline" and was never proposed or argued. No
/// issue, design doc or review comment discusses it, and a repo-wide scan
/// found no query relying on it outside this test. So the deferral was
/// procedural, and this lands it.
///
/// Both spellings are covered, because they fail identically and only one of
/// them is the real analogue of the Cypher bug (#1857) this follows from:
/// `(as <expr> ?v)` is the alias/projection form, `["bind", "?v", …]` the
/// WHERE-clause form. `filter` remains the way to express the constraint
/// reading this used to back into.
#[tokio::test]
async fn jsonld_bind_on_bound_variable_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/grouping-parity-v5jsonld:main").await;
    let ctx = context_ex_schema();

    // WHERE-clause bind onto a bound variable — the V5 analogue. Previously
    // accepted and silently 0 rows (the conflicting `?age = ?age + 1` case).
    let where_bind = json!({
        "@context": ctx,
        "select": ["?name", "?age"],
        "where": [
            {"@id": "?s", "schema:name": "?name", "schema:age": "?age"},
            ["bind", "?age", ["expr", ["+", "?age", 1]]]
        ]
    });
    let err = support::query_jsonld(&fluree, &ledger, &where_bind)
        .await
        .expect_err("bind onto a bound variable must be rejected");
    assert!(
        err.to_string()
            .contains("bind target ?age is already bound"),
        "unexpected error: {err}"
    );

    // Select alias onto a bound variable — the V6 analogue, and the shape that
    // actually mirrors Cypher's `RETURN expr AS v`. Previously accepted and
    // silently 0 rows, even in the consistent case.
    let select_alias = json!({
        "@context": ctx,
        "select": ["?name", "(as (+ ?age 0) ?age)"],
        "where": [{"@id": "?s", "schema:name": "?name", "schema:age": "?age"}]
    });
    let err = support::query_jsonld(&fluree, &ledger, &select_alias)
        .await
        .expect_err("select alias onto a bound variable must be rejected");
    assert!(
        err.to_string()
            .contains("select alias ?age is already bound"),
        "the message must name the select spelling, not the where one: {err}"
    );

    // The consistent case is gone too, deliberately: it only "worked" because
    // `?age + 0` happened to equal `?age`, which is the filter semantics the
    // old pin recorded rather than a projection. `filter` is the replacement
    // and still expresses it.
    let replacement = json!({
        "@context": ctx,
        "select": ["?name", "?age"],
        "where": [
            {"@id": "?s", "schema:name": "?name", "schema:age": "?age"},
            ["filter", "(= ?age (+ ?age 0))"]
        ]
    });
    let result = support::query_jsonld(&fluree, &ledger, &replacement)
        .await
        .expect("filter is the supported spelling for the constraint reading");
    let rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(rows.as_array().map(Vec::len), Some(3), "{rows:?}");

    // Binding to a FRESH variable is untouched — the guard rejects collisions,
    // not computation.
    let fresh = json!({
        "@context": ctx,
        "select": ["?name", "?next"],
        "where": [
            {"@id": "?s", "schema:name": "?name", "schema:age": "?age"},
            ["bind", "?next", ["expr", ["+", "?age", 1]]]
        ]
    });
    let result = support::query_jsonld(&fluree, &ledger, &fresh)
        .await
        .expect("bind to a fresh variable still works");
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

/// Fan-in fixture for the grouped-list dedup regression: three `?a`s collapse
/// onto one `?b`, so `?a` dying mid-chain is what WHERE-level early dedup
/// would otherwise use to collapse rows.
async fn seed_fanin(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let ctx = context_ex_schema();
    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:a1", "ex:p1": {"@id": "ex:b1"}},
            {"@id": "ex:a2", "ex:p1": {"@id": "ex:b1"}},
            {"@id": "ex:a3", "ex:p1": {"@id": "ex:b1"}},
            {"@id": "ex:a4", "ex:p1": {"@id": "ex:b2"}},
            {"@id": "ex:b1", "ex:p2": 10},
            {"@id": "ex:b2", "ex:p2": [20, 30]}
        ]
    });
    fluree
        .insert(ledger0, &insert)
        .await
        .expect("seed insert should succeed")
        .ledger
}

/// The grouped list is a duplicate-SENSITIVE consumer: a duplicate-insensitive
/// aggregate (`max`) licenses WHERE-level early dedup for itself, but a
/// non-key variable projected alongside it still comes back as a per-group
/// multiset. Companion to `jsonld_groupby_ungrouped_select_projects_grouped_list`,
/// which has no fan-in and so cannot catch this.
#[tokio::test]
async fn jsonld_grouped_list_keeps_duplicates_under_insensitive_aggregate() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_fanin(&fluree, "query/grouped-list-dedup:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?x", "(as (max ?x) ?m)"],
        "where": [{"@id": "?a", "ex:p1": "?b"}, {"@id": "?b", "ex:p2": "?x"}]
    });
    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(rows, json!([[[10, 10, 10, 20, 30], 30]]));
}

/// A dedup-only `GROUP BY` has an EMPTY aggregate set, so an
/// "all aggregates are duplicate-insensitive" check passes vacuously. The
/// grouped list must still keep its duplicates.
#[tokio::test]
async fn jsonld_grouped_list_keeps_duplicates_without_aggregation() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_fanin(&fluree, "query/grouped-list-no-agg:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?b", "?x"],
        "where": [{"@id": "?a", "ex:p1": "?b"}, {"@id": "?b", "ex:p2": "?x"}],
        "groupBy": ["?b"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(rows, json!([["ex:b1", [10, 10, 10]], ["ex:b2", [20, 30]]]));
}

/// Review round (bplatz) on the JSON-LD guard: the regression it introduced,
/// and the two scopes it did not see.
#[tokio::test]
async fn jsonld_bind_guard_review_round() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/grouping-parity-v5review:main").await;
    let ctx = context_ex_schema();

    // REGRESSION FIX. Reusing one node-map metadata variable across two
    // properties is the natural spelling of "both asserted in the same
    // transaction", and it binds onto an already-bound `?t` by design. The
    // guard's doc claimed these binds were `?__`-prefixed and exempt; they
    // carry the author's own name, so the guard rejected a working query.
    let shared_t = json!({
        "@context": ctx,
        "select": ["?name", "?t"],
        "where": [{"@id": "?s",
                   "schema:name": {"@value": "?name", "@t": "?t"},
                   "schema:age":  {"@value": "?age",  "@t": "?t"}}]
    });
    let result = support::query_jsonld(&fluree, &ledger, &shared_t)
        .await
        .expect("a shared metadata variable is a join, and must keep working");
    let rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(rows.as_array().map(Vec::len), Some(3), "{rows:?}");

    // …while a genuine collision on the same shape of query is still refused,
    // so the exemption is the metadata join and not a blanket pass.
    let genuine = json!({
        "@context": ctx,
        "select": ["?name"],
        "where": [{"@id": "?s", "schema:name": "?name", "schema:age": "?age"},
                  ["bind", "?age", ["expr", ["+", "?age", 1]]]]
    });
    support::query_jsonld(&fluree, &ledger, &genuine)
        .await
        .expect_err("a user bind onto a bound variable is still rejected");

    // Nested subquery: nothing checked it, so the defect survived one level
    // down and returned an unexplained empty result.
    let nested = json!({
        "@context": ctx,
        "select": ["?age"],
        "where": [["query", {"@context": ctx,
                             "select": ["?age"],
                             "where": [{"@id": "?s2", "schema:age": "?age"},
                                       ["bind", "?age", ["expr", ["+", "?age", 1]]]]}]]
    });
    let err = support::query_jsonld(&fluree, &ledger, &nested)
        .await
        .expect_err("a nested select must be guarded like the top level");
    assert!(err.to_string().contains("bind target ?age"), "{err}");

    // `unwind` onto a bound variable: the Cypher twin of this exact shape is
    // rejected in the same PR, so the surfaces should agree.
    let unwind = json!({
        "@context": ctx,
        "select": ["?name", "?age"],
        "where": [{"@id": "?s", "schema:name": "?name", "schema:age": "?age"},
                  ["unwind", "?age", ["expr", ["range", 1, 3]]]]
    });
    let err = support::query_jsonld(&fluree, &ledger, &unwind)
        .await
        .expect_err("unwind onto a bound variable must be rejected");
    assert!(err.to_string().contains("unwind target ?age"), "{err}");
}
