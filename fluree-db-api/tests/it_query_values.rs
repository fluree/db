//! VALUES query integration tests
//!
//! Uses explicit `@context` on every insert/query.
//!
//! Notes:
//! - Federated query behavior (`query-connection` + `:from`) is covered.
//! - VALUES inside multi-pattern OPTIONAL is supported.

use crate::support;
use crate::support::{
    context_ex_schema, genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger,
};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

async fn seed_values_dataset(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    // Local explicit context: mirror the values-test usage (flur.ee + default context + ex).
    // For Rust, we keep it explicit and minimal for what these tests need.
    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.com/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:brian","schema:name":"Brian","schema:email":"brian@example.org","schema:age":50,"ex:favNums":7},
            {"@id":"ex:alice","schema:name":"Alice","schema:email":"alice@example.org","schema:age":50,"ex:favNums":[42,76,9],"ex:friend":[{"@id":"ex:brian"}]},
            {"@id":"ex:cam","schema:name":"Cam","schema:email":"cam@example.org","schema:age":34,"ex:favNums":[5,10],"ex:friend":[{"@id":"ex:alice"},{"@id":"ex:brian"}]},
            {"@id":"ex:liam","schema:name":"Liam","schema:email":"liam@example.org","schema:age":13,"ex:favNums":[42,11],"ex:friend":[{"@id":"ex:alice"},{"@id":"ex:brian"},{"@id":"ex:cam"}]},
            {"@id":"ex:nikola",
             "schema:name":"Nikola",
             "ex:greeting":[{"@value":"Здраво","@language":"sb"},{"@value":"Hello","@language":"en"}],
             "ex:birthday":{"@value":"2000-01-01","@type":"xsd:datetime"},
             "ex:cool":true}
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("seed insert should succeed")
        .ledger
}

#[tokio::test]
async fn values_top_level_no_where_multiple_vars() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let query = json!({
        "@context": context_ex_schema(),
        "select": ["?foo", "?bar"],
        "values": [["?foo", "?bar"],
                   [["foo1","bar1"],["foo2","bar2"],["foo3","bar3"]]]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["foo1", "bar1"],
            ["foo2", "bar2"],
            ["foo3", "bar3"]
        ]))
    );
}

#[tokio::test]
async fn values_top_level_no_where_single_var() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let query = json!({
        "@context": context_ex_schema(),
        "select": "?foo",
        "values": ["?foo", ["foo1","foo2","foo3"]]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!(["foo1", "foo2", "foo3"]))
    );
}

#[tokio::test]
async fn values_top_level_iri_values_constrain_where() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.com/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let query = json!({
        "@context": ctx,
        "select": ["?name","?email"],
        "values": ["?s", [
            {"@value":"ex:brian","@type":"@id"},
            {"@value":"ex:cam","@type":"@id"}
        ]],
        "where": [
            {"@id":"?s","schema:name":"?name"},
            {"@id":"?s","schema:email":"?email"}
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["Brian", "brian@example.org"],
            ["Cam", "cam@example.org"]
        ]))
    );
}

#[tokio::test]
async fn values_equivalent_iri_forms_var_in_id_map() {
    // Mirrors the three "equivalent syntactic forms" checks.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.com/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    // baseline: IRI literal in pattern
    let q1 = json!({
        "@context": ctx,
        "where": [{"@id":"?s","ex:friend":{"@id":"ex:alice"}}],
        "select": "?s"
    });

    // variable via VALUES
    let q2 = json!({
        "@context": ctx,
        "values": ["?friend", [{"@value":"ex:alice","@type":"@id"}]],
        "where": [{"@id":"?s","ex:friend":"?friend"}],
        "select": "?s"
    });

    // variable inside id-map
    let q3 = json!({
        "@context": ctx,
        "values": ["?friend", [{"@value":"ex:alice","@type":"@id"}]],
        "where": [{"@id":"?s","ex:friend":{"@id":"?friend"}}],
        "select": "?s"
    });

    let r1 = support::query_jsonld(&fluree, &ledger, &q1)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let r2 = support::query_jsonld(&fluree, &ledger, &q2)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let r3 = support::query_jsonld(&fluree, &ledger, &q3)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();

    assert_eq!(
        normalize_rows(&r1),
        normalize_rows(&json!(["ex:cam", "ex:liam"]))
    );
    assert_eq!(
        normalize_rows(&r2),
        normalize_rows(&json!(["ex:cam", "ex:liam"]))
    );
    assert_eq!(
        normalize_rows(&r3),
        normalize_rows(&json!(["ex:cam", "ex:liam"]))
    );
}

#[tokio::test]
async fn values_where_clause_keyword_single_var() {
    // VALUES nested in WHERE: ["values", ["?s", [{@type:"@id",@value:"ex:cam"}, ...]]]
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.com/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let query = json!({
        "@context": ctx,
        "select": ["?name","?email"],
        "where": [
            {"@id":"?s","schema:name":"?name"},
            {"@id":"?s","schema:email":"?email"},
            ["values", ["?s", [
                {"@type":"@id","@value":"ex:cam"},
                {"@type":"@id","@value":"ex:brian"}
            ]]]
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["Brian", "brian@example.org"],
            ["Cam", "cam@example.org"]
        ]))
    );
}

#[tokio::test]
async fn values_nested_under_optional_clause() {
    // Tests multi-pattern OPTIONAL containing both triple patterns and VALUES
    // The OPTIONAL contains: triple pattern + VALUES clause
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.com/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    // Query all users, optionally get the name and cool status for Nikola only
    let query = json!({
        "@context": ctx,
        "select": ["?s", "?name", "?cool"],
        "where": [
            {"@id": "?s", "schema:email": "?email"},
            ["optional",
                {"@id": "?s", "schema:name": "?name", "ex:cool": "?cool"},
                ["values", ["?s", [{"@type": "@id", "@value": "ex:nikola"}]]]
            ]
        ],
        "orderBy": "?s"
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    // Only nikola matches the OPTIONAL (VALUES constrains to nikola, and nikola has ex:cool)
    // Other users have email so they match the required pattern, but don't match the OPTIONAL
    // because VALUES constrains ?s to nikola only
    assert_eq!(
        json_rows,
        json!([
            ["ex:alice", null, null],
            ["ex:brian", null, null],
            ["ex:cam", null, null],
            ["ex:liam", null, null]
        ])
    );
}

#[tokio::test]
async fn values_match_meta_language_tag() {
    // Scenario: match meta (language tag) => ["ex:nikola"]
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.com/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let query = json!({
        "@context": ctx,
        "select": "?s",
        "where": [
            {"@id":"?s","ex:greeting":"?greet"},
            ["values", ["?greet", [{"@value":"Здраво","@language":"sb"}]]]
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(json_rows, json!(["ex:nikola"]));
}

#[tokio::test]
async fn values_with_empty_solution_seed() {
    // Scenario: VALUES first, then match by name.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.com/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let query = json!({
        "@context": ctx,
        "where": [
            ["values", ["?name", ["Liam", "Cam"]]],
            {"@id":"?s","schema:name":"?name"}
        ],
        "select": ["?s","?name"]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([["ex:cam", "Cam"], ["ex:liam", "Liam"]]))
    );
}

#[tokio::test]
async fn values_federated_query_connection_from_two_ledgers() {
    // Scenario: federated VALUES across two ledgers via query_connection.
    let fluree = FlureeBuilder::memory().build_memory();
    let _ = seed_values_dataset(&fluree, "values-test:main").await;

    // Seed second ledger with a single person.
    let other_ledger0 = genesis_ledger(&fluree, "other-ledger:main");
    let other_insert = json!({
        "@context": {
            "schema": "http://schema.org/",
            "ex": "http://example.com/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {"@id":"ex:khris","schema:name":"Khris"}
        ]
    });
    let _ = fluree
        .insert(other_ledger0, &other_insert)
        .await
        .expect("insert other-ledger");

    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.com/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let query = json!({
        "@context": ctx,
        "from": ["values-test:main", "other-ledger:main"],
        "select": "?name",
        "where": [
            {"@id":"?s","schema:name":"?name"},
            ["values", ["?s", [
                {"@type":"@id","@value":"ex:nikola"},
                {"@type":"@id","@value":"ex:khris"}
            ]]]
        ],
        "orderBy": "?name"
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree.ledger("values-test:main").await.expect("ledger");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!(["Khris", "Nikola"]))
    );
}

// --- single-row VALUES folded into a bound object -------------------------
//
// `VALUES ?o { <iri> }` is the constant `<iri>`, so the planner rewrites the
// object position of the triples it constrains (see
// `where_plan::inline_singleton_values_objects`). These tests pin the result
// equivalence that licenses the rewrite, and the shapes deliberately left out
// of it.

const VALUES_PREFIXES: &str = "PREFIX ex: <http://example.com/>\n\
     PREFIX schema: <http://schema.org/>\n";

async fn sparql_rows(
    fluree: &MemoryFluree,
    ledger: &MemoryLedger,
    body: &str,
) -> Vec<serde_json::Value> {
    let query = format!("{VALUES_PREFIXES}{body}");
    let result = support::query_sparql(fluree, ledger, &query)
        .await
        .expect("sparql query should succeed");
    normalize_rows(&result.to_jsonld(&ledger.snapshot).expect("to_jsonld"))
}

#[tokio::test]
async fn values_object_iri_matches_inlined_constant() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let with_values = sparql_rows(
        &fluree,
        &ledger,
        "SELECT ?s ?name WHERE { VALUES ?f { ex:brian } \
         ?s ex:friend ?f . ?s schema:name ?name }",
    )
    .await;
    let inlined = sparql_rows(
        &fluree,
        &ledger,
        "SELECT ?s ?name WHERE { ?s ex:friend ex:brian . ?s schema:name ?name }",
    )
    .await;

    assert_eq!(with_values, inlined);
    assert_eq!(
        with_values,
        normalize_rows(&json!([
            ["ex:alice", "Alice"],
            ["ex:cam", "Cam"],
            ["ex:liam", "Liam"]
        ]))
    );
}

#[tokio::test]
async fn values_object_var_stays_projectable_after_inlining() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    // ?f no longer appears in any triple once the constant is folded in, so it
    // has to come back from the retained VALUES pattern.
    let rows = sparql_rows(
        &fluree,
        &ledger,
        "SELECT ?s ?f WHERE { VALUES ?f { ex:brian } \
         ?s ex:friend ?f . ?s schema:age ?age }",
    )
    .await;

    assert_eq!(
        rows,
        normalize_rows(&json!([
            ["ex:alice", "ex:brian"],
            ["ex:cam", "ex:brian"],
            ["ex:liam", "ex:brian"]
        ]))
    );
}

#[tokio::test]
async fn values_object_iri_still_filters_a_wide_star() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let with_values = sparql_rows(
        &fluree,
        &ledger,
        "SELECT ?s ?name ?age WHERE { VALUES ?f { ex:alice } \
         ?s ex:friend ?f . ?s schema:name ?name . ?s schema:age ?age . \
         OPTIONAL { ?s schema:email ?email } }",
    )
    .await;
    let inlined = sparql_rows(
        &fluree,
        &ledger,
        "SELECT ?s ?name ?age WHERE { ?s ex:friend ex:alice . \
         ?s schema:name ?name . ?s schema:age ?age . \
         OPTIONAL { ?s schema:email ?email } }",
    )
    .await;

    assert_eq!(with_values, inlined);
    assert_eq!(
        with_values,
        normalize_rows(&json!([["ex:cam", "Cam", 34], ["ex:liam", "Liam", 13]]))
    );
}

#[tokio::test]
async fn values_object_iri_keeps_minus_sharing_the_variable() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    // MINUS is scoped by the variables it shares with the left side. Folding
    // the constant into the MINUS group would remove ?f from it and turn the
    // MINUS into a no-op, so nested groups are left alone.
    let rows = sparql_rows(
        &fluree,
        &ledger,
        "SELECT ?s WHERE { VALUES ?f { ex:brian } ?s ex:friend ?f . \
         MINUS { ?other ex:friend ?f } }",
    )
    .await;

    assert_eq!(rows, normalize_rows(&json!([])));
}

#[tokio::test]
async fn multi_row_values_object_is_a_set_not_a_constant() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let rows = sparql_rows(
        &fluree,
        &ledger,
        "SELECT ?s ?f WHERE { VALUES ?f { ex:brian ex:cam } \
         ?s ex:friend ?f . ?s schema:name ?name }",
    )
    .await;

    assert_eq!(
        rows,
        normalize_rows(&json!([
            ["ex:alice", "ex:brian"],
            ["ex:cam", "ex:brian"],
            ["ex:liam", "ex:brian"],
            ["ex:liam", "ex:cam"]
        ]))
    );
}

#[tokio::test]
async fn values_object_literal_matches_inlined_literal() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    // Literal cells are NOT folded (datatype/language matching stays with the
    // scan layer); the answer must still match the inlined form.
    let with_values = sparql_rows(
        &fluree,
        &ledger,
        "SELECT ?s WHERE { VALUES ?age { 50 } ?s schema:age ?age . ?s schema:name ?name }",
    )
    .await;
    let inlined = sparql_rows(
        &fluree,
        &ledger,
        "SELECT ?s WHERE { ?s schema:age 50 . ?s schema:name ?name }",
    )
    .await;

    assert_eq!(with_values, inlined);
    assert_eq!(
        with_values,
        normalize_rows(&json!([["ex:alice"], ["ex:brian"]]))
    );
}

#[tokio::test]
async fn chained_union_after_a_broad_scan_returns_every_branch() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    // A 3-way UNION nests as Union([[Union([[A],[B]])],[C]]); the outer branch
    // holding the inner UNION has no triples of its own, and used to be costed
    // as an unknown property scan. Ordering must not change the answer.
    let rows = sparql_rows(
        &fluree,
        &ledger,
        "SELECT ?s ?name WHERE { ?s schema:name ?name . \
         { ?s ex:friend ex:brian } UNION { ?s ex:friend ex:alice } \
         UNION { ?s ex:friend ex:cam } }",
    )
    .await;

    assert_eq!(
        rows,
        normalize_rows(&json!([
            ["ex:alice", "Alice"],
            ["ex:cam", "Cam"],
            ["ex:cam", "Cam"],
            ["ex:liam", "Liam"],
            ["ex:liam", "Liam"],
            ["ex:liam", "Liam"]
        ]))
    );
}

#[tokio::test]
async fn trailing_values_keeps_minus_correlated() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    // A VALUES written AFTER the MINUS still binds ?f for the whole group, so
    // the anti-join is correlated on ?f and removes every row. Folding the
    // constant into the triple would take ?f out of the MINUS's order-
    // preservation set, letting it float ahead of the VALUES and degenerate
    // into a disjoint-domain no-op.
    let rows = sparql_rows(
        &fluree,
        &ledger,
        "SELECT ?s WHERE { ?s ex:friend ?f . \
         MINUS { ?other ex:friend ?f } VALUES ?f { ex:brian } }",
    )
    .await;

    assert_eq!(rows, normalize_rows(&json!([])));
}

#[tokio::test]
async fn trailing_values_keeps_not_exists_correlated() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let rows = sparql_rows(
        &fluree,
        &ledger,
        "SELECT ?s WHERE { ?s ex:friend ?f . \
         FILTER NOT EXISTS { ?other ex:friend ?f . FILTER(?other != ?s) } \
         VALUES ?f { ex:brian } }",
    )
    .await;

    assert_eq!(rows, normalize_rows(&json!([])));
}
