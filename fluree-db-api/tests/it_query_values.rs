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

// --- a trailing VALUES over an OPTIONAL-bound variable (#1690) -------------
//
// `{ P . OPTIONAL { O } . VALUES ?v { … } }` is `Join(LeftJoin(P, O), V)`.
// Hoisting the VALUES to seed position makes it `LeftJoin(Join(P, V), O)` —
// the left join then matches an already-bound `?v` and, dropping nothing, lets
// every driving row out carrying the seeded value. The answer is not merely
// too big: rows report a binding the data never had for them.
//
// The correct answer is NOT the FILTER rewrite. Per SPARQL 1.1 §18.2.4 this is
// a JOIN, and a solution whose OPTIONAL left `?v` UNBOUND is compatible with
// every VALUES row: it SURVIVES and ADOPTS the value. W3C
// `sparql11/bindings/values07` pins exactly this three-way outcome, and the
// fixture below reproduces it:
//
//   * `ex:cam` / `ex:liam` — OPTIONAL bound `?f` to `ex:alice`     → KEPT
//   * `ex:alice`           — OPTIONAL bound `?f` to `ex:brian`     → DROPPED
//   * `ex:brian` / `ex:nikola` — no `ex:friend` at all, `?f` UNBOUND
//                                                     → KEPT, ADOPTS `ex:alice`

const OPTIONAL_THEN_VALUES: &str = "SELECT ?s ?f WHERE { ?s schema:name ?name . \
     OPTIONAL { ?s ex:friend ?f } VALUES ?f { ex:alice } }";

/// The spec answer for [`OPTIONAL_THEN_VALUES`] — two kept-as-bound rows and
/// two unbound-and-adopted rows. `ex:alice` is absent: her only `ex:friend` is
/// `ex:brian`, so the VALUES join drops that solution.
fn optional_then_values_rows() -> serde_json::Value {
    json!([
        ["ex:cam", "ex:alice"],
        ["ex:liam", "ex:alice"],
        ["ex:brian", "ex:alice"],
        ["ex:nikola", "ex:alice"]
    ])
}

/// The row the hoist invented: `ex:alice` reported as her own friend.
fn fabricated_row() -> Vec<serde_json::Value> {
    normalize_rows(&json!([["ex:alice", "ex:alice"]]))
}

#[tokio::test]
async fn trailing_values_over_an_optional_var_joins_instead_of_seeding() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let rows = sparql_rows(&fluree, &ledger, OPTIONAL_THEN_VALUES).await;

    // `ex:alice`'s only friend is `ex:brian`, so no solution may report her
    // with `?f = ex:alice`. That row is the fabricated binding the hoist
    // invented — it used to be here, and its absence is the whole fix.
    let fabricated = fabricated_row();
    assert!(
        !rows.iter().any(|r| fabricated.contains(r)),
        "ex:alice has no ex:friend ex:alice; the VALUES must not fabricate one: {rows:?}"
    );

    // `ex:cam`/`ex:liam` were bound and compatible — kept with the binding the
    // data gave them. `ex:brian`/`ex:nikola` have no `ex:friend` at all, so the
    // OPTIONAL left `?f` UNBOUND: compatible with every VALUES row, therefore
    // kept AND bound (§18.2.4). A "drop the unbound rows" fix loses those two.
    assert_eq!(rows, normalize_rows(&optional_then_values_rows()));
}

/// The ops of a query's planned physical plan, root first.
async fn physical_plan_ops(ledger: &MemoryLedger, body: &str) -> Vec<String> {
    let query = format!("{VALUES_PREFIXES}{body}");
    let plan = fluree_db_api::explain::explain_sparql(&ledger.snapshot, &query)
        .await
        .expect("explain should succeed");
    fn walk(node: &serde_json::Value, out: &mut Vec<String>) {
        if let Some(op) = node.get("op").and_then(serde_json::Value::as_str) {
            out.push(op.to_string());
        }
        if let Some(children) = node.get("children").and_then(serde_json::Value::as_array) {
            for edge in children {
                if let Some(child) = edge.get("node") {
                    walk(child, out);
                }
            }
        }
    }
    let mut ops = Vec::new();
    walk(&plan["plan"]["physical"], &mut ops);
    ops
}

#[tokio::test]
async fn a_union_binding_the_var_does_not_suppress_the_barrier() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    // One UNION away from the OPTIONAL-only shape. `Union::produced_vars` unions
    // its branches, so treating it as "already bound" recorded `?f` as
    // required-bound and SUPPRESSED the barrier for the sibling OPTIONAL that
    // genuinely introduces it — the VALUES went back to seed position. The
    // planner now accumulates MUST-bind vars (branch intersection for a UNION),
    // so `?f` is correctly not required-bound and the barrier fires.
    //
    // Asserted on the PLAN, not the rows, and deliberately so: this query's
    // answer is also wrong for a SECOND, independent reason that this PR does
    // not touch. `{ ?s schema:name ?name . { {?s ex:friend ?f} UNION
    // {?s schema:age ?age} } OPTIONAL { ?s ex:friend ?f } }` — no VALUES
    // anywhere — returns 10 rows where §18.2.4 says 13, identical to the same
    // query with the OPTIONAL deleted: the left join is a no-op over the rows a
    // UNION branch left `?f` unbound on, so it never binds `?f = ex:brian` for
    // `ex:alice`. Pinning rows here would pin that unrelated defect's output.
    // The row-level §18.2.4 contract is pinned by
    // `trailing_values_over_an_optional_var_joins_instead_of_seeding` and
    // `a_subquery_exposing_an_optional_bound_var_still_joins`, whose inputs are
    // correct.
    let ops = physical_plan_ops(
        &ledger,
        "SELECT ?s ?f WHERE { ?s schema:name ?name . \
         { { ?s ex:friend ?f } UNION { ?s schema:age ?age } } \
         OPTIONAL { ?s ex:friend ?f } VALUES ?f { ex:alice } }",
    )
    .await;

    let values_at = ops
        .iter()
        .position(|o| o == "ValuesOperator")
        .unwrap_or_else(|| panic!("the VALUES must be applied somewhere: {ops:?}"));
    let optional_at = ops
        .iter()
        .position(|o| o.contains("Optional"))
        .unwrap_or_else(|| panic!("the OPTIONAL must survive planning: {ops:?}"));
    assert!(
        values_at < optional_at,
        "a UNION branch binding ?f must not suppress the barrier: {ops:?}"
    );
}

#[tokio::test]
async fn an_undef_values_column_does_not_suppress_the_barrier() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    // The same suppressor class as the UNION above, reached through a leading
    // VALUES instead. `Values::produced_vars` is its whole variable list, but
    // both surfaces lower `UNDEF` to `Binding::Unbound`, so an all-UNDEF `?f`
    // column binds `?f` in no row at all while reading as "required-bound" —
    // the barrier didn't fire and the trailing `VALUES ?f` went back to seed
    // position. `must_bind_vars` now counts only the columns with no `Unbound`
    // cell.
    //
    // Not a synthetic corner: an UNDEF placeholder column is the parameterized
    // -query idiom #1690 names as its motivating usage.
    //
    // Asserted on the PLAN, not the rows, for the same reason as
    // `a_union_binding_the_var_does_not_suppress_the_barrier` — this query's
    // answer is wrong for the second, independent reason filed as #1713.
    // Without the arm the plan is
    // `ProjectOperator > OptionalOperator > NestedLoopJoin > Values > Values`
    // and the answer is the 5-row fabrication (`["ex:alice","ex:alice"]`, whose
    // only `ex:friend` is `ex:brian`; the reference answer is 4). With it the
    // trailing VALUES lands above the left join as it must — and the answer
    // becomes 8 rows still carrying that fabricated one, because #1713 leaves
    // `?f` Null on every row the OPTIONAL matched. Pinning rows here would pin
    // that defect's output.
    let ops = physical_plan_ops(
        &ledger,
        "SELECT ?s ?f WHERE { ?s schema:name ?name . \
         VALUES (?s ?f) { (ex:alice UNDEF) (ex:cam UNDEF) (ex:liam UNDEF) \
         (ex:brian UNDEF) (ex:nikola UNDEF) } \
         OPTIONAL { ?s ex:friend ?f } VALUES ?f { ex:alice } }",
    )
    .await;

    let optional_at = ops
        .iter()
        .position(|o| o.contains("Optional"))
        .unwrap_or_else(|| panic!("the OPTIONAL must survive planning: {ops:?}"));
    // Pre-order walk of the physical tree, so an earlier index is HIGHER in the
    // plan: the trailing VALUES applying above the left join is the fix.
    let values_at = ops
        .iter()
        .position(|o| o == "ValuesOperator")
        .unwrap_or_else(|| panic!("the VALUES must be applied somewhere: {ops:?}"));
    assert!(
        values_at < optional_at,
        "an all-UNDEF VALUES column must not suppress the barrier: {ops:?}"
    );
}

#[tokio::test]
async fn a_subquery_exposing_an_optional_bound_var_still_joins() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    // `Subquery::produced_vars` is the SELECT list, so `?f` read as
    // unconditionally bound even though the body binds it only in an OPTIONAL —
    // the same may-bind class as the UNION above, reached through the
    // subquery's outward schema. Unlike the UNION shape this one has a correct
    // input, so it pins the ANSWER: `ex:brian` has no `ex:friend` at all, so the
    // subquery emits him with `?f` unbound and he survives and adopts.
    //
    // HONEST SCOPE: this shape is already correct without the barrier — the
    // correlated-subquery deferral keeps the VALUES below the subquery, and this
    // test stays green under a barrier-disabling mutation. It is a regression
    // pin on the §18.2.4 answer for the may-bind class, not evidence about the
    // barrier.
    let rows = sparql_rows(
        &fluree,
        &ledger,
        "SELECT ?s ?f WHERE { ?s schema:name ?name . \
         { SELECT ?s ?f WHERE { ?s schema:email ?e . OPTIONAL { ?s ex:friend ?f } } } \
         VALUES ?f { ex:alice } }",
    )
    .await;

    let fabricated = fabricated_row();
    assert!(
        !rows.iter().any(|r| fabricated.contains(r)),
        "ex:alice's only ex:friend is ex:brian; no row may report otherwise: {rows:?}"
    );
    assert_eq!(
        rows,
        normalize_rows(&json!([
            ["ex:brian", "ex:alice"], // unbound in the subquery, adopts
            ["ex:cam", "ex:alice"],   // bound to ex:alice, kept
            ["ex:liam", "ex:alice"]   // bound to ex:alice, kept
        ]))
    );
}

#[tokio::test]
async fn in_group_values_matches_the_post_query_spelling() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    // The two spellings of the same query. The post-query form (`} VALUES …`)
    // has always planned correctly; it is the reference answer.
    let in_group = sparql_rows(&fluree, &ledger, OPTIONAL_THEN_VALUES).await;
    let post_query = sparql_rows(
        &fluree,
        &ledger,
        "SELECT ?s ?f WHERE { ?s schema:name ?name . \
         OPTIONAL { ?s ex:friend ?f } } VALUES ?f { ex:alice }",
    )
    .await;

    assert_eq!(in_group, post_query);
}

#[tokio::test]
async fn trailing_values_is_a_join_not_a_filter() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    // Pins the §18.2.4 distinction so the fix is never "simplified" into the
    // FILTER semantics: FILTER evaluates `?f = ex:alice` to an error on an
    // unbound `?f` and drops the row; VALUES joins, and an unbound `?f` is
    // compatible with the VALUES row, so the solution survives and adopts it.
    let with_values = sparql_rows(&fluree, &ledger, OPTIONAL_THEN_VALUES).await;
    let with_filter = sparql_rows(
        &fluree,
        &ledger,
        "SELECT ?s ?f WHERE { ?s schema:name ?name . \
         OPTIONAL { ?s ex:friend ?f } FILTER(?f = ex:alice) }",
    )
    .await;

    // Both sides asserted exactly. An `assert_ne!` alone would be vacuous —
    // the pre-fix VALUES answer is also unequal to the FILTER answer, so this
    // test would pass with the barrier reverted and earn none of its name.
    assert_eq!(
        with_values,
        normalize_rows(&optional_then_values_rows()),
        "VALUES keeps the unbound rows and gives them the value"
    );
    assert_eq!(
        with_filter,
        normalize_rows(&json!([["ex:cam", "ex:alice"], ["ex:liam", "ex:alice"]])),
        "FILTER drops the rows the OPTIONAL left unbound"
    );
    assert_ne!(
        with_values, with_filter,
        "VALUES adopts the unbound rows FILTER drops — they are not interchangeable"
    );
}

#[tokio::test]
async fn jsonld_values_after_optional_joins_instead_of_seeding() {
    // Parity twin: the JSON-LD `["values", …]` after `["optional", …]` lowers
    // to the same IR, so it clobbered identically and must now agree.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.com/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let query = json!({
        "@context": ctx,
        "select": ["?s", "?f"],
        "where": [
            {"@id": "?s", "schema:name": "?name"},
            ["optional", {"@id": "?s", "ex:friend": "?f"}],
            ["values", ["?f", [{"@type": "@id", "@value": "ex:alice"}]]]
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["ex:cam", "ex:alice"],
            ["ex:liam", "ex:alice"],
            ["ex:brian", "ex:alice"],
            ["ex:nikola", "ex:alice"]
        ]))
    );
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
