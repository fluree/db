//! Negation query integration tests
//!
//! All inserts and queries are explicit with `@context`.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};

fn ctx_ex() -> serde_json::Value {
    // Match the minimal {"ex" "http://example.com/"} context and include xsd/schema for safety.
    json!({
        "ex": "http://example.com/",
        "schema": "http://schema.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

/// Filter query results to only rows whose subject starts with "ex:".
///
/// Commit/db metadata flakes introduce additional subjects like `fluree:commit:...`
/// and commit metadata subjects which are orthogonal to these negation semantics tests.
fn filter_rows_subject_ex(v: &serde_json::Value) -> serde_json::Value {
    let Some(arr) = v.as_array() else {
        return v.clone();
    };

    let mut out = Vec::with_capacity(arr.len());
    for row in arr {
        match row {
            // Scalar row: subject string directly
            serde_json::Value::String(s) => {
                if s.starts_with("ex:") {
                    out.push(row.clone());
                }
            }
            // Tuple row: first element is subject
            serde_json::Value::Array(cols) => {
                let subj = cols.first().and_then(|x| x.as_str());
                if subj.is_some_and(|s| s.starts_with("ex:")) {
                    out.push(row.clone());
                }
            }
            _ => {
                // Unexpected shape; keep it so failures are visible.
                out.push(row.clone());
            }
        }
    }
    serde_json::Value::Array(out)
}

async fn seed_people(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let ctx = ctx_ex();

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:alice",
                "@type": "ex:Person",
                "ex:nickname": "Ali",
                "ex:givenName": "Alice",
                "ex:familyName": "Smith"
            },
            {
                "@id": "ex:bob",
                "ex:givenName": "Bob",
                "ex:familyName": "Jones"
            },
            {
                "@id": "ex:carol",
                "ex:givenName": "Carol",
                "ex:familyName": "Smith"
            }
        ]
    });

    fluree.insert(ledger0, &insert).await.unwrap().ledger
}

#[tokio::test]
async fn exists_when_pattern_present_returns_subjects() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "negation:people").await;
    let ctx = ctx_ex();

    let q = json!({
        "@context": ctx,
        "select": "?person",
        "where": [
            {"@id":"?person","@type":"ex:Person"},
            ["exists", {"@id":"?person","ex:givenName":"?name"}]
        ]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(rows, json!(["ex:alice"]));
}

#[tokio::test]
async fn exists_when_pattern_absent_returns_no_subjects() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "negation:people").await;
    let ctx = ctx_ex();

    let q = json!({
        "@context": ctx,
        "select": ["?person"],
        "where": [
            {"@id":"?person","@type":"ex:Person"},
            ["exists", {"@id":"?person","ex:name":"?name"}]
        ]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(rows, json!([]));
}

#[tokio::test]
async fn not_exists_filters_subjects_without_nickname() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "negation:people").await;
    let ctx = ctx_ex();

    let q = json!({
        "@context": ctx,
        "select": "?person",
        "where": [
            {"@id":"?person","ex:givenName":"?gname"},
            ["not-exists", {"@id":"?person","ex:nickname":"?name"}]
        ]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!(["ex:bob", "ex:carol"]))
    );
}

/// `OPTIONAL { ?s p ?v } FILTER(NOT BOUND(?v))` is the SPARQL-muscle-memory
/// idiom for "subjects without a value for predicate p". It is recognized at
/// `where_plan.rs:1595-1623` and dispatched through the shared EXISTS strategy
/// helper, so this query should return the same rows as the pattern-level
/// `["not-exists", ...]` form above. Inner shares ?person with the outer
/// triple, so the helper picks `SemijoinOperator` (build-once + hash probe).
#[tokio::test]
async fn optional_not_bound_filters_same_as_not_exists() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "negation:people").await;
    let ctx = ctx_ex();

    let q = json!({
        "@context": ctx,
        "select": "?person",
        "where": [
            {"@id":"?person","ex:givenName":"?gname"},
            ["optional", {"@id":"?person","ex:nickname":"?name"}],
            ["filter", "(not (bound ?name))"]
        ]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!(["ex:bob", "ex:carol"]))
    );
}

/// Same idiom expressed in the data filter form `["not", ["bound", "?v"]]`
/// rather than the s-expression form `"(not (bound ?v))"`. Both should parse
/// to the same `Expression::Not(Expression::Call(Function::Bound, ...))` and
/// hit the same OPTIONAL+not-bound rewrite.
#[tokio::test]
async fn optional_not_bound_data_form_filters_same_as_not_exists() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "negation:people").await;
    let ctx = ctx_ex();

    let q = json!({
        "@context": ctx,
        "select": "?person",
        "where": [
            {"@id":"?person","ex:givenName":"?gname"},
            ["optional", {"@id":"?person","ex:nickname":"?name"}],
            ["filter", ["not", ["bound", "?name"]]]
        ]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!(["ex:bob", "ex:carol"]))
    );
}

#[tokio::test]
async fn not_exists_when_everyone_has_family_name_returns_none() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "negation:people").await;
    let ctx = ctx_ex();

    let q = json!({
        "@context": ctx,
        "select": ["?person"],
        "where": [
            {"@id":"?person","ex:givenName":"?gname"},
            ["not-exists", {"@id":"?person","ex:familyName":"?fname"}]
        ]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(rows, json!([]));
}

#[tokio::test]
async fn not_exists_all_variables_filters_everything() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "negation:people").await;
    let ctx = ctx_ex();

    let q = json!({
        "@context": ctx,
        "select": ["?s","?p","?o"],
        "where": [
            {"@id":"?s","?p":"?o"},
            ["not-exists", {"@id":"?x","?y":"?z"}]
        ]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(rows, json!([]));
}

#[tokio::test]
async fn not_exists_all_literals_filters_everything_when_match_exists() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "negation:people").await;
    let ctx = ctx_ex();

    let q = json!({
        "@context": ctx,
        "select": ["?s","?p","?o"],
        "where": [
            {"@id":"?s","?p":"?o"},
            // NOTE: Some clients use {"@id":"ex:alice","type","ex:Person"} but in JSON-LD WHERE
            // our parser expects @type for rdf:type matching.
            ["not-exists", {"@id":"ex:alice","@type":"ex:Person"}]
        ]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(rows, json!([]));
}

#[tokio::test]
async fn minus_removes_bound_solutions() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "negation:people").await;
    let ctx = ctx_ex();

    let q = json!({
        "@context": ctx,
        "select": "?s",
        "distinct": true,
        "where": [
            {"@id":"?s","?p":"?o"},
            ["minus", {"@id":"?s","ex:givenName":"Bob"}]
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();

    // Rust selectDistinct returns an array of scalar rows (one column).
    assert_eq!(
        normalize_rows(&filter_rows_subject_ex(&result)),
        normalize_rows(&json!(["ex:alice", "ex:carol"]))
    );
}

#[tokio::test]
async fn minus_all_variables_has_no_common_bindings_removes_nothing() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "negation:people").await;
    let ctx = ctx_ex();

    let q = json!({
        "@context": ctx,
        "select": ["?s","?p","?o"],
        "where": [
            {"@id":"?s","?p":"?o"},
            ["minus", {"@id":"?x","?y":"?z"}]
        ]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    // Compare as sets: order isn't stable.
    assert_eq!(
        normalize_rows(&filter_rows_subject_ex(&rows)),
        normalize_rows(&json!([
            [
                "ex:alice",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "ex:Person"
            ],
            ["ex:alice", "ex:familyName", "Smith"],
            ["ex:alice", "ex:givenName", "Alice"],
            ["ex:alice", "ex:nickname", "Ali"],
            ["ex:bob", "ex:familyName", "Jones"],
            ["ex:bob", "ex:givenName", "Bob"],
            ["ex:carol", "ex:familyName", "Smith"],
            ["ex:carol", "ex:givenName", "Carol"]
        ]))
    );
}

#[tokio::test]
async fn minus_all_literals_no_common_bindings_removes_nothing() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "negation:people").await;
    let ctx = ctx_ex();

    let q = json!({
        "@context": ctx,
        "select": ["?s","?p","?o"],
        "where": [
            {"@id":"?s","?p":"?o"},
            ["minus", {"@id":"ex:alice","ex:familyName":"Smith"}]
        ]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&filter_rows_subject_ex(&rows)),
        normalize_rows(&json!([
            [
                "ex:alice",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "ex:Person"
            ],
            ["ex:alice", "ex:familyName", "Smith"],
            ["ex:alice", "ex:givenName", "Alice"],
            ["ex:alice", "ex:nickname", "Ali"],
            ["ex:bob", "ex:familyName", "Jones"],
            ["ex:bob", "ex:givenName", "Bob"],
            ["ex:carol", "ex:familyName", "Smith"],
            ["ex:carol", "ex:givenName", "Carol"]
        ]))
    );
}

#[tokio::test]
async fn inner_filter_not_exists_vs_minus_behavior() {
    // Scenario: demonstrates that NOT-EXISTS sees existing bindings for filter,
    // while MINUS does not (i.e. filter inside MINUS can't reference outer vars).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "negation:inner-filters");
    let ctx = ctx_ex();

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:a","ex:p":1,"ex:q":[1,2]},
            {"@id":"ex:b","ex:p":3.0,"ex:q":[4.0,5.0]}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let q_not_exists = json!({
        "@context": ctx,
        "select": ["?x","?p"],
        "where": [
            {"@id":"?x","ex:p":"?p"},
            ["not-exists",
                {"@id":"?x","ex:q":"?q"},
                ["filter","(= ?p ?q)"]
            ]
        ]
    });
    let r1 = support::query_jsonld(&fluree, &ledger, &q_not_exists)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(r1, json!([["ex:b", 3.0]]));

    let q_minus = json!({
        "@context": ctx,
        "select": ["?x","?p"],
        "where": [
            {"@id":"?x","ex:p":"?p"},
            ["minus",
                {"@id":"?x","ex:q":"?q"},
                ["filter","(= ?p ?q)"]
            ]
        ]
    });
    let r2 = support::query_jsonld(&fluree, &ledger, &q_minus)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&r2),
        normalize_rows(&json!([["ex:a", 1], ["ex:b", 3.0]]))
    );
}

/// Compound filter expression with NOT EXISTS inside an OR.
///
/// Tests the new Expression::Exists path for JSON-LD queries.
/// Equivalent to SPARQL: `FILTER(?name = "Alice" || NOT EXISTS { ?person ex:nickname ?nick })`
#[tokio::test]
async fn compound_filter_not_exists_in_or() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "negation:compound-filter").await;
    let ctx = ctx_ex();

    // Keep people whose name is "Alice" OR who don't have a nickname.
    // Data: alice has nickname "Ali" + name "Alice"; bob and carol don't have nicknames.
    // alice passes the =Alice check; bob and carol pass NOT EXISTS.
    let q = json!({
        "@context": ctx,
        "select": ["?person"],
        "where": [
            {"@id": "?person", "ex:givenName": "?name"},
            ["filter", ["or",
                ["=", "?name", "Alice"],
                ["not-exists", {"@id": "?person", "ex:nickname": "?nick"}]
            ]]
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .expect("compound filter NOT EXISTS should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).unwrap();
    let rows = normalize_rows(&jsonld);

    // All 3 should pass: alice via name="Alice", bob and carol via NOT EXISTS
    assert_eq!(
        rows.len(),
        3,
        "Expected 3 results (alice via name check, bob+carol via NOT EXISTS), got: {rows:?}"
    );
}

/// Standalone NOT EXISTS in filter expression (non-compound).
///
/// Tests that `["filter", ["or", false, ["not-exists", {...}]]]` produces
/// the same result as pattern-level `["not-exists", {...}]`.
#[tokio::test]
async fn filter_not_exists_expression_equals_pattern_level() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "negation:expr-vs-pattern").await;
    let ctx = ctx_ex();

    // Pattern-level NOT EXISTS
    let q_pattern = json!({
        "@context": ctx,
        "select": ["?person"],
        "where": [
            {"@id": "?person", "ex:givenName": "?name"},
            ["not-exists", {"@id": "?person", "ex:nickname": "?nick"}]
        ]
    });

    let r_pattern = support::query_jsonld(&fluree, &ledger, &q_pattern)
        .await
        .expect("pattern-level NOT EXISTS");
    let pattern_rows = normalize_rows(&r_pattern.to_jsonld(&ledger.snapshot).unwrap());

    // Expression-level: FILTER(false || NOT EXISTS { ... }) should equal pattern-level
    let q_expr = json!({
        "@context": ctx,
        "select": ["?person"],
        "where": [
            {"@id": "?person", "ex:givenName": "?name"},
            ["filter", ["or", false, ["not-exists", {"@id": "?person", "ex:nickname": "?nick"}]]]
        ]
    });

    let r_expr = support::query_jsonld(&fluree, &ledger, &q_expr)
        .await
        .expect("expression-level NOT EXISTS");
    let expr_rows = normalize_rows(&r_expr.to_jsonld(&ledger.snapshot).unwrap());

    assert_eq!(
        pattern_rows, expr_rows,
        "expression-level NOT EXISTS should equal pattern-level NOT EXISTS"
    );
}
