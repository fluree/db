//! Subquery integration tests
//!
//! All inserts and queries are explicit with `@context`.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{context_ex_schema, genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};

async fn seed_people(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let ctx = context_ex_schema();

    // Matches the dataset used across other query tests (people-strings)
    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:brian","@type":"ex:User","schema:name":"Brian","schema:email":"brian@example.org","schema:age":50,"ex:favNums":7},
            {"@id":"ex:alice","@type":"ex:User","schema:name":"Alice","schema:email":"alice@example.org","schema:age":50,"ex:favNums":[42,76,9]},
            {"@id":"ex:cam","@type":"ex:User","schema:name":"Cam","schema:email":"cam@example.org","schema:age":34,"ex:favNums":[5,10]},
            {"@id":"ex:liam","@type":"ex:User","schema:name":"Liam","schema:email":"liam@example.org","schema:age":13,"ex:favNums":[42,11]}
        ]
    });

    fluree.insert(ledger0, &insert).await.unwrap().ledger
}

#[tokio::test]
async fn subquery_basic_correlated_join() {
    // Scenario: subquery-basics / "binding an IRI in the select"
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "subquery:people").await;

    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.org/ns/"
    });

    let q = json!({
        "@context": ctx,
        "select": ["?name","?age"],
        "where": [
            {"@id":"?s","schema:name":"?name"},
            ["query", {
                "@context": ctx,
                "select": ["?s","?age"],
                "where": {"@id":"?s","schema:age":"?age"}
            }]
        ],
        "orderBy": "?name"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        rows,
        json!([["Alice", 50], ["Brian", 50], ["Cam", 34], ["Liam", 13]])
    );
}

#[tokio::test]
async fn subquery_unrelated_vars_cartesian_expand() {
    // Scenario: subquery-basics / "with unrelated vars in subquery expand to all parent vals"
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "subquery:people").await;

    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.org/ns/"
    });

    let q = json!({
        "@context": ctx,
        "select": ["?age","?favNums"],
        "where": [
            {"schema:age":"?age"},
            ["query", {
                "@context": ctx,
                "select": ["?favNums"],
                "where": {"ex:favNums":"?favNums"}
            }]
        ],
        "orderBy": ["?age","?favNums"]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    // Order-insensitive: result ordering may vary.
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([
            [13, 5],
            [13, 7],
            [13, 9],
            [13, 10],
            [13, 11],
            [13, 42],
            [13, 42],
            [13, 76],
            [34, 5],
            [34, 7],
            [34, 9],
            [34, 10],
            [34, 11],
            [34, 42],
            [34, 42],
            [34, 76],
            [50, 5],
            [50, 5],
            [50, 7],
            [50, 7],
            [50, 9],
            [50, 9],
            [50, 10],
            [50, 10],
            [50, 11],
            [50, 11],
            [50, 42],
            [50, 42],
            [50, 42],
            [50, 42],
            [50, 76],
            [50, 76]
        ]))
    );
}

#[tokio::test]
async fn subquery_limit_applies_inside_subquery() {
    // Scenario: subquery-basics / "shorten results with subquery limit"
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "subquery:people").await;

    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.org/ns/"
    });

    let q = json!({
        "@context": ctx,
        "select": ["?age","?favNums"],
        "where": [
            {"schema:age":"?age"},
            ["query", {
                "@context": ctx,
                "select": ["?favNums"],
                "where": {"ex:favNums":"?favNums"},
                // Make the LIMIT deterministic (depends on deterministic index order;
                // in Rust we make the intended behavior explicit).
                "orderBy": "?favNums",
                "limit": 2
            }]
        ],
        "orderBy": ["?age","?favNums"]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([
            [13, 5],
            [13, 7],
            [34, 5],
            [34, 7],
            [50, 5],
            [50, 5],
            [50, 7],
            [50, 7]
        ]))
    );
}

#[tokio::test]
async fn subquery_distinct_applies_to_subquery_select() {
    // Scenario: subquery-basics / "obeys selectDistinct in subquery"
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "subquery:people").await;

    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.org/ns/"
    });

    let q = json!({
        "@context": ctx,
        "select": ["?age","?favNums"],
        "where": [
            {"ex:favNums":"?favNums"},
            ["query", {
                "@context": ctx,
                "select": ["?age"],
                "distinct": true,
                "where": {"schema:age":"?age"}
            }]
        ],
        "orderBy": ["?age","?favNums"]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([
            [13, 5],
            [13, 7],
            [13, 9],
            [13, 10],
            [13, 11],
            [13, 42],
            [13, 42],
            [13, 76],
            [34, 5],
            [34, 7],
            [34, 9],
            [34, 10],
            [34, 11],
            [34, 42],
            [34, 42],
            [34, 76],
            [50, 5],
            [50, 7],
            [50, 9],
            [50, 10],
            [50, 11],
            [50, 42],
            [50, 42],
            [50, 76]
        ]))
    );
}

#[tokio::test]
async fn multiple_subqueries_parallel() {
    // Scenario: multiple-subqueries / "in parallel gets all values"
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "subquery:people").await;

    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.org/ns/"
    });

    let q = json!({
        "@context": ctx,
        "select": ["?age","?favNums"],
        "where": [
            ["query", {"@context": ctx, "select": ["?age"], "where": {"schema:age":"?age"}}],
            ["query", {"@context": ctx, "select": ["?favNums"], "where": {"ex:favNums":"?favNums"}}]
        ],
        "orderBy": ["?age","?favNums"]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([
            [13, 5],
            [13, 7],
            [13, 9],
            [13, 10],
            [13, 11],
            [13, 42],
            [13, 42],
            [13, 76],
            [34, 5],
            [34, 7],
            [34, 9],
            [34, 10],
            [34, 11],
            [34, 42],
            [34, 42],
            [34, 76],
            [50, 5],
            [50, 5],
            [50, 7],
            [50, 7],
            [50, 9],
            [50, 9],
            [50, 10],
            [50, 10],
            [50, 11],
            [50, 11],
            [50, 42],
            [50, 42],
            [50, 42],
            [50, 42],
            [50, 76],
            [50, 76]
        ]))
    );
}

#[tokio::test]
async fn nested_subqueries_distinct() {
    // Scenario: multiple-subqueries / "with nested subqueries"
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "subquery:people").await;

    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.org/ns/"
    });

    let q = json!({
        "@context": ctx,
        "select": ["?name","?email","?age"],
        "where": [
            {"schema:name":"?name"},
            ["query", {
                "@context": ctx,
                "select": ["?age","?email"],
                "distinct": true,
                "where": [
                    {"schema:age":"?age"},
                    ["query", {"@context": ctx, "select": ["?email"], "where": {"schema:email":"?email"}}]
                ]
            }]
        ],
        "orderBy": ["?name","?email","?age"]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    // This is a large cartesian product; just assert cardinality and a few sentinel rows.
    assert_eq!(rows.as_array().unwrap().len(), 48);
    let set = normalize_rows(&rows);
    assert!(set.contains(&json!(["Alice", "alice@example.org", 13])));
    assert!(set.contains(&json!(["Liam", "liam@example.org", 50])));
}

#[tokio::test]
async fn subquery_inside_union() {
    // Scenario: subquery-unions
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "subquery:people").await;
    let ctx = context_ex_schema();

    let q = json!({
        "@context": ctx,
        "select": ["?person","?avgFavNum"],
        "where": [
            ["union",
                [
                    ["query", {
                        "@context": ctx,
                        "select": ["?person","(as (avg ?favN) ?avgFavNum)"],
                        "where": {"@id":"ex:alice","ex:favNums":"?favN"},
                        "groupBy": ["?person"],
                        "values": ["?person", ["Alice"]]
                    }]
                ],
                [
                    ["query", {
                        "@context": ctx,
                        "select": ["?person","(as (avg ?favN) ?avgFavNum)"],
                        "where": {"@id":"ex:cam","ex:favNums":"?favN"},
                        "groupBy": ["?person"],
                        "values": ["?person", ["Cam"]]
                    }]
                ]
            ]
        ]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    // AVG of integer inputs is xsd:decimal per W3C SPARQL §17.4.1.7
    // (integer ÷ integer is decimal-typed). The to_jsonld formatter
    // renders xsd:decimal as a decimal string. Compare numerically with
    // a small tolerance for non-terminating quotients.
    let arr = rows.as_array().expect("rows array").clone();
    let mut values: Vec<(String, f64)> = arr
        .iter()
        .map(|row| {
            let row = row.as_array().expect("row array");
            let name = row[0].as_str().expect("name").to_string();
            let avg = row[1]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .or_else(|| row[1].as_f64())
                .expect("avg value");
            (name, avg)
        })
        .collect();
    values.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(values.len(), 2);
    assert_eq!(values[0].0, "Alice");
    assert!((values[0].1 - 42.333_333_333_333_336).abs() < 1e-12);
    assert_eq!(values[1].0, "Cam");
    assert!((values[1].1 - 7.5).abs() < 1e-12);
}

#[tokio::test]
async fn subquery_union_branch_query_alone_has_results() {
    // Sanity check: the branch query used in subquery_inside_union should produce a row on its own.
    // If this fails, the issue is aggregates/groupBy/values in the query engine (not UNION).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "subquery:people").await;
    let ctx = context_ex_schema();

    let q_alice = json!({
        "@context": ctx,
        "select": ["?person","(as (avg ?favN) ?avgFavNum)"],
        "values": ["?person", ["Alice"]],
        "where": {"@id":"ex:alice","ex:favNums":"?favN"},
        "groupBy": ["?person"]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q_alice)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let arr = rows.as_array().expect("rows array");
    assert_eq!(arr.len(), 1);
    let row = arr[0].as_array().expect("row array");
    assert_eq!(row[0].as_str(), Some("Alice"));
    let avg = row[1]
        .as_str()
        .and_then(|s| s.parse::<f64>().ok())
        .or_else(|| row[1].as_f64())
        .expect("avg value");
    assert!((avg - 42.333_333_333_333_336).abs() < 1e-12);
}

#[tokio::test]
async fn subquery_with_values_filters_results() {
    // Scenario: subquery-values
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "subquery:people").await;
    let ctx = context_ex_schema();

    let q = json!({
        "@context": ctx,
        "select": "?person",
        "where": [
            ["query", {
                "@context": ctx,
                "values": ["?name", ["Alice","Liam"]],
                "where": [
                    {"@id":"?person","schema:name":"?name"},
                    {"@id":"?person","ex:favNums":"?num"}
                ],
                "select": ["?person"],
                "distinct": true
            }]
        ]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!(["ex:alice", "ex:liam"]))
    );
}
