//! Negation query integration tests
//!
//! All inserts and queries are explicit with `@context`.

use crate::support;
use crate::support::{genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

async fn assert_minus_count(
    fluree: &fluree_db_api::Fluree,
    view: &fluree_db_api::GraphDb,
    body: &str,
    expected: usize,
) {
    use fluree_db_api::QueryInput;
    let count_query =
        format!("PREFIX ex: <http://example.com/> SELECT (COUNT(*) AS ?n) WHERE {{ {body} }}");
    let count = fluree
        .query(view, QueryInput::Sparql(&count_query))
        .await
        .unwrap();
    assert_eq!(
        count.to_jsonld(&view.snapshot).unwrap(),
        json!([[expected]]),
        "{count_query}"
    );
    let row_query = count_query.replacen("SELECT (COUNT(*) AS ?n)", "SELECT ?p", 1);
    let rows = fluree
        .query(view, QueryInput::Sparql(&row_query))
        .await
        .unwrap();
    assert_eq!(
        rows.batches
            .iter()
            .map(fluree_db_api::Batch::len)
            .sum::<usize>(),
        expected,
        "{row_query}"
    );
}

async fn seed_minus_regression_people(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let graph: Vec<_> = (0..12)
        .map(|i| {
            let mut person =
                json!({"@id": format!("ex:p{i}"), "@type": "ex:Person", "ex:tag": i % 4});
            if i % 3 == 0 {
                person["ex:worksFor"] = json!([{"@id":"ex:org1"}, {"@id":"ex:org2"}]);
            }
            person
        })
        .collect();
    fluree
        .insert(
            genesis_ledger(fluree, ledger_id),
            &json!({"@context": ctx_ex(), "@graph": graph}),
        )
        .await
        .unwrap()
        .ledger
}

async fn assert_negation_subjects(
    fluree: &MemoryFluree,
    view: &fluree_db_api::GraphDb,
    body: &str,
    expected: &[usize],
) {
    assert_minus_count(fluree, view, body, expected.len()).await;
    let query = format!("PREFIX ex: <http://example.com/> SELECT ?p WHERE {{ {body} }}");
    let result = fluree
        .query(view, fluree_db_api::QueryInput::Sparql(&query))
        .await
        .unwrap();
    let expected: Vec<_> = expected
        .iter()
        .map(|i| json!([format!("ex:p{i}")]))
        .collect();
    assert_eq!(
        normalize_rows(&result.to_jsonld(&view.snapshot).unwrap()),
        normalize_rows(&json!(expected)),
        "{query}"
    );
}

#[tokio::test]
async fn minus_after_values_undef_binds_shared_variable() {
    use fluree_db_api::ReindexOptions;
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "minus-values-undef:main";
    seed_minus_regression_people(&fluree, ledger_id).await;
    for indexed in [false, true] {
        if indexed {
            fluree
                .reindex(ledger_id, ReindexOptions::default())
                .await
                .unwrap();
        }
        let view = fluree.db(ledger_id).await.unwrap();
        // The UNDEF row is filled in by the triple before negation. The two
        // explicit p1 rows also survive, preserving three copies of p1.
        for negation in [
            "MINUS { ?p ex:worksFor ?org }",
            "FILTER NOT EXISTS { ?p ex:worksFor ?org }",
        ] {
            let body = format!("VALUES ?p {{ ex:p0 ex:p1 ex:p1 UNDEF }} ?p a ex:Person {negation}");
            assert_negation_subjects(&fluree, &view, &body, &[1, 1, 1, 2, 4, 5, 7, 8, 10, 11])
                .await;
        }
        assert_negation_subjects(
            &fluree,
            &view,
            "VALUES ?p { ex:p0 ex:p1 ex:p1 UNDEF } ?p a ex:Person FILTER EXISTS { ?p ex:worksFor ?org }",
            &[0, 0, 3, 6, 9],
        ).await;
    }
}

#[tokio::test]
async fn minus_at_historical_time_after_reindex() {
    use fluree_db_api::ReindexOptions;
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "minus-historical:main";
    let ledger = seed_minus_regression_people(&fluree, ledger_id).await;
    let before_update = ledger.t();
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .unwrap();
    fluree
        .update(
            ledger,
            &json!({
                "@context": ctx_ex(),
                "delete": {"@id":"ex:p0", "ex:worksFor":[{"@id":"ex:org1"}, {"@id":"ex:org2"}]},
                "insert": [
                    {"@id":"ex:p1", "ex:worksFor":{"@id":"ex:org1"}},
                    {"@id":"ex:new", "@type":"ex:Person", "ex:tag":0}
                ]
            }),
        )
        .await
        .unwrap();
    for indexed in [false, true] {
        if indexed {
            fluree
                .reindex(ledger_id, ReindexOptions::default())
                .await
                .unwrap();
        }
        let current = fluree.db(ledger_id).await.unwrap();
        let historical = fluree.db_at_t(ledger_id, before_update).await.unwrap();
        assert_eq!(historical.t, before_update);
        // These predicates have no retractions. Their encoded history
        // segments can be empty, but replay must still remove the new person.
        for body in ["?p a ex:Person", "?p ex:tag ?tag"] {
            assert_minus_count(&fluree, &current, body, 13).await;
            assert_negation_subjects(&fluree, &historical, body, &(0..12).collect::<Vec<_>>())
                .await;
        }
        for body in [
            "?p a ex:Person MINUS { ?p ex:worksFor ?org }",
            "?p ex:tag ?tag MINUS { ?p ex:worksFor ?org }",
        ] {
            assert_minus_count(&fluree, &current, body, 9).await;
            assert_negation_subjects(&fluree, &historical, body, &[1, 2, 4, 5, 7, 8, 10, 11]).await;
        }
    }
}

#[tokio::test]
async fn minus_single_key_preserves_mixed_bindings_and_visible_facts() {
    use fluree_db_api::{QueryInput, ReindexOptions};
    const PEOPLE: usize = 1200;
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "minus-single-key:main";
    let ledger = genesis_ledger(&fluree, ledger_id);
    let graph: Vec<_> = (0..PEOPLE)
        .map(|i| {
            let mut person =
                json!({"@id": format!("ex:p{i}"), "@type": "ex:Person", "ex:tag": i % 4});
            if i % 3 == 0 {
                person["ex:worksFor"] = json!([{"@id":"ex:org1"}, {"@id":"ex:org2"}]);
            }
            person
        })
        .collect();
    let ledger = fluree
        .insert(ledger, &json!({"@context": ctx_ex(), "@graph": graph}))
        .await
        .unwrap()
        .ledger;
    let body = "?p a ex:Person MINUS { ?p ex:worksFor ?org }";
    for indexed in [false, true] {
        if indexed {
            fluree
                .reindex(ledger_id, ReindexOptions::default())
                .await
                .unwrap();
        }
        let view = fluree.db(ledger_id).await.unwrap();
        let (spans, guard) = support::span_capture::init_test_tracing();
        let query =
            format!("PREFIX ex: <http://example.com/> SELECT (COUNT(*) AS ?n) WHERE {{ {body} }}");
        let result = fluree
            .query(&view, QueryInput::Sparql(&query))
            .await
            .unwrap();
        drop(guard);
        assert_eq!(result.to_jsonld(&view.snapshot).unwrap(), json!([[800]]));
        if indexed {
            let events = spans.find_events("minus index built");
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].fields["subject_keys"], "400");
            assert_eq!(events[0].fields["tuple_keys"], "0");
        }
        for (body, expected) in [
            (body, 800),
            ("VALUES ?p { ex:p0 ex:p1 ex:p1 UNDEF } MINUS { ?p ex:worksFor ?org }", 3),
            ("?p a ex:Person MINUS { VALUES ?p { ex:p0 UNDEF } }", PEOPLE - 1),
            ("?p a ex:Person MINUS { VALUES ?p { UNDEF } }", PEOPLE),
            ("?p a ex:Person MINUS { ?other ex:worksFor ?org }", PEOPLE),
            ("?p a ex:Person MINUS { ?p ex:missing ?value }", PEOPLE),
            ("?p a ex:Person ; ex:tag ?tag MINUS { ?p ex:worksFor ?org ; ex:tag ?tag }", 800),
            ("?p a ex:Person ; ex:tag ?tag MINUS { ?p ex:worksFor ?org OPTIONAL { ?p ex:missing ?tag } }", 800),
        ] {
            assert_minus_count(&fluree, &view, body, expected).await;
        }
    }
    fluree
        .update(
            ledger,
            &json!({
                "@context": ctx_ex(),
                "delete": {"@id":"ex:p0", "ex:worksFor":[{"@id":"ex:org1"}, {"@id":"ex:org2"}]},
                "insert": [
                    {"@id":"ex:p1", "ex:worksFor":{"@id":"ex:org1"}},
            {"@id":"ex:new", "@type":"ex:Person", "ex:tag":0}
                ]
            }),
        )
        .await
        .unwrap();
    for indexed in [false, true] {
        if indexed {
            fluree
                .reindex(ledger_id, ReindexOptions::default())
                .await
                .unwrap();
        }
        let view = fluree.db(ledger_id).await.unwrap();
        assert_minus_count(&fluree, &view, body, 801).await;
    }
}

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

/// alice's only acquaintance works somewhere but alice doesn't; carol works at
/// a different org than her acquaintance; dave's acquaintance has no employer.
async fn seed_knows_works_for(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let ttl = r"@prefix ex: <http://example.com/> .
ex:alice ex:knows ex:bob .
ex:bob ex:worksFor ex:acme .
ex:carol ex:knows ex:bob ;
    ex:worksFor ex:globex .
ex:dave ex:knows ex:erin .
";
    fluree
        .insert_turtle(ledger0, ttl)
        .await
        .expect("insert turtle")
        .ledger
}

async fn sparql_rows(
    fluree: &MemoryFluree,
    ledger: &MemoryLedger,
    q: &str,
) -> Vec<serde_json::Value> {
    let r = support::query_sparql(fluree, ledger, q)
        .await
        .unwrap_or_else(|e| panic!("sparql query failed: {e}\n{q}"));
    normalize_rows(&r.to_jsonld(&ledger.snapshot).unwrap())
}

/// A variable left unbound by an unmatched OPTIONAL is free inside a later
/// NOT EXISTS: substitution (SPARQL 1.1 §18.6) replaces only bound variables.
/// For alice (`?org` unbound) the body becomes
/// `ex:alice ex:knows ?x . ?x ex:worksFor ?org`, which matches via bob/acme,
/// so the row is removed. Each query routes the NOT EXISTS through a
/// different evaluator; all must agree.
#[tokio::test]
async fn sparql_not_exists_treats_optional_unbound_var_as_free() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_knows_works_for(&fluree, "negation:optional-unbound").await;
    let expected = normalize_rows(&json!([["ex:carol", "ex:globex"], ["ex:dave", null]]));

    let cases = [
        // Pattern-level; correlated only via inner-produced vars (?p, ?org) →
        // semijoin, whose unbound-key rows probe a projected key set.
        (
            "semijoin",
            r"PREFIX ex: <http://example.com/>
SELECT ?p ?org WHERE {
  ?p ex:knows ?f .
  OPTIONAL { ?p ex:worksFor ?org }
  FILTER NOT EXISTS { ?p ex:knows ?x . ?x ex:worksFor ?org }
}",
        ),
        // Inner consumes outer-only ?f → per-row ExistsOperator.
        (
            "per-row",
            r"PREFIX ex: <http://example.com/>
SELECT ?p ?org WHERE {
  ?p ex:knows ?f .
  OPTIONAL { ?p ex:worksFor ?org }
  FILTER NOT EXISTS { ?p ex:knows ?x . ?x ex:worksFor ?org FILTER(?x = ?f) }
}",
        ),
        // NOT EXISTS inside a compound expression → FilterOperator.
        (
            "expression",
            r"PREFIX ex: <http://example.com/>
SELECT ?p ?org WHERE {
  ?p ex:knows ?f .
  OPTIONAL { ?p ex:worksFor ?org }
  FILTER (?p = ex:nobody || NOT EXISTS { ?p ex:knows ?x . ?x ex:worksFor ?org })
}",
        ),
    ];
    let mut wrong = Vec::new();
    for (lane, q) in cases {
        let rows = sparql_rows(&fluree, &ledger, q).await;
        if rows != expected {
            wrong.push(format!("{lane}: {rows:?}"));
        }
    }
    assert!(wrong.is_empty(), "expected {expected:?}; got {wrong:#?}");

    let q_exists = r"PREFIX ex: <http://example.com/>
SELECT ?p ?org WHERE {
  ?p ex:knows ?f .
  OPTIONAL { ?p ex:worksFor ?org }
  FILTER EXISTS { ?p ex:knows ?x . ?x ex:worksFor ?org }
}";
    assert_eq!(
        sparql_rows(&fluree, &ledger, q_exists).await,
        normalize_rows(&json!([["ex:alice", null]]))
    );
}

#[tokio::test]
async fn jsonld_not_exists_treats_optional_unbound_var_as_free() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_knows_works_for(&fluree, "negation:optional-unbound-jsonld").await;

    let q = json!({
        "@context": ctx_ex(),
        "select": ["?p", "?org"],
        "where": [
            {"@id": "?p", "ex:knows": "?f"},
            ["optional", {"@id": "?p", "ex:worksFor": "?org"}],
            ["not-exists",
                {"@id": "?p", "ex:knows": "?x"},
                {"@id": "?x", "ex:worksFor": "?org"}]
        ]
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([["ex:carol", "ex:globex"], ["ex:dave", null]]))
    );
}

/// A missing OPTIONAL binding must use a reusable existence lookup, while
/// bound values still constrain the inner match and outer duplicates survive.
#[tokio::test]
async fn optional_exists_reuses_partial_keys_across_batches() {
    use fluree_db_api::{QueryInput, ReindexOptions};
    use support::span_capture;

    const PEOPLE: usize = 1200;
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "negation:optional-partial-keys";
    let ledger = genesis_ledger(&fluree, ledger_id);
    let mut graph = vec![
        json!({"@id": "ex:worker1", "ex:worksFor": {"@id": "ex:orgA"}}),
        json!({"@id": "ex:worker2", "ex:worksFor": {"@id": "ex:orgA"}}),
    ];
    for i in 0..PEOPLE {
        let mut node = json!({
            "@id": format!("ex:p{i}"), "@type": "ex:Person",
            "ex:knows": if i % 4 == 3 {
                json!({"@id": "ex:worker3"})
            } else {
                json!([{"@id": "ex:worker1"}, {"@id": "ex:worker2"}])
            }
        });
        if i % 4 == 1 {
            node["ex:worksFor"] = json!({"@id": "ex:orgA"});
        } else if i % 4 == 2 {
            node["ex:worksFor"] = json!([{"@id": "ex:orgB"}, {"@id": "ex:orgC"}]);
        }
        graph.push(node);
    }
    let ledger = fluree
        .insert(ledger, &json!({"@context": ctx_ex(), "@graph": graph}))
        .await
        .expect("seed")
        .ledger;
    let before_update = ledger.t();
    let mut expected = Vec::new();
    for i in 0..PEOPLE {
        match i % 4 {
            2 => {
                for org in ["ex:orgB", "ex:orgC"] {
                    expected.push(json!([format!("ex:p{i}"), org]));
                }
            }
            3 => expected.push(json!([format!("ex:p{i}"), null])),
            _ => {}
        }
    }
    expected.sort_by_key(ToString::to_string);
    let query = "PREFIX ex: <http://example.com/> SELECT ?p ?org WHERE { \
        ?p a ex:Person OPTIONAL { ?p ex:worksFor ?org } \
        FILTER NOT EXISTS { ?p ex:knows ?x . ?x ex:worksFor ?org } }";

    // Live novelty, then the indexed view. Count uses the same lookup as rows.
    for indexed in [false, true] {
        if indexed {
            fluree
                .reindex(ledger_id, ReindexOptions::default())
                .await
                .expect("reindex");
        }
        let view = fluree.db(ledger_id).await.expect("view");
        let (spans, guard) = span_capture::init_test_tracing();
        let result = fluree
            .query(&view, QueryInput::Sparql(query))
            .await
            .expect("query");
        drop(guard);
        assert_eq!(
            normalize_rows(&result.to_jsonld(&view.snapshot).unwrap()),
            expected
        );
        let builds = spans.find_events("semijoin partial-key lookup built");
        assert_eq!(
            builds.len(),
            1,
            "indexed={indexed}: expected one reused lookup"
        );
        let probes = spans.find_events("semijoin partial-key probes");
        let projected: usize = probes
            .iter()
            .map(|e| e.fields["projected_rows"].parse::<usize>().unwrap())
            .sum();
        let correlated: usize = probes
            .iter()
            .map(|e| e.fields["correlated_rows"].parse::<usize>().unwrap())
            .sum();
        assert_eq!(projected, PEOPLE / 2);
        assert_eq!(correlated, 0);

        let count_query = query.replace("SELECT ?p ?org", "SELECT (COUNT(*) AS ?n)");
        let count = fluree
            .query(&view, QueryInput::Sparql(&count_query))
            .await
            .expect("count");
        assert_eq!(
            count.to_jsonld(&view.snapshot).unwrap(),
            json!([[expected.len()]])
        );

        // Compound expression forces seeded evaluation as an independent oracle.
        assert_eq!(query.matches("FILTER NOT EXISTS").count(), 1);
        assert_eq!(query.matches("?x ex:worksFor ?org } }").count(), 1);
        let control = query
            .replacen("FILTER NOT EXISTS", "FILTER (false || NOT EXISTS", 1)
            .replacen("?x ex:worksFor ?org } }", "?x ex:worksFor ?org }) }", 1);
        let control = fluree
            .query(&view, QueryInput::Sparql(&control))
            .await
            .expect("control");
        assert_eq!(
            normalize_rows(&control.to_jsonld(&view.snapshot).unwrap()),
            expected
        );

        let exists = query.replace("NOT EXISTS", "EXISTS");
        let result = fluree
            .query(&view, QueryInput::Sparql(&exists))
            .await
            .expect("EXISTS");
        let mut matches = Vec::new();
        for i in 0..PEOPLE {
            match i % 4 {
                0 => matches.push(json!([format!("ex:p{i}"), null])),
                1 => matches.push(json!([format!("ex:p{i}"), "ex:orgA"])),
                _ => {}
            }
        }
        matches.sort_by_key(ToString::to_string);
        assert_eq!(
            normalize_rows(&result.to_jsonld(&view.snapshot).unwrap()),
            matches
        );

        if indexed {
            // Expressions in the inner body deliberately keep the seeded
            // fallback. This FILTER preserves this fixture's expected answer.
            let filtered = query.replace(
                "?x ex:worksFor ?org }",
                "?x ex:worksFor ?org FILTER(?x != ex:nobody) }",
            );
            let (spans, guard) = span_capture::init_test_tracing();
            let result = fluree
                .query(&view, QueryInput::Sparql(&filtered))
                .await
                .expect("filtered body");
            drop(guard);
            assert_eq!(
                normalize_rows(&result.to_jsonld(&view.snapshot).unwrap()),
                expected
            );
            assert!(spans
                .find_events("semijoin partial-key lookup built")
                .is_empty());
            let probes = spans.find_events("semijoin partial-key probes");
            assert_eq!(
                probes
                    .iter()
                    .map(|e| e.fields["correlated_rows"].parse::<usize>().unwrap())
                    .sum::<usize>(),
                PEOPLE / 2
            );
        }
    }

    // The lookup belongs to one execution: both an overlay and a historical
    // view must build from their own visible facts.
    fluree
        .update(
            ledger,
            &json!({
                "@context": ctx_ex(),
                "delete": [
                    {"@id": "ex:worker1", "ex:worksFor": {"@id": "ex:orgA"}},
                    {"@id": "ex:worker2", "ex:worksFor": {"@id": "ex:orgA"}}
                ],
                "insert": {"@id": "ex:worker3", "ex:worksFor": {"@id": "ex:orgC"}}
            }),
        )
        .await
        .expect("change employers");
    let view = fluree.db(ledger_id).await.expect("overlay view");
    let result = fluree
        .query(&view, QueryInput::Sparql(query))
        .await
        .expect("overlay query");
    let mut updated = Vec::new();
    for i in 0..PEOPLE {
        match i % 4 {
            0 => updated.push(json!([format!("ex:p{i}"), null])),
            1 => updated.push(json!([format!("ex:p{i}"), "ex:orgA"])),
            2 => {
                for org in ["ex:orgB", "ex:orgC"] {
                    updated.push(json!([format!("ex:p{i}"), org]));
                }
            }
            _ => {}
        }
    }
    updated.sort_by_key(ToString::to_string);
    assert_eq!(
        normalize_rows(&result.to_jsonld(&view.snapshot).unwrap()),
        updated
    );
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex updated");
    let historical = fluree
        .db_at_t(ledger_id, before_update)
        .await
        .expect("historical view");
    let result = fluree
        .query(&historical, QueryInput::Sparql(query))
        .await
        .expect("historical OPTIONAL");
    assert_eq!(
        normalize_rows(&result.to_jsonld(&historical.snapshot).unwrap()),
        expected
    );
    let historical_query = "PREFIX ex: <http://example.com/> SELECT ?p ?org WHERE { \
        VALUES (?p ?org) { (ex:p0 UNDEF) (ex:p1 ex:orgA) (ex:p2 ex:orgB) (ex:p3 UNDEF) } \
        FILTER NOT EXISTS { ?p ex:knows ?x . ?x ex:worksFor ?org } }";
    let result = fluree
        .query(&historical, QueryInput::Sparql(historical_query))
        .await
        .expect("historical query");
    assert_eq!(
        normalize_rows(&result.to_jsonld(&historical.snapshot).unwrap()),
        normalize_rows(&json!([["ex:p2", "ex:orgB"], ["ex:p3", null]]))
    );
}
