//! SPARQL integration tests
//!
//! Covers query + update semantics (DELETE/INSERT/WHERE) using JSON-LD Update transactions.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;
use support::{
    assert_index_defaults, genesis_ledger, normalize_rows, normalize_sparql_bindings, MemoryFluree,
    MemoryLedger,
};

fn normalize_object_rows(value: &JsonValue) -> Vec<String> {
    let Some(array) = value.as_array() else {
        return Vec::new();
    };
    let mut rows: Vec<String> = array
        .iter()
        .map(|row| serde_json::to_string(row).expect("serialize row"))
        .collect();
    rows.sort();
    rows
}

async fn seed_people(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    // Seed dataset roughly equivalent to a SPARQL INSERT DATA payload.
    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "person": "http://example.org/Person#",
            "foaf": "http://xmlns.com/foaf/0.1/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {
                "@id": "ex:jdoe",
                "@type": "ex:Person",
                "person:handle": "jdoe",
                "person:fullName": "Jane Doe",
                "person:favNums": [3, 7, 42, 99]
            },
            {
                "@id": "ex:bbob",
                "@type": "ex:Person",
                "person:handle": "bbob",
                "person:fullName": "Billy Bob",
                "person:favNums": [23]
            },
            {
                "@id": "ex:jbob",
                "@type": "ex:Person",
                "person:handle": "jbob",
                "person:fullName": "Jenny Bob",
                "person:favNums": [8, 6, 7, 5, 3, 0, 9]
            },
            {
                "@id": "ex:fbueller",
                "@type": "ex:Person",
                "person:handle": "dankeshön",
                "person:fullName": "Ferris Bueller",
                "person:email": "fb@example.com"
            },
            { "@id": "ex:alice", "foaf:givenname": "Alice", "foaf:family_name": "Hacker" },
            { "@id": "ex:bob", "foaf:firstname": "Bob", "foaf:surname": "Hacker" },
            {
                "@id": "ex:carol",
                "ex:catchphrase": [
                    {"@value": "Heyyyy", "@language": "en"},
                    {"@value": "¡Eyyyy!", "@language": "es"}
                ]
            }
        ]
    });

    let committed = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert+commit should succeed");
    committed.ledger
}

async fn seed_books(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    let insert = json!({
        "@context": {
            "book": "http://example.org/book/",
            "ex": "http://example.org/book/"
        },
        "@graph": [
            {
                "@id": "book:1",
                "@type": "book:Book",
                "book:title": "For Whom the Bell Tolls"
            },
            {
                "@id": "book:2",
                "@type": "book:Book",
                "book:title": "The Hitchhiker's Guide to the Galaxy"
            }
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert books")
        .ledger
}

// =========================================================================
// SPARQL Property Path Tests: Inverse (^) and Alternative (|)
// =========================================================================

/// Seed a knows-chain for SPARQL property path tests.
///
/// Graph: a→b, b→c, b→d, d→e
async fn sparql_seed_knows_chain(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:a","ex:knows":{"@id":"ex:b"}},
            {"@id":"ex:b","ex:knows":[{"@id":"ex:c"},{"@id":"ex:d"}]},
            {"@id":"ex:d","ex:knows":{"@id":"ex:e"}}
        ]
    });
    fluree.insert(ledger0, &insert).await.unwrap().ledger
}

// =========================================================================
// SPARQL Property Path Tests: Sequence (/)
// =========================================================================

/// Seed chain data for SPARQL sequence tests.
///
/// Graph: alice --friend--> bob --friend--> carol
///        alice --name--> "Alice"
///        bob   --name--> "Bob"
///        carol --name--> "Carol"
///        alice --parent--> bob
///        bob   --parent--> carol
async fn sparql_seed_chain_data(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {
                "@id": "ex:alice",
                "ex:name": "Alice",
                "ex:friend": {"@id": "ex:bob"},
                "ex:parent": {"@id": "ex:bob"}
            },
            {
                "@id": "ex:bob",
                "ex:name": "Bob",
                "ex:friend": {"@id": "ex:carol"},
                "ex:parent": {"@id": "ex:carol"}
            },
            {
                "@id": "ex:carol",
                "ex:name": "Carol"
            }
        ]
    });
    fluree.insert(ledger0, &insert).await.unwrap().ledger
}

// =========================================================================
// SPARQL Property Path Tests: Sequence-in-Alternative
// =========================================================================

/// Seed data for alternative-of-sequences tests.
///
/// Graph:
///   ex:alice --ex:friend--> ex:bob
///   ex:alice --ex:colleague--> ex:carol
///   ex:bob   --ex:name--> "Bob"
///   ex:carol --ex:name--> "Carol"
async fn sparql_seed_alt_seq_data(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:alice","ex:friend":{"@id":"ex:bob"},"ex:colleague":{"@id":"ex:carol"}},
            {"@id":"ex:bob","ex:name":"Bob"},
            {"@id":"ex:carol","ex:name":"Carol"}
        ]
    });
    fluree.insert(ledger0, &insert).await.unwrap().ledger
}

// =============================================================================
// SPARQL Alternative-in-Sequence distribution tests
// =============================================================================

async fn sparql_seed_alt_in_seq_data(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {
                "@id": "ex:alice",
                "ex:name": "Alice",
                "ex:nick": "Ali",
                "ex:friend": {"@id": "ex:bob"}
            },
            {
                "@id": "ex:bob",
                "ex:name": "Bob",
                "ex:nick": "Bobby"
            }
        ]
    });
    fluree.insert(ledger0, &insert).await.unwrap().ledger
}

// ============================================================================
// Custom namespace STR() and full-IRI predicate matching
// ============================================================================

/// Seed data with a custom namespace that is NOT one of the default W3C namespaces.
async fn seed_custom_ns(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    let insert = json!({
        "@context": {
            "cust": "https://taxo.cbcrc.ca/ns/",
            "ex": "http://example.org/ns/"
        },
        "@graph": [
            {
                "@id": "ex:item1",
                "@type": "ex:Item",
                "cust:packageType": "premium",
                "cust:category": "electronics"
            },
            {
                "@id": "ex:item2",
                "@type": "ex:Item",
                "cust:packageType": "standard",
                "cust:category": "books"
            }
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert custom ns data")
        .ledger
}

// =============================================================================
// Bug regression: exact user repro — overlapping namespace prefixes + ref values
// =============================================================================

/// Seed ledger with exact data from the user's bug report.
///
/// Uses overlapping namespace prefixes (`https://taxo.cbcrc.ca/ns/` and
/// `https://taxo.cbcrc.ca/id/`) and ref-valued custom predicates.
async fn seed_exact_repro(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    let insert = json!({
        "@context": {
            "skos": "http://www.w3.org/2004/02/skos/core#",
            "skosxl": "http://www.w3.org/2008/05/skos-xl#",
            "cust": "https://taxo.cbcrc.ca/ns/",
            "cbc": "https://taxo.cbcrc.ca/id/"
        },
        "@graph": [
            {
                "@id": "cust:assocType/coverage",
                "skosxl:prefLabel": {
                    "@id": "cbc:label/assocType-coverage-en",
                    "@type": "skosxl:Label",
                    "skosxl:literalForm": {"@value": "Coverage Package", "@language": "en"}
                }
            },
            {
                "@id": "cbc:assoc/coverage-001",
                "@type": "cust:CoveragePackage",
                "cust:associationType": {"@id": "cust:assocType/coverage"},
                "cust:anchor": {"@id": "https://taxo.cbcrc.ca/id/e9235fd0-c1fc-4f9e-828b-b933922b5764"},
                "cust:member": [
                    {"@id": "https://taxo.cbcrc.ca/id/5b33544d-d6cf-413b-915f-f1f084ba11c7"},
                    {"@id": "https://taxo.cbcrc.ca/id/0476a33f-bcfc-459e-8b6b-e78baa81be3b"}
                ]
            }
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert exact repro data")
        .ledger
}

// =========================================================================
// Built-in Function Coverage: multi-byte chars, TIMEZONE, UUID, isNumeric,
// language-tag preservation
// =========================================================================

/// Seed dataset with multi-byte strings, datetime, and decimal values for
/// built-in function tests.
async fn seed_builtin_fn_data(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {
                "@id": "ex:sushi",
                "ex:label": "食べ物",
                "ex:note": {"@value": "Hola mundo", "@language": "es"},
                "ex:price": {"@value": "12.50", "@type": "xsd:decimal"},
                "ex:created": {"@value": "2024-06-15T10:30:00Z", "@type": "xsd:dateTime"}
            },
            {
                "@id": "ex:beer",
                "ex:label": "Ölympics",
                "ex:note": {"@value": "Good stuff", "@language": "en"},
                "ex:price": {"@value": "7.99", "@type": "xsd:decimal"},
                "ex:created": {"@value": "2024-01-20T14:00:00+05:30", "@type": "xsd:dateTime"}
            }
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("seed builtin fn data")
        .ledger
}

#[tokio::test]
async fn sparql_basic_query_outputs_jsonld_and_sparql_json() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r#"
        PREFIX ex: <http://example.org/ns/>
        PREFIX person: <http://example.org/Person#>
        SELECT ?person ?fullName
        WHERE {
          ?person person:handle "jdoe" .
          ?person person:fullName ?fullName .
        }
    "#;

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("sparql query should succeed");

    // Default output (array rows).
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(jsonld, json!([["ex:jdoe", "Jane Doe"]]));

    // SPARQL JSON output uses compact IRIs.
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    assert_eq!(
        sparql_json,
        json!({
            "head": {"vars": ["fullName", "person"]},
            "results": {"bindings": [
                {
                    "person": {"type": "uri", "value": "ex:jdoe"},
                    "fullName": {"type": "literal", "value": "Jane Doe"}
                }
            ]}
        })
    );
}

#[tokio::test]
async fn sparql_filter_query_outputs_jsonld_and_sparql_json() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle ?favNum
        WHERE {
          ?person person:handle ?handle ;
                  person:favNums ?favNum .
          FILTER ( ?favNum > 10 ) .
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();

    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["bbob", 23], ["jdoe", 42], ["jdoe", 99]]))
    );

    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    // Order is not guaranteed; compare bindings as a set.
    assert_eq!(
        normalize_sparql_bindings(&sparql_json),
        normalize_sparql_bindings(&json!({
            "head": {"vars": ["favNum", "handle"]},
            "results": {"bindings": [
                {
                    "handle": {"type": "literal", "value": "bbob"},
                    "favNum": {"type": "literal", "value": "23", "datatype": "http://www.w3.org/2001/XMLSchema#integer"}
                },
                {
                    "handle": {"type": "literal", "value": "jdoe"},
                    "favNum": {"type": "literal", "value": "42", "datatype": "http://www.w3.org/2001/XMLSchema#integer"}
                },
                {
                    "handle": {"type": "literal", "value": "jdoe"},
                    "favNum": {"type": "literal", "value": "99", "datatype": "http://www.w3.org/2001/XMLSchema#integer"}
                }
            ]}
        }))
    );
}

#[tokio::test]
async fn sparql_count_star_counts_solutions() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT (COUNT(*) AS ?cnt)
        WHERE { ?p a ex:Person . }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(jsonld, json!([[4]]));
}

#[tokio::test]
async fn sparql_count_distinct_with_group_by_and_order_by() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    // Test the user's exact query pattern:
    // SELECT ?handle (COUNT(DISTINCT ?favNum) AS ?distinctCount)
    // WHERE { ... } GROUP BY ?handle ORDER BY DESC(?distinctCount) LIMIT 10
    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle (COUNT(DISTINCT ?favNum) AS ?distinctCount)
        WHERE {
          ?person person:handle ?handle ;
                  person:favNums ?favNum .
        }
        GROUP BY ?handle
        ORDER BY DESC(?distinctCount)
        LIMIT 10
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Expected: jbob has 7 distinct favNums, jdoe has 4, bbob has 1
    // fbueller has no favNums so won't appear
    // ORDER BY DESC means jbob first, then jdoe, then bbob
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["jbob", 7], ["jdoe", 4], ["bbob", 1]]))
    );
}

#[tokio::test]
async fn sparql_order_by_expression_no_aggregation() {
    // Bug 1: expression-based ORDER BY (no aggregation). `(0 - ?favNum)`
    // ascending must produce DESCENDING ?favNum, proving the expression is
    // evaluated per-solution (a bare `ORDER BY ?favNum` would yield the
    // opposite order).
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        PREFIX person: <http://example.org/Person#>
        SELECT ?favNum
        WHERE { ex:jdoe person:favNums ?favNum }
        ORDER BY (0 - ?favNum)
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // jdoe favNums [3,7,42,99]; ascending by (0 - favNum) => 99,42,7,3.
    assert_eq!(jsonld, json!([[99], [42], [7], [3]]));
}

#[tokio::test]
async fn sparql_order_by_expression_over_aggregate_with_limit() {
    // Bug 1 + top-k: expression ORDER BY over an aggregate output is evaluated
    // AFTER grouping (dedicated post-grouping stage). `(0 - ?c)` ascending =>
    // descending count; LIMIT exercises top-k on the synthetic sort key.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle (COUNT(?favNum) AS ?c)
        WHERE {
          ?person person:handle ?handle ;
                  person:favNums ?favNum .
        }
        GROUP BY ?handle
        ORDER BY (0 - ?c)
        LIMIT 2
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // Counts: jbob 7, jdoe 4, bbob 1; descending by count, top 2.
    assert_eq!(jsonld, json!([["jbob", 7], ["jdoe", 4]]));
}

#[tokio::test]
async fn sparql_order_by_desc_expression_with_tiebreak() {
    // BSBM-shaped `ORDER BY DESC(expr) ?tiebreak`: descending computed key then
    // a bare variable tiebreaker, with LIMIT.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle ?favNum
        WHERE {
          ?person person:handle ?handle ;
                  person:favNums ?favNum .
        }
        ORDER BY DESC(?favNum * 1) ?handle
        LIMIT 3
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // Highest favNums: 99 (jdoe), 42 (jdoe), 23 (bbob).
    assert_eq!(jsonld, json!([["jdoe", 99], ["jdoe", 42], ["bbob", 23]]));
}

#[tokio::test]
async fn sparql_order_by_float_ratio_expression() {
    // BSBM BI Q5 shape: `ORDER BY DESC(xsd:float(?count) / N)`. Exercises
    // expression ORDER BY (Bug 1) together with mixed float/int arithmetic in
    // the sort key. Must neither reject at parse/lowering nor error at eval.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle (COUNT(?favNum) AS ?c)
        WHERE {
          ?person person:handle ?handle ;
                  person:favNums ?favNum .
        }
        GROUP BY ?handle
        ORDER BY DESC(xsd:float(?c) / 2)
        LIMIT 10
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // xsd:float(count)/2: jbob 3.5, jdoe 2.0, bbob 0.5 => descending.
    assert_eq!(jsonld, json!([["jbob", 7], ["jdoe", 4], ["bbob", 1]]));
}

#[tokio::test]
async fn sparql_order_by_expression_dedup_only_group_by() {
    // Regression (P1a): expression ORDER BY over a *dedup-only* GROUP BY (no
    // aggregates) must work. The order key is computed by the dedicated
    // post-grouping stage; the group key ?favNum survives grouping as a scalar.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?favNum
        WHERE { ?person person:favNums ?favNum }
        GROUP BY ?favNum
        ORDER BY (0 - ?favNum)
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // Distinct favNums across all people, descending.
    assert_eq!(
        jsonld,
        json!([[99], [42], [23], [9], [8], [7], [6], [5], [3], [0]])
    );
}

#[tokio::test]
async fn sparql_order_by_inline_aggregate_shared_with_select_alias() {
    // `ORDER BY DESC(COUNT(?x))` with an explicit GROUP BY. The inline aggregate
    // is hoisted and deduped against the SELECT alias `?c`, so it sorts by the
    // same count without recomputing it.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle (COUNT(?favNum) AS ?c)
        WHERE {
          ?person person:handle ?handle ;
                  person:favNums ?favNum .
        }
        GROUP BY ?handle
        ORDER BY DESC(COUNT(?favNum))
        LIMIT 10
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(jsonld, json!([["jbob", 7], ["jdoe", 4], ["bbob", 1]]));
}

#[tokio::test]
async fn sparql_order_by_inline_aggregate_not_selected() {
    // `ORDER BY DESC(COUNT(?favNum))` where the aggregate is NOT in the SELECT.
    // It must be hoisted into the grouping with its own synthetic output var so
    // the rows order by a count that never appears in the output.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle
        WHERE {
          ?person person:handle ?handle ;
                  person:favNums ?favNum .
        }
        GROUP BY ?handle
        ORDER BY DESC(COUNT(?favNum))
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // Counts: jbob 7, jdoe 4, bbob 1 → handles in that order, count not projected.
    assert_eq!(jsonld, json!([["jbob"], ["jdoe"], ["bbob"]]));
}

#[tokio::test]
async fn sparql_order_by_inline_aggregate_implicit_single_group() {
    // No explicit GROUP BY: the aggregate in ORDER BY triggers implicit
    // single-group aggregation (must not error).
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT (COUNT(?favNum) AS ?c)
        WHERE { ?person person:favNums ?favNum }
        ORDER BY DESC(COUNT(?favNum))
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // Total favNums across all people = 4 + 1 + 7 = 12 (single group).
    assert_eq!(jsonld, json!([[12]]));
}

#[tokio::test]
async fn sparql_order_by_inline_aggregate_in_compound_expression() {
    // Aggregate nested inside a compound order expression: `DESC(COUNT(?x) * 2)`.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle (COUNT(?favNum) AS ?c)
        WHERE {
          ?person person:handle ?handle ;
                  person:favNums ?favNum .
        }
        GROUP BY ?handle
        ORDER BY DESC(COUNT(?favNum) * 2)
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(jsonld, json!([["jbob", 7], ["jdoe", 4], ["bbob", 1]]));
}

#[tokio::test]
async fn sparql_order_by_expression_count_by_predicate_not_dropped() {
    // Regression: the `?s ?p ?o` / GROUP BY ?p / COUNT(?s) shape can match the
    // stats (and predicate-object) count fast paths, which sort on
    // `query.ordering` directly. With an expression ORDER BY the sort var is
    // synthetic, so a fast path that skips the order-bind stage would silently
    // drop the sort. The result counts must come back strictly grouped and
    // descending here.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        SELECT ?p (COUNT(?s) AS ?c)
        WHERE { ?s ?p ?o }
        GROUP BY ?p
        ORDER BY (0 - ?c)
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    let rows = jsonld.as_array().expect("array of rows");
    let counts: Vec<i64> = rows
        .iter()
        .map(|row| row[1].as_i64().expect("count is an integer"))
        .collect();
    assert!(
        counts.len() >= 2,
        "expected multiple predicate groups, got: {counts:?}"
    );
    assert!(
        counts.windows(2).all(|w| w[0] >= w[1]),
        "ORDER BY (0 - ?c) must yield descending counts, got: {counts:?}"
    );
}

#[tokio::test]
async fn sparql_order_by_expression_over_grouped_var_errors_cleanly() {
    // Regression (P1b): an aggregating query whose ORDER BY expression reads a
    // variable that is neither a GROUP BY key nor an aggregate output must be
    // rejected with a clean error — NOT panic on a Grouped binding.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle (COUNT(?favNum) AS ?c)
        WHERE {
          ?person person:handle ?handle ;
                  person:favNums ?favNum .
        }
        GROUP BY ?handle
        ORDER BY (?favNum + 1)
    ";

    let result = support::query_sparql(&fluree, &ledger, query).await;
    assert!(
        result.is_err(),
        "ORDER BY over a non-grouped variable should error, got: {result:?}"
    );
}

// =============================================================================
// Subquery / top-level modifier unification (subquery-unification proposal).
//
// After unification, a `{ SELECT … }` subquery inherits the full top-level
// solution-modifier tail (HAVING, post-aggregation binds, expression/aggregate
// ORDER BY, ORDER-BY-before-PROJECT, sort-var validation) via the shared
// `apply_solution_modifiers`. These mirror the top-level ORDER BY / aggregate
// tests above. LIMIT inside the subquery makes the returned *set* reflect the
// inner ordering, so `normalize_rows` (set comparison) stays order-robust.
// =============================================================================

#[tokio::test]
async fn sparql_subquery_expression_order_by_with_limit() {
    // Pattern 1: expression ORDER BY inside a subquery — previously REJECTED at
    // lowering, now works. `(0 - ?favNum)` ascending = ?favNum descending; the
    // synthetic order-bind key is evaluated and sorted before project/merge.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?favNum
        WHERE {
          { SELECT ?favNum WHERE { ?person person:favNums ?favNum }
            ORDER BY (0 - ?favNum) LIMIT 1 }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // Max favNum across all people is 99 (jdoe); LIMIT 1 keeps the largest.
    assert_eq!(normalize_rows(&jsonld), normalize_rows(&json!([[99]])));
}

/// Regression: a variable-free subquery (`{ SELECT * WHERE { <ground> } }`)
/// produces an empty schema. When the ground pattern matches it is one
/// empty-binding solution and must NOT collapse to zero rows — otherwise it
/// would wrongly wipe out the joined outer pattern. Guards
/// `SubqueryOperator::drain_buffer`'s empty-schema handling.
#[tokio::test]
async fn sparql_ground_subquery_does_not_wipe_outer_pattern() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    // Outer pattern matches exactly one row (jdoe's full name). The inner
    // subquery is fully ground (jdoe's handle is "jdoe"), projects no variables,
    // and exists — so the join must preserve the single outer row.
    let query = r#"
        PREFIX ex: <http://example.org/ns/>
        PREFIX person: <http://example.org/Person#>
        SELECT ?fullName
        WHERE {
          ex:jdoe person:fullName ?fullName .
          { SELECT * WHERE { ex:jdoe person:handle "jdoe" } }
        }
    "#;

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["Jane Doe"]])),
        "ground matching subquery must yield one empty solution, not zero"
    );
}

#[tokio::test]
async fn sparql_subquery_aggregate_order_by_with_limit() {
    // Pattern 2: aggregate ORDER BY `DESC(COUNT(?favNum))` inside a subquery.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle ?c
        WHERE {
          { SELECT ?handle (COUNT(?favNum) AS ?c)
            WHERE { ?person person:handle ?handle ; person:favNums ?favNum . }
            GROUP BY ?handle
            ORDER BY DESC(COUNT(?favNum))
            LIMIT 1 }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // Counts: jbob 7, jdoe 4, bbob 1; DESC top-1 = jbob.
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["jbob", 7]]))
    );
}

#[tokio::test]
async fn sparql_subquery_aggregate_order_by_not_selected() {
    // Pattern 3: aggregate ORDER BY where the aggregate is NOT in the SELECT.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle
        WHERE {
          { SELECT ?handle
            WHERE { ?person person:handle ?handle ; person:favNums ?favNum . }
            GROUP BY ?handle
            ORDER BY DESC(COUNT(?favNum))
            LIMIT 1 }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // Sorted by an aggregate not projected; top-1 by count = jbob.
    assert_eq!(normalize_rows(&jsonld), normalize_rows(&json!([["jbob"]])));
}

#[tokio::test]
async fn sparql_subquery_having() {
    // Pattern 4: HAVING inside a subquery — previously a parse error, then a
    // dropped clause. Now lowered and applied.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle ?c
        WHERE {
          { SELECT ?handle (COUNT(?favNum) AS ?c)
            WHERE { ?person person:handle ?handle ; person:favNums ?favNum . }
            GROUP BY ?handle
            HAVING (COUNT(?favNum) > 5) }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // Counts: jbob 7 (>5), jdoe 4, bbob 1 → only jbob survives HAVING.
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["jbob", 7]]))
    );
}

#[tokio::test]
async fn sparql_subquery_post_aggregation_bind() {
    // Pattern 5: post-aggregation SELECT expression inside a subquery —
    // previously silently dropped. `(?c + 100 AS ?bumped)` references the
    // aggregate alias `?c`, so it rides as a post-aggregation bind.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle ?bumped
        WHERE {
          { SELECT ?handle (COUNT(?favNum) AS ?c) (?c + 100 AS ?bumped)
            WHERE { ?person person:handle ?handle ; person:favNums ?favNum . }
            GROUP BY ?handle }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // Counts jbob 7, jdoe 4, bbob 1 → bumped = count + 100.
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["jbob", 107], ["jdoe", 104], ["bbob", 101]]))
    );
}

#[tokio::test]
async fn sparql_subquery_order_by_non_projected_var() {
    // Pattern 6: ORDER BY a variable NOT in the subquery SELECT — previously
    // silently unordered (sort ran after project, so the key was gone). Now the
    // sort runs before project, so LIMIT 1 picks the max-?favNum row.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle
        WHERE {
          { SELECT ?handle
            WHERE { ?person person:handle ?handle ; person:favNums ?favNum . }
            ORDER BY DESC(?favNum) LIMIT 1 }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // Max favNum is 99 (jdoe); sort-before-project makes LIMIT 1 deterministic.
    assert_eq!(normalize_rows(&jsonld), normalize_rows(&json!([["jdoe"]])));
}

#[tokio::test]
async fn sparql_subquery_order_by_select_alias_expression() {
    // Pattern 7: ORDER BY a select-expression alias inside a subquery.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle ?s
        WHERE {
          { SELECT ?handle (?favNum + 1000 AS ?s)
            WHERE { ?person person:handle ?handle ; person:favNums ?favNum . }
            ORDER BY DESC(?s) LIMIT 1 }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // ?s = favNum + 1000; max favNum 99 (jdoe) → s = 1099.
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["jdoe", 1099]]))
    );
}

#[tokio::test]
async fn sparql_subquery_full_modifier_stack() {
    // Pattern 8: GROUP BY + HAVING + aggregate ORDER BY + LIMIT in one subquery.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle ?c
        WHERE {
          { SELECT ?handle (COUNT(?favNum) AS ?c)
            WHERE { ?person person:handle ?handle ; person:favNums ?favNum . }
            GROUP BY ?handle
            HAVING (COUNT(?favNum) > 0)
            ORDER BY DESC(COUNT(?favNum))
            LIMIT 2 }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // Counts jbob 7, jdoe 4, bbob 1; HAVING>0 keeps all; DESC top-2 = jbob, jdoe.
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["jbob", 7], ["jdoe", 4]]))
    );
}

#[tokio::test]
async fn sparql_correlated_subquery_group_by_aggregate_order_by() {
    // Pattern 9: correlated subquery + GROUP BY + aggregate ORDER BY, with the
    // per-row correlation preserved. The outer binds ?handle; the subquery
    // correlates on ?handle (it appears in the subquery SELECT) and counts that
    // person's favNums.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle ?c
        WHERE {
          ?person person:handle ?handle .
          { SELECT ?handle (COUNT(?favNum) AS ?c)
            WHERE { ?p2 person:handle ?handle ; person:favNums ?favNum . }
            GROUP BY ?handle
            ORDER BY DESC(COUNT(?favNum)) }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // Each handle with its own favNum count; dankeshön (no favNums) drops out.
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["jbob", 7], ["jdoe", 4], ["bbob", 1]]))
    );
}

#[tokio::test]
async fn sparql_subquery_distinct_order_by_limit_reorder() {
    // Pipeline-reorder safety: `SELECT DISTINCT ?x … ORDER BY DESC(?x) LIMIT k`
    // (ORDER BY references only projected vars) takes the project-distinct-
    // before-sort path. It must still yield the top-k distinct values.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?favNum
        WHERE {
          { SELECT DISTINCT ?favNum
            WHERE { ?person person:favNums ?favNum }
            ORDER BY DESC(?favNum) LIMIT 3 }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // Distinct favNums {0,3,5,6,7,8,9,23,42,99}; DESC top-3 = 99, 42, 23.
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([[99], [42], [23]]))
    );
}

#[tokio::test]
async fn sparql_subquery_having_eliminates_all_groups() {
    // HAVING inside a subquery that NO group satisfies must yield an empty
    // result — proving the clause actually filters rather than passing through.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle ?c
        WHERE {
          { SELECT ?handle (COUNT(?favNum) AS ?c)
            WHERE { ?person person:handle ?handle ; person:favNums ?favNum . }
            GROUP BY ?handle
            HAVING (COUNT(?favNum) > 100) }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // Max count is 7 (jbob); none exceed 100 → every group removed.
    assert_eq!(normalize_rows(&jsonld), normalize_rows(&json!([])));
}

#[tokio::test]
async fn sparql_correlated_subquery_order_by_limit_per_row() {
    // Correlated subquery where the inner ORDER BY + LIMIT genuinely matter:
    // for each outer ?person, pick that person's MAX favNum via
    // `ORDER BY DESC(?top) LIMIT 1`. Proves per-row correlation seeding is
    // preserved AND the inner sort/limit run before the merge-back.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle ?top
        WHERE {
          ?person person:handle ?handle .
          { SELECT ?person ?top
            WHERE { ?person person:favNums ?top }
            ORDER BY DESC(?top)
            LIMIT 1 }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // Per-person max favNum: jdoe 99, bbob 23, jbob 9; dankeshön (no favNums) drops.
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["jdoe", 99], ["bbob", 23], ["jbob", 9]]))
    );
}

#[tokio::test]
async fn sparql_subquery_group_concat_sum_respects_inner_offset() {
    // Regression: the fused SUM(STRLEN(GROUP_CONCAT(..))) fast path sums over
    // EVERY group and ignores the inner subquery's slice/distinct modifiers, so
    // it must decline when any are present. Here OFFSET skips all groups, so the
    // correct answer is the empty/zero sum — NOT the all-groups sum the fast path
    // would otherwise return.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    // Baseline: no inner modifier → SUM of STRLEN(GROUP_CONCAT) over all groups.
    // Per-group lengths (separator=""): jdoe "374299"=6, bbob "23"=2,
    // jbob "8675309"=7 → 15 (STRLEN is order-invariant).
    let all = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT (SUM(STRLEN(?cat)) AS ?total)
        WHERE {
          { SELECT ?person (GROUP_CONCAT(?favNum; SEPARATOR="") AS ?cat)
            WHERE { ?person person:favNums ?favNum }
            GROUP BY ?person }
        }
    "#;
    let all_rows = support::query_sparql(&fluree, &ledger, all)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(all_rows, json!([[15]]));

    // Inner OFFSET past every group → empty subquery → sum over nothing (0).
    let offset = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT (SUM(STRLEN(?cat)) AS ?total)
        WHERE {
          { SELECT ?person (GROUP_CONCAT(?favNum; SEPARATOR="") AS ?cat)
            WHERE { ?person person:favNums ?favNum }
            GROUP BY ?person
            OFFSET 100 }
        }
    "#;
    let offset_rows = support::query_sparql(&fluree, &ledger, offset)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    // With the guard, the fast path declines and the generic pipeline honors the
    // OFFSET: the subquery is empty, so SUM has nothing to add (unbound). Without
    // the guard this would wrongly equal the all-groups sum (15).
    assert_eq!(offset_rows, json!([[null]]));
}

#[tokio::test]
async fn sparql_aggregate_over_expression_in_joined_subqueries() {
    // Regression (benchmark-db bug #4, BSBM BI-5): an aggregate over a COMPUTED
    // EXPRESSION (`AVG(xsd:float(?n))`) inside a grouped sub-SELECT used to fail
    // when two such sub-SELECTs are joined — the shared aggregate-input
    // expression was CSE-deduplicated across subquery scopes, leaving the second
    // subquery's synthetic input var unbound. Each subquery must get its own.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT ?handle {
          { SELECT ?handle (AVG(xsd:float(?favNum)) AS ?x)
            WHERE { ?p person:handle ?handle ; person:favNums ?favNum }
            GROUP BY ?handle }
          { SELECT ?handle (AVG(xsd:float(?favNum)) AS ?y)
            WHERE { ?p2 person:handle ?handle ; person:favNums ?favNum }
            GROUP BY ?handle }
        }
    ";

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("aggregate-over-expression in joined sub-SELECTs should not error")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    // The two grouped sub-SELECTs join on ?handle; handles with favNums are
    // jdoe, bbob, jbob (dankeshön has none).
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["jdoe"], ["bbob"], ["jbob"]]))
    );
}

#[tokio::test]
async fn sparql_group_by_unbound_variable() {
    // SPARQL 1.1 §11.4 / §18.5 (benchmark-db bug #5, BSBM BI-4): GROUP BY (and
    // SELECT) of a variable the pattern never binds is legal — the variable is
    // unbound, so all solutions share it and collapse to one group per the bound
    // keys. Previously this failed at plan time ("GROUP BY variable not found in
    // query schema").
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    // ?country is never bound in the WHERE.
    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?country ?handle (COUNT(?favNum) AS ?c)
        WHERE { ?p person:handle ?handle ; person:favNums ?favNum }
        GROUP BY ?country ?handle
    ";

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("GROUP BY an unbound variable is legal")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    // ?country unbound (null) for every group; counts: jdoe 4, bbob 1, jbob 7.
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([
            [null, "jdoe", 4],
            [null, "bbob", 1],
            [null, "jbob", 7]
        ]))
    );
}

#[tokio::test]
async fn sparql_select_unbound_variable_no_grouping() {
    // Companion to the GROUP BY case: selecting a never-bound variable in an
    // ungrouped query reports it unbound rather than erroring.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle ?missing
        WHERE { ?p person:handle ?handle }
    ";

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("selecting an unbound variable is legal")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    // Each handle (column order ?handle ?missing) with ?missing unbound (null).
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([
            ["jdoe", null],
            ["bbob", null],
            ["jbob", null],
            ["dankeshön", null]
        ]))
    );
}

#[tokio::test]
async fn sparql_filter_equality_equijoin_results_preserved() {
    // Performance optimization (benchmark-db perf #1, BSBM BI-2): FILTER(?x = ?y)
    // between two ref-valued triple objects folds into an equijoin (var unify).
    // This verifies the rewrite preserves results — the shared-feature count per
    // product, the BI-2 shape.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "features:main");
    let insert = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": [
            {"@id":"ex:p1","ex:feature":[{"@id":"ex:fa"},{"@id":"ex:fb"},{"@id":"ex:fc"}]},
            {"@id":"ex:p2","ex:feature":[{"@id":"ex:fa"},{"@id":"ex:fb"}]},
            {"@id":"ex:p3","ex:feature":[{"@id":"ex:fc"},{"@id":"ex:fd"}]},
            {"@id":"ex:p4","ex:feature":[{"@id":"ex:fx"}]}
        ]
    });
    let ledger = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert+commit should succeed")
        .ledger;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?other (COUNT(?f2) AS ?shared)
        WHERE {
          ex:p1 ex:feature ?f1 .
          ?other ex:feature ?f2 .
          FILTER(?f1 = ?f2)
        }
        GROUP BY ?other
    ";

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    // Shared features with p1's {fa,fb,fc}: p1 itself 3, p2 {fa,fb}=2, p3 {fc}=1;
    // p4 ({fx}) shares none, so it never binds ?f2 and is absent.
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["ex:p1", 3], ["ex:p2", 2], ["ex:p3", 1]]))
    );
}

#[tokio::test]
async fn sparql_filter_equality_equijoin_inside_subquery() {
    // The exact BSBM BI-2 shape: the FILTER(?f1 = ?f2) equijoin lives inside a
    // grouped sub-SELECT. The fold must recurse into the subquery scope and
    // preserve results (the aggregate COUNT(?f2) follows the unified variable).
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "features:sub");
    let insert = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": [
            {"@id":"ex:p1","ex:feature":[{"@id":"ex:fa"},{"@id":"ex:fb"},{"@id":"ex:fc"}]},
            {"@id":"ex:p2","ex:feature":[{"@id":"ex:fa"},{"@id":"ex:fb"}]},
            {"@id":"ex:p3","ex:feature":[{"@id":"ex:fc"},{"@id":"ex:fd"}]}
        ]
    });
    let ledger = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert+commit should succeed")
        .ledger;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?other ?shared {
          { SELECT ?other (COUNT(?f2) AS ?shared)
            {
              ex:p1 ex:feature ?f1 .
              ?other ex:feature ?f2 .
              FILTER(?f1 = ?f2)
            }
            GROUP BY ?other }
        }
    ";

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["ex:p1", 3], ["ex:p2", 2], ["ex:p3", 1]]))
    );
}

#[tokio::test]
async fn sparql_delete_data_removes_specified_triples() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    // Equivalent SPARQL: DELETE DATA { ex:jdoe person:favNums 3 . ex:jdoe person:favNums 7 . }
    // Represented as a JSON-LD Update transaction (no WHERE needed).
    let delete_txn = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "person": "http://example.org/Person#"
        },
        "delete": [
            {"@id": "ex:jdoe", "person:favNums": 3},
            {"@id": "ex:jdoe", "person:favNums": 7}
        ]
    });

    let ledger2 = fluree.update(ledger, &delete_txn).await.unwrap().ledger;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        PREFIX person: <http://example.org/Person#>
        SELECT ?favNum
        WHERE { ex:jdoe person:favNums ?favNum }
        ORDER BY ?favNum
    ";

    let result = support::query_sparql(&fluree, &ledger2, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger2.snapshot).expect("to_jsonld");
    assert_eq!(jsonld, json!([[42], [99]]));
}

#[tokio::test]
async fn sparql_select_star_returns_object_rows() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        PREFIX person: <http://example.org/Person#>
        SELECT *
        WHERE {
          ?person person:handle ?handle ;
                  person:favNums ?favNums .
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    let expected = json!([
        {"?person": "ex:bbob", "?handle": "bbob", "?favNums": 23},
        {"?person": "ex:jdoe", "?handle": "jdoe", "?favNums": 3},
        {"?person": "ex:jdoe", "?handle": "jdoe", "?favNums": 7},
        {"?person": "ex:jdoe", "?handle": "jdoe", "?favNums": 42},
        {"?person": "ex:jdoe", "?handle": "jdoe", "?favNums": 99},
        {"?person": "ex:jbob", "?handle": "jbob", "?favNums": 0},
        {"?person": "ex:jbob", "?handle": "jbob", "?favNums": 3},
        {"?person": "ex:jbob", "?handle": "jbob", "?favNums": 5},
        {"?person": "ex:jbob", "?handle": "jbob", "?favNums": 6},
        {"?person": "ex:jbob", "?handle": "jbob", "?favNums": 7},
        {"?person": "ex:jbob", "?handle": "jbob", "?favNums": 8},
        {"?person": "ex:jbob", "?handle": "jbob", "?favNums": 9}
    ]);

    assert_eq!(
        normalize_object_rows(&jsonld),
        normalize_object_rows(&expected)
    );
}

#[tokio::test]
async fn sparql_lang_filter_limits_language_tagged_literals() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r#"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?phrase
        WHERE {
          ex:carol ex:catchphrase ?phrase .
          FILTER ( LANG(?phrase) = "en" ) .
        }
    "#;

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(jsonld, json!([[{"@value": "Heyyyy", "@language": "en"}]]));
}

#[tokio::test]
async fn sparql_union_combines_unioned_patterns() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT ?name
        WHERE {
          { ?s foaf:givenname ?name }
          UNION
          { ?s foaf:firstname ?name }
        }
        ORDER BY ?name
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(jsonld, json!([["Alice"], ["Bob"]]));
}

#[tokio::test]
async fn sparql_optional_includes_unbound_values_as_null() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        PREFIX person: <http://example.org/Person#>
        SELECT ?person ?favNums
        WHERE {
          ?person person:handle ?handle .
          OPTIONAL { ?person person:favNums ?favNums . }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    let expected = json!([
        ["ex:bbob", 23],
        ["ex:fbueller", null],
        ["ex:jbob", 0],
        ["ex:jbob", 3],
        ["ex:jbob", 5],
        ["ex:jbob", 6],
        ["ex:jbob", 7],
        ["ex:jbob", 8],
        ["ex:jbob", 9],
        ["ex:jdoe", 3],
        ["ex:jdoe", 7],
        ["ex:jdoe", 42],
        ["ex:jdoe", 99]
    ]);

    assert_eq!(normalize_rows(&jsonld), normalize_rows(&expected));
}

#[tokio::test]
async fn sparql_optional_multi_pattern_requires_conjunctive_match() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        PREFIX person: <http://example.org/Person#>
        SELECT ?person ?favNums ?email
        WHERE {
          ?person person:handle ?handle .
          OPTIONAL {
            ?person person:favNums ?favNums .
            ?person person:email ?email .
          }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    let expected = json!([
        ["ex:bbob", null, null],
        ["ex:fbueller", null, null],
        ["ex:jbob", null, null],
        ["ex:jdoe", null, null]
    ]);

    assert_eq!(normalize_rows(&jsonld), normalize_rows(&expected));
}

#[tokio::test]
async fn sparql_group_by_with_optional_preserves_grouped_lists() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        PREFIX person: <http://example.org/Person#>
        SELECT ?person ?favNums
        WHERE {
          ?person person:handle ?handle .
          OPTIONAL { ?person person:favNums ?favNums . }
        }
        GROUP BY ?person
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    let expected = json!([
        ["ex:bbob", [23]],
        ["ex:fbueller", [null]],
        ["ex:jbob", [0, 3, 5, 6, 7, 8, 9]],
        ["ex:jdoe", [3, 7, 42, 99]]
    ]);

    assert_eq!(normalize_rows(&jsonld), normalize_rows(&expected));
}

#[tokio::test]
async fn sparql_omitted_subjects_match_expanded_subject_bindings() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r#"
        PREFIX ex: <http://example.org/ns/>
        PREFIX person: <http://example.org/Person#>
        SELECT ?person ?fullName ?favNums
        WHERE {
          ?person person:handle "jdoe" ;
                  person:fullName ?fullName ;
                  person:favNums ?favNums .
        }
    "#;

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    let expected = json!([
        ["ex:jdoe", "Jane Doe", 3],
        ["ex:jdoe", "Jane Doe", 7],
        ["ex:jdoe", "Jane Doe", 42],
        ["ex:jdoe", "Jane Doe", 99]
    ]);

    assert_eq!(normalize_rows(&jsonld), normalize_rows(&expected));
}

#[tokio::test]
async fn sparql_scalar_sha512_function_binds_values() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT (SHA512(?handle) AS ?handleHash)
        WHERE { ?person person:handle ?handle . }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    let expected = json!([
        ["f162b1f2b3a824f459164fe40ffc24a019993058061ca1bf90eca98a4652f98ccaa5f17496be3da45ce30a1f79f45d82d8b8b532c264d4455babc1359aaa461d"],
        ["eca2f5ab92fddbf2b1c51a60f5269086ce2415cb37964a05ae8a0b999625a8a50df876e97d34735ebae3fa3abb088fca005a596312fdf3326c4e73338f4c8c90"],
        ["696ba1c7597f0d80287b8f0917317a904fa23a8c25564331a0576a482342d3807c61eff8e50bf5cf09859cfdeb92d448490073f34fb4ea4be43663d2359b51a9"],
        ["fee256e1850ef33410630557356ea3efd56856e9045e59350dbceb6b5794041d50991093c07ad871e1124e6961f2198c178057cf391435051ac24eb8952bc401"]
    ]);

    assert_eq!(normalize_rows(&jsonld), normalize_rows(&expected));
}

#[tokio::test]
async fn sparql_aggregate_avg_over_values() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT (AVG(?favNums) AS ?avgFav)
        WHERE { ?person person:favNums ?favNums . }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Per W3C, AVG of integers yields xsd:decimal — JSON-LD renders decimals
    // as strings to preserve exactness (vs. xsd:double which renders as a
    // number). Parse the string and check the numeric value.
    let avg_cell = jsonld
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|row| row.as_array())
        .and_then(|row| row.first())
        .expect("avg cell");
    let avg: f64 = avg_cell
        .as_str()
        .expect("avg rendered as decimal string")
        .parse()
        .expect("decimal parses as number");
    assert!((avg - 17.666_666_666_666_67).abs() < 1e-12);
}

#[tokio::test]
async fn sparql_group_by_having_filters_groups() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT (AVG(?favNums) AS ?avgFav)
        WHERE { ?person person:favNums ?favNums . }
        GROUP BY ?person
        HAVING (AVG(?favNums) > 10)
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Per W3C, AVG of integers yields xsd:decimal — JSON-LD serializes
    // decimals as strings to preserve precision.
    let mut values: Vec<f64> = jsonld
        .as_array()
        .expect("avg rows array")
        .iter()
        .flat_map(|row| row.as_array().expect("row array").iter())
        .map(|cell| {
            cell.as_str()
                .expect("avg cell rendered as decimal string")
                .parse::<f64>()
                .expect("parses as number")
        })
        .collect();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let expected = [23.0, 37.75];
    assert_eq!(values.len(), expected.len());
    for (actual, target) in values.iter().zip(expected.iter()) {
        assert!((*actual - *target).abs() < 1e-12);
    }
}

#[tokio::test]
async fn sparql_having_with_multiple_string_constraints() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle
        WHERE { ?person person:handle ?handle . }
        GROUP BY ?person ?handle
        HAVING (STRLEN(?handle) < 5 && (STRSTARTS(?handle, "foo") || STRSTARTS(?handle, "bar")))
    "#;

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(jsonld, json!([]));
}

#[tokio::test]
async fn sparql_having_aggregate_without_select_alias() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        PREFIX person: <http://example.org/Person#>
        SELECT ?person
        WHERE { ?person person:favNums ?favNums . }
        GROUP BY ?person
        HAVING (COUNT(?favNums) > 4)
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(jsonld, json!([["ex:jbob"]]));
}

#[tokio::test]
async fn sparql_multiple_select_expressions_with_aggregate_alias() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT (AVG(?favNums) AS ?avgFav) (CEIL(?avgFav) AS ?caf)
        WHERE { ?person person:favNums ?favNums . }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    let rows = normalize_rows(&jsonld);
    assert_eq!(rows.len(), 1);
    // Per W3C, AVG of integers yields xsd:decimal (JSON-LD: string);
    // CEIL of an xsd:decimal yields xsd:decimal too.
    let avg: f64 = rows[0][0]
        .as_str()
        .expect("avg as decimal string")
        .parse()
        .expect("parses");
    let ceil: f64 = rows[0][1]
        .as_str()
        .expect("ceil as decimal string")
        .parse()
        .expect("parses");
    assert!((avg - 17.666_666_666_666_67).abs() < 1e-12);
    assert!((ceil - 18.0).abs() < 1e-12);
}

#[tokio::test]
async fn sparql_group_concat_aggregate_per_group() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT (GROUP_CONCAT(?favNums; separator=", ") AS ?nums)
        WHERE { ?person person:favNums ?favNums . }
        GROUP BY ?person
    "#;

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["0, 3, 5, 6, 7, 8, 9"], ["3, 7, 42, 99"], ["23"]]))
    );
}

#[tokio::test]
async fn sparql_concat_function_formats_strings() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT (CONCAT(?handle, "-", ?fullName) AS ?hfn)
        WHERE {
          ?person person:handle ?handle .
          ?person person:fullName ?fullName .
        }
    "#;

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([
            ["bbob-Billy Bob"],
            ["dankeshön-Ferris Bueller"],
            ["jbob-Jenny Bob"],
            ["jdoe-Jane Doe"]
        ]))
    );
}

#[tokio::test]
async fn sparql_mix_of_grouped_values_and_aggregates() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        PREFIX person: <http://example.org/Person#>
        SELECT ?favNums (AVG(?favNums) AS ?avg) ?person ?handle (MAX(?favNums) AS ?max)
        WHERE {
          ?person person:handle ?handle .
          ?person person:favNums ?favNums .
        }
        GROUP BY ?person ?handle
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    let mut rows: Vec<(String, String, Vec<i64>, f64, i64)> = normalize_rows(&jsonld)
        .into_iter()
        .map(|row| {
            let fav_nums = row[0]
                .as_array()
                .expect("favNums array")
                .iter()
                .map(|v| v.as_i64().expect("favNum"))
                .collect::<Vec<_>>();
            // AVG of integers → xsd:decimal (JSON string).
            let avg: f64 = row[1]
                .as_str()
                .expect("avg as decimal string")
                .parse()
                .expect("parses");
            let person = row[2].as_str().expect("person").to_string();
            let handle = row[3].as_str().expect("handle").to_string();
            let max = row[4].as_i64().expect("max");
            (person, handle, fav_nums, avg, max)
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));

    let expected = [
        (
            "ex:bbob".to_string(),
            "bbob".to_string(),
            vec![23],
            23.0,
            23,
        ),
        (
            "ex:jbob".to_string(),
            "jbob".to_string(),
            vec![0, 3, 5, 6, 7, 8, 9],
            5.428_571_428_571_429,
            9,
        ),
        (
            "ex:jdoe".to_string(),
            "jdoe".to_string(),
            vec![3, 7, 42, 99],
            37.75,
            99,
        ),
    ];

    assert_eq!(rows.len(), expected.len());
    for (actual, target) in rows.iter().zip(expected.iter()) {
        assert_eq!(actual.0, target.0);
        assert_eq!(actual.1, target.1);
        assert_eq!(actual.2, target.2);
        assert!((actual.3 - target.3).abs() < 1e-12);
        assert_eq!(actual.4, target.4);
    }
}

#[tokio::test]
async fn sparql_count_aggregate_per_group() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT (COUNT(?favNums) AS ?numFavs)
        WHERE { ?person person:favNums ?favNums . }
        GROUP BY ?person
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([[7], [4], [1]]))
    );
}

#[tokio::test]
async fn sparql_count_star_per_group() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT (COUNT(*) AS ?count)
        WHERE { ?person person:favNums ?favNums . }
        GROUP BY ?person
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([[7], [4], [1]]))
    );
}

#[tokio::test]
async fn sparql_sample_aggregate_returns_one_value() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT (SAMPLE(?favNums) AS ?favNum)
        WHERE { ?person person:favNums ?favNums . }
        GROUP BY ?person
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    let rows = normalize_rows(&jsonld);
    assert_eq!(rows.len(), 3);
    for row in rows {
        assert!(row[0].as_i64().is_some());
    }
}

#[tokio::test]
async fn sparql_sum_aggregate_per_group() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT (SUM(?favNums) AS ?favNum)
        WHERE { ?person person:favNums ?favNums . }
        GROUP BY ?person
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([[38], [151], [23]]))
    );
}

#[tokio::test]
async fn sparql_sum_boolean_comparison_counts_true_rows() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT (SUM(?favNums > 10) AS ?count)
        WHERE { ?person person:favNums ?favNums . }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(normalize_rows(&jsonld), normalize_rows(&json!([[3]])));
}

#[tokio::test]
async fn sparql_order_by_ascending_sorts_results() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle
        WHERE { ?person person:handle ?handle . }
        ORDER BY ?handle
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(jsonld, json!([["bbob"], ["dankeshön"], ["jbob"], ["jdoe"]]));
}

#[tokio::test]
async fn sparql_order_by_descending_sorts_results() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle
        WHERE { ?person person:handle ?handle . }
        ORDER BY DESC(?handle)
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(jsonld, json!([["jdoe"], ["jbob"], ["dankeshön"], ["bbob"]]));
}

#[tokio::test]
async fn sparql_values_filters_bindings() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle
        WHERE {
          VALUES ?handle { "jdoe" "bbob" }
          ?person person:handle ?handle .
        }
    "#;

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["bbob"], ["jdoe"]]))
    );
}

#[tokio::test]
async fn sparql_construct_query_outputs_jsonld_graph() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        CONSTRUCT {
          ?x ex:givenName ?gname .
          ?x ex:familyName ?fname .
        }
        WHERE {
          { ?x foaf:firstname ?gname } UNION { ?x foaf:givenname ?gname } .
          { ?x foaf:surname ?fname } UNION { ?x foaf:family_name ?fname } .
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_construct(&ledger.snapshot).expect("to_construct");

    let expected = json!({
        "@context": {
            "ex": "http://example.org/ns/"
        },
        "@graph": [
            {
                "@id": "ex:alice",
                "ex:givenName": ["Alice"],
                "ex:familyName": ["Hacker"]
            },
            {
                "@id": "ex:bob",
                "ex:givenName": ["Bob"],
                "ex:familyName": ["Hacker"]
            }
        ]
    });

    let mut json_graph = jsonld
        .get("@graph")
        .and_then(|v| v.as_array())
        .expect("@graph array")
        .clone();
    let mut expected_graph = expected
        .get("@graph")
        .and_then(|v| v.as_array())
        .expect("@graph array")
        .clone();

    let sort_by_id = |a: &JsonValue, b: &JsonValue| {
        a.get("@id")
            .and_then(|v| v.as_str())
            .cmp(&b.get("@id").and_then(|v| v.as_str()))
    };
    json_graph.sort_by(sort_by_id);
    expected_graph.sort_by(sort_by_id);

    assert_eq!(json_graph, expected_graph);
}

#[tokio::test]
async fn sparql_construct_where_outputs_graph() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        CONSTRUCT WHERE { ?x foaf:firstname ?fname }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_construct(&ledger.snapshot).expect("to_construct");

    let mut json_graph = jsonld
        .get("@graph")
        .and_then(|v| v.as_array())
        .expect("@graph array")
        .clone();
    json_graph.sort_by(|a, b| {
        a.get("@id")
            .and_then(|v| v.as_str())
            .cmp(&b.get("@id").and_then(|v| v.as_str()))
    });

    let expected = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "foaf": "http://xmlns.com/foaf/0.1/"
        },
        "@graph": [
            {
                "@id": "ex:bob",
                "foaf:firstname": ["Bob"]
            }
        ]
    });

    let mut expected_graph = expected
        .get("@graph")
        .and_then(|v| v.as_array())
        .expect("@graph array")
        .clone();
    expected_graph.sort_by(|a, b| {
        a.get("@id")
            .and_then(|v| v.as_str())
            .cmp(&b.get("@id").and_then(|v| v.as_str()))
    });

    assert_eq!(json_graph, expected_graph);
}

#[tokio::test]
async fn sparql_base_iri_compacts_relative_ids() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "books:main";
    let ledger = seed_books(&fluree, ledger_id).await;

    let query = r"
        BASE <http://example.org/book/>
        SELECT ?book ?title
        WHERE { ?book <title> ?title . }
        ORDER BY ?book
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([
            ["1", "For Whom the Bell Tolls"],
            ["2", "The Hitchhiker's Guide to the Galaxy"]
        ]))
    );
}

#[tokio::test]
async fn sparql_prefix_declarations_compact_ids() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "books:main";
    let ledger = seed_books(&fluree, ledger_id).await;

    let query = r"
        PREFIX book: <http://example.org/book/>
        SELECT ?book ?title
        WHERE { ?book book:title ?title . }
        ORDER BY ?book
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([
            ["book:1", "For Whom the Bell Tolls"],
            ["book:2", "The Hitchhiker's Guide to the Galaxy"]
        ]))
    );
}

#[tokio::test]
async fn sparql_sparql_json_language_tags() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?catchphrase
        WHERE { ex:carol ex:catchphrase ?catchphrase }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");

    let bindings = sparql_json
        .get("results")
        .and_then(|r| r.get("bindings"))
        .and_then(|b| b.as_array())
        .expect("bindings array");

    assert_eq!(bindings.len(), 2);
    for binding in bindings {
        let lang = binding
            .get("catchphrase")
            .and_then(|v| v.get("xml:lang"))
            .and_then(|v| v.as_str())
            .expect("xml:lang");
        assert!(lang == "en" || lang == "es");
    }
}

#[tokio::test]
async fn sparql_concat_with_langtag_argument() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "people:main";
    let ledger = seed_people(&fluree, ledger_id).await;

    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT (CONCAT(?fullName, "'s handle is "@en, ?handle) AS ?hfn)
        WHERE {
          ?person person:handle ?handle .
          ?person person:fullName ?fullName .
        }
    "#;

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([
            ["Billy Bob's handle is bbob"],
            ["Ferris Bueller's handle is dankeshön"],
            ["Jenny Bob's handle is jbob"],
            ["Jane Doe's handle is jdoe"]
        ]))
    );
}

#[tokio::test]
async fn sparql_property_path_inverse_object_var() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = sparql_seed_knows_chain(&fluree, "sparql/path-inv-o:main").await;

    // ^ex:knows from ex:b → who points to b? → a
    let query = "\
        PREFIX ex: <http://example.org/>
        SELECT ?who WHERE { ex:b ^ex:knows ?who }";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("inverse path query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(normalize_rows(&jsonld), normalize_rows(&json!([["ex:a"]])));
}

#[tokio::test]
async fn sparql_property_path_inverse_subject_var() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = sparql_seed_knows_chain(&fluree, "sparql/path-inv-s:main").await;

    // ?who ^ex:knows ex:a → who is known-by a? → b
    let query = "\
        PREFIX ex: <http://example.org/>
        SELECT ?who WHERE { ?who ^ex:knows ex:a }";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("inverse path subject var query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(normalize_rows(&jsonld), normalize_rows(&json!([["ex:b"]])));
}

#[tokio::test]
async fn sparql_property_path_alternative_object_var() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "sparql/path-alt-o:main");
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:a","ex:knows":{"@id":"ex:b"}},
            {"@id":"ex:a","ex:likes":{"@id":"ex:x"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    // ex:knows|ex:likes from ex:a → ex:b and ex:x
    let query = "\
        PREFIX ex: <http://example.org/>
        SELECT ?o WHERE { ex:a ex:knows|ex:likes ?o }";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("alternative path query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["ex:b"], ["ex:x"]]))
    );
}

#[tokio::test]
async fn sparql_property_path_alternative_with_inverse() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = sparql_seed_knows_chain(&fluree, "sparql/path-alt-inv:main").await;

    // ex:knows|^ex:knows from ex:b → forward (c, d) + inverse (a)
    let query = "\
        PREFIX ex: <http://example.org/>
        SELECT ?who WHERE { ex:b ex:knows|^ex:knows ?who }";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("alternative with inverse query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["ex:a"], ["ex:c"], ["ex:d"]]))
    );
}

#[tokio::test]
async fn sparql_property_path_alternative_three_way() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "sparql/path-alt-3:main");
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:a","ex:knows":{"@id":"ex:b"}},
            {"@id":"ex:a","ex:likes":{"@id":"ex:c"}},
            {"@id":"ex:a","ex:trusts":{"@id":"ex:d"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    // ex:knows|ex:likes|ex:trusts from ex:a → b, c, d
    let query = "\
        PREFIX ex: <http://example.org/>
        SELECT ?o WHERE { ex:a ex:knows|ex:likes|ex:trusts ?o }";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("three-way alternative query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["ex:b"], ["ex:c"], ["ex:d"]]))
    );
}

#[tokio::test]
async fn sparql_property_path_alternative_duplicate_semantics() {
    // When both predicates match the same (s,o) pair, UNION bag semantics
    // produces the result twice (one per branch).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "sparql/path-alt-dup:main");
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:a","ex:knows":{"@id":"ex:b"}},
            {"@id":"ex:a","ex:likes":{"@id":"ex:b"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let query = "\
        PREFIX ex: <http://example.org/>
        SELECT ?o WHERE { ex:a ex:knows|ex:likes ?o }";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("duplicate semantics query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // Bag semantics: ex:b appears once per matching branch
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["ex:b"], ["ex:b"]]))
    );
}

#[tokio::test]
async fn sparql_property_path_alternation_under_transitive() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = sparql_seed_knows_chain(&fluree, "sparql/path-alt-trans:main").await;

    // `(ex:knows|ex:likes)+` — an alternation inside a transitive path follows
    // an edge of either predicate per hop. Here only `knows` edges exist
    // (a→b→{c,d}, d→e), so the closure from ex:a is {b, c, d, e}.
    let query = "\
        PREFIX ex: <http://example.org/>
        SELECT ?o WHERE { ex:a (ex:knows|ex:likes)+ ?o }";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("alternation under transitive now supported");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["ex:b"], ["ex:c"], ["ex:d"], ["ex:e"]])),
        "(knows|likes)+ closure over the knows chain: {jsonld}"
    );
}

#[tokio::test]
async fn sparql_property_path_sequence_two_step() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = sparql_seed_chain_data(&fluree, "sparql/path-seq-2:main").await;

    // ex:friend/ex:name from ex:alice → bob's name → "Bob"
    let query = "\
        PREFIX ex: <http://example.org/>
        SELECT ?name WHERE { ex:alice ex:friend/ex:name ?name }";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("two-step sequence query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(normalize_rows(&jsonld), normalize_rows(&json!([["Bob"]])));
}

#[tokio::test]
async fn sparql_property_path_sequence_three_step() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = sparql_seed_chain_data(&fluree, "sparql/path-seq-3:main").await;

    // ex:friend/ex:friend/ex:name from ex:alice → carol's name → "Carol"
    let query = "\
        PREFIX ex: <http://example.org/>
        SELECT ?name WHERE { ex:alice ex:friend/ex:friend/ex:name ?name }";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("three-step sequence query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(normalize_rows(&jsonld), normalize_rows(&json!([["Carol"]])));
}

#[tokio::test]
async fn sparql_property_path_sequence_with_inverse() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = sparql_seed_chain_data(&fluree, "sparql/path-seq-inv:main").await;

    // ^ex:friend/ex:name from ex:bob → who has ex:bob as friend (alice) → alice's name → "Alice"
    let query = "\
        PREFIX ex: <http://example.org/>
        SELECT ?name WHERE { ex:bob ^ex:friend/ex:name ?name }";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("sequence with inverse query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(normalize_rows(&jsonld), normalize_rows(&json!([["Alice"]])));
}

#[tokio::test]
async fn sparql_property_path_sequence_wildcard_hides_internal_vars() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = sparql_seed_chain_data(&fluree, "sparql/path-seq-wc:main").await;

    // SELECT * with a sequence path — internal ?__pp vars should not appear
    let query = "\
        PREFIX ex: <http://example.org/>
        SELECT * WHERE { ex:alice ex:friend/ex:name ?name }";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("wildcard sequence query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Verify results contain ?name but no ?__pp* keys
    let arr = jsonld.as_array().expect("result should be array");
    assert!(!arr.is_empty(), "Should have results");
    for row in arr {
        let obj = row.as_object().expect("row should be object");
        for key in obj.keys() {
            assert!(
                !key.starts_with("?__"),
                "Wildcard output should not contain internal variables, found: {key}"
            );
        }
    }
}

#[tokio::test]
async fn sparql_property_path_sequence_transitive_step_allowed() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = sparql_seed_chain_data(&fluree, "sparql/path-seq-err:main").await;

    // ex:friend+/ex:name — transitive modifier inside sequence should work
    let query = "\
        PREFIX ex: <http://example.org/>
        SELECT ?name WHERE { ex:alice ex:friend+/ex:name ?name }";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("transitive step inside sequence should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["Bob"], ["Carol"]]))
    );
}

// =========================================================================
// SPARQL Property Path Tests: Inverse-Transitive (^p+ / ^p*)
// =========================================================================

#[tokio::test]
async fn sparql_property_path_inverse_one_or_more() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = sparql_seed_knows_chain(&fluree, "sparql/path-inv-plus:main").await;

    // ^ex:knows+ from ex:c → reverse-traverse one-or-more: who knows c? b. who knows b? a.
    let query = "\
        PREFIX ex: <http://example.org/>
        SELECT ?x WHERE { ex:c ^ex:knows+ ?x }";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("inverse one-or-more query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["ex:a"], ["ex:b"]]))
    );
}

#[tokio::test]
async fn sparql_property_path_inverse_zero_or_more() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = sparql_seed_knows_chain(&fluree, "sparql/path-inv-star:main").await;

    // ^ex:knows* from ex:b → reverse-traverse zero-or-more (includes self):
    // zero hops: b. who knows b? a. who knows a? nobody.
    let query = "\
        PREFIX ex: <http://example.org/>
        SELECT ?x WHERE { ex:b ^ex:knows* ?x }";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("inverse zero-or-more query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["ex:a"], ["ex:b"]]))
    );
}

#[tokio::test]
async fn sparql_property_path_alternative_of_sequences() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = sparql_seed_alt_seq_data(&fluree, "sparql/path-alt-seq:main").await;

    // (ex:friend/ex:name) | (ex:colleague/ex:name) from ex:alice
    // Should return "Bob" (via friend) and "Carol" (via colleague)
    let query = "\
        PREFIX ex: <http://example.org/>
        SELECT ?name WHERE { ex:alice (ex:friend/ex:name)|(ex:colleague/ex:name) ?name }";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("alternative-of-sequences query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["Bob"], ["Carol"]]))
    );
}

#[tokio::test]
async fn sparql_property_path_alternative_mixed_simple_and_sequence() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = sparql_seed_alt_seq_data(&fluree, "sparql/path-alt-mix:main").await;

    // Give alice a direct name
    let insert2 = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [{"@id":"ex:alice","ex:name":"Alice"}]
    });
    let ledger = fluree.insert(ledger, &insert2).await.unwrap().ledger;

    // ex:name | (ex:friend/ex:name) — direct name OR friend's name
    let query = "\
        PREFIX ex: <http://example.org/>
        SELECT ?val WHERE { ex:alice ex:name|(ex:friend/ex:name) ?val }";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("mixed simple+sequence alternative query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["Alice"], ["Bob"]]))
    );
}

#[tokio::test]
async fn sparql_property_path_sequence_with_alternative_step() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = sparql_seed_alt_in_seq_data(&fluree, "sparql/path-alt-in-seq:main").await;

    // ex:friend/(ex:name|ex:nick) — friend's name or nick
    let query = "\
        PREFIX ex: <http://example.org/>
        SELECT ?val WHERE { ex:alice ex:friend/(ex:name|ex:nick) ?val }";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("alternative-in-sequence query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["Bob"], ["Bobby"]]))
    );
}

#[tokio::test]
async fn sparql_property_path_sequence_with_middle_alternative() {
    // Three-step chain with middle alternative: ex:friend/(ex:name|ex:nick)
    // Uses the same data but with a different ledger alias to test isolation
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "sparql/path-mid-alt:main");
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {
                "@id": "ex:alice",
                "ex:knows": {"@id": "ex:bob"}
            },
            {
                "@id": "ex:bob",
                "ex:friend": {"@id": "ex:carol"}
            },
            {
                "@id": "ex:carol",
                "ex:name": "Carol",
                "ex:nick": "Caz"
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    // ex:knows/ex:friend/(ex:name|ex:nick) — three steps, alternative in last position
    let query = "\
        PREFIX ex: <http://example.org/>
        SELECT ?val WHERE { ex:alice ex:knows/ex:friend/(ex:name|ex:nick) ?val }";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("three-step alternative-in-sequence query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["Carol"], ["Caz"]]))
    );
}

#[tokio::test]
async fn sparql_property_path_inverse_of_sequence() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = sparql_seed_chain_data(&fluree, "sparql/path-inv-seq:main").await;

    // ^(ex:friend/ex:friend): reverse sequence and invert each step
    // Rewrites to (^ex:friend)/(^ex:friend)
    // From ex:carol: ^friend → bob (bob has friend→carol), ^friend → alice (alice has friend→bob)
    let query = "\
        PREFIX ex: <http://example.org/>
        SELECT ?who WHERE { ex:carol ^(ex:friend/ex:friend) ?who }";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("inverse-of-sequence query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["ex:alice"]]))
    );
}

#[tokio::test]
async fn sparql_property_path_inverse_of_alternative() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = sparql_seed_chain_data(&fluree, "sparql/path-inv-alt:main").await;

    // ^(ex:friend|ex:parent): distribute inverse into each branch
    // Rewrites to (^ex:friend)|(^ex:parent)
    // From ex:bob: ^friend → alice, ^parent → alice (both branches find alice)
    let query = "\
        PREFIX ex: <http://example.org/>
        SELECT DISTINCT ?who WHERE { ex:bob ^(ex:friend|ex:parent) ?who }";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("inverse-of-alternative query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["ex:alice"]]))
    );
}

/// STR() on a custom-namespace predicate variable must return the full IRI,
/// not the internal `{code}:{name}` form.
#[tokio::test]
async fn sparql_str_expands_custom_namespace_predicate() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_custom_ns(&fluree, "custom_ns:main").await;

    // Query all predicates for ex:item1 and apply STR() to the predicate variable
    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT (STR(?p) AS ?predicate)
        WHERE {
            ex:item1 ?p ?o .
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("STR() query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    let rows_json = normalize_rows(&jsonld);
    let rows: Vec<String> = rows_json
        .iter()
        .map(|v| serde_json::to_string(v).unwrap())
        .collect();

    // STR(?p) must produce full IRIs, never internal code:name format like "21:packageType"
    for row in &rows {
        assert!(
            !row.contains("\"21:") && !row.contains("\"20:"),
            "STR() returned internal namespace code form: {row}"
        );
    }
    // Verify the custom namespace predicates are present as full IRIs
    assert!(
        rows.iter()
            .any(|r| r.contains("https://taxo.cbcrc.ca/ns/packageType")),
        "Expected cust:packageType as full IRI in STR() output, got: {rows:?}"
    );
    assert!(
        rows.iter()
            .any(|r| r.contains("https://taxo.cbcrc.ca/ns/category")),
        "Expected cust:category as full IRI in STR() output, got: {rows:?}"
    );
}

/// SPARQL queries using full IRI predicates (angle-bracket syntax) must match
/// data stored under custom namespace codes.
#[tokio::test]
async fn sparql_full_iri_predicate_matches_custom_namespace() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_custom_ns(&fluree, "custom_ns_match:main").await;

    // Query using the full IRI (not PREFIX shorthand)
    let query = r"
        SELECT ?type
        WHERE {
            ?s <https://taxo.cbcrc.ca/ns/packageType> ?type .
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("full-IRI predicate query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    let rows_json = normalize_rows(&jsonld);
    let rows: Vec<String> = rows_json
        .iter()
        .map(|v| serde_json::to_string(v).unwrap())
        .collect();

    assert_eq!(
        rows.len(),
        2,
        "Expected 2 rows for packageType, got: {rows:?}"
    );
    assert!(
        rows.iter().any(|r| r.contains("premium")),
        "Expected 'premium', got: {rows:?}"
    );
    assert!(
        rows.iter().any(|r| r.contains("standard")),
        "Expected 'standard', got: {rows:?}"
    );
}

/// SPARQL queries using PREFIX shorthand for custom namespaces must also work.
#[tokio::test]
async fn sparql_prefix_shorthand_matches_custom_namespace() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_custom_ns(&fluree, "custom_ns_prefix:main").await;

    let query = r"
        PREFIX cust: <https://taxo.cbcrc.ca/ns/>
        SELECT ?type
        WHERE {
            ?s cust:packageType ?type .
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("PREFIX shorthand query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    let rows_json = normalize_rows(&jsonld);
    let rows: Vec<String> = rows_json
        .iter()
        .map(|v| serde_json::to_string(v).unwrap())
        .collect();

    assert_eq!(
        rows.len(),
        2,
        "Expected 2 rows for packageType, got: {rows:?}"
    );
    assert!(
        rows.iter().any(|r| r.contains("premium")),
        "Expected 'premium', got: {rows:?}"
    );
    assert!(
        rows.iter().any(|r| r.contains("standard")),
        "Expected 'standard', got: {rows:?}"
    );
}

/// Bug 1 repro: custom namespace predicate without rdf:type returns 0 rows.
#[tokio::test]
async fn sparql_exact_repro_custom_pred_without_type() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_exact_repro(&fluree, "repro/bug1:main").await;

    // WITH rdf:type → should return 1 row (baseline)
    let with_type = r"
        PREFIX cust: <https://taxo.cbcrc.ca/ns/>
        SELECT ?s ?o
        WHERE { ?s a cust:CoveragePackage ; cust:anchor ?o . }
    ";
    let result = support::query_sparql(&fluree, &ledger, with_type)
        .await
        .expect("query with type");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    let rows = jsonld.as_array().expect("array");
    assert_eq!(
        rows.len(),
        1,
        "WITH rdf:type should return 1 row; got {}: {:?}",
        rows.len(),
        jsonld
    );

    // WITHOUT rdf:type → should ALSO return 1 row (BUG: returned 0)
    let without_type = r"
        PREFIX cust: <https://taxo.cbcrc.ca/ns/>
        SELECT ?s ?o
        WHERE { ?s cust:anchor ?o . }
    ";
    let result = support::query_sparql(&fluree, &ledger, without_type)
        .await
        .expect("query without type");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    let rows = jsonld.as_array().expect("array");
    assert_eq!(
        rows.len(),
        1,
        "WITHOUT rdf:type should also return 1 row; got {}: {:?}",
        rows.len(),
        jsonld
    );
}

/// Bug 2 repro: JSON-LD expansion returns empty for custom namespace type.
#[tokio::test]
async fn jsonld_exact_repro_expansion_custom_type() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_exact_repro(&fluree, "repro/bug2:main").await;

    // Graph crawl for the cust:CoveragePackage entity
    let query = json!({
        "@context": {
            "cust": "https://taxo.cbcrc.ca/ns/",
            "cbc": "https://taxo.cbcrc.ca/id/"
        },
        "select": {"?s": ["*"]},
        "values": ["?s", [{"@id": "cbc:assoc/coverage-001"}]]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("expansion should not error");
    let jsonld = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");
    let rows = jsonld.as_array().expect("array");
    assert_eq!(rows.len(), 1, "should find 1 entity");
    let obj = rows[0].as_object().expect("should be object");
    assert!(
        obj.len() > 1,
        "expansion should return properties, not just @id; got: {obj:?}"
    );
}

// ============================================================================
// Bug 3b regression: BIND IRI + OPTIONAL returns empty bindings
// ============================================================================

/// BIND(<iri> AS ?x) followed by OPTIONAL { ?x pred ?val } should propagate
/// the bound IRI into the OPTIONAL pattern. Previously this returned nulls
/// while using the IRI directly (without BIND) worked.
#[tokio::test]
async fn sparql_bind_iri_with_optional_propagates_binding() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "bind_opt");

    // Insert a simple entity with a known IRI and property.
    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/"
        },
        "@graph": [{
            "@id": "ex:thing1",
            "ex:label": "Hello"
        }]
    });
    let receipt = fluree.insert(ledger, &insert).await.expect("insert");
    let ledger = receipt.ledger;

    // Control: direct IRI in OPTIONAL works
    let control = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?label WHERE {
            OPTIONAL { ex:thing1 ex:label ?label }
        }
    ";
    let result = support::query_sparql(&fluree, &ledger, control)
        .await
        .expect("control query");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        jsonld,
        json!([["Hello"]]),
        "control: direct IRI in OPTIONAL should find label"
    );

    // Bug 3b: BIND + OPTIONAL returns empty binding
    let bind_query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?label WHERE {
            BIND(ex:thing1 AS ?s)
            OPTIONAL { ?s ex:label ?label }
        }
    ";
    let result = support::query_sparql(&fluree, &ledger, bind_query)
        .await
        .expect("bind query");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        jsonld,
        json!([["Hello"]]),
        "BIND+OPTIONAL should propagate IRI into OPTIONAL"
    );
}

/// W3C negation test: MINUS with FILTER in subtree (subset-by-exclusion-minus-1)
///
/// Reproduces the W3C test where MINUS subtree patterns are executed with
/// empty seed (fresh scope) and must produce results from a full scan.
#[tokio::test]
async fn sparql_minus_with_filter_in_subtree() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "negation:minus");

    // Seed data equivalent to subsetByExcl.ttl
    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/"
        },
        "@graph": [
            {"@id": "ex:lifeForm1", "@type": ["ex:Mammal", "ex:Animal"]},
            {"@id": "ex:lifeForm2", "@type": ["ex:Reptile", "ex:Animal"]},
            {"@id": "ex:lifeForm3", "@type": ["ex:Insect", "ex:Animal"]}
        ]
    });

    let ledger = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert lifeforms");

    // Query: keep animals that are NOT Reptile or Insect (via MINUS)
    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?animal WHERE {
            ?animal a ex:Animal
            MINUS {
                ?animal a ?type
                FILTER(?type = ex:Reptile || ?type = ex:Insect)
            }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger.ledger, query)
        .await
        .expect("MINUS query should succeed");
    let jsonld = result
        .to_jsonld(&ledger.ledger.snapshot)
        .expect("to_jsonld");

    // Only lifeForm1 (Mammal) should remain
    let rows = normalize_rows(&jsonld);
    assert_eq!(rows.len(), 1, "Expected 1 result, got: {rows:?}");
    let row_str = serde_json::to_string(&rows[0]).unwrap();
    assert!(
        row_str.contains("lifeForm1"),
        "Expected lifeForm1 (Mammal), got: {rows:?}",
    );
}

/// Simpler MINUS test: basic anti-join without FILTER in subtree
#[tokio::test]
async fn sparql_minus_basic_anti_join() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "negation:basic");

    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/"
        },
        "@graph": [
            {"@id": "ex:lifeForm1", "@type": ["ex:Mammal", "ex:Animal"]},
            {"@id": "ex:lifeForm2", "@type": ["ex:Reptile", "ex:Animal"]},
            {"@id": "ex:lifeForm3", "@type": ["ex:Insect", "ex:Animal"]}
        ]
    });

    let ledger = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert lifeforms");

    // Simple MINUS: remove animals that are insects
    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?animal WHERE {
            ?animal a ex:Animal
            MINUS { ?animal a ex:Insect }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger.ledger, query)
        .await
        .expect("basic MINUS query should succeed");
    let jsonld = result
        .to_jsonld(&ledger.ledger.snapshot)
        .expect("to_jsonld");

    // lifeForm1 and lifeForm2 should remain (lifeForm3 is Insect, removed)
    let rows = normalize_rows(&jsonld);
    assert_eq!(rows.len(), 2, "Expected 2 results, got: {rows:?}");
}

/// Test compound FILTER NOT EXISTS inside MINUS subtree (subset-02 pattern).
///
/// Verifies that NOT EXISTS inside an OR expression works correctly when
/// used within a MINUS block. The MINUS should remove set pairs where s1
/// has a member that s2 doesn't.
#[tokio::test]
async fn sparql_minus_compound_not_exists() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "negation:compound-nex");

    // Minimal set data: two sets with overlapping members
    // set_a = {1, 2}, set_b = {1}
    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/"
        },
        "@graph": [
            {"@id": "ex:set_a", "@type": "ex:Set", "ex:member": [1, 2]},
            {"@id": "ex:set_b", "@type": "ex:Set", "ex:member": [1]}
        ]
    });

    let ledger = fluree.insert(ledger0, &insert).await.expect("insert sets");

    // MINUS with compound FILTER: remove pairs where s1 has a member not in s2
    // (OR with NOT EXISTS), also removing self-pairs via s1=s2 clause.
    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?s1 ?s2 WHERE {
            ?s1 a ex:Set .
            ?s2 a ex:Set .
            MINUS {
                ?s1 a ex:Set .
                ?s2 a ex:Set .
                ?s1 ex:member ?x .
                FILTER ( ?s1 = ?s2 || NOT EXISTS { ?s2 ex:member ?x } )
            }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger.ledger, query)
        .await
        .expect("compound NOT EXISTS MINUS query should succeed");
    let sparql_json = result
        .to_sparql_json(&ledger.ledger.snapshot)
        .expect("to_sparql_json");

    // Expected: only (set_b, set_a) should remain.
    // - (set_a, set_a): removed by MINUS (s1=s2 with member)
    // - (set_a, set_b): removed (a has member 2 which b doesn't → NOT EXISTS true)
    // - (set_b, set_a): set_b only has {1}, a has {1,2} ⊃ {1} → all b's members in a
    //   FILTER: b≠a → check NOT EXISTS {a member 1} → a has 1 → false → OR = false
    //   No MINUS row passes → pair NOT removed → KEPT
    // - (set_b, set_b): removed by MINUS (s1=s2 with member)
    let bindings = sparql_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");

    assert_eq!(
        bindings.len(),
        1,
        "Expected 1 result (set_b subset of set_a), got: {bindings:?}"
    );
}

/// Test that compound FILTER NOT EXISTS evaluates correctly (no MINUS wrapper).
///
/// Isolates whether Expression::Exists evaluation works in compound expressions.
#[tokio::test]
async fn sparql_compound_filter_not_exists_standalone() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "negation:compound-nex-standalone");

    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/"
        },
        "@graph": [
            {"@id": "ex:set_a", "@type": "ex:Set", "ex:member": [1, 2]},
            {"@id": "ex:set_b", "@type": "ex:Set", "ex:member": [1]}
        ]
    });

    let ledger = fluree.insert(ledger0, &insert).await.expect("insert sets");

    // This is the MINUS subtree from subset-02, run as a standalone query.
    // Should return rows where s1=s2 OR s2 doesn't have member x.
    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?s1 ?s2 ?x WHERE {
            ?s1 a ex:Set .
            ?s2 a ex:Set .
            ?s1 ex:member ?x .
            FILTER ( ?s1 = ?s2 || NOT EXISTS { ?s2 ex:member ?x } )
        }
    ";

    let result = support::query_sparql(&fluree, &ledger.ledger, query)
        .await
        .expect("compound filter query should succeed");
    let sparql_json = result
        .to_sparql_json(&ledger.ledger.snapshot)
        .expect("to_sparql_json");

    let bindings = sparql_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");

    // Expected rows (s1, s2, x):
    // (a, a, 1) — s1=s2 → true
    // (a, a, 2) — s1=s2 → true
    // (a, b, 2) — a≠b, NOT EXISTS {b :member 2} → true → true
    // (b, a, 1) — b≠a, NOT EXISTS {a :member 1} → false → false (skip)
    // (b, b, 1) — s1=s2 → true
    // Total: 4 rows
    assert_eq!(
        bindings.len(),
        4,
        "Expected 4 results from compound NOT EXISTS filter, got: {bindings:?}"
    );
}

/// Compound FILTER NOT EXISTS — exercises the async EXISTS pre-evaluation path.
///
/// Verifies that `FILTER(false || NOT EXISTS { ... })` produces the same
/// result as standalone `FILTER NOT EXISTS { ... }`.
#[tokio::test]
async fn sparql_compound_filter_not_exists_equals_standalone() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "negation:compound-vs-standalone");

    let insert = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": [
            {"@id": "ex:set_a", "@type": "ex:Set", "ex:member": [1, 2]},
            {"@id": "ex:set_b", "@type": "ex:Set", "ex:member": [1]}
        ]
    });

    let ledger = fluree.insert(ledger0, &insert).await.expect("insert sets");

    // Standalone NOT EXISTS: s2 does NOT have member x
    let standalone = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?s1 ?s2 ?x WHERE {
            ?s1 a ex:Set . ?s2 a ex:Set . ?s1 ex:member ?x .
            FILTER NOT EXISTS { ?s2 ex:member ?x }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger.ledger, standalone)
        .await
        .expect("standalone NOT EXISTS");
    let json = result.to_sparql_json(&ledger.ledger.snapshot).unwrap();
    let standalone_count = json["results"]["bindings"].as_array().unwrap().len();
    // Only (set_a, set_b, 2) — set_b doesn't have member 2
    assert_eq!(standalone_count, 1, "standalone NOT EXISTS");

    // Compound: false || NOT EXISTS should produce the same result
    let compound = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?s1 ?s2 ?x WHERE {
            ?s1 a ex:Set . ?s2 a ex:Set . ?s1 ex:member ?x .
            FILTER ( false || NOT EXISTS { ?s2 ex:member ?x } )
        }
    ";

    let result = support::query_sparql(&fluree, &ledger.ledger, compound)
        .await
        .expect("compound NOT EXISTS");
    let json = result.to_sparql_json(&ledger.ledger.snapshot).unwrap();
    let compound_count = json["results"]["bindings"].as_array().unwrap().len();
    assert_eq!(
        compound_count, standalone_count,
        "compound (false || NOT EXISTS) should equal standalone NOT EXISTS"
    );
}

/// Test that SELECT * with empty results still produces a variable header.
///
/// W3C requires that `head.vars` includes the query's projected variables
/// even when the result set is empty. This tests the VarRegistry fallback
/// in the SPARQL JSON formatter.
#[tokio::test]
async fn sparql_wildcard_header_with_empty_results() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "negation:empty-wildcard");

    let insert = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": [
            {"@id": "ex:a", "@type": "ex:Thing"}
        ]
    });

    let ledger = fluree.insert(ledger0, &insert).await.expect("insert data");

    // SELECT * with a condition that matches nothing
    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT * WHERE {
            ?x ex:nonExistentProperty ?y
        }
    ";

    let result = support::query_sparql(&fluree, &ledger.ledger, query)
        .await
        .expect("empty wildcard query should succeed");
    let sparql_json = result
        .to_sparql_json(&ledger.ledger.snapshot)
        .expect("to_sparql_json");

    // Should have variables in head even with 0 results
    let head_vars = sparql_json["head"]["vars"]
        .as_array()
        .expect("head.vars array");
    assert!(
        !head_vars.is_empty(),
        "SELECT * with empty results should still have variables in head.vars, got: {sparql_json}"
    );

    // Should have 0 bindings
    let bindings = sparql_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    assert_eq!(bindings.len(), 0, "Should have 0 results");
}

#[tokio::test]
async fn sparql_strlen_multibyte_characters() {
    // STRLEN must count Unicode characters, not bytes.
    // "食べ物" is 3 characters but 9 bytes in UTF-8.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_builtin_fn_data(&fluree, "fn:strlen-mb").await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?label (STRLEN(?label) AS ?len)
        WHERE { ex:sushi ex:label ?label }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("STRLEN query");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // "食べ物" → 3 characters
    assert_eq!(jsonld, json!([["食べ物", 3]]));
}

#[tokio::test]
async fn sparql_substr_multibyte_characters() {
    // SUBSTR must use character-based indexing (1-based per W3C fn:substring).
    // SUBSTR("食べ物", 2, 2) should return "べ物" (chars 2 and 3).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_builtin_fn_data(&fluree, "fn:substr-mb").await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT (SUBSTR(?label, 2, 2) AS ?sub)
        WHERE { ex:sushi ex:label ?label }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("SUBSTR query");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(jsonld, json!([["べ物"]]));
}

#[tokio::test]
async fn sparql_substr_multibyte_no_length() {
    // SUBSTR("食べ物", 2) without length → rest of the string from position 2.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_builtin_fn_data(&fluree, "fn:substr-mb-nolen").await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT (SUBSTR(?label, 2) AS ?sub)
        WHERE { ex:sushi ex:label ?label }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("SUBSTR no-length query");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(jsonld, json!([["べ物"]]));
}

#[tokio::test]
async fn sparql_timezone_returns_day_time_duration() {
    // TIMEZONE() must return xsd:dayTimeDuration, not a plain string.
    // For UTC ("Z"), should return "PT0S".
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_builtin_fn_data(&fluree, "fn:timezone").await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT (TIMEZONE(?dt) AS ?tz)
        WHERE { ex:sushi ex:created ?dt }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("TIMEZONE query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");

    let bindings = normalize_sparql_bindings(&sparql_json);
    assert_eq!(bindings.len(), 1);
    let tz = &bindings[0]["tz"];
    assert_eq!(
        tz["value"].as_str().unwrap(),
        "PT0S",
        "UTC timezone should be PT0S"
    );
    assert_eq!(
        tz["datatype"].as_str().unwrap(),
        "http://www.w3.org/2001/XMLSchema#dayTimeDuration",
        "TIMEZONE must return xsd:dayTimeDuration"
    );
}

#[tokio::test]
async fn sparql_timezone_positive_offset() {
    // TIMEZONE for +05:30 → "PT5H30M"
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_builtin_fn_data(&fluree, "fn:timezone-pos").await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT (TIMEZONE(?dt) AS ?tz)
        WHERE { ex:beer ex:created ?dt }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("TIMEZONE +offset query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");

    let bindings = normalize_sparql_bindings(&sparql_json);
    assert_eq!(bindings.len(), 1);
    let tz = &bindings[0]["tz"];
    assert_eq!(tz["value"].as_str().unwrap(), "PT5H30M");
}

#[tokio::test]
async fn sparql_tz_returns_string() {
    // TZ() returns a plain string ("Z", "+05:30", etc.), not a typed literal.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_builtin_fn_data(&fluree, "fn:tz-string").await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT (TZ(?dt) AS ?tz)
        WHERE { ex:sushi ex:created ?dt }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("TZ query");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(jsonld, json!([["Z"]]));
}

#[tokio::test]
async fn sparql_uuid_returns_iri() {
    // UUID() must return an IRI of the form urn:uuid:..., not a plain string.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_builtin_fn_data(&fluree, "fn:uuid-iri").await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT (UUID() AS ?id)
        WHERE { ex:sushi ex:label ?label }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("UUID query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");

    let bindings = normalize_sparql_bindings(&sparql_json);
    assert_eq!(bindings.len(), 1);
    let id = &bindings[0]["id"];
    assert_eq!(
        id["type"].as_str().unwrap(),
        "uri",
        "UUID() must return an IRI (type=uri), not a literal"
    );
    assert!(
        id["value"].as_str().unwrap().starts_with("urn:uuid:"),
        "UUID() value must start with urn:uuid:"
    );
}

#[tokio::test]
async fn sparql_struuid_returns_string() {
    // STRUUID() returns a plain string (no urn:uuid: prefix), type=literal.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_builtin_fn_data(&fluree, "fn:struuid-str").await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT (STRUUID() AS ?id)
        WHERE { ex:sushi ex:label ?label }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("STRUUID query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");

    let bindings = normalize_sparql_bindings(&sparql_json);
    assert_eq!(bindings.len(), 1);
    let id = &bindings[0]["id"];
    assert_eq!(
        id["type"].as_str().unwrap(),
        "literal",
        "STRUUID() must return a literal, not a URI"
    );
    assert!(
        !id["value"].as_str().unwrap().starts_with("urn:uuid:"),
        "STRUUID() value must NOT have urn:uuid: prefix"
    );
    // Should be a valid UUID format (8-4-4-4-12 hex)
    assert_eq!(
        id["value"].as_str().unwrap().len(),
        36,
        "STRUUID() should be 36 chars (UUID format)"
    );
}

#[tokio::test]
async fn sparql_isnumeric_decimal() {
    // isNumeric must recognize xsd:decimal values as numeric.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_builtin_fn_data(&fluree, "fn:isnumeric-dec").await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?label (isNumeric(?price) AS ?numP) (isNumeric(?label) AS ?numL)
        WHERE {
            ex:sushi ex:price ?price .
            ex:sushi ex:label ?label .
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("isNumeric query");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // price is decimal → true, label is string → false
    assert_eq!(jsonld, json!([["食べ物", true, false]]));
}

#[tokio::test]
async fn sparql_sum_avg_over_xsd_decimal_repro() {
    // Repro for reported bug: SUM(?x) over xsd:decimal returns 0,
    // AVG(?x) returns unbound. SUM(xsd:integer) and MIN/MAX over the same
    // decimals work, so the bug is specific to arithmetic aggregates +
    // xsd:decimal.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_builtin_fn_data(&fluree, "agg:decimal-sum").await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT
          (SUM(?price) AS ?total)
          (AVG(?price) AS ?avg)
          (MIN(?price) AS ?lo)
          (MAX(?price) AS ?hi)
          (COUNT(?price) AS ?n)
        WHERE { ?x ex:price ?price }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("aggregate over decimal");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    let bindings = normalize_sparql_bindings(&sparql_json);
    assert_eq!(bindings.len(), 1);
    let row = &bindings[0];

    // 12.50 + 7.99 = 20.49 — exact xsd:decimal arithmetic, not lossy f64.
    let total = row.get("total").expect("total bound");
    assert_eq!(total["value"].as_str().unwrap(), "20.49");
    assert_eq!(
        total["datatype"].as_str().unwrap(),
        "http://www.w3.org/2001/XMLSchema#decimal",
        "SUM(xsd:decimal) must yield xsd:decimal per W3C arithmetic promotion"
    );

    let avg = row.get("avg").expect("avg bound (not Unbound)");
    assert_eq!(avg["value"].as_str().unwrap(), "10.245");
    assert_eq!(
        avg["datatype"].as_str().unwrap(),
        "http://www.w3.org/2001/XMLSchema#decimal"
    );
}

async fn seed_receipt_line_items(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    let insert = json!({
        "@context": {
            "sup": "http://Magna/SupplyChain#",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {
                "@id": "sup:r1",
                "@type": "sup:ReceiptLineItem",
                "sup:forEngcPart": {"@id": "sup:partA"},
                "sup:receiptUnitPrice": 10
            },
            {
                "@id": "sup:r2",
                "@type": "sup:ReceiptLineItem",
                "sup:forEngcPart": {"@id": "sup:partA"},
                "sup:receiptUnitPrice": 14
            },
            {
                "@id": "sup:r3",
                "@type": "sup:ReceiptLineItem",
                "sup:forEngcPart": {"@id": "sup:partA"},
                "sup:receiptUnitPrice": 12
            },
            {
                "@id": "sup:r4",
                "@type": "sup:ReceiptLineItem",
                "sup:forEngcPart": {"@id": "sup:partB"},
                "sup:receiptUnitPrice": 5
            },
            {
                "@id": "sup:r5",
                "@type": "sup:ReceiptLineItem",
                "sup:forEngcPart": {"@id": "sup:partB"},
                "sup:receiptUnitPrice": 9
            }
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("seed receipt line items")
        .ledger
}

#[tokio::test]
async fn sparql_arithmetic_over_min_max_in_select_repro() {
    // Repro for reported bug: SELECT with arithmetic over MAX(?u) - MIN(?u)
    // grouped by ?part fails or returns wrong rows.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_receipt_line_items(&fluree, "agg:spread").await;

    let query = r"
        PREFIX sup: <http://Magna/SupplyChain#>
        SELECT ?part ((MAX(?u) - MIN(?u)) AS ?spread)
        WHERE { ?r a sup:ReceiptLineItem ; sup:forEngcPart ?part ; sup:receiptUnitPrice ?u }
        GROUP BY ?part
        LIMIT 5
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("MAX - MIN over grouped values");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    let bindings = normalize_sparql_bindings(&sparql_json);
    assert_eq!(bindings.len(), 2, "expected one row per part");

    // partA: values 10, 14, 12 → spread 4
    // partB: values 5, 9       → spread 4
    let mut spreads: Vec<&str> = bindings
        .iter()
        .map(|b| b["spread"]["value"].as_str().expect("spread bound"))
        .collect();
    spreads.sort();
    assert_eq!(spreads, vec!["4", "4"]);
}

#[tokio::test]
async fn sparql_bare_min_max_in_select_works() {
    // Control for the arithmetic-over-aggregates repro: bare MAX/MIN columns
    // should succeed.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_receipt_line_items(&fluree, "agg:bare-minmax").await;

    let query = r"
        PREFIX sup: <http://Magna/SupplyChain#>
        SELECT ?part (MAX(?u) AS ?hi) (MIN(?u) AS ?lo)
        WHERE { ?r a sup:ReceiptLineItem ; sup:forEngcPart ?part ; sup:receiptUnitPrice ?u }
        GROUP BY ?part
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("bare MAX/MIN per group");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    let bindings = normalize_sparql_bindings(&sparql_json);
    assert_eq!(bindings.len(), 2);
}

#[tokio::test]
async fn sparql_ucase_preserves_language_tag() {
    // W3C: UCASE must preserve language tags from the input.
    // SPARQL JSON output should include xml:lang on the result.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_builtin_fn_data(&fluree, "fn:ucase-lang").await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT (UCASE(?note) AS ?upper)
        WHERE { ex:sushi ex:note ?note }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("UCASE lang query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");

    let bindings = normalize_sparql_bindings(&sparql_json);
    assert_eq!(bindings.len(), 1);
    let upper = &bindings[0]["upper"];
    assert_eq!(upper["value"].as_str().unwrap(), "HOLA MUNDO");
    assert_eq!(
        upper["xml:lang"].as_str().unwrap(),
        "es",
        "UCASE must preserve the language tag"
    );
}

#[tokio::test]
async fn sparql_lcase_preserves_language_tag() {
    // W3C: LCASE must preserve language tags from the input.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_builtin_fn_data(&fluree, "fn:lcase-lang").await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT (LCASE(?note) AS ?lower)
        WHERE { ex:beer ex:note ?note }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("LCASE lang query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");

    let bindings = normalize_sparql_bindings(&sparql_json);
    assert_eq!(bindings.len(), 1);
    let lower = &bindings[0]["lower"];
    assert_eq!(lower["value"].as_str().unwrap(), "good stuff");
    assert_eq!(
        lower["xml:lang"].as_str().unwrap(),
        "en",
        "LCASE must preserve the language tag"
    );
}

// ---------------------------------------------------------------------------
// XSD cast functions (W3C SPARQL 1.1 §17.5)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sparql_xsd_cast_integer_from_bool() {
    // xsd:integer(true) should return 1
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_builtin_fn_data(&fluree, "cast:int-bool").await;

    let query = r"
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX ex: <http://example.org/ns/>
        SELECT (xsd:integer(true) AS ?one) (xsd:integer(false) AS ?zero)
        WHERE { ex:sushi ex:label ?label }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("xsd:integer cast query");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(jsonld, json!([[1, 0]]));
}

#[tokio::test]
async fn sparql_xsd_cast_boolean_from_string() {
    // xsd:boolean("true") should return true
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_builtin_fn_data(&fluree, "cast:bool-str").await;

    let query = r#"
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX ex: <http://example.org/ns/>
        SELECT (xsd:boolean("true") AS ?t) (xsd:boolean("0") AS ?f)
        WHERE { ex:sushi ex:label ?label }
    "#;

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("xsd:boolean cast query");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(jsonld, json!([[true, false]]));
}

#[tokio::test]
async fn sparql_xsd_cast_double_from_integer() {
    // xsd:double(42) should return 42.0
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_builtin_fn_data(&fluree, "cast:dbl-int").await;

    let query = r"
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX ex: <http://example.org/ns/>
        SELECT (xsd:double(42) AS ?d)
        WHERE { ex:sushi ex:label ?label }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("xsd:double cast query");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(jsonld, json!([[42.0]]));
}

#[tokio::test]
async fn sparql_integer_division_yields_decimal() {
    // Per XPath op:numeric-divide, xsd:integer / xsd:integer yields xsd:decimal:
    // 10 / 4 = 2.5, NOT a truncated integer 2.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_builtin_fn_data(&fluree, "arith:int-div").await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT (10 / 4 AS ?r)
        WHERE { ex:sushi ex:label ?label }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("integer division query");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // xsd:decimal renders as a string to preserve exactness (see the
    // xsd:double note elsewhere in this file); the value is 2.5, not 2.
    assert_eq!(jsonld, json!([["2.5"]]));
}

#[tokio::test]
async fn sparql_float_divided_by_integer_promotes() {
    // Mixed numeric arithmetic: xsd:float(...) / xsd:integer must promote (not
    // error with a type mismatch). 10.0 / 4 = 2.5.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_builtin_fn_data(&fluree, "arith:float-div-int").await;

    let query = r"
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX ex: <http://example.org/ns/>
        SELECT (xsd:float(10) / 4 AS ?r)
        WHERE { ex:sushi ex:label ?label }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("float / integer query");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(jsonld, json!([[2.5]]));
}

#[tokio::test]
async fn sparql_xsd_cast_string_from_integer() {
    // xsd:string(42) should return "42"
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_builtin_fn_data(&fluree, "cast:str-int").await;

    let query = r"
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX ex: <http://example.org/ns/>
        SELECT (xsd:string(42) AS ?s)
        WHERE { ex:sushi ex:label ?label }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("xsd:string cast query");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(jsonld, json!([["42"]]));
}

#[tokio::test]
async fn sparql_xsd_cast_invalid_returns_unbound() {
    // xsd:integer("not_a_number") should produce unbound (no binding), not an error.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_builtin_fn_data(&fluree, "cast:invalid").await;

    let query = r#"
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX ex: <http://example.org/ns/>
        SELECT (xsd:integer("not_a_number") AS ?i)
        WHERE { ex:sushi ex:label ?label }
    "#;

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("invalid cast should not error");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // Unbound projected variables serialize as null in JSON-LD
    assert_eq!(jsonld, json!([[null]]));
}

// ============================================================================
// W3C BIND compliance tests (#51)
// ============================================================================

/// W3C bind01 exact replica: unbound ?p, SELECT ?z WHERE { ?s ?p ?o . BIND(?o+10 AS ?z) }
#[tokio::test]
async fn sparql_bind01_exact_w3c_unbound_predicate() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "bind:w3c01");

    // Load turtle data matching W3C bind/data.ttl
    let turtle = r"
        @prefix : <http://example.org/> .
        :s1 :p 1 .
        :s2 :p 2 .
        :s3 :p 3 .
        :s4 :p 4 .
    ";
    let receipt = fluree
        .insert_turtle(ledger0, turtle)
        .await
        .expect("insert turtle");
    let ledger = receipt.ledger;

    // Exact W3C bind01 query
    let query = r"
        PREFIX : <http://example.org/>
        SELECT ?z
        {
          ?s ?p ?o .
          BIND(?o+10 AS ?z)
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("bind01 exact W3C query should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    let mut values: Vec<i64> = jsonld
        .as_array()
        .expect("array")
        .iter()
        .flat_map(|row| row.as_array().expect("row array").iter())
        .filter_map(serde_json::Value::as_i64)
        .collect();
    values.sort();
    assert_eq!(
        values,
        vec![11, 12, 13, 14],
        "W3C bind01: BIND(?o+10 AS ?z)"
    );
}

/// W3C bind01: BIND with arithmetic expression where output is in SELECT.
/// SELECT ?z WHERE { ?s ?p ?o . BIND(?o+10 AS ?z) }
#[tokio::test]
async fn sparql_bind_expression_in_select() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "bind:expr");

    let insert = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            { "@id": "ex:s1", "ex:p": 1 },
            { "@id": "ex:s2", "ex:p": 2 }
        ]
    });
    let receipt = fluree.insert(ledger0, &insert).await.expect("insert");
    let ledger = receipt.ledger;

    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?z WHERE {
            ?s ex:p ?o .
            BIND(?o + 10 AS ?z)
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("bind01 pattern should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    let mut values: Vec<i64> = jsonld
        .as_array()
        .expect("array")
        .iter()
        .flat_map(|row| row.as_array().expect("row array").iter())
        .map(|v| v.as_i64().expect("int"))
        .collect();
    values.sort();
    assert_eq!(values, vec![11, 12], "BIND(?o+10 AS ?z) with SELECT ?z");
}

/// W3C bind06: SELECT * with BIND — BIND output variable should appear in results.
#[tokio::test]
async fn sparql_bind_wildcard_select() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "bind:wildcard");

    let insert = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            { "@id": "ex:s1", "ex:p": 1 }
        ]
    });
    let receipt = fluree.insert(ledger0, &insert).await.expect("insert");
    let ledger = receipt.ledger;

    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT * WHERE {
            ?s ex:p ?o .
            BIND(?o + 10 AS ?z)
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("bind06 wildcard should succeed");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    let bindings = normalize_sparql_bindings(&sparql_json);

    assert_eq!(bindings.len(), 1);
    // ?z should be present in the result
    assert!(
        bindings[0].get("z").is_some(),
        "BIND output ?z should appear in SELECT * results: {bindings:?}"
    );
}

// ============================================================================
// W3C VALUES compliance tests (#51)
// ============================================================================

/// Post-query VALUES constraining WHERE results (values01 pattern).
#[tokio::test]
async fn sparql_post_query_values() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "values:post");

    let insert = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            { "@id": "ex:book1", "ex:title": "SPARQL Tutorial", "ex:price": 42 },
            { "@id": "ex:book2", "ex:title": "The Semantic Web", "ex:price": 23 }
        ]
    });
    let receipt = fluree.insert(ledger0, &insert).await.expect("insert");
    let ledger = receipt.ledger;

    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?book ?title ?price
        WHERE {
            ?book ex:title ?title ;
                  ex:price ?price .
        }
        VALUES ?book { ex:book1 }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("post-query VALUES should succeed");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    let bindings = normalize_sparql_bindings(&sparql_json);

    assert_eq!(
        bindings.len(),
        1,
        "Post-query VALUES should filter to one book: {bindings:?}"
    );
}

/// W3C bind02: chained BINDs — BIND(?o+10 AS ?z) BIND(?o+100 AS ?z2).
#[tokio::test]
async fn sparql_bind_chained_binds() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "bind:chained");

    let insert = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            { "@id": "ex:s1", "ex:p": 1 },
            { "@id": "ex:s2", "ex:p": 2 }
        ]
    });
    let receipt = fluree.insert(ledger0, &insert).await.expect("insert");
    let ledger = receipt.ledger;

    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?o ?z ?z2
        WHERE {
            ?s ex:p ?o .
            BIND(?o + 10 AS ?z)
            BIND(?o + 100 AS ?z2)
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("chained BINDs should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    let rows = jsonld.as_array().expect("array");
    assert_eq!(rows.len(), 2, "two solutions: {rows:?}");
    // Each row is [?o, ?z, ?z2]
    let mut sorted: Vec<_> = rows
        .iter()
        .map(|r| {
            let arr = r.as_array().expect("row array");
            (
                arr[0].as_i64().expect("o"),
                arr[1].as_i64().expect("z"),
                arr[2].as_i64().expect("z2"),
            )
        })
        .collect();
    sorted.sort();
    assert_eq!(
        sorted,
        vec![(1, 11, 101), (2, 12, 102)],
        "chained BINDs: ?z=?o+10, ?z2=?o+100"
    );
}

/// W3C bind07: BIND inside UNION branches.
#[tokio::test]
async fn sparql_bind_in_union() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "bind:union");

    let insert = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            { "@id": "ex:s1", "ex:p": 1 },
            { "@id": "ex:s2", "ex:q": 10 }
        ]
    });
    let receipt = fluree.insert(ledger0, &insert).await.expect("insert");
    let ledger = receipt.ledger;

    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?z
        WHERE {
            { ?s ex:p ?o . BIND(?o + 10 AS ?z) }
            UNION
            { ?s ex:q ?o . BIND(?o + 100 AS ?z) }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("BIND in UNION should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    let mut values: Vec<i64> = jsonld
        .as_array()
        .expect("array")
        .iter()
        .flat_map(|row| row.as_array().expect("row array").iter())
        .filter_map(serde_json::Value::as_i64)
        .collect();
    values.sort();
    assert_eq!(values, vec![11, 110], "BIND in each UNION branch");
}

/// W3C bind05: BIND with FILTER — FILTER references BIND output.
#[tokio::test]
async fn sparql_bind_with_filter() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "bind:filter");

    let insert = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            { "@id": "ex:s1", "ex:p": 1 },
            { "@id": "ex:s2", "ex:p": 2 },
            { "@id": "ex:s3", "ex:p": 3 }
        ]
    });
    let receipt = fluree.insert(ledger0, &insert).await.expect("insert");
    let ledger = receipt.ledger;

    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?z
        WHERE {
            ?s ex:p ?o .
            BIND(?o + 10 AS ?z)
            FILTER(?z > 11)
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("BIND+FILTER should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    let mut values: Vec<i64> = jsonld
        .as_array()
        .expect("array")
        .iter()
        .flat_map(|row| row.as_array().expect("row array").iter())
        .filter_map(serde_json::Value::as_i64)
        .collect();
    values.sort();
    assert_eq!(values, vec![12, 13], "FILTER(?z > 11) on BIND(?o+10 AS ?z)");
}

/// W3C bind10: BIND scoping — outer BIND not visible inside nested { }.
#[tokio::test]
async fn sparql_bind_scoping() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "bind:scope");

    let insert = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            { "@id": "ex:s1", "ex:p": 4 }
        ]
    });
    let receipt = fluree.insert(ledger0, &insert).await.expect("insert");
    let ledger = receipt.ledger;

    // bind10: BIND(4 AS ?z) { ?s :p ?v . FILTER(?v = ?z) }
    // The inner { } creates a new scope. ?z from outer BIND should NOT
    // be visible inside, so FILTER(?v = ?z) never matches → 0 results.
    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?z ?v
        WHERE {
            BIND(4 AS ?z)
            { ?s ex:p ?v . FILTER(?v = ?z) }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("bind10 scoping should succeed");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    let bindings = normalize_sparql_bindings(&sparql_json);

    assert_eq!(
        bindings.len(),
        0,
        "bind10: outer BIND should not be visible in nested scope: {bindings:?}"
    );
}

/// Post-query VALUES with UNDEF (values04 pattern).
/// UNDEF acts as wildcard for that variable position.
#[tokio::test]
async fn sparql_post_query_values_with_undef() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "values:undef");

    let insert = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            { "@id": "ex:s1", "ex:color": "red" },
            { "@id": "ex:s2", "ex:color": "blue" },
            { "@id": "ex:s3", "ex:color": "green" }
        ]
    });
    let receipt = fluree.insert(ledger0, &insert).await.expect("insert");
    let ledger = receipt.ledger;

    // Multi-var VALUES with UNDEF — constrains ?color to "red" for any ?s
    let query = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?s ?color
        WHERE { ?s ex:color ?color }
        VALUES (?s ?color) { (UNDEF "red") }
    "#;

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("VALUES with UNDEF should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    let rows = jsonld.as_array().expect("array");

    // UNDEF for ?s means any ?s; "red" constrains ?color
    assert_eq!(
        rows.len(),
        1,
        "VALUES with UNDEF should match exactly one row (ex:s1, red): {rows:?}"
    );
}

// =========================================================================
// SPARQL DESCRIBE Tests
// =========================================================================

#[tokio::test]
async fn sparql_describe_constant_iri_outgoing_triples() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "describe:const");

    let insert = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            { "@id": "ex:alice", "ex:name": "Alice", "ex:knows": {"@id": "ex:bob"} },
            { "@id": "ex:bob", "ex:name": "Bob" }
        ]
    });
    let receipt = fluree.insert(ledger0, &insert).await.expect("insert");
    let ledger = receipt.ledger;

    let query = r"
        PREFIX ex: <http://example.org/>
        DESCRIBE ex:alice
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("DESCRIBE should succeed");

    // DESCRIBE lowers to a graph (CONSTRUCT-style) result.
    let json = result.to_construct(&ledger.snapshot).expect("to_construct");

    let graph = json
        .get("@graph")
        .and_then(|v| v.as_array())
        .expect("@graph array");

    let alice = graph
        .iter()
        .find(|n| n.get("@id") == Some(&JsonValue::String("ex:alice".to_string())))
        .expect("graph should include ex:alice node");

    let name = alice.get("ex:name").expect("ex:name present");
    let has_alice_name = match name {
        JsonValue::String(s) => s == "Alice",
        JsonValue::Array(items) => items.iter().any(|v| v == "Alice"),
        _ => false,
    };
    assert!(
        has_alice_name,
        "DESCRIBE should include outgoing properties for ex:alice: {alice}"
    );
    assert!(
        alice.get("ex:knows").is_some(),
        "DESCRIBE should include outgoing link ex:knows: {alice}"
    );
}

// ---------------------------------------------------------------------------
// Star-shaped query with OPTIONAL + FILTER
// ---------------------------------------------------------------------------

/// Exercises a star-shaped multi-predicate pattern with a trailing OPTIONAL and
/// a FILTER on a bound object. This pattern is eligible for property-join
/// (and, when indexed, fused-star) optimization.
#[tokio::test]
async fn sparql_star_query_with_optional_and_filter() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "star-opt-filter:main").await;

    // Star shape: same ?person subject, bound predicates handle + fullName,
    // OPTIONAL email, FILTER on handle containing "bob" (case-insensitive).
    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT ?person ?fullName ?handle ?email
        WHERE {
          ?person person:handle  ?handle .
          ?person person:fullName ?fullName .
          OPTIONAL { ?person person:email ?email . }
          FILTER( CONTAINS(LCASE(?handle), "bob") )
        }
    "#;

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Expected: bbob and jbob match the filter; only fbueller has email but is
    // excluded by FILTER. So email should be null for both matches.
    let expected = json!([
        ["http://example.org/ns/bbob", "Billy Bob", "bbob", null],
        ["http://example.org/ns/jbob", "Jenny Bob", "jbob", null]
    ]);

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&expected),
        "Star query with OPTIONAL + FILTER should return matching rows.\nGot: {jsonld:#}"
    );
}

// ── SERVICE integration tests ──────────────────────────────────────

#[tokio::test]
async fn sparql_service_self_reference_returns_data() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle
        WHERE {
          SERVICE <fluree:ledger:people:main> {
            ?s person:handle ?handle .
          }
        }
        ORDER BY ?handle
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("SERVICE self-reference should succeed");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        jsonld,
        json!([["bbob"], ["dankeshön"], ["jbob"], ["jdoe"]]),
        "SERVICE self-reference should return all handles.\nGot: {jsonld:#}"
    );
}

#[tokio::test]
async fn sparql_service_external_endpoint_errors() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    let query = r"
        SELECT * WHERE {
          SERVICE <http://remote.example.org/sparql> { ?s ?p ?o }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query).await;
    assert!(
        result.is_err(),
        "External SERVICE endpoint should produce an error"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("External SERVICE endpoints not supported"),
        "Error should mention unsupported external endpoints, got: {err}"
    );
}

#[tokio::test]
async fn sparql_service_silent_external_yields_empty() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    // SERVICE SILENT with an unsupported external endpoint should not error,
    // and since SERVICE is the only pattern, should yield zero results.
    let query = r"
        SELECT ?s ?p ?o
        WHERE {
          SERVICE SILENT <http://remote.example.org/sparql> {
            ?s ?p ?o .
          }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("SERVICE SILENT should not error");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    let rows = jsonld.as_array().expect("should be array");
    assert!(
        rows.is_empty(),
        "SERVICE SILENT with external endpoint should yield empty results, got: {jsonld:#}"
    );
}

// ── Remote SERVICE integration tests ───────────────────────────────

#[tokio::test]
async fn sparql_service_remote_returns_mock_data() {
    assert_index_defaults();
    let mut fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    // Set up a mock remote executor with canned response
    let mock = fluree_db_api::remote_service::MockRemoteService::new();
    mock.register_response(
        "acme",
        "customers:main",
        json!({
            "head": {"vars": ["name", "email"]},
            "results": {"bindings": [
                {
                    "name": {"type": "literal", "value": "Alice"},
                    "email": {"type": "literal", "value": "alice@example.com"}
                },
                {
                    "name": {"type": "literal", "value": "Bob"},
                    "email": {"type": "literal", "value": "bob@example.com"}
                }
            ]}
        }),
    );
    fluree.set_remote_service(Arc::new(mock));

    let query = r"
        SELECT ?name ?email
        WHERE {
          SERVICE <fluree:remote:acme/customers:main> {
            ?s <http://example.org/name> ?name .
            ?s <http://example.org/email> ?email .
          }
        }
        ORDER BY ?name
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("Remote SERVICE should succeed with mock");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        jsonld,
        json!([["Alice", "alice@example.com"], ["Bob", "bob@example.com"]]),
        "Remote SERVICE should return mock data.\nGot: {jsonld:#}"
    );
}

#[tokio::test]
async fn sparql_service_remote_unknown_connection_errors() {
    assert_index_defaults();
    let mut fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    // Set up mock but don't register "nonexistent" connection
    let mock = fluree_db_api::remote_service::MockRemoteService::new();
    fluree.set_remote_service(Arc::new(mock));

    let query = r"
        SELECT * WHERE {
          SERVICE <fluree:remote:nonexistent/db:main> {
            ?s ?p ?o .
          }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query).await;
    assert!(result.is_err(), "Unknown remote connection should error");
}

#[tokio::test]
async fn sparql_service_remote_silent_swallows_error() {
    assert_index_defaults();
    let mut fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    // Set up mock but don't register the endpoint
    let mock = fluree_db_api::remote_service::MockRemoteService::new();
    fluree.set_remote_service(Arc::new(mock));

    let query = r"
        SELECT ?s ?p ?o
        WHERE {
          SERVICE SILENT <fluree:remote:missing/db:main> {
            ?s ?p ?o .
          }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("SERVICE SILENT should not error");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    let rows = jsonld.as_array().expect("should be array");
    assert!(
        rows.is_empty(),
        "SERVICE SILENT with failed remote should yield empty results, got: {jsonld:#}"
    );
}

#[tokio::test]
async fn sparql_service_remote_no_executor_errors() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "people:main").await;

    // No remote executor set
    let query = r"
        SELECT * WHERE {
          SERVICE <fluree:remote:acme/db:main> { ?s ?p ?o }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, query).await;
    assert!(
        result.is_err(),
        "Remote SERVICE without executor should error"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("No remote service executor configured"),
        "Error should mention missing executor, got: {err}"
    );
}

// =========================================================================
// Field report repro: GROUP BY over an expression must collapse + honor LIMIT
// =========================================================================

/// Seed line items across three currencies (mixed case) so that
/// `GROUP BY (LCASE(?cur))` collapses to exactly three groups.
async fn seed_currency_line_items(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    // 5 USD, 3 CAD, 2 EUR (mixed case to exercise LCASE collapsing).
    let currencies = [("USD", 5usize), ("Cad", 3usize), ("eur", 2usize)];
    let mut graph = Vec::new();
    let mut idx = 0usize;
    for (cur, n) in currencies {
        for _ in 0..n {
            graph.push(json!({
                "@id": format!("ex:item{idx}"),
                "@type": "ex:LineItem",
                "ex:currency": cur,
            }));
            idx += 1;
        }
    }

    let insert = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": graph,
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert currency line items")
        .ledger
}

#[tokio::test]
async fn sparql_group_by_expression_collapses_and_honors_limit() {
    // Field P0 repro: `GROUP BY (LCASE(?cur))` with the same expression aliased
    // in the SELECT must collapse to one row per distinct expression value, and
    // LIMIT must bound the result. The buggy behavior returned one row per input
    // binding (10 rows) with LIMIT ignored.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_currency_line_items(&fluree, "currency:main").await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT (LCASE(?cur) AS ?k) (COUNT(?s) AS ?n)
        WHERE { ?s a ex:LineItem ; ex:currency ?cur }
        GROUP BY (LCASE(?cur))
        ORDER BY DESC(?n)
        LIMIT 15
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();

    // SPARQL-results JSON: exactly 3 binding rows (one per distinct group), not
    // one per input line item. This is the format the field observed exploding.
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    let bindings = support::normalize_sparql_bindings(&sparql_json);
    assert_eq!(
        bindings.len(),
        3,
        "GROUP BY (expr) must collapse to one row per group, got {} rows: {bindings:#?}",
        bindings.len()
    );

    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    // Expected per SPARQL 1.1: three groups — usd 5, cad 3, eur 2.
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["usd", 5], ["cad", 3], ["eur", 2]]))
    );
}

#[tokio::test]
async fn sparql_group_by_expression_via_bind_workaround() {
    // Control: the BIND-then-GROUP-BY-?k workaround the field is using. This is
    // expected to already pass; it isolates the bug to the GROUP-BY-expression
    // desugar path.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_currency_line_items(&fluree, "currency:main").await;

    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?k (COUNT(?s) AS ?n)
        WHERE { ?s a ex:LineItem ; ex:currency ?cur . BIND(LCASE(?cur) AS ?k) }
        GROUP BY ?k
        ORDER BY DESC(?n)
        LIMIT 15
    ";

    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["usd", 5], ["cad", 3], ["eur", 2]]))
    );
}

/// `ORDER BY (EXISTS { ... })` routes the EXISTS through an order-expression
/// BIND built in `apply_solution_modifiers` (the `order_binds` site). That
/// `BindOperator` resolves EXISTS per row, so this exercises the projected/
/// ORDER-BY EXISTS path — distinct from a WHERE-clause `FILTER EXISTS`. It also
/// pins the `with_planning` wiring through `apply_solution_modifiers`: the BIND
/// must carry the query's temporal context, not default to current-state.
#[tokio::test]
async fn sparql_order_by_exists_expression_sorts_by_correlated_existence() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "exists:order-by");

    // alice and dave know someone; bob and carol know no one.
    let insert = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:Person", "ex:knows": {"@id": "ex:bob"}},
            {"@id": "ex:bob",   "@type": "ex:Person"},
            {"@id": "ex:carol", "@type": "ex:Person"},
            {"@id": "ex:dave",  "@type": "ex:Person", "ex:knows": {"@id": "ex:carol"}}
        ]
    });
    let ledger = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert people");

    // ORDER BY the EXISTS boolean (false sorts before true), then by ?s.
    // Expect the two friendless people first, then the two who know someone.
    let query = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?s WHERE {
            ?s a ex:Person .
        }
        ORDER BY (EXISTS { ?s ex:knows ?o }) ?s
    ";

    let result = support::query_sparql(&fluree, &ledger.ledger, query)
        .await
        .expect("ORDER BY EXISTS query should succeed");
    let jsonld = result
        .to_jsonld(&ledger.ledger.snapshot)
        .expect("to_jsonld");

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([
            ["ex:bob"],
            ["ex:carol"],
            ["ex:alice"],
            ["ex:dave"]
        ])),
        "friendless subjects (EXISTS=false) must sort before those who know someone"
    );
}

#[tokio::test]
async fn sparql_alternation_transitive_path() {
    // `(ex:a|ex:b)*` — an alternation inside a transitive path. The closure
    // follows an edge of EITHER predicate per hop. Chain mixing both:
    //   n0 -a-> n1 -b-> n2 -a-> n3
    // From n0, `(a|b)*` reaches n0 (zero hops), n1, n2, n3. Neither `a*` nor
    // `b*` alone reaches past the first heterogeneous hop.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/sparql:alt-transitive");
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:n0","ex:a":{"@id":"ex:n1"}},
            {"@id":"ex:n1","ex:b":{"@id":"ex:n2"}},
            {"@id":"ex:n2","ex:a":{"@id":"ex:n3"}},
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?x WHERE { ex:n0 (ex:a|ex:b)* ?x }
    ";
    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("alternation-transitive sparql");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["ex:n0"], ["ex:n1"], ["ex:n2"], ["ex:n3"]])),
        "closure follows either predicate per hop: {jsonld}"
    );

    // `ex:a*` alone stops at n1 (the n1->n2 hop is ex:b).
    let single = r"
        PREFIX ex: <http://example.org/>
        SELECT ?x WHERE { ex:n0 ex:a* ?x }
    ";
    let r2 = support::query_sparql(&fluree, &ledger, single)
        .await
        .expect("single-predicate star");
    let j2 = r2.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&j2),
        normalize_rows(&json!([["ex:n0"], ["ex:n1"]])),
        "single predicate stops at the heterogeneous hop: {j2}"
    );
}

#[tokio::test]
async fn sparql_both_bound_path_reachability() {
    // `:a :p+ :c` with BOTH endpoints bound is a reachability test (W3C pp36
    // shape). With a sibling variable it yields one row iff reachable, none if
    // not. Chain a->b->c via ex:p.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/sparql:both-bound");
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:a","ex:p":{"@id":"ex:b"},"ex:tag":"A"},
            {"@id":"ex:b","ex:p":{"@id":"ex:c"}},
            {"@id":"ex:z","ex:tag":"Z"},
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    // Reachable a -> c: one row (the sibling tag binds).
    let q1 = r"PREFIX ex: <http://example.org/>
        SELECT ?t WHERE { ex:a ex:p+ ex:c . ex:a ex:tag ?t }";
    let r1 = support::query_sparql(&fluree, &ledger, q1)
        .await
        .expect("reachable");
    let j1 = r1.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&j1),
        normalize_rows(&json!([["A"]])),
        "a reaches c: {j1}"
    );

    // Not reachable a -> z: zero rows.
    let q2 = r"PREFIX ex: <http://example.org/>
        SELECT ?t WHERE { ex:a ex:p+ ex:z . ex:a ex:tag ?t }";
    let r2 = support::query_sparql(&fluree, &ledger, q2)
        .await
        .expect("unreachable");
    let j2 = r2.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&j2),
        normalize_rows(&json!([])),
        "a cannot reach z: {j2}"
    );
}
