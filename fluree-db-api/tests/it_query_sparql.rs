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

    let avg = jsonld
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|row| row.as_array())
        .and_then(|row| row.first())
        .and_then(serde_json::Value::as_f64)
        .expect("avg result");
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

    let mut values: Vec<f64> = jsonld
        .as_array()
        .expect("avg rows array")
        .iter()
        .flat_map(|row| row.as_array().expect("row array").iter())
        .filter_map(serde_json::Value::as_f64)
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
    let avg = rows[0][0].as_f64().expect("avg");
    let ceil = rows[0][1].as_f64().expect("ceil");
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
            let avg = row[1].as_f64().expect("avg");
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
async fn sparql_property_path_nested_alternative_under_transitive_errors() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = sparql_seed_knows_chain(&fluree, "sparql/path-alt-trans-err:main").await;

    // (ex:knows|ex:likes)+ — alternative inside transitive is not supported
    let query = "\
        PREFIX ex: <http://example.org/>
        SELECT ?o WHERE { ex:a (ex:knows|ex:likes)+ ?o }";

    let result = support::query_sparql(&fluree, &ledger, query).await;
    assert!(
        result.is_err(),
        "Nested alternative under transitive should error"
    );
    let msg = format!("{}", result.unwrap_err());
    assert!(
        msg.contains("simple predicate IRI"),
        "Error should mention 'simple predicate IRI', got: {msg}"
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

/// Bug 2 repro: JSON-LD graph crawl returns empty for custom namespace type.
#[tokio::test]
async fn jsonld_exact_repro_graph_crawl_custom_type() {
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
        .expect("graph crawl should not error");
    let jsonld = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");
    let rows = jsonld.as_array().expect("array");
    assert_eq!(rows.len(), 1, "should find 1 entity");
    let obj = rows[0].as_object().expect("should be object");
    assert!(
        obj.len() > 1,
        "graph crawl should return properties, not just @id; got: {obj:?}"
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
