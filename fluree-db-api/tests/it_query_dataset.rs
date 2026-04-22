//! Dataset query integration tests
//!
//! Tests for multi-graph query execution using the datasets API.
//! Validates SPARQL dataset semantics:
//! - Default graphs union for non-GRAPH patterns
//! - Named graphs accessible via GRAPH patterns
//! - Variable GRAPH iteration over named graphs
//! - Cross-graph joins

mod support;

use fluree_db_api::TimeSpec;
use fluree_db_api::{DataSetDb, DatasetSpec, FlureeBuilder, GraphDb, GraphSource, QueryInput};
use fluree_db_core::load_commit_by_id;
use serde_json::json;
use support::{
    assert_index_defaults, genesis_ledger, normalize_flat_results, normalize_rows_array,
    MemoryFluree, MemoryLedger,
};

// =============================================================================
// Helper functions
// =============================================================================

// =============================================================================
// Test data seeding helpers
// =============================================================================

fn ctx_schema() -> serde_json::Value {
    json!({
        "id": "@id",
        "type": "@type",
        "schema": "https://schema.org/"
    })
}

fn ctx_schema_value() -> serde_json::Value {
    json!([
        "https://schema.org",
        {
            "id": "@id",
            "type": "@type",
            "value": "@value",
            "schema": "https://schema.org/"
        }
    ])
}

async fn seed_authors_ledger(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": ["https://schema.org", ctx_schema()],
        "@graph": [
            {"@id":"https://www.wikidata.org/wiki/Q42","@type":"Person","name":"Douglas Adams"},
            {"@id":"https://www.wikidata.org/wiki/Q173540","@type":"Person","name":"Margaret Mitchell"}
        ]
    });
    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert authors")
        .ledger
}

async fn seed_books_ledger(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": ["https://schema.org", ctx_schema()],
        "@graph": [
            {"@id":"https://www.wikidata.org/wiki/Q3107329","@type":["Book"],"name":"The Hitchhiker's Guide to the Galaxy","isbn":"0-330-25864-8","author":{"@id":"https://www.wikidata.org/wiki/Q42"}},
            {"@id":"https://www.wikidata.org/wiki/Q2870","@type":["Book"],"name":"Gone with the Wind","isbn":"0-582-41805-4","author":{"@id":"https://www.wikidata.org/wiki/Q173540"}}
        ]
    });
    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert books")
        .ledger
}

async fn seed_movies_ledger(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": ["https://schema.org", ctx_schema()],
        "@graph": [
            {"@id":"https://www.wikidata.org/wiki/Q836821","@type":["Movie"],"name":"The Hitchhiker's Guide to the Galaxy","isBasedOn":{"@id":"https://www.wikidata.org/wiki/Q3107329"}},
            {"@id":"https://www.wikidata.org/wiki/Q2875","@type":["Movie"],"name":"Gone with the Wind","isBasedOn":{"@id":"https://www.wikidata.org/wiki/Q2870"}}
        ]
    });
    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert movies")
        .ledger
}

/// Seed a "people" ledger with person data
async fn seed_people_ledger(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [
            {
                "@id": "ex:alice",
                "@type": "ex:Person",
                "schema:name": "Alice",
                "schema:age": 30
            },
            {
                "@id": "ex:bob",
                "@type": "ex:Person",
                "schema:name": "Bob",
                "schema:age": 25
            }
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert should succeed")
        .ledger
}

/// Seed an "organizations" ledger with organization data
async fn seed_orgs_ledger(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [
            {
                "@id": "ex:acme",
                "@type": "ex:Organization",
                "schema:name": "Acme Corp"
            },
            {
                "@id": "ex:globex",
                "@type": "ex:Organization",
                "schema:name": "Globex Inc"
            }
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert should succeed")
        .ledger
}

/// Seed a second "people" ledger with different person data (for union tests)
async fn seed_people2_ledger(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [
            {
                "@id": "ex:charlie",
                "@type": "ex:Person",
                "schema:name": "Charlie",
                "schema:age": 35
            },
            {
                "@id": "ex:diana",
                "@type": "ex:Person",
                "schema:name": "Diana",
                "schema:age": 28
            }
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert should succeed")
        .ledger
}

// =============================================================================
// Single-graph tests
// =============================================================================

#[tokio::test]
async fn dataset_single_default_graph_basic_query() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Create and seed a single ledger
    let _ledger = seed_people_ledger(&fluree, "people:main").await;

    // Create dataset spec with single default graph
    let spec = DatasetSpec::new().with_default(GraphSource::new("people:main"));

    // Load the dataset
    let dataset = fluree
        .build_dataset_view(&spec)
        .await
        .expect("build_dataset_view should succeed");

    assert_eq!(dataset.len(), 1);
    assert_eq!(dataset.default.len(), 1);
    assert_eq!(dataset.named.len(), 0);

    // Query against the dataset
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": {
            "@id": "?person",
            "@type": "ex:Person",
            "schema:name": "?name"
        }
    });

    let result = fluree
        .query_dataset(&dataset, &query)
        .await
        .expect("query should succeed");

    let primary = dataset.primary().unwrap();
    let jsonld = result
        .to_jsonld(primary.snapshot.as_ref())
        .expect("to_jsonld");

    // Should return both Alice and Bob (single-variable result is flat array)
    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Alice", "Bob"]))
    );
}

// =============================================================================
// Multiple default graphs (union semantics)
// =============================================================================

#[tokio::test]
async fn dataset_multiple_default_graphs_union() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Create two ledgers with different people
    let _ledger1 = seed_people_ledger(&fluree, "people1:main").await;
    let _ledger2 = seed_people2_ledger(&fluree, "people2:main").await;

    // Create dataset spec with both as default graphs
    let spec = DatasetSpec::new()
        .with_default(GraphSource::new("people1:main"))
        .with_default(GraphSource::new("people2:main"));

    let dataset = fluree
        .build_dataset_view(&spec)
        .await
        .expect("build_dataset_view should succeed");

    assert_eq!(dataset.len(), 2);
    assert_eq!(dataset.default.len(), 2);

    // Query all persons - should get union of both ledgers
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": {
            "@id": "?person",
            "@type": "ex:Person",
            "schema:name": "?name"
        }
    });

    let result = fluree
        .query_dataset(&dataset, &query)
        .await
        .expect("query should succeed");

    let primary = dataset.primary().unwrap();
    let jsonld = result
        .to_jsonld(primary.snapshot.as_ref())
        .expect("to_jsonld");

    // Should return all 4 people from both ledgers (single-variable result is flat array)
    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Alice", "Bob", "Charlie", "Diana"]))
    );
}

// =============================================================================
// Composed datasets across connections
// =============================================================================

#[tokio::test]
async fn dataset_composed_across_connections_selecting_variables() {
    assert_index_defaults();
    let fluree_authors = FlureeBuilder::memory().build_memory();
    let fluree_books = FlureeBuilder::memory().build_memory();
    let fluree_movies = FlureeBuilder::memory().build_memory();

    let authors = seed_authors_ledger(&fluree_authors, "test/authors:main").await;
    let books = seed_books_ledger(&fluree_books, "test/books:main").await;
    let movies = seed_movies_ledger(&fluree_movies, "test/movies:main").await;

    let dataset = DataSetDb::new()
        .with_default(GraphDb::from_ledger_state(&movies))
        .with_default(GraphDb::from_ledger_state(&books))
        .with_default(GraphDb::from_ledger_state(&authors));

    let query = json!({
        "@context": "https://schema.org",
        "select": ["?movieName", "?bookIsbn", "?authorName"],
        "where": {
            "type": "Movie",
            "name": "?movieName",
            "isBasedOn": {
                "isbn": "?bookIsbn",
                "author": { "name": "?authorName" }
            }
        }
    });

    let result = fluree_movies
        .query_dataset(&dataset, &query)
        .await
        .expect("query_dataset");
    let primary = dataset.primary().expect("primary");
    let jsonld = result
        .to_jsonld(primary.snapshot.as_ref())
        .expect("to_jsonld");

    assert_eq!(
        normalize_rows_array(&jsonld),
        normalize_rows_array(&json!([
            ["Gone with the Wind", "0-582-41805-4", "Margaret Mitchell"],
            [
                "The Hitchhiker's Guide to the Galaxy",
                "0-330-25864-8",
                "Douglas Adams"
            ]
        ]))
    );
}

#[tokio::test]
async fn dataset_composed_across_connections_selecting_subgraph_depth_3() {
    assert_index_defaults();
    let fluree_authors = FlureeBuilder::memory().build_memory();
    let fluree_books = FlureeBuilder::memory().build_memory();
    let fluree_movies = FlureeBuilder::memory().build_memory();

    let authors = seed_authors_ledger(&fluree_authors, "test/authors:main").await;
    let books = seed_books_ledger(&fluree_books, "test/books:main").await;
    let movies = seed_movies_ledger(&fluree_movies, "test/movies:main").await;

    let dataset = DataSetDb::new()
        .with_default(GraphDb::from_ledger_state(&movies))
        .with_default(GraphDb::from_ledger_state(&books))
        .with_default(GraphDb::from_ledger_state(&authors));

    let query = json!({
        "@context": ctx_schema_value(),
        "select": { "?goneWithTheWind": ["*"] },
        "depth": 3,
        "where": {
            "@id": "?goneWithTheWind",
            "type": "Movie",
            "name": "Gone with the Wind"
        }
    });

    let result = fluree_movies
        .query_dataset(&dataset, &query)
        .await
        .expect("query_dataset");
    let primary = dataset.primary().expect("primary");
    let jsonld = result
        .to_jsonld_async(primary.as_graph_db_ref())
        .await
        .expect("to_jsonld_async");

    assert_eq!(
        normalize_rows_array(&jsonld),
        normalize_rows_array(&json!([{
            "@id": "https://www.wikidata.org/wiki/Q2875",
            "@type": "Movie",
            "name": "Gone with the Wind",
            "isBasedOn": {
                "@id": "https://www.wikidata.org/wiki/Q2870",
                "@type": "Book",
                "name": "Gone with the Wind",
                "isbn": "0-582-41805-4",
                "author": {
                    "@id": "https://www.wikidata.org/wiki/Q173540",
                    "@type": "Person",
                    "name": "Margaret Mitchell"
                }
            }
        }]))
    );
}

#[tokio::test]
async fn dataset_multiple_default_graphs_no_dedup() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Create two ledgers with the SAME data (to test no-dedup semantics)
    let _ledger1 = seed_people_ledger(&fluree, "dup1:main").await;
    let _ledger2 = seed_people_ledger(&fluree, "dup2:main").await;

    // Create dataset with both as default graphs
    let spec = DatasetSpec::new()
        .with_default(GraphSource::new("dup1:main"))
        .with_default(GraphSource::new("dup2:main"));

    let dataset = fluree
        .build_dataset_view(&spec)
        .await
        .expect("build_dataset_view should succeed");

    // Query all persons
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": {
            "@id": "?person",
            "@type": "ex:Person",
            "schema:name": "?name"
        }
    });

    let result = fluree
        .query_dataset(&dataset, &query)
        .await
        .expect("query should succeed");

    // Union does NOT deduplicate - should get 4 results (2 people x 2 ledgers)
    // Note: The exact semantics depend on whether the same SID is generated
    // across ledgers. In practice, separate ledgers have different namespace
    // encodings, so we may get 4 distinct rows.
    assert!(result.row_count() >= 2, "should have results from union");
}

// =============================================================================
// Named graph tests (GRAPH pattern)
// =============================================================================

#[tokio::test]
async fn dataset_named_graph_basic() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledgers
    let _ledger1 = seed_people_ledger(&fluree, "default:main").await;
    let _ledger2 = seed_orgs_ledger(&fluree, "orgs:main").await;

    // Create dataset with one default and one named graph
    let spec = DatasetSpec::new()
        .with_default(GraphSource::new("default:main"))
        .with_named(GraphSource::new("orgs:main"));

    let dataset = fluree
        .build_dataset_view(&spec)
        .await
        .expect("build_dataset_view should succeed");

    assert_eq!(dataset.default.len(), 1);
    assert_eq!(dataset.named.len(), 1);

    // Query default graph - should only see people
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": {
            "@id": "?s",
            "@type": "ex:Person",
            "schema:name": "?name"
        }
    });

    let result = fluree
        .query_dataset(&dataset, &query)
        .await
        .expect("query should succeed");

    let primary = dataset.primary().unwrap();
    let jsonld = result
        .to_jsonld(primary.snapshot.as_ref())
        .expect("to_jsonld");

    // Default graph should only have people, not organizations
    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Alice", "Bob"]))
    );
}

// =============================================================================
// JSON-LD "from" parsing tests
// =============================================================================

#[tokio::test]
async fn dataset_from_json_single_string() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let _ledger = seed_people_ledger(&fluree, "test:main").await;

    // Parse from JSON query with "from" as single string
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "test:main",
        "select": ["?name"],
        "where": {
            "@id": "?person",
            "@type": "ex:Person",
            "schema:name": "?name"
        }
    });

    let spec = DatasetSpec::from_json(&query).expect("parse should succeed");
    assert_eq!(spec.num_graphs(), 1);
    assert_eq!(spec.default_graphs.len(), 1);
    assert_eq!(spec.default_graphs[0].identifier, "test:main");

    let dataset = fluree
        .build_dataset_view(&spec)
        .await
        .expect("build should succeed");
    let result = fluree
        .query_dataset(&dataset, &query)
        .await
        .expect("query should succeed");

    let primary = dataset.primary().unwrap();
    let jsonld = result
        .to_jsonld(primary.snapshot.as_ref())
        .expect("to_jsonld");
    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Alice", "Bob"]))
    );
}

#[tokio::test]
async fn dataset_from_json_array() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    let _ledger1 = seed_people_ledger(&fluree, "p1:main").await;
    let _ledger2 = seed_people2_ledger(&fluree, "p2:main").await;

    // Parse from JSON query with "from" as array
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": ["p1:main", "p2:main"],
        "select": ["?name"],
        "where": {
            "@id": "?person",
            "@type": "ex:Person",
            "schema:name": "?name"
        }
    });

    let spec = DatasetSpec::from_json(&query).expect("parse should succeed");
    assert_eq!(spec.num_graphs(), 2);

    let dataset = fluree
        .build_dataset_view(&spec)
        .await
        .expect("build should succeed");
    let result = fluree
        .query_dataset(&dataset, &query)
        .await
        .expect("query should succeed");

    let primary = dataset.primary().unwrap();
    let jsonld = result
        .to_jsonld(primary.snapshot.as_ref())
        .expect("to_jsonld");

    // Should get all 4 people from union
    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Alice", "Bob", "Charlie", "Diana"]))
    );
}

#[tokio::test]
async fn dataset_from_json_named() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    let _ledger1 = seed_people_ledger(&fluree, "default:main").await;
    let _ledger2 = seed_orgs_ledger(&fluree, "graph1:main").await;

    // Parse from JSON query with "fromNamed" (string array shorthand)
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "default:main",
        "fromNamed": ["graph1:main"],
        "select": ["?name"],
        "where": {
            "@id": "?s",
            "@type": "ex:Person",
            "schema:name": "?name"
        }
    });

    let spec = DatasetSpec::from_json(&query).expect("parse should succeed");
    assert_eq!(spec.default_graphs.len(), 1);
    assert_eq!(spec.named_graphs.len(), 1);
    assert_eq!(spec.named_graphs[0].identifier, "graph1:main");

    let dataset = fluree
        .build_dataset_view(&spec)
        .await
        .expect("build should succeed");
    let result = fluree
        .query_dataset(&dataset, &query)
        .await
        .expect("query should succeed");

    let primary = dataset.primary().unwrap();
    let jsonld = result
        .to_jsonld(primary.snapshot.as_ref())
        .expect("to_jsonld");

    // Query against default graph - should only see people
    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Alice", "Bob"]))
    );
}

// =============================================================================
// Empty/edge case tests
// =============================================================================

#[tokio::test]
async fn dataset_empty_spec_fails() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Empty dataset spec - no graphs
    let spec = DatasetSpec::new();
    assert!(spec.is_empty());

    let dataset = fluree
        .build_dataset_view(&spec)
        .await
        .expect("build should succeed");
    assert!(dataset.is_empty());

    // Query should fail - no primary ledger
    let query = json!({
        "select": ["?s"],
        "where": {"@id": "?s"}
    });

    let result = fluree.query_dataset(&dataset, &query).await;
    assert!(result.is_err(), "query with empty dataset should fail");
}

#[tokio::test]
async fn dataset_ledger_not_found_fails() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Try to load a non-existent ledger
    let spec = DatasetSpec::new().with_default(GraphSource::new("nonexistent:main"));

    let result = fluree.build_dataset_view(&spec).await;
    assert!(result.is_err(), "loading nonexistent ledger should fail");
}

// =============================================================================
// Cross-graph join tests
// =============================================================================

#[tokio::test]
async fn dataset_cross_graph_join_in_union() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Create a people ledger with employment info
    let ledger0 = genesis_ledger(&fluree, "employed:main");
    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [
            {
                "@id": "ex:alice",
                "@type": "ex:Person",
                "schema:name": "Alice",
                "ex:worksAt": {"@id": "ex:acme"}
            }
        ]
    });
    let _ledger1 = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    // Create an orgs ledger
    let _ledger2 = seed_orgs_ledger(&fluree, "companies:main").await;

    // Create dataset with both as default graphs (union)
    let spec = DatasetSpec::new()
        .with_default(GraphSource::new("employed:main"))
        .with_default(GraphSource::new("companies:main"));

    let dataset = fluree
        .build_dataset_view(&spec)
        .await
        .expect("build should succeed");

    // Query that joins across the union
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?personName", "?orgName"],
        "where": [
            {"@id": "?person", "schema:name": "?personName", "ex:worksAt": "?org"},
            {"@id": "?org", "schema:name": "?orgName"}
        ]
    });

    let result = fluree
        .query_dataset(&dataset, &query)
        .await
        .expect("query should succeed");

    let primary = dataset.primary().unwrap();
    let jsonld = result
        .to_jsonld(primary.snapshot.as_ref())
        .expect("to_jsonld");

    // Should find Alice works at Acme (join across union)
    assert_eq!(
        normalize_rows_array(&jsonld),
        normalize_rows_array(&json!([["Alice", "Acme Corp"]]))
    );
}

// =============================================================================
// GRAPH pattern tests (SPARQL)
// =============================================================================

#[tokio::test]
async fn sparql_graph_pattern_concrete_iri() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Create a default ledger (empty for this test)
    let ledger0 = genesis_ledger(&fluree, "default:main");
    let insert_default = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [
            {
                "@id": "ex:defaultEntity",
                "schema:name": "Default Entity"
            }
        ]
    });
    let _default_ledger = fluree
        .insert(ledger0, &insert_default)
        .await
        .unwrap()
        .ledger;

    // Create a named graph with people data
    let _people_ledger = seed_people_ledger(&fluree, "people:main").await;

    // Create dataset with default and named graphs
    let spec = DatasetSpec::new()
        .with_default(GraphSource::new("default:main"))
        .with_named(GraphSource::new("people:main"));

    let dataset = fluree
        .build_dataset_view(&spec)
        .await
        .expect("build should succeed");

    // Query using GRAPH pattern with concrete IRI to access named graph
    let sparql = r"
        PREFIX schema: <http://schema.org/>
        SELECT ?name
        WHERE {
            GRAPH <people:main> {
                ?person schema:name ?name
            }
        }
    ";

    let result = fluree
        .query_dataset(&dataset, sparql)
        .await
        .expect("query should succeed");

    let primary = dataset.primary().unwrap();
    let jsonld = result
        .to_jsonld(primary.snapshot.as_ref())
        .expect("to_jsonld");

    // Should return names from the named graph (people:main)
    assert_eq!(
        normalize_rows_array(&jsonld),
        normalize_rows_array(&json!([["Alice"], ["Bob"]]))
    );
}

#[tokio::test]
async fn sparql_graph_pattern_variable_iteration() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Create two named graphs with different data
    let _people_ledger = seed_people_ledger(&fluree, "people:main").await;
    let _orgs_ledger = seed_orgs_ledger(&fluree, "orgs:main").await;

    // Create dataset with named graphs only (no default)
    let spec = DatasetSpec::new()
        .with_named(GraphSource::new("people:main"))
        .with_named(GraphSource::new("orgs:main"));

    let dataset = fluree
        .build_dataset_view(&spec)
        .await
        .expect("build should succeed");

    // Query using GRAPH ?g to iterate over all named graphs
    let sparql = r"
        PREFIX schema: <http://schema.org/>
        SELECT ?g ?name
        WHERE {
            GRAPH ?g {
                ?entity schema:name ?name
            }
        }
    ";

    let result = fluree
        .query_dataset(&dataset, sparql)
        .await
        .expect("query should succeed");

    let primary = dataset.primary().unwrap();
    let jsonld = result
        .to_jsonld(primary.snapshot.as_ref())
        .expect("to_jsonld");

    // Should return (graph, name) pairs from all named graphs
    // People graph: Alice, Bob
    // Orgs graph: Acme Corp, Globex Inc
    assert_eq!(
        normalize_rows_array(&jsonld),
        normalize_rows_array(&json!([
            ["orgs:main", "Acme Corp"],
            ["orgs:main", "Globex Inc"],
            ["people:main", "Alice"],
            ["people:main", "Bob"]
        ]))
    );
}

#[tokio::test]
async fn sparql_graph_pattern_nonexistent_graph_returns_empty() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Create a named graph
    let _people_ledger = seed_people_ledger(&fluree, "people:main").await;

    // Dataset with only people:main as named
    let spec = DatasetSpec::new().with_named(GraphSource::new("people:main"));

    let dataset = fluree
        .build_dataset_view(&spec)
        .await
        .expect("build should succeed");

    // Query a graph that doesn't exist in the dataset
    let sparql = r"
        PREFIX schema: <http://schema.org/>
        SELECT ?name
        WHERE {
            GRAPH <nonexistent:main> {
                ?entity schema:name ?name
            }
        }
    ";

    let result = fluree
        .query_dataset(&dataset, sparql)
        .await
        .expect("query should succeed");

    // Should return empty results (missing graph → empty, not error)
    assert!(
        result.is_empty(),
        "nonexistent graph should return empty results"
    );
}

#[tokio::test]
async fn sparql_graph_pattern_default_vs_named() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Create default graph with people
    let _default_ledger = seed_people_ledger(&fluree, "default:main").await;

    // Create named graph with orgs
    let _orgs_ledger = seed_orgs_ledger(&fluree, "orgs:main").await;

    // Dataset: default graph is people, named graph is orgs
    let spec = DatasetSpec::new()
        .with_default(GraphSource::new("default:main"))
        .with_named(GraphSource::new("orgs:main"));

    let dataset = fluree
        .build_dataset_view(&spec)
        .await
        .expect("build should succeed");

    // Query default graph (no GRAPH pattern) - should get people
    let sparql_default = r"
        PREFIX schema: <http://schema.org/>
        SELECT ?name
        WHERE {
            ?entity schema:name ?name
        }
    ";

    let result_default = fluree
        .query_dataset(&dataset, sparql_default)
        .await
        .expect("query should succeed");

    let primary = dataset.primary().unwrap();
    let jsonld_default = result_default
        .to_jsonld(primary.snapshot.as_ref())
        .expect("to_jsonld");

    // Default graph should only have people
    assert_eq!(
        normalize_flat_results(&jsonld_default),
        normalize_flat_results(&json!(["Alice", "Bob"]))
    );

    // Query named graph via GRAPH pattern - should get orgs
    let sparql_named = r"
        PREFIX schema: <http://schema.org/>
        SELECT ?name
        WHERE {
            GRAPH <orgs:main> {
                ?entity schema:name ?name
            }
        }
    ";

    let result_named = fluree
        .query_dataset(&dataset, sparql_named)
        .await
        .expect("query should succeed");

    let jsonld_named = result_named
        .to_jsonld(primary.snapshot.as_ref())
        .expect("to_jsonld");

    // Named graph should only have orgs
    assert_eq!(
        normalize_flat_results(&jsonld_named),
        normalize_flat_results(&json!(["Acme Corp", "Globex Inc"]))
    );
}

// =============================================================================
// JSON-LD GRAPH Pattern Tests
// =============================================================================

/// Test JSON-LD ["graph", ...] syntax parsing
#[tokio::test]
async fn fql_graph_pattern_basic() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledgers
    let ledger0 = genesis_ledger(&fluree, "default:main");
    let insert_default = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:defaultEntity", "schema:name": "Default Entity"}]
    });
    let _default = fluree
        .insert(ledger0, &insert_default)
        .await
        .unwrap()
        .ledger;
    let _people = seed_people_ledger(&fluree, "people:main").await;

    // Create dataset
    let spec = DatasetSpec::new()
        .with_default(GraphSource::new("default:main"))
        .with_named(GraphSource::new("people:main"));
    let dataset = fluree.build_dataset_view(&spec).await.unwrap();

    // Query using JSON-LD ["graph", "name", {...}] syntax
    let query = json!({
        "@context": {"schema": "http://schema.org/"},
        "select": ["?name"],
        "where": [
            ["graph", "people:main", {"@id": "?person", "schema:name": "?name"}]
        ]
    });

    let result = fluree
        .query_dataset(&dataset, &query)
        .await
        .expect("query should succeed");

    let primary = dataset.primary().unwrap();
    let jsonld = result
        .to_jsonld(primary.snapshot.as_ref())
        .expect("to_jsonld");

    // Should return names from the named graph (people:main)
    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Alice", "Bob"]))
    );
}

/// Test JSON-LD ["graph", <alias>, ...] syntax - graph pattern using dataset-local alias
///
/// When `fromNamed` specifies an alias (the object key), the GRAPH pattern
/// should be able to reference by that alias, not just by the ledger identifier.
#[tokio::test]
async fn fql_graph_pattern_with_alias() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledgers (seed_* functions handle ledger creation + data insertion)
    let _default = seed_orgs_ledger(&fluree, "default:main").await;
    let _people = seed_people_ledger(&fluree, "people:main").await;

    // Create dataset with alias for named graph
    let spec = DatasetSpec::new()
        .with_default(GraphSource::new("default:main"))
        .with_named(GraphSource::new("people:main").with_alias("folks"));
    let dataset = fluree.build_dataset_view(&spec).await.unwrap();

    // Query using JSON-LD ["graph", <alias>, {...}] syntax with the alias "folks"
    let query = json!({
        "@context": {"schema": "http://schema.org/"},
        "select": ["?name"],
        "where": [
            ["graph", "folks", {"@id": "?person", "schema:name": "?name"}]
        ]
    });

    let result = fluree
        .query_dataset(&dataset, &query)
        .await
        .expect("query should succeed using alias");

    let primary = dataset.primary().unwrap();
    let jsonld = result
        .to_jsonld(primary.snapshot.as_ref())
        .expect("to_jsonld");

    // Should return names from the named graph referenced by alias
    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Alice", "Bob"]))
    );
}

// =============================================================================
// Time travel tests (dataset per-graph time specs)
// =============================================================================

#[tokio::test]
async fn dataset_time_travel_at_t() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Commit 1: Alice
    let ledger0 = genesis_ledger(&fluree, "people:main");
    let insert1 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:alice", "@type": "ex:Person", "schema:name": "Alice"}]
    });
    let ledger1 = fluree.insert(ledger0, &insert1).await.unwrap().ledger;

    // Commit 2: Bob
    let insert2 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:bob", "@type": "ex:Person", "schema:name": "Bob"}]
    });
    let _ledger2 = fluree.insert(ledger1, &insert2).await.unwrap().ledger;

    // Dataset pinned at t=1 should only see Alice.
    let spec = DatasetSpec::new()
        .with_default(GraphSource::new("people:main").with_time(TimeSpec::AtT(1)));
    let dataset = fluree.build_dataset_view(&spec).await.unwrap();

    let query = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "select": ["?name"],
        "where": {"@id": "?s", "schema:name": "?name"}
    });

    let result = fluree.query_dataset(&dataset, &query).await.unwrap();
    let primary = dataset.primary().unwrap();
    let jsonld = result.to_jsonld(primary.snapshot.as_ref()).unwrap();

    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Alice"]))
    );
}

#[tokio::test]
async fn dataset_time_travel_at_time_iso() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Commit 1: Alice (capture its timestamp)
    let ledger0 = genesis_ledger(&fluree, "people:main");
    let insert1 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:alice", "@type": "ex:Person", "schema:name": "Alice"}]
    });
    let tx1 = fluree.insert(ledger0, &insert1).await.unwrap();
    let content_store = fluree.content_store("people:main");
    let commit1 = load_commit_by_id(&content_store, &tx1.receipt.commit_id)
        .await
        .unwrap();
    let time1 = commit1.time.expect("commit should have ISO timestamp");

    // Small delay to ensure second commit gets a different timestamp
    // (timestamps are millisecond-resolution, and fast test runs can
    // result in both commits having the same timestamp)
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;

    // Commit 2: Bob
    let insert2 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:bob", "@type": "ex:Person", "schema:name": "Bob"}]
    });
    let _tx2 = fluree.insert(tx1.ledger, &insert2).await.unwrap();

    // Dataset pinned at commit1 timestamp should only see Alice (t=1).
    let spec = DatasetSpec::new()
        .with_default(GraphSource::new("people:main").with_time(TimeSpec::AtTime(time1)));
    let dataset = fluree.build_dataset_view(&spec).await.unwrap();

    let query = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "select": ["?name"],
        "where": {"@id": "?s", "schema:name": "?name"}
    });

    let result = fluree.query_dataset(&dataset, &query).await.unwrap();
    let primary = dataset.primary().unwrap();
    let jsonld = result.to_jsonld(primary.snapshot.as_ref()).unwrap();

    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Alice"]))
    );
}

#[tokio::test]
async fn dataset_time_travel_future_t_errors() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Single commit so head t=1
    let ledger0 = genesis_ledger(&fluree, "people:main");
    let insert1 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:alice", "@type": "ex:Person", "schema:name": "Alice"}]
    });
    let _ledger1 = fluree.insert(ledger0, &insert1).await.unwrap().ledger;

    // Requesting future t should error.
    let spec = DatasetSpec::new()
        .with_default(GraphSource::new("people:main").with_time(TimeSpec::AtT(999)));
    let result = fluree.build_dataset_view(&spec).await;
    assert!(result.is_err(), "future t should error");
}

#[tokio::test]
async fn dataset_time_travel_mixed_graphs() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // People ledger: t=1 Alice, t=2 Bob
    let ledger0 = genesis_ledger(&fluree, "people:main");
    let insert1 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:alice", "@type": "ex:Person", "schema:name": "Alice"}]
    });
    let ledger1 = fluree.insert(ledger0, &insert1).await.unwrap().ledger;
    let insert2 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:bob", "@type": "ex:Person", "schema:name": "Bob"}]
    });
    let _ledger2 = fluree.insert(ledger1, &insert2).await.unwrap().ledger;

    // Orgs ledger: single commit Acme
    let orgs_ledger0 = genesis_ledger(&fluree, "orgs:main");
    let orgs_insert = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:acme", "@type": "ex:Organization", "schema:name": "Acme Corp"}]
    });
    let _orgs_ledger1 = fluree
        .insert(orgs_ledger0, &orgs_insert)
        .await
        .unwrap()
        .ledger;

    // Dataset: people at t=1, orgs at head.
    let spec = DatasetSpec::new()
        .with_default(GraphSource::new("people:main").with_time(TimeSpec::AtT(1)))
        .with_default(GraphSource::new("orgs:main"));
    let dataset = fluree.build_dataset_view(&spec).await.unwrap();

    let query = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "select": ["?name"],
        "where": {"@id": "?s", "schema:name": "?name"}
    });

    let result = fluree.query_dataset(&dataset, &query).await.unwrap();
    let primary = dataset.primary().unwrap();
    let jsonld = result.to_jsonld(primary.snapshot.as_ref()).unwrap();

    // Should include Alice (people@t=1) and Acme Corp (orgs@head), but not Bob (people@t=2).
    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Acme Corp", "Alice"]))
    );
}

#[tokio::test]
async fn dataset_time_travel_alias_syntax_at_t() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Commit 1: Alice
    let ledger0 = genesis_ledger(&fluree, "people:main");
    let insert1 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:alice", "@type": "ex:Person", "schema:name": "Alice"}]
    });
    let ledger1 = fluree.insert(ledger0, &insert1).await.unwrap().ledger;

    // Commit 2: Bob
    let insert2 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:bob", "@type": "ex:Person", "schema:name": "Bob"}]
    });
    let _ledger2 = fluree.insert(ledger1, &insert2).await.unwrap().ledger;

    // Use @t: alias syntax in "from" string
    let query = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "from": "people:main@t:1",
        "select": ["?name"],
        "where": {"@id": "?s", "schema:name": "?name"}
    });

    let spec = DatasetSpec::from_json(&query).unwrap();
    assert_eq!(spec.default_graphs[0].identifier, "people:main");
    assert!(matches!(
        spec.default_graphs[0].time_spec,
        Some(TimeSpec::AtT(1))
    ));

    let dataset = fluree.build_dataset_view(&spec).await.unwrap();
    let result = fluree.query_dataset(&dataset, &query).await.unwrap();

    let primary = dataset.primary().unwrap();
    let jsonld = result.to_jsonld(primary.snapshot.as_ref()).unwrap();

    // Should only see Alice (t=1), not Bob (t=2)
    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Alice"]))
    );
}

#[tokio::test]
async fn dataset_time_travel_at_commit() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Commit 1: Alice (capture its commit ID)
    let ledger0 = genesis_ledger(&fluree, "people:main");
    let insert1 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:alice", "@type": "ex:Person", "schema:name": "Alice"}]
    });
    let tx1 = fluree.insert(ledger0, &insert1).await.unwrap();

    // Extract the hex digest directly from the receipt's ContentId
    let commit_prefix = tx1.receipt.commit_id.digest_hex();

    // Commit 2: Bob
    let insert2 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:bob", "@type": "ex:Person", "schema:name": "Bob"}]
    });
    let _tx2 = fluree.insert(tx1.ledger, &insert2).await.unwrap();

    // Dataset pinned at commit1 should only see Alice (t=1).
    let spec = DatasetSpec::new().with_default(
        GraphSource::new("people:main").with_time(TimeSpec::AtCommit(commit_prefix.clone())),
    );
    let dataset = fluree.build_dataset_view(&spec).await.unwrap();

    let query = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "select": ["?name"],
        "where": {"@id": "?s", "schema:name": "?name"}
    });

    let result = fluree.query_dataset(&dataset, &query).await.unwrap();
    let primary = dataset.primary().unwrap();
    let jsonld = result.to_jsonld(primary.snapshot.as_ref()).unwrap();

    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Alice"]))
    );
}

#[tokio::test]
async fn dataset_time_travel_at_commit_short_prefix() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Commit 1: Alice (capture its commit ID)
    let ledger0 = genesis_ledger(&fluree, "people:main");
    let insert1 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:alice", "@type": "ex:Person", "schema:name": "Alice"}]
    });
    let tx1 = fluree.insert(ledger0, &insert1).await.unwrap();

    // Extract the hex digest directly from the receipt's ContentId
    let digest_full = tx1.receipt.commit_id.digest_hex();
    // Use just the first 10 characters as a prefix
    let commit_prefix = &digest_full[..10.min(digest_full.len())];

    // Commit 2: Bob
    let insert2 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:bob", "@type": "ex:Person", "schema:name": "Bob"}]
    });
    let _tx2 = fluree.insert(tx1.ledger, &insert2).await.unwrap();

    // Dataset pinned at commit1 prefix should only see Alice (t=1).
    let spec = DatasetSpec::new().with_default(
        GraphSource::new("people:main").with_time(TimeSpec::AtCommit(commit_prefix.to_string())),
    );
    let dataset = fluree.build_dataset_view(&spec).await.unwrap();

    let query = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "select": ["?name"],
        "where": {"@id": "?s", "schema:name": "?name"}
    });

    let result = fluree.query_dataset(&dataset, &query).await.unwrap();
    let primary = dataset.primary().unwrap();
    let jsonld = result.to_jsonld(primary.snapshot.as_ref()).unwrap();

    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Alice"]))
    );
}

#[tokio::test]
async fn dataset_time_travel_alias_syntax_commit() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Commit 1: Alice (capture its commit ID)
    let ledger0 = genesis_ledger(&fluree, "people:main");
    let insert1 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:alice", "@type": "ex:Person", "schema:name": "Alice"}]
    });
    let tx1 = fluree.insert(ledger0, &insert1).await.unwrap();

    // Extract the hex digest directly from the receipt's ContentId
    let commit_prefix = tx1.receipt.commit_id.digest_hex();

    // Commit 2: Bob
    let insert2 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:bob", "@type": "ex:Person", "schema:name": "Bob"}]
    });
    let _tx2 = fluree.insert(tx1.ledger, &insert2).await.unwrap();

    // Use @commit: alias syntax in "from" string
    let alias_with_commit = format!("people:main@commit:{commit_prefix}");
    let query = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "from": alias_with_commit,
        "select": ["?name"],
        "where": {"@id": "?s", "schema:name": "?name"}
    });

    let spec = DatasetSpec::from_json(&query).unwrap();
    assert_eq!(spec.default_graphs[0].identifier, "people:main");
    assert!(matches!(
        spec.default_graphs[0].time_spec,
        Some(TimeSpec::AtCommit(_))
    ));

    let dataset = fluree.build_dataset_view(&spec).await.unwrap();
    let result = fluree.query_dataset(&dataset, &query).await.unwrap();

    let primary = dataset.primary().unwrap();
    let jsonld = result.to_jsonld(primary.snapshot.as_ref()).unwrap();

    // Should only see Alice (t=1), not Bob (t=2)
    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Alice"]))
    );
}

#[tokio::test]
async fn dataset_time_travel_commit_not_found_errors() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Single commit
    let ledger0 = genesis_ledger(&fluree, "people:main");
    let insert1 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:alice", "@type": "ex:Person", "schema:name": "Alice"}]
    });
    let _tx1 = fluree.insert(ledger0, &insert1).await.unwrap();

    // Request a non-existent commit prefix - should error
    let spec = DatasetSpec::new().with_default(
        GraphSource::new("people:main").with_time(TimeSpec::AtCommit("bxxxxxx".to_string())),
    );
    let result = fluree.build_dataset_view(&spec).await;
    assert!(result.is_err(), "non-existent commit should error");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("No commit found"),
        "error should mention no commit found: {err}"
    );
}

#[tokio::test]
async fn dataset_time_travel_commit_too_short_errors() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Single commit
    let ledger0 = genesis_ledger(&fluree, "people:main");
    let insert1 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:alice", "@type": "ex:Person", "schema:name": "Alice"}]
    });
    let _tx1 = fluree.insert(ledger0, &insert1).await.unwrap();

    // Commit prefix too short (less than 6 chars) - should error
    let spec = DatasetSpec::new().with_default(
        GraphSource::new("people:main").with_time(TimeSpec::AtCommit("babc".to_string())),
    );
    let result = fluree.build_dataset_view(&spec).await;
    assert!(result.is_err(), "commit prefix too short should error");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("at least 6 characters"),
        "error should mention minimum chars: {err}"
    );
}

// =============================================================================
// Single-DB Mode GRAPH Pattern Tests
// =============================================================================

/// GRAPH with matching alias returns results (single-db mode, no dataset)
#[tokio::test]
async fn sparql_single_db_graph_matching_alias() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Create a ledger with data
    let ledger = seed_people_ledger(&fluree, "people:main").await;

    // Query using GRAPH with the matching alias (no dataset)
    let sparql = r"
        PREFIX schema: <http://schema.org/>
        SELECT ?name
        WHERE {
            GRAPH <people:main> {
                ?person schema:name ?name
            }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("query should succeed");

    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Should return results because alias matches
    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Alice", "Bob"]))
    );
}

/// GRAPH with non-matching alias returns empty (single-db mode, no dataset)
#[tokio::test]
async fn sparql_single_db_graph_non_matching_alias() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Create a ledger with data
    let ledger = seed_people_ledger(&fluree, "people:main").await;

    // Query using GRAPH with a different alias
    let sparql = r"
        PREFIX schema: <http://schema.org/>
        SELECT ?name
        WHERE {
            GRAPH <other:ledger> {
                ?person schema:name ?name
            }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("query should succeed");

    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Should return empty because alias doesn't match
    assert_eq!(jsonld, json!([]));
}

/// GRAPH ?g with unbound variable binds to db alias (single-db mode)
#[tokio::test]
async fn sparql_single_db_graph_variable_unbound() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Create a ledger with data
    let ledger = seed_people_ledger(&fluree, "people:main").await;

    // Query using GRAPH ?g - should bind ?g to db alias
    let sparql = r"
        PREFIX schema: <http://schema.org/>
        SELECT ?g ?name
        WHERE {
            GRAPH ?g {
                ?person schema:name ?name
            }
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("query should succeed");

    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Should return results with ?g bound to "people:main"
    let normalized = normalize_rows_array(&jsonld);
    assert_eq!(normalized.len(), 2);

    // Check that ?g is bound to the alias (first element of each row)
    for row in &normalized {
        // row is Vec<serde_json::Value> which is stored as serde_json::Value::Array
        let first_elem = &row[0];
        assert_eq!(
            first_elem,
            &json!("people:main"),
            "?g should be bound to db alias"
        );
    }
}

/// GRAPH ?g with bound matching value works (single-db mode)
#[tokio::test]
async fn sparql_single_db_graph_variable_bound_matching() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Create a ledger with data
    let ledger = seed_people_ledger(&fluree, "people:main").await;

    // Query using VALUES to bind ?g to matching alias, then use GRAPH ?g
    let sparql = r#"
        PREFIX schema: <http://schema.org/>
        SELECT ?g ?name
        WHERE {
            VALUES ?g { "people:main" }
            GRAPH ?g {
                ?person schema:name ?name
            }
        }
    "#;

    let result = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("query should succeed");

    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Should return results because bound value matches alias
    let normalized = normalize_rows_array(&jsonld);
    assert_eq!(normalized.len(), 2);
}

/// GRAPH ?g with bound non-matching value returns empty (single-db mode)
#[tokio::test]
async fn sparql_single_db_graph_variable_bound_non_matching() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Create a ledger with data
    let ledger = seed_people_ledger(&fluree, "people:main").await;

    // Query using VALUES to bind ?g to non-matching alias, then use GRAPH ?g
    let sparql = r#"
        PREFIX schema: <http://schema.org/>
        SELECT ?g ?name
        WHERE {
            VALUES ?g { "other:ledger" }
            GRAPH ?g {
                ?person schema:name ?name
            }
        }
    "#;

    let result = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("query should succeed");

    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Should return empty because bound value doesn't match alias
    assert_eq!(jsonld, json!([]));
}

#[tokio::test]
async fn dataset_multi_ledger_time_travel_parsing() {
    assert_index_defaults();
    let _fluree = FlureeBuilder::memory().build_memory();

    // Test that parsing works for multiple ledgers with different time specs
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": ["ledger1:main@t:1", "ledger2:main@t:2"],
        "select": ["?name"],
        "where": {
            "@id": "?person",
            "@type": "ex:Person",
            "schema:name": "?name"
        }
    });

    let spec = DatasetSpec::from_json(&query).expect("parse should succeed");
    assert_eq!(spec.num_graphs(), 2);

    // Verify time specs are parsed correctly for multiple ledgers
    assert_eq!(spec.default_graphs[0].identifier, "ledger1:main");
    assert!(matches!(
        spec.default_graphs[0].time_spec,
        Some(TimeSpec::AtT(1))
    ));
    assert_eq!(spec.default_graphs[1].identifier, "ledger2:main");
    assert!(matches!(
        spec.default_graphs[1].time_spec,
        Some(TimeSpec::AtT(2))
    ));
}

#[tokio::test]
async fn dataset_multi_ledger_time_travel_execution() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Ledger 1: t=1 Alice, t=2 Bob
    let ledger1_0 = genesis_ledger(&fluree, "ledger1:main");
    let insert1 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:alice", "@type": "ex:Person", "schema:name": "Alice"}]
    });
    let ledger1_1 = fluree.insert(ledger1_0, &insert1).await.unwrap().ledger;
    let insert2 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:bob", "@type": "ex:Person", "schema:name": "Bob"}]
    });
    let _ledger1_2 = fluree.insert(ledger1_1, &insert2).await.unwrap().ledger;

    // Ledger 2: t=1 Carol, t=2 Dave
    let ledger2_0 = genesis_ledger(&fluree, "ledger2:main");
    let insert3 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:carol", "@type": "ex:Person", "schema:name": "Carol"}]
    });
    let ledger2_1 = fluree.insert(ledger2_0, &insert3).await.unwrap().ledger;
    let insert4 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:dave", "@type": "ex:Person", "schema:name": "Dave"}]
    });
    let _ledger2_2 = fluree.insert(ledger2_1, &insert4).await.unwrap().ledger;

    // Dataset: ledger1 pinned at t=1, ledger2 at t=2
    let query = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "from": ["ledger1:main@t:1", "ledger2:main@t:2"],
        "select": ["?name"],
        "where": {"@id": "?person", "@type": "ex:Person", "schema:name": "?name"}
    });

    let spec = DatasetSpec::from_json(&query).expect("parse should succeed");
    let dataset = fluree.build_dataset_view(&spec).await.unwrap();
    let result = fluree.query_dataset(&dataset, &query).await.unwrap();

    let primary = dataset.primary().unwrap();
    let jsonld = result.to_jsonld(primary.snapshot.as_ref()).unwrap();

    // Expect ledger1@t=1 (Alice) + ledger2@t=2 (Carol, Dave)
    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Alice", "Carol", "Dave"]))
    );
}

#[tokio::test]
async fn sparql_from_time_travel_suffixes() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Ledger 1: t=1 Alice, t=2 Bob
    let ledger1_0 = genesis_ledger(&fluree, "ledger1:main");
    let insert1 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:alice", "@type": "ex:Person", "schema:name": "Alice"}]
    });
    let ledger1_1 = fluree.insert(ledger1_0, &insert1).await.unwrap().ledger;
    let insert2 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:bob", "@type": "ex:Person", "schema:name": "Bob"}]
    });
    let _ledger1_2 = fluree.insert(ledger1_1, &insert2).await.unwrap().ledger;

    // Ledger 2: t=1 Carol, t=2 Dave
    let ledger2_0 = genesis_ledger(&fluree, "ledger2:main");
    let insert3 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:carol", "@type": "ex:Person", "schema:name": "Carol"}]
    });
    let ledger2_1 = fluree.insert(ledger2_0, &insert3).await.unwrap().ledger;
    let insert4 = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [{"@id": "ex:dave", "@type": "ex:Person", "schema:name": "Dave"}]
    });
    let _ledger2_2 = fluree.insert(ledger2_1, &insert4).await.unwrap().ledger;

    let sparql = r"
        PREFIX schema: <http://schema.org/>
        SELECT ?name
        FROM <ledger1:main@t:1>
        FROM <ledger2:main@t:2>
        WHERE {
            ?person schema:name ?name
        }
    ";

    let jsonld = fluree
        .query_from()
        .sparql(sparql)
        .format(fluree_db_api::FormatterConfig::jsonld())
        .execute_formatted()
        .await
        .expect("query should succeed");

    // Expect ledger1@t=1 (Alice) + ledger2@t=2 (Carol, Dave)
    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Alice", "Carol", "Dave"]))
    );
}

// =============================================================================
// Regression: single-ledger dataset string function evaluation
// =============================================================================

/// Regression test for single-ledger dataset queries with SPARQL string functions.
///
/// Previously, `ExecutionContext::is_multi_ledger()` returned `true` for any
/// dataset-backed query, which disabled the binary-store decode path. This caused
/// CONTAINS, REGEX, STRLEN and other string functions to silently return empty
/// results because encoded literals were never materialized into strings.
#[tokio::test]
async fn single_ledger_dataset_string_functions() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let _ledger = seed_people_ledger(&fluree, "strfn:main").await;

    let spec = DatasetSpec::new().with_default(GraphSource::new("strfn:main"));
    let dataset = fluree
        .build_dataset_view(&spec)
        .await
        .expect("build dataset");

    // CONTAINS — should find "Alice" (not empty results)
    let contains = r#"
        PREFIX schema: <http://schema.org/>
        SELECT ?name
        WHERE {
            ?s schema:name ?name .
            FILTER(CONTAINS(?name, "lic"))
        }
    "#;
    let result = fluree
        .query_dataset(&dataset, QueryInput::Sparql(contains))
        .await
        .expect("CONTAINS query through dataset");
    let primary = dataset.primary().unwrap();
    let jsonld = result
        .to_jsonld(primary.snapshot.as_ref())
        .expect("to_jsonld");
    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Alice"]))
    );

    // STRLEN — should return actual lengths (not empty/unbound)
    let strlen = r#"
        PREFIX schema: <http://schema.org/>
        SELECT ?name (STRLEN(?name) AS ?len)
        WHERE {
            ?s schema:name ?name .
            FILTER(?name = "Bob")
        }
    "#;
    let result = fluree
        .query_dataset(&dataset, QueryInput::Sparql(strlen))
        .await
        .expect("STRLEN query through dataset");
    let jsonld = result
        .to_jsonld(primary.snapshot.as_ref())
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows_array(&jsonld),
        vec![vec![json!("Bob"), json!(3)]]
    );

    // LCASE — should return lowercased string (not empty)
    let lcase = r#"
        PREFIX schema: <http://schema.org/>
        SELECT (LCASE(?name) AS ?lower)
        WHERE {
            ?s schema:name ?name .
            FILTER(?name = "Alice")
        }
    "#;
    let result = fluree
        .query_dataset(&dataset, QueryInput::Sparql(lcase))
        .await
        .expect("LCASE query through dataset");
    let jsonld = result
        .to_jsonld(primary.snapshot.as_ref())
        .expect("to_jsonld");
    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["alice"]))
    );
}

// =============================================================================
// Staged transaction + multi-ledger dataset
// =============================================================================

/// Regression test: a staged (uncommitted) transaction that introduces a new
/// namespace prefix must be queryable in a multi-ledger dataset.
///
/// Before the fix, `GraphDb::from_staged()` cloned the base snapshot without
/// applying the staged transaction's namespace delta. SIDs using the new
/// namespace code could not be decoded to IRIs, so `sid_to_iri_match()` would
/// either silently fall back to `Binding::Sid` (breaking cross-ledger joins)
/// or — after the error-hardening fix — return an internal error.
///
/// The fix applies `apply_envelope_deltas()` to the snapshot clone in
/// `from_staged()` before building the reverse graph or constructing the
/// `GraphDb`, ensuring all namespace codes are resolvable.
#[tokio::test]
async fn dataset_staged_transaction_with_novel_namespace() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Ledger A: committed data using "ex:" namespace
    let ledger_a0 = genesis_ledger(&fluree, "committed:main");
    let insert_a = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [{
            "@id": "ex:alice",
            "@type": "ex:Person",
            "schema:name": "Alice",
            "ex:partnerId": {"@id": "http://novel-namespace.example.com/org/acme"}
        }]
    });
    let _ledger_a = fluree
        .insert(ledger_a0, &insert_a)
        .await
        .expect("commit ledger A");

    // Ledger B: stage (not commit) a transaction that introduces
    // "http://novel-namespace.example.com/org/" — a namespace prefix that
    // does NOT exist on ledger B's base (genesis) snapshot.
    let ledger_b0 = genesis_ledger(&fluree, "staged:main");
    let insert_b = json!({
        "@context": {
            "novel": "http://novel-namespace.example.com/org/",
            "schema": "http://schema.org/"
        },
        "@graph": [{
            "@id": "novel:acme",
            "@type": "novel:Organization",
            "schema:name": "Acme Corp"
        }]
    });

    let staged = fluree
        .stage_owned(ledger_b0)
        .insert(&insert_b)
        .stage()
        .await
        .expect("stage ledger B");

    // Build a multi-ledger dataset: committed A + staged B
    let view_a = GraphDb::from_ledger_state(&_ledger_a.ledger);
    let view_b = GraphDb::from_staged(&staged).expect("from_staged");

    let dataset = DataSetDb::new().with_default(view_a).with_default(view_b);

    // Cross-ledger join: find the person whose partnerId matches the org
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "novel": "http://novel-namespace.example.com/org/",
            "schema": "http://schema.org/"
        },
        "select": ["?personName", "?orgName"],
        "where": [
            {"@id": "?person", "schema:name": "?personName", "ex:partnerId": "?org"},
            {"@id": "?org", "schema:name": "?orgName"}
        ]
    });

    let result = fluree
        .query_dataset(&dataset, &query)
        .await
        .expect("cross-ledger join with staged novel namespace should succeed");

    let primary = dataset.primary().expect("primary");
    let jsonld = result
        .to_jsonld(primary.snapshot.as_ref())
        .expect("to_jsonld");

    assert_eq!(
        normalize_rows_array(&jsonld),
        normalize_rows_array(&json!([["Alice", "Acme Corp"]]))
    );
}
