//! JSON-LD ↔ SPARQL parity for RDF list (rdf:first/rdf:rest/rdf:nil) patterns.
//!
//! PR-1 of the W3C burn-down teaches the SPARQL parser to desugar collection
//! patterns `( ... )` into `rdf:first`/`rdf:rest`/`rdf:nil` triples at parse
//! time (SPARQL 1.1 §4.2.4) — surface sugar over triples the shared engine
//! already executes. Per `docs/contributing/sparql-compliance.md`
//! § "Query Surface Parity", this guards the equivalence: a JSON-LD query
//! spelling out the same first/rest pattern must match exactly the data a
//! SPARQL `( ... )` pattern matches, through the same IR/engine path.
//!
//! Note on ingest: Fluree's JSON-LD `@list` (and Turtle object-position
//! collections) ingest as ordered `list_index` values, NOT as
//! rdf:first/rest triples — so these tests seed an explicit first/rest
//! chain, which both query surfaces then match identically.

use crate::support;
use crate::support::{genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

const RDF_NS: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

/// Seed an explicit rdf:first/rest chain (`ex:x ex:letters ("one" "two")`
/// in RDF list terms) plus an empty-list assertion
/// (`ex:e ex:letters rdf:nil`, i.e. `ex:e ex:letters ()`).
async fn seed_rdf_lists(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "rdf": RDF_NS
        },
        "@graph": [
            { "@id": "ex:x", "ex:letters": { "@id": "ex:cell1" } },
            { "@id": "ex:cell1", "rdf:first": "one", "rdf:rest": { "@id": "ex:cell2" } },
            { "@id": "ex:cell2", "rdf:first": "two", "rdf:rest": { "@id": "rdf:nil" } },
            { "@id": "ex:e", "ex:letters": { "@id": "rdf:nil" } }
        ]
    });

    let committed = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert rdf list chain");
    committed.ledger
}

/// A SPARQL collection pattern `( ?v ?w )` and a JSON-LD query spelling out
/// the equivalent rdf:first/rest/nil pattern must return identical rows.
#[tokio::test]
async fn jsonld_first_rest_pattern_matches_sparql_collection_pattern() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_rdf_lists(&fluree, "query/rdf-list-parity:chain").await;

    // SPARQL surface: parse-time desugaring of `( ?v ?w )`.
    let sparql = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?v ?w
        WHERE { ex:x ex:letters (?v ?w) . }
    ";
    let sparql_rows = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("sparql collection pattern query")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");

    // JSON-LD surface: the same first/rest pattern, spelled explicitly.
    let jsonld_query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "rdf": RDF_NS
        },
        "select": ["?v", "?w"],
        "where": [
            { "@id": "ex:x", "ex:letters": { "@id": "?cell1" } },
            { "@id": "?cell1", "rdf:first": "?v", "rdf:rest": { "@id": "?cell2" } },
            { "@id": "?cell2", "rdf:first": "?w", "rdf:rest": { "@id": "rdf:nil" } }
        ]
    });
    let jsonld_rows = support::query_jsonld(&fluree, &ledger, &jsonld_query)
        .await
        .expect("jsonld first/rest pattern query")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");

    assert_eq!(
        normalize_rows(&sparql_rows),
        normalize_rows(&json!([["one", "two"]])),
        "SPARQL collection pattern should walk the first/rest chain"
    );
    assert_eq!(
        normalize_rows(&sparql_rows),
        normalize_rows(&jsonld_rows),
        "JSON-LD first/rest pattern must match exactly what the SPARQL ( ... ) pattern matches"
    );
}

/// The empty collection `()` is the constant `rdf:nil` on both surfaces.
#[tokio::test]
async fn jsonld_rdf_nil_matches_sparql_empty_collection() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_rdf_lists(&fluree, "query/rdf-list-parity:nil").await;

    // SPARQL surface: `()` lowers to the IRI rdf:nil.
    let sparql = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?s
        WHERE { ?s ex:letters () . }
    ";
    let sparql_rows = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("sparql empty collection query")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");

    // JSON-LD surface: match the rdf:nil IRI object directly.
    let jsonld_query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "rdf": RDF_NS
        },
        "select": ["?s"],
        "where": [
            { "@id": "?s", "ex:letters": { "@id": "rdf:nil" } }
        ]
    });
    let jsonld_rows = support::query_jsonld(&fluree, &ledger, &jsonld_query)
        .await
        .expect("jsonld rdf:nil query")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");

    assert_eq!(
        normalize_rows(&sparql_rows),
        normalize_rows(&json!([["ex:e"]])),
        "SPARQL () should match the rdf:nil-valued subject only"
    );
    assert_eq!(
        normalize_rows(&sparql_rows),
        normalize_rows(&jsonld_rows),
        "JSON-LD rdf:nil object pattern must match the SPARQL () pattern"
    );
}
