//! Turtle empty-collection (`()`) ingest — issue #1694, the silent-loss half.
//!
//! An object-position `()` in Turtle denotes the IRI `rdf:nil`
//! (`ex:s2 ex:items ()` IS `ex:s2 ex:items rdf:nil`). The default
//! `CollectionStyle::IndexedItems` parser used to consume the statement and
//! emit NOTHING — the subject never reached the database and no diagnostic
//! fired. These tests pin the fix end-to-end through the public transact
//! surface: `()` now stores the `rdf:nil` triple, identically to the same
//! statement written with an explicit `rdf:nil`, while non-empty collections
//! keep their `list_index` storage shape (the D-13 spine decision is
//! deliberately NOT taken here — `?l rdf:first ?x` still matches nothing).

use crate::support;
use crate::support::normalize_rows;
use fluree_db_api::FlureeBuilder;

const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";

/// The issue's repro document: as RDF it denotes eight triples — three list
/// members under `ex:s` (stored as indexed list items) and
/// `ex:s2 ex:items rdf:nil`.
const REPRO_TTL: &str = r"@prefix ex: <http://example.org/> .
ex:s  ex:items ( ex:a ex:b ex:c ) .
ex:s2 ex:items () .
";

/// The same statements with the empty collection written as the IRI it
/// denotes. `()` must lower to exactly this.
const CONTROL_TTL: &str = r"@prefix ex: <http://example.org/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
ex:s  ex:items ( ex:a ex:b ex:c ) .
ex:s2 ex:items rdf:nil .
";

async fn seed_turtle(
    fluree: &support::MemoryFluree,
    ledger_id: &str,
    ttl: &str,
) -> support::MemoryLedger {
    let ledger0 = fluree.create_ledger(ledger_id).await.expect("genesis");
    fluree
        .insert_turtle(ledger0, ttl)
        .await
        .expect("insert turtle")
        .ledger
}

async fn rows(
    fluree: &support::MemoryFluree,
    ledger: &support::MemoryLedger,
    sparql: &str,
) -> Vec<serde_json::Value> {
    let out = support::query_sparql(fluree, ledger, sparql)
        .await
        .expect("sparql query")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    normalize_rows(&out)
}

/// Issue #1694 repro: `ex:s2 ex:items ()` must store `ex:s2 ex:items rdf:nil`
/// rather than vanishing.
#[tokio::test]
async fn empty_collection_object_stores_rdf_nil() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_turtle(&fluree, "tx/turtle-empty-coll:main", REPRO_TTL).await;

    // The subject exists and its object is rdf:nil.
    let nil_rows = rows(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/>
          SELECT ?o WHERE { ex:s2 ex:items ?o }",
    )
    .await;
    assert_eq!(nil_rows.len(), 1, "ex:s2 must exist: {nil_rows:?}");
    let obj = nil_rows[0]
        .as_array()
        .and_then(|r| r.first())
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("expected an IRI object row, got {nil_rows:?}"));
    assert!(
        obj == RDF_NIL || obj == "rdf:nil",
        "object must be rdf:nil, got {obj}"
    );

    // And it is reachable through the SPARQL `()` pattern (which lowers to
    // the rdf:nil constant — the W3C basic#list-1 shape).
    let by_nil_pattern = rows(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/>
          SELECT ?s WHERE { ?s ex:items () }",
    )
    .await;
    assert_eq!(
        by_nil_pattern.len(),
        1,
        "`?s ex:items ()` must find ex:s2: {by_nil_pattern:?}"
    );

    // The whole graph: 3 indexed list items + the nil edge. Before the fix
    // this document committed 3 flakes and ex:s2 did not exist at all.
    let all = rows(&fluree, &ledger, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }").await;
    assert_eq!(all.len(), 4, "expected 4 stored statements: {all:?}");
}

/// `()` and a literally-written `rdf:nil` must produce identical graphs —
/// the empty collection is only surface syntax for that IRI.
#[tokio::test]
async fn empty_collection_matches_literal_rdf_nil() {
    let fluree = FlureeBuilder::memory().build_memory();
    let from_sugar = seed_turtle(&fluree, "tx/turtle-empty-coll:sugar", REPRO_TTL).await;
    let from_iri = seed_turtle(&fluree, "tx/turtle-empty-coll:iri", CONTROL_TTL).await;

    let q = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }";
    assert_eq!(
        rows(&fluree, &from_sugar, q).await,
        rows(&fluree, &from_iri, q).await,
        "`()` and explicit rdf:nil must store identically"
    );
}

/// Non-empty collections keep the `list_index` storage shape: members are
/// direct objects of the enclosing predicate and NO rdf:first/rest spine is
/// materialized. This is the D-13 decision left exactly where it was.
#[tokio::test]
async fn non_empty_collections_unchanged_no_spine() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_turtle(&fluree, "tx/turtle-empty-coll:nospine", REPRO_TTL).await;

    let items = rows(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/>
          SELECT ?o WHERE { ex:s ex:items ?o }",
    )
    .await;
    assert_eq!(
        items.len(),
        3,
        "list members stay direct objects: {items:?}"
    );

    let spine = rows(
        &fluree,
        &ledger,
        r"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
          SELECT ?l ?x WHERE { ?l rdf:first ?x }",
    )
    .await;
    assert!(spine.is_empty(), "no spine must be materialized: {spine:?}");
}

/// `()` in SUBJECT position (`() ex:p ex:o .`) already parsed as the IRI
/// `rdf:nil` before this fix — pinned here so the two positions stay
/// consistent.
#[tokio::test]
async fn empty_collection_subject_is_rdf_nil() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_turtle(
        &fluree,
        "tx/turtle-empty-coll:subject",
        r"@prefix ex: <http://example.org/> .
          () ex:p ex:o .
",
    )
    .await;

    let found = rows(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/>
          SELECT ?o WHERE { () ex:p ?o }",
    )
    .await;
    assert_eq!(found.len(), 1, "rdf:nil subject must be stored: {found:?}");
}
