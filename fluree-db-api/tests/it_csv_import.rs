//! CSV bulk import (neo4j-admin header convention → JSON-LD front-end) — end to
//! end. Proves a single CSV-loaded dataset is queryable from BOTH Cypher and
//! SPARQL, including edge properties carried as `@annotation` (RDF 1.2 / LPG).

mod support;

use fluree_db_api::csv_import::{csv_files_to_jsonld, CsvImportOptions, EdgePolicy};
use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{genesis_ledger, graphdb_from_ledger};

/// Base namespace = the Cypher default `@vocab`, so bare Cypher labels /
/// predicates (`Person`, `KNOWS`, `name`) resolve to the minted IRIs.
fn opts() -> CsvImportOptions {
    CsvImportOptions {
        base_iri: "http://example.org/".to_string(),
        ..Default::default()
    }
}

// person nodes; knows edges carry a creationDate property → annotation.
const PERSONS: &str = "id:ID(Person),name:string,:LABEL\n\
    10,Alice,Person\n\
    20,Bob,Person\n\
    30,Carol,Person\n";
const KNOWS: &str = ":START_ID(Person),:END_ID(Person),:TYPE,creationDate:long\n\
    10,20,KNOWS,1577934245\n\
    20,30,KNOWS,1580000000\n";

#[tokio::test]
async fn csv_import_round_trips_to_cypher_and_sparql() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/csv:round-trip");
    let doc = csv_files_to_jsonld(&[PERSONS, KNOWS], &opts()).expect("csv → jsonld");
    let l = fluree
        .insert(ledger0, &doc)
        .await
        .expect("import csv-derived jsonld")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // Cypher, plain (set semantics) — sees the base edges.
    let plain = fluree
        .query_cypher(
            &db,
            r"MATCH (a:Person)-[:KNOWS]->(b:Person)
               RETURN a.name AS from, b.name AS to ORDER BY from",
        )
        .await
        .expect("cypher plain")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        plain,
        json!([["Alice", "Bob"], ["Bob", "Carol"]]),
        "{plain}"
    );

    // Cypher, rel-var — reads the edge property from the annotation.
    let weighted = fluree
        .query_cypher(
            &db,
            r"MATCH (a:Person)-[r:KNOWS]->(b:Person)
               RETURN a.name AS from, b.name AS to, r.creationDate AS since
               ORDER BY from",
        )
        .await
        .expect("cypher rel-var")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        weighted,
        json!([["Alice", "Bob", 1_577_934_245], ["Bob", "Carol", 1_580_000_000]]),
        "edge property read via annotation: {weighted}"
    );

    // SPARQL reads the SAME edge property via the 1.2 annotation tail `{| |}`.
    let sparql = r"
        PREFIX ex: <http://example.org/>
        SELECT ?since WHERE {
          ex:Person/10 ex:KNOWS ex:Person/20 {| ex:creationDate ?since |} .
        }
    ";
    let res = support::query_sparql(&fluree, &l, sparql)
        .await
        .expect("sparql annotation");
    let rows = res.to_sparql_json(&l.snapshot).expect("sparql json");
    let bindings = rows["results"]["bindings"].as_array().expect("bindings");
    assert_eq!(bindings.len(), 1, "one annotated edge: {bindings:#?}");
    assert_eq!(
        bindings[0]["since"]["value"].as_str(),
        Some("1577934245"),
        "SPARQL reads the edge property: {bindings:#?}"
    );
}

#[tokio::test]
async fn csv_import_plain_policy_yields_pure_rdf_edges() {
    // Under EdgePolicy::Plain the knows edge is a plain triple — visible to
    // Cypher set-semantics and SPARQL as an ordinary triple, with no annotation.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/csv:plain");
    let doc = csv_files_to_jsonld(
        &[PERSONS, KNOWS],
        &CsvImportOptions {
            edge_policy: EdgePolicy::Plain,
            ..opts()
        },
    )
    .expect("csv → jsonld");
    let l = fluree.insert(ledger0, &doc).await.expect("import").ledger;
    let db = graphdb_from_ledger(&l);

    // Base edges present.
    let plain = fluree
        .query_cypher(
            &db,
            r"MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS f, b.name AS t ORDER BY f",
        )
        .await
        .expect("cypher plain")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        plain,
        json!([["Alice", "Bob"], ["Bob", "Carol"]]),
        "{plain}"
    );

    // No reifier bundle → SPARQL annotation tail finds nothing.
    let sparql = r"
        PREFIX ex: <http://example.org/>
        SELECT ?since WHERE {
          ex:Person/10 ex:KNOWS ex:Person/20 {| ex:creationDate ?since |} .
        }
    ";
    let res = support::query_sparql(&fluree, &l, sparql)
        .await
        .expect("sparql");
    let rows = res.to_sparql_json(&l.snapshot).expect("sparql json");
    assert_eq!(
        rows["results"]["bindings"].as_array().map(Vec::len),
        Some(0),
        "plain policy keeps edges property-free: {rows}"
    );
}
