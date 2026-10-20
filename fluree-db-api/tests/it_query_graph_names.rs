//! A JSON-LD `["graph", <name>, …]` clause names its graph the way SPARQL's
//! `GRAPH` does: a compact IRI whose prefix the query's `@context` defines is
//! expanded. Names that are not IRIs (dataset aliases, ledger names) are kept
//! as written, and so is a name the query's own `fromNamed` spells exactly.

use crate::support::{genesis_ledger, normalize_rows, query_jsonld, query_sparql, MemoryLedger};
use fluree_db_api::{DatasetSpec, FlureeBuilder};
use serde_json::{json, Value};

fn ctx() -> Value {
    json!({"ex": "http://example.org/ns/", "schema": "http://schema.org/"})
}

async fn ledger(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> MemoryLedger {
    fluree
        .stage_owned(genesis_ledger(fluree, ledger_id))
        .upsert_turtle(
            "@prefix ex: <http://example.org/ns/> .\n\
             @prefix schema: <http://schema.org/> .\n\
             ex:alice schema:name \"Alice\" .\n\
             GRAPH ex:staff { ex:bob schema:name \"Bob\" . }\n\
             GRAPH <urn:g2> { ex:carol schema:name \"Carol\" . }\n",
        )
        .execute()
        .await
        .expect("seed")
        .ledger
}

fn names_in(graph: &str) -> Value {
    json!({
        "@context": ctx(),
        "select": "?name",
        "where": [["graph", graph, {"@id": "?s", "schema:name": "?name"}]]
    })
}

async fn names(fluree: &fluree_db_api::Fluree, ledger: &MemoryLedger, query: &Value) -> Vec<Value> {
    let rows = query_jsonld(fluree, ledger, query)
        .await
        .expect("query")
        .to_jsonld(&ledger.snapshot)
        .expect("format");
    normalize_rows(&rows)
}

#[tokio::test]
async fn a_compact_graph_name_expands_like_sparql() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger(&fluree, "graph-names/compact:main").await;

    assert_eq!(
        names(&fluree, &ledger, &names_in("ex:staff")).await,
        vec![json!("Bob")]
    );
    assert_eq!(
        names(&fluree, &ledger, &names_in("http://example.org/ns/staff")).await,
        vec![json!("Bob")]
    );
    // A name whose prefix the context does not define is not an expansion.
    assert_eq!(
        names(&fluree, &ledger, &names_in("urn:g2")).await,
        vec![json!("Carol")]
    );

    let sparql = query_sparql(
        &fluree,
        &ledger,
        "PREFIX ex: <http://example.org/ns/> PREFIX schema: <http://schema.org/> \
         SELECT ?name WHERE { GRAPH ex:staff { ?s schema:name ?name } }",
    )
    .await
    .expect("sparql")
    .to_jsonld(&ledger.snapshot)
    .expect("format");
    assert_eq!(normalize_rows(&sparql), vec![json!(["Bob"])]);
}

/// A `fromNamed` alias may look like a compact IRI. The clause naming it
/// means the alias, not the IRI its spelling would expand to.
#[tokio::test]
async fn a_dataset_alias_spelled_like_a_compact_iri_is_the_alias() {
    let fluree = FlureeBuilder::memory().build_memory();
    let _ledger = ledger(&fluree, "graph-names/alias:main").await;
    let mut query = names_in("ex:people");
    query["fromNamed"] =
        json!({"ex:people": {"@id": "graph-names/alias:main", "@graph": "urn:g2"}});
    query["from"] = json!("graph-names/alias:main");

    let spec = DatasetSpec::from_json(&query).expect("dataset spec");
    let dataset = fluree.build_dataset_view(&spec).await.expect("dataset");
    let result = fluree.query_dataset(&dataset, &query).await.expect("query");
    let rows = result
        .to_jsonld(dataset.primary().unwrap().snapshot.as_ref())
        .expect("format");
    assert_eq!(normalize_rows(&rows), vec![json!("Carol")]);
}

/// A dataset built outside the query body protects its aliases too: the
/// query names `ex:people` without a `fromNamed` of its own, and still means
/// the alias the dataset registered.
#[tokio::test]
async fn an_alias_from_a_dataset_built_outside_the_query_is_the_alias() {
    let fluree = FlureeBuilder::memory().build_memory();
    let _ledger = ledger(&fluree, "graph-names/outside-alias:main").await;
    let spec = DatasetSpec::from_json(&json!({
        "from": "graph-names/outside-alias:main",
        "fromNamed": {"ex:people": {"@id": "graph-names/outside-alias:main", "@graph": "urn:g2"}}
    }))
    .expect("dataset spec");
    let dataset = fluree.build_dataset_view(&spec).await.expect("dataset");
    let result = fluree
        .query_dataset(&dataset, &names_in("ex:people"))
        .await
        .expect("query");
    let rows = result
        .to_jsonld(dataset.primary().unwrap().snapshot.as_ref())
        .expect("format");
    assert_eq!(normalize_rows(&rows), vec![json!("Carol")]);
}

/// A rule body reads a named graph by its compact name, too.
#[tokio::test]
async fn a_rule_body_names_its_graph_compactly() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger(&fluree, "graph-names/rule:main").await;
    let query = json!({
        "@context": ctx(),
        "select": "?s",
        "where": {"@id": "?s", "@type": "ex:Staff"},
        "reasoning": "datalog",
        "rules": [{
            "@context": ctx(),
            "where": [["graph", "ex:staff", {"@id": "?s", "schema:name": "?name"}]],
            "insert": {"@id": "?s", "@type": "ex:Staff"}
        }]
    });
    assert_eq!(names(&fluree, &ledger, &query).await, vec![json!("ex:bob")]);
}

/// An update's `where` names its graph the same way, so an update that reads
/// and writes one graph by its compact name copies the value instead of
/// matching nothing.
#[tokio::test]
async fn an_update_where_clause_names_its_graph_compactly() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger(&fluree, "graph-names/update:main").await;
    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": [["graph", "ex:staff", {"@id": "ex:bob", "schema:name": "?n"}]],
                "insert": [["graph", "ex:staff", {"@id": "ex:bob", "ex:copied": "?n"}]]
            }),
        )
        .await
        .expect("update")
        .ledger;
    let copied = json!({
        "@context": ctx(),
        "select": "?n",
        "where": [["graph", "http://example.org/ns/staff", {"@id": "ex:bob", "ex:copied": "?n"}]]
    });
    assert_eq!(names(&fluree, &ledger, &copied).await, vec![json!("Bob")]);
}
