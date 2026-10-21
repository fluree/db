//! SPARQL's view of RDF lists.
//!
//! A list stored compactly is, as RDF, its owner edge `s p head` and a chain
//! of `rdf:first`/`rdf:rest` cells; SPARQL sees that graph, so a member is not
//! a value of its predicate. JSON-LD queries keep the member view.

use crate::support;
use fluree_db_api::{FlureeBuilder, LedgerState};
use serde_json::{json, Value as JsonValue};

const PREFIXES: &str = "PREFIX ex: <http://example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
";

async fn ledger_with(fluree: &fluree_db_api::Fluree, name: &str, ttl: &str) -> LedgerState {
    let ledger = support::genesis_ledger(fluree, &format!("it/sparql-lists/{name}"));
    fluree
        .insert_turtle(
            ledger,
            &format!("@prefix ex: <http://example.org/> .\n{ttl}"),
        )
        .await
        .unwrap_or_else(|e| panic!("insert: {e}"))
        .ledger
}

async fn rows(fluree: &fluree_db_api::Fluree, ledger: &LedgerState, body: &str) -> Vec<JsonValue> {
    let result = support::query_sparql(fluree, ledger, &format!("{PREFIXES}{body}"))
        .await
        .unwrap_or_else(|e| panic!("query: {e}\n{body}"))
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    support::normalize_rows(&result)
}

fn is_blank(row: &JsonValue) -> bool {
    row[0].as_str().is_some_and(|s| s.starts_with("_:"))
}

#[tokio::test]
async fn a_list_is_its_head_and_cells() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(&fluree, "head", "ex:s ex:items (1 2 3) ; ex:items 9 .").await;

    let values = rows(&fluree, &ledger, "SELECT ?o WHERE { ex:s ex:items ?o }").await;
    assert_eq!(values.len(), 2, "the head and the plain value: {values:?}");
    assert!(values.contains(&json!([9])), "{values:?}");
    assert!(values.iter().any(is_blank), "{values:?}");

    let first = rows(
        &fluree,
        &ledger,
        "SELECT ?x WHERE { ex:s ex:items ?l . ?l rdf:first ?x }",
    )
    .await;
    assert_eq!(first, [json!([1])]);

    let spelled = rows(
        &fluree,
        &ledger,
        "SELECT ?x ?y ?z WHERE { ex:s ex:items (?x ?y ?z) }",
    )
    .await;
    assert_eq!(spelled, [json!([1, 2, 3])]);

    let all = rows(&fluree, &ledger, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }").await;
    assert_eq!(
        all.len(),
        1 + 1 + 3 * 2,
        "plain value, owner edge, cells: {all:?}"
    );

    let count = rows(
        &fluree,
        &ledger,
        "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }",
    )
    .await;
    assert_eq!(count, [json!([8])]);
}

#[tokio::test]
async fn jsonld_queries_keep_the_members() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(&fluree, "jsonld", "ex:s ex:items (1 2 3) .").await;
    let result = support::query_jsonld(
        &fluree,
        &ledger,
        &json!({
            "@context": {"ex": "http://example.org/"},
            "select": "?o",
            "where": {"@id": "ex:s", "ex:items": "?o"}
        }),
    )
    .await
    .expect("query")
    .to_jsonld(&ledger.snapshot)
    .expect("to_jsonld");
    assert_eq!(
        support::normalize_rows(&result),
        [json!(1), json!(2), json!(3)]
    );
}

/// The RDF idioms for a list's members: `rdf:rest*/rdf:first` reaches every
/// member, in either direction, and a fixed path reaches one position.
#[tokio::test]
async fn property_paths_walk_a_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(&fluree, "paths", "ex:s ex:items (ex:a ex:b ex:c) .").await;
    let members = rows(
        &fluree,
        &ledger,
        "SELECT ?x WHERE { ex:s ex:items/rdf:rest*/rdf:first ?x }",
    )
    .await;
    assert_eq!(members, [json!(["ex:a"]), json!(["ex:b"]), json!(["ex:c"])]);
    let second = rows(
        &fluree,
        &ledger,
        "SELECT ?x WHERE { ex:s ex:items/rdf:rest/rdf:first ?x }",
    )
    .await;
    assert_eq!(second, [json!(["ex:b"])]);
    let owner = rows(
        &fluree,
        &ledger,
        "SELECT ?s WHERE { ?s ex:items/rdf:rest*/rdf:first ex:c }",
    )
    .await;
    assert_eq!(owner, [json!(["ex:s"])]);
    let cells = rows(
        &fluree,
        &ledger,
        "SELECT (COUNT(*) AS ?n) WHERE { ?a rdf:rest+ ?b }",
    )
    .await;
    assert_eq!(
        cells,
        [json!([3 + 2 + 1])],
        "every cell to each later cell and rdf:nil"
    );
}

/// CONSTRUCT writes the list it reads: `( … )` in Turtle, `@list` in JSON-LD.
#[tokio::test]
async fn construct_writes_a_list_as_a_collection() {
    use fluree_db_api::format::{format_results_string, FormatterConfig};

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(&fluree, "construct", "ex:s ex:items (1 2 3) .").await;
    let query = format!("{PREFIXES}CONSTRUCT {{ ?s ?p ?o }} WHERE {{ ?s ?p ?o }}");
    let result = support::query_sparql(&fluree, &ledger, &query)
        .await
        .expect("construct");
    let ttl = format_results_string(
        &result,
        &result.context,
        &ledger.snapshot,
        &FormatterConfig::turtle(),
    )
    .expect("turtle");
    assert!(ttl.contains("ex:s ex:items ( 1 2 3 ) ."), "{ttl}");
    let graph = result.to_construct(&ledger.snapshot).expect("jsonld");
    assert_eq!(
        graph["@graph"],
        json!([{"@id": "ex:s", "ex:items": {"@list": [1, 2, 3]}}]),
        "{graph}"
    );
}

/// The fuel of `rdf:rest*/rdf:first` grows with the list, not with its
/// square or cube: the walk starts at the owner, though statistics count no
/// `rdf:first` for a compact list, and reads the list once.
#[tokio::test]
async fn walking_a_list_is_linear_in_its_length() {
    async fn fuel(n: usize) -> f64 {
        let fluree = FlureeBuilder::memory().build_memory();
        let members: Vec<String> = (0..n).map(|i| i.to_string()).collect();
        let ttl = format!("ex:s ex:items ({}) .", members.join(" "));
        let ledger = ledger_with(&fluree, &format!("walk-{n}"), &ttl).await;
        let query = format!(
            "{PREFIXES}SELECT (COUNT(*) AS ?n) WHERE {{ ex:s ex:items/rdf:rest*/rdf:first ?x }}"
        );
        let tracked = support::graphdb_from_ledger(&ledger)
            .query(&fluree)
            .sparql(&query)
            .execute_tracked()
            .await
            .expect("walk");
        tracked.fuel.expect("tracked query reports fuel")
    }
    let (small, large) = (fuel(100).await, fuel(400).await);
    assert!(
        large < small * 6.0,
        "4x the members cost {:.1}x the fuel ({small} -> {large})",
        large / small
    );
}

/// Explain plans what execution runs: the list view included.
#[tokio::test]
async fn explain_shows_the_list_view() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(&fluree, "explain", "ex:s ex:items (1 2) .").await;
    let view = support::graphdb_from_ledger(&ledger);
    let query = format!("{PREFIXES}SELECT ?x WHERE {{ ex:s ex:items ?l . ?l rdf:first ?x }}");
    let plan = fluree.explain_sparql(&view, &query).await.expect("explain");
    let physical = plan["plan"]["physical"].to_string();
    assert_eq!(
        physical.matches("ListViewOperator").count(),
        2,
        "{physical}"
    );
}

/// DESCRIBE writes a described node's list whole, and a blank node it
/// points to with its own triples.
#[tokio::test]
async fn describe_carries_the_list_cells() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(
        &fluree,
        "describe",
        "ex:s ex:items (1 2) ; ex:addr [ ex:city \"Oslo\" ] ; ex:knows ex:t .\nex:t ex:name \"T\" .",
    )
    .await;
    let result = support::query_sparql(&fluree, &ledger, &format!("{PREFIXES}DESCRIBE ex:s"))
        .await
        .expect("describe");
    let graph = result.to_construct(&ledger.snapshot).expect("jsonld");
    let nodes = graph["@graph"].as_array().expect("nodes");
    let s = nodes.iter().find(|n| n["@id"] == "ex:s").expect("ex:s");
    assert_eq!(s["ex:items"], json!({"@list": [1, 2]}), "{graph}");
    let addr = s["ex:addr"][0]["@id"].as_str().expect("blank addr");
    let blank = nodes.iter().find(|n| n["@id"] == addr).expect("addr node");
    assert_eq!(blank["ex:city"], json!(["Oslo"]), "{graph}");
    assert!(
        nodes.iter().all(|n| n["@id"] != "ex:t"),
        "an IRI object is not described: {graph}"
    );
}

/// The cells are nodes of the graph for every path shape: a zero-length
/// match pairs each with itself, and a composite step walks them.
#[tokio::test]
async fn every_path_shape_reaches_the_cells() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(&fluree, "path-shapes", "ex:s ex:items (1 2 3) .").await;
    for (query, n) in [
        // 6 rest+ pairs; every subject and object of the graph with itself:
        // ex:s, three cells, rdf:nil and the three members.
        ("SELECT ?a ?b WHERE { ?a rdf:rest* ?b }", 6 + 8),
        ("SELECT ?a ?b WHERE { ?a (rdf:rest/rdf:rest)+ ?b }", 2),
        ("SELECT ?a ?b WHERE { ?a (rdf:rest/rdf:rest)* ?b }", 2 + 8),
        (
            "SELECT ?b WHERE { ex:s ex:items ?h . ?h (rdf:rest/rdf:rest)+ ?b }",
            1,
        ),
        ("SELECT ?a WHERE { ?a (rdf:rest/rdf:rest)+ rdf:nil }", 1),
        (
            "SELECT ?x WHERE { ex:s ex:items/rdf:rest?/rdf:first ?x }",
            2,
        ),
    ] {
        let found = rows(&fluree, &ledger, query).await;
        assert_eq!(found.len(), n, "{query}: {found:?}");
    }
}

// ---------------------------------------------------------------------------
// SPARQL UPDATE writes lists as RDF too.
// ---------------------------------------------------------------------------

async fn update(fluree: &fluree_db_api::Fluree, ledger: LedgerState, body: &str) -> LedgerState {
    let text = format!("{PREFIXES}{body}");
    let parsed = fluree_db_sparql::parse_sparql(&text);
    assert!(!parsed.has_errors(), "{text}: {:?}", parsed.diagnostics);
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::lower_sparql_update_ast(
        &parsed.ast.expect("ast"),
        &mut ns,
        fluree_db_transact::TxnOpts::default(),
    )
    .expect("lower");
    fluree
        .stage_owned(ledger)
        .txn(txn)
        .execute()
        .await
        .unwrap_or_else(|e| panic!("update: {e}\n{body}"))
        .ledger
}

/// `ex:s ex:items`'s values in Fluree's view: a compact list's members, and
/// a list stored as triples as its head.
async fn values(fluree: &fluree_db_api::Fluree, ledger: &LedgerState, s: &str) -> Vec<JsonValue> {
    let result = support::query_jsonld(
        fluree,
        ledger,
        &json!({
            "@context": {"ex": "http://example.org/"},
            "select": "?x",
            "where": {"@id": s, "ex:items": "?x"}
        }),
    )
    .await
    .expect("query")
    .to_jsonld(&ledger.snapshot)
    .expect("to_jsonld");
    result.as_array().cloned().unwrap_or_default()
}

async fn lists(ledger: &LedgerState, s: &str) -> Vec<Vec<i64>> {
    support::stored_int_lists(
        ledger,
        &format!("http://example.org/{s}"),
        "http://example.org/items",
    )
    .await
}

#[tokio::test]
async fn an_update_edits_a_member_in_place() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(&fluree, "edit", "ex:s ex:items (1 2 3) .").await;
    let ledger = update(
        &fluree,
        ledger,
        "DELETE { ?c rdf:first 2 } INSERT { ?c rdf:first 20 } \
         WHERE { ex:s ex:items/rdf:rest ?c . ?c rdf:first 2 }",
    )
    .await;
    assert_eq!(lists(&ledger, "s").await, [vec![1, 20, 3]]);
    let stored = values(&fluree, &ledger, "ex:s").await;
    assert!(
        stored.len() == 3 && stored.iter().all(JsonValue::is_number),
        "compact: {stored:?}"
    );
}

#[tokio::test]
async fn an_update_appends_to_a_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(&fluree, "append", "ex:s ex:items (1 2 3) .").await;
    let ledger = update(
        &fluree,
        ledger,
        "DELETE { ?last rdf:rest rdf:nil } INSERT { ?last rdf:rest ( 4 ) } \
         WHERE { ex:s ex:items/rdf:rest* ?last . ?last rdf:rest rdf:nil }",
    )
    .await;
    assert_eq!(lists(&ledger, "s").await, [vec![1, 2, 3, 4]]);
    assert_eq!(values(&fluree, &ledger, "ex:s").await.len(), 4, "compact");
}

#[tokio::test]
async fn deleting_a_list_deletes_its_cells() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(&fluree, "delete", "ex:s ex:items (1 2 3) ; ex:name \"s\" .").await;
    let ledger = update(
        &fluree,
        ledger,
        "DELETE { ex:s ex:items ?l . ?c ?p ?o } \
         WHERE { ex:s ex:items ?l . ?l rdf:rest* ?c . ?c ?p ?o }",
    )
    .await;
    let all = rows(&fluree, &ledger, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }").await;
    assert_eq!(all.len(), 1, "only ex:name is left: {all:?}");
}

/// A list whose owner's edge a write deletes, naming its cells in no
/// insertion, goes with the edge: its cells are unreachable, and deleting a
/// subject stays a delete.
#[tokio::test]
async fn deleting_the_owners_edge_deletes_the_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(&fluree, "orphan", "ex:s ex:items (1 2 3) .").await;
    let ledger = update(&fluree, ledger, "DELETE WHERE { ex:s ?p ?o }").await;
    let all = rows(&fluree, &ledger, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }").await;
    assert!(all.is_empty(), "{all:?}");
}

/// The edge moved, the cells are named again: the list moves, still compact.
#[tokio::test]
async fn moving_a_list_keeps_it_compact() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(&fluree, "move", "ex:s ex:items (1 2 3) .").await;
    let ledger = update(
        &fluree,
        ledger,
        "DELETE { ex:s ex:items ?l } INSERT { ex:t ex:items ?l } WHERE { ex:s ex:items ?l }",
    )
    .await;
    assert!(lists(&ledger, "s").await.is_empty());
    assert_eq!(lists(&ledger, "t").await, [vec![1, 2, 3]]);
    assert_eq!(values(&fluree, &ledger, "ex:t").await.len(), 3, "compact");
}

/// N-Triples has no `( … )`: a list in it is a chain of the document's blank
/// nodes, stored as the list it is, and upserted again as no change.
#[tokio::test]
async fn a_chain_written_out_is_stored_as_a_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let chain = "<http://example.org/s> <http://example.org/items> _:a .
        _:a <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> \"1\"^^<http://www.w3.org/2001/XMLSchema#integer> .
        _:a <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:b .
        _:b <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> \"2\"^^<http://www.w3.org/2001/XMLSchema#integer> .
        _:b <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
        ";
    let ledger = support::genesis_ledger(&fluree, "it/sparql-lists/chain");
    let ledger = fluree
        .insert_turtle(ledger, chain)
        .await
        .expect("insert")
        .ledger;
    assert_eq!(
        values(&fluree, &ledger, "ex:s").await,
        [json!(1), json!(2)],
        "compact"
    );
    let t = ledger.t();
    let ledger = fluree
        .upsert_turtle(ledger, chain)
        .await
        .expect("upsert")
        .ledger;
    assert_eq!(
        ledger.t(),
        t,
        "the same list upserted again changes nothing"
    );
    let ledger = update(
        &fluree,
        ledger,
        "INSERT DATA { ex:u ex:items _:c . _:c rdf:first 3 ; rdf:rest rdf:nil }",
    )
    .await;
    assert_eq!(
        values(&fluree, &ledger, "ex:u").await,
        [json!(3)],
        "compact"
    );
}

/// A `( … )` an update writes is stored compactly when it is the first list
/// on its property, and as triples beside one.
#[tokio::test]
async fn insert_data_writes_a_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(&fluree, "insert", "ex:a ex:name \"a\" .").await;
    let ledger = update(&fluree, ledger, "INSERT DATA { ex:t ex:items (1 2) }").await;
    assert_eq!(
        values(&fluree, &ledger, "ex:t").await,
        [json!(1), json!(2)],
        "compact"
    );
    let ledger = update(&fluree, ledger, "INSERT DATA { ex:t ex:items (3 4) }").await;
    assert_eq!(lists(&ledger, "t").await, [vec![1, 2], vec![3, 4]]);
    let all = rows(&fluree, &ledger, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }").await;
    assert_eq!(all.len(), 1 + 2 * (1 + 2 * 2), "{all:?}");
}

/// `ex:s ex:items 2` is no triple of the list, so a write of it is a write
/// of a plain value beside it.
#[tokio::test]
async fn a_plain_value_write_leaves_the_members() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(&fluree, "plain", "ex:s ex:items (1 2 3) .").await;
    let ledger = update(&fluree, ledger, "DELETE DATA { ex:s ex:items 2 }").await;
    assert_eq!(lists(&ledger, "s").await, [vec![1, 2, 3]]);
    let ledger = update(&fluree, ledger, "INSERT DATA { ex:s ex:items 2 }").await;
    let found = rows(&fluree, &ledger, "SELECT ?o WHERE { ex:s ex:items ?o }").await;
    assert_eq!(found.len(), 2, "the head and the value: {found:?}");
    assert!(found.contains(&json!([2])), "{found:?}");
    let members = rows(
        &fluree,
        &ledger,
        "SELECT ?a ?b ?c WHERE { ex:s ex:items (?a ?b ?c) }",
    )
    .await;
    assert_eq!(members, [json!([1, 2, 3])]);
}

/// A cell something else names is no longer a plain cell: the list is stored
/// as its triples, the same RDF.
#[tokio::test]
async fn a_shared_cell_stores_the_list_as_triples() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(&fluree, "shared", "ex:s ex:items (1 2 3) .").await;
    let ledger = update(
        &fluree,
        ledger,
        "INSERT { ex:t ex:tail ?c } WHERE { ex:s ex:items/rdf:rest ?c }",
    )
    .await;
    assert_eq!(lists(&ledger, "s").await, [vec![1, 2, 3]]);
    let stored = values(&fluree, &ledger, "ex:s").await;
    assert_eq!(stored.len(), 1, "the head, stored: {stored:?}");
    let tail = rows(
        &fluree,
        &ledger,
        "SELECT ?x WHERE { ex:t ex:tail/rdf:rest*/rdf:first ?x }",
    )
    .await;
    assert_eq!(tail, [json!([2]), json!([3])]);
}
