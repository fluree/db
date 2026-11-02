//! The RDF view of lists, which the Graph Store `GET` reads through
//! (`QueryExecutionOptions::with_rdf_list_view`).
//!
//! A list stored compactly is, as RDF, its owner edge `s p head` and a chain
//! of `rdf:first`/`rdf:rest` cells; the view reads that graph, so a member is
//! not a value of its predicate. Every query surface reads the members as
//! values instead (`it_sparql_list_members.rs`).

use crate::support;
use fluree_db_api::{FlureeBuilder, LedgerState, QueryExecutionOptions, QueryResult};
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

/// Reads lists as their RDF cells.
fn rdf_lists() -> QueryExecutionOptions {
    QueryExecutionOptions::new().with_rdf_list_view(true)
}

/// `sparql` with lists read as their RDF cells.
async fn rdf_query(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    sparql: &str,
) -> fluree_db_api::Result<QueryResult> {
    support::graphdb_from_ledger(ledger)
        .query(fluree)
        .sparql(sparql)
        .execution_options(rdf_lists())
        .execute()
        .await
}

async fn rows(fluree: &fluree_db_api::Fluree, ledger: &LedgerState, body: &str) -> Vec<JsonValue> {
    let result = rdf_query(fluree, ledger, &format!("{PREFIXES}{body}"))
        .await
        .unwrap_or_else(|e| panic!("query: {e}\n{body}"))
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    support::normalize_rows(&result)
}

/// [`rows`] in the order the query gives them.
async fn ordered_rows(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    body: &str,
) -> Vec<JsonValue> {
    let result = rdf_query(fluree, ledger, &format!("{PREFIXES}{body}"))
        .await
        .unwrap_or_else(|e| panic!("query: {e}\n{body}"))
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    result.as_array().cloned().unwrap_or_default()
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

/// A ledger holding `ttl`, read from its index alone: the list view's
/// predicates come from the index store, not from novelty.
async fn indexed_ledger_with(fluree: &fluree_db_api::Fluree, name: &str, ttl: &str) -> LedgerState {
    let id = format!("it/sparql-lists/{name}:main");
    let ledger = fluree.create_ledger(&id).await.expect("create");
    fluree
        .insert_turtle(
            ledger,
            &format!("@prefix ex: <http://example.org/> .\n{ttl}"),
        )
        .await
        .unwrap_or_else(|e| panic!("insert: {e}"));
    support::rebuild_and_publish_index(fluree, &id).await;
    let ledger = fluree.ledger(&id).await.expect("reload");
    assert!(
        !ledger.novelty.has_list_meta,
        "{name}: novelty holds no list"
    );
    ledger
}

#[tokio::test]
async fn an_indexed_list_is_its_head_and_cells() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = indexed_ledger_with(
        &fluree,
        "indexed-head",
        "ex:s ex:items (1 2 3) ; ex:items 9 .",
    )
    .await;

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
    let cells = rows(&fluree, &ledger, "SELECT ?x WHERE { ?l rdf:first ?x }").await;
    assert_eq!(cells, [json!([1]), json!([2]), json!([3])]);
}

/// A variable a plain triple shares with a list-view pattern joins them, from
/// novelty and from the index, on a ledger whose only list is unrelated.
#[tokio::test]
async fn a_shared_variable_joins_a_triple_into_the_list_view() {
    let ttl = "ex:n0 ex:name \"n0\" ; ex:knows ex:n1 ; ex:likes ex:config .
               ex:n1 ex:name \"n1\" ; ex:knows ex:n2 .
               ex:n2 ex:name \"n2\" ; ex:knows ex:n0 .
               ex:config ex:order (ex:a ex:b) .";
    let fluree = FlureeBuilder::memory().build_memory();
    let novelty = ledger_with(&fluree, "join-novelty", ttl).await;
    let indexed = indexed_ledger_with(&fluree, "join-indexed", ttl).await;
    let friend = [json!(["ex:knows", "ex:n2"]), json!(["ex:name", "n1"])];
    for (lane, ledger) in [("novelty", &novelty), ("index", &indexed)] {
        for body in [
            "SELECT ?p ?o WHERE { ex:n0 ex:knows ?f . ?f ?p ?o }",
            "SELECT ?p ?o WHERE { ?f ?p ?o . ex:n0 ex:knows ?f }",
            "SELECT ?p ?o WHERE { ex:n0 ex:knows ?f OPTIONAL { ?f ?p ?o } }",
        ] {
            assert_eq!(rows(&fluree, ledger, body).await, friend, "{lane}: {body}");
        }
        assert_eq!(
            rows(
                &fluree,
                ledger,
                "SELECT (COUNT(*) AS ?n) WHERE { ?s ex:knows ?f . ?f ?p ?o }",
            )
            .await,
            [json!([2 + 2 + 3])],
            "{lane}: the triples of ex:n1, ex:n2 and ex:n0"
        );
        assert!(
            rows(
                &fluree,
                ledger,
                "SELECT ?x WHERE { ex:n0 ex:knows ?f . ?f rdf:first ?x }"
            )
            .await
            .is_empty(),
            "{lane}: a friend is no cell"
        );
        let liked = rows(
            &fluree,
            ledger,
            "SELECT ?p ?o WHERE { ex:n0 ex:likes ?f . ?f ?p ?o }",
        )
        .await;
        assert_eq!(liked.len(), 1, "{lane}: the owner edge alone: {liked:?}");
        assert_eq!(liked[0][0], json!("ex:order"), "{lane}: {liked:?}");
        assert!(
            liked[0][1].as_str().is_some_and(|o| o.starts_with("_:")),
            "{lane}: the list's head: {liked:?}"
        );
    }
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
    let result = rdf_query(&fluree, &ledger, &query)
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
            .execution_options(rdf_lists())
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
    let result = rdf_query(&fluree, &ledger, &format!("{PREFIXES}DESCRIBE ex:s"))
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
// Writing a list spelled as its cells.
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

/// A zero-length match pairs every node with itself, the cells and
/// `rdf:nil` included, whichever predicate the path names.
#[tokio::test]
async fn a_zero_length_path_reaches_the_cells_on_any_predicate() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(
        &fluree,
        "zero",
        "ex:s ex:items (ex:a ex:b) ; ex:name ex:n .",
    )
    .await;
    let found = rows(&fluree, &ledger, "SELECT ?x ?y WHERE { ?x ex:name* ?y }").await;
    // ex:s, ex:n, two cells, rdf:nil, ex:a and ex:b with themselves, and
    // the name edge.
    assert_eq!(found.len(), 7 + 1, "{found:?}");
}

/// A variable the pattern repeats takes one term, whether or not a row bound
/// it before.
#[tokio::test]
async fn a_repeated_variable_takes_one_term() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(&fluree, "repeat", "ex:s ex:items (1 2) .").await;
    let found = rows(
        &fluree,
        &ledger,
        "SELECT ?x WHERE { VALUES ?x { UNDEF 7 } ?x rdf:first ?x }",
    )
    .await;
    assert!(found.is_empty(), "no cell is its own member: {found:?}");
}

/// An `EXISTS` inside an expression reads the same graph as the patterns
/// around it.
#[tokio::test]
async fn an_exists_expression_reads_lists_as_rdf() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(&fluree, "exists", "ex:s ex:items (1 2) .").await;
    for (query, expected) in [
        (
            "SELECT ?ok WHERE { BIND(EXISTS { ex:s ex:items ?h . ?h rdf:first 1 } AS ?ok) }",
            true,
        ),
        (
            "SELECT ?ok WHERE { BIND(EXISTS { ex:s ex:items 1 } AS ?ok) }",
            false,
        ),
    ] {
        let found = rows(&fluree, &ledger, query).await;
        assert_eq!(found, [json!([expected])], "{query}");
    }
}

/// A collection named only from its own cells has nowhere to be written
/// inline, and keeps its triples.
#[tokio::test]
async fn construct_keeps_a_collection_it_cannot_inline() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(&fluree, "self-list", "ex:s ex:name ex:n .").await;
    let query =
        format!("{PREFIXES}CONSTRUCT {{ _:a rdf:first _:a ; rdf:rest rdf:nil }} WHERE {{ }}");
    let result = rdf_query(&fluree, &ledger, &query)
        .await
        .expect("construct");
    let graph = result.to_construct(&ledger.snapshot).expect("jsonld");
    assert_eq!(graph["@graph"].as_array().map(Vec::len), Some(1), "{graph}");
}

/// A list in the ledger leaves a wildcard pattern streaming: `LIMIT 1`
/// reads the first batch, not every triple.
#[tokio::test]
async fn a_limit_stops_a_list_view_scan() {
    let fluree = FlureeBuilder::memory().build_memory();
    let mut ttl: String = (0..12_000)
        .map(|i| format!("ex:s{i} ex:name {i} .\n"))
        .collect();
    ttl.push_str("ex:owner ex:items (1 2) .\n");
    let ledger = ledger_with(&fluree, "limit", &ttl).await;
    let view = support::graphdb_from_ledger(&ledger);
    let mut fuel = Vec::new();
    for limit in [" LIMIT 1", ""] {
        let query = format!("{PREFIXES}SELECT ?s ?p ?o WHERE {{ ?s ?p ?o }}{limit}");
        let tracked = view
            .query(&fluree)
            .sparql(&query)
            .execution_options(rdf_lists())
            .execute_tracked()
            .await
            .expect("query");
        fuel.push(tracked.fuel.expect("fuel"));
    }
    assert!(fuel[0] < fuel[1] / 3.0, "LIMIT 1 vs all: {fuel:?}");
}

/// The `EXISTS` of a grouped projection, a HAVING and an ORDER BY read the
/// same graph as the WHERE: `ex:s ex:items 1` is no triple of the list.
#[tokio::test]
async fn solution_modifiers_read_lists_as_rdf() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(&fluree, "modifiers", "ex:s ex:items (1 2) ; ex:name 1 .").await;
    let grouped = rows(
        &fluree,
        &ledger,
        "SELECT (EXISTS { ex:s ex:items 1 } AS ?ok) WHERE { ex:s ex:name ?v } GROUP BY ?v",
    )
    .await;
    assert_eq!(grouped, [json!([false])]);
    let having = rows(
        &fluree,
        &ledger,
        "SELECT (COUNT(*) AS ?n) WHERE { ex:s ex:name ?v } HAVING (EXISTS { ex:s ex:items 1 })",
    )
    .await;
    assert!(having.is_empty(), "{having:?}");

    let ledger = ledger_with(
        &fluree,
        "ordered",
        "ex:a ex:items (1 2) . ex:z ex:items 1 .",
    )
    .await;
    let ordered = ordered_rows(
        &fluree,
        &ledger,
        "SELECT ?s WHERE { VALUES ?s { ex:a ex:z } } ORDER BY DESC(EXISTS { ?s ex:items 1 }) ?s",
    )
    .await;
    assert_eq!(
        ordered,
        [json!(["ex:z"]), json!(["ex:a"])],
        "only ex:z holds 1"
    );
}

// ---------------------------------------------------------------------------
// Policy on a list stored as its triples.
// ---------------------------------------------------------------------------

async fn update_as(
    fluree: &fluree_db_api::Fluree,
    ledger: LedgerState,
    body: &str,
    policy: fluree_db_policy::PolicyContext,
) -> Result<fluree_db_api::TransactResult, fluree_db_api::ApiError> {
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
        .policy(policy)
        .execute()
        .await
}

fn assert_refused(r: &Result<fluree_db_api::TransactResult, fluree_db_api::ApiError>, what: &str) {
    match r {
        Err(e) => assert!(
            e.to_string()
                .contains("Policy enforcement prevents modification"),
            "{what}: refused for another reason: {e}"
        ),
        Ok(_) => panic!("{what}: permitted"),
    }
}

/// Everything allowed but what `rule` denies or conditions.
async fn allowing_all_but(
    ledger: &LedgerState,
    rule: JsonValue,
) -> fluree_db_policy::PolicyContext {
    let opts = fluree_db_api::GovernanceOptions {
        policy: Some(json!([rule])),
        default_allow: Some(true),
        ..Default::default()
    };
    fluree_db_api::policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &opts,
        &[0],
    )
    .await
    .expect("policy")
}

/// A second list on one subject and property is stored as its triples, and a
/// change to its cells, wherever along the list, is judged as a change to the
/// owner's property.
#[tokio::test]
async fn a_change_to_a_stored_cell_is_judged_as_its_owners() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(
        &fluree,
        "owner-deny",
        "ex:s ex:items (1 2) . ex:t ex:tags (5 6) .",
    )
    .await;
    let ledger = update(
        &fluree,
        ledger,
        "INSERT DATA { ex:s ex:items (3 4) . ex:t ex:tags (7 8) }",
    )
    .await;
    let policy = allowing_all_but(
        &ledger,
        json!({
            "@id": "http://example.org/denyItems",
            "f:required": true,
            "f:onProperty": [{"@id": "http://example.org/items"}],
            "f:action": "f:modify",
            "f:allow": false
        }),
    )
    .await;
    for edit in [
        "DELETE { ?h rdf:first 3 } INSERT { ?h rdf:first 99 } \
         WHERE { ex:s ex:items ?h . ?h rdf:first 3 }",
        "DELETE { ?c rdf:first 4 } WHERE { ex:s ex:items ?h . ?h rdf:first 3 ; rdf:rest ?c }",
        "DELETE { ?h rdf:rest ?c } INSERT { ?h rdf:rest rdf:nil } \
         WHERE { ex:s ex:items ?h . ?h rdf:first 3 ; rdf:rest ?c }",
    ] {
        let r = update_as(&fluree, ledger.clone(), edit, policy.clone()).await;
        assert_refused(&r, edit);
    }
    let allowed = update_as(
        &fluree,
        ledger,
        "DELETE { ?h rdf:first 7 } INSERT { ?h rdf:first 99 } \
         WHERE { ex:t ex:tags ?h . ?h rdf:first 7 }",
        policy,
    )
    .await
    .expect("a list on another property may be edited")
    .ledger;
    let firsts = support::query_sparql(
        &fluree,
        &allowed,
        &format!("{PREFIXES}SELECT ?x WHERE {{ ex:t ex:tags ?h . ?h rdf:first ?x }}"),
    )
    .await
    .expect("query")
    .to_jsonld(&allowed.snapshot)
    .expect("to_jsonld");
    assert_eq!(firsts, json!([[99]]), "the edit reached the stored cell");
}

/// A post-state condition reads everything the write stores: a list whose
/// cell carries another triple is stored as its triples, which the condition
/// reads.
#[tokio::test]
async fn a_post_state_condition_reads_a_list_stored_as_triples() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(
        &fluree,
        "post-state",
        "ex:s ex:items (1 2) . ex:t ex:name 1 .",
    )
    .await;
    let policy = allowing_all_but(
        &ledger,
        json!({
            "@id": "http://example.org/noTailToOne",
            "f:required": true,
            "f:onProperty": [{"@id": "http://example.org/tail"}],
            "f:action": "f:modify",
            "f:queryState": {"@id": "f:postState"},
            "f:query": {
                "@type": "f:sparql",
                "@value": "ASK { FILTER NOT EXISTS { $value <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> 1 } }"
            }
        }),
    )
    .await;
    let stated = update_as(
        &fluree,
        ledger.clone(),
        "INSERT DATA { ex:t ex:tail _:c . _:c rdf:first 1 ; rdf:rest rdf:nil ; ex:tag 1 }",
        policy.clone(),
    )
    .await;
    assert_refused(&stated, "a tail to a cell holding 1");
    let allowed = update_as(
        &fluree,
        ledger,
        "INSERT DATA { ex:t ex:tail _:c . _:c rdf:first 2 ; rdf:rest rdf:nil ; ex:tag 1 }",
        policy,
    )
    .await;
    assert!(
        allowed.is_ok(),
        "a tail to a cell holding 2: {:?}",
        allowed.err()
    );
}

/// The fast `?s ?p ?o` counts read stored rows, so over the index they add
/// the list view's triples: each list's owner edge in place of its members,
/// and an `rdf:first` and an `rdf:rest` per member. Hand-counted: stored rows
/// are `9`, three and two members, `ex:name` and the empty list's `rdf:nil`.
/// With a list in novelty the lanes decline and the generic plan answers.
#[tokio::test]
async fn whole_graph_counts_add_the_list_triples() {
    const COUNT: &str = "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }";
    const BY_PREDICATE: &str = "SELECT ?p (COUNT(?s) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?p";
    let ttl = "ex:s ex:items (1 2 3) ; ex:items 9 .
               ex:t ex:tags (ex:a ex:b) .
               ex:u ex:name \"u\" .
               ex:v ex:items () .";
    let by_predicate = |items: i64, tags: i64, name: i64, cells: i64| {
        let mut rows = vec![
            json!(["ex:items", items]),
            json!(["ex:name", name]),
            json!(["ex:tags", tags]),
            json!(["rdf:first", cells]),
            json!(["rdf:rest", cells]),
        ];
        rows.sort_by_key(ToString::to_string);
        rows
    };
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = indexed_ledger_with(&fluree, "whole-graph-counts", ttl).await;

    let (spans, _guard) = support::span_capture::init_test_tracing();
    let outcomes = || -> Vec<String> {
        spans
            .find_events("fast-path outcome")
            .iter()
            .map(|e| format!("{:?} {:?}", e.fields.get("site"), e.fields.get("outcome")))
            .collect()
    };
    let proceeded = |site: &str| {
        spans.find_events("fast-path outcome").iter().any(|e| {
            e.fields.get("site").map(String::as_str) == Some(site)
                && e.fields.get("outcome").map(String::as_str) == Some("proceed")
        })
    };
    assert_eq!(rows(&fluree, &ledger, COUNT).await, [json!([8 + 5 + 2])]);
    assert!(
        proceeded("triples COUNT"),
        "the count lane answered: {:?}",
        outcomes()
    );
    let mut grouped = rows(&fluree, &ledger, BY_PREDICATE).await;
    grouped.sort_by_key(ToString::to_string);
    // `ex:items`: five stored rows, less three members, plus one owner edge.
    assert_eq!(grouped, by_predicate(5 - 3 + 1, 1, 1, 5));
    assert!(
        proceeded("COUNT by predicate (directory)"),
        "the per-predicate lane answered: {:?}",
        outcomes()
    );

    // A list in novelty: the lanes decline, and the generic plan counts it.
    let ledger = fluree
        .insert_turtle(
            ledger,
            "@prefix ex: <http://example.org/> .\nex:w ex:tags (ex:c) .",
        )
        .await
        .expect("insert")
        .ledger;
    assert_eq!(rows(&fluree, &ledger, COUNT).await, [json!([15 + 3])]);
    let mut grouped = rows(&fluree, &ledger, BY_PREDICATE).await;
    grouped.sort_by_key(ToString::to_string);
    assert_eq!(grouped, by_predicate(3, 2, 1, 6));
}

/// A JSON-LD crawl reads a list stored as `rdf:first`/`rdf:rest` triples as
/// its members, as it reads a compact list: one whose head carries another
/// property, and a second list on the property.
#[tokio::test]
async fn jsonld_reads_a_list_stored_as_triples_as_its_members() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/sparql-lists/stored-as-triples:main";
    let ctx = json!({"ex": "http://example.org/"});
    let list = |items: &[&str]| {
        let items: Vec<JsonValue> = items.iter().map(|i| json!({"@id": i})).collect();
        json!({"@context": ctx, "@id": "ex:c", "ex:items": {"@list": items}})
    };
    let items = |ledger: LedgerState| {
        let fluree = &fluree;
        let ctx = ctx.clone();
        async move {
            support::query_jsonld(
                fluree,
                &ledger,
                &json!({"@context": ctx, "selectOne": {"ex:c": ["*"]}}),
            )
            .await
            .expect("crawl")
            .to_jsonld_async(ledger.as_graph_db_ref(0))
            .await
            .expect("to_jsonld")["ex:items"]
                .clone()
        }
    };
    let ids = |ids: &[&str]| json!(ids.iter().map(|i| json!({"@id": i})).collect::<Vec<_>>());
    let ledger = fluree.create_ledger(id).await.expect("create");
    let ledger = fluree
        .insert_turtle(
            ledger,
            "@prefix ex: <http://example.org/> .
             @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
             ex:c ex:items _:h .
             _:h rdf:first ex:A ; rdf:rest _:b ; ex:note \"x\" .
             _:b rdf:first ex:B ; rdf:rest _:c .
             _:c rdf:first ex:C ; rdf:rest rdf:nil .",
        )
        .await
        .expect("insert")
        .ledger;
    assert_eq!(items(ledger.clone()).await, ids(&["ex:A", "ex:B", "ex:C"]));

    // Each list in its order; two lists on a property in no set order.
    let ledger = fluree
        .insert(ledger, &list(&["ex:X", "ex:Y"]))
        .await
        .expect("insert")
        .ledger;
    let got = items(ledger).await;
    assert!(
        got == ids(&["ex:A", "ex:B", "ex:C", "ex:X", "ex:Y"])
            || got == ids(&["ex:X", "ex:Y", "ex:A", "ex:B", "ex:C"]),
        "{got}"
    );
}

/// A crawl asks whether a blank-node value heads a list only of a graph
/// holding an `rdf:first`, which it reads from the index and from novelty.
#[tokio::test]
async fn a_crawl_reads_a_list_stored_as_triples_from_the_index_or_novelty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let crawl = |ledger: LedgerState, s: &'static str| {
        let fluree = &fluree;
        async move {
            support::query_jsonld(
                fluree,
                &ledger,
                &json!({
                    "@context": {"ex": "http://example.org/"},
                    "selectOne": {s: ["*"]}
                }),
            )
            .await
            .expect("crawl")
            .to_jsonld_async(ledger.as_graph_db_ref(0))
            .await
            .expect("to_jsonld")
        }
    };
    let chain = "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        ex:c ex:items _:h .
        _:h rdf:first ex:A ; rdf:rest _:t ; ex:note \"x\" .
        _:t rdf:first ex:B ; rdf:rest rdf:nil .";

    let indexed = indexed_ledger_with(&fluree, "crawl-indexed-chain", chain).await;
    assert_eq!(
        crawl(indexed, "ex:c").await["ex:items"],
        json!([{"@id": "ex:A"}, {"@id": "ex:B"}])
    );

    let ledger = indexed_ledger_with(
        &fluree,
        "crawl-novel-chain",
        "ex:d ex:addr [ ex:city \"x\" ] .",
    )
    .await;
    let addr = crawl(ledger.clone(), "ex:d").await["ex:addr"].clone();
    assert!(
        addr["@id"].as_str().is_some_and(|id| id.starts_with("_:")),
        "a blank node, not a list: {addr}"
    );
    let ledger = fluree
        .insert_turtle(
            ledger,
            &format!("@prefix ex: <http://example.org/> .\n{chain}"),
        )
        .await
        .expect("insert")
        .ledger;
    assert_eq!(
        crawl(ledger, "ex:c").await["ex:items"],
        json!([{"@id": "ex:A"}, {"@id": "ex:B"}])
    );
}

/// A FILTER or BIND in the group of a list-view pattern applies to its list
/// triples too, and its stored triples still hide the members: the block is
/// planned with the FILTER, not as a plain triple. From novelty and the index.
#[tokio::test]
async fn a_filter_or_bind_beside_a_list_view_pattern() {
    let ttl = "ex:s ex:name \"s\" ; ex:items (10 20 30) ; ex:items 1 .";
    let fluree = FlureeBuilder::memory().build_memory();
    let novelty = ledger_with(&fluree, "filter-novelty", ttl).await;
    let indexed = indexed_ledger_with(&fluree, "filter-indexed", ttl).await;
    for (lane, ledger) in [("novelty", &novelty), ("index", &indexed)] {
        assert_eq!(
            rows(
                &fluree,
                ledger,
                "SELECT ?x WHERE { ex:s ex:items ?l . ?l rdf:first ?x FILTER(?x != 0) }"
            )
            .await,
            [json!([10])],
            "{lane}: a cell walk under a FILTER"
        );
        assert_eq!(
            rows(
                &fluree,
                ledger,
                "SELECT ?o WHERE { ex:s ex:items ?o FILTER(isLiteral(?o)) }"
            )
            .await,
            [json!([1])],
            "{lane}: the members are no values of their property"
        );
        assert_eq!(
            rows(
                &fluree,
                ledger,
                "SELECT ?y WHERE { ex:s ex:items ?l . ?l rdf:first ?x . BIND(?x + 1 AS ?y) }"
            )
            .await,
            [json!([11])],
            "{lane}: a BIND over a cell's member"
        );
        assert!(
            rows(
                &fluree,
                ledger,
                "SELECT DISTINCT ?s WHERE { ?s ex:name ?n . ?s ex:items ?o FILTER(?o > 5) }"
            )
            .await
            .is_empty(),
            "{lane}: a range FILTER on the list's property sees no member"
        );
    }
}
