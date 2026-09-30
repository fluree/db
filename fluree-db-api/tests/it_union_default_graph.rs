//! The union default graph: with `f:queryDefaults` / `f:unionDefaultGraph`
//! on the ledger, or the request's own `# PRAGMA union-default-graph` /
//! `opts.unionDefaultGraph`, the default graph reads as the union of the
//! ledger's default graph and its named graphs. `GRAPH` still addresses each
//! named graph as before, and the reserved `#txn-meta` / `#config` graphs stay
//! out.

use crate::support::{self, genesis_ledger, MemoryFluree, MemoryLedger};
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};

const G1: &str = "http://example.org/g1";
const G2: &str = "http://example.org/g2";

/// Alice in the default graph, Bob in `g1`, Carol in `g2`. `knows` runs
/// alice → bob (default graph) → carol (`g1`), and `g1` also repeats one
/// default-graph triple, so the union holds five distinct triples.
async fn seed(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let trig = format!(
        r#"
        @prefix ex: <http://example.org/> .

        ex:alice ex:name "Alice" .
        ex:alice ex:knows ex:bob .

        GRAPH <{G1}> {{
            ex:bob ex:name "Bob" .
            ex:bob ex:knows ex:carol .
            ex:alice ex:name "Alice" .
        }}

        GRAPH <{G2}> {{
            ex:carol ex:name "Carol" .
        }}
        "#
    );
    fluree
        .stage_owned(genesis_ledger(fluree, ledger_id))
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("seed")
        .ledger
}

/// Turn the ledger's union default graph on (or off) in its config graph.
async fn set_union(fluree: &MemoryFluree, ledger: MemoryLedger, on: bool) -> MemoryLedger {
    let ledger_id = ledger.ledger_id().to_string();
    let config = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <urn:fluree:{ledger_id}#config> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:queryDefaults <urn:config:query> .
            <urn:config:query> f:unionDefaultGraph {on} .
        }}
        "
    );
    fluree
        .stage_owned(ledger)
        .upsert_turtle(&config)
        .execute()
        .await
        .expect("config")
        .ledger
}

/// A query's rows, each as its values joined by a space, sorted.
fn column(rows: &JsonValue) -> Vec<String> {
    let text = |v: &JsonValue| v.as_str().map_or_else(|| v.to_string(), str::to_string);
    let mut out: Vec<String> = rows
        .as_array()
        .unwrap_or_else(|| panic!("rows: {rows}"))
        .iter()
        .map(|row| match row.as_array() {
            Some(values) => values.iter().map(text).collect::<Vec<_>>().join(" "),
            None => text(row),
        })
        .collect();
    out.sort();
    out
}

async fn sparql(fluree: &MemoryFluree, ledger: &MemoryLedger, query: &str) -> Vec<String> {
    let result = support::query_sparql(fluree, ledger, query)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    column(&result.to_jsonld(&ledger.snapshot).expect("jsonld"))
}

const NAMES: &str = "PREFIX ex: <http://example.org/>
                     SELECT ?name WHERE { ?s ex:name ?name }";

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| (*s).to_string()).collect()
}

#[tokio::test]
async fn default_graph_reads_alone_unless_configured() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "union/off:main").await;
    assert_eq!(sparql(&fluree, &ledger, NAMES).await, strs(&["Alice"]));

    let ledger = set_union(&fluree, ledger, false).await;
    assert_eq!(sparql(&fluree, &ledger, NAMES).await, strs(&["Alice"]));
}

/// With the ledger setting on, the default graph is the union: every named
/// graph's triples, a triple held by two graphs once, and nothing from the
/// reserved graphs (the config that switched the union on lives in one).
#[tokio::test]
async fn ledger_setting_unions_the_named_graphs() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "union/on:main").await;
    let ledger = set_union(&fluree, ledger, true).await;

    assert_eq!(
        sparql(&fluree, &ledger, NAMES).await,
        strs(&["Alice", "Bob", "Carol"])
    );
    assert_eq!(
        sparql(
            &fluree,
            &ledger,
            "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }"
        )
        .await,
        strs(&["5"])
    );
    assert!(sparql(
        &fluree,
        &ledger,
        "SELECT ?s WHERE { ?s a <https://ns.flur.ee/db#LedgerConfig> }"
    )
    .await
    .is_empty());
}

/// `GRAPH` resolves as it does without the union: `GRAPH ?g` ranges over the
/// named graphs, and the ledger alias names the default graph alone.
#[tokio::test]
async fn graph_patterns_address_graphs_as_before() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "union/graph:main").await;
    let ledger = set_union(&fluree, ledger, true).await;

    let by_graph = "PREFIX ex: <http://example.org/>
                    SELECT ?g ?name WHERE { GRAPH ?g { ?s ex:name ?name } }";
    assert_eq!(
        sparql(&fluree, &ledger, by_graph).await,
        strs(&[
            "http://example.org/g1 Alice",
            "http://example.org/g1 Bob",
            "http://example.org/g2 Carol"
        ])
    );

    let in_alias = "PREFIX ex: <http://example.org/>
                    SELECT ?name WHERE { GRAPH <union/graph:main> { ?s ex:name ?name } }";
    assert_eq!(sparql(&fluree, &ledger, in_alias).await, strs(&["Alice"]));

    let in_g2 = format!(
        "PREFIX ex: <http://example.org/>
         SELECT ?name WHERE {{ GRAPH <{G2}> {{ ?s ex:name ?name }} }}"
    );
    assert_eq!(sparql(&fluree, &ledger, &in_g2).await, strs(&["Carol"]));
}

/// The request's own switch wins over the ledger's, both ways, in SPARQL and
/// in JSON-LD.
#[tokio::test]
async fn request_switch_wins_over_the_ledger() {
    let fluree = FlureeBuilder::memory().build_memory();
    let off = seed(&fluree, "union/req-off:main").await;
    let on = set_union(&fluree, seed(&fluree, "union/req-on:main").await, true).await;

    let pragma_on = format!("# PRAGMA union-default-graph: true\n{NAMES}");
    let pragma_off = format!("# PRAGMA union-default-graph: false\n{NAMES}");
    assert_eq!(
        sparql(&fluree, &off, &pragma_on).await,
        strs(&["Alice", "Bob", "Carol"])
    );
    assert_eq!(sparql(&fluree, &on, &pragma_off).await, strs(&["Alice"]));

    let jsonld = |union: bool| {
        json!({
            "@context": {"ex": "http://example.org/"},
            "select": "?name",
            "where": {"@id": "?s", "ex:name": "?name"},
            "opts": {"unionDefaultGraph": union}
        })
    };
    let run = |ledger: &MemoryLedger, q: JsonValue| {
        let fluree = &fluree;
        let ledger = ledger.clone();
        async move {
            let result = support::query_jsonld(fluree, &ledger, &q)
                .await
                .unwrap_or_else(|e| panic!("{q}: {e}"));
            column(&result.to_jsonld(&ledger.snapshot).expect("jsonld"))
        }
    };
    assert_eq!(
        run(&off, jsonld(true)).await,
        strs(&["Alice", "Bob", "Carol"])
    );
    assert_eq!(run(&on, jsonld(false)).await, strs(&["Alice"]));
}

/// A non-boolean switch is an error, not an option silently ignored.
#[tokio::test]
async fn malformed_request_switch_is_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "union/bad:main").await;

    let err = support::query_sparql(
        &fluree,
        &ledger,
        &format!("# PRAGMA union-default-graph: yes\n{NAMES}"),
    )
    .await
    .expect_err("bad pragma value");
    assert!(err.to_string().contains("union-default-graph"), "{err}");

    let err = support::query_jsonld(
        &fluree,
        &ledger,
        &json!({
            "select": "?s",
            "where": {"@id": "?s", "http://example.org/name": "?n"},
            "opts": {"unionDefaultGraph": "yes"}
        }),
    )
    .await
    .expect_err("bad opts value");
    assert!(err.to_string().contains("unionDefaultGraph"), "{err}");
}

/// A property path follows edges across the union's graphs: alice knows bob
/// in the default graph, and bob knows carol in `g1`.
#[tokio::test]
async fn property_paths_cross_graphs() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "union/path:main").await;

    let reach = "PREFIX ex: <http://example.org/>
                 SELECT (STR(?x) AS ?iri) WHERE { ex:alice ex:knows+ ?x }";
    assert_eq!(
        sparql(&fluree, &ledger, reach).await,
        strs(&["http://example.org/bob"])
    );

    let ledger = set_union(&fluree, ledger, true).await;
    assert_eq!(
        sparql(&fluree, &ledger, reach).await,
        strs(&["http://example.org/bob", "http://example.org/carol"])
    );

    let back = "PREFIX ex: <http://example.org/>
                SELECT (STR(?x) AS ?iri) WHERE { ?x ex:knows+ ex:carol }";
    assert_eq!(
        sparql(&fluree, &ledger, back).await,
        strs(&["http://example.org/alice", "http://example.org/bob"])
    );

    let closure = "PREFIX ex: <http://example.org/>
                   SELECT (COUNT(*) AS ?n) WHERE { ?a ex:knows+ ?b }";
    assert_eq!(sparql(&fluree, &ledger, closure).await, strs(&["3"]));
}

/// A `FROM` naming several graphs of one ledger runs property paths too:
/// they used to be refused over any multi-graph default graph.
#[tokio::test]
async fn property_paths_over_explicit_same_ledger_from() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "union/from-path:main").await;

    let reach = format!(
        "PREFIX ex: <http://example.org/>
         SELECT (STR(?x) AS ?iri)
         FROM <union/from-path:main> FROM <{G1}>
         WHERE {{ ex:alice ex:knows+ ?x }}"
    );
    assert_eq!(
        sparql(&fluree, &ledger, &reach).await,
        strs(&["http://example.org/bob", "http://example.org/carol"])
    );
}

/// Naming the ledger itself in `FROM` reads its default graph, which the
/// setting makes the union; naming a graph reads just that graph.
#[tokio::test]
async fn from_the_ledger_reads_the_union_and_a_named_graph_narrows() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "union/from:main").await;
    let ledger = set_union(&fluree, ledger, true).await;

    let from_ledger = "PREFIX ex: <http://example.org/>
                       SELECT ?name FROM <union/from:main> WHERE { ?s ex:name ?name }";
    assert_eq!(
        sparql(&fluree, &ledger, from_ledger).await,
        strs(&["Alice", "Bob", "Carol"])
    );

    let from_g2 = format!(
        "PREFIX ex: <http://example.org/>
         SELECT ?name FROM <{G2}> WHERE {{ ?s ex:name ?name }}"
    );
    assert_eq!(sparql(&fluree, &ledger, &from_g2).await, strs(&["Carol"]));

    let connection = fluree
        .query_connection_sparql(from_ledger)
        .await
        .expect("connection query");
    assert_eq!(
        column(&connection.to_jsonld(&ledger.snapshot).expect("jsonld")),
        strs(&["Alice", "Bob", "Carol"])
    );

    let jsonld = json!({
        "@context": {"ex": "http://example.org/"},
        "from": "union/from:main",
        "select": "?name",
        "where": {"@id": "?s", "ex:name": "?name"}
    });
    let connection = fluree
        .query_connection(&jsonld)
        .await
        .expect("JSON-LD connection query");
    assert_eq!(
        column(&connection.to_jsonld(&ledger.snapshot).expect("jsonld")),
        strs(&["Alice", "Bob", "Carol"])
    );
}

/// The setting is read as of the query's `t`: before it was written, the
/// default graph reads alone.
#[tokio::test]
async fn setting_is_read_as_of_the_query_t() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "union/tt:main").await;
    let ledger = set_union(&fluree, ledger, true).await;

    let at = |t: i64| {
        format!(
            "PREFIX ex: <http://example.org/>
             SELECT ?name FROM <union/tt:main@t:{t}> WHERE {{ ?s ex:name ?name }}"
        )
    };
    let run = |q: String| {
        let fluree = &fluree;
        let snapshot = ledger.snapshot.clone();
        async move {
            let result = fluree
                .query_connection_sparql(&q)
                .await
                .unwrap_or_else(|e| panic!("{q}: {e}"));
            column(&result.to_jsonld(&snapshot).expect("jsonld"))
        }
    };
    assert_eq!(run(at(1)).await, strs(&["Alice"]));
    assert_eq!(run(at(2)).await, strs(&["Alice", "Bob", "Carol"]));
}

/// Query shapes the planner answers from single-graph fast paths (counts,
/// distinct counts, grouped counts, min/max, top-k, joins, paths), each
/// checked against the union. Each must read every graph, not the default
/// graph's index alone.
async fn assert_union_answers(fluree: &MemoryFluree, ledger: &MemoryLedger) {
    let cases: &[(&str, &[&str])] = &[
        ("SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }", &["5"]),
        (
            "SELECT (COUNT(DISTINCT ?s) AS ?n) WHERE { ?s ?p ?o }",
            &["3"],
        ),
        (
            "SELECT (COUNT(DISTINCT ?p) AS ?n) WHERE { ?s ?p ?o }",
            &["2"],
        ),
        (
            "SELECT (COUNT(?name) AS ?n) WHERE { ?s ex:name ?name }",
            &["3"],
        ),
        (
            "SELECT ?p (COUNT(*) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?p",
            &["ex:knows 2", "ex:name 3"],
        ),
        (
            "SELECT (MAX(?name) AS ?m) WHERE { ?s ex:name ?name }",
            &["Carol"],
        ),
        (
            "SELECT ?name WHERE { ?s ex:name ?name } ORDER BY DESC(?name) LIMIT 1",
            &["Carol"],
        ),
        (
            "SELECT ?name WHERE { ?s ex:knows ?o . ?o ex:name ?name }",
            &["Bob", "Carol"],
        ),
        (
            "SELECT (STR(?x) AS ?iri) WHERE { ex:alice ex:knows+ ?x }",
            &["http://example.org/bob", "http://example.org/carol"],
        ),
        ("SELECT ?name WHERE { ex:carol ex:name ?name }", &["Carol"]),
    ];
    for (body, expected) in cases {
        let query = format!("PREFIX ex: <http://example.org/>\n{body}");
        assert_eq!(
            sparql(fluree, ledger, &query).await,
            strs(expected),
            "{body}"
        );
    }
}

#[tokio::test]
async fn union_answers_from_novelty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "union/novelty:main").await;
    let ledger = set_union(&fluree, ledger, true).await;
    assert_union_answers(&fluree, &ledger).await;
}

#[tokio::test]
async fn union_answers_from_the_index() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "union/indexed:main";
    let ledger = seed(&fluree, ledger_id).await;
    set_union(&fluree, ledger, true).await;
    assert_union_answers(&fluree, &indexed(&fluree, ledger_id).await).await;
}

/// Load `ledger_id` with its commits indexed, so scans take the binary cursor.
async fn indexed(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    support::rebuild_and_publish_index(fluree, ledger_id).await;
    let ledger = fluree.ledger(ledger_id).await.expect("load indexed ledger");
    assert!(
        ledger.snapshot.range_provider.is_some(),
        "expected the binary index to be attached"
    );
    ledger
}

/// On an indexed ledger, a join into a default graph of several graphs binds
/// the shared variable. The right-hand scan used to receive the left row's
/// value still encoded, with no single graph view to decode it against, and
/// ran unbound: every `knows` row paired with every name.
#[tokio::test]
async fn joins_over_several_graphs_of_an_indexed_ledger() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "union/join:main";
    seed(&fluree, ledger_id).await;
    let ledger = indexed(&fluree, ledger_id).await;

    let join = format!(
        "PREFIX ex: <http://example.org/>
         SELECT ?name FROM <{ledger_id}> FROM <{G1}> FROM <{G2}>
         WHERE {{ ?s ex:knows ?o . ?o ex:name ?name }}"
    );
    assert_eq!(
        sparql(&fluree, &ledger, &join).await,
        strs(&["Bob", "Carol"])
    );
}

/// A `GRAPH` scope's rows reach a union default graph decoded, so a join on
/// them binds: bob knows carol in `g1`, and carol's name is in `g2`.
#[tokio::test]
async fn graph_scope_rows_join_the_union() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "union/graph-join:main";
    let ledger = seed(&fluree, ledger_id).await;
    set_union(&fluree, ledger, true).await;
    let ledger = indexed(&fluree, ledger_id).await;

    let join = format!(
        "PREFIX ex: <http://example.org/>
         SELECT ?name WHERE {{ GRAPH <{G1}> {{ ?s ex:knows ?o }} ?o ex:name ?name }}"
    );
    assert_eq!(sparql(&fluree, &ledger, &join).await, strs(&["Carol"]));
}

/// `SERVICE` naming the queried ledger itself reads what the query reads: its
/// default graph, which the setting makes the union.
#[tokio::test]
async fn service_on_the_same_ledger_reads_the_union() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "union/service:main").await;
    let ledger = set_union(&fluree, ledger, true).await;

    let service = "PREFIX ex: <http://example.org/>
                   SELECT ?name WHERE {
                     SERVICE <fluree:ledger:union/service:main> { ?s ex:name ?name }
                   }";
    assert_eq!(
        sparql(&fluree, &ledger, service).await,
        strs(&["Alice", "Bob", "Carol"])
    );
}

/// A ledger whose only `Person` lives in `g1`, with the union on and Cypher's
/// bare names resolving under `http://example.org/`.
async fn cypher_ledger(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let trig = format!(
        r#"
        @prefix ex: <http://example.org/> .
        ex:bob ex:name "Bob" .
        GRAPH <{G1}> {{ ex:alice a ex:Person ; ex:name "Alice" . }}
        "#
    );
    let ledger = fluree
        .stage_owned(genesis_ledger(fluree, ledger_id))
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("seed")
        .ledger;
    fluree
        .set_default_context(ledger_id, &json!({"@vocab": "http://example.org/"}))
        .await
        .expect("vocab");
    set_union(fluree, ledger, true).await
}

/// `Person` nodes in the default graph alone.
async fn default_graph_people(fluree: &MemoryFluree, ledger: &MemoryLedger) -> Vec<String> {
    sparql(
        fluree,
        ledger,
        "# PRAGMA union-default-graph: false
         SELECT (COUNT(?s) AS ?n) WHERE { ?s a <http://example.org/Person> }",
    )
    .await
}

/// A Cypher query reads the union like any other query.
#[tokio::test]
async fn cypher_queries_read_the_union() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = cypher_ledger(&fluree, "union/cypher-read:main").await;
    let db = support::graphdb_from_ledger(&ledger)
        .with_default_context(Some(json!({"@vocab": "http://example.org/"})));
    let result = fluree
        .query_cypher(&db, "MATCH (n:Person) RETURN n.name")
        .await
        .expect("cypher query");
    assert_eq!(result.row_count(), 1);
}

/// A write statement's reads see the default graph alone, as a transaction's
/// WHERE does: the write stages against that graph. A `MERGE` whose pattern
/// matches only in a named graph therefore creates in the default graph,
/// through each driver: a bare `MERGE`, the conditional `ON MATCH SET` probe,
/// and the multi-clause driver's own probe.
#[tokio::test]
async fn cypher_writes_read_the_default_graph_alone() {
    let fluree = FlureeBuilder::memory().build_memory();
    for (ledger_id, stmt) in [
        ("union/merge:main", r#"MERGE (a:Person {name: "Alice"})"#),
        (
            "union/merge-set:main",
            r#"MERGE (a:Person {name: "Alice"}) ON MATCH SET a.seen = true"#,
        ),
        (
            "union/merge-seq:main",
            r#"MERGE (a:Person {name: "Alice"}) MERGE (b:City {name: "Paris"})"#,
        ),
    ] {
        let ledger = cypher_ledger(&fluree, ledger_id).await;
        assert_eq!(default_graph_people(&fluree, &ledger).await, strs(&["0"]));
        let ledger = fluree
            .transact_cypher(ledger, stmt)
            .await
            .unwrap_or_else(|e| panic!("{stmt}: {e}"))
            .ledger;
        assert_eq!(
            default_graph_people(&fluree, &ledger).await,
            strs(&["1"]),
            "{stmt} merged onto a node it cannot write to"
        );
    }
}

/// The search-provider connection path reads the union too, and plans its
/// default graph of several graphs as a set.
#[tokio::test]
async fn search_connection_path_reads_the_union() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "union/bm25:main").await;
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "from": "union/bm25:main",
        "select": "?name",
        "where": {"@id": "?s", "ex:name": "?name"},
        "opts": {"unionDefaultGraph": true}
    });
    let result = fluree
        .query_connection_with_bm25(&query)
        .await
        .expect("search connection query");
    assert_eq!(
        column(&result.to_jsonld(&ledger.snapshot).expect("jsonld")),
        strs(&["Alice", "Bob", "Carol"])
    );
}
