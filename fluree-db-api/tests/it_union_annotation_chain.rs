//! An annotated edge under a default graph of several graphs answers as the
//! RDF merge of those graphs: an edge and its annotation that two members
//! both hold are one solution, as in the same data held once in one graph.
//! Two annotations of one edge stay two solutions, whichever members hold
//! them.
//!
//! Each query runs before the ledgers are indexed and after, over two graphs
//! of one ledger (SPARQL `FROM` and the union pragma, JSON-LD `from`) and two
//! ledgers, with fast paths on and off, and is checked against a ledger whose
//! default graph holds the union's facts once (the floor).
//!
//! Fast paths are switched off process-wide, so this is its own `[[test]]`.

use fluree_db_api::{set_fast_paths_disabled, Fluree, FlureeBuilder, GraphDb, ReindexOptions};
use serde_json::{json, Value as JsonValue};

const EX: &str = "http://example.org/";
const LEDGER: &str = "it/union-annotation-chain:main";
/// g1's facts, and g2's, each as a ledger's default graph.
const LEDGER_A: &str = "it/union-annotation-chain-a:main";
const LEDGER_B: &str = "it/union-annotation-chain-b:main";
const FLOOR: &str = "it/union-annotation-chain-floor:main";
const G1: &str = "urn:g1";
const G2: &str = "urn:g2";

/// `who`'s edge to acme, annotated by `ann` (an id and a role) or not, in
/// `graph` (`None`: the default graph).
fn edge(who: &str, ann: Option<(&str, &str)>, graph: Option<&str>) -> JsonValue {
    let mut node = json!({"@id": format!("ex:{who}"), "ex:worksFor": {"@id": "ex:acme"}});
    if let Some((id, role)) = ann {
        node["ex:worksFor"]["@annotation"] = json!({"@id": format!("ex:{id}"), "ex:role": role});
    }
    if let Some(g) = graph {
        node["@graph"] = json!(g);
    }
    node
}

/// g1: alice (annotation empA), bob (empB), carol (empC1), dave (empD).
fn g1_edges(graph: Option<&str>) -> Vec<JsonValue> {
    vec![
        edge("alice", Some(("empA", "Engineer")), graph),
        edge("bob", Some(("empB", "Manager")), graph),
        edge("carol", Some(("empC1", "Intern")), graph),
        edge("dave", Some(("empD", "Lead")), graph),
    ]
}

/// g2: alice's edge and annotation again, carol's edge under another
/// annotation of the same role (empC2), and dave's edge bare.
fn g2_edges(graph: Option<&str>) -> Vec<JsonValue> {
    vec![
        edge("alice", Some(("empA", "Engineer")), graph),
        edge("carol", Some(("empC2", "Intern")), graph),
        edge("dave", None, graph),
    ]
}

fn txn(nodes: Vec<JsonValue>) -> JsonValue {
    json!({"@context": {"ex": EX}, "@graph": nodes})
}

/// The union's facts, once each, in one default graph: alice's edge and
/// annotation once, carol's edge with both annotations.
fn floor_txn() -> JsonValue {
    let mut nodes = g1_edges(None);
    nodes.push(edge("carol", Some(("empC2", "Intern")), None));
    txn(nodes)
}

/// The hand answer to `SELECT ?s ?role`, which the floor must give too:
/// carol twice (two annotations), everyone else once.
const WANT: &[&str] = &[
    "alice Engineer",
    "bob Manager",
    "carol Intern",
    "carol Intern",
    "dave Lead",
];

async fn build(indexed: bool) -> Fluree {
    let fluree = FlureeBuilder::memory().without_indexing().build_memory();
    let mut union = g1_edges(Some(G1));
    union.extend(g2_edges(Some(G2)));
    for (ledger, txn) in [
        (LEDGER, txn(union)),
        (LEDGER_A, txn(g1_edges(None))),
        (LEDGER_B, txn(g2_edges(None))),
        (FLOOR, floor_txn()),
    ] {
        let created = fluree.create_ledger(ledger).await.expect("create");
        fluree.insert(created, &txn).await.expect("insert");
        if indexed {
            fluree
                .reindex(ledger, ReindexOptions::default())
                .await
                .expect("index");
            fluree.disconnect_ledger(ledger).await;
        }
    }
    fluree
}

fn short(v: &str) -> String {
    v.strip_prefix(EX)
        .or_else(|| v.strip_prefix("ex:"))
        .unwrap_or(v)
        .to_string()
}

async fn sparql(fluree: &Fluree, ledger: &str, query: &str) -> Vec<String> {
    let handle = fluree.ledger_cached(ledger).await.expect("ledger");
    let view = GraphDb::from_ledger_state(&handle.snapshot().await.to_ledger_state());
    let json = view
        .query(fluree)
        .sparql(query)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let mut rows: Vec<String> = json["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("{query}: {json}"))
        .iter()
        .map(|b| {
            ["s", "role", "c"]
                .iter()
                .filter_map(|v| b.get(*v))
                .map(|t| short(t["value"].as_str().unwrap_or_default()))
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    rows.sort();
    rows
}

/// A connection query's rows, its columns joined by a space, sorted.
async fn connection_rows(fluree: &Fluree, result: fluree_db_api::QueryResult) -> Vec<String> {
    let db = fluree.ledger(LEDGER).await.expect("ledger");
    let json = result
        .to_jsonld_async(db.as_graph_db_ref(0))
        .await
        .expect("format");
    let mut rows: Vec<String> = json
        .as_array()
        .unwrap_or_else(|| panic!("rows: {json}"))
        .iter()
        .map(|row| match row {
            JsonValue::Array(cols) => cols
                .iter()
                .map(|c| short(c.as_str().unwrap_or(&c.to_string())))
                .collect::<Vec<_>>()
                .join(" "),
            single => short(single.as_str().unwrap_or(&single.to_string())),
        })
        .collect();
    rows.sort();
    rows
}

async fn jsonld(fluree: &Fluree, query: &JsonValue) -> Vec<String> {
    let result = fluree
        .query_connection(query)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    connection_rows(fluree, result).await
}

async fn connection_sparql(fluree: &Fluree, query: &str) -> Vec<String> {
    let result = fluree
        .query_connection_sparql(query)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    connection_rows(fluree, result).await
}

const BODY: &str = "?s ex:worksFor ex:acme {| ex:role ?role |}";

/// Every spelling of the union, against the floor's answer to the same
/// query.
async fn check(fluree: &Fluree, state: &str, failures: &mut Vec<String>) {
    let prefix = format!("PREFIX ex: <{EX}>\n");
    let select = |head: &str, dataset: &str, pragma: bool| {
        format!(
            "{}{prefix}SELECT {head} {dataset} WHERE {{ {BODY} }}",
            if pragma {
                "# PRAGMA union-default-graph: true\n"
            } else {
                ""
            }
        )
    };
    let mut compare = |spelling: &str, got: Vec<String>, floor: &[String]| {
        if got != floor {
            failures.push(format!("{state}: {spelling}: {got:?}, floor {floor:?}"));
        }
    };
    for head in ["?s ?role", "(COUNT(*) AS ?c)"] {
        let floor = sparql(fluree, FLOOR, &select(head, "", false)).await;
        let graphs = select(head, &format!("FROM <{G1}> FROM <{G2}>"), false);
        compare(
            &format!("SPARQL FROM g1 FROM g2 `{head}`"),
            sparql(fluree, LEDGER, &graphs).await,
            &floor,
        );
        compare(
            &format!("SPARQL union pragma `{head}`"),
            sparql(fluree, LEDGER, &select(head, "", true)).await,
            &floor,
        );
        let ledgers = select(head, &format!("FROM <{LEDGER_A}> FROM <{LEDGER_B}>"), false);
        compare(
            &format!("SPARQL FROM two ledgers `{head}`"),
            connection_sparql(fluree, &ledgers).await,
            &floor,
        );
    }
    let where_ = json!({
        "@id": "?s",
        "ex:worksFor": {"@id": "ex:acme", "@annotation": {"ex:role": "?role"}}
    });
    let query = |from: JsonValue| json!({"@context": {"ex": EX}, "from": from, "select": ["?s", "?role"], "where": where_});
    let floor = jsonld(fluree, &query(json!(FLOOR))).await;
    compare(
        "JSON-LD from g1 g2",
        jsonld(
            fluree,
            &query(json!([format!("{LEDGER}#{G1}"), format!("{LEDGER}#{G2}")])),
        )
        .await,
        &floor,
    );
    compare(
        "JSON-LD from two ledgers",
        jsonld(fluree, &query(json!([LEDGER_A, LEDGER_B]))).await,
        &floor,
    );
}

#[tokio::test(flavor = "current_thread")]
async fn an_annotated_edge_two_graphs_hold_is_one_solution_of_their_union() {
    let want_rows: Vec<String> = WANT.iter().map(|r| (*r).to_string()).collect();
    let mut failures = Vec::new();
    for indexed in [false, true] {
        let fluree = build(indexed).await;
        let state = if indexed { "indexed" } else { "novelty" };
        // The floor gives the hand answer.
        let floor = sparql(
            &fluree,
            FLOOR,
            &format!("PREFIX ex: <{EX}>\nSELECT ?s ?role WHERE {{ {BODY} }}"),
        )
        .await;
        assert_eq!(floor, want_rows, "{state}: the floor");
        for disabled in [false, true] {
            set_fast_paths_disabled(disabled);
            let lane = if disabled {
                "fast paths off"
            } else {
                "fast paths on"
            };
            check(&fluree, &format!("{state}, {lane}"), &mut failures).await;
        }
        set_fast_paths_disabled(false);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
