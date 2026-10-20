//! The cyclic join over one graph keeps each solution's multiplicity: a pair
//! stored twice (two list items with the same value) is joined twice, in
//! whatever position of the cycle it lands. The lane once collapsed it on
//! the closing edge, where both endpoints are bound, while the subject and
//! object scans of the other edges kept it, so a solution came back once or
//! twice depending on which edge closed the cycle.
//!
//! The oracle is the general plan with the lane switched off
//! (`FLUREE_CYCLIC_BGP=0`); the fast-path kill switch can't see this, since
//! the one-graph lane doesn't sit behind it. Both answer to hand-written
//! rows, and every lane run must stamp `cyclic_bgp` `proceed`, so a case
//! can't pass by falling back.
//!
//! Over a default graph of several graphs, the union variant keeps each
//! edge's facts (AJ-66): a fact two members both hold once, two list items
//! with one value twice, a plain fact and a list item with one value twice.
//! There the oracle is the general plan under the fast-path kill switch, which
//! the union variant sits behind, and every lane run must stamp
//! `cyclic_bgp_union` `proceed`.
//!
//! Env and switch mutation live in ONE test fn (and this file is its own test
//! binary) so parallel test threads can't race on process-global state.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{Fluree, FlureeBuilder, GraphDb, LedgerHandle, ReindexOptions};
use serde_json::{json, Value as JsonValue};
use support::span_capture::{init_test_tracing, SpanStore};

const EX: &str = "http://example.org/";
const PREFIXES: &str = "PREFIX ex: <http://example.org/> ";

/// Cycles whose edges include a pair stored twice, as list items 0 and 1:
/// - over `ex:knows`, `m → n` twice, closed by `n → o`, `o → m`; and a clean
///   triangle k1 → k2 → k3. Each rotation of the first puts the pair in a
///   different position of the pattern.
/// - over `ex:p`, `ex:q`, `ex:r`, a triangle with the pair on `p` (a), on
///   `q` (b), on `r` (c), and one with none (d).
/// - a square over `ex:e1`…`ex:e4` whose `e4` pair is stored twice.
fn cycles() -> JsonValue {
    let twice = |id: &str| json!({"@list": [{"@id": id}, {"@id": id}]});
    json!({
        "@context": {"ex": EX},
        "@graph": [
            {"@id": "ex:m", "ex:knows": twice("ex:n")},
            {"@id": "ex:n", "ex:knows": {"@id": "ex:o"}},
            {"@id": "ex:o", "ex:knows": {"@id": "ex:m"}},
            {"@id": "ex:k1", "ex:knows": {"@id": "ex:k2"}},
            {"@id": "ex:k2", "ex:knows": {"@id": "ex:k3"}},
            {"@id": "ex:k3", "ex:knows": {"@id": "ex:k1"}},

            {"@id": "ex:a1", "ex:p": twice("ex:a2")},
            {"@id": "ex:a2", "ex:q": {"@id": "ex:a3"}},
            {"@id": "ex:a3", "ex:r": {"@id": "ex:a1"}},
            {"@id": "ex:b1", "ex:p": {"@id": "ex:b2"}},
            {"@id": "ex:b2", "ex:q": twice("ex:b3")},
            {"@id": "ex:b3", "ex:r": {"@id": "ex:b1"}},
            {"@id": "ex:c1", "ex:p": {"@id": "ex:c2"}},
            {"@id": "ex:c2", "ex:q": {"@id": "ex:c3"}},
            {"@id": "ex:c3", "ex:r": twice("ex:c1")},
            {"@id": "ex:d1", "ex:p": {"@id": "ex:d2"}},
            {"@id": "ex:d2", "ex:q": {"@id": "ex:d3"}},
            {"@id": "ex:d3", "ex:r": {"@id": "ex:d1"}},

            {"@id": "ex:w1", "ex:e1": {"@id": "ex:x1"}},
            {"@id": "ex:x1", "ex:e2": {"@id": "ex:y1"}},
            {"@id": "ex:y1", "ex:e3": {"@id": "ex:z1"}},
            {"@id": "ex:z1", "ex:e4": twice("ex:w1")}
        ]
    })
}

/// Edges on every predicate above that close no cycle: the index the
/// novelty state writes [`cycles`] over, so each predicate has a base id
/// (a predicate only novelty holds makes the lane stand down).
fn base_edges() -> JsonValue {
    json!({
        "@context": {"ex": EX},
        "@graph": [{
            "@id": "ex:base",
            "ex:knows": {"@id": "ex:base2"}, "ex:p": {"@id": "ex:base2"},
            "ex:q": {"@id": "ex:base2"}, "ex:r": {"@id": "ex:base2"},
            "ex:e1": {"@id": "ex:base2"}, "ex:e2": {"@id": "ex:base2"},
            "ex:e3": {"@id": "ex:base2"}, "ex:e4": {"@id": "ex:base2"}
        }]
    })
}

/// `urn:g1` and `urn:g2`, whose union keeps facts by identity:
/// - triangle m → n → o: `m → n` as list items 0 and 1 in both graphs, the
///   same two facts, so each rotation twice;
/// - triangle u → v → w: `u → v` plain in g1 and a list item in g2, two
///   facts with one value, so each rotation twice;
/// - triangle x → y → z: the same plain facts in both graphs, so once.
fn union_graphs() -> [JsonValue; 2] {
    let twice = |id: &str| json!({"@list": [{"@id": id}, {"@id": id}]});
    let edge = |s: &str, o: &str| json!({"@id": s, "ex:knows": {"@id": o}});
    let xyz = [
        edge("ex:x", "ex:y"),
        edge("ex:y", "ex:z"),
        edge("ex:z", "ex:x"),
    ];
    let mut g1 = vec![
        json!({"@id": "ex:m", "ex:knows": twice("ex:n")}),
        edge("ex:n", "ex:o"),
        edge("ex:o", "ex:m"),
        edge("ex:u", "ex:v"),
        edge("ex:v", "ex:w"),
        edge("ex:w", "ex:u"),
    ];
    g1.extend(xyz.clone());
    let mut g2 = vec![
        json!({"@id": "ex:m", "ex:knows": twice("ex:n")}),
        json!({"@id": "ex:u", "ex:knows": {"@list": [{"@id": "ex:v"}]}}),
    ];
    g2.extend(xyz);
    [
        json!({"@context": {"ex": EX}, "@id": "urn:g1", "@graph": g1}),
        json!({"@context": {"ex": EX}, "@id": "urn:g2", "@graph": g2}),
    ]
}

/// A knows edge that closes no cycle in each of the default graph, g1 and
/// g2: the index the novelty state writes [`union_graphs`] over, so both
/// graphs and the predicate are in it.
fn union_base_edges() -> [JsonValue; 3] {
    let edge = json!([{"@id": "ex:base", "ex:knows": {"@id": "ex:base2"}}]);
    [
        json!({"@context": {"ex": EX}, "@graph": edge.clone()}),
        json!({"@context": {"ex": EX}, "@id": "urn:g1", "@graph": edge.clone()}),
        json!({"@context": {"ex": EX}, "@id": "urn:g2", "@graph": edge}),
    ]
}

#[derive(Clone, Copy, Debug)]
enum State {
    /// [`cycles`] indexed, no novelty.
    Indexed,
    /// [`base_edges`] indexed, [`cycles`] written after it: novelty.
    Novelty,
}

async fn insert(fluree: &Fluree, handle: &LedgerHandle, doc: &JsonValue) {
    fluree
        .stage(handle)
        .insert(doc)
        .execute()
        .await
        .expect("insert");
}

async fn index(fluree: &Fluree, ledger_id: &str) -> LedgerHandle {
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("index");
    fluree.disconnect_ledger(ledger_id).await;
    fluree.ledger_cached(ledger_id).await.expect("reload")
}

async fn union_ledger(state: State) -> (Fluree, LedgerHandle) {
    let fluree = FlureeBuilder::memory().without_indexing().build_memory();
    let ledger_id = "it/cyclic-multiplicity-union:main";
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
    let mut handle = fluree.ledger_cached(ledger_id).await.expect("cache");
    if matches!(state, State::Novelty) {
        for doc in union_base_edges() {
            insert(&fluree, &handle, &doc).await;
        }
        handle = index(&fluree, ledger_id).await;
    }
    for doc in union_graphs() {
        insert(&fluree, &handle, &doc).await;
    }
    if matches!(state, State::Indexed) {
        handle = index(&fluree, ledger_id).await;
    }
    (fluree, handle)
}

async fn ledger(state: State) -> (Fluree, LedgerHandle) {
    let fluree = FlureeBuilder::memory().without_indexing().build_memory();
    let ledger_id = "it/cyclic-multiplicity:main";
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
    let handle = fluree.ledger_cached(ledger_id).await.expect("cache");
    let handle = match state {
        State::Indexed => {
            insert(&fluree, &handle, &cycles()).await;
            index(&fluree, ledger_id).await
        }
        State::Novelty => {
            insert(&fluree, &handle, &base_edges()).await;
            let handle = index(&fluree, ledger_id).await;
            insert(&fluree, &handle, &cycles()).await;
            handle
        }
    };
    let view = handle.snapshot().await;
    assert!(view.binary_store.is_some(), "{state:?}: indexed");
    assert_eq!(
        view.novelty.is_empty(),
        matches!(state, State::Indexed),
        "{state:?}: novelty"
    );
    (fluree, handle)
}

type Rows = Vec<Vec<String>>;

fn short(value: &str) -> String {
    value
        .strip_prefix(EX)
        .or_else(|| value.strip_prefix("ex:"))
        .unwrap_or(value)
        .to_string()
}

fn sparql_rows(result: &JsonValue, vars: &[&str]) -> Rows {
    let mut out: Rows = result["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("SPARQL results: {result}"))
        .iter()
        .map(|b| {
            vars.iter()
                .map(|v| short(b[*v]["value"].as_str().unwrap_or("-")))
                .collect()
        })
        .collect();
    out.sort();
    out
}

fn jsonld_rows(result: &JsonValue) -> Rows {
    let value = |v: &JsonValue| match v {
        JsonValue::String(s) => short(s),
        JsonValue::Object(o) => o
            .get("@id")
            .and_then(JsonValue::as_str)
            .map_or_else(|| v.to_string(), short),
        other => other.to_string(),
    };
    let mut out: Rows = result
        .as_array()
        .unwrap_or_else(|| panic!("JSON-LD rows: {result}"))
        .iter()
        .map(|row| match row {
            JsonValue::Array(cols) => cols.iter().map(value).collect(),
            single => vec![value(single)],
        })
        .collect();
    out.sort();
    out
}

/// `rows`, each `times` times, sorted.
fn times(rows: &[(&[&str], usize)]) -> Rows {
    let mut out: Rows = rows
        .iter()
        .flat_map(|(row, n)| {
            std::iter::repeat_n(row.iter().map(|s| (*s).to_string()).collect(), *n)
        })
        .collect();
    out.sort();
    out
}

fn stamps(store: &SpanStore, site: &str) -> Vec<String> {
    store
        .all_events()
        .iter()
        .filter(|e| e.message() == "fast-path outcome")
        .filter(|e| e.fields.get("site").map(String::as_str) == Some(site))
        .filter_map(|e| e.fields.get("outcome").cloned())
        .collect()
}

#[derive(Clone, Copy)]
enum Surface {
    Sparql,
    Cypher,
}

struct Case {
    label: &'static str,
    surface: Surface,
    query: String,
    vars: &'static [&'static str],
    want: Rows,
}

fn cases() -> Vec<Case> {
    let knows = times(&[
        (&["m", "n", "o"], 2),
        (&["n", "o", "m"], 2),
        (&["o", "m", "n"], 2),
        (&["k1", "k2", "k3"], 1),
        (&["k2", "k3", "k1"], 1),
        (&["k3", "k1", "k2"], 1),
    ]);
    let pqr = times(&[
        (&["a1", "a2", "a3"], 2),
        (&["b1", "b2", "b3"], 2),
        (&["c1", "c2", "c3"], 2),
        (&["d1", "d2", "d3"], 1),
    ]);
    let cypher = |a: &str, b: &str, c: &str| {
        format!("MATCH (x)-[:`{EX}{a}`]->(y)-[:`{EX}{b}`]->(z)-[:`{EX}{c}`]->(x) RETURN x, y, z")
    };
    vec![
        Case {
            label: "knows triangle, the pair in each rotation's position",
            surface: Surface::Sparql,
            query: format!(
                "{PREFIXES}SELECT ?x ?y ?z WHERE {{ ?x ex:knows ?y . ?y ex:knows ?z . ?z ex:knows ?x }}"
            ),
            vars: &["x", "y", "z"],
            want: knows.clone(),
        },
        Case {
            label: "p/q/r triangles, the pair on p, on q, on r",
            surface: Surface::Sparql,
            query: format!(
                "{PREFIXES}SELECT ?x ?y ?z WHERE {{ ?x ex:p ?y . ?y ex:q ?z . ?z ex:r ?x }}"
            ),
            vars: &["x", "y", "z"],
            want: pqr.clone(),
        },
        Case {
            label: "square, the pair on its last edge",
            surface: Surface::Sparql,
            query: format!(
                "{PREFIXES}SELECT ?w ?x ?y ?z \
                 WHERE {{ ?w ex:e1 ?x . ?x ex:e2 ?y . ?y ex:e3 ?z . ?z ex:e4 ?w }}"
            ),
            vars: &["w", "x", "y", "z"],
            want: times(&[(&["w1", "x1", "y1", "z1"], 2)]),
        },
        Case {
            label: "Cypher: knows triangle",
            surface: Surface::Cypher,
            query: cypher("knows", "knows", "knows"),
            vars: &[],
            want: knows,
        },
        Case {
            label: "Cypher: p/q/r triangles",
            surface: Surface::Cypher,
            query: cypher("p", "q", "r"),
            vars: &[],
            want: pqr,
        },
    ]
}

async fn answer(fluree: &Fluree, handle: &LedgerHandle, case: &Case) -> Result<Rows, String> {
    let db = GraphDb::from_ledger_state(&handle.snapshot().await.to_ledger_state());
    match case.surface {
        Surface::Sparql => db
            .query(fluree)
            .sparql(&case.query)
            .execute_formatted()
            .await
            .map(|json| sparql_rows(&json, case.vars))
            .map_err(|e| e.to_string()),
        Surface::Cypher => {
            let result = fluree
                .query_cypher(&db, &case.query)
                .await
                .map_err(|e| e.to_string())?;
            result
                .to_jsonld_async(db.as_graph_db_ref())
                .await
                .map(|json| jsonld_rows(&json))
                .map_err(|e| e.to_string())
        }
    }
}

/// The union cases: the triangle over g1 and g2, by `FROM` and as the union
/// default graph (whose default graph adds only the novelty state's base
/// edge, which closes no cycle).
fn union_cases() -> Vec<Case> {
    let want = times(&[
        (&["m", "n", "o"], 2),
        (&["n", "o", "m"], 2),
        (&["o", "m", "n"], 2),
        (&["u", "v", "w"], 2),
        (&["v", "w", "u"], 2),
        (&["w", "u", "v"], 2),
        (&["x", "y", "z"], 1),
        (&["y", "z", "x"], 1),
        (&["z", "x", "y"], 1),
    ]);
    let tri = |prologue: &str, ds: &str| {
        format!(
            "{prologue}{PREFIXES}SELECT ?x ?y ?z {ds} \
             WHERE {{ ?x ex:knows ?y . ?y ex:knows ?z . ?z ex:knows ?x }}"
        )
    };
    vec![
        Case {
            label: "union: FROM g1 FROM g2",
            surface: Surface::Sparql,
            query: tri("", "FROM <urn:g1> FROM <urn:g2>"),
            vars: &["x", "y", "z"],
            want: want.clone(),
        },
        Case {
            label: "union: the union default graph",
            surface: Surface::Sparql,
            query: tri("# PRAGMA union-default-graph: true\n", ""),
            vars: &["x", "y", "z"],
            want,
        },
    ]
}

/// Turns the process-wide fast-path switch back off on every exit path.
struct FastPathsBackOn;
impl Drop for FastPathsBackOn {
    fn drop(&mut self) {
        fluree_db_api::set_fast_paths_disabled(false);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cyclic_join_keeps_the_multiplicity_of_a_pair_stored_twice() {
    std::env::remove_var("FLUREE_CYCLIC_BGP");
    let _back_on = FastPathsBackOn;
    let mut failures: Vec<String> = Vec::new();
    for state in [State::Indexed, State::Novelty] {
        let (fluree, handle) = ledger(state).await;
        for case in cases() {
            let label = format!("{state:?} / {}", case.label);

            let (store, guard) = init_test_tracing();
            let lane = answer(&fluree, &handle, &case).await;
            drop(guard);
            let ran = stamps(&store, "cyclic_bgp");
            if ran != ["proceed"] {
                failures.push(format!(
                    "{label}: `cyclic_bgp` stamps {ran:?}, want [proceed]"
                ));
            }

            std::env::set_var("FLUREE_CYCLIC_BGP", "0");
            let generic = answer(&fluree, &handle, &case).await;
            std::env::remove_var("FLUREE_CYCLIC_BGP");

            for (which, got) in [("the lane", lane), ("FLUREE_CYCLIC_BGP=0", generic)] {
                match got {
                    Ok(rows) if rows == case.want => {}
                    Ok(rows) => failures.push(format!(
                        "{label}: {which}:\n    got  {rows:?}\n    want {:?}",
                        case.want
                    )),
                    Err(e) => failures.push(format!("{label}: {which}: failed: {e}")),
                }
            }
        }

        let (fluree, handle) = union_ledger(state).await;
        for case in union_cases() {
            let label = format!("{state:?} / {}", case.label);

            let (store, guard) = init_test_tracing();
            let lane = answer(&fluree, &handle, &case).await;
            drop(guard);
            let ran = stamps(&store, "cyclic_bgp_union");
            if ran != ["proceed"] {
                failures.push(format!(
                    "{label}: `cyclic_bgp_union` stamps {ran:?}, want [proceed]"
                ));
            }

            fluree_db_api::set_fast_paths_disabled(true);
            let generic = answer(&fluree, &handle, &case).await;
            fluree_db_api::set_fast_paths_disabled(false);

            for (which, got) in [("the lane", lane), ("the general plan", generic)] {
                match got {
                    Ok(rows) if rows == case.want => {}
                    Ok(rows) => failures.push(format!(
                        "{label}: {which}:\n    got  {rows:?}\n    want {:?}",
                        case.want
                    )),
                    Err(e) => failures.push(format!("{label}: {which}: failed: {e}")),
                }
            }
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}
