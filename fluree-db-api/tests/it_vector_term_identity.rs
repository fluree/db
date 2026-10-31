//! Vector literals compare by value on every equality surface.
//!
//! On an indexed ledger a vector's object key is a handle into its
//! predicate's vector arena: handles start at 0 for each predicate, and the
//! arena gives every stored fact its own handle, so one vector on two
//! subjects has two. Neither direction may leak into a result: distinct
//! vectors whose handles coincide stay apart, and one vector under two
//! handles is one term. This pins DISTINCT, GROUP BY (hashed and the
//! partitioned lane), COUNT(DISTINCT), joins, MINUS, EXISTS and VALUES, on
//! SPARQL and JSON-LD, on an indexed ledger, in novelty, and on an indexed
//! ledger with novelty on top, with the fast paths on and off. The oracle is
//! the hand-derived answer, not agreement between the lanes, which shared the
//! wrong answer.
//!
//! The fast paths that key objects on the raw index key decline a predicate
//! (or graph) that holds vectors. Each decline is stamped and paired with a
//! canary on a string predicate of the same ledger that the fast path serves.

#![cfg(feature = "native")]

use crate::support::span_capture::{init_test_tracing, SpanStore};
use crate::support::FAST_PATH_SWITCH;
use fluree_db_api::{set_fast_paths_disabled, Fluree, FlureeBuilder, GraphSnapshot};
use serde_json::{json, Value};
use std::io::Write;
use tempfile::TempDir;

/// Distinct vectors that share a handle (each predicate's first), one vector
/// stored twice under one predicate, and two predicates on the same subjects
/// that agree under `ex:f` and differ under `ex:g`. The strings are the
/// canaries' data.
const FIXTURE: &str = r#"@prefix ex: <http://example.org/> .
@prefix f: <https://ns.flur.ee/db#> .
ex:a ex:v1 "[1.0, 0.0]"^^f:embeddingVector .
ex:b ex:v2 "[0.0, 1.0]"^^f:embeddingVector .
ex:c ex:w "[0.5, 0.5]"^^f:embeddingVector ; ex:label "x" .
ex:d ex:w "[0.5, 0.5]"^^f:embeddingVector ; ex:label "x" .
ex:e ex:w "[0.25, 0.75]"^^f:embeddingVector ; ex:label "y" .
ex:f ex:j1 "[0.75, 0.25]"^^f:embeddingVector ; ex:j2 "[0.75, 0.25]"^^f:embeddingVector ;
    ex:k1 "s" ; ex:k2 "s" ; ex:link ex:f .
ex:g ex:j1 "[0.125, 0.875]"^^f:embeddingVector ; ex:j2 "[0.875, 0.125]"^^f:embeddingVector ;
    ex:k1 "t" ; ex:k2 "u" ; ex:link ex:g .
"#;

/// Written on top of the indexed fixture: the repeated vector on a third
/// subject, now held in novelty.
const NOVELTY_EXTRA: &str = r#"@prefix ex: <http://example.org/> .
@prefix f: <https://ns.flur.ee/db#> .
ex:h ex:w "[0.5, 0.5]"^^f:embeddingVector .
"#;

const EX: &str = "http://example.org/";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Condition {
    /// Bulk-imported, so every fact is in the index.
    Indexed,
    /// Never indexed: every fact is in novelty.
    Novelty,
    /// Indexed, plus [`NOVELTY_EXTRA`] in novelty.
    IndexedPlusNovelty,
}

/// One SPARQL query and its rows, each rendered `var=value` in variable
/// order and joined by `,`; IRIs drop the `ex:` namespace.
struct Case {
    name: &'static str,
    body: &'static str,
    rows: &'static [&'static str],
}

fn sparql_cases() -> Vec<Case> {
    vec![
        Case {
            name: "DISTINCT over two predicates' first vectors",
            body: "SELECT DISTINCT ?v WHERE { { ?s ex:v1 ?v } UNION { ?s ex:v2 ?v } }",
            rows: &["v=[0.0,1.0]", "v=[1.0,0.0]"],
        },
        Case {
            name: "DISTINCT over one vector stored twice",
            body: "SELECT DISTINCT ?v WHERE { ?s ex:w ?v }",
            rows: &["v=[0.25,0.75]", "v=[0.5,0.5]"],
        },
        Case {
            name: "GROUP BY over two predicates' first vectors",
            body: "SELECT ?v (COUNT(*) AS ?n) WHERE { { ?s ex:v1 ?v } UNION { ?s ex:v2 ?v } } \
                   GROUP BY ?v",
            rows: &["n=1,v=[0.0,1.0]", "n=1,v=[1.0,0.0]"],
        },
        Case {
            name: "GROUP BY over one vector stored twice (partitioned lane)",
            body: "SELECT ?v (COUNT(?s) AS ?n) WHERE { ?s ex:w ?v } GROUP BY ?v",
            rows: &["n=1,v=[0.25,0.75]", "n=2,v=[0.5,0.5]"],
        },
        Case {
            name: "GROUP BY count top-k over one vector stored twice",
            body: "SELECT ?v (COUNT(?s) AS ?n) WHERE { ?s ex:w ?v } GROUP BY ?v \
                   ORDER BY DESC(?n) LIMIT 5",
            rows: &["n=1,v=[0.25,0.75]", "n=2,v=[0.5,0.5]"],
        },
        Case {
            name: "COUNT(DISTINCT) over two predicates' first vectors",
            body: "SELECT (COUNT(DISTINCT ?v) AS ?n) \
                   WHERE { { ?s ex:v1 ?v } UNION { ?s ex:v2 ?v } }",
            rows: &["n=2"],
        },
        Case {
            name: "COUNT(DISTINCT) over one vector stored twice",
            body: "SELECT (COUNT(DISTINCT ?v) AS ?n) WHERE { ?s ex:w ?v }",
            rows: &["n=2"],
        },
        Case {
            name: "COUNT(DISTINCT) over every object of the graph",
            // Seven vectors, five strings, two IRIs.
            body: "SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s ?p ?o }",
            rows: &["n=14"],
        },
        Case {
            name: "join on one vector stored twice",
            body: "SELECT ?a ?b WHERE { ?a ex:w ?v . ?b ex:w ?v }",
            rows: &["a=c,b=c", "a=c,b=d", "a=d,b=c", "a=d,b=d", "a=e,b=e"],
        },
        Case {
            name: "join on a vector across two predicates",
            body: "SELECT ?s ?t WHERE { ?s ex:j1 ?v . ?t ex:j2 ?v }",
            rows: &["s=f,t=f"],
        },
        Case {
            name: "join count on a subject and a vector across two predicates",
            body: "SELECT (COUNT(*) AS ?n) WHERE { ?s ex:j1 ?o . ?s ex:j2 ?o }",
            rows: &["n=1"],
        },
        Case {
            name: "cycle through a vector across two predicates",
            body: "SELECT ?a ?b WHERE { ?a ex:j1 ?v . ?b ex:j2 ?v . ?a ex:link ?b }",
            rows: &["a=f,b=f"],
        },
        Case {
            name: "MINUS on a vector across two predicates",
            body: "SELECT ?s WHERE { ?s ex:j1 ?v MINUS { ?t ex:j2 ?v } }",
            rows: &["s=g"],
        },
        Case {
            name: "FILTER EXISTS on a vector across two predicates",
            body: "SELECT ?s WHERE { ?s ex:j1 ?v FILTER EXISTS { ?t ex:j2 ?v } }",
            rows: &["s=f"],
        },
        Case {
            name: "VALUES joined with one vector stored twice",
            body: "SELECT ?s WHERE { VALUES ?v { \"[0.5, 0.5]\"^^f:embeddingVector } \
                   ?s ex:w ?v }",
            rows: &["s=c", "s=d"],
        },
    ]
}

/// The rows [`NOVELTY_EXTRA`] changes on [`Condition::IndexedPlusNovelty`].
fn with_novelty_extra(case: &Case) -> Vec<String> {
    let rows: &[&str] = match case.name {
        "GROUP BY over one vector stored twice (partitioned lane)"
        | "GROUP BY count top-k over one vector stored twice" => {
            &["n=1,v=[0.25,0.75]", "n=3,v=[0.5,0.5]"]
        }
        "join on one vector stored twice" => &[
            "a=c,b=c", "a=c,b=d", "a=c,b=h", "a=d,b=c", "a=d,b=d", "a=d,b=h", "a=e,b=e", "a=h,b=c",
            "a=h,b=d", "a=h,b=h",
        ],
        "VALUES joined with one vector stored twice" => &["s=c", "s=d", "s=h"],
        _ => case.rows,
    };
    rows.iter().map(ToString::to_string).collect()
}

/// One JSON-LD query and its rows, each serialized and sorted.
fn jsonld_cases() -> Vec<(&'static str, Value, Vec<Value>)> {
    let ctx = json!({"ex": EX});
    vec![
        (
            "JSON-LD selectDistinct over two predicates' first vectors",
            json!({
                "@context": ctx,
                "selectDistinct": ["?v"],
                "where": [["union", {"@id": "?s", "ex:v1": "?v"}, {"@id": "?s", "ex:v2": "?v"}]]
            }),
            vec![json!([[0.0, 1.0]]), json!([[1.0, 0.0]])],
        ),
        (
            "JSON-LD selectDistinct over one vector stored twice",
            json!({
                "@context": ctx,
                "selectDistinct": ["?v"],
                "where": {"@id": "?s", "ex:w": "?v"}
            }),
            vec![json!([[0.25, 0.75]]), json!([[0.5, 0.5]])],
        ),
        (
            "JSON-LD groupBy over one vector stored twice",
            json!({
                "@context": ctx,
                "select": ["?v", "(as (count ?s) ?n)"],
                "where": {"@id": "?s", "ex:w": "?v"},
                "groupBy": ["?v"]
            }),
            vec![json!([[0.25, 0.75], 1]), json!([[0.5, 0.5], 2])],
        ),
        (
            "JSON-LD count-distinct over two predicates' first vectors",
            json!({
                "@context": ctx,
                "select": ["(as (count-distinct ?v) ?n)"],
                "where": [["union", {"@id": "?s", "ex:v1": "?v"}, {"@id": "?s", "ex:v2": "?v"}]]
            }),
            vec![json!([2])],
        ),
        (
            "JSON-LD join on one vector stored twice",
            json!({
                "@context": ctx,
                "select": ["?a", "?b"],
                "where": [{"@id": "?a", "ex:w": "?v"}, {"@id": "?b", "ex:w": "?v"}]
            }),
            vec![
                json!(["ex:c", "ex:c"]),
                json!(["ex:c", "ex:d"]),
                json!(["ex:d", "ex:c"]),
                json!(["ex:d", "ex:d"]),
                json!(["ex:e", "ex:e"]),
            ],
        ),
        (
            "JSON-LD join on a vector across two predicates",
            json!({
                "@context": ctx,
                "select": ["?s", "?t"],
                "where": [{"@id": "?s", "ex:j1": "?v"}, {"@id": "?t", "ex:j2": "?v"}]
            }),
            vec![json!(["ex:f", "ex:f"])],
        ),
    ]
}

/// A Fluree holding `ledger_id` with [`FIXTURE`] under `condition`. The
/// directories live as long as the returned handles.
async fn ledger(condition: Condition, ledger_id: &str) -> (Vec<TempDir>, Fluree) {
    let mut dirs = Vec::new();
    let fluree = match condition {
        Condition::Novelty => {
            let fluree = FlureeBuilder::memory().without_indexing().build_memory();
            fluree.create_ledger(ledger_id).await.expect("create");
            fluree
                .graph(ledger_id)
                .transact()
                .insert_turtle(FIXTURE)
                .commit()
                .await
                .expect("insert fixture");
            fluree
        }
        Condition::Indexed | Condition::IndexedPlusNovelty => {
            let db_dir = TempDir::new().expect("db tmpdir");
            let data_dir = TempDir::new().expect("data tmpdir");
            std::fs::File::create(data_dir.path().join("00-data.ttl"))
                .expect("create ttl")
                .write_all(FIXTURE.as_bytes())
                .expect("write ttl");
            // No background indexing, so the extra facts stay in novelty.
            let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
                .without_indexing()
                .build()
                .expect("build file-backed Fluree");
            fluree
                .create(ledger_id)
                .import(data_dir.path())
                .threads(1)
                .memory_budget_mb(256)
                .cleanup(false)
                .execute()
                .await
                .expect("import");
            if condition == Condition::IndexedPlusNovelty {
                fluree
                    .graph(ledger_id)
                    .transact()
                    .insert_turtle(NOVELTY_EXTRA)
                    .commit()
                    .await
                    .expect("insert novelty");
            }
            dirs.push(db_dir);
            dirs.push(data_dir);
            fluree
        }
    };
    (dirs, fluree)
}

/// A binding's value with the `ex:` namespace dropped.
fn short(value: &Value) -> String {
    let v = value["value"].as_str().unwrap_or_default();
    v.strip_prefix(EX).unwrap_or(v).to_string()
}

/// SPARQL JSON results as sorted `var=value,…` rows, variables in `vars`
/// order.
fn sparql_rows(results: &Value) -> Vec<String> {
    let vars: Vec<String> = results["head"]["vars"]
        .as_array()
        .expect("head.vars")
        .iter()
        .map(|v| v.as_str().expect("var").to_string())
        .collect();
    let mut rows: Vec<String> = results["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|row| {
            vars.iter()
                .map(|v| format!("{v}={}", short(&row[v])))
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect();
    rows.sort();
    rows
}

async fn sparql(db: &GraphSnapshot<'_>, body: &str) -> Vec<String> {
    let query =
        format!("PREFIX ex: <http://example.org/>\nPREFIX f: <https://ns.flur.ee/db#>\n{body}");
    let results = db
        .query()
        .sparql(&query)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{e}\n{query}"));
    sparql_rows(&results)
}

async fn jsonld(db: &GraphSnapshot<'_>, query: &Value) -> Vec<Value> {
    let results = db
        .query()
        .jsonld(query)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{e}\n{query}"));
    let mut rows: Vec<Value> = results.as_array().expect("rows").clone();
    rows.sort_by_key(ToString::to_string);
    rows
}

/// Every query on `db`, with the fast paths on when `fast` and off
/// otherwise. A wrong answer is collected, not raised, so one run reports
/// them all.
async fn run_all(
    db: &GraphSnapshot<'_>,
    condition: Condition,
    fast: bool,
    failures: &mut Vec<String>,
) {
    let lane = if fast {
        "fast paths on"
    } else {
        "fast paths off"
    };
    for case in sparql_cases() {
        let want: Vec<String> = match condition {
            Condition::IndexedPlusNovelty => with_novelty_extra(&case),
            _ => case.rows.iter().map(ToString::to_string).collect(),
        };
        let got = sparql(db, case.body).await;
        if got != want {
            failures.push(format!(
                "{condition:?}, {lane}: {}\n  want {want:?}\n  got  {got:?}",
                case.name
            ));
        }
    }
    if condition == Condition::IndexedPlusNovelty {
        // The extra fact changes these answers; the SPARQL twins cover it.
        return;
    }
    for (name, query, mut want) in jsonld_cases() {
        want.sort_by_key(ToString::to_string);
        let got = jsonld(db, &query).await;
        if got != want {
            failures.push(format!(
                "{condition:?}, {lane}: {name}\n  want {want:?}\n  got  {got:?}"
            ));
        }
    }
}

/// Run every query with the fast paths on, then off. The switch is
/// process-wide: the runs with it on hold the read side of
/// [`FAST_PATH_SWITCH`] and the runs with it off the write side.
async fn answers_in_both_lanes(condition: Condition, ledger_id: &str) {
    let (_dirs, fluree) = ledger(condition, ledger_id).await;
    let db = fluree.graph(ledger_id).load().await.expect("load");
    let mut failures = Vec::new();
    {
        let _switch = FAST_PATH_SWITCH.read().await;
        run_all(&db, condition, true, &mut failures).await;
    }
    {
        let _switch = FAST_PATH_SWITCH.write().await;
        set_fast_paths_disabled(true);
        let _restore = FastPathsGuard;
        run_all(&db, condition, false, &mut failures).await;
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Restores the fast paths when a run with them off ends, panicking or not.
struct FastPathsGuard;

impl Drop for FastPathsGuard {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

#[tokio::test]
async fn vectors_compare_by_value_on_an_indexed_ledger() {
    answers_in_both_lanes(Condition::Indexed, "vector-identity/indexed:main").await;
}

#[tokio::test]
async fn vectors_compare_by_value_in_novelty() {
    answers_in_both_lanes(Condition::Novelty, "vector-identity/novelty:main").await;
}

#[tokio::test]
async fn vectors_compare_by_value_across_index_and_novelty() {
    answers_in_both_lanes(
        Condition::IndexedPlusNovelty,
        "vector-identity/indexed-plus-novelty:main",
    )
    .await;
}

/// The fast-path sites that stamped an outcome starting with `outcome` since
/// event `before`.
fn stamped(store: &SpanStore, before: usize, outcome: &str) -> Vec<String> {
    store.find_events("fast-path outcome")[before..]
        .iter()
        .filter(|e| {
            e.fields
                .get("outcome")
                .is_some_and(|o| o.starts_with(outcome))
        })
        .filter_map(|e| e.fields.get("site").cloned())
        .collect()
}

/// Whether `site` served the query: it proceeded, and did not decline at
/// open after the plan chose it.
fn served(store: &SpanStore, before: usize, site: &str) -> bool {
    stamped(store, before, "proceed").iter().any(|s| s == site)
        && !stamped(store, before, "fallback").iter().any(|s| s == site)
}

/// Whether the partitioned GROUP BY lane ran for a query, since span `before`.
fn partitioned(store: &SpanStore, before: usize) -> bool {
    store.find_spans("group_aggregate_streaming")[before..]
        .iter()
        .any(|s| s.fields.get("partitioned").map(String::as_str) == Some("true"))
}

/// The reasons the cyclic join bailed with since event `before`.
fn cyclic_bails(store: &SpanStore, before: usize) -> Vec<String> {
    store.find_events("cyclic bgp fast path bail")[before..]
        .iter()
        .filter_map(|e| e.fields.get("reason").cloned())
        .collect()
}

/// A fast-path site, a vector query it must decline and its rows, and a
/// string canary it must serve and its rows.
type CanaryPair = (
    &'static str,
    &'static str,
    &'static [&'static str],
    &'static str,
    &'static [&'static str],
);

/// Each fast path that keys objects on their index key declines the vector
/// query and serves its string canary on the same ledger; the partitioned
/// GROUP BY lane runs for both and stamps only the vector one.
#[tokio::test(flavor = "current_thread")]
async fn object_keyed_lanes_decline_vectors_and_serve_strings() {
    let ledger_id = "vector-identity/routing:main";
    let (_dirs, fluree) = ledger(Condition::Indexed, ledger_id).await;
    let db = fluree.graph(ledger_id).load().await.expect("load");
    let _switch = FAST_PATH_SWITCH.read().await;
    let (store, _guard) = init_test_tracing();

    // (site, vector query, its rows, string canary, its rows)
    let pairs: [CanaryPair; 3] = [
        (
            "COUNT(DISTINCT)",
            "SELECT (COUNT(DISTINCT ?v) AS ?n) WHERE { ?s ex:w ?v }",
            &["n=2"],
            "SELECT (COUNT(DISTINCT ?l) AS ?n) WHERE { ?s ex:label ?l }",
            &["n=2"],
        ),
        (
            "group_by_object_count_topk",
            "SELECT ?v (COUNT(?s) AS ?n) WHERE { ?s ex:w ?v } GROUP BY ?v \
             ORDER BY DESC(?n) LIMIT 5",
            &["n=1,v=[0.25,0.75]", "n=2,v=[0.5,0.5]"],
            "SELECT ?l (COUNT(?s) AS ?n) WHERE { ?s ex:label ?l } GROUP BY ?l \
             ORDER BY DESC(?n) LIMIT 5",
            &["l=x,n=2", "l=y,n=1"],
        ),
        (
            "count-plan",
            "SELECT (COUNT(*) AS ?n) WHERE { ?s ex:j1 ?o . ?s ex:j2 ?o }",
            &["n=1"],
            "SELECT (COUNT(*) AS ?n) WHERE { ?s ex:k1 ?o . ?s ex:k2 ?o }",
            &["n=1"],
        ),
    ];
    for (site, vector_query, vector_rows, canary, canary_rows) in pairs {
        let before = store.find_events("fast-path outcome").len();
        assert_eq!(sparql(&db, canary).await, canary_rows, "{site} canary rows");
        assert!(
            served(&store, before, site),
            "{site} must serve its string canary: {:?}",
            stamped(&store, before, "")
        );
        let before = store.find_events("fast-path outcome").len();
        assert_eq!(
            sparql(&db, vector_query).await,
            vector_rows,
            "{site} vector rows"
        );
        assert!(
            !served(&store, before, site),
            "{site} must decline a predicate holding vectors: {:?}",
            stamped(&store, before, "")
        );
    }

    // The whole-graph distinct-object count: this graph holds vectors.
    let before = store.find_events("fast-path outcome").len();
    assert_eq!(
        sparql(&db, "SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s ?p ?o }").await,
        ["n=14"]
    );
    assert!(
        !served(&store, before, "distinct object COUNT"),
        "{:?}",
        stamped(&store, before, "")
    );

    // The partitioned GROUP BY lane: a string key stays on it; a vector key
    // folds by value and stamps the decline.
    let spans = store.find_spans("group_aggregate_streaming").len();
    let before = store.find_events("fast-path outcome").len();
    assert_eq!(
        sparql(
            &db,
            "SELECT ?l (COUNT(?s) AS ?n) WHERE { ?s ex:label ?l } GROUP BY ?l"
        )
        .await,
        ["l=x,n=2", "l=y,n=1"]
    );
    assert!(partitioned(&store, spans), "string GROUP BY is partitioned");
    assert!(
        !stamped(&store, before, "fallback")
            .iter()
            .any(|s| s == "partitioned GROUP BY"),
        "a string key does not decline the partitioned lane"
    );
    let spans = store.find_spans("group_aggregate_streaming").len();
    let before = store.find_events("fast-path outcome").len();
    assert_eq!(
        sparql(
            &db,
            "SELECT ?v (COUNT(?s) AS ?n) WHERE { ?s ex:w ?v } GROUP BY ?v"
        )
        .await,
        ["n=1,v=[0.25,0.75]", "n=2,v=[0.5,0.5]"]
    );
    assert!(partitioned(&store, spans), "vector GROUP BY is partitioned");
    assert!(
        stamped(&store, before, "fallback")
            .iter()
            .any(|s| s == "partitioned GROUP BY"),
        "a vector key declines the partitioned lane: {:?}",
        stamped(&store, before, "")
    );

    // The cyclic join bails on the vector's arena handle.
    let before = store.find_events("cyclic bgp fast path bail").len();
    assert_eq!(
        sparql(
            &db,
            "SELECT ?a ?b WHERE { ?a ex:j1 ?v . ?b ex:j2 ?v . ?a ex:link ?b }"
        )
        .await,
        ["a=f,b=f"]
    );
    assert!(
        cyclic_bails(&store, before)
            .iter()
            .any(|r| r == "arena-literal-object"),
        "{:?}",
        cyclic_bails(&store, before)
    );
}
