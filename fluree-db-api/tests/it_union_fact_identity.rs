//! A default graph of several graphs deduplicates on fact identity:
//! storage's identity, one fact per value and list position (#2109's
//! `FactIdentity`). Only a fact two members both hold collapses, so a union
//! with a graph that adds nothing answers as the graph alone.
//!
//! The cases are the ones where a dedup keyed on the emitted row loses the
//! fact: the term a constant object matched, and the list position (both
//! surfaces read a list's members as values, #2217). Each runs before the
//! ledger is indexed (the
//! scan reads decoded flakes) and after (it reads index rows), with fast
//! paths on and off, and every mismatch of every run is reported together.
//! The kill switch is process-wide: the runs with fast paths on hold the
//! read side of `support::FAST_PATH_SWITCH`, so no test in this binary turns
//! them off underneath, and the runs with them off hold the write side.

use fluree_db_api::{
    fast_paths_disabled, set_fast_paths_disabled, Fluree, FlureeBuilder, GraphDb, ReindexOptions,
};
use serde_json::{json, Value as JsonValue};

const LEDGER: &str = "it/union-fact-identity:main";

/// g1: a bare number under two datatypes for one subject, and a list that
/// repeats a value. gEmpty: nothing `ex:p` or `ex:q` can match. g2: the
/// list's first item and one of g1's numbers, the facts g1 holds too.
const TRIG: &str = r#"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
GRAPH <urn:g1> {
  ex:a ex:p 1 , "1.0e0"^^xsd:double .
  ex:b ex:p 1 .
  ex:d ex:q ( 5 5 7 ) .
}
GRAPH <urn:gEmpty> { ex:z ex:unrelated "x" . }
GRAPH <urn:g2> {
  ex:a ex:p 1 .
  ex:d ex:q ( 5 ) .
}
"#;

const G1: &str = "FROM <urn:g1>";
const G1_EMPTY: &str = "FROM <urn:g1> FROM <urn:gEmpty>";
const G1_G2: &str = "FROM <urn:g1> FROM <urn:g2>";

/// The query's rows, each `var=value^^datatype` joined by `;`, sorted.
async fn sparql_rows(fluree: &Fluree, body: &str, dataset: &str) -> Vec<String> {
    sparql_rows_in(fluree, LEDGER, body, dataset).await
}

/// [`sparql_rows`] over `ledger`.
async fn sparql_rows_in(fluree: &Fluree, ledger: &str, body: &str, dataset: &str) -> Vec<String> {
    let handle = fluree.ledger_cached(ledger).await.expect("ledger");
    let view = GraphDb::from_ledger_state(&handle.snapshot().await.to_ledger_state());
    let query = format!(
        "PREFIX ex: <http://example.org/>\n{}",
        body.replace("{ds}", dataset)
    );
    let json = view
        .query(fluree)
        .sparql(&query)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let mut rows: Vec<String> = json["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("{query}: {json}"))
        .iter()
        .map(|b| {
            let mut cols: Vec<String> = b
                .as_object()
                .expect("binding object")
                .iter()
                .map(|(var, v)| {
                    let value = v["value"].as_str().unwrap_or_default();
                    let value = value.rsplit('/').next().unwrap_or(value);
                    match v["datatype"].as_str() {
                        Some(dt) => format!("{var}={value}^^{}", dt.rsplit('#').next().unwrap()),
                        None => format!("{var}={value}"),
                    }
                })
                .collect();
            cols.sort();
            cols.join(";")
        })
        .collect();
    rows.sort();
    rows
}

fn want(rows: &[&str]) -> Vec<String> {
    let mut rows: Vec<String> = rows.iter().map(|r| (*r).to_string()).collect();
    rows.sort();
    rows
}

/// Every case of the probe table, plus the facts two members both hold.
async fn check_sparql(fluree: &Fluree, state: &str, failures: &mut Vec<String>) {
    let int = "integer";
    let one_a = format!("o=1^^{int};s=a");
    let one_b = format!("o=1^^{int};s=b");
    let five_d = format!("o=5^^{int};s=d");
    let cases: Vec<(&str, &str, Vec<String>)> = vec![
        // A bare number matches 1 and 1.0e0: two facts of a.
        (
            "SELECT ?s {ds} WHERE { ?s ex:p 1 }",
            G1,
            want(&["s=a", "s=a", "s=b"]),
        ),
        (
            "SELECT ?s {ds} WHERE { ?s ex:p 1 }",
            G1_EMPTY,
            want(&["s=a", "s=a", "s=b"]),
        ),
        // g2's `a p 1` is g1's.
        (
            "SELECT ?s {ds} WHERE { ?s ex:p 1 }",
            G1_G2,
            want(&["s=a", "s=a", "s=b"]),
        ),
        (
            "SELECT (COUNT(?s) AS ?c) {ds} WHERE { ?s ex:p 1 }",
            G1_EMPTY,
            want(&["c=3^^integer"]),
        ),
        (
            "SELECT ?s ?o {ds} WHERE { ?s ex:p ?o }",
            G1_EMPTY,
            want(&[&one_a, "o=1.0E0^^double;s=a", &one_b]),
        ),
        (
            "SELECT ?s ?o {ds} WHERE { ?s ex:p ?o }",
            G1_G2,
            want(&[&one_a, "o=1.0E0^^double;s=a", &one_b]),
        ),
        // A list's members are values of `ex:q` (#2217), a fact per list
        // position: g1's `d q (5 5 7)` repeats 5, two facts, and g2's
        // `d q (5)` is g1's first member.
        (
            "SELECT ?s {ds} WHERE { ?s ex:q 5 }",
            G1,
            want(&["s=d", "s=d"]),
        ),
        (
            "SELECT ?s {ds} WHERE { ?s ex:q 5 }",
            G1_EMPTY,
            want(&["s=d", "s=d"]),
        ),
        (
            "SELECT ?s {ds} WHERE { ?s ex:q 5 }",
            G1_G2,
            want(&["s=d", "s=d"]),
        ),
        (
            "SELECT (COUNT(?s) AS ?c) {ds} WHERE { ?s ex:q 5 }",
            G1_EMPTY,
            want(&["c=2^^integer"]),
        ),
        (
            "SELECT ?s ?o {ds} WHERE { ?s ex:q ?o }",
            G1_EMPTY,
            want(&[&five_d, &five_d, "o=7^^integer;s=d"]),
        ),
        (
            "SELECT ?s ?o {ds} WHERE { ?s ex:q ?o }",
            G1_G2,
            want(&[&five_d, &five_d, "o=7^^integer;s=d"]),
        ),
        // A ground pattern: one empty solution per fact it matches.
        (
            "SELECT (COUNT(*) AS ?c) {ds} WHERE { ex:a ex:p 1 }",
            G1_EMPTY,
            want(&["c=2^^integer"]),
        ),
        (
            "SELECT (COUNT(*) AS ?c) {ds} WHERE { ex:a ex:p 1 }",
            G1_G2,
            want(&["c=2^^integer"]),
        ),
    ];
    for (body, dataset, expected) in cases {
        let got = sparql_rows(fluree, body, dataset).await;
        if got != expected {
            failures.push(format!(
                "{state}: {dataset} {body}: {got:?}, want {expected:?}"
            ));
        }
        // The union with a graph that adds nothing answers as g1 alone.
        if dataset == G1_EMPTY {
            let alone = sparql_rows(fluree, body, G1).await;
            if got != alone {
                failures.push(format!(
                    "{state}: {body}: {got:?} with gEmpty, {alone:?} alone"
                ));
            }
        }
    }
}

/// The list cases in JSON-LD, through the connection: the SPARQL cases' answer
/// counts. The list repeats 5: two facts, one per list position, and g2's
/// first item is g1's first item.
async fn check_jsonld(fluree: &Fluree, state: &str, failures: &mut Vec<String>) {
    let query = |graphs: &[&str], object: JsonValue, select: &[&str]| {
        let from: Vec<String> = graphs.iter().map(|g| format!("{LEDGER}#{g}")).collect();
        json!({
            "@context": {"ex": "http://example.org/"},
            "from": from,
            "select": select,
            "where": [{"@id": "?s", "ex:q": object}]
        })
    };
    for graphs in [
        &["urn:g1"][..],
        &["urn:g1", "urn:gEmpty"],
        &["urn:g1", "urn:g2"],
    ] {
        for (q, want) in [
            (query(graphs, json!(5), &["?s"]), 2),
            (query(graphs, json!("?o"), &["?s", "?o"]), 3),
        ] {
            let result = match fluree.query_connection(&q).await {
                Ok(result) => result,
                Err(e) => {
                    failures.push(format!("{state}: JSON-LD {q} from {graphs:?}: {e}"));
                    continue;
                }
            };
            let ledger = fluree.ledger(LEDGER).await.expect("ledger");
            let json: JsonValue = result
                .to_jsonld_async(ledger.as_graph_db_ref(0))
                .await
                .expect("format");
            let rows = json.as_array().map_or(0, Vec::len);
            if rows != want {
                failures.push(format!(
                    "{state}: JSON-LD {q} from {graphs:?}: {rows} rows, want {want}: {json}"
                ));
            }
        }
    }
}

/// Every check in `state`, with fast paths on and then off.
async fn check_state(fluree: &Fluree, state: &str, failures: &mut Vec<String>) {
    {
        let _on = crate::support::FAST_PATH_SWITCH.read().await;
        assert!(!fast_paths_disabled(), "fast paths must be on");
        let state = format!("{state}, fast paths on");
        check_sparql(fluree, &state, failures).await;
        check_jsonld(fluree, &state, failures).await;
    }
    let _off = crate::support::FAST_PATH_SWITCH.write().await;
    let _restore = FastPathsOff::set();
    let state = format!("{state}, fast paths off");
    check_sparql(fluree, &state, failures).await;
    check_jsonld(fluree, &state, failures).await;
}

/// The kill switch on, and off again however the holder exits.
struct FastPathsOff;

impl FastPathsOff {
    fn set() -> Self {
        set_fast_paths_disabled(true);
        assert!(fast_paths_disabled(), "the kill switch must read as set");
        Self
    }
}

impl Drop for FastPathsOff {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

#[tokio::test]
async fn union_dedups_on_fact_identity() {
    // The env var ORs into the switch and can't be cleared from here, so with
    // it set the fast-paths-on runs would be generic ones.
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "FLUREE_DISABLE_QUERY_FAST_PATHS must be unset"
    );
    let fluree = FlureeBuilder::memory().without_indexing().build_memory();
    fluree.create_ledger(LEDGER).await.expect("create ledger");
    let handle = fluree.ledger_cached(LEDGER).await.expect("cache");
    fluree
        .stage(&handle)
        .upsert_turtle(TRIG)
        .execute()
        .await
        .expect("seed");
    let mut failures = Vec::new();

    // Novelty only: the scans read decoded flakes.
    check_state(&fluree, "novelty", &mut failures).await;

    // Indexed: the scans read index rows.
    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("index");
    fluree.disconnect_ledger(LEDGER).await;
    let handle = fluree.ledger_cached(LEDGER).await.expect("reload");
    assert!(handle.snapshot().await.binary_store.is_some(), "indexed");
    check_state(&fluree, "indexed", &mut failures).await;

    assert!(failures.is_empty(), "{failures:#?}");
}

/// Two ledgers code their namespaces independently, so the datatype of a
/// constant object is a different `Sid` in each. A union across ledgers
/// re-codes its rows into the query's codes; the fact a member's scan
/// matched must be re-coded too, or one fact both ledgers hold comes back
/// once per ledger.
#[tokio::test]
async fn cross_ledger_union_keys_a_fact_on_the_querys_datatype_codes() {
    const A: &str = "it/union-xl-a:main";
    const B: &str = "it/union-xl-b:main";
    let fluree = FlureeBuilder::memory().without_indexing().build_memory();
    let seed = |ledger: &'static str, turtle: &'static str| {
        let fluree = fluree.clone();
        async move {
            let handle = fluree.ledger_cached(ledger).await.expect("cache");
            fluree
                .stage(&handle)
                .upsert_turtle(turtle)
                .execute()
                .await
                .expect("seed");
        }
    };
    fluree.create_ledger(A).await.expect("create A");
    fluree.create_ledger(B).await.expect("create B");
    // A registers another namespace first, so `ex:` takes a later code there.
    seed(
        A,
        "<http://other.example/x> <http://other.example/q> \"z\" .",
    )
    .await;
    seed(
        A,
        r#"@prefix ex: <http://example.org/> .
           ex:s ex:p "v"^^ex:dt ."#,
    )
    .await;
    seed(
        B,
        r#"@prefix ex: <http://example.org/> .
           ex:s ex:p "v"^^ex:dt .
           ex:t ex:p "v"^^ex:dt ."#,
    )
    .await;

    let mut failures = Vec::new();
    for state in ["novelty", "indexed"] {
        if state == "indexed" {
            for ledger in [A, B] {
                fluree
                    .reindex(ledger, ReindexOptions::default())
                    .await
                    .expect("index");
                fluree.disconnect_ledger(ledger).await;
            }
        }
        let code = |ledger: &'static str| {
            let fluree = fluree.clone();
            async move {
                let db = fluree.db(ledger).await.expect("db");
                db.snapshot
                    .encode_iri_strict("http://example.org/dt")
                    .expect("registered")
                    .namespace_code
            }
        };
        assert_ne!(
            code(A).await,
            code(B).await,
            "the fixture must give `ex:` a different code in each ledger"
        );

        let sparql = |body: &str| {
            format!("PREFIX ex: <http://example.org/>\n{body}")
                .replace("{ds}", &format!("FROM <{A}> FROM <{B}>"))
        };
        for (body, expected) in [
            (
                r#"SELECT ?s {ds} WHERE { ?s ex:p "v"^^ex:dt }"#,
                vec!["s=s", "s=t"],
            ),
            (
                "SELECT ?s ?o {ds} WHERE { ?s ex:p ?o }",
                vec!["o=v^^dt;s=s", "o=v^^dt;s=t"],
            ),
            (
                r#"SELECT (COUNT(*) AS ?n) {ds} WHERE { ex:s ex:p "v"^^ex:dt }"#,
                vec!["n=1^^integer"],
            ),
        ] {
            let query = sparql(body);
            let json = fluree
                .query_from()
                .sparql(&query)
                .execute_formatted()
                .await
                .unwrap_or_else(|e| panic!("{query}: {e}"));
            let mut rows: Vec<String> = json["results"]["bindings"]
                .as_array()
                .unwrap_or_else(|| panic!("{query}: {json}"))
                .iter()
                .map(|b| {
                    let mut cols: Vec<String> = b
                        .as_object()
                        .expect("binding object")
                        .iter()
                        .map(|(var, v)| {
                            let tail =
                                |s: &str| s.rsplit(['/', '#']).next().unwrap_or(s).to_string();
                            let value = tail(v["value"].as_str().unwrap_or_default());
                            match v["datatype"].as_str() {
                                Some(dt) => format!("{var}={value}^^{}", tail(dt)),
                                None => format!("{var}={value}"),
                            }
                        })
                        .collect();
                    cols.sort();
                    cols.join(";")
                })
                .collect();
            rows.sort();
            if rows != expected {
                failures.push(format!("{state}: {body}: {rows:?}, expected {expected:?}"));
            }
        }

        let jsonld = json!({
            "@context": {"ex": "http://example.org/"},
            "from": [A, B],
            "select": ["?s"],
            "where": {"@id": "?s", "ex:p": {"@value": "v", "@type": "ex:dt"}}
        });
        let json = fluree
            .query_from()
            .jsonld(&jsonld)
            .execute_formatted()
            .await
            .expect("jsonld");
        let mut rows: Vec<JsonValue> = json.as_array().cloned().unwrap_or_default();
        rows.sort_by_key(ToString::to_string);
        if rows != vec![json!(["ex:s"]), json!(["ex:t"])] {
            failures.push(format!("{state}: JSON-LD constant object: {json}"));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

const STRING_LEDGER: &str = "it/union-string-object:main";

/// s1 and s2 both hold `ex:b ex:alias "x"`: one fact of their union. `"y"@en`
/// is another term than the plain `"y"`.
const STRING_TRIG: &str = r#"
@prefix ex: <http://example.org/> .
GRAPH <urn:s1> {
  ex:a ex:name "x" .
  ex:b ex:alias "x" .
  ex:e ex:name "y" .
}
GRAPH <urn:s2> {
  ex:b ex:alias "x" .
  ex:c ex:alias "x" .
  ex:f ex:alias "y"@en .
}
"#;

/// A string object bound by the left row, joined across a union. The
/// bound-object lane keys string terms, but over a union it probes ref
/// objects only and leaves a row on a string object to the per-row scan,
/// which reads the union: the answer is the facts', before and after
/// indexing, with fast paths on and off.
#[tokio::test]
async fn a_string_object_joins_across_a_union_once_per_fact() {
    let fluree = FlureeBuilder::memory().without_indexing().build_memory();
    fluree
        .create_ledger(STRING_LEDGER)
        .await
        .expect("create ledger");
    let handle = fluree.ledger_cached(STRING_LEDGER).await.expect("cache");
    fluree
        .stage(&handle)
        .upsert_turtle(STRING_TRIG)
        .execute()
        .await
        .expect("seed");
    const QUERY: &str = "SELECT ?s ?y {ds} WHERE { ?s ex:name ?n . ?y ex:alias ?n }";
    const UNION: &str = "FROM <urn:s1> FROM <urn:s2>";
    let expected = want(&["s=a;y=b", "s=a;y=c"]);
    let mut failures = Vec::new();
    for state in ["novelty", "indexed"] {
        if state == "indexed" {
            fluree
                .reindex(STRING_LEDGER, ReindexOptions::default())
                .await
                .expect("index");
            fluree.disconnect_ledger(STRING_LEDGER).await;
        }
        for fast in [true, false] {
            let rows = if fast {
                let _on = crate::support::FAST_PATH_SWITCH.read().await;
                sparql_rows_in(&fluree, STRING_LEDGER, QUERY, UNION).await
            } else {
                let _off = crate::support::FAST_PATH_SWITCH.write().await;
                let _restore = FastPathsOff::set();
                sparql_rows_in(&fluree, STRING_LEDGER, QUERY, UNION).await
            };
            if rows != expected {
                failures.push(format!(
                    "{state}, fast paths {}: {rows:?}, want {expected:?}",
                    if fast { "on" } else { "off" }
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
