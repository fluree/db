//! SPARQL's view of RDF lists, read in one pass.
//!
//! A pattern `?s <p> ?o` on a predicate that holds lists, read with no input,
//! is the predicate's stored rows that are not list members plus each list's
//! owner edge `s p head`. One scan answers it: it skips the members and emits
//! each list's owner edge as it meets the list. So a `LIMIT` stops it after a
//! batch, as it stops any scan, and no second read of the predicate follows.
//!
//! The answers are those of the list view read with an input, which the
//! one-pass scan does not serve (`VALUES ?z { 1 } FILTER(?z = 1)`, whose
//! block applies the VALUES before any triple, star or not): a
//! tee holds the input rows and a read of the lists joins their list triples.
//! Both readings are asked in every state of the ledger (novelty only,
//! indexed, indexed with novelty), over its default graph, a named graph and
//! unions of both, with fast paths on and off.
//!
//! This file is its own test binary: it toggles the process-global fast-path
//! kill switch.

#![cfg(feature = "native")]

use fluree_db_api::{set_fast_paths_disabled, Fluree, FlureeBuilder, GraphDb, ReindexOptions};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

const LEDGER: &str = "it/sparql-list-single-pass:main";
const PREFIXES: &str = "PREFIX ex: <http://example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
";

/// Serializes this binary's tests: one flips the fast-path switch.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Turns the fast-path switch back off on every exit path.
struct FastPathsBackOn;
impl Drop for FastPathsBackOn {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

/// The ledger's first write: `ex:items` mixes lists with plain values (s1
/// holds both), `ex:tags` is a second list on s3, and the named graph holds a
/// list of its own and a plain value of s1.
const BASE: &str = r#"
@prefix ex: <http://example.org/> .
ex:s1 ex:items ( 1 2 ) , 9 .
ex:s2 ex:items 3 .
ex:s3 ex:items ( 4 ) ; ex:tags ( "x" "y" ) .
GRAPH <urn:g1> { ex:s4 ex:items ( 5 6 ) . ex:s1 ex:items 7 . }
"#;

/// A later write: a list for a subject that held only a plain value, a list
/// on a new subject, and a predicate whose only rows are a list.
const LATER: &str = r"
@prefix ex: <http://example.org/> .
ex:s2 ex:items ( 10 ) .
ex:s5 ex:items ( 8 8 ) .
ex:s6 ex:fresh ( 11 12 ) .
GRAPH <urn:g1> { ex:s7 ex:items ( 13 ) . }
";

#[derive(Clone, Copy, Debug)]
enum State {
    /// Both writes, never indexed: every row is novelty.
    Novelty,
    /// Both writes, indexed.
    Indexed,
    /// The first write indexed, the second in novelty.
    IndexedWithNovelty,
}

async fn ledger(state: State) -> (Fluree, GraphDb) {
    let fluree = FlureeBuilder::memory().without_indexing().build_memory();
    fluree.create_ledger(LEDGER).await.expect("create");
    let write = |ttl: &'static str| {
        let fluree = &fluree;
        async move {
            let handle = fluree.ledger_cached(LEDGER).await.expect("cache");
            fluree
                .stage(&handle)
                .insert_turtle(ttl)
                .execute()
                .await
                .expect("write");
        }
    };
    let index = || async {
        fluree
            .reindex(LEDGER, ReindexOptions::default())
            .await
            .expect("reindex");
        fluree.disconnect_ledger(LEDGER).await;
    };
    write(BASE).await;
    match state {
        State::Novelty => write(LATER).await,
        State::Indexed => {
            write(LATER).await;
            index().await;
        }
        State::IndexedWithNovelty => {
            index().await;
            write(LATER).await;
        }
    }
    let view = fluree.db(LEDGER).await.expect("view");
    (fluree, view)
}

/// A query's rows: each binding's value, a blank node as `_:` alone, sorted;
/// and how many distinct blank nodes the rows name.
fn rows(json: &JsonValue, vars: &[&str]) -> (Vec<Vec<String>>, usize) {
    let mut blanks = std::collections::BTreeSet::new();
    let mut out: Vec<Vec<String>> = json["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("SPARQL results: {json}"))
        .iter()
        .map(|b| {
            vars.iter()
                .map(|v| match b.get(*v) {
                    Some(t) if t["type"] == "bnode" => {
                        blanks.insert(t["value"].as_str().unwrap_or("").to_string());
                        "_:".to_string()
                    }
                    Some(t) => t["value"].as_str().unwrap_or("").to_string(),
                    None => "-".to_string(),
                })
                .collect()
        })
        .collect();
    out.sort();
    (out, blanks.len())
}

async fn answer(
    fluree: &Fluree,
    view: &GraphDb,
    query: &str,
    vars: &[&str],
) -> (Vec<Vec<String>>, usize) {
    let json = view
        .query(fluree)
        .sparql(&format!("{PREFIXES}{query}"))
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    rows(&json, vars)
}

async fn physical_plan(fluree: &Fluree, view: &GraphDb, query: &str) -> String {
    fluree
        .explain_sparql(view, &format!("{PREFIXES}{query}"))
        .await
        .unwrap_or_else(|e| panic!("explain {query}: {e}"))["plan"]["physical"]
        .to_string()
}

/// (dataset clause, prologue) of the scopes the cases run in.
fn scopes() -> Vec<(&'static str, String)> {
    vec![
        ("the default graph", String::new()),
        ("FROM g1", "FROM <urn:g1>".to_string()),
        (
            "FROM default + g1",
            format!("FROM <{LEDGER}> FROM <urn:g1>"),
        ),
    ]
}

/// (label, SELECT clause, WHERE body with `{in}` where the input goes, ORDER/
/// GROUP tail, projected variables).
const CASES: &[(&str, &str, &str, &str, &[&str])] = &[
    (
        "owner edges and plain values",
        "?s ?o",
        "{in} ?s ex:items ?o",
        "",
        &["s", "o"],
    ),
    (
        "the object unprojected",
        "?s",
        "{in} ?s ex:items ?o",
        "",
        &["s"],
    ),
    (
        "COUNT(*)",
        "(COUNT(*) AS ?n)",
        "{in} ?s ex:items ?o",
        "",
        &["n"],
    ),
    (
        "grouped by the object",
        "?o (COUNT(*) AS ?n)",
        "{in} ?s ex:items ?o",
        "GROUP BY ?o",
        &["o", "n"],
    ),
    (
        "grouped by the subject",
        "?s (COUNT(*) AS ?n)",
        "{in} ?s ex:items ?o",
        "GROUP BY ?s",
        &["s", "n"],
    ),
    (
        "an owner edge's first cell",
        "?s ?x",
        "{in} ?s ex:items ?l . ?l rdf:first ?x",
        "",
        &["s", "x"],
    ),
    (
        "a second list on one subject",
        "?s ?o",
        "{in} ?s ex:tags ?o",
        "",
        &["s", "o"],
    ),
    (
        "a predicate only lists hold",
        "?s ?o",
        "{in} ?s ex:fresh ?o",
        "",
        &["s", "o"],
    ),
    (
        "ordered",
        "?s ?o",
        "{in} ?s ex:items ?o",
        "ORDER BY ?s ?o",
        &["s", "o"],
    ),
    (
        "joined to a stored triple",
        "?s ?o ?t",
        "{in} ?s ex:items ?o . ?s ex:tags ?t",
        "",
        &["s", "o", "t"],
    ),
    // The second pattern reads every list by its terms, the first names
    // them as it scans: a head cell joins only if both give a list one name.
    (
        "one list met twice",
        "?s ?s2",
        "{in} ?s ex:items ?o . ?s2 ex:items ?o",
        "",
        &["s", "s2"],
    ),
    (
        "in each named graph",
        "?g ?s ?o",
        "{in} GRAPH ?g { ?s ex:items ?o }",
        "",
        &["g", "s", "o"],
    ),
];

/// The one-pass reading answers as the reading with an input does, in every
/// state, scope and case, with fast paths on and off; it plans no list
/// augmentation, and the reading with an input does (so the comparison is
/// not of one plan with itself). A `LIMIT 2` returns two of the rows.
#[tokio::test]
async fn one_pass_answers_as_the_list_view_with_an_input() {
    let _serial = SERIAL.lock().await;
    let _back_on = FastPathsBackOn;
    let mut failures = Vec::new();
    let mut compared = 0;
    for state in [State::Novelty, State::Indexed, State::IndexedWithNovelty] {
        let (fluree, view) = ledger(state).await;
        for (scope, ds) in scopes() {
            for (label, select, body, tail, vars) in CASES {
                let query = |input: &str| {
                    format!(
                        "SELECT {select} {ds} WHERE {{ {} }} {tail}",
                        body.replace("{in}", input)
                    )
                };
                let one_pass = query("");
                let with_input = query("VALUES ?z { 1 } FILTER(?z = 1)");
                let label = format!("{state:?} / {scope} / {label}");
                for disabled in [false, true] {
                    set_fast_paths_disabled(disabled);
                    let got = answer(&fluree, &view, &one_pass, vars).await;
                    let want = answer(&fluree, &view, &with_input, vars).await;
                    if got != want {
                        failures.push(format!(
                            "{label} (fast paths off: {disabled}):\n  one pass   {got:?}\n  with input {want:?}"
                        ));
                    }
                    compared += 1;
                }
                set_fast_paths_disabled(false);
                // A GRAPH pattern plans its body per graph as it runs, which
                // EXPLAIN does not show.
                if body.contains("GRAPH") {
                    continue;
                }
                let plan = physical_plan(&fluree, &view, &one_pass).await;
                let oracle = physical_plan(&fluree, &view, &with_input).await;
                // Each list-view pattern read with an input plans a tee and an
                // augmentation; the one read first, with none, plans neither.
                let augments = |plan: &str| plan.matches("ListAugmentOperator").count();
                if augments(&plan) + 1 != augments(&oracle) {
                    failures.push(format!(
                        "{label}: one pass plans {} list augmentations, the oracle {}:\n  {plan}\n  {oracle}",
                        augments(&plan),
                        augments(&oracle)
                    ));
                }
                if !oracle.contains("ListTeeOperator") {
                    failures.push(format!("{label}: the oracle plans no tee: {oracle}"));
                }
            }
            // A LIMIT returns rows of the full answer.
            let full = answer(
                &fluree,
                &view,
                &format!("SELECT ?s ?o {ds} WHERE {{ ?s ex:items ?o }}"),
                &["s", "o"],
            )
            .await;
            let limited = answer(
                &fluree,
                &view,
                &format!("SELECT ?s ?o {ds} WHERE {{ ?s ex:items ?o }} LIMIT 2"),
                &["s", "o"],
            )
            .await;
            let mut pool: BTreeMap<&Vec<String>, usize> = BTreeMap::new();
            for row in &full.0 {
                *pool.entry(row).or_default() += 1;
            }
            let mut bad = limited.0.len() != 2;
            for row in &limited.0 {
                match pool.get_mut(row) {
                    Some(n) if *n > 0 => *n -= 1,
                    _ => bad = true,
                }
            }
            if bad {
                failures.push(format!(
                    "{state:?} / {scope}: LIMIT 2 {limited:?} of {full:?}"
                ));
            }
        }
    }
    assert!(compared > 0, "no case ran");
    assert!(
        failures.is_empty(),
        "{} failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Expected answers for the default graph once both writes are indexed, by
/// hand: the list view's owner edges sit beside the stored values.
#[tokio::test]
async fn one_pass_answers_by_hand() {
    let _serial = SERIAL.lock().await;
    let (fluree, view) = ledger(State::Indexed).await;
    let s = |n: &str| format!("http://example.org/{n}");
    let (got, blanks) = answer(
        &fluree,
        &view,
        "SELECT ?s ?o WHERE { ?s ex:items ?o }",
        &["s", "o"],
    )
    .await;
    let mut want = vec![
        vec![s("s1"), "_:".to_string()],
        vec![s("s1"), "9".to_string()],
        vec![s("s2"), "3".to_string()],
        vec![s("s2"), "_:".to_string()],
        vec![s("s3"), "_:".to_string()],
        vec![s("s5"), "_:".to_string()],
    ];
    want.sort();
    assert_eq!(got, want);
    assert_eq!(blanks, 4, "one head cell per list");
    let (n, _) = answer(
        &fluree,
        &view,
        "SELECT (COUNT(*) AS ?n) WHERE { ?s ex:fresh ?o }",
        &["n"],
    )
    .await;
    assert_eq!(n, vec![vec!["1".to_string()]]);
}

/// A ledger of `lists` subjects, each owning a two-item list on `ex:tags`,
/// indexed.
async fn many_lists(lists: usize) -> (Fluree, GraphDb) {
    let fluree = FlureeBuilder::memory().without_indexing().build_memory();
    let id = format!("it/sparql-list-single-pass-{lists}:main");
    fluree.create_ledger(&id).await.expect("create");
    let handle = fluree.ledger_cached(&id).await.expect("cache");
    let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
    for i in 0..lists {
        ttl.push_str(&format!("ex:s{i} ex:tags ( \"a{}\" \"b\" ) .\n", i % 7));
    }
    fluree
        .stage(&handle)
        .upsert_turtle(&ttl)
        .execute()
        .await
        .expect("write");
    fluree
        .reindex(&id, ReindexOptions::default())
        .await
        .expect("reindex");
    fluree.disconnect_ledger(&id).await;
    let view = fluree.db(&id).await.expect("view");
    (fluree, view)
}

async fn fuel(fluree: &Fluree, view: &GraphDb, query: &str) -> (f64, usize) {
    let tracked = view
        .query(fluree)
        .sparql(&format!("{PREFIXES}{query}"))
        .execute_tracked()
        .await
        .unwrap_or_else(|e| panic!("{query}: {e:?}"));
    let rows = tracked.result["results"]["bindings"]
        .as_array()
        .map_or(0, Vec::len);
    (tracked.fuel.expect("fuel is tracked"), rows)
}

/// A `LIMIT` over a predicate whose rows are all list members stops after a
/// batch: its fuel does not grow with the number of lists, and stays well
/// under the full read's. A member-hiding scan followed by a read of every
/// list reads the whole predicate twice first, whatever the LIMIT.
#[tokio::test]
async fn a_limit_over_lists_reads_one_batch() {
    let _serial = SERIAL.lock().await;
    let limit = "SELECT ?s ?t WHERE { ?s ex:tags ?t } LIMIT 5";
    let all = "SELECT ?s ?t WHERE { ?s ex:tags ?t }";
    let (small_fluree, small) = many_lists(16_000).await;
    let (large_fluree, large) = many_lists(48_000).await;
    let (small_fuel, small_rows) = fuel(&small_fluree, &small, limit).await;
    let (large_fuel, large_rows) = fuel(&large_fluree, &large, limit).await;
    let (all_fuel, all_rows) = fuel(&large_fluree, &large, all).await;
    assert_eq!((small_rows, large_rows, all_rows), (5, 5, 48_000));
    assert!(
        large_fuel <= small_fuel * 1.5,
        "LIMIT 5 fuel grew with the lists: {small_fuel} at 16k, {large_fuel} at 48k"
    );
    assert!(
        large_fuel * 2.0 <= all_fuel,
        "LIMIT 5 fuel {large_fuel} is not under half the full read's {all_fuel}"
    );
}
