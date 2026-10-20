//! Materialization runs while a query is prepared, on the query's behalf: its
//! cancellation stops OWL 2 RL and datalog rule bodies mid-run, and the work
//! is charged as the query's fuel. The reasoning budget still bounds the run
//! on its own; these tests give it 20 seconds, so a query cancelled after
//! 100ms is stopped by the cancellation, and a regression fails in seconds
//! rather than hanging.

use crate::support::{genesis_ledger, graphdb_from_ledger, query_jsonld_tracked, MemoryLedger};
use fluree_db_api::{FlureeBuilder, QueryCancellation, QueryCancellationReason};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

async fn ledger_with(fluree: &fluree_db_api::Fluree, ledger_id: &str, data: Value) -> MemoryLedger {
    let ledger = genesis_ledger(fluree, ledger_id).await;
    fluree.insert(ledger, &data).await.expect("seed").ledger
}

/// Cancels `handle` from another thread after `delay`, as a client
/// disconnecting mid-query would. A thread, not a task: materialization over
/// in-memory data need not yield to the runtime before it finishes.
fn disconnect_after(handle: &QueryCancellation, delay: Duration) {
    let handle = handle.clone();
    std::thread::spawn(move || {
        std::thread::sleep(delay);
        handle.cancel_with(QueryCancellationReason::ClientDisconnected);
    });
}

/// A rule whose body joins 3,000 nodes with each other and keeps none of the
/// nine million pairs: about half a minute of work.
async fn slow_rule_ledger(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> MemoryLedger {
    let mut graph: Vec<Value> = (0..3_000)
        .map(|i| json!({"@id": format!("ex:a{i}"), "ex:kind": {"@id": "ex:k"}}))
        .collect();
    graph.push(json!({
        "@id": "ex:pairs",
        "f:rule": {
            "@type": "f:sparql",
            "@value": "PREFIX ex: <http://example.org/> \
                       CONSTRUCT { ?x ex:sameKind ?y } WHERE { ?x ex:kind ?k . ?y ex:kind ?k \
                       FILTER(STRLEN(STR(?x)) + STRLEN(STR(?y)) < 0) }"
        }
    }));
    ledger_with(
        fluree,
        ledger_id,
        json!({
            "@context": {"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"},
            "@graph": graph
        }),
    )
    .await
}

#[tokio::test]
async fn a_cancelled_query_stops_its_rule_bodies() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = slow_rule_ledger(&fluree, "reasoning/cancel-datalog:main").await;
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?x",
        "where": {"@id": "?x", "ex:sameKind": "?y"},
        "reasoning": "datalog",
        "reasoningBudget": {"maxSeconds": 20}
    });
    let handle = QueryCancellation::new();
    disconnect_after(&handle, Duration::from_millis(100));

    let started = Instant::now();
    let err = graphdb_from_ledger(&ledger)
        .query(&fluree)
        .jsonld(&query)
        .cancellation(handle)
        .execute()
        .await
        .expect_err("the query was cancelled");
    let elapsed = started.elapsed();
    assert!(
        err.to_string().contains("client_disconnected"),
        "the cancellation is the error, not a capped run: {err}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "cancelled after 100ms, the rule body ran {elapsed:?}"
    );
}

#[tokio::test]
async fn a_cancelled_query_stops_owl_materialization() {
    const LEN: usize = 3_000;
    let fluree = FlureeBuilder::memory().build_memory();
    let mut graph = vec![json!({
        "@id": "ex:partOf",
        "@type": "http://www.w3.org/2002/07/owl#TransitiveProperty"
    })];
    graph.extend((0..LEN).map(
        |i| json!({"@id": format!("ex:n{i}"), "ex:partOf": {"@id": format!("ex:n{}", i + 1)}}),
    ));
    let ledger = ledger_with(
        &fluree,
        "reasoning/cancel-owl:main",
        json!({"@context": {"ex": "http://example.org/"}, "@graph": graph}),
    )
    .await;
    // The closure has 4.5 million facts; the budget allows all of them, but
    // not the time they take.
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["(as (count ?y) ?n)"],
        "where": {"@id": "ex:n0", "ex:partOf": "?y"},
        "reasoning": "owl2rl",
        "reasoningBudget": {"maxFacts": 100_000_000, "maxSeconds": 20}
    });
    let handle = QueryCancellation::new();
    disconnect_after(&handle, Duration::from_millis(100));

    let started = Instant::now();
    let err = graphdb_from_ledger(&ledger)
        .query(&fluree)
        .jsonld(&query)
        .cancellation(handle)
        .execute()
        .await
        .expect_err("the query was cancelled, not answered without OWL entailments");
    let elapsed = started.elapsed();
    assert!(err.to_string().contains("client_disconnected"), "{err}");
    assert!(
        elapsed < Duration::from_secs(5),
        "cancelled after 100ms, OWL 2 RL ran {elapsed:?}"
    );
}

/// Fuel reported by a tracked query, in fuel units.
async fn fuel(fluree: &fluree_db_api::Fluree, ledger: &MemoryLedger, query: &Value) -> f64 {
    query_jsonld_tracked(fluree, ledger, query)
        .await
        .expect("tracked query")
        .fuel
        .expect("fuel is tracked")
}

/// The query that materializes pays for it; a query served from the cached
/// result does not. (Before materialization was charged, the two cost the
/// same.) A fuel limit below the materialization's cost stops the
/// query that would have to pay it.
async fn fuel_covers_materialization(ledger_id: &str, data: Value, query: Value) {
    fuel_covers_materialization_of(ledger_id, data, query, 0).await;
}

/// [`fuel_covers_materialization`], where materializing must cost at least
/// one row of fuel for each of `derived` facts.
async fn fuel_covers_materialization_of(
    ledger_id: &str,
    data: Value,
    query: Value,
    derived: usize,
) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(&fluree, ledger_id, data.clone()).await;
    let cold = fuel(&fluree, &ledger, &query).await;
    let warm = fuel(&fluree, &ledger, &query).await;
    assert!(
        cold > warm && cold - warm >= derived as f64 * 0.001,
        "materializing {derived} facts cost {cold} fuel, reading the cached result {warm}"
    );

    let mut limited = query.clone();
    limited["opts"]["max-fuel"] = json!(f64::midpoint(cold, warm));
    // A second ledger, not a branch of the first: the suffix goes on the name.
    let limited_id = match ledger_id.split_once(':') {
        Some((name, branch)) => format!("{name}-limited:{branch}"),
        None => format!("{ledger_id}-limited"),
    };
    let fresh = ledger_with(&fluree, &limited_id, data).await;
    let err = query_jsonld_tracked(&fluree, &fresh, &limited)
        .await
        .expect_err("materializing exceeds the limit");
    assert!(err.error.to_lowercase().contains("fuel"), "{}", err.error);
}

#[tokio::test]
async fn rule_bodies_are_charged_as_query_fuel() {
    let graph: Vec<Value> = (0..200)
        .map(|i| json!({"@id": format!("ex:n{i}"), "ex:parent": {"@id": format!("ex:n{}", i + 1)}}))
        .collect();
    fuel_covers_materialization(
        "reasoning/fuel-datalog:main",
        json!({"@context": {"ex": "http://example.org/"}, "@graph": graph}),
        json!({
            "@context": {"ex": "http://example.org/"},
            "select": ["(as (count ?y) ?n)"],
            "where": {"@id": "ex:n0", "ex:ancestor": "?y"},
            "reasoning": "datalog",
            "rules": [
                {
                    "@context": {"ex": "http://example.org/"},
                    "where": {"@id": "?x", "ex:parent": "?y"},
                    "insert": {"@id": "?x", "ex:ancestor": {"@id": "?y"}}
                },
                {
                    "@context": {"ex": "http://example.org/"},
                    "where": [
                        {"@id": "?x", "ex:parent": "?y"},
                        {"@id": "?y", "ex:ancestor": "?z"},
                        ["filter", "(!= ?x ?z)"]
                    ],
                    "insert": {"@id": "?x", "ex:ancestor": {"@id": "?z"}}
                }
            ],
            "opts": {"meta": true}
        }),
    )
    .await;
}

#[tokio::test]
async fn owl_materialization_is_charged_as_query_fuel() {
    let mut graph = vec![json!({
        "@id": "ex:partOf",
        "@type": "http://www.w3.org/2002/07/owl#TransitiveProperty"
    })];
    graph.extend((0..200).map(
        |i| json!({"@id": format!("ex:n{i}"), "ex:partOf": {"@id": format!("ex:n{}", i + 1)}}),
    ));
    fuel_covers_materialization(
        "reasoning/fuel-owl:main",
        json!({"@context": {"ex": "http://example.org/"}, "@graph": graph}),
        json!({
            "@context": {"ex": "http://example.org/"},
            "select": ["(as (count ?y) ?n)"],
            "where": {"@id": "ex:n0", "ex:partOf": "?y"},
            "reasoning": "owl2rl",
            "opts": {"meta": true}
        }),
    )
    .await;
}

/// Pairs in the transitive closure of a 50-edge chain.
const CLOSURE_OF_50: usize = 50 * 51 / 2;

fn chain(len: usize) -> Vec<Value> {
    (0..len)
        .map(|i| json!({"@id": format!("ex:n{i}"), "ex:partOf": {"@id": format!("ex:n{}", i + 1)}}))
        .collect()
}

fn within_query() -> Value {
    json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["(as (count ?y) ?n)"],
        "where": {"@id": "ex:n0", "ex:within": "?y"},
        "reasoning": "datalog",
        "opts": {"meta": true}
    })
}

/// A rule body that is a property path is charged for the edges its
/// traversal reads and the rows it emits.
#[tokio::test]
async fn property_path_rule_bodies_are_charged_as_query_fuel() {
    let mut graph = chain(50);
    graph.push(json!({
        "@id": "ex:withinRule",
        "f:rule": {"@type": "f:sparql", "@value":
            "PREFIX ex: <http://example.org/> CONSTRUCT { ?x ex:within ?y } WHERE { ?x ex:partOf+ ?y }"}
    }));
    fuel_covers_materialization_of(
        "reasoning/fuel-path:main",
        json!({"@context": {"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"}, "@graph": graph}),
        within_query(),
        CLOSURE_OF_50,
    )
    .await;
}

/// A recursive rule compiled to a property path is charged the same way.
#[tokio::test]
async fn compiled_closures_are_charged_as_query_fuel() {
    let mut query = within_query();
    query["rules"] = json!([
        {"@context": {"ex": "http://example.org/"},
         "where": {"@id": "?x", "ex:partOf": "?y"},
         "insert": {"@id": "?x", "ex:within": {"@id": "?y"}}},
        {"@context": {"ex": "http://example.org/"},
         "where": [{"@id": "?x", "ex:partOf": "?y"}, {"@id": "?y", "ex:within": "?z"}],
         "insert": {"@id": "?x", "ex:within": {"@id": "?z"}}}
    ]);
    fuel_covers_materialization_of(
        "reasoning/fuel-closure:main",
        json!({"@context": {"ex": "http://example.org/"}, "@graph": chain(50)}),
        query,
        CLOSURE_OF_50,
    )
    .await;
}
