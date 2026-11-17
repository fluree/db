//! An unordered query answers in the same order on every execution.
//!
//! A query with no ORDER BY may answer in any order, but it must answer in the
//! same one each time it runs over the same data: a client pages such an
//! answer with LIMIT/OFFSET, and pages cut from two different orders skip and
//! repeat rows. Each test below runs one query shape several times, each run a
//! fresh execution, and checks that every run answers identically, row order
//! and rendered content included. Each shape reaches an operator or formatter
//! that once iterated a std `HashMap`/`HashSet` (seeded at random per
//! instance) into its output:
//!
//! - the generic GROUP BY (grouped-list output, `GROUP_CONCAT`), which held
//!   its groups in such a map;
//! - a property path with both ends unbound, whose closure iterated its node
//!   set;
//! - SPARQL's list view with a variable predicate, which read the
//!   list-holding predicates in set order (and labelled the list cells in
//!   that order);
//! - a JSON-LD graph crawl over a cycle, whose rendering of a shared node
//!   depended on which predicate was formatted first.

use crate::support::{self, genesis_ledger, MemoryFluree, MemoryLedger};
use fluree_db_api::{FlureeBuilder, QueryExecutionOptions};
use serde_json::{json, Value as JsonValue};

/// Executions per shape. A std map's order differs between two instances
/// with high probability once it holds dozens of keys; the seeds below make
/// every shape hold enough that identical answers across these runs are not
/// luck.
const RUNS: usize = 6;

const EX: &str = "http://example.org/";

fn ctx() -> JsonValue {
    json!({"ex": EX})
}

async fn turtle(fluree: &MemoryFluree, ledger: MemoryLedger, ttl: &str) -> MemoryLedger {
    fluree
        .insert_turtle(ledger, &format!("@prefix ex: <{EX}> .\n{ttl}"))
        .await
        .unwrap_or_else(|e| panic!("insert: {e}"))
        .ledger
}

/// Run `run` [`RUNS`] times and assert every answer equals the first.
async fn assert_same_every_run<F, Fut>(label: &str, run: F) -> JsonValue
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = JsonValue>,
{
    let first = run().await;
    for i in 1..RUNS {
        let next = run().await;
        assert_eq!(
            next, first,
            "{label}: run {i} answered differently from run 0"
        );
    }
    first
}

async fn sparql_rows(fluree: &MemoryFluree, ledger: &MemoryLedger, sparql: &str) -> JsonValue {
    support::query_sparql(fluree, ledger, sparql)
        .await
        .unwrap_or_else(|e| panic!("{sparql}: {e}"))
        .to_sparql_json(&ledger.snapshot)
        .expect("SPARQL JSON")
}

async fn jsonld_rows(fluree: &MemoryFluree, ledger: &MemoryLedger, query: &JsonValue) -> JsonValue {
    support::query_jsonld(fluree, ledger, query)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"))
        .to_jsonld(&ledger.snapshot)
        .expect("JSON-LD rows")
}

/// The generic GROUP BY operator (taken for `GROUP_CONCAT` and for JSON-LD's
/// grouped-list projection, which the streaming aggregate declines) emits its
/// groups in one order every run.
#[tokio::test]
async fn a_generic_group_by_emits_its_groups_in_one_order() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "it/unordered-order/groupby:main");
    let ttl: String = (0..120)
        .map(|i| format!("ex:s{i} ex:k \"key-{}\" ; ex:v {i} .\n", i % 48))
        .collect();
    let ledger = turtle(&fluree, ledger, &ttl).await;

    let sparql = "PREFIX ex: <http://example.org/> \
                  SELECT ?k (GROUP_CONCAT(?v) AS ?vs) WHERE { ?s ex:k ?k ; ex:v ?v } GROUP BY ?k";
    let rows = assert_same_every_run("SPARQL GROUP_CONCAT", || {
        sparql_rows(&fluree, &ledger, sparql)
    })
    .await;
    assert_eq!(rows["results"]["bindings"].as_array().unwrap().len(), 48);

    let grouped_list = json!({
        "@context": ctx(),
        "select": ["?k", "?v"],
        "where": {"@id": "?s", "ex:k": "?k", "ex:v": "?v"},
        "groupBy": ["?k"],
    });
    let rows = assert_same_every_run("JSON-LD grouped list", || {
        jsonld_rows(&fluree, &ledger, &grouped_list)
    })
    .await;
    assert_eq!(rows.as_array().unwrap().len(), 48);

    let concat = json!({
        "@context": ctx(),
        "select": ["?k", "(as (groupconcat ?v) ?vs)"],
        "where": {"@id": "?s", "ex:k": "?k", "ex:v": "?v"},
        "groupBy": ["?k"],
    });
    assert_same_every_run("JSON-LD groupconcat", || {
        jsonld_rows(&fluree, &ledger, &concat)
    })
    .await;
}

/// A property path with both ends unbound computes the closure from every
/// node the path's edges touch, and answers its pairs in one order every run.
#[tokio::test]
async fn a_closure_with_both_ends_unbound_answers_in_one_order() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "it/unordered-order/path:main");
    // Six chains of eight nodes: 48 nodes, each reaching the rest of its chain.
    let ttl: String = (0..6)
        .flat_map(|c| (0..7).map(move |i| format!("ex:n{c}_{i} ex:next ex:n{c}_{} .\n", i + 1)))
        .collect();
    let ledger = turtle(&fluree, ledger, &ttl).await;

    for (label, path, pairs) in [("+", "ex:next+", 6 * 28), ("?", "ex:next?", 6 * (8 + 7))] {
        let sparql = format!("PREFIX ex: <{EX}> SELECT ?s ?o WHERE {{ ?s {path} ?o }}");
        let rows = assert_same_every_run(&format!("SPARQL {label}"), || {
            sparql_rows(&fluree, &ledger, &sparql)
        })
        .await;
        assert_eq!(
            rows["results"]["bindings"].as_array().unwrap().len(),
            pairs,
            "SPARQL {label}"
        );

        let jsonld = json!({
            "@context": {"ex": EX, "step": {"@path": path}},
            "select": ["?s", "?o"],
            "where": {"@id": "?s", "step": "?o"},
        });
        let rows = assert_same_every_run(&format!("JSON-LD {label}"), || {
            jsonld_rows(&fluree, &ledger, &jsonld)
        })
        .await;
        assert_eq!(rows.as_array().unwrap().len(), pairs, "JSON-LD {label}");
    }
}

/// The RDF view of lists (the Graph Store `GET` reads through it) answers a
/// variable predicate over an indexed ledger by reading each list-holding
/// predicate in turn; the lists come out, and their cells are labelled, in
/// one order every run. (Every query surface reads a list's members as
/// values, so the view has no JSON-LD twin.)
#[tokio::test]
async fn the_list_view_reads_its_predicates_in_one_order() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/unordered-order/lists:main";
    let ledger = genesis_ledger(&fluree, id);
    let ttl: String = (0..24)
        .map(|p| format!("ex:s ex:p{p} ({p} {}) .\n", p + 100))
        .collect();
    turtle(&fluree, ledger, &ttl).await;
    support::rebuild_and_publish_index(&fluree, id).await;
    let ledger = fluree.ledger(id).await.expect("reload the indexed ledger");

    for (label, sparql) in [
        (
            "subject bound",
            format!("PREFIX ex: <{EX}> SELECT ?p ?o WHERE {{ ex:s ?p ?o }}"),
        ),
        (
            "all open",
            format!("PREFIX ex: <{EX}> SELECT ?s ?p ?o WHERE {{ ?s ?p ?o }}"),
        ),
    ] {
        let rows = assert_same_every_run(label, || async {
            support::graphdb_from_ledger(&ledger)
                .query(&fluree)
                .sparql(&sparql)
                .execution_options(QueryExecutionOptions::new().with_rdf_list_view(true))
                .execute()
                .await
                .unwrap_or_else(|e| panic!("{sparql}: {e}"))
                .to_sparql_json(&ledger.snapshot)
                .expect("SPARQL JSON")
        })
        .await;
        let bindings = rows["results"]["bindings"].as_array().unwrap();
        assert!(
            bindings
                .iter()
                .filter(|b| b["o"]["type"] == "bnode")
                .count()
                >= 24,
            "{label}: one list head per predicate: {rows}"
        );
    }
}

/// A JSON-LD graph crawl renders a node shared by two paths once and reuses
/// it, cutting a cycle at the first node the path revisits. Which path
/// reaches the shared node first decides what that rendering holds, so the
/// predicates of each subject are formatted in one order every run, and the
/// rendered graph is the same every run.
#[tokio::test]
async fn a_graph_crawl_over_a_cycle_renders_the_same_graph_every_run() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "it/unordered-order/crawl:main");
    // Per root: two paths to one shared node `b`, which points back at `x`.
    let ttl: String = (0..12)
        .map(|r| {
            format!(
                "ex:a{r} a ex:Root ; ex:p1 ex:x{r} ; ex:p2 ex:y{r} .\n\
                 ex:x{r} ex:to ex:b{r} .\n\
                 ex:y{r} ex:to ex:b{r} .\n\
                 ex:b{r} ex:back ex:x{r} .\n"
            )
        })
        .collect();
    let ledger = turtle(&fluree, ledger, &ttl).await;

    let crawl = json!({
        "@context": ctx(),
        "select": {"?s": ["*"]},
        "where": {"@id": "?s", "@type": "ex:Root"},
        "depth": 6,
    });
    let rendered = assert_same_every_run("JSON-LD crawl", || async {
        support::query_jsonld_formatted(&fluree, &ledger, &crawl)
            .await
            .unwrap_or_else(|e| panic!("{crawl}: {e}"))
    })
    .await;
    assert_eq!(rendered.as_array().unwrap().len(), 12);
}
