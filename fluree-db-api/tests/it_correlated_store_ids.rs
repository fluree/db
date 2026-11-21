//! A correlated scan takes the store id its row already has.
//!
//! A join or OPTIONAL probing one row at a time binds the row's store id into
//! the scanned pattern as the IRI it decodes to. The scan used to look that
//! IRI up again in the subject dictionary, a leaf search per probe that ends
//! at the id it started from. It now takes the id, when the id is persisted
//! and belongs to the store it scans. Each test counts the subject dictionary's
//! leaf lookups across one query and checks the answer is unchanged, in
//! subject and object position, with novelty, in a named graph, across two
//! ledgers whose ids collide, and under a policy, whose fallback reads the
//! still-bound pattern instead.

use crate::support;
use fluree_db_api::{FlureeBuilder, LedgerState};
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_query::binary_scan::CORRELATED_STORE_IDS_SITE;
use serde_json::{json, Value};

const PREFIXES: &str = "PREFIX ex: <http://example.org/>\n";
const DATA: &str = "ex:a ex:knows ex:b , ex:c .
    ex:b ex:name \"b\" ; ex:age 1 ; ex:secret \"sb\" .
    ex:c ex:name \"c\" ; ex:knows ex:a .
    ex:d ex:likes ex:b .";

async fn indexed(fluree: &fluree_db_api::Fluree, id: &str, ttl: &str) -> LedgerState {
    let ledger = fluree.create_ledger(id).await.expect("create");
    fluree
        .insert_turtle(
            ledger,
            &format!("@prefix ex: <http://example.org/> .\n{ttl}"),
        )
        .await
        .expect("insert");
    support::rebuild_and_publish_index(fluree, id).await;
    fluree.ledger(id).await.expect("reload")
}

async fn update(fluree: &fluree_db_api::Fluree, ledger: LedgerState, body: &str) -> LedgerState {
    let parsed = fluree_db_sparql::parse_sparql(&format!("{PREFIXES}{body}"));
    assert!(!parsed.has_errors(), "{body}: {:?}", parsed.diagnostics);
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
        .expect("update")
        .ledger
}

/// How many correlated scans stamped `outcome` for taking their store ids.
fn stamps(spans: &support::span_capture::SpanStore, outcome: &str) -> usize {
    spans
        .find_events("fast-path outcome")
        .iter()
        .filter(|e| {
            e.fields.get("site").map(String::as_str) == Some(CORRELATED_STORE_IDS_SITE)
                && e.fields.get("outcome").map(String::as_str) == Some(outcome)
        })
        .count()
}

fn subject_lookups(ledger: &LedgerState) -> u64 {
    ledger
        .binary_store
        .as_ref()
        .expect("indexed ledger")
        .0
        .downcast_ref::<BinaryIndexStore>()
        .expect("binary store")
        .subject_reverse_leaf_lookups()
}

/// The query's rows, sorted, and the subject leaf lookups it made.
async fn sparql(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    body: &str,
) -> (Vec<Value>, u64) {
    let q = format!("{PREFIXES}{body}");
    let before = subject_lookups(ledger);
    let result = support::query_sparql(fluree, ledger, &q)
        .await
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    let lookups = subject_lookups(ledger) - before;
    let rows = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    (support::normalize_rows(&rows), lookups)
}

#[tokio::test]
async fn subject_and_object_probes_take_the_row_store_id() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = indexed(&fluree, "it/correlated-ids/positions:main", DATA).await;

    // `ex:a` is looked up once; the probes of `?x` take its ids.
    let (rows, lookups) = sparql(
        &fluree,
        &ledger,
        "SELECT ?x ?p ?o WHERE { ex:a ex:knows ?x . ?x ?p ?o }",
    )
    .await;
    assert_eq!(rows.len(), 5, "{rows:?}");
    assert_eq!(lookups, 1, "subject probes looked their ids up again");

    let (rows, lookups) = sparql(
        &fluree,
        &ledger,
        "SELECT ?x ?s ?p WHERE { ex:a ex:knows ?x . ?s ?p ?x }",
    )
    .await;
    assert_eq!(
        rows,
        support::normalize_rows(&json!([
            ["ex:b", "ex:a", "ex:knows"],
            ["ex:b", "ex:d", "ex:likes"],
            ["ex:c", "ex:a", "ex:knows"],
        ])),
    );
    assert_eq!(lookups, 1, "object probes looked their ids up again");

    // DESCRIBE expands each target through the same per-row probe. Its
    // output DISTINCT makes lookups of its own, so count the probes instead.
    let (spans, _guard) = support::span_capture::init_test_tracing();
    let q = format!("{PREFIXES}DESCRIBE ?x WHERE {{ ex:a ex:knows ?x }}");
    let described = support::query_sparql(&fluree, &ledger, &q)
        .await
        .expect("describe")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld")
        .to_string();
    assert_eq!(
        (
            stamps(&spans, "proceed"),
            stamps(&spans, "fallback:gate_declined")
        ),
        (2, 0),
        "each target's probe takes its id"
    );
    assert!(
        described.contains("sb") && described.contains("\"c\"") && !described.contains("likes"),
        "{described}"
    );
}

#[tokio::test]
async fn an_optional_probe_takes_the_row_store_id() {
    // A variable predicate keeps OPTIONAL off its batched probes, so each
    // row's lookup is a scan of its own.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = indexed(&fluree, "it/correlated-ids/optional:main", DATA).await;
    let (rows, lookups) = sparql(
        &fluree,
        &ledger,
        "SELECT ?x ?p ?o WHERE { ex:a ex:knows ?x OPTIONAL { ?x ?p ?o } }",
    )
    .await;
    assert_eq!(rows.len(), 5, "{rows:?}");
    assert_eq!(lookups, 1, "OPTIONAL probes looked their ids up again");
}

#[tokio::test]
async fn jsonld_probes_take_the_row_store_id() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = indexed(&fluree, "it/correlated-ids/jsonld:main", DATA).await;
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?x", "?p", "?o"],
        "where": [{"@id": "ex:a", "ex:knows": "?x"}, {"@id": "?x", "?p": "?o"}]
    });
    let before = subject_lookups(&ledger);
    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    assert_eq!(subject_lookups(&ledger) - before, 1);
    let rows = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(support::normalize_rows(&rows).len(), 5, "{rows}");
}

#[tokio::test]
async fn probes_answer_from_novelty() {
    // With live novelty a scan decodes its rows, novelty ids being
    // ephemeral, so a probe fed by one has no store id to take and looks its
    // subject up: a retraction and a subject first written there still show.
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/correlated-ids/novelty:main";
    let ledger = indexed(&fluree, id, DATA).await;
    let ledger = update(&fluree, ledger, "DELETE DATA { ex:b ex:age 1 }").await;
    let ledger = update(
        &fluree,
        ledger,
        "INSERT DATA { ex:a ex:knows ex:n . ex:n ex:name \"n\" }",
    )
    .await;
    let (rows, _) = sparql(
        &fluree,
        &ledger,
        "SELECT ?x ?p ?o WHERE { ex:a ex:knows ?x . ?x ?p ?o }",
    )
    .await;
    assert_eq!(
        rows,
        support::normalize_rows(&json!([
            ["ex:b", "ex:name", "b"],
            ["ex:b", "ex:secret", "sb"],
            ["ex:c", "ex:knows", "ex:a"],
            ["ex:c", "ex:name", "c"],
            ["ex:n", "ex:name", "n"],
        ])),
    );
}

/// Rows from the batched subject probe carry store ids under novelty too,
/// novelty ids among them. A probe takes a persisted id and looks a novelty
/// id's subject up: `ex:a` takes its id, `ex:n`, first written in novelty,
/// does not.
#[tokio::test]
async fn a_novelty_id_from_a_batched_probe_is_looked_up() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/correlated-ids/batched-novelty:main";
    let ledger = indexed(&fluree, id, DATA).await;
    let ledger = update(
        &fluree,
        ledger,
        "INSERT DATA { ex:b ex:knows ex:n . ex:n ex:name \"n\" ; ex:knows ex:c }",
    )
    .await;

    let (spans, _guard) = support::span_capture::init_test_tracing();
    let (rows, _) = sparql(
        &fluree,
        &ledger,
        "SELECT ?y ?p ?o WHERE { ex:a ex:knows ?x . ?x ex:knows ?y . ?y ?p ?o }",
    )
    .await;
    assert_eq!(
        rows,
        support::normalize_rows(&json!([
            ["ex:a", "ex:knows", "ex:b"],
            ["ex:a", "ex:knows", "ex:c"],
            ["ex:n", "ex:knows", "ex:c"],
            ["ex:n", "ex:name", "n"],
        ])),
    );
    assert_eq!(
        stamps(&spans, "proceed"),
        1,
        "only the persisted subject's probe takes its id"
    );
}

#[tokio::test]
async fn a_named_graph_probe_takes_ids_of_its_own_ledger() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/correlated-ids/graph:main";
    let ledger = indexed(&fluree, id, DATA).await;
    update(
        &fluree,
        ledger,
        "INSERT DATA { GRAPH ex:g { ex:a ex:knows ex:b . ex:b ex:name \"gb\" } }",
    )
    .await;
    support::rebuild_and_publish_index(&fluree, id).await;
    let ledger = fluree.ledger(id).await.expect("reload");

    let (rows, lookups) = sparql(
        &fluree,
        &ledger,
        "SELECT ?p ?o WHERE { GRAPH ex:g { ex:a ex:knows ?x . ?x ?p ?o } }",
    )
    .await;
    assert_eq!(rows, support::normalize_rows(&json!([["ex:name", "gb"]])));
    assert_eq!(lookups, 1);
}

#[tokio::test]
async fn two_ledgers_with_colliding_ids_keep_their_own_subjects() {
    let fluree = FlureeBuilder::memory().build_memory();
    // Same shape in the same order, so `ex:one` and `ex:two` take one id.
    indexed(
        &fluree,
        "it/correlated-ids/l1:main",
        "ex:a1 ex:knows ex:one . ex:one ex:name \"one\" .",
    )
    .await;
    indexed(
        &fluree,
        "it/correlated-ids/l2:main",
        "ex:a2 ex:knows ex:two . ex:two ex:name \"two\" .",
    )
    .await;
    let (spans, _guard) = support::span_capture::init_test_tracing();
    let result = fluree
        .query_connection_sparql(
            "PREFIX ex: <http://example.org/>
             SELECT ?p ?o
             FROM <it/correlated-ids/l1:main> FROM <it/correlated-ids/l2:main>
             WHERE { ex:a1 ex:knows ?x . ?x ?p ?o }",
        )
        .await
        .expect("dataset query");
    let l1 = fluree
        .ledger("it/correlated-ids/l1:main")
        .await
        .expect("load");
    let rows = result.to_jsonld(&l1.snapshot).expect("to_jsonld");
    assert_eq!(
        support::normalize_rows(&rows),
        support::normalize_rows(&json!([["ex:name", "one"]])),
    );
    // Across ledgers a row carries the IRI, never an id to take.
    assert_eq!(stamps(&spans, "proceed"), 0);
}

#[tokio::test]
async fn a_policy_fallback_stays_bound_to_the_probed_subject() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/correlated-ids/policy:main";
    indexed(&fluree, id, DATA).await;
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "from": id,
        "opts": {
            "policy": [{
                "@id": "ex:hideSecret",
                "f:required": true,
                "f:onProperty": [{"@id": "http://example.org/secret"}],
                "f:action": "f:view",
                "f:allow": false
            }],
            "default-allow": true
        },
        "select": ["?x", "?p", "?o"],
        "where": [{"@id": "ex:a", "ex:knows": "?x"}, {"@id": "?x", "?p": "?o"}]
    });
    let result = fluree.query_connection(&query).await.expect("query");
    let ledger = fluree.ledger(id).await.expect("ledger");
    let rows = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        support::normalize_rows(&rows),
        support::normalize_rows(&json!([
            ["ex:b", "ex:age", 1],
            ["ex:b", "ex:name", "b"],
            ["ex:c", "ex:knows", "ex:a"],
            ["ex:c", "ex:name", "c"],
        ])),
    );
}
