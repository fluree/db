//! Graph-scoped writes: sync of the default graph, graph insert into the
//! default graph or a named graph, and `graph_exists`. These back the Graph
//! Store Protocol's `PUT` (sync), `POST` (insert) and its status codes.

#![cfg(feature = "native")]

use crate::support::genesis_ledger;
use fluree_db_api::{
    FlureeBuilder, GraphPayload, GraphSel, SyncGraphOpts, SyncGraphReport, TxnOpts,
};
use serde_json::{json, Value as JsonValue};

const NAMED: &str = "http://example.org/graphs/tools";
const OTHER: &str = "http://example.org/graphs/other";

const PREFIX: &str = "@prefix ex: <http://example.org/> .\n";

/// Seed one default-graph triple and one triple in OTHER.
async fn seed(fluree: &fluree_db_api::Fluree, ledger_id: &str) {
    let trig = format!(
        "{PREFIX}ex:seed ex:p \"default\" .\nGRAPH <{OTHER}> {{ ex:zed ex:name \"Zed\" . }}\n"
    );
    fluree
        .stage_owned(genesis_ledger(fluree, ledger_id))
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("seed");
}

async fn rows(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    graph: Option<&str>,
) -> Vec<JsonValue> {
    let from = match graph {
        Some(iri) => format!("{ledger_id}#{iri}"),
        None => ledger_id.to_string(),
    };
    let q = json!({
        "from": from,
        "select": ["?s", "?p", "?o"],
        "where": {"@id": "?s", "?p": "?o"},
        "orderBy": ["?s", "?p", "?o"]
    });
    let result = fluree.query_connection(&q).await.expect("query");
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    let rows = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    rows.as_array().cloned().unwrap_or_default()
}

async fn count(fluree: &fluree_db_api::Fluree, ledger_id: &str, graph: Option<&str>) -> usize {
    rows(fluree, ledger_id, graph).await.len()
}

async fn sync(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    graph: &GraphSel,
    payload: GraphPayload<'_>,
    allow_empty: bool,
) -> fluree_db_api::Result<SyncGraphReport> {
    let opts = SyncGraphOpts {
        allow_empty,
        ..Default::default()
    };
    fluree
        .sync_graph_with(ledger_id, graph, payload, opts, TxnOpts::default(), None)
        .await
}

async fn insert(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    graph: GraphSel,
    payload: GraphPayload<'_>,
) -> fluree_db_api::Result<fluree_db_api::TransactResultRef> {
    let handle = fluree.ledger_cached(ledger_id).await.expect("handle");
    fluree
        .stage(&handle)
        .insert_graph_payload(graph, payload)
        .execute()
        .await
}

#[tokio::test]
async fn default_graph_sync_replaces_only_the_default_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-writes/default-sync:main";
    seed(&fluree, ledger_id).await;

    let v1 = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            { "@id": "ex:alice", "ex:name": "Alice", "ex:role": "engineer" },
            { "@id": "ex:bob", "ex:name": "Bob" }
        ]
    });
    let first = sync(
        &fluree,
        ledger_id,
        &GraphSel::Default,
        GraphPayload::JsonLd(&v1),
        false,
    )
    .await
    .expect("default-graph sync");
    // The seed triple is not in the payload, so it goes.
    assert_eq!((first.asserted, first.retracted), (3, 1), "{first:?}");
    assert_eq!(first.graph_iri, None);
    assert_eq!(count(&fluree, ledger_id, None).await, 3);
    assert_eq!(
        count(&fluree, ledger_id, Some(OTHER)).await,
        1,
        "named graphs untouched"
    );

    // The same graph as Turtle stages the same flakes.
    let turtle = format!(
        "{PREFIX}ex:alice ex:name \"Alice\" ; ex:role \"engineer\" .\nex:bob ex:name \"Bob\" .\n"
    );
    let again = sync(
        &fluree,
        ledger_id,
        &GraphSel::Default,
        GraphPayload::Rdf(&turtle),
        false,
    )
    .await
    .expect("Turtle resync");
    assert!(
        !again.committed && again.asserted + again.retracted == 0,
        "{again:?}"
    );

    let v2 = format!("{PREFIX}ex:alice ex:name \"Alice\" ; ex:role \"manager\" .\n");
    let delta = sync(
        &fluree,
        ledger_id,
        &GraphSel::Default,
        GraphPayload::Rdf(&v2),
        false,
    )
    .await
    .expect("delta");
    assert_eq!((delta.asserted, delta.retracted), (1, 2), "{delta:?}");
    assert_eq!(count(&fluree, ledger_id, Some(OTHER)).await, 1);
}

#[tokio::test]
async fn default_graph_sync_keeps_blank_nodes_and_guards_empty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-writes/default-bnodes:main";
    seed(&fluree, ledger_id).await;

    let spec = format!("{PREFIX}ex:search ex:param [ ex:name \"q\" ; ex:required true ] .\n");
    let first = sync(
        &fluree,
        ledger_id,
        &GraphSel::Default,
        GraphPayload::Rdf(&spec),
        false,
    )
    .await
    .expect("sync");
    assert!(first.committed);
    let again = sync(
        &fluree,
        ledger_id,
        &GraphSel::Default,
        GraphPayload::Rdf(&spec),
        false,
    )
    .await
    .expect("resync");
    assert!(
        !again.committed,
        "blank nodes keep their identity in the default graph: {again:?}"
    );

    // A GRAPH block names some graph other than the default one.
    let trig = format!("{PREFIX}GRAPH <{NAMED}> {{ ex:a ex:p \"x\" . }}\n");
    let err = sync(
        &fluree,
        ledger_id,
        &GraphSel::Default,
        GraphPayload::Rdf(&trig),
        false,
    )
    .await
    .expect_err("a GRAPH block cannot fill the default graph");
    assert!(err.to_string().contains(NAMED), "{err}");

    let err = sync(
        &fluree,
        ledger_id,
        &GraphSel::Default,
        GraphPayload::Rdf(PREFIX),
        false,
    )
    .await
    .expect_err("empty without allowEmpty");
    assert!(err.to_string().contains("allowEmpty"), "{err}");
    let cleared = sync(
        &fluree,
        ledger_id,
        &GraphSel::Default,
        GraphPayload::Rdf(PREFIX),
        true,
    )
    .await
    .expect("allowEmpty clears");
    assert_eq!(cleared.retracted, 3, "{cleared:?}");
    assert_eq!(count(&fluree, ledger_id, None).await, 0);
    assert_eq!(count(&fluree, ledger_id, Some(OTHER)).await, 1);
}

#[tokio::test]
async fn graph_insert_adds_without_retracting() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-writes/insert:main";
    seed(&fluree, ledger_id).await;
    let named = || GraphSel::Graph(NAMED.to_string());

    let first = format!("{PREFIX}ex:alice ex:name \"Alice\" .\n");
    insert(&fluree, ledger_id, named(), GraphPayload::Rdf(&first))
        .await
        .expect("insert into a new named graph");
    let second = json!({
        "@context": { "ex": "http://example.org/" },
        "@id": "ex:alice", "ex:role": "engineer"
    });
    insert(&fluree, ledger_id, named(), GraphPayload::JsonLd(&second))
        .await
        .expect("JSON-LD insert into the same graph");
    assert_eq!(count(&fluree, ledger_id, Some(NAMED)).await, 2, "both kept");

    // Blank nodes are fresh per insert: the same document twice is two
    // parameter nodes (RDF merge), where a sync would keep one.
    let spec = format!("{PREFIX}ex:search ex:param [ ex:name \"q\" ] .\n");
    for _ in 0..2 {
        insert(&fluree, ledger_id, named(), GraphPayload::Rdf(&spec))
            .await
            .expect("insert blank-node structure");
    }
    let params = rows(&fluree, ledger_id, Some(NAMED))
        .await
        .into_iter()
        .filter(|row| row[1] == "http://example.org/param")
        .count();
    assert_eq!(params, 2);

    insert(
        &fluree,
        ledger_id,
        GraphSel::Default,
        GraphPayload::Rdf(&format!("{PREFIX}ex:d ex:p \"x\" .\n")),
    )
    .await
    .expect("insert into the default graph");
    assert_eq!(count(&fluree, ledger_id, None).await, 2);
    assert_eq!(count(&fluree, ledger_id, Some(OTHER)).await, 1);
}

#[tokio::test]
async fn graph_insert_refusals() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-writes/insert-refusals:main";
    seed(&fluree, ledger_id).await;
    let named = || GraphSel::Graph(NAMED.to_string());

    for empty in ["", PREFIX] {
        let err = insert(&fluree, ledger_id, named(), GraphPayload::Rdf(empty))
            .await
            .expect_err("nothing to add");
        assert!(err.to_string().contains("nothing to add"), "{err}");
    }
    let empty_json = json!({ "@graph": [] });
    let err = insert(
        &fluree,
        ledger_id,
        named(),
        GraphPayload::JsonLd(&empty_json),
    )
    .await
    .expect_err("nothing to add");
    assert!(err.to_string().contains("nothing to add"), "{err}");

    let other_block = format!("{PREFIX}GRAPH <{OTHER}> {{ ex:a ex:p \"x\" . }}\n");
    let err = insert(&fluree, ledger_id, named(), GraphPayload::Rdf(&other_block))
        .await
        .expect_err("a block for another graph");
    assert!(err.to_string().contains(OTHER), "{err}");

    let addressed = json!({
        "@context": { "ex": "http://example.org/" },
        "@id": OTHER,
        "@graph": [{ "@id": "ex:a", "ex:p": "x" }]
    });
    // The insert parser has no named-graph form, so this fails to parse; a
    // parser that learned one would meet `parse_graph_insert`'s guard.
    insert(
        &fluree,
        ledger_id,
        named(),
        GraphPayload::JsonLd(&addressed),
    )
    .await
    .expect_err("a JSON-LD payload naming its own graph");
    assert!(!fluree.graph_exists(ledger_id, &named()).await.unwrap());
}

#[tokio::test]
async fn graph_exists_tracks_whether_a_named_graph_holds_triples() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-writes/exists:main";
    seed(&fluree, ledger_id).await;
    let named = GraphSel::Graph(NAMED.to_string());

    assert!(fluree
        .graph_exists(ledger_id, &GraphSel::Default)
        .await
        .unwrap());
    assert!(!fluree.graph_exists(ledger_id, &named).await.unwrap());
    let other = GraphSel::Graph(OTHER.to_string());
    assert!(fluree.graph_exists(ledger_id, &other).await.unwrap());

    let turtle = format!("{PREFIX}ex:a ex:p \"x\" .\n");
    insert(
        &fluree,
        ledger_id,
        named.clone(),
        GraphPayload::Rdf(&turtle),
    )
    .await
    .expect("insert");
    assert!(fluree.graph_exists(ledger_id, &named).await.unwrap());

    // Emptied by a sync, the graph is registered but no longer exists.
    sync(&fluree, ledger_id, &named, GraphPayload::Rdf(""), true)
        .await
        .expect("sync to empty");
    assert!(!fluree.graph_exists(ledger_id, &named).await.unwrap());
}
