//! Which `f:embeddingVector` values a write accepts.
//!
//! The index keeps each property's vectors in each graph at one length: the
//! length of the first vector the property received there, history included.
//! A vector has at most 65,535 elements. Every write path refuses a vector
//! that breaks either rule, so whatever a ledger accepts can be indexed.

#![cfg(feature = "native")]

use crate::support::{self, MemoryFluree, MemoryLedger};
use fluree_db_api::LedgerHandle;
use fluree_db_api::{
    ApiError, Base64Bytes, ConflictStrategy, FlureeBuilder, GovernanceOptions, IndexConfig,
    PushCommitsRequest, TransactError,
};
use fluree_db_core::commit::codec::{read_commit, write_commit};
use fluree_db_core::plan_commit_transfer;
use fluree_db_core::vector::MAX_VECTOR_DIMS;
use fluree_db_core::FlakeValue;
use serde_json::json;
use std::collections::HashMap;

const EX: &str = "http://example.org/";
const PREFIXES: &str = "PREFIX ex: <http://example.org/> PREFIX f: <https://ns.flur.ee/db#>\n";
const TURTLE_PREFIXES: &str =
    "@prefix ex: <http://example.org/> .\n@prefix f: <https://ns.flur.ee/db#> .\n";

/// `len` elements, each exact in f32.
fn numbers(len: usize) -> Vec<f64> {
    (0..len).map(|i| ((i % 64) as f64 + 1.0) / 8.0).collect()
}

/// The `f:embeddingVector` lexical form of [`numbers`].
fn lexical(len: usize) -> String {
    let parts: Vec<String> = numbers(len).iter().map(f64::to_string).collect();
    format!("[{}]", parts.join(", "))
}

fn ctx() -> serde_json::Value {
    json!({"ex": EX, "ex:embedding": {"@type": "@vector"}, "ex:other": {"@type": "@vector"}})
}

/// `ex:{s} ex:{p} <vector of len>` as a JSON-LD node.
fn node(s: &str, p: &str, len: usize) -> serde_json::Value {
    json!({"@id": format!("ex:{s}"), format!("ex:{p}"): numbers(len)})
}

/// The write paths a client reaches.
#[derive(Clone, Copy, Debug)]
enum Surface {
    JsonLdInsert,
    JsonLdUpsert,
    JsonLdUpdate,
    SparqlInsertData,
    SparqlInsertWhere,
    TurtleInsert,
    TurtleUpsert,
}

const SURFACES: [Surface; 7] = [
    Surface::JsonLdInsert,
    Surface::JsonLdUpsert,
    Surface::JsonLdUpdate,
    Surface::SparqlInsertData,
    Surface::SparqlInsertWhere,
    Surface::TurtleInsert,
    Surface::TurtleUpsert,
];

/// A handle on `ledger_id` loaded fresh from the nameservice, so it holds
/// every commit and the latest published index.
async fn handle(fluree: &MemoryFluree, ledger_id: &str) -> LedgerHandle {
    fluree.disconnect_ledger(ledger_id).await;
    fluree.ledger_cached(ledger_id).await.expect("load ledger")
}

/// Write `ex:{s} ex:embedding <vector of len>` through `surface`.
async fn write(
    fluree: &MemoryFluree,
    ledger_id: &str,
    surface: Surface,
    s: &str,
    len: usize,
) -> fluree_db_api::Result<()> {
    let data = json!({"@context": ctx(), "@graph": [node(s, "embedding", len)]});
    let update = json!({"@context": ctx(), "insert": node(s, "embedding", len)});
    let triple = format!(
        "ex:{s} ex:embedding \"{}\"^^f:embeddingVector .",
        lexical(len)
    );
    let insert_data = format!("{PREFIXES}INSERT DATA {{ {triple} }}");
    let insert_where = format!("{PREFIXES}INSERT {{ {triple} }} WHERE {{ }}");
    let turtle = format!("{TURTLE_PREFIXES}{triple}\n");
    let handle = handle(fluree, ledger_id).await;
    let builder = fluree.stage(&handle);
    let builder = match surface {
        Surface::JsonLdInsert => builder.insert(&data),
        Surface::JsonLdUpsert => builder.upsert(&data),
        Surface::JsonLdUpdate => builder.update(&update),
        Surface::SparqlInsertData => builder.sparql_update(&insert_data),
        Surface::SparqlInsertWhere => builder.sparql_update(&insert_where),
        Surface::TurtleInsert => builder.insert_turtle(&turtle),
        Surface::TurtleUpsert => builder.upsert_turtle(&turtle),
    };
    builder.execute().await.map(|_| ())
}

async fn create(fluree: &MemoryFluree, ledger_id: &str) {
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
}

/// The ledger's commit `t`.
async fn t(fluree: &MemoryFluree, ledger_id: &str) -> i64 {
    fluree.ledger(ledger_id).await.expect("load ledger").t()
}

/// The refusal message, or a panic naming `what` if the write was accepted
/// or failed some other way. A refused vector is HTTP 422, as a write past
/// the datatype limit is.
fn refused<T>(result: fluree_db_api::Result<T>, what: &str) -> String {
    match result {
        Err(e) => {
            let status = e.status_code();
            match e {
                ApiError::Transact(TransactError::InvalidVector(msg)) => {
                    assert_eq!(status, 422, "{what}: {msg}");
                    msg
                }
                e => panic!("{what}: expected an invalid-vector refusal, got: {e}"),
            }
        }
        Ok(_) => panic!("{what}: the write was accepted"),
    }
}

/// Insert JSON-LD nodes in one transaction.
async fn insert(
    fluree: &MemoryFluree,
    ledger_id: &str,
    nodes: Vec<serde_json::Value>,
) -> fluree_db_api::Result<()> {
    let data = json!({"@context": ctx(), "@graph": nodes});
    let handle = handle(fluree, ledger_id).await;
    fluree
        .stage(&handle)
        .insert(&data)
        .execute()
        .await
        .map(|_| ())
}

/// SPARQL UPDATE.
async fn sparql(fluree: &MemoryFluree, ledger_id: &str, body: &str) -> fluree_db_api::Result<()> {
    let update = format!("{PREFIXES}{body}");
    let handle = handle(fluree, ledger_id).await;
    fluree
        .stage(&handle)
        .sparql_update(&update)
        .execute()
        .await
        .map(|_| ())
}

/// The number of elements in a vector as a query returns it.
fn vector_len(v: &serde_json::Value) -> usize {
    match v {
        serde_json::Value::Array(elements) => elements.len(),
        serde_json::Value::Object(o) => vector_len(o.get("@value").expect("@value")),
        serde_json::Value::String(s) => s.trim_matches(['[', ']']).split(',').count(),
        other => panic!("not a vector: {other}"),
    }
}

/// `(graph, length)` for every vector on `ex:embedding` in `graphs`
/// (`"default"` or a name under `ex:`), sorted.
async fn vector_lengths(
    fluree: &MemoryFluree,
    ledger: &MemoryLedger,
    graphs: &[&str],
) -> Vec<(String, usize)> {
    let mut out = Vec::new();
    for &graph in graphs {
        let q = if graph == "default" {
            format!("{PREFIXES}SELECT ?v WHERE {{ ?s ex:embedding ?v }}")
        } else {
            format!(
                "{PREFIXES}SELECT ?v FROM NAMED ex:{graph} \
                 WHERE {{ GRAPH ex:{graph} {{ ?s ex:embedding ?v }} }}"
            )
        };
        let rows = support::query_sparql_formatted(fluree, ledger, &q)
            .await
            .expect("query vectors");
        for row in rows.as_array().expect("rows") {
            let v = row.as_array().map_or(row, |cols| &cols[0]);
            out.push((graph.to_string(), vector_len(v)));
        }
    }
    out.sort();
    out
}

/// Rebuild the ledger's index from its commits, publish it, and load the
/// ledger over it.
async fn rebuild(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    support::rebuild_and_publish_index(fluree, ledger_id).await;
    let ledger = fluree.ledger(ledger_id).await.expect("load indexed ledger");
    assert_eq!(ledger.index_t(), ledger.t(), "fully indexed");
    ledger
}

/// Build the index incrementally over the published one, and load it.
async fn build_incremental(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    support::build_and_publish_index(fluree, ledger_id).await;
    let ledger = fluree.ledger(ledger_id).await.expect("load indexed ledger");
    assert_eq!(ledger.index_t(), ledger.t(), "fully indexed");
    ledger
}

/// The first vector a property receives sets its length, in novelty and
/// once indexed; a refused write commits nothing; the ledger indexes.
#[tokio::test]
async fn a_property_keeps_the_length_of_its_first_vector() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "vector-lengths/first:main";
    create(&fluree, id).await;
    insert(&fluree, id, vec![node("a", "embedding", 3)])
        .await
        .expect("the first vector");
    let msg = refused(
        insert(&fluree, id, vec![node("b", "embedding", 2)]).await,
        "a second length, from novelty",
    );
    assert!(
        msg.contains("<http://example.org/embedding>") && msg.contains("expected 3, got 2"),
        "{msg}"
    );
    assert_eq!(t(&fluree, id).await, 1, "a refused write commits nothing");
    insert(
        &fluree,
        id,
        vec![node("b", "embedding", 3), node("c", "other", 2)],
    )
    .await
    .expect("the same length, and another property with its own");

    rebuild(&fluree, id).await;
    refused(
        insert(&fluree, id, vec![node("d", "embedding", 4)]).await,
        "a second length, from the index",
    );
    refused(
        insert(&fluree, id, vec![node("d", "other", 3)]).await,
        "the other property's length, from the index",
    );
    insert(&fluree, id, vec![node("d", "embedding", 3)])
        .await
        .expect("the indexed length");
    refused(
        insert(&fluree, id, vec![node("e", "embedding", 2)]).await,
        "a second length, from novelty over the index",
    );
    let ledger = build_incremental(&fluree, id).await;
    assert_eq!(
        vector_lengths(&fluree, &ledger, &["default"])
            .await
            .into_iter()
            .map(|(_, len)| len)
            .collect::<Vec<_>>(),
        vec![3, 3, 3]
    );
}

/// One transaction cannot give a property two lengths, and a length set
/// earlier in the same transaction counts.
#[tokio::test]
async fn one_transaction_holds_one_length_per_property() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "vector-lengths/one-txn:main";
    create(&fluree, id).await;
    refused(
        insert(
            &fluree,
            id,
            vec![node("a", "embedding", 3), node("b", "embedding", 2)],
        )
        .await,
        "two lengths in one transaction",
    );
    refused(
        sparql(
            &fluree,
            id,
            &format!(
                "INSERT DATA {{ ex:a ex:embedding \"{}\"^^f:embeddingVector . \
                 ex:a ex:embedding \"{}\"^^f:embeddingVector . }}",
                lexical(2),
                lexical(5)
            ),
        )
        .await,
        "two lengths on one subject in one SPARQL update",
    );
    assert_eq!(t(&fluree, id).await, 0, "nothing committed");

    insert(
        &fluree,
        id,
        vec![
            node("a", "embedding", 3),
            node("b", "embedding", 3),
            node("c", "other", 5),
        ],
    )
    .await
    .expect("one length per property");
    rebuild(&fluree, id).await;
}

/// Each named graph keeps its own length for a property, as the index
/// stores one arena per graph.
#[tokio::test]
async fn each_graph_keeps_its_own_length() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "vector-lengths/graphs:main";
    create(&fluree, id).await;
    sparql(
        &fluree,
        id,
        &format!(
            "INSERT DATA {{ ex:a ex:embedding \"{}\"^^f:embeddingVector . \
             GRAPH ex:g1 {{ ex:a ex:embedding \"{}\"^^f:embeddingVector . }} \
             GRAPH ex:g2 {{ ex:a ex:embedding \"{}\"^^f:embeddingVector . }} }}",
            lexical(3),
            lexical(2),
            lexical(4)
        ),
    )
    .await
    .expect("one length per graph");

    let in_graph = |g: &str, len: usize| {
        format!(
            "INSERT DATA {{ GRAPH ex:{g} {{ ex:b ex:embedding \"{}\"^^f:embeddingVector . }} }}",
            lexical(len)
        )
    };
    let msg = refused(
        sparql(&fluree, id, &in_graph("g1", 3)).await,
        "the default graph's length in g1, from novelty",
    );
    assert!(msg.contains("in graph <http://example.org/g1>"), "{msg}");
    sparql(&fluree, id, &in_graph("g1", 2))
        .await
        .expect("g1's own length");

    rebuild(&fluree, id).await;
    refused(
        sparql(&fluree, id, &in_graph("g1", 3)).await,
        "the default graph's length in g1, from the index",
    );
    refused(
        insert(&fluree, id, vec![node("b", "embedding", 2)]).await,
        "g1's length in the default graph, from the index",
    );
    let trig = |len: usize| {
        format!(
            "{TURTLE_PREFIXES}GRAPH ex:g1 {{ ex:c ex:embedding \"{}\"^^f:embeddingVector . }}\n",
            lexical(len)
        )
    };
    let refused_trig = trig(3);
    let handle = self::handle(&fluree, id).await;
    refused(
        fluree
            .stage(&handle)
            .insert_turtle(&refused_trig)
            .execute()
            .await,
        "the default graph's length in g1 by TriG",
    );
    let kept_trig = trig(2);
    let handle = self::handle(&fluree, id).await;
    fluree
        .stage(&handle)
        .insert_turtle(&kept_trig)
        .execute()
        .await
        .expect("g1's own length by TriG");
    sparql(&fluree, id, &in_graph("g3", 5))
        .await
        .expect("a new graph takes any length");
    let ledger = build_incremental(&fluree, id).await;
    assert_eq!(
        vector_lengths(&fluree, &ledger, &["default", "g1", "g2", "g3"]).await,
        [
            ("default", 3),
            ("g1", 2),
            ("g1", 2),
            ("g1", 2),
            ("g2", 4),
            ("g3", 5)
        ]
        .map(|(g, len)| (g.to_string(), len))
        .to_vec()
    );
}

/// Retracting every vector of a property keeps its length: the index keeps
/// history, so the retracted vectors are still stored.
#[tokio::test]
async fn retracting_the_vectors_keeps_the_length() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "vector-lengths/retract:main";
    create(&fluree, id).await;
    insert(&fluree, id, vec![node("a", "embedding", 3)])
        .await
        .unwrap();
    sparql(&fluree, id, "DELETE WHERE { ex:a ex:embedding ?v }")
        .await
        .expect("retract the only vector");
    let ledger = fluree.ledger(id).await.unwrap();
    assert!(vector_lengths(&fluree, &ledger, &["default"])
        .await
        .is_empty());
    refused(
        insert(&fluree, id, vec![node("a", "embedding", 2)]).await,
        "a new length after a retraction, from novelty",
    );

    rebuild(&fluree, id).await;
    refused(
        insert(&fluree, id, vec![node("a", "embedding", 2)]).await,
        "a new length after a retraction, from the index",
    );
    insert(&fluree, id, vec![node("a", "embedding", 3)])
        .await
        .expect("the first length");
    build_incremental(&fluree, id).await;
}

/// The refusal of a write to a ledger whose index this process has not
/// loaded: the process's state, not the request's, so 503 and retryable.
fn not_loaded<T>(result: fluree_db_api::Result<T>, what: &str) -> String {
    match result {
        Err(e) => {
            let status = e.status_code();
            match e {
                ApiError::Transact(TransactError::IndexNotLoaded(msg)) => {
                    assert_eq!(status, 503, "{what}: {msg}");
                    msg
                }
                e => panic!("{what}: expected an index-not-loaded refusal, got: {e}"),
            }
        }
        Ok(_) => panic!("{what}: the write was accepted"),
    }
}

/// The ledger as this process holds it when its binary store failed to load
/// (`ledger_manager` logs "continuing without"): the snapshot at the index's
/// `t`, no range provider, no store, and dictionaries seeded from novelty
/// alone.
async fn unloaded(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .unwrap()
        .expect("ledger record");
    let ledger =
        fluree_db_api::LedgerState::load_with_store(fluree.content_store(ledger_id), record)
            .await
            .expect("load without the store");
    assert!(ledger.index_t() > 0 && ledger.snapshot.range_provider.is_none());
    ledger
}

/// A ledger whose index this process has not loaded refuses every write
/// before its first read, rather than failing inside one with an error that
/// reads as the request's fault, or committing without the checks that need
/// the index.
#[tokio::test]
async fn a_ledger_whose_index_is_not_loaded_refuses_writes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "vector-lengths/not-loaded:main";
    create(&fluree, id).await;
    insert(&fluree, id, vec![node("a", "embedding", 3)])
        .await
        .unwrap();
    let indexed = rebuild(&fluree, id).await;
    let plain = json!({"@context": ctx(), "@graph": [{"@id": "ex:p", "ex:name": "x"}]});
    let update = json!({
        "@context": ctx(),
        "where": {"@id": "?s", "ex:embedding": "?v"},
        "insert": {"@id": "?s", "ex:name": "y"}
    });
    let results = [
        (
            "insert",
            fluree
                .insert(unloaded(&fluree, id).await, &plain)
                .await
                .map(drop),
        ),
        (
            "upsert",
            fluree
                .upsert(unloaded(&fluree, id).await, &plain)
                .await
                .map(drop),
        ),
        (
            "update",
            fluree
                .update(unloaded(&fluree, id).await, &update)
                .await
                .map(drop),
        ),
        (
            "vector insert",
            insert_state(
                &fluree,
                unloaded(&fluree, id).await,
                vec![node("c", "embedding", 3)],
            )
            .await,
        ),
    ];
    for (what, result) in results {
        let msg = not_loaded(result, what);
        assert!(msg.contains("is not loaded"), "{what}: {msg}");
    }
    // The committers' first read: the write's policy context.
    let ledger = unloaded(&fluree, id).await;
    not_loaded(
        fluree_db_api::build_transact_policy_context(
            &fluree,
            &ledger.snapshot,
            ledger.novelty.as_ref(),
            Some(ledger.novelty.as_ref()),
            ledger.t(),
            &GovernanceOptions::default(),
        )
        .await,
        "policy context",
    );
    assert_eq!(t(&fluree, id).await, indexed.t(), "nothing committed");
}

/// A ledger whose attached index is older than the index its novelty starts
/// from reads nothing indexed in between (a property's vector length, for
/// one), so a write to it is refused rather than certified.
#[tokio::test]
async fn an_attached_index_older_than_the_ledgers_refuses_writes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "vector-lengths/old-store:main";
    create(&fluree, id).await;
    insert(&fluree, id, vec![node("a", "embedding", 3)])
        .await
        .unwrap();
    let older = rebuild(&fluree, id).await;
    let older_store = older.binary_store.clone();
    let older_provider = older.snapshot.range_provider.clone();
    assert!(
        older_store.is_some() && older_provider.is_some(),
        "a loaded index has its store and range provider attached"
    );

    // `ex:other` first gets a length in the next index, not the older one.
    insert(&fluree, id, vec![node("b", "other", 2)])
        .await
        .unwrap();
    let mut ledger = rebuild(&fluree, id).await;
    assert!(ledger.index_t() > older.index_t());
    assert!(ledger.novelty.is_empty());

    // The newer index installed, but the older store and its range provider
    // left attached. Both stay consistent with each other, so reads through
    // the provider still work; they miss what was indexed in between.
    ledger.binary_store = older_store;
    std::sync::Arc::make_mut(&mut ledger.snapshot).range_provider = older_provider;
    let plain = json!({"@context": ctx(), "@graph": [{"@id": "ex:p", "ex:name": "x"}]});
    for (what, result) in [
        (
            "a vector",
            insert_state(&fluree, ledger.clone(), vec![node("c", "other", 5)]).await,
        ),
        (
            "no vector",
            fluree.insert(ledger.clone(), &plain).await.map(drop),
        ),
    ] {
        let msg = not_loaded(result, what);
        assert!(msg.contains("older than the ledger's"), "{what}: {msg}");
    }
}

/// Insert JSON-LD nodes in one transaction on a given ledger state.
async fn insert_state(
    fluree: &MemoryFluree,
    ledger: MemoryLedger,
    nodes: Vec<serde_json::Value>,
) -> fluree_db_api::Result<()> {
    let data = json!({"@context": ctx(), "@graph": nodes});
    fluree.insert(ledger, &data).await.map(|_| ())
}

/// A vector has at most 65,535 elements, on every write surface.
#[tokio::test]
async fn a_vector_has_at_most_65535_elements() {
    let fluree = FlureeBuilder::memory().build_memory();
    for (i, surface) in SURFACES.into_iter().enumerate() {
        let id = format!("vector-lengths/longest-{i}:main");
        create(&fluree, &id).await;
        let msg = refused(
            write(&fluree, &id, surface, "a", MAX_VECTOR_DIMS + 1).await,
            &format!("{surface:?}: 65,536 elements"),
        );
        assert!(msg.contains("65536"), "{surface:?}: {msg}");
        assert_eq!(t(&fluree, &id).await, 0, "{surface:?}");
    }

    let id = "vector-lengths/longest:main";
    create(&fluree, id).await;
    write(&fluree, id, Surface::JsonLdInsert, "a", MAX_VECTOR_DIMS)
        .await
        .expect("65,535 elements");
    let ledger = rebuild(&fluree, id).await;
    assert_eq!(
        vector_lengths(&fluree, &ledger, &["default"]).await,
        vec![("default".to_string(), MAX_VECTOR_DIMS)],
        "the longest vector reads back whole from the index"
    );
}

/// Every write surface refuses a second length and accepts the first.
#[tokio::test]
async fn every_write_surface_keeps_one_length() {
    let fluree = FlureeBuilder::memory().build_memory();
    for (i, surface) in SURFACES.into_iter().enumerate() {
        let id = format!("vector-lengths/surface-{i}:main");
        create(&fluree, &id).await;
        write(&fluree, &id, surface, "a", 3)
            .await
            .unwrap_or_else(|e| panic!("{surface:?}: the first vector: {e}"));
        refused(
            write(&fluree, &id, surface, "b", 2).await,
            &format!("{surface:?}: a second length"),
        );
        // An upsert retracts `ex:a`'s vector first; the length stays.
        refused(
            write(&fluree, &id, surface, "a", 4).await,
            &format!("{surface:?}: a second length on the same subject"),
        );
        write(&fluree, &id, surface, "b", 3)
            .await
            .unwrap_or_else(|e| panic!("{surface:?}: the same length: {e}"));
        rebuild(&fluree, &id).await;
    }
}

/// Graph sync replaces a named graph's contents; the graph's length stays.
#[tokio::test]
async fn graph_sync_keeps_the_graphs_length() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "vector-lengths/sync:main";
    create(&fluree, id).await;
    let sync = |graph: &'static str, len: usize| {
        let fluree = &fluree;
        async move {
            let data = json!({"@context": ctx(), "@graph": [node("a", "embedding", len)]});
            let graph = format!("{EX}{graph}");
            let handle = handle(fluree, id).await;
            fluree
                .stage(&handle)
                .sync_graph(&graph, &data)
                .execute()
                .await
                .map(|_| ())
        }
    };
    sync("g", 3).await.expect("sync the first vector");
    refused(sync("g", 2).await, "a second length by sync");
    sync("h", 2).await.expect("another graph's own length");
    rebuild(&fluree, id).await;
}

/// A merge and a rebase write their source's vectors onto the target; a
/// second length on the target is refused.
#[tokio::test]
async fn merge_and_rebase_keep_one_length() {
    let fluree = FlureeBuilder::memory().build_memory();
    let name = "vector-lengths-branch";
    let main = "vector-lengths-branch:main";
    let dev = "vector-lengths-branch:dev";
    create(&fluree, main).await;
    insert(&fluree, main, vec![node("x", "other", 1)])
        .await
        .unwrap();
    fluree
        .create_branch(name, "dev", None, None)
        .await
        .expect("branch");
    insert(&fluree, dev, vec![node("a", "embedding", 2)])
        .await
        .expect("dev's first vector");
    insert(&fluree, main, vec![node("b", "embedding", 3)])
        .await
        .expect("main's first vector");

    refused(
        fluree
            .merge_branch(name, "dev", None, ConflictStrategy::default())
            .await,
        "merging dev's length into main",
    );
    refused(
        fluree
            .rebase_branch(name, "dev", ConflictStrategy::default())
            .await,
        "rebasing dev's length onto main",
    );
    rebuild(&fluree, main).await;
}

/// A pushed commit, with the txn blob it references.
struct PushedCommit {
    bytes: Vec<u8>,
    txn_blob: Option<(String, Vec<u8>)>,
}

/// The commits on `ledger_id`'s line, oldest first.
async fn line_commits(fluree: &MemoryFluree, ledger_id: &str) -> Vec<PushedCommit> {
    let store = fluree.branched_content_store(ledger_id).await.unwrap();
    let head = fluree
        .ledger(ledger_id)
        .await
        .unwrap()
        .head_commit_id
        .expect("ledger has a head");
    let plan = plan_commit_transfer(store.as_ref(), &head, None)
        .await
        .unwrap()
        .expect("a full line");
    let mut out = Vec::new();
    for cid in &plan.lineage {
        let bytes = store.get(cid).await.unwrap();
        let txn_blob = match read_commit(&bytes).unwrap().txn {
            Some(txn) => Some((txn.to_string(), store.get(&txn).await.unwrap())),
            None => None,
        };
        out.push(PushedCommit { bytes, txn_blob });
    }
    out
}

async fn push(
    fluree: &MemoryFluree,
    ledger_id: &str,
    commits: &[&PushedCommit],
) -> fluree_db_api::Result<()> {
    let request = PushCommitsRequest {
        commits: commits
            .iter()
            .map(|c| Base64Bytes(c.bytes.clone()))
            .collect(),
        blobs: commits
            .iter()
            .filter_map(|c| c.txn_blob.clone())
            .map(|(cid, bytes)| (cid, Base64Bytes(bytes)))
            .collect::<HashMap<_, _>>(),
        missing_blobs: Vec::new(),
        merged_commits: Vec::new(),
    };
    let index_config = IndexConfig {
        reindex_min_bytes: 100_000,
        reindex_max_bytes: 1_000_000_000,
    };
    fluree
        .push_commits(
            ledger_id,
            request,
            &GovernanceOptions::default(),
            &index_config,
        )
        .await
        .map(|_| ())
}

/// `commit` with every vector it asserts replaced by one of `len`.
fn with_vector_length(commit: &PushedCommit, len: usize) -> PushedCommit {
    let mut decoded = read_commit(&commit.bytes).unwrap();
    for flake in &mut decoded.flakes {
        if let FlakeValue::Vector(_) = flake.o {
            flake.o = FlakeValue::Vector(numbers(len).into());
        }
    }
    PushedCommit {
        bytes: write_commit(&decoded, false, None).unwrap().bytes,
        txn_blob: commit.txn_blob.clone(),
    }
}

/// A sender that does not check vectors can push commits with a second
/// length. The receiver refuses them, whether the first length arrived in
/// the same push, is in novelty, or is in the index.
#[tokio::test]
async fn push_keeps_one_length() {
    let fluree = FlureeBuilder::memory().build_memory();
    let sender = "vector-lengths/push-sender:main";
    create(&fluree, sender).await;
    insert(&fluree, sender, vec![node("a", "embedding", 3)])
        .await
        .unwrap();
    insert(&fluree, sender, vec![node("b", "embedding", 3)])
        .await
        .unwrap();
    let [first, second]: [PushedCommit; 2] = line_commits(&fluree, sender)
        .await
        .try_into()
        .unwrap_or_else(|_| panic!("the sender has two commits"));
    let second_length = with_vector_length(&second, 2);
    let too_long = with_vector_length(&second, MAX_VECTOR_DIMS + 1);

    let control = "vector-lengths/push-control:main";
    create(&fluree, control).await;
    push(&fluree, control, &[&first, &second])
        .await
        .expect("the sender's own commits");

    let same_push = "vector-lengths/push-same:main";
    create(&fluree, same_push).await;
    refused(
        push(&fluree, same_push, &[&first, &second_length]).await,
        "a length set earlier in the same push",
    );
    let too_long_push = "vector-lengths/push-long:main";
    create(&fluree, too_long_push).await;
    // The length is known (3) when the long vector arrives; it is still too
    // long, not of the wrong length.
    let msg = refused(
        push(&fluree, too_long_push, &[&first, &too_long]).await,
        "65,536 elements by push",
    );
    assert_eq!(
        msg,
        "<http://example.org/embedding>: vector dims 65536 exceeds the maximum of 65535"
    );

    let later = "vector-lengths/push-later:main";
    create(&fluree, later).await;
    push(&fluree, later, &[&first])
        .await
        .expect("the first commit");
    refused(
        push(&fluree, later, &[&second_length]).await,
        "a length in the receiver's novelty",
    );
    rebuild(&fluree, later).await;
    // Push writes through the cached ledger; load it over the new index.
    fluree.disconnect_ledger(later).await;
    refused(
        push(&fluree, later, &[&second_length]).await,
        "a length in the receiver's index",
    );
    push(&fluree, later, &[&second])
        .await
        .expect("the same length");
    build_incremental(&fluree, later).await;
}

/// Import `data` (a file with extension `ext`) into a new file-backed ledger.
async fn import(
    fluree: &fluree_db_api::Fluree,
    data_dir: &std::path::Path,
    ledger_id: &str,
    ext: &str,
    data: &str,
) -> Result<(), fluree_db_api::ImportError> {
    let path = data_dir.join(format!("{}.{ext}", ledger_id.replace([':', '/'], "-")));
    std::fs::write(&path, data).unwrap();
    fluree
        .create(ledger_id)
        .import(&path)
        .threads(2)
        .memory_budget_mb(256)
        .execute()
        .await
        .map(|_| ())
}

/// The message of an import's refusal: the `Invalid vector` a transaction of
/// the same data gets.
fn import_refused(result: Result<(), fluree_db_api::ImportError>, what: &str) -> String {
    match result {
        Err(fluree_db_api::ImportError::Invalid(TransactError::InvalidVector(msg))) => msg,
        Err(e) => panic!("{what}: expected an invalid-vector refusal, got: {e}"),
        Ok(()) => panic!("{what}: the import was accepted"),
    }
}

/// Bulk import builds the index as it reads, and refuses what that index
/// cannot store with the refusal a transaction gets, naming the property and
/// the graph, on every import format.
#[tokio::test]
async fn bulk_import_keeps_one_length() {
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let triple = |s: &str, len: usize| {
        format!(
            "ex:{s} ex:embedding \"{}\"^^f:embeddingVector .\n",
            lexical(len)
        )
    };
    let turtle = |body: String| format!("{TURTLE_PREFIXES}{body}");

    let msg = import_refused(
        import(
            &fluree,
            data_dir.path(),
            "vector-lengths-import-two",
            "ttl",
            &turtle(format!("{}{}", triple("a", 3), triple("b", 2))),
        )
        .await,
        "two lengths on one property",
    );
    assert_eq!(
        msg,
        "<http://example.org/embedding>: vector dims mismatch: expected 3, got 2"
    );
    // A refused import publishes nothing: the ledger has no commits.
    assert_eq!(
        fluree
            .ledger("vector-lengths-import-two:main")
            .await
            .unwrap()
            .t(),
        0
    );

    let msg = import_refused(
        import(
            &fluree,
            data_dir.path(),
            "vector-lengths-import-long",
            "ttl",
            &turtle(triple("a", MAX_VECTOR_DIMS + 1)),
        )
        .await,
        "65,536 elements",
    );
    assert_eq!(
        msg,
        "<http://example.org/embedding>: vector dims 65536 exceeds the maximum of 65535"
    );

    // A named graph keeps its own length, and the refusal names it.
    let msg = import_refused(
        import(
            &fluree,
            data_dir.path(),
            "vector-lengths-import-trig",
            "trig",
            &format!(
                "{TURTLE_PREFIXES}{}ex:g1 {{ {}{} }}\n",
                triple("a", 2),
                triple("b", 3),
                triple("c", 4)
            ),
        )
        .await,
        "two lengths in a named graph",
    );
    assert_eq!(
        msg,
        "<http://example.org/embedding> in graph <http://example.org/g1>: \
         vector dims mismatch: expected 3, got 4"
    );

    // JSON-LD, which the Cypher and CSV imports also feed.
    let jsonld = json!({
        "@context": ctx(),
        "@graph": [node("a", "embedding", 3), node("b", "embedding", 2)]
    });
    let msg = import_refused(
        import(
            &fluree,
            data_dir.path(),
            "vector-lengths-import-jsonld",
            "jsonld",
            &jsonld.to_string(),
        )
        .await,
        "two lengths in JSON-LD",
    );
    assert!(
        msg.starts_with("<http://example.org/embedding>: vector dims mismatch"),
        "{msg}"
    );

    let id = "vector-lengths-import-ok:main";
    import(
        &fluree,
        data_dir.path(),
        "vector-lengths-import-ok",
        "ttl",
        &turtle(format!("{}{}", triple("a", 3), triple("b", 3))),
    )
    .await
    .expect("one length");
    let ledger = fluree.ledger(id).await.unwrap();
    assert_eq!(ledger.index_t(), ledger.t(), "import indexes what it reads");
    assert_eq!(
        vector_lengths(&fluree, &ledger, &["default"]).await.len(),
        2
    );
    refused(
        insert(&fluree, id, vec![node("c", "embedding", 2)]).await,
        "a second length over the imported index",
    );
}

fn copy_dir(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), &to).unwrap();
        }
    }
}

/// A ledger holding two vector lengths on one property
/// (`tests/fixtures/vector-lengths-4.2.3`, written by the v4.2.3 CLI) refuses
/// any other length, and its data moves to a new ledger by export and import
/// once one length is kept.
#[tokio::test]
async fn a_ledger_already_holding_two_lengths_moves_by_export_and_import() {
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/vector-lengths-4.2.3/storage");
    let db_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    copy_dir(&fixture, db_dir.path());
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let id = "ex:main";
    let ledger = fluree.ledger(id).await.expect("load the fixture");
    assert_eq!((ledger.index_t(), ledger.t()), (1, 2));

    insert(&fluree, id, vec![node("c", "other", 2)])
        .await
        .expect("a write that holds no vector of ex:embedding");
    refused(
        insert(&fluree, id, vec![node("c", "embedding", 4)]).await,
        "a third length",
    );

    let mut exported = Vec::new();
    fluree
        .export(id)
        .format(fluree_db_api::export::ExportFormat::NTriples)
        .write_to(&mut exported)
        .await
        .expect("export");
    let exported = String::from_utf8(exported).unwrap();
    let import_lines = |lines: Vec<&str>| {
        let path = data_dir.path().join(format!("{}.ttl", lines.len()));
        std::fs::write(&path, lines.join("\n")).unwrap();
        path
    };
    let all: Vec<&str> = exported.lines().collect();
    let err = fluree
        .create("ex-moved")
        .import(import_lines(all.clone()))
        .execute()
        .await
        .expect_err("the export holds both lengths");
    assert!(err.to_string().contains("vector dims mismatch"), "{err}");

    // Keep the vectors of three elements.
    let kept: Vec<&str> = all
        .into_iter()
        .filter(|line| !line.contains("embedding> \"[") || line.matches(',').count() == 2)
        .collect();
    fluree
        .create("ex-kept")
        .import(import_lines(kept))
        .execute()
        .await
        .expect("one length");
    let moved = fluree.ledger("ex-kept:main").await.unwrap();
    assert_eq!(moved.index_t(), moved.t(), "imported and indexed");
    assert_eq!(
        vector_lengths(&fluree, &moved, &["default"]).await,
        vec![("default".to_string(), 3)]
    );
}

/// Restore, like pull and clone, creates no commits: it copies a ledger's
/// commits and index as they are. A ledger an earlier release left with two
/// lengths on one property restores whole, and the copy refuses what its
/// source refuses.
#[tokio::test]
async fn a_restored_copy_refuses_what_its_source_refuses() {
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/vector-lengths-4.2.3/storage");
    let db_dir = tempfile::tempdir().unwrap();
    copy_dir(&fixture, db_dir.path());
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let mut archive = Vec::new();
    fluree
        .archive_ledger("ex:main", true, &mut archive)
        .await
        .expect("archive the fixture");
    let restored = fluree
        .restore_ledger("ex-restored", &mut std::io::Cursor::new(archive))
        .await
        .expect("a two-length ledger restores");
    assert_eq!((restored.index_t, restored.commit_t), (Some(1), 2));

    let id = "ex-restored:main";
    let copy = fluree.ledger(id).await.unwrap();
    let source = fluree.ledger("ex:main").await.unwrap();
    assert_eq!(
        vector_lengths(&fluree, &copy, &["default"]).await,
        vector_lengths(&fluree, &source, &["default"]).await,
        "the copy holds the source's vectors"
    );
    insert(&fluree, id, vec![node("c", "other", 2)])
        .await
        .expect("a write that holds no vector of ex:embedding");
    refused(
        insert(&fluree, id, vec![node("c", "embedding", 4)]).await,
        "a third length on the copy",
    );
}
