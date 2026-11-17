//! A BM25 index over a graph the ledger drops: the sync after the drop
//! rebuilds it from what the ledger still holds, and retires the snapshots
//! from before the drop, which hold what it took. Until that sync, searches
//! keep serving the snapshot they had.

use fluree_db_api::{Bm25CreateConfig, Fluree, FlureeBuilder};
use serde_json::{json, Value as JsonValue};

const PREFIX: &str = "PREFIX ex: <http://example.org/>";

async fn update(fluree: &Fluree, ledger_id: &str, sparql: &str) -> i64 {
    fluree
        .graph(ledger_id)
        .transact()
        .sparql_update(&format!("{PREFIX} {sparql}"))
        .commit()
        .await
        .unwrap_or_else(|e| panic!("{sparql}: {e}"))
        .receipt
        .t
}

/// Write `ex:<name>`, titled "<name> report" in the default graph and typed
/// `ex:Doc` in `typed_in` (the default graph when `None`). The title stays in
/// the default graph, the one whose properties an indexing query's select
/// reads.
async fn write_doc(fluree: &Fluree, ledger_id: &str, typed_in: Option<&str>, name: &str) -> i64 {
    let title = format!("ex:{name} ex:title \"{name} report\" .");
    let typed = format!("ex:{name} a ex:Doc .");
    let data = match typed_in {
        Some(graph) => format!("{title} GRAPH <{graph}> {{ {typed} }}"),
        None => format!("{title} {typed}"),
    };
    update(fluree, ledger_id, &format!("INSERT DATA {{ {data} }}")).await
}

async fn ledger(fluree: &Fluree, ledger_id: &str) {
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
}

async fn create_index(fluree: &Fluree, name: &str, ledger_id: &str, query: JsonValue) -> String {
    fluree
        .create_full_text_index(Bm25CreateConfig::new(name, ledger_id, query))
        .await
        .expect("create index")
        .graph_source_id
}

fn docs_of(index: &fluree_db_query::bm25::Bm25Index) -> Vec<String> {
    let mut docs: Vec<String> = index
        .iter_doc_keys()
        .map(|key| {
            // `sync_bm25_index_to` keeps the compact `@id` the query returns.
            key.subject_iri
                .trim_start_matches("http://example.org/")
                .trim_start_matches("ex:")
                .to_string()
        })
        .collect();
    docs.sort();
    docs
}

/// The documents of the snapshot a search as of `t` reads, or `None` when no
/// snapshot serves `t`.
async fn docs_at(fluree: &Fluree, index: &str, t: i64) -> Option<Vec<String>> {
    match fluree.load_bm25_index_at(index, t).await {
        Ok((snapshot, _)) => Some(docs_of(&snapshot)),
        Err(fluree_db_api::ApiError::NotFound(_)) => None,
        Err(e) => panic!("load at t={t}: {e}"),
    }
}

async fn docs_at_head(fluree: &Fluree, index: &str) -> Vec<String> {
    docs_of(&fluree.load_bm25_index(index).await.expect("head snapshot"))
}

fn default_graph_docs() -> JsonValue {
    json!({
        "@context": {"ex": "http://example.org/"},
        "where": [{"@id": "?x", "@type": "ex:Doc", "ex:title": "?title"}],
        "select": {"?x": ["@id", "ex:title"]}
    })
}

#[tokio::test]
async fn the_sync_after_a_drop_rebuilds_and_retires_the_snapshots_before_it() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "bm25-drop/default:main";
    ledger(&fluree, ledger_id).await;
    let t1 = write_doc(&fluree, ledger_id, None, "a").await;
    let index = create_index(&fluree, "drop-default", ledger_id, default_graph_docs()).await;
    let t2 = write_doc(&fluree, ledger_id, None, "b").await;
    let synced = fluree.sync_bm25_index(&index).await.unwrap();
    assert!(
        !synced.was_full_resync,
        "an ordinary write syncs incrementally"
    );

    update(&fluree, ledger_id, "DROP DEFAULT").await;
    let t4 = write_doc(&fluree, ledger_id, None, "c").await;

    // Searches lag a write until the next sync, and a drop is no exception.
    assert_eq!(docs_at_head(&fluree, &index).await, ["a", "b"]);
    assert_eq!(docs_at(&fluree, &index, t1).await.unwrap(), ["a"]);

    let synced = fluree.sync_bm25_index(&index).await.unwrap();
    assert!(synced.was_full_resync, "a drop takes the full rebuild");
    assert_eq!(docs_at_head(&fluree, &index).await, ["c"]);
    assert_eq!(docs_at(&fluree, &index, t4).await.unwrap(), ["c"]);
    for t in [t1, t2] {
        assert_eq!(
            docs_at(&fluree, &index, t).await,
            None,
            "the snapshot as of t={t} held what the drop took"
        );
    }
}

/// A full resync and a sync to a given `t` rebuild without the commit walk,
/// and retire the snapshots from before a drop all the same.
#[tokio::test]
async fn an_explicit_rebuild_retires_the_snapshots_before_a_drop() {
    for to_t in [false, true] {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger_id = format!("bm25-drop/rebuild-{to_t}:main");
        ledger(&fluree, &ledger_id).await;
        let t1 = write_doc(&fluree, &ledger_id, None, "a").await;
        let index = create_index(&fluree, "drop-rebuild", &ledger_id, default_graph_docs()).await;
        update(&fluree, &ledger_id, "DROP DEFAULT").await;
        let t3 = write_doc(&fluree, &ledger_id, None, "c").await;

        if to_t {
            fluree.sync_bm25_index_to(&index, t3, None).await.unwrap();
        } else {
            fluree.resync_bm25_index(&index).await.unwrap();
        }
        assert_eq!(docs_at_head(&fluree, &index).await, ["c"], "to_t={to_t}");
        assert_eq!(docs_at(&fluree, &index, t1).await, None, "to_t={to_t}");
    }
}

#[tokio::test]
async fn a_query_the_sync_cannot_track_still_retires_the_snapshots_before_a_drop() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "bm25-drop/untracked:main";
    ledger(&fluree, ledger_id).await;
    let t1 = write_doc(&fluree, ledger_id, None, "a").await;
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "where": [{"@id": "?x", "@type": "ex:Doc"}],
        "select": {"?x": ["*"]}
    });
    let index = create_index(&fluree, "drop-untracked", ledger_id, query).await;
    update(&fluree, ledger_id, "DROP DEFAULT").await;
    write_doc(&fluree, ledger_id, None, "c").await;

    let synced = fluree.sync_bm25_index(&index).await.unwrap();
    assert!(synced.was_full_resync);
    assert_eq!(docs_at_head(&fluree, &index).await, ["c"]);
    assert_eq!(docs_at(&fluree, &index, t1).await, None);
}

#[tokio::test]
async fn an_index_over_a_named_graph_rebuilds_only_for_a_drop_of_that_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "bm25-drop/named:main";
    ledger(&fluree, ledger_id).await;
    write_doc(&fluree, ledger_id, Some("http://example.org/h"), "h1").await;
    let t1 = write_doc(&fluree, ledger_id, Some("http://example.org/g"), "g1").await;
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "where": [
            {"@id": "?x", "ex:title": "?title"},
            ["graph", "ex:g", {"@id": "?x", "@type": "ex:Doc"}]
        ],
        "select": {"?x": ["@id", "ex:title"]}
    });
    let index = create_index(&fluree, "drop-named", ledger_id, query).await;
    assert_eq!(docs_at_head(&fluree, &index).await, ["g1"]);

    update(&fluree, ledger_id, "DROP GRAPH <http://example.org/h>").await;
    let synced = fluree.sync_bm25_index(&index).await.unwrap();
    assert!(!synced.was_full_resync, "the index reads nothing of <h>");
    assert_eq!(docs_at(&fluree, &index, t1).await.unwrap(), ["g1"]);

    update(&fluree, ledger_id, "DROP GRAPH <http://example.org/g>").await;
    let t5 = write_doc(&fluree, ledger_id, Some("http://example.org/g"), "g2").await;
    let synced = fluree.sync_bm25_index(&index).await.unwrap();
    assert!(synced.was_full_resync);
    assert_eq!(docs_at_head(&fluree, &index).await, ["g2"]);
    assert_eq!(docs_at(&fluree, &index, t5).await.unwrap(), ["g2"]);
    assert_eq!(docs_at(&fluree, &index, t1).await, None);
}

/// A graph named through an alias of `@graph`, or only inside a subquery, is
/// as much a graph the index reads as one a `graph` pattern names.
#[tokio::test]
async fn an_aliased_graph_and_a_subquery_graph_retire_the_snapshots_before_their_drop() {
    let context = json!({"ex": "http://example.org/", "in": "@graph"});
    let aliased = json!({
        "@context": context,
        "where": [
            {"@id": "?x", "ex:title": "?title"},
            {"@id": "?x", "in": "ex:g", "@type": "ex:Doc"}
        ],
        "select": {"?x": ["@id", "ex:title"]}
    });
    let subquery = json!({
        "@context": context,
        "where": [
            {"@id": "?x", "ex:title": "?title"},
            ["query", {
                "@context": context,
                "select": ["?x"],
                "where": [["graph", "ex:g", {"@id": "?x", "@type": "ex:Doc"}]]
            }]
        ],
        "select": {"?x": ["@id", "ex:title"]}
    });
    for (name, query) in [("aliased", aliased), ("subquery", subquery)] {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger_id = format!("bm25-drop/{name}:main");
        ledger(&fluree, &ledger_id).await;
        let t1 = write_doc(&fluree, &ledger_id, Some("http://example.org/g"), "a").await;
        let index = create_index(&fluree, name, &ledger_id, query).await;
        assert_eq!(docs_at_head(&fluree, &index).await, ["a"], "{name}");

        update(&fluree, &ledger_id, "DROP GRAPH <http://example.org/g>").await;
        fluree.sync_bm25_index(&index).await.unwrap();
        assert!(docs_at_head(&fluree, &index).await.is_empty(), "{name}");
        assert_eq!(docs_at(&fluree, &index, t1).await, None, "{name}");
    }
}
