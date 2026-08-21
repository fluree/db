//! Filtered-DELETE list-meta hydration against the `has_list_meta` flag.
//!
//! A `WHERE { ?s p ?o } DELETE { ?s p ?o }` retraction carries no list
//! position, so staging copies `m.i` from the currently-asserted flake
//! before the accumulator dedups. That lookup used to cost a full overlay
//! walk per retracted (subject, predicate) — quadratic in retractions ×
//! novelty (a 40k-retraction delete over a novelty-heavy ledger never
//! finished). Now:
//!
//! 1. ledgers whose index root AND novelty report no `@list` rows skip the
//!    lookup entirely (`IndexRoot.has_list_meta == Some(false)`), and
//! 2. ledgers with lists resolve every group from one overlay walk plus a
//!    base-only seek per group, merged with the same lifecycle rule
//!    `range_with_overlay` applies.
//!
//! These pin the flag's three states through a real index build and the
//! list-retraction semantics on the merged path.

#![cfg(feature = "native")]

mod support;
use crate::support::{
    query_jsonld_formatted, start_background_indexer_local, trigger_index_and_wait_outcome,
};
use fluree_db_api::{CommitOpts, FlureeBuilder, IndexConfig, TxnOpts};
use serde_json::json;

fn ctx() -> serde_json::Value {
    json!({"ex": "http://example.org/ns/"})
}

/// Novelty never indexed: `reindex_min_bytes` is unreachable so the
/// background worker only runs on explicit triggers.
fn index_cfg() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 1 << 40,
        reindex_max_bytes: 1 << 41,
    }
}

async fn count(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    pred: &str,
) -> usize {
    let q = json!({
        "@context": ctx(),
        "select": ["?s", "?o"],
        "where": {"@id": "?s", pred: "?o"}
    });
    query_jsonld_formatted(fluree, ledger, &q)
        .await
        .expect("count query")
        .as_array()
        .map(Vec::len)
        .unwrap_or(0)
}

async fn list_values(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    subject: &str,
) -> Vec<String> {
    let q = json!({
        "@context": ctx(),
        "select": {"?s": ["ex:items"]},
        "where": {"@id": "?s"},
        "values": ["?s", [{"@id": subject}]]
    });
    let rows = query_jsonld_formatted(fluree, ledger, &q)
        .await
        .expect("list query");
    rows.as_array()
        .and_then(|a| a.first())
        .and_then(|node| node.get("ex:items"))
        .map(|v| match v {
            serde_json::Value::Array(items) => items
                .iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect(),
            serde_json::Value::String(s) => vec![s.clone()],
            _ => vec![],
        })
        .unwrap_or_default()
}

/// No `@list` anywhere: the indexed root records `Some(false)`, novelty
/// stays `false`, and a predicate-wide filtered delete spanning base rows
/// and novelty-only rows still retracts everything (the skip path must not
/// drop retractions).
#[tokio::test]
async fn no_list_ledger_records_flag_and_deletes_cleanly() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/list-meta-none:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();
            assert_eq!(ledger.snapshot.has_list_meta, Some(false), "empty snapshot is exact");

            let batch = |from: usize, to: usize| {
                let graph: Vec<_> = (from..to)
                    .map(|n| json!({"@id": format!("ex:r{n}"), "ex:created": format!("2024-01-{:02}", n % 28 + 1), "ex:name": format!("r{n}")}))
                    .collect();
                json!({"@context": ctx(), "@graph": graph})
            };
            let r = fluree
                .upsert_with_opts(ledger, &batch(0, 300), TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            trigger_index_and_wait_outcome(&handle, ledger_id, r.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert!(ledger.snapshot.range_provider.is_some());
            assert_eq!(ledger.snapshot.has_list_meta, Some(false), "full build observed no list rows");
            assert!(!ledger.novelty.has_list_meta);

            // Novelty-only subjects on top of the indexed base.
            let _ = fluree
                .upsert_with_opts(ledger, &batch(300, 600), TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert!(!ledger.novelty.has_list_meta);
            assert_eq!(count(&fluree, &ledger, "ex:created").await, 600);

            let del = json!({
                "@context": ctx(),
                "where": [{"@id": "?s", "ex:created": "?d"}],
                "delete": [{"@id": "?s", "ex:created": "?d"}]
            });
            let r = fluree
                .update_with_opts(ledger, &del, TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            assert_eq!(r.receipt.flake_count, 600);
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(count(&fluree, &ledger, "ex:created").await, 0);
            assert_eq!(count(&fluree, &ledger, "ex:name").await, 600, "unrelated predicate untouched");
        })
        .await;
}

/// `@list` rows in the base AND in novelty: the root records `Some(true)`
/// after indexing, novelty flips its bit on the first list commit, and a
/// filtered delete of one value removes exactly one positional entry per
/// WHERE binding — for a base-indexed list, a novelty-only list, and a
/// base list whose entry was re-asserted in novelty.
#[tokio::test]
async fn list_ledger_hydrates_positions_across_base_and_novelty() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/list-meta-some:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

            // Base: two lists, plus filler subjects so the delete's WHERE
            // spans many (s,p) groups.
            let mut graph = vec![
                json!({"@id": "ex:base1", "ex:items": {"@list": ["a", "b", "c"]}}),
                json!({"@id": "ex:base2", "ex:items": {"@list": ["b", "b", "d"]}}),
            ];
            graph.extend((0..200).map(|n| json!({"@id": format!("ex:f{n}"), "ex:items": {"@list": ["b"]}})));
            let r = fluree
                .insert_with_opts(ledger, &json!({"@context": ctx(), "@graph": graph}), TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert!(ledger.novelty.has_list_meta, "novelty saw a list position");
            trigger_index_and_wait_outcome(&handle, ledger_id, r.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(ledger.snapshot.has_list_meta, Some(true), "full build observed list rows");

            // Novelty: a new list subject, and a second commit that retracts
            // `ex:base2`'s middle entry so the merged view must drop it.
            let _ = fluree
                .insert_with_opts(
                    ledger,
                    &json!({"@context": ctx(), "@id": "ex:nov1", "ex:items": {"@list": ["b", "e"]}}),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg(),
                )
                .await
                .unwrap();
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(list_values(&fluree, &ledger, "ex:base1").await, vec!["a", "b", "c"]);
            assert_eq!(list_values(&fluree, &ledger, "ex:nov1").await, vec!["b", "e"]);

            // Delete every "b" everywhere. Each (subject, "b") binding must
            // remove exactly one positional entry: base1 loses its b, base2
            // keeps one of its two b's (one binding → one entry), nov1 loses
            // its b, every filler empties.
            let del = json!({
                "@context": ctx(),
                "where": [{"@id": "?s", "ex:items": "b"}],
                "delete": [{"@id": "?s", "ex:items": "b"}]
            });
            let r = fluree
                .update_with_opts(ledger, &del, TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            assert_eq!(r.receipt.flake_count, 203, "base1 + base2 + nov1 + 200 fillers");
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(list_values(&fluree, &ledger, "ex:base1").await, vec!["a", "c"]);
            assert_eq!(list_values(&fluree, &ledger, "ex:base2").await, vec!["b", "d"]);
            assert_eq!(list_values(&fluree, &ledger, "ex:nov1").await, vec!["e"]);
            assert_eq!(list_values(&fluree, &ledger, "ex:f7").await, Vec::<String>::new());
        })
        .await;
}
