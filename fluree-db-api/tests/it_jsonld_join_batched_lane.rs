//! JSON-LD joins must reach the nested-loop join's batched probe lanes.
//!
//! The lanes require the right pattern's predicate as a `Ref::Sid`. JSON-LD
//! lowering used to leave every constant as `Ref::Iri` for "deferred
//! encoding", and nothing encoded it outside the reasoning path, so a JSON-LD
//! join never took the batched lane while the identical SPARQL join did.
//! Pinned by the join's routing event, not the answer — the per-row path
//! computes the same rows.

#![cfg(feature = "native")]

use crate::support::genesis_ledger;
use crate::support::span_capture::init_test_tracing;
use fluree_db_api::{
    CommitOpts, Fluree, FlureeBuilder, IndexConfig, QueryInput, ReindexOptions, TxnOpts,
};
use serde_json::json;

const LEDGER: &str = "jsonld/join-batched-lane:main";
const EX: &str = "http://example.org/ns/";

async fn indexed_people() -> Fluree {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, LEDGER);
    let no_background = IndexConfig {
        reindex_min_bytes: 1_000_000_000,
        reindex_max_bytes: 1_000_000_000,
    };
    fluree
        .insert_with_opts(
            ledger,
            &json!({
                "@context": {"ex": EX},
                "@graph": [
                    {"@id": "ex:alice", "ex:name": "Alice", "ex:age": 30},
                    {"@id": "ex:bob", "ex:name": "Bob", "ex:age": 25},
                    {"@id": "ex:carol", "ex:name": "Carol", "ex:age": 41}
                ]
            }),
            TxnOpts::default(),
            CommitOpts::default(),
            &no_background,
        )
        .await
        .expect("insert people");
    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("reindex");
    fluree
}

#[tokio::test(flavor = "current_thread")]
async fn jsonld_join_takes_batched_subject_probe_like_sparql() {
    let fluree = indexed_people().await;
    let view = fluree.db(LEDGER).await.expect("db");
    let (store, guard) = init_test_tracing();

    let jsonld = json!({
        "@context": {"ex": EX},
        "select": ["?n", "?a"],
        "where": [{"@id": "?s", "ex:name": "?n"}, {"@id": "?s", "ex:age": "?a"}]
    });
    let cases: [(&str, QueryInput<'_>); 2] = [
        ("jsonld", QueryInput::from(&jsonld)),
        (
            "sparql",
            QueryInput::Sparql(
                "SELECT ?n ?a WHERE { ?s <http://example.org/ns/name> ?n . ?s <http://example.org/ns/age> ?a }",
            ),
        ),
    ];
    for (label, query) in cases {
        let before = store.find_events("nested loop join runtime path").len();
        let result = fluree.query(&view, query).await.expect(label);
        let rows = result.to_jsonld(&view.snapshot).expect("jsonld");
        assert_eq!(rows.as_array().map(Vec::len), Some(3), "{label}: {rows}");
        let routed: Vec<String> = store.find_events("nested loop join runtime path")[before..]
            .iter()
            .filter_map(|e| e.fields.get("use_batched").cloned())
            .collect();
        assert_eq!(
            routed,
            vec!["true".to_string()],
            "{label}: the join must take the batched subject-probe lane"
        );
    }
    drop(guard);
}
