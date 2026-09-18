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
    CommitOpts, Fluree, FlureeBuilder, FormatterConfig, GraphDb, IndexConfig, QueryInput,
    ReindexOptions, TxnOpts,
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

/// Encoding at lowering means a JSON-LD pattern SID is the *primary* graph's
/// SID and reaches every other graph of a dataset through `reencode_sid`,
/// as SPARQL's always has. Pin that path with two ledgers that assign the
/// join predicates' namespace different codes, and assert the JSON-LD and
/// SPARQL spellings return the same rows.
#[tokio::test(flavor = "current_thread")]
async fn jsonld_cross_ledger_join_matches_sparql() {
    let fluree = FlureeBuilder::memory().build_memory();
    // Catalog registers its own prefix first and `people.example` only via a
    // ref; people registers `people.example` first. The shared namespace
    // therefore gets a different code in each ledger.
    fluree
        .insert(
            genesis_ledger(&fluree, "xl-catalog:main"),
            &json!({
                "@context": {"cat": "http://catalog.example/", "p": "http://people.example/"},
                "@graph": [
                    {"@id": "cat:item1", "cat:sku": "X-1", "cat:curator": {"@id": "p:ada"}},
                    {"@id": "cat:item2", "cat:sku": "X-2"}
                ]
            }),
        )
        .await
        .expect("insert catalog");
    fluree
        .insert(
            genesis_ledger(&fluree, "xl-people:main"),
            &json!({
                "@context": {"p": "http://people.example/", "cat": "http://catalog.example/"},
                "@graph": [
                    {"@id": "p:ada", "p:fullName": "Ada", "p:buys": [{"@id": "cat:item1"}, {"@id": "cat:item2"}]},
                    {"@id": "p:bob", "p:fullName": "Bob", "p:buys": {"@id": "cat:item2"}}
                ]
            }),
        )
        .await
        .expect("insert people");

    let catalog = fluree.db("xl-catalog:main").await.expect("catalog");
    let people = fluree.db("xl-people:main").await.expect("people");
    let code = |db: &GraphDb| {
        db.snapshot
            .encode_iri_strict("http://people.example/buys")
            .expect("registered")
            .namespace_code
    };
    assert_ne!(
        code(&catalog),
        code(&people),
        "the fixture must give the join predicate divergent namespace codes"
    );

    let jsonld = json!({
        "@context": {"cat": "http://catalog.example/", "p": "http://people.example/"},
        "from": ["xl-catalog:main", "xl-people:main"],
        "select": ["?name", "?sku"],
        "where": [
            {"@id": "?person", "p:fullName": "?name"},
            {"@id": "?person", "p:buys": "?item"},
            {"@id": "?item", "cat:sku": "?sku"}
        ]
    });
    let sparql = r"
PREFIX cat: <http://catalog.example/>
PREFIX p: <http://people.example/>
SELECT ?name ?sku FROM <xl-catalog:main> FROM <xl-people:main>
WHERE { ?person p:fullName ?name . ?person p:buys ?item . ?item cat:sku ?sku }";

    let mut via_jsonld = fluree
        .query_from()
        .jsonld(&jsonld)
        .execute_formatted()
        .await
        .expect("jsonld");
    let mut via_sparql = fluree
        .query_from()
        .sparql(sparql)
        .format(FormatterConfig::jsonld())
        .execute_formatted()
        .await
        .expect("sparql");
    for rows in [&mut via_jsonld, &mut via_sparql] {
        rows.as_array_mut()
            .expect("rows")
            .sort_by_key(ToString::to_string);
    }
    assert_eq!(
        via_jsonld,
        json!([["Ada", "X-1"], ["Ada", "X-2"], ["Bob", "X-2"]]),
        "jsonld rows"
    );
    assert_eq!(via_jsonld, via_sparql, "jsonld and sparql must agree");
}
