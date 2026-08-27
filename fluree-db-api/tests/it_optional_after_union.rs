//! `OPTIONAL` over a variable a preceding `UNION` left unbound (SPARQL 1.1
//! §18.2.4 `LeftJoin`).
//!
//! ```sparql
//! SELECT ?s ?f WHERE {
//!   ?s schema:name ?name .
//!   { { ?s ex:friend ?f } UNION { ?s schema:age ?age } }
//!   OPTIONAL { ?s ex:friend ?f }
//! }
//! ```
//!
//! The UNION emits ten solutions: six that bind `?f` (branch one) and four that
//! leave it unbound (branch two). A solution with `?f` unbound IS compatible
//! with one that binds it, so the left join must EXTEND those four rows rather
//! than pass them through — thirteen rows, not ten.
//!
//! The property that cannot be satisfied by accident is the second assertion in
//! each test: deleting the `OPTIONAL` clause outright must change the answer.
//! Before the fix the two queries returned byte-identical rows, which is the
//! sharpest possible statement of a left join contributing nothing.
//!
//! Both lanes are covered on purpose. The novelty lane and the indexed lane
//! reach the OPTIONAL through different code (per-row substituted scan +
//! result cache vs. the batched subject probe), and they failed differently.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{
    FlureeBuilder, IndexConfig, LedgerManagerConfig, LedgerState, QueryInput, QueryResult,
};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::{json, Value};
use support::{
    genesis_ledger_for_fluree, graphdb_from_ledger, normalize_rows, start_background_indexer_local,
    trigger_index_and_wait_outcome, wait_for_index_application,
};

/// `seed_values_dataset` from `it_query_values.rs`, verbatim — the fixture the
/// issue's row counts were measured on.
fn seed_data() -> Value {
    json!({
        "@context": {
            "schema": "http://schema.org/",
            "ex": "http://example.com/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {"@id":"ex:brian","schema:name":"Brian","schema:email":"brian@example.org","schema:age":50,"ex:favNums":7},
            {"@id":"ex:alice","schema:name":"Alice","schema:email":"alice@example.org","schema:age":50,"ex:favNums":[42,76,9],"ex:friend":[{"@id":"ex:brian"}]},
            {"@id":"ex:cam","schema:name":"Cam","schema:email":"cam@example.org","schema:age":34,"ex:favNums":[5,10],"ex:friend":[{"@id":"ex:alice"},{"@id":"ex:brian"}]},
            {"@id":"ex:liam","schema:name":"Liam","schema:email":"liam@example.org","schema:age":13,"ex:favNums":[42,11],"ex:friend":[{"@id":"ex:alice"},{"@id":"ex:brian"},{"@id":"ex:cam"}]},
            {"@id":"ex:nikola",
             "schema:name":"Nikola",
             "ex:greeting":[{"@value":"Здраво","@language":"sb"},{"@value":"Hello","@language":"en"}],
             "ex:birthday":{"@value":"2000-01-01","@type":"xsd:datetime"},
             "ex:cool":true}
        ]
    })
}

const PREFIXES: &str = "PREFIX ex: <http://example.com/>\n\
     PREFIX schema: <http://schema.org/>\n";

const UNION_THEN_OPTIONAL: &str = "SELECT ?s ?f WHERE { \
     ?s schema:name ?name . \
     { { ?s ex:friend ?f } UNION { ?s schema:age ?age } } \
     OPTIONAL { ?s ex:friend ?f } }";

/// The same query with the `OPTIONAL` clause deleted.
const UNION_ONLY: &str = "SELECT ?s ?f WHERE { \
     ?s schema:name ?name . \
     { { ?s ex:friend ?f } UNION { ?s schema:age ?age } } }";

fn query_context() -> Value {
    json!({
        "schema": "http://schema.org/",
        "ex": "http://example.com/"
    })
}

fn jsonld_union_then_optional() -> Value {
    json!({
        "@context": query_context(),
        "select": ["?s", "?f"],
        "where": [
            {"@id": "?s", "schema:name": "?name"},
            ["union",
                {"@id": "?s", "ex:friend": "?f"},
                {"@id": "?s", "schema:age": "?age"}
            ],
            ["optional", {"@id": "?s", "ex:friend": "?f"}]
        ]
    })
}

fn jsonld_union_only() -> Value {
    json!({
        "@context": query_context(),
        "select": ["?s", "?f"],
        "where": [
            {"@id": "?s", "schema:name": "?name"},
            ["union",
                {"@id": "?s", "ex:friend": "?f"},
                {"@id": "?s", "schema:age": "?age"}
            ]
        ]
    })
}

/// The ten UNION solutions: six bind `?f`, four leave it unbound. `ex:nikola`
/// has a name but neither `ex:friend` nor `schema:age`, so neither branch
/// admits it.
fn expected_union_only() -> Vec<Value> {
    normalize_rows(&json!([
        ["ex:alice", "ex:brian"],
        ["ex:cam", "ex:alice"],
        ["ex:cam", "ex:brian"],
        ["ex:liam", "ex:alice"],
        ["ex:liam", "ex:brian"],
        ["ex:liam", "ex:cam"],
        ["ex:brian", null],
        ["ex:alice", null],
        ["ex:cam", null],
        ["ex:liam", null]
    ]))
}

/// §18.2.4 over those ten. The six `?f`-bound rows join to their own friend
/// triple and come back unchanged. Of the four unbound rows, `ex:brian` has no
/// friends and passes through unbound, while `ex:alice`/`ex:cam`/`ex:liam` are
/// extended by their 1/2/3 friends — 6 + 1 + 1 + 2 + 3 = 13.
fn expected_union_then_optional() -> Vec<Value> {
    normalize_rows(&json!([
        ["ex:alice", "ex:brian"],
        ["ex:cam", "ex:alice"],
        ["ex:cam", "ex:brian"],
        ["ex:liam", "ex:alice"],
        ["ex:liam", "ex:brian"],
        ["ex:liam", "ex:cam"],
        ["ex:brian", null],
        ["ex:alice", "ex:brian"],
        ["ex:cam", "ex:alice"],
        ["ex:cam", "ex:brian"],
        ["ex:liam", "ex:alice"],
        ["ex:liam", "ex:brian"],
        ["ex:liam", "ex:cam"]
    ]))
}

fn rows(result: &QueryResult, snapshot: &fluree_db_core::LedgerSnapshot) -> Vec<Value> {
    normalize_rows(&result.to_jsonld(snapshot).expect("to_jsonld"))
}

/// Assert both the exact row multiset and the delete-the-clause property.
fn assert_left_join_contributes(lane: &str, with_optional: &[Value], without_optional: &[Value]) {
    assert_eq!(
        without_optional,
        expected_union_only(),
        "{lane}: the UNION alone must still be the ten solutions the left join starts from"
    );
    assert_ne!(
        with_optional, without_optional,
        "{lane}: deleting the OPTIONAL must change the answer — identical rows mean the left \
         join contributed nothing"
    );
    assert_eq!(
        with_optional,
        expected_union_then_optional(),
        "{lane}: OPTIONAL must extend the four rows the UNION left ?f unbound on"
    );
}

async fn seed_novelty(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> LedgerState {
    let ledger = genesis_ledger_for_fluree(fluree, ledger_id);
    fluree
        .insert(ledger, &seed_data())
        .await
        .expect("seed insert")
        .ledger
}

#[tokio::test]
async fn sparql_optional_extends_union_unbound_rows() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "optional-after-union:sparql").await;
    let db = graphdb_from_ledger(&ledger);

    let with_optional = fluree
        .query(
            &db,
            QueryInput::Sparql(&format!("{PREFIXES}{UNION_THEN_OPTIONAL}")),
        )
        .await
        .expect("query with OPTIONAL");
    let without_optional = fluree
        .query(&db, QueryInput::Sparql(&format!("{PREFIXES}{UNION_ONLY}")))
        .await
        .expect("query without OPTIONAL");

    assert_left_join_contributes(
        "sparql/novelty",
        &rows(&with_optional, &ledger.snapshot),
        &rows(&without_optional, &ledger.snapshot),
    );
}

#[tokio::test]
async fn jsonld_optional_extends_union_unbound_rows() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "optional-after-union:jsonld").await;

    let with_optional = support::query_jsonld(&fluree, &ledger, &jsonld_union_then_optional())
        .await
        .expect("query with OPTIONAL");
    let without_optional = support::query_jsonld(&fluree, &ledger, &jsonld_union_only())
        .await
        .expect("query without OPTIONAL");

    assert_left_join_contributes(
        "json-ld/novelty",
        &rows(&with_optional, &ledger.snapshot),
        &rows(&without_optional, &ledger.snapshot),
    );
}

/// The merge is not specific to the object position. Here the UNION's second
/// branch binds neither `?s` nor `?f`, so the single solution it contributes is
/// compatible with every `ex:friend` triple and the left join extends it into
/// all six — the six `?f`-bound rows plus six more, twelve in all.
#[tokio::test]
async fn optional_extends_a_row_with_no_correlation_bound_at_all() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "optional-after-union:free").await;
    let db = graphdb_from_ledger(&ledger);

    let query = "SELECT ?s ?f WHERE { \
         { { ?s ex:friend ?f } UNION { ex:nikola schema:name ?nn } } \
         OPTIONAL { ?s ex:friend ?f } }";

    let result = fluree
        .query(&db, QueryInput::Sparql(&format!("{PREFIXES}{query}")))
        .await
        .expect("query");

    assert_eq!(
        rows(&result, &ledger.snapshot),
        normalize_rows(&json!([
            ["ex:alice", "ex:brian"],
            ["ex:cam", "ex:alice"],
            ["ex:cam", "ex:brian"],
            ["ex:liam", "ex:alice"],
            ["ex:liam", "ex:brian"],
            ["ex:liam", "ex:cam"],
            ["ex:alice", "ex:brian"],
            ["ex:cam", "ex:alice"],
            ["ex:cam", "ex:brian"],
            ["ex:liam", "ex:alice"],
            ["ex:liam", "ex:brian"],
            ["ex:liam", "ex:cam"]
        ]))
    );
}

/// A two-pattern OPTIONAL routes to `PlanTreeOptionalBuilder` instead of the
/// single-triple builder, and its batched lane partitions inner results by a
/// correlation key an unbound variable has no value for. Every friend here has
/// a `schema:name`, so the second pattern removes no solutions and the answer
/// is the same thirteen rows.
#[tokio::test]
async fn multi_pattern_optional_extends_union_unbound_rows() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "optional-after-union:multi").await;
    let db = graphdb_from_ledger(&ledger);

    let multi_pattern = "SELECT ?s ?f WHERE { \
         ?s schema:name ?name . \
         { { ?s ex:friend ?f } UNION { ?s schema:age ?age } } \
         OPTIONAL { ?s ex:friend ?f . ?f schema:name ?fname } }";

    let with_optional = fluree
        .query(
            &db,
            QueryInput::Sparql(&format!("{PREFIXES}{multi_pattern}")),
        )
        .await
        .expect("query with multi-pattern OPTIONAL");
    let without_optional = fluree
        .query(&db, QueryInput::Sparql(&format!("{PREFIXES}{UNION_ONLY}")))
        .await
        .expect("query without OPTIONAL");

    assert_left_join_contributes(
        "sparql/multi-pattern",
        &rows(&with_optional, &ledger.snapshot),
        &rows(&without_optional, &ledger.snapshot),
    );
}

/// The indexed lane reaches the OPTIONAL through `build_batch`'s subject probe
/// rather than the per-row substituted scan, so it needs its own coverage —
/// a novelty-only pin would have gone green on a half fix.
#[tokio::test]
async fn indexed_optional_extends_union_unbound_rows() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "optional-after-union:indexed";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };
            let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
            let ledger = fluree
                .insert_with_opts(
                    ledger,
                    &seed_data(),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("seed insert")
                .ledger;

            trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            wait_for_index_application(&fluree, ledger_id, ledger.t()).await;
            let view = fluree.db(ledger_id).await.expect("indexed view");
            // Lane guard. Post-fix this shape declines the batched probe, so
            // without this a lost indexer race would silently downgrade the
            // whole test to a second novelty-lane assertion — which passes on
            // the unfixed engine too.
            assert!(
                view.binary_store().is_some(),
                "the indexed lane needs a binary store on the view; without one this test is a \
                 duplicate of the novelty-lane one"
            );

            let with_optional = fluree
                .query(
                    &view,
                    QueryInput::Sparql(&format!("{PREFIXES}{UNION_THEN_OPTIONAL}")),
                )
                .await
                .expect("query with OPTIONAL");
            let without_optional = fluree
                .query(
                    &view,
                    QueryInput::Sparql(&format!("{PREFIXES}{UNION_ONLY}")),
                )
                .await
                .expect("query without OPTIONAL");

            assert_left_join_contributes(
                "sparql/indexed",
                &rows(&with_optional, &view.snapshot),
                &rows(&without_optional, &view.snapshot),
            );
        })
        .await;
}
