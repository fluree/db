//! The cyclic-BGP fast path stays ON when an edge carries string-dictionary
//! datatypes without a reserved `DatatypeDictId` (#1729 follow-through).
//!
//! After #1729's fix, `late_materialized_object_binding` declines
//! `xsd:anyURI` / `xsd:token` / customer datatypes, and the cyclic operator
//! used to propagate that decline as a whole-edge fast-path bail
//! (`unsupported-object-binding`) — one such value anywhere in a scanned
//! predicate switched the fast path off for the query. The join now keys
//! encoded literals on `(o_type, o_key)` — the full term identity of a
//! string-dictionary value within one store view — and decodes only the rows
//! that survive the join at emit. This test pins:
//!
//! 1. the fast path *engages* (positive `cyclic_enumerate` marker, no bail
//!    event) on a triangle whose literal edge mixes `xsd:string`,
//!    `xsd:anyURI` and a customer datatype;
//! 2. the join splits same-lexical-form/different-datatype pairs and unifies
//!    same-datatype pairs, byte-identical to the fallback operator tree;
//! 3. novelty layered over the index still matches the fallback (the emit
//!    decode must route novelty string ids through `DictNovelty`).
//!
//! Env mutation lives in ONE test fn (and this file is its own test binary)
//! so parallel test threads can't race on process-global state.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, IndexConfig, LedgerManagerConfig, QueryInput};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::{
    genesis_ledger_for_fluree, normalize_rows, span_capture, start_background_indexer_local,
    trigger_index_and_wait_outcome,
};

const QUERY: &str = r"PREFIX ex: <http://example.org/ns/>
    SELECT ?a ?b ?c
    WHERE { ?a ex:p1 ?b . ?b ex:code ?c . ?a ex:ref ?c }
    ORDER BY ?a ?b ?c";

#[tokio::test]
async fn cyclic_bgp_fast_path_stays_on_with_string_dict_datatypes() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/cyclic-bgp-string-dict:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .as_arc_indexing_nameservice()
            .expect("test fluree has writable nameservice"),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };
            let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);

            // Shortcut triangle with a literal-valued join var ?c
            // (object-only → EncodedObject mode). The `ex:code`/`ex:ref`
            // columns mix the whole string-dictionary lane:
            //   a1: "abc"          = "abc"          (xsd:string, joins)
            //   a2: "u1"^^anyURI   = "u1"^^anyURI   (non-reserved, joins)
            //   a3: "xyz"^^anyURI vs "xyz"          (same lexical form,
            //       different datatype — must NOT join)
            //   a4: "k1"^^ex:kind  = "k1"^^ex:kind  (customer datatype, joins)
            //   a5: "k2"^^ex:kind vs "k2"           (must NOT join)
            let data = json!({
                "@context": {"ex": "http://example.org/ns/",
                             "xsd": "http://www.w3.org/2001/XMLSchema#"},
                "@graph": [
                    {"@id": "ex:a1", "ex:p1": {"@id": "ex:b1"},
                     "ex:ref": "abc"},
                    {"@id": "ex:b1", "ex:code": "abc"},
                    {"@id": "ex:a2", "ex:p1": {"@id": "ex:b2"},
                     "ex:ref": {"@value": "u1", "@type": "xsd:anyURI"}},
                    {"@id": "ex:b2", "ex:code": {"@value": "u1", "@type": "xsd:anyURI"}},
                    {"@id": "ex:a3", "ex:p1": {"@id": "ex:b3"},
                     "ex:ref": "xyz"},
                    {"@id": "ex:b3", "ex:code": {"@value": "xyz", "@type": "xsd:anyURI"}},
                    {"@id": "ex:a4", "ex:p1": {"@id": "ex:b4"},
                     "ex:ref": {"@value": "k1", "@type": "ex:kind"}},
                    {"@id": "ex:b4", "ex:code": {"@value": "k1", "@type": "ex:kind"}},
                    {"@id": "ex:a5", "ex:p1": {"@id": "ex:b5"},
                     "ex:ref": "k2"},
                    {"@id": "ex:b5", "ex:code": {"@value": "k2", "@type": "ex:kind"}}
                ]
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &data,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert");
            let ledger = result.ledger;
            trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;

            let view = fluree.db(ledger_id).await.expect("indexed view");

            // Ground truth: fallback operator tree.
            std::env::set_var("FLUREE_CYCLIC_BGP", "0");
            let expected = run_query(&fluree, &view, QUERY).await;
            std::env::remove_var("FLUREE_CYCLIC_BGP");
            assert_eq!(
                expected.len(),
                3,
                "fallback should join exactly a1/a2/a4; rows: {expected:?}"
            );
            let flat = format!("{expected:?}");
            for present in ["ex:a1", "ex:a2", "ex:a4"] {
                assert!(flat.contains(present), "{present} missing: {expected:?}");
            }
            for absent in ["ex:a3", "ex:a5"] {
                assert!(
                    !flat.contains(absent),
                    "{absent} joined across datatypes: {expected:?}"
                );
            }

            // Cyclic operator (default): the fast path must ENGAGE — not
            // decline on the first non-reserved string-dict row — and match
            // the fallback exactly.
            let (spans, guard) = span_capture::init_test_tracing();
            let actual = run_query(&fluree, &view, QUERY).await;
            drop(guard);
            assert_eq!(actual, expected, "cyclic fast path != fallback");
            assert!(
                spans.has_span("cyclic_enumerate"),
                "cyclic fast path never enumerated — it declined instead"
            );
            assert!(
                !spans.has_event("cyclic bgp fast path bail"),
                "cyclic fast path bailed on the indexed HEAD view: {:?}",
                spans.find_events("cyclic bgp fast path bail")
            );

            // Novelty tail: a new triangle whose anyURI lexical form exists
            // only in novelty. The overlay-merging edge cursors surface a
            // novelty string id, and the emit-side decode must resolve it
            // through DictNovelty.
            let _ = fluree
                .insert(
                    ledger,
                    &json!({
                        "@context": {"ex": "http://example.org/ns/",
                                     "xsd": "http://www.w3.org/2001/XMLSchema#"},
                        "@graph": [
                            {"@id": "ex:a6", "ex:p1": {"@id": "ex:b6"},
                             "ex:ref": {"@value": "novel-u", "@type": "xsd:anyURI"}},
                            {"@id": "ex:b6", "ex:code": {"@value": "novel-u", "@type": "xsd:anyURI"}}
                        ]
                    }),
                )
                .await
                .expect("novelty insert");
            let view = fluree.db(ledger_id).await.expect("novelty view");

            std::env::set_var("FLUREE_CYCLIC_BGP", "0");
            let expected = run_query(&fluree, &view, QUERY).await;
            std::env::remove_var("FLUREE_CYCLIC_BGP");
            assert_eq!(expected.len(), 4, "novelty triangle joins: {expected:?}");

            let (spans, guard) = span_capture::init_test_tracing();
            let actual = run_query(&fluree, &view, QUERY).await;
            drop(guard);
            assert_eq!(actual, expected, "cyclic under novelty != fallback");
            assert!(
                format!("{actual:?}").contains("ex:a6"),
                "novelty-only triangle missing: {actual:?}"
            );
            // The one decline this PR removes must not come back under
            // novelty either (other bail reasons — e.g. probe gating — are
            // legitimate off-HEAD and not asserted here).
            assert!(
                !spans
                    .find_events("cyclic bgp fast path bail")
                    .iter()
                    .any(|e| e.fields.get("reason").map(String::as_str)
                        == Some("unsupported-object-binding")),
                "string-dict rows bailed the fast path again: {:?}",
                spans.find_events("cyclic bgp fast path bail")
            );
        })
        .await;
}

async fn run_query(
    fluree: &fluree_db_api::Fluree,
    view: &fluree_db_api::GraphDb,
    query: &str,
) -> Vec<serde_json::Value> {
    let result = fluree
        .query(view, QueryInput::Sparql(query))
        .await
        .expect("query");
    let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
    normalize_rows(&jsonld)
}
