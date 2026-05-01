//! Regression tests for novelty-only dictionary entries with an existing index.
//!
//! These scenarios exercise decoding paths that can fail if the query execution
//! context does not carry the ledger's `DictNovelty` when using the binary scan
//! path (BinaryIndexStore + overlay).

#![cfg(feature = "native")]

use std::sync::Arc;
mod support;

use fluree_db_api::{FlureeBuilder, IndexConfig};
use fluree_db_core::FlakeValue;
use fluree_db_query::{
    execute_where, ExecutionContext, Pattern, Ref, Term, TriplePattern, VarRegistry,
};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::{genesis_ledger_for_fluree, query_sparql, start_background_indexer_local};

#[tokio::test]
async fn novelty_only_strings_subjects_predicates_and_json_decode_with_existing_index() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let mut fluree = FlureeBuilder::file(path)
        .build()
        .expect("build file fluree");

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/novelty-dict-decode:main";

            // 1) Seed + build an index so the query engine takes the binary scan path.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let seed = json!({
                "@context": { "ex": "http://example.org/" },
                "@graph": [
                    // Seed a different predicate so `ex:label` only exists in novelty.
                    {"@id":"ex:s","ex:seed":"seed"}
                ]
            });
            let seeded = fluree
                .insert_with_opts(
                    ledger0,
                    &seed,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("seed insert");

            let commit_t = seeded.receipt.t;
            let _ = support::trigger_index_and_wait_outcome(
                &handle,
                seeded.ledger.ledger_id(),
                commit_t,
            )
            .await;

            // Reload a ledger state with the binary store attached.
            let indexed = fluree.ledger(ledger_id).await.expect("load indexed ledger");
            assert!(
                indexed.snapshot.range_provider.is_some(),
                "expected range_provider after indexing"
            );

            // 2) Commit novelty-only data introducing:
            // - a new string value (not in persisted dict)
            // - a new subject IRI
            // - a new predicate IRI
            // - a new @json literal
            //
            // Keep it novelty-only (don't trigger indexing).
            let no_index_cfg = IndexConfig {
                reindex_min_bytes: 10_000_000_000,
                reindex_max_bytes: 20_000_000_000,
            };
            let novel_str = "NovelStringValue-xyz";
            let novel_pred_val = "NovelPredicateValue-abc";

            let novelty_txn = json!({
                "@context": { "ex": "http://example.org/" },
                "@graph": [
                    {"@id":"ex:s","ex:label": novel_str},
                    {"@id":"ex:newSubject","ex:label":"Foo"},
                    {"@id":"ex:s","ex:newPred": novel_pred_val},
                    {"@id":"ex:s","ex:data": {"@value": {"k": 1, "n": 42}, "@type": "@json"}}
                ]
            });

            let after = fluree
                .insert_with_opts(
                    indexed,
                    &novelty_txn,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &no_index_cfg,
                )
                .await
                .expect("novelty insert")
                .ledger;

            // 3) Query the novelty-only values immediately.

            // (a) Return the novelty-only string value.
            let q1 = r"
PREFIX ex: <http://example.org/>
SELECT ?o WHERE { ex:s ex:label ?o }
";
            let r1 = query_sparql(&fluree, &after, q1).await.expect("q1");
            let j1 = r1.to_sparql_json(&after.snapshot).expect("sparql json");
            let v1 = j1["results"]["bindings"][0]["o"]["value"]
                .as_str()
                .expect("o value");
            assert_eq!(v1, novel_str);

            // (a2) Same query, but execute via fluree-db-query directly using GraphDbRef.
            //
            // This is the regression path: GraphDbRef does not carry DictNovelty, so the
            // ExecutionContext must extract DictNovelty from the snapshot's BinaryRangeProvider.
            // Without that, overlay translation for novelty-only string IDs can fail, leading to
            // missing rows or "string id not found in forward packs" during decode.
            let db_ref = after.as_graph_db_ref(0);
            assert!(
                db_ref.snapshot.range_provider.is_some(),
                "expected snapshot.range_provider to be set"
            );
            let vars_smoke = VarRegistry::new();
            let ctx_smoke = ExecutionContext::from_graph_db_ref(db_ref, &vars_smoke);
            assert!(
                ctx_smoke.binary_store.is_some(),
                "expected ExecutionContext to extract binary_store from range_provider"
            );

            let mut vars = VarRegistry::new();
            let o_var = vars.get_or_insert("?o");
            let patterns = vec![Pattern::Triple(TriplePattern::new(
                Ref::Sid(fluree_db_core::Sid::new(0, "http://example.org/s")),
                Ref::Sid(fluree_db_core::Sid::new(0, "http://example.org/label")),
                Term::Var(o_var),
            ))];
            let batches = execute_where(after.as_graph_db_ref(0), &vars, &patterns, None)
                .await
                .expect("execute_where");
            assert!(!batches.is_empty(), "expected at least one batch");
            let batch = &batches[0];
            let o_col = batch.column_by_idx(0).expect("o column");
            let first = o_col[0].clone();
            match first {
                fluree_db_query::binding::Binding::Lit { val, .. } => {
                    assert_eq!(val, FlakeValue::String(novel_str.to_string()));
                }
                other => panic!("expected materialized literal binding, got: {other:?}"),
            }

            // (b) Use the novelty-only string as a bound constant (requires string-id resolution).
            let q2 = format!(
                r#"
PREFIX ex: <http://example.org/>
SELECT (COUNT(*) AS ?c) WHERE {{ ?s ex:label "{novel_str}" }}
"#
            );
            let r2 = query_sparql(&fluree, &after, &q2).await.expect("q2");
            let j2 = r2.to_sparql_json(&after.snapshot).expect("sparql json");
            let c2 = j2["results"]["bindings"][0]["c"]["value"]
                .as_str()
                .expect("count string");
            assert_eq!(c2, "1");

            // (c) Return a novelty-only subject IRI by matching a novelty-only string constant.
            let q3 = r#"
PREFIX ex: <http://example.org/>
SELECT ?s WHERE { ?s ex:label "Foo" }
"#;
            let r3 = query_sparql(&fluree, &after, q3).await.expect("q3");
            let j3 = r3.to_sparql_json(&after.snapshot).expect("sparql json");
            let s3 = j3["results"]["bindings"][0]["s"]["value"]
                .as_str()
                .expect("s value");
            assert!(
                s3 == "http://example.org/newSubject" || s3 == "ex:newSubject",
                "expected ex:newSubject, got: {s3}"
            );

            // (d) Query a novelty-only predicate IRI (predicate not in persisted dict).
            let q4 = r"
PREFIX ex: <http://example.org/>
SELECT ?o WHERE { ex:s ex:newPred ?o }
";
            let r4 = query_sparql(&fluree, &after, q4).await.expect("q4");
            let j4 = r4.to_sparql_json(&after.snapshot).expect("sparql json");
            let v4 = j4["results"]["bindings"][0]["o"]["value"]
                .as_str()
                .expect("o value");
            assert_eq!(v4, novel_pred_val);

            // (e) Return the novelty-only @json typed literal (sanity: no panics, datatype preserved).
            // We only assert the datatype string contains "json" to allow either shorthand (@json)
            // or full IRI forms.
            let q5 = r"
PREFIX ex: <http://example.org/>
SELECT ?o (DATATYPE(?o) AS ?dt) WHERE { ex:s ex:data ?o }
";
            let r5 = query_sparql(&fluree, &after, q5).await.expect("q5");
            let j5 = r5.to_sparql_json(&after.snapshot).expect("sparql json");
            let dt5 = j5["results"]["bindings"][0]["dt"]["value"]
                .as_str()
                .expect("dt value")
                .to_lowercase();
            assert!(dt5.contains("json"), "expected @json datatype, got: {dt5}");
        })
        .await;
}
