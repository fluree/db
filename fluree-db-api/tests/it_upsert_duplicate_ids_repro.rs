//! Regression: JSON-LD upsert with repeated `@id` must not create duplicates.
//!
//! Modeled after Slack-ish payloads where many entities reference the same member
//! and the member itself is duplicated in the top-level `@graph`.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, IndexConfig};
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::Sid;
use fluree_db_query::binding::Binding;
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use std::sync::Arc;
use support::{query_sparql, start_background_indexer_local, trigger_index_and_wait_outcome};

fn sid_to_iri(sid: &Sid, codes: &std::collections::HashMap<u16, String>) -> String {
    if sid.namespace_code == fluree_vocab::namespaces::OVERFLOW {
        return sid.name_str().to_string();
    }
    let prefix = codes
        .get(&sid.namespace_code)
        .unwrap_or_else(|| panic!("missing namespace code {}", sid.namespace_code));
    format!("{prefix}{}", sid.name_str())
}

fn make_slackish_upsert_tx(channel_count: usize, member_dups: usize) -> serde_json::Value {
    let mut graph: Vec<serde_json::Value> = Vec::with_capacity(channel_count + member_dups);

    for i in 0..channel_count {
        graph.push(json!({
            "@id": format!("msg:channel/c{i}"),
            "@type": "msg:Channel",
            "msg:members": [{"@id": "msg:member/m1"}]
        }));
    }

    let member = json!({
        "@id": "msg:member/m1",
        "@type": ["msg:Member"],
        "msg:userId": {"@id": "hr:employee/e1"},
        "msg:name": "Name"
    });
    for _ in 0..member_dups {
        graph.push(member.clone());
    }

    json!({
        "@context": {
            "msg": "https://ns.flur.ee/messaging/",
            "hr": "https://ns.flur.ee/hr/"
        },
        "@graph": graph
    })
}

#[tokio::test]
async fn repro_upsert_repeated_ids_create_duplicate_subject_ids() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    // File-backed Fluree is required: the reproduction depends on the native/binary-index path.
    let mut fluree = FlureeBuilder::file(path.clone())
        .build()
        .expect("build file fluree");

    // Start background indexer.
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    // Force indexing_needed=true.
    let index_cfg = IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 1_000_000,
    };

    local
        .run_until(async move {
            let ledger_id = "it/upsert-dup-ids-repro:main";
            let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

            let tx = make_slackish_upsert_tx(100, 100);

            // 1) Apply once and index to establish a binary-index base.
            let r1 = fluree
                .upsert_with_opts(
                    ledger0,
                    &tx,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .unwrap();
            trigger_index_and_wait_outcome(&handle, ledger_id, r1.receipt.t).await;

            // 2) Reload (indexed) and apply again; this is where the bug historically reproduced.
            let ledger_indexed = fluree.ledger(ledger_id).await.unwrap();
            assert!(
                ledger_indexed
                    .snapshot
                    .namespaces()
                    .values()
                    .any(|p| p == "https://ns.flur.ee/messaging/"),
                "expected messaging/ namespace prefix in indexed snapshot"
            );
            let r2 = fluree
                .upsert_with_opts(
                    ledger_indexed,
                    &tx,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .unwrap();

            // IMPORTANT: create a fresh Fluree instance to force a reload from disk.
            drop(fluree);
            let fluree2 = FlureeBuilder::file(path.clone())
                .build()
                .expect("rebuild fluree");
            let ledger2_loaded = fluree2.ledger(ledger_id).await.unwrap();

            // Persisted store + dict_novelty sanity.
            let store: Arc<BinaryIndexStore> = ledger2_loaded
                .binary_store
                .as_ref()
                .expect("expected binary_store on reloaded ledger")
                .0
                .clone()
                .downcast::<BinaryIndexStore>()
                .expect("downcast BinaryIndexStore");

            let member_iri = "https://ns.flur.ee/messaging/member/m1";
            let member_sid = store.encode_iri(member_iri);
            let persisted_id = store
                .find_subject_id_by_parts(member_sid.namespace_code, member_sid.name_str())
                .expect("find_subject_id_by_parts io")
                .expect("member should exist in persisted dict after indexing t=1");

            let novelty_id = ledger2_loaded
                .dict_novelty
                .subjects
                .find_subject(member_sid.namespace_code, member_sid.name_str());
            assert!(
                novelty_id.is_none(),
                "dict_novelty must not allocate IDs for persisted subjects"
            );

            // Queries: variable-bound patterns must not duplicate solutions.
            let q_var = r"
                PREFIX msg: <https://ns.flur.ee/messaging/>
                PREFIX hr: <https://ns.flur.ee/hr/>
                SELECT (COUNT(*) AS ?c)
                WHERE { ?m msg:userId hr:employee/e1 . }
            ";
            let q_var_bindings = r"
                PREFIX msg: <https://ns.flur.ee/messaging/>
                PREFIX hr: <https://ns.flur.ee/hr/>
                SELECT ?m
                WHERE { ?m msg:userId hr:employee/e1 . }
            ";
            let q_const = r"
                PREFIX msg: <https://ns.flur.ee/messaging/>
                PREFIX hr: <https://ns.flur.ee/hr/>
                SELECT (COUNT(*) AS ?c)
                WHERE { msg:member/m1 msg:userId hr:employee/e1 . }
            ";
            let q_type = r"
                PREFIX msg: <https://ns.flur.ee/messaging/>
                SELECT (COUNT(*) AS ?c)
                WHERE { ?m a msg:Member . }
            ";
            let q_join = r"
                PREFIX msg: <https://ns.flur.ee/messaging/>
                PREFIX hr: <https://ns.flur.ee/hr/>
                SELECT (COUNT(?m) AS ?count) (COUNT(DISTINCT ?m) AS ?distinctMembers)
                WHERE { ?m a msg:Member ; msg:userId hr:employee/e1 . }
            ";
            let q_graph = r"
                PREFIX msg: <https://ns.flur.ee/messaging/>
                PREFIX hr: <https://ns.flur.ee/hr/>
                SELECT ?g (COUNT(*) AS ?c)
                WHERE { GRAPH ?g { msg:member/m1 msg:userId hr:employee/e1 . } }
                GROUP BY ?g
            ";

            let var_count = query_sparql(&fluree2, &ledger2_loaded, q_var)
                .await
                .unwrap()
                .to_jsonld(&ledger2_loaded.snapshot)
                .unwrap();
            let const_count = query_sparql(&fluree2, &ledger2_loaded, q_const)
                .await
                .unwrap()
                .to_jsonld(&ledger2_loaded.snapshot)
                .unwrap();
            let type_count = query_sparql(&fluree2, &ledger2_loaded, q_type)
                .await
                .unwrap()
                .to_jsonld(&ledger2_loaded.snapshot)
                .unwrap();
            let join_counts = query_sparql(&fluree2, &ledger2_loaded, q_join)
                .await
                .unwrap()
                .to_jsonld(&ledger2_loaded.snapshot)
                .unwrap();
            let graph_counts = query_sparql(&fluree2, &ledger2_loaded, q_graph)
                .await
                .unwrap()
                .to_jsonld(&ledger2_loaded.snapshot)
                .unwrap();

            assert_eq!(const_count, json!([[1]]));
            assert_eq!(var_count, json!([[1]]));
            assert_eq!(type_count, json!([[1]]));
            assert_eq!(join_counts, json!([[1, 1]]));
            assert_eq!(graph_counts, json!([[ledger_id, 1]]));

            // Compare COUNT(*) vs raw bindings row count + decoded identity.
            let raw_bindings = query_sparql(&fluree2, &ledger2_loaded, q_var_bindings)
                .await
                .unwrap();
            let row_count: usize = raw_bindings
                .batches
                .iter()
                .map(fluree_db_api::Batch::len)
                .sum();
            assert_eq!(row_count, 1);

            let bg = raw_bindings.binary_graph.as_ref();
            let codes = ledger2_loaded.snapshot.namespaces();

            let mut encoded_ids: Vec<u64> = Vec::new();
            let mut decoded_iris: Vec<String> = Vec::new();
            for batch in &raw_bindings.batches {
                for row in 0..batch.len() {
                    let b = batch.get_by_col(row, 0);
                    match b {
                        Binding::EncodedSid { s_id } => {
                            let bg = bg.expect("EncodedSid requires binary_graph");
                            encoded_ids.push(*s_id);
                            decoded_iris
                                .push(bg.resolve_subject_iri(*s_id).expect("decode subject iri"));
                        }
                        Binding::Sid(sid) => decoded_iris.push(sid_to_iri(sid, codes)),
                        other => panic!("unexpected binding for ?m: {other:?}"),
                    }
                }
            }
            assert_eq!(decoded_iris, vec![member_iri.to_string()]);
            if !encoded_ids.is_empty() {
                assert_eq!(encoded_ids, vec![persisted_id]);
            }

            // Avoid unused warning for the transact result (we still want the commit to occur).
            assert!(r2.receipt.t >= 2);
        })
        .await;
}
